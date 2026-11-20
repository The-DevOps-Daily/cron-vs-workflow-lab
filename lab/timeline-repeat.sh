#!/bin/bash
# Second run, for the two failures the first run mixed up: in timeline.sh the power-off
# came six minutes after the reboot, while Kestra and Prefect were still stuck from it.
# Here the box is powered off while idle first, then rebooted mid-run, then left alone
# for 14 ticks.
#
#   LAB=root@<ip> DROPLET_ID=<id> DO_TOKEN=<token> ./lab/timeline-repeat.sh
set -euo pipefail

LAB=${LAB:?set LAB=root@<ip>}
: "${DROPLET_ID:?}" "${DO_TOKEN:?}"
OUT=${OUT:-results/timeline-repeat-controller.jsonl}

lab() { ssh -o ConnectTimeout=5 -o BatchMode=yes "$LAB" "$@"; }
log() { printf '{"ts": %s, "action": "%s"}\n' "$(date +%s.%N | cut -c1-14)" "$1" | tee -a "$OUT"; }

S=$(( ( $(date +%s) / 60 + 2 ) * 60 ))
part() { date -u -d "@$(( S + $1 * 60 ))" +%Y-%m-%dT%H:%M; }
at() { local t=$(( S + $1 * 60 + $2 )); while (( $(date +%s) < t )); do sleep 1; done; }

P_REBOOT=$(part 12)
log "start S=$(part 0) reboot=$P_REBOOT"
lab "curl -fsS -X POST localhost:8080/admin/faults -d '{\"*|$P_REBOOT\": {\"page_delay_ms\": 20000}}'" >/dev/null

# the machine is down while schedules pass: off 20 s into tick S+3 (runs are done), on at S+5:50
at 3 20
log "outage: systemctl poweroff"
lab 'systemctl poweroff' || true
at 5 50
log "outage: power_on requested"
curl -fsS -X POST -H "Authorization: Bearer $DO_TOKEN" -H "Content-Type: application/json" \
  "https://api.digitalocean.com/v2/droplets/$DROPLET_ID/actions" -d '{"type":"power_on"}' >/dev/null
until lab true 2>/dev/null; do sleep 2; done
log "outage: ssh is back"

# the host reboots 25 s into the ~60 s run of P_REBOOT (tick S+13)
at 13 25
log "reboot: systemctl reboot"
lab 'systemctl reboot' || true
sleep 15
until lab true 2>/dev/null; do sleep 2; done
log "reboot: ssh is back"

at 28 0
log "end"
lab "cd /opt/lab && set -a && . ./.env && set +a && python3 lab/score.py --from $(part 0) --to $(part 26) --json results/timeline-repeat-score.json" | tee results/timeline-repeat-score.txt
