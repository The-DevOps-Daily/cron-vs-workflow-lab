#!/bin/bash
# The failure timeline. Run it from a second machine, because it reboots and powers off the lab box.
#
#   LAB=root@<ip> DROPLET_ID=<id> DO_TOKEN=<token> ./lab/timeline.sh
#
# One tick is one minute, and the run at minute T loads partition T-1.
# Every setup sees the same faults on the same partitions at the same time.
# Partition faults are programmed up front; reboot, power-off and the manual
# duplicate runs happen live. Every action is logged to results/timeline-controller.jsonl
# with the controller's clock (keep both clocks on NTP).
set -euo pipefail

LAB=${LAB:?set LAB=root@<ip>}
: "${DROPLET_ID:?}" "${DO_TOKEN:?}"
OUT=${OUT:-results/timeline-controller.jsonl}

lab() { ssh -o ConnectTimeout=5 -o BatchMode=yes "$LAB" "$@"; }
log() { printf '{"ts": %s, "action": "%s"}\n' "$(date +%s.%N | cut -c1-14)" "$1" | tee -a "$OUT"; }

# start at the next full minute plus one, so the first partition is clean
S=$(( ( $(date +%s) / 60 + 2 ) * 60 ))
part() { date -u -d "@$(( S + $1 * 60 ))" +%Y-%m-%dT%H:%M; }   # partition minute S+k
at() { local t=$(( S + $1 * 60 + $2 )); while (( $(date +%s) < t )); do sleep 1; done; }  # tick S+k, second s

P_CRASH=$(part 3) P_SLOW=$(part 6) P_POISON=$(part 11) P_DUP=$(part 14) P_REBOOT=$(part 18)
log "start S=$(part 0) crash=$P_CRASH overlap=$P_SLOW poison=$P_POISON dup=$P_DUP reboot=$P_REBOOT"

lab "curl -fsS -X POST localhost:8080/admin/faults -d '{
  \"*|$P_CRASH\":  {\"crash_load_after\": 300, \"crash_times\": 1},
  \"*|$P_SLOW\":   {\"page_delay_ms\": 35000},
  \"*|$P_POISON\": {\"poison\": true},
  \"*|$P_DUP\":    {\"page_delay_ms\": 8000},
  \"*|$P_REBOOT\": {\"page_delay_ms\": 20000}
}'" >/dev/null
log "faults programmed"

# 1. a step that dies halfway: load of P_CRASH crashes after 300 rows, once per setup (tick S+4)
# 2. a run that overlaps the next one: P_SLOW extract takes ~105 s (tick S+7 runs into S+8)
# 6. a poison record in P_POISON (tick S+12)

# 7. a duplicate run: someone starts the job by hand while the scheduled run of P_DUP is going (tick S+15)
at 15 8
log "duplicate: manual runs start"
lab 'nohup /opt/lab/setups/1-cron-naive/etl.sh >> /var/log/etl-naive.log 2>&1 < /dev/null &
     nohup /opt/lab/setups/2-cron-hardened/etl.sh >> /var/log/etl-hardened.log 2>&1 < /dev/null &
     kubectl -n lab create job --from=cronjob/etl manual-dup-$(date +%s)
     kubectl -n lab create job --from=cronjob/etl-append manual-dup-append-$(date +%s)
     curl -fsS -u admin@example.com:Lab-pass-1234 -X POST localhost:8088/api/v1/main/executions/lab/etl -o /dev/null
     docker exec lab-prefect-etl-serve-1 prefect deployment run etl/nightly >/dev/null' || log "duplicate: a manual trigger failed"
log "duplicate: manual runs triggered"

# 3. the host reboots in the middle of the P_REBOOT run (~60 s long), 25 s into tick S+19
at 19 25
log "reboot: systemctl reboot"
lab 'systemctl reboot' || true
sleep 15
until lab true 2>/dev/null; do sleep 2; done
log "reboot: ssh is back"

# 4. the machine is down while schedules pass: power off 20 s into tick S+25, power on at S+27:50
at 25 20
log "outage: systemctl poweroff"
lab 'systemctl poweroff' || true
at 27 50
log "outage: power_on requested"
curl -fsS -X POST -H "Authorization: Bearer $DO_TOKEN" -H "Content-Type: application/json" \
  "https://api.digitalocean.com/v2/droplets/$DROPLET_ID/actions" -d '{"type":"power_on"}' >/dev/null
until lab true 2>/dev/null; do sleep 2; done
log "outage: ssh is back"

# let every setup settle for five ticks, then score
at 35 0
log "end"
lab "cd /opt/lab && python3 lab/score.py --from $(part 0) --to $(part 33) --json results/timeline-score.json" | tee results/timeline-score.txt
