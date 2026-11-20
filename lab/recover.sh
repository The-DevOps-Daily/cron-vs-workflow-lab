#!/bin/bash
# What a person types to repair each setup after the timeline (run on the lab box).
# The partitions are the ones results/timeline-score.txt marked as not ok.
# Each setup's repair is logged with start and end times to results/raw/recovery.jsonl.
#
#   ./lab/recover.sh poison-fixed | naive | hardened | k8s | k8s_append | kestra | prefect | kestra-backfill-demo
set -euo pipefail
cd "$(dirname "$0")/.."
LOG=results/raw/recovery.jsonl
set -a; . ./.env; set +a
DB=$DATABASE_URL
KESTRA="curl -fsS -u $KESTRA_USER:$KESTRA_PASSWORD"
D=2026-11-20T

note() { printf '{"ts": %s, "setup": "%s", "event": "%s"}\n' "$(date +%s.%N | cut -c1-14)" "$1" "$2" >> "$LOG"; }

# k8s: a Job from the CronJob template with PARTITION set. `kubectl create job --from`
# cannot set env vars, so render it, patch it, apply it.
k8s_job() { # cronjob partition
  local name="bf-$1-$(echo "$2" | tr -d ':T-' | tail -c 5)"
  kubectl -n lab create job --from=cronjob/"$1" "$name" --dry-run=client -o json |
    jq --arg p "$2" '.spec.template.spec.containers[0].env += [{"name": "PARTITION", "value": $p}]' |
    kubectl apply -f - >/dev/null
  JOBS+=("job/$name")
}
JOBS=()

case "${1:?which setup}" in
  poison-fixed)
    # stands in for "the upstream corrected the bad record"
    curl -fsS -X POST localhost:8080/admin/faults -d "{\"*|${D}17:11\": {}}" >/dev/null
    note all "poison record fixed upstream";;

  naive)
    note naive start
    # append-only table: delete what is there first, or a rerun duplicates it
    for p in 17:03 17:06 17:11 17:14 17:18 17:19 17:25 17:26 17:27; do
      psql -q "$DB" -c "DELETE FROM naive.orders WHERE partition = '$D$p'"
      PARTITION=$D$p /opt/lab/setups/1-cron-naive/etl.sh >> /var/log/etl-naive.log 2>&1
    done
    note naive end;;

  naive-paused)
    # second attempt: the first one collided with the scheduled run in the shared work
    # directory, so stop cron first, then backfill the ticks skipped while it was stopped
    note naive "pause cron"
    mv /etc/cron.d/etl-naive /root/etl-naive.cron.paused
    m1=$(date -u +%s)
    while pgrep -f 1-cron-naive/etl.sh >/dev/null; do sleep 2; done
    for p in 17:06 17:11 17:14 17:18 17:19 17:25 17:26 17:27; do
      psql -q "$DB" -c "DELETE FROM naive.orders WHERE partition = '$D$p'"
      PARTITION=$D$p /opt/lab/setups/1-cron-naive/etl.sh >> /var/log/etl-naive.log 2>&1
    done
    mv /root/etl-naive.cron.paused /etc/cron.d/etl-naive
    m2=$(date -u +%s)
    note naive "cron restored"
    # partitions whose ticks fell inside the pause
    for (( t = m1 / 60 * 60; t < m2 / 60 * 60; t += 60 )); do
      p=$(date -u -d "@$t" +%Y-%m-%dT%H:%M)
      PARTITION=$p /opt/lab/setups/1-cron-naive/etl.sh >> /var/log/etl-naive.log 2>&1
    done
    note naive end;;

  hardened-again)
    # the backfill loop held the flock, so the scheduled run at 17:53 skipped partition 17:52;
    # this rerun then did the same to the scheduled run at 18:04 (partition 18:03)
    note hardened start
    PARTITION=${D}17:52 /opt/lab/setups/2-cron-hardened/etl.sh >> /var/log/etl-hardened.log 2>&1
    note hardened end;;

  hardened-third)
    # start 15 s after a tick, when the scheduled run (about 6 s) is done
    while (( $(date +%S | sed 's/^0//') < 15 || $(date +%S | sed 's/^0//') > 30 )); do sleep 1; done
    note hardened start
    PARTITION=${D}18:03 /opt/lab/setups/2-cron-hardened/etl.sh >> /var/log/etl-hardened.log 2>&1
    note hardened end;;

  kestra-new-execution)
    # restarting from the failed task reused the extracted file, which still had the poison
    # record in it; start a fresh execution for the partition instead
    note kestra start
    $KESTRA -X POST -F partition=${D}17:11 localhost:8088/api/v1/main/executions/lab/etl -o /dev/null
    note kestra end;;

  hardened)
    note hardened start
    for p in 17:07 17:11 17:18 17:25 17:26 17:27; do
      PARTITION=$D$p /opt/lab/setups/2-cron-hardened/etl.sh >> /var/log/etl-hardened.log 2>&1
    done
    note hardened end;;

  k8s)
    note k8s start
    for p in 17:11 17:18 17:25 17:26; do k8s_job etl "$D$p"; done
    kubectl -n lab wait --for=condition=complete --timeout=300s "${JOBS[@]}"
    note k8s end;;

  k8s_append)
    note k8s_append start
    for p in 17:03 17:11 17:14 17:18 17:19 17:25 17:26; do
      psql -q "$DB" -c "DELETE FROM k8s_append.orders WHERE partition = '$D$p'"
      k8s_job etl-append "$D$p"
    done
    kubectl -n lab wait --for=condition=complete --timeout=300s "${JOBS[@]}"
    note k8s_append end;;

  kestra)
    note kestra start
    # restart the failed execution from its failed task; it keeps its original trigger date
    id=$($KESTRA "localhost:8088/api/v1/main/executions/search?namespace=lab&flowId=etl&state=FAILED&size=1000" |
      jq -r '.results[] | select(.state.current == "FAILED" and .trigger.variables.date == "2026-11-20T17:12:00.000000Z") | .id')
    $KESTRA -X POST -H "Content-Type: application/json" -d "[\"$id\"]" \
      localhost:8088/api/v1/main/executions/restart/by-ids -o /dev/null
    note kestra end;;

  prefect)
    note prefect start
    # the run that was Running when the box rebooted never ends on its own and holds the
    # deployment's only concurrency slot. `prefect flow-run cancel <id>` only moved it to
    # Cancelling (there is no process left to cancel), so force it to Crashed.
    docker cp lab/prefect-unstick.py lab-prefect-etl-serve-1:/tmp/
    docker exec lab-prefect-etl-serve-1 python /tmp/prefect-unstick.py
    note prefect "zombie forced to Crashed"
    for p in 17:03 17:11 17:18; do
      docker exec lab-prefect-etl-serve-1 prefect deployment run etl/nightly --param partition=$D$p >/dev/null
    done
    note prefect end;;

  kestra-backfill-demo)
    # the built-in backfill: replay three schedule ticks that already succeeded
    note kestra-backfill start
    $KESTRA -X PUT localhost:8088/api/v1/main/triggers/backfill/create -H "Content-Type: application/json" \
      -d '{"namespace": "lab", "flowId": "etl", "triggerId": "nightly",
           "backfill": {"start": "2026-11-20T17:26:00Z", "end": "2026-11-20T17:28:00Z",
                        "labels": [{"key": "reason", "value": "outage"}]}}' -o /dev/null
    note kestra-backfill end;;
esac
