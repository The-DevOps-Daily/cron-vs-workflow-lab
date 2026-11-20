#!/bin/sh
# Used by the Kubernetes CronJob: one pod runs the four steps in order.
set -eu
PARTITION=${PARTITION:-$(date -u -d '@'$(( $(date +%s) - 60 )) +%Y-%m-%dT%H:%M)}
WORK=$(mktemp -d)
for step in extract transform load report; do
  python3 /app/etl.py "$step" --setup "$SETUP" --partition "$PARTITION" --workdir "$WORK" \
    --mode "${LOAD_MODE:-upsert}" --report-mode "${REPORT_MODE:-once}"
done
