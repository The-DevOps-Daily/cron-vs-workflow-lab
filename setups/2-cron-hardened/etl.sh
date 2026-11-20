#!/bin/bash
# Setup 2: the same job with the usual cron hardening:
#   flock (no overlap), timeout per step, retries with backoff,
#   dead-man pings (healthchecks.io style), idempotent load and report.
set -euo pipefail

PARTITION=${PARTITION:-$(date -u -d '1 minute ago' +%Y-%m-%dT%H:%M)}
HC=${HC_URL:-http://127.0.0.1:8080/ping/hardened}

exec 9>/run/lock/etl-hardened.lock
if ! flock -n 9; then
  echo "$(date -u +%FT%TZ) previous run still active, skipping $PARTITION"
  exit 0
fi

WORK=$(mktemp -d /var/tmp/etl-hardened.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
cd /opt/lab/etl
export RUN_ID=$(cat /proc/sys/kernel/random/uuid)

ping() { curl -fsS -m 10 --retry 3 -o /dev/null "$HC$1" || true; }

# 3 attempts, 10 s then 20 s apart, each attempt killed after 240 s
step() {
  local attempt=1
  until timeout 240 python3 etl.py "$@" --setup hardened --partition "$PARTITION" --workdir "$WORK"; do
    if (( attempt >= 3 )); then return 1; fi
    sleep $(( attempt * 10 ))
    attempt=$(( attempt + 1 ))
  done
}

ping /start
if step extract && step transform && step load --mode upsert && step report --report-mode once; then
  ping ""
else
  ping /fail
  exit 1
fi
