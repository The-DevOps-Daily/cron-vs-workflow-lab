#!/bin/bash
# Reboot the box in the middle of a Prefect run, then record every 15 s what happens to the
# orphaned run and to the deployment's concurrency slot. Run from a second machine.
#   LAB=root@<ip> OUT=results/prefect-reboot-<variant>.txt ./lab/prefect-reboot-test.sh
set -euo pipefail
LAB=${LAB:?}; OUT=${OUT:?}
lab() { ssh -o ConnectTimeout=5 -o BatchMode=yes "$LAB" "$@"; }
S=$(( ( $(date +%s) / 60 + 2 ) * 60 ))
# WATCH_ONLY=<partition> skips the fault and the reboot and only records
if [ -n "${WATCH_ONLY:-}" ]; then P=$WATCH_ONLY; else
P=$(date -u -d "@$S" +%Y-%m-%dT%H:%M)            # loaded by the run at tick S+1
lab "curl -fsS -X POST localhost:8080/admin/faults -d '{\"*|$P\": {\"page_delay_ms\": 30000}}' >/dev/null"
echo "partition $P is slow (about 90 s); the run starts at $(date -u -d "@$((S+60))" +%T)" | tee "$OUT"
while (( $(date +%s) < S + 60 + 50 )); do sleep 1; done
echo "$(date -u +%T) systemctl reboot" | tee -a "$OUT"
lab 'systemctl reboot' || true
sleep 15; until lab true 2>/dev/null; do sleep 2; done
echo "$(date -u +%T) ssh is back" | tee -a "$OUT"
fi
for i in $(seq 1 52); do       # 13 minutes
  lab "docker exec -i lab-prefect-etl-serve-1 python - $P" 2>/dev/null <<'PY' | tee -a "$OUT" || echo "$(date -u +%T) server not ready" | tee -a "$OUT"
import asyncio, sys, datetime
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterExpectedStartTime
async def main():
    p = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%dT%H:%M").replace(tzinfo=datetime.timezone.utc)
    async with get_client() as c:
        dep = await c.read_deployment_by_name("etl/nightly")
        slots = dep.global_concurrency_limit.active_slots if dep.global_concurrency_limit else None
        f = FlowRunFilter(expected_start_time=FlowRunFilterExpectedStartTime(
            after_=p, before_=datetime.datetime.now(datetime.timezone.utc)))
        runs = sorted(await c.read_flow_runs(flow_run_filter=f), key=lambda r: r.expected_start_time)
        states = " ".join(f"{r.expected_start_time:%H:%M}={r.state.name}" for r in runs)
        print(f"{datetime.datetime.now(datetime.timezone.utc):%H:%M:%S} active_slots={slots} {states}")
asyncio.run(main())
PY
  sleep 15
done
