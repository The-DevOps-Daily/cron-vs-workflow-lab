#!/bin/bash
# Dump what each scheduler itself recorded, next to the raw logs (run on the lab box).
cd "$(dirname "$0")/../results/raw"
kubectl -n lab get jobs -o json | jq -c '.items[] | {name: .metadata.name,
  scheduled: .metadata.annotations["batch.kubernetes.io/cronjob-scheduled-timestamp"],
  created: .metadata.creationTimestamp, start: .status.startTime, done: .status.completionTime,
  succeeded: .status.succeeded, failed: .status.failed,
  conditions: [.status.conditions[]? | {type, reason}]}' > k8s-jobs.new
# the CronJob keeps 100 Jobs; keep the ones earlier collections saw (newest record wins)
cat k8s-jobs.new k8s-jobs.jsonl 2>/dev/null | jq -sc 'group_by(.name) | map(.[0]) | sort_by(.created) | .[]' > k8s-jobs.tmp &&
  mv k8s-jobs.tmp k8s-jobs.jsonl && rm k8s-jobs.new
kubectl -n lab get events --sort-by=.lastTimestamp -o json | jq -c '.items[] | {last: .lastTimestamp,
  count, reason, object: .involvedObject.name, message}' > k8s-events.new
# events expire after an hour, so keep what earlier collections saw
cat k8s-events.new k8s-events.jsonl 2>/dev/null | sort -u > k8s-events.tmp && mv k8s-events.tmp k8s-events.jsonl && rm k8s-events.new
set -a; . ../../.env; set +a
K="curl -fsS -u $KESTRA_USER:$KESTRA_PASSWORD localhost:8088/api/v1/main/executions"
for id in $($K/search?namespace=lab\&flowId=etl\&size=1000 | jq -r '.results[].id'); do $K/$id; echo; done |
  jq -c '{id, state: .state.current, created: .state.histories[0].date, labels,
    histories: [.state.histories[] | {state, date}], trigger: .trigger.variables.date,
    taskRuns: [.taskRunList[]? | {taskId, state: .state.current, attempts: (.attempts | length)}]}' > kestra-executions.jsonl
docker exec -i lab-prefect-etl-serve-1 python - <<'PY' > prefect-flow-runs.jsonl
import asyncio, json
from prefect.client.orchestration import get_client
async def main():
    async with get_client() as c:
        runs, off = [], 0
        while True:
            page = await c.read_flow_runs(limit=200, offset=off)
            runs += page
            off += 200
            if len(page) < 200:
                break
        for r in sorted(runs, key=lambda r: r.expected_start_time):
            print(json.dumps({"id": str(r.id), "name": r.name, "state": r.state.name if r.state else None,
                "state_type": str(r.state.type) if r.state else None,
                "expected_start": r.expected_start_time.isoformat() if r.expected_start_time else None,
                "start": r.start_time.isoformat() if r.start_time else None,
                "end": r.end_time.isoformat() if r.end_time else None,
                "parameters": r.parameters, "run_count": r.run_count,
                "message": (r.state.message or "")[:200] if r.state else None}))
asyncio.run(main())
PY
wc -l k8s-jobs.jsonl k8s-events.jsonl kestra-executions.jsonl prefect-flow-runs.jsonl
