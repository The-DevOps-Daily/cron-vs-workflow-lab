# cron-vs-workflow-lab

One nightly ETL job (extract from an API, transform, load into Postgres, send a report),
scheduled five ways on one box, with the same seven failures injected into all of them
at the same time. The lab records what each scheduler does on its own and what a person
has to do afterwards.

Write-up: [Cron Is Not a Workflow Engine: When Scheduled Jobs Need an Orchestrator](https://devops-daily.com/posts/cron-vs-workflow-engine-when-scheduled-jobs-need-orchestration)

| # | Setup | What it is |
|---|-------|-----------|
| 1 | `naive` | cron + a shell script with `set -euo pipefail`, plain INSERTs |
| 2 | `hardened` | cron + `flock`, `timeout`, retries, dead-man pings, idempotent load and report |
| 3 | `k8s` | Kubernetes CronJob (k3s), `concurrencyPolicy: Forbid`, `backoffLimit: 2`, `startingDeadlineSeconds: 300`, `activeDeadlineSeconds: 240`, idempotent load |
| 3b | `k8s_append` | the same CronJob with the plain INSERT load |
| 4 | `kestra` | Kestra 2.0.10 standalone + Postgres, task retries, flow concurrency limit 1 (queue) |
| 5 | `prefect` | Prefect 3.9.4 server + Postgres + `flow.serve()`, task retries, deployment concurrency limit 1 (enqueue) |

To compress time, one "night" is one minute: every setup runs `* * * * *`, and the run at
minute T loads partition T-1.

## Run it

You need a disposable Ubuntu 24.04 VM with 4 vCPU and 8 GB RAM (the lab used a DigitalOcean
`s-4vcpu-8gb` droplet), plus a second machine to drive the timeline, because the timeline
reboots and powers off the VM.

Every published port binds to 127.0.0.1, because Docker's published ports bypass ufw and
nothing in the lab needs to be reachable from outside. Open the Kestra (8088) and Prefect
(4200) UIs through an SSH tunnel: `ssh -L 8088:127.0.0.1:8088 -L 4200:127.0.0.1:4200 root@<vm-ip>`.
Credentials are random, generated into an uncommitted `.env` by `lab/make-env.sh` (see
`.env.example`), and the jobs connect as a non-superuser `etl` role.

```bash
# on the VM, as root
apt-get install -y docker.io docker-compose-v2 python3-psycopg python3-requests postgresql-client jq cron
curl -sfL https://get.k3s.io | sh -s - --disable traefik --disable metrics-server
git clone https://github.com/The-DevOps-Daily/cron-vs-workflow-lab /opt/lab && cd /opt/lab
./lab/make-env.sh && set -a && . ./.env && set +a
./lab/sync-etl.sh
(cd services && docker compose up -d)
docker build -t lab/etl:1 etl && docker save lab/etl:1 | k3s ctr images import -
(cd setups/4-kestra && docker compose up -d --build)
(cd setups/5-prefect && docker compose up -d --build)
curl -s -u "$KESTRA_USER:$KESTRA_PASSWORD" -X POST -H "Content-Type: application/x-yaml" \
  --data-binary @setups/4-kestra/etl.yml localhost:8088/api/v1/main/flows
curl -s -X POST "localhost:8080/admin/checks/hardened?period=60&grace=120"
cp setups/1-cron-naive/crontab /etc/cron.d/etl-naive
cp setups/2-cron-hardened/crontab /etc/cron.d/etl-hardened
kubectl create namespace lab
kubectl -n lab create secret generic etl-db --from-literal=DATABASE_URL="$DATABASE_URL"
kubectl apply -f setups/3-k8s-cronjob/cronjob.yaml

# on the second machine (it needs ssh to the VM, and a DigitalOcean token to power it back on)
LAB=root@<vm-ip> DROPLET_ID=<id> DO_TOKEN=<token> ./lab/timeline.sh

# back on the VM
./lab/collect.sh                      # dump what each scheduler recorded
python3 lab/score.py --from <first partition> --to <last partition>
```

The published run (2026-11-20) used an earlier revision of these files that published the
ports on all interfaces with fixed lab passwords; the droplet sat behind a DigitalOcean cloud
firewall that allowed only SSH in, and was destroyed after the run. The localhost binding,
the generated `.env` and the non-superuser role were added afterwards and checked on a fresh
droplet (`results/hardened-config-check.txt`). The schedulers and the job are unchanged.

If you are not on DigitalOcean, replace the `power_on` call in `lab/timeline.sh` with your
provider's equivalent.

## What is in here

- `etl/etl.py`: the job. Every setup runs this file. `--mode append|upsert` and
  `--report-mode always|once` are the only differences between the naive and hardened code.
- `services/upstream/app.py`: one stdlib Python process that plays the source API (with
  fault injection), the report sink, a dead-man switch with healthchecks.io semantics,
  and the event log every run writes to.
- `setups/*`: one folder per scheduler.
- `lab/timeline.sh`: the seven failures, in order, against all setups at once.
- `lab/score.py`: per setup and partition: expected rows, rows loaded, distinct rows,
  reports sent, runs started, verdict.
- `lab/recover.sh`: the commands we ran to repair each setup afterwards, including the
  second attempts (each case says why the first one was not enough).
- `lab/prefect-unstick.py`: forces Prefect flow runs that were orphaned by a reboot to `Crashed`.
- `lab/timeline-repeat.sh`: the second run, power-off and reboot kept apart.
- `lab/footprint.sh`: resident memory of what each setup keeps running.
- `results/`: everything the published run recorded. `results/raw/*.jsonl` are the raw logs.

## Results

First run (`lab/timeline.sh`), 34 partitions per setup, state after the failures and before
any human help (`results/timeline-score.txt`):

| Setup | Wrong partitions | Notes |
|---|---|---|
| naive | 9 | 1 half loaded, 1 doubled, 7 missing, no alert |
| hardened | 6 | all missing (overlap skip, poison, reboot, 3 outage ticks); 2 dead-man alerts |
| k8s | 4 | after the reboot a retried pod loaded the wrong partition; 1 Job for 3 missed ticks |
| k8s_append | 7 | same as k8s, plus 3 doubled partitions |
| kestra | 1 | poison record only; the reboot blocked the schedule until the task was resubmitted |
| prefect | 18 | crash not retried; after the reboot a zombie run held the concurrency slot |

`results/recovered-score.txt` is the state after `lab/recover.sh`: every row correct, with
duplicate reports left in `naive` and `k8s_append`. `results/raw/recovery.jsonl` logs every
repair step, including the attempts that failed.

The second run (`lab/timeline-repeat.sh`, `results/timeline-repeat-score.txt`) repeats the
power-off and the reboot separately. Power-off: both cron setups lost 3 partitions, the
CronJob 2, Kestra and Prefect 0. Reboot: every setup lost the killed partition except
Kestra, which resubmitted the task 5 min 9 s later and caught up; the CronJob again loaded
the wrong partition from a retried pod; Prefect deadlocked on its concurrency slot again.

Memory each setup keeps resident is in `results/footprint.jsonl` (anon memory per cgroup).

## Follow-up: the Prefect reboot stall with Prefect's documented fix

On 2026-11-21 we re-ran only the reboot-during-a-run failure for Prefect 3.9.4, on a fresh
`s-2vcpu-4gb` droplet with the localhost-only config, using `lab/prefect-reboot-test.sh`:

- `results/prefect-reboot-zombie-automation.txt`: with `PREFECT_FLOWS_HEARTBEAT_FREQUENCY=30`
  (`setups/5-prefect/docker-compose.zombie.yml`) and the zombie-run automation from Prefect's
  docs (`setups/5-prefect/zombie-automation.py`), the orphaned run was marked `Crashed`
  70 s after the reboot, the slot was released, and the schedule continued.
- `results/prefect-reboot-defaults-control.txt`: the same reboot with defaults and the
  automation paused.
- `results/prefect-flow-retries-on-crash.txt`: `@flow(retries=1)` does not retry a run whose
  process exits with 137; it ends `Crashed` after one attempt.

The lab ran on 2026-11-20 on a DigitalOcean `s-4vcpu-8gb` droplet in `lon1`. Times in the
results are UTC.

## License

MIT
