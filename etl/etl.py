"""The nightly job: extract orders from the API, transform, load into Postgres, send a report.

Every setup in the lab runs this same code. Only the scheduler around it changes,
plus two switches that the hardened setups turn on:

  load   --mode append   plain INSERTs committed in batches of 100 (what most first versions do)
  load   --mode upsert   one transaction, INSERT ... ON CONFLICT (order_id) DO UPDATE
  report --mode always   POST the report every time the step runs
  report --mode once     claim the partition in reports_sent first; only the claimer sends

Usage: python3 etl.py <extract|transform|load|report|all> --setup S --partition P --workdir DIR
"""

import argparse
import json
import os
import sys
import time
import uuid
from decimal import Decimal

import psycopg
import requests

API = os.environ.get("API_URL", "http://127.0.0.1:8080")
DSN = os.environ.get("DATABASE_URL", "postgresql://lab:lab@127.0.0.1:5432/warehouse")
RUN_ID = os.environ.get("RUN_ID") or str(uuid.uuid4())


def event(args, step, status, **extra):
    try:
        requests.post(f"{API}/events", timeout=5, json={
            "setup": args.setup, "partition": args.partition, "run_id": RUN_ID,
            "step": step, "status": status, "pid": os.getpid(),
            "host": os.uname().nodename, **extra})
    except requests.RequestException:
        pass  # the event log is for scoring; never fail the job because of it


def extract(args):
    page, n = 1, 0
    with open(os.path.join(args.workdir, "raw.jsonl"), "w") as out:
        while page:
            r = requests.get(f"{API}/orders", timeout=60, headers={"X-Run-Id": RUN_ID},
                             params={"setup": args.setup, "partition": args.partition, "page": page})
            r.raise_for_status()
            body = r.json()
            for rec in body["records"]:
                out.write(json.dumps(rec) + "\n")
                n += 1
            out.flush()
            page = body["next_page"]
    return {"rows": n}


def transform(args):
    n = 0
    with open(os.path.join(args.workdir, "raw.jsonl")) as src, \
            open(os.path.join(args.workdir, "clean.jsonl"), "w") as out:
        for line in src:
            rec = json.loads(line)
            # Decimal("N/A") raises: this is where a poison record stops the run
            cents = int(Decimal(rec["amount"]) * 100)
            out.write(json.dumps({"order_id": rec["order_id"], "partition": rec["partition"],
                                  "customer": rec["customer"], "amount_cents": cents}) + "\n")
            n += 1
    return {"rows": n}


def rows(args):
    with open(os.path.join(args.workdir, "clean.jsonl")) as f:
        return [json.loads(line) for line in f]


def crash_point(args):
    r = requests.get(f"{API}/faults/load", timeout=10,
                     params={"setup": args.setup, "partition": args.partition})
    return r.json().get("crash_after")


def load(args):
    data = rows(args)
    crash_after = crash_point(args)
    schema = args.setup
    with psycopg.connect(DSN) as conn:
        if args.mode == "append":
            sql = (f"INSERT INTO {schema}.orders (order_id, partition, customer, amount_cents) "
                   "VALUES (%(order_id)s, %(partition)s, %(customer)s, %(amount_cents)s)")
            for i in range(0, len(data), 100):
                batch = data[i:i + 100]
                if crash_after is not None and i >= crash_after:
                    event(args, "load", "crash", rows_written=i)
                    os._exit(137)  # the process dies, as if OOM-killed mid-load
                with conn.cursor() as cur:
                    cur.executemany(sql, batch)
                conn.commit()
        else:
            sql = (f"INSERT INTO {schema}.orders (order_id, partition, customer, amount_cents) "
                   "VALUES (%(order_id)s, %(partition)s, %(customer)s, %(amount_cents)s) "
                   "ON CONFLICT (order_id) DO UPDATE SET customer = EXCLUDED.customer, "
                   "amount_cents = EXCLUDED.amount_cents, loaded_at = now()")
            with conn.transaction():
                with conn.cursor() as cur:
                    for i in range(0, len(data), 100):
                        if crash_after is not None and i >= crash_after:
                            event(args, "load", "crash", rows_written=i)
                            os._exit(137)  # uncommitted: Postgres rolls the transaction back
                        cur.executemany(sql, data[i:i + 100])
    return {"rows": len(data)}


def report(args):
    n = len(rows(args))
    total = sum(r["amount_cents"] for r in rows(args))
    payload = {"setup": args.setup, "partition": args.partition, "rows": n,
               "total_cents": total, "run_id": RUN_ID}
    if args.mode == "always":
        requests.post(f"{API}/report", json=payload, timeout=10).raise_for_status()
        return {"sent": True}
    # once: the row lock on reports_sent makes a concurrent duplicate wait, then skip
    with psycopg.connect(DSN) as conn, conn.transaction():
        claimed = conn.execute(
            f"INSERT INTO {args.setup}.reports_sent (partition, run_id) VALUES (%s, %s) "
            "ON CONFLICT (partition) DO NOTHING RETURNING partition",
            (args.partition, RUN_ID)).fetchone()
        if not claimed:
            return {"sent": False, "reason": "already sent"}
        requests.post(f"{API}/report", json=payload, timeout=10).raise_for_status()
    return {"sent": True}


STEPS = {"extract": extract, "transform": transform, "load": load, "report": report}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=[*STEPS, "all"])
    p.add_argument("--setup", required=True)
    p.add_argument("--partition", required=True)
    p.add_argument("--workdir", required=True)
    p.add_argument("--mode", default="append", choices=["append", "upsert"])
    p.add_argument("--report-mode", default="always", choices=["always", "once"])
    args = p.parse_args()
    os.makedirs(args.workdir, exist_ok=True)
    steps = list(STEPS) if args.step == "all" else [args.step]
    for step in steps:
        a = argparse.Namespace(**vars(args))
        if step == "report":
            a.mode = args.report_mode
        t0 = time.time()
        event(a, step, "start")
        try:
            out = STEPS[step](a)
        except Exception as e:
            event(a, step, "error", error=f"{type(e).__name__}: {e}"[:300],
                  seconds=round(time.time() - t0, 2))
            raise
        event(a, step, "ok", seconds=round(time.time() - t0, 2), **out)
        print(f"{step} {args.setup} {args.partition}: {out}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
