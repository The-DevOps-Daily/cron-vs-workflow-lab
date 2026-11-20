"""Score every setup, partition by partition, from the warehouse and the raw logs.

python3 lab/score.py --from 2026-11-20T17:00 --to 2026-11-20T18:30 [--json out.json]

For each (setup, partition): expected rows (from the upstream), rows in the
warehouse, distinct order_ids, reports received, runs that started (distinct
run ids seen by the extract step), step errors and crashes. Verdict:
  ok       every expected row exactly once, exactly one report
  missing  no rows at all
  partial  some rows missing
  dup      some rows loaded more than once
  report!  rows are right but the report count is not 1
"""

import argparse
import collections
import json
import os
from datetime import datetime, timedelta

import psycopg
import requests

SETUPS = ["naive", "hardened", "k8s", "k8s_append", "kestra", "prefect"]
API = os.environ.get("API_URL", "http://127.0.0.1:8080")
DSN = os.environ.get("DATABASE_URL", "postgresql://lab:lab@127.0.0.1:5432/warehouse")
RAW = os.environ.get("RAW_DIR", os.path.join(os.path.dirname(__file__), "..", "results", "raw"))


def jsonl(name):
    try:
        with open(os.path.join(RAW, name)) as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []


def partitions(a, b):
    t = datetime.strptime(a, "%Y-%m-%dT%H:%M")
    end = datetime.strptime(b, "%Y-%m-%dT%H:%M")
    while t <= end:
        yield t.strftime("%Y-%m-%dT%H:%M")
        t += timedelta(minutes=1)


def score(frm, to):
    parts = list(partitions(frm, to))
    reports = collections.Counter((r["setup"], r["partition"]) for r in jsonl("reports.jsonl"))
    runs = collections.defaultdict(set)
    errors = collections.Counter()
    crashes = collections.Counter()
    for e in jsonl("events.jsonl"):
        k = (e["setup"], e["partition"])
        if e["step"] == "extract" and e["status"] == "start":
            runs[k].add(e["run_id"])
        if e["status"] == "error":
            errors[k] += 1
        if e["status"] == "crash":
            crashes[k] += 1
    out = []
    with psycopg.connect(DSN) as conn:
        for s in SETUPS:
            got = {p: (n, d) for p, n, d in conn.execute(
                f"SELECT partition, count(*), count(DISTINCT order_id) FROM {s}.orders "
                "WHERE partition BETWEEN %s AND %s GROUP BY partition", (frm, to))}
            for p in parts:
                exp = requests.get(f"{API}/truth", params={"partition": p}, timeout=5).json()["rows"]
                n, d = got.get(p, (0, 0))
                rep = reports[(s, p)]
                if n == 0:
                    v = "missing"
                elif n > d:
                    v = "dup"
                elif d < exp:
                    v = "partial"
                elif rep != 1:
                    v = "report!"
                else:
                    v = "ok"
                out.append({"setup": s, "partition": p, "expected": exp, "rows": n,
                            "distinct": d, "reports": rep, "runs": len(runs[(s, p)]),
                            "step_errors": errors[(s, p)], "crashes": crashes[(s, p)],
                            "verdict": v})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", required=True)
    ap.add_argument("--to", required=True)
    ap.add_argument("--json")
    ap.add_argument("--only-bad", action="store_true")
    a = ap.parse_args()
    res = score(a.frm, a.to)
    if a.json:
        with open(a.json, "w") as f:
            json.dump(res, f, indent=1)
    print(f"{'setup':<11} {'partition':<17} {'exp':>4} {'rows':>5} {'dist':>5} {'rep':>3} "
          f"{'runs':>4} {'err':>3} {'crash':>5}  verdict")
    for r in res:
        if a.only_bad and r["verdict"] == "ok":
            continue
        print(f"{r['setup']:<11} {r['partition']:<17} {r['expected']:>4} {r['rows']:>5} "
              f"{r['distinct']:>5} {r['reports']:>3} {r['runs']:>4} {r['step_errors']:>3} "
              f"{r['crashes']:>5}  {r['verdict']}")


if __name__ == "__main__":
    main()
