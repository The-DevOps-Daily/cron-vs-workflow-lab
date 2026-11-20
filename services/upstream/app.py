"""Fake upstream for the lab, standard library only.

One process plays four roles so every setup talks to the same thing:
  * the source API the nightly job extracts from (/orders), with fault injection
  * the report sink the job sends its summary to (/report)
  * a dead-man switch in the style of healthchecks.io (/ping/<check>[/start|/fail])
  * a central event log every run writes to (/events)

Everything it records goes to JSONL files under DATA_DIR, so the results survive
a reboot of the box and can be scored afterwards.
"""

import hashlib
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DATA = os.environ.get("DATA_DIR", "/data")
PAGE_SIZE = 200
DEFAULT_PAGE_DELAY_MS = int(os.environ.get("PAGE_DELAY_MS", "500"))
lock = threading.Lock()


def now():
    return time.time()


def append(name, rec):
    rec = {"ts": round(now(), 3), **rec}
    with lock, open(os.path.join(DATA, name), "a") as f:
        f.write(json.dumps(rec) + "\n")


def load_json(name, default):
    try:
        with open(os.path.join(DATA, name)) as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def save_json(name, obj):
    tmp = os.path.join(DATA, name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, os.path.join(DATA, name))


# ---------------------------------------------------------------- data

def rows_for(partition):
    h = hashlib.sha256(partition.encode()).digest()
    return 500 + h[0] % 100


def record(partition, i):
    h = hashlib.sha256(f"{partition}/{i}".encode()).digest()
    cents = int.from_bytes(h[:3], "big") % 50000 + 100
    return {
        "order_id": f"{partition}-{i:04d}",
        "partition": partition,
        "customer": f"c{int.from_bytes(h[3:5], 'big') % 5000:04d}",
        "amount": f"{cents // 100}.{cents % 100:02d}",
    }


# ---------------------------------------------------------------- faults
# keys are "<setup>|<partition>" or "*|<partition>"
# spec fields: page_delay_ms, poison (bool), crash_load_after (rows), crash_times

def fault_for(setup, partition):
    faults = load_json("faults.json", {})
    spec = {}
    spec.update(faults.get(f"*|{partition}", {}))
    spec.update(faults.get(f"{setup}|{partition}", {}))
    return spec


def take_load_crash(setup, partition):
    """Return the crash point once per remaining crash_times, then nothing."""
    with lock:
        faults = load_json("faults.json", {})
        for key in (f"{setup}|{partition}", f"*|{partition}"):
            spec = faults.get(key)
            if not spec or "crash_load_after" not in spec:
                continue
            used = spec.setdefault("crash_used", {})
            if used.get(setup, 0) < spec.get("crash_times", 1):
                used[setup] = used.get(setup, 0) + 1
                save_json("faults.json", faults)
                return spec["crash_load_after"]
    return None


# ---------------------------------------------------------------- dead-man
# Semantics follow the healthchecks.io docs: a check is down when no success
# ping arrived within period + grace, when a /fail ping arrives, or when a
# /start ping is not followed by success within the grace time.

checks = {}  # name -> {"period", "grace", "last_success", "last_start", "last_fail", "status"}


def load_checks():
    cfg = load_json("checks.json", {})
    for name, c in cfg.items():
        checks[name] = {**c, "last_success": None, "last_start": None, "last_fail": None,
                        "status": "new"}
    try:
        with open(os.path.join(DATA, "pings.jsonl")) as f:
            for line in f:
                p = json.loads(line)
                c = checks.get(p["check"])
                if not c:
                    continue
                c["last_" + p["kind"]] = p["ts"]
        with open(os.path.join(DATA, "alerts.jsonl")) as f:
            for line in f:
                a = json.loads(line)
                if a["check"] in checks:
                    checks[a["check"]]["status"] = a["status"]
    except FileNotFoundError:
        pass


def evaluate(name, c, t):
    if c["status"] == "new" and c["last_success"] is None:
        return "new"
    if c["last_fail"] and c["last_fail"] >= max(c["last_success"] or 0, c["last_start"] or 0):
        return "down"
    if c["last_start"] and c["last_start"] > (c["last_success"] or 0) and t - c["last_start"] > c["grace"]:
        return "down"
    if c["last_success"] and t - c["last_success"] > c["period"] + c["grace"]:
        return "down"
    return "up"


def evaluator():
    while True:
        t = now()
        with lock:
            items = list(checks.items())
        for name, c in items:
            s = evaluate(name, c, t)
            if s != c["status"] and s != "new":
                reason = "fail ping" if s == "down" and c["last_fail"] and c["last_fail"] >= max(
                    c["last_success"] or 0, c["last_start"] or 0) else "late" if s == "down" else "recovered"
                c["status"] = s
                append("alerts.jsonl", {"check": name, "status": s, "reason": reason})
        time.sleep(2)


# ---------------------------------------------------------------- http

class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_DELETE(self):
        self.route("DELETE")

    def route(self, method):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        parts = [p for p in u.path.split("/") if p]

        if parts[:1] == ["orders"]:
            setup, partition, page = q["setup"], q["partition"], int(q.get("page", "1"))
            spec = fault_for(setup, partition)
            n = rows_for(partition)
            append("requests.jsonl", {"setup": setup, "partition": partition, "page": page,
                                      "run_id": self.headers.get("X-Run-Id", "")})
            time.sleep(spec.get("page_delay_ms", DEFAULT_PAGE_DELAY_MS) / 1000)
            start = (page - 1) * PAGE_SIZE
            recs = [record(partition, i) for i in range(start, min(start + PAGE_SIZE, n))]
            if spec.get("poison") and start <= 250 < start + PAGE_SIZE:
                recs[250 - start]["amount"] = "N/A"
            nxt = page + 1 if start + PAGE_SIZE < n else None
            return self.send(200, {"records": recs, "next_page": nxt})

        if parts[:1] == ["truth"]:
            p = q["partition"]
            return self.send(200, {"partition": p, "rows": rows_for(p),
                                   "poison": bool(fault_for("*", p).get("poison"))})

        if parts[:2] == ["faults", "load"]:
            return self.send(200, {"crash_after": take_load_crash(q["setup"], q["partition"])})

        if parts[:1] == ["events"] and method == "POST":
            append("events.jsonl", self.body())
            return self.send(200, {"ok": True})

        if parts[:1] == ["report"] and method == "POST":
            append("reports.jsonl", self.body())
            return self.send(200, {"ok": True})

        if parts[:1] == ["ping"] and len(parts) >= 2:
            name = parts[1]
            kind = {"start": "start", "fail": "fail"}.get(parts[2] if len(parts) > 2 else "", "success")
            append("pings.jsonl", {"check": name, "kind": kind})
            with lock:
                c = checks.get(name)
                if c:
                    c["last_" + kind] = round(now(), 3)
            return self.send(200, {"ok": True})

        if parts[:2] == ["admin", "faults"]:
            with lock:
                faults = load_json("faults.json", {})
                if method == "POST":
                    faults.update(self.body())
                elif method == "DELETE":
                    faults = {}
                save_json("faults.json", faults)
            return self.send(200, faults)

        if parts[:2] == ["admin", "checks"] and method == "POST":
            name = parts[2]
            cfg = load_json("checks.json", {})
            cfg[name] = {"period": int(q.get("period", 60)), "grace": int(q.get("grace", 60))}
            save_json("checks.json", cfg)
            with lock:
                checks.setdefault(name, {"last_success": None, "last_start": None,
                                         "last_fail": None, "status": "new"}).update(cfg[name])
            return self.send(200, cfg[name])

        if parts[:1] == ["status"]:
            return self.send(200, {k: {**v, "now": now()} for k, v in checks.items()})

        if parts[:1] == ["healthz"]:
            return self.send(200, {"ok": True})

        self.send(404, {"error": "not found"})


if __name__ == "__main__":
    os.makedirs(DATA, exist_ok=True)
    load_checks()
    threading.Thread(target=evaluator, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()
