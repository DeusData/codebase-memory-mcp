#!/usr/bin/env python3
"""Run the same tool calls from N MCP sessions at once and report errors,
timeouts and latency (spec 11, SCALE-CONC).

    python3 evals/harness/concurrent.py --binary BIN --project P --sessions 8 --rounds 3 \\
        --calls evals/cases/neo4j/conc_calls.json [--wait 300] [--max-ms 60000]

calls file: [{"tool": "...", "args": {...}}, ...]  ({{project}} is substituted)
Every session runs every call `rounds` times. Prints one JSON summary:
{sessions, rounds, calls, ok, errors, hangs, p50_ms, p95_ms, max_ms, slowest}
and exits 1 when there is any error/hang or max_ms exceeds --max-ms.
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mcp_client import Mcp, reply_text  # noqa: E402


def worker(idx, binary, project, calls, rounds, wait, out):
    try:
        m = Mcp(binary)
    except Exception as e:  # noqa: BLE001
        out.append({"session": idx, "status": "EXIT", "ms": 0, "tool": "initialize", "text": str(e)})
        return
    for r in range(rounds):
        for c in calls:
            args = json.loads(json.dumps(c["args"]).replace("{{project}}", project))
            t0 = time.time()
            status, reply = m.call(c["tool"], args, wait)
            ms = int((time.time() - t0) * 1000)
            out.append({"session": idx, "round": r, "tool": c["tool"], "status": status, "ms": ms,
                        "text": reply_text(reply)[:160] if status != "ok" else ""})
            if status in ("HANG", "EXIT"):
                m.kill()
                return
    m.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", required=True)
    ap.add_argument("--project", required=True)
    ap.add_argument("--calls", required=True)
    ap.add_argument("--sessions", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--wait", type=int, default=300)
    ap.add_argument("--max-ms", type=int, default=120000)
    a = ap.parse_args()
    with open(a.calls, encoding="utf-8") as f:
        calls = json.load(f)
    out = []
    threads = [threading.Thread(target=worker, args=(i, a.binary, a.project, calls, a.rounds, a.wait, out))
               for i in range(a.sessions)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    lat = [r["ms"] for r in out if r["status"] == "ok"]
    errors = [r for r in out if r["status"] == "error"]
    hangs = [r for r in out if r["status"] in ("HANG", "EXIT")]
    slowest = sorted(out, key=lambda r: -r["ms"])[:5]
    summary = {
        "sessions": a.sessions, "rounds": a.rounds, "calls": len(out), "ok": len(lat), "errors": len(errors),
        "hangs": len(hangs), "wall_ms": int((time.time() - t0) * 1000),
        "p50_ms": int(statistics.median(lat)) if lat else None,
        "p95_ms": int(sorted(lat)[int(len(lat) * 0.95) - 1]) if len(lat) >= 20 else (max(lat) if lat else None),
        "max_ms": max(lat) if lat else None,
        "slowest": [{k: r[k] for k in ("session", "tool", "status", "ms")} for r in slowest],
        "error_samples": [r["text"] for r in (errors + hangs)[:5]],
    }
    print(json.dumps(summary, ensure_ascii=False))
    bad = errors or hangs or (summary["max_ms"] or 0) > a.max_ms
    print("concurrency_ok" if not bad else "concurrency_bad")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
