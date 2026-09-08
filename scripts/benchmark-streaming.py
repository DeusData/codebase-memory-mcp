#!/usr/bin/env python3
"""Isolated batch-index A/B. Run sequentially; retain raw logs and graph digests."""

import argparse
import hashlib
import json
import os
import pathlib
import re
import sqlite3
import subprocess
import sys
import tempfile
import time

p = argparse.ArgumentParser()
p.add_argument("--binary", required=True)
p.add_argument("--repo", required=True)
p.add_argument("--output", required=True)
p.add_argument("--batch", type=int)
p.add_argument("--runs", type=int, default=3)
p.add_argument("--runtime-parent", required=True)
args = p.parse_args()
if not hasattr(os, "wait4"):
    p.error("Native worker RSS measurement currently requires macOS or Linux")
if args.runs < 1 or (args.batch is not None and not 1 <= args.batch <= 4096):
    p.error("runs must be positive; batch must be between 1 and 4096")
root = pathlib.Path(args.output).resolve()
root.mkdir(parents=True, exist_ok=True)
binary = pathlib.Path(args.binary).resolve()
repo = pathlib.Path(args.repo).resolve()


def digest_rows(db, query, normalize=None):
    rows = [list(row) for row in db.execute(query)]
    if normalize:
        rows = [normalize(row) for row in rows]
    rows.sort(key=lambda row: json.dumps(row, sort_keys=True))
    return {
        "count": len(rows),
        "sha256": hashlib.sha256(
            json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }


def props(row):
    row[-1] = json.loads(row[-1] or "{}")
    return row


fingerprint = hashlib.sha256(binary.read_bytes()).hexdigest()
records = []
for i in range(args.runs):
    run = root / f"run{i+1}"
    run.mkdir()
    cache = run / "cache"
    cache.mkdir(mode=0o700)
    runtime = pathlib.Path(tempfile.mkdtemp(prefix="cbmb-", dir=args.runtime_parent))
    env = os.environ.copy()
    for key in (
        "CBM_STREAMING_BATCH_FILES",
        "CBM_DISABLE_LSP_CROSS",
        "CBM_INDEX_SINGLE_THREAD",
    ):
        env.pop(key, None)
    env.update(
        CBM_RUNTIME_DIR=str(runtime),
        CBM_CACHE_DIR=str(cache),
        CBM_ALLOWED_ROOT=str(repo),
        CBM_SESSION_REPO_ROOT=str(repo),
        CBM_WORKERS="4",
        CBM_PROFILE="1",
        CBM_LOG_LEVEL="info",
    )
    if args.batch:
        env["CBM_STREAMING_BATCH_FILES"] = str(args.batch)
    cmd = [
        str(binary),
        "cli",
        "--index-worker",
        "--index-worker-build",
        fingerprint,
        "index_repository",
        json.dumps({"repo_path": str(repo), "mode": "full"}),
        "--response-out",
        str(run / "response.json"),
    ]
    start = time.monotonic()
    with (run / "stdout.json").open("w") as out, (run / "stderr.log").open("w") as err:
        process = subprocess.Popen(cmd, cwd=repo, env=env, stdout=out, stderr=err)
        while True:
            pid, status, usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                process.returncode = os.waitstatus_to_exitcode(status)
                break
            if time.monotonic() - start > 600:
                process.kill()
            time.sleep(0.02)
        result = process
    elapsed = time.monotonic() - start
    record = {
        "binary": str(binary),
        "binary_sha256": fingerprint,
        "repo": str(repo),
        "batch": args.batch,
        "run": i + 1,
        "returncode": result.returncode,
        "worker_wall_s": round(elapsed, 3),
        "worker_peak_rss_bytes": usage.ru_maxrss
        * (1 if sys.platform == "darwin" else 1024),
        "runtime": str(runtime),
        "command": cmd,
    }
    if result.returncode:
        records.append(record)
        print(json.dumps(record), flush=True)
        break
    response = json.loads((run / "response.json").read_text())
    record["result"] = response.get("structuredContent") or (
        json.loads(response["content"][0]["text"])
        if "content" in response
        else response
    )
    logs = (
        (run / "stderr.log").read_text(errors="replace")
        + "\n"
        + "\n".join(
            f.read_text(errors="replace")
            for f in (cache / "logs").glob(".worker-log-*")
        )
    )
    (run / "worker.log").write_text(logs)
    peaks = [int(x) for x in re.findall(r"msg=mem.phase[^\n]*peak_mb=(\d+)", logs)]
    timings = re.findall(r"msg=pipeline.done[^\n]*elapsed_ms=(\d+)", logs)
    record["peak_mb"] = max(peaks) if peaks else None
    record["pipeline_ms"] = int(timings[-1]) if timings else None
    record["batch_peak_rss_mb"] = max(
        [
            int(x)
            for x in re.findall(
                r"msg=mem.phase phase=streaming_pass_[ab]_batch rss_mb=(\d+)", logs
            )
        ]
        or [0]
    )
    dbpath = next(f for f in cache.glob("*.db") if f.name != "_config.db")
    db = sqlite3.connect(dbpath)
    record["nodes"] = digest_rows(
        db, "select label,name,qualified_name,file_path,start_line,end_line from nodes"
    )
    record["node_properties"] = digest_rows(
        db, "select qualified_name,properties from nodes", props
    )
    record["edges"] = digest_rows(
        db,
        "select s.qualified_name,t.qualified_name,e.type,e.properties from edges e join nodes s on s.id=e.source_id join nodes t on t.id=e.target_id",
        props,
    )
    record["surfaces"] = digest_rows(
        db, "select rel_path,surface_sha,defs_json from lsp_surface"
    )
    record["integrity"] = db.execute("pragma integrity_check").fetchone()[0]
    db.close()
    records.append(record)
    (root / "results.json").write_text(json.dumps(records, indent=2))
    print(json.dumps(record), flush=True)
(root / "results.json").write_text(json.dumps(records, indent=2))

if any(row["returncode"] != 0 for row in records):
    sys.exit(1)
