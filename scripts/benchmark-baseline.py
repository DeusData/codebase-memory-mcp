#!/usr/bin/env python3
"""Capture a reproducible local indexing baseline.

The parent process creates one isolated cache per repetition. A short-lived
worker process runs the binary so peak RSS is measured per repetition instead
of accumulating across the benchmark.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time


DERIVED_EDGE_TYPES = {"SIMILAR_TO", "SEMANTICALLY_RELATED"}


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summarize(values: list[float]) -> dict[str, float]:
    median = statistics.median(values)
    return {
        "median": median,
        "p95": percentile(values, 0.95),
        "mad": statistics.median(abs(value - median) for value in values),
    }


def git_value(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    return result.stdout.strip()


def unwrap_response(raw: str) -> dict:
    parsed = json.loads(raw)
    if "structuredContent" in parsed:
        return parsed["structuredContent"]
    content = parsed.get("content")
    if isinstance(content, list) and content:
        text = content[0].get("text")
        if isinstance(text, str):
            return json.loads(text)
    return parsed


def normalized_graph_digest(db_path: Path) -> dict[str, str | int]:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    digest = hashlib.sha256()

    node_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(nodes)").fetchall()
    }
    optional_node_columns = [
        name
        for name in ("symbol_id", "signature", "origin", "confidence")
        if name in node_columns
    ]
    selected_nodes = [
        "project",
        "label",
        "name",
        "qualified_name",
        "file_path",
        "start_line",
        "end_line",
        *optional_node_columns,
        "properties",
    ]
    node_sql = (
        f"SELECT {', '.join(selected_nodes)} FROM nodes "
        f"ORDER BY {', '.join(selected_nodes)}"
    )
    node_count = 0
    for row in connection.execute(node_sql):
        digest.update(b"N\0")
        digest.update(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\n")
        node_count += 1

    edge_count = 0
    edge_sql = """
        SELECT
          s.qualified_name, s.file_path, s.start_line, s.end_line,
          e.type,
          t.qualified_name, t.file_path, t.start_line, t.end_line,
          e.properties
        FROM edges e
        JOIN nodes s ON s.id = e.source_id
        JOIN nodes t ON t.id = e.target_id
        WHERE e.type NOT IN ('SIMILAR_TO', 'SEMANTICALLY_RELATED')
        ORDER BY
          s.qualified_name, s.file_path, s.start_line, s.end_line,
          e.type,
          t.qualified_name, t.file_path, t.start_line, t.end_line,
          e.properties
    """
    for row in connection.execute(edge_sql):
        digest.update(b"E\0")
        digest.update(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\n")
        edge_count += 1
    connection.close()
    return {
        "sha256": digest.hexdigest(),
        "structural_nodes": node_count,
        "structural_edges": edge_count,
    }


def worker(args: argparse.Namespace) -> int:
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    request = json.dumps(
        {"repo_path": str(Path(args.repo).resolve()), "mode": args.mode},
        separators=(",", ":"),
    )
    environment = os.environ.copy()
    environment["CBM_CACHE_DIR"] = str(cache_dir)
    if args.worker_count == "auto":
        environment.pop("CBM_WORKERS", None)
    else:
        environment["CBM_WORKERS"] = args.worker_count
    started = time.perf_counter_ns()
    completed = subprocess.run(
        [args.binary, "cli", "index_repository", request],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
    )
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    rss_kib = float(usage.ru_maxrss)
    if sys.platform == "darwin":
        rss_kib /= 1024.0
    payload = {
        "workers": args.worker_count,
        "exit_code": completed.returncode,
        "elapsed_ms": elapsed_ms,
        "peak_rss_kib": rss_kib,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_checksums(output: Path) -> None:
    lines: list[str] = []
    for path in sorted(output.rglob("*")):
        if not path.is_file() or path.name == "checksums.txt":
            continue
        checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{checksum}  {path.relative_to(output).as_posix()}")
    (output / "checksums.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    repo = Path(args.repo).resolve()
    binary = Path(args.binary).resolve()
    output = Path(args.output).resolve()
    if not repo.is_dir():
        raise SystemExit(f"repository does not exist: {repo}")
    if not binary.is_file():
        raise SystemExit(f"binary does not exist: {binary}")
    if output.exists() and any(output.iterdir()):
        raise SystemExit(f"output directory is not empty: {output}")

    raw_dir = output / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    temp_root = Path(tempfile.mkdtemp(prefix="cbm-benchmark-"))
    records: list[dict] = []
    try:
        worker_counts = [item.strip() for item in args.workers.split(",") if item.strip()]
        if not worker_counts or any(
            item != "auto" and (not item.isdigit() or int(item) < 1)
            for item in worker_counts
        ):
            raise SystemExit("--workers must be a comma-separated list such as 1,2,4,auto")
        for worker_count in worker_counts:
            total = args.warmup + args.repetitions
            for index in range(total):
                cache_dir = temp_root / f"workers-{worker_count}-run-{index:02d}"
                command = [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker",
                    "--binary",
                    str(binary),
                    "--repo",
                    str(repo),
                    "--mode",
                    args.mode,
                    "--cache-dir",
                    str(cache_dir),
                    "--worker-count",
                    worker_count,
                ]
                completed = subprocess.run(
                    command,
                    check=True,
                    text=True,
                    stdout=subprocess.PIPE,
                )
                worker_result = json.loads(completed.stdout)
                phase = "warmup" if index < args.warmup else "measure"
                run_name = f"workers-{worker_count}-{index:02d}-{phase}"
                (raw_dir / f"{run_name}.stdout.json").write_text(
                    worker_result.pop("stdout"), encoding="utf-8"
                )
                (raw_dir / f"{run_name}.stderr.log").write_text(
                    worker_result.pop("stderr"), encoding="utf-8"
                )
                databases = sorted(cache_dir.glob("*.db"))
                if worker_result["exit_code"] != 0 or len(databases) != 1:
                    raise RuntimeError(
                        f"{run_name} failed: exit={worker_result['exit_code']}, "
                        f"databases={len(databases)}"
                    )
                response_text = (raw_dir / f"{run_name}.stdout.json").read_text(
                    encoding="utf-8"
                )
                response = unwrap_response(response_text)
                digest = normalized_graph_digest(databases[0])
                record = {
                    "run": run_name,
                    "phase": phase,
                    **worker_result,
                    "database_bytes": databases[0].stat().st_size,
                    "response": response,
                    "digest": digest,
                }
                records.append(record)
                print(
                    f"{run_name}: {record['elapsed_ms']:.1f} ms, "
                    f"{record['peak_rss_kib']:.0f} KiB, {digest['sha256'][:12]}"
                )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

    measured = [record for record in records if record["phase"] == "measure"]
    digests = sorted({record["digest"]["sha256"] for record in measured})
    by_worker = {}
    for worker_count in sorted({record["workers"] for record in measured}):
        selected = [record for record in measured if record["workers"] == worker_count]
        by_worker[worker_count] = {
            "elapsed_ms": summarize([record["elapsed_ms"] for record in selected]),
            "peak_rss_kib": summarize([record["peak_rss_kib"] for record in selected]),
            "database_bytes": summarize(
                [float(record["database_bytes"]) for record in selected]
            ),
        }
    metrics = {
        "repetitions": args.repetitions,
        "warmup": args.warmup,
        "workers": by_worker,
        "elapsed_ms": summarize([record["elapsed_ms"] for record in measured]),
        "peak_rss_kib": summarize([record["peak_rss_kib"] for record in measured]),
        "database_bytes": summarize(
            [float(record["database_bytes"]) for record in measured]
        ),
        "normalized_structural_digests": digests,
        "deterministic": len(digests) == 1,
        "runs": records,
    }
    manifest = {
        "schema_version": 1,
        "kind": "diagnostic-local-baseline",
        "repository": str(repo),
        "commit": git_value(repo, "rev-parse", "HEAD"),
        "dirty_status": git_value(repo, "status", "--porcelain=v1"),
        "binary": str(binary),
        "mode": args.mode,
        "worker_counts": args.workers,
        "command": " ".join(sys.argv),
        "created_at_unix": int(time.time()),
    }
    environment = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
    }
    write_json(output / "manifest.json", manifest)
    write_json(output / "environment.json", environment)
    write_json(output / "metrics.json", metrics)
    (output / "report.md").write_text(
        "# Local diagnostic baseline\n\n"
        "This is a worktree diagnostic, not a release-grade public benchmark.\n\n"
        f"- Runs per worker setting: {args.repetitions} after {args.warmup} warmup\n"
        f"- Worker settings: {args.workers}\n"
        f"- Median full index: {metrics['elapsed_ms']['median']:.1f} ms\n"
        f"- p95 full index: {metrics['elapsed_ms']['p95']:.1f} ms\n"
        f"- Median peak RSS: {metrics['peak_rss_kib']['median']:.0f} KiB\n"
        f"- Median DB size: {metrics['database_bytes']['median']:.0f} bytes\n"
        f"- Deterministic structural digest: {metrics['deterministic']}\n",
        encoding="utf-8",
    )
    write_checksums(output)
    return 0 if metrics["deterministic"] else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--mode", default="structural")
    parser.add_argument("--output")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--workers", default="1,2,4,auto")
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--cache-dir")
    parser.add_argument("--worker-count", default="auto")
    args = parser.parse_args()
    if args.worker:
        if not args.cache_dir:
            parser.error("--worker requires --cache-dir")
    elif not args.output:
        parser.error("--output is required")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    raise SystemExit(worker(arguments) if arguments.worker else run(arguments))
