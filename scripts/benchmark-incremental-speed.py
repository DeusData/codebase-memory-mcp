#!/usr/bin/env python3
"""Measure fast-mode exact incremental indexing against a fresh full rebuild.

This is an explicit opt-in performance gate. It creates a synthetic Go repo in
a temporary work root, uses an isolated CBM_CACHE_DIR, enables disk incremental
indexing only for that cache, and removes only paths it created.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_FILE_COUNT = 240
DEFAULT_FUNCTIONS_PER_FILE = 12
DEFAULT_CHANGED_FILES = 2
DEFAULT_MIN_SPEEDUP = 10.0
DEFAULT_TIMEOUT_SECONDS = 240
DEFAULT_RANK_REFRESH = "stale_on_exact"
PROJECT_DB_SUFFIX = ".db"
CONFIG_DB_NAME = "_config.db"
LOG_TAIL_LINES = 24
MCP_INIT_PROTOCOL_VERSION = "2024-11-05"
PUBLISH_FULL = "full"
PUBLISH_INCREMENTAL_NOOP = "incremental_noop"
PUBLISH_INCREMENTAL_EXACT = "incremental_exact"
PUBLISH_INCREMENTAL_CONTAINMENT = "incremental_containment"


def now_ms() -> float:
    return time.perf_counter() * 1000.0


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def go_file_content(index: int, revision: int, funcs_per_file: int) -> str:
    lines = ["package main", ""]
    for func_index in range(funcs_per_file):
        value = index * funcs_per_file + func_index + revision
        lines.extend(
            [
                f"func Func{index:04d}_{func_index:02d}() int {{",
                f"\treturn {value}",
                "}",
                "",
            ]
        )
    return "\n".join(lines)


def create_repo(repo_dir: Path, file_count: int, funcs_per_file: int) -> None:
    write_text(repo_dir / "go.mod", "module example.com/cbmbench\n\ngo 1.22\n")
    write_text(repo_dir / "main.go", "package main\n\nfunc main() {}\n")
    for index in range(file_count):
        write_text(repo_dir / f"pkg/file_{index:04d}.go", go_file_content(index, 0, funcs_per_file))


def modify_existing_files(repo_dir: Path, changed_files: int, funcs_per_file: int) -> list[str]:
    changed: list[str] = []
    for index in range(changed_files):
        rel = Path("pkg") / f"file_{index:04d}.go"
        write_text(repo_dir / rel, go_file_content(index, 1000, funcs_per_file))
        changed.append(rel.as_posix())
    return changed


def command_result(
    cmd: list[str],
    env: dict[str, str],
    timeout: int,
    cwd: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], float]:
    start = now_ms()
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return proc, now_ms() - start


def unwrap_cli_json(stdout: str) -> dict[str, Any]:
    outer = json.loads(stdout)
    if "content" in outer:
        return json.loads(outer["content"][0]["text"])
    return outer


def unwrap_mcp_result(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result", {})
    if "content" in result:
        return json.loads(result["content"][0]["text"])
    return result


class McpClient:
    def __init__(self, binary: Path, env: dict[str, str], timeout: int) -> None:
        self.binary = binary
        self.env = env
        self.timeout = timeout
        self.next_id = 1
        self.stderr_lines: list[str] = []
        self.stderr_lock = threading.Lock()
        self.stdout_queue: queue.Queue[str | None] = queue.Queue()
        self.proc: subprocess.Popen[str] | None = None
        self.stdout_thread: threading.Thread | None = None
        self.stderr_thread: threading.Thread | None = None

    def __enter__(self) -> "McpClient":
        self.proc = subprocess.Popen(
            [str(self.binary)],
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.stdout_thread = threading.Thread(target=self._read_stdout, daemon=True)
        self.stderr_thread = threading.Thread(target=self._read_stderr, daemon=True)
        self.stdout_thread.start()
        self.stderr_thread.start()
        self._initialize()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if not self.proc:
            return
        if self.proc.stdin:
            self.proc.stdin.close()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)

    def _read_stdout(self) -> None:
        assert self.proc and self.proc.stdout
        for line in self.proc.stdout:
            self.stdout_queue.put(line)
        self.stdout_queue.put(None)

    def _read_stderr(self) -> None:
        assert self.proc and self.proc.stderr
        for line in self.proc.stderr:
            with self.stderr_lock:
                self.stderr_lines.append(line.rstrip("\n"))

    def _stderr_mark(self) -> int:
        with self.stderr_lock:
            return len(self.stderr_lines)

    def _stderr_since(self, mark: int) -> str:
        with self.stderr_lock:
            return "\n".join(self.stderr_lines[mark:])

    def _send(self, message: dict[str, Any]) -> None:
        if not self.proc or not self.proc.stdin:
            raise RuntimeError("MCP server is not running")
        self.proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        self.proc.stdin.flush()

    def _request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        req_id = self.next_id
        self.next_id += 1
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params is not None:
            message["params"] = params
        self._send(message)

        deadline = time.monotonic() + self.timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"MCP request timed out: {method}")
            line = self.stdout_queue.get(timeout=remaining)
            if line is None:
                raise RuntimeError(f"MCP server exited before response: {method}")
            try:
                response = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"non-JSON MCP stdout line: {line[:200]!r}") from exc
            if response.get("id") == req_id:
                if "error" in response:
                    raise RuntimeError(f"MCP request failed: {response['error']}")
                return response

    def _notification(self, method: str, params: dict[str, Any] | None = None) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._send(message)

    def _initialize(self) -> None:
        self._request(
            "initialize",
            {
                "protocolVersion": MCP_INIT_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "cbm-incr-speed", "version": "1.0"},
            },
        )
        self._notification("notifications/initialized")

    def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> tuple[dict[str, Any], str, int, float]:
        mark = self._stderr_mark()
        start = now_ms()
        response = self._request("tools/call", {"name": name, "arguments": arguments})
        elapsed_ms = now_ms() - start
        stderr = self._stderr_since(mark)
        stdout_bytes = len(json.dumps(response, separators=(",", ":")).encode("utf-8"))
        return unwrap_mcp_result(response), stderr, stdout_bytes, elapsed_ms


def log_tail(stderr: str) -> list[str]:
    lines = stderr.splitlines()
    return lines[-LOG_TAIL_LINES:]


def log_has(stderr: str, marker: str) -> bool:
    return marker in stderr


def response_publish_kind(data: dict[str, Any]) -> str:
    publish_kind = data.get("publish_kind")
    return publish_kind if isinstance(publish_kind, str) else ""


def is_incremental_publish_kind(publish_kind: str) -> bool:
    return publish_kind in {
        PUBLISH_INCREMENTAL_NOOP,
        PUBLISH_INCREMENTAL_EXACT,
        PUBLISH_INCREMENTAL_CONTAINMENT,
    }


def parse_logged_elapsed_ms(stderr: str, marker: str) -> int | None:
    for line in stderr.splitlines():
        if marker not in line:
            continue
        for item in line.split():
            if item.startswith("elapsed_ms="):
                try:
                    return int(item.split("=", 1)[1])
                except ValueError:
                    return None
    return None


def indexed_work_elapsed_ms(logged_elapsed_ms: dict[str, int | None]) -> int | None:
    incremental_ms = logged_elapsed_ms.get("incremental_done")
    if incremental_ms is not None:
        return incremental_ms
    return logged_elapsed_ms.get("pipeline_done")


def run_config_set(binary: Path, env: dict[str, str], key: str, value: str, timeout: int) -> None:
    proc, _ = command_result([str(binary), "config", "set", key, value], env, timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"config set {key} failed: {proc.stderr.strip()}")


def build_index_result(
    data: dict[str, Any],
    stderr: str,
    stdout_bytes: int,
    elapsed_ms: float,
    include_logs: bool,
) -> dict[str, Any]:
    elapsed_ms_int = int(elapsed_ms)
    publish_kind = response_publish_kind(data)
    logged_elapsed_ms = {
        "pipeline_done": parse_logged_elapsed_ms(stderr, "pipeline.done"),
        "incremental_done": parse_logged_elapsed_ms(stderr, "incremental.done"),
    }
    indexed_ms = indexed_work_elapsed_ms(logged_elapsed_ms)
    result: dict[str, Any] = {
        "elapsed_ms": elapsed_ms_int,
        "indexed_work_elapsed_ms": indexed_ms,
        "unlogged_overhead_ms": (elapsed_ms_int - indexed_ms) if indexed_ms is not None else None,
        "response": data,
        "publish_kind": publish_kind or None,
        "stdout_bytes": stdout_bytes,
        "markers": {
            "incremental_exact_done": log_has(stderr, "incremental.exact.done")
            or publish_kind == PUBLISH_INCREMENTAL_EXACT,
            "incremental_done": log_has(stderr, "incremental.done")
            or is_incremental_publish_kind(publish_kind),
            "pagerank_done": log_has(stderr, "pagerank.done"),
            "pagerank_defer": log_has(stderr, "pagerank.defer"),
            "full_route": log_has(stderr, "pipeline.route path=full")
            or publish_kind == PUBLISH_FULL,
            "incremental_route": log_has(stderr, "pipeline.route path=incremental")
            or is_incremental_publish_kind(publish_kind),
        },
        "logged_elapsed_ms": logged_elapsed_ms,
        "stderr_tail": log_tail(stderr),
    }
    if include_logs:
        result["stderr"] = stderr
    return result


def run_index(
    binary: Path,
    env: dict[str, str],
    repo_dir: Path,
    timeout: int,
    include_logs: bool,
) -> dict[str, Any]:
    args = json.dumps({"repo_path": str(repo_dir), "mode": "fast"})
    proc, elapsed_ms = command_result(
        [str(binary), "cli", "--json", "index_repository", args],
        env,
        timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"index_repository failed: {proc.stderr.strip()}")
    data = unwrap_cli_json(proc.stdout)
    return build_index_result(
        data, proc.stderr, len(proc.stdout.encode("utf-8")), elapsed_ms, include_logs
    )


def run_index_mcp(client: McpClient, repo_dir: Path, include_logs: bool) -> dict[str, Any]:
    data, stderr, stdout_bytes, elapsed_ms = client.call_tool(
        "index_repository", {"repo_path": str(repo_dir), "mode": "fast"}
    )
    return build_index_result(data, stderr, stdout_bytes, elapsed_ms, include_logs)


def remove_project_dbs(cache_dir: Path) -> list[str]:
    removed: list[str] = []
    for path in cache_dir.iterdir():
        if not path.is_file():
            continue
        if path.name == CONFIG_DB_NAME or not path.name.endswith(PROJECT_DB_SUFFIX):
            continue
        path.unlink()
        removed.append(path.name)
        for suffix in ("-wal", "-shm"):
            sidecar = cache_dir / f"{path.name}{suffix}"
            if sidecar.exists():
                sidecar.unlink()
                removed.append(sidecar.name)
    return removed


def build_env(cache_dir: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = str(cache_dir)
    env["CBM_AUTO_INDEX"] = "false"
    env["CBM_CONTEXT_INJECTION"] = "false"
    return env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gate exact fast-mode incremental indexing against a fresh full rebuild."
    )
    parser.add_argument("--binary", default="build/c/codebase-memory-mcp")
    parser.add_argument("--work-root", default="")
    parser.add_argument("--out", default="")
    parser.add_argument("--files", type=int, default=DEFAULT_FILE_COUNT)
    parser.add_argument("--functions-per-file", type=int, default=DEFAULT_FUNCTIONS_PER_FILE)
    parser.add_argument("--changed-files", type=int, default=DEFAULT_CHANGED_FILES)
    parser.add_argument("--min-speedup", type=float, default=DEFAULT_MIN_SPEEDUP)
    parser.add_argument(
        "--rank-refresh",
        choices=("eager", "stale_on_exact"),
        default=DEFAULT_RANK_REFRESH,
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--keep-work-root", action="store_true")
    parser.add_argument("--include-logs", action="store_true")
    parser.add_argument(
        "--transport",
        choices=("cli", "mcp"),
        default="cli",
        help="Measure cold CLI subprocess calls or persistent MCP tool-call latency.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = Path(args.binary).expanduser()
    if not binary.is_absolute():
        binary = Path.cwd() / binary
    binary = binary.resolve()
    if not binary.is_file():
        print(f"error: binary not found: {binary}", file=sys.stderr)
        return 2

    auto_root = not bool(args.work_root)
    work_root = Path(args.work_root).expanduser() if args.work_root else Path(
        tempfile.mkdtemp(prefix="cbm-incr-speed-")
    )
    work_root.mkdir(parents=True, exist_ok=True)
    repo_dir = work_root / "repo"
    cache_dir = work_root / "cache"
    repo_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "work_root": str(work_root),
        "parameters": {
            "files": args.files,
            "functions_per_file": args.functions_per_file,
            "changed_files": args.changed_files,
            "min_speedup": args.min_speedup,
            "rank_refresh": args.rank_refresh,
            "timeout": args.timeout,
            "transport": args.transport,
        },
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
    }

    exit_code = 1
    try:
        create_repo(repo_dir, args.files, args.functions_per_file)
        env = build_env(cache_dir)
        run_config_set(binary, env, "incremental_reindex", "always", args.timeout)
        run_config_set(binary, env, "rank_refresh", args.rank_refresh, args.timeout)

        if args.transport == "mcp":
            with McpClient(binary, env, args.timeout) as client:
                initial = run_index_mcp(client, repo_dir, args.include_logs)
                changed_paths = modify_existing_files(
                    repo_dir, args.changed_files, args.functions_per_file
                )
                incremental = run_index_mcp(client, repo_dir, args.include_logs)
            removed_dbs = remove_project_dbs(cache_dir)
            with McpClient(binary, env, args.timeout) as client:
                full_rebuild = run_index_mcp(client, repo_dir, args.include_logs)
        else:
            initial = run_index(binary, env, repo_dir, args.timeout, args.include_logs)
            changed_paths = modify_existing_files(
                repo_dir, args.changed_files, args.functions_per_file
            )
            incremental = run_index(binary, env, repo_dir, args.timeout, args.include_logs)
            removed_dbs = remove_project_dbs(cache_dir)
            full_rebuild = run_index(binary, env, repo_dir, args.timeout, args.include_logs)

        incr_ms = max(1, int(incremental["elapsed_ms"]))
        full_ms = max(1, int(full_rebuild["elapsed_ms"]))
        speedup = full_ms / incr_ms
        incremental_markers = incremental["markers"]
        exact_marker = bool(incremental_markers["incremental_exact_done"])
        defer_marker = bool(incremental_markers["pagerank_defer"])
        passed = speedup >= args.min_speedup and exact_marker

        report.update(
            {
                "changed_paths": changed_paths,
                "removed_project_dbs": removed_dbs,
                "measurements": {
                    "initial_fast_full": initial,
                    "incremental_exact": incremental,
                    "fresh_fast_full_after_change": full_rebuild,
                },
                "derived": {
                    "speedup_full_rebuild_over_incremental": speedup,
                    "exact_incremental_marker_seen": exact_marker,
                    "rank_defer_marker_seen": defer_marker,
                    "passed": passed,
                },
            }
        )
        exit_code = 0 if passed else 1
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        exit_code = 1
    finally:
        if auto_root and not args.keep_work_root:
            shutil.rmtree(work_root, ignore_errors=True)
            report["cleanup"]["removed"] = not work_root.exists()
        if args.out:
            out_path = Path(args.out).expanduser()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2, sort_keys=True))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
