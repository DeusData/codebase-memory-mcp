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
import re
import shutil
import sqlite3
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
DEFAULT_OVERHEAD_PROBES = 0
DEFAULT_OVERHEAD_TOOL = "index_status"
PROJECT_DB_SUFFIX = ".db"
CONFIG_DB_NAME = "_config.db"
LOG_TAIL_LINES = 24
FAILURE_TAIL_LINES = 80
FAILURE_ARTIFACT_DIRNAME = "failures"
FAILURE_FALLBACK_DIRNAME = "cbm-benchmark-failures"
FAILURE_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
MCP_INIT_PROTOCOL_VERSION = "2024-11-05"
MATRIX_SCENARIOS_DEFAULT = "go_modify_1,go_modify_2,go_create,go_delete,go_rename,go_new_folder,route_decorator,python_reexport"
SELF_DOGFOOD_SCENARIOS_DEFAULT = "noop,one_source_file,route_handler,store_pipeline_batch,multi_file_small"
SELF_DOGFOOD_MARKER_PREFIX = "cbm_pan4_oracle"
SELF_DOGFOOD_REPO_SUBDIR = "repo"
SELF_DOGFOOD_CACHE_SUBDIR = "cache"
PUBLISH_FULL = "full"
PUBLISH_INCREMENTAL_NOOP = "incremental_noop"
PUBLISH_INCREMENTAL_EXACT = "incremental_exact"
PUBLISH_INCREMENTAL_OVERLAY = "incremental_overlay"
PUBLISH_INCREMENTAL_CONTAINMENT = "incremental_containment"
OVERLAY_STATUS_READY = "overlay_ready"  # CBM_STORE_OVERLAY_STATUS_READY
OVERLAY_TOMBSTONE_FILE = "file"  # CBM_STORE_OVERLAY_TOMBSTONE_FILE
OVERLAY_TOMBSTONE_ACTIVE = 1  # STORE_OVERLAY_TOMBSTONE_ACTIVE
OVERLAY_ROW_OWNED = 1  # STORE_OVERLAY_ROW_OWNED
SOURCE_SPAN_LABELS = frozenset(
    {
        "Function",
        "Method",
        "Class",
        "Struct",
        "Interface",
        "Enum",
        "Type",
        "Trait",
        "Module",
    }
)
LOG_MARKER_PIPELINE_DONE = "pipeline.done"
LOG_MARKER_INCREMENTAL_DONE = "incremental.done"
LOG_MARKER_EXACT_DONE = "incremental.exact.done"
LOG_MARKER_EXACT_FRONTIER = "incremental.exact.frontier"
LOG_MARKER_EXACT_FALLBACK = "incremental.exact.fallback"
LOG_MARKER_EXACT_DELETE_FALLBACK = "incremental.exact.delete.fallback"
LOG_MARKER_EXACT_SKIP = "incremental.exact.skip"


class BenchmarkCommandError(RuntimeError):
    def __init__(self, message: str, detail: dict[str, Any]) -> None:
        super().__init__(message)
        self.detail = detail


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


def create_python_reexport_repo(repo_dir: Path) -> None:
    write_text(repo_dir / "fastapi" / "__init__.py", "from .param_functions import Header\n")
    write_text(repo_dir / "fastapi" / "param_functions.py", "def Header(default=None):\n    return default\n")
    write_text(repo_dir / "fastapi" / "openapi" / "models.py", "class Header:\n    pass\n")
    write_text(
        repo_dir / "docs_src" / "app" / "main.py",
        "from fastapi import Header\n\n"
        "def create_item():\n"
        "    return Header(None)\n",
    )


def create_route_repo(repo_dir: Path, route_path: str) -> None:
    write_text(
        repo_dir / "routes.py",
        "from fastapi import FastAPI\n\n"
        "app = FastAPI()\n\n"
        f"@app.get('{route_path}')\n"
        "def orders():\n"
        "    return {'ok': True}\n",
    )


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


def text_tail(text: str, max_lines: int = FAILURE_TAIL_LINES) -> list[str]:
    lines = text.splitlines()
    return lines[-max_lines:]


def failure_artifact_dir(env: dict[str, str]) -> Path:
    cache_dir = env.get("CBM_CACHE_DIR")
    if cache_dir:
        return Path(cache_dir).expanduser().parent / FAILURE_ARTIFACT_DIRNAME
    return Path(tempfile.gettempdir()) / FAILURE_FALLBACK_DIRNAME


def command_failure(
    label: str,
    cmd: list[str],
    env: dict[str, str],
    proc: subprocess.CompletedProcess[str],
    elapsed_ms: float,
) -> BenchmarkCommandError:
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_") or "command"
    stamp = datetime.now(timezone.utc).strftime(FAILURE_TIMESTAMP_FORMAT)
    prefix = failure_artifact_dir(env) / f"{stamp}-{safe_label}"
    stdout_path = Path(f"{prefix}.stdout.txt")
    stderr_path = Path(f"{prefix}.stderr.txt")
    meta_path = Path(f"{prefix}.meta.json")

    write_text(stdout_path, proc.stdout)
    write_text(stderr_path, proc.stderr)
    detail: dict[str, Any] = {
        "label": label,
        "returncode": proc.returncode,
        "elapsed_ms": round(elapsed_ms, 3),
        "stdout_bytes": len(proc.stdout.encode("utf-8")),
        "stderr_bytes": len(proc.stderr.encode("utf-8")),
        "stdout_tail": text_tail(proc.stdout),
        "stderr_tail": text_tail(proc.stderr),
        "artifacts": {
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
            "meta": str(meta_path),
        },
    }
    write_text(
        meta_path,
        json.dumps({"cmd": cmd, **detail}, indent=2, sort_keys=True) + "\n",
    )
    return BenchmarkCommandError(
        f"{label} failed with rc={proc.returncode}; artifacts={detail['artifacts']}",
        detail,
    )


def record_report_error(report: dict[str, Any], exc: Exception) -> None:
    report["error"] = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, BenchmarkCommandError):
        report["error_detail"] = exc.detail


def command_stdout(cmd: list[str], timeout: int, cwd: Path | None = None) -> str:
    proc, _ = command_result(cmd, dict(os.environ), timeout, cwd)
    if proc.returncode != 0:
        rendered = " ".join(cmd)
        raise RuntimeError(f"{rendered} failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def append_text(path: Path, text: str) -> None:
    current = path.read_text(encoding="utf-8")
    path.write_text(current + text, encoding="utf-8")


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


def response_publish_reason(data: dict[str, Any]) -> str:
    publish_reason = data.get("publish_reason")
    return publish_reason if isinstance(publish_reason, str) else ""


def response_freshness(data: dict[str, Any]) -> dict[str, Any] | None:
    freshness = data.get("freshness")
    return freshness if isinstance(freshness, dict) else None


def response_freshness_state(data: dict[str, Any]) -> str:
    freshness = response_freshness(data)
    if not freshness:
        return ""
    state = freshness.get("state")
    return state if isinstance(state, str) else ""


def is_incremental_publish_kind(publish_kind: str) -> bool:
    return publish_kind in {
        PUBLISH_INCREMENTAL_NOOP,
        PUBLISH_INCREMENTAL_EXACT,
        PUBLISH_INCREMENTAL_OVERLAY,
        PUBLISH_INCREMENTAL_CONTAINMENT,
    }


def is_explicit_incremental_route(publish_kind: str | None, reason: str | None = None) -> bool:
    return is_incremental_publish_kind(publish_kind or "") or bool(reason)


def parse_logged_elapsed_ms(stderr: str, marker: str) -> int | None:
    return parse_log_int_field(stderr, marker, "elapsed_ms")


def parse_log_int_field(stderr: str, marker: str, field: str) -> int | None:
    prefix = f"{field}="
    for line in stderr.splitlines():
        if marker not in line:
            continue
        for item in line.split():
            if item.startswith(prefix):
                try:
                    return int(item.split("=", 1)[1])
                except ValueError:
                    return None
    return None


def parse_exact_reason(stderr: str) -> str | None:
    detail = parse_exact_route_detail(stderr)
    reason = detail.get("reason")
    return reason if isinstance(reason, str) and reason else None


def parse_exact_route_detail(stderr: str) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "frontier_changed_files": parse_log_int_field(stderr, LOG_MARKER_EXACT_FRONTIER, "changed"),
        "frontier_expanded_files": parse_log_int_field(stderr, LOG_MARKER_EXACT_FRONTIER, "expanded"),
        "exact_done_files": parse_log_int_field(stderr, LOG_MARKER_EXACT_DONE, "files"),
        "event": None,
        "reason": None,
    }
    reason_markers = (
        (LOG_MARKER_EXACT_FALLBACK, "fallback"),
        (LOG_MARKER_EXACT_DELETE_FALLBACK, "delete_fallback"),
        (LOG_MARKER_EXACT_SKIP, "skip"),
    )
    for line in stderr.splitlines():
        for marker, event in reason_markers:
            prefix = f"msg={marker} reason="
            if prefix not in line:
                continue
            reason = line.split(prefix, 1)[1].split()[0]
            detail["event"] = event
            detail["reason"] = reason or None
            return detail
    if detail["exact_done_files"] is not None:
        detail["event"] = "exact"
    elif detail["frontier_expanded_files"] is not None:
        detail["event"] = "frontier_observed"
    return detail


def response_exact_delta(data: dict[str, Any]) -> dict[str, Any]:
    exact_delta = data.get("exact_delta")
    return exact_delta if isinstance(exact_delta, dict) else {}


def merge_exact_route_detail(
    detail: dict[str, Any],
    data: dict[str, Any],
    publish_kind: str,
    publish_reason: str,
) -> dict[str, Any]:
    exact_delta = response_exact_delta(data)
    field_map = {
        "changed_paths": "frontier_changed_files",
        "affected_paths": "frontier_expanded_files",
        "published_paths": "exact_done_files",
    }
    for response_key, detail_key in field_map.items():
        value = exact_delta.get(response_key)
        if detail.get(detail_key) is None and isinstance(value, int):
            detail[detail_key] = value
    if not detail.get("reason") and publish_reason:
        detail["reason"] = publish_reason
    if not detail.get("event"):
        published = detail.get("exact_done_files")
        if isinstance(published, int) and published > 0:
            detail["event"] = "exact"
        elif isinstance(published, int) and published == 0:
            detail["event"] = "noop"
        elif publish_reason:
            detail["event"] = "fallback"
        elif publish_kind == PUBLISH_INCREMENTAL_EXACT:
            detail["event"] = "exact"
        elif publish_kind == PUBLISH_INCREMENTAL_OVERLAY:
            detail["event"] = "overlay"
        elif publish_kind == PUBLISH_INCREMENTAL_NOOP:
            detail["event"] = "noop"
    return detail


def indexed_work_elapsed_ms(logged_elapsed_ms: dict[str, int | None]) -> int | None:
    incremental_ms = logged_elapsed_ms.get("incremental_done")
    if incremental_ms is not None:
        return incremental_ms
    return logged_elapsed_ms.get("pipeline_done")


def run_config_set(binary: Path, env: dict[str, str], key: str, value: str, timeout: int) -> None:
    cmd = [str(binary), "config", "set", key, value]
    proc, elapsed_ms = command_result(cmd, env, timeout)
    if proc.returncode != 0:
        raise command_failure(f"config_set_{key}", cmd, env, proc, elapsed_ms)


def parse_config_overrides(items: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key or not value:
            raise SystemExit(f"error: --config must be key=value, got {item!r}")
        overrides[key] = value
    return overrides


def apply_config_overrides(
    binary: Path, env: dict[str, str], overrides: dict[str, str], timeout: int
) -> None:
    for key, value in overrides.items():
        run_config_set(binary, env, key, value, timeout)


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
        "pipeline_done": parse_logged_elapsed_ms(stderr, LOG_MARKER_PIPELINE_DONE),
        "incremental_done": parse_logged_elapsed_ms(stderr, LOG_MARKER_INCREMENTAL_DONE),
    }
    indexed_ms = indexed_work_elapsed_ms(logged_elapsed_ms)
    publish_reason = response_publish_reason(data)
    exact_route_detail = merge_exact_route_detail(
        parse_exact_route_detail(stderr), data, publish_kind, publish_reason
    )
    freshness = response_freshness(data)
    freshness_state = response_freshness_state(data)
    result: dict[str, Any] = {
        "elapsed_ms": elapsed_ms_int,
        "indexed_work_elapsed_ms": indexed_ms,
        "unlogged_overhead_ms": (elapsed_ms_int - indexed_ms) if indexed_ms is not None else None,
        "response": data,
        "publish_kind": publish_kind or None,
        "freshness_state": freshness_state or None,
        "freshness": freshness,
        "stdout_bytes": stdout_bytes,
        "markers": {
            "incremental_exact_done": log_has(stderr, LOG_MARKER_EXACT_DONE)
            or publish_kind == PUBLISH_INCREMENTAL_EXACT,
            "incremental_done": log_has(stderr, LOG_MARKER_INCREMENTAL_DONE)
            or is_incremental_publish_kind(publish_kind),
            "pagerank_done": log_has(stderr, "pagerank.done"),
            "pagerank_defer": log_has(stderr, "pagerank.defer"),
            "full_route": log_has(stderr, "pipeline.route path=full")
            or publish_kind == PUBLISH_FULL,
            "incremental_route": log_has(stderr, "pipeline.route path=incremental")
            or is_incremental_publish_kind(publish_kind),
        },
        "logged_elapsed_ms": logged_elapsed_ms,
        "exact_reason": publish_reason or exact_route_detail.get("reason"),
        "exact_route_detail": exact_route_detail,
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
    cmd = [str(binary), "cli", "--json", "index_repository", args]
    proc, elapsed_ms = command_result(
        cmd,
        env,
        timeout,
    )
    if proc.returncode != 0:
        raise command_failure("index_repository", cmd, env, proc, elapsed_ms)
    data = unwrap_cli_json(proc.stdout)
    return build_index_result(
        data, proc.stderr, len(proc.stdout.encode("utf-8")), elapsed_ms, include_logs
    )


def run_index_mcp(client: McpClient, repo_dir: Path, include_logs: bool) -> dict[str, Any]:
    data, stderr, stdout_bytes, elapsed_ms = client.call_tool(
        "index_repository", {"repo_path": str(repo_dir), "mode": "fast"}
    )
    return build_index_result(data, stderr, stdout_bytes, elapsed_ms, include_logs)


def build_tool_probe_result(
    data: dict[str, Any],
    stderr: str,
    stdout_bytes: int,
    elapsed_ms: float,
    include_logs: bool,
) -> dict[str, Any]:
    elapsed_ms_value = round(elapsed_ms, 3)
    result: dict[str, Any] = {
        "elapsed_ms": elapsed_ms_value,
        "stdout_bytes": stdout_bytes,
        "response_keys": sorted(str(key) for key in data.keys()),
        "stderr_tail": log_tail(stderr),
    }
    if include_logs:
        result["stderr"] = stderr
    return result


def run_cli_tool_probe(
    binary: Path,
    env: dict[str, str],
    tool_name: str,
    timeout: int,
    include_logs: bool,
) -> dict[str, Any]:
    cmd = [str(binary), "cli", "--json", tool_name, "{}"]
    proc, elapsed_ms = command_result(cmd, env, timeout)
    if proc.returncode != 0:
        raise command_failure(f"{tool_name}_probe", cmd, env, proc, elapsed_ms)
    data = unwrap_cli_json(proc.stdout)
    return build_tool_probe_result(
        data, proc.stderr, len(proc.stdout.encode("utf-8")), elapsed_ms, include_logs
    )


def run_mcp_tool_probe(
    client: McpClient,
    tool_name: str,
    include_logs: bool,
) -> dict[str, Any]:
    data, stderr, stdout_bytes, elapsed_ms = client.call_tool(tool_name, {})
    return build_tool_probe_result(data, stderr, stdout_bytes, elapsed_ms, include_logs)


def build_tool_call_result(
    data: dict[str, Any],
    stderr: str,
    stdout_bytes: int,
    elapsed_ms: float,
    include_logs: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "elapsed_ms": round(elapsed_ms, 3),
        "stdout_bytes": stdout_bytes,
        "response": data,
        "freshness_state": response_freshness_state(data) or None,
        "freshness": response_freshness(data),
        "stderr_tail": log_tail(stderr),
    }
    if include_logs:
        result["stderr"] = stderr
    return result


def run_cli_tool_call(
    binary: Path,
    env: dict[str, str],
    tool_name: str,
    arguments: dict[str, Any],
    timeout: int,
    include_logs: bool,
) -> dict[str, Any]:
    cmd = [str(binary), "cli", "--json", tool_name, json.dumps(arguments, separators=(",", ":"))]
    proc, elapsed_ms = command_result(cmd, env, timeout)
    if proc.returncode != 0:
        raise command_failure(f"{tool_name}_call", cmd, env, proc, elapsed_ms)
    data = unwrap_cli_json(proc.stdout)
    return build_tool_call_result(
        data, proc.stderr, len(proc.stdout.encode("utf-8")), elapsed_ms, include_logs
    )


def run_mcp_tool_call(
    client: McpClient,
    tool_name: str,
    arguments: dict[str, Any],
    include_logs: bool,
) -> dict[str, Any]:
    data, stderr, stdout_bytes, elapsed_ms = client.call_tool(tool_name, arguments)
    return build_tool_call_result(data, stderr, stdout_bytes, elapsed_ms, include_logs)


def run_tool_call_for_transport(
    transport: str,
    binary: Path,
    env: dict[str, str],
    tool_name: str,
    arguments: dict[str, Any],
    timeout: int,
    include_logs: bool,
    client: McpClient | None = None,
) -> dict[str, Any]:
    if transport == "mcp":
        if client is None:
            raise RuntimeError("MCP transport requires an active client")
        return run_mcp_tool_call(client, tool_name, arguments, include_logs)
    return run_cli_tool_call(binary, env, tool_name, arguments, timeout, include_logs)


def summarize_elapsed_ms(probes: list[dict[str, Any]]) -> dict[str, Any]:
    elapsed = sorted(float(probe["elapsed_ms"]) for probe in probes)
    if not elapsed:
        return {"count": 0}
    return {
        "count": len(elapsed),
        "min_ms": elapsed[0],
        "median_ms": elapsed[len(elapsed) // 2],
        "max_ms": elapsed[-1],
    }


def measure_cli_overhead_probes(
    binary: Path,
    env: dict[str, str],
    tool_name: str,
    count: int,
    timeout: int,
    include_logs: bool,
) -> dict[str, Any] | None:
    if count <= 0:
        return None
    probes = [
        run_cli_tool_probe(binary, env, tool_name, timeout, include_logs)
        for _ in range(count)
    ]
    return {"tool": tool_name, "trials": probes, "summary": summarize_elapsed_ms(probes)}


def measure_mcp_overhead_probes(
    client: McpClient,
    tool_name: str,
    count: int,
    include_logs: bool,
) -> dict[str, Any] | None:
    if count <= 0:
        return None
    probes = [run_mcp_tool_probe(client, tool_name, include_logs) for _ in range(count)]
    return {"tool": tool_name, "trials": probes, "summary": summarize_elapsed_ms(probes)}


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


def find_project_db(cache_dir: Path) -> Path:
    dbs = sorted(
        path
        for path in cache_dir.iterdir()
        if path.is_file() and path.name != CONFIG_DB_NAME and path.name.endswith(PROJECT_DB_SUFFIX)
    )
    if len(dbs) != 1:
        names = ", ".join(path.name for path in dbs)
        raise RuntimeError(f"expected one project DB in {cache_dir}, found {len(dbs)}: {names}")
    return dbs[0]


def remove_sqlite_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()


def copy_sqlite_snapshot(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination.unlink()
    remove_sqlite_sidecars(destination)
    uri = f"{source.resolve().as_uri()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as src, sqlite3.connect(str(destination)) as dst:
        src.backup(dst)


def decode_sqlite_text(data: bytes) -> str:
    return data.decode("utf-8", "surrogateescape")


def sqlite_cbm_source_span_label(label: str | None) -> int:
    return int(label in SOURCE_SPAN_LABELS)


def query_rows(db_path: Path, sql: str, params: tuple[Any, ...]) -> list[str]:
    con = sqlite3.connect(str(db_path))
    con.text_factory = decode_sqlite_text
    con.create_function("cbm_source_span_label", 1, sqlite_cbm_source_span_label)
    try:
        rows = [str(row[0]) for row in con.execute(sql, params)]
    finally:
        con.close()
    return rows


def canonical_query_rows(db_path: Path, project: str, sql: str) -> list[str]:
    return query_rows(db_path, sql, (project,))


def compare_query_rows(
    left_db: Path,
    right_db: Path,
    kind: str,
    left_sql: str,
    left_params: tuple[Any, ...],
    right_sql: str,
    right_params: tuple[Any, ...],
) -> dict[str, Any]:
    left = query_rows(left_db, left_sql, left_params)
    right = query_rows(right_db, right_sql, right_params)
    if left != right:
        left_set = set(left)
        right_set = set(right)
        left_only = next((row for row in left if row not in right_set), None)
        right_only = next((row for row in right if row not in left_set), None)
        return {
            "equal": False,
            "kind": kind,
            "left_count": len(left),
            "right_count": len(right),
            "left_only": left_only,
            "right_only": right_only,
        }
    return {"equal": True}


CANONICAL_NODES_SQL = (
    "SELECT quote(label) || char(9) || quote(name) || char(9) || "
    "quote(qualified_name) || char(9) || quote(coalesce(file_path,'')) || char(9) || "
    "start_line || char(9) || end_line || char(9) || "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(n.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '') "
    "FROM nodes n WHERE project = ?1 "
    "ORDER BY label, name, qualified_name, coalesce(file_path,''), start_line, end_line, "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(n.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '')"
)

CANONICAL_EDGES_SQL = (
    "SELECT quote(s.label) || char(9) || quote(s.qualified_name) || char(9) || "
    "quote(coalesce(s.file_path,'')) || char(9) || s.start_line || char(9) || "
    "s.end_line || char(9) || quote(t.label) || char(9) || quote(t.qualified_name) || "
    "char(9) || quote(coalesce(t.file_path,'')) || char(9) || t.start_line || char(9) || "
    "t.end_line || char(9) || quote(e.type) || char(9) || "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(e.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '') "
    "FROM edges e "
    "JOIN nodes s ON s.id = e.source_id "
    "JOIN nodes t ON t.id = e.target_id "
    "WHERE e.project = ?1 "
    "ORDER BY s.label, s.qualified_name, coalesce(s.file_path,''), s.start_line, s.end_line, "
    "t.label, t.qualified_name, coalesce(t.file_path,''), t.start_line, t.end_line, "
    "e.type, COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(e.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '')"
)

CANONICAL_HASHES_SQL = (
    "SELECT quote(rel_path) || char(9) || quote(sha256) || char(9) || mtime_ns || char(9) || "
    "size FROM file_hashes WHERE project = ?1 ORDER BY rel_path"
)

ACTIVE_OVERLAY_CTE_SQL = (
    "WITH active_overlay_files AS ("
    "  SELECT project, rel_path, MAX(overlay_generation) AS overlay_generation"
    "  FROM ("
    "    SELECT n.project, n.rel_path, n.overlay_generation"
    "    FROM overlay_nodes n"
    "    JOIN overlay_generations g"
    "      ON g.project = n.project AND g.overlay_generation = n.overlay_generation"
    "    WHERE g.status = ?1 AND n.project = ?4"
    "    UNION"
    "    SELECT e.project, e.rel_path, e.overlay_generation"
    "    FROM overlay_edges e"
    "    JOIN overlay_generations g"
    "      ON g.project = e.project AND g.overlay_generation = e.overlay_generation"
    "    WHERE g.status = ?1 AND e.project = ?4"
    "    UNION"
    "    SELECT t.project, t.rel_path, t.overlay_generation"
    "    FROM overlay_tombstones t"
    "    JOIN overlay_generations g"
    "      ON g.project = t.project AND g.overlay_generation = t.overlay_generation"
    "    WHERE g.status = ?1 AND t.active = ?3 AND t.project = ?4"
    "  ) overlay_files"
    "  GROUP BY project, rel_path"
    "), active_file_tombstones AS ("
    "  SELECT t.project, t.rel_path, MAX(t.overlay_generation) AS overlay_generation"
    "  FROM overlay_tombstones t"
    "  JOIN overlay_generations g"
    "    ON g.project = t.project AND g.overlay_generation = t.overlay_generation"
    "  WHERE g.status = ?1 AND t.entity_kind = ?2 AND t.active = ?3 AND t.project = ?4"
    "  GROUP BY t.project, t.rel_path"
    "), active_node_candidates AS ("
    "  SELECT 0 AS overlay_row, n.project, n.label, n.name, n.qualified_name, n.file_path,"
    "         n.start_line, n.end_line, n.properties"
    "  FROM nodes n"
    "  WHERE n.project = ?4"
    "    AND NOT EXISTS (SELECT 1 FROM active_file_tombstones af"
    "                    WHERE af.project = n.project AND af.rel_path = n.file_path)"
    "  UNION ALL"
    "  SELECT 1 AS overlay_row, n.project, n.label, n.name, n.qualified_name, n.file_path,"
    "         n.start_line, n.end_line, n.properties"
    "  FROM overlay_nodes n"
    "  JOIN active_overlay_files af"
    "    ON af.project = n.project AND af.rel_path = n.rel_path"
    "   AND af.overlay_generation = n.overlay_generation"
    "  WHERE n.owned = ?5"
    "), active_nodes AS ("
    "  SELECT project, label, name, qualified_name, file_path, start_line, end_line, properties"
    "  FROM ("
    "    SELECT c.*, ROW_NUMBER() OVER ("
    "      PARTITION BY c.project, c.qualified_name"
    "      ORDER BY cbm_source_span_label(c.label) DESC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1"
    "                    THEN CASE WHEN c.file_path <> '' THEN 1 ELSE 0 END"
    "                    ELSE c.overlay_row END DESC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1"
    "                         AND c.start_line > 0 AND c.end_line >= c.start_line"
    "                    THEN c.end_line - c.start_line + 1 ELSE 0 END DESC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1 THEN c.start_line ELSE 0 END ASC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1 THEN c.end_line ELSE 0 END DESC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1 THEN c.file_path ELSE '' END ASC,"
    "               c.overlay_row DESC"
    "    ) AS rn"
    "    FROM active_node_candidates c"
    "  ) ranked_nodes"
    "  WHERE rn = 1"
    "), active_edges AS ("
    "  SELECT e.project, s.qualified_name AS source_qn, t.qualified_name AS target_qn,"
    "         e.type, e.properties"
    "  FROM edges e"
    "  JOIN nodes s ON s.id = e.source_id"
    "  JOIN nodes t ON t.id = e.target_id"
    "  WHERE e.project = ?4"
    "    AND NOT EXISTS (SELECT 1 FROM active_file_tombstones af"
    "                    WHERE af.project = s.project AND af.rel_path = s.file_path)"
    "    AND NOT EXISTS (SELECT 1 FROM active_file_tombstones af"
    "                    WHERE af.project = t.project AND af.rel_path = t.file_path)"
    "  UNION"
    "  SELECT e.project, e.source_qn, e.target_qn, e.type, e.properties"
    "  FROM overlay_edges e"
    "  JOIN active_overlay_files af"
    "    ON af.project = e.project AND af.rel_path = e.rel_path"
    "   AND af.overlay_generation = e.overlay_generation"
    "  WHERE e.owned = ?5"
    ") "
)

ACTIVE_OVERLAY_NODES_SQL = (
    ACTIVE_OVERLAY_CTE_SQL
    + "SELECT quote(label) || char(9) || quote(name) || char(9) || "
    "quote(qualified_name) || char(9) || quote(coalesce(file_path,'')) || char(9) || "
    "start_line || char(9) || end_line || char(9) || "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(n.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '') "
    "FROM active_nodes n WHERE project = ?4 "
    "ORDER BY label, name, qualified_name, coalesce(file_path,''), start_line, end_line, "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(n.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '')"
)

ACTIVE_OVERLAY_EDGES_SQL = (
    ACTIVE_OVERLAY_CTE_SQL
    + "SELECT quote(s.label) || char(9) || quote(s.qualified_name) || char(9) || "
    "quote(coalesce(s.file_path,'')) || char(9) || s.start_line || char(9) || "
    "s.end_line || char(9) || quote(t.label) || char(9) || quote(t.qualified_name) || "
    "char(9) || quote(coalesce(t.file_path,'')) || char(9) || t.start_line || char(9) || "
    "t.end_line || char(9) || quote(e.type) || char(9) || "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(e.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '') "
    "FROM active_edges e "
    "JOIN active_nodes s ON s.project = e.project AND s.qualified_name = e.source_qn "
    "JOIN active_nodes t ON t.project = e.project AND t.qualified_name = e.target_qn "
    "WHERE e.project = ?4 "
    "ORDER BY s.label, s.qualified_name, coalesce(s.file_path,''), s.start_line, s.end_line, "
    "t.label, t.qualified_name, coalesce(t.file_path,''), t.start_line, t.end_line, "
    "e.type, COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(e.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '')"
)


def compare_canonical_graph(left_db: Path, right_db: Path, project: str) -> dict[str, Any]:
    for kind, sql in (
        ("canonical nodes", CANONICAL_NODES_SQL),
        ("canonical edges", CANONICAL_EDGES_SQL),
        ("file hashes", CANONICAL_HASHES_SQL),
    ):
        result = compare_query_rows(left_db, right_db, kind, sql, (project,), sql, (project,))
        if not result["equal"]:
            return result
    return {"equal": True}


def compare_active_overlay_graph(left_db: Path, right_db: Path, project: str) -> dict[str, Any]:
    left_params = (
        OVERLAY_STATUS_READY,
        OVERLAY_TOMBSTONE_FILE,
        OVERLAY_TOMBSTONE_ACTIVE,
        project,
        OVERLAY_ROW_OWNED,
    )
    for kind, left_sql, right_sql in (
        ("active overlay nodes", ACTIVE_OVERLAY_NODES_SQL, CANONICAL_NODES_SQL),
        ("active overlay edges", ACTIVE_OVERLAY_EDGES_SQL, CANONICAL_EDGES_SQL),
    ):
        result = compare_query_rows(
            left_db, right_db, kind, left_sql, left_params, right_sql, (project,)
        )
        if not result["equal"]:
            return result
    return {"equal": True}


def graph_gate_for_publish_kind(
    canonical: dict[str, Any],
    publish_kind: str | None,
    oracle_passed: bool | None = None,
    active_overlay: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_equal = bool(canonical.get("equal"))
    active_overlay_equal = bool(active_overlay and active_overlay.get("equal"))
    if publish_kind == PUBLISH_INCREMENTAL_OVERLAY and active_overlay is not None:
        return {
            "passed": active_overlay_equal,
            "policy": "overlay_active_graph",
            "canonical_equal": canonical_equal,
            "active_overlay_equal": active_overlay_equal,
            "reason": (
                "overlay publish leaves canonical rows unchanged; validate active overlay "
                "nodes and edges against a fresh full graph"
            ),
        }
    if publish_kind == PUBLISH_INCREMENTAL_OVERLAY and oracle_passed is not None:
        return {
            "passed": bool(oracle_passed),
            "policy": "overlay_active_oracles",
            "canonical_equal": canonical_equal,
            "reason": (
                "overlay publish leaves canonical rows unchanged; self-dogfood gates "
                "on active read oracles and freshness metadata"
            ),
        }
    return {
        "passed": canonical_equal,
        "policy": "canonical_graph",
        "canonical_equal": canonical_equal,
    }


def build_env(cache_dir: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = str(cache_dir)
    env["CBM_AUTO_INDEX"] = "false"
    env["CBM_CONTEXT_INJECTION"] = "false"
    return env


def prepare_matrix_scenario(name: str, repo_dir: Path, files: int, funcs_per_file: int) -> None:
    if name in {
        "go_modify_1",
        "go_modify_2",
        "go_create",
        "go_delete",
        "go_rename",
        "go_new_folder",
    }:
        create_repo(repo_dir, files, funcs_per_file)
        return
    if name == "route_decorator":
        create_route_repo(repo_dir, "/api/orders")
        return
    if name == "python_reexport":
        create_python_reexport_repo(repo_dir)
        return
    raise ValueError(f"unknown matrix scenario: {name}")


def mutate_matrix_scenario(name: str, repo_dir: Path, funcs_per_file: int) -> list[str]:
    if name == "go_modify_1":
        return modify_existing_files(repo_dir, 1, funcs_per_file)
    if name == "go_modify_2":
        return modify_existing_files(repo_dir, 2, funcs_per_file)
    if name == "go_create":
        rel = Path("pkg") / "file_created.go"
        write_text(repo_dir / rel, go_file_content(9999, 1, funcs_per_file))
        return [rel.as_posix()]
    if name == "go_delete":
        rel = Path("pkg") / "file_0000.go"
        (repo_dir / rel).unlink()
        return [rel.as_posix()]
    if name == "go_rename":
        old_rel = Path("pkg") / "file_0000.go"
        new_rel = Path("pkg") / "file_renamed.go"
        (repo_dir / old_rel).unlink()
        write_text(repo_dir / new_rel, go_file_content(9998, 1, funcs_per_file))
        return [old_rel.as_posix(), new_rel.as_posix()]
    if name == "go_new_folder":
        rel = Path("newpkg") / "leaf.go"
        write_text(repo_dir / rel, "package newpkg\n\nfunc NewFolderLeaf() int {\n\treturn 23\n}\n")
        return [rel.as_posix()]
    if name == "route_decorator":
        create_route_repo(repo_dir, "/api/items")
        return ["routes.py"]
    if name == "python_reexport":
        rel = Path("fastapi") / "__init__.py"
        write_text(repo_dir / rel, "from .openapi.models import Header\n")
        return [rel.as_posix()]
    raise ValueError(f"unknown matrix scenario: {name}")


def resolve_git_repo_root(repo_root: Path, timeout: int) -> Path:
    root = repo_root.expanduser().resolve()
    return Path(command_stdout(["git", "rev-parse", "--show-toplevel"], timeout, root)).resolve()


def git_metadata(repo_root: Path, timeout: int) -> dict[str, Any]:
    def maybe(args: list[str]) -> str:
        try:
            return command_stdout(["git", *args], timeout, repo_root)
        except Exception as exc:  # noqa: BLE001 - metadata should not abort benchmark execution.
            return f"<error: {type(exc).__name__}: {exc}>"

    return {
        "repo_root": str(repo_root),
        "head": maybe(["rev-parse", "HEAD"]),
        "short_head": maybe(["rev-parse", "--short", "HEAD"]),
        "branch": maybe(["branch", "--show-current"]),
        "dirty_status_short": maybe(["status", "--short"]),
    }


def create_self_dogfood_worktree(source_repo: Path, case_root: Path, timeout: int) -> Path:
    repo_dir = case_root / SELF_DOGFOOD_REPO_SUBDIR
    if repo_dir.exists():
        raise RuntimeError(f"self-dogfood worktree already exists: {repo_dir}")
    proc, _ = command_result(
        ["git", "worktree", "add", "--detach", str(repo_dir), "HEAD"],
        dict(os.environ),
        timeout,
        source_repo,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git worktree add failed: {proc.stderr.strip()}")
    return repo_dir


def remove_self_dogfood_worktree(source_repo: Path, repo_dir: Path, timeout: int) -> dict[str, Any]:
    cleanup: dict[str, Any] = {"requested": True, "path": str(repo_dir), "removed": False}
    proc, _ = command_result(
        ["git", "worktree", "remove", "--force", str(repo_dir)],
        dict(os.environ),
        timeout,
        source_repo,
    )
    if proc.returncode != 0:
        cleanup["git_worktree_remove_error"] = proc.stderr.strip()
        shutil.rmtree(repo_dir, ignore_errors=True)
    cleanup["removed"] = not repo_dir.exists()
    return cleanup


def self_dogfood_marker(name: str) -> str:
    return f"{SELF_DOGFOOD_MARKER_PREFIX}_{name}"


def append_c_marker_function(repo_dir: Path, rel_path: str, marker: str, value: int) -> str:
    append_text(
        repo_dir / rel_path,
        (
            "\n"
            f"static int {marker}(void) {{\n"
            f"    return {value};\n"
            "}\n"
        ),
    )
    return rel_path


def mutate_self_dogfood_scenario(name: str, repo_dir: Path) -> dict[str, Any]:
    marker = self_dogfood_marker(name)
    changed: list[str] = []
    if name == "noop":
        return {"marker": None, "changed_paths": changed, "description": "no source mutation"}
    if name == "one_source_file":
        changed.append(
            append_c_marker_function(repo_dir, "src/pipeline/pipeline_internal.h", marker, 4101)
        )
        return {"marker": marker, "changed_paths": changed, "description": "single C header edit"}
    if name == "route_handler":
        changed.append(append_c_marker_function(repo_dir, "src/ui/http_server.c", marker, 4102))
        append_text(
            repo_dir / "src/ui/http_server.c",
            "\n/* P.A.N4 route oracle literal: /api/pan4-oracle */\n",
        )
        return {
            "marker": marker,
            "changed_paths": changed,
            "description": "HTTP UI handler source edit with route literal oracle",
        }
    if name == "store_pipeline_batch":
        changed.append(append_c_marker_function(repo_dir, "src/store/store.h", marker, 4103))
        second_marker = f"{marker}_pipeline"
        changed.append(
            append_c_marker_function(repo_dir, "src/pipeline/pipeline_internal.h", second_marker, 4104)
        )
        return {
            "marker": marker,
            "secondary_marker": second_marker,
            "changed_paths": changed,
            "description": "small store plus pipeline header batch",
        }
    if name == "multi_file_small":
        changed.append(append_c_marker_function(repo_dir, "src/mcp/mcp.c", marker, 4105))
        second_marker = f"{marker}_test"
        changed.append(append_c_marker_function(repo_dir, "tests/test_mcp.c", second_marker, 4106))
        return {
            "marker": marker,
            "secondary_marker": second_marker,
            "changed_paths": changed,
            "description": "small production plus test source batch",
        }
    raise ValueError(f"unknown self-dogfood scenario: {name}")


def oracle_passed(tool_result: dict[str, Any], marker: str | None) -> bool:
    if not marker:
        return True
    response = tool_result.get("response")
    return marker in json.dumps(response, sort_keys=True)


def run_self_dogfood_oracles(
    transport: str,
    binary: Path,
    env: dict[str, str],
    project: str,
    mutation: dict[str, Any],
    args: argparse.Namespace,
    client: McpClient | None = None,
) -> dict[str, Any]:
    marker = mutation.get("marker")
    changed_paths = list(mutation.get("changed_paths") or [])
    first_changed = changed_paths[0] if changed_paths else ""
    oracles: dict[str, Any] = {}
    if marker:
        search_code_args: dict[str, Any] = {"project": project, "pattern": marker, "limit": 5}
        if first_changed:
            search_code_args["file_pattern"] = Path(first_changed).name
            search_code_args["path_filter"] = f"^{re.escape(first_changed)}$"
        oracles["marker_search_graph"] = run_tool_call_for_transport(
            transport,
            binary,
            env,
            "search_graph",
            {"project": project, "name_pattern": marker, "limit": 5},
            args.timeout,
            args.include_logs,
            client,
        )
        oracles["marker_search_code"] = run_tool_call_for_transport(
            transport,
            binary,
            env,
            "search_code",
            search_code_args,
            args.timeout,
            args.include_logs,
            client,
        )
    if first_changed:
        oracles["changed_file_query_graph"] = run_tool_call_for_transport(
            transport,
            binary,
            env,
            "query_graph",
            {
                "project": project,
                "query": (
                    "MATCH (n) WHERE n.file_path CONTAINS "
                    f"'{first_changed}' RETURN n.name, n.label, n.file_path LIMIT 10"
                ),
            },
            args.timeout,
            args.include_logs,
            client,
        )
        oracles["scoped_architecture"] = run_tool_call_for_transport(
            transport,
            binary,
            env,
            "get_architecture",
            {"project": project, "path": first_changed, "aspects": ["all"]},
            args.timeout,
            args.include_logs,
            client,
        )
    oracles["route_freshness_probe"] = run_tool_call_for_transport(
        transport,
        binary,
        env,
        "search_graph",
        {"project": project, "label": "Route", "limit": 3},
        args.timeout,
        args.include_logs,
        client,
    )
    oracles["passed"] = all(
        oracle_passed(result, marker)
        for key, result in oracles.items()
        if key in {"marker_search_graph", "marker_search_code"}
    )
    return oracles


def run_index_for_transport(
    transport: str,
    binary: Path,
    env: dict[str, str],
    repo_dir: Path,
    timeout: int,
    include_logs: bool,
    client: McpClient | None = None,
) -> dict[str, Any]:
    if transport == "mcp":
        if client is None:
            raise RuntimeError("MCP transport requires an active client")
        return run_index_mcp(client, repo_dir, include_logs)
    return run_index(binary, env, repo_dir, timeout, include_logs)


def run_matrix_case(
    scenario: str,
    binary: Path,
    env: dict[str, str],
    case_root: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    repo_dir = case_root / "repo"
    cache_dir = case_root / "cache"
    repo_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    case_env = dict(env)
    case_env["CBM_CACHE_DIR"] = str(cache_dir)
    run_config_set(binary, case_env, "incremental_reindex", "always", args.timeout)
    run_config_set(binary, case_env, "rank_refresh", args.rank_refresh, args.timeout)
    apply_config_overrides(binary, case_env, args.config_overrides, args.timeout)

    prepare_matrix_scenario(scenario, repo_dir, args.files, args.functions_per_file)
    if args.transport == "mcp":
        with McpClient(binary, case_env, args.timeout) as client:
            initial = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client
            )
            changed_paths = mutate_matrix_scenario(scenario, repo_dir, args.functions_per_file)
            incremental = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client
            )
    else:
        initial = run_index_for_transport(
            args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs
        )
        changed_paths = mutate_matrix_scenario(scenario, repo_dir, args.functions_per_file)
        incremental = run_index_for_transport(
            args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs
        )

    project_db = find_project_db(cache_dir)
    project = str(incremental.get("response", {}).get("project") or project_db.stem)
    incremental_snapshot = case_root / "incremental.db"
    copy_sqlite_snapshot(project_db, incremental_snapshot)
    removed_dbs = remove_project_dbs(cache_dir)

    if args.transport == "mcp":
        with McpClient(binary, case_env, args.timeout) as client:
            full_rebuild = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client
            )
    else:
        full_rebuild = run_index_for_transport(
            args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs
        )

    full_db = find_project_db(cache_dir)
    canonical = compare_canonical_graph(incremental_snapshot, full_db, project)
    incremental_reason = incremental.get("exact_reason")
    publish_kind = incremental.get("publish_kind")
    active_overlay = None
    if publish_kind == PUBLISH_INCREMENTAL_OVERLAY:
        active_overlay = compare_active_overlay_graph(incremental_snapshot, full_db, project)
    graph_gate = graph_gate_for_publish_kind(
        canonical, str(publish_kind or ""), active_overlay=active_overlay
    )
    explicit_route = is_explicit_incremental_route(publish_kind, incremental_reason)
    passed = bool(graph_gate.get("passed")) and explicit_route
    speedup = max(1, int(full_rebuild["elapsed_ms"])) / max(1, int(incremental["elapsed_ms"]))
    return {
        "scenario": scenario,
        "project": project,
        "changed_paths": changed_paths,
        "removed_project_dbs": removed_dbs,
        "initial_fast_full": initial,
        "incremental": incremental,
        "fresh_fast_full_after_change": full_rebuild,
        "canonical_graph": canonical,
        "active_overlay_graph": active_overlay,
        "graph_gate": graph_gate,
        "explicit_exact_or_fallback": explicit_route,
        "explicit_incremental_route": explicit_route,
        "exact_reason": incremental_reason,
        "speedup_full_rebuild_over_incremental": speedup,
        "passed": passed,
    }


def run_matrix(args: argparse.Namespace, binary: Path) -> tuple[dict[str, Any], int]:
    auto_root = not bool(args.work_root)
    work_root = Path(args.work_root).expanduser() if args.work_root else Path(
        tempfile.mkdtemp(prefix="cbm-incr-matrix-")
    )
    work_root.mkdir(parents=True, exist_ok=True)
    scenarios = [item.strip() for item in args.matrix_scenarios.split(",") if item.strip()]
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "work_root": str(work_root),
        "mode": "matrix",
        "parameters": {
            "files": args.files,
            "functions_per_file": args.functions_per_file,
            "rank_refresh": args.rank_refresh,
            "config_overrides": args.config_overrides,
            "timeout": args.timeout,
            "transport": args.transport,
            "scenarios": scenarios,
        },
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
        "cases": [],
    }
    exit_code = 1
    try:
        base_env = build_env(work_root / "cache-base")
        for scenario in scenarios:
            case = run_matrix_case(scenario, binary, base_env, work_root / scenario, args)
            report["cases"].append(case)
        report["derived"] = {
            "passed": all(bool(case.get("passed")) for case in report["cases"]),
            "case_count": len(report["cases"]),
        }
        exit_code = 0 if report["derived"]["passed"] else 1
    except Exception as exc:
        record_report_error(report, exc)
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
    return report, exit_code


def run_self_dogfood_case(
    scenario: str,
    source_repo: Path,
    binary: Path,
    case_root: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    cache_dir = case_root / SELF_DOGFOOD_CACHE_SUBDIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    repo_dir = create_self_dogfood_worktree(source_repo, case_root, args.timeout)
    case_env = build_env(cache_dir)
    cleanup: dict[str, Any] = {"requested": not args.keep_work_root, "removed": False}
    result: dict[str, Any] | None = None
    try:
        run_config_set(binary, case_env, "incremental_reindex", "always", args.timeout)
        run_config_set(binary, case_env, "rank_refresh", args.rank_refresh, args.timeout)
        apply_config_overrides(binary, case_env, args.config_overrides, args.timeout)
        if args.transport == "mcp":
            with McpClient(binary, case_env, args.timeout) as client:
                initial = run_index_for_transport(
                    args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client
                )
                mutation = mutate_self_dogfood_scenario(scenario, repo_dir)
                incremental = run_index_for_transport(
                    args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client
                )
                project_db = find_project_db(cache_dir)
                project = str(incremental.get("response", {}).get("project") or project_db.stem)
                oracles = run_self_dogfood_oracles(
                    args.transport, binary, case_env, project, mutation, args, client
                )
        else:
            initial = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs
            )
            mutation = mutate_self_dogfood_scenario(scenario, repo_dir)
            incremental = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs
            )
            project_db = find_project_db(cache_dir)
            project = str(incremental.get("response", {}).get("project") or project_db.stem)
            oracles = run_self_dogfood_oracles(
                args.transport, binary, case_env, project, mutation, args
            )

        incremental_snapshot = case_root / "incremental.db"
        copy_sqlite_snapshot(project_db, incremental_snapshot)
        removed_dbs = remove_project_dbs(cache_dir)
        if args.transport == "mcp":
            with McpClient(binary, case_env, args.timeout) as client:
                full_rebuild = run_index_for_transport(
                    args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client
                )
        else:
            full_rebuild = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs
            )
        full_db = find_project_db(cache_dir)
        canonical = compare_canonical_graph(incremental_snapshot, full_db, project)
        publish_kind = incremental.get("publish_kind")
        incremental_reason = incremental.get("exact_reason")
        active_overlay = None
        if publish_kind == PUBLISH_INCREMENTAL_OVERLAY:
            active_overlay = compare_active_overlay_graph(incremental_snapshot, full_db, project)
        explicit_route = is_explicit_incremental_route(publish_kind, incremental_reason)
        speedup = max(1, int(full_rebuild["elapsed_ms"])) / max(1, int(incremental["elapsed_ms"]))
        graph_gate = graph_gate_for_publish_kind(
            canonical,
            str(publish_kind or ""),
            bool(oracles.get("passed")),
            active_overlay=active_overlay,
        )
        passed = bool(graph_gate.get("passed")) and explicit_route and bool(oracles.get("passed"))
        result = {
            "scenario": scenario,
            "project": project,
            "repo_dir": str(repo_dir),
            "mutation": mutation,
            "removed_project_dbs": removed_dbs,
            "initial_fast_full": initial,
            "incremental": incremental,
            "fresh_fast_full_after_change": full_rebuild,
            "canonical_graph": canonical,
            "active_overlay_graph": active_overlay,
            "graph_gate": graph_gate,
            "oracles": oracles,
            "explicit_incremental_route": explicit_route,
            "exact_reason": incremental_reason,
            "speedup_full_rebuild_over_incremental": speedup,
            "passed": passed,
        }
    finally:
        if not args.keep_work_root:
            cleanup = remove_self_dogfood_worktree(source_repo, repo_dir, args.timeout)
        if cache_dir.exists() and not args.keep_work_root:
            shutil.rmtree(cache_dir, ignore_errors=True)
            cleanup["cache_removed"] = not cache_dir.exists()
        cleanup["case_root_removed"] = False
        if not args.keep_work_root and case_root.exists():
            shutil.rmtree(case_root, ignore_errors=True)
            cleanup["case_root_removed"] = not case_root.exists()
    if result is None:
        raise RuntimeError(f"self-dogfood case did not produce a result: {scenario}")
    result["cleanup"] = cleanup
    return result


def run_self_dogfood(args: argparse.Namespace, binary: Path) -> tuple[dict[str, Any], int]:
    auto_root = not bool(args.work_root)
    work_root = Path(args.work_root).expanduser() if args.work_root else Path(
        tempfile.mkdtemp(prefix="cbm-self-dogfood-")
    )
    work_root.mkdir(parents=True, exist_ok=True)
    source_repo = resolve_git_repo_root(Path(args.repo_root), args.timeout)
    scenarios = [item.strip() for item in args.self_dogfood_scenarios.split(",") if item.strip()]
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "work_root": str(work_root),
        "source_repo": str(source_repo),
        "source_git": git_metadata(source_repo, args.timeout),
        "mode": "self_dogfood",
        "parameters": {
            "rank_refresh": args.rank_refresh,
            "config_overrides": args.config_overrides,
            "timeout": args.timeout,
            "transport": args.transport,
            "scenarios": scenarios,
        },
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
        "cases": [],
    }
    exit_code = 1
    try:
        for scenario in scenarios:
            case = run_self_dogfood_case(
                scenario, source_repo, binary, work_root / scenario, args
            )
            report["cases"].append(case)
        report["derived"] = {
            "passed": all(bool(case.get("passed")) for case in report["cases"]),
            "case_count": len(report["cases"]),
        }
        exit_code = 0 if report["derived"]["passed"] else 1
    except Exception as exc:
        record_report_error(report, exc)
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
    return report, exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gate exact fast-mode incremental indexing against a fresh full rebuild."
    )
    parser.add_argument("--binary", default="build/c/codebase-memory-mcp")
    parser.add_argument("--work-root", default="")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--out", default="")
    parser.add_argument("--files", type=int, default=DEFAULT_FILE_COUNT)
    parser.add_argument("--functions-per-file", type=int, default=DEFAULT_FUNCTIONS_PER_FILE)
    parser.add_argument("--changed-files", type=int, default=DEFAULT_CHANGED_FILES)
    parser.add_argument("--min-speedup", type=float, default=DEFAULT_MIN_SPEEDUP)
    parser.add_argument(
        "--rank-refresh",
        choices=("eager", "stale_on_exact", "stale_on_incremental"),
        default=DEFAULT_RANK_REFRESH,
    )
    parser.add_argument(
        "--config",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional config override; repeat to set multiple keys. Applied after built-in settings.",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--keep-work-root", action="store_true")
    parser.add_argument("--include-logs", action="store_true")
    parser.add_argument("--matrix", action="store_true", help="Run the affected-frontier scenario matrix.")
    parser.add_argument(
        "--self-dogfood",
        action="store_true",
        help="Run isolated edit-loop scenarios against a detached worktree of --repo-root.",
    )
    parser.add_argument(
        "--matrix-scenarios",
        default=MATRIX_SCENARIOS_DEFAULT,
        help="Comma-separated matrix scenarios to run.",
    )
    parser.add_argument(
        "--self-dogfood-scenarios",
        default=SELF_DOGFOOD_SCENARIOS_DEFAULT,
        help="Comma-separated real-repo edit-loop scenarios to run.",
    )
    parser.add_argument(
        "--transport",
        choices=("cli", "mcp"),
        default="cli",
        help="Measure cold CLI subprocess calls or persistent MCP tool-call latency.",
    )
    parser.add_argument(
        "--overhead-probes",
        type=int,
        default=DEFAULT_OVERHEAD_PROBES,
        help=(
            "Run N cheap tool-call probes before indexing to estimate invocation overhead; "
            "0 preserves the historical gate behavior."
        ),
    )
    parser.add_argument(
        "--overhead-tool",
        default=DEFAULT_OVERHEAD_TOOL,
        help="Existing MCP tool used by --overhead-probes.",
    )
    args = parser.parse_args()
    args.config_overrides = parse_config_overrides(args.config)
    return args


def resolve_binary_path(binary_arg: str) -> Path:
    binary = Path(binary_arg).expanduser()
    if binary.is_absolute():
        return binary.resolve()
    cwd_candidate = (Path.cwd() / binary).resolve()
    if cwd_candidate.is_file():
        return cwd_candidate
    script_candidate = (Path(__file__).resolve().parents[1] / binary).resolve()
    return script_candidate


def main() -> int:
    args = parse_args()
    binary = resolve_binary_path(args.binary)
    if not binary.is_file():
        print(f"error: binary not found: {binary}", file=sys.stderr)
        return 2
    if args.matrix:
        _, matrix_exit_code = run_matrix(args, binary)
        return matrix_exit_code
    if args.self_dogfood:
        _, self_dogfood_exit_code = run_self_dogfood(args, binary)
        return self_dogfood_exit_code

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
            "config_overrides": args.config_overrides,
            "timeout": args.timeout,
            "transport": args.transport,
            "overhead_probes": args.overhead_probes,
            "overhead_tool": args.overhead_tool,
        },
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
    }

    exit_code = 1
    try:
        create_repo(repo_dir, args.files, args.functions_per_file)
        env = build_env(cache_dir)
        run_config_set(binary, env, "incremental_reindex", "always", args.timeout)
        run_config_set(binary, env, "rank_refresh", args.rank_refresh, args.timeout)
        apply_config_overrides(binary, env, args.config_overrides, args.timeout)

        if args.transport == "mcp":
            with McpClient(binary, env, args.timeout) as client:
                overhead_probe = measure_mcp_overhead_probes(
                    client, args.overhead_tool, args.overhead_probes, args.include_logs
                )
                initial = run_index_mcp(client, repo_dir, args.include_logs)
                changed_paths = modify_existing_files(
                    repo_dir, args.changed_files, args.functions_per_file
                )
                incremental = run_index_mcp(client, repo_dir, args.include_logs)
            removed_dbs = remove_project_dbs(cache_dir)
            with McpClient(binary, env, args.timeout) as client:
                full_rebuild = run_index_mcp(client, repo_dir, args.include_logs)
        else:
            overhead_probe = measure_cli_overhead_probes(
                binary,
                env,
                args.overhead_tool,
                args.overhead_probes,
                args.timeout,
                args.include_logs,
            )
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
        explicit_incremental_route = is_incremental_publish_kind(
            str(incremental.get("publish_kind") or "")
        )
        defer_marker = bool(incremental_markers["pagerank_defer"])
        passed = speedup >= args.min_speedup and explicit_incremental_route

        report.update(
            {
                "changed_paths": changed_paths,
                "removed_project_dbs": removed_dbs,
                "measurements": {
                    "overhead_probe": overhead_probe,
                    "initial_fast_full": initial,
                    "incremental_exact": incremental,
                    "incremental": incremental,
                    "fresh_fast_full_after_change": full_rebuild,
                },
                "derived": {
                    "speedup_full_rebuild_over_incremental": speedup,
                    "exact_incremental_marker_seen": explicit_incremental_route,
                    "explicit_incremental_route_seen": explicit_incremental_route,
                    "rank_defer_marker_seen": defer_marker,
                    "passed": passed,
                },
            }
        )
        exit_code = 0 if passed else 1
    except Exception as exc:
        record_report_error(report, exc)
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
