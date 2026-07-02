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
MCP_INIT_PROTOCOL_VERSION = "2024-11-05"
MATRIX_SCENARIOS_DEFAULT = "go_modify_1,go_modify_2,go_create,go_delete,go_rename,go_new_folder,route_decorator,python_reexport"
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
        PUBLISH_INCREMENTAL_CONTAINMENT,
    }


def is_explicit_incremental_route(publish_kind: str | None, reason: str | None = None) -> bool:
    return is_incremental_publish_kind(publish_kind or "") or bool(reason)


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


def parse_exact_reason(stderr: str) -> str | None:
    prefixes = (
        "msg=incremental.exact.fallback reason=",
        "msg=incremental.exact.delete.fallback reason=",
        "msg=incremental.exact.skip reason=",
    )
    for line in stderr.splitlines():
        for prefix in prefixes:
            if prefix not in line:
                continue
            reason = line.split(prefix, 1)[1].split()[0]
            return reason or None
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
    publish_reason = response_publish_reason(data)
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
        "exact_reason": publish_reason or parse_exact_reason(stderr),
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
    proc, elapsed_ms = command_result(
        [str(binary), "cli", "--json", tool_name, "{}"],
        env,
        timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{tool_name} probe failed: {proc.stderr.strip()}")
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


def canonical_query_rows(db_path: Path, project: str, sql: str) -> list[str]:
    con = sqlite3.connect(str(db_path))
    try:
        rows = [str(row[0]) for row in con.execute(sql, (project,))]
    finally:
        con.close()
    return rows


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


def compare_canonical_graph(left_db: Path, right_db: Path, project: str) -> dict[str, Any]:
    for kind, sql in (
        ("canonical nodes", CANONICAL_NODES_SQL),
        ("canonical edges", CANONICAL_EDGES_SQL),
        ("file hashes", CANONICAL_HASHES_SQL),
    ):
        left = canonical_query_rows(left_db, project, sql)
        right = canonical_query_rows(right_db, project, sql)
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
    shutil.copy2(project_db, incremental_snapshot)
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
    explicit_route = is_explicit_incremental_route(publish_kind, incremental_reason)
    passed = bool(canonical.get("equal")) and explicit_route
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
    return report, exit_code


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
    parser.add_argument("--matrix", action="store_true", help="Run the affected-frontier scenario matrix.")
    parser.add_argument(
        "--matrix-scenarios",
        default=MATRIX_SCENARIOS_DEFAULT,
        help="Comma-separated matrix scenarios to run.",
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
    if args.matrix:
        _, matrix_exit_code = run_matrix(args, binary)
        return matrix_exit_code

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
