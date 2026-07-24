#!/usr/bin/env python3
"""Bounded interactive MCP smoke scenarios.

Unlike a regular-file fixture, this keeps stdin open while each potentially
long-running request completes. That models a standing coding-agent session
and prevents EOF/session cancellation from racing the response assertions.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from typing import Any, BinaryIO, Optional


INITIALIZE_PARAMS = {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {"name": "interactive-smoke", "version": "1.0"},
}


class SmokeFailure(RuntimeError):
    pass


def mcp_command(binary: str) -> list[str]:
    """Select a direct product launch or an explicit Windows shebang runner."""
    if os.name != "nt":
        return [binary]
    try:
        with open(binary, "rb") as stream:
            shebang = stream.read(2) == b"#!"
    except OSError:
        return [binary]
    if not shebang:
        return [binary]
    bash = shutil.which("bash")
    if not bash:
        raise SmokeFailure("Windows shebang MCP fixture requires bash on PATH")
    return [bash, binary]


def read_json_lines(
    stream: BinaryIO,
    responses: "queue.Queue[dict[str, Any]]",
    transcript: list[dict[str, Any]],
) -> None:
    for raw_line in iter(stream.readline, b""):
        try:
            message = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(message, dict):
            transcript.append(message)
            responses.put(message)


def drain(stream: BinaryIO, chunks: list[bytes]) -> None:
    for chunk in iter(lambda: stream.read(8192), b""):
        chunks.append(chunk)


def cache_log_tail(name: str, limit: int = 16384) -> str:
    cache_dir = os.environ.get("CBM_CACHE_DIR")
    if not cache_dir:
        return ""
    path = os.path.join(cache_dir, "logs", name)
    try:
        with open(path, "rb") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - limit), os.SEEK_SET)
            return stream.read(limit).decode("utf-8", errors="replace")
    except OSError:
        return ""


def send(process: subprocess.Popen[bytes], message: dict[str, Any]) -> None:
    if process.stdin is None:
        raise SmokeFailure("MCP stdin is unavailable")
    encoded = json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"
    try:
        process.stdin.write(encoded)
        process.stdin.flush()
    except (BrokenPipeError, OSError) as error:
        raise SmokeFailure("MCP server closed stdin early") from error


def wait_response(
    process: subprocess.Popen[bytes],
    responses: "queue.Queue[dict[str, Any]]",
    request_id: int,
    timeout: float,
    accept_tool_error: bool = False,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        if process.poll() is not None and responses.empty():
            raise SmokeFailure(
                f"MCP server exited {process.returncode} before response id={request_id}"
            )
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise queue.Empty
            message = responses.get(timeout=remaining)
        except queue.Empty as error:
            raise SmokeFailure(f"timed out waiting for MCP response id={request_id}") from error
        if message.get("id") != request_id:
            continue
        if "result" not in message and "error" not in message:
            # An echoed request is not a JSON-RPC response and must never count
            # as proof that the server dispatched the request.
            continue
        if "error" in message:
            raise SmokeFailure(
                f"MCP response id={request_id} returned JSON-RPC error: {message['error']!r}"
            )
        result = message.get("result")
        if (
            isinstance(result, dict)
            and result.get("isError") is True
            and not accept_tool_error
        ):
            rendered = json.dumps(message, separators=(",", ":"), ensure_ascii=False)
            raise SmokeFailure(
                f"MCP tool response id={request_id} reported isError=true: {rendered}"
            )
        return message


def request(
    process: subprocess.Popen[bytes],
    responses: "queue.Queue[dict[str, Any]]",
    request_id: int,
    method: str,
    params: dict[str, Any],
    timeout: float,
    accept_tool_error: bool = False,
) -> dict[str, Any]:
    send(
        process,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        },
    )
    return wait_response(process, responses, request_id, timeout, accept_tool_error)


def grouped_search_qualified_name(
    structured: Any, expected_name: str
) -> Optional[str]:
    """Extract a qualified name from search_graph's grouped JSON tree."""
    if not isinstance(structured, dict):
        return None
    columns = structured.get("cols")
    groups = structured.get("groups")
    if not isinstance(columns, list) or not isinstance(groups, list):
        return None
    try:
        name_column = columns.index("name")
    except ValueError:
        return None

    for group in groups:
        if not isinstance(group, dict):
            return None
        prefix = group.get("qn_prefix")
        rows = group.get("rows")
        if not isinstance(prefix, str) or not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, list) or name_column >= len(row):
                return None
            name = row[name_column]
            if not isinstance(name, str):
                return None
            if name == expected_name:
                return f"{prefix}.{name}" if prefix else name
    return None


def run_scenario(
    process: subprocess.Popen[bytes],
    responses: "queue.Queue[dict[str, Any]]",
    scenario: str,
    repo_path: str,
    timeout: float,
) -> None:
    request(process, responses, 1, "initialize", INITIALIZE_PARAMS, timeout)
    send(
        process,
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
    )
    if scenario == "initialize":
        return
    if scenario == "invalid-index":
        invalid_index_response = request(
            process,
            responses,
            2,
            "tools/call",
            {
                "name": "index_repository",
                "arguments": {"repo_path": repo_path, "mode": "fast"},
            },
            timeout,
            accept_tool_error=True,
        )
        invalid_index_result = invalid_index_response.get("result")
        if (
            not isinstance(invalid_index_result, dict)
            or invalid_index_result.get("isError") is not True
        ):
            raise SmokeFailure("index_repository unexpectedly accepted a nonexistent path")
        request(process, responses, 3, "ping", {}, timeout)
        return
    index_response = request(
        process,
        responses,
        2,
        "tools/call",
        {
            "name": "index_repository",
            "arguments": {"repo_path": repo_path, "mode": "fast"},
        },
        timeout,
    )
    index_result = index_response.get("result")
    structured = index_result.get("structuredContent") if isinstance(index_result, dict) else None
    project = structured.get("project") if isinstance(structured, dict) else None
    if not isinstance(project, str) or not project:
        raise SmokeFailure("index_repository response did not identify the indexed project")
    if scenario == "roundtrip":
        request(
            process,
            responses,
            3,
            "tools/call",
            {
                "name": "search_graph",
                "arguments": {"project": project, "name_pattern": "compute"},
            },
            timeout,
        )
        return
    request(
        process,
        responses,
        3,
        "tools/call",
        {
            "name": "search_code",
            "arguments": {
                "project": project,
                "pattern": "compute",
                "mode": "compact",
                "limit": 3,
            },
        },
        timeout,
    )
    discovery_response = request(
        process,
        responses,
        4,
        "tools/call",
        {
            "name": "search_graph",
            "arguments": {
                "project": project,
                "name_pattern": "compute",
                "format": "json",
                "limit": 1,
            },
        },
        timeout,
    )
    discovery_result = discovery_response.get("result")
    discovery_structured = (
        discovery_result.get("structuredContent")
        if isinstance(discovery_result, dict)
        else None
    )
    qualified_name = grouped_search_qualified_name(discovery_structured, "compute")
    if not qualified_name:
        raise SmokeFailure("search_graph did not discover compute's qualified name")
    request(
        process,
        responses,
        5,
        "tools/call",
        {
            "name": "get_code_snippet",
            "arguments": {"project": project, "qualified_name": qualified_name},
        },
        timeout,
    )


def stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument(
        "--scenario",
        choices=("initialize", "invalid-index", "roundtrip", "advanced"),
        required=True,
    )
    parser.add_argument("--repo-path", required=True)
    parser.add_argument("--response-timeout", type=float, default=45.0)
    parser.add_argument("--exit-timeout", type=float, default=15.0)
    args = parser.parse_args()

    try:
        command = mcp_command(args.binary)
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
    except (OSError, SmokeFailure) as error:
        print(f"FAIL: could not start MCP server: {error}", file=sys.stderr)
        return 1
    assert process.stdout is not None
    assert process.stderr is not None
    responses: "queue.Queue[dict[str, Any]]" = queue.Queue()
    transcript: list[dict[str, Any]] = []
    stderr_chunks: list[bytes] = []
    stdout_thread = threading.Thread(
        target=read_json_lines,
        args=(process.stdout, responses, transcript),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=drain, args=(process.stderr, stderr_chunks), daemon=True
    )
    stdout_thread.start()
    stderr_thread.start()

    try:
        run_scenario(
            process,
            responses,
            args.scenario,
            args.repo_path,
            args.response_timeout,
        )
        assert process.stdin is not None
        process.stdin.close()
        try:
            return_code = process.wait(timeout=args.exit_timeout)
        except subprocess.TimeoutExpired as error:
            raise SmokeFailure("MCP server did not exit after interactive stdin EOF") from error
        if return_code != 0:
            raise SmokeFailure(f"MCP server exited nonzero after completed session: {return_code}")
        stdout_thread.join(timeout=2)
        stderr_thread.join(timeout=2)
        for message in transcript:
            print(json.dumps(message, separators=(",", ":"), ensure_ascii=False))
        return 0
    except SmokeFailure as error:
        stop_process(process)
        stderr = b"".join(stderr_chunks).decode("utf-8", errors="replace")
        daemon_log = cache_log_tail("cbm-daemon.log")
        conflicts = cache_log_tail("daemon-conflicts.ndjson")
        print(f"FAIL: {error}", file=sys.stderr)
        if stderr:
            print("--- MCP stderr ---", file=sys.stderr)
            print(stderr, file=sys.stderr, end="" if stderr.endswith("\n") else "\n")
        if daemon_log:
            print("--- daemon log (tail) ---", file=sys.stderr)
            print(
                daemon_log,
                file=sys.stderr,
                end="" if daemon_log.endswith("\n") else "\n",
            )
        if conflicts:
            print("--- daemon-conflicts.ndjson (tail) ---", file=sys.stderr)
            print(
                conflicts,
                file=sys.stderr,
                end="" if conflicts.endswith("\n") else "\n",
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
