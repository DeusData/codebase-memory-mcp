#!/usr/bin/env python3
"""Integration test for CBM_IN_PROCESS (daemon-free, read-only stdio MCP).

Spawns the MCP server binary with CBM_IN_PROCESS=1 and asserts that it

  1. completes an initialize + tools/list handshake,
  2. creates no daemon rendezvous inside CBM_RUNTIME_DIR, and
  3. refuses a mutating tool call, writing no index database.

(2) and (3) are the real assertions. The in-process path exists for hosts whose
sandbox denies socket syscalls outright, so "it answered" is not enough — it must
have answered without ever attempting a rendezvous. A regression that quietly
routes the session back through the coordination daemon still answers on an
ordinary host, and only (2) catches it.

(3) guards the correctness hazard: with no daemon there is no cross-session
mutation lease, and mcp_project_mutation_begin() treats an *unset* guard as
permission, so a missing guard would let an in-process index_repository write the
shared cache while a daemon session mutates the same project. Asserting that no
database appears is what distinguishes a refusing guard from an absent one — an
error message alone could be printed after a partial write.

Both CBM_RUNTIME_DIR and CBM_CACHE_DIR are redirected to fresh temporary
directories, so the test neither joins nor disturbs a developer's running daemon
or indexes.

Usage:
    python3 scripts/test_mcp_in_process.py [/path/to/binary]

Exit codes:
    0 - PASS
    1 - FAIL
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

TIMEOUT_S = 60
DB_SUFFIXES = (".db", ".db-wal", ".db-shm")

MESSAGES = (
    b'{"jsonrpc":"2.0","id":1,"method":"initialize",'
    b'"params":{"protocolVersion":"2025-11-25","capabilities":{}}}\n'
    b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
    b'{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}\n'
    b'{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"index_repository",'
    b'"arguments":{"repo_path":"%s"}}}\n'
)


def fail(message, output=None):
    print(f"FAIL: {message}")
    if output is not None:
        print(f"Server output was:\n{output!r}")
    sys.exit(1)


def files_under(root):
    return sorted(
        os.path.relpath(os.path.join(d, f), root)
        for d, _dirs, fs in os.walk(root)
        for f in fs
    )


def main():
    if len(sys.argv) >= 2:
        binary = sys.argv[1]
    else:
        # Default: look for build artifact relative to this script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(script_dir)
        binary = os.path.join(repo_root, "build", "c", "codebase-memory-mcp")

    if not os.path.isfile(binary):
        fail(f"binary not found at {binary}")

    if not os.access(binary, os.X_OK):
        fail(f"binary not executable: {binary}")

    workdir = tempfile.mkdtemp(prefix="cbm-in-process-")
    runtime_dir = os.path.join(workdir, "runtime")
    cache_dir = os.path.join(workdir, "cache")
    repo_dir = os.path.join(workdir, "repo")
    for d in (runtime_dir, cache_dir, repo_dir):
        os.mkdir(d, 0o700)
    with open(os.path.join(repo_dir, "a.c"), "w") as fh:
        fh.write("int main(void) { return 0; }\n")

    env = dict(os.environ)
    env["CBM_IN_PROCESS"] = "1"
    env["CBM_RUNTIME_DIR"] = runtime_dir
    env["CBM_CACHE_DIR"] = cache_dir

    try:
        proc = subprocess.Popen(
            [binary],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
        )

        try:
            stdout_data, _ = proc.communicate(
                input=MESSAGES % repo_dir.encode(), timeout=TIMEOUT_S
            )
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            fail(
                f"server did not respond within {TIMEOUT_S}s "
                f"(CBM_IN_PROCESS did not bypass the coordination daemon)"
            )

        output = stdout_data.decode("utf-8", errors="replace")

        responses = []
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                responses.append(json.loads(line))
            except json.JSONDecodeError:
                pass

        by_id = {obj["id"]: obj for obj in responses if "id" in obj}

        # 1. Handshake
        if 1 not in by_id:
            fail("missing initialize response (id:1)", output)
        if "result" not in by_id[1]:
            fail(f"initialize returned an error: {by_id[1].get('error')}", output)
        if 2 not in by_id:
            fail("missing tools/list response (id:2)", output)
        if "tools" not in output:
            fail("tools/list response body missing 'tools' key", output)

        # 2. No rendezvous: what distinguishes in-process from daemon-backed.
        strays = files_under(runtime_dir)
        if strays:
            fail(
                "CBM_IN_PROCESS still created a daemon rendezvous in "
                f"CBM_RUNTIME_DIR: {strays}"
            )

        # 3. Mutations refused, and refused BEFORE any write.
        if 3 not in by_id:
            fail("missing index_repository response (id:3)", output)
        call = by_id[3].get("result") or {}
        if not call.get("isError"):
            fail(
                "index_repository was NOT refused in CBM_IN_PROCESS mode — an "
                "unset mutation guard reads as permission, so this would write "
                f"the shared cache with no lease. Response: {by_id[3]}",
            )
        dbs = [f for f in files_under(cache_dir) if f.endswith(DB_SUFFIXES)]
        if dbs:
            fail(
                "index_repository reported an error but still wrote an index "
                f"database under CBM_CACHE_DIR: {dbs}"
            )

        print("PASS")
        sys.exit(0)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
