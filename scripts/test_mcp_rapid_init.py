#!/usr/bin/env python3
"""Integration test for the poll/getline FILE* buffering fix.

Spawns the MCP server binary, sends initialize + notifications/initialized +
tools/list all at once (no delays), and asserts that the tools/list response
arrives within 5 seconds.

Usage:
    python3 scripts/test_mcp_rapid_init.py [/path/to/binary]

Exit codes:
    0 - PASS
    1 - FAIL
"""

import subprocess
import sys
import os

TIMEOUT_S = 5

MESSAGES = (
    b'{"jsonrpc":"2.0","id":1,"method":"initialize",'
    b'"params":{"protocolVersion":"2025-11-25","capabilities":{}}}\n'
    b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
    b'{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}\n'
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
        print(f"FAIL: binary not found at {binary}")
        sys.exit(1)

    if not os.access(binary, os.X_OK):
        print(f"FAIL: binary not executable: {binary}")
        sys.exit(1)

    proc = subprocess.Popen(
        [binary],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

    try:
        # Write all 3 messages in one call and close stdin to signal EOF
        stdout_data, _ = proc.communicate(input=MESSAGES, timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        print(
            f"FAIL: server did not respond within {TIMEOUT_S}s "
            f"(poll/getline buffering bug not fixed)"
        )
        sys.exit(1)

    output = stdout_data.decode("utf-8", errors="replace")

    # Expect exactly 2 JSON responses: id:1 (initialize) and id:2 (tools/list).
    # notifications/initialized has no id and produces no response.
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    import json as _json
    json_lines = []
    for ln in lines:
        try:
            json_lines.append(_json.loads(ln))
        except _json.JSONDecodeError:
            pass

    ids = {obj.get("id") for obj in json_lines if "id" in obj}
    if 1 not in ids:
        print("FAIL: missing initialize response (id:1) in server output")
        print(f"Server output was:\n{output!r}")
        sys.exit(1)
    if 2 not in ids:
        print("FAIL: missing tools/list response (id:2) in server output")
        print(f"Server output was:\n{output!r}")
        sys.exit(1)
    if "tools" not in output:
        print("FAIL: tools/list response body missing 'tools' key")
        print(f"Server output was:\n{output!r}")
        sys.exit(1)

    print("PASS")
    sys.exit(0)


if __name__ == "__main__":
    main()
