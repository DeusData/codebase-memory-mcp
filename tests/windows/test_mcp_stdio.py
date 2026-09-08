"""Regression guard for clean Windows MCP stdio startup."""

import os
import sys
import tempfile

from mcp_stdio import McpServer


def main():
    if os.name != "nt":
        print("SKIP: Windows-only MCP stdio guard")
        return 2

    binary = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CBM_TEST_BINARY")
    if not binary:
        print("SETUP FAIL: CBM_TEST_BINARY is required", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="cbm-mcp-stdio-") as cache:
        with McpServer(binary, cache_dir=cache) as server:
            server.initialize(timeout=30)
            server.tools_list(timeout=30)
            server.close()
            stderr = server.stderr_text()

    forbidden = (
        "The system cannot find the path specified.",
        "El sistema no puede encontrar la ruta especificada.",
    )
    leaked = [message for message in forbidden if message in stderr]
    if leaked:
        print("FAIL: startup leaked an OS path error to stderr: " + ", ".join(leaked),
              file=sys.stderr)
        return 1
    print("PASS: successful MCP startup emitted no OS path error")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
