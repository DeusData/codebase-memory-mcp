"""Regression guard for clean Windows MCP stdio startup."""

import os
import subprocess
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
        runtime = os.path.join(os.path.dirname(cache), "cbm-mcp-stdio-runtime")
        os.makedirs(runtime)
        server = McpServer(binary, cache_dir=cache,
                           extra_env={"CBM_RUNTIME_DIR": runtime})
        try:
            with server:
                server.initialize(timeout=30)
                server.tools_list(timeout=30)
            stderr = server.stderr_text()
        finally:
            stop = subprocess.run(
                [binary, "daemon", "stop"],
                env=server.env,
                capture_output=True,
                timeout=30,
            )
            if stop.returncode != 0:
                detail = (stop.stdout + stop.stderr).decode("utf-8", "replace")
                raise RuntimeError("daemon cleanup failed (%d): %s" %
                                   (stop.returncode, detail.strip()))

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
