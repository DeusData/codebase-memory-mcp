"""Regression guard for clean Windows MCP stdio startup (#654).

A successful initialize + tools/list exchange must leave nothing on stderr but
the server's own structured log lines. #654 reported the OS "path not found"
message leaking to stderr on startup, in English and on a Spanish Windows. Those
two messages are named cases; any other non-empty line that is not a structured
`level=<name> msg=<event>` record fails as well, so the same leak on a machine
in any other language is caught without listing its translation.

The verdict is read only after the stderr reader has reached EOF. A reader that
is still running after the server exited means the capture may be partial, and
that is a setup failure, never a pass.

Exit codes follow the guard contract of scripts/test-windows.ps1:
0 = GREEN, 1 = RED (regression), 2 = SETUP / precondition.
"""

import os
import re
import subprocess
import sys
import tempfile

from mcp_stdio import McpServer

# The OS path-not-found message as reported in #654 (English and Spanish
# Windows). Named so a RED says exactly which known leak came back.
KNOWN_PATH_ERRORS = (
    "The system cannot find the path specified.",
    "El sistema no puede encontrar la ruta especificada.",
)

# The text-format record cbm_log writes (src/foundation/log.c):
# "level=<debug|info|warn|error> msg=<event> key=value ...".
STRUCTURED_LOG_LINE = re.compile(r"level=[a-z]+ msg=\S")

# Liveness backstop for the stderr reader. The server has already exited when
# this is waited on, so the reader is at EOF unless another process still holds
# the pipe. Expiry is a SETUP failure; it never decides PASS or FAIL.
STDERR_EOF_BACKSTOP_S = 60


def classify_stderr(text):
    """Return (known, raw): the named #654 messages found, and every other
    non-empty stderr line that is not a structured log record."""
    known = [message for message in KNOWN_PATH_ERRORS if message in text]
    raw = []
    for line in text.splitlines():
        line = line.strip()
        if not line or STRUCTURED_LOG_LINE.match(line):
            continue
        if any(message in line for message in KNOWN_PATH_ERRORS):
            continue
        raw.append(line)
    return known, raw


def main():
    if os.name != "nt":
        print("SKIP: Windows-only MCP stdio guard")
        return 2

    binary = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CBM_TEST_BINARY")
    if not binary:
        print("SETUP FAIL: CBM_TEST_BINARY is required", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="cbm-mcp-stdio-") as work:
        cache = os.path.join(work, "cache")
        runtime = os.path.join(work, "runtime")
        os.makedirs(cache)
        os.makedirs(runtime)
        # The verdict reads the text log shape, so select it explicitly: an
        # inherited CBM_LOG_FORMAT=json must not turn every record into a raw
        # line.
        server = McpServer(
            binary,
            cache_dir=cache,
            extra_env={"CBM_RUNTIME_DIR": runtime, "CBM_LOG_FORMAT": "text"},
        )
        try:
            with server:
                server.initialize(timeout=30)
                server.tools_list(timeout=30)
        finally:
            stop = subprocess.run(
                [binary, "daemon", "stop"],
                env=server.env,
                capture_output=True,
                timeout=30,
            )
            if stop.returncode != 0:
                detail = (stop.stdout + stop.stderr).decode("utf-8", "replace")
                raise RuntimeError(
                    "daemon cleanup failed (%d): %s" % (stop.returncode, detail.strip())
                )

    if not server.stderr_complete(STDERR_EOF_BACKSTOP_S):
        print(
            "SETUP FAIL: the stderr reader did not reach EOF after the server "
            "exited; refusing to judge a possibly partial capture",
            file=sys.stderr,
        )
        return 2

    known, raw = classify_stderr(server.stderr_text())
    if known or raw:
        if known:
            print(
                "FAIL: startup leaked an OS path error to stderr: " + ", ".join(known),
                file=sys.stderr,
            )
        if raw:
            print(
                "FAIL: startup wrote %d stderr line(s) that are not structured "
                "level= log records:" % len(raw),
                file=sys.stderr,
            )
            for line in raw:
                print("  " + line, file=sys.stderr)
        return 1
    print("PASS: successful MCP startup emitted only structured log lines on stderr")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
