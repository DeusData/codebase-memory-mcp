#!/usr/bin/env python3
"""Run a wrapper smoke command; reserve the outer bound for hang diagnosis."""

import os
import signal
import subprocess
import sys


def run(command, timeout=600):
    process = subprocess.Popen(command, start_new_session=os.name != "nt")
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print("HANG go: wrapper smoke did not complete within the outer bound "
              "(download, verification or execution); terminating its process tree",
              file=sys.stderr, flush=True)
        # Kill descendants too: killing only the wrapper leaves its verifier alive.
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                           check=True, timeout=30)
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.wait(timeout=30)
        return 124


if __name__ == "__main__":
    try:
        result = run(sys.argv[1:])
    except (OSError, subprocess.SubprocessError) as error:
        print(f"wrapper smoke could not run or clean up: {error}", file=sys.stderr)
        sys.exit(125)
    sys.exit(result if result >= 0 else 128 - result)
