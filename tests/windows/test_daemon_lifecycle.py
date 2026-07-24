r"""GREEN guard — explicit daemon lifecycle (`daemon start|status|stop`).

Guards the PR #1139 daemon-control surface at the product level:

* ``daemon status`` with no daemon reports not-running and exits nonzero.
* ``daemon start`` launches a PERMANENT daemon: it reports a pid, and the
  daemon survives its clients (a one-shot ``cli`` command recycles it without
  printing the cold-start hint, and the daemon is still active afterwards).
* A cold one-shot ``cli`` command (no daemon) prints the startup-tax hint.
* ``daemon stop`` on an idle daemon stops it; a second ``stop`` is idempotent.

Every path carries a kill-by-pid backstop so a stuck daemon can never hang the
suite (the Windows leg's test-infra hang sensitivity is on record).

Exit code: 0 == lifecycle behaves (green), 1 == regression, 2 == setup error.

Usage:
    python test_daemon_lifecycle.py <path-to-codebase-memory-mcp[.exe]>
"""
import os
import re
import subprocess
import sys
import tempfile


def run_cli(binary, cache, args, timeout=60):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = cache
    return subprocess.run([binary] + args, capture_output=True, timeout=timeout, env=env)


def output_text(result):
    return ((result.stdout or b"") + (result.stderr or b"")).decode("utf-8", "replace")


def force_kill(pid):
    if not pid:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True, timeout=30)
    else:
        subprocess.run(["kill", "-9", str(pid)], capture_output=True, timeout=30)


def main():
    if len(sys.argv) < 2:
        print("usage: python test_daemon_lifecycle.py <binary>")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    work = tempfile.mkdtemp(prefix="cbm_win_daemonctl_")
    cache = os.path.join(work, "cache")
    os.makedirs(cache, exist_ok=True)
    daemon_pid = 0
    try:
        status_absent = run_cli(binary, cache, ["daemon", "status"])
        if status_absent.returncode == 0 or "not running" not in output_text(status_absent):
            print("RED: `daemon status` with no daemon should report not-running "
                  "and exit nonzero:\n%s" % output_text(status_absent)[:300])
            return 1
        print("PASS: status reports not-running before any daemon exists")

        cold = run_cli(binary, cache, ["cli", "list_projects", "{}"])
        cold_text = output_text(cold)
        if cold.returncode != 0 or "daemon start" not in cold_text:
            print("RED: a cold one-shot cli command should succeed and hint at "
                  "`daemon start`:\n%s" % cold_text[:400])
            return 1
        print("PASS: cold cli one-shot succeeded and printed the startup-tax hint")

        start = run_cli(binary, cache, ["daemon", "start"])
        start_text = output_text(start)
        pid_match = re.search(r"pid (\d+)", start_text)
        daemon_pid = int(pid_match.group(1)) if pid_match else 0
        if start.returncode != 0 or "permanent" not in start_text or not daemon_pid:
            print("RED: `daemon start` should report a permanent daemon with a pid:\n%s"
                  % start_text[:400])
            return 1
        print("PASS: daemon start reported permanent pid %d" % daemon_pid)

        warm = run_cli(binary, cache, ["cli", "list_projects", "{}"])
        warm_text = output_text(warm)
        if warm.returncode != 0 or "daemon start" in warm_text:
            print("RED: a warm cli one-shot should recycle the daemon without the "
                  "cold-start hint:\n%s" % warm_text[:400])
            return 1
        status_active = run_cli(binary, cache, ["daemon", "status"])
        status_text = output_text(status_active)
        if status_active.returncode != 0 or "permanent" not in status_text:
            print("RED: the permanent daemon should survive its cli client:\n%s"
                  % status_text[:400])
            return 1
        print("PASS: warm cli recycled the daemon; daemon survived its client")

        stop = run_cli(binary, cache, ["daemon", "stop"])
        if stop.returncode != 0:
            print("RED: `daemon stop` on an idle daemon failed:\n%s"
                  % output_text(stop)[:300])
            return 1
        stop_again = run_cli(binary, cache, ["daemon", "stop"])
        if stop_again.returncode != 0:
            print("RED: a second `daemon stop` should be idempotent:\n%s"
                  % output_text(stop_again)[:300])
            return 1
        print("PASS: stop retired the idle daemon; second stop was idempotent")

        print("\nGREEN: daemon lifecycle (status/start/recycle/stop) behaves.")
        return 0
    finally:
        force_kill(daemon_pid)


if __name__ == "__main__":
    sys.exit(main())
