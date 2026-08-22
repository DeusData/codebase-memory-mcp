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


def run_cli(binary, cache, args, timeout=60, extra_env=None):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = cache
    if extra_env:
        env.update(extra_env)
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
        # Only a UI build configures a UI, and only it can refuse to.
        ui_build = "ui:" in start_text

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
        daemon_pid = 0

        if not ui_build:
            print("SKIP: non-UI binary cannot exercise the UI-configuration refusal")
        else:
            refused_env = {"CBM_TEST_DAEMON_UI_CONFIG_REFUSED": "1"}
            # A refused UI handshake must not be reported as a failed start. The
            # daemon is already up by then, so a bare `daemon start` got exactly
            # what it asked for; only `--port`/`--open` make the UI the point of
            # the command. Before the fix a busy machine — one second of delay
            # after an abrupt shutdown was enough — turned a healthy start into a
            # nonzero exit, which is how this reached CI as a phantom failure.
            refused = run_cli(binary, cache, ["daemon", "start"], extra_env=refused_env)
            refused_text = output_text(refused)
            pid_match = re.search(r"pid (\d+)", refused_text)
            daemon_pid = int(pid_match.group(1)) if pid_match else 0
            if refused.returncode != 0 or "permanent" not in refused_text:
                print("RED: a refused UI configuration must not fail `daemon start`; "
                      "the daemon is already running by then:\n%s" % refused_text[:400])
                return 1
            if "warning" not in refused_text:
                print("RED: a refused UI configuration must still be reported, as a "
                      "warning:\n%s" % refused_text[:400])
                return 1
            alive = run_cli(binary, cache, ["daemon", "status"])
            if alive.returncode != 0 or "permanent" not in output_text(alive):
                print("RED: the daemon that survived a refused UI configuration should "
                      "be reachable:\n%s" % output_text(alive)[:400])
                return 1
            print("PASS: a refused UI configuration warns and leaves the daemon running")

            stopped = run_cli(binary, cache, ["daemon", "stop"])
            if stopped.returncode != 0:
                print("RED: could not retire the daemon started without a UI:\n%s"
                      % output_text(stopped)[:300])
                return 1
            daemon_pid = 0

            # The other half of the contract: when the UI *was* asked for, failing
            # to configure it is still a failed command.
            demanded = run_cli(binary, cache, ["daemon", "start", "--port=45871"],
                               extra_env=refused_env)
            demanded_text = output_text(demanded)
            pid_match = re.search(r"pid (\d+)", demanded_text)
            daemon_pid = int(pid_match.group(1)) if pid_match else 0
            if demanded.returncode == 0:
                print("RED: `daemon start --port=N` must fail when the UI it asked for "
                      "could not be configured:\n%s" % demanded_text[:400])
                return 1
            print("PASS: an explicitly requested UI still fails the command when refused")
            run_cli(binary, cache, ["daemon", "stop"])
            daemon_pid = 0

        print("\nGREEN: daemon lifecycle (status/start/recycle/stop) behaves.")
        return 0
    finally:
        force_kill(daemon_pid)


if __name__ == "__main__":
    sys.exit(main())
