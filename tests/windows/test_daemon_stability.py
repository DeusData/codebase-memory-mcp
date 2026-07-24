r"""GREEN guard — daemon stability, parameter surface, and failure modes.

Extends the basic lifecycle guard (test_daemon_lifecycle.py) with the angles
that guard the daemon's PRODUCT contract under stress and misuse:

* Parameter surface: bare ``daemon`` and unknown options print usage and fail;
  an out-of-range ``--port=`` value is rejected before anything spawns.
* Hook fail-open without a daemon: ``hook-augment`` exits 0, surfaces the
  visible one-time notice (stderr + Claude systemMessage), stamps the
  cache-scoped rate-limit marker, and stays silent on the next call.
* ``daemon start`` while a permanent daemon is active reports already-active
  with the SAME pid (no second daemon, no restart); ``daemon status`` reports
  the active pid; a ``--port=N`` pointing at an occupied port must not block
  the start (the UI bind is retried in the background by design).
* ``daemon stop`` REFUSES while an MCP session is attached, lists the blocking
  client, and succeeds once the session closes.
* Crash recovery: after a kill -9 the stale daemon state must clear, a cold
  one-shot works again, and a fresh ``daemon start`` yields a NEW pid.
* Churn stability: a permanent daemon must survive sequential and parallel
  one-shot client storms with its pid UNCHANGED (no silent restart) and stay
  responsive afterwards.
* Concurrent cold start: parallel one-shots racing with no daemon must all
  succeed, and the ephemeral daemon they share must retire afterwards.

Every daemon this guard starts carries a kill-by-pid backstop so a stuck
daemon can never hang the suite. Each section runs under its OWN cache
directory: daemon coordination is cache-scoped, so sections are isolated from
each other and from any interactive CBM use on the host.

Exit code: 0 == all sections green, 1 == regression, 2 == setup error.

Usage:
    python test_daemon_stability.py <path-to-codebase-memory-mcp[.exe]>
"""
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

STATUS_POLL_S = 0.5


def run_cli(binary, cache, args, stdin=None, timeout=90):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = cache
    return subprocess.run([binary] + args, capture_output=True, timeout=timeout,
                          env=env, input=stdin)


def out_text(result):
    return ((result.stdout or b"") + (result.stderr or b"")).decode("utf-8", "replace")


def kill_pid(pid):
    if not pid:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True, timeout=30)
    else:
        subprocess.run(["kill", "-9", str(pid)], capture_output=True, timeout=30)


def pid_from(text):
    match = re.search(r"pid[: ]+(\d+)", text)
    return int(match.group(1)) if match else 0


def wait_status_not_running(binary, cache, deadline_s):
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        status = run_cli(binary, cache, ["daemon", "status"], timeout=30)
        if status.returncode != 0 and "not running" in out_text(status):
            return True
        time.sleep(STATUS_POLL_S)
    return False


def read_line_with_timeout(stream, timeout_s):
    box = []

    def _reader():
        try:
            box.append(stream.readline())
        except Exception:
            box.append(b"")

    thread = threading.Thread(target=_reader, daemon=True)
    thread.start()
    thread.join(timeout_s)
    return box[0] if box else None


def section_params(binary, work):
    cache = os.path.join(work, "cache-params")
    os.makedirs(cache, exist_ok=True)
    bare = run_cli(binary, cache, ["daemon"])
    if bare.returncode == 0 or "usage:" not in out_text(bare):
        print("RED: bare `daemon` should print usage and fail:\n%s" % out_text(bare)[:300])
        return False
    unknown = run_cli(binary, cache, ["daemon", "bogus"])
    if unknown.returncode == 0 or "unknown daemon option" not in out_text(unknown):
        print("RED: `daemon bogus` should be rejected:\n%s" % out_text(unknown)[:300])
        return False
    bad_port = run_cli(binary, cache, ["daemon", "start", "--port=0"])
    if bad_port.returncode == 0 or "--port requires" not in out_text(bad_port):
        print("RED: `--port=0` should be rejected before spawning:\n%s"
              % out_text(bad_port)[:300])
        return False
    print("PASS: parameter surface rejects bare/unknown/out-of-range daemon invocations")
    return True


def section_hook_fail_open(binary, work):
    cache = os.path.join(work, "cache-hook")
    os.makedirs(cache, exist_ok=True)
    payload = json.dumps({
        "hook_event_name": "PreToolUse",
        "tool_name": "Grep",
        "cwd": work.replace("\\", "/"),
        "tool_input": {"pattern": "anything"},
    }).encode("utf-8")
    first = run_cli(binary, cache, ["hook-augment"], stdin=payload, timeout=60)
    first_out = (first.stdout or b"").decode("utf-8", "replace")
    first_err = (first.stderr or b"").decode("utf-8", "replace")
    marker = os.path.join(cache, ".hook-daemon-absent-notice")
    if first.returncode != 0:
        print("RED: hook-augment without a daemon must fail OPEN (exit 0), got rc=%d:\n%s"
              % (first.returncode, (first_out + first_err)[:300]))
        return False
    if "systemMessage" not in first_out or "no CBM daemon" not in first_err:
        print("RED: the first absent-daemon hook call must surface the visible notice "
              "(stdout systemMessage + stderr):\nstdout=%r\nstderr=%r"
              % (first_out[:200], first_err[:200]))
        return False
    if not os.path.exists(marker):
        print("RED: the absent-daemon notice did not stamp its rate-limit marker: %s" % marker)
        return False
    second = run_cli(binary, cache, ["hook-augment"], stdin=payload, timeout=60)
    second_out = (second.stdout or b"").decode("utf-8", "replace")
    if second.returncode != 0 or "systemMessage" in second_out:
        print("RED: the second absent-daemon hook call must stay silent (rate-limited), "
              "rc=%d stdout=%r" % (second.returncode, second_out[:200]))
        return False
    print("PASS: hook fails open without a daemon; notice is visible once, then rate-limited")
    return True


def section_start_status_port(binary, work):
    cache = os.path.join(work, "cache-start")
    os.makedirs(cache, exist_ok=True)
    daemon_pid = 0
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        busy_port = blocker.getsockname()[1]
        start = run_cli(binary, cache, ["daemon", "start", "--port=%d" % busy_port], timeout=60)
        start_text = out_text(start)
        daemon_pid = pid_from(start_text)
        if start.returncode != 0 or "permanent" not in start_text or not daemon_pid:
            print("RED: `daemon start --port=<occupied>` must still start the daemon "
                  "(UI bind is non-blocking):\n%s" % start_text[:400])
            return False
        status = run_cli(binary, cache, ["daemon", "status"])
        status_text = out_text(status)
        if status.returncode != 0 or "active (permanent" not in status_text or \
                pid_from(status_text) != daemon_pid:
            print("RED: status must report the active permanent daemon pid %d:\n%s"
                  % (daemon_pid, status_text[:400]))
            return False
        again = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        again_text = out_text(again)
        if again.returncode != 0 or "already active (permanent" not in again_text or \
                pid_from(again_text) != daemon_pid:
            print("RED: a second `daemon start` must report already-active with the SAME "
                  "pid %d:\n%s" % (daemon_pid, again_text[:400]))
            return False
        stop = run_cli(binary, cache, ["daemon", "stop"])
        if stop.returncode != 0 or not wait_status_not_running(binary, cache, 45):
            print("RED: the daemon did not stop cleanly after the start/status checks")
            return False
        daemon_pid = 0
        print("PASS: start survives an occupied --port, status reports the pid, "
              "second start is a no-op on the same daemon")
        return True
    finally:
        blocker.close()
        kill_pid(daemon_pid)


def section_stop_refuses_busy(binary, work):
    cache = os.path.join(work, "cache-busy")
    os.makedirs(cache, exist_ok=True)
    daemon_pid = 0
    session = None
    try:
        start = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        daemon_pid = pid_from(out_text(start))
        if start.returncode != 0 or not daemon_pid:
            print("SETUP FAIL: permanent daemon did not start for the busy-stop check")
            return False
        env = dict(os.environ)
        env["CBM_CACHE_DIR"] = cache
        session = subprocess.Popen([binary], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                   env=env)
        session.stdin.write(b'{"jsonrpc":"2.0","id":0,"method":"initialize",'
                            b'"params":{"capabilities":{}}}\n')
        session.stdin.flush()
        reply = read_line_with_timeout(session.stdout, 45)
        if not reply or b'"result"' not in reply:
            print("SETUP FAIL: MCP session did not complete initialize: %r"
                  % (reply[:200] if reply else reply))
            return False
        busy = run_cli(binary, cache, ["daemon", "stop"])
        busy_text = out_text(busy)
        listed_pids = re.findall(r"- pid (\d+)", busy_text)
        # The daemon lists its authenticated peer process. On Windows the MCP
        # frontend's daemon client is an internal child of the spawned .exe, so
        # the listed pid need not equal the Popen pid (the same process-boundary
        # caveat the soak's idle-CPU check documents); POSIX peers match exactly.
        pids_ok = bool(listed_pids) if os.name == "nt" else str(session.pid) in listed_pids
        if busy.returncode == 0 or "NOT stopped" not in busy_text or not pids_ok:
            print("RED: `daemon stop` with an attached MCP session (pid %d) must refuse "
                  "and list the blocking client:\n%s" % (session.pid, busy_text[:400]))
            return False
        session.stdin.close()
        session.wait(timeout=45)
        deadline = time.monotonic() + 45
        stopped = False
        while time.monotonic() < deadline:
            retry = run_cli(binary, cache, ["daemon", "stop"])
            if retry.returncode == 0:
                stopped = True
                break
            time.sleep(STATUS_POLL_S)
        if not stopped or not wait_status_not_running(binary, cache, 45):
            print("RED: `daemon stop` did not succeed after the blocking session closed")
            return False
        daemon_pid = 0
        print("PASS: stop refuses while a session is attached (listing its pid) and "
              "succeeds once the session closes")
        return True
    finally:
        if session and session.poll() is None:
            session.kill()
        kill_pid(daemon_pid)


def section_crash_recovery(binary, work):
    cache = os.path.join(work, "cache-crash")
    os.makedirs(cache, exist_ok=True)
    daemon_pid = 0
    second_pid = 0
    try:
        start = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        daemon_pid = pid_from(out_text(start))
        if start.returncode != 0 or not daemon_pid:
            print("SETUP FAIL: permanent daemon did not start for the crash check")
            return False
        kill_pid(daemon_pid)
        if not wait_status_not_running(binary, cache, 60):
            print("RED: after kill -9 of pid %d the stale daemon state never cleared "
                  "(`daemon status` kept reporting it)" % daemon_pid)
            return False
        cold = run_cli(binary, cache, ["cli", "list_projects", "{}"], timeout=90)
        if cold.returncode != 0 or "daemon start" not in out_text(cold):
            print("RED: a cold one-shot after the daemon crash should succeed with the "
                  "startup-tax hint:\n%s" % out_text(cold)[:400])
            return False
        restart = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        second_pid = pid_from(out_text(restart))
        if restart.returncode != 0 or not second_pid or second_pid == daemon_pid:
            print("RED: `daemon start` after the crash must launch a FRESH daemon:\n%s"
                  % out_text(restart)[:400])
            return False
        daemon_pid = 0
        stop = run_cli(binary, cache, ["daemon", "stop"])
        if stop.returncode != 0 or not wait_status_not_running(binary, cache, 45):
            print("RED: the recovered daemon did not stop cleanly")
            return False
        second_pid = 0
        print("PASS: kill -9 recovery — stale state cleared, cold client worked, fresh "
              "start got a new pid")
        return True
    finally:
        kill_pid(daemon_pid)
        kill_pid(second_pid)


def _parallel_one_shots(binary, cache, count):
    results = [None] * count

    def _one(index):
        results[index] = run_cli(binary, cache, ["cli", "list_projects", "{}"], timeout=120)

    threads = [threading.Thread(target=_one, args=(i,)) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(150)
    return results


def section_churn_stability(binary, work):
    cache = os.path.join(work, "cache-churn")
    os.makedirs(cache, exist_ok=True)
    daemon_pid = 0
    try:
        start = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        daemon_pid = pid_from(out_text(start))
        if start.returncode != 0 or not daemon_pid:
            print("SETUP FAIL: permanent daemon did not start for the churn check")
            return False
        for round_index in range(10):
            one = run_cli(binary, cache, ["cli", "list_projects", "{}"], timeout=90)
            if one.returncode != 0:
                print("RED: sequential churn one-shot %d failed:\n%s"
                      % (round_index, out_text(one)[:300]))
                return False
        for wave in range(2):
            results = _parallel_one_shots(binary, cache, 6)
            for index, result in enumerate(results):
                if result is None or result.returncode != 0:
                    print("RED: parallel churn wave %d client %d failed (rc=%s):\n"
                          "stdout=%r\nstderr=%r"
                          % (wave, index,
                             result.returncode if result else "none",
                             (result.stdout or b"")[-300:] if result else b"",
                             (result.stderr or b"")[-400:] if result else b""))
                    return False
        status = run_cli(binary, cache, ["daemon", "status"])
        status_text = out_text(status)
        if status.returncode != 0 or pid_from(status_text) != daemon_pid:
            print("RED: after the churn the daemon must still be the SAME process "
                  "(expected pid %d):\n%s" % (daemon_pid, status_text[:400]))
            return False
        stop = run_cli(binary, cache, ["daemon", "stop"])
        if stop.returncode != 0 or not wait_status_not_running(binary, cache, 45):
            print("RED: the churned daemon did not stop cleanly")
            return False
        daemon_pid = 0
        print("PASS: permanent daemon survived 10 sequential + 2x6 parallel clients "
              "with an unchanged pid")
        return True
    finally:
        kill_pid(daemon_pid)


def section_cold_storm(binary, work):
    cache = os.path.join(work, "cache-storm")
    os.makedirs(cache, exist_ok=True)
    results = _parallel_one_shots(binary, cache, 6)
    for index, result in enumerate(results):
        if result is None or result.returncode != 0:
            print("RED: cold-storm client %d failed (racing daemon spawn):\n%s"
                  % (index, out_text(result)[:300] if result else "(no result)"))
            return False
    if not wait_status_not_running(binary, cache, 90):
        print("RED: the ephemeral daemon shared by the cold storm never retired")
        return False
    print("PASS: 6 racing cold clients all succeeded and the shared ephemeral daemon retired")
    return True


def main():
    if len(sys.argv) < 2:
        print("usage: python test_daemon_stability.py <binary>")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    work = tempfile.mkdtemp(prefix="cbm_daemon_stab_")
    sections = [
        section_params,
        section_hook_fail_open,
        section_start_status_port,
        section_stop_refuses_busy,
        section_crash_recovery,
        section_churn_stability,
        section_cold_storm,
    ]
    try:
        for section in sections:
            if not section(binary, work):
                print("\nRED (tests/windows/test_daemon_stability.py): %s failed"
                      % section.__name__)
                return 1
        print("\nGREEN: daemon stability, parameters, and failure modes behave.")
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
