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
directory AND its own ``CBM_RUNTIME_DIR``; the second one is what isolates it.
Daemon coordination is NOT cache-scoped: the rendezvous key hashes a
compile-time constant under a machine-global parent (the real
``CSIDL_LOCAL_APPDATA`` -- the ``LOCALAPPDATA`` variable is ignored), so
without the override every section, every sibling guard on the runner and
any interactive CBM use on the host share ONE daemon, and a daemon wedged in
one place fails every check that comes after it. ``CBM_RUNTIME_DIR`` is the
only relocation hook; the daemon inherits it from the client that spawns it,
and the directory must exist before the binary runs because the endpoint
code validates the parent and never creates it.

Exit code: 0 == all sections green; 1 == regression (every failing section is
named -- sections are isolated, so a red one no longer poisons the rest);
2 == setup error: a precondition the lifecycle guard owns -- ``daemon start``
itself -- did not hold. The daemon's FULL output is printed either way, so the
board shows why instead of a 300-character allocator preamble.

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
# Failure excerpts show the TAIL of the output: the allocator/runtime preamble
# comes first, and a fixed head slice used to hide the actual error.
EXCERPT_CHARS = 1500
# Return code reported for a client that hit its subprocess timeout.
TIMEOUT_RC = 124


class SetupFailure(Exception):
    """A precondition this guard does not own did not hold.

    Only ``daemon start`` failures raise this: test_daemon_lifecycle.py runs
    first and asserts that surface as a regression, so reporting it here as
    exit 2 (precondition) hides nothing from the board.  Everything the guard
    owns -- including an MCP session failing to initialize against a daemon
    that DID start -- stays a regression (exit 1).
    """


def runtime_for(cache):
    """The section's private daemon rendezvous parent, a sibling of its cache."""
    head, tail = os.path.split(cache)
    return os.path.join(head, tail.replace("cache-", "runtime-", 1))


def section_dirs(work, name):
    cache = os.path.join(work, "cache-" + name)
    os.makedirs(cache, exist_ok=True)
    # Must exist before the binary runs: the endpoint validates the parent
    # (exists, is a directory, not a reparse point) and never creates it.
    os.makedirs(runtime_for(cache), exist_ok=True)
    return cache


def cli_env(cache):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = cache
    env["CBM_RUNTIME_DIR"] = runtime_for(cache)
    return env


def run_cli(binary, cache, args, stdin=None, timeout=90):
    return subprocess.run([binary] + args, capture_output=True, timeout=timeout,
                          env=cli_env(cache), input=stdin)


def out_text(result):
    return ((result.stdout or b"") + (result.stderr or b"")).decode("utf-8", "replace")


def excerpt(text, limit=EXCERPT_CHARS):
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    if len(text) <= limit:
        return text
    return "...(%d chars elided)...\n%s" % (len(text) - limit, text[-limit:])


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
    cache = section_dirs(work, "params")
    bare = run_cli(binary, cache, ["daemon"])
    if bare.returncode == 0 or "usage:" not in out_text(bare):
        print("RED: bare `daemon` should print usage and fail:\n%s" % excerpt(out_text(bare)))
        return False
    unknown = run_cli(binary, cache, ["daemon", "bogus"])
    if unknown.returncode == 0 or "unknown daemon option" not in out_text(unknown):
        print("RED: `daemon bogus` should be rejected:\n%s" % excerpt(out_text(unknown)))
        return False
    bad_port = run_cli(binary, cache, ["daemon", "start", "--port=0"])
    if bad_port.returncode == 0 or "--port requires" not in out_text(bad_port):
        print("RED: `--port=0` should be rejected before spawning:\n%s"
              % excerpt(out_text(bad_port)))
        return False
    print("PASS: parameter surface rejects bare/unknown/out-of-range daemon invocations")
    return True


def section_hook_fail_open(binary, work):
    cache = section_dirs(work, "hook")
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
              % (first.returncode, excerpt(first_out + first_err)))
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
    cache = section_dirs(work, "start")
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
                  "(UI bind is non-blocking):\n%s" % excerpt(start_text))
            return False
        status = run_cli(binary, cache, ["daemon", "status"])
        status_text = out_text(status)
        if status.returncode != 0 or "active (permanent" not in status_text or \
                pid_from(status_text) != daemon_pid:
            print("RED: status must report the active permanent daemon pid %d:\n%s"
                  % (daemon_pid, excerpt(status_text)))
            return False
        again = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        again_text = out_text(again)
        if again.returncode != 0 or "already active (permanent" not in again_text or \
                pid_from(again_text) != daemon_pid:
            print("RED: a second `daemon start` must report already-active with the SAME "
                  "pid %d:\n%s" % (daemon_pid, excerpt(again_text)))
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
    cache = section_dirs(work, "busy")
    daemon_pid = 0
    session = None
    session_err = None
    try:
        start = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        daemon_pid = pid_from(out_text(start))
        if start.returncode != 0 or not daemon_pid:
            raise SetupFailure("permanent daemon did not start for the busy-stop check "
                               "(rc=%s):\n%s" % (start.returncode, out_text(start)))
        # The session's stderr goes to a file, not DEVNULL: when initialize
        # never answers, it is the only trace of what the frontend saw.
        session_err_path = os.path.join(work, "busy-session.stderr")
        session_err = open(session_err_path, "wb")
        session = subprocess.Popen([binary], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=session_err,
                                   env=cli_env(cache))
        session.stdin.write(b'{"jsonrpc":"2.0","id":0,"method":"initialize",'
                            b'"params":{"capabilities":{}}}\n')
        session.stdin.flush()
        reply = read_line_with_timeout(session.stdout, 45)
        if not reply or b'"result"' not in reply:
            session.kill()
            session.wait(timeout=30)
            session_err.close()
            with open(session_err_path, "rb") as handle:
                session_stderr = handle.read()
            print("RED: MCP session did not complete initialize against a running "
                  "daemon (pid %d); reply=%r\nsession stderr:\n%s"
                  % (daemon_pid, reply, excerpt(session_stderr)))
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
                  "and list the blocking client:\n%s" % (session.pid, excerpt(busy_text)))
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
        if session_err:
            session_err.close()
        kill_pid(daemon_pid)


def section_crash_recovery(binary, work):
    cache = section_dirs(work, "crash")
    daemon_pid = 0
    second_pid = 0
    try:
        start = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        daemon_pid = pid_from(out_text(start))
        if start.returncode != 0 or not daemon_pid:
            raise SetupFailure("permanent daemon did not start for the crash check "
                               "(rc=%s):\n%s" % (start.returncode, out_text(start)))
        kill_pid(daemon_pid)
        if not wait_status_not_running(binary, cache, 60):
            print("RED: after kill -9 of pid %d the stale daemon state never cleared "
                  "(`daemon status` kept reporting it)" % daemon_pid)
            return False
        cold = run_cli(binary, cache, ["cli", "list_projects", "{}"], timeout=90)
        if cold.returncode != 0 or "daemon start" in out_text(cold):
            print("RED: a cold one-shot after the daemon crash should succeed without "
                  "default startup chatter:\n%s" % excerpt(out_text(cold)))
            return False
        restart = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        second_pid = pid_from(out_text(restart))
        if restart.returncode != 0 or not second_pid or second_pid == daemon_pid:
            print("RED: `daemon start` after the crash must launch a FRESH daemon:\n%s"
                  % excerpt(out_text(restart)))
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
        try:
            results[index] = run_cli(binary, cache, ["cli", "list_projects", "{}"],
                                     timeout=120)
        except subprocess.TimeoutExpired as exc:
            # Keep the hang as a failed result WITH its partial output; raised
            # on the worker thread it was lost, and the client reported as
            # "rc=none" with nothing to read.
            results[index] = subprocess.CompletedProcess(
                exc.cmd, TIMEOUT_RC, exc.stdout or b"",
                (exc.stderr or b"") + ("\n[timed out after %ss]" % exc.timeout).encode())

    threads = [threading.Thread(target=_one, args=(i,)) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(150)
    return results


def section_churn_stability(binary, work):
    cache = section_dirs(work, "churn")
    daemon_pid = 0
    try:
        start = run_cli(binary, cache, ["daemon", "start"], timeout=60)
        daemon_pid = pid_from(out_text(start))
        if start.returncode != 0 or not daemon_pid:
            raise SetupFailure("permanent daemon did not start for the churn check "
                               "(rc=%s):\n%s" % (start.returncode, out_text(start)))
        for round_index in range(10):
            one = run_cli(binary, cache, ["cli", "list_projects", "{}"], timeout=90)
            if one.returncode != 0:
                print("RED: sequential churn one-shot %d failed:\n%s"
                      % (round_index, excerpt(out_text(one))))
                return False
        for wave in range(2):
            results = _parallel_one_shots(binary, cache, 6)
            for index, result in enumerate(results):
                if result is None or result.returncode != 0:
                    print("RED: parallel churn wave %d client %d failed (rc=%s):\n%s"
                          % (wave, index,
                             result.returncode if result else "none (still running)",
                             excerpt(out_text(result)) if result else "(no result)"))
                    return False
        status = run_cli(binary, cache, ["daemon", "status"])
        status_text = out_text(status)
        if status.returncode != 0 or pid_from(status_text) != daemon_pid:
            print("RED: after the churn the daemon must still be the SAME process "
                  "(expected pid %d):\n%s" % (daemon_pid, excerpt(status_text)))
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
    cache = section_dirs(work, "storm")
    results = _parallel_one_shots(binary, cache, 6)
    for index, result in enumerate(results):
        if result is None or result.returncode != 0:
            print("RED: cold-storm client %d failed (racing daemon spawn):\n%s"
                  % (index, excerpt(out_text(result)) if result else "(no result)"))
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

    # POSIX is for local iteration only, and there the rendezvous is a Unix
    # socket whose path is capped at 104 bytes on macOS (sun_path); the default
    # TMPDIR is already ~50 bytes deep, so keep the whole namespace under /tmp.
    # On Windows TEMP is the guard root the runner hardened; leave it alone.
    work = tempfile.mkdtemp(prefix="cbm_daemon_stab_",
                            dir=None if os.name == "nt" else "/tmp")
    sections = [
        section_params,
        section_hook_fail_open,
        section_start_status_port,
        section_stop_refuses_busy,
        section_crash_recovery,
        section_churn_stability,
        section_cold_storm,
    ]
    failed = []
    setup_failed = []
    try:
        for section in sections:
            try:
                ok = section(binary, work)
            except SetupFailure as exc:
                print("\nSETUP FAIL (%s): %s" % (section.__name__, exc))
                setup_failed.append(section.__name__)
                continue
            except subprocess.TimeoutExpired as exc:
                # The section's own finally already fired its kill backstop.
                print("\nRED: %s hung -- %s timed out after %ss:\n%s"
                      % (section.__name__, exc.cmd, exc.timeout,
                         excerpt((exc.stdout or b"") + (exc.stderr or b""))))
                ok = False
            if not ok:
                failed.append(section.__name__)
        if failed:
            print("\nRED (tests/windows/test_daemon_stability.py): %s failed"
                  % ", ".join(failed))
            return 1
        if setup_failed:
            print("\nSETUP FAIL (tests/windows/test_daemon_stability.py): %s could not "
                  "run -- `daemon start` did not hold; the lifecycle guard owns that red"
                  % ", ".join(setup_failed))
            return 2
        print("\nGREEN: daemon stability, parameters, and failure modes behave.")
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
