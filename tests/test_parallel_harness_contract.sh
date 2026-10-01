#!/usr/bin/env bash
# Regression contract for the cross-platform parallel suite scheduler.
#
# On real Windows/MSYS2, a native suite completed and printed its green
# summary, but the exported `run_one` function's nested `bash -c` worker never
# returned to xargs or appended its result.  The native process was gone; the
# orphaned MSYS shell could not be terminated, so the whole gate waited
# forever.  Keep process ownership in one Python parent and forbid that nested
# shell-worker shape from returning.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
driver="$ROOT/scripts/run-tests-parallel.sh"
scheduler="$ROOT/scripts/run-test-wave.py"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/cbm-parallel-harness.XXXXXX")"
trap 'rm -rf -- "$fixture"' EXIT

if grep -Eq '(^|[[:space:]])xargs([[:space:]]|$)|export[[:space:]]+-f|bash[[:space:]]+-c' \
    "$driver"; then
    echo "FAIL: parallel harness must not put native runners behind nested MSYS bash workers" >&2
    exit 1
fi
if [ ! -f "$scheduler" ]; then
    echo "FAIL: parent-owned parallel scheduler is missing: $scheduler" >&2
    exit 1
fi
if ! grep -Fq 'run-test-wave.py' "$driver"; then
    echo "FAIL: parallel harness is not wired to the parent-owned scheduler" >&2
    exit 1
fi

# The Windows descendant proof must not be timed by --kill-grace. That argument
# bounds how long a *process* may resist termination (this file runs the
# scheduler with 1s); the probe is a cold PowerShell + CIM start that routinely
# costs seconds on a runner. Binding one to the other made the verdict a
# function of interpreter latency: a slow start became "assume the worst" and
# reddened an already-clean shard. Asserted structurally -- no sleeps, no timing
# thresholds -- so the contract stays deterministic on every platform.
# (Command substitution, not `| grep -q`: under pipefail an early-exiting
# reader can hand the writer EPIPE and turn a satisfied match into status 141.)
probe_sites=$(grep -n 'windows_tree_cleanup_blocker(' "$scheduler" || true)
if [[ "$probe_sites" == *kill_grace* ]]; then
    echo "FAIL: the Windows descendant probe is still timed by --kill-grace" >&2
    exit 1
fi
# Same defect one call earlier (#2345): taskkill /F cannot be resisted, so its
# runtime is taskkill.exe's own cold start, and bounding it with --kill-grace
# reported a slow start as a failed cleanup of an ordinary hung suite. One
# taskkill site, owned by a helper that takes no kill-grace at all.
taskkill_sites=$(grep -c '"taskkill.exe"' "$scheduler" || true)
if [ "$taskkill_sites" != 1 ]; then
    echo "FAIL: the scheduler must run taskkill.exe from exactly one site (found $taskkill_sites)" >&2
    exit 1
fi
kill_sites=$(grep -n 'windows_taskkill_tree(' "$scheduler" || true)
if [ -z "$kill_sites" ] || [[ "$kill_sites" == *kill_grace* ]]; then
    echo "FAIL: taskkill is not run through windows_taskkill_tree(), or is still timed by --kill-grace" >&2
    exit 1
fi

# Barrier files must be published atomically. Path.write_text creates and
# truncates before it writes, so a poller that saw `<suite>.ready` appear and
# then parsed the leader pid could read the zero-byte window and fail on
# int("") -- a scheduler-side race surfacing as a harness flake. Asserted
# structurally: no barrier file is written in place, and the scheduler renames
# a same-directory temp file onto the destination instead.
barrier_writes=$(grep -nE '^[[:space:]]*(ready|leader_exited)\.write_text\(' "$scheduler" || true)
if [ -n "$barrier_writes" ]; then
    echo "FAIL: scheduler barrier files are written in place (non-atomic):" >&2
    echo "$barrier_writes" >&2
    exit 1
fi
if ! grep -Fq 'os.replace(' "$scheduler"; then
    echo "FAIL: scheduler does not rename barrier files into place" >&2
    exit 1
fi

python3 - "$scheduler" <<'PROBE'
from __future__ import annotations

import importlib.util
import subprocess
import sys


spec = importlib.util.spec_from_file_location("cbm_run_test_wave", sys.argv[1])
module = importlib.util.module_from_spec(spec)
# @dataclass resolves its own module out of sys.modules; register before exec.
sys.modules[spec.name] = module
spec.loader.exec_module(module)

budget = getattr(module, "WINDOWS_DESCENDANT_PROBE_SECONDS", None)
if not isinstance(budget, int) or budget < 15:
    raise SystemExit(
        "FAIL: the descendant probe has no independent budget "
        f"(WINDOWS_DESCENDANT_PROBE_SECONDS={budget!r})"
    )

probe = module.windows_tree_cleanup_blocker
original_run = subprocess.run
observed: list[object] = []


def timing_out(*args: object, **kwargs: object) -> object:
    observed.append(kwargs.get("timeout"))
    raise subprocess.TimeoutExpired(cmd="probe", timeout=kwargs.get("timeout"))


class _Completed:
    def __init__(self, stdout: str, returncode: int = 0) -> None:
        self.returncode = returncode
        self.stdout = stdout


kill_budget = getattr(module, "WINDOWS_TASKKILL_SECONDS", None)
if not isinstance(kill_budget, int) or kill_budget < 15:
    raise SystemExit(
        "FAIL: taskkill has no independent budget "
        f"(WINDOWS_TASKKILL_SECONDS={kill_budget!r})"
    )
taskkill = module.windows_taskkill_tree
kill_observed: list[object] = []


def taskkill_timing_out(*args: object, **kwargs: object) -> object:
    kill_observed.append(kwargs.get("timeout"))
    raise subprocess.TimeoutExpired(cmd="taskkill", timeout=kwargs.get("timeout"))


def cannot_start(*args: object, **kwargs: object) -> object:
    raise OSError("taskkill.exe cannot start")


try:
    subprocess.run = timing_out
    timed_out_reason = probe(4321)
    subprocess.run = lambda *a, **k: _Completed("3\n")
    live_reason = probe(4321)
    subprocess.run = lambda *a, **k: _Completed("0\n")
    clean_reason = probe(4321)
    subprocess.run = taskkill_timing_out
    kill_timed_out = taskkill(4321)
    subprocess.run = lambda *a, **k: _Completed("", 0)
    kill_succeeded = taskkill(4321)
    subprocess.run = lambda *a, **k: _Completed("", 128)
    kill_failed = taskkill(4321)
    subprocess.run = cannot_start
    kill_unstartable = taskkill(4321)
finally:
    subprocess.run = original_run

if kill_observed != [kill_budget]:
    raise SystemExit(
        f"FAIL: taskkill is not bounded by its own budget (timeouts={kill_observed})"
    )
if kill_succeeded is not True or (kill_timed_out, kill_failed, kill_unstartable) != (
    False,
    False,
    False,
):
    raise SystemExit(
        "FAIL: taskkill stopped failing closed "
        f"(ok={kill_succeeded!r}, timed_out={kill_timed_out!r}, "
        f"failed={kill_failed!r}, unstartable={kill_unstartable!r})"
    )

if observed != [budget] * len(observed):
    raise SystemExit(
        "FAIL: the descendant probe is not bounded by its own budget "
        f"(timeouts={observed})"
    )
if len(observed) < 2:
    raise SystemExit(
        "FAIL: the descendant probe does not retry a timed-out probe "
        f"(attempts={len(observed)})"
    )
if timed_out_reason is None or live_reason is None:
    raise SystemExit(
        "FAIL: the descendant probe stopped failing closed "
        f"(timed_out={timed_out_reason!r}, live={live_reason!r})"
    )
if clean_reason is not None:
    raise SystemExit(f"FAIL: a clean tree was not proven clean ({clean_reason!r})")
if "could not complete" not in timed_out_reason:
    raise SystemExit(
        f"FAIL: an unfinished probe is not named as one ({timed_out_reason!r})"
    )
if "3 live descendant" not in live_reason:
    raise SystemExit(
        f"FAIL: proven descendants are not reported with their count ({live_reason!r})"
    )
if timed_out_reason == live_reason:
    raise SystemExit(
        "FAIL: an unfinished probe and a leaked tree are reported identically"
    )
PROBE

mkdir "$fixture/barrier"

# The fixture suites. Nothing here ends on a clock: a process that must outlive
# the scheduler's decision is HELD on the gate -- the read end of a pipe whose
# only writer is held_wave.py, handed down as stdin through the scheduler -- and
# returns only at EOF, when the contract has recorded its verdict and closed
# the write end (or has itself died, so nothing is left orphaned). The previous
# `time.sleep(30)` raced that lifetime against the scheduler: on a slow Windows
# runner the leader-exit refusal took 38s, and the "surviving" descendant had
# already exited on its own before it could be observed (#2394).
cat >"$fixture/fake_runner.py" <<'PY'
from __future__ import annotations

import os
import pathlib
import subprocess
import sys


DESCENDANT = "\n".join(
    (
        "import os, signal, sys",
        "if os.name != 'nt':",
        "    signal.signal(signal.SIGTERM, signal.SIG_IGN)",
        "print('armed', flush=True)",
        "sys.stdin.buffer.read()",
    )
)
state = pathlib.Path(sys.argv[1])
suite = sys.argv[-1]


def publish(name: str, text: str) -> None:
    # Rename into place: a reader sees no file or all of it, never the empty
    # window between create and write that once failed a run with int('').
    temporary = state / f".{name}.{os.getpid()}.tmp"
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, state / name)


def hold() -> None:
    sys.stdin.buffer.read()


if suite == "hang_after_summary":
    print("  1 passed", flush=True)
    publish(f"{suite}.established", "none\n")
    hold()
elif suite in ("stubborn_tree", "timeout_exit_race"):
    child = subprocess.Popen(
        [sys.executable, "-c", DESCENDANT],
        stdin=0,
        stdout=subprocess.PIPE,
        text=True,
    )
    # Blocks until the descendant has installed its SIGTERM handler, so the
    # POSIX legs reach the SIGKILL escalation instead of racing it.
    armed = child.stdout.readline().strip() == "armed"
    print("  1 passed", flush=True)
    publish(f"{suite}.established", f"{child.pid}\n" if armed else "unarmed\n")
    hold()
elif suite == "no_summary":
    pass
else:
    print("  1 passed", flush=True)
PY

# Drives one scheduler wave whose hanging suite is parked at its timeout by the
# pre-terminate barrier and released only once the fixture has ESTABLISHED
# everything the assertions read: summary printed, descendant spawned and
# armed, its pid published. The scheduler's 1s timeout is a wall clock, so
# releasing on `ready` alone let it kill a slow leader before any of that
# existed.
cat >"$fixture/held_wave.py" <<'PY'
from __future__ import annotations

import ctypes
import os
import pathlib
import signal
import subprocess
import sys
import time


# Liveness backstop, never a verdict. Every wait below is for a MONOTONIC state
# -- a published file stays published, an exited process stays exited -- so
# the bound only turns "never happened" into a FAIL instead of a hang, and a
# passing run never approaches it.
BACKSTOP_SECONDS = 120
POLL_SECONDS = 0.02

scheduler, fixture_arg, python, scenario = sys.argv[1:5]
fixture = pathlib.Path(fixture_arg)
barrier = fixture / "barrier"
results = fixture / "results.txt"


def process_state(pid: int) -> str:
    if os.name == "nt":
        handle = ctypes.windll.kernel32.OpenProcess(0x101000, False, pid)
        if not handle:
            return "gone"
        code = ctypes.c_ulong()
        active = (
            ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
            and code.value == 259
        )
        ctypes.windll.kernel32.CloseHandle(handle)
        return "live" if active else "gone"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "gone"
    stat_path = pathlib.Path("/proc") / str(pid) / "stat"
    if stat_path.exists():
        state = stat_path.read_text(encoding="utf-8", errors="replace").rsplit(
            ")", 1
        )[-1].strip()
        if state.startswith("Z "):
            return "zombie"
    return "live"


def terminated(pid: int) -> bool:
    """Whether a process the scheduler was obliged to kill is gone.

    POSIX answers in one look: the scheduler returns only after the whole
    process group has vanished. Windows must wait on the handle, because
    TerminateProcess returns before its target has finished exiting. That wait
    is exact rather than a race: the process is held on the gate, so a kill is
    the only way it can ever end -- killed, it ends; missed, it never does.
    """
    if os.name != "nt":
        return process_state(pid) != "live"
    handle = ctypes.windll.kernel32.OpenProcess(0x100000, False, pid)
    if not handle:
        return True
    try:
        return (
            ctypes.windll.kernel32.WaitForSingleObject(handle, BACKSTOP_SECONDS * 1000)
            == 0
        )
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)


def force_cleanup(pid: int) -> None:
    if process_state(pid) != "live":
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill.exe", "/PID", str(pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        os.kill(pid, signal.SIGKILL)


def wait_for(process: subprocess.Popen[str], what: str, *paths: pathlib.Path) -> None:
    deadline = time.monotonic() + BACKSTOP_SECONDS
    while not all(path.exists() for path in paths):
        if process.poll() is not None:
            _, stderr = process.communicate()
            raise SystemExit(
                f"FAIL: scheduler exited before {what} "
                f"(rc={process.returncode}, stderr={stderr!r})"
            )
        if time.monotonic() >= deadline:
            raise SystemExit(f"FAIL: {what} never happened")
        time.sleep(POLL_SECONDS)


def held_wave(suites: list[str], held: str, check, act=None) -> None:
    (fixture / "suites.txt").write_text(
        "".join(f"{suite}\n" for suite in suites), encoding="utf-8"
    )
    results.write_text("", encoding="utf-8")
    # A stale marker from an earlier wave would satisfy a wait below at once.
    for stale in ("ready", "established", "leader-exited", "release"):
        (barrier / f"{held}.{stale}").unlink(missing_ok=True)
    (barrier / f"{held}.hold").write_text("", encoding="utf-8")
    ready = barrier / f"{held}.ready"
    established = barrier / f"{held}.established"
    release = barrier / f"{held}.release"
    # The gate. This process holds the only write end (os.pipe() descriptors
    # are never inherited); the scheduler, the suite leader and its descendant
    # all read the other end as stdin.
    gate_read, gate_write = os.pipe()
    try:
        process = subprocess.Popen(
            [
                python,
                scheduler,
                "--suite-file",
                str(fixture / "suites.txt"),
                "--log-dir",
                str(fixture / "logs"),
                "--results-file",
                str(results),
                "--jobs",
                "1",
                "--timeout",
                "1",
                "--slow-timeout",
                "1",
                "--kill-grace",
                "1",
                "--test-pre-terminate-barrier-dir",
                str(barrier),
                python,
                str(fixture / "fake_runner.py"),
                str(barrier),
            ],
            stdin=gate_read,
            stderr=subprocess.PIPE,
            text=True,
        )
    except BaseException:
        os.close(gate_write)
        raise
    finally:
        os.close(gate_read)
    descendant = None
    try:
        wait_for(
            process,
            f"{held} reached its timeout with the fixture established",
            ready,
            established,
        )
        # Both are held -- the scheduler parked at the barrier, the fixture on
        # the gate -- so anything but "live" means the fixture is broken and
        # would no longer exercise what the assertions below claim.
        leader = int(ready.read_text(encoding="utf-8"))
        marker = established.read_text(encoding="utf-8").strip()
        if marker != "none":
            if not marker.isdigit():
                raise SystemExit(f"FAIL: {held} could not arm its descendant ({marker!r})")
            descendant = int(marker)
        for role, pid in (("leader", leader), ("descendant", descendant)):
            state = "live" if pid is None else process_state(pid)
            if state != "live":
                raise SystemExit(f"FAIL: {held} {role} {pid} is not held by the gate ({state})")
        if act is not None:
            act(process, leader)
        release.write_text("release\n", encoding="utf-8")
        # Generous on purpose: on Windows a refusal spends up to the
        # descendant-probe budget twice (wave loop, then the cleanup pass).
        _, stderr = process.communicate(timeout=BACKSTOP_SECONDS)
        check(process.returncode, stderr, descendant)
    finally:
        if not release.exists():
            release.write_text("release\n", encoding="utf-8")
        if process.poll() is None:
            process.kill()
            process.wait()
        # Only now -- verdict recorded -- does anything held get to end.
        os.close(gate_write)
        if descendant is not None and not terminated(descendant):
            force_cleanup(descendant)


def expect_clean_exit(returncode: int, stderr: str, descendant: int | None) -> None:
    if returncode != 0:
        raise SystemExit(f"FAIL: scheduler wave failed (rc={returncode}, stderr={stderr!r})")


def expect_tree_killed(returncode: int, stderr: str, descendant: int | None) -> None:
    expect_clean_exit(returncode, stderr, descendant)
    if not terminated(descendant):
        raise SystemExit("FAIL: timed-out suite left a stubborn descendant alive")


def kill_leader_first(process: subprocess.Popen[str], leader: int) -> None:
    # The race under test: the leader exits after the timeout decision but
    # before the scheduler terminates its tree.
    os.kill(leader, signal.SIGTERM)
    wait_for(
        process,
        "the scheduler observed the forced leader exit",
        barrier / "timeout_exit_race.leader-exited",
    )


def expect_race_handled(returncode: int, stderr: str, descendant: int | None) -> None:
    if os.name == "nt":
        # Assert the PROPERTY, not the wording: the scheduler refused (rc=2)
        # AND did not quietly leave a live descendant behind as if cleanup had
        # succeeded. The descendant is held on the gate, so it is still there
        # to be refused over however long the refusal took.
        if returncode != 2:
            raise SystemExit(
                f"FAIL: Windows timeout race did not fail closed "
                f"(rc={returncode}, stderr={stderr!r})"
            )
        if process_state(descendant) != "live":
            raise SystemExit(
                "FAIL: Windows timeout race refused without a surviving descendant "
                "to refuse over -- the fixture no longer exercises the race"
            )
        if "cleanup" not in stderr.lower():
            raise SystemExit(
                f"FAIL: Windows timeout race refused without naming a cleanup "
                f"failure (stderr={stderr!r})"
            )
        return
    if returncode != 0:
        raise SystemExit(
            f"FAIL: POSIX timeout race cleanup failed (rc={returncode}, stderr={stderr!r})"
        )
    if not results.read_text(encoding="utf-8").startswith(
        "timeout_exit_race rc=124 pass=1 fail=0 skip=0 secs="
    ):
        raise SystemExit("FAIL: POSIX timeout race lost its bounded result")
    if not terminated(descendant):
        raise SystemExit("FAIL: POSIX timeout race leaked the surviving descendant")


if scenario == "hang_after_summary":
    held_wave(
        ["hang_after_summary", "no_summary", "pass_after"],
        "hang_after_summary",
        expect_clean_exit,
    )
elif scenario == "stubborn_tree":
    held_wave(["stubborn_tree"], "stubborn_tree", expect_tree_killed)
elif scenario == "timeout_exit_race":
    held_wave(
        ["timeout_exit_race"],
        "timeout_exit_race",
        expect_race_handled,
        act=kill_leader_first,
    )
else:
    raise SystemExit(f"FAIL: unknown held-wave scenario {scenario!r}")
PY

seq 1 32 | sed 's/^/pass_/' >"$fixture/suites.txt"
: >"$fixture/results.txt"
python3 "$scheduler" \
    --suite-file "$fixture/suites.txt" \
    --log-dir "$fixture/logs" \
    --results-file "$fixture/results.txt" \
    --jobs 8 \
    --timeout 5 \
    --slow-timeout 5 \
    --kill-grace 1 \
    "$(command -v python3)" "$fixture/fake_runner.py" "$fixture/barrier"

if [ "$(wc -l <"$fixture/results.txt" | tr -d ' ')" -ne 32 ] ||
    grep -qvE '^pass_[0-9]+ rc=0 pass=1 fail=0 skip=0 secs=[0-9]+$' \
        "$fixture/results.txt"; then
    echo "FAIL: scheduler lost or corrupted a completed child's result" >&2
    cat "$fixture/results.txt" >&2
    exit 1
fi

python3 "$fixture/held_wave.py" "$scheduler" "$fixture" "$(command -v python3)" \
    hang_after_summary

if ! grep -qE '^hang_after_summary rc=124 pass=1 fail=0 skip=0 secs=[0-9]+$' \
    "$fixture/results.txt"; then
    echo "FAIL: scheduler did not bound and record a child that stayed alive after its summary" >&2
    cat "$fixture/results.txt" >&2
    exit 1
fi
if ! grep -qE '^no_summary rc=97 pass=0 fail=0 skip=0 secs=[0-9]+$' \
    "$fixture/results.txt"; then
    echo "FAIL: scheduler accepted a zero-test child" >&2
    cat "$fixture/results.txt" >&2
    exit 1
fi
if ! grep -qE '^pass_after rc=0 pass=1 fail=0 skip=0 secs=[0-9]+$' \
    "$fixture/results.txt"; then
    echo "FAIL: scheduler did not continue after a bounded child failure" >&2
    cat "$fixture/results.txt" >&2
    exit 1
fi

python3 "$fixture/held_wave.py" "$scheduler" "$fixture" "$(command -v python3)" \
    stubborn_tree

printf '%s\n' pass_after >"$fixture/suites.txt"
: >"$fixture/results.txt"
: >"$fixture/barrier/pass_after.hold"
python3 - "$scheduler" "$fixture" "$(command -v python3)" <<'PY'
from __future__ import annotations

import pathlib
import subprocess
import sys
import time


scheduler = sys.argv[1]
fixture = pathlib.Path(sys.argv[2])
python = sys.argv[3]
results = fixture / "results.txt"
barrier = fixture / "barrier"
process = subprocess.Popen(
    [
        python,
        scheduler,
        "--suite-file",
        str(fixture / "suites.txt"),
        "--log-dir",
        str(fixture / "logs"),
        "--results-file",
        str(results),
        "--jobs",
        "1",
        "--timeout",
        "5",
        "--slow-timeout",
        "5",
        "--kill-grace",
        "1",
        "--test-post-exit-barrier-dir",
        str(barrier),
        python,
        str(fixture / "fake_runner.py"),
        str(barrier),
    ]
)
try:
    deadline = time.monotonic() + 5
    while not (barrier / "pass_after.ready").exists():
        if process.poll() is not None:
            raise SystemExit(
                f"FAIL: scheduler exited before exposing the post-exit barrier "
                f"(rc={process.returncode})"
            )
        if time.monotonic() >= deadline:
            raise SystemExit("FAIL: scheduler never reached the post-exit barrier")
        time.sleep(0.02)
    if results.read_text(encoding="utf-8"):
        raise SystemExit("FAIL: scheduler recorded a result before the forced barrier released")
    (barrier / "pass_after.release").write_text("release\n", encoding="utf-8")
    returncode = process.wait(timeout=5)
    if returncode != 0:
        raise SystemExit(f"FAIL: scheduler failed after the post-exit release (rc={returncode})")
finally:
    if process.poll() is None:
        process.kill()
        process.wait()

expected = "pass_after rc=0 pass=1 fail=0 skip=0 secs="
if not results.read_text(encoding="utf-8").startswith(expected):
    raise SystemExit("FAIL: exited child result was lost after the forced barrier")
PY

python3 "$fixture/held_wave.py" "$scheduler" "$fixture" "$(command -v python3)" \
    timeout_exit_race

echo "Parallel harness contract passed"
