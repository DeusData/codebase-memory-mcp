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
    def __init__(self, stdout: str) -> None:
        self.returncode = 0
        self.stdout = stdout


try:
    subprocess.run = timing_out
    timed_out_reason = probe(4321)
    subprocess.run = lambda *a, **k: _Completed("3\n")
    live_reason = probe(4321)
    subprocess.run = lambda *a, **k: _Completed("0\n")
    clean_reason = probe(4321)
finally:
    subprocess.run = original_run

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

cat >"$fixture/fake_runner.py" <<'PY'
from __future__ import annotations

import os
import pathlib
import signal
import subprocess
import sys
import time


pid_path = pathlib.Path(sys.argv[1])
suite = sys.argv[-1]
if suite == "hang_after_summary":
    print("  1 passed", flush=True)
    time.sleep(30)
elif suite in ("stubborn_tree", "timeout_exit_race"):
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import os,signal,time;"
                "signal.signal(signal.SIGTERM,signal.SIG_IGN) "
                "if os.name != 'nt' else None;"
                "time.sleep(30)"
            ),
        ]
    )
    pid_path.write_text(str(child.pid), encoding="utf-8")
    print("  1 passed", flush=True)
    time.sleep(30)
elif suite == "no_summary":
    pass
else:
    print("  1 passed", flush=True)
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
    "$(command -v python3)" "$fixture/fake_runner.py" "$fixture/descendant.pid"

if [ "$(wc -l <"$fixture/results.txt" | tr -d ' ')" -ne 32 ] ||
    grep -qvE '^pass_[0-9]+ rc=0 pass=1 fail=0 skip=0 secs=[0-9]+$' \
        "$fixture/results.txt"; then
    echo "FAIL: scheduler lost or corrupted a completed child's result" >&2
    cat "$fixture/results.txt" >&2
    exit 1
fi

printf '%s\n' hang_after_summary no_summary pass_after >"$fixture/suites.txt"
: >"$fixture/results.txt"
python3 "$scheduler" \
    --suite-file "$fixture/suites.txt" \
    --log-dir "$fixture/logs" \
    --results-file "$fixture/results.txt" \
    --jobs 1 \
    --timeout 1 \
    --slow-timeout 1 \
    --kill-grace 1 \
    "$(command -v python3)" "$fixture/fake_runner.py" "$fixture/descendant.pid"

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

printf '%s\n' stubborn_tree >"$fixture/suites.txt"
: >"$fixture/results.txt"
python3 "$scheduler" \
    --suite-file "$fixture/suites.txt" \
    --log-dir "$fixture/logs" \
    --results-file "$fixture/results.txt" \
    --jobs 1 \
    --timeout 1 \
    --slow-timeout 1 \
    --kill-grace 1 \
    "$(command -v python3)" "$fixture/fake_runner.py" "$fixture/descendant.pid"

python3 - "$fixture/descendant.pid" <<'PY'
import ctypes
import os
import pathlib
import signal
import subprocess
import sys


pid = int(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
alive = False
if os.name == "nt":
    process = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
    if process:
        code = ctypes.c_ulong()
        if ctypes.windll.kernel32.GetExitCodeProcess(process, ctypes.byref(code)):
            alive = code.value == 259
        ctypes.windll.kernel32.CloseHandle(process)
else:
    try:
        os.kill(pid, 0)
        alive = True
    except ProcessLookupError:
        pass
    if alive:
        stat_path = pathlib.Path("/proc") / str(pid) / "stat"
        state = ""
        if stat_path.exists():
            state = stat_path.read_text(encoding="utf-8", errors="replace").rsplit(
                ")", 1
            )[-1].strip()
        if state.startswith("Z "):
            alive = False

if alive:
    if os.name == "nt":
        subprocess.run(
            ["taskkill.exe", "/PID", str(pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        os.kill(pid, signal.SIGKILL)
    raise SystemExit("FAIL: timed-out suite left a stubborn descendant alive")
PY

printf '%s\n' pass_after >"$fixture/suites.txt"
: >"$fixture/results.txt"
mkdir "$fixture/barrier"
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
        str(fixture / "descendant.pid"),
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

printf '%s\n' timeout_exit_race >"$fixture/suites.txt"
: >"$fixture/results.txt"
rm -f "$fixture/descendant.pid"
: >"$fixture/barrier/timeout_exit_race.hold"
python3 - "$scheduler" "$fixture" "$(command -v python3)" <<'PY'
from __future__ import annotations

import ctypes
import os
import pathlib
import signal
import subprocess
import sys
import time


scheduler = sys.argv[1]
fixture = pathlib.Path(sys.argv[2])
python = sys.argv[3]
results = fixture / "results.txt"
barrier = fixture / "barrier"
ready = barrier / "timeout_exit_race.ready"
leader_exited = barrier / "timeout_exit_race.leader-exited"
release = barrier / "timeout_exit_race.release"
descendant_path = fixture / "descendant.pid"


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
        str(descendant_path),
    ],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)
try:
    deadline = time.monotonic() + 5
    while not ready.exists():
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise SystemExit(
                f"FAIL: scheduler exited before the timeout-race barrier "
                f"(rc={process.returncode}, stdout={stdout!r}, stderr={stderr!r})"
            )
        if time.monotonic() >= deadline:
            raise SystemExit("FAIL: scheduler never reached the timeout-race barrier")
        time.sleep(0.02)

    leader_pid = int(ready.read_text(encoding="utf-8"))
    os.kill(leader_pid, signal.SIGTERM)
    deadline = time.monotonic() + 3
    while not leader_exited.exists():
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise SystemExit(
                f"FAIL: scheduler exited before observing the forced leader exit "
                f"(rc={process.returncode}, stdout={stdout!r}, stderr={stderr!r})"
            )
        if time.monotonic() >= deadline:
            raise SystemExit("FAIL: scheduler did not observe the forced leader exit")
        time.sleep(0.02)
    release.write_text("release\n", encoding="utf-8")
    # Generous on purpose: the scheduler's refusal is the asserted state, and
    # on Windows it now spends up to the descendant-probe budget (twice --
    # once in the wave loop, once in the cleanup pass) before refusing. This
    # bound only has to exceed that worst case; it never decides the verdict.
    stdout, stderr = process.communicate(timeout=120)

    if os.name == "nt":
        # Assert the PROPERTY, not the wording. This used to require the phrase
        # "tree cleanup" in stderr, which pinned one specific refusal message:
        # rewording the guard broke the contract while the behaviour was still
        # correct. What "fails closed" actually means is that the scheduler
        # refused (rc=2) AND did not silently leave the descendant behind as if
        # cleanup had succeeded.
        descendant_pid = int(descendant_path.read_text(encoding="utf-8"))
        if process.returncode != 2:
            raise SystemExit(
                f"FAIL: Windows timeout race did not fail closed "
                f"(rc={process.returncode}, stdout={stdout!r}, stderr={stderr!r})"
            )
        if process_state(descendant_pid) != "live":
            raise SystemExit(
                "FAIL: Windows timeout race refused without a surviving descendant "
                "to refuse over -- the fixture no longer exercises the race"
            )
        if "cleanup" not in stderr.lower():
            raise SystemExit(
                f"FAIL: Windows timeout race refused without naming a cleanup "
                f"failure (stderr={stderr!r})"
            )
    else:
        if process.returncode != 0:
            raise SystemExit(
                f"FAIL: POSIX timeout race cleanup failed "
                f"(rc={process.returncode}, stdout={stdout!r}, stderr={stderr!r})"
            )
        if not results.read_text(encoding="utf-8").startswith(
            "timeout_exit_race rc=124 pass=1 fail=0 skip=0 secs="
        ):
            raise SystemExit("FAIL: POSIX timeout race lost its bounded result")
        descendant_pid = int(descendant_path.read_text(encoding="utf-8"))
        if process_state(descendant_pid) == "live":
            raise SystemExit("FAIL: POSIX timeout race leaked the surviving descendant")
finally:
    release.write_text("release\n", encoding="utf-8")
    if process.poll() is None:
        process.kill()
        process.wait()
    if descendant_path.exists():
        force_cleanup(int(descendant_path.read_text(encoding="utf-8")))
PY

echo "Parallel harness contract passed"
