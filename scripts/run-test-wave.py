#!/usr/bin/env python3
"""Run one wave of C test suites without nested shell worker processes.

The caller owns suite selection, sharding, and final union/count checks.  This
helper owns native child processes directly, writes one result for every suite,
and bounds a child that never exits.  Keeping accounting in this single parent
avoids an MSYS2 failure mode where a completed native child left its `bash -c`
worker permanently stuck before the result append.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass


SUITE_NAME = re.compile(r"^[a-z0-9_]+$")
SUMMARY = re.compile(r"^  (?P<passed>[0-9]+) passed")
FAILED = re.compile(r"(?:^|, )(?P<failed>[0-9]+) failed")
SKIPPED = re.compile(r"(?:^|, )(?P<skipped>[0-9]+) skipped")
SLOW_SUITES = frozenset(("incremental", "store_arch", "daemon_runtime"))
POLL_SECONDS = 0.05


@dataclass
class ActiveSuite:
    name: str
    process: subprocess.Popen[bytes]
    log_path: pathlib.Path
    log_file: object
    started: float
    timeout: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a bounded parallel wave of test-runner suites"
    )
    parser.add_argument("--suite-file", required=True, type=pathlib.Path)
    parser.add_argument("--log-dir", required=True, type=pathlib.Path)
    parser.add_argument("--results-file", required=True, type=pathlib.Path)
    parser.add_argument("--jobs", required=True, type=int)
    parser.add_argument("--timeout", required=True, type=int)
    parser.add_argument("--slow-timeout", required=True, type=int)
    parser.add_argument("--kill-grace", required=True, type=int)
    parser.add_argument(
        "--test-post-exit-barrier-dir",
        type=pathlib.Path,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--test-pre-terminate-barrier-dir",
        type=pathlib.Path,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "runner_command",
        nargs="+",
        help="runner executable and any fixed arguments; suite name is appended",
    )
    args = parser.parse_args()
    for name in ("jobs", "timeout", "slow_timeout", "kill_grace"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be at least 1")
    return args


def read_suites(path: pathlib.Path) -> list[str]:
    try:
        suites = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RuntimeError(f"cannot read suite file {path}: {exc}") from exc
    malformed = [suite for suite in suites if SUITE_NAME.fullmatch(suite) is None]
    if malformed:
        raise RuntimeError(f"malformed suite name in {path}: {malformed[0]!r}")
    if len(set(suites)) != len(suites):
        raise RuntimeError(f"duplicate suite name in {path}")
    return suites


def append_log(path: pathlib.Path, message: str) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(message)
        stream.write("\n")


def start_suite(
    suite: str,
    runner_command: list[str],
    log_dir: pathlib.Path,
    timeout: int,
) -> ActiveSuite | None:
    log_path = log_dir / f"{suite}.log"
    log_file = log_path.open("wb")
    popen_args: dict[str, object] = {
        "stdout": log_file,
        "stderr": subprocess.STDOUT,
    }
    if os.name == "nt":
        popen_args["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_args["start_new_session"] = True
    try:
        process = subprocess.Popen(runner_command + [suite], **popen_args)
    except OSError as exc:
        log_file.close()
        append_log(log_path, f"  FAIL: could not start suite {suite!r}: {exc}")
        return None
    return ActiveSuite(
        name=suite,
        process=process,
        log_path=log_path,
        log_file=log_file,
        started=time.monotonic(),
        timeout=timeout,
    )


def terminate_process_tree(active: ActiveSuite, kill_grace: int) -> None:
    process = active.process
    leader_exited = process.poll() is not None
    if os.name == "nt":
        if leader_exited:
            raise RuntimeError(
                f"suite {active.name!r} leader exited before Windows tree cleanup "
                "could be proven"
            )
        try:
            completed = subprocess.run(
                [
                    "taskkill.exe",
                    "/PID",
                    str(process.pid),
                    "/T",
                    "/F",
                ],
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=kill_grace,
            )
        except (OSError, subprocess.TimeoutExpired):
            completed = None
        if completed is None or completed.returncode != 0:
            if process.poll() is None:
                process.kill()
                try:
                    process.wait(timeout=kill_grace)
                except subprocess.TimeoutExpired:
                    pass
            raise RuntimeError(
                f"suite {active.name!r} taskkill could not prove process-tree cleanup"
            )
        try:
            process.wait(timeout=kill_grace)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            raise RuntimeError(
                f"suite {active.name!r} process tree resisted forced termination"
            ) from exc
        return

    def group_active() -> bool:
        try:
            os.killpg(process.pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def wait_for_group_exit(deadline: float) -> bool:
        while time.monotonic() < deadline:
            process.poll()
            if not group_active():
                return True
            time.sleep(POLL_SECONDS)
        process.poll()
        return not group_active()

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    if wait_for_group_exit(time.monotonic() + kill_grace):
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    if wait_for_group_exit(time.monotonic() + kill_grace):
        return
    raise RuntimeError(
        f"suite {active.name!r} process group persisted after forced termination"
    )


def wait_for_test_pre_terminate_barrier(
    barrier_dir: pathlib.Path | None,
    active: ActiveSuite,
) -> None:
    if barrier_dir is None:
        return
    hold = barrier_dir / f"{active.name}.hold"
    if not hold.exists():
        return
    ready = barrier_dir / f"{active.name}.ready"
    leader_exited = barrier_dir / f"{active.name}.leader-exited"
    release = barrier_dir / f"{active.name}.release"
    ready.write_text(f"{active.process.pid}\n", encoding="utf-8")
    deadline = time.monotonic() + 10
    while not release.exists():
        returncode = active.process.poll()
        if returncode is not None and not leader_exited.exists():
            leader_exited.write_text(f"{returncode}\n", encoding="utf-8")
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"test pre-terminate barrier for {active.name!r} was not released"
            )
        time.sleep(POLL_SECONDS)


def wait_for_test_post_exit_barrier(
    barrier_dir: pathlib.Path | None,
    suite: str,
) -> None:
    if barrier_dir is None:
        return
    hold = barrier_dir / f"{suite}.hold"
    if not hold.exists():
        return
    ready = barrier_dir / f"{suite}.ready"
    release = barrier_dir / f"{suite}.release"
    ready.write_text("child exited; result intentionally not recorded\n", encoding="utf-8")
    deadline = time.monotonic() + 10
    while not release.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError(f"test post-exit barrier for {suite!r} was not released")
        time.sleep(POLL_SECONDS)


def parse_summary(log_path: pathlib.Path) -> tuple[int, int, int] | None:
    try:
        stream = log_path.open(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise RuntimeError(f"cannot read suite log {log_path}: {exc}") from exc
    last_summary = None
    with stream:
        for line in stream:
            match = SUMMARY.match(line)
            if match is None:
                continue
            failed = FAILED.search(line)
            skipped = SKIPPED.search(line)
            last_summary = (
                int(match.group("passed")),
                int(failed.group("failed")) if failed is not None else 0,
                int(skipped.group("skipped")) if skipped is not None else 0,
            )
    return last_summary


def record_result(
    active: ActiveSuite,
    returncode: int,
    results_file: pathlib.Path,
    timed_out: bool,
) -> None:
    active.log_file.close()
    elapsed = max(0, int(time.monotonic() - active.started))
    if timed_out:
        returncode = 124
        append_log(
            active.log_path,
            f"  FAIL: suite {active.name!r} exceeded {active.timeout}s wall clock "
            "(killed as hung)",
        )
    summary = parse_summary(active.log_path)
    if returncode == 0 and summary is None:
        returncode = 97
        append_log(
            active.log_path,
            f"  FAIL: suite {active.name!r} exited 0 without a completion summary "
            "(ran nothing?)",
        )
    passed, failed, skipped = summary or (0, 0, 0)
    result = (
        f"{active.name} rc={returncode} pass={passed} fail={failed} "
        f"skip={skipped} secs={elapsed}"
    )
    with results_file.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(result)
        stream.write("\n")
    print(f"  {result}", flush=True)


def record_start_failure(
    suite: str,
    log_dir: pathlib.Path,
    results_file: pathlib.Path,
) -> None:
    result = f"{suite} rc=98 pass=0 fail=0 skip=0 secs=0"
    with results_file.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(result)
        stream.write("\n")
    print(f"  {result}", flush=True)
    if not (log_dir / f"{suite}.log").exists():
        append_log(log_dir / f"{suite}.log", f"  FAIL: suite {suite!r} did not start")


def run_wave(args: argparse.Namespace) -> None:
    suites = read_suites(args.suite_file)
    args.log_dir.mkdir(parents=True, exist_ok=True)
    args.results_file.parent.mkdir(parents=True, exist_ok=True)
    args.results_file.touch(exist_ok=True)
    pending = list(suites)
    active: dict[str, ActiveSuite] = {}
    try:
        while pending or active:
            while pending and len(active) < args.jobs:
                suite = pending.pop(0)
                timeout = (
                    args.slow_timeout if suite in SLOW_SUITES else args.timeout
                )
                started = start_suite(
                    suite,
                    list(args.runner_command),
                    args.log_dir,
                    timeout,
                )
                if started is None:
                    record_start_failure(suite, args.log_dir, args.results_file)
                else:
                    active[suite] = started

            made_progress = False
            now = time.monotonic()
            for suite, running in list(active.items()):
                returncode = running.process.poll()
                timed_out = returncode is None and now - running.started >= running.timeout
                if returncode is None and not timed_out:
                    continue
                if timed_out:
                    wait_for_test_pre_terminate_barrier(
                        args.test_pre_terminate_barrier_dir,
                        running,
                    )
                    terminate_process_tree(running, args.kill_grace)
                    returncode = running.process.returncode
                wait_for_test_post_exit_barrier(
                    args.test_post_exit_barrier_dir,
                    suite,
                )
                record_result(
                    running,
                    int(returncode if returncode is not None else 124),
                    args.results_file,
                    timed_out,
                )
                del active[suite]
                made_progress = True
            if active and not made_progress:
                time.sleep(POLL_SECONDS)
    finally:
        cleanup_errors: list[str] = []
        for running in active.values():
            try:
                terminate_process_tree(running, args.kill_grace)
            except (OSError, RuntimeError) as exc:
                cleanup_errors.append(f"{running.name}: {exc}")
            finally:
                try:
                    running.log_file.close()
                except OSError as exc:
                    cleanup_errors.append(f"{running.name} log close: {exc}")
        if cleanup_errors:
            raise RuntimeError(
                "parallel scheduler cleanup failed: " + "; ".join(cleanup_errors)
            )


def main() -> int:
    args = parse_args()
    try:
        if os.environ.get("MSYSTEM") and os.name != "nt":
            raise RuntimeError(
                "Windows/MSYS test runs require the native MinGW Python "
                "(os.name must be 'nt')"
            )
        run_wave(args)
    except (OSError, RuntimeError) as exc:
        print(f"FAIL: parallel scheduler infrastructure error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
