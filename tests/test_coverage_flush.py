#!/usr/bin/env python3
"""Real canonical-runner regression for normal-parent profile write failures.

The same fixed oracle applies to every source tree. There is no baseline mode.
This driver neither changes the collector nor substitutes an LLVM writer.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import threading
import time

DEADLINE_SECONDS = 60
REAP_SECONDS = 5
LOG_BYTES = 1024 * 1024
TEST = "gi_exact_file"
SELECTOR = "gitignore:" + TEST
ANSI = re.compile(r"\x1b\[[0-9;]*m")
BAD_SETUP = (
    "Unknown test suite:",
    "No matching test suites requested",
    "selected test not compiled into this build or unknown:",
    "test selection names no test",
    "cannot use coverage directory:",
    "cannot route coverage profiles under:",
    "a coverage run starts with LLVM_PROFILE_FILE=",
    "failed to create isolated test cache",
)


def identity(path):
    resolved = Path(path).resolve(strict=True)
    info = resolved.stat()
    if not stat.S_ISREG(info.st_mode) or not os.access(str(resolved), os.X_OK):
        raise ValueError("not an executable regular file: " + str(resolved))
    digest = hashlib.sha256()
    with resolved.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return {"path": str(resolved), "sha256": digest.hexdigest(), "bytes": info.st_size}


def save_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def drain(pipe, destination, state, stop):
    try:
        with destination.open("xb", buffering=0) as log:
            while True:
                data = pipe.read(4096)
                if not data:
                    break
                state["seen_bytes"] += len(data)
                remaining = LOG_BYTES - state["saved_bytes"]
                prefix = memoryview(data)[:max(remaining, 0)]
                while prefix:
                    count = log.write(prefix)
                    if not count:
                        raise OSError("short log write")
                    state["saved_bytes"] += count
                    prefix = prefix[count:]
                if state["seen_bytes"] > LOG_BYTES:
                    state["overflow"] = True
                    stop.set()
    except (OSError, ValueError) as exc:
        state["error"] = str(exc)
        stop.set()
    finally:
        pipe.close()


def reap(proc):
    if proc.poll() is not None:
        return None
    try:
        proc.terminate()
    except OSError:
        pass
    try:
        proc.wait(timeout=REAP_SECONDS)
        return None
    except subprocess.TimeoutExpired:
        pass
    try:
        proc.kill()
        proc.wait(timeout=REAP_SECONDS)
        return None
    except (OSError, subprocess.TimeoutExpired):
        return "owned child did not terminate after kill"


def run_process(directory, label, argv, environment):
    record = {"argv": argv, "deadline_seconds": DEADLINE_SECONDS, "returncode": None,
              "failure": None, "stdout": label + ".stdout.log", "stderr": label + ".stderr.log"}
    started = time.monotonic()
    try:
        proc = subprocess.Popen(argv, env=environment, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    except OSError as exc:
        record["failure"] = "spawn: " + str(exc)
        return record
    record["pid"] = proc.pid
    stop = threading.Event()
    streams = []
    threads = []
    started_pipes = []
    try:
        for kind, pipe in (("stdout", proc.stdout), ("stderr", proc.stderr)):
            state = {"seen_bytes": 0, "saved_bytes": 0, "overflow": False, "error": None}
            thread = threading.Thread(target=drain,
                                      args=(pipe, directory / record[kind], state, stop), daemon=True)
            thread.start()
            streams.append(state)
            threads.append(thread)
            started_pipes.append(pipe)
        while proc.poll() is None:
            remaining = DEADLINE_SECONDS - (time.monotonic() - started)
            if stop.is_set() or remaining <= 0:
                record["failure"] = "log limit/read failure" if stop.is_set() else "timeout"
                break
            try:
                proc.wait(timeout=min(remaining, 0.1))
            except subprocess.TimeoutExpired:
                pass
    except (OSError, RuntimeError, KeyboardInterrupt) as exc:
        record["failure"] = type(exc).__name__ + ": " + str(exc)
    finally:
        cleanup_error = reap(proc)
        if cleanup_error:
            record["cleanup_error"] = cleanup_error
            record["failure"] = record["failure"] or cleanup_error
        for pipe in (proc.stdout, proc.stderr):
            if pipe not in started_pipes:
                pipe.close()
        join_until = time.monotonic() + REAP_SECONDS
        for thread in threads:
            thread.join(timeout=max(0, join_until - time.monotonic()))
        if any(thread.is_alive() for thread in threads):
            record["failure"] = "pipe reader did not quiesce; unexpected inherited pipe"
        if any(item["overflow"] or item["error"] for item in streams):
            record["failure"] = record["failure"] or "bounded log capture failed"
        record["returncode"] = proc.poll()
        record["elapsed_seconds"] = round(time.monotonic() - started, 3)
        record["streams"] = dict(zip(("stdout", "stderr"), streams))
    return record


def regular_nonempty(path):
    try:
        info = path.lstat()
        return stat.S_ISREG(info.st_mode) and info.st_size > 0
    except OSError:
        return False


def same_path(left, right):
    return os.path.normcase(os.path.normpath(left)) == os.path.normcase(os.path.normpath(str(right)))


def write_error_evidence(stderr, obstacle):
    # Static LLVM22 library evidence; first native RED must confirm this binding.
    # Different wording is an unknown precondition, never guessed to be success.
    errors = [line for line in stderr.splitlines() if line.startswith("LLVM Profile Error:")]
    matched = []
    for line in errors:
        found = re.fullmatch(r'LLVM Profile Error: Failed to write file "(.*)": (.+)', line)
        if not found or not same_path(found.group(1), obstacle):
            return False, errors
        matched.append(line)
    return bool(matched), matched


def test_output(stdout):
    clean = ANSI.sub("", stdout)
    starts = re.findall(r"(?m)^  " + TEST + r"\b", clean)
    rows = re.findall(r"(?m)^  ([A-Za-z_][A-Za-z0-9_]*)[ \t]+(PASS|FAIL|SKIP)\b", clean)
    exact_passes = re.findall(r"(?m)^  " + TEST + r"[ \t]+PASS[ \t]*$", clean)
    summaries = re.findall(
        r"(?m)^[ \t]*(\d+) passed(?:, (\d+) failed)?(?:, (\d+) skipped)?"
        r"(?:, (\d+) deselected)?[ \t]*$", clean)
    return {"started": len(starts), "passed": len(exact_passes), "rows": rows,
            "summaries": [[int(value or 0) for value in row] for row in summaries]}


def expected_test_once(observation):
    return (observation["started"] == 1 and observation["passed"] == 1
            and observation["rows"] == [(TEST, "PASS")])


def merge_profile(case, raw, name, tool, environment):
    output = case / (name + ".profdata")
    result = {"raw": str(raw), "merged": str(output), "valid": False}
    if not regular_nonempty(raw):
        result["failure"] = "raw profile is not a regular nonempty file"
        return result
    command = [tool, "merge", "--instr", "--output=" + str(output), str(raw)]
    result["process"] = run_process(case, "merge-" + name, command, environment)
    process = result["process"]
    result["valid"] = (not process["failure"] and process["returncode"] == 0
                       and regular_nonempty(output))
    if not result["valid"]:
        result["failure"] = "matching llvm-profdata did not validate the raw profile"
    return result


def classify(case, mode, process, obstacle):
    answer = {"classification": "infrastructure_error", "reasons": [], "observed_test": None}
    if process["failure"]:
        answer["reasons"].append(process["failure"])
        return answer
    rc = process["returncode"]
    if rc is None or rc < 0 or rc > 255:
        answer["reasons"].append("child did not exit normally")
        return answer
    stdout = (case / process["stdout"]).read_text(encoding="utf-8", errors="replace")
    stderr = (case / process["stderr"]).read_text(encoding="utf-8", errors="replace")
    observed = test_output(stdout)
    answer["observed_test"] = observed
    if any(text in stderr for text in BAD_SETUP):
        answer["reasons"].append("initialization or selection failed")
        return answer
    if observed["started"] > 1 or any(row != (TEST, "PASS") for row in observed["rows"]):
        answer["reasons"].append("unexpected test execution or body failure")
        return answer
    if mode != "begin" and not expected_test_once(observed):
        answer["reasons"].append("selected test did not run exactly once and pass")
        return answer
    if mode == "success":
        if rc or len(observed["summaries"]) != 1 or observed["summaries"][0][:3] != [1, 0, 0]:
            answer["reasons"].append("positive runner control failed")
        elif "LLVM Profile Error:" in stderr or "coverage profile" in stderr.lower():
            answer["reasons"].append("positive control reported a coverage error")
        else:
            answer["classification"] = "pass"
        return answer
    try:
        is_directory = stat.S_ISDIR(obstacle.lstat().st_mode)
    except OSError:
        is_directory = False
    if not is_directory:
        answer["reasons"].append("owned obstacle is no longer a directory")
        return answer
    known_error, evidence = write_error_evidence(stderr, obstacle)
    answer["llvm_write_error_lines"] = evidence
    if not known_error:
        answer["classification"] = "unknown_error_precondition"
        answer["reasons"].append("intended LLVM target-path write failure was not recognized")
        return answer
    if rc == 0:
        answer["classification"] = "regression_failure"
        answer["reasons"].append("coverage write failed but runner silently exited 0")
    elif not any(line.startswith("coverage profile write failed:") for line in stderr.splitlines()):
        answer["reasons"].append("nonzero exit lacks the runner-owned coverage failure diagnostic")
    else:
        answer["classification"] = "pass"
    return answer


def run_case(root, mode, runner, tool):
    case = root / mode
    case.mkdir(mode=0o700)
    coverage = case / "coverage"
    suite = coverage / "gitignore"
    suite.mkdir(mode=0o700, parents=True)
    setup = suite / "_setup.0.profraw"
    parent = suite / (TEST + ".parent.profraw")
    obstacle = setup if mode == "begin" else parent if mode == "end" else None
    if obstacle is not None:
        obstacle.mkdir(mode=0o700)
    environment = os.environ.copy()
    environment["LLVM_PROFILE_FILE"] = "/dev/null"
    environment["CBM_TEST_COVERAGE_DIR"] = str(coverage)
    environment["CBM_TEST_ONLY"] = SELECTOR
    environment.pop("CBM_TEST_ONLY_FILE", None)
    process = run_process(case, "runner", [runner, "gitignore"], environment)
    result = {"case": mode, "runner": process, "coverage_root": str(coverage),
              "obstacle": str(obstacle) if obstacle is not None else None}
    result.update(classify(case, mode, process, obstacle))
    profiles = []
    eligible = result["classification"] in ("pass", "regression_failure")
    if mode in ("success", "end") and eligible:
        profiles.append(merge_profile(case, setup, "setup", tool, environment))
    if mode == "success" and eligible and all(profile["valid"] for profile in profiles):
        profiles.append(merge_profile(case, parent, "parent", tool, environment))
    result["profile_checks"] = profiles
    if any(not profile["valid"] for profile in profiles):
        result["classification"] = "infrastructure_error"
        result["reasons"].append("required success/setup profile could not be validated")
    save_json(case / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", required=True, help="canonical instrumented test-runner")
    parser.add_argument("--llvm-profdata", required=True, help="matching LLVM tool")
    parser.add_argument("--output-dir", required=True, help="new owned evidence directory")
    args = parser.parse_args()
    try:
        runner = identity(args.runner)
        tool = identity(args.llvm_profdata)
        root = Path(args.output_dir).resolve()
        root.mkdir(mode=0o700)
    except (OSError, ValueError) as exc:
        print("INFRASTRUCTURE ERROR: " + str(exc), file=sys.stderr)
        return 2
    receipt = {"schema": 1, "fixed_oracle": True, "selector": SELECTOR,
               "deadline_seconds": DEADLINE_SECONDS, "reap_grace_seconds": REAP_SECONDS,
               "log_bytes_per_stream": LOG_BYTES, "runner": runner, "llvm_profdata": tool,
               "cwd": str(Path.cwd()), "cases": [], "overall": "infrastructure_error"}
    try:
        for mode in ("success", "begin", "end"):
            result = run_case(root, mode, runner["path"], tool["path"])
            receipt["cases"].append(result)
            print(mode + ": " + result["classification"])
            if result["classification"] in ("infrastructure_error", "unknown_error_precondition"):
                break
        unchanged = identity(runner["path"]) == runner and identity(tool["path"]) == tool
        if not unchanged:
            receipt["error"] = "runner or llvm-profdata changed during proof"
        classes = [case["classification"] for case in receipt["cases"]]
        if unchanged and len(classes) == 3 and all(item in ("pass", "regression_failure") for item in classes):
            receipt["overall"] = "regression_failure" if "regression_failure" in classes else "pass"
    except (OSError, ValueError, KeyboardInterrupt) as exc:
        receipt["error"] = type(exc).__name__ + ": " + str(exc)
    finally:
        save_json(root / "result.json", receipt)
    return {"pass": 0, "regression_failure": 1, "infrastructure_error": 2}[receipt["overall"]]


if __name__ == "__main__":
    sys.exit(main())
