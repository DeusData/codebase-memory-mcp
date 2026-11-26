#!/usr/bin/env python3
"""Six genuine canonical-runner child-coverage cases; one oracle in every phase."""
import argparse
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import threading
import time

DEADLINE = 60
GRACE = 5
LOG_CAP = 1024 * 1024
ANSI = re.compile(r"\x1b\[[0-9;]*m")
COUNT_PREFIX = "cbm_cov_child_count_"
PROCESS_RECORDS = []
MATRIX = (
    ("explicit_success", "coverage_children", ["coverage_explicit_success"], ["lower", "upper"]),
    ("explicit_write_failure", "coverage_children", ["coverage_explicit_write_failure"], ["lower", "upper"]),
    ("normal_exit", "coverage_children", ["coverage_normal_exit"], ["normal_ok", "normal_bad"]),
    ("exec_wrappers", "coverage_children", ["coverage_exec_wrappers"],
     ["execve", "execv", "execvp", "execl", "failed_exec"]),
    ("setup_success", "coverage_setup_success", ["coverage_setup_first", "coverage_setup_second"], ["setup_ok"]),
    ("setup_uncertain", "coverage_setup_uncertain", ["coverage_setup_first", "coverage_setup_second"], ["setup_killed"]),
)


class Infrastructure(Exception):
    pass


def save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def identity(path, executable=False):
    path = Path(path).resolve(strict=True)
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or (executable and not os.access(str(path), os.X_OK)):
        raise Infrastructure("invalid input file: " + str(path))
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(data)
    return {"path": str(path), "bytes": info.st_size, "sha256": digest.hexdigest()}


def drain(pipe, destination, state, stop):
    try:
        with destination.open("xb", buffering=0) as stream:
            while True:
                data = pipe.read(4096)
                if not data:
                    break
                state["seen"] += len(data)
                part = memoryview(data)[:max(0, LOG_CAP - state["saved"])]
                while part:
                    size = stream.write(part)
                    if not size:
                        raise OSError("short evidence write")
                    state["saved"] += size
                    part = part[size:]
                if state["seen"] > LOG_CAP:
                    state["overflow"] = True
                    stop.set()
    except (OSError, ValueError) as exc:
        state["error"] = str(exc)
        stop.set()
    finally:
        pipe.close()


def group_exists(pid):
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False


def signal_group(pid, sig):
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def quiesce(proc):
    # Every spawned process is the leader of its own new session/group. Nested
    # reducer tools inherit that group. C fixtures separately prove native reaps.
    notes = []
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if proc.poll() is not None and not group_exists(proc.pid):
            return True, notes
        signal_group(proc.pid, sig)
        notes.append("sent " + signal.Signals(sig).name)
        until = time.monotonic() + GRACE
        while time.monotonic() < until:
            if proc.poll() is not None and not group_exists(proc.pid):
                return True, notes
            time.sleep(min(0.05, max(0, until - time.monotonic())))
    return proc.poll() is not None and not group_exists(proc.pid), notes


def run_process(directory, label, argv, environment):
    record = {"argv": argv, "deadline_seconds": DEADLINE, "returncode": None,
              "failure": None, "quiescent": False,
              "stdout": label + ".stdout.log", "stderr": label + ".stderr.log"}
    start = time.monotonic()
    proc = None
    threads, started_pipes, states = [], [], {}
    stop = threading.Event()
    try:
        proc = subprocess.Popen(argv, env=environment, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                bufsize=0, start_new_session=True)
        record["pid"] = proc.pid
        for kind, pipe in (("stdout", proc.stdout), ("stderr", proc.stderr)):
            state = {"seen": 0, "saved": 0, "overflow": False, "error": None}
            thread = threading.Thread(target=drain,
                                      args=(pipe, directory / record[kind], state, stop), daemon=True)
            thread.start()
            threads.append(thread)
            started_pipes.append(pipe)
            states[kind] = state
        while proc.poll() is None:
            left = DEADLINE - (time.monotonic() - start)
            if stop.is_set() or left <= 0:
                record["failure"] = "capture limit/error" if stop.is_set() else "timeout"
                break
            try:
                proc.wait(timeout=min(left, 0.1))
            except subprocess.TimeoutExpired:
                pass
    except (OSError, RuntimeError, KeyboardInterrupt) as exc:
        record["failure"] = type(exc).__name__ + ": " + str(exc)
    finally:
        if proc is not None:
            try:
                record["quiescent"], record["cleanup"] = quiesce(proc)
                if record["cleanup"] and record["failure"] is None:
                    record["failure"] = "unexpected processes remained after leader exit"
            except (OSError, KeyboardInterrupt) as exc:
                record["cleanup_error"] = str(exc)
            if not record["quiescent"]:
                record["failure"] = record["failure"] or "owned process group did not quiesce"
            for pipe in (proc.stdout, proc.stderr):
                if pipe not in started_pipes:
                    pipe.close()
            until = time.monotonic() + GRACE
            for thread in threads:
                thread.join(timeout=max(0, until - time.monotonic()))
            if any(thread.is_alive() for thread in threads):
                record["failure"] = record["failure"] or "pipe readers did not quiesce"
            record["returncode"] = proc.poll()
        if any(state["overflow"] or state["error"] for state in states.values()):
            record["failure"] = record["failure"] or "bounded capture failed"
        record["streams"] = states
        record["elapsed_seconds"] = round(time.monotonic() - start, 3)
        PROCESS_RECORDS.append(record)
        save(directory / (label + ".process.json"), record)
    return record


def require_process(record):
    if record["failure"] or not record["quiescent"] or record["returncode"] != 0:
        raise Infrastructure("process failed: " + json.dumps(record))


def regular_nonempty(path):
    try:
        value = path.lstat()
        return stat.S_ISREG(value.st_mode) and value.st_size > 0
    except OSError:
        return False


def read_log(directory, record, kind):
    return (directory / record[kind]).read_text(encoding="utf-8", errors="replace")


def validate_test_output(stdout, tests):
    clean = ANSI.sub("", stdout)
    rows = re.findall(r"(?m)^  ([A-Za-z_][A-Za-z0-9_]*)[ \t]+(PASS|FAIL|SKIP)\b", clean)
    starts = re.findall(r"(?m)^  (coverage_[A-Za-z0-9_]+)\b", clean)
    summaries = re.findall(r"(?m)^[ \t]*(\d+) passed(?:, (\d+) failed)?(?:, (\d+) skipped)?"
                           r"(?:, (\d+) deselected)?[ \t]*$", clean)
    if rows != [(name, "PASS") for name in tests] or starts != tests or len(summaries) != 1:
        raise Infrastructure("selected native tests did not run exactly once and pass")
    if [int(value or 0) for value in summaries[0]][:3] != [len(tests), 0, 0]:
        raise Infrastructure("native suite summary failed")


def parse_events(stderr, ids):
    events = {name: [] for name in ids}
    for line in stderr.splitlines():
        if not line.startswith("COV_CHILD "):
            continue
        try:
            item = json.loads(line[len("COV_CHILD "):])
        except ValueError as exc:
            raise Infrastructure("invalid fixture event") from exc
        if (not isinstance(item, dict) or set(item) != {"id", "pid", "stage", "marker", "code"}
                or not isinstance(item["id"], str) or item["id"] not in events
                or type(item["pid"]) is not int or item["pid"] <= 0
                or type(item["marker"]) is not int or item["marker"] not in (-1, 0, 1)
                or type(item["code"]) is not int or not isinstance(item["stage"], str)):
            raise Infrastructure("invalid fixture event fields")
        events[item["id"]].append(item)
    return events


def validate_events(case_name, events):
    pids, final = set(), {}
    for name, rows in events.items():
        obstacle = case_name == "explicit_write_failure" or name == "normal_bad"
        stages = ["ready"]
        if obstacle:
            stages += ["obstacle_held", "obstacle_reaped", "obstacle_removed"]
        if name in ("execve", "execv", "execvp", "execl"):
            stages += ["exec_held"]
        if name == "failed_exec":
            stages += ["execve_failed", "execv_failed", "execvp_failed", "execl_failed"]
        stages += ["reaped"]
        if [row["stage"] for row in rows] != stages:
            raise Infrastructure("missing, duplicate, or unordered child events: " + name)
        pid = rows[0]["pid"]
        if pid in pids or any(row["pid"] != pid for row in rows):
            raise Infrastructure("child PID evidence inconsistent")
        pids.add(pid)
        if rows[0]["marker"] != 1 or rows[0]["code"] != 0:
            raise Infrastructure("post-atfork initial marker was not observed")
        expected_status = 128 + signal.SIGKILL if name == "setup_killed" else 0
        if rows[-1]["code"] != expected_status or rows[-1]["marker"] < 0:
            raise Infrastructure("child was not reaped with its expected status")
        for row in rows[1:-1]:
            if row["stage"].startswith("obstacle_"):
                good = row["code"] == 1 and row["marker"] == -1
            else:
                good = row["marker"] >= 0 and row["code"] == (errno.ENOENT if name == "failed_exec" else 0)
            if not good:
                raise Infrastructure("child gate/obstacle control failed")
        final[name] = rows[-1]
    return final


def validate_native_errors(stderr, expected_paths):
    found = set()
    for line in stderr.splitlines():
        if line.startswith("coverage child fixture failed:") or line.startswith("coverage profile write failed:"):
            raise Infrastructure("native fixture or normal-parent coverage failed")
        if line.startswith("LLVM Profile Error:"):
            match = re.fullmatch(r'LLVM Profile Error: Failed to write file "(.*)": (.+)', line)
            if not match:
                raise Infrastructure("unknown LLVM diagnostic precondition")
            path = os.path.normpath(match.group(1))
            if path not in expected_paths:
                raise Infrastructure("LLVM write failure did not name an owned obstacle")
            found.add(path)
    if found != expected_paths:
        raise Infrastructure("intended genuine LLVM write failure was not observed")


def exact_positive_count(text, function):
    blocks = []
    current = None
    for line in text.splitlines():
        heading = re.fullmatch(r"  (.+):", line)
        if heading:
            current = {"name": heading.group(1), "counts": []}
            blocks.append(current)
        elif current is not None:
            count = re.fullmatch(r"    Function count: ([0-9]+)", line)
            if count:
                current["counts"].append(int(count.group(1)))
    exact = [block for block in blocks if block["name"] == function]
    if len(exact) != 1 or len(exact[0]["counts"]) != 1 or exact[0]["counts"][0] <= 0:
        raise Infrastructure("exact child-only function lacks one positive native count: " + function)
    return exact[0]["counts"][0]


def profile_controls(case, suite_dir, tool, environment, witnesses):
    profiles = sorted(suite_dir.glob("*.profraw"))
    if not profiles or len(profiles) > 64:
        raise Infrastructure("unexpected finite profile inventory")
    results = []
    for index, raw in enumerate(profiles):
        if not regular_nonempty(raw):
            raise Infrastructure("profile is not regular/nonempty: " + str(raw))
        merged = case / ("profile-%02d.profdata" % index)
        record = run_process(case, "merge-%02d" % index,
                             [tool, "merge", "--instr", "--output=" + str(merged), str(raw)], environment)
        require_process(record)
        if not regular_nonempty(merged):
            raise Infrastructure("matching profdata produced no valid indexed profile")
        item = {"raw": str(raw), "merged": str(merged), "valid": True}
        if raw.name in witnesses:
            function = witnesses[raw.name]
            shown = run_process(case, "count-%02d" % index,
                                [tool, "show", "--counts", "--function=" + function, str(merged)], environment)
            require_process(shown)
            item["function"] = function
            item["function_count"] = exact_positive_count(read_log(case, shown, "stdout"), function)
        results.append(item)
    if set(witnesses) - {Path(item["raw"]).name for item in results}:
        raise Infrastructure("expected genuine child profile is absent")
    return results


def reducer_worker():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reduce-worker", action="store_true", required=True)
    parser.add_argument("--reducer", required=True)
    parser.add_argument("--suite", required=True)
    parser.add_argument("--suite-dir", required=True)
    parser.add_argument("--profdata", required=True)
    parser.add_argument("--runner-returncode", required=True, type=int)
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location("coverage_child_actual_reducer", args.reducer)
    if spec is None or spec.loader is None:
        raise Infrastructure("cannot load actual reducer")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.reduce_suite(args.suite, args.suite_dir, args.profdata, args.runner_returncode == 0)
    rows = {name: {"status": row[0], "reason": row[1], "names_count": len(row[2])}
            for name, row in result.items()}
    print(json.dumps(rows, sort_keys=True))
    return 0


def reduce_actual(case, suite, suite_dir, tool, reducer, runner_record, environment):
    argv = [sys.executable, str(Path(__file__).resolve()), "--reduce-worker",
            "--reducer", reducer, "--suite", suite, "--suite-dir", str(suite_dir),
            "--profdata", tool, "--runner-returncode", str(runner_record["returncode"])]
    record = run_process(case, "reduce", argv, environment)
    require_process(record)
    try:
        rows = json.loads(read_log(case, record, "stdout"))
    except ValueError as exc:
        raise Infrastructure("actual reducer did not return valid JSON") from exc
    if not isinstance(rows, dict) or len(rows) > 16:
        raise Infrastructure("unexpected reducer row inventory")
    for row in rows.values():
        if (not isinstance(row, dict) or set(row) != {"status", "reason", "names_count"}
                or row["status"] not in ("complete", "incomplete") or not isinstance(row["reason"], str)
                or type(row["names_count"]) is not int or row["names_count"] < 0):
            raise Infrastructure("invalid actual reducer row")
    return rows


def features(case_name, tests, events, final, rows):
    problems = []
    retained = []
    for name, row in final.items():
        expected = case_name in ("explicit_write_failure", "normal_exit", "setup_uncertain")
        expected = expected or (case_name == "exec_wrappers" and name != "failed_exec")
        if row["marker"] != int(expected):
            problems.append(name + ": final marker retirement incorrect")
        if expected:
            retained.append(str(row["pid"]))
        for held in events[name]:
            if held["stage"] == "exec_held" or held["stage"].endswith("_failed"):
                if held["marker"] != 1:
                    problems.append(name + ": marker absent at " + held["stage"])
    expected_keys = set(tests) | {"*"}
    if set(rows) != expected_keys:
        problems.append("actual reducer emitted missing or synthetic runnable rows")
    uncertain_key = "*" if case_name == "setup_uncertain" else tests[0]
    reason = "unaccounted forked child: " + ",".join(sorted(retained)) if retained else ""
    for key in expected_keys:
        expected_reason = reason if key == uncertain_key else ""
        status = "incomplete" if expected_reason else "complete"
        row = rows.get(key)
        if row is None or row["status"] != status or row["reason"] != expected_reason:
            problems.append("actual reducer status/reason mismatch: " + key)
    return problems


def run_case(root, definition, runner, tool, reducer):
    name, suite, tests, ids = definition
    case = root / name
    case.mkdir(mode=0o700)
    coverage = case / "coverage"
    coverage.mkdir(mode=0o700)
    suite_dir = coverage / suite
    result = {"case": name, "suite": suite, "tests": tests, "controls_passed": False}
    environment = os.environ.copy()
    environment["LLVM_PROFILE_FILE"] = "/dev/null"
    environment["CBM_TEST_COVERAGE_DIR"] = str(coverage)
    environment["CBM_TEST_ONLY"] = ",".join(suite + ":" + test for test in tests)
    environment.pop("CBM_TEST_ONLY_FILE", None)
    try:
        record = run_process(case, "runner", [runner, suite], environment)
        result["runner"] = record
        require_process(record)
        stdout, stderr = read_log(case, record, "stdout"), read_log(case, record, "stderr")
        validate_test_output(stdout, tests)
        events = parse_events(stderr, ids)
        final = validate_events(name, events)
        result["events"] = events
        witnesses, obstacles = {}, set()
        stem = "_setup.child" if name.startswith("setup_") else tests[0]
        suffixes = {"lower": "lower", "upper": "upper", "normal_ok": "normal",
                    "failed_exec": "failed_exec", "setup_ok": "setup"}
        for child_id, event in final.items():
            raw = suite_dir / (stem + "." + str(event["pid"]) + ".profraw")
            marker = raw.with_suffix(".forked")
            present = marker.exists()
            if present != bool(event["marker"]) or (present and not stat.S_ISREG(marker.lstat().st_mode)):
                raise Infrastructure("recorded final marker differs from completed filesystem")
            obstructed = name == "explicit_write_failure" or child_id == "normal_bad"
            if obstructed:
                obstacles.add(os.path.normpath(str(raw)))
                if os.path.lexists(str(raw)):
                    raise Infrastructure("owned empty obstruction was not removed after reap")
            elif child_id == "setup_killed":
                if os.path.lexists(str(raw)):
                    raise Infrastructure("deliberately killed held child unexpectedly wrote a profile")
            elif child_id in suffixes:
                witnesses[raw.name] = COUNT_PREFIX + suffixes[child_id]
        validate_native_errors(stderr, obstacles)
        for test in tests:
            if not regular_nonempty(suite_dir / (test + ".parent.profraw")):
                raise Infrastructure("passing selected test lacks its genuine parent profile")
        result["profiles"] = profile_controls(case, suite_dir, tool, environment, witnesses)
        result["rows"] = reduce_actual(case, suite, suite_dir, tool, reducer, record, environment)
        result["feature_problems"] = features(name, tests, events, final, result["rows"])
        if name in ("explicit_success", "setup_success") and result["feature_problems"]:
            raise Infrastructure("positive retirement/reducer control failed")
        result["controls_passed"] = True
    except (Infrastructure, OSError, ValueError, KeyboardInterrupt) as exc:
        result["infrastructure_error"] = type(exc).__name__ + ": " + str(exc)
    finally:
        save(case / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--llvm-profdata", required=True)
    parser.add_argument("--reducer", required=True)
    parser.add_argument("--output-dir", required=True, help="new owned evidence directory")
    args = parser.parse_args()
    if os.name != "posix":
        print("INFRASTRUCTURE: this fixture requires the frozen POSIX coverage boundary", file=sys.stderr)
        return 2
    try:
        inputs = {"runner": identity(args.runner, True), "profdata": identity(args.llvm_profdata, True),
                  "reducer": identity(args.reducer), "driver": identity(__file__)}
        root = Path(args.output_dir).resolve()
        if any(char in str(root) for char in ("%", "\n", "\r")):
            raise Infrastructure("output path must not contain LLVM pattern tokens or newlines")
        root.mkdir(mode=0o700)
    except (OSError, Infrastructure) as exc:
        print("INFRASTRUCTURE: " + str(exc), file=sys.stderr)
        return 2
    receipt = {"schema": 1, "fixed_oracle": True, "runner_invocations": 6,
               "deadline_seconds": DEADLINE, "termination_grace_seconds": GRACE,
               "log_bytes_per_stream": LOG_CAP, "inputs": inputs, "cases": [],
               "overall": "infrastructure_error",
               "capture_limit": "driver streams only; existing reducer internal capture is unchanged"}
    try:
        for definition in MATRIX:
            result = run_case(root, definition, inputs["runner"]["path"], inputs["profdata"]["path"],
                              inputs["reducer"]["path"])
            receipt["cases"].append(result)
            if not result["controls_passed"]:
                break
        for name, value in inputs.items():
            if identity(value["path"], name in ("runner", "profdata")) != value:
                raise Infrastructure("proof input changed: " + name)
        if len(receipt["cases"]) == 6 and all(case["controls_passed"] for case in receipt["cases"]):
            failed = False
            for case in receipt["cases"]:
                case["classification"] = "regression_failure" if case["feature_problems"] else "pass"
                failed = failed or bool(case["feature_problems"])
                print(case["case"] + ": " + case["classification"])
            receipt["overall"] = "regression_failure" if failed else "pass"
    except (Infrastructure, OSError, ValueError, KeyboardInterrupt) as exc:
        receipt["error"] = type(exc).__name__ + ": " + str(exc)
    finally:
        receipt["processes"] = PROCESS_RECORDS
        receipt["all_owned_groups_quiescent"] = all(record["quiescent"] for record in PROCESS_RECORDS
                                                     if "pid" in record)
        commands = {(inputs["runner"]["path"], definition[1]) for definition in MATRIX}
        receipt["runner_invocations_actual"] = sum(tuple(record["argv"]) in commands
                                                   for record in PROCESS_RECORDS)
        save(root / "result.json", receipt)
    return {"pass": 0, "regression_failure": 1, "infrastructure_error": 2}[receipt["overall"]]


if __name__ == "__main__":
    sys.exit(reducer_worker() if "--reduce-worker" in sys.argv[1:] else main())
