#!/usr/bin/env python3
"""Two native marker-open controls; unchanged oracle for baseline/fix/revert."""
import argparse
import errno
import json
import os
from pathlib import Path
import re
import sys

# Checked-in sibling, not scratch: bounded groups/capture and the actual reducer
# worker are shared with the six-case child proof. Its __main__ guard is inert.
sys.dont_write_bytecode = True
import test_coverage_children as common

SUITE = "coverage_parent_failure"
TESTS = ["coverage_marker_creation", "coverage_marker_sentinel"]
FUNCTION = "cbm_cov_parent_failure_child_count"
OPTION = "--coverage-marker-open-emfile"
FIELDS = {"mode", "stage", "owner_pid", "child_pid", "control_errno", "marker", "exit_code",
          "saved_soft", "saved_hard", "parent_soft", "parent_hard", "child_seen_soft",
          "child_seen_hard", "child_restored_soft", "child_restored_hard"}


def output_controls(stdout, code):
    clean = common.ANSI.sub("", stdout)
    rows = re.findall(r"(?m)^  ([A-Za-z_][A-Za-z0-9_]*)[ \t]+(PASS|FAIL|SKIP)\b", clean)
    starts = re.findall(r"(?m)^  (coverage_[A-Za-z0-9_]+)\b", clean)
    summaries = re.findall(r"(?m)^[ \t]*(\d+) passed(?:, (\d+) failed)?(?:, (\d+) skipped)?"
                           r"(?:, (\d+) deselected)?[ \t]*$", clean)
    if rows != [(name, "PASS") for name in TESTS] or starts != TESTS or len(summaries) != 1:
        raise common.Infrastructure("native fixture and later sentinel must both PASS exactly once")
    counts = [int(value or 0) for value in summaries[0]]
    if code not in (0, 1) or counts[:3] != [2, code, 0]:
        raise common.Infrastructure("unrelated native failure or unexpected report-once summary")
    return counts


def event_controls(stderr, mode, owner_pid):
    rows = []
    for line in stderr.splitlines():
        if not line.startswith("COV_PARENT "):
            continue
        try:
            row = json.loads(line[len("COV_PARENT "):])
        except ValueError as exc:
            raise common.Infrastructure("invalid fixture receipt") from exc
        if not isinstance(row, dict) or set(row) != FIELDS:
            raise common.Infrastructure("wrong fixture receipt schema")
        if row["mode"] != mode or not isinstance(row["stage"], str):
            raise common.Infrastructure("wrong or missing fixture mode")
        if any(type(value) is not int for key, value in row.items() if key not in ("mode", "stage")):
            raise common.Infrastructure("noninteger native receipt")
        rows.append(row)
    if len(rows) != 2 or [row["stage"] for row in rows] != ["held", "reaped"]:
        raise common.Infrastructure("missing, duplicate or unordered held/reaped receipts")
    held, reaped = rows
    if held["owner_pid"] != owner_pid or held["child_pid"] <= 0 or held["child_pid"] == owner_pid:
        raise common.Infrastructure("receipt is not from the original parent/owned child")
    for key in FIELDS - {"stage", "marker", "exit_code"}:
        if held[key] != reaped[key]:
            raise common.Infrastructure("child identity or limit receipt changed")
    if held["marker"] != (0 if mode == "emfile" else 1) or held["exit_code"] != -1:
        raise common.Infrastructure("wrong marker state before explicit exit")
    if reaped["marker"] != 0 or reaped["exit_code"] != 0:
        raise common.Infrastructure("child did not reap successfully with retired/absent marker")
    if held["saved_soft"] <= 0 or held["saved_hard"] < held["saved_soft"]:
        raise common.Infrastructure("invalid saved limits")
    pairs = (("parent_soft", "saved_soft"), ("parent_hard", "saved_hard"),
             ("child_seen_hard", "saved_hard"), ("child_restored_soft", "saved_soft"),
             ("child_restored_hard", "saved_hard"))
    if any(held[left] != held[right] for left, right in pairs):
        raise common.Infrastructure("parent or child did not restore exact original soft/hard limits")
    expected_soft = 0 if mode == "emfile" else held["saved_soft"]
    if held["child_seen_soft"] != expected_soft or held["control_errno"] != (errno.EMFILE if mode == "emfile" else 0):
        raise common.Infrastructure("native open restriction was not exactly established")
    return rows


def accounting_controls(stderr, owner_pid):
    diagnostics = []
    for line in stderr.splitlines():
        if (line.startswith("LLVM Profile Error:") or line.startswith("coverage profile write failed:")
                or line.startswith("coverage parent fixture failed:")
                or line.startswith("coverage parent fixture usage error:")
                or line.startswith("coverage child accounting unavailable:")):
            raise common.Infrastructure("unrelated writer/fixture/activation failure: " + line)
        if line.startswith("coverage child accounting failed:"):
            expected = "coverage child accounting failed: owner_pid=%d reason=child-marker" % owner_pid
            if line != expected:
                raise common.Infrastructure("accounting diagnostic is not the original parent's pending marker event")
            diagnostics.append(line)
    if len(diagnostics) > 1:
        raise common.Infrastructure("accounting error was not reported exactly once")
    return diagnostics


def complete_rows(rows):
    return set(rows) == set(TESTS) | {"*"} and all(
        row["status"] == "complete" and row["reason"] == "" for row in rows.values())


def verdict(mode, code, diagnostics, rows):
    if mode == "control":
        problems = []
        if code != 0 or diagnostics:
            problems.append("unrestricted positive child caused parent accounting failure")
        if not complete_rows(rows):
            problems.append("unrestricted positive reducer rows were not complete")
        return problems, False
    if code == 0:
        if diagnostics or not complete_rows(rows):
            raise common.Infrastructure("zero-status restricted run did not demonstrate the frozen baseline false success")
        return ["valid native marker-open failure was silently accepted by original parent"], True
    if len(diagnostics) != 1:
        raise common.Infrastructure("nonzero parent status lacks exactly one original-parent accounting diagnostic")
    required = {TESTS[0], "*"}
    problems = []
    if not required <= set(rows) or not set(rows) <= set(TESTS) | {"*"}:
        problems.append("actual reducer omitted fixture/setup or emitted unrelated rows")
    for key, row in rows.items():
        if row["status"] != "incomplete" or "suite did not exit 0" not in row["reason"].split("; "):
            problems.append("actual reducer did not retain suite failure floor: " + key)
    return problems, False


def run_case(root, mode, inputs):
    case = root / mode
    case.mkdir(mode=0o700)
    coverage = case / "coverage"
    coverage.mkdir(mode=0o700)
    suite_dir = coverage / SUITE
    result = {"mode": mode, "controls_passed": False}
    environment = os.environ.copy()
    environment["LLVM_PROFILE_FILE"] = "/dev/null"
    environment["CBM_TEST_COVERAGE_DIR"] = str(coverage)
    environment["CBM_TEST_ONLY"] = ",".join(SUITE + ":" + test for test in TESTS)
    environment.pop("CBM_TEST_ONLY_FILE", None)
    argv = [inputs["runner"]["path"]] + ([OPTION] if mode == "emfile" else []) + [SUITE]
    try:
        process = common.run_process(case, "runner", argv, environment)
        result["runner"] = process
        if process["failure"] or not process["quiescent"] or process["returncode"] not in (0, 1):
            raise common.Infrastructure("runner infrastructure/usage/containment failed")
        code = process["returncode"]
        stdout = common.read_log(case, process, "stdout")
        stderr = common.read_log(case, process, "stderr")
        result["summary"] = output_controls(stdout, code)
        events = event_controls(stderr, mode, process["pid"])
        result["events"] = events
        diagnostics = accounting_controls(stderr, process["pid"])
        result["accounting_diagnostics"] = diagnostics
        child_pid = events[0]["child_pid"]
        raw = suite_dir / (TESTS[0] + "." + str(child_pid) + ".profraw")
        if os.path.lexists(str(raw.with_suffix(".forked"))):
            raise common.Infrastructure("final marker receipt differs from actual filesystem")
        if code == 0:
            for test in TESTS:
                if not common.regular_nonempty(suite_dir / (test + ".parent.profraw")):
                    raise common.Infrastructure("zero-status parent lacks its ordinary genuine profile")
        result["profiles"] = common.profile_controls(case, suite_dir, inputs["profdata"]["path"],
                                                     environment, {raw.name: FUNCTION})
        result["rows"] = common.reduce_actual(case, SUITE, suite_dir, inputs["profdata"]["path"],
                                              inputs["reducer"]["path"], process, environment)
        result["feature_problems"], result["baseline_false_success"] = verdict(mode, code, diagnostics, result["rows"])
        result["controls_passed"] = True
    except (common.Infrastructure, OSError, ValueError, KeyboardInterrupt) as exc:
        result["infrastructure_error"] = type(exc).__name__ + ": " + str(exc)
    finally:
        common.save(case / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--llvm-profdata", required=True)
    parser.add_argument("--reducer", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if os.name != "posix":
        print("INFRASTRUCTURE: native fixture requires the frozen POSIX boundary", file=sys.stderr)
        return 2
    try:
        inputs = {"runner": common.identity(args.runner, True),
                  "profdata": common.identity(args.llvm_profdata, True),
                  "reducer": common.identity(args.reducer), "driver": common.identity(__file__),
                  "shared_driver": common.identity(common.__file__)}
        expected_helper = Path(__file__).resolve().with_name("test_coverage_children.py")
        if Path(inputs["shared_driver"]["path"]) != expected_helper:
            raise common.Infrastructure("shared process helper is not the checked-in sibling")
        root = Path(args.output_dir).resolve()
        if any(char in str(root) for char in ("%", "\n", "\r")):
            raise common.Infrastructure("output path contains LLVM pattern tokens/newlines")
        root.mkdir(mode=0o700)
    except (OSError, common.Infrastructure) as exc:
        print("INFRASTRUCTURE: " + str(exc), file=sys.stderr)
        return 2
    receipt = {"schema": 1, "fixed_oracle": True, "runner_invocations": 2,
               "inputs": inputs, "cases": [], "overall": "infrastructure_error",
               "deadline_seconds": common.DEADLINE, "grace_seconds_per_phase": common.GRACE,
               "log_bytes_per_stream": common.LOG_CAP, "stdin": "DEVNULL",
               "capture_limit": "external streams only; reducer internal capture unchanged"}
    try:
        for mode in ("control", "emfile"):
            case = run_case(root, mode, inputs)
            receipt["cases"].append(case)
            if not case["controls_passed"]:
                break
        for name, value in inputs.items():
            if common.identity(value["path"], name in ("runner", "profdata")) != value:
                raise common.Infrastructure("proof input changed: " + name)
        if len(receipt["cases"]) == 2 and all(case["controls_passed"] for case in receipt["cases"]):
            failed = False
            for case in receipt["cases"]:
                case["classification"] = "regression_failure" if case["feature_problems"] else "pass"
                failed = failed or bool(case["feature_problems"])
                print(case["mode"] + ": " + case["classification"])
            receipt["overall"] = "regression_failure" if failed else "pass"
    except (common.Infrastructure, OSError, ValueError, KeyboardInterrupt) as exc:
        receipt["error"] = type(exc).__name__ + ": " + str(exc)
    finally:
        receipt["processes"] = common.PROCESS_RECORDS
        receipt["all_owned_groups_quiescent"] = all(record["quiescent"] for record in common.PROCESS_RECORDS
                                                     if "pid" in record)
        commands = {(inputs["runner"]["path"], SUITE), (inputs["runner"]["path"], OPTION, SUITE)}
        receipt["runner_invocations_actual"] = sum(tuple(record["argv"]) in commands
                                                   for record in common.PROCESS_RECORDS)
        common.save(root / "result.json", receipt)
    return {"pass": 0, "regression_failure": 1, "infrastructure_error": 2}[receipt["overall"]]


if __name__ == "__main__":
    sys.dont_write_bytecode = True
    sys.exit(main())
