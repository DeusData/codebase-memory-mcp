#!/usr/bin/env python3
"""Fixed native transport and administrative rejection tests for coverage --format 2.

Exit 0: all assertions passed. Exit 1: regression. Exit 2: unsupported venue,
missing dependency, failed fixed native-fault precondition, or custody failure.
No project module is imported; the collector is exercised as a command boundary.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import sys

sys.dont_write_bytecode = True

from coverage_live_v2_admin import (bounds, converter_errors, existing_output,
                                     image_mismatch, invalid_registry)
from coverage_live_v2_native import (SUITE, TESTS, dependency_controls, failed_native,
                                    positive, read)
from coverage_live_v2_support import (STOP, Infrastructure, Processes, Regression, clean_env,
                                      digest, identity, require, write_json)


class Context:
    def __init__(self, args, output):
        self.output = output
        self.collector_identity = identity(args.collector)
        self.runner_identity = identity(args.runner, executable=True)
        self.tool_identity = identity(args.llvm_profdata, executable=True)
        self.collector = self.collector_identity["path"]
        self.runner = self.runner_identity["path"]
        self.tool = self.tool_identity["path"]
        self.processes = Processes(output / "commands")
        self.universe = None
        self.owned_scripts = [identity(Path(__file__).with_name(name)) for name in
                              ("test_coverage_live_v2.py", "coverage_live_v2_support.py",
                               "coverage_live_v2_native.py", "coverage_live_v2_admin.py",
                               "coverage_live_v2_fault.py")]

    def registry(self, case, tests=None, image=None):
        path = case / "registry.json"
        write_json(path, {"format": "cbm.coverage.registry.v0",
                          "image_sha256": image or self.runner_identity["sha256"],
                          "suites": [{"name": SUITE, "tests": tests if tests is not None else TESTS}]})
        return path

    def collect(self, case, registry, runner=None, destination=None, llvm_bin=None,
                extra=None, poison=False, fault=False):
        destination = destination or (case / "bundle")
        runner = str(runner or self.runner)
        arguments = ["--format", "2", "--runner", runner, "--registry", str(registry),
                     "--out", str(destination), "--llvm-bin", str(llvm_bin or Path(self.tool).parent),
                     "--timeout", "70"] + (extra or [])
        argv = [sys.executable, self.collector] + arguments
        if fault:
            adapter = str(Path(__file__).with_name("coverage_live_v2_fault.py"))
            argv = [sys.executable, adapter, self.collector, self.runner, SUITE,
                    str(case / "native-fault.json")] + arguments
        env = clean_env()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["LLVM_PROFILE_FILE"] = os.devnull
        sentinel = None
        if poison:
            poison_root = case / "caller-coverage"
            poison_root.mkdir()
            sentinel = poison_root / "sentinel"
            sentinel.write_bytes(b"caller coverage is not the collection destination\n")
            only_file = case / "caller-selection.txt"
            only_file.write_text("nonexistent_suite:nonexistent_test\n", encoding="utf-8")
            env.update(CBM_TEST_COVERAGE_DIR=str(poison_root),
                       CBM_TEST_ONLY="nonexistent_suite:nonexistent_test", CBM_TEST_ONLY_FILE=str(only_file))
        process = self.processes.run(case.name, argv, env, timeout=600)
        if sentinel is not None:
            require(list(sentinel.parent.iterdir()) == [sentinel]
                    and sentinel.read_bytes() == b"caller coverage is not the collection destination\n",
                    "collector used or changed inherited caller coverage directory")
        return process, destination


def fresh_output(value):
    requested = Path(value).absolute()
    if os.path.lexists(requested):
        raise Infrastructure("test output already exists; preserving caller data")
    parent = requested.parent.resolve(strict=True)
    if not parent.is_dir():
        raise Infrastructure("test output parent is not a directory")
    result = parent / requested.name
    if any(character in str(result) for character in ("\n", "\r", "%")):
        raise Infrastructure("unsupported LLVM evidence path characters")
    result.mkdir(mode=0o700)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--llvm-profdata", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--case", choices=("native-positive", "native-missing-row",
        "native-failed-process", "image-mismatch", "existing-output", "output-symlink",
        "invalid-registry", "resource-bounds", "converter-errors"),
        help="run one case with the same native dependency controls")
    args = parser.parse_args()
    if os.name != "posix":
        print("unsupported: this driver requires POSIX owned process groups", file=sys.stderr)
        return 2
    def interrupted(signum, frame):
        STOP.set()
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    output, context = None, None
    receipt = {"schema": "cbm.coverage.live-v2-regression.v0", "overall": "infrastructure_failure",
               "dependency_controls_passed": False, "cases": [], "no_completeness_claim": True,
               "fault_policy": "fixed RLIMIT_NOFILE=7; no tuning or retries",
               "scope": "live native transport; administrative rejection; no authenticated admission"}
    code = 2
    try:
        output = fresh_output(args.output)
        context = Context(args, output)
        receipt["inputs"] = {"collector": context.collector_identity,
                             "runner": context.runner_identity, "llvm_profdata": context.tool_identity,
                             "tests": context.owned_scripts}
        receipt["preserved_cbm_skip_perf"] = os.environ.get("CBM_SKIP_PERF")
        version = context.processes.run("native-tool-version", [context.tool, "--version"], clean_env(), 30)
        require(version["exit"] == 0 and b"22.1.8" in read(version["stdout"]),
                "unsupported proftext tool version: this test contract pins LLVM22.1.8")
        try:
            context.universe, receipt["native_controls"] = dependency_controls(context)
        except Regression as exc:
            raise Infrastructure("native dependency control failed: " + str(exc)) from exc
        receipt["dependency_controls_passed"] = True
        receipt["selected_case"] = args.case or "all"
        definitions = [
            ("native-positive", lambda case: positive(context, case)),
            ("native-missing-row", lambda case: positive(context, case, missing=True)),
            ("native-failed-process", lambda case: failed_native(context, case)),
            ("image-mismatch", lambda case: image_mismatch(context, case)),
            ("existing-output", lambda case: existing_output(context, case)),
            ("output-symlink", lambda case: existing_output(context, case, symlink=True)),
            ("invalid-registry", lambda case: invalid_registry(context, case)),
            ("resource-bounds", lambda case: bounds(context, case)),
            ("converter-errors", lambda case: converter_errors(context, case)),
        ]
        for name, check in definitions:
            if args.case is not None and name != args.case:
                continue
            case = output / name
            case.mkdir()
            result = {"case": name}
            try:
                result.update(status="pass", evidence=check(case))
            except Regression as exc:
                result.update(status="regression_failure", error=str(exc))
            except (Infrastructure, OSError, ValueError) as exc:
                result.update(status="infrastructure_failure", error=str(exc))
                receipt["cases"].append(result)
                write_json(case / "result.json", result)
                raise Infrastructure(name + ": " + str(exc)) from exc
            receipt["cases"].append(result)
            write_json(case / "result.json", result)
        code = 1 if any(case["status"] != "pass" for case in receipt["cases"]) else 0
        receipt["overall"] = "regression_failure" if code else "pass"
    except (Infrastructure, OSError, ValueError, Regression, KeyboardInterrupt) as exc:
        receipt["error"] = type(exc).__name__ + ": " + str(exc)
        code = 2
    finally:
        if context is not None:
            receipt["processes"] = context.processes.records
            checks = [context.collector_identity, context.runner_identity, context.tool_identity] + context.owned_scripts
            unchanged = []
            for item in checks:
                try:
                    unchanged.append(digest(item["path"]) == item["sha256"])
                except OSError:
                    unchanged.append(False)
            receipt["input_identities_unchanged"] = all(unchanged)
            if not all(unchanged):
                receipt["overall"] = "infrastructure_failure"
                receipt["error"] = "test or native/collector input changed during run"
                code = 2
        if output is not None:
            write_json(output / "result.json", receipt)
            print(json.dumps({"overall": receipt["overall"], "receipt": str(output / "result.json")}, sort_keys=True))
        else:
            print(receipt.get("error", "unsupported test output"), file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())
