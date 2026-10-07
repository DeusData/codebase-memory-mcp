#!/usr/bin/env python3
"""One fixed native terminal-write oracle; no baseline-pass mode or retries."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys

HELPER_SHA = "a807273c03a001f713cc9425779c3e8e423eae812e0fc73fb35a6f38fc537f1e"
REDUCER_SHA = "503ba9c3be9671761c505d7819e1f638de006f30c321f98e7ad0c53a23ac224a"
SUITE = "str_intern"
TEST = "intern_basic"
FINAL_FUNCTION = "tf_coverage_channel_finish"
WORK_CAP = 48  # Direct/reserved native processes plus distinct admitted input files.
RAW_NAMES = ("_setup.0.profraw", "_setup.1.profraw", "intern_basic.parent.profraw")


class Infrastructure(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Infrastructure(reason)


def guarded_import(path, expected, name):
    path = Path(path).resolve(strict=True)
    with path.open("rb") as stream:
        snapshot = stream.read(256 * 1024 + 1)
    require(len(snapshot) <= 256 * 1024 and hashlib.sha256(snapshot).hexdigest() == expected,
            "reviewed source snapshot differs: " + str(path))
    spec = importlib.util.spec_from_file_location(name, str(path))
    require(spec is not None, "cannot construct guarded import")
    module = importlib.util.module_from_spec(spec)
    exec(compile(snapshot, str(path), "exec"), module.__dict__)
    return module


def reducer_worker():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reduce-worker", action="store_true", required=True)
    parser.add_argument("--helper", required=True)
    parser.add_argument("--reducer", required=True)
    parser.add_argument("--suite-dir", required=True)
    parser.add_argument("--profdata", required=True)
    parser.add_argument("--runner-returncode", required=True, type=int)
    args = parser.parse_args()
    guarded_import(args.helper, HELPER_SHA, "terminal_verified_helpers")
    reducer = guarded_import(args.reducer, REDUCER_SHA, "terminal_actual_reducer")
    directory = Path(args.suite_dir)
    require(sorted(item.name for item in directory.iterdir()) == sorted(RAW_NAMES),
            "reducer input inventory changed")
    rows = reducer.reduce_suite(SUITE, str(directory), args.profdata, args.runner_returncode == 0)
    print(json.dumps({name: {"status": row[0], "reason": row[1], "names_count": len(row[2])}
                      for name, row in rows.items()}, sort_keys=True))
    return 0


class Proof:
    def __init__(self, args, root, receipt):
        self.common = guarded_import(args.helper, HELPER_SHA, "terminal_verified_helpers")
        require((self.common.DEADLINE, self.common.GRACE, self.common.LOG_CAP) == (60, 5, 1048576),
                "reviewed process-helper bounds differ")
        self.root, self.receipt = root, receipt
        self.work, self.admitted = [], set()
        receipt["work"] = self.work
        receipt["processes"] = self.common.PROCESS_RECORDS
        self.inputs = {}
        for name, path, executable in (
                ("runner", args.runner, True), ("profdata", args.llvm_profdata, True),
                ("reducer", args.reducer, False), ("helper", args.helper, False),
                ("driver", __file__, False)):
            self.admit(Path(path).resolve(strict=True))
            self.inputs[name] = self.common.identity(path, executable)
        require(self.inputs["helper"]["sha256"] == HELPER_SHA, "helper identity differs")
        require(self.inputs["reducer"]["sha256"] == REDUCER_SHA, "reducer identity differs")
        receipt["inputs"] = self.inputs
        self.runner = self.inputs["runner"]["path"]
        self.tool = self.inputs["profdata"]["path"]
        self.environment = os.environ.copy()
        self.environment["LLVM_PROFILE_FILE"] = "/dev/null"
        self.environment.pop("CBM_TEST_COVERAGE_DIR", None)
        self.environment.pop("CBM_TEST_ONLY", None)
        self.environment.pop("CBM_TEST_ONLY_FILE", None)
        self.seed = None
        self.seed_identity = None

    def charge(self, kind, detail):
        require(len(self.work) < WORK_CAP, "combined finite work cap exhausted")
        self.work.append({"kind": kind, "detail": detail})

    def admit(self, path):
        key = str(path)
        if key not in self.admitted:
            self.charge("input_file", key)
            self.admitted.add(key)

    def run(self, directory, label, argv, environment, success=False):
        self.charge("process", label)
        record = self.common.run_process(directory, label, argv, environment)
        record["evidence_directory"] = str(directory)
        record["process_receipt"] = str(directory / (label + ".process.json"))
        require(not record["failure"] and record["quiescent"]
                and type(record["returncode"]) is int and record["returncode"] >= 0,
                "process infrastructure failure: " + label)
        if success:
            require(record["returncode"] == 0, "native tool/control failed: " + label)
        return record

    def log(self, directory, record, kind):
        return self.common.ANSI.sub("", self.common.read_log(directory, record, kind))

    def fingerprint(self, path):
        value = path.lstat()
        require(stat.S_ISREG(value.st_mode) and value.st_nlink == 1 and value.st_uid == os.geteuid(),
                "owned seed/profile is not a private regular file")
        identity = self.common.identity(path)
        identity.update({"device": value.st_dev, "inode": value.st_ino,
                         "mode": stat.S_IMODE(value.st_mode), "owner": value.st_uid,
                         "links": value.st_nlink})
        return identity

    def inventory(self, directory):
        names = []
        with os.scandir(str(directory)) as entries:
            for entry in entries:
                require(len(names) < len(RAW_NAMES), "unexpected extra suite artifact")
                names.append(entry.name)
        require(sorted(names) == sorted(RAW_NAMES), "expected three profile artifacts are absent")
        for name in RAW_NAMES:
            path = directory / name
            self.admit(path)
            require(self.common.regular_nonempty(path), "invalid native raw profile: " + name)
        return sorted(names)

    def count(self, case, label, raw, function):
        self.admit(raw)
        require(self.common.regular_nonempty(raw), "raw profile is absent/unreadable")
        indexed = case / (label + ".profdata")
        self.run(case, label + "-merge", [self.tool, "merge", "--instr",
                                         "--output=" + str(indexed), str(raw)],
                 self.environment, success=True)
        require(self.common.regular_nonempty(indexed), "merge produced no indexed profile")
        self.admit(indexed)
        shown = self.run(case, label + "-count",
                         [self.tool, "show", "--counts", "--function=" + function, str(indexed)],
                         self.environment, success=True)
        blocks, current = [], None
        for line in self.log(case, shown, "stdout").splitlines():
            heading = re.fullmatch(r"  (.+):", line)
            if heading:
                current = {"name": heading.group(1), "counts": []}
                blocks.append(current)
            elif current is not None:
                value = re.fullmatch(r"    Function count: ([0-9]+)", line)
                if value:
                    current["counts"].append(int(value.group(1)))
        exact = [block for block in blocks if block["name"] == function
                 or block["name"].endswith(":" + function)]
        require(len(exact) == 1 and len(exact[0]["counts"]) == 1,
                "missing/ambiguous exact function record: " + function)
        return {"raw": str(raw), "indexed": str(indexed), "function": exact[0]["name"],
                "count": exact[0]["counts"][0]}

    def reduce(self, case, directory, code):
        # The pinned reducer invokes exactly one profdata reader per admitted raw
        # profile here. Reserve those nested processes before launching its group.
        self.inventory(directory)
        for name in RAW_NAMES:
            self.charge("reserved_reducer_process", name)
        argv = [sys.executable, self.inputs["driver"]["path"], "--reduce-worker",
                "--helper", self.inputs["helper"]["path"],
                "--reducer", self.inputs["reducer"]["path"],
                "--suite-dir", str(directory), "--profdata", self.tool,
                "--runner-returncode", str(code)]
        record = self.run(case, "actual-reducer", argv, self.environment, success=True)
        rows = json.loads(self.log(case, record, "stdout"))
        require(isinstance(rows, dict) and set(rows) == {TEST, "*"}, "unexpected actual reducer rows")
        for row in rows.values():
            require(isinstance(row, dict) and set(row) == {"status", "reason", "names_count"}
                    and row["status"] in ("complete", "incomplete")
                    and isinstance(row["reason"], str) and type(row["names_count"]) is int
                    and row["names_count"] >= 0, "invalid actual reducer response")
        return rows

    def body(self, stdout, code):
        rows = re.findall(r"(?m)^  ([A-Za-z_][A-Za-z0-9_]*)[ \t]+(PASS|FAIL|SKIP)\b", stdout)
        starts = re.findall(r"(?m)^  " + re.escape(TEST) + r"\b", stdout)
        summaries = re.findall(r"(?m)^[ \t]*(\d+) passed(?:, (\d+) failed)?(?:, (\d+) skipped)?"
                               r"(?:, (\d+) deselected)?[ \t]*$", stdout)
        require(rows == [(TEST, "PASS")] and len(starts) == 1 and len(summaries) == 1,
                "exact selected body did not PASS once")
        numbers = [int(value or 0) for value in summaries[0]]
        require(numbers[0] == 1 and numbers[2] == 0 and numbers[1] in (0, 1),
                "unrelated body/summary failure")
        if code == 0:
            require(numbers[1] == 0, "zero exit with failed summary")
        return numbers

    def errors(self, stderr, target, required):
        native, owned = [], []
        for line in stderr.splitlines():
            if line.startswith("LLVM Profile Error:"):
                match = re.fullmatch(r'LLVM Profile Error: Failed to write file "(.*)": (.+)', line)
                require(match is not None and match.group(1) == str(target),
                        "unrecognized or wrong-path native LLVM error")
                native.append({"line": line, "reason": match.group(2)})
            elif line.startswith("coverage profile write failed:"):
                require(line == "coverage profile write failed: " + str(target),
                        "wrong-path runner-owned profile failure")
                owned.append(line)
            elif (line.startswith("coverage child accounting ")
                  or line.startswith("coverage profile could not be routed:")
                  or line.startswith("a coverage run starts with")
                  or line.startswith("cannot use coverage directory:")
                  or line.startswith("cannot route coverage profiles under:")):
                raise Infrastructure("unrelated coverage activation/accounting failure")
        require(bool(native) == required, "intended native terminal-write precondition not observed")
        require(len(owned) <= 1, "duplicate runner-owned final failure")
        if not required:
            require(not owned, "positive control reported final writer failure")
        return {"native": native, "runner_owned": owned}

    def case(self, mode, source_seed=None):
        case = self.root / mode
        case.mkdir(mode=0o700)
        coverage = case / "coverage"
        directory = coverage / SUITE
        directory.mkdir(mode=0o700, parents=True)
        terminal = directory / "_setup.1.profraw"
        result = {"mode": mode, "coverage_root": str(coverage), "controls_passed": False}
        self.receipt["cases"].append(result)
        if source_seed is not None:
            self.seed = terminal
            result["seed_source"] = self.common.identity(source_seed)
            with source_seed.open("rb") as source, terminal.open("xb") as output:
                os.fchmod(output.fileno(), 0o600)
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    output.write(block)
                output.flush()
                os.fchmod(output.fileno(), 0o400)
            self.admit(terminal)
            self.seed_identity = self.fingerprint(terminal)
            require(self.seed_identity["mode"] == 0o400, "seed did not retain the read-only mode")
            require(self.seed_identity["sha256"] == result["seed_source"]["sha256"],
                    "seed copy differs from genuine successful initial profile")
            result["seed_before"] = self.seed_identity
        environment = self.environment.copy()
        environment["CBM_TEST_COVERAGE_DIR"] = str(coverage)
        environment["CBM_TEST_ONLY"] = SUITE + ":" + TEST
        result["input_environment"] = {"LLVM_PROFILE_FILE": "/dev/null",
                                       "CBM_TEST_COVERAGE_DIR": str(coverage),
                                       "CBM_TEST_ONLY": SUITE + ":" + TEST,
                                       "CBM_TEST_ONLY_FILE": "absent"}
        process = self.run(case, "runner", [self.runner, SUITE], environment)
        result["runner"] = process
        code = process["returncode"]
        result["summary"] = self.body(self.log(case, process, "stdout"), code)
        result["errors"] = self.errors(self.log(case, process, "stderr"), terminal, source_seed is not None)
        if source_seed is not None:
            result["seed_after_quiescence"] = self.fingerprint(terminal)
            require(result["seed_after_quiescence"] == self.seed_identity,
                    "native run replaced/changed seeded terminal artifact; precondition unmet")
        result["inventory"] = self.inventory(directory)
        result["initial"] = self.count(case, "initial", directory / "_setup.0.profraw", FINAL_FUNCTION)
        result["terminal"] = self.count(case, "terminal", terminal, FINAL_FUNCTION)
        result["body"] = self.count(case, "body", directory / (TEST + ".parent.profraw"), "test_" + TEST)
        require(result["initial"]["count"] == 0 and result["body"]["count"] > 0,
                "current initial/body profile controls failed")
        require((result["terminal"]["count"] > 0) if source_seed is None
                else (result["terminal"]["count"] == 0), "terminal native count precondition failed")
        rows = self.reduce(case, directory, code)
        result["rows"] = rows
        require(all(row["reason"] in ("", "suite did not exit 0") for row in rows.values()),
                "actual reduction reports unrelated unreadable/missing-profile evidence")
        complete = all(row["status"] == "complete" and row["reason"] == "" for row in rows.values())
        conservative = all(row["status"] == "incomplete"
                           and "suite did not exit 0" in row["reason"].split("; ") for row in rows.values())
        if source_seed is None:
            require(code == 0 and complete, "successful control failed")
            result["feature_passed"] = True
        elif code == 0:
            require(not result["errors"]["runner_owned"] and complete,
                    "zero-status result differs from the bounded stale-complete regression")
            result["feature_passed"] = False
            result["regression"] = "failed automatic final write accepted with readable stale setup"
        else:
            require(code == 1 and result["summary"][1] == 1,
                    "terminal failure did not produce the exact runner failure status/summary")
            require(len(result["errors"]["runner_owned"]) == 1,
                    "nonzero status lacks attributed final profile failure")
            result["feature_passed"] = conservative
            if not conservative:
                result["regression"] = "actual reducer did not retain the final failure floor"
        result["controls_passed"] = True
        self.common.save(case / "result.json", result)
        return directory / "_setup.0.profraw"

    def restore_seed_mode(self):
        if self.seed is None or self.seed_identity is None:
            return
        if not all(row["quiescent"] for row in self.common.PROCESS_RECORDS):
            self.receipt["seed_cleanup"] = "retained unchanged: quiescence not established"
            return
        if self.fingerprint(self.seed) != self.seed_identity:
            self.receipt["seed_cleanup"] = "retained unchanged: original seed identity/content differs"
            raise Infrastructure("seed changed before controlled mode restoration")
        os.chmod(str(self.seed), 0o600)
        self.receipt["seed_cleanup"] = {"owned_mode_restored": 0o600,
                                        "post_cleanup": self.fingerprint(self.seed)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("runner", "llvm-profdata", "reducer", "helper", "output-dir"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    require(os.name == "posix", "this finite permission/native-profile proof requires POSIX")
    root = Path(args.output_dir).resolve()
    require(not any(char in str(root) for char in ("%", "\n", "\r")), "invalid owned profile root")
    root.mkdir(mode=0o700)  # Fresh exclusive evidence, never delete/reuse an old run.
    receipt = {"schema": 1, "oracle": "terminal-parent-write-v1", "cases": [],
               "classification": "infrastructure_failure", "exit_code": 2,
               "combined_work_cap": WORK_CAP, "process_deadline_seconds": 60,
               "grace_seconds_per_phase": 5, "stream_cap_bytes": 1048576,
               "limits": ["Shared helper/reducer source guarded before import/exec.",
                          "Reducer internal captures are not covered by the outer stream cap.",
                          "No proof of later atexit callbacks or all-future process/image closure."]}
    proof = None
    try:
        proof = Proof(args, root, receipt)
        version = proof.run(root, "profdata-version", [proof.tool, "--version"],
                            proof.environment, success=True)
        require(re.search(r"\bversion 22\.1\.8\b", proof.log(root, version, "stdout")),
                "matching LLVM 22.1.8 tool precondition failed")
        seed = proof.case("control")
        proof.case("readonly-terminal", seed)
        for name, identity in proof.inputs.items():
            require(proof.common.identity(identity["path"], name in ("runner", "profdata")) == identity,
                    "proof input changed: " + name)
        passed = all(case["feature_passed"] for case in receipt["cases"])
        receipt["classification"] = "pass" if passed else "regression_failure"
        receipt["exit_code"] = 0 if passed else 1
    except (Exception, KeyboardInterrupt) as exc:
        receipt["error"] = type(exc).__name__ + ": " + str(exc)
    finally:
        try:
            if proof is not None:
                proof.restore_seed_mode()
        except (Exception, KeyboardInterrupt) as exc:
            receipt["cleanup_error"] = type(exc).__name__ + ": " + str(exc)
            receipt["classification"], receipt["exit_code"] = "infrastructure_failure", 2
        records = receipt.get("processes", [])
        receipt["all_owned_groups_quiescent"] = all(row.get("quiescent", False) for row in records)
        if not receipt["all_owned_groups_quiescent"]:
            receipt["classification"], receipt["exit_code"] = "infrastructure_failure", 2
        with (root / "result.json").open("x", encoding="utf-8") as stream:
            json.dump(receipt, stream, indent=2, sort_keys=True)
            stream.write("\n")
    print(receipt["classification"] + ": " + str(root / "result.json"))
    return receipt["exit_code"]


if __name__ == "__main__":
    sys.dont_write_bytecode = True
    try:
        sys.exit(reducer_worker() if sys.argv[1:2] == ["--reduce-worker"] else main())
    except (Exception, KeyboardInterrupt) as error:
        print("infrastructure_failure: " + type(error).__name__ + ": " + str(error), file=sys.stderr)
        sys.exit(2)
