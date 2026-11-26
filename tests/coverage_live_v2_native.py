"""Native fixture controls and independent live-v2 bundle assertions."""

import json
import os
from pathlib import Path
import re

from coverage_live_v2_support import (Infrastructure, Regression, clean_env, digest, proftext,
                                      profile_wire, require, test_rows)

SUITE = "coverage_setup_success"
TESTS = ["coverage_setup_first", "coverage_setup_second"]
FUNCTION = b"cbm_cov_child_count_setup"
MISSING = "coverage.expected.absent"
ANSI = re.compile(rb"\x1b\[[0-9;]*m")


def read(path, maximum=64 * 1024 * 1024):
    path = Path(path)
    require(not path.is_symlink() and path.is_file(), "missing/unsafe evidence file: " + str(path))
    with path.open("rb") as stream:
        data = stream.read(maximum + 1)
    require(len(data) <= maximum, "evidence file exceeds test bound")
    return data


def json_read(path):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate metadata key")
            result[key] = value
        return result
    return json.loads(read(path, 4 * 1024 * 1024).decode("utf-8"), object_pairs_hook=pairs)


def native_summary(data, failed=False):
    clean = ANSI.sub(b"", data)
    rows = re.findall(rb"(?m)^  ([A-Za-z_][A-Za-z0-9_]*)[ \t]+(PASS|FAIL|SKIP)\b", clean)
    require(rows == [(name.encode(), b"PASS") for name in TESTS], "native fixture tests did not pass exactly once")
    summaries = re.findall(rb"(?m)^[ \t]*(\d+) passed(?:, (\d+) failed)?(?:, (\d+) skipped)?"
                           rb"(?:, (\d+) deselected)?[ \t]*$", clean)
    require(len(summaries) == 1, "native summary absent/ambiguous")
    counts = [int(value or b"0") for value in summaries[0]]
    require(counts == [2, 1 if failed else 0, 0, 0], "unexpected native summary counts")


def convert(context, raw, label):
    process = context.processes.run(label, [context.tool, "merge", "--instr", "--text",
                                           "--failure-mode=any", "--num-threads=1", "--output=-", raw],
                                    clean_env(), timeout=70)
    require(process["exit"] == 0, "native profdata conversion failed")
    return proftext(read(process["stdout"]))


def dependency_controls(context):
    root = context.output / "native-controls"
    root.mkdir()
    raw = root / "raw"
    raw.mkdir()
    env = clean_env()
    env["LLVM_PROFILE_FILE"] = str(root / "listing.profraw")
    listing = context.processes.run("native-list", [context.runner, "--list-suites"], env, timeout=70)
    require(listing["exit"] == 0 and SUITE.encode() in read(listing["stdout"]).splitlines(),
            "real runner does not list setup fixture")
    universe = convert(context, root / "listing.profraw", "native-list-text")
    require(any(not any(values) for values in universe.values()), "native enumeration has no zero-record control")
    require(any(key[0] == FUNCTION for key in universe), "native enumeration lacks exact setup identity")
    require(not any(key[0] == b"__cbm_live_v2_test_unknown_identity__" for key in universe),
            "administrative unknown-identity sentinel collides with native universe")
    env["LLVM_PROFILE_FILE"] = os.devnull
    env["CBM_TEST_COVERAGE_DIR"] = str(raw)
    suite = context.processes.run("native-setup", [context.runner, SUITE], env, timeout=70)
    require(suite["exit"] == 0, "real setup fixture process failed")
    native_summary(read(suite["stdout"]))
    events = [json.loads(line[len(b"COV_CHILD "):]) for line in read(suite["stderr"]).splitlines()
              if line.startswith(b"COV_CHILD ")]
    selected = [event for event in events if event.get("id") == "setup_ok"]
    require([event.get("stage") for event in selected] == ["ready", "reaped"],
            "setup child custody control differs")
    require(selected[0].get("marker") == 1 and selected[1].get("marker") == 0
            and selected[1].get("code") == 0 and selected[0].get("pid") == selected[1].get("pid"),
            "setup child did not successfully write, retire marker and reap")
    child = raw / SUITE / ("_setup.child.%d.profraw" % selected[1]["pid"])
    child_records = convert(context, child, "native-setup-child-text")
    require(set(child_records) == set(universe) and all(len(child_records[key]) == len(universe[key])
                                                      for key in universe), "native full-record shape control")
    require(any(any(values) for key, values in child_records.items() if key[0] == FUNCTION),
            "genuine setup child did not hit setup counter")
    for test in TESTS:
        require(bool(read(raw / SUITE / (test + ".parent.profraw"))), "missing native parent control")
    return universe, {"listing": listing, "suite": suite, "child_profile": str(child),
                      "child_sha256": digest(child), "events": selected,
                      "universe_records": len(universe), "actual_setup_hit": True}


def bundle(context, destination, registry, tests, outcome, exit_code):
    metadata = json_read(destination / "meta.json")
    require(isinstance(metadata, dict) and metadata.get("format") == 2 and metadata.get("admission") == "unverified",
            "bundle format/admission marker")
    profiles, tests_data = read(destination / "profiles.tsv"), read(destination / "tests.tsv")
    expected_digests = {"image_sha256": context.runner_identity["sha256"],
                        "registry_sha256": digest(registry),
                        "profiles_sha256": digest(destination / "profiles.tsv"),
                        "tests_sha256": digest(destination / "tests.tsv")}
    for key, expected in expected_digests.items():
        require(metadata.get(key) == expected, "metadata identity/digest mismatch: " + key)
    tool = metadata.get("llvm_profdata", {})
    require(isinstance(tool, dict) and tool.get("path") == context.tool and tool.get("sha256") == context.tool_identity["sha256"]
            and isinstance(tool.get("version"), str) and "22.1.8" in tool["version"],
            "metadata native tool identity/version")
    require(metadata.get("scope") == [SUITE], "unexpected collected registry scope")
    outcomes = metadata.get("suites")
    require(isinstance(outcomes, list) and len(outcomes) == 1, "suite outcome cardinality")
    row = outcomes[0]
    require(isinstance(row, dict) and row.get("suite") == SUITE
            and isinstance(row.get("tests"), list) and len(row["tests"]) == len(tests)
            and all(isinstance(test, str) for test in row["tests"]) and set(row["tests"]) == set(tests)
            and row.get("outcome") == outcome and type(row.get("exit")) is int
            and row["exit"] == exit_code, "suite outcome/expected-registry mismatch")
    process = row.get("process", {})
    require(isinstance(process, dict) and process.get("argv") == [context.runner, SUITE]
            and process.get("returncode") == exit_code and process.get("quiescent") is True,
            "native process identity/status/custody receipt")
    stdout = native_log(destination, process.get("stdout"))
    stderr = native_log(destination, process.get("stderr"))
    native_summary(stdout, failed=outcome == "failed")
    if outcome == "failed":
        require(b"coverage child fixture failed: setup_ok" in stderr,
                "fixed native failure did not reach the actual fixture failure branch")
    ids = profile_wire(profiles, context.runner_identity["sha256"], context.universe)
    rows = test_rows(tests_data)
    require(set(rows) == {(SUITE + ":" + test).encode() for test in ["*"] + tests},
            "producer lost or invented expected rows")
    require(all(number < len(ids) for reasons, numbers in rows.values() for number in numbers),
            "test row references unknown profile identity")
    return rows, ids, metadata


def native_log(destination, record):
    require(isinstance(record, dict) and isinstance(record.get("path"), str)
            and "capture" in record, "native capture log metadata")
    path = Path(record["path"])
    require(path.is_absolute(), "native capture path must be absolute")
    try:
        relative = path.relative_to(destination)
    except ValueError as exc:
        raise Regression("native capture path escaped owned bundle") from exc
    require(relative.parts and ".." not in relative.parts, "native capture path traversal")
    current = destination
    for part in relative.parts:
        current = current / part
        require(not current.is_symlink(), "native capture path contains symlink")
    data = read(path, 32 * 1024 * 1024)
    require(type(record.get("bytes")) is int and record["bytes"] == len(data)
            and record.get("sha256") == digest(path), "native capture length/digest mismatch")
    return data


def retained_profiles(destination):
    result = []
    entries = 0
    for directory, dirs, files in os.walk(destination, followlinks=False):
        for name in dirs + files:
            entries += 1
            require(entries <= 10000, "owned bundle evidence entry bound")
            require(not (Path(directory) / name).is_symlink(), "unexpected symlink in owned evidence")
        for name in files:
            if name.endswith(".profraw"):
                result.append(Path(directory) / name)
    return result


def exact_native_hits(context, raw, rows, ids, label):
    routed = {"*": [], TESTS[0]: [], TESTS[1]: []}
    for path in raw:
        if re.fullmatch(r"_setup\.(?:child\.)?[0-9]+\.profraw", path.name):
            routed["*"].append(path)
        for test in TESTS:
            if path.name == test + ".parent.profraw":
                routed[test].append(path)
    require(routed["*"] and all(len(routed[test]) == 1 for test in TESTS),
            "retained real parent/setup routes missing or duplicate")
    for test, paths in routed.items():
        hits = set()
        for index, path in enumerate(sorted(paths)):
            native = convert(context, path, "%s-row-%s-%d" % (label, "setup" if test == "*" else test, index))
            require(set(native) == set(context.universe)
                    and all(len(native[key]) == len(context.universe[key]) for key in native),
                    "retained native profile identity/shape mismatch")
            hits.update(ids[key] for key, values in native.items() if any(values))
        require(rows[(SUITE + ":" + test).encode()][1] == sorted(hits),
                "v2 row differs from exact genuine native counter hits: " + test)


def positive(context, case, missing=False):
    tests = TESTS + ([MISSING] if missing else [])
    registry = context.registry(case, tests=tests)
    process, destination = context.collect(case, registry, poison=True)
    require(process["exit"] == 0, "format2 native positive exited %r (missing integration on baseline)" % process["exit"])
    rows, ids, _ = bundle(context, destination, registry, tests, "passed", 0)
    for test in ["*"] + TESTS:
        require(rows[(SUITE + ":" + test).encode()][0] == [b"row_evidence_missing"],
                "positive native row has unexpected/missing evidence reasons")
    hit_ids = {number for key, number in ids.items() if key[0] == FUNCTION}
    require(bool(hit_ids.intersection(rows[(SUITE + ":*").encode()][1])), "actual setup hit lost in v2")
    if missing:
        require(rows[(SUITE + ":" + MISSING).encode()] == ([b"row_evidence_missing", b"missing_parent"], []),
                "missing expected parent row must remain explicit and empty")
    raw = retained_profiles(destination)
    children = [path for path in raw if re.fullmatch(r"_setup\.child\.[0-9]+\.profraw", path.name)]
    require(len(children) == 1, "native raw setup evidence not retained")
    exact_native_hits(context, raw, rows, ids, case.name)
    return {"bundle": str(destination), "rows": len(rows), "native_setup_hit": True,
            "all_rows_incomplete": True, "missing_row": missing}


def failed_native(context, case):
    registry = context.registry(case)
    process, destination = context.collect(case, registry, fault=True)
    fault = json_read(case / "native-fault.json")
    children = fault.get("children", [])
    require(len(children) == 1, "native failure boundary was not reached (missing integration on baseline)")
    child = children[0]
    require(child.get("argv") == [context.runner, SUITE] and child.get("rlimit_nofile") == 7
            and child.get("start_new_session") is True, "native fault custody boundary changed")
    if not isinstance(child.get("exit"), int) or child["exit"] <= 0:
        raise Infrastructure("fixed native RLIMIT_NOFILE=7 failure precondition unmet; no tuning/retry")
    raw = retained_profiles(destination)
    parents = [path for path in raw if path.name == TESTS[0] + ".parent.profraw"]
    if len(parents) != 1:
        raise Infrastructure("native fixed fault failed before genuine first-parent evidence existed")
    actual = convert(context, parents[0], "failed-native-parent-text")
    require(set(actual) == set(context.universe), "native failure parent universe changed")
    require(any(any(values) for values in actual.values()), "failed native parent has no observed counters")
    require(process["exit"] == 1, "native suite failure must publish conservative bundle and exit1")
    rows, _, _ = bundle(context, destination, registry, TESTS, "failed", child["exit"])
    require(all(b"suite_failed" in reasons for reasons, _ in rows.values()),
            "failed native suite rows lost failure reason")
    return {"native_exit": child["exit"], "fault": fault, "genuine_parent": str(parents[0]),
            "bundle": str(destination), "all_rows_incomplete": True}
