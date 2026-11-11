#!/usr/bin/env python3
"""coverage-map.py — build the per-test coverage map of the C test runner: for
every test, the set of functions it executed, children included. The test-impact
engine uses it to narrow the suites whose tests cross a process boundary, where
the call graph alone cannot say which test reaches which function.

Input: the instrumented runner (make -f Makefile.cbm test-runner-cov). Run with
CBM_TEST_COVERAGE_DIR it leaves, per suite, raw profiles named after the test
(tests/test_main.c, "Per-test coverage profiles"):
    <test>.parent.profraw     the runner inside the test
    <test>.<pid>.profraw      one per child process of the test
    <test>.<pid>.forked       an unaccounted forked child
    _setup.<n>.profraw        the runner between tests
    _setup.child.<pid>.profraw  children started between tests
    _setup.child.<pid>.forked   unaccounted children started between tests

Output directory:
    functions.tsv   <id> TAB <source file> TAB <name>
                    The file is where the function body stands, relative to the
                    repository root, read from the coverage mapping of the
                    runner itself (llvm-cov export). The profile only names the
                    translation unit: a function of a unity build or a static
                    inline of a header would otherwise carry the wrong file,
                    and the engine joins the map to the graph by file and name.
    tests.tsv       <suite>:<test> TAB <complete|incomplete> TAB <reason> TAB <ids>
                    ids ascending, space separated. <suite>:* is what ran BETWEEN
                    the tests of a suite (its setup); a change there concerns
                    every test of the suite.
    meta.json       commit, toolchain, and per suite: tests, incomplete tests,
                    exit code, wall seconds

A test is `incomplete` when its coverage cannot be trusted to be whole: a
forked child left its marker, a profile is unreadable, the parent profile is
missing, or the suite did not exit 0. An incomplete test must always be
selected; the map never claims less than it knows.

Raw profiles are about 7 MiB each, so every suite is reduced and deleted before
the next one runs.

Usage:
    coverage-map.py --runner build/cov/test-runner --out DIR
                    [--suite NAME ...] [--llvm-bin DIR] [--root DIR]
                    [--keep-raw] [--timeout SECONDS]
Exit status: 0 map written; 1 a suite failed or timed out (map still written,
its tests incomplete); 2 usage or toolchain error.
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time

PROFILE = re.compile(r"^(?P<stem>.+?)\.(?P<who>parent|\d+)\.profraw$")
MARKER = re.compile(r"^(?P<stem>.+?)\.(?P<pid>\d+)\.forked$")
SETUP = re.compile(r"^_setup\.(?:\d+|child\.\d+)\.profraw$")
# `llvm-profdata show --all-functions` prints one block per function, opened by
# the name at two spaces of indent and a trailing colon.
FUNCTION_LINE = re.compile(r"^  (?P<name>\S.*):$")
SETUP_TEST = "*"


def fail(message):
    print(f"coverage-map: {message}", file=sys.stderr)
    sys.exit(2)


def executed_functions(profdata, profile):
    """Names of the functions `profile` counted at least once, or None when the
    profile cannot be read."""
    proc = subprocess.run([profdata, "show", "--all-functions", "--value-cutoff=1", profile],
                          capture_output=True, text=True)
    if proc.returncode != 0:
        return None
    names = set()
    for line in proc.stdout.splitlines():
        match = FUNCTION_LINE.match(line)
        if match:
            names.add(match.group("name"))
    return names


def source_table(llvm_cov, profdata_tool, runner, profile, root, work):
    """{profile name: (source file, function name)} for every function compiled
    into the runner, from its coverage mapping. `profile` is any raw profile of
    that runner: llvm-cov wants one, its counts are not used."""
    indexed = os.path.join(work, "table.profdata")
    merged = subprocess.run([profdata_tool, "merge", "-sparse", "-o", indexed, profile],
                            capture_output=True, text=True)
    if merged.returncode != 0:
        fail(f"llvm-profdata merge failed: {merged.stderr.strip()[-300:]}")
    exported = subprocess.run([llvm_cov, "export", "-format=text", "-skip-expansions",
                               "-skip-branches", f"-instr-profile={indexed}", runner],
                              capture_output=True, text=True)
    os.remove(indexed)
    if exported.returncode != 0:
        fail(f"llvm-cov export failed: {exported.stderr.strip()[-300:]}")
    base = os.path.realpath(root) + os.sep
    table = {}
    for record in json.loads(exported.stdout)["data"][0]["functions"]:
        files = record.get("filenames") or []
        if not files:
            continue
        path = os.path.realpath(files[0])
        if path.startswith(base):
            path = path[len(base):]
        table[record["name"]] = (path.replace(os.sep, "/"), record["name"].rpartition(":")[2])
    return table


class Functions:
    """Stable ids for (source file, function name), in order of first
    appearance. Several profile names can be one function: a static inline of a
    header is recorded once per file that includes it."""

    def __init__(self, table):
        self.table = table
        self.ids = {}

    def key(self, name):
        if name in self.table:
            return self.table[name]
        # Not in the coverage mapping: keep what the profile says, the
        # translation unit for a function with internal linkage.
        source, _, short = name.rpartition(":")
        return (source, short)

    def ids_of(self, names):
        out = set()
        for name in names:
            key = self.key(name)
            if key not in self.ids:
                self.ids[key] = len(self.ids)
            out.add(self.ids[key])
        return sorted(out)

    def rows(self):
        for (source, short), ident in sorted(self.ids.items(), key=lambda item: item[1]):
            yield ident, source, short


def reduce_suite(suite, suite_dir, profdata, suite_ok):
    """Turn one suite's raw profiles into {test: (status, reason, names)}."""
    tests = {}
    markers = {}
    setup = set()
    setup_unreadable = False
    setup_markers = []
    entries = sorted(os.listdir(suite_dir)) if os.path.isdir(suite_dir) else []
    for entry in entries:
        path = os.path.join(suite_dir, entry)
        marker = MARKER.match(entry)
        if marker:
            if marker.group("stem") == "_setup.child":
                setup_markers.append(marker.group("pid"))
            else:
                markers.setdefault(marker.group("stem"), []).append(marker.group("pid"))
            continue
        if SETUP.match(entry):
            names = executed_functions(profdata, path)
            if names is None:
                setup_unreadable = True
            else:
                setup |= names
            continue
        profile = PROFILE.match(entry)
        if not profile:
            continue
        test = tests.setdefault(profile.group("stem"),
                                {"names": set(), "parent": False, "unreadable": []})
        names = executed_functions(profdata, path)
        if names is None:
            test["unreadable"].append(entry)
            continue
        test["names"] |= names
        test["parent"] = test["parent"] or profile.group("who") == "parent"

    result = {}
    for stem, pids in markers.items():
        tests.setdefault(stem, {"names": set(), "parent": False, "unreadable": []})
    for stem, test in sorted(tests.items()):
        reasons = []
        if stem in markers:
            reasons.append("unaccounted forked child: " + ",".join(sorted(markers[stem])))
        if test["unreadable"]:
            reasons.append("unreadable profile: " + ",".join(sorted(test["unreadable"])))
        if not test["parent"]:
            reasons.append("no parent profile")
        if not suite_ok:
            reasons.append("suite did not exit 0")
        result[stem] = ("incomplete" if reasons else "complete", "; ".join(reasons), test["names"])
    setup_reasons = []
    if setup_markers:
        setup_reasons.append("unaccounted forked child: " + ",".join(sorted(setup_markers)))
    if setup_unreadable:
        setup_reasons.append("unreadable setup profile")
    if not suite_ok:
        setup_reasons.append("suite did not exit 0")
    result[SETUP_TEST] = ("incomplete" if setup_reasons else "complete",
                          "; ".join(setup_reasons), setup)
    return result


def tool_version(tool):
    proc = subprocess.run([tool, "--version"], capture_output=True, text=True)
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    return lines[0] if lines else "unknown"


def git_commit(root):
    proc = subprocess.run(["git", "-C", root, "rev-parse", "HEAD"], capture_output=True, text=True)
    return proc.stdout.strip() if proc.returncode == 0 else "unknown"



def add_v2_arguments(parser):
    parser.add_argument("--format", type=int, choices=(1, 2), default=1,
                        help="wire format (2 is experimental and unadmitted)")
    parser.add_argument("--registry", help="format2 image-bound expected registry JSON")
    for flag, default, help_text in (
        ("max-output-bytes", 32 * 1024 * 1024, "combined stdout/stderr bytes per command"),
        ("max-total-output-bytes", 512 * 1024 * 1024, "aggregate command stdout/stderr bytes"),
        ("max-profile-bytes", 64 * 1024 * 1024, "bytes per raw input profile"),
        ("max-total-profile-bytes", 512 * 1024 * 1024, "aggregate raw and separately converted bytes"),
        ("max-profile-files", 4096, "raw entries including directories, markers and listing profile"),
        ("conversion-timeout", 60, "seconds per tool command"),
        ("reduction-timeout", 600, "seconds per strict parse or final pure reduction"),
        ("max-map-bytes", 64 * 1024 * 1024, "combined producer wire bytes"),
    ):
        parser.add_argument("--" + flag, type=int, default=default,
                            help="format2: %s (default: %s)" % (help_text, default))
    parser.epilog = ((parser.epilog or "") +
                     " Format2 always retains bounded raw and diagnostic evidence; "
                     "--keep-raw only changes format1. Format2 supports the observed "
                     "LLVM 22.1.8 frontend text dialect and POSIX process groups.")


def main():
    parser = argparse.ArgumentParser(description="Build the per-test coverage map.")
    parser.add_argument("--runner", required=True, help="instrumented test runner")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument("--suite", action="append", default=[], help="suite to run (repeatable)")
    parser.add_argument("--llvm-bin", default=os.environ.get("COV_LLVM_BIN", ""),
                        help="directory holding llvm-profdata and llvm-cov of the compiler that "
                             "built the runner")
    parser.add_argument("--root", default="",
                        help="repository root the source files are named relative to "
                             "(default: the repository this script is in)")
    parser.add_argument("--keep-raw", action="store_true", help="keep the raw profiles")
    parser.add_argument("--timeout", type=int, default=3600, help="seconds per suite")
    add_v2_arguments(parser)
    args = parser.parse_args()
    if args.format == 2:
        from coverage_live_v2 import collect
        return collect(args)

    runner = os.path.abspath(args.runner)
    if not os.access(runner, os.X_OK):
        fail(f"runner is not executable: {runner}")
    profdata = os.path.join(args.llvm_bin, "llvm-profdata") if args.llvm_bin else "llvm-profdata"
    llvm_cov = os.path.join(args.llvm_bin, "llvm-cov") if args.llvm_bin else "llvm-cov"
    for tool in (profdata, llvm_cov):
        if not shutil.which(tool):
            fail(f"not found: {tool}")
    root = args.root or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    out = os.path.abspath(args.out)
    raw = os.path.join(out, "raw")
    os.makedirs(raw, exist_ok=True)

    # Listing the suites is also the cheapest run of the runner: the profile it
    # leaves is the one llvm-cov needs to print the coverage mapping.
    listing_profile = os.path.join(out, "listing.profraw")
    listed = subprocess.run([runner, "--list-suites"], capture_output=True, text=True,
                            env=dict(os.environ, LLVM_PROFILE_FILE=listing_profile))
    if listed.returncode != 0:
        fail("the runner could not list its suites")
    if not os.path.exists(listing_profile):
        fail("the runner wrote no profile: is it the instrumented build (make test-runner-cov)?")
    table = source_table(llvm_cov, profdata, runner, listing_profile, root, out)
    os.remove(listing_profile)
    suites = args.suite or [line.strip() for line in listed.stdout.splitlines() if line.strip()]
    if not suites:
        fail("no suites to run")

    functions = Functions(table)
    test_rows = []
    suite_meta = []
    failed = False
    env = dict(os.environ)
    # A plain name: a runner built for continuous profiles would otherwise start
    # in that mode and ignore every per-test name (tests/test_main.c).
    env["LLVM_PROFILE_FILE"] = os.devnull
    env["CBM_TEST_COVERAGE_DIR"] = raw
    for suite in suites:
        suite_dir = os.path.join(raw, suite)
        shutil.rmtree(suite_dir, ignore_errors=True)
        started = time.monotonic()
        try:
            run = subprocess.run([runner, suite], env=env, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, timeout=args.timeout)
            code = run.returncode
        except subprocess.TimeoutExpired:
            code = -1
        wall = time.monotonic() - started
        suite_ok = code == 0
        failed = failed or not suite_ok
        reduced = reduce_suite(suite, suite_dir, profdata, suite_ok)
        incomplete = 0
        for test, (status, reason, names) in sorted(reduced.items()):
            if test != SETUP_TEST and status != "complete":
                incomplete += 1
            ids = " ".join(str(i) for i in functions.ids_of(names))
            test_rows.append(f"{suite}:{test}\t{status}\t{reason}\t{ids}")
        suite_meta.append({"suite": suite, "tests": len(reduced) - 1, "incomplete": incomplete,
                           "exit": code, "wall_s": round(wall, 2)})
        print(f"{suite}: {len(reduced) - 1} tests, {incomplete} incomplete, exit {code}, "
              f"{wall:.1f} s", flush=True)
        if not args.keep_raw:
            shutil.rmtree(suite_dir, ignore_errors=True)
    if not args.keep_raw:
        shutil.rmtree(raw, ignore_errors=True)

    with open(os.path.join(out, "functions.tsv"), "w", encoding="utf-8", newline="\n") as handle:
        for ident, source, short in functions.rows():
            handle.write(f"{ident}\t{source}\t{short}\n")
    with open(os.path.join(out, "tests.tsv"), "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(test_rows) + "\n")
    meta = {
        "format": 1,
        "commit": git_commit(root),
        "llvm_profdata": tool_version(profdata),
        "platform": sys.platform,
        "functions": len(functions.ids),
        "functions_compiled": len(set(table.values())),
        "tests": sum(entry["tests"] for entry in suite_meta),
        "incomplete": sum(entry["incomplete"] for entry in suite_meta),
        "suites": suite_meta,
    }
    with open(os.path.join(out, "meta.json"), "w", encoding="utf-8", newline="\n") as handle:
        json.dump(meta, handle, indent=1)
        handle.write("\n")
    print(f"map: {meta['tests']} tests, {meta['incomplete']} incomplete, "
          f"{meta['functions']} functions -> {out}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
