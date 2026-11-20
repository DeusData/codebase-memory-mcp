#!/usr/bin/env python3
"""Turn a test-impact answer (`codebase-memory-mcp test-impact select`) into
what a CI consumer runs.

  selection.py suites ANSWER
      One line per suite whose tests the answer selects, or a single `*` when a
      lane runs everything. The team-artifact producer re-runs exactly these
      suites with coverage (scripts/ci/test-impact-publish.sh).

  selection.py filter ANSWER --lane LANE --out FILE [--optional-out FILE2]
      exit 0   narrowed: FILE holds the lane's CBM_TEST_ONLY_FILE lines, and
               FILE2 the selected suites the engine saw registered only
               conditionally (CONDITIONAL): a build may legitimately lack them
               (scripts/run-tests-parallel.sh, CBM_TEST_SELECTION_OPTIONAL)
      exit 11  the lane selects nothing: FILE is empty
      anything else: run everything (FILE and FILE2 are removed)

It fails safe by construction: a run-all answer, a lane it cannot find, an
unknown schema or filter encoding, a token that is not `suite` or
`suite:test`, and a filter that disagrees with the lane's own suite list all
mean "run everything", and so does any crash (exit 1).
"""
import argparse
import json
import os
import re
import sys

SCHEMAS = {"cbm.test_impact.v0", "cbm.test_impact.v1"}
TOKEN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(:[A-Za-z_][A-Za-z0-9_]*)?$")
NARROWED, NOTHING, RUN_ALL = 0, 11, 10


def suites(answer):
    if answer.get("decision") == "run_all":
        return ["*"]
    out = []
    for lane in answer.get("lanes") or []:
        if lane.get("decision") == "run_all" or lane.get("suites") is None:
            return ["*"]
        out += [suite["suite"] for suite in lane["suites"]]
    return sorted(set(out))


def run_all(why):
    print(f"selection: run everything: {why}", file=sys.stderr)
    return RUN_ALL


def lane_filter(answer, name):
    """-> (exit code, lines)"""
    if answer.get("schema") not in SCHEMAS:
        return run_all(f"unknown schema {answer.get('schema')!r}"), []
    if answer.get("decision") == "run_all":
        return run_all("the answer runs everything"), []
    lanes = [lane for lane in answer.get("lanes") or [] if lane.get("lane") == name]
    if len(lanes) != 1:
        return run_all(f"lane {name!r} appears {len(lanes)} times"), []
    lane = lanes[0]
    decision = lane.get("decision")
    if decision == "skip":
        if lane.get("suites") or lane.get("runner_filter") is not None:
            return run_all("a skipped lane that still lists suites or a filter"), []
        return NOTHING, []
    if decision != "selected" or not isinstance(lane.get("suites"), list):
        return run_all(f"lane decision {decision!r}"), []
    flt = lane.get("runner_filter")
    if not isinstance(flt, dict) or flt.get("encoding") != "cbm-test-only-lines" or \
            not isinstance(flt.get("value"), str):
        return run_all("no cbm-test-only-lines filter"), []
    lines = [line for line in flt["value"].split("\n") if line]
    bad = [line for line in lines if not TOKEN.match(line)]
    if not lines or bad:
        return run_all(f"filter tokens unusable ({bad[:3] or 'empty'})"), []
    expected = set()
    for suite in lane["suites"]:
        if suite.get("mode") == "whole":
            expected.add(suite["suite"])
        elif suite.get("mode") == "tests" and suite.get("tests"):
            expected |= {f"{suite['suite']}:{test['test']}" for test in suite["tests"]}
        else:
            return run_all(f"suite {suite.get('suite')!r} has mode {suite.get('mode')!r}"), []
    if set(lines) != expected:
        return run_all("the filter disagrees with the lane's suite list"), []
    return NARROWED, lines


def conditional_suites(answer, name):
    """The lane's selected suites whose registration the engine saw as conditional."""
    for lane in answer.get("lanes") or []:
        if lane.get("lane") == name:
            return sorted({suite["suite"] for suite in lane.get("suites") or []
                           if "CONDITIONAL" in (suite.get("whole_reason") or [])})
    return []


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("suites")
    s.add_argument("answer")
    f = sub.add_parser("filter")
    f.add_argument("answer")
    f.add_argument("--lane", required=True)
    f.add_argument("--out", required=True)
    f.add_argument("--optional-out")
    args = parser.parse_args()
    if args.command == "suites":
        # LF on every host, like the files below: a Windows python would
        # otherwise write CRLF.
        sys.stdout.reconfigure(newline="\n")
        with open(args.answer, encoding="utf-8") as handle:
            print("\n".join(suites(json.load(handle))))
        return 0
    for stale in (args.out, args.optional_out):
        if stale and os.path.exists(stale):
            os.remove(stale)
    try:
        with open(args.answer, encoding="utf-8") as handle:
            answer = json.load(handle)
    except (OSError, ValueError) as error:
        return run_all(f"unreadable answer ({error})")
    if not isinstance(answer, dict):
        return run_all("the answer is not an object")
    code, lines = lane_filter(answer, args.lane)
    if code in (NARROWED, NOTHING):
        with open(args.out, "w", encoding="utf-8", newline="\n") as handle:
            handle.write("".join(line + "\n" for line in lines))
        if args.optional_out:
            optional = conditional_suites(answer, args.lane) if code == NARROWED else []
            with open(args.optional_out, "w", encoding="utf-8", newline="\n") as handle:
                handle.write("".join(suite + "\n" for suite in optional))
    return code


if __name__ == "__main__":
    sys.exit(main())
