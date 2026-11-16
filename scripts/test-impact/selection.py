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
      --whole-suites FILE3: the view for a leg the coverage was not recorded
               on: every suite FILE3 lists runs whole there, selected or not
               (an unselected one is optional: a build without it drops it)

  selection.py platform-suites --repo DIR --rev REV [--rev REV ...]
      The platform-specific runner suites, one per line: those whose defining
      test file, at any of the revisions, holds a SKIP_PLATFORM( call or a
      preprocessor conditional on an OS or architecture macro — the two
      markers scripts/check-no-test-skips.sh names for a genuinely
      platform-specific test — and those tests/test_main.c registers under
      such a conditional. Any git failure exits 1 (the caller then narrows no
      leg the classification would have protected).

It fails safe by construction: a run-all answer, a lane it cannot find, an
unknown schema or filter encoding, a token that is not `suite` or
`suite:test`, and a filter that disagrees with the lane's own suite list all
mean "run everything", and so does any crash (exit 1).
"""
import argparse
import json
import os
import re
import subprocess
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


def whole_view(lines, optional, whole):
    """The filter lines and optional suites for a leg where every suite in
    `whole` runs whole: their per-test tokens collapse to the suite, and those
    the answer did not select are added — optional, since only the answer
    knows which suites every build registers."""
    selected = {line.split(":", 1)[0] for line in lines}
    out, seen = [], set()
    for line in lines:
        suite = line.split(":", 1)[0]
        token = suite if suite in whole else line
        if token not in seen:
            seen.add(token)
            out.append(token)
    added = sorted(set(whole) - selected)
    return out + added, sorted(set(optional) | set(added))


# The OS and architecture macros a test is compile-gated on.
PLATFORM_MACROS = ("_WIN32", "_WIN64", "__APPLE__", "__MACH__", "__linux__", "__unix__",
                   "__FreeBSD__", "__OpenBSD__", "__NetBSD__", "__CYGWIN__", "__MINGW32__",
                   "__MINGW64__", "_MSC_VER", "__aarch64__", "__arm64__", "__x86_64__",
                   "__i386__", "_M_ARM64", "_M_X64", "_M_IX86")
PLATFORM_CONDITIONAL = re.compile(r"^\s*#\s*(?:if|ifdef|ifndef|elif)\b.*\b(?:"
                                  + "|".join(re.escape(m) for m in PLATFORM_MACROS) + r")\b")
SKIP_PLATFORM = re.compile(r"\bSKIP_PLATFORM\s*\(")
SUITE_DEFINITION = re.compile(r"^\s*(?:SUITE\s*\(\s*([A-Za-z0-9_]+)\s*\)|"
                              r"void\s+suite_([A-Za-z0-9_]+)\s*\(\s*void\s*\)\s*\{?\s*$)")
RUNNER_REGISTRATION = re.compile(r"^\s*RUN_SELECTED_SUITE(?:_PERF)?\s*\(\s*([a-z0-9_]+)\s*\)")
RUNNER = "tests/test_main.c"


def git(repo, *args, stdin=None):
    # Pinned behaviour: no pager, no replacement objects.
    return subprocess.run(["git", "-C", repo, "--no-pager", "-c", "core.quotePath=false",
                           *args], check=True, capture_output=True, input=stdin,
                          env={**os.environ, "GIT_NO_REPLACE_OBJECTS": "1"}).stdout


def blobs(repo, rev, paths):
    """{path: text} of `paths` at `rev`, read in one `git cat-file --batch`."""
    out = git(repo, "cat-file", "--batch",
              stdin="".join(f"{rev}:{path}\n" for path in paths).encode("utf-8"))
    texts, at = {}, 0
    for path in paths:
        end = out.index(b"\n", at)
        header = out[at:end].split()
        if len(header) != 3 or header[1] != b"blob":
            raise OSError(f"{rev}:{path}: {out[at:end][:80]!r}")
        size = int(header[2])
        texts[path] = out[end + 1:end + 1 + size].decode("utf-8", "replace")
        at = end + 1 + size + 1  # the blob, then its newline
    return texts


def runner_conditional_suites(text):
    """Suites tests/test_main.c registers inside an OS or architecture #if."""
    out, stack = set(), []
    for line in text.splitlines():
        directive = re.match(r"^\s*#\s*(if|ifdef|ifndef|elif|else|endif)\b", line)
        if directive:
            kind = directive.group(1)
            if kind in ("if", "ifdef", "ifndef"):
                stack.append(bool(PLATFORM_CONDITIONAL.match(line)))
            elif kind == "elif" and stack:
                stack[-1] = stack[-1] or bool(PLATFORM_CONDITIONAL.match(line))
            elif kind == "endif" and stack:
                stack.pop()
            continue
        registered = RUNNER_REGISTRATION.match(line)
        if registered and any(stack):
            out.add(registered.group(1))
    return out


def platform_suites(repo, revs):
    out = set()
    for rev in revs:
        names = git(repo, "ls-tree", "-r", "-z", "--name-only", rev, "--", "tests")
        paths = sorted(p for p in names.decode("utf-8").split("\0") if p.endswith(".c"))
        for path, text in blobs(repo, rev, paths).items():
            if path == RUNNER:
                out |= runner_conditional_suites(text)
            lines = text.splitlines()
            if not any(PLATFORM_CONDITIONAL.match(l) or SKIP_PLATFORM.search(l) for l in lines):
                continue
            for line in lines:
                defined = SUITE_DEFINITION.match(line)
                if defined:
                    out.add(defined.group(1) or defined.group(2))
    return sorted(out)


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
    f.add_argument("--whole-suites")
    p = sub.add_parser("platform-suites")
    p.add_argument("--repo", required=True)
    p.add_argument("--rev", action="append", required=True)
    args = parser.parse_args()
    # LF on every host, like the files below: a Windows python would
    # otherwise write CRLF.
    sys.stdout.reconfigure(newline="\n")
    if args.command == "suites":
        with open(args.answer, encoding="utf-8") as handle:
            print("\n".join(suites(json.load(handle))))
        return 0
    if args.command == "platform-suites":
        try:
            found = platform_suites(args.repo, args.rev)
        except (OSError, subprocess.CalledProcessError, UnicodeDecodeError) as error:
            print(f"selection: platform suites unknown: {error}", file=sys.stderr)
            return 1
        print("".join(suite + "\n" for suite in found), end="")
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
    optional = conditional_suites(answer, args.lane) if code == NARROWED else []
    if code == NARROWED and args.whole_suites:
        try:
            with open(args.whole_suites, encoding="utf-8") as handle:
                whole = [line.strip() for line in handle if line.strip()]
        except OSError as error:
            return run_all(f"unreadable whole-suite list ({error})")
        bad = [suite for suite in whole if not TOKEN.match(suite) or ":" in suite]
        if bad:
            return run_all(f"whole-suite list unusable ({bad[:3]})")
        lines, optional = whole_view(lines, optional, whole)
    if code in (NARROWED, NOTHING):
        with open(args.out, "w", encoding="utf-8", newline="\n") as handle:
            handle.write("".join(line + "\n" for line in lines))
        if args.optional_out:
            with open(args.optional_out, "w", encoding="utf-8", newline="\n") as handle:
                handle.write("".join(suite + "\n" for suite in optional))
    return code


if __name__ == "__main__":
    sys.exit(main())
