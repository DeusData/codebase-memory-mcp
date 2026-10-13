#!/usr/bin/env bash
# Lane-selector contract: scripts/ci/select-lanes.sh decides which PR lanes a
# change set runs, so a wrong answer is a SILENT gate loss -- a lane that
# should have caught a regression simply never starts, and ci-ok (which only
# demands the lanes that were selected) stays green. This pins the decision
# table case by case (tests/fixtures/select-lanes/cases.tsv), and the
# properties the workflows lean on:
#   - fail-safe: an unmatched path, an empty list, the selector itself, any
#     workflow, the test harness, Makefile.cbm and vendored code select FULL;
#   - determinism: order and duplicates never change a byte of the output;
#   - one decision per input channel: stdin, a file, --batch and the
#     github-output form all agree;
#   - a truncated file list (the pulls/files API stops at 3000) selects FULL.
# The history replay (every September PR push + the real failures) is
# scripts/test-impact/replay-selector.sh, a separate step.
#
# Usage: tests/test_select_lanes.sh [repo-root]

set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
SELECTOR="$ROOT/scripts/ci/select-lanes.sh"
CASES="$ROOT/tests/fixtures/select-lanes/cases.tsv"
for f in "$SELECTOR" "$CASES"; do
    if [ ! -f "$f" ]; then
        echo "FAIL: $f not found" >&2
        exit 1
    fi
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/cbm-select-lanes.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

# Embedded Python launches the selector through the CURRENT bash ($BASH, by
# explicit path): a bare "bash" argv can resolve to the WSL alias on Windows.
python3 - "$BASH" "$SELECTOR" "$CASES" "$WORK" <<'PY'
import json
import pathlib
import random
import subprocess
import sys

bash, selector, cases_path, work = sys.argv[1:5]
work = pathlib.Path(work)
failures = []

# Every lane of the full PR pipeline, spelled out here rather than read from
# the selector, so a lane silently dropped from the selector's universe fails.
FULL = {
    "codeql-gate", "diag", "license-gate", "lint", "lint-mem", "lsan-macos",
    "memwaste", "msan", "pkg-wrappers", "security-static",
    "shard-completeness", "smoke-mac", "smoke-ubuntu", "smoke-win",
    "tsan-arm", "tsan-mac", "tsan-x86", "unix-arm64", "unix-macos-intel",
    "unix-macos14", "unix-x86", "windows", "windows-guards",
}
UNIVERSE = FULL | {"contracts"}


def run(args, stdin=""):
    proc = subprocess.run([bash, selector, *args], input=stdin,
                          capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def expand(spec):
    words = spec.split()
    if words and words[0] == "FULL":
        return FULL - {w[1:] for w in words[1:]}
    return set(words)


cases = []
for number, line in enumerate(open(cases_path, encoding="utf-8"), 1):
    line = line.rstrip("\r\n")
    if not line or line.startswith("#"):
        continue
    cols = line.split("\t")
    if len(cols) != 5:
        failures.append(f"cases.tsv:{number}: expected 5 TAB-separated columns")
        continue
    name, files, tier, full, lanes = cols
    files = [] if files == "-" else files.split()
    cases.append((name, files, tier, full == "true", expand(lanes)))

# 1. The decision table, one selector run per case (stdin channel).
single = {}
for name, files, tier, full, lanes in cases:
    rc, out, err = run([], "".join(f + "\n" for f in files))
    if rc != 0:
        failures.append(f"{name}: selector exited {rc}: {err.strip()}")
        continue
    got = json.loads(out)
    single[name] = out
    if got["tier"] != tier:
        failures.append(f"{name}: tier {got['tier']}, expected {tier}")
    if got["full"] is not full:
        failures.append(f"{name}: full={got['full']}, expected {full}")
    if set(got["lanes"]) != lanes:
        failures.append(
            f"{name}: lanes differ -- missing {sorted(lanes - set(got['lanes']))}, "
            f"unexpected {sorted(set(got['lanes']) - lanes)}")
    if got["lanes"] != sorted(got["lanes"]) or got["reasons"] != sorted(got["reasons"]):
        failures.append(f"{name}: lanes/reasons are not sorted")
    if full and not any(r.startswith("full:") for r in got["reasons"]):
        failures.append(f"{name}: full=true without a 'full:' reason")

# 2. Determinism: shuffled, duplicated, CRLF-terminated input with blank lines
#    is byte-identical to the canonical run.
rng = random.Random(1)
for name, files, *_ in cases:
    if name not in single or not files:
        continue
    noisy = files * 2
    rng.shuffle(noisy)
    rc, out, _ = run([], "\n" + "".join(f + "\r\n" for f in noisy) + "\n")
    if rc != 0 or out != single[name]:
        failures.append(f"{name}: output depends on order/duplicates/CRLF")

# 3. File channel == stdin channel; --batch == one run per case.
probe = cases[0]
listing = work / "files.txt"
listing.write_text("".join(f + "\n" for f in probe[1]), encoding="utf-8")
rc, out, _ = run([str(listing)])
if rc != 0 or out != single.get(probe[0]):
    failures.append("file argument and stdin disagree")
batch = work / "batch.jsonl"
batch.write_text("".join(json.dumps({"id": n, "files": f}) + "\n"
                         for n, f, *_ in cases), encoding="utf-8")
rc, out, err = run(["--batch", str(batch)])
if rc != 0:
    failures.append(f"--batch exited {rc}: {err.strip()}")
else:
    for row in map(json.loads, out.splitlines()):
        ident = row.pop("id")
        if json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" != single.get(ident):
            failures.append(f"--batch disagrees with a single run for {ident}")

# 4. github-output form carries the same decision as key=value lines.
rc, out, _ = run(["--format", "github-output"], "src/pipeline/a.c\n")
kv = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
want = json.loads(single["pipeline"])
if rc != 0 or kv.get("tier") != want["tier"] or kv.get("full") != "false" \
        or json.loads(kv.get("lanes", "null")) != want["lanes"]:
    failures.append(f"--format github-output disagrees with json: {out!r}")

# 5. The lane universe is exactly the PR pipeline's lanes (+ contracts).
rc, out, _ = run(["--list-lanes"])
if rc != 0 or out.split() != sorted(UNIVERSE):
    failures.append(f"--list-lanes is {out.split()}, expected {sorted(UNIVERSE)}")

# 6. Interface: --help answers, an unknown flag is a usage error (exit 2).
rc, out, _ = run(["--help"])
if rc != 0 or "Usage:" not in out:
    failures.append("--help must exit 0 and print a Usage: block")
rc, _, err = run(["--definitely-not-a-flag"])
if rc != 2 or "Please consult --help." not in err:
    failures.append("an unknown flag must exit 2 with 'Please consult --help.'")

# 7. A truncated file list is never trusted. The pulls/files API lists at most
#    3000 files, so fewer distinct paths than the PR's changed_files, a PR at
#    the cap, or a list that reaches the cap selects FULL (file_list_truncated);
#    the tier still classifies what was listed. A complete list decides as usual
#    (renames add their old path, so a complete list may exceed the count).
DOCS = ["README.md", "docs/a.md", "docs/b.md"]
CAP_LIST = [f"docs/page{i:04d}.md" for i in range(3000)]


def truncation(label, files, args, want_full):
    rc, out, err = run(args, "".join(f + "\n" for f in files))
    if rc != 0:
        failures.append(f"truncation: {label}: selector exited {rc}: {err.strip()}")
        return
    got = json.loads(out)
    flagged = any("file_list_truncated" in r for r in got["reasons"])
    if got["full"] is not want_full or flagged is not want_full:
        failures.append(f"truncation: {label}: full={got['full']}, file_list_truncated "
                        f"reason={flagged}, expected {want_full}")
    if want_full and set(got["lanes"]) != FULL:
        failures.append(f"truncation: {label}: a truncated list must select every lane")
    if got["tier"] != "T0a-docs":
        failures.append(f"truncation: {label}: tier {got['tier']}, expected T0a-docs")


truncation("fewer paths than changed_files", DOCS, ["--expect-files", "5"], True)
truncation("complete list, count matches", DOCS, ["--expect-files", "3"], False)
truncation("renames list their old path too", DOCS, ["--expect-files", "2"], False)
truncation("a PR at the 3000-file cap", DOCS, ["--expect-files", "3000"], True)
truncation("a list that reaches the cap", CAP_LIST, [], True)
truncation("a complete list just under the cap", CAP_LIST[:2999], ["--expect-files", "2999"], False)
rc, out, _ = run(["--format", "github-output", "--expect-files", "5"], "".join(f + "\n" for f in DOCS))
if rc != 0 or "full=true" not in out.splitlines():
    failures.append(f"truncation: github-output form must say full=true: {out!r}")
trunc_batch = work / "trunc.jsonl"
trunc_batch.write_text(json.dumps({"id": "t", "files": DOCS, "expect_files": 5}) + "\n", encoding="utf-8")
rc, out, _ = run(["--batch", str(trunc_batch)])
if rc != 0 or not json.loads(out.splitlines()[0])["full"]:
    failures.append("truncation: --batch rows must honour expect_files")
for bad in ("many", ""):   # "" = a workflow whose count went missing
    rc, _, err = run(["--expect-files", bad], "README.md\n")
    if rc != 2 or "Please consult --help." not in err:
        failures.append(f"--expect-files {bad!r} must be a usage error (exit 2), never "
                        f"a silently skipped truncation check")

if failures:
    print("LANE SELECTOR CONTRACT VIOLATED:")
    for failure in failures:
        print(f"  {failure}")
    sys.exit(1)
print(f"lane-selector contract OK ({len(cases)} cases, determinism, "
      f"channels, universe of {len(UNIVERSE)} lanes)")
PY
