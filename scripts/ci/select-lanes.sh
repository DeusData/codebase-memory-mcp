#!/usr/bin/env bash
# select-lanes.sh — decide which PR CI lanes a change set must run.
#
# Canonical CI step (called by pr.yml's `changes` job on the PR's file list).
# This is the R1 rule set, measured against every PR push of September 2026
# (767 pushes; scripts/test-impact/replay-selector.sh replays them, plus the 22
# real failures, as a contract): each changed path gets a category, the
# categories give a tier, the tier gives a lane set, and a few categories add
# lanes of their own. `tier` is the classification the analysis measured;
# `full` overrides it with every lane.
#
# It FAILS SAFE: full=true for tier T5, a path no rule knows, an empty file
# list, a truncated one (file_list_truncated: the pulls/files API stops at
# 3000 files), this script, any workflow, the test harness, Makefile.cbm
# (source registration and flag changes look identical in a file list) and
# vendored code. It is a pure function of the file SET (and the expected
# count): order, duplicates, blank lines and CRLF never change a byte of the
# output.
#
# Usage: scripts/ci/select-lanes.sh [--format json|github-output]
#                                   [--expect-files N] [FILE]
#          FILE (default stdin): changed paths, one per line.
#          --expect-files N: the PR's changed_files; fewer distinct paths, or
#          N or the list at the 3000-file API cap, means truncated -> full.
#          json (default): {"full","lanes","reasons","tier"} on one line.
#          github-output: tier=, full=, lanes=, reasons= lines ($GITHUB_OUTPUT).
#        scripts/ci/select-lanes.sh --batch FILE
#          FILE: JSON lines {"id":..., "files":[...], "expect_files"?: N};
#          prints one json result per line with its "id" (the history
#          replay's single process).
#        scripts/ci/select-lanes.sh --list-lanes
#          Every lane name, one per line (the universe ci-ok checks against).
#        scripts/ci/select-lanes.sh --codeql-scope LANGUAGE [FILE]
#          The paths of FILE (default stdin) in that CodeQL language's PR
#          scope (c-cpp, javascript-typescript, python, actions), one per line.
#          The codeql-* lanes are selected by these scopes; the interpreted
#          ones follow .github/codeql/codeql-config.yml (paths/paths-ignore).
# Exit: 0 = decided · 2 = usage error.
set -euo pipefail

usage() { sed -n '2,38p' "$0" | sed 's/^# \{0,1\}//'; }

FORMAT=json
MODE="select"
CODEQL_LANGUAGE=""
INPUT=""
EXPECT=""
while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help)
        usage
        exit 0
        ;;
    --format)
        FORMAT="${2:-}"
        [ $# -gt 1 ] && shift
        ;;
    --format=*) FORMAT="${1#--format=}" ;;
    --batch)
        MODE="batch"
        INPUT="${2:-}"
        [ $# -gt 1 ] && shift
        ;;
    --list-lanes) MODE="list" ;;
    --codeql-scope)
        MODE="codeql-scope"
        CODEQL_LANGUAGE="${2:-}"
        [ $# -gt 1 ] && shift
        ;;
    --expect-files)
        # An empty value (a workflow whose count went missing) must fail
        # loudly, never quietly skip the truncation check.
        EXPECT="${2:-}"
        [ -n "$EXPECT" ] || EXPECT="missing"
        [ $# -gt 1 ] && shift
        ;;
    -*)
        echo "select-lanes.sh: unknown option '$1'. Please consult --help." >&2
        exit 2
        ;;
    *)
        if [ -n "$INPUT" ]; then
            echo "select-lanes.sh: more than one input file. Please consult --help." >&2
            exit 2
        fi
        INPUT="$1"
        ;;
    esac
    shift
done
case "$FORMAT" in
json | github-output) ;;
*)
    echo "select-lanes.sh: --format must be json or github-output. Please consult --help." >&2
    exit 2
    ;;
esac
case "${EXPECT:-0}" in
*[!0-9]*)
    echo "select-lanes.sh: --expect-files takes a file count. Please consult --help." >&2
    exit 2
    ;;
esac
if [ "$MODE" = batch ] && [ -z "$INPUT" ]; then
    echo "select-lanes.sh: --batch needs a file. Please consult --help." >&2
    exit 2
fi
if [ -n "$INPUT" ] && [ ! -f "$INPUT" ]; then
    echo "select-lanes.sh: no such file '$INPUT'. Please consult --help." >&2
    exit 2
fi

# The heredoc below is the program, so stdin input is staged in a file first.
STAGED=""
case "$MODE:$CODEQL_LANGUAGE" in
codeql-scope:c-cpp | codeql-scope:javascript-typescript | codeql-scope:python | codeql-scope:actions | select:* | batch:* | list:*) ;;
*)
    echo "select-lanes.sh: --codeql-scope takes c-cpp, javascript-typescript, python or actions. Please consult --help." >&2
    exit 2
    ;;
esac
if { [ "$MODE" = select ] || [ "$MODE" = codeql-scope ]; } && [ -z "$INPUT" ]; then
    STAGED=$(mktemp "${TMPDIR:-/tmp}/cbm-select-lanes.XXXXXX")
    trap 'rm -f "$STAGED"' EXIT
    cat >"$STAGED"
    INPUT="$STAGED"
fi

CODEQL_CONFIG="$(cd "$(dirname "$0")/../.." && pwd)/.github/codeql/codeql-config.yml"
python3 - "$MODE" "$FORMAT" "$INPUT" "$EXPECT" "$CODEQL_CONFIG" "$CODEQL_LANGUAGE" <<'PY'
import json
import re
import sys

mode, fmt, source = sys.argv[1:4]
expect = int(sys.argv[4]) if sys.argv[4] else None
codeql_config, codeql_language = sys.argv[5:7]

# ── Path -> category (first matching rule wins; order is part of the rule) ──
GATE_WORKFLOWS = {"pr.yml", "_test.yml", "_lint.yml", "_security.yml",
                  "_smoke.yml", "_memwaste.yml", "codeql.yml", "dco.yml"}
BUILD = {"Makefile.cbm", "scripts/test.sh", "scripts/run-tests-parallel.sh",
         "scripts/env.sh", "scripts/build.sh", "scripts/clean.sh",
         "scripts/path-safety.sh", "scripts/test-runtime.sh", "scripts/msan.sh",
         "scripts/lint.sh", ".clang-format", ".clang-tidy",
         "scripts/vendored-checksums.txt", "scripts/run-test-wave.py",
         "scripts/embed-frontend.sh"}
SMOKE = {"scripts/smoke-test.sh", "scripts/smoke-local.sh",
         "scripts/smoke-fixture-server.py", "scripts/smoke-invariants.sh",
         "test-infrastructure/vm/vm-smoke.sh",
         "test-infrastructure/vm/windows-user-path-guard.ps1",
         "scripts/test-windows.ps1", "scripts/smoke-wrappers.sh",
         "scripts/security-fuzz.sh", "scripts/security-fuzz-random.sh",
         "scripts/security-install.sh", "scripts/security-network.sh",
         "scripts/security-strings.sh", "scripts/test_mcp_interactive.py"}
LINT_CONFIG = {"scripts/memory-core-baseline.txt", "scripts/lint-memory-core.py",
               "scripts/hooks/pre-commit", ".cppcheck", "scripts/check-no-test-skips.sh",
               "scripts/check-nolint-whitelist.sh", "scripts/lint-mem-gate.py",
               "scripts/lint-mem-whitelist.txt", "scripts/lint-tidy-diff.sh"}
# Local tools and release-only jobs: no PR lane runs them.
NON_GATING = {"scripts/package-release.sh", "scripts/benchmark-index.sh",
              "scripts/benchmark-search-graph.sh", "scripts/benchmark-streaming.py",
              "scripts/soak-test.sh", ".cbmignore", ".node-version", "flake.nix",
              "flake.lock", "scripts/soak-legs.sh", "scripts/repro.sh",
              "scripts/audit-grammar-security.sh", "scripts/check-dco.sh",
              "scripts/check-lsp-originality.sh", "scripts/clone-bench-repos.sh",
              "scripts/extract_nomic_vectors.py", "scripts/gen-pdf-unicode.py",
              "scripts/gen-py-stdlib.py", "scripts/generate-lang-code.py",
              "scripts/new-languages.json", "scripts/git-hooks/commit-msg",
              "scripts/install-git-hooks.sh", "scripts/sanitizer-coverage-map.py",
              "scripts/sanitizer-map.json", "scripts/test_mcp_rapid_init.py",
              "scripts/vendor-grammar.sh"}
# The license-gate lane runs these (and only that lane).
LICENSE_TOOLS = {"scripts/license-gate.sh", "scripts/license-gate-check.py",
                 "scripts/license-gate-check-npm.py", "scripts/license-policy.json",
                 "scripts/ui_browser_licenses.py"}
# security-static runs these; it runs on every PR (ALWAYS).
SECURITY_TOOLS = {"scripts/security-audit.sh", "scripts/security-allowlist.txt",
                  "scripts/security-network-source-audit.py", "scripts/security-ui.sh"}
# Repository files C tests read: the test legs run (a docs-only tier runs no C
# suite, so test_cli's README/site contract would never see a README change).
READ_BY_TESTS = {"scripts/security-vendored.sh", ".codebase-memory.json", "README.md",
                 "docs/index.html", "docs/llms.txt", "pkg/npm/README.md"}
# The test legs' failure report (tests/test_failure_report_contract.sh).
TEST_REPORT = {"scripts/suite-failure-report.sh"}
# The release notices ship inside every archive.
PKG_SCRIPTS = {"scripts/gen-third-party-notices.sh", "scripts/gen-ui-licenses.py"}
PKG_FILES = {"install.sh", "install.ps1", "server.json", "package.json", "manifest.json",
             "Formula"}
DOC_SUFFIXES = (".md", ".png", ".svg", ".jpg", ".gif", ".txt")
DOC_FILES = {"LICENSE", "CODEOWNERS", ".gitignore", ".gitattributes", "DCO",
             ".gitleaksignore", "glama.json"}


def category(path):
    base = path.rsplit("/", 1)[-1]
    if path == "Makefile.cbm":
        return "build"
    if path in LINT_CONFIG:
        return "lint-cfg"
    if path.startswith(("scripts/memwaste", "scripts/memlab")):
        return "memwaste-tools"
    if path in NON_GATING or path.startswith("scripts/test-impact/"):
        return "ci-other"
    if path == "scripts/test_mcp_in_process.py":
        return "test-contract"
    if path == "scripts/fuzz.sh":
        return "fuzz"
    if path == "scripts/audit-license-provenance.py":
        return "ci-gate"
    if path.startswith(("internal/cbm/vendored/grammars/", "tools/tree-sitter-")):
        return "grammar"
    if path.startswith(".github/workflows/"):
        return "ci-gate" if base in GATE_WORKFLOWS else "ci-other"
    if path.startswith(".github/codeql/"):
        return "ci-gate"
    if path.startswith(".github/"):
        if path.endswith(".md") or ("ISSUE_TEMPLATE" in path and path.endswith((".yml", ".yaml"))):
            return "docs"
        return "ci-other"
    if path.startswith("scripts/ci/"):
        return "ci-gate"
    if path in BUILD:
        return "build"
    if path in SMOKE:
        return "smoke"
    if path in LICENSE_TOOLS:
        return "license-tools"
    if path in SECURITY_TOOLS:
        return "security-tools"
    if path in READ_BY_TESTS:
        return "test"
    if path in TEST_REPORT:
        return "test-contract"
    if path in PKG_SCRIPTS:
        return "pkg"
    if path.startswith("scripts/") and path.endswith(".md"):
        return "docs"
    if path.startswith("tests/fixtures/"):
        return "test"
    if path.startswith("tests/windows/"):
        return "test-win"
    if path.startswith("tests/fuzz/"):
        return "fuzz"
    if path.startswith("tests/"):
        return "test-contract" if path.endswith((".sh", ".py", ".ps1")) else "test"
    if path.startswith(("docs/", "eval-results/")) or base in DOC_FILES or (
            path.endswith(DOC_SUFFIXES) and not path.startswith(("src/", "internal/", "scripts/"))):
        return "docs"
    if path.startswith("graph-ui/"):
        return "ui-frontend"
    if path.startswith(("pkg/", "scripts/setup")) or base in PKG_FILES:
        return "pkg"
    if path.startswith(("internal/cbm/vendored/", "vendored/")):
        return "vendored"
    m = re.match(r"internal/cbm/lsp/([a-z]+)_", path)
    if m and m.group(1) not in ("lsp", "type", "scope"):
        return "lsp:" + m.group(1)
    if path.startswith("internal/cbm/lsp/"):
        return "lsp-core"
    if path.startswith("internal/cbm/"):
        return "extract"
    if path == "src/main.c":
        return "cli"
    if path.startswith("src/foundation/"):
        return "mem-core" if re.search(r"/(mem|slab|arena|alloc|mimalloc)", path) else "foundation"
    if path.startswith("src/"):
        return "src:" + path.split("/")[1]
    if path.startswith("test-infrastructure/"):
        return "local-infra"
    if path.startswith("scripts/"):
        return "scripts-other"
    return "other"


# ── Categories -> tier (highest wins) ──
EXTRACT = {"grammar", "lsp-core", "extract"}
MID = {"pipeline", "mcp", "store", "cypher", "discover", "git", "graph_buffer",
       "semantic", "simhash", "traces", "personal_memory"}
PLATFORM = {"daemon", "cli", "watcher", "ui", "foundation", "smoke", "pkg", "test-win"}
CORE = {"mem-core", "build", "vendored", "ci-gate", "other", "scripts-other"}
LOWER = {"test", "test-contract", "fuzz", "ui-frontend", "ci-other",
         "local-infra", "docs", "lint-cfg", "memwaste-tools", "license-tools",
         "security-tools"}


def normalized(cat):
    # src/<dir>/... categories are the directory name; a directory no tier
    # names stays unknown (and therefore T5 + full).
    if cat.startswith("src:"):
        name = cat[4:]
        return name if name in MID | PLATFORM else cat
    return cat


def known(cat):
    return cat in EXTRACT | MID | PLATFORM | CORE | LOWER or cat.startswith("lsp:")


def tier_of(cats):
    if cats & CORE or not all(known(c) for c in cats):
        return "T5-core"
    if cats & PLATFORM:
        return "T4-platform"
    if cats & MID:
        return "T3-mid"
    if cats & EXTRACT or any(c.startswith("lsp:") for c in cats):
        return "T2-extract"
    if cats & {"test", "test-contract", "fuzz"}:
        return "T1-tests"
    if cats & {"ui-frontend", "ci-other", "local-infra", "lint-cfg", "memwaste-tools",
               "license-tools", "security-tools"}:
        return "T0b-nonproduct"
    return "T0a-docs"


# ── Tier -> lanes (R1) ──
# The PR CodeQL scans, one lane per language (pr.yml's `codeql` matrix). Each
# is selected by a change in its scope (codeql_scope below) and, like every
# lane, by any full run: an undeterminable diff scans every language.
CODEQL_LANES = {"c-cpp": "codeql-cpp", "javascript-typescript": "codeql-js",
                "python": "codeql-py", "actions": "codeql-actions"}
FULL = {"diag", "license-gate", "lint", "lint-mem", "lsan-macos",
        "memwaste", "msan", "pkg-wrappers", "security-static", "shard-completeness",
        "smoke-mac", "smoke-ubuntu", "smoke-win", "tsan-arm", "tsan-mac", "tsan-x86",
        "unix-arm64", "unix-macos-intel", "unix-macos14", "unix-x86", "windows",
        "windows-guards"} | set(CODEQL_LANES.values())
UNIVERSE = FULL | {"contracts"}   # contracts: the T0a/T0b job; every test leg runs them too
LINT = {"lint", "lint-mem"}
CORE_LEGS = {"unix-x86", "unix-macos14", "windows", "shard-completeness"}
TEST_LEGS = {"unix-x86", "unix-arm64", "unix-macos14", "unix-macos-intel", "windows"}
ALWAYS = {"security-static"}
R1 = {
    "T0a-docs": {"contracts"},
    # The contract steps police test-infrastructure/ and lint config too.
    "T0b-nonproduct": LINT | {"contracts"},
    "T1-tests": LINT | CORE_LEGS,
    "T2-extract": LINT | CORE_LEGS | {"tsan-x86", "smoke-ubuntu"},
    "T3-mid": LINT | CORE_LEGS | {"tsan-x86", "smoke-ubuntu"},
    "T4-platform": FULL - {"msan", "diag", "memwaste"} - set(CODEQL_LANES.values()),
    "T5-core": FULL,
}
# Category add-ons: (categories, lanes). pkg adds nothing to a docs-only set
# because a pkg path is never docs-only (it lifts the tier to T4).
ADD_ONS = [
    ({"pkg"}, {"pkg-wrappers", "smoke-ubuntu", "smoke-mac", "smoke-win"}),
    ({"vendored", "pkg", "ci-gate", "build", "license-tools"}, {"license-gate"}),
    ({"ui-frontend"}, {"windows-guards", "smoke-ubuntu"}),
    ({"memwaste-tools", "mem-core"}, {"memwaste"}),
    ({"test-win"}, {"windows", "windows-guards"}),
    # the contract steps run inside test.sh's default leg
    ({"ci-other", "test-contract", "fuzz"}, {"unix-x86"}),
]
HARNESS = {"scripts/test.sh", "scripts/run-tests-parallel.sh", "scripts/run-test-wave.py",
           "tests/test_main.c", "tests/test_framework.h"}


# ── CodeQL PR scope (the codeql-* lanes) ──
# c-cpp: any C/C++ the production build can compile -- first-party AND
# vendored (a vendored-only change is scanned: codeql.yml traces the vendored
# files a PR changes). Tests are not in the analysed build.
# Interpreted languages: their extensions inside the CodeQL config's
# paths minus paths-ignore (.github/codeql/codeql-config.yml, read below).
# A change to the CodeQL machinery itself scans every language (fail closed);
# those paths are full runs anyway, this keeps that true if a tier moves.
CODEQL_C_EXT = (".c", ".h", ".cc", ".cpp", ".cxx", ".hh", ".hpp", ".hxx", ".inc")
CODEQL_C_ROOTS = ("src/", "internal/", "vendored/")
CODEQL_EXT = {"javascript-typescript": (".js", ".mjs", ".cjs", ".jsx", ".ts", ".tsx",
                                        ".mts", ".cts", ".vue", ".html", ".htm"),
              "python": (".py",)}
CODEQL_FAIL_CLOSED_PREFIXES = (".github/workflows/", ".github/codeql/")
CODEQL_FAIL_CLOSED = {"Makefile.cbm", "scripts/build.sh", "scripts/ci/select-lanes.sh",
                      "scripts/ci/codeql-retrace-vendored.sh"}


def config_lists(path):
    """paths / paths-ignore of the CodeQL config: `key:` then `  - item` lines.
    None when the file is unreadable -- every interpreted scope then matches
    by extension alone (fail closed)."""
    try:
        lines = open(path, encoding="utf-8").read().splitlines()
    except OSError:
        return None
    lists, key = {"paths": [], "paths-ignore": []}, None
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = re.match(r"^([A-Za-z-]+):\s*$", line)
        if m:
            key = m.group(1) if m.group(1) in lists else None
            continue
        m = re.match(r"^\s+-\s+['\"]?([^'\"#]+?)['\"]?\s*$", line)
        if m and key:
            lists[key].append(m.group(1).rstrip("/"))
        elif not line.startswith((" ", "\t")):
            key = None
    return lists


CODEQL_PATHS = config_lists(codeql_config)


def under(path, prefixes):
    return any(path == p or path.startswith(p + "/") for p in prefixes)


def codeql_fail_closed(path):
    return path in CODEQL_FAIL_CLOSED or path.startswith(CODEQL_FAIL_CLOSED_PREFIXES)


def codeql_scope(language, path):
    if codeql_fail_closed(path):
        return True
    if language == "c-cpp":
        return path.startswith(CODEQL_C_ROOTS) and path.endswith(CODEQL_C_EXT)
    if CODEQL_PATHS is not None and (not under(path, CODEQL_PATHS["paths"])
                                     or under(path, CODEQL_PATHS["paths-ignore"])):
        return False
    if language == "actions":
        return path.startswith(".github/workflows/") or \
            path.rsplit("/", 1)[-1] in ("action.yml", "action.yaml")
    return path.endswith(CODEQL_EXT[language])


def triggers(path, cat):
    found = []
    if path == "scripts/ci/select-lanes.sh":
        found.append("the lane selector itself")
    if path.startswith(".github/workflows/"):
        found.append("workflow")
    if path in HARNESS:
        found.append("test harness")
    if path == "Makefile.cbm":
        found.append("Makefile.cbm (registration and flag changes are indistinguishable by name)")
    if cat == "vendored":
        found.append("vendored code")
    if cat in ("other", "scripts-other") or not known(cat):
        found.append("path matched by no rule")
    return found


LIST_CAP = 3000   # the pulls/files API lists at most this many files


def truncated(paths, expect):
    """A list that may be missing files: at or over the API cap, or shorter
    than the PR's changed_files (a complete list is never shorter: every
    changed file is listed, renames add their old path on top)."""
    if len(paths) >= LIST_CAP:
        return f"{len(paths)} paths listed, at the {LIST_CAP}-file API cap"
    if expect is not None and expect >= LIST_CAP:
        return f"the PR changes {expect} files, over the {LIST_CAP}-file API cap"
    if expect is not None and len(paths) < expect:
        return f"{len(paths)} paths listed for {expect} changed files"
    return None


def select(paths, expect=None):
    paths = sorted({p for p in paths if p})
    by_cat, by_trigger = {}, {}
    for path in paths:
        cat = normalized(category(path))
        by_cat.setdefault(cat, []).append(path)
        for trigger in triggers(path, cat):
            by_trigger.setdefault(trigger, []).append(path)
    cats = set(by_cat)
    tier = tier_of(cats)
    lanes = R1[tier] | ALWAYS
    reasons = [f"category {c}: {len(p)} file(s), e.g. {p[0]}" for c, p in by_cat.items()]
    for when, extra in ADD_ONS:
        hit = sorted(cats & when)
        if hit and tier != "T0a-docs" and not extra <= lanes:
            reasons.append(f"add-on {'/'.join(hit)}: +{' +'.join(sorted(extra - lanes))}")
            lanes = lanes | extra
    if lanes & TEST_LEGS:
        lanes = lanes | {"shard-completeness"}
    for language, lane in sorted(CODEQL_LANES.items()):
        hits = [p for p in paths if codeql_scope(language, p)]
        if hits and lane not in lanes:
            more = f" (+{len(hits) - 1} more)" if len(hits) > 1 else ""
            reasons.append(f"codeql {language}: {hits[0]}{more}")
            lanes = lanes | {lane}
    for trigger, hits in by_trigger.items():
        more = f" (+{len(hits) - 1} more)" if len(hits) > 1 else ""
        reasons.append(f"full: {trigger}: {hits[0]}{more}")
    if not paths:
        reasons.append("full: empty file list (fail safe)")
    if tier == "T5-core":
        reasons.append("full: tier T5-core")
    short = truncated(paths, expect)
    if short:
        reasons.append(f"full: file_list_truncated: {short} (fail safe)")
    full = tier == "T5-core" or bool(by_trigger) or not paths or bool(short)
    if full:
        lanes = set(FULL)
    return {"full": full, "lanes": sorted(lanes), "reasons": sorted(reasons), "tier": tier}


def dump(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def read_paths(path):
    with open(path, encoding="utf-8") as fh:   # universal newlines: CRLF == LF
        return [line.strip() for line in fh]


if mode == "list":
    print("\n".join(sorted(UNIVERSE)))
elif mode == "codeql-scope":
    scoped = sorted({p for p in read_paths(source) if p and codeql_scope(codeql_language, p)})
    if scoped:
        print("\n".join(scoped))
elif mode == "batch":
    with open(source, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                row = json.loads(line)
                print(dump({"id": row["id"], **select(row["files"], row.get("expect_files"))}))
elif fmt == "github-output":
    decision = select(read_paths(source), expect)
    print(f"tier={decision['tier']}")
    print(f"full={'true' if decision['full'] else 'false'}")
    print(f"lanes={dump(decision['lanes'])}")
    print(f"reasons={dump(decision['reasons'])}")
else:
    print(dump(select(read_paths(source), expect)))
PY
