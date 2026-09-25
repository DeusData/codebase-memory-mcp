#!/usr/bin/env bash
# memwaste.sh — run the waste sanitizer on a repository and print the ranked report.
#
# The sanitizer (src/foundation/mem_events.h) measures memory that is CORRECT but
# unnecessary: allocator slack, churn, realloc copying, memory that never reaches
# the memory core. This driver is the one entry point for every venue: a local
# run on a bench corpus, the PR-CI fixture run, the dry-run and release runs.
#
# Usage:
#   scripts/memwaste.sh <repo-path> [--lane event|access] [--top N] [--sort METRIC]
#                                   [--out DIR] [--no-build] [--json] [--binary PATH]
#
# --binary runs a kept copy of a flavour binary (a baseline, a replay) instead of
# the one in the build directory; it implies --no-build.
#
# What it does
#   1. builds the `memwaste` flavour (production flags + the event layer) into
#      build/memwaste, unless --no-build;
#   2. indexes <repo-path> with CBM_MEMWASTE=1 into a private cache and a private,
#      SHORT runtime directory (the coordination socket has a 104-byte path limit);
#   3. prints the symbolised report and writes the raw dump + JSON next to it;
#   4. FAILS when the measurement itself is unsound: a table overflowed, or more
#      than 0.1 % of frees were of blocks the layer never saw. A sanitizer that
#      silently loses records produces false confidence.
#
# Nothing here reads a clock to decide anything: wall time is printed as
# information only. Gates are counts.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO=""
TOP=25
SORT=bytes
OUT=""
BUILD=1
JSON=0
LANE=event
BINARY_OVERRIDE=""

while [ $# -gt 0 ]; do
    case "$1" in
    --binary) BINARY_OVERRIDE="$2"; BUILD=0; shift 2 ;;
    --top) TOP="$2"; shift 2 ;;
    --sort) SORT="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --no-build) BUILD=0; shift ;;
    --json) JSON=1; shift ;;
    --lane) LANE="$2"; shift 2 ;;
    -h | --help) sed -n '2,24p' "$0"; exit 0 ;;
    # A typo'd flag must never be read as the repository path — it would index
    # nothing and report an empty lane as a clean one.
    -*) echo "memwaste.sh: unknown argument $1. Please consult --help." >&2; exit 2 ;;
    *) REPO="$1"; shift ;;
    esac
done

if [ -z "$REPO" ] || [ ! -d "$REPO" ]; then
    echo "memwaste.sh: a repository directory is required. Please consult --help." >&2
    exit 2
fi

case "$LANE" in
event) BUILD_ARGS=(BUILD_DIR=build/memwaste MEMWASTE=1 ${CC:+CC=$CC} ${CXX:+CXX=$CXX})
    BIN="$ROOT/build/memwaste/codebase-memory-mcp" ;;
access) BUILD_ARGS=(BUILD_DIR=build/memwaste-access MEMWASTE_ACCESS=1 CC=clang CXX=clang++)
    BIN="$ROOT/build/memwaste-access/codebase-memory-mcp" ;;
*) echo "memwaste.sh: --lane must be event or access" >&2; exit 2 ;;
esac
if [ "$BUILD" -eq 1 ]; then
    "$ROOT/scripts/build.sh" "${BUILD_ARGS[@]}" >/dev/null
fi
if [ -n "$BINARY_OVERRIDE" ]; then
    BIN="$BINARY_OVERRIDE"
fi
if [ ! -x "$BIN" ]; then
    echo "memwaste.sh: $BIN is missing (build failed, or --no-build without a build)" >&2
    exit 2
fi

if [ -z "$OUT" ]; then
    OUT="$(mktemp -d "${TMPDIR:-/tmp}/cbm-memwaste.XXXXXX")"
fi
mkdir -p "$OUT"
DUMP="$OUT/memwaste.jsonl"
rm -f "$DUMP"

RUN_BIN="$BIN"
if command -v cygpath >/dev/null 2>&1 && ! command -v winepath >/dev/null 2>&1; then
    # Native Windows: the server walks the ancestor chain of its binary, cache and
    # runtime directories and refuses any that grant mutation to untrusted SIDs.
    # The checkout and msys /tmp both do, so the run happens from a root under
    # USERPROFILE stamped to the current user, SYSTEM and Administrators -- by
    # SID, the way scripts/memlab.sh does it (#1532).
    PROFILE_DIR="$(cygpath -u "$USERPROFILE")"
    WORK="$(mktemp -d "$PROFILE_DIR/cbm-mw.XXXXXX")"
    WORK_W="$(cygpath -w "$WORK")"
    SID_QUERY='[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value'
    ME="$(powershell.exe -NoProfile -NonInteractive -Command "$SID_QUERY" 2>/dev/null | tr -d '\r\n')"
    case "$ME" in
    S-1-*) ME="*$ME" ;;
    *) echo "memwaste.sh: could not resolve the current user's SID" >&2; exit 2 ;;
    esac
    if ! MSYS2_ARG_CONV_EXCL='*' icacls "$WORK_W" /inheritance:r \
        /grant:r "${ME}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' /Q >/dev/null 2>&1; then
        echo "memwaste.sh: could not stamp $WORK" >&2
        exit 2
    fi
    mkdir -p "$WORK/cache" "$WORK/rt"
    cp "$BIN.exe" "$WORK/codebase-memory-mcp.exe" 2>/dev/null || cp "$BIN" "$WORK/codebase-memory-mcp.exe"
    RUN_BIN="$WORK/codebase-memory-mcp.exe"
    CACHE_DIR="$WORK_W\\cache"
    RUNTIME="$WORK_W\\rt"
    DUMP_ENV="$(cygpath -w "$DUMP")"
else
    WORK="$(mktemp -d /tmp/cbm-mw.XXXXXX)"
    chmod 700 "$WORK"
    # A fresh cache every run: an existing index turns --mode full into an
    # incremental pass that measures hashing instead of indexing.
    rm -rf "$OUT/cache"
    mkdir -p "$OUT/cache"
    CACHE_DIR="$OUT/cache"
    RUNTIME="$WORK"
    DUMP_ENV="$DUMP"
fi

# CBM_MEMWASTE_KEEP=1 keeps the private root (binary copy, cache, logs) for inspection.
cleanup() {
    if [ "${CBM_MEMWASTE_KEEP:-0}" != 1 ]; then
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

INDEX_RC=0
CBM_MEMWASTE=1 CBM_MEMWASTE_OUT="$DUMP_ENV" CBM_CACHE_DIR="$CACHE_DIR" CBM_RUNTIME_DIR="$RUNTIME" \
    CBM_LOG_LEVEL=error "$RUN_BIN" cli index_repository --repo-path "$REPO" --mode full \
    >"$OUT/index.json" || INDEX_RC=$?
if [ "$INDEX_RC" -ne 0 ]; then
    # A failed index is a finding about the flavour, never a silent partial report.
    echo "memwaste.sh: the index failed (rc=$INDEX_RC); its logs follow" >&2
    for log in "$WORK"/cache/logs/* "$WORK"/cache/logs/.worker-log-* "$OUT"/cache/logs/* "$OUT"/cache/logs/.worker-log-*; do
        if [ -f "$log" ]; then
            echo "--- $log" >&2
            tail -n 60 "$log" >&2
        fi
    done
    exit 1
fi

if [ ! -s "$DUMP" ]; then
    echo "memwaste.sh: the sanitizer wrote no dump -- is the flavour built with MEMWASTE=1?" >&2
    exit 1
fi

python3 "$ROOT/scripts/memwaste-report.py" "$DUMP" --binary "$BIN" --json >"$OUT/memwaste.json"
if [ "$JSON" -eq 1 ]; then
    cat "$OUT/memwaste.json"
else
    python3 "$ROOT/scripts/memwaste-report.py" "$DUMP" --binary "$BIN" --top "$TOP" --sort "$SORT"
    echo
    echo "raw dump: $DUMP"
    echo "json:     $OUT/memwaste.json"
fi

# Soundness of the measurement itself.
python3 - "$OUT/memwaste.json" <<'PY'
import json, sys
procs = json.load(open(sys.argv[1]))["processes"]
bad = []
for pid, proc in procs.items():
    h = proc["header"]
    if h.get("site_table_full", 0) or h.get("pointer_table_full", 0):
        bad.append(f"process {pid}: a table overflowed (sites={h.get('site_table_full')}, pointers={h.get('pointer_table_full')})")
    frees = h.get("frees", 0) + h.get("untracked_frees", 0)
    if frees and h.get("untracked_frees", 0) * 1000 > frees:
        bad.append(f"process {pid}: {h['untracked_frees']:,} of {frees:,} frees were of blocks the layer never saw (> 0.1 %)")
if bad:
    print("memwaste.sh: UNSOUND MEASUREMENT", file=sys.stderr)
    for b in bad:
        print("  " + b, file=sys.stderr)
    sys.exit(1)
PY
