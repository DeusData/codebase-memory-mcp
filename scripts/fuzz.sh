#!/usr/bin/env bash
# fuzz.sh — run the libFuzzer targets of build/fuzz/cbm-fuzz.
#
# Usage:
#   scripts/fuzz.sh <extract|cypher|config|all> [--runs N] [--max-total-time S]
#                   [--jobs N] [--seed N] [--no-build]
#
# Two shapes, one driver:
#   --runs N --seed S    a fixed number of executions from a fixed seed: the same
#                        inputs every time, so a smoke in CI is a function of the
#                        code (O9). This is the default (--runs 20000 --seed 1).
#   --max-total-time S   exploration for dry-run and nightly: new inputs are the
#                        point, and a crash is saved with its reproducer.
#
# The checked-in seeds (tests/fuzz/corpus/<target>/) are copied into a working
# corpus under build/fuzz/, so a run never rewrites the repository. A crash,
# leak or timeout leaves its input in build/fuzz/artifacts-<target>/ and the
# script exits non-zero; reproduce with
#   CBM_FUZZ_TARGET=<target> build/fuzz/cbm-fuzz <artifact>
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# --help before the positional target: the venue-parity interface probe calls
# every canonical entry as `<entry> --help` with no other argument.
case "${1:-}" in
-h | --help) sed -n '2,21p' "$0"; exit 0 ;;
esac
TARGET="${1:-}"
[ $# -gt 0 ] && shift
RUNS=20000
SEED=1
MAX_TIME=""
JOBS=1
BUILD=1
while [ $# -gt 0 ]; do
    case "$1" in
    --runs) RUNS="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --max-total-time) MAX_TIME="$2"; RUNS=""; shift 2 ;;
    --jobs) JOBS="$2"; shift 2 ;;
    --no-build) BUILD=0; shift ;;
    -h | --help) sed -n '2,21p' "$0"; exit 0 ;;
    *) echo "fuzz.sh: unknown argument $1. Please consult --help." >&2; exit 2 ;;
    esac
done

case "$TARGET" in
extract | cypher | config) TARGETS=("$TARGET") ;;
all) TARGETS=(extract cypher config) ;;
*) echo "fuzz.sh: target must be extract, cypher, config or all. Please consult --help." >&2
    exit 2 ;;
esac

BIN="$ROOT/build/fuzz/cbm-fuzz"
if [ "$BUILD" -eq 1 ]; then
    make -C "$ROOT" -f Makefile.cbm -j"$(getconf _NPROCESSORS_ONLN)" fuzz >/dev/null
fi
if [ ! -x "$BIN" ]; then
    echo "fuzz.sh: $BIN is missing (build failed, or --no-build without a build)" >&2
    exit 2
fi

status=0
for t in "${TARGETS[@]}"; do
    corpus="$ROOT/build/fuzz/corpus-$t"
    artifacts="$ROOT/build/fuzz/artifacts-$t/"
    mkdir -p "$corpus" "$artifacts"
    cp -R "$ROOT/tests/fuzz/corpus/$t/." "$corpus/"
    args=(-artifact_prefix="$artifacts" -seed="$SEED" -rss_limit_mb=4096 -timeout=30 -print_final_stats=1)
    if [ -n "$RUNS" ]; then
        args+=(-runs="$RUNS")
    else
        args+=(-max_total_time="$MAX_TIME")
    fi
    if [ "$JOBS" -gt 1 ]; then
        args+=(-jobs="$JOBS" -workers="$JOBS")
    fi
    echo "=== fuzz $t: ${RUNS:+$RUNS runs}${MAX_TIME:+$MAX_TIME s} (seed $SEED) ==="
    if ! CBM_FUZZ_TARGET="$t" "$BIN" "${args[@]}" "$corpus"; then
        echo "fuzz.sh: target $t FAILED -- reproducer in $artifacts" >&2
        status=1
    fi
done
exit "$status"
