#!/usr/bin/env bash
# test-impact-shadow.sh — the PR test-impact SHADOW: predict from the code graph
# which C test suites a change can affect, and publish the prediction. It is
# REPORT-ONLY: pr.yml runs it continue-on-error and ci-ok does not need it, so
# it can never block or pass a merge. Its predictions accumulate next to the
# lanes that actually ran and failed, which is how graph-based selection gets
# evaluated before anyone gates on it.
#
# The binary is the latest PUBLISHED release, not this checkout's build: a few
# seconds of download instead of minutes of compile, and a PR that breaks the
# indexer cannot corrupt its own prediction. pr.yml builds the checkout only
# if the download fails.
#
# Usage: scripts/ci/test-impact-shadow.sh --fetch-release DIR [--repo OWNER/REPO]
#          Download the latest release's archive for this host + checksums.txt
#          into DIR, verify the SHA-256, extract DIR/codebase-memory-mcp.
#          Needs GH_TOKEN (read-only); $GH overrides the gh command.
#        scripts/ci/test-impact-shadow.sh --binary PATH --out DIR [--base REV] [--depth N]
#          Index this checkout into a throwaway cache (HOME, XDG dirs and
#          CBM_CACHE_DIR under DIR/work, a private CBM_RUNTIME_DIR under /tmp:
#          the daemon's socket path must fit sun_path), run
#          detect_changes against REV (default HEAD^1, the base side of a PR
#          merge commit) at depth N (default 15), and let
#          scripts/test-impact/predict.py map it to suites with the lane
#          selector's run-all triggers. Writes DIR/prediction.json and
#          DIR/summary.md (appended to $GITHUB_STEP_SUMMARY when set). The
#          graph is only consulted when C sources changed.
# Exit: 0 = prediction written · 1 = the tooling broke · 2 = usage error.
set -euo pipefail

usage() { sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'; }

FETCH_DIR=""
REPO="${GITHUB_REPOSITORY:-DeusData/codebase-memory-mcp}"
BINARY=""
OUT=""
BASE="HEAD^1"
DEPTH=15
while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help)
        usage
        exit 0
        ;;
    --fetch-release | --repo | --binary | --out | --base | --depth)
        if [ $# -lt 2 ]; then
            echo "test-impact-shadow.sh: $1 needs a value. Please consult --help." >&2
            exit 2
        fi
        case "$1" in
        --fetch-release) FETCH_DIR="$2" ;;
        --repo) REPO="$2" ;;
        --binary) BINARY="$2" ;;
        --out) OUT="$2" ;;
        --base) BASE="$2" ;;
        --depth) DEPTH="$2" ;;
        esac
        shift
        ;;
    *)
        echo "test-impact-shadow.sh: unknown argument '$1'. Please consult --help." >&2
        exit 2
        ;;
    esac
    shift
done

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

sha256_check() { # file with "<sha>  <name>" lines, run in the archive's dir
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum -c "$1"
    else
        shasum -a 256 -c "$1"
    fi
}

if [ -n "$FETCH_DIR" ]; then
    case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) asset=codebase-memory-mcp-linux-amd64.tar.gz ;;
    Linux/aarch64 | Linux/arm64) asset=codebase-memory-mcp-linux-arm64.tar.gz ;;
    Darwin/arm64) asset=codebase-memory-mcp-darwin-arm64.tar.gz ;;
    Darwin/x86_64) asset=codebase-memory-mcp-darwin-amd64.tar.gz ;;
    *)
        echo "test-impact-shadow.sh: no release archive for $(uname -s)/$(uname -m)" >&2
        exit 1
        ;;
    esac
    read -r -a gh_cmd <<<"${GH:-gh}"
    mkdir -p "$FETCH_DIR"
    # No tag: the latest release.
    "${gh_cmd[@]}" release download --repo "$REPO" --pattern "$asset" \
        --pattern checksums.txt --dir "$FETCH_DIR" --clobber
    grep " $asset\$" "$FETCH_DIR/checksums.txt" >"$FETCH_DIR/asset.sha256"
    (cd "$FETCH_DIR" && sha256_check asset.sha256)
    tar -xzf "$FETCH_DIR/$asset" -C "$FETCH_DIR" codebase-memory-mcp
    echo "release binary: $FETCH_DIR/codebase-memory-mcp ($asset, SHA-256 verified)"
    exit 0
fi

if [ -z "$BINARY" ] || [ -z "$OUT" ]; then
    echo "test-impact-shadow.sh: --binary and --out are required. Please consult --help." >&2
    exit 2
fi
case "$DEPTH" in
'' | *[!0-9]*)
    echo "test-impact-shadow.sh: --depth must be a number. Please consult --help." >&2
    exit 2
    ;;
esac
BINARY="$(cd "$(dirname "$BINARY")" && pwd)/$(basename "$BINARY")"
PREDICT="$ROOT/scripts/test-impact/predict.py"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
WORK="$OUT/work"
rm -rf "$WORK"
mkdir -p "$WORK/home/.config" "$WORK/home/.cache" "$WORK/cache"
# <runtime>/cbm-daemon-<uid>/cbm-<key>.sock adds ~42 bytes to this path, and
# sun_path holds 104 (macOS) / 108: a deep $OUT or $TMPDIR does not fit.
RUNTIME=$(mktemp -d /tmp/cbm-rt.XXXXXX)
trap 'rm -rf "$RUNTIME"' EXIT

BASE_SHA=$(git -C "$ROOT" rev-parse --verify "$BASE^{commit}")
git -C "$ROOT" diff --name-only --no-renames "$BASE_SHA" HEAD >"$OUT/changed-files.txt"
bash "$ROOT/scripts/ci/select-lanes.sh" "$OUT/changed-files.txt" >"$OUT/selection.json"

# From here on nothing may touch the host's real config, cache or daemon.
export HOME="$WORK/home"
export XDG_CONFIG_HOME="$WORK/home/.config"
export XDG_CACHE_HOME="$WORK/home/.cache"
export CBM_CACHE_DIR="$WORK/cache"
export CBM_RUNTIME_DIR="$RUNTIME"

VERSION=$("$BINARY" --version 2>/dev/null | head -1 || true)
DETECT_ARGS=()
T_INDEX="-"
T_DETECT="-"
if [ "$(python3 "$PREDICT" --needs-graph --changed "$OUT/changed-files.txt")" = yes ]; then
    python3 -c 'import json, sys; json.dump({"repo_path": sys.argv[1]}, open(sys.argv[2], "w"))' \
        "$ROOT" "$WORK/index-args.json"
    t0=$(date +%s)
    "$BINARY" cli --quiet index_repository --args-file "$WORK/index-args.json" >"$WORK/index.json"
    t1=$(date +%s)
    python3 -c '
import json, sys
project = json.load(open(sys.argv[1]))["project"]
json.dump({"project": project, "base_branch": sys.argv[2], "depth": int(sys.argv[3]),
           "limit": 5000, "changed_limit": 5000, "module_limit": 0,
           "max_output_tokens": 1000000, "format": "json"}, open(sys.argv[4], "w"))
' "$WORK/index.json" "$BASE_SHA" "$DEPTH" "$WORK/detect-args.json"
    "$BINARY" cli --quiet detect_changes --args-file "$WORK/detect-args.json" >"$WORK/detect.json"
    t2=$(date +%s)
    # The CLI started a private daemon under $RUNTIME; stop it (best effort).
    "$BINARY" daemon stop >/dev/null 2>&1 || true
    T_INDEX="$((t1 - t0))s"
    T_DETECT="$((t2 - t1))s"
    DETECT_ARGS=(--detect "$WORK/detect.json")
fi

META=$(python3 -c 'import json, sys; print(json.dumps(dict(zip(sys.argv[1::2], sys.argv[2::2]))))' \
    binary "${VERSION:-unknown}" base "$BASE_SHA" "index time" "$T_INDEX" "detect time" "$T_DETECT")
python3 "$PREDICT" --root "$ROOT" --changed "$OUT/changed-files.txt" \
    --selection "$OUT/selection.json" ${DETECT_ARGS[@]+"${DETECT_ARGS[@]}"} --depth "$DEPTH" \
    --meta "$META" --out "$OUT/prediction.json" --summary "$OUT/summary.md"
cat "$OUT/summary.md"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    cat "$OUT/summary.md" >>"$GITHUB_STEP_SUMMARY"
fi
