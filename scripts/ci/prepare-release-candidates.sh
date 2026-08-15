#!/usr/bin/env bash
# Derive the three final, immutable release candidates for one existing product
# tuple from a single linker output.  Strip/signing happens here and nowhere
# after this boundary.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

usage() {
    cat <<'EOF'
Usage: scripts/ci/prepare-release-candidates.sh <goos> <goarch> \
         --binary FILE --out-dir DIR

Derive three final executables - unstripped, debug-stripped and stripped - for
exactly one of the eight release product tuples. All three come from the same
linker output
and are finalized before hashing and atomic publication. Runtime testing starts
only after VirusTotal selection, using the selected candidate.

Environment:
  STRIP       exact strip executable override
  CODESIGN    exact codesign executable override (Darwin only)
EOF
}

GOOS=""
GOARCH=""
BINARY=""
OUT_DIR=""
expect=""
for arg in "$@"; do
    case "$expect" in
    binary) BINARY="$arg"; expect=""; continue ;;
    out-dir) OUT_DIR="$arg"; expect=""; continue ;;
    esac
    case "$arg" in
    -h | --help) usage; exit 0 ;;
    --binary) expect=binary ;;
    --binary=*) BINARY="${arg#--binary=}" ;;
    --out-dir) expect=out-dir ;;
    --out-dir=*) OUT_DIR="${arg#--out-dir=}" ;;
    -*) echo "prepare-release-candidates: unknown option '$arg'" >&2; exit 2 ;;
    *)
        if [ -z "$GOOS" ]; then GOOS="$arg"
        elif [ -z "$GOARCH" ]; then GOARCH="$arg"
        else echo "prepare-release-candidates: unexpected argument '$arg'" >&2; exit 2
        fi
        ;;
    esac
done
[ -z "$expect" ] || { echo "prepare-release-candidates: --$expect needs a value" >&2; exit 2; }
[ -n "$GOOS" ] && [ -n "$GOARCH" ] && [ -n "$BINARY" ] && [ -n "$OUT_DIR" ] || {
    usage >&2
    exit 2
}

TARGET="$GOOS-$GOARCH"
case "$TARGET" in
linux-amd64 | linux-arm64 | linux-amd64-portable | linux-arm64-portable | \
darwin-amd64 | darwin-arm64 | windows-amd64 | windows-arm64) ;;
*) echo "prepare-release-candidates: unsupported release target '$TARGET'" >&2; exit 2 ;;
esac

[ -f "$BINARY" ] && [ ! -L "$BINARY" ] || {
    echo "prepare-release-candidates: input is not a regular non-symlink file: $BINARY" >&2
    exit 2
}
command -v python3 >/dev/null 2>&1 || {
    echo "prepare-release-candidates: python3 is required" >&2
    exit 2
}

BINARY="$(cd "$(dirname "$BINARY")" && pwd -P)/$(basename "$BINARY")"
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd -P)"
PUBLISHED="$OUT_DIR/$TARGET"
[ ! -e "$PUBLISHED" ] && [ ! -L "$PUBLISHED" ] || {
    echo "prepare-release-candidates: refusing to overwrite $PUBLISHED" >&2
    exit 2
}

sha256_file() {
    python3 - "$1" <<'PY'
import hashlib
import pathlib
import sys

digest = hashlib.sha256()
with pathlib.Path(sys.argv[1]).open("rb") as stream:
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
}

one_line() {
    LC_ALL=C tr '\t\r\n' '   ' | sed 's/  */ /g; s/^ //; s/ $//'
}

resolve_strip() {
    if [ -n "${STRIP:-}" ]; then
        [ -x "$STRIP" ] || command -v "$STRIP" >/dev/null 2>&1 || {
            echo "prepare-release-candidates: explicit STRIP is not executable: $STRIP" >&2
            return 1
        }
        command -v "$STRIP" 2>/dev/null || printf '%s\n' "$STRIP"
        return
    fi
    local candidate
    for candidate in llvm-strip strip; do
        if command -v "$candidate" >/dev/null 2>&1; then
            command -v "$candidate"
            return
        fi
    done
    echo "prepare-release-candidates: no strip tool is available" >&2
    return 1
}

tool_version() {
    local tool="$1" value=""
    value="$({ "$tool" --version || "$tool" -V || true; } 2>&1 | head -n 1 | one_line)"
    [ -n "$value" ] || value="$(basename "$tool") (version unavailable)"
    printf '%s\n' "$value"
}

STRIP_TOOL="$(resolve_strip)" || exit 2
STRIP_VERSION="$(tool_version "$STRIP_TOOL")"
SOURCE_SHA="$(sha256_file "$BINARY")"

WORK="$(mktemp -d "$OUT_DIR/.${TARGET}.candidates.XXXXXX")"
cleanup() {
    local status=$?
    trap - EXIT
    rm -rf "$WORK"
    exit "$status"
}
trap cleanup EXIT

STAGE="$WORK/$TARGET"
BINARY_NAME=codebase-memory-mcp
[ "$GOOS" = windows ] && BINARY_NAME=codebase-memory-mcp.exe
mkdir -p "$STAGE/unstripped" "$STAGE/debug-stripped" "$STAGE/stripped"
UNSTRIPPED="$STAGE/unstripped/$BINARY_NAME"
DEBUG_STRIPPED="$STAGE/debug-stripped/$BINARY_NAME"
STRIPPED="$STAGE/stripped/$BINARY_NAME"
cp "$BINARY" "$UNSTRIPPED"
cp "$BINARY" "$DEBUG_STRIPPED"
cp "$BINARY" "$STRIPPED"
chmod 0755 "$UNSTRIPPED" "$DEBUG_STRIPPED" "$STRIPPED"

STRIP_INVOCATION="$(basename "$STRIP_TOOL") --strip-all"
if ! "$STRIP_TOOL" --strip-all "$STRIPPED" 2>/dev/null; then
    # Apple's strip rejects GNU/LLVM --strip-all.  Plain Apple strip is the
    # measured equivalent; no weaker flag combination is allowed.  Restore the
    # pristine linker output first because a failed tool may have written.
    if [ "$GOOS" != darwin ]; then
        echo "prepare-release-candidates: $STRIP_TOOL --strip-all failed" >&2
        exit 2
    fi
    cp "$BINARY" "$STRIPPED"
    chmod 0755 "$STRIPPED"
    "$STRIP_TOOL" "$STRIPPED"
    STRIP_INVOCATION="$(basename "$STRIP_TOOL")"
fi

# Third candidate: debug information removed, symbol table kept. Behaviourally
# identical to the other two — same linker output, nothing but metadata differs
# — but a distinct byte image, so VirusTotal scans it as its own file.
#
# That is the entire point. Release-run evidence shows the single tolerated
# Microsoft `!ml` verdict landing on stripped and unstripped candidates of the
# SAME build essentially at random (they disagreed on 4 of 8 targets, in both
# directions), so each variant is an independent draw. With two candidates one
# target came back flagged on both and had no clean binary to ship; a third
# independent draw makes that outcome substantially rarer.
DEBUG_STRIP_INVOCATION="$(basename "$STRIP_TOOL") --strip-debug"
if ! "$STRIP_TOOL" --strip-debug "$DEBUG_STRIPPED" 2>/dev/null; then
    # Apple's strip rejects --strip-debug; -S is its measured equivalent.
    if [ "$GOOS" != darwin ]; then
        echo "prepare-release-candidates: $STRIP_TOOL --strip-debug failed" >&2
        exit 2
    fi
    cp "$BINARY" "$DEBUG_STRIPPED"
    chmod 0755 "$DEBUG_STRIPPED"
    "$STRIP_TOOL" -S "$DEBUG_STRIPPED"
    DEBUG_STRIP_INVOCATION="$(basename "$STRIP_TOOL") -S"
fi

UNSTRIPPED_PRE_SIGN_SHA="$(sha256_file "$UNSTRIPPED")"
DEBUG_STRIPPED_PRE_SIGN_SHA="$(sha256_file "$DEBUG_STRIPPED")"
STRIPPED_PRE_SIGN_SHA="$(sha256_file "$STRIPPED")"
SIGNATURE=not-applicable
if [ "$GOOS" = darwin ]; then
    CODESIGN_TOOL="${CODESIGN:-codesign}"
    command -v "$CODESIGN_TOOL" >/dev/null 2>&1 || {
        echo "prepare-release-candidates: codesign is required for Darwin candidates" >&2
        exit 2
    }
    CODESIGN_TOOL="$(command -v "$CODESIGN_TOOL")"
    for candidate in "$UNSTRIPPED" "$DEBUG_STRIPPED" "$STRIPPED"; do
        "$CODESIGN_TOOL" --sign - --force --timestamp=none "$candidate"
        "$CODESIGN_TOOL" --verify --strict "$candidate"
    done
    SIGNATURE=adhoc-verified
fi

UNSTRIPPED_SHA="$(sha256_file "$UNSTRIPPED")"
DEBUG_STRIPPED_SHA="$(sha256_file "$DEBUG_STRIPPED")"
STRIPPED_SHA="$(sha256_file "$STRIPPED")"
# All three must differ: identical candidates would be one draw wearing three
# hats, and the selector would believe it had alternatives it does not have.
if [ "$UNSTRIPPED_SHA" = "$STRIPPED_SHA" ] || \
   [ "$UNSTRIPPED_SHA" = "$DEBUG_STRIPPED_SHA" ] || \
   [ "$DEBUG_STRIPPED_SHA" = "$STRIPPED_SHA" ]; then
    echo "prepare-release-candidates: strip did not produce byte-distinct candidates" >&2
    exit 2
fi

for candidate in "$UNSTRIPPED" "$DEBUG_STRIPPED" "$STRIPPED"; do
    bash "$ROOT/scripts/ci/check-binary-composition.sh" "$candidate" >/dev/null
done

[ "$(sha256_file "$BINARY")" = "$SOURCE_SHA" ] || {
    echo "prepare-release-candidates: linker output changed during derivation" >&2
    exit 2
}
[ "$(sha256_file "$UNSTRIPPED")" = "$UNSTRIPPED_SHA" ] && \
    [ "$(sha256_file "$DEBUG_STRIPPED")" = "$DEBUG_STRIPPED_SHA" ] && \
    [ "$(sha256_file "$STRIPPED")" = "$STRIPPED_SHA" ] || {
    echo "prepare-release-candidates: a final candidate changed during validation" >&2
    exit 2
}

case "$GOOS" in
linux) FORMAT=elf ;;
darwin) FORMAT=macho ;;
windows) FORMAT=pe ;;
esac
ARCHITECTURE="${GOARCH%-portable}"
case "$TARGET" in
linux-*-portable) LINKAGE=portable ;;
linux-*) LINKAGE=dynamic ;;
*) LINKAGE=native ;;
esac

UNSTRIPPED_SIZE="$(wc -c < "$UNSTRIPPED" | tr -d '[:space:]')"
DEBUG_STRIPPED_SIZE="$(wc -c < "$DEBUG_STRIPPED" | tr -d '[:space:]')"
STRIPPED_SIZE="$(wc -c < "$STRIPPED" | tr -d '[:space:]')"
MANIFEST="$STAGE/candidate-provenance.tsv"
{
    printf '# cbm-release-candidate-provenance-v1\n'
    printf 'target\tvariant\trelative_path\tsource_sha256\tpre_sign_sha256\tsha256\tsize\tformat\tarchitecture\tlinkage\ttransform\tsignature\tstrip_tool\tstrip_version\tpair_verification\n'
    printf '%s\tunstripped\tunstripped/%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tcopy\t%s\t%s\t%s\tsame-linker-output-v1\n' \
        "$TARGET" "$BINARY_NAME" "$SOURCE_SHA" "$UNSTRIPPED_PRE_SIGN_SHA" \
        "$UNSTRIPPED_SHA" "$UNSTRIPPED_SIZE" "$FORMAT" "$ARCHITECTURE" \
        "$LINKAGE" "$SIGNATURE" "$STRIP_INVOCATION" "$STRIP_VERSION"
    printf '%s\tdebug-stripped\tdebug-stripped/%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tstrip-debug\t%s\t%s\t%s\tsame-linker-output-v1\n' \
        "$TARGET" "$BINARY_NAME" "$SOURCE_SHA" "$DEBUG_STRIPPED_PRE_SIGN_SHA" \
        "$DEBUG_STRIPPED_SHA" "$DEBUG_STRIPPED_SIZE" "$FORMAT" "$ARCHITECTURE" \
        "$LINKAGE" "$SIGNATURE" "$DEBUG_STRIP_INVOCATION" "$STRIP_VERSION"
    printf '%s\tstripped\tstripped/%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tstrip\t%s\t%s\t%s\tsame-linker-output-v1\n' \
        "$TARGET" "$BINARY_NAME" "$SOURCE_SHA" "$STRIPPED_PRE_SIGN_SHA" \
        "$STRIPPED_SHA" "$STRIPPED_SIZE" "$FORMAT" "$ARCHITECTURE" \
        "$LINKAGE" "$SIGNATURE" "$STRIP_INVOCATION" "$STRIP_VERSION"
} > "$MANIFEST"

chmod 0555 "$UNSTRIPPED" "$DEBUG_STRIPPED" "$STRIPPED"
chmod 0444 "$MANIFEST"
mv "$STAGE" "$PUBLISHED"
echo "prepare-release-candidates: published $TARGET unstripped/debug-stripped/stripped candidates"
