#!/usr/bin/env bash
# Contract: release candidates are two immutable derivations of one linker
# output. This uses the host's real compiler/strip/signing tools.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/cbm-candidate-derive.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

PREPARE="$ROOT/scripts/ci/prepare-release-candidates.sh"

[[ -x "$PREPARE" ]] || {
    echo "FAIL: missing executable release-candidate producer: $PREPARE" >&2
    exit 1
}
command -v python3 >/dev/null 2>&1 || { echo "FAIL: python3 is required" >&2; exit 1; }
command -v "${CC:-cc}" >/dev/null 2>&1 || {
    echo "SKIP: no native C compiler is available for derivation contract"
    exit 0
}

case "$(uname -s)" in
Darwin) GOOS=darwin ;;
Linux) GOOS=linux ;;
MINGW* | MSYS* | CYGWIN*) GOOS=windows ;;
*) echo "SKIP: unsupported native kernel $(uname -s)"; exit 0 ;;
esac
case "$(uname -m)" in
x86_64 | amd64 | AMD64) GOARCH=amd64 ;;
arm64 | aarch64 | ARM64) GOARCH=arm64 ;;
*) echo "SKIP: unsupported native architecture $(uname -m)"; exit 0 ;;
esac

TARGET="$GOOS-$GOARCH"
BINARY_NAME=codebase-memory-mcp
[[ "$GOOS" == windows ]] && BINARY_NAME+=.exe
INPUT="$FIX/$BINARY_NAME"

python3 - "$FIX/stub.c" <<'PY'
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text(
    r'''#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static const char release_canary[] =
    "codebase-memory-mcp OMIT_LOAD_EXTENSION candidate-derivation-contract";
static _Thread_local unsigned long release_tls_counter;
static int visible_symbol_for_real_strip(int value) { return value + 17; }
int main(int argc, char **argv) {
    const char *sentinel = getenv("CBM_CANDIDATE_EXEC_SENTINEL");
    if (sentinel) {
        FILE *handle = fopen(sentinel, "wb");
        if (handle) fclose(handle);
    }
    release_tls_counter += (unsigned long)argc;
    if (argc == 2 && strcmp(argv[1], "--version") == 0) {
        puts("codebase-memory-mcp 0.0.0-candidate-contract");
        return 0;
    }
    puts(release_canary);
    return visible_symbol_for_real_strip(argc) == -1 || release_tls_counter == 0;
}
''',
    encoding="utf-8",
)
PY
"${CC:-cc}" -g -O0 -o "$INPUT" "$FIX/stub.c"
INPUT_SHA="$(python3 - "$INPUT" <<'PY'
import hashlib, pathlib, sys
print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)"

OUT="$FIX/published"
env CBM_CANDIDATE_EXEC_SENTINEL="$FIX/executed-before-selection" \
    STRIP="${STRIP:-$(command -v strip)}" CODESIGN="${CODESIGN:-$(command -v codesign 2>/dev/null || true)}" \
    "$PREPARE" "$GOOS" "$GOARCH" --binary "$INPUT" --out-dir "$OUT"
[[ ! -e "$FIX/executed-before-selection" ]] || {
    echo "FAIL: candidate producer executed a binary before VirusTotal selection" >&2
    exit 1
}

UNSTRIPPED="$OUT/$TARGET/unstripped/$BINARY_NAME"
STRIPPED="$OUT/$TARGET/stripped/$BINARY_NAME"
PROVENANCE="$OUT/$TARGET/candidate-provenance.tsv"
for path in "$UNSTRIPPED" "$STRIPPED" "$PROVENANCE"; do
    [[ -f "$path" && ! -L "$path" ]] || { echo "FAIL: missing regular output $path" >&2; exit 1; }
done

# The source is never the place stripping/signing occurs.
[[ "$(python3 - "$INPUT" <<'PY'
import hashlib, pathlib, sys
print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
PY
)" == "$INPUT_SHA" ]] || { echo "FAIL: linker output was mutated in place" >&2; exit 1; }

python3 - "$PROVENANCE" "$OUT/$TARGET" "$INPUT_SHA" "$TARGET" "$BINARY_NAME" <<'PY'
from __future__ import annotations

import csv
import hashlib
import pathlib
import stat
import sys

manifest = pathlib.Path(sys.argv[1])
target_root = pathlib.Path(sys.argv[2])
source_hash = sys.argv[3]
target = sys.argv[4]
binary_name = sys.argv[5]
lines = manifest.read_text(encoding="utf-8").splitlines()
if not lines or lines[0] != "# cbm-release-candidate-provenance-v1":
    raise SystemExit("FAIL: candidate provenance marker is missing")
rows = list(csv.DictReader(lines[1:], delimiter="\t"))
expected_fields = (
    "target", "variant", "relative_path", "source_sha256", "pre_sign_sha256",
    "sha256", "size", "format", "architecture", "linkage", "transform",
    "signature", "strip_tool", "strip_version", "pair_verification",
)
actual_fields = tuple(rows[0].keys()) if rows else ()
if actual_fields != expected_fields:
    raise SystemExit("FAIL: candidate provenance header changed")
if [row["variant"] for row in rows] != ["unstripped", "debug-stripped", "stripped"]:
    raise SystemExit("FAIL: provenance rows are not in canonical variant order")
if any(row["target"] != target for row in rows):
    raise SystemExit("FAIL: provenance target is not exact")
if any(row["source_sha256"] != source_hash for row in rows):
    raise SystemExit("FAIL: every candidate must bind the unchanged linker-output hash")
digests = [row["sha256"] for row in rows]
if len(set(digests)) != len(digests):
    # Identical candidates would be one VirusTotal draw wearing three hats, and
    # the selector would believe it has alternatives that do not exist.
    raise SystemExit("FAIL: strip did not produce three byte-distinct candidates")
if [row["transform"] for row in rows] != ["copy", "strip-debug", "strip"]:
    raise SystemExit("FAIL: transforms are not explicit copy/strip-debug/strip")
if any(row["pair_verification"] != "same-linker-output-v1" for row in rows):
    raise SystemExit("FAIL: common linker-output provenance is not recorded")
if target.startswith("darwin-") and any(row["signature"] != "adhoc-verified" for row in rows):
    raise SystemExit("FAIL: both final Darwin candidates must be ad-hoc-signature verified")
for row in rows:
    expected_rel = f'{row["variant"]}/{binary_name}'
    if row["relative_path"] != expected_rel:
        raise SystemExit(f"FAIL: unsafe/noncanonical candidate path {row['relative_path']!r}")
    path = target_root / pathlib.PurePosixPath(row["relative_path"])
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != row["sha256"] or len(data) != int(row["size"]):
        raise SystemExit(f"FAIL: provenance is not content-bound: {path}")
    if stat.S_IMODE(path.stat().st_mode) & 0o222:
        raise SystemExit(f"FAIL: published candidate is writable: {path}")
    if not stat.S_IMODE(path.stat().st_mode) & 0o111:
        raise SystemExit(f"FAIL: published candidate lost executable mode: {path}")
PY

# An explicit unusable strip tool is a hard failure, and atomic publication
# means it leaves no target directory or half-manifest behind.
FAILED_OUT="$FIX/failed-publication"
if STRIP="$FIX/does-not-exist" "$PREPARE" "$GOOS" "$GOARCH" \
    --binary "$INPUT" --out-dir "$FAILED_OUT" >"$FIX/failure.out" 2>&1; then
    echo "FAIL: producer silently fell back after an explicit STRIP failed" >&2
    exit 1
fi
[[ ! -e "$FAILED_OUT/$TARGET" ]] || {
    echo "FAIL: failed derivation left a partial published target" >&2
    exit 1
}

echo "PASS: native stripped/unstripped candidates derive from one linker output and publish atomically"
