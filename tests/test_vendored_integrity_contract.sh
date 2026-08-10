#!/usr/bin/env bash
# Contract: every vendored input is content-pinned, including opaque blobs and
# the generated Tree-sitter sources that are compiled into release binaries.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/cbm-vendored-integrity.XXXXXX")"
trap 'rm -rf "$FIXTURE"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

make_fixture() {
  local destination="$1"
  mkdir -p "$destination/scripts" "$destination/vendored/yyjson"
  cp "$ROOT/scripts/security-vendored.sh" "$destination/scripts/security-vendored.sh"
  : > "$destination/vendored/yyjson/safe.c"
  printf '%s  %s\n' \
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' \
    'vendored/yyjson/safe.c' > "$destination/scripts/vendored-checksums.txt"
}

opaque="$FIXTURE/opaque"
make_fixture "$opaque"
printf '\230\237\000\000opaque-vector-bytes\n' > "$opaque/vendored/yyjson/code_vectors.bin"
if (cd "$opaque" && bash scripts/security-vendored.sh >/dev/null 2>&1); then
  fail "vendored integrity gate accepted an unmanifested opaque binary blob"
fi

grammar="$FIXTURE/grammar"
make_fixture "$grammar"
mkdir -p "$grammar/internal/cbm/vendored/grammars/example"
printf '%s\n' 'int tree_sitter_example(void) { return 1; }' \
  > "$grammar/internal/cbm/vendored/grammars/example/parser.c"
if (cd "$grammar" && bash scripts/security-vendored.sh >/dev/null 2>&1); then
  fail "vendored integrity gate accepted an unmanifested generated grammar source"
fi

echo "vendored integrity contract: OK"
