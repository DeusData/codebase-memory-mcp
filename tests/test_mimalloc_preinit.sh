#!/usr/bin/env bash
set -euo pipefail

# ELF preinitializers run before allocator constructors, independent of libc version.
if [[ "$(uname -s)" != Linux ]]; then
    echo "mimalloc pre-init regression: ELF-only check not applicable"
    exit 0
fi
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/cbm-mi-preinit.XXXXXX")"
trap 'rm -rf "$probe_dir"' EXIT

cat >"$probe_dir/probe.c" <<'C'
#include <stddef.h>
#include "mimalloc.h"
static void before_init(void) { mi_free(NULL); }
__attribute__((used, section(".preinit_array")))
static void (*const preinit)(void) = before_init;
int main(void) {
    void *p = mi_malloc(16);
    if (!p) return 1;
    mi_free(p);
    return 0;
}
C
"${CC:-cc}" -O2 -DMI_DEBUG=0 -DMI_OVERRIDE=0 \
    -I"$ROOT/vendored/mimalloc/include" -I"$ROOT/vendored/mimalloc/src" \
    "$probe_dir/probe.c" "$ROOT/vendored/mimalloc/src/static.c" \
    -lpthread -lm -o "$probe_dir/probe"
"$probe_dir/probe"
echo "mimalloc pre-init free(NULL) regression passed"
