#!/usr/bin/env bash
set -euo pipefail

# The sanitizer runner does not interpose malloc/free. Link the production
# allocator instead and call free(NULL) from ELF preinit, before its constructor.
# This reproduces mimalloc #1341 even on a libc that does not call free(NULL)
# during libstdc++ startup. Run after building cbm:
#   bash tests/test_mimalloc_startup.sh [build-directory]
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${1:-$ROOT/build/c}"
if [[ "$(uname -s)" != Linux ]]; then
    echo "SKIP_PLATFORM: mimalloc startup regression requires ELF preinit (Linux)"
    exit 0
fi
[[ -f "$BUILD_DIR/prod_mimalloc.o" ]] || {
    echo "FAIL: build the production allocator first: $BUILD_DIR/prod_mimalloc.o" >&2
    exit 1
}
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/startup.c" <<'EOF'
#include <stdbool.h>
#include <stdlib.h>
#include "mimalloc.h"

static bool early_free_called;

static void early_free(void) {
    void (*volatile release)(void *) = free;  // Prevent optimizing away free(NULL).
    release(NULL);
    early_free_called = true;
}

__attribute__((section(".preinit_array"), used))
static void (*const preinit)(void) = early_free;

int main(void) {
    if (!early_free_called) {
        return 1;
    }
    void *p = mi_malloc(32);
    if (p == NULL) {
        return 1;
    }
    mi_free(p);
    return 0;
}
EOF

read -r -a compiler <<< "${CC:-cc}"
"${compiler[@]}" -std=c11 -O2 -Wall -Wextra -Werror \
    -I"$ROOT/vendored/mimalloc/include" "$WORK/startup.c" \
    "$BUILD_DIR/prod_mimalloc.o" -lm -lpthread -o "$WORK/startup"
"$WORK/startup"
echo "PASS: production free(NULL) before allocator initialization"
