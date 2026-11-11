#!/usr/bin/env bash
# Contract: the PRODUCTION allocator object ($BUILD_DIR/prod_mimalloc.o, mimalloc's
# release build with cbm's override defines) keeps three behaviours that no unit
# suite can see, because the test runner links mimalloc's debug build with no
# global override (MIMALLOC_CFLAGS_TEST in Makefile.cbm).
#
#   1. free(NULL) before mimalloc's constructor is a no-op (Linux / ELF only).
#      glibc 2.44's newlocale() frees NULL during libstdc++ start-up, before the
#      allocator's constructor has run. mimalloc 3.3.2's release build read its
#      not-yet-created page map there and every binary died with SIGSEGV at
#      start-up (mimalloc #1341, fixed upstream by b2226c19, first released in
#      v3.4.4). An ELF .preinit_array hook reproduces it on ANY libc, because it
#      runs before every constructor. Reported with this exact reproduction by
#      Felix Rath in PR #2562.
#   2. The version banner carries no build clock. Up to mimalloc 3.4.4 that took
#      a local patch to src/options.c; upstream dropped __DATE__/__TIME__ in
#      v3.4.5 (mimalloc #1355), so it is now upstream behaviour that a refresh
#      must keep. prod_mimalloc.o is compiled with -w and without
#      CFLAGS_COMMON, so -Wdate-time does NOT guard the vendored sources; this
#      fixture does. The same banner also shows the object was built with
#      MI_PROFILE=0 (Makefile.cbm): a profiling build prints a "profiling" line.
#   3. macOS always takes the recommit path on decommit (local patch in
#      vendored/mimalloc/src/prim/unix/prim.c, 63fb7b3d, see
#      vendored/mimalloc/PATCHES.md): needs_recommit is true on Apple and stays
#      upstream's false elsewhere.
#
# Each fixture also proves it linked the object it claims to test (an
# allocator-owned pointer from malloc on Linux, "debug level : 0" in the banner),
# so an accidental link against a different allocator fails instead of passing.
#
# Usage (after `make -f Makefile.cbm cbm`): bash tests/test_prod_allocator_contract.sh [build-dir]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-$ROOT/build/c}"
OBJ="$BUILD_DIR/prod_mimalloc.o"

case "$(uname -s)" in
    Linux) PLATFORM=linux ;;
    Darwin) PLATFORM=macos ;;
    *)
        # WHY skipped: on Windows the production allocator is mimalloc's
        # static-CRT override, linked with --wrap flags this fixture does not
        # reproduce; ELF preinit and the unix decommit path do not exist there.
        # The banner patch is platform-independent source and is covered by the
        # Linux and macOS legs.
        echo "SKIP_PLATFORM: production allocator contract runs on Linux and macOS"
        exit 0
        ;;
esac

if [[ ! -f "$OBJ" ]]; then
    echo "FAIL: production allocator object missing: $OBJ (build cbm first)" >&2
    exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
read -r -a CC_CMD <<< "${CC:-cc}"

build_fixture() {
    local name="$1"
    "${CC_CMD[@]}" -std=gnu11 -O2 -Wall -Wextra -Werror \
        -I"$ROOT/vendored/mimalloc/include" "$WORK/$name.c" "$OBJ" \
        -lm -lpthread -o "$WORK/$name"
}

run_fixture() {
    local name="$1" what="$2" status=0
    "$WORK/$name" || status=$?
    if [[ "$status" -ne 0 ]]; then
        echo "FAIL: $what (exit $status)" >&2
        exit 1
    fi
    echo "PASS: $what"
}

# --- 1. free(NULL) before allocator initialization (ELF preinit) -------------
if [[ "$PLATFORM" == linux ]]; then
    cat > "$WORK/preinit_free.c" <<'EOF'
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "mimalloc.h"

static volatile bool early_free_ran;

static void early_free(void) {
    void (*volatile release)(void *) = free; /* keep the call: free(NULL) is foldable */
    release(NULL);
    early_free_ran = true;
}

__attribute__((section(".preinit_array"), used))
static void (*const cbm_preinit_free)(void) = early_free;

int main(void) {
    if (!early_free_ran) {
        return 2; /* the hook never ran: the fixture proves nothing */
    }
    void *p = malloc(64);
    if (p == NULL) {
        return 3;
    }
    memset(p, 0x5A, 64); /* GCC warns on passing unwritten heap memory as const */
    if (!mi_is_in_heap_region(p)) {
        return 4; /* malloc is not mimalloc: the override was not linked */
    }
    free(p);
    return 0;
}
EOF
    build_fixture preinit_free
    run_fixture preinit_free "free(NULL) before allocator initialization (mimalloc #1341)"
fi

# --- 2. version banner without __DATE__/__TIME__ -----------------------------
cat > "$WORK/banner.c" <<'EOF'
#include <stdio.h>
#include <string.h>
#include "mimalloc.h"

static char out[16384];
static size_t out_len;

static void capture(const char *msg, void *arg) {
    (void)arg;
    size_t n = strlen(msg);
    if (out_len + n < sizeof(out)) {
        memcpy(out + out_len, msg, n);
        out_len += n;
        out[out_len] = '\0';
    }
}

int main(void) {
    mi_options_print_out(capture, NULL);
    int v = mi_version();
    char want[64];
    snprintf(want, sizeof(want), "v%d.%d.%d\n", v / 10000, (v % 10000) / 100, v % 100);
    if (strncmp(out, want, strlen(want)) != 0) {
        fprintf(stderr, "banner first line is not exactly %s", want);
        fprintf(stderr, "got: %.120s\n", out);
        return 1;
    }
    if (strstr(out, "built on") != NULL) {
        fprintf(stderr, "banner carries the build clock\n");
        return 1;
    }
    if (strstr(out, "debug level : 0\n") == NULL) {
        fprintf(stderr, "not the release allocator (MI_DEBUG=0) the product links\n");
        return 1;
    }
    if (strstr(out, "\nprofiling") != NULL) {
        fprintf(stderr, "allocator built with its sampling profiler (production sets MI_PROFILE=0)\n");
        return 1;
    }
    return 0;
}
EOF
build_fixture banner
run_fixture banner "allocator banner is reproducible (no __DATE__/__TIME__) and MI_PROFILE=0"

# --- 3. decommit: macOS recommits, other platforms keep upstream -------------
cat > "$WORK/decommit.c" <<'EOF'
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

/* mimalloc-internal primitive (include/mimalloc/prim.h); hidden visibility still
 * links statically. */
int _mi_prim_decommit(void *addr, size_t size, bool *needs_recommit);

int main(void) {
#if defined(__APPLE__)
    const bool want = true;
#else
    const bool want = false;
#endif
    const size_t size = (size_t)1 << 20;
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (p == MAP_FAILED) {
        return 3;
    }
    memset(p, 0xA5, size);
    bool needs_recommit = !want; /* the call must overwrite it */
    if (_mi_prim_decommit(p, size, &needs_recommit) != 0) {
        return 4;
    }
    if (needs_recommit != want) {
        fprintf(stderr, "needs_recommit=%d, expected %d\n", (int)needs_recommit, (int)want);
        return 1;
    }
    return 0;
}
EOF
build_fixture decommit
if [[ "$PLATFORM" == macos ]]; then
    run_fixture decommit "decommit takes the recommit path on macOS (local patch 63fb7b3d)"
else
    run_fixture decommit "decommit keeps upstream's release behaviour off macOS"
fi
