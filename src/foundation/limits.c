/*
 * limits.c — Env-configurable safety limits (Stage 2 / Track B4).
 */
#include "foundation/limits.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>

long cbm_max_file_bytes(void) {
    /* 512 MiB — generous: real source files never approach it, but a
     * pathological / vendored blob degrades to a reported "oversized" skip
     * instead of a silent drop or an unbounded read. */
    const long default_cap = 512L * 1024 * 1024;

    const char *raw = getenv("CBM_MAX_FILE_BYTES");
    if (raw && raw[0]) {
        errno = 0;
        char *end = NULL;
        long v = strtol(raw, &end, 10);
        if (errno == 0 && end != raw && *end == '\0' && v > 0) {
            return v;
        }
        /* Unparseable / non-positive → fall through to the safe default. */
    }
    return default_cap;
}

/* Shared env-int parser: a positive integer in [1, INT_MAX], else the fallback.
 * Read fresh each call (see cbm_max_file_bytes rationale — cheap, test-friendly,
 * no stale memoized copy across runs). */
static int env_positive_int(const char *name, int fallback) {
    const char *raw = getenv(name);
    if (raw && raw[0]) {
        errno = 0;
        char *end = NULL;
        long v = strtol(raw, &end, 10);
        if (errno == 0 && end != raw && *end == '\0' && v > 0 && v <= INT_MAX) {
            return (int)v;
        }
        /* Unparseable / non-positive / out-of-range → safe default. */
    }
    return fallback;
}

int cbm_cypher_max_depth(void) {
    /* 10 — generous for a code call/def graph; an explicit `*1..N` above this is
     * WARN-capped, never an unbounded (cyclic-graph DoS) traversal. */
    return env_positive_int("CBM_CYPHER_MAX_DEPTH", 10);
}

int cbm_mcp_max_depth(void) {
    /* 15 — ceiling for client-driven MCP graph traversals (trace_call_path,
     * detect_changes); the caller's `depth` is WARN-clamped to this. */
    return env_positive_int("CBM_MCP_MAX_DEPTH", 15);
}
