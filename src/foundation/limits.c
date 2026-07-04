/*
 * limits.c — Env-configurable safety limits (Stage 2 / Track B4).
 */
#include "foundation/limits.h"

#include <errno.h>
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
