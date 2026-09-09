/*
 * content_hash.c — Streaming XXH3-128 file hashing.
 */
#include "pipeline/content_hash.h"

#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

#include <inttypes.h>
#include <stdio.h>

enum { CONTENT_HASH_BUFFER_SIZE = 64 * 1024 };

int cbm_content_hash_file(const char *path, char out[CBM_CONTENT_HASH_SIZE]) {
    if (!path || !out) {
        return -1;
    }
    out[0] = '\0';

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        return -1;
    }

    XXH3_state_t *state = XXH3_createState();
    if (!state) {
        fclose(fp);
        return -1;
    }
    if (XXH3_128bits_reset(state) == XXH_ERROR) {
        XXH3_freeState(state);
        fclose(fp);
        return -1;
    }

    unsigned char buffer[CONTENT_HASH_BUFFER_SIZE];
    size_t nread;
    while ((nread = fread(buffer, 1, sizeof(buffer), fp)) > 0) {
        if (XXH3_128bits_update(state, buffer, nread) == XXH_ERROR) {
            XXH3_freeState(state);
            fclose(fp);
            return -1;
        }
    }
    if (ferror(fp)) {
        XXH3_freeState(state);
        fclose(fp);
        return -1;
    }

    XXH128_hash_t hash = XXH3_128bits_digest(state);
    XXH3_freeState(state);
    if (fclose(fp) != 0) {
        return -1;
    }

    int written = snprintf(out, CBM_CONTENT_HASH_SIZE, "xxh3-128:%016" PRIx64 "%016" PRIx64,
                           hash.high64, hash.low64);
    return written == CBM_CONTENT_HASH_SIZE - 1 ? 0 : -1;
}
