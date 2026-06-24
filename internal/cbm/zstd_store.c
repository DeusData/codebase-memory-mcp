// zstd_store.c — Thin C wrappers around Zstandard.

#include "vendored/zstd/zstd.h"

#include "zstd_store.h"

#include <stddef.h>

int cbm_zstd_compress(const char *src, int srcLen, char *dst, int dstCap, int level) {
    size_t rc = ZSTD_compress(dst, (size_t)dstCap, src, (size_t)srcLen, level);
    if (ZSTD_isError(rc)) {
        return 0;
    }
    return (int)rc;
}

int cbm_zstd_decompress(const char *src, int srcLen, char *dst, int dstCap) {
    size_t rc = ZSTD_decompress(dst, (size_t)dstCap, src, (size_t)srcLen);
    if (ZSTD_isError(rc)) {
        return 0;
    }
    return (int)rc;
}

size_t cbm_zstd_compress_bound(int inputSize) {
    return ZSTD_compressBound((size_t)inputSize);
}
