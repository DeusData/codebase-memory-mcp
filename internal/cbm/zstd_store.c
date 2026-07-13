// zstd_store.c — Thin C wrappers around Zstandard.

#include "vendored/zstd/zstd.h"

#include "zstd_store.h"

#include <stddef.h>
#include <stdint.h>

int cbm_zstd_compress(const char *src, int srcLen, char *dst, int dstCap, int level) {
    size_t rc = ZSTD_compress(dst, (size_t)dstCap, src, (size_t)srcLen, level);
    if (ZSTD_isError(rc)) {
        return 0;
    }
    return (int)rc;
}

int64_t cbm_zstd_decompress(const char *src, size_t srcLen, char *dst, size_t dstCap) {
    size_t rc = ZSTD_decompress(dst, dstCap, src, srcLen);
    if (ZSTD_isError(rc)) {
        return 0;
    }
    return (int64_t)rc;
}

size_t cbm_zstd_frame_content_size(const char *src, size_t srcLen) {
    unsigned long long n = ZSTD_getFrameContentSize(src, srcLen);
    if (n == ZSTD_CONTENTSIZE_UNKNOWN || n == ZSTD_CONTENTSIZE_ERROR) {
        return 0;
    }
    return (size_t)n;
}

size_t cbm_zstd_compress_bound(int inputSize) {
    return ZSTD_compressBound((size_t)inputSize);
}
