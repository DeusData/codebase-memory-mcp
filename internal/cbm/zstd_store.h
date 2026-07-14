#ifndef CBM_ZSTD_STORE_H
#define CBM_ZSTD_STORE_H

#include <stddef.h>
#include <stdint.h>

// Zstd compression at specified level (1=fast .. 22=best).
// Returns compressed size on success, 0 on error.
int cbm_zstd_compress(const char *src, int srcLen, char *dst, int dstCap, int level);

// Zstd decompression. srcLen/dstCap are size_t so a >2 GiB destination capacity
// is never truncated (a large DB artifact, or a crafted one, would otherwise
// wrap through int and desync the real buffer size from the decoder capacity).
// Returns decompressed size on success (>0), 0 on error.
int64_t cbm_zstd_decompress(const char *src, size_t srcLen, char *dst, size_t dstCap);

// Declared decompressed content size of a zstd frame (from its header), or 0
// when unknown/unreadable. Lets the caller size the destination from the frame
// itself instead of trusting a separate, attacker-controllable size field.
size_t cbm_zstd_frame_content_size(const char *src, size_t srcLen);

// Maximum compressed size bound for given input size.
size_t cbm_zstd_compress_bound(int inputSize);

#endif // CBM_ZSTD_STORE_H
