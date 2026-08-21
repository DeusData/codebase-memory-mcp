#ifndef CBM_SHA256_H
#define CBM_SHA256_H

/* In-process SHA-256 (FIPS 180-4). Used to verify the integrity of a
 * downloaded release before installing it, without shelling out to a
 * platform hashing tool (shasum / sha256sum / certutil) — those differ per
 * OS, may be absent, and mis-quote paths under cmd.exe. */

#include <stddef.h>
#include <stdint.h>

#define CBM_SHA256_DIGEST_LEN 32 /* raw digest bytes */
#define CBM_SHA256_HEX_LEN 64    /* lowercase hex chars (no NUL) */

typedef struct {
    uint32_t state[8];
    uint64_t bitlen;
    uint8_t buf[64];
    size_t buflen;
#ifdef __APPLE__
    /* On Apple the streaming API is backed by CommonCrypto's hardware-
     * accelerated SHA-256 (~9x the scalar path on Apple silicon). This
     * over-aligned opaque store holds its CC_SHA256_CTX without leaking
     * <CommonCrypto/CommonDigest.h> into every includer; sha256.c static-asserts
     * it is large enough. The state/buf fields above then go unused, but are
     * retained so the portable scalar path stays compilable for the test that
     * asserts the two implementations are bit-identical. */
    _Alignas(8) unsigned char apple_cc[128];
#endif
} cbm_sha256_ctx;

void cbm_sha256_init(cbm_sha256_ctx *c);
void cbm_sha256_update(cbm_sha256_ctx *c, const void *data, size_t len);
void cbm_sha256_final(cbm_sha256_ctx *c, uint8_t out[CBM_SHA256_DIGEST_LEN]);

/* One-shot hash of a buffer to lowercase hex. `out` must hold
 * CBM_SHA256_HEX_LEN + 1 bytes (hex chars + NUL). */
void cbm_sha256_hex(const void *data, size_t len, char out[CBM_SHA256_HEX_LEN + 1]);

/* RFC 2104 HMAC-SHA-256. The output is always CBM_SHA256_DIGEST_LEN bytes.
 * A NULL key/data pointer is accepted only when its corresponding length is
 * zero. */
void cbm_hmac_sha256(const void *key, size_t key_len, const void *data, size_t data_len,
                     uint8_t out[CBM_SHA256_DIGEST_LEN]);

#ifdef CBM_ENABLE_TEST_SEAMS
/* Always runs the portable scalar SHA-256, on every platform, so a test can
 * assert the platform-optimized cbm_sha256_* path is bit-identical to it. */
void cbm_sha256_scalar_hex_for_testing(const void *data, size_t len,
                                       char out[CBM_SHA256_HEX_LEN + 1]);
#endif

#endif /* CBM_SHA256_H */
