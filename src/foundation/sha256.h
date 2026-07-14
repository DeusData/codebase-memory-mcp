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
} cbm_sha256_ctx;

void cbm_sha256_init(cbm_sha256_ctx *c);
void cbm_sha256_update(cbm_sha256_ctx *c, const void *data, size_t len);
void cbm_sha256_final(cbm_sha256_ctx *c, uint8_t out[CBM_SHA256_DIGEST_LEN]);

/* One-shot hash of a buffer to lowercase hex. `out` must hold
 * CBM_SHA256_HEX_LEN + 1 bytes (hex chars + NUL). */
void cbm_sha256_hex(const void *data, size_t len, char out[CBM_SHA256_HEX_LEN + 1]);

#endif /* CBM_SHA256_H */
