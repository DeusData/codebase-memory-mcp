/*
 * build_stamp.h — Build identity stamped into the executable after linking.
 *
 * The build fingerprint is the SHA-256 of the executable image with the
 * 32-byte stamp payload treated as zeros. A post-link step
 * (scripts/build-stamp.py, which never executes the image) computes that
 * digest and writes it into the payload, so a process can read its own
 * fingerprint from memory instead of hashing its ~300 MB image on every
 * start (#2683).
 *
 * An unstamped image has an all-zero payload, so its fingerprint equals a
 * plain SHA-256 of the file and the process falls back to hashing itself.
 * The daemon still hashes images it has to verify (its own image at startup
 * and peers running a different file), so a wrong stamp is refused there.
 */
#ifndef CBM_FOUNDATION_BUILD_STAMP_H
#define CBM_FOUNDATION_BUILD_STAMP_H

#include "foundation/sha256.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CBM_BUILD_STAMP_MAGIC_LEN 16U
#define CBM_BUILD_STAMP_PAYLOAD_LEN 32U
#define CBM_BUILD_STAMP_HEX_SIZE (CBM_BUILD_STAMP_PAYLOAD_LEN * 2U + 1U)

/* Streaming SHA-256 over an image in which the CBM_BUILD_STAMP_PAYLOAD_LEN
 * bytes following every occurrence of the stamp magic are read as zeros. The
 * transform is applied to the byte stream, so the digest does not depend on
 * how the input is split into update() calls. update() zeroes payload bytes
 * in the caller's buffer before hashing them. */
typedef struct {
    cbm_sha256_ctx sha;
    uint8_t magic[CBM_BUILD_STAMP_MAGIC_LEN];
    uint8_t tail[CBM_BUILD_STAMP_MAGIC_LEN - 1U];
    size_t tail_len;
    uint64_t offset;     /* stream offset of the next byte fed */
    uint64_t zero_until; /* payload bytes before this offset are zeroed */
    size_t matches;      /* magic occurrences seen so far */
} cbm_build_stamp_hasher_t;

void cbm_build_stamp_hasher_init(cbm_build_stamp_hasher_t *hasher);
void cbm_build_stamp_hasher_update(cbm_build_stamp_hasher_t *hasher, uint8_t *data, size_t len);
void cbm_build_stamp_hasher_final(cbm_build_stamp_hasher_t *hasher,
                                  uint8_t digest[CBM_SHA256_DIGEST_LEN]);

/* Copy the stamp magic out of this executable's own slot. The magic exists
 * as a literal only in the slot initializer, so the image holds it once. */
void cbm_build_stamp_magic(uint8_t out[CBM_BUILD_STAMP_MAGIC_LEN]);

/* The fingerprint stamped into this executable, as lowercase hex. False when
 * the payload is all zeros (an unstamped build). */
bool cbm_build_stamp_self_fingerprint(char out[CBM_BUILD_STAMP_HEX_SIZE]);

#endif /* CBM_FOUNDATION_BUILD_STAMP_H */
