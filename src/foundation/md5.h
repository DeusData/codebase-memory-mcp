#ifndef CBM_MD5_H
#define CBM_MD5_H

/* In-process MD5 (RFC 1321).
 *
 * MD5 is broken for collision resistance and must never be used for
 * signatures or integrity checks — cbm uses SHA-256 for those. It exists here
 * for one reason: the TrackerV2 HashUID is defined as
 * `md5(<eight identity fields joined by '|'>)`, and re-deriving it means
 * reproducing that digest byte-for-byte. The function is therefore frozen
 * against `hashlib.md5(...)` on the UTF-8 encoding of the input text and is
 * not offered for any other purpose.
 */

#include <stddef.h>
#include <stdint.h>

#define CBM_MD5_DIGEST_LEN 16 /* raw digest bytes */
#define CBM_MD5_HEX_LEN 32    /* lowercase hex chars (no NUL) */

typedef struct {
    uint32_t state[4];
    uint64_t bitlen;
    uint8_t buf[64];
    size_t buflen;
} cbm_md5_ctx;

void cbm_md5_init(cbm_md5_ctx *c);
void cbm_md5_update(cbm_md5_ctx *c, const void *data, size_t len);
void cbm_md5_final(cbm_md5_ctx *c, uint8_t out[CBM_MD5_DIGEST_LEN]);

/* One-shot hash of a buffer to lowercase hex. `out` must hold
 * CBM_MD5_HEX_LEN + 1 bytes (hex chars + NUL). */
void cbm_md5_hex(const void *data, size_t len, char out[CBM_MD5_HEX_LEN + 1]);

#endif /* CBM_MD5_H */
