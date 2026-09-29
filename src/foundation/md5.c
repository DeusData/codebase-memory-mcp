/* MD5 (RFC 1321) — see md5.h for why this exists at all. */

#include "foundation/md5.h"

#include <string.h>

/* Per-round left-rotation amounts, in RFC 1321 order. */
static const uint32_t MD5_S[64] = {
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9,  14, 20, 5, 9,
    14, 20, 5, 9,  14, 20, 5, 9,  14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
};

/* floor(2^32 * abs(sin(i + 1))) for i = 0..63. */
static const uint32_t MD5_K[64] = {
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
    0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
    0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
    0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
    0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
    0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
    0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
    0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
    0xeb86d391,
};

static uint32_t md5_rotl(uint32_t v, uint32_t n) {
    return (v << n) | (v >> (32U - n));
}

static uint32_t md5_load_le32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static void md5_store_le32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xffU);
    p[1] = (uint8_t)((v >> 8) & 0xffU);
    p[2] = (uint8_t)((v >> 16) & 0xffU);
    p[3] = (uint8_t)((v >> 24) & 0xffU);
}

/* One 64-byte block: 64 rounds in four 16-round groups. The message word is
 * chosen by a different index formula per group, which is what the switch-free
 * g table below encodes. */
static void md5_transform(uint32_t state[4], const uint8_t block[64]) {
    uint32_t m[16];
    for (int i = 0; i < 16; i++) {
        m[i] = md5_load_le32(block + (size_t)i * 4U);
    }

    uint32_t a = state[0];
    uint32_t b = state[1];
    uint32_t c = state[2];
    uint32_t d = state[3];

    for (uint32_t i = 0; i < 64U; i++) {
        uint32_t f;
        uint32_t g;
        if (i < 16U) {
            f = (b & c) | (~b & d);
            g = i;
        } else if (i < 32U) {
            f = (d & b) | (~d & c);
            g = (5U * i + 1U) & 15U;
        } else if (i < 48U) {
            f = b ^ c ^ d;
            g = (3U * i + 5U) & 15U;
        } else {
            f = c ^ (b | ~d);
            g = (7U * i) & 15U;
        }
        f += a + MD5_K[i] + m[g];
        a = d;
        d = c;
        c = b;
        b += md5_rotl(f, MD5_S[i]);
    }

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
}

void cbm_md5_init(cbm_md5_ctx *c) {
    if (!c) {
        return;
    }
    c->state[0] = 0x67452301U;
    c->state[1] = 0xefcdab89U;
    c->state[2] = 0x98badcfeU;
    c->state[3] = 0x10325476U;
    c->bitlen = 0;
    c->buflen = 0;
    memset(c->buf, 0, sizeof(c->buf));
}

void cbm_md5_update(cbm_md5_ctx *c, const void *data, size_t len) {
    if (!c || (!data && len > 0)) {
        return;
    }
    const uint8_t *p = (const uint8_t *)data;
    c->bitlen += (uint64_t)len * 8U;

    if (c->buflen > 0) {
        size_t need = sizeof(c->buf) - c->buflen;
        size_t take = len < need ? len : need;
        memcpy(c->buf + c->buflen, p, take);
        c->buflen += take;
        p += take;
        len -= take;
        if (c->buflen == sizeof(c->buf)) {
            md5_transform(c->state, c->buf);
            c->buflen = 0;
        }
    }

    while (len >= sizeof(c->buf)) {
        md5_transform(c->state, p);
        p += sizeof(c->buf);
        len -= sizeof(c->buf);
    }

    if (len > 0) {
        memcpy(c->buf, p, len);
        c->buflen = len;
    }
}

void cbm_md5_final(cbm_md5_ctx *c, uint8_t out[CBM_MD5_DIGEST_LEN]) {
    if (!c || !out) {
        return;
    }
    uint64_t bits = c->bitlen;
    uint8_t pad[72];
    memset(pad, 0, sizeof(pad));
    pad[0] = 0x80U;
    /* Pad to 56 mod 64, then append the message length in little-endian bits. */
    size_t padlen = (c->buflen < 56U) ? (56U - c->buflen) : (120U - c->buflen);
    for (int i = 0; i < 8; i++) {
        pad[padlen + (size_t)i] = (uint8_t)((bits >> (8 * i)) & 0xffU);
    }
    cbm_md5_update(c, pad, padlen + 8U);

    for (int i = 0; i < 4; i++) {
        md5_store_le32(out + (size_t)i * 4U, c->state[i]);
    }
}

void cbm_md5_hex(const void *data, size_t len, char out[CBM_MD5_HEX_LEN + 1]) {
    if (!out) {
        return;
    }
    uint8_t digest[CBM_MD5_DIGEST_LEN];
    cbm_md5_ctx ctx;
    cbm_md5_init(&ctx);
    cbm_md5_update(&ctx, data, len);
    cbm_md5_final(&ctx, digest);

    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < CBM_MD5_DIGEST_LEN; i++) {
        out[i * 2] = hex[(digest[i] >> 4) & 0xfU];
        out[i * 2 + 1] = hex[digest[i] & 0xfU];
    }
    out[CBM_MD5_HEX_LEN] = '\0';
}
