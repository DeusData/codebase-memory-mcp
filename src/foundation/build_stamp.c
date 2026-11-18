/*
 * build_stamp.c — see build_stamp.h.
 */
#include "foundation/build_stamp.h"

#include <string.h>

#if defined(__GNUC__) || defined(__clang__)
#define CBM_BUILD_STAMP_USED __attribute__((used))
#else
#define CBM_BUILD_STAMP_USED
#endif

/* The slot: magic followed by the payload. volatile keeps the compiler from
 * folding reads of the (initially zero) payload into constants; the bytes the
 * process reads are the ones the stamp step wrote into the file. */
CBM_BUILD_STAMP_USED static volatile const uint8_t
    cbm_build_stamp_slot[CBM_BUILD_STAMP_MAGIC_LEN + CBM_BUILD_STAMP_PAYLOAD_LEN] = {
        0x89, 'C', 'B', 'M', '-', 'B', 'U', 'I', 'L', 'D', 'S', 'T', 'A', 'M', 'P', 0x01,
        /* payload: 32 zero bytes until stamped */
};

void cbm_build_stamp_magic(uint8_t out[CBM_BUILD_STAMP_MAGIC_LEN]) {
    for (size_t i = 0; i < CBM_BUILD_STAMP_MAGIC_LEN; i++) {
        out[i] = cbm_build_stamp_slot[i];
    }
}

static const char k_hex_digits[] = "0123456789abcdef";

static void stamp_to_hex(const uint8_t bytes[CBM_BUILD_STAMP_PAYLOAD_LEN],
                         char out[CBM_BUILD_STAMP_HEX_SIZE]) {
    for (size_t i = 0; i < CBM_BUILD_STAMP_PAYLOAD_LEN; i++) {
        out[i * 2U] = k_hex_digits[(bytes[i] >> 4U) & 0x0FU];
        out[i * 2U + 1U] = k_hex_digits[bytes[i] & 0x0FU];
    }
    out[CBM_BUILD_STAMP_PAYLOAD_LEN * 2U] = '\0';
}

bool cbm_build_stamp_self_fingerprint(char out[CBM_BUILD_STAMP_HEX_SIZE]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    uint8_t payload[CBM_BUILD_STAMP_PAYLOAD_LEN];
    uint8_t any = 0;
    for (size_t i = 0; i < CBM_BUILD_STAMP_PAYLOAD_LEN; i++) {
        payload[i] = cbm_build_stamp_slot[CBM_BUILD_STAMP_MAGIC_LEN + i];
        any |= payload[i];
    }
    if (any == 0) {
        return false;
    }
    stamp_to_hex(payload, out);
    return true;
}

void cbm_build_stamp_hasher_init(cbm_build_stamp_hasher_t *hasher) {
    memset(hasher, 0, sizeof(*hasher));
    cbm_sha256_init(&hasher->sha);
    cbm_build_stamp_magic(hasher->magic);
}

static void stamp_note_match(cbm_build_stamp_hasher_t *hasher, uint64_t payload_at) {
    hasher->matches++;
    uint64_t end = payload_at + CBM_BUILD_STAMP_PAYLOAD_LEN;
    if (end > hasher->zero_until) {
        hasher->zero_until = end;
    }
}

/* Zero the payload bytes of data[from..len) that fall before zero_until. */
static void stamp_zero_pending(const cbm_build_stamp_hasher_t *hasher, uint8_t *data, size_t len,
                               uint64_t base, size_t from) {
    if (hasher->zero_until <= base + from) {
        return;
    }
    uint64_t stop = hasher->zero_until - base;
    size_t end = stop < (uint64_t)len ? (size_t)stop : len;
    if (end > from) {
        memset(data + from, 0, end - from);
    }
}

void cbm_build_stamp_hasher_update(cbm_build_stamp_hasher_t *hasher, uint8_t *data, size_t len) {
    if (!hasher || !data || len == 0) {
        return;
    }
    const size_t magic_len = CBM_BUILD_STAMP_MAGIC_LEN;
    const uint64_t base = hasher->offset;
    /* Payload carried over from a match in an earlier call. */
    stamp_zero_pending(hasher, data, len, base, 0);

    /* Matches that start in the retained tail and end inside data. Bytes are
     * read from the transformed stream, so zeroed payload never matches. */
    for (size_t start = 0; start < hasher->tail_len; start++) {
        size_t from_tail = hasher->tail_len - start;
        if (magic_len - from_tail > len) {
            continue;
        }
        if (memcmp(hasher->tail + start, hasher->magic, from_tail) == 0 &&
            memcmp(data, hasher->magic + from_tail, magic_len - from_tail) == 0) {
            size_t payload = magic_len - from_tail;
            stamp_note_match(hasher, base + payload);
            stamp_zero_pending(hasher, data, len, base, payload);
        }
    }

    /* Matches that lie entirely inside data, in stream order. */
    size_t pos = 0;
    while (len - pos >= magic_len) {
        const uint8_t *hit = memchr(data + pos, hasher->magic[0], len - pos - magic_len + 1U);
        if (!hit) {
            break;
        }
        size_t at = (size_t)(hit - data);
        if (memcmp(hit, hasher->magic, magic_len) == 0) {
            stamp_note_match(hasher, base + at + magic_len);
            stamp_zero_pending(hasher, data, len, base, at + magic_len);
        }
        pos = at + 1U;
    }

    cbm_sha256_update(&hasher->sha, data, len);

    /* Keep the last magic_len - 1 transformed bytes for the next call. */
    const size_t keep = magic_len - 1U;
    if (len >= keep) {
        memcpy(hasher->tail, data + len - keep, keep);
        hasher->tail_len = keep;
    } else {
        size_t total = hasher->tail_len + len;
        size_t drop = total > keep ? total - keep : 0;
        memmove(hasher->tail, hasher->tail + drop, hasher->tail_len - drop);
        memcpy(hasher->tail + hasher->tail_len - drop, data, len);
        hasher->tail_len = total - drop;
    }
    hasher->offset = base + len;
}

void cbm_build_stamp_hasher_final(cbm_build_stamp_hasher_t *hasher,
                                  uint8_t digest[CBM_SHA256_DIGEST_LEN]) {
    cbm_sha256_final(&hasher->sha, digest);
}
