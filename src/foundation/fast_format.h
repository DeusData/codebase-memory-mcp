#ifndef CBM_FAST_FORMAT_H
#define CBM_FAST_FORMAT_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Composing a key once per edge or occurrence. snprintf reads the locale on every call,
 * and on macOS that takes a process-wide lock, so with all workers running it spent more
 * time waiting than formatting. These write the same digits as %llu, without a NUL, and
 * return the new end; the caller has already sized the buffer. */
#define CBM_PUT_U64_MAX 20

static inline char *cbm_put_u64(char *out, uint64_t value) {
    char digits[CBM_PUT_U64_MAX];
    size_t at = sizeof(digits);
    do {
        digits[--at] = (char)('0' + value % 10);
        value /= 10;
    } while (value);
    memcpy(out, digits + at, sizeof(digits) - at);
    return out + (sizeof(digits) - at);
}

static inline char *cbm_put_text(char *out, const char *text, size_t length) {
    memcpy(out, text, length);
    return out + length;
}

#endif
