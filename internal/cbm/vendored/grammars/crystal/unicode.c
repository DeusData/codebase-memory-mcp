#include <stdint.h>
#include <stdlib.h>

/*
 * UTF-8 <-> UTF-32 conversion
 */

// The following code is adapted from https://stackoverflow.com/a/70760522/910109 under
// the terms of CC BY-SA 4.0. The function signature and variable names are changed.

#define UTF8_SURROGATE_LOW_BITS 0x7FF
#define UTF8_MAX_SURROGATE     0xDFFF
#define UTF8_MAX_FOUR_BYTE   0x10FFFF
#define UTF8_ONE_BYTE_BITS          7
#define UTF8_TWO_BYTE_BITS         11
#define UTF8_TWO_BYTE_PREFIX     0xC0
#define UTF8_THREE_BYTE_BITS       16
#define UTF8_THREE_BYTE_PREFIX   0xE0
#define UTF8_FOUR_BYTE_PREFIX    0xF0
#define UTF8_CONTINUATION_BYTE   0x80
#define UTF8_CONTINUATION_MASK   0x3F

/**
 * Ensure that buffer has space for AT LEAST 4 bytes before calling this function,
 *   or a buffer overrun will occur.
 * Returns the number of bytes written to buffer (0-4).
 * If codepoint is a surrogate value, or is out of range for a Unicode codepoint,
 *   writes nothing and returns 0.
 * Surrogate values are integers from 0xD800 to 0xDFFF, inclusive.
 * Valid Unicode codepoint values are non-surrogate integers between
 *   0 and 1_114_111 decimal (0x10_FFFF hex), inclusive.
 */
static size_t codepoint_to_utf8(int32_t codepoint, uint8_t *buffer) {
    if ((codepoint | UTF8_SURROGATE_LOW_BITS) == UTF8_MAX_SURROGATE
        || codepoint > UTF8_MAX_FOUR_BYTE
        || codepoint < 0) {
        return 0;
    }

    int bytes_written = 0;

    if ((codepoint >> UTF8_ONE_BYTE_BITS) == 0) {
        *buffer++ = codepoint;
        bytes_written = 1;
    } else if ((codepoint >> UTF8_TWO_BYTE_BITS) == 0) {
        *buffer++ = UTF8_TWO_BYTE_PREFIX | (codepoint >> 6);
        bytes_written = 2;
    } else if ((codepoint >> UTF8_THREE_BYTE_BITS) == 0) {
        *buffer++ = UTF8_THREE_BYTE_PREFIX | (codepoint >> 12);
        bytes_written = 3;
    } else {
        *buffer++ = UTF8_FOUR_BYTE_PREFIX | (codepoint >> 18);
        bytes_written = 4;
    }
    // Intentionally falling through each case
    switch (bytes_written) {
        case 4:
            *buffer++ = UTF8_CONTINUATION_BYTE | ((codepoint >> 12) & UTF8_CONTINUATION_MASK);
            // fall through
        case 3:
            *buffer++ = UTF8_CONTINUATION_BYTE | ((codepoint >> 6) & UTF8_CONTINUATION_MASK);
            // fall through
        case 2:
            *buffer++ = UTF8_CONTINUATION_BYTE | (codepoint & UTF8_CONTINUATION_MASK);
            // fall through
        default:
            return bytes_written;
    }
}


// The following code is adapted from https://codereview.stackexchange.com/q/197548 and replies
// under the terms of CC BY-SA 4.0. The function signatures and variable names are changed.

#define UTF8_ONE_BYTE_MASK  0b10000000
#define UTF8_ONE_BYTE_COUNT 0

#define UTF8_TWO_BYTE_MASK  0b11100000
#define UTF8_TWO_BYTE_COUNT 0b11000000

#define UTF8_THREE_BYTE_MASK  0b11110000
#define UTF8_THREE_BYTE_COUNT 0b11100000

#define UTF8_FOUR_BYTE_MASK  0b11111000
#define UTF8_FOUR_BYTE_COUNT 0b11110000

static size_t utf8_codepoint_size(uint8_t text) {
    if ((text & UTF8_ONE_BYTE_MASK) == UTF8_ONE_BYTE_COUNT) {
        return 1;
    }

    if ((text & UTF8_TWO_BYTE_MASK) == UTF8_TWO_BYTE_COUNT) {
        return 2;
    }

    if ((text & UTF8_THREE_BYTE_MASK) == UTF8_THREE_BYTE_COUNT) {
        return 3;
    }

    return 4;
}

static size_t utf8_to_codepoints(int32_t *codepoints, uint8_t *text, size_t byte_count) {
    size_t n = 0; // index of codepoints
    size_t i;     // index of text
    uint32_t b0, b1, b2, b3 = 0;

    for (i = 0; i < byte_count;) {
        size_t bytes = utf8_codepoint_size(text[i]);

        switch (bytes) {
            case 1:
                codepoints[n] = (text[i] & ~UTF8_ONE_BYTE_MASK);
                break;

            case 2:
                b0 = (text[i]     & ~UTF8_TWO_BYTE_MASK);
                b1 = (text[i + 1] & UTF8_CONTINUATION_MASK);
                codepoints[n] = (b0 << 6) | b1;
                break;

            case 3:
                b0 = (text[i]     & ~UTF8_THREE_BYTE_MASK);
                b1 = (text[i + 1] & UTF8_CONTINUATION_MASK);
                b2 = (text[i + 2] & UTF8_CONTINUATION_MASK);
                codepoints[n] = (b0 << 12) | (b1 << 6) | b2;
                break;

            case 4:
                b0 = (text[i]     & ~UTF8_FOUR_BYTE_MASK);
                b1 = (text[i + 1] & UTF8_CONTINUATION_MASK);
                b2 = (text[i + 2] & UTF8_CONTINUATION_MASK);
                b3 = (text[i + 3] & UTF8_CONTINUATION_MASK);
                codepoints[n] = (b0 << 18) | (b1 << 12) | (b2 << 6) | b3;
                break;
        }

        i += bytes;
        n++;
    }

    return n;
}
