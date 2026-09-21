#include "foundation/source_encoding.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <errno.h>
#include <iconv.h>
#endif

static int source_valid_utf8(const unsigned char *bytes, size_t length) {
    for (size_t i = 0; i < length;) {
        unsigned char first = bytes[i];
        if (first < 0x80) {
            if (first == 0) return 0;
            i++;
            continue;
        }
        unsigned int count;
        uint32_t cp;
        if (first >= 0xc2 && first <= 0xdf) {
            count = 2;
            cp = first & 0x1f;
        } else if (first >= 0xe0 && first <= 0xef) {
            count = 3;
            cp = first & 0x0f;
        } else if (first >= 0xf0 && first <= 0xf4) {
            count = 4;
            cp = first & 0x07;
        } else {
            return 0;
        }
        if (count > length - i) return 0;
        for (unsigned int j = 1; j < count; j++) {
            unsigned char next = bytes[i + j];
            if ((next & 0xc0) != 0x80) return 0;
            cp = (cp << 6) | (next & 0x3f);
        }
        if ((count == 2 && cp < 0x80) || (count == 3 && cp < 0x800) ||
            (count == 4 && cp < 0x10000) || cp > 0x10ffff ||
            (cp >= 0xd800 && cp <= 0xdfff)) return 0;
        i += count;
    }
    return 1;
}

char *cbm_source_transcode_utf8(const char *input, size_t length, size_t *out_length,
                                cbm_source_encoding_t *encoding) {
    if (!input || !out_length || !encoding) return NULL;
    *out_length = length;
    *encoding = CBM_SOURCE_INVALID;
    size_t bom = length >= 3 && (unsigned char)input[0] == 0xef &&
                         (unsigned char)input[1] == 0xbb &&
                         (unsigned char)input[2] == 0xbf ? 3 : 0;
    const char *requested = getenv("CBM_SOURCE_ENCODING");
    int force_cp949 = requested &&
                      (strcmp(requested, "cp949") == 0 || strcmp(requested, "euc-kr") == 0);
    int force_utf8 = requested && strcmp(requested, "utf8") == 0;
    int valid_utf8 = source_valid_utf8((const unsigned char *)input + bom, length - bom);
    if (bom && !valid_utf8) return NULL;
    if (valid_utf8 && (!force_cp949 || bom)) {
        *encoding = bom ? CBM_SOURCE_UTF8_BOM : CBM_SOURCE_UTF8;
        if (!bom) return NULL;
        char *copy = malloc(length - bom + 17);
        if (!copy) {
            *encoding = CBM_SOURCE_INVALID;
            return NULL;
        }
        memcpy(copy, input + bom, length - bom);
        memset(copy + length - bom, 0, 17);
        *out_length = length - bom;
        return copy;
    }
    if (force_utf8) return NULL;
    if (length == 0 || length > (size_t)INT32_MAX) return NULL;
    /* A NUL in a source buffer is binary data, not a legacy text encoding. */
    if (memchr(input, 0, length)) return NULL;
#ifdef _WIN32
    int wide_length = MultiByteToWideChar(949, MB_ERR_INVALID_CHARS, input, (int)length, NULL, 0);
    if (wide_length <= 0) return NULL;
    wchar_t *wide = malloc((size_t)wide_length * sizeof(*wide));
    if (!wide) return NULL;
    if (MultiByteToWideChar(949, MB_ERR_INVALID_CHARS, input, (int)length, wide,
                            wide_length) != wide_length) {
        free(wide);
        return NULL;
    }
    int needed = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide, wide_length, NULL, 0,
                                     NULL, NULL);
    char *converted = needed > 0 ? malloc((size_t)needed + 17) : NULL;
    if (!converted || WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide, wide_length,
                                          converted, needed, NULL, NULL) != needed) {
        free(converted);
        free(wide);
        return NULL;
    }
    free(wide);
    memset(converted + needed, 0, 17);
    *out_length = (size_t)needed;
#else
    if (length > (SIZE_MAX - 17) / 4) return NULL;
    iconv_t converter = iconv_open("UTF-8", "CP949");
    if (converter == (iconv_t)-1) return NULL;
    size_t capacity = length * 4 + 17;
    char *converted = malloc(capacity);
    if (!converted) {
        iconv_close(converter);
        return NULL;
    }
    char *read_ptr = (char *)input;
    char *write_ptr = converted;
    size_t remaining = length;
    size_t available = capacity - 17;
    size_t result = iconv(converter, &read_ptr, &remaining, &write_ptr, &available);
    iconv_close(converter);
    if (result == (size_t)-1 || remaining != 0) {
        free(converted);
        return NULL;
    }
    *out_length = (size_t)(write_ptr - converted);
    memset(write_ptr, 0, 17);
#endif
    *encoding = CBM_SOURCE_CP949;
    return converted;
}
