#ifndef CBM_SOURCE_ENCODING_H
#define CBM_SOURCE_ENCODING_H

#include <stddef.h>

typedef enum {
    CBM_SOURCE_UTF8,
    CBM_SOURCE_UTF8_BOM,
    CBM_SOURCE_CP949,
    CBM_SOURCE_INVALID
} cbm_source_encoding_t;

/* Auto-detect UTF-8 and CP949/EUC-KR, or honor CBM_SOURCE_ENCODING=utf8,
 * cp949, or euc-kr for a repository with a known encoding.
 * Return a malloc-owned UTF-8 replacement when conversion is needed.
 * A NULL result with CBM_SOURCE_UTF8 means the input is already UTF-8;
 * a NULL result with CBM_SOURCE_INVALID means conversion failed. */
char *cbm_source_transcode_utf8(const char *input, size_t length, size_t *out_length,
                                cbm_source_encoding_t *encoding);

#endif
