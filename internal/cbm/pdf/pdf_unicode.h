/*
 * pdf_unicode.h — the Unicode facts the PDF mention scanner needs, exactly
 * as CPython answers them (pdf_unicode.c is generated from CPython's
 * unicodedata; the field-tested scanner ran on CPython's str and re).
 */
#ifndef CBM_PDF_UNICODE_H
#define CBM_PDF_UNICODE_H

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    uint32_t cp;
    uint16_t off; /* into PDF_NFKC_SEQ */
    uint8_t len;
} pdf_nfkc_t;

typedef struct {
    uint32_t first;
    uint32_t second;
    uint32_t composite;
} pdf_ucomp_t;

typedef struct {
    uint32_t lo;
    uint32_t hi;
} pdf_urange_t;

extern const pdf_nfkc_t PDF_NFKC[]; /* sorted by cp: NFKC of one code point where it differs */
extern const int PDF_NFKC_COUNT;
extern const uint32_t PDF_NFKC_SEQ[];
extern const pdf_ucomp_t PDF_UCOMP[]; /* sorted by (first, second): canonical compositions */
extern const int PDF_UCOMP_COUNT;
extern const pdf_urange_t PDF_U_SPACE[]; /* str.isspace */
extern const int PDF_U_SPACE_COUNT;
extern const pdf_urange_t PDF_U_WORD[]; /* re's \w: str.isalnum or '_' */
extern const int PDF_U_WORD_COUNT;
extern const pdf_urange_t PDF_U_ALPHA[]; /* str.isalpha */
extern const int PDF_U_ALPHA_COUNT;
extern const pdf_urange_t PDF_U_UPPER[]; /* one-character str.isupper */
extern const int PDF_U_UPPER_COUNT;
extern const pdf_urange_t PDF_U_LOWER[]; /* one-character str.islower */
extern const int PDF_U_LOWER_COUNT;
extern const pdf_urange_t PDF_U_TITLE[]; /* titlecase letters (neither upper nor lower) */
extern const int PDF_U_TITLE_COUNT;

static inline bool pdf_u_in(const pdf_urange_t *r, int n, uint32_t cp) {
    int lo = 0;
    int hi = n - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (r[mid].hi < cp) {
            lo = mid + 1;
        } else if (r[mid].lo > cp) {
            hi = mid - 1;
        } else {
            return true;
        }
    }
    return false;
}

static inline bool pdf_u_space(uint32_t c) {
    if (c < 0x80) {
        return c == ' ' || (c >= 0x09 && c <= 0x0D) || (c >= 0x1C && c <= 0x1F);
    }
    return pdf_u_in(PDF_U_SPACE, PDF_U_SPACE_COUNT, c);
}

static inline bool pdf_u_word(uint32_t c) {
    if (c < 0x80) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
               c == '_';
    }
    return pdf_u_in(PDF_U_WORD, PDF_U_WORD_COUNT, c);
}

static inline bool pdf_u_alpha(uint32_t c) {
    if (c < 0x80) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }
    return pdf_u_in(PDF_U_ALPHA, PDF_U_ALPHA_COUNT, c);
}

static inline bool pdf_u_upper(uint32_t c) {
    if (c < 0x80) {
        return c >= 'A' && c <= 'Z';
    }
    return pdf_u_in(PDF_U_UPPER, PDF_U_UPPER_COUNT, c);
}

static inline bool pdf_u_lower(uint32_t c) {
    if (c < 0x80) {
        return c >= 'a' && c <= 'z';
    }
    return pdf_u_in(PDF_U_LOWER, PDF_U_LOWER_COUNT, c);
}

static inline bool pdf_u_title(uint32_t c) {
    return c >= 0x80 && pdf_u_in(PDF_U_TITLE, PDF_U_TITLE_COUNT, c);
}

#endif /* CBM_PDF_UNICODE_H */
