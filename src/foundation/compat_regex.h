/*
 * compat_regex.h — Portable regular expression API.
 *
 * POSIX: direct wrappers around <regex.h> (regcomp, regexec, regfree).
 * Windows: TODO — vendor TRE regex or use a C++ wrapper around <regex>.
 *
 * Uses our own types so callers never include <regex.h> directly.
 */
#ifndef CBM_COMPAT_REGEX_H
#define CBM_COMPAT_REGEX_H

#include "foundation/constants.h"
#include <stddef.h>

/* ── Flags ────────────────────────────────────────────────────── */

#define CBM_REG_EXTENDED 1
#define CBM_REG_ICASE 2
#define CBM_REG_NOSUB 4
#define CBM_REG_NEWLINE 8

/* ── Error codes ──────────────────────────────────────────────── */

#define CBM_REG_OK 0
#define CBM_REG_NOMATCH (-1)

/* ── Types ────────────────────────────────────────────────────── */

/* Opaque regex handle — sized to hold the platform's regex_t. */
typedef struct {
    /* CBM_SZ_256 bytes should be large enough for any platform's regex_t.
     * POSIX regex_t is typically 48-CBM_SZ_64 bytes; TRE is ~80 bytes. */
    char opaque[CBM_SZ_256];
} cbm_regex_t;

typedef struct {
    int rm_so; /* byte offset of match start, -1 if no match */
    int rm_eo; /* byte offset past match end */
} cbm_regmatch_t;

/* ── Functions ────────────────────────────────────────────────── */

/* Compile a regular expression. Returns CBM_REG_OK on success, non-zero on error. */
int cbm_regcomp(cbm_regex_t *r, const char *pattern, int flags);

/* Execute compiled regex against str. nmatch/matches may be 0/NULL.
 * eflags: 0 or combination of platform-specific exec flags.
 * Returns CBM_REG_OK on match, CBM_REG_NOMATCH on no match. */
int cbm_regexec(const cbm_regex_t *r, const char *str, int nmatch, cbm_regmatch_t *matches,
                int eflags);

/* Free compiled regex. */
void cbm_regfree(cbm_regex_t *r);

#endif /* CBM_COMPAT_REGEX_H */
