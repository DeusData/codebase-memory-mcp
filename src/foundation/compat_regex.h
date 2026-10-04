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
#include <stdint.h>

/* ── Flags ────────────────────────────────────────────────────── */

#define CBM_REG_EXTENDED 1
#define CBM_REG_ICASE 2
#define CBM_REG_NOSUB 4
#define CBM_REG_NEWLINE 8

/* ── Error codes ──────────────────────────────────────────────── */

#define CBM_REG_OK 0
#define CBM_REG_NOMATCH (-1)
/* cbm_regcomp refused the pattern before compiling it: longer than
 * CBM_REGEX_PATTERN_MAX_BYTES, or its compiled form would exceed
 * CBM_REGEX_COMPILE_BUDGET_UNITS (see cbm_regcomp_estimate_units). Well above
 * every platform compile error code, which are small positive integers. */
#define CBM_REG_ETOOBIG 100
/* Reason text for a CBM_REG_ETOOBIG refusal, for user-facing error messages. */
#define CBM_REG_ETOOBIG_REASON \
    "regex pattern too large to compile: reduce nested {m,n} repetition or alternation"

/* ── Compile-size guard ───────────────────────────────────────── */

enum {
    /* Longest pattern cbm_regcomp hands to the platform compiler. Bounds only
     * the sizing pass: a pattern without repetition costs one unit per atom,
     * so the unit budget below already bounds its compiled size. Equal to the
     * budget, so a realistic pattern is only ever refused for its cost. */
    CBM_REGEX_PATTERN_MAX_BYTES = CBM_SZ_32K,
    /* Largest estimated compiled size, in units of one expanded atom, that
     * cbm_regcomp accepts. Bounds what bounded repetition and alternation can
     * expand a pattern into. compat_regex.c documents the measured bytes per
     * unit and what the budget admits. */
    CBM_REGEX_COMPILE_BUDGET_UNITS = CBM_SZ_32K
};

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

/* Compile a regular expression. Returns CBM_REG_OK on success, non-zero on error:
 * CBM_REG_ETOOBIG when the pattern is refused before compilation (nothing to
 * free), otherwise the platform compiler's error code. */
int cbm_regcomp(cbm_regex_t *r, const char *pattern, int flags);

/* Estimated size of the compiled form of pattern, in units of one expanded
 * atom: an atom costs 1, concatenation and alternation add, a group costs its
 * content, and an interval `{m}`, `{m,}`, `{m,n}` multiplies the element it
 * applies to by the number of copies the compiler makes (the upper bound; m for
 * `{m,}`; at least 1), so nested intervals multiply. `*`, `+` and `?` do not
 * multiply. A K-way alternation adds K*K/8 for its position sets. Arithmetic
 * saturates at UINT64_MAX; a group nesting deeper than the pass tracks returns
 * UINT64_MAX. Honors CBM_REG_EXTENDED (basic syntax otherwise). */
uint64_t cbm_regcomp_estimate_units(const char *pattern, int flags);

/* Execute compiled regex against str. nmatch/matches may be 0/NULL.
 * eflags: 0 or combination of platform-specific exec flags.
 * Returns CBM_REG_OK on match, CBM_REG_NOMATCH on no match. */
int cbm_regexec(const cbm_regex_t *r, const char *str, int nmatch, cbm_regmatch_t *matches,
                int eflags);

/* Free compiled regex. */
void cbm_regfree(cbm_regex_t *r);

#endif /* CBM_COMPAT_REGEX_H */
