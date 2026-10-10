/*
 * doc_adr.c — architecture decision records: detection, facts, supersedes.
 *
 * A Markdown file that is an ADR gets one more definition: label "ADR", name
 * its canonical id ("ADR-12"; another prefix as written, "DEC-5"; a dated
 * record without a number keeps its file stem), qualified name
 * "<module>.__adr__", and the record's facts as node properties (adr_id,
 * title, status, date, deciders, superseded_by). Its File DEFINES it like any
 * definition, its sections link to code like any document's (doclink_md.c),
 * and its own "supersedes X" / "replaces X" statements become tokens of the
 * `supersedes` family, whose source is the ADR node: the resolver turns them
 * into SUPERSEDES edges between ADR nodes.
 *
 * "Superseded by Y" is written in the OLD record, about another file: an edge
 * from Y's node would be owned by a file whose text does not hold it, so it
 * stays a fact of this record (status superseded, superseded_by) and the link
 * itself is a MENTIONS edge of the status section.
 *
 * Detection (the field tests' rules, 99.2 % on 379 files): a file in a
 * conventional ADR directory (adr, adrs, decisions, decision-records, ...)
 * whose name carries an id or whose text has ADR structure; any `adr-NNN-*`
 * file; a numbered or dated file with ADR structure. Never README, index or
 * template files without an id, never test fixtures. Structure: a status, and
 * a context, decision, consequences, decision outcome or considered options
 * section. ADR-log configuration files (.adr-dir, log4brains) are not read:
 * per-file detection, a pure function of the path and the text.
 */
#include "doclink.h"

#include "arena.h"
#include "foundation/constants.h"
#include "helpers.h" /* cbm_memmem */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    ADR_ID_DIGITS = 5,      /* an ADR number has at most this many digits */
    ADR_PADDED_DIGITS = 3,  /* a bare number names a record when padded: `0259`, `012` */
    ADR_FIELD_KEY_MAX = 32, /* a field line key: `Status`, `Decision makers` */
    ADR_PREFIX_MIN = 2,     /* `DEC-5`: a prefix of 2 ... */
    ADR_PREFIX_MAX = 12,    /* ... to 12 letters */
    ADR_DOC_MAX = 500,      /* the node's docstring: the decision, collapsed */
    ADR_FIELD_MAX = 256,    /* a fact value is cut at a character boundary */
    ADR_FM_SCAN = 400,      /* front matter ends within this many lines */
    ADR_HEAD_SCAN = 40,     /* header fields within this many lines when there is no heading */
    ADR_NAME_MAX = 64,      /* canonical id buffer */
    ADR_YEAR_MIN = 1900,
    ADR_YEAR_MAX = 2099,
    ADR_MONTHS = 12,
    ADR_DAYS = 31,
    ADR_YEAR_DIGITS = 4,
    ADR_MD_DIGITS = 2,
    ADR_DATE8 = 8,
    ADR_ISO_LEN = 10, /* yyyy-mm-dd */
    ADR_MONTH_ABBR = 3,
    ADR_BOM_LEN = 3,         /* EF BB BF */
    ADR_RELATION_SPAN = 400, /* a relation's targets follow it in this many bytes */
};

/* ── Text helpers ────────────────────────────────────────────────── */

static bool adr_blank(char c) {
    return c == ' ' || c == '\t' || c == '\r';
}

static bool adr_digit(char c) {
    return c >= '0' && c <= '9';
}

static bool adr_alpha(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

static char adr_lower(char c) {
    return (c >= 'A' && c <= 'Z') ? (char)(c - 'A' + 'a') : c;
}

/* s[0..n) starts with `word`, ignoring case. */
static bool adr_starts_ci(const char *s, size_t n, const char *word) {
    size_t w = strlen(word);
    if (n < w) {
        return false;
    }
    for (size_t i = 0; i < w; i++) {
        if (adr_lower(s[i]) != word[i]) {
            return false;
        }
    }
    return true;
}

static bool adr_eq_ci(const char *s, size_t n, const char *word) {
    return n == strlen(word) && adr_starts_ci(s, n, word);
}

/* Trim blanks and Markdown emphasis/code marks around a value. */
static void adr_trim(const char **s, size_t *n) {
    const char *p = *s;
    size_t len = *n;
    while (len > 0 && (adr_blank(*p) || *p == '*' || *p == '_' || *p == '`' || *p == '"')) {
        p++;
        len--;
    }
    while (len > 0 &&
           (adr_blank(p[len - SKIP_ONE]) || p[len - SKIP_ONE] == '*' || p[len - SKIP_ONE] == '_' ||
            p[len - SKIP_ONE] == '`' || p[len - SKIP_ONE] == '"')) {
        len--;
    }
    *s = p;
    *n = len;
}

/* A copy of s[0..n) with Markdown links reduced to their text, `**`, `__`
 * and backticks dropped, blanks collapsed, cut at `cap` bytes on a UTF-8
 * boundary. NULL when nothing is left. */
static char *adr_plain(CBMArena *a, const char *s, size_t n, size_t cap) {
    char *out = (char *)cbm_arena_alloc(a, (n < cap ? n : cap) + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    bool space = false;
    for (size_t i = 0; i < n && w < cap; i++) {
        char c = s[i];
        if (c == '[') {
            continue; /* [text](target): keep the text */
        }
        if (c == ']' && i + SKIP_ONE < n && s[i + SKIP_ONE] == '(') {
            const char *close = memchr(s + i, ')', n - i);
            if (close) {
                i = (size_t)(close - s);
                continue;
            }
        }
        if (c == '*' || c == '`' || (c == '_' && i + SKIP_ONE < n && s[i + SKIP_ONE] == '_')) {
            continue;
        }
        if (adr_blank(c) || c == '\n') {
            space = w > 0;
            continue;
        }
        if (space && w < cap) {
            out[w++] = ' ';
        }
        space = false;
        if (w < cap) {
            out[w++] = c;
        }
    }
    while (w > 0 && ((unsigned char)out[w - SKIP_ONE] & 0xC0) == 0x80) {
        w--; /* never end inside a character */
    }
    if (w > 0 && ((unsigned char)out[w - SKIP_ONE] & 0xC0) == 0xC0) {
        w--;
    }
    out[w] = '\0';
    return w > 0 ? out : NULL;
}

/* ── Lines ───────────────────────────────────────────────────────── */

typedef struct {
    const char *s; /* the line, not NUL-terminated */
    int n;
    bool text; /* not front matter, not fenced code, not an HTML comment */
} adr_line_t;

typedef struct {
    adr_line_t *v;
    int count;
    int fm_end; /* the first line after the front matter (0: none) */
} adr_lines_t;

static bool adr_trim_eq(const char *s, int n, const char *word) {
    const char *p = s;
    size_t len = (size_t)n;
    while (len > 0 && adr_blank(*p)) {
        p++;
        len--;
    }
    while (len > 0 && adr_blank(p[len - SKIP_ONE])) {
        len--;
    }
    return len == strlen(word) && memcmp(p, word, len) == 0;
}

static bool adr_split_lines(CBMArena *a, const char *src, int len, adr_lines_t *out) {
    int count = 0;
    for (int i = 0; i < len; i++) {
        count += src[i] == '\n';
    }
    count++;
    out->v = (adr_line_t *)cbm_arena_alloc(a, (size_t)count * sizeof(*out->v));
    if (!out->v) {
        return false;
    }
    out->count = 0;
    out->fm_end = 0;
    int pos = 0;
    if (len >= ADR_BOM_LEN && memcmp(src, "\xEF\xBB\xBF", ADR_BOM_LEN) == 0) {
        pos = ADR_BOM_LEN; /* a byte-order mark is no part of the first line */
    }
    while (pos <= len && out->count < count) {
        const char *nl = pos < len ? memchr(src + pos, '\n', (size_t)(len - pos)) : NULL;
        int end = nl ? (int)(nl - src) : len;
        int n = end - pos;
        if (n > 0 && src[pos + n - SKIP_ONE] == '\r') {
            n--;
        }
        out->v[out->count++] = (adr_line_t){.s = src + pos, .n = n, .text = true};
        pos = end + SKIP_ONE;
    }
    /* front matter */
    if (out->count > 0 && adr_trim_eq(out->v[0].s, out->v[0].n, "---")) {
        for (int k = 1; k < out->count && k < ADR_FM_SCAN; k++) {
            if (adr_trim_eq(out->v[k].s, out->v[k].n, "---") ||
                adr_trim_eq(out->v[k].s, out->v[k].n, "...")) {
                out->fm_end = k + SKIP_ONE;
                break;
            }
        }
    }
    for (int k = 0; k < out->fm_end; k++) {
        out->v[k].text = false;
    }
    /* fences and HTML comments */
    char fence = 0;
    int fence_len = 0;
    bool comment = false;
    for (int k = out->fm_end; k < out->count; k++) {
        adr_line_t *l = &out->v[k];
        int i = 0;
        while (i < l->n && adr_blank(l->s[i])) {
            i++;
        }
        int run = 0;
        while (i + run < l->n && (l->s[i + run] == '`' || l->s[i + run] == '~') &&
               l->s[i + run] == l->s[i]) {
            run++;
        }
        if (fence) {
            l->text = false;
            if (run >= fence_len && l->s[i] == fence) {
                fence = 0;
            }
            continue;
        }
        if (run >= PAIR_LEN + SKIP_ONE) {
            fence = l->s[i];
            fence_len = run;
            l->text = false;
            continue;
        }
        if (comment) {
            l->text = false;
            if (cbm_memmem(l->s, (size_t)l->n, "-->", PAIR_LEN + SKIP_ONE)) {
                comment = false;
            }
            continue;
        }
        if (l->n - i >= 4 && memcmp(l->s + i, "<!--", 4) == 0 &&
            !cbm_memmem(l->s + i + 4, (size_t)(l->n - i - 4), "-->", PAIR_LEN + SKIP_ONE)) {
            comment = true;
            l->text = false;
        }
    }
    return true;
}

/* ── Front matter (top-level scalars and lists) ──────────────────── */

/* The value of front-matter key `key` (a list joined with ", "), or NULL. */
static char *adr_fm_value(CBMArena *a, const adr_lines_t *L, const char *key) {
    size_t kl = strlen(key);
    for (int k = SKIP_ONE; k < L->fm_end - SKIP_ONE; k++) {
        const adr_line_t *l = &L->v[k];
        if ((size_t)l->n <= kl || l->s[0] == ' ' || l->s[0] == '\t' ||
            !adr_starts_ci(l->s, (size_t)l->n, key) || l->s[kl] != ':') {
            continue;
        }
        const char *v = l->s + kl + SKIP_ONE;
        size_t vn = (size_t)l->n - kl - SKIP_ONE;
        adr_trim(&v, &vn);
        if (vn > 0 && v[0] == '[' && v[vn - SKIP_ONE] == ']') {
            v++;
            vn -= PAIR_LEN;
        }
        if (vn > 0) {
            return adr_plain(a, v, vn, ADR_FIELD_MAX);
        }
        /* a block list on the following lines */
        char buf[ADR_FIELD_MAX + SKIP_ONE];
        size_t w = 0;
        for (int j = k + SKIP_ONE; j < L->fm_end - SKIP_ONE; j++) {
            const adr_line_t *it = &L->v[j];
            int i = 0;
            while (i < it->n && adr_blank(it->s[i])) {
                i++;
            }
            if (i == 0 || i >= it->n || it->s[i] != '-') {
                break;
            }
            const char *iv = it->s + i + SKIP_ONE;
            size_t ivn = (size_t)(it->n - i - SKIP_ONE);
            adr_trim(&iv, &ivn);
            if (ivn > 0 && w + ivn + PAIR_LEN < sizeof(buf)) {
                if (w > 0) {
                    buf[w++] = ',';
                    buf[w++] = ' ';
                }
                memcpy(buf + w, iv, ivn);
                w += ivn;
            }
        }
        return w > 0 ? adr_plain(a, buf, w, ADR_FIELD_MAX) : NULL;
    }
    return NULL;
}

/* ── Headings (the file's Section definitions) ───────────────────── */

typedef struct {
    int line;  /* 0-based index into the lines */
    int level; /* 1..6 */
    const char *title;
    size_t title_n;
} adr_head_t;

static int adr_head_cmp(const void *a, const void *b) {
    const adr_head_t *x = (const adr_head_t *)a;
    const adr_head_t *y = (const adr_head_t *)b;
    return (x->line > y->line) - (x->line < y->line);
}

/* The level of the heading on line k: `#` count, or 1/2 for a setext
 * underline of `=`/`-` on the next line; 0 when the line is no heading. */
static int adr_heading_level(const adr_lines_t *L, int k, const char **title, size_t *title_n) {
    const adr_line_t *l = &L->v[k];
    int i = 0;
    while (i < l->n && i < (PAIR_LEN + SKIP_ONE) && l->s[i] == ' ') {
        i++;
    }
    int hashes = 0;
    while (i + hashes < l->n && l->s[i + hashes] == '#') {
        hashes++;
    }
    if (hashes > 0 && hashes <= (PAIR_LEN * PAIR_LEN + PAIR_LEN) &&
        (i + hashes == l->n || adr_blank(l->s[i + hashes]))) {
        const char *t = l->s + i + hashes;
        size_t tn = (size_t)(l->n - i - hashes);
        while (tn > 0 && (t[tn - SKIP_ONE] == '#' || adr_blank(t[tn - SKIP_ONE]))) {
            tn--;
        }
        adr_trim(&t, &tn);
        *title = t;
        *title_n = tn;
        return hashes;
    }
    if (k + SKIP_ONE < L->count) {
        const adr_line_t *u = &L->v[k + SKIP_ONE];
        int j = 0;
        while (j < u->n && u->s[j] == ' ') {
            j++;
        }
        char c = j < u->n ? u->s[j] : 0;
        int run = 0;
        while (j + run < u->n && u->s[j + run] == c) {
            run++;
        }
        bool rest_blank = true;
        for (int r = j + run; r < u->n; r++) {
            rest_blank = rest_blank && adr_blank(u->s[r]);
        }
        if ((c == '=' || c == '-') && run >= SKIP_ONE && rest_blank) {
            const char *t = l->s;
            size_t tn = (size_t)l->n;
            adr_trim(&t, &tn);
            *title = t;
            *title_n = tn;
            return c == '=' ? SKIP_ONE : PAIR_LEN;
        }
    }
    return 0;
}

/* Section kinds the facts and the detection read. */
typedef enum {
    ADR_SEC_OTHER = 0,
    ADR_SEC_STATUS,
    ADR_SEC_CONTEXT,
    ADR_SEC_DECISION,
    ADR_SEC_OUTCOME,
    ADR_SEC_OPTIONS,
    ADR_SEC_CONSEQUENCES,
} adr_sec_t;

static adr_sec_t adr_section_kind(const char *t, size_t n) {
    /* a leading "1." / "2.3 " numbering is no part of the title */
    size_t i = 0;
    while (i < n && (adr_digit(t[i]) || t[i] == '.')) {
        i++;
    }
    if (i > 0 && i < n && t[i] == ' ') {
        t += i + SKIP_ONE;
        n -= i + SKIP_ONE;
    }
    while (n > 0 && (t[n - SKIP_ONE] == ':' || adr_blank(t[n - SKIP_ONE]))) {
        n--;
    }
    if (adr_eq_ci(t, n, "status") || adr_starts_ci(t, n, "status:")) {
        return ADR_SEC_STATUS;
    }
    if (adr_eq_ci(t, n, "context") || adr_starts_ci(t, n, "context and problem") ||
        adr_eq_ci(t, n, "problem") || adr_eq_ci(t, n, "problem statement") ||
        adr_eq_ci(t, n, "background") || adr_eq_ci(t, n, "motivation")) {
        return ADR_SEC_CONTEXT;
    }
    if (adr_eq_ci(t, n, "decision outcome")) {
        return ADR_SEC_OUTCOME;
    }
    if (adr_eq_ci(t, n, "decision") || adr_eq_ci(t, n, "decisions") ||
        adr_eq_ci(t, n, "the decision")) {
        return ADR_SEC_DECISION;
    }
    if (adr_eq_ci(t, n, "considered options") || adr_eq_ci(t, n, "options considered") ||
        adr_eq_ci(t, n, "alternatives") || adr_eq_ci(t, n, "alternatives considered")) {
        return ADR_SEC_OPTIONS;
    }
    if (adr_eq_ci(t, n, "consequences") || adr_starts_ci(t, n, "positive consequences") ||
        adr_starts_ci(t, n, "negative consequences")) {
        return ADR_SEC_CONSEQUENCES;
    }
    return ADR_SEC_OTHER;
}

/* ── File names ──────────────────────────────────────────────────── */

typedef enum {
    ADR_ID_NONE = 0,
    ADR_ID_ADR,    /* adr-012-title.md */
    ADR_ID_NUMBER, /* 0012-title.md */
    ADR_ID_PREFIX, /* DEC-005-title.md */
    ADR_ID_DATE,   /* 2023-04-05-title.md, 20230405-title.md */
} adr_id_kind_t;

typedef struct {
    adr_id_kind_t kind;
    unsigned number;
    char prefix[ADR_PREFIX_MAX + SKIP_ONE];
    char date[ADR_ISO_LEN + SKIP_ONE];
} adr_id_t;

/* Digits at s, at most ADR_ID_DIGITS, followed by a separator or the end. */
static bool adr_number_at(const char *s, size_t n, size_t *used, unsigned *value) {
    size_t i = 0;
    unsigned v = 0;
    while (i < n && adr_digit(s[i])) {
        if (i >= ADR_ID_DIGITS + ADR_ID_DIGITS) {
            return false;
        }
        v = (v * CBM_DECIMAL_BASE) + (unsigned)(s[i] - '0');
        i++;
    }
    size_t significant = i;
    for (size_t z = 0; z < i && s[z] == '0' && significant > SKIP_ONE; z++) {
        significant--;
    }
    if (i == 0 || significant > ADR_ID_DIGITS) {
        return false;
    }
    if (i < n && s[i] != '-' && s[i] != '_' && s[i] != ' ' && s[i] != '.') {
        return false;
    }
    *used = i;
    *value = v;
    return true;
}

static bool adr_date_prefix(const char *s, size_t n, char *iso) {
    /* yyyy-mm-dd or yyyymmdd, then '-' or '_' */
    if (n > ADR_ISO_LEN && adr_digit(s[0]) && s[4] == '-' && s[7] == '-' &&
        (s[ADR_ISO_LEN] == '-' || s[ADR_ISO_LEN] == '_')) {
        for (int i = 0; i < ADR_ISO_LEN; i++) {
            if (i != 4 && i != 7 && !adr_digit(s[i])) {
                return false;
            }
        }
        memcpy(iso, s, ADR_ISO_LEN);
        iso[ADR_ISO_LEN] = '\0';
        return (s[0] == '1' && s[1] == '9') || (s[0] == '2' && s[1] == '0');
    }
    if (n > ADR_DATE8 && (s[ADR_DATE8] == '-' || s[ADR_DATE8] == '_')) {
        for (int i = 0; i < ADR_DATE8; i++) {
            if (!adr_digit(s[i])) {
                return false;
            }
        }
        snprintf(iso, ADR_ISO_LEN + SKIP_ONE, "%.4s-%.2s-%.2s", s, s + 4, s + 6);
        return (s[0] == '1' && s[1] == '9') || (s[0] == '2' && s[1] == '0');
    }
    return false;
}

static void adr_filename_id(const char *base, adr_id_t *id) {
    memset(id, 0, sizeof(*id));
    size_t n = strlen(base);
    size_t used = 0;
    unsigned v = 0;
    if (adr_starts_ci(base, n, "adr")) {
        size_t i = PAIR_LEN + SKIP_ONE;
        if (i < n && (base[i] == '-' || base[i] == '_' || base[i] == ' ')) {
            i++;
        }
        if (adr_number_at(base + i, n - i, &used, &v)) {
            id->kind = ADR_ID_ADR;
            id->number = v;
            return;
        }
    }
    if (adr_date_prefix(base, n, id->date)) {
        id->kind = ADR_ID_DATE;
        return;
    }
    if (adr_number_at(base, n, &used, &v) && used < n) {
        id->kind = ADR_ID_NUMBER;
        id->number = v;
        return;
    }
    size_t p = 0;
    while (p < n && adr_alpha(base[p])) {
        p++;
    }
    if (p >= ADR_PREFIX_MIN && p <= ADR_PREFIX_MAX && p < n && (base[p] == '-' || base[p] == '_') &&
        adr_number_at(base + p + SKIP_ONE, n - p - SKIP_ONE, &used, &v) &&
        p + SKIP_ONE + used < n) {
        id->kind = ADR_ID_PREFIX;
        id->number = v;
        for (size_t i = 0; i < p; i++) {
            id->prefix[i] =
                (base[i] >= 'a' && base[i] <= 'z') ? (char)(base[i] - 'a' + 'A') : base[i];
        }
        id->prefix[p] = '\0';
    }
}

static const char *const ADR_CONV_DIRS[] = {"adr",
                                            "adrs",
                                            "decisions",
                                            "decision-records",
                                            "decision_records",
                                            "architecture-decisions",
                                            "architecture_decisions",
                                            "architectural-decisions",
                                            "architecture-decision-records",
                                            "decision-log",
                                            NULL};

static const char *const ADR_FIXTURE_DIRS[] = {
    "test",      "tests",     "__tests__",    "integration-tests", "integration_tests",
    "e2e-tests", "e2e_tests", "testdata",     "test-data",         "test_data",
    "fixture",   "fixtures",  "__fixtures__", "__mocks__",         "mock",
    "mocks",     NULL};

static const char *const ADR_NON_ADR_STEMS[] = {"readme",   "index",        "_index",  "toc",
                                                "summary",  "contributing", "process", "changelog",
                                                "glossary", "license",      NULL};

static bool adr_in(const char *s, size_t n, const char *const *list) {
    for (size_t i = 0; list[i]; i++) {
        if (adr_eq_ci(s, n, list[i])) {
            return true;
        }
    }
    return false;
}

/* Does a directory of `rel` match the list? */
static bool adr_dir_in(const char *rel, const char *const *list) {
    const char *s = rel;
    for (const char *slash = strchr(s, '/'); slash; slash = strchr(s, '/')) {
        if (adr_in(s, (size_t)(slash - s), list)) {
            return true;
        }
        s = slash + SKIP_ONE;
    }
    return false;
}

/* README, index, template ... (only for a name without an id). */
static bool adr_non_adr_name(const char *base) {
    size_t n = strlen(base);
    const char *dot = strchr(base, '.');
    size_t stem = dot ? (size_t)(dot - base) : n;
    if (adr_in(base, stem, ADR_NON_ADR_STEMS)) {
        return true;
    }
    for (size_t i = 0; i + strlen("template") <= n; i++) {
        bool start = i == 0 || base[i - SKIP_ONE] == '-' || base[i - SKIP_ONE] == '_' ||
                     base[i - SKIP_ONE] == '.';
        size_t e = i + strlen("template");
        bool end = e == n || base[e] == '-' || base[e] == '_' || base[e] == '.';
        if (start && end && adr_starts_ci(base + i, n - i, "template")) {
            return true;
        }
    }
    return false;
}

/* ── Status, dates, people ───────────────────────────────────────── */

static const struct {
    const char *status;
    const char *words[8];
} ADR_STATUS_WORDS[] = {
    {"superseded", {"supersed", "replaced by", NULL}},
    {"deprecated", {"deprecat", "obsolete", NULL}},
    {"rejected", {"reject", "declined", NULL}},
    {"abandoned", {"abandon", "withdrawn", "cancelled", "canceled", NULL}},
    {"implemented", {"implemented", "done", "completed", "complete", NULL}},
    {"accepted", {"accept", "approved", "adopted", "agreed", "decided", "active", "final", NULL}},
    {"proposed",
     {"propos", "in review", "under review", "pending", "rfc", "under consideration", NULL}},
    {"draft", {"draft", "wip", "work in progress", "in progress", NULL}},
};

/* The normalized status of a status text: the first status word (at a word
 * start, not after "not ", "non-" or "un"), "unknown" when none. */
static const char *adr_norm_status(const char *s) {
    size_t n = strlen(s);
    for (size_t i = 0; i < n; i++) {
        if (i > 0 && (adr_alpha(s[i - SKIP_ONE]) || adr_digit(s[i - SKIP_ONE]))) {
            continue;
        }
        bool negated = (i >= 4 && adr_starts_ci(s + i - 4, 4, "not ")) ||
                       (i >= 4 && adr_starts_ci(s + i - 4, 4, "non-"));
        for (size_t k = 0; k < sizeof(ADR_STATUS_WORDS) / sizeof(ADR_STATUS_WORDS[0]); k++) {
            for (size_t w = 0; ADR_STATUS_WORDS[k].words[w]; w++) {
                /* a whole word, or a stem ("accept" in "accepted") */
                if (adr_starts_ci(s + i, n - i, ADR_STATUS_WORDS[k].words[w]) && !negated &&
                    !(i >= PAIR_LEN && adr_starts_ci(s + i - PAIR_LEN, PAIR_LEN, "un"))) {
                    return ADR_STATUS_WORDS[k].status;
                }
            }
        }
    }
    return "unknown";
}

static const char *const ADR_MONTHS_ABBR[ADR_MONTHS] = {"jan", "feb", "mar", "apr", "may", "jun",
                                                        "jul", "aug", "sep", "oct", "nov", "dec"};

static int adr_month(const char *s, size_t n) {
    if (n < ADR_MONTH_ABBR) {
        return 0;
    }
    for (int m = 0; m < ADR_MONTHS; m++) {
        if (adr_starts_ci(s, n, ADR_MONTHS_ABBR[m])) {
            return m + SKIP_ONE;
        }
    }
    return 0;
}

static int adr_num(const char *s, size_t n, size_t *used) {
    size_t i = 0;
    int v = 0;
    while (i < n && adr_digit(s[i]) && i < ADR_YEAR_DIGITS + SKIP_ONE) {
        v = (v * CBM_DECIMAL_BASE) + (s[i] - '0');
        i++;
    }
    *used = i;
    return v;
}

/* An explicit date in s: yyyy-mm-dd (or / .), yyyymmdd, "d Month yyyy",
 * "Month d, yyyy", or "Month yyyy" (month precision). Ambiguous numeric
 * day/month orders give nothing. */
static bool adr_parse_date(const char *s, char *out, size_t cap) {
    size_t n = strlen(s);
    for (size_t i = 0; i < n; i++) {
        if (i > 0 && (adr_digit(s[i - SKIP_ONE]) || adr_alpha(s[i - SKIP_ONE]))) {
            continue;
        }
        size_t u = 0;
        int y = adr_num(s + i, n - i, &u);
        if (u == ADR_YEAR_DIGITS && y >= ADR_YEAR_MIN && y <= ADR_YEAR_MAX && i + u < n &&
            (s[i + u] == '-' || s[i + u] == '/' || s[i + u] == '.')) {
            size_t u2 = 0;
            int mo = adr_num(s + i + u + SKIP_ONE, n - i - u - SKIP_ONE, &u2);
            size_t at = i + u + SKIP_ONE + u2;
            if (u2 >= SKIP_ONE && u2 <= ADR_MD_DIGITS && at < n && s[at] == s[i + u]) {
                size_t u3 = 0;
                int d = adr_num(s + at + SKIP_ONE, n - at - SKIP_ONE, &u3);
                if (u3 >= SKIP_ONE && u3 <= ADR_MD_DIGITS && mo >= 1 && mo <= ADR_MONTHS &&
                    d >= 1 && d <= ADR_DAYS) {
                    snprintf(out, cap, "%04d-%02d-%02d", y, mo, d);
                    return true;
                }
            }
        }
        /* yyyy Month d: "2019 Jul 31" */
        if (u == ADR_YEAR_DIGITS && y >= ADR_YEAR_MIN && y <= ADR_YEAR_MAX && i + u < n &&
            (s[i + u] == ' ' || s[i + u] == ',')) {
            const char *p = s + i + u;
            while (p < s + n && (*p == ' ' || *p == ',')) {
                p++;
            }
            int mo = adr_month(p, (size_t)(s + n - p));
            if (mo) {
                while (p < s + n && adr_alpha(*p)) {
                    p++;
                }
                while (p < s + n && (*p == '.' || *p == ' ' || *p == ',')) {
                    p++;
                }
                size_t ud = 0;
                int d = adr_num(p, (size_t)(s + n - p), &ud);
                if (ud >= SKIP_ONE && ud <= ADR_MD_DIGITS && d >= 1 && d <= ADR_DAYS) {
                    snprintf(out, cap, "%04d-%02d-%02d", y, mo, d);
                    return true;
                }
            }
        }
        /* a/b/yyyy, a.b.yyyy: only when one of a, b is above 12 */
        if (u >= SKIP_ONE && u <= ADR_MD_DIGITS && i + u < n &&
            (s[i + u] == '/' || s[i + u] == '.')) {
            char sep = s[i + u];
            size_t u2 = 0;
            int b = adr_num(s + i + u + SKIP_ONE, n - i - u - SKIP_ONE, &u2);
            size_t at = i + u + SKIP_ONE + u2;
            size_t u3 = 0;
            int yy = (u2 >= SKIP_ONE && u2 <= ADR_MD_DIGITS && at < n && s[at] == sep)
                         ? adr_num(s + at + SKIP_ONE, n - at - SKIP_ONE, &u3)
                         : 0;
            if (u3 == ADR_YEAR_DIGITS && yy >= ADR_YEAR_MIN && yy <= ADR_YEAR_MAX) {
                int mo = 0;
                int d = 0;
                if (y > ADR_MONTHS && b >= 1 && b <= ADR_MONTHS && y <= ADR_DAYS) {
                    d = y;
                    mo = b;
                } else if (b > ADR_MONTHS && y >= 1 && y <= ADR_MONTHS && b <= ADR_DAYS) {
                    mo = y;
                    d = b;
                }
                if (mo) {
                    snprintf(out, cap, "%04d-%02d-%02d", yy, mo, d);
                    return true;
                }
            }
        }
        if (u == ADR_DATE8) {
            int yy = y / 10000;
            int mo = (y / 100) % 100;
            int d = y % 100;
            if (yy >= ADR_YEAR_MIN && yy <= ADR_YEAR_MAX && mo >= 1 && mo <= ADR_MONTHS && d >= 1 &&
                d <= ADR_DAYS) {
                snprintf(out, cap, "%04d-%02d-%02d", yy, mo, d);
                return true;
            }
        }
        /* d Month yyyy */
        if (u >= SKIP_ONE && u <= ADR_MD_DIGITS && i + u < n && s[i + u] == ' ') {
            const char *m = s + i + u + SKIP_ONE;
            int mo = adr_month(m, n - (size_t)(m - s));
            if (mo) {
                const char *p = m;
                while (p < s + n && adr_alpha(*p)) {
                    p++;
                }
                while (p < s + n && (*p == '.' || *p == ',' || *p == ' ')) {
                    p++;
                }
                size_t uy = 0;
                int yy = adr_num(p, (size_t)(s + n - p), &uy);
                if (uy == ADR_YEAR_DIGITS && yy >= ADR_YEAR_MIN && yy <= ADR_YEAR_MAX && y >= 1 &&
                    y <= ADR_DAYS) {
                    snprintf(out, cap, "%04d-%02d-%02d", yy, mo, y);
                    return true;
                }
            }
        }
        /* Month d, yyyy / Month yyyy */
        int mo = u == 0 ? adr_month(s + i, n - i) : 0;
        if (mo) {
            const char *p = s + i;
            while (p < s + n && adr_alpha(*p)) {
                p++;
            }
            while (p < s + n && (*p == '.' || *p == ' ')) {
                p++;
            }
            size_t ud = 0;
            int d = adr_num(p, (size_t)(s + n - p), &ud);
            if (ud >= SKIP_ONE && ud <= ADR_MD_DIGITS) {
                const char *q = p + ud;
                while (q < s + n && (adr_alpha(*q) || *q == ',' || *q == ' ')) {
                    q++; /* "5th, " */
                }
                size_t uy = 0;
                int yy = adr_num(q, (size_t)(s + n - q), &uy);
                if (uy == ADR_YEAR_DIGITS && yy >= ADR_YEAR_MIN && yy <= ADR_YEAR_MAX && d >= 1 &&
                    d <= ADR_DAYS) {
                    snprintf(out, cap, "%04d-%02d-%02d", yy, mo, d);
                    return true;
                }
            } else if (ud == ADR_YEAR_DIGITS && d >= ADR_YEAR_MIN && d <= ADR_YEAR_MAX) {
                snprintf(out, cap, "%04d-%02d", d, mo);
                return true;
            }
        }
    }
    return false;
}

/* ── Header fields ───────────────────────────────────────────────── */

/* `Status: Accepted`, `* Status: Accepted`, `**Status:** Accepted`,
 * `| Status | Accepted |`: the value of `key` on line l, or NULL. */
static char *adr_field(CBMArena *a, const adr_line_t *l, const char *key) {
    char buf[ADR_FIELD_MAX * PAIR_LEN];
    size_t w = 0;
    for (int i = 0; i < l->n && w + SKIP_ONE < sizeof(buf); i++) {
        if (l->s[i] == '*' || (l->s[i] == '_' && i + SKIP_ONE < l->n && l->s[i + 1] == '_')) {
            continue; /* emphasis around the key */
        }
        buf[w++] = l->s[i];
    }
    buf[w] = '\0';
    const char *p = buf;
    while (*p && adr_blank(*p)) {
        p++;
    }
    bool table = *p == '|';
    if (table) {
        p++;
        while (*p && adr_blank(*p)) {
            p++;
        }
    } else if ((*p == '-' || *p == '+') && adr_blank(p[SKIP_ONE])) {
        p += PAIR_LEN;
        while (*p && adr_blank(*p)) {
            p++;
        }
    }
    size_t kl = strlen(key);
    if (!adr_starts_ci(p, strlen(p), key)) {
        return NULL;
    }
    p += kl;
    while (*p && adr_blank(*p)) {
        p++;
    }
    if (*p != (table ? '|' : ':')) {
        return NULL;
    }
    p++;
    const char *end = table ? strchr(p, '|') : NULL;
    size_t vn = end ? (size_t)(end - p) : strlen(p);
    adr_trim(&p, &vn);
    return vn > 0 ? adr_plain(a, p, vn, ADR_FIELD_MAX) : NULL;
}

/* ── The record ──────────────────────────────────────────────────── */

typedef struct {
    CBMExtractCtx *ctx;
    adr_lines_t L;
    adr_head_t *heads;
    int nheads;
    int title_head;  /* index into heads, or -1 */
    bool numbered;   /* the file name gives the record a number: */
    unsigned number; /* this one */
} adr_doc_t;

/* The paragraph after heading h (its first non-blank text lines). */
static char *adr_first_paragraph(CBMArena *a, const adr_doc_t *d, int h, size_t cap) {
    int start = d->heads[h].line + SKIP_ONE;
    int end = h + SKIP_ONE < d->nheads ? d->heads[h + SKIP_ONE].line : d->L.count;
    int k = start;
    while (k < end &&
           (!d->L.v[k].text || d->L.v[k].n == 0 || adr_trim_eq(d->L.v[k].s, d->L.v[k].n, "") ||
            d->L.v[k].s[0] == '=' || d->L.v[k].s[0] == '-')) {
        k++;
    }
    if (k >= end) {
        return NULL;
    }
    const char *s = d->L.v[k].s;
    int last = k;
    while (last + SKIP_ONE < end && d->L.v[last + SKIP_ONE].text &&
           !adr_trim_eq(d->L.v[last + SKIP_ONE].s, d->L.v[last + SKIP_ONE].n, "")) {
        last++;
    }
    const char *e = d->L.v[last].s + d->L.v[last].n;
    return adr_plain(a, s, (size_t)(e - s), cap);
}

/* The text of section h, as one collapsed string. */
static char *adr_section_text(CBMArena *a, const adr_doc_t *d, int h, size_t cap) {
    int start = d->heads[h].line + SKIP_ONE;
    int end = h + SKIP_ONE < d->nheads ? d->heads[h + SKIP_ONE].line : d->L.count;
    if (start >= end) {
        return NULL;
    }
    const char *s = d->L.v[start].s;
    const char *e = d->L.v[end - SKIP_ONE].s + d->L.v[end - SKIP_ONE].n;
    return e > s ? adr_plain(a, s, (size_t)(e - s), cap) : NULL;
}

static int adr_find_section(const adr_doc_t *d, adr_sec_t kind) {
    for (int h = 0; h < d->nheads; h++) {
        if (adr_section_kind(d->heads[h].title, d->heads[h].title_n) == kind) {
            return h;
        }
    }
    return CBM_NOT_FOUND;
}

/* The value of header field `key` between the title and the first body
 * heading (the first lines when there is no heading). */
static char *adr_header_field(CBMArena *a, const adr_doc_t *d, const char *key) {
    int from = d->title_head >= 0 ? d->heads[d->title_head].line + SKIP_ONE : d->L.fm_end;
    int to = d->L.count;
    for (int h = 0; h < d->nheads; h++) {
        if (h != d->title_head && d->heads[h].line >= from) {
            to = d->heads[h].line;
            break;
        }
    }
    if (to == d->L.count && to - from > ADR_HEAD_SCAN) {
        to = from + ADR_HEAD_SCAN;
    }
    for (int k = from; k < to; k++) {
        if (!d->L.v[k].text) {
            continue;
        }
        char *v = adr_field(a, &d->L.v[k], key);
        if (v) {
            return v;
        }
    }
    return NULL;
}

/* ── Relations ───────────────────────────────────────────────────── */

/* `ADR-12`, `ADR 012`, `adr_7`: the number, and the reference's length. */
static bool adr_id_ref(const char *s, size_t n, unsigned *number, size_t *len) {
    if (n < PAIR_LEN * PAIR_LEN || !adr_starts_ci(s, n, "adr") ||
        (s[3] != '-' && s[3] != '_' && s[3] != ' ')) {
        return false;
    }
    size_t used = 0;
    unsigned v = 0;
    size_t i = PAIR_LEN * PAIR_LEN;
    size_t d = 0;
    while (i + d < n && adr_digit(s[i + d])) {
        d++;
    }
    if (d == 0 || (i + d < n && (adr_alpha(s[i + d]) || s[i + d] == '_'))) {
        return false;
    }
    if (!adr_number_at(s + i, d, &used, &v)) {
        return false;
    }
    *number = v;
    *len = i + d;
    return true;
}

static void adr_push_relation(adr_doc_t *d, const char *adr_qn, const char *raw, size_t n,
                              uint32_t line, uint16_t syntax) {
    CBMArena *a = d->ctx->arena;
    char *text = cbm_arena_strndup(a, raw, n);
    if (!text) {
        d->ctx->result->doc_links.failed = true;
        return;
    }
    CBMDocLink link = {.source_qn = adr_qn,
                       .raw = text,
                       .line = line,
                       .def_line = SKIP_ONE,
                       .syntax = syntax,
                       .flags = 0};
    cbm_doclinks_push(&d->ctx->result->doc_links, a, link);
}

/* The targets after a relation phrase, up to the end of its sentence: link
 * destinations and ADR ids. Forward relations become tokens of family
 * `syntax`; for a backward one ("superseded by") the targets are joined into
 * `*joined`. */
static void adr_targets(adr_doc_t *d, const char *adr_qn, const char *s, size_t n, uint32_t line,
                        bool forward, uint16_t syntax, char *joined, size_t jcap) {
    size_t end = n < ADR_RELATION_SPAN ? n : ADR_RELATION_SPAN;
    for (size_t i = 0; i + SKIP_ONE < end; i++) {
        /* a link is one reference whatever its text holds (adr-tools writes
         * `Supersedes [3. Show links](0003-show-links.md)`) */
        const char *rb = s[i] == '[' ? memchr(s + i, ']', end - i) : NULL;
        const char *close = rb && rb + SKIP_ONE < s + end && rb[SKIP_ONE] == '('
                                ? memchr(rb, ')', (size_t)(s + end - rb))
                                : NULL;
        if (close) {
            i = (size_t)(close - s);
            continue;
        }
        /* a full stop after a number ends the sentence too (`supersedes 0092
         * and 0094. ADR 0202 is ...`: in a held-out audit the next sentence's
         * record was taken for a target) */
        if ((s[i] == '.' && (i + SKIP_ONE == end || adr_blank(s[i + SKIP_ONE]))) || s[i] == ';') {
            end = i;
            break;
        }
    }
    for (size_t i = 0; i < end; i++) {
        const char *target = NULL;
        size_t tn = 0;
        const char *rb = s[i] == '[' ? memchr(s + i, ']', end - i) : NULL;
        if (rb && rb + SKIP_ONE < s + end && rb[SKIP_ONE] == '(') {
            /* [text](destination): one reference, the destination; ids in
             * the link text name the same record */
            const char *dest = rb + PAIR_LEN;
            const char *close = memchr(dest, ')', (size_t)(s + end - dest));
            if (close) {
                target = dest;
                tn = (size_t)(close - dest);
                i = (size_t)(close - s);
            }
        } else {
            unsigned num = 0;
            size_t len = 0;
            if ((i == 0 || !adr_alpha(s[i - SKIP_ONE])) && adr_id_ref(s + i, end - i, &num, &len)) {
                target = s + i;
                tn = len;
                i += len - SKIP_ONE;
            }
        }
        if (!target || tn == 0) {
            continue;
        }
        if (forward) {
            adr_push_relation(d, adr_qn, target, tn, line, syntax);
        } else if (joined) {
            size_t w = strlen(joined);
            bool seen = false;
            for (const char *p = joined; *p && !seen; p++) {
                seen = strncmp(p, target, tn) == 0 && (p[tn] == '\0' || p[tn] == ',') &&
                       (p == joined || p[-SKIP_ONE] == ' ');
            }
            if (!seen && w + tn + PAIR_LEN < jcap) {
                if (w > 0) {
                    joined[w++] = ',';
                    joined[w++] = ' ';
                }
                memcpy(joined + w, target, tn);
                joined[w + tn] = '\0';
            }
        }
    }
}

/* Only list markers, quote marks, table bars and emphasis before position i
 * of the line: the phrase opens the line's statement. */
static bool adr_line_head(const adr_line_t *l, int i) {
    for (int k = 0; k < i; k++) {
        char c = l->s[k];
        if (!adr_blank(c) && c != '-' && c != '*' && c != '+' && c != '>' && c != '_' && c != '|') {
            return false;
        }
    }
    return true;
}

/* A relation word joined to another by a slash, `Supersedes / Depends on:`:
 * a template's combined field label, which states neither relation (in a
 * held-out audit the record depended on the one it named). */
static bool adr_label_choice(const adr_line_t *l, int a, int b) {
    while (b < l->n && (l->s[b] == ' ' || l->s[b] == '\t')) {
        b++;
    }
    while (a > 0 && (l->s[a - SKIP_ONE] == ' ' || l->s[a - SKIP_ONE] == '\t')) {
        a--;
    }
    return (b < l->n && l->s[b] == '/') || (a > 0 && l->s[a - SKIP_ONE] == '/');
}

/* The field's value is "none" or "n/a" (`Supersedes: none (it extends ADR
 * 0031)`): the record states no relation, and the records it goes on to name
 * are extended or refined (a held-out audit's most frequent error). */
static bool adr_none_value(const char *s, size_t n) {
    size_t i = 0;
    while (i < n && (adr_blank(s[i]) || s[i] == ':' || s[i] == '*' || s[i] == '_')) {
        i++;
    }
    static const char *const none[] = {"none", "n/a", NULL};
    for (size_t k = 0; none[k]; k++) {
        size_t w = strlen(none[k]);
        if (adr_starts_ci(s + i, n - i, none[k]) && (i + w == n || !adr_alpha(s[i + w]))) {
            return true;
        }
    }
    return false;
}

/* Another record is the phrase's subject (`ADR 0239 supersedes ADR 0203`, a
 * table cell `0259 (supersedes ADR 0114's ...)`, a link to a record before
 * it): this record reports that record's relation and states none of its
 * own. Its own number as the subject is its own statement. */
static bool adr_reported(const adr_doc_t *d, const adr_line_t *l, int i) {
    int k = i;
    while (k > 0 &&
           (adr_blank(l->s[k - SKIP_ONE]) || l->s[k - SKIP_ONE] == '(' ||
            l->s[k - SKIP_ONE] == '*' || l->s[k - SKIP_ONE] == '_' || l->s[k - SKIP_ONE] == '`')) {
        k--;
    }
    if (k >= PAIR_LEN && l->s[k - SKIP_ONE] == ')') {
        for (int o = k - PAIR_LEN; o > 0; o--) { /* `[text](destination)` */
            if (l->s[o] == '(') {
                return l->s[o - SKIP_ONE] == ']';
            }
            if (l->s[o] == ')' || adr_blank(l->s[o])) {
                break;
            }
        }
        return false;
    }
    int e = k;
    while (k > 0 && adr_digit(l->s[k - SKIP_ONE])) {
        k--;
    }
    if (e == k || (k > 0 && adr_alpha(l->s[k - SKIP_ONE]))) {
        return false;
    }
    size_t used = 0;
    unsigned v = 0;
    if (!adr_number_at(l->s + k, (size_t)(e - k), &used, &v)) {
        return false;
    }
    unsigned ref = 0;
    size_t len = 0;
    bool id = k >= PAIR_LEN * PAIR_LEN &&
              adr_id_ref(l->s + k - PAIR_LEN * PAIR_LEN, (size_t)(e - k) + PAIR_LEN * PAIR_LEN,
                         &ref, &len) &&
              len == (size_t)(e - k) + PAIR_LEN * PAIR_LEN;
    /* a bare number is a record's only when padded like one (`0259`) */
    if (!id && e - k < ADR_PADDED_DIGITS) {
        return false;
    }
    return !(d->numbered && v == d->number);
}

/* A record reference right after the phrase (past a colon, a table bar and
 * emphasis): a link or an ADR id, `Supersedes: [ADR-3](0003-x.md)`,
 * `| Supersedes | ADR-0003 |`. */
/* A colon right after the phrase (past emphasis): `Supersedes: ADR-3`, a
 * field, whatever surrounds it. */
static bool adr_field_value(const char *s, size_t n) {
    size_t i = 0;
    while (i < n && (adr_blank(s[i]) || s[i] == '*' || s[i] == '_')) {
        i++;
    }
    return i < n && s[i] == ':';
}

/* `Status: Accepted`, `- **Date**: 2024-01-02`: a field line. */
static bool adr_field_line(const adr_line_t *l) {
    int i = 0;
    while (i < l->n && (adr_blank(l->s[i]) || l->s[i] == '-' || l->s[i] == '*' || l->s[i] == '+' ||
                        l->s[i] == '_' || l->s[i] == '|')) {
        i++;
    }
    int w = i;
    while (i < l->n && (adr_alpha(l->s[i]) || l->s[i] == ' ' || l->s[i] == '-')) {
        i++;
    }
    if (i == w || i - w > ADR_FIELD_KEY_MAX) {
        return false;
    }
    while (i < l->n && (l->s[i] == '*' || l->s[i] == '_' || l->s[i] == ' ')) {
        i++;
    }
    return i < l->n && (l->s[i] == ':' || l->s[i] == '|');
}

/* Line k continues a paragraph: no list, quote or table mark opens it and the
 * line before is running text, not blank, a heading or a field
 * (`[ADR-0240](0240-x.md)` / `supersedes ADR-0134 and ...`: in a held-out
 * census the subject was the record on the line before). */
static bool adr_continuation(const adr_doc_t *d, int k) {
    const adr_line_t *l = &d->L.v[k];
    int j = 0;
    while (j < l->n && adr_blank(l->s[j])) {
        j++;
    }
    if (j < l->n && (l->s[j] == '-' || l->s[j] == '*' || l->s[j] == '+' || l->s[j] == '>' ||
                     l->s[j] == '|' || adr_digit(l->s[j]))) {
        return false; /* its own list item, quote or table row */
    }
    if (k <= d->L.fm_end || k == 0) {
        return false;
    }
    const adr_line_t *p = &d->L.v[k - SKIP_ONE];
    int a = 0;
    while (a < p->n && adr_blank(p->s[a])) {
        a++;
    }
    return p->text && a < p->n && p->s[a] != '#' && !adr_field_line(p);
}

static bool adr_direct_ref(const char *s, size_t n) {
    size_t i = 0;
    while (i < n && (adr_blank(s[i]) || s[i] == ':' || s[i] == '|' || s[i] == '*' || s[i] == '_')) {
        i++;
    }
    unsigned number = 0;
    size_t len = 0;
    return i < n && (s[i] == '[' || adr_id_ref(s + i, n - i, &number, &len));
}

/* "supersedes" and "superseded by" are ADR words wherever they stand;
 * "replaces" and "replaced by" are ordinary verbs of technical prose ("this
 * middleware replaces the recovery described in ADR-22") and state a relation
 * only where they open a line or list item ("Replaces ADR-3"). */
static void adr_relations(adr_doc_t *d, const char *adr_qn, char *superseded_by, size_t cap) {
    static const struct {
        const char *phrase;
        bool forward;
        bool line_head;
    } phrases[] = {{"superseded by", false, false},
                   {"replaced by", false, true},
                   {"supersedes", true, false},
                   {"replaces", true, true}};
    for (int k = d->L.fm_end; k < d->L.count; k++) {
        const adr_line_t *l = &d->L.v[k];
        if (!l->text) {
            continue;
        }
        for (int i = 0; i < l->n; i++) {
            if (i > 0 && adr_alpha(l->s[i - SKIP_ONE])) {
                continue;
            }
            for (size_t p = 0; p < sizeof(phrases) / sizeof(phrases[0]); p++) {
                size_t pl = strlen(phrases[p].phrase);
                if (adr_starts_ci(l->s + i, (size_t)(l->n - i), phrases[p].phrase) &&
                    (i + (int)pl == l->n || !adr_alpha(l->s[i + (int)pl])) &&
                    (!phrases[p].line_head || adr_line_head(l, i)) &&
                    !adr_label_choice(l, i, i + (int)pl)) {
                    if (!adr_none_value(l->s + i + pl, (size_t)(l->n - i - (int)pl)) &&
                        !adr_reported(d, l, i)) {
                        /* a statement opens its line or field and names a
                         * record directly; the same words in running prose
                         * are their own family (held-out audits: prose 38 of
                         * 42 correct, statements 44 of 45) */
                        const char *v = l->s + i + pl;
                        size_t vn = (size_t)(l->n - i - (int)pl);
                        bool statement = adr_line_head(l, i) && adr_direct_ref(v, vn) &&
                                         (adr_field_value(v, vn) || !adr_continuation(d, k));
                        uint16_t syntax = statement ? (uint16_t)CBM_DOCLINK_MD_SUPERSEDES
                                                    : (uint16_t)CBM_DOCLINK_MD_SUPERSEDES_PROSE;
                        adr_targets(d, adr_qn, v, vn, (uint32_t)(k + SKIP_ONE), phrases[p].forward,
                                    syntax, superseded_by, cap);
                    }
                    i += (int)pl - SKIP_ONE;
                    break;
                }
            }
        }
    }
}

/* A template's choice list, `{proposed | accepted}` or `[a | b]`: the field
 * was never filled in. A filled-in value that kept its braces (`{proposed}`)
 * is a value. */
static bool adr_placeholder(const char *s) {
    for (const char *p = s; *p; p++) {
        if (*p != '{' && *p != '[') {
            continue;
        }
        char close = *p == '{' ? '}' : ']';
        for (const char *q = p + SKIP_ONE; *q && *q != close; q++) {
            if (*q == '|') {
                return true;
            }
        }
    }
    return false;
}

/* The first date in the record's change-log section ("Changelog", "Change
 * log", "History", "Revision history"): `* 2019-12-12: Initial version`. */
static bool adr_changelog_date(const adr_doc_t *d, char *out, size_t cap) {
    for (int h = 0; h < d->nheads; h++) {
        const char *t = d->heads[h].title;
        size_t n = d->heads[h].title_n;
        while (n > 0 && (t[n - SKIP_ONE] == ':' || adr_blank(t[n - SKIP_ONE]))) {
            n--;
        }
        if (!adr_eq_ci(t, n, "changelog") && !adr_eq_ci(t, n, "change log") &&
            !adr_eq_ci(t, n, "history") && !adr_eq_ci(t, n, "revision history")) {
            continue;
        }
        int end = h + SKIP_ONE < d->nheads ? d->heads[h + SKIP_ONE].line : d->L.count;
        for (int k = d->heads[h].line + SKIP_ONE; k < end; k++) {
            char line[ADR_FIELD_MAX];
            const adr_line_t *l = &d->L.v[k];
            if (!l->text || l->n == 0) {
                continue;
            }
            snprintf(line, sizeof(line), "%.*s", l->n, l->s);
            if (adr_parse_date(line, out, cap)) {
                return true;
            }
        }
        return false;
    }
    return false;
}

/* ── The driver ──────────────────────────────────────────────────── */

static bool adr_ends_ci(const char *s, const char *suffix) {
    size_t n = strlen(s);
    size_t k = strlen(suffix);
    return n > k && adr_eq_ci(s + n - k, k, suffix);
}

/* A Markdown or reStructuredText record (reST titles underlined with `=` or
 * `-` read like setext headings). */
static bool adr_document_path(const char *rel) {
    return adr_ends_ci(rel, ".md") || adr_ends_ci(rel, ".mdx") || adr_ends_ci(rel, ".markdown") ||
           adr_ends_ci(rel, ".rst");
}

void cbm_adr_extract(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || !ctx->source || ctx->source_len <= 0 || !ctx->rel_path ||
        !adr_document_path(ctx->rel_path)) {
        return;
    }
    const char *rel = ctx->rel_path;
    const char *base = strrchr(rel, '/');
    base = base ? base + SKIP_ONE : rel;
    adr_id_t id;
    adr_filename_id(base, &id);
    bool conv = adr_dir_in(rel, ADR_CONV_DIRS);
    if ((id.kind == ADR_ID_NONE && adr_non_adr_name(base)) || adr_dir_in(rel, ADR_FIXTURE_DIRS)) {
        return;
    }
    if (!conv && id.kind == ADR_ID_NONE) {
        return; /* structure alone never makes a record */
    }
    CBMArena *scratch = ctx->scratch ? ctx->scratch : ctx->arena;
    CBMArena *a = ctx->arena;
    adr_doc_t d = {.ctx = ctx,
                   .title_head = CBM_NOT_FOUND,
                   .numbered = id.kind == ADR_ID_ADR || id.kind == ADR_ID_NUMBER ||
                               id.kind == ADR_ID_PREFIX,
                   .number = id.number};
    if (!adr_split_lines(scratch, ctx->source, ctx->source_len, &d.L)) {
        ctx->result->doc_links.failed = true;
        return;
    }
    /* headings: the Section definitions, read back at their lines */
    int nsec = 0;
    for (int i = 0; i < ctx->result->defs.count; i++) {
        const CBMDefinition *def = &ctx->result->defs.items[i];
        nsec += def->label && strcmp(def->label, "Section") == 0;
    }
    d.heads =
        nsec > 0 ? (adr_head_t *)cbm_arena_alloc(scratch, (size_t)nsec * sizeof(*d.heads)) : NULL;
    if (nsec > 0 && !d.heads) {
        ctx->result->doc_links.failed = true;
        return;
    }
    for (int i = 0; i < ctx->result->defs.count; i++) {
        const CBMDefinition *def = &ctx->result->defs.items[i];
        if (!def->label || strcmp(def->label, "Section") != 0 || def->start_line < SKIP_ONE ||
            (int)def->start_line > d.L.count) {
            continue;
        }
        int k = (int)def->start_line - SKIP_ONE;
        const char *t = NULL;
        size_t tn = 0;
        int level = adr_heading_level(&d.L, k, &t, &tn);
        if (level > 0) {
            d.heads[d.nheads++] =
                (adr_head_t){.line = k, .level = level, .title = t, .title_n = tn};
        }
    }
    qsort(d.heads, (size_t)d.nheads, sizeof(*d.heads), adr_head_cmp);
    for (int h = 0; h < d.nheads; h++) {
        if (d.heads[h].level == SKIP_ONE) {
            d.title_head = h;
            break;
        }
    }
    /* status: front matter, header field, the Status section */
    char *status_raw = adr_fm_value(a, &d.L, "status");
    if (!status_raw) {
        status_raw = adr_header_field(a, &d, "status");
    }
    int status_sec = adr_find_section(&d, ADR_SEC_STATUS);
    if (!status_raw && status_sec >= 0) {
        const adr_head_t *sh = &d.heads[status_sec];
        const char *colon = memchr(sh->title, ':', sh->title_n);
        if (colon && colon + SKIP_ONE < sh->title + sh->title_n) {
            status_raw =
                adr_plain(a, colon + SKIP_ONE, (size_t)(sh->title + sh->title_n - colon - SKIP_ONE),
                          ADR_FIELD_MAX);
        } else {
            status_raw = adr_first_paragraph(a, &d, status_sec, ADR_FIELD_MAX);
        }
    }
    bool placeholder = status_raw && adr_placeholder(status_raw);
    bool structure = status_raw && (adr_find_section(&d, ADR_SEC_CONTEXT) >= 0 ||
                                    adr_find_section(&d, ADR_SEC_DECISION) >= 0 ||
                                    adr_find_section(&d, ADR_SEC_CONSEQUENCES) >= 0 ||
                                    adr_find_section(&d, ADR_SEC_OUTCOME) >= 0 ||
                                    adr_find_section(&d, ADR_SEC_OPTIONS) >= 0);
    bool is_adr = (conv && (id.kind != ADR_ID_NONE || structure)) || id.kind == ADR_ID_ADR ||
                  ((id.kind == ADR_ID_NUMBER || id.kind == ADR_ID_DATE) && structure);
    if (!is_adr) {
        return;
    }
    /* the record's facts */
    char name[ADR_NAME_MAX];
    if (id.kind == ADR_ID_ADR || id.kind == ADR_ID_NUMBER) {
        snprintf(name, sizeof(name), "ADR-%u", id.number);
    } else if (id.kind == ADR_ID_PREFIX) {
        snprintf(name, sizeof(name), "%s-%u", id.prefix, id.number);
    } else {
        const char *dot = strrchr(base, '.');
        size_t stem = dot ? (size_t)(dot - base) : strlen(base);
        snprintf(name, sizeof(name), "%.*s",
                 (int)(stem < sizeof(name) - SKIP_ONE ? stem : sizeof(name) - SKIP_ONE), base);
    }
    char *title = NULL;
    if (d.title_head >= 0) {
        title =
            adr_plain(a, d.heads[d.title_head].title, d.heads[d.title_head].title_n, ADR_FIELD_MAX);
    }
    if (!title) {
        title = adr_fm_value(a, &d.L, "title");
    }
    char date[ADR_ISO_LEN + SKIP_ONE] = "";
    /* date: front matter, header field, "last updated", the change log's
     * first date, the file name */
    char *date_raw = adr_fm_value(a, &d.L, "date");
    if (!date_raw) {
        date_raw = adr_header_field(a, &d, "date");
    }
    if (!date_raw) {
        date_raw = adr_header_field(a, &d, "last updated");
    }
    if (!(date_raw && adr_parse_date(date_raw, date, sizeof(date))) &&
        !(!date_raw && adr_changelog_date(&d, date, sizeof(date))) && id.date[0]) {
        memcpy(date, id.date, sizeof(id.date));
    }
    static const char *const people_keys[] = {"deciders", "decision-makers", "decision_makers",
                                              "authors",  "author",          NULL};
    char *deciders = NULL;
    for (size_t k = 0; !deciders && people_keys[k]; k++) {
        deciders = adr_fm_value(a, &d.L, people_keys[k]);
    }
    for (size_t k = 0; !deciders && people_keys[k]; k++) {
        deciders = adr_header_field(a, &d, people_keys[k]);
    }
    if (deciders && adr_placeholder(deciders)) {
        deciders = NULL; /* a template placeholder */
    }
    int decision = adr_find_section(&d, ADR_SEC_DECISION);
    if (decision < 0) {
        decision = adr_find_section(&d, ADR_SEC_OUTCOME);
    }
    if (decision < 0) {
        decision = adr_find_section(&d, ADR_SEC_CONTEXT);
    }
    char *doc = decision >= 0 ? adr_section_text(a, &d, decision, ADR_DOC_MAX) : NULL;
    char *qn = cbm_fqn_compute(a, ctx->project, rel, CBM_ADR_QN_NAME);
    char *adr_name = cbm_arena_strdup(a, name);
    char *adr_date = date[0] ? cbm_arena_strdup(a, date) : NULL;
    char superseded_by[ADR_FIELD_MAX] = "";
    char *fm_by = adr_fm_value(a, &d.L, "superseded-by");
    if (!fm_by) {
        fm_by = adr_fm_value(a, &d.L, "superseded_by");
    }
    if (fm_by) {
        snprintf(superseded_by, sizeof(superseded_by), "%s", fm_by);
    }
    if (!qn || !adr_name) {
        ctx->result->doc_links.failed = true;
        return;
    }
    adr_relations(&d, qn, superseded_by, sizeof(superseded_by));
    const char *status = placeholder || !status_raw ? NULL : adr_norm_status(status_raw);
    const char *facts[][PAIR_LEN] = {
        {"adr_id", adr_name},
        {"title", title},
        {"status", status},
        {"status_text", placeholder ? NULL : status_raw},
        {"date", adr_date},
        {"deciders", deciders},
        {"superseded_by", superseded_by[0] ? cbm_arena_strdup(a, superseded_by) : NULL},
    };
    /* sized by the facts themselves: room for every pair and the terminator */
    size_t nfacts = sizeof(facts) / sizeof(facts[0]);
    const char **kv =
        (const char **)cbm_arena_alloc(a, ((nfacts * PAIR_LEN) + SKIP_ONE) * sizeof(char *));
    if (!kv) {
        ctx->result->doc_links.failed = true;
        return;
    }
    size_t w = 0;
    for (size_t f = 0; f < nfacts; f++) {
        if (facts[f][SKIP_ONE] && facts[f][SKIP_ONE][0]) {
            kv[w++] = facts[f][0];
            kv[w++] = facts[f][SKIP_ONE];
        }
    }
    kv[w] = NULL;
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = adr_name;
    def.qualified_name = qn;
    def.label = "ADR";
    def.file_path = rel;
    def.start_line = SKIP_ONE;
    def.end_line = (uint32_t)d.L.count;
    def.is_exported = true;
    def.docstring = doc;
    def.extra_props = kv;
    cbm_defs_push(&ctx->result->defs, a, def);
}
