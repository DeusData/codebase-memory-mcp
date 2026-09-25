/* compact_out.c — tree-format emission helpers. See compact_out.h for the contract. */
#include "mcp/compact_out.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <yyjson/yyjson.h>

enum { SB_INITIAL_CAP = 1024 };

void cbm_sb_init(cbm_sb_t *sb) {
    sb->buf = NULL;
    sb->len = 0;
    sb->cap = 0;
    sb->oom = false;
}

static bool sb_reserve(cbm_sb_t *sb, size_t extra) {
    if (sb->oom) {
        return false;
    }
    if (sb->len + extra + 1 <= sb->cap) {
        return true;
    }
    size_t ncap = sb->cap ? sb->cap : SB_INITIAL_CAP;
    while (ncap < sb->len + extra + 1) {
        ncap *= 2;
    }
    char *nbuf = (char *)realloc(sb->buf, ncap);
    if (!nbuf) {
        sb->oom = true;
        return false;
    }
    sb->buf = nbuf;
    sb->cap = ncap;
    return true;
}

void cbm_sb_append_n(cbm_sb_t *sb, const char *s, size_t n) {
    if (!s || n == 0 || !sb_reserve(sb, n)) {
        return;
    }
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

void cbm_sb_append(cbm_sb_t *sb, const char *s) {
    if (s) {
        cbm_sb_append_n(sb, s, strlen(s));
    }
}

char *cbm_sb_finish(cbm_sb_t *sb) {
    if (sb->oom) {
        free(sb->buf);
        cbm_sb_init(sb);
        return NULL;
    }
    if (!sb->buf) {
        /* Empty but valid: return an owned empty string. */
        char *empty = (char *)calloc(1, 1);
        return empty;
    }
    char *out = sb->buf;
    cbm_sb_init(sb);
    return out;
}

void cbm_sb_free(cbm_sb_t *sb) {
    free(sb->buf);
    cbm_sb_init(sb);
}

/* ── Tree quoting ────────────────────────────────────────────────── */

/* True when `s` parses as an integer or real literal (sign, digits, one dot). */
static bool looks_numeric_n(const char *s, size_t len) {
    if (!s || len == 0) {
        return false;
    }
    size_t offset = 0;
    if (s[offset] == '-' || s[offset] == '+') {
        offset++;
    }
    bool digit = false;
    bool dot = false;
    for (; offset < len; offset++) {
        if (isdigit((unsigned char)s[offset])) {
            digit = true;
        } else if (s[offset] == '.' && !dot) {
            dot = true;
        } else {
            return false;
        }
    }
    return digit;
}

/* UTF-8 sequence length starting at p, validating continuation bytes and the
 * lead-byte ranges (RFC 3629: no overlongs, no surrogates, max U+10FFFF).
 * `remaining` makes truncated multibyte input safe to inspect. */
static size_t utf8_sequence_length(const unsigned char *p, size_t remaining) {
    if (!p || remaining == 0) {
        return 0;
    }
    unsigned char c = p[0];
    if (c < 0x80) {
        return 1;
    }
    if (c < 0xC2) {
        return 0; /* bare continuation byte or overlong lead */
    }
    if (c < 0xE0) {
        return remaining >= 2 && (p[1] & 0xC0) == 0x80 ? 2 : 0;
    }
    if (c < 0xF0) {
        if (remaining < 3) {
            return 0;
        }
        unsigned char lo = c == 0xE0 ? 0xA0 : 0x80;
        unsigned char hi = c == 0xED ? 0x9F : 0xBF;
        if (p[1] < lo || p[1] > hi || (p[2] & 0xC0) != 0x80) {
            return 0;
        }
        return 3;
    }
    if (c < 0xF5) {
        if (remaining < 4) {
            return 0;
        }
        unsigned char lo = c == 0xF0 ? 0x90 : 0x80;
        unsigned char hi = c == 0xF4 ? 0x8F : 0xBF;
        if (p[1] < lo || p[1] > hi || (p[2] & 0xC0) != 0x80 || (p[3] & 0xC0) != 0x80) {
            return 0;
        }
        return 4;
    }
    return 0;
}

static bool output_text_requires_encoding(const char *data, size_t len) {
    static const char bytes_prefix[] = "@bytes:";
    static const char utf8_prefix[] = "@utf8:";
    if (!data) {
        return len != 0;
    }
    if ((len >= sizeof(bytes_prefix) - 1 &&
         memcmp(data, bytes_prefix, sizeof(bytes_prefix) - 1) == 0) ||
        (len >= sizeof(utf8_prefix) - 1 &&
         memcmp(data, utf8_prefix, sizeof(utf8_prefix) - 1) == 0)) {
        return true;
    }
    size_t offset = 0;
    while (offset < len) {
        size_t sequence = utf8_sequence_length((const unsigned char *)data + offset, len - offset);
        if (!sequence) {
            return true;
        }
        offset += sequence;
    }
    return false;
}

bool cbm_output_encode_text(const char *data, size_t len, char **encoded, size_t *encoded_len) {
    static const char bytes_prefix[] = "@bytes:";
    static const char utf8_prefix[] = "@utf8:";
    static const char hex[] = "0123456789abcdef";
    if (!encoded || !encoded_len || (!data && len != 0)) {
        return false;
    }
    *encoded = NULL;
    *encoded_len = len;
    if (!output_text_requires_encoding(data, len)) {
        return true;
    }

    bool valid_utf8 = true;
    size_t offset = 0;
    while (valid_utf8 && offset < len) {
        size_t sequence = utf8_sequence_length((const unsigned char *)data + offset, len - offset);
        if (!sequence) {
            valid_utf8 = false;
            break;
        }
        offset += sequence;
    }

    size_t prefix_len = valid_utf8 ? sizeof(utf8_prefix) - 1 : sizeof(bytes_prefix) - 1;
    size_t payload_len = len;
    if (!valid_utf8) {
        if (len > ((size_t)-1 - prefix_len - 1) / 2) {
            return false;
        }
        payload_len = len * 2;
    } else if (len > (size_t)-1 - prefix_len - 1) {
        return false;
    }
    char *out = (char *)malloc(prefix_len + payload_len + 1);
    if (!out) {
        return false;
    }
    memcpy(out, valid_utf8 ? utf8_prefix : bytes_prefix, prefix_len);
    if (valid_utf8) {
        if (len > 0) {
            memcpy(out + prefix_len, data, len);
        }
    } else {
        for (size_t i = 0; i < len; i++) {
            unsigned char byte = (unsigned char)data[i];
            out[prefix_len + i * 2] = hex[byte >> 4];
            out[prefix_len + i * 2 + 1] = hex[byte & 0x0f];
        }
    }
    *encoded_len = prefix_len + payload_len;
    out[*encoded_len] = '\0';
    *encoded = out;
    return true;
}

static bool needs_quotes_n(const char *s, size_t len) {
    if (!s || len == 0) {
        return false; /* empty cells emit as the "-" placeholder, not quotes */
    }
    size_t offset = 0;
    while (offset < len) {
        unsigned char c = (unsigned char)s[offset];
        /* Space-delimited rows: any internal whitespace or quote forces
         * quoting so column positions stay parseable. Control bytes and
         * invalid UTF-8 force the quoted path too, which sanitizes them —
         * one raw byte otherwise makes line-oriented consumers (BSD grep)
         * treat the ENTIRE tool output as unmatchable binary. */
        if (isspace(c) || s[offset] == '"' || s[offset] == '\r' || c < 0x20 || c == 0x7f) {
            return true;
        }
        if (c >= 0x80) {
            size_t sequence = utf8_sequence_length((const unsigned char *)s + offset, len - offset);
            if (!sequence) {
                return true;
            }
            offset += sequence;
        } else {
            offset++;
        }
    }
    if ((len == 4 && memcmp(s, "true", 4) == 0) || (len == 5 && memcmp(s, "false", 5) == 0) ||
        (len == 4 && memcmp(s, "null", 4) == 0) || (len == 1 && s[0] == '-')) {
        return true;
    }
    return looks_numeric_n(s, len);
}

static bool needs_quotes(const char *s) {
    return needs_quotes_n(s, s ? strlen(s) : 0);
}

static void append_quoted_n(cbm_sb_t *sb, const char *s, size_t len) {
    cbm_sb_append_n(sb, "\"", 1);
    size_t offset = 0;
    while (s && offset < len) {
        const char *p = s + offset;
        switch (*p) {
        case '"':
            cbm_sb_append_n(sb, "\\\"", 2);
            break;
        case '\\':
            cbm_sb_append_n(sb, "\\\\", 2);
            break;
        case '\n':
            cbm_sb_append_n(sb, "\\n", 2);
            break;
        case '\r':
            cbm_sb_append_n(sb, "\\r", 2);
            break;
        default: {
            unsigned char c = (unsigned char)*p;
            if (c >= 0x20 && c != 0x7f && c < 0x80) {
                cbm_sb_append_n(sb, p, 1);
            } else if (c < 0x20 || c == 0x7f) {
                /* JSON-style escape: the value stays one printable line. */
                char esc[8];
                snprintf(esc, sizeof(esc), "\\u%04x", (unsigned)c);
                cbm_sb_append(sb, esc);
            } else {
                size_t sequence = utf8_sequence_length((const unsigned char *)p, len - offset);
                if (sequence) {
                    cbm_sb_append_n(sb, p, sequence);
                    offset += sequence;
                    continue;
                } else {
                    /* Callers normalize first; retain a defensive printable
                     * escape if an internal caller ever violates that rule. */
                    char esc[8];
                    snprintf(esc, sizeof(esc), "\\u%04x", (unsigned)c);
                    cbm_sb_append(sb, esc);
                }
            }
            break;
        }
        }
        offset++;
    }
    cbm_sb_append_n(sb, "\"", 1);
}

static void append_quoted(cbm_sb_t *sb, const char *s) {
    append_quoted_n(sb, s, s ? strlen(s) : 0);
}

static void append_value_n(cbm_sb_t *sb, const char *s, size_t len) {
    if (!s || len == 0) {
        cbm_sb_append_n(sb, "-", 1); /* stable column positions for empties */
        return;
    }
    char *encoded = NULL;
    size_t encoded_len = 0;
    if (!cbm_output_encode_text(s, len, &encoded, &encoded_len)) {
        sb->oom = true;
        return;
    }
    const char *safe = encoded ? encoded : s;
    size_t safe_len = encoded ? encoded_len : len;
    if (needs_quotes_n(safe, safe_len)) {
        append_quoted_n(sb, safe, safe_len);
    } else {
        cbm_sb_append_n(sb, safe, safe_len);
    }
    free(encoded);
}

static void append_value(cbm_sb_t *sb, const char *s) {
    append_value_n(sb, s, s ? strlen(s) : 0);
}

/* Object keys and table column names share the compact tree's structural
 * line. Keep common identifier-like names bare, but quote anything that can
 * be confused with a delimiter or escape sequence. Unlike a value cell, an
 * empty key must remain the lossless string "" rather than the missing-value
 * placeholder "-". */
static void append_key_n(cbm_sb_t *sb, const char *s, size_t len) {
    const char *raw = s ? s : "";
    char *encoded = NULL;
    size_t encoded_len = 0;
    if (!cbm_output_encode_text(raw, len, &encoded, &encoded_len)) {
        sb->oom = true;
        return;
    }
    const char *safe = encoded ? encoded : raw;
    size_t safe_len = encoded ? encoded_len : len;
    if (safe_len == 0 || needs_quotes_n(safe, safe_len) || memchr(safe, ':', safe_len) ||
        memchr(safe, '(', safe_len) || memchr(safe, ')', safe_len) ||
        memchr(safe, '\\', safe_len)) {
        append_quoted_n(sb, safe, safe_len);
    } else {
        cbm_sb_append_n(sb, safe, safe_len);
    }
    free(encoded);
}

static void append_key(cbm_sb_t *sb, const char *s) {
    append_key_n(sb, s, s ? strlen(s) : 0);
}

/* ── Scalars ────────────────────────────────────────────────────── */

void cbm_tree_scalar_str(cbm_sb_t *sb, const char *key, const char *val) {
    append_key(sb, key);
    cbm_sb_append_n(sb, ": ", 2);
    append_value(sb, val);
    cbm_sb_append_n(sb, "\n", 1);
}

void cbm_tree_scalar_int(cbm_sb_t *sb, const char *key, long long v) {
    char num[32];
    snprintf(num, sizeof(num), "%lld", v);
    append_key(sb, key);
    cbm_sb_append_n(sb, ": ", 2);
    cbm_sb_append(sb, num);
    cbm_sb_append_n(sb, "\n", 1);
}

void cbm_tree_scalar_bool(cbm_sb_t *sb, const char *key, bool v) {
    append_key(sb, key);
    cbm_sb_append_n(sb, ": ", 2);
    cbm_sb_append(sb, v ? "true" : "false");
    cbm_sb_append_n(sb, "\n", 1);
}

/* ── Tables ─────────────────────────────────────────────────────── */

/* Tree-syntax table header: `key: N  (cols: a b c)` — count first (agents
 * read scale before rows), column names once, rows indented beneath. */
void cbm_tree_table_header(cbm_sb_t *sb, const char *key, int n, const char *const *cols,
                           int ncols) {
    char num[32];
    snprintf(num, sizeof(num), ": %d  (cols:", n);
    append_key(sb, key);
    cbm_sb_append(sb, num);
    for (int i = 0; i < ncols; i++) {
        cbm_sb_append_n(sb, " ", 1);
        append_key(sb, cols[i]);
    }
    cbm_sb_append_n(sb, ")\n", 2);
}

void cbm_tree_row_begin(cbm_sb_t *sb) {
    cbm_sb_append_n(sb, "  ", 2);
}

void cbm_tree_cell_str(cbm_sb_t *sb, const char *val, bool first) {
    if (!first) {
        cbm_sb_append_n(sb, " ", 1);
    }
    append_value(sb, val ? val : "");
}

void cbm_tree_cell_int(cbm_sb_t *sb, long long v, bool first) {
    char num[32];
    snprintf(num, sizeof(num), "%lld", v);
    if (!first) {
        cbm_sb_append_n(sb, " ", 1);
    }
    cbm_sb_append(sb, num);
}

void cbm_tree_cell_real(cbm_sb_t *sb, double v, bool first) {
    char num[48];
    snprintf(num, sizeof(num), "%.4g", v);
    if (!first) {
        cbm_sb_append_n(sb, " ", 1);
    }
    cbm_sb_append(sb, num);
}

void cbm_tree_cell_bool(cbm_sb_t *sb, bool v, bool first) {
    if (!first) {
        cbm_sb_append_n(sb, " ", 1);
    }
    cbm_sb_append(sb, v ? "true" : "false");
}

void cbm_tree_row_end(cbm_sb_t *sb) {
    cbm_sb_append_n(sb, "\n", 1);
}

/* ── Response-local prefix directories ─────────────────────────── */

enum {
    TREE_PREFIX_MIN_LEN = 12,
    TREE_PREFIX_MIN_USES = 3,
    TREE_PREFIX_MIN_SAVING = 64,
    TREE_PREFIX_MIN_PERCENT = 15,
    TREE_PREFIX_MIN_TOKEN_SHAPE_PERCENT = 1,
    TREE_PREFIX_MAX_REFS = 16,
    TREE_PREFIX_MAX_SEEDS = 1024,
    TREE_PREFIX_MAX_CANDIDATES = 512,
    TREE_PREFIX_MAX_CELLS = 8192,
    TREE_PREFIX_MAX_LEN = 16 * 1024,
};

typedef struct {
    char *text;
    size_t len;
    size_t uses;
    size_t score;
    int id;
    bool active;
} tree_prefix_candidate_t;

/* A dependency-free lower-resolution proxy for modern tokenizer pre-splits.
 * It deliberately undercounts words (camelCase and Unicode stay whole), while
 * counting punctuation, whitespace runs, and the common <=3-digit chunks.
 * Comparing complete renders catches byte wins such as `establishment.` ->
 * `@0+` that increase real cl100k/o200k tokens despite shorter text. */
static size_t tree_token_shape_units(const char *text) {
    const unsigned char *p = (const unsigned char *)text;
    size_t units = 0;
    while (p && *p) {
        if (isdigit(*p)) {
            size_t digits = 0;
            while (isdigit(*p)) {
                digits++;
                p++;
            }
            units += (digits + 2U) / 3U;
            continue;
        }
        if (isalpha(*p) || *p >= 0x80U) {
            do {
                p++;
            } while (*p && (isalpha(*p) || *p >= 0x80U));
            units++;
            continue;
        }
        if (isspace(*p)) {
            do {
                p++;
            } while (*p && isspace(*p));
            units++;
            continue;
        }

        p++;
        units++;
    }
    return units;
}

static bool tree_column_is_string(const bool *string_cols, int col) {
    return !string_cols || string_cols[col];
}

static bool tree_column_is_prefixable(const bool *string_cols, const bool *prefix_cols, int col) {
    return prefix_cols && prefix_cols[col] && tree_column_is_string(string_cols, col);
}

static void tree_render_table_direct(cbm_sb_t *sb, const char *key, int nrows,
                                     const char *const *cols, int ncols, const char *const *cells,
                                     const bool *string_cols) {
    cbm_tree_table_header(sb, key, nrows, cols, ncols);
    for (int row = 0; row < nrows; row++) {
        cbm_tree_row_begin(sb);
        for (int col = 0; col < ncols; col++) {
            const char *value = cells[(size_t)row * (size_t)ncols + (size_t)col];
            if (tree_column_is_string(string_cols, col)) {
                cbm_tree_cell_str(sb, value, col == 0);
            } else {
                if (col > 0) {
                    cbm_sb_append_n(sb, " ", 1);
                }
                cbm_sb_append(sb, value ? value : "null");
            }
        }
        cbm_tree_row_end(sb);
    }
}

static int tree_string_ptr_compare(const void *left, const void *right) {
    const char *const *a = (const char *const *)left;
    const char *const *b = (const char *const *)right;
    return strcmp(*a, *b);
}

static int tree_prefix_score_compare(const void *left, const void *right) {
    const tree_prefix_candidate_t *a = (const tree_prefix_candidate_t *)left;
    const tree_prefix_candidate_t *b = (const tree_prefix_candidate_t *)right;
    if (a->score != b->score) {
        return a->score > b->score ? -1 : 1;
    }
    if (a->len != b->len) {
        return a->len > b->len ? -1 : 1;
    }
    return strcmp(a->text, b->text);
}

static bool tree_prefix_candidate_add(tree_prefix_candidate_t *candidates, int *count,
                                      const char *text, size_t len) {
    if (len < TREE_PREFIX_MIN_LEN || len > TREE_PREFIX_MAX_LEN) {
        return true;
    }
    for (int i = 0; i < *count; i++) {
        if (candidates[i].len == len && memcmp(candidates[i].text, text, len) == 0) {
            return true;
        }
    }
    if (*count >= TREE_PREFIX_MAX_CANDIDATES) {
        return true;
    }
    char *copy = (char *)malloc(len + 1);
    if (!copy) {
        return false;
    }
    memcpy(copy, text, len);
    copy[len] = '\0';
    candidates[*count].text = copy;
    candidates[*count].len = len;
    (*count)++;
    return true;
}

static size_t tree_semantic_prefix_boundary(const char *text, size_t shared) {
    size_t boundary = 0;
    for (size_t i = 0; i < shared; i++) {
        if (text[i] == '/' || text[i] == '\\' || text[i] == '.') {
            boundary = i + 1;
        } else if (text[i] == ':' && i + 1 < shared && text[i + 1] == ':') {
            boundary = i + 2;
            i++;
        }
    }
    return boundary;
}

/* Values are sorted once before candidate scoring.  Binary-searching the
 * contiguous range that shares a prefix avoids rescanning every cell for
 * every candidate (up to 512 * 8192 comparisons in the bounded worst case). */
static size_t tree_sorted_prefix_count(const char *const *values, size_t count,
                                       const tree_prefix_candidate_t *candidate) {
    size_t lo = 0;
    size_t hi = count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (strncmp(values[mid], candidate->text, candidate->len) < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    size_t first = lo;
    hi = count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (strncmp(values[mid], candidate->text, candidate->len) <= 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo - first;
}

static int tree_prefix_choice(const char *value, const tree_prefix_candidate_t *candidates,
                              int selected_count, bool active_only) {
    if (!value) {
        return -1;
    }
    size_t value_len = strlen(value);
    if (output_text_requires_encoding(value, value_len) || needs_quotes(value)) {
        return -1;
    }
    int choice = -1;
    size_t longest = 0;
    for (int i = 0; i < selected_count; i++) {
        if ((active_only && !candidates[i].active) || value_len < candidates[i].len ||
            memcmp(value, candidates[i].text, candidates[i].len) != 0) {
            continue;
        }
        if (candidates[i].len > longest) {
            choice = i;
            longest = candidates[i].len;
        }
    }
    return choice;
}

/* Unquoted @N+suffix is reserved for a directory reference whenever a table
 * has a sibling <key>_refs table. Quote matching literals so a decoder can
 * always distinguish data from an encoded reference. */
static bool tree_looks_like_prefix_ref(const char *value) {
    if (!value || value[0] != '@' || !isdigit((unsigned char)value[1])) {
        return false;
    }
    const char *p = value + 2;
    while (isdigit((unsigned char)*p)) {
        p++;
    }
    return *p == '+';
}

static void tree_append_unambiguous_literal(cbm_sb_t *sb, const char *value) {
    if (tree_looks_like_prefix_ref(value)) {
        append_quoted(sb, value);
    } else {
        append_value(sb, value);
    }
}

static void tree_prefix_candidates_free(tree_prefix_candidate_t *candidates, int count) {
    for (int i = 0; i < count; i++) {
        free(candidates[i].text);
    }
}

static void tree_table_rows_impl(cbm_sb_t *sb, const char *key, int nrows, const char *const *cols,
                                 int ncols, const char *const *cells, const bool *string_cols,
                                 const bool *prefix_cols) {
    if (!sb || !key || !cols || !cells || nrows < TREE_PREFIX_MIN_USES || ncols < 1 ||
        !prefix_cols || (size_t)nrows > (size_t)-1 / (size_t)ncols) {
        if (sb && key && cols && cells && nrows >= 0 && ncols >= 0) {
            tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        }
        return;
    }

    size_t cell_count = (size_t)nrows * (size_t)ncols;
    if (cell_count > TREE_PREFIX_MAX_CELLS) {
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }

    const char **seeds = (const char **)malloc(sizeof(*seeds) * TREE_PREFIX_MAX_SEEDS);
    tree_prefix_candidate_t *candidates =
        (tree_prefix_candidate_t *)calloc(TREE_PREFIX_MAX_CANDIDATES, sizeof(*candidates));
    if (!seeds || !candidates) {
        free(seeds);
        free(candidates);
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }

    int seed_count = 0;
    for (size_t i = 0; i < cell_count && seed_count < TREE_PREFIX_MAX_SEEDS; i++) {
        int col = (int)(i % (size_t)ncols);
        if (!tree_column_is_prefixable(string_cols, prefix_cols, col)) {
            continue;
        }
        const char *value = cells[i];
        if (value && strlen(value) >= TREE_PREFIX_MIN_LEN && !needs_quotes(value)) {
            seeds[seed_count++] = value;
        }
    }
    if (seed_count < TREE_PREFIX_MIN_USES) {
        free(seeds);
        free(candidates);
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }
    qsort(seeds, (size_t)seed_count, sizeof(*seeds), tree_string_ptr_compare);

    int candidate_count = 0;
    bool candidate_oom = false;
    for (int i = 1; i < seed_count && !candidate_oom; i++) {
        const char *left = seeds[i - 1];
        const char *right = seeds[i];
        size_t shared = 0;
        while (left[shared] && right[shared] && left[shared] == right[shared] &&
               shared < TREE_PREFIX_MAX_LEN) {
            shared++;
        }
        if (left[shared] == '\0' && right[shared] == '\0') {
            candidate_oom = !tree_prefix_candidate_add(candidates, &candidate_count, left, shared);
        }
        size_t boundary = tree_semantic_prefix_boundary(left, shared);
        if (!candidate_oom && boundary >= TREE_PREFIX_MIN_LEN) {
            candidate_oom =
                !tree_prefix_candidate_add(candidates, &candidate_count, left, boundary);
        }
    }
    free(seeds);
    if (candidate_oom || candidate_count == 0) {
        tree_prefix_candidates_free(candidates, candidate_count);
        free(candidates);
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }

    const char **eligible_values = (const char **)malloc(sizeof(*eligible_values) * cell_count);
    if (!eligible_values) {
        tree_prefix_candidates_free(candidates, candidate_count);
        free(candidates);
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }
    size_t eligible_count = 0;
    for (size_t cell = 0; cell < cell_count; cell++) {
        int col = (int)(cell % (size_t)ncols);
        const char *value = cells[cell];
        if (tree_column_is_prefixable(string_cols, prefix_cols, col) && value &&
            strlen(value) >= TREE_PREFIX_MIN_LEN && !needs_quotes(value) &&
            !output_text_requires_encoding(value, strlen(value))) {
            eligible_values[eligible_count++] = value;
        }
    }
    qsort(eligible_values, eligible_count, sizeof(*eligible_values), tree_string_ptr_compare);

    /* Score candidates independently first. The final byte-for-byte render
     * comparison below accounts for overlap and directory overhead exactly. */
    for (int i = 0; i < candidate_count; i++) {
        candidates[i].uses =
            tree_sorted_prefix_count(eligible_values, eligible_count, &candidates[i]);
        if (candidates[i].uses >= TREE_PREFIX_MIN_USES && candidates[i].len > 5) {
            size_t gross = candidates[i].uses * (candidates[i].len - 5);
            size_t entry_cost = candidates[i].len + 12;
            candidates[i].score = gross > entry_cost ? gross - entry_cost : 0;
        }
    }
    free(eligible_values);
    qsort(candidates, (size_t)candidate_count, sizeof(*candidates), tree_prefix_score_compare);

    int selected_count = 0;
    while (selected_count < candidate_count && selected_count < TREE_PREFIX_MAX_REFS &&
           candidates[selected_count].score > 0) {
        selected_count++;
    }
    if (selected_count == 0) {
        tree_prefix_candidates_free(candidates, candidate_count);
        free(candidates);
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }

    size_t exclusive_uses[TREE_PREFIX_MAX_REFS] = {0};
    for (size_t cell = 0; cell < cell_count; cell++) {
        int col = (int)(cell % (size_t)ncols);
        if (!tree_column_is_prefixable(string_cols, prefix_cols, col)) {
            continue;
        }
        int choice = tree_prefix_choice(cells[cell], candidates, selected_count, false);
        if (choice >= 0) {
            exclusive_uses[choice]++;
        }
    }

    int active_count = 0;
    for (int i = 0; i < selected_count; i++) {
        candidates[i].active = exclusive_uses[i] >= TREE_PREFIX_MIN_USES;
        if (candidates[i].active) {
            candidates[i].id = active_count++;
        }
    }
    if (active_count == 0) {
        tree_prefix_candidates_free(candidates, candidate_count);
        free(candidates);
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }

    signed char *active_choices = (signed char *)malloc(cell_count);
    if (!active_choices) {
        tree_prefix_candidates_free(candidates, candidate_count);
        free(candidates);
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }
    for (size_t cell = 0; cell < cell_count; cell++) {
        int col = (int)(cell % (size_t)ncols);
        int choice = tree_column_is_prefixable(string_cols, prefix_cols, col)
                         ? tree_prefix_choice(cells[cell], candidates, selected_count, true)
                         : -1;
        active_choices[cell] = (signed char)choice;
    }

    size_t key_len = strlen(key);
    char *refs_key = (char *)malloc(key_len + sizeof("_refs"));
    char *rule_key = (char *)malloc(key_len + sizeof("_ref_rule"));
    if (!refs_key || !rule_key) {
        free(refs_key);
        free(rule_key);
        free(active_choices);
        tree_prefix_candidates_free(candidates, candidate_count);
        free(candidates);
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
        return;
    }
    snprintf(refs_key, key_len + sizeof("_refs"), "%s_refs", key);
    snprintf(rule_key, key_len + sizeof("_ref_rule"), "%s_ref_rule", key);

    cbm_sb_t plain;
    cbm_sb_init(&plain);
    tree_render_table_direct(&plain, key, nrows, cols, ncols, cells, string_cols);
    char *plain_text = cbm_sb_finish(&plain);

    cbm_sb_t compact;
    cbm_sb_init(&compact);
    const char *ref_cols[] = {"id", "prefix"};
    cbm_tree_table_header(&compact, refs_key, active_count, ref_cols, 2);
    for (int i = 0; i < selected_count; i++) {
        if (!candidates[i].active) {
            continue;
        }
        cbm_tree_row_begin(&compact);
        cbm_tree_cell_int(&compact, candidates[i].id, true);
        cbm_sb_append_n(&compact, " ", 1);
        /* Directories are declarations, never recursively expanded refs. Quote
         * a ref-looking prefix anyway so a simple decoder cannot mistake it
         * for another directory entry. */
        tree_append_unambiguous_literal(&compact, candidates[i].text);
        cbm_tree_row_end(&compact);
    }
    cbm_tree_scalar_str(&compact, rule_key, "@N+suffix=prefix+suffix");
    cbm_tree_table_header(&compact, key, nrows, cols, ncols);
    for (int row = 0; row < nrows; row++) {
        cbm_tree_row_begin(&compact);
        for (int col = 0; col < ncols; col++) {
            if (col > 0) {
                cbm_sb_append_n(&compact, " ", 1);
            }
            const char *value = cells[(size_t)row * (size_t)ncols + (size_t)col];
            bool string_cell = tree_column_is_string(string_cols, col);
            int choice = active_choices[(size_t)row * (size_t)ncols + (size_t)col];
            if (choice >= 0) {
                char ref[24];
                snprintf(ref, sizeof(ref), "@%d+", candidates[choice].id);
                cbm_sb_append(&compact, ref);
                cbm_sb_append(&compact, value + candidates[choice].len);
            } else if (string_cell) {
                tree_append_unambiguous_literal(&compact, value);
            } else {
                cbm_sb_append(&compact, value ? value : "null");
            }
        }
        cbm_tree_row_end(&compact);
    }
    char *compact_text = cbm_sb_finish(&compact);

    bool use_compact = false;
    if (plain_text && compact_text) {
        size_t plain_len = strlen(plain_text);
        size_t compact_len = strlen(compact_text);
        size_t saving = plain_len > compact_len ? plain_len - compact_len : 0;
        size_t plain_token_shape = tree_token_shape_units(plain_text);
        size_t compact_token_shape = tree_token_shape_units(compact_text);
        size_t token_shape_saving =
            plain_token_shape > compact_token_shape ? plain_token_shape - compact_token_shape : 0;
        use_compact =
            saving >= TREE_PREFIX_MIN_SAVING &&
            saving * 100 >= plain_len * TREE_PREFIX_MIN_PERCENT &&
            token_shape_saving * 100 >= plain_token_shape * TREE_PREFIX_MIN_TOKEN_SHAPE_PERCENT;
    }
    if (use_compact) {
        cbm_sb_append(sb, compact_text);
    } else if (plain_text) {
        cbm_sb_append(sb, plain_text);
    } else {
        tree_render_table_direct(sb, key, nrows, cols, ncols, cells, string_cols);
    }

    free(plain_text);
    free(compact_text);
    free(refs_key);
    free(rule_key);
    free(active_choices);
    tree_prefix_candidates_free(candidates, candidate_count);
    free(candidates);
}

void cbm_tree_table_rows_typed(cbm_sb_t *sb, const char *key, int nrows, const char *const *cols,
                               int ncols, const char *const *cells, const bool *string_cols) {
    tree_table_rows_impl(sb, key, nrows, cols, ncols, cells, string_cols, NULL);
}

void cbm_tree_table_rows(cbm_sb_t *sb, const char *key, int nrows, const char *const *cols,
                         int ncols, const char *const *cells) {
    tree_table_rows_impl(sb, key, nrows, cols, ncols, cells, NULL, NULL);
}

void cbm_tree_table_rows_profiled(cbm_sb_t *sb, const char *key, int nrows, const char *const *cols,
                                  int ncols, const char *const *cells, const bool *string_cols,
                                  const bool *prefix_cols) {
    tree_table_rows_impl(sb, key, nrows, cols, ncols, cells, string_cols, prefix_cols);
}

/* ── Generic JSON projection ────────────────────────────────────── */

enum { JSON_TREE_MAX_DEPTH = 32 };

static void json_tree_indent(cbm_sb_t *sb, int depth) {
    for (int i = 0; i < depth; i++) {
        cbm_sb_append_n(sb, "  ", 2);
    }
}

/* Source and Markdown lose substantial model utility when every newline is
 * escaped into one long quoted scalar. Use the familiar YAML block markers:
 * `|-` means no trailing newline, `|` means one, and `|+` preserves several.
 * The content remains readable/copyable and valid UTF-8. */
static void json_tree_block_string(cbm_sb_t *sb, const char *text, size_t len, int depth) {
    size_t trailing_newlines = 0;
    while (trailing_newlines < len && text[len - trailing_newlines - 1] == '\n') {
        trailing_newlines++;
    }
    cbm_sb_append(sb, trailing_newlines == 0 ? "|-\n" : trailing_newlines == 1 ? "|\n" : "|+\n");

    size_t line_offset = 0;
    for (;;) {
        const char *line = text + line_offset;
        const char *newline = memchr(line, '\n', len - line_offset);
        const char *end = newline ? newline : text + len;
        json_tree_indent(sb, depth + 1);
        for (const char *p = line; p < end; p++) {
            unsigned char c = (unsigned char)*p;
            if (c == '\t' || (c >= 0x20 && c != 0x7f && c < 0x80)) {
                cbm_sb_append_n(sb, p, 1);
            } else if (c >= 0x80) {
                size_t sequence = utf8_sequence_length((const unsigned char *)p, (size_t)(end - p));
                if (sequence && p + sequence <= end) {
                    cbm_sb_append_n(sb, p, sequence);
                    p += sequence - 1;
                } else {
                    cbm_sb_append_n(sb, "\xEF\xBF\xBD", 3);
                }
            } else {
                char escaped[8];
                snprintf(escaped, sizeof(escaped), "\\u%04x", (unsigned)c);
                cbm_sb_append(sb, escaped);
            }
        }
        cbm_sb_append_n(sb, "\n", 1);
        if (!newline) {
            break;
        }
        line_offset = (size_t)(newline - text) + 1;
        if (line_offset == len) {
            break;
        }
    }
}

static void json_tree_scalar(cbm_sb_t *sb, yyjson_val *value) {
    if (yyjson_is_str(value)) {
        append_value_n(sb, yyjson_get_str(value), yyjson_get_len(value));
    } else if (yyjson_is_bool(value)) {
        cbm_sb_append(sb, yyjson_get_bool(value) ? "true" : "false");
    } else if (yyjson_is_null(value)) {
        cbm_sb_append(sb, "null");
    } else if (yyjson_is_num(value)) {
        char *encoded = yyjson_val_write(value, 0, NULL);
        cbm_sb_append(sb, encoded ? encoded : "null");
        free(encoded);
    } else {
        char *encoded = yyjson_val_write(value, 0, NULL);
        append_value(sb, encoded ? encoded : "null");
        free(encoded);
    }
}

static bool json_tree_array_is_object_table(yyjson_val *array, yyjson_val **first_out) {
    if (!array || !first_out) {
        return false;
    }
    size_t count = yyjson_arr_size(array);
    if (count == 0) {
        return false;
    }
    yyjson_val *first = yyjson_arr_get_first(array);
    if (!first || !yyjson_is_obj(first) || yyjson_obj_size(first) == 0) {
        return false;
    }
    size_t expected = yyjson_obj_size(first);
    size_t index;
    size_t maximum;
    yyjson_val *item;
    yyjson_arr_foreach(array, index, maximum, item) {
        if (!yyjson_is_obj(item) || yyjson_obj_size(item) != expected) {
            return false;
        }
        size_t key_index;
        size_t key_maximum;
        yyjson_val *key;
        yyjson_val *unused;
        yyjson_obj_foreach(first, key_index, key_maximum, key, unused) {
            if (!yyjson_obj_getn(item, yyjson_get_str(key), yyjson_get_len(key))) {
                return false;
            }
        }
    }
    *first_out = first;
    return true;
}

static void json_tree_value(cbm_sb_t *sb, const char *key_name, size_t key_len, yyjson_val *value,
                            int depth);

static void json_tree_object_table(cbm_sb_t *sb, const char *key_name, size_t key_len,
                                   yyjson_val *array, yyjson_val *first, int depth) {
    if (!sb || !key_name || !array || !first) {
        return;
    }
    json_tree_indent(sb, depth);
    append_key_n(sb, key_name, key_len);
    char count[48];
    snprintf(count, sizeof(count), ": %zu  (cols:", yyjson_arr_size(array));
    cbm_sb_append(sb, count);

    size_t key_index;
    size_t key_maximum;
    yyjson_val *column;
    yyjson_val *unused;
    yyjson_obj_foreach(first, key_index, key_maximum, column, unused) {
        cbm_sb_append_n(sb, " ", 1);
        append_key_n(sb, yyjson_get_str(column), yyjson_get_len(column));
    }
    cbm_sb_append_n(sb, ")\n", 2);

    size_t row_index;
    size_t row_maximum;
    yyjson_val *row;
    yyjson_arr_foreach(array, row_index, row_maximum, row) {
        json_tree_indent(sb, depth + 1);
        bool first_cell = true;
        yyjson_obj_foreach(first, key_index, key_maximum, column, unused) {
            if (!first_cell) {
                cbm_sb_append_n(sb, " ", 1);
            }
            yyjson_val *cell = yyjson_obj_getn(row, yyjson_get_str(column), yyjson_get_len(column));
            json_tree_scalar(sb, cell);
            first_cell = false;
        }
        cbm_sb_append_n(sb, "\n", 1);
    }
}

static void json_tree_array(cbm_sb_t *sb, const char *key_name, size_t key_len, yyjson_val *array,
                            int depth) {
    yyjson_val *first = NULL;
    if (json_tree_array_is_object_table(array, &first)) {
        json_tree_object_table(sb, key_name, key_len, array, first, depth);
        return;
    }

    json_tree_indent(sb, depth);
    append_key_n(sb, key_name, key_len);
    char count[48];
    snprintf(count, sizeof(count), ": %zu\n", yyjson_arr_size(array));
    cbm_sb_append(sb, count);
    size_t index;
    size_t maximum;
    yyjson_val *item;
    yyjson_arr_foreach(array, index, maximum, item) {
        if (yyjson_is_obj(item)) {
            json_tree_indent(sb, depth + 1);
            cbm_sb_append(sb, "-\n");
            size_t field_index;
            size_t field_maximum;
            yyjson_val *field;
            yyjson_val *field_value;
            yyjson_obj_foreach(item, field_index, field_maximum, field, field_value) {
                json_tree_value(sb, yyjson_get_str(field), yyjson_get_len(field), field_value,
                                depth + 2);
            }
        } else if (yyjson_is_arr(item)) {
            json_tree_value(sb, "-", 1, item, depth + 1);
        } else {
            json_tree_indent(sb, depth + 1);
            cbm_sb_append(sb, "- ");
            json_tree_scalar(sb, item);
            cbm_sb_append_n(sb, "\n", 1);
        }
    }
}

static void json_tree_value(cbm_sb_t *sb, const char *key_name, size_t key_len, yyjson_val *value,
                            int depth) {
    if (depth >= JSON_TREE_MAX_DEPTH) {
        json_tree_indent(sb, depth);
        append_key_n(sb, key_name, key_len);
        cbm_sb_append(sb, ": ");
        char *encoded = yyjson_val_write(value, 0, NULL);
        append_value(sb, encoded ? encoded : "null");
        free(encoded);
        cbm_sb_append_n(sb, "\n", 1);
        return;
    }
    if (yyjson_is_obj(value)) {
        json_tree_indent(sb, depth);
        append_key_n(sb, key_name, key_len);
        cbm_sb_append(sb, ":\n");
        size_t index;
        size_t maximum;
        yyjson_val *key;
        yyjson_val *child;
        yyjson_obj_foreach(value, index, maximum, key, child) {
            json_tree_value(sb, yyjson_get_str(key), yyjson_get_len(key), child, depth + 1);
        }
    } else if (yyjson_is_arr(value)) {
        json_tree_array(sb, key_name, key_len, value, depth);
    } else if (yyjson_is_str(value) &&
               !memchr(yyjson_get_str(value), '\0', yyjson_get_len(value)) &&
               (memchr(yyjson_get_str(value), '\n', yyjson_get_len(value)) ||
                memchr(yyjson_get_str(value), '\r', yyjson_get_len(value)))) {
        json_tree_indent(sb, depth);
        append_key_n(sb, key_name, key_len);
        cbm_sb_append(sb, ": ");
        json_tree_block_string(sb, yyjson_get_str(value), yyjson_get_len(value), depth);
    } else {
        json_tree_indent(sb, depth);
        append_key_n(sb, key_name, key_len);
        cbm_sb_append(sb, ": ");
        json_tree_scalar(sb, value);
        cbm_sb_append_n(sb, "\n", 1);
    }
}

char *cbm_json_to_tree(const char *json) {
    if (!json) {
        return NULL;
    }
    yyjson_doc *document = yyjson_read(json, strlen(json), 0);
    if (!document) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(document);
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    if (yyjson_is_obj(root)) {
        size_t index;
        size_t maximum;
        yyjson_val *key;
        yyjson_val *value;
        yyjson_obj_foreach(root, index, maximum, key, value) {
            json_tree_value(&sb, yyjson_get_str(key), yyjson_get_len(key), value, 0);
        }
    } else if (yyjson_is_arr(root)) {
        json_tree_array(&sb, "items", sizeof("items") - 1, root, 0);
    } else {
        cbm_sb_append(&sb, "value: ");
        json_tree_scalar(&sb, root);
        cbm_sb_append_n(&sb, "\n", 1);
    }
    yyjson_doc_free(document);
    return cbm_sb_finish(&sb);
}
