/* compact_out.c — tree-format emission helpers. See compact_out.h for the contract. */
#include "mcp/compact_out.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
static bool looks_numeric(const char *s) {
    if (!s || !*s) {
        return false;
    }
    const char *p = s;
    if (*p == '-' || *p == '+') {
        p++;
    }
    bool digit = false;
    bool dot = false;
    for (; *p; p++) {
        if (isdigit((unsigned char)*p)) {
            digit = true;
        } else if (*p == '.' && !dot) {
            dot = true;
        } else {
            return false;
        }
    }
    return digit;
}

static bool needs_quotes(const char *s) {
    if (!s || !*s) {
        return false; /* empty cells emit as the "-" placeholder, not quotes */
    }
    for (const char *p = s; *p; p++) {
        /* Space-delimited rows: any internal whitespace or quote forces
         * quoting so column positions stay parseable. */
        if (isspace((unsigned char)*p) || *p == '"' || *p == '\r') {
            return true;
        }
    }
    if (strcmp(s, "true") == 0 || strcmp(s, "false") == 0 || strcmp(s, "null") == 0 ||
        strcmp(s, "-") == 0) {
        return true;
    }
    return looks_numeric(s);
}

static void append_quoted(cbm_sb_t *sb, const char *s) {
    cbm_sb_append_n(sb, "\"", 1);
    for (const char *p = s ? s : ""; *p; p++) {
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
        default:
            cbm_sb_append_n(sb, p, 1);
            break;
        }
    }
    cbm_sb_append_n(sb, "\"", 1);
}

static void append_value(cbm_sb_t *sb, const char *s) {
    if (!s || !*s) {
        cbm_sb_append_n(sb, "-", 1); /* stable column positions for empties */
        return;
    }
    if (needs_quotes(s)) {
        append_quoted(sb, s);
    } else {
        cbm_sb_append(sb, s);
    }
}

/* ── Scalars ────────────────────────────────────────────────────── */

void cbm_tree_scalar_str(cbm_sb_t *sb, const char *key, const char *val) {
    cbm_sb_append(sb, key);
    cbm_sb_append_n(sb, ": ", 2);
    append_value(sb, val);
    cbm_sb_append_n(sb, "\n", 1);
}

void cbm_tree_scalar_int(cbm_sb_t *sb, const char *key, long long v) {
    char num[32];
    snprintf(num, sizeof(num), "%lld", v);
    cbm_sb_append(sb, key);
    cbm_sb_append_n(sb, ": ", 2);
    cbm_sb_append(sb, num);
    cbm_sb_append_n(sb, "\n", 1);
}

void cbm_tree_scalar_bool(cbm_sb_t *sb, const char *key, bool v) {
    cbm_sb_append(sb, key);
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
    cbm_sb_append(sb, key);
    cbm_sb_append(sb, num);
    for (int i = 0; i < ncols; i++) {
        cbm_sb_append_n(sb, " ", 1);
        cbm_sb_append(sb, cols[i]);
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
