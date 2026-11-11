// sql_values.c — exclude literal-only INSERT value tuples from the SQL parse (#1735).
//
// See sql_values.h for why. The scanner is one forward pass over the file. It
// tracks enough of SQL's lexical structure to never mistake the inside of a
// string, quoted identifier or comment for syntax:
//   - '...' strings with both the standard '' escape and MySQL's backslash escape
//   - "..." and `...` quoted identifiers, $tag$...$tag$ dollar-quoted bodies
//   - -- and # line comments, /* */ block comments
// and it knows only one statement shape: INSERT/REPLACE ... VALUES (..), (..).
//
// Everything it is unsure about stays in the parse. A tuple is excluded only
// when every value in it is a plain literal and the tuple closes cleanly; a
// string holding a raw newline also keeps its tuple, because a dump writes line
// breaks as \n and a raw one means the scan's idea of where strings end may not
// match the grammar's.

#include "sql_values.h"
#include "foundation/mem_core.h"
#include <ctype.h>
#include <stddef.h>
#include <string.h>
#include <strings.h>

typedef struct {
    const char *s;
    uint32_t len;
    uint32_t i;
    uint32_t row;
    uint32_t bol; /* byte offset where the current row starts */
} sqlv_scan_t;

typedef struct {
    TSRange *items;
    uint32_t count;
    uint32_t cap;
    bool failed;
} sqlv_ranges_t;

enum { SQLV_INITIAL_CAP = 64, SQLV_GROWTH = 2 };

static char sqlv_at(const sqlv_scan_t *sc, uint32_t off) {
    return sc->i + off < sc->len ? sc->s[sc->i + off] : '\0';
}

static void sqlv_step(sqlv_scan_t *sc) {
    if (sc->s[sc->i] == '\n') {
        sc->row++;
        sc->bol = sc->i + 1;
    }
    sc->i++;
}

static TSPoint sqlv_point(const sqlv_scan_t *sc) {
    return (TSPoint){sc->row, sc->i - sc->bol};
}

static bool sqlv_word_char(char c) {
    return isalnum((unsigned char)c) || c == '_' || c == '$' || (unsigned char)c >= 0x80;
}

static bool sqlv_word_is(const char *w, uint32_t n, const char *kw) {
    return strlen(kw) == n && strncasecmp(w, kw, n) == 0;
}

/* Consume a '..', "..", or `..` run starting at the opening quote. Doubled
 * quotes escape everywhere; a backslash escapes in strings and "..". Sets
 * *raw_newline when the run crosses a line break. False if it never closes. */
static bool sqlv_skip_quoted(sqlv_scan_t *sc, bool *raw_newline) {
    char q = sc->s[sc->i];
    sqlv_step(sc);
    while (sc->i < sc->len) {
        char c = sc->s[sc->i];
        if (c == '\n') {
            *raw_newline = true;
        }
        if (c == '\\' && q != '`' && sc->i + 1 < sc->len) {
            sqlv_step(sc);
            sqlv_step(sc);
            continue;
        }
        sqlv_step(sc);
        if (c == q) {
            if (sqlv_at(sc, 0) != q) {
                return true;
            }
            sqlv_step(sc); /* doubled quote: an escaped quote character */
        }
    }
    return false;
}

/* $tag$ ... $tag$ (PostgreSQL). Returns false, consuming nothing, when the `$`
 * does not open a dollar quote ($1 parameters, a lone $). */
static bool sqlv_skip_dollar(sqlv_scan_t *sc) {
    uint32_t t = sc->i + 1;
    if (t < sc->len && isdigit((unsigned char)sc->s[t])) {
        return false;
    }
    while (t < sc->len && sc->s[t] != '$' && sqlv_word_char(sc->s[t])) {
        t++;
    }
    if (t >= sc->len || sc->s[t] != '$') {
        return false;
    }
    uint32_t tag_len = t - sc->i + 1; /* the opener, both dollars included */
    const char *tag = sc->s + sc->i;
    for (uint32_t k = 0; k < tag_len; k++) {
        sqlv_step(sc);
    }
    while (sc->i < sc->len) {
        if (sc->s[sc->i] == '$' && sc->len - sc->i >= tag_len &&
            memcmp(sc->s + sc->i, tag, tag_len) == 0) {
            for (uint32_t k = 0; k < tag_len; k++) {
                sqlv_step(sc);
            }
            return true;
        }
        sqlv_step(sc);
    }
    return true;
}

/* Consume one comment starting at the cursor; false if there is none. */
static bool sqlv_skip_comment(sqlv_scan_t *sc) {
    char c = sqlv_at(sc, 0);
    char n = sqlv_at(sc, 1);
    if ((c == '-' && n == '-') || c == '#') {
        while (sc->i < sc->len && sc->s[sc->i] != '\n') {
            sqlv_step(sc);
        }
        return true;
    }
    if (c == '/' && n == '*') {
        sqlv_step(sc);
        sqlv_step(sc);
        while (sc->i < sc->len && !(sc->s[sc->i] == '*' && sqlv_at(sc, 1) == '/')) {
            sqlv_step(sc);
        }
        if (sc->i < sc->len) {
            sqlv_step(sc);
            sqlv_step(sc);
        }
        return true;
    }
    return false;
}

/* Skip whitespace and comments. *ws_only (optional) is cleared if a comment was
 * among them. */
static void sqlv_skip_trivia(sqlv_scan_t *sc, bool *ws_only) {
    while (sc->i < sc->len) {
        if (isspace((unsigned char)sc->s[sc->i])) {
            sqlv_step(sc);
        } else if (sqlv_skip_comment(sc)) {
            if (ws_only) {
                *ws_only = false;
            }
        } else {
            return;
        }
    }
}

/* Consume one quoted or dollar-quoted run if one starts here. */
static bool sqlv_skip_any_quoted(sqlv_scan_t *sc) {
    char c = sc->s[sc->i];
    bool nl = false;
    if (c == '\'' || c == '"' || c == '`') {
        (void)sqlv_skip_quoted(sc, &nl);
        return true;
    }
    return c == '$' && sqlv_skip_dollar(sc);
}

/* From inside an open parenthesis, consume through its matching ')'. */
static bool sqlv_skip_balanced(sqlv_scan_t *sc) {
    int depth = 1;
    while (sc->i < sc->len) {
        if (sqlv_skip_comment(sc) || sqlv_skip_any_quoted(sc)) {
            continue;
        }
        char c = sc->s[sc->i];
        sqlv_step(sc);
        if (c == '(') {
            depth++;
        } else if (c == ')' && --depth == 0) {
            return true;
        }
    }
    return false;
}

static bool sqlv_read_string(sqlv_scan_t *sc) {
    bool nl = false;
    return sqlv_skip_quoted(sc, &nl) && !nl;
}

/* 12, -3.5, 1e-9, .5, 0x1F, 0b101 — followed by a non-word character. */
static bool sqlv_read_number(sqlv_scan_t *sc) {
    char n = sqlv_at(sc, 1);
    if (sqlv_at(sc, 0) == '0' && (n == 'x' || n == 'X') &&
        isxdigit((unsigned char)sqlv_at(sc, 2))) {
        sqlv_step(sc);
        sqlv_step(sc);
        while (isxdigit((unsigned char)sqlv_at(sc, 0))) {
            sqlv_step(sc);
        }
    } else if (sqlv_at(sc, 0) == '0' && (n == 'b' || n == 'B') &&
               (sqlv_at(sc, 2) == '0' || sqlv_at(sc, 2) == '1')) {
        sqlv_step(sc);
        sqlv_step(sc);
        while (sqlv_at(sc, 0) == '0' || sqlv_at(sc, 0) == '1') {
            sqlv_step(sc);
        }
    } else {
        while (isdigit((unsigned char)sqlv_at(sc, 0)) || sqlv_at(sc, 0) == '.') {
            sqlv_step(sc);
        }
        char e = sqlv_at(sc, 0);
        char s1 = sqlv_at(sc, 1);
        if ((e == 'e' || e == 'E') &&
            (isdigit((unsigned char)s1) ||
             ((s1 == '+' || s1 == '-') && isdigit((unsigned char)sqlv_at(sc, 2))))) {
            sqlv_step(sc);
            sqlv_step(sc);
            while (isdigit((unsigned char)sqlv_at(sc, 0))) {
                sqlv_step(sc);
            }
        }
    }
    return !sqlv_word_char(sqlv_at(sc, 0)) && sqlv_at(sc, 0) != '.';
}

/* A word in value position: NULL/TRUE/FALSE/DEFAULT, an X'..' / B'..' / N'..'
 * prefixed string, or a _charset introducer before a string or number. */
static bool sqlv_read_word_literal(sqlv_scan_t *sc) {
    const char *w = sc->s + sc->i;
    uint32_t n = 0;
    while (sqlv_word_char(sqlv_at(sc, 0))) {
        sqlv_step(sc);
        n++;
    }
    if (sqlv_word_is(w, n, "NULL") || sqlv_word_is(w, n, "TRUE") || sqlv_word_is(w, n, "FALSE") ||
        sqlv_word_is(w, n, "DEFAULT")) {
        return true;
    }
    if (n == 1 && strchr("xXbBnN", w[0]) && sqlv_at(sc, 0) == '\'') {
        return sqlv_read_string(sc);
    }
    if (n > 1 && w[0] == '_') {
        sqlv_skip_trivia(sc, NULL);
        if (sqlv_at(sc, 0) == '\'') {
            return sqlv_read_string(sc);
        }
        return isdigit((unsigned char)sqlv_at(sc, 0)) && sqlv_read_number(sc);
    }
    return false;
}

/* Consume one value if it is a plain literal. Never consumes a parenthesis, so
 * on false the caller can still find the tuple's end from the cursor. */
static bool sqlv_read_literal(sqlv_scan_t *sc) {
    char c = sqlv_at(sc, 0);
    char n = sqlv_at(sc, 1);
    if (c == '\'') {
        return sqlv_read_string(sc);
    }
    if ((c == '-' || c == '+') &&
        (isdigit((unsigned char)n) || (n == '.' && isdigit((unsigned char)sqlv_at(sc, 2))))) {
        sqlv_step(sc);
        return sqlv_read_number(sc);
    }
    if (isdigit((unsigned char)c) || (c == '.' && isdigit((unsigned char)n))) {
        return sqlv_read_number(sc);
    }
    if (isalpha((unsigned char)c) || c == '_') {
        return sqlv_read_word_literal(sc);
    }
    return false;
}

/* Consume one tuple starting at its '('. *literal says whether every value in
 * it was a plain literal. Returns false if the tuple never closes. */
static bool sqlv_scan_tuple(sqlv_scan_t *sc, bool *literal) {
    sqlv_step(sc);
    sqlv_skip_trivia(sc, NULL);
    if (sqlv_at(sc, 0) == ')') {
        sqlv_step(sc);
        *literal = true;
        return true;
    }
    while (sqlv_read_literal(sc)) {
        sqlv_skip_trivia(sc, NULL);
        char c = sqlv_at(sc, 0);
        if (c == ')') {
            sqlv_step(sc);
            *literal = true;
            return true;
        }
        if (c != ',') {
            break;
        }
        sqlv_step(sc);
        sqlv_skip_trivia(sc, NULL);
    }
    *literal = false;
    return sqlv_skip_balanced(sc);
}

static void sqlv_push(sqlv_ranges_t *r, TSRange range) {
    if (r->failed) {
        return;
    }
    if (r->count == r->cap) {
        uint32_t cap = r->cap ? r->cap * SQLV_GROWTH : SQLV_INITIAL_CAP;
        TSRange *grown =
            (TSRange *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, r->items, (size_t)cap * sizeof(TSRange));
        if (!grown) {
            r->failed = true;
            return;
        }
        r->items = grown;
        r->cap = cap;
    }
    r->items[r->count++] = range;
}

/* The cursor sits just past VALUES. Keep the first tuple; exclude each later
 * literal-only tuple together with the comma before it. Runs of excluded tuples
 * separated only by whitespace become one exclusion. */
static void sqlv_handle_values(sqlv_scan_t *sc, sqlv_ranges_t *ex) {
    sqlv_skip_trivia(sc, NULL);
    bool literal = false;
    if (sqlv_at(sc, 0) != '(' || !sqlv_scan_tuple(sc, &literal)) {
        return;
    }
    bool pending = false;
    TSRange cur = {0};
    for (;;) {
        bool ws_only = true;
        sqlv_skip_trivia(sc, &ws_only);
        if (sqlv_at(sc, 0) != ',') {
            break;
        }
        uint32_t comma = sc->i;
        TSPoint comma_pt = sqlv_point(sc);
        sqlv_step(sc);
        sqlv_skip_trivia(sc, NULL);
        if (sqlv_at(sc, 0) != '(' || !sqlv_scan_tuple(sc, &literal)) {
            break;
        }
        if (!literal) {
            if (pending) {
                sqlv_push(ex, cur);
            }
            pending = false;
            continue;
        }
        if (!pending || !ws_only) {
            if (pending) {
                sqlv_push(ex, cur);
            }
            cur.start_byte = comma;
            cur.start_point = comma_pt;
            pending = true;
        }
        cur.end_byte = sc->i;
        cur.end_point = sqlv_point(sc);
    }
    if (pending) {
        sqlv_push(ex, cur);
    }
}

typedef struct {
    bool started; /* a token of the current statement has been seen */
    bool insert;  /* the statement began with INSERT or REPLACE */
    int depth;
} sqlv_stmt_t;

static void sqlv_on_word(sqlv_scan_t *sc, sqlv_stmt_t *st, sqlv_ranges_t *ex) {
    const char *w = sc->s + sc->i;
    uint32_t n = 0;
    while (sc->i < sc->len && sqlv_word_char(sc->s[sc->i])) {
        sqlv_step(sc);
        n++;
    }
    if (!st->started) {
        st->started = true;
        st->insert = sqlv_word_is(w, n, "INSERT") || sqlv_word_is(w, n, "REPLACE");
    } else if (st->insert && st->depth == 0 &&
               (sqlv_word_is(w, n, "VALUES") || sqlv_word_is(w, n, "VALUE"))) {
        sqlv_handle_values(sc, ex);
    }
}

static void sqlv_scan_file(sqlv_scan_t *sc, sqlv_ranges_t *ex) {
    sqlv_stmt_t st = {false, false, 0};
    while (sc->i < sc->len && !ex->failed) {
        if (sqlv_skip_comment(sc)) {
            continue;
        }
        char c = sc->s[sc->i];
        if (sqlv_skip_any_quoted(sc)) {
            st.started = true;
            continue;
        }
        if (sqlv_word_char(c)) {
            sqlv_on_word(sc, &st, ex);
            continue;
        }
        if (c == ';') {
            st = (sqlv_stmt_t){false, false, 0};
        } else if (c == '(') {
            st.depth++;
        } else if (c == ')' && st.depth > 0) {
            st.depth--;
        }
        if (!isspace((unsigned char)c) && c != ';') {
            st.started = true;
        }
        sqlv_step(sc);
    }
}

bool cbm_sql_values_kept_ranges(const char *src, uint32_t len, CBMSqlKeptRanges *out) {
    out->items = NULL;
    out->count = 0;
    if (!src || len == 0) {
        return false;
    }
    sqlv_scan_t sc = {src, len, 0, 0, 0};
    sqlv_ranges_t ex = {NULL, 0, 0, false};
    sqlv_scan_file(&sc, &ex);
    if (ex.failed || ex.count == 0) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, ex.items);
        return false;
    }
    /* Kept = the complement of the exclusions. The scan ended at EOF, so its
     * cursor point is the end of the file. */
    TSRange *kept =
        (TSRange *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, ((size_t)ex.count + 1) * sizeof(TSRange));
    if (!kept) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, ex.items);
        return false;
    }
    uint32_t n = 0;
    uint32_t from = 0;
    TSPoint from_pt = {0, 0};
    for (uint32_t k = 0; k <= ex.count; k++) {
        uint32_t to = k < ex.count ? ex.items[k].start_byte : len;
        TSPoint to_pt = k < ex.count ? ex.items[k].start_point : sqlv_point(&sc);
        if (to > from) {
            kept[n++] = (TSRange){from_pt, to_pt, from, to};
        }
        if (k < ex.count) {
            from = ex.items[k].end_byte;
            from_pt = ex.items[k].end_point;
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, ex.items);
    out->items = kept;
    out->count = n;
    return true;
}

void cbm_sql_kept_ranges_free(CBMSqlKeptRanges *ranges) {
    if (!ranges) {
        return;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, ranges->items);
    ranges->items = NULL;
    ranges->count = 0;
}
