/*
 * cypher.c — Cypher query engine: lexer, parser, planner, executor.
 *
 * Translates a subset of Cypher into SQL queries against cbm_store.
 * Supports MATCH patterns with relationships, WHERE filters,
 * RETURN with COUNT/ORDER BY/LIMIT/DISTINCT.
 */
#include "cypher/cypher.h"
#include "store/store.h"
#include "foundation/platform.h"

enum {
    CYP_BUF_16 = 16,
    CYP_BUF_48 = 48, /* ASCII '0' */
    CYP_BUF_8 = 8,
    CYP_BUF_4 = 4,
    CYP_MAX_TOKEN = 10, /* max token lookahead */
    CYP_PAIR = 2,
    CYP_TRIPLE = 3,
    CYP_INIT_CAP4 = 4,     /* initial small array capacity */
    CYP_INIT_CAP8 = 8,     /* initial medium array capacity */
    CYP_MAX_VARS = 16,     /* max Cypher variables in a query */
    CYP_MAX_EDGE_VARS = 8, /* max edge variables */
    CYP_GROWTH_10 = 10,    /* binding growth factor */
    CYP_MAX_DEPTH = 10,    /* max variable-length path depth */
    CYP_CHAR_IDX1 = 1,     /* second character index (e.g. op[1]) */
    CYP_EBUF_MASK = 7,
    CYP_NODE_COLS = 4, /* columns per node var: name, qn, label, file */
    CYP_EDGE_COLS = 3, /* columns per edge var: name, qn, label */
    CYP_COL_BUF = 48,  /* max column buffer (16 vars * 3 cols) */
    CYP_FOUND_NONE = -1,
    /* search miss sentinel */ /* mask for ebuf ring buffer (8 entries) */
};
#define CYP_DBL_MAX 1e308

#include <ctype.h>
#include "foundation/compat_regex.h"
#include <stddef.h>
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Helpers ────────────────────────────────────────────────────── */

static char *heap_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *d = malloc(len + SKIP_ONE);
    if (d) {
        memcpy(d, s, len + 1);
    }
    return d;
}

static char *heap_strndup(const char *s, size_t n) {
    char *d = malloc(n + SKIP_ONE);
    if (d) {
        memcpy(d, s, n);
        d[n] = '\0';
    }
    return d;
}

/* ══════════════════════════════════════════════════════════════════
 *  LEXER
 * ══════════════════════════════════════════════════════════════════ */

static void lex_push(cbm_lex_result_t *r, cbm_token_type_t type, const char *text, int pos) {
    if (r->count >= r->capacity) {
        r->capacity = r->capacity ? r->capacity * PAIR_LEN : CBM_SZ_32;
        r->tokens = safe_realloc(r->tokens, r->capacity * sizeof(cbm_token_t));
    }
    r->tokens[r->count++] = (cbm_token_t){.type = type, .text = heap_strdup(text), .pos = pos};
}

static void lex_push_n(cbm_lex_result_t *r, cbm_token_type_t type, const char *start, size_t len,
                       int pos) {
    if (r->count >= r->capacity) {
        r->capacity = r->capacity ? r->capacity * PAIR_LEN : CBM_SZ_32;
        r->tokens = safe_realloc(r->tokens, r->capacity * sizeof(cbm_token_t));
    }
    r->tokens[r->count++] =
        (cbm_token_t){.type = type, .text = heap_strndup(start, len), .pos = pos};
}

/* Parse a string literal (with escape handling) into the token list.
 * *pos points at the character after the opening quote; updated past closing quote. */
static void lex_string_literal(const char *input, int len, int *pos, char quote,
                               cbm_lex_result_t *out) {
    int start = *pos;
    char buf[CBM_SZ_4K];
    int blen = 0;
    const int max_blen = CBM_SZ_4K - 1;
    while (*pos < len && input[*pos] != quote) {
        if (input[*pos] == '\\' && *pos + SKIP_ONE < len) {
            (*pos)++;
            if (blen < max_blen) {
                switch (input[*pos]) {
                case 'n':
                    buf[blen++] = '\n';
                    break;
                case 't':
                    buf[blen++] = '\t';
                    break;
                case '\\':
                    buf[blen++] = '\\';
                    break;
                default:
                    buf[blen++] = input[*pos];
                    break;
                }
            }
        } else {
            if (blen < max_blen) {
                buf[blen++] = input[*pos];
            }
        }
        (*pos)++;
    }
    buf[blen] = '\0';
    if (*pos < len) {
        (*pos)++; /* skip closing quote */
    }
    lex_push(out, TOK_STRING, buf, start - SKIP_ONE);
}

/* Keyword table (case-insensitive lookup) */
typedef struct {
    const char *name;
    cbm_token_type_t type;
} kw_entry_t;
static const kw_entry_t keywords[] = {
    /* Core query */
    {"MATCH", TOK_MATCH},
    {"WHERE", TOK_WHERE},
    {"RETURN", TOK_RETURN},
    {"ORDER", TOK_ORDER},
    {"BY", TOK_BY},
    {"LIMIT", TOK_LIMIT},
    {"AND", TOK_AND},
    {"OR", TOK_OR},
    {"AS", TOK_AS},
    {"DISTINCT", TOK_DISTINCT},
    {"COUNT", TOK_COUNT},
    {"CONTAINS", TOK_CONTAINS},
    {"STARTS", TOK_STARTS},
    {"WITH", TOK_WITH},
    {"NOT", TOK_NOT},
    {"ASC", TOK_ASC},
    {"DESC", TOK_DESC},
    /* Phase 1-2: operators + expression */
    {"ENDS", TOK_ENDS},
    {"IN", TOK_IN},
    {"IS", TOK_IS},
    {"NULL", TOK_NULL_KW},
    {"XOR", TOK_XOR},
    /* Phase 3-4: SKIP, UNION, UNWIND, aggregates */
    {"SKIP", TOK_SKIP},
    {"UNION", TOK_UNION},
    {"UNWIND", TOK_UNWIND},
    {"SUM", TOK_SUM},
    {"AVG", TOK_AVG},
    {"MIN", TOK_MIN_KW},
    {"MAX", TOK_MAX_KW},
    {"COLLECT", TOK_COLLECT},
    /* Phase 5: string functions + CASE */
    {"toLower", TOK_TOLOWER},
    {"toUpper", TOK_TOUPPER},
    {"toString", TOK_TOSTRING},
    {"tolower", TOK_TOLOWER},
    {"toupper", TOK_TOUPPER},
    {"tostring", TOK_TOSTRING},
    {"CASE", TOK_CASE},
    {"WHEN", TOK_WHEN},
    {"THEN", TOK_THEN},
    {"ELSE", TOK_ELSE},
    {"END", TOK_END},
    /* Phase 7: OPTIONAL */
    {"OPTIONAL", TOK_OPTIONAL},
    /* Recognized-but-unsupported write/admin keywords */
    {"CREATE", TOK_CREATE},
    {"DELETE", TOK_DELETE},
    {"DETACH", TOK_DETACH},
    {"SET", TOK_SET},
    {"REMOVE", TOK_REMOVE},
    {"MERGE", TOK_MERGE},
    {"YIELD", TOK_YIELD},
    {"CALL", TOK_CALL},
    {"ALL", TOK_ALL},
    {"TRUE", TOK_TRUE},
    {"FALSE", TOK_FALSE},
    {"EXISTS", TOK_EXISTS},
    {"MANDATORY", TOK_MANDATORY},
    {"FOREACH", TOK_FOREACH},
    {"ON", TOK_ON},
    {"ADD", TOK_ADD},
    {"CONSTRAINT", TOK_CONSTRAINT},
    {"DO", TOK_DO},
    {"DROP", TOK_DROP},
    {"FOR", TOK_FOR},
    {"FROM", TOK_FROM},
    {"GRAPH", TOK_GRAPH},
    {"OF", TOK_OF},
    {"REQUIRE", TOK_REQUIRE},
    {"SCALAR", TOK_SCALAR},
    {"UNIQUE", TOK_UNIQUE},
    {NULL, 0}};

static cbm_token_type_t keyword_lookup(const char *word) {
    /* Case-insensitive compare */
    for (const kw_entry_t *kw = keywords; kw->name; kw++) {
        if (strcasecmp(word, kw->name) == 0) {
            return kw->type;
        }
    }
    return TOK_IDENT;
}

/* Try to match a two-character token at position i. Returns true and advances i if matched. */
static bool lex_try_two_char(const char *input, int len, int *i, cbm_lex_result_t *out) {
    static const struct {
        char c1, c2;
        cbm_token_type_t type;
        const char *text;
    } pairs[] = {
        {'!', '=', TOK_NEQ, "!="}, {'<', '>', TOK_NEQ, "<>"}, {'=', '~', TOK_EQTILDE, "=~"},
        {'>', '=', TOK_GTE, ">="}, {'<', '=', TOK_LTE, "<="}, {'.', '.', TOK_DOTDOT, ".."},
    };
    char c = input[*i];
    if (*i + SKIP_ONE >= len) {
        return false;
    }
    char c2 = input[*i + SKIP_ONE];
    for (int p = 0; p < (int)(sizeof(pairs) / sizeof(pairs[0])); p++) {
        if (c == pairs[p].c1 && c2 == pairs[p].c2) {
            lex_push(out, pairs[p].type, pairs[p].text, *i);
            *i += PAIR_LEN;
            return true;
        }
    }
    return false;
}

/* Try to match a single-character token. Returns TOK_EOF if not matched. */
static cbm_token_type_t lex_single_char(char c) {
    switch (c) {
    case '(':
        return TOK_LPAREN;
    case ')':
        return TOK_RPAREN;
    case '[':
        return TOK_LBRACKET;
    case ']':
        return TOK_RBRACKET;
    case '-':
        return TOK_DASH;
    case '>':
        return TOK_GT;
    case '<':
        return TOK_LT;
    case ':':
        return TOK_COLON;
    case '.':
        return TOK_DOT;
    case '{':
        return TOK_LBRACE;
    case '}':
        return TOK_RBRACE;
    case '*':
        return TOK_STAR;
    case ',':
        return TOK_COMMA;
    case '=':
        return TOK_EQ;
    case '|':
        return TOK_PIPE;
    default:
        return TOK_EOF;
    }
}

/* Try to lex an identifier or keyword starting at position i. Returns true if matched. */
static bool lex_try_ident(const char *input, int len, int *i, cbm_lex_result_t *out) {
    char c = input[*i];
    if (!isalpha((unsigned char)c) && c != '_') {
        return false;
    }
    int start = *i;
    while (*i < len && (isalnum((unsigned char)input[*i]) || input[*i] == '_')) {
        (*i)++;
    }
    char word[CBM_SZ_256];
    int wlen = *i - start;
    if (wlen >= (int)sizeof(word)) {
        wlen = (int)sizeof(word) - SKIP_ONE;
    }
    memcpy(word, input + start, wlen);
    word[wlen] = '\0';
    cbm_token_type_t type = keyword_lookup(word);
    lex_push_n(out, type, input + start, *i - start, start);
    return true;
}

/* Try to lex a number starting at position i. Returns true if matched. */
static bool lex_try_number(const char *input, int len, int *i, cbm_lex_result_t *out) {
    char c = input[*i];
    if (!isdigit((unsigned char)c) &&
        !(c == '.' && *i + SKIP_ONE < len && isdigit((unsigned char)input[*i + SKIP_ONE]))) {
        return false;
    }
    int start = *i;
    while (*i < len && (isdigit((unsigned char)input[*i]) ||
                        (input[*i] == '.' && *i + SKIP_ONE < len && input[*i + SKIP_ONE] != '.'))) {
        (*i)++;
    }
    lex_push_n(out, TOK_NUMBER, input + start, *i - start, start);
    return true;
}

/* Skip whitespace and comments. Returns true if something was skipped. */
static bool lex_skip_whitespace_comments(const char *input, int len, int *i) {
    if (isspace((unsigned char)input[*i])) {
        (*i)++;
        return true;
    }
    if (*i + SKIP_ONE < len && input[*i] == '/' && input[*i + SKIP_ONE] == '/') {
        while (*i < len && input[*i] != '\n') {
            (*i)++;
        }
        return true;
    }
    /* SQL-style -- single-line comment */
    if (*i + SKIP_ONE < len && input[*i] == '-' && input[*i + SKIP_ONE] == '-') {
        while (*i < len && input[*i] != '\n') {
            (*i)++;
        }
        return true;
    }
    if (*i + SKIP_ONE < len && input[*i] == '/' && input[*i + SKIP_ONE] == '*') {
        *i += PAIR_LEN;
        while (*i + SKIP_ONE < len && !(input[*i] == '*' && input[*i + SKIP_ONE] == '/')) {
            (*i)++;
        }
        if (*i + SKIP_ONE < len) {
            *i += PAIR_LEN;
        }
        return true;
    }
    return false;
}

int cbm_lex(const char *input, cbm_lex_result_t *out) {
    memset(out, 0, sizeof(*out));
    if (!input) {
        return CBM_NOT_FOUND;
    }

    int len = (int)strlen(input);
    int i = 0;

    while (i < len) {
        if (lex_skip_whitespace_comments(input, len, &i)) {
            continue;
        }

        char c = input[i];

        /* String literals */
        if (c == '"' || c == '\'') {
            char quote = c;
            i++;
            lex_string_literal(input, len, &i, quote, out);
            continue;
        }

        /* Numbers — stop before ".." (DOTDOT operator) */
        if (lex_try_number(input, len, &i, out)) {
            continue;
        }

        /* Identifiers / keywords */
        if (lex_try_ident(input, len, &i, out)) {
            continue;
        }

        /* Two-character tokens */
        {
            bool found_two = lex_try_two_char(input, len, &i, out);
            if (found_two) {
                continue;
            }
        }

        /* Single-character tokens */
        cbm_token_type_t stype = lex_single_char(c);

        if (stype != TOK_EOF) {
            char buf[PAIR_LEN] = {c, '\0'};
            lex_push(out, stype, buf, i);
            i++;
            continue;
        }

        /* Unknown character — skip */
        i++;
    }

    /* Add EOF */
    lex_push(out, TOK_EOF, "", i);
    return 0;
}

void cbm_lex_free(cbm_lex_result_t *r) {
    if (!r) {
        return;
    }
    for (int i = 0; i < r->count; i++) {
        free((void *)r->tokens[i].text);
    }
    free(r->tokens);
    free(r->error);
    memset(r, 0, sizeof(*r));
}

/* ══════════════════════════════════════════════════════════════════
 *  PARSER
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    const cbm_token_t *tokens;
    int count;
    int pos;
    char error[CBM_SZ_512];
} parser_t;

static const cbm_token_t *peek(parser_t *p) {
    if (p->pos >= p->count) {
        return &p->tokens[p->count - SKIP_ONE]; /* EOF */
    }
    return &p->tokens[p->pos];
}

static const cbm_token_t *advance(parser_t *p) {
    if (p->pos >= p->count) {
        return &p->tokens[p->count - SKIP_ONE];
    }
    return &p->tokens[p->pos++];
}

static bool check(parser_t *p, cbm_token_type_t type) {
    return peek(p)->type == type;
}

static bool match(parser_t *p, cbm_token_type_t type) {
    if (check(p, type)) {
        advance(p);
        return true;
    }
    return false;
}

static const cbm_token_t *expect(parser_t *p, cbm_token_type_t type) {
    if (check(p, type)) {
        return advance(p);
    }
    snprintf(p->error, sizeof(p->error), "expected token type %d, got %d at pos %d", type,
             peek(p)->type, peek(p)->pos);
    return NULL;
}

/* Parse inline properties: {key: "value", ...} */
static int parse_props(parser_t *p, cbm_prop_filter_t **out, int *count) {
    *out = NULL;
    *count = 0;
    if (!match(p, TOK_LBRACE)) {
        return 0;
    }

    int cap = CYP_INIT_CAP4;
    int n = 0;
    cbm_prop_filter_t *arr = malloc(cap * sizeof(cbm_prop_filter_t));
    if (!arr) {
        return CBM_NOT_FOUND;
    }

    while (!check(p, TOK_RBRACE) && !check(p, TOK_EOF)) {
        const cbm_token_t *key = expect(p, TOK_IDENT);
        if (!key) {
            free(arr);
            return CBM_NOT_FOUND;
        }
        if (!expect(p, TOK_COLON)) {
            free(arr);
            return CBM_NOT_FOUND;
        }
        const cbm_token_t *val = expect(p, TOK_STRING);
        if (!val) {
            free(arr);
            return CBM_NOT_FOUND;
        }

        if (n >= cap) {
            int new_cap = cap * PAIR_LEN;
            void *tmp = realloc(arr, new_cap * sizeof(cbm_prop_filter_t));
            if (!tmp) {
                for (int i = 0; i < n; i++) {
                    free((void *)arr[i].key);
                    free((void *)arr[i].value);
                }
                free(arr);
                return CBM_NOT_FOUND;
            }
            arr = tmp;
            cap = new_cap;
        }
        arr[n].key = heap_strdup(key->text);
        arr[n].value = heap_strdup(val->text);
        n++;

        match(p, TOK_COMMA); /* optional comma */
    }
    expect(p, TOK_RBRACE);

    *out = arr;
    *count = n;
    return 0;
}

/* Parse node: (variable:Label {props}) */
static int parse_node(parser_t *p, cbm_node_pattern_t *out) {
    memset(out, 0, sizeof(*out));
    if (!expect(p, TOK_LPAREN)) {
        return CBM_NOT_FOUND;
    }

    /* Optional variable */
    if (check(p, TOK_IDENT)) {
        /* Lookahead: if next is COLON, this is a variable */
        /* Or if next is RPAREN/LBRACE, this is a variable without label */
        out->variable = heap_strdup(advance(p)->text);
    }

    /* Optional :Label */
    if (match(p, TOK_COLON)) {
        const cbm_token_t *label = expect(p, TOK_IDENT);
        if (!label) {
            return CBM_NOT_FOUND;
        }
        out->label = heap_strdup(label->text);
    }

    /* Optional {props} */
    if (check(p, TOK_LBRACE)) {
        if (parse_props(p, &out->props, &out->prop_count) < 0) {
            return CBM_NOT_FOUND;
        }
    }

    if (!expect(p, TOK_RPAREN)) {
        return CBM_NOT_FOUND;
    }
    return 0;
}

/* Parse *min..max hop range after the star token has been consumed */
static void parse_hop_range(parser_t *p, int *min_hops, int *max_hops) {
    if (check(p, TOK_NUMBER)) {
        int val = (int)strtol(peek(p)->text, NULL, CBM_DECIMAL_BASE);
        advance(p);
        if (match(p, TOK_DOTDOT)) {
            *min_hops = val;
            *max_hops =
                check(p, TOK_NUMBER) ? (int)strtol(advance(p)->text, NULL, CBM_DECIMAL_BASE) : 0;
        } else {
            /* *N means 1..N */
            *min_hops = SKIP_ONE;
            *max_hops = val;
        }
    } else if (match(p, TOK_DOTDOT)) {
        *min_hops = SKIP_ONE;
        *max_hops =
            check(p, TOK_NUMBER) ? (int)strtol(advance(p)->text, NULL, CBM_DECIMAL_BASE) : 0;
    } else {
        /* * alone = unbounded */
        *min_hops = SKIP_ONE;
        *max_hops = 0;
    }
}

/* Parse relationship type list after ':' inside brackets. Returns -1 on error. */
static int parse_rel_types(parser_t *p, cbm_rel_pattern_t *out) {
    int cap = CYP_INIT_CAP4;
    int n = 0;
    const char **types = malloc(cap * sizeof(const char *));
    if (!types) {
        return CBM_NOT_FOUND;
    }

    const cbm_token_t *t = expect(p, TOK_IDENT);
    if (!t) {
        free(types);
        return CBM_NOT_FOUND;
    }
    types[n++] = heap_strdup(t->text);

    while (match(p, TOK_PIPE)) {
        t = expect(p, TOK_IDENT);
        if (!t) {
            for (int i = 0; i < n; i++) {
                free((void *)types[i]);
            }
            free(types);
            return CBM_NOT_FOUND;
        }
        if (n >= cap) {
            int new_cap = cap * PAIR_LEN;
            void *tmp = realloc(types, new_cap * sizeof(const char *));
            if (!tmp) {
                for (int i = 0; i < n; i++) {
                    free((void *)types[i]);
                }
                free(types);
                return CBM_NOT_FOUND;
            }
            types = (const char **)tmp;
            cap = new_cap;
        }
        types[n++] = heap_strdup(t->text);
    }

    out->types = types;
    out->type_count = n;
    return 0;
}

/* Parse bracket content of a relationship: [var:TYPE*hops] */
static int parse_rel_bracket(parser_t *p, cbm_rel_pattern_t *out) {
    /* Optional variable */
    if (check(p, TOK_IDENT) && !check(p, TOK_COLON)) {
        out->variable = heap_strdup(advance(p)->text);
    }
    /* Optional :Types */
    if (match(p, TOK_COLON)) {
        if (parse_rel_types(p, out) < 0) {
            return CBM_NOT_FOUND;
        }
    }
    /* Optional *hop_range */
    if (match(p, TOK_STAR)) {
        parse_hop_range(p, &out->min_hops, &out->max_hops);
    }
    if (!expect(p, TOK_RBRACKET)) {
        return CBM_NOT_FOUND;
    }
    return 0;
}

/* Parse relationship: -[:TYPE|TYPE2*min..max]-> or <-[...]-  */
static int parse_rel(parser_t *p, cbm_rel_pattern_t *out) {
    memset(out, 0, sizeof(*out));
    out->min_hops = SKIP_ONE;
    out->max_hops = SKIP_ONE;

    /* Check for leading < (inbound) */
    bool leading_lt = match(p, TOK_LT);
    if (!expect(p, TOK_DASH)) {
        return CBM_NOT_FOUND;
    }

    /* Optional bracket content */
    if (match(p, TOK_LBRACKET)) {
        if (parse_rel_bracket(p, out) < 0) {
            return CBM_NOT_FOUND;
        }
    }

    if (!expect(p, TOK_DASH)) {
        return CBM_NOT_FOUND;
    }

    /* Check for trailing > (outbound) */
    bool trailing_gt = match(p, TOK_GT);

    /* Determine direction */
    if (leading_lt && !trailing_gt) {
        out->direction = heap_strdup("inbound");
    } else if (!leading_lt && trailing_gt) {
        out->direction = heap_strdup("outbound");
    } else {
        out->direction = heap_strdup("any");
    }

    return 0;
}

/* ── Expression tree helpers ────────────────────────────────────── */

static void expr_free(cbm_expr_t *e) {
    enum { EXPR_FREE_STACK = 128 };
    cbm_expr_t *stack[EXPR_FREE_STACK];
    int top = 0;
    if (e) {
        stack[top++] = e;
    }
    while (top > 0) {
        cbm_expr_t *cur = stack[--top];
        if (cur->type == EXPR_CONDITION) {
            free((void *)cur->cond.variable);
            free((void *)cur->cond.property);
            free((void *)cur->cond.op);
            free((void *)cur->cond.value);
            for (int i = 0; i < cur->cond.in_value_count; i++) {
                free((void *)cur->cond.in_values[i]);
            }
            free(cur->cond.in_values);
        }
        if (cur->right && top < EXPR_FREE_STACK) {
            stack[top++] = cur->right;
        }
        if (cur->left && top < EXPR_FREE_STACK) {
            stack[top++] = cur->left;
        }
        free(cur);
    }
}

static cbm_expr_t *expr_leaf(cbm_condition_t c) {
    cbm_expr_t *e = calloc(CBM_ALLOC_ONE, sizeof(cbm_expr_t));
    e->type = EXPR_CONDITION;
    e->cond = c;
    return e;
}

static cbm_expr_t *expr_binary(cbm_expr_type_t type, cbm_expr_t *left, cbm_expr_t *right) {
    cbm_expr_t *e = calloc(CBM_ALLOC_ONE, sizeof(cbm_expr_t));
    e->type = type;
    e->left = left;
    e->right = right;
    return e;
}

static cbm_expr_t *expr_not(cbm_expr_t *child) {
    cbm_expr_t *e = calloc(CBM_ALLOC_ONE, sizeof(cbm_expr_t));
    e->type = EXPR_NOT;
    e->left = child;
    return e;
}

/* ── Unsupported keyword detection ─────────────────────────────── */

static const char *unsupported_clause_error(cbm_token_type_t type) {
    switch (type) {
    case TOK_CREATE:
        return "unsupported Cypher feature: CREATE clause (write operations not supported)";
    case TOK_DELETE:
        return "unsupported Cypher feature: DELETE clause (write operations not supported)";
    case TOK_DETACH:
        return "unsupported Cypher feature: DETACH DELETE (write operations not supported)";
    case TOK_SET:
        return "unsupported Cypher feature: SET clause (write operations not supported)";
    case TOK_REMOVE:
        return "unsupported Cypher feature: REMOVE clause (write operations not supported)";
    case TOK_MERGE:
        return "unsupported Cypher feature: MERGE clause (write operations not supported)";
    case TOK_YIELD:
        return "unsupported Cypher feature: YIELD clause";
    case TOK_CALL:
        return "unsupported Cypher feature: CALL clause (stored procedures not supported)";
    case TOK_FOREACH:
        return "unsupported Cypher feature: FOREACH clause";
    case TOK_MANDATORY:
        return "unsupported Cypher feature: MANDATORY MATCH";
    case TOK_DROP:
        return "unsupported Cypher feature: DROP (schema operations not supported)";
    case TOK_CONSTRAINT:
        return "unsupported Cypher feature: CONSTRAINT (schema operations not supported)";
    default:
        return NULL;
    }
}

/* ── Recursive descent WHERE parser (Phase 2) ──────────────────── */

/* Forward declarations for recursive descent */
static cbm_expr_t *parse_or_expr(parser_t *p);

/* Parse IN [val, val, ...] list. Returns expr_leaf or NULL on error. */
static cbm_expr_t *parse_in_list(parser_t *p, cbm_condition_t *c) {
    advance(p);
    c->op = heap_strdup("IN");
    if (!expect(p, TOK_LBRACKET)) {
        free((void *)c->variable);
        free((void *)c->property);
        free((void *)c->op);
        return NULL;
    }
    int vcap = CYP_INIT_CAP8;
    int vn = 0;
    const char **vals = malloc(vcap * sizeof(const char *));
    if (!vals) {
        free((void *)c->variable);
        free((void *)c->property);
        free((void *)c->op);
        return NULL;
    }
    while (!check(p, TOK_RBRACKET) && !check(p, TOK_EOF)) {
        if (vn > 0) {
            match(p, TOK_COMMA);
        }
        if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
            if (vn >= vcap) {
                int new_vcap = vcap * PAIR_LEN;
                void *tmp = realloc((void *)vals, new_vcap * sizeof(const char *));
                if (!tmp) {
                    for (int i = 0; i < vn; i++) {
                        free((void *)vals[i]);
                    }
                    free((void *)vals);
                    free((void *)c->variable);
                    free((void *)c->property);
                    free((void *)c->op);
                    return NULL;
                }
                vals = (const char **)tmp;
                vcap = new_vcap;
            }
            vals[vn++] = heap_strdup(advance(p)->text);
        } else {
            break;
        }
    }
    expect(p, TOK_RBRACKET);
    c->in_values = vals;
    c->in_value_count = vn;
    return expr_leaf(*c);
}

/* Try to parse a comparison operator. Returns heap-allocated op string or NULL. */
static char *parse_comparison_op(parser_t *p) {
    if (match(p, TOK_EQ)) {
        return heap_strdup("=");
    }
    if (match(p, TOK_NEQ)) {
        return heap_strdup("<>");
    }
    if (match(p, TOK_EQTILDE)) {
        return heap_strdup("=~");
    }
    if (match(p, TOK_GTE)) {
        return heap_strdup(">=");
    }
    if (match(p, TOK_LTE)) {
        return heap_strdup("<=");
    }
    if (match(p, TOK_GT)) {
        return heap_strdup(">");
    }
    if (match(p, TOK_LT)) {
        return heap_strdup("<");
    }
    if (check(p, TOK_CONTAINS)) {
        advance(p);
        return heap_strdup("CONTAINS");
    }
    if (check(p, TOK_STARTS)) {
        advance(p);
        expect(p, TOK_WITH);
        return heap_strdup("STARTS WITH");
    }
    if (check(p, TOK_ENDS)) {
        advance(p);
        expect(p, TOK_WITH);
        return heap_strdup("ENDS WITH");
    }
    return NULL;
}

/* Parse a single condition: var.prop OP value | var.prop IS [NOT] NULL | var.prop IN [...] */
static cbm_expr_t *parse_condition_expr(parser_t *p) {
    /* Check for NOT prefix at condition level (e.g. NOT n.name CONTAINS "x") */
    bool negated = match(p, TOK_NOT);

    const cbm_token_t *var = expect(p, TOK_IDENT);
    if (!var) {
        return NULL;
    }

    cbm_condition_t c = {0};
    c.negated = negated;

    if (match(p, TOK_DOT)) {
        const cbm_token_t *prop = expect(p, TOK_IDENT);
        if (!prop) {
            return NULL;
        }
        c.variable = heap_strdup(var->text);
        c.property = heap_strdup(prop->text);
    } else {
        /* No dot: bare alias (e.g. post-WITH variable like "cnt") */
        c.variable = heap_strdup(var->text);
        c.property = NULL;
    }

    /* IS NULL / IS NOT NULL */
    if (check(p, TOK_IS)) {
        advance(p);
        if (match(p, TOK_NOT)) {
            c.op = heap_strdup("IS NOT NULL");
            expect(p, TOK_NULL_KW);
        } else {
            expect(p, TOK_NULL_KW);
            c.op = heap_strdup("IS NULL");
        }
        return expr_leaf(c);
    }

    /* IN [...] */
    if (check(p, TOK_IN)) {
        return parse_in_list(p, &c);
    }

    /* Standard operators */
    c.op = parse_comparison_op(p);
    if (!c.op) {
        snprintf(p->error, sizeof(p->error), "unexpected operator at pos %d", peek(p)->pos);
        free((void *)c.variable);
        free((void *)c.property);
        return NULL;
    }

    /* Value */
    if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
        c.value = heap_strdup(advance(p)->text);
    } else if (check(p, TOK_TRUE)) {
        advance(p);
        c.value = heap_strdup("true");
    } else if (check(p, TOK_FALSE)) {
        advance(p);
        c.value = heap_strdup("false");
    } else {
        snprintf(p->error, sizeof(p->error), "expected value at pos %d", peek(p)->pos);
        free((void *)c.variable);
        free((void *)c.property);
        free((void *)c.op);
        return NULL;
    }

    return expr_leaf(c);
}

/* Atom: ( expr ) | condition */
static cbm_expr_t *parse_atom_expr(parser_t *p) { // NOLINT(misc-no-recursion)
    if (match(p, TOK_LPAREN)) {
        cbm_expr_t *e = parse_or_expr(p);
        expect(p, TOK_RPAREN);
        return e;
    }
    return parse_condition_expr(p);
}

/* NOT: NOT atom | atom */
static cbm_expr_t *parse_not_expr(parser_t *p) { // NOLINT(misc-no-recursion)
    if (match(p, TOK_NOT)) {
        cbm_expr_t *child = parse_not_expr(p);
        return child ? expr_not(child) : NULL;
    }
    return parse_atom_expr(p);
}

/* AND: not (AND not)* */
static cbm_expr_t *parse_and_expr(parser_t *p) { // NOLINT(misc-no-recursion)
    cbm_expr_t *left = parse_not_expr(p);
    if (!left) {
        return NULL;
    }
    while (check(p, TOK_AND)) {
        advance(p);
        cbm_expr_t *right = parse_not_expr(p);
        if (!right) {
            expr_free(left);
            return NULL;
        }
        left = expr_binary(EXPR_AND, left, right);
    }
    return left;
}

/* XOR: and (XOR and)* */
static cbm_expr_t *parse_xor_expr(parser_t *p) { // NOLINT(misc-no-recursion)
    cbm_expr_t *left = parse_and_expr(p);
    if (!left) {
        return NULL;
    }
    while (check(p, TOK_XOR)) {
        advance(p);
        cbm_expr_t *right = parse_and_expr(p);
        if (!right) {
            expr_free(left);
            return NULL;
        }
        left = expr_binary(EXPR_XOR, left, right);
    }
    return left;
}

/* OR: xor (OR xor)* */
static cbm_expr_t *parse_or_expr(parser_t *p) { // NOLINT(misc-no-recursion)
    cbm_expr_t *left = parse_xor_expr(p);
    if (!left) {
        return NULL;
    }
    while (check(p, TOK_OR)) {
        advance(p);
        cbm_expr_t *right = parse_xor_expr(p);
        if (!right) {
            expr_free(left);
            return NULL;
        }
        left = expr_binary(EXPR_OR, left, right);
    }
    return left;
}

/* Parse WHERE clause — builds expression tree */
static int parse_where(parser_t *p, cbm_where_clause_t **out) {
    if (!match(p, TOK_WHERE)) {
        *out = NULL;
        return 0;
    }

    cbm_where_clause_t *w = calloc(CBM_ALLOC_ONE, sizeof(cbm_where_clause_t));
    w->root = parse_or_expr(p);
    if (!w->root && p->error[0]) {
        free(w);
        return CBM_NOT_FOUND;
    }

    *out = w;
    return 0;
}

/* Helper: is token an aggregate function? */
static bool is_aggregate_tok(cbm_token_type_t t) {
    return (t == TOK_COUNT || t == TOK_SUM || t == TOK_AVG || t == TOK_MIN_KW || t == TOK_MAX_KW ||
            t == TOK_COLLECT) != 0;
}

/* Helper: is token a string function? */
static bool is_string_func_tok(cbm_token_type_t t) {
    return (t == TOK_TOLOWER || t == TOK_TOUPPER || t == TOK_TOSTRING) != 0;
}

/* Token type to function name */
static const char *agg_func_name(cbm_token_type_t t) {
    switch (t) {
    case TOK_COUNT:
        return "COUNT";
    case TOK_SUM:
        return "SUM";
    case TOK_AVG:
        return "AVG";
    case TOK_MIN_KW:
        return "MIN";
    case TOK_MAX_KW:
        return "MAX";
    case TOK_COLLECT:
        return "COLLECT";
    default:
        return "COUNT";
    }
}

static const char *str_func_name(cbm_token_type_t t) {
    switch (t) {
    case TOK_TOLOWER:
        return "toLower";
    case TOK_TOUPPER:
        return "toUpper";
    case TOK_TOSTRING:
        return "toString";
    default:
        return "";
    }
}

/* Parse a value literal: string, number, ident[.prop], true, false. Returns heap-allocated. */
static const char *parse_value_literal(parser_t *p) {
    if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
        return heap_strdup(advance(p)->text);
    }
    if (check(p, TOK_IDENT)) {
        char buf[CBM_SZ_256];
        const cbm_token_t *v = advance(p);
        if (match(p, TOK_DOT)) {
            const cbm_token_t *pr = expect(p, TOK_IDENT);
            snprintf(buf, sizeof(buf), "%s.%s", v->text, pr ? pr->text : "");
        } else {
            snprintf(buf, sizeof(buf), "%s", v->text);
        }
        return heap_strdup(buf);
    }
    if (check(p, TOK_TRUE)) {
        advance(p);
        return heap_strdup("true");
    }
    if (check(p, TOK_FALSE)) {
        advance(p);
        return heap_strdup("false");
    }
    return NULL;
}

/* Parse CASE WHEN ... THEN ... [ELSE ...] END */
static cbm_case_expr_t *parse_case_expr(parser_t *p) {
    /* CASE already consumed */
    cbm_case_expr_t *kase = calloc(CBM_ALLOC_ONE, sizeof(cbm_case_expr_t));
    if (!kase) {
        return NULL;
    }
    int bcap = CYP_INIT_CAP4;
    kase->branches = malloc(bcap * sizeof(cbm_case_branch_t));
    if (!kase->branches) {
        free(kase);
        return NULL;
    }

    while (check(p, TOK_WHEN)) {
        advance(p);
        cbm_expr_t *when = parse_or_expr(p);
        if (!expect(p, TOK_THEN)) {
            expr_free(when);
            break;
        }
        const char *then_val = parse_value_literal(p);
        if (kase->branch_count >= bcap) {
            int new_bcap = bcap * PAIR_LEN;
            void *tmp = realloc(kase->branches, new_bcap * sizeof(cbm_case_branch_t));
            if (!tmp) {
                expr_free(when);
                for (int i = 0; i < kase->branch_count; i++) {
                    expr_free(kase->branches[i].when_expr);
                }
                free(kase->branches);
                free(kase);
                return NULL;
            }
            kase->branches = tmp;
            bcap = new_bcap;
        }
        kase->branches[kase->branch_count++] =
            (cbm_case_branch_t){.when_expr = when, .then_val = then_val};
    }

    if (match(p, TOK_ELSE)) {
        kase->else_val = parse_value_literal(p);
    }
    expect(p, TOK_END);
    return kase;
}

/* Parse a single RETURN/WITH item (aggregate, string func, CASE, or plain var.prop).
 * Returns 0 on success, -1 on error. */
/* Parse var[.prop] into item->variable and item->property. Returns -1 on error. */
static int parse_var_dot_prop(parser_t *p, cbm_return_item_t *item) {
    const cbm_token_t *var = expect(p, TOK_IDENT);
    if (!var) {
        return CBM_NOT_FOUND;
    }
    item->variable = heap_strdup(var->text);
    if (match(p, TOK_DOT)) {
        const cbm_token_t *prop = expect(p, TOK_IDENT);
        if (prop) {
            item->property = heap_strdup(prop->text);
        }
    }
    return 0;
}

/* Parse aggregate function call: COUNT(var.prop) */
static int parse_aggregate_item(parser_t *p, cbm_return_item_t *item) {
    cbm_token_type_t ft = peek(p)->type;
    advance(p);
    expect(p, TOK_LPAREN);
    if (match(p, TOK_STAR)) {
        item->variable = heap_strdup("*");
    } else {
        if (parse_var_dot_prop(p, item) < 0) {
            return CBM_NOT_FOUND;
        }
    }
    expect(p, TOK_RPAREN);
    item->func = heap_strdup(agg_func_name(ft));
    return 0;
}

/* Parse string function call: toLower(var.prop) */
static int parse_string_func_item(parser_t *p, cbm_return_item_t *item) {
    cbm_token_type_t ft = peek(p)->type;
    advance(p);
    expect(p, TOK_LPAREN);
    if (parse_var_dot_prop(p, item) < 0) {
        return CBM_NOT_FOUND;
    }
    expect(p, TOK_RPAREN);
    item->func = heap_strdup(str_func_name(ft));
    return 0;
}

static int parse_return_item(parser_t *p, cbm_return_item_t *item) {
    memset(item, 0, sizeof(*item));
    int rc = 0;
    if (check(p, TOK_CASE)) {
        advance(p);
        item->kase = parse_case_expr(p);
        item->variable = heap_strdup("CASE");
    } else if (is_aggregate_tok(peek(p)->type)) {
        rc = parse_aggregate_item(p, item);
    } else if (is_string_func_tok(peek(p)->type)) {
        rc = parse_string_func_item(p, item);
    } else {
        rc = parse_var_dot_prop(p, item);
    }
    if (rc < 0) {
        return CBM_NOT_FOUND;
    }
    /* Optional AS alias */
    if (match(p, TOK_AS)) {
        const cbm_token_t *alias = expect(p, TOK_IDENT);
        if (alias) {
            item->alias = heap_strdup(alias->text);
        }
    }
    return 0;
}

/* Parse ORDER BY field into r->order_by and r->order_dir */
/* Parse aggregate function call for ORDER BY */
static void parse_order_by_agg(parser_t *p, char *buf, size_t buf_sz) {
    const char *fn = agg_func_name(peek(p)->type);
    advance(p);
    expect(p, TOK_LPAREN);
    if (match(p, TOK_STAR)) {
        snprintf(buf, buf_sz, "%s(*)", fn);
    } else {
        const cbm_token_t *var = expect(p, TOK_IDENT);
        snprintf(buf, buf_sz, "%s(%s)", fn, var ? var->text : "");
    }
    expect(p, TOK_RPAREN);
}

/* Parse var[.prop] for ORDER BY */
static void parse_order_by_var(parser_t *p, char *buf, size_t buf_sz) {
    const cbm_token_t *var = expect(p, TOK_IDENT);
    if (!var) {
        return;
    }
    snprintf(buf, buf_sz, "%s", var->text);
    if (match(p, TOK_DOT)) {
        const cbm_token_t *prop = expect(p, TOK_IDENT);
        if (prop) {
            snprintf(buf, buf_sz, "%s.%s", var->text, prop->text);
        }
    }
}

/* Parse ORDER BY expression into buf. Returns buf. */
static char *parse_order_by_expr(parser_t *p, char *buf, size_t buf_sz) {
    buf[0] = '\0';
    if (is_aggregate_tok(peek(p)->type)) {
        parse_order_by_agg(p, buf, buf_sz);
    } else {
        parse_order_by_var(p, buf, buf_sz);
    }
    return buf;
}

static void parse_order_by_clause(parser_t *p, cbm_return_clause_t *r) {
    expect(p, TOK_BY);
    char order_buf[CBM_SZ_256];
    parse_order_by_expr(p, order_buf, sizeof(order_buf));
    r->order_by = heap_strdup(order_buf);
    if (match(p, TOK_ASC)) {
        r->order_dir = heap_strdup("ASC");
    } else if (match(p, TOK_DESC)) {
        r->order_dir = heap_strdup("DESC");
    }
}

/* Parse RETURN/WITH clause (shared logic) */
static int parse_return_or_with(parser_t *p, cbm_return_clause_t **out, bool is_with) {
    cbm_token_type_t tok = (int)is_with ? TOK_WITH : TOK_RETURN;
    /* For WITH, we need to check it's standalone (not preceded by STARTS) */
    if (!match(p, tok)) {
        *out = NULL;
        return 0;
    }

    cbm_return_clause_t *r = calloc(CBM_ALLOC_ONE, sizeof(cbm_return_clause_t));
    int cap = CYP_INIT_CAP8;
    r->items = malloc(cap * sizeof(cbm_return_item_t));

    r->distinct = match(p, TOK_DISTINCT);

    /* Check for RETURN * */
    if (!is_with && match(p, TOK_STAR)) {
        r->star = true;
        /* Skip to ORDER BY / SKIP / LIMIT */
        goto tail;
    }

    do {
        if (r->count > 0 && !match(p, TOK_COMMA)) {
            break;
        }

        cbm_return_item_t item = {0};
        if (parse_return_item(p, &item) < 0) {
            free(r->items);
            free(r);
            return CBM_NOT_FOUND;
        }

        if (r->count >= cap) {
            cap *= PAIR_LEN;
            r->items = safe_realloc(r->items, cap * sizeof(cbm_return_item_t));
        }
        r->items[r->count++] = item;

    } while (check(p, TOK_COMMA));

tail:
    /* Optional ORDER BY */
    if (match(p, TOK_ORDER)) {
        parse_order_by_clause(p, r);
    }

    /* Optional SKIP */
    if (match(p, TOK_SKIP)) {
        const cbm_token_t *num = expect(p, TOK_NUMBER);
        if (num) {
            r->skip = (int)strtol(num->text, NULL, CBM_DECIMAL_BASE);
        }
    }

    /* Optional LIMIT */
    if (match(p, TOK_LIMIT)) {
        const cbm_token_t *num = expect(p, TOK_NUMBER);
        if (num) {
            r->limit = (int)strtol(num->text, NULL, CBM_DECIMAL_BASE);
        }
    }

    *out = r;
    return 0;
}

/* Parse RETURN clause */
static int parse_return(parser_t *p, cbm_return_clause_t **out) {
    return parse_return_or_with(p, out, false);
}

/* Parse a single MATCH pattern into pat */
static int parse_match_pattern(parser_t *p, cbm_pattern_t *pat) {
    memset(pat, 0, sizeof(*pat));
    int node_cap = CYP_INIT_CAP4;
    int rel_cap = CYP_INIT_CAP4;
    pat->nodes = malloc(node_cap * sizeof(cbm_node_pattern_t));
    pat->rels = calloc(rel_cap, sizeof(cbm_rel_pattern_t));

    if (parse_node(p, &pat->nodes[0]) < 0) {
        return CBM_NOT_FOUND;
    }
    pat->node_count = SKIP_ONE;

    while (check(p, TOK_DASH) || check(p, TOK_LT)) {
        if (pat->rel_count >= rel_cap) {
            rel_cap *= PAIR_LEN;
            pat->rels = safe_realloc(pat->rels, rel_cap * sizeof(cbm_rel_pattern_t));
        }
        if (parse_rel(p, &pat->rels[pat->rel_count]) < 0) {
            return CBM_NOT_FOUND;
        }
        pat->rel_count++;

        if (pat->node_count >= node_cap) {
            node_cap *= PAIR_LEN;
            pat->nodes = safe_realloc(pat->nodes, node_cap * sizeof(cbm_node_pattern_t));
        }
        if (parse_node(p, &pat->nodes[pat->node_count]) < 0) {
            return CBM_NOT_FOUND;
        }
        pat->node_count++;
    }
    return 0;
}

/* Parse UNWIND [...] AS var clause into query */
static void parse_unwind_clause(parser_t *p, cbm_query_t *q) {
    advance(p);
    if (check(p, TOK_LBRACKET)) {
        /* Literal list: [1, 2, 3] — collect as JSON array string */
        advance(p);
        char buf[CBM_SZ_2K] = "[";
        int blen = SKIP_ONE;
        while (!check(p, TOK_RBRACKET) && !check(p, TOK_EOF)) {
            if (blen > SKIP_ONE) {
                buf[blen++] = ',';
            }
            if (check(p, TOK_STRING)) {
                blen += snprintf(buf + blen, sizeof(buf) - blen, "\"%s\"", peek(p)->text);
                advance(p);
            } else if (check(p, TOK_NUMBER)) {
                blen += snprintf(buf + blen, sizeof(buf) - blen, "%s", peek(p)->text);
                advance(p);
            } else {
                advance(p);
            }
            match(p, TOK_COMMA);
        }
        expect(p, TOK_RBRACKET);
        buf[blen++] = ']';
        buf[blen] = '\0';
        q->unwind_expr = heap_strdup(buf);
    } else if (check(p, TOK_IDENT)) {
        q->unwind_expr = heap_strdup(advance(p)->text);
    }
    expect(p, TOK_AS);
    const cbm_token_t *alias = expect(p, TOK_IDENT);
    if (alias) {
        q->unwind_alias = heap_strdup(alias->text);
    }
}

/* Parse a chain of MATCH / OPTIONAL MATCH patterns into query.
 * Returns -1 on error (fills p->error). */
static int parse_match_chain(parser_t *p, cbm_query_t *q, int *pat_cap) {
    while (check(p, TOK_MATCH) || check(p, TOK_OPTIONAL)) {
        bool opt = false;
        if (check(p, TOK_OPTIONAL)) {
            advance(p);
            opt = true;
        }
        if (!expect(p, TOK_MATCH)) {
            break;
        }
        if (q->pattern_count >= *pat_cap) {
            *pat_cap *= PAIR_LEN;
            q->patterns = safe_realloc(q->patterns, *pat_cap * sizeof(cbm_pattern_t));
            q->pattern_optional = safe_realloc(q->pattern_optional, *pat_cap * sizeof(bool));
        }
        if (parse_match_pattern(p, &q->patterns[q->pattern_count]) < 0) {
            return CBM_NOT_FOUND;
        }
        q->pattern_optional[q->pattern_count] = opt;
        q->pattern_count++;
    }
    return 0;
}

/* Parse post-WHERE clauses: additional MATCH, WITH, RETURN, UNION */
static int parse_post_where(parser_t *p, cbm_query_t *q, // NOLINT(misc-no-recursion)
                            int *pat_cap) {
    /* More MATCH / OPTIONAL MATCH after WHERE */
    if (parse_match_chain(p, q, pat_cap) < 0) {
        return CBM_NOT_FOUND;
    }
    /* Check for unsupported keywords */
    const char *unsup = unsupported_clause_error(peek(p)->type);
    if (unsup) {
        snprintf(p->error, sizeof(p->error), "%s", unsup);
        return CBM_NOT_FOUND;
    }
    /* Optional WITH clause (standalone, not STARTS WITH) */
    if (check(p, TOK_WITH) &&
        (p->pos < PAIR_LEN || p->tokens[p->pos - SKIP_ONE].type != TOK_STARTS)) {
        if (parse_return_or_with(p, &q->with_clause, true) < 0) {
            return CBM_NOT_FOUND;
        }
        if (parse_where(p, &q->post_with_where) < 0) {
            return CBM_NOT_FOUND;
        }
    }
    /* Optional RETURN */
    if (parse_return(p, &q->ret) < 0) {
        return CBM_NOT_FOUND;
    }
    /* UNION [ALL] */
    if (check(p, TOK_UNION)) {
        advance(p);
        q->union_all = match(p, TOK_ALL);
        cbm_parse_result_t sub = {0};
        if (cbm_parse(&p->tokens[p->pos], p->count - p->pos, &sub) < 0) {
            if (sub.error) {
                snprintf(p->error, sizeof(p->error), "%s", sub.error);
            }
            cbm_parse_free(&sub);
            return CBM_NOT_FOUND;
        }
        q->union_next = sub.query;
        sub.query = NULL;
        cbm_parse_free(&sub);
    }
    return 0;
}

int cbm_parse(const cbm_token_t *tokens, int token_count, // NOLINT(misc-no-recursion)
              cbm_parse_result_t *out) {
    memset(out, 0, sizeof(*out));
    parser_t p = {.tokens = tokens, .count = token_count, .pos = 0};

    /* Check for unsupported leading keywords */
    const char *unsup = unsupported_clause_error(peek(&p)->type);
    if (unsup) {
        out->error = heap_strdup(unsup);
        return CBM_NOT_FOUND;
    }

    cbm_query_t *q = calloc(CBM_ALLOC_ONE, sizeof(cbm_query_t));

    if (check(&p, TOK_UNWIND)) {
        parse_unwind_clause(&p, q);
    }

    bool first_optional = false;
    if (check(&p, TOK_OPTIONAL)) {
        advance(&p);
        first_optional = true;
    }
    if (!expect(&p, TOK_MATCH)) {
        out->error = heap_strdup(p.error[0] ? p.error : "expected MATCH");
        cbm_query_free(q);
        return CBM_NOT_FOUND;
    }

    int pat_cap = CYP_INIT_CAP4;
    q->patterns = malloc(pat_cap * sizeof(cbm_pattern_t));
    q->pattern_optional = malloc(pat_cap * sizeof(bool));

    if (parse_match_pattern(&p, &q->patterns[0]) < 0) {
        out->error = heap_strdup(p.error[0] ? p.error : "failed to parse pattern");
        cbm_query_free(q);
        return CBM_NOT_FOUND;
    }
    q->pattern_optional[0] = first_optional;
    q->pattern_count = SKIP_ONE;

    if (parse_match_chain(&p, q, &pat_cap) < 0) {
        out->error = heap_strdup(p.error[0] ? p.error : "failed to parse additional pattern");
        cbm_query_free(q);
        return CBM_NOT_FOUND;
    }

    if (parse_where(&p, &q->where) < 0) {
        out->error = heap_strdup(p.error[0] ? p.error : "failed to parse WHERE");
        cbm_query_free(q);
        return CBM_NOT_FOUND;
    }

    if (parse_post_where(&p, q, &pat_cap) < 0) {
        out->error = heap_strdup(p.error[0] ? p.error : "failed to parse query");
        cbm_query_free(q);
        return CBM_NOT_FOUND;
    }

    out->query = q;
    return 0;
}

void cbm_parse_free(cbm_parse_result_t *r) {
    if (!r) {
        return;
    }
    cbm_query_free(r->query);
    free(r->error);
    memset(r, 0, sizeof(*r));
}

/* ── Query free ─────────────────────────────────────────────────── */

static void free_pattern(cbm_pattern_t *pat) {
    for (int i = 0; i < pat->node_count; i++) {
        cbm_node_pattern_t *n = &pat->nodes[i];
        free((void *)n->variable);
        free((void *)n->label);
        for (int j = 0; j < n->prop_count; j++) {
            free((void *)n->props[j].key);
            free((void *)n->props[j].value);
        }
        free(n->props);
    }
    free(pat->nodes);
    for (int i = 0; i < pat->rel_count; i++) {
        cbm_rel_pattern_t *r = &pat->rels[i];
        free((void *)r->variable);
        for (int j = 0; j < r->type_count; j++) {
            free((void *)r->types[j]);
        }
        free(r->types);
        free((void *)r->direction);
    }
    free(pat->rels);
}

static void free_where(cbm_where_clause_t *w) {
    if (!w) {
        return;
    }
    expr_free(w->root);
    for (int i = 0; i < w->count; i++) {
        free((void *)w->conditions[i].variable);
        free((void *)w->conditions[i].property);
        free((void *)w->conditions[i].op);
        free((void *)w->conditions[i].value);
        for (int j = 0; j < w->conditions[i].in_value_count; j++) {
            free((void *)w->conditions[i].in_values[j]);
        }
        free(w->conditions[i].in_values);
    }
    free(w->conditions);
    free((void *)w->op);
    free(w);
}

static void free_case_expr(cbm_case_expr_t *k) {
    if (!k) {
        return;
    }
    for (int i = 0; i < k->branch_count; i++) {
        expr_free(k->branches[i].when_expr);
        free((void *)k->branches[i].then_val);
    }
    free(k->branches);
    free((void *)k->else_val);
    free(k);
}

static void free_return_clause(cbm_return_clause_t *r) {
    if (!r) {
        return;
    }
    for (int i = 0; i < r->count; i++) {
        free((void *)r->items[i].variable);
        free((void *)r->items[i].property);
        free((void *)r->items[i].alias);
        free((void *)r->items[i].func);
        free_case_expr(r->items[i].kase);
    }
    free(r->items);
    free((void *)r->order_by);
    free((void *)r->order_dir);
    free(r);
}

void cbm_query_free(cbm_query_t *q) {
    while (q) {
        cbm_query_t *next = q->union_next;
        for (int i = 0; i < q->pattern_count; i++) {
            free_pattern(&q->patterns[i]);
        }
        free(q->patterns);
        free(q->pattern_optional);
        free_where(q->where);
        free_where(q->post_with_where);
        free_return_clause(q->with_clause);
        free_return_clause(q->ret);
        free((void *)q->unwind_expr);
        free((void *)q->unwind_alias);
        free(q);
        q = next;
    }
}

/* ── Convenience: lex + parse ───────────────────────────────────── */

int cbm_cypher_parse(const char *query, cbm_query_t **out, char **error) {
    *out = NULL;
    *error = NULL;

    cbm_lex_result_t lr = {0};
    if (cbm_lex(query, &lr) < 0 || lr.error) {
        *error = heap_strdup(lr.error ? lr.error : "lex error");
        cbm_lex_free(&lr);
        return CBM_NOT_FOUND;
    }

    cbm_parse_result_t pr = {0};
    if (cbm_parse(lr.tokens, lr.count, &pr) < 0) {
        *error = heap_strdup(pr.error ? pr.error : "parse error");
        cbm_parse_free(&pr);
        cbm_lex_free(&lr);
        return CBM_NOT_FOUND;
    }

    *out = pr.query;
    pr.query = NULL;
    cbm_parse_free(&pr);
    cbm_lex_free(&lr);
    return 0;
}

/* ══════════════════════════════════════════════════════════════════
 *  EXECUTOR
 * ══════════════════════════════════════════════════════════════════ */

/* A binding: maps variable names to nodes and/or edges */
typedef struct {
    const char *var_names[CYP_MAX_VARS]; /* variable names (nodes) */
    cbm_node_t var_nodes[CYP_MAX_VARS];  /* node data */
    int var_count;
    const char *edge_var_names[CYP_MAX_EDGE_VARS]; /* variable names (edges) */
    cbm_edge_t edge_vars[CYP_MAX_EDGE_VARS];       /* edge data */
    int edge_var_count;
    cbm_store_t *store; /* for computing in_degree/out_degree on demand */
} binding_t;

/* Return a string field from a node by property name.  NULL-safe. */
static const char *node_string_field(const cbm_node_t *n, const char *prop) {
    static const struct {
        const char *key;
        size_t offset;
    } fields[] = {
        {"name", offsetof(cbm_node_t, name)},
        {"qualified_name", offsetof(cbm_node_t, qualified_name)},
        {"label", offsetof(cbm_node_t, label)},
        {"file_path", offsetof(cbm_node_t, file_path)},
    };
    for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); i++) {
        if (strcmp(prop, fields[i].key) == 0) {
            const char *val = *(const char **)((const char *)n + fields[i].offset);
            return val ? val : "";
        }
    }
    return NULL;
}

/* Get node property by name.
 * store may be NULL; only needed for virtual degree properties. */
static const char *node_prop(const cbm_node_t *n, const char *prop, cbm_store_t *store) {
    if (!n || !prop) {
        return "";
    }
    const char *str = node_string_field(n, prop);
    if (str) {
        return str;
    }
    /* Integer properties returned as strings. */
    static _Thread_local char int_buf[CBM_SZ_32];
    if (strcmp(prop, "start_line") == 0) {
        snprintf(int_buf, sizeof(int_buf), "%d", n->start_line);
        return int_buf;
    }
    if (strcmp(prop, "end_line") == 0) {
        snprintf(int_buf, sizeof(int_buf), "%d", n->end_line);
        return int_buf;
    }
    /* Virtual computed properties: in_degree/out_degree via CALLS edges.
     * Enables Cypher dead-code detection: WHERE n.in_degree = '0'. */
    if (store && (strcmp(prop, "in_degree") == 0 || strcmp(prop, "out_degree") == 0)) {
        int in_deg = 0;
        int out_deg = 0;
        cbm_store_node_degree(store, n->id, &in_deg, &out_deg);
        int val = (strcmp(prop, "in_degree") == 0) ? in_deg : out_deg;
        snprintf(int_buf, sizeof(int_buf), "%d", val);
        return int_buf;
    }
    return "";
}

/* Extract a string value from JSON properties_json by key.
 * Writes result to buf (up to buf_sz). Returns buf if found, "" otherwise.
 * Handles both string values ("key":"value") and numeric values ("key":1.5). */
static const char *json_extract_prop(const char *json, const char *key, char *buf, size_t buf_sz) {
    if (!json || !key) {
        buf[0] = '\0';
        return buf;
    }
    /* Build search pattern: "key": */
    char pattern[CBM_SZ_256];
    snprintf(pattern, sizeof(pattern), "\"%s\":", key);
    const char *p = strstr(json, pattern);
    if (!p) {
        buf[0] = '\0';
        return buf;
    }
    p += strlen(pattern);
    /* Skip whitespace */
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    if (*p == '"') {
        /* String value */
        p++;
        size_t i = 0;
        while (*p && *p != '"' && i < buf_sz - SKIP_ONE) {
            buf[i++] = *p++;
        }
        buf[i] = '\0';
    } else {
        /* Numeric or other value */
        size_t i = 0;
        while (*p && *p != ',' && *p != '}' && *p != ' ' && i < buf_sz - SKIP_ONE) {
            buf[i++] = *p++;
        }
        buf[i] = '\0';
    }
    return buf;
}

/* Get edge property by name. Uses rotating static buffers to allow
 * multiple concurrent calls (e.g. projecting r.url_path, r.confidence
 * in the same row). */
static const char *edge_prop(const cbm_edge_t *e, const char *prop) {
    if (!e || !prop) {
        return "";
    }
    if (strcmp(prop, "type") == 0) {
        return e->type ? e->type : "";
    }
    /* Rotate through 8 static buffers so multiple props can be accessed per row */
    static char ebufs[CYP_BUF_8][CBM_SZ_512];
    static int ebuf_idx = 0;
    char *buf = ebufs[ebuf_idx++ & CYP_EBUF_MASK];
    json_extract_prop(e->properties_json, prop, buf, CBM_SZ_512);
    return buf;
}

/* Find an edge variable in a binding */
static cbm_edge_t *binding_get_edge(binding_t *b, const char *var) {
    for (int i = 0; i < b->edge_var_count; i++) {
        if (strcmp(b->edge_var_names[i], var) == 0) {
            return &b->edge_vars[i];
        }
    }
    return NULL;
}

/* Find a variable's node in a binding */
static cbm_node_t *binding_get(binding_t *b, const char *var) {
    for (int i = 0; i < b->var_count; i++) {
        if (strcmp(b->var_names[i], var) == 0) {
            return &b->var_nodes[i];
        }
    }
    return NULL;
}

/* Deep copy a node: heap-dup all string fields so the binding owns them */
static void node_deep_copy(cbm_node_t *dst, const cbm_node_t *src) {
    *dst = *src;
    dst->project = heap_strdup(src->project);
    dst->label = heap_strdup(src->label);
    dst->name = heap_strdup(src->name);
    dst->qualified_name = heap_strdup(src->qualified_name);
    dst->file_path = heap_strdup(src->file_path);
    dst->properties_json = heap_strdup(src->properties_json);
}

static void node_fields_free(cbm_node_t *n) {
    if (!n) {
        return;
    }
    free((void *)n->project);
    free((void *)n->label);
    free((void *)n->name);
    free((void *)n->qualified_name);
    free((void *)n->file_path);
    free((void *)n->properties_json);
}

/* Deep copy an edge (binding owns the strings) */
static void edge_deep_copy(cbm_edge_t *dst, const cbm_edge_t *src) {
    *dst = *src;
    dst->project = heap_strdup(src->project);
    dst->type = heap_strdup(src->type);
    dst->properties_json = heap_strdup(src->properties_json);
}

static void edge_fields_free(cbm_edge_t *e) {
    free((void *)e->project);
    free((void *)e->type);
    free((void *)e->properties_json);
}

/* Set an edge variable in a binding */
static void binding_set_edge(binding_t *b, const char *var, const cbm_edge_t *edge) {
    /* Check existing — free old fields first */
    for (int i = 0; i < b->edge_var_count; i++) {
        if (strcmp(b->edge_var_names[i], var) == 0) {
            edge_fields_free(&b->edge_vars[i]);
            edge_deep_copy(&b->edge_vars[i], edge);
            return;
        }
    }
    if (b->edge_var_count >= CYP_MAX_EDGE_VARS) {
        return;
    }
    b->edge_var_names[b->edge_var_count] = var; /* not owned — points to AST string */
    edge_deep_copy(&b->edge_vars[b->edge_var_count], edge);
    b->edge_var_count++;
}

/* Free all deep-copied nodes and edges in a binding */
static void binding_free(binding_t *b) {
    for (int i = 0; i < b->var_count; i++) {
        node_fields_free(&b->var_nodes[i]);
    }
    for (int i = 0; i < b->edge_var_count; i++) {
        edge_fields_free(&b->edge_vars[i]);
    }
}

/* Deep-copy a binding (so source and dest own separate string copies) */
static void binding_copy(binding_t *dst, const binding_t *src) {
    dst->var_count = src->var_count;
    for (int i = 0; i < src->var_count; i++) {
        dst->var_names[i] = src->var_names[i]; /* AST-owned, not freed */
        node_deep_copy(&dst->var_nodes[i], &src->var_nodes[i]);
    }
    dst->edge_var_count = src->edge_var_count;
    for (int i = 0; i < src->edge_var_count; i++) {
        dst->edge_var_names[i] = src->edge_var_names[i]; /* AST-owned */
        edge_deep_copy(&dst->edge_vars[i], &src->edge_vars[i]);
    }
    dst->store = src->store;
}

/* Deep-copy a node into a binding (binding owns the strings) */
static void binding_set(binding_t *b, const char *var, const cbm_node_t *node) {
    /* Check existing — free old fields first */
    for (int i = 0; i < b->var_count; i++) {
        if (strcmp(b->var_names[i], var) == 0) {
            node_fields_free(&b->var_nodes[i]);
            node_deep_copy(&b->var_nodes[i], node);
            return;
        }
    }
    if (b->var_count >= CYP_MAX_VARS) {
        return;
    }
    b->var_names[b->var_count] = var; /* not owned — points to AST string */
    node_deep_copy(&b->var_nodes[b->var_count], node);
    b->var_count++;
}

/* Resolve the actual property value for a condition from a binding */
static const char *resolve_condition_value(const cbm_condition_t *c, binding_t *b) {
    cbm_edge_t *e = binding_get_edge(b, c->variable);
    if (e) {
        return edge_prop(e, c->property);
    }
    cbm_node_t *n = binding_get(b, c->variable);
    if (!n) {
        return NULL; /* unbound variable */
    }
    if (c->property) {
        return node_prop(n, c->property, b->store);
    }
    /* Bare alias (e.g. post-WITH virtual var) — use node name directly */
    return n->name ? n->name : "";
}

/* Evaluate a comparison operator between actual and expected strings. */
static bool eval_comparison_op(const char *op, const char *actual, const char *expected) {
    if (strcmp(op, "=") == 0) {
        return strcmp(actual, expected) == 0;
    }
    if (strcmp(op, "<>") == 0) {
        return strcmp(actual, expected) != 0;
    }
    if (strcmp(op, "=~") == 0) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, expected, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            return false;
        }
        int rc = cbm_regexec(&re, actual, 0, NULL, 0);
        cbm_regfree(&re);
        return rc == 0;
    }
    if (strcmp(op, "CONTAINS") == 0) {
        return strstr(actual, expected) != NULL;
    }
    if (strcmp(op, "STARTS WITH") == 0) {
        return strncmp(actual, expected, strlen(expected)) == 0;
    }
    if (strcmp(op, "ENDS WITH") == 0) {
        size_t alen = strlen(actual);
        size_t elen = strlen(expected);
        return alen >= elen && strcmp(actual + alen - elen, expected) == 0;
    }
    if (strcmp(op, ">") == 0 || strcmp(op, "<") == 0 || strcmp(op, ">=") == 0 ||
        strcmp(op, "<=") == 0) {
        double a = strtod(actual, NULL);
        double exp_val = strtod(expected, NULL);
        if (op[0] == '>' && op[CYP_CHAR_IDX1] == '=') {
            return a >= exp_val;
        }
        if (op[0] == '<' && op[CYP_CHAR_IDX1] == '=') {
            return a <= exp_val;
        }
        if (op[0] == '>') {
            return a > exp_val;
        }
        return a < exp_val;
    }
    return false;
}

/* Evaluate a WHERE condition against a binding */
static bool eval_condition(const cbm_condition_t *c, binding_t *b) {
    const char *actual = resolve_condition_value(c, b);
    if (!actual) {
        return true;
    }

    bool result;

    /* IS NULL / IS NOT NULL */
    if (strcmp(c->op, "IS NULL") == 0) {
        result = (!actual || actual[0] == '\0');
        return c->negated ? !result : result;
    }
    if (strcmp(c->op, "IS NOT NULL") == 0) {
        result = (actual && actual[0] != '\0');
        return c->negated ? !result : result;
    }

    /* IN [...] */
    if (strcmp(c->op, "IN") == 0) {
        result = false;
        for (int i = 0; i < c->in_value_count; i++) {
            if (strcmp(actual, c->in_values[i]) == 0) {
                result = true;
                break;
            }
        }
        return c->negated ? !result : result;
    }

    result = eval_comparison_op(c->op, actual, c->value);
    return c->negated ? !result : result;
}

/* Recursive expression tree evaluator */
static bool eval_expr(const cbm_expr_t *e, binding_t *b) { // NOLINT(misc-no-recursion)
    if (!e) {
        return true;
    }
    switch (e->type) {
    case EXPR_CONDITION:
        return eval_condition(&e->cond, b);
    case EXPR_AND:
        return (eval_expr(e->left, b) && eval_expr(e->right, b)) != 0;
    case EXPR_OR:
        return (eval_expr(e->left, b) || eval_expr(e->right, b)) != 0;
    case EXPR_NOT:
        return (!eval_expr(e->left, b)) != 0;
    case EXPR_XOR:
        return eval_expr(e->left, b) != eval_expr(e->right, b);
    }
    return true;
}

/* Evaluate WHERE clause — uses expression tree if available, falls back to legacy */
static bool eval_where(const cbm_where_clause_t *w, binding_t *b) {
    if (!w) {
        return true;
    }
    if (w->root) {
        return eval_expr(w->root, b);
    }

    /* Legacy flat evaluation */
    if (w->count == 0) {
        return true;
    }
    bool is_and = (w->op && strcmp(w->op, "AND") == 0) != 0;
    for (int i = 0; i < w->count; i++) {
        bool r = eval_condition(&w->conditions[i], b);
        if (is_and && !r) {
            return false;
        }
        if (!is_and && r) {
            return true;
        }
    }
    return is_and;
}

/* Check if a string value looks like a regex pattern. */
static bool looks_like_regex(const char *s) {
    if (!s) {
        return false;
    }
    return strstr(s, ".*") || strstr(s, ".+") || strchr(s, '[') || strchr(s, '(') ||
           strchr(s, '|') || strchr(s, '^') || strchr(s, '$');
}

/* Check inline property filters.
 * Values that look like regex patterns are matched with POSIX ERE;
 * plain values use exact strcmp. */
static bool check_inline_props(const cbm_node_t *n, const cbm_prop_filter_t *props, int count,
                               cbm_store_t *store) {
    for (int i = 0; i < count; i++) {
        const char *actual = node_prop(n, props[i].key, store);
        if (looks_like_regex(props[i].value)) {
            cbm_regex_t re;
            if (cbm_regcomp(&re, props[i].value, CBM_REG_EXTENDED | CBM_REG_NOSUB) == 0) {
                bool matched = cbm_regexec(&re, actual, 0, NULL, 0) == 0;
                cbm_regfree(&re);
                if (!matched) {
                    return false;
                }
            } else if (strcmp(actual, props[i].value) != 0) {
                return false;
            }
        } else if (strcmp(actual, props[i].value) != 0) {
            return false;
        }
    }
    return true;
}

/* ── Result building helpers ────────────────────────────────────── */

typedef struct {
    const char ***rows;
    int row_count;
    int row_cap;
    const char **columns;
    int col_count;
} result_builder_t;

static void rb_init(result_builder_t *rb) {
    memset(rb, 0, sizeof(*rb));
    rb->row_cap = CBM_SZ_32;
    rb->rows = malloc(rb->row_cap * sizeof(const char **));
}

static void rb_set_columns(result_builder_t *rb, const char **cols, int count) {
    rb->columns = malloc((count > 0 ? (size_t)count : SKIP_ONE) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        rb->columns[i] = heap_strdup(cols[i]);
    }
    rb->col_count = count;
}

static void rb_add_row(result_builder_t *rb, const char **values) {
    if (rb->row_count >= rb->row_cap) {
        rb->row_cap *= PAIR_LEN;
        rb->rows = safe_realloc(rb->rows, rb->row_cap * sizeof(const char **));
    }
    const char **row =
        malloc((rb->col_count > 0 ? (size_t)rb->col_count : SKIP_ONE) * sizeof(const char *));
    for (int i = 0; i < rb->col_count; i++) {
        row[i] = values[i] ? heap_strdup(values[i]) : heap_strdup("");
    }
    rb->rows[rb->row_count++] = row;
}

/* ── Main execution ─────────────────────────────────────────────── */

/* Hard ceiling: queries returning more than this trigger an error instead of data.
 * Prevents accidental multi-GB JSON payloads from unbounded MATCH (n) RETURN n. */
#define CYPHER_RESULT_CEILING 100000

/* ── Binding virtual variables (for WITH clause) ──────────────── */

static const char *binding_get_virtual(binding_t *b, const char *var, const char *prop) {
    /* Check virtual vars first (from WITH projection) */
    char full[CBM_SZ_256];
    if (prop) {
        snprintf(full, sizeof(full), "%s.%s", var, prop);
    } else {
        snprintf(full, sizeof(full), "%s", var);
    }
    for (int i = 0; i < b->var_count; i++) {
        if (strcmp(b->var_names[i], full) == 0) {
            return b->var_nodes[i].name ? b->var_nodes[i].name : "";
        }
    }
    /* Fall through to normal lookup */
    cbm_edge_t *e = binding_get_edge(b, var);
    if (e) {
        return prop ? edge_prop(e, prop) : "";
    }
    cbm_node_t *n = binding_get(b, var);
    if (n) {
        if (prop) {
            return node_prop(n, prop, b->store);
        }
        return n->name ? n->name : "";
    }
    return "";
}

/* ── String function application ──────────────────────────────── */

static const char *apply_string_func(const char *func, const char *val, char *buf, size_t buf_sz) {
    if (!func || !val) {
        return val ? val : "";
    }
    if (strcmp(func, "toLower") == 0) {
        size_t i = 0;
        for (; i < buf_sz - SKIP_ONE && val[i]; i++) {
            buf[i] = (char)tolower((unsigned char)val[i]);
        }
        buf[i] = '\0';
        return buf;
    }
    if (strcmp(func, "toUpper") == 0) {
        size_t i = 0;
        for (; i < buf_sz - SKIP_ONE && val[i]; i++) {
            buf[i] = (char)toupper((unsigned char)val[i]);
        }
        buf[i] = '\0';
        return buf;
    }
    if (strcmp(func, "toString") == 0) {
        return val; /* already strings */
    }
    return val;
}

/* ── CASE expression evaluation ───────────────────────────────── */

static const char *eval_case_expr(const cbm_case_expr_t *k, binding_t *b) {
    if (!k) {
        return "";
    }
    for (int i = 0; i < k->branch_count; i++) {
        if (eval_expr(k->branches[i].when_expr, b)) {
            return k->branches[i].then_val ? k->branches[i].then_val : "";
        }
    }
    return k->else_val ? k->else_val : "";
}

/* ── Scan nodes for a pattern ─────────────────────────────────── */

static void scan_pattern_nodes(cbm_store_t *store, const char *project, int max_rows,
                               cbm_node_pattern_t *first, cbm_node_t **out_nodes, int *out_count) {
    if (first->label) {
        cbm_store_find_nodes_by_label(store, project, first->label, out_nodes, out_count);
    } else {
        cbm_search_params_t params = {.project = project,
                                      .min_degree = CYP_FOUND_NONE,
                                      .max_degree = CYP_FOUND_NONE,
                                      .limit = max_rows * CYP_GROWTH_10};
        cbm_search_output_t sout = {0};
        cbm_store_search(store, &params, &sout);
        *out_count = sout.count;
        *out_nodes = malloc(sout.count * sizeof(cbm_node_t));
        for (int i = 0; i < sout.count; i++) {
            (*out_nodes)[i] = sout.results[i].node;
            sout.results[i].node.name = NULL;
            sout.results[i].node.project = NULL;
            sout.results[i].node.label = NULL;
            sout.results[i].node.qualified_name = NULL;
            sout.results[i].node.file_path = NULL;
            sout.results[i].node.properties_json = NULL;
        }
        cbm_store_search_free(&sout);
    }
    /* Apply inline property filters — free rejected nodes' strings */
    if (first->prop_count > 0) {
        int kept = 0;
        for (int i = 0; i < *out_count; i++) {
            if (check_inline_props(&(*out_nodes)[i], first->props, first->prop_count, store)) {
                if (kept != i) {
                    (*out_nodes)[kept] = (*out_nodes)[i];
                }
                kept++;
            } else {
                node_fields_free(&(*out_nodes)[i]);
            }
        }
        *out_count = kept;
    }
}

/* ── Expand one pattern's relationships on a set of bindings ──── */

/* Process edges: look up target node, filter by label/props, add binding.
 * `inbound` controls which end of the edge is the target id. */
static void process_edges(cbm_store_t *store, cbm_edge_t *edges, int edge_count, bool inbound,
                          const cbm_node_pattern_t *target_node, binding_t *b, const char *to_var,
                          const char *rel_var, binding_t *new_bindings, int *new_count, int max_new,
                          int *match_count) {
    for (int ei = 0; ei < edge_count && *new_count < max_new; ei++) {
        int64_t tid = inbound ? edges[ei].source_id : edges[ei].target_id;
        cbm_node_t found = {0};
        if (cbm_store_find_node_by_id(store, tid, &found) != CBM_STORE_OK) {
            continue;
        }
        if (target_node->label && strcmp(found.label, target_node->label) != 0) {
            node_fields_free(&found);
            continue;
        }
        if (!check_inline_props(&found, target_node->props, target_node->prop_count, store)) {
            node_fields_free(&found);
            continue;
        }
        binding_t nb = {0};
        binding_copy(&nb, b);
        binding_set(&nb, to_var, &found);
        if (rel_var) {
            binding_set_edge(&nb, rel_var, &edges[ei]);
        }
        node_fields_free(&found);
        new_bindings[(*new_count)++] = nb;
        (*match_count)++;
    }
}

/* Expand variable-length relationship via BFS */
static void expand_var_length(cbm_store_t *store, cbm_rel_pattern_t *rel,
                              cbm_node_pattern_t *target_node, binding_t *b, cbm_node_t *src,
                              const char *to_var, binding_t *new_bindings, int *new_count,
                              int max_new, int *match_count) {
    int max_depth = rel->max_hops > 0 ? rel->max_hops : CYP_MAX_DEPTH;
    cbm_traverse_result_t tr = {0};
    const char *dir = rel->direction ? rel->direction : "outbound";
    cbm_store_bfs(store, src->id, dir, rel->types, rel->type_count, max_depth, CBM_PERCENT, &tr);
    for (int v = 0; v < tr.visited_count && *new_count < max_new; v++) {
        cbm_node_hop_t *hop = &tr.visited[v];
        if (hop->hop < rel->min_hops) {
            continue;
        }
        if (target_node->label && strcmp(hop->node.label, target_node->label) != 0) {
            continue;
        }
        if (!check_inline_props(&hop->node, target_node->props, target_node->prop_count, store)) {
            continue;
        }
        binding_t nb = {0};
        binding_copy(&nb, b);
        binding_set(&nb, to_var, &hop->node);
        new_bindings[(*new_count)++] = nb;
        (*match_count)++;
    }
    cbm_store_traverse_free(&tr);
}

/* Expand fixed-length (1-hop) relationship edges */
static void expand_fixed_length(cbm_store_t *store, cbm_rel_pattern_t *rel,
                                cbm_node_pattern_t *target_node, binding_t *b, cbm_node_t *src,
                                const char *to_var, binding_t *new_bindings, int *new_count,
                                int max_new, int *match_count) {
    bool is_inbound = rel->direction && strcmp(rel->direction, "inbound") == 0;
    bool is_any = rel->direction && strcmp(rel->direction, "any") == 0;
    const char *rel_var = rel->variable;

    if (rel->type_count > 0) {
        for (int ti = 0; ti < rel->type_count; ti++) {
            cbm_edge_t *edges = NULL;
            int edge_count = 0;
            if (is_inbound) {
                cbm_store_find_edges_by_target_type(store, src->id, rel->types[ti], &edges,
                                                    &edge_count);
            } else {
                cbm_store_find_edges_by_source_type(store, src->id, rel->types[ti], &edges,
                                                    &edge_count);
            }
            process_edges(store, edges, edge_count, is_inbound, target_node, b, to_var, rel_var,
                          new_bindings, new_count, max_new, match_count);
            cbm_store_free_edges(edges, edge_count);
        }
        if (is_any) {
            for (int ti = 0; ti < rel->type_count; ti++) {
                cbm_edge_t *edges = NULL;
                int edge_count = 0;
                cbm_store_find_edges_by_target_type(store, src->id, rel->types[ti], &edges,
                                                    &edge_count);
                process_edges(store, edges, edge_count, true, target_node, b, to_var, rel_var,
                              new_bindings, new_count, max_new, match_count);
                cbm_store_free_edges(edges, edge_count);
            }
        }
    } else {
        cbm_edge_t *edges = NULL;
        int edge_count = 0;
        if (is_inbound) {
            cbm_store_find_edges_by_target(store, src->id, &edges, &edge_count);
        } else {
            cbm_store_find_edges_by_source(store, src->id, &edges, &edge_count);
        }
        process_edges(store, edges, edge_count, is_inbound, target_node, b, to_var, rel_var,
                      new_bindings, new_count, max_new, match_count);
        cbm_store_free_edges(edges, edge_count);
        if (is_any) {
            edges = NULL;
            edge_count = 0;
            cbm_store_find_edges_by_target(store, src->id, &edges, &edge_count);
            process_edges(store, edges, edge_count, true, target_node, b, to_var, rel_var,
                          new_bindings, new_count, max_new, match_count);
            cbm_store_free_edges(edges, edge_count);
        }
    }
}

static void expand_pattern_rels(cbm_store_t *store, cbm_pattern_t *pat, binding_t **bindings,
                                int *bind_count, const int *bind_cap, const char **var_name,
                                bool is_optional) {
    for (int ri = 0; ri < pat->rel_count; ri++) {
        cbm_rel_pattern_t *rel = &pat->rels[ri];
        cbm_node_pattern_t *target_node = &pat->nodes[ri + SKIP_ONE];
        const char *to_var = target_node->variable ? target_node->variable : "_n_t";

        bool is_variable_length = (rel->min_hops != SKIP_ONE || rel->max_hops != SKIP_ONE);

        binding_t *new_bindings =
            malloc(((*bind_cap * CYP_GROWTH_10) + SKIP_ONE) * sizeof(binding_t));
        int new_count = 0;

        for (int bi = 0; bi < *bind_count; bi++) {
            binding_t *b = &(*bindings)[bi];
            cbm_node_t *src = binding_get(b, *var_name);
            if (!src) {
                continue;
            }

            int match_count = 0;

            int max_new = *bind_cap * CYP_GROWTH_10;
            if (is_variable_length) {
                expand_var_length(store, rel, target_node, b, src, to_var, new_bindings, &new_count,
                                  max_new, &match_count);
            } else {
                expand_fixed_length(store, rel, target_node, b, src, to_var, new_bindings,
                                    &new_count, max_new, &match_count);
            }

            /* OPTIONAL MATCH: keep binding with empty target if no matches */
            if (is_optional && match_count == 0) {
                binding_t nb = {0};
                binding_copy(&nb, b);
                /* Don't set to_var — it remains unbound; projection returns "" */
                new_bindings[new_count++] = nb;
            }
        }

        for (int bi = 0; bi < *bind_count; bi++) {
            binding_free(&(*bindings)[bi]);
        }
        free(*bindings);
        *bindings = new_bindings;
        *bind_count = new_count;
        *var_name = to_var;
    }
}

/* ── Result postprocessing helpers ─────────────────────────────── */

/* Find the column index for ORDER BY, checking both column names and aliases.
 * Returns -1 if not found. */
static int rb_find_order_column(const result_builder_t *rb, const cbm_return_clause_t *ret) {
    for (int ci = 0; ci < rb->col_count; ci++) {
        if (strcmp(rb->columns[ci], ret->order_by) == 0) {
            return ci;
        }
    }
    for (int ci = 0; ci < ret->count; ci++) {
        if (ret->items[ci].alias && strcmp(ret->items[ci].alias, ret->order_by) == 0) {
            return ci;
        }
    }
    return CBM_NOT_FOUND;
}

/* Check whether a column contains numeric data by examining the first non-empty value */
static bool rb_is_numeric_column(const result_builder_t *rb, int col) {
    for (int i = 0; i < rb->row_count; i++) {
        const char *v = rb->rows[i][col];
        if (v && *v) {
            const char *p2 = (*v == '-') ? v + SKIP_ONE : v;
            if (*p2 == '\0') {
                return false;
            }
            for (; *p2; p2++) {
                if (*p2 < '0' || *p2 > '9') {
                    return false;
                }
            }
            return true;
        }
    }
    return false;
}

static void rb_apply_order_by(result_builder_t *rb, const cbm_return_clause_t *ret) {
    if (!ret->order_by) {
        return;
    }
    int order_col = rb_find_order_column(rb, ret);
    if (order_col < 0) {
        return;
    }

    bool desc = ret->order_dir && strcmp(ret->order_dir, "DESC") == 0;
    bool numeric = rb_is_numeric_column(rb, order_col);
    for (int i = 0; i < rb->row_count - SKIP_ONE; i++) {
        for (int j = 0; j < rb->row_count - i - SKIP_ONE; j++) {
            int cmp;
            if (numeric) {
                cmp = (int)strtol(rb->rows[j][order_col], NULL, CBM_DECIMAL_BASE) -
                      (int)strtol(rb->rows[j + SKIP_ONE][order_col], NULL, CBM_DECIMAL_BASE);
            } else {
                cmp = strcmp(rb->rows[j][order_col], rb->rows[j + SKIP_ONE][order_col]);
            }
            if (desc ? cmp < 0 : cmp > 0) {
                const char **tmp = rb->rows[j];
                rb->rows[j] = rb->rows[j + SKIP_ONE];
                rb->rows[j + SKIP_ONE] = tmp;
            }
        }
    }
}

static void rb_apply_skip_limit(result_builder_t *rb, int skip_n, int limit) {
    /* Skip */
    if (skip_n > 0 && skip_n < rb->row_count) {
        for (int i = 0; i < skip_n; i++) {
            for (int c = 0; c < rb->col_count; c++) {
                free((void *)rb->rows[i][c]);
            }
            free(rb->rows[i]);
        }
        memmove(rb->rows, rb->rows + skip_n, (rb->row_count - skip_n) * sizeof(const char **));
        rb->row_count -= skip_n;
    } else if (skip_n >= rb->row_count) {
        for (int i = 0; i < rb->row_count; i++) {
            for (int c = 0; c < rb->col_count; c++) {
                free((void *)rb->rows[i][c]);
            }
            free(rb->rows[i]);
        }
        rb->row_count = 0;
    }
    /* Limit */
    if (limit > 0 && rb->row_count > limit) {
        for (int i = limit; i < rb->row_count; i++) {
            for (int c = 0; c < rb->col_count; c++) {
                free((void *)rb->rows[i][c]);
            }
            free(rb->rows[i]);
        }
        rb->row_count = limit;
    }
}

static void rb_apply_distinct(result_builder_t *rb) {
    if (rb->row_count <= SKIP_ONE) {
        return;
    }
    int kept = SKIP_ONE;
    for (int i = SKIP_ONE; i < rb->row_count; i++) {
        bool dup = false;
        for (int j = 0; j < kept && !dup; j++) {
            bool same = true;
            for (int c = 0; c < rb->col_count && same; c++) {
                if (strcmp(rb->rows[i][c], rb->rows[j][c]) != 0) {
                    same = false;
                }
            }
            if (same) {
                dup = true;
            }
        }
        if (!dup) {
            if (kept != i) {
                rb->rows[kept] = rb->rows[i];
            }
            kept++;
        } else {
            for (int c = 0; c < rb->col_count; c++) {
                free((void *)rb->rows[i][c]);
            }
            free(rb->rows[i]);
        }
    }
    rb->row_count = kept;
}

static void rb_free(result_builder_t *rb) {
    for (int i = 0; i < rb->row_count; i++) {
        for (int c = 0; c < rb->col_count; c++) {
            free((void *)rb->rows[i][c]);
        }
        free(rb->rows[i]);
    }
    free(rb->rows);
    for (int i = 0; i < rb->col_count; i++) {
        free((void *)rb->columns[i]);
    }
    free(rb->columns);
}

/* ── Get projection value for a binding + return item ─────────── */

static const char *project_item(binding_t *b, cbm_return_item_t *item, char *func_buf,
                                size_t buf_sz) {
    if (item->kase) {
        return eval_case_expr(item->kase, b);
    }
    const char *raw = binding_get_virtual(b, item->variable, item->property);
    if (item->func && (strcmp(item->func, "toLower") == 0 || strcmp(item->func, "toUpper") == 0 ||
                       strcmp(item->func, "toString") == 0)) {
        return apply_string_func(item->func, raw, func_buf, buf_sz);
    }
    return raw;
}

/* Check if a function name is an aggregate */
static bool is_aggregate_func(const char *func) {
    return func &&
           (strcmp(func, "COUNT") == 0 || strcmp(func, "SUM") == 0 || strcmp(func, "AVG") == 0 ||
            strcmp(func, "MIN") == 0 || strcmp(func, "MAX") == 0 || strcmp(func, "COLLECT") == 0);
}

/* Sort bindings by a virtual variable using bubble sort */
static void sort_bindings(binding_t *vbindings, int count, const char *key, bool desc) {
    for (int i = 0; i < count - SKIP_ONE; i++) {
        for (int j = 0; j < count - i - SKIP_ONE; j++) {
            const char *va = binding_get_virtual(&vbindings[j], key, NULL);
            const char *vb2 = binding_get_virtual(&vbindings[j + SKIP_ONE], key, NULL);
            char *ea = NULL;
            char *eb = NULL;
            double da = strtod(va, &ea);
            double db = strtod(vb2, &eb);
            int cmp = (ea != va && eb != vb2) ? ((da > db) - (da < db)) : strcmp(va, vb2);
            if (desc ? cmp < 0 : cmp > 0) {
                binding_t tmp = vbindings[j];
                vbindings[j] = vbindings[j + SKIP_ONE];
                vbindings[j + SKIP_ONE] = tmp;
            }
        }
    }
}

/* Apply skip and limit to a binding array, freeing discarded entries */
static void bindings_skip_limit(binding_t *vbindings, int *count, int skip, int limit) {
    if (skip > 0 && skip < *count) {
        for (int i = 0; i < skip; i++) {
            binding_free(&vbindings[i]);
        }
        memmove(vbindings, vbindings + skip, (*count - skip) * sizeof(binding_t));
        *count -= skip;
    } else if (skip >= *count) {
        for (int i = 0; i < *count; i++) {
            binding_free(&vbindings[i]);
        }
        *count = 0;
    }
    if (limit > 0 && *count > limit) {
        for (int i = limit; i < *count; i++) {
            binding_free(&vbindings[i]);
        }
        *count = limit;
    }
}

/* Sort, skip, and limit binding array in-place */
static void with_sort_skip_limit(const cbm_return_clause_t *wc, binding_t *vbindings, int *vcount) {
    if (wc->order_by) {
        bool wdesc = wc->order_dir && strcmp(wc->order_dir, "DESC") == 0;
        sort_bindings(vbindings, *vcount, wc->order_by, wdesc);
    }
    bindings_skip_limit(vbindings, vcount, wc->skip, wc->limit);
}

/* Resolve the alias or compute a default name for a WITH/RETURN item */
static const char *resolve_item_alias(const cbm_return_item_t *item, char *name_buf,
                                      size_t buf_sz) {
    if (item->alias) {
        return item->alias;
    }
    if (item->property) {
        snprintf(name_buf, buf_sz, "%s.%s", item->variable, item->property);
    } else {
        snprintf(name_buf, buf_sz, "%s", item->variable);
    }
    return name_buf;
}

/* ── WITH clause: project bindings through aggregation or rename ── */

/* WITH aggregation group entry */
typedef struct {
    char group_key[CBM_SZ_1K];
    const char **group_vals;
    double *sums;
    int *counts;
    double *mins, *maxs;
} with_agg_t;

/* Build a group key from non-aggregate WITH items */
static int with_agg_build_key(cbm_return_clause_t *wc, binding_t *b, char *key, size_t key_sz) {
    int kl = 0;
    for (int ci = 0; ci < wc->count; ci++) {
        if (wc->items[ci].func) {
            continue;
        }
        const char *v = binding_get_virtual(b, wc->items[ci].variable, wc->items[ci].property);
        kl += snprintf(key + kl, key_sz - (size_t)kl, "%s|", v);
        if (kl >= (int)key_sz) {
            kl = (int)key_sz - SKIP_ONE;
        }
    }
    return kl;
}

/* Find or create an aggregation group. Returns index. */
static int with_agg_find_or_create(with_agg_t **aggs, int *agg_cnt, int *agg_cap,
                                   cbm_return_clause_t *wc, binding_t *b, const char *key) {
    for (int a = 0; a < *agg_cnt; a++) {
        if (strcmp((*aggs)[a].group_key, key) == 0) {
            return a;
        }
    }
    if (*agg_cnt >= *agg_cap) {
        *agg_cap *= PAIR_LEN;
        *aggs = safe_realloc(*aggs, *agg_cap * sizeof(with_agg_t));
    }
    int found = (*agg_cnt)++;
    snprintf((*aggs)[found].group_key, sizeof((*aggs)[found].group_key), "%s", key);
    (*aggs)[found].group_vals = calloc(wc->count, sizeof(const char *));
    (*aggs)[found].sums = calloc(wc->count, sizeof(double));
    (*aggs)[found].counts = calloc(wc->count, sizeof(int));
    (*aggs)[found].mins = calloc(wc->count, sizeof(double));
    (*aggs)[found].maxs = calloc(wc->count, sizeof(double));
    for (int ci = 0; ci < wc->count; ci++) {
        (*aggs)[found].mins[ci] = CYP_DBL_MAX;
        (*aggs)[found].maxs[ci] = -CYP_DBL_MAX;
    }
    for (int ci = 0; ci < wc->count; ci++) {
        if (wc->items[ci].func) {
            (*aggs)[found].group_vals[ci] = heap_strdup("0");
            continue;
        }
        const char *v = binding_get_virtual(b, wc->items[ci].variable, wc->items[ci].property);
        (*aggs)[found].group_vals[ci] = heap_strdup(v);
    }
    return found;
}

/* Accumulate aggregation values for a binding */
static void with_agg_accumulate(with_agg_t *agg, cbm_return_clause_t *wc, binding_t *b) {
    for (int ci = 0; ci < wc->count; ci++) {
        if (!wc->items[ci].func) {
            continue;
        }
        agg->counts[ci]++;
        const char *raw = binding_get_virtual(b, wc->items[ci].variable, wc->items[ci].property);
        double dv = strtod(raw, NULL);
        agg->sums[ci] += dv;
        if (dv < agg->mins[ci]) {
            agg->mins[ci] = dv;
        }
        if (dv > agg->maxs[ci]) {
            agg->maxs[ci] = dv;
        }
    }
}

/* Format a WITH aggregation value into buf */
static void with_agg_format(const char *func, with_agg_t *agg, int ci, char *buf, size_t buf_sz) {
    if (strcmp(func, "SUM") == 0) {
        snprintf(buf, buf_sz, "%.10g", agg->sums[ci]);
    } else if (strcmp(func, "AVG") == 0) {
        snprintf(buf, buf_sz, "%.10g", agg->counts[ci] > 0 ? agg->sums[ci] / agg->counts[ci] : 0.0);
    } else if (strcmp(func, "MIN") == 0) {
        snprintf(buf, buf_sz, "%.10g", agg->mins[ci]);
    } else if (strcmp(func, "MAX") == 0) {
        snprintf(buf, buf_sz, "%.10g", agg->maxs[ci]);
    } else {
        snprintf(buf, buf_sz, "%d", agg->counts[ci]);
    }
}

/* Add a virtual variable binding for one WITH item */
static void with_add_vbinding_var(binding_t *vb, const char *alias, const char *val) {
    cbm_node_t vn = {.name = heap_strdup(val), .qualified_name = heap_strdup(alias)};
    if (vb->var_count < CYP_BUF_16) {
        vb->var_names[vb->var_count] = vn.qualified_name;
        vb->var_nodes[vb->var_count] = vn;
        vb->var_count++;
    }
}

/* Free with_agg_t array */
static void with_agg_free(with_agg_t *aggs, int agg_cnt, int item_count) {
    for (int a = 0; a < agg_cnt; a++) {
        for (int ci = 0; ci < item_count; ci++) {
            free((void *)aggs[a].group_vals[ci]);
        }
        free(aggs[a].group_vals);
        free(aggs[a].sums);
        free(aggs[a].counts);
        free(aggs[a].mins);
        free(aggs[a].maxs);
    }
    free(aggs);
}

/* Execute WITH aggregation path */
static void execute_with_aggregate(cbm_return_clause_t *wc, binding_t *bindings, int bind_count,
                                   binding_t **vbindings, int *vcount) {
    int agg_cap = CBM_SZ_256;
    with_agg_t *aggs = calloc(agg_cap, sizeof(with_agg_t));
    int agg_cnt = 0;

    for (int bi = 0; bi < bind_count; bi++) {
        char key[CBM_SZ_1K] = "";
        with_agg_build_key(wc, &bindings[bi], key, sizeof(key));
        int found = with_agg_find_or_create(&aggs, &agg_cnt, &agg_cap, wc, &bindings[bi], key);
        with_agg_accumulate(&aggs[found], wc, &bindings[bi]);
    }

    *vbindings = safe_realloc(*vbindings, (agg_cnt + SKIP_ONE) * sizeof(binding_t));
    if (!*vbindings) {
        with_agg_free(aggs, agg_cnt, wc->count);
        return;
    }
    for (int a = 0; a < agg_cnt; a++) {
        binding_t vb = {0};
        for (int ci = 0; ci < wc->count; ci++) {
            char name_buf[CBM_SZ_256];
            const char *alias = resolve_item_alias(&wc->items[ci], name_buf, sizeof(name_buf));
            if (wc->items[ci].func) {
                char vbuf[CBM_SZ_64];
                with_agg_format(wc->items[ci].func, &aggs[a], ci, vbuf, sizeof(vbuf));
                with_add_vbinding_var(&vb, alias, vbuf);
            } else {
                with_add_vbinding_var(&vb, alias, aggs[a].group_vals[ci]);
            }
        }
        (*vbindings)[(*vcount)++] = vb;
    }
    with_agg_free(aggs, agg_cnt, wc->count);
}

/* Execute WITH simple (non-aggregate) projection */
static void execute_with_simple(cbm_return_clause_t *wc, binding_t *bindings, int bind_count,
                                binding_t *vbindings, int *vcount) {
    for (int bi = 0; bi < bind_count; bi++) {
        binding_t vb = {0};
        for (int ci = 0; ci < wc->count; ci++) {
            char name_buf[CBM_SZ_256];
            const char *alias = resolve_item_alias(&wc->items[ci], name_buf, sizeof(name_buf));
            char func_buf[CBM_SZ_512];
            const char *val =
                project_item(&bindings[bi], &wc->items[ci], func_buf, sizeof(func_buf));
            with_add_vbinding_var(&vb, alias, val);
        }
        vbindings[(*vcount)++] = vb;
    }
}

/* Apply post-WITH WHERE filter */
static void filter_bindings_where(const cbm_where_clause_t *where, binding_t *vbindings,
                                  int *vcount) {
    int kept = 0;
    for (int i = 0; i < *vcount; i++) {
        if (eval_where(where, &vbindings[i])) {
            if (kept != i) {
                vbindings[kept] = vbindings[i];
            }
            kept++;
        } else {
            binding_free(&vbindings[i]);
        }
    }
    *vcount = kept;
}

static void execute_with_clause(cbm_query_t *q, binding_t **bindings_ptr, int *bind_count_ptr) {
    cbm_return_clause_t *wc = q->with_clause;
    if (!wc) {
        return;
    }
    binding_t *bindings = *bindings_ptr;
    int bind_count = *bind_count_ptr;

    binding_t *vbindings = malloc((bind_count + SKIP_ONE) * sizeof(binding_t));
    int vcount = 0;

    bool has_agg = false;
    for (int i = 0; i < wc->count; i++) {
        if (is_aggregate_func(wc->items[i].func)) {
            has_agg = true;
            break;
        }
    }

    if (has_agg) {
        execute_with_aggregate(wc, bindings, bind_count, &vbindings, &vcount);
    } else {
        execute_with_simple(wc, bindings, bind_count, vbindings, &vcount);
    }

    with_sort_skip_limit(wc, vbindings, &vcount);

    for (int bi = 0; bi < bind_count; bi++) {
        binding_free(&bindings[bi]);
    }
    free(bindings);

    if (q->post_with_where) {
        filter_bindings_where(q->post_with_where, vbindings, &vcount);
    }

    *bindings_ptr = vbindings;
    *bind_count_ptr = vcount;
}

/* ── Execute a single query (no UNION recursion) ──────────────── */

/* Project RETURN * — all bound variable properties */
/* Collect all variable names from query patterns */
static int collect_pattern_vars(cbm_query_t *q, const char **vars, int max_vars) {
    int vc = 0;
    for (int pi = 0; pi < q->pattern_count; pi++) {
        for (int ni = 0; ni < q->patterns[pi].node_count && vc < max_vars; ni++) {
            if (q->patterns[pi].nodes[ni].variable) {
                vars[vc++] = q->patterns[pi].nodes[ni].variable;
            }
        }
        for (int ri = 0; ri < q->patterns[pi].rel_count && vc < max_vars; ri++) {
            if (q->patterns[pi].rels[ri].variable) {
                vars[vc++] = q->patterns[pi].rels[ri].variable;
            }
        }
    }
    return vc;
}

/* Build star-projection columns: var.name, var.qualified_name, var.label, var.file_path */
static void build_star_columns(result_builder_t *rb, const char **vars, int vc) {
    int col_n = vc * CYP_NODE_COLS;
    const char *col_names[CBM_SZ_128];
    for (int v = 0; v < vc; v++) {
        char buf[CBM_SZ_128];
        snprintf(buf, sizeof(buf), "%s.name", vars[v]);
        col_names[(size_t)v * CYP_NODE_COLS] = heap_strdup(buf);
        snprintf(buf, sizeof(buf), "%s.qualified_name", vars[v]);
        col_names[((size_t)v * CYP_NODE_COLS) + SKIP_ONE] = heap_strdup(buf);
        snprintf(buf, sizeof(buf), "%s.label", vars[v]);
        col_names[((size_t)v * CYP_NODE_COLS) + PAIR_LEN] = heap_strdup(buf);
        snprintf(buf, sizeof(buf), "%s.file_path", vars[v]);
        col_names[((size_t)v * CYP_NODE_COLS) + CYP_TRIPLE] = heap_strdup(buf);
    }
    rb_set_columns(rb, col_names, col_n);
    for (int i = 0; i < col_n; i++) {
        free((void *)col_names[i]);
    }
}

/* Project one variable's 4 columns for RETURN * */
static void project_star_var(binding_t *b, const char *var, const char **vals) {
    cbm_edge_t *edge = binding_get_edge(b, var);
    if (edge) {
        vals[0] = edge_prop(edge, "type");
        vals[SKIP_ONE] = "";
        vals[PAIR_LEN] = "";
        vals[CYP_TRIPLE] = "";
        return;
    }
    cbm_node_t *n = binding_get(b, var);
    vals[0] = n && n->name ? n->name : "";
    vals[SKIP_ONE] = n && n->qualified_name ? n->qualified_name : "";
    vals[PAIR_LEN] = n && n->label ? n->label : "";
    vals[CYP_TRIPLE] = n && n->file_path ? n->file_path : "";
}

/* Project one binding row for RETURN * */
static void project_star_row(binding_t *b, const char **vars, int vc, const char **vals) {
    for (int v = 0; v < vc; v++) {
        project_star_var(b, vars[v], vals + ((size_t)v * CYP_NODE_COLS));
    }
}

static void execute_return_star(cbm_query_t *q, binding_t *bindings, int bind_count, int max_rows,
                                result_builder_t *rb) {
    const char *vars[CBM_SZ_32];
    int vc = collect_pattern_vars(q, vars, CBM_SZ_32);
    build_star_columns(rb, vars, vc);
    for (int bi = 0; bi < bind_count && rb->row_count < max_rows; bi++) {
        const char *vals[CBM_SZ_128];
        project_star_row(&bindings[bi], vars, vc, vals);
        rb_add_row(rb, vals);
    }
}

/* Format an aggregate value into buf based on function name */
/* Format a COLLECT list as JSON array string */
static void format_collect_list(char **items, int item_count, char *buf, size_t buf_sz) {
    char cbuf[CBM_SZ_2K] = "[";
    int bl = SKIP_ONE;
    for (int i = 0; i < item_count; i++) {
        if (i > 0) {
            cbuf[bl++] = ',';
        }
        bl += snprintf(cbuf + bl, sizeof(cbuf) - (size_t)bl, "\"%s\"", items[i]);
        if (bl >= (int)sizeof(cbuf)) {
            bl = (int)sizeof(cbuf) - SKIP_ONE;
        }
    }
    if (bl < (int)sizeof(cbuf) - SKIP_ONE) {
        cbuf[bl++] = ']';
    }
    cbuf[bl] = '\0';
    snprintf(buf, buf_sz, "%s", cbuf);
}

static void format_agg_value(const char *func, int count, double sum, double min_val,
                             double max_val, char ***collect_lists, int *collect_counts, int ci,
                             char *buf, size_t buf_sz) {
    if (strcmp(func, "SUM") == 0) {
        snprintf(buf, buf_sz, "%.10g", sum);
    } else if (strcmp(func, "AVG") == 0) {
        snprintf(buf, buf_sz, "%.10g", count > 0 ? sum / count : 0.0);
    } else if (strcmp(func, "MIN") == 0) {
        snprintf(buf, buf_sz, "%.10g", min_val);
    } else if (strcmp(func, "MAX") == 0) {
        snprintf(buf, buf_sz, "%.10g", max_val);
    } else if (strcmp(func, "COLLECT") == 0) {
        format_collect_list(collect_lists[ci], collect_counts[ci], buf, buf_sz);
    } else {
        snprintf(buf, buf_sz, "%d", count);
    }
}

/* RETURN aggregation entry */
typedef struct {
    char group_key[CBM_SZ_1K];
    const char **group_vals;
    double *sums;
    int *counts;
    double *mins, *maxs;
    char ***collect_lists;
    int *collect_counts;
} ret_agg_entry_t;

/* Initialize a new RETURN aggregation group */
static void ret_agg_init_group(ret_agg_entry_t *entry, const char *key, int item_count,
                               const char **vals) {
    snprintf(entry->group_key, sizeof(entry->group_key), "%s", key);
    entry->group_vals = calloc(item_count, sizeof(const char *));
    entry->sums = calloc(item_count, sizeof(double));
    entry->counts = calloc(item_count, sizeof(int));
    entry->mins = malloc(item_count * sizeof(double));
    entry->maxs = malloc(item_count * sizeof(double));
    entry->collect_lists = calloc(item_count, sizeof(char **));
    entry->collect_counts = calloc(item_count, sizeof(int));
    for (int ci = 0; ci < item_count; ci++) {
        entry->mins[ci] = CYP_DBL_MAX;
        entry->maxs[ci] = -CYP_DBL_MAX;
        entry->group_vals[ci] = heap_strdup(vals[ci]);
    }
}

/* Accumulate a binding into RETURN aggregation */
static void ret_agg_accumulate(ret_agg_entry_t *entry, cbm_return_clause_t *ret, binding_t *b) {
    for (int ci = 0; ci < ret->count; ci++) {
        if (!ret->items[ci].func) {
            continue;
        }
        entry->counts[ci]++;
        const char *raw = binding_get_virtual(b, ret->items[ci].variable, ret->items[ci].property);
        double dv = strtod(raw, NULL);
        entry->sums[ci] += dv;
        if (dv < entry->mins[ci]) {
            entry->mins[ci] = dv;
        }
        if (dv > entry->maxs[ci]) {
            entry->maxs[ci] = dv;
        }
        if (strcmp(ret->items[ci].func, "COLLECT") == 0) {
            int idx = entry->collect_counts[ci]++;
            entry->collect_lists[ci] =
                safe_realloc(entry->collect_lists[ci], (idx + SKIP_ONE) * sizeof(char *));
            entry->collect_lists[ci][idx] = heap_strdup(raw);
        }
    }
}

/* Free RETURN aggregation entries */
static void ret_agg_free(ret_agg_entry_t *aggs, int agg_count, int item_count) {
    for (int a = 0; a < agg_count; a++) {
        for (int ci = 0; ci < item_count; ci++) {
            free((void *)aggs[a].group_vals[ci]);
            for (int j = 0; j < aggs[a].collect_counts[ci]; j++) {
                free(aggs[a].collect_lists[ci][j]);
            }
            free(aggs[a].collect_lists[ci]);
        }
        free(aggs[a].group_vals);
        free(aggs[a].sums);
        free(aggs[a].counts);
        free(aggs[a].mins);
        free(aggs[a].maxs);
        free(aggs[a].collect_lists);
        free(aggs[a].collect_counts);
    }
    free(aggs);
}

/* Execute RETURN with aggregation */
/* Build group key and projected values for one binding */
static void ret_agg_build_key(cbm_return_clause_t *ret, binding_t *b, char *key, size_t key_sz,
                              const char **vals) {
    int klen = 0;
    for (int ci = 0; ci < ret->count; ci++) {
        if (ret->items[ci].func) {
            vals[ci] = "0";
            continue;
        }
        char func_buf[CBM_SZ_512];
        vals[ci] = project_item(b, &ret->items[ci], func_buf, sizeof(func_buf));
        klen += snprintf(key + klen, key_sz - (size_t)klen, "%s|", vals[ci]);
        if (klen >= (int)key_sz) {
            klen = (int)key_sz - SKIP_ONE;
        }
    }
}

/* Emit one aggregated row into the result builder */
static void ret_agg_emit_row(cbm_return_clause_t *ret, ret_agg_entry_t *agg, result_builder_t *rb) {
    const char *row[CBM_SZ_32];
    char bufs[CBM_SZ_32][CBM_SZ_64];
    for (int ci = 0; ci < ret->count; ci++) {
        if (!ret->items[ci].func) {
            row[ci] = agg->group_vals[ci];
            continue;
        }
        format_agg_value(ret->items[ci].func, agg->counts[ci], agg->sums[ci], agg->mins[ci],
                         agg->maxs[ci], agg->collect_lists, agg->collect_counts, ci, bufs[ci],
                         sizeof(bufs[ci]));
        row[ci] = bufs[ci];
    }
    rb_add_row(rb, row);
}

static void execute_return_agg(cbm_return_clause_t *ret, binding_t *bindings, int bind_count,
                               result_builder_t *rb) {
    int agg_cap = CBM_SZ_256;
    ret_agg_entry_t *aggs = calloc(agg_cap, sizeof(ret_agg_entry_t));
    int agg_count = 0;

    for (int bi = 0; bi < bind_count; bi++) {
        char key[CBM_SZ_1K] = "";
        const char *vals[CBM_SZ_32];
        ret_agg_build_key(ret, &bindings[bi], key, sizeof(key), vals);

        int found = CYP_FOUND_NONE;
        for (int a = 0; a < agg_count; a++) {
            if (strcmp(aggs[a].group_key, key) == 0) {
                found = a;
                break;
            }
        }
        if (found < 0) {
            if (agg_count >= agg_cap) {
                agg_cap *= PAIR_LEN;
                aggs = safe_realloc(aggs, agg_cap * sizeof(ret_agg_entry_t));
            }
            found = agg_count++;
            ret_agg_init_group(&aggs[found], key, ret->count, vals);
        }
        ret_agg_accumulate(&aggs[found], ret, &bindings[bi]);
    }

    for (int a = 0; a < agg_count; a++) {
        ret_agg_emit_row(ret, &aggs[a], rb);
    }
    ret_agg_free(aggs, agg_count, ret->count);
}

/* Build RETURN column names from items */
static void build_return_columns(result_builder_t *rb, cbm_return_clause_t *ret) {
    const char *col_names[CBM_SZ_32];
    for (int i = 0; i < ret->count && i < CBM_SZ_32; i++) {
        cbm_return_item_t *item = &ret->items[i];
        if (item->alias) {
            col_names[i] = item->alias;
        } else if (item->func) {
            char buf[CBM_SZ_128];
            snprintf(buf, sizeof(buf), "%s(%s)", item->func, item->variable);
            col_names[i] = heap_strdup(buf);
        } else if (item->kase) {
            col_names[i] = "CASE";
        } else if (item->property) {
            char buf[CBM_SZ_128];
            snprintf(buf, sizeof(buf), "%s.%s", item->variable, item->property);
            col_names[i] = heap_strdup(buf);
        } else {
            col_names[i] = item->variable;
        }
    }
    rb_set_columns(rb, col_names, ret->count);
    for (int i = 0; i < ret->count && i < CBM_SZ_32; i++) {
        cbm_return_item_t *item = &ret->items[i];
        if (!item->alias && (item->func || (!item->kase && item->property))) {
            free((void *)col_names[i]);
        }
    }
}

/* Execute simple (non-aggregate) RETURN projection */
static void execute_return_simple(cbm_return_clause_t *ret, binding_t *bindings, int bind_count,
                                  int max_rows, result_builder_t *rb) {
    int proj_cap = max_rows;
    if (ret->limit > 0 && !ret->order_by && ret->skip <= 0) {
        proj_cap = ret->limit;
    }
    for (int bi = 0; bi < bind_count && rb->row_count < proj_cap; bi++) {
        const char *vals[CBM_SZ_32];
        char func_bufs[CBM_SZ_32][CBM_SZ_512];
        for (int ci = 0; ci < ret->count; ci++) {
            vals[ci] =
                project_item(&bindings[bi], &ret->items[ci], func_bufs[ci], sizeof(func_bufs[ci]));
        }
        rb_add_row(rb, vals);
    }
}

/* Build default 3-column headers (name, qualified_name, label) per variable */
static void build_default_columns(result_builder_t *rb, const char **vars, int vc) {
    int col_n = vc * CYP_EDGE_COLS;
    const char *col_names[CYP_COL_BUF];
    for (int v = 0; v < vc; v++) {
        char buf[CBM_SZ_128];
        snprintf(buf, sizeof(buf), "%s.name", vars[v]);
        col_names[(size_t)v * CYP_EDGE_COLS] = heap_strdup(buf);
        snprintf(buf, sizeof(buf), "%s.qualified_name", vars[v]);
        col_names[((size_t)v * CYP_EDGE_COLS) + SKIP_ONE] = heap_strdup(buf);
        snprintf(buf, sizeof(buf), "%s.label", vars[v]);
        col_names[((size_t)v * CYP_EDGE_COLS) + PAIR_LEN] = heap_strdup(buf);
    }
    rb_set_columns(rb, col_names, col_n);
    for (int i = 0; i < col_n; i++) {
        free((void *)col_names[i]);
    }
}

/* Default projection when no RETURN clause */
static void execute_default_projection(cbm_pattern_t *pat0, binding_t *bindings, int bind_count,
                                       int max_rows, result_builder_t *rb) {
    const char *vars[CYP_MAX_VARS];
    int vc = 0;
    for (int ni = 0; ni < pat0->node_count && vc < CYP_MAX_VARS; ni++) {
        if (pat0->nodes[ni].variable) {
            vars[vc++] = pat0->nodes[ni].variable;
        }
    }
    build_default_columns(rb, vars, vc);
    for (int bi = 0; bi < bind_count && rb->row_count < max_rows; bi++) {
        const char *vals[CYP_COL_BUF];
        for (int v = 0; v < vc; v++) {
            cbm_node_t *n = binding_get(&bindings[bi], vars[v]);
            vals[(size_t)v * CYP_EDGE_COLS] = n && n->name ? n->name : "";
            vals[((size_t)v * CYP_EDGE_COLS) + SKIP_ONE] =
                n && n->qualified_name ? n->qualified_name : "";
            vals[((size_t)v * CYP_EDGE_COLS) + PAIR_LEN] = n && n->label ? n->label : "";
        }
        rb_add_row(rb, vals);
    }
}

/* Cross-join node-only pattern into existing bindings */
static void cross_join_nodes(binding_t **bindings, int *bind_count, cbm_node_t *extra_nodes,
                             int extra_count, const char *nvar, bool opt) {
    binding_t *new_bindings = malloc(((*bind_count * extra_count) + SKIP_ONE) * sizeof(binding_t));
    int new_count = 0;
    for (int bi = 0; bi < *bind_count; bi++) {
        for (int ni = 0; ni < extra_count; ni++) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            binding_set(&nb, nvar, &extra_nodes[ni]);
            new_bindings[new_count++] = nb;
        }
        if (opt && extra_count == 0) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            new_bindings[new_count++] = nb;
        }
    }
    for (int bi = 0; bi < *bind_count; bi++) {
        binding_free(&(*bindings)[bi]);
    }
    free(*bindings);
    *bindings = new_bindings;
    *bind_count = new_count;
}

/* Cross-join pattern-with-rels into existing bindings */
static void cross_join_with_rels(cbm_store_t *store, cbm_pattern_t *patn, binding_t **bindings,
                                 int *bind_count, cbm_node_t *extra_nodes, int extra_count,
                                 const char *nvar, bool opt) {
    binding_t *new_bindings =
        malloc(((*bind_count * extra_count * CYP_GROWTH_10) + SKIP_ONE) * sizeof(binding_t));
    int new_count = 0;
    for (int bi = 0; bi < *bind_count; bi++) {
        for (int ni = 0; ni < extra_count; ni++) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            binding_set(&nb, nvar, &extra_nodes[ni]);
            binding_t *tmp = malloc(PAIR_LEN * sizeof(binding_t));
            tmp[0] = nb;
            int tc = SKIP_ONE;
            int tcap = SKIP_ONE;
            const char *tv = nvar;
            expand_pattern_rels(store, patn, &tmp, &tc, &tcap, &tv, opt);
            for (int ti = 0; ti < tc; ti++) {
                new_bindings[new_count++] = tmp[ti];
            }
            free(tmp);
        }
        if (opt && extra_count == 0) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            new_bindings[new_count++] = nb;
        }
    }
    for (int bi = 0; bi < *bind_count; bi++) {
        binding_free(&(*bindings)[bi]);
    }
    free(*bindings);
    *bindings = new_bindings;
    *bind_count = new_count;
}

/* Expand additional MATCH patterns (pi >= 1) */
static void expand_additional_patterns(cbm_store_t *store, cbm_query_t *q, const char *project,
                                       int max_rows, binding_t **bindings, int *bind_count,
                                       int *bind_cap) {
    for (int pi = SKIP_ONE; pi < q->pattern_count; pi++) {
        cbm_pattern_t *patn = &q->patterns[pi];
        bool opt = q->pattern_optional[pi];
        const char *nvar = patn->nodes[0].variable ? patn->nodes[0].variable : "_n_extra";
        bool start_bound = *bind_count > 0 && binding_get(&(*bindings)[0], nvar) != NULL;

        if (start_bound && patn->rel_count > 0) {
            const char *tv = nvar;
            expand_pattern_rels(store, patn, bindings, bind_count, bind_cap, &tv, opt);
        } else {
            cbm_node_t *extra_nodes = NULL;
            int extra_count = 0;
            scan_pattern_nodes(store, project, max_rows, &patn->nodes[0], &extra_nodes,
                               &extra_count);
            if (patn->rel_count == 0) {
                cross_join_nodes(bindings, bind_count, extra_nodes, extra_count, nvar, opt);
            } else {
                cross_join_with_rels(store, patn, bindings, bind_count, extra_nodes, extra_count,
                                     nvar, opt);
            }
            cbm_store_free_nodes(extra_nodes, extra_count);
        }
    }
}

/* Project RETURN clause results */
static void execute_return_clause(cbm_query_t *q, cbm_return_clause_t *ret, binding_t *bindings,
                                  int bind_count, int max_rows, result_builder_t *rb) {
    bool has_agg = false;
    for (int i = 0; i < ret->count; i++) {
        if (is_aggregate_func(ret->items[i].func)) {
            has_agg = true;
            break;
        }
    }

    if (ret->star) {
        execute_return_star(q, bindings, bind_count, max_rows, rb);
    } else {
        build_return_columns(rb, ret);
        if (has_agg) {
            execute_return_agg(ret, bindings, bind_count, rb);
        } else {
            execute_return_simple(ret, bindings, bind_count, max_rows, rb);
        }
    }

    rb_apply_order_by(rb, ret);
    rb_apply_skip_limit(rb, ret->skip, ret->limit > 0 ? ret->limit : max_rows);
    if (ret->distinct) {
        rb_apply_distinct(rb);
    }
}

static int execute_single(cbm_store_t *store, cbm_query_t *q, const char *project, int max_rows,
                          result_builder_t *rb) {
    cbm_pattern_t *pat0 = &q->patterns[0];

    /* Step 1: Scan initial nodes */
    cbm_node_t *scanned = NULL;
    int scan_count = 0;
    scan_pattern_nodes(store, project, max_rows, &pat0->nodes[0], &scanned, &scan_count);

    /* Build initial bindings with early WHERE */
    int bind_cap = scan_count > 0 ? scan_count : SKIP_ONE;
    binding_t *bindings = malloc((bind_cap + SKIP_ONE) * sizeof(binding_t));
    int bind_count = 0;
    const char *var_name = pat0->nodes[0].variable ? pat0->nodes[0].variable : "_n0";

    for (int i = 0; i < scan_count && bind_count < bind_cap; i++) {
        binding_t b = {0};
        b.store = store;
        binding_set(&b, var_name, &scanned[i]);
        bool pass = !q->where || eval_where(q->where, &b);
        if (pass) {
            bindings[bind_count++] = b;
        } else {
            binding_free(&b);
        }
    }

    /* Step 2: Expand first pattern's relationships */
    expand_pattern_rels(store, pat0, &bindings, &bind_count, &bind_cap, &var_name,
                        q->pattern_optional[0]);

    /* Step 2b: Additional patterns */
    expand_additional_patterns(store, q, project, max_rows, &bindings, &bind_count, &bind_cap);

    /* Step 3: Late WHERE */
    if (q->where && (pat0->rel_count > 0 || q->pattern_count > SKIP_ONE)) {
        filter_bindings_where(q->where, bindings, &bind_count);
    }

    /* Step 3b: WITH clause */
    execute_with_clause(q, &bindings, &bind_count);

    /* Step 4: Project results */
    rb_init(rb);
    if (q->ret) {
        execute_return_clause(q, q->ret, bindings, bind_count, max_rows, rb);
    } else {
        execute_default_projection(pat0, bindings, bind_count, max_rows, rb);
    }

    for (int bi = 0; bi < bind_count; bi++) {
        binding_free(&bindings[bi]);
    }
    free(bindings);
    cbm_store_free_nodes(scanned, scan_count);
    return 0;
}

/* ── Main entry point ─────────────────────────────────────────── */

int cbm_cypher_execute(cbm_store_t *store, const char *query, const char *project, int max_rows,
                       cbm_cypher_result_t *out) {
    memset(out, 0, sizeof(*out));
    if (max_rows <= 0) {
        max_rows = CYPHER_RESULT_CEILING;
    }

    cbm_query_t *q = NULL;
    char *err = NULL;
    if (cbm_cypher_parse(query, &q, &err) < 0) {
        out->error = err;
        return CBM_NOT_FOUND;
    }

    result_builder_t rb = {0};
    // cppcheck-suppress knownConditionTrueFalse
    if (execute_single(store, q, project, max_rows, &rb) < 0) {
        cbm_query_free(q);
        return CBM_NOT_FOUND;
    }

    /* UNION chain */
    cbm_query_t *uq = q->union_next;
    while (uq) {
        result_builder_t rb2 = {0};
        // cppcheck-suppress knownConditionTrueFalse
        if (execute_single(store, uq, project, max_rows, &rb2) < 0) {
            rb_free(&rb);
            rb_free(&rb2);
            cbm_query_free(q);
            return CBM_NOT_FOUND;
        }
        /* Concatenate rows from rb2 into rb */
        for (int i = 0; i < rb2.row_count; i++) {
            rb_add_row(&rb, rb2.rows[i]);
        }
        rb_free(&rb2);

        uq = uq->union_next;
    }

    /* UNION (not ALL) deduplication */
    if (q->union_next && !q->union_all) {
        rb_apply_distinct(&rb);
    }

    /* Check ceiling */
    if (rb.row_count >= CYPHER_RESULT_CEILING) {
        rb_free(&rb);
        cbm_query_free(q);
        out->error = heap_strdup("result exceeded 100k rows — use narrower filters or add LIMIT");
        return CBM_NOT_FOUND;
    }

    out->columns = rb.columns;
    out->col_count = rb.col_count;
    out->rows = rb.rows;
    out->row_count = rb.row_count;

    cbm_query_free(q);
    return 0;
}

void cbm_cypher_result_free(cbm_cypher_result_t *r) {
    if (!r) {
        return;
    }
    for (int i = 0; i < r->col_count; i++) {
        free((void *)r->columns[i]);
    }
    free(r->columns);
    for (int i = 0; i < r->row_count; i++) {
        for (int j = 0; j < r->col_count; j++) {
            free((void *)r->rows[i][j]);
        }
        free(r->rows[i]);
    }
    free(r->rows);
    free(r->error);
    memset(r, 0, sizeof(*r));
}
