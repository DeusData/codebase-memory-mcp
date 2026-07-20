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
#include "foundation/limits.h"
#include "foundation/log.h"

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
#include <limits.h>
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
        safe_str_free(&r->tokens[i].text);
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
                    safe_str_free(&arr[i].key);
                    safe_str_free(&arr[i].value);
                }
                free(arr);
                return CBM_NOT_FOUND;
            }
            arr = tmp;
            cap = new_cap;
        }
        const char *new_key = heap_strdup(key->text);
        const char *new_val = heap_strdup(val->text);
        if (!new_key || !new_val) {
            safe_str_free(&new_key);
            safe_str_free(&new_val);
            for (int i = 0; i < n; i++) {
                safe_str_free(&arr[i].key);
                safe_str_free(&arr[i].value);
            }
            free(arr);
            return CBM_NOT_FOUND;
        }
        arr[n].key = new_key;
        arr[n].value = new_val;
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

    /* Optional :Label, with openCypher label alternation :A|B|C (#242).
     * Stored as a single "A|B|C" string; the matcher splits on '|'. */
    if (match(p, TOK_COLON)) {
        const cbm_token_t *label = expect(p, TOK_IDENT);
        if (!label) {
            return CBM_NOT_FOUND;
        }
        char lbuf[CBM_SZ_256];
        int ll = snprintf(lbuf, sizeof(lbuf), "%s", label->text);
        while (match(p, TOK_PIPE)) {
            const cbm_token_t *alt = expect(p, TOK_IDENT);
            if (!alt) {
                return CBM_NOT_FOUND;
            }
            int w = snprintf(lbuf + ll, (ll < (int)sizeof(lbuf)) ? sizeof(lbuf) - (size_t)ll : 0,
                             "|%s", alt->text);
            if (w > 0) {
                ll += w;
            }
            if (ll >= (int)sizeof(lbuf)) {
                break; /* buffer full */
            }
        }
        out->label = heap_strdup(lbuf);
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
    const char *first_type = heap_strdup(t->text);
    if (!first_type) {
        free(types);
        return CBM_NOT_FOUND;
    }
    types[n++] = first_type;

    while (match(p, TOK_PIPE)) {
        t = expect(p, TOK_IDENT);
        if (!t) {
            for (int i = 0; i < n; i++) {
                safe_str_free(&types[i]);
            }
            free(types);
            return CBM_NOT_FOUND;
        }
        if (n >= cap) {
            int new_cap = cap * PAIR_LEN;
            void *tmp = realloc(types, new_cap * sizeof(const char *));
            if (!tmp) {
                for (int i = 0; i < n; i++) {
                    safe_str_free(&types[i]);
                }
                free(types);
                return CBM_NOT_FOUND;
            }
            types = (const char **)tmp;
            cap = new_cap;
        }
        const char *next_type = heap_strdup(t->text);
        if (!next_type) {
            for (int i = 0; i < n; i++) {
                safe_str_free(&types[i]);
            }
            free(types);
            return CBM_NOT_FOUND;
        }
        types[n++] = next_type;
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
            safe_str_free(&cur->cond.variable);
            safe_str_free(&cur->cond.property);
            safe_str_free(&cur->cond.op);
            safe_str_free(&cur->cond.value);
            safe_str_free(&cur->cond.coalesce_default);
            for (int i = 0; i < cur->cond.in_value_count; i++) {
                safe_str_free(&cur->cond.in_values[i]);
            }
            free(cur->cond.in_values);
            safe_str_free(&cur->cond.func);
            for (int i = 0; i < cur->cond.arg_count; i++) {
                safe_str_free(&cur->cond.args[i].variable);
                safe_str_free(&cur->cond.args[i].property);
                safe_str_free(&cur->cond.args[i].literal);
            }
            free(cur->cond.args);
        }
        if (cur->right) {
            if (top < EXPR_FREE_STACK) {
                stack[top++] = cur->right;
            } else {
                expr_free(cur->right); /* recurse when stack overflows */
            }
        }
        if (cur->left) {
            if (top < EXPR_FREE_STACK) {
                stack[top++] = cur->left;
            } else {
                expr_free(cur->left); /* recurse when stack overflows */
            }
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
/* Multi-arg scalar function support, shared with the RETURN-item parser (#874) */
static bool is_multiarg_func_call(parser_t *p);
static int parse_multiarg_func_item(parser_t *p, cbm_return_item_t *item);

/* Free a multi-arg function argument array. */
static void func_args_free(cbm_func_arg_t *args, int count) {
    for (int i = 0; i < count; i++) {
        safe_str_free(&args[i].variable);
        safe_str_free(&args[i].property);
        safe_str_free(&args[i].literal);
    }
    free(args);
}

/* Free the fields of a partially-parsed multi-arg function item. */
static void func_item_fields_free(cbm_return_item_t *item) {
    safe_str_free(&item->variable);
    safe_str_free(&item->property);
    safe_str_free(&item->func);
    func_args_free(item->args, item->arg_count);
    item->args = NULL;
    item->arg_count = 0;
}

/* Free the function-call fields of a WHERE condition (#874). */
static void cond_func_fields_free(cbm_condition_t *c) {
    safe_str_free(&c->func);
    func_args_free(c->args, c->arg_count);
    c->args = NULL;
    c->arg_count = 0;
}

/* Parse IN [val, val, ...] list. Returns expr_leaf or NULL on error. */
static cbm_expr_t *parse_in_list(parser_t *p, cbm_condition_t *c) {
    advance(p);
    c->op = heap_strdup("IN");
    if (!c->op) {
        safe_str_free(&c->variable);
        safe_str_free(&c->property);
        return NULL;
    }
    if (!expect(p, TOK_LBRACKET)) {
        safe_str_free(&c->variable);
        safe_str_free(&c->property);
        safe_str_free(&c->op);
        return NULL;
    }
    int vcap = CYP_INIT_CAP8;
    int vn = 0;
    const char **vals = malloc(vcap * sizeof(const char *));
    if (!vals) {
        safe_str_free(&c->variable);
        safe_str_free(&c->property);
        safe_str_free(&c->op);
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
                        safe_str_free(&vals[i]);
                    }
                    safe_free(vals);
                    safe_str_free(&c->variable);
                    safe_str_free(&c->property);
                    safe_str_free(&c->op);
                    return NULL;
                }
                vals = (const char **)tmp;
                vcap = new_vcap;
            }
            const char *new_val = heap_strdup(advance(p)->text);
            if (!new_val) {
                for (int i = 0; i < vn; i++) {
                    safe_str_free(&vals[i]);
                }
                safe_free(vals);
                safe_str_free(&c->variable);
                safe_str_free(&c->property);
                safe_str_free(&c->op);
                return NULL;
            }
            vals[vn++] = new_val;
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
/* Free the heap fields of a standalone node pattern (not owned by a pattern). */
static void free_one_node_pattern(cbm_node_pattern_t *n) {
    safe_str_free(&n->variable);
    safe_str_free(&n->label);
    for (int j = 0; j < n->prop_count; j++) {
        safe_str_free(&n->props[j].key);
        safe_str_free(&n->props[j].value);
    }
    free(n->props);
    memset(n, 0, sizeof(*n));
}

/* Free the heap fields of a standalone relationship pattern. */
static void free_one_rel_pattern(cbm_rel_pattern_t *r) {
    safe_str_free(&r->variable);
    for (int j = 0; j < r->type_count; j++) {
        safe_str_free(&r->types[j]);
    }
    free(r->types);
    safe_str_free(&r->direction);
    memset(r, 0, sizeof(*r));
}

/* Parse a bounded EXISTS predicate: EXISTS { (anchor)-[:TYPE]->() } — a
 * single-hop, edge-type-specific existence check anchored on a bound variable
 * (e.g. WHERE NOT EXISTS { (f)<-[:CALLS]-() } finds functions with no callers).
 * Multi-hop / nested-WHERE EXISTS is intentionally unsupported. */
static cbm_expr_t *parse_exists_predicate(parser_t *p, bool negated) {
    advance(p); /* EXISTS */
    if (!match(p, TOK_LBRACE)) {
        snprintf(p->error, sizeof(p->error), "expected '{' after EXISTS at pos %d", peek(p)->pos);
        return NULL;
    }
    cbm_node_pattern_t anchor = {0};
    cbm_rel_pattern_t rel = {0};
    cbm_node_pattern_t far = {0};
    if (parse_node(p, &anchor) < 0 || parse_rel(p, &rel) < 0 || parse_node(p, &far) < 0) {
        free_one_node_pattern(&anchor);
        free_one_rel_pattern(&rel);
        free_one_node_pattern(&far);
        snprintf(p->error, sizeof(p->error),
                 "unsupported EXISTS pattern — only the single-hop form "
                 "'(var)-[:TYPE]->()' is supported");
        return NULL;
    }
    expect(p, TOK_RBRACE);

    cbm_condition_t c = {0};
    c.negated = negated;
    c.op = heap_strdup("EXISTS");
    c.variable = heap_strdup(anchor.variable ? anchor.variable : "");
    c.value = (rel.type_count > 0 && rel.types[0]) ? heap_strdup(rel.types[0]) : NULL;
    c.exists_dir = (rel.direction && strcmp(rel.direction, "inbound") == 0) ? 1
                   : (rel.direction && strcmp(rel.direction, "any") == 0)   ? 2
                                                                            : 0;
    free_one_node_pattern(&anchor);
    free_one_rel_pattern(&rel);
    free_one_node_pattern(&far);
    return expr_leaf(c);
}

/* Parse the operator + value tail shared by every condition subject
 * (var[.prop] and multi-arg functions like coalesce(...)): IS [NOT] NULL,
 * IN [...], or a comparison operator with a literal value. */
static cbm_expr_t *parse_condition_op(parser_t *p, cbm_condition_t *c) {
    /* IS NULL / IS NOT NULL */
    if (check(p, TOK_IS)) {
        advance(p);
        if (match(p, TOK_NOT)) {
            c->op = heap_strdup("IS NOT NULL");
            expect(p, TOK_NULL_KW);
        } else {
            expect(p, TOK_NULL_KW);
            c->op = heap_strdup("IS NULL");
        }
        return expr_leaf(*c);
    }

    /* IN [...] */
    if (check(p, TOK_IN)) {
        cbm_expr_t *e = parse_in_list(p, c);
        if (!e) {
            cond_func_fields_free(c);
        }
        return e;
    }

    /* Standard operators */
    c->op = parse_comparison_op(p);
    if (!c->op) {
        snprintf(p->error, sizeof(p->error), "unexpected operator at pos %d", peek(p)->pos);
        cond_func_fields_free(c);
        safe_str_free(&c->variable);
        safe_str_free(&c->property);
        safe_str_free(&c->coalesce_default);
        return NULL;
    }

    /* Value */
    if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
        c->value = heap_strdup(advance(p)->text);
    } else if (check(p, TOK_TRUE)) {
        advance(p);
        c->value = heap_strdup("true");
    } else if (check(p, TOK_FALSE)) {
        advance(p);
        c->value = heap_strdup("false");
    } else {
        snprintf(p->error, sizeof(p->error), "expected value at pos %d", peek(p)->pos);
        cond_func_fields_free(c);
        safe_str_free(&c->variable);
        safe_str_free(&c->property);
        safe_str_free(&c->op);
        safe_str_free(&c->coalesce_default);
        return NULL;
    }

    return expr_leaf(*c);
}

/* parse_condition_lhs result: the label-test form is a complete condition. */
enum { COND_LHS_COMPLETE = 1 };

/* Parse the left-hand side of a WHERE condition into c.
 * Returns CBM_NOT_FOUND on error, 0 when an operator/value should follow, and
 * COND_LHS_COMPLETE when the condition is already complete (label test). */
static int parse_condition_lhs(parser_t *p, cbm_condition_t *c) {
    if (is_multiarg_func_call(p)) {
        /* Multi-arg scalar function LHS: coalesce(f.depth, 0) >= 2 (#874).
         * Reuse the RETURN-item parser, then move ownership into the condition. */
        cbm_return_item_t fitem;
        memset(&fitem, 0, sizeof(fitem));
        if (parse_multiarg_func_item(p, &fitem) < 0) {
            func_item_fields_free(&fitem);
            return CBM_NOT_FOUND;
        }
        c->variable = fitem.variable;
        c->property = fitem.property;
        c->func = fitem.func;
        c->args = fitem.args;
        c->arg_count = fitem.arg_count;
        return 0;
    }

    if (check(p, TOK_IDENT) && p->pos + SKIP_ONE < p->count &&
        p->tokens[p->pos + SKIP_ONE].type == TOK_LPAREN) {
        /* Unrecognised function call in WHERE — fail loudly with the supported
         * set instead of the misleading "unexpected operator" (#874). */
        snprintf(p->error, sizeof(p->error),
                 "unsupported function '%s' in WHERE (supported: coalesce, substring, replace, "
                 "left, right)",
                 peek(p)->text);
        return CBM_NOT_FOUND;
    }

    const cbm_token_t *var = expect(p, TOK_IDENT);
    if (!var) {
        return CBM_NOT_FOUND;
    }

    /* Label test: WHERE n:Label (openCypher, #241). Modelled as a leaf with
     * op="HAS_LABEL" and value=Label, evaluated against the bound node's label. */
    if (check(p, TOK_COLON)) {
        advance(p);
        const cbm_token_t *lbl = expect(p, TOK_IDENT);
        if (!lbl) {
            return CBM_NOT_FOUND;
        }
        c->variable = heap_strdup(var->text);
        c->op = heap_strdup("HAS_LABEL");
        c->value = heap_strdup(lbl->text);
        return COND_LHS_COMPLETE;
    }

    if (match(p, TOK_DOT)) {
        const cbm_token_t *prop = expect(p, TOK_IDENT);
        if (!prop) {
            return CBM_NOT_FOUND;
        }
        c->variable = heap_strdup(var->text);
        c->property = heap_strdup(prop->text);
    } else {
        /* No dot: bare alias (e.g. post-WITH variable like "cnt") */
        c->variable = heap_strdup(var->text);
        c->property = NULL;
    }
    return 0;
}

static cbm_expr_t *parse_condition_expr(parser_t *p) {
    /* Check for NOT prefix at condition level (e.g. NOT n.name CONTAINS "x") */
    bool negated = match(p, TOK_NOT);

    /* EXISTS { pattern } predicate (anchored single-hop existence). */
    if (check(p, TOK_EXISTS)) {
        return parse_exists_predicate(p, negated);
    }

    cbm_condition_t c = {0};
    c.negated = negated;

    int lhs_rc = parse_condition_lhs(p, &c);
    if (lhs_rc < 0) {
        return NULL;
    }
    if (lhs_rc > 0) {
        /* HAS_LABEL leaf — complete condition, no operator follows */
        return expr_leaf(c);
    }

    return parse_condition_op(p, &c);
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
                safe_str_free(&then_val);
                for (int i = 0; i < kase->branch_count; i++) {
                    expr_free(kase->branches[i].when_expr);
                    safe_str_free(&kase->branches[i].then_val);
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
/* ASCII case-insensitive string equality. */
static bool cyp_ci_eq(const char *a, const char *b) {
    if (!a || !b) {
        return false;
    }
    for (; *a && *b; a++, b++) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) {
            return false;
        }
    }
    return *a == '\0' && *b == '\0';
}

/* Canonical name for a single-argument scalar / entity-introspection function
 * invoked by identifier — labels/type/id/keys/properties and the numeric/bool
 * casts toInteger/toFloat/toBoolean — or NULL if unrecognised (case-insensitive).
 * toLower/toUpper/toString are separate keyword tokens handled elsewhere. */
static const char *scalar_func_canonical(const char *s) {
    static const char *const names[] = {
        "labels", "type",   "id",   "keys",  "properties", "toInteger", "toFloat", "toBoolean",
        "size",   "length", "trim", "ltrim", "rtrim",      "reverse",   NULL};
    for (int i = 0; names[i]; i++) {
        if (cyp_ci_eq(s, names[i])) {
            return names[i];
        }
    }
    return NULL;
}

/* True for single-argument functions that transform a scalar string value
 * (vs. entity-introspection funcs that act on the bound node/edge). */
static bool is_scalar_value_func(const char *f) {
    return f && (strcmp(f, "toLower") == 0 || strcmp(f, "toUpper") == 0 ||
                 strcmp(f, "toString") == 0 || strcmp(f, "toInteger") == 0 ||
                 strcmp(f, "toFloat") == 0 || strcmp(f, "toBoolean") == 0 ||
                 strcmp(f, "size") == 0 || strcmp(f, "length") == 0 || strcmp(f, "trim") == 0 ||
                 strcmp(f, "ltrim") == 0 || strcmp(f, "rtrim") == 0 || strcmp(f, "reverse") == 0);
}

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

/* True if the cursor is at `IDENT(` where IDENT is a supported scalar function. */
static bool is_named_func_call(parser_t *p) {
    if (!check(p, TOK_IDENT) || p->pos + SKIP_ONE >= p->count) {
        return false;
    }
    if (p->tokens[p->pos + SKIP_ONE].type != TOK_LPAREN) {
        return false;
    }
    return scalar_func_canonical(peek(p)->text) != NULL;
}

/* Parse a single-argument scalar / introspection call: labels(n), type(r),
 * id(n), keys(n), properties(n), toInteger(n.start_line), ... */
static int parse_named_func_item(parser_t *p, cbm_return_item_t *item) {
    const char *canon = scalar_func_canonical(peek(p)->text);
    advance(p); /* consume the function name */
    expect(p, TOK_LPAREN);
    if (parse_var_dot_prop(p, item) < 0) {
        return CBM_NOT_FOUND;
    }
    expect(p, TOK_RPAREN);
    item->func = heap_strdup(canon);
    return 0;
}

/* Canonical name for a multi-argument scalar function, or NULL. */
static const char *multiarg_func_canonical(const char *s) {
    static const char *const names[] = {"coalesce", "substring", "replace", "left", "right", NULL};
    for (int i = 0; names[i]; i++) {
        if (cyp_ci_eq(s, names[i])) {
            return names[i];
        }
    }
    return NULL;
}

static bool is_multiarg_func_call(parser_t *p) {
    if (!check(p, TOK_IDENT) || p->pos + SKIP_ONE >= p->count) {
        return false;
    }
    if (p->tokens[p->pos + SKIP_ONE].type != TOK_LPAREN) {
        return false;
    }
    return multiarg_func_canonical(peek(p)->text) != NULL;
}

/* Parse one function argument: a string/number literal or a var[.prop]. */
static int parse_func_arg(parser_t *p, cbm_func_arg_t *arg) {
    memset(arg, 0, sizeof(*arg));
    if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
        arg->literal = heap_strdup(peek(p)->text);
        advance(p);
        return 0;
    }
    const cbm_token_t *var = expect(p, TOK_IDENT);
    if (!var) {
        return CBM_NOT_FOUND;
    }
    arg->variable = heap_strdup(var->text);
    if (match(p, TOK_DOT)) {
        const cbm_token_t *prop = expect(p, TOK_IDENT);
        if (prop) {
            arg->property = heap_strdup(prop->text);
        }
    }
    return 0;
}

/* Parse a multi-argument scalar call: coalesce(a, b, ...), substring(s, i[, n]),
 * replace(s, from, to), left(s, n), right(s, n). */
static int parse_multiarg_func_item(parser_t *p, cbm_return_item_t *item) {
    const char *canon = multiarg_func_canonical(peek(p)->text);
    advance(p); /* function name */
    expect(p, TOK_LPAREN);
    int cap = CYP_INIT_CAP4;
    item->args = malloc((size_t)cap * sizeof(cbm_func_arg_t));
    item->arg_count = 0;
    while (!check(p, TOK_RPAREN) && !check(p, TOK_EOF)) {
        if (item->arg_count > 0 && !match(p, TOK_COMMA)) {
            break;
        }
        if (item->arg_count >= cap) {
            cap *= PAIR_LEN;
            item->args = safe_realloc(item->args, (size_t)cap * sizeof(cbm_func_arg_t));
        }
        if (parse_func_arg(p, &item->args[item->arg_count]) < 0) {
            return CBM_NOT_FOUND;
        }
        item->arg_count++;
    }
    expect(p, TOK_RPAREN);
    item->func = heap_strdup(canon);
    /* Surface the first variable arg as variable/property for column naming. */
    if (item->arg_count > 0 && item->args[0].variable) {
        item->variable = heap_strdup(item->args[0].variable);
        if (item->args[0].property) {
            item->property = heap_strdup(item->args[0].property);
        }
    }
    return 0;
}

/* Parse aggregate function call: COUNT(var.prop) */
static int parse_aggregate_item(parser_t *p, cbm_return_item_t *item) {
    cbm_token_type_t ft = peek(p)->type;
    advance(p);
    expect(p, TOK_LPAREN);
    /* Optional DISTINCT inside the call: COUNT(DISTINCT x) (#239). */
    item->distinct = match(p, TOK_DISTINCT);
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

static void set_unsupported_list_index_error(parser_t *p) {
    snprintf(p->error, sizeof(p->error),
             "unsupported expression: list indexing/slicing '[...]' is not supported; return "
             "labels(n) AS labels directly, or group scalar node labels with RETURN n.label AS "
             "label, count(*) AS node_count ORDER BY node_count DESC LIMIT 5");
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
    } else if (is_multiarg_func_call(p)) {
        rc = parse_multiarg_func_item(p, item);
    } else if (is_named_func_call(p)) {
        rc = parse_named_func_item(p, item);
    } else {
        rc = parse_var_dot_prop(p, item);
    }
    if (rc < 0) {
        return CBM_NOT_FOUND;
    }
    /* A bare identifier followed by '(' is a function we don't recognise
     * (recognised aggregates / string funcs / scalar funcs are handled above),
     * and '[' begins list indexing/slicing we don't support. Rather than
     * silently projecting an empty column — which looks like a valid but blank
     * result and hides the real problem — fail loudly with a clear message so
     * the caller knows the query used an unsupported feature (#373). */
    if (!item->func && !item->kase && (check(p, TOK_LPAREN) || check(p, TOK_LBRACKET))) {
        if (check(p, TOK_LPAREN)) {
            snprintf(p->error, sizeof(p->error),
                     "unsupported function '%s' (supported: count, sum, avg, min, max, collect, "
                     "toLower, toUpper, toString, toInteger, toFloat, toBoolean, size, length, "
                     "trim, ltrim, rtrim, reverse, labels, type, id, keys, properties)",
                     item->variable ? item->variable : "?");
        } else {
            set_unsupported_list_index_error(p);
        }
        safe_str_free(&item->variable);
        safe_str_free(&item->property);
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

static void free_return_clause(cbm_return_clause_t *r);

/* Parse one ORDER BY expression. */
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

static int parse_order_by_clause(parser_t *p, cbm_return_clause_t *r) {
    if (!expect(p, TOK_BY)) {
        return CBM_NOT_FOUND;
    }
    do {
        if (r->order_count >= CBM_SZ_32) {
            snprintf(p->error, sizeof(p->error), "ORDER BY supports at most %d expressions",
                     CBM_SZ_32);
            return CBM_NOT_FOUND;
        }
        if (r->order_count > 0 && !match(p, TOK_COMMA)) {
            break;
        }
        char order_buf[CBM_SZ_256] = "";
        parse_order_by_expr(p, order_buf, sizeof(order_buf));
        if (!order_buf[0]) {
            snprintf(p->error, sizeof(p->error), "expected ORDER BY expression");
            return CBM_NOT_FOUND;
        }
        r->order_items = safe_realloc(r->order_items, (size_t)(r->order_count + SKIP_ONE) *
                                                          sizeof(cbm_order_item_t));
        cbm_order_item_t *item = &r->order_items[r->order_count++];
        item->expression = heap_strdup(order_buf);
        item->direction = NULL;
        if (match(p, TOK_ASC)) {
            item->direction = heap_strdup("ASC");
        } else if (match(p, TOK_DESC)) {
            item->direction = heap_strdup("DESC");
        }
    } while (check(p, TOK_COMMA));
    return 0;
}

static bool order_expression_is_projected(const cbm_return_clause_t *r, const char *expression,
                                          bool is_with) {
    if (r->star) {
        return true;
    }
    for (int i = 0; i < r->count; i++) {
        const cbm_return_item_t *item = &r->items[i];
        if (item->alias && strcmp(item->alias, expression) == 0) {
            return true;
        }
        if (is_with && !item->func && !item->kase && !item->property) {
            const char *node_name = item->alias ? item->alias : item->variable;
            size_t node_len = node_name ? strlen(node_name) : 0;
            if (node_len > 0 && strncmp(expression, node_name, node_len) == 0 &&
                expression[node_len] == '.') {
                return true;
            }
        }
        char projected[CBM_SZ_256] = "";
        if (item->func) {
            snprintf(projected, sizeof(projected), "%s(%s)", item->func,
                     item->variable ? item->variable : "");
        } else if (item->kase) {
            snprintf(projected, sizeof(projected), "CASE");
        } else if (item->property) {
            snprintf(projected, sizeof(projected), "%s.%s", item->variable, item->property);
        } else if (item->variable) {
            snprintf(projected, sizeof(projected), "%s", item->variable);
        }
        if (strcmp(projected, expression) == 0) {
            return true;
        }
    }
    return false;
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
    /* -1 = no LIMIT clause (return all). An explicit `LIMIT 0` parses to 0 below
     * and must return 0 rows — distinguishing the two requires a sentinel, since
     * calloc zeroes limit and `limit > 0` would treat LIMIT 0 as "no limit". */
    r->limit = -1;
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

    /* Projection is materialized per row into fixed-width stack arrays sized at
     * CBM_SZ_32 columns (execute_return_simple and its siblings). Bound the
     * parsed item count to that width so an over-wide RETURN is rejected here
     * instead of writing past those arrays downstream. */
    if (r->count > CBM_SZ_32) {
        free(r->items);
        free(r);
        return CBM_NOT_FOUND;
    }

tail:
    /* Optional ORDER BY */
    if (match(p, TOK_ORDER)) {
        if (parse_order_by_clause(p, r) < 0) {
            free_return_clause(r);
            return CBM_NOT_FOUND;
        }
        for (int i = 0; i < r->order_count; i++) {
            if (!order_expression_is_projected(r, r->order_items[i].expression, is_with)) {
                snprintf(p->error, sizeof(p->error),
                         "ORDER BY expression '%s' is not projected; add it to %s or assign an "
                         "alias and order by that alias",
                         r->order_items[i].expression, is_with ? "WITH" : "RETURN");
                free_return_clause(r);
                return CBM_NOT_FOUND;
            }
        }
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

/* Parse a complete query from the current token through EOF and transfer its
 * ownership to the caller. Both WITH stage chaining and UNION use this path so
 * recursive cursor/error ownership cannot drift between the two constructs. */
static int parse_query_remainder(parser_t *p, cbm_query_t **out) { // NOLINT(misc-no-recursion)
    cbm_parse_result_t sub = {0};
    if (cbm_parse(&p->tokens[p->pos], p->count - p->pos, &sub) < 0) {
        if (sub.error) {
            snprintf(p->error, sizeof(p->error), "%s", sub.error);
        }
        cbm_parse_free(&sub);
        return CBM_NOT_FOUND;
    }
    *out = sub.query;
    sub.query = NULL;
    cbm_parse_free(&sub);
    p->pos = p->count - SKIP_ONE;
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
        /* WITH is a scope and cardinality boundary. Parse the following MATCH
         * part as its own query stage so execution consumes the projected rows
         * instead of rescanning the graph or retaining de-scoped variables. */
        if (check(p, TOK_MATCH) || check(p, TOK_OPTIONAL)) {
            if (parse_query_remainder(p, &q->next_stage) < 0) {
                return CBM_NOT_FOUND;
            }
            /* UNION separates complete queries, not WITH stages. The recursive
             * parser initially encounters it at the terminal stage; promote
             * ownership to this query root so the existing UNION executor and
             * destructor consume every branch exactly once. */
            cbm_query_t *terminal = q->next_stage;
            while (terminal->next_stage) {
                terminal = terminal->next_stage;
            }
            if (terminal->union_next) {
                q->union_next = terminal->union_next;
                q->union_all = terminal->union_all;
                terminal->union_next = NULL;
                terminal->union_all = false;
            }
            return 0;
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
        if (parse_query_remainder(p, &q->union_next) < 0) {
            return CBM_NOT_FOUND;
        }
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

    if (!check(&p, TOK_EOF)) {
        if (check(&p, TOK_LBRACKET)) {
            set_unsupported_list_index_error(&p);
            out->error = heap_strdup(p.error);
        } else {
            snprintf(p.error, sizeof(p.error), "unexpected trailing token '%s' at pos %d",
                     peek(&p)->text, peek(&p)->pos);
            out->error = heap_strdup(p.error);
        }
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
        safe_str_free(&n->variable);
        safe_str_free(&n->label);
        for (int j = 0; j < n->prop_count; j++) {
            safe_str_free(&n->props[j].key);
            safe_str_free(&n->props[j].value);
        }
        free(n->props);
    }
    free(pat->nodes);
    for (int i = 0; i < pat->rel_count; i++) {
        cbm_rel_pattern_t *r = &pat->rels[i];
        safe_str_free(&r->variable);
        for (int j = 0; j < r->type_count; j++) {
            safe_str_free(&r->types[j]);
        }
        free(r->types);
        safe_str_free(&r->direction);
    }
    free(pat->rels);
}

static void free_where(cbm_where_clause_t *w) {
    if (!w) {
        return;
    }
    expr_free(w->root);
    for (int i = 0; i < w->count; i++) {
        safe_str_free(&w->conditions[i].variable);
        safe_str_free(&w->conditions[i].property);
        safe_str_free(&w->conditions[i].op);
        safe_str_free(&w->conditions[i].value);
        for (int j = 0; j < w->conditions[i].in_value_count; j++) {
            safe_str_free(&w->conditions[i].in_values[j]);
        }
        free(w->conditions[i].in_values);
        safe_str_free(&w->conditions[i].func);
        func_args_free(w->conditions[i].args, w->conditions[i].arg_count);
    }
    free(w->conditions);
    safe_str_free(&w->op);
    free(w);
}

static void free_case_expr(cbm_case_expr_t *k) {
    if (!k) {
        return;
    }
    for (int i = 0; i < k->branch_count; i++) {
        expr_free(k->branches[i].when_expr);
        safe_str_free(&k->branches[i].then_val);
    }
    free(k->branches);
    safe_str_free(&k->else_val);
    free(k);
}

static void free_return_clause(cbm_return_clause_t *r) {
    if (!r) {
        return;
    }
    for (int i = 0; i < r->count; i++) {
        safe_str_free(&r->items[i].variable);
        safe_str_free(&r->items[i].property);
        safe_str_free(&r->items[i].alias);
        safe_str_free(&r->items[i].func);
        free_case_expr(r->items[i].kase);
        for (int j = 0; j < r->items[i].arg_count; j++) {
            safe_str_free(&r->items[i].args[j].variable);
            safe_str_free(&r->items[i].args[j].property);
            safe_str_free(&r->items[i].args[j].literal);
        }
        free(r->items[i].args);
    }
    free(r->items);
    for (int i = 0; i < r->order_count; i++) {
        safe_str_free(&r->order_items[i].expression);
        safe_str_free(&r->order_items[i].direction);
    }
    free(r->order_items);
    free(r);
}

void cbm_query_free(cbm_query_t *q) {
    while (q) {
        cbm_query_t *next_union = q->union_next;
        cbm_query_t *stage = q;
        while (stage) {
            cbm_query_t *next_stage = stage->next_stage;
            for (int i = 0; i < stage->pattern_count; i++) {
                free_pattern(&stage->patterns[i]);
            }
            free(stage->patterns);
            free(stage->pattern_optional);
            free_where(stage->where);
            free_where(stage->post_with_where);
            free_return_clause(stage->with_clause);
            free_return_clause(stage->ret);
            safe_str_free(&stage->unwind_expr);
            safe_str_free(&stage->unwind_alias);
            free(stage);
            stage = next_stage;
        }
        q = next_union;
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
    bool var_name_owned[CYP_MAX_VARS];   /* WITH aliases are heap-owned */
    bool var_is_null[CYP_MAX_VARS];      /* projected null is distinct from an empty string */
    cbm_node_t var_nodes[CYP_MAX_VARS];  /* node data */
    int var_count;
    const char *edge_var_names[CYP_MAX_EDGE_VARS]; /* variable names (edges) */
    cbm_edge_t edge_vars[CYP_MAX_EDGE_VARS];       /* edge data */
    int edge_var_count;
    cbm_store_t *store; /* for computing in_degree/out_degree on demand */
    const char *project; /* borrowed project filter for active overlay qn-keyed lookups */
    bool use_active_overlay_edges;
} binding_t;

static void binding_free(binding_t *b);

/* Per-execution state: query execution is re-entrant across server threads,
 * while a ceiling hit must never be reported by another request. */
static _Thread_local int g_cypher_row_ceiling_hit = 0;

/* Grow an owning binding array without losing the existing rows on OOM.
 * The caller retains and frees the old allocation when growth fails. */
static bool binding_array_reserve(binding_t **rows, int *capacity, int needed, int limit) {
    if (needed <= *capacity) {
        return true;
    }
    int next = *capacity > 0 ? *capacity : CYP_INIT_CAP8;
    while (next < needed && next < limit) {
        next = next > limit / PAIR_LEN ? limit : next * PAIR_LEN;
    }
    if (next < needed) {
        return false;
    }
    void *grown = realloc(*rows, (size_t)next * sizeof(**rows));
    if (!grown) {
        return false;
    }
    *rows = grown;
    *capacity = next;
    return true;
}

/* Move one owned binding into a ceiling-bounded geometric array. On failure,
 * release the row here so every caller has the same ownership contract. */
static bool binding_array_append(binding_t **rows, int *count, int *capacity, int limit,
                                 binding_t *row) {
    if (*count >= limit) {
        g_cypher_row_ceiling_hit = limit;
        binding_free(row);
        return false;
    }
    if (!binding_array_reserve(rows, capacity, *count + SKIP_ONE, limit)) {
        binding_free(row);
        return false;
    }
    (*rows)[(*count)++] = *row;
    memset(row, 0, sizeof(*row));
    return true;
}

/* Return a string field from a node by property name.  NULL-safe. */
static const char *node_string_field(const cbm_node_t *n, const char *prop, bool *is_null) {
    static const struct {
        const char *key;
        size_t offset;
    } fields[] = {
        {"name", offsetof(cbm_node_t, name)},
        {"qualified_name", offsetof(cbm_node_t, qualified_name)},
        /* Aliases: field-eval agents reach for the short names, and a miss
         * used to return a silent empty column costing a round-trip. */
        {"qn", offsetof(cbm_node_t, qualified_name)},
        {"label", offsetof(cbm_node_t, label)},
        {"file_path", offsetof(cbm_node_t, file_path)},
        {"file", offsetof(cbm_node_t, file_path)},
        {"path", offsetof(cbm_node_t, file_path)},
    };
    for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); i++) {
        if (strcmp(prop, fields[i].key) == 0) {
            const char *val = *(const char **)((const char *)n + fields[i].offset);
            /* SQLite-backed optional core columns are normalized to an empty
             * string on read, which is their established null representation.
             * Dynamic JSON properties retain an exact empty-vs-null distinction
             * and are handled separately by json_extract_prop_ex(). */
            *is_null = val == NULL || val[0] == '\0';
            return val ? val : "";
        }
    }
    return NULL;
}

/* Get node property by name.
 * store may be NULL; only needed for virtual degree properties. */
static const char *json_extract_prop_ex(const char *json, const char *key, char *buf, size_t buf_sz,
                                        bool *is_null);
static void node_fields_free(cbm_node_t *n); /* defined below; used by the stub re-fetch */

static const char *node_prop_ex(const cbm_node_t *n, const char *prop, cbm_store_t *store,
                                const char *project, bool use_active_overlay_edges, bool *is_null) {
    *is_null = true;
    if (!n || !prop) {
        return "";
    }
    const char *str = node_string_field(n, prop, is_null);
    bool may_be_projected_stub = store && n->id > 0 && !n->file_path && !n->label;
    if (str && (!*is_null || !may_be_projected_stub)) {
        return str;
    }
    /* Computed and JSON-derived values live in rotating thread-local buffers:
     * a single row (or an ORDER-BY comparison) reads several of these before any
     * of them is copied out, so returning one shared static buffer would alias
     * every column to the last value read. Mirrors edge_prop's rotation. */
    static _Thread_local char bufs[CYP_BUF_8][CBM_SZ_512];
    static _Thread_local int buf_idx = 0;
    char *out = bufs[buf_idx];
    buf_idx = (buf_idx + SKIP_ONE) % CYP_BUF_8;

    if (strcmp(prop, "start_line") == 0) {
        snprintf(out, CBM_SZ_512, "%d", n->start_line);
        *is_null = false;
        return out;
    }
    if (strcmp(prop, "end_line") == 0) {
        snprintf(out, CBM_SZ_512, "%d", n->end_line);
        *is_null = false;
        return out;
    }
    /* Virtual computed properties: in_degree/out_degree via the same
     * all-but-INHERITS edge contract as cbm_store_node_degree(). */
    if (store && (strcmp(prop, "in_degree") == 0 || strcmp(prop, "out_degree") == 0)) {
        int in_deg = 0;
        int out_deg = 0;
        if (use_active_overlay_edges && project && n->qualified_name && n->qualified_name[0]) {
            (void)cbm_store_active_node_degree_by_qn(store, project, n->qualified_name, &in_deg,
                                                     &out_deg);
        } else {
            cbm_store_node_degree(store, n->id, &in_deg, &out_deg);
        }
        int val = (strcmp(prop, "in_degree") == 0) ? in_deg : out_deg;
        snprintf(out, CBM_SZ_512, "%d", val);
        *is_null = false;
        return out;
    }
    /* Fall back to any value stored in the node's properties JSON — exposes the
     * extraction metrics (complexity, cognitive, loop_count, loop_depth,
     * transitive_loop_depth, recursive) and any other persisted property to
     * WHERE/RETURN, e.g. WHERE n.loop_depth >= 2. */
    if (n->properties_json && n->properties_json[0] == '{') {
        const char *v = json_extract_prop_ex(n->properties_json, prop, out, CBM_SZ_512, is_null);
        if (!*is_null) {
            return v;
        }
    }
    /* WITH aggregation carries a node group var by id + name only (the group key
     * is the node name), so every other property is absent on the stub. Detect
     * the stub (id set, but the full string fields were never populated) and
     * re-fetch the node so RETURN g.file_path / g.label / g.<metric> project
     * correctly instead of returning blank. The gate is heuristic, not an exact
     * stub discriminator: a real bound node with NULL label AND file_path would
     * also match, but in that case the worst case is one redundant indexed fetch
     * that returns the same value — never a wrong result. */
    if (may_be_projected_stub) {
        cbm_node_t full = {0};
        if (cbm_store_find_node_by_id(store, n->id, &full) == CBM_STORE_OK) {
            bool full_is_null = true;
            const char *rv = node_prop_ex(&full, prop, NULL, project, use_active_overlay_edges,
                                          &full_is_null);
            if (!full_is_null) {
                snprintf(out, CBM_SZ_512, "%s", rv);
            }
            node_fields_free(&full);
            if (!full_is_null) {
                *is_null = false;
                return out;
            }
        }
    }
    return "";
}

static const char *node_prop(const cbm_node_t *n, const char *prop, cbm_store_t *store,
                             const char *project, bool use_active_overlay_edges) {
    bool is_null = true;
    return node_prop_ex(n, prop, store, project, use_active_overlay_edges, &is_null);
}

/* Extract a string value from JSON properties_json by key.
 * Writes result to buf (up to buf_sz). Returns buf if found, "" otherwise.
 * Handles both string values ("key":"value") and numeric values ("key":1.5). */
static const char *json_extract_prop_ex(const char *json, const char *key, char *buf, size_t buf_sz,
                                        bool *is_null) {
    *is_null = true;
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
    if (strncmp(p, "null", 4) == 0 &&
        (p[4] == ',' || p[4] == '}' || isspace((unsigned char)p[4]))) {
        buf[0] = '\0';
        return buf;
    }
    *is_null = false;
    if (*p == '"') {
        /* String value — honor backslash escapes: without this, an embedded \"
         * cuts the value short at the first escaped quote. */
        p++;
        size_t i = 0;
        while (*p && *p != '"' && i < buf_sz - SKIP_ONE) {
            if (*p == '\\' && p[SKIP_ONE] && i + SKIP_ONE < buf_sz - SKIP_ONE) {
                buf[i++] = *p++; /* keep the escape pair intact */
            }
            buf[i++] = *p++;
        }
        buf[i] = '\0';
    } else if (*p == '[' || *p == '{') {
        /* Array/object value — copy the whole balanced construct. A scan-to-comma
         * truncates at the first comma INSIDE the value: e.g. a decorators array
         * ["@Roles('OWNER', 'ADMIN')","@Get()"] came back as ["@Roles('OWNER'. */
        char open = *p;
        char close = (open == '[') ? ']' : '}';
        int depth = 0;
        int in_str = 0;
        size_t i = 0;
        while (*p && i < buf_sz - SKIP_ONE) {
            char c = *p;
            if (in_str) {
                if (c == '\\' && p[SKIP_ONE] && i + SKIP_ONE < buf_sz - SKIP_ONE) {
                    buf[i++] = *p++; /* escape pair stays intact */
                } else if (c == '"') {
                    in_str = 0;
                }
            } else if (c == '"') {
                in_str = 1;
            } else if (c == open) {
                depth++;
            } else if (c == close) {
                depth--;
            }
            buf[i++] = *p++;
            if (!in_str && depth == 0) {
                break; /* outer bracket closed */
            }
        }
        buf[i] = '\0';
    } else {
        /* Numeric or other scalar value */
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
static const char *edge_prop_ex(const cbm_edge_t *e, const char *prop, bool *is_null) {
    *is_null = true;
    if (!e || !prop) {
        return "";
    }
    if (strcmp(prop, "type") == 0) {
        *is_null = e->type == NULL;
        return e->type ? e->type : "";
    }
    /* Rotate through per-thread buffers so columns cannot alias and concurrent
     * MCP requests cannot race over projection scratch storage. */
    static _Thread_local char ebufs[CYP_BUF_8][CBM_SZ_512];
    static _Thread_local int ebuf_idx = 0;
    char *buf = ebufs[ebuf_idx++ & CYP_EBUF_MASK];
    json_extract_prop_ex(e->properties_json, prop, buf, CBM_SZ_512, is_null);
    return buf;
}

static const char *edge_prop(const cbm_edge_t *e, const char *prop) {
    bool is_null = true;
    return edge_prop_ex(e, prop, &is_null);
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
    safe_str_free(&n->project);
    safe_str_free(&n->label);
    safe_str_free(&n->name);
    safe_str_free(&n->qualified_name);
    safe_str_free(&n->file_path);
    safe_str_free(&n->properties_json);
}

/* Deep copy an edge (binding owns the strings) */
static void edge_deep_copy(cbm_edge_t *dst, const cbm_edge_t *src) {
    *dst = *src;
    dst->project = heap_strdup(src->project);
    dst->type = heap_strdup(src->type);
    dst->properties_json = heap_strdup(src->properties_json);
}

static void edge_fields_free(cbm_edge_t *e) {
    safe_str_free(&e->project);
    safe_str_free(&e->type);
    safe_str_free(&e->properties_json);
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
        if (b->var_name_owned[i]) {
            free((void *)b->var_names[i]);
            b->var_names[i] = NULL;
            b->var_name_owned[i] = false;
        }
    }
    for (int i = 0; i < b->edge_var_count; i++) {
        edge_fields_free(&b->edge_vars[i]);
    }
}

/* Deep-copy a binding (so source and dest own separate string copies) */
static void binding_copy(binding_t *dst, const binding_t *src) {
    dst->var_count = src->var_count;
    for (int i = 0; i < src->var_count; i++) {
        node_deep_copy(&dst->var_nodes[i], &src->var_nodes[i]);
        dst->var_name_owned[i] = src->var_name_owned[i];
        dst->var_is_null[i] = src->var_is_null[i];
        dst->var_names[i] = src->var_name_owned[i] ? heap_strdup(src->var_names[i])
                                                   : src->var_names[i];
    }
    dst->edge_var_count = src->edge_var_count;
    for (int i = 0; i < src->edge_var_count; i++) {
        dst->edge_var_names[i] = src->edge_var_names[i]; /* AST-owned */
        edge_deep_copy(&dst->edge_vars[i], &src->edge_vars[i]);
    }
    dst->store = src->store;
    dst->project = src->project;
    dst->use_active_overlay_edges = src->use_active_overlay_edges;
}

/* Deep-copy a node into a binding (binding owns the strings) */
static void binding_set(binding_t *b, const char *var, const cbm_node_t *node) {
    /* Check existing — free old fields first */
    for (int i = 0; i < b->var_count; i++) {
        if (strcmp(b->var_names[i], var) == 0) {
            node_fields_free(&b->var_nodes[i]);
            node_deep_copy(&b->var_nodes[i], node);
            b->var_is_null[i] = false;
            return;
        }
    }
    if (b->var_count >= CYP_MAX_VARS) {
        return;
    }
    b->var_names[b->var_count] = var; /* not owned — points to AST string */
    b->var_name_owned[b->var_count] = false;
    b->var_is_null[b->var_count] = false;
    node_deep_copy(&b->var_nodes[b->var_count], node);
    b->var_count++;
}

static const char *binding_get_virtual_ex(binding_t *b, const char *var, const char *prop,
                                          bool *is_null);
static const char *eval_multiarg_func(binding_t *b, const cbm_return_item_t *item, char *buf,
                                      size_t bufsz, bool *is_null);

/* Resolve the actual property value and preserve the Cypher distinction between
 * null and a valid empty string. This is the shared lookup used by projection,
 * aggregation, WHERE, and scalar functions. */
static const char *resolve_condition_value(const cbm_condition_t *c, binding_t *b,
                                           bool *is_null) {
    *is_null = true;
    /* Multi-arg scalar function LHS: coalesce(f.depth, 0) >= 2 (#874).
     * Evaluated through the same code path as RETURN projections. The value is
     * consumed by eval_condition before any other condition is resolved, so a
     * single thread-local buffer per call is safe. */
    if (c->func) {
        static _Thread_local char func_buf[CBM_SZ_512];
        cbm_return_item_t item = {0};
        item.variable = c->variable;
        item.property = c->property;
        item.func = c->func;
        item.args = c->args;
        item.arg_count = c->arg_count;
        return eval_multiarg_func(b, &item, func_buf, sizeof(func_buf), is_null);
    }

    cbm_edge_t *e = binding_get_edge(b, c->variable);
    if (e) {
        if (c->property) {
            return edge_prop_ex(e, c->property, is_null);
        }
        *is_null = false;
        return e->properties_json ? e->properties_json : "{}";
    }
    cbm_node_t *n = binding_get(b, c->variable);
    if (!n) {
        return ""; /* unbound variable */
    }
    if (c->property) {
        return node_prop_ex(n, c->property, b->store, b->project, b->use_active_overlay_edges,
                            is_null);
    }
    /* Bare alias (e.g. post-WITH virtual var) — use node name directly */
    *is_null = false;
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
    /* Label test: WHERE n:Label (#241) — compare the bound node's label
     * directly rather than a property value. */
    if (strcmp(c->op, "HAS_LABEL") == 0) {
        cbm_node_t *n = binding_get(b, c->variable);
        bool result = n && n->label && c->value && strcmp(n->label, c->value) == 0;
        return c->negated ? !result : result;
    }

    /* EXISTS { (var)-[:TYPE]->() }: does the bound node have any edge of the
     * given type in the requested direction? (dir 0=out, 1=in, 2=any) */
    if (strcmp(c->op, "EXISTS") == 0) {
        cbm_node_t *n = binding_get(b, c->variable);
        bool result = false;
        if (n && b->store) {
            if (b->use_active_overlay_edges && b->project && n->qualified_name &&
                n->qualified_name[0]) {
                int dir = c->exists_dir == CBM_STORE_EDGE_DIR_INBOUND
                              ? CBM_STORE_EDGE_DIR_INBOUND
                              : (c->exists_dir == CBM_STORE_EDGE_DIR_ANY
                                     ? CBM_STORE_EDGE_DIR_ANY
                                     : CBM_STORE_EDGE_DIR_OUTBOUND);
                (void)cbm_store_active_edge_exists_by_qn(b->store, b->project,
                                                         n->qualified_name, c->value, dir,
                                                         &result);
            } else {
                cbm_edge_t *edges = NULL;
                int cnt = 0;
                if (c->exists_dir != CBM_STORE_EDGE_DIR_INBOUND) { /* outbound or any */
                    if (c->value) {
                        cbm_store_find_edges_by_source_type(b->store, n->id, c->value, &edges,
                                                            &cnt);
                    } else {
                        cbm_store_find_edges_by_source(b->store, n->id, &edges, &cnt);
                    }
                    result = cnt > 0;
                    cbm_store_free_edges(edges, cnt);
                }
                if (!result && c->exists_dir != CBM_STORE_EDGE_DIR_OUTBOUND) { /* inbound or any */
                    edges = NULL;
                    cnt = 0;
                    if (c->value) {
                        cbm_store_find_edges_by_target_type(b->store, n->id, c->value, &edges,
                                                            &cnt);
                    } else {
                        cbm_store_find_edges_by_target(b->store, n->id, &edges, &cnt);
                    }
                    result = cnt > 0;
                    cbm_store_free_edges(edges, cnt);
                }
            }
        }
        return c->negated ? !result : result;
    }

    bool actual_is_null = true;
    const char *actual = resolve_condition_value(c, b, &actual_is_null);
    /* Legacy two-argument coalesce representation: fall back only for null,
     * never for a present empty string. */
    if (c->coalesce_default && actual_is_null) {
        actual = c->coalesce_default;
        actual_is_null = false;
    }

    bool result;

    /* IS NULL / IS NOT NULL */
    if (strcmp(c->op, "IS NULL") == 0) {
        result = actual_is_null;
        return c->negated ? !result : result;
    }
    if (strcmp(c->op, "IS NOT NULL") == 0) {
        result = !actual_is_null;
        return c->negated ? !result : result;
    }
    /* A null comparison is unknown and therefore does not pass WHERE. */
    if (actual_is_null) {
        return false;
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

typedef enum { CYP_PARTIAL_FALSE = 0, CYP_PARTIAL_TRUE, CYP_PARTIAL_UNKNOWN } cypher_partial_bool_t;

static cypher_partial_bool_t partial_and(cypher_partial_bool_t left, cypher_partial_bool_t right) {
    if (left == CYP_PARTIAL_FALSE || right == CYP_PARTIAL_FALSE) {
        return CYP_PARTIAL_FALSE;
    }
    return left == CYP_PARTIAL_TRUE && right == CYP_PARTIAL_TRUE ? CYP_PARTIAL_TRUE
                                                                 : CYP_PARTIAL_UNKNOWN;
}

static cypher_partial_bool_t partial_or(cypher_partial_bool_t left, cypher_partial_bool_t right) {
    if (left == CYP_PARTIAL_TRUE || right == CYP_PARTIAL_TRUE) {
        return CYP_PARTIAL_TRUE;
    }
    return left == CYP_PARTIAL_FALSE && right == CYP_PARTIAL_FALSE ? CYP_PARTIAL_FALSE
                                                                   : CYP_PARTIAL_UNKNOWN;
}

/* Evaluate the portion of a WHERE expression whose aliases are already
 * bound. Unknown leaves keep the seed; definitively false source predicates
 * still prune before relationship expansion. Per seed this is O(expression
 * nodes) time, O(expression depth) stack, and allocation-free; AND/OR retain
 * the full evaluator's safe short-circuit behavior. */
static cypher_partial_bool_t eval_expr_partial(const cbm_expr_t *e, // NOLINT(misc-no-recursion)
                                               binding_t *b) {
    if (!e) {
        return CYP_PARTIAL_TRUE;
    }
    if (e->type == EXPR_CONDITION) {
        if (!binding_get(b, e->cond.variable) && !binding_get_edge(b, e->cond.variable)) {
            return CYP_PARTIAL_UNKNOWN;
        }
        return eval_condition(&e->cond, b) ? CYP_PARTIAL_TRUE : CYP_PARTIAL_FALSE;
    }

    cypher_partial_bool_t left = eval_expr_partial(e->left, b);
    if (e->type == EXPR_NOT) {
        return left == CYP_PARTIAL_UNKNOWN
                   ? CYP_PARTIAL_UNKNOWN
                   : (left == CYP_PARTIAL_TRUE ? CYP_PARTIAL_FALSE : CYP_PARTIAL_TRUE);
    }
    if (e->type == EXPR_AND && left == CYP_PARTIAL_FALSE) {
        return CYP_PARTIAL_FALSE;
    }
    if (e->type == EXPR_OR && left == CYP_PARTIAL_TRUE) {
        return CYP_PARTIAL_TRUE;
    }
    cypher_partial_bool_t right = eval_expr_partial(e->right, b);
    if (e->type == EXPR_AND) {
        return partial_and(left, right);
    }
    if (e->type == EXPR_OR) {
        return partial_or(left, right);
    }
    if (left == CYP_PARTIAL_UNKNOWN || right == CYP_PARTIAL_UNKNOWN) {
        return CYP_PARTIAL_UNKNOWN;
    }
    return left != right ? CYP_PARTIAL_TRUE : CYP_PARTIAL_FALSE;
}

static cypher_partial_bool_t eval_where_partial(const cbm_where_clause_t *where, binding_t *b) {
    if (!where) {
        return CYP_PARTIAL_TRUE;
    }
    if (where->root) {
        return eval_expr_partial(where->root, b);
    }
    cypher_partial_bool_t result =
        where->op && strcmp(where->op, "AND") == 0 ? CYP_PARTIAL_TRUE : CYP_PARTIAL_FALSE;
    for (int i = 0; i < where->count; i++) {
        cypher_partial_bool_t item =
            (!binding_get(b, where->conditions[i].variable) &&
             !binding_get_edge(b, where->conditions[i].variable))
                ? CYP_PARTIAL_UNKNOWN
                : (eval_condition(&where->conditions[i], b) ? CYP_PARTIAL_TRUE : CYP_PARTIAL_FALSE);
        result = where->op && strcmp(where->op, "AND") == 0 ? partial_and(result, item)
                                                            : partial_or(result, item);
    }
    return result;
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
        const char *actual = node_prop(n, props[i].key, store, NULL, false);
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

typedef enum {
    CYP_NODE_SCAN_CANONICAL = 0,
    CYP_NODE_SCAN_ACTIVE_OVERLAY
} cypher_node_scan_mode_t;

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

/* Wall-clock execution deadline (#601). The row ceiling above only fires once
 * rows exist, but an unbounded `OPTIONAL MATCH` over the full node set (or a
 * GROUP BY with ~one group per node) does O(bindings x groups) work and can run
 * for minutes — exhausting RAM/CPU — before a single row is produced, so the
 * ceiling never trips and the caller just hangs. A monotonic deadline aborts
 * such runaway queries with a clear, actionable error. Checked (throttled) in
 * the scan, expansion and aggregation hot loops. */
#define CYPHER_DEADLINE_BUDGET_MS 30000  /* 30s: generous for legit heavy queries */
#define CYPHER_DEADLINE_CHECK_MASK 0x3FF /* sample the clock every 1024 iterations */

static _Thread_local uint64_t g_cypher_deadline_ms = 0; /* absolute; 0 = disarmed */
static _Thread_local bool g_cypher_timed_out = false;
static _Thread_local int64_t g_cypher_deadline_override_ms = -1; /* test hook; <0 = default */

static void cypher_deadline_arm(void) {
    g_cypher_timed_out = false;
    int64_t budget = g_cypher_deadline_override_ms >= 0 ? g_cypher_deadline_override_ms
                                                        : CYPHER_DEADLINE_BUDGET_MS;
    g_cypher_deadline_ms = cbm_now_ms() + (uint64_t)budget;
}

/* True once the query has run past its wall-clock budget. Sticky: after the
 * first trip every subsequent call returns true, so later loops short-circuit. */
static bool cypher_deadline_exceeded(void) {
    if (g_cypher_timed_out) {
        return true;
    }
    if (g_cypher_deadline_ms == 0) {
        return false;
    }
    if (cbm_now_ms() >= g_cypher_deadline_ms) {
        g_cypher_timed_out = true;
        return true;
    }
    return false;
}

/* Test-only: force the execution budget (ms) for subsequent queries on this
 * thread. 0 = trip on the first hot-loop check; <0 restores the default. */
void cbm_cypher_test_set_deadline_ms(int64_t budget_ms) {
    g_cypher_deadline_override_ms = budget_ms;
}

/* ── Binding virtual variables (for WITH clause) ──────────────── */

static const char *binding_get_virtual_ex(binding_t *b, const char *var, const char *prop,
                                          bool *is_null) {
    *is_null = true;
    if (!var) {
        return "";
    }
    /* COUNT(*) counts rows, so its synthetic argument is always non-null. */
    if (strcmp(var, "*") == 0 && !prop) {
        *is_null = false;
        return "";
    }
    /* Check virtual vars first (from WITH projection) */
    char full[CBM_SZ_256];
    if (prop) {
        snprintf(full, sizeof(full), "%s.%s", var, prop);
    } else {
        snprintf(full, sizeof(full), "%s", var);
    }
    for (int i = 0; i < b->var_count; i++) {
        if (strcmp(b->var_names[i], full) == 0) {
            *is_null = b->var_is_null[i];
            return b->var_nodes[i].name ? b->var_nodes[i].name : "";
        }
    }
    /* Fall through to normal lookup */
    cbm_edge_t *e = binding_get_edge(b, var);
    if (e) {
        /* Bare `RETURN r` on an edge: surface the full properties JSON
         * (or "{}" if none) so callers can inspect timestamps, weights,
         * etc. without naming each property. */
        if (prop) {
            return edge_prop_ex(e, prop, is_null);
        }
        *is_null = false;
        return e->properties_json ? e->properties_json : "{}";
    }
    cbm_node_t *n = binding_get(b, var);
    if (n) {
        if (prop) {
            return node_prop_ex(n, prop, b->store, b->project, b->use_active_overlay_edges,
                                is_null);
        }
        *is_null = false;
        return n->name ? n->name : "";
    }
    return "";
}

static const char *binding_get_virtual(binding_t *b, const char *var, const char *prop) {
    bool is_null = true;
    return binding_get_virtual_ex(b, var, prop, &is_null);
}

/* Append one aggregation grouping component. Entity values group by canonical
 * store identity, not display name; scalar values use a length prefix so a
 * delimiter inside user data cannot merge otherwise distinct tuples. */
static int group_key_append(char *key, size_t key_sz, int pos, binding_t *binding,
                            const char *var, const char *prop, const char *value, bool is_null) {
    if ((size_t)pos >= key_sz - SKIP_ONE) {
        return (int)key_sz - SKIP_ONE;
    }
    size_t remaining = key_sz - (size_t)pos;
    int written = 0;
    if (!prop) {
        cbm_node_t *node = binding_get(binding, var);
        if (node && node->id > 0) {
            written = snprintf(key + pos, remaining, "N:%lld|", (long long)node->id);
        } else {
            cbm_edge_t *edge = binding_get_edge(binding, var);
            if (edge && edge->id > 0) {
                written = snprintf(key + pos, remaining, "E:%lld|", (long long)edge->id);
            }
        }
    }
    if (written == 0) {
        written = is_null ? snprintf(key + pos, remaining, "Z|")
                          : snprintf(key + pos, remaining, "V:%zu:%s|", strlen(value), value);
    }
    if (written < 0) {
        return pos;
    }
    if ((size_t)written >= remaining) {
        return (int)key_sz - SKIP_ONE;
    }
    return pos + written;
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
    if (strcmp(func, "toInteger") == 0) {
        char *end = NULL;
        long long v = strtoll(val, &end, CBM_DECIMAL_BASE);
        if (end == val) {
            /* Not an integer literal — accept a float string and truncate. */
            char *fend = NULL;
            double d = strtod(val, &fend);
            if (fend == val) {
                return ""; /* non-numeric → null */
            }
            v = (long long)d;
        }
        snprintf(buf, buf_sz, "%lld", v);
        return buf;
    }
    if (strcmp(func, "toFloat") == 0) {
        char *end = NULL;
        double d = strtod(val, &end);
        if (end == val) {
            return ""; /* non-numeric → null */
        }
        snprintf(buf, buf_sz, "%g", d);
        return buf;
    }
    if (strcmp(func, "toBoolean") == 0) {
        if (cyp_ci_eq(val, "true")) {
            return "true";
        }
        if (cyp_ci_eq(val, "false")) {
            return "false";
        }
        return ""; /* not a boolean → null */
    }
    if (strcmp(func, "size") == 0 || strcmp(func, "length") == 0) {
        snprintf(buf, buf_sz, "%zu", strlen(val));
        return buf;
    }
    if (strcmp(func, "trim") == 0 || strcmp(func, "ltrim") == 0 || strcmp(func, "rtrim") == 0) {
        bool do_left = (strcmp(func, "trim") == 0 || strcmp(func, "ltrim") == 0);
        bool do_right = (strcmp(func, "trim") == 0 || strcmp(func, "rtrim") == 0);
        const char *start = val;
        const char *end = val + strlen(val);
        while (do_left && (*start == ' ' || *start == '\t' || *start == '\n' || *start == '\r')) {
            start++;
        }
        while (do_right && end > start &&
               (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' || end[-1] == '\r')) {
            end--;
        }
        size_t n = (size_t)(end - start);
        if (n >= buf_sz) {
            n = buf_sz - SKIP_ONE;
        }
        memcpy(buf, start, n);
        buf[n] = '\0';
        return buf;
    }
    if (strcmp(func, "reverse") == 0) {
        size_t len = strlen(val);
        if (len >= buf_sz) {
            len = buf_sz - SKIP_ONE;
        }
        for (size_t i = 0; i < len; i++) {
            buf[i] = val[len - SKIP_ONE - i];
        }
        buf[len] = '\0';
        return buf;
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

/* True if `actual` matches `pat`, where `pat` may be a '|'-alternation of
 * labels ("A|B|C") — openCypher label alternation (#242). */
static bool label_alt_matches(const char *actual, const char *pat) {
    if (!pat) {
        return true;
    }
    if (!actual) {
        return false;
    }
    if (!strchr(pat, '|')) {
        return strcmp(actual, pat) == 0;
    }
    size_t al = strlen(actual);
    const char *seg = pat;
    while (*seg) {
        const char *bar = strchr(seg, '|');
        size_t seglen = bar ? (size_t)(bar - seg) : strlen(seg);
        if (seglen == al && strncmp(seg, actual, seglen) == 0) {
            return true;
        }
        if (!bar) {
            break;
        }
        seg = bar + SKIP_ONE;
    }
    return false;
}

/* Seed nodes for a label alternation "A|B|C": union the per-label results.
 * Node-struct fields are moved (shallow) into out_nodes; each per-label array
 * container is freed. */
static void scan_alternation_labels(cbm_store_t *store, const char *project, const char *labels,
                                    cypher_node_scan_mode_t scan_mode, cbm_node_t **out_nodes,
                                    int *out_count) {
    *out_nodes = NULL;
    *out_count = 0;
    int cap = 0;
    char *copy = heap_strdup(labels);
    if (!copy) {
        return;
    }
    char *save = NULL;
    for (char *tok = strtok_r(copy, "|", &save); tok; tok = strtok_r(NULL, "|", &save)) {
        cbm_node_t *part = NULL;
        int pc = 0;
        if (scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY) {
            cbm_store_find_nodes_by_label_overlay_view(store, project, tok, &part, &pc);
        } else {
            cbm_store_find_nodes_by_label(store, project, tok, &part, &pc);
        }
        if (pc > 0 && part) {
            if (*out_count + pc > cap) {
                cap = (*out_count + pc) * PAIR_LEN;
                *out_nodes = safe_realloc(*out_nodes, (size_t)cap * sizeof(cbm_node_t));
            }
            memcpy(*out_nodes + *out_count, part, (size_t)pc * sizeof(cbm_node_t));
            *out_count += pc;
        }
        free(part); /* container only — node fields moved to out_nodes */
    }
    free(copy);
}

static const char *condition_file_contains_value(const cbm_condition_t *cond,
                                                 const char *variable) {
    if (!cond || !variable || cond->negated || cond->func || cond->coalesce_default ||
        cond->arg_count != 0 || !cond->variable || strcmp(cond->variable, variable) != 0 ||
        !cond->property || strcmp(cond->property, "file_path") != 0 || !cond->op ||
        strcmp(cond->op, "CONTAINS") != 0) {
        return NULL;
    }
    return cond->value;
}

/* Find a literal file-path CONTAINS predicate that is a mandatory conjunct of
 * the initial node match. Predicates below OR/XOR/NOT are not mandatory and
 * must remain C-only filters. The executor still evaluates the complete WHERE
 * tree after the store scan, so this is a candidate reduction, not a second
 * semantic authority. */
static void find_file_contains_conjunct(const cbm_expr_t *expr, const char *variable,
                                        const char **out_value) {
    if (!expr || !variable || !out_value || *out_value) {
        return;
    }
    if (expr->type == EXPR_AND) {
        find_file_contains_conjunct(expr->left, variable, out_value);
        find_file_contains_conjunct(expr->right, variable, out_value);
        return;
    }
    if (expr->type != EXPR_CONDITION) {
        return;
    }
    const char *value = condition_file_contains_value(&expr->cond, variable);
    if (value) {
        *out_value = value;
    }
}

static const char *where_file_contains_conjunct(const cbm_where_clause_t *where,
                                                const char *variable) {
    if (!where || !variable) {
        return NULL;
    }
    const char *value = NULL;
    if (where->root) {
        find_file_contains_conjunct(where->root, variable, &value);
        return value;
    }
    if (where->op && strcmp(where->op, "OR") == 0) {
        return NULL;
    }
    for (int i = 0; i < where->count && !value; i++) {
        value = condition_file_contains_value(&where->conditions[i], variable);
    }
    return value;
}

static void scan_pattern_nodes(cbm_store_t *store, const char *project, int candidate_limit,
                               cbm_node_pattern_t *first, const cbm_where_clause_t *where,
                               const char *variable, cypher_node_scan_mode_t scan_mode,
                               cbm_node_t **out_nodes, int *out_count) {
    if (first->label && strchr(first->label, '|')) {
        scan_alternation_labels(store, project, first->label, scan_mode, out_nodes, out_count);
    } else if (first->label) {
        if (scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY) {
            cbm_store_find_nodes_by_label_overlay_view(store, project, first->label, out_nodes,
                                                       out_count);
        } else {
            cbm_store_find_nodes_by_label(store, project, first->label, out_nodes, out_count);
        }
    } else if (scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY) {
        cbm_store_find_nodes_by_label_overlay_view_limited(store, project, NULL, candidate_limit,
                                                           out_nodes, out_count);
    } else {
        const char *file_contains = where_file_contains_conjunct(where, variable);
        cbm_search_params_t params = {.project = project,
                                      .file_contains = file_contains,
                                      .min_degree = CYP_FOUND_NONE,
                                      .max_degree = CYP_FOUND_NONE,
                                      .limit = candidate_limit};
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
                          const char *rel_var, binding_t **new_bindings, int *new_count,
                          int *new_capacity, int max_new, int *match_count,
                          const cbm_where_clause_t *pattern_where) {
    /* When the terminal node variable is ALREADY bound (e.g. the second pattern
     * `(c)-[:CALLS]->(f)` where `f` came from an earlier MATCH), we must FILTER
     * to edges that actually reach the bound node — not overwrite the caller's
     * `f` binding with whatever node the edge leads to. Overwriting corrupted
     * the result of dead-code queries and produced wrong rows (#627). */
    cbm_node_t *bound_to = binding_get(b, to_var);
    int64_t bound_to_id = bound_to ? bound_to->id : 0;
    for (int ei = 0; ei < edge_count && *new_count < max_new; ei++) {
        int64_t tid = inbound ? edges[ei].source_id : edges[ei].target_id;
        if (bound_to && tid != bound_to_id) {
            continue;
        }
        cbm_node_t found = {0};
        if (cbm_store_find_node_by_id(store, tid, &found) != CBM_STORE_OK) {
            continue;
        }
        if (target_node->label && !label_alt_matches(found.label, target_node->label)) {
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
        if (pattern_where && !eval_where(pattern_where, &nb)) {
            binding_free(&nb);
            continue;
        }
        if (binding_array_append(new_bindings, new_count, new_capacity, max_new, &nb)) {
            (*match_count)++;
        }
    }
}

static void process_active_edge_nodes(cbm_store_edge_node_t *rows, int row_count,
                                      const cbm_node_pattern_t *target_node, binding_t *b,
                                      const char *to_var, const char *rel_var,
                                      binding_t **new_bindings, int *new_count, int *new_capacity,
                                      int max_new, int *match_count,
                                      const cbm_where_clause_t *pattern_where) {
    cbm_node_t *bound_to = binding_get(b, to_var);
    const char *bound_to_qn =
        bound_to && bound_to->qualified_name && bound_to->qualified_name[0]
            ? bound_to->qualified_name
            : NULL;
    int64_t bound_to_id = bound_to ? bound_to->id : 0;
    for (int ri = 0; ri < row_count && *new_count < max_new; ri++) {
        cbm_node_t *found = &rows[ri].node;
        if (bound_to_qn) {
            if (!found->qualified_name || strcmp(bound_to_qn, found->qualified_name) != 0) {
                continue;
            }
        } else if (bound_to && found->id != bound_to_id) {
            continue;
        }
        if (target_node->label && !label_alt_matches(found->label, target_node->label)) {
            continue;
        }
        if (!check_inline_props(found, target_node->props, target_node->prop_count, b->store)) {
            continue;
        }
        binding_t nb = {0};
        binding_copy(&nb, b);
        binding_set(&nb, to_var, found);
        if (rel_var) {
            binding_set_edge(&nb, rel_var, &rows[ri].edge);
        }
        if (pattern_where && !eval_where(pattern_where, &nb)) {
            binding_free(&nb);
            continue;
        }
        if (binding_array_append(new_bindings, new_count, new_capacity, max_new, &nb)) {
            (*match_count)++;
        }
    }
}

/* Expand variable-length relationship via BFS */
/* Set when a variable-length hop range is clamped to the engine ceiling
 * during the CURRENT execution; cbm_cypher_execute turns it into
 * result->warning so callers can tell "clamped" from "no such path" (#797). */
/* C11 _Thread_local directly: cypher.c stays windows.h-free (compat.h pulls
 * in windows.h, whose legacy `far` macro breaks this file's identifiers). */
static _Thread_local int g_cypher_depth_clamped = 0;

static void expand_var_length(cbm_store_t *store, cbm_rel_pattern_t *rel,
                              cbm_node_pattern_t *target_node, binding_t *b, cbm_node_t *src,
                              const char *to_var, binding_t **new_bindings, int *new_count,
                              int *new_capacity, int max_new, int *match_count,
                              const cbm_where_clause_t *pattern_where) {
    /* Clamp BOTH the explicit (`*1..N`) and unbounded (`*`, `*..m`) forms to the
     * engine ceiling: an explicit N above the cap was previously honoured
     * verbatim, driving cbm_store_bfs to an unbounded hop count (#887). WARN on
     * clamp — never a silent truncation. */
    int depth_cap = cbm_cypher_max_depth();
    int max_depth = rel->max_hops > 0 ? rel->max_hops : depth_cap;
    if (max_depth > depth_cap) {
        char req_buf[16];
        char cap_buf[16];
        snprintf(req_buf, sizeof(req_buf), "%d", max_depth);
        snprintf(cap_buf, sizeof(cap_buf), "%d", depth_cap);
        cbm_log_warn("cypher.depth_capped", "requested", req_buf, "cap", cap_buf);
        g_cypher_depth_clamped = depth_cap; /* surfaced as result->warning (#797) */
        max_depth = depth_cap;
    }
    const char *dir = rel->direction ? rel->direction : "outbound";
    if (b->use_active_overlay_edges && b->project && src->qualified_name &&
        src->qualified_name[0]) {
        int max_results = max_new - *new_count;
        if (max_results <= 0) {
            return;
        }
        cbm_traverse_result_t tr = {0};
        if (cbm_store_bfs_overlay_view(store, b->project, src->qualified_name, dir,
                                       (const char **)rel->types, rel->type_count, max_depth,
                                       max_results, &tr) == CBM_STORE_OK) {
            for (int v = 0; v < tr.visited_count && *new_count < max_new; v++) {
                cbm_node_hop_t *hop = &tr.visited[v];
                if (hop->hop < rel->min_hops) {
                    continue;
                }
                if (target_node->label && !label_alt_matches(hop->node.label, target_node->label)) {
                    continue;
                }
                if (!check_inline_props(&hop->node, target_node->props, target_node->prop_count,
                                        store)) {
                    continue;
                }
                binding_t nb = {0};
                binding_copy(&nb, b);
                binding_set(&nb, to_var, &hop->node);
                if (pattern_where && !eval_where(pattern_where, &nb)) {
                    binding_free(&nb);
                    continue;
                }
                if (binding_array_append(new_bindings, new_count, new_capacity, max_new, &nb)) {
                    (*match_count)++;
                }
            }
        }
        cbm_store_traverse_free(&tr);
        return;
    }
    cbm_traverse_result_t tr = {0};
    cbm_store_bfs(store, src->id, dir, rel->types, rel->type_count, max_depth, CBM_PERCENT, &tr);
    for (int v = 0; v < tr.visited_count && *new_count < max_new; v++) {
        cbm_node_hop_t *hop = &tr.visited[v];
        if (hop->hop < rel->min_hops) {
            continue;
        }
        if (target_node->label && !label_alt_matches(hop->node.label, target_node->label)) {
            continue;
        }
        if (!check_inline_props(&hop->node, target_node->props, target_node->prop_count, store)) {
            continue;
        }
        binding_t nb = {0};
        binding_copy(&nb, b);
        binding_set(&nb, to_var, &hop->node);
        if (pattern_where && !eval_where(pattern_where, &nb)) {
            binding_free(&nb);
            continue;
        }
        if (binding_array_append(new_bindings, new_count, new_capacity, max_new, &nb)) {
            (*match_count)++;
        }
    }
    cbm_store_traverse_free(&tr);
}

/* Expand fixed-length (1-hop) relationship edges */
static void expand_fixed_length(cbm_store_t *store, cbm_rel_pattern_t *rel,
                                cbm_node_pattern_t *target_node, binding_t *b, cbm_node_t *src,
                                const char *to_var, binding_t **new_bindings, int *new_count,
                                int *new_capacity, int max_new, int *match_count,
                                const cbm_where_clause_t *pattern_where) {
    bool is_inbound = rel->direction && strcmp(rel->direction, "inbound") == 0;
    bool is_any = rel->direction && strcmp(rel->direction, "any") == 0;
    const char *rel_var = rel->variable;

    if (b->use_active_overlay_edges && b->project && src->qualified_name &&
        src->qualified_name[0]) {
        int direction = is_inbound ? CBM_STORE_EDGE_DIR_INBOUND
                                   : (is_any ? CBM_STORE_EDGE_DIR_ANY
                                             : CBM_STORE_EDGE_DIR_OUTBOUND);
        cbm_store_edge_node_t *rows = NULL;
        int row_count = 0;
        if (cbm_store_find_active_edge_nodes_by_qn(store, b->project, src->qualified_name,
                                                   (const char **)rel->types, rel->type_count,
                                                   direction, &rows, &row_count) ==
            CBM_STORE_OK) {
            process_active_edge_nodes(rows, row_count, target_node, b, to_var, rel_var,
                                      new_bindings, new_count, new_capacity, max_new, match_count,
                                      pattern_where);
        }
        cbm_store_free_edge_nodes(rows, row_count);
        return;
    }

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
                          new_bindings, new_count, new_capacity, max_new, match_count,
                          pattern_where);
            cbm_store_free_edges(edges, edge_count);
        }
        if (is_any) {
            for (int ti = 0; ti < rel->type_count; ti++) {
                cbm_edge_t *edges = NULL;
                int edge_count = 0;
                cbm_store_find_edges_by_target_type(store, src->id, rel->types[ti], &edges,
                                                    &edge_count);
                process_edges(store, edges, edge_count, true, target_node, b, to_var, rel_var,
                              new_bindings, new_count, new_capacity, max_new, match_count,
                              pattern_where);
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
                      new_bindings, new_count, new_capacity, max_new, match_count, pattern_where);
        cbm_store_free_edges(edges, edge_count);
        if (is_any) {
            edges = NULL;
            edge_count = 0;
            cbm_store_find_edges_by_target(store, src->id, &edges, &edge_count);
            process_edges(store, edges, edge_count, true, target_node, b, to_var, rel_var,
                          new_bindings, new_count, new_capacity, max_new, match_count,
                          pattern_where);
            cbm_store_free_edges(edges, edge_count);
        }
    }
}

static void expand_pattern_rels(cbm_store_t *store, cbm_pattern_t *pat, binding_t **bindings,
                                int *bind_count, const char **var_name, bool is_optional,
                                const cbm_where_clause_t *pattern_where) {
    for (int ri = 0; ri < pat->rel_count; ri++) {
        /* #601: stop expanding further hops once the wall-clock budget is spent
         * (an unbounded expansion is exactly what blows up here). */
        if (cypher_deadline_exceeded()) {
            return;
        }
        cbm_rel_pattern_t *rel = &pat->rels[ri];
        cbm_node_pattern_t *target_node = &pat->nodes[ri + SKIP_ONE];
        const char *to_var = target_node->variable ? target_node->variable : "_n_t";

        bool is_variable_length = (rel->min_hops != SKIP_ONE || rel->max_hops != SKIP_ONE);

        int max_new = CYPHER_RESULT_CEILING;
        int new_capacity = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
        if (new_capacity > max_new) {
            new_capacity = max_new;
        }
        binding_t *new_bindings = malloc((size_t)new_capacity * sizeof(binding_t));
        if (!new_bindings) {
            return;
        }
        int new_count = 0;

        for (int bi = 0; bi < *bind_count; bi++) {
            if ((bi & CYPHER_DEADLINE_CHECK_MASK) == 0 && cypher_deadline_exceeded()) {
                break;
            }
            binding_t *b = &(*bindings)[bi];
            cbm_node_t *src = binding_get(b, *var_name);
            if (!src) {
                continue;
            }

            int match_count = 0;
            const cbm_where_clause_t *candidate_where =
                (ri == pat->rel_count - SKIP_ONE) ? pattern_where : NULL;

            if (is_variable_length) {
                expand_var_length(store, rel, target_node, b, src, to_var, &new_bindings,
                                  &new_count, &new_capacity, max_new, &match_count,
                                  candidate_where);
            } else {
                expand_fixed_length(store, rel, target_node, b, src, to_var, &new_bindings,
                                    &new_count, &new_capacity, max_new, &match_count,
                                    candidate_where);
            }

            /* OPTIONAL MATCH: keep binding with empty target if no matches */
            if (is_optional && match_count == 0) {
                binding_t nb = {0};
                binding_copy(&nb, b);
                /* Don't set to_var — it remains unbound; projection returns "" */
                (void)binding_array_append(&new_bindings, &new_count, &new_capacity, max_new, &nb);
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
static int rb_find_order_column(const result_builder_t *rb, const cbm_return_clause_t *ret,
                                const char *expression) {
    for (int ci = 0; ci < rb->col_count; ci++) {
        if (strcmp(rb->columns[ci], expression) == 0) {
            return ci;
        }
    }
    for (int ci = 0; ci < ret->count; ci++) {
        if (ret->items[ci].alias && strcmp(ret->items[ci].alias, expression) == 0) {
            return ci;
        }
    }
    return CBM_NOT_FOUND;
}

/* Check whether a column contains numeric data by examining the first non-empty value */
static bool rb_is_numeric_column(const result_builder_t *rb, int col) {
    bool saw_value = false;
    for (int i = 0; i < rb->row_count; i++) {
        const char *v = rb->rows[i][col];
        if (v && *v) {
            char *end = NULL;
            (void)strtod(v, &end);
            if (end == v || *end != '\0') {
                return false;
            }
            saw_value = true;
        }
    }
    return saw_value;
}

static int rb_compare_ordered_rows(const char **a, const char **b, const int *columns,
                                   const bool *numeric, const bool *descending, int key_count) {
    for (int key = 0; key < key_count; key++) {
        const char *av = a[columns[key]] ? a[columns[key]] : "";
        const char *bv = b[columns[key]] ? b[columns[key]] : "";
        bool a_null = av[0] == '\0';
        bool b_null = bv[0] == '\0';
        int cmp = 0;
        if (a_null != b_null) {
            cmp = a_null ? 1 : -1; /* null sorts last ascending */
        } else if (!a_null && numeric[key]) {
            double da = strtod(av, NULL);
            double db = strtod(bv, NULL);
            cmp = (da > db) - (da < db);
        } else if (!a_null) {
            cmp = strcmp(av, bv);
        }
        if (cmp != 0) {
            return descending[key] ? -cmp : cmp;
        }
    }
    return 0;
}

static void rb_apply_order_by(result_builder_t *rb, const cbm_return_clause_t *ret) {
    if (ret->order_count <= 0 || rb->row_count < PAIR_LEN) {
        return;
    }
    int columns[CBM_SZ_32];
    bool numeric[CBM_SZ_32];
    bool descending[CBM_SZ_32];
    int key_count = 0;
    for (int i = 0; i < ret->order_count; i++) {
        int col = rb_find_order_column(rb, ret, ret->order_items[i].expression);
        if (col < 0) {
            continue;
        }
        columns[key_count] = col;
        numeric[key_count] = rb_is_numeric_column(rb, col);
        descending[key_count] =
            ret->order_items[i].direction && strcmp(ret->order_items[i].direction, "DESC") == 0;
        key_count++;
    }
    if (key_count == 0) {
        return;
    }

    const char ***scratch = malloc((size_t)rb->row_count * sizeof(*scratch));
    if (!scratch) {
        return;
    }
    const char ***src = rb->rows;
    const char ***dst = scratch;
    for (int width = SKIP_ONE; width < rb->row_count;) {
        for (int left = 0; left < rb->row_count; left += width * PAIR_LEN) {
            int mid = left + width < rb->row_count ? left + width : rb->row_count;
            int right =
                left + width * PAIR_LEN < rb->row_count ? left + width * PAIR_LEN : rb->row_count;
            int i = left;
            int j = mid;
            int out = left;
            while (i < mid && j < right) {
                if (rb_compare_ordered_rows(src[i], src[j], columns, numeric, descending,
                                            key_count) <= 0) {
                    dst[out++] = src[i++];
                } else {
                    dst[out++] = src[j++];
                }
            }
            while (i < mid) {
                dst[out++] = src[i++];
            }
            while (j < right) {
                dst[out++] = src[j++];
            }
        }
        const char ***swap = src;
        src = dst;
        dst = swap;
        if (width > rb->row_count / PAIR_LEN) {
            break;
        }
        width *= PAIR_LEN;
    }
    if (src != rb->rows) {
        memcpy(rb->rows, src, (size_t)rb->row_count * sizeof(*rb->rows));
    }
    free(scratch);
}

static void rb_apply_skip_limit(result_builder_t *rb, int skip_n, int limit) {
    /* Skip */
    if (skip_n > 0 && skip_n < rb->row_count) {
        for (int i = 0; i < skip_n; i++) {
            for (int c = 0; c < rb->col_count; c++) {
                safe_str_free(&rb->rows[i][c]);
            }
            free(rb->rows[i]);
        }
        memmove(rb->rows, rb->rows + skip_n, (rb->row_count - skip_n) * sizeof(const char **));
        rb->row_count -= skip_n;
    } else if (skip_n >= rb->row_count) {
        for (int i = 0; i < rb->row_count; i++) {
            for (int c = 0; c < rb->col_count; c++) {
                safe_str_free(&rb->rows[i][c]);
            }
            free(rb->rows[i]);
        }
        rb->row_count = 0;
    }
    /* Limit */
    if (limit >= 0 && rb->row_count > limit) {
        for (int i = limit; i < rb->row_count; i++) {
            for (int c = 0; c < rb->col_count; c++) {
                safe_str_free(&rb->rows[i][c]);
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
                safe_str_free(&rb->rows[i][c]);
            }
            free(rb->rows[i]);
        }
    }
    rb->row_count = kept;
}

static void rb_free(result_builder_t *rb) {
    for (int i = 0; i < rb->row_count; i++) {
        for (int c = 0; c < rb->col_count; c++) {
            safe_str_free(&rb->rows[i][c]);
        }
        free(rb->rows[i]);
    }
    free(rb->rows);
    for (int i = 0; i < rb->col_count; i++) {
        safe_str_free(&rb->columns[i]);
    }
    free(rb->columns);
}

/* ── Get projection value for a binding + return item ─────────── */

/* Build a JSON list of a node's non-null property keys: keys(n). */
static const char *node_keys_list(const cbm_node_t *n, char *buf, size_t buf_sz) {
    const struct {
        const char *k;
        bool present;
    } ks[] = {
        {"name", n->name && n->name[0]},
        {"qualified_name", n->qualified_name && n->qualified_name[0]},
        {"label", n->label && n->label[0]},
        {"file_path", n->file_path && n->file_path[0]},
        {"start_line", n->start_line > 0},
        {"end_line", n->end_line > 0},
    };
    size_t pos = 0;
    bool first = true;
    if (pos < buf_sz - SKIP_ONE) {
        buf[pos++] = '[';
    }
    for (size_t i = 0; i < sizeof(ks) / sizeof(ks[0]) && pos < buf_sz - SKIP_ONE; i++) {
        if (!ks[i].present) {
            continue;
        }
        int w = snprintf(buf + pos, buf_sz - pos, "%s\"%s\"", first ? "" : ",", ks[i].k);
        if (w < 0 || (size_t)w >= buf_sz - pos) {
            break;
        }
        pos += (size_t)w;
        first = false;
    }
    if (pos < buf_sz - SKIP_ONE) {
        buf[pos++] = ']';
    }
    buf[pos] = '\0';
    return buf;
}

/* Resolve a function argument to its string value (literal or var.prop). */
static const char *eval_func_arg_ex(binding_t *b, const cbm_func_arg_t *a, bool *is_null) {
    if (a->literal) {
        *is_null = false;
        return a->literal;
    }
    return binding_get_virtual_ex(b, a->variable, a->property, is_null);
}

static const char *eval_func_arg(binding_t *b, const cbm_func_arg_t *a) {
    bool is_null = true;
    return eval_func_arg_ex(b, a, &is_null);
}

/* Evaluate a multi-argument scalar function into func_buf (or a direct value). */
static const char *eval_multiarg_func(binding_t *b, const cbm_return_item_t *item, char *buf,
                                      size_t bufsz, bool *is_null) {
    if (is_null) {
        *is_null = false;
    }
    const char *f = item->func;
    int n = item->arg_count;
    if (strcmp(f, "coalesce") == 0) {
        for (int i = 0; i < n; i++) {
            bool arg_is_null = true;
            const char *v = eval_func_arg_ex(b, &item->args[i], &arg_is_null);
            if (!arg_is_null) {
                return v;
            }
        }
        if (is_null) {
            *is_null = true;
        }
        return "";
    }
    if (strcmp(f, "substring") == 0 && n >= 2) {
        const char *s = eval_func_arg(b, &item->args[0]);
        long start = strtol(eval_func_arg(b, &item->args[1]), NULL, CBM_DECIMAL_BASE);
        size_t slen = strlen(s);
        if (start < 0 || (size_t)start >= slen) {
            return "";
        }
        size_t take = slen - (size_t)start;
        if (n >= 3) {
            long len = strtol(eval_func_arg(b, &item->args[2]), NULL, CBM_DECIMAL_BASE);
            if (len < 0) {
                len = 0;
            }
            if ((size_t)len < take) {
                take = (size_t)len;
            }
        }
        if (take >= bufsz) {
            take = bufsz - SKIP_ONE;
        }
        memcpy(buf, s + start, take);
        buf[take] = '\0';
        return buf;
    }
    if ((strcmp(f, "left") == 0 || strcmp(f, "right") == 0) && n >= 2) {
        const char *s = eval_func_arg(b, &item->args[0]);
        long k = strtol(eval_func_arg(b, &item->args[1]), NULL, CBM_DECIMAL_BASE);
        if (k < 0) {
            k = 0;
        }
        size_t slen = strlen(s);
        size_t take = (size_t)k < slen ? (size_t)k : slen;
        if (take >= bufsz) {
            take = bufsz - SKIP_ONE;
        }
        memcpy(buf, (strcmp(f, "left") == 0) ? s : s + (slen - take), take);
        buf[take] = '\0';
        return buf;
    }
    if (strcmp(f, "replace") == 0 && n >= 3) {
        const char *s = eval_func_arg(b, &item->args[0]);
        const char *from = eval_func_arg(b, &item->args[1]);
        const char *to = eval_func_arg(b, &item->args[2]);
        size_t fromlen = strlen(from);
        size_t tolen = strlen(to);
        size_t pos = 0;
        const char *pp = s;
        if (fromlen == 0) {
            snprintf(buf, bufsz, "%s", s);
            return buf;
        }
        while (*pp && pos < bufsz - SKIP_ONE) {
            if (strncmp(pp, from, fromlen) == 0) {
                size_t cpy = tolen;
                if (pos + cpy >= bufsz) {
                    cpy = bufsz - SKIP_ONE - pos;
                }
                memcpy(buf + pos, to, cpy);
                pos += cpy;
                pp += fromlen;
            } else {
                buf[pos++] = *pp++;
            }
        }
        buf[pos] = '\0';
        return buf;
    }
    return ""; /* wrong arity → null */
}

static const char *project_item(binding_t *b, cbm_return_item_t *item, char *func_buf,
                                size_t buf_sz) {
    if (item->kase) {
        return eval_case_expr(item->kase, b);
    }
    if (item->args) {
        bool is_null = true;
        return eval_multiarg_func(b, item, func_buf, buf_sz, &is_null);
    }
    /* Entity-introspection functions operate on the bound node/edge itself,
     * not on a scalar property value. */
    if (item->func) {
        if (strcmp(item->func, "labels") == 0) {
            cbm_node_t *n = binding_get(b, item->variable);
            if (n && n->label) {
                snprintf(func_buf, buf_sz, "[\"%s\"]", n->label);
                return func_buf;
            }
            return "[]";
        }
        if (strcmp(item->func, "type") == 0) {
            cbm_edge_t *e = binding_get_edge(b, item->variable);
            return (e && e->type) ? e->type : "";
        }
        if (strcmp(item->func, "id") == 0) {
            cbm_node_t *n = binding_get(b, item->variable);
            if (n) {
                snprintf(func_buf, buf_sz, "%lld", (long long)n->id);
                return func_buf;
            }
            cbm_edge_t *e = binding_get_edge(b, item->variable);
            if (e) {
                snprintf(func_buf, buf_sz, "%lld", (long long)e->id);
                return func_buf;
            }
            return "";
        }
        if (strcmp(item->func, "keys") == 0) {
            cbm_node_t *n = binding_get(b, item->variable);
            return n ? node_keys_list(n, func_buf, buf_sz) : "[]";
        }
        if (strcmp(item->func, "properties") == 0) {
            cbm_node_t *n = binding_get(b, item->variable);
            if (n) {
                return n->properties_json ? n->properties_json : "{}";
            }
            cbm_edge_t *e = binding_get_edge(b, item->variable);
            if (e) {
                return e->properties_json ? e->properties_json : "{}";
            }
            return "{}";
        }
    }
    const char *raw = binding_get_virtual(b, item->variable, item->property);
    if (is_scalar_value_func(item->func)) {
        return apply_string_func(item->func, raw, func_buf, buf_sz);
    }
    /* Copy into the caller's per-column buffer. `raw` may point to node_prop's
     * rotating scratch buffer, which the next column's projection would overwrite
     * before rb_add_row copies the assembled row — aliasing every such column to
     * the last value read. The per-column func_buf gives each column stable storage. */
    if (raw && raw != func_buf && raw[0]) {
        size_t len = strlen(raw);
        if (len >= buf_sz) {
            len = buf_sz - SKIP_ONE;
        }
        memcpy(func_buf, raw, len);
        func_buf[len] = '\0';
        return func_buf;
    }
    return raw ? raw : "";
}

/* Check if a function name is an aggregate */
static bool is_aggregate_func(const char *func) {
    return func &&
           (strcmp(func, "COUNT") == 0 || strcmp(func, "SUM") == 0 || strcmp(func, "AVG") == 0 ||
            strcmp(func, "MIN") == 0 || strcmp(func, "MAX") == 0 || strcmp(func, "COLLECT") == 0);
}

/* Append `val` to a string list only if not already present — i.e. maintain a
 * set of distinct values. Used by COUNT(DISTINCT x) (#239). */
static void distinct_list_add(char ***list, int *count, const char *val) {
    for (int i = 0; i < *count; i++) {
        if (strcmp((*list)[i], val) == 0) {
            return;
        }
    }
    int idx = (*count)++;
    *list = safe_realloc(*list, (size_t)(idx + SKIP_ONE) * sizeof(char *));
    (*list)[idx] = heap_strdup(val);
}

static int compare_ordered_bindings(binding_t *a, binding_t *b, const cbm_order_item_t *keys,
                                    int key_count) {
    for (int key = 0; key < key_count; key++) {
        const char *av = binding_get_virtual(a, keys[key].expression, NULL);
        const char *bv = binding_get_virtual(b, keys[key].expression, NULL);
        av = av ? av : "";
        bv = bv ? bv : "";
        bool a_null = av[0] == '\0';
        bool b_null = bv[0] == '\0';
        int cmp = 0;
        if (a_null != b_null) {
            cmp = a_null ? 1 : -1;
        } else if (!a_null) {
            char *a_end = NULL;
            char *b_end = NULL;
            double da = strtod(av, &a_end);
            double db = strtod(bv, &b_end);
            bool both_numeric = a_end != av && *a_end == '\0' && b_end != bv && *b_end == '\0';
            cmp = both_numeric ? ((da > db) - (da < db)) : strcmp(av, bv);
        }
        if (cmp != 0) {
            bool desc = keys[key].direction && strcmp(keys[key].direction, "DESC") == 0;
            return desc ? -cmp : cmp;
        }
    }
    return 0;
}

/* Stable bottom-up merge sort over integer row indices: O(rows log rows)
 * comparisons and O(rows) integers. Sorting full binding_t values in scratch
 * would duplicate every owned node/edge slot and materially inflate peak RSS. */
static void sort_bindings(binding_t *bindings, int count, const cbm_order_item_t *keys,
                          int key_count) {
    if (count < PAIR_LEN || key_count <= 0) {
        return;
    }
    int *order = malloc((size_t)count * sizeof(*order));
    int *scratch = malloc((size_t)count * sizeof(*scratch));
    if (!order || !scratch) {
        free(order);
        free(scratch);
        return;
    }
    for (int i = 0; i < count; i++) {
        order[i] = i;
    }
    int *src = order;
    int *dst = scratch;
    for (int width = SKIP_ONE; width < count;) {
        for (int left = 0; left < count; left += width * PAIR_LEN) {
            int mid = left + width < count ? left + width : count;
            int right = left + width * PAIR_LEN < count ? left + width * PAIR_LEN : count;
            int i = left;
            int j = mid;
            int out = left;
            while (i < mid && j < right) {
                if (compare_ordered_bindings(&bindings[src[i]], &bindings[src[j]], keys,
                                             key_count) <= 0) {
                    dst[out++] = src[i++];
                } else {
                    dst[out++] = src[j++];
                }
            }
            while (i < mid) {
                dst[out++] = src[i++];
            }
            while (j < right) {
                dst[out++] = src[j++];
            }
        }
        int *swap = src;
        src = dst;
        dst = swap;
        if (width > count / PAIR_LEN) {
            break;
        }
        width *= PAIR_LEN;
    }
    if (src != order) {
        memcpy(order, src, (size_t)count * sizeof(*order));
    }

    /* Convert destination->source order into source->destination, then apply
     * permutation cycles in-place with one binding_t temporary per swap. */
    for (int destination = 0; destination < count; destination++) {
        scratch[order[destination]] = destination;
    }
    for (int source = 0; source < count; source++) {
        while (scratch[source] != source) {
            int destination = scratch[source];
            binding_t moved = bindings[source];
            bindings[source] = bindings[destination];
            bindings[destination] = moved;
            int mapped = scratch[source];
            scratch[source] = scratch[destination];
            scratch[destination] = mapped;
        }
    }
    free(order);
    free(scratch);
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
    if (limit >= 0 && *count > limit) {
        for (int i = limit; i < *count; i++) {
            binding_free(&vbindings[i]);
        }
        *count = limit;
    }
}

/* Sort, skip, and limit binding array in-place */
static void with_sort_skip_limit(const cbm_return_clause_t *wc, binding_t *vbindings, int *vcount) {
    sort_bindings(vbindings, *vcount, wc->order_items, wc->order_count);
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
    bool *group_nulls;
    double *sums;
    int *counts;
    double *mins, *maxs;
    char ***distinct_lists;  /* per-item set of seen values for COUNT(DISTINCT) */
    int *distinct_n;         /* per-item distinct count (#239) */
    int64_t *group_node_ids; /* per-item node id when the group var is a node (0 = not) */
} with_agg_t;

/* Build a group key from non-aggregate WITH items */
static int with_agg_build_key(cbm_return_clause_t *wc, binding_t *b, char *key, size_t key_sz) {
    int kl = 0;
    for (int ci = 0; ci < wc->count; ci++) {
        if (wc->items[ci].func) {
            continue;
        }
        bool is_null = true;
        const char *v = binding_get_virtual_ex(b, wc->items[ci].variable,
                                               wc->items[ci].property, &is_null);
        kl = group_key_append(key, key_sz, kl, b, wc->items[ci].variable,
                              wc->items[ci].property, v, is_null);
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
    (*aggs)[found].group_nulls = calloc(wc->count, sizeof(bool));
    (*aggs)[found].sums = calloc(wc->count, sizeof(double));
    (*aggs)[found].counts = calloc(wc->count, sizeof(int));
    (*aggs)[found].mins = calloc(wc->count, sizeof(double));
    (*aggs)[found].maxs = calloc(wc->count, sizeof(double));
    (*aggs)[found].distinct_lists = calloc(wc->count, sizeof(char **));
    (*aggs)[found].distinct_n = calloc(wc->count, sizeof(int));
    (*aggs)[found].group_node_ids = calloc(wc->count, sizeof(int64_t));
    for (int ci = 0; ci < wc->count; ci++) {
        (*aggs)[found].mins[ci] = CYP_DBL_MAX;
        (*aggs)[found].maxs[ci] = -CYP_DBL_MAX;
    }
    for (int ci = 0; ci < wc->count; ci++) {
        if (wc->items[ci].func) {
            (*aggs)[found].group_vals[ci] = heap_strdup("0");
            continue;
        }
        bool is_null = true;
        const char *v = binding_get_virtual_ex(b, wc->items[ci].variable,
                                               wc->items[ci].property, &is_null);
        (*aggs)[found].group_vals[ci] = heap_strdup(v);
        (*aggs)[found].group_nulls[ci] = is_null;
        /* If this group item is a bare node variable, remember its id so the
         * carried virtual var can re-fetch any property (group_vals holds only
         * the name). */
        if (!wc->items[ci].property && wc->items[ci].variable) {
            cbm_node_t *gn = binding_get(b, wc->items[ci].variable);
            if (gn) {
                (*aggs)[found].group_node_ids[ci] = gn->id;
            }
        }
    }
    return found;
}

/* Accumulate aggregation values for a binding */
static void with_agg_accumulate(with_agg_t *agg, cbm_return_clause_t *wc, binding_t *b) {
    for (int ci = 0; ci < wc->count; ci++) {
        if (!wc->items[ci].func) {
            continue;
        }
        bool is_null = true;
        const char *raw = binding_get_virtual_ex(b, wc->items[ci].variable,
                                                 wc->items[ci].property, &is_null);
        if (is_null) {
            continue;
        }
        agg->counts[ci]++;
        if (wc->items[ci].distinct && strcmp(wc->items[ci].func, "COUNT") == 0) {
            distinct_list_add(&agg->distinct_lists[ci], &agg->distinct_n[ci], raw);
        }
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
        if (agg->counts[ci] == 0) {
            buf[0] = '\0';
        } else {
            snprintf(buf, buf_sz, "%.10g", agg->sums[ci] / agg->counts[ci]);
        }
    } else if (strcmp(func, "MIN") == 0) {
        if (agg->counts[ci] == 0) {
            buf[0] = '\0';
        } else {
            snprintf(buf, buf_sz, "%.10g", agg->mins[ci]);
        }
    } else if (strcmp(func, "MAX") == 0) {
        if (agg->counts[ci] == 0) {
            buf[0] = '\0';
        } else {
            snprintf(buf, buf_sz, "%.10g", agg->maxs[ci]);
        }
    } else {
        snprintf(buf, buf_sz, "%d", agg->counts[ci]);
    }
}

/* Add a virtual variable binding for one WITH item */
static void with_add_vbinding_var(binding_t *vb, const char *alias, const char *val, bool is_null) {
    if (vb->var_count >= CYP_MAX_VARS) {
        return;
    }
    int index = vb->var_count++;
    vb->var_names[index] = heap_strdup(alias);
    vb->var_name_owned[index] = true;
    vb->var_is_null[index] = is_null;
    vb->var_nodes[index].name = heap_strdup(val);
}

/* Free with_agg_t array */
static void with_agg_free(with_agg_t *aggs, int agg_cnt, int item_count) {
    for (int a = 0; a < agg_cnt; a++) {
        for (int ci = 0; ci < item_count; ci++) {
            safe_str_free(&aggs[a].group_vals[ci]);
            if (aggs[a].distinct_lists && aggs[a].distinct_lists[ci]) {
                for (int j = 0; j < aggs[a].distinct_n[ci]; j++) {
                    free(aggs[a].distinct_lists[ci][j]);
                }
                free(aggs[a].distinct_lists[ci]);
            }
        }
        free(aggs[a].group_vals);
        free(aggs[a].group_nulls);
        free(aggs[a].sums);
        free(aggs[a].counts);
        free(aggs[a].mins);
        free(aggs[a].maxs);
        free(aggs[a].distinct_lists);
        free(aggs[a].distinct_n);
        free(aggs[a].group_node_ids);
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
        /* Carry the store so node_prop can re-fetch a carried node's properties
         * (and compute in_degree/out_degree) on the projected virtual binding. */
        vb.store = (bind_count > 0) ? bindings[0].store : NULL;
        vb.project = (bind_count > 0) ? bindings[0].project : NULL;
        vb.use_active_overlay_edges =
            (bind_count > 0) ? bindings[0].use_active_overlay_edges : false;
        for (int ci = 0; ci < wc->count; ci++) {
            char name_buf[CBM_SZ_256];
            const char *alias = resolve_item_alias(&wc->items[ci], name_buf, sizeof(name_buf));
            if (wc->items[ci].func) {
                char vbuf[CBM_SZ_64];
                if (wc->items[ci].distinct && strcmp(wc->items[ci].func, "COUNT") == 0) {
                    snprintf(vbuf, sizeof(vbuf), "%d", aggs[a].distinct_n[ci]); /* #239 */
                } else {
                    with_agg_format(wc->items[ci].func, &aggs[a], ci, vbuf, sizeof(vbuf));
                }
                bool is_null = aggs[a].counts[ci] == 0 &&
                               (strcmp(wc->items[ci].func, "AVG") == 0 ||
                                strcmp(wc->items[ci].func, "MIN") == 0 ||
                                strcmp(wc->items[ci].func, "MAX") == 0);
                with_add_vbinding_var(&vb, alias, vbuf, is_null);
            } else {
                with_add_vbinding_var(&vb, alias, aggs[a].group_vals[ci],
                                      aggs[a].group_nulls[ci]);
                /* Tag the carried virtual var with the node id (when the group
                 * var is a node) so node_prop can re-fetch its full properties. */
                if (aggs[a].group_node_ids[ci] > 0 && vb.var_count > 0) {
                    vb.var_nodes[vb.var_count - 1].id = aggs[a].group_node_ids[ci];
                }
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
        vb.store = bindings[bi].store; /* so node_prop can re-fetch / compute on the projection */
        vb.project = bindings[bi].project;
        vb.use_active_overlay_edges = bindings[bi].use_active_overlay_edges;
        for (int ci = 0; ci < wc->count; ci++) {
            char name_buf[CBM_SZ_256];
            const char *alias = resolve_item_alias(&wc->items[ci], name_buf, sizeof(name_buf));
            char func_buf[CBM_SZ_512];
            const char *val =
                project_item(&bindings[bi], &wc->items[ci], func_buf, sizeof(func_buf));
            bool is_null = false;
            if (!wc->items[ci].func && !wc->items[ci].kase && !wc->items[ci].args) {
                (void)binding_get_virtual_ex(&bindings[bi], wc->items[ci].variable,
                                             wc->items[ci].property, &is_null);
            }
            with_add_vbinding_var(&vb, alias, val, is_null);
            /* A whole-node projection must remain a node binding across the
             * WITH boundary. Retain its canonical id so the next MATCH stage
             * can traverse from it and node_prop can re-fetch complete fields. */
            if (!wc->items[ci].func && !wc->items[ci].property && vb.var_count > 0) {
                cbm_node_t *carried = binding_get(&bindings[bi], wc->items[ci].variable);
                if (carried) {
                    vb.var_nodes[vb.var_count - SKIP_ONE].id = carried->id;
                }
            }
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

/* Build a key from a projected vbinding's value tuple (all WITH output items),
 * used to detect duplicate rows for WITH DISTINCT (#238). */
static void with_proj_key(cbm_return_clause_t *wc, binding_t *b, char *key, size_t key_sz) {
    int kl = 0;
    key[0] = '\0';
    char name_buf[CBM_SZ_256];
    for (int ci = 0; ci < wc->count; ci++) {
        const char *alias = resolve_item_alias(&wc->items[ci], name_buf, sizeof(name_buf));
        const char *v = binding_get_virtual(b, alias, NULL);
        int w = snprintf(key + kl, (kl < (int)key_sz) ? key_sz - (size_t)kl : 0, "%s|", v ? v : "");
        if (w > 0) {
            kl += w;
        }
        if (kl >= (int)key_sz) {
            break; /* buffer full */
        }
    }
}

/* Apply WITH DISTINCT: drop projected rows whose value tuple duplicates an
 * earlier one, keeping first occurrence (#238 — previously silently ignored). */
static void with_apply_distinct(cbm_return_clause_t *wc, binding_t *vbindings, int *vcount) {
    int kept = 0;
    for (int i = 0; i < *vcount; i++) {
        char key[CBM_SZ_1K];
        with_proj_key(wc, &vbindings[i], key, sizeof(key));
        bool dup = false;
        for (int j = 0; j < kept; j++) {
            char pkey[CBM_SZ_1K];
            with_proj_key(wc, &vbindings[j], pkey, sizeof(pkey));
            if (strcmp(key, pkey) == 0) {
                dup = true;
                break;
            }
        }
        if (dup) {
            binding_free(&vbindings[i]);
        } else {
            if (kept != i) {
                vbindings[kept] = vbindings[i];
            }
            kept++;
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

    /* WITH DISTINCT: dedup projected rows (no-op for aggregation, which already
     * collapses to one row per group). */
    if (wc->distinct) {
        with_apply_distinct(wc, vbindings, &vcount);
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
        safe_str_free(&col_names[i]);
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
        if (count == 0) {
            buf[0] = '\0';
        } else {
            snprintf(buf, buf_sz, "%.10g", sum / count);
        }
    } else if (strcmp(func, "MIN") == 0) {
        if (count == 0) {
            buf[0] = '\0';
        } else {
            snprintf(buf, buf_sz, "%.10g", min_val);
        }
    } else if (strcmp(func, "MAX") == 0) {
        if (count == 0) {
            buf[0] = '\0';
        } else {
            snprintf(buf, buf_sz, "%.10g", max_val);
        }
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
        bool is_null = true;
        const char *raw = binding_get_virtual_ex(b, ret->items[ci].variable,
                                                 ret->items[ci].property, &is_null);
        if (is_null) {
            continue;
        }
        entry->counts[ci]++;
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
        } else if (ret->items[ci].distinct && strcmp(ret->items[ci].func, "COUNT") == 0) {
            /* COUNT(DISTINCT x): track unique values; emit the set size (#239). */
            distinct_list_add(&entry->collect_lists[ci], &entry->collect_counts[ci], raw);
        }
    }
}

/* Free RETURN aggregation entries */
static void ret_agg_free(ret_agg_entry_t *aggs, int agg_count, int item_count) {
    for (int a = 0; a < agg_count; a++) {
        for (int ci = 0; ci < item_count; ci++) {
            safe_str_free(&aggs[a].group_vals[ci]);
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
                              const char **vals, char valbufs[][CBM_SZ_512]) {
    int klen = 0;
    for (int ci = 0; ci < ret->count; ci++) {
        if (ret->items[ci].func) {
            vals[ci] = "0";
            continue;
        }
        /* project_item may return its own scratch (stable static or a per-column
         * buffer it copied into); persist the value in the caller-owned valbufs
         * so vals[] survives until ret_agg_init_group strdup's it. */
        bool is_null = false;
        if (!ret->items[ci].func && !ret->items[ci].kase && !ret->items[ci].args) {
            (void)binding_get_virtual_ex(b, ret->items[ci].variable, ret->items[ci].property,
                                         &is_null);
        }
        const char *v = project_item(b, &ret->items[ci], valbufs[ci], CBM_SZ_512);
        if (v != valbufs[ci]) {
            snprintf(valbufs[ci], CBM_SZ_512, "%s", v ? v : "");
        }
        vals[ci] = valbufs[ci];
        klen = group_key_append(key, key_sz, klen, b, ret->items[ci].variable,
                                ret->items[ci].property, vals[ci], is_null);
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
        if (ret->items[ci].distinct && strcmp(ret->items[ci].func, "COUNT") == 0) {
            /* COUNT(DISTINCT x) — number of unique values accumulated (#239). */
            snprintf(bufs[ci], sizeof(bufs[ci]), "%d", agg->collect_counts[ci]);
            row[ci] = bufs[ci];
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
        /* #601: grouping is O(bindings x groups) — the dominant cost on a
         * whole-graph GROUP BY. Abort if we blow the wall-clock budget. */
        if ((bi & CYPHER_DEADLINE_CHECK_MASK) == 0 && cypher_deadline_exceeded()) {
            break;
        }
        char key[CBM_SZ_1K] = "";
        const char *vals[CBM_SZ_32];
        char valbufs[CBM_SZ_32][CBM_SZ_512];
        ret_agg_build_key(ret, &bindings[bi], key, sizeof(key), vals, valbufs);

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
            safe_str_free(&col_names[i]);
        }
    }
}

/* Execute simple (non-aggregate) RETURN projection */
static void execute_return_simple(cbm_return_clause_t *ret, binding_t *bindings, int bind_count,
                                  int max_rows, result_builder_t *rb) {
    /* ORDER BY, DISTINCT, and SKIP select from the complete eligible set before
     * the output cap is applied. Prefix projection is safe only when no later
     * result-selection operator can change which rows belong in the response. */
    int proj_cap = bind_count;
    if (!ret->distinct && ret->order_count == 0 && ret->skip <= 0) {
        proj_cap = max_rows;
        if (ret->limit >= 0 && ret->limit < proj_cap) {
            proj_cap = ret->limit;
        }
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
        safe_str_free(&col_names[i]);
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
                             int extra_count, const char *nvar, bool opt,
                             const cbm_where_clause_t *pattern_where) {
    /* Bound intermediate cardinality at the engine's public result ceiling.
     * This avoids signed multiplication overflow and keeps memory O(ceiling)
     * while still scanning rejected candidates until a qualifying row exists. */
    int max_new = CYPHER_RESULT_CEILING;
    int new_cap = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
    if (new_cap > max_new) {
        new_cap = max_new;
    }
    binding_t *new_bindings = malloc((size_t)new_cap * sizeof(binding_t));
    if (!new_bindings) {
        return;
    }
    int new_count = 0;
    for (int bi = 0; bi < *bind_count && new_count < max_new; bi++) {
        int match_count = 0;
        for (int ni = 0; ni < extra_count && new_count < max_new; ni++) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            binding_set(&nb, nvar, &extra_nodes[ni]);
            if (pattern_where && !eval_where(pattern_where, &nb)) {
                binding_free(&nb);
                continue;
            }
            if (!binding_array_reserve(&new_bindings, &new_cap, new_count + SKIP_ONE, max_new)) {
                binding_free(&nb);
                goto cross_join_nodes_done;
            }
            new_bindings[new_count++] = nb;
            match_count++;
        }
        if (opt && match_count == 0 && new_count < max_new) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            if (!binding_array_reserve(&new_bindings, &new_cap, new_count + SKIP_ONE, max_new)) {
                binding_free(&nb);
                goto cross_join_nodes_done;
            }
            new_bindings[new_count++] = nb;
        }
    }
cross_join_nodes_done:
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
                                 const char *nvar, bool opt,
                                 const cbm_where_clause_t *pattern_where) {
    int max_new = CYPHER_RESULT_CEILING;
    int new_capacity = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
    if (new_capacity > max_new) {
        new_capacity = max_new;
    }
    binding_t *new_bindings = malloc((size_t)new_capacity * sizeof(binding_t));
    if (!new_bindings) {
        return;
    }
    int new_count = 0;
    for (int bi = 0; bi < *bind_count && new_count < max_new; bi++) {
        for (int ni = 0; ni < extra_count && new_count < max_new; ni++) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            binding_set(&nb, nvar, &extra_nodes[ni]);
            binding_t *tmp = malloc(sizeof(binding_t));
            if (!tmp) {
                binding_free(&nb);
                goto cross_join_rels_done;
            }
            tmp[0] = nb;
            int tc = SKIP_ONE;
            const char *tv = nvar;
            expand_pattern_rels(store, patn, &tmp, &tc, &tv, opt, pattern_where);
            for (int ti = 0; ti < tc; ti++) {
                if (!binding_array_append(&new_bindings, &new_count, &new_capacity, max_new,
                                          &tmp[ti])) {
                    for (int rest = ti + SKIP_ONE; rest < tc; rest++) {
                        binding_free(&tmp[rest]);
                    }
                    free(tmp);
                    goto cross_join_rels_done;
                }
            }
            free(tmp);
        }
        if (opt && extra_count == 0) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            if (!binding_array_append(&new_bindings, &new_count, &new_capacity, max_new, &nb)) {
                goto cross_join_rels_done;
            }
        }
    }
cross_join_rels_done:
    for (int bi = 0; bi < *bind_count; bi++) {
        binding_free(&(*bindings)[bi]);
    }
    free(*bindings);
    *bindings = new_bindings;
    *bind_count = new_count;
}

/* Drive a single-relationship additional pattern from its ALREADY-BOUND
 * terminal node, binding the unbound START var to the edge's other endpoint.
 *
 * Handles `OPTIONAL MATCH (c)-[:CALLS]->(f)` where `f` is bound from an earlier
 * MATCH and `c` is new: scanning every node for `c` and cross-joining (a) risks
 * an int-overflow OOB write on large graphs and (b) leaves `c` bound to an
 * arbitrary node so a later `WHERE c IS NULL` wrongly drops every row (#627).
 * Instead we scan only the bound terminal's edges and bind `c` to real
 * neighbours; with OPTIONAL we keep the row with `c` unbound when there are
 * none — the correct dead-code semantics. */
static void expand_from_bound_terminal(cbm_store_t *store, cbm_pattern_t *patn,
                                       binding_t **bindings, int *bind_count, const char *start_var,
                                       bool opt, const cbm_where_clause_t *pattern_where) {
    cbm_rel_pattern_t *rel = &patn->rels[0];
    const cbm_node_pattern_t *start_node = &patn->nodes[0];
    /* The relationship is written start-[r]->terminal. To enumerate the start
     * nodes reachable from the bound terminal we invert the stored direction. */
    bool rel_inbound = rel->direction && strcmp(rel->direction, "inbound") == 0;
    /* (start)->(term): start = edge source = scan terminal's inbound edges. */
    bool scan_targets = !rel_inbound;

    int max_new = CYPHER_RESULT_CEILING;
    int new_capacity = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
    if (new_capacity > max_new) {
        new_capacity = max_new;
    }
    binding_t *new_bindings = malloc((size_t)new_capacity * sizeof(binding_t));
    if (!new_bindings) {
        return;
    }
    int new_count = 0;

    for (int bi = 0; bi < *bind_count && new_count < max_new; bi++) {
        binding_t *b = &(*bindings)[bi];
        cbm_node_t *term = binding_get(b, patn->nodes[1].variable ? patn->nodes[1].variable : "");
        int match_count = 0;
        if (term) {
            bool used_active_overlay_edges = false;
            if (b->use_active_overlay_edges && b->project && term->qualified_name &&
                term->qualified_name[0]) {
                used_active_overlay_edges = true;
                bool rel_any = rel->direction && strcmp(rel->direction, "any") == 0;
                int direction = rel_any ? CBM_STORE_EDGE_DIR_ANY
                                        : (scan_targets ? CBM_STORE_EDGE_DIR_INBOUND
                                                        : CBM_STORE_EDGE_DIR_OUTBOUND);
                cbm_store_edge_node_t *rows = NULL;
                int row_count = 0;
                if (cbm_store_find_active_edge_nodes_by_qn(store, b->project, term->qualified_name,
                                                           (const char **)rel->types,
                                                           rel->type_count, direction, &rows,
                                                           &row_count) == CBM_STORE_OK) {
                    process_active_edge_nodes(rows, row_count, start_node, b, start_var,
                                              rel->variable, &new_bindings, &new_count,
                                              &new_capacity, max_new, &match_count, pattern_where);
                }
                cbm_store_free_edge_nodes(rows, row_count);
            }
            if (!used_active_overlay_edges) {
                int type_count = rel->type_count > 0 ? rel->type_count : SKIP_ONE;
                for (int ti = 0; ti < type_count && new_count < max_new; ti++) {
                    cbm_edge_t *edges = NULL;
                    int edge_count = 0;
                    if (rel->type_count > 0) {
                        if (scan_targets) {
                            cbm_store_find_edges_by_target_type(store, term->id, rel->types[ti],
                                                                &edges, &edge_count);
                        } else {
                            cbm_store_find_edges_by_source_type(store, term->id, rel->types[ti],
                                                                &edges, &edge_count);
                        }
                    } else if (scan_targets) {
                        cbm_store_find_edges_by_target(store, term->id, &edges, &edge_count);
                    } else {
                        cbm_store_find_edges_by_source(store, term->id, &edges, &edge_count);
                    }
                    for (int ei = 0; ei < edge_count && new_count < max_new; ei++) {
                        int64_t sid = scan_targets ? edges[ei].source_id : edges[ei].target_id;
                        cbm_node_t found = {0};
                        if (cbm_store_find_node_by_id(store, sid, &found) != CBM_STORE_OK) {
                            continue;
                        }
                        if (start_node->label &&
                            !label_alt_matches(found.label, start_node->label)) {
                            node_fields_free(&found);
                            continue;
                        }
                        if (!check_inline_props(&found, start_node->props, start_node->prop_count,
                                                store)) {
                            node_fields_free(&found);
                            continue;
                        }
                        binding_t nb = {0};
                        binding_copy(&nb, b);
                        binding_set(&nb, start_var, &found);
                        if (rel->variable) {
                            binding_set_edge(&nb, rel->variable, &edges[ei]);
                        }
                        node_fields_free(&found);
                        if (pattern_where && !eval_where(pattern_where, &nb)) {
                            binding_free(&nb);
                            continue;
                        }
                        if (binding_array_append(&new_bindings, &new_count, &new_capacity, max_new,
                                                 &nb)) {
                            match_count++;
                        }
                    }
                    cbm_store_free_edges(edges, edge_count);
                }
            }
        }
        if (opt && match_count == 0 && new_count < max_new) {
            /* No matching neighbour: keep the row with start_var left UNBOUND so
             * `WHERE <start> IS NULL` correctly identifies the no-edge case. */
            binding_t nb = {0};
            binding_copy(&nb, b);
            (void)binding_array_append(&new_bindings, &new_count, &new_capacity, max_new, &nb);
        }
    }

    for (int bi = 0; bi < *bind_count; bi++) {
        binding_free(&(*bindings)[bi]);
    }
    free(*bindings);
    *bindings = new_bindings;
    *bind_count = new_count;
}

/* Expand MATCH patterns from an existing row stream. The initial query starts
 * at pattern 1 because pattern 0 seeds that stream from a node scan; a stage
 * after WITH starts at pattern 0 and consumes only the projected bindings. */
static void expand_patterns_from(cbm_store_t *store, cbm_query_t *q, int first_pattern,
                                 const char *project, int max_rows,
                                 cypher_node_scan_mode_t scan_mode, binding_t **bindings,
                                 int *bind_count, int *bind_cap) {
    for (int pi = first_pattern; pi < q->pattern_count; pi++) {
        cbm_pattern_t *patn = &q->patterns[pi];
        bool opt = q->pattern_optional[pi];
        const cbm_where_clause_t *pattern_where =
            (pi == q->pattern_count - SKIP_ONE) ? q->where : NULL;
        const char *nvar = patn->nodes[0].variable ? patn->nodes[0].variable : "_n_extra";
        bool start_bound = *bind_count > 0 && binding_get(&(*bindings)[0], nvar) != NULL;

        if (start_bound && patn->rel_count > 0) {
            const char *tv = nvar;
            expand_pattern_rels(store, patn, bindings, bind_count, &tv, opt, pattern_where);
            continue;
        }

        /* Single-rel pattern whose START is unbound but whose TERMINAL is already
         * bound: drive from the bound terminal instead of scanning all nodes for
         * the start var (avoids the int-overflow OOB write and the c-IS-NULL
         * corruption of #627). */
        if (!start_bound && patn->rel_count == SKIP_ONE && *bind_count > 0) {
            const char *term_var = patn->nodes[1].variable;
            bool term_bound = term_var && binding_get(&(*bindings)[0], term_var) != NULL;
            if (term_bound) {
                expand_from_bound_terminal(store, patn, bindings, bind_count, nvar, opt,
                                           pattern_where);
                continue;
            }
        }

        cbm_node_t *extra_nodes = NULL;
        int extra_count = 0;
        scan_pattern_nodes(store, project, INT_MAX, &patn->nodes[0], pattern_where, nvar, scan_mode,
                           &extra_nodes, &extra_count);
        if (patn->rel_count == 0) {
            cross_join_nodes(bindings, bind_count, extra_nodes, extra_count, nvar, opt,
                             pattern_where);
        } else {
            cross_join_with_rels(store, patn, bindings, bind_count, extra_nodes, extra_count, nvar,
                                 opt, pattern_where);
        }
        cbm_store_free_nodes(extra_nodes, extra_count);
    }
}

static void execute_return_clause(cbm_query_t *q, cbm_return_clause_t *ret, binding_t *bindings,
                                  int bind_count, int max_rows, result_builder_t *rb);

static bool query_where_is_optional_pattern_predicate(const cbm_query_t *q) {
    if (!q || !q->where || q->pattern_count <= 0) {
        return false;
    }
    int last = q->pattern_count - SKIP_ONE;
    return q->pattern_optional[last];
}

/* Execute a MATCH stage that consumes bindings projected by a preceding WITH.
 * Ownership of the binding array remains with the outer execute_single call;
 * expansion/projection helpers replace it only after freeing the prior rows. */
static void execute_bound_stage(cbm_store_t *store, cbm_query_t *q, const char *project,
                                int max_rows, cypher_node_scan_mode_t scan_mode,
                                binding_t **bindings, int *bind_count, result_builder_t *rb) {
    while (q) {
        int bind_cap = *bind_count;
        if (bind_cap < max_rows) {
            bind_cap = max_rows;
        }
        if (bind_cap < SKIP_ONE) {
            bind_cap = SKIP_ONE;
        }

        expand_patterns_from(store, q, 0, project, max_rows, scan_mode, bindings, bind_count,
                             &bind_cap);
        if (q->where && !query_where_is_optional_pattern_predicate(q)) {
            filter_bindings_where(q->where, *bindings, bind_count);
        }
        execute_with_clause(q, bindings, bind_count);
        if (!q->next_stage) {
            break;
        }
        q = q->next_stage;
    }

    rb_init(rb);
    if (q->ret) {
        execute_return_clause(q, q->ret, *bindings, *bind_count, max_rows, rb);
    } else {
        execute_default_projection(&q->patterns[0], *bindings, *bind_count, max_rows, rb);
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

    if (ret->distinct) {
        rb_apply_distinct(rb);
    }
    rb_apply_order_by(rb, ret);
    int output_limit = max_rows;
    if (ret->limit >= 0 && ret->limit < output_limit) {
        output_limit = ret->limit;
    }
    rb_apply_skip_limit(rb, ret->skip, output_limit);
}

static bool where_is_exact_file_contains(const cbm_where_clause_t *where, const char *variable) {
    if (!where || !variable) {
        return false;
    }
    if (where->root) {
        return where->root->type == EXPR_CONDITION &&
               condition_file_contains_value(&where->root->cond, variable) != NULL;
    }
    return where->count == SKIP_ONE && (!where->op || strcmp(where->op, "AND") == 0) &&
           condition_file_contains_value(&where->conditions[0], variable) != NULL;
}

/* An output cap may bound the initial SQL scan only when every later operation
 * preserves that scan prefix. Predicates, relationship expansion, aggregation,
 * DISTINCT, ordering, skipping, and later stages can all make a row outside an
 * arbitrary prefix the correct result. For those shapes, scan every candidate
 * (after exact SQL pushdowns) and let the Cypher evaluator enforce max_rows on
 * output. This keeps resource limits from silently changing query semantics. */
static bool query_initial_scan_can_stop_at_output_cap(const cbm_query_t *q, const char *variable,
                                                      cypher_node_scan_mode_t scan_mode) {
    if (!q || q->pattern_count != SKIP_ONE || q->patterns[0].rel_count != 0 ||
        q->patterns[0].nodes[0].prop_count != 0 || q->with_clause || q->post_with_where ||
        q->next_stage) {
        return false;
    }
    if (q->where && (scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY ||
                     !where_is_exact_file_contains(q->where, variable))) {
        return false;
    }
    const cbm_return_clause_t *ret = q->ret;
    if (!ret) {
        return true;
    }
    if (ret->distinct || ret->order_count > 0 || ret->skip > 0) {
        return false;
    }
    for (int i = 0; i < ret->count; i++) {
        if (is_aggregate_func(ret->items[i].func)) {
            return false;
        }
    }
    return true;
}

static int execute_single(cbm_store_t *store, cbm_query_t *q, const char *project, int max_rows,
                          cypher_node_scan_mode_t scan_mode, result_builder_t *rb) {
    cbm_pattern_t *pat0 = &q->patterns[0];
    const char *var_name = pat0->nodes[0].variable ? pat0->nodes[0].variable : "_n0";

    /* Step 1: Scan initial nodes */
    cbm_node_t *scanned = NULL;
    int scan_count = 0;
    int candidate_limit =
        query_initial_scan_can_stop_at_output_cap(q, var_name, scan_mode) ? max_rows : INT_MAX;
    scan_pattern_nodes(store, project, candidate_limit, &pat0->nodes[0], q->where, var_name,
                       scan_mode, &scanned, &scan_count);

    /* Build initial bindings with early WHERE */
    int bind_cap = scan_count > max_rows ? scan_count : (max_rows > 0 ? max_rows : SKIP_ONE);
    binding_t *bindings = malloc((bind_cap + SKIP_ONE) * sizeof(binding_t));
    int bind_count = 0;
    for (int i = 0; i < scan_count && bind_count < bind_cap; i++) {
        if ((i & CYPHER_DEADLINE_CHECK_MASK) == 0 && cypher_deadline_exceeded()) {
            break;
        }
        binding_t b = {0};
        b.store = store;
        b.project = project;
        b.use_active_overlay_edges = scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY;
        binding_set(&b, var_name, &scanned[i]);
        bool pass = eval_where_partial(q->where, &b) != CYP_PARTIAL_FALSE;
        if (pass) {
            bindings[bind_count++] = b;
        } else {
            binding_free(&b);
        }
    }

    /* OPTIONAL MATCH over an empty or fully predicate-rejected initial scan
     * still produces one null-extended row. Keep graph/project context on the
     * synthetic binding so later stages use the same store and overlay mode. */
    if (q->pattern_optional[0] && bind_count == 0) {
        binding_t b = {0};
        b.store = store;
        b.project = project;
        b.use_active_overlay_edges = scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY;
        bindings[bind_count++] = b;
    }

    /* Step 2: Expand first pattern's relationships */
    const cbm_where_clause_t *first_pattern_where = q->pattern_count == SKIP_ONE ? q->where : NULL;
    expand_pattern_rels(store, pat0, &bindings, &bind_count, &var_name, q->pattern_optional[0],
                        first_pattern_where);

    /* Step 2b: Additional patterns */
    expand_patterns_from(store, q, SKIP_ONE, project, max_rows, scan_mode, &bindings, &bind_count,
                         &bind_cap);

    /* Step 3: Late WHERE */
    if (q->where && !query_where_is_optional_pattern_predicate(q) &&
        (pat0->rel_count > 0 || q->pattern_count > SKIP_ONE)) {
        filter_bindings_where(q->where, bindings, &bind_count);
    }

    /* Step 3b: WITH clause */
    execute_with_clause(q, &bindings, &bind_count);

    /* Step 4: Project results */
    if (q->next_stage) {
        execute_bound_stage(store, q->next_stage, project, max_rows, scan_mode, &bindings,
                            &bind_count, rb);
    } else {
        rb_init(rb);
        if (q->ret) {
            execute_return_clause(q, q->ret, bindings, bind_count, max_rows, rb);
        } else {
            execute_default_projection(pat0, bindings, bind_count, max_rows, rb);
        }
    }

    for (int bi = 0; bi < bind_count; bi++) {
        binding_free(&bindings[bi]);
    }
    free(bindings);
    cbm_store_free_nodes(scanned, scan_count);
    return 0;
}

static bool cypher_is_degree_prop(const char *prop) {
    return prop && (strcmp(prop, "in_degree") == 0 || strcmp(prop, "out_degree") == 0);
}

static bool cypher_where_requires_canonical_identity(const cbm_where_clause_t *where) {
    (void)where;
    return false;
}

static bool cypher_return_requires_canonical_identity(const cbm_return_clause_t *ret) {
    if (!ret) {
        return false;
    }
    for (int i = 0; i < ret->order_count; i++) {
        const char *expression = ret->order_items[i].expression;
        if (expression &&
            (strstr(expression, ".in_degree") || strstr(expression, ".out_degree"))) {
            return false;
        }
    }
    for (int i = 0; i < ret->count; i++) {
        /* Overlay rows do not have stable canonical node/edge ids until
         * compaction, so id() keeps the query on the canonical read model. */
        if (ret->items[i].func && strcmp(ret->items[i].func, "id") == 0) {
            return true;
        }
        if (cypher_is_degree_prop(ret->items[i].property)) {
            continue;
        }
        for (int a = 0; a < ret->items[i].arg_count; a++) {
            if (cypher_is_degree_prop(ret->items[i].args[a].property)) {
                continue;
            }
        }
    }
    return false;
}

static bool cypher_pattern_supports_active_relationships(const cbm_pattern_t *pat) {
    if (!pat || pat->rel_count == 0) {
        return true;
    }
    if (pat->rel_count != SKIP_ONE) {
        return false;
    }
    return true;
}

static bool cypher_query_supports_active_nodes(const cbm_query_t *q) {
    for (const cbm_query_t *root = q; root; root = root->union_next) {
        for (const cbm_query_t *stage = root; stage; stage = stage->next_stage) {
            for (int pi = 0; pi < stage->pattern_count; pi++) {
                if (!cypher_pattern_supports_active_relationships(&stage->patterns[pi])) {
                    return false;
                }
            }
            if (cypher_where_requires_canonical_identity(stage->where) ||
                cypher_where_requires_canonical_identity(stage->post_with_where) ||
                cypher_return_requires_canonical_identity(stage->with_clause) ||
                cypher_return_requires_canonical_identity(stage->ret)) {
                return false;
            }
        }
    }
    return true;
}

/* ── Main entry point ─────────────────────────────────────────── */

static int cbm_cypher_execute_impl(cbm_store_t *store, const char *query, const char *project,
                                   int max_rows, bool request_active_nodes,
                                   cbm_cypher_result_t *out, bool *used_active_nodes) {
    memset(out, 0, sizeof(*out));
    if (used_active_nodes) {
        *used_active_nodes = false;
    }
    g_cypher_depth_clamped = 0;
    g_cypher_row_ceiling_hit = 0;
    cypher_deadline_arm();
    if (max_rows <= 0) {
        max_rows = CYPHER_RESULT_CEILING;
    }

    cbm_query_t *q = NULL;
    char *err = NULL;
    if (cbm_cypher_parse(query, &q, &err) < 0) {
        out->error = err;
        return CBM_NOT_FOUND;
    }

    cypher_node_scan_mode_t scan_mode = CYP_NODE_SCAN_CANONICAL;
    if (request_active_nodes && project && cypher_query_supports_active_nodes(q)) {
        scan_mode = CYP_NODE_SCAN_ACTIVE_OVERLAY;
        if (used_active_nodes) {
            *used_active_nodes = true;
        }
    }

    result_builder_t rb = {0};
    // cppcheck-suppress knownConditionTrueFalse
    if (execute_single(store, q, project, max_rows, scan_mode, &rb) < 0) {
        cbm_query_free(q);
        return CBM_NOT_FOUND;
    }

    /* UNION chain */
    cbm_query_t *uq = q->union_next;
    while (uq) {
        result_builder_t rb2 = {0};
        // cppcheck-suppress knownConditionTrueFalse
        if (execute_single(store, uq, project, max_rows, scan_mode, &rb2) < 0) {
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

    /* #601: abort a runaway query that blew the wall-clock budget before it can
     * return a misleading partial result. Checked before the row ceiling. */
    if (g_cypher_timed_out) {
        rb_free(&rb);
        cbm_query_free(q);
        out->error =
            heap_strdup("query exceeded the execution time limit — narrow the pattern with a WHERE "
                        "filter, use a directed MATCH instead of an unbounded OPTIONAL MATCH, or "
                        "add LIMIT");
        return CBM_NOT_FOUND;
    }

    /* Check ceiling */
    if (g_cypher_row_ceiling_hit > 0 || rb.row_count >= CYPHER_RESULT_CEILING) {
        rb_free(&rb);
        cbm_query_free(q);
        out->error = heap_strdup("result exceeded row ceiling; use narrower filters or add LIMIT");
        return CBM_NOT_FOUND;
    }

    out->columns = rb.columns;
    out->col_count = rb.col_count;
    out->rows = rb.rows;
    out->row_count = rb.row_count;
    if (g_cypher_depth_clamped > 0) {
        char wbuf[CBM_SZ_256];
        snprintf(wbuf, sizeof(wbuf),
                 "variable-length hop range clamped to the engine ceiling (%d) — an empty "
                 "result may mean \"clamped\", not \"no such path\"",
                 g_cypher_depth_clamped);
        out->warning = heap_strdup(wbuf);
    }

    cbm_query_free(q);
    return 0;
}

int cbm_cypher_execute(cbm_store_t *store, const char *query, const char *project, int max_rows,
                       cbm_cypher_result_t *out) {
    return cbm_cypher_execute_impl(store, query, project, max_rows, false, out, NULL);
}

int cbm_cypher_execute_active_nodes(cbm_store_t *store, const char *query, const char *project,
                                    int max_rows, cbm_cypher_result_t *out,
                                    bool *used_active_nodes) {
    return cbm_cypher_execute_impl(store, query, project, max_rows, true, out, used_active_nodes);
}

void cbm_cypher_result_free(cbm_cypher_result_t *r) {
    if (!r) {
        return;
    }
    free(r->warning);
    r->warning = NULL;
    for (int i = 0; i < r->col_count; i++) {
        safe_str_free(&r->columns[i]);
    }
    free(r->columns);
    for (int i = 0; i < r->row_count; i++) {
        for (int j = 0; j < r->col_count; j++) {
            safe_str_free(&r->rows[i][j]);
        }
        free(r->rows[i]);
    }
    free(r->rows);
    free(r->error);
    memset(r, 0, sizeof(*r));
}
