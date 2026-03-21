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

#include <ctype.h>
#include "foundation/compat_regex.h"
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// NOLINTNEXTLINE(misc-include-cleaner) — strings.h included for interface contract
#include <strings.h> // strcasecmp

/* ── Helpers ────────────────────────────────────────────────────── */

static char *heap_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *d = malloc(len + 1);
    if (d) {
        memcpy(d, s, len + 1);
    }
    return d;
}

static char *heap_strndup(const char *s, size_t n) {
    char *d = malloc(n + 1);
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
        r->capacity = r->capacity ? r->capacity * 2 : 32;
        r->tokens = safe_realloc(r->tokens, r->capacity * sizeof(cbm_token_t));
    }
    r->tokens[r->count++] = (cbm_token_t){.type = type, .text = heap_strdup(text), .pos = pos};
}

static void lex_push_n(cbm_lex_result_t *r, cbm_token_type_t type, const char *start, size_t len,
                       int pos) {
    if (r->count >= r->capacity) {
        r->capacity = r->capacity ? r->capacity * 2 : 32;
        r->tokens = safe_realloc(r->tokens, r->capacity * sizeof(cbm_token_t));
    }
    r->tokens[r->count++] =
        (cbm_token_t){.type = type, .text = heap_strndup(start, len), .pos = pos};
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
        // NOLINTNEXTLINE(misc-include-cleaner) — strcasecmp provided by standard header
        if (strcasecmp(word, kw->name) == 0) {
            return kw->type;
        }
    }
    return TOK_IDENT;
}

int cbm_lex(const char *input, cbm_lex_result_t *out) {
    memset(out, 0, sizeof(*out));
    if (!input) {
        return -1;
    }

    int len = (int)strlen(input);
    int i = 0;

    while (i < len) {
        /* Skip whitespace */
        if (isspace((unsigned char)input[i])) {
            i++;
            continue;
        }

        /* Skip // line comments */
        if (i + 1 < len && input[i] == '/' && input[i + 1] == '/') {
            while (i < len && input[i] != '\n') {
                i++;
            }
            continue;
        }
        /* Skip block comments */
        if (i + 1 < len && input[i] == '/' && input[i + 1] == '*') {
            i += 2;
            while (i + 1 < len && !(input[i] == '*' && input[i + 1] == '/')) {
                i++;
            }
            if (i + 1 < len) {
                i += 2;
            }
            continue;
        }

        char c = input[i];

        /* String literals */
        if (c == '"' || c == '\'') {
            char quote = c;
            i++;
            int start = i;
            /* Build string handling escapes */
            char buf[4096];
            int blen = 0;
            while (i < len && input[i] != quote) {
                if (input[i] == '\\' && i + 1 < len) {
                    i++;
                    switch (input[i]) {
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
                        buf[blen++] = input[i];
                        break;
                    }
                } else {
                    buf[blen++] = input[i];
                }
                i++;
            }
            buf[blen] = '\0';
            if (i < len) {
                i++; /* skip closing quote */
            }
            lex_push(out, TOK_STRING, buf, start - 1);
            continue;
        }

        /* Numbers — stop before ".." (DOTDOT operator) */
        if (isdigit((unsigned char)c) ||
            (c == '.' && i + 1 < len && isdigit((unsigned char)input[i + 1]))) {
            int start = i;
            while (i < len && (isdigit((unsigned char)input[i]) ||
                               (input[i] == '.' && i + 1 < len && input[i + 1] != '.'))) {
                i++;
            }
            lex_push_n(out, TOK_NUMBER, input + start, i - start, start);
            continue;
        }

        /* Identifiers / keywords */
        if (isalpha((unsigned char)c) || c == '_') {
            int start = i;
            while (i < len && (isalnum((unsigned char)input[i]) || input[i] == '_')) {
                i++;
            }
            char word[256];
            int wlen = i - start;
            if (wlen >= (int)sizeof(word)) {
                wlen = (int)sizeof(word) - 1;
            }
            memcpy(word, input + start, wlen);
            word[wlen] = '\0';
            cbm_token_type_t type = keyword_lookup(word);
            lex_push_n(out, type, input + start, i - start, start);
            continue;
        }

        /* Two-character tokens */
        if (c == '!' && i + 1 < len && input[i + 1] == '=') {
            lex_push(out, TOK_NEQ, "!=", i);
            i += 2;
            continue;
        }
        if (c == '<' && i + 1 < len && input[i + 1] == '>') {
            lex_push(out, TOK_NEQ, "<>", i);
            i += 2;
            continue;
        }
        if (c == '=' && i + 1 < len && input[i + 1] == '~') {
            lex_push(out, TOK_EQTILDE, "=~", i);
            i += 2;
            continue;
        }
        if (c == '>' && i + 1 < len && input[i + 1] == '=') {
            lex_push(out, TOK_GTE, ">=", i);
            i += 2;
            continue;
        }
        if (c == '<' && i + 1 < len && input[i + 1] == '=') {
            lex_push(out, TOK_LTE, "<=", i);
            i += 2;
            continue;
        }
        if (c == '.' && i + 1 < len && input[i + 1] == '.') {
            lex_push(out, TOK_DOTDOT, "..", i);
            i += 2;
            continue;
        }

        /* Single-character tokens */
        cbm_token_type_t stype = TOK_EOF;
        switch (c) {
        case '(':
            stype = TOK_LPAREN;
            break;
        case ')':
            stype = TOK_RPAREN;
            break;
        case '[':
            stype = TOK_LBRACKET;
            break;
        case ']':
            stype = TOK_RBRACKET;
            break;
        case '-':
            stype = TOK_DASH;
            break;
        case '>':
            stype = TOK_GT;
            break;
        case '<':
            stype = TOK_LT;
            break;
        case ':':
            stype = TOK_COLON;
            break;
        case '.':
            stype = TOK_DOT;
            break;
        case '{':
            stype = TOK_LBRACE;
            break;
        case '}':
            stype = TOK_RBRACE;
            break;
        case '*':
            stype = TOK_STAR;
            break;
        case ',':
            stype = TOK_COMMA;
            break;
        case '=':
            stype = TOK_EQ;
            break;
        case '|':
            stype = TOK_PIPE;
            break;
        default:
            break;
        }

        if (stype != TOK_EOF) {
            char buf[2] = {c, '\0'};
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
    char error[512];
} parser_t;

static const cbm_token_t *peek(parser_t *p) {
    if (p->pos >= p->count) {
        return &p->tokens[p->count - 1]; /* EOF */
    }
    return &p->tokens[p->pos];
}

static const cbm_token_t *advance(parser_t *p) {
    if (p->pos >= p->count) {
        return &p->tokens[p->count - 1];
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

    int cap = 4;
    int n = 0;
    cbm_prop_filter_t *arr = malloc(cap * sizeof(cbm_prop_filter_t));

    while (!check(p, TOK_RBRACE) && !check(p, TOK_EOF)) {
        const cbm_token_t *key = expect(p, TOK_IDENT);
        if (!key) {
            free(arr);
            return -1;
        }
        if (!expect(p, TOK_COLON)) {
            free(arr);
            return -1;
        }
        const cbm_token_t *val = expect(p, TOK_STRING);
        if (!val) {
            free(arr);
            return -1;
        }

        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_prop_filter_t));
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
        return -1;
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
            return -1;
        }
        out->label = heap_strdup(label->text);
    }

    /* Optional {props} */
    if (check(p, TOK_LBRACE)) {
        if (parse_props(p, &out->props, &out->prop_count) < 0) {
            return -1;
        }
    }

    if (!expect(p, TOK_RPAREN)) {
        return -1;
    }
    return 0;
}

/* Parse relationship: -[:TYPE|TYPE2*min..max]-> or <-[...]-  */
static int parse_rel(parser_t *p, cbm_rel_pattern_t *out) {
    memset(out, 0, sizeof(*out));
    out->min_hops = 1;
    out->max_hops = 1;

    /* Check for leading < (inbound) */
    bool leading_lt = match(p, TOK_LT);
    if (!expect(p, TOK_DASH)) {
        return -1;
    }

    /* Optional bracket content */
    if (match(p, TOK_LBRACKET)) {
        /* Optional variable */
        if (check(p, TOK_IDENT) && !check(p, TOK_COLON)) {
            /* Peek ahead: variable if followed by : or ] or * */
            /* Actually, check if next-next is colon */
            out->variable = heap_strdup(advance(p)->text);
        }

        /* Optional :Types */
        if (match(p, TOK_COLON)) {
            int cap = 4;
            int n = 0;
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            const char **types = malloc(cap * sizeof(const char *));

            const cbm_token_t *t = expect(p, TOK_IDENT);
            if (!t) {
                // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                free(types);
                return -1;
            }
            types[n++] = heap_strdup(t->text);

            while (match(p, TOK_PIPE)) {
                t = expect(p, TOK_IDENT);
                if (!t) {
                    for (int i = 0; i < n; i++) {
                        free((void *)types[i]);
                    }
                    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                    free(types);
                    return -1;
                }
                if (n >= cap) {
                    cap *= 2;
                    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                    types = safe_realloc(types, cap * sizeof(const char *));
                }
                types[n++] = heap_strdup(t->text);
            }

            out->types = types;
            out->type_count = n;
        }

        /* Optional *hop_range */
        if (match(p, TOK_STAR)) {
            /* Parse hop range */
            if (check(p, TOK_NUMBER)) {
                int val = (int)strtol(peek(p)->text, NULL, 10);
                advance(p);
                if (match(p, TOK_DOTDOT)) {
                    out->min_hops = val;
                    if (check(p, TOK_NUMBER)) {
                        out->max_hops = (int)strtol(advance(p)->text, NULL, 10);
                    } else {
                        out->max_hops = 0; /* unbounded */
                    }
                } else {
                    /* *N means 1..N */
                    out->min_hops = 1;
                    out->max_hops = val;
                }
            } else if (match(p, TOK_DOTDOT)) {
                out->min_hops = 1;
                if (check(p, TOK_NUMBER)) {
                    out->max_hops = (int)strtol(advance(p)->text, NULL, 10);
                } else {
                    out->max_hops = 0;
                }
            } else {
                /* * alone = unbounded */
                out->min_hops = 1;
                out->max_hops = 0;
            }
        }

        if (!expect(p, TOK_RBRACKET)) {
            return -1;
        }
    }

    if (!expect(p, TOK_DASH)) {
        return -1;
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
    if (!e) {
        return;
    }
    if (e->type == EXPR_CONDITION) {
        free((void *)e->cond.variable);
        free((void *)e->cond.property);
        free((void *)e->cond.op);
        free((void *)e->cond.value);
        for (int i = 0; i < e->cond.in_value_count; i++) {
            free((void *)e->cond.in_values[i]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(e->cond.in_values);
    }
    expr_free(e->left);
    expr_free(e->right);
    free(e);
}

static cbm_expr_t *expr_leaf(cbm_condition_t c) {
    cbm_expr_t *e = calloc(1, sizeof(cbm_expr_t));
    e->type = EXPR_CONDITION;
    e->cond = c;
    return e;
}

static cbm_expr_t *expr_binary(cbm_expr_type_t type, cbm_expr_t *left, cbm_expr_t *right) {
    cbm_expr_t *e = calloc(1, sizeof(cbm_expr_t));
    e->type = type;
    e->left = left;
    e->right = right;
    return e;
}

static cbm_expr_t *expr_not(cbm_expr_t *child) {
    cbm_expr_t *e = calloc(1, sizeof(cbm_expr_t));
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
        advance(p);
        c.op = heap_strdup("IN");
        if (!expect(p, TOK_LBRACKET)) {
            free((void *)c.variable);
            free((void *)c.property);
            free((void *)c.op);
            return NULL;
        }
        int vcap = 8;
        int vn = 0;
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        const char **vals = malloc(vcap * sizeof(const char *));
        while (!check(p, TOK_RBRACKET) && !check(p, TOK_EOF)) {
            if (vn > 0) {
                match(p, TOK_COMMA);
            }
            if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
                if (vn >= vcap) {
                    vcap *= 2;
                    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                    vals = safe_realloc(vals, vcap * sizeof(const char *));
                }
                vals[vn++] = heap_strdup(advance(p)->text);
            } else {
                break;
            }
        }
        expect(p, TOK_RBRACKET);
        c.in_values = vals;
        c.in_value_count = vn;
        return expr_leaf(c);
    }

    /* Standard operators */
    if (match(p, TOK_EQ)) {
        c.op = heap_strdup("=");
    } else if (match(p, TOK_NEQ)) {
        c.op = heap_strdup("<>");
    } else if (match(p, TOK_EQTILDE)) {
        c.op = heap_strdup("=~");
    } else if (match(p, TOK_GTE)) {
        c.op = heap_strdup(">=");
    } else if (match(p, TOK_LTE)) {
        c.op = heap_strdup("<=");
    } else if (match(p, TOK_GT)) {
        c.op = heap_strdup(">");
    } else if (match(p, TOK_LT)) {
        c.op = heap_strdup("<");
    } else if (check(p, TOK_CONTAINS)) {
        advance(p);
        c.op = heap_strdup("CONTAINS");
    } else if (check(p, TOK_STARTS)) {
        advance(p);
        expect(p, TOK_WITH);
        c.op = heap_strdup("STARTS WITH");
    } else if (check(p, TOK_ENDS)) {
        advance(p);
        expect(p, TOK_WITH);
        c.op = heap_strdup("ENDS WITH");
    } else {
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
static cbm_expr_t *parse_atom_expr(parser_t *p) {
    if (match(p, TOK_LPAREN)) {
        cbm_expr_t *e = parse_or_expr(p);
        expect(p, TOK_RPAREN);
        return e;
    }
    return parse_condition_expr(p);
}

/* NOT: NOT atom | atom */
static cbm_expr_t *parse_not_expr(parser_t *p) {
    if (match(p, TOK_NOT)) {
        cbm_expr_t *child = parse_not_expr(p);
        return child ? expr_not(child) : NULL;
    }
    return parse_atom_expr(p);
}

/* AND: not (AND not)* */
static cbm_expr_t *parse_and_expr(parser_t *p) {
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
static cbm_expr_t *parse_xor_expr(parser_t *p) {
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
static cbm_expr_t *parse_or_expr(parser_t *p) {
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

    cbm_where_clause_t *w = calloc(1, sizeof(cbm_where_clause_t));
    w->root = parse_or_expr(p);
    if (!w->root && p->error[0]) {
        free(w);
        return -1;
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

/* Parse CASE WHEN ... THEN ... [ELSE ...] END */
static cbm_case_expr_t *parse_case_expr(parser_t *p) {
    /* CASE already consumed */
    cbm_case_expr_t *kase = calloc(1, sizeof(cbm_case_expr_t));
    int bcap = 4;
    kase->branches = malloc(bcap * sizeof(cbm_case_branch_t));

    while (check(p, TOK_WHEN)) {
        advance(p);
        cbm_expr_t *when = parse_or_expr(p);
        if (!expect(p, TOK_THEN)) {
            expr_free(when);
            break;
        }
        const char *then_val = NULL;
        if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
            then_val = heap_strdup(advance(p)->text);
        } else if (check(p, TOK_IDENT)) {
            /* Could be var.prop */
            char buf[256];
            const cbm_token_t *v = advance(p);
            if (match(p, TOK_DOT)) {
                const cbm_token_t *pr = expect(p, TOK_IDENT);
                snprintf(buf, sizeof(buf), "%s.%s", v->text, pr ? pr->text : "");
            } else {
                snprintf(buf, sizeof(buf), "%s", v->text);
            }
            then_val = heap_strdup(buf);
        } else if (check(p, TOK_TRUE)) {
            advance(p);
            then_val = heap_strdup("true");
        } else if (check(p, TOK_FALSE)) {
            advance(p);
            then_val = heap_strdup("false");
        }
        if (kase->branch_count >= bcap) {
            bcap *= 2;
            kase->branches = safe_realloc(kase->branches, bcap * sizeof(cbm_case_branch_t));
        }
        kase->branches[kase->branch_count++] =
            (cbm_case_branch_t){.when_expr = when, .then_val = then_val};
    }

    if (match(p, TOK_ELSE)) {
        if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
            kase->else_val = heap_strdup(advance(p)->text);
        } else if (check(p, TOK_IDENT)) {
            kase->else_val = heap_strdup(advance(p)->text);
        }
    }
    expect(p, TOK_END);
    return kase;
}

/* Parse RETURN/WITH clause (shared logic) */
static int parse_return_or_with(parser_t *p, cbm_return_clause_t **out, bool is_with) {
    cbm_token_type_t tok = (int)is_with ? TOK_WITH : TOK_RETURN;
    /* For WITH, we need to check it's standalone (not preceded by STARTS) */
    if (!match(p, tok)) {
        *out = NULL;
        return 0;
    }

    cbm_return_clause_t *r = calloc(1, sizeof(cbm_return_clause_t));
    int cap = 8;
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

        /* CASE expression */
        if (check(p, TOK_CASE)) {
            advance(p);
            item.kase = parse_case_expr(p);
            item.variable = heap_strdup("CASE");
        }
        /* Aggregate: COUNT/SUM/AVG/MIN/MAX/COLLECT */
        else if (is_aggregate_tok(peek(p)->type)) {
            cbm_token_type_t ft = peek(p)->type;
            advance(p);
            expect(p, TOK_LPAREN);
            if (match(p, TOK_STAR)) {
                item.variable = heap_strdup("*");
            } else {
                const cbm_token_t *var = expect(p, TOK_IDENT);
                if (!var) {
                    free(r->items);
                    free(r);
                    return -1;
                }
                item.variable = heap_strdup(var->text);
                if (match(p, TOK_DOT)) {
                    const cbm_token_t *prop = expect(p, TOK_IDENT);
                    if (prop) {
                        item.property = heap_strdup(prop->text);
                    }
                }
            }
            expect(p, TOK_RPAREN);
            item.func = heap_strdup(agg_func_name(ft));
        }
        /* String function: toLower/toUpper/toString */
        else if (is_string_func_tok(peek(p)->type)) {
            cbm_token_type_t ft = peek(p)->type;
            advance(p);
            expect(p, TOK_LPAREN);
            const cbm_token_t *var = expect(p, TOK_IDENT);
            if (!var) {
                free(r->items);
                free(r);
                return -1;
            }
            item.variable = heap_strdup(var->text);
            if (match(p, TOK_DOT)) {
                const cbm_token_t *prop = expect(p, TOK_IDENT);
                if (prop) {
                    item.property = heap_strdup(prop->text);
                }
            }
            expect(p, TOK_RPAREN);
            item.func = heap_strdup(str_func_name(ft));
        }
        /* Plain var.prop */
        else {
            const cbm_token_t *var = expect(p, TOK_IDENT);
            if (!var) {
                free(r->items);
                free(r);
                return -1;
            }
            item.variable = heap_strdup(var->text);
            if (match(p, TOK_DOT)) {
                const cbm_token_t *prop = expect(p, TOK_IDENT);
                if (prop) {
                    item.property = heap_strdup(prop->text);
                }
            }
        }

        /* Optional AS alias */
        if (match(p, TOK_AS)) {
            const cbm_token_t *alias = expect(p, TOK_IDENT);
            if (alias) {
                item.alias = heap_strdup(alias->text);
            }
        }

        if (r->count >= cap) {
            cap *= 2;
            r->items = safe_realloc(r->items, cap * sizeof(cbm_return_item_t));
        }
        r->items[r->count++] = item;

    } while (check(p, TOK_COMMA));

tail:
    /* Optional ORDER BY */
    if (match(p, TOK_ORDER)) {
        expect(p, TOK_BY);
        char order_buf[256] = "";
        if (is_aggregate_tok(peek(p)->type)) {
            const char *fn = agg_func_name(peek(p)->type);
            advance(p);
            expect(p, TOK_LPAREN);
            if (match(p, TOK_STAR)) {
                snprintf(order_buf, sizeof(order_buf), "%s(*)", fn);
            } else {
                const cbm_token_t *var = expect(p, TOK_IDENT);
                snprintf(order_buf, sizeof(order_buf), "%s(%s)", fn, var ? var->text : "");
            }
            expect(p, TOK_RPAREN);
        } else {
            const cbm_token_t *var = expect(p, TOK_IDENT);
            if (var) {
                snprintf(order_buf, sizeof(order_buf), "%s", var->text);
                if (match(p, TOK_DOT)) {
                    const cbm_token_t *prop = expect(p, TOK_IDENT);
                    if (prop) {
                        char tmp[256];
                        snprintf(tmp, sizeof(tmp), "%s.%s", var->text, prop->text);
                        snprintf(order_buf, sizeof(order_buf), "%s", tmp);
                    }
                }
            }
        }
        r->order_by = heap_strdup(order_buf);
        if (match(p, TOK_ASC)) {
            r->order_dir = heap_strdup("ASC");
        } else if (match(p, TOK_DESC)) {
            r->order_dir = heap_strdup("DESC");
        }
    }

    /* Optional SKIP */
    if (match(p, TOK_SKIP)) {
        const cbm_token_t *num = expect(p, TOK_NUMBER);
        if (num) {
            r->skip = (int)strtol(num->text, NULL, 10);
        }
    }

    /* Optional LIMIT */
    if (match(p, TOK_LIMIT)) {
        const cbm_token_t *num = expect(p, TOK_NUMBER);
        if (num) {
            r->limit = (int)strtol(num->text, NULL, 10);
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
    int node_cap = 4;
    int rel_cap = 4;
    pat->nodes = malloc(node_cap * sizeof(cbm_node_pattern_t));
    pat->rels = malloc(rel_cap * sizeof(cbm_rel_pattern_t));

    if (parse_node(p, &pat->nodes[0]) < 0) {
        return -1;
    }
    pat->node_count = 1;

    while (check(p, TOK_DASH) || check(p, TOK_LT)) {
        if (pat->rel_count >= rel_cap) {
            rel_cap *= 2;
            pat->rels = safe_realloc(pat->rels, rel_cap * sizeof(cbm_rel_pattern_t));
        }
        if (parse_rel(p, &pat->rels[pat->rel_count]) < 0) {
            return -1;
        }
        pat->rel_count++;

        if (pat->node_count >= node_cap) {
            node_cap *= 2;
            pat->nodes = safe_realloc(pat->nodes, node_cap * sizeof(cbm_node_pattern_t));
        }
        if (parse_node(p, &pat->nodes[pat->node_count]) < 0) {
            return -1;
        }
        pat->node_count++;
    }
    return 0;
}

int cbm_parse(const cbm_token_t *tokens, int token_count, cbm_parse_result_t *out) {
    memset(out, 0, sizeof(*out));
    parser_t p = {.tokens = tokens, .count = token_count, .pos = 0};

    /* Check for unsupported leading keywords */
    const char *unsup = unsupported_clause_error(peek(&p)->type);
    if (unsup) {
        out->error = heap_strdup(unsup);
        return -1;
    }

    /* UNWIND ... AS var (before MATCH) */
    cbm_query_t *q = calloc(1, sizeof(cbm_query_t));

    if (check(&p, TOK_UNWIND)) {
        advance(&p);
        /* Parse expression — could be a literal list or a variable */
        if (check(&p, TOK_LBRACKET)) {
            /* Literal list: [1, 2, 3] — collect as JSON array string */
            advance(&p);
            char buf[2048] = "[";
            int blen = 1;
            while (!check(&p, TOK_RBRACKET) && !check(&p, TOK_EOF)) {
                if (blen > 1) {
                    buf[blen++] = ',';
                }
                if (check(&p, TOK_STRING)) {
                    blen += snprintf(buf + blen, sizeof(buf) - blen, "\"%s\"", peek(&p)->text);
                    advance(&p);
                } else if (check(&p, TOK_NUMBER)) {
                    blen += snprintf(buf + blen, sizeof(buf) - blen, "%s", peek(&p)->text);
                    advance(&p);
                } else {
                    advance(&p);
                }
                match(&p, TOK_COMMA);
            }
            expect(&p, TOK_RBRACKET);
            buf[blen++] = ']';
            buf[blen] = '\0';
            q->unwind_expr = heap_strdup(buf);
        } else if (check(&p, TOK_IDENT)) {
            q->unwind_expr = heap_strdup(advance(&p)->text);
        }
        expect(&p, TOK_AS);
        const cbm_token_t *alias = expect(&p, TOK_IDENT);
        if (alias) {
            q->unwind_alias = heap_strdup(alias->text);
        }
    }

    /* Expect MATCH or OPTIONAL MATCH */
    bool first_optional = false;
    if (check(&p, TOK_OPTIONAL)) {
        advance(&p);
        first_optional = true;
    }
    if (!expect(&p, TOK_MATCH)) {
        out->error = heap_strdup(p.error[0] ? p.error : "expected MATCH");
        cbm_query_free(q);
        return -1;
    }

    /* Parse first pattern */
    int pat_cap = 4;
    q->patterns = malloc(pat_cap * sizeof(cbm_pattern_t));
    q->pattern_optional = malloc(pat_cap * sizeof(bool));

    if (parse_match_pattern(&p, &q->patterns[0]) < 0) {
        out->error = heap_strdup(p.error[0] ? p.error : "failed to parse pattern");
        cbm_query_free(q);
        return -1;
    }
    q->pattern_optional[0] = first_optional;
    q->pattern_count = 1;

    /* Additional MATCH / OPTIONAL MATCH patterns */
    while (check(&p, TOK_MATCH) || check(&p, TOK_OPTIONAL)) {
        bool opt = false;
        if (check(&p, TOK_OPTIONAL)) {
            advance(&p);
            opt = true;
        }
        if (!expect(&p, TOK_MATCH)) {
            break;
        }
        if (q->pattern_count >= pat_cap) {
            pat_cap *= 2;
            q->patterns = safe_realloc(q->patterns, pat_cap * sizeof(cbm_pattern_t));
            q->pattern_optional = safe_realloc(q->pattern_optional, pat_cap * sizeof(bool));
        }
        if (parse_match_pattern(&p, &q->patterns[q->pattern_count]) < 0) {
            out->error = heap_strdup(p.error[0] ? p.error : "failed to parse additional pattern");
            cbm_query_free(q);
            return -1;
        }
        q->pattern_optional[q->pattern_count] = opt;
        q->pattern_count++;
    }

    /* Optional WHERE */
    if (parse_where(&p, &q->where) < 0) {
        out->error = heap_strdup(p.error[0] ? p.error : "failed to parse WHERE");
        cbm_query_free(q);
        return -1;
    }

    /* More MATCH / OPTIONAL MATCH after WHERE (e.g. MATCH ... WHERE ... OPTIONAL MATCH ...) */
    while (check(&p, TOK_MATCH) || check(&p, TOK_OPTIONAL)) {
        bool opt2 = false;
        if (check(&p, TOK_OPTIONAL)) {
            advance(&p);
            opt2 = true;
        }
        if (!expect(&p, TOK_MATCH)) {
            break;
        }
        if (q->pattern_count >= pat_cap) {
            pat_cap *= 2;
            q->patterns = safe_realloc(q->patterns, pat_cap * sizeof(cbm_pattern_t));
            q->pattern_optional = safe_realloc(q->pattern_optional, pat_cap * sizeof(bool));
        }
        if (parse_match_pattern(&p, &q->patterns[q->pattern_count]) < 0) {
            out->error = heap_strdup(p.error[0] ? p.error : "failed to parse post-WHERE pattern");
            cbm_query_free(q);
            return -1;
        }
        q->pattern_optional[q->pattern_count] = opt2;
        q->pattern_count++;
    }

    /* Check for unsupported keywords after WHERE */
    unsup = unsupported_clause_error(peek(&p)->type);
    if (unsup) {
        out->error = heap_strdup(unsup);
        cbm_query_free(q);
        return -1;
    }

    /* Optional WITH clause (standalone, not STARTS WITH) */
    if (check(&p, TOK_WITH) && (p.pos < 2 || p.tokens[p.pos - 1].type != TOK_STARTS)) {
        /* Make sure this isn't inside a WHERE STARTS WITH */
        if (parse_return_or_with(&p, &q->with_clause, true) < 0) {
            out->error = heap_strdup(p.error[0] ? p.error : "failed to parse WITH");
            cbm_query_free(q);
            return -1;
        }
        /* Optional post-WITH WHERE */
        if (parse_where(&p, &q->post_with_where) < 0) {
            out->error = heap_strdup(p.error[0] ? p.error : "failed to parse post-WITH WHERE");
            cbm_query_free(q);
            return -1;
        }
    }

    /* Optional RETURN */
    if (parse_return(&p, &q->ret) < 0) {
        out->error = heap_strdup(p.error[0] ? p.error : "failed to parse RETURN");
        cbm_query_free(q);
        return -1;
    }

    /* UNION [ALL] → recursive parse of rest */
    if (check(&p, TOK_UNION)) {
        advance(&p);
        q->union_all = match(&p, TOK_ALL);
        /* Parse remaining tokens as a new query */
        cbm_parse_result_t sub = {0};
        if (cbm_parse(&p.tokens[p.pos], p.count - p.pos, &sub) < 0) {
            out->error = heap_strdup(sub.error ? sub.error : "failed to parse UNION query");
            cbm_parse_free(&sub);
            cbm_query_free(q);
            return -1;
        }
        q->union_next = sub.query;
        sub.query = NULL;
        cbm_parse_free(&sub);
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
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
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
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
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
    if (!q) {
        return;
    }

    for (int i = 0; i < q->pattern_count; i++) {
        free_pattern(&q->patterns[i]);
    }
    free(q->patterns);
    free(q->pattern_optional);

    free_where(q->where);
    free_where(q->post_with_where);
    free_return_clause(q->with_clause);
    free_return_clause(q->ret);

    cbm_query_free(q->union_next);

    free((void *)q->unwind_expr);
    free((void *)q->unwind_alias);

    free(q);
}

/* ── Convenience: lex + parse ───────────────────────────────────── */

int cbm_cypher_parse(const char *query, cbm_query_t **out, char **error) {
    *out = NULL;
    *error = NULL;

    cbm_lex_result_t lr = {0};
    if (cbm_lex(query, &lr) < 0 || lr.error) {
        *error = heap_strdup(lr.error ? lr.error : "lex error");
        cbm_lex_free(&lr);
        return -1;
    }

    cbm_parse_result_t pr = {0};
    if (cbm_parse(lr.tokens, lr.count, &pr) < 0) {
        *error = heap_strdup(pr.error ? pr.error : "parse error");
        cbm_parse_free(&pr);
        cbm_lex_free(&lr);
        return -1;
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
    const char *var_names[16]; /* variable names (nodes) */
    cbm_node_t var_nodes[16];  /* node data */
    int var_count;
    const char *edge_var_names[8]; /* variable names (edges) */
    cbm_edge_t edge_vars[8];       /* edge data */
    int edge_var_count;
} binding_t;

/* Get node property by name */
static const char *node_prop(const cbm_node_t *n, const char *prop) {
    if (!n || !prop) {
        return "";
    }
    if (strcmp(prop, "name") == 0) {
        return n->name ? n->name : "";
    }
    if (strcmp(prop, "qualified_name") == 0) {
        return n->qualified_name ? n->qualified_name : "";
    }
    if (strcmp(prop, "label") == 0) {
        return n->label ? n->label : "";
    }
    if (strcmp(prop, "file_path") == 0) {
        return n->file_path ? n->file_path : "";
    }
    if (strcmp(prop, "start_line") == 0) {
        /* Return as string */
        static char buf[32];
        snprintf(buf, sizeof(buf), "%d", n->start_line);
        return buf;
    }
    if (strcmp(prop, "end_line") == 0) {
        static char buf[32];
        snprintf(buf, sizeof(buf), "%d", n->end_line);
        return buf;
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
    char pattern[256];
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
        while (*p && *p != '"' && i < buf_sz - 1) {
            buf[i++] = *p++;
        }
        buf[i] = '\0';
    } else {
        /* Numeric or other value */
        size_t i = 0;
        while (*p && *p != ',' && *p != '}' && *p != ' ' && i < buf_sz - 1) {
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
    static char ebufs[8][512];
    static int ebuf_idx = 0;
    char *buf = ebufs[ebuf_idx++ & 7];
    json_extract_prop(e->properties_json, prop, buf, 512);
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
    if (b->edge_var_count >= 8) {
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
    if (b->var_count >= 16) {
        return;
    }
    b->var_names[b->var_count] = var; /* not owned — points to AST string */
    node_deep_copy(&b->var_nodes[b->var_count], node);
    b->var_count++;
}

/* Evaluate a WHERE condition against a binding */
static bool eval_condition(const cbm_condition_t *c, binding_t *b) {
    const char *actual;

    /* Check if variable is an edge binding */
    cbm_edge_t *e = binding_get_edge(b, c->variable);
    if (e) {
        actual = edge_prop(e, c->property);
    } else {
        cbm_node_t *n = binding_get(b, c->variable);
        if (!n) {
            /* Unbound variable — optimistic pass-through for early WHERE */
            return true;
        }
        if (c->property) {
            actual = node_prop(n, c->property);
        } else {
            /* Bare alias (e.g. post-WITH virtual var) — use node name directly */
            actual = n->name ? n->name : "";
        }
    }

    bool result;

    /* IS NULL / IS NOT NULL */
    if (strcmp(c->op, "IS NULL") == 0) {
        result = ((!actual || actual[0] == '\0') != 0);
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return (int)(c->negated ? !result : result);
    }
    if (strcmp(c->op, "IS NOT NULL") == 0) {
        result = ((actual && actual[0] != '\0') != 0);
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return (int)(c->negated ? !result : result);
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
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return (int)(c->negated ? !result : result);
    }

    const char *expected = c->value;

    if (strcmp(c->op, "=") == 0) {
        result = strcmp(actual, expected) == 0;
    } else if (strcmp(c->op, "<>") == 0) {
        result = strcmp(actual, expected) != 0;
    } else if (strcmp(c->op, "=~") == 0) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, expected, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            return false;
        }
        int rc = cbm_regexec(&re, actual, 0, NULL, 0);
        cbm_regfree(&re);
        result = (rc == 0);
    } else if (strcmp(c->op, "CONTAINS") == 0) {
        result = strstr(actual, expected) != NULL;
    } else if (strcmp(c->op, "STARTS WITH") == 0) {
        result = strncmp(actual, expected, strlen(expected)) == 0;
    } else if (strcmp(c->op, "ENDS WITH") == 0) {
        size_t alen = strlen(actual);
        size_t elen = strlen(expected);
        result = ((alen >= elen && strcmp(actual + alen - elen, expected) == 0) != 0);
    } else if (strcmp(c->op, ">") == 0 || strcmp(c->op, "<") == 0 || strcmp(c->op, ">=") == 0 ||
               strcmp(c->op, "<=") == 0) {
        double a = strtod(actual, NULL);
        double exp_val = strtod(expected, NULL);
        if (strcmp(c->op, ">") == 0) {
            result = a > exp_val;
        } else if (strcmp(c->op, "<") == 0) {
            result = a < exp_val;
        } else if (strcmp(c->op, ">=") == 0) {
            result = a >= exp_val;
        } else {
            result = a <= exp_val;
        }
    } else {
        result = false;
    }

    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return (int)(c->negated ? !result : result);
}

/* Recursive expression tree evaluator */
static bool eval_expr(const cbm_expr_t *e, binding_t *b) {
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

/* Check inline property filters */
static bool check_inline_props(const cbm_node_t *n, const cbm_prop_filter_t *props, int count) {
    for (int i = 0; i < count; i++) {
        const char *actual = node_prop(n, props[i].key);
        if (strcmp(actual, props[i].value) != 0) {
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
    rb->row_cap = 32;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    rb->rows = malloc(rb->row_cap * sizeof(const char **));
}

static void rb_set_columns(result_builder_t *rb, const char **cols, int count) {
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion,
    rb->columns = malloc(count * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        rb->columns[i] = heap_strdup(cols[i]);
    }
    rb->col_count = count;
}

static void rb_add_row(result_builder_t *rb, const char **values) {
    if (rb->row_count >= rb->row_cap) {
        rb->row_cap *= 2;
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        rb->rows = safe_realloc(rb->rows, rb->row_cap * sizeof(const char **));
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    const char **row = malloc(rb->col_count * sizeof(const char *));
    for (int i = 0; i < rb->col_count; i++) {
        row[i] = heap_strdup(values[i]);
    }
    rb->rows[rb->row_count++] = row;
}

/* ── Main execution ─────────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters,readability-function-cognitive-complexity,readability-function-size)
/* Hard ceiling: queries returning more than this trigger an error instead of data.
 * Prevents accidental multi-GB JSON payloads from unbounded MATCH (n) RETURN n. */
#define CYPHER_RESULT_CEILING 100000

/* ── Binding virtual variables (for WITH clause) ──────────────── */

static const char *binding_get_virtual(binding_t *b, const char *var, const char *prop) {
    /* Check virtual vars first (from WITH projection) */
    char full[256];
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
        return prop ? node_prop(n, prop) : (n->name ? n->name : "");
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
        for (; i < buf_sz - 1 && val[i]; i++) {
            buf[i] = (char)tolower((unsigned char)val[i]);
        }
        buf[i] = '\0';
        return buf;
    }
    if (strcmp(func, "toUpper") == 0) {
        size_t i = 0;
        for (; i < buf_sz - 1 && val[i]; i++) {
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
        cbm_search_params_t params = {
            .project = project, .min_degree = -1, .max_degree = -1, .limit = max_rows * 10};
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
            if (check_inline_props(&(*out_nodes)[i], first->props, first->prop_count)) {
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

static void expand_pattern_rels(cbm_store_t *store, cbm_pattern_t *pat, binding_t **bindings,
                                int *bind_count, const int *bind_cap, const char **var_name,
                                bool is_optional) {
    for (int ri = 0; ri < pat->rel_count; ri++) {
        cbm_rel_pattern_t *rel = &pat->rels[ri];
        cbm_node_pattern_t *target_node = &pat->nodes[ri + 1];
        const char *to_var = target_node->variable ? target_node->variable : "_n_t";

        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        bool is_variable_length = (rel->min_hops != 1 || rel->max_hops != 1);

        binding_t *new_bindings = malloc(((*bind_cap * 10) + 1) * sizeof(binding_t));
        int new_count = 0;

        for (int bi = 0; bi < *bind_count; bi++) {
            binding_t *b = &(*bindings)[bi];
            cbm_node_t *src = binding_get(b, *var_name);
            if (!src) {
                continue;
            }

            int match_count = 0;

            if (is_variable_length) {
                int max_depth = rel->max_hops > 0 ? rel->max_hops : 10;
                cbm_traverse_result_t tr = {0};
                const char *dir = rel->direction ? rel->direction : "outbound";
                cbm_store_bfs(store, src->id, dir, rel->types, rel->type_count, max_depth, 100,
                              &tr);
                for (int v = 0; v < tr.visited_count && new_count < *bind_cap * 10; v++) {
                    cbm_node_hop_t *hop = &tr.visited[v];
                    if (hop->hop < rel->min_hops) {
                        continue;
                    }
                    if (target_node->label && strcmp(hop->node.label, target_node->label) != 0) {
                        continue;
                    }
                    if (!check_inline_props(&hop->node, target_node->props,
                                            target_node->prop_count)) {
                        continue;
                    }
                    binding_t nb = {0};
                    binding_copy(&nb, b);
                    binding_set(&nb, to_var, &hop->node);
                    new_bindings[new_count++] = nb;
                    match_count++;
                }
                cbm_store_traverse_free(&tr);
            } else {
                // NOLINTNEXTLINE(readability-implicit-bool-conversion)
                bool is_inbound = rel->direction && strcmp(rel->direction, "inbound") == 0;
                // NOLINTNEXTLINE(readability-implicit-bool-conversion)
                bool is_any = rel->direction && strcmp(rel->direction, "any") == 0;
                const char *rel_var = rel->variable;

#define PROCESS_EDGES(edges_arr, edge_count_val, get_target_id)                         \
    for (int ei = 0; ei < (edge_count_val) && new_count < *bind_cap * 10; ei++) {       \
        int64_t tid = (get_target_id);                                                  \
        cbm_node_t found = {0};                                                         \
        if (cbm_store_find_node_by_id(store, tid, &found) != CBM_STORE_OK)              \
            continue;                                                                   \
        if (target_node->label && strcmp(found.label, target_node->label) != 0) {       \
            node_fields_free(&found);                                                   \
            continue;                                                                   \
        }                                                                               \
        if (!check_inline_props(&found, target_node->props, target_node->prop_count)) { \
            node_fields_free(&found);                                                   \
            continue;                                                                   \
        }                                                                               \
        binding_t nb = {0};                                                             \
        binding_copy(&nb, b);                                                           \
        binding_set(&nb, to_var, &found);                                               \
        if (rel_var)                                                                    \
            binding_set_edge(&nb, rel_var, &(edges_arr)[ei]);                           \
        node_fields_free(&found);                                                       \
        new_bindings[new_count++] = nb;                                                 \
        match_count++;                                                                  \
    }

                if (rel->type_count > 0) {
                    for (int ti = 0; ti < rel->type_count; ti++) {
                        cbm_edge_t *edges = NULL;
                        int edge_count = 0;
                        if (is_inbound) {
                            cbm_store_find_edges_by_target_type(store, src->id, rel->types[ti],
                                                                &edges, &edge_count);
                        } else {
                            cbm_store_find_edges_by_source_type(store, src->id, rel->types[ti],
                                                                &edges, &edge_count);
                        }
                        PROCESS_EDGES(edges, edge_count,
                                      is_inbound ? edges[ei].source_id : edges[ei].target_id);
                        cbm_store_free_edges(edges, edge_count);
                    }
                    if (is_any) {
                        for (int ti = 0; ti < rel->type_count; ti++) {
                            cbm_edge_t *edges = NULL;
                            int edge_count = 0;
                            cbm_store_find_edges_by_target_type(store, src->id, rel->types[ti],
                                                                &edges, &edge_count);
                            PROCESS_EDGES(edges, edge_count, edges[ei].source_id);
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
                    PROCESS_EDGES(edges, edge_count,
                                  is_inbound ? edges[ei].source_id : edges[ei].target_id);
                    cbm_store_free_edges(edges, edge_count);
                    if (is_any) {
                        edges = NULL;
                        edge_count = 0;
                        cbm_store_find_edges_by_target(store, src->id, &edges, &edge_count);
                        PROCESS_EDGES(edges, edge_count, edges[ei].source_id);
                        cbm_store_free_edges(edges, edge_count);
                    }
                }
#undef PROCESS_EDGES
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

static void rb_apply_order_by(result_builder_t *rb, const cbm_return_clause_t *ret) {
    if (!ret->order_by) {
        return;
    }
    int order_col = -1;
    for (int ci = 0; ci < rb->col_count; ci++) {
        if (strcmp(rb->columns[ci], ret->order_by) == 0) {
            order_col = ci;
            break;
        }
    }
    if (order_col < 0) {
        for (int ci = 0; ci < ret->count; ci++) {
            if (ret->items[ci].alias && strcmp(ret->items[ci].alias, ret->order_by) == 0) {
                order_col = ci;
                break;
            }
        }
    }
    if (order_col < 0) {
        return;
    }

    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool desc = ret->order_dir && strcmp(ret->order_dir, "DESC") == 0;
    bool numeric = false;
    for (int i = 0; i < rb->row_count; i++) {
        const char *v = rb->rows[i][order_col];
        if (v && *v) {
            const char *p2 = (*v == '-') ? v + 1 : v;
            numeric = (*p2 != '\0');
            for (; *p2; p2++) {
                if (*p2 < '0' || *p2 > '9') {
                    numeric = false;
                    break;
                }
            }
            break;
        }
    }
    for (int i = 0; i < rb->row_count - 1; i++) {
        for (int j = 0; j < rb->row_count - i - 1; j++) {
            int cmp;
            if (numeric) {
                cmp = (int)strtol(rb->rows[j][order_col], NULL, 10) -
                      (int)strtol(rb->rows[j + 1][order_col], NULL, 10);
            } else {
                cmp = strcmp(rb->rows[j][order_col], rb->rows[j + 1][order_col]);
            }
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            if (desc ? cmp < 0 : cmp > 0) {
                const char **tmp = rb->rows[j];
                rb->rows[j] = rb->rows[j + 1];
                rb->rows[j + 1] = tmp;
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
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            free(rb->rows[i]);
        }
        memmove(rb->rows, rb->rows + skip_n, (rb->row_count - skip_n) * sizeof(const char **));
        rb->row_count -= skip_n;
    } else if (skip_n >= rb->row_count) {
        for (int i = 0; i < rb->row_count; i++) {
            for (int c = 0; c < rb->col_count; c++) {
                free((void *)rb->rows[i][c]);
            }
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
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
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            free(rb->rows[i]);
        }
        rb->row_count = limit;
    }
}

static void rb_apply_distinct(result_builder_t *rb) {
    if (rb->row_count <= 1) {
        return;
    }
    int kept = 1;
    for (int i = 1; i < rb->row_count; i++) {
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
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
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
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(rb->rows[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(rb->rows);
    for (int i = 0; i < rb->col_count; i++) {
        free((void *)rb->columns[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
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

/* ── Execute a single query (no UNION recursion) ──────────────── */

// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
static int execute_single(cbm_store_t *store, cbm_query_t *q, const char *project, int max_rows,
                          result_builder_t *rb) {
    cbm_pattern_t *pat0 = &q->patterns[0];

    /* Step 1: Scan initial nodes for first pattern */
    cbm_node_t *scanned = NULL;
    int scan_count = 0;
    scan_pattern_nodes(store, project, max_rows, &pat0->nodes[0], &scanned, &scan_count);

    /* Build initial bindings */
    int bind_cap = scan_count > 0 ? scan_count : 1;
    binding_t *bindings = malloc((bind_cap + 1) * sizeof(binding_t));
    int bind_count = 0;
    const char *var_name = pat0->nodes[0].variable ? pat0->nodes[0].variable : "_n0";

    /* Early WHERE: evaluate full expression tree (unbound vars return true) */
    for (int i = 0; i < scan_count && bind_count < bind_cap; i++) {
        binding_t b = {0};
        binding_set(&b, var_name, &scanned[i]);

        bool pass = true;
        if (q->where && pat0->rel_count > 0) {
            /* With expression tree, evaluate full tree — unbound vars pass through */
            pass = eval_where(q->where, &b);
        } else if (q->where && pat0->rel_count == 0) {
            pass = eval_where(q->where, &b);
        }

        if (pass) {
            bindings[bind_count++] = b;
        } else {
            binding_free(&b);
        }
    }

    /* Step 2: Expand first pattern's relationships */
    bool opt0 = q->pattern_optional[0];
    expand_pattern_rels(store, pat0, &bindings, &bind_count, &bind_cap, &var_name, opt0);

    /* Step 2b: Additional MATCH / OPTIONAL MATCH patterns */
    for (int pi = 1; pi < q->pattern_count; pi++) {
        cbm_pattern_t *patn = &q->patterns[pi];
        bool opt = q->pattern_optional[pi];

        /* Check if the second pattern's start node variable is already bound */
        const char *nvar = patn->nodes[0].variable ? patn->nodes[0].variable : "_n_extra";
        bool start_bound = (bind_count > 0 && binding_get(&bindings[0], nvar) != NULL) != 0;

        if (start_bound && patn->rel_count > 0) {
            /* Start variable already bound: expand rels directly from existing bindings */
            const char *tv = nvar;
            expand_pattern_rels(store, patn, &bindings, &bind_count, &bind_cap, &tv, opt);
        } else {
            /* Start variable not bound: scan + cross-join */
            cbm_node_t *extra_nodes = NULL;
            int extra_count = 0;
            scan_pattern_nodes(store, project, max_rows, &patn->nodes[0], &extra_nodes,
                               &extra_count);

            if (patn->rel_count == 0) {
                /* Node-only pattern: cross join */
                binding_t *new_bindings =
                    malloc(((bind_count * extra_count) + 1) * sizeof(binding_t));
                int new_count = 0;
                for (int bi = 0; bi < bind_count; bi++) {
                    for (int ni = 0; ni < extra_count; ni++) {
                        binding_t nb = {0};
                        binding_copy(&nb, &bindings[bi]);
                        binding_set(&nb, nvar, &extra_nodes[ni]);
                        new_bindings[new_count++] = nb;
                    }
                    if (opt && extra_count == 0) {
                        binding_t nb = {0};
                        binding_copy(&nb, &bindings[bi]);
                        new_bindings[new_count++] = nb;
                    }
                }
                for (int bi = 0; bi < bind_count; bi++) {
                    binding_free(&bindings[bi]);
                }
                free(bindings);
                bindings = new_bindings;
                bind_count = new_count;
            } else {
                /* Pattern with rels: for each binding, set start node + expand */
                binding_t *new_bindings =
                    malloc(((bind_count * extra_count * 10) + 1) * sizeof(binding_t));
                int new_count = 0;
                for (int bi = 0; bi < bind_count; bi++) {
                    for (int ni = 0; ni < extra_count; ni++) {
                        binding_t nb = {0};
                        binding_copy(&nb, &bindings[bi]);
                        binding_set(&nb, nvar, &extra_nodes[ni]);

                        binding_t *tmp = malloc(2 * sizeof(binding_t));
                        tmp[0] = nb;
                        int tc = 1;
                        int tcap = 1;
                        const char *tv = nvar;
                        expand_pattern_rels(store, patn, &tmp, &tc, &tcap, &tv, opt);
                        for (int ti = 0; ti < tc; ti++) {
                            new_bindings[new_count++] = tmp[ti];
                        }
                        free(tmp);
                    }
                    if (opt && extra_count == 0) {
                        binding_t nb = {0};
                        binding_copy(&nb, &bindings[bi]);
                        new_bindings[new_count++] = nb;
                    }
                }
                for (int bi = 0; bi < bind_count; bi++) {
                    binding_free(&bindings[bi]);
                }
                free(bindings);
                bindings = new_bindings;
                bind_count = new_count;
            }
            cbm_store_free_nodes(extra_nodes, extra_count);
        }
    }

    /* Step 3: Apply late WHERE (re-evaluate after all bindings are fully expanded) */
    if (q->where && (pat0->rel_count > 0 || q->pattern_count > 1)) {
        int kept = 0;
        for (int i = 0; i < bind_count; i++) {
            if (eval_where(q->where, &bindings[i])) {
                if (kept != i) {
                    bindings[kept] = bindings[i];
                }
                kept++;
            } else {
                binding_free(&bindings[i]);
            }
        }
        bind_count = kept;
    }

    /* Step 3b: WITH clause */
    if (q->with_clause) {
        cbm_return_clause_t *wc = q->with_clause;
        /* Project through WITH into virtual bindings */
        binding_t *vbindings = malloc((bind_count + 1) * sizeof(binding_t));
        int vcount = 0;

        /* Check if WITH has aggregation */
        bool has_agg = false;
        for (int i = 0; i < wc->count; i++) {
            if (wc->items[i].func &&
                (strcmp(wc->items[i].func, "COUNT") == 0 || strcmp(wc->items[i].func, "SUM") == 0 ||
                 strcmp(wc->items[i].func, "AVG") == 0 || strcmp(wc->items[i].func, "MIN") == 0 ||
                 strcmp(wc->items[i].func, "MAX") == 0 ||
                 strcmp(wc->items[i].func, "COLLECT") == 0)) {
                has_agg = true;
                break;
            }
        }

        if (has_agg) {
            /* Group-by aggregation through WITH */
            typedef struct {
                char group_key[1024];
                const char **group_vals;
                double *sums;
                int *counts;
                double *mins, *maxs;
            } with_agg_t;
            int agg_cap = 256;
            with_agg_t *aggs = calloc(agg_cap, sizeof(with_agg_t));
            int agg_cnt = 0;

            for (int bi = 0; bi < bind_count; bi++) {
                char key[1024] = "";
                int kl = 0;
                for (int ci = 0; ci < wc->count; ci++) {
                    if (wc->items[ci].func) {
                        continue;
                    }
                    const char *v = binding_get_virtual(&bindings[bi], wc->items[ci].variable,
                                                        wc->items[ci].property);
                    kl += snprintf(key + kl, sizeof(key) - (size_t)kl, "%s|", v);
                    if (kl >= (int)sizeof(key)) {
                        kl = (int)sizeof(key) - 1;
                    }
                }
                int found = -1;
                for (int a = 0; a < agg_cnt; a++) {
                    if (strcmp(aggs[a].group_key, key) == 0) {
                        found = a;
                        break;
                    }
                }
                if (found < 0) {
                    if (agg_cnt >= agg_cap) {
                        agg_cap *= 2;
                        aggs = safe_realloc(aggs, agg_cap * sizeof(with_agg_t));
                    }
                    found = agg_cnt++;
                    snprintf(aggs[found].group_key, sizeof(aggs[found].group_key), "%s", key);
                    aggs[found].group_vals = malloc(wc->count * sizeof(const char *));
                    aggs[found].sums = calloc(wc->count, sizeof(double));
                    aggs[found].counts = calloc(wc->count, sizeof(int));
                    aggs[found].mins = malloc(wc->count * sizeof(double));
                    aggs[found].maxs = malloc(wc->count * sizeof(double));
                    for (int ci = 0; ci < wc->count; ci++) {
                        aggs[found].mins[ci] = 1e308;
                        aggs[found].maxs[ci] = -1e308;
                    }
                    for (int ci = 0; ci < wc->count; ci++) {
                        if (wc->items[ci].func) {
                            aggs[found].group_vals[ci] = heap_strdup("0");
                            continue;
                        }
                        const char *v = binding_get_virtual(&bindings[bi], wc->items[ci].variable,
                                                            wc->items[ci].property);
                        aggs[found].group_vals[ci] = heap_strdup(v);
                    }
                }
                for (int ci = 0; ci < wc->count; ci++) {
                    if (!wc->items[ci].func) {
                        continue;
                    }
                    aggs[found].counts[ci]++;
                    const char *raw = binding_get_virtual(&bindings[bi], wc->items[ci].variable,
                                                          wc->items[ci].property);
                    double dv = strtod(raw, NULL);
                    aggs[found].sums[ci] += dv;
                    if (dv < aggs[found].mins[ci]) {
                        aggs[found].mins[ci] = dv;
                    }
                    if (dv > aggs[found].maxs[ci]) {
                        aggs[found].maxs[ci] = dv;
                    }
                }
            }

            /* Build virtual bindings from aggregated groups */
            vbindings = safe_realloc(vbindings, (agg_cnt + 1) * sizeof(binding_t));
            for (int a = 0; a < agg_cnt; a++) {
                binding_t vb = {0};
                for (int ci = 0; ci < wc->count; ci++) {
                    const char *alias = wc->items[ci].alias;
                    char name_buf[256];
                    if (!alias) {
                        if (wc->items[ci].property) {
                            snprintf(name_buf, sizeof(name_buf), "%s.%s", wc->items[ci].variable,
                                     wc->items[ci].property);
                        } else {
                            snprintf(name_buf, sizeof(name_buf), "%s", wc->items[ci].variable);
                        }
                        alias = name_buf;
                    }
                    char vbuf[64];
                    if (wc->items[ci].func) {
                        const char *f = wc->items[ci].func;
                        if (strcmp(f, "COUNT") == 0) {
                            snprintf(vbuf, sizeof(vbuf), "%d", aggs[a].counts[ci]);
                        } else if (strcmp(f, "SUM") == 0) {
                            snprintf(vbuf, sizeof(vbuf), "%.10g", aggs[a].sums[ci]);
                        } else if (strcmp(f, "AVG") == 0) {
                            snprintf(vbuf, sizeof(vbuf), "%.10g",
                                     aggs[a].counts[ci] > 0 ? aggs[a].sums[ci] / aggs[a].counts[ci]
                                                            : 0);
                        } else if (strcmp(f, "MIN") == 0) {
                            snprintf(vbuf, sizeof(vbuf), "%.10g", aggs[a].mins[ci]);
                        } else if (strcmp(f, "MAX") == 0) {
                            snprintf(vbuf, sizeof(vbuf), "%.10g", aggs[a].maxs[ci]);
                        } else {
                            snprintf(vbuf, sizeof(vbuf), "%d", aggs[a].counts[ci]);
                        }
                        /* Store as a "virtual node" with the value in name,
                         * alias in qualified_name (freed by node_fields_free). */
                        cbm_node_t vn = {.name = heap_strdup(vbuf),
                                         .qualified_name = heap_strdup(alias)};
                        if (vb.var_count < 16) {
                            vb.var_names[vb.var_count] = vn.qualified_name;
                            vb.var_nodes[vb.var_count] = vn;
                            vb.var_count++;
                        }
                    } else {
                        cbm_node_t vn = {.name = heap_strdup(aggs[a].group_vals[ci]),
                                         .qualified_name = heap_strdup(alias)};
                        if (vb.var_count < 16) {
                            vb.var_names[vb.var_count] = vn.qualified_name;
                            vb.var_nodes[vb.var_count] = vn;
                            vb.var_count++;
                        }
                    }
                }
                vbindings[vcount++] = vb;
            }
            for (int a = 0; a < agg_cnt; a++) {
                for (int ci = 0; ci < wc->count; ci++) {
                    free((void *)aggs[a].group_vals[ci]);
                }
                // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                free(aggs[a].group_vals);
                free(aggs[a].sums);
                free(aggs[a].counts);
                free(aggs[a].mins);
                free(aggs[a].maxs);
            }
            free(aggs);
        } else {
            /* Non-aggregating WITH: simple projection rename */
            for (int bi = 0; bi < bind_count; bi++) {
                binding_t vb = {0};
                for (int ci = 0; ci < wc->count; ci++) {
                    const char *alias = wc->items[ci].alias;
                    char name_buf[256];
                    if (!alias) {
                        if (wc->items[ci].property) {
                            snprintf(name_buf, sizeof(name_buf), "%s.%s", wc->items[ci].variable,
                                     wc->items[ci].property);
                        } else {
                            snprintf(name_buf, sizeof(name_buf), "%s", wc->items[ci].variable);
                        }
                        alias = name_buf;
                    }
                    char func_buf[512];
                    const char *val =
                        project_item(&bindings[bi], &wc->items[ci], func_buf, sizeof(func_buf));
                    cbm_node_t vn = {.name = heap_strdup(val),
                                     .qualified_name = heap_strdup(alias)};
                    if (vb.var_count < 16) {
                        vb.var_names[vb.var_count] = vn.qualified_name;
                        vb.var_nodes[vb.var_count] = vn;
                        vb.var_count++;
                    }
                }
                vbindings[vcount++] = vb;
            }
        }

        /* Apply WITH ORDER BY + SKIP + LIMIT on virtual bindings */
        if (wc->order_by) {
            const char *ob = wc->order_by;
            bool wdesc = (wc->order_dir && strcmp(wc->order_dir, "DESC") == 0) != 0;
            for (int i2 = 0; i2 < vcount - 1; i2++) {
                for (int j2 = 0; j2 < vcount - i2 - 1; j2++) {
                    const char *va = binding_get_virtual(&vbindings[j2], ob, NULL);
                    const char *vb2 = binding_get_virtual(&vbindings[j2 + 1], ob, NULL);
                    int cmp2;
                    char *ea = NULL;
                    char *eb = NULL;
                    double da = strtod(va, &ea);
                    double db = strtod(vb2, &eb);
                    if (ea != va && eb != vb2) {
                        cmp2 = (da > db) - (da < db);
                    } else {
                        cmp2 = strcmp(va, vb2);
                    }
                    if ((int)wdesc ? cmp2 < 0 : cmp2 > 0) {
                        binding_t tmp2 = vbindings[j2];
                        vbindings[j2] = vbindings[j2 + 1];
                        vbindings[j2 + 1] = tmp2;
                    }
                }
            }
        }
        if (wc->skip > 0 && wc->skip < vcount) {
            for (int i2 = 0; i2 < wc->skip; i2++) {
                binding_free(&vbindings[i2]);
            }
            memmove(vbindings, vbindings + wc->skip, (vcount - wc->skip) * sizeof(binding_t));
            vcount -= wc->skip;
        } else if (wc->skip >= vcount) {
            for (int i2 = 0; i2 < vcount; i2++) {
                binding_free(&vbindings[i2]);
            }
            vcount = 0;
        }
        if (wc->limit > 0 && vcount > wc->limit) {
            for (int i2 = wc->limit; i2 < vcount; i2++) {
                binding_free(&vbindings[i2]);
            }
            vcount = wc->limit;
        }

        /* Replace bindings with virtual */
        for (int bi = 0; bi < bind_count; bi++) {
            binding_free(&bindings[bi]);
        }
        free(bindings);
        bindings = vbindings;
        bind_count = vcount;

        /* Apply post-WITH WHERE */
        if (q->post_with_where) {
            int kept = 0;
            for (int i = 0; i < bind_count; i++) {
                if (eval_where(q->post_with_where, &bindings[i])) {
                    if (kept != i) {
                        bindings[kept] = bindings[i];
                    }
                    kept++;
                } else {
                    binding_free(&bindings[i]);
                }
            }
            bind_count = kept;
        }
    }

    /* Step 4: Project results */
    rb_init(rb);
    cbm_return_clause_t *ret = q->ret;

    if (ret) {
        /* Check for aggregation */
        bool has_agg = false;
        for (int i = 0; i < ret->count; i++) {
            if (ret->items[i].func &&
                (strcmp(ret->items[i].func, "COUNT") == 0 ||
                 strcmp(ret->items[i].func, "SUM") == 0 || strcmp(ret->items[i].func, "AVG") == 0 ||
                 strcmp(ret->items[i].func, "MIN") == 0 || strcmp(ret->items[i].func, "MAX") == 0 ||
                 strcmp(ret->items[i].func, "COLLECT") == 0)) {
                has_agg = true;
                break;
            }
        }

        /* RETURN * */
        if (ret->star) {
            /* Collect all named variables from all patterns */
            const char *vars[32];
            int vc = 0;
            for (int pi = 0; pi < q->pattern_count; pi++) {
                for (int ni = 0; ni < q->patterns[pi].node_count && vc < 32; ni++) {
                    if (q->patterns[pi].nodes[ni].variable) {
                        vars[vc++] = q->patterns[pi].nodes[ni].variable;
                    }
                }
                for (int ri2 = 0; ri2 < q->patterns[pi].rel_count && vc < 32; ri2++) {
                    if (q->patterns[pi].rels[ri2].variable) {
                        vars[vc++] = q->patterns[pi].rels[ri2].variable;
                    }
                }
            }
            /* Build columns: var.name, var.qualified_name, var.label, var.file_path for nodes;
             * var.type for edges */
            int col_n = vc * 4;
            const char *col_names[128];
            for (int v = 0; v < vc; v++) {
                char buf[128];
                snprintf(buf, sizeof(buf), "%s.name", vars[v]);
                col_names[(size_t)v * 4] = heap_strdup(buf);
                snprintf(buf, sizeof(buf), "%s.qualified_name", vars[v]);
                col_names[((size_t)v * 4) + 1] = heap_strdup(buf);
                snprintf(buf, sizeof(buf), "%s.label", vars[v]);
                col_names[((size_t)v * 4) + 2] = heap_strdup(buf);
                snprintf(buf, sizeof(buf), "%s.file_path", vars[v]);
                col_names[((size_t)v * 4) + 3] = heap_strdup(buf);
            }
            rb_set_columns(rb, col_names, col_n);
            for (int i = 0; i < col_n; i++) {
                free((void *)col_names[i]);
            }

            for (int bi = 0; bi < bind_count && rb->row_count < max_rows; bi++) {
                const char *vals[128];
                for (int v = 0; v < vc; v++) {
                    cbm_edge_t *edge = binding_get_edge(&bindings[bi], vars[v]);
                    if (edge) {
                        vals[(size_t)v * 4] = edge_prop(edge, "type");
                        vals[((size_t)v * 4) + 1] = "";
                        vals[((size_t)v * 4) + 2] = "";
                        vals[((size_t)v * 4) + 3] = "";
                    } else {
                        cbm_node_t *n = binding_get(&bindings[bi], vars[v]);
                        vals[(size_t)v * 4] = n ? (n->name ? n->name : "") : "";
                        vals[((size_t)v * 4) + 1] =
                            n ? (n->qualified_name ? n->qualified_name : "") : "";
                        vals[((size_t)v * 4) + 2] = n ? (n->label ? n->label : "") : "";
                        vals[((size_t)v * 4) + 3] = n ? (n->file_path ? n->file_path : "") : "";
                    }
                }
                rb_add_row(rb, vals);
            }
            goto postprocess;
        }

        /* Build column names */
        const char *col_names[32];
        for (int i = 0; i < ret->count && i < 32; i++) {
            cbm_return_item_t *item = &ret->items[i];
            if (item->alias) {
                col_names[i] = item->alias;
            } else if (item->func) {
                char buf[128];
                snprintf(buf, sizeof(buf), "%s(%s)", item->func, item->variable);
                col_names[i] = heap_strdup(buf);
            } else if (item->kase) {
                col_names[i] = "CASE";
            } else if (item->property) {
                char buf[128];
                snprintf(buf, sizeof(buf), "%s.%s", item->variable, item->property);
                col_names[i] = heap_strdup(buf);
            } else {
                col_names[i] = item->variable;
            }
        }
        rb_set_columns(rb, col_names, ret->count);
        /* Free heap_strdup'd column names (rb_set_columns made its own copies).
         * Only func/property branches heap-allocate; alias takes priority. */
        for (int i = 0; i < ret->count && i < 32; i++) {
            cbm_return_item_t *item = &ret->items[i];
            if (!item->alias && (item->func || (!item->kase && item->property))) {
                free((void *)col_names[i]);
            }
        }

        if (has_agg) {
            /* Generalized aggregation: COUNT, SUM, AVG, MIN, MAX, COLLECT */
            typedef struct {
                char group_key[1024];
                const char **group_vals;
                double *sums;
                int *counts;
                double *mins, *maxs;
                char ***collect_lists;
                int *collect_counts;
            } agg_entry_t;

            int agg_cap = 256;
            agg_entry_t *aggs = calloc(agg_cap, sizeof(agg_entry_t));
            int agg_count = 0;

            for (int bi = 0; bi < bind_count; bi++) {
                char key[1024] = "";
                int klen = 0;
                const char *vals[32];
                for (int ci = 0; ci < ret->count; ci++) {
                    cbm_return_item_t *item = &ret->items[ci];
                    if (item->func) {
                        vals[ci] = "0";
                        continue;
                    }
                    char func_buf[512];
                    vals[ci] = project_item(&bindings[bi], item, func_buf, sizeof(func_buf));
                    klen += snprintf(key + klen, sizeof(key) - (size_t)klen, "%s|", vals[ci]);
                    if (klen >= (int)sizeof(key)) {
                        klen = (int)sizeof(key) - 1;
                    }
                }

                int found = -1;
                for (int a = 0; a < agg_count; a++) {
                    if (strcmp(aggs[a].group_key, key) == 0) {
                        found = a;
                        break;
                    }
                }
                if (found < 0) {
                    if (agg_count >= agg_cap) {
                        agg_cap *= 2;
                        aggs = safe_realloc(aggs, agg_cap * sizeof(agg_entry_t));
                    }
                    found = agg_count++;
                    snprintf(aggs[found].group_key, sizeof(aggs[found].group_key), "%s", key);
                    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                    aggs[found].group_vals = malloc(ret->count * sizeof(const char *));
                    aggs[found].sums = calloc(ret->count, sizeof(double));
                    aggs[found].counts = calloc(ret->count, sizeof(int));
                    aggs[found].mins = malloc(ret->count * sizeof(double));
                    aggs[found].maxs = malloc(ret->count * sizeof(double));
                    aggs[found].collect_lists = calloc(ret->count, sizeof(char **));
                    aggs[found].collect_counts = calloc(ret->count, sizeof(int));
                    for (int ci = 0; ci < ret->count; ci++) {
                        aggs[found].mins[ci] = 1e308;
                        aggs[found].maxs[ci] = -1e308;
                        aggs[found].group_vals[ci] = heap_strdup(vals[ci]);
                    }
                }
                for (int ci = 0; ci < ret->count; ci++) {
                    if (!ret->items[ci].func) {
                        continue;
                    }
                    aggs[found].counts[ci]++;
                    const char *raw = binding_get_virtual(&bindings[bi], ret->items[ci].variable,
                                                          ret->items[ci].property);
                    double dv = strtod(raw, NULL);
                    aggs[found].sums[ci] += dv;
                    if (dv < aggs[found].mins[ci]) {
                        aggs[found].mins[ci] = dv;
                    }
                    if (dv > aggs[found].maxs[ci]) {
                        aggs[found].maxs[ci] = dv;
                    }
                    if (strcmp(ret->items[ci].func, "COLLECT") == 0) {
                        int idx = aggs[found].collect_counts[ci]++;
                        aggs[found].collect_lists[ci] =
                            safe_realloc(aggs[found].collect_lists[ci], (idx + 1) * sizeof(char *));
                        aggs[found].collect_lists[ci][idx] = heap_strdup(raw);
                    }
                }
            }

            for (int a = 0; a < agg_count; a++) {
                const char *row[32];
                char bufs[32][64];
                for (int ci = 0; ci < ret->count; ci++) {
                    if (!ret->items[ci].func) {
                        row[ci] = aggs[a].group_vals[ci];
                        continue;
                    }
                    const char *f = ret->items[ci].func;
                    if (strcmp(f, "COUNT") == 0) {
                        {
                            snprintf(bufs[ci], sizeof(bufs[ci]), "%d", aggs[a].counts[ci]);
                        }
                    } else if (strcmp(f, "SUM") == 0) {
                        { snprintf(bufs[ci], sizeof(bufs[ci]), "%.10g", aggs[a].sums[ci]); }
                    } else if (strcmp(f, "AVG") == 0) {
                        {
                            snprintf(bufs[ci], sizeof(bufs[ci]), "%.10g",
                                     aggs[a].counts[ci] > 0 ? aggs[a].sums[ci] / aggs[a].counts[ci]
                                                            : 0.0);
                        }
                    } else if (strcmp(f, "MIN") == 0) {
                        { snprintf(bufs[ci], sizeof(bufs[ci]), "%.10g", aggs[a].mins[ci]); }
                    } else if (strcmp(f, "MAX") == 0) {
                        { snprintf(bufs[ci], sizeof(bufs[ci]), "%.10g", aggs[a].maxs[ci]); }
                    } else if (strcmp(f, "COLLECT") == 0) {
                        char cbuf[2048] = "[";
                        int bl = 1;
                        for (int ci2 = 0; ci2 < aggs[a].collect_counts[ci]; ci2++) {
                            if (ci2 > 0) {
                                cbuf[bl++] = ',';
                            }
                            bl += snprintf(cbuf + bl, sizeof(cbuf) - (size_t)bl, "\"%s\"",
                                           aggs[a].collect_lists[ci][ci2]);
                            if (bl >= (int)sizeof(cbuf)) {
                                bl = (int)sizeof(cbuf) - 1;
                            }
                        }
                        if (bl < (int)sizeof(cbuf) - 1) {
                            cbuf[bl++] = ']';
                        }
                        cbuf[bl] = '\0';
                        snprintf(bufs[ci], sizeof(bufs[ci]), "%s", cbuf);
                    } else {
                        snprintf(bufs[ci], sizeof(bufs[ci]), "%d", aggs[a].counts[ci]);
                    }
                    row[ci] = bufs[ci];
                }
                rb_add_row(rb, row);
            }

            for (int a = 0; a < agg_count; a++) {
                for (int ci = 0; ci < ret->count; ci++) {
                    free((void *)aggs[a].group_vals[ci]);
                    for (int j = 0; j < aggs[a].collect_counts[ci]; j++) {
                        free(aggs[a].collect_lists[ci][j]);
                    }
                    free(aggs[a].collect_lists[ci]);
                }
                // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                free(aggs[a].group_vals);
                free(aggs[a].sums);
                free(aggs[a].counts);
                free(aggs[a].mins);
                free(aggs[a].maxs);
                free(aggs[a].collect_lists);
                free(aggs[a].collect_counts);
            }
            free(aggs);
        } else {
            /* Simple projection — when ORDER BY or SKIP are present, project all rows
             * since we need the full set before sorting/skipping. Only cap early when
             * there's a simple LIMIT with no ORDER BY and no SKIP. */
            int proj_cap = max_rows;
            if (ret->limit > 0 && !ret->order_by && ret->skip <= 0) {
                proj_cap = ret->limit;
            }
            for (int bi = 0; bi < bind_count && rb->row_count < proj_cap; bi++) {
                const char *vals[32];
                char func_bufs[32][512];
                for (int ci = 0; ci < ret->count; ci++) {
                    vals[ci] = project_item(&bindings[bi], &ret->items[ci], func_bufs[ci],
                                            sizeof(func_bufs[ci]));
                }
                rb_add_row(rb, vals);
            }
        }

    postprocess:
        rb_apply_order_by(rb, ret);
        rb_apply_skip_limit(rb, ret->skip, ret->limit > 0 ? ret->limit : max_rows);
        if (ret->distinct) {
            rb_apply_distinct(rb);
        }

    } else {
        /* Default projection */
        const char *vars[16];
        int vc = 0;
        for (int ni = 0; ni < pat0->node_count && vc < 16; ni++) {
            if (pat0->nodes[ni].variable) {
                vars[vc++] = pat0->nodes[ni].variable;
            }
        }
        int col_n = vc * 3;
        const char *col_names[48];
        for (int v = 0; v < vc; v++) {
            char buf[128];
            snprintf(buf, sizeof(buf), "%s.name", vars[v]);
            col_names[(size_t)v * 3] = heap_strdup(buf);
            snprintf(buf, sizeof(buf), "%s.qualified_name", vars[v]);
            col_names[((size_t)v * 3) + 1] = heap_strdup(buf);
            snprintf(buf, sizeof(buf), "%s.label", vars[v]);
            col_names[((size_t)v * 3) + 2] = heap_strdup(buf);
        }
        rb_set_columns(rb, col_names, col_n);
        for (int i = 0; i < col_n; i++) {
            free((void *)col_names[i]);
        }

        for (int bi = 0; bi < bind_count && rb->row_count < max_rows; bi++) {
            const char *vals[48];
            for (int v = 0; v < vc; v++) {
                cbm_node_t *n = binding_get(&bindings[bi], vars[v]);
                vals[(size_t)v * 3] = n && n->name ? n->name : "";
                vals[((size_t)v * 3) + 1] = n && n->qualified_name ? n->qualified_name : "";
                vals[((size_t)v * 3) + 2] = n && n->label ? n->label : "";
            }
            rb_add_row(rb, vals);
        }
    }

    for (int bi = 0; bi < bind_count; bi++) {
        binding_free(&bindings[bi]);
    }
    free(bindings);
    cbm_store_free_nodes(scanned, scan_count);
    return 0;
}

/* ── Main entry point ─────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters,readability-function-cognitive-complexity,readability-function-size)
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
        return -1;
    }

    result_builder_t rb = {0};
    // cppcheck-suppress knownConditionTrueFalse
    if (execute_single(store, q, project, max_rows, &rb) < 0) {
        cbm_query_free(q);
        return -1;
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
            return -1;
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
        return -1;
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
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(r->columns);
    for (int i = 0; i < r->row_count; i++) {
        for (int j = 0; j < r->col_count; j++) {
            free((void *)r->rows[i][j]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(r->rows[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(r->rows);
    free(r->error);
    memset(r, 0, sizeof(*r));
}
