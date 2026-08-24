/*
 * cypher.c — Cypher query engine: lexer, parser, planner, executor.
 *
 * Translates a subset of Cypher into SQL queries against cbm_store.
 * Supports MATCH patterns with relationships, WHERE filters,
 * RETURN with COUNT/ORDER BY/LIMIT/DISTINCT.
 */
#include "cypher/cypher.h"
#include "foundation/compat.h"
#include "foundation/hash_table.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "store/store.h"

#include <yyjson/yyjson.h>

enum {
    CYP_BUF_16 = 16,
    CYP_BUF_48 = 48, /* ASCII '0' */
    CYP_BUF_8 = 8,
    CYP_BUF_4 = 4,
    CYP_MAX_TOKEN = 10, /* max token lookahead */
    CYP_PAIR = 2,
    CYP_TRIPLE = 3,
    CYP_INIT_CAP4 = 4,             /* initial small array capacity */
    CYP_JSON_CONTROL_LIMIT = 0x20, /* JSON escapes bytes below U+0020 */
    CYP_INIT_CAP8 = 8,             /* initial medium array capacity */
    /* Keep common bindings allocation-free. Wider queries spill into geometric
     * overflow storage instead of silently losing variables. */
    CYP_INLINE_NODE_VARS = 16,
    CYP_INLINE_EDGE_VARS = 8,
    CYP_GROWTH_10 = 10, /* binding growth factor */
    CYP_CHAR_IDX1 = 1,  /* second character index (e.g. op[1]) */
    CYP_EBUF_MASK = 7,
    CYP_NODE_COLS = 4, /* columns per node var: name, qn, label, file */
    CYP_EDGE_COLS = 3, /* columns per edge var: name, qn, label */
    CYP_FOUND_NONE = -1,
    /* search miss sentinel */ /* mask for ebuf ring buffer (8 entries) */
};
#define CYP_DBL_MAX 1e308

#include <ctype.h>
#include <errno.h>
#include "foundation/compat_regex.h"
#include <limits.h>
#include <math.h>
#include <stddef.h>
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CYPHER_ARRAY_COUNT(array) (sizeof(array) / sizeof((array)[0]))

/* Each lexer-keyword function has one source of truth for its stable schema
 * spelling, executor canonical spelling, and token. Compile-time expansion
 * keeps lexer lookup and token dispatch O(1) over the same fixed set, adds no
 * retained memory, and prevents the MCP contract from drifting from parser
 * recognition. Identifier-form functions use the schema arrays directly. */
#define CYPHER_AGGREGATE_FUNCTIONS(X) \
    X("count", "COUNT", TOK_COUNT)    \
    X("sum", "SUM", TOK_SUM)          \
    X("avg", "AVG", TOK_AVG)          \
    X("min", "MIN", TOK_MIN_KW)       \
    X("max", "MAX", TOK_MAX_KW)       \
    X("collect", "COLLECT", TOK_COLLECT)

#define CYPHER_KEYWORD_SCALAR_FUNCTIONS(X) \
    X("toLower", "toLower", TOK_TOLOWER)   \
    X("toUpper", "toUpper", TOK_TOUPPER)   \
    X("toString", "toString", TOK_TOSTRING)

/* One stable language registry for descriptions, conformance tests, and
 * identifier-form function recognition. These arrays describe executable
 * capability only; they never depend on an indexed repository or language.
 * Access is O(1), and description generation is O(C) for this fixed registry
 * with output-proportional memory owned by the caller's serializer. */
static const char *const CYPHER_CAPABILITY_CLAUSES[] = {
    "MATCH",    "OPTIONAL MATCH", "WHERE", "WITH",  "UNWIND", "RETURN",
    "DISTINCT", "ORDER BY",       "SKIP",  "LIMIT", "UNION",  "UNION ALL",
};
static const char *const CYPHER_CAPABILITY_PATTERNS[] = {
    "node labels, label alternation, and inline properties",
    "typed, type-alternative, untyped, directed, and undirected relationships",
    "exact, bounded, lower-only, upper-only, and unbounded variable-length relationships",
    "multiple MATCH/OPTIONAL MATCH stages",
};
static const char *const CYPHER_CAPABILITY_PREDICATES[] = {
    "comparisons", "boolean AND/OR/XOR/NOT", "IN",          "CONTAINS/STARTS WITH/ENDS WITH",
    "regex =~",    "IS NULL/IS NOT NULL",    "label tests", "single-hop EXISTS patterns",
};
#define CYPHER_CAPABILITY_NAME(schema_name, canonical_name, token) schema_name,
static const char *const CYPHER_CAPABILITY_AGGREGATES[] = {
    CYPHER_AGGREGATE_FUNCTIONS(CYPHER_CAPABILITY_NAME)};
static const char *const CYPHER_CAPABILITY_KEYWORD_SCALARS[] = {
    CYPHER_KEYWORD_SCALAR_FUNCTIONS(CYPHER_CAPABILITY_NAME)};
#undef CYPHER_CAPABILITY_NAME
static const char *const CYPHER_CAPABILITY_NAMED_SCALARS[] = {
    "labels",    "type", "id",     "keys", "properties", "toInteger", "toFloat",
    "toBoolean", "size", "length", "trim", "ltrim",      "rtrim",     "reverse",
};
static const char *const CYPHER_CAPABILITY_MULTI_ARGUMENT[] = {
    "coalesce", "substring", "replace", "left", "right",
};
static const char *const CYPHER_CAPABILITY_SEMANTICS[] = {
    "read-only execution",
    "relationship-unique trails for ordinary variable-length matching",
    "exact selection and ordering before output shaping",
    "unsupported syntax fails with a supported rewrite",
};
static const char *const CYPHER_CAPABILITY_UNSUPPORTED[] = {
    "write/admin/CALL clauses", "parameters",
    "path variables",           "variable-length relationship bindings",
    "list/map comprehensions",
};

static const cbm_cypher_capability_schema_t CYPHER_CAPABILITY_SCHEMA = {
    .schema_id = CBM_CYPHER_CAPABILITY_SCHEMA_ID,
    .version = CBM_CYPHER_CAPABILITY_SCHEMA_VERSION,
    .clauses = CYPHER_CAPABILITY_CLAUSES,
    .clause_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_CLAUSES),
    .patterns = CYPHER_CAPABILITY_PATTERNS,
    .pattern_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_PATTERNS),
    .predicates = CYPHER_CAPABILITY_PREDICATES,
    .predicate_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_PREDICATES),
    .aggregate_functions = CYPHER_CAPABILITY_AGGREGATES,
    .aggregate_function_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_AGGREGATES),
    .keyword_scalar_functions = CYPHER_CAPABILITY_KEYWORD_SCALARS,
    .keyword_scalar_function_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_KEYWORD_SCALARS),
    .named_scalar_functions = CYPHER_CAPABILITY_NAMED_SCALARS,
    .named_scalar_function_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_NAMED_SCALARS),
    .multi_argument_functions = CYPHER_CAPABILITY_MULTI_ARGUMENT,
    .multi_argument_function_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_MULTI_ARGUMENT),
    .semantics = CYPHER_CAPABILITY_SEMANTICS,
    .semantic_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_SEMANTICS),
    .unsupported = CYPHER_CAPABILITY_UNSUPPORTED,
    .unsupported_count = CYPHER_ARRAY_COUNT(CYPHER_CAPABILITY_UNSUPPORTED),
};

const cbm_cypher_capability_schema_t *cbm_cypher_capability_schema(void) {
    return &CYPHER_CAPABILITY_SCHEMA;
}

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

/* Join a small, caller-owned set of string parts without format buffers.
 * Sizing and copying are each O(L) for total output length L, with one exact
 * O(L) allocation. NULL parts and representational overflow fail atomically. */
static char *cypher_join_parts(const char *const *parts, size_t part_count) {
    if (!parts || part_count == 0) {
        return NULL;
    }
    size_t total = 0;
    for (size_t i = 0; i < part_count; i++) {
        if (!parts[i]) {
            return NULL;
        }
        size_t length = strlen(parts[i]);
        if (length > SIZE_MAX - total) {
            return NULL;
        }
        total += length;
    }
    if (total > SIZE_MAX - SKIP_ONE) {
        return NULL;
    }
    char *joined = malloc(total + SKIP_ONE);
    if (!joined) {
        return NULL;
    }
    size_t offset = 0;
    for (size_t i = 0; i < part_count; i++) {
        size_t length = strlen(parts[i]);
        memcpy(joined + offset, parts[i], length);
        offset += length;
    }
    joined[offset] = '\0';
    return joined;
}

/* Compare an existing expression with the same canonical part composition
 * used by the parser/column builder, avoiding temporary allocation. O(L)
 * runtime and O(1) auxiliary memory for total expression length L. */
static bool cypher_text_equals_parts(const char *text, const char *const *parts,
                                     size_t part_count) {
    if (!text || !parts || part_count == 0) {
        return false;
    }
    const char *cursor = text;
    for (size_t i = 0; i < part_count; i++) {
        if (!parts[i]) {
            return false;
        }
        size_t length = strlen(parts[i]);
        if (strncmp(cursor, parts[i], length) != 0) {
            return false;
        }
        cursor += length;
    }
    return *cursor == '\0';
}

static int cypher_geometric_capacity(int current, int needed);

/* ══════════════════════════════════════════════════════════════════
 *  LEXER
 * ══════════════════════════════════════════════════════════════════ */

enum {
    CYP_LEX_ALLOC_NONE = 0,
    CYP_LEX_ALLOC_TOKEN_ARRAY,
    CYP_LEX_ALLOC_TOKEN_TEXT,
    CYP_LEX_ALLOC_STRING_TEXT,
};

#ifdef CBM_ENABLE_TEST_SEAMS
_Static_assert((int)CBM_CYPHER_TEST_LEX_ALLOC_NONE == CYP_LEX_ALLOC_NONE,
               "lexer allocation seam values must match");
_Static_assert((int)CBM_CYPHER_TEST_LEX_ALLOC_TOKEN_ARRAY == CYP_LEX_ALLOC_TOKEN_ARRAY,
               "lexer allocation seam values must match");
_Static_assert((int)CBM_CYPHER_TEST_LEX_ALLOC_TOKEN_TEXT == CYP_LEX_ALLOC_TOKEN_TEXT,
               "lexer allocation seam values must match");
_Static_assert((int)CBM_CYPHER_TEST_LEX_ALLOC_STRING_TEXT == CYP_LEX_ALLOC_STRING_TEXT,
               "lexer allocation seam values must match");
static CBM_TLS int g_cypher_test_lex_alloc_site = CYP_LEX_ALLOC_NONE;
static CBM_TLS int g_cypher_test_lex_alloc_successes_before_failure = -1;

void cbm_cypher_test_fail_lex_allocation(cbm_cypher_test_lex_alloc_site_t site,
                                         int successful_before) {
    g_cypher_test_lex_alloc_site = (int)site;
    g_cypher_test_lex_alloc_successes_before_failure = successful_before;
}
#endif

static bool lex_allocation_should_fail(int site) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (g_cypher_test_lex_alloc_site != site ||
        g_cypher_test_lex_alloc_successes_before_failure < 0) {
        return false;
    }
    if (g_cypher_test_lex_alloc_successes_before_failure == 0) {
        g_cypher_test_lex_alloc_site = CYP_LEX_ALLOC_NONE;
        g_cypher_test_lex_alloc_successes_before_failure = -1;
        return true;
    }
    g_cypher_test_lex_alloc_successes_before_failure--;
#else
    (void)site;
#endif
    return false;
}

static void *lex_malloc(int site, size_t bytes) {
    return lex_allocation_should_fail(site) ? NULL : malloc(bytes);
}

static void *lex_realloc(int site, void *allocation, size_t bytes) {
    return lex_allocation_should_fail(site) ? NULL : realloc(allocation, bytes);
}

static void lex_set_error(cbm_lex_result_t *r, const char *message) {
    if (r) {
        r->failed = true;
    }
    if (r && !r->error) {
        r->error = heap_strdup(message);
    }
}

/* Geometric token-vector growth performs O(T) total pointer-copy work and
 * retains O(T) token metadata for T tokens. Overflow/OOM leaves the old owner
 * intact and aborts the entire lex rather than publishing a partial token. */
static bool lex_reserve_token(cbm_lex_result_t *r) {
    if (!r || r->failed || r->count < 0 || r->capacity < 0 || r->count > INT_MAX - SKIP_ONE) {
        lex_set_error(r, "Cypher token count is too large to represent");
        return false;
    }
    if (r->count < r->capacity) {
        return true;
    }
    int needed = r->count + SKIP_ONE;
    int next_capacity =
        r->capacity > 0 ? cypher_geometric_capacity(r->capacity, needed) : CBM_SZ_32;
    if (next_capacity < needed || (size_t)next_capacity > SIZE_MAX / sizeof(*r->tokens)) {
        lex_set_error(r, "Cypher token storage is too large to represent");
        return false;
    }
    cbm_token_t *grown = lex_realloc(CYP_LEX_ALLOC_TOKEN_ARRAY, r->tokens,
                                     (size_t)next_capacity * sizeof(*r->tokens));
    if (!grown) {
        lex_set_error(r, "out of memory growing Cypher token storage");
        return false;
    }
    r->tokens = grown;
    r->capacity = next_capacity;
    return true;
}

static void lex_push_owned(cbm_lex_result_t *r, cbm_token_type_t type, char *text, int pos) {
    if (!text) {
        lex_set_error(r, "out of memory copying Cypher token text");
        return;
    }
    if (!lex_reserve_token(r)) {
        free(text);
        return;
    }
    r->tokens[r->count++] = (cbm_token_t){.type = type, .text = text, .pos = pos};
}

static void lex_push_n(cbm_lex_result_t *r, cbm_token_type_t type, const char *start, size_t len,
                       int pos) {
    if (!r || r->failed) {
        return;
    }
    if (len > SIZE_MAX - SKIP_ONE) {
        lex_set_error(r, "Cypher token text is too large to represent");
        return;
    }
    char *text = lex_malloc(CYP_LEX_ALLOC_TOKEN_TEXT, len + SKIP_ONE);
    if (text) {
        memcpy(text, start, len);
        text[len] = '\0';
    }
    lex_push_owned(r, type, text, pos);
}

static void lex_push(cbm_lex_result_t *r, cbm_token_type_t type, const char *text, int pos) {
    lex_push_n(r, type, text, strlen(text), pos);
}

/* Parse a string literal with escape handling. A validation/count pass finds
 * the unescaped closing quote before allocation; one exact allocation and one
 * decode pass then produce the owned token. Runtime is O(L), retained and peak
 * literal memory are exactly O(decoded L), and malformed input allocates no
 * speculative remaining-query buffer. */
static void lex_string_literal(const char *input, int len, int *pos, char quote,
                               cbm_lex_result_t *out) {
    int start = *pos;
    int cursor = start;
    size_t decoded_length = 0;
    while (cursor < len && input[cursor] != quote) {
        if (input[cursor] == '\\' && cursor + SKIP_ONE < len) {
            cursor += PAIR_LEN;
        } else {
            cursor++;
        }
        decoded_length++;
    }
    if (cursor >= len) {
        lex_set_error(out, "unterminated string literal in Cypher query");
        return;
    }
    char *text = lex_malloc(CYP_LEX_ALLOC_STRING_TEXT, decoded_length + SKIP_ONE);
    if (!text) {
        lex_set_error(out, "out of memory decoding Cypher string literal");
        return;
    }
    size_t decoded_index = 0;
    while (*pos < cursor) {
        if (input[*pos] == '\\' && *pos + SKIP_ONE < len) {
            (*pos)++;
            switch (input[*pos]) {
            case 'n':
                text[decoded_index++] = '\n';
                break;
            case 't':
                text[decoded_index++] = '\t';
                break;
            case '\\':
                text[decoded_index++] = '\\';
                break;
            default:
                text[decoded_index++] = input[*pos];
                break;
            }
        } else {
            text[decoded_index++] = input[*pos];
        }
        (*pos)++;
    }
    (*pos)++; /* skip closing quote */
    text[decoded_index] = '\0';
    lex_push_owned(out, TOK_STRING, text, start - SKIP_ONE);
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
#define CYPHER_KEYWORD_ENTRY(schema_name, canonical_name, token) {canonical_name, token},
    CYPHER_AGGREGATE_FUNCTIONS(CYPHER_KEYWORD_ENTRY)
    /* Phase 5: string functions + CASE */
    CYPHER_KEYWORD_SCALAR_FUNCTIONS(CYPHER_KEYWORD_ENTRY)
#undef CYPHER_KEYWORD_ENTRY
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

static cbm_token_type_t keyword_lookup_n(const char *word, size_t word_length) {
    /* Length-aware lookup avoids copying or truncating identifiers merely to
     * compare against this finite registry. */
    for (const kw_entry_t *kw = keywords; kw->name; kw++) {
        size_t keyword_length = strlen(kw->name);
        if (word_length == keyword_length && strncasecmp(word, kw->name, word_length) == 0) {
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
    size_t word_length = (size_t)(*i - start);
    cbm_token_type_t type = keyword_lookup_n(input + start, word_length);
    lex_push_n(out, type, input + start, word_length, start);
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
static bool lex_skip_whitespace_comments(const char *input, int len, int *i,
                                         cbm_lex_result_t *out) {
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
        int comment_start = *i;
        *i += PAIR_LEN;
        while (*i + SKIP_ONE < len && !(input[*i] == '*' && input[*i + SKIP_ONE] == '/')) {
            (*i)++;
        }
        if (*i + SKIP_ONE < len) {
            *i += PAIR_LEN;
        } else {
            char message[CBM_SZ_128];
            snprintf(message, sizeof(message), "unterminated block comment at byte %d",
                     comment_start);
            lex_set_error(out, message);
        }
        return true;
    }
    return false;
}

int cbm_lex(const char *input, cbm_lex_result_t *out) {
    if (!out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));
    if (!input) {
        lex_set_error(out, "Cypher query is NULL");
        return CBM_NOT_FOUND;
    }

    size_t input_length = strlen(input);
    if (input_length > (size_t)INT_MAX) {
        lex_set_error(out, "Cypher query length is too large to represent");
        return CBM_NOT_FOUND;
    }
    int len = (int)input_length;
    int i = 0;

    while (i < len && !out->failed) {
        if (lex_skip_whitespace_comments(input, len, &i, out)) {
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

        char message[CBM_SZ_128];
        unsigned char unknown = (unsigned char)c;
        if (isprint(unknown)) {
            snprintf(message, sizeof(message), "unsupported character '%c' at byte %d", c, i);
        } else {
            snprintf(message, sizeof(message), "unsupported byte 0x%02X at byte %d", unknown, i);
        }
        lex_set_error(out, message);
    }

    if (out->failed) {
        return CBM_NOT_FOUND;
    }
    /* Add EOF */
    lex_push(out, TOK_EOF, "", i);
    return out->failed ? CBM_NOT_FOUND : 0;
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

/* The WHERE grammar descends once per nested '(' and once per NOT, so parse
 * depth follows the query text rather than anything bounded. A few tens of KB of
 * either exhausts the stack before any semantic limit applies. 256 is far past
 * any hand-written or generated query while leaving the stack untouched. */
enum { CYPHER_MAX_PARSE_DEPTH = 256 };

typedef struct {
    const cbm_token_t *tokens;
    int count;
    int pos;
    int depth; /* current recursive-descent depth; see CYPHER_MAX_PARSE_DEPTH */
    char error[CBM_SZ_512];
} parser_t;

static void parser_append_function_names(char *out, size_t out_size, int *offset,
                                         const char *const *names, size_t name_count, bool *first) {
    for (size_t i = 0; i < name_count; i++) {
        CBM_SNPRINTF_APPEND(out, out_size, *offset, "%s%s", *first ? "" : ", ", names[i]);
        *first = false;
    }
}

/* Build actionable parser diagnostics from the same immutable registry used
 * by recognition and MCP descriptions. C is a fixed capability count, so the
 * error path costs O(C) time and O(1) stack memory with bounded writes and no
 * allocation that could obscure the original parse failure. WHERE currently
 * accepts only the multi-argument scalar subset on its left-hand side. */
static void parser_set_unsupported_function_error(parser_t *p, const char *name,
                                                  bool where_context) {
    const cbm_cypher_capability_schema_t *schema = cbm_cypher_capability_schema();
    int offset = 0;
    bool first = true;
    CBM_SNPRINTF_APPEND(p->error, sizeof(p->error), offset,
                        "unsupported function '%s'%s "
                        "(supported: ",
                        name ? name : "?", where_context ? " in WHERE" : "");
    if (!where_context) {
        parser_append_function_names(p->error, sizeof(p->error), &offset,
                                     schema->aggregate_functions, schema->aggregate_function_count,
                                     &first);
        parser_append_function_names(p->error, sizeof(p->error), &offset,
                                     schema->keyword_scalar_functions,
                                     schema->keyword_scalar_function_count, &first);
        parser_append_function_names(p->error, sizeof(p->error), &offset,
                                     schema->named_scalar_functions,
                                     schema->named_scalar_function_count, &first);
    }
    parser_append_function_names(p->error, sizeof(p->error), &offset,
                                 schema->multi_argument_functions,
                                 schema->multi_argument_function_count, &first);
    CBM_SNPRINTF_APPEND(p->error, sizeof(p->error), offset, ")");
}

/* Enter one level of recursive descent. Returns false when the cap is hit, in
 * which case the caller must return NULL without recursing. */
static bool parse_depth_enter(parser_t *p) {
    if (p->depth >= CYPHER_MAX_PARSE_DEPTH) {
        snprintf(p->error, sizeof(p->error), "expression nested deeper than %d levels",
                 CYPHER_MAX_PARSE_DEPTH);
        return false;
    }
    p->depth++;
    return true;
}

static void parse_depth_leave(parser_t *p) {
    p->depth--;
}

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
        if (!out->variable) {
            snprintf(p->error, sizeof(p->error), "could not allocate node variable");
            return CBM_NOT_FOUND;
        }
    }

    /* Optional :Label, with openCypher label alternation :A|B|C (#242).
     * Stored as a single "A|B|C" string; the matcher splits on '|'. */
    if (match(p, TOK_COLON)) {
        const cbm_token_t *label = expect(p, TOK_IDENT);
        if (!label) {
            return CBM_NOT_FOUND;
        }
        int first_label_token = p->pos - SKIP_ONE;
        size_t label_length = strlen(label->text);
        while (match(p, TOK_PIPE)) {
            const cbm_token_t *alt = expect(p, TOK_IDENT);
            if (!alt) {
                return CBM_NOT_FOUND;
            }
            size_t alternate_length = strlen(alt->text);
            if (label_length > SIZE_MAX - SKIP_ONE ||
                alternate_length > SIZE_MAX - (label_length + SKIP_ONE)) {
                snprintf(p->error, sizeof(p->error), "node label alternation is too large");
                return CBM_NOT_FOUND;
            }
            label_length += SKIP_ONE + alternate_length;
        }
        if (label_length > SIZE_MAX - SKIP_ONE) {
            snprintf(p->error, sizeof(p->error), "node label alternation is too large");
            return CBM_NOT_FOUND;
        }
        char *joined_label = malloc(label_length + SKIP_ONE);
        if (!joined_label) {
            snprintf(p->error, sizeof(p->error), "could not allocate node label alternation");
            return CBM_NOT_FOUND;
        }
        size_t offset = 0;
        for (int token_index = first_label_token; token_index < p->pos; token_index++) {
            const cbm_token_t *part = &p->tokens[token_index];
            size_t part_length = strlen(part->text);
            memcpy(joined_label + offset, part->text, part_length);
            offset += part_length;
        }
        joined_label[offset] = '\0';
        out->label = joined_label;
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

static int parse_hop_bound(parser_t *p, int *out) {
    const cbm_token_t *token = advance(p);
    char *end = NULL;
    errno = 0;
    unsigned long long value = strtoull(token->text, &end, CBM_DECIMAL_BASE);
    if (errno == ERANGE || !end || *end != '\0' || value > (unsigned long long)INT_MAX) {
        snprintf(p->error, sizeof(p->error), "invalid hop range bound '%s' at pos %d", token->text,
                 token->pos);
        return CBM_NOT_FOUND;
    }
    *out = (int)value;
    return 0;
}

/* Parse *min..max hop range after the star token has been consumed. */
static int parse_hop_range(parser_t *p, int *min_hops, int *max_hops) {
    if (check(p, TOK_NUMBER)) {
        int val = 0;
        if (parse_hop_bound(p, &val) != 0) {
            return CBM_NOT_FOUND;
        }
        if (match(p, TOK_DOTDOT)) {
            *min_hops = val;
            if (check(p, TOK_NUMBER)) {
                if (parse_hop_bound(p, max_hops) != 0) {
                    return CBM_NOT_FOUND;
                }
            } else {
                *max_hops = CBM_CYPHER_HOPS_UNBOUNDED;
            }
        } else {
            /* Cypher's single-bound form is exact: *N is equivalent to *N..N. */
            *min_hops = val;
            *max_hops = val;
        }
    } else if (match(p, TOK_DOTDOT)) {
        *min_hops = SKIP_ONE;
        if (check(p, TOK_NUMBER)) {
            if (parse_hop_bound(p, max_hops) != 0) {
                return CBM_NOT_FOUND;
            }
        } else {
            *max_hops = CBM_CYPHER_HOPS_UNBOUNDED;
        }
    } else {
        /* * alone = unbounded */
        *min_hops = SKIP_ONE;
        *max_hops = CBM_CYPHER_HOPS_UNBOUNDED;
    }
    return 0;
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
        if (!out->variable) {
            snprintf(p->error, sizeof(p->error), "out of memory parsing relationship variable");
            return CBM_NOT_FOUND;
        }
    }
    /* Optional :Types */
    if (match(p, TOK_COLON)) {
        if (parse_rel_types(p, out) < 0) {
            return CBM_NOT_FOUND;
        }
    }
    /* Optional *hop_range */
    if (match(p, TOK_STAR)) {
        if (out->variable) {
            snprintf(p->error, sizeof(p->error),
                     "unsupported Cypher feature: variable-length relationship variables; "
                     "omit the relationship variable or use a fixed-length relationship");
            return CBM_NOT_FOUND;
        }
        if (parse_hop_range(p, &out->min_hops, &out->max_hops) != 0) {
            return CBM_NOT_FOUND;
        }
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
    cbm_node_pattern_t far_node = {0};
    if (parse_node(p, &anchor) < 0 || parse_rel(p, &rel) < 0 || parse_node(p, &far_node) < 0) {
        free_one_node_pattern(&anchor);
        free_one_rel_pattern(&rel);
        free_one_node_pattern(&far_node);
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
    free_one_node_pattern(&far_node);
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
        parser_set_unsupported_function_error(p, peek(p)->text, true);
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
        if (!parse_depth_enter(p)) {
            return NULL;
        }
        cbm_expr_t *e = parse_or_expr(p);
        parse_depth_leave(p);
        expect(p, TOK_RPAREN);
        return e;
    }
    return parse_condition_expr(p);
}

/* NOT: NOT atom | atom */
static cbm_expr_t *parse_not_expr(parser_t *p) { // NOLINT(misc-no-recursion)
    if (match(p, TOK_NOT)) {
        if (!parse_depth_enter(p)) {
            return NULL;
        }
        cbm_expr_t *child = parse_not_expr(p);
        parse_depth_leave(p);
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
    switch (t) {
#define CYPHER_TOKEN_CASE(schema_name, canonical_name, token) case token:
        CYPHER_AGGREGATE_FUNCTIONS(CYPHER_TOKEN_CASE)
#undef CYPHER_TOKEN_CASE
        return true;
    default:
        return false;
    }
}

/* Helper: is token a string function? */
static bool is_string_func_tok(cbm_token_type_t t) {
    switch (t) {
#define CYPHER_TOKEN_CASE(schema_name, canonical_name, token) case token:
        CYPHER_KEYWORD_SCALAR_FUNCTIONS(CYPHER_TOKEN_CASE)
#undef CYPHER_TOKEN_CASE
        return true;
    default:
        return false;
    }
}

/* Token type to function name */
static const char *agg_func_name(cbm_token_type_t t) {
    switch (t) {
#define CYPHER_TOKEN_NAME(schema_name, canonical_name, token) \
    case token:                                               \
        return canonical_name;
        CYPHER_AGGREGATE_FUNCTIONS(CYPHER_TOKEN_NAME)
#undef CYPHER_TOKEN_NAME
    default:
        return NULL;
    }
}

static const char *str_func_name(cbm_token_type_t t) {
    switch (t) {
#define CYPHER_TOKEN_NAME(schema_name, canonical_name, token) \
    case token:                                               \
        return canonical_name;
        CYPHER_KEYWORD_SCALAR_FUNCTIONS(CYPHER_TOKEN_NAME)
#undef CYPHER_TOKEN_NAME
    default:
        return NULL;
    }
}

/* Parse a value literal: string, number, ident[.prop], true, false. Returns heap-allocated. */
static const char *parse_value_literal(parser_t *p) {
    if (check(p, TOK_STRING) || check(p, TOK_NUMBER)) {
        return heap_strdup(advance(p)->text);
    }
    if (check(p, TOK_IDENT)) {
        const cbm_token_t *v = advance(p);
        if (match(p, TOK_DOT)) {
            const cbm_token_t *pr = expect(p, TOK_IDENT);
            if (!pr) {
                return NULL;
            }
            const char *parts[] = {v->text, ".", pr->text};
            const char *joined = cypher_join_parts(parts, sizeof(parts) / sizeof(parts[0]));
            if (!joined) {
                snprintf(p->error, sizeof(p->error), "could not allocate CASE value reference");
            }
            return joined;
        }
        const char *value = heap_strdup(v->text);
        if (!value) {
            snprintf(p->error, sizeof(p->error), "could not allocate CASE value");
        }
        return value;
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
    const cbm_cypher_capability_schema_t *schema = cbm_cypher_capability_schema();
    for (size_t i = 0; i < schema->named_scalar_function_count; i++) {
        if (cyp_ci_eq(s, schema->named_scalar_functions[i])) {
            return schema->named_scalar_functions[i];
        }
    }
    return NULL;
}

/* String transforms whose exact output can be derived directly from a
 * cypher_value_t without a bounded compatibility buffer. */
static bool is_exact_string_value_func(const char *function) {
    return function && (strcmp(function, "toLower") == 0 || strcmp(function, "toUpper") == 0 ||
                        strcmp(function, "toString") == 0 || strcmp(function, "size") == 0 ||
                        strcmp(function, "length") == 0 || strcmp(function, "trim") == 0 ||
                        strcmp(function, "ltrim") == 0 || strcmp(function, "rtrim") == 0 ||
                        strcmp(function, "reverse") == 0);
}

static bool is_numeric_bool_value_func(const char *function) {
    return function && (strcmp(function, "toInteger") == 0 || strcmp(function, "toFloat") == 0 ||
                        strcmp(function, "toBoolean") == 0);
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
    const cbm_cypher_capability_schema_t *schema = cbm_cypher_capability_schema();
    for (size_t i = 0; i < schema->multi_argument_function_count; i++) {
        if (cyp_ci_eq(s, schema->multi_argument_functions[i])) {
            return schema->multi_argument_functions[i];
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
    /* Optional DISTINCT inside a supported aggregate call. */
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
            parser_set_unsupported_function_error(p, item->variable, false);
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

static void free_return_item(cbm_return_item_t *item);
static void free_order_item(cbm_order_item_t *item);
static void free_return_clause(cbm_return_clause_t *r);

/* Parse one ORDER BY expression into exact owned storage. */
static char *parse_order_by_agg(parser_t *p) {
    const char *fn = agg_func_name(peek(p)->type);
    advance(p);
    if (!expect(p, TOK_LPAREN)) {
        return NULL;
    }
    const char *argument = NULL;
    if (match(p, TOK_STAR)) {
        argument = "*";
    } else {
        const cbm_token_t *var = expect(p, TOK_IDENT);
        if (!var) {
            return NULL;
        }
        argument = var->text;
    }
    if (!expect(p, TOK_RPAREN)) {
        return NULL;
    }
    const char *parts[] = {fn, "(", argument, ")"};
    return cypher_join_parts(parts, sizeof(parts) / sizeof(parts[0]));
}

/* Parse var[.prop] for ORDER BY */
static char *parse_order_by_var(parser_t *p) {
    const cbm_token_t *var = expect(p, TOK_IDENT);
    if (!var) {
        return NULL;
    }
    if (match(p, TOK_DOT)) {
        const cbm_token_t *prop = expect(p, TOK_IDENT);
        if (!prop) {
            return NULL;
        }
        const char *parts[] = {var->text, ".", prop->text};
        return cypher_join_parts(parts, sizeof(parts) / sizeof(parts[0]));
    }
    return heap_strdup(var->text);
}

static char *parse_order_by_expr(parser_t *p) {
    if (is_aggregate_tok(peek(p)->type)) {
        return parse_order_by_agg(p);
    }
    return parse_order_by_var(p);
}

static int parse_order_by_clause(parser_t *p, cbm_return_clause_t *r) {
    if (!expect(p, TOK_BY)) {
        return CBM_NOT_FOUND;
    }
    /* Geometric metadata growth is O(K) total pointer-copy work and O(K)
     * retained memory for K sort keys; each item publishes atomically. */
    int capacity = 0;
    do {
        if (r->order_count > 0 && !match(p, TOK_COMMA)) {
            break;
        }
        char *order_expression = parse_order_by_expr(p);
        if (!order_expression) {
            if (!p->error[0]) {
                snprintf(p->error, sizeof(p->error),
                         "could not allocate or parse ORDER BY expression");
            }
            return CBM_NOT_FOUND;
        }
        cbm_order_item_t item = {.expression = order_expression, .direction = NULL};
        bool has_direction = false;
        if (match(p, TOK_ASC)) {
            has_direction = true;
            item.direction = heap_strdup("ASC");
        } else if (match(p, TOK_DESC)) {
            has_direction = true;
            item.direction = heap_strdup("DESC");
        }
        if (has_direction && !item.direction) {
            snprintf(p->error, sizeof(p->error), "could not allocate ORDER BY expression");
            free_order_item(&item);
            return CBM_NOT_FOUND;
        }
        if (r->order_count >= capacity) {
            int next_capacity = cypher_geometric_capacity(capacity, r->order_count + SKIP_ONE);
            if ((size_t)next_capacity > SIZE_MAX / sizeof(*r->order_items)) {
                snprintf(p->error, sizeof(p->error), "ORDER BY is too wide to represent");
                free_order_item(&item);
                return CBM_NOT_FOUND;
            }
            cbm_order_item_t *grown =
                realloc(r->order_items, (size_t)next_capacity * sizeof(*grown));
            if (!grown) {
                snprintf(p->error, sizeof(p->error), "could not grow ORDER BY expressions");
                free_order_item(&item);
                return CBM_NOT_FOUND;
            }
            r->order_items = grown;
            capacity = next_capacity;
        }
        r->order_items[r->order_count++] = item;
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
        if (item->func) {
            const char *parts[] = {item->func, "(", item->variable ? item->variable : "", ")"};
            if (cypher_text_equals_parts(expression, parts, sizeof(parts) / sizeof(parts[0]))) {
                return true;
            }
        } else if (item->kase) {
            if (strcmp(expression, "CASE") == 0) {
                return true;
            }
        } else if (item->property) {
            const char *parts[] = {item->variable, ".", item->property};
            if (cypher_text_equals_parts(expression, parts, sizeof(parts) / sizeof(parts[0]))) {
                return true;
            }
        } else if (item->variable) {
            if (strcmp(item->variable, expression) == 0) {
                return true;
            }
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
    if (!r) {
        snprintf(p->error, sizeof(p->error), "could not allocate RETURN/WITH clause");
        return CBM_NOT_FOUND;
    }
    /* -1 = no LIMIT clause (return all). An explicit `LIMIT 0` parses to 0 below
     * and must return 0 rows — distinguishing the two requires a sentinel, since
     * calloc zeroes limit and `limit > 0` would treat LIMIT 0 as "no limit". */
    r->limit = -1;
    int cap = CYP_INIT_CAP8;
    r->items = malloc((size_t)cap * sizeof(cbm_return_item_t));
    if (!r->items) {
        snprintf(p->error, sizeof(p->error), "could not allocate RETURN/WITH projection items");
        free(r);
        return CBM_NOT_FOUND;
    }

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
            free_return_item(&item);
            free_return_clause(r);
            return CBM_NOT_FOUND;
        }

        if (r->count >= cap) {
            if (cap > INT_MAX / PAIR_LEN ||
                (size_t)(cap * PAIR_LEN) > SIZE_MAX / sizeof(cbm_return_item_t)) {
                snprintf(p->error, sizeof(p->error),
                         "RETURN/WITH projection is too wide to represent");
                free_return_item(&item);
                free_return_clause(r);
                return CBM_NOT_FOUND;
            }
            int next_cap = cap * PAIR_LEN;
            cbm_return_item_t *grown = realloc(r->items, (size_t)next_cap * sizeof(*grown));
            if (!grown) {
                snprintf(p->error, sizeof(p->error), "could not grow RETURN/WITH projection items");
                free_return_item(&item);
                free_return_clause(r);
                return CBM_NOT_FOUND;
            }
            r->items = grown;
            cap = next_cap;
        }
        r->items[r->count++] = item;

    } while (check(p, TOK_COMMA));

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

static void free_pattern(cbm_pattern_t *pat);

/* Grow a parser-owned zeroed element vector without losing its live owner.
 * Geometric growth keeps total element-copy/initialization work O(count) and
 * retained memory O(count); overflow/OOM leaves the old vector releasable. */
static void *parse_grow_zeroed(void *elements, int *capacity, size_t element_size) {
    if (!elements || !capacity || *capacity <= 0 || element_size == 0 ||
        *capacity > INT_MAX / PAIR_LEN) {
        return NULL;
    }
    int next = *capacity * PAIR_LEN;
    if ((size_t)next > SIZE_MAX / element_size) {
        return NULL;
    }
    void *grown = realloc(elements, (size_t)next * element_size);
    if (!grown) {
        return NULL;
    }
    memset((char *)grown + ((size_t)*capacity * element_size), 0,
           (size_t)(next - *capacity) * element_size);
    *capacity = next;
    return grown;
}

/* Parse a single MATCH pattern into pat */
static int parse_match_pattern(parser_t *p, cbm_pattern_t *pat) {
    memset(pat, 0, sizeof(*pat));
    int node_cap = CYP_INIT_CAP4;
    int rel_cap = CYP_INIT_CAP4;
    pat->nodes = calloc((size_t)node_cap, sizeof(cbm_node_pattern_t));
    pat->rels = calloc(rel_cap, sizeof(cbm_rel_pattern_t));
    if (!pat->nodes || !pat->rels) {
        snprintf(p->error, sizeof(p->error), "out of memory parsing MATCH pattern");
        goto fail;
    }

    pat->node_count = SKIP_ONE;
    if (parse_node(p, &pat->nodes[0]) < 0) {
        goto fail;
    }

    while (check(p, TOK_DASH) || check(p, TOK_LT)) {
        if (pat->rel_count >= rel_cap) {
            cbm_rel_pattern_t *grown = parse_grow_zeroed(pat->rels, &rel_cap, sizeof(*pat->rels));
            if (!grown) {
                snprintf(p->error, sizeof(p->error),
                         "MATCH relationship list is too large or out of memory");
                goto fail;
            }
            pat->rels = grown;
        }
        int relationship_index = pat->rel_count++;
        if (parse_rel(p, &pat->rels[relationship_index]) < 0) {
            goto fail;
        }

        if (pat->node_count >= node_cap) {
            cbm_node_pattern_t *grown =
                parse_grow_zeroed(pat->nodes, &node_cap, sizeof(*pat->nodes));
            if (!grown) {
                snprintf(p->error, sizeof(p->error),
                         "MATCH node list is too large or out of memory");
                goto fail;
            }
            pat->nodes = grown;
        }
        int node_index = pat->node_count++;
        if (parse_node(p, &pat->nodes[node_index]) < 0) {
            goto fail;
        }
    }
    return 0;

fail:
    free_pattern(pat);
    memset(pat, 0, sizeof(*pat));
    return CBM_NOT_FOUND;
}

/* Parse UNWIND [...] AS var into a normalized JSON array. The executor consumes
 * this same owned representation, so parser acceptance cannot drift into a
 * write-only AST field. Dynamic growth removes the former 2 KiB serialization
 * ceiling; the lexer still enforces its shared per-token bound. */
static int parse_unwind_clause(parser_t *p, cbm_query_t *q) {
    advance(p);
    if (check(p, TOK_LBRACKET)) {
        advance(p);
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *list = doc ? yyjson_mut_arr(doc) : NULL;
        if (!doc || !list) {
            yyjson_mut_doc_free(doc);
            snprintf(p->error, sizeof(p->error), "out of memory parsing UNWIND list");
            return CBM_NOT_FOUND;
        }
        while (!check(p, TOK_RBRACKET) && !check(p, TOK_EOF)) {
            yyjson_mut_val *value = NULL;
            if (check(p, TOK_STRING)) {
                value = yyjson_mut_strcpy(doc, advance(p)->text);
            } else if (check(p, TOK_NUMBER)) {
                value = yyjson_mut_rawcpy(doc, advance(p)->text);
            } else if (check(p, TOK_TRUE)) {
                advance(p);
                value = yyjson_mut_true(doc);
            } else if (check(p, TOK_FALSE)) {
                advance(p);
                value = yyjson_mut_false(doc);
            } else if (check(p, TOK_NULL_KW)) {
                advance(p);
                value = yyjson_mut_null(doc);
            } else {
                yyjson_mut_doc_free(doc);
                snprintf(p->error, sizeof(p->error),
                         "UNWIND literal lists support scalar JSON values");
                return CBM_NOT_FOUND;
            }
            if (!value || !yyjson_mut_arr_append(list, value)) {
                yyjson_mut_doc_free(doc);
                snprintf(p->error, sizeof(p->error), "out of memory parsing UNWIND list");
                return CBM_NOT_FOUND;
            }
            if (!match(p, TOK_COMMA) && !check(p, TOK_RBRACKET)) {
                yyjson_mut_doc_free(doc);
                snprintf(p->error, sizeof(p->error), "expected ',' or ']' in UNWIND list");
                return CBM_NOT_FOUND;
            }
        }
        if (!expect(p, TOK_RBRACKET)) {
            yyjson_mut_doc_free(doc);
            return CBM_NOT_FOUND;
        }
        yyjson_mut_doc_set_root(doc, list);
        q->unwind_expr = yyjson_mut_write(doc, 0, NULL);
        yyjson_mut_doc_free(doc);
        if (!q->unwind_expr) {
            snprintf(p->error, sizeof(p->error), "out of memory parsing UNWIND list");
            return CBM_NOT_FOUND;
        }
    } else if (check(p, TOK_IDENT)) {
        q->unwind_expr = heap_strdup(advance(p)->text);
        if (!q->unwind_expr) {
            snprintf(p->error, sizeof(p->error), "out of memory parsing UNWIND expression");
            return CBM_NOT_FOUND;
        }
    } else {
        snprintf(p->error, sizeof(p->error), "expected a literal list or variable after UNWIND");
        return CBM_NOT_FOUND;
    }
    if (!expect(p, TOK_AS)) {
        return CBM_NOT_FOUND;
    }
    const cbm_token_t *alias = expect(p, TOK_IDENT);
    if (alias) {
        q->unwind_alias = heap_strdup(alias->text);
        if (!q->unwind_alias) {
            snprintf(p->error, sizeof(p->error), "out of memory parsing UNWIND alias");
            return CBM_NOT_FOUND;
        }
    }
    return alias ? 0 : CBM_NOT_FOUND;
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
    if (!q) {
        out->error = heap_strdup("out of memory parsing query");
        return CBM_NOT_FOUND;
    }

    if (check(&p, TOK_UNWIND)) {
        if (parse_unwind_clause(&p, q) < 0) {
            out->error = heap_strdup(p.error[0] ? p.error : "failed to parse UNWIND");
            cbm_query_free(q);
            return CBM_NOT_FOUND;
        }
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

static void free_return_item(cbm_return_item_t *item) {
    if (!item) {
        return;
    }
    safe_str_free(&item->variable);
    safe_str_free(&item->property);
    safe_str_free(&item->alias);
    safe_str_free(&item->func);
    free_case_expr(item->kase);
    for (int j = 0; j < item->arg_count; j++) {
        safe_str_free(&item->args[j].variable);
        safe_str_free(&item->args[j].property);
        safe_str_free(&item->args[j].literal);
    }
    free(item->args);
    memset(item, 0, sizeof(*item));
}

static void free_order_item(cbm_order_item_t *item) {
    if (!item) {
        return;
    }
    safe_str_free(&item->expression);
    safe_str_free(&item->direction);
    memset(item, 0, sizeof(*item));
}

static void free_return_clause(cbm_return_clause_t *r) {
    if (!r) {
        return;
    }
    for (int i = 0; i < r->count; i++) {
        free_return_item(&r->items[i]);
    }
    free(r->items);
    for (int i = 0; i < r->order_count; i++) {
        free_order_item(&r->order_items[i]);
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

/* Logical kind travels beside the existing exact string representation. It
 * adds O(1) work and one enum per live value/binding slot, while allowing
 * scalar semantics to distinguish JSON strings from numbers and booleans
 * without reparsing or creating a second value/evaluator hierarchy. */
typedef enum {
    CYP_VALUE_NULL = 0,
    CYP_VALUE_STRING,
    CYP_VALUE_INTEGER,
    CYP_VALUE_FLOAT,
    CYP_VALUE_BOOLEAN,
    CYP_VALUE_COMPOSITE,
    CYP_VALUE_NODE,
    CYP_VALUE_RELATIONSHIP,
} cypher_value_kind_t;

typedef struct {
    const char *name;
    bool name_owned;
    bool is_null;
    cypher_value_kind_t kind;
    cbm_node_t node;
} binding_node_overflow_t;

typedef struct {
    const char *name;
    cbm_edge_t edge;
} binding_edge_overflow_t;

/* A binding maps variable names to nodes and/or edges. The inline arrays keep
 * ordinary queries allocation-free; geometric overflow makes query width an
 * O(V + E) storage concern rather than a correctness cap. */
typedef struct {
    const char *var_names[CYP_INLINE_NODE_VARS]; /* variable names (nodes) */
    bool var_name_owned[CYP_INLINE_NODE_VARS];   /* WITH aliases are heap-owned */
    bool var_is_null[CYP_INLINE_NODE_VARS];      /* projected null differs from an empty string */
    cypher_value_kind_t var_kinds[CYP_INLINE_NODE_VARS]; /* projected logical type */
    cbm_node_t var_nodes[CYP_INLINE_NODE_VARS];          /* node data */
    binding_node_overflow_t *var_overflow;
    int var_overflow_capacity;
    int var_count;
    const char *edge_var_names[CYP_INLINE_EDGE_VARS]; /* variable names (edges) */
    cbm_edge_t edge_vars[CYP_INLINE_EDGE_VARS];       /* edge data */
    binding_edge_overflow_t *edge_overflow;
    int edge_overflow_capacity;
    int edge_var_count;
    bool allocation_failed;
    cbm_store_t *store;  /* for computing in_degree/out_degree on demand */
    const char *project; /* borrowed project filter for active overlay qn-keyed lookups */
    bool use_active_overlay_edges;
} binding_t;

static void binding_free(binding_t *b);

/* Per-execution state: query execution is re-entrant across server threads,
 * while a ceiling hit must never be reported by another request. */
static _Thread_local int g_cypher_working_row_limit_hit = 0;
static _Thread_local bool g_cypher_allocation_failed = false;
static _Thread_local int g_cypher_trail_work_rows = 0;
static _Thread_local int g_cypher_trail_work_limit = 0;
static _Thread_local bool g_cypher_store_failed = false;
static _Thread_local char g_cypher_store_error[CBM_SZ_256];

typedef struct {
    char *data;
    size_t length;
    size_t capacity;
} cypher_string_builder_t;

/* Query-local geometric string construction. Appending total L bytes performs
 * O(L) total copy work and retains O(L) memory with constant-factor slack. */
static bool cypher_string_builder_reserve(cypher_string_builder_t *builder, size_t needed) {
    if (needed <= builder->capacity) {
        return true;
    }
    size_t next = builder->capacity ? builder->capacity : CBM_SZ_128;
    while (next < needed) {
        if (next > SIZE_MAX / PAIR_LEN) {
            next = needed;
            break;
        }
        next *= PAIR_LEN;
    }
    char *grown = realloc(builder->data, next);
    if (!grown) {
        g_cypher_allocation_failed = true;
        return false;
    }
    builder->data = grown;
    builder->capacity = next;
    return true;
}

static bool cypher_string_builder_reset(cypher_string_builder_t *builder) {
    builder->length = 0;
    if (!cypher_string_builder_reserve(builder, SKIP_ONE)) {
        return false;
    }
    builder->data[0] = '\0';
    return true;
}

static bool cypher_string_builder_append(cypher_string_builder_t *builder, const char *bytes,
                                         size_t length) {
    if (!bytes || builder->length > SIZE_MAX - SKIP_ONE ||
        length > SIZE_MAX - SKIP_ONE - builder->length) {
        g_cypher_allocation_failed = true;
        return false;
    }
    size_t needed = builder->length + length + SKIP_ONE;
    if (!cypher_string_builder_reserve(builder, needed)) {
        return false;
    }
    memcpy(builder->data + builder->length, bytes, length);
    builder->length += length;
    builder->data[builder->length] = '\0';
    return true;
}

static void cypher_string_builder_free(cypher_string_builder_t *builder) {
    free(builder->data);
    memset(builder, 0, sizeof(*builder));
}

static bool cypher_string_builder_append_json_string(cypher_string_builder_t *builder,
                                                     const char *text, size_t length);

/* Return a geometrically grown element capacity. If doubling an int would
 * overflow, the exact already-validated requirement is the only safe value. */
static int cypher_geometric_capacity(int current, int needed) {
    int next = current > 0 ? current : CYP_INIT_CAP8;
    while (next < needed) {
        if (next > INT_MAX / PAIR_LEN) {
            return needed;
        }
        next *= PAIR_LEN;
    }
    return next;
}

static bool binding_reserve_node_index(binding_t *b, int index) {
    if (index < CYP_INLINE_NODE_VARS) {
        return true;
    }
    int needed = index - CYP_INLINE_NODE_VARS + SKIP_ONE;
    if (needed <= b->var_overflow_capacity) {
        return true;
    }
    int next = cypher_geometric_capacity(b->var_overflow_capacity, needed);
    if ((size_t)next > SIZE_MAX / sizeof(*b->var_overflow)) {
        b->allocation_failed = true;
        return false;
    }
    void *grown = realloc(b->var_overflow, (size_t)next * sizeof(*b->var_overflow));
    if (!grown) {
        b->allocation_failed = true;
        return false;
    }
    b->var_overflow = grown;
    memset(&b->var_overflow[b->var_overflow_capacity], 0,
           (size_t)(next - b->var_overflow_capacity) * sizeof(*b->var_overflow));
    b->var_overflow_capacity = next;
    return true;
}

static bool binding_reserve_edge_index(binding_t *b, int index) {
    if (index < CYP_INLINE_EDGE_VARS) {
        return true;
    }
    int needed = index - CYP_INLINE_EDGE_VARS + SKIP_ONE;
    if (needed <= b->edge_overflow_capacity) {
        return true;
    }
    int next = cypher_geometric_capacity(b->edge_overflow_capacity, needed);
    if ((size_t)next > SIZE_MAX / sizeof(*b->edge_overflow)) {
        b->allocation_failed = true;
        return false;
    }
    void *grown = realloc(b->edge_overflow, (size_t)next * sizeof(*b->edge_overflow));
    if (!grown) {
        b->allocation_failed = true;
        return false;
    }
    b->edge_overflow = grown;
    memset(&b->edge_overflow[b->edge_overflow_capacity], 0,
           (size_t)(next - b->edge_overflow_capacity) * sizeof(*b->edge_overflow));
    b->edge_overflow_capacity = next;
    return true;
}

static const char *binding_node_name_at(const binding_t *b, int index) {
    return index < CYP_INLINE_NODE_VARS ? b->var_names[index]
                                        : b->var_overflow[index - CYP_INLINE_NODE_VARS].name;
}

static bool binding_node_name_owned_at(const binding_t *b, int index) {
    return index < CYP_INLINE_NODE_VARS ? b->var_name_owned[index]
                                        : b->var_overflow[index - CYP_INLINE_NODE_VARS].name_owned;
}

static bool binding_node_is_null_at(const binding_t *b, int index) {
    return index < CYP_INLINE_NODE_VARS ? b->var_is_null[index]
                                        : b->var_overflow[index - CYP_INLINE_NODE_VARS].is_null;
}

static cypher_value_kind_t binding_node_kind_at(const binding_t *b, int index) {
    return index < CYP_INLINE_NODE_VARS ? b->var_kinds[index]
                                        : b->var_overflow[index - CYP_INLINE_NODE_VARS].kind;
}

static cbm_node_t *binding_node_at(binding_t *b, int index) {
    return index < CYP_INLINE_NODE_VARS ? &b->var_nodes[index]
                                        : &b->var_overflow[index - CYP_INLINE_NODE_VARS].node;
}

static const cbm_node_t *binding_const_node_at(const binding_t *b, int index) {
    return index < CYP_INLINE_NODE_VARS ? &b->var_nodes[index]
                                        : &b->var_overflow[index - CYP_INLINE_NODE_VARS].node;
}

static const char *binding_edge_name_at(const binding_t *b, int index) {
    return index < CYP_INLINE_EDGE_VARS ? b->edge_var_names[index]
                                        : b->edge_overflow[index - CYP_INLINE_EDGE_VARS].name;
}

static cbm_edge_t *binding_edge_at(binding_t *b, int index) {
    return index < CYP_INLINE_EDGE_VARS ? &b->edge_vars[index]
                                        : &b->edge_overflow[index - CYP_INLINE_EDGE_VARS].edge;
}

static const cbm_edge_t *binding_const_edge_at(const binding_t *b, int index) {
    return index < CYP_INLINE_EDGE_VARS ? &b->edge_vars[index]
                                        : &b->edge_overflow[index - CYP_INLINE_EDGE_VARS].edge;
}

static void binding_set_node_metadata(binding_t *b, int index, const char *name, bool name_owned,
                                      bool is_null, cypher_value_kind_t kind) {
    if (index < CYP_INLINE_NODE_VARS) {
        b->var_names[index] = name;
        b->var_name_owned[index] = name_owned;
        b->var_is_null[index] = is_null;
        b->var_kinds[index] = is_null ? CYP_VALUE_NULL : kind;
        return;
    }
    binding_node_overflow_t *slot = &b->var_overflow[index - CYP_INLINE_NODE_VARS];
    slot->name = name;
    slot->name_owned = name_owned;
    slot->is_null = is_null;
    slot->kind = is_null ? CYP_VALUE_NULL : kind;
}

static void binding_set_edge_name(binding_t *b, int index, const char *name) {
    if (index < CYP_INLINE_EDGE_VARS) {
        b->edge_var_names[index] = name;
    } else {
        b->edge_overflow[index - CYP_INLINE_EDGE_VARS].name = name;
    }
}

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
        g_cypher_allocation_failed = true;
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
    if (row->allocation_failed) {
        g_cypher_allocation_failed = true;
        binding_free(row);
        return false;
    }
    if (*count >= limit) {
        g_cypher_working_row_limit_hit = limit;
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
typedef struct {
    const char *data; /* borrowed unless owned is non-NULL */
    size_t length;
    char *owned;
    bool is_null;
    cypher_value_kind_t kind;
    char inline_text[CBM_SZ_64];
} cypher_value_t;

typedef enum {
    CYP_JSON_PROP_NOT_FOUND = 0,
    CYP_JSON_PROP_FOUND,
    CYP_JSON_PROP_INVALID,
} cypher_json_prop_status_t;

static void cypher_value_set_borrowed(cypher_value_t *value, const char *data, size_t length,
                                      bool is_null) {
    memset(value, 0, sizeof(*value));
    value->data = data ? data : "";
    value->length = data ? length : 0;
    value->is_null = is_null;
    value->kind = is_null ? CYP_VALUE_NULL : CYP_VALUE_STRING;
}

static void cypher_value_set_borrowed_kind(cypher_value_t *value, const char *data, size_t length,
                                           cypher_value_kind_t kind) {
    bool is_null = kind == CYP_VALUE_NULL;
    cypher_value_set_borrowed(value, is_null ? "" : data, is_null ? 0 : length, is_null);
    value->kind = kind;
}

static void cypher_value_set_cstr(cypher_value_t *value, const char *text, bool is_null) {
    cypher_value_set_borrowed(value, text, text ? strlen(text) : 0, is_null);
}

static void cypher_value_set_int64(cypher_value_t *value, int64_t number);

static void cypher_value_set_int(cypher_value_t *value, int number) {
    cypher_value_set_int64(value, (int64_t)number);
}

/* Serialize through yyjson's public number writer so query-visible numeric
 * text is locale-independent and shares the same representation as UNWIND
 * JSON values. yyjson documents 21 bytes for integers and 40 for doubles;
 * cypher_value_t's existing CBM_SZ_64 inline storage covers both contracts.
 * Runtime and auxiliary/retained memory are O(1). */
static bool cypher_value_set_number(cypher_value_t *value, const yyjson_val *number,
                                    cypher_value_kind_t kind) {
    if (!value || !number || (kind != CYP_VALUE_INTEGER && kind != CYP_VALUE_FLOAT)) {
        return false;
    }
    memset(value, 0, sizeof(*value));
    char *end = yyjson_write_number(number, value->inline_text);
    if (!end || end < value->inline_text ||
        (size_t)(end - value->inline_text) >= sizeof(value->inline_text)) {
        g_cypher_allocation_failed = true;
        value->data = "";
        value->is_null = true;
        value->kind = CYP_VALUE_NULL;
        return false;
    }
    value->data = value->inline_text;
    value->length = (size_t)(end - value->inline_text);
    /* The established result-cell surface renders integral doubles like `%g`
     * ("2", not yyjson's type-preserving "2.0"). Keep that stable text while
     * the separate query-local kind continues to record FLOAT. */
    if (kind == CYP_VALUE_FLOAT && value->length >= sizeof(".0") - SKIP_ONE &&
        memcmp(value->inline_text + value->length - (sizeof(".0") - SKIP_ONE), ".0",
               sizeof(".0") - SKIP_ONE) == 0) {
        value->length -= sizeof(".0") - SKIP_ONE;
    }
    value->inline_text[value->length] = '\0';
    value->kind = kind;
    return true;
}

static void cypher_value_set_int64(cypher_value_t *value, int64_t number) {
    yyjson_val json_number = {0};
    if (!yyjson_set_sint(&json_number, number) ||
        !cypher_value_set_number(value, &json_number, CYP_VALUE_INTEGER)) {
        g_cypher_allocation_failed = true;
    }
}

static void cypher_value_set_double(cypher_value_t *value, double number) {
    yyjson_val json_number = {0};
    if (!isfinite(number) || !yyjson_set_real(&json_number, number) ||
        !cypher_value_set_number(value, &json_number, CYP_VALUE_FLOAT)) {
        g_cypher_allocation_failed = true;
        cypher_value_set_cstr(value, "", true);
    }
}

static bool cypher_value_own(cypher_value_t *value) {
    if (value->owned || value->is_null) {
        return true;
    }
    if (value->length > SIZE_MAX - SKIP_ONE) {
        g_cypher_allocation_failed = true;
        return false;
    }
    char *copy = malloc(value->length + SKIP_ONE);
    if (!copy) {
        g_cypher_allocation_failed = true;
        return false;
    }
    memcpy(copy, value->data, value->length);
    copy[value->length] = '\0';
    value->owned = copy;
    value->data = copy;
    return true;
}

static void cypher_value_free(cypher_value_t *value) {
    if (!value) {
        return;
    }
    free(value->owned);
    memset(value, 0, sizeof(*value));
}

static void cypher_value_move(cypher_value_t *destination, cypher_value_t *source) {
    *destination = *source;
    if (source->data == source->inline_text) {
        destination->data = destination->inline_text;
    }
    memset(source, 0, sizeof(*source));
}

/* Adapt yyjson's representation at the one executor boundary that consumes a
 * parsed JSON value. The Cypher kind remains independent of yyjson's packed
 * ABI tags, while conversion stays O(N) runtime in serialized value bytes and
 * O(N) retained memory only for arrays/objects; scalar values are borrowed or
 * use the existing fixed numeric representation. */
static bool cypher_value_set_yyjson(cypher_value_t *value, const yyjson_val *json) {
    if (!value || !json) {
        return false;
    }
    if (yyjson_is_null(json)) {
        cypher_value_set_cstr(value, "", true);
        return true;
    }
    if (yyjson_is_str(json)) {
        const char *text = yyjson_get_str(json);
        cypher_value_set_borrowed_kind(value, text ? text : "", text ? strlen(text) : 0,
                                       CYP_VALUE_STRING);
        return true;
    }
    if (yyjson_is_bool(json)) {
        const char *text = yyjson_get_bool(json) ? "true" : "false";
        cypher_value_set_borrowed_kind(value, text, strlen(text), CYP_VALUE_BOOLEAN);
        return true;
    }
    if (yyjson_is_num(json)) {
        cypher_value_kind_t kind = yyjson_is_real(json) ? CYP_VALUE_FLOAT : CYP_VALUE_INTEGER;
        return cypher_value_set_number(value, json, kind);
    }
    if (yyjson_is_arr(json) || yyjson_is_obj(json)) {
        size_t length = 0;
        char *serialized = yyjson_val_write(json, YYJSON_WRITE_NOFLAG, &length);
        if (!serialized) {
            return false;
        }
        memset(value, 0, sizeof(*value));
        value->data = serialized;
        value->length = length;
        value->owned = serialized;
        value->kind = CYP_VALUE_COMPOSITE;
        return true;
    }
    return false;
}

static const char *cypher_json_skip_ws(const char *cursor) {
    while (*cursor && isspace((unsigned char)*cursor)) {
        cursor++;
    }
    return cursor;
}

static bool cypher_json_is_empty_object(const char *json) {
    if (!json) {
        return true;
    }
    const char *cursor = cypher_json_skip_ws(json);
    if (*cursor++ != '{') {
        return false;
    }
    cursor = cypher_json_skip_ws(cursor);
    if (*cursor++ != '}') {
        return false;
    }
    return *cypher_json_skip_ws(cursor) == '\0';
}

/* Return the closing quote for a JSON string, honoring escape pairs. */
static const char *cypher_json_string_end(const char *opening_quote) {
    if (!opening_quote || *opening_quote != '"') {
        return NULL;
    }
    for (const char *cursor = opening_quote + SKIP_ONE; *cursor; cursor++) {
        if (*cursor == '\\') {
            if (!cursor[SKIP_ONE]) {
                return NULL;
            }
            cursor++;
        } else if (*cursor == '"') {
            return cursor;
        }
    }
    return NULL;
}

/* Find one exact top-level JSON object member and return a borrowed raw-value
 * view. Runtime is O(J + K) for JSON bytes J and key bytes K, with O(1)
 * auxiliary memory and no pattern allocation. String escape bytes and complete
 * array/object spelling are preserved to match the established Cypher surface. */
static cypher_json_prop_status_t cypher_json_property_view(const char *json, const char *key,
                                                           cypher_value_t *value) {
    cypher_value_set_cstr(value, "", true);
    if (!json || !key) {
        return CYP_JSON_PROP_NOT_FOUND;
    }
    size_t key_length = strlen(key);
    const char *cursor = cypher_json_skip_ws(json);
    if (*cursor++ != '{') {
        return CYP_JSON_PROP_INVALID;
    }
    for (;;) {
        cursor = cypher_json_skip_ws(cursor);
        if (*cursor == '}') {
            return CYP_JSON_PROP_NOT_FOUND;
        }
        if (*cursor != '"') {
            return CYP_JSON_PROP_INVALID;
        }
        const char *key_end = cypher_json_string_end(cursor);
        if (!key_end) {
            return CYP_JSON_PROP_INVALID;
        }
        const char *member_key = cursor + SKIP_ONE;
        size_t member_key_length = (size_t)(key_end - member_key);
        bool matches = member_key_length == key_length && memcmp(member_key, key, key_length) == 0;
        cursor = cypher_json_skip_ws(key_end + SKIP_ONE);
        if (*cursor++ != ':') {
            return CYP_JSON_PROP_INVALID;
        }
        cursor = cypher_json_skip_ws(cursor);
        const char *value_start = cursor;
        const char *value_end = NULL;
        cypher_value_kind_t kind = CYP_VALUE_STRING;
        if (*cursor == '"') {
            const char *string_end = cypher_json_string_end(cursor);
            if (!string_end) {
                return CYP_JSON_PROP_INVALID;
            }
            value_start = cursor + SKIP_ONE;
            value_end = string_end;
            cursor = string_end + SKIP_ONE;
        } else if (*cursor == '[' || *cursor == '{') {
            kind = CYP_VALUE_COMPOSITE;
            int array_depth = 0;
            int object_depth = 0;
            for (; *cursor; cursor++) {
                if (*cursor == '"') {
                    const char *string_end = cypher_json_string_end(cursor);
                    if (!string_end) {
                        return CYP_JSON_PROP_INVALID;
                    }
                    cursor = string_end;
                    continue;
                }
                if (*cursor == '[') {
                    array_depth++;
                } else if (*cursor == ']') {
                    if (--array_depth < 0) {
                        return CYP_JSON_PROP_INVALID;
                    }
                } else if (*cursor == '{') {
                    object_depth++;
                } else if (*cursor == '}') {
                    if (--object_depth < 0) {
                        return CYP_JSON_PROP_INVALID;
                    }
                }
                if (array_depth == 0 && object_depth == 0) {
                    cursor++;
                    value_end = cursor;
                    break;
                }
            }
            if (!value_end) {
                return CYP_JSON_PROP_INVALID;
            }
        } else {
            while (*cursor && *cursor != ',' && *cursor != '}' &&
                   !isspace((unsigned char)*cursor)) {
                cursor++;
            }
            value_end = cursor;
            size_t raw_length = (size_t)(value_end - value_start);
            bool is_null = raw_length == sizeof("null") - SKIP_ONE &&
                           memcmp(value_start, "null", sizeof("null") - SKIP_ONE) == 0;
            if (is_null) {
                kind = CYP_VALUE_NULL;
            } else if ((raw_length == sizeof("true") - SKIP_ONE &&
                        memcmp(value_start, "true", raw_length) == 0) ||
                       (raw_length == sizeof("false") - SKIP_ONE &&
                        memcmp(value_start, "false", raw_length) == 0)) {
                kind = CYP_VALUE_BOOLEAN;
            } else if (memchr(value_start, '.', raw_length) ||
                       memchr(value_start, 'e', raw_length) ||
                       memchr(value_start, 'E', raw_length)) {
                kind = CYP_VALUE_FLOAT;
            } else {
                kind = CYP_VALUE_INTEGER;
            }
        }
        if (matches) {
            cypher_value_set_borrowed_kind(value, value_start, (size_t)(value_end - value_start),
                                           kind);
            return CYP_JSON_PROP_FOUND;
        }
        cursor = cypher_json_skip_ws(cursor);
        if (*cursor == ',') {
            cursor++;
            continue;
        }
        if (*cursor == '}') {
            return CYP_JSON_PROP_NOT_FOUND;
        }
        return CYP_JSON_PROP_INVALID;
    }
}

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
             * and are handled separately by cypher_json_property_view(). */
            *is_null = val == NULL || val[0] == '\0';
            return val ? val : "";
        }
    }
    return NULL;
}

/* Get node property by name.
 * store may be NULL; only needed for virtual degree properties. */
static void node_fields_free(cbm_node_t *n); /* defined below; used by the stub re-fetch */
static void node_prop_value(const cbm_node_t *n, const char *prop, cbm_store_t *store,
                            const char *project, bool use_active_overlay_edges,
                            cypher_value_t *value);

static const char *node_prop_ex(const cbm_node_t *n, const char *prop, cbm_store_t *store,
                                const char *project, bool use_active_overlay_edges, bool *is_null) {
    cypher_value_t value;
    node_prop_value(n, prop, store, project, use_active_overlay_edges, &value);
    *is_null = value.is_null;
    if (value.is_null) {
        cypher_value_free(&value);
        return "";
    }
    /* Preserve direct NUL-terminated node fields without scratch copying. */
    if (!value.owned && value.data != value.inline_text && value.data[value.length] == '\0') {
        return value.data;
    }
    /* Legacy string consumers receive a bounded compatibility view. Exact
     * projection/aggregation paths consume cypher_value_t directly below. */
    static _Thread_local char bufs[CYP_BUF_8][CBM_SZ_512];
    static _Thread_local int buf_idx = 0;
    char *out = bufs[buf_idx];
    buf_idx = (buf_idx + SKIP_ONE) % CYP_BUF_8;
    size_t copy_length =
        value.length < CBM_SZ_512 - SKIP_ONE ? value.length : CBM_SZ_512 - SKIP_ONE;
    memcpy(out, value.data, copy_length);
    out[copy_length] = '\0';
    cypher_value_free(&value);
    return out;
}

static const char *node_prop(const cbm_node_t *n, const char *prop, cbm_store_t *store,
                             const char *project, bool use_active_overlay_edges) {
    bool is_null = true;
    return node_prop_ex(n, prop, store, project, use_active_overlay_edges, &is_null);
}

/* Resolve one node property as an exact-length value. Borrowed core/JSON
 * storage remains valid with the bound node; only projected-stub re-fetches
 * take an owned copy before releasing the temporary store row. */
static void node_prop_value(const cbm_node_t *n, const char *prop, cbm_store_t *store,
                            const char *project, bool use_active_overlay_edges,
                            cypher_value_t *value) {
    cypher_value_set_cstr(value, "", true);
    if (!n || !prop) {
        return;
    }
    bool field_is_null = true;
    const char *field = node_string_field(n, prop, &field_is_null);
    bool may_be_projected_stub = store && n->id > 0 && !n->file_path && !n->label;
    if (field && (!field_is_null || !may_be_projected_stub)) {
        cypher_value_set_cstr(value, field, field_is_null);
        return;
    }
    if (strcmp(prop, "start_line") == 0) {
        cypher_value_set_int(value, n->start_line);
        return;
    }
    if (strcmp(prop, "end_line") == 0) {
        cypher_value_set_int(value, n->end_line);
        return;
    }
    if (store && (strcmp(prop, "in_degree") == 0 || strcmp(prop, "out_degree") == 0)) {
        int in_degree = 0;
        int out_degree = 0;
        if (use_active_overlay_edges && project && n->qualified_name && n->qualified_name[0]) {
            (void)cbm_store_active_node_degree_by_qn(store, project, n->qualified_name, &in_degree,
                                                     &out_degree);
        } else {
            cbm_store_node_degree(store, n->id, &in_degree, &out_degree);
        }
        cypher_value_set_int(value, strcmp(prop, "in_degree") == 0 ? in_degree : out_degree);
        return;
    }
    if (n->properties_json && n->properties_json[0] == '{') {
        cypher_json_prop_status_t status =
            cypher_json_property_view(n->properties_json, prop, value);
        if (status == CYP_JSON_PROP_FOUND) {
            return;
        }
        if (status == CYP_JSON_PROP_INVALID) {
            g_cypher_store_failed = true;
            snprintf(g_cypher_store_error, sizeof(g_cypher_store_error),
                     "node %lld has invalid properties JSON", (long long)n->id);
            return;
        }
    }
    if (may_be_projected_stub) {
        cbm_node_t full = {0};
        if (cbm_store_find_node_by_id(store, n->id, &full) == CBM_STORE_OK) {
            node_prop_value(&full, prop, NULL, project, use_active_overlay_edges, value);
            if (!value->is_null) {
                (void)cypher_value_own(value);
            }
            node_fields_free(&full);
        }
    }
}

static void edge_prop_value(const cbm_edge_t *edge, const char *prop, cypher_value_t *value) {
    cypher_value_set_cstr(value, "", true);
    if (!edge || !prop) {
        return;
    }
    if (strcmp(prop, "type") == 0) {
        cypher_value_set_cstr(value, edge->type ? edge->type : "", edge->type == NULL);
        return;
    }
    cypher_json_prop_status_t status =
        cypher_json_property_view(edge->properties_json, prop, value);
    if (status == CYP_JSON_PROP_INVALID) {
        g_cypher_store_failed = true;
        snprintf(g_cypher_store_error, sizeof(g_cypher_store_error),
                 "edge %lld has invalid properties JSON", (long long)edge->id);
    }
}

/* Get edge property by name. Uses rotating thread-local buffers to allow
 * multiple concurrent calls (e.g. projecting r.url_path, r.confidence
 * in the same row). */
static const char *edge_prop_ex(const cbm_edge_t *e, const char *prop, bool *is_null) {
    cypher_value_t value;
    edge_prop_value(e, prop, &value);
    *is_null = value.is_null;
    if (value.is_null) {
        return "";
    }
    if (!value.owned && value.data != value.inline_text && value.data[value.length] == '\0') {
        return value.data;
    }
    static CBM_TLS char ebufs[CYP_BUF_8][CBM_SZ_512];
    static CBM_TLS int ebuf_idx = 0;
    char *buf = ebufs[ebuf_idx++ & CYP_EBUF_MASK];
    size_t copy_length =
        value.length < CBM_SZ_512 - SKIP_ONE ? value.length : CBM_SZ_512 - SKIP_ONE;
    memcpy(buf, value.data, copy_length);
    buf[copy_length] = '\0';
    cypher_value_free(&value);
    return buf;
}

static const char *edge_prop(const cbm_edge_t *e, const char *prop) {
    bool is_null = true;
    return edge_prop_ex(e, prop, &is_null);
}

/* Find an edge variable in a binding */
static cbm_edge_t *binding_get_edge(binding_t *b, const char *var) {
    for (int i = 0; i < b->edge_var_count; i++) {
        if (strcmp(binding_edge_name_at(b, i), var) == 0) {
            return binding_edge_at(b, i);
        }
    }
    return NULL;
}

/* Find a variable's node in a binding */
static cbm_node_t *binding_get(binding_t *b, const char *var) {
    for (int i = 0; i < b->var_count; i++) {
        if (strcmp(binding_node_name_at(b, i), var) == 0) {
            return binding_node_at(b, i);
        }
    }
    return NULL;
}

/* Deep copy a node: heap-dup all string fields so the binding owns them */
static bool node_deep_copy(cbm_node_t *dst, const cbm_node_t *src) {
    *dst = *src;
    dst->project = NULL;
    dst->label = NULL;
    dst->name = NULL;
    dst->qualified_name = NULL;
    dst->file_path = NULL;
    dst->properties_json = NULL;
    dst->project = heap_strdup(src->project);
    dst->label = heap_strdup(src->label);
    dst->name = heap_strdup(src->name);
    dst->qualified_name = heap_strdup(src->qualified_name);
    dst->file_path = heap_strdup(src->file_path);
    dst->properties_json = heap_strdup(src->properties_json);
    if ((src->project && !dst->project) || (src->label && !dst->label) ||
        (src->name && !dst->name) || (src->qualified_name && !dst->qualified_name) ||
        (src->file_path && !dst->file_path) || (src->properties_json && !dst->properties_json)) {
        node_fields_free(dst);
        memset(dst, 0, sizeof(*dst));
        return false;
    }
    return true;
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
static void edge_fields_free(cbm_edge_t *e);

static bool edge_deep_copy(cbm_edge_t *dst, const cbm_edge_t *src) {
    *dst = *src;
    dst->project = NULL;
    dst->type = NULL;
    dst->properties_json = NULL;
    dst->project = heap_strdup(src->project);
    dst->type = heap_strdup(src->type);
    dst->properties_json = heap_strdup(src->properties_json);
    if ((src->project && !dst->project) || (src->type && !dst->type) ||
        (src->properties_json && !dst->properties_json)) {
        edge_fields_free(dst);
        memset(dst, 0, sizeof(*dst));
        return false;
    }
    return true;
}

static void edge_fields_free(cbm_edge_t *e) {
    safe_str_free(&e->project);
    safe_str_free(&e->type);
    safe_str_free(&e->properties_json);
}

/* Set an edge variable in a binding */
static void binding_set_edge(binding_t *b, const char *var, const cbm_edge_t *edge) {
    /* Build replacements before releasing existing storage so OOM cannot
     * corrupt a binding that remains reachable during unwinding. */
    for (int i = 0; i < b->edge_var_count; i++) {
        if (strcmp(binding_edge_name_at(b, i), var) == 0) {
            cbm_edge_t replacement = {0};
            if (!edge_deep_copy(&replacement, edge)) {
                b->allocation_failed = true;
                return;
            }
            cbm_edge_t *slot = binding_edge_at(b, i);
            edge_fields_free(slot);
            *slot = replacement;
            return;
        }
    }
    int index = b->edge_var_count;
    if (!binding_reserve_edge_index(b, index)) {
        return;
    }
    cbm_edge_t *slot = binding_edge_at(b, index);
    if (!edge_deep_copy(slot, edge)) {
        b->allocation_failed = true;
        return;
    }
    binding_set_edge_name(b, index, var); /* borrowed from the AST */
    b->edge_var_count++;
}

/* Free all deep-copied nodes and edges in a binding */
static void binding_free(binding_t *b) {
    for (int i = 0; i < b->var_count; i++) {
        node_fields_free(binding_node_at(b, i));
        if (binding_node_name_owned_at(b, i)) {
            free((void *)binding_node_name_at(b, i));
        }
    }
    for (int i = 0; i < b->edge_var_count; i++) {
        edge_fields_free(binding_edge_at(b, i));
    }
    free(b->var_overflow);
    free(b->edge_overflow);
    memset(b, 0, sizeof(*b));
}

/* Deep-copy a binding (so source and dest own separate string copies) */
static void binding_copy(binding_t *dst, const binding_t *src) {
    memset(dst, 0, sizeof(*dst));
    dst->store = src->store;
    dst->project = src->project;
    dst->use_active_overlay_edges = src->use_active_overlay_edges;
    if (src->allocation_failed) {
        dst->allocation_failed = true;
        return;
    }
    for (int i = 0; i < src->var_count; i++) {
        if (!binding_reserve_node_index(dst, i)) {
            return;
        }
        const char *src_name = binding_node_name_at(src, i);
        bool owned = binding_node_name_owned_at(src, i);
        const char *dst_name = owned ? heap_strdup(src_name) : src_name;
        if ((owned && src_name && !dst_name) ||
            !node_deep_copy(binding_node_at(dst, i), binding_const_node_at(src, i))) {
            free(owned ? (void *)dst_name : NULL);
            dst->allocation_failed = true;
            return;
        }
        binding_set_node_metadata(dst, i, dst_name, owned, binding_node_is_null_at(src, i),
                                  binding_node_kind_at(src, i));
        dst->var_count++;
    }
    for (int i = 0; i < src->edge_var_count; i++) {
        if (!binding_reserve_edge_index(dst, i) ||
            !edge_deep_copy(binding_edge_at(dst, i), binding_const_edge_at(src, i))) {
            dst->allocation_failed = true;
            return;
        }
        binding_set_edge_name(dst, i, binding_edge_name_at(src, i)); /* AST-owned */
        dst->edge_var_count++;
    }
}

/* Deep-copy a node into a binding (binding owns the strings) */
static void binding_set(binding_t *b, const char *var, const cbm_node_t *node) {
    for (int i = 0; i < b->var_count; i++) {
        if (strcmp(binding_node_name_at(b, i), var) == 0) {
            cbm_node_t replacement = {0};
            if (!node_deep_copy(&replacement, node)) {
                b->allocation_failed = true;
                return;
            }
            cbm_node_t *slot = binding_node_at(b, i);
            node_fields_free(slot);
            *slot = replacement;
            binding_set_node_metadata(b, i, binding_node_name_at(b, i),
                                      binding_node_name_owned_at(b, i), false, CYP_VALUE_NODE);
            return;
        }
    }
    int index = b->var_count;
    if (!binding_reserve_node_index(b, index)) {
        return;
    }
    if (!node_deep_copy(binding_node_at(b, index), node)) {
        b->allocation_failed = true;
        return;
    }
    binding_set_node_metadata(b, index, var, false, false,
                              CYP_VALUE_NODE); /* borrowed from the AST */
    b->var_count++;
}

static const char *binding_get_virtual_ex(binding_t *b, const char *var, const char *prop,
                                          bool *is_null);
static void binding_get_virtual_value(binding_t *b, const char *var, const char *prop,
                                      cypher_value_t *value);
static void eval_multiarg_value(binding_t *binding, const cbm_return_item_t *item,
                                cypher_value_t *value);

/* Resolve the actual property value and preserve the Cypher distinction between
 * null and a valid empty string. This is the shared lookup used by projection,
 * aggregation, WHERE, and scalar functions. */
static void resolve_condition_value(const cbm_condition_t *c, binding_t *b, cypher_value_t *value) {
    /* Multi-arg scalar function LHS: coalesce(f.depth, 0) >= 2 (#874).
     * Reuse the exact projection evaluator so WHERE cannot observe a shorter
     * value than RETURN, WITH, or aggregate grouping. */
    if (c->func) {
        cbm_return_item_t item = {0};
        item.variable = c->variable;
        item.property = c->property;
        item.func = c->func;
        item.args = c->args;
        item.arg_count = c->arg_count;
        eval_multiarg_value(b, &item, value);
        return;
    }
    binding_get_virtual_value(b, c->variable, c->property, value);
}

static bool cypher_value_equals_cstr(const cypher_value_t *actual, const char *expected) {
    size_t expected_length = expected ? strlen(expected) : 0;
    return actual->length == expected_length &&
           memcmp(actual->data, expected ? expected : "", expected_length) == 0;
}

/* Build the KMP failure table shared by CONTAINS and replace(). For pattern
 * length P, this is O(P) runtime and O(P) temporary memory. */
static size_t *cypher_kmp_prefix_create(const char *pattern, size_t pattern_length) {
    if (!pattern || pattern_length == 0 || pattern_length > SIZE_MAX / sizeof(size_t)) {
        if (pattern_length > SIZE_MAX / sizeof(size_t)) {
            g_cypher_allocation_failed = true;
        }
        return NULL;
    }
    size_t *prefix = calloc(pattern_length, sizeof(*prefix));
    if (!prefix) {
        g_cypher_allocation_failed = true;
        return NULL;
    }
    for (size_t i = SKIP_ONE, matched = 0; i < pattern_length; i++) {
        while (matched > 0 && pattern[i] != pattern[matched]) {
            matched = prefix[matched - SKIP_ONE];
        }
        if (pattern[i] == pattern[matched]) {
            matched++;
        }
        prefix[i] = matched;
    }
    return prefix;
}

/* Portable linear-time substring search over a non-NUL-terminated value view.
 * For actual length A and expected length E, runtime is O(A + E), auxiliary
 * memory O(E), and allocation failure aborts the query rather than changing a
 * CONTAINS answer. */
static bool cypher_value_contains_cstr(const cypher_value_t *actual, const char *expected) {
    size_t expected_length = expected ? strlen(expected) : 0;
    if (expected_length == 0) {
        return true;
    }
    if (expected_length > actual->length) {
        return false;
    }
    size_t *prefix = cypher_kmp_prefix_create(expected, expected_length);
    if (!prefix) {
        return false;
    }
    bool found = false;
    for (size_t i = 0, matched = 0; i < actual->length; i++) {
        while (matched > 0 && actual->data[i] != expected[matched]) {
            matched = prefix[matched - SKIP_ONE];
        }
        if (actual->data[i] == expected[matched]) {
            matched++;
        }
        if (matched == expected_length) {
            found = true;
            break;
        }
    }
    free(prefix);
    return found;
}

/* Evaluate a comparison operator against an exact-length value. Equality and
 * string predicates avoid materialization; regex/numeric library calls take
 * one exact owned copy because their portable APIs require NUL termination. */
static bool eval_comparison_op(const char *op, cypher_value_t *actual, const char *expected) {
    if (strcmp(op, "=") == 0) {
        return cypher_value_equals_cstr(actual, expected);
    }
    if (strcmp(op, "<>") == 0) {
        return !cypher_value_equals_cstr(actual, expected);
    }
    if (strcmp(op, "=~") == 0) {
        if (!cypher_value_own(actual)) {
            return false;
        }
        cbm_regex_t re;
        if (cbm_regcomp(&re, expected, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            return false;
        }
        int rc = cbm_regexec(&re, actual->data, 0, NULL, 0);
        cbm_regfree(&re);
        return rc == 0;
    }
    if (strcmp(op, "CONTAINS") == 0) {
        return cypher_value_contains_cstr(actual, expected);
    }
    if (strcmp(op, "STARTS WITH") == 0) {
        size_t expected_length = strlen(expected);
        return actual->length >= expected_length &&
               memcmp(actual->data, expected, expected_length) == 0;
    }
    if (strcmp(op, "ENDS WITH") == 0) {
        size_t expected_length = strlen(expected);
        return actual->length >= expected_length &&
               memcmp(actual->data + actual->length - expected_length, expected, expected_length) ==
                   0;
    }
    if (strcmp(op, ">") == 0 || strcmp(op, "<") == 0 || strcmp(op, ">=") == 0 ||
        strcmp(op, "<=") == 0) {
        if (!cypher_value_own(actual)) {
            return false;
        }
        double a = strtod(actual->data, NULL);
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
                int dir =
                    c->exists_dir == CBM_STORE_EDGE_DIR_INBOUND
                        ? CBM_STORE_EDGE_DIR_INBOUND
                        : (c->exists_dir == CBM_STORE_EDGE_DIR_ANY ? CBM_STORE_EDGE_DIR_ANY
                                                                   : CBM_STORE_EDGE_DIR_OUTBOUND);
                (void)cbm_store_active_edge_exists_by_qn(b->store, b->project, n->qualified_name,
                                                         c->value, dir, &result);
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

    cypher_value_t actual;
    resolve_condition_value(c, b, &actual);
    /* Legacy two-argument coalesce representation: fall back only for null,
     * never for a present empty string. */
    if (c->coalesce_default && actual.is_null) {
        cypher_value_free(&actual);
        cypher_value_set_cstr(&actual, c->coalesce_default, false);
    }

    bool result;

    /* IS NULL / IS NOT NULL */
    if (strcmp(c->op, "IS NULL") == 0) {
        result = actual.is_null;
        cypher_value_free(&actual);
        return c->negated ? !result : result;
    }
    if (strcmp(c->op, "IS NOT NULL") == 0) {
        result = !actual.is_null;
        cypher_value_free(&actual);
        return c->negated ? !result : result;
    }
    /* A null comparison is unknown and therefore does not pass WHERE. */
    if (actual.is_null) {
        cypher_value_free(&actual);
        return false;
    }

    /* IN [...] */
    if (strcmp(c->op, "IN") == 0) {
        result = false;
        for (int i = 0; i < c->in_value_count; i++) {
            if (cypher_value_equals_cstr(&actual, c->in_values[i])) {
                result = true;
                break;
            }
        }
        cypher_value_free(&actual);
        return c->negated ? !result : result;
    }

    result = eval_comparison_op(c->op, &actual, c->value);
    cypher_value_free(&actual);
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
    bool truncated;
} result_builder_t;

typedef enum { CYP_NODE_SCAN_CANONICAL = 0, CYP_NODE_SCAN_ACTIVE_OVERLAY } cypher_node_scan_mode_t;

static void rb_init(result_builder_t *rb) {
    memset(rb, 0, sizeof(*rb));
    rb->row_cap = CBM_SZ_32;
    rb->rows = calloc((size_t)rb->row_cap, sizeof(*rb->rows));
    if (!rb->rows) {
        rb->row_cap = 0;
        g_cypher_allocation_failed = true;
    }
}

/* Allocate at least one zeroed element while preserving exact query-sized
 * cardinality. Runtime and memory are O(count); representational overflow and
 * OOM become one query-wide allocation failure rather than partial output. */
static void *cypher_calloc_elements(int count, size_t element_size) {
    size_t allocation_count = count > 0 ? (size_t)count : SKIP_ONE;
    if (count < 0 || element_size == 0 || allocation_count > SIZE_MAX / element_size) {
        g_cypher_allocation_failed = true;
        return NULL;
    }
    void *memory = calloc(allocation_count, element_size);
    if (!memory) {
        g_cypher_allocation_failed = true;
    }
    return memory;
}

/* Transfer an exact-size column vector to the result builder. Generated
 * column names already have result lifetime, so adopting them avoids an
 * otherwise redundant O(total_name_bytes) copy and peak allocation. On
 * success rb owns both the vector and every string in it. */
static bool rb_adopt_columns(result_builder_t *rb, const char **columns, int count) {
    if (!rb || !columns || count < 0 || rb->columns) {
        g_cypher_allocation_failed = true;
        return false;
    }
    rb->columns = columns;
    rb->col_count = count;
    return true;
}

/* Append one row, optionally transferring individual exact string owners into
 * the result. Transferred slots are set to NULL; untransferred values are
 * copied as before. This keeps ordinary borrowed projections simple while
 * avoiding a second O(value bytes) allocation for constructed aggregates. */
static void rb_add_row_sized_owned(result_builder_t *rb, const char **values, const size_t *lengths,
                                   char **owned_values) {
    if (g_cypher_allocation_failed || !rb || !values || rb->col_count < 0) {
        g_cypher_allocation_failed = true;
        return;
    }
    if (rb->row_count >= rb->row_cap) {
        if (rb->row_cap > INT_MAX / PAIR_LEN) {
            g_cypher_allocation_failed = true;
            return;
        }
        int next = rb->row_cap > 0 ? rb->row_cap * PAIR_LEN : CBM_SZ_32;
        if ((size_t)next > SIZE_MAX / sizeof(*rb->rows)) {
            g_cypher_allocation_failed = true;
            return;
        }
        void *grown = realloc(rb->rows, (size_t)next * sizeof(const char **));
        if (!grown) {
            g_cypher_allocation_failed = true;
            return;
        }
        rb->rows = grown;
        memset(rb->rows + rb->row_cap, 0, (size_t)(next - rb->row_cap) * sizeof(*rb->rows));
        rb->row_cap = next;
    }
    const char **row = cypher_calloc_elements(rb->col_count, sizeof(*row));
    if (!row) {
        return;
    }
    for (int i = 0; i < rb->col_count; i++) {
        const char *source = values[i] ? values[i] : "";
        size_t length = lengths ? lengths[i] : strlen(source);
        if (owned_values && owned_values[i]) {
            row[i] = owned_values[i];
            owned_values[i] = NULL;
            continue;
        }
        if (length > SIZE_MAX - SKIP_ONE) {
            g_cypher_allocation_failed = true;
            for (int j = 0; j < i; j++) {
                safe_str_free(&row[j]);
            }
            free(row);
            return;
        }
        char *copy = malloc(length + SKIP_ONE);
        if (copy) {
            memcpy(copy, source, length);
            copy[length] = '\0';
        }
        row[i] = copy;
        if (!row[i]) {
            for (int j = 0; j < i; j++) {
                safe_str_free(&row[j]);
            }
            free(row);
            g_cypher_allocation_failed = true;
            return;
        }
    }
    rb->rows[rb->row_count++] = row;
}

static void rb_add_row_sized(result_builder_t *rb, const char **values, const size_t *lengths) {
    rb_add_row_sized_owned(rb, values, lengths, NULL);
}

static void rb_add_row(result_builder_t *rb, const char **values) {
    rb_add_row_sized(rb, values, NULL);
}

/* Cypher UNION requires identical column counts and names. Validation is O(C)
 * once per branch for C columns, O(1) memory, and precedes every row copy. */
static bool rb_union_schema_matches(const result_builder_t *first, const result_builder_t *branch,
                                    int branch_number, char *error, size_t error_size) {
    if (!first || !branch || !error || error_size == 0) {
        return false;
    }
    if (first->col_count != branch->col_count) {
        snprintf(error, error_size,
                 "UNION branches must return identical column counts and names; first branch "
                 "has %d columns but branch %d has %d",
                 first->col_count, branch_number, branch->col_count);
        return false;
    }
    for (int column = 0; column < first->col_count; column++) {
        const char *first_name = first->columns ? first->columns[column] : NULL;
        const char *branch_name = branch->columns ? branch->columns[column] : NULL;
        if (!first_name || !branch_name || strcmp(first_name, branch_name) != 0) {
            snprintf(error, error_size,
                     "UNION branches must return identical column counts and names; column %d "
                     "is '%s' in the first branch but '%s' in branch %d",
                     column + SKIP_ONE, first_name ? first_name : "<missing>",
                     branch_name ? branch_name : "<missing>", branch_number);
            return false;
        }
    }
    return true;
}

/* ── Main execution ─────────────────────────────────────────────── */

/* Wall-clock execution deadline (#601). A working-row budget only fires once
 * rows exist, but an unbounded `OPTIONAL MATCH` over the full node set (or a
 * high-fanout OPTIONAL MATCH can run for minutes before a single row is
 * produced, so the ceiling never trips. Aggregate grouping formerly had the
 * same failure mode before its hash index removed the O(bindings x groups)
 * scan. The monotonic deadline remains a defense for genuinely expansive
 * queries and is checked (throttled) in scan, expansion, and aggregation. */
#define CYPHER_DEADLINE_BUDGET_MS 30000  /* 30s: generous for legit heavy queries */
#define CYPHER_DEADLINE_CHECK_MASK 0x3FF /* sample the clock every 1024 iterations */

static _Thread_local uint64_t g_cypher_deadline_ms = 0; /* absolute; 0 = disarmed */
static _Thread_local bool g_cypher_timed_out = false;
static _Thread_local int64_t g_cypher_deadline_override_ms = -1; /* test hook; <0 = default */
enum {
    CYP_AGG_ALLOC_NONE = 0,
    CYP_AGG_ALLOC_INITIAL,
    CYP_AGG_ALLOC_GROUP_ENTRY,
    CYP_AGG_ALLOC_GROUP_ARRAY_GROWTH,
    CYP_AGG_ALLOC_VALUE_ARRAY_GROWTH,
    CYP_AGG_ALLOC_VALUE_COPY,
    CYP_AGG_ALLOC_DISTINCT_INDEX,
};
#ifdef CBM_ENABLE_TEST_SEAMS
_Static_assert((int)CBM_CYPHER_TEST_AGG_ALLOC_DISTINCT_INDEX == CYP_AGG_ALLOC_DISTINCT_INDEX,
               "aggregate allocation seam values must match");
static _Thread_local bool g_cypher_force_whole_pattern_provider = false;
static _Thread_local int g_cypher_test_agg_alloc_site = CYP_AGG_ALLOC_NONE;
static _Thread_local int g_cypher_test_agg_alloc_successes_before_failure = -1;
static _Thread_local bool g_cypher_track_aggregate_list_growths = false;
static _Thread_local uint64_t g_cypher_aggregate_list_growths = 0;
static _Thread_local int g_cypher_test_label_alt_growths_before_failure = -1;
#endif
static _Thread_local bool g_cypher_track_group_lookup_probes = false;
static _Thread_local uint64_t g_cypher_group_lookup_probes = 0;
static _Thread_local bool g_cypher_track_aggregate_distinct_probes = false;
static _Thread_local uint64_t g_cypher_aggregate_distinct_probes = 0;
static _Thread_local bool g_cypher_track_row_distinct_probes = false;
static _Thread_local uint64_t g_cypher_row_distinct_probes = 0;
#ifdef CBM_ENABLE_TEST_SEAMS
static _Thread_local int g_cypher_row_distinct_key_copies_before_failure = -1;
#endif

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

#ifdef CBM_ENABLE_TEST_SEAMS
void cbm_cypher_test_force_whole_pattern_provider(bool force) {
    g_cypher_force_whole_pattern_provider = force;
}

void cbm_cypher_test_fail_aggregation_allocation(cbm_cypher_test_agg_alloc_site_t site,
                                                 int successful_before) {
    g_cypher_test_agg_alloc_site = (int)site;
    g_cypher_test_agg_alloc_successes_before_failure = successful_before;
}

void cbm_cypher_test_reset_aggregate_list_growths(void) {
    g_cypher_aggregate_list_growths = 0;
    g_cypher_track_aggregate_list_growths = true;
}

uint64_t cbm_cypher_test_aggregate_list_growths(void) {
    g_cypher_track_aggregate_list_growths = false;
    return g_cypher_aggregate_list_growths;
}

void cbm_cypher_test_fail_label_alternation_growth(int successful_before) {
    g_cypher_test_label_alt_growths_before_failure = successful_before;
}
#endif

static bool cypher_label_alt_growth_should_fail(void) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (g_cypher_test_label_alt_growths_before_failure < 0) {
        return false;
    }
    if (g_cypher_test_label_alt_growths_before_failure == 0) {
        g_cypher_test_label_alt_growths_before_failure = -1;
        return true;
    }
    g_cypher_test_label_alt_growths_before_failure--;
#endif
    return false;
}

static bool cypher_agg_allocation_should_fail(int site) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (g_cypher_test_agg_alloc_site != site ||
        g_cypher_test_agg_alloc_successes_before_failure < 0) {
        return false;
    }
    if (g_cypher_test_agg_alloc_successes_before_failure == 0) {
        g_cypher_test_agg_alloc_site = CYP_AGG_ALLOC_NONE;
        g_cypher_test_agg_alloc_successes_before_failure = -1;
        return true;
    }
    g_cypher_test_agg_alloc_successes_before_failure--;
#else
    (void)site;
#endif
    return false;
}

void cbm_cypher_test_reset_group_lookup_probes(void) {
    g_cypher_group_lookup_probes = 0;
    g_cypher_track_group_lookup_probes = true;
}

uint64_t cbm_cypher_test_group_lookup_probes(void) {
    g_cypher_track_group_lookup_probes = false;
    return g_cypher_group_lookup_probes;
}

void cbm_cypher_test_reset_aggregate_distinct_probes(void) {
    g_cypher_aggregate_distinct_probes = 0;
    g_cypher_track_aggregate_distinct_probes = true;
}

uint64_t cbm_cypher_test_aggregate_distinct_probes(void) {
    g_cypher_track_aggregate_distinct_probes = false;
    return g_cypher_aggregate_distinct_probes;
}

void cbm_cypher_test_reset_row_distinct_probes(void) {
    g_cypher_row_distinct_probes = 0;
    g_cypher_track_row_distinct_probes = true;
}

uint64_t cbm_cypher_test_row_distinct_probes(void) {
    g_cypher_track_row_distinct_probes = false;
    return g_cypher_row_distinct_probes;
}

#ifdef CBM_ENABLE_TEST_SEAMS
void cbm_cypher_test_fail_row_distinct_key_copy(int successful_before) {
    g_cypher_row_distinct_key_copies_before_failure = successful_before;
}
#endif

static bool row_distinct_key_copy_should_fail(void) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (g_cypher_row_distinct_key_copies_before_failure < 0) {
        return false;
    }
    if (g_cypher_row_distinct_key_copies_before_failure == 0) {
        g_cypher_row_distinct_key_copies_before_failure = -1;
        return true;
    }
    g_cypher_row_distinct_key_copies_before_failure--;
#endif
    return false;
}

/* Aggregate rows stay in first-seen order for deterministic output, while this
 * side index removes the quadratic scan needed to locate an existing group.
 * Keys are owned by the aggregate entries, not the borrowed-key hash table, so
 * growing the entry array cannot invalidate them. If allocation inside the
 * index fails, callers retain correctness by switching to the linear lookup. */
typedef struct {
    CBMHashTable *table;
    bool valid;
} aggregate_group_index_t;

static aggregate_group_index_t aggregate_group_index_create(void) {
    aggregate_group_index_t index = {.table = cbm_ht_create(CBM_SZ_256)};
    index.valid = index.table != NULL;
    return index;
}

static int aggregate_group_index_lookup(const aggregate_group_index_t *index, const char *key) {
    if (!index->valid) {
        return CYP_FOUND_NONE;
    }
    if (g_cypher_track_group_lookup_probes) {
        g_cypher_group_lookup_probes++;
    }
    void *encoded = cbm_ht_get(index->table, key);
    return encoded ? (int)((uintptr_t)encoded - 1u) : CYP_FOUND_NONE;
}

static void aggregate_group_index_insert(aggregate_group_index_t *index, const char *owned_key,
                                         int group_index) {
    if (!index->valid) {
        return;
    }
    void *encoded = (void *)(uintptr_t)(group_index + 1);
    (void)cbm_ht_set(index->table, owned_key, encoded);
    if (!cbm_ht_has(index->table, owned_key)) {
        index->valid = false;
    }
}

static void aggregate_group_index_free(aggregate_group_index_t *index) {
    cbm_ht_free(index->table);
    index->table = NULL;
    index->valid = false;
}

/* ── Binding virtual variables (for WITH clause) ──────────────── */

static void binding_get_virtual_value(binding_t *b, const char *var, const char *prop,
                                      cypher_value_t *value) {
    cypher_value_set_cstr(value, "", true);
    if (!var) {
        return;
    }
    /* COUNT(*) counts rows, so its synthetic argument is always non-null. */
    if (strcmp(var, "*") == 0 && !prop) {
        cypher_value_set_cstr(value, "", false);
        return;
    }
    /* Compare virtual names from WITH against canonical parts directly, so a
     * long variable.property identity is never formatted through scratch. */
    for (int i = 0; i < b->var_count; i++) {
        const char *bound_name = binding_node_name_at(b, i);
        const char *property_parts[] = {var, ".", prop ? prop : ""};
        bool matches =
            prop ? cypher_text_equals_parts(bound_name, property_parts,
                                            sizeof(property_parts) / sizeof(property_parts[0]))
                 : strcmp(bound_name, var) == 0;
        if (matches) {
            const cbm_node_t *node = binding_const_node_at(b, i);
            bool is_null = binding_node_is_null_at(b, i);
            cypher_value_set_cstr(value, node->name ? node->name : "", is_null);
            value->kind = is_null ? CYP_VALUE_NULL : binding_node_kind_at(b, i);
            return;
        }
    }
    /* Fall through to normal lookup */
    cbm_edge_t *e = binding_get_edge(b, var);
    if (e) {
        /* Bare `RETURN r` on an edge: surface the full properties JSON
         * (or "{}" if none) so callers can inspect timestamps, weights,
         * etc. without naming each property. */
        if (prop) {
            edge_prop_value(e, prop, value);
            return;
        }
        cypher_value_set_borrowed_kind(value, e->properties_json ? e->properties_json : "{}",
                                       strlen(e->properties_json ? e->properties_json : "{}"),
                                       CYP_VALUE_RELATIONSHIP);
        return;
    }
    cbm_node_t *n = binding_get(b, var);
    if (n) {
        if (prop) {
            node_prop_value(n, prop, b->store, b->project, b->use_active_overlay_edges, value);
            return;
        }
        cypher_value_set_borrowed_kind(value, n->name ? n->name : "",
                                       strlen(n->name ? n->name : ""), CYP_VALUE_NODE);
        return;
    }
}

static const char *binding_get_virtual_ex(binding_t *b, const char *var, const char *prop,
                                          bool *is_null) {
    cypher_value_t value;
    binding_get_virtual_value(b, var, prop, &value);
    *is_null = value.is_null;
    if (value.is_null) {
        cypher_value_free(&value);
        return "";
    }
    if (!value.owned && value.data != value.inline_text && value.data[value.length] == '\0') {
        return value.data;
    }
    static CBM_TLS char buffers[CYP_BUF_8][CBM_SZ_512];
    static CBM_TLS int buffer_index = 0;
    char *out = buffers[buffer_index++ & CYP_EBUF_MASK];
    size_t copy_length =
        value.length < CBM_SZ_512 - SKIP_ONE ? value.length : CBM_SZ_512 - SKIP_ONE;
    memcpy(out, value.data, copy_length);
    out[copy_length] = '\0';
    cypher_value_free(&value);
    return out;
}

static const char *binding_get_virtual(binding_t *b, const char *var, const char *prop) {
    bool is_null = true;
    return binding_get_virtual_ex(b, var, prop, &is_null);
}

/* Append one scalar grouping component. A length prefix prevents delimiters in
 * user data from merging distinct tuples. Runtime and added key storage are
 * O(V) for value bytes V, with O(1) auxiliary memory beyond the shared builder. */
static bool group_key_append_scalar_value(cypher_string_builder_t *key,
                                          const cypher_value_t *value) {
    if (value->is_null) {
        return cypher_string_builder_append(key, "Z|", sizeof("Z|") - SKIP_ONE);
    }
    char prefix[CBM_SZ_64];
    int written = snprintf(prefix, sizeof(prefix), "V:%zu:", value->length);
    if (written < 0 || (size_t)written >= sizeof(prefix) ||
        !cypher_string_builder_append(key, prefix, (size_t)written) ||
        !cypher_string_builder_append(key, value->data, value->length)) {
        return false;
    }
    return cypher_string_builder_append(key, "|", sizeof("|") - SKIP_ONE);
}

/* Append one binding-aware grouping component. Bare graph entities group by
 * canonical store identity rather than display text; other expressions reuse
 * the exact scalar encoding above. Identity lookup is O(B) for B live binding
 * slots and uses O(1) auxiliary memory. */
static bool group_key_append_value(cypher_string_builder_t *key, binding_t *binding,
                                   const char *var, bool preserve_entity_identity,
                                   const cypher_value_t *value) {
    if (!preserve_entity_identity) {
        return group_key_append_scalar_value(key, value);
    }
    if (!binding || !var) {
        return false;
    }
    char prefix[CBM_SZ_64];
    int written = 0;
    cbm_node_t *node = binding_get(binding, var);
    if (node && node->id > 0) {
        written = snprintf(prefix, sizeof(prefix), "N:%lld|", (long long)node->id);
    } else {
        cbm_edge_t *edge = binding_get_edge(binding, var);
        if (edge && edge->id > 0) {
            written = snprintf(prefix, sizeof(prefix), "E:%lld|", (long long)edge->id);
        }
    }
    if (written > 0) {
        return (size_t)written < sizeof(prefix) &&
               cypher_string_builder_append(key, prefix, (size_t)written);
    }
    return group_key_append_scalar_value(key, value);
}

/* ── String function application ──────────────────────────────── */

static bool cypher_parse_consumed_value(const cypher_value_t *value, const char *end) {
    if (!value || !end || end == value->data) {
        return false;
    }
    const char *limit = value->data + value->length;
    while (end < limit && isspace((unsigned char)*end)) {
        end++;
    }
    return end == limit;
}

/* yyjson's extended-number grammar deliberately rejects redundant leading
 * zeroes even though both merge parents accepted them through strto*. Remove
 * only zeroes followed by another decimal digit, retaining one zero before a
 * decimal point/exponent and preserving an optional sign. The value is already
 * an owned compatibility copy, so normalization is O(N) in-place with O(1)
 * auxiliary memory and cannot mutate a binding or stored property. */
static void cypher_value_normalize_leading_zeroes(cypher_value_t *value, const char **begin,
                                                  const char **limit) {
    char *number = (char *)*begin;
    char *digits = number;
    if (digits < *limit && (*digits == '+' || *digits == '-')) {
        digits++;
    }
    char *retained = digits;
    while (retained + SKIP_ONE < *limit && retained[0] == '0' &&
           isdigit((unsigned char)retained[SKIP_ONE])) {
        retained++;
    }
    if (retained == digits) {
        return;
    }
    size_t removed = (size_t)(retained - digits);
    memmove(digits, retained, (size_t)(*limit - retained) + SKIP_ONE);
    value->length -= removed;
    *limit -= removed;
}

/* Parse one complete numeric slice through yyjson's public standalone number
 * API. YYJSON_READ_ALLOW_EXT_NUMBER retains useful forms accepted by both
 * merge parents (`+42`, `.5`, `1.`); the shared normalization above retains
 * redundant leading zeroes without accepting a
 * numeric prefix such as `12junk`. The existing owned NUL-terminated copy is
 * needed only for borrowed query slices. Runtime is O(N); worst-case temporary
 * memory is O(N), while yyjson's fast converter itself does not allocate. */
static bool cypher_value_parse_number(cypher_value_t *value, yyjson_read_flag flags,
                                      yyjson_val *result) {
    if (!value || !result || value->is_null || !cypher_value_own(value)) {
        return false;
    }
    const char *begin = value->data;
    const char *limit = value->data + value->length;
    while (begin < limit && isspace((unsigned char)*begin)) {
        begin++;
    }
    if (begin == limit) {
        return false;
    }
    cypher_value_normalize_leading_zeroes(value, &begin, &limit);
    const char *end =
        yyjson_read_number(begin, result, flags | YYJSON_READ_ALLOW_EXT_NUMBER, NULL, NULL);
    return end && cypher_parse_consumed_value(value, end);
}

/* yyjson classifies parsed numbers as signed, unsigned, or correctly-rounded
 * double. Integer subtypes are range-checked exactly. For real values, the
 * type-derived exclusive +2^(bits-1) bound makes the subsequent cast defined;
 * -2^(bits-1) is inclusive. Runtime and auxiliary memory are O(1). */
static bool cypher_number_to_int64(const yyjson_val *number, int64_t *result) {
    if (!number || !result) {
        return false;
    }
    if (yyjson_is_sint(number)) {
        *result = yyjson_get_sint(number);
        return true;
    }
    if (yyjson_is_uint(number)) {
        uint64_t parsed = yyjson_get_uint(number);
        if (parsed > (uint64_t)INT64_MAX) {
            return false;
        }
        *result = (int64_t)parsed;
        return true;
    }
    if (!yyjson_is_real(number)) {
        return false;
    }
    double truncated = trunc(yyjson_get_real(number));
    const int magnitude_bits = (int)(sizeof(int64_t) * CHAR_BIT - SKIP_ONE);
    const double positive_limit = ldexp(1.0, magnitude_bits);
    if (!isfinite(truncated) || truncated < -positive_limit || truncated >= positive_limit) {
        return false;
    }
    *result = (int64_t)truncated;
    return true;
}

/* Apply the documented scalar conversions to one typed exact value. The
 * public result remains the established string representation; logical kind
 * exists only query-locally and survives WITH. Conversion is O(N) runtime for
 * N input bytes, O(N) worst-case compatibility memory, and O(1) retained
 * memory because every result fits cypher_value_t.inline_text. */
static void cypher_value_apply_numeric_bool_cast(const char *function, cypher_value_t *value) {
    if (!function || !value || value->is_null) {
        return;
    }
    cypher_value_kind_t input_kind = value->kind;
    if (strcmp(function, "toBoolean") == 0) {
        const char *boolean_text = NULL;
        if (input_kind == CYP_VALUE_BOOLEAN || input_kind == CYP_VALUE_STRING) {
            if (value->length == sizeof("true") - SKIP_ONE &&
                strncasecmp(value->data, "true", value->length) == 0) {
                boolean_text = "true";
            } else if (value->length == sizeof("false") - SKIP_ONE &&
                       strncasecmp(value->data, "false", value->length) == 0) {
                boolean_text = "false";
            }
        } else if (input_kind == CYP_VALUE_INTEGER) {
            yyjson_val number = {0};
            int64_t integer = 0;
            if (cypher_value_parse_number(value, YYJSON_READ_BIGNUM_AS_RAW, &number) &&
                cypher_number_to_int64(&number, &integer)) {
                boolean_text = integer == 0 ? "false" : "true";
            }
        }
        cypher_value_free(value);
        cypher_value_set_cstr(value, boolean_text ? boolean_text : "", boolean_text == NULL);
        if (boolean_text) {
            value->kind = CYP_VALUE_BOOLEAN;
        }
        return;
    }

    if (strcmp(function, "toInteger") == 0) {
        int64_t integer = 0;
        bool converted = false;
        if (input_kind == CYP_VALUE_BOOLEAN) {
            bool is_true = value->length == sizeof("true") - SKIP_ONE &&
                           strncasecmp(value->data, "true", value->length) == 0;
            bool is_false = value->length == sizeof("false") - SKIP_ONE &&
                            strncasecmp(value->data, "false", value->length) == 0;
            converted = is_true || is_false;
            integer = is_true ? 1 : 0;
        } else if (input_kind == CYP_VALUE_INTEGER || input_kind == CYP_VALUE_STRING ||
                   input_kind == CYP_VALUE_FLOAT) {
            yyjson_val number = {0};
            converted = cypher_value_parse_number(value, YYJSON_READ_BIGNUM_AS_RAW, &number) &&
                        cypher_number_to_int64(&number, &integer);
        }
        cypher_value_free(value);
        if (!converted) {
            cypher_value_set_cstr(value, "", true);
        } else {
            cypher_value_set_int64(value, integer);
        }
        return;
    }

    if (strcmp(function, "toFloat") == 0) {
        yyjson_val number = {0};
        bool converted = (input_kind == CYP_VALUE_INTEGER || input_kind == CYP_VALUE_FLOAT ||
                          input_kind == CYP_VALUE_STRING) &&
                         cypher_value_parse_number(value, YYJSON_READ_NOFLAG, &number);
        double floating = converted ? yyjson_get_num(&number) : 0.0;
        converted = converted && isfinite(floating);
        cypher_value_free(value);
        if (!converted) {
            cypher_value_set_cstr(value, "", true);
            return;
        }
        cypher_value_set_double(value, floating);
    }
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

static bool cypher_record_store_failure(cbm_store_t *store, const char *operation) {
    g_cypher_store_failed = true;
    snprintf(g_cypher_store_error, sizeof(g_cypher_store_error), "%s: %s", operation,
             cbm_store_error(store) ? cbm_store_error(store) : "store error");
    return false;
}

/* Seed nodes for a label alternation "A|B|C": union the per-label results.
 * Node-struct fields are moved (shallow) into out_nodes; each per-label array
 * container is freed. Geometric capacity makes total relocation work O(N)
 * for N returned nodes and retains O(N) node structs. Any failure releases
 * both the accumulated and current per-label ownership sets. */
static bool scan_alternation_labels(cbm_store_t *store, const char *project, const char *labels,
                                    int candidate_limit, cypher_node_scan_mode_t scan_mode,
                                    cbm_node_t **out_nodes, int *out_count) {
    if (!store || !project || !labels || !out_nodes || !out_count) {
        g_cypher_allocation_failed = true;
        return false;
    }
    *out_nodes = NULL;
    *out_count = 0;
    int cap = 0;
    char *copy = heap_strdup(labels);
    if (!copy) {
        g_cypher_allocation_failed = true;
        return false;
    }
    char *save = NULL;
    for (char *tok = strtok_r(copy, "|", &save); tok; tok = strtok_r(NULL, "|", &save)) {
        int remaining = candidate_limit > 0 ? candidate_limit - *out_count : 0;
        if (candidate_limit > 0 && remaining <= 0) {
            break;
        }
        cbm_node_t *part = NULL;
        int pc = 0;
        int rc = CBM_STORE_OK;
        if (scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY) {
            rc = cbm_store_find_nodes_by_label_overlay_view_limited(store, project, tok, remaining,
                                                                    &part, &pc);
        } else {
            rc = cbm_store_find_nodes_by_label_limited(store, project, tok, remaining, &part, &pc);
        }
        if (rc != CBM_STORE_OK || pc < 0) {
            cbm_store_free_nodes(part, pc > 0 ? pc : 0);
            cbm_store_free_nodes(*out_nodes, *out_count);
            *out_nodes = NULL;
            *out_count = 0;
            free(copy);
            return cypher_record_store_failure(store, "label alternation scan failed");
        }
        if (pc > 0 && !part) {
            cbm_store_free_nodes(*out_nodes, *out_count);
            *out_nodes = NULL;
            *out_count = 0;
            g_cypher_allocation_failed = true;
            free(copy);
            return false;
        }
        if (pc > 0 && part) {
            if (*out_count > INT_MAX - pc) {
                cbm_store_free_nodes(part, pc);
                cbm_store_free_nodes(*out_nodes, *out_count);
                *out_nodes = NULL;
                *out_count = 0;
                g_cypher_allocation_failed = true;
                free(copy);
                return false;
            }
            int required = *out_count + pc;
            if (required > cap) {
                int next_cap = cypher_geometric_capacity(cap, required);
                if ((size_t)next_cap > SIZE_MAX / sizeof(cbm_node_t) ||
                    cypher_label_alt_growth_should_fail()) {
                    cbm_store_free_nodes(part, pc);
                    cbm_store_free_nodes(*out_nodes, *out_count);
                    *out_nodes = NULL;
                    *out_count = 0;
                    g_cypher_allocation_failed = true;
                    free(copy);
                    return false;
                }
                cbm_node_t *grown = realloc(*out_nodes, (size_t)next_cap * sizeof(cbm_node_t));
                if (!grown) {
                    cbm_store_free_nodes(part, pc);
                    cbm_store_free_nodes(*out_nodes, *out_count);
                    *out_nodes = NULL;
                    *out_count = 0;
                    g_cypher_allocation_failed = true;
                    free(copy);
                    return false;
                }
                *out_nodes = grown;
                cap = next_cap;
            }
            memcpy(*out_nodes + *out_count, part, (size_t)pc * sizeof(cbm_node_t));
            *out_count += pc;
        }
        free(part); /* container only — node fields moved to out_nodes */
    }
    free(copy);
    return true;
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

static bool scan_pattern_nodes(cbm_store_t *store, const char *project, int candidate_limit,
                               int working_row_budget, cbm_node_pattern_t *first,
                               const cbm_where_clause_t *where, const char *variable,
                               cypher_node_scan_mode_t scan_mode, cbm_node_t **out_nodes,
                               int *out_count) {
    if (first->label && strchr(first->label, '|')) {
        if (!scan_alternation_labels(store, project, first->label, candidate_limit, scan_mode,
                                     out_nodes, out_count)) {
            return false;
        }
    } else if (first->label) {
        int rc = CBM_STORE_OK;
        if (scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY) {
            rc = cbm_store_find_nodes_by_label_overlay_view_limited(
                store, project, first->label, candidate_limit, out_nodes, out_count);
        } else {
            rc = cbm_store_find_nodes_by_label_limited(store, project, first->label,
                                                       candidate_limit, out_nodes, out_count);
        }
        if (rc != CBM_STORE_OK) {
            cbm_store_free_nodes(*out_nodes, *out_count);
            *out_nodes = NULL;
            *out_count = 0;
            return cypher_record_store_failure(store, "label scan failed");
        }
    } else if (scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY) {
        int rc = cbm_store_find_nodes_by_label_overlay_view_limited(
            store, project, NULL, candidate_limit, out_nodes, out_count);
        if (rc != CBM_STORE_OK) {
            cbm_store_free_nodes(*out_nodes, *out_count);
            *out_nodes = NULL;
            *out_count = 0;
            return cypher_record_store_failure(store, "active-node scan failed");
        }
    } else {
        const char *file_contains = where_file_contains_conjunct(where, variable);
        cbm_search_params_t params = {.project = project,
                                      .file_contains = file_contains,
                                      .min_degree = CYP_FOUND_NONE,
                                      .max_degree = CYP_FOUND_NONE,
                                      .limit = candidate_limit};
        cbm_search_output_t sout = {0};
        if (cbm_store_search(store, &params, &sout) != CBM_STORE_OK) {
            cbm_store_search_free(&sout);
            return cypher_record_store_failure(store, "node scan failed");
        }
        if (sout.count < 0 || (size_t)sout.count > SIZE_MAX / sizeof(cbm_node_t)) {
            cbm_store_search_free(&sout);
            g_cypher_allocation_failed = true;
            return false;
        }
        cbm_node_t *nodes = sout.count > 0 ? malloc((size_t)sout.count * sizeof(*nodes)) : NULL;
        if (sout.count > 0 && !nodes) {
            cbm_store_search_free(&sout);
            g_cypher_allocation_failed = true;
            return false;
        }
        for (int i = 0; i < sout.count; i++) {
            nodes[i] = sout.results[i].node;
            sout.results[i].node.name = NULL;
            sout.results[i].node.project = NULL;
            sout.results[i].node.label = NULL;
            sout.results[i].node.qualified_name = NULL;
            sout.results[i].node.file_path = NULL;
            sout.results[i].node.properties_json = NULL;
        }
        *out_nodes = nodes;
        *out_count = sout.count;
        cbm_store_search_free(&sout);
    }
    if (working_row_budget > 0 && *out_count > working_row_budget) {
        g_cypher_working_row_limit_hit = working_row_budget;
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
    return true;
}

/* ── Expand one pattern's relationships on a set of bindings ──── */

/* Process edges: look up target node, filter by label/props, add binding.
 * `inbound` controls which end of the edge is the target id. */
static void process_edges(cbm_store_t *store, cbm_edge_t *edges, int edge_count, bool inbound,
                          bool skip_self_loops, const cbm_node_pattern_t *target_node, binding_t *b,
                          const char *to_var, const char *rel_var, binding_t **new_bindings,
                          int *new_count, int *new_capacity, int max_new, int *match_count,
                          const cbm_where_clause_t *pattern_where) {
    /* When the terminal node variable is ALREADY bound (e.g. the second pattern
     * `(c)-[:CALLS]->(f)` where `f` came from an earlier MATCH), we must FILTER
     * to edges that actually reach the bound node — not overwrite the caller's
     * `f` binding with whatever node the edge leads to. Overwriting corrupted
     * the result of dead-code queries and produced wrong rows (#627). */
    cbm_node_t *bound_to = binding_get(b, to_var);
    int64_t bound_to_id = bound_to ? bound_to->id : 0;
    /* The budget caps MATERIALISATION, not detection: `match_count` must stay
     * truthful even after `new_count` hits `max_new`. Gating the loop itself
     * (`ei < edge_count && *new_count < max_new`) stopped fetching entirely, so
     * a saturated source reported `match_count == 0` despite having neighbours —
     * and the OPTIONAL fallback in expand_pattern_rels then emitted an unbound
     * "no match" row for it. `WHERE x IS NULL` reads those rows as "nothing
     * points here", i.e. it reported live code as dead. Losing rows is
     * recoverable; asserting a match does not exist when it does is not. */
    for (int ei = 0; ei < edge_count; ei++) {
        /* An undirected self-loop has only one orientation. The inbound half
         * of an ANY-direction lookup must not emit the same relationship twice. */
        if (skip_self_loops && edges[ei].source_id == edges[ei].target_id) {
            continue;
        }
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
        (*match_count)++; /* a real matching neighbour exists, budget or not */
        if (!binding_array_append(new_bindings, new_count, new_capacity, max_new, &nb)) {
            return;
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
    const char *bound_to_qn = bound_to && bound_to->qualified_name && bound_to->qualified_name[0]
                                  ? bound_to->qualified_name
                                  : NULL;
    int64_t bound_to_id = bound_to ? bound_to->id : 0;
    /* Keep match detection in the required O(row_count) scan with O(1)
     * auxiliary memory: materialisation may stop at max_new, but qualifying a
     * row must still make OPTIONAL aware that a match exists. */
    for (int ri = 0; ri < row_count; ri++) {
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
        (*match_count)++; /* qualifying match exists even if the row budget is full */
        if (!binding_array_append(new_bindings, new_count, new_capacity, max_new, &nb)) {
            return;
        }
    }
}

static bool cypher_trail_cancel(void *ctx) {
    (void)ctx;
    return cypher_deadline_exceeded();
}

/* Expand variable-length relationships as exact relationship-unique trails.
 * The adjacency snapshot is loaded once per pattern stage and reused across
 * source bindings. Explicit hop bounds are semantics; unbounded forms stop
 * naturally after every edge on a trail has been used. Work-budget, deadline,
 * allocation, and store failures abort the whole query rather than returning a
 * warning-only or otherwise incomplete answer. */
static void expand_var_length(cbm_store_t *store, cbm_store_trail_graph_t *trail_graph,
                              cbm_rel_pattern_t *rel, cbm_node_pattern_t *target_node, binding_t *b,
                              cbm_node_t *src, const char *to_var, binding_t **new_bindings,
                              int *new_count, int *new_capacity, int max_new, int *match_count,
                              const cbm_where_clause_t *pattern_where) {
    if (!trail_graph || g_cypher_store_failed || g_cypher_timed_out ||
        g_cypher_working_row_limit_hit > 0) {
        return;
    }

    int remaining_work = g_cypher_trail_work_limit - g_cypher_trail_work_rows;
    if (remaining_work <= 0) {
        g_cypher_working_row_limit_hit = g_cypher_trail_work_limit;
        return;
    }
    cbm_traverse_result_t tr = {0};
    int work_rows = 0;
    bool work_limit_hit = false;
    bool cancelled = false;
    int rc = cbm_store_trail_graph_traverse(
        trail_graph, src->id, src->qualified_name, rel->min_hops, rel->max_hops, remaining_work,
        cypher_trail_cancel, NULL, &tr, &work_rows, &work_limit_hit, &cancelled);
    g_cypher_trail_work_rows += work_rows;
    if (rc != CBM_STORE_OK) {
        g_cypher_store_failed = true;
        snprintf(g_cypher_store_error, sizeof(g_cypher_store_error), "%s",
                 cbm_store_error(store) ? cbm_store_error(store) : "graph traversal failed");
        cbm_store_traverse_free(&tr);
        return;
    }
    if (cancelled) {
        cbm_store_traverse_free(&tr);
        return;
    }
    if (work_limit_hit) {
        g_cypher_working_row_limit_hit = g_cypher_trail_work_limit;
        cbm_store_traverse_free(&tr);
        return;
    }
    /* A repeated variable unifies: when to_var is already bound, only the hop
     * that IS that node can extend the row. Without this a pattern such as
     * `MATCH (a)-[*1..3]->(b), (b)-[:CALLS]->(c)` re-binds b to every reachable
     * node and fabricates rows the query never asked for. Same rule, same
     * QN-then-id precedence as process_active_edge_nodes. */
    cbm_node_t *bound_to = binding_get(b, to_var);
    const char *bound_to_qn = bound_to && bound_to->qualified_name && bound_to->qualified_name[0]
                                  ? bound_to->qualified_name
                                  : NULL;
    int64_t bound_to_id = bound_to ? bound_to->id : 0;
    for (int v = 0; v < tr.visited_count; v++) {
        cbm_node_hop_t *hop = &tr.visited[v];
        if (bound_to_qn) {
            if (!hop->node.qualified_name || strcmp(bound_to_qn, hop->node.qualified_name) != 0) {
                continue;
            }
        } else if (bound_to && hop->node.id != bound_to_id) {
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
        /* Counting during the existing O(visited_count) result scan adds O(1)
         * work and memory per candidate while keeping OPTIONAL truth separate
         * from the bounded materialised-row array. */
        (*match_count)++;
        if (!binding_array_append(new_bindings, new_count, new_capacity, max_new, &nb)) {
            break;
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
        int direction = is_inbound
                            ? CBM_STORE_EDGE_DIR_INBOUND
                            : (is_any ? CBM_STORE_EDGE_DIR_ANY : CBM_STORE_EDGE_DIR_OUTBOUND);
        cbm_store_edge_node_t *rows = NULL;
        int row_count = 0;
        if (cbm_store_find_active_edge_nodes_by_qn(store, b->project, src->qualified_name,
                                                   (const char **)rel->types, rel->type_count,
                                                   direction, &rows, &row_count) == CBM_STORE_OK) {
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
            process_edges(store, edges, edge_count, is_inbound, false, target_node, b, to_var,
                          rel_var, new_bindings, new_count, new_capacity, max_new, match_count,
                          pattern_where);
            cbm_store_free_edges(edges, edge_count);
        }
        if (is_any) {
            for (int ti = 0; ti < rel->type_count; ti++) {
                cbm_edge_t *edges = NULL;
                int edge_count = 0;
                cbm_store_find_edges_by_target_type(store, src->id, rel->types[ti], &edges,
                                                    &edge_count);
                process_edges(store, edges, edge_count, true, true, target_node, b, to_var, rel_var,
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
        process_edges(store, edges, edge_count, is_inbound, false, target_node, b, to_var, rel_var,
                      new_bindings, new_count, new_capacity, max_new, match_count, pattern_where);
        cbm_store_free_edges(edges, edge_count);
        if (is_any) {
            edges = NULL;
            edge_count = 0;
            cbm_store_find_edges_by_target(store, src->id, &edges, &edge_count);
            process_edges(store, edges, edge_count, true, true, target_node, b, to_var, rel_var,
                          new_bindings, new_count, new_capacity, max_new, match_count,
                          pattern_where);
            cbm_store_free_edges(edges, edge_count);
        }
    }
}

typedef struct cypher_whole_match_ctx cypher_whole_match_ctx_t;

typedef struct {
    cypher_whole_match_ctx_t *match;
    binding_t *binding;
    int rel_index;
} cypher_segment_visit_ctx_t;

struct cypher_whole_match_ctx {
    cbm_store_t *store;
    cbm_pattern_t *pattern;
    cbm_store_trail_graph_t *graph;
    bool *used_edges;
    const cbm_where_clause_t *pattern_where;
    binding_t **outputs;
    int *output_count;
    int *output_capacity;
    int max_outputs;
    int complete_matches;
    bool work_limit_hit;
    bool cancelled;
};

static int cypher_match_pattern_segment(cypher_whole_match_ctx_t *ctx, int rel_index,
                                        binding_t *binding, const cbm_node_t *source);

static bool cypher_nodes_same_identity(const cbm_node_t *lhs, const cbm_node_t *rhs, bool overlay) {
    if (!lhs || !rhs) {
        return false;
    }
    if (overlay) {
        return lhs->qualified_name && rhs->qualified_name &&
               strcmp(lhs->qualified_name, rhs->qualified_name) == 0;
    }
    return lhs->id > 0 && lhs->id == rhs->id;
}

static bool cypher_edges_same_identity(const cbm_edge_t *lhs, const cbm_edge_t *rhs) {
    /* Canonical edges use positive SQLite ids; active-overlay whole-pattern
     * edges use stable negative query-local ids. Zero is never an identity.
     * Equality is O(1) and does not compare potentially large property JSON. */
    return lhs && rhs && lhs->id != 0 && lhs->id == rhs->id;
}

static int cypher_visit_segment_endpoint(const cbm_node_t *node, const cbm_edge_t *last_edge,
                                         void *userdata) {
    cypher_segment_visit_ctx_t *visit = userdata;
    cypher_whole_match_ctx_t *ctx = visit->match;
    cbm_rel_pattern_t *rel = &ctx->pattern->rels[visit->rel_index];
    cbm_node_pattern_t *target = &ctx->pattern->nodes[visit->rel_index + SKIP_ONE];
    const char *to_var = target->variable ? target->variable : "_n_t";
    cbm_edge_t *bound_rel = rel->variable ? binding_get_edge(visit->binding, rel->variable) : NULL;
    if (bound_rel && !cypher_edges_same_identity(bound_rel, last_edge)) {
        return CBM_STORE_OK;
    }
    cbm_node_t *bound_target = target->variable ? binding_get(visit->binding, to_var) : NULL;
    if (bound_target &&
        !cypher_nodes_same_identity(bound_target, node, visit->binding->use_active_overlay_edges)) {
        return CBM_STORE_OK;
    }
    if (target->label && !label_alt_matches(node->label, target->label)) {
        return CBM_STORE_OK;
    }
    if (!check_inline_props(node, target->props, target->prop_count, ctx->store)) {
        return CBM_STORE_OK;
    }

    binding_t next = {0};
    binding_copy(&next, visit->binding);
    binding_set(&next, to_var, node);
    if (rel->variable && last_edge) {
        binding_set_edge(&next, rel->variable, last_edge);
    }
    if (next.allocation_failed) {
        binding_free(&next);
        g_cypher_allocation_failed = true;
        return CBM_STORE_ERR;
    }

    int rc = CBM_STORE_OK;
    if (visit->rel_index + SKIP_ONE == ctx->pattern->rel_count) {
        if (!ctx->pattern_where || eval_where(ctx->pattern_where, &next)) {
            if (!binding_array_append(ctx->outputs, ctx->output_count, ctx->output_capacity,
                                      ctx->max_outputs, &next)) {
                rc = CBM_STORE_ERR;
            } else {
                ctx->complete_matches++;
            }
        }
    } else {
        cbm_node_t *next_source = binding_get(&next, to_var);
        rc = cypher_match_pattern_segment(ctx, visit->rel_index + SKIP_ONE, &next, next_source);
    }
    binding_free(&next);
    return rc;
}

static int cypher_match_pattern_segment(cypher_whole_match_ctx_t *ctx, int rel_index,
                                        binding_t *binding, const cbm_node_t *source) {
    if (!source || g_cypher_allocation_failed || g_cypher_working_row_limit_hit > 0 ||
        g_cypher_store_failed || g_cypher_timed_out) {
        return CBM_STORE_OK;
    }
    cbm_rel_pattern_t *rel = &ctx->pattern->rels[rel_index];
    cypher_segment_visit_ctx_t visit = {.match = ctx, .binding = binding, .rel_index = rel_index};
    int rc = cbm_store_trail_graph_visit(
        ctx->graph, source->id, source->qualified_name,
        rel->direction ? rel->direction : "outbound", (const char **)rel->types, rel->type_count,
        rel->min_hops, rel->max_hops, ctx->used_edges, g_cypher_trail_work_limit,
        &g_cypher_trail_work_rows, cypher_trail_cancel, NULL, cypher_visit_segment_endpoint, &visit,
        &ctx->work_limit_hit, &ctx->cancelled);
    if (ctx->work_limit_hit) {
        g_cypher_working_row_limit_hit = g_cypher_trail_work_limit;
    }
    if (rc != CBM_STORE_OK && !g_cypher_allocation_failed && g_cypher_working_row_limit_hit == 0) {
        g_cypher_store_failed = true;
        snprintf(g_cypher_store_error, sizeof(g_cypher_store_error), "%s",
                 cbm_store_error(ctx->store) ? cbm_store_error(ctx->store)
                                             : "whole-pattern traversal failed");
    }
    return rc;
}

static bool cypher_collect_pattern_edge_types(const cbm_pattern_t *pattern, const char ***out_types,
                                              int *out_count) {
    *out_types = NULL;
    *out_count = 0;
    int total = 0;
    for (int ri = 0; ri < pattern->rel_count; ri++) {
        const cbm_rel_pattern_t *rel = &pattern->rels[ri];
        if (rel->type_count == 0) {
            /* One untyped segment may consume every relationship type, so a
             * snapshot-level type predicate would be a semantic restriction. */
            return true;
        }
        if (rel->type_count > INT_MAX - total) {
            g_cypher_allocation_failed = true;
            return false;
        }
        total += rel->type_count;
    }
    if (total <= 0 || (size_t)total > SIZE_MAX / sizeof(**out_types)) {
        g_cypher_allocation_failed = true;
        return false;
    }
    const char **types = malloc((size_t)total * sizeof(*types));
    if (!types) {
        g_cypher_allocation_failed = true;
        return false;
    }
    int count = 0;
    for (int ri = 0; ri < pattern->rel_count; ri++) {
        const cbm_rel_pattern_t *rel = &pattern->rels[ri];
        for (int ti = 0; ti < rel->type_count; ti++) {
            types[count++] = rel->types[ti];
        }
    }
    *out_types = types;
    *out_count = count;
    return true;
}

static const char *cypher_pattern_snapshot_direction(const cbm_pattern_t *pattern) {
    const char *selected = NULL;
    for (int ri = 0; ri < pattern->rel_count; ri++) {
        const char *direction =
            pattern->rels[ri].direction ? pattern->rels[ri].direction : "outbound";
        if (strcmp(direction, "any") == 0) {
            return "any";
        }
        if (selected && strcmp(selected, direction) != 0) {
            return "any";
        }
        selected = direction;
    }
    return selected ? selected : "any";
}

/* Multi-segment graph patterns require one relationship-identity scope.
 * Descending into the next segment inside the trail visitor keeps one dense
 * used-edge bitmap live until the complete pattern succeeds or backtracks.
 * This avoids both staged semantic loss and O(partial_rows * path_length)
 * copied histories. Single-segment patterns retain the indexed adjacency fast
 * path below because graph-wide state cannot affect their result. */
static void expand_pattern_rels_whole(cbm_store_t *store, cbm_pattern_t *pat, binding_t **bindings,
                                      int *bind_count, const char **var_name, bool is_optional,
                                      const cbm_where_clause_t *pattern_where, int max_new) {
    int output_capacity = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
    if (output_capacity > max_new) {
        output_capacity = max_new;
    }
    binding_t *outputs = malloc((size_t)output_capacity * sizeof(*outputs));
    if (!outputs) {
        g_cypher_allocation_failed = true;
        return;
    }
    int output_count = 0;
    cbm_store_trail_graph_t *canonical_graph = NULL;
    cbm_store_trail_graph_t *overlay_graph = NULL;
    bool *canonical_used = NULL;
    bool *overlay_used = NULL;
    const char **pattern_edge_types = NULL;
    int pattern_edge_type_count = 0;
    if (!cypher_collect_pattern_edge_types(pat, &pattern_edge_types, &pattern_edge_type_count)) {
        free(outputs);
        return;
    }
    const char *snapshot_direction = cypher_pattern_snapshot_direction(pat);

    for (int bi = 0; bi < *bind_count; bi++) {
        binding_t *input = &(*bindings)[bi];
        cbm_node_t *source = binding_get(input, *var_name);
        if (!source) {
            continue;
        }
        bool overlay = input->use_active_overlay_edges && input->project &&
                       source->qualified_name && source->qualified_name[0];
        cbm_store_trail_graph_t **graph_slot = overlay ? &overlay_graph : &canonical_graph;
        bool **used_slot = overlay ? &overlay_used : &canonical_used;
        if (!*graph_slot) {
            int load_rc = overlay
                              ? cbm_store_trail_graph_load_overlay_view(
                                    store, input->project, snapshot_direction, pattern_edge_types,
                                    pattern_edge_type_count, graph_slot)
                              : cbm_store_trail_graph_load(store, input->project,
                                                           snapshot_direction, pattern_edge_types,
                                                           pattern_edge_type_count, graph_slot);
            if (load_rc != CBM_STORE_OK) {
                g_cypher_store_failed = true;
                snprintf(g_cypher_store_error, sizeof(g_cypher_store_error), "%s",
                         cbm_store_error(store) ? cbm_store_error(store)
                                                : "whole-pattern graph setup failed");
                break;
            }
            int edge_count = cbm_store_trail_graph_edge_count(*graph_slot);
            *used_slot = calloc((size_t)(edge_count > 0 ? edge_count : SKIP_ONE), sizeof(bool));
            if (!*used_slot) {
                g_cypher_allocation_failed = true;
                break;
            }
        }

        cypher_whole_match_ctx_t ctx = {.store = store,
                                        .pattern = pat,
                                        .graph = *graph_slot,
                                        .used_edges = *used_slot,
                                        .pattern_where = pattern_where,
                                        .outputs = &outputs,
                                        .output_count = &output_count,
                                        .output_capacity = &output_capacity,
                                        .max_outputs = max_new};
        (void)cypher_match_pattern_segment(&ctx, 0, input, source);
        if (is_optional && ctx.complete_matches == 0 && !g_cypher_allocation_failed &&
            !g_cypher_store_failed && g_cypher_working_row_limit_hit == 0 && !g_cypher_timed_out) {
            binding_t null_extended = {0};
            binding_copy(&null_extended, input);
            (void)binding_array_append(&outputs, &output_count, &output_capacity, max_new,
                                       &null_extended);
        }
        if (g_cypher_allocation_failed || g_cypher_store_failed ||
            g_cypher_working_row_limit_hit > 0 || g_cypher_timed_out) {
            break;
        }
    }

    free(canonical_used);
    free(overlay_used);
    free(pattern_edge_types);
    cbm_store_trail_graph_free(canonical_graph);
    cbm_store_trail_graph_free(overlay_graph);
    for (int bi = 0; bi < *bind_count; bi++) {
        binding_free(&(*bindings)[bi]);
    }
    free(*bindings);
    *bindings = outputs;
    *bind_count = output_count;
    cbm_node_pattern_t *last_node = &pat->nodes[pat->rel_count];
    *var_name = last_node->variable ? last_node->variable : "_n_t";
}

static void expand_pattern_rels(cbm_store_t *store, cbm_pattern_t *pat, binding_t **bindings,
                                int *bind_count, const char **var_name, bool is_optional,
                                const cbm_where_clause_t *pattern_where, int max_new) {
    bool use_whole_pattern_provider = pat->rel_count > SKIP_ONE;
#ifdef CBM_ENABLE_TEST_SEAMS
    use_whole_pattern_provider =
        use_whole_pattern_provider || g_cypher_force_whole_pattern_provider;
#endif
    if (use_whole_pattern_provider) {
        expand_pattern_rels_whole(store, pat, bindings, bind_count, var_name, is_optional,
                                  pattern_where, max_new);
        return;
    }
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
        bool is_empty_range =
            rel->max_hops != CBM_CYPHER_HOPS_UNBOUNDED && rel->min_hops > rel->max_hops;
        cbm_store_trail_graph_t *canonical_trails = NULL;
        cbm_store_trail_graph_t *overlay_trails = NULL;

        /* Start at O(min(bind_count, max_new)) slots and grow geometrically.
         * This preserves O(output_rows) memory and amortized O(1) append work;
         * preallocating max_new + bind_count for every hop increases peak
         * memory and duplicates binding_array_append's checked ownership path. */
        int new_capacity = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
        if (new_capacity > max_new) {
            new_capacity = max_new;
        }
        binding_t *new_bindings = malloc((size_t)new_capacity * sizeof(binding_t));
        if (!new_bindings) {
            g_cypher_allocation_failed = true;
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

            if (is_empty_range) {
                /* Reversed intervals are valid and have an empty match domain.
                 * OPTIONAL handling below still null-extends the input row. */
            } else if (is_variable_length) {
                bool use_overlay = b->use_active_overlay_edges && b->project &&
                                   src->qualified_name && src->qualified_name[0];
                cbm_store_trail_graph_t **trail_slot =
                    use_overlay ? &overlay_trails : &canonical_trails;
                if (!*trail_slot) {
                    const char *direction = rel->direction ? rel->direction : "outbound";
                    int load_rc =
                        use_overlay
                            ? cbm_store_trail_graph_load_overlay_view(store, b->project, direction,
                                                                      (const char **)rel->types,
                                                                      rel->type_count, trail_slot)
                            : cbm_store_trail_graph_load(store, b->project, direction, rel->types,
                                                         rel->type_count, trail_slot);
                    if (load_rc != CBM_STORE_OK) {
                        g_cypher_store_failed = true;
                        snprintf(g_cypher_store_error, sizeof(g_cypher_store_error), "%s",
                                 cbm_store_error(store) ? cbm_store_error(store)
                                                        : "graph traversal setup failed");
                        break;
                    }
                }
                expand_var_length(store, *trail_slot, rel, target_node, b, src, to_var,
                                  &new_bindings, &new_count, &new_capacity, max_new, &match_count,
                                  candidate_where);
            } else {
                expand_fixed_length(store, rel, target_node, b, src, to_var, &new_bindings,
                                    &new_count, &new_capacity, max_new, &match_count,
                                    candidate_where);
            }

            /* OPTIONAL MATCH: no qualifying relationship exists for this source,
             * so keep one null-extended binding. The shared checked append path
             * grows geometrically and turns budget/OOM failures into query errors;
             * it never returns a silently shortened result. */
            if (is_optional && match_count == 0) {
                binding_t nb = {0};
                binding_copy(&nb, b);
                /* Don't set to_var — it remains unbound; projection returns "" */
                (void)binding_array_append(&new_bindings, &new_count, &new_capacity, max_new, &nb);
            }
        }
        cbm_store_trail_graph_free(canonical_trails);
        cbm_store_trail_graph_free(overlay_trails);

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

/* Stable merge sort over R rows and K keys: O(K * R log R) worst-case
 * comparisons, O(K * R) numeric classification, and O(R + K) scratch. */
static void rb_apply_order_by(result_builder_t *rb, const cbm_return_clause_t *ret) {
    if (ret->order_count <= 0 || rb->row_count < PAIR_LEN) {
        return;
    }
    int *columns = cypher_calloc_elements(ret->order_count, sizeof(*columns));
    bool *numeric = cypher_calloc_elements(ret->order_count, sizeof(*numeric));
    bool *descending = cypher_calloc_elements(ret->order_count, sizeof(*descending));
    if (!columns || !numeric || !descending) {
        free(columns);
        free(numeric);
        free(descending);
        return;
    }
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
        free(columns);
        free(numeric);
        free(descending);
        return;
    }

    const char ***scratch = cypher_calloc_elements(rb->row_count, sizeof(*scratch));
    if (!scratch) {
        free(columns);
        free(numeric);
        free(descending);
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
    free(columns);
    free(numeric);
    free(descending);
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

/* Stable first-seen row DISTINCT. Length-prefixed column values make the hash
 * key injective without delimiter assumptions. Classifying all R rows before
 * mutating owners makes allocation failure atomic. For total projected bytes
 * B and unique encoded bytes U, expected runtime is O(B), temporary memory is
 * O(U + R), and final row compaction is O(R). */
static bool rb_apply_distinct(result_builder_t *rb) {
    if (!rb || rb->row_count < 0 || rb->col_count < 0 || (rb->row_count > 0 && !rb->rows)) {
        g_cypher_allocation_failed = true;
        return false;
    }
    if (rb->row_count <= SKIP_ONE) {
        return true;
    }

    int original_row_count = rb->row_count;
    CBMHashTable *seen = cbm_ht_create(0);
    bool *duplicates = cypher_calloc_elements(original_row_count, sizeof(*duplicates));
    char **owned_keys = cypher_calloc_elements(original_row_count, sizeof(*owned_keys));
    cypher_string_builder_t key = {0};
    bool complete = seen && duplicates && owned_keys && cypher_string_builder_reset(&key);
    for (int row_index = 0; complete && row_index < original_row_count; row_index++) {
        const char **row = rb->rows[row_index];
        if (!row) {
            complete = false;
            break;
        }
        complete = cypher_string_builder_reset(&key);
        for (int column = 0; complete && column < rb->col_count; column++) {
            if (!row[column]) {
                complete = false;
                break;
            }
            cypher_value_t value;
            cypher_value_set_cstr(&value, row[column], false);
            complete = group_key_append_scalar_value(&key, &value);
        }
        if (!complete) {
            break;
        }
        if (g_cypher_track_row_distinct_probes) {
            g_cypher_row_distinct_probes++;
        }
        if (cbm_ht_has(seen, key.data)) {
            duplicates[row_index] = true;
            continue;
        }
        owned_keys[row_index] =
            row_distinct_key_copy_should_fail() ? NULL : malloc(key.length + SKIP_ONE);
        if (!owned_keys[row_index]) {
            complete = false;
            break;
        }
        memcpy(owned_keys[row_index], key.data, key.length + SKIP_ONE);
        (void)cbm_ht_set(seen, owned_keys[row_index], (void *)(uintptr_t)SKIP_ONE);
        if (g_cypher_track_row_distinct_probes) {
            g_cypher_row_distinct_probes++;
        }
        if (!cbm_ht_has(seen, owned_keys[row_index])) {
            complete = false;
            break;
        }
    }

    if (complete) {
        int kept = 0;
        for (int row_index = 0; row_index < original_row_count; row_index++) {
            const char **row = rb->rows[row_index];
            if (!duplicates[row_index]) {
                rb->rows[kept++] = row;
                continue;
            }
            for (int column = 0; column < rb->col_count; column++) {
                safe_str_free(&row[column]);
            }
            free(row);
        }
        rb->row_count = kept;
    } else {
        g_cypher_allocation_failed = true;
    }

    cbm_ht_free(seen);
    for (int row_index = 0; owned_keys && row_index < original_row_count; row_index++) {
        free(owned_keys[row_index]);
    }
    cypher_string_builder_free(&key);
    free(owned_keys);
    free(duplicates);
    return complete;
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

static bool cypher_keys_append(cypher_string_builder_t *builder, CBMHashTable *seen,
                               const char *key, size_t key_length, bool *first) {
    if (seen) {
        if (cbm_ht_has(seen, key)) {
            return true;
        }
        (void)cbm_ht_set(seen, key, (void *)(uintptr_t)SKIP_ONE);
        if (!cbm_ht_has(seen, key)) {
            g_cypher_allocation_failed = true;
            return false;
        }
    }
    if ((!*first && !cypher_string_builder_append(builder, ",", sizeof(",") - SKIP_ONE)) ||
        !cypher_string_builder_append_json_string(builder, key, key_length)) {
        return false;
    }
    *first = false;
    return true;
}

/* Build an exact JSON list of a node's non-null canonical and dynamic property
 * keys. yyjson decodes each stored key once; the shared expected-O(1) hash
 * table removes canonical/dynamic or duplicate-JSON collisions without a
 * quadratic scan. For JSON bytes J, serialized key bytes K, and distinct key
 * bytes P, runtime is O(J + K) expected and peak memory is O(J + K + P).
 * The common empty-object case skips both the JSON document and hash table,
 * retaining only the O(K) exact result builder. */
static bool node_keys_value(const cbm_node_t *node, cypher_value_t *value) {
    cypher_value_set_cstr(value, "[]", false);
    value->kind = CYP_VALUE_COMPOSITE;
    if (!node) {
        return true;
    }
    const struct {
        const char *key;
        bool present;
    } core_keys[] = {
        {"name", node->name && node->name[0]},
        {"qualified_name", node->qualified_name && node->qualified_name[0]},
        {"label", node->label && node->label[0]},
        {"file_path", node->file_path && node->file_path[0]},
        {"start_line", node->start_line > 0},
        {"end_line", node->end_line > 0},
    };
    yyjson_doc *document = NULL;
    const char *json = node->properties_json;
    bool complete = true;
    if (json && json[0] && !cypher_json_is_empty_object(json)) {
        yyjson_read_err error = {0};
        document = yyjson_read_opts((char *)json, strlen(json), 0, NULL, &error);
        if (!document) {
            if (error.code == YYJSON_READ_ERROR_MEMORY_ALLOCATION) {
                g_cypher_allocation_failed = true;
            } else {
                g_cypher_store_failed = true;
                snprintf(g_cypher_store_error, sizeof(g_cypher_store_error),
                         "node %lld has invalid properties JSON at byte %zu", (long long)node->id,
                         error.pos);
            }
            complete = false;
        }
    }
    yyjson_val *object = document ? yyjson_doc_get_root(document) : NULL;
    if (complete && document && !yyjson_is_obj(object)) {
        g_cypher_store_failed = true;
        snprintf(g_cypher_store_error, sizeof(g_cypher_store_error),
                 "node %lld properties JSON is not an object", (long long)node->id);
        complete = false;
    }
    cypher_string_builder_t builder = {0};
    CBMHashTable *seen = complete && document ? cbm_ht_create(0) : NULL;
    complete = complete && (!document || seen) && cypher_string_builder_reset(&builder) &&
               cypher_string_builder_append(&builder, "[", sizeof("[") - SKIP_ONE);
    bool first = true;
    for (size_t i = 0; complete && i < sizeof(core_keys) / sizeof(core_keys[0]); i++) {
        if (core_keys[i].present) {
            complete = cypher_keys_append(&builder, seen, core_keys[i].key,
                                          strlen(core_keys[i].key), &first);
        }
    }
    if (complete && object) {
        size_t index = 0;
        size_t maximum = 0;
        yyjson_val *key = NULL;
        yyjson_val *member = NULL;
        yyjson_obj_foreach(object, index, maximum, key, member) {
            if (yyjson_is_null(member)) {
                continue;
            }
            const char *key_text = yyjson_get_str(key);
            size_t key_length = yyjson_get_len(key);
            if (!key_text || strlen(key_text) != key_length) {
                g_cypher_store_failed = true;
                snprintf(g_cypher_store_error, sizeof(g_cypher_store_error),
                         "node %lld has a property key containing NUL", (long long)node->id);
                complete = false;
                break;
            }
            if (!cypher_keys_append(&builder, seen, key_text, key_length, &first)) {
                complete = false;
                break;
            }
        }
    }
    if (complete) {
        complete = cypher_string_builder_append(&builder, "]", sizeof("]") - SKIP_ONE);
    }
    cbm_ht_free(seen);
    yyjson_doc_free(document);
    if (!complete) {
        if (!g_cypher_store_failed) {
            g_cypher_allocation_failed = true;
        }
        cypher_string_builder_free(&builder);
        return false;
    }
    memset(value, 0, sizeof(*value));
    value->data = builder.data;
    value->length = builder.length;
    value->owned = builder.data;
    value->kind = CYP_VALUE_COMPOSITE;
    builder.data = NULL;
    cypher_string_builder_free(&builder);
    return true;
}

/* Serialize a zero- or one-element JSON string list without a semantic size
 * cap. Runtime and retained memory are O(N) for N input bytes; the geometric
 * builder owns the only output allocation and transfers it atomically. */
static bool cypher_value_set_single_json_string_list(const char *text, cypher_value_t *value) {
    cypher_value_set_cstr(value, "[]", false);
    value->kind = CYP_VALUE_COMPOSITE;
    if (!text) {
        return true;
    }
    cypher_string_builder_t builder = {0};
    bool complete = cypher_string_builder_reset(&builder) &&
                    cypher_string_builder_append(&builder, "[", sizeof("[") - SKIP_ONE) &&
                    cypher_string_builder_append_json_string(&builder, text, strlen(text)) &&
                    cypher_string_builder_append(&builder, "]", sizeof("]") - SKIP_ONE);
    if (!complete) {
        g_cypher_allocation_failed = true;
        cypher_string_builder_free(&builder);
        return false;
    }
    memset(value, 0, sizeof(*value));
    value->data = builder.data;
    value->length = builder.length;
    value->owned = builder.data;
    value->kind = CYP_VALUE_COMPOSITE;
    builder.data = NULL;
    cypher_string_builder_free(&builder);
    return true;
}

/* Resolve one literal or var.property argument without a compatibility buffer. */
static void eval_func_arg_value(binding_t *binding, const cbm_func_arg_t *argument,
                                cypher_value_t *value) {
    if (argument->literal) {
        cypher_value_set_cstr(value, argument->literal, false);
    } else {
        binding_get_virtual_value(binding, argument->variable, argument->property, value);
    }
}

/* strtol() requires a NUL-terminated input. Materialize only the numeric
 * argument being parsed, then release it immediately. */
static long eval_func_arg_long(binding_t *binding, const cbm_func_arg_t *argument) {
    cypher_value_t value;
    eval_func_arg_value(binding, argument, &value);
    if (!cypher_value_own(&value)) {
        cypher_value_free(&value);
        return 0;
    }
    long result = strtol(value.data, NULL, CBM_DECIMAL_BASE);
    cypher_value_free(&value);
    return result;
}

static size_t cypher_nonnegative_long_clamp(long value, size_t maximum) {
    if (value <= 0) {
        return 0;
    }
    return (uintmax_t)value >= (uintmax_t)maximum ? maximum : (size_t)value;
}

/* Evaluate the accepted multi-argument scalar functions into one exact value.
 * substring/left/right retain O(1) slice state over their source. replace uses
 * shared KMP matching, so source S, pattern P, and output O cost O(S + P + O)
 * runtime, O(P + O) peak auxiliary/result memory, and no semantic byte cap.
 * Allocation failure sets the query-wide failure state and publishes no row. */
static void eval_multiarg_value(binding_t *binding, const cbm_return_item_t *item,
                                cypher_value_t *value) {
    cypher_value_set_cstr(value, "", false);
    const char *function = item->func;
    int argument_count = item->arg_count;
    if (strcmp(function, "coalesce") == 0) {
        for (int i = 0; i < argument_count; i++) {
            eval_func_arg_value(binding, &item->args[i], value);
            if (!value->is_null) {
                return;
            }
            cypher_value_free(value);
        }
        cypher_value_set_cstr(value, "", true);
        return;
    }
    if (strcmp(function, "substring") == 0 && argument_count >= 2) {
        eval_func_arg_value(binding, &item->args[0], value);
        value->is_null = false; /* preserve the established scalar null behavior */
        long start = eval_func_arg_long(binding, &item->args[1]);
        if (g_cypher_allocation_failed || start < 0 || (uintmax_t)start >= value->length) {
            cypher_value_free(value);
            cypher_value_set_cstr(value, "", false);
            return;
        }
        size_t offset = (size_t)start;
        size_t take = value->length - offset;
        if (argument_count >= 3) {
            take = cypher_nonnegative_long_clamp(eval_func_arg_long(binding, &item->args[2]), take);
        }
        value->data += offset;
        value->length = take;
        return;
    }
    if ((strcmp(function, "left") == 0 || strcmp(function, "right") == 0) && argument_count >= 2) {
        eval_func_arg_value(binding, &item->args[0], value);
        value->is_null = false;
        size_t take = cypher_nonnegative_long_clamp(eval_func_arg_long(binding, &item->args[1]),
                                                    value->length);
        if (strcmp(function, "right") == 0) {
            value->data += value->length - take;
        }
        value->length = take;
        return;
    }
    if (strcmp(function, "replace") == 0 && argument_count >= 3) {
        cypher_value_t source;
        cypher_value_t pattern;
        cypher_value_t replacement;
        eval_func_arg_value(binding, &item->args[0], &source);
        eval_func_arg_value(binding, &item->args[1], &pattern);
        eval_func_arg_value(binding, &item->args[2], &replacement);
        source.is_null = false;
        pattern.is_null = false;
        replacement.is_null = false;
        if (pattern.length == 0 || source.length == 0 || pattern.length > source.length) {
            cypher_value_move(value, &source);
            cypher_value_free(&pattern);
            cypher_value_free(&replacement);
            return;
        }

        size_t *prefix = cypher_kmp_prefix_create(pattern.data, pattern.length);
        cypher_string_builder_t output = {0};
        bool complete = prefix != NULL;
        size_t last_emit = 0;
        for (size_t i = 0, matched = 0; complete && i < source.length; i++) {
            while (matched > 0 && source.data[i] != pattern.data[matched]) {
                matched = prefix[matched - SKIP_ONE];
            }
            if (source.data[i] == pattern.data[matched]) {
                matched++;
            }
            if (matched == pattern.length) {
                size_t match_start = i + SKIP_ONE - pattern.length;
                complete =
                    (output.data || cypher_string_builder_reset(&output)) &&
                    cypher_string_builder_append(&output, source.data + last_emit,
                                                 match_start - last_emit) &&
                    cypher_string_builder_append(&output, replacement.data, replacement.length);
                last_emit = i + SKIP_ONE;
                matched = 0; /* Cypher replace consumes non-overlapping matches. */
            }
        }
        if (complete && last_emit == 0) {
            free(prefix);
            cypher_value_move(value, &source);
            cypher_value_free(&pattern);
            cypher_value_free(&replacement);
            return;
        }
        if (complete) {
            complete = cypher_string_builder_append(&output, source.data + last_emit,
                                                    source.length - last_emit);
        }
        free(prefix);
        cypher_value_free(&source);
        cypher_value_free(&pattern);
        cypher_value_free(&replacement);
        if (complete) {
            value->owned = output.data;
            value->data = output.data;
            value->length = output.length;
            value->is_null = false;
            output.data = NULL;
        }
        cypher_string_builder_free(&output);
        return;
    }
    /* Preserve the existing wrong-arity/unknown-function empty scalar. */
    return;
}

static const char *project_item(binding_t *b, cbm_return_item_t *item, char *func_buf,
                                size_t buf_sz) {
    if (!b || !item || !func_buf || buf_sz == 0) {
        g_cypher_allocation_failed = true;
        return "";
    }
    if (item->kase) {
        return eval_case_expr(item->kase, b);
    }
    const char *raw = binding_get_virtual(b, item->variable, item->property);
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

/* Project exact multi-argument, direct, and string-scalar values. String input
 * bytes N cost O(N) runtime for case conversion, trimming, and reversal;
 * size/length and toString are O(1). Trimming retains an O(1) borrowed slice.
 * Case conversion and reversal own exactly O(N) bytes only when mutation is
 * required. Returns false only when the item still requires entity or CASE
 * evaluation. */
static bool project_item_exact_value(binding_t *binding, cbm_return_item_t *item,
                                     cypher_value_t *value) {
    if (item->args) {
        eval_multiarg_value(binding, item, value);
        return true;
    }
    if (item->func && strcmp(item->func, "labels") == 0) {
        cbm_node_t *node = binding_get(binding, item->variable);
        (void)cypher_value_set_single_json_string_list(node ? node->label : NULL, value);
        return true;
    }
    if (item->func && strcmp(item->func, "keys") == 0) {
        (void)node_keys_value(binding_get(binding, item->variable), value);
        return true;
    }
    if (item->func && strcmp(item->func, "type") == 0) {
        cbm_edge_t *edge = binding_get_edge(binding, item->variable);
        cypher_value_set_cstr(value, edge && edge->type ? edge->type : "", edge == NULL);
        return true;
    }
    if (item->func && strcmp(item->func, "id") == 0) {
        cbm_node_t *node = binding_get(binding, item->variable);
        cbm_edge_t *edge = node ? NULL : binding_get_edge(binding, item->variable);
        if (!node && !edge) {
            cypher_value_set_cstr(value, "", true);
        } else {
            cypher_value_set_int64(value, node ? node->id : edge->id);
        }
        return true;
    }
    if (item->func && strcmp(item->func, "properties") == 0) {
        cbm_node_t *node = binding_get(binding, item->variable);
        cbm_edge_t *edge = node ? NULL : binding_get_edge(binding, item->variable);
        const char *json = node ? node->properties_json : (edge ? edge->properties_json : NULL);
        cypher_value_set_borrowed_kind(value, json ? json : "{}", strlen(json ? json : "{}"),
                                       CYP_VALUE_COMPOSITE);
        return true;
    }
    if (is_numeric_bool_value_func(item->func)) {
        binding_get_virtual_value(binding, item->variable, item->property, value);
        cypher_value_apply_numeric_bool_cast(item->func, value);
        return true;
    }
    bool direct = !item->func && !item->kase && !item->args;
    bool exact_string_func = !item->args && is_exact_string_value_func(item->func);
    if (!direct && !exact_string_func) {
        return false;
    }
    binding_get_virtual_value(binding, item->variable, item->property, value);
    if (direct) {
        return true;
    }
    if (strcmp(item->func, "toString") == 0) {
        value->kind = value->is_null ? CYP_VALUE_NULL : CYP_VALUE_STRING;
        return true;
    }
    if (value->is_null &&
        (strcmp(item->func, "toLower") == 0 || strcmp(item->func, "toUpper") == 0)) {
        return true;
    }

    if (strcmp(item->func, "size") == 0 || strcmp(item->func, "length") == 0) {
        size_t length = value->length;
        cypher_value_free(value);
        int written = snprintf(value->inline_text, sizeof(value->inline_text), "%zu", length);
        if (written < 0 || (size_t)written >= sizeof(value->inline_text)) {
            g_cypher_allocation_failed = true;
            cypher_value_set_cstr(value, "", true);
            return true;
        }
        value->data = value->inline_text;
        value->length = (size_t)written;
        value->kind = CYP_VALUE_INTEGER;
        return true;
    }

    value->is_null = false; /* preserve established empty-input scalar behavior */
    value->kind = CYP_VALUE_STRING;
    if (strcmp(item->func, "trim") == 0 || strcmp(item->func, "ltrim") == 0 ||
        strcmp(item->func, "rtrim") == 0) {
        bool trim_left = strcmp(item->func, "rtrim") != 0;
        bool trim_right = strcmp(item->func, "ltrim") != 0;
        size_t begin = 0;
        size_t end = value->length;
        while (trim_left && begin < end &&
               (value->data[begin] == ' ' || value->data[begin] == '\t' ||
                value->data[begin] == '\n' || value->data[begin] == '\r')) {
            begin++;
        }
        while (trim_right && end > begin &&
               (value->data[end - SKIP_ONE] == ' ' || value->data[end - SKIP_ONE] == '\t' ||
                value->data[end - SKIP_ONE] == '\n' || value->data[end - SKIP_ONE] == '\r')) {
            end--;
        }
        value->data += begin;
        value->length = end - begin;
        return true;
    }

    if (!cypher_value_own(value)) {
        return true;
    }
    if (strcmp(item->func, "reverse") == 0) {
        size_t left = 0;
        size_t right = value->length;
        while (left < right) {
            right--;
            if (left >= right) {
                break;
            }
            char byte = value->owned[left];
            value->owned[left] = value->owned[right];
            value->owned[right] = byte;
            left++;
        }
    } else {
        bool lower = strcmp(item->func, "toLower") == 0;
        for (size_t i = 0; i < value->length; i++) {
            value->owned[i] = lower ? (char)tolower((unsigned char)value->owned[i])
                                    : (char)toupper((unsigned char)value->owned[i]);
        }
    }
    return true;
}

/* Check if a function name is an aggregate */
static bool is_aggregate_func(const char *func) {
    if (!func) {
        return false;
    }
#define CYPHER_CANONICAL_MATCH(schema_name, canonical_name, token) \
    if (strcmp(func, canonical_name) == 0) {                       \
        return true;                                               \
    }
    CYPHER_AGGREGATE_FUNCTIONS(CYPHER_CANONICAL_MATCH)
#undef CYPHER_CANONICAL_MATCH
    return false;
}

static void *cypher_agg_malloc(size_t size, int site) {
    return cypher_agg_allocation_should_fail(site) ? NULL : malloc(size);
}

static void *cypher_agg_calloc(size_t count, size_t size, int site) {
    return cypher_agg_allocation_should_fail(site) ? NULL : calloc(count, size);
}

/* Raw realloc retains the caller's owner on failure. Callers publish the
 * returned pointer only after success, so an injected or real OOM cannot lose
 * the existing aggregate array. */
static void *cypher_agg_realloc(void *memory, size_t size, int site) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (site == CYP_AGG_ALLOC_VALUE_ARRAY_GROWTH && g_cypher_track_aggregate_list_growths) {
        g_cypher_aggregate_list_growths++;
    }
#endif
    return cypher_agg_allocation_should_fail(site) ? NULL : realloc(memory, size);
}

static char *cypher_agg_strdup(const char *value, int site) {
    return cypher_agg_allocation_should_fail(site) ? NULL : heap_strdup(value);
}

static char *cypher_agg_strndup(const char *value, size_t length, int site) {
    if (!value || length > SIZE_MAX - SKIP_ONE || cypher_agg_allocation_should_fail(site)) {
        return NULL;
    }
    char *copy = malloc(length + SKIP_ONE);
    if (copy) {
        memcpy(copy, value, length);
        copy[length] = '\0';
    }
    return copy;
}

typedef struct {
    char *value;
    size_t length;
    char *entity_key; /* NULL for scalar values; otherwise owns the index key. */
} aggregate_value_entry_t;

typedef struct {
    aggregate_value_entry_t *entries;
    int count;
    int capacity;
    CBMHashTable *distinct_index; /* borrows entry value/entity_key strings */
} aggregate_value_list_t;

static void aggregate_value_list_free(aggregate_value_list_t *list) {
    if (!list) {
        return;
    }
    /* The index borrows entry strings, so release it before its keys. */
    cbm_ht_free(list->distinct_index);
    for (int i = 0; i < list->count; i++) {
        free(list->entries[i].entity_key);
        free(list->entries[i].value);
    }
    free(list->entries);
    memset(list, 0, sizeof(*list));
}

/* Return a bounded canonical identity for a bare graph entity. A property or
 * virtual scalar has no entity identity and uses its exact value as the set
 * key. Node and relationship identifiers are signed 64-bit values, so the
 * fixed representation is a type bound rather than a query-data limit. */
static bool aggregate_entity_key(binding_t *binding, const cbm_return_item_t *item,
                                 char key[CBM_SZ_64], size_t *key_length) {
    if (!binding || !item || !item->variable || item->property) {
        return false;
    }
    int written = 0;
    cbm_node_t *node = binding_get(binding, item->variable);
    if (node && node->id > 0) {
        written = snprintf(key, CBM_SZ_64, "N:%lld", (long long)node->id);
    } else {
        cbm_edge_t *edge = binding_get_edge(binding, item->variable);
        if (edge && edge->id > 0) {
            written = snprintf(key, CBM_SZ_64, "E:%lld", (long long)edge->id);
        }
    }
    if (written <= 0 || written >= CBM_SZ_64) {
        return false;
    }
    *key_length = (size_t)written;
    return true;
}

/* Append an aggregate value, optionally through an exact membership index.
 * The value vector grows geometrically, so append is amortized O(1) metadata
 * work plus O(value bytes). DISTINCT performs one expected-O(1) hash probe
 * over an exact scalar value or canonical entity id; total expected runtime is
 * O(total input bytes), retained memory is O(total unique value bytes), and
 * first-seen order remains available to COLLECT. Every allocation is owned or
 * rolled back before the count/index publishes the new entry. */
static bool aggregate_value_list_add(aggregate_value_list_t *list, binding_t *binding,
                                     const cbm_return_item_t *item, const cypher_value_t *value,
                                     bool distinct, bool *inserted) {
    if (!list || !binding || !item || !value || !value->data || !inserted || list->count < 0 ||
        list->capacity < 0 || list->count > list->capacity || list->count > INT_MAX - SKIP_ONE) {
        g_cypher_allocation_failed = true;
        return false;
    }
    *inserted = false;

    char entity_key[CBM_SZ_64];
    size_t entity_key_length = 0;
    bool has_entity_key =
        distinct && aggregate_entity_key(binding, item, entity_key, &entity_key_length);
    char *owned_value = NULL;
    char *owned_entity_key = NULL;

    if (distinct && !list->distinct_index) {
        if (cypher_agg_allocation_should_fail(CYP_AGG_ALLOC_DISTINCT_INDEX)) {
            g_cypher_allocation_failed = true;
            return false;
        }
        /* Capacity zero lets the shared table allocate only when the first
         * unique key is inserted instead of reserving per aggregate group. */
        list->distinct_index = cbm_ht_create(0);
        if (!list->distinct_index) {
            g_cypher_allocation_failed = true;
            return false;
        }
    }

    if (!has_entity_key) {
        owned_value = cypher_agg_strndup(value->data, value->length, CYP_AGG_ALLOC_VALUE_COPY);
        if (!owned_value) {
            g_cypher_allocation_failed = true;
            return false;
        }
    }
    const char *probe_key = has_entity_key ? entity_key : owned_value;
    if (distinct) {
        if (g_cypher_track_aggregate_distinct_probes) {
            g_cypher_aggregate_distinct_probes++;
        }
        if (cbm_ht_has(list->distinct_index, probe_key)) {
            free(owned_value);
            return true;
        }
    }

    if (has_entity_key) {
        owned_entity_key =
            cypher_agg_strndup(entity_key, entity_key_length, CYP_AGG_ALLOC_VALUE_COPY);
        owned_value = cypher_agg_strndup(value->data, value->length, CYP_AGG_ALLOC_VALUE_COPY);
        if (!owned_entity_key || !owned_value) {
            free(owned_entity_key);
            free(owned_value);
            g_cypher_allocation_failed = true;
            return false;
        }
    }

    if (list->count == list->capacity) {
        int next_capacity = list->capacity == 0 ? CBM_SZ_8 : list->capacity;
        if (list->capacity != 0) {
            if (list->capacity > INT_MAX / PAIR_LEN) {
                free(owned_entity_key);
                free(owned_value);
                g_cypher_allocation_failed = true;
                return false;
            }
            next_capacity = list->capacity * PAIR_LEN;
        }
        if ((size_t)next_capacity > SIZE_MAX / sizeof(*list->entries)) {
            free(owned_entity_key);
            free(owned_value);
            g_cypher_allocation_failed = true;
            return false;
        }
        aggregate_value_entry_t *grown =
            cypher_agg_realloc(list->entries, (size_t)next_capacity * sizeof(*grown),
                               CYP_AGG_ALLOC_VALUE_ARRAY_GROWTH);
        if (!grown) {
            free(owned_entity_key);
            free(owned_value);
            g_cypher_allocation_failed = true;
            return false;
        }
        list->entries = grown;
        list->capacity = next_capacity;
    }

    aggregate_value_entry_t *entry = &list->entries[list->count];
    *entry = (aggregate_value_entry_t){
        .value = owned_value, .length = value->length, .entity_key = owned_entity_key};
    if (distinct) {
        const char *owned_key = entry->entity_key ? entry->entity_key : entry->value;
        (void)cbm_ht_set(list->distinct_index, owned_key, (void *)(uintptr_t)SKIP_ONE);
        if (g_cypher_track_aggregate_distinct_probes) {
            g_cypher_aggregate_distinct_probes++;
        }
        if (!cbm_ht_has(list->distinct_index, owned_key)) {
            free(entry->entity_key);
            free(entry->value);
            memset(entry, 0, sizeof(*entry));
            g_cypher_allocation_failed = true;
            return false;
        }
    }
    list->count++;
    *inserted = true;
    return true;
}

/* Append one exact byte span as a JSON string. Contiguous ordinary bytes are
 * copied in runs; quotes, backslashes, and control bytes use their canonical
 * JSON escape spelling. Runtime is O(length), auxiliary memory is O(1), and
 * the caller's geometric builder owns the only retained output allocation. */
static bool cypher_string_builder_append_json_string(cypher_string_builder_t *builder,
                                                     const char *text, size_t length) {
    if (!builder || !text ||
        !cypher_string_builder_append(builder, "\"", sizeof("\"") - SKIP_ONE)) {
        return false;
    }
    size_t run_start = 0;
    for (size_t i = 0; i < length; i++) {
        unsigned char byte = (unsigned char)text[i];
        const char *escape = NULL;
        size_t escape_length = PAIR_LEN;
        char unicode_escape[sizeof("\\u00FF")];
        switch (byte) {
        case '"':
            escape = "\\\"";
            break;
        case '\\':
            escape = "\\\\";
            break;
        case '\b':
            escape = "\\b";
            break;
        case '\f':
            escape = "\\f";
            break;
        case '\n':
            escape = "\\n";
            break;
        case '\r':
            escape = "\\r";
            break;
        case '\t':
            escape = "\\t";
            break;
        default:
            if (byte < CYP_JSON_CONTROL_LIMIT) {
                int written =
                    snprintf(unicode_escape, sizeof(unicode_escape), "\\u%04X", (unsigned int)byte);
                if (written < 0 || (size_t)written >= sizeof(unicode_escape)) {
                    return false;
                }
                escape = unicode_escape;
                escape_length = (size_t)written;
            }
            break;
        }
        if (!escape) {
            continue;
        }
        if ((i > run_start &&
             !cypher_string_builder_append(builder, text + run_start, i - run_start)) ||
            !cypher_string_builder_append(builder, escape, escape_length)) {
            return false;
        }
        run_start = i + SKIP_ONE;
    }
    return (run_start >= length ||
            cypher_string_builder_append(builder, text + run_start, length - run_start)) &&
           cypher_string_builder_append(builder, "\"", sizeof("\"") - SKIP_ONE);
}

/* Serialize an aggregate-owned string list without a fixed output ceiling.
 * Each item is JSON-escaped from its exact value span. Total runtime and
 * retained output are O(total item bytes + item count), with one reusable
 * geometric builder and O(1) scalar scratch. */
static bool format_collect_list_exact(const aggregate_value_list_t *list, cypher_value_t *output) {
    cypher_string_builder_t builder = {0};
    if (!list || !output || list->count < 0 || !cypher_string_builder_reset(&builder) ||
        !cypher_string_builder_append(&builder, "[", sizeof("[") - SKIP_ONE)) {
        cypher_string_builder_free(&builder);
        return false;
    }
    for (int i = 0; i < list->count; i++) {
        if ((i > 0 && !cypher_string_builder_append(&builder, ",", sizeof(",") - SKIP_ONE)) ||
            !cypher_string_builder_append_json_string(&builder, list->entries[i].value,
                                                      list->entries[i].length)) {
            cypher_string_builder_free(&builder);
            return false;
        }
    }
    if (!cypher_string_builder_append(&builder, "]", sizeof("]") - SKIP_ONE)) {
        cypher_string_builder_free(&builder);
        return false;
    }
    memset(output, 0, sizeof(*output));
    output->data = builder.data;
    output->length = builder.length;
    output->owned = builder.data;
    output->kind = CYP_VALUE_COMPOSITE;
    builder.data = NULL;
    cypher_string_builder_free(&builder);
    return true;
}

/* Format every aggregate through one value interface shared by RETURN and
 * WITH. Numeric/count spellings fit the bounded inline representation by type;
 * COLLECT transfers an exact query-sized owner. */
static bool format_aggregate_value_exact(const char *func, int count, double sum, double min_value,
                                         double max_value, aggregate_value_list_t *value_lists,
                                         int ci, cypher_value_t *output) {
    if (!func || !output) {
        g_cypher_allocation_failed = true;
        return false;
    }
    if (strcmp(func, "COLLECT") == 0) {
        return format_collect_list_exact(&value_lists[ci], output);
    }
    memset(output, 0, sizeof(*output));
    if (count == 0 &&
        (strcmp(func, "AVG") == 0 || strcmp(func, "MIN") == 0 || strcmp(func, "MAX") == 0)) {
        output->data = "";
        output->is_null = true;
        output->kind = CYP_VALUE_NULL;
        return true;
    }
    int written = 0;
    if (strcmp(func, "SUM") == 0) {
        written = snprintf(output->inline_text, sizeof(output->inline_text), "%.10g", sum);
    } else if (strcmp(func, "AVG") == 0) {
        written = snprintf(output->inline_text, sizeof(output->inline_text), "%.10g", sum / count);
    } else if (strcmp(func, "MIN") == 0) {
        written = snprintf(output->inline_text, sizeof(output->inline_text), "%.10g", min_value);
    } else if (strcmp(func, "MAX") == 0) {
        written = snprintf(output->inline_text, sizeof(output->inline_text), "%.10g", max_value);
    } else {
        written = snprintf(output->inline_text, sizeof(output->inline_text), "%d", count);
    }
    if (written < 0 || (size_t)written >= sizeof(output->inline_text)) {
        g_cypher_allocation_failed = true;
        output->data = "";
        output->is_null = true;
        return false;
    }
    output->data = output->inline_text;
    output->length = (size_t)written;
    output->kind = strcmp(func, "COUNT") == 0 ? CYP_VALUE_INTEGER : CYP_VALUE_FLOAT;
    return true;
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

/* Return one owned canonical output name shared by RETURN and WITH. Keeping a
 * single derivation prevents the two public projection surfaces from drifting
 * as expression support grows. Runtime and memory are O(name bytes). */
static char *cypher_item_name_owned(const cbm_return_item_t *item) {
    if (!item) {
        return NULL;
    }
    if (item->alias) {
        return heap_strdup(item->alias);
    }
    if (item->func) {
        const char *parts[] = {item->func, "(", item->variable ? item->variable : "", ")"};
        return cypher_join_parts(parts, sizeof(parts) / sizeof(parts[0]));
    }
    if (item->kase) {
        return heap_strdup("CASE");
    }
    if (item->property) {
        const char *parts[] = {item->variable, ".", item->property};
        return cypher_join_parts(parts, sizeof(parts) / sizeof(parts[0]));
    }
    return heap_strdup(item->variable);
}

/* Compute each WITH output name once per query clause, rather than formatting
 * a bounded name for every projected row. Construction is O(C + A) time and
 * O(C + A) memory for C items and A total alias bytes; callers reuse cached
 * lengths while each virtual binding takes its required independent copy. */
static bool build_with_aliases(const cbm_return_clause_t *wc, const char ***aliases_out,
                               size_t **lengths_out) {
    if (!wc || !aliases_out || !lengths_out || wc->count < 0) {
        g_cypher_allocation_failed = true;
        return false;
    }
    const char **aliases = cypher_calloc_elements(wc->count, sizeof(*aliases));
    size_t *lengths = cypher_calloc_elements(wc->count, sizeof(*lengths));
    if (!aliases || !lengths) {
        free(aliases);
        free(lengths);
        return false;
    }
    for (int i = 0; i < wc->count; i++) {
        aliases[i] = cypher_item_name_owned(&wc->items[i]);
        if (!aliases[i]) {
            for (int j = 0; j < i; j++) {
                safe_str_free(&aliases[j]);
            }
            free(aliases);
            free(lengths);
            g_cypher_allocation_failed = true;
            return false;
        }
        lengths[i] = strlen(aliases[i]);
    }
    *aliases_out = aliases;
    *lengths_out = lengths;
    return true;
}

static void free_string_vector(const char **strings, int count) {
    if (!strings) {
        return;
    }
    for (int i = 0; i < count; i++) {
        safe_str_free(&strings[i]);
    }
    free(strings);
}

/* ── WITH clause: project bindings through aggregation or rename ── */

/* WITH aggregation group entry */
typedef struct {
    const char *group_key; /* owned; also borrowed by aggregate_group_index_t */
    const char **group_vals;
    bool *group_nulls;
    cypher_value_kind_t *group_kinds;
    double *sums;
    int *counts;
    double *mins, *maxs;
    aggregate_value_list_t *value_lists; /* COLLECT and aggregate DISTINCT state */
    int64_t *group_node_ids; /* per-item node id when the group var is a node (0 = not) */
} with_agg_t;

/* Build a group key from non-aggregate WITH items */
static bool with_agg_build_key(cbm_return_clause_t *wc, binding_t *b,
                               cypher_string_builder_t *key) {
    for (int ci = 0; ci < wc->count; ci++) {
        if (is_aggregate_func(wc->items[ci].func)) {
            continue;
        }
        cbm_return_item_t *item = &wc->items[ci];
        bool direct = !item->func && !item->kase && !item->args;
        cypher_value_t value;
        char function_buffer[CBM_SZ_512];
        if (!project_item_exact_value(b, item, &value)) {
            const char *text = project_item(b, item, function_buffer, sizeof(function_buffer));
            cypher_value_set_cstr(&value, text ? text : "", false);
        }
        bool appended =
            group_key_append_value(key, b, item->variable, direct && !item->property, &value);
        cypher_value_free(&value);
        if (!appended) {
            return false;
        }
    }
    return true;
}

static void with_agg_entry_free(with_agg_t *entry, int item_count) {
    if (!entry) {
        return;
    }
    safe_str_free(&entry->group_key);
    for (int ci = 0; ci < item_count; ci++) {
        if (entry->group_vals) {
            safe_str_free(&entry->group_vals[ci]);
        }
        if (entry->value_lists) {
            aggregate_value_list_free(&entry->value_lists[ci]);
        }
    }
    free(entry->group_vals);
    free(entry->group_nulls);
    free(entry->group_kinds);
    free(entry->sums);
    free(entry->counts);
    free(entry->mins);
    free(entry->maxs);
    free(entry->value_lists);
    free(entry->group_node_ids);
    memset(entry, 0, sizeof(*entry));
}

/* Construct a WITH group atomically. Each group retains O(item_count) state;
 * failure releases the initialized prefix and leaves one zeroed slot. */
static bool with_agg_init_group(with_agg_t *entry, cbm_return_clause_t *wc, binding_t *binding,
                                const char *key) {
    if (!entry || !wc || !binding || !key || wc->count <= 0 ||
        (size_t)wc->count > SIZE_MAX / sizeof(void *) ||
        (size_t)wc->count > SIZE_MAX / sizeof(double) ||
        (size_t)wc->count > SIZE_MAX / sizeof(cypher_value_kind_t) ||
        (size_t)wc->count > SIZE_MAX / sizeof(int64_t)) {
        g_cypher_allocation_failed = true;
        return false;
    }
    int item_count = wc->count;
    memset(entry, 0, sizeof(*entry));
    entry->group_key = cypher_agg_strdup(key, CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->group_vals =
        cypher_agg_calloc((size_t)item_count, sizeof(const char *), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->group_nulls =
        cypher_agg_calloc((size_t)item_count, sizeof(bool), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->group_kinds = cypher_agg_calloc((size_t)item_count, sizeof(*entry->group_kinds),
                                           CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->sums = cypher_agg_calloc((size_t)item_count, sizeof(double), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->counts = cypher_agg_calloc((size_t)item_count, sizeof(int), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->mins = cypher_agg_malloc((size_t)item_count * sizeof(double), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->maxs = cypher_agg_malloc((size_t)item_count * sizeof(double), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->value_lists = cypher_agg_calloc((size_t)item_count, sizeof(*entry->value_lists),
                                           CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->group_node_ids =
        cypher_agg_calloc((size_t)item_count, sizeof(int64_t), CYP_AGG_ALLOC_GROUP_ENTRY);
    if (!entry->group_key || !entry->group_vals || !entry->group_nulls || !entry->group_kinds ||
        !entry->sums || !entry->counts || !entry->mins || !entry->maxs || !entry->value_lists ||
        !entry->group_node_ids) {
        g_cypher_allocation_failed = true;
        with_agg_entry_free(entry, item_count);
        return false;
    }
    for (int ci = 0; ci < item_count; ci++) {
        entry->mins[ci] = CYP_DBL_MAX;
        entry->maxs[ci] = -CYP_DBL_MAX;
        char value_buffer[CBM_SZ_512];
        cypher_value_t projected;
        cypher_value_set_cstr(&projected, "0", false);
        if (!is_aggregate_func(wc->items[ci].func)) {
            if (!project_item_exact_value(binding, &wc->items[ci], &projected)) {
                const char *value =
                    project_item(binding, &wc->items[ci], value_buffer, sizeof(value_buffer));
                cypher_value_set_cstr(&projected, value ? value : "", false);
            }
            entry->group_nulls[ci] = projected.is_null;
            entry->group_kinds[ci] = projected.kind;
            if (!wc->items[ci].func && !wc->items[ci].property && wc->items[ci].variable) {
                cbm_node_t *group_node = binding_get(binding, wc->items[ci].variable);
                if (group_node) {
                    entry->group_node_ids[ci] = group_node->id;
                }
            }
        }
        entry->group_vals[ci] =
            cypher_agg_strndup(projected.data, projected.length, CYP_AGG_ALLOC_GROUP_ENTRY);
        cypher_value_free(&projected);
        if (!entry->group_vals[ci]) {
            g_cypher_allocation_failed = true;
            with_agg_entry_free(entry, item_count);
            return false;
        }
    }
    return true;
}

/* Find or create an aggregation group. Returns index. */
static int with_agg_find_or_create(with_agg_t **aggs, int *agg_cnt, int *agg_cap,
                                   aggregate_group_index_t *index, cbm_return_clause_t *wc,
                                   binding_t *b, const char *key) {
    int indexed = aggregate_group_index_lookup(index, key);
    if (indexed >= 0) {
        return indexed;
    }
    if (!index->valid) {
        for (int a = 0; a < *agg_cnt; a++) {
            if (g_cypher_track_group_lookup_probes) {
                g_cypher_group_lookup_probes++;
            }
            if (strcmp((*aggs)[a].group_key, key) == 0) {
                return a;
            }
        }
    }
    if (*agg_cnt >= *agg_cap) {
        if (*agg_cap > INT_MAX / PAIR_LEN ||
            (size_t)(*agg_cap * PAIR_LEN) > SIZE_MAX / sizeof(**aggs)) {
            g_cypher_allocation_failed = true;
            return CBM_NOT_FOUND;
        }
        int next_cap = *agg_cap * PAIR_LEN;
        with_agg_t *grown = cypher_agg_realloc(*aggs, (size_t)next_cap * sizeof(*grown),
                                               CYP_AGG_ALLOC_GROUP_ARRAY_GROWTH);
        if (!grown) {
            g_cypher_allocation_failed = true;
            return CBM_NOT_FOUND;
        }
        *aggs = grown;
        memset(&(*aggs)[*agg_cap], 0, (size_t)(next_cap - *agg_cap) * sizeof(**aggs));
        *agg_cap = next_cap;
    }
    int found = *agg_cnt;
    if (!with_agg_init_group(&(*aggs)[found], wc, b, key)) {
        return CBM_NOT_FOUND;
    }
    (*agg_cnt)++;
    aggregate_group_index_insert(index, (*aggs)[found].group_key, found);
    return found;
}

/* Accumulate aggregation values for a binding */
static bool with_agg_accumulate(with_agg_t *agg, cbm_return_clause_t *wc, binding_t *b) {
    for (int ci = 0; ci < wc->count; ci++) {
        if (!is_aggregate_func(wc->items[ci].func)) {
            continue;
        }
        cypher_value_t value;
        binding_get_virtual_value(b, wc->items[ci].variable, wc->items[ci].property, &value);
        if (value.is_null) {
            cypher_value_free(&value);
            continue;
        }
        bool is_collect = strcmp(wc->items[ci].func, "COLLECT") == 0;
        bool inserted = true;
        if (is_collect || wc->items[ci].distinct) {
            if (!aggregate_value_list_add(&agg->value_lists[ci], b, &wc->items[ci], &value,
                                          wc->items[ci].distinct, &inserted)) {
                cypher_value_free(&value);
                return false;
            }
        }
        if (!inserted) {
            cypher_value_free(&value);
            continue;
        }
        agg->counts[ci]++;
        bool is_numeric =
            strcmp(wc->items[ci].func, "SUM") == 0 || strcmp(wc->items[ci].func, "AVG") == 0 ||
            strcmp(wc->items[ci].func, "MIN") == 0 || strcmp(wc->items[ci].func, "MAX") == 0;
        if (is_numeric) {
            if (!cypher_value_own(&value)) {
                cypher_value_free(&value);
                return false;
            }
            double numeric = strtod(value.data, NULL);
            agg->sums[ci] += numeric;
            if (numeric < agg->mins[ci]) {
                agg->mins[ci] = numeric;
            }
            if (numeric > agg->maxs[ci]) {
                agg->maxs[ci] = numeric;
            }
        }
        cypher_value_free(&value);
    }
    return true;
}

/* Add a virtual variable binding for one WITH item */
static void with_add_vbinding_var_sized(binding_t *vb, const char *alias, size_t alias_length,
                                        const char *val, size_t value_length, bool is_null,
                                        cypher_value_kind_t kind) {
    int index = vb->var_count;
    if (!binding_reserve_node_index(vb, index)) {
        return;
    }
    char *owned_alias = alias_length > SIZE_MAX - SKIP_ONE ? NULL : malloc(alias_length + SKIP_ONE);
    char *owned_value = value_length > SIZE_MAX - SKIP_ONE ? NULL : malloc(value_length + SKIP_ONE);
    if (owned_alias) {
        memcpy(owned_alias, alias, alias_length);
        owned_alias[alias_length] = '\0';
    }
    if (owned_value) {
        memcpy(owned_value, val, value_length);
        owned_value[value_length] = '\0';
    }
    if (!owned_alias || !owned_value) {
        free(owned_alias);
        free(owned_value);
        vb->allocation_failed = true;
        return;
    }
    binding_set_node_metadata(vb, index, owned_alias, true, is_null, kind);
    binding_node_at(vb, index)->name = owned_value;
    vb->var_count++;
}

/* Cross each existing MATCH binding with a leading literal UNWIND list.
 * The list is independent of graph traversal, so applying the cross product
 * after MATCH expansion preserves Cypher row semantics while avoiding repeated
 * store scans. Runtime is O(B * L * V) and memory is O(min(B * L, W) * V),
 * where B is matched bindings, L list length, V bound variables, and W the
 * configured working-row budget. Hitting W uses the executor's existing loud
 * error path; a partial intermediate binding set is never returned. */
static void execute_unwind_literal(cbm_query_t *q, binding_t **bindings, int *bind_count,
                                   int *bind_cap, int max_working_rows) {
    if (!q->unwind_expr || q->unwind_expr[0] != '[' || !q->unwind_alias) {
        return;
    }
    yyjson_doc *doc = yyjson_read(q->unwind_expr, strlen(q->unwind_expr), 0);
    yyjson_val *list = doc ? yyjson_doc_get_root(doc) : NULL;
    if (!list || !yyjson_is_arr(list)) {
        yyjson_doc_free(doc);
        g_cypher_allocation_failed = true;
        return;
    }

    binding_t *source = *bindings;
    int source_count = *bind_count;
    binding_t *expanded = NULL;
    int expanded_count = 0;
    int expanded_cap = 0;
    bool stop = false;

    for (int bi = 0; bi < source_count && !stop; bi++) {
        size_t index;
        size_t list_count;
        yyjson_val *value;
        yyjson_arr_foreach(list, index, list_count, value) {
            cypher_value_t item;
            if (!cypher_value_set_yyjson(&item, value)) {
                g_cypher_allocation_failed = true;
                stop = true;
                break;
            }
            binding_t row = {0};
            binding_copy(&row, &source[bi]);
            with_add_vbinding_var_sized(&row, q->unwind_alias, strlen(q->unwind_alias), item.data,
                                        item.length, item.is_null, item.kind);
            cypher_value_free(&item);
            if (!binding_array_append(&expanded, &expanded_count, &expanded_cap, max_working_rows,
                                      &row)) {
                stop = true;
                break;
            }
        }
    }

    for (int bi = 0; bi < source_count; bi++) {
        binding_free(&source[bi]);
    }
    free(source);
    yyjson_doc_free(doc);
    *bindings = expanded;
    *bind_count = expanded_count;
    *bind_cap = expanded_cap;
}

/* Free with_agg_t array */
static void with_agg_free(with_agg_t *aggs, int agg_cnt, int item_count) {
    for (int a = 0; a < agg_cnt; a++) {
        with_agg_entry_free(&aggs[a], item_count);
    }
    free(aggs);
}

/* Execute WITH aggregation path */
static void execute_with_aggregate(cbm_return_clause_t *wc, binding_t *bindings, int bind_count,
                                   binding_t **vbindings, int *vcount, const char **aliases,
                                   const size_t *alias_lengths) {
    int agg_cap = CBM_SZ_256;
    with_agg_t *aggs =
        cypher_agg_calloc((size_t)agg_cap, sizeof(with_agg_t), CYP_AGG_ALLOC_INITIAL);
    int agg_cnt = 0;
    aggregate_group_index_t group_index = aggregate_group_index_create();
    cypher_string_builder_t key = {0};
    if (!aggs || !cypher_string_builder_reset(&key)) {
        g_cypher_allocation_failed = true;
        aggregate_group_index_free(&group_index);
        cypher_string_builder_free(&key);
        free(aggs);
        return;
    }

    for (int bi = 0; bi < bind_count; bi++) {
        if (!cypher_string_builder_reset(&key) || !with_agg_build_key(wc, &bindings[bi], &key)) {
            break;
        }
        int found = with_agg_find_or_create(&aggs, &agg_cnt, &agg_cap, &group_index, wc,
                                            &bindings[bi], key.data);
        if (found < 0) {
            break;
        }
        if (!with_agg_accumulate(&aggs[found], wc, &bindings[bi])) {
            break;
        }
    }

    if (g_cypher_allocation_failed) {
        cypher_string_builder_free(&key);
        aggregate_group_index_free(&group_index);
        with_agg_free(aggs, agg_cnt, wc->count);
        return;
    }
    cypher_string_builder_free(&key);
    *vbindings = safe_realloc(*vbindings, (size_t)(agg_cnt + SKIP_ONE) * sizeof(binding_t));
    if (!*vbindings) {
        g_cypher_allocation_failed = true;
        aggregate_group_index_free(&group_index);
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
        for (int ci = 0; ci < wc->count && !g_cypher_allocation_failed && !vb.allocation_failed;
             ci++) {
            if (is_aggregate_func(wc->items[ci].func)) {
                int aggregate_count =
                    wc->items[ci].distinct && strcmp(wc->items[ci].func, "COUNT") == 0
                        ? aggs[a].value_lists[ci].count
                        : aggs[a].counts[ci];
                cypher_value_t formatted;
                if (!format_aggregate_value_exact(
                        wc->items[ci].func, aggregate_count, aggs[a].sums[ci], aggs[a].mins[ci],
                        aggs[a].maxs[ci], aggs[a].value_lists, ci, &formatted)) {
                    break;
                }
                with_add_vbinding_var_sized(&vb, aliases[ci], alias_lengths[ci], formatted.data,
                                            formatted.length, formatted.is_null, formatted.kind);
                cypher_value_free(&formatted);
            } else {
                const char *value = aggs[a].group_vals[ci] ? aggs[a].group_vals[ci] : "";
                with_add_vbinding_var_sized(&vb, aliases[ci], alias_lengths[ci], value,
                                            strlen(value), aggs[a].group_nulls[ci],
                                            aggs[a].group_kinds[ci]);
                /* Tag the carried virtual var with the node id (when the group
                 * var is a node) so node_prop can re-fetch its full properties. */
                if (aggs[a].group_node_ids[ci] > 0 && vb.var_count > 0) {
                    binding_node_at(&vb, vb.var_count - SKIP_ONE)->id = aggs[a].group_node_ids[ci];
                }
            }
        }
        if (g_cypher_allocation_failed || vb.allocation_failed) {
            g_cypher_allocation_failed = true;
            binding_free(&vb);
            break;
        }
        (*vbindings)[(*vcount)++] = vb;
    }
    aggregate_group_index_free(&group_index);
    with_agg_free(aggs, agg_cnt, wc->count);
}

/* Execute WITH simple (non-aggregate) projection */
static void execute_with_simple(cbm_return_clause_t *wc, binding_t *bindings, int bind_count,
                                binding_t *vbindings, int *vcount, const char **aliases,
                                const size_t *alias_lengths) {
    for (int bi = 0; bi < bind_count; bi++) {
        binding_t vb = {0};
        vb.store = bindings[bi].store; /* so node_prop can re-fetch / compute on the projection */
        vb.project = bindings[bi].project;
        vb.use_active_overlay_edges = bindings[bi].use_active_overlay_edges;
        for (int ci = 0; ci < wc->count; ci++) {
            cbm_return_item_t *item = &wc->items[ci];
            cypher_value_t value;
            if (project_item_exact_value(&bindings[bi], item, &value)) {
                with_add_vbinding_var_sized(&vb, aliases[ci], alias_lengths[ci], value.data,
                                            value.length, value.is_null, value.kind);
                cypher_value_free(&value);
            } else {
                char func_buf[CBM_SZ_512];
                const char *val = project_item(&bindings[bi], item, func_buf, sizeof(func_buf));
                const char *projected_text = val ? val : "";
                with_add_vbinding_var_sized(&vb, aliases[ci], alias_lengths[ci], projected_text,
                                            strlen(projected_text), false, CYP_VALUE_STRING);
            }
            /* A whole-node projection must remain a node binding across the
             * WITH boundary. Retain its canonical id so the next MATCH stage
             * can traverse from it and node_prop can re-fetch complete fields. */
            if (!wc->items[ci].func && !wc->items[ci].property && vb.var_count > 0) {
                cbm_node_t *carried = binding_get(&bindings[bi], wc->items[ci].variable);
                if (carried) {
                    binding_node_at(&vb, vb.var_count - SKIP_ONE)->id = carried->id;
                }
            }
        }
        if (vb.allocation_failed) {
            g_cypher_allocation_failed = true;
            binding_free(&vb);
            break;
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

/* Build one exact, unambiguous projected-value tuple using the same null,
 * length-prefix, and entity-identity encoding as aggregate grouping. */
static bool with_proj_key(const cbm_return_clause_t *wc, const char **aliases, binding_t *binding,
                          cypher_string_builder_t *key) {
    for (int ci = 0; ci < wc->count; ci++) {
        cypher_value_t value;
        binding_get_virtual_value(binding, aliases[ci], NULL, &value);
        bool appended = group_key_append_value(key, binding, aliases[ci], true, &value);
        cypher_value_free(&value);
        if (!appended) {
            return false;
        }
    }
    return true;
}

/* Apply WITH DISTINCT: drop projected rows whose value tuple duplicates an
 * earlier one, keeping first occurrence. For R rows containing T total tuple
 * bytes, exact key construction plus hash lookup is expected O(T) time and
 * O(T) memory; representational/OOM failures abort instead of publishing a
 * partially deduplicated or quadratically rescanned result. */
static void with_apply_distinct(cbm_return_clause_t *wc, const char **aliases, binding_t *vbindings,
                                int *vcount) {
    int original_count = *vcount;
    const char **owned_keys = cypher_calloc_elements(original_count, sizeof(*owned_keys));
    aggregate_group_index_t index = aggregate_group_index_create();
    cypher_string_builder_t key = {0};
    if (!owned_keys || !index.valid || !cypher_string_builder_reset(&key)) {
        g_cypher_allocation_failed = true;
        for (int i = 0; i < original_count; i++) {
            binding_free(&vbindings[i]);
        }
        *vcount = 0;
        free(owned_keys);
        aggregate_group_index_free(&index);
        cypher_string_builder_free(&key);
        return;
    }

    int kept = 0;
    int i = 0;
    for (; i < original_count; i++) {
        if (!cypher_string_builder_reset(&key) ||
            !with_proj_key(wc, aliases, &vbindings[i], &key)) {
            break;
        }
        if (aggregate_group_index_lookup(&index, key.data) != CYP_FOUND_NONE) {
            binding_free(&vbindings[i]);
            continue;
        }
        owned_keys[kept] = heap_strdup(key.data);
        if (!owned_keys[kept]) {
            g_cypher_allocation_failed = true;
            break;
        }
        aggregate_group_index_insert(&index, owned_keys[kept], kept);
        if (!index.valid) {
            g_cypher_allocation_failed = true;
            break;
        }
        if (kept != i) {
            vbindings[kept] = vbindings[i];
        }
        kept++;
    }
    if (g_cypher_allocation_failed) {
        for (; i < original_count; i++) {
            binding_free(&vbindings[i]);
        }
    }
    *vcount = kept;
    aggregate_group_index_free(&index);
    cypher_string_builder_free(&key);
    for (int key_index = 0; key_index < original_count; key_index++) {
        safe_str_free(&owned_keys[key_index]);
    }
    free(owned_keys);
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
    if (!vbindings) {
        g_cypher_allocation_failed = true;
        for (int bi = 0; bi < bind_count; bi++) {
            binding_free(&bindings[bi]);
        }
        free(bindings);
        *bindings_ptr = NULL;
        *bind_count_ptr = 0;
        return;
    }
    const char **aliases = NULL;
    size_t *alias_lengths = NULL;
    if (!build_with_aliases(wc, &aliases, &alias_lengths)) {
        free(vbindings);
        for (int bi = 0; bi < bind_count; bi++) {
            binding_free(&bindings[bi]);
        }
        free(bindings);
        *bindings_ptr = NULL;
        *bind_count_ptr = 0;
        return;
    }

    bool has_agg = false;
    for (int i = 0; i < wc->count; i++) {
        if (is_aggregate_func(wc->items[i].func)) {
            has_agg = true;
            break;
        }
    }

    if (has_agg) {
        execute_with_aggregate(wc, bindings, bind_count, &vbindings, &vcount, aliases,
                               alias_lengths);
    } else {
        execute_with_simple(wc, bindings, bind_count, vbindings, &vcount, aliases, alias_lengths);
    }

    /* WITH DISTINCT: dedup projected rows (no-op for aggregation, which already
     * collapses to one row per group). */
    if (!g_cypher_allocation_failed && wc->distinct) {
        with_apply_distinct(wc, aliases, vbindings, &vcount);
    }

    if (!g_cypher_allocation_failed) {
        with_sort_skip_limit(wc, vbindings, &vcount);
    }

    free(alias_lengths);
    free_string_vector(aliases, wc->count);

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

/* Project RETURN * — all bound variable properties. Two linear passes give
 * one exact O(V) allocation without an arbitrary variable ceiling or geometric
 * reallocation; V is the number of explicitly bound pattern variables. */
static int count_pattern_vars(const cbm_query_t *q) {
    int vc = 0;
    for (int pi = 0; pi < q->pattern_count; pi++) {
        for (int ni = 0; ni < q->patterns[pi].node_count; ni++) {
            if (q->patterns[pi].nodes[ni].variable) {
                if (vc > INT_MAX - SKIP_ONE) {
                    g_cypher_allocation_failed = true;
                    return CBM_NOT_FOUND;
                }
                vc++;
            }
        }
        for (int ri = 0; ri < q->patterns[pi].rel_count; ri++) {
            if (q->patterns[pi].rels[ri].variable) {
                if (vc > INT_MAX - SKIP_ONE) {
                    g_cypher_allocation_failed = true;
                    return CBM_NOT_FOUND;
                }
                vc++;
            }
        }
    }
    return vc;
}

static int collect_pattern_vars(const cbm_query_t *q, const char **vars, int capacity) {
    int vc = 0;
    for (int pi = 0; pi < q->pattern_count; pi++) {
        for (int ni = 0; ni < q->patterns[pi].node_count; ni++) {
            if (q->patterns[pi].nodes[ni].variable) {
                if (vc >= capacity) {
                    g_cypher_allocation_failed = true;
                    return CBM_NOT_FOUND;
                }
                vars[vc++] = q->patterns[pi].nodes[ni].variable;
            }
        }
        for (int ri = 0; ri < q->patterns[pi].rel_count; ri++) {
            if (q->patterns[pi].rels[ri].variable) {
                if (vc >= capacity) {
                    g_cypher_allocation_failed = true;
                    return CBM_NOT_FOUND;
                }
                vars[vc++] = q->patterns[pi].rels[ri].variable;
            }
        }
    }
    return vc;
}

/* Build star-projection columns: var.name, var.qualified_name, var.label, var.file_path */
static void build_star_columns(result_builder_t *rb, const char **vars, int vc) {
    if (vc < 0 || vc > INT_MAX / CYP_NODE_COLS) {
        g_cypher_allocation_failed = true;
        return;
    }
    int col_n = vc * CYP_NODE_COLS;
    const char **col_names = cypher_calloc_elements(col_n, sizeof(*col_names));
    if (!col_names) {
        return;
    }
    static const char *const suffixes[CYP_NODE_COLS] = {".name", ".qualified_name", ".label",
                                                        ".file_path"};
    for (int v = 0; v < vc && !g_cypher_allocation_failed; v++) {
        for (int ci = 0; ci < CYP_NODE_COLS; ci++) {
            const char *parts[] = {vars[v], suffixes[ci]};
            size_t index = ((size_t)v * CYP_NODE_COLS) + (size_t)ci;
            col_names[index] = cypher_join_parts(parts, sizeof(parts) / sizeof(parts[0]));
            if (!col_names[index]) {
                g_cypher_allocation_failed = true;
            }
        }
    }
    if (g_cypher_allocation_failed || !rb_adopt_columns(rb, col_names, col_n)) {
        free_string_vector(col_names, col_n);
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
    int vc = count_pattern_vars(q);
    if (vc < 0 || vc > INT_MAX / CYP_NODE_COLS) {
        g_cypher_allocation_failed = true;
        return;
    }
    const char **vars = cypher_calloc_elements(vc, sizeof(*vars));
    if (!vars || collect_pattern_vars(q, vars, vc) != vc) {
        free(vars);
        return;
    }
    build_star_columns(rb, vars, vc);
    int col_n = vc * CYP_NODE_COLS;
    const char **vals = cypher_calloc_elements(col_n, sizeof(*vals));
    if (!vals) {
        free(vars);
        return;
    }
    if (bind_count > max_rows) {
        rb->truncated = true;
    }
    for (int bi = 0; bi < bind_count && rb->row_count < max_rows && !g_cypher_allocation_failed;
         bi++) {
        project_star_row(&bindings[bi], vars, vc, vals);
        rb_add_row(rb, vals);
    }
    free(vals);
    free(vars);
}

/* RETURN aggregation entry */
typedef struct {
    const char *group_key; /* owned; also borrowed by aggregate_group_index_t */
    const char **group_vals;
    double *sums;
    int *counts;
    double *mins, *maxs;
    aggregate_value_list_t *value_lists;
} ret_agg_entry_t;

static void ret_agg_entry_free(ret_agg_entry_t *entry, int item_count) {
    if (!entry) {
        return;
    }
    safe_str_free(&entry->group_key);
    for (int ci = 0; ci < item_count; ci++) {
        if (entry->group_vals) {
            safe_str_free(&entry->group_vals[ci]);
        }
        if (entry->value_lists) {
            aggregate_value_list_free(&entry->value_lists[ci]);
        }
    }
    free(entry->group_vals);
    free(entry->sums);
    free(entry->counts);
    free(entry->mins);
    free(entry->maxs);
    free(entry->value_lists);
    memset(entry, 0, sizeof(*entry));
}

/* Construct a group atomically. Runtime and retained memory are O(item_count)
 * plus copied projected bytes. A failed field or value allocation releases the
 * exact initialized prefix and leaves `entry` zeroed for the shared unwind. */
static bool ret_agg_init_group(ret_agg_entry_t *entry, const char *key, int item_count,
                               const char **vals, const size_t *value_lengths) {
    if (!entry || !key || item_count <= 0 || !vals || !value_lengths ||
        (size_t)item_count > SIZE_MAX / sizeof(void *)) {
        g_cypher_allocation_failed = true;
        return false;
    }
    memset(entry, 0, sizeof(*entry));
    entry->group_key = cypher_agg_strdup(key, CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->group_vals =
        cypher_agg_calloc((size_t)item_count, sizeof(const char *), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->sums = cypher_agg_calloc((size_t)item_count, sizeof(double), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->counts = cypher_agg_calloc((size_t)item_count, sizeof(int), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->mins = cypher_agg_malloc((size_t)item_count * sizeof(double), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->maxs = cypher_agg_malloc((size_t)item_count * sizeof(double), CYP_AGG_ALLOC_GROUP_ENTRY);
    entry->value_lists = cypher_agg_calloc((size_t)item_count, sizeof(*entry->value_lists),
                                           CYP_AGG_ALLOC_GROUP_ENTRY);
    if (!entry->group_key || !entry->group_vals || !entry->sums || !entry->counts || !entry->mins ||
        !entry->maxs || !entry->value_lists) {
        g_cypher_allocation_failed = true;
        ret_agg_entry_free(entry, item_count);
        return false;
    }
    for (int ci = 0; ci < item_count; ci++) {
        entry->mins[ci] = CYP_DBL_MAX;
        entry->maxs[ci] = -CYP_DBL_MAX;
        entry->group_vals[ci] =
            cypher_agg_strndup(vals[ci], value_lengths[ci], CYP_AGG_ALLOC_GROUP_ENTRY);
        if (vals[ci] && !entry->group_vals[ci]) {
            g_cypher_allocation_failed = true;
            ret_agg_entry_free(entry, item_count);
            return false;
        }
    }
    return true;
}

/* Accumulate a binding into RETURN aggregation */
static bool ret_agg_accumulate(ret_agg_entry_t *entry, cbm_return_clause_t *ret, binding_t *b) {
    for (int ci = 0; ci < ret->count; ci++) {
        if (!is_aggregate_func(ret->items[ci].func)) {
            continue;
        }
        cypher_value_t value;
        binding_get_virtual_value(b, ret->items[ci].variable, ret->items[ci].property, &value);
        if (value.is_null) {
            cypher_value_free(&value);
            continue;
        }
        bool is_collect = strcmp(ret->items[ci].func, "COLLECT") == 0;
        bool inserted = true;
        if ((is_collect || ret->items[ci].distinct) &&
            !aggregate_value_list_add(&entry->value_lists[ci], b, &ret->items[ci], &value,
                                      ret->items[ci].distinct, &inserted)) {
            cypher_value_free(&value);
            return false;
        }
        if (!inserted) {
            cypher_value_free(&value);
            continue;
        }
        entry->counts[ci]++;
        bool is_numeric =
            strcmp(ret->items[ci].func, "SUM") == 0 || strcmp(ret->items[ci].func, "AVG") == 0 ||
            strcmp(ret->items[ci].func, "MIN") == 0 || strcmp(ret->items[ci].func, "MAX") == 0;
        if (is_numeric) {
            if (!cypher_value_own(&value)) {
                cypher_value_free(&value);
                return false;
            }
            double numeric = strtod(value.data, NULL);
            entry->sums[ci] += numeric;
            if (numeric < entry->mins[ci]) {
                entry->mins[ci] = numeric;
            }
            if (numeric > entry->maxs[ci]) {
                entry->maxs[ci] = numeric;
            }
        }
        cypher_value_free(&value);
    }
    return true;
}

/* Free RETURN aggregation entries */
static void ret_agg_free(ret_agg_entry_t *aggs, int agg_count, int item_count) {
    for (int a = 0; a < agg_count; a++) {
        ret_agg_entry_free(&aggs[a], item_count);
    }
    free(aggs);
}

/* Execute RETURN with aggregation */
/* Build group key and projected values for one binding */
static bool ret_agg_build_key(cbm_return_clause_t *ret, binding_t *b, cypher_string_builder_t *key,
                              const char **vals, size_t *value_lengths,
                              cypher_value_t *direct_values, char func_buffers[][CBM_SZ_512]) {
    for (int ci = 0; ci < ret->count; ci++) {
        if (is_aggregate_func(ret->items[ci].func)) {
            vals[ci] = "0";
            value_lengths[ci] = SKIP_ONE;
            continue;
        }
        cbm_return_item_t *item = &ret->items[ci];
        bool direct = !item->func && !item->kase && !item->args;
        cypher_value_t fallback_value;
        cypher_value_t *value = NULL;
        if (project_item_exact_value(b, item, &direct_values[ci])) {
            value = &direct_values[ci];
        } else {
            const char *text = project_item(b, item, func_buffers[ci], CBM_SZ_512);
            cypher_value_set_cstr(&fallback_value, text ? text : "", false);
            value = &fallback_value;
        }
        vals[ci] = value->data;
        value_lengths[ci] = value->length;
        bool preserve_entity_identity = direct && !item->property;
        if (!group_key_append_value(key, b, item->variable, preserve_entity_identity, value)) {
            return false;
        }
    }
    return true;
}

/* Emit one aggregated row into the result builder */
static void ret_agg_emit_row(cbm_return_clause_t *ret, ret_agg_entry_t *agg, result_builder_t *rb,
                             const char **row, size_t *lengths, char **owned_values,
                             cypher_value_t *formatted_values) {
    bool complete = true;
    for (int ci = 0; ci < ret->count; ci++) {
        if (!is_aggregate_func(ret->items[ci].func)) {
            row[ci] = agg->group_vals[ci];
            lengths[ci] = row[ci] ? strlen(row[ci]) : 0;
            continue;
        }
        int aggregate_count = ret->items[ci].distinct && strcmp(ret->items[ci].func, "COUNT") == 0
                                  ? agg->value_lists[ci].count
                                  : agg->counts[ci];
        if (!format_aggregate_value_exact(ret->items[ci].func, aggregate_count, agg->sums[ci],
                                          agg->mins[ci], agg->maxs[ci], agg->value_lists, ci,
                                          &formatted_values[ci])) {
            complete = false;
            break;
        }
        row[ci] = formatted_values[ci].data;
        lengths[ci] = formatted_values[ci].length;
        owned_values[ci] = formatted_values[ci].owned;
        formatted_values[ci].owned = NULL;
    }
    if (complete) {
        rb_add_row_sized_owned(rb, row, lengths, owned_values);
    }
    for (int ci = 0; ci < ret->count; ci++) {
        free(owned_values[ci]);
        owned_values[ci] = NULL;
        cypher_value_free(&formatted_values[ci]);
    }
}

static void execute_return_agg(cbm_return_clause_t *ret, binding_t *bindings, int bind_count,
                               result_builder_t *rb) {
    const char **key_values = cypher_calloc_elements(ret->count, sizeof(*key_values));
    size_t *key_value_lengths = cypher_calloc_elements(ret->count, sizeof(*key_value_lengths));
    cypher_value_t *direct_values = cypher_calloc_elements(ret->count, sizeof(*direct_values));
    char (*func_buffers)[CBM_SZ_512] = cypher_calloc_elements(ret->count, sizeof(*func_buffers));
    cypher_string_builder_t key = {0};
    if (!key_values || !key_value_lengths || !direct_values || !func_buffers ||
        !cypher_string_builder_reset(&key)) {
        free(key_values);
        free(key_value_lengths);
        free(direct_values);
        free(func_buffers);
        cypher_string_builder_free(&key);
        return;
    }
    int agg_cap = CBM_SZ_256;
    ret_agg_entry_t *aggs =
        cypher_agg_calloc((size_t)agg_cap, sizeof(ret_agg_entry_t), CYP_AGG_ALLOC_INITIAL);
    int agg_count = 0;
    aggregate_group_index_t group_index = aggregate_group_index_create();
    if (!aggs) {
        g_cypher_allocation_failed = true;
        aggregate_group_index_free(&group_index);
        cypher_string_builder_free(&key);
        free(func_buffers);
        free(direct_values);
        free(key_value_lengths);
        free(key_values);
        return;
    }

    for (int bi = 0; bi < bind_count; bi++) {
        /* Keep the general execution deadline: group lookup is expected O(1),
         * but projection and aggregate functions still process every binding. */
        if ((bi & CYPHER_DEADLINE_CHECK_MASK) == 0 && cypher_deadline_exceeded()) {
            break;
        }
        if (!cypher_string_builder_reset(&key) ||
            !ret_agg_build_key(ret, &bindings[bi], &key, key_values, key_value_lengths,
                               direct_values, func_buffers)) {
            break;
        }

        int found = aggregate_group_index_lookup(&group_index, key.data);
        if (!group_index.valid) {
            for (int a = 0; a < agg_count; a++) {
                if (g_cypher_track_group_lookup_probes) {
                    g_cypher_group_lookup_probes++;
                }
                if (strcmp(aggs[a].group_key, key.data) == 0) {
                    found = a;
                    break;
                }
            }
        }
        if (found < 0) {
            if (agg_count >= agg_cap) {
                if (agg_cap > INT_MAX / PAIR_LEN ||
                    (size_t)(agg_cap * PAIR_LEN) > SIZE_MAX / sizeof(*aggs)) {
                    g_cypher_allocation_failed = true;
                    break;
                }
                int next_cap = agg_cap * PAIR_LEN;
                ret_agg_entry_t *grown = cypher_agg_realloc(aggs, (size_t)next_cap * sizeof(*grown),
                                                            CYP_AGG_ALLOC_GROUP_ARRAY_GROWTH);
                if (!grown) {
                    g_cypher_allocation_failed = true;
                    break;
                }
                aggs = grown;
                memset(&aggs[agg_cap], 0, (size_t)(next_cap - agg_cap) * sizeof(*aggs));
                agg_cap = next_cap;
            }
            found = agg_count;
            if (!ret_agg_init_group(&aggs[found], key.data, ret->count, key_values,
                                    key_value_lengths)) {
                break;
            }
            agg_count++;
            aggregate_group_index_insert(&group_index, aggs[found].group_key, found);
        }
        if (!ret_agg_accumulate(&aggs[found], ret, &bindings[bi])) {
            break;
        }
        for (int ci = 0; ci < ret->count; ci++) {
            cypher_value_free(&direct_values[ci]);
        }
    }

    for (int ci = 0; ci < ret->count; ci++) {
        cypher_value_free(&direct_values[ci]);
    }
    cypher_string_builder_free(&key);
    free(func_buffers);
    free(direct_values);
    free(key_value_lengths);
    free(key_values);
    if (!g_cypher_allocation_failed) {
        const char **emit_values = cypher_calloc_elements(ret->count, sizeof(*emit_values));
        size_t *emit_lengths = cypher_calloc_elements(ret->count, sizeof(*emit_lengths));
        char **emit_owned = cypher_calloc_elements(ret->count, sizeof(*emit_owned));
        cypher_value_t *formatted_values =
            cypher_calloc_elements(ret->count, sizeof(*formatted_values));
        if (emit_values && emit_lengths && emit_owned && formatted_values) {
            for (int a = 0; a < agg_count && !g_cypher_allocation_failed; a++) {
                ret_agg_emit_row(ret, &aggs[a], rb, emit_values, emit_lengths, emit_owned,
                                 formatted_values);
            }
        }
        if (formatted_values) {
            for (int ci = 0; ci < ret->count; ci++) {
                free(emit_owned ? emit_owned[ci] : NULL);
                cypher_value_free(&formatted_values[ci]);
            }
        }
        free(formatted_values);
        free(emit_owned);
        free(emit_lengths);
        free(emit_values);
    }
    aggregate_group_index_free(&group_index);
    ret_agg_free(aggs, agg_count, ret->count);
}

/* Build RETURN column names from items */
static void build_return_columns(result_builder_t *rb, cbm_return_clause_t *ret) {
    if (!ret) {
        g_cypher_allocation_failed = true;
        return;
    }
    const char **col_names = cypher_calloc_elements(ret->count, sizeof(*col_names));
    if (!col_names) {
        return;
    }
    for (int i = 0; i < ret->count; i++) {
        col_names[i] = cypher_item_name_owned(&ret->items[i]);
        if (!col_names[i]) {
            g_cypher_allocation_failed = true;
            break;
        }
    }
    if (g_cypher_allocation_failed || !rb_adopt_columns(rb, col_names, ret->count)) {
        free_string_vector(col_names, ret->count);
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
        if ((ret->limit < 0 || ret->limit > max_rows) && bind_count > max_rows) {
            rb->truncated = true;
        }
    }
    const char **vals = cypher_calloc_elements(ret->count, sizeof(*vals));
    size_t *value_lengths = cypher_calloc_elements(ret->count, sizeof(*value_lengths));
    cypher_value_t *direct_values = cypher_calloc_elements(ret->count, sizeof(*direct_values));
    char (*func_bufs)[CBM_SZ_512] = cypher_calloc_elements(ret->count, sizeof(*func_bufs));
    if (!vals || !value_lengths || !direct_values || !func_bufs) {
        free(vals);
        free(value_lengths);
        free(direct_values);
        free(func_bufs);
        return;
    }
    for (int bi = 0; bi < bind_count && rb->row_count < proj_cap && !g_cypher_allocation_failed;
         bi++) {
        for (int ci = 0; ci < ret->count; ci++) {
            cbm_return_item_t *item = &ret->items[ci];
            if (project_item_exact_value(&bindings[bi], item, &direct_values[ci])) {
                vals[ci] = direct_values[ci].data;
                value_lengths[ci] = direct_values[ci].length;
            } else {
                vals[ci] = project_item(&bindings[bi], item, func_bufs[ci], sizeof(func_bufs[ci]));
                value_lengths[ci] = vals[ci] ? strlen(vals[ci]) : 0;
            }
        }
        rb_add_row_sized(rb, vals, value_lengths);
        for (int ci = 0; ci < ret->count; ci++) {
            cypher_value_free(&direct_values[ci]);
        }
    }
    free(func_bufs);
    free(direct_values);
    free(value_lengths);
    free(vals);
}

/* Build default 3-column headers (name, qualified_name, label) per variable */
static void build_default_columns(result_builder_t *rb, const char **vars, int vc) {
    if (vc < 0 || vc > INT_MAX / CYP_EDGE_COLS) {
        g_cypher_allocation_failed = true;
        return;
    }
    int col_n = vc * CYP_EDGE_COLS;
    const char **col_names = cypher_calloc_elements(col_n, sizeof(*col_names));
    if (!col_names) {
        g_cypher_allocation_failed = true;
        return;
    }
    for (int v = 0; v < vc; v++) {
        static const char *const suffixes[CYP_EDGE_COLS] = {".name", ".qualified_name", ".label"};
        size_t base = (size_t)v * CYP_EDGE_COLS;
        for (int ci = 0; ci < CYP_EDGE_COLS; ci++) {
            const char *parts[] = {vars[v], suffixes[ci]};
            col_names[base + (size_t)ci] =
                cypher_join_parts(parts, sizeof(parts) / sizeof(parts[0]));
            if (!col_names[base + (size_t)ci]) {
                g_cypher_allocation_failed = true;
                break;
            }
        }
    }
    if (g_cypher_allocation_failed || !rb_adopt_columns(rb, col_names, col_n)) {
        free_string_vector(col_names, col_n);
    }
}

/* Default projection when no RETURN clause */
static void execute_default_projection(cbm_pattern_t *pat0, binding_t *bindings, int bind_count,
                                       int max_rows, result_builder_t *rb) {
    int vc = 0;
    for (int ni = 0; ni < pat0->node_count; ni++) {
        if (pat0->nodes[ni].variable) {
            if (vc > INT_MAX - SKIP_ONE) {
                g_cypher_allocation_failed = true;
                return;
            }
            vc++;
        }
    }
    if (vc > INT_MAX / CYP_EDGE_COLS) {
        g_cypher_allocation_failed = true;
        return;
    }
    const char **vars = malloc((vc > 0 ? (size_t)vc : SKIP_ONE) * sizeof(*vars));
    if (!vars) {
        g_cypher_allocation_failed = true;
        return;
    }
    int vi = 0;
    for (int ni = 0; ni < pat0->node_count; ni++) {
        if (pat0->nodes[ni].variable) {
            vars[vi++] = pat0->nodes[ni].variable;
        }
    }
    build_default_columns(rb, vars, vc);
    int col_n = vc * CYP_EDGE_COLS;
    const char **vals = malloc((col_n > 0 ? (size_t)col_n : SKIP_ONE) * sizeof(*vals));
    if (!vals) {
        free(vars);
        g_cypher_allocation_failed = true;
        return;
    }
    if (bind_count > max_rows) {
        rb->truncated = true;
    }
    for (int bi = 0; bi < bind_count && rb->row_count < max_rows && !g_cypher_allocation_failed;
         bi++) {
        for (int v = 0; v < vc; v++) {
            cbm_node_t *n = binding_get(&bindings[bi], vars[v]);
            vals[(size_t)v * CYP_EDGE_COLS] = n && n->name ? n->name : "";
            vals[((size_t)v * CYP_EDGE_COLS) + SKIP_ONE] =
                n && n->qualified_name ? n->qualified_name : "";
            vals[((size_t)v * CYP_EDGE_COLS) + PAIR_LEN] = n && n->label ? n->label : "";
        }
        rb_add_row(rb, vals);
    }
    free(vals);
    free(vars);
}

/* Arithmetic-only compatibility seam for callers and tests that need to
 * validate the historical full-product allocation boundary. The executor
 * below deliberately does not allocate this product: it grows only to its
 * max_new working-row budget. O(1) time and O(1) memory. */
int cbm_cypher_cross_join_alloc(int bind_count, int extra_count, bool opt, size_t *out_n) {
    if (!out_n || bind_count < 0 || extra_count < 0) {
        return CBM_NOT_FOUND;
    }
    size_t per_binding = extra_count > 0 ? (size_t)extra_count : (opt ? 1U : 0U);
    if (per_binding > 0 && (size_t)bind_count > (size_t)INT_MAX / per_binding) {
        return CBM_NOT_FOUND;
    }
    size_t rows = (size_t)bind_count * per_binding;
    size_t alloc_n = rows + 1U;
    if (alloc_n > SIZE_MAX / sizeof(binding_t)) {
        return CBM_NOT_FOUND;
    }
    *out_n = alloc_n;
    return 0;
}

/* Cross-join node-only pattern into existing bindings */
static void cross_join_nodes(binding_t **bindings, int *bind_count, cbm_node_t *extra_nodes,
                             int extra_count, const char *nvar, bool opt,
                             const cbm_where_clause_t *pattern_where, int max_new) {
    /* Bound intermediate cardinality at the engine's public result ceiling.
     * This avoids signed multiplication overflow and keeps memory O(ceiling)
     * while still scanning rejected candidates until a qualifying row exists. */
    int new_cap = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
    if (new_cap > max_new) {
        new_cap = max_new;
    }
    binding_t *new_bindings = malloc((size_t)new_cap * sizeof(binding_t));
    if (!new_bindings) {
        g_cypher_allocation_failed = true;
        return;
    }
    int new_count = 0;
    for (int bi = 0; bi < *bind_count; bi++) {
        int match_count = 0;
        for (int ni = 0; ni < extra_count; ni++) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            binding_set(&nb, nvar, &extra_nodes[ni]);
            if (pattern_where && !eval_where(pattern_where, &nb)) {
                binding_free(&nb);
                continue;
            }
            if (!binding_array_append(&new_bindings, &new_count, &new_cap, max_new, &nb)) {
                goto cross_join_nodes_done;
            }
            match_count++;
        }
        if (opt && match_count == 0) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            if (!binding_array_append(&new_bindings, &new_count, &new_cap, max_new, &nb)) {
                goto cross_join_nodes_done;
            }
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
                                 const cbm_where_clause_t *pattern_where, int max_new) {
    int new_capacity = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
    if (new_capacity > max_new) {
        new_capacity = max_new;
    }
    binding_t *new_bindings = malloc((size_t)new_capacity * sizeof(binding_t));
    if (!new_bindings) {
        g_cypher_allocation_failed = true;
        return;
    }
    int new_count = 0;
    for (int bi = 0; bi < *bind_count; bi++) {
        for (int ni = 0; ni < extra_count; ni++) {
            binding_t nb = {0};
            binding_copy(&nb, &(*bindings)[bi]);
            binding_set(&nb, nvar, &extra_nodes[ni]);
            if (nb.allocation_failed) {
                g_cypher_allocation_failed = true;
                binding_free(&nb);
                goto cross_join_rels_done;
            }
            binding_t *tmp = malloc(sizeof(binding_t));
            if (!tmp) {
                g_cypher_allocation_failed = true;
                binding_free(&nb);
                goto cross_join_rels_done;
            }
            tmp[0] = nb;
            int tc = SKIP_ONE;
            const char *tv = nvar;
            expand_pattern_rels(store, patn, &tmp, &tc, &tv, opt, pattern_where, max_new);
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
                                       bool opt, const cbm_where_clause_t *pattern_where,
                                       int max_new) {
    cbm_rel_pattern_t *rel = &patn->rels[0];
    const cbm_node_pattern_t *start_node = &patn->nodes[0];
    /* The relationship is written start-[r]->terminal. To enumerate the start
     * nodes reachable from the bound terminal we invert the stored direction. */
    bool rel_inbound = rel->direction && strcmp(rel->direction, "inbound") == 0;
    /* (start)->(term): start = edge source = scan terminal's inbound edges. */
    bool scan_targets = !rel_inbound;

    int new_capacity = *bind_count > CYP_INIT_CAP8 ? *bind_count : CYP_INIT_CAP8;
    if (new_capacity > max_new) {
        new_capacity = max_new;
    }
    binding_t *new_bindings = malloc((size_t)new_capacity * sizeof(binding_t));
    if (!new_bindings) {
        g_cypher_allocation_failed = true;
        return;
    }
    int new_count = 0;
    for (int bi = 0; bi < *bind_count; bi++) {
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
                if (cbm_store_find_active_edge_nodes_by_qn(
                        store, b->project, term->qualified_name, (const char **)rel->types,
                        rel->type_count, direction, &rows, &row_count) == CBM_STORE_OK) {
                    process_active_edge_nodes(rows, row_count, start_node, b, start_var,
                                              rel->variable, &new_bindings, &new_count,
                                              &new_capacity, max_new, &match_count, pattern_where);
                }
                cbm_store_free_edge_nodes(rows, row_count);
                if (g_cypher_working_row_limit_hit > 0) {
                    goto expand_bound_terminal_done;
                }
            }
            if (!used_active_overlay_edges) {
                int type_count = rel->type_count > 0 ? rel->type_count : SKIP_ONE;
                for (int ti = 0; ti < type_count; ti++) {
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
                    for (int ei = 0; ei < edge_count; ei++) {
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
                        if (!binding_array_append(&new_bindings, &new_count, &new_capacity, max_new,
                                                  &nb)) {
                            break;
                        }
                        match_count++;
                    }
                    cbm_store_free_edges(edges, edge_count);
                    if (g_cypher_working_row_limit_hit > 0) {
                        goto expand_bound_terminal_done;
                    }
                }
            }
        }
        if (opt && match_count == 0) {
            /* No matching neighbour: keep the row with start_var left UNBOUND so
             * `WHERE <start> IS NULL` correctly identifies the no-edge case. The
             * shared append path fails the whole query if the working-row budget is
             * exhausted; it never publishes a partial OPTIONAL result. */
            binding_t nb = {0};
            binding_copy(&nb, b);
            if (!binding_array_append(&new_bindings, &new_count, &new_capacity, max_new, &nb)) {
                goto expand_bound_terminal_done;
            }
        }
    }

expand_bound_terminal_done:
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
                                 const char *project, int max_rows, int max_working_rows,
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
            expand_pattern_rels(store, patn, bindings, bind_count, &tv, opt, pattern_where,
                                max_working_rows);
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
                                           pattern_where, max_working_rows);
                continue;
            }
        }

        cbm_node_t *extra_nodes = NULL;
        int extra_count = 0;
        if (!scan_pattern_nodes(store, project, max_working_rows + SKIP_ONE, max_working_rows,
                                &patn->nodes[0], pattern_where, nvar, scan_mode, &extra_nodes,
                                &extra_count)) {
            return;
        }
        if (patn->rel_count == 0) {
            cross_join_nodes(bindings, bind_count, extra_nodes, extra_count, nvar, opt,
                             pattern_where, max_working_rows);
        } else {
            cross_join_with_rels(store, patn, bindings, bind_count, extra_nodes, extra_count, nvar,
                                 opt, pattern_where, max_working_rows);
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
                                int max_rows, int max_working_rows,
                                cypher_node_scan_mode_t scan_mode, binding_t **bindings,
                                int *bind_count, result_builder_t *rb) {
    if (!q) {
        rb_init(rb);
        return;
    }
    for (;;) {
        int bind_cap = *bind_count;
        if (bind_cap < max_rows) {
            bind_cap = max_rows;
        }
        if (bind_cap < SKIP_ONE) {
            bind_cap = SKIP_ONE;
        }

        expand_patterns_from(store, q, 0, project, max_rows, max_working_rows, scan_mode, bindings,
                             bind_count, &bind_cap);
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
        if ((ret->limit < 0 || ret->limit > max_rows) && bind_count > max_rows) {
            rb->truncated = true;
        }
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
        if (!rb_apply_distinct(rb)) {
            return;
        }
    }
    rb_apply_order_by(rb, ret);
    int output_limit = max_rows;
    if (ret->limit >= 0 && ret->limit < output_limit) {
        output_limit = ret->limit;
    }
    int available_after_skip = rb->row_count - (ret->skip > 0 ? ret->skip : 0);
    if ((ret->limit < 0 || ret->limit > max_rows) && available_after_skip > max_rows) {
        rb->truncated = true;
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

static void execute_single(cbm_store_t *store, cbm_query_t *q, const char *project, int max_rows,
                           int max_working_rows, cypher_node_scan_mode_t scan_mode,
                           bool allow_output_prefix, result_builder_t *rb) {
    cbm_pattern_t *pat0 = &q->patterns[0];
    const char *var_name = pat0->nodes[0].variable ? pat0->nodes[0].variable : "_n0";

    /* Step 1: Scan initial nodes */
    cbm_node_t *scanned = NULL;
    int scan_count = 0;
    bool output_prefix_is_complete =
        allow_output_prefix && query_initial_scan_can_stop_at_output_cap(q, var_name, scan_mode);
    bool server_output_cap_applies = !q->ret || q->ret->limit < 0 || q->ret->limit > max_rows;
    int exact_output_limit =
        q->ret && q->ret->limit >= 0 && q->ret->limit < max_rows ? q->ret->limit : max_rows;
    int candidate_limit = output_prefix_is_complete
                              ? exact_output_limit + (server_output_cap_applies ? SKIP_ONE : 0)
                              : max_working_rows + SKIP_ONE;
    int scan_working_budget = output_prefix_is_complete ? 0 : max_working_rows;
    /* An explicit LIMIT 0 is a complete empty result for this prefix-safe
     * shape, so avoid touching the store at all. */
    if (!output_prefix_is_complete || exact_output_limit > 0) {
        if (!scan_pattern_nodes(store, project, candidate_limit, scan_working_budget,
                                &pat0->nodes[0], q->where, var_name, scan_mode, &scanned,
                                &scan_count)) {
            return;
        }
    }

    /* Build initial bindings with early WHERE */
    int bind_cap = scan_count > max_rows ? scan_count : (max_rows > 0 ? max_rows : SKIP_ONE);
    binding_t *bindings = malloc((bind_cap + SKIP_ONE) * sizeof(binding_t));
    int bind_count = 0;
    if (!bindings) {
        g_cypher_allocation_failed = true;
        cbm_store_free_nodes(scanned, scan_count);
        return;
    }
    for (int i = 0; i < scan_count && bind_count < bind_cap; i++) {
        if ((i & CYPHER_DEADLINE_CHECK_MASK) == 0 && cypher_deadline_exceeded()) {
            break;
        }
        binding_t b = {0};
        b.store = store;
        b.project = project;
        b.use_active_overlay_edges = scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY;
        binding_set(&b, var_name, &scanned[i]);
        if (b.allocation_failed) {
            g_cypher_allocation_failed = true;
            binding_free(&b);
            break;
        }
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
    if (!g_cypher_allocation_failed && q->pattern_optional[0] && bind_count == 0) {
        binding_t b = {0};
        b.store = store;
        b.project = project;
        b.use_active_overlay_edges = scan_mode == CYP_NODE_SCAN_ACTIVE_OVERLAY;
        bindings[bind_count++] = b;
    }

    /* Step 2: Expand first pattern's relationships */
    const cbm_where_clause_t *first_pattern_where = q->pattern_count == SKIP_ONE ? q->where : NULL;
    expand_pattern_rels(store, pat0, &bindings, &bind_count, &var_name, q->pattern_optional[0],
                        first_pattern_where, max_working_rows);

    /* Step 2b: Additional patterns */
    expand_patterns_from(store, q, SKIP_ONE, project, max_rows, max_working_rows, scan_mode,
                         &bindings, &bind_count, &bind_cap);

    /* Step 2c: Leading literal UNWIND. The alias must exist before the late
     * WHERE/projection stages consume it. */
    execute_unwind_literal(q, &bindings, &bind_count, &bind_cap, max_working_rows);

    /* Step 3: Late WHERE */
    if (q->where && !query_where_is_optional_pattern_predicate(q) &&
        (q->unwind_expr || pat0->rel_count > 0 || q->pattern_count > SKIP_ONE)) {
        filter_bindings_where(q->where, bindings, &bind_count);
    }

    /* Step 3b: WITH clause */
    execute_with_clause(q, &bindings, &bind_count);

    /* Step 4: Project results */
    if (q->next_stage) {
        execute_bound_stage(store, q->next_stage, project, max_rows, max_working_rows, scan_mode,
                            &bindings, &bind_count, rb);
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
}

static bool cypher_is_degree_prop(const char *prop) {
    return prop && (strcmp(prop, "in_degree") == 0 || strcmp(prop, "out_degree") == 0);
}

static bool cypher_return_requires_canonical_identity(const cbm_return_clause_t *ret) {
    if (!ret) {
        return false;
    }
    for (int i = 0; i < ret->order_count; i++) {
        const char *expression = ret->order_items[i].expression;
        if (expression && (strstr(expression, ".in_degree") || strstr(expression, ".out_degree"))) {
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
    /* Both the indexed one-segment path and the whole-pattern matcher consume
     * the same active-overlay graph definition. Pattern length is therefore no
     * longer a capability restriction; identity-bearing projections are still
     * screened separately by cypher_return_requires_canonical_identity(). */
    return pat != NULL;
}

static bool cypher_query_supports_active_nodes(const cbm_query_t *q) {
    for (const cbm_query_t *root = q; root; root = root->union_next) {
        for (const cbm_query_t *stage = root; stage; stage = stage->next_stage) {
            for (int pi = 0; pi < stage->pattern_count; pi++) {
                if (!cypher_pattern_supports_active_relationships(&stage->patterns[pi])) {
                    return false;
                }
            }
            /* The WHERE grammar rejects identity functions; identity-bearing
             * expressions currently enter through WITH/RETURN projections. */
            if (cypher_return_requires_canonical_identity(stage->with_clause) ||
                cypher_return_requires_canonical_identity(stage->ret)) {
                return false;
            }
        }
    }
    return true;
}

/* ── Main entry point ─────────────────────────────────────────── */

static int cbm_cypher_execute_impl(cbm_store_t *store, const char *query, const char *project,
                                   const cbm_cypher_limits_t *limits, bool request_active_nodes,
                                   cbm_cypher_result_t *out, bool *used_active_nodes) {
    memset(out, 0, sizeof(*out));
    if (used_active_nodes) {
        *used_active_nodes = false;
    }
    g_cypher_working_row_limit_hit = 0;
    g_cypher_allocation_failed = false;
    g_cypher_trail_work_rows = 0;
    g_cypher_trail_work_limit = 0;
    g_cypher_store_failed = false;
    g_cypher_store_error[0] = '\0';
    cypher_deadline_arm();
    int max_rows = limits ? limits->max_output_rows : 0;
    int max_working_rows = limits ? limits->max_working_rows : 0;
    if (max_rows < 0 || max_rows > CBM_MAX_QUERY_ROWS || max_working_rows < 0 ||
        max_working_rows > CBM_MAX_QUERY_WORKING_ROWS) {
        out->error =
            heap_strdup("query row limits are outside the supported range; use max_rows "
                        "0.." CBM_STRINGIFY(CBM_MAX_QUERY_ROWS) " and query_max_working_rows "
                                                                "0 (default) or 1.." CBM_STRINGIFY(
                                                                    CBM_MAX_QUERY_WORKING_ROWS));
        return CBM_NOT_FOUND;
    }
    if (max_rows == 0) {
        max_rows = CBM_DEFAULT_QUERY_MAX_ROWS;
    }
    if (max_working_rows == 0) {
        max_working_rows = CBM_DEFAULT_QUERY_MAX_WORKING_ROWS;
    }
    if (max_working_rows < max_rows) {
        max_working_rows = max_rows;
    }
    g_cypher_trail_work_limit = max_working_rows;

    cbm_query_t *q = NULL;
    char *err = NULL;
    if (cbm_cypher_parse(query, &q, &err) < 0 || !q) {
        out->error = err ? err : heap_strdup("query parser returned no executable query");
        return CBM_NOT_FOUND;
    }

    /* The public grammar historically accepted a leading variable expression
     * (`UNWIND items`) despite having no query-parameter or preceding-row scope
     * from which `items` could be resolved. Reject it explicitly instead of
     * silently ignoring the write-only AST field. Literal lists are executed
     * below; a future parameter API can extend this branch deliberately. */
    for (cbm_query_t *branch = q; branch; branch = branch->union_next) {
        if (branch->unwind_expr && branch->unwind_expr[0] != '[') {
            out->error = heap_strdup(
                "UNWIND variable input is unavailable without query parameters; use a literal "
                "list such as UNWIND [\"a\", \"b\"] AS item");
            cbm_query_free(q);
            return CBM_NOT_FOUND;
        }
    }

    cypher_node_scan_mode_t scan_mode = CYP_NODE_SCAN_CANONICAL;
    if (request_active_nodes && project && cypher_query_supports_active_nodes(q)) {
        scan_mode = CYP_NODE_SCAN_ACTIVE_OVERLAY;
        if (used_active_nodes) {
            *used_active_nodes = true;
        }
    }

    bool has_union = q->union_next != NULL;
    int branch_output_limit = has_union ? max_working_rows : max_rows;
    result_builder_t rb = {0};
    execute_single(store, q, project, branch_output_limit, max_working_rows, scan_mode, !has_union,
                   &rb);

    /* UNION chain */
    char union_schema_error[CBM_SZ_512] = "";
    int union_branch_number = PAIR_LEN;
    cbm_query_t *uq = q->union_next;
    while (uq) {
        result_builder_t rb2 = {0};
        execute_single(store, uq, project, max_working_rows, max_working_rows, scan_mode, false,
                       &rb2);
        if (!g_cypher_allocation_failed && !g_cypher_store_failed && !g_cypher_timed_out &&
            g_cypher_working_row_limit_hit == 0 &&
            !rb_union_schema_matches(&rb, &rb2, union_branch_number, union_schema_error,
                                     sizeof(union_schema_error))) {
            rb_free(&rb2);
            break;
        }
        /* Concatenate rows from rb2 into rb */
        for (int i = 0; i < rb2.row_count; i++) {
            if (rb.row_count >= max_working_rows) {
                g_cypher_working_row_limit_hit = max_working_rows;
                break;
            }
            rb_add_row(&rb, rb2.rows[i]);
        }
        rb_free(&rb2);

        uq = uq->union_next;
        union_branch_number++;
    }

    if (union_schema_error[0]) {
        rb_free(&rb);
        cbm_query_free(q);
        out->error = heap_strdup(union_schema_error);
        return CBM_NOT_FOUND;
    }

    /* UNION (not ALL) deduplication */
    if (q->union_next && !q->union_all) {
        (void)rb_apply_distinct(&rb);
    }
    /* max_rows is authoritative for the whole query, not independently for
     * each UNION branch. Apply it after UNION deduplication. */
    if (rb.row_count > max_rows) {
        rb.truncated = true;
    }
    rb_apply_skip_limit(&rb, 0, max_rows);

    /* #601: abort a runaway query that blew the wall-clock budget before it can
     * return a misleading partial result. Checked before the working budget. */
    if (g_cypher_timed_out) {
        rb_free(&rb);
        cbm_query_free(q);
        out->error = heap_strdup(
            "query exceeded the execution time limit — narrow starting nodes with labels, "
            "properties, or WHERE predicates; specify relationship types and directions; or "
            "use the finite hop bound required by the task (LIMIT cannot reduce match work)");
        return CBM_NOT_FOUND;
    }

    if (g_cypher_allocation_failed) {
        rb_free(&rb);
        cbm_query_free(q);
        out->error =
            heap_strdup("query could not allocate memory while building bindings or results");
        return CBM_NOT_FOUND;
    }

    if (g_cypher_store_failed) {
        char error[CBM_SZ_512];
        snprintf(error, sizeof(error), "query graph traversal failed: %s",
                 g_cypher_store_error[0] ? g_cypher_store_error : "store error");
        rb_free(&rb);
        cbm_query_free(q);
        out->error = heap_strdup(error);
        return CBM_NOT_FOUND;
    }

    if (g_cypher_working_row_limit_hit > 0) {
        /* Intermediate rows are not a valid partial Cypher result: WHERE,
         * DISTINCT, aggregation, ORDER BY, and UNION may still change both
         * membership and ordering. Return a tool-visible error so callers can
         * narrow the query or explicitly raise the configured budget. */
        char error[CBM_SZ_256];
        snprintf(error, sizeof(error),
                 "query exceeded the working-row budget (%d); raise "
                 "query_max_working_rows or narrow the pattern",
                 g_cypher_working_row_limit_hit);
        rb_free(&rb);
        cbm_query_free(q);
        out->error = heap_strdup(error);
        return CBM_NOT_FOUND;
    }

    out->columns = rb.columns;
    out->col_count = rb.col_count;
    out->rows = rb.rows;
    out->row_count = rb.row_count;
    out->truncated = rb.truncated;
    cbm_query_free(q);
    return 0;
}

int cbm_cypher_execute(cbm_store_t *store, const char *query, const char *project, int max_rows,
                       cbm_cypher_result_t *out) {
    cbm_cypher_limits_t limits = {
        .max_output_rows = max_rows > 0 ? max_rows : 0,
        .max_working_rows = CBM_DEFAULT_QUERY_MAX_WORKING_ROWS,
    };
    return cbm_cypher_execute_impl(store, query, project, &limits, false, out, NULL);
}

int cbm_cypher_execute_with_limits(cbm_store_t *store, const char *query, const char *project,
                                   const cbm_cypher_limits_t *limits, cbm_cypher_result_t *out) {
    return cbm_cypher_execute_impl(store, query, project, limits, false, out, NULL);
}

int cbm_cypher_execute_active_nodes(cbm_store_t *store, const char *query, const char *project,
                                    int max_rows, cbm_cypher_result_t *out,
                                    bool *used_active_nodes) {
    cbm_cypher_limits_t limits = {
        .max_output_rows = max_rows > 0 ? max_rows : 0,
        .max_working_rows = CBM_DEFAULT_QUERY_MAX_WORKING_ROWS,
    };
    return cbm_cypher_execute_impl(store, query, project, &limits, true, out, used_active_nodes);
}

int cbm_cypher_execute_active_nodes_with_limits(cbm_store_t *store, const char *query,
                                                const char *project,
                                                const cbm_cypher_limits_t *limits,
                                                cbm_cypher_result_t *out, bool *used_active_nodes) {
    return cbm_cypher_execute_impl(store, query, project, limits, true, out, used_active_nodes);
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
