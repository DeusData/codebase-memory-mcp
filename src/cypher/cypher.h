/*
 * cypher.h — Public API for the Cypher query engine.
 *
 * Provides lexing, parsing, planning, and execution of a subset of
 * Cypher queries against the cbm_store graph database.
 *
 * Supported syntax:
 *   MATCH (n:Label)-[:TYPE*1..3]->(m:Label {prop: "val"})
 *   WHERE n.name =~ ".*pattern.*" AND m.label = "Function"
 *   RETURN n.name, COUNT(m) AS cnt ORDER BY cnt DESC LIMIT 10
 */
#ifndef CBM_CYPHER_H
#define CBM_CYPHER_H

#include <stdint.h>
#include <stdbool.h>
#include <store/store.h>

/* ── Token types ────────────────────────────────────────────────── */

typedef enum {
    /* Keywords */
    TOK_MATCH,
    TOK_WHERE,
    TOK_RETURN,
    TOK_ORDER,
    TOK_BY,
    TOK_LIMIT,
    TOK_AND,
    TOK_OR,
    TOK_AS,
    TOK_DISTINCT,
    TOK_COUNT,
    TOK_CONTAINS,
    TOK_STARTS,
    TOK_WITH,
    TOK_NOT,
    TOK_ASC,
    TOK_DESC,
    TOK_NEQ,  /* <> or != */
    TOK_ENDS, /* ENDS (as in ENDS WITH) */
    TOK_IN,
    TOK_IS,
    TOK_NULL_KW, /* NULL keyword */
    TOK_XOR,
    TOK_SKIP,
    TOK_UNION,
    TOK_UNWIND,

    /* Aggregate functions */
    TOK_SUM,
    TOK_AVG,
    TOK_MIN_KW,
    TOK_MAX_KW,
    TOK_COLLECT,

    /* String functions — recognized as keywords */
    TOK_TOLOWER,
    TOK_TOUPPER,
    TOK_TOSTRING,

    /* CASE expression */
    TOK_CASE,
    TOK_WHEN,
    TOK_THEN,
    TOK_ELSE,
    TOK_END,

    /* Recognized-but-unsupported write/admin keywords */
    TOK_CREATE,
    TOK_DELETE,
    TOK_DETACH,
    TOK_SET,
    TOK_REMOVE,
    TOK_MERGE,
    TOK_OPTIONAL,
    TOK_YIELD,
    TOK_CALL,
    TOK_ALL,
    TOK_TRUE,
    TOK_FALSE,
    TOK_EXISTS,
    TOK_MANDATORY,
    TOK_FOREACH,
    TOK_ON,
    TOK_ADD,
    TOK_CONSTRAINT,
    TOK_DO,
    TOK_DROP,
    TOK_FOR,
    TOK_FROM,
    TOK_GRAPH,
    TOK_OF,
    TOK_REQUIRE,
    TOK_SCALAR,
    TOK_UNIQUE,

    /* Symbols */
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_LBRACKET,
    TOK_RBRACKET,
    TOK_DASH,
    TOK_GT,
    TOK_LT,
    TOK_COLON,
    TOK_DOT,
    TOK_LBRACE,
    TOK_RBRACE,
    TOK_STAR,
    TOK_COMMA,
    TOK_EQ,
    TOK_EQTILDE,
    TOK_GTE,
    TOK_LTE,
    TOK_PIPE,
    TOK_DOTDOT,

    /* Literals */
    TOK_IDENT,
    TOK_STRING,
    TOK_NUMBER,

    /* End of input */
    TOK_EOF,

    TOK_COUNT_TYPES /* sentinel for array sizing */
} cbm_token_type_t;

typedef struct {
    cbm_token_type_t type;
    const char *text; /* owned pointer to token text */
    int pos;          /* byte offset in source */
} cbm_token_t;

/* ── Lexer ──────────────────────────────────────────────────────── */

typedef struct {
    cbm_token_t *tokens;
    int count;
    int capacity;
    char *error; /* NULL if no error */
} cbm_lex_result_t;

/* Tokenize a Cypher query string. Caller must call cbm_lex_free(). */
int cbm_lex(const char *input, cbm_lex_result_t *out);
void cbm_lex_free(cbm_lex_result_t *r);

/* ── AST ────────────────────────────────────────────────────────── */

/* Inline property filter {key: "value"} */
typedef struct {
    const char *key;
    const char *value;
} cbm_prop_filter_t;

/* Node pattern: (variable:Label {props}) */
typedef struct {
    const char *variable; /* NULL if anonymous */
    const char *label;    /* NULL if unlabeled */
    cbm_prop_filter_t *props;
    int prop_count;
} cbm_node_pattern_t;

/* Relationship pattern: -[:TYPE|TYPE2*min..max]-> */
typedef struct {
    const char *variable; /* NULL if anonymous */
    const char **types;   /* edge type names */
    int type_count;
    const char *direction; /* "outbound", "inbound", "any" */
    int min_hops;          /* default 1 */
    int max_hops;          /* 0 = unbounded */
} cbm_rel_pattern_t;

/* A pattern is alternating nodes and relationships:
 * node0 rel0 node1 rel1 node2 ... */
typedef struct {
    cbm_node_pattern_t *nodes;
    int node_count;
    cbm_rel_pattern_t *rels;
    int rel_count;
} cbm_pattern_t;

/* WHERE condition */
typedef struct {
    const char *variable;
    const char *property;
    const char *op; /* "=", "<>", "=~", "CONTAINS", "STARTS WITH", "ENDS WITH",
                       ">", "<", ">=", "<=", "IN", "IS NULL", "IS NOT NULL" */
    const char *value;
    bool negated;           /* NOT prefix */
    const char **in_values; /* IN [...] list */
    int in_value_count;
} cbm_condition_t;

/* Expression tree for WHERE clause */
typedef enum {
    EXPR_CONDITION, /* leaf: single condition */
    EXPR_AND,
    EXPR_OR,
    EXPR_NOT,
    EXPR_XOR
} cbm_expr_type_t;

typedef struct cbm_expr cbm_expr_t;
struct cbm_expr {
    cbm_expr_type_t type;
    cbm_condition_t cond; /* leaf (EXPR_CONDITION only) */
    cbm_expr_t *left;     /* AND/OR/XOR left; NOT child */
    cbm_expr_t *right;    /* AND/OR/XOR right; NULL for NOT */
};

typedef struct {
    cbm_expr_t *root; /* expression tree (NULL = use legacy conditions) */
    /* Legacy flat model — kept during migration, removed after Phase 2 */
    cbm_condition_t *conditions;
    int count;
    const char *op; /* "AND" or "OR" */
} cbm_where_clause_t;

/* CASE expression: CASE WHEN expr THEN val [ELSE val] END */
typedef struct {
    cbm_expr_t *when_expr; /* condition */
    const char *then_val;  /* result if true */
} cbm_case_branch_t;

typedef struct {
    cbm_case_branch_t *branches;
    int branch_count;
    const char *else_val; /* NULL if no ELSE */
} cbm_case_expr_t;

/* RETURN item */
typedef struct {
    const char *variable;
    const char *property;  /* NULL for whole node */
    const char *alias;     /* NULL if no alias */
    const char *func;      /* "COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT",
                              "toLower", "toUpper", "toString" or NULL */
    cbm_case_expr_t *kase; /* CASE expression (NULL if not CASE) */
    bool distinct_arg;     /* true when func is count(DISTINCT ...) */
} cbm_return_item_t;

typedef struct {
    cbm_return_item_t *items;
    int count;
    bool distinct;
    bool star;             /* RETURN * */
    const char *order_by;  /* "variable.property" or "COUNT(var)" or alias */
    const char *order_dir; /* "ASC" or "DESC", NULL = default */
    int skip;              /* SKIP N, 0 = none */
    int limit;             /* 0 = default */
} cbm_return_clause_t;

/* Full query AST */
typedef struct cbm_query cbm_query_t;
struct cbm_query {
    cbm_pattern_t *patterns; /* array of patterns (first = main MATCH) */
    int pattern_count;
    bool *pattern_optional;              /* pattern_optional[i] = true → OPTIONAL MATCH */
    cbm_where_clause_t *where;           /* NULL if no WHERE */
    cbm_return_clause_t *with_clause;    /* WITH clause (NULL if none) */
    cbm_where_clause_t *post_with_where; /* WHERE after WITH */
    cbm_return_clause_t *ret;            /* NULL if no RETURN */
    cbm_query_t *union_next;             /* next query in UNION chain (NULL if none) */
    bool union_all;                      /* true = UNION ALL, false = UNION */
    /* UNWIND expr AS var */
    const char *unwind_expr;  /* expression (literal list or var ref) */
    const char *unwind_alias; /* variable name */
};

/* Convenience: access first pattern (backwards compat) */
#define cbm_query_pattern(q) ((q)->patterns[0])

/* ── Parser ─────────────────────────────────────────────────────── */

typedef struct {
    cbm_query_t *query;
    char *error; /* NULL if no error */
} cbm_parse_result_t;

/* Parse tokens into AST. Caller must call cbm_parse_free(). */
int cbm_parse(const cbm_token_t *tokens, int token_count, cbm_parse_result_t *out);
void cbm_parse_free(cbm_parse_result_t *r);

/* ── Executor ───────────────────────────────────────────────────── */

/* Query result: columns + rows */
typedef struct {
    const char **columns;
    int col_count;
    /* rows[row_idx][col_idx] = string value */
    const char ***rows;
    int row_count;
    /* Non-NULL when the query was rejected (e.g. result too large) */
    char *error;
} cbm_cypher_result_t;

/* Execute a Cypher query against a store.
 * max_rows: limit on output rows (0 = use virtual ceiling of 100k).
 * project: project name filter (NULL = all projects).
 * Returns -1 on error (check out->error for message). */
int cbm_cypher_execute(cbm_store_t *store, const char *query, const char *project, int max_rows,
                       cbm_cypher_result_t *out);

/* Free a query result. */
void cbm_cypher_result_free(cbm_cypher_result_t *r);

/* Convenience: lex + parse in one step. */
int cbm_cypher_parse(const char *query, cbm_query_t **out, char **error);

/* Free a query AST. */
void cbm_query_free(cbm_query_t *q);

#endif /* CBM_CYPHER_H */
