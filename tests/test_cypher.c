/*
 * test_cypher.c — Tests for the Cypher query engine.
 *
 * Ported from internal/cypher/cypher_test.go (1016 LOC).
 * Covers lexer, parser, and end-to-end execution.
 */
#include "test_framework.h"
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_thread.h"
#include <cypher/cypher.h>
#include <limits.h>
#include <sqlite3.h>
#include <store/store.h>
#include <yyjson/yyjson.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ══════════════════════════════════════════════════════════════════
 *  LEXER TESTS
 * ══════════════════════════════════════════════════════════════════ */

enum {
    CYPHER_TEST_LONG_TOKEN_BYTES = 5000,
    CYPHER_TEST_QUOTED_STRING_OVERHEAD = 3,
    CYPHER_TEST_TOKEN_GROWTH_COUNT = 33,
    CYPHER_TEST_REPEATED_VARIABLE_COUNT = 3,
    CYPHER_TEST_DYNAMIC_PROPERTY_COUNT = 10,
    CYPHER_TEST_JSON_STRING_MEMBER_OVERHEAD = 5,
    CYPHER_TEST_STAR_COLUMN_COUNT = 4,
    CYPHER_TEST_DEFAULT_COLUMN_COUNT = 3,
    CYPHER_TEST_DISTINCT_VALUE_BYTES = CBM_SZ_2K,
};

TEST(cypher_lex_simple_match) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("MATCH (n:Function)", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);

    /* MATCH ( n : Function ) EOF */
    ASSERT_GTE(r.count, 6);
    ASSERT_EQ(r.tokens[0].type, TOK_MATCH);
    ASSERT_EQ(r.tokens[1].type, TOK_LPAREN);
    ASSERT_EQ(r.tokens[2].type, TOK_IDENT);
    ASSERT_STR_EQ(r.tokens[2].text, "n");
    ASSERT_EQ(r.tokens[3].type, TOK_COLON);
    ASSERT_EQ(r.tokens[4].type, TOK_IDENT);
    ASSERT_STR_EQ(r.tokens[4].text, "Function");
    ASSERT_EQ(r.tokens[5].type, TOK_RPAREN);

    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_relationship) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("-[:CALLS]->", &r);
    ASSERT_EQ(rc, 0);

    /* - [ : CALLS ] - > EOF */
    ASSERT_GTE(r.count, 7);
    ASSERT_EQ(r.tokens[0].type, TOK_DASH);
    ASSERT_EQ(r.tokens[1].type, TOK_LBRACKET);
    ASSERT_EQ(r.tokens[2].type, TOK_COLON);
    ASSERT_EQ(r.tokens[3].type, TOK_IDENT);
    ASSERT_STR_EQ(r.tokens[3].text, "CALLS");
    ASSERT_EQ(r.tokens[4].type, TOK_RBRACKET);
    ASSERT_EQ(r.tokens[5].type, TOK_DASH);
    ASSERT_EQ(r.tokens[6].type, TOK_GT);

    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_string_literal) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("\"hello world\"", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.count, 1);
    ASSERT_EQ(r.tokens[0].type, TOK_STRING);
    ASSERT_STR_EQ(r.tokens[0].text, "hello world");

    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_single_quote_string) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("'hello'", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.tokens[0].type, TOK_STRING);
    ASSERT_STR_EQ(r.tokens[0].text, "hello");

    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_preserves_long_string) {
    /* A safety guard must not silently change the literal's query meaning. */
    const int big = CYPHER_TEST_LONG_TOKEN_BYTES;
    /* query: "AAAA...A"  (quotes included) */
    char *query = malloc((size_t)big + CYPHER_TEST_QUOTED_STRING_OVERHEAD);
    ASSERT_NOT_NULL(query);
    query[0] = '"';
    memset(query + 1, 'A', big);
    query[big + 1] = '"';
    query[big + 2] = '\0';

    cbm_lex_result_t r = {0};
    int rc = cbm_lex(query, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_GTE(r.count, 1);
    ASSERT_EQ(r.tokens[0].type, TOK_STRING);
    ASSERT_EQ((int)strlen(r.tokens[0].text), big);

    cbm_lex_free(&r);
    free(query);
    PASS();
}

TEST(cypher_lex_preserves_long_identifier) {
    const int length = CYPHER_TEST_LONG_TOKEN_BYTES;
    char *identifier = malloc((size_t)length + SKIP_ONE);
    ASSERT_NOT_NULL(identifier);
    memset(identifier, 'a', (size_t)length);
    identifier[length] = '\0';

    cbm_lex_result_t r = {0};
    ASSERT_EQ(cbm_lex(identifier, &r), 0);
    ASSERT_NULL(r.error);
    ASSERT_GTE(r.count, 1);
    ASSERT_EQ(r.tokens[0].type, TOK_IDENT);
    ASSERT_EQ((int)strlen(r.tokens[0].text), length);

    cbm_lex_free(&r);
    free(identifier);
    PASS();
}

TEST(cypher_lex_rejects_unterminated_string) {
    cbm_lex_result_t r = {0};
    ASSERT_EQ(cbm_lex("RETURN 'unterminated", &r), CBM_NOT_FOUND);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "unterminated string literal"));
    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_rejects_unterminated_block_comment) {
    cbm_lex_result_t r = {0};
    ASSERT_EQ(cbm_lex("MATCH (n) /* unterminated", &r), CBM_NOT_FOUND);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "unterminated block comment"));
    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_rejects_unknown_character) {
    cbm_lex_result_t r = {0};
    ASSERT_EQ(cbm_lex("MATCH (n) @ RETURN n", &r), CBM_NOT_FOUND);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "unsupported character '@'"));
    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_allocation_failures_are_atomic) {
    static const struct {
        cbm_cypher_test_lex_alloc_site_t site;
        const char *query;
        const char *expected_error;
        int successful_before;
        int expected_published_tokens;
    } cases[] = {
        {CBM_CYPHER_TEST_LEX_ALLOC_TOKEN_ARRAY, "MATCH", "growing Cypher token storage", 0, 0},
        {CBM_CYPHER_TEST_LEX_ALLOC_TOKEN_TEXT, "MATCH RETURN", "copying Cypher token text", 1,
         1},
        {CBM_CYPHER_TEST_LEX_ALLOC_STRING_TEXT, "RETURN 'value'",
         "decoding Cypher string literal", 0, 1},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        cbm_lex_result_t r = {0};
        cbm_cypher_test_fail_lex_allocation(cases[i].site, cases[i].successful_before);
        int rc = cbm_lex(cases[i].query, &r);
        cbm_cypher_test_fail_lex_allocation(CBM_CYPHER_TEST_LEX_ALLOC_NONE, -1);
        ASSERT_EQ(rc, CBM_NOT_FOUND);
        ASSERT_TRUE(r.failed);
        ASSERT_NOT_NULL(r.error);
        ASSERT_NOT_NULL(strstr(r.error, cases[i].expected_error));
        ASSERT_EQ(r.count, cases[i].expected_published_tokens);
        cbm_lex_free(&r);
    }

    size_t growth_query_size = (size_t)CYPHER_TEST_TOKEN_GROWTH_COUNT * PAIR_LEN + SKIP_ONE;
    char *growth_query = malloc(growth_query_size);
    ASSERT_NOT_NULL(growth_query);
    for (int i = 0; i < CYPHER_TEST_TOKEN_GROWTH_COUNT; i++) {
        growth_query[(size_t)i * PAIR_LEN] = 'n';
        growth_query[(size_t)i * PAIR_LEN + SKIP_ONE] = ' ';
    }
    growth_query[growth_query_size - SKIP_ONE] = '\0';
    cbm_lex_result_t growth = {0};
    cbm_cypher_test_fail_lex_allocation(CBM_CYPHER_TEST_LEX_ALLOC_TOKEN_ARRAY, 1);
    int growth_rc = cbm_lex(growth_query, &growth);
    cbm_cypher_test_fail_lex_allocation(CBM_CYPHER_TEST_LEX_ALLOC_NONE, -1);
    ASSERT_EQ(growth_rc, CBM_NOT_FOUND);
    ASSERT_NOT_NULL(growth.error);
    ASSERT_NOT_NULL(strstr(growth.error, "growing Cypher token storage"));
    ASSERT_EQ(growth.count, CBM_SZ_32);
    cbm_lex_free(&growth);
    free(growth_query);

    cbm_lex_result_t recovered = {0};
    ASSERT_EQ(cbm_lex("MATCH", &recovered), 0);
    ASSERT_FALSE(recovered.failed);
    cbm_lex_free(&recovered);
    PASS();
}

TEST(cypher_parse_propagates_lex_error) {
    cbm_query_t *query = NULL;
    char *error = NULL;
    ASSERT_EQ(cbm_cypher_parse("MATCH (n) @ RETURN n", &query, &error), CBM_NOT_FOUND);
    ASSERT_NULL(query);
    ASSERT_NOT_NULL(error);
    ASSERT_NOT_NULL(strstr(error, "unsupported character '@'"));
    free(error);
    PASS();
}

TEST(cypher_lex_number) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("42 3.14", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.count, 2);
    ASSERT_EQ(r.tokens[0].type, TOK_NUMBER);
    ASSERT_STR_EQ(r.tokens[0].text, "42");
    ASSERT_EQ(r.tokens[1].type, TOK_NUMBER);
    ASSERT_STR_EQ(r.tokens[1].text, "3.14");

    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_operators) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("= =~ >= <= ..", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.count, 5);
    ASSERT_EQ(r.tokens[0].type, TOK_EQ);
    ASSERT_EQ(r.tokens[1].type, TOK_EQTILDE);
    ASSERT_EQ(r.tokens[2].type, TOK_GTE);
    ASSERT_EQ(r.tokens[3].type, TOK_LTE);
    ASSERT_EQ(r.tokens[4].type, TOK_DOTDOT);

    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_keywords_case_insensitive) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("match WHERE Return limit", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.tokens[0].type, TOK_MATCH);
    ASSERT_EQ(r.tokens[1].type, TOK_WHERE);
    ASSERT_EQ(r.tokens[2].type, TOK_RETURN);
    ASSERT_EQ(r.tokens[3].type, TOK_LIMIT);

    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_pipe_and_star) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("[:TYPE1|TYPE2*1..3]", &r);
    ASSERT_EQ(rc, 0);

    /* [ : TYPE1 | TYPE2 * 1 .. 3 ] */
    ASSERT_GTE(r.count, 9);
    ASSERT_EQ(r.tokens[3].type, TOK_PIPE);
    ASSERT_EQ(r.tokens[5].type, TOK_STAR);
    ASSERT_EQ(r.tokens[6].type, TOK_NUMBER);
    ASSERT_STR_EQ(r.tokens[6].text, "1");
    ASSERT_EQ(r.tokens[7].type, TOK_DOTDOT);
    ASSERT_EQ(r.tokens[8].type, TOK_NUMBER);
    ASSERT_STR_EQ(r.tokens[8].text, "3");

    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_full_query) {
    const char *q = "MATCH (f:Function)-[:CALLS]->(g:Function) "
                    "WHERE f.name =~ \".*Order.*\" "
                    "RETURN f.name, g.name LIMIT 10";
    cbm_lex_result_t r = {0};
    int rc = cbm_lex(q, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    /* Should have many tokens; just check it doesn't crash */
    ASSERT_GT(r.count, 20);

    cbm_lex_free(&r);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PARSER TESTS
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_parse_simple_node) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function)", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(err);
    ASSERT_NOT_NULL(q);

    ASSERT_EQ(cbm_query_pattern(q).node_count, 1);
    ASSERT_EQ(cbm_query_pattern(q).rel_count, 0);
    ASSERT_STR_EQ(cbm_query_pattern(q).nodes[0].variable, "f");
    ASSERT_STR_EQ(cbm_query_pattern(q).nodes[0].label, "Function");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_relationship_outbound) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function)-[:CALLS]->(g:Function)", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);

    ASSERT_EQ(cbm_query_pattern(q).node_count, 2);
    ASSERT_EQ(cbm_query_pattern(q).rel_count, 1);
    ASSERT_STR_EQ(cbm_query_pattern(q).rels[0].types[0], "CALLS");
    ASSERT_STR_EQ(cbm_query_pattern(q).rels[0].direction, "outbound");
    ASSERT_EQ(cbm_query_pattern(q).rels[0].min_hops, 1);
    ASSERT_EQ(cbm_query_pattern(q).rels[0].max_hops, 1);

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_relationship_inbound) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function)<-[:CALLS]-(g:Function)", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);

    ASSERT_STR_EQ(cbm_query_pattern(q).rels[0].direction, "inbound");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_relationship_any) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function)-[:CALLS]-(g:Function)", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);

    ASSERT_STR_EQ(cbm_query_pattern(q).rels[0].direction, "any");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_variable_length) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function)-[:CALLS*1..3]->(g:Function)", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);

    ASSERT_EQ(cbm_query_pattern(q).rels[0].min_hops, 1);
    ASSERT_EQ(cbm_query_pattern(q).rels[0].max_hops, 3);

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_variable_length_unbounded) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f)-[:CALLS*]->(g)", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);

    ASSERT_EQ(cbm_query_pattern(q).rels[0].min_hops, 1);
    ASSERT_EQ(cbm_query_pattern(q).rels[0].max_hops, CBM_CYPHER_HOPS_UNBOUNDED);

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_rejects_unsupported_variable_length_relationship_variable) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    ASSERT_NEQ(cbm_cypher_parse("MATCH (a)-[r:CALLS*]->(b) RETURN b.name", &q, &err), 0);
    ASSERT_NULL(q);
    ASSERT_NOT_NULL(err);
    ASSERT_NOT_NULL(strstr(err, "variable-length relationship variables"));
    free(err);
    PASS();
}

TEST(cypher_parse_variable_length_single_bound_and_zero_range) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    ASSERT_EQ(cbm_cypher_parse("MATCH (f)-[:CALLS*3]->(g)", &q, &err), 0);
    /* Cypher variable-length fixed bounds are exact: *N is *N..N. */
    ASSERT_EQ(cbm_query_pattern(q).rels[0].min_hops, 3);
    ASSERT_EQ(cbm_query_pattern(q).rels[0].max_hops, 3);
    cbm_query_free(q);

    q = NULL;
    ASSERT_EQ(cbm_cypher_parse("MATCH (f)-[:CALLS*0..0]->(g)", &q, &err), 0);
    ASSERT_EQ(cbm_query_pattern(q).rels[0].min_hops, 0);
    ASSERT_EQ(cbm_query_pattern(q).rels[0].max_hops, 0);
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_hop_range_boundaries) {
    const char *queries[] = {
        "MATCH (f)-[:CALLS*2147483648]->(g)",
        "MATCH (f)-[:CALLS*1..2147483648]->(g)",
    };
    for (size_t i = 0; i < sizeof(queries) / sizeof(queries[0]); i++) {
        cbm_query_t *q = NULL;
        char *err = NULL;
        ASSERT_NEQ(cbm_cypher_parse(queries[i], &q, &err), 0);
        ASSERT_NULL(q);
        ASSERT_NOT_NULL(err);
        ASSERT_NOT_NULL(strstr(err, "hop range"));
        free(err);
    }

    /* The openCypher TCK defines an empty interval as a valid pattern that
     * produces no matches; it is not a parse error. */
    cbm_query_t *q = NULL;
    char *err = NULL;
    ASSERT_EQ(cbm_cypher_parse("MATCH (f)-[:CALLS*3..2]->(g)", &q, &err), 0);
    ASSERT_NOT_NULL(q);
    ASSERT_EQ(cbm_query_pattern(q).rels[0].min_hops, 3);
    ASSERT_EQ(cbm_query_pattern(q).rels[0].max_hops, 2);
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_multiple_edge_types) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f)-[:CALLS|HTTP_CALLS]->(g)", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);

    ASSERT_EQ(cbm_query_pattern(q).rels[0].type_count, 2);
    ASSERT_STR_EQ(cbm_query_pattern(q).rels[0].types[0], "CALLS");
    ASSERT_STR_EQ(cbm_query_pattern(q).rels[0].types[1], "HTTP_CALLS");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_where_clause) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) WHERE f.name = \"Foo\"", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);
    ASSERT_NOT_NULL(q->where);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_CONDITION);
    ASSERT_STR_EQ(q->where->root->cond.variable, "f");
    ASSERT_STR_EQ(q->where->root->cond.property, "name");
    ASSERT_STR_EQ(q->where->root->cond.op, "=");
    ASSERT_STR_EQ(q->where->root->cond.value, "Foo");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_where_regex) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) WHERE f.name =~ \".*Order.*\"", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_CONDITION);
    ASSERT_STR_EQ(q->where->root->cond.op, "=~");
    ASSERT_STR_EQ(q->where->root->cond.value, ".*Order.*");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_where_and) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) WHERE f.name = \"A\" AND f.label = \"Function\"",
                              &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_AND);
    ASSERT_NOT_NULL(q->where->root->left);
    ASSERT_NOT_NULL(q->where->root->right);

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_return_simple) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) RETURN f.name, f.qualified_name", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->ret);
    ASSERT_EQ(q->ret->count, 2);
    ASSERT_STR_EQ(q->ret->items[0].variable, "f");
    ASSERT_STR_EQ(q->ret->items[0].property, "name");
    ASSERT_STR_EQ(q->ret->items[1].property, "qualified_name");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_return_count) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f)-[:CALLS]->(g) RETURN f.name, COUNT(g) AS cnt", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(q->ret->count, 2);
    ASSERT_NOT_NULL(q->ret->items[1].func);
    ASSERT_STR_EQ(q->ret->items[1].func, "COUNT");
    ASSERT_STR_EQ(q->ret->items[1].alias, "cnt");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_return_order_limit) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("MATCH (f:Function) RETURN f.name ORDER BY f.name DESC LIMIT 5", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(q->ret->order_count, 1);
    ASSERT_STR_EQ(q->ret->order_items[0].expression, "f.name");
    ASSERT_STR_EQ(q->ret->order_items[0].direction, "DESC");
    ASSERT_EQ(q->ret->limit, 5);

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_preserves_query_sized_label) {
    static const char prefix[] = "MATCH (n:";
    static const char suffix[] = ") RETURN n";
    const size_t label_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    const size_t query_size = (sizeof(prefix) - SKIP_ONE) + label_length + sizeof(suffix);
    char *query = malloc(query_size);
    ASSERT_NOT_NULL(query);
    size_t offset = 0;
    memcpy(query + offset, prefix, sizeof(prefix) - SKIP_ONE);
    offset += sizeof(prefix) - SKIP_ONE;
    memset(query + offset, 'L', label_length);
    offset += label_length;
    memcpy(query + offset, suffix, sizeof(suffix));

    cbm_query_t *parsed = NULL;
    char *error = NULL;
    ASSERT_EQ(cbm_cypher_parse(query, &parsed, &error), 0);
    ASSERT_NULL(error);
    ASSERT_NOT_NULL(parsed);
    ASSERT_NOT_NULL(cbm_query_pattern(parsed).nodes[0].label);
    ASSERT_EQ(strlen(cbm_query_pattern(parsed).nodes[0].label), label_length);
    for (size_t i = 0; i < label_length; i++) {
        ASSERT_EQ(cbm_query_pattern(parsed).nodes[0].label[i], 'L');
    }

    cbm_query_free(parsed);
    free(query);
    PASS();
}

TEST(cypher_parse_preserves_query_sized_case_value_reference) {
    static const char prefix[] = "MATCH (n) RETURN CASE WHEN n.name = 'x' THEN ";
    static const char suffix[] = ".property ELSE false END AS value";
    const size_t variable_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    const size_t expected_length = variable_length + sizeof(".property") - SKIP_ONE;
    const size_t query_size = (sizeof(prefix) - SKIP_ONE) + variable_length + sizeof(suffix);
    char *query = malloc(query_size);
    ASSERT_NOT_NULL(query);
    size_t offset = 0;
    memcpy(query + offset, prefix, sizeof(prefix) - SKIP_ONE);
    offset += sizeof(prefix) - SKIP_ONE;
    memset(query + offset, 'v', variable_length);
    offset += variable_length;
    memcpy(query + offset, suffix, sizeof(suffix));

    cbm_query_t *parsed = NULL;
    char *error = NULL;
    ASSERT_EQ(cbm_cypher_parse(query, &parsed, &error), 0);
    ASSERT_NULL(error);
    ASSERT_NOT_NULL(parsed);
    ASSERT_NOT_NULL(parsed->ret);
    ASSERT_NOT_NULL(parsed->ret->items[0].kase);
    ASSERT_EQ(parsed->ret->items[0].kase->branch_count, 1);
    const char *then_value = parsed->ret->items[0].kase->branches[0].then_val;
    ASSERT_NOT_NULL(then_value);
    ASSERT_EQ(strlen(then_value), expected_length);
    ASSERT_STR_EQ(then_value + variable_length, ".property");

    cbm_query_free(parsed);
    free(query);
    PASS();
}

/* #1334: every ORDER BY key is parsed (per-key direction) and the LIMIT that
 * follows the key list is consumed instead of silently dropped. */
TEST(cypher_parse_multikey_order_by_issue1334) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse(
        "MATCH (f:Function) RETURN f.name, f.complexity "
        "ORDER BY f.complexity DESC, f.name ASC LIMIT 5",
        &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(q->ret->order_count, 2);
    ASSERT_STR_EQ(q->ret->order_items[0].expression, "f.complexity");
    ASSERT_STR_EQ(q->ret->order_items[0].direction, "DESC");
    ASSERT_STR_EQ(q->ret->order_items[1].expression, "f.name");
    ASSERT_STR_EQ(q->ret->order_items[1].direction, "ASC");
    ASSERT_EQ(q->ret->limit, 5);

    cbm_query_free(q);
    PASS();
}

/* #1334: the dynamic ORDER BY model preserves keys beyond the former fixed
 * cap and still consumes the following LIMIT. */
TEST(cypher_parse_order_by_beyond_legacy_cap_issue1334) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) RETURN * ORDER BY "
                              "f.a, f.b, f.c, f.d, f.e, f.f, f.g, f.h, f.i LIMIT 5",
                              &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(err);
    ASSERT_EQ(q->ret->order_count, 9);
    ASSERT_STR_EQ(q->ret->order_items[8].expression, "f.i");
    ASSERT_EQ(q->ret->limit, 5);
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_return_distinct) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) RETURN DISTINCT f.label", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT(q->ret->distinct);

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_inline_props) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function {name: \"Foo\"})", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(cbm_query_pattern(q).nodes[0].prop_count, 1);
    ASSERT_STR_EQ(cbm_query_pattern(q).nodes[0].props[0].key, "name");
    ASSERT_STR_EQ(cbm_query_pattern(q).nodes[0].props[0].value, "Foo");

    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_error) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("INVALID QUERY", &q, &err);
    ASSERT_EQ(rc, -1);
    ASSERT_NOT_NULL(err);
    free(err);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  EXECUTION TESTS (end-to-end against store)
 * ══════════════════════════════════════════════════════════════════ */

/* Helper: set up the standard test graph.
 * Nodes: HandleOrder, ValidateOrder, SubmitOrder (Function), main (Module), LogError (Function)
 * Edges: HandleOrder→ValidateOrder (CALLS), ValidateOrder→SubmitOrder (CALLS),
 *        HandleOrder→LogError (CALLS), main→HandleOrder (DEFINES)
 */
static cbm_store_t *setup_cypher_store(void) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "HandleOrder",
                     .qualified_name = "test.HandleOrder",
                     .file_path = "handler.go",
                     .start_line = 10,
                     .end_line = 30};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "ValidateOrder",
                     .qualified_name = "test.ValidateOrder",
                     .file_path = "validate.go",
                     .start_line = 5,
                     .end_line = 15};
    cbm_node_t n3 = {.project = "test",
                     .label = "Function",
                     .name = "SubmitOrder",
                     .qualified_name = "test.SubmitOrder",
                     .file_path = "submit.go"};
    cbm_node_t n4 = {
        .project = "test", .label = "Module", .name = "main", .qualified_name = "test.main"};
    cbm_node_t n5 = {.project = "test",
                     .label = "Function",
                     .name = "LogError",
                     .qualified_name = "test.LogError",
                     .file_path = "log.go",
                     .properties_json = "{\"empty_value\":\"\",\"null_value\":null}"};

    int64_t id1 = cbm_store_upsert_node(s, &n1);
    int64_t id2 = cbm_store_upsert_node(s, &n2);
    int64_t id3 = cbm_store_upsert_node(s, &n3);
    int64_t id4 = cbm_store_upsert_node(s, &n4);
    int64_t id5 = cbm_store_upsert_node(s, &n5);

    cbm_edge_t e1 = {.project = "test", .source_id = id1, .target_id = id2, .type = "CALLS"};
    cbm_edge_t e2 = {.project = "test", .source_id = id2, .target_id = id3, .type = "CALLS"};
    cbm_edge_t e3 = {.project = "test", .source_id = id1, .target_id = id5, .type = "CALLS"};
    cbm_edge_t e4 = {.project = "test", .source_id = id4, .target_id = id1, .type = "DEFINES"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);
    cbm_store_insert_edge(s, &e3);
    cbm_store_insert_edge(s, &e4);

    return s;
}

/* The public capability registry is the stable language contract, not merely
 * MCP prose. Execute every advertised function name through the real parser
 * and evaluator so a registry/parser drift fails locally. There are a fixed C
 * names and a bounded fixture; runtime is O(C) and retained memory is O(1)
 * beyond each normal one-row query result. Clause/pattern/predicate semantics
 * have dedicated tests below because synthesizing those from prose would
 * create a second parser in the test suite. */
TEST(cypher_capability_registry_functions_are_executable) {
    const cbm_cypher_capability_schema_t *schema = cbm_cypher_capability_schema();
    ASSERT_NOT_NULL(schema);
    ASSERT_TRUE(schema == cbm_cypher_capability_schema());
    ASSERT_STR_EQ(schema->schema_id, CBM_CYPHER_CAPABILITY_SCHEMA_ID);
    ASSERT_EQ(schema->version, CBM_CYPHER_CAPABILITY_SCHEMA_VERSION);

    cbm_store_t *store = setup_cypher_store();
    ASSERT_NOT_NULL(store);
    char query[CBM_SZ_512];

    for (size_t i = 0; i < schema->aggregate_function_count; i++) {
        const char *name = schema->aggregate_functions[i];
        int written =
            strcmp(name, "count") == 0
                ? snprintf(query, sizeof(query), "MATCH (n) RETURN %s(*)", name)
                : snprintf(query, sizeof(query), "MATCH (n) RETURN %s(n.start_line)", name);
        ASSERT_GT(written, 0);
        ASSERT_LT((size_t)written, sizeof(query));
        cbm_cypher_result_t result = {0};
        ASSERT_EQ(cbm_cypher_execute(store, query, "test", 1, &result), CBM_STORE_OK);
        ASSERT_NULL(result.error);
        cbm_cypher_result_free(&result);
    }

    for (size_t i = 0; i < schema->keyword_scalar_function_count; i++) {
        int written = snprintf(query, sizeof(query), "MATCH (n) RETURN %s(n.name) LIMIT 1",
                               schema->keyword_scalar_functions[i]);
        ASSERT_GT(written, 0);
        ASSERT_LT((size_t)written, sizeof(query));
        cbm_cypher_result_t result = {0};
        ASSERT_EQ(cbm_cypher_execute(store, query, "test", 1, &result), CBM_STORE_OK);
        ASSERT_NULL(result.error);
        cbm_cypher_result_free(&result);
    }

    for (size_t i = 0; i < schema->named_scalar_function_count; i++) {
        const char *name = schema->named_scalar_functions[i];
        int written;
        if (strcmp(name, "type") == 0) {
            written = snprintf(query, sizeof(query), "MATCH ()-[r]->() RETURN %s(r) LIMIT 1", name);
        } else if (strcmp(name, "labels") == 0 || strcmp(name, "id") == 0 ||
                   strcmp(name, "keys") == 0 || strcmp(name, "properties") == 0) {
            written = snprintf(query, sizeof(query), "MATCH (n) RETURN %s(n) LIMIT 1", name);
        } else {
            written = snprintf(query, sizeof(query), "MATCH (n) RETURN %s(n.name) LIMIT 1", name);
        }
        ASSERT_GT(written, 0);
        ASSERT_LT((size_t)written, sizeof(query));
        cbm_cypher_result_t result = {0};
        ASSERT_EQ(cbm_cypher_execute(store, query, "test", 1, &result), CBM_STORE_OK);
        ASSERT_NULL(result.error);
        cbm_cypher_result_free(&result);
    }

    for (size_t i = 0; i < schema->multi_argument_function_count; i++) {
        const char *name = schema->multi_argument_functions[i];
        int written;
        if (strcmp(name, "coalesce") == 0) {
            written = snprintf(query, sizeof(query),
                               "MATCH (n) RETURN %s(n.name, 'fallback') LIMIT 1", name);
        } else if (strcmp(name, "substring") == 0) {
            written =
                snprintf(query, sizeof(query), "MATCH (n) RETURN %s(n.name, 0, 1) LIMIT 1", name);
        } else if (strcmp(name, "replace") == 0) {
            written = snprintf(query, sizeof(query),
                               "MATCH (n) RETURN %s(n.name, 'a', 'b') LIMIT 1", name);
        } else {
            written =
                snprintf(query, sizeof(query), "MATCH (n) RETURN %s(n.name, 1) LIMIT 1", name);
        }
        ASSERT_GT(written, 0);
        ASSERT_LT((size_t)written, sizeof(query));
        cbm_cypher_result_t result = {0};
        ASSERT_EQ(cbm_cypher_execute(store, query, "test", 1, &result), CBM_STORE_OK);
        ASSERT_NULL(result.error);
        cbm_cypher_result_free(&result);
    }

    cbm_store_close(store);
    PASS();
}

TEST(cypher_exec_preserves_query_sized_order_expression_and_column) {
    static const char match_prefix[] = "MATCH (";
    static const char return_prefix[] = ":Function) RETURN ";
    static const char order_prefix[] = ".name ORDER BY ";
    static const char suffix[] = ".name DESC";
    const size_t variable_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    const size_t expected_expression_length = variable_length + sizeof(".name") - SKIP_ONE;
    const size_t query_size = (sizeof(match_prefix) - SKIP_ONE) +
                              (sizeof(return_prefix) - SKIP_ONE) +
                              (sizeof(order_prefix) - SKIP_ONE) + sizeof(suffix) +
                              variable_length * CYPHER_TEST_REPEATED_VARIABLE_COUNT;
    char *query = malloc(query_size);
    ASSERT_NOT_NULL(query);
    size_t offset = 0;
    memcpy(query + offset, match_prefix, sizeof(match_prefix) - SKIP_ONE);
    offset += sizeof(match_prefix) - SKIP_ONE;
    memset(query + offset, 'v', variable_length);
    offset += variable_length;
    memcpy(query + offset, return_prefix, sizeof(return_prefix) - SKIP_ONE);
    offset += sizeof(return_prefix) - SKIP_ONE;
    memset(query + offset, 'v', variable_length);
    offset += variable_length;
    memcpy(query + offset, order_prefix, sizeof(order_prefix) - SKIP_ONE);
    offset += sizeof(order_prefix) - SKIP_ONE;
    memset(query + offset, 'v', variable_length);
    offset += variable_length;
    memcpy(query + offset, suffix, sizeof(suffix));

    cbm_query_t *parsed = NULL;
    char *error = NULL;
    ASSERT_EQ(cbm_cypher_parse(query, &parsed, &error), 0);
    ASSERT_NULL(error);
    ASSERT_NOT_NULL(parsed);
    ASSERT_EQ(parsed->ret->order_count, 1);
    ASSERT_EQ(strlen(parsed->ret->order_items[0].expression), expected_expression_length);
    cbm_query_free(parsed);

    cbm_store_t *store = setup_cypher_store();
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, query, "test", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.col_count, 1);
    ASSERT_EQ(strlen(result.columns[0]), expected_expression_length);
    ASSERT_EQ(result.row_count, 4);

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    free(query);
    PASS();
}

TEST(cypher_exec_preserves_query_sized_star_and_default_columns) {
    static const char prefix[] = "MATCH (";
    static const char star_suffix[] = ":Function) RETURN *";
    static const char default_suffix[] = ":Function)";
    static const char *const star_column_suffixes[CYPHER_TEST_STAR_COLUMN_COUNT] = {
        ".name", ".qualified_name", ".label", ".file_path"};
    static const char *const default_column_suffixes[CYPHER_TEST_DEFAULT_COLUMN_COUNT] = {
        ".name", ".qualified_name", ".label"};
    const size_t variable_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    size_t star_size = sizeof(prefix) - SKIP_ONE + variable_length + sizeof(star_suffix);
    size_t default_size = sizeof(prefix) - SKIP_ONE + variable_length + sizeof(default_suffix);
    char *star_query = malloc(star_size);
    char *default_query = malloc(default_size);
    ASSERT_NOT_NULL(star_query);
    ASSERT_NOT_NULL(default_query);
    memcpy(star_query, prefix, sizeof(prefix) - SKIP_ONE);
    memset(star_query + sizeof(prefix) - SKIP_ONE, 'v', variable_length);
    memcpy(star_query + sizeof(prefix) - SKIP_ONE + variable_length, star_suffix,
           sizeof(star_suffix));
    memcpy(default_query, prefix, sizeof(prefix) - SKIP_ONE);
    memset(default_query + sizeof(prefix) - SKIP_ONE, 'v', variable_length);
    memcpy(default_query + sizeof(prefix) - SKIP_ONE + variable_length, default_suffix,
           sizeof(default_suffix));

    cbm_store_t *store = setup_cypher_store();
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, star_query, "test", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.col_count, CYPHER_TEST_STAR_COLUMN_COUNT);
    for (int i = 0; i < CYPHER_TEST_STAR_COLUMN_COUNT; i++) {
        ASSERT_EQ(strlen(result.columns[i]), variable_length + strlen(star_column_suffixes[i]));
        ASSERT_EQ(memcmp(result.columns[i], star_query + sizeof(prefix) - SKIP_ONE,
                         variable_length),
                  0);
        ASSERT_STR_EQ(result.columns[i] + variable_length, star_column_suffixes[i]);
    }
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(store, default_query, "test", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.col_count, CYPHER_TEST_DEFAULT_COLUMN_COUNT);
    for (int i = 0; i < CYPHER_TEST_DEFAULT_COLUMN_COUNT; i++) {
        ASSERT_EQ(strlen(result.columns[i]),
                  variable_length + strlen(default_column_suffixes[i]));
        ASSERT_EQ(memcmp(result.columns[i], default_query + sizeof(prefix) - SKIP_ONE,
                         variable_length),
                  0);
        ASSERT_STR_EQ(result.columns[i] + variable_length, default_column_suffixes[i]);
    }
    cbm_cypher_result_free(&result);

    cbm_store_close(store);
    free(default_query);
    free(star_query);
    PASS();
}

typedef struct {
    bool saw_file_contains_pushdown;
} cypher_sql_trace_t;

static int cypher_sql_trace(unsigned trace_type, void *context, void *statement, void *sql_text) {
    (void)statement;
    if (trace_type == SQLITE_TRACE_STMT && context && sql_text &&
        strstr((const char *)sql_text, "instr(n.file_path")) {
        ((cypher_sql_trace_t *)context)->saw_file_contains_pushdown = true;
    }
    return 0;
}

TEST(cypher_exec_file_contains_pushes_down_beyond_seed_window) {
    enum { NAME_SIZE = 32, QN_SIZE = 64 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    /* max_rows=1 historically seeded only 10 unfiltered nodes, then evaluated
     * WHERE in C. Put the sole match after that window to prove both exactness
     * and SQL pushdown; '%' and '_' must remain literal CONTAINS characters. */
    for (int i = 0; i < 12; i++) {
        char name[NAME_SIZE];
        char qn[QN_SIZE];
        snprintf(name, sizeof(name), "unrelated_%02d", i);
        snprintf(qn, sizeof(qn), "test.%s", name);
        cbm_node_t node = {.project = "test",
                           .label = "Function",
                           .name = name,
                           .qualified_name = qn,
                           .file_path = "src/unrelated.c"};
        ASSERT_GT(cbm_store_upsert_node(s, &node), 0);
    }
    cbm_node_t target = {.project = "test",
                         .label = "Function",
                         .name = "target",
                         .qualified_name = "test.target",
                         .file_path = "src/100%_done/target.c"};
    ASSERT_GT(cbm_store_upsert_node(s, &target), 0);

    cypher_sql_trace_t trace = {0};
    sqlite3 *db = cbm_store_get_db(s);
    ASSERT_NOT_NULL(db);
    ASSERT_EQ(sqlite3_trace_v2(db, SQLITE_TRACE_STMT, cypher_sql_trace, &trace), SQLITE_OK);

    cbm_cypher_result_t r = {0};
    int rc =
        cbm_cypher_execute(s,
                           "MATCH (n) WHERE n.file_path CONTAINS '100%_done' AND n.name = 'target' "
                           "RETURN n.name, n.file_path LIMIT 1",
                           "test", 1, &r);
    ASSERT_EQ(sqlite3_trace_v2(db, 0, NULL, NULL), SQLITE_OK);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(trace.saw_file_contains_pushdown);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "target");
    ASSERT_STR_EQ(r.rows[0][1], "src/100%_done/target.c");
    cbm_cypher_result_free(&r);

    /* A file predicate below OR is not mandatory. Pushing it would remove the
     * valid name branch and change Cypher semantics. */
    trace.saw_file_contains_pushdown = false;
    ASSERT_EQ(sqlite3_trace_v2(db, SQLITE_TRACE_STMT, cypher_sql_trace, &trace), SQLITE_OK);
    rc = cbm_cypher_execute(
        s,
        "MATCH (n) WHERE n.file_path CONTAINS 'never-present' OR n.name = 'unrelated_00' "
        "RETURN n.name LIMIT 1",
        "test", 1, &r);
    ASSERT_EQ(sqlite3_trace_v2(db, 0, NULL, NULL), SQLITE_OK);
    ASSERT_EQ(rc, 0);
    ASSERT_FALSE(trace.saw_file_contains_pushdown);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "unrelated_00");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_output_cap_does_not_limit_predicate_scan) {
    enum { NAME_SIZE = 32, QN_SIZE = 64, UNRELATED_COUNT = 64 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    /* max_rows is an output bound, not a search-effort bound. The historical
     * max_rows * 10 seed window silently missed ordinary matches in projects
     * larger than that window. Keep this fixture large enough to reproduce the
     * practical failure while remaining cheap under sanitizers. */
    for (int i = 0; i < UNRELATED_COUNT; i++) {
        char name[NAME_SIZE];
        char qn[QN_SIZE];
        snprintf(name, sizeof(name), "unrelated_%02d", i);
        snprintf(qn, sizeof(qn), "test.%s", name);
        cbm_node_t node = {.project = "test",
                           .label = "Function",
                           .name = name,
                           .qualified_name = qn,
                           .file_path = "src/unrelated.c"};
        ASSERT_GT(cbm_store_upsert_node(s, &node), 0);
    }
    cbm_node_t target = {.project = "test",
                         .label = "Function",
                         .name = "zz_target_after_output_window",
                         .qualified_name = "test.zz_target_after_output_window",
                         .file_path = "src/target.c"};
    ASSERT_GT(cbm_store_upsert_node(s, &target), 0);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (n) WHERE n.name = 'zz_target_after_output_window' RETURN n.name LIMIT 1", "test",
        5, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "zz_target_after_output_window");
    cbm_cypher_result_free(&r);

    /* ORDER BY selects from the eligible set before LIMIT. Ranking only the
     * old prefix would return unrelated_49 instead of the global top row. */
    rc = cbm_cypher_execute(s, "MATCH (n) RETURN n.name ORDER BY n.name DESC LIMIT 1", "test", 5,
                            &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "zz_target_after_output_window");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* The query string is caller-supplied and the WHERE grammar recurses once per
 * nested '(' and once per NOT, with no depth counter between the MCP entry point
 * and the recursive descent. A few tens of KB of '(' therefore exhausted the
 * stack at parse time. Parse in a forked child so the crash surfaces as a
 * killing signal rather than taking the test runner with it; a bounded parser
 * must reject the query cleanly instead. */
TEST(cypher_deep_nesting_rejected_not_crash) {
#ifdef _WIN32
    SKIP_PLATFORM("fork crash-isolation is POSIX-only; the depth cap is platform-agnostic");
#else
    enum { NEST = 30000 };
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        char *q = malloc(NEST * 2 + 64);
        if (!q) {
            _exit(2);
        }
        int n = snprintf(q, 64, "MATCH (f:Function) WHERE ");
        for (int i = 0; i < NEST; i++) {
            q[n++] = '(';
        }
        n += snprintf(q + n, 32, "f.name = \"x\"");
        for (int i = 0; i < NEST; i++) {
            q[n++] = ')';
        }
        q[n] = '\0';
        cbm_store_t *s = setup_cypher_store();
        cbm_cypher_result_t r = {0};
        /* Any clean outcome is acceptable — success or a parse error. Only a
         * crash is a failure, and that is what the signal check below catches. */
        (void)cbm_cypher_execute(s, q, "test", 0, &r);
        cbm_cypher_result_free(&r);
        cbm_store_close(s);
        free(q);
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFSIGNALED(status)) {
        char m[96];
        snprintf(m, sizeof(m), "parser killed by signal %d — unbounded recursion depth",
                 WTERMSIG(status));
        FAIL(m);
    }
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    PASS();
#endif
}

TEST(cypher_exec_match_all_functions) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s, "MATCH (f:Function)", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4); /* HandleOrder, ValidateOrder, SubmitOrder, LogError */
    ASSERT_GT(r.col_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Regression: an OPTIONAL MATCH whose label matches zero nodes drove
 * cross_join_nodes with extra_count == 0. The old allocation
 * (bind_count * 0 + 1) reserved a single binding slot, but the OPTIONAL
 * fallback then wrote one binding per existing row — a heap buffer overflow
 * once the first MATCH bound more than one node (ASan: heap-buffer-overflow).
 * The fork executor avoids that full-product allocation entirely: it grows
 * geometrically only to the max_new working-row budget. The compatibility
 * arithmetic for the former allocation contract is exercised independently by
 * cypher_cross_join_alloc_rejects_overflow below.
 * The query text is agent-controlled via the MCP query tool. */
TEST(cypher_exec_optional_empty_label_no_overflow) {
    cbm_store_t *s = setup_cypher_store(); /* 4 Function nodes */
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(
        s, "MATCH (a:Function) OPTIONAL MATCH (b:NoSuchLabel) RETURN a.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* One row per Function, each with b left unbound (dead-code semantics). */
    ASSERT_EQ(r.row_count, 4);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Output limits must not become hidden matching limits. This query expands far
 * more rows than max_output_rows, then applies the output cap after matching;
 * the geometrically grown binding array must remain memory-safe under ASan. */
TEST(cypher_exec_optional_rel_output_limit_does_not_bound_matching) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    enum { LEAF_COUNT = 20, CALLEE_COUNT = 300, OUTPUT_ROWS = 5 };
    /* The hub is inserted first and produces many matches; the leaves exercise
     * null-extension after that expansion without tying working memory to the
     * much smaller requested output window. */
    cbm_node_t hub = {
        .project = "test", .label = "Function", .name = "hub", .qualified_name = "test.hub"};
    int64_t hub_id = cbm_store_upsert_node(s, &hub);
    for (int i = 0; i < LEAF_COUNT; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "leaf%02d", i);
        snprintf(qn, sizeof(qn), "test.leaf%02d", i);
        cbm_node_t leaf = {
            .project = "test", .label = "Function", .name = nm, .qualified_name = qn};
        cbm_store_upsert_node(s, &leaf);
    }

    for (int i = 0; i < CALLEE_COUNT; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "callee%d", i);
        snprintf(qn, sizeof(qn), "test.callee%d", i);
        cbm_node_t callee = {.project = "test", .label = "Var", .name = nm, .qualified_name = qn};
        int64_t cid = cbm_store_upsert_node(s, &callee);
        cbm_edge_t e = {.project = "test", .source_id = hub_id, .target_id = cid, .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (a:Function) OPTIONAL MATCH (a)-[:CALLS]->(b) RETURN a.name", "test",
        OUTPUT_ROWS, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, OUTPUT_ROWS);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Reproduce-first: after the working-row budget is exhausted, OPTIONAL MATCH
 * must fail explicitly rather than FABRICATE a "no match" row for a source
 * that genuinely has matches.
 *
 * process_edges / expand_var_length used to carry the budget in the LOOP
 * condition (`ei < edge_count && *new_count < max_new`), so once new_count hit
 * max_new they stopped iterating entirely and never incremented match_count —
 * even though neighbours existed. expand_pattern_rels' ungated fallback then saw
 * match_count == 0 and emitted an unbound row. `WHERE b IS NULL` reads that as
 * "nothing points here", so a dead-code query reported LIVE code as dead.
 *
 * Shape: A saturates the explicit budget, B has no callees, and C has five.
 * The null filter follows WITH so it cannot be pushed into relationship
 * candidate evaluation: this deliberately exercises real intermediate-row
 * exhaustion rather than rejecting exact O(E)-time predicate pushdown that
 * avoids materializing non-qualifying rows. Returning a partial success could
 * misclassify C as dead; the fork's stable contract returns an actionable
 * error instead. */
TEST(cypher_exec_optional_saturated_does_not_fabricate_no_match) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    enum { A_CALLEE_COUNT = 40, C_CALLEE_COUNT = 5, WORKING_ROWS = 30 };
    cbm_node_t a = {.project = "test", .label = "Function", .name = "A", .qualified_name = "test.A"};
    cbm_node_t b = {.project = "test", .label = "Function", .name = "B", .qualified_name = "test.B"};
    cbm_node_t c = {.project = "test", .label = "Function", .name = "C", .qualified_name = "test.C"};
    int64_t a_id = cbm_store_upsert_node(s, &a);
    (void)cbm_store_upsert_node(s, &b); /* B: no outgoing CALLS at all */
    int64_t c_id = cbm_store_upsert_node(s, &c);
    ASSERT_GT(a_id, 0);
    ASSERT_GT(c_id, 0);

    /* Callees are label Var so they do not inflate scan_count/bind_cap. */
    for (int i = 0; i < A_CALLEE_COUNT; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "acallee%d", i);
        snprintf(qn, sizeof(qn), "test.acallee%d", i);
        cbm_node_t t = {.project = "test", .label = "Var", .name = nm, .qualified_name = qn};
        int64_t tid = cbm_store_upsert_node(s, &t);
        cbm_edge_t e = {.project = "test", .source_id = a_id, .target_id = tid, .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }
    for (int i = 0; i < C_CALLEE_COUNT; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "ccallee%d", i);
        snprintf(qn, sizeof(qn), "test.ccallee%d", i);
        cbm_node_t t = {.project = "test", .label = "Var", .name = nm, .qualified_name = qn};
        int64_t tid = cbm_store_upsert_node(s, &t);
        cbm_edge_t e = {.project = "test", .source_id = c_id, .target_id = tid, .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }

    cbm_cypher_result_t r = {0};
    cbm_cypher_limits_t limits = {.max_output_rows = 3, .max_working_rows = WORKING_ROWS};
    int rc = cbm_cypher_execute_with_limits(
        s, "MATCH (f:Function) OPTIONAL MATCH (f)-[:CALLS]->(g) "
           "WITH f, g WHERE g IS NULL RETURN f.name",
        "test", &limits, &r);
    ASSERT_TRUE(rc != 0);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "working-row budget"));

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Arithmetic-boundary companion to the zero-label overflow above. The legacy
 * full cross-join allocation multiplied bind_count by extra_count; validate
 * that compatibility seam in O(1) runtime and O(1) memory without allocating
 * billions of bindings. The active executor grows only to its configured
 * working-row budget. */
TEST(cypher_cross_join_alloc_rejects_overflow) {
    enum {
        CROSS_JOIN_INT_OVERFLOW_FACTOR = 46341,
        NORMAL_BINDING_COUNT = 4,
        NORMAL_EXTRA_COUNT = 3,
    };
    size_t n = 0;

    /* 46341 * 46341 = 2147488281 > INT_MAX (2147483647): pre-fix the int product
     * wrapped negative -> tiny malloc -> heap OOB. Now rejected. */
    ASSERT_TRUE(cbm_cypher_cross_join_alloc(CROSS_JOIN_INT_OVERFLOW_FACTOR,
                                            CROSS_JOIN_INT_OVERFLOW_FACTOR, false, &n) != 0);

    /* A normal join still succeeds: bind_count * extra_count + 1 slots. */
    ASSERT_EQ(cbm_cypher_cross_join_alloc(NORMAL_BINDING_COUNT, NORMAL_EXTRA_COUNT, false, &n), 0);
    ASSERT_EQ(n, (size_t)(NORMAL_BINDING_COUNT * NORMAL_EXTRA_COUNT + 1));

    /* OPTIONAL with no extra nodes reserves one fallback row per binding + 1. */
    ASSERT_EQ(cbm_cypher_cross_join_alloc(NORMAL_BINDING_COUNT, 0, true, &n), 0);
    ASSERT_EQ(n, (size_t)(NORMAL_BINDING_COUNT + 1));

    /* Non-OPTIONAL with no extra nodes: just the sentinel slot. */
    ASSERT_EQ(cbm_cypher_cross_join_alloc(NORMAL_BINDING_COUNT, 0, false, &n), 0);
    ASSERT_EQ(n, (size_t)1);

    PASS();
}

/* Companion to the truncation regression: when the expansion does NOT saturate
 * the ceiling, every leaf's OPTIONAL fallback row must survive with its target
 * unbound. A `row_count > 0` check is too weak — it can pass on hub rows alone —
 * so this asserts a specific leaf appears with an empty b.name, and that a real
 * expanded hub row is present too. */
TEST(cypher_exec_optional_rel_leaf_fallback_survives) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* 1 hub (2 CALLS edges) + 3 leaves (no edges). The executor starts from a
     * small geometric binding capacity and the default working-row budget is
     * far above this six-row result, so this exercises fallback correctness and
     * amortized O(1) append growth rather than the saturation path. */
    cbm_node_t hub = {
        .project = "test", .label = "Function", .name = "hub", .qualified_name = "test.hub"};
    int64_t hub_id = cbm_store_upsert_node(s, &hub);
    for (int i = 0; i < 3; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "leaf%d", i);
        snprintf(qn, sizeof(qn), "test.leaf%d", i);
        cbm_node_t leaf = {
            .project = "test", .label = "Function", .name = nm, .qualified_name = qn};
        cbm_store_upsert_node(s, &leaf);
    }
    for (int i = 0; i < 2; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "callee%d", i);
        snprintf(qn, sizeof(qn), "test.callee%d", i);
        cbm_node_t callee = {.project = "test", .label = "Var", .name = nm, .qualified_name = qn};
        int64_t cid = cbm_store_upsert_node(s, &callee);
        cbm_edge_t e = {.project = "test", .source_id = hub_id, .target_id = cid, .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (a:Function) OPTIONAL MATCH (a)-[:CALLS]->(b) RETURN a.name, b.name", "test", 0,
        &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.col_count, 2);

    /* Scan for a leaf fallback row (a.name = "leaf0", b.name unbound = "") and a
     * real expanded hub row (a.name = "hub", b.name non-empty). */
    bool leaf_fallback = false;
    bool hub_expanded = false;
    for (int i = 0; i < r.row_count; i++) {
        const char *a = r.rows[i][0];
        const char *b = r.rows[i][1];
        if (strcmp(a, "leaf0") == 0 && b[0] == '\0') {
            leaf_fallback = true;
        }
        if (strcmp(a, "hub") == 0 && b[0] != '\0') {
            hub_expanded = true;
        }
    }
    ASSERT_TRUE(leaf_fallback); /* the OPTIONAL no-match row survived */
    ASSERT_TRUE(hub_expanded);  /* the expansion still produced bound rows */

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_eq) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc =
        cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.name = \"HandleOrder\"", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #874: coalesce(var.prop, literal) in WHERE — null-safe numeric filters
 * for audit queries over OPTIONAL graph properties. The parser rejected the
 * call outright ("unexpected operator"); RETURN-side coalesce already
 * worked, so only the WHERE leaf needs it. Semantics: when the property is
 * missing/empty, the literal default is compared instead. */
/* #797: variable-length / repeated-variable path semantics. Fixture:
 * loopy has a SELF-LOOP as one of its outbound CALLS edges plus a real
 * 2-chain loopy->mid->leaf. Ordinary MATCH follows relationship-unique
 * trail semantics:
 *  - a repeated node variable must unify: (a)-[:CALLS]->(a) matches ONLY
 *    the self-loop, not every edge;
 *  - relationship uniqueness within a path: the self-loop cannot be
 *    traversed repeatedly, but it may be used once before the real chain;
 *  - an implementation work budget must error rather than fabricate or
 *    silently truncate results. */
TEST(cypher_exec_varlength_path_semantics_issue797) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t loopy = {.project = "test",
                        .label = "Function",
                        .name = "loopy",
                        .qualified_name = "test.mod.loopy",
                        .file_path = "mod.go",
                        .start_line = 1,
                        .end_line = 2};
    cbm_node_t mid = {.project = "test",
                      .label = "Function",
                      .name = "mid",
                      .qualified_name = "test.mod.mid",
                      .file_path = "mod.go",
                      .start_line = 3,
                      .end_line = 4};
    cbm_node_t leaf = {.project = "test",
                       .label = "Function",
                       .name = "leaf",
                       .qualified_name = "test.mod.leaf",
                       .file_path = "mod.go",
                       .start_line = 5,
                       .end_line = 6};
    int64_t id_loopy = cbm_store_upsert_node(s, &loopy);
    int64_t id_mid = cbm_store_upsert_node(s, &mid);
    int64_t id_leaf = cbm_store_upsert_node(s, &leaf);
    ASSERT_GT(id_loopy, 0);
    cbm_edge_t self_loop = {
        .project = "test", .source_id = id_loopy, .target_id = id_loopy, .type = "CALLS"};
    cbm_edge_t e1 = {
        .project = "test", .source_id = id_loopy, .target_id = id_mid, .type = "CALLS"};
    cbm_edge_t e2 = {.project = "test", .source_id = id_mid, .target_id = id_leaf, .type = "CALLS"};
    cbm_store_insert_edge(s, &self_loop);
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);

    /* Bug 1: repeated variable must unify — only the self-loop matches. */
    cbm_cypher_result_t r1 = {0};
    ASSERT_EQ(cbm_cypher_execute(s, "MATCH (a)-[:CALLS]->(a) RETURN a.name", "test", 0, &r1), 0);
    ASSERT_EQ(r1.row_count, 1);
    cbm_cypher_result_free(&r1);

    /* Ordinary MATCH follows Cypher's relationship-unique trail semantics.
     * There are two exact two-hop trails: self-loop then e1 reaches mid, and
     * e1 then e2 reaches leaf. */
    cbm_cypher_result_t r2 = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a {name: \"loopy\"})-[:CALLS*2..2]->(b) "
                                 "RETURN DISTINCT b.name",
                                 "test", 0, &r2),
              0);
    ASSERT_EQ(r2.row_count, 2);
    bool saw_mid = false;
    bool saw_leaf = false;
    for (int i = 0; i < r2.row_count; i++) {
        saw_mid = saw_mid || strcmp(r2.rows[i][0], "mid") == 0;
        saw_leaf = saw_leaf || strcmp(r2.rows[i][0], "leaf") == 0;
    }
    ASSERT_TRUE(saw_mid);
    ASSERT_TRUE(saw_leaf);
    cbm_cypher_result_free(&r2);

    /* A single fixed bound is exact, not the historical 1..N shorthand. */
    cbm_cypher_result_t exact = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a {name: \"loopy\"})-[:CALLS*2]->(b) "
                                 "RETURN DISTINCT b.name",
                                 "test", 0, &exact),
              0);
    ASSERT_EQ(exact.row_count, 2);
    cbm_cypher_result_free(&exact);

    /* Bug 2 amplifier: no directed path of length 5 exists at all. */
    cbm_cypher_result_t r3 = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a {name: \"loopy\"})-[:CALLS*5..5]->(b) "
                                 "RETURN b.name",
                                 "test", 0, &r3),
              0);
    ASSERT_EQ(r3.row_count, 0);
    cbm_cypher_result_free(&r3);

    /* The finite graph proves no shortest endpoint at hop 150; execution
     * terminates by graph exhaustion without an arbitrary hop cap. */
    cbm_cypher_result_t r4 = {0};
    ASSERT_EQ(
        cbm_cypher_execute(s, "MATCH (a)-[:CALLS*150..150]->(b) RETURN b.name", "test", 0, &r4), 0);
    ASSERT_EQ(r4.row_count, 0);
    ASSERT_NULL(r4.warning);
    cbm_cypher_result_free(&r4);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_untyped_variable_length_matches_all_relationship_types) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t result = {0};

    /* main is a Module with a DEFINES edge to HandleOrder and no CALLS edge.
     * The second hop then follows HandleOrder's CALLS edges. Omitting the
     * relationship type must therefore traverse both types, not inherit the
     * separate store traversal API's default-CALLS policy. */
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a:Module {name: \"main\"})-[*1..2]->(b:Function) "
                                 "RETURN b.name",
                                 "test", 0, &result),
              0);
    ASSERT_EQ(result.row_count, 3);
    bool saw_handle = false;
    bool saw_validate = false;
    bool saw_log = false;
    for (int i = 0; i < result.row_count; i++) {
        saw_handle = saw_handle || strcmp(result.rows[i][0], "HandleOrder") == 0;
        saw_validate = saw_validate || strcmp(result.rows[i][0], "ValidateOrder") == 0;
        saw_log = saw_log || strcmp(result.rows[i][0], "LogError") == 0;
    }
    ASSERT_TRUE(saw_handle);
    ASSERT_TRUE(saw_validate);
    ASSERT_TRUE(saw_log);

    cbm_cypher_result_free(&result);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_relationship_uniqueness_spans_entire_pattern) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    cbm_node_t loopy = {
        .project = "test", .label = "Function", .name = "loopy", .qualified_name = "test.loopy"};
    cbm_node_t mid = {
        .project = "test", .label = "Function", .name = "mid", .qualified_name = "test.mid"};
    cbm_node_t leaf = {
        .project = "test", .label = "Function", .name = "leaf", .qualified_name = "test.leaf"};
    int64_t loopy_id = cbm_store_upsert_node(s, &loopy);
    int64_t mid_id = cbm_store_upsert_node(s, &mid);
    int64_t leaf_id = cbm_store_upsert_node(s, &leaf);
    ASSERT_GT(loopy_id, 0);
    ASSERT_GT(mid_id, 0);
    ASSERT_GT(leaf_id, 0);
    cbm_edge_t loop = {
        .project = "test", .source_id = loopy_id, .target_id = loopy_id, .type = "CALLS"};
    cbm_edge_t to_mid = {
        .project = "test", .source_id = loopy_id, .target_id = mid_id, .type = "CALLS"};
    cbm_edge_t to_leaf = {
        .project = "test", .source_id = mid_id, .target_id = leaf_id, .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(s, &loop), 0);
    ASSERT_GT(cbm_store_insert_edge(s, &to_mid), 0);
    ASSERT_GT(cbm_store_insert_edge(s, &to_leaf), 0);

    /* Relationship uniqueness applies to the complete graph pattern, not to
     * each relationship segment independently. Reusing loop twice is invalid;
     * loop→to_mid and to_mid→to_leaf are the two valid fixed-hop trails. */
    cbm_cypher_result_t fixed = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a {name: \"loopy\"})-[:CALLS]->(b)-[:CALLS]->(c) "
                                 "RETURN c.name",
                                 "test", 0, &fixed),
              0);
    ASSERT_EQ(fixed.row_count, 2);
    cbm_cypher_result_free(&fixed);

    /* The used-edge set must also cross a variable/fixed segment boundary.
     * Valid trails are loop→to_mid, to_mid→to_leaf, and
     * loop→to_mid→to_leaf; the per-segment endpoint product fabricates one
     * extra loop→loop match. */
    cbm_cypher_result_t mixed = {0};
    ASSERT_EQ(cbm_cypher_execute(
                  s, "MATCH (a {name: \"loopy\"})-[:CALLS*1..2]->(b)-[:CALLS]->(c) RETURN c.name",
                  "test", 0, &mixed),
              0);
    ASSERT_EQ(mixed.row_count, 3);
    cbm_cypher_result_free(&mixed);

    /* A repeated relationship variable is an equijoin requiring both
     * occurrences to bind the same logical relationship. The default
     * DIFFERENT RELATIONSHIPS match mode simultaneously forbids reusing that
     * relationship, so the intersection is empty. Do not overwrite r with a
     * distinct second relationship and fabricate a result. */
    cbm_cypher_result_t repeated_variable = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a {name: \"loopy\"})-[r:CALLS]->(b)-[r:CALLS]->(c) "
                                 "RETURN c.name",
                                 "test", 0, &repeated_variable),
              0);
    ASSERT_EQ(repeated_variable.row_count, 0);
    cbm_cypher_result_free(&repeated_variable);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_preserves_parallel_relationship_identity) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_node_t source = {
        .project = "test", .label = "Function", .name = "source", .qualified_name = "test.source"};
    cbm_node_t target = {
        .project = "test", .label = "Function", .name = "target", .qualified_name = "test.target"};
    int64_t source_id = cbm_store_upsert_node(s, &source);
    int64_t target_id = cbm_store_upsert_node(s, &target);
    ASSERT_GT(source_id, 0);
    ASSERT_GT(target_id, 0);
    /* The project graph intentionally upserts identical non-IMPORTS tuples.
     * IMPORTS local_name is part of canonical identity, so these are the
     * representable parallel relationships the matcher must preserve. */
    cbm_edge_t first = {.project = "test",
                        .source_id = source_id,
                        .target_id = target_id,
                        .type = "IMPORTS",
                        .properties_json = "{\"local_name\":\"first\"}"};
    cbm_edge_t second = {.project = "test",
                         .source_id = source_id,
                         .target_id = target_id,
                         .type = "IMPORTS",
                         .properties_json = "{\"local_name\":\"second\"}"};
    int64_t first_id = cbm_store_insert_edge(s, &first);
    int64_t second_id = cbm_store_insert_edge(s, &second);
    ASSERT_GT(first_id, 0);
    ASSERT_GT(second_id, 0);
    ASSERT_NEQ(first_id, second_id);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(s, "MATCH (a {name: \"source\"})-[:IMPORTS]->(b) RETURN b.name",
                                 "test", 0, &result),
              0);
    ASSERT_EQ(result.row_count, 2);

    cbm_cypher_result_free(&result);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_undirected_self_loop_is_one_relationship_match) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_node_t node = {
        .project = "test", .label = "Function", .name = "loopy", .qualified_name = "test.loopy"};
    int64_t node_id = cbm_store_upsert_node(s, &node);
    ASSERT_GT(node_id, 0);
    cbm_edge_t loop = {
        .project = "test", .source_id = node_id, .target_id = node_id, .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(s, &loop), 0);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(s, "MATCH (a {name: \"loopy\"})-[:CALLS]-(b) RETURN b.name",
                                 "test", 0, &result),
              0);
    ASSERT_EQ(result.row_count, 1);

    cbm_cypher_result_free(&result);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_reversed_hop_interval_is_empty_not_error) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(
        cbm_cypher_execute(s, "MATCH (a)-[:CALLS*3..2]->(b) RETURN b.name", "test", 0, &result), 0);
    ASSERT_EQ(result.row_count, 0);
    ASSERT_NULL(result.error);

    cbm_cypher_result_free(&result);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_indexed_and_whole_pattern_providers_are_result_equivalent) {
    cbm_store_t *s = setup_cypher_store();
    const char *queries[] = {
        "MATCH (a {name: \"HandleOrder\"})-[:CALLS]->(b) RETURN b.name ORDER BY b.name",
        "MATCH (b {name: \"ValidateOrder\"})<-[:CALLS]-(a) RETURN a.name ORDER BY a.name",
        "MATCH (a {name: \"HandleOrder\"})-[:CALLS]-(b) RETURN b.name ORDER BY b.name",
    };
    for (size_t qi = 0; qi < sizeof(queries) / sizeof(queries[0]); qi++) {
        cbm_cypher_result_t indexed = {0};
        cbm_cypher_result_t whole = {0};
        cbm_cypher_test_force_whole_pattern_provider(false);
        int indexed_rc = cbm_cypher_execute(s, queries[qi], "test", 0, &indexed);
        cbm_cypher_test_force_whole_pattern_provider(true);
        int whole_rc = cbm_cypher_execute(s, queries[qi], "test", 0, &whole);
        cbm_cypher_test_force_whole_pattern_provider(false);
        ASSERT_EQ(indexed_rc, 0);
        ASSERT_EQ(whole_rc, 0);
        ASSERT_EQ(indexed.row_count, whole.row_count);
        ASSERT_EQ(indexed.col_count, whole.col_count);
        for (int row = 0; row < indexed.row_count; row++) {
            ASSERT_STR_EQ(indexed.rows[row][0], whole.rows[row][0]);
        }
        cbm_cypher_result_free(&indexed);
        cbm_cypher_result_free(&whole);
    }

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_coalesce_issue874) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t a = {.project = "test",
                    .label = "Function",
                    .name = "deep_a",
                    .qualified_name = "test.mod.deep_a",
                    .file_path = "mod.py",
                    .start_line = 1,
                    .end_line = 2,
                    .properties_json = "{\"transitive_loop_depth\":3}"};
    cbm_node_t b = {.project = "test",
                    .label = "Function",
                    .name = "deep_b",
                    .qualified_name = "test.mod.deep_b",
                    .file_path = "mod.py",
                    .start_line = 3,
                    .end_line = 4,
                    .properties_json = "{\"transitive_loop_depth\":1}"};
    cbm_node_t c = {.project = "test",
                    .label = "Function",
                    .name = "plain_c",
                    .qualified_name = "test.mod.plain_c",
                    .file_path = "mod.py",
                    .start_line = 5,
                    .end_line = 6};
    ASSERT_GT(cbm_store_upsert_node(s, &a), 0);
    ASSERT_GT(cbm_store_upsert_node(s, &b), 0);
    ASSERT_GT(cbm_store_upsert_node(s, &c), 0);

    /* Default FAILS the predicate: only the node with depth 3 matches. */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE "
                                "coalesce(f.transitive_loop_depth, 0) >= 2 "
                                "RETURN f.qualified_name LIMIT 10",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    cbm_cypher_result_free(&r);

    /* Default PASSES: the property-less node is included via the default. */
    cbm_cypher_result_t r2 = {0};
    rc = cbm_cypher_execute(s,
                            "MATCH (f:Function) WHERE "
                            "coalesce(f.transitive_loop_depth, 9) >= 2 "
                            "RETURN f.qualified_name LIMIT 10",
                            "test", 0, &r2);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r2.row_count, 2); /* deep_a (3) + plain_c (default 9) */
    cbm_cypher_result_free(&r2);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_regex) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc =
        cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.name =~ \".*Order.*\"", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3); /* HandleOrder, ValidateOrder, SubmitOrder */

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_contains) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc =
        cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.name CONTAINS \"Order\"", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_starts_with) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.name STARTS WITH \"Handle\"", "test",
                                0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_return_properties) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" "
                                "RETURN f.name, f.qualified_name, f.file_path",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_EQ(r.col_count, 3);
    /* Columns should be f.name, f.qualified_name, f.file_path */
    ASSERT_STR_EQ(r.columns[0], "f.name");
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    ASSERT_STR_EQ(r.rows[0][1], "test.HandleOrder");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* ── Scalar / introspection functions (full-suite Tier 1) ──────── */

TEST(cypher_func_labels) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN labels(f)", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "[\"Function\"]");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_func_labels_preserves_query_sized_label) {
    const size_t label_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    char *label = malloc(label_length + SKIP_ONE);
    ASSERT_NOT_NULL(label);
    memset(label, 'L', label_length);
    label[label_length] = '\0';

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "labels", "/tmp/labels"), CBM_STORE_OK);
    cbm_node_t node = {.project = "labels",
                       .label = label,
                       .name = "LongLabel",
                       .qualified_name = "labels.LongLabel",
                       .file_path = "labels.c"};
    ASSERT_TRUE(cbm_store_upsert_node(store, &node) > 0);
    free(label);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, "MATCH (n) RETURN labels(n)", "labels", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 1);
    const char *serialized = result.rows[0][0];
    ASSERT_EQ(strlen(serialized), label_length + sizeof("[\"\"]") - SKIP_ONE);
    ASSERT_EQ(serialized[0], '[');
    ASSERT_EQ(serialized[SKIP_ONE], '"');
    for (size_t i = 0; i < label_length; i++) {
        ASSERT_EQ(serialized[i + PAIR_LEN], 'L');
    }
    ASSERT_EQ(serialized[label_length + PAIR_LEN], '"');
    ASSERT_EQ(serialized[label_length + PAIR_LEN + SKIP_ONE], ']');

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_func_labels_json_escapes_bytes) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "labels", "/tmp/labels"), CBM_STORE_OK);
    cbm_node_t node = {.project = "labels",
                       .label = "Type\"\\\n\t",
                       .name = "EscapedLabel",
                       .qualified_name = "labels.EscapedLabel",
                       .file_path = "labels.c"};
    ASSERT_TRUE(cbm_store_upsert_node(store, &node) > 0);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, "MATCH (n) RETURN labels(n)", "labels", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_STR_EQ(result.rows[0][0], "[\"Type\\\"\\\\\\n\\t\"]");

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_func_type) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function)-[r:CALLS]->(g:Function) RETURN type(r) LIMIT 1", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "CALLS");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

enum {
    WIDE_BINDING_NODE_COUNT = 17,
    WIDE_BINDING_NINTH_EDGE_INDEX = 8,
    WIDE_BINDING_NAME_CAP = 32,
    WIDE_BINDING_QN_CAP = 64,
    WIDE_BINDING_QUERY_CAP = 4096
};

static cbm_store_t *setup_wide_binding_store(void) {
    cbm_store_t *s = cbm_store_open_memory();
    if (!s || cbm_store_upsert_project(s, "wide-bindings", "/tmp/wide-bindings") != CBM_STORE_OK) {
        cbm_store_close(s);
        return NULL;
    }

    int64_t ids[WIDE_BINDING_NODE_COUNT] = {0};
    for (int i = 0; i < WIDE_BINDING_NODE_COUNT; i++) {
        char name[WIDE_BINDING_NAME_CAP];
        char qn[WIDE_BINDING_QN_CAP];
        if (snprintf(name, sizeof(name), "node%02d", i) <= 0 ||
            snprintf(qn, sizeof(qn), "wide-bindings.%s", name) <= 0) {
            cbm_store_close(s);
            return NULL;
        }
        cbm_node_t node = {.project = "wide-bindings",
                           .label = "Function",
                           .name = name,
                           .qualified_name = qn,
                           .file_path = "wide.c"};
        ids[i] = cbm_store_upsert_node(s, &node);
        if (ids[i] <= 0) {
            cbm_store_close(s);
            return NULL;
        }
    }
    for (int i = 0; i + 1 < WIDE_BINDING_NODE_COUNT; i++) {
        cbm_edge_t edge = {.project = "wide-bindings",
                           .source_id = ids[i],
                           .target_id = ids[i + 1],
                           .type = "CALLS"};
        if (cbm_store_insert_edge(s, &edge) <= 0) {
            cbm_store_close(s);
            return NULL;
        }
    }
    return s;
}

static bool build_wide_binding_match(char *query, size_t query_capacity, size_t *used_out) {
    if (!query || query_capacity == 0 || !used_out) {
        return false;
    }
    int written = snprintf(query, query_capacity, "MATCH (n00:Function)");
    if (written <= 0 || (size_t)written >= query_capacity) {
        return false;
    }
    size_t used = (size_t)written;
    for (int i = 0; i + 1 < WIDE_BINDING_NODE_COUNT; i++) {
        written = snprintf(query + used, query_capacity - used, "-[r%02d:CALLS]->(n%02d:Function)",
                           i, i + 1);
        if (written <= 0 || (size_t)written >= query_capacity - used) {
            return false;
        }
        used += (size_t)written;
    }
    *used_out = used;
    return true;
}

TEST(cypher_exec_binds_every_node_and_edge_variable_beyond_inline_capacity) {
    cbm_store_t *s = setup_wide_binding_store();
    ASSERT_NOT_NULL(s);

    char query[WIDE_BINDING_QUERY_CAP];
    size_t used = 0;
    ASSERT_TRUE(build_wide_binding_match(query, sizeof(query), &used));
    int written = snprintf(query + used, sizeof(query) - used,
                           " WHERE n00.name = 'node00' RETURN n%02d.name, type(r%02d)",
                           WIDE_BINDING_NODE_COUNT - 1, WIDE_BINDING_NINTH_EDGE_INDEX);
    ASSERT_GT(written, 0);
    ASSERT_LT((size_t)written, sizeof(query) - used);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(s, query, "wide-bindings", 0, &result), CBM_STORE_OK);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 2);
    ASSERT_STR_EQ(result.rows[0][0], "node16");
    ASSERT_STR_EQ(result.rows[0][1], "CALLS");

    cbm_cypher_result_free(&result);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_default_projection_includes_every_variable_beyond_inline_capacity) {
    cbm_store_t *s = setup_wide_binding_store();
    ASSERT_NOT_NULL(s);

    char query[WIDE_BINDING_QUERY_CAP];
    size_t used = 0;
    ASSERT_TRUE(build_wide_binding_match(query, sizeof(query), &used));
    int written = snprintf(query + used, sizeof(query) - used, " WHERE n00.name = 'node00'");
    ASSERT_GT(written, 0);
    ASSERT_LT((size_t)written, sizeof(query) - used);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(s, query, "wide-bindings", 0, &result), CBM_STORE_OK);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, WIDE_BINDING_NODE_COUNT * 3);
    ASSERT_STR_EQ(result.columns[result.col_count - 3], "n16.name");
    ASSERT_STR_EQ(result.rows[0][result.col_count - 3], "node16");

    cbm_cypher_result_free(&result);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_with_projects_every_variable_beyond_inline_capacity) {
    cbm_store_t *s = setup_wide_binding_store();
    ASSERT_NOT_NULL(s);

    char query[WIDE_BINDING_QUERY_CAP];
    size_t used = 0;
    ASSERT_TRUE(build_wide_binding_match(query, sizeof(query), &used));
    int written = snprintf(query + used, sizeof(query) - used, " WHERE n00.name = 'node00' WITH ");
    ASSERT_GT(written, 0);
    ASSERT_LT((size_t)written, sizeof(query) - used);
    used += (size_t)written;
    for (int i = 0; i < WIDE_BINDING_NODE_COUNT; i++) {
        written = snprintf(query + used, sizeof(query) - used, "%sn%02d AS a%02d",
                           i == 0 ? "" : ", ", i, i);
        ASSERT_GT(written, 0);
        ASSERT_LT((size_t)written, sizeof(query) - used);
        used += (size_t)written;
    }
    written = snprintf(query + used, sizeof(query) - used, " RETURN a16");
    ASSERT_GT(written, 0);
    ASSERT_LT((size_t)written, sizeof(query) - used);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(s, query, "wide-bindings", 0, &result), CBM_STORE_OK);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 1);
    ASSERT_STR_EQ(result.rows[0][0], "node16");

    cbm_cypher_result_free(&result);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_func_id) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN id(f)",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    /* id is a non-empty numeric string */
    ASSERT_TRUE(r.rows[0][0][0] >= '0' && r.rows[0][0][0] <= '9');
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_active_overlay_id_query_uses_canonical_identity) {
    cbm_store_t *s = setup_cypher_store();

    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", 1, &overlay_generation),
              CBM_STORE_OK);
    cbm_node_t fresh_fn = {.project = "test",
                           .label = "Function",
                           .name = "FreshIdSource",
                           .qualified_name = "test.FreshIdSource",
                           .file_path = "handler.go"};
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "handler.go",
                                    .generation = 1,
                                    .nodes = &fresh_fn,
                                    .node_count = 1};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &delta, overlay_generation), CBM_STORE_OK);

    cbm_cypher_result_t active = {0};
    bool used_active = false;
    int rc = cbm_cypher_execute_active_nodes(
        s, "MATCH (f:Function) WHERE f.name = \"FreshIdSource\" RETURN f.name", "test", 0, &active,
        &used_active);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(used_active);
    ASSERT_EQ(active.row_count, 1);
    ASSERT_STR_EQ(active.rows[0][0], "FreshIdSource");
    cbm_cypher_result_free(&active);

    cbm_cypher_result_t id_result = {0};
    used_active = true;
    rc = cbm_cypher_execute_active_nodes(s, "MATCH (f:Function) RETURN id(f), f.name LIMIT 10",
                                         "test", 0, &id_result, &used_active);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(!used_active);
    ASSERT_EQ(id_result.row_count, 4);
    for (int i = 0; i < id_result.row_count; i++) {
        ASSERT_STR_NEQ(id_result.rows[i][1], "FreshIdSource");
    }
    cbm_cypher_result_free(&id_result);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_active_overlay_whole_pattern_preserves_edge_identity) {
    cbm_store_t *s = setup_cypher_store();
    int64_t overlay_generation = 0;
    ASSERT_EQ(cbm_store_reserve_overlay_generation(s, "test", 1, &overlay_generation),
              CBM_STORE_OK);
    cbm_node_t nodes[] = {
        {.project = "test",
         .label = "Function",
         .name = "OverlayA",
         .qualified_name = "test.OverlayA",
         .file_path = "overlay.go"},
        {.project = "test",
         .label = "Function",
         .name = "OverlayB",
         .qualified_name = "test.OverlayB",
         .file_path = "overlay.go"},
        {.project = "test",
         .label = "Function",
         .name = "OverlayC",
         .qualified_name = "test.OverlayC",
         .file_path = "overlay.go"},
        {.project = "test",
         .label = "Function",
         .name = "OverlayLoop",
         .qualified_name = "test.OverlayLoop",
         .file_path = "overlay.go"},
    };
    cbm_store_delta_edge_t edges[] = {
        {.source_qn = "test.OverlayA",
         .target_qn = "test.OverlayB",
         .type = "CALLS",
         .properties_json = "{}"},
        {.source_qn = "test.OverlayB",
         .target_qn = "test.OverlayC",
         .type = "CALLS",
         .properties_json = "{}"},
        {.source_qn = "test.OverlayLoop",
         .target_qn = "test.OverlayLoop",
         .type = "CALLS",
         .properties_json = "{}"},
    };
    cbm_store_file_delta_t delta = {.project = "test",
                                    .rel_path = "overlay.go",
                                    .generation = 1,
                                    .nodes = nodes,
                                    .node_count = 4,
                                    .edges = edges,
                                    .edge_count = 3};
    ASSERT_EQ(cbm_store_publish_overlay_file_delta(s, &delta, overlay_generation), CBM_STORE_OK);

    cbm_cypher_result_t chain = {0};
    bool used_active = false;
    ASSERT_EQ(cbm_cypher_execute_active_nodes(
                  s, "MATCH (a {name: \"OverlayA\"})-[:CALLS]->(b)-[:CALLS]->(c) RETURN c.name",
                  "test", 0, &chain, &used_active),
              CBM_STORE_OK);
    ASSERT_TRUE(used_active);
    ASSERT_EQ(chain.row_count, 1);
    ASSERT_STR_EQ(chain.rows[0][0], "OverlayC");
    cbm_cypher_result_free(&chain);

    cbm_cypher_result_t reused = {0};
    used_active = false;
    ASSERT_EQ(cbm_cypher_execute_active_nodes(
                  s, "MATCH (a {name: \"OverlayLoop\"})-[:CALLS]->(b)-[:CALLS]->(c) RETURN c.name",
                  "test", 0, &reused, &used_active),
              CBM_STORE_OK);
    ASSERT_TRUE(used_active);
    ASSERT_EQ(reused.row_count, 0);
    cbm_cypher_result_free(&reused);

    cbm_cypher_result_t repeated_variable = {0};
    used_active = false;
    ASSERT_EQ(cbm_cypher_execute_active_nodes(
                  s, "MATCH (a {name: \"OverlayA\"})-[r:CALLS]->(b)-[r:CALLS]->(c) RETURN c.name",
                  "test", 0, &repeated_variable, &used_active),
              CBM_STORE_OK);
    ASSERT_TRUE(used_active);
    ASSERT_EQ(repeated_variable.row_count, 0);
    cbm_cypher_result_free(&repeated_variable);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_func_keys) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN keys(f)", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_TRUE(strstr(r.rows[0][0], "\"name\"") != NULL);
    ASSERT_TRUE(strstr(r.rows[0][0], "\"qualified_name\"") != NULL);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_func_keys_dynamic_null_escape_and_dedup) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "keys", "/tmp/keys"), CBM_STORE_OK);
    cbm_node_t node = {
        .project = "keys",
        .label = "Function",
        .name = "Keyed",
        .qualified_name = "keys.Keyed",
        .file_path = "keys.c",
        .properties_json = "{\"dynamic\":\"value\",\"empty\":\"\",\"null_value\":null,"
                           "\"name\":\"shadow\",\"quoted\\\"key\":1}",
    };
    ASSERT_TRUE(cbm_store_upsert_node(store, &node) > 0);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, "MATCH (n:Function) RETURN keys(n)", "keys", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_STR_EQ(result.rows[0][0],
                  "[\"name\",\"qualified_name\",\"label\",\"file_path\",\"dynamic\","
                  "\"empty\",\"quoted\\\"key\"]");

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_func_properties) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN properties(f)", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_EQ(r.rows[0][0][0], '{'); /* a JSON object */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_func_tointeger_tofloat) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" "
                                "RETURN toInteger(f.start_line), toFloat(f.start_line)",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "10"); /* start_line = 10 */
    ASSERT_STR_EQ(r.rows[0][1], "10");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_func_casts_preserve_logical_type_and_reject_invalid_numbers) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "casts", "/tmp/casts"), CBM_STORE_OK);
    cbm_node_t node = {
        .project = "casts",
        .label = "Function",
        .name = "CastTarget",
        .qualified_name = "casts.CastTarget",
        .file_path = "casts.c",
        .start_line = 10,
        .properties_json =
            "{\"zero\":0,\"integer\":2,\"floating\":2.75,\"true_value\":true,"
            "\"false_value\":false,\"integer_text\":\"42\","
            "\"float_text\":\"11.5\",\"partial\":\"12junk\","
            "\"overflow\":\"9223372036854775808\","
            "\"max_text\":\"9223372036854775807\","
            "\"min_text\":\"-9223372036854775808\","
            "\"below_min\":\"-9223372036854775809\","
            "\"plus_text\":\"+42\",\"leading_decimal\":\".5\","
            "\"trailing_decimal\":\"1.\",\"leading_zeroes\":\"001\","
            "\"signed_zeroes\":\"-001\",\"plus_zeroes\":\"+001\","
            "\"zero_fraction\":\"000.5\",\"signed_zero_fraction\":\"-000.5\","
            "\"spaced_number\":\" 42 \","
            "\"precise_text\":\"1.2345678901234567\","
            "\"lower_float\":-9.223372036854776e18,"
            "\"upper_float\":9.223372036854776e18,\"null_value\":null}",
    };
    ASSERT_TRUE(cbm_store_upsert_node(store, &node) > 0);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(
                  store,
                  "MATCH (n:Function) RETURN "
                  "toBoolean(n.zero), toBoolean(n.integer), toBoolean(n.true_value), "
                  "toBoolean(n.false_value), toBoolean(n.integer_text), "
                  "toInteger(n.true_value), toInteger(n.false_value), "
                  "toInteger(n.floating), toInteger(n.integer_text), "
                  "toInteger(n.partial), toInteger(n.overflow), toInteger(n.null_value), "
                  "toFloat(n.integer), toFloat(n.float_text), toFloat(n.partial), "
                  "toInteger(n.max_text), toInteger(n.min_text), toInteger(n.below_min), "
                  "toInteger(n.lower_float), toInteger(n.upper_float), "
                  "toInteger(n.plus_text), toInteger(n.leading_decimal), "
                  "toInteger(n.trailing_decimal), toInteger(n.leading_zeroes), "
                  "toInteger(n.signed_zeroes), toInteger(n.plus_zeroes), "
                  "toInteger(n.zero_fraction), toInteger(n.signed_zero_fraction), "
                  "toInteger(n.spaced_number), toFloat(n.plus_text), "
                  "toFloat(n.leading_decimal), toFloat(n.trailing_decimal), "
                  "toFloat(n.leading_zeroes), toFloat(n.signed_zeroes), "
                  "toFloat(n.plus_zeroes), toFloat(n.zero_fraction), "
                  "toFloat(n.signed_zero_fraction), toFloat(n.spaced_number), "
                  "toFloat(n.precise_text)",
                  "casts", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    const char *expected[] = {
        "false", "true", "true", "false", "", "1", "0", "2", "42", "", "", "", "2",
        "11.5", "", "9223372036854775807", "-9223372036854775808", "",
        "-9223372036854775808", "", "42", "0", "1", "1", "-1", "1", "0", "0", "42",
        "42", "0.5", "1", "1", "-1", "1", "0.5", "-0.5", "42",
        "1.2345678901234567"};
    ASSERT_EQ(result.col_count, (int)(sizeof(expected) / sizeof(expected[0])));
    for (int column = 0; column < result.col_count; column++) {
        ASSERT_STR_EQ(result.rows[0][column], expected[column]);
    }
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) WITH n.start_line AS line "
                                 "RETURN toBoolean(line), toInteger(line), toFloat(line)",
                                 "casts", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_STR_EQ(result.rows[0][0], "true");
    ASSERT_STR_EQ(result.rows[0][1], "10");
    ASSERT_STR_EQ(result.rows[0][2], "10");
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(
                  store,
                  "MATCH (n:Function) WITH id(n) AS identifier, "
                  "toString(n.integer) AS integer_text, size(n.integer_text) AS text_length "
                  "RETURN toBoolean(identifier), toBoolean(integer_text), "
                  "toBoolean(text_length)",
                  "casts", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_STR_EQ(result.rows[0][0], "true");
    ASSERT_STR_EQ(result.rows[0][1], "");
    ASSERT_STR_EQ(result.rows[0][2], "true");
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(
                  store,
                  "MATCH (n:Function) WITH n.zero AS zero, COUNT(n) AS total "
                  "RETURN toBoolean(zero), toBoolean(total), toInteger(total)",
                  "casts", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_STR_EQ(result.rows[0][0], "false");
    ASSERT_STR_EQ(result.rows[0][1], "true");
    ASSERT_STR_EQ(result.rows[0][2], "1");
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(
                  store,
                  "UNWIND [0, 2, 2.0, true, false, \"2\", null] AS x "
                  "MATCH (n:Function) RETURN toBoolean(x), toInteger(x), toFloat(x)",
                  "casts", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 7);
    const char *unwind_expected[][3] = {
        {"false", "0", "0"}, {"true", "2", "2"}, {"", "2", "2"}, {"true", "1", ""},
        {"false", "0", ""},  {"", "2", "2"},     {"", "", ""},
    };
    for (int row = 0; row < result.row_count; row++) {
        for (int column = 0; column < result.col_count; column++) {
            ASSERT_STR_EQ(result.rows[row][column], unwind_expected[row][column]);
        }
    }
    cbm_cypher_result_free(&result);

    cbm_store_close(store);
    PASS();
}

TEST(cypher_func_casts_require_exactly_one_argument) {
    cbm_store_t *store = setup_cypher_store();
    ASSERT_NOT_NULL(store);
    const char *queries[] = {
        "MATCH (n:Function) RETURN toInteger()",
        "MATCH (n:Function) RETURN toFloat(n.start_line, n.end_line)",
        "MATCH (n:Function) RETURN toBoolean()",
    };
    for (size_t query_index = 0; query_index < sizeof(queries) / sizeof(queries[0]); query_index++) {
        cbm_cypher_result_t result = {0};
        ASSERT_TRUE(cbm_cypher_execute(store, queries[query_index], "test", 0, &result) != 0);
        ASSERT_NOT_NULL(result.error);
        cbm_cypher_result_free(&result);
    }
    cbm_store_close(store);
    PASS();
}

TEST(cypher_func_size_reverse) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"LogError\" "
                                "RETURN size(f.name), length(f.name), reverse(f.name)",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "8"); /* "LogError" has 8 chars */
    ASSERT_STR_EQ(r.rows[0][1], "8");
    ASSERT_STR_EQ(r.rows[0][2], "rorrEgoL");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_func_trim_variants_and_odd_reverse) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "trim", "/tmp/trim"), CBM_STORE_OK);
    cbm_node_t node = {.project = "trim",
                       .label = "Function",
                       .name = " \tvalue\r\n",
                       .qualified_name = "trim.value",
                       .file_path = "trim.c"};
    ASSERT_TRUE(cbm_store_upsert_node(store, &node) > 0);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) RETURN trim(n.name), ltrim(n.name), "
                                 "rtrim(n.name), reverse(n.name)",
                                 "trim", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 4);
    ASSERT_STR_EQ(result.rows[0][0], "value");
    ASSERT_STR_EQ(result.rows[0][1], "value\r\n");
    ASSERT_STR_EQ(result.rows[0][2], " \tvalue");
    ASSERT_STR_EQ(result.rows[0][3], "\n\reulav\t ");

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_func_multiarg) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" "
                                "RETURN substring(f.name, 0, 6), left(f.name, 6), "
                                "right(f.name, 5), replace(f.name, \"Order\", \"Req\"), "
                                "coalesce(f.missing, \"fallback\")",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "Handle");    /* substring("HandleOrder",0,6) */
    ASSERT_STR_EQ(r.rows[0][1], "Handle");    /* left(...,6) */
    ASSERT_STR_EQ(r.rows[0][2], "Order");     /* right("HandleOrder",5) */
    ASSERT_STR_EQ(r.rows[0][3], "HandleReq"); /* replace Order->Req */
    ASSERT_STR_EQ(r.rows[0][4], "fallback");  /* coalesce: f.missing empty -> literal */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_func_multiarg_exact_edge_cases) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WHERE f.name = 'HandleOrder' "
        "RETURN replace('aaaaa','aa','b'), replace('abc','','x'), replace('a','long','x'), "
        "replace('abc','abd','x'), "
        "substring(f.name,999,2), substring(f.name,6,0), "
        "right(f.name,2147483647), coalesce(f.missing,'','fallback')",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_EQ(r.col_count, 8);
    ASSERT_STR_EQ(r.rows[0][0], "bba");
    ASSERT_STR_EQ(r.rows[0][1], "abc");
    ASSERT_STR_EQ(r.rows[0][2], "a");
    ASSERT_STR_EQ(r.rows[0][3], "abc");
    ASSERT_STR_EQ(r.rows[0][4], "");
    ASSERT_STR_EQ(r.rows[0][5], "");
    ASSERT_STR_EQ(r.rows[0][6], "HandleOrder");
    ASSERT_STR_EQ(r.rows[0][7], "");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* issue #874: coalesce() in WHERE — null-safe numeric filter over an optional
 * JSON property. Exact repro shape from the issue: nodes lacking the property
 * fall back to the literal default instead of failing to parse. */
TEST(cypher_issue874_where_coalesce_numeric) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "DeepLoop",
                     .qualified_name = "test.DeepLoop",
                     .file_path = "deep.go",
                     .properties_json = "{\"transitive_loop_depth\":5}"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "NoMetrics",
                     .qualified_name = "test.NoMetrics",
                     .file_path = "flat.go"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) "
                                "WHERE coalesce(f.transitive_loop_depth, 0) >= 2 "
                                "RETURN f.qualified_name LIMIT 10",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1); /* only DeepLoop; NoMetrics coalesces to 0 */
    ASSERT_STR_EQ(r.rows[0][0], "test.DeepLoop");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* issue #874: coalesce() in WHERE with a string fallback and first-arg-wins. */
TEST(cypher_issue874_where_coalesce_string) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    /* Missing property on every node → fallback literal matches all 4 Functions */
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WHERE coalesce(f.missing, \"fallback\") = \"fallback\" "
        "RETURN f.name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4);
    cbm_cypher_result_free(&r);

    /* Present first arg wins over the fallback */
    cbm_cypher_result_t r2 = {0};
    rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE coalesce(f.name, \"zz\") = \"HandleOrder\" RETURN f.name",
        "test", 0, &r2);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r2.row_count, 1);
    ASSERT_STR_EQ(r2.rows[0][0], "HandleOrder");
    cbm_cypher_result_free(&r2);

    cbm_store_close(s);
    PASS();
}

/* issue #874: function LHS composes with NOT and AND like any other condition. */
TEST(cypher_issue874_where_coalesce_not_and) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE NOT coalesce(f.missing, \"x\") = \"x\" RETURN f.name", "test",
        0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0); /* every node coalesces to "x" — NOT filters all */
    cbm_cypher_result_free(&r);

    cbm_cypher_result_t r2 = {0};
    rc = cbm_cypher_execute(s,
                            "MATCH (f:Function) WHERE coalesce(f.missing, \"1\") = \"1\" "
                            "AND f.name CONTAINS \"Order\" RETURN f.name",
                            "test", 0, &r2);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r2.row_count, 3); /* HandleOrder, ValidateOrder, SubmitOrder */
    cbm_cypher_result_free(&r2);

    cbm_store_close(s);
    PASS();
}

/* issue #874: the other multi-arg scalar functions work in WHERE too. */
TEST(cypher_issue874_where_substring) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE substring(f.name, 0, 6) = \"Handle\" RETURN f.name", "test", 0,
        &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* issue #874: an unrecognised function in WHERE must fail loudly with the
 * supported set, not the misleading "unexpected operator". */
TEST(cypher_issue874_where_unsupported_func_error) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("MATCH (f:Function) WHERE foo(f.name) = \"x\" RETURN f.name", &q, &err);
    ASSERT_EQ(rc, -1);
    ASSERT_NOT_NULL(err);
    ASSERT_TRUE(strstr(err, "unsupported function 'foo'") != NULL);
    ASSERT_NOT_NULL(strstr(err, "coalesce"));
    ASSERT_NOT_NULL(strstr(err, "right"));
    free(err);
    PASS();
}

TEST(cypher_exists_no_callers) {
    /* NOT EXISTS { (f)<-[:CALLS]-() } → functions with no CALLS caller.
     * HandleOrder has only an incoming DEFINES edge (not CALLS), so it is the
     * sole match — proving EXISTS is edge-type-specific (in_degree=1 here). */
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE NOT EXISTS { (f)<-[:CALLS]-() } RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exists_has_outgoing_calls) {
    /* EXISTS { (f)-[:CALLS]->() } → functions that call something.
     * HandleOrder (→ValidateOrder, →LogError) and ValidateOrder (→SubmitOrder). */
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE EXISTS { (f)-[:CALLS]->() } RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_calls_relationship) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                "RETURN f.name, g.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* HandleOrder→ValidateOrder, HandleOrder→LogError, ValidateOrder→SubmitOrder */
    ASSERT_EQ(r.row_count, 3);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_calls_with_where) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                "WHERE f.name = \"HandleOrder\" "
                                "RETURN f.name, g.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2); /* →ValidateOrder, →LogError */

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_inbound) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)<-[:CALLS]-(g:Function) "
                                "WHERE f.name = \"ValidateOrder\" "
                                "RETURN g.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1); /* HandleOrder calls ValidateOrder */
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_count) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                "RETURN f.name, COUNT(g) AS cnt",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* HandleOrder→2, ValidateOrder→1 */
    ASSERT_EQ(r.row_count, 2);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_limit) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name LIMIT 2", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_order_by) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name ORDER BY f.name ASC", "test",
                                0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4);
    /* Alphabetical: HandleOrder, LogError, SubmitOrder, ValidateOrder */
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    ASSERT_STR_EQ(r.rows[1][0], "LogError");
    ASSERT_STR_EQ(r.rows[2][0], "SubmitOrder");
    ASSERT_STR_EQ(r.rows[3][0], "ValidateOrder");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_variable_length) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    /* HandleOrder →CALLS→ ValidateOrder →CALLS→ SubmitOrder (2 hops) */
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS*1..3]->(g:Function) "
                                "WHERE f.name = \"HandleOrder\" "
                                "RETURN g.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* Should find: ValidateOrder (1 hop), SubmitOrder (2 hops), LogError (1 hop) */
    ASSERT_GTE(r.row_count, 3);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_variable_length_any_direction) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS*1..2]-(g:Function) "
                                "WHERE f.name = \"SubmitOrder\" "
                                "RETURN g.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.row_count, 2);
    int saw_validate = 0;
    int saw_handle = 0;
    for (int i = 0; i < r.row_count; i++) {
        if (strcmp(r.rows[i][0], "ValidateOrder") == 0) {
            saw_validate = 1;
        }
        if (strcmp(r.rows[i][0], "HandleOrder") == 0) {
            saw_handle = 1;
        }
    }
    ASSERT_EQ(saw_validate, 1);
    ASSERT_EQ(saw_handle, 1);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* An explicit variable-length upper bound is query semantics, not an output
 * budget. The traversal must honor all 12 requested hops without a warning-only
 * clamp. The store BFS uses a visited frontier, so cyclic graphs terminate at
 * graph exhaustion in O(V + E) traversal work and O(V) visited memory rather
 * than materializing O(V * requested_depth) (node, hop) pairs. */
TEST(cypher_exec_var_length_bounds_preserve_reachability) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* Linear chain N00 -CALLS-> N01 -> ... -> N12 (13 nodes, one node per hop). */
    int64_t ids[13];
    for (int i = 0; i < 13; i++) {
        char name[8];
        char qn[24];
        snprintf(name, sizeof(name), "N%02d", i);
        snprintf(qn, sizeof(qn), "test.N%02d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "chain.go"};
        ids[i] = cbm_store_upsert_node(s, &n);
    }
    for (int i = 0; i < 12; i++) {
        cbm_edge_t e = {
            .project = "test", .source_id = ids[i], .target_id = ids[i + 1], .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (a:Function {name: \"N00\"})-[:CALLS*1..12]->"
                                "(x:Function) RETURN x.name",
                                "test", 64, &r);
    ASSERT_EQ(rc, 0);

    ASSERT_EQ(r.row_count, 12);
    bool saw_n10 = false;
    bool saw_n11 = false;
    bool saw_n12 = false;
    for (int i = 0; i < r.row_count; i++) {
        const char *v = r.rows[i][0];
        if (v && strcmp(v, "N10") == 0) {
            saw_n10 = true;
        }
        if (v && strcmp(v, "N11") == 0) {
            saw_n11 = true;
        }
        if (v && strcmp(v, "N12") == 0) {
            saw_n12 = true;
        }
    }
    ASSERT_TRUE(saw_n10);
    ASSERT_TRUE(saw_n11);
    ASSERT_TRUE(saw_n12);
    ASSERT_NULL(r.warning);

    cbm_cypher_result_free(&r);

    /* An omitted upper bound means traverse to graph exhaustion, not silently
     * stop at an implementation depth. The same finite chain therefore has the
     * same complete reachable set. */
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a:Function {name: \"N00\"})-[:CALLS*1..]->"
                                 "(x:Function) RETURN x.name",
                                 "test", 64, &r),
              0);
    ASSERT_EQ(r.row_count, 12);
    ASSERT_NULL(r.warning);
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_var_length_zero_hops_returns_start_only) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a:Function {name: \"SubmitOrder\"})"
                                 "-[:CALLS*0..0]->(b:Function) RETURN b.name",
                                 "test", 0, &r),
              0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "SubmitOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_var_length_preserves_all_requested_edge_types) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "types", "/tmp/types"), CBM_STORE_OK);
    cbm_node_t root = {
        .project = "types", .label = "Function", .name = "Root", .qualified_name = "types.Root"};
    cbm_node_t target = {.project = "types",
                         .label = "Function",
                         .name = "Target",
                         .qualified_name = "types.Target"};
    cbm_node_t middle = {.project = "types",
                         .label = "Function",
                         .name = "Middle",
                         .qualified_name = "types.Middle"};
    int64_t root_id = cbm_store_upsert_node(s, &root);
    int64_t middle_id = cbm_store_upsert_node(s, &middle);
    int64_t target_id = cbm_store_upsert_node(s, &target);
    ASSERT_GT(root_id, 0);
    ASSERT_GT(middle_id, 0);
    ASSERT_GT(target_id, 0);
    cbm_edge_t first = {
        .project = "types", .source_id = root_id, .target_id = middle_id, .type = "TARGET"};
    cbm_edge_t second = {
        .project = "types", .source_id = middle_id, .target_id = target_id, .type = "TARGET"};
    ASSERT_GT(cbm_store_insert_edge(s, &first), 0);
    ASSERT_GT(cbm_store_insert_edge(s, &second), 0);

    const char *typed_query =
        "MATCH (a:Function {name: \"Root\"})"
        "-[:T00|T01|T02|T03|T04|T05|T06|T07|T08|T09|T10|T11|T12|T13|T14|T15|TARGET*1..2]->"
        "(b:Function) RETURN b.name";
    cbm_cypher_result_t r = {0};
    ASSERT_EQ(cbm_cypher_execute(s, typed_query, "types", 0, &r), 0);
    ASSERT_EQ(r.row_count, 2);
    bool saw_target = false;
    for (int i = 0; i < r.row_count; i++) {
        saw_target = saw_target || strcmp(r.rows[i][0], "Target") == 0;
    }
    ASSERT_TRUE(saw_target);
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_defines_edge) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (m:Module)-[:DEFINES]->(f:Function) "
                                "RETURN m.name, f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "main");
    ASSERT_STR_EQ(r.rows[0][1], "HandleOrder");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_no_results) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc =
        cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.name = \"NonExistent\"", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_numeric) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.start_line > \"8\" "
                                "RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* HandleOrder starts at 10 */
    ASSERT_GTE(r.row_count, 1);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* --- Ported from cypher_test.go: TestExecuteDistinct --- */
TEST(cypher_exec_distinct) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN DISTINCT f.label", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* All 4 Function nodes share label "Function" → 1 distinct row */
    ASSERT_EQ(r.row_count, 1);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* issue #238: WITH DISTINCT must deduplicate projected rows (previously the
 * DISTINCT keyword on WITH was parsed but silently ignored). */
TEST(cypher_exec_with_distinct_issue238) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    /* 4 Function nodes all share label "Function" → WITH DISTINCT collapses to
     * one row; without dedup this returned 4. */
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) WITH DISTINCT f.label AS lbl RETURN lbl",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    cbm_cypher_result_free(&r);

    /* Control: without DISTINCT, all 4 rows flow through. */
    cbm_cypher_result_t r2 = {0};
    rc = cbm_cypher_execute(s, "MATCH (f:Function) WITH f.label AS lbl RETURN lbl", "test", 0, &r2);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r2.row_count, 4);
    cbm_cypher_result_free(&r2);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_with_distinct_preserves_values_differing_after_inline_prefix) {
    static const char json_prefix[] = "{\"payload\":\"";
    static const char json_suffix[] = "\"}";
    static const char *const names[] = {"first", "second"};
    static const char *const qualified_names[] = {"test.first", "test.second"};
    const size_t value_length = CYPHER_TEST_DISTINCT_VALUE_BYTES;
    const size_t json_size = sizeof(json_prefix) - SKIP_ONE + value_length + sizeof(json_suffix);
    char *json = malloc(json_size);
    ASSERT_NOT_NULL(json);

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "test", "/tmp/test"), CBM_STORE_OK);
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        size_t offset = 0;
        memcpy(json + offset, json_prefix, sizeof(json_prefix) - SKIP_ONE);
        offset += sizeof(json_prefix) - SKIP_ONE;
        memset(json + offset, 'v', value_length);
        json[offset + value_length - SKIP_ONE] = (char)('a' + i);
        offset += value_length;
        memcpy(json + offset, json_suffix, sizeof(json_suffix));
        cbm_node_t node = {.project = "test",
                           .label = "Function",
                           .name = names[i],
                           .qualified_name = qualified_names[i],
                           .file_path = "distinct.c",
                           .properties_json = json};
        ASSERT_GT(cbm_store_upsert_node(store, &node), 0);
    }

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store,
                                "MATCH (n:Function) WITH DISTINCT n.payload AS p "
                                "RETURN p ORDER BY p",
                                "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 2);
    ASSERT_EQ(strlen(result.rows[0][0]), value_length);
    ASSERT_EQ(strlen(result.rows[1][0]), value_length);
    ASSERT_EQ(result.rows[0][0][value_length - SKIP_ONE], 'a');
    ASSERT_EQ(result.rows[1][0][value_length - SKIP_ONE], 'b');

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    free(json);
    PASS();
}

/* issue #241: label tests in WHERE clauses (openCypher `WHERE n:Label`) —
 * previously a parse error. */
TEST(cypher_exec_where_label_test_issue241) {
    cbm_store_t *s = setup_cypher_store();

    /* f:Function is true for all 4 Function nodes. */
    cbm_cypher_result_t r = {0};
    int rc =
        cbm_cypher_execute(s, "MATCH (f:Function) WHERE f:Function RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4);
    cbm_cypher_result_free(&r);

    /* f:Class matches none of the functions. */
    cbm_cypher_result_t r2 = {0};
    rc = cbm_cypher_execute(s, "MATCH (f:Function) WHERE f:Class RETURN f.name", "test", 0, &r2);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r2.row_count, 0);
    cbm_cypher_result_free(&r2);

    /* Negated label test: NOT f:Class is always true. */
    cbm_cypher_result_t r3 = {0};
    rc =
        cbm_cypher_execute(s, "MATCH (f:Function) WHERE NOT f:Class RETURN f.name", "test", 0, &r3);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r3.row_count, 4);
    cbm_cypher_result_free(&r3);

    cbm_store_close(s);
    PASS();
}

/* issue #239: COUNT(DISTINCT x) — previously a parse error. */
TEST(cypher_exec_count_distinct_issue239) {
    cbm_store_t *s = setup_cypher_store();

    /* 4 functions all share label "Function" → COUNT(DISTINCT f.label) = 1. */
    cbm_cypher_result_t r = {0};
    int rc =
        cbm_cypher_execute(s, "MATCH (f:Function) RETURN count(DISTINCT f.label)", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "1");
    cbm_cypher_result_free(&r);

    /* Non-distinct COUNT counts all 4 occurrences. */
    cbm_cypher_result_t r2 = {0};
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN count(f.label)", "test", 0, &r2);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(r2.rows[0][0], "4");
    cbm_cypher_result_free(&r2);

    /* DISTINCT over the 4 unique function names = 4. */
    cbm_cypher_result_t r3 = {0};
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN count(DISTINCT f.name)", "test", 0, &r3);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(r3.rows[0][0], "4");
    cbm_cypher_result_free(&r3);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_aggregate_distinct_preserves_entity_identity_and_collect_semantics) {
    cbm_store_t *s = setup_cypher_store();
    cbm_node_t same_display_name = {.project = "test",
                                    .label = "Function",
                                    .name = "HandleOrder",
                                    .qualified_name = "test.other.HandleOrder",
                                    .file_path = "other_handler.go",
                                    .start_line = 5};
    ASSERT_TRUE(cbm_store_upsert_node(s, &same_display_name) > 0);

    cbm_cypher_result_t count = {0};
    ASSERT_EQ(cbm_cypher_execute(s, "MATCH (f:Function) RETURN COUNT(DISTINCT f)", "test", 0,
                                 &count),
              0);
    /* Two nodes may share a display name but remain distinct graph entities. */
    ASSERT_EQ(count.row_count, 1);
    ASSERT_STR_EQ(count.rows[0][0], "5");
    cbm_cypher_result_free(&count);

    cbm_cypher_result_t collect = {0};
    ASSERT_EQ(cbm_cypher_execute(s, "MATCH (f:Function) RETURN COLLECT(DISTINCT f.label)",
                                 "test", 0, &collect),
              0);
    ASSERT_EQ(collect.row_count, 1);
    ASSERT_STR_EQ(collect.rows[0][0], "[\"Function\"]");
    cbm_cypher_result_free(&collect);

    cbm_cypher_result_t numeric = {0};
    ASSERT_EQ(cbm_cypher_execute(
                  s,
                  "MATCH (f:Function) RETURN SUM(DISTINCT f.start_line), "
                  "AVG(DISTINCT f.start_line), MIN(DISTINCT f.start_line), "
                  "MAX(DISTINCT f.start_line)",
                  "test", 0, &numeric),
              0);
    ASSERT_EQ(numeric.row_count, 1);
    ASSERT_STR_EQ(numeric.rows[0][0], "15");
    ASSERT_STR_EQ(numeric.rows[0][1], "5");
    ASSERT_STR_EQ(numeric.rows[0][2], "0");
    ASSERT_STR_EQ(numeric.rows[0][3], "10");
    cbm_cypher_result_free(&numeric);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_collect_json_escapes_core_string_values) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_node_t node = {.project = "test",
                       .label = "Function",
                       .name = "quote\"slash\\line\n",
                       .qualified_name = "test.escaped",
                       .file_path = "escaped.c"};
    ASSERT_TRUE(cbm_store_upsert_node(s, &node) > 0);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(s, "MATCH (n:Function) RETURN COLLECT(n.name)", "test", 0,
                                 &result),
              0);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_STR_EQ(result.rows[0][0], "[\"quote\\\"slash\\\\line\\n\"]");
    yyjson_doc *doc = yyjson_read(result.rows[0][0], strlen(result.rows[0][0]), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *list = yyjson_doc_get_root(doc);
    ASSERT_TRUE(yyjson_is_arr(list));
    ASSERT_EQ(yyjson_arr_size(list), 1);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_arr_get_first(list)), node.name);
    yyjson_doc_free(doc);

    cbm_cypher_result_free(&result);
    cbm_store_close(s);
    PASS();
}

/* issue #373: an unsupported computed expression in WITH/RETURN (an unknown
 * function like split(...) or list indexing [..]) must FAIL LOUDLY with a clear
 * "unsupported function" error rather than silently projecting an empty column
 * (which looks like a valid-but-blank result and hides the real problem). */
TEST(cypher_exec_unsupported_func_errors_issue373) {
    cbm_store_t *s = setup_cypher_store();

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WITH split(f.name)[0] AS top, count(*) AS c RETURN top, c", "test",
        0, &r);
    ASSERT_TRUE(rc != 0); /* unsupported function now fails loudly */
    ASSERT_NOT_NULL(r.error);
    ASSERT_TRUE(strstr(r.error, "unsupported") != NULL);
    ASSERT_TRUE(strstr(r.error, "split") != NULL);
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

/* A recognised function still works, and an unknown one in plain RETURN errors. */
TEST(cypher_exec_unknown_func_return_errors) {
    cbm_store_t *s = setup_cypher_store();

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN nosuchfunc(f.name)", "test", 0, &r);
    ASSERT_TRUE(rc != 0);
    ASSERT_NOT_NULL(r.error);
    ASSERT_TRUE(strstr(r.error, "unsupported function") != NULL);
    ASSERT_NOT_NULL(strstr(r.error, "count"));
    ASSERT_NOT_NULL(strstr(r.error, "properties"));
    ASSERT_NOT_NULL(strstr(r.error, "coalesce"));
    ASSERT_NOT_NULL(strstr(r.error, "right"));
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

enum { CYPHER_AGGREGATE_GROWTH_GROUP_COUNT = 600 };
static cbm_store_t *setup_cypher_aggregate_group_store(int group_count);

/* issue #242: openCypher label alternation in MATCH — (n:A|B). */
TEST(cypher_exec_label_alternation_issue242) {
    cbm_store_t *s = setup_cypher_store();

    /* Store has 4 Function + 1 Module node → alternation seeds all 5. */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (n:Function|Module) RETURN n.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 5);
    cbm_cypher_result_free(&r);

    /* Alternation with a non-existent label still returns the existing one. */
    cbm_cypher_result_t r2 = {0};
    rc = cbm_cypher_execute(s, "MATCH (n:Function|Class) RETURN n.name", "test", 0, &r2);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r2.row_count, 4);
    cbm_cypher_result_free(&r2);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_label_alternation_growth_failure_is_atomic) {
    cbm_store_t *store = setup_cypher_aggregate_group_store(CYPHER_AGGREGATE_GROWTH_GROUP_COUNT);
    ASSERT_NOT_NULL(store);
    cbm_node_t module = {.project = "test",
                         .label = "Module",
                         .name = "module",
                         .qualified_name = "test.module",
                         .file_path = "module.c"};
    ASSERT_TRUE(cbm_store_upsert_node(store, &module) > 0);

    cbm_cypher_result_t result = {0};
    /* Module grows the first vector; Function then needs a second growth while
     * the scan owns both the first result and the new per-label result. */
    cbm_cypher_test_fail_label_alternation_growth(1);
    int rc =
        cbm_cypher_execute(store, "MATCH (n:Module|Function) RETURN n.name", "test", 0, &result);
    cbm_cypher_test_fail_label_alternation_growth(-1);

    ASSERT_TRUE(rc != 0);
    ASSERT_NOT_NULL(result.error);
    ASSERT_NOT_NULL(strstr(result.error, "allocate memory"));
    ASSERT_EQ(result.row_count, 0);
    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

/* --- Ported from cypher_test.go: TestExecuteInlinePropertyFilter --- */
TEST(cypher_exec_inline_props) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function {name: \"SubmitOrder\"}) "
                                "RETURN f.name, f.qualified_name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* --- Ported from cypher_test.go: TestParseWhereStartsWith --- */
TEST(cypher_parse_where_starts_with) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("MATCH (f:Function) WHERE f.name STARTS WITH \"Send\" RETURN f", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);
    ASSERT_NOT_NULL(q->where);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_CONDITION);
    ASSERT_STR_EQ(q->where->root->cond.op, "STARTS WITH");
    ASSERT_STR_EQ(q->where->root->cond.value, "Send");
    cbm_query_free(q);
    PASS();
}

/* --- Ported from cypher_test.go: TestParseWhereContains --- */
TEST(cypher_parse_where_contains) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("MATCH (f:Function) WHERE f.name CONTAINS \"Handler\" RETURN f", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);
    ASSERT_NOT_NULL(q->where);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_CONDITION);
    ASSERT_STR_EQ(q->where->root->cond.op, "CONTAINS");
    ASSERT_STR_EQ(q->where->root->cond.value, "Handler");
    cbm_query_free(q);
    PASS();
}

/* --- Ported from cypher_test.go: TestParseWhereNumericComparison --- */
TEST(cypher_parse_where_numeric) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) WHERE f.start_line > 10 RETURN f", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);
    ASSERT_NOT_NULL(q->where);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_CONDITION);
    ASSERT_STR_EQ(q->where->root->cond.op, ">");
    ASSERT_STR_EQ(q->where->root->cond.value, "10");
    cbm_query_free(q);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  EDGE PROPERTY TESTS (ported from cypher_test.go Feature 2)
 * ══════════════════════════════════════════════════════════════════ */

/* Helper: set up store with HTTP_CALLS edge having properties.
 * Creates same graph as setup_cypher_store + one HTTP_CALLS edge. */
static cbm_store_t *setup_cypher_http_store(void) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "HandleOrder",
                     .qualified_name = "test.main.HandleOrder",
                     .file_path = "main.go",
                     .start_line = 10,
                     .end_line = 30};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "ValidateOrder",
                     .qualified_name = "test.service.ValidateOrder",
                     .file_path = "service.go",
                     .start_line = 5,
                     .end_line = 20};
    cbm_node_t n3 = {.project = "test",
                     .label = "Function",
                     .name = "SubmitOrder",
                     .qualified_name = "test.service.SubmitOrder",
                     .file_path = "service.go",
                     .start_line = 25,
                     .end_line = 50};

    int64_t id1 = cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    int64_t id3 = cbm_store_upsert_node(s, &n3);

    cbm_edge_t http = {
        .project = "test",
        .source_id = id1,
        .target_id = id3,
        .type = "HTTP_CALLS",
        .properties_json =
            "{\"url_path\":\"/api/orders\",\"confidence\":0.85,\"method\":\"POST\"}"};
    cbm_store_insert_edge(s, &http);

    return s;
}

/* Helper: set up store with TWO HTTP_CALLS edges for filtering tests. */
static cbm_store_t *setup_cypher_multi_edge_store(void) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "testproj", "/tmp/test");

    cbm_node_t n1 = {.project = "testproj",
                     .label = "Function",
                     .name = "SendOrder",
                     .qualified_name = "testproj.caller.SendOrder",
                     .file_path = "caller/client.go"};
    cbm_node_t n2 = {.project = "testproj",
                     .label = "Function",
                     .name = "HandleOrder",
                     .qualified_name = "testproj.handler.HandleOrder",
                     .file_path = "handler/routes.go"};
    cbm_node_t n3 = {.project = "testproj",
                     .label = "Function",
                     .name = "HandleHealth",
                     .qualified_name = "testproj.handler.HandleHealth",
                     .file_path = "handler/health.go"};

    int64_t id1 = cbm_store_upsert_node(s, &n1);
    int64_t id2 = cbm_store_upsert_node(s, &n2);
    int64_t id3 = cbm_store_upsert_node(s, &n3);

    cbm_edge_t e1 = {.project = "testproj",
                     .source_id = id1,
                     .target_id = id2,
                     .type = "HTTP_CALLS",
                     .properties_json =
                         "{\"url_path\":\"/api/orders\",\"confidence\":0.85,\"method\":\"POST\"}"};
    cbm_edge_t e2 = {.project = "testproj",
                     .source_id = id1,
                     .target_id = id3,
                     .type = "HTTP_CALLS",
                     .properties_json = "{\"url_path\":\"/health\",\"confidence\":0.45}"};
    cbm_store_insert_edge(s, &e1);
    cbm_store_insert_edge(s, &e2);

    return s;
}

/* Helper: find a column value in a cypher result row */
static const char *cypher_get_col(const cbm_cypher_result_t *r, int row, const char *col) {
    for (int c = 0; c < r->col_count; c++) {
        if (strcmp(r->columns[c], col) == 0)
            return r->rows[row][c];
    }
    return NULL;
}

/* Helper: check if any row has a column matching a value */
static bool cypher_has_row_with(const cbm_cypher_result_t *r, const char *col, const char *val) {
    int ci = -1;
    for (int c = 0; c < r->col_count; c++) {
        if (strcmp(r->columns[c], col) == 0) {
            ci = c;
            break;
        }
    }
    if (ci < 0)
        return false;
    for (int row = 0; row < r->row_count; row++) {
        if (strcmp(r->rows[row][ci], val) == 0)
            return true;
    }
    return false;
}

TEST(cypher_edge_prop_access) {
    cbm_store_t *s = setup_cypher_http_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (a:Function)-[r:HTTP_CALLS]->(b:Function) "
                                "RETURN a.name, b.name, r.url_path, r.confidence",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "a.name"), "HandleOrder");
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "b.name"), "SubmitOrder");
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "r.url_path"), "/api/orders");
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "r.confidence"), "0.85");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

typedef struct {
    atomic_int *ready;
    atomic_int *start;
    bool succeeded;
} cypher_edge_thread_ctx_t;

static void *cypher_edge_props_concurrently(void *opaque) {
    cypher_edge_thread_ctx_t *ctx = opaque;
    cbm_store_t *store = setup_cypher_http_store();
    if (!store) {
        return NULL;
    }

    /* Keep projection busy after the store scan has completed.  A single-edge
     * query can be incidentally ordered by SQLite's internal mutexes, masking
     * the independent Cypher scratch-buffer race from TSan. */
    cbm_node_t source = {.project = "test",
                         .label = "Function",
                         .name = "HandleOrder",
                         .qualified_name = "test.main.HandleOrder",
                         .file_path = "main.go",
                         .start_line = 10,
                         .end_line = 30};
    int64_t source_id = cbm_store_upsert_node(store, &source);
    for (int i = 0; i < 256; i++) {
        char name[64];
        char qualified_name[96];
        snprintf(name, sizeof(name), "ConcurrentTarget%d", i);
        snprintf(qualified_name, sizeof(qualified_name), "test.concurrent.%s", name);
        cbm_node_t target = {.project = "test",
                             .label = "Function",
                             .name = name,
                             .qualified_name = qualified_name,
                             .file_path = "concurrent.go"};
        int64_t target_id = cbm_store_upsert_node(store, &target);
        cbm_edge_t edge = {
            .project = "test",
            .source_id = source_id,
            .target_id = target_id,
            .type = "HTTP_CALLS",
            .properties_json =
                "{\"url_path\":\"/api/orders\",\"confidence\":0.85,\"method\":\"POST\"}"};
        if (source_id < 0 || target_id < 0 || cbm_store_insert_edge(store, &edge) < 0) {
            cbm_store_close(store);
            return NULL;
        }
    }

    atomic_fetch_add_explicit(ctx->ready, 1, memory_order_release);
    while (atomic_load_explicit(ctx->start, memory_order_acquire) == 0) {
        cbm_usleep(1000);
    }
    ctx->succeeded = true;
    for (int i = 0; i < 128; i++) {
        cbm_cypher_result_t result = {0};
        int rc = cbm_cypher_execute(store,
                                    "MATCH (a:Function)-[r:HTTP_CALLS]->(b:Function) "
                                    "RETURN r.url_path, r.confidence, r.method",
                                    "test", 0, &result);
        if (rc != 0 || result.row_count != 257 ||
            strcmp(cypher_get_col(&result, 0, "r.url_path"), "/api/orders") != 0 ||
            strcmp(cypher_get_col(&result, 0, "r.confidence"), "0.85") != 0 ||
            strcmp(cypher_get_col(&result, 0, "r.method"), "POST") != 0) {
            ctx->succeeded = false;
        }
        cbm_cypher_result_free(&result);
        if (!ctx->succeeded) {
            break;
        }
    }
    cbm_store_close(store);
    return NULL;
}

/* Daemon sessions execute independent graph queries concurrently. TSan must
 * see no shared rotating edge-property scratch buffer between those threads. */
TEST(cypher_edge_prop_storage_is_per_thread) {
    atomic_int ready;
    atomic_int start;
    atomic_init(&ready, 0);
    atomic_init(&start, 0);
    cypher_edge_thread_ctx_t ctx[2] = {
        {.ready = &ready, .start = &start},
        {.ready = &ready, .start = &start},
    };
    cbm_thread_t threads[2];
    bool started0 = cbm_thread_create(&threads[0], 0, cypher_edge_props_concurrently, &ctx[0]) == 0;
    bool started1 = cbm_thread_create(&threads[1], 0, cypher_edge_props_concurrently, &ctx[1]) == 0;
    for (int spins = 0; started0 && started1 && spins < 5000 &&
                        atomic_load_explicit(&ready, memory_order_acquire) < 2;
         spins++) {
        cbm_usleep(1000);
    }
    bool both_ready = atomic_load_explicit(&ready, memory_order_acquire) == 2;
    atomic_store_explicit(&start, 1, memory_order_release);
    if (started0) {
        (void)cbm_thread_join(&threads[0]);
    }
    if (started1) {
        (void)cbm_thread_join(&threads[1]);
    }

    ASSERT_TRUE(started0);
    ASSERT_TRUE(started1);
    ASSERT_TRUE(both_ready);
    ASSERT_TRUE(ctx[0].succeeded);
    ASSERT_TRUE(ctx[1].succeeded);
    PASS();
}

TEST(cypher_edge_prop_in_where) {
    cbm_store_t *s = setup_cypher_http_store();
    cbm_cypher_result_t r = {0};

    /* confidence > 0.8 → should match */
    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.confidence > 0.8 "
                                "RETURN a.name, b.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    cbm_cypher_result_free(&r);

    /* confidence > 0.9 → should NOT match */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s,
                            "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.confidence > 0.9 "
                            "RETURN a.name",
                            "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_type_prop) {
    cbm_store_t *s = setup_cypher_http_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s, "MATCH (a)-[r:HTTP_CALLS]->(b) RETURN r.type", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "r.type"), "HTTP_CALLS");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_filter_contains) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.url_path CONTAINS 'orders' "
                                "RETURN a.name, b.name, r.url_path",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "a.name"), "SendOrder");
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "b.name"), "HandleOrder");
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "r.url_path"), "/api/orders");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_filter_numeric_gte) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.confidence >= 0.6 "
                                "RETURN a.name, b.name, r.confidence LIMIT 20",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "b.name"), "HandleOrder");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_bare_edge_return_exposes_properties_json) {
    /* `RETURN r` on an edge variable, with no property accessor, should
     * surface the edge's full properties JSON (or "{}"). Before the fix,
     * binding_get_virtual returned an empty string, which made bare edge
     * returns useless for callers that wanted to inspect timestamps,
     * weights, etc. without naming each property up front. */
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s, "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.method = 'POST' RETURN r",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    const char *r_val = cypher_get_col(&r, 0, "r");
    ASSERT_NOT_NULL(r_val);
    /* Expect JSON object content rather than the previous empty string. */
    ASSERT_NOT_NULL(strstr(r_val, "url_path"));
    ASSERT_NOT_NULL(strstr(r_val, "/api/orders"));

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_return_without_filter) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) "
                                "RETURN a.name, b.name, r.url_path, r.confidence LIMIT 20",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.row_count, 2);
    ASSERT(cypher_has_row_with(&r, "r.url_path", "/api/orders"));
    ASSERT(cypher_has_row_with(&r, "r.url_path", "/health"));

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_filter_equals) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.method = 'POST' "
                                "RETURN a.name, b.name",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "b.name"), "HandleOrder");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_filter_starts_with) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.url_path STARTS WITH '/api' "
                                "RETURN a.name, b.name, r.url_path",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "r.url_path"), "/api/orders");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_combined_node_and_edge_filter) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (a:Function)-[r:HTTP_CALLS]->(b:Function) "
                                "WHERE a.name = 'SendOrder' AND r.confidence >= 0.6 "
                                "RETURN b.name, r.url_path",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "b.name"), "HandleOrder");
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "r.url_path"), "/api/orders");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_filter_no_match) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    /* No edge has method = 'DELETE' */
    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.method = 'DELETE' "
                                "RETURN a.name",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_filter_numeric_lt) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    /* Only health edge (0.45) should match confidence < 0.5 */
    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.confidence < 0.5 "
                                "RETURN b.name, r.confidence",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "b.name"), "HandleHealth");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_filter_regex) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.url_path =~ \"/api/.*\" "
                                "RETURN b.name",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(cypher_get_col(&r, 0, "b.name"), "HandleOrder");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_edge_builtin_type_filter) {
    cbm_store_t *s = setup_cypher_multi_edge_store();
    cbm_cypher_result_t r = {0};

    /* Untyped rel [r] — filter on r.type in WHERE */
    int rc = cbm_cypher_execute(s,
                                "MATCH (a)-[r]->(b) WHERE r.type = 'HTTP_CALLS' "
                                "RETURN a.name, b.name LIMIT 20",
                                "testproj", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2); /* Both HTTP_CALLS edges */

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Ported from cypher_test.go: TestApplyLimitRespectsExplicit */
TEST(cypher_apply_limit) {
    /* Create store with many nodes */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "lim", "/tmp/lim");

    for (int i = 0; i < 50; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "func%d", i);
        snprintf(qn, sizeof(qn), "lim.func%d", i);
        cbm_node_t n = {.project = "lim",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "test.go"};
        cbm_store_upsert_node(s, &n);
    }

    /* LIMIT 5 → 5 rows */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name LIMIT 5", "lim", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 5);
    ASSERT_FALSE(r.truncated);
    cbm_cypher_result_free(&r);

    /* No LIMIT, max_rows=10 → capped at 10 */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name", "lim", 10, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 10);
    ASSERT_TRUE(r.truncated);
    cbm_cypher_result_free(&r);

    /* LIMIT can reduce but cannot bypass the caller/server output cap. */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name LIMIT 30", "lim", 10, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 10);
    ASSERT_TRUE(r.truncated);
    cbm_cypher_result_free(&r);

    /* LIMIT 0 is an explicit empty result, not the no-limit sentinel. */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name LIMIT 0", "lim", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0);
    ASSERT_FALSE(r.truncated);
    cbm_cypher_result_free(&r);

    /* WITH has a separate skip/limit path and must preserve the same semantics. */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s, "MATCH (f:Function) WITH f LIMIT 0 RETURN f.name", "lim", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0);
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 1: SIMPLE OPERATORS
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_lex_neq_operators) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("<> !=", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.count, 2);
    ASSERT_EQ(r.tokens[0].type, TOK_NEQ);
    ASSERT_EQ(r.tokens[1].type, TOK_NEQ);
    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_ends_keyword) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("ENDS WITH", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.count, 2);
    ASSERT_EQ(r.tokens[0].type, TOK_ENDS);
    ASSERT_EQ(r.tokens[1].type, TOK_WITH);
    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_lex_in_is_null) {
    cbm_lex_result_t r = {0};
    int rc = cbm_lex("IN IS NULL", &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.count, 3);
    ASSERT_EQ(r.tokens[0].type, TOK_IN);
    ASSERT_EQ(r.tokens[1].type, TOK_IS);
    ASSERT_EQ(r.tokens[2].type, TOK_NULL_KW);
    cbm_lex_free(&r);
    PASS();
}

TEST(cypher_exec_where_neq) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name <> \"HandleOrder\" RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3); /* ValidateOrder, SubmitOrder, LogError */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_neq_bang) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name != \"HandleOrder\" RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_ends_with) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name ENDS WITH \"Order\" RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* HandleOrder, ValidateOrder, SubmitOrder */
    ASSERT_EQ(r.row_count, 3);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_not) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE NOT f.name = \"HandleOrder\" RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_not_on_relationship_target) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (a:Function)-[:CALLS]->(b:Function) "
                                "WHERE NOT b.name CONTAINS \"Order\" RETURN b.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "LogError");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_mixed_alias_and) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (a:Function)-[:CALLS]->(b:Function) "
        "WHERE a.name = \"HandleOrder\" AND NOT b.name CONTAINS \"Order\" RETURN b.name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "LogError");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_mixed_alias_or) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (a:Function)-[:CALLS]->(b:Function) "
        "WHERE a.name = \"NoSuchFunction\" OR NOT b.name CONTAINS \"Order\" RETURN b.name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "LogError");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_mixed_alias_xor) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc =
        cbm_cypher_execute(s,
                           "MATCH (a:Function)-[:CALLS]->(b:Function) "
                           "WHERE a.name = \"HandleOrder\" XOR b.name = \"LogError\" RETURN b.name",
                           "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "ValidateOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_in) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f) WHERE f.label IN [\"Function\", \"Module\"] RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 5); /* 4 Functions + 1 Module */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_not_in) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f) WHERE NOT f.label IN [\"Module\"] RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4); /* 4 Functions only */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_is_null) {
    /* SubmitOrder has no start_line (defaults to 0, so start_line prop = "0") */
    /* But file_path is set for all. Use a node with missing data. */
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "WithFile",
                     .qualified_name = "test.WithFile",
                     .file_path = "a.go"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "NoFile",
                     .qualified_name = "test.NoFile",
                     .file_path = NULL};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.file_path IS NULL RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1); /* NoFile has NULL file_path */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_is_not_null) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "WithFile",
                     .qualified_name = "test.WithFile",
                     .file_path = "a.go"};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "NoFile",
                     .qualified_name = "test.NoFile",
                     .file_path = NULL};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.file_path IS NOT NULL RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1); /* WithFile */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Empty strings are values, not Cypher null. Keep this distinction across
 * property predicates and coalesce() so an empty indexed property is neither
 * reported as absent nor replaced by a fallback value. */
TEST(cypher_exec_null_predicates_and_coalesce_preserve_empty_strings) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "EmptyValue",
                    .qualified_name = "test.EmptyValue",
                    .file_path = "empty.py",
                    .properties_json = "{\"empty\":\"\",\"explicit_null\":null}"};
    ASSERT_GT(cbm_store_upsert_node(s, &n), 0);

    cbm_cypher_result_t empty_is_null = {0};
    ASSERT_EQ(cbm_cypher_execute(s, "MATCH (f:Function) WHERE f.empty IS NULL RETURN f.name",
                                 "test", 0, &empty_is_null),
              0);
    ASSERT_EQ(empty_is_null.row_count, 0);
    cbm_cypher_result_free(&empty_is_null);

    cbm_cypher_result_t nulls_are_null = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (f:Function) WHERE f.explicit_null IS NULL "
                                 "AND f.missing IS NULL RETURN f.name",
                                 "test", 0, &nulls_are_null),
              0);
    ASSERT_EQ(nulls_are_null.row_count, 1);
    cbm_cypher_result_free(&nulls_are_null);

    cbm_cypher_result_t coalesced = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (f:Function) RETURN coalesce(f.empty, \"fallback\"), "
                                 "coalesce(f.explicit_null, \"fallback\"), "
                                 "coalesce(f.missing, \"fallback\")",
                                 "test", 0, &coalesced),
              0);
    ASSERT_EQ(coalesced.row_count, 1);
    ASSERT_STR_EQ(coalesced.rows[0][0], "");
    ASSERT_STR_EQ(coalesced.rows[0][1], "fallback");
    ASSERT_STR_EQ(coalesced.rows[0][2], "fallback");
    cbm_cypher_result_free(&coalesced);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_return_star) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN * LIMIT 3", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3);
    /* Should have columns: f.name, f.qualified_name, f.label, f.file_path */
    ASSERT_EQ(r.col_count, 4);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_parse_neq) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) WHERE f.name <> \"X\"", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->where);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_CONDITION);
    ASSERT_STR_EQ(q->where->root->cond.op, "<>");
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_in) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f) WHERE f.label IN [\"Function\", \"Module\"]", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_CONDITION);
    ASSERT_STR_EQ(q->where->root->cond.op, "IN");
    ASSERT_EQ(q->where->root->cond.in_value_count, 2);
    ASSERT_STR_EQ(q->where->root->cond.in_values[0], "Function");
    ASSERT_STR_EQ(q->where->root->cond.in_values[1], "Module");
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_is_null) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f) WHERE f.file_path IS NULL", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_STR_EQ(q->where->root->cond.op, "IS NULL");
    cbm_query_free(q);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 2: EXPRESSION TREE
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_exec_where_or) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WHERE f.name = \"HandleOrder\" OR f.name = \"LogError\" RETURN f.name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_complex_bool) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* (name CONTAINS "Order" OR name = "LogError") AND label = "Function" */
    int rc = cbm_cypher_execute(s,
                                "MATCH (f) WHERE (f.name CONTAINS \"Order\" OR f.name = "
                                "\"LogError\") AND f.label = \"Function\" "
                                "RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4); /* HandleOrder, ValidateOrder, SubmitOrder, LogError */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_xor) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* name CONTAINS "Handle" XOR name CONTAINS "Order" → XOR = true when exactly one is true
     * HandleOrder: both true → false
     * ValidateOrder: false, true → true
     * SubmitOrder: false, true → true
     * LogError: false, false → false */
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name CONTAINS \"Handle\" XOR f.name "
                                "CONTAINS \"Order\" RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2); /* ValidateOrder, SubmitOrder */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_where_not_prefix) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE NOT (f.name CONTAINS \"Order\") RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1); /* LogError */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_parse_expr_tree_and_or) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("MATCH (f) WHERE f.a = \"1\" AND f.b = \"2\" OR f.c = \"3\"", &q, &err);
    ASSERT_EQ(rc, 0);
    /* Precedence: AND binds tighter than OR → root is OR */
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_OR);
    ASSERT_EQ(q->where->root->left->type, EXPR_AND);
    ASSERT_EQ(q->where->root->right->type, EXPR_CONDITION);
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_expr_tree_nested) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("MATCH (f) WHERE (f.a = \"1\" OR f.b = \"2\") AND f.c = \"3\"", &q, &err);
    ASSERT_EQ(rc, 0);
    /* Parens override precedence: root is AND, left is OR */
    ASSERT_NOT_NULL(q->where->root);
    ASSERT_EQ(q->where->root->type, EXPR_AND);
    ASSERT_EQ(q->where->root->left->type, EXPR_OR);
    ASSERT_EQ(q->where->root->right->type, EXPR_CONDITION);
    cbm_query_free(q);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 3: UNSUPPORTED KEYWORD ERRORS
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_error_create) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("CREATE (n:Node {name: \"X\"})", &q, &err);
    ASSERT_EQ(rc, -1);
    ASSERT_NOT_NULL(err);
    ASSERT(strstr(err, "CREATE") != NULL);
    free(err);
    PASS();
}

TEST(cypher_error_delete) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("DELETE n", &q, &err);
    ASSERT_EQ(rc, -1);
    ASSERT_NOT_NULL(err);
    ASSERT(strstr(err, "DELETE") != NULL);
    free(err);
    PASS();
}

TEST(cypher_error_set) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("SET n.name = \"X\"", &q, &err);
    ASSERT_EQ(rc, -1);
    ASSERT_NOT_NULL(err);
    ASSERT(strstr(err, "SET") != NULL);
    free(err);
    PASS();
}

TEST(cypher_error_merge) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MERGE (n:Node)", &q, &err);
    ASSERT_EQ(rc, -1);
    ASSERT_NOT_NULL(err);
    ASSERT(strstr(err, "MERGE") != NULL);
    free(err);
    PASS();
}

TEST(cypher_error_call) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("CALL db.labels()", &q, &err);
    ASSERT_EQ(rc, -1);
    ASSERT_NOT_NULL(err);
    ASSERT(strstr(err, "CALL") != NULL);
    free(err);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 4: SKIP + GENERALIZED AGGREGATION
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_exec_skip) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name ORDER BY f.name ASC SKIP 2",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* 4 functions ordered: HandleOrder, LogError, SubmitOrder, ValidateOrder → skip 2 = 2 */
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "SubmitOrder");
    ASSERT_STR_EQ(r.rows[1][0], "ValidateOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_skip_limit) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) RETURN f.name ORDER BY f.name ASC SKIP 1 LIMIT 2", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "LogError");
    ASSERT_STR_EQ(r.rows[1][0], "SubmitOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Regression for #1334: a LIMIT must survive a multi-key ORDER BY. The parser
 * used to stop at the first sort key, leaving ", key2 ... LIMIT n" unconsumed —
 * the whole result set came back (6326 rows instead of 5 on the reporter's
 * graph: a token-flood into agent context). */
TEST(cypher_exec_multikey_order_by_keeps_limit_issue1334) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* start_lines: HandleOrder=10, ValidateOrder=5, SubmitOrder=0, LogError=0 */
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) RETURN f.name, f.start_line "
        "ORDER BY f.start_line DESC, f.name ASC LIMIT 2",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    ASSERT_STR_EQ(r.rows[1][0], "ValidateOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #1334: the secondary key must actually break ties, per-key direction. */
TEST(cypher_exec_multikey_order_by_tiebreak_issue1334) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* start_line ASC puts the two 0-line functions first; name DESC breaks the
     * tie: SubmitOrder before LogError. */
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) RETURN f.name, f.start_line "
        "ORDER BY f.start_line ASC, f.name DESC LIMIT 2",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "SubmitOrder");
    ASSERT_STR_EQ(r.rows[1][0], "LogError");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #1334: the WITH-clause pipeline has the same multi-key ORDER BY contract. */
TEST(cypher_exec_with_multikey_order_by_keeps_limit_issue1334) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WITH f.name AS n, f.start_line AS sl "
                                "ORDER BY sl DESC, n ASC LIMIT 2 RETURN n",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    ASSERT_STR_EQ(r.rows[1][0], "ValidateOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_sum) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* start_lines: HandleOrder=10, ValidateOrder=5, SubmitOrder=0, LogError=0 → sum=15 */
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN SUM(f.start_line) AS total", "test",
                                0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "15");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_avg) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* start_lines: 10, 5, 0, 0 → avg = 3.75 */
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN AVG(f.start_line) AS avg_line",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "3.75");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_min) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* Among functions with nonzero: HandleOrder=10, ValidateOrder=5 → but MIN is 0 from others */
    int rc =
        cbm_cypher_execute(s, "MATCH (f:Function) RETURN MIN(f.start_line) AS mn", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "0");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_max) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc =
        cbm_cypher_execute(s, "MATCH (f:Function) RETURN MAX(f.start_line) AS mx", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "10");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_collect) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                "WHERE f.name = \"HandleOrder\" "
                                "RETURN f.name, COLLECT(g.name) AS callees",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    /* Should be a JSON array like ["ValidateOrder","LogError"] */
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    ASSERT(strstr(r.rows[0][1], "ValidateOrder") != NULL);
    ASSERT(strstr(r.rows[0][1], "LogError") != NULL);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_count_star) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN COUNT(*) AS n", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "4");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #1111: type(r) grouped with count(*) must return the actual relationship type,
 * not the row count. ret_agg_build_key/ret_agg_emit_row classified aggregate vs.
 * scalar columns with a bare `item->func` truthy check, so type(r) (a non-aggregate
 * function, func != NULL) was misrouted into the aggregate-value branch and
 * formatted via format_agg_value's default case, silently substituting the row
 * count for the relationship type. */
TEST(cypher_issue1111_return_type_count_group) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (a)-[r]->(b) RETURN type(r) AS t, count(*) AS n ORDER BY n DESC", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "CALLS");
    ASSERT_STR_EQ(r.rows[0][1], "3");
    ASSERT_STR_EQ(r.rows[1][0], "DEFINES");
    ASSERT_STR_EQ(r.rows[1][1], "1");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_parse_skip) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f) RETURN f.name SKIP 5 LIMIT 10", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->ret);
    ASSERT_EQ(q->ret->skip, 5);
    ASSERT_EQ(q->ret->limit, 10);
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_sum_avg) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f) RETURN SUM(f.x) AS s, AVG(f.y) AS a", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(q->ret->count, 2);
    ASSERT_STR_EQ(q->ret->items[0].func, "SUM");
    ASSERT_STR_EQ(q->ret->items[0].alias, "s");
    ASSERT_STR_EQ(q->ret->items[1].func, "AVG");
    ASSERT_STR_EQ(q->ret->items[1].alias, "a");
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_collect) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f)-[:CALLS]->(g) RETURN f.name, COLLECT(g.name) AS names", &q,
                              &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(q->ret->count, 2);
    ASSERT_STR_EQ(q->ret->items[1].func, "COLLECT");
    ASSERT_STR_EQ(q->ret->items[1].alias, "names");
    cbm_query_free(q);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 5: STRING FUNCTIONS + CASE
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_exec_tolower) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN toLower(f.name) AS lower_name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "handleorder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_toupper) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN toUpper(f.name) AS upper_name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "HANDLEORDER");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_tostring) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN toString(f.start_line) AS sl",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "10");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_case) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WHERE f.name = \"HandleOrder\" "
        "RETURN CASE WHEN f.start_line > \"5\" THEN \"high\" ELSE \"low\" END AS pos",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "high");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_parse_tolower) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f) RETURN toLower(f.name) AS n", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(q->ret->items[0].func, "toLower");
    ASSERT_STR_EQ(q->ret->items[0].alias, "n");
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_case) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse(
        "MATCH (f) RETURN CASE WHEN f.x = \"1\" THEN \"a\" ELSE \"b\" END AS val", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->ret->items[0].kase);
    ASSERT_EQ(q->ret->items[0].kase->branch_count, 1);
    ASSERT_STR_EQ(q->ret->items[0].kase->branches[0].then_val, "a");
    ASSERT_STR_EQ(q->ret->items[0].kase->else_val, "b");
    cbm_query_free(q);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 6: WITH CLAUSE
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_exec_with_rename) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" "
                                "WITH f.name AS fname RETURN fname",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_with_count) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                "WITH f.name AS caller, COUNT(g) AS cnt "
                                "RETURN caller, cnt ORDER BY cnt DESC",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.row_count, 1);
    /* HandleOrder calls 2 (ValidateOrder, LogError), ValidateOrder calls 1 (SubmitOrder) */
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    ASSERT_STR_EQ(r.rows[0][1], "2");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Regression: a bare node group-var carried through WITH aggregation must project
 * its real properties (not blank). Pre-fix, the carried var held only the node
 * name, so RETURN g.file_path returned "". */
TEST(cypher_exec_with_node_groupvar_prop) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                "WHERE g.name = \"ValidateOrder\" "
                                "WITH g, COUNT(*) AS c "
                                "RETURN g.file_path, g.name, g.qualified_name, c",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "validate.go"); /* was "" before the fix */
    ASSERT_STR_EQ(r.rows[0][1], "ValidateOrder");
    ASSERT_STR_EQ(r.rows[0][2], "test.ValidateOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #1111, WITH variant: the same misrouting in with_agg_build_key/with_agg_accumulate/
 * execute_with_aggregate's per-column func check. */
TEST(cypher_issue1111_with_type_count_group) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (a)-[r]->(b) WITH type(r) AS t, count(*) AS n RETURN t, n ORDER BY n DESC",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "CALLS");
    ASSERT_STR_EQ(r.rows[0][1], "3");
    ASSERT_STR_EQ(r.rows[1][0], "DEFINES");
    ASSERT_STR_EQ(r.rows[1][1], "1");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #1111 follow-up (review from DeusData on #1221): with_agg_find_or_create's
 * bare-node-carry check only tested `!property && variable`, so an entity-
 * introspection alias like `labels(f) AS l` (variable set, property NULL, func
 * set) was ALSO tagged with the source node's id. A later `l.file_path` then
 * hit node_prop's stub re-fetch heuristic (id set, file_path/label both NULL on
 * the virtual stub) and silently returned HandleOrder's real file_path instead
 * of "" for the non-node alias `l`. */
TEST(cypher_issue1111_with_scalar_func_alias_no_node_leak) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" "
                                "WITH labels(f) AS l, COUNT(*) AS c "
                                "RETURN l, l.file_path, c",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "[\"Function\"]");
    ASSERT_STR_EQ(r.rows[0][1], ""); /* was "handler.go" before the fix */
    ASSERT_STR_EQ(r.rows[0][2], "1");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_with_where) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                "WITH f.name AS caller, COUNT(g) AS cnt "
                                "WHERE cnt > \"1\" "
                                "RETURN caller, cnt",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* Only HandleOrder has cnt > 1 (cnt=2) */
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_with_orderby_limit) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                "WITH f.name AS caller, COUNT(g) AS cnt "
                                "ORDER BY cnt DESC LIMIT 1 "
                                "RETURN caller, cnt",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_parse_with) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse(
        "MATCH (f)-[:CALLS]->(g) WITH f.name AS caller, COUNT(g) AS cnt RETURN caller, cnt", &q,
        &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->with_clause);
    ASSERT_EQ(q->with_clause->count, 2);
    ASSERT_STR_EQ(q->with_clause->items[0].alias, "caller");
    ASSERT_STR_EQ(q->with_clause->items[1].func, "COUNT");
    ASSERT_NOT_NULL(q->ret);
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_with_where) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f)-[:CALLS]->(g) WITH f.name AS caller, COUNT(g) AS cnt "
                              "WHERE cnt > \"1\" RETURN caller",
                              &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->with_clause);
    ASSERT_NOT_NULL(q->post_with_where);
    ASSERT_NOT_NULL(q->post_with_where->root);
    ASSERT_NOT_NULL(q->ret);
    cbm_query_free(q);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 7: OPTIONAL MATCH + MULTIPLE MATCH
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_exec_optional_match_no_result) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* LogError has no CALLS outbound edges → OPTIONAL MATCH keeps binding with empty target */
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"LogError\" "
                                "OPTIONAL MATCH (f)-[:CALLS]->(g:Function) "
                                "RETURN f.name, g.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "LogError");
    /* g.name should be empty since OPTIONAL MATCH found nothing */
    ASSERT_STR_EQ(r.rows[0][1], "");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_optional_match_null_aggregates) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = 'LogError' "
                                "OPTIONAL MATCH (f)-[:CALLS]->(g:Function) "
                                "RETURN count(g), count(DISTINCT g), count(*), collect(g)",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    /* Cypher count(expression), count(DISTINCT expression), and collect()
     * ignore the null introduced by OPTIONAL MATCH; count(*) counts its row. */
    ASSERT_STR_EQ(r.rows[0][0], "0");
    ASSERT_STR_EQ(r.rows[0][1], "0");
    ASSERT_STR_EQ(r.rows[0][2], "1");
    ASSERT_STR_EQ(r.rows[0][3], "[]");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_optional_match_null_count_survives_with) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WHERE f.name = 'LogError' "
        "OPTIONAL MATCH (f)-[:CALLS]->(g:Function) "
        "WITH f, count(g) AS targets, count(DISTINCT g) AS distinct_targets, count(*) AS rows "
        "RETURN f.name, targets, distinct_targets, rows",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "LogError");
    ASSERT_STR_EQ(r.rows[0][1], "0");
    ASSERT_STR_EQ(r.rows[0][2], "0");
    ASSERT_STR_EQ(r.rows[0][3], "1");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_aggregates_distinguish_null_from_empty_string) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WHERE f.name = 'LogError' "
        "RETURN count(f.empty_value), count(f.null_value), count(f.absent_value), "
        "collect(f.empty_value), collect(f.null_value), collect(f.absent_value)",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "1");
    ASSERT_STR_EQ(r.rows[0][1], "0");
    ASSERT_STR_EQ(r.rows[0][2], "0");
    ASSERT_STR_EQ(r.rows[0][3], "[\"\"]");
    ASSERT_STR_EQ(r.rows[0][4], "[]");
    ASSERT_STR_EQ(r.rows[0][5], "[]");
    cbm_cypher_result_free(&r);

    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s,
                            "MATCH (f:Function) WHERE f.name = 'LogError' "
                            "WITH f.empty_value AS empty, f.null_value AS explicit_null, "
                            "f.absent_value AS missing "
                            "RETURN count(empty), count(explicit_null), count(missing)",
                            "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "1");
    ASSERT_STR_EQ(r.rows[0][1], "0");
    ASSERT_STR_EQ(r.rows[0][2], "0");
    cbm_cypher_result_free(&r);

    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s,
                            "MATCH (f:Function) "
                            "RETURN f.empty_value, count(*) AS rows ORDER BY rows",
                            "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "");
    ASSERT_STR_EQ(r.rows[0][1], "1");
    ASSERT_STR_EQ(r.rows[1][0], "");
    ASSERT_STR_EQ(r.rows[1][1], "3");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_optional_match_null_numeric_aggregates) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WHERE f.name = 'LogError' "
        "OPTIONAL MATCH (f)-[:CALLS]->(g:Function) "
        "RETURN sum(g.start_line), avg(g.start_line), min(g.start_line), max(g.start_line)",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "0");
    ASSERT_STR_EQ(r.rows[0][1], "");
    ASSERT_STR_EQ(r.rows[0][2], "");
    ASSERT_STR_EQ(r.rows[0][3], "");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_grouping_uses_node_identity_not_display_name) {
    cbm_store_t *s = setup_cypher_store();
    cbm_node_t first = {.project = "test",
                        .label = "Function",
                        .name = "SharedName",
                        .qualified_name = "test.alpha.SharedName",
                        .file_path = "alpha.go"};
    cbm_node_t second = {.project = "test",
                         .label = "Function",
                         .name = "SharedName",
                         .qualified_name = "test.beta.SharedName",
                         .file_path = "beta.go"};
    ASSERT_GT(cbm_store_upsert_node(s, &first), 0);
    ASSERT_GT(cbm_store_upsert_node(s, &second), 0);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = 'SharedName' "
                                "WITH f, count(*) AS rows "
                                "RETURN id(f), f.qualified_name, rows ORDER BY f.qualified_name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][1], "test.alpha.SharedName");
    ASSERT_STR_EQ(r.rows[0][2], "1");
    ASSERT_STR_EQ(r.rows[1][1], "test.beta.SharedName");
    ASSERT_STR_EQ(r.rows[1][2], "1");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_optional_match_has_result) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" "
                                "OPTIONAL MATCH (f)-[:CALLS]->(g:Function) "
                                "RETURN f.name, g.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2); /* ValidateOrder, LogError */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_optional_match_bound_terminal_no_callers) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) "
                                "OPTIONAL MATCH (c:Function)-[:CALLS]->(f) "
                                "WHERE c IS NULL "
                                "RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* WHERE belongs to OPTIONAL MATCH. A non-null caller fails `c IS NULL`,
     * so that optional pattern has no match and the outer function row is
     * preserved with c=null; every function therefore remains. */
    ASSERT_EQ(r.row_count, 4);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_optional_where_after_with_null_extends_failed_candidates) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WITH f OPTIONAL MATCH (f)-[:CALLS]->(g:Function) "
        "WHERE g.name = 'NoSuchFunction' RETURN f.name, g.name ORDER BY f.name ASC",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4);
    for (int i = 0; i < r.row_count; i++) {
        ASSERT_STR_EQ(r.rows[i][1], "");
    }
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_node_only_optional_where_null_extends_failed_candidates) {
    cbm_store_t *s = setup_cypher_store();
    cbm_query_t *q = NULL;
    char *error = NULL;
    ASSERT_EQ(cbm_cypher_parse("MATCH (f:Function) WITH f OPTIONAL MATCH (g:Function) "
                               "WHERE g.name = 'NoSuchFunction' RETURN f.name, g.name",
                               &q, &error),
              0);
    ASSERT_NOT_NULL(q->next_stage);
    ASSERT(q->next_stage->pattern_optional[0]);
    ASSERT_NOT_NULL(q->next_stage->where);
    cbm_query_free(q);
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WITH f OPTIONAL MATCH (g:Function) "
        "WHERE g.name = 'NoSuchFunction' RETURN f.name, g.name ORDER BY f.name ASC",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4);
    for (int i = 0; i < r.row_count; i++) {
        ASSERT_STR_EQ(r.rows[i][1], "");
    }
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_union_after_with_stage_executes_both_branches) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WHERE f.name = 'HandleOrder' WITH f "
        "MATCH (f)-[:CALLS]->(g:Function) RETURN g.name AS name "
        "UNION ALL MATCH (h:Function) WHERE h.name = 'LogError' RETURN h.name AS name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3);
    ASSERT_STR_EQ(r.rows[0][0], "ValidateOrder");
    ASSERT_STR_EQ(r.rows[1][0], "LogError");
    ASSERT_STR_EQ(r.rows[2][0], "LogError");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_multi_match) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* Two MATCH clauses: first finds a module, second finds functions */
    int rc =
        cbm_cypher_execute(s,
                           "MATCH (m:Module) MATCH (f:Function) WHERE f.name CONTAINS \"Order\" "
                           "RETURN m.name, f.name",
                           "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* 1 module × 3 *Order functions = 3 */
    ASSERT_EQ(r.row_count, 3);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_relationship_cross_join_grows_past_fanout_heuristic) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "fanout", "/tmp/fanout");

    cbm_node_t root = {
        .project = "fanout", .label = "Module", .name = "root", .qualified_name = "fanout.root"};
    ASSERT_GT(cbm_store_upsert_node(s, &root), 0);

    int64_t ids[12] = {0};
    char names[12][16] = {{0}};
    char qualified_names[12][32] = {{0}};
    for (int i = 0; i < 12; i++) {
        snprintf(names[i], sizeof(names[i]), "fan_%02d", i);
        snprintf(qualified_names[i], sizeof(qualified_names[i]), "fanout.%s", names[i]);
        cbm_node_t node = {.project = "fanout",
                           .label = "Fan",
                           .name = names[i],
                           .qualified_name = qualified_names[i]};
        ids[i] = cbm_store_upsert_node(s, &node);
        ASSERT_GT(ids[i], 0);
    }
    for (int source = 0; source < 12; source++) {
        for (int target = 0; target < 12; target++) {
            if (source == target) {
                continue;
            }
            cbm_edge_t edge = {.project = "fanout",
                               .source_id = ids[source],
                               .target_id = ids[target],
                               .type = "CALLS"};
            ASSERT_GT(cbm_store_insert_edge(s, &edge), 0);
        }
    }

    cbm_cypher_result_t r = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (m:Module) MATCH (a:Fan)-[:CALLS]->(b:Fan) "
                                 "RETURN a.name, b.name",
                                 "fanout", 1000, &r),
              0);
    ASSERT_EQ(r.row_count, 132);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_parse_optional_match) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse(
        "MATCH (f:Function) OPTIONAL MATCH (f)-[:CALLS]->(g) RETURN f.name, g.name", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(q->pattern_count, 2);
    ASSERT(!q->pattern_optional[0]);
    ASSERT(q->pattern_optional[1]);
    cbm_query_free(q);
    PASS();
}

TEST(cypher_exec_optional_match_after_with_uses_projected_rows) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s,
        "MATCH (caller:Function)-[:CALLS]->(target:Function) "
        "WITH target, count(DISTINCT caller) AS caller_count "
        "OPTIONAL MATCH (target)-[:CALLS]->(next:Function) "
        "RETURN target.name AS target_name, caller_count, next.name AS next_name "
        "ORDER BY target_name ASC",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3);
    ASSERT_STR_EQ(r.rows[0][0], "LogError");
    ASSERT_STR_EQ(r.rows[0][1], "1");
    ASSERT_STR_EQ(r.rows[0][2], "");
    ASSERT_STR_EQ(r.rows[1][0], "SubmitOrder");
    ASSERT_STR_EQ(r.rows[1][2], "");
    ASSERT_STR_EQ(r.rows[2][0], "ValidateOrder");
    ASSERT_STR_EQ(r.rows[2][2], "SubmitOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_simple_with_carries_node_identity) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WITH f MATCH (f)-[:CALLS]->(g:Function) "
                                "RETURN f.name, g.name ORDER BY f.name ASC, g.name ASC",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 3);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    ASSERT_STR_EQ(r.rows[0][1], "LogError");
    ASSERT_STR_EQ(r.rows[1][0], "HandleOrder");
    ASSERT_STR_EQ(r.rows[1][1], "ValidateOrder");
    ASSERT_STR_EQ(r.rows[2][0], "ValidateOrder");
    ASSERT_STR_EQ(r.rows[2][1], "SubmitOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_multiple_with_match_stages) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc =
        cbm_cypher_execute(s,
                           "MATCH (f:Function) WITH f MATCH (f)-[:CALLS]->(g:Function) WITH f, g "
                           "MATCH (g)-[:CALLS]->(h:Function) RETURN f.name, g.name, h.name",
                           "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "HandleOrder");
    ASSERT_STR_EQ(r.rows[0][1], "ValidateOrder");
    ASSERT_STR_EQ(r.rows[0][2], "SubmitOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_multi_key_order_by_mixed_directions) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (n) RETURN n.label, n.name ORDER BY n.label ASC, n.name DESC",
                              &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(err);
    ASSERT_EQ(q->ret->order_count, 2);
    ASSERT_STR_EQ(q->ret->order_items[0].expression, "n.label");
    ASSERT_STR_EQ(q->ret->order_items[0].direction, "ASC");
    ASSERT_STR_EQ(q->ret->order_items[1].expression, "n.name");
    ASSERT_STR_EQ(q->ret->order_items[1].direction, "DESC");
    cbm_query_free(q);

    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    rc = cbm_cypher_execute(s,
                            "MATCH (n) RETURN n.label, n.name "
                            "ORDER BY n.label ASC, n.name DESC",
                            "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_GTE(r.row_count, 4);
    ASSERT_STR_EQ(r.rows[0][0], "Function");
    ASSERT_STR_EQ(r.rows[0][1], "ValidateOrder");
    ASSERT_STR_EQ(r.rows[1][1], "SubmitOrder");
    ASSERT_STR_EQ(r.rows[2][1], "LogError");
    ASSERT_STR_EQ(r.rows[3][1], "HandleOrder");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_order_by_rejects_unprojected_key_with_rewrite) {
    cbm_query_t *q = NULL;
    char *error = NULL;
    int rc = cbm_cypher_parse("MATCH (n) RETURN n.name ORDER BY n.label", &q, &error);
    ASSERT_EQ(rc, -1);
    ASSERT_NULL(q);
    ASSERT_NOT_NULL(error);
    ASSERT_NOT_NULL(strstr(error, "not projected"));
    ASSERT_NOT_NULL(strstr(error, "add it to RETURN"));
    free(error);
    PASS();
}

TEST(cypher_with_order_by_allows_carried_node_property) {
    cbm_query_t *q = NULL;
    char *error = NULL;
    int rc = cbm_cypher_parse(
        "MATCH (n) WITH n ORDER BY n.name MATCH (n)-[:CALLS]->(m) RETURN m.name", &q, &error);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q);
    ASSERT_NULL(error);
    cbm_query_free(q);
    PASS();
}

TEST(cypher_exec_multi_key_order_by_nulls_and_limit) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) OPTIONAL MATCH (f)-[:CALLS]->(g:Function) "
                                "RETURN f.name, g.name ORDER BY g.name ASC, f.name ASC LIMIT 5",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 5);
    ASSERT_STR_EQ(r.rows[0][1], "LogError");
    ASSERT_STR_EQ(r.rows[1][1], "SubmitOrder");
    ASSERT_STR_EQ(r.rows[2][1], "ValidateOrder");
    /* Cypher places nulls last for ascending order; this engine's wire
     * representation for a missing optional value is the empty string. */
    ASSERT_STR_EQ(r.rows[3][0], "LogError");
    ASSERT_STR_EQ(r.rows[3][1], "");
    ASSERT_STR_EQ(r.rows[4][0], "SubmitOrder");
    ASSERT_STR_EQ(r.rows[4][1], "");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_parse_multi_match) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("MATCH (a:Module) MATCH (b:Function) RETURN a.name, b.name", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(q->pattern_count, 2);
    ASSERT(!q->pattern_optional[0]);
    ASSERT(!q->pattern_optional[1]);
    cbm_query_free(q);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 8: UNION / UNION ALL
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_exec_union) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN f.name "
                                "UNION "
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* UNION deduplicates → 1 row */
    ASSERT_EQ(r.row_count, 1);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_union_all) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN f.name "
                                "UNION ALL "
                                "MATCH (f:Function) WHERE f.name = \"HandleOrder\" RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    /* UNION ALL keeps duplicates → 2 rows */
    ASSERT_EQ(r.row_count, 2);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_union_requires_identical_column_schema) {
    cbm_store_t *store = setup_cypher_store();
    ASSERT_NOT_NULL(store);
    const char *queries[] = {
        "MATCH (f:Function) RETURN f.name AS name UNION ALL "
        "MATCH (g:Function) RETURN g.name AS different_name",
        "MATCH (f:Function) RETURN f.name AS name UNION ALL "
        "MATCH (g:Function) RETURN g.name AS name, g.label AS label",
    };
    for (size_t query_index = 0; query_index < sizeof(queries) / sizeof(queries[0]);
         query_index++) {
        cbm_cypher_result_t result = {0};
        int rc = cbm_cypher_execute(store, queries[query_index], "test", 0, &result);
        ASSERT_TRUE(rc != 0);
        ASSERT_NOT_NULL(result.error);
        ASSERT_NOT_NULL(strstr(result.error, "identical column"));
        ASSERT_EQ(result.row_count, 0);
        cbm_cypher_result_free(&result);
    }
    cbm_store_close(store);
    PASS();
}

TEST(cypher_exec_union_all_respects_caller_output_cap) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) WHERE f.name CONTAINS \"Order\" RETURN f.name "
                                "UNION ALL "
                                "MATCH (f:Function) WHERE f.name CONTAINS \"Order\" RETURN f.name",
                                "test", 2, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_TRUE(r.truncated);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_union_deduplicates_complete_branches_before_output_cap) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "union-cap", "/tmp/union-cap"), CBM_STORE_OK);
    const char *names[] = {"Duplicate", "Duplicate", "UniqueAfterDuplicates"};
    for (int i = 0; i < 3; i++) {
        char qn[CBM_SZ_64];
        snprintf(qn, sizeof(qn), "union.cap.%d", i);
        cbm_node_t node = {.project = "union-cap",
                           .label = "Function",
                           .name = names[i],
                           .qualified_name = qn,
                           .file_path = "src/union.c"};
        ASSERT_GT(cbm_store_upsert_node(s, &node), 0);
    }

    const char *query = "MATCH (f:Function) RETURN f.name AS name "
                        "UNION "
                        "MATCH (m:Module) WHERE m.name = \"Missing\" RETURN m.name AS name";
    cbm_cypher_limits_t limits = {.max_output_rows = 2, .max_working_rows = 3};
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute_with_limits(s, query, "union-cap", &limits, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "Duplicate");
    ASSERT_STR_EQ(r.rows[1][0], "UniqueAfterDuplicates");
    ASSERT_FALSE(r.truncated);
    cbm_cypher_result_free(&r);

    limits.max_working_rows = 2;
    rc = cbm_cypher_execute_with_limits(s, query, "union-cap", &limits, &r);
    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "working-row budget (2)"));
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_limits_separate_output_cap_from_working_budget) {
    enum { NAME_SIZE = 32, QN_SIZE = 64 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "limits", "/tmp/limits"), CBM_STORE_OK);
    for (int i = 0; i < 2; i++) {
        char name[NAME_SIZE];
        char qn[QN_SIZE];
        snprintf(name, sizeof(name), "Fn%d", i);
        snprintf(qn, sizeof(qn), "limits.Fn%d", i);
        cbm_node_t node = {.project = "limits",
                           .label = "Function",
                           .name = name,
                           .qualified_name = qn,
                           .file_path = "src/limits.c"};
        ASSERT_GT(cbm_store_upsert_node(s, &node), 0);
    }

    const char *query = "MATCH (a:Function) MATCH (b:Function) RETURN a.name, b.name";
    cbm_cypher_limits_t limits = {.max_output_rows = 4, .max_working_rows = 2};
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute_with_limits(s, query, "limits", &limits, &r);
    ASSERT_EQ(rc, 0);
    /* An explicit output cap raises the effective working budget, and reaching
     * that budget exactly is valid: two nodes cross-join to four rows. */
    ASSERT_EQ(r.row_count, 4);
    cbm_cypher_result_free(&r);

    limits.max_output_rows = 1;
    rc = cbm_cypher_execute_with_limits(s, query, "limits", &limits, &r);
    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "working-row budget (2)"));
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_working_budget_replaces_silent_bfs_prefix_cap) {
    enum { DECOY_COUNT = 100, NAME_SIZE = 32, QN_SIZE = 64 };
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "bfs-budget", "/tmp/bfs-budget"), CBM_STORE_OK);

    cbm_node_t root = {.project = "bfs-budget",
                       .label = "Module",
                       .name = "Root",
                       .qualified_name = "bfs.Root",
                       .file_path = "src/bfs.c"};
    int64_t root_id = cbm_store_upsert_node(s, &root);
    ASSERT_GT(root_id, 0);
    for (int i = 0; i <= DECOY_COUNT; i++) {
        char name[NAME_SIZE];
        char qn[QN_SIZE];
        bool target = i == DECOY_COUNT;
        snprintf(name, sizeof(name), target ? "TargetAfterHundred" : "Decoy%03d", i);
        snprintf(qn, sizeof(qn), "bfs.%s", name);
        cbm_node_t node = {.project = "bfs-budget",
                           .label = "Function",
                           .name = name,
                           .qualified_name = qn,
                           .file_path = "src/bfs.c"};
        int64_t node_id = cbm_store_upsert_node(s, &node);
        ASSERT_GT(node_id, 0);
        cbm_edge_t edge = {
            .project = "bfs-budget", .source_id = root_id, .target_id = node_id, .type = "CALLS"};
        ASSERT_GT(cbm_store_insert_edge(s, &edge), 0);
    }

    const char *query = "MATCH (a:Module {name: \"Root\"})-[:CALLS*1..2]->"
                        "(b:Function {name: \"TargetAfterHundred\"}) RETURN b.name";
    cbm_cypher_limits_t limits = {.max_output_rows = 1, .max_working_rows = 101};
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute_with_limits(s, query, "bfs-budget", &limits, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "TargetAfterHundred");
    cbm_cypher_result_free(&r);

    limits.max_working_rows = 100;
    rc = cbm_cypher_execute_with_limits(s, query, "bfs-budget", &limits, &r);
    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "working-row budget (100)"));
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_working_budget_bounds_initial_scan_without_prefix_answer) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "scan-budget", "/tmp/scan-budget"), CBM_STORE_OK);
    const char *names[] = {"Decoy0", "Decoy1", "TargetAfterBudget"};
    for (int i = 0; i < 3; i++) {
        char qn[CBM_SZ_64];
        snprintf(qn, sizeof(qn), "scan.%s", names[i]);
        cbm_node_t node = {.project = "scan-budget",
                           .label = "Function",
                           .name = names[i],
                           .qualified_name = qn,
                           .file_path = "src/scan.c"};
        ASSERT_GT(cbm_store_upsert_node(s, &node), 0);
    }

    const char *query = "MATCH (f:Function {name: \"TargetAfterBudget\"}) RETURN f.name";
    cbm_cypher_limits_t limits = {.max_output_rows = 1, .max_working_rows = 2};
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute_with_limits(s, query, "scan-budget", &limits, &r);
    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "working-row budget (2)"));
    ASSERT_EQ(r.row_count, 0);
    cbm_cypher_result_free(&r);

    limits.max_working_rows = 3;
    rc = cbm_cypher_execute_with_limits(s, query, "scan-budget", &limits, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "TargetAfterBudget");
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

TEST(cypher_parse_union) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("MATCH (f) RETURN f.name UNION ALL MATCH (g) RETURN g.name", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT(q->union_all);
    ASSERT_NOT_NULL(q->union_next);
    cbm_query_free(q);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 9: UNWIND
 * ══════════════════════════════════════════════════════════════════ */

TEST(cypher_parse_unwind) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc =
        cbm_cypher_parse("UNWIND [\"a\", \"b\", \"c\"] AS x MATCH (f) RETURN f.name", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->unwind_expr);
    ASSERT_STR_EQ(q->unwind_expr, "[\"a\",\"b\",\"c\"]");
    ASSERT_STR_EQ(q->unwind_alias, "x");
    cbm_query_free(q);
    PASS();
}

TEST(cypher_parse_unwind_var) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("UNWIND items AS item MATCH (f) RETURN f.name", &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(q->unwind_expr, "items");
    ASSERT_STR_EQ(q->unwind_alias, "item");
    cbm_query_free(q);
    PASS();
}

/* Parsing UNWIND without consuming its list made the clause a silent no-op:
 * the result cardinality stayed at the MATCH cardinality and the alias
 * projected as null. Pin the observable language contract, not just the AST
 * fields, so a write-only unwind_expr/unwind_alias pair cannot recur. */
TEST(cypher_exec_unwind_literal_multiplies_rows_and_binds_alias) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "unwind", "/tmp/unwind"), CBM_STORE_OK);
    cbm_node_t node = {.project = "unwind",
                       .label = "Function",
                       .name = "target",
                       .qualified_name = "unwind.target",
                       .file_path = "src/unwind.c"};
    ASSERT_GT(cbm_store_upsert_node(s, &node), 0);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "UNWIND [\"alpha\\\"quoted\", \"beta\"] AS item MATCH (f:Function) "
                                "RETURN item, f.name ORDER BY item",
                                "unwind", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.col_count, 2);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "alpha\"quoted");
    ASSERT_STR_EQ(r.rows[0][1], "target");
    ASSERT_STR_EQ(r.rows[1][0], "beta");
    ASSERT_STR_EQ(r.rows[1][1], "target");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_unwind_empty_list_returns_no_rows) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "unwind-empty", "/tmp/unwind-empty"), CBM_STORE_OK);
    cbm_node_t node = {.project = "unwind-empty",
                       .label = "Function",
                       .name = "target",
                       .qualified_name = "unwind_empty.target",
                       .file_path = "src/unwind.c"};
    ASSERT_GT(cbm_store_upsert_node(s, &node), 0);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "UNWIND [] AS item MATCH (f:Function) RETURN item, f.name",
                                "unwind-empty", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_unwind_variable_without_parameter_scope_fails_loudly) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "UNWIND items AS item MATCH (f) RETURN item", NULL, 0, &r);
    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "without query parameters"));
    ASSERT_EQ(r.row_count, 0);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_unwind_cross_product_obeys_working_row_budget) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "unwind-budget", "/tmp/unwind-budget"), CBM_STORE_OK);
    cbm_node_t node = {.project = "unwind-budget",
                       .label = "Function",
                       .name = "target",
                       .qualified_name = "unwind_budget.target",
                       .file_path = "src/unwind.c"};
    ASSERT_GT(cbm_store_upsert_node(s, &node), 0);

    cbm_cypher_limits_t limits = {.max_output_rows = 1, .max_working_rows = 3};
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute_with_limits(
        s, "UNWIND [1, 2, 3, 4] AS item MATCH (f:Function) RETURN item", "unwind-budget", &limits,
        &r);
    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(r.error);
    ASSERT_NOT_NULL(strstr(r.error, "working-row budget (3)"));
    ASSERT_EQ(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Regression: an UNWIND literal list whose element is longer than the 2KB
 * assembly buffer used to overflow the stack. snprintf reports the length it
 * WOULD have written, so blen ran past sizeof(buf) and the trailing
 * buf[blen++]=']' / buf[blen]='\0' wrote out of bounds (ASan: stack-buffer-
 * overflow). The query text is agent-controlled via the MCP query tool. */
TEST(cypher_parse_unwind_oversized_literal_no_overflow) {
    char query[4096];
    char big[3000];
    memset(big, 'a', sizeof(big) - 1);
    big[sizeof(big) - 1] = '\0';
    snprintf(query, sizeof(query), "UNWIND [\"%s\"] AS x MATCH (f) RETURN f.name", big);

    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse(query, &q, &err);
    /* Must not crash and must produce a NUL-terminated, in-bounds expression. */
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->unwind_expr);
    ASSERT_STR_EQ(q->unwind_alias, "x");
    cbm_query_free(q);
    PASS();
}

/* Regression: many oversized elements accumulate blen well past the buffer,
 * which also underflowed the (size_t)(cap - blen) length passed to snprintf. */
TEST(cypher_parse_unwind_many_elements_no_overflow) {
    /* 200 elements (~20 chars each) accumulate well past the 2KB assembly
     * buffer, which also underflowed the (size_t)(cap - blen) length. */
    char query[8192];
    int off = snprintf(query, sizeof(query), "UNWIND [");
    for (int i = 0; i < 200; i++) {
        off += snprintf(query + off, sizeof(query) - (size_t)off, "%s\"element_value_%d\"",
                        i ? "," : "", i);
    }
    snprintf(query + off, sizeof(query) - (size_t)off, "] AS x MATCH (f) RETURN f.name");

    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse(query, &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(q->unwind_expr);
    cbm_query_free(q);
    PASS();
}

/* ── Issue #389 group: Cypher feature reproductions ─────────────────
 * Each asserts the CORRECT behavior; a failure reproduces the bug. */

/* #240: labels() function */
TEST(cypher_issue240_labels_function) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (n:Module) RETURN labels(n) AS lbl", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.row_count, 1);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_rejects_list_index_after_function_result) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count "
                              "ORDER BY count DESC LIMIT 5",
                              &q, &err);
    ASSERT(rc != 0);
    ASSERT_NULL(q);
    ASSERT_NOT_NULL(err);
    ASSERT_NOT_NULL(strstr(err, "list indexing"));
    ASSERT_NOT_NULL(strstr(err, "labels(n) AS labels"));
    ASSERT_NOT_NULL(strstr(err, "n.label AS label"));
    ASSERT_NOT_NULL(strstr(err, "count(*) AS node_count"));
    free(err);
    PASS();
}

TEST(cypher_rejects_unconsumed_trailing_tokens) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (n) RETURN n.name trailing", &q, &err);
    ASSERT(rc != 0);
    ASSERT_NULL(q);
    ASSERT_NOT_NULL(err);
    ASSERT_NOT_NULL(strstr(err, "unexpected trailing token"));
    free(err);
    PASS();
}

/* #237: DISTINCT applied before ORDER BY + LIMIT */
TEST(cypher_issue237_distinct_order_limit) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) RETURN DISTINCT f.label AS l ORDER BY l LIMIT 10", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.row_count, 1);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #873: duplicate projected rows must be deduped before ORDER BY + LIMIT */
TEST(cypher_issue873_distinct_order_limit_dedupes_before_limit) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (n) RETURN DISTINCT n.label AS label ORDER BY label LIMIT 2", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.row_count, 2);
    ASSERT_STR_EQ(r.rows[0][0], "Function");
    ASSERT_STR_EQ(r.rows[1][0], "Module");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #873: early LIMIT must not truncate rows before DISTINCT for simple RETURN */
TEST(cypher_issue873_distinct_limit_dedupes_before_limit) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc =
        cbm_cypher_execute(s, "MATCH (n) RETURN DISTINCT n.label AS label LIMIT 2", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.row_count, 2);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #873: SKIP is applied after DISTINCT and ORDER BY, not before dedupe */
TEST(cypher_issue873_distinct_order_skip_limit_dedupes_before_skip) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (n) RETURN DISTINCT n.label AS label ORDER BY label SKIP 1 LIMIT 1", "test", 0,
        &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "Module");
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #252: toInteger() */
TEST(cypher_issue252_tointeger) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN toInteger(f.start_line) AS ln",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #305: count(*) + AS alias */
TEST(cypher_issue305_count_star_alias) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (n) RETURN count(*) AS total", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_EQ(r.row_count, 1);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Regression: projecting several computed/JSON properties in one row must yield
 * DISTINCT values. node_prop previously returned a single shared static buffer,
 * so every such column aliased the last property read — and because the search
 * key is matched in the JSON, `loop_depth` must not be confused with its suffix
 * `transitive_loop_depth`. Exercises the bottleneck metrics end-to-end. */
TEST(cypher_multi_prop_projection_no_alias) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "Hot",
                    .qualified_name = "test.Hot",
                    .file_path = "hot.go",
                    .start_line = 10,
                    .end_line = 42,
                    .properties_json = "{\"complexity\":3,\"cognitive\":7,\"loop_count\":2,"
                                       "\"loop_depth\":1,\"self_recursive\":false,"
                                       "\"transitive_loop_depth\":5,\"recursive\":true}"};
    cbm_store_upsert_node(s, &n);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function) RETURN f.loop_depth, f.transitive_loop_depth, "
                                "f.cognitive, f.complexity, f.start_line, f.end_line",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_EQ(r.col_count, 6);
    ASSERT_STR_EQ(r.rows[0][0], "1");  /* loop_depth — NOT the suffix transitive_loop_depth */
    ASSERT_STR_EQ(r.rows[0][1], "5");  /* transitive_loop_depth */
    ASSERT_STR_EQ(r.rows[0][2], "7");  /* cognitive */
    ASSERT_STR_EQ(r.rows[0][3], "3");  /* complexity */
    ASSERT_STR_EQ(r.rows[0][4], "10"); /* start_line (computed) */
    ASSERT_STR_EQ(r.rows[0][5], "42"); /* end_line (computed) — distinct from start_line */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Property projection must return the WHOLE value of composite properties.
 * json_extract_prop() scanned a non-string value up to the first ',' — so an
 * array/object property was truncated at its first INTERNAL comma. Real-world
 * hit: a NestJS handler's decorators
 *   ["@Roles('OWNER', 'ADMIN')","@Get()"]
 * projected as ["@Roles('OWNER'   — unusable for route/authz queries. */
TEST(cypher_exec_prop_array_with_internal_commas) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t n = {.project = "test",
                    .label = "Method",
                    .name = "findAll",
                    .qualified_name = "test.PacienteController.findAll",
                    .file_path = "paciente.controller.ts",
                    .properties_json =
                        "{\"decorators\":[\"@Roles('OWNER', 'ADMIN')\",\"@Get()\"],\"lines\":3}"};
    cbm_store_upsert_node(s, &n);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (m:Method) RETURN m.decorators, m.lines", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    /* whole array, commas and all — was ["@Roles('OWNER' before the fix */
    ASSERT_STR_EQ(r.rows[0][0], "[\"@Roles('OWNER', 'ADMIN')\",\"@Get()\"]");
    ASSERT_STR_EQ(r.rows[0][1], "3"); /* scalar sibling still parses */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* A string property must not end at an ESCAPED quote: the scan stopped at the
 * first '"' regardless of a preceding backslash, cutting the value short. */
TEST(cypher_exec_prop_string_with_escaped_quote) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");
    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "parse",
                    .qualified_name = "test.parse",
                    .file_path = "p.ts",
                    .properties_json = "{\"signature\":\"(sep: \\\"a,b\\\") => void\"}"};
    cbm_store_upsert_node(s, &n);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.signature", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "(sep: \\\"a,b\\\") => void"); /* was: (sep: \ */
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

static cbm_store_t *setup_repeated_dynamic_property_store(const char *key, char fill,
                                                           size_t value_length) {
    size_t key_length = strlen(key);
    size_t json_size = sizeof("{\"") - SKIP_ONE + key_length +
                       sizeof("\":\"") - SKIP_ONE + value_length + sizeof("\"}");
    char *properties_json = malloc(json_size);
    if (!properties_json) {
        return NULL;
    }
    size_t offset = 0;
    memcpy(properties_json + offset, "{\"", sizeof("{\"") - SKIP_ONE);
    offset += sizeof("{\"") - SKIP_ONE;
    memcpy(properties_json + offset, key, key_length);
    offset += key_length;
    memcpy(properties_json + offset, "\":\"", sizeof("\":\"") - SKIP_ONE);
    offset += sizeof("\":\"") - SKIP_ONE;
    memset(properties_json + offset, fill, value_length);
    offset += value_length;
    memcpy(properties_json + offset, "\"}", sizeof("\"}"));

    cbm_store_t *store = cbm_store_open_memory();
    if (!store || cbm_store_upsert_project(store, "test", "/tmp/test") != CBM_STORE_OK) {
        cbm_store_close(store);
        free(properties_json);
        return NULL;
    }
    cbm_node_t node = {.project = "test",
                       .label = "Function",
                       .name = "LargeProperty",
                       .qualified_name = "test.LargeProperty",
                       .file_path = "large.c",
                       .properties_json = properties_json};
    if (cbm_store_upsert_node(store, &node) <= 0) {
        cbm_store_close(store);
        store = NULL;
    }
    free(properties_json);
    return store;
}

static bool text_is_repeated(const char *text, char expected, size_t expected_length) {
    if (!text || strlen(text) != expected_length) {
        return false;
    }
    for (size_t i = 0; i < expected_length; i++) {
        if (text[i] != expected) {
            return false;
        }
    }
    return true;
}

static bool collect_is_single_repeated_string(const char *text, char expected,
                                               size_t expected_length) {
    static const size_t json_array_overhead = sizeof("[\"\"]") - SKIP_ONE;
    if (!text || strlen(text) != expected_length + json_array_overhead || text[0] != '[' ||
        text[SKIP_ONE] != '"' ||
        text[expected_length + json_array_overhead - PAIR_LEN] != '"' ||
        text[expected_length + json_array_overhead - SKIP_ONE] != ']') {
        return false;
    }
    for (size_t i = 0; i < expected_length; i++) {
        if (text[i + PAIR_LEN] != expected) {
            return false;
        }
    }
    return true;
}

TEST(cypher_exec_preserves_large_dynamic_property_in_aggregation) {
    const size_t value_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    cbm_store_t *store = setup_repeated_dynamic_property_store("payload", 'v', value_length);
    ASSERT_NOT_NULL(store);
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store,
                                "MATCH (n:Function) RETURN n.payload, count(*) AS total",
                                "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 2);
    ASSERT_TRUE(text_is_repeated(result.rows[0][0], 'v', value_length));
    ASSERT_STR_EQ(result.rows[0][1], "1");
    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_exec_preserves_large_dynamic_property_across_with) {
    const size_t value_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    cbm_store_t *store = setup_repeated_dynamic_property_store("payload", 'v', value_length);
    ASSERT_NOT_NULL(store);
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store,
                                "MATCH (n:Function) WITH n.payload AS carried RETURN carried",
                                "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_TRUE(text_is_repeated(result.rows[0][0], 'v', value_length));
    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_exec_preserves_large_dynamic_property_across_grouped_with) {
    const size_t value_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    cbm_store_t *store = setup_repeated_dynamic_property_store("payload", 'v', value_length);
    ASSERT_NOT_NULL(store);
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(
                  store,
                  "MATCH (n:Function) WITH n.payload AS carried, count(*) AS total "
                  "RETURN carried, total",
                  "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 2);
    ASSERT_TRUE(text_is_repeated(result.rows[0][0], 'v', value_length));
    ASSERT_STR_EQ(result.rows[0][1], "1");
    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_exec_preserves_query_sized_default_with_alias) {
    static const char prefix[] = "MATCH (n:Function) WITH n.";
    static const char separator[] = " RETURN n.";
    const size_t key_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    const size_t query_size = sizeof(prefix) - SKIP_ONE + key_length +
                              sizeof(separator) - SKIP_ONE + key_length + SKIP_ONE;
    char *key = malloc(key_length + SKIP_ONE);
    char *query = malloc(query_size);
    ASSERT_NOT_NULL(key);
    ASSERT_NOT_NULL(query);
    memset(key, 'k', key_length);
    key[key_length] = '\0';
    size_t offset = 0;
    memcpy(query + offset, prefix, sizeof(prefix) - SKIP_ONE);
    offset += sizeof(prefix) - SKIP_ONE;
    memcpy(query + offset, key, key_length);
    offset += key_length;
    memcpy(query + offset, separator, sizeof(separator) - SKIP_ONE);
    offset += sizeof(separator) - SKIP_ONE;
    memcpy(query + offset, key, key_length);
    offset += key_length;
    query[offset] = '\0';

    cbm_store_t *store = setup_repeated_dynamic_property_store(key, 'v', key_length);
    ASSERT_NOT_NULL(store);
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, query, "test", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_TRUE(text_is_repeated(result.rows[0][0], 'v', key_length));

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    free(query);
    free(key);
    PASS();
}

TEST(cypher_exec_preserves_large_dynamic_property_in_scalar_function) {
    const size_t value_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    cbm_store_t *store = setup_repeated_dynamic_property_store("payload", 'v', value_length);
    ASSERT_NOT_NULL(store);
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) RETURN toUpper(n.payload) AS payload, "
                                 "size(n.payload), length(n.payload), reverse(n.payload), "
                                 "trim(n.payload), ltrim(n.payload), rtrim(n.payload)",
                                 "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 7);
    ASSERT_TRUE(text_is_repeated(result.rows[0][0], 'V', value_length));
    ASSERT_STR_EQ(result.rows[0][1], "5000");
    ASSERT_STR_EQ(result.rows[0][2], "5000");
    for (int column = 3; column < result.col_count; column++) {
        ASSERT_TRUE(text_is_repeated(result.rows[0][column], 'v', value_length));
    }
    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_exec_preserves_large_dynamic_property_in_multiarg_functions) {
    const size_t value_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    cbm_store_t *store = setup_repeated_dynamic_property_store("payload", 'v', value_length);
    ASSERT_NOT_NULL(store);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) RETURN substring(n.payload,0,5000), "
                                 "left(n.payload,5000), right(n.payload,5000), "
                                 "replace(n.payload,'v','w'), coalesce(n.missing,n.payload)",
                                 "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 5);
    for (int column = 0; column < result.col_count; column++) {
        ASSERT_TRUE(
            text_is_repeated(result.rows[0][column], column == 3 ? 'w' : 'v', value_length));
    }
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) WITH replace(n.payload,'v','w') AS payload "
                                 "RETURN payload",
                                 "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_TRUE(text_is_repeated(result.rows[0][0], 'w', value_length));
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) RETURN replace(n.payload,'v','w') AS payload, "
                                 "count(*) AS matches",
                                 "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_TRUE(text_is_repeated(result.rows[0][0], 'w', value_length));
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) WITH replace(n.payload,'v','w') AS payload, "
                                 "count(*) AS matches RETURN payload",
                                 "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_TRUE(text_is_repeated(result.rows[0][0], 'w', value_length));
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) "
                                 "WHERE substring(n.payload,4999,1) = 'v' RETURN n.name",
                                 "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_STR_EQ(result.rows[0][0], "LargeProperty");

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_exec_preserves_large_dynamic_property_in_collect_and_with) {
    const size_t value_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    cbm_store_t *store = setup_repeated_dynamic_property_store("payload", 'v', value_length);
    ASSERT_NOT_NULL(store);
    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, "MATCH (n:Function) RETURN collect(n.payload)", "test",
                                 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_TRUE(collect_is_single_repeated_string(result.rows[0][0], 'v', value_length));
    cbm_cypher_result_free(&result);

    ASSERT_EQ(cbm_cypher_execute(store,
                                "MATCH (n:Function) WITH collect(n.payload) AS values "
                                "RETURN values",
                                "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_TRUE(collect_is_single_repeated_string(result.rows[0][0], 'v', value_length));

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_exec_preserves_query_sized_dynamic_property_key_and_value) {
    static const char json_prefix[] = "{\"";
    static const char json_separator[] = "\":\"";
    static const char json_suffix[] = "\"}";
    static const char query_prefix[] = "MATCH (n:Function) RETURN n.";
    const size_t key_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    const size_t value_length = CYPHER_TEST_LONG_TOKEN_BYTES;
    const size_t json_size = (sizeof(json_prefix) - SKIP_ONE) + key_length +
                             (sizeof(json_separator) - SKIP_ONE) + value_length +
                             sizeof(json_suffix);
    char *properties_json = malloc(json_size);
    ASSERT_NOT_NULL(properties_json);
    size_t offset = 0;
    memcpy(properties_json + offset, json_prefix, sizeof(json_prefix) - SKIP_ONE);
    offset += sizeof(json_prefix) - SKIP_ONE;
    memset(properties_json + offset, 'k', key_length);
    offset += key_length;
    memcpy(properties_json + offset, json_separator, sizeof(json_separator) - SKIP_ONE);
    offset += sizeof(json_separator) - SKIP_ONE;
    memset(properties_json + offset, 'V', value_length);
    offset += value_length;
    memcpy(properties_json + offset, json_suffix, sizeof(json_suffix));

    const size_t query_size = (sizeof(query_prefix) - SKIP_ONE) + key_length + SKIP_ONE;
    char *query = malloc(query_size);
    ASSERT_NOT_NULL(query);
    memcpy(query, query_prefix, sizeof(query_prefix) - SKIP_ONE);
    memset(query + sizeof(query_prefix) - SKIP_ONE, 'k', key_length);
    query[query_size - SKIP_ONE] = '\0';

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_node_t node = {.project = "test",
                       .label = "Function",
                       .name = "LongProperty",
                       .qualified_name = "test.LongProperty",
                       .file_path = "long.c",
                       .properties_json = properties_json};
    ASSERT_GT(cbm_store_upsert_node(store, &node), 0);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, query, "test", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 1);
    ASSERT_EQ(strlen(result.rows[0][0]), value_length);
    for (size_t i = 0; i < value_length; i++) {
        ASSERT_EQ(result.rows[0][0][i], 'V');
    }

    cbm_cypher_result_free(&result);
    static const char where_prefix[] = "MATCH (n:Function) WHERE n.";
    static const char where_separator[] = " = '";
    static const char where_suffix[] = "' RETURN n.name";
    const size_t where_query_size = (sizeof(where_prefix) - SKIP_ONE) + key_length +
                                    (sizeof(where_separator) - SKIP_ONE) + value_length +
                                    sizeof(where_suffix);
    char *where_query = malloc(where_query_size);
    ASSERT_NOT_NULL(where_query);
    offset = 0;
    memcpy(where_query + offset, where_prefix, sizeof(where_prefix) - SKIP_ONE);
    offset += sizeof(where_prefix) - SKIP_ONE;
    memset(where_query + offset, 'k', key_length);
    offset += key_length;
    memcpy(where_query + offset, where_separator, sizeof(where_separator) - SKIP_ONE);
    offset += sizeof(where_separator) - SKIP_ONE;
    memset(where_query + offset, 'V', value_length);
    offset += value_length;
    memcpy(where_query + offset, where_suffix, sizeof(where_suffix));

    ASSERT_EQ(cbm_cypher_execute(store, where_query, "test", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_STR_EQ(result.rows[0][0], "LongProperty");
    cbm_cypher_result_free(&result);

    static const char keys_prefix[] = "[\"name\",\"qualified_name\",\"label\",\"file_path\",\"";
    static const char keys_suffix[] = "\"]";
    ASSERT_EQ(cbm_cypher_execute(store, "MATCH (n:Function) RETURN keys(n)", "test", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, 1);
    ASSERT_EQ(strlen(result.rows[0][0]),
              sizeof(keys_prefix) - SKIP_ONE + key_length + sizeof(keys_suffix) - SKIP_ONE);
    ASSERT_TRUE(memcmp(result.rows[0][0], keys_prefix, sizeof(keys_prefix) - SKIP_ONE) == 0);
    for (size_t i = 0; i < key_length; i++) {
        ASSERT_EQ(result.rows[0][0][sizeof(keys_prefix) - SKIP_ONE + i], 'k');
    }
    ASSERT_STR_EQ(result.rows[0][0] + sizeof(keys_prefix) - SKIP_ONE + key_length, keys_suffix);
    cbm_cypher_result_free(&result);

    free(where_query);
    cbm_store_close(store);
    free(query);
    free(properties_json);
    PASS();
}

TEST(cypher_exec_dynamic_property_projection_exceeds_tls_ring_without_aliasing) {
    char names[CYPHER_TEST_DYNAMIC_PROPERTY_COUNT][CBM_SZ_16];
    char values[CYPHER_TEST_DYNAMIC_PROPERTY_COUNT][CBM_SZ_16];
    size_t json_length = PAIR_LEN; /* opening and closing braces */
    size_t query_length = sizeof("MATCH (n:Function) RETURN ") - SKIP_ONE;
    for (int i = 0; i < CYPHER_TEST_DYNAMIC_PROPERTY_COUNT; i++) {
        snprintf(names[i], sizeof(names[i]), "p%d", i);
        snprintf(values[i], sizeof(values[i]), "value_%d", i);
        json_length +=
            strlen(names[i]) + strlen(values[i]) + CYPHER_TEST_JSON_STRING_MEMBER_OVERHEAD;
        query_length += sizeof("n.") - SKIP_ONE + strlen(names[i]);
        if (i > 0) {
            json_length += SKIP_ONE;
            query_length += PAIR_LEN;
        }
    }
    char *properties_json = malloc(json_length + SKIP_ONE);
    char *query = malloc(query_length + SKIP_ONE);
    ASSERT_NOT_NULL(properties_json);
    ASSERT_NOT_NULL(query);

    size_t json_offset = 0;
    size_t query_offset = 0;
    properties_json[json_offset++] = '{';
    static const char query_prefix[] = "MATCH (n:Function) RETURN ";
    memcpy(query, query_prefix, sizeof(query_prefix) - SKIP_ONE);
    query_offset += sizeof(query_prefix) - SKIP_ONE;
    for (int i = 0; i < CYPHER_TEST_DYNAMIC_PROPERTY_COUNT; i++) {
        if (i > 0) {
            properties_json[json_offset++] = ',';
            query[query_offset++] = ',';
            query[query_offset++] = ' ';
        }
        int json_written = snprintf(properties_json + json_offset,
                                    json_length + SKIP_ONE - json_offset, "\"%s\":\"%s\"",
                                    names[i], values[i]);
        ASSERT_GT(json_written, 0);
        json_offset += (size_t)json_written;
        query[query_offset++] = 'n';
        query[query_offset++] = '.';
        size_t name_length = strlen(names[i]);
        memcpy(query + query_offset, names[i], name_length);
        query_offset += name_length;
    }
    properties_json[json_offset++] = '}';
    properties_json[json_offset] = '\0';
    query[query_offset] = '\0';
    ASSERT_EQ(json_offset, json_length);
    ASSERT_EQ(query_offset, query_length);

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "test", "/tmp/test"), CBM_STORE_OK);
    cbm_node_t node = {.project = "test",
                       .label = "Function",
                       .name = "WideProperties",
                       .qualified_name = "test.WideProperties",
                       .file_path = "wide.c",
                       .properties_json = properties_json};
    ASSERT_GT(cbm_store_upsert_node(store, &node), 0);

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store, query, "test", 0, &result), 0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 1);
    ASSERT_EQ(result.col_count, CYPHER_TEST_DYNAMIC_PROPERTY_COUNT);
    for (int i = 0; i < CYPHER_TEST_DYNAMIC_PROPERTY_COUNT; i++) {
        ASSERT_STR_EQ(result.rows[0][i], values[i]);
    }

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    free(query);
    free(properties_json);
    PASS();
}

enum { CYPHER_WIDE_PROJECTION_COLUMN_COUNT = 48 };

TEST(cypher_wide_return_projection_is_query_sized) {
    char query[4096];
    int off = snprintf(query, sizeof(query), "MATCH (f:Function) RETURN ");
    for (int i = 0; i < CYPHER_WIDE_PROJECTION_COLUMN_COUNT; i++) {
        off += snprintf(query + off, sizeof(query) - (size_t)off, "%sf.p%d", i ? ", " : "", i);
    }
    off += snprintf(query + off, sizeof(query) - (size_t)off, " ORDER BY ");
    for (int i = 0; i < CYPHER_WIDE_PROJECTION_COLUMN_COUNT; i++) {
        off += snprintf(query + off, sizeof(query) - (size_t)off, "%sf.p%d", i ? ", " : "", i);
    }
    ASSERT_TRUE(off > 0 && (size_t)off < sizeof(query));

    cbm_store_t *store = setup_cypher_store();
    ASSERT_NOT_NULL(store);
    cbm_cypher_result_t result = {0};
    int rc = cbm_cypher_execute(store, query, "test", 0, &result);

    ASSERT_EQ(rc, 0);
    ASSERT_EQ(result.col_count, CYPHER_WIDE_PROJECTION_COLUMN_COUNT);
    ASSERT_EQ(result.row_count, 4);
    ASSERT_STR_EQ(result.columns[0], "f.p0");
    ASSERT_STR_EQ(result.columns[CYPHER_WIDE_PROJECTION_COLUMN_COUNT - 1], "f.p47");
    ASSERT_STR_EQ(result.rows[0][CYPHER_WIDE_PROJECTION_COLUMN_COUNT - 1], "");
    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_wide_aggregate_projections_are_query_sized) {
    char return_query[4096];
    int return_offset = snprintf(return_query, sizeof(return_query), "MATCH (f:Function) RETURN ");
    for (int i = 0; i < CYPHER_WIDE_PROJECTION_COLUMN_COUNT - 1; i++) {
        return_offset +=
            snprintf(return_query + return_offset, sizeof(return_query) - (size_t)return_offset,
                     "%sf.p%d", i ? ", " : "", i);
    }
    return_offset += snprintf(return_query + return_offset,
                              sizeof(return_query) - (size_t)return_offset, ", count(f.name) AS c");
    ASSERT_TRUE(return_offset > 0 && (size_t)return_offset < sizeof(return_query));

    char with_query[8192];
    int with_offset = snprintf(with_query, sizeof(with_query), "MATCH (f:Function) WITH ");
    for (int i = 0; i < CYPHER_WIDE_PROJECTION_COLUMN_COUNT - 1; i++) {
        with_offset += snprintf(with_query + with_offset, sizeof(with_query) - (size_t)with_offset,
                                "%sf.p%d AS p%d", i ? ", " : "", i, i);
    }
    with_offset += snprintf(with_query + with_offset, sizeof(with_query) - (size_t)with_offset,
                            ", count(f.name) AS c RETURN ");
    for (int i = 0; i < CYPHER_WIDE_PROJECTION_COLUMN_COUNT - 1; i++) {
        with_offset += snprintf(with_query + with_offset, sizeof(with_query) - (size_t)with_offset,
                                "%sp%d", i ? ", " : "", i);
    }
    with_offset +=
        snprintf(with_query + with_offset, sizeof(with_query) - (size_t)with_offset, ", c");
    ASSERT_TRUE(with_offset > 0 && (size_t)with_offset < sizeof(with_query));

    cbm_store_t *store = setup_cypher_store();
    ASSERT_NOT_NULL(store);
    const char *queries[] = {return_query, with_query};
    for (size_t query_index = 0; query_index < sizeof(queries) / sizeof(queries[0]);
         query_index++) {
        cbm_cypher_result_t result = {0};
        int rc = cbm_cypher_execute(store, queries[query_index], "test", 0, &result);
        ASSERT_EQ(rc, 0);
        ASSERT_EQ(result.col_count, CYPHER_WIDE_PROJECTION_COLUMN_COUNT);
        ASSERT_EQ(result.row_count, 1);
        ASSERT_STR_EQ(result.columns[CYPHER_WIDE_PROJECTION_COLUMN_COUNT - 1], "c");
        ASSERT_STR_EQ(result.rows[0][CYPHER_WIDE_PROJECTION_COLUMN_COUNT - 1], "4");
        cbm_cypher_result_free(&result);
    }
    cbm_store_close(store);
    PASS();
}

/* #601: an unbounded whole-graph OPTIONAL MATCH can run for minutes before the
 * 100k result ceiling sees a row. Group lookup had the same failure mode before
 * it became hash-indexed. With the execution deadline armed to trip immediately
 * (budget 0), expansive work must abort instead of returning a partial result.
 *
 * RED on unfixed code: no deadline exists, so the query completes and returns
 * rc==0 with rows and no error — the assertions below fail. */
TEST(cypher_exec_deadline_aborts_runaway_query_issue601) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    cbm_cypher_test_set_deadline_ms(0); /* trip on the first hot-loop check */
    int rc = cbm_cypher_execute(
        s, "MATCH (a) OPTIONAL MATCH (a)-[:CALLS]->(b) RETURN a.qualified_name, count(b)", "test",
        0, &r);
    cbm_cypher_test_set_deadline_ms(-1); /* restore default before asserting (thread-local) */

    ASSERT_TRUE(rc != 0); /* CBM_NOT_FOUND (-1) — query aborted, not success */
    ASSERT_NOT_NULL(r.error);
    ASSERT_TRUE(strstr(r.error, "time limit") != NULL);
    ASSERT_TRUE(strstr(r.error, "relationship types and directions") != NULL);
    ASSERT_TRUE(strstr(r.error, "LIMIT cannot reduce match work") != NULL);
    ASSERT_EQ(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #601 companion: the default (ample) budget must NOT false-positive on a
 * normal small query — it still returns its rows. */
TEST(cypher_exec_deadline_allows_normal_query_issue601) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(
        s, "MATCH (a) OPTIONAL MATCH (a)-[:CALLS]->(b) RETURN a.qualified_name, count(b)", "test",
        0, &r);

    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(r.error == NULL);
    ASSERT_GT(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

static cbm_store_t *setup_cypher_aggregate_group_store(int group_count) {
    enum { NAME_SIZE = 32, QN_SIZE = 64 };
    cbm_store_t *s = cbm_store_open_memory();
    if (!s || cbm_store_upsert_project(s, "test", "/tmp/test") != CBM_STORE_OK) {
        cbm_store_close(s);
        return NULL;
    }

    /* More than the 256-entry initial aggregate capacity exercises array
     * relocation. Every row has a distinct key, the quadratic worst case. */
    for (int i = 0; i < group_count; i++) {
        char name[NAME_SIZE];
        char qn[QN_SIZE];
        snprintf(name, sizeof(name), "group_%04d", i);
        snprintf(qn, sizeof(qn), "test.%s", name);
        cbm_node_t node = {.project = "test",
                           .label = "Function",
                           .name = name,
                           .qualified_name = qn,
                           .file_path = "groups.c"};
        if (cbm_store_upsert_node(s, &node) <= 0) {
            cbm_store_close(s);
            return NULL;
        }
    }
    return s;
}

TEST(cypher_aggregate_group_lookup_is_linear_across_growth) {
    cbm_store_t *s = setup_cypher_aggregate_group_store(CYPHER_AGGREGATE_GROWTH_GROUP_COUNT);
    ASSERT_NOT_NULL(s);

    const char *queries[] = {
        "MATCH (n:Function) RETURN n.qualified_name AS q, count(*) AS c ORDER BY q",
        "MATCH (n:Function) WITH n.qualified_name AS q, count(*) AS c RETURN q, c ORDER BY q",
    };
    for (size_t qi = 0; qi < sizeof(queries) / sizeof(queries[0]); qi++) {
        cbm_cypher_result_t r = {0};
        cbm_cypher_test_reset_group_lookup_probes();
        int rc =
            cbm_cypher_execute(s, queries[qi], "test", CYPHER_AGGREGATE_GROWTH_GROUP_COUNT + 1, &r);
        uint64_t probes = cbm_cypher_test_group_lookup_probes();

        ASSERT_EQ(rc, 0);
        ASSERT_EQ(r.row_count, CYPHER_AGGREGATE_GROWTH_GROUP_COUNT);
        ASSERT_STR_EQ(r.rows[0][0], "test.group_0000");
        ASSERT_STR_EQ(r.rows[CYPHER_AGGREGATE_GROWTH_GROUP_COUNT - 1][0], "test.group_0599");
        ASSERT_STR_EQ(r.rows[0][1], "1");
        ASSERT_LTE(probes, (uint64_t)CYPHER_AGGREGATE_GROWTH_GROUP_COUNT * 2u);
        cbm_cypher_result_free(&r);
    }

    cbm_store_close(s);
    PASS();
}

TEST(cypher_aggregate_distinct_lookup_is_expected_linear) {
    cbm_store_t *s = setup_cypher_aggregate_group_store(CYPHER_AGGREGATE_GROWTH_GROUP_COUNT);
    ASSERT_NOT_NULL(s);

    const char *queries[] = {
        "MATCH (n:Function) RETURN COUNT(DISTINCT n.name)",
        "MATCH (n:Function) WITH COUNT(DISTINCT n.name) AS n RETURN n",
        "MATCH (n:Function) RETURN COLLECT(DISTINCT n.name)",
    };
    for (size_t query_index = 0; query_index < sizeof(queries) / sizeof(queries[0]);
         query_index++) {
        cbm_cypher_result_t result = {0};
        cbm_cypher_test_reset_aggregate_distinct_probes();
        int rc = cbm_cypher_execute(s, queries[query_index], "test", 1, &result);
        uint64_t probes = cbm_cypher_test_aggregate_distinct_probes();

        ASSERT_EQ(rc, 0);
        ASSERT_EQ(result.row_count, 1);
        /* A hash-backed set performs one expected-constant membership probe per
         * input. The factor permits a checked insert probe without hiding the
         * former N*(N-1)/2 scan. */
        ASSERT_LTE(probes, (uint64_t)CYPHER_AGGREGATE_GROWTH_GROUP_COUNT * PAIR_LEN);
        cbm_cypher_result_free(&result);
    }

    cbm_store_close(s);
    PASS();
}

TEST(cypher_row_distinct_lookup_is_expected_linear) {
    cbm_store_t *store = setup_cypher_aggregate_group_store(CYPHER_AGGREGATE_GROWTH_GROUP_COUNT);
    ASSERT_NOT_NULL(store);

    cbm_cypher_result_t result = {0};
    cbm_cypher_test_reset_row_distinct_probes();
    int rc = cbm_cypher_execute(store, "MATCH (n:Function) RETURN DISTINCT n.name", "test",
                                CYPHER_AGGREGATE_GROWTH_GROUP_COUNT + SKIP_ONE, &result);
    uint64_t probes = cbm_cypher_test_row_distinct_probes();

    ASSERT_EQ(rc, 0);
    ASSERT_EQ(result.row_count, CYPHER_AGGREGATE_GROWTH_GROUP_COUNT);
    ASSERT_LTE(probes, (uint64_t)CYPHER_AGGREGATE_GROWTH_GROUP_COUNT * PAIR_LEN);
    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_row_distinct_length_prefix_prevents_column_boundary_collision) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "distinct", "/tmp/distinct"), CBM_STORE_OK);
    cbm_node_t nodes[] = {
        {.project = "distinct",
         .label = "Function",
         .name = "First",
         .qualified_name = "distinct.First",
         .file_path = "distinct.c",
         .properties_json = "{\"left\":\"a\",\"right\":\"bc\"}"},
        {.project = "distinct",
         .label = "Function",
         .name = "Second",
         .qualified_name = "distinct.Second",
         .file_path = "distinct.c",
         .properties_json = "{\"left\":\"ab\",\"right\":\"c\"}"},
    };
    for (size_t i = 0; i < sizeof(nodes) / sizeof(nodes[0]); i++) {
        ASSERT_TRUE(cbm_store_upsert_node(store, &nodes[i]) > 0);
    }

    cbm_cypher_result_t result = {0};
    ASSERT_EQ(cbm_cypher_execute(store,
                                 "MATCH (n:Function) RETURN DISTINCT n.left, n.right "
                                 "ORDER BY n.left",
                                 "distinct", 0, &result),
              0);
    ASSERT_NULL(result.error);
    ASSERT_EQ(result.row_count, 2);
    ASSERT_STR_EQ(result.rows[0][0], "a");
    ASSERT_STR_EQ(result.rows[0][1], "bc");
    ASSERT_STR_EQ(result.rows[1][0], "ab");
    ASSERT_STR_EQ(result.rows[1][1], "c");

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    PASS();
}

TEST(cypher_row_distinct_key_copy_failure_is_atomic) {
    cbm_store_t *store = setup_cypher_store();
    ASSERT_NOT_NULL(store);

    for (int successful_before = 0; successful_before <= SKIP_ONE; successful_before++) {
        cbm_cypher_result_t result = {0};
        cbm_cypher_test_fail_row_distinct_key_copy(successful_before);
        int rc = cbm_cypher_execute(store, "MATCH (n:Function) RETURN DISTINCT n.name", "test", 0,
                                    &result);
        cbm_cypher_test_fail_row_distinct_key_copy(-1);
        ASSERT_TRUE(rc != 0);
        ASSERT_NOT_NULL(result.error);
        ASSERT_NOT_NULL(strstr(result.error, "allocate memory"));
        ASSERT_EQ(result.row_count, 0);
        cbm_cypher_result_free(&result);
    }

    cbm_store_close(store);
    PASS();
}

static int assert_cypher_agg_allocation_failure(cbm_store_t *store, const char *query,
                                                cbm_cypher_test_agg_alloc_site_t site,
                                                int successful_before) {
    cbm_cypher_result_t result = {0};
    cbm_cypher_test_fail_aggregation_allocation(site, successful_before);
    int rc = cbm_cypher_execute(store, query, "test", 0, &result);
    cbm_cypher_test_fail_aggregation_allocation(CBM_CYPHER_TEST_AGG_ALLOC_NONE, -1);
    ASSERT_TRUE(rc != 0);
    ASSERT_NOT_NULL(result.error);
    ASSERT_NOT_NULL(strstr(result.error, "allocate memory"));
    ASSERT_EQ(result.row_count, 0);
    cbm_cypher_result_free(&result);
    return 0;
}

TEST(cypher_aggregation_allocation_failures_are_atomic) {
    cbm_store_t *small = setup_cypher_store();
    ASSERT_NOT_NULL(small);
    const char *group_query =
        "MATCH (n:Function) RETURN n.qualified_name AS q, count(*) AS c ORDER BY q";

    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, group_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_INITIAL, 0),
              0);
    /* One group owns its key, eight parallel arrays, and two projected-value
     * copies. Fail each construction point to prove partial cleanup. */
    for (int successful = 0; successful < 11; successful++) {
        ASSERT_EQ(assert_cypher_agg_allocation_failure(
                      small, group_query, CBM_CYPHER_TEST_AGG_ALLOC_GROUP_ENTRY, successful),
                  0);
    }

    const char *with_group_query =
        "MATCH (n:Function) WITH n.qualified_name AS q, count(*) AS c RETURN q, c ORDER BY q";
    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, with_group_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_INITIAL, 0),
              0);
    /* WITH owns its key, ten parallel arrays, and two projected-value copies. */
    for (int successful = 0; successful < 13; successful++) {
        ASSERT_EQ(assert_cypher_agg_allocation_failure(
                      small, with_group_query, CBM_CYPHER_TEST_AGG_ALLOC_GROUP_ENTRY, successful),
                  0);
    }

    const char *collect_query =
        "MATCH (f:Function)-[:CALLS]->(g:Function) RETURN f.name, COLLECT(g.name)";
    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, collect_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_VALUE_ARRAY_GROWTH, 0),
              0);
    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, collect_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_VALUE_COPY, 0),
              0);
    const char *distinct_query =
        "MATCH (f:Function)-[:CALLS]->(g:Function) RETURN COUNT(DISTINCT g.name)";
    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, distinct_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_VALUE_ARRAY_GROWTH, 0),
              0);
    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, distinct_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_VALUE_COPY, 0),
              0);
    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, distinct_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_DISTINCT_INDEX, 0),
              0);
    const char *with_distinct_query = "MATCH (f:Function)-[:CALLS]->(g:Function) "
                                      "WITH COUNT(DISTINCT g.name) AS n RETURN n";
    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, with_distinct_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_VALUE_ARRAY_GROWTH, 0),
              0);
    ASSERT_EQ(assert_cypher_agg_allocation_failure(small, with_distinct_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_VALUE_COPY, 0),
              0);
    ASSERT_EQ(assert_cypher_agg_allocation_failure(
                  small, with_distinct_query, CBM_CYPHER_TEST_AGG_ALLOC_DISTINCT_INDEX, 0),
              0);
    cbm_store_close(small);

    cbm_store_t *many = setup_cypher_aggregate_group_store(CYPHER_AGGREGATE_GROWTH_GROUP_COUNT);
    ASSERT_NOT_NULL(many);
    ASSERT_EQ(assert_cypher_agg_allocation_failure(many, group_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_GROUP_ARRAY_GROWTH, 0),
              0);
    ASSERT_EQ(assert_cypher_agg_allocation_failure(many, with_group_query,
                                                   CBM_CYPHER_TEST_AGG_ALLOC_GROUP_ARRAY_GROWTH, 0),
              0);
    cbm_store_close(many);
    PASS();
}

TEST(cypher_aggregate_value_lists_grow_geometrically) {
    cbm_store_t *store = setup_cypher_aggregate_group_store(CYPHER_AGGREGATE_GROWTH_GROUP_COUNT);
    ASSERT_NOT_NULL(store);

    const char *queries[] = {
        "MATCH (n:Function) RETURN COLLECT(n.name)",
        "MATCH (n:Function) RETURN COUNT(DISTINCT n.name)",
        "MATCH (n:Function) WITH COUNT(DISTINCT n.name) AS n RETURN n",
    };
    for (size_t query_index = 0; query_index < sizeof(queries) / sizeof(queries[0]);
         query_index++) {
        cbm_cypher_result_t result = {0};
        cbm_cypher_test_reset_aggregate_list_growths();
        int rc = cbm_cypher_execute(store, queries[query_index], "test", 1, &result);
        uint64_t growths = cbm_cypher_test_aggregate_list_growths();

        ASSERT_EQ(rc, 0);
        ASSERT_EQ(result.row_count, 1);
        /* A doubling vector grows at most once per size_t bit. This portable
         * logarithmic bound rejects the former one-realloc-per-value path. */
        ASSERT_LTE(growths, (uint64_t)(sizeof(size_t) * CHAR_BIT));
        cbm_cypher_result_free(&result);
    }

    cbm_store_close(store);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════ */

SUITE(cypher) {
    RUN_TEST(cypher_capability_registry_functions_are_executable);
    /* Lexer */
    RUN_TEST(cypher_lex_simple_match);
    RUN_TEST(cypher_lex_relationship);
    RUN_TEST(cypher_lex_string_literal);
    RUN_TEST(cypher_lex_single_quote_string);
    RUN_TEST(cypher_lex_preserves_long_string);
    RUN_TEST(cypher_lex_preserves_long_identifier);
    RUN_TEST(cypher_lex_rejects_unterminated_string);
    RUN_TEST(cypher_lex_rejects_unterminated_block_comment);
    RUN_TEST(cypher_lex_rejects_unknown_character);
    RUN_TEST(cypher_lex_allocation_failures_are_atomic);
    RUN_TEST(cypher_parse_propagates_lex_error);
    RUN_TEST(cypher_lex_number);
    RUN_TEST(cypher_lex_operators);
    RUN_TEST(cypher_lex_keywords_case_insensitive);
    RUN_TEST(cypher_lex_pipe_and_star);
    RUN_TEST(cypher_lex_full_query);
    /* Parser */
    RUN_TEST(cypher_parse_simple_node);
    RUN_TEST(cypher_parse_relationship_outbound);
    RUN_TEST(cypher_parse_relationship_inbound);
    RUN_TEST(cypher_parse_relationship_any);
    RUN_TEST(cypher_parse_variable_length);
    RUN_TEST(cypher_parse_variable_length_unbounded);
    RUN_TEST(cypher_parse_rejects_unsupported_variable_length_relationship_variable);
    RUN_TEST(cypher_parse_variable_length_single_bound_and_zero_range);
    RUN_TEST(cypher_parse_hop_range_boundaries);
    RUN_TEST(cypher_parse_multiple_edge_types);
    RUN_TEST(cypher_parse_where_clause);
    RUN_TEST(cypher_parse_where_regex);
    RUN_TEST(cypher_parse_where_and);
    RUN_TEST(cypher_parse_return_simple);
    RUN_TEST(cypher_parse_return_count);
    RUN_TEST(cypher_parse_return_order_limit);
    RUN_TEST(cypher_parse_preserves_query_sized_label);
    RUN_TEST(cypher_parse_preserves_query_sized_case_value_reference);
    RUN_TEST(cypher_parse_multikey_order_by_issue1334);
    RUN_TEST(cypher_parse_order_by_beyond_legacy_cap_issue1334);
    RUN_TEST(cypher_parse_return_distinct);
    RUN_TEST(cypher_parse_inline_props);
    RUN_TEST(cypher_parse_error);
    /* Execution */
    RUN_TEST(cypher_exec_preserves_query_sized_order_expression_and_column);
    RUN_TEST(cypher_exec_preserves_query_sized_star_and_default_columns);
    RUN_TEST(cypher_exec_deadline_aborts_runaway_query_issue601);
    RUN_TEST(cypher_exec_deadline_allows_normal_query_issue601);
    RUN_TEST(cypher_aggregate_group_lookup_is_linear_across_growth);
    RUN_TEST(cypher_aggregate_distinct_lookup_is_expected_linear);
    RUN_TEST(cypher_row_distinct_lookup_is_expected_linear);
    RUN_TEST(cypher_row_distinct_length_prefix_prevents_column_boundary_collision);
    RUN_TEST(cypher_row_distinct_key_copy_failure_is_atomic);
    RUN_TEST(cypher_aggregation_allocation_failures_are_atomic);
    RUN_TEST(cypher_aggregate_value_lists_grow_geometrically);
    RUN_TEST(cypher_exec_file_contains_pushes_down_beyond_seed_window);
    RUN_TEST(cypher_exec_output_cap_does_not_limit_predicate_scan);
    RUN_TEST(cypher_deep_nesting_rejected_not_crash);
    RUN_TEST(cypher_exec_match_all_functions);
    RUN_TEST(cypher_exec_optional_empty_label_no_overflow);
    RUN_TEST(cypher_cross_join_alloc_rejects_overflow);
    RUN_TEST(cypher_exec_optional_rel_output_limit_does_not_bound_matching);
    RUN_TEST(cypher_exec_optional_saturated_does_not_fabricate_no_match);
    RUN_TEST(cypher_exec_optional_rel_leaf_fallback_survives);
    RUN_TEST(cypher_issue240_labels_function);
    RUN_TEST(cypher_rejects_list_index_after_function_result);
    RUN_TEST(cypher_rejects_unconsumed_trailing_tokens);
    RUN_TEST(cypher_issue237_distinct_order_limit);
    RUN_TEST(cypher_issue873_distinct_order_limit_dedupes_before_limit);
    RUN_TEST(cypher_issue873_distinct_limit_dedupes_before_limit);
    RUN_TEST(cypher_issue873_distinct_order_skip_limit_dedupes_before_skip);
    RUN_TEST(cypher_issue252_tointeger);
    RUN_TEST(cypher_issue305_count_star_alias);
    RUN_TEST(cypher_exec_where_eq);
    RUN_TEST(cypher_exec_varlength_path_semantics_issue797);
    RUN_TEST(cypher_exec_untyped_variable_length_matches_all_relationship_types);
    RUN_TEST(cypher_exec_relationship_uniqueness_spans_entire_pattern);
    RUN_TEST(cypher_exec_preserves_parallel_relationship_identity);
    RUN_TEST(cypher_exec_undirected_self_loop_is_one_relationship_match);
    RUN_TEST(cypher_exec_reversed_hop_interval_is_empty_not_error);
    RUN_TEST(cypher_exec_indexed_and_whole_pattern_providers_are_result_equivalent);
    RUN_TEST(cypher_exec_where_coalesce_issue874);
    RUN_TEST(cypher_exec_where_regex);
    RUN_TEST(cypher_exec_where_contains);
    RUN_TEST(cypher_exec_where_starts_with);
    RUN_TEST(cypher_exec_return_properties);
    RUN_TEST(cypher_func_labels);
    RUN_TEST(cypher_func_labels_preserves_query_sized_label);
    RUN_TEST(cypher_func_labels_json_escapes_bytes);
    RUN_TEST(cypher_func_type);
    RUN_TEST(cypher_exec_binds_every_node_and_edge_variable_beyond_inline_capacity);
    RUN_TEST(cypher_exec_default_projection_includes_every_variable_beyond_inline_capacity);
    RUN_TEST(cypher_exec_with_projects_every_variable_beyond_inline_capacity);
    RUN_TEST(cypher_func_id);
    RUN_TEST(cypher_active_overlay_id_query_uses_canonical_identity);
    RUN_TEST(cypher_active_overlay_whole_pattern_preserves_edge_identity);
    RUN_TEST(cypher_func_keys);
    RUN_TEST(cypher_func_keys_dynamic_null_escape_and_dedup);
    RUN_TEST(cypher_func_properties);
    RUN_TEST(cypher_func_tointeger_tofloat);
    RUN_TEST(cypher_func_casts_preserve_logical_type_and_reject_invalid_numbers);
    RUN_TEST(cypher_func_casts_require_exactly_one_argument);
    RUN_TEST(cypher_func_size_reverse);
    RUN_TEST(cypher_func_trim_variants_and_odd_reverse);
    RUN_TEST(cypher_func_multiarg);
    RUN_TEST(cypher_func_multiarg_exact_edge_cases);
    RUN_TEST(cypher_issue874_where_coalesce_numeric);
    RUN_TEST(cypher_issue874_where_coalesce_string);
    RUN_TEST(cypher_issue874_where_coalesce_not_and);
    RUN_TEST(cypher_issue874_where_substring);
    RUN_TEST(cypher_issue874_where_unsupported_func_error);
    RUN_TEST(cypher_multi_prop_projection_no_alias);
    RUN_TEST(cypher_exists_no_callers);
    RUN_TEST(cypher_exists_has_outgoing_calls);
    RUN_TEST(cypher_exec_calls_relationship);
    RUN_TEST(cypher_exec_calls_with_where);
    RUN_TEST(cypher_exec_inbound);
    RUN_TEST(cypher_exec_count);
    RUN_TEST(cypher_exec_limit);
    RUN_TEST(cypher_exec_order_by);
    RUN_TEST(cypher_exec_variable_length);
    RUN_TEST(cypher_exec_variable_length_any_direction);
    RUN_TEST(cypher_exec_var_length_bounds_preserve_reachability);
    RUN_TEST(cypher_exec_var_length_zero_hops_returns_start_only);
    RUN_TEST(cypher_exec_var_length_preserves_all_requested_edge_types);
    RUN_TEST(cypher_exec_defines_edge);
    RUN_TEST(cypher_exec_no_results);
    RUN_TEST(cypher_exec_where_numeric);
    /* Go test ports */
    RUN_TEST(cypher_exec_distinct);
    RUN_TEST(cypher_exec_with_distinct_issue238);
    RUN_TEST(cypher_exec_with_distinct_preserves_values_differing_after_inline_prefix);
    RUN_TEST(cypher_exec_where_label_test_issue241);
    RUN_TEST(cypher_exec_label_alternation_issue242);
    RUN_TEST(cypher_label_alternation_growth_failure_is_atomic);
    RUN_TEST(cypher_exec_count_distinct_issue239);
    RUN_TEST(cypher_exec_aggregate_distinct_preserves_entity_identity_and_collect_semantics);
    RUN_TEST(cypher_exec_collect_json_escapes_core_string_values);
    RUN_TEST(cypher_exec_unsupported_func_errors_issue373);
    RUN_TEST(cypher_exec_unknown_func_return_errors);
    RUN_TEST(cypher_exec_inline_props);
    RUN_TEST(cypher_parse_where_starts_with);
    RUN_TEST(cypher_parse_where_contains);
    RUN_TEST(cypher_parse_where_numeric);
    /* Edge property tests (ported from cypher_test.go Feature 2) */
    RUN_TEST(cypher_edge_prop_access);
    RUN_TEST(cypher_edge_prop_storage_is_per_thread);
    RUN_TEST(cypher_edge_prop_in_where);
    RUN_TEST(cypher_edge_type_prop);
    RUN_TEST(cypher_edge_filter_contains);
    RUN_TEST(cypher_edge_filter_numeric_gte);
    RUN_TEST(cypher_bare_edge_return_exposes_properties_json);
    RUN_TEST(cypher_edge_return_without_filter);
    RUN_TEST(cypher_edge_filter_equals);
    RUN_TEST(cypher_edge_filter_starts_with);
    RUN_TEST(cypher_edge_combined_node_and_edge_filter);
    RUN_TEST(cypher_edge_filter_no_match);
    RUN_TEST(cypher_edge_filter_numeric_lt);
    RUN_TEST(cypher_edge_filter_regex);
    RUN_TEST(cypher_edge_builtin_type_filter);
    RUN_TEST(cypher_apply_limit);
    /* Phase 1: Simple operators */
    RUN_TEST(cypher_lex_neq_operators);
    RUN_TEST(cypher_lex_ends_keyword);
    RUN_TEST(cypher_lex_in_is_null);
    RUN_TEST(cypher_exec_where_neq);
    RUN_TEST(cypher_exec_where_neq_bang);
    RUN_TEST(cypher_exec_where_ends_with);
    RUN_TEST(cypher_exec_where_not);
    RUN_TEST(cypher_exec_where_not_on_relationship_target);
    RUN_TEST(cypher_exec_where_mixed_alias_and);
    RUN_TEST(cypher_exec_where_mixed_alias_or);
    RUN_TEST(cypher_exec_where_mixed_alias_xor);
    RUN_TEST(cypher_exec_where_in);
    RUN_TEST(cypher_exec_where_not_in);
    RUN_TEST(cypher_exec_where_is_null);
    RUN_TEST(cypher_exec_where_is_not_null);
    RUN_TEST(cypher_exec_null_predicates_and_coalesce_preserve_empty_strings);
    RUN_TEST(cypher_exec_return_star);
    RUN_TEST(cypher_parse_neq);
    RUN_TEST(cypher_parse_in);
    RUN_TEST(cypher_parse_is_null);
    /* Phase 2: Expression tree */
    RUN_TEST(cypher_exec_where_or);
    RUN_TEST(cypher_exec_where_complex_bool);
    RUN_TEST(cypher_exec_where_xor);
    RUN_TEST(cypher_exec_where_not_prefix);
    RUN_TEST(cypher_parse_expr_tree_and_or);
    RUN_TEST(cypher_parse_expr_tree_nested);
    /* Phase 3: Unsupported keyword errors */
    RUN_TEST(cypher_error_create);
    RUN_TEST(cypher_error_delete);
    RUN_TEST(cypher_error_set);
    RUN_TEST(cypher_error_merge);
    RUN_TEST(cypher_error_call);
    /* Phase 4: SKIP + aggregation */
    RUN_TEST(cypher_exec_skip);
    RUN_TEST(cypher_exec_skip_limit);
    RUN_TEST(cypher_exec_multikey_order_by_keeps_limit_issue1334);
    RUN_TEST(cypher_exec_multikey_order_by_tiebreak_issue1334);
    RUN_TEST(cypher_exec_with_multikey_order_by_keeps_limit_issue1334);
    RUN_TEST(cypher_exec_sum);
    RUN_TEST(cypher_exec_avg);
    RUN_TEST(cypher_exec_min);
    RUN_TEST(cypher_exec_max);
    RUN_TEST(cypher_exec_collect);
    RUN_TEST(cypher_exec_count_star);
    RUN_TEST(cypher_issue1111_return_type_count_group);
    RUN_TEST(cypher_parse_skip);
    RUN_TEST(cypher_parse_sum_avg);
    RUN_TEST(cypher_parse_collect);
    /* Phase 5: String functions + CASE */
    RUN_TEST(cypher_exec_tolower);
    RUN_TEST(cypher_exec_toupper);
    RUN_TEST(cypher_exec_tostring);
    RUN_TEST(cypher_exec_case);
    RUN_TEST(cypher_parse_tolower);
    RUN_TEST(cypher_parse_case);
    /* Phase 6: WITH clause */
    RUN_TEST(cypher_exec_with_rename);
    RUN_TEST(cypher_exec_with_count);
    RUN_TEST(cypher_issue1111_with_type_count_group);
    RUN_TEST(cypher_issue1111_with_scalar_func_alias_no_node_leak);
    RUN_TEST(cypher_exec_with_node_groupvar_prop);
    RUN_TEST(cypher_exec_with_where);
    RUN_TEST(cypher_exec_with_orderby_limit);
    RUN_TEST(cypher_parse_with);
    RUN_TEST(cypher_parse_with_where);
    /* Phase 7: OPTIONAL MATCH + multiple MATCH */
    RUN_TEST(cypher_exec_optional_match_no_result);
    RUN_TEST(cypher_exec_optional_match_null_aggregates);
    RUN_TEST(cypher_exec_optional_match_null_count_survives_with);
    RUN_TEST(cypher_exec_aggregates_distinguish_null_from_empty_string);
    RUN_TEST(cypher_exec_optional_match_null_numeric_aggregates);
    RUN_TEST(cypher_exec_grouping_uses_node_identity_not_display_name);
    RUN_TEST(cypher_exec_optional_match_has_result);
    RUN_TEST(cypher_exec_optional_match_bound_terminal_no_callers);
    RUN_TEST(cypher_exec_optional_where_after_with_null_extends_failed_candidates);
    RUN_TEST(cypher_exec_node_only_optional_where_null_extends_failed_candidates);
    RUN_TEST(cypher_exec_union_after_with_stage_executes_both_branches);
    RUN_TEST(cypher_exec_multi_match);
    RUN_TEST(cypher_exec_relationship_cross_join_grows_past_fanout_heuristic);
    RUN_TEST(cypher_parse_optional_match);
    RUN_TEST(cypher_exec_optional_match_after_with_uses_projected_rows);
    RUN_TEST(cypher_exec_simple_with_carries_node_identity);
    RUN_TEST(cypher_exec_multiple_with_match_stages);
    RUN_TEST(cypher_exec_multi_key_order_by_mixed_directions);
    RUN_TEST(cypher_order_by_rejects_unprojected_key_with_rewrite);
    RUN_TEST(cypher_with_order_by_allows_carried_node_property);
    RUN_TEST(cypher_exec_multi_key_order_by_nulls_and_limit);
    RUN_TEST(cypher_parse_multi_match);
    /* Phase 8: UNION */
    RUN_TEST(cypher_exec_union);
    RUN_TEST(cypher_exec_union_all);
    RUN_TEST(cypher_union_requires_identical_column_schema);
    RUN_TEST(cypher_exec_union_all_respects_caller_output_cap);
    RUN_TEST(cypher_exec_union_deduplicates_complete_branches_before_output_cap);
    RUN_TEST(cypher_exec_limits_separate_output_cap_from_working_budget);
    RUN_TEST(cypher_exec_working_budget_replaces_silent_bfs_prefix_cap);
    RUN_TEST(cypher_exec_working_budget_bounds_initial_scan_without_prefix_answer);
    RUN_TEST(cypher_parse_union);
    /* Phase 9: UNWIND */
    RUN_TEST(cypher_parse_unwind);
    RUN_TEST(cypher_parse_unwind_var);
    RUN_TEST(cypher_exec_unwind_literal_multiplies_rows_and_binds_alias);
    RUN_TEST(cypher_exec_unwind_empty_list_returns_no_rows);
    RUN_TEST(cypher_exec_unwind_variable_without_parameter_scope_fails_loudly);
    RUN_TEST(cypher_exec_unwind_cross_product_obeys_working_row_budget);
    RUN_TEST(cypher_parse_unwind_oversized_literal_no_overflow);
    RUN_TEST(cypher_parse_unwind_many_elements_no_overflow);
    RUN_TEST(cypher_wide_return_projection_is_query_sized);
    RUN_TEST(cypher_wide_aggregate_projections_are_query_sized);
    /* Composite property projection (arrays/objects, escaped quotes) */
    RUN_TEST(cypher_exec_prop_array_with_internal_commas);
    RUN_TEST(cypher_exec_prop_string_with_escaped_quote);
    RUN_TEST(cypher_exec_preserves_large_dynamic_property_in_aggregation);
    RUN_TEST(cypher_exec_preserves_large_dynamic_property_across_with);
    RUN_TEST(cypher_exec_preserves_large_dynamic_property_across_grouped_with);
    RUN_TEST(cypher_exec_preserves_query_sized_default_with_alias);
    RUN_TEST(cypher_exec_preserves_large_dynamic_property_in_scalar_function);
    RUN_TEST(cypher_exec_preserves_large_dynamic_property_in_multiarg_functions);
    RUN_TEST(cypher_exec_preserves_large_dynamic_property_in_collect_and_with);
    RUN_TEST(cypher_exec_preserves_query_sized_dynamic_property_key_and_value);
    RUN_TEST(cypher_exec_dynamic_property_projection_exceeds_tls_ring_without_aliasing);
}
