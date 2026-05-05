/*
 * test_cypher.c — Tests for the Cypher query engine.
 *
 * Ported from internal/cypher/cypher_test.go (1016 LOC).
 * Covers lexer, parser, and end-to-end execution.
 */
#include "test_framework.h"
#include <cypher/cypher.h>
#include <store/store.h>
#include <string.h>
#include <stdlib.h>

/* ══════════════════════════════════════════════════════════════════
 *  LEXER TESTS
 * ══════════════════════════════════════════════════════════════════ */

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

TEST(cypher_lex_string_overflow) {
    /* Build a string literal longer than 4096 bytes to verify we don't
     * overflow the stack buffer in lex_string_literal. */
    const int big = 5000;
    /* query: "AAAA...A"  (quotes included) */
    char *query = malloc(big + 3); /* quote + big chars + quote + NUL */
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
    /* The string should be truncated to CBM_SZ_4K - 1 (4095) characters. */
    ASSERT_EQ((int)strlen(r.tokens[0].text), 4095);

    cbm_lex_free(&r);
    free(query);
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
    ASSERT_EQ(cbm_query_pattern(q).rels[0].max_hops, 0); /* 0 = unbounded */

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
    ASSERT_NOT_NULL(q->ret->order_by);
    ASSERT_STR_EQ(q->ret->order_dir, "DESC");
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
                     .file_path = "log.go"};

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
    cbm_cypher_result_free(&r);

    /* No LIMIT, max_rows=10 → capped at 10 */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name", "lim", 10, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 10);
    cbm_cypher_result_free(&r);

    /* LIMIT above max_rows → explicit limit wins */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name LIMIT 30", "lim", 10, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 30);
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

/* ══════════════════════════════════════════════════════════════════ */

SUITE(cypher) {
    /* Lexer */
    RUN_TEST(cypher_lex_simple_match);
    RUN_TEST(cypher_lex_relationship);
    RUN_TEST(cypher_lex_string_literal);
    RUN_TEST(cypher_lex_single_quote_string);
    RUN_TEST(cypher_lex_string_overflow);
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
    RUN_TEST(cypher_parse_multiple_edge_types);
    RUN_TEST(cypher_parse_where_clause);
    RUN_TEST(cypher_parse_where_regex);
    RUN_TEST(cypher_parse_where_and);
    RUN_TEST(cypher_parse_return_simple);
    RUN_TEST(cypher_parse_return_count);
    RUN_TEST(cypher_parse_return_order_limit);
    RUN_TEST(cypher_parse_return_distinct);
    RUN_TEST(cypher_parse_inline_props);
    RUN_TEST(cypher_parse_error);
    /* Execution */
    RUN_TEST(cypher_exec_match_all_functions);
    RUN_TEST(cypher_exec_where_eq);
    RUN_TEST(cypher_exec_where_regex);
    RUN_TEST(cypher_exec_where_contains);
    RUN_TEST(cypher_exec_where_starts_with);
    RUN_TEST(cypher_exec_return_properties);
    RUN_TEST(cypher_exec_calls_relationship);
    RUN_TEST(cypher_exec_calls_with_where);
    RUN_TEST(cypher_exec_inbound);
    RUN_TEST(cypher_exec_count);
    RUN_TEST(cypher_exec_limit);
    RUN_TEST(cypher_exec_order_by);
    RUN_TEST(cypher_exec_variable_length);
    RUN_TEST(cypher_exec_defines_edge);
    RUN_TEST(cypher_exec_no_results);
    RUN_TEST(cypher_exec_where_numeric);
    /* Go test ports */
    RUN_TEST(cypher_exec_distinct);
    RUN_TEST(cypher_exec_inline_props);
    RUN_TEST(cypher_parse_where_starts_with);
    RUN_TEST(cypher_parse_where_contains);
    RUN_TEST(cypher_parse_where_numeric);
    /* Edge property tests (ported from cypher_test.go Feature 2) */
    RUN_TEST(cypher_edge_prop_access);
    RUN_TEST(cypher_edge_prop_in_where);
    RUN_TEST(cypher_edge_type_prop);
    RUN_TEST(cypher_edge_filter_contains);
    RUN_TEST(cypher_edge_filter_numeric_gte);
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
    RUN_TEST(cypher_exec_where_in);
    RUN_TEST(cypher_exec_where_not_in);
    RUN_TEST(cypher_exec_where_is_null);
    RUN_TEST(cypher_exec_where_is_not_null);
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
    RUN_TEST(cypher_exec_sum);
    RUN_TEST(cypher_exec_avg);
    RUN_TEST(cypher_exec_min);
    RUN_TEST(cypher_exec_max);
    RUN_TEST(cypher_exec_collect);
    RUN_TEST(cypher_exec_count_star);
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
    RUN_TEST(cypher_exec_with_where);
    RUN_TEST(cypher_exec_with_orderby_limit);
    RUN_TEST(cypher_parse_with);
    RUN_TEST(cypher_parse_with_where);
    /* Phase 7: OPTIONAL MATCH + multiple MATCH */
    RUN_TEST(cypher_exec_optional_match_no_result);
    RUN_TEST(cypher_exec_optional_match_has_result);
    RUN_TEST(cypher_exec_multi_match);
    RUN_TEST(cypher_parse_optional_match);
    RUN_TEST(cypher_parse_multi_match);
    /* Phase 8: UNION */
    RUN_TEST(cypher_exec_union);
    RUN_TEST(cypher_exec_union_all);
    RUN_TEST(cypher_parse_union);
    /* Phase 9: UNWIND */
    RUN_TEST(cypher_parse_unwind);
    RUN_TEST(cypher_parse_unwind_var);
}
