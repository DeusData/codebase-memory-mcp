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
#include <store/store.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifndef _WIN32
#include <sys/wait.h> /* fork/waitpid crash-isolation for the projection-width guard */
#include <unistd.h>
#endif

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

/* Trailing input must be an error, never a silent drop. The parser used to
 * stop at the first thing it did not understand and report success, so the
 * engine answered from the fragment it had parsed. */
TEST(cypher_parse_rejects_trailing_tokens) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) RETURN f.name AS n BANANA SPLIT 99", &q, &err);
    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(err);
    ASSERT_NULL(q);

    /* The message must name what actually stopped the parse, and must not
     * mention WITH: this query has no WITH in it anywhere. */
    ASSERT(strstr(err, "BANANA") != NULL);
    ASSERT(strstr(err, "WITH") == NULL);

    free(err);
    PASS();
}

/* Only one WITH is supported. A second one used to take the rest of the
 * query with it — the filter and the RETURN both vanished, and every row
 * came back unfiltered under the default projection. */
TEST(cypher_parse_rejects_second_with_clause) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) "
                              "OPTIONAL MATCH (a)-[:CALLS]->(f) "
                              "WITH f, count(a) AS calls "
                              "OPTIONAL MATCH (b)-[:USAGE]->(f) "
                              "WITH f, calls, count(b) AS usages "
                              "WHERE calls = 0 AND usages = 0 "
                              "RETURN f.name AS n",
                              &q, &err);
    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(err);
    ASSERT_NULL(q);

    /* Here the note earns its place. The parse stops at OPTIONAL, and the
     * reason is the second WITH further along, which the reader cannot see
     * from the stopping point alone. */
    ASSERT(strstr(err, "only one WITH clause is supported") != NULL);

    free(err);
    PASS();
}

/* The guard must not reject a query that is simply finished. One WITH, a
 * WHERE after it and a RETURN is the shape the grammar does support. */
TEST(cypher_parse_accepts_single_with_clause) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) "
                              "WITH f, f.name AS n "
                              "WHERE n = 'buildTree' "
                              "RETURN n",
                              &q, &err);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(err);
    ASSERT_NOT_NULL(q);

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
    ASSERT_EQ(q->ret->order_key_count, 1);
    ASSERT_STR_EQ(q->ret->order_keys[0], "f.name");
    ASSERT(q->ret->order_descs[0]);
    ASSERT_EQ(q->ret->limit, 5);

    cbm_query_free(q);
    PASS();
}

/* #1334: every ORDER BY key is parsed (per-key direction) and the LIMIT that
 * follows the key list is consumed instead of silently dropped. */
TEST(cypher_parse_multikey_order_by_issue1334) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse(
        "MATCH (f:Function) RETURN f.name ORDER BY f.complexity DESC, f.name ASC LIMIT 5", &q,
        &err);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(q->ret->order_key_count, 2);
    ASSERT_STR_EQ(q->ret->order_keys[0], "f.complexity");
    ASSERT(q->ret->order_descs[0]);
    ASSERT_STR_EQ(q->ret->order_keys[1], "f.name");
    ASSERT_FALSE(q->ret->order_descs[1]);
    ASSERT_EQ(q->ret->limit, 5);

    cbm_query_free(q);
    PASS();
}

/* #1334: more keys than the modeled maximum is a loud parse error - the old
 * failure mode (ignore the remainder, drop the LIMIT) must never come back. */
TEST(cypher_parse_order_by_over_cap_rejected_issue1334) {
    cbm_query_t *q = NULL;
    char *err = NULL;
    int rc = cbm_cypher_parse("MATCH (f:Function) RETURN f.name ORDER BY "
                              "f.a, f.b, f.c, f.d, f.e, f.f, f.g, f.h, f.i LIMIT 5",
                              &q, &err);
    ASSERT(rc != 0);
    free(err);
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
 * (The same function also used a plain-int bind_count*extra_count product,
 * which wraps to a tiny malloc on large graphs; the count is now computed and
 * bounds-checked in size_t by cbm_cypher_cross_join_alloc — exercised at its
 * arithmetic boundary by cypher_cross_join_alloc_rejects_overflow below.)
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

/* Regression: expand_pattern_rels sized its OPTIONAL-expansion output buffer as
 * bind_cap*10 + 1 — room for the bounded expansion (max_new = bind_cap*10) plus
 * a SINGLE OPTIONAL fallback row. When one source saturated the expansion to
 * max_new and two or more later sources took the OPTIONAL (no-match) path, the
 * second fallback write ran past the allocation (ASan: heap-buffer-overflow).
 *
 * The fix sizes the buffer for both writers losslessly (max_new + *bind_count),
 * so every OPTIONAL no-match row keeps its slot — no overflow AND no dropped
 * rows. Here the hub saturates the expansion to max_new while all 20 leaves take
 * the fallback; the buffer holds them all, and the max_rows LIMIT bounds only the
 * OUTPUT. Query text is agent-controlled via the MCP query tool. */
TEST(cypher_exec_optional_rel_saturated_no_overflow) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* 1 hub + 20 leaf Function nodes → bind_cap = 21, max_new = 210, so the hop
     * buffer holds max_new + *bind_count = 231 rows. The hub is inserted first so
     * it is expanded before the leaves; it saturates the expansion to max_new,
     * then each of the 20 leaves adds one OPTIONAL fallback row — under the old
     * alloc (211 slots) the 2nd such write overflowed; now all 230 fit. */
    cbm_node_t hub = {
        .project = "test", .label = "Function", .name = "hub", .qualified_name = "test.hub"};
    int64_t hub_id = cbm_store_upsert_node(s, &hub);
    for (int i = 0; i < 20; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "leaf%02d", i);
        snprintf(qn, sizeof(qn), "test.leaf%02d", i);
        cbm_node_t leaf = {
            .project = "test", .label = "Function", .name = nm, .qualified_name = qn};
        cbm_store_upsert_node(s, &leaf);
    }

    /* Give the hub 300 CALLS edges (> max_new = 210) so its expansion saturates
     * max_new; targets are non-Function so they don't inflate bind_cap. */
    for (int i = 0; i < 300; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "callee%d", i);
        snprintf(qn, sizeof(qn), "test.callee%d", i);
        cbm_node_t callee = {.project = "test", .label = "Var", .name = nm, .qualified_name = qn};
        int64_t cid = cbm_store_upsert_node(s, &callee);
        cbm_edge_t e = {.project = "test", .source_id = hub_id, .target_id = cid, .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }

    /* max_rows below the Function count (21) so bind_cap tracks scan_count (21)
     * rather than the 100000 result ceiling — the same regime a large repo
     * (> ceiling functions) or an agent-supplied small limit hits. */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (a:Function) OPTIONAL MATCH (a)-[:CALLS]->(b) RETURN a.name", "test", 5, &r);
    /* Bounded success, no overflow (ASan proves the buffer holds every row); the
     * LIMIT caps the output rows rather than the query crashing. */
    ASSERT_EQ(rc, 0);
    ASSERT_GT(r.row_count, 0);
    ASSERT_TRUE(r.row_count <= 5);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Reproduce-first: after the expansion budget is exhausted, OPTIONAL MATCH must
 * not FABRICATE a "no match" row for a source that genuinely has matches.
 *
 * process_edges / expand_var_length used to carry the budget in the LOOP
 * condition (`ei < edge_count && *new_count < max_new`), so once new_count hit
 * max_new they stopped iterating entirely and never incremented match_count —
 * even though neighbours existed. expand_pattern_rels' ungated fallback then saw
 * match_count == 0 and emitted an unbound row. `WHERE b IS NULL` reads that as
 * "nothing points here", so a dead-code query reported LIVE code as dead.
 *
 * Shape: A saturates the budget, B genuinely has no callees, C has 5. Only B may
 * appear with an unbound b. Asserting on C specifically is what discriminates —
 * the pre-fix code emits C with an empty b, and a `row_count` check would not
 * notice. Deterministic: insertion order fixes the scan order (rowid), and every
 * count is exact. */
TEST(cypher_exec_optional_saturated_does_not_fabricate_no_match) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* 3 Function nodes → scan_count = 3; max_rows = 3 → bind_cap = 3 and
     * max_new = 30. A alone exceeds that, so B and C are reached with the
     * budget already spent — the regime that produced the fabrication. */
    cbm_node_t a = {
        .project = "test", .label = "Function", .name = "A", .qualified_name = "test.A"};
    cbm_node_t b = {
        .project = "test", .label = "Function", .name = "B", .qualified_name = "test.B"};
    cbm_node_t c = {
        .project = "test", .label = "Function", .name = "C", .qualified_name = "test.C"};
    int64_t a_id = cbm_store_upsert_node(s, &a);
    (void)cbm_store_upsert_node(s, &b); /* B: no outgoing CALLS at all */
    int64_t c_id = cbm_store_upsert_node(s, &c);
    ASSERT_GT(a_id, 0);
    ASSERT_GT(c_id, 0);

    /* Callees are label Var so they do not inflate scan_count/bind_cap. */
    for (int i = 0; i < 40; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "acallee%d", i);
        snprintf(qn, sizeof(qn), "test.acallee%d", i);
        cbm_node_t t = {.project = "test", .label = "Var", .name = nm, .qualified_name = qn};
        int64_t tid = cbm_store_upsert_node(s, &t);
        cbm_edge_t e = {.project = "test", .source_id = a_id, .target_id = tid, .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }
    for (int i = 0; i < 5; i++) {
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
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) OPTIONAL MATCH (f)-[:CALLS]->(g) WHERE g IS NULL RETURN f.name",
        "test", 3, &r);
    ASSERT_EQ(rc, 0);

    /* Positive control: B genuinely has no callees, so the query must still find
     * it. Without this a "C is absent" assertion could pass on an empty result. */
    bool saw_b = false;
    bool saw_c = false;
    for (int i = 0; i < r.row_count; i++) {
        const char *name = r.rows[i][0];
        if (name && strcmp(name, "B") == 0) {
            saw_b = true;
        }
        if (name && strcmp(name, "C") == 0) {
            saw_c = true;
        }
    }
    ASSERT_TRUE(saw_b);
    /* The discriminator: C has 5 callees, so claiming it has none is a
     * fabrication. Pre-fix this is exactly what the saturated path emitted. */
    ASSERT_FALSE(saw_c);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Arithmetic-boundary companion to the zero-label overflow above: the node
 * cross-join sizes its buffer from bind_count * extra_count. As a plain int that
 * product wraps past INT_MAX to a negative/garbage malloc size (the large-graph
 * #627 failure mode). cbm_cypher_cross_join_alloc now computes it in size_t and
 * rejects a count that would not fit the int binding counter or overflow the
 * byte size. Tested directly so the boundary is exercised without allocating
 * billions of bindings. */
TEST(cypher_cross_join_alloc_rejects_overflow) {
    size_t n = 0;

    /* 46341 * 46341 = 2147488281 > INT_MAX (2147483647): pre-fix the int product
     * wrapped negative -> tiny malloc -> heap OOB. Now rejected. */
    ASSERT_TRUE(cbm_cypher_cross_join_alloc(46341, 46341, false, &n) != 0);

    /* A normal join still succeeds: bind_count * extra_count + 1 slots. */
    ASSERT_EQ(cbm_cypher_cross_join_alloc(4, 3, false, &n), 0);
    ASSERT_EQ(n, (size_t)13);

    /* OPTIONAL with no extra nodes reserves one fallback row per binding + 1. */
    ASSERT_EQ(cbm_cypher_cross_join_alloc(4, 0, true, &n), 0);
    ASSERT_EQ(n, (size_t)5);

    /* Non-OPTIONAL with no extra nodes: just the sentinel slot. */
    ASSERT_EQ(cbm_cypher_cross_join_alloc(4, 0, false, &n), 0);
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

    /* 1 hub (2 CALLS edges) + 3 leaves (no edges). max_rows 0 is defaulted to
     * CYPHER_RESULT_CEILING (100000) in cbm_cypher_execute before bind_cap is
     * computed, so bind_cap = max(scan_count, 100000) = 100000 and the buffer is
     * far larger than needed here — this exercises the fallback rows, not the
     * saturation edge. */
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

/* Sibling of the leaf-fallback test, but for the BOUND-TERMINAL expansion path
 * (expand_from_bound_terminal): the OPTIONAL start var is unbound and the
 * terminal is bound, e.g. `MATCH (f) OPTIONAL MATCH (c)-[:CALLS]->(f)`. That
 * function sized its hop buffer bind_count*10 + 1 and gated the OPTIONAL
 * fallback on `new_count < max_new`, so once one terminal's expansion saturated
 * the buffer, every LATER terminal's no-match row was silently dropped — the
 * rows `WHERE c IS NULL` is meant to surface. Not an overflow (the guard kept
 * the write in bounds) but real data loss. Lossless sizing (bind_count*10 +
 * bind_count, fallback ungated) preserves them. */
TEST(cypher_exec_bound_terminal_optional_fallback_survives) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* 2 Function nodes (hub + leaf) → bind_count = 2, max_new = 20. The hub has
     * 21 incoming CALLS edges (> max_new), so its expansion saturates the write
     * buffer; the leaf has none, so it must still yield its OPTIONAL no-match
     * row. Under the original "+ SKIP_ONE" sizing that row was dropped once the
     * buffer filled; the lossless sizing preserves it regardless of the order in
     * which the (unordered) label scan visits the two terminals. */
    cbm_node_t hub = {
        .project = "test", .label = "Function", .name = "hub", .qualified_name = "test.hub"};
    int64_t hub_id = cbm_store_upsert_node(s, &hub);
    cbm_node_t leaf = {
        .project = "test", .label = "Function", .name = "leaf", .qualified_name = "test.leaf"};
    cbm_store_upsert_node(s, &leaf);

    /* Callers are non-Function so they do not inflate bind_count. Each CALLS the
     * hub (source = caller, target = hub), so from the bound terminal `hub` the
     * expansion binds the unbound start `c` to each caller. */
    for (int i = 0; i < 21; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "caller%02d", i);
        snprintf(qn, sizeof(qn), "test.caller%02d", i);
        cbm_node_t caller = {.project = "test", .label = "Var", .name = nm, .qualified_name = qn};
        int64_t cid = cbm_store_upsert_node(s, &caller);
        cbm_edge_t e = {.project = "test", .source_id = cid, .target_id = hub_id, .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }

    /* max_rows 0 → the 100000 result ceiling, so the output LIMIT does not hide
     * the leaf row; we are testing the hop buffer, not the output cap. */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) OPTIONAL MATCH (c)-[:CALLS]->(f) RETURN f.name, c.name", "test", 0,
        &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.col_count, 2);

    /* The leaf terminal has no incoming CALLS edge, so it must yield one row with
     * the start var `c` unbound (""). Before the fix this row was dropped once the
     * hub saturated the buffer. Also confirm the hub still expanded to bound rows. */
    bool leaf_fallback = false;
    bool hub_expanded = false;
    for (int i = 0; i < r.row_count; i++) {
        const char *f = r.rows[i][0];
        const char *c = r.rows[i][1];
        if (strcmp(f, "leaf") == 0 && c[0] == '\0') {
            leaf_fallback = true;
        }
        if (strcmp(f, "hub") == 0 && c[0] != '\0') {
            hub_expanded = true;
        }
    }
    ASSERT_TRUE(leaf_fallback); /* the bound-terminal OPTIONAL no-match row survived */
    ASSERT_TRUE(hub_expanded);  /* the expansion still produced bound rows */

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Discriminating companion to the test above: preserving OPTIONAL no-match rows
 * under saturation must NOT be bought by fabricating them. If match detection is
 * gated on the same ceiling as the write, then once one terminal fills the
 * buffer, another terminal that genuinely HAS callers is never scanned, its
 * match_count stays 0, and the fallback invents an unbound "dead code" row for it
 * — reporting live code as dead, which is worse than dropping a row.
 *
 * The construction is deliberately order-independent: BOTH hubs have enough
 * callers to saturate the buffer on their own, so whichever the scan visits
 * second is guaranteed to be processed after saturation. Under the gated variant
 * that second hub is fabricated as dead; the assertion "no hub with callers is
 * dead" then fails no matter which order `find_nodes_by_label` returns (its query
 * has no ORDER BY, so the test must not depend on one). The fix makes it pass. */
TEST(cypher_exec_bound_terminal_saturation_no_false_deadcode) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    /* 3 Function terminals → bind_count = 3, max_new = 30. */
    cbm_node_t hubA = {
        .project = "test", .label = "Function", .name = "hubA", .qualified_name = "test.hubA"};
    int64_t hubA_id = cbm_store_upsert_node(s, &hubA);
    cbm_node_t hubB = {
        .project = "test", .label = "Function", .name = "hubB", .qualified_name = "test.hubB"};
    int64_t hubB_id = cbm_store_upsert_node(s, &hubB);
    cbm_node_t leaf = {
        .project = "test", .label = "Function", .name = "leaf", .qualified_name = "test.leaf"};
    cbm_store_upsert_node(s, &leaf);

    /* BOTH hubs get 35 callers (> max_new = 30), so either one saturates the write
     * buffer by itself; leaf gets none. Callers are non-Function so they don't
     * inflate bind_count. */
    for (int i = 0; i < 70; i++) {
        char nm[32];
        char qn[48];
        snprintf(nm, sizeof(nm), "caller%02d", i);
        snprintf(qn, sizeof(qn), "test.caller%02d", i);
        cbm_node_t caller = {.project = "test", .label = "Var", .name = nm, .qualified_name = qn};
        int64_t cid = cbm_store_upsert_node(s, &caller);
        int64_t tgt = i < 35 ? hubA_id : hubB_id; /* 35 -> hubA, 35 -> hubB */
        cbm_edge_t e = {.project = "test", .source_id = cid, .target_id = tgt, .type = "CALLS"};
        cbm_store_insert_edge(s, &e);
    }

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) OPTIONAL MATCH (c)-[:CALLS]->(f) RETURN f.name, c.name", "test", 0,
        &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.col_count, 2);

    bool hub_expanded = false;       /* sanity: the buffer really did fill from a hub */
    bool hub_false_deadcode = false; /* the bug: a hub with callers invented as dead */
    bool leaf_deadcode = false;      /* the lossless property: genuine dead code kept */
    for (int i = 0; i < r.row_count; i++) {
        const char *f = r.rows[i][0];
        const char *c = r.rows[i][1];
        bool is_hub = strcmp(f, "hubA") == 0 || strcmp(f, "hubB") == 0;
        if (is_hub && c[0] != '\0') {
            hub_expanded = true;
        }
        if (is_hub && c[0] == '\0') {
            hub_false_deadcode = true;
        }
        if (strcmp(f, "leaf") == 0 && c[0] == '\0') {
            leaf_deadcode = true;
        }
    }
    ASSERT_TRUE(hub_expanded);
    ASSERT_FALSE(hub_false_deadcode); /* live code with callers must never appear as dead */
    ASSERT_TRUE(leaf_deadcode);       /* genuine no-match row still survives saturation */

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

/* #1196: max_rows limits projected results, not the source-node candidates
 * considered before WHERE. The old unlabeled scan searched only
 * 10 * max_rows nodes, so a valid match later in search order disappeared. */
TEST(cypher_exec_unlabeled_where_beyond_result_limit_issue1196) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    for (int i = 0; i < 11; i++) {
        char name[32];
        char qn[64];
        snprintf(name, sizeof(name), "early_%02d", i);
        snprintf(qn, sizeof(qn), "test.%s", name);
        cbm_node_t distractor = {.project = "test",
                                 .label = "Function",
                                 .name = name,
                                 .qualified_name = qn,
                                 .file_path = "early.py"};
        ASSERT_GT(cbm_store_upsert_node(s, &distractor), 0);
    }

    cbm_node_t late = {.project = "test",
                       .label = "Function",
                       .name = "zz_late_match",
                       .qualified_name = "test.zz_late_match",
                       .file_path = "late.py"};
    ASSERT_GT(cbm_store_upsert_node(s, &late), 0);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (n) WHERE n.name = \"zz_late_match\" RETURN n.name",
                                "test", 1, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "zz_late_match");

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* #1196 (second mechanism): relationship expansion capped this hop's TOTAL
 * output at bind_cap*10, so edges past the cap were silently dropped BEFORE
 * WHERE and aggregation — a count() then reported the scanned prefix as if
 * it were a fact (field-measured: 9,360 of 13,691 DEFINES with a labeled
 * source and --max-rows 1000). max_rows is an OUTPUT-row limit (projection
 * already enforces it); expansion must see every matched edge. Fixture: 2
 * labeled sources with 30 edges each; max_rows=2 makes the old cap 20. */
TEST(cypher_exec_aggregate_sees_all_edges_beyond_expansion_cap_issue1196) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_store_upsert_project(s, "test", "/tmp/test"), CBM_STORE_OK);

    int64_t src_ids[2];
    for (int i = 0; i < 2; i++) {
        char name[32];
        char qn[64];
        snprintf(name, sizeof(name), "file_%d", i);
        snprintf(qn, sizeof(qn), "test.%s", name);
        cbm_node_t src = {.project = "test",
                          .label = "File",
                          .name = name,
                          .qualified_name = qn,
                          .file_path = name};
        src_ids[i] = cbm_store_upsert_node(s, &src);
        ASSERT_GT(src_ids[i], 0);
    }
    for (int i = 0; i < 2; i++) {
        for (int j = 0; j < 30; j++) {
            char name[32];
            char qn[64];
            snprintf(name, sizeof(name), "def_%d_%02d", i, j);
            snprintf(qn, sizeof(qn), "test.%s", name);
            cbm_node_t target = {.project = "test",
                                 .label = "Function",
                                 .name = name,
                                 .qualified_name = qn,
                                 .file_path = "defs.py"};
            int64_t tid = cbm_store_upsert_node(s, &target);
            ASSERT_GT(tid, 0);
            cbm_edge_t e = {
                .project = "test", .source_id = src_ids[i], .target_id = tid, .type = "DEFINES"};
            cbm_store_insert_edge(s, &e);
        }
    }

    /* Aggregate: one output row, so max_rows=2 never limits the OUTPUT —
     * only the (buggy) expansion. Ground truth: 60 edges. */
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s, "MATCH (a:File)-[rel]->(b) RETURN count(rel)", "test", 2, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "60");
    cbm_cypher_result_free(&r);

    /* The list form must saturate at the output limit, not at the scan:
     * max_rows=25 returns exactly 25 rows (old cap: bind_cap=25 -> 250,
     * fine here — but max_rows=2 must return 2 rows, not 2-of-20-scanned). */
    cbm_cypher_result_t r2 = {0};
    rc = cbm_cypher_execute(s, "MATCH (a:File)-[rel]->(b) RETURN b.name", "test", 2, &r2);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r2.row_count, 2);
    cbm_cypher_result_free(&r2);

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
 * 2-chain loopy->mid->leaf. Correct openCypher semantics:
 *  - a repeated node variable must unify: (a)-[:CALLS]->(a) matches ONLY
 *    the self-loop, not every edge;
 *  - relationship uniqueness within a path: the self-loop cannot be
 *    traversed repeatedly, so no *k..k path exists beyond the real chain;
 *  - the engine hop cap must not fabricate or silently truncate results. */
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

    /* Bug 2: *2..2 from loopy has two relationship-unique trails: the
     * self-loop followed by e1 reaches mid, and e1 followed by e2 reaches leaf.
     * Reusing the self-loop within one trail remains forbidden. */
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
        saw_mid |= strcmp(r2.rows[i][0], "mid") == 0;
        saw_leaf |= strcmp(r2.rows[i][0], "leaf") == 0;
    }
    ASSERT_TRUE(saw_mid);
    ASSERT_TRUE(saw_leaf);
    cbm_cypher_result_free(&r2);

    /* Bug 2 amplifier: no directed path of length 5 exists at all. */
    cbm_cypher_result_t r3 = {0};
    ASSERT_EQ(cbm_cypher_execute(s,
                                 "MATCH (a {name: \"loopy\"})-[:CALLS*5..5]->(b) "
                                 "RETURN b.name",
                                 "test", 0, &r3),
              0);
    ASSERT_EQ(r3.row_count, 0);
    cbm_cypher_result_free(&r3);

    /* Bug 3: a hop range beyond the engine ceiling must be an ADVERTISED
     * clamp, not silently indistinguishable from "no such path". */
    cbm_cypher_result_t r4 = {0};
    ASSERT_EQ(
        cbm_cypher_execute(s, "MATCH (a)-[:CALLS*150..150]->(b) RETURN b.name", "test", 0, &r4), 0);
    ASSERT_EQ(r4.row_count, 0);
    ASSERT_NOT_NULL(r4.warning);
    ASSERT_NOT_NULL(strstr(r4.warning, "clamped"));
    cbm_cypher_result_free(&r4);

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

TEST(cypher_exec_variable_length_repeated_node_var_unifies) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};

    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function)-[:CALLS*1..2]->(f:Function) "
                                "RETURN f.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Reproduce-first (#887): an EXPLICIT variable-length upper bound must still be
 * capped at the engine ceiling (cbm_cypher_max_depth(), default 10). On
 * origin/main, expand_var_length honoured an explicit `*1..N` verbatim (only the
 * unbounded `*` / `*..m` forms were capped), so `[:CALLS*1..N]` passed N straight
 * to cbm_store_bfs — an unbounded traversal (a DoS on cyclic graphs). RED before
 * the clamp: a *1..12 walk over a 13-node chain
 * returns all 12 hops (N01..N12). GREEN after: it stops at the depth-10 ceiling
 * (N01..N10); N11/N12 are never emitted. max_rows=64 keeps the binding-expansion
 * cap (bind_cap*10) well above the hop count, so DEPTH — not the binding cap — is
 * the bound under test. */
TEST(cypher_exec_var_length_explicit_bound_capped) {
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

    /* Capped at 10 hops → exactly N01..N10; N11/N12 are beyond the ceiling. */
    ASSERT_EQ(r.row_count, 10);
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
    ASSERT_TRUE(saw_n10);  /* within the ceiling — proves the traversal really ran */
    ASSERT_FALSE(saw_n11); /* clamped away */
    ASSERT_FALSE(saw_n12);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* Pin the relationship-trail contract: a self-loop edge cannot be reused
 * within one variable-length trail, so *2..2 yields no fabricated match. */
TEST(cypher_exec_var_length_no_reuse_self_loop) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "Recursive",
                    .qualified_name = "test.Recursive",
                    .file_path = "recursive.go"};
    int64_t id = cbm_store_upsert_node(s, &n);
    cbm_edge_t e = {.project = "test", .source_id = id, .target_id = id, .type = "CALLS"};
    cbm_store_insert_edge(s, &e);

    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (f:Function {name: \"Recursive\"})-[:CALLS*2..2]"
                                "->(g:Function) RETURN g.name",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0);

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

TEST(cypher_exec_var_length_truncation_surfaces_warning) {
    cbm_store_t *s = cbm_store_open_memory();
    cbm_store_upsert_project(s, "test", "/tmp/test");

    int64_t ids[18];
    for (int i = 0; i < 18; i++) {
        char name[16];
        snprintf(name, sizeof(name), "node-%d", i);
        cbm_node_t node = {.project = "test",
                           .label = "Function",
                           .name = name,
                           .qualified_name = name,
                           .file_path = "graph.c"};
        ids[i] = cbm_store_upsert_node(s, &node);
    }
    for (int source = 0; source < 17; source++) {
        for (int target = source + 1; target < 18; target++) {
            cbm_edge_t edge = {.project = "test",
                               .source_id = ids[source],
                               .target_id = ids[target],
                               .type = "CALLS"};
            cbm_store_insert_edge(s, &edge);
        }
    }

    cbm_cypher_result_t result = {0};
    int rc = cbm_cypher_execute(s,
                                "MATCH (a:Function {name: \"node-0\"})-[:CALLS*10..10]->"
                                "(b:Function) RETURN b.name",
                                "test", 0, &result);
    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(result.warning);
    ASSERT_TRUE(strstr(result.warning, "traversal budget") != NULL);
    ASSERT_TRUE(result.row_count > 0);

    cbm_cypher_result_free(&result);
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

/* issue #373: an unsupported computed expression in WITH/RETURN (an unknown
 * function like split(...) or list indexing [..]) must FAIL LOUDLY with a clear
 * "unsupported function" error rather than silently projecting an empty column
 * (which looks like a valid-but-blank result and hides the real problem). */
/* issue #1919: a variable the WITH clause dropped must be REFUSED, not projected
 * as an empty column. `g` does not survive `WITH f.name AS caller`, so naming it
 * afterwards is a query fault. The old code answered NULL for the unbound name,
 * rendered it "", and exited clean — a column of nothing that reads as "the graph
 * holds no such data" rather than "your query named something out of scope".
 * Same principle as #373: fail loudly instead of projecting a blank column. */
TEST(cypher_rejects_projection_of_dropped_with_var_issue1919) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function)-[:CALLS]->(g) WITH f.name AS caller RETURN caller, g.name", "test",
        0, &r);
    ASSERT_TRUE(rc != 0);
    ASSERT_NOT_NULL(r.error);
    /* The message has to name the offending variable — an error that does not
     * say which name is wrong sends the reader back to guessing. */
    ASSERT_TRUE(strstr(r.error, "g") != NULL);
    ASSERT_TRUE(strstr(r.error, "scope") != NULL);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* The guard must NOT reject an OPTIONAL MATCH target that simply did not match.
 * `g` is a declared pattern variable there, so it stays in scope; it is merely
 * unbound at run time, and projecting "" for it is the documented convention.
 * This is the line between the two cases, and the reason the check reads the
 * query's declared variables rather than the run-time bindings. */
TEST(cypher_optional_match_target_still_allowed_issue1919) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function) OPTIONAL MATCH (f)-[:CALLS]->(g) RETURN f.name, g.name", "test", 0,
        &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_TRUE(r.row_count > 0);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* An alias the WITH created is in scope afterwards, and a name carried through
 * unchanged is too. Both must keep working. */
TEST(cypher_with_alias_stays_in_scope_issue1919) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    int rc = cbm_cypher_execute(
        s, "MATCH (f:Function)-[:CALLS]->(g) WITH f.name AS caller, g AS callee RETURN caller, callee.name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(r.error);
    ASSERT_TRUE(r.row_count > 0);
    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

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
    cbm_cypher_result_free(&r);

    cbm_store_close(s);
    PASS();
}

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

/* Regression for #1334: a LIMIT must survive a multi-key ORDER BY. The parser
 * used to stop at the first sort key, leaving ", key2 ... LIMIT n" unconsumed —
 * the whole result set came back (6326 rows instead of 5 on the reporter's
 * graph: a token-flood into agent context). */
TEST(cypher_exec_multikey_order_by_keeps_limit_issue1334) {
    cbm_store_t *s = setup_cypher_store();
    cbm_cypher_result_t r = {0};
    /* start_lines: HandleOrder=10, ValidateOrder=5, SubmitOrder=0, LogError=0 */
    int rc = cbm_cypher_execute(s,
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
    int rc = cbm_cypher_execute(s,
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
                                "RETURN g.file_path, g.name, c",
                                "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 1);
    ASSERT_STR_EQ(r.rows[0][0], "validate.go"); /* was "" before the fix */
    ASSERT_STR_EQ(r.rows[0][1], "ValidateOrder");
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
        s, "MATCH (a)-[r]->(b) WITH type(r) AS t, count(*) AS n RETURN t, n ORDER BY n DESC",
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

/* Result projection writes into fixed-width per-row stack arrays
 * (vals[CBM_SZ_32] / func_bufs[CBM_SZ_32][…] in execute_return_simple and its
 * siblings), indexed by the parsed RETURN item count. The parser must bound
 * that count to the array width; an over-wide RETURN has to be rejected, not
 * allowed to run and write past the arrays. Drive a >32-column RETURN in a
 * forked child so a stack overwrite (ASan abort, or a raw segfault) shows up
 * as a killing signal instead of taking down the whole runner; the bounded
 * path returns an ordinary error and the child exits cleanly. */
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

TEST(cypher_wide_return_projection_bounded) {
#ifdef _WIN32
    SKIP_PLATFORM("fork crash-isolation is POSIX-only; the parse-time bound is platform-agnostic");
#else
    char query[4096];
    int off = snprintf(query, sizeof(query), "MATCH (f:Function) RETURN ");
    for (int i = 0; i < 48; i++) { /* 48 > CBM_SZ_32 (32) fixed columns */
        off += snprintf(query + off, sizeof(query) - (size_t)off, "%sf.p%d", i ? ", " : "", i);
    }
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        cbm_store_t *s = setup_cypher_store();
        cbm_cypher_result_t r = {0};
        (void)cbm_cypher_execute(s, query, "test", 0, &r);
        cbm_cypher_result_free(&r);
        cbm_store_close(s);
        _exit(0); /* reached only when execution did NOT overflow */
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFSIGNALED(status)) {
        char msg[96];
        snprintf(msg, sizeof(msg), "wide RETURN killed by signal %d — projection stack overflow",
                 WTERMSIG(status));
        FAIL(msg);
    }
    ASSERT_TRUE(WIFEXITED(status));
    PASS();
#endif
}

/* #601: an unbounded whole-graph OPTIONAL MATCH / GROUP BY does
 * O(bindings x groups) work and can run for minutes with no wall-clock guard —
 * the 100k row ceiling never fires because no rows are produced, so query_graph
 * just hangs. With the execution deadline armed to trip immediately (budget 0),
 * the runaway query must abort with a clear error instead of returning a
 * (misleading, possibly partial) result.
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
    RUN_TEST(cypher_parse_rejects_trailing_tokens);
    RUN_TEST(cypher_parse_rejects_second_with_clause);
    RUN_TEST(cypher_parse_accepts_single_with_clause);
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
    RUN_TEST(cypher_parse_multikey_order_by_issue1334);
    RUN_TEST(cypher_parse_order_by_over_cap_rejected_issue1334);
    RUN_TEST(cypher_parse_return_distinct);
    RUN_TEST(cypher_parse_inline_props);
    RUN_TEST(cypher_parse_error);
    /* Execution */
    RUN_TEST(cypher_exec_deadline_aborts_runaway_query_issue601);
    RUN_TEST(cypher_exec_deadline_allows_normal_query_issue601);
    RUN_TEST(cypher_deep_nesting_rejected_not_crash);
    RUN_TEST(cypher_exec_match_all_functions);
    RUN_TEST(cypher_exec_optional_empty_label_no_overflow);
    RUN_TEST(cypher_cross_join_alloc_rejects_overflow);
    RUN_TEST(cypher_exec_optional_rel_saturated_no_overflow);
    RUN_TEST(cypher_exec_optional_saturated_does_not_fabricate_no_match);
    RUN_TEST(cypher_exec_optional_rel_leaf_fallback_survives);
    RUN_TEST(cypher_exec_bound_terminal_optional_fallback_survives);
    RUN_TEST(cypher_exec_bound_terminal_saturation_no_false_deadcode);
    RUN_TEST(cypher_issue240_labels_function);
    RUN_TEST(cypher_issue237_distinct_order_limit);
    RUN_TEST(cypher_issue873_distinct_order_limit_dedupes_before_limit);
    RUN_TEST(cypher_issue873_distinct_limit_dedupes_before_limit);
    RUN_TEST(cypher_issue873_distinct_order_skip_limit_dedupes_before_skip);
    RUN_TEST(cypher_issue252_tointeger);
    RUN_TEST(cypher_issue305_count_star_alias);
    RUN_TEST(cypher_exec_where_eq);
    RUN_TEST(cypher_exec_unlabeled_where_beyond_result_limit_issue1196);
    RUN_TEST(cypher_exec_aggregate_sees_all_edges_beyond_expansion_cap_issue1196);
    RUN_TEST(cypher_exec_varlength_path_semantics_issue797);
    RUN_TEST(cypher_exec_where_coalesce_issue874);
    RUN_TEST(cypher_exec_where_regex);
    RUN_TEST(cypher_exec_where_contains);
    RUN_TEST(cypher_exec_where_starts_with);
    RUN_TEST(cypher_exec_return_properties);
    RUN_TEST(cypher_func_labels);
    RUN_TEST(cypher_func_type);
    RUN_TEST(cypher_func_id);
    RUN_TEST(cypher_func_keys);
    RUN_TEST(cypher_func_properties);
    RUN_TEST(cypher_func_tointeger_tofloat);
    RUN_TEST(cypher_func_size_reverse);
    RUN_TEST(cypher_func_multiarg);
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
    RUN_TEST(cypher_exec_variable_length_repeated_node_var_unifies);
    RUN_TEST(cypher_exec_var_length_explicit_bound_capped);
    RUN_TEST(cypher_exec_var_length_no_reuse_self_loop);
    RUN_TEST(cypher_exec_var_length_truncation_surfaces_warning);
    RUN_TEST(cypher_exec_defines_edge);
    RUN_TEST(cypher_exec_no_results);
    RUN_TEST(cypher_exec_where_numeric);
    /* Go test ports */
    RUN_TEST(cypher_exec_distinct);
    RUN_TEST(cypher_exec_with_distinct_issue238);
    RUN_TEST(cypher_exec_where_label_test_issue241);
    RUN_TEST(cypher_exec_label_alternation_issue242);
    RUN_TEST(cypher_exec_count_distinct_issue239);
    RUN_TEST(cypher_exec_unsupported_func_errors_issue373);
    RUN_TEST(cypher_rejects_projection_of_dropped_with_var_issue1919);
    RUN_TEST(cypher_optional_match_target_still_allowed_issue1919);
    RUN_TEST(cypher_with_alias_stays_in_scope_issue1919);
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
    RUN_TEST(cypher_parse_unwind_oversized_literal_no_overflow);
    RUN_TEST(cypher_parse_unwind_many_elements_no_overflow);
    RUN_TEST(cypher_wide_return_projection_bounded);
    /* Composite property projection (arrays/objects, escaped quotes) */
    RUN_TEST(cypher_exec_prop_array_with_internal_commas);
    RUN_TEST(cypher_exec_prop_string_with_escaped_quote);
}
