/*
 * repro_new_cypher_limit_zero.c -- Reproduce-first case for a NEW, un-filed
 * bug discovered during QA sweep (2026-06-26).
 *
 * BUG: `LIMIT 0` in a Cypher query does NOT return 0 rows; instead it
 * returns ALL rows, treating `LIMIT 0` as equivalent to "no limit".
 *
 * ROOT CAUSE -- src/cypher/cypher.c, two co-located guards that conflate
 * "no limit specified" (limit==-1 or limit==0 as sentinel) with
 * "explicitly requested limit of zero".
 *
 * GUARD 1 -- rb_apply_skip_limit (~line 3095):
 *
 *   if (limit > 0 && rb->row_count > limit) { ... rb->row_count = limit; }
 *
 *   When limit==0 (from LIMIT 0), the condition `limit > 0` is FALSE, so
 *   the row count is never trimmed to zero.
 *
 * GUARD 2 -- execute_single RETURN path (~line 4249):
 *
 *   rb_apply_skip_limit(rb, ret->skip,
 *                        ret->limit > 0 ? ret->limit : max_rows);
 *
 *   When ret->limit==0, `ret->limit > 0` is FALSE so max_rows is passed
 *   as the limit argument instead of 0, returning ALL rows.
 *
 * GUARD 3 -- with_sort_skip_limit / bindings_skip_limit (~line 3409):
 *
 *   if (limit > 0 && *count > limit) { ... *count = limit; }
 *
 *   Same pattern: limit==0 never triggers the trim.
 *
 * The root cause: the engine uses `limit == 0` as the sentinel value for
 * "no LIMIT clause was specified" rather than using a distinct negative
 * sentinel (e.g. -1).  When the user explicitly writes `LIMIT 0`, the
 * parsed value is also 0 -- indistinguishable from "unset" -- so all
 * guards treat it as "no limit".
 *
 * EXPECTED (correct) behavior:
 *   `MATCH (f:Function) RETURN f.name LIMIT 0` must return 0 rows.
 *   In standard Cypher, LIMIT N is an upper bound; LIMIT 0 means "at most
 *   0 rows", i.e., an empty result set.
 *
 * ACTUAL (buggy) behavior:
 *   All rows are returned (row_count == 4 in the standard fixture).
 *   ASSERT_EQ(r.row_count, 0) fires -> RED.
 *
 * HOW TO CONFIRM WITHOUT COMPILING:
 *   1. cypher.c parse_return_or_with (~line 1665): `LIMIT N` sets
 *      r->limit = strtol(num->text) = 0 for `LIMIT 0`.
 *   2. rb_apply_skip_limit (~line 3095): guard `if (limit > 0 ...)` --
 *      FALSE for limit=0 -- trimming is skipped.
 *   3. execute_single return path (~line 4249): `ret->limit > 0 ?
 *      ret->limit : max_rows` evaluates to max_rows when limit==0, so
 *      the full row set is preserved.
 *
 * FIX LOCATION (not implemented here):
 *   Use a sentinel of -1 (not 0) for "LIMIT not specified" so that
 *   limit==0 can be distinguished as an explicit request for zero rows.
 *   Change the initializer in cbm_return_clause_t to use -1, update the
 *   parser to set limit = (int)strtol() only (already correct), and change
 *   all guards from `limit > 0` to `limit >= 0` (or `limit != -1`).
 */

#include "test_framework.h"
#include <cypher/cypher.h>
#include <store/store.h>
#include <string.h>
#include <stdlib.h>

/* Build the same standard 4-Function fixture used by test_cypher.c. */
static cbm_store_t *setup_limit_store(void) {
    cbm_store_t *s = cbm_store_open_memory();
    if (!s) return NULL;
    cbm_store_upsert_project(s, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test", .label = "Function", .name = "HandleOrder",
                     .qualified_name = "test.HandleOrder", .file_path = "handler.go"};
    cbm_node_t n2 = {.project = "test", .label = "Function", .name = "ValidateOrder",
                     .qualified_name = "test.ValidateOrder", .file_path = "validate.go"};
    cbm_node_t n3 = {.project = "test", .label = "Function", .name = "SubmitOrder",
                     .qualified_name = "test.SubmitOrder", .file_path = "submit.go"};
    cbm_node_t n4 = {.project = "test", .label = "Function", .name = "LogError",
                     .qualified_name = "test.LogError", .file_path = "log.go"};

    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);
    cbm_store_upsert_node(s, &n4);
    return s;
}

/*
 * repro_new_cypher_limit_zero_returns_no_rows
 *
 * PRECONDITION: LIMIT 2 works correctly (so the engine is running).
 *
 * PRIMARY ASSERTION: LIMIT 0 must return row_count == 0.
 *
 * WHY RED on current code:
 *   rb_apply_skip_limit is called with limit=max_rows (not 0) because
 *   `ret->limit > 0 ? ret->limit : max_rows` evaluates to max_rows when
 *   ret->limit==0.  All 4 Function rows are preserved -> row_count==4 ->
 *   ASSERT_EQ(r.row_count, 0) fires -> RED.
 */
TEST(repro_new_cypher_limit_zero_returns_no_rows) {
    cbm_store_t *s = setup_limit_store();
    ASSERT_NOT_NULL(s);

    cbm_cypher_result_t r = {0};

    /* Precondition: LIMIT 2 works and returns exactly 2 rows.
     * If RED here, the engine itself is broken -- unrelated to #limit-zero. */
    int rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name LIMIT 2", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 2);
    cbm_cypher_result_free(&r);

    /* Precondition: without LIMIT there are 4 Function rows (ground truth). */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 4);
    cbm_cypher_result_free(&r);

    /* PRIMARY ASSERTION: LIMIT 0 must return 0 rows.
     *
     * WHY RED: limit is parsed as 0.  In execute_single's return path:
     *   rb_apply_skip_limit(rb, ret->skip,
     *                        ret->limit > 0 ? ret->limit : max_rows)
     * evaluates to rb_apply_skip_limit(rb, 0, max_rows) -- limit arg is
     * max_rows, not 0 -- so rb_apply_skip_limit's own guard
     * `if (limit > 0 && rb->row_count > limit)` triggers and trims to
     * max_rows (which >= 4), leaving all 4 rows.
     * row_count == 4 -> ASSERT_EQ(r.row_count, 0) fires -> RED. */
    memset(&r, 0, sizeof(r));
    rc = cbm_cypher_execute(s, "MATCH (f:Function) RETURN f.name LIMIT 0", "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0); /* RED on buggy code: returns 4 rows */

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/*
 * repro_new_cypher_limit_zero_with_clause
 *
 * The same LIMIT 0 bug manifests in the WITH clause path, which uses
 * with_sort_skip_limit -> bindings_skip_limit.
 *
 * WHY RED on current code:
 *   with_sort_skip_limit calls bindings_skip_limit(vbindings, vcount, skip, wc->limit).
 *   bindings_skip_limit guard: `if (limit > 0 && *count > limit)` -- FALSE for
 *   limit==0 -- count is not trimmed to 0.  The WITH ... LIMIT 0 clause carries
 *   all bindings forward -> RETURN still returns 4 rows -> ASSERT_EQ fires -> RED.
 */
TEST(repro_new_cypher_limit_zero_with_clause) {
    cbm_store_t *s = setup_limit_store();
    ASSERT_NOT_NULL(s);

    cbm_cypher_result_t r = {0};

    /* WITH ... LIMIT 0 should produce zero bindings, so RETURN returns nothing. */
    int rc = cbm_cypher_execute(
        s,
        "MATCH (f:Function) WITH f LIMIT 0 RETURN f.name",
        "test", 0, &r);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(r.row_count, 0); /* RED on buggy code: returns 4 rows */

    cbm_cypher_result_free(&r);
    cbm_store_close(s);
    PASS();
}

/* ---- Suite --------------------------------------------------------------- */
SUITE(repro_new_cypher_limit_zero) {
    RUN_TEST(repro_new_cypher_limit_zero_returns_no_rows);
    RUN_TEST(repro_new_cypher_limit_zero_with_clause);
}
