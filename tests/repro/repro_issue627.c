/*
 * repro_issue627.c -- Reproduce-first case for OPEN bug #627.
 *
 * Issue: #627 -- "Crash when calling query_graph"
 * Reporter: zbynekwinkler
 *
 * EXACT CRASHING INPUT (from issue body):
 *
 *   MATCH (f:Function)
 *   WHERE NOT f.file_path CONTAINS 'ext'
 *     AND NOT f.file_path CONTAINS 'Tests'
 *     AND NOT f.file_path CONTAINS 'examples'
 *     AND NOT f.name = 'main'
 *   OPTIONAL MATCH (c)-[:CALLS]->(f)
 *   WITH f, c
 *   WHERE c IS NULL
 *   RETURN f.name, f.qualified_name, f.file_path, f.start_line
 *   ORDER BY f.file_path
 *   LIMIT 50
 *
 * ROOT CAUSE (src/cypher/cypher.c, expand_additional_patterns + cross_join_with_rels):
 *
 *   When executing the second pattern "OPTIONAL MATCH (c)-[:CALLS]->(f)",
 *   expand_additional_patterns() (line ~4201) checks whether nodes[0] of the
 *   second pattern (variable "c") is already bound.  "c" is a NEW variable, so
 *   start_bound=false and execution falls into the else branch (line ~4210).
 *
 *   That branch calls scan_pattern_nodes() for "c" -- returning ALL nodes in the
 *   graph (no label filter on "c") -- and then cross_join_with_rels() to combine
 *   each candidate "c" with the existing "f" bindings.
 *
 *   cross_join_with_rels() computes its pre-allocation as:
 *
 *     malloc((*bind_count * extra_count * CYP_GROWTH_10 + 1) * sizeof(binding_t))
 *
 *   All three operands are "int".  With a graph of ~29 K nodes:
 *     bind_count  ~ 29 000  (Function nodes from the first MATCH after WHERE)
 *     extra_count ~ 29 000  (ALL nodes scanned for unbound "c")
 *     CYP_GROWTH_10 = 10
 *
 *   29000 * 29000 * 10 = 8 410 000 000 -- overflows signed 32-bit int, wrapping
 *   to a small/negative value.  cast to size_t this becomes a near-zero or
 *   near-SIZE_MAX value.  malloc returns either NULL (OOM) or a tiny block.
 *   The subsequent loop writes new_bindings[new_count++] past the allocation
 *   boundary, corrupting the heap -> SIGSEGV / SIGABRT.
 *
 *   A secondary bug compounds the crash: even when the multiplication does NOT
 *   overflow (small graphs), expand_additional_patterns() ignores the fact that
 *   the second pattern's terminal node "f" IS ALREADY BOUND.  process_edges()
 *   (line ~2860) calls binding_set(&nb, "f", &found) unconditionally, overwriting
 *   the caller's copy of "f" with whatever node the edge leads to, instead of
 *   filtering to only edges whose target matches the already-bound "f".  This
 *   produces semantically wrong results: the final WHERE c IS NULL filter and
 *   the RETURN f.name etc. operate on corrupted "f" bindings.
 *
 * EXPECTED (correct) behaviour:
 *   query_graph returns -- without crashing -- the list of Function nodes that
 *   have NO inbound CALLS edges (i.e. dead-code / uncalled functions).  In our
 *   fixture, "orphan_func" is defined but never called; "leaf_func" is called by
 *   "caller_func".  The correct result set must include "orphan_func" and must
 *   NOT include "leaf_func".
 *
 * ACTUAL (buggy) behaviour:
 *   On a graph with tens of thousands of nodes: SIGSEGV / SIGABRT (integer
 *   overflow in the malloc size, heap OOB write).
 *   On a small fixture: wrong result set due to overwritten "f" bindings; the
 *   assertion that "orphan_func" appears in the result and "leaf_func" does not
 *   fails.
 *
 * WHY RED on current code:
 *   - The fork detects a crash signal (WIFSIGNALED) if it occurs.
 *     ASSERT_FALSE(WIFSIGNALED(st)) fires when the child is killed by a signal.
 *   - Even without a crash signal the result-content assertion is RED: because
 *     expand_additional_patterns() misbinds "f", the query does not correctly
 *     identify uncalled functions.  "orphan_func" may be absent or "leaf_func"
 *     may be present in the response, causing one of the content assertions to
 *     fail -> RED.
 *
 * Fix location (NOT implemented here):
 *   src/cypher/cypher.c -- expand_additional_patterns() must detect when the
 *   TERMINAL node of the additional pattern is already bound (here "f") and drive
 *   the join from that side (inbound edge scan from f), not by scanning all nodes
 *   for "c".  Additionally, process_edges() must check whether to_var is already
 *   bound and, if so, only emit a match when the found node's id equals the
 *   already-bound node's id.  The malloc in cross_join_with_rels() must use
 *   size_t arithmetic (not int) to avoid the overflow.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "repro_harness.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#if !defined(_WIN32)
#include <sys/wait.h>
#endif

/*
 * Fixture: three Python functions.
 *
 *   leaf_func()    -- called by caller_func(); has >= 1 inbound CALLS edge
 *   caller_func()  -- calls leaf_func(); has 0 inbound CALLS edges
 *   orphan_func()  -- never called; has 0 inbound CALLS edges
 *
 * A dead-code query ("find functions with no inbound CALLS edges") must
 * return both "caller_func" and "orphan_func" but NOT "leaf_func".
 *
 * We assert the narrower claim: "orphan_func" IN result AND "leaf_func" NOT IN
 * result.  This is the minimal check that distinguishes correct behaviour from
 * the current buggy one (which either crashes or returns the wrong set).
 *
 * Python is chosen because Python CALLS extraction is confirmed reliable
 * (test_extraction.c validates it, and the regression suite's python fixtures
 * consistently produce CALLS edges).
 */
static const RFile k_files[] = {
    {
        "funcs.py",
        "def leaf_func():\n"
        "    return 42\n"
        "\n"
        "def caller_func():\n"
        "    return leaf_func()\n"
        "\n"
        "def orphan_func():\n"
        "    return 99\n"
    }
};

/*
 * Dead-code Cypher query -- identical structure to the reporter's crashing query.
 * We omit the file_path / name filters (the fixture path can vary) so we test
 * the OPTIONAL MATCH + WITH + WHERE c IS NULL pattern in isolation.
 */
static const char k_query[] =
    "MATCH (f:Function) "
    "OPTIONAL MATCH (c)-[:CALLS]->(f) "
    "WITH f, c "
    "WHERE c IS NULL "
    "RETURN f.name, f.qualified_name, f.file_path, f.start_line "
    "ORDER BY f.name "
    "LIMIT 50";

/* --------------------------------------------------------------------------
 * repro_issue627_query_graph_no_crash
 *
 * Precondition: the indexer produced at least one CALLS edge (leaf_func
 * called by caller_func).  If this fires RED the fixture or Python CALLS
 * extraction is broken -- unrelated to #627.
 *
 * Primary crash assertion (POSIX only):
 *   Run query_graph in a forked child; assert WIFSIGNALED is false.
 *   RED if the child is killed (SIGSEGV/SIGABRT from the heap OOB).
 *
 * Secondary correctness assertion (all platforms):
 *   The result must include "orphan_func" (an uncalled function) and must
 *   NOT include "leaf_func" (which has an inbound CALLS edge).
 *   RED if the wrong-binding bug causes the result to be empty or inverted.
 * -------------------------------------------------------------------------- */
TEST(repro_issue627_query_graph_no_crash) {
    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, k_files,
                                        (int)(sizeof(k_files) / sizeof(k_files[0])));
    ASSERT_NOT_NULL(store);

    /* Precondition: caller_func -> leaf_func must have produced >= 1 CALLS edge.
     * If RED here, the fixture has an extraction problem, not a #627 symptom. */
    int calls_count = rh_count_edges(store, lp.project, "CALLS");
    ASSERT_GT(calls_count, 0);

    char args[1024];
    snprintf(args, sizeof(args),
             "{\"project\":\"%s\","
             "\"query\":\"%s\"}",
             lp.project, k_query);

#if !defined(_WIN32)
    /* ---- POSIX crash-isolation via fork ---------------------------------- */
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        /* Child: run query_graph; exit cleanly if no crash. */
        char *r = cbm_mcp_handle_tool(lp.srv, "query_graph", args);
        if (r)
            free(r);
        _exit(0);
    }

    int st = 0;
    (void)waitpid(pid, &st, 0);

    /* PRIMARY assertion: query_graph must NOT crash the process.
     * WHY RED on buggy code (large graphs):
     *   integer overflow in cross_join_with_rels malloc size ->
     *   heap OOB write -> child receives SIGSEGV or SIGABRT ->
     *   WIFSIGNALED(st) is true -> ASSERT_FALSE fires. */
    ASSERT_FALSE(WIFSIGNALED(st));
#endif

    /* ---- Correctness assertion (all platforms) --------------------------- */
    /* Run the query in the parent to inspect the result content.
     * Even on small graphs where the crash does not occur, the wrong-binding
     * bug causes query_graph to return an incorrect result set. */
    char *resp = cbm_mcp_handle_tool(lp.srv, "query_graph", args);
    ASSERT_NOT_NULL(resp);

    /* Must not be an error response. */
    ASSERT_NULL(strstr(resp, "\"is_error\":true"));

    /* "orphan_func" has zero inbound CALLS edges -> must appear in the
     * dead-code result set.
     * WHY RED on buggy code: expand_additional_patterns scans ALL nodes
     * for "c", overwrites the already-bound "f" in each binding with the
     * CALLS-edge target, and the corrupted "f" bindings fail to identify
     * orphan_func as uncalled.  strstr returns NULL -> ASSERT_NOT_NULL fails. */
    ASSERT_NOT_NULL(strstr(resp, "orphan_func"));

    /* "leaf_func" IS called by caller_func -> must NOT appear in the dead-code
     * result.
     * WHY RED on buggy code: the "f" binding corruption may let leaf_func
     * slip through the WHERE c IS NULL filter. */
    ASSERT_NULL(strstr(resp, "leaf_func"));

    free(resp);
    rh_cleanup(&lp, store);
    PASS();
}

/* ---- Suite --------------------------------------------------------------- */
SUITE(repro_issue627) {
    RUN_TEST(repro_issue627_query_graph_no_crash);
}
