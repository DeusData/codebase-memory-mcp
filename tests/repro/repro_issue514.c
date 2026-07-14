/*
 * repro_issue514.c -- Reproduce-first case for OPEN bug #514.
 *
 * Issue: #514 -- "trace_path data_flow mode doesn't surface arg expressions;
 *                NestJS DI patterns defeat ~70% of caller resolution"
 *
 * Sub-claim reproduced: (A) data_flow mode omits argument expressions.
 *
 * Why sub-claim A over sub-claim B (NestJS DI caller resolution):
 *   (A) has a crisp binary assertion: the "e" field either appears in the JSON
 *   output or it does not.  (B) is a statistical claim (~70% failure rate) that
 *   requires a NestJS-specific fixture and a headcount of resolved callers across
 *   many call sites -- impossible to assert precisely in a unit test.  (A) can
 *   be reproduced with a small two-function Python fixture and one strstr check.
 *
 * Root cause:
 *   The MCP schema for trace_path documents data_flow mode as "follow CALLS +
 *   DATA_FLOWS with arg expressions" (mcp.c line 356-357 and 363-364).  Argument
 *   expressions at each call site ARE stored in the graph: pass_parallel.c::
 *   append_args_json serializes each CBMCallArg as {"i":<index>,"e":"<expr>,...}
 *   into the CALLS edge properties_json column.  However,
 *   bfs_to_json_array() (mcp.c ~line 2283) only emits the node fields (name,
 *   qualified_name, hop, risk, is_test) from cbm_node_hop_t.  The edge that
 *   carried the arg expressions is NOT propagated by cbm_store_bfs() into the
 *   cbm_traverse_result_t (cbm_edge_info_t carries only from_name, to_name,
 *   type, confidence -- no properties_json).  So even if the user requests
 *   mode="data_flow", every hop in the response lacks the "args" field and the
 *   individual arg expression text ("e") is permanently absent from the output.
 *
 * Expected (correct) behaviour:
 *   After indexing a two-function Python file where caller() passes a compound
 *   expression (payload_info + 1) to callee(), a trace_path call with
 *   mode="data_flow" and direction="outbound" on "caller" must include the
 *   argument expression text "payload_info" in the response JSON -- either in an
 *   "args" array inside the hop object, or as a standalone "e" field.
 *
 * Actual (buggy) behaviour:
 *   The response is:
 *     {"function":"caller","direction":"outbound","mode":"data_flow",
 *      "callees":[{"name":"callee","qualified_name":"...","hop":1}]}
 *   The hop object contains NO "args" and NO "e"/"arg_expr" field.
 *   strstr(resp, "payload_info") returns NULL.
 *
 * Why RED on current code:
 *   The precondition assertion (CALLS edges >= 1) passes -- edge creation
 *   and arg serialisation in pass_parallel.c are correct.  The final
 *   ASSERT_NOT_NULL(strstr(resp, "payload_info")) FAILS because
 *   bfs_to_json_array() never reads or re-emits edge properties_json, so the
 *   arg expression "payload_info" stored in the CALLS edge is permanently
 *   discarded before it reaches the MCP JSON output.
 *
 * Fix location (not implemented here):
 *   cbm_store_bfs() in src/store/store.c must propagate edge properties_json
 *   into the cbm_traverse_result_t (extend cbm_edge_info_t or cbm_node_hop_t).
 *   bfs_to_json_array() in src/mcp/mcp.c must then emit an "args" field when
 *   mode == "data_flow" and the incoming edge has a non-empty args array.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "repro_harness.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/*
 * Fixture: two Python functions in one file.
 *
 *   def callee(x):
 *       return x * 2
 *
 *   def caller():
 *       result = callee(payload_info + 1)
 *       return result
 *
 * caller() passes the compound expression (payload_info + 1) as the first
 * positional argument to callee().  The extractor captures this as a CBMCallArg
 * with .expr == "payload_info + 1" (or a prefix thereof after sanitization).
 * append_args_json serializes it into the CALLS edge as:
 *   {"args":[{"i":0,"e":"payload_info + 1"}]}
 *
 * The expression token "payload_info" is unique enough to identify in the
 * output: strstr(resp, "payload_info") is the assertion anchor.
 *
 * Python is used here because its CALLS extraction (including arg expressions)
 * is proven reliable -- see repro_issue480.c for the same fixture approach.
 */
static const RFile k_files[] = {
    {
        "service.py",
        "def callee(x):\n"
        "    return x * 2\n"
        "\n"
        "def caller():\n"
        "    result = callee(payload_info + 1)\n"
        "    return result\n"
    }
};

/*
 * TEST: repro_issue514_data_flow_surfaces_arg_expr
 *
 * Precondition (must be GREEN to prove this is a data_flow surfacing bug):
 *   rh_count_edges(store, project, "CALLS") >= 1
 *   If this fires RED, the extractor has a regression unrelated to #514.
 *
 * Failing assertion (RED on current code):
 *   strstr(resp, "payload_info") != NULL
 *   i.e. the argument expression text must appear somewhere in the response.
 */
TEST(repro_issue514_data_flow_surfaces_arg_expr) {
    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, k_files,
                                        (int)(sizeof(k_files) / sizeof(k_files[0])));
    ASSERT_NOT_NULL(store);

    /*
     * Precondition: at least one CALLS edge must exist after indexing.
     * If this fires RED the fixture is broken, not data_flow mode.
     * The caller() -> callee(payload_info + 1) call must produce one edge.
     */
    int calls_count = rh_count_edges(store, lp.project, "CALLS");
    fprintf(stderr,
            "  [514] CALLS edges=%d  (expected>=1; 0=extraction regression)\n",
            calls_count);
    ASSERT_GT(calls_count, 0);

    /*
     * Invoke trace_path with mode="data_flow", direction="outbound" on "caller".
     *
     * Args (matching the trace_path JSON schema in mcp.c ~line 355-374):
     *   function_name  -- "caller": the function that passes the argument
     *   project        -- lp.project: derived from the temp dir
     *   direction      -- "outbound": follow callees (caller -> callee)
     *   depth          -- 2: one hop is enough
     *   mode           -- "data_flow": the mode that promises arg expressions
     *
     * Expected response (correct):
     *   {"function":"caller","direction":"outbound","mode":"data_flow",
     *    "callees":[{"name":"callee","qualified_name":"...","hop":1,
     *                "args":[{"i":0,"e":"payload_info + 1"}]}]}
     *   -- or any JSON structure that includes the string "payload_info".
     *
     * Buggy response:
     *   {"function":"caller","direction":"outbound","mode":"data_flow",
     *    "callees":[{"name":"callee","qualified_name":"...","hop":1}]}
     *   -- no "args", no "e", no "payload_info" anywhere.
     */
    char args[512];
    snprintf(args, sizeof(args),
             "{\"function_name\":\"caller\","
             "\"project\":\"%s\","
             "\"direction\":\"outbound\","
             "\"depth\":2,"
             "\"mode\":\"data_flow\"}",
             lp.project);

    char *resp = cbm_mcp_handle_tool(lp.srv, "trace_path", args);
    ASSERT_NOT_NULL(resp);

    fprintf(stderr, "  [514] trace_path data_flow response: %.400s\n", resp);

    /* The response must not be an error -- the node must be found. */
    ASSERT_NULL(strstr(resp, "function not found"));

    /* The response is the MCP tool-result envelope (inner json embedded as an
     * escaped string value), so the "callees" key appears as \"callees\".
     * Match the escaped form (see repro_issue480 / test_incremental's
     * resp_has_key idiom). */
    ASSERT_NOT_NULL(strstr(resp, "\\\"callees\\\""));

    /* The callees array must be non-empty: the callee's QN tail "service.callee"
     * must appear as a hop (unambiguous + escaping-proof). RED if the CALLS
     * traversal is broken (separate from #514). */
    ASSERT_NULL(strstr(resp, "\\\"callees\\\":[]"));
    ASSERT_NOT_NULL(strstr(resp, "service.callee"));

    /*
     * THE CORE ASSERTION FOR BUG #514:
     *
     * The argument expression "payload_info" (part of "payload_info + 1" passed
     * to callee()) must appear in the response JSON when mode="data_flow".
     *
     * WHY RED on current code:
     *   bfs_to_json_array() (mcp.c ~line 2283) only emits cbm_node_hop_t fields
     *   (name, qualified_name, hop).  cbm_edge_info_t (store.h ~line 146) does
     *   not carry properties_json, so the "e":"payload_info + 1" stored in the
     *   CALLS edge never reaches the JSON output.  strstr returns NULL.
     *
     * This assertion is the canonical RED line for bug #514.
     */
    ASSERT_NOT_NULL(strstr(resp, "payload_info"));

    free(resp);
    rh_cleanup(&lp, store);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────────────────── */
SUITE(repro_issue514) {
    RUN_TEST(repro_issue514_data_flow_surfaces_arg_expr);
}
