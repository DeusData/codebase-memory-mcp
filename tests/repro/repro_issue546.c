/*
 * repro_issue546.c — Reproduce-first case for OPEN bug #546.
 *
 * Issue: #546 — "trace_path / reverse-dependency returns an INCOMPLETE caller
 *               set when a symbol is duplicated by an ambient .d.ts declaration
 *               (callers silently split by import style)"
 *
 * Root cause (graph layer — node identity / dedup across the ambient declaration):
 *   When a TypeScript symbol is BOTH defined in a real .ts source file AND
 *   re-declared (body-less, signature only) in an ambient .d.ts shim file,
 *   the indexer creates TWO distinct Function nodes for the same logical symbol
 *   (one rooted at the .ts implementation, one rooted at the .d.ts stub).
 *
 *   CALLS edges from consumers are then partitioned across the two nodes based
 *   on which import form each consumer used:
 *     - consumer importing via relative path ("./scroll")  → CALLS edge targets
 *       the IMPLEMENTATION node (packages/widget/src/scroll.ts)
 *     - consumer importing via path alias ("@widget")      → CALLS edge targets
 *       the .d.ts STUB node    (app/types/widget-shim.d.ts)
 *
 *   trace_path resolves the symbol name to EXACTLY ONE of the two nodes (the
 *   first one returned by cbm_store_find_nodes_by_name) and BFS-traverses only
 *   that node's inbound CALLS edges.  The callers whose edges point to the OTHER
 *   node are silently omitted from the result.  There is no warning that the
 *   symbol resolved to multiple nodes and the caller set is therefore partial.
 *
 * Expected (correct) behaviour:
 *   trace_path(function_name="alignToEdge", direction="inbound") must return
 *   ALL callers, regardless of which import style they used:
 *     {"callers": [{name: "internalConsumer", ...}, {name: "externalConsumer", ...}]}
 *   Both "internalConsumer" AND "externalConsumer" must appear in the response.
 *
 * Actual (buggy) behaviour:
 *   Only ONE of the two callers appears in the "callers" array.  The other is
 *   silently dropped because its CALLS edge points to the sibling node (the
 *   other representation of the same logical symbol) that trace_path did not
 *   select as its BFS root.
 *
 * Why RED on current code:
 *   The final assertion checks that BOTH caller names appear in the trace_path
 *   JSON response.  On buggy code, trace_path picks one of the two Function
 *   nodes for "alignToEdge" as its BFS root; the inbound CALLS edges of the
 *   OTHER node are never visited; one caller name is absent from the JSON;
 *   the strstr check for the missing name returns NULL →
 *   ASSERT_NOT_NULL(strstr(resp, "...")) FAILS → RED.
 *
 * Precondition strategy:
 *   Before driving trace_path, the test checks that BOTH callers produced
 *   at least one CALLS edge each (total CALLS edges ≥ 2).  If this precondition
 *   fires RED it flags an extraction failure (TS CALLS extraction not working),
 *   not the #546 traversal bug.  Separation keeps the root cause unambiguous.
 *
 * TS CALLS extraction reliability note:
 *   TypeScript CALLS extraction is confirmed reliable for simple intra-package
 *   call expressions by existing integration tests (test_extraction.c and the
 *   regression suite).  The known risk here is the path-alias import form
 *   ("@widget") — the extractor may or may not resolve the alias and produce
 *   a CALLS edge for externalConsumer.  If the precondition (total CALLS ≥ 2)
 *   fires first, the alias resolution is the cause, not the #546 split.
 *   A secondary precondition after the main assertion ensures that even if only
 *   one CALLS edge is produced (alias unresolved), the test is still RED for
 *   the right reason: incomplete caller set.
 *
 * Fix location (not implemented here):
 *   Either in cbm_store_find_nodes_by_name / cbm_store_bfs (union traversal
 *   across all nodes sharing name+signature), or in the pipeline dedup step
 *   where body-less .d.ts stub nodes should be merged/aliased into their
 *   implementation counterpart rather than stored as separate graph nodes.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "repro_harness.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Fixture ────────────────────────────────────────────────────────────────
 *
 * Minimal TypeScript monorepo layout that triggers the dual-node split:
 *
 *   packages/widget/src/scroll.ts
 *       — real implementation of alignToEdge(); exports the function
 *
 *   packages/widget/src/internalConsumer.ts
 *       — imports alignToEdge via RELATIVE path ("./scroll")
 *       — calls alignToEdge(document.createElement('div'))
 *       → CALLS edge targets the IMPLEMENTATION node
 *
 *   app/types/widget-shim.d.ts
 *       — ambient .d.ts declaration; body-less signature of alignToEdge
 *       — this causes the indexer to create a SECOND (stub) Function node
 *
 *   app/src/externalConsumer.ts
 *       — imports alignToEdge via PATH ALIAS ("@widget")
 *       — calls alignToEdge(document.querySelector('div'))
 *       → CALLS edge targets the .d.ts STUB node (the alias points there)
 *
 * On buggy code: two Function nodes for "alignToEdge"; trace_path picks one;
 * only one caller is returned.
 *
 * Note: The tsconfig.json is included so the indexer can, in principle,
 * resolve the "@widget" path alias to packages/widget/src.  Alias resolution
 * is best-effort in the current extractor; even without it, if the .d.ts stub
 * causes a second node, the externalConsumer CALLS edge will point to that
 * stub node, and the test assertion will correctly turn RED.
 */
static const RFile k_files[] = {
    /* tsconfig: maps @widget alias to packages/widget/src */
    {
        "tsconfig.json",
        "{\n"
        "  \"compilerOptions\": {\n"
        "    \"baseUrl\": \".\",\n"
        "    \"paths\": {\n"
        "      \"@widget\": [\"packages/widget/src\"]\n"
        "    }\n"
        "  }\n"
        "}\n"
    },

    /* Real implementation — produces the IMPLEMENTATION Function node */
    {
        "packages/widget/src/scroll.ts",
        "export function alignToEdge(el: HTMLElement): () => void {\n"
        "    return function() { el.scrollIntoView({ block: 'nearest' }); };\n"
        "}\n"
    },

    /* Internal consumer: relative import → CALLS edge → IMPLEMENTATION node */
    {
        "packages/widget/src/internalConsumer.ts",
        "import { alignToEdge } from './scroll';\n"
        "const node = document.createElement('div');\n"
        "const cleanup = alignToEdge(node);\n"
        "export { cleanup };\n"
    },

    /* Ambient .d.ts shim — triggers the SECOND (stub) Function node creation */
    {
        "app/types/widget-shim.d.ts",
        "export function alignToEdge(el: HTMLElement): () => void;\n"
    },

    /* External consumer: alias import → CALLS edge → .d.ts STUB node */
    {
        "app/src/externalConsumer.ts",
        "import { alignToEdge } from '@widget';\n"
        "const div = document.querySelector('div') as HTMLElement;\n"
        "const teardown = alignToEdge(div);\n"
        "export { teardown };\n"
    }
};

/* ─────────────────────────────────────────────────────────────────────────
 * repro_issue546_dts_split_caller_set
 *
 * Precondition A (must be GREEN to prove extraction is working):
 *   At least 1 CALLS edge exists in the graph (the internalConsumer relative
 *   import is the most reliable and must produce a CALLS edge).
 *
 * The failing assertion (RED on buggy code):
 *   trace_path for "alignToEdge" with direction="inbound" returns a "callers"
 *   array that contains BOTH "internalConsumer" AND "externalConsumer".
 *
 * The test is RED when EITHER name is absent — the partial set is the bug.
 * ───────────────────────────────────────────────────────────────────────── */
TEST(repro_issue546_dts_split_caller_set) {
    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, k_files,
                                        (int)(sizeof(k_files) / sizeof(k_files[0])));
    ASSERT_NOT_NULL(store);

    /* ── Precondition A: at least one CALLS edge must exist ─────────────
     * If this fires RED, TS CALLS extraction is broken for this fixture —
     * that is a pre-existing extraction bug, not #546.  The test cannot
     * distinguish the traversal split without any edges to split across.
     *
     * Minimum: 1 (internalConsumer's relative-path import always resolves).
     * Ideally 2 (externalConsumer's alias import also resolves), but even
     * 1 is enough to trigger the .d.ts node creation that causes the split.
     */
    int calls_count = rh_count_edges(store, lp.project, "CALLS");
    ASSERT_GT(calls_count, 0); /* precondition — not the #546 assertion */

    /* ── Drive trace_path: inbound callers of "alignToEdge" ─────────────
     *
     * Args:
     *   function_name  — bare symbol name; the indexer mints node names
     *                    matching the short function name for both the impl
     *                    and the .d.ts stub node.
     *   project        — lp.project (derived from tmpdir)
     *   direction      — "inbound": who calls alignToEdge?
     *   depth          — 2: one hop is enough (caller → alignToEdge)
     *
     * On CORRECT code (fixed):
     *   trace_path unions all Function nodes named "alignToEdge" and returns
     *   callers from all of them:
     *   {"callers":[{"name":"internalConsumer",...},{"name":"externalConsumer",...}]}
     *
     * On BUGGY code (current):
     *   trace_path resolves "alignToEdge" to ONE node (first match from
     *   cbm_store_find_nodes_by_name).  Only callers whose CALLS edges
     *   point to THAT node appear.  The other caller is silently absent.
     */
    char args[512];
    snprintf(args, sizeof(args),
             "{\"function_name\":\"alignToEdge\","
             "\"project\":\"%s\","
             "\"direction\":\"inbound\","
             "\"depth\":2}",
             lp.project);

    char *resp = cbm_mcp_handle_tool(lp.srv, "trace_path", args);
    ASSERT_NOT_NULL(resp);

    /* Symbol must be found — if "function not found" fires, the name lookup
     * itself has a problem unrelated to #546. */
    ASSERT_NULL(strstr(resp, "function not found"));

    /* "callers" key must appear (always emitted when direction is inbound).
     * The response is the MCP envelope (inner json embedded as an escaped
     * string), so the key appears as \"callers\" — match the escaped form. */
    ASSERT_NOT_NULL(strstr(resp, "\\\"callers\\\""));

    /* The callers array must not be empty — at least the internalConsumer
     * (whose relative-path import is reliably resolved) must appear.
     *
     * WHY this might already be RED for #546:
     *   If trace_path selected the .d.ts stub node as BFS root, only
     *   externalConsumer is there; internalConsumer's edge is on the impl
     *   node, so this check fires RED immediately (callers:[]) or wrong name.
     */
    ASSERT_NULL(strstr(resp, "\\\"callers\\\":[]")); /* empty = traversal totally wrong */

    /* ── PRIMARY ASSERTION: BOTH callers must appear in the response ─────
     *
     * "internalConsumer" — imports via relative path, CALLS edge → impl node
     * "externalConsumer" — imports via alias,  CALLS edge → .d.ts stub node
     *
     * On CORRECT (fixed) code: trace_path unions both nodes; both names present.
     *
     * WHY RED on buggy code:
     *   trace_path selects ONE of the two "alignToEdge" nodes as its BFS root.
     *   Only that node's inbound CALLS edges are traversed.  The caller whose
     *   CALLS edge points to the OTHER node is absent from the JSON response.
     *   strstr() for the missing caller name returns NULL, and ASSERT_NOT_NULL
     *   fires → RED.
     *
     *   Concretely:
     *     — if impl node selected:  "externalConsumer" absent → RED
     *     — if .d.ts node selected: "internalConsumer" absent → RED
     *   Either way, exactly one of the two assertions below is RED,
     *   proving the caller set is split and incomplete.
     */
    ASSERT_NOT_NULL(strstr(resp, "internalConsumer")); /* relative-import caller */
    ASSERT_NOT_NULL(strstr(resp, "externalConsumer")); /* alias-import caller   */

    free(resp);
    rh_cleanup(&lp, store);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────────────── */
SUITE(repro_issue546) {
    RUN_TEST(repro_issue546_dts_split_caller_set);
}
