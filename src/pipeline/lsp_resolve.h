/*
 * lsp_resolve.h — Shared LSP-override resolver for the call-edge pipeline.
 *
 * Both pipeline paths (sequential cbm_pipeline_pass_calls and parallel
 * cbm_parallel_extract → resolve_file_calls) need to look up an
 * LSP-resolved call for a given (caller, callee) pair before falling back
 * to the registry's name-based resolver. Before this header existed, each
 * pipeline carried its own copy of that lookup with divergent confidence
 * floors and slightly different match semantics — most production
 * indexing went through the parallel path with a 0.5 floor while the
 * sequential path used 0.6, so the same project produced different
 * CALLS-edge attributions depending on which pipeline mode kicked in.
 *
 * Centralising the lookup here means both pipelines admit exactly the
 * same set of LSP overrides. Each pipeline still owns its own edge
 * emission (sequential uses emit_classified_edge, parallel uses
 * emit_service_edge) — this header only does the matching.
 *
 * Inline-only: no .c file needed.
 */
#ifndef CBM_PIPELINE_LSP_RESOLVE_H
#define CBM_PIPELINE_LSP_RESOLVE_H

#include "cbm.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/constants.h"

#include <stdio.h>
#include <string.h>

/* Confidence floor below which LSP-resolved calls are ignored and the
 * registry resolver is consulted instead. Locked at 0.6 per the v1
 * Python-LSP integration plan; revisit when telemetry justifies a knob.
 * Applies to every language whose LSP populates result->resolved_calls
 * (Go, C/C++, Python, PHP). */
#define CBM_LSP_CONFIDENCE_FLOOR 0.6f

/* Look up the highest-confidence LSP-resolved call entry whose caller QN
 * matches the textual call's enclosing function and whose callee QN
 * short-name matches the textual callee. Returns a pointer into `arr`
 * or NULL if no qualifying entry exists.
 *
 * Match rule: the LSP emits CBMResolvedCall entries whose caller_qn
 * matches the call's enclosing function and whose callee_qn ends with
 * the textual callee_name as the last dot-separated segment. The
 * pointer returned aliases into `arr` and stays valid as long as the
 * underlying CBMFileResult is alive. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_resolution(
    const CBMResolvedCallArray *arr, const CBMCall *call) {
    if (!arr || arr->count == 0 || !call) {
        return NULL;
    }
    if (!call->enclosing_func_qn || !call->callee_name) {
        return NULL;
    }
    const CBMResolvedCall *best = NULL;
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (!rc->caller_qn || !rc->callee_qn) {
            continue;
        }
        if (rc->confidence < CBM_LSP_CONFIDENCE_FLOOR) {
            continue;
        }
        if (strcmp(rc->caller_qn, call->enclosing_func_qn) != 0) {
            continue;
        }
        const char *short_name = strrchr(rc->callee_qn, '.');
        short_name = short_name ? short_name + SKIP_ONE : rc->callee_qn;
        if (strcmp(short_name, call->callee_name) != 0) {
            continue;
        }
        if (!best || rc->confidence > best->confidence) {
            best = rc;
        }
    }
    return best;
}

/* Resolve an LSP-emitted callee_qn to a graph-buffer node.
 *
 * Per-file LSPs (notably py_lsp) sometimes emit `callee_qn` as the raw
 * import-module path the source code uses (e.g. `greeter.Greeter` from
 * `from greeter import Greeter`) rather than the project-qualified QN
 * the gbuf actually stores (`<project>.greeter.Greeter`). This is
 * unavoidable at the per-file LSP layer: the LSP cannot tell in-project
 * imports (qualify) from external imports (don't qualify, e.g. `os.path`)
 * without consulting the gbuf, which is built downstream.
 *
 * The fallback rule: try the LSP-emitted QN as-is first; on miss, retry
 * with `<project>.<callee_qn>`. If that also misses, the target is
 * external/unknown and the caller drops the edge — same as today.
 *
 * Returns the matching node, or NULL if neither lookup hits. */
static inline const cbm_gbuf_node_t *cbm_pipeline_lsp_target_node(const cbm_gbuf_t *gbuf,
                                                                  const char *project_name,
                                                                  const char *callee_qn) {
    if (!gbuf || !callee_qn) {
        return NULL;
    }
    const cbm_gbuf_node_t *direct = cbm_gbuf_find_by_qn(gbuf, callee_qn);
    if (direct) {
        return direct;
    }
    if (!project_name || !project_name[0]) {
        return NULL;
    }
    /* Skip the prefix retry if callee_qn is already project-qualified —
     * avoids producing nonsense like `proj.proj.foo.Bar`. */
    size_t proj_len = strlen(project_name);
    if (strncmp(callee_qn, project_name, proj_len) == 0 && callee_qn[proj_len] == '.') {
        return NULL;
    }
    char buf[CBM_SZ_1K];
    int written = snprintf(buf, sizeof(buf), "%s.%s", project_name, callee_qn);
    if (written < 0 || (size_t)written >= sizeof(buf)) {
        return NULL;
    }
    return cbm_gbuf_find_by_qn(gbuf, buf);
}

#endif /* CBM_PIPELINE_LSP_RESOLVE_H */
