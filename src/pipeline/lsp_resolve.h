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

/* Bare last segment of a (possibly qualified) name, splitting on the LAST
 * member/scope separator. C++ textual callees carry `::` (Class::method,
 * Ns::f) and `->` (p->run), while the LSP records dotted internal QNs
 * (Class.method). Splitting only on '.' (strrchr) leaves `Math::square`
 * and `p->run` intact, so they never match the LSP's `square`/`run` short
 * name and the type-aware strategy is silently dropped to the textual
 * registry. Treat '.', ':' and '>' as terminal separators so the bare
 * method name is recovered on BOTH the QN side (dotted, occasionally `::`
 * for template/alias scopes) and the textual side (`.`/`::`/`->`). Other
 * languages' callee names contain none of `::`/`->`, so this is a no-op
 * for them. */
static inline const char *cbm_lsp_bare_segment(const char *name) {
    if (!name) {
        return name;
    }
    const char *seg = name;
    for (const char *p = name; *p; p++) {
        /* '.' (dotted QN / Java-style member) and ':' (C++ `::`, last colon
         * wins) are member/scope separators. '>' is only a separator when it
         * closes the `->` arrow (preceded by '-'); a bare '>' closes a template
         * argument list ("identity<int>") and must NOT split, else the segment
         * would be the empty string after the trailing '>'. */
        if (*p == '.' || *p == ':' || (*p == '>' && p != name && p[-1] == '-')) {
            seg = p + SKIP_ONE;
        }
    }
    return seg;
}

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
        const char *short_name = cbm_lsp_bare_segment(rc->callee_qn);
        /* The call's callee_name is receiver-qualified for method/qualified
         * calls ("c.inc", "A.Helper", "Math::square", "p->run"); the LSP
         * records the resolved class-qualified callee_qn ("Class.inc"). Compare
         * the bare last segment on BOTH sides so method-dispatch resolutions
         * join — the LSP already did the receiver->type resolution, and matching
         * the full "c.inc" against "inc" would always miss, silently dropping the
         * type-aware LSP strategy to the weaker textual registry. Free-function
         * calls (bare callee_name) are unaffected. */
        const char *call_short = cbm_lsp_bare_segment(call->callee_name);
        if (strcmp(short_name, call_short) != 0) {
            /* Indirect/implicit resolution: the textual callee differs from the
             * resolved callee_qn's short name. A function-pointer / DLL call's
             * callee is the pointer name (`fp`); a C++ destructor's only textual
             * anchor is the deleted operand (`p`, vs. the `T.~T` callee QN). In
             * both the LSP stashed the original textual name in `reason`. Match
             * the call site on that name, gated to those strategies so `reason`
             * is never misread as an unresolved-call diagnostic. */
            if (!(rc->reason && rc->strategy &&
                  (strcmp(rc->strategy, "lsp_func_ptr") == 0 ||
                   strcmp(rc->strategy, "lsp_dll_resolve") == 0 ||
                   strcmp(rc->strategy, "lsp_method_ref_ctor") == 0 ||
                   strcmp(rc->strategy, "lsp_method_ref_ctor_synth") == 0 ||
                   strcmp(rc->strategy, "lsp_dict_dispatch") == 0 ||
                   strcmp(rc->strategy, "lsp_destructor") == 0 ||
                   strcmp(rc->strategy, "php_method_dynamic") == 0) &&
                  strcmp(cbm_lsp_bare_segment(rc->reason), call_short) == 0)) {
                continue;
            }
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
