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
#include "foundation/compat.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Confidence floor below which LSP-resolved calls are ignored and the
 * registry resolver is consulted instead. Locked at 0.6 per the v1
 * Python-LSP integration plan; revisit when telemetry justifies a knob.
 * Applies to every language whose LSP populates result->resolved_calls
 * (Go, C/C++, Python, PHP). */
#define CBM_LSP_CONFIDENCE_FLOOR 0.6f
#define CBM_LSP_RESOLUTION_KEY_SEP '|'

/* Bare last segment of a possibly qualified callee. C++ textual calls may use
 * `::` or `->` while LSP QNs use dots; splitting on all member separators keeps
 * sequential and parallel LSP override matching semantically identical. */
static inline const char *cbm_lsp_bare_segment(const char *name) {
    if (!name) {
        return name;
    }
    const char *seg = name;
    for (const char *p = name; *p; p++) {
        if (*p == '.' || *p == ':' || (*p == '>' && p != name && p[-1] == '-')) {
            seg = p + SKIP_ONE;
        }
    }
    return seg;
}

static inline bool cbm_lsp_reason_join_strategy(const char *strategy) {
    return strategy &&
           (strcmp(strategy, "lsp_func_ptr") == 0 ||
            strcmp(strategy, "lsp_dll_resolve") == 0 ||
            strcmp(strategy, "lsp_method_ref_ctor") == 0 ||
            strcmp(strategy, "lsp_method_ref_ctor_synth") == 0 ||
            strcmp(strategy, "lsp_dict_dispatch") == 0 ||
            strcmp(strategy, "lsp_destructor") == 0 ||
            strcmp(strategy, "php_method_dynamic") == 0);
}

static inline bool cbm_lsp_resolution_matches_call(const CBMResolvedCall *rc,
                                                   const CBMCall *call) {
    const char *call_short = cbm_lsp_bare_segment(call->callee_name);
    const char *resolved_short = cbm_lsp_bare_segment(rc->callee_qn);
    if (strcmp(resolved_short, call_short) == 0) {
        return true;
    }
    return rc->reason && cbm_lsp_reason_join_strategy(rc->strategy) &&
           strcmp(cbm_lsp_bare_segment(rc->reason), call_short) == 0;
}

/* Look up the highest-confidence LSP-resolved call entry whose caller QN
 * matches the textual call's enclosing function and whose callee QN
 * short-name matches the textual callee. Returns a pointer into `arr`
 * or NULL if no qualifying entry exists.
 *
 * Match rule: the LSP emits CBMResolvedCall entries whose caller_qn
 * matches the call's enclosing function. The resolved callee QN and textual
 * callee are compared by their last member segment, including C++ `::` and
 * `->` forms; selected indirect strategies may instead match the original
 * textual callee carried in `reason`. The returned pointer aliases into `arr`
 * and stays valid as long as the underlying CBMFileResult is alive. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_resolution_with_floor(
    const CBMResolvedCallArray *arr, const CBMCall *call, double confidence_floor) {
    if (!arr || arr->count == 0 || !call) {
        return NULL;
    }
    if (!call->enclosing_func_qn || !call->callee_name) {
        return NULL;
    }
    double floor =
        confidence_floor > 0.0 ? confidence_floor : (double)CBM_LSP_CONFIDENCE_FLOOR;
    const CBMResolvedCall *best = NULL;
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (!rc->caller_qn || !rc->callee_qn) {
            continue;
        }
        if ((double)rc->confidence < floor) {
            continue;
        }
        if (strcmp(rc->caller_qn, call->enclosing_func_qn) != 0) {
            continue;
        }
        if (!cbm_lsp_resolution_matches_call(rc, call)) {
            continue;
        }
        if (!best || rc->confidence > best->confidence) {
            best = rc;
        }
    }
    return best;
}

static inline const CBMResolvedCall *cbm_pipeline_find_lsp_resolution(
    const CBMResolvedCallArray *arr, const CBMCall *call) {
    return cbm_pipeline_find_lsp_resolution_with_floor(arr, call, 0.0);
}

typedef struct cbm_lsp_resolution_index {
    CBMHashTable *entries;
    bool complete;
} cbm_lsp_resolution_index_t;

static inline void cbm_lsp_resolution_index_free_key(const char *key, void *value, void *ud) {
    (void)value;
    (void)ud;
    free((char *)key);
}

static inline void cbm_lsp_resolution_index_store(cbm_lsp_resolution_index_t *idx,
                                                  const char *caller_qn,
                                                  const char *callee_short,
                                                  CBMResolvedCall *rc) {
    if (!idx || !idx->entries || !caller_qn || !callee_short || !rc) {
        if (idx) {
            idx->complete = false;
        }
        return;
    }
    char key[CBM_SZ_1K];
    int written = snprintf(key, sizeof(key), "%s%c%s", caller_qn,
                           CBM_LSP_RESOLUTION_KEY_SEP, callee_short);
    if (written <= 0 || (size_t)written >= sizeof(key)) {
        idx->complete = false;
        return;
    }

    CBMResolvedCall *existing = (CBMResolvedCall *)cbm_ht_get(idx->entries, key);
    if (!existing) {
        char *owned_key = cbm_strdup(key);
        if (!owned_key) {
            idx->complete = false;
            return;
        }
        cbm_ht_set(idx->entries, owned_key, rc);
    } else if (rc->confidence > existing->confidence) {
        const char *stored_key = cbm_ht_get_key(idx->entries, key);
        if (stored_key) {
            cbm_ht_set(idx->entries, stored_key, rc);
        } else {
            idx->complete = false;
        }
    }
}

/* Build a per-file lookup table keyed by "caller_qn|callee_short".
 *
 * This preserves cbm_pipeline_find_lsp_resolution_with_floor() semantics:
 * it applies the function's confidence_floor argument, requires exact caller_qn
 * equality, compares shared callee segments with C++ `::`/`->` awareness,
 * indexes allowed original-text reason joins, and keeps the highest-confidence
 * entry for duplicate keys. The index changes lookup cost from O(call_count * resolved_count) to
 * O(resolved_count + call_count) for files where every eligible key is indexed.
 *
 * If memory allocation or key formatting fails for any eligible entry,
 * `complete` is cleared. A later miss then falls back to the linear helper so
 * correctness is preserved even when the optimization cannot cover every row. */
static inline void cbm_lsp_resolution_index_build(cbm_lsp_resolution_index_t *idx,
                                                  const CBMResolvedCallArray *arr,
                                                  int call_count,
                                                  double confidence_floor) {
    if (!idx) {
        return;
    }
    idx->entries = NULL;
    idx->complete = false;
    if (!arr || arr->count == 0 || call_count <= 0) {
        return;
    }

    idx->entries = cbm_ht_create((uint32_t)arr->count * 2u + (uint32_t)CBM_SZ_16);
    if (!idx->entries) {
        return;
    }
    idx->complete = true;

    double floor =
        confidence_floor > 0.0 ? confidence_floor : (double)CBM_LSP_CONFIDENCE_FLOOR;
    for (int i = 0; i < arr->count; i++) {
        CBMResolvedCall *rc = &arr->items[i];
        if (!rc->caller_qn || !rc->callee_qn || (double)rc->confidence < floor) {
            continue;
        }
        cbm_lsp_resolution_index_store(idx, rc->caller_qn,
                                       cbm_lsp_bare_segment(rc->callee_qn), rc);
        if (rc->reason && cbm_lsp_reason_join_strategy(rc->strategy)) {
            cbm_lsp_resolution_index_store(idx, rc->caller_qn,
                                           cbm_lsp_bare_segment(rc->reason), rc);
        }
    }
}

static inline const CBMResolvedCall *cbm_lsp_resolution_index_find(
    const cbm_lsp_resolution_index_t *idx, const CBMResolvedCallArray *arr, const CBMCall *call,
    double confidence_floor) {
    if (!call || !call->enclosing_func_qn || !call->callee_name) {
        return NULL;
    }
    if (idx && idx->entries) {
        char key[CBM_SZ_1K];
        int written = snprintf(key, sizeof(key), "%s%c%s", call->enclosing_func_qn,
                               CBM_LSP_RESOLUTION_KEY_SEP,
                               cbm_lsp_bare_segment(call->callee_name));
        if (written > 0 && (size_t)written < sizeof(key)) {
            const CBMResolvedCall *hit = (const CBMResolvedCall *)cbm_ht_get(idx->entries, key);
            if (hit || idx->complete) {
                return hit;
            }
        }
    }
    return cbm_pipeline_find_lsp_resolution_with_floor(arr, call, confidence_floor);
}

static inline void cbm_lsp_resolution_index_free(cbm_lsp_resolution_index_t *idx) {
    if (!idx || !idx->entries) {
        return;
    }
    cbm_ht_foreach(idx->entries, cbm_lsp_resolution_index_free_key, NULL);
    cbm_ht_free(idx->entries);
    idx->entries = NULL;
    idx->complete = false;
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
 * external/unknown and the caller drops the edge, preserving the historical
 * behavior for unresolved LSP targets.
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
