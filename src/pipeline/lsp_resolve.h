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

static inline bool cbm_lsp_reason_join_strategy(const char *strategy) {
    return strategy &&
           (strcmp(strategy, "lsp_func_ptr") == 0 ||
            strcmp(strategy, "lsp_dll_resolve") == 0 ||
            strcmp(strategy, "lsp_method_ref_ctor") == 0 ||
            strcmp(strategy, "lsp_method_ref_ctor_synth") == 0 ||
            strcmp(strategy, "lsp_dict_dispatch") == 0 ||
            strcmp(strategy, "lsp_import_alias") == 0 ||
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

/* Tail helper: return the start of the final two dot-separated segments
 * ("Class.method") or NULL when the QN is too short. */
static inline const char *cbm_pipeline_qn_class_method_tail(const char *qn) {
    if (!qn) {
        return NULL;
    }
    const char *last = strrchr(qn, '.');
    if (!last || last == qn) {
        return NULL;
    }
    const char *second = last;
    while (second > qn) {
        second--;
        if (*second == '.') {
            if (second == qn) {
                return qn;
            }
            return second + 1;
        }
    }
    return qn;
}

static inline const char *cbm_pipeline_call_callee_leaf(const char *callee_name) {
    return cbm_lsp_bare_segment(callee_name);
}

/* Gate for the unique-`Class.method`-tail fallbacks below. Tail-matching by
 * leaf is safe where class-per-file package semantics hold — the JVM
 * languages (Java/Kotlin): the declared `package` is ground truth, a class
 * name is unique within a package, and mixed Gradle/Maven source roots
 * (`src/main/java` + `src/main/kotlin`) legitimately produce path-derived
 * module QNs that disagree with the package-shaped QNs the LSP emits, so
 * the tail is the only reliable join key. In other languages the same-name
 * guarantee does not exist (Python/TS re-export shims, Go internal clones,
 * C++ template instantiations), and a single wrong-module coincidence
 * would fabricate a CALLS edge — so the fallbacks stay off there. */
static inline bool cbm_pipeline_lsp_allow_tail_match(CBMLanguage lang) {
    return lang == CBM_LANG_JAVA || lang == CBM_LANG_KOTLIN;
}

static inline int cbm_pipeline_qn_class_method_tail_eq(const char *qn, const char *tail) {
    const char *qt = cbm_pipeline_qn_class_method_tail(qn);
    return qt && tail && strcmp(qt, tail) == 0;
}

/* Look up the highest-confidence LSP-resolved call entry whose caller QN
 * matches the textual call's enclosing function and whose callee QN
 * short-name matches the textual callee. Returns a pointer into `arr`
 * or NULL if no qualifying entry exists.
 *
 * Exact matches compare the shared bare member segment, including C++ `::`
 * and `->` forms and selected indirect strategies whose original textual
 * callee is carried in `reason`. When explicitly enabled for JVM languages,
 * a unique Class.method caller tail may match; ambiguous tails fail closed.
 * The returned pointer aliases into `arr`. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_resolution_with_floor(
    const CBMResolvedCallArray *arr, const CBMCall *call, double confidence_floor,
    bool allow_tail_match) {
    if (!arr || arr->count == 0 || !call) {
        return NULL;
    }
    if (!call->enclosing_func_qn || !call->callee_name) {
        return NULL;
    }
    double floor =
        confidence_floor > 0.0 ? confidence_floor : (double)CBM_LSP_CONFIDENCE_FLOOR;
    const CBMResolvedCall *best_exact = NULL;
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
        if (!best_exact || rc->confidence > best_exact->confidence) {
            best_exact = rc;
        }
    }
    if (best_exact) {
        return best_exact;
    }
    if (!allow_tail_match) {
        return NULL;
    }

    const char *call_tail = cbm_pipeline_qn_class_method_tail(call->enclosing_func_qn);
    if (!call_tail) {
        return NULL;
    }

    const CBMResolvedCall *best_tail = NULL;
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (!rc->caller_qn || !rc->callee_qn) {
            continue;
        }
        if ((double)rc->confidence < floor) {
            continue;
        }
        const char *short_name = strrchr(rc->callee_qn, '.');
        short_name = short_name ? short_name + SKIP_ONE : rc->callee_qn;
        const char *call_leaf = cbm_pipeline_call_callee_leaf(call->callee_name);
        if (!call_leaf || strcmp(short_name, call_leaf) != 0) {
            continue;
        }
        if (!cbm_pipeline_qn_class_method_tail_eq(rc->caller_qn, call_tail)) {
            continue;
        }
        if (best_tail) {
            return NULL;
        }
        best_tail = rc;
    }
    return best_tail;
}

static inline const CBMResolvedCall *cbm_pipeline_find_lsp_resolution(
    const CBMResolvedCallArray *arr, const CBMCall *call, bool allow_tail_match) {
    return cbm_pipeline_find_lsp_resolution_with_floor(arr, call, 0.0, allow_tail_match);
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
    double confidence_floor, bool allow_tail_match) {
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
            if (hit || (idx->complete && !allow_tail_match)) {
                return hit;
            }
        }
    }
    return cbm_pipeline_find_lsp_resolution_with_floor(arr, call, confidence_floor,
                                                       allow_tail_match);
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
 * Per-file LSPs sometimes emit `callee_qn` as the raw package-shaped
 * import path the source code uses rather than the project-qualified QN
 * the gbuf actually stores. The fallback rule is:
 *   1. try the LSP-emitted QN as-is;
 *   2. retry with `<project>.<callee_qn>` when needed;
 *   3. if both fail AND allow_tail_match is set (JVM callers only, see
 *      cbm_pipeline_lsp_allow_tail_match), use the exact node-name index
 *      to narrow candidates by short method name and accept exactly one
 *      Function/Method whose qualified_name has the same Class.method
 *      tail.
 *
 * Returns the matching node, or NULL if neither lookup hits. */
static inline const cbm_gbuf_node_t *cbm_pipeline_lsp_target_node(const cbm_gbuf_t *gbuf,
                                                                  const char *project_name,
                                                                  const char *callee_qn,
                                                                  bool allow_tail_match) {
    if (!gbuf || !callee_qn) {
        return NULL;
    }
    const cbm_gbuf_node_t *direct = cbm_gbuf_find_by_qn(gbuf, callee_qn);
    if (direct) {
        return direct;
    }
    if (project_name && project_name[0]) {
        size_t proj_len = strlen(project_name);
        if (!(strncmp(callee_qn, project_name, proj_len) == 0 && callee_qn[proj_len] == '.')) {
            char buf[CBM_SZ_1K];
            int written = snprintf(buf, sizeof(buf), "%s.%s", project_name, callee_qn);
            if (written > 0 && (size_t)written < sizeof(buf)) {
                const cbm_gbuf_node_t *prefixed = cbm_gbuf_find_by_qn(gbuf, buf);
                if (prefixed) {
                    return prefixed;
                }
            }
        }
    }
    if (!allow_tail_match) {
        return NULL;
    }

    const char *short_name = strrchr(callee_qn, '.');
    short_name = short_name ? short_name + SKIP_ONE : callee_qn;
    const char *callee_tail = cbm_pipeline_qn_class_method_tail(callee_qn);
    if (!callee_tail) {
        return NULL;
    }
    const cbm_gbuf_node_t **hits = NULL;
    int hit_count = 0;
    if (cbm_gbuf_find_by_name(gbuf, short_name, &hits, &hit_count) != 0 || hit_count == 0) {
        return NULL;
    }

    const cbm_gbuf_node_t *match = NULL;
    for (int i = 0; i < hit_count; i++) {
        const cbm_gbuf_node_t *cand = hits[i];
        if (!cand || !cand->label || !cand->qualified_name) {
            continue;
        }
        if (strcmp(cand->label, "Function") != 0 && strcmp(cand->label, "Method") != 0) {
            continue;
        }
        if (!cbm_pipeline_qn_class_method_tail_eq(cand->qualified_name, callee_tail)) {
            continue;
        }
        if (match) {
            return NULL;
        }
        match = cand;
    }
    return match;
}

/* Whether an unresolved LSP target is explicitly inside the indexed project.
 *
 * Cross-file registries can resolve a declaration to a project-qualified QN
 * that is not itself a graph node. C/C++ forward declarations are the common
 * case: the declaration QN belongs to the caller translation unit while the
 * canonical definition node belongs to another file. In that case the textual
 * registry resolver remains a valid canonical-definition fallback.
 *
 * Unqualified targets are deliberately not assumed to be internal. Python and
 * other language resolvers use those QNs for third-party libraries (for example
 * starlette.routing.Route), where a weak short-name fallback would manufacture
 * a project edge. Explicit `external.*` targets are also excluded naturally.
 */
static inline bool cbm_lsp_resolution_targets_project(const CBMResolvedCall *resolution,
                                                      const char *project_name) {
    if (!resolution || !resolution->callee_qn || !project_name || !project_name[0]) {
        return false;
    }
    size_t project_len = strlen(project_name);
    return strncmp(resolution->callee_qn, project_name, project_len) == 0 &&
           resolution->callee_qn[project_len] == '.';
}

#endif /* CBM_PIPELINE_LSP_RESOLVE_H */
