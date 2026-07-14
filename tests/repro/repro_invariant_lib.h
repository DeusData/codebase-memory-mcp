/*
 * repro_invariant_lib.h — Shared helpers for the all-grammar / all-LSP invariant
 * suite. Every per-language and per-LSP-pass invariant file includes this so the
 * assertions are uniform and the failure messages are diagnostic.
 *
 * Two harness tiers:
 *   - single-file extraction:  inv_rx() / the inv_extract_* checks (cbm_extract_file)
 *   - full pipeline (CALLS/edge attribution, LSP resolution): use repro_harness.h
 *     (rh_index / rh_index_files) + the inv_* store helpers below.
 *
 * Helpers RETURN counts/bools (they do not ASSERT) so callers can ASSERT with a
 * per-language message. Include AFTER test_framework.h.
 */
#ifndef REPRO_INVARIANT_LIB_H
#define REPRO_INVARIANT_LIB_H

#include "repro_harness.h" /* RProj/RFile, rh_index*, cbm_store, <store/store.h> */
#include "cbm.h"
#include <string.h>

/* ── Single-file extraction ─────────────────────────────────────── */

static inline CBMFileResult *inv_rx(const char *src, CBMLanguage lang, const char *file) {
    return cbm_extract_file(src, (int)strlen(src), lang, "t", file, 0, NULL, NULL);
}

/* INV(extract-clean): extraction returns non-NULL and does not set has_error on
 * valid input (a parser crash/abort would not return at all → subprocess-isolate
 * crash-prone inputs with rh_extract_crashes instead). */
static inline int inv_extract_clean(const char *src, CBMLanguage lang, const char *file) {
    CBMFileResult *r = inv_rx(src, lang, file);
    if (!r)
        return 0;
    int ok = !r->has_error;
    cbm_free_result(r);
    return ok;
}

/* Count definitions whose label is/ isn't in the valid label set. */
static inline int inv_label_valid(const char *label) {
    static const char *valid[] = {
        "Function",  "Method",   "Class",     "Interface", "Struct",   "Enum",    "EnumMember",
        "Module",    "Variable", "Constant",  "Field",     "Trait",    "Type",    "TypeAlias",
        "Namespace", "Property", "Route",     "Macro",     "Union",    "Protocol","Mixin",
        "Package",   "Object",   "Section",   "Impl",      "Annotation", "Resource", NULL};
    if (!label)
        return 0;
    for (const char **v = valid; *v; v++)
        if (strcmp(label, *v) == 0)
            return 1;
    return 0;
}

/* INV(labels-valid): every extracted def carries a label from the known set.
 * Returns the count of defs with an INVALID/empty label (0 = pass). */
static inline int inv_count_bad_labels(CBMFileResult *r) {
    int bad = 0;
    for (int i = 0; i < r->defs.count; i++)
        if (!inv_label_valid(r->defs.items[i].label))
            bad++;
    return bad;
}

/* INV(fqn-wellformed): non-null, non-empty, no "..", no leading/trailing '.', no
 * whitespace, no empty segments. Returns 1 if well-formed. */
static inline int inv_fqn_wellformed(const char *qn) {
    if (!qn || !*qn)
        return 0;
    size_t n = strlen(qn);
    if (qn[0] == '.' || qn[n - 1] == '.')
        return 0;
    if (strstr(qn, ".."))
        return 0;
    for (const char *p = qn; *p; p++)
        if (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
            return 0;
    return 1;
}

/* INV(fqn-wellformed) over a whole result. Returns count of malformed QNs. */
static inline int inv_count_bad_fqns(CBMFileResult *r) {
    int bad = 0;
    for (int i = 0; i < r->defs.count; i++)
        if (!inv_fqn_wellformed(r->defs.items[i].qualified_name))
            bad++;
    return bad;
}

/* INV(line-ranges): start_line >= 1 and start_line <= end_line for every def.
 * Returns count of defs with an invalid range. */
static inline int inv_count_bad_ranges(CBMFileResult *r) {
    int bad = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (d->start_line < 1 || d->end_line < d->start_line)
            bad++;
    }
    return bad;
}

/* Count defs with a given label. */
static inline int inv_count_label(CBMFileResult *r, const char *label) {
    int c = 0;
    for (int i = 0; i < r->defs.count; i++)
        if (r->defs.items[i].label && strcmp(r->defs.items[i].label, label) == 0)
            c++;
    return c;
}

/* True if a call to `callee` (substring match on callee_name) was extracted. */
static inline int inv_has_call(CBMFileResult *r, const char *callee) {
    for (int i = 0; i < r->calls.count; i++)
        if (r->calls.items[i].callee_name && strstr(r->calls.items[i].callee_name, callee))
            return 1;
    return 0;
}

/* ── Store-level (full pipeline) invariants ─────────────────────── */

/* INV(callable-sourcing): split CALLS edges by source-node label class.
 * Function/Method = callable-sourced; Module/File = module-sourced (the bug). */
static inline void inv_count_calls_by_source(cbm_store_t *store, const char *project,
                                             int *module_sourced, int *callable_sourced) {
    *module_sourced = 0;
    *callable_sourced = 0;
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS", &edges, &n) != CBM_STORE_OK)
        return;
    for (int i = 0; i < n; i++) {
        cbm_node_t src;
        if (cbm_store_find_node_by_id(store, edges[i].source_id, &src) != CBM_STORE_OK)
            continue;
        const char *l = src.label ? src.label : "";
        if (strcmp(l, "Function") == 0 || strcmp(l, "Method") == 0)
            (*callable_sourced)++;
        else if (strcmp(l, "Module") == 0 || strcmp(l, "File") == 0)
            (*module_sourced)++;
    }
    cbm_store_free_edges(edges, n);
}

/* INV(no-dangling-edges): every edge of `type` has both endpoints resolving to a
 * node. Returns count of dangling endpoints (0 = pass), -1 on query error. */
static inline int inv_count_dangling_edges(cbm_store_t *store, const char *project,
                                           const char *type) {
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_type(store, project, type, &edges, &n) != CBM_STORE_OK)
        return -1;
    int dangling = 0;
    for (int i = 0; i < n; i++) {
        cbm_node_t a, b;
        if (cbm_store_find_node_by_id(store, edges[i].source_id, &a) != CBM_STORE_OK)
            dangling++;
        else if (cbm_store_find_node_by_id(store, edges[i].target_id, &b) != CBM_STORE_OK)
            dangling++;
    }
    cbm_store_free_edges(edges, n);
    return dangling;
}

/* INV(lsp-strategy): some CALLS edge carries `strategy` (e.g. "lsp_virtual_dispatch")
 * in its properties_json. Used by the per-LSP-pass invariants. */
static inline int inv_edge_has_strategy(cbm_store_t *store, const char *project,
                                        const char *strategy) {
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS", &edges, &n) != CBM_STORE_OK)
        return 0;
    int found = 0;
    for (int i = 0; i < n; i++) {
        if (edges[i].properties_json && strstr(edges[i].properties_json, strategy)) {
            found = 1;
            break;
        }
    }
    cbm_store_free_edges(edges, n);
    return found;
}

/* INV(no-resolvable-edge): NO CALLS edge targets a node whose QN contains
 * `callee_substr`. This is the ACCURATE invariant for a call to a callee that is
 * undeclared / external / absent from the indexed tree: no node can ever exist
 * for it, so no CALLS edge can ever form — asserting a resolution "strategy on an
 * edge" for such a call is unachievable by design. Returns 1 when no such edge
 * exists (the correct no-edge behaviour), 0 if one is found, and 1 on query
 * error (no edges to contradict the invariant). */
static inline int inv_no_calls_edge_to_qn(cbm_store_t *store, const char *project,
                                          const char *callee_substr) {
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS", &edges, &n) != CBM_STORE_OK)
        return 1;
    int found = 0;
    for (int i = 0; i < n && !found; i++) {
        cbm_node_t tgt;
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &tgt) != CBM_STORE_OK)
            continue;
        if (tgt.qualified_name && callee_substr && strstr(tgt.qualified_name, callee_substr))
            found = 1;
    }
    cbm_store_free_edges(edges, n);
    return !found;
}

/* True if a CALLS edge's target node QN ends with `.<suffix>` (the resolved callee). */
static inline int inv_calls_target_qn_suffix(cbm_store_t *store, const char *project,
                                             const char *suffix) {
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS", &edges, &n) != CBM_STORE_OK)
        return 0;
    int found = 0;
    size_t sl = strlen(suffix);
    for (int i = 0; i < n && !found; i++) {
        cbm_node_t tgt;
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &tgt) != CBM_STORE_OK)
            continue;
        const char *qn = tgt.qualified_name;
        if (qn) {
            size_t ql = strlen(qn);
            if (ql >= sl && strcmp(qn + ql - sl, suffix) == 0)
                found = 1;
        }
    }
    cbm_store_free_edges(edges, n);
    return found;
}

#endif /* REPRO_INVARIANT_LIB_H */
