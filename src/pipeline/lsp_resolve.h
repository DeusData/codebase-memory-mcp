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

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Confidence floor below which LSP-resolved calls are ignored and the
 * registry resolver is consulted instead. Locked at 0.6 per the v1
 * Python-LSP integration plan; revisit when telemetry justifies a knob.
 * Applies to every language whose LSP populates result->resolved_calls
 * (Go, C/C++, Python, PHP). */
#define CBM_LSP_CONFIDENCE_FLOOR 0.6f

#if defined(CBM_CALL_REFERENCE_LOOKUP_TEST_API) && CBM_CALL_REFERENCE_LOOKUP_TEST_API
/* Test-build-only operation counter implemented by pass_usages.c. Keeping the
 * increment behind this seam lets the regression measure the real sequential
 * and fused-parallel materialization helper without retaining instrumentation
 * in production builds. */
void cbm_pipeline_lsp_reference_lookup_test_note_row(void);
#else
static inline void cbm_pipeline_lsp_reference_lookup_test_note_row(void) {}
#endif

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
    while (*seg && isspace((unsigned char)*seg)) {
        seg++;
    }
    return seg;
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

/* When a JVM callable-reference occurrence has no exact semantic target, its
 * ordinary-USAGE fallback must still respect package/import reachability.
 * `unique_name` and `suffix_match` are project-wide guesses: admitting either
 * would bind an unresolved `::handler` in one package to an unrelated callable
 * in another. Other languages retain their legacy value-usage fallback. */
static inline bool cbm_pipeline_call_reference_usage_fallback_allowed(CBMLanguage lang,
                                                                      const char *strategy) {
    if (lang != CBM_LANG_JAVA && lang != CBM_LANG_KOTLIN) {
        return true;
    }
    if (!strategy || !strategy[0]) {
        return false;
    }
    return strcmp(strategy, "unique_name") != 0 && strcmp(strategy, "suffix_match") != 0;
}

static inline bool cbm_pipeline_reference_candidate_fallback_allowed(
    CBMLanguage lang, const CBMUsage *usage, const char *strategy, const char *const *import_names,
    int import_count) {
    if (!usage) {
        return false;
    }
    bool reference_candidate =
        usage->kind == CBM_USAGE_CALL_REFERENCE ||
        (lang == CBM_LANG_KOTLIN && usage->kind == CBM_USAGE_VALUE && usage->may_be_call_reference);
    if (!reference_candidate ||
        cbm_pipeline_call_reference_usage_fallback_allowed(lang, strategy)) {
        return true;
    }
    /* Kotlin's project import map currently names the imported module rather
     * than the member. A syntactically explicit, locally imported `::name`
     * may therefore resolve by unique name after LSP proves it is not a
     * callable. Admit that ordinary USAGE only when the import map confirms
     * the local spelling; an unimported same-name symbol remains unreachable. */
    if (lang == CBM_LANG_KOTLIN && usage->ref_name && import_names) {
        for (int i = 0; i < import_count; i++) {
            if (import_names[i] && strcmp(import_names[i], usage->ref_name) == 0) {
                return true;
            }
        }
    }
    return false;
}

/* Explicit reference syntax always participates in the semantic join. An
 * ordinary value participates only when extraction marked the narrow direct
 * argument shape in a language whose resolver can prove callable identity;
 * without an exact LSP record it remains USAGE. */
static inline bool cbm_pipeline_usage_semantic_reference_candidate(const CBMUsage *usage) {
    if (!usage) {
        return false;
    }
    return usage->kind == CBM_USAGE_CALL_REFERENCE ||
           (usage->kind == CBM_USAGE_VALUE && usage->may_be_call_reference);
}

/* Lexical extraction has whole-scope information that a source-order semantic
 * walk can lack. In Python and JS/TS, a later local binding makes an earlier
 * same-named value local too; an LSP row that still points at the module
 * function must not override that proof. A callable-alias row is different:
 * it explicitly resolves the local binding itself to its assigned callable. */
static inline bool cbm_pipeline_usage_allows_semantic_reference(const CBMUsage *usage,
                                                                const CBMResolvedCall *resolved) {
    if (!usage || !resolved) {
        return false;
    }
    if (usage->kind != CBM_USAGE_VALUE || !usage->semantic_reference_blocked ||
        !usage->semantic_reference_local_shadow) {
        return true;
    }
    return resolved->strategy && strcmp(resolved->strategy, "lsp_callable_alias") == 0;
}

/* A blocked module value can still retain an ordinary edge to a same-named
 * non-callable declaration, but it must never fall back to an executable
 * declaration. A local lexical shadow rejects every raw-name fallback because
 * even a same-named module variable is a different binding. Exact semantic
 * rows are joined before either rule. This label check keeps sequential and
 * fused resolution aligned. */
static inline bool cbm_pipeline_node_is_callable_target(const cbm_gbuf_node_t *node) {
    if (!node || !node->label) {
        return false;
    }
    return strcmp(node->label, "Function") == 0 || strcmp(node->label, "Method") == 0 ||
           strcmp(node->label, "Constructor") == 0 || strcmp(node->label, "Class") == 0;
}

static inline int cbm_pipeline_qn_class_method_tail_eq(const char *qn, const char *tail) {
    const char *qt = cbm_pipeline_qn_class_method_tail(qn);
    return qt && tail && strcmp(qt, tail) == 0;
}

static inline const cbm_gbuf_node_t *cbm_pipeline_lsp_target_node_policy(
    const cbm_gbuf_t *gbuf, const char *project_name, const char *callee_qn, bool allow_tail_match,
    bool allow_unique_callable_fallback);

/* Local and cross passes may spell one exact invocation target with and without
 * the project prefix. Collapse that cross-language case only when exact graph
 * lookup maps both spellings to one materialized node. JVM passes additionally
 * reconcile file/project and declared-package QNs below; Class.method-tail
 * equality alone is insufficient because two packages can declare the same
 * class and method. */
static inline bool cbm_pipeline_invocation_targets_equal(const char *left_qn, const char *right_qn,
                                                         const cbm_gbuf_t *gbuf,
                                                         const char *project_name,
                                                         bool allow_tail_match) {
    if (!left_qn || !right_qn) {
        return false;
    }
    if (strcmp(left_qn, right_qn) == 0) {
        return true;
    }
    if (!gbuf || !project_name) {
        return false;
    }
    const cbm_gbuf_node_t *left_exact =
        cbm_pipeline_lsp_target_node_policy(gbuf, project_name, left_qn, false, false);
    const cbm_gbuf_node_t *right_exact =
        cbm_pipeline_lsp_target_node_policy(gbuf, project_name, right_qn, false, false);
    if (left_exact && right_exact) {
        return left_exact->id == right_exact->id;
    }
    if (!allow_tail_match) {
        return false;
    }
    const char *left_tail = cbm_pipeline_qn_class_method_tail(left_qn);
    const char *right_tail = cbm_pipeline_qn_class_method_tail(right_qn);
    if (!left_tail || !right_tail || strcmp(left_tail, right_tail) != 0) {
        return false;
    }
    /* Two package-shaped rows that both reach a node only through the tail
     * fallback are still distinct semantic claims. Require at least one row
     * to name the materialized node exactly (or by project-prefix retry). */
    if (!left_exact && !right_exact) {
        return false;
    }
    const cbm_gbuf_node_t *left =
        left_exact ? left_exact
                   : cbm_pipeline_lsp_target_node_policy(gbuf, project_name, left_qn, true, true);
    const cbm_gbuf_node_t *right =
        right_exact ? right_exact
                    : cbm_pipeline_lsp_target_node_policy(gbuf, project_name, right_qn, true, true);
    return left && right && left->id == right->id;
}

/* A semantic join is occurrence-exact only when both producers recorded the
 * same non-empty source span. Byte spans, unlike line numbers, distinguish
 * repeated same-named references/operators on one line. */
static inline bool cbm_pipeline_source_site_present(uint32_t start, uint32_t end) {
    return end > start;
}

static inline bool cbm_pipeline_source_site_legacy(uint32_t start, uint32_t end) {
    return start == 0 && end == 0;
}

static inline bool cbm_pipeline_source_site_eq(uint32_t lhs_start, uint32_t lhs_end,
                                               uint32_t rhs_start, uint32_t rhs_end) {
    return cbm_pipeline_source_site_present(lhs_start, lhs_end) &&
           cbm_pipeline_source_site_present(rhs_start, rhs_end) && lhs_start == rhs_start &&
           lhs_end == rhs_end;
}

static inline bool cbm_pipeline_source_occurrence_eq(uint32_t lhs_start, uint32_t lhs_end,
                                                     CBMSourceOrigin lhs_origin, uint32_t rhs_start,
                                                     uint32_t rhs_end, CBMSourceOrigin rhs_origin) {
    return lhs_origin == rhs_origin &&
           cbm_pipeline_source_site_eq(lhs_start, lhs_end, rhs_start, rhs_end);
}

/* Rank an invocation record for one parser carrier. Exact occurrence identity
 * is stronger than the legacy 0:0 compatibility join. Partial/reversed spans
 * are corrupt metadata and never participate in either class. */
static inline int cbm_pipeline_invocation_site_rank(const CBMResolvedCall *resolved,
                                                    const CBMCall *call) {
    if (!resolved || !call) {
        return 0;
    }
    bool call_has_site =
        cbm_pipeline_source_site_present(call->site_start_byte, call->site_end_byte);
    bool call_is_legacy =
        cbm_pipeline_source_site_legacy(call->site_start_byte, call->site_end_byte);
    if (!call_has_site && !call_is_legacy) {
        return 0;
    }
    if (cbm_pipeline_source_site_present(resolved->site_start_byte, resolved->site_end_byte)) {
        return cbm_pipeline_source_occurrence_eq(call->site_start_byte, call->site_end_byte,
                                                 call->source_origin, resolved->site_start_byte,
                                                 resolved->site_end_byte, resolved->source_origin)
                   ? 2
                   : 0;
    }
    if (!cbm_pipeline_source_site_legacy(resolved->site_start_byte, resolved->site_end_byte)) {
        return 0;
    }
    return call->requires_lsp_resolution || call->source_origin != resolved->source_origin ? 0 : 1;
}

static inline bool cbm_pipeline_invocation_reason_join_strategy(const char *strategy) {
    return strategy &&
           (strcmp(strategy, "lsp_func_ptr") == 0 || strcmp(strategy, "lsp_callable_alias") == 0 ||
            strcmp(strategy, "lsp_dll_resolve") == 0 ||
            strcmp(strategy, "lsp_method_ref_ctor") == 0 ||
            strcmp(strategy, "lsp_method_ref_ctor_synth") == 0 ||
            strcmp(strategy, "lsp_dict_dispatch") == 0 ||
            strcmp(strategy, "lsp_import_alias") == 0 ||
            strcmp(strategy, "php_method_dynamic") == 0);
}

static inline bool cbm_pipeline_invocation_leaf_matches(const CBMResolvedCall *resolved,
                                                        const CBMCall *call, int site_rank) {
    if (!resolved || !resolved->callee_qn || !call || !call->callee_name) {
        return false;
    }
    const char *resolved_leaf = cbm_lsp_bare_segment(resolved->callee_qn);
    const char *call_leaf = cbm_lsp_bare_segment(call->callee_name);
    if (resolved_leaf && call_leaf && strcmp(resolved_leaf, call_leaf) == 0) {
        return true;
    }

    /* Destructors intentionally join by their exact delete-expression
     * occurrence because the parser carrier starts as a neutral "~" marker. */
    if (site_rank == 2 && call->requires_lsp_resolution && resolved->strategy &&
        strcmp(resolved->strategy, "lsp_destructor") == 0) {
        return true;
    }

    /* Indirect resolvers retain the textual source leaf in reason. */
    return cbm_pipeline_invocation_reason_join_strategy(resolved->strategy) && resolved->reason &&
           call_leaf && strcmp(cbm_lsp_bare_segment(resolved->reason), call_leaf) == 0;
}

/* Look up the highest-confidence LSP-resolved call entry whose caller QN
 * matches the textual call's enclosing function and whose callee QN
 * short-name matches the textual callee. Returns a pointer into `arr`
 * or NULL if no qualifying entry exists.
 *
 * Match rule:
 *   1. exact caller_qn + callee short-name match wins first;
 *   2. if no exact caller match exists AND allow_tail_match is set
 *      (JVM callers only, see cbm_pipeline_lsp_allow_tail_match), a
 *      unique Class.method tail match may win; an occurrence-exact row may
 *      also reconcile Kotlin's package-vs-file top-level owner QNs when their
 *      bare caller leaves agree;
 *   3. ambiguous tails return NULL so the registry fallback stays in
 *      control.
 *
 * Qualified static callees (e.g. Perl `Pkg::sub`) are reduced to their
 * bare last segment by cbm_lsp_bare_segment before matching.
 *
 * The pointer returned aliases into `arr` and stays valid as long as the
 * underlying CBMFileResult is alive. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_resolution_in_graph(
    const CBMResolvedCallArray *arr, const CBMCall *call, bool allow_tail_match,
    const cbm_gbuf_t *gbuf, const char *project_name) {
    if (!arr || arr->count == 0 || !call) {
        return NULL;
    }
    if (!call->enclosing_func_qn || !call->callee_name) {
        return NULL;
    }

    const CBMResolvedCall *best_exact_caller = NULL;
    int best_exact_caller_rank = 0;
    bool exact_caller_ambiguous = false;
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->kind != CBM_RESOLVED_INVOCATION) {
            continue;
        }
        if (!rc->caller_qn || !rc->callee_qn) {
            continue;
        }
        if (rc->confidence < CBM_LSP_CONFIDENCE_FLOOR) {
            continue;
        }
        if (strcmp(rc->caller_qn, call->enclosing_func_qn) != 0) {
            continue;
        }
        int site_rank = cbm_pipeline_invocation_site_rank(rc, call);
        if (site_rank == 0 || !cbm_pipeline_invocation_leaf_matches(rc, call, site_rank)) {
            continue;
        }
        if (!best_exact_caller || site_rank > best_exact_caller_rank) {
            best_exact_caller = rc;
            best_exact_caller_rank = site_rank;
            exact_caller_ambiguous = false;
            continue;
        }
        if (site_rank < best_exact_caller_rank) {
            continue;
        }
        if (!cbm_pipeline_invocation_targets_equal(best_exact_caller->callee_qn, rc->callee_qn,
                                                   gbuf, project_name, allow_tail_match)) {
            exact_caller_ambiguous = true;
            continue;
        }
        if (rc->confidence > best_exact_caller->confidence) {
            best_exact_caller = rc;
        }
    }
    if (best_exact_caller) {
        return exact_caller_ambiguous ? NULL : best_exact_caller;
    }
    if (!allow_tail_match) {
        return NULL;
    }

    const char *call_tail = cbm_pipeline_qn_class_method_tail(call->enclosing_func_qn);

    const CBMResolvedCall *best_tail_exact = NULL;
    const CBMResolvedCall *best_tail_legacy = NULL;
    bool tail_exact_ambiguous = false;
    bool tail_legacy_ambiguous = false;
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->kind != CBM_RESOLVED_INVOCATION) {
            continue;
        }
        if (!rc->caller_qn || !rc->callee_qn) {
            continue;
        }
        if (rc->confidence < CBM_LSP_CONFIDENCE_FLOOR) {
            continue;
        }
        int site_rank = cbm_pipeline_invocation_site_rank(rc, call);
        if (site_rank == 0) {
            continue;
        }
        bool caller_matches =
            call_tail && cbm_pipeline_qn_class_method_tail_eq(rc->caller_qn, call_tail);
        if (!caller_matches && site_rank == 2) {
            const char *resolved_caller = cbm_lsp_bare_segment(rc->caller_qn);
            const char *source_caller = cbm_lsp_bare_segment(call->enclosing_func_qn);
            caller_matches =
                resolved_caller && source_caller && strcmp(resolved_caller, source_caller) == 0;
        }
        if (!caller_matches || !cbm_pipeline_invocation_leaf_matches(rc, call, site_rank)) {
            continue;
        }

        const CBMResolvedCall **best = site_rank == 2 ? &best_tail_exact : &best_tail_legacy;
        bool *ambiguous = site_rank == 2 ? &tail_exact_ambiguous : &tail_legacy_ambiguous;
        if (!*best) {
            *best = rc;
            continue;
        }
        if (!cbm_pipeline_invocation_targets_equal((*best)->callee_qn, rc->callee_qn, gbuf,
                                                   project_name, allow_tail_match)) {
            *ambiguous = true;
            continue;
        }
        if (rc->confidence > (*best)->confidence) {
            *best = rc;
        }
    }
    if (best_tail_exact) {
        return tail_exact_ambiguous ? NULL : best_tail_exact;
    }
    return tail_legacy_ambiguous ? NULL : best_tail_legacy;
}

/* Graph-free compatibility entry point for resolver unit tests. Raw distinct
 * targets remain ambiguous because no materialized-node identity is available. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_resolution(
    const CBMResolvedCallArray *arr, const CBMCall *call, bool allow_tail_match) {
    return cbm_pipeline_find_lsp_resolution_in_graph(arr, call, allow_tail_match, NULL, NULL);
}

static inline bool cbm_pipeline_reference_reason_matches(const CBMResolvedCall *resolved,
                                                         const char *reference_leaf) {
    if (!resolved || !resolved->strategy || !resolved->reason || !reference_leaf) {
        return false;
    }
    bool reason_join_strategy = strcmp(resolved->strategy, "php_method_dynamic") == 0 ||
                                strcmp(resolved->strategy, "php_static_magic_reference") == 0 ||
                                strcmp(resolved->strategy, "php_invokable_reference") == 0 ||
                                strcmp(resolved->strategy, "lsp_callable_alias") == 0 ||
                                strcmp(resolved->strategy, "lsp_callable_value") == 0 ||
                                strcmp(resolved->strategy, "lsp_callable_value_reference") == 0;
    return reason_join_strategy &&
           strcmp(cbm_lsp_bare_segment(resolved->reason), reference_leaf) == 0;
}

static inline const cbm_gbuf_node_t *cbm_pipeline_lsp_target_node(const cbm_gbuf_t *gbuf,
                                                                  const char *project_name,
                                                                  const char *callee_qn,
                                                                  bool allow_tail_match) {
    return cbm_pipeline_lsp_target_node_policy(gbuf, project_name, callee_qn, allow_tail_match,
                                               true);
}

/* External Kotlin protocol carriers must resolve to the exact semantic target.
 * A project-qualified retry is still exact, but Class.method and unique-leaf
 * fallbacks could bind `kotlin.IntArray.iterator` to an unrelated project
 * method and are therefore deliberately disabled. */
static inline const cbm_gbuf_node_t *cbm_pipeline_lsp_target_node_strict(const cbm_gbuf_t *gbuf,
                                                                         const char *project_name,
                                                                         const char *callee_qn,
                                                                         bool allow_tail_match) {
    (void)allow_tail_match;
    return cbm_pipeline_lsp_target_node_policy(gbuf, project_name, callee_qn, false, false);
}

/* Keep exact-only lookup scoped to known external Kotlin namespaces. Project
 * callable aliases and operators still need the normal JVM QN reconciliation
 * path, while unresolved synthetic carriers remain fail-closed at the caller. */
static inline bool cbm_pipeline_kotlin_external_target(CBMLanguage lang, const char *callee_qn) {
    if (lang != CBM_LANG_KOTLIN || !callee_qn) {
        return false;
    }
    return strncmp(callee_qn, "kotlin.", strlen("kotlin.")) == 0 ||
           strncmp(callee_qn, "java.", strlen("java.")) == 0 ||
           strncmp(callee_qn, "javax.", strlen("javax.")) == 0;
}

/* Resolvers may report one graph target both project-relative and already
 * project-prefixed. Raw string inequality is not ambiguity when both spellings
 * resolve to the same materialized node; conversely, if both nodes exist they
 * remain distinct and the reference must fail closed. */
static inline bool cbm_pipeline_reference_targets_equal(const char *left_qn, const char *right_qn,
                                                        const cbm_gbuf_t *gbuf,
                                                        const char *project_name,
                                                        bool allow_tail_match) {
    if (!left_qn || !right_qn) {
        return false;
    }
    if (strcmp(left_qn, right_qn) == 0) {
        return true;
    }
    if (!gbuf || !project_name) {
        return false;
    }
    /* Caller-tail fallback helps JVM rows find a materialized target, but it is
     * not proof that two distinct semantic target QNs denote the same symbol.
     * Equivalence is deliberately stricter: collapse only direct and
     * project-prefix spellings of one graph node, otherwise stay ambiguous. */
    (void)allow_tail_match;
    const cbm_gbuf_node_t *left = cbm_pipeline_lsp_target_node(gbuf, project_name, left_qn, false);
    const cbm_gbuf_node_t *right =
        cbm_pipeline_lsp_target_node(gbuf, project_name, right_qn, false);
    return left && right && left->id == right->id;
}

typedef struct {
    const CBMResolvedCall *row;
    int original_index;
} cbm_pipeline_lsp_reference_index_entry_t;

typedef struct {
    cbm_pipeline_lsp_reference_index_entry_t *entries;
    int count;
} cbm_pipeline_lsp_reference_index_t;

/* Sort by exact occurrence while retaining extraction order within one site.
 * The final ordinal key is intentional: confidence ties keep the first row,
 * so an unstable qsort tie must not silently change semantic selection. */
static inline int cbm_pipeline_lsp_reference_index_entry_cmp(const void *left_ptr,
                                                             const void *right_ptr) {
    const cbm_pipeline_lsp_reference_index_entry_t *left =
        (const cbm_pipeline_lsp_reference_index_entry_t *)left_ptr;
    const cbm_pipeline_lsp_reference_index_entry_t *right =
        (const cbm_pipeline_lsp_reference_index_entry_t *)right_ptr;
    if (left->row->source_origin != right->row->source_origin) {
        return left->row->source_origin < right->row->source_origin ? -1 : 1;
    }
    if (left->row->site_start_byte != right->row->site_start_byte) {
        return left->row->site_start_byte < right->row->site_start_byte ? -1 : 1;
    }
    if (left->row->site_end_byte != right->row->site_end_byte) {
        return left->row->site_end_byte < right->row->site_end_byte ? -1 : 1;
    }
    if (left->original_index == right->original_index) {
        return 0;
    }
    return left->original_index < right->original_index ? -1 : 1;
}

/* Build one compact pointer index per extracted file. Invocation rows cannot
 * participate in CALL_REFERENCE matching and are excluded. False means only
 * allocation failure; callers must then use the unchanged linear matcher. */
static inline bool cbm_pipeline_lsp_reference_index_build(
    const CBMResolvedCallArray *arr, cbm_pipeline_lsp_reference_index_t *index) {
    if (!index) {
        return false;
    }
    index->entries = NULL;
    index->count = 0;
    if (!arr || arr->count <= 0) {
        return true;
    }

    int reference_count = 0;
    for (int i = 0; i < arr->count; i++) {
        if (arr->items[i].kind == CBM_RESOLVED_CALL_REFERENCE) {
            reference_count++;
        }
    }
    if (reference_count == 0) {
        return true;
    }
    if ((size_t)reference_count > SIZE_MAX / sizeof(cbm_pipeline_lsp_reference_index_entry_t)) {
        return false;
    }

    cbm_pipeline_lsp_reference_index_entry_t *entries =
        (cbm_pipeline_lsp_reference_index_entry_t *)malloc((size_t)reference_count *
                                                           sizeof(*entries));
    if (!entries) {
        return false;
    }
    int next = 0;
    for (int i = 0; i < arr->count; i++) {
        if (arr->items[i].kind != CBM_RESOLVED_CALL_REFERENCE) {
            continue;
        }
        entries[next].row = &arr->items[i];
        entries[next].original_index = i;
        next++;
    }
    qsort(entries, (size_t)reference_count, sizeof(*entries),
          cbm_pipeline_lsp_reference_index_entry_cmp);
    index->entries = entries;
    index->count = reference_count;
    return true;
}

static inline void cbm_pipeline_lsp_reference_index_free(
    cbm_pipeline_lsp_reference_index_t *index) {
    if (!index) {
        return;
    }
    free(index->entries);
    index->entries = NULL;
    index->count = 0;
}

static inline int cbm_pipeline_lsp_reference_site_cmp_usage(const CBMResolvedCall *row,
                                                            const CBMUsage *usage) {
    if (row->source_origin != usage->source_origin) {
        return row->source_origin < usage->source_origin ? -1 : 1;
    }
    if (row->site_start_byte != usage->site_start_byte) {
        return row->site_start_byte < usage->site_start_byte ? -1 : 1;
    }
    if (row->site_end_byte != usage->site_end_byte) {
        return row->site_end_byte < usage->site_end_byte ? -1 : 1;
    }
    return 0;
}

static inline const CBMResolvedCall *cbm_pipeline_lsp_reference_view_row(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_reference_index_entry_t *entries,
    int first, int offset) {
    return entries ? entries[first + offset].row : &arr->items[first + offset];
}

/* Look up the highest-confidence LSP resolution for a callable reference.
 * Reference records intentionally use a separate kind from invocations: an
 * explicit `Type::method`/`obj::method`, PHP first-class callable, or an exact
 * TS function-value argument denotes a function value and must never enter
 * the CALLS pipeline.
 *
 * Exact enclosing-function matches win. The unique Class.method caller-tail
 * fallback is admitted only when the caller opted into the existing JVM-only
 * rule via `allow_tail_match`; ambiguity returns NULL so the usage pipeline can
 * fall back to the textual registry without fabricating a semantic match.
 *
 * `entries` is either NULL for the original full-array fallback or a sorted
 * occurrence slice. The matching body is deliberately shared so indexing
 * cannot diverge on caller, reason, ambiguity, target identity, or confidence. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_reference_view_in_graph(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_reference_index_entry_t *entries,
    int first, int row_count, const CBMUsage *usage, bool allow_tail_match, const cbm_gbuf_t *gbuf,
    const char *project_name) {
    if ((!entries && !arr) || row_count <= 0 || !usage || !usage->enclosing_func_qn ||
        !usage->ref_name) {
        return NULL;
    }

    const char *ref_leaf = cbm_lsp_bare_segment(usage->ref_name);
    if (!ref_leaf || !ref_leaf[0]) {
        return NULL;
    }

    const CBMResolvedCall *best_exact = NULL;
    bool exact_ambiguous = false;
    for (int i = 0; i < row_count; i++) {
        cbm_pipeline_lsp_reference_lookup_test_note_row();
        const CBMResolvedCall *rc = cbm_pipeline_lsp_reference_view_row(arr, entries, first, i);
        if (rc->kind != CBM_RESOLVED_CALL_REFERENCE || !rc->caller_qn || !rc->callee_qn ||
            rc->confidence < CBM_LSP_CONFIDENCE_FLOOR) {
            continue;
        }
        if (strcmp(rc->caller_qn, usage->enclosing_func_qn) != 0 ||
            !cbm_pipeline_source_occurrence_eq(usage->site_start_byte, usage->site_end_byte,
                                               usage->source_origin, rc->site_start_byte,
                                               rc->site_end_byte, rc->source_origin)) {
            continue;
        }
        bool name_matches = strcmp(cbm_lsp_bare_segment(rc->callee_qn), ref_leaf) == 0;
        bool semantic_reason_matches = cbm_pipeline_reference_reason_matches(rc, ref_leaf);
        if (!name_matches && !semantic_reason_matches) {
            continue;
        }
        if (!best_exact) {
            best_exact = rc;
            continue;
        }
        if (!cbm_pipeline_reference_targets_equal(best_exact->callee_qn, rc->callee_qn, gbuf,
                                                  project_name, allow_tail_match)) {
            exact_ambiguous = true;
            continue;
        }
        if (rc->confidence > best_exact->confidence) {
            best_exact = rc;
        }
    }
    if (best_exact || !allow_tail_match) {
        return exact_ambiguous ? NULL : best_exact;
    }

    const char *caller_tail = cbm_pipeline_qn_class_method_tail(usage->enclosing_func_qn);
    if (!caller_tail) {
        return NULL;
    }

    const CBMResolvedCall *best_tail = NULL;
    bool tail_ambiguous = false;
    for (int i = 0; i < row_count; i++) {
        cbm_pipeline_lsp_reference_lookup_test_note_row();
        const CBMResolvedCall *rc = cbm_pipeline_lsp_reference_view_row(arr, entries, first, i);
        if (rc->kind != CBM_RESOLVED_CALL_REFERENCE || !rc->caller_qn || !rc->callee_qn ||
            rc->confidence < CBM_LSP_CONFIDENCE_FLOOR) {
            continue;
        }
        if (!cbm_pipeline_qn_class_method_tail_eq(rc->caller_qn, caller_tail) ||
            !cbm_pipeline_source_occurrence_eq(usage->site_start_byte, usage->site_end_byte,
                                               usage->source_origin, rc->site_start_byte,
                                               rc->site_end_byte, rc->source_origin)) {
            continue;
        }
        bool name_matches = strcmp(cbm_lsp_bare_segment(rc->callee_qn), ref_leaf) == 0;
        bool semantic_reason_matches = cbm_pipeline_reference_reason_matches(rc, ref_leaf);
        if (!name_matches && !semantic_reason_matches) {
            continue;
        }
        if (!best_tail) {
            best_tail = rc;
            continue;
        }
        if (!cbm_pipeline_reference_targets_equal(best_tail->callee_qn, rc->callee_qn, gbuf,
                                                  project_name, allow_tail_match)) {
            tail_ambiguous = true;
            continue;
        }
        if (rc->confidence > best_tail->confidence) {
            best_tail = rc;
        }
    }
    return tail_ambiguous ? NULL : best_tail;
}

/* Full-array compatibility/fallback path. Allocation failure in either
 * materializer deliberately lands here rather than dropping semantic edges. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_reference_in_graph(
    const CBMResolvedCallArray *arr, const CBMUsage *usage, bool allow_tail_match,
    const cbm_gbuf_t *gbuf, const char *project_name) {
    int count = arr ? arr->count : 0;
    return cbm_pipeline_find_lsp_reference_view_in_graph(arr, NULL, 0, count, usage,
                                                         allow_tail_match, gbuf, project_name);
}

/* Binary-search the exact (origin,start,end) occurrence, then run the shared
 * matcher only over that stable-order slice. A NULL index means construction
 * failed and selects the original linear fallback above. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_reference_indexed_in_graph(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_reference_index_t *index,
    const CBMUsage *usage, bool allow_tail_match, const cbm_gbuf_t *gbuf,
    const char *project_name) {
    if (!index) {
        return cbm_pipeline_find_lsp_reference_in_graph(arr, usage, allow_tail_match, gbuf,
                                                        project_name);
    }
    if (!usage || !usage->enclosing_func_qn || !usage->ref_name || index->count <= 0) {
        return NULL;
    }

    int low = 0;
    int high = index->count;
    while (low < high) {
        int middle = low + (high - low) / 2;
        if (cbm_pipeline_lsp_reference_site_cmp_usage(index->entries[middle].row, usage) < 0) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    int first = low;
    high = index->count;
    while (low < high) {
        int middle = low + (high - low) / 2;
        if (cbm_pipeline_lsp_reference_site_cmp_usage(index->entries[middle].row, usage) <= 0) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    int row_count = low - first;
    return cbm_pipeline_find_lsp_reference_view_in_graph(
        arr, index->entries, first, row_count, usage, allow_tail_match, gbuf, project_name);
}

/* String-strict helper retained for unit tests and callers without a graph.
 * Production edge materialization uses the graph-aware variant above. */
static inline const CBMResolvedCall *cbm_pipeline_find_lsp_reference(
    const CBMResolvedCallArray *arr, const CBMUsage *usage, bool allow_tail_match) {
    return cbm_pipeline_find_lsp_reference_in_graph(arr, usage, allow_tail_match, NULL, NULL);
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
 *      tail;
 *   4. when a package-shaped top-level JVM QN cannot share a tail with the
 *      graph's file-shaped QN, accept one globally unique callable short name.
 *
 * Returns the matching node, or NULL if neither lookup hits. */
static inline const cbm_gbuf_node_t *cbm_pipeline_lsp_target_node_policy(
    const cbm_gbuf_t *gbuf, const char *project_name, const char *callee_qn, bool allow_tail_match,
    bool allow_unique_callable_fallback) {
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
            size_t callee_len = strlen(callee_qn);
            if (proj_len <= SIZE_MAX - callee_len - 2U) {
                size_t prefixed_len = proj_len + 1U + callee_len;
                char *prefixed_qn = (char *)malloc(prefixed_len + 1U);
                if (prefixed_qn) {
                    memcpy(prefixed_qn, project_name, proj_len);
                    prefixed_qn[proj_len] = '.';
                    memcpy(prefixed_qn + proj_len + 1U, callee_qn, callee_len + 1U);
                    const cbm_gbuf_node_t *prefixed = cbm_gbuf_find_by_qn(gbuf, prefixed_qn);
                    free(prefixed_qn);
                    if (prefixed) {
                        return prefixed;
                    }
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
    const cbm_gbuf_node_t *unique_callable = NULL;
    bool tail_ambiguous = false;
    bool callable_ambiguous = false;
    for (int i = 0; i < hit_count; i++) {
        const cbm_gbuf_node_t *cand = hits[i];
        if (!cand || !cand->label || !cand->qualified_name) {
            continue;
        }
        if (strcmp(cand->label, "Function") != 0 && strcmp(cand->label, "Method") != 0) {
            continue;
        }
        if (!unique_callable) {
            unique_callable = cand;
        } else if (unique_callable->id != cand->id) {
            callable_ambiguous = true;
        }
        if (!cbm_pipeline_qn_class_method_tail_eq(cand->qualified_name, callee_tail)) {
            continue;
        }
        if (match && match->id != cand->id) {
            tail_ambiguous = true;
            continue;
        }
        match = cand;
    }
    if (match) {
        return tail_ambiguous ? NULL : match;
    }
    if (!allow_unique_callable_fallback) {
        return NULL;
    }
    return callable_ambiguous ? NULL : unique_callable;
}

#endif /* CBM_PIPELINE_LSP_RESOLVE_H */
