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
#include "callable_sig.h"        /* cbm_qn_callable_base_len — leaf splitters skip the suffix */
#include "foundation/mem_core.h" /* field-owner index storage */
#include "graph_buffer/graph_buffer.h"
#include "foundation/constants.h"
#include "pipeline/pipeline.h" /* cbm_registry_unique_field_qn, cbm_resolution_t */
#include "resolve/policy.h"
#include "resolve/proof.h"
#include "service_patterns.h" /* cbm_service_pattern_match for cbm_pipeline_service_name */

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
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
    /* Scan the base only: a callable identity suffix (#2061) belongs to the
     * leaf and may itself contain ':' (Swift labels) or '>' (generics). */
    const char *end = name + cbm_qn_callable_base_len(name);
    for (const char *p = name; p < end; p++) {
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
    /* The last '.' of the BASE QN: a callable identity suffix (#2061) never
     * contains '.', so this is the historical strrchr for every QN. */
    const char *last = NULL;
    size_t base_len = cbm_qn_callable_base_len(qn);
    for (size_t i = 0; i < base_len; i++) {
        if (qn[i] == '.') {
            last = qn + i;
        }
    }
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

/* Package/file identity mismatches need an explicit canonical mapping.
 * A matching class/method suffix is never binding evidence. */
static inline bool cbm_pipeline_lsp_allow_tail_match(CBMLanguage lang) {
    (void)lang;
    return false;
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

/* The Kotlin resolver claims a property READ on a typed receiver with an exact
 * row (lsp_kt_property_ref) so that the raw-name fallback cannot borrow a
 * same-named function. That row names a property, never a function value. Its
 * QN is not always materialized under that spelling (a property of a nested
 * class); the graph lookup then finds nothing, or its unique-callable step
 * picks a same-named function elsewhere (#1555: okhttp `server.webSocket` -> a
 * test function `webSocket`, a fabricated CALL_REFERENCE). Such a row is not
 * admissible: the caller falls back by name, restricted to non-callable
 * targets, which binds the property's own node as before the receiver was
 * typed and can never borrow a function. */
static inline bool cbm_pipeline_reference_target_admissible(const CBMResolvedCall *ref,
                                                            const cbm_gbuf_node_t *tgt) {
    if (!ref || !ref->strategy || strcmp(ref->strategy, "lsp_kt_property_ref") != 0) {
        return true;
    }
    return tgt && !cbm_pipeline_node_is_callable_target(tgt);
}

static inline int cbm_pipeline_qn_class_method_tail_eq(const char *qn, const char *tail) {
    const char *qt = cbm_pipeline_qn_class_method_tail(qn);
    return qt && tail && strcmp(qt, tail) == 0;
}

static inline const cbm_gbuf_node_t *cbm_pipeline_lsp_target_node_policy(
    const cbm_gbuf_t *gbuf, const char *project_name, const char *callee_qn, bool allow_tail_match,
    bool allow_unique_callable_fallback);

/* Local and cross passes may spell one target with and without the project
 * prefix. Equality requires both spellings to identify the same graph node;
 * suffix coincidence never establishes declaration identity. */
static inline bool cbm_pipeline_invocation_targets_equal(const char *left_qn, const char *right_qn,
                                                         const cbm_gbuf_t *gbuf,
                                                         const char *project_name,
                                                         bool allow_tail_match) {
    (void)allow_tail_match;
    if (!left_qn || !right_qn)
        return false;
    if (strcmp(left_qn, right_qn) == 0)
        return true;
    if (!gbuf || !project_name)
        return false;
    const cbm_gbuf_node_t *left =
        cbm_pipeline_lsp_target_node_policy(gbuf, project_name, left_qn, false, false);
    const cbm_gbuf_node_t *right =
        cbm_pipeline_lsp_target_node_policy(gbuf, project_name, right_qn, false, false);
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

/* A resolver row that proves the call's receiver is a type OUTSIDE the project:
 * PHP `$r->m()` / `R::m()` where `R` is imported from a vendor namespace
 * (#1186). When its target has no graph node, it rules out every name-only
 * fallback: the method of a vendor class is not a same-named project method. */
static inline bool cbm_pipeline_lsp_external_receiver(const CBMResolvedCall *resolved) {
    return resolved && resolved->strategy &&
           strcmp(resolved->strategy, "php_external_receiver") == 0;
}

/* The service-pattern name of a call: its callee text, or, when that names no
 * service and the LSP proved the receiver a vendor class, the vendor method
 * (`GuzzleClient::get` -> `GuzzleHttp.Client.get`). The extractor names a
 * scoped call by its method alone, and such a call never reaches the
 * registry, so the vendor class is its only service evidence (#1186). */
static inline const char *cbm_pipeline_service_name(const CBMCall *call,
                                                    const CBMResolvedCall *external,
                                                    cbm_svc_kind_t *kind) {
    *kind = cbm_service_pattern_match(call->callee_name);
    if (*kind == CBM_SVC_NONE && cbm_pipeline_lsp_external_receiver(external) &&
        external->callee_qn && external->callee_qn[0]) {
        *kind = cbm_service_pattern_match(external->callee_qn);
        if (*kind != CBM_SVC_NONE) {
            return external->callee_qn;
        }
    }
    return call->callee_name;
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

    /* A proved local callable can name a different target than its source
     * variable. Join its exact occurrence and spelling using the actual proof;
     * diagnostic strategy labels do not establish this binding. */
    if (site_rank == 2 && resolved->reason && strcmp(resolved->reason, call->callee_name) == 0 &&
        resolved->evidence.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE &&
        resolved->evidence.rule == CBM_RESOLVE_RULE_CALLABLE_BINDING &&
        (resolved->evidence.facts & CBM_RESOLVE_FACT_OCCURRENCE_EXACT)) {
        const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(resolved->evidence.proof);
        if (proof && proof->predecessors_closed && cbm_resolve_admit(&proof->outcome) &&
            proof->outcome.rule == resolved->evidence.rule &&
            proof->outcome.facts == resolved->evidence.facts &&
            strcmp(proof->outcome.target, resolved->callee_qn) == 0)
            return true;
    }

    /* Indirect resolvers retain the textual source leaf in reason. */
    return cbm_pipeline_invocation_reason_join_strategy(resolved->strategy) && resolved->reason &&
           call_leaf && strcmp(cbm_lsp_bare_segment(resolved->reason), call_leaf) == 0;
}

/* #606: a sub-floor `php_method_typed_unindexed` row still proves the receiver
 * is not a project class, so a weak short-name fallback must not bind it. A
 * project class with the same simple name keeps the fallback, since the PHP
 * LSP does not follow every cross-file type. */
static inline bool cbm_pipeline_php_receiver_type_unindexed(const CBMResolvedCallArray *arr,
                                                            const CBMCall *call,
                                                            const cbm_registry_t *registry) {
    if (!arr || !call || !call->enclosing_func_qn || !call->callee_name) {
        return false;
    }
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->kind != CBM_RESOLVED_INVOCATION || !rc->caller_qn || !rc->callee_qn ||
            !rc->strategy || strcmp(rc->strategy, "php_method_typed_unindexed") != 0 ||
            strcmp(rc->caller_qn, call->enclosing_func_qn) != 0) {
            continue;
        }
        int site_rank = cbm_pipeline_invocation_site_rank(rc, call);
        if (site_rank == 0 || !cbm_pipeline_invocation_leaf_matches(rc, call, site_rank)) {
            continue;
        }
        const char *method_dot = strrchr(rc->callee_qn, '.');
        if (!method_dot || method_dot == rc->callee_qn) {
            continue;
        }
        char class_qn[CBM_SZ_512];
        size_t class_len = (size_t)(method_dot - rc->callee_qn);
        if (class_len >= sizeof(class_qn)) {
            continue;
        }
        memcpy(class_qn, rc->callee_qn, class_len);
        class_qn[class_len] = '\0';
        return !cbm_registry_has_type_named(registry, cbm_lsp_bare_segment(class_qn));
    }
    return false;
}

/* Select a candidate only within the exact enclosing function. Exact source
 * occurrences outrank legacy spanless rows; competing targets abstain. The
 * compatibility flag cannot authorize caller suffix matching. Edge admission
 * separately requires immutable evidence for the selected occurrence.
 * The returned pointer aliases `arr` while its CBMFileResult remains alive. */
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
    return exact_caller_ambiguous ? NULL : best_exact_caller;
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

/* Rust (#2053): true when the Rust LSP positively resolved a call to a
 * registered symbol that lives OUTSIDE the project — a std/core/alloc seed
 * method (`root.join()` on `root: &Path` -> std.path.Path.join) or a seeded
 * crate API. Such a row has no graph node, and before this gate the callers
 * then fell back to the textual registry, which bound the call to whatever
 * project method shares the leaf name (EvidenceTier::join, unique_name).
 *
 * The evidence is the strategy plus the QN's namespace. The listed strategies
 * emit the QN of a function the LSP registry actually holds; project defs are
 * registered under their project-prefixed QNs, so a registered QN outside the
 * project prefix is an embedded seed by construction. A project-prefixed QN
 * that merely failed the gbuf lookup is NOT external and keeps the #1085
 * registry fallback, as does every unresolved/unknown-receiver row (those
 * never reach the callers: they sit below CBM_LSP_CONFIDENCE_FLOOR).
 * Rust-only: suppressors stay per-language. Pure; unit-tested. */
static inline bool cbm_pipeline_rust_external_target(CBMLanguage lang, const char *strategy,
                                                     const char *callee_qn,
                                                     const char *project_name) {
    if (lang != CBM_LANG_RUST || !strategy || !callee_qn || !callee_qn[0] || !project_name ||
        !project_name[0]) {
        return false;
    }
    static const char *const registered_target_strategies[] = {
        "lsp_method_dispatch", "lsp_trait_dispatch", "lsp_deref_dispatch", "lsp_bound_dispatch",
        "lsp_direct",          "lsp_ufcs",           "lsp_constructor",
    };
    bool registered_target = false;
    for (size_t i = 0;
         i < sizeof(registered_target_strategies) / sizeof(registered_target_strategies[0]); i++) {
        if (strcmp(strategy, registered_target_strategies[i]) == 0) {
            registered_target = true;
            break;
        }
    }
    if (!registered_target) {
        return false;
    }
    size_t proj_len = strlen(project_name);
    return !(strncmp(callee_qn, project_name, proj_len) == 0 && callee_qn[proj_len] == '.');
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
 * Only exact enclosing-function identities and source occurrences can match.
 * The compatibility flag cannot authorize a Class.method caller-tail join.
 * Competing target identities return NULL.
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
    return exact_ambiguous ? NULL : best_exact;
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

/* C fields retain occurrence identity before graph aggregation. The index
 * orders (caller, member, origin, start, end, target); a lookup only examines
 * the exact occurrence's duplicates. Missing indexes and legacy spanless
 * carriers abstain, rather than performing an unbounded scan per reference. */
typedef struct {
    const CBMResolvedCall **rows;
    int count;
} cbm_pipeline_lsp_field_index_t;

static inline bool cbm_pipeline_lsp_field_row_usable(const CBMResolvedCall *row) {
    return row && row->kind == CBM_RESOLVED_FIELD_REFERENCE && row->caller_qn && row->callee_qn;
}

static inline bool cbm_pipeline_lsp_field_row_proven(const CBMResolvedCall *row) {
    const uint32_t required = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                              CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    return cbm_pipeline_lsp_field_row_usable(row) &&
           row->evidence.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE &&
           row->evidence.rule == CBM_RESOLVE_RULE_TYPE_MEMBER &&
           (row->evidence.facts & required) == required &&
           cbm_pipeline_source_site_present(row->site_start_byte, row->site_end_byte);
}

static inline int cbm_pipeline_lsp_field_key_cmp(const CBMResolvedCall *row, const char *caller_qn,
                                                 const char *member) {
    int by_caller = strcmp(row->caller_qn, caller_qn);
    return by_caller != 0 ? by_caller : strcmp(cbm_lsp_bare_segment(row->callee_qn), member);
}

static inline int cbm_pipeline_lsp_field_site_cmp(const CBMResolvedCall *row, const char *caller_qn,
                                                  const char *member, uint32_t start, uint32_t end,
                                                  CBMSourceOrigin origin) {
    int by_key = cbm_pipeline_lsp_field_key_cmp(row, caller_qn, member);
    if (by_key)
        return by_key;
    if (row->source_origin != origin)
        return row->source_origin < origin ? -1 : 1;
    if (row->site_start_byte != start)
        return row->site_start_byte < start ? -1 : 1;
    if (row->site_end_byte != end)
        return row->site_end_byte < end ? -1 : 1;
    return 0;
}

static inline int cbm_pipeline_lsp_field_row_cmp(const void *left_ptr, const void *right_ptr) {
    const CBMResolvedCall *left = *(const CBMResolvedCall *const *)left_ptr;
    const CBMResolvedCall *right = *(const CBMResolvedCall *const *)right_ptr;
    int by_key = cbm_pipeline_lsp_field_site_cmp(
        left, right->caller_qn, cbm_lsp_bare_segment(right->callee_qn), right->site_start_byte,
        right->site_end_byte, right->source_origin);
    return by_key ? by_key : strcmp(left->callee_qn, right->callee_qn);
}

static inline bool cbm_pipeline_lsp_field_index_build(const CBMResolvedCallArray *arr,
                                                      cbm_pipeline_lsp_field_index_t *index) {
    *index = (cbm_pipeline_lsp_field_index_t){0};
    int count = 0;
    for (int i = 0; arr && i < arr->count; i++) {
        if (cbm_pipeline_lsp_field_row_usable(&arr->items[i]))
            count++;
    }
    if (!count)
        return true;
    const CBMResolvedCall **rows = cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)count * sizeof(*rows));
    if (!rows)
        return false;
    int next = 0;
    for (int i = 0; i < arr->count; i++) {
        if (cbm_pipeline_lsp_field_row_usable(&arr->items[i]))
            rows[next++] = &arr->items[i];
    }
    qsort(rows, (size_t)count, sizeof(*rows), cbm_pipeline_lsp_field_row_cmp);
    index->rows = rows;
    index->count = count;
    return true;
}

static inline void cbm_pipeline_lsp_field_index_free(cbm_pipeline_lsp_field_index_t *index) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, (void *)index->rows);
    *index = (cbm_pipeline_lsp_field_index_t){0};
}

typedef struct {
    const CBMResolvedCall *row;
    bool typed;
} cbm_pipeline_lsp_field_cursor_t;

static inline cbm_pipeline_lsp_field_cursor_t cbm_pipeline_lsp_field_cursor_at(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_field_index_t *index,
    const char *caller, const char *member, uint32_t start, uint32_t end, CBMSourceOrigin origin) {
    (void)arr;
    cbm_pipeline_lsp_field_cursor_t cursor = {0};
    if (!index || !caller || !member || !cbm_pipeline_source_site_present(start, end))
        return cursor;
    int low = 0;
    int high = index->count;
    while (low < high) {
        int middle = low + (high - low) / 2;
        if (cbm_pipeline_lsp_field_site_cmp(index->rows[middle], caller, member, start, end,
                                            origin) < 0)
            low = middle + 1;
        else
            high = middle;
    }
    bool rejected = false;
    for (int i = low; i < index->count; i++) {
        const CBMResolvedCall *row = index->rows[i];
        if (cbm_pipeline_lsp_field_site_cmp(row, caller, member, start, end, origin))
            break;
        cursor.typed = true;
        if (!cbm_pipeline_lsp_field_row_proven(row) ||
            (cursor.row && strcmp(cursor.row->callee_qn, row->callee_qn) != 0))
            rejected = true;
        if (!cursor.row)
            cursor.row = row;
    }
    if (rejected)
        cursor.row = NULL;
    return cursor;
}

/* Kept for source compatibility. Aggregated rows cannot prove an occurrence. */
static inline cbm_pipeline_lsp_field_cursor_t cbm_pipeline_lsp_field_cursor(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_field_index_t *index,
    const char *caller, const char *member) {
    return cbm_pipeline_lsp_field_cursor_at(arr, index, caller, member, 0, 0,
                                            CBM_SOURCE_ORIGIN_RAW);
}

static inline const CBMResolvedCall *cbm_pipeline_lsp_field_next(
    cbm_pipeline_lsp_field_cursor_t *cursor) {
    const CBMResolvedCall *row = cursor->row;
    cursor->row = NULL;
    return row;
}

static inline const cbm_gbuf_node_t *cbm_pipeline_lsp_field_node(const cbm_gbuf_t *gbuf,
                                                                 const char *project_name,
                                                                 const CBMResolvedCall *row) {
    if (!cbm_pipeline_lsp_field_row_proven(row))
        return NULL;
    const cbm_gbuf_node_t *node =
        cbm_pipeline_lsp_target_node_policy(gbuf, project_name, row->callee_qn, false, false);
    return node && node->label && strcmp(node->label, "Field") == 0 ? node : NULL;
}

static inline const cbm_gbuf_node_t *cbm_pipeline_c_member_call_field_at(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_field_index_t *index,
    const cbm_gbuf_t *gbuf, const char *project_name, const char *caller, const char *callee,
    uint32_t start, uint32_t end, CBMSourceOrigin origin, bool *typed) {
    cbm_pipeline_lsp_field_cursor_t cursor = cbm_pipeline_lsp_field_cursor_at(
        arr, index, caller, cbm_lsp_bare_segment(callee), start, end, origin);
    *typed = cursor.typed;
    return cursor.row ? cbm_pipeline_lsp_field_node(gbuf, project_name, cursor.row) : NULL;
}

static inline const cbm_gbuf_node_t *cbm_pipeline_c_member_call_field(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_field_index_t *index,
    const cbm_gbuf_t *gbuf, const char *project_name, const char *caller, const char *callee,
    bool *typed) {
    (void)index;
    (void)gbuf;
    (void)project_name;
    *typed = false;
    for (int i = 0; arr && caller && callee && i < arr->count; i++) {
        const CBMResolvedCall *row = &arr->items[i];
        if (cbm_pipeline_lsp_field_row_usable(row) &&
            !cbm_pipeline_lsp_field_key_cmp(row, caller, cbm_lsp_bare_segment(callee))) {
            *typed = true;
            break;
        }
    }
    return NULL;
}

#define CBM_CONF_MEMBER_TYPED 0.95

static inline const cbm_gbuf_node_t *cbm_pipeline_c_member_call_resolve_at(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_field_index_t *index,
    const cbm_gbuf_t *gbuf, const cbm_registry_t *registry, const char *project_name,
    const char *caller, const char *callee, bool allow_unique_name, uint32_t start, uint32_t end,
    CBMSourceOrigin origin, cbm_resolution_t *res) {
    (void)registry;
    (void)allow_unique_name;
    bool typed = false;
    const cbm_gbuf_node_t *field = cbm_pipeline_c_member_call_field_at(
        arr, index, gbuf, project_name, caller, callee, start, end, origin, &typed);
    if (field) {
        *res = (cbm_resolution_t){
            .qualified_name = field->qualified_name,
            .strategy = "lsp_field_access",
            .confidence = CBM_CONF_MEMBER_TYPED,
            .candidate_count = 1,
            .proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                      .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                      .rule = CBM_RESOLVE_RULE_TYPE_MEMBER,
                      .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                               CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                      .target = field->qualified_name,
                      .candidate_count = 1}};
    }
    return field;
}

static inline const cbm_gbuf_node_t *cbm_pipeline_c_member_call_resolve(
    const CBMResolvedCallArray *arr, const cbm_pipeline_lsp_field_index_t *index,
    const cbm_gbuf_t *gbuf, const cbm_registry_t *registry, const char *project_name,
    const char *caller, const char *callee, bool allow_unique_name, cbm_resolution_t *res) {
    return cbm_pipeline_c_member_call_resolve_at(arr, index, gbuf, registry, project_name, caller,
                                                 callee, allow_unique_name, 0, 0,
                                                 CBM_SOURCE_ORIGIN_RAW, res);
}

/* Materialize canonical identity only: exact QN, project qualification, or
 * the established C macro spelling. Names and class/method suffixes supply
 * no target identity and never participate. Counters remain ABI-compatible. */
extern _Atomic uint64_t g_lsp_tail_lookups;
extern _Atomic uint64_t g_lsp_tail_candidates;

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
    /* Exact retries, in tie-rule order (CBM_MACRO_QN_SUFFIX): the definition
     * first -- the QN as given, then with the project prefix -- and only when
     * neither names a node the C-preprocessor macro of that name, in the same
     * two spellings. The macro case is a prototype the C resolver knows whose
     * only node in the graph is its rename macro (`extern T f(...);` plus
     * `#define f f_v2`). One buffer, "<project>.<callee_qn>#macro", serves all
     * three retries. */
    size_t proj_len = (project_name && project_name[0]) ? strlen(project_name) : 0;
    size_t callee_len = strlen(callee_qn);
    size_t suffix_len = sizeof(CBM_MACRO_QN_SUFFIX) - 1U;
    bool add_prefix = proj_len > 0 && !(strncmp(callee_qn, project_name, proj_len) == 0 &&
                                        callee_qn[proj_len] == '.');
    if (proj_len <= SIZE_MAX - callee_len - suffix_len - 2U) {
        char *buf = (char *)malloc(proj_len + 1U + callee_len + suffix_len + 1U);
        if (buf) {
            char *own = buf + proj_len + 1U; /* callee_qn inside the buffer */
            if (proj_len > 0) {
                memcpy(buf, project_name, proj_len);
            }
            buf[proj_len] = '.';
            memcpy(own, callee_qn, callee_len + 1U);
            const cbm_gbuf_node_t *hit = add_prefix ? cbm_gbuf_find_by_qn(gbuf, buf) : NULL;
            if (!hit) {
                memcpy(own + callee_len, CBM_MACRO_QN_SUFFIX, suffix_len + 1U);
                hit = cbm_gbuf_find_by_qn(gbuf, own);
                if (!hit && add_prefix) {
                    hit = cbm_gbuf_find_by_qn(gbuf, buf);
                }
            }
            free(buf);
            if (hit) {
                return hit;
            }
        }
    }
    (void)allow_tail_match;
    (void)allow_unique_callable_fallback;
    return NULL;
}

#endif /* CBM_PIPELINE_LSP_RESOLVE_H */
