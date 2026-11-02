#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "resolve/policy.h"
#include "pipeline/import_resolve.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/pass_lsp_cross.h"
#include "lsp/type_registry.h"
#include "resolve/rust_names.h"

/* Semantic import bindings survive edits to the rendered graph. A source or
 * configuration change, on the other hand, invalidates the captured evidence. */
typedef struct {
    bool setup, original, tampered, deleted, source_changed, configuration_changed, predecessor;
} authority_result_t;

static bool authority_seed(cbm_gbuf_t *graph, const char *rel, const CBMFileResult *result) {
    char *file = cbm_pipeline_fqn_compute("p", rel, "__file__");
    bool ok = file && cbm_gbuf_upsert_node(graph, "File", rel, file, rel, 0, 0, "{}") > 0;
    free(file);
    for (int i = 0; ok && i < result->defs.count; i++) {
        const CBMDefinition *def = &result->defs.items[i];
        ok = cbm_gbuf_upsert_node(graph, def->label, def->name, def->qualified_name, rel,
                                  def->start_line, def->end_line, "{}") > 0;
    }
    return ok;
}

static bool authority_binding(cbm_gbuf_t *graph, const char *rel, const CBMFileResult *result,
                              bool rust, const char *expected) {
    const char **keys = NULL, **values = NULL;
    int count = 0;
    int rc = rust ? cbm_pipeline_rust_import_map(graph, "p", rel, result, &keys, &values, &count)
                  : cbm_pipeline_c_include_map(graph, "p", rel, result, &keys, &values, &count);
    bool found = false;
    const char *key = rust ? "T" : "right.h";
    for (int i = 0; i < count; i++) {
        if (!strcmp(keys[i], key)) {
            found = expected ? !strcmp(values[i], expected) : !values[i][0];
            if (!found)
                fprintf(stderr, "import authority %s: got '%s', expected '%s'\n", key, values[i],
                        expected ? expected : "<unresolved>");
        }
        free((void *)keys[i]);
        free((void *)values[i]);
    }
    free(keys);
    free(values);
    return rc == 0 && found;
}

static bool authority_predecessor(cbm_gbuf_t *graph, const CBMFileResult *result) {
    const cbm_resolve_import_snapshot_t *snapshot =
        cbm_pipeline_import_snapshot(graph, "p", result->source_file, result);
    if (!snapshot)
        return false;
    for (size_t i = 0; i < snapshot->count; i++) {
        const cbm_resolve_import_fact_t *fact = &snapshot->items[i];
        const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(fact->binding.proof);
        if (!view || !cbm_resolve_admit(&fact->binding))
            continue;
        if (view->kind != CBM_PROOF_EXPORT_BINDING || !cbm_resolve_admit(&fact->dependency) ||
            !fact->dependency.proof || !strcmp(fact->dependency.target, fact->binding.target))
            return false;
        for (size_t j = 0; j < view->premise_count; j++)
            if (view->premises[j] == fact->dependency.proof)
                return true;
    }
    return false;
}

static authority_result_t authority_fixture(bool rust) {
    authority_result_t out = {0};
    const char *root = th_mktempdir("cbm_import_authority");
    const char *c_rels[] = {"main.c", "right.h", "wrong.h"};
    const char *c_sources[] = {"#include \"right.h\"\nvoid run(void) { foo(); }\n",
                               "void foo(void);\n", "void foo(void);\n"};
    const char *rust_rels[] = {"main.rs", "right.rs", "wrong.rs"};
    const char *rust_sources[] = {
        "mod right; mod wrong; use crate::right::T; fn run(x: &T) { x.foo(); }\n",
        "pub struct T; impl T { pub fn foo(&self) {} }\n",
        "pub struct T; impl T { pub fn foo(&self) {} }\n"};
    const char *const *rels = rust ? rust_rels : c_rels;
    const char *const *sources = rust ? rust_sources : c_sources;
    CBMLanguage language = rust ? CBM_LANG_RUST : CBM_LANG_C;
    CBMFileResult *results[3] = {0};
    cbm_file_info_t files[3] = {0};
    cbm_gbuf_t *graph = root ? cbm_gbuf_new("p", root) : NULL;
    CBMHashTable *map = NULL;
    char *expected = NULL, *file_qn = NULL, *tampered = NULL;
    char *paths[3] = {0};
    bool ok = graph != NULL;
    for (int i = 0; ok && i < 3; i++) {
        ok = th_write_file(TH_PATH(root, rels[i]), sources[i]) == 0;
        paths[i] = strdup(TH_PATH(root, rels[i]));
        results[i] = ok ? cbm_extract_file(sources[i], (int)strlen(sources[i]), language, "p",
                                           rels[i], 0, NULL, NULL)
                        : NULL;
        files[i].rel_path = (char *)rels[i];
        files[i].path = paths[i];
        files[i].language = language;
        ok = results[i] && authority_seed(graph, rels[i], results[i]);
    }
    cbm_pipeline_ctx_t context = {.project_name = "p", .repo_path = root, .gbuf = graph};
    if (ok)
        map = cbm_pipeline_namespace_map_build("p", results, rels, 3);
    ok = ok && map && cbm_pipeline_import_context_build(&context, files, 3, results, map);
    if (ok) {
        (void)cbm_pipeline_create_import_edges(&context, results[0], rels[0], map);
        expected = rust ? cbm_pipeline_fqn_compute("p", rels[1], "T")
                        : cbm_pipeline_fqn_module("p", rels[1]);
        file_qn = cbm_pipeline_fqn_compute("p", rels[0], "__file__");
    }
    const cbm_gbuf_node_t *file = file_qn ? cbm_gbuf_find_by_qn(graph, file_qn) : NULL;
    ok = ok && expected && file && file->properties_json;
    out.setup = ok;
    if (ok) {
        out.original = authority_binding(graph, rels[0], results[0], rust, expected);
        out.predecessor = authority_predecessor(graph, results[0]);
        tampered = strdup(file->properties_json);
        ok = tampered != NULL;
        /* Preserve source identity and change only rendered target fields. */
        bool target_changed = false;
        for (char *cursor = tampered; cursor && (cursor = strstr(cursor, "\"target\":\""));) {
            cursor += strlen("\"target\":\"");
            char *end = strchr(cursor, '"');
            char *right = strstr(cursor, "right");
            if (end && right && right < end) {
                memcpy(right, "wrong", 5);
                target_changed = true;
            }
            cursor = end ? end + 1 : NULL;
        }
        /* C writes no binding list to alter, so forge one: a property naming a wrong
         * target must carry no more authority than an altered one. */
        if (!rust) {
            free(tampered);
            tampered = strdup("{\"c_includes_proven\":1,"
                              "\"c_include_bindings\":[{\"target\":\"wrong\"}]}");
            ok = tampered != NULL;
            target_changed = ok;
        }
        ok = ok && target_changed &&
             cbm_gbuf_node_set_properties_json((cbm_gbuf_node_t *)file, tampered) == 0;
        out.tampered = ok && authority_binding(graph, rels[0], results[0], rust, expected);
        ok = cbm_gbuf_delete_edges_by_type(graph, "IMPORTS") == 0 &&
             cbm_gbuf_node_set_properties_json((cbm_gbuf_node_t *)file, "{}") == 0;
        out.deleted = ok && authority_binding(graph, rels[0], results[0], rust, expected);
        /* Restore an authentic projection so these are independent freshness
         * checks, not merely rejection of the deliberately empty projection. */
        (void)cbm_pipeline_create_import_edges(&context, results[0], rels[0], map);
        CBMFileResult changed = *results[0];
        snprintf(changed.source_content_id[CBM_SOURCE_ORIGIN_RAW],
                 sizeof(changed.source_content_id[CBM_SOURCE_ORIGIN_RAW]), "%s", "changed-source");
        out.source_changed = authority_binding(graph, rels[0], &changed, rust, NULL);
        changed = *results[0];
        snprintf(changed.source_configuration_id, sizeof(changed.source_configuration_id), "%s",
                 "changed-configuration");
        out.configuration_changed = authority_binding(graph, rels[0], &changed, rust, NULL);
    }
    free(tampered);
    free(file_qn);
    free(expected);
    cbm_rust_project_free(context.rust_project);
    cbm_pipeline_namespace_map_free(map);
    cbm_gbuf_free(graph);
    for (int i = 0; i < 3; i++) {
        cbm_free_result(results[i]);
        free(paths[i]);
    }
    if (root)
        th_cleanup(root);
    return out;
}

typedef struct {
    const cbm_resolve_proof_t *binding, *dependency;
    bool binding_seen, dependency_seen;
} authority_closure_t;
static bool authority_find_predecessors(const cbm_resolve_proof_t *proof, void *context) {
    authority_closure_t *closure = context;
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof);
    closure->binding_seen |= view->fingerprint == closure->binding->value.fingerprint &&
                             cbm_resolve_proof_compare(proof, closure->binding) == 0;
    closure->dependency_seen |= view->fingerprint == closure->dependency->value.fingerprint &&
                                cbm_resolve_proof_compare(proof, closure->dependency) == 0;
    return true;
}
typedef struct {
    bool setup, audited, admitted, cloned, closed, binding_seen, dependency_seen;
    bool carrier, site_seen, any_admitted;
    char actual_target[256];
} authority_callable_result_t;
static void authority_callable_diagnostics(int mode, const CBMFileResult *result,
                                           const CBMLSPDef *defs, int count) {
    fprintf(stderr,
            "C pointer fixture mode=%d source_retained=%d definitions=%d calls=%d rows=%d\n", mode,
            result->source != NULL, count, result->calls.count, result->resolved_calls.count);
    for (int i = 0; i < count && i < 16; i++) {
        const CBMLSPDef *def = &defs[i];
        fprintf(stderr, "  def qn=%s module=%s file=%s declaration=%s flags=%u signature=%d\n",
                def->qualified_name ? def->qualified_name : "<none>",
                def->def_module_qn ? def->def_module_qn : "<none>",
                def->source_file_qn ? def->source_file_qn : "<none>",
                def->declaration_identity ? def->declaration_identity : "<none>", def->source_flags,
                def->source_signature != NULL);
    }
    for (int i = 0; i < result->calls.count && i < 16; i++) {
        const CBMCall *call = &result->calls.items[i];
        fprintf(stderr, "  carrier name=%s scope=%s span=%u:%u origin=%u\n",
                call->callee_name ? call->callee_name : "<none>",
                call->enclosing_func_qn ? call->enclosing_func_qn : "<none>", call->site_start_byte,
                call->site_end_byte, (unsigned)call->source_origin);
    }
    for (int i = 0; i < result->resolved_calls.count && i < 16; i++) {
        const CBMResolvedCall *row = &result->resolved_calls.items[i];
        const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(row->evidence.proof);
        fprintf(stderr,
                "  row kind=%d target=%s span=%u:%u strategy=%s reason=%s analysis=%d rule=%d "
                "facts=%u proof=%d proof_reason=%s\n",
                row->kind, row->callee_qn ? row->callee_qn : "<none>", row->site_start_byte,
                row->site_end_byte, row->strategy ? row->strategy : "<none>",
                row->reason ? row->reason : "<none>", row->evidence.analysis, row->evidence.rule,
                row->evidence.facts, proof != NULL,
                proof && proof->outcome.reason ? proof->outcome.reason : "<none>");
    }
}
static authority_callable_result_t authority_c_function_pointer(int mode) {
    authority_callable_result_t out = {0};
    const char *root = th_mktempdir("cbm_c_pointer_authority");
    bool cpp = mode == 4 || mode == 10 || mode == 11 || mode == 14;
    bool require_abstention = mode == 3 || mode == 4 || (mode >= 8 && mode <= 14 && mode != 9);
    CBMLanguage language = cpp ? CBM_LANG_CPP : CBM_LANG_C;
    const char *rels[] = {cpp ? "main.cpp" : "main.c", "right.h"};
    const char *direct = "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; fp(); }\n";
    const char *copy = "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; "
                       "void (*copy)(void) = fp; fp = 0; copy(); }\n";
    const char *assignment = "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = 0; "
                             "fp = foo; fp(); }\n";
    const char *escaped = "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; "
                          "void (**alias)(void) = &fp; fp = foo; *alias = bar; fp(); }\n";
    const char *delayed = "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; "
                          "auto later = [&] { fp = bar; }; fp(); }\n";
    const char *direct_call = "#include \"right.h\"\nvoid run(void) { foo(); }\n";
    const char *inner_escape = "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; "
                               "{ void (*fp)(void) = bar; void (**alias)(void) = &fp; } fp(); }\n";
    const char *fresh_shadow = "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = bar; "
                               "void (**outer_alias)(void) = &fp; "
                               "{ void (*fp)(void) = bar; void (**inner_alias)(void) = &fp; } "
                               "{ void (*fp)(void) = foo; fp(); } }\n";
    const char *parenthesized_address =
        "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; "
        "void (**alias)(void) = &(fp); fp = foo; *alias = bar; fp(); }\n";
    const char *parenthesized_assignment =
        "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; (fp) = bar; fp(); }\n";
    const char *parenthesized_reference =
        "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; "
        "auto &alias = (fp); fp = foo; alias = bar; fp(); }\n";
    const char *parenthesized_argument =
        "#include \"right.h\"\n"
        "void mutate(void (*&slot)(void)) { slot = bar; }\n"
        "void run(void) { void (*fp)(void) = foo; mutate((fp)); fp(); }\n";
    const char *nested_assignment =
        "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; "
        "fp = (fp = bar, foo); fp(); }\n";
    const char *nested_initializer =
        "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = (fp = bar, foo); fp(); }\n";
    const char *reference_capture =
        "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = foo; "
        "auto later = [&alias = fp] { alias = bar; }; later(); fp(); }\n";
    const char *parenthesized_copy =
        "#include \"right.h\"\nvoid run(void) { void (*fp)(void) = (foo); "
        "void (*copy)(void) = (/* exact value */ fp); fp = bar; copy(); }\n";
    const char *fixtures[] = {direct,
                              copy,
                              assignment,
                              escaped,
                              delayed,
                              direct_call,
                              inner_escape,
                              fresh_shadow,
                              parenthesized_address,
                              parenthesized_assignment,
                              parenthesized_reference,
                              parenthesized_argument,
                              nested_assignment,
                              nested_initializer,
                              reference_capture,
                              parenthesized_copy};
    const char *sources[] = {fixtures[mode], "static inline void foo(void) {}\n"
                                             "static inline void bar(void) {}\n"};
    CBMFileResult *results[2] = {0};
    cbm_file_info_t files[2] = {0};
    char *paths[2] = {0};
    CBMArena definitions, retained;
    cbm_arena_init(&definitions);
    cbm_arena_init(&retained);
    cbm_resolve_proof_pool_t retained_proofs;
    cbm_resolve_proof_pool_init(&retained_proofs, &retained, 0, 0, 0);
    authority_closure_t closure = {0};
    const cbm_resolve_proof_t *call = NULL;
    cbm_gbuf_t *graph = root ? cbm_gbuf_new("p", root) : NULL;
    CBMHashTable *map = NULL;
    CBMLSPDef *defs = NULL;
    char *modules[2] = {0};
    const char **names = NULL, **values = NULL;
    int count = 0, def_count = 0;
    bool ok = graph != NULL;
    for (int i = 0; ok && i < 2; i++) {
        paths[i] = strdup(TH_PATH(root, rels[i]));
        files[i].path = paths[i];
        ok = paths[i] && th_write_file(paths[i], sources[i]) == 0;
        results[i] = ok ? cbm_extract_file(sources[i], (int)strlen(sources[i]), language, "p",
                                           rels[i], 0, NULL, NULL)
                        : NULL;
        files[i].rel_path = (char *)rels[i];
        files[i].language = language;
        ok = results[i] && authority_seed(graph, rels[i], results[i]);
    }
    cbm_pipeline_ctx_t context = {.project_name = "p", .repo_path = root, .gbuf = graph};
    if (ok)
        map = cbm_pipeline_namespace_map_build("p", results, rels, 2);
    ok = ok && map && cbm_pipeline_import_context_build(&context, files, 2, results, map);
    const cbm_resolve_import_snapshot_t *snapshot = NULL;
    if (ok) {
        (void)cbm_pipeline_create_import_edges(&context, results[0], rels[0], map);
        snapshot = cbm_pipeline_import_snapshot(graph, "p", rels[0], results[0]);
        ok = snapshot && snapshot->count == 1 && cbm_resolve_admit(&snapshot->items[0].binding) &&
             cbm_resolve_admit(&snapshot->items[0].dependency);
    }
    if (ok) {
        closure.binding =
            cbm_resolve_proof_clone(&retained_proofs, snapshot->items[0].binding.proof);
        closure.dependency =
            cbm_resolve_proof_clone(&retained_proofs, snapshot->items[0].dependency.proof);
        defs = cbm_pxc_collect_all_defs(&context, &definitions, results, files, 2, "p", modules,
                                        &def_count, NULL);
        ok = closure.binding && closure.dependency && defs && def_count > 0 && modules[0] &&
             cbm_pipeline_c_include_map(graph, "p", rels[0], results[0], &names, &values, &count) ==
                 0;
        /* Exact imported definitions require audited declaration identity.
         * source_signature/FIXED_ARITY are a separate prototype-to-definition
         * linkage contract; its restricted parser does not support inline. */
        const uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_TOP_LEVEL |
                                  CBM_SOURCE_DECL_STATIC | CBM_SOURCE_FUNCTION_DEFINITION;
        char *header_file = cbm_pipeline_fqn_compute("p", rels[1], "__file__");
        bool foo_audited = false, bar_audited = false;
        for (int i = 0; defs && i < def_count; i++) {
            const CBMLSPDef *def = &defs[i];
            const CBMDefinition *extracted = NULL;
            for (int j = 0; j < results[1]->defs.count; j++)
                if (def->qualified_name &&
                    !strcmp(def->qualified_name, results[1]->defs.items[j].qualified_name))
                    extracted = &results[1]->defs.items[j];
            bool audited = extracted && header_file && def->source_file_qn &&
                           !strcmp(def->source_file_qn, header_file) && def->def_module_qn &&
                           !strcmp(def->def_module_qn, results[1]->module_qn) &&
                           def->declaration_identity && def->declaration_identity[0] &&
                           def->graph_qualified_name &&
                           !strcmp(def->graph_qualified_name, extracted->qualified_name) &&
                           def->source_start_line == extracted->start_line &&
                           def->lang == language && (def->source_flags & required) == required;
            if (def->short_name && !strcmp(def->short_name, "foo"))
                foo_audited |= audited;
            if (def->short_name && !strcmp(def->short_name, "bar"))
                bar_audited |= audited;
        }
        free(header_file);
        out.audited = foo_audited && bar_audited;
    }
    out.setup = ok;
    if (ok) {
        cbm_pxc_run_one_with_imports(language, results[0], sources[0], (int)strlen(sources[0]),
                                     modules[0], NULL, defs, def_count, names, values, count, NULL,
                                     snapshot);
        const char *spelling = mode == 1 || mode == 15 ? "copy" : mode == 5 ? "foo" : "fp";
        const char *needle = mode == 1 || mode == 15 ? "copy();" : mode == 5 ? "foo();" : "fp();";
        uint32_t start = (uint32_t)(strstr(sources[0], needle) - sources[0]);
        /* C invocation carriers and producer proofs cover the call expression,
         * including its parentheses but excluding the expression semicolon. */
        uint32_t end = start + (uint32_t)strlen(needle) - 1;
        const char *caller = NULL;
        for (int i = 0; i < results[0]->defs.count; i++)
            if (!strcmp(results[0]->defs.items[i].name, "run"))
                caller = results[0]->defs.items[i].qualified_name;
        for (int i = 0; i < results[0]->calls.count; i++) {
            const CBMCall *carrier = &results[0]->calls.items[i];
            out.carrier |= carrier->source_origin == CBM_SOURCE_ORIGIN_RAW &&
                           carrier->site_start_byte == start && carrier->site_end_byte == end &&
                           caller && carrier->enclosing_func_qn &&
                           !strcmp(carrier->enclosing_func_qn, caller) && carrier->callee_name &&
                           !strcmp(carrier->callee_name, spelling);
        }
        const char *target = NULL;
        const char *expected_name = mode == 9 ? "bar" : "foo";
        for (int i = 0; i < results[1]->defs.count; i++)
            if (!strcmp(results[1]->defs.items[i].name, expected_name))
                target = results[1]->defs.items[i].qualified_name;
        for (int i = 0; i < results[0]->resolved_calls.count; i++) {
            const CBMResolvedCall *row = &results[0]->resolved_calls.items[i];
            if (row->kind != CBM_RESOLVED_INVOCATION || row->site_start_byte != start ||
                row->site_end_byte != end || row->source_origin != CBM_SOURCE_ORIGIN_RAW)
                continue;
            const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(row->evidence.proof);
            out.site_seen = true;
            bool admitted = view && cbm_resolve_admit(&view->outcome) && view->predecessors_closed;
            out.any_admitted |= admitted;
            if (admitted && row->callee_qn)
                snprintf(out.actual_target, sizeof(out.actual_target), "%s", row->callee_qn);
            bool exact = admitted && target && row->callee_qn && !strcmp(target, row->callee_qn) &&
                         view->outcome.target && !strcmp(target, view->outcome.target) && caller &&
                         row->caller_qn && !strcmp(caller, row->caller_qn) &&
                         cbm_resolve_import_source_matches(snapshot, &view->source) &&
                         view->source.scope && !strcmp(caller, view->source.scope) &&
                         view->key.start_byte == start && view->key.end_byte == end &&
                         view->key.origin == CBM_SOURCE_ORIGIN_RAW && view->key.lookup_key &&
                         !strcmp(spelling, view->key.lookup_key) && view->key.detail &&
                         !strcmp("CALLS", view->key.detail);
            out.admitted |= exact;
            if (exact && !call)
                call = cbm_resolve_proof_clone(&retained_proofs, row->evidence.proof);
        }
    }
    if (ok && (!out.audited || (!require_abstention && !out.admitted) ||
               (require_abstention && (!out.carrier || out.any_admitted)))) {
        fprintf(
            stderr,
            "C pointer oracle mode=%d expected=%s audited=%d carrier=%d admitted=%d target=%s\n",
            mode,
            require_abstention ? "abstain"
            : mode == 9        ? "bar"
                               : "foo",
            out.audited, out.carrier, out.any_admitted,
            out.actual_target[0] ? out.actual_target : "<none>");
        authority_callable_diagnostics(mode, results[0], defs, def_count);
    }
    /* Dispatcher scratch already ended; also discard registry, source results
     * and the normalized graph before traversing the retained call closure. */
    cbm_pxc_free_import_map(names, values, count);
    free(defs);
    free(modules[0]);
    free(modules[1]);
    free(paths[0]);
    free(paths[1]);
    cbm_pipeline_namespace_map_free(map);
    cbm_gbuf_free(graph);
    cbm_free_result(results[0]);
    cbm_free_result(results[1]);
    cbm_arena_destroy(&definitions);
    out.cloned = call != NULL;
    if (call) {
        out.closed = cbm_resolve_proof_visit(call, 4096, authority_find_predecessors, &closure);
        out.binding_seen = closure.binding_seen;
        out.dependency_seen = closure.dependency_seen;
    }
    cbm_arena_destroy(&retained);
    if (root)
        th_cleanup(root);
    return out;
}
TEST(resolver_c_direct_import_call_retains_normalized_predecessors) {
    authority_callable_result_t result = authority_c_function_pointer(5);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(result.admitted);
    ASSERT(result.cloned);
    ASSERT(result.closed);
    ASSERT(result.binding_seen);
    ASSERT(result.dependency_seen);
    PASS();
}

TEST(resolver_c_function_pointer_retains_normalized_import_predecessors) {
    authority_callable_result_t result = authority_c_function_pointer(0);
    if (!result.setup || !result.admitted || !result.cloned || !result.closed ||
        !result.binding_seen || !result.dependency_seen)
        fprintf(stderr,
                "C function-pointer lineage: setup=%d admitted=%d cloned=%d closed=%d binding=%d "
                "dependency=%d\n",
                result.setup, result.admitted, result.cloned, result.closed, result.binding_seen,
                result.dependency_seen);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(result.admitted);
    ASSERT(result.cloned);
    ASSERT(result.closed);
    ASSERT(result.binding_seen);
    ASSERT(result.dependency_seen);
    PASS();
}

TEST(resolver_c_function_pointer_copies_and_assignments_retain_import_predecessors) {
    for (int mode = 1; mode <= 2; mode++) {
        authority_callable_result_t result = authority_c_function_pointer(mode);
        if (!result.setup || !result.admitted || !result.cloned || !result.closed ||
            !result.binding_seen || !result.dependency_seen)
            fprintf(stderr,
                    "C function-pointer transfer mode=%d setup=%d admitted=%d cloned=%d "
                    "closed=%d binding=%d dependency=%d\n",
                    mode, result.setup, result.admitted, result.cloned, result.closed,
                    result.binding_seen, result.dependency_seen);
        ASSERT(result.setup);
        ASSERT(result.audited);
        ASSERT(result.carrier);
        ASSERT(result.admitted);
        ASSERT(result.cloned);
        ASSERT(result.closed);
        ASSERT(result.binding_seen);
        ASSERT(result.dependency_seen);
    }
    PASS();
}

TEST(resolver_c_function_pointer_escape_is_lexically_scoped) {
    for (int mode = 6; mode <= 7; mode++) {
        authority_callable_result_t result = authority_c_function_pointer(mode);
        ASSERT(result.setup);
        ASSERT(result.audited);
        ASSERT(result.carrier);
        ASSERT(result.admitted);
        ASSERT(result.cloned);
        ASSERT(result.closed);
        ASSERT(result.binding_seen);
        ASSERT(result.dependency_seen);
    }
    PASS();
}

TEST(resolver_c_function_pointer_escape_survives_reassignment) {
    authority_callable_result_t result = authority_c_function_pointer(3);
    if (!result.setup || !result.carrier || result.any_admitted)
        fprintf(stderr,
                "C escaped pointer: setup=%d carrier=%d resolved_site=%d admitted=%d target=%s\n",
                result.setup, result.carrier, result.site_seen, result.any_admitted,
                result.actual_target[0] ? result.actual_target : "<none>");
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(!result.any_admitted);
    PASS();
}
TEST(resolver_cpp_delayed_capture_does_not_assign_outer_pointer) {
    authority_callable_result_t result = authority_c_function_pointer(4);
    if (!result.setup || !result.carrier || result.any_admitted)
        fprintf(stderr,
                "C++ delayed pointer: setup=%d carrier=%d resolved_site=%d admitted=%d target=%s\n",
                result.setup, result.carrier, result.site_seen, result.any_admitted,
                result.actual_target[0] ? result.actual_target : "<none>");
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(!result.any_admitted);
    PASS();
}

TEST(resolver_c_parenthesized_address_escape_survives_reassignment) {
    authority_callable_result_t result = authority_c_function_pointer(8);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(!result.any_admitted);
    PASS();
}

TEST(resolver_c_parenthesized_assignment_updates_callable) {
    authority_callable_result_t result = authority_c_function_pointer(9);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(result.admitted);
    ASSERT(result.cloned);
    ASSERT(result.closed);
    ASSERT(result.binding_seen);
    ASSERT(result.dependency_seen);
    PASS();
}

TEST(resolver_cpp_parenthesized_reference_initializer_blocks_callable) {
    authority_callable_result_t result = authority_c_function_pointer(10);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(!result.any_admitted);
    PASS();
}

TEST(resolver_cpp_parenthesized_reference_argument_blocks_callable) {
    authority_callable_result_t result = authority_c_function_pointer(11);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(!result.any_admitted);
    PASS();
}

TEST(resolver_c_nested_assignment_effect_does_not_install_inner_target) {
    authority_callable_result_t result = authority_c_function_pointer(12);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(!result.any_admitted);
    PASS();
}

TEST(resolver_c_nested_initializer_effect_does_not_install_inner_target) {
    authority_callable_result_t result = authority_c_function_pointer(13);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(!result.any_admitted);
    PASS();
}

TEST(resolver_cpp_reference_init_capture_blocks_outer_callable) {
    authority_callable_result_t result = authority_c_function_pointer(14);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(!result.any_admitted);
    PASS();
}

TEST(resolver_c_parenthesized_callable_copy_retains_import_predecessors) {
    authority_callable_result_t result = authority_c_function_pointer(15);
    ASSERT(result.setup);
    ASSERT(result.audited);
    ASSERT(result.carrier);
    ASSERT(result.admitted);
    ASSERT(result.cloned);
    ASSERT(result.closed);
    ASSERT(result.binding_seen);
    ASSERT(result.dependency_seen);
    PASS();
}

TEST(resolver_c_import_authority_ignores_projection) {
    authority_result_t result = authority_fixture(false);
    ASSERT(result.setup);
    ASSERT(result.original);
    ASSERT(result.tampered);
    ASSERT(result.deleted);
    ASSERT(result.predecessor);
    PASS();
}
TEST(resolver_c_import_authority_rejects_stale_source) {
    authority_result_t result = authority_fixture(false);
    ASSERT(result.setup);
    ASSERT(result.original);
    ASSERT(result.source_changed);
    ASSERT(result.configuration_changed);
    PASS();
}
TEST(resolver_rust_import_authority_ignores_projection) {
    authority_result_t result = authority_fixture(true);
    ASSERT(result.setup);
    ASSERT(result.original);
    ASSERT(result.tampered);
    ASSERT(result.deleted);
    ASSERT(result.predecessor);
    PASS();
}
TEST(resolver_rust_import_authority_rejects_stale_source) {
    authority_result_t result = authority_fixture(true);
    ASSERT(result.setup);
    ASSERT(result.original);
    ASSERT(result.source_changed);
    ASSERT(result.configuration_changed);
    PASS();
}
SUITE(resolver_import_authority) {
    RUN_TEST(resolver_c_direct_import_call_retains_normalized_predecessors);
    RUN_TEST(resolver_c_function_pointer_retains_normalized_import_predecessors);
    RUN_TEST(resolver_c_function_pointer_copies_and_assignments_retain_import_predecessors);
    RUN_TEST(resolver_c_function_pointer_escape_is_lexically_scoped);
    RUN_TEST(resolver_c_function_pointer_escape_survives_reassignment);
    RUN_TEST(resolver_cpp_delayed_capture_does_not_assign_outer_pointer);
    RUN_TEST(resolver_c_parenthesized_address_escape_survives_reassignment);
    RUN_TEST(resolver_c_parenthesized_assignment_updates_callable);
    RUN_TEST(resolver_cpp_parenthesized_reference_initializer_blocks_callable);
    RUN_TEST(resolver_cpp_parenthesized_reference_argument_blocks_callable);
    RUN_TEST(resolver_c_nested_assignment_effect_does_not_install_inner_target);
    RUN_TEST(resolver_c_nested_initializer_effect_does_not_install_inner_target);
    RUN_TEST(resolver_cpp_reference_init_capture_blocks_outer_callable);
    RUN_TEST(resolver_c_parenthesized_callable_copy_retains_import_predecessors);
    RUN_TEST(resolver_c_import_authority_ignores_projection);
    RUN_TEST(resolver_c_import_authority_rejects_stale_source);
    RUN_TEST(resolver_rust_import_authority_ignores_projection);
    RUN_TEST(resolver_rust_import_authority_rejects_stale_source);
}
