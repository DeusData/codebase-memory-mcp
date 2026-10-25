#include "test_framework.h"
#include "pipeline/shared_resolver.h"
#include "graph_buffer/graph_buffer.h"
#include "resolve/policy.h"

#include <stdlib.h>
#include <string.h>

static bool add_result(cbm_gbuf_t *graph, cbm_registry_t *registry, CBMFileResult *result,
                       const char *rel, const char *source) {
    result->source = source;
    result->source_len = (int)strlen(source);
    char *file_qn = cbm_pipeline_fqn_compute("proj", rel, "__file__");
    bool ok = file_qn && cbm_gbuf_upsert_node(graph, "File", rel, file_qn, rel, 1, 1, "{}");
    free(file_qn);
    for (int d = 0; ok && d < result->defs.count; d++) {
        const CBMDefinition *def = &result->defs.items[d];
        if (!def->qualified_name || !def->name || !def->label)
            continue;
        ok = cbm_gbuf_upsert_node(graph, def->label, def->name, def->qualified_name, rel,
                                  def->start_line, def->end_line, "{}") != 0;
        cbm_registry_add(registry, def->name, def->qualified_name, def->label);
    }
    return ok;
}

static int emitted_target(const cbm_gbuf_t *graph, const char *relation, const char *target) {
    size_t count = 0;
    const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
    int matched = 0;
    for (size_t r = 0; r < count; r++) {
        if (records[r].emitted && strcmp(records[r].relation, relation) == 0 &&
            records[r].result.target && strcmp(records[r].result.target, target) == 0)
            matched++;
    }
    return matched;
}

TEST(resolver_pipeline_c_declaration_evidence_survives_extraction) {
    const char *source = "void target(void) {}\nvoid caller(void) { target(); }\n";
    CBMFileResult *result =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_C, "proj", "main.c", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(graph);
    ASSERT_NOT_NULL(registry);
    ASSERT(add_result(graph, registry, result, "main.c", source));
    cbm_pipeline_ctx_t ctx = {
        .project_name = "proj", .repo_path = "/tmp", .gbuf = graph, .registry = registry};
    cbm_file_info_t file = {.path = "/tmp/main.c", .rel_path = "main.c", .language = CBM_LANG_C};
    cbm_shared_resolver_t *shared = cbm_shared_resolver_build(&ctx, &file, 1, &result);
    ASSERT_NOT_NULL(shared);
    cbm_resolver_file_t input = {.ctx = &ctx,
                                 .registry = registry,
                                 .graph = graph,
                                 .output = graph,
                                 .result = result,
                                 .rel_path = "main.c",
                                 .module_qn = result->module_qn,
                                 .language = CBM_LANG_C};
    ASSERT(cbm_shared_resolver_file(shared, &input));
    int emitted = emitted_target(graph, "CALLS", "proj.main.target");
    if (emitted != 1) {
        for (int i = 0; i < result->resolved_calls.count; i++) {
            const CBMResolvedCall *row = &result->resolved_calls.items[i];
            fprintf(stderr, "C evidence %s -> %s [%u,%u] origin=%u kind=%u facts=%u\n",
                    row->caller_qn, row->callee_qn, row->site_start_byte, row->site_end_byte,
                    row->source_origin, row->kind, row->evidence.facts);
        }
        size_t count = 0;
        const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
        for (size_t i = 0; i < count; i++)
            fprintf(stderr, "C occurrence %s %s [%u,%u] origin=%u reason=%s\n", records[i].scope,
                    records[i].spelling, records[i].start_byte, records[i].end_byte,
                    records[i].source_origin, records[i].result.reason);
    }
    cbm_shared_resolver_free(shared);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(result);
    ASSERT_EQ(emitted, 1);
    PASS();
}

TEST(resolver_pipeline_import_proof_resolves_call_and_value_without_namesake) {
    const char *library = "def target():\n    return 1\n";
    const char *source = "from library import target\n\ndef caller():\n    return target()\n"
                         "\ndef reference():\n    return target\n";
    CBMFileResult *cache[] = {cbm_extract_file(library, (int)strlen(library), CBM_LANG_PYTHON,
                                               "proj", "library.py", 0, NULL, NULL),
                              cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "proj",
                                               "main.py", 0, NULL, NULL)};
    ASSERT_NOT_NULL(cache[0]);
    ASSERT_NOT_NULL(cache[1]);
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(graph);
    ASSERT_NOT_NULL(registry);
    ASSERT(add_result(graph, registry, cache[0], "library.py", library));
    ASSERT(add_result(graph, registry, cache[1], "main.py", source));
    cbm_registry_add(registry, "target", "proj.decoy.target", "Function");
    cbm_gbuf_upsert_node(graph, "Function", "target", "proj.decoy.target", "decoy.py", 1, 2, "{}");
    cbm_file_info_t files[] = {
        {.path = "/tmp/library.py", .rel_path = "library.py", .language = CBM_LANG_PYTHON},
        {.path = "/tmp/main.py", .rel_path = "main.py", .language = CBM_LANG_PYTHON}};
    cbm_pipeline_ctx_t ctx = {
        .project_name = "proj", .repo_path = "/tmp", .gbuf = graph, .registry = registry};
    cbm_shared_resolver_t *shared = cbm_shared_resolver_build(&ctx, files, 2, cache);
    ASSERT_NOT_NULL(shared);
    const char *keys[] = {"target"}, *values[] = {"proj.library.target"};
    cbm_resolver_file_t input = {.ctx = &ctx,
                                 .registry = registry,
                                 .graph = graph,
                                 .output = graph,
                                 .result = cache[1],
                                 .rel_path = "main.py",
                                 .module_qn = cache[1]->module_qn,
                                 .language = CBM_LANG_PYTHON,
                                 .import_keys = keys,
                                 .import_values = values,
                                 .import_count = 1};
    ASSERT(cbm_shared_resolver_file(shared, &input));
    ASSERT_EQ(emitted_target(graph, "CALLS", "proj.library.target"), 1);
    ASSERT_EQ(emitted_target(graph, "USAGE", "proj.library.target"), 1);
    ASSERT_EQ(emitted_target(graph, "CALLS", "proj.decoy.target"), 0);
    const cbm_gbuf_node_t *caller = cbm_gbuf_find_by_qn(graph, "proj.main.caller");
    const cbm_gbuf_node_t *decoy = cbm_gbuf_find_by_qn(graph, "proj.decoy.target");
    ASSERT_NOT_NULL(caller);
    ASSERT_NOT_NULL(decoy);
    ASSERT(cbm_gbuf_insert_edge(graph, caller->id, decoy->id, "CALLS", "{}"));
    ASSERT(cbm_shared_resolver_project(graph));
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "CALLS"), 1);
    ASSERT(cbm_shared_resolver_project(graph));
    ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "CALLS"), 1);
    cbm_shared_resolver_free(shared);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(cache[0]);
    cbm_free_result(cache[1]);
    PASS();
}

TEST(resolver_pipeline_missing_import_records_quality_gap_without_local_substitute) {
    const char *source = "from missing import target\ndef caller():\n    return target()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "proj",
                                             "main.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(graph);
    ASSERT_NOT_NULL(registry);
    ASSERT(add_result(graph, registry, result, "main.py", source));
    cbm_registry_add(registry, "target", "proj.main.target", "Function");
    cbm_gbuf_upsert_node(graph, "Function", "target", "proj.main.target", "main.py", 1, 1, "{}");
    cbm_pipeline_ctx_t ctx = {
        .project_name = "proj", .repo_path = "/tmp", .gbuf = graph, .registry = registry};
    const char *keys[] = {"target"}, *values[] = {""};
    cbm_resolver_file_t input = {.ctx = &ctx,
                                 .registry = registry,
                                 .graph = graph,
                                 .output = graph,
                                 .result = result,
                                 .rel_path = "main.py",
                                 .module_qn = result->module_qn,
                                 .language = CBM_LANG_PYTHON,
                                 .import_keys = keys,
                                 .import_values = values,
                                 .import_count = 1};
    ASSERT(cbm_shared_resolver_file(NULL, &input));
    ASSERT_EQ(emitted_target(graph, "CALLS", "proj.main.target"), 0);
    size_t count = 0;
    const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
    int misses = 0;
    for (size_t r = 0; r < count; r++) {
        if (strcmp(records[r].relation, "CALLS") == 0 &&
            strcmp(records[r].spelling, "target") == 0) {
            ASSERT_FALSE(records[r].emitted);
            ASSERT_STR_EQ(records[r].result.reason, "import_target_unresolved");
            misses++;
        }
    }
    ASSERT_EQ(misses, 1);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_pipeline_lua_local_function_uses_shared_binding) {
    const char *source = "local function target()\n return 1\nend\n"
                         "local function caller()\n return target()\nend\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_LUA, "proj",
                                             "main.lua", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(graph);
    ASSERT_NOT_NULL(registry);
    ASSERT(add_result(graph, registry, result, "main.lua", source));
    cbm_pipeline_ctx_t ctx = {
        .project_name = "proj", .repo_path = "/tmp", .gbuf = graph, .registry = registry};
    cbm_resolver_file_t input = {.ctx = &ctx,
                                 .registry = registry,
                                 .graph = graph,
                                 .output = graph,
                                 .result = result,
                                 .rel_path = "main.lua",
                                 .module_qn = result->module_qn,
                                 .language = CBM_LANG_LUA};
    ASSERT(cbm_shared_resolver_file(NULL, &input));
    ASSERT_EQ(emitted_target(graph, "CALLS", "proj.main.target"), 1);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_pipeline_shadow_ambiguity_and_unsupported_are_recorded) {
    const char *sources[] = {
        "def target():\n    return 1\ndef caller(target):\n    return target()\n",
        ("def target():\n    return 1\ndef target():\n    return 2\ndef caller():\n    return "
         "target()\n"),
        "def target\n 1\nend\ndef caller\n target()\nend\n"};
    for (int example = 0; example < 3; example++) {
        CBMLanguage language = example == 2 ? CBM_LANG_RUBY : CBM_LANG_PYTHON;
        const char *rel = example == 2 ? "main.rb" : "main.py";
        CBMFileResult *result = cbm_extract_file(sources[example], (int)strlen(sources[example]),
                                                 language, "proj", rel, 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
        cbm_registry_t *registry = cbm_registry_new();
        ASSERT_NOT_NULL(graph);
        ASSERT_NOT_NULL(registry);
        ASSERT(add_result(graph, registry, result, rel, sources[example]));
        cbm_pipeline_ctx_t ctx = {
            .project_name = "proj", .repo_path = "/tmp", .gbuf = graph, .registry = registry};
        cbm_resolver_file_t input = {.ctx = &ctx,
                                     .registry = registry,
                                     .graph = graph,
                                     .output = graph,
                                     .result = result,
                                     .rel_path = rel,
                                     .module_qn = result->module_qn,
                                     .language = language};
        ASSERT(cbm_shared_resolver_file(NULL, &input));
        size_t count = 0;
        const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
        int misses = 0;
        for (size_t r = 0; r < count; r++) {
            if (strcmp(records[r].relation, "CALLS") == 0 &&
                strcmp(records[r].spelling, "target") == 0) {
                ASSERT_FALSE(records[r].emitted);
                ASSERT_FALSE(cbm_resolve_admit(&records[r].result));
                misses++;
            }
        }
        ASSERT_EQ(misses, 1);
        ASSERT(cbm_shared_resolver_project(graph));
        ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "CALLS"), 0);
        cbm_registry_free(registry);
        cbm_gbuf_free(graph);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_pipeline_projection_refuses_fabricated_emitted_claim) {
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(graph);
    cbm_gbuf_upsert_node(graph, "Function", "caller", "proj.caller", "main.rb", 1, 2, "{}");
    cbm_gbuf_upsert_node(graph, "Function", "target", "proj.target", "other.rb", 1, 2, "{}");
    cbm_resolve_record_t record = {.file_path = "main.rb",
                                   .scope = "proj.caller",
                                   .relation = "CALLS",
                                   .spelling = "target",
                                   .emitted = true,
                                   .result = {.target = "proj.target"}};
    ASSERT(cbm_gbuf_record_resolution(graph, &record));
    ASSERT_FALSE(cbm_shared_resolver_project(graph));
    cbm_gbuf_free(graph);
    PASS();
}

static int resolver_pipeline_identity_collision(bool source_collision) {
    const char *library = "def target():\n    return 1\n";
    const char *source = "def caller():\n    return target()\n";
    CBMFileResult *cache[] = {cbm_extract_file(library, (int)strlen(library), CBM_LANG_PYTHON,
                                               "proj", "library.py", 0, NULL, NULL),
                              cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "proj",
                                               "main.py", 0, NULL, NULL)};
    ASSERT_NOT_NULL(cache[0]);
    ASSERT_NOT_NULL(cache[1]);
    ASSERT_GT(cache[1]->calls.count, 0);
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(graph);
    ASSERT_NOT_NULL(registry);
    ASSERT(add_result(graph, registry, cache[0], "library.py", library));
    ASSERT(add_result(graph, registry, cache[1], "main.py", source));
    const CBMCall *call = &cache[1]->calls.items[0];
    CBMResolvedCall semantic = {
        .caller_qn = call->enclosing_func_qn,
        .callee_qn = "proj.library.target",
        .site_start_byte = call->site_start_byte,
        .site_end_byte = call->site_end_byte,
        .source_origin = call->source_origin,
        .evidence = {CBM_RESOLVE_ANALYSIS_COMPLETE, CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                     CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                         CBM_RESOLVE_FACT_LANGUAGE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT}};
    cbm_resolvedcall_push(&cache[1]->resolved_calls, &cache[1]->arena, semantic);
    cbm_file_info_t files[] = {
        {.path = "/tmp/library.py", .rel_path = "library.py", .language = CBM_LANG_PYTHON},
        {.path = "/tmp/main.py", .rel_path = "main.py", .language = CBM_LANG_PYTHON}};
    cbm_pipeline_ctx_t ctx = {
        .project_name = "proj", .repo_path = "/tmp", .gbuf = graph, .registry = registry};
    cbm_shared_resolver_t *shared = cbm_shared_resolver_build(&ctx, files, 2, cache);
    ASSERT_NOT_NULL(shared);
    const char *collision = source_collision ? call->enclosing_func_qn : "proj.library.target";
    ASSERT(cbm_gbuf_upsert_node(graph, "Function", source_collision ? "caller" : "target",
                                collision, "decoy.py", 1, 2, "{}"));
    const cbm_gbuf_node_t *materialized = cbm_gbuf_find_by_qn(graph, collision);
    ASSERT_NOT_NULL(materialized);
    ASSERT_STR_EQ(materialized->file_path, "decoy.py");
    cbm_resolver_file_t input = {.ctx = &ctx,
                                 .registry = registry,
                                 .graph = graph,
                                 .output = graph,
                                 .result = cache[1],
                                 .rel_path = "main.py",
                                 .module_qn = cache[1]->module_qn,
                                 .language = CBM_LANG_PYTHON};
    ASSERT(cbm_shared_resolver_file(shared, &input));
    int emitted = emitted_target(graph, "CALLS", "proj.library.target");
    cbm_shared_resolver_free(shared);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(cache[0]);
    cbm_free_result(cache[1]);
    ASSERT_EQ(emitted, 0);
    PASS();
}
TEST(resolver_pipeline_semantic_proof_cannot_cross_graph_identity_collision) {
    return resolver_pipeline_identity_collision(false);
}
TEST(resolver_pipeline_source_identity_cannot_cross_graph_collision) {
    return resolver_pipeline_identity_collision(true);
}

SUITE(resolver_pipeline) {
    RUN_TEST(resolver_pipeline_c_declaration_evidence_survives_extraction);
    RUN_TEST(resolver_pipeline_source_identity_cannot_cross_graph_collision);
    RUN_TEST(resolver_pipeline_semantic_proof_cannot_cross_graph_identity_collision);
    RUN_TEST(resolver_pipeline_import_proof_resolves_call_and_value_without_namesake);
    RUN_TEST(resolver_pipeline_missing_import_records_quality_gap_without_local_substitute);
    RUN_TEST(resolver_pipeline_lua_local_function_uses_shared_binding);
    RUN_TEST(resolver_pipeline_shadow_ambiguity_and_unsupported_are_recorded);
    RUN_TEST(resolver_pipeline_projection_refuses_fabricated_emitted_claim);
}
