#include "test_framework.h"
#include "pipeline/shared_resolver.h"
#include "graph_buffer/graph_buffer.h"
#include "resolve/policy.h"
#include "resolve/proof.h"
#include "resolve/snapshot.h"

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

/* Synthetic adapter reports still need an explicit proof backed by this
 * fixture's captured source. This is a coarse adapter attestation, including
 * deliberately conflicting reports; it does not invent parsed predecessors. */
static bool attest_semantic_row(CBMFileResult *result, CBMResolvedCall *row, const CBMCall *call) {
    if (!call || row->kind != CBM_RESOLVED_INVOCATION ||
        row->site_start_byte != call->site_start_byte ||
        row->site_end_byte != call->site_end_byte || row->source_origin != call->source_origin ||
        strcmp(row->caller_qn, call->enclosing_func_qn))
        return false;
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &result->arena, 0, 0, 0);
    cbm_resolve_proof_source_t source =
        cbm_result_proof_source(result, row->source_origin, row->caller_qn);
    cbm_resolve_proof_key_t key = {.lookup_domain = row->caller_qn,
                                   .lookup_key = call->callee_name,
                                   .binding_identity = row->callee_qn,
                                   .lookup_closed = true,
                                   .declaration_identity = row->callee_qn,
                                   .start_byte = row->site_start_byte,
                                   .end_byte = row->site_end_byte,
                                   .origin = row->source_origin,
                                   .detail = "CALLS"};
    cbm_resolve_result_t outcome = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                    .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                    .analysis = row->evidence.analysis,
                                    .rule = row->evidence.rule,
                                    .facts = row->evidence.facts,
                                    .target = row->callee_qn,
                                    .candidate_count = 1};
    const cbm_resolve_proof_t *audit =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_ADAPTER_DOMAIN, &source, &key, &outcome);
    if (!audit)
        return false;
    row->evidence.proof = cbm_resolve_proof_derive(&pool, CBM_PROOF_REGISTRY_LOOKUP, &source, &key,
                                                   &outcome, &audit, 1, true);
    return row->evidence.proof != NULL;
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
    ASSERT(cbm_gbuf_resolutions_own_shared(graph));
    ASSERT(cbm_shared_resolver_project(graph));
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
    ASSERT(cbm_gbuf_resolutions_own_shared(graph));
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
            /* This fixture intentionally supplies no shared namespace snapshot.
             * Absence of that resolver is distinct from a closed missing target. */
            ASSERT_STR_EQ(records[r].result.reason, "python_import_namespace_unavailable");
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
        /* Julia has no adapter. This example held Ruby until Ruby got one: see
         * resolver_pipeline_ruby_parenthesized_call_binds_its_top_level_def. */
        "function target()\n 1\nend\nfunction caller()\n target()\nend\n"};
    for (int example = 0; example < 3; example++) {
        CBMLanguage language = example == 2 ? CBM_LANG_JULIA : CBM_LANG_PYTHON;
        const char *rel = example == 2 ? "main.jl" : "main.py";
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

/* This test used to pin Ruby's `target()` at no edge because methods are dynamic
 * and a bare identifier may be a local. Neither reason applies to this form.
 * Ruby's parser reads an identifier as a local only when it has no arguments and
 * no parentheses and an assignment precedes it, so `target()` is a method call by
 * the grammar. A top-level `def` defines a private method of Object, which every
 * object inherits, so with an implicit receiver the static rule selects it. A
 * class overriding it at runtime is dispatch, which CALLS does not model. What
 * does stop the rule is another definition of that name, computed or literal. */
static int ruby_target_edges(const char *source, const char *rel) {
    CBMFileResult *result =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUBY, "proj", rel, 0, NULL, NULL);
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    int edges = -1;
    if (result && graph && registry && add_result(graph, registry, result, rel, source)) {
        cbm_pipeline_ctx_t ctx = {
            .project_name = "proj", .repo_path = "/tmp", .gbuf = graph, .registry = registry};
        cbm_resolver_file_t input = {.ctx = &ctx,
                                     .registry = registry,
                                     .graph = graph,
                                     .output = graph,
                                     .result = result,
                                     .rel_path = rel,
                                     .module_qn = result->module_qn,
                                     .language = CBM_LANG_RUBY};
        if (cbm_shared_resolver_file(NULL, &input)) {
            size_t count = 0;
            const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
            edges = 0;
            for (size_t r = 0; r < count; r++)
                edges += records[r].emitted && !strcmp(records[r].relation, "CALLS");
        }
    }
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(result);
    return edges;
}

TEST(resolver_pipeline_ruby_parenthesized_call_binds_its_top_level_def) {
    ASSERT_EQ(ruby_target_edges("def target\n 1\nend\ndef caller\n target()\nend\n", "main.rb"), 1);
    ASSERT_EQ(ruby_target_edges("def target\n 1\nend\ndef caller\n target 1\nend\n", "main.rb"), 1);
    /* A local of that name cannot capture a call with parentheses. */
    ASSERT_EQ(
        ruby_target_edges("def target\n 1\nend\ndef caller\n target = 2\n target()\nend\n", "main.rb"),
        1);
    PASS();
}

TEST(resolver_pipeline_ruby_redefinition_and_metaprogramming_abstain) {
    const char *const sources[] = {
        /* Two definitions of one name. */
        "def target\n 1\nend\ndef target\n 2\nend\ndef caller\n target()\nend\n",
        /* `define_method` replaces the literal name in the class. */
        "class A\n def target\n 1\n end\n define_method(:target) { 2 }\n def caller\n target()\n "
        "end\nend\n",
        /* An attribute macro declares it too. */
        "class A\n attr_reader :target\n def target\n 1\n end\n def caller\n target()\n "
        "end\nend\n",
        /* A prepended module outside the project comes before the class. */
        "class A\n prepend Outside\n def target\n 1\n end\n def caller\n target()\n end\nend\n",
        /* instance_eval runs the block with another self. */
        "class A\n def target\n 1\n end\n def caller(o)\n o.instance_eval { target() }\n "
        "end\nend\n",
        /* A DSL block in a class body runs against whatever the DSL chooses. */
        "class A\n def self.target\n 1\n end\n before do\n target()\n end\nend\n",
        /* An unknown superclass may declare a name the class does not. */
        "class A < Outside\n def caller\n target()\n end\nend\ndef target\n 1\nend\n",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++)
        ASSERT_EQ(ruby_target_edges(sources[i], "main.rb"), 0);
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
        .evidence = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                     .rule = CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                     .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                              CBM_RESOLVE_FACT_LANGUAGE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT}};
    ASSERT(attest_semantic_row(cache[1], &semantic, call));
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

static int resolver_pipeline_duplicate_semantics(int example, bool reverse) {
    const char *source = "void target(void) {}\nvoid decoy(void) {}\n"
                         "void caller(void) { target(); }\n";
    CBMFileResult *result =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_C, "proj", "main.c", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    ASSERT_EQ(result->calls.count, 1);
    const CBMCall *call = &result->calls.items[0];
    CBMResolvedCall rows[2] = {
        {.caller_qn = call->enclosing_func_qn,
         .callee_qn = "proj.main.target",
         .site_start_byte = call->site_start_byte,
         .site_end_byte = call->site_end_byte,
         .source_origin = call->source_origin,
         .kind = CBM_RESOLVED_INVOCATION,
         .evidence = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                      .rule = CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                      .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                               CBM_RESOLVE_FACT_LANGUAGE_BOUND |
                               CBM_RESOLVE_FACT_OCCURRENCE_EXACT}}};
    rows[1] = rows[0];
    if (example == 0)
        rows[1].evidence.facts &= ~CBM_RESOLVE_FACT_LANGUAGE_BOUND;
    else if (example == 1)
        rows[1].evidence.facts &= ~CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    else if (example == 2)
        rows[1].callee_qn = "proj.main.decoy";
    else {
        rows[1].evidence.rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
        rows[1].evidence.facts &= ~CBM_RESOLVE_FACT_LANGUAGE_BOUND;
        rows[1].evidence.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    }
    ASSERT(attest_semantic_row(result, &rows[0], call));
    ASSERT(attest_semantic_row(result, &rows[1], call));
    result->resolved_calls.count = 0;
    cbm_resolvedcall_push(&result->resolved_calls, &result->arena, rows[reverse ? 1 : 0]);
    cbm_resolvedcall_push(&result->resolved_calls, &result->arena, rows[reverse ? 0 : 1]);
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
    ASSERT(cbm_gbuf_resolutions_own_shared(graph));
    ASSERT(cbm_shared_resolver_project(graph));
    ASSERT_EQ(emitted_target(graph, "CALLS", "proj.main.target"), example == 2 ? 0 : 1);
    ASSERT_EQ(emitted_target(graph, "CALLS", "proj.main.decoy"), 0);
    size_t count = 0;
    const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
    int calls = 0;
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_record_t *record = &records[i];
        if (strcmp(record->relation, "CALLS"))
            continue;
        calls++;
        if (example == 2) {
            ASSERT_FALSE(record->emitted);
            ASSERT_EQ(record->result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
            ASSERT_STR_EQ(record->result.reason, "conflicting_semantic_targets");
        } else {
            ASSERT(record->emitted);
            ASSERT_EQ(record->result.rule, example == 3 ? CBM_RESOLVE_RULE_SCOPE_BINDING
                                                        : CBM_RESOLVE_RULE_LANGUAGE_BINDING);
        }
    }
    cbm_shared_resolver_free(shared);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(result);
    ASSERT_EQ(calls, 1);
    PASS();
}

TEST(resolver_pipeline_duplicate_semantics_are_order_independent) {
    for (int example = 0; example < 4; example++)
        for (int reverse = 0; reverse < 2; reverse++)
            ASSERT_EQ(resolver_pipeline_duplicate_semantics(example, reverse != 0), 0);
    PASS();
}

TEST(resolver_pipeline_python_default_keeps_class_target_and_body_keeps_module_target) {
    const char *source = "def target():\n    return 1\n"
                         "class C:\n    def target():\n        return 2\n"
                         "    def method(value=target()):\n        target()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "proj",
                                             "main.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    /* Exercise the shared source-binding path independently of hybrid rows. */
    result->resolved_calls.count = 0;
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(graph);
    ASSERT_NOT_NULL(registry);
    ASSERT(add_result(graph, registry, result, "main.py", source));
    cbm_pipeline_ctx_t ctx = {
        .project_name = "proj", .repo_path = "/tmp", .gbuf = graph, .registry = registry};
    cbm_file_info_t file = {
        .path = "/tmp/main.py", .rel_path = "main.py", .language = CBM_LANG_PYTHON};
    cbm_shared_resolver_t *shared = cbm_shared_resolver_build(&ctx, &file, 1, &result);
    ASSERT_NOT_NULL(shared);
    cbm_resolver_file_t input = {.ctx = &ctx,
                                 .registry = registry,
                                 .graph = graph,
                                 .output = graph,
                                 .result = result,
                                 .rel_path = "main.py",
                                 .module_qn = result->module_qn,
                                 .language = CBM_LANG_PYTHON};
    ASSERT(cbm_shared_resolver_file(shared, &input));
    uint32_t body = (uint32_t)(strstr(source, "        target()") - source);
    size_t count = 0;
    const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
    int calls = 0;
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_record_t *record = &records[i];
        if (strcmp(record->relation, "CALLS") || strcmp(record->spelling, "target"))
            continue;
        ASSERT(record->emitted);
        ASSERT_STR_EQ(record->result.target,
                      record->start_byte < body ? "proj.main.C.target" : "proj.main.target");
        calls++;
    }
    ASSERT(cbm_gbuf_resolutions_own_shared(graph));
    ASSERT(cbm_shared_resolver_project(graph));
    ASSERT_EQ(emitted_target(graph, "CALLS", "proj.main.C.target"), 1);
    ASSERT_EQ(emitted_target(graph, "CALLS", "proj.main.target"), 1);
    cbm_shared_resolver_free(shared);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(result);
    ASSERT_EQ(calls, 2);
    PASS();
}

TEST(resolver_pipeline_changed_source_cannot_reuse_old_occurrence_proof) {
    const char *source = "void target(void) {}\nvoid caller(void) { target(); }\n";
    const char *changed = "void decoy_(void) {}\nvoid caller(void) { decoy_(); }\n";
    ASSERT_EQ(strlen(source), strlen(changed));
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
                                 .language = CBM_LANG_C,
                                 .source = changed,
                                 .source_len = strlen(changed)};
    ASSERT(cbm_shared_resolver_file(shared, &input));
    ASSERT(cbm_gbuf_resolutions_own_shared(graph));
    ASSERT(cbm_shared_resolver_project(graph));
    int emitted = emitted_target(graph, "CALLS", "proj.main.target");
    size_t count = 0;
    const cbm_resolve_record_t *rows = cbm_gbuf_resolutions(graph, &count);
    int failed = 0;
    for (size_t i = 0; i < count; i++) {
        if (!strcmp(rows[i].relation, "CALLS") && !strcmp(rows[i].spelling, "target")) {
            ASSERT_FALSE(rows[i].emitted);
            ASSERT_STR_EQ(rows[i].result.reason, "source_snapshot_mismatch");
            failed++;
        }
    }
    cbm_shared_resolver_free(shared);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(result);
    ASSERT_EQ(emitted, 0);
    ASSERT_EQ(failed, 1);
    PASS();
}

static bool go_shared_build_succeeds(size_t name_bytes) {
    enum { FUNCTIONS = 400, LINE_BYTES = 80 };
    char *project = malloc(name_bytes + 1);
    char *source = malloc((size_t)FUNCTIONS * LINE_BYTES + 128);
    if (!project || !source) {
        free(project);
        free(source);
        return false;
    }
    memset(project, 'p', name_bytes);
    project[name_bytes] = '\0';
    size_t used = (size_t)snprintf(source, 128,
                                   "package main\n\nimport \"fmt\"\n\n"
                                   "func base(x int) int { return x }\n");
    for (int i = 0; i < FUNCTIONS; i++)
        used += (size_t)snprintf(source + used, LINE_BYTES,
                                 "func f%d(a int) int { v := base(a); fmt.Println(v); return v }\n",
                                 i);
    CBMFileResult *result =
        cbm_extract_file(source, (int)used, CBM_LANG_GO, project, "module.go", 0, NULL, NULL);
    cbm_gbuf_t *graph = cbm_gbuf_new(project, "/tmp");
    cbm_registry_t *registry = cbm_registry_new();
    bool built = false;
    if (result && graph && registry) {
        result->source = source;
        result->source_len = (int)used;
        for (int d = 0; d < result->defs.count; d++) {
            const CBMDefinition *def = &result->defs.items[d];
            if (!def->qualified_name || !def->name || !def->label)
                continue;
            cbm_gbuf_upsert_node(graph, def->label, def->name, def->qualified_name, "module.go",
                                 def->start_line, def->end_line, "{}");
            cbm_registry_add(registry, def->name, def->qualified_name, def->label);
        }
        cbm_file_info_t file = {
            .path = "/tmp/module.go", .rel_path = "module.go", .language = CBM_LANG_GO};
        cbm_pipeline_ctx_t ctx = {
            .project_name = project, .repo_path = "/tmp", .gbuf = graph, .registry = registry};
        cbm_shared_resolver_t *shared = cbm_shared_resolver_build(&ctx, &file, 1, &result);
        built = shared != NULL;
        cbm_shared_resolver_free(shared);
    }
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    if (result)
        cbm_free_result(result);
    free(source);
    free(project);
    return built;
}

/* A project's proof work is its fact count times the text in each fact, including the
 * project name. django's 7000 files reached 133M against a fixed 64M budget and lost
 * publication entirely. The name buys the same work with one small file, and the short
 * name is the control: only the amount of work differs between these two builds. */
TEST(resolver_pipeline_shared_build_has_no_proof_work_budget) {
    ASSERT(go_shared_build_succeeds(4));
    ASSERT(go_shared_build_succeeds(4000));
    PASS();
}

/* The graph-wide pool grows the same way. django used 1.36G of the 4G it was allowed,
 * so a project three times that size lost publication. A generation past that point
 * must keep recording; only the memory core may refuse it. */
TEST(resolver_pipeline_graph_pool_has_no_proof_work_budget) {
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(graph);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_proof_source_t source = {.project = "proj", .file = "main.py"};
    cbm_resolve_proof_key_t key = {.lookup_domain = "proj.caller", .lookup_key = "target"};
    cbm_resolve_result_t outcome = {.binding = CBM_RESOLVE_BINDING_UNRESOLVED,
                                    .domain = CBM_RESOLVE_DOMAIN_UNKNOWN,
                                    .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
    const cbm_resolve_proof_t *proof =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_ADAPTER_DOMAIN, &source, &key, &outcome);
    ASSERT_NOT_NULL(proof);
    cbm_gbuf_test_add_resolution_proof_work(graph, UINT64_C(4294967296));
    outcome.proof = proof;
    cbm_resolve_record_t record = {.file_path = "main.py",
                                   .scope = "proj.caller",
                                   .relation = "CALLS",
                                   .spelling = "target",
                                   .result = outcome};
    bool recorded = cbm_gbuf_record_resolution(graph, &record);
    bool complete = cbm_gbuf_resolutions_complete(graph);
    cbm_gbuf_free(graph);
    cbm_arena_destroy(&arena);
    ASSERT(recorded);
    ASSERT(complete);
    PASS();
}

/* Infrastructure adapters keep one pool per project, not per file. kubernetes' 6456
 * YAML files spent the 4M they were allowed, and every occurrence after that failed
 * the whole generation. */
TEST(resolver_pipeline_infra_adapter_pool_has_no_proof_work_budget) {
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(graph);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_pipeline_ctx_t ctx = {.project_name = "proj", .repo_path = "/tmp", .gbuf = graph};
    cbm_shared_resolver_adapter_t adapter;
    ASSERT(cbm_shared_resolver_adapter_init(&adapter, &ctx, &arena));
    adapter.proofs.work = UINT64_C(4194304);
    cbm_resolve_proof_source_t source = {.project = "proj", .file = "manifest.yaml"};
    cbm_resolve_proof_key_t key = {.lookup_domain = "manifest.yaml", .lookup_key = "credentials"};
    cbm_resolve_result_t outcome = {.binding = CBM_RESOLVE_BINDING_UNRESOLVED,
                                    .domain = CBM_RESOLVE_DOMAIN_UNKNOWN,
                                    .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
    const cbm_resolve_proof_t *proof =
        cbm_resolve_proof_leaf(&adapter.proofs, CBM_PROOF_SOURCE_FACT, &source, &key, &outcome);
    bool created = proof != NULL;
    cbm_shared_resolver_adapter_dispose(&adapter);
    cbm_gbuf_free(graph);
    cbm_arena_destroy(&arena);
    ASSERT(created);
    PASS();
}

SUITE(resolver_pipeline) {
    RUN_TEST(resolver_pipeline_shared_build_has_no_proof_work_budget);
    RUN_TEST(resolver_pipeline_graph_pool_has_no_proof_work_budget);
    RUN_TEST(resolver_pipeline_infra_adapter_pool_has_no_proof_work_budget);
    RUN_TEST(resolver_pipeline_changed_source_cannot_reuse_old_occurrence_proof);
    RUN_TEST(resolver_pipeline_duplicate_semantics_are_order_independent);
    RUN_TEST(resolver_pipeline_python_default_keeps_class_target_and_body_keeps_module_target);
    RUN_TEST(resolver_pipeline_c_declaration_evidence_survives_extraction);
    RUN_TEST(resolver_pipeline_source_identity_cannot_cross_graph_collision);
    RUN_TEST(resolver_pipeline_semantic_proof_cannot_cross_graph_identity_collision);
    RUN_TEST(resolver_pipeline_import_proof_resolves_call_and_value_without_namesake);
    RUN_TEST(resolver_pipeline_missing_import_records_quality_gap_without_local_substitute);
    RUN_TEST(resolver_pipeline_lua_local_function_uses_shared_binding);
    RUN_TEST(resolver_pipeline_shadow_ambiguity_and_unsupported_are_recorded);
    RUN_TEST(resolver_pipeline_ruby_parenthesized_call_binds_its_top_level_def);
    RUN_TEST(resolver_pipeline_ruby_redefinition_and_metaprogramming_abstain);
    RUN_TEST(resolver_pipeline_projection_refuses_fabricated_emitted_claim);
}
