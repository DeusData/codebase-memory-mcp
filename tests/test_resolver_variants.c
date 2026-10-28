/* Canonical graph identity is not declaration-selection authority. These
 * fixtures independently require both explicit body calls, while the incoming
 * conditional binding remains a measured resolver quality gap. */
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/shared_resolver.h"
#include "pipeline/pipeline.h"
#include "resolve/policy.h"
#include "resolver_quality.h"

static const char VARIANT_SOURCE[] = "import sys\n"
                                     "def alpha(): pass\n"
                                     "def beta(): pass\n"
                                     "if sys.platform == 'win32':\n"
                                     "    def pick():\n"
                                     "        alpha()\n"
                                     "else:\n"
                                     "    def pick():\n"
                                     "        beta()\n"
                                     "def caller():\n"
                                     "    pick()\n";

typedef enum {
    VARIANT_UNMODIFIED,
    VARIANT_BAD_PROPERTIES,
    VARIANT_SAME_LINE_DECLARATIONS,
    VARIANT_FOREIGN_SPAN,
    VARIANT_WRONG_ORIGIN,
    VARIANT_CHANGED_CONFIGURATION,
    VARIANT_MISSING_MEMBERSHIP,
    VARIANT_RELOADED_SOURCE,
    VARIANT_RELOADED_SOURCE_CHANGED,
    VARIANT_RELEASED_TREE,
    VARIANT_RELEASED_TREE_CHANGED
} variant_mutation_t;

typedef struct {
    size_t alpha, beta, incoming, other;
    size_t owner_proofs;
} variant_counts_t;

static bool variant_add(cbm_gbuf_t *graph, cbm_registry_t *registry, CBMFileResult *result) {
    char *file_qn = cbm_pipeline_fqn_compute("proj", "main.py", "__file__");
    bool ok =
        file_qn && cbm_gbuf_upsert_node(graph, "File", "main.py", file_qn, "main.py", 1, 11, "{}");
    free(file_qn);
    for (int i = 0; ok && i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        if (!definition->qualified_name || !definition->name || !definition->label)
            continue;
        ok = cbm_gbuf_upsert_node(graph, definition->label, definition->name,
                                  definition->qualified_name, "main.py", definition->start_line,
                                  definition->end_line, "{}") != 0;
        cbm_registry_add(registry, definition->name, definition->qualified_name, definition->label);
    }
    return ok;
}

static bool variant_has_owner_proof(const cbm_resolve_proof_t *proof, unsigned depth) {
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof);
    if (!view || depth >= 32)
        return false;
    if (view->kind == CBM_PROOF_SOURCE_FACT && view->key.detail &&
        !strcmp(view->key.detail, "raw_python_if_else_canonical_owner"))
        return true;
    for (size_t i = 0; i < view->premise_count; i++)
        if (variant_has_owner_proof(view->premises[i], depth + 1))
            return true;
    return false;
}

static bool variant_run(const char *source, variant_mutation_t mutation, variant_counts_t *counts) {
    *counts = (variant_counts_t){0};
    bool released = mutation == VARIANT_RELEASED_TREE || mutation == VARIANT_RELEASED_TREE_CHANGED;
    bool reload = released || mutation == VARIANT_RELOADED_SOURCE ||
                  mutation == VARIANT_RELOADED_SOURCE_CHANGED;
    char root[512] = "/tmp", path[768] = "/tmp/main.py";
    bool fixture_ok = true;
    if (reload) {
        const char *temporary = th_mktempdir("cbm_variant_reload");
        if (!temporary)
            return false;
        snprintf(root, sizeof(root), "%s", temporary);
        snprintf(path, sizeof(path), "%s/main.py", root);
        fixture_ok = th_write_file(path, source) == 0;
        if (fixture_ok && (mutation == VARIANT_RELOADED_SOURCE_CHANGED ||
                           mutation == VARIANT_RELEASED_TREE_CHANGED))
            fixture_ok = th_append_file(path, "# changed after extraction\n") == 0;
    }
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "proj",
                                             "main.py", 0, NULL, NULL);
    cbm_gbuf_t *graph = cbm_gbuf_new("proj", root);
    cbm_registry_t *registry = cbm_registry_new();
    bool ok = fixture_ok && result && graph && registry;
    if (ok) {
        result->source = reload ? NULL : source;
        result->source_len = (int)strlen(source);
        if (released)
            cbm_free_tree(result);
        CBMDefinition *first = NULL;
        for (int i = 0; i < result->defs.count; i++) {
            CBMDefinition *definition = &result->defs.items[i];
            if (!definition->qualified_name || strcmp(definition->qualified_name, "proj.main.pick"))
                continue;
            if (!first)
                first = definition;
            else if (mutation == VARIANT_SAME_LINE_DECLARATIONS) {
                first->start_line = definition->start_line;
                first->end_line = definition->end_line;
            }
        }
        ok = variant_add(graph, registry, result);
    }
    cbm_pipeline_ctx_t ctx = {
        .project_name = "proj", .repo_path = root, .gbuf = graph, .registry = registry};
    cbm_file_info_t file = {.path = path, .rel_path = "main.py", .language = CBM_LANG_PYTHON};
    cbm_shared_resolver_t *shared = ok ? cbm_shared_resolver_build(&ctx, &file, 1, &result) : NULL;
    ok = ok && shared;
    cbm_gbuf_node_t *pick =
        ok ? (cbm_gbuf_node_t *)cbm_gbuf_find_by_qn(graph, "proj.main.pick") : NULL;
    if (ok && !pick)
        ok = false;
    if (ok && mutation == VARIANT_BAD_PROPERTIES) {
        cbm_gbuf_settle_variants(graph);
        ok = cbm_gbuf_node_set_properties_json(
                 pick, "{\"variants\":[{\"file_path\":\"main.py\",\"start_line\":5,\"end_line\":6},"
                       "{\"file_path\":\"main.py\",\"start_line\":8,\"end_line\":9},"
                       "{\"file_path\":\"main.py\",\"start_line\":\"invalid\"}]}") == 0;
    }
    if (ok && mutation == VARIANT_FOREIGN_SPAN) {
        cbm_gbuf_settle_variants(graph);
        ok =
            cbm_gbuf_node_set_properties_json(
                pick, "{\"variants\":[{\"file_path\":\"other.py\",\"start_line\":5,\"end_line\":6},"
                      "{\"file_path\":\"main.py\",\"start_line\":8,\"end_line\":9}]}") == 0;
    }
    if (ok && mutation == VARIANT_MISSING_MEMBERSHIP) {
        cbm_gbuf_settle_variants(graph);
        ok = cbm_gbuf_node_set_properties_json(pick, "{}") == 0;
    }
    if (ok && mutation == VARIANT_WRONG_ORIGIN)
        for (int i = 0; i < result->calls.count; i++)
            if (!strcmp(result->calls.items[i].callee_name, "alpha"))
                result->calls.items[i].source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
    if (ok && mutation == VARIANT_CHANGED_CONFIGURATION)
        result->source_configuration_id[0] = result->source_configuration_id[0] == 'a' ? 'b' : 'a';
    cbm_resolver_file_t input = {.ctx = &ctx,
                                 .registry = registry,
                                 .graph = graph,
                                 .output = graph,
                                 .result = result,
                                 .rel_path = "main.py",
                                 .module_qn = result ? result->module_qn : NULL,
                                 .language = CBM_LANG_PYTHON};
    ok = ok && cbm_shared_resolver_file(shared, &input) && cbm_gbuf_resolutions_own_shared(graph) &&
         cbm_shared_resolver_project(graph);
    if (ok) {
        size_t count = 0;
        const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
        for (size_t i = 0; i < count; i++) {
            const cbm_resolve_record_t *row = records + i;
            if (!row->emitted || strcmp(row->relation, "CALLS"))
                continue;
            if (!strcmp(row->scope, "proj.main.pick") &&
                !strcmp(row->result.target, "proj.main.alpha"))
                counts->alpha++;
            else if (!strcmp(row->scope, "proj.main.pick") &&
                     !strcmp(row->result.target, "proj.main.beta"))
                counts->beta++;
            else if (!strcmp(row->scope, "proj.main.caller") &&
                     !strcmp(row->result.target, "proj.main.pick"))
                counts->incoming++;
            else
                counts->other++;
            counts->owner_proofs += variant_has_owner_proof(row->result.proof, 0);
        }
    }
    cbm_shared_resolver_free(shared);
    cbm_registry_free(registry);
    cbm_gbuf_free(graph);
    cbm_free_result(result);
    if (reload)
        th_cleanup(root);
    return ok;
}

TEST(resolver_variants_python_outgoing_owner_requires_raw_equivalence) {
    variant_counts_t counts;
    ASSERT(variant_run(VARIANT_SOURCE, VARIANT_UNMODIFIED, &counts));
    ASSERT_EQ(counts.alpha, 1);
    ASSERT_EQ(counts.beta, 1);
    ASSERT_EQ(counts.incoming, 0);
    ASSERT_EQ(counts.other, 0);
    ASSERT_EQ(counts.owner_proofs, 2);
    PASS();
}

TEST(resolver_variants_arbitrary_redefinitions_are_not_equivalent) {
    const char *source = "def alpha(): pass\ndef beta(): pass\n"
                         "def pick():\n    alpha()\ndef pick():\n    beta()\n"
                         "def caller():\n    pick()\n";
    variant_counts_t counts;
    ASSERT(variant_run(source, VARIANT_UNMODIFIED, &counts));
    ASSERT_EQ(counts.alpha + counts.beta + counts.incoming + counts.other, 0);
    PASS();
}

TEST(resolver_variants_same_line_metadata_is_not_identity) {
    variant_counts_t counts;
    ASSERT(variant_run(VARIANT_SOURCE, VARIANT_SAME_LINE_DECLARATIONS, &counts));
    ASSERT_EQ(counts.alpha + counts.beta + counts.incoming + counts.other, 0);
    PASS();
}

TEST(resolver_variants_malformed_or_foreign_membership_is_rejected) {
    const variant_mutation_t mutations[] = {VARIANT_BAD_PROPERTIES, VARIANT_FOREIGN_SPAN,
                                            VARIANT_MISSING_MEMBERSHIP};
    for (size_t i = 0; i < sizeof(mutations) / sizeof(mutations[0]); i++) {
        variant_counts_t counts;
        ASSERT(variant_run(VARIANT_SOURCE, mutations[i], &counts));
        ASSERT_EQ(counts.alpha + counts.beta + counts.incoming + counts.other, 0);
    }
    PASS();
}

TEST(resolver_variants_origins_and_configuration_are_not_interchangeable) {
    variant_counts_t counts;
    ASSERT(variant_run(VARIANT_SOURCE, VARIANT_WRONG_ORIGIN, &counts));
    ASSERT_EQ(counts.alpha, 0);
    ASSERT_EQ(counts.beta, 1);
    ASSERT_EQ(counts.incoming + counts.other, 0);
    ASSERT(variant_run(VARIANT_SOURCE, VARIANT_CHANGED_CONFIGURATION, &counts));
    ASSERT_EQ(counts.alpha + counts.beta + counts.incoming + counts.other, 0);
    /* Missing retained bytes must use the exact extraction snapshot from disk;
     * a later edit cannot lend its bytes to the original cached syntax tree. */
    ASSERT(variant_run(VARIANT_SOURCE, VARIANT_RELOADED_SOURCE, &counts));
    ASSERT_EQ(counts.alpha, 1);
    ASSERT_EQ(counts.beta, 1);
    ASSERT_EQ(counts.incoming + counts.other, 0);
    ASSERT_EQ(counts.owner_proofs, 2);
    ASSERT(variant_run(VARIANT_SOURCE, VARIANT_RELOADED_SOURCE_CHANGED, &counts));
    ASSERT_EQ(counts.alpha + counts.beta + counts.incoming + counts.other, 0);
    PASS();
}

TEST(resolver_variants_released_tree_uses_matching_snapshot) {
    variant_counts_t counts;
    ASSERT(variant_run(VARIANT_SOURCE, VARIANT_RELEASED_TREE, &counts));
    ASSERT_EQ(counts.alpha, 1);
    ASSERT_EQ(counts.beta, 1);
    ASSERT_EQ(counts.incoming + counts.other, 0);
    ASSERT_EQ(counts.owner_proofs, 2);
    /* Re-parsing cannot replace the extraction snapshot with newer bytes. */
    ASSERT(variant_run(VARIANT_SOURCE, VARIANT_RELEASED_TREE_CHANGED, &counts));
    ASSERT_EQ(counts.alpha + counts.beta + counts.incoming + counts.other, 0);
    PASS();
}

TEST(resolver_variants_same_branch_definitions_are_not_exclusive) {
    const char *source = "import sys\ndef alpha(): pass\ndef beta(): pass\n"
                         "if sys.platform == 'win32':\n"
                         "    def pick():\n        alpha()\n"
                         "    def pick():\n        beta()\nelse:\n    pass\n"
                         "def caller():\n    pick()\n";
    variant_counts_t counts;
    ASSERT(variant_run(source, VARIANT_UNMODIFIED, &counts));
    ASSERT_EQ(counts.alpha + counts.beta + counts.incoming + counts.other, 0);
    PASS();
}

/* Exact authored occurrences cover the first body even when its lines are
 * outside the canonical node range. The oracle below independently counts
 * only the unselected incoming binding, preserving its strict range contract. */
static bool variant_persisted_body(cbm_store_t *store, const char *callee, int declaration_line,
                                   const char *needle) {
    const char *site = strstr(VARIANT_SOURCE, needle);
    if (!store || !site)
        return false;
    site += 8; /* The fixture body has exactly eight leading spaces. */
    int start = (int)(site - VARIANT_SOURCE), end = start + (int)strlen(callee) + 2;
    char target[128];
    snprintf(target, sizeof(target), "variants.main.%s", callee);
    const char *sql =
        "SELECT count(*) FROM resolver_occurrences r "
        "JOIN nodes s ON s.project=r.project AND s.qualified_name=r.scope "
        "JOIN nodes t ON t.project=r.project AND t.qualified_name=r.target "
        "JOIN edges e ON e.project=r.project AND e.source_id=s.id AND e.target_id=t.id "
        "AND e.type=r.relation WHERE r.project='variants' AND r.file_path='main.py' "
        "AND r.scope='variants.main.pick' AND r.relation='CALLS' AND r.source_origin=0 "
        "AND r.start_byte=?1 AND r.end_byte=?2 AND r.target=?3 AND r.emitted=1 "
        "AND s.file_path='main.py' AND s.label='Function' AND t.file_path='main.py' "
        "AND t.label='Function' AND t.start_line=?4 AND t.end_line=?4";
    sqlite3_stmt *statement = NULL;
    bool ok = sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &statement, NULL) == SQLITE_OK &&
              sqlite3_bind_int(statement, 1, start) == SQLITE_OK &&
              sqlite3_bind_int(statement, 2, end) == SQLITE_OK &&
              sqlite3_bind_text(statement, 3, target, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
              sqlite3_bind_int(statement, 4, declaration_line) == SQLITE_OK &&
              sqlite3_step(statement) == SQLITE_ROW && sqlite3_column_int(statement, 0) == 1;
    sqlite3_finalize(statement);
    if (!ok) {
        fprintf(stderr, "variant body missing: %s at bytes %d:%d, target declaration line %d\n",
                callee, start, end, declaration_line);
        const char *diagnostic =
            "SELECT r.scope,r.spelling,r.start_byte,r.end_byte,r.target,r.emitted,r.reason,"
            "s.start_line AS owner_start,s.end_line AS owner_end,"
            "t.start_line AS target_start,t.end_line AS target_end "
            "FROM resolver_occurrences r "
            "LEFT JOIN nodes s ON s.project=r.project AND s.qualified_name=r.scope "
            "LEFT JOIN nodes t ON t.project=r.project AND t.qualified_name=r.target "
            "WHERE r.project='variants' AND r.relation='CALLS' ORDER BY r.ordinal LIMIT 12";
        statement = NULL;
        if (sqlite3_prepare_v2(cbm_store_get_db(store), diagnostic, -1, &statement, NULL) ==
            SQLITE_OK) {
            while (sqlite3_step(statement) == SQLITE_ROW) {
                for (int i = 0; i < sqlite3_column_count(statement); i++) {
                    const unsigned char *value = sqlite3_column_text(statement, i);
                    fprintf(stderr, "%s=%s%s", sqlite3_column_name(statement, i),
                            value ? (const char *)value : "null",
                            i + 1 == sqlite3_column_count(statement) ? "\n" : " ");
                }
            }
        }
        sqlite3_finalize(statement);
    }
    return ok;
}

TEST(resolver_variants_unselected_incoming_binding_remains_quality_debt) {
    for (int driver = 0; driver < 2; driver++) {
        const char *temporary = th_mktempdir("cbm_resolver_variants");
        ASSERT_NOT_NULL(temporary);
        char repo[768], database[768];
        snprintf(repo, sizeof(repo), "%s/repo", temporary);
        snprintf(database, sizeof(database), "%s/index.db", temporary);
        bool ok =
            cbm_mkdir_p(repo, 0755) && th_write_file(TH_PATH(repo, "main.py"), VARIANT_SOURCE) == 0;
        /* Both drivers see identical files. One worker selects the sequential
         * pipeline; four workers and 56 files cross the parallel threshold. */
        for (int i = 0; ok && i < 55; i++) {
            char name[32];
            snprintf(name, sizeof(name), "filler_%02d.py", i);
            ok = th_write_file(TH_PATH(repo, name), "# unrelated module\n") == 0;
        }
        const char *old_workers = getenv("CBM_WORKERS");
        char *saved_workers = old_workers ? strdup(old_workers) : NULL;
        ok = ok && (!old_workers || saved_workers);
        cbm_setenv("CBM_WORKERS", driver ? "4" : "1", 1);
        cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
        ok = pipeline && cbm_pipeline_set_project_name(pipeline, "variants") &&
             cbm_pipeline_run(pipeline) == 0;
        cbm_pipeline_free(pipeline);
        if (saved_workers) {
            cbm_setenv("CBM_WORKERS", saved_workers, 1);
            free(saved_workers);
        } else {
            cbm_unsetenv("CBM_WORKERS");
        }
        cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
        const RQ_Source sources[] = {{.path = "main.py", .text = VARIANT_SOURCE}};
        const RQ_Expected expected[] = {
            {.case_id = "unselected-conditional-binding",
             .site = {.file = "main.py",
                      .caller = "main.caller",
                      .needle = "pick()",
                      .occurrence_index = 2},
             .relation = "CALLS",
             .target = {.file = "main.py",
                        .suffix = "main.pick",
                        .label = "Function",
                        .declaration_line = 8,
                        .declaration_text = "    def pick():"},
             .mode = RQ_KNOWN_GAP,
             .gap_id = "conditional-binding-selection",
             .why = "raw conditional owner equivalence does not select an incoming binding",
             .tried = "explicit mutually exclusive module-level if/else pair; both body calls are "
                      "required"}};
        bool alpha = variant_persisted_body(store, "alpha", 2, "        alpha()");
        bool beta = variant_persisted_body(store, "beta", 3, "        beta()");
        RQ_Report report = {0};
        ok = store &&
             rq_check(store, "variants", "python-conditional-owners", sources, 1, expected,
                      sizeof(expected) / sizeof(expected[0]), NULL, 0, &report, stderr) &&
             ok;
        cbm_store_close(store);
        th_cleanup(temporary);
        ASSERT(ok);
        ASSERT(alpha);
        ASSERT(beta);
        ASSERT_EQ(report.genuine, 1);
        ASSERT_EQ(report.found, 0);
        ASSERT_EQ(report.missing, 1);
        ASSERT_EQ(report.required_missing + report.wrong, 0);
    }
    PASS();
}

SUITE(resolver_variants) {
    RUN_TEST(resolver_variants_python_outgoing_owner_requires_raw_equivalence);
    RUN_TEST(resolver_variants_arbitrary_redefinitions_are_not_equivalent);
    RUN_TEST(resolver_variants_same_line_metadata_is_not_identity);
    RUN_TEST(resolver_variants_malformed_or_foreign_membership_is_rejected);
    RUN_TEST(resolver_variants_origins_and_configuration_are_not_interchangeable);
    RUN_TEST(resolver_variants_released_tree_uses_matching_snapshot);
    RUN_TEST(resolver_variants_same_branch_definitions_are_not_exclusive);
    RUN_TEST(resolver_variants_unselected_incoming_binding_remains_quality_debt);
}
