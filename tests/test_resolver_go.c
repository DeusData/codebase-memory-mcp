#include "test_framework.h"
#include "cbm.h"
#include "lsp/go_lsp.h"
#include "resolve/policy.h"
#include "resolve/proof.h"

static bool go_row_admitted(const CBMResolvedCall *row) {
    cbm_resolve_result_t proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = row->evidence.analysis,
                                  .rule = row->evidence.rule,
                                  .facts = row->evidence.facts,
                                  .target = row->callee_qn,
                                  .candidate_count = 1};
    return row->site_end_byte > row->site_start_byte && cbm_resolve_admit(&proof);
}

static int go_proven_count(const CBMResolvedCallArray *rows, const char *target) {
    int count = 0;
    for (int i = 0; i < rows->count; i++) {
        const CBMResolvedCall *row = &rows->items[i];
        if (go_row_admitted(row) && (!target || !strcmp(row->callee_qn, target)))
            count++;
    }
    return count;
}

static void go_cross(CBMArena *arena, const char *source, CBMLSPDef *defs, int count,
                     const char **names, const char **qns, int imports, CBMResolvedCallArray *out) {
    cbm_run_go_lsp_cross(arena, source, (int)strlen(source), "test.main", defs, count, names, qns,
                         imports, NULL, out);
}

TEST(resolver_go_scoped_calls_and_callable_values_have_proof) {
    const char *source = "package main\nimport lib \"pkg/lib\"\n"
                         "func run() { work(); lib.Export(); sink(work); "
                         "alias := work; alias(); sink(alias) }\n";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.work",
                         .short_name = "work",
                         .label = "Function",
                         .declaration_identity = "scoped:1"},
                        {.qualified_name = "test.main.sink",
                         .short_name = "sink",
                         .label = "Function",
                         .declaration_identity = "scoped:2"},
                        {.qualified_name = "pkg/lib.Export",
                         .short_name = "Export",
                         .label = "Function",
                         .declaration_identity = "scoped:3"},
                        {.qualified_name = "other.work",
                         .short_name = "work",
                         .label = "Function",
                         .declaration_identity = "scoped:4"},
                        {.qualified_name = "other.Export",
                         .short_name = "Export",
                         .label = "Function",
                         .declaration_identity = "scoped:5"}};
    const char *names[] = {"lib"}, *qns[] = {"pkg/lib"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    go_cross(&arena, source, defs, 5, names, qns, 1, &out);
    int work = go_proven_count(&out, "test.main.work");
    int imported = go_proven_count(&out, "pkg/lib.Export");
    int decoys = go_proven_count(&out, "other.work") + go_proven_count(&out, "other.Export");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(work, 4);
    ASSERT_EQ(imported, 1);
    ASSERT_EQ(decoys, 0);
    PASS();
}

TEST(resolver_go_import_shadow_and_alias_conflict_abstain) {
    CBMLSPDef defs[] = {{.qualified_name = "pkg/one.Export",
                         .short_name = "Export",
                         .label = "Function",
                         .declaration_identity = "imports:1"},
                        {.qualified_name = "pkg/two.Export",
                         .short_name = "Export",
                         .label = "Function",
                         .declaration_identity = "imports:2"}};
    const char *names[] = {"lib", "lib"}, *qns[] = {"pkg/one", "pkg/two"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray shadow = {0}, conflict = {0};
    go_cross(&arena, "package main\nfunc run(lib Missing) { lib.Export() }\n", defs, 2, names, qns,
             1, &shadow);
    go_cross(&arena, "package main\nfunc run() { lib.Export() }\n", defs, 2, names, qns, 2,
             &conflict);
    int shadow_count = go_proven_count(&shadow, NULL);
    int conflict_count = go_proven_count(&conflict, NULL);
    CBMResolvedCallArray control = {0};
    go_cross(&arena, "package main\nimport lib \"pkg/one\"\nfunc run() { lib.Export() }\n", defs, 2,
             names, qns, 1, &control);
    int positive = go_proven_count(&control, "pkg/one.Export");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(positive, 1);
    ASSERT_EQ(shadow_count, 0);
    ASSERT_EQ(conflict_count, 0);
    PASS();
}

TEST(resolver_go_declared_methods_keep_interface_declaration_target) {
    const char *source = "package main\nfunc run(c Conn, r Reader) { c.Query(); r.Read(); "
                         "sink(c.Query); bound := c.Query; bound() }\n";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.Conn",
                         .short_name = "Conn",
                         .label = "Type",
                         .declaration_identity = "methods:1"},
                        {.qualified_name = "test.main.Conn.Query",
                         .short_name = "Query",
                         .label = "Method",
                         .receiver_type = "test.main.Conn",
                         .declaration_identity = "methods:2"},
                        {.qualified_name = "test.main.Reader",
                         .short_name = "Reader",
                         .label = "Interface",
                         .is_interface = true,
                         .method_names_str = "Read",
                         .declaration_identity = "methods:3"},
                        {.qualified_name = "test.main.Reader.Read",
                         .short_name = "Read",
                         .label = "Method",
                         .receiver_type = "test.main.Reader",
                         .declaration_identity = "methods:4"},
                        {.qualified_name = "test.main.Impl",
                         .short_name = "Impl",
                         .label = "Type",
                         .declaration_identity = "methods:5"},
                        {.qualified_name = "test.main.Impl.Read",
                         .short_name = "Read",
                         .label = "Method",
                         .receiver_type = "test.main.Impl",
                         .declaration_identity = "methods:6"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    go_cross(&arena, source, defs, 6, NULL, NULL, 0, &out);
    int method = go_proven_count(&out, "test.main.Conn.Query");
    int iface = go_proven_count(&out, "test.main.Reader.Read");
    int impl = go_proven_count(&out, "test.main.Impl.Read");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(method, 3);
    ASSERT_EQ(iface, 1);
    ASSERT_EQ(impl, 0);
    PASS();
}

TEST(resolver_go_promoted_or_unknown_receiver_does_not_guess) {
    const char *source =
        "package main\nfunc run(c C, unknown Missing) { c.Close(); unknown.Close() }\n";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.A",
                         .short_name = "A",
                         .label = "Type",
                         .declaration_identity = "promoted:1"},
                        {.qualified_name = "test.main.B",
                         .short_name = "B",
                         .label = "Type",
                         .declaration_identity = "promoted:2"},
                        {.qualified_name = "test.main.C",
                         .short_name = "C",
                         .label = "Type",
                         .embedded_types = "test.main.A|test.main.B",
                         .declaration_identity = "promoted:3"},
                        {.qualified_name = "test.main.A.Close",
                         .short_name = "Close",
                         .label = "Method",
                         .receiver_type = "test.main.A",
                         .declaration_identity = "promoted:4"},
                        {.qualified_name = "test.main.B.Close",
                         .short_name = "Close",
                         .label = "Method",
                         .receiver_type = "test.main.B",
                         .declaration_identity = "promoted:5"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    go_cross(&arena, source, defs, 5, NULL, NULL, 0, &out);
    int count = go_proven_count(&out, NULL);
    CBMResolvedCallArray control = {0};
    go_cross(&arena, "package main\nfunc run(a A, b B) { a.Close(); b.Close() }\n", defs, 5, NULL,
             NULL, 0, &control);
    int direct_a = go_proven_count(&control, "test.main.A.Close");
    int direct_b = go_proven_count(&control, "test.main.B.Close");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(direct_a, 1);
    ASSERT_EQ(direct_b, 1);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_go_alias_reassignment_and_branch_join_clear_identity) {
    const char *source = "package main\nfunc run(dynamic func(), cond bool) { "
                         "alias := work; { alias = dynamic }; alias(); "
                         "other := work; if cond { other = dynamic }; other() }\n";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.work",
                         .short_name = "work",
                         .label = "Function",
                         .declaration_identity = "alias:1"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    go_cross(&arena, source, defs, 1, NULL, NULL, 0, &out);
    int count = go_proven_count(&out, "test.main.work");
    CBMResolvedCallArray control = {0};
    go_cross(&arena,
             "package main\nfunc run() { alias := work; alias(); "
             "other := work; other() }\n",
             defs, 1, NULL, NULL, 0, &control);
    int positive = go_proven_count(&control, "test.main.work");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(positive, 2);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_go_fast_crossfile_requires_ast_binding_evidence) {
    const char *source = "package main\nfunc run() { work() }\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test",
                                             "main.go", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMLSPDef def = {.qualified_name =
                         cbm_arena_sprintf(&result->arena, "%s.work", result->module_qn),
                     .short_name = "work",
                     .label = "Function",
                     .lang = CBM_LANG_GO,
                     .declaration_identity = "fast-cross:work"};
    CBMTypeRegistry *reg = cbm_go_build_cross_registry(&result->arena, &def, 1);
    ASSERT_NOT_NULL(reg);
    CBMResolvedCall fake = {.caller_qn = "test.main.decoy",
                            .callee_qn = def.qualified_name,
                            .kind = CBM_RESOLVED_INVOCATION,
                            .strategy = "lsp_unresolved",
                            .reason = "function_not_in_registry",
                            .site_start_byte = 1,
                            .site_end_byte = 2};
    cbm_resolvedcall_push(&result->resolved_calls, &result->arena, fake);
    cbm_go_fast_resolve_qualified_calls(result, reg, NULL, NULL, 0);
    int admitted = go_proven_count(&result->resolved_calls, def.qualified_name);
    int wrong = 0;
    for (int i = 0; i < result->resolved_calls.count; i++) {
        const CBMResolvedCall *row = &result->resolved_calls.items[i];
        if (go_row_admitted(row) && !strcmp(row->caller_qn, "test.main.decoy"))
            wrong++;
    }
    cbm_free_result(result);
    ASSERT_EQ(admitted, 1);
    ASSERT_EQ(wrong, 0);
    PASS();
}

/* Only the metadata carrier is changed: every variant retains the same
 * genuine AST capture and target, including its complete source snapshot. */
TEST(resolver_go_fast_crossfile_rejects_pending_metadata_changes) {
    const char *source = "package main\nfunc run() { work() }\n";
    int admitted[4] = {0};
    for (int mutation = 0; mutation < 4; mutation++) {
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test",
                                                 "main.go", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMLSPDef def = {.qualified_name =
                             cbm_arena_sprintf(&result->arena, "%s.work", result->module_qn),
                         .short_name = "work",
                         .label = "Function",
                         .lang = CBM_LANG_GO,
                         .declaration_identity = "pending-metadata:work"};
        CBMTypeRegistry *reg = cbm_go_build_cross_registry(&result->arena, &def, 1);
        ASSERT_NOT_NULL(reg);
        CBMResolvedCall *pending = NULL;
        int pending_count = 0;
        for (int i = 0; i < result->resolved_calls.count; i++) {
            CBMResolvedCall *row = &result->resolved_calls.items[i];
            if (row->evidence.analysis == CBM_RESOLVE_ANALYSIS_LIMITED && row->callee_qn &&
                !strcmp(row->callee_qn, def.qualified_name)) {
                pending = row;
                pending_count++;
            }
        }
        ASSERT_EQ(pending_count, 1);
        ASSERT_NOT_NULL(pending);
        const cbm_resolve_proof_view_t *capture = cbm_resolve_proof_view(pending->evidence.proof);
        ASSERT_NOT_NULL(capture);
        ASSERT_EQ(capture->kind, CBM_PROOF_NAMED_JOIN);
        ASSERT_EQ(capture->outcome.analysis, CBM_RESOLVE_ANALYSIS_COMPLETE);
        ASSERT_EQ(pending->evidence.rule, CBM_RESOLVE_RULE_SCOPE_BINDING);
        ASSERT_EQ(pending->evidence.facts,
                  CBM_RESOLVE_FACT_SCOPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT);
        if (mutation == 1)
            pending->evidence.rule = CBM_RESOLVE_RULE_IMPORT_BINDING;
        else if (mutation == 2)
            pending->evidence.facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
        else if (mutation == 3) {
            pending->evidence.rule = CBM_RESOLVE_RULE_CALLABLE_BINDING;
            pending->evidence.facts =
                CBM_RESOLVE_FACT_CALLABLE_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
        }
        cbm_go_fast_resolve_qualified_calls(result, reg, NULL, NULL, 0);
        admitted[mutation] = go_proven_count(&result->resolved_calls, def.qualified_name);
        cbm_free_result(result);
    }
    ASSERT_EQ(admitted[0], 1);
    ASSERT_EQ(admitted[1], 0);
    ASSERT_EQ(admitted[2], 0);
    ASSERT_EQ(admitted[3], 0);
    PASS();
}

/* A pending method has complete receiver authority, but the registry has not
 * supplied its declaration yet. Its capture must retain that distinct proof. */
TEST(resolver_go_fast_crossfile_preserves_pending_receiver_proof) {
    const char *source = "package main\ntype Conn struct{}\n"
                         "func run(c Conn) { c.Query() }\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test",
                                             "main.go", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    const char *owner = cbm_arena_sprintf(&result->arena, "%s.Conn", result->module_qn);
    const char *target = cbm_arena_sprintf(&result->arena, "%s.Query", owner);
    CBMLSPDef defs[] = {{.qualified_name = owner,
                         .short_name = "Conn",
                         .label = "Type",
                         .lang = CBM_LANG_GO,
                         .declaration_identity = "pending-receiver:Conn"},
                        {.qualified_name = target,
                         .short_name = "Query",
                         .label = "Method",
                         .lang = CBM_LANG_GO,
                         .receiver_type = owner,
                         .declaration_identity = "pending-receiver:Query"}};
    CBMTypeRegistry *reg = cbm_go_build_cross_registry(&result->arena, defs, 2);
    ASSERT_NOT_NULL(reg);
    const CBMResolvedCall *pending = NULL;
    int pending_count = 0;
    for (int i = 0; i < result->resolved_calls.count; i++) {
        const CBMResolvedCall *row = &result->resolved_calls.items[i];
        if (row->evidence.analysis == CBM_RESOLVE_ANALYSIS_LIMITED && row->callee_qn &&
            !strcmp(row->callee_qn, target)) {
            pending = row;
            pending_count++;
        }
    }
    ASSERT_EQ(pending_count, 1);
    ASSERT_NOT_NULL(pending);
    const cbm_resolve_proof_view_t *capture = cbm_resolve_proof_view(pending->evidence.proof);
    ASSERT_NOT_NULL(capture);
    ASSERT_EQ(pending->evidence.rule, CBM_RESOLVE_RULE_TYPE_MEMBER);
    ASSERT_EQ(capture->outcome.rule, CBM_RESOLVE_RULE_DECLARED_TYPE);
    ASSERT(capture->outcome.facts & CBM_RESOLVE_FACT_TYPE_BOUND);
    cbm_go_fast_resolve_qualified_calls(result, reg, NULL, NULL, 0);
    int admitted = go_proven_count(&result->resolved_calls, target);
    cbm_free_result(result);
    ASSERT_EQ(admitted, 1);
    PASS();
}

TEST(resolver_go_duplicate_declarations_never_choose_first) {
    const char *source = "package main\nfunc run(c Conn) { work(); c.Query() }\n";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.work",
                         .short_name = "work",
                         .label = "Function",
                         .declaration_identity = "file1:work"},
                        {.qualified_name = "test.main.work",
                         .short_name = "work",
                         .label = "Function",
                         .declaration_identity = "file2:work"},
                        {.qualified_name = "test.main.Conn",
                         .short_name = "Conn",
                         .label = "Type",
                         .declaration_identity = "duplicates:1"},
                        {.qualified_name = "test.main.Conn.Query",
                         .short_name = "Query",
                         .label = "Method",
                         .receiver_type = "test.main.Conn",
                         .declaration_identity = "file1:Query"},
                        {.qualified_name = "test.main.Conn.Query",
                         .short_name = "Query",
                         .label = "Method",
                         .receiver_type = "test.main.Conn",
                         .declaration_identity = "file2:Query"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    go_cross(&arena, source, defs, 5, NULL, NULL, 0, &out);
    int count = go_proven_count(&out, NULL);
    CBMLSPDef unique[] = {defs[0], defs[2], defs[3]};
    CBMResolvedCallArray control = {0};
    go_cross(&arena, source, unique, 3, NULL, NULL, 0, &control);
    int unique_work = go_proven_count(&control, "test.main.work");
    int unique_method = go_proven_count(&control, "test.main.Conn.Query");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(unique_work, 1);
    ASSERT_EQ(unique_method, 1);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_go_duplicate_receiver_type_never_chooses_first) {
    const char *source = "package main\nfunc run(c Conn) { c.Query() }\n";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.Conn",
                         .short_name = "Conn",
                         .label = "Type",
                         .declaration_identity = "one:Conn"},
                        {.qualified_name = "test.main.Conn",
                         .short_name = "Conn",
                         .label = "Type",
                         .declaration_identity = "two:Conn"},
                        {.qualified_name = "test.main.Conn.Query",
                         .short_name = "Query",
                         .label = "Method",
                         .receiver_type = "test.main.Conn",
                         .declaration_identity = "receiver:1"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    go_cross(&arena, source, defs, 3, NULL, NULL, 0, &out);
    int count = go_proven_count(&out, NULL);
    CBMLSPDef unique[] = {defs[0], defs[2]};
    CBMResolvedCallArray control = {0};
    go_cross(&arena, source, unique, 2, NULL, NULL, 0, &control);
    int positive = go_proven_count(&control, "test.main.Conn.Query");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(positive, 1);
    ASSERT_EQ(count, 0);
    PASS();
}
TEST(resolver_go_generic_type_parameter_never_binds_project_namesake) {
    const char *source =
        "package main\nfunc run[Conn interface{ Query() }](c Conn) { c.Query() }\n";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.Conn",
                         .short_name = "Conn",
                         .label = "Type",
                         .declaration_identity = "generic:1"},
                        {.qualified_name = "test.main.Conn.Query",
                         .short_name = "Query",
                         .label = "Method",
                         .receiver_type = "test.main.Conn",
                         .declaration_identity = "generic:2"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    go_cross(&arena, source, defs, 2, NULL, NULL, 0, &out);
    int count = go_proven_count(&out, "test.main.Conn.Query");
    CBMResolvedCallArray control = {0};
    go_cross(&arena, "package main\nfunc run(c Conn) { c.Query() }\n", defs, 2, NULL, NULL, 0,
             &control);
    int positive = go_proven_count(&control, "test.main.Conn.Query");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(positive, 1);
    ASSERT_EQ(count, 0);
    PASS();
}

TEST(resolver_go_declared_factory_return_propagates_proof) {
    const char *source = "package main\n"
                         "type Conn struct{}\n"
                         "func New() *Conn { return &Conn{} }\n"
                         "func (c *Conn) Query() {}\n"
                         "func run() { c := New(); alias := c; alias.Query() }\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test",
                                             "main.go", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    int methods = go_proven_count(&result->resolved_calls, "test.Conn.Query");
    if (methods != 1) {
        fprintf(stderr, "factory module=%s defs=%d rows=%d\n", result->module_qn,
                result->defs.count, result->resolved_calls.count);
        for (int i = 0; i < result->defs.count; i++)
            fprintf(stderr, "factory def=%s line=%u\n", result->defs.items[i].qualified_name,
                    result->defs.items[i].start_line);
        for (int i = 0; i < result->resolved_calls.count; i++) {
            const CBMResolvedCall *row = &result->resolved_calls.items[i];
            fprintf(stderr, "factory row=%s rule=%d facts=%u status=%d\n", row->callee_qn,
                    row->evidence.rule, row->evidence.facts, row->evidence.analysis);
        }
    }
    cbm_free_result(result);
    ASSERT_EQ(methods, 1);
    PASS();
}

TEST(resolver_go_cross_factory_return_requires_source_annotation) {
    const char *library = "package lib\ntype Conn struct{}\n"
                          "func New() *Conn { return &Conn{} }\n"
                          "func (c *Conn) Query() {}\n";
    const char *generic = "package lib\ntype Conn struct{}\n"
                          "func New[Conn any]() Conn { var c Conn; return c }\n";
    const char *source = "package main\nimport lib \"pkg/lib\"\n"
                         "func run() { value := lib.New(); value.Query() }\n";
    const char *names[] = {"lib"}, *qns[] = {"pkg.lib"};
    CBMLSPDef defs[] = {{.qualified_name = "pkg.lib.Conn",
                         .short_name = "Conn",
                         .label = "Struct",
                         .declaration_identity = "type",
                         .def_module_qn = "pkg.lib"},
                        {.qualified_name = "pkg.lib.New",
                         .short_name = "New",
                         .label = "Function",
                         .source_start_line = 3,
                         .declaration_identity = "new",
                         .return_types = "*Conn",
                         .def_module_qn = "pkg.lib"},
                        {.qualified_name = "pkg.lib.Conn.Query",
                         .short_name = "Query",
                         .label = "Method",
                         .receiver_type = "pkg.lib.Conn",
                         .declaration_identity = "query",
                         .def_module_qn = "pkg.lib"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    bool audited =
        cbm_go_collect_source_facts(&arena, library, (int)strlen(library), "pkg.lib", defs, 3);
    CBMResolvedCallArray positive = {0};
    go_cross(&arena, source, defs, 3, names, qns, 1, &positive);
    int found = go_proven_count(&positive, "pkg.lib.Conn.Query");
    bool generic_audited =
        cbm_go_collect_source_facts(&arena, generic, (int)strlen(generic), "pkg.lib", defs, 3);
    CBMResolvedCallArray rejected = {0};
    go_cross(&arena, source, defs, 3, names, qns, 1, &rejected);
    int guessed = go_proven_count(&rejected, "pkg.lib.Conn.Query");
    cbm_arena_destroy(&arena);
    ASSERT(audited && generic_audited);
    ASSERT_EQ(found, 1);
    ASSERT_EQ(guessed, 0);
    PASS();
}

TEST(resolver_go_composite_value_leaves_require_exact_scope) {
    const char *sources[] = {
        "package main\nfunc run(flag bool) { sink(map[bool]func(){true: work}[flag]) }\n",
        "package main\nfunc run(flag bool, work func()) { sink(map[bool]func(){true: work}[flag]) "
        "}\n"};
    CBMLSPDef defs[] = {{.qualified_name = "test.main.work",
                         .short_name = "work",
                         .label = "Function",
                         .declaration_identity = "composite:1"},
                        {.qualified_name = "test.main.sink",
                         .short_name = "sink",
                         .label = "Function",
                         .declaration_identity = "composite:2"}};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMResolvedCallArray rows = {0};
        go_cross(&arena, sources[i], defs, 2, NULL, NULL, 0, &rows);
        int count = go_proven_count(&rows, "test.main.work");
        cbm_arena_destroy(&arena);
        ASSERT_EQ(count, i == 0);
    }
    PASS();
}

SUITE(resolver_go) {
    RUN_TEST(resolver_go_composite_value_leaves_require_exact_scope);
    RUN_TEST(resolver_go_cross_factory_return_requires_source_annotation);
    RUN_TEST(resolver_go_declared_factory_return_propagates_proof);
    RUN_TEST(resolver_go_duplicate_receiver_type_never_chooses_first);
    RUN_TEST(resolver_go_generic_type_parameter_never_binds_project_namesake);
    RUN_TEST(resolver_go_scoped_calls_and_callable_values_have_proof);
    RUN_TEST(resolver_go_import_shadow_and_alias_conflict_abstain);
    RUN_TEST(resolver_go_declared_methods_keep_interface_declaration_target);
    RUN_TEST(resolver_go_promoted_or_unknown_receiver_does_not_guess);
    RUN_TEST(resolver_go_alias_reassignment_and_branch_join_clear_identity);
    RUN_TEST(resolver_go_fast_crossfile_requires_ast_binding_evidence);
    RUN_TEST(resolver_go_fast_crossfile_rejects_pending_metadata_changes);
    RUN_TEST(resolver_go_fast_crossfile_preserves_pending_receiver_proof);
    RUN_TEST(resolver_go_duplicate_declarations_never_choose_first);
}
