#include "test_framework.h"
#include "cbm.h"
#include "lsp/ts_lsp.h"
#include "resolve/policy.h"

static bool ts_proved_row(const CBMResolvedCall *row) {
    cbm_resolve_result_t result = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                   .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                   .analysis = row->evidence.analysis,
                                   .rule = row->evidence.rule,
                                   .facts = row->evidence.facts,
                                   .target = row->callee_qn,
                                   .candidate_count = 1};
    return row->site_end_byte > row->site_start_byte && cbm_resolve_admit(&result);
}

static int ts_proved_count(const CBMResolvedCallArray *rows, const char *target) {
    int count = 0;
    for (int i = 0; i < rows->count; i++) {
        const CBMResolvedCall *row = &rows->items[i];
        if (ts_proved_row(row) && (!target || strcmp(target, row->callee_qn) == 0))
            count++;
    }
    return count;
}

TEST(resolver_ts_declared_receiver_selects_exact_occurrence) {
    const char *source = "class A { run() {} } class B { run() {} } "
                         "function use(a: A, b: B) { a.run(); b.run(); }";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT, "test",
                                           "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    int a = ts_proved_count(&file->resolved_calls, "test.main.A.run");
    int b = ts_proved_count(&file->resolved_calls, "test.main.B.run");
    cbm_free_result(file);
    ASSERT_EQ(a, 1);
    ASSERT_EQ(b, 1);
    PASS();
}

/* Destructuring, catch, generics and decorators are in nearly every real file.
 * Each used to withdraw the proof of every method in the file. */
TEST(resolver_ts_ordinary_syntax_keeps_method_proofs) {
    static const char *const extras[] = {
        "function other() { const { x, y: z, ...rest } = make(); return [x, z, rest]; }",
        "function other() { const [first, ...others] = make(); return [first, others]; }",
        "function other(input: { x: number }) { const { x = 1 } = input; return x; }",
        "function other({ x }: { x: number }, [y]: number[]) { return x + y; }",
        "function other() { try { make(); } catch (err) { return err; } }",
        "function other<T>(value: T): T { return value; }",
        "@decorate() class Decorated { @field() run2() {} }",
        "namespace Inner { export function f() { return 1; } }",
        "function other(list: number[]) { list[0] = 1; list[1] += 1; }",
        "function other(box: { n: number }) { box.n = 1; box.n++; }",
        "function other() { let a1, b1; [a1, b1] = make(); ({ a1 } = make()); }",
        "function other() { for (const item of make()) { item(); } }",
    };
    for (size_t i = 0; i < sizeof(extras) / sizeof(extras[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source),
                 "class A { run() {} } function use(a: A) { a.run(); } function make(): any "
                 "{ return 1; } %s",
                 extras[i]);
        CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT,
                                               "test", "main.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        int count = ts_proved_count(&file->resolved_calls, "test.main.A.run");
        cbm_free_result(file);
        ASSERT_EQ(count, 1);
    }
    PASS();
}

TEST(resolver_ts_destructured_and_caught_names_still_shadow) {
    static const char *const sources[] = {
        ("class A { run() {} } function use(a: A) { { const { a } = make(); a.run(); } } "
         "function make(): any { return 1; }"),
        "class A { run() {} } function use(a: A) { try { } catch (a) { a.run(); } }",
        ("class A { run() {} } function use(a: A) { [a] = make(); a.run(); } "
         "function make(): any { return 1; }"),
        ("class A { run() {} } function use(a: A) { ({ a } = make()); a.run(); } "
         "function make(): any { return 1; }"),
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        CBMFileResult *file =
            cbm_extract_file(sources[i], (int)strlen(sources[i]), CBM_LANG_TYPESCRIPT, "test",
                             "main.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        int count = ts_proved_count(&file->resolved_calls, "test.main.A.run");
        cbm_free_result(file);
        ASSERT_EQ(count, 0);
    }
    PASS();
}

/* A class's own method wins over anything it inherits, so `extends` does not
 * block it. An inherited one resolves up the extends chain, and an unknown base
 * in that chain could declare the name first, so it abstains. */
TEST(resolver_ts_own_method_of_a_subclass_is_proved) {
    const char *source = "class Base { inherited() {} shadowed() {} }\n"
                         "class Middle extends Base { }\n"
                         "class Child extends Middle {\n"
                         "  shadowed() {}\n"
                         "  own() {}\n"
                         "  use() { this.own(); this.shadowed(); this.inherited(); this.absent(); }\n"
                         "}\n"
                         "abstract class Shape { area() {} show() { this.area(); } }\n"
                         "class Open extends Unknown { use() { this.inherited(); } }\n";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT, "test",
                                           "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    int own = ts_proved_count(&file->resolved_calls, "test.main.Child.own");
    int shadowed = ts_proved_count(&file->resolved_calls, "test.main.Child.shadowed");
    int base_shadowed = ts_proved_count(&file->resolved_calls, "test.main.Base.shadowed");
    int inherited = ts_proved_count(&file->resolved_calls, "test.main.Base.inherited");
    int abstract_own = ts_proved_count(&file->resolved_calls, "test.main.Shape.area");
    cbm_free_result(file);
    ASSERT_EQ(own, 1);
    ASSERT_EQ(shadowed, 1);
    ASSERT_EQ(base_shadowed, 0);
    /* Child only: `Open` has a base the index cannot see. */
    ASSERT_EQ(inherited, 1);
    ASSERT_EQ(abstract_own, 1);
    PASS();
}

/* Assignment can replace a method in JavaScript. Only that method abstains, and a
 * property write elsewhere no longer withdraws every proof in a CommonJS file. */
TEST(resolver_ts_javascript_member_write_withdraws_only_that_method) {
    const char *source = "class A {\n"
                         "  kept() {}\n"
                         "  replaced() {}\n"
                         "  use() { this.kept(); this.replaced(); }\n"
                         "}\n"
                         "A.prototype.replaced = other;\n"
                         "module.exports.count = 1;\n"
                         "function other() {}\n";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_JAVASCRIPT, "test",
                                           "main.js", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    int kept = ts_proved_count(&file->resolved_calls, "test.main.A.kept");
    int replaced = ts_proved_count(&file->resolved_calls, "test.main.A.replaced");
    cbm_free_result(file);
    ASSERT_EQ(kept, 1);
    ASSERT_EQ(replaced, 0);
    PASS();
}

/* Annotations name their types inside `: T`, `T[]` and `A | B`. The reference is
 * the type name's own occurrence, or no site can ever validate it. */
TEST(resolver_ts_annotation_type_refs_are_exact_occurrences) {
    const char *source = "function use(a: Foo, b: Bar[], c: Baz | Qux, d?: readonly Ns.Deep[]): "
                         "Ret { const e: Local = make(); return e as Cast; }\n";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT, "test",
                                           "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    static const char *const names[] = {"Foo", "Bar", "Baz", "Qux", "Ns.Deep", "Ret", "Local", "Cast"};
    for (size_t n = 0; n < sizeof(names) / sizeof(names[0]); n++) {
        bool exact = false;
        for (int t = 0; t < file->type_refs.count; t++) {
            const CBMTypeRef *ref = &file->type_refs.items[t];
            exact |= !strcmp(ref->type_name, names[n]) && ref->site_is_exact;
        }
        ASSERT(exact);
    }
    for (int t = 0; t < file->type_refs.count; t++)
        ASSERT_NULL(strchr(file->type_refs.items[t].type_name, ':'));
    cbm_free_result(file);
    PASS();
}

/* `new Connection().ping()` and `this.store.get()`: receivers with no variable. */
TEST(resolver_ts_constructed_and_declared_field_receivers_are_proved) {
    const char *source = "class Store { get() {} }\n"
                         "class Other { get() {} }\n"
                         "class View {\n"
                         "  field: Store;\n"
                         "  built = new Other();\n"
                         "  untyped;\n"
                         "  twice: Store;\n"
                         "  twice: Other;\n"
                         "  constructor(private held: Store, plain: Other, readonly kept: Other) {}\n"
                         "  use() {\n"
                         "    this.field.get();\n"
                         "    this.held.get();\n"
                         "    this.built.get();\n"
                         "    this.kept.get();\n"
                         "    this.untyped.get();\n"
                         "    this.plain.get();\n"
                         "    this.twice.get();\n"
                         "    this.field.missing();\n"
                         "  }\n"
                         "}\n"
                         "function direct() { new Store().get(); }\n";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT, "test",
                                           "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    int store = ts_proved_count(&file->resolved_calls, "test.main.Store.get");
    int other = ts_proved_count(&file->resolved_calls, "test.main.Other.get");
    int missing = ts_proved_count(&file->resolved_calls, "test.main.Store.missing");
    cbm_free_result(file);
    /* field, held, direct -> Store; built, kept -> Other. */
    ASSERT_EQ(store, 3);
    ASSERT_EQ(other, 2);
    ASSERT_EQ(missing, 0);
    PASS();
}

/* A large class must not lose the proofs of its last methods. No budget decides
 * which occurrences of a file are proved; 30,000 is past every former default. */
TEST(resolver_ts_large_class_keeps_every_method_proof) {
    enum { CALLS = 30000 };
    const char *head = "class Large {\n  target() {}\n  use() {\n";
    const char *line = "    this.target();\n";
    const char *tail = "  }\n}\n";
    size_t size = strlen(head) + CALLS * strlen(line) + strlen(tail) + 1;
    char *source = malloc(size);
    ASSERT_NOT_NULL(source);
    char *at = source;
    at += snprintf(at, size, "%s", head);
    for (int i = 0; i < CALLS; i++)
        at += snprintf(at, size - (size_t)(at - source), "%s", line);
    snprintf(at, size - (size_t)(at - source), "%s", tail);
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT, "test",
                                           "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    int count = ts_proved_count(&file->resolved_calls, "test.main.Large.target");
    cbm_free_result(file);
    free(source);
    ASSERT_EQ(count, CALLS);
    PASS();
}

/* A class field's initializer runs with `this` as the instance. A function nested
 * in a class member has no node of its own, so its calls belong to that member. */
TEST(resolver_ts_field_initializer_this_is_proved) {
    const char *source = "class View {\n"
                         "  handler = () => this.target();\n"
                         "  wrapped = make({ post: (m: string) => this.target() });\n"
                         "  run() {\n"
                         "    const inner = () => this.target();\n"
                         "    return { deep: () => this.target() };\n"
                         "  }\n"
                         "  target() {}\n"
                         "}\n"
                         "function make(x: any) { return x; }\n";
    CBMFileResult *file = cbm_extract_file(source, (int)strlen(source), CBM_LANG_TYPESCRIPT, "test",
                                           "main.ts", 0, NULL, NULL);
    ASSERT_NOT_NULL(file);
    int count = ts_proved_count(&file->resolved_calls, "test.main.View.target");
    int in_run = 0;
    for (int i = 0; i < file->resolved_calls.count; i++) {
        const CBMResolvedCall *row = &file->resolved_calls.items[i];
        if (ts_proved_row(row) && !strcmp(row->callee_qn, "test.main.View.target") &&
            !strcmp(row->caller_qn, "test.main.View.run"))
            in_run++;
    }
    cbm_free_result(file);
    ASSERT_EQ(count, 4);
    ASSERT_EQ(in_run, 2);
    PASS();
}

TEST(resolver_ts_unknown_union_and_later_shadow_abstain) {
    const char *sources[] = {
        "class A { run() {} } function use(a) { a.run(); }",
        "class A { run() {} } class B { run() {} } function use(a: A | B) { a.run(); }",
        "class A { run() {} } function use(a: A) { { a.run(); let a; } }",
        "class A { run() {} } function use(a: A) { a.run(); a = unknownValue; }",
        ("class A { run() {} } function use(a: A) { mutate(); a.run(); "
         "function mutate() { a = unknownValue; } }")};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        CBMFileResult *file =
            cbm_extract_file(sources[i], (int)strlen(sources[i]), CBM_LANG_TYPESCRIPT, "test",
                             "main.ts", 0, NULL, NULL);
        ASSERT_NOT_NULL(file);
        int count = ts_proved_count(&file->resolved_calls, NULL);
        cbm_free_result(file);
        ASSERT_EQ(count, 0);
    }
    PASS();
}

TEST(resolver_ts_imported_type_requires_export_binding) {
    const char *source = "import { A } from './lib'; function use(a: A) { a.run(); }";
    CBMLSPDef defs[] = {{.qualified_name = "test.lib.A",
                         .short_name = "A",
                         .label = "Class",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:class",
                         .export_binding_proven = true},
                        {.qualified_name = "test.lib.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.lib.A",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:method"},
                        {.qualified_name = "test.decoy.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.decoy.A",
                         .def_module_qn = "test.decoy",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "decoy:method"}};
    const char *names[] = {"A"}, *targets[] = {"test.lib"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray proven = {0}, private = {0};
    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 3, names, targets, 1, NULL, &proven);
    defs[0].export_binding_proven = false;
    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 3, names, targets, 1, NULL, &private);
    int exact = ts_proved_count(&proven, "test.lib.A.run");
    int decoy = ts_proved_count(&proven, "test.decoy.A.run");
    int rejected = ts_proved_count(&private, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(exact, 1);
    ASSERT_EQ(decoy, 0);
    ASSERT_EQ(rejected, 0);
    PASS();
}

TEST(resolver_ts_overlay_copies_coalesce_only_same_declaration) {
    const char *source = "class A { run() {} } function use(a: A) { a.run(); }";
    CBMLSPDef defs[] = {{.qualified_name = "test.main.A",
                         .short_name = "A",
                         .label = "Class",
                         .def_module_qn = "test.main",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "main:class"},
                        {.qualified_name = "test.main.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.main.A",
                         .def_module_qn = "test.main",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "main:method"}};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry *registry = cbm_ts_build_cross_registry(&arena, defs, 2);
    ASSERT_NOT_NULL(registry);
    CBMResolvedCallArray rows = {0};
    cbm_run_ts_lsp_cross_with_registry(&arena, source, (int)strlen(source), "test.main", false,
                                       false, false, registry, defs, 2, NULL, NULL, 0, NULL, &rows);
    int count = ts_proved_count(&rows, "test.main.A.run");
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 1);
    PASS();
}

TEST(resolver_ts_duplicate_method_declarations_abstain) {
    const char *source = "import { A } from './lib'; function use(a: A) { a.run(); }";
    CBMLSPDef defs[] = {{.qualified_name = "test.lib.A",
                         .short_name = "A",
                         .label = "Class",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:class",
                         .export_binding_proven = true},
                        {.qualified_name = "test.lib.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.lib.A",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:method1"},
                        {.qualified_name = "test.lib.A.run",
                         .short_name = "run",
                         .label = "Method",
                         .receiver_type = "test.lib.A",
                         .def_module_qn = "test.lib",
                         .lang = CBM_LANG_TYPESCRIPT,
                         .declaration_identity = "lib:method2"}};
    const char *names[] = {"A"}, *targets[] = {"test.lib"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray rows = {0};
    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 3, names, targets, 1, NULL, &rows);
    int count = ts_proved_count(&rows, NULL);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(count, 0);
    PASS();
}

SUITE(resolver_ts) {
    RUN_TEST(resolver_ts_declared_receiver_selects_exact_occurrence);
    RUN_TEST(resolver_ts_unknown_union_and_later_shadow_abstain);
    RUN_TEST(resolver_ts_ordinary_syntax_keeps_method_proofs);
    RUN_TEST(resolver_ts_large_class_keeps_every_method_proof);
    RUN_TEST(resolver_ts_field_initializer_this_is_proved);
    RUN_TEST(resolver_ts_constructed_and_declared_field_receivers_are_proved);
    RUN_TEST(resolver_ts_destructured_and_caught_names_still_shadow);
    RUN_TEST(resolver_ts_own_method_of_a_subclass_is_proved);
    RUN_TEST(resolver_ts_javascript_member_write_withdraws_only_that_method);
    RUN_TEST(resolver_ts_annotation_type_refs_are_exact_occurrences);
    RUN_TEST(resolver_ts_imported_type_requires_export_binding);
    RUN_TEST(resolver_ts_overlay_copies_coalesce_only_same_declaration);
    RUN_TEST(resolver_ts_duplicate_method_declarations_abstain);
}
