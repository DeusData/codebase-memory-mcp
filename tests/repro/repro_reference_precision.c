/*
 * repro_reference_precision.c — occurrence- and target-exact reference edges.
 *
 * These are valid-syntax, production-pipeline contracts. Explicit reference
 * syntax earns CALL_REFERENCE only when the semantic pass proves an exact,
 * materialized callable target at that occurrence. Properties, dynamic names,
 * shadowed locals without graph nodes, and unimported definitions must not be
 * promoted by a same-name fallback. Ordinary calls have the same occurrence
 * requirement when two receiver types expose the same method name.
 */
#include "test_framework.h"
#include "repro_harness.h"
#include "lsp/rust_lsp.h"
#include "lsp/ts_lsp.h"
#include "pipeline/lsp_resolve.h"

#include <store/store.h>

#include <stdio.h>
#include <string.h>

static int rp_qn_ends_with(const char *qn, const char *suffix) {
    if (!qn || !suffix)
        return 0;
    size_t qn_len = strlen(qn);
    size_t suffix_len = strlen(suffix);
    return qn_len >= suffix_len && strcmp(qn + qn_len - suffix_len, suffix) == 0;
}

static int rp_extracts_clean(const char *source, CBMLanguage language, const char *filename) {
    CBMFileResult *result =
        cbm_extract_file(source, (int)strlen(source), language, "repro", filename, 0, NULL, NULL);
    if (!result)
        return 0;
    int clean = !result->has_error && !result->parse_incomplete;
    cbm_free_result(result);
    return clean;
}

static int rp_node_count(cbm_store_t *store, const char *project, const char *label,
                         const char *name, const char *qn_suffix) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    int matches = 0;
    for (int i = 0; i < count; i++) {
        if (name && (!nodes[i].name || strcmp(nodes[i].name, name) != 0))
            continue;
        if (qn_suffix && !rp_qn_ends_with(nodes[i].qualified_name, qn_suffix))
            continue;
        matches++;
    }
    cbm_store_free_nodes(nodes, count);
    return matches;
}

static int rp_edge_count(cbm_store_t *store, const char *project, const char *edge_type,
                         const char *source_name, const char *target_label, const char *target_name,
                         const char *target_qn_suffix) {
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    if (cbm_store_find_edges_by_type(store, project, edge_type, &edges, &edge_count) !=
        CBM_STORE_OK) {
        return -1;
    }

    int matches = 0;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t source = {0};
        cbm_node_t target = {0};
        int source_ok =
            cbm_store_find_node_by_id(store, edges[i].source_id, &source) == CBM_STORE_OK;
        int target_ok =
            cbm_store_find_node_by_id(store, edges[i].target_id, &target) == CBM_STORE_OK;
        if (source_ok && target_ok &&
            (!source_name || (source.name && strcmp(source.name, source_name) == 0)) &&
            (!target_label || (target.label && strcmp(target.label, target_label) == 0)) &&
            (!target_name || (target.name && strcmp(target.name, target_name) == 0)) &&
            (!target_qn_suffix || rp_qn_ends_with(target.qualified_name, target_qn_suffix))) {
            matches++;
        }
        cbm_node_free_fields(&source);
        cbm_node_free_fields(&target);
    }
    cbm_store_free_edges(edges, edge_count);
    return matches;
}

static int rp_is_constructor_node(const cbm_node_t *node, const char *class_name) {
    if (!node || !node->label || !node->name || !node->qualified_name || !class_name)
        return 0;
    int callable_label = strcmp(node->label, "Constructor") == 0 ||
                         strcmp(node->label, "Function") == 0 || strcmp(node->label, "Method") == 0;
    int constructor_name =
        strcmp(node->name, class_name) == 0 || strcmp(node->name, "constructor") == 0;
    char class_segment[128];
    snprintf(class_segment, sizeof(class_segment), ".%s", class_name);
    return callable_label && constructor_name && strstr(node->qualified_name, class_segment);
}

static int rp_constructor_node_count(cbm_store_t *store, const char *project,
                                     const char *class_name) {
    static const char *const labels[] = {"Constructor", "Function", "Method"};
    int matches = 0;
    for (size_t li = 0; li < sizeof(labels) / sizeof(labels[0]); li++) {
        cbm_node_t *nodes = NULL;
        int count = 0;
        if (cbm_store_find_nodes_by_label(store, project, labels[li], &nodes, &count) !=
            CBM_STORE_OK) {
            return -1;
        }
        for (int i = 0; i < count; i++) {
            if (rp_is_constructor_node(&nodes[i], class_name))
                matches++;
        }
        cbm_store_free_nodes(nodes, count);
    }
    return matches;
}

static int rp_constructor_edge_count(cbm_store_t *store, const char *project, const char *edge_type,
                                     const char *source_name, const char *class_name) {
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    if (cbm_store_find_edges_by_type(store, project, edge_type, &edges, &edge_count) !=
        CBM_STORE_OK) {
        return -1;
    }
    int matches = 0;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t source = {0};
        cbm_node_t target = {0};
        int source_ok =
            cbm_store_find_node_by_id(store, edges[i].source_id, &source) == CBM_STORE_OK;
        int target_ok =
            cbm_store_find_node_by_id(store, edges[i].target_id, &target) == CBM_STORE_OK;
        if (source_ok && target_ok && source.name && strcmp(source.name, source_name) == 0 &&
            rp_is_constructor_node(&target, class_name)) {
            matches++;
        }
        cbm_node_free_fields(&source);
        cbm_node_free_fields(&target);
    }
    cbm_store_free_edges(edges, edge_count);
    return matches;
}

static int rp_raw_usage_count(const CBMFileResult *result, const char *caller, const char *target,
                              CBMUsageKind kind) {
    int count = 0;
    for (int i = 0; result && i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        const char *owner = usage->enclosing_func_qn;
        const char *owner_name = owner ? strrchr(owner, '.') : NULL;
        owner_name = owner_name ? owner_name + 1 : owner;
        if (usage->kind == kind && usage->ref_name && owner_name &&
            strcmp(owner_name, caller) == 0 && strcmp(usage->ref_name, target) == 0) {
            count++;
        }
    }
    return count;
}

static int rp_raw_source_callable_def_count(const CBMFileResult *result, const char *source_path) {
    /* Kotlin raw results also contain synthetic kotlin.Any method defs. This
     * helper verifies the fixture's own source declarations, not builtins. */
    int count = 0;
    for (int i = 0; result && i < result->defs.count; i++) {
        const char *file_path = result->defs.items[i].file_path;
        if (!file_path || !source_path || strcmp(file_path, source_path) != 0)
            continue;
        const char *label = result->defs.items[i].label;
        if (label && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0 ||
                      strcmp(label, "Constructor") == 0)) {
            count++;
        }
    }
    return count;
}

TEST(repro_kotlin_property_reference_does_not_borrow_same_name_function) {
    static const RFile files[] = {
        {"Holder.kt", "class Holder { val handler: Int = 1 }\n"},
        {"Functions.kt", "fun handler(): Unit {}\n"},
        {"Use.kt", "fun propertyReference(holder: Holder): kotlin.reflect.KProperty0<Int> = "
                   "holder::handler\n"},
    };
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_KOTLIN, files[0].name));
    ASSERT_TRUE(rp_extracts_clean(files[1].content, CBM_LANG_KOTLIN, files[1].name));
    ASSERT_TRUE(rp_extracts_clean(files[2].content, CBM_LANG_KOTLIN, files[2].name));

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 3);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin property-reference collision fixture did not produce a graph store");
    }
    int property_node =
        rp_node_count(store, project.project, "Variable", "handler", ".Holder.handler");
    int function_node =
        rp_node_count(store, project.project, "Function", "handler", ".Functions.handler");
    int property_usage = rp_edge_count(store, project.project, "USAGE", "propertyReference",
                                       "Variable", "handler", ".Holder.handler");
    int wrong_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "propertyReference", "Function",
                      "handler", ".Functions.handler");
    int property_promoted =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "propertyReference", "Variable",
                      "handler", ".Holder.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(property_node, 1);
    ASSERT_GTE(function_node, 1);
    ASSERT_EQ(property_usage, 1);
    ASSERT_EQ(wrong_reference, 0);
    ASSERT_EQ(property_promoted, 0);
    PASS();
}

TEST(repro_kotlin_constructor_reference_requires_concrete_constructor) {
    /* Inferred return types keep the Class occurrence unique to `::Class`;
     * spelling the class again in an annotation would be a legitimate USAGE. */
    static const RFile files[] = {{
        "Constructors.kt",
        "class Explicit { constructor(seed: Int) {} }\n"
        "class Implicit\n"
        "fun explicitReference() = ::Explicit\n"
        "fun implicitReference() = ::Implicit\n",
    }};
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_KOTLIN, files[0].name));

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 1);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin constructor-reference fixture did not produce a graph store");
    }
    int concrete_constructor = rp_constructor_node_count(store, project.project, "Explicit");
    int explicit_reference = rp_constructor_edge_count(store, project.project, "CALL_REFERENCE",
                                                       "explicitReference", "Explicit");
    int explicit_class_usage = rp_edge_count(store, project.project, "USAGE", "explicitReference",
                                             "Class", "Explicit", ".Explicit");
    int implicit_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                           "implicitReference", NULL, NULL, NULL);
    int implicit_class_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "implicitReference", "Class",
                      "Implicit", ".Implicit");
    int implicit_class_usage = rp_edge_count(store, project.project, "USAGE", "implicitReference",
                                             "Class", "Implicit", ".Implicit");
    rh_cleanup(&project, store);

    ASSERT_GTE(concrete_constructor, 1);
    ASSERT_EQ(explicit_reference, 1);
    ASSERT_EQ(explicit_class_usage, 0);
    ASSERT_EQ(implicit_reference, 0);
    ASSERT_EQ(implicit_class_reference, 0);
    ASSERT_EQ(implicit_class_usage, 1);
    PASS();
}

TEST(repro_kotlin_local_reference_never_binds_same_name_top_level) {
    static const RFile files[] = {
        {"Other.kt", "fun handler(): Unit {}\n"},
        {"Use.kt", "fun localReference(): Unit {\n"
                   "  fun handler(): Unit {}\n"
                   "  val callback = ::handler\n"
                   "  callback()\n"
                   "}\n"},
    };
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_KOTLIN, files[0].name));
    ASSERT_TRUE(rp_extracts_clean(files[1].content, CBM_LANG_KOTLIN, files[1].name));

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin local callable-reference fixture did not produce a graph store");
    }
    int top_level_node =
        rp_node_count(store, project.project, "Function", "handler", ".Other.handler");
    int local_node = rp_node_count(store, project.project, "Function", "handler", ".Use.handler");
    int local_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "localReference",
                                        "Function", "handler", ".Use.handler");
    int any_precise_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "localReference", NULL, NULL, NULL);
    int wrong_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "localReference",
                                        "Function", "handler", ".Other.handler");
    int wrong_usage = rp_edge_count(store, project.project, "USAGE", "localReference", "Function",
                                    "handler", ".Other.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(top_level_node, 1);
    ASSERT_EQ(wrong_reference, 0);
    ASSERT_EQ(wrong_usage, 0);
    /* A future local-function node may be referenced precisely. Until such a
     * node exists, the explicit syntax must remain non-precise. */
    ASSERT_TRUE(local_node > 0 ? local_reference == 1 : any_precise_reference == 0);
    PASS();
}

TEST(repro_php_dynamic_first_class_terminal_is_not_precise) {
    static const char source[] =
        "<?php\n"
        "class DynamicTarget { public function concrete(): void {} }\n"
        "function method(): void {}\n"
        "function dynamicReference(DynamicTarget $object, string $method): callable {\n"
        "  return $object->$method(...);\n"
        "}\n";
    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PHP, "repro",
                                          "Dynamic.php", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    int ordinary_terminal = rp_raw_usage_count(raw, "dynamicReference", "$method", CBM_USAGE_VALUE);
    int precise_terminal =
        rp_raw_usage_count(raw, "dynamicReference", "$method", CBM_USAGE_CALL_REFERENCE);
    cbm_free_result(raw);
    ASSERT_EQ(ordinary_terminal, 1);
    ASSERT_EQ(precise_terminal, 0);

    RProj project;
    cbm_store_t *store = rh_index(&project, "Dynamic.php", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("PHP dynamic first-class callable fixture did not produce a graph store");
    }
    int fabricated_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                             "dynamicReference", NULL, NULL, NULL);
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "dynamicReference",
                                        "Function", "method", ".Dynamic.method");
    rh_cleanup(&project, store);

    ASSERT_EQ(fabricated_reference, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(repro_php_static_magic_first_class_reference_targets_callstatic) {
    static const char source[] =
        "<?php\n"
        "class MagicStatic {\n"
        "  public static function __callStatic(string $name, array $arguments): mixed {\n"
        "    return null;\n"
        "  }\n"
        "}\n"
        "function staticMagicReference(): callable { return MagicStatic::missing(...); }\n";
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_PHP, "MagicStatic.php"));

    RProj project;
    cbm_store_t *store = rh_index(&project, "MagicStatic.php", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("PHP __callStatic reference fixture did not produce a graph store");
    }
    int magic_node = rp_node_count(store, project.project, "Method", "__callStatic",
                                   ".MagicStatic.__callStatic");
    int magic_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "staticMagicReference", "Method",
                      "__callStatic", ".MagicStatic.__callStatic");
    int missing_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                          "staticMagicReference", NULL, "missing", NULL);
    int magic_call = rp_edge_count(store, project.project, "CALLS", "staticMagicReference",
                                   "Method", "__callStatic", ".MagicStatic.__callStatic");
    int magic_usage = rp_edge_count(store, project.project, "USAGE", "staticMagicReference",
                                    "Method", "__callStatic", ".MagicStatic.__callStatic");
    rh_cleanup(&project, store);

    ASSERT_GTE(magic_node, 1);
    ASSERT_EQ(magic_reference, 1);
    ASSERT_EQ(missing_reference, 0);
    ASSERT_EQ(magic_call, 0);
    ASSERT_EQ(magic_usage, 0);
    PASS();
}

TEST(repro_php_typed_invokable_first_class_reference_targets_invoke) {
    static const char source[] =
        "<?php\n"
        "class Invokable {\n"
        "  public function __invoke(int $value): int { return $value; }\n"
        "}\n"
        "function invokableReference(Invokable $handler): callable { return $handler(...); }\n";
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_PHP, "Invokable.php"));

    RProj project;
    cbm_store_t *store = rh_index(&project, "Invokable.php", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("PHP invokable-object reference fixture did not produce a graph store");
    }
    int invoke_node =
        rp_node_count(store, project.project, "Method", "__invoke", ".Invokable.__invoke");
    int invoke_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "invokableReference", "Method",
                      "__invoke", ".Invokable.__invoke");
    int invoke_call = rp_edge_count(store, project.project, "CALLS", "invokableReference", "Method",
                                    "__invoke", ".Invokable.__invoke");
    int invoke_usage = rp_edge_count(store, project.project, "USAGE", "invokableReference",
                                     "Method", "__invoke", ".Invokable.__invoke");
    rh_cleanup(&project, store);

    ASSERT_GTE(invoke_node, 1);
    ASSERT_EQ(invoke_reference, 1);
    ASSERT_EQ(invoke_call, 0);
    ASSERT_EQ(invoke_usage, 0);
    PASS();
}

TEST(repro_java_same_name_receiver_calls_keep_occurrence_identity) {
    static const char source[] = "class Alpha { void handle() {} }\n"
                                 "class Beta { void handle() {} }\n"
                                 "class Calls {\n"
                                 "  static void callBoth(Alpha alpha, Beta beta) {\n"
                                 "    alpha.handle();\n"
                                 "    beta.handle();\n"
                                 "  }\n"
                                 "}\n";
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_JAVA, "Calls.java"));

    RProj project;
    cbm_store_t *store = rh_index(&project, "Calls.java", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Java same-name receiver-call fixture did not produce a graph store");
    }
    int alpha_node = rp_node_count(store, project.project, "Method", "handle", ".Alpha.handle");
    int beta_node = rp_node_count(store, project.project, "Method", "handle", ".Beta.handle");
    int alpha_call = rp_edge_count(store, project.project, "CALLS", "callBoth", "Method", "handle",
                                   ".Alpha.handle");
    int beta_call = rp_edge_count(store, project.project, "CALLS", "callBoth", "Method", "handle",
                                  ".Beta.handle");
    rh_cleanup(&project, store);

    ASSERT_GTE(alpha_node, 1);
    ASSERT_GTE(beta_node, 1);
    ASSERT_EQ(alpha_call, 1);
    ASSERT_EQ(beta_call, 1);
    PASS();
}

TEST(repro_typescript_function_value_upgrades_only_with_exact_semantic_target) {
    static const RFile files[] = {
        {"target.ts", "export function handler(): void {}\n"},
        {"use.ts", "import { handler } from './target';\n"
                   "function accept(callback: () => void): void {}\n"
                   "function consume(value: number): void {}\n"
                   "export function exactReference(): void { accept(handler); }\n"
                   "export function shadowedValue(handler: number): void { consume(handler); }\n"},
    };
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_TYPESCRIPT, files[0].name));
    ASSERT_TRUE(rp_extracts_clean(files[1].content, CBM_LANG_TYPESCRIPT, files[1].name));

    CBMFileResult *raw =
        cbm_extract_file(files[1].content, (int)strlen(files[1].content), CBM_LANG_TYPESCRIPT,
                         "repro", files[1].name, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_EQ(rp_raw_usage_count(raw, "exactReference", "handler", CBM_USAGE_VALUE), 1);
    ASSERT_EQ(rp_raw_usage_count(raw, "exactReference", "handler", CBM_USAGE_CALL_REFERENCE), 0);
    ASSERT_EQ(rp_raw_usage_count(raw, "shadowedValue", "handler", CBM_USAGE_VALUE), 1);
    cbm_free_result(raw);

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("TypeScript function-value precision fixture did not produce a graph store");
    }
    int function_node =
        rp_node_count(store, project.project, "Function", "handler", ".target.handler");
    int exact_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "exactReference",
                                        "Function", "handler", ".target.handler");
    int exact_usage = rp_edge_count(store, project.project, "USAGE", "exactReference", "Function",
                                    "handler", ".target.handler");
    int exact_call = rp_edge_count(store, project.project, "CALLS", "exactReference", "Function",
                                   "handler", ".target.handler");
    int shadow_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "shadowedValue",
                                         "Function", "handler", ".target.handler");
    int shadow_usage = rp_edge_count(store, project.project, "USAGE", "shadowedValue", "Function",
                                     "handler", ".target.handler");
    int shadow_call = rp_edge_count(store, project.project, "CALLS", "shadowedValue", "Function",
                                    "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(function_node, 1);
    ASSERT_EQ(exact_reference, 1);
    ASSERT_EQ(exact_usage, 0);
    ASSERT_EQ(exact_call, 0);
    ASSERT_EQ(shadow_reference, 0);
    ASSERT_EQ(shadow_usage, 0);
    ASSERT_EQ(shadow_call, 0);
    PASS();
}

TEST(repro_kotlin_empty_cross_filter_does_not_fail_open) {
    static const RFile files[] = {
        {"src/main/kotlin/unrelated/Unrelated.kt", "package unrelated\n"
                                                   "fun handler(): Unit {}\n"},
        {"src/main/kotlin/consumer/Use.kt", "package consumer\n"
                                            "val leakedReference: () -> Unit = ::handler\n"},
    };
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_KOTLIN, files[0].name));
    ASSERT_TRUE(rp_extracts_clean(files[1].content, CBM_LANG_KOTLIN, files[1].name));

    CBMFileResult *consumer =
        cbm_extract_file(files[1].content, (int)strlen(files[1].content), CBM_LANG_KOTLIN, "repro",
                         files[1].name, 0, NULL, NULL);
    ASSERT_NOT_NULL(consumer);
    int own_callable_defs = rp_raw_source_callable_def_count(consumer, files[1].name);
    cbm_free_result(consumer);
    ASSERT_EQ(own_callable_defs, 0);

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin empty cross-filter fixture did not produce a graph store");
    }
    int unrelated_node =
        rp_node_count(store, project.project, "Function", "handler", ".Unrelated.handler");
    int leaked_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", NULL, "Function",
                                         "handler", ".Unrelated.handler");
    int leaked_usage = rp_edge_count(store, project.project, "USAGE", NULL, "Function", "handler",
                                     ".Unrelated.handler");
    int leaked_call = rp_edge_count(store, project.project, "CALLS", NULL, "Function", "handler",
                                    ".Unrelated.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(unrelated_node, 1);
    ASSERT_EQ(leaked_reference, 0);
    ASSERT_EQ(leaked_usage, 0);
    ASSERT_EQ(leaked_call, 0);
    PASS();
}

static int rp_assert_callable_alias_graph(const char *filename, const char *source,
                                          CBMLanguage language, const char *caller,
                                          const char *callable_label) {
    ASSERT_TRUE(rp_extracts_clean(source, language, filename));

    RProj project;
    cbm_store_t *store = rh_index(&project, filename, source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("callable-alias fixture did not produce a graph store");
    }
    int shadowed_node = rp_node_count(store, project.project, callable_label, "shadowed", NULL);
    int actual_node = rp_node_count(store, project.project, callable_label, "actual", NULL);
    int wrong_call =
        rp_edge_count(store, project.project, "CALLS", caller, callable_label, "shadowed", NULL);
    int exact_call =
        rp_edge_count(store, project.project, "CALLS", caller, callable_label, "actual", NULL);
    rh_cleanup(&project, store);

    ASSERT_GTE(shadowed_node, 1);
    ASSERT_GTE(actual_node, 1);
    ASSERT_EQ(wrong_call, 0);
    ASSERT_EQ(exact_call, 1);
    return 0;
}

TEST(repro_go_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "package aliases\n"
                                 "func shadowed() int { return 1 }\n"
                                 "func actual() int { return 2 }\n"
                                 "func aliasCaller() int {\n"
                                 "  shadowed := actual\n"
                                 "  return shadowed()\n"
                                 "}\n";
    int rc = rp_assert_callable_alias_graph("aliases.go", source, CBM_LANG_GO, "aliasCaller",
                                            "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "from typing import Callable\n"
                                 "def shadowed() -> int:\n"
                                 "    return 1\n"
                                 "def actual() -> int:\n"
                                 "    return 2\n"
                                 "def aliasCaller() -> int:\n"
                                 "    shadowed: Callable[[], int] = actual\n"
                                 "    return shadowed()\n";
    int rc = rp_assert_callable_alias_graph("aliases.py", source, CBM_LANG_PYTHON, "aliasCaller",
                                            "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "fn shadowed() -> i32 { 1 }\n"
                                 "fn actual() -> i32 { 2 }\n"
                                 "fn aliasCaller() -> i32 {\n"
                                 "    let shadowed: fn() -> i32 = actual;\n"
                                 "    shadowed()\n"
                                 "}\n";
    int rc = rp_assert_callable_alias_graph("aliases.rs", source, CBM_LANG_RUST, "aliasCaller",
                                            "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "fun shadowed(): Int = 1\n"
                                 "fun actual(): Int = 2\n"
                                 "fun aliasCaller(): Int {\n"
                                 "  val shadowed = ::actual\n"
                                 "  return shadowed()\n"
                                 "}\n";
    int rc = rp_assert_callable_alias_graph("Aliases.kt", source, CBM_LANG_KOTLIN, "aliasCaller",
                                            "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_csharp_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "using System;\n"
                                 "class Aliases {\n"
                                 "  static int shadowed() { return 1; }\n"
                                 "  static int actual() { return 2; }\n"
                                 "  static int aliasCaller() {\n"
                                 "    Func<int> shadowed = actual;\n"
                                 "    return shadowed();\n"
                                 "  }\n"
                                 "}\n";
    int rc = rp_assert_callable_alias_graph("Aliases.cs", source, CBM_LANG_CSHARP, "aliasCaller",
                                            "Method");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_exact_function_value_reference(const char *filename, const char *source,
                                                    CBMLanguage language,
                                                    const char *callable_label) {
    ASSERT_TRUE(rp_extracts_clean(source, language, filename));

    RProj project;
    cbm_store_t *store = rh_index(&project, filename, source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("function-value fixture did not produce a graph store");
    }
    int target_node = rp_node_count(store, project.project, callable_label, "handler", NULL);
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "argument",
                                          callable_label, "handler", NULL);
    int ordinary_usage =
        rp_edge_count(store, project.project, "USAGE", "argument", callable_label, "handler", NULL);
    int fabricated_call =
        rp_edge_count(store, project.project, "CALLS", "argument", callable_label, "handler", NULL);
    rh_cleanup(&project, store);

    ASSERT_GTE(target_node, 1);
    ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    return 0;
}

TEST(repro_go_exact_function_value_is_call_reference) {
    static const char source[] = "package refs\n"
                                 "func handler() {}\n"
                                 "func accept(callback func()) {}\n"
                                 "func argument() { accept(handler) }\n";
    int rc = rp_assert_exact_function_value_reference("refs.go", source, CBM_LANG_GO, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_exact_function_value_is_call_reference) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument():\n"
                                 "    accept(handler)\n";
    int rc =
        rp_assert_exact_function_value_reference("refs.py", source, CBM_LANG_PYTHON, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_javascript_exact_function_value_is_call_reference) {
    static const char source[] = "function handler() {}\n"
                                 "function accept(callback) {}\n"
                                 "function argument() { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.js", source, CBM_LANG_JAVASCRIPT,
                                                      "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_c_exact_function_value_is_call_reference) {
    static const char source[] = "typedef void (*callback_t)(void);\n"
                                 "void handler(void) {}\n"
                                 "void accept(callback_t callback) {}\n"
                                 "void argument(void) { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.c", source, CBM_LANG_C, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_cpp_exact_function_value_is_call_reference) {
    static const char source[] = "using Callback = void (*)();\n"
                                 "void handler() {}\n"
                                 "void accept(Callback callback) {}\n"
                                 "void argument() { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.cpp", source, CBM_LANG_CPP, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_exact_function_value_is_call_reference) {
    static const char source[] = "fn handler() {}\n"
                                 "fn accept(_callback: fn()) {}\n"
                                 "fn argument() { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.rs", source, CBM_LANG_RUST, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_direct_scoped_function_value_is_exact_reference) {
    static const RFile files[] = {
        {"target.rs", "pub fn handler() {}\n"},
        {"decoy.rs", "pub fn handler() {}\n"},
        {"main.rs", "mod target;\n"
                    "mod decoy;\n"
                    "fn accept(_callback: fn()) {}\n"
                    "fn scoped_argument() { accept(target::handler); }\n"},
    };
    const char *source = files[2].content;
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_TRUE(rp_extracts_clean(files[i].content, CBM_LANG_RUST, files[i].name));
    }

    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST, "repro",
                                          files[2].name, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);

    CBMRustLSPDef defs[3];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "repro.target.handler";
    defs[0].short_name = "handler";
    defs[0].label = "Function";
    defs[0].def_module_qn = "repro.target";
    defs[1].qualified_name = "repro.decoy.handler";
    defs[1].short_name = "handler";
    defs[1].label = "Function";
    defs[1].def_module_qn = "repro.decoy";
    defs[2].qualified_name = "repro.main.accept";
    defs[2].short_name = "accept";
    defs[2].label = "Function";
    defs[2].def_module_qn = "repro.main";
    cbm_run_rust_lsp_cross(&raw->arena, source, (int)strlen(source), "repro.main", defs, 3, NULL,
                           NULL, 0, NULL, &raw->resolved_calls);

    const char *site = strstr(source, "target::handler");
    ASSERT_NOT_NULL(site);
    uint32_t start = (uint32_t)(site - source);
    uint32_t end = start + (uint32_t)strlen("target::handler");
    const CBMUsage *site_usage = NULL;
    for (int i = 0; i < raw->usages.count; i++) {
        const CBMUsage *usage = &raw->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->ref_name &&
            strcmp(usage->ref_name, "target::handler") == 0 && usage->enclosing_func_qn &&
            rp_qn_ends_with(usage->enclosing_func_qn, "scoped_argument") &&
            usage->site_start_byte == start && usage->site_end_byte == end) {
            site_usage = usage;
            break;
        }
    }

    int target_reference_rows = 0;
    int decoy_reference_rows = 0;
    int target_invocation_rows = 0;
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (!resolved->caller_qn || !rp_qn_ends_with(resolved->caller_qn, "scoped_argument") ||
            resolved->site_start_byte != start || resolved->site_end_byte != end ||
            !resolved->callee_qn) {
            continue;
        }
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE &&
            rp_qn_ends_with(resolved->callee_qn, ".target.handler")) {
            target_reference_rows++;
        }
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE &&
            rp_qn_ends_with(resolved->callee_qn, ".decoy.handler")) {
            decoy_reference_rows++;
        }
        if (resolved->kind == CBM_RESOLVED_INVOCATION &&
            rp_qn_ends_with(resolved->callee_qn, ".target.handler")) {
            target_invocation_rows++;
        }
    }
    const CBMResolvedCall *joined =
        site_usage ? cbm_pipeline_find_lsp_reference(&raw->resolved_calls, site_usage, false)
                   : NULL;
    bool semantic_join_is_exact = joined && joined->kind == CBM_RESOLVED_CALL_REFERENCE &&
                                  joined->callee_qn &&
                                  rp_qn_ends_with(joined->callee_qn, ".target.handler");
    bool has_site_usage = site_usage != NULL;
    bool site_is_reference_candidate = site_usage && site_usage->may_be_call_reference;
    cbm_free_result(raw);

    ASSERT_TRUE(has_site_usage);
    ASSERT_EQ(target_reference_rows, 1);
    ASSERT_EQ(decoy_reference_rows, 0);
    ASSERT_EQ(target_invocation_rows, 0);
    ASSERT_TRUE(semantic_join_is_exact);
    ASSERT_TRUE(site_is_reference_candidate);

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 3);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Rust scoped function-value fixture did not produce a graph store");
    }
    int target_node =
        rp_node_count(store, project.project, "Function", "handler", ".target.handler");
    int decoy_node = rp_node_count(store, project.project, "Function", "handler", ".decoy.handler");
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "scoped_argument",
                                       "Function", "accept", NULL);
    int target_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "scoped_argument", "Function",
                      "handler", ".target.handler");
    int target_usage = rp_edge_count(store, project.project, "USAGE", "scoped_argument", "Function",
                                     "handler", ".target.handler");
    int target_call = rp_edge_count(store, project.project, "CALLS", "scoped_argument", "Function",
                                    "handler", ".target.handler");
    int decoy_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "scoped_argument",
                                        "Function", "handler", ".decoy.handler");
    int decoy_usage = rp_edge_count(store, project.project, "USAGE", "scoped_argument", "Function",
                                    "handler", ".decoy.handler");
    int decoy_call = rp_edge_count(store, project.project, "CALLS", "scoped_argument", "Function",
                                   "handler", ".decoy.handler");
    rh_cleanup(&project, store);

    ASSERT_EQ(target_node, 1);
    ASSERT_EQ(decoy_node, 1);
    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(target_reference, 1);
    ASSERT_EQ(target_usage, 0);
    ASSERT_EQ(target_call, 0);
    ASSERT_EQ(decoy_reference, 0);
    ASSERT_EQ(decoy_usage, 0);
    ASSERT_EQ(decoy_call, 0);
    PASS();
}

TEST(repro_csharp_exact_function_value_is_call_reference) {
    static const char source[] = "using System;\n"
                                 "class Refs {\n"
                                 "  static void handler() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void argument() { accept(handler); }\n"
                                 "}\n";
    int rc = rp_assert_exact_function_value_reference("Refs.cs", source, CBM_LANG_CSHARP, "Method");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_csharp_callable_value_shape(const char *filename, const char *source,
                                                 const char *caller, const char *target_qn_suffix) {
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_CSHARP, filename));

    RProj project;
    cbm_store_t *store = rh_index(&project, filename, source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("C# callable-value shape fixture did not produce a graph store");
    }
    int target_node = rp_node_count(store, project.project, "Method", "handler", target_qn_suffix);
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", caller,
                                          "Method", "handler", target_qn_suffix);
    int ordinary_usage = rp_edge_count(store, project.project, "USAGE", caller, "Method", "handler",
                                       target_qn_suffix);
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", caller, "Method",
                                        "handler", target_qn_suffix);
    rh_cleanup(&project, store);

    if (target_node != 1 || precise_reference != 1 || ordinary_usage != 0 || fabricated_call != 0) {
        fprintf(stderr,
                "  [csharp-reference-shape] caller=%s target=%s node=%d reference=%d "
                "usage=%d call=%d\n",
                caller, target_qn_suffix, target_node, precise_reference, ordinary_usage,
                fabricated_call);
    }

    ASSERT_EQ(target_node, 1);
    ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    return 0;
}

/* C# callable-value resolution already proves the exact Service.handler
 * target.  The graph contract requires that proof to join the raw carrier at
 * this member-access occurrence, rather than leaving an ordinary USAGE. */
TEST(repro_csharp_member_method_group_is_exact_reference) {
    static const char source[] = "using System;\n"
                                 "class Service { public void handler() {} }\n"
                                 "class MemberReference {\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void memberArgument(Service s) { accept(s.handler); }\n"
                                 "}\n";
    int rc = rp_assert_csharp_callable_value_shape("MemberReference.cs", source, "memberArgument",
                                                   ".Service.handler");
    if (rc != 0)
        return rc;
    PASS();
}

/* Parentheses do not make a single method group ambiguous.  They must not
 * prevent the exact semantic target from joining the inner raw carrier. */
TEST(repro_csharp_parenthesized_method_group_is_exact_reference) {
    static const char source[] = "using System;\n"
                                 "class ParenthesizedReference {\n"
                                 "  static void handler() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void parenthesizedArgument() { accept((handler)); }\n"
                                 "}\n";
    int rc = rp_assert_csharp_callable_value_shape("ParenthesizedReference.cs", source,
                                                   "parenthesizedArgument",
                                                   ".ParenthesizedReference.handler");
    if (rc != 0)
        return rc;
    PASS();
}

/* An explicitly constructed generic method group is one exact callable
 * value: handler<int> has no overload or inference ambiguity in this fixture. */
TEST(repro_csharp_generic_method_group_is_exact_reference) {
    static const char source[] = "using System;\n"
                                 "class GenericReference {\n"
                                 "  static void handler<T>() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void genericArgument() { accept(handler<int>); }\n"
                                 "}\n";
    int rc = rp_assert_csharp_callable_value_shape("GenericReference.cs", source, "genericArgument",
                                                   ".GenericReference.handler");
    if (rc != 0)
        return rc;
    PASS();
}

/* Both imported static classes contain an equally applicable method group.
 * C# must fail closed: the parser keeps an ordinary value usage, but the
 * semantic pass and graph may not select either target precisely. */
TEST(repro_csharp_ambiguous_using_static_method_group_is_not_precise) {
    static const char source[] = "using System;\n"
                                 "using static A;\n"
                                 "using static B;\n"
                                 "class A { public static void handler() {} }\n"
                                 "class B { public static void handler() {} }\n"
                                 "class AmbiguousStaticReference {\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void ambiguousArgument() { accept(handler); }\n"
                                 "}\n";

    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_CSHARP, "repro",
                                          "AmbiguousStaticReference.cs", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument = strstr(source, "accept(handler)");
    ASSERT_NOT_NULL(argument);
    uint32_t site_start = (uint32_t)(argument - source) + 7U;
    uint32_t site_end = site_start + (uint32_t)strlen("handler");
    int ordinary_usage = rp_raw_usage_count(raw, "ambiguousArgument", "handler", CBM_USAGE_VALUE);
    int precise_rows = 0;
    int a_rows = 0;
    int b_rows = 0;
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (resolved->kind != CBM_RESOLVED_CALL_REFERENCE || !resolved->caller_qn ||
            !rp_qn_ends_with(resolved->caller_qn, "ambiguousArgument") ||
            resolved->site_start_byte != site_start || resolved->site_end_byte != site_end) {
            continue;
        }
        precise_rows++;
        if (rp_qn_ends_with(resolved->callee_qn, ".A.handler"))
            a_rows++;
        if (rp_qn_ends_with(resolved->callee_qn, ".B.handler"))
            b_rows++;
    }
    cbm_free_result(raw);

    RProj project;
    cbm_store_t *store = rh_index(&project, "AmbiguousStaticReference.cs", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("ambiguous C# using-static fixture did not produce a graph store");
    }
    int a_node = rp_node_count(store, project.project, "Method", "handler", ".A.handler");
    int b_node = rp_node_count(store, project.project, "Method", "handler", ".B.handler");
    int a_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "ambiguousArgument",
                                    "Method", "handler", ".A.handler");
    int b_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "ambiguousArgument",
                                    "Method", "handler", ".B.handler");
    int a_call = rp_edge_count(store, project.project, "CALLS", "ambiguousArgument", "Method",
                               "handler", ".A.handler");
    int b_call = rp_edge_count(store, project.project, "CALLS", "ambiguousArgument", "Method",
                               "handler", ".B.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(ordinary_usage, 1);
    ASSERT_EQ(a_node, 1);
    ASSERT_EQ(b_node, 1);
    ASSERT_EQ(precise_rows, 0);
    ASSERT_EQ(a_rows, 0);
    ASSERT_EQ(b_rows, 0);
    ASSERT_EQ(a_reference, 0);
    ASSERT_EQ(b_reference, 0);
    ASSERT_EQ(a_call, 0);
    ASSERT_EQ(b_call, 0);
    PASS();
}

TEST(repro_cuda_exact_function_value_is_call_reference) {
    static const char source[] = "using Callback = void (*)();\n"
                                 "void handler() {}\n"
                                 "void accept(Callback callback) {}\n"
                                 "void argument() { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.cu", source, CBM_LANG_CUDA, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

typedef struct {
    const char *tag;
    const char *filename;
    const char *source;
    CBMLanguage language;
} RPExactReferenceSiteCase;

static int rp_check_exact_function_value_lsp_site(const RPExactReferenceSiteCase *c) {
    CBMFileResult *result = cbm_extract_file(c->source, (int)strlen(c->source), c->language,
                                             "repro", c->filename, 0, NULL, NULL);
    if (!result) {
        fprintf(stderr, "  [reference-site] lang=%s invariant=extract_result\n", c->tag);
        return 1;
    }
    const char *argument = strstr(c->source, "accept(handler)");
    const uint32_t start = argument ? (uint32_t)(argument - c->source) + 7U : 0U;
    const uint32_t end = start + (uint32_t)strlen("handler");
    const bool ts_family = c->language == CBM_LANG_JAVASCRIPT ||
                           c->language == CBM_LANG_TYPESCRIPT || c->language == CBM_LANG_TSX;
    CBMArena cross_arena;
    CBMResolvedCallArray cross_resolved = {0};
    if (ts_family) {
        /* TS/JS value references intentionally wait for the project-wide pass:
         * only that registry proves that the target is materialized. Exercise
         * that production phase while retaining the exact source-site oracle. */
        CBMLSPDef defs[] = {
            {.qualified_name = "repro.refs.handler",
             .short_name = "handler",
             .label = "Function",
             .def_module_qn = "repro.refs",
             .lang = c->language},
            {.qualified_name = "repro.refs.accept",
             .short_name = "accept",
             .label = "Function",
             .def_module_qn = "repro.refs",
             .lang = c->language},
            {.qualified_name = "repro.refs.argument",
             .short_name = "argument",
             .label = "Function",
             .def_module_qn = "repro.refs",
             .lang = c->language},
        };
        cbm_arena_init(&cross_arena);
        cbm_run_ts_lsp_cross(&cross_arena, c->source, (int)strlen(c->source), "repro.refs",
                             c->language == CBM_LANG_JAVASCRIPT, c->language == CBM_LANG_TSX, false,
                             defs, (int)(sizeof(defs) / sizeof(defs[0])), NULL, NULL, 0, NULL,
                             &cross_resolved);
    }
    const CBMResolvedCallArray *resolved_calls =
        ts_family ? &cross_resolved : &result->resolved_calls;
    int exact_references = 0;
    int handler_invocations = 0;
    for (int i = 0; i < resolved_calls->count; i++) {
        const CBMResolvedCall *resolved = &resolved_calls->items[i];
        if (!resolved->caller_qn || !rp_qn_ends_with(resolved->caller_qn, "argument") ||
            !resolved->callee_qn || !rp_qn_ends_with(resolved->callee_qn, "handler")) {
            continue;
        }
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE && resolved->site_start_byte == start &&
            resolved->site_end_byte == end) {
            exact_references++;
        }
        if (resolved->kind == CBM_RESOLVED_INVOCATION && resolved->site_start_byte == start &&
            resolved->site_end_byte == end) {
            handler_invocations++;
        }
    }
    int failures = 0;
    if (result->has_error || result->parse_incomplete) {
        fprintf(stderr, "  [reference-site] lang=%s invariant=valid_fixture\n", c->tag);
        failures++;
    }
    if (exact_references != 1) {
        fprintf(stderr,
                "  [reference-site] lang=%s invariant=exact_reference expected=1 actual=%d "
                "site=%u:%u\n",
                c->tag, exact_references, start, end);
        failures++;
    }
    if (handler_invocations != 0) {
        fprintf(stderr, "  [reference-site] lang=%s invariant=argument_not_invocation actual=%d\n",
                c->tag, handler_invocations);
        failures++;
    }
    if (ts_family) {
        cbm_arena_destroy(&cross_arena);
    }
    cbm_free_result(result);
    return failures;
}

TEST(repro_exact_function_value_lsp_sites) {
    static const RPExactReferenceSiteCase cases[] = {
        {"go", "refs.go",
         "package refs\n"
         "func handler() {}\n"
         "func accept(callback func()) {}\n"
         "func argument() { accept(handler) }\n",
         CBM_LANG_GO},
        {"python", "refs.py",
         "def handler():\n    pass\n"
         "def accept(callback):\n    pass\n"
         "def argument():\n    accept(handler)\n",
         CBM_LANG_PYTHON},
        {"javascript", "refs.js",
         "function handler() {}\n"
         "function accept(callback) {}\n"
         "function argument() { accept(handler); }\n",
         CBM_LANG_JAVASCRIPT},
        {"typescript", "refs.ts",
         "function handler(): void {}\n"
         "function accept(callback: () => void): void {}\n"
         "function argument(): void { accept(handler); }\n",
         CBM_LANG_TYPESCRIPT},
        {"tsx", "refs.tsx",
         "function handler(): void {}\n"
         "function accept(callback: () => void): void {}\n"
         "function argument(): void { accept(handler); }\n",
         CBM_LANG_TSX},
        {"c", "refs.c",
         "typedef void (*callback_t)(void);\n"
         "void handler(void) {}\n"
         "void accept(callback_t callback) {}\n"
         "void argument(void) { accept(handler); }\n",
         CBM_LANG_C},
        {"cpp", "refs.cpp",
         "using Callback = void (*)();\n"
         "void handler() {}\n"
         "void accept(Callback callback) {}\n"
         "void argument() { accept(handler); }\n",
         CBM_LANG_CPP},
        {"cuda", "refs.cu",
         "using Callback = void (*)();\n"
         "void handler() {}\n"
         "void accept(Callback callback) {}\n"
         "void argument() { accept(handler); }\n",
         CBM_LANG_CUDA},
        {"rust", "refs.rs",
         "fn handler() {}\n"
         "fn accept(_callback: fn()) {}\n"
         "fn argument() { accept(handler); }\n",
         CBM_LANG_RUST},
        {"csharp", "Refs.cs",
         "using System;\n"
         "class Refs {\n"
         "  static void handler() {}\n"
         "  static void accept(Action callback) {}\n"
         "  static void argument() { accept(handler); }\n"
         "}\n",
         CBM_LANG_CSHARP},
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        failures += rp_check_exact_function_value_lsp_site(&cases[i]);
    }
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_typescript_exact_bound_method_is_call_reference) {
    static const char source[] =
        "class Service { handler(): void {} }\n"
        "function accept(callback: () => void): void {}\n"
        "function argument(service: Service): void { accept(service.handler); }\n";
    int rc =
        rp_assert_exact_function_value_reference("bound.ts", source, CBM_LANG_TYPESCRIPT, "Method");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_tsx_exact_bound_method_is_call_reference) {
    static const char source[] =
        "class Service { handler(): void {} }\n"
        "function accept(callback: () => void): void {}\n"
        "function argument(service: Service): void { accept(service.handler); }\n";
    int rc = rp_assert_exact_function_value_reference("bound.tsx", source, CBM_LANG_TSX, "Method");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_perl_exact_coderef_is_call_reference) {
    static const char source[] = "sub handler {}\n"
                                 "sub accept { my ($callback) = @_; }\n"
                                 "sub argument { accept(\\&handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.pl", source, CBM_LANG_PERL, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_cross_file_exact_function_value(const RFile *files, int file_count,
                                                     CBMLanguage language,
                                                     const char *callable_label) {
    for (int i = 0; i < file_count; i++) {
        ASSERT_TRUE(rp_extracts_clean(files[i].content, language, files[i].name));
    }
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, file_count);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("cross-file function-value fixture did not produce a graph store");
    }
    int target_node = rp_node_count(store, project.project, callable_label, "handler", NULL);
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                       callable_label, "accept", NULL);
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                          callable_label, "handler", NULL);
    int ordinary_usage = rp_edge_count(store, project.project, "USAGE", "crossArgument",
                                       callable_label, "handler", NULL);
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                        callable_label, "handler", NULL);
    rh_cleanup(&project, store);

    if (target_node < 1 || registrar_call != 1 || precise_reference != 1 || ordinary_usage != 0 ||
        fabricated_call != 0) {
        fprintf(stderr,
                "  [cross-reference] language=%d target=%d registrar=%d reference=%d "
                "usage=%d call=%d\n",
                (int)language, target_node, registrar_call, precise_reference, ordinary_usage,
                fabricated_call);
    }

    ASSERT_GTE(target_node, 1);
    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    return 0;
}

TEST(repro_go_cross_file_exact_function_value_is_call_reference) {
    static const RFile files[] = {
        {"target.go", "package refs\nfunc handler() {}\n"},
        {"use.go", "package refs\n"
                   "func accept(callback func()) {}\n"
                   "func crossArgument() { accept(handler) }\n"},
    };
    int rc = rp_assert_cross_file_exact_function_value(files, 2, CBM_LANG_GO, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_cross_file_exact_function_value_is_call_reference) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler\n"
                   "def accept(callback):\n    pass\n"
                   "def crossArgument():\n    accept(handler)\n"},
    };
    int rc = rp_assert_cross_file_exact_function_value(files, 2, CBM_LANG_PYTHON, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_aliased_import_function_value_is_call_reference) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler as callback\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    for (int i = 0; i < 2; i++)
        ASSERT_TRUE(rp_extracts_clean(files[i].content, CBM_LANG_PYTHON, files[i].name));
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python aliased from-import fixture did not produce a graph store");
    }
    int target_node =
        rp_node_count(store, project.project, "Function", "handler", ".target.handler");
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                       "Function", "accept", ".use.accept");
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                          "crossArgument", "Function", "handler",
                                          ".target.handler");
    int ordinary_usage = rp_edge_count(store, project.project, "USAGE", "crossArgument",
                                       "Function", "handler", ".target.handler");
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                        "Function", "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_EQ(target_node, 1);
    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(repro_python_alias_equal_module_leaf_targets_imported_member) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\ndef target():\n    pass\n"},
        {"use.py", "from target import handler as target\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(target)\n"},
    };
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python alias/module-leaf collision fixture did not produce a graph store");
    }
    int correct_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                          "crossArgument", "Function", "handler",
                                          ".target.handler");
    int wrong_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                        "crossArgument", "Function", "target", ".target.target");
    rh_cleanup(&project, store);

    ASSERT_EQ(correct_reference, 1);
    ASSERT_EQ(wrong_reference, 0);
    PASS();
}

TEST(repro_rust_cross_file_exact_function_value_is_call_reference) {
    static const RFile files[] = {
        {"target.rs", "pub fn handler() {}\n"},
        {"main.rs", "mod target;\n"
                    "use target::handler;\n"
                    "fn accept(_callback: fn()) {}\n"
                    "fn crossArgument() { accept(handler); }\n"},
    };
    int rc = rp_assert_cross_file_exact_function_value(files, 2, CBM_LANG_RUST, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_csharp_cross_file_exact_function_value_is_call_reference) {
    static const RFile files[] = {
        {"Target.cs", "class Target { public static void handler() {} }\n"},
        {"Use.cs", "using System;\n"
                   "using static Target;\n"
                   "class Use {\n"
                   "  static void accept(Action callback) {}\n"
                   "  static void crossArgument() { accept(handler); }\n"
                   "}\n"},
    };
    int rc = rp_assert_cross_file_exact_function_value(files, 2, CBM_LANG_CSHARP, "Method");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_cross_file_callable_alias_graph(const RFile *files, int file_count,
                                                     CBMLanguage language,
                                                     const char *callable_label) {
    for (int i = 0; i < file_count; i++) {
        ASSERT_TRUE(rp_extracts_clean(files[i].content, language, files[i].name));
    }
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, file_count);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("cross-file callable-alias fixture did not produce a graph store");
    }
    int wrong_call = rp_edge_count(store, project.project, "CALLS", "crossAliasCaller",
                                   callable_label, "shadowed", NULL);
    int exact_call = rp_edge_count(store, project.project, "CALLS", "crossAliasCaller",
                                   callable_label, "actual", NULL);
    rh_cleanup(&project, store);
    ASSERT_EQ(wrong_call, 0);
    ASSERT_EQ(exact_call, 1);
    return 0;
}

TEST(repro_csharp_cross_file_callable_alias_invocation_targets_exact_value) {
    static const RFile files[] = {
        {"Target.cs", "class Target { public static int actual() { return 2; } }\n"},
        {"Use.cs", "using System;\n"
                   "using static Target;\n"
                   "class Use {\n"
                   "  static int shadowed() { return 1; }\n"
                   "  static int crossAliasCaller() {\n"
                   "    Func<int> shadowed = actual;\n"
                   "    return shadowed();\n"
                   "  }\n"
                   "}\n"},
    };
    int rc = rp_assert_cross_file_callable_alias_graph(files, 2, CBM_LANG_CSHARP, "Method");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_cross_file_callable_alias_invocation_targets_exact_value) {
    static const RFile files[] = {
        {"Target.kt", "package target\nfun actual(): Int = 2\n"},
        {"Use.kt", "package use\n"
                   "import target.actual\n"
                   "fun shadowed(): Int = 1\n"
                   "fun crossAliasCaller(): Int {\n"
                   "  val shadowed = ::actual\n"
                   "  return shadowed()\n"
                   "}\n"},
    };
    int rc = rp_assert_cross_file_callable_alias_graph(files, 2, CBM_LANG_KOTLIN, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_alias_argument_exact_join(const char *filename, const char *source,
                                               CBMLanguage language, const char *caller,
                                               const char *target, bool allow_tail_match) {
    CBMFileResult *raw =
        cbm_extract_file(source, (int)strlen(source), language, "repro", filename, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument = NULL;
    const char *scan = source;
    while ((scan = strstr(scan, "accept(callback)")) != NULL) {
        argument = scan;
        scan++;
    }
    ASSERT_NOT_NULL(argument);
    uint32_t start = (uint32_t)(argument - source) + 7U;
    uint32_t end = start + (uint32_t)strlen("callback");
    const CBMUsage *site_usage = NULL;
    for (int i = 0; i < raw->usages.count; i++) {
        const CBMUsage *usage = &raw->usages.items[i];
        if (usage->ref_name && strcmp(usage->ref_name, "callback") == 0 &&
            usage->enclosing_func_qn && rp_qn_ends_with(usage->enclosing_func_qn, caller) &&
            usage->site_start_byte == start && usage->site_end_byte == end) {
            site_usage = usage;
            break;
        }
    }
    const CBMResolvedCall *joined =
        site_usage
            ? cbm_pipeline_find_lsp_reference(&raw->resolved_calls, site_usage, allow_tail_match)
            : NULL;
    bool exact_target = joined && joined->callee_qn && rp_qn_ends_with(joined->callee_qn, target);
    cbm_free_result(raw);
    ASSERT_NOT_NULL(site_usage);
    ASSERT_TRUE(exact_target);
    return 0;
}

TEST(repro_csharp_callable_alias_argument_is_exact_reference) {
    static const char source[] = "using System;\n"
                                 "class AliasArgument {\n"
                                 "  static void handler() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void argument() {\n"
                                 "    Action callback = handler;\n"
                                 "    accept(callback);\n"
                                 "  }\n"
                                 "}\n";
    int rc = rp_assert_alias_argument_exact_join("AliasArgument.cs", source, CBM_LANG_CSHARP,
                                                 "argument", "handler", false);
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_callable_alias_argument_is_exact_reference) {
    static const char source[] = "fun handler(): Unit {}\n"
                                 "fun accept(callback: () -> Unit): Unit {}\n"
                                 "fun argument(): Unit {\n"
                                 "  val callback = ::handler\n"
                                 "  accept(callback)\n"
                                 "}\n";
    int rc = rp_assert_alias_argument_exact_join("AliasArgument.kt", source, CBM_LANG_KOTLIN,
                                                 "argument", "handler", true);
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_receiver_bound_alias_invocation_targets_exact_method) {
    static const char source[] = "class Service { fun run(): Int = 2 }\n"
                                 "fun shadowed(): Int = 1\n"
                                 "fun receiverAliasCaller(): Int {\n"
                                 "  val service = Service()\n"
                                 "  val shadowed = service::run\n"
                                 "  return shadowed()\n"
                                 "}\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "ReceiverAlias.kt", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin receiver-alias fixture did not produce a graph store");
    }
    int wrong_call = rp_edge_count(store, project.project, "CALLS", "receiverAliasCaller",
                                   "Function", "shadowed", NULL);
    int exact_call = rp_edge_count(store, project.project, "CALLS", "receiverAliasCaller", "Method",
                                   "run", NULL);
    rh_cleanup(&project, store);
    ASSERT_EQ(wrong_call, 0);
    ASSERT_EQ(exact_call, 1);
    PASS();
}

TEST(repro_kotlin_unknown_imported_property_is_not_callable_reference) {
    static const RFile files[] = {
        {"Values.kt", "package values\nval handler: Int = 1\n"},
        {"Use.kt", "package consumer\n"
                   "import values.handler\n"
                   "fun importedPropertyReference(): kotlin.reflect.KProperty0<Int> = ::handler\n"},
    };
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin imported-property fixture did not produce a graph store");
    }
    int property_node = rp_node_count(store, project.project, "Variable", "handler", NULL);
    int precise_property = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                         "importedPropertyReference", "Variable", "handler", NULL);
    int property_usage = rp_edge_count(store, project.project, "USAGE", "importedPropertyReference",
                                       "Variable", "handler", NULL);
    rh_cleanup(&project, store);
    ASSERT_GTE(property_node, 1);
    ASSERT_EQ(precise_property, 0);
    ASSERT_EQ(property_usage, 1);
    PASS();
}

/* A branch/map expression can name callable definitions without selecting one
 * statically at the argument occurrence.  Those target mentions remain USAGE;
 * only an exact single-target value earns CALL_REFERENCE. */
static int rp_assert_complex_function_values_stay_usage(const char *filename, const char *source,
                                                        CBMLanguage language,
                                                        const char *callable_label) {
    ASSERT_TRUE(rp_extracts_clean(source, language, filename));

    RProj project;
    cbm_store_t *store = rh_index(&project, filename, source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("complex function-value fixture did not produce a graph store");
    }
    int handler_node = rp_node_count(store, project.project, callable_label, "handler", NULL);
    int alternate_node = rp_node_count(store, project.project, callable_label, "alternate", NULL);
    int registrar_call =
        rp_edge_count(store, project.project, "CALLS", "complex", callable_label, "accept", NULL);
    int handler_usage =
        rp_edge_count(store, project.project, "USAGE", "complex", callable_label, "handler", NULL);
    int alternate_usage = rp_edge_count(store, project.project, "USAGE", "complex", callable_label,
                                        "alternate", NULL);
    int handler_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "complex",
                                          callable_label, "handler", NULL);
    int alternate_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "complex",
                                            callable_label, "alternate", NULL);
    int handler_call =
        rp_edge_count(store, project.project, "CALLS", "complex", callable_label, "handler", NULL);
    int alternate_call = rp_edge_count(store, project.project, "CALLS", "complex", callable_label,
                                       "alternate", NULL);
    rh_cleanup(&project, store);

    if (handler_node < 1 || alternate_node < 1 || registrar_call != 1 || handler_usage != 1 ||
        alternate_usage != 1 || handler_reference != 0 || alternate_reference != 0 ||
        handler_call != 0 || alternate_call != 0) {
        fprintf(stderr,
                "  [complex-reference] file=%s nodes=%d/%d registrar=%d usage=%d/%d "
                "reference=%d/%d calls=%d/%d\n",
                filename, handler_node, alternate_node, registrar_call, handler_usage,
                alternate_usage, handler_reference, alternate_reference, handler_call,
                alternate_call);
    }

    ASSERT_GTE(handler_node, 1);
    ASSERT_GTE(alternate_node, 1);
    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(handler_usage, 1);
    ASSERT_EQ(alternate_usage, 1);
    ASSERT_EQ(handler_reference, 0);
    ASSERT_EQ(alternate_reference, 0);
    ASSERT_EQ(handler_call, 0);
    ASSERT_EQ(alternate_call, 0);
    return 0;
}

TEST(repro_javascript_complex_function_value_stays_usage) {
    static const char source[] = "function handler() {}\n"
                                 "function alternate() {}\n"
                                 "function accept(callback) {}\n"
                                 "function complex(flag) { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.js", source, CBM_LANG_JAVASCRIPT,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_typescript_complex_function_value_stays_usage) {
    static const char source[] =
        "function handler(): void {}\n"
        "function alternate(): void {}\n"
        "function accept(callback: () => void): void {}\n"
        "function complex(flag: boolean): void { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.ts", source, CBM_LANG_TYPESCRIPT,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_tsx_complex_function_value_stays_usage) {
    static const char source[] =
        "function handler(): void {}\n"
        "function alternate(): void {}\n"
        "function accept(callback: () => void): void {}\n"
        "function complex(flag: boolean): void { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.tsx", source, CBM_LANG_TSX,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_go_complex_function_value_stays_usage) {
    static const char source[] =
        "package refs\n"
        "func handler() {}\n"
        "func alternate() {}\n"
        "func accept(callback func()) {}\n"
        "func complex(flag bool) { "
        "accept(map[bool]func(){true: handler, false: alternate}[flag]) }\n";
    int rc =
        rp_assert_complex_function_values_stay_usage("complex.go", source, CBM_LANG_GO, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_complex_function_value_stays_usage) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def alternate():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def complex(flag):\n"
                                 "    accept(handler if flag else alternate)\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.py", source, CBM_LANG_PYTHON,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_c_complex_function_value_stays_usage) {
    static const char source[] = "typedef void (*callback_t)(void);\n"
                                 "void handler(void) {}\n"
                                 "void alternate(void) {}\n"
                                 "void accept(callback_t callback) {}\n"
                                 "void complex(int flag) { accept(flag ? handler : alternate); }\n";
    int rc =
        rp_assert_complex_function_values_stay_usage("complex.c", source, CBM_LANG_C, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_cpp_complex_function_value_stays_usage) {
    static const char source[] =
        "using Callback = void (*)();\n"
        "void handler() {}\n"
        "void alternate() {}\n"
        "void accept(Callback callback) {}\n"
        "void complex(bool flag) { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.cpp", source, CBM_LANG_CPP,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_cuda_complex_function_value_stays_usage) {
    static const char source[] =
        "using Callback = void (*)();\n"
        "void handler() {}\n"
        "void alternate() {}\n"
        "void accept(Callback callback) {}\n"
        "void complex(bool flag) { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.cu", source, CBM_LANG_CUDA,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_complex_function_value_stays_usage) {
    static const char source[] = "fn handler() {}\n"
                                 "fn alternate() {}\n"
                                 "fn accept(_callback: fn()) {}\n"
                                 "fn complex(flag: bool) { "
                                 "accept(if flag { handler } else { alternate }); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.rs", source, CBM_LANG_RUST,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_csharp_complex_function_value_stays_usage) {
    static const char source[] =
        "using System;\n"
        "class Refs {\n"
        "  static void handler() {}\n"
        "  static void alternate() {}\n"
        "  static void accept(Action callback) {}\n"
        "  static void complex(bool flag) { accept(flag ? handler : alternate); }\n"
        "}\n";
    int rc = rp_assert_complex_function_values_stay_usage("Complex.cs", source, CBM_LANG_CSHARP,
                                                          "Method");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_java_complex_method_reference_stays_usage) {
    static const char source[] = "interface Task { void run(); }\n"
                                 "class Refs {\n"
                                 "  static void handler() {}\n"
                                 "  static void alternate() {}\n"
                                 "  static void accept(Task callback) {}\n"
                                 "  static void complex(boolean flag) { "
                                 "accept(flag ? Refs::handler : Refs::alternate); }\n"
                                 "}\n";
    int rc =
        rp_assert_complex_function_values_stay_usage("Refs.java", source, CBM_LANG_JAVA, "Method");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_complex_callable_reference_stays_usage) {
    static const char source[] = "fun handler() {}\n"
                                 "fun alternate() {}\n"
                                 "fun accept(callback: () -> Unit) {}\n"
                                 "fun complex(flag: Boolean) { "
                                 "accept(if (flag) ::handler else ::alternate) }\n";
    int rc = rp_assert_complex_function_values_stay_usage("Refs.kt", source, CBM_LANG_KOTLIN,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_php_complex_first_class_callable_stays_usage) {
    static const char source[] = "<?php\n"
                                 "function handler(): void {}\n"
                                 "function alternate(): void {}\n"
                                 "function accept(callable $callback): void {}\n"
                                 "function complex(bool $flag): void { "
                                 "accept($flag ? handler(...) : alternate(...)); }\n";
    int rc =
        rp_assert_complex_function_values_stay_usage("refs.php", source, CBM_LANG_PHP, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_perl_conditional_coderefs_stay_usage) {
    static const char source[] = "sub handler {}\n"
                                 "sub alternate {}\n"
                                 "sub accept { my ($callback) = @_; }\n"
                                 "sub complex { my ($flag) = @_; "
                                 "accept($flag ? \\&handler : \\&alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.pl", source, CBM_LANG_PERL,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

/* A function-pointer binding is exact only until an assignment makes its
 * target multi-valued.  Keeping the initializer's target alive here would
 * fabricate a CALL_REFERENCE for the later argument occurrence. */
TEST(repro_c_ambiguous_alias_reassignment_drops_stale_target) {
    static const char source[] = "void actual(void) {}\n"
                                 "void alternate(void) {}\n"
                                 "void accept(void (*callback)(void)) {}\n"
                                 "void staleAliasCaller(int flag) {\n"
                                 "  void (*callback)(void) = actual;\n"
                                 "  callback = flag ? actual : alternate;\n"
                                 "  accept(callback);\n"
                                 "}\n";

    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_C, "stale_alias.c"));
    RProj project;
    cbm_store_t *store = rh_index(&project, "stale_alias.c", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("ambiguous C alias fixture did not produce a graph store");
    }
    int actual_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                         "staleAliasCaller", "Function", "actual", NULL);
    int alternate_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                            "staleAliasCaller", "Function", "alternate", NULL);
    rh_cleanup(&project, store);

    ASSERT_EQ(actual_reference, 0);
    ASSERT_EQ(alternate_reference, 0);
    PASS();
}

/* A write in only one branch cannot become the unconditional post-branch
 * callable identity.  The merge has two possible targets and must fail closed
 * to ordinary USAGE at accept(callback). */
TEST(repro_c_conditional_alias_reassignment_drops_stale_target) {
    static const char source[] = "void actual(void) {}\n"
                                 "void alternate(void) {}\n"
                                 "void accept(void (*callback)(void)) {}\n"
                                 "void conditionalAliasCaller(int flag) {\n"
                                 "  void (*callback)(void) = actual;\n"
                                 "  if (flag) callback = alternate;\n"
                                 "  accept(callback);\n"
                                 "}\n";

    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_C, "conditional_alias.c"));
    RProj project;
    cbm_store_t *store = rh_index(&project, "conditional_alias.c", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("conditional C alias fixture did not produce a graph store");
    }
    int actual_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                         "conditionalAliasCaller", "Function", "actual", NULL);
    int alternate_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "conditionalAliasCaller",
                      "Function", "alternate", NULL);
    rh_cleanup(&project, store);

    ASSERT_EQ(actual_reference, 0);
    ASSERT_EQ(alternate_reference, 0);
    PASS();
}

/* The nearest ordinary lexical binding wins even when an outer function
 * pointer with the same spelling has an exact target. */
TEST(repro_c_noncallable_shadow_blocks_outer_alias_target) {
    static const char source[] = "void actual(void) {}\n"
                                 "void accept(void (*callback)(void)) {}\n"
                                 "void shadowedAliasCaller(void) {\n"
                                 "  void (*callback)(void) = actual;\n"
                                 "  {\n"
                                 "    int callback = 0;\n"
                                 "    accept(callback);\n"
                                 "  }\n"
                                 "}\n";

    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_C, "shadowed_alias.c"));
    RProj project;
    cbm_store_t *store = rh_index(&project, "shadowed_alias.c", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("shadowed C alias fixture did not produce a graph store");
    }
    int leaked_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                         "shadowedAliasCaller", "Function", "actual", NULL);
    rh_cleanup(&project, store);

    ASSERT_EQ(leaked_reference, 0);
    PASS();
}

static int rp_assert_conditional_alias_reference_fails_closed(const char *filename,
                                                              const char *source,
                                                              CBMLanguage language,
                                                              const char *caller) {
    CBMFileResult *raw =
        cbm_extract_file(source, (int)strlen(source), language, "repro", filename, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument_call = NULL;
    const char *scan = source;
    while ((scan = strstr(scan, "accept(callback)")) != NULL) {
        argument_call = scan;
        scan++;
    }
    ASSERT_NOT_NULL(argument_call);
    uint32_t site_start = (uint32_t)(argument_call - source) + 7U;
    uint32_t site_end = site_start + (uint32_t)strlen("callback");
    int ordinary_usage = 0;
    int precise_reference = 0;
    int fabricated_invocation = 0;
    for (int i = 0; i < raw->usages.count; i++) {
        const CBMUsage *usage = &raw->usages.items[i];
        if (usage->ref_name && strcmp(usage->ref_name, "callback") == 0 &&
            usage->enclosing_func_qn && rp_qn_ends_with(usage->enclosing_func_qn, caller) &&
            usage->site_start_byte == site_start && usage->site_end_byte == site_end &&
            usage->kind == CBM_USAGE_VALUE) {
            ordinary_usage++;
        }
    }
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (!resolved->caller_qn || !rp_qn_ends_with(resolved->caller_qn, caller) ||
            resolved->site_start_byte != site_start || resolved->site_end_byte != site_end) {
            continue;
        }
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE) {
            precise_reference++;
        } else if (resolved->kind == CBM_RESOLVED_INVOCATION) {
            fabricated_invocation++;
        }
    }
    cbm_free_result(raw);

    ASSERT_GTE(ordinary_usage, 1);
    ASSERT_EQ(precise_reference, 0);
    ASSERT_EQ(fabricated_invocation, 0);
    return 0;
}

TEST(repro_cpp_conditional_alias_reference_fails_closed) {
    static const char source[] = "void actual() {}\n"
                                 "void alternate() {}\n"
                                 "void accept(void (*callback)()) {}\n"
                                 "void conditionalAliasCaller(bool flag) {\n"
                                 "  void (*callback)() = actual;\n"
                                 "  if (flag) callback = alternate;\n"
                                 "  accept(callback);\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.cpp", source, CBM_LANG_CPP, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_cuda_conditional_alias_reference_fails_closed) {
    static const char source[] = "void actual() {}\n"
                                 "void alternate() {}\n"
                                 "void accept(void (*callback)()) {}\n"
                                 "void conditionalAliasCaller(bool flag) {\n"
                                 "  void (*callback)() = actual;\n"
                                 "  if (flag) callback = alternate;\n"
                                 "  accept(callback);\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.cu", source, CBM_LANG_CUDA, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_go_conditional_alias_reference_fails_closed) {
    static const char source[] = "package refs\n"
                                 "func actual() {}\n"
                                 "func alternate() {}\n"
                                 "func accept(callback func()) {}\n"
                                 "func conditionalAliasCaller(flag bool) {\n"
                                 "  callback := actual\n"
                                 "  if flag { callback = alternate }\n"
                                 "  accept(callback)\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.go", source, CBM_LANG_GO, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_conditional_alias_reference_fails_closed) {
    static const char source[] = "def actual():\n    pass\n"
                                 "def alternate():\n    pass\n"
                                 "def accept(callback):\n    pass\n"
                                 "def conditionalAliasCaller(flag):\n"
                                 "    callback = actual\n"
                                 "    if flag:\n        callback = alternate\n"
                                 "    accept(callback)\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.py", source, CBM_LANG_PYTHON, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_conditional_alias_reference_fails_closed) {
    static const char source[] = "fn actual() {}\n"
                                 "fn alternate() {}\n"
                                 "fn accept(callback: fn()) {}\n"
                                 "fn conditionalAliasCaller(flag: bool) {\n"
                                 "    let mut callback: fn() = actual;\n"
                                 "    if flag { callback = alternate; }\n"
                                 "    accept(callback);\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.rs", source, CBM_LANG_RUST, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_unconditional_alias_reassignment_tracks_new_exact_target) {
    static const char source[] = "fn actual() {}\n"
                                 "fn alternate() {}\n"
                                 "fn accept(callback: fn()) {}\n"
                                 "fn exactReassignmentCaller() {\n"
                                 "    let mut callback: fn() = actual;\n"
                                 "    callback = alternate;\n"
                                 "    accept(callback);\n"
                                 "}\n";

    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST, "repro",
                                          "exact_reassignment.rs", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument_call = strstr(source, "accept(callback)");
    ASSERT_NOT_NULL(argument_call);
    uint32_t site_start = (uint32_t)(argument_call - source) + 7U;
    uint32_t site_end = site_start + (uint32_t)strlen("callback");
    int stale_reference = 0;
    int exact_reference = 0;
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (resolved->kind != CBM_RESOLVED_CALL_REFERENCE ||
            resolved->site_start_byte != site_start || resolved->site_end_byte != site_end ||
            !resolved->callee_qn) {
            continue;
        }
        if (rp_qn_ends_with(resolved->callee_qn, "actual")) {
            stale_reference++;
        }
        if (rp_qn_ends_with(resolved->callee_qn, "alternate")) {
            exact_reference++;
        }
    }
    cbm_free_result(raw);

    ASSERT_EQ(stale_reference, 0);
    ASSERT_EQ(exact_reference, 1);
    PASS();
}

TEST(repro_kotlin_conditional_alias_reference_fails_closed) {
    static const char source[] = "fun actual(): Unit {}\n"
                                 "fun alternate(): Unit {}\n"
                                 "fun accept(callback: () -> Unit): Unit {}\n"
                                 "fun conditionalAliasCaller(flag: Boolean): Unit {\n"
                                 "  var callback = ::actual\n"
                                 "  if (flag) callback = ::alternate\n"
                                 "  accept(callback)\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "ConditionalAlias.kt", source, CBM_LANG_KOTLIN, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_csharp_conditional_alias_reference_fails_closed) {
    static const char source[] = "using System;\n"
                                 "class ConditionalAlias {\n"
                                 "  static void actual() {}\n"
                                 "  static void alternate() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void conditionalAliasCaller(bool flag) {\n"
                                 "    Action callback = actual;\n"
                                 "    if (flag) callback = alternate;\n"
                                 "    accept(callback);\n"
                                 "  }\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "ConditionalAlias.cs", source, CBM_LANG_CSHARP, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

/* Branch-gating graph contracts for every frontend that has dedicated exact
 * callable-reference semantics. Complex/ambiguous expressions must retain an
 * ordinary USAGE edge instead of being promoted by a name-only match. */
SUITE(call_reference_language_complex_contract) {
    RUN_TEST(repro_javascript_complex_function_value_stays_usage);
    RUN_TEST(repro_typescript_complex_function_value_stays_usage);
    RUN_TEST(repro_tsx_complex_function_value_stays_usage);
    RUN_TEST(repro_go_complex_function_value_stays_usage);
    RUN_TEST(repro_python_complex_function_value_stays_usage);
    RUN_TEST(repro_c_complex_function_value_stays_usage);
    RUN_TEST(repro_cpp_complex_function_value_stays_usage);
    RUN_TEST(repro_cuda_complex_function_value_stays_usage);
    RUN_TEST(repro_rust_complex_function_value_stays_usage);
    RUN_TEST(repro_csharp_complex_function_value_stays_usage);
    RUN_TEST(repro_java_complex_method_reference_stays_usage);
    RUN_TEST(repro_kotlin_complex_callable_reference_stays_usage);
    RUN_TEST(repro_php_complex_first_class_callable_stays_usage);
    RUN_TEST(repro_perl_conditional_coderefs_stay_usage);
}

SUITE(repro_reference_precision) {
    RUN_TEST(repro_kotlin_property_reference_does_not_borrow_same_name_function);
    RUN_TEST(repro_kotlin_constructor_reference_requires_concrete_constructor);
    RUN_TEST(repro_kotlin_local_reference_never_binds_same_name_top_level);
    RUN_TEST(repro_php_dynamic_first_class_terminal_is_not_precise);
    RUN_TEST(repro_php_static_magic_first_class_reference_targets_callstatic);
    RUN_TEST(repro_php_typed_invokable_first_class_reference_targets_invoke);
    RUN_TEST(repro_java_same_name_receiver_calls_keep_occurrence_identity);
    RUN_TEST(repro_typescript_function_value_upgrades_only_with_exact_semantic_target);
    RUN_TEST(repro_kotlin_empty_cross_filter_does_not_fail_open);
    RUN_TEST(repro_go_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_python_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_rust_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_kotlin_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_csharp_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_go_exact_function_value_is_call_reference);
    RUN_TEST(repro_python_exact_function_value_is_call_reference);
    RUN_TEST(repro_javascript_exact_function_value_is_call_reference);
    RUN_TEST(repro_c_exact_function_value_is_call_reference);
    RUN_TEST(repro_cpp_exact_function_value_is_call_reference);
    RUN_TEST(repro_rust_exact_function_value_is_call_reference);
    RUN_TEST(repro_rust_direct_scoped_function_value_is_exact_reference);
    RUN_TEST(repro_csharp_exact_function_value_is_call_reference);
    RUN_TEST(repro_csharp_member_method_group_is_exact_reference);
    RUN_TEST(repro_csharp_parenthesized_method_group_is_exact_reference);
    RUN_TEST(repro_csharp_generic_method_group_is_exact_reference);
    RUN_TEST(repro_csharp_ambiguous_using_static_method_group_is_not_precise);
    RUN_TEST(repro_cuda_exact_function_value_is_call_reference);
    RUN_TEST(repro_exact_function_value_lsp_sites);
    RUN_TEST(repro_typescript_exact_bound_method_is_call_reference);
    RUN_TEST(repro_tsx_exact_bound_method_is_call_reference);
    RUN_TEST(repro_perl_exact_coderef_is_call_reference);
    RUN_TEST(repro_go_cross_file_exact_function_value_is_call_reference);
    RUN_TEST(repro_python_cross_file_exact_function_value_is_call_reference);
    RUN_TEST(repro_python_aliased_import_function_value_is_call_reference);
    RUN_TEST(repro_python_alias_equal_module_leaf_targets_imported_member);
    RUN_TEST(repro_rust_cross_file_exact_function_value_is_call_reference);
    RUN_TEST(repro_csharp_cross_file_exact_function_value_is_call_reference);
    RUN_TEST(repro_csharp_cross_file_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_kotlin_cross_file_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_csharp_callable_alias_argument_is_exact_reference);
    RUN_TEST(repro_kotlin_callable_alias_argument_is_exact_reference);
    RUN_TEST(repro_kotlin_receiver_bound_alias_invocation_targets_exact_method);
    RUN_TEST(repro_kotlin_unknown_imported_property_is_not_callable_reference);
    RUN_TEST(repro_javascript_complex_function_value_stays_usage);
    RUN_TEST(repro_typescript_complex_function_value_stays_usage);
    RUN_TEST(repro_tsx_complex_function_value_stays_usage);
    RUN_TEST(repro_go_complex_function_value_stays_usage);
    RUN_TEST(repro_python_complex_function_value_stays_usage);
    RUN_TEST(repro_c_complex_function_value_stays_usage);
    RUN_TEST(repro_cpp_complex_function_value_stays_usage);
    RUN_TEST(repro_cuda_complex_function_value_stays_usage);
    RUN_TEST(repro_rust_complex_function_value_stays_usage);
    RUN_TEST(repro_csharp_complex_function_value_stays_usage);
    RUN_TEST(repro_java_complex_method_reference_stays_usage);
    RUN_TEST(repro_kotlin_complex_callable_reference_stays_usage);
    RUN_TEST(repro_php_complex_first_class_callable_stays_usage);
    RUN_TEST(repro_perl_conditional_coderefs_stay_usage);
    RUN_TEST(repro_c_ambiguous_alias_reassignment_drops_stale_target);
    RUN_TEST(repro_c_conditional_alias_reassignment_drops_stale_target);
    RUN_TEST(repro_c_noncallable_shadow_blocks_outer_alias_target);
    RUN_TEST(repro_cpp_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_cuda_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_go_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_python_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_rust_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_rust_unconditional_alias_reassignment_tracks_new_exact_target);
    RUN_TEST(repro_kotlin_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_csharp_conditional_alias_reference_fails_closed);
}
