/* Production-graph contracts for callable values. These tests intentionally
 * exercise the full MCP indexing path rather than only raw extraction/LSP rows. */
#include "test_framework.h"
#include "repro_harness.h"

#include <store/store.h>

#include <string.h>

static int crc_edge_count(cbm_store_t *store, const char *project, const char *type,
                          const char *source_name, const char *target_label,
                          const char *target_name) {
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    if (cbm_store_find_edges_by_type(store, project, type, &edges, &edge_count) != CBM_STORE_OK) {
        return -1;
    }
    int matches = 0;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t source = {0};
        cbm_node_t target = {0};
        bool source_ok =
            cbm_store_find_node_by_id(store, edges[i].source_id, &source) == CBM_STORE_OK;
        bool target_ok =
            cbm_store_find_node_by_id(store, edges[i].target_id, &target) == CBM_STORE_OK;
        if (source_ok && target_ok && source.name && target.name &&
            strcmp(source.name, source_name) == 0 && strcmp(target.name, target_name) == 0 &&
            (!target_label || (target.label && strcmp(target.label, target_label) == 0))) {
            matches++;
        }
        cbm_node_free_fields(&source);
        cbm_node_free_fields(&target);
    }
    cbm_store_free_edges(edges, edge_count);
    return matches;
}

/* Edges of `type` out of `source_name` whose target's qualified name ends with
 * `target_qn_suffix`. Two methods in one project can share a short name, and
 * then only the qualified name tells them apart. */
static int crc_edge_count_to_qn(cbm_store_t *store, const char *project, const char *type,
                                const char *source_name, const char *target_qn_suffix) {
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    if (cbm_store_find_edges_by_type(store, project, type, &edges, &edge_count) != CBM_STORE_OK) {
        return -1;
    }
    int matches = 0;
    size_t suffix_len = strlen(target_qn_suffix);
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t source = {0};
        cbm_node_t target = {0};
        bool source_ok =
            cbm_store_find_node_by_id(store, edges[i].source_id, &source) == CBM_STORE_OK;
        bool target_ok =
            cbm_store_find_node_by_id(store, edges[i].target_id, &target) == CBM_STORE_OK;
        if (source_ok && target_ok && source.name && target.qualified_name &&
            strcmp(source.name, source_name) == 0) {
            size_t qn_len = strlen(target.qualified_name);
            if (qn_len >= suffix_len &&
                strcmp(target.qualified_name + qn_len - suffix_len, target_qn_suffix) == 0) {
                matches++;
            }
        }
        cbm_node_free_fields(&source);
        cbm_node_free_fields(&target);
    }
    cbm_store_free_edges(edges, edge_count);
    return matches;
}

static int crc_global_handler_edge_count(cbm_store_t *store, const char *project,
                                         const char *caller) {
    int references =
        crc_edge_count(store, project, "CALL_REFERENCE", caller, "Function", "handler");
    int calls = crc_edge_count(store, project, "CALLS", caller, "Function", "handler");
    int usages = crc_edge_count(store, project, "USAGE", caller, "Function", "handler");
    return references < 0 || calls < 0 || usages < 0 ? -1 : references + calls + usages;
}

static int crc_function_count(cbm_store_t *store, const char *project, const char *name) {
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    if (cbm_store_find_nodes_by_name(store, project, name, &nodes, &node_count) != CBM_STORE_OK) {
        return -1;
    }
    int functions = 0;
    for (int i = 0; i < node_count; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, "Function") == 0) {
            functions++;
        }
    }
    cbm_store_free_nodes(nodes, node_count);
    return functions;
}

TEST(call_reference_typescript_direct_argument_is_exact) {
    static const char source[] = "function handler(): void {}\n"
                                 "function register(callback: () => void): void {}\n"
                                 "function bootstrap(): void { register(handler); }\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "main.ts", source);
    ASSERT_NOT_NULL(store);
    int registrar =
        crc_edge_count(store, project.project, "CALLS", "bootstrap", "Function", "register");
    int reference = crc_edge_count(store, project.project, "CALL_REFERENCE", "bootstrap",
                                   "Function", "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "bootstrap", "Function", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "bootstrap", "Function", "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(registrar, 1);
    ASSERT_EQ(reference, 1);
    ASSERT_EQ(usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_kotlin_alias_argument_is_exact) {
    static const char source[] = "fun handler(): Unit {}\n"
                                 "fun accept(callback: () -> Unit): Unit {}\n"
                                 "val callback: () -> Unit = ::handler\n"
                                 "fun argument(): Unit {\n"
                                 "  accept(callback)\n"
                                 "}\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "AliasArgument.kt", source);
    ASSERT_NOT_NULL(store);
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", "Function", "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Function", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Function", "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(reference, 1);
    ASSERT_EQ(usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_python_use_before_local_shadow_is_not_global) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def caller():\n"
                                 "    accept(handler)\n"
                                 "    handler = lambda: None\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "shadow.py", source);
    ASSERT_NOT_NULL(store);
    int registrar = crc_edge_count(store, project.project, "CALLS", "caller", "Function", "accept");
    int handler_nodes = crc_function_count(store, project.project, "handler");
    int global_edges = crc_global_handler_edge_count(store, project.project, "caller");
    rh_cleanup(&project, store);
    ASSERT_EQ(registrar, 1);
    ASSERT_GTE(handler_nodes, 1);
    ASSERT_EQ(global_edges, 0);
    PASS();
}

TEST(call_reference_javascript_use_before_local_shadow_is_not_global) {
    static const char source[] = "function handler() {}\n"
                                 "function accept(callback) {}\n"
                                 "function caller() {\n"
                                 "  accept(handler);\n"
                                 "  const handler = () => {};\n"
                                 "}\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "shadow.js", source);
    ASSERT_NOT_NULL(store);
    int registrar = crc_edge_count(store, project.project, "CALLS", "caller", "Function", "accept");
    int handler_nodes = crc_function_count(store, project.project, "handler");
    int global_edges = crc_global_handler_edge_count(store, project.project, "caller");
    rh_cleanup(&project, store);
    ASSERT_EQ(registrar, 1);
    ASSERT_GTE(handler_nodes, 1);
    ASSERT_EQ(global_edges, 0);
    PASS();
}

TEST(call_reference_typescript_use_before_local_shadow_is_not_global) {
    static const char source[] = "function handler(): void {}\n"
                                 "function accept(callback: () => void): void {}\n"
                                 "function caller(): void {\n"
                                 "  accept(handler);\n"
                                 "  const handler = (): void => {};\n"
                                 "}\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "shadow.ts", source);
    ASSERT_NOT_NULL(store);
    int registrar = crc_edge_count(store, project.project, "CALLS", "caller", "Function", "accept");
    int handler_nodes = crc_function_count(store, project.project, "handler");
    int global_edges = crc_global_handler_edge_count(store, project.project, "caller");
    rh_cleanup(&project, store);
    ASSERT_EQ(registrar, 1);
    ASSERT_GTE(handler_nodes, 1);
    ASSERT_EQ(global_edges, 0);
    PASS();
}

TEST(call_reference_python_keyword_argument_is_exact) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback=None):\n"
                                 "    pass\n"
                                 "def argument():\n"
                                 "    accept(callback=handler)\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "keyword.py", source);
    ASSERT_NOT_NULL(store);
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", "Function", "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Function", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Function", "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(reference, 1);
    ASSERT_EQ(usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_python_bound_method_argument_is_exact) {
    static const char source[] = "class Service:\n"
                                 "    def handler(self):\n"
                                 "        pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument(service: Service):\n"
                                 "    accept(service.handler)\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "bound.py", source);
    ASSERT_NOT_NULL(store);
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", "Method", "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Method", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Method", "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(reference, 1);
    ASSERT_EQ(usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_python_parenthesized_bound_method_is_exact) {
    static const char source[] = "class Service:\n"
                                 "    def handler(self):\n"
                                 "        pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument(service: Service):\n"
                                 "    accept((service.handler))\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "parenthesized.py", source);
    ASSERT_NOT_NULL(store);
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", "Method", "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Method", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Method", "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(reference, 1);
    ASSERT_EQ(usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_python_instance_field_shadow_is_not_method) {
    static const char source[] = "class Service:\n"
                                 "    def __init__(self):\n"
                                 "        self.handler = 42\n"
                                 "    def handler(self):\n"
                                 "        pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument(service: Service):\n"
                                 "    accept(service.handler)\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "field_shadow.py", source);
    ASSERT_NOT_NULL(store);
    int method_reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", "Method", "handler");
    int any_reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", NULL, "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", NULL, "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", NULL, "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(method_reference, 0);
    ASSERT_EQ(any_reference, 0);
    ASSERT_EQ(usage, 1);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_python_property_value_stays_usage) {
    static const char source[] = "class Service:\n"
                                 "    @property\n"
                                 "    def handler(self) -> int:\n"
                                 "        return 42\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument(service: Service):\n"
                                 "    accept(service.handler)\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "property.py", source);
    ASSERT_NOT_NULL(store);
    int accept_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Function", "accept");
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", NULL, "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Method", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", NULL, "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(accept_call, 1);
    ASSERT_EQ(reference, 0);
    ASSERT_EQ(usage, 1);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_python_unknown_decorated_value_stays_usage) {
    static const char source[] = "def replace_with_value(function):\n"
                                 "    return 42\n"
                                 "class Service:\n"
                                 "    @replace_with_value\n"
                                 "    def handler(self):\n"
                                 "        pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument(service: Service):\n"
                                 "    accept(service.handler)\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "unknown_decorator.py", source);
    ASSERT_NOT_NULL(store);
    int accept_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Function", "accept");
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", NULL, "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Method", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", NULL, "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(accept_call, 1);
    ASSERT_EQ(reference, 0);
    ASSERT_EQ(usage, 1);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_python_later_decorated_function_rebinding_stays_usage) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def replace_with_value(function):\n"
                                 "    return 42\n"
                                 "@replace_with_value\n"
                                 "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument():\n"
                                 "    accept(handler)\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "function_rebinding.py", source);
    ASSERT_NOT_NULL(store);
    int accept_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Function", "accept");
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", NULL, "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Function", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", NULL, "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(accept_call, 1);
    ASSERT_EQ(reference, 0);
    ASSERT_EQ(usage, 1);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_python_later_decorated_method_rebinding_stays_usage) {
    static const char source[] = "def replace_with_value(function):\n"
                                 "    return 42\n"
                                 "class Service:\n"
                                 "    def handler(self):\n"
                                 "        pass\n"
                                 "    @replace_with_value\n"
                                 "    def handler(self):\n"
                                 "        pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument(service: Service):\n"
                                 "    accept(service.handler)\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "method_rebinding.py", source);
    ASSERT_NOT_NULL(store);
    int accept_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Function", "accept");
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", NULL, "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Method", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", NULL, "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(accept_call, 1);
    ASSERT_EQ(reference, 0);
    ASSERT_EQ(usage, 1);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_go_bound_method_argument_is_exact) {
    static const char source[] = "package refs\n"
                                 "type Service struct{}\n"
                                 "func (Service) handler() {}\n"
                                 "func accept(callback func()) {}\n"
                                 "func argument(service Service) { accept(service.handler) }\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "bound.go", source);
    ASSERT_NOT_NULL(store);
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", "Method", "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Method", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Method", "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(reference, 1);
    ASSERT_EQ(usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_reference_go_ambiguous_promoted_method_stays_usage) {
    static const char source[] = "package refs\n"
                                 "type Alpha struct{}\n"
                                 "func (Alpha) handler() {}\n"
                                 "type Beta struct{}\n"
                                 "func (Beta) handler() {}\n"
                                 "type Service struct { Alpha; Beta }\n"
                                 "func accept(callback func()) {}\n"
                                 "func argument(service Service) { accept(service.handler) }\n";
    RProj project;
    cbm_store_t *store = rh_index(&project, "ambiguous.go", source);
    ASSERT_NOT_NULL(store);
    int reference =
        crc_edge_count(store, project.project, "CALL_REFERENCE", "argument", "Method", "handler");
    int usage = crc_edge_count(store, project.project, "USAGE", "argument", "Method", "handler");
    int fabricated_call =
        crc_edge_count(store, project.project, "CALLS", "argument", "Method", "handler");
    rh_cleanup(&project, store);
    ASSERT_EQ(reference, 0);
    ASSERT_EQ(usage, 1);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(call_java_record_accessor_binds_the_record) {
    /* A record's accessors are implicit, so `record Point(double x, ...)`
     * declares no `x()` in its source. Extraction emits them, and `other.x()`
     * binds to the record's own accessor.
     *
     * Two things must not happen instead. The call must not reach the
     * project-wide short-name registry and land on the first `x` in the tree —
     * here an unrelated class's private field. And the guard that refuses such
     * data binds must not swallow a real method that merely shares the
     * spelling: the builder's `x(int)`, declared beside its own `x` field,
     * keeps its edge. */
    static const RFile files[] = {
        {"Point.java", "package com.example;\n"
                       "public record Point(double x, double y, double z) {\n"
                       "    public double first(Point other) {\n"
                       "        return Maths.abs(other.x()) + new Builder().x(5).made();\n"
                       "    }\n"
                       "}\n"},
        {"Spring.java", "package com.example;\n"
                        "public final class Spring {\n"
                        "    private double x;\n"
                        "    public void put(double v) { this.x = v; }\n"
                        "}\n"},
        {"Builder.java", "package com.example;\n"
                         "public final class Builder {\n"
                         "    private int x;\n"
                         "    public Builder x(int v) { this.x = v; return this; }\n"
                         "    public int made() { return x; }\n"
                         "}\n"},
        {"Maths.java", "package com.example;\n"
                       "public final class Maths {\n"
                       "    public static double abs(double v) { return v < 0 ? -v : v; }\n"
                       "}\n"}};
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 4);
    ASSERT_NOT_NULL(store);
    int fabricated_field = crc_edge_count(store, project.project, "CALLS", "first", "Field", "x");
    int fabricated_var = crc_edge_count(store, project.project, "CALLS", "first", "Variable", "x");
    int genuine = crc_edge_count(store, project.project, "CALLS", "first", "Method", "abs");
    int accessor = crc_edge_count_to_qn(store, project.project, "CALLS", "first", ".Point.x");
    int builder = crc_edge_count_to_qn(store, project.project, "CALLS", "first", ".Builder.x");
    rh_cleanup(&project, store);
    ASSERT_EQ(fabricated_field, 0);
    ASSERT_EQ(fabricated_var, 0);
    ASSERT_EQ(genuine, 1);
    ASSERT_EQ(accessor, 1);
    ASSERT_EQ(builder, 1);
    PASS();
}

/* Static imports can name both an inherited method and a field. Thread's
 * activeCount() is external to this project, whereas Owner.activeCount is
 * local data; an unresolved import must not turn the field into a callee. */
static int crc_java_static_import_data_call_count(bool parallel) {
    RFile files[52] = {{"example/Owner.java", "package example;\n"
                                              "public class Owner extends Thread {\n"
                                              "    public static int activeCount;\n"
                                              "}\n"},
                       {"example/Caller.java", "package example;\n"
                                               "import static example.Owner.activeCount;\n"
                                               "class Caller {\n"
                                               "    int caller() { return activeCount(); }\n"
                                               "}\n"}};
    char names[50][64];
    char sources[50][64];
    int count = parallel ? 52 : 2;
    for (int i = 2; i < count; i++) {
        snprintf(names[i - 2], sizeof(names[i - 2]), "example/Pad%d.java", i);
        snprintf(sources[i - 2], sizeof(sources[i - 2]), "package example; class Pad%d {}\n", i);
        files[i].name = names[i - 2];
        files[i].content = sources[i - 2];
    }
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, count);
    if (!store) {
        return -1;
    }
    int fields = crc_edge_count(store, project.project, "CALLS", "caller", "Field", "activeCount");
    int vars = crc_edge_count(store, project.project, "CALLS", "caller", "Variable", "activeCount");
    rh_cleanup(&project, store);
    return fields < 0 || vars < 0 ? -1 : fields + vars;
}

TEST(call_java_static_import_does_not_call_field_sequential) {
    ASSERT_EQ(crc_java_static_import_data_call_count(false), 0);
    PASS();
}

TEST(call_java_static_import_does_not_call_field_parallel) {
    ASSERT_EQ(crc_java_static_import_data_call_count(true), 0);
    PASS();
}

TEST(call_java_record_varargs_accessor_has_array_return) {
    static const char *sources[] = {
        "record Values(String... xs) {}\n", "record Values(String[]... xs) {}\n",
        "record Values(int... xs) {}\n", "record Values(@Deprecated String... xs) {}\n",
        "record Values(String... xs) { public String[] xs() { return xs; } }\n"};
    static const char *return_types[] = {"String[]", "String[][]", "int[]", "String[]", "String[]"};
    for (int fixture = 0; fixture < 5; fixture++) {
        CBMFileResult *result =
            cbm_extract_file(sources[fixture], (int)strlen(sources[fixture]), CBM_LANG_JAVA,
                             "repro", "Values.java", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        int accessors = 0;
        bool array_return = false;
        for (int i = 0; i < result->defs.count; i++) {
            CBMDefinition *def = &result->defs.items[i];
            if (strcmp(def->name, "xs") == 0 && strcmp(def->label, "Method") == 0) {
                accessors++;
                array_return =
                    def->return_type && strcmp(def->return_type, return_types[fixture]) == 0;
            }
        }
        cbm_free_result(result);
        ASSERT_EQ(accessors, 1);
        ASSERT_TRUE(array_return);
    }
    PASS();
}

/* Comment extras in an otherwise empty parameter list must not create a
 * second, implicit accessor at the record component's source location. */
TEST(call_java_record_commented_accessor_has_only_explicit_span) {
    static const char *sources[] = {"record Value(int x) {\n"
                                    "    public int x(/* explanation */) {\n"
                                    "        return x;\n"
                                    "    }\n"
                                    "}\n",
                                    "record Value(int x) {\n"
                                    "    public int x(// explanation\n"
                                    "    ) {\n"
                                    "        return x;\n"
                                    "    }\n"
                                    "}\n"};
    static const int end_lines[] = {4, 5};
    for (int fixture = 0; fixture < 2; fixture++) {
        CBMFileResult *result =
            cbm_extract_file(sources[fixture], (int)strlen(sources[fixture]), CBM_LANG_JAVA,
                             "repro", "Value.java", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        int accessors = 0;
        int explicit_spans = 0;
        for (int i = 0; i < result->defs.count; i++) {
            CBMDefinition *def = &result->defs.items[i];
            if (strcmp(def->name, "x") == 0 && strcmp(def->label, "Method") == 0) {
                accessors++;
                if (def->start_line == 2 && def->end_line == end_lines[fixture]) {
                    explicit_spans++;
                }
            }
        }
        cbm_free_result(result);
        ASSERT_EQ(accessors, 1);
        ASSERT_EQ(explicit_spans, 1);
    }
    PASS();
}

TEST(call_java_record_commented_overload_keeps_implicit_accessor) {
    static const char source[] =
        "record Value(int x) {\n"
        "    public int x(/* explanation */ int scale) { return x * scale; }\n"
        "}\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_JAVA, "repro",
                                             "Value.java", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    int methods = 0;
    int implicit_spans = 0;
    int overload_spans = 0;
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *def = &result->defs.items[i];
        if (strcmp(def->name, "x") == 0 && strcmp(def->label, "Method") == 0) {
            methods++;
            if (def->start_line == 1 && def->end_line == 1) {
                implicit_spans++;
            }
            if (def->start_line == 2 && def->end_line == 2) {
                overload_spans++;
            }
        }
    }
    cbm_free_result(result);
    ASSERT_EQ(methods, 2);
    ASSERT_EQ(implicit_spans, 1);
    ASSERT_EQ(overload_spans, 1);
    PASS();
}

SUITE(call_reference_contract) {
    RUN_TEST(call_reference_typescript_direct_argument_is_exact);
    RUN_TEST(call_reference_kotlin_alias_argument_is_exact);
    RUN_TEST(call_reference_python_use_before_local_shadow_is_not_global);
    RUN_TEST(call_reference_javascript_use_before_local_shadow_is_not_global);
    RUN_TEST(call_reference_typescript_use_before_local_shadow_is_not_global);
    RUN_TEST(call_reference_python_keyword_argument_is_exact);
    RUN_TEST(call_reference_python_bound_method_argument_is_exact);
    RUN_TEST(call_reference_python_parenthesized_bound_method_is_exact);
    RUN_TEST(call_reference_python_instance_field_shadow_is_not_method);
    RUN_TEST(call_reference_python_property_value_stays_usage);
    RUN_TEST(call_reference_python_unknown_decorated_value_stays_usage);
    RUN_TEST(call_reference_python_later_decorated_function_rebinding_stays_usage);
    RUN_TEST(call_reference_python_later_decorated_method_rebinding_stays_usage);
    RUN_TEST(call_reference_go_bound_method_argument_is_exact);
    RUN_TEST(call_reference_go_ambiguous_promoted_method_stays_usage);
    RUN_TEST(call_java_record_accessor_binds_the_record);
    RUN_TEST(call_java_record_varargs_accessor_has_array_return);
    RUN_TEST(call_java_record_commented_accessor_has_only_explicit_span);
    RUN_TEST(call_java_record_commented_overload_keeps_implicit_accessor);
    RUN_TEST(call_java_static_import_does_not_call_field_sequential);
    RUN_TEST(call_java_static_import_does_not_call_field_parallel);
}
