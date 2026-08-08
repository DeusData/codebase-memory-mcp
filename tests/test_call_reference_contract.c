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
}
