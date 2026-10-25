#include "binding.h"
#include "../lang_specs.h"
#include <stdlib.h>
#include <string.h>

enum { BINDING_MAX_DEPTH = 512, BINDING_MAX_NODES = 1000000, BINDING_INITIAL_CAP = 32 };
enum {
    BINDING_DECLARATION = 1,
    BINDING_IMPORT = 2,
    BINDING_BLOCKED = 4,
    BINDING_EXPORTED = 8,
    BINDING_EXPORT_MARKER = 16,
    BINDING_TYPE_DECLARATION = 32,
    BINDING_VALUE_TYPE = 64,
    BINDING_EXPORT_CONFLICT = 128,
    BINDING_TYPE_EXPORT_MARKER = 256,
    BINDING_TYPE_EXPORTED = 512,
    BINDING_NAMESPACE_MEMBER = 1024
};

typedef struct {
    const char *name;
    const char *target;
    uint32_t at;
    unsigned flags;
    uint32_t scope;
} binding_name_t;

typedef struct {
    const char *name;
    uint32_t start;
    uint32_t end;
    uint32_t scope;
} binding_site_t;

typedef struct {
    const char *qualified_name;
    uint32_t parent;
    bool class_scope;
} binding_scope_t;

struct cbm_resolve_binding_index {
    CBMArena *arena;
    const CBMFileResult *result;
    CBMLanguage language;
    const char *source;
    size_t source_len;
    binding_name_t *names;
    size_t name_count;
    size_t name_capacity;
    binding_site_t *sites;
    size_t site_count;
    size_t site_capacity;
    const char *reason;
    cbm_resolve_analysis_t analysis;
    bool javascript_module;
    binding_scope_t *scopes;
    size_t scope_count, scope_capacity;
    uint32_t current_scope;
    uint32_t python_wildcard_barrier;
    cbm_resolve_source_import_t *imports;
    size_t import_count, import_capacity;
};

static bool kind_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && strcmp(ts_node_type(node), kind) == 0;
}

static TSNode field(TSNode node, const char *name) {
    return ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}

static const char *node_text(cbm_resolve_binding_index_t *index, TSNode node) {
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    if (end <= start || end > index->source_len) {
        return NULL;
    }
    return cbm_arena_strndup(index->arena, index->source + start, end - start);
}

static void fail_index(cbm_resolve_binding_index_t *index, cbm_resolve_analysis_t analysis,
                       const char *reason) {
    index->analysis = analysis;
    index->reason = reason;
}

static void *grow_array(cbm_resolve_binding_index_t *index, void *old, size_t count,
                        size_t *capacity, size_t width) {
    if (count < *capacity) {
        return old;
    }
    size_t next = *capacity ? *capacity * 2 : BINDING_INITIAL_CAP;
    if (next < *capacity || next > SIZE_MAX / width) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_capacity");
        return NULL;
    }
    void *result = cbm_arena_alloc(index->arena, next * width);
    if (!result) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_allocation");
        return NULL;
    }
    if (count) {
        memcpy(result, old, count * width);
    }
    *capacity = next;
    return result;
}

static void add_name(cbm_resolve_binding_index_t *index, const char *name, uint32_t at,
                     unsigned flags) {
    if (!name || !*name) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_name_missing");
        return;
    }
    binding_name_t *names =
        grow_array(index, index->names, index->name_count, &index->name_capacity, sizeof(*names));
    if (!names) {
        return;
    }
    index->names = names;
    const char *target = NULL;
    if (flags & (BINDING_DECLARATION | BINDING_TYPE_DECLARATION | BINDING_NAMESPACE_MEMBER)) {
        const char *owner = index->current_scope < index->scope_count
                                ? index->scopes[index->current_scope].qualified_name
                                : index->result->module_qn;
        target = cbm_arena_sprintf(index->arena, "%s.%s", owner, name);
        if (!target) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_target_allocation");
            return;
        }
    }
    names[index->name_count++] = (binding_name_t){name, target, at, flags, index->current_scope};
}

static void add_site(cbm_resolve_binding_index_t *index, const char *name, TSNode node) {
    if (!name || !*name) {
        return;
    }
    binding_site_t *sites =
        grow_array(index, index->sites, index->site_count, &index->site_capacity, sizeof(*sites));
    if (!sites) {
        return;
    }
    index->sites = sites;
    sites[index->site_count++] = (binding_site_t){name, ts_node_start_byte(node),
                                                  ts_node_end_byte(node), index->current_scope};
}

static bool listed(const char *kind, const char *const *list) {
    for (; *list; list++) {
        if (strcmp(kind, *list) == 0) {
            return true;
        }
    }
    return false;
}

/* Explicit grammar capability lists: a new binding form cannot silently
 * become a complete lookup merely because generic extraction supports it. */
static bool supported_node(CBMLanguage language, const char *kind) {
    static const char *const python[] = {"module",
                                         "lambda",
                                         "lambda_parameters",
                                         "decorated_definition",
                                         "decorator",
                                         "class_definition",
                                         "with_statement",
                                         "with_clause",
                                         "with_item",
                                         "as_pattern",
                                         "as_pattern_target",
                                         "function_definition",
                                         "parameters",
                                         "identifier",
                                         "block",
                                         "expression_statement",
                                         "return_statement",
                                         "call",
                                         "argument_list",
                                         "integer",
                                         "float",
                                         "string",
                                         "string_start",
                                         "string_content",
                                         "string_end",
                                         "escape_sequence",
                                         "comment",
                                         "attribute",
                                         "comparison_operator",
                                         "binary_operator",
                                         "unary_operator",
                                         "boolean_operator",
                                         "not_operator",
                                         "conditional_expression",
                                         "true",
                                         "false",
                                         "none",
                                         "parenthesized_expression",
                                         "list",
                                         "dictionary",
                                         "pair",
                                         "set",
                                         "tuple",
                                         "subscript",
                                         "slice",
                                         "if_statement",
                                         "elif_clause",
                                         "else_clause",
                                         "pass_statement",
                                         "break_statement",
                                         "continue_statement",
                                         "while_statement",
                                         "assert_statement",
                                         "raise_statement",
                                         "yield",
                                         "await",
                                         "concatenated_string",
                                         "assignment",
                                         "augmented_assignment",
                                         "named_expression",
                                         "for_statement",
                                         "for_in_clause",
                                         "list_comprehension",
                                         "set_comprehension",
                                         "dictionary_comprehension",
                                         "generator_expression",
                                         "if_clause",
                                         "default_parameter",
                                         "typed_parameter",
                                         "typed_default_parameter",
                                         "list_splat_pattern",
                                         "dictionary_splat_pattern",
                                         "list_pattern",
                                         "tuple_pattern",
                                         "pattern_list",
                                         "keyword_argument",
                                         "type",
                                         "import_statement",
                                         "import_from_statement",
                                         "wildcard_import",
                                         "dotted_name",
                                         "aliased_import",
                                         "future_import_statement",
                                         "delete_statement",
                                         "global_statement",
                                         "nonlocal_statement",
                                         NULL};
    static const char *const lua[] = {"chunk",
                                      "block",
                                      "function_declaration",
                                      "function_definition",
                                      "parameters",
                                      "identifier",
                                      "variable",
                                      "variable_list",
                                      "expression_list",
                                      "function_call",
                                      "arguments",
                                      "return_statement",
                                      "assignment_statement",
                                      "variable_declaration",
                                      "number",
                                      "string",
                                      "string_content",
                                      "escape_sequence",
                                      "true",
                                      "false",
                                      "nil",
                                      "binary_expression",
                                      "unary_expression",
                                      "parenthesized_expression",
                                      "if_statement",
                                      "elseif_statement",
                                      "else_statement",
                                      "while_statement",
                                      "repeat_statement",
                                      "for_statement",
                                      "for_numeric_clause",
                                      "for_generic_clause",
                                      "do_statement",
                                      "break_statement",
                                      "comment",
                                      "empty_statement",
                                      "table_constructor",
                                      "field",
                                      "dot_index_expression",
                                      "method_index_expression",
                                      "bracket_index_expression",
                                      NULL};
    return listed(kind, language == CBM_LANG_PYTHON ? python : lua);
}

static bool blocks_identifiers(const char *kind) {
    return strstr(kind, "parameter") || strstr(kind, "assignment") || strstr(kind, "pattern") ||
           strcmp(kind, "named_expression") == 0 || strcmp(kind, "for_statement") == 0 ||
           strcmp(kind, "for_in_clause") == 0 || strcmp(kind, "for_generic_clause") == 0 ||
           strcmp(kind, "for_numeric_clause") == 0 || strcmp(kind, "variable_declaration") == 0 ||
           strcmp(kind, "delete_statement") == 0 || strcmp(kind, "global_statement") == 0 ||
           strcmp(kind, "nonlocal_statement") == 0;
}

/* A Python binder is the target of a declaration/write, never every name
 * read underneath the enclosing statement. Preserve the role through tuple
 * and splat targets, and stop it at attribute/subscript expressions. */
static bool python_target_role(const char *kind, const char *parent, const char *field_name,
                               bool inherited) {
    bool target = inherited;
    if (parent) {
        if (!strcmp(parent, "assignment") || !strcmp(parent, "augmented_assignment") ||
            !strcmp(parent, "for_statement") || !strcmp(parent, "for_in_clause"))
            target = field_name && !strcmp(field_name, "left");
        else if (!strcmp(parent, "named_expression"))
            target = field_name && !strcmp(field_name, "name");
        else if (!strcmp(parent, "parameters") || !strcmp(parent, "lambda_parameters"))
            target = true;
        else if ((!strcmp(parent, "default_parameter") ||
                  !strcmp(parent, "typed_default_parameter")) &&
                 field_name)
            target = !strcmp(field_name, "name");
        else if (!strcmp(parent, "typed_parameter") && field_name && !strcmp(field_name, "type"))
            target = false;
    }
    if (!strcmp(kind, "as_pattern_target") || !strcmp(kind, "delete_statement"))
        target = true;
    if (!strcmp(kind, "attribute") || !strcmp(kind, "subscript"))
        target = false;
    return target;
}

static bool dynamic_name(const char *name) {
    static const char *const names[] = {"exec",     "eval",    "globals",    "locals",   "vars",
                                        "setattr",  "delattr", "__import__", "__dict__", "load",
                                        "loadfile", "dofile",  "loadstring", "setfenv",  "getfenv",
                                        "debug",    "_ENV",    "_G",         "rawset",   NULL};
    return listed(name, names);
}

static bool lua_local_function(TSNode node) {
    if (!ts_node_is_null(field(node, "local_declaration"))) {
        return true;
    }
    uint32_t children = ts_node_child_count(node);
    for (uint32_t i = 0; i < children; i++) {
        if (kind_is(ts_node_child(node, i), "local")) {
            return true;
        }
    }
    return false;
}

static void note_import(cbm_resolve_binding_index_t *index, TSNode node) {
    bool from = kind_is(node, "import_from_statement");
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (from && kind_is(child, "wildcard_import")) {
            index->python_wildcard_barrier = ts_node_end_byte(node);
            continue;
        }
        const char *field_name = ts_node_field_name_for_child(node, i);
        if (!field_name || strcmp(field_name, "name") != 0) {
            continue;
        }
        TSNode alias = field(child, "alias");
        const char *name = node_text(index, ts_node_is_null(alias) ? child : alias);
        if (ts_node_is_null(alias) && kind_is(child, "aliased_import")) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "import_alias_unavailable");
            return;
        }
        if (name && !from && ts_node_is_null(alias)) {
            const char *dot = strchr(name, '.');
            if (dot) {
                name = cbm_arena_strndup(index->arena, name, (size_t)(dot - name));
            }
        }
        add_name(index, name, ts_node_start_byte(node), BINDING_IMPORT);
    }
}

static void note_call(cbm_resolve_binding_index_t *index, TSNode node) {
    TSNode callee = field(node, index->language == CBM_LANG_LUA ? "name" : "function");
    if (kind_is(node, "new_expression")) {
        callee = field(node, "constructor");
    }
    if (ts_node_is_null(callee) && ts_node_named_child_count(node)) {
        callee = ts_node_named_child(node, 0);
    }
    if (!kind_is(callee, "identifier") && !kind_is(callee, "attribute") &&
        !kind_is(callee, "dot_index_expression") && !kind_is(callee, "member_expression")) {
        return;
    }
    const char *name = node_text(index, callee);
    add_site(index, name, node);
    add_site(index, name, callee);
}

typedef struct {
    bool blocked;
    bool import;
    bool named_export;
    bool type_only;
    bool conditional;
    uint32_t scope;
} binding_walk_t;

static uint32_t python_child_scope(cbm_resolve_binding_index_t *index, TSNode node, uint32_t parent,
                                   bool class_scope) {
    const char *name = kind_is(node, "lambda") ? cbm_arena_sprintf(index->arena, "<lambda@%u>",
                                                                   ts_node_start_byte(node))
                                               : node_text(index, field(node, "name"));
    const char *owner = parent < index->scope_count ? index->scopes[parent].qualified_name
                                                    : index->result->module_qn;
    const char *qn = name ? cbm_arena_sprintf(index->arena, "%s.%s", owner, name) : NULL;
    binding_scope_t *scopes = grow_array(index, index->scopes, index->scope_count,
                                         &index->scope_capacity, sizeof(*scopes));
    if (!scopes || !qn) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_scope_allocation");
        return parent;
    }
    index->scopes = scopes;
    /* A method closes over enclosing functions, never its class dictionary.
     * The qualified owner above still retains that class in graph identity. */
    if (!class_scope)
        while (parent && scopes[parent].class_scope)
            parent = scopes[parent].parent;
    uint32_t id = (uint32_t)index->scope_count++;
    scopes[id] = (binding_scope_t){qn, parent, class_scope};
    return id;
}

static bool javascript_language(CBMLanguage language) {
    return language == CBM_LANG_JAVASCRIPT || language == CBM_LANG_TYPESCRIPT ||
           language == CBM_LANG_TSX || language == CBM_LANG_ARKTS;
}

static bool supported_javascript_node(const char *kind) {
    static const char *const syntax[] = {"program",
                                         "function_declaration",
                                         "generator_function_declaration",
                                         "function_expression",
                                         "generator_function",
                                         "arrow_function",
                                         "formal_parameters",
                                         "identifier",
                                         "statement_block",
                                         "expression_statement",
                                         "return_statement",
                                         "call_expression",
                                         "arguments",
                                         "number",
                                         "string",
                                         "string_fragment",
                                         "escape_sequence",
                                         "comment",
                                         "member_expression",
                                         "property_identifier",
                                         "binary_expression",
                                         "unary_expression",
                                         "ternary_expression",
                                         "true",
                                         "false",
                                         "null",
                                         "undefined",
                                         "parenthesized_expression",
                                         "array",
                                         "object",
                                         "pair",
                                         "shorthand_property_identifier",
                                         "subscript_expression",
                                         "if_statement",
                                         "else_clause",
                                         "empty_statement",
                                         "break_statement",
                                         "continue_statement",
                                         "while_statement",
                                         "do_statement",
                                         "for_statement",
                                         "for_in_statement",
                                         "throw_statement",
                                         "try_statement",
                                         "catch_clause",
                                         "finally_clause",
                                         "switch_statement",
                                         "switch_body",
                                         "switch_case",
                                         "switch_default",
                                         "await_expression",
                                         "yield_expression",
                                         "sequence_expression",
                                         "lexical_declaration",
                                         "variable_declaration",
                                         "variable_declarator",
                                         "assignment_expression",
                                         "augmented_assignment_expression",
                                         "update_expression",
                                         "assignment_pattern",
                                         "array_pattern",
                                         "object_pattern",
                                         "pair_pattern",
                                         "rest_pattern",
                                         "shorthand_property_identifier_pattern",
                                         "object_assignment_pattern",
                                         "spread_element",
                                         "required_parameter",
                                         "optional_parameter",
                                         "type_annotation",
                                         "predefined_type",
                                         "type_identifier",
                                         "generic_type",
                                         "type_arguments",
                                         "nested_type_identifier",
                                         "array_type",
                                         "union_type",
                                         "intersection_type",
                                         "parenthesized_type",
                                         "literal_type",
                                         "tuple_type",
                                         "optional_type",
                                         "rest_type",
                                         "function_type",
                                         "type_query",
                                         "keyof_type",
                                         "lookup_type",
                                         "conditional_type",
                                         "infer_type",
                                         "type_predicate",
                                         "type_predicate_annotation",
                                         "asserts_annotation",
                                         "asserts",
                                         "type_parameters",
                                         "type_parameter",
                                         "constraint",
                                         "default_type",
                                         "object_type",
                                         "property_signature",
                                         "method_signature",
                                         "call_signature",
                                         "construct_signature",
                                         "index_signature",
                                         "mapped_type_clause",
                                         "index_type_query",
                                         "readonly_type",
                                         "template_literal_type",
                                         "template_string",
                                         "template_substitution",
                                         "regex",
                                         "regex_pattern",
                                         "regex_flags",
                                         "non_null_expression",
                                         "as_expression",
                                         "satisfies_expression",
                                         "type_assertion",
                                         "new_expression",
                                         "this",
                                         "super",
                                         "class_declaration",
                                         "abstract_class_declaration",
                                         "class",
                                         "class_body",
                                         "class_heritage",
                                         "extends_clause",
                                         "implements_clause",
                                         "method_definition",
                                         "public_field_definition",
                                         "field_definition",
                                         "private_property_identifier",
                                         "accessibility_modifier",
                                         "static_block",
                                         "interface_declaration",
                                         "extends_type_clause",
                                         "interface_body",
                                         "type_alias_declaration",
                                         "enum_declaration",
                                         "enum_body",
                                         "enum_assignment",
                                         "computed_property_name",
                                         "import_statement",
                                         "import_clause",
                                         "named_imports",
                                         "import_specifier",
                                         "namespace_import",
                                         "export_statement",
                                         "export_clause",
                                         "export_specifier",
                                         "namespace_export",
                                         "meta_property",
                                         "jsx_element",
                                         "jsx_opening_element",
                                         "jsx_closing_element",
                                         "jsx_self_closing_element",
                                         "jsx_attribute",
                                         "jsx_expression",
                                         "jsx_text",
                                         "jsx_namespace_name",
                                         "jsx_fragment",
                                         NULL};
    return listed(kind, syntax);
}

static bool child_token(TSNode node, const char *token) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        if (kind_is(ts_node_child(node, i), token)) {
            return true;
        }
    }
    return false;
}

static bool javascript_function_kind(const char *kind) {
    return strcmp(kind, "function_declaration") == 0 ||
           strcmp(kind, "generator_function_declaration") == 0;
}

static unsigned javascript_declaration_kind(const char *kind) {
    if (javascript_function_kind(kind)) {
        return BINDING_DECLARATION;
    }
    if (strcmp(kind, "class_declaration") == 0 || strcmp(kind, "abstract_class_declaration") == 0 ||
        strcmp(kind, "enum_declaration") == 0) {
        return BINDING_TYPE_DECLARATION | BINDING_VALUE_TYPE;
    }
    if (strcmp(kind, "interface_declaration") == 0 || strcmp(kind, "type_alias_declaration") == 0) {
        return BINDING_TYPE_DECLARATION;
    }
    return 0;
}

static void note_javascript_import(cbm_resolve_binding_index_t *index, TSNode node,
                                   const char *parent_kind, bool type_only) {
    TSNode name = {0};
    if (kind_is(node, "import_specifier")) {
        name = field(node, "alias");
        if (ts_node_is_null(name)) {
            name = field(node, "name");
        }
        type_only |= child_token(node, "type");
    } else if (kind_is(node, "namespace_import")) {
        name = ts_node_named_child(node, 0);
    } else if (kind_is(node, "identifier") && parent_kind &&
               strcmp(parent_kind, "import_clause") == 0) {
        name = node;
    }
    if (!ts_node_is_null(name)) {
        if (!kind_is(name, "identifier")) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "unsupported_import_binding");
            return;
        }
        add_name(index, node_text(index, name), 0, type_only ? BINDING_BLOCKED : BINDING_IMPORT);
    }
}

static void note_javascript_export(cbm_resolve_binding_index_t *index, TSNode node) {
    /* Only a local named export clause can certify this module's declaration.
     * Export aliases and re-exports need an exported-name -> target map; the
     * current QN summary cannot represent that evidence and withholds it. */
    if (!kind_is(node, "export_statement") || child_token(node, "default")) {
        return;
    }
    bool local_export = ts_node_is_null(field(node, "source"));
    bool type_export = child_token(node, "type");
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode clause = ts_node_named_child(node, i);
        if (!kind_is(clause, "export_clause")) {
            continue;
        }
        uint32_t specifiers = ts_node_named_child_count(clause);
        for (uint32_t s = 0; s < specifiers; s++) {
            TSNode specifier = ts_node_named_child(clause, s);
            TSNode local = field(specifier, "name");
            TSNode alias = field(specifier, "alias");
            const char *local_name = node_text(index, local);
            const char *alias_name = ts_node_is_null(alias) ? local_name : node_text(index, alias);
            if (local_name && alias_name) {
                if (local_export && kind_is(local, "identifier") &&
                    strcmp(local_name, alias_name) == 0) {
                    add_name(index, local_name, 0,
                             type_export || child_token(specifier, "type")
                                 ? BINDING_TYPE_EXPORT_MARKER
                                 : BINDING_EXPORT_MARKER);
                } else {
                    add_name(index, alias_name, 0, BINDING_EXPORT_CONFLICT);
                }
            }
        }
    }
}

static bool javascript_binding_child(const char *parent, const char *field_name) {
    if (!parent || !field_name) {
        return false;
    }
    if (strcmp(parent, "variable_declarator") == 0) {
        return strcmp(field_name, "name") == 0;
    }
    if (strcmp(parent, "assignment_expression") == 0 ||
        strcmp(parent, "augmented_assignment_expression") == 0 ||
        strcmp(parent, "for_in_statement") == 0) {
        return strcmp(field_name, "left") == 0;
    }
    if (strcmp(parent, "update_expression") == 0) {
        return true;
    }
    return (strcmp(parent, "arrow_function") == 0 || strcmp(parent, "catch_clause") == 0) &&
           strcmp(field_name, "parameter") == 0;
}

static binding_walk_t note_javascript_node(cbm_resolve_binding_index_t *index, TSNode node,
                                           const char *parent_kind, const char *field_name,
                                           unsigned depth, binding_walk_t state) {
    const char *kind = ts_node_type(node);
    if (!ts_node_is_named(node)) {
        return state;
    }
    if (!supported_javascript_node(kind)) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "unsupported_binding_syntax");
        return state;
    }
    if (kind_is(node, "import_statement")) {
        index->javascript_module |= depth == 1;
        state.import = true;
        state.type_only = child_token(node, "type");
        if (depth != 1) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "non_module_import_binding");
        }
    }
    if (kind_is(node, "export_statement")) {
        index->javascript_module |= depth == 1;
        state.named_export = depth == 1 && !child_token(node, "default");
        note_javascript_export(index, node);
    }
    if (state.import) {
        note_javascript_import(index, node, parent_kind, state.type_only);
    }
    unsigned declaration = javascript_declaration_kind(kind);
    bool module_declaration =
        depth == 1 || (depth == 2 && parent_kind && strcmp(parent_kind, "export_statement") == 0);
    if (declaration) {
        TSNode name = field(node, "name");
        unsigned flags = module_declaration ? declaration : BINDING_BLOCKED;
        if (module_declaration && state.named_export) {
            flags |= BINDING_EXPORTED;
        }
        if (!ts_node_is_null(name)) {
            /* Module function declarations are hoisted, including exported
             * declarations. Other binding forms block same-name references. */
            add_name(index, node_text(index, name), 0, flags);
        }
    } else if (strcmp(kind, "function_expression") == 0 ||
               strcmp(kind, "generator_function") == 0 || strcmp(kind, "class") == 0) {
        TSNode name = field(node, "name");
        if (!ts_node_is_null(name)) {
            add_name(index, node_text(index, name), 0, BINDING_BLOCKED);
        }
    }
    if (strcmp(kind, "formal_parameters") == 0 ||
        javascript_binding_child(parent_kind, field_name)) {
        state.blocked = true;
    }
    bool declaration_name = declaration && field_name && strcmp(field_name, "name") == 0;
    bool parent_declaration_name = parent_kind && field_name && strcmp(field_name, "name") == 0 &&
                                   javascript_declaration_kind(parent_kind);
    if (strcmp(kind, "identifier") == 0 || strcmp(kind, "shorthand_property_identifier") == 0 ||
        strcmp(kind, "shorthand_property_identifier_pattern") == 0) {
        const char *name = node_text(index, node);
        if (name && (strcmp(name, "eval") == 0 || strcmp(name, "Function") == 0)) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dynamic_namespace_effect");
        }
        if (state.blocked && !parent_declaration_name) {
            add_name(index, name, 0, BINDING_BLOCKED);
        }
        if (!state.import && !state.blocked && !parent_declaration_name && !declaration_name) {
            add_site(index, name, node);
        }
    }
    if (strcmp(kind, "call_expression") == 0 || strcmp(kind, "new_expression") == 0) {
        note_call(index, node);
    }
    return state;
}

/* Dart libraries bind their complete public namespace. Source prefixes and
 * combinators are retained independently of the generic import extractor. */
static TSNode dart_identifier(TSNode node) {
    TSNode name = field(node, "name");
    if (kind_is(name, "identifier") || kind_is(name, "type_identifier"))
        return name;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (kind_is(child, "identifier"))
            return child;
    }
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (kind_is(child, "type_identifier"))
            return child;
    }
    return (TSNode){0};
}

static const char *dart_uri(cbm_resolve_binding_index_t *index, TSNode node) {
    const char *text = node_text(index, node);
    size_t length = text ? strlen(text) : 0;
    if (length < 3 || (text[0] != '\'' && text[0] != '"') || text[length - 1] != text[0])
        return NULL;
    for (size_t i = 1; i + 1 < length; i++) {
        if (text[i] == '\\' || text[i] == '$' || text[i] == '\n' || text[i] == '\r' ||
            text[i] == '%' || text[i] == '?' || text[i] == '#' || text[i] == text[0])
            return NULL;
    }
    return cbm_arena_strndup(index->arena, text + 1, length - 2);
}

static void dart_combinator(cbm_resolve_binding_index_t *index, TSNode node,
                            cbm_resolve_source_import_t *import) {
    bool show = child_token(node, "show"), hide = child_token(node, "hide");
    if (show == hide || (show && import->has_show) || (hide && import->hide)) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dart_combinator_composition_required");
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    const char **names = cbm_arena_calloc(index->arena, (size_t)count * sizeof(*names));
    if (!names && count) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "dart_import_allocation");
        return;
    }
    size_t size = 0;
    for (uint32_t c = 0; c < count; c++) {
        TSNode child = ts_node_named_child(node, c);
        if (!kind_is(child, "identifier")) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dart_combinator_syntax_required");
            return;
        }
        names[size++] = node_text(index, child);
    }
    if (show) {
        import->has_show = true;
        import->show = names;
        import->show_count = size;
    } else {
        import->hide = names;
        import->hide_count = size;
    }
}

static void note_dart_import(cbm_resolve_binding_index_t *index, TSNode node) {
    cbm_resolve_source_import_t import = {0};
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    unsigned depth = 0;
    uint32_t visited = 0;
    for (;;) {
        TSNode child = ts_tree_cursor_current_node(&cursor);
        if (++visited > BINDING_MAX_NODES || depth >= BINDING_MAX_DEPTH) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_LIMITED, "dart_import_walk_limit");
            break;
        }
        const char *kind = ts_node_type(child);
        if (strcmp(kind, "configuration_uri") == 0 || strcmp(kind, "deferred") == 0) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                       "dart_import_configuration_required");
            break;
        }
        if (strcmp(kind, "string_literal") == 0) {
            if (import.uri || !(import.uri = dart_uri(index, child))) {
                fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dart_import_uri_required");
                break;
            }
        }
        if (strcmp(kind, "identifier") == 0 &&
            kind_is(ts_node_parent(child), "import_specification")) {
            if (import.prefix) {
                fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dart_import_prefix_required");
                break;
            }
            import.prefix = node_text(index, child);
        }
        if (strcmp(kind, "combinator") == 0)
            dart_combinator(index, child, &import);
        if (index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
            break;
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor))
                goto finished;
            depth--;
        }
    }
finished:
    ts_tree_cursor_delete(&cursor);
    if (index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return;
    if (!import.uri) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dart_import_uri_required");
        return;
    }
    cbm_resolve_source_import_t *imports = grow_array(index, index->imports, index->import_count,
                                                      &index->import_capacity, sizeof(*imports));
    if (!imports)
        return;
    index->imports = imports;
    imports[index->import_count++] = import;
    if (import.prefix)
        add_name(index, import.prefix, 0, BINDING_IMPORT);
}

static bool supported_dart_node(const char *kind) {
    static const char *const syntax[] = {"program",
                                         "void_type",
                                         "identifier",
                                         "type_identifier",
                                         "decimal_integer_literal",
                                         "hex_integer_literal",
                                         "decimal_floating_point_literal",
                                         "true",
                                         "false",
                                         "string_literal",
                                         "template_substitution",
                                         "escape_sequence",
                                         "null_literal",
                                         "list_literal",
                                         "set_or_map_literal",
                                         "pair",
                                         "record_literal",
                                         "record_field",
                                         "throw_expression",
                                         "throw_expression_without_cascade",
                                         "rethrow_expression",
                                         "assignment_expression",
                                         "assignment_expression_without_cascade",
                                         "assignable_expression",
                                         "lambda_expression",
                                         "function_expression",
                                         "if_null_expression",
                                         "conditional_expression",
                                         "logical_or_expression",
                                         "logical_and_expression",
                                         "equality_expression",
                                         "type_cast_expression",
                                         "type_test_expression",
                                         "relational_expression",
                                         "relational_operator",
                                         "bitwise_or_expression",
                                         "bitwise_xor_expression",
                                         "bitwise_and_expression",
                                         "shift_expression",
                                         "additive_expression",
                                         "multiplicative_expression",
                                         "bitwise_operator",
                                         "shift_operator",
                                         "additive_operator",
                                         "multiplicative_operator",
                                         "unary_expression",
                                         "postfix_expression",
                                         "postfix_operator",
                                         "spread_element",
                                         "selector",
                                         "prefix_operator",
                                         "negation_operator",
                                         "tilde_operator",
                                         "await_expression",
                                         "type_test",
                                         "is_operator",
                                         "type_cast",
                                         "as_operator",
                                         "new_expression",
                                         "const_object_expression",
                                         "parenthesized_expression",
                                         "constructor_invocation",
                                         "arguments",
                                         "argument",
                                         "named_argument",
                                         "index_selector",
                                         "argument_part",
                                         "unconditional_assignable_selector",
                                         "conditional_assignable_selector",
                                         "type_arguments",
                                         "local_function_declaration",
                                         "block",
                                         "expression_statement",
                                         "assert_statement",
                                         "assertion",
                                         "assertion_arguments",
                                         "do_statement",
                                         "break_statement",
                                         "continue_statement",
                                         "yield_statement",
                                         "yield_each_statement",
                                         "return_statement",
                                         "try_statement",
                                         "catch_clause",
                                         "catch_parameters",
                                         "finally_clause",
                                         "if_element",
                                         "if_statement",
                                         "while_statement",
                                         "for_statement",
                                         "for_loop_parts",
                                         "for_element",
                                         "import_or_export",
                                         "library_import",
                                         "import_specification",
                                         "uri",
                                         "configurable_uri",
                                         "combinator",
                                         "class_definition",
                                         "superclass",
                                         "interfaces",
                                         "class_body",
                                         "getter_signature",
                                         "setter_signature",
                                         "method_signature",
                                         "declaration",
                                         "identifier_list",
                                         "initialized_identifier_list",
                                         "initialized_identifier",
                                         "static_final_declaration_list",
                                         "static_final_declaration",
                                         "initialized_variable_definition",
                                         "function_type",
                                         "parameter_type_list",
                                         "normal_parameter_type",
                                         "optional_parameter_types",
                                         "optional_positional_parameter_types",
                                         "named_parameter_types",
                                         "typed_identifier",
                                         "nullable_type",
                                         "nullable_selector",
                                         "inferred_type",
                                         "function_body",
                                         "function_expression_body",
                                         "function_signature",
                                         "formal_parameter_list",
                                         "optional_formal_parameters",
                                         "formal_parameter",
                                         "local_variable_declaration",
                                         "type_parameters",
                                         "type_parameter",
                                         "type_bound",
                                         "type_alias",
                                         "this",
                                         "super",
                                         "label",
                                         "comment",
                                         "documentation_comment",
                                         "script_tag",
                                         "library_name",
                                         "dotted_identifier_list",
                                         "abstract",
                                         "interface",
                                         "base",
                                         "sealed",
                                         NULL};
    return listed(kind, syntax);
}

static void note_dart_call(cbm_resolve_binding_index_t *index, TSNode node) {
    TSNode callee = ts_node_prev_named_sibling(node);
    if (!kind_is(callee, "identifier"))
        return;
    /* A selector for a member is not an invocation of the preceding receiver. */
    TSNode first = ts_node_named_child(node, 0);
    if (!kind_is(first, "argument_part") && !kind_is(first, "arguments"))
        return;
    const char *name = node_text(index, callee);
    add_site(index, name, node);
    add_site(index, name, callee);
    binding_site_t *sites =
        grow_array(index, index->sites, index->site_count, &index->site_capacity, sizeof(*sites));
    if (!sites)
        return;
    index->sites = sites;
    sites[index->site_count++] =
        (binding_site_t){name, ts_node_start_byte(callee), ts_node_end_byte(node), 0};
}

static binding_walk_t note_dart_node(cbm_resolve_binding_index_t *index, TSNode node,
                                     const char *parent_kind, const char *field_name,
                                     unsigned depth, binding_walk_t state) {
    (void)field_name;
    const char *kind = ts_node_type(node);
    if (!ts_node_is_named(node))
        return state;
    if (strcmp(kind, "part_directive") == 0 || strcmp(kind, "part_of_directive") == 0 ||
        strcmp(kind, "library_export") == 0) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                   "dart_library_namespace_expansion_required");
        return state;
    }
    if (!supported_dart_node(kind)) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "unsupported_dart_binding_syntax");
        return state;
    }
    if (strcmp(kind, "library_import") == 0)
        note_dart_import(index, node);
    if (strcmp(kind, "import_or_export") == 0)
        state.import = true;
    if (strcmp(kind, "function_body") == 0 || strcmp(kind, "class_body") == 0)
        state.named_export = false;
    bool function = strcmp(kind, "function_signature") == 0;
    bool type = strcmp(kind, "class_definition") == 0 || strcmp(kind, "type_alias") == 0;
    bool accessor = strcmp(kind, "getter_signature") == 0 || strcmp(kind, "setter_signature") == 0;
    if (function || type || accessor) {
        TSNode name = dart_identifier(node);
        unsigned flags = BINDING_BLOCKED;
        if (depth == 1) {
            flags = BINDING_NAMESPACE_MEMBER;
            flags |= function ? BINDING_DECLARATION
                     : type   ? BINDING_TYPE_DECLARATION
                              : BINDING_BLOCKED;
        }
        if (!ts_node_is_null(name))
            add_name(index, node_text(index, name), 0, flags);
        else
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dart_declaration_name_required");
    }
    bool variable = strcmp(kind, "initialized_identifier_list") == 0 ||
                    strcmp(kind, "initialized_variable_definition") == 0 ||
                    strcmp(kind, "static_final_declaration_list") == 0 ||
                    strcmp(kind, "local_variable_declaration") == 0;
    if (variable || strcmp(kind, "formal_parameter_list") == 0 ||
        strcmp(kind, "catch_parameters") == 0 || strcmp(kind, "for_loop_parts") == 0 ||
        strstr(kind, "assignment_expression") ||
        ((strcmp(kind, "postfix_expression") == 0 || strcmp(kind, "unary_expression") == 0) &&
         (child_token(node, "++") || child_token(node, "--"))))
        state.blocked = true;
    if (variable && depth == 1)
        state.named_export = true;
    /* Selector/type names are not free lexical occurrences. */
    if (strcmp(kind, "selector") == 0 || strcmp(kind, "type_arguments") == 0 ||
        strcmp(kind, "type_parameters") == 0 || strcmp(kind, "library_name") == 0)
        state.type_only = true;
    if (strcmp(kind, "identifier") == 0) {
        const char *name = node_text(index, node);
        bool declaration_name = parent_kind && (strcmp(parent_kind, "function_signature") == 0 ||
                                                strcmp(parent_kind, "class_definition") == 0 ||
                                                strcmp(parent_kind, "type_alias") == 0 ||
                                                strcmp(parent_kind, "getter_signature") == 0 ||
                                                strcmp(parent_kind, "setter_signature") == 0);
        if (state.blocked && !state.import && !declaration_name)
            add_name(index, name, 0,
                     BINDING_BLOCKED | (state.named_export ? BINDING_NAMESPACE_MEMBER : 0));
        TSNode parent = ts_node_parent(node);
        bool label_name =
            kind_is(parent, "label") ||
            (kind_is(parent, "named_argument") && ts_node_eq(node, ts_node_named_child(parent, 0)));
        if (!state.blocked && !state.import && !state.type_only && !declaration_name && !label_name)
            add_site(index, name, node);
    }
    if (strcmp(kind, "selector") == 0 && !state.import)
        note_dart_call(index, node);
    return state;
}

static bool supported_rust_node(const char *kind) {
    static const char *const syntax[] = {"source_file",
                                         "function_item",
                                         "function_modifiers",
                                         "visibility_modifier",
                                         "identifier",
                                         "type_identifier",
                                         "primitive_type",
                                         "parameters",
                                         "parameter",
                                         "self_parameter",
                                         "mutable_specifier",
                                         "reference_type",
                                         "pointer_type",
                                         "function_type",
                                         "unit_type",
                                         "tuple_type",
                                         "block",
                                         "expression_statement",
                                         "empty_statement",
                                         "call_expression",
                                         "arguments",
                                         "integer_literal",
                                         "float_literal",
                                         "boolean_literal",
                                         "char_literal",
                                         "string_literal",
                                         "string_content",
                                         "escape_sequence",
                                         "raw_string_literal",
                                         "line_comment",
                                         "block_comment",
                                         "doc_comment",
                                         "inner_doc_comment_marker",
                                         "outer_doc_comment_marker",
                                         "return_expression",
                                         "binary_expression",
                                         "unary_expression",
                                         "parenthesized_expression",
                                         "reference_expression",
                                         "assignment_expression",
                                         "compound_assignment_expr",
                                         "let_declaration",
                                         "tuple_pattern",
                                         "tuple_struct_pattern",
                                         "struct_pattern",
                                         "field_pattern",
                                         "ref_pattern",
                                         "mut_pattern",
                                         "remaining_field_pattern",
                                         "captured_pattern",
                                         "slice_pattern",
                                         "use_declaration",
                                         "scoped_identifier",
                                         "use_as_clause",
                                         "crate",
                                         "self",
                                         "super",
                                         "mod_item",
                                         "const_item",
                                         "static_item",
                                         "struct_item",
                                         "field_declaration_list",
                                         "field_declaration",
                                         "field_identifier",
                                         "ordered_field_declaration_list",
                                         "field_expression",
                                         "struct_expression",
                                         "field_initializer_list",
                                         "field_initializer",
                                         "shorthand_field_initializer",
                                         "array_expression",
                                         "tuple_expression",
                                         "type_cast_expression",
                                         "if_expression",
                                         "else_clause",
                                         "while_expression",
                                         "loop_expression",
                                         "for_expression",
                                         "break_expression",
                                         "continue_expression",
                                         "range_expression",
                                         "index_expression",
                                         NULL};
    return listed(kind, syntax);
}

static void note_rust_import(cbm_resolve_binding_index_t *index, TSNode node, unsigned depth) {
    TSNode argument = field(node, "argument");
    TSNode name = argument;
    if (kind_is(argument, "use_as_clause")) {
        name = field(argument, "alias");
        argument = field(argument, "path");
    } else if (kind_is(argument, "scoped_identifier")) {
        name = field(argument, "name");
    }
    if (depth != 1 || !kind_is(name, "identifier") ||
        (!kind_is(argument, "identifier") && !kind_is(argument, "scoped_identifier"))) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "rust_import_binding_unsupported");
        return;
    }
    add_name(index, node_text(index, name), 0, BINDING_IMPORT);
}

static binding_walk_t note_rust_node(cbm_resolve_binding_index_t *index, TSNode node,
                                     const char *parent_kind, const char *field_name,
                                     unsigned depth, binding_walk_t inherited) {
    binding_walk_t state = inherited;
    const char *kind = ts_node_type(node);
    if (!ts_node_is_named(node))
        return state;
    if (!supported_rust_node(kind)) {
        /* Attributes can conditionally remove or generate declarations; a
         * macro can introduce lexical bindings. Neither is a completed domain. */
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "unsupported_rust_binding_syntax");
        return state;
    }
    if (strcmp(kind, "use_declaration") == 0) {
        note_rust_import(index, node, depth);
        state.import = true;
    }
    if (strcmp(kind, "mod_item") == 0 && !ts_node_is_null(field(node, "body"))) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "rust_inline_module_domain_required");
        return state;
    }
    bool function = strcmp(kind, "function_item") == 0;
    bool declaration = function || strcmp(kind, "struct_item") == 0 ||
                       strcmp(kind, "const_item") == 0 || strcmp(kind, "static_item") == 0 ||
                       strcmp(kind, "mod_item") == 0;
    if (declaration) {
        unsigned flags = function && depth == 1 ? BINDING_DECLARATION : BINDING_BLOCKED;
        for (uint32_t i = 0; function && depth == 1 && i < ts_node_named_child_count(node); i++) {
            TSNode child = ts_node_named_child(node, i);
            if (kind_is(child, "visibility_modifier")) {
                const char *visibility = node_text(index, child);
                if (visibility && strcmp(visibility, "pub") == 0)
                    flags |= BINDING_EXPORTED;
            }
        }
        add_name(index, node_text(index, field(node, "name")), 0, flags);
    }
    if (strcmp(kind, "parameter") == 0 || strcmp(kind, "self_parameter") == 0 ||
        strcmp(kind, "let_declaration") == 0 || strcmp(kind, "for_expression") == 0 ||
        strcmp(kind, "assignment_expression") == 0 || strcmp(kind, "compound_assignment_expr") == 0)
        state.blocked = true;
    if (field_name && strcmp(field_name, "type") == 0)
        state.type_only = true;
    if (strcmp(kind, "identifier") == 0) {
        bool declaration_name =
            field_name && strcmp(field_name, "name") == 0 && parent_kind &&
            (strcmp(parent_kind, "function_item") == 0 || strcmp(parent_kind, "const_item") == 0 ||
             strcmp(parent_kind, "static_item") == 0 || strcmp(parent_kind, "mod_item") == 0);
        const char *name = node_text(index, node);
        if (state.blocked && !state.import && !state.type_only && !declaration_name)
            add_name(index, name, 0, BINDING_BLOCKED);
        if (!state.import && !state.type_only && !declaration_name)
            add_site(index, name, node);
    }
    if (strcmp(kind, "call_expression") == 0)
        note_call(index, node);
    return state;
}

static binding_walk_t note_node(cbm_resolve_binding_index_t *index, TSNode node,
                                const char *parent_kind, const char *field_name, unsigned depth,
                                binding_walk_t inherited) {
    if (index->language == CBM_LANG_RUST) {
        return note_rust_node(index, node, parent_kind, field_name, depth, inherited);
    }
    if (index->language == CBM_LANG_DART) {
        return note_dart_node(index, node, parent_kind, field_name, depth, inherited);
    }
    if (javascript_language(index->language)) {
        return note_javascript_node(index, node, parent_kind, field_name, depth, inherited);
    }
    const char *kind = ts_node_type(node);
    binding_walk_t state = inherited;
    if (index->language == CBM_LANG_PYTHON) {
        index->current_scope = inherited.scope;
        if (!strcmp(kind, "global_statement") || !strcmp(kind, "nonlocal_statement")) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "python_namespace_write_unproven");
            return state;
        }
        if (!strcmp(kind, "if_statement") || !strcmp(kind, "for_statement") ||
            !strcmp(kind, "while_statement") || !strcmp(kind, "try_statement") ||
            !strcmp(kind, "with_statement"))
            state.conditional = true;
    }
    if (!ts_node_is_named(node)) {
        return state;
    }
    if (!supported_node(index->language, kind)) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "unsupported_binding_syntax");
        return state;
    }
    if (index->language == CBM_LANG_PYTHON && !strcmp(kind, "wildcard_import") &&
        (inherited.scope || inherited.conditional)) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "conditional_wildcard_import");
        return state;
    }
    bool import = kind_is(node, "import_statement") || kind_is(node, "import_from_statement");
    if (import) {
        state.import = true;
        if (depth == 1) {
            note_import(index, node);
        } else {
            state.blocked = true;
        }
    }
    if (index->language == CBM_LANG_PYTHON && strcmp(kind, "class_definition") == 0) {
        /* A class creates a binding in its enclosing namespace. Its members
         * do not authorize callable targets, but must not erase independent
         * module function/import proofs elsewhere in the file. */
        add_name(index, node_text(index, field(node, "name")), ts_node_start_byte(node),
                 !state.conditional && (!parent_kind || strcmp(parent_kind, "decorated_definition"))
                     ? BINDING_TYPE_DECLARATION | BINDING_VALUE_TYPE
                     : BINDING_BLOCKED);
    }
    if (strcmp(kind, "function_definition") == 0 || strcmp(kind, "function_declaration") == 0) {
        TSNode name = field(node, "name");
        bool supported = kind_is(name, "identifier") &&
                         (index->language == CBM_LANG_PYTHON
                              ? !state.conditional &&
                                    (!parent_kind || strcmp(parent_kind, "decorated_definition"))
                              : depth == 1 && lua_local_function(node));
        if (!ts_node_is_null(name)) {
            add_name(index, node_text(index, name), ts_node_start_byte(node),
                     supported ? BINDING_DECLARATION : BINDING_BLOCKED);
        }
    }
    if (index->language == CBM_LANG_PYTHON &&
        (!strcmp(kind, "function_definition") || !strcmp(kind, "class_definition") ||
         !strcmp(kind, "lambda"))) {
        state.scope =
            python_child_scope(index, node, inherited.scope, !strcmp(kind, "class_definition"));
        state.conditional = false;
        index->current_scope = state.scope;
    }
    /* Annotation expressions read names; they do not bind the names inside
     * the type. The parameter name itself remains a shadowing declaration. */
    if (index->language == CBM_LANG_PYTHON && strcmp(kind, "type") == 0 && field_name &&
        strcmp(field_name, "type") == 0 && parent_kind &&
        (strcmp(parent_kind, "typed_parameter") == 0 ||
         strcmp(parent_kind, "typed_default_parameter") == 0))
        state.blocked = false;
    if (index->language == CBM_LANG_PYTHON) {
        state.blocked = python_target_role(kind, parent_kind, field_name, state.blocked);
    } else if (blocks_identifiers(kind)) {
        state.blocked = true;
    }
    if (strcmp(kind, "identifier") == 0) {
        const char *name = node_text(index, node);
        if (!name) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "identifier_unavailable");
            return state;
        }
        if (dynamic_name(name)) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dynamic_namespace_effect");
        }
        bool function_name = field_name && strcmp(field_name, "name") == 0 && parent_kind &&
                             (strcmp(parent_kind, "function_definition") == 0 ||
                              strcmp(parent_kind, "function_declaration") == 0);
        if (state.blocked && !function_name) {
            add_name(index, name, ts_node_start_byte(node), BINDING_BLOCKED);
        }
        if (!state.import && !function_name) {
            add_site(index, name, node);
        }
    }
    if (strcmp(kind, "call") == 0 || strcmp(kind, "function_call") == 0) {
        note_call(index, node);
    }
    return state;
}

static int compare_name(const void *left, const void *right) {
    const binding_name_t *a = left;
    const binding_name_t *b = right;
    int order = strcmp(a->name, b->name);
    return order ? order : (a->scope > b->scope) - (a->scope < b->scope);
}

static void apply_export_markers(cbm_resolve_binding_index_t *index) {
    size_t destination = 0;
    for (size_t start = 0; start < index->name_count;) {
        size_t end = start;
        bool exported = false;
        bool conflict = false;
        bool type_exported = false;
        while (end < index->name_count &&
               strcmp(index->names[start].name, index->names[end].name) == 0 &&
               index->names[start].scope == index->names[end].scope) {
            exported |= (index->names[end].flags & BINDING_EXPORT_MARKER) != 0;
            conflict |= (index->names[end].flags & BINDING_EXPORT_CONFLICT) != 0;
            type_exported |= (index->names[end].flags & BINDING_TYPE_EXPORT_MARKER) != 0;
            end++;
        }
        for (size_t i = start; i < end; i++) {
            binding_name_t name = index->names[i];
            if (name.flags &
                (BINDING_EXPORT_MARKER | BINDING_EXPORT_CONFLICT | BINDING_TYPE_EXPORT_MARKER)) {
                continue;
            }
            if (exported && (name.flags & (BINDING_DECLARATION | BINDING_TYPE_DECLARATION))) {
                name.flags |= BINDING_EXPORTED;
            }
            if (type_exported && (name.flags & BINDING_TYPE_DECLARATION)) {
                name.flags |= BINDING_TYPE_EXPORTED;
            }
            if (conflict) {
                name.flags &= ~(BINDING_EXPORTED | BINDING_TYPE_EXPORTED);
            }
            index->names[destination++] = name;
        }
        start = end;
    }
    index->name_count = destination;
}

static int compare_site(const void *left, const void *right) {
    const binding_site_t *a = left;
    const binding_site_t *b = right;
    if (a->start != b->start) {
        return a->start < b->start ? -1 : 1;
    }
    if (a->end != b->end) {
        return a->end < b->end ? -1 : 1;
    }
    return strcmp(a->name, b->name);
}

static void walk_tree(cbm_resolve_binding_index_t *index, TSNode root) {
    binding_walk_t state[BINDING_MAX_DEPTH] = {{0}};
    const char *kinds[BINDING_MAX_DEPTH] = {0};
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    unsigned depth = 0;
    uint32_t visited = 0;
    for (;;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (++visited > BINDING_MAX_NODES || depth >= BINDING_MAX_DEPTH) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_LIMITED, "binding_walk_limit");
            break;
        }
        kinds[depth] = ts_node_type(node);
        state[depth] = note_node(index, node, depth ? kinds[depth - 1] : NULL,
                                 ts_tree_cursor_current_field_name(&cursor), depth,
                                 depth ? state[depth - 1] : (binding_walk_t){0});
        if (index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE) {
            break;
        }
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor)) {
                ts_tree_cursor_delete(&cursor);
                return;
            }
            depth--;
        }
    }
    ts_tree_cursor_delete(&cursor);
}

cbm_resolve_binding_index_t *cbm_resolve_binding_build(CBMArena *arena, const CBMFileResult *result,
                                                       CBMLanguage language, const char *source,
                                                       size_t source_len) {
    cbm_resolve_binding_index_t *index = arena ? cbm_arena_calloc(arena, sizeof(*index)) : NULL;
    if (!index) {
        return NULL;
    }
    *index = (cbm_resolve_binding_index_t){.arena = arena,
                                           .result = result,
                                           .language = language,
                                           .source = source,
                                           .source_len = source_len,
                                           .analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                           .reason = "unsupported_binding_language"};
    if (language != CBM_LANG_PYTHON && language != CBM_LANG_LUA && language != CBM_LANG_DART &&
        language != CBM_LANG_RUST && !javascript_language(language)) {
        if (language == CBM_LANG_SWIFT)
            index->reason = "swift_build_module_membership_required";
        return index;
    }
    if (!result || !source || !result->module_qn || source_len > UINT32_MAX || result->has_error ||
        result->parse_incomplete || result->parse_unusable || result->lsp_skipped) {
        index->reason = "incomplete_binding_source";
        return index;
    }
    TSTree *owned_tree = NULL;
    const TSTree *tree = result->cached_tree;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        if (parser && ts_parser_set_language(parser, cbm_ts_language(language))) {
            owned_tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)source_len);
        }
        if (parser) {
            ts_parser_delete(parser);
        }
        tree = owned_tree;
    }
    if (!tree || ts_node_has_error(ts_tree_root_node(tree)) ||
        ts_node_end_byte(ts_tree_root_node(tree)) != source_len) {
        index->reason = "incomplete_binding_tree";
    } else {
        index->analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        index->reason = "binding_complete_supported_subset";
        if (language == CBM_LANG_PYTHON) {
            index->scopes =
                grow_array(index, NULL, 0, &index->scope_capacity, sizeof(*index->scopes));
            if (index->scopes) {
                index->scopes[0] = (binding_scope_t){result->module_qn, 0, false};
                index->scope_count = 1;
            }
        }
        if (index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE)
            walk_tree(index, ts_tree_root_node(tree));
        if (index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE && javascript_language(language) &&
            !index->javascript_module) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "javascript_script_scope_unproven");
        }
    }
    if (owned_tree) {
        ts_tree_delete(owned_tree);
    }
    if (index->name_count) {
        qsort(index->names, index->name_count, sizeof(*index->names), compare_name);
        apply_export_markers(index);
    }
    if (index->site_count) {
        qsort(index->sites, index->site_count, sizeof(*index->sites), compare_site);
    }
    return index;
}

static const binding_name_t *find_scoped_name(const cbm_resolve_binding_index_t *index,
                                              const char *name, size_t length, uint32_t scope,
                                              bool *present) {
    size_t low = 0, high = index->name_count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        int compare = strncmp(index->names[mid].name, name, length);
        if (!compare && index->names[mid].name[length])
            compare = 1;
        if (!compare)
            compare = (index->names[mid].scope > scope) - (index->names[mid].scope < scope);
        if (compare < 0)
            low = mid + 1;
        else
            high = mid;
    }
    *present = low < index->name_count && index->names[low].scope == scope &&
               strlen(index->names[low].name) == length &&
               !strncmp(index->names[low].name, name, length);
    if (!*present)
        return NULL;
    if (low + 1 < index->name_count && index->names[low + 1].scope == scope &&
        !strcmp(index->names[low].name, index->names[low + 1].name))
        return NULL;
    if (index->language == CBM_LANG_PYTHON && scope == 0 &&
        index->names[low].at < index->python_wildcard_barrier)
        return NULL;
    return &index->names[low];
}

static const binding_name_t *find_name(const cbm_resolve_binding_index_t *index, const char *name,
                                       size_t length) {
    bool present;
    return find_scoped_name(index, name, length, 0, &present);
}

static const binding_name_t *find_lexical_name(const cbm_resolve_binding_index_t *index,
                                               const char *name, size_t length, uint32_t scope) {
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        bool present;
        const binding_name_t *found = find_scoped_name(index, name, length, scope, &present);
        if (present || !scope)
            return found;
        if (scope >= index->scope_count)
            return NULL;
        scope = index->scopes[scope].parent;
    }
    return NULL;
}

cbm_resolve_result_t cbm_resolve_binding_validate(const cbm_resolve_binding_index_t *index,
                                                  const char *name, uint32_t start, uint32_t end,
                                                  CBMSourceOrigin origin,
                                                  cbm_resolve_result_t candidate) {
    uint32_t prior_facts = candidate.facts;
    candidate.facts &= ~CBM_RESOLVE_FACT_LOOKUP_COMPLETE;
    candidate.analysis = index && index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE
                             ? index->analysis
                             : CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
    candidate.reason = cbm_resolve_binding_status(index);
    if (!index || index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !name || !*name ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start || !candidate.target ||
        candidate.binding != CBM_RESOLVE_BINDING_UNIQUE || candidate.candidate_count != 1 ||
        !(prior_facts & CBM_RESOLVE_FACT_EXACT_TARGET)) {
        return candidate;
    }
    binding_site_t site = {.name = name, .start = start, .end = end};
    const binding_site_t *located =
        bsearch(&site, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    if (!located) {
        candidate.reason = "binding_occurrence_missing";
        return candidate;
    }
    const char *dot = strchr(name, '.');
    size_t root_length = dot ? (size_t)(dot - name) : strlen(name);
    const binding_name_t *binding = find_lexical_name(index, name, root_length, located->scope);
    bool dart_import = index->language == CBM_LANG_DART && !dot && !binding &&
                       !cbm_resolve_binding_name_present(index, name) &&
                       candidate.rule == CBM_RESOLVE_RULE_IMPORT_BINDING &&
                       (prior_facts & CBM_RESOLVE_FACT_IMPORT_BOUND) &&
                       (prior_facts & CBM_RESOLVE_FACT_LOOKUP_COMPLETE);
    if (!dart_import && (!binding || (binding->flags & BINDING_BLOCKED) || start < binding->at)) {
        candidate.reason = "binding_shadowed_or_unavailable";
        return candidate;
    }
    if (dart_import) {
        candidate.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    } else if (candidate.rule == CBM_RESOLVE_RULE_SCOPE_BINDING && !dot &&
               (binding->flags & (BINDING_DECLARATION | BINDING_VALUE_TYPE)) && binding->target &&
               strcmp(binding->target, candidate.target) == 0) {
        candidate.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    } else if (candidate.rule == CBM_RESOLVE_RULE_IMPORT_BINDING &&
               (binding->flags & BINDING_IMPORT) && (prior_facts & CBM_RESOLVE_FACT_IMPORT_BOUND) &&
               (prior_facts & CBM_RESOLVE_FACT_LOOKUP_COMPLETE)) {
        candidate.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    } else {
        candidate.reason = "binding_target_or_export_unproven";
        return candidate;
    }
    if (index->language == CBM_LANG_PYTHON && candidate.rule == CBM_RESOLVE_RULE_SCOPE_BINDING &&
        (binding->flags & BINDING_TYPE_DECLARATION) && (binding->flags & BINDING_VALUE_TYPE))
        candidate.facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
    candidate.facts |= CBM_RESOLVE_FACT_LOOKUP_COMPLETE | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    candidate.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
    candidate.reason = "exact_lexical_binding";
    return candidate;
}

cbm_resolve_result_t cbm_resolve_binding_local(const cbm_resolve_binding_index_t *index,
                                               const char *name, uint32_t start, uint32_t end,
                                               CBMSourceOrigin origin) {
    cbm_resolve_result_t result = {.reason = "local_binding_unavailable"};
    if (!index || index->language != CBM_LANG_PYTHON ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !name || strchr(name, '.') ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start)
        return result;
    binding_site_t site = {.name = name, .start = start, .end = end};
    const binding_site_t *located =
        bsearch(&site, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    const binding_name_t *binding =
        located ? find_lexical_name(index, name, strlen(name), located->scope) : NULL;
    if (!binding || !binding->target || (binding->flags & BINDING_BLOCKED) ||
        !(binding->flags & (BINDING_DECLARATION | BINDING_VALUE_TYPE)))
        return result;
    result.binding = CBM_RESOLVE_BINDING_UNIQUE;
    result.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    result.rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
    result.facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_SCOPE_BOUND;
    result.target = binding->target;
    result.candidate_count = 1;
    return cbm_resolve_binding_validate(index, name, start, end, origin, result);
}

const char *cbm_resolve_binding_status(const cbm_resolve_binding_index_t *index) {
    return index ? index->reason : "binding_allocation_failed";
}

unsigned cbm_resolve_binding_export_kind(const cbm_resolve_binding_index_t *index,
                                         const char *qualified_name) {
    if (!index || index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !qualified_name ||
        (index->language != CBM_LANG_PYTHON && index->language != CBM_LANG_DART &&
         index->language != CBM_LANG_RUST && !javascript_language(index->language))) {
        return 0;
    }
    const char *name = strrchr(qualified_name, '.');
    if (!name || !name[1]) {
        return 0;
    }
    name++;
    if (index->language == CBM_LANG_DART && name[0] == '_')
        return 0;
    const binding_name_t *binding = find_name(index, name, strlen(name));
    if (!binding || !binding->target || strcmp(binding->target, qualified_name) != 0 ||
        ((javascript_language(index->language) || index->language == CBM_LANG_RUST) &&
         !(binding->flags & (BINDING_EXPORTED | BINDING_TYPE_EXPORTED)))) {
        return 0;
    }
    unsigned kind = 0;
    if ((binding->flags & (BINDING_DECLARATION | BINDING_VALUE_TYPE)) &&
        (index->language == CBM_LANG_PYTHON || index->language == CBM_LANG_DART ||
         (binding->flags & BINDING_EXPORTED))) {
        kind |= CBM_RESOLVE_EXPORT_VALUE;
    }
    if (binding->flags & BINDING_TYPE_DECLARATION) {
        kind |= CBM_RESOLVE_EXPORT_TYPE;
    }
    return kind;
}

bool cbm_resolve_binding_import(const cbm_resolve_binding_index_t *index, const char *local_name) {
    if (!index || index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !local_name)
        return false;
    const binding_name_t *binding = find_name(index, local_name, strlen(local_name));
    return binding && binding->flags == BINDING_IMPORT;
}

bool cbm_resolve_binding_export(const cbm_resolve_binding_index_t *index,
                                const char *qualified_name) {
    return cbm_resolve_binding_export_kind(index, qualified_name) != 0;
}

bool cbm_resolve_binding_name_present(const cbm_resolve_binding_index_t *index, const char *name) {
    if (!index || !name)
        return false;
    size_t low = 0, high = index->name_count;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (strcmp(index->names[middle].name, name) < 0)
            low = middle + 1;
        else
            high = middle;
    }
    return low < index->name_count && strcmp(index->names[low].name, name) == 0;
}

bool cbm_resolve_binding_namespace_complete(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_DART &&
           index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE;
}

size_t cbm_resolve_binding_namespace_size(const cbm_resolve_binding_index_t *index) {
    return cbm_resolve_binding_namespace_complete(index) ? index->name_count : 0;
}

const char *cbm_resolve_binding_namespace_member(const cbm_resolve_binding_index_t *index,
                                                 size_t offset, unsigned *kind) {
    if (kind)
        *kind = 0;
    if (!cbm_resolve_binding_namespace_complete(index) || offset >= index->name_count)
        return NULL;
    const binding_name_t *name = &index->names[offset];
    if (!(name->flags & BINDING_NAMESPACE_MEMBER) || name->name[0] == '_')
        return NULL;
    if (kind)
        *kind = cbm_resolve_binding_export_kind(index, name->target);
    return name->target;
}

size_t cbm_resolve_binding_import_count(const cbm_resolve_binding_index_t *index) {
    return cbm_resolve_binding_namespace_complete(index) ? index->import_count : 0;
}

const cbm_resolve_source_import_t *cbm_resolve_binding_import_at(
    const cbm_resolve_binding_index_t *index, size_t offset) {
    return cbm_resolve_binding_namespace_complete(index) && offset < index->import_count
               ? &index->imports[offset]
               : NULL;
}
