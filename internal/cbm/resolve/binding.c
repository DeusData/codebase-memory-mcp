#include "binding.h"
#include "../helpers.h"
#include "../lang_specs.h"
#include <stdlib.h>
#include <limits.h>
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
    BINDING_NAMESPACE_MEMBER = 1024,
    BINDING_VALUE_DECLARATION = 2048,
    /* ECMAScript only. A type-only import names nothing at runtime. */
    BINDING_TYPE_IMPORT = 4096,
    /* A declarator initialized by a function: callable once joined to its
     * extracted Function definition by exact source lines. */
    BINDING_FUNCTION_VALUE = 8192,
    /* Assigning a const is a runtime error, so a write cannot be a value flow. */
    BINDING_CONST_VALUE = 16384,
    /* Some write rebinds this variable: its members are no longer knowable. */
    BINDING_WRITTEN = 32768
};

typedef enum {
    JS_BINDER_NONE = 0,
    JS_BINDER_VAR,
    JS_BINDER_LEXICAL,
    JS_BINDER_PARAMETER,
    JS_BINDER_WRITE
} js_binder_t;

typedef enum { JS_SPACE_ANY = 0, JS_SPACE_VALUE, JS_SPACE_TYPE } js_space_t;

typedef struct {
    const char *name;
    const char *target;
    uint32_t at;
    unsigned flags;
    uint32_t scope;
    uint32_t value_start_line, value_end_line;
    unsigned value_definitions;
    bool http_escaped, http_factory_escaped;
    const char *http_import_path;
    /* ECMAScript: start of the initializer that binds at `at`, and the module
     * binding this row introduces. */
    uint32_t at_start;
    const cbm_resolve_js_import_t *js_import;
} binding_name_t;

typedef struct binding_pending_declaration {
    const char *name;
    uint32_t scope;
    const struct binding_pending_declaration *parent;
} binding_pending_declaration_t;

typedef struct {
    const char *name;
    uint32_t start;
    uint32_t end;
    uint32_t scope;
    const binding_pending_declaration_t *pending;
    bool invocation;
    bool jsx_component;
    bool type_position;
    /* A plain `a.b.c` member read. The occurrence is `c`; `a` is what binds. */
    const char *member_path;
    /* What `this` means here, when a member assignment or literal decides it. */
    const char *this_owner;
    bool this_prototype;
    bool http_escape, http_factory_escape;
    const char *http_scope, *http_url;
} binding_site_t;

typedef struct {
    const char *qualified_name;
    uint32_t parent;
    bool class_scope;
    bool function_scope; /* ECMAScript: the boundary a `var` hoists to. */
    bool namespace_scope;
} binding_scope_t;

/* A module-scope write or an `exports.x = v` value, captured during the walk
 * and settled once every declaration in the file is known. */
typedef struct {
    const char *name;
    uint32_t scope;
} js_write_t;

typedef enum {
    JS_VALUE_UNKNOWN = 0,
    JS_VALUE_IDENTIFIER,
    JS_VALUE_FUNCTION,
    JS_VALUE_REQUIRE,
    JS_VALUE_OBJECT
} js_value_kind_t;

/* `Owner.member = function ...`, `Owner.prototype.member = ...` or a member of the
 * object literal `const Owner = {...}`, at module level. */
#define JS_MEMBER_COMPUTED "[]"
typedef struct {
    const char *owner, *member;
    const char *function_name, *local;
    const char *target;
    uint32_t start_line, end_line;
    bool prototype, conflict;
} js_member_t;

typedef struct {
    const char *export_name;
    const char *owner; /* `owner.name = v`, kept only if owner proves to be module.exports. */
    js_value_kind_t value;
    const char *local;
    const char *source, *member;
    const char *function_name;
    uint32_t start_line, end_line;
    unsigned kind;
    bool whole, bare_exports, default_import;
} js_raw_export_t;

typedef struct {
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_lookup_context_t context;
    const char *name;
    uint32_t start, end;
    CBMSourceOrigin origin;
    const cbm_resolve_proof_t *proof;
} binding_observation_t;

struct cbm_resolve_binding_index {
    CBMArena *arena;
    binding_observation_t *observation;
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
    bool python_namespace_closed;
    bool python_annotation_binding_preserved;
    bool python_annotation_storage_bound;
    const char **python_escapes;
    size_t python_escape_count, python_escape_capacity;
    bool python_effects_complete;
    const char *http_default_target, *http_default_base;
    cbm_resolve_http_dependency_t *http_dependencies;
    size_t http_dependency_count, http_dependency_capacity;
    bool http_domain_unknown;
    binding_scope_t *scopes;
    size_t scope_count, scope_capacity;
    uint32_t current_scope;
    const binding_pending_declaration_t *current_pending;
    uint32_t python_wildcard_barrier;
    cbm_resolve_source_import_t *imports;
    size_t import_count, import_capacity;
    js_write_t *js_writes;
    size_t js_write_count, js_write_capacity;
    js_raw_export_t *js_raw_exports;
    size_t js_raw_export_count, js_raw_export_capacity;
    cbm_resolve_js_export_t *js_exports;
    size_t js_export_count;
    bool js_commonjs, js_whole_export, js_exports_rebound, js_exports_open;
    const char **js_export_aliases;
    size_t js_export_alias_count, js_export_alias_capacity;
    js_member_t *js_members;
    size_t js_member_count, js_member_capacity;
    const char *current_this_owner;
    bool current_this_prototype;
};

static bool kind_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && strcmp(ts_node_type(node), kind) == 0;
}

static TSNode field(TSNode node, const char *name) {
    if (ts_node_is_null(node))
        return (TSNode){0};
    return ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}

static const char *node_text(cbm_resolve_binding_index_t *index, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
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
    if (index->language == CBM_LANG_PYTHON && index->current_scope == 0 &&
        !strcmp(name, "__annotations__"))
        index->python_annotation_storage_bound = true;
    binding_name_t *names =
        grow_array(index, index->names, index->name_count, &index->name_capacity, sizeof(*names));
    if (!names) {
        return;
    }
    index->names = names;
    const char *target = NULL;
    if (flags & (BINDING_DECLARATION | BINDING_TYPE_DECLARATION | BINDING_NAMESPACE_MEMBER |
                 BINDING_VALUE_DECLARATION)) {
        const char *owner = index->current_scope < index->scope_count
                                ? index->scopes[index->current_scope].qualified_name
                                : index->result->module_qn;
        target = cbm_arena_sprintf(index->arena, "%s.%s", owner, name);
        if (!target) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_target_allocation");
            return;
        }
    }
    names[index->name_count++] = (binding_name_t){
        .name = name, .target = target, .at = at, .flags = flags, .scope = index->current_scope};
}

/* A value declaration never establishes callable identity. Mark both the
 * complete invocation and its identifier, including transparent wrappers;
 * duplicate site rows therefore retain the same invocation classification. */
static bool invocation_site(TSNode node) {
    if (kind_is(node, "call_expression") || kind_is(node, "new_expression"))
        return true;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        TSNode parent = ts_node_parent(node);
        if (ts_node_is_null(parent))
            return false;
        if (kind_is(parent, "call_expression"))
            return ts_node_eq(node, field(parent, "function"));
        if (kind_is(parent, "new_expression"))
            return ts_node_eq(node, field(parent, "constructor"));
        if (!kind_is(parent, "parenthesized_expression") && !kind_is(parent, "as_expression") &&
            !kind_is(parent, "satisfies_expression") && !kind_is(parent, "non_null_expression") &&
            !kind_is(parent, "type_assertion"))
            return false;
        node = parent;
    }
    return true;
}

static bool javascript_language(CBMLanguage language);

static bool http_method(TSNode member, cbm_resolve_binding_index_t *index) {
    const char *method = node_text(index, field(member, "property"));
    return method && (!strcmp(method, "get") || !strcmp(method, "post") || !strcmp(method, "put") ||
                      !strcmp(method, "delete") || !strcmp(method, "patch") ||
                      !strcmp(method, "head") || !strcmp(method, "options"));
}

/* A request is the sole non-escaping use admitted here. Even reading a
 * mutable subobject, passing the client as an argument, or calling an unknown
 * method invalidates the HTTP-only claim. Generic binding facts are unchanged. */
static bool http_escape(TSNode node, cbm_resolve_binding_index_t *index, bool factory) {
    if (!kind_is(node, "identifier") && !kind_is(node, "shorthand_property_identifier"))
        return false;
    TSNode member = ts_node_parent(node), call = ts_node_parent(member);
    const char *method = factory ? node_text(index, field(member, "property")) : NULL;
    if (method && !strcmp(method, "create") && kind_is(member, "member_expression") &&
        ts_node_eq(node, field(member, "object")) && kind_is(call, "call_expression") &&
        ts_node_eq(member, field(call, "function")))
        return false;
    return !kind_is(member, "member_expression") || !ts_node_eq(node, field(member, "object")) ||
           !http_method(member, index) || !kind_is(call, "call_expression") ||
           !ts_node_eq(member, field(call, "function")) ||
           ts_node_is_null(field(call, "arguments")) ||
           ts_node_named_child_count(field(call, "arguments")) != 1 ||
           !kind_is(ts_node_named_child(field(call, "arguments"), 0), "string");
}

static const char *http_call_scope(cbm_resolve_binding_index_t *index, TSNode node) {
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        node = ts_node_parent(node);
        if (ts_node_is_null(node))
            return NULL;
        if (kind_is(node, "program"))
            return index->result->module_qn;
        if (kind_is(node, "function_declaration")) {
            TSNode parent = ts_node_parent(node);
            if (kind_is(parent, "export_statement"))
                parent = ts_node_parent(parent);
            TSNode name = field(node, "name");
            if (!kind_is(parent, "program") || !kind_is(name, "identifier"))
                return NULL;
            const char *text = node_text(index, name);
            return text ? cbm_arena_sprintf(index->arena, "%s.%s", index->result->module_qn, text)
                        : NULL;
        }
        if (kind_is(node, "arrow_function") || kind_is(node, "function_expression") ||
            kind_is(node, "generator_function") ||
            kind_is(node, "generator_function_declaration") || kind_is(node, "method_definition") ||
            kind_is(node, "class_body"))
            return NULL;
    }
    return NULL;
}

static const char *http_call_url(cbm_resolve_binding_index_t *index, TSNode node) {
    if (!kind_is(node, "call_expression"))
        return NULL;
    TSNode args = field(node, "arguments");
    /* Additional request options may override baseURL or the HTTP method. */
    if (ts_node_is_null(args) || ts_node_named_child_count(args) != 1)
        return NULL;
    TSNode value = ts_node_named_child(args, 0);
    if (!kind_is(value, "string"))
        return NULL;
    const char *text = node_text(index, value);
    size_t length = text ? strlen(text) : 0;
    if (length < 2 || strchr(text, '\\'))
        return NULL;
    return cbm_arena_strndup(index->arena, text + 1, length - 2);
}

/* The extractor's invocation span is the whole opening tag. Only an exact
 * simple component identifier may use ordinary lexical binding proof;
 * member, namespace and intrinsic tag syntax needs a separate contract. */
static bool jsx_component_site(const cbm_resolve_binding_index_t *index, TSNode node) {
    if (!kind_is(node, "jsx_opening_element") && !kind_is(node, "jsx_self_closing_element"))
        return false;
    TSNode name = field(node, "name");
    if (!kind_is(name, "identifier"))
        return false;
    uint32_t start = ts_node_start_byte(name), end = ts_node_end_byte(name);
    return end > start && end <= index->source_len && index->source[start] >= 'A' &&
           index->source[start] <= 'Z';
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
    bool ecma = javascript_language(index->language);
    bool jsx = ecma && jsx_component_site(index, node);
    sites[index->site_count++] = (binding_site_t){
        .name = name,
        .start = ts_node_start_byte(node),
        .end = ts_node_end_byte(node),
        .scope = index->current_scope,
        .pending = index->current_pending,
        .invocation = jsx || invocation_site(node),
        .jsx_component = jsx,
        .this_owner = index->current_this_owner,
        .this_prototype = index->current_this_prototype,
        .http_escape = ecma && http_escape(node, index, false),
        .http_factory_escape = ecma && http_escape(node, index, true),
        .http_scope =
            ecma && kind_is(node, "call_expression") ? http_call_scope(index, node) : NULL,
        .http_url = ecma ? http_call_url(index, node) : NULL};
}

/* Python's direct-argument carrier retains transparent parentheses. Record
 * only those exact AST sites, with the identifier's lexical context. A tuple,
 * invocation result, attribute or other expression cannot borrow its name. */
static void note_python_argument_wrappers(cbm_resolve_binding_index_t *index, const char *name,
                                          TSNode identifier) {
    TSNode value = identifier;
    TSNode parent = ts_node_parent(value);
    unsigned wrappers = 0;
    while (kind_is(parent, "parenthesized_expression") && wrappers < BINDING_MAX_DEPTH) {
        if (ts_node_named_child_count(parent) != 1 ||
            !ts_node_eq(ts_node_named_child(parent, 0), value))
            return;
        wrappers++;
        value = parent;
        parent = ts_node_parent(value);
    }
    if (!wrappers || wrappers == BINDING_MAX_DEPTH || !kind_is(parent, "argument_list"))
        return;
    TSNode call = ts_node_parent(parent);
    if (!kind_is(call, "call") || !ts_node_eq(parent, field(call, "arguments")))
        return;
    value = identifier;
    for (unsigned i = 0; i < wrappers; i++) {
        value = ts_node_parent(value);
        add_site(index, name, value);
    }
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
                                         "escape_interpolation",
                                         "interpolation",
                                         "type_conversion",
                                         "format_specifier",
                                         "format_expression",
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
                                         "try_statement",
                                         "except_clause",
                                         "finally_clause",
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
                                         "relative_import",
                                         "import_prefix",
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

/* These grammar containers add no binders. The ordinary walk still audits
 * every child in the inherited evaluation/target context. In particular,
 * type_parameter is also used by PEP 695 declarations: only a generic_type
 * argument list is transparent, never a declaration's type_parameters field. */
static bool python_transparent_container(TSNode node, const char *parent) {
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "expression_list"))
        return true;
    if (!strcmp(kind, "generic_type"))
        return parent && !strcmp(parent, "type");
    if (!strcmp(kind, "type_parameter"))
        return parent && !strcmp(parent, "generic_type");
    if (!strcmp(kind, "keyword_separator") || !strcmp(kind, "positional_separator"))
        return parent && (!strcmp(parent, "parameters") || !strcmp(parent, "lambda_parameters")) &&
               ts_node_named_child_count(node) == 0;
    return false;
}

/* Formatting arbitrary values can invoke user-defined __format__, __str__
 * or __repr__. Only literal builtin values are effect-free here. The normal
 * tree walk still audits every nested interpolation and format expression;
 * no subtree is skipped and no identifier gains a type fact by spelling. */
static bool python_literal_format_value(TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 16)
        return false;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "integer") || !strcmp(kind, "float") || !strcmp(kind, "true") ||
        !strcmp(kind, "false") || !strcmp(kind, "none") || !strcmp(kind, "string") ||
        !strcmp(kind, "concatenated_string"))
        return true;
    if (!strcmp(kind, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return python_literal_format_value(ts_node_named_child(node, 0), depth + 1);
    if (!strcmp(kind, "unary_operator") && ts_node_named_child_count(node) == 1) {
        TSNode value = ts_node_named_child(node, 0), operation = field(node, "operator");
        return (kind_is(operation, "+") || kind_is(operation, "-")) &&
               (kind_is(value, "integer") || kind_is(value, "float"));
    }
    return false;
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

/* A no-value annotation has a lexical target but no runtime target read or
 * write. Only module simple names preserve a preceding runtime binding here;
 * function locals remain shadowing declarations and class scope stays closed. */
static bool python_annotation_only(TSNode node) {
    return kind_is(node, "assignment") && !ts_node_is_null(field(node, "type")) &&
           ts_node_is_null(field(node, "right"));
}

static bool python_inert_annotation(TSNode node) {
    if (kind_is(node, "type") && ts_node_named_child_count(node) == 1)
        node = ts_node_named_child(node, 0);
    /* A bare name is a value read. Calls, descriptors, subscriptions and
     * postponed/deferred expression semantics need separate effect proofs. */
    return kind_is(node, "identifier");
}

static bool dynamic_name(CBMLanguage language, const char *name) {
    static const char *const python[] = {"exec",    "eval",    "globals",    "locals",   "vars",
                                         "setattr", "delattr", "__import__", "__dict__", NULL};
    static const char *const lua[] = {"load",    "loadfile", "dofile", "loadstring",
                                      "setfenv", "getfenv",  "debug",  "_ENV",
                                      "_G",      "rawset",   NULL};
    /* Lua environment operations have no special meaning in Python. Keep
     * Python's existing dynamic mechanisms and the full conservative Lua
     * barrier; this change does not expand the Lua proof domain. */
    return listed(name, python) || (language == CBM_LANG_LUA && listed(name, lua));
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
    uint32_t header_scope;
    const binding_pending_declaration_t *pending;
    /* ECMAScript: how an identifier here binds, what a declarator under this
     * statement declares, and whether the next block is a function body. */
    unsigned char js_binder, js_declaration, js_for_binder;
    bool js_type_level, js_ambient, js_body, js_no_site, js_this_prototype;
    const char *js_this_owner;
} binding_walk_t;

/* Function defaults/annotations and class bases execute in the enclosing
 * namespace. Parameter names still bind in the body scope. Keep declarations
 * under construction unavailable to these eager reads, including a class
 * name while its methods' defaults are being evaluated. */
static binding_walk_t python_child_context(const char *parent, const char *field_name,
                                           binding_walk_t state) {
    if (!parent || !field_name)
        return state;
    bool function = !strcmp(parent, "function_definition") || !strcmp(parent, "lambda");
    if (function && !strcmp(field_name, "body")) {
        state.pending = NULL;
    } else if ((function && !strcmp(field_name, "return_type")) ||
               (!strcmp(parent, "class_definition") && !strcmp(field_name, "superclasses")) ||
               ((!strcmp(parent, "default_parameter") ||
                 !strcmp(parent, "typed_default_parameter")) &&
                !strcmp(field_name, "value")) ||
               ((!strcmp(parent, "typed_parameter") ||
                 !strcmp(parent, "typed_default_parameter")) &&
                !strcmp(field_name, "type"))) {
        state.scope = state.header_scope;
    }
    return state;
}

static const binding_pending_declaration_t *python_pending_declaration(
    cbm_resolve_binding_index_t *index, TSNode node, uint32_t scope,
    const binding_pending_declaration_t *parent) {
    if (kind_is(node, "lambda"))
        return parent;
    const char *name = node_text(index, field(node, "name"));
    binding_pending_declaration_t *pending = cbm_arena_alloc(index->arena, sizeof(*pending));
    if (!name || !pending) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_declaration_allocation");
        return parent;
    }
    *pending = (binding_pending_declaration_t){name, scope, parent};
    return pending;
}

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
    scopes[id] =
        (binding_scope_t){.qualified_name = qn, .parent = parent, .class_scope = class_scope};
    return id;
}

static bool javascript_language(CBMLanguage language) {
    return language == CBM_LANG_JAVASCRIPT || language == CBM_LANG_TYPESCRIPT ||
           language == CBM_LANG_TSX || language == CBM_LANG_ARKTS;
}

static bool supported_javascript_node(const char *kind) {
    static const char *const syntax[] = {
        "program", "function_declaration", "generator_function_declaration", "function_expression",
        "generator_function", "arrow_function", "formal_parameters", "identifier",
        "statement_block", "expression_statement", "return_statement", "call_expression",
        "arguments", "number", "string", "string_fragment", "escape_sequence", "comment",
        "member_expression", "property_identifier", "binary_expression", "unary_expression",
        "ternary_expression", "true", "false", "null", "undefined", "parenthesized_expression",
        "array", "object", "pair", "shorthand_property_identifier", "subscript_expression",
        "if_statement", "else_clause", "empty_statement", "break_statement", "continue_statement",
        "while_statement", "do_statement", "for_statement", "for_in_statement", "throw_statement",
        "try_statement", "catch_clause", "finally_clause", "switch_statement", "switch_body",
        "switch_case", "switch_default", "await_expression", "yield_expression",
        "sequence_expression", "lexical_declaration", "variable_declaration", "variable_declarator",
        "assignment_expression", "augmented_assignment_expression", "update_expression",
        "assignment_pattern", "array_pattern", "object_pattern", "pair_pattern", "rest_pattern",
        "shorthand_property_identifier_pattern", "object_assignment_pattern", "spread_element",
        "required_parameter", "optional_parameter", "type_annotation", "predefined_type",
        "type_identifier", "generic_type", "type_arguments", "nested_type_identifier", "array_type",
        "union_type", "intersection_type", "parenthesized_type", "literal_type", "tuple_type",
        "optional_type", "rest_type", "function_type", "type_query", "keyof_type", "lookup_type",
        "conditional_type", "infer_type", "type_predicate", "type_predicate_annotation",
        "asserts_annotation", "asserts", "type_parameters", "type_parameter", "constraint",
        "default_type", "object_type", "property_signature", "method_signature", "call_signature",
        "construct_signature", "index_signature", "mapped_type_clause", "index_type_query",
        "readonly_type", "template_literal_type", "template_string", "template_substitution",
        "regex", "regex_pattern", "regex_flags", "non_null_expression", "as_expression",
        "satisfies_expression", "type_assertion", "new_expression", "this", "super",
        "class_declaration", "abstract_class_declaration", "class", "class_body", "class_heritage",
        "extends_clause", "implements_clause", "method_definition", "public_field_definition",
        "field_definition", "private_property_identifier", "accessibility_modifier", "static_block",
        "interface_declaration", "extends_type_clause", "interface_body", "type_alias_declaration",
        "enum_declaration", "enum_body", "enum_assignment", "computed_property_name",
        "import_statement", "import_clause", "named_imports", "import_specifier",
        "namespace_import", "export_statement", "export_clause", "export_specifier",
        "namespace_export", "meta_property", "jsx_element", "jsx_opening_element",
        "jsx_closing_element", "jsx_self_closing_element", "jsx_attribute", "jsx_expression",
        "jsx_text", "jsx_namespace_name", "jsx_fragment",
        /* Tokens and statements that bind nothing. */
        "optional_chain", "hash_bang_line", "html_comment", "html_character_reference",
        "debugger_statement", "labeled_statement", "statement_identifier", "import",
        "import_attribute", "instantiation_expression", "decorator", "class_static_block",
        "using_declaration",
        /* Type-level forms have no runtime binder. */
        "function_signature", "abstract_method_signature", "adding_type_annotation",
        "opting_type_annotation", "omitting_type_annotation", "constructor_type",
        "existential_type", "flow_maybe_type", "template_type", "this_type", "override_modifier",
        /* Scopes and module forms with an explicit rule below. */
        "ambient_declaration", "internal_module", "module", "nested_identifier", "import_alias",
        "import_require_clause", NULL};
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

static const char *http_module_literal(cbm_resolve_binding_index_t *index, TSNode node) {
    const char *text = kind_is(node, "string") ? node_text(index, node) : NULL;
    size_t length = text ? strlen(text) : 0;
    return length >= 2 && !strchr(text, '\\')
               ? cbm_arena_strndup(index->arena, text + 1, length - 2)
               : NULL;
}

static void http_dependency(cbm_resolve_binding_index_t *index, const char *path, const char *local,
                            bool side_effect) {
    if (!path || !*path) {
        index->http_domain_unknown = true;
        return;
    }
    /* These packages expose loaders/evaluators whose aliases need a separate
     * capability analysis. They can reach any client in this repository. */
    if (!strcmp(path, "module") || !strcmp(path, "node:module") || !strcmp(path, "vm") ||
        !strcmp(path, "node:vm"))
        index->http_domain_unknown = true;
    cbm_resolve_http_dependency_t *items =
        grow_array(index, index->http_dependencies, index->http_dependency_count,
                   &index->http_dependency_capacity, sizeof(*items));
    if (!items)
        return;
    index->http_dependencies = items;
    items[index->http_dependency_count++] =
        (cbm_resolve_http_dependency_t){path, local, side_effect};
}

static void note_http_dependency(cbm_resolve_binding_index_t *index, TSNode node) {
    /* Computed/reflection access can hide loader and evaluator aliases.
     * Until that capability flow is audited, the repository domain is open. */
    if (kind_is(node, "subscript_expression"))
        index->http_domain_unknown = true;
    if (kind_is(node, "identifier") || kind_is(node, "property_identifier")) {
        const char *capability = node_text(index, node);
        if (capability &&
            (!strcmp(capability, "globalThis") || !strcmp(capability, "global") ||
             !strcmp(capability, "window") || !strcmp(capability, "self") ||
             !strcmp(capability, "Reflect") || !strcmp(capability, "eval") ||
             !strcmp(capability, "Function") || !strcmp(capability, "mainModule") ||
             !strcmp(capability, "setTimeout") || !strcmp(capability, "setInterval") ||
             (!strcmp(capability, "constructor") &&
              kind_is(ts_node_parent(node), "member_expression"))))
            index->http_domain_unknown = true;
    }
    if (kind_is(node, "import_statement") || kind_is(node, "export_statement")) {
        TSNode source = field(node, "source");
        if (ts_node_is_null(source) || child_token(node, "type"))
            return;
        const char *local = NULL;
        bool side_effect = kind_is(node, "import_statement");
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode clause = ts_node_named_child(node, i);
            if (!kind_is(clause, "import_clause"))
                continue;
            side_effect = false;
            if (ts_node_named_child_count(clause) == 1 &&
                kind_is(ts_node_named_child(clause, 0), "identifier"))
                local = node_text(index, ts_node_named_child(clause, 0));
        }
        http_dependency(index, http_module_literal(index, source), local, side_effect);
    }
    if (kind_is(node, "call_expression")) {
        TSNode function = field(node, "function");
        const char *text = node_text(index, function);
        bool loader = kind_is(function, "import") ||
                      (kind_is(function, "identifier") && text && !strcmp(text, "require"));
        if (loader) {
            TSNode args = field(node, "arguments");
            const char *path = !ts_node_is_null(args) && ts_node_named_child_count(args) == 1
                                   ? http_module_literal(index, ts_node_named_child(args, 0))
                                   : NULL;
            http_dependency(index, path, NULL, false);
        }
    }
    if (kind_is(node, "identifier") || kind_is(node, "property_identifier")) {
        const char *text = node_text(index, node);
        if (!text || strcmp(text, "require"))
            return;
        TSNode parent = ts_node_parent(node);
        if (!kind_is(node, "identifier") || !kind_is(parent, "call_expression") ||
            !ts_node_eq(node, field(parent, "function")))
            index->http_domain_unknown = true;
    }
}

/* The export name carrying a CommonJS whole-module value. It cannot collide
 * with a source export name: those are identifiers or string literals that the
 * lookup never spells with this exact dotted form. */
static const char JS_WHOLE_EXPORT_NAME[] = "module.exports";

static js_raw_export_t *js_raw_export(cbm_resolve_binding_index_t *index) {
    js_raw_export_t *items = grow_array(index, index->js_raw_exports, index->js_raw_export_count,
                                        &index->js_raw_export_capacity, sizeof(*items));
    if (!items)
        return NULL;
    index->js_raw_exports = items;
    js_raw_export_t *raw = &items[index->js_raw_export_count++];
    *raw = (js_raw_export_t){.kind = CBM_RESOLVE_EXPORT_VALUE | CBM_RESOLVE_EXPORT_TYPE};
    return raw;
}

static void js_write(cbm_resolve_binding_index_t *index, const char *name, uint32_t scope) {
    js_write_t *items = grow_array(index, index->js_writes, index->js_write_count,
                                   &index->js_write_capacity, sizeof(*items));
    if (!items || !name)
        return;
    index->js_writes = items;
    items[index->js_write_count++] = (js_write_t){name, scope};
}

static void js_export_alias(cbm_resolve_binding_index_t *index, const char *name) {
    const char **items = grow_array(index, index->js_export_aliases, index->js_export_alias_count,
                                    &index->js_export_alias_capacity, sizeof(*items));
    if (!items || !name)
        return;
    index->js_export_aliases = items;
    items[index->js_export_alias_count++] = name;
}

static const cbm_resolve_js_import_t *js_import_fact(cbm_resolve_binding_index_t *index,
                                                     cbm_resolve_js_import_t fact) {
    cbm_resolve_js_import_t *copy = cbm_arena_alloc(index->arena, sizeof(*copy));
    if (!copy) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_import_allocation");
        return NULL;
    }
    *copy = fact;
    return copy;
}

static void note_javascript_import(cbm_resolve_binding_index_t *index, TSNode node,
                                   const char *parent_kind, bool type_only) {
    TSNode name = {0};
    TSNode member = {0};
    TSNode statement = ts_node_parent(node);
    bool is_default = false;
    bool is_require = false;
    if (kind_is(node, "import_specifier")) {
        name = field(node, "alias");
        member = field(node, "name");
        if (ts_node_is_null(name)) {
            name = member;
        }
        type_only |= child_token(node, "type");
    } else if (kind_is(node, "namespace_import")) {
        name = ts_node_named_child(node, 0);
    } else if (kind_is(node, "identifier") && parent_kind &&
               strcmp(parent_kind, "import_clause") == 0) {
        name = node;
        is_default = true;
    } else if (kind_is(node, "import_require_clause")) {
        name = ts_node_named_child(node, 0);
        is_require = true;
        statement = node;
    }
    if (ts_node_is_null(name))
        return;
    if (!kind_is(name, "identifier")) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "unsupported_import_binding");
        return;
    }
    if (!is_require)
        while (!ts_node_is_null(statement) && !kind_is(statement, "import_statement"))
            statement = ts_node_parent(statement);
    const char *local = node_text(index, name);
    size_t before = index->name_count;
    add_name(index, local, 0, type_only ? BINDING_BLOCKED | BINDING_TYPE_IMPORT : BINDING_IMPORT);
    if (index->name_count == before)
        return;
    const char *member_name = ts_node_is_null(member) ? NULL : node_text(index, member);
    if (member_name && !strcmp(member_name, "default")) {
        member_name = NULL;
        is_default = true;
    }
    index->names[index->name_count - 1].js_import = js_import_fact(
        index,
        (cbm_resolve_js_import_t){.local = local,
                                  .source = http_module_literal(index, field(statement, "source")),
                                  .member = member_name,
                                  .is_default = is_default,
                                  .is_require = is_require,
                                  .type_only = type_only});
    /* Retain the actual default import's module literal for the HTTP
     * domain. A mutable extraction carrier cannot replace that spelling. */
    if (!type_only && kind_is(node, "identifier") && parent_kind &&
        !strcmp(parent_kind, "import_clause") && index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE) {
        TSNode source = field(statement, "source");
        const char *text = kind_is(statement, "import_statement") && kind_is(source, "string")
                               ? node_text(index, source)
                               : NULL;
        size_t length = text ? strlen(text) : 0;
        if (length >= 2 && !strchr(text, '\\'))
            index->names[index->name_count - 1].http_import_path =
                cbm_arena_strndup(index->arena, text + 1, length - 2);
    }
}

static TSNode js_unwrap_value(TSNode value) {
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH && !ts_node_is_null(value); depth++) {
        if (kind_is(value, "assignment_expression"))
            value = field(value, "right");
        else if (kind_is(value, "parenthesized_expression") &&
                 ts_node_named_child_count(value) == 1)
            value = ts_node_named_child(value, 0);
        else
            break;
    }
    return value;
}

static bool js_identifier_is(cbm_resolve_binding_index_t *index, TSNode node, const char *name) {
    if (!kind_is(node, "identifier"))
        return false;
    size_t length = strlen(name);
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end - start == length && end <= index->source_len &&
           !memcmp(index->source + start, name, length);
}

static bool js_is_module_exports(cbm_resolve_binding_index_t *index, TSNode node) {
    if (!kind_is(node, "member_expression") ||
        !js_identifier_is(index, field(node, "object"), "module"))
        return false;
    TSNode property = field(node, "property");
    uint32_t start = ts_node_start_byte(property), end = ts_node_end_byte(property);
    return kind_is(property, "property_identifier") && end - start == 7 &&
           !memcmp(index->source + start, "exports", 7);
}

static bool js_require_call(cbm_resolve_binding_index_t *index, TSNode call, const char **source) {
    if (!kind_is(call, "call_expression") ||
        !js_identifier_is(index, field(call, "function"), "require"))
        return false;
    TSNode args = field(call, "arguments");
    if (ts_node_is_null(args) || ts_node_named_child_count(args) != 1)
        return false;
    TSNode literal = ts_node_named_child(args, 0);
    if (!kind_is(literal, "string"))
        return false;
    *source = http_module_literal(index, literal);
    return *source != NULL;
}

/* `require("m")` or `require("m").member`. */
static bool js_require_value(cbm_resolve_binding_index_t *index, TSNode value, const char **source,
                             const char **member) {
    value = js_unwrap_value(value);
    *member = NULL;
    if (js_require_call(index, value, source))
        return true;
    if (!kind_is(value, "member_expression"))
        return false;
    TSNode property = field(value, "property");
    if (!kind_is(property, "property_identifier") ||
        !js_require_call(index, field(value, "object"), source))
        return false;
    *member = node_text(index, property);
    return *member != NULL;
}

static bool js_chain_writes_module_exports(cbm_resolve_binding_index_t *index, TSNode value) {
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH && !ts_node_is_null(value); depth++) {
        if (kind_is(value, "parenthesized_expression") && ts_node_named_child_count(value) == 1) {
            value = ts_node_named_child(value, 0);
            continue;
        }
        if (!kind_is(value, "assignment_expression"))
            return false;
        if (js_is_module_exports(index, field(value, "left")))
            return true;
        value = field(value, "right");
    }
    return false;
}

static bool js_function_value(TSNode value) {
    return kind_is(value, "arrow_function") || kind_is(value, "function_expression") ||
           kind_is(value, "generator_function");
}

/* Classify one exported value. A function needs an extracted definition at
 * these exact lines; nothing else is taken on trust from its spelling. */
static void js_fill_value(cbm_resolve_binding_index_t *index, js_raw_export_t *raw, TSNode value,
                          const char *fallback_name) {
    value = js_unwrap_value(value);
    const char *source = NULL, *member = NULL;
    if (kind_is(value, "identifier")) {
        raw->value = JS_VALUE_IDENTIFIER;
        raw->local = node_text(index, value);
    } else if (kind_is(value, "class") && !ts_node_is_null(field(value, "name"))) {
        raw->value = JS_VALUE_FUNCTION;
        raw->function_name = node_text(index, field(value, "name"));
        raw->start_line = ts_node_start_point(value).row + 1;
        raw->end_line = ts_node_end_point(value).row + 1;
    } else if (js_function_value(value) || kind_is(value, "method_definition")) {
        TSNode name = field(value, "name");
        raw->value = JS_VALUE_FUNCTION;
        raw->function_name = kind_is(value, "method_definition") ? node_text(index, name)
                             : !ts_node_is_null(name)            ? node_text(index, name)
                                                                 : fallback_name;
        raw->start_line = ts_node_start_point(value).row + 1;
        raw->end_line = ts_node_end_point(value).row + 1;
    } else if (js_require_value(index, value, &source, &member)) {
        raw->value = JS_VALUE_REQUIRE;
        raw->source = source;
        raw->member = member;
    } else if (kind_is(value, "object")) {
        raw->value = JS_VALUE_OBJECT;
    }
}

static void js_export_object_members(cbm_resolve_binding_index_t *index, TSNode object) {
    uint32_t count = ts_node_named_child_count(object);
    for (uint32_t i = 0; i < count; i++) {
        TSNode item = ts_node_named_child(object, i);
        if (kind_is(item, "comment"))
            continue;
        if (kind_is(item, "shorthand_property_identifier")) {
            const char *name = node_text(index, item);
            js_raw_export_t *raw = js_raw_export(index);
            if (raw) {
                raw->export_name = name;
                raw->value = JS_VALUE_IDENTIFIER;
                raw->local = name;
            }
        } else if (kind_is(item, "pair") || kind_is(item, "method_definition")) {
            TSNode key = kind_is(item, "pair") ? field(item, "key") : field(item, "name");
            if (!kind_is(key, "property_identifier")) {
                index->js_exports_open = true;
                continue;
            }
            const char *name = node_text(index, key);
            js_raw_export_t *raw = js_raw_export(index);
            if (raw) {
                raw->export_name = name;
                js_fill_value(index, raw, kind_is(item, "pair") ? field(item, "value") : item,
                              name);
                /* A nested object has no declaration of its own. */
                if (raw->value == JS_VALUE_OBJECT)
                    raw->value = JS_VALUE_UNKNOWN;
            }
        } else {
            index->js_exports_open = true; /* spread or computed member */
        }
    }
}

static js_member_t *js_member_row(cbm_resolve_binding_index_t *index, const char *owner,
                                  const char *member, bool prototype) {
    js_member_t *items = grow_array(index, index->js_members, index->js_member_count,
                                    &index->js_member_capacity, sizeof(*items));
    if (!items || !owner || !member)
        return NULL;
    index->js_members = items;
    js_member_t *row = &items[index->js_member_count++];
    *row = (js_member_t){.owner = owner, .member = member, .prototype = prototype};
    return row;
}

/* The function or identifier a member is given. Anything else is not a declaration
 * the member could name, so the row stays a known conflict for that name. */
static void js_member_value(cbm_resolve_binding_index_t *index, js_member_t *row, TSNode value,
                            const char *fallback_name) {
    value = js_unwrap_value(value);
    if (kind_is(value, "identifier")) {
        row->local = node_text(index, value);
    } else if (js_function_value(value) || kind_is(value, "method_definition")) {
        TSNode name = field(value, "name");
        row->function_name = !ts_node_is_null(name) ? node_text(index, name) : fallback_name;
        row->start_line = ts_node_start_point(value).row + 1;
        row->end_line = ts_node_end_point(value).row + 1;
    } else {
        row->conflict = true;
    }
}

static void note_javascript_object_members(cbm_resolve_binding_index_t *index, const char *owner,
                                           TSNode object, bool prototype) {
    uint32_t count = ts_node_named_child_count(object);
    for (uint32_t i = 0; i < count; i++) {
        TSNode item = ts_node_named_child(object, i);
        const char *member = NULL;
        js_member_t *row = NULL;
        if (kind_is(item, "spread_element")) {
            (void)js_member_row(index, owner, JS_MEMBER_COMPUTED, prototype);
        } else if (kind_is(item, "shorthand_property_identifier")) {
            member = node_text(index, item);
            row = js_member_row(index, owner, member, prototype);
            if (row)
                row->local = member;
        } else if (kind_is(item, "pair") || kind_is(item, "method_definition")) {
            TSNode key = kind_is(item, "pair") ? field(item, "key") : field(item, "name");
            if (!kind_is(key, "property_identifier")) {
                (void)js_member_row(index, owner, JS_MEMBER_COMPUTED, prototype);
                continue;
            }
            member = node_text(index, key);
            row = js_member_row(index, owner, member, prototype);
            if (row)
                js_member_value(index, row, kind_is(item, "pair") ? field(item, "value") : item,
                                member);
        }
    }
}

/* `this` inside `Owner.member = function` and friends. An arrow keeps the `this`
 * around it; any other function starts over unless a member assignment names it. */
static void javascript_this_owner(cbm_resolve_binding_index_t *index, TSNode node,
                                  binding_walk_t *state) {
    if (kind_is(node, "arrow_function"))
        return;
    state->js_this_owner = NULL;
    state->js_this_prototype = false;
    TSNode parent = ts_node_parent(node);
    TSNode object = (TSNode){0};
    if (kind_is(node, "function_declaration")) {
        /* A constructor function's `this` is its instance. */
        if (state->scope == 0) {
            state->js_this_owner = node_text(index, field(node, "name"));
            state->js_this_prototype = true;
        }
        return;
    }
    while (kind_is(parent, "assignment_expression")) {
        object = field(parent, "left");
        parent = ts_node_parent(parent);
    }
    if (kind_is(object, "member_expression")) {
        TSNode owner = field(object, "object");
        if (kind_is(owner, "identifier")) {
            state->js_this_owner = node_text(index, owner);
            return;
        }
        if (kind_is(owner, "member_expression") && kind_is(field(owner, "object"), "identifier") &&
            kind_is(field(owner, "property"), "property_identifier")) {
            const char *property = node_text(index, field(owner, "property"));
            if (property && !strcmp(property, "prototype")) {
                state->js_this_owner = node_text(index, field(owner, "object"));
                state->js_this_prototype = true;
            }
        }
        return;
    }
    /* A member of `const Owner = {...}` or `Owner.prototype = {...}`. */
    TSNode literal =
        kind_is(node, "method_definition") ? ts_node_parent(node) : ts_node_parent(parent);
    if (!kind_is(literal, "object") ||
        (!kind_is(node, "method_definition") && !kind_is(parent, "pair")))
        return;
    TSNode holder = ts_node_parent(literal);
    if (kind_is(holder, "variable_declarator") && ts_node_eq(literal, field(holder, "value")) &&
        kind_is(field(holder, "name"), "identifier")) {
        state->js_this_owner = node_text(index, field(holder, "name"));
    } else if (kind_is(holder, "assignment_expression") &&
               ts_node_eq(literal, field(holder, "right"))) {
        TSNode left = field(holder, "left");
        if (kind_is(left, "member_expression") && kind_is(field(left, "object"), "identifier") &&
            kind_is(field(left, "property"), "property_identifier")) {
            const char *property = node_text(index, field(left, "property"));
            if (property && !strcmp(property, "prototype")) {
                state->js_this_owner = node_text(index, field(left, "object"));
                state->js_this_prototype = true;
            }
        }
    }
}

/* `owner[key] = v` and `owner.prototype[key] = v` can replace any member. */
static void note_javascript_computed_write(cbm_resolve_binding_index_t *index, TSNode left,
                                           const binding_walk_t *state) {
    TSNode object = field(left, "object");
    const char *owner = NULL;
    bool prototype = false;
    if (kind_is(object, "this")) {
        owner = state->js_this_owner;
        prototype = state->js_this_prototype;
    } else if (kind_is(object, "identifier")) {
        owner = node_text(index, object);
    } else if (kind_is(object, "member_expression") &&
               kind_is(field(object, "object"), "identifier") &&
               kind_is(field(object, "property"), "property_identifier")) {
        const char *middle = node_text(index, field(object, "property"));
        if (middle && !strcmp(middle, "prototype")) {
            owner = node_text(index, field(object, "object"));
            prototype = true;
        }
    }
    if (owner)
        (void)js_member_row(index, owner, JS_MEMBER_COMPUTED, prototype);
}

/* Writes that define or replace a member. Only a module-level assignment defines
 * one; `this.x = v` or an assignment inside a function just makes x unknowable. */
static void note_javascript_member_write(cbm_resolve_binding_index_t *index, TSNode left,
                                         TSNode right, const binding_walk_t *state) {
    TSNode object = field(left, "object");
    TSNode property = field(left, "property");
    if (!kind_is(property, "property_identifier"))
        return;
    const char *member = node_text(index, property);
    if (!member)
        return;
    if (kind_is(object, "this")) {
        if (state->js_this_owner) {
            js_member_t *row =
                js_member_row(index, state->js_this_owner, member, state->js_this_prototype);
            if (row)
                row->conflict = true;
        }
        return;
    }
    const char *owner = NULL;
    bool prototype = false;
    if (kind_is(object, "identifier")) {
        owner = node_text(index, object);
    } else if (kind_is(object, "member_expression") &&
               kind_is(field(object, "object"), "identifier") &&
               kind_is(field(object, "property"), "property_identifier")) {
        const char *middle = node_text(index, field(object, "property"));
        if (middle && !strcmp(middle, "prototype")) {
            owner = node_text(index, field(object, "object"));
            prototype = true;
        }
    }
    /* `Owner.prototype = {...}` itself. Any other value replaces every member. */
    if (owner && !prototype && !strcmp(member, "prototype")) {
        TSNode value = js_unwrap_value(right);
        if (kind_is(value, "object") && !state->scope)
            note_javascript_object_members(index, owner, value, true);
        else
            (void)js_member_row(index, owner, JS_MEMBER_COMPUTED, true);
        return;
    }
    js_member_t *row = owner ? js_member_row(index, owner, member, prototype) : NULL;
    if (!row)
        return;
    if (state->scope)
        row->conflict = true;
    else
        js_member_value(index, row, right, member);
}

static void note_javascript_commonjs(cbm_resolve_binding_index_t *index, TSNode node,
                                     const binding_walk_t *state) {
    const char *ignored = NULL;
    if (js_is_module_exports(index, node)) {
        index->javascript_module = true;
        index->js_commonjs = true;
    } else if (js_require_call(index, node, &ignored)) {
        index->javascript_module = true;
    }
    if (!kind_is(node, "assignment_expression"))
        return;
    TSNode left = field(node, "left"), right = field(node, "right");
    if (js_identifier_is(index, left, "exports")) {
        index->js_exports_rebound |= js_chain_writes_module_exports(index, right);
        return;
    }
    if (kind_is(left, "subscript_expression"))
        note_javascript_computed_write(index, left, state);
    if (kind_is(left, "subscript_expression") &&
        (js_identifier_is(index, field(left, "object"), "exports") ||
         js_is_module_exports(index, field(left, "object")))) {
        index->js_exports_open = true;
        return;
    }
    if (!kind_is(left, "member_expression"))
        return;
    TSNode object = field(left, "object"), property = field(left, "property");
    bool whole = js_is_module_exports(index, left);
    bool bare = js_identifier_is(index, object, "exports");
    bool member = js_is_module_exports(index, object);
    if (bare) {
        index->javascript_module = true;
        index->js_commonjs = true;
    }
    note_javascript_member_write(index, left, right, state);
    /* A value assigned inside a function may or may not run. */
    if (state->scope) {
        index->js_exports_open |= whole || bare || member;
        return;
    }
    if (whole) {
        js_raw_export_t *raw = js_raw_export(index);
        if (!raw)
            return;
        raw->export_name = JS_WHOLE_EXPORT_NAME;
        raw->whole = true;
        js_fill_value(index, raw, right, NULL);
        index->js_whole_export = true;
        if (raw->value == JS_VALUE_OBJECT)
            js_export_object_members(index, js_unwrap_value(right));
        else if (raw->value == JS_VALUE_IDENTIFIER)
            js_export_alias(index, raw->local);
        return;
    }
    if (!kind_is(property, "property_identifier"))
        return;
    const char *owner = bare || member                  ? NULL
                        : kind_is(object, "identifier") ? node_text(index, object)
                                                        : NULL;
    if (!bare && !member && !owner)
        return;
    const char *name = node_text(index, property);
    js_raw_export_t *raw = js_raw_export(index);
    if (!raw)
        return;
    raw->export_name = name;
    raw->owner = owner;
    raw->bare_exports = bare;
    js_fill_value(index, raw, right, name);
    if (raw->value == JS_VALUE_OBJECT)
        raw->value = JS_VALUE_UNKNOWN;
}

static void note_javascript_export(cbm_resolve_binding_index_t *index, TSNode node,
                                   bool module_level) {
    /* The exported-name -> target map is built from these raw facts once every
     * declaration is known. The QN summary still certifies only same-named
     * local exports: an alias or re-export has no declaration of that QN. */
    if (!kind_is(node, "export_statement"))
        return;
    TSNode source_node = field(node, "source");
    bool local_export = ts_node_is_null(source_node);
    const char *source = local_export ? NULL : http_module_literal(index, source_node);
    bool type_export = child_token(node, "type");
    unsigned kind =
        type_export ? CBM_RESOLVE_EXPORT_TYPE : CBM_RESOLVE_EXPORT_VALUE | CBM_RESOLVE_EXPORT_TYPE;
    if (child_token(node, "default")) {
        js_raw_export_t *raw = module_level ? js_raw_export(index) : NULL;
        if (!raw)
            return;
        raw->export_name = "default";
        raw->kind = kind;
        TSNode declaration = field(node, "declaration");
        TSNode name = field(declaration, "name");
        if (!ts_node_is_null(declaration) && !ts_node_is_null(name)) {
            raw->value = JS_VALUE_IDENTIFIER;
            raw->local = node_text(index, name);
        } else {
            js_fill_value(index, raw, field(node, "value"), NULL);
            if (raw->value == JS_VALUE_OBJECT)
                raw->value = JS_VALUE_UNKNOWN;
        }
        return;
    }
    if (child_token(node, "=")) {
        js_raw_export_t *raw = module_level ? js_raw_export(index) : NULL;
        if (!raw)
            return;
        raw->export_name = JS_WHOLE_EXPORT_NAME;
        raw->whole = true;
        js_fill_value(index, raw, ts_node_named_child(node, 0), NULL);
        index->js_whole_export = true;
        if (raw->value == JS_VALUE_OBJECT)
            js_export_object_members(index, js_unwrap_value(ts_node_named_child(node, 0)));
        return;
    }
    if (!local_export && child_token(node, "*")) {
        js_raw_export_t *raw = module_level ? js_raw_export(index) : NULL;
        if (raw) {
            raw->kind = kind;
            raw->source = source;
            raw->value = JS_VALUE_REQUIRE;
        }
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode clause = ts_node_named_child(node, i);
        if (kind_is(clause, "namespace_export") && module_level) {
            js_raw_export_t *raw = js_raw_export(index);
            if (raw) {
                raw->export_name = node_text(index, ts_node_named_child(clause, 0));
                raw->kind = kind;
                raw->source = source;
                raw->value = JS_VALUE_REQUIRE;
            }
            continue;
        }
        if (!kind_is(clause, "export_clause")) {
            continue;
        }
        uint32_t specifiers = ts_node_named_child_count(clause);
        for (uint32_t s = 0; s < specifiers; s++) {
            TSNode specifier = ts_node_named_child(clause, s);
            if (!kind_is(specifier, "export_specifier"))
                continue;
            TSNode local = field(specifier, "name");
            TSNode alias = field(specifier, "alias");
            const char *local_name = node_text(index, local);
            const char *alias_name = ts_node_is_null(alias) ? local_name : node_text(index, alias);
            if (local_name && alias_name) {
                bool type_specifier = type_export || child_token(specifier, "type");
                if (local_export && kind_is(local, "identifier") &&
                    strcmp(local_name, alias_name) == 0) {
                    add_name(index, local_name, 0,
                             type_specifier ? BINDING_TYPE_EXPORT_MARKER : BINDING_EXPORT_MARKER);
                } else {
                    add_name(index, alias_name, 0, BINDING_EXPORT_CONFLICT);
                }
                js_raw_export_t *raw = module_level ? js_raw_export(index) : NULL;
                if (raw) {
                    raw->export_name = alias_name;
                    raw->kind = type_specifier ? CBM_RESOLVE_EXPORT_TYPE : kind;
                    if (local_export) {
                        raw->value = JS_VALUE_IDENTIFIER;
                        raw->local = local_name;
                    } else {
                        raw->value = JS_VALUE_REQUIRE;
                        raw->source = source;
                        raw->member = local_name;
                    }
                }
            }
        }
    }
}

static bool javascript_runtime_function(TSNode node) {
    const char *kind = ts_node_type(node);
    return javascript_function_kind(kind) || !strcmp(kind, "function_expression") ||
           !strcmp(kind, "generator_function") || !strcmp(kind, "arrow_function") ||
           !strcmp(kind, "method_definition");
}

static uint32_t javascript_new_scope(cbm_resolve_binding_index_t *index, const char *qualified_name,
                                     uint32_t parent, bool function_scope, bool namespace_scope) {
    binding_scope_t *scopes = grow_array(index, index->scopes, index->scope_count,
                                         &index->scope_capacity, sizeof(*scopes));
    if (!scopes) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "javascript_scope_allocation");
        return parent;
    }
    index->scopes = scopes;
    uint32_t scope = (uint32_t)index->scope_count++;
    scopes[scope] = (binding_scope_t){.qualified_name = qualified_name,
                                      .parent = parent,
                                      .function_scope = function_scope,
                                      .namespace_scope = namespace_scope};
    return scope;
}

/* A var hoists to here. Scope 0 is the module's own function scope. */
static uint32_t javascript_function_scope(const cbm_resolve_binding_index_t *index,
                                          uint32_t scope) {
    while (scope && scope < index->scope_count && !index->scopes[scope].function_scope)
        scope = index->scopes[scope].parent;
    return scope;
}

/* How the child in this grammar position binds. Only a pattern position
 * passes a binder down: a default value, a computed key or `a.b = 1` reads its
 * identifiers and rebinds nothing. */
static binding_walk_t javascript_child_context(const char *parent, const char *field_name,
                                               binding_walk_t state) {
    if (!parent)
        return state;
    unsigned char inherited = state.js_binder;
    unsigned char binder = JS_BINDER_NONE;
    bool name = field_name && !strcmp(field_name, "name");
    bool left = field_name && !strcmp(field_name, "left");
    bool type = field_name && !strcmp(field_name, "type");
    if (!strcmp(parent, "variable_declarator")) {
        if (name)
            binder = state.js_declaration;
        else
            state.named_export = false;
    } else if (!strcmp(parent, "for_in_statement")) {
        if (left)
            binder = state.js_for_binder;
    } else if (!strcmp(parent, "assignment_expression") ||
               !strcmp(parent, "augmented_assignment_expression")) {
        if (left)
            binder = JS_BINDER_WRITE;
    } else if (!strcmp(parent, "update_expression")) {
        if (field_name && !strcmp(field_name, "argument"))
            binder = JS_BINDER_WRITE;
    } else if (!strcmp(parent, "catch_clause") || !strcmp(parent, "arrow_function")) {
        if (field_name && !strcmp(field_name, "parameter"))
            binder = JS_BINDER_PARAMETER;
    } else if (!strcmp(parent, "formal_parameters")) {
        binder = JS_BINDER_PARAMETER;
    } else if (!strcmp(parent, "required_parameter") || !strcmp(parent, "optional_parameter")) {
        if (name || (field_name && !strcmp(field_name, "pattern")))
            binder = JS_BINDER_PARAMETER;
    } else if (!strcmp(parent, "assignment_pattern") ||
               !strcmp(parent, "object_assignment_pattern")) {
        if (left)
            binder = inherited;
    } else if (!strcmp(parent, "pair_pattern")) {
        if (field_name && !strcmp(field_name, "value"))
            binder = inherited;
    } else if (!strcmp(parent, "object_pattern") || !strcmp(parent, "array_pattern") ||
               !strcmp(parent, "rest_pattern")) {
        binder = inherited;
    } else if (inherited == JS_BINDER_WRITE && (!strcmp(parent, "parenthesized_expression") ||
                                                !strcmp(parent, "non_null_expression"))) {
        binder = inherited;
    }
    if (type)
        state.js_type_level = true;
    state.js_binder = state.js_type_level ? JS_BINDER_NONE : binder;
    if (!strcmp(parent, "nested_identifier") ||
        (name && (!strcmp(parent, "internal_module") || !strcmp(parent, "module"))))
        state.js_no_site = true;
    return state;
}

static bool javascript_type_level_kind(const char *kind) {
    static const char *const kinds[] = {"type_annotation",
                                        "type_arguments",
                                        "type_parameters",
                                        "function_type",
                                        "constructor_type",
                                        "object_type",
                                        "interface_declaration",
                                        "interface_body",
                                        "type_alias_declaration",
                                        "extends_type_clause",
                                        "implements_clause",
                                        "function_signature",
                                        "abstract_method_signature",
                                        "method_signature",
                                        "call_signature",
                                        "construct_signature",
                                        "index_signature",
                                        "adding_type_annotation",
                                        "opting_type_annotation",
                                        "omitting_type_annotation",
                                        "asserts_annotation",
                                        "type_predicate_annotation",
                                        "template_type",
                                        NULL};
    return listed(kind, kinds);
}

static void js_set_lines(cbm_resolve_binding_index_t *index, size_t row, TSNode node) {
    if (row < index->name_count) {
        index->names[row].value_start_line = ts_node_start_point(node).row + 1;
        index->names[row].value_end_line = ts_node_end_point(node).row + 1;
    }
}

/* `const { a, b: c } = require("m")`: the property name a local binds. */
static const char *js_require_pattern_member(cbm_resolve_binding_index_t *index, TSNode node,
                                             const char **source) {
    TSNode pattern = ts_node_parent(node);
    const char *member = NULL;
    if (kind_is(node, "shorthand_property_identifier_pattern") &&
        kind_is(pattern, "object_pattern")) {
        member = node_text(index, node);
    } else if (kind_is(pattern, "pair_pattern") && ts_node_eq(node, field(pattern, "value")) &&
               kind_is(field(pattern, "key"), "property_identifier")) {
        member = node_text(index, field(pattern, "key"));
        pattern = ts_node_parent(pattern);
    }
    if (!member || !kind_is(pattern, "object_pattern"))
        return NULL;
    TSNode declarator = ts_node_parent(pattern);
    const char *property = NULL;
    if (!kind_is(declarator, "variable_declarator") ||
        !ts_node_eq(pattern, field(declarator, "name")) ||
        !js_require_value(index, field(declarator, "value"), source, &property) || property)
        return NULL;
    return member;
}

static void note_javascript_binder(cbm_resolve_binding_index_t *index, TSNode node,
                                   const char *name, const binding_walk_t *state) {
    if (state->js_binder == JS_BINDER_WRITE) {
        js_write(index, name, state->scope);
        add_site(index, name, node);
        return;
    }
    if (state->js_binder == JS_BINDER_PARAMETER) {
        add_name(index, name, 0, BINDING_BLOCKED);
        return;
    }
    /* The occurrence reads in its own block; a var binds at its function. */
    add_site(index, name, node);
    uint32_t site_scope = index->current_scope;
    if (state->js_binder == JS_BINDER_VAR)
        index->current_scope = javascript_function_scope(index, site_scope);
    const binding_scope_t *scope = &index->scopes[index->current_scope];
    bool module_level = index->current_scope == 0 || scope->namespace_scope;
    TSNode declarator = ts_node_parent(node);
    bool direct =
        kind_is(declarator, "variable_declarator") && ts_node_eq(node, field(declarator, "name"));
    TSNode value = direct ? field(declarator, "value") : (TSNode){0};
    const char *source = NULL, *member = NULL;
    size_t row = index->name_count;
    if (state->js_ambient) {
        add_name(index, name, 0, BINDING_BLOCKED);
    } else if (direct && js_require_value(index, value, &source, &member)) {
        add_name(index, name, 0, BINDING_IMPORT);
        if (index->name_count > row)
            index->names[row].js_import = js_import_fact(
                index, (cbm_resolve_js_import_t){
                           .local = name, .source = source, .member = member, .is_require = true});
    } else if (!direct && (member = js_require_pattern_member(index, node, &source)) != NULL) {
        add_name(index, name, 0, BINDING_IMPORT);
        if (index->name_count > row)
            index->names[row].js_import = js_import_fact(
                index, (cbm_resolve_js_import_t){
                           .local = name, .source = source, .member = member, .is_require = true});
    } else if (direct && js_function_value(value)) {
        add_name(index, name, 0,
                 BINDING_VALUE_DECLARATION | BINDING_FUNCTION_VALUE |
                     (state->named_export && module_level ? BINDING_EXPORTED : 0));
        js_set_lines(index, row, value);
    } else if (direct && module_level) {
        bool is_const = kind_is(ts_node_parent(declarator), "lexical_declaration") &&
                        child_token(ts_node_parent(declarator), "const");
        add_name(index, name, ts_node_end_byte(declarator),
                 BINDING_VALUE_DECLARATION | (is_const ? BINDING_CONST_VALUE : 0) |
                     (state->named_export ? BINDING_EXPORTED : 0));
        js_set_lines(index, row, declarator);
        if (index->name_count > row)
            index->names[row].at_start = ts_node_start_byte(declarator);
    } else {
        add_name(index, name, 0, BINDING_BLOCKED);
    }
    if (direct && js_chain_writes_module_exports(index, value))
        js_export_alias(index, name);
    if (direct && module_level && !state->js_ambient && index->current_scope == 0 &&
        kind_is(js_unwrap_value(value), "object"))
        note_javascript_object_members(index, name, js_unwrap_value(value), false);
    index->current_scope = site_scope;
}

/* `a.b.c`: identifiers and dots only, nothing evaluated along the way. */
static bool js_plain_member_path(const cbm_resolve_binding_index_t *index, TSNode member) {
    (void)index;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        TSNode object = field(member, "object");
        if (kind_is(object, "identifier"))
            return true;
        if (!kind_is(object, "member_expression") || child_token(object, "optional_chain"))
            return false;
        member = object;
    }
    return false;
}

static bool javascript_named_function_scope_kind(const char *kind) {
    return !strcmp(kind, "function_expression") || !strcmp(kind, "generator_function");
}

static binding_walk_t note_javascript_node(cbm_resolve_binding_index_t *index, TSNode node,
                                           const char *parent_kind, const char *field_name,
                                           unsigned depth, binding_walk_t state) {
    const char *kind = ts_node_type(node);
    state = javascript_child_context(parent_kind, field_name, state);
    index->current_scope = state.scope;
    index->current_this_owner = state.js_this_owner;
    index->current_this_prototype = state.js_this_prototype;
    if (!ts_node_is_named(node)) {
        return state;
    }
    if (!supported_javascript_node(kind)) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "unsupported_binding_syntax");
        return state;
    }
    if (javascript_type_level_kind(kind)) {
        state.js_type_level = true;
        state.js_binder = JS_BINDER_NONE;
    }
    if (kind_is(node, "import_statement")) {
        index->javascript_module |= depth == 1;
        state.import = true;
        state.type_only = child_token(node, "type");
        if (depth != 1) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "non_module_import_binding");
        }
    }
    note_http_dependency(index, node);
    note_javascript_commonjs(index, node, &state);
    if (kind_is(node, "export_statement")) {
        index->javascript_module |= depth == 1;
        bool namespace_level = index->scopes[state.scope].namespace_scope && parent_kind &&
                               !strcmp(parent_kind, "statement_block");
        state.named_export = (depth == 1 || namespace_level) && !child_token(node, "default");
        note_javascript_export(index, node, depth == 1);
    }
    if (state.import) {
        note_javascript_import(index, node, parent_kind, state.type_only);
    }
    if (kind_is(node, "ambient_declaration"))
        state.js_ambient = true;
    if (kind_is(node, "variable_declaration"))
        state.js_declaration = JS_BINDER_VAR;
    else if (kind_is(node, "lexical_declaration") || kind_is(node, "using_declaration"))
        state.js_declaration = JS_BINDER_LEXICAL;
    if (kind_is(node, "for_in_statement"))
        state.js_for_binder = child_token(node, "var") ? JS_BINDER_VAR
                              : child_token(node, "let") || child_token(node, "const")
                                  ? JS_BINDER_LEXICAL
                                  : JS_BINDER_WRITE;

    unsigned declaration = javascript_declaration_kind(kind);
    bool namespace_kind = kind_is(node, "internal_module") || kind_is(node, "module");
    if (declaration || namespace_kind) {
        TSNode name = field(node, "name");
        const binding_scope_t *scope = &index->scopes[state.scope];
        bool module_level = state.scope == 0 || scope->namespace_scope;
        unsigned flags =
            namespace_kind ? BINDING_TYPE_DECLARATION | BINDING_VALUE_TYPE : declaration;
        if (state.js_ambient)
            flags = BINDING_BLOCKED;
        else if (module_level && state.named_export)
            flags |= BINDING_EXPORTED;
        const char *text = kind_is(name, "identifier") || kind_is(name, "type_identifier")
                               ? node_text(index, name)
                               : NULL;
        if (text) {
            size_t row = index->name_count;
            add_name(index, text, 0, flags);
            js_set_lines(index, row, node);
            /* A sloppy-mode block function also assigns a function-scoped var. */
            uint32_t boundary = javascript_function_scope(index, state.scope);
            if (javascript_function_kind(kind) && boundary != state.scope) {
                index->current_scope = boundary;
                add_name(index, text, 0, BINDING_BLOCKED);
                index->current_scope = state.scope;
            }
        }
        if (namespace_kind) {
            const char *owner = scope->qualified_name;
            const char *qn =
                text && owner ? cbm_arena_sprintf(index->arena, "%s.%s", owner, text) : owner;
            state.scope = javascript_new_scope(index, qn, state.scope, true, true);
            state.js_body = true;
            state.named_export = false;
            index->current_scope = state.scope;
        }
    } else if (kind_is(node, "function_signature")) {
        const char *text = node_text(index, field(node, "name"));
        if (state.js_ambient && text)
            add_name(index, text, 0, BINDING_BLOCKED);
    } else if (kind_is(node, "import_alias")) {
        /* `import a = b.c` names a namespace path with no binding proof. */
        const char *text = node_text(index, ts_node_named_child(node, 0));
        if (text)
            add_name(index, text, 0, BINDING_BLOCKED);
        state.js_no_site = true;
    }

    if (javascript_runtime_function(node)) {
        javascript_this_owner(index, node, &state);
        index->current_this_owner = state.js_this_owner;
        index->current_this_prototype = state.js_this_prototype;
        const char *owner = index->scopes[state.scope].qualified_name;
        const char *qn =
            owner ? cbm_arena_sprintf(index->arena, "%s.<function@%u>", owner,
                                      ts_node_start_byte(node))
                  : cbm_arena_sprintf(index->arena, "%s.<function@%u>", index->result->module_qn,
                                      ts_node_start_byte(node));
        state.scope = javascript_new_scope(index, qn, state.scope, true, false);
        index->current_scope = state.scope;
        state.js_body = true;
        state.named_export = false;
        /* A function expression's own name is visible only inside it. */
        if (javascript_named_function_scope_kind(kind)) {
            const char *text = node_text(index, field(node, "name"));
            if (text) {
                size_t row = index->name_count;
                add_name(index, text, 0, BINDING_DECLARATION);
                js_set_lines(index, row, node);
            }
        }
    } else if (kind_is(node, "class")) {
        const char *text = node_text(index, field(node, "name"));
        if (text) {
            state.scope = javascript_new_scope(index, NULL, state.scope, false, false);
            index->current_scope = state.scope;
            add_name(index, text, 0, BINDING_BLOCKED);
        }
    } else if (kind_is(node, "statement_block")) {
        if (state.js_body)
            state.js_body = false;
        else
            state.scope = javascript_new_scope(index, NULL, state.scope, false, false);
        state.named_export = false;
        index->current_scope = state.scope;
    } else if (kind_is(node, "for_statement") || kind_is(node, "for_in_statement") ||
               kind_is(node, "switch_body")) {
        state.scope = javascript_new_scope(index, NULL, state.scope, false, false);
        index->current_scope = state.scope;
    } else if (kind_is(node, "catch_clause")) {
        state.scope = javascript_new_scope(index, NULL, state.scope, false, false);
        state.js_body = true;
        index->current_scope = state.scope;
    } else if (kind_is(node, "class_static_block")) {
        state.scope = javascript_new_scope(index, NULL, state.scope, true, false);
        state.js_body = true;
        index->current_scope = state.scope;
    } else if (kind_is(node, "class_body")) {
        state.named_export = false;
    }

    bool declaration_name =
        parent_kind && field_name && strcmp(field_name, "name") == 0 &&
        (javascript_declaration_kind(parent_kind) || !strcmp(parent_kind, "function_signature") ||
         javascript_named_function_scope_kind(parent_kind) || !strcmp(parent_kind, "class"));
    if (strcmp(kind, "identifier") == 0 || strcmp(kind, "shorthand_property_identifier") == 0 ||
        strcmp(kind, "shorthand_property_identifier_pattern") == 0) {
        const char *name = node_text(index, node);
        if (name && (strcmp(name, "eval") == 0 || strcmp(name, "Function") == 0)) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dynamic_namespace_effect");
        }
        if (!state.import && !declaration_name && !state.js_no_site) {
            if (state.js_binder)
                note_javascript_binder(index, node, name, &state);
            else
                add_site(index, name, node);
        }
    } else if (kind_is(node, "nested_type_identifier") && !state.import) {
        size_t before = index->site_count;
        add_site(index, node_text(index, node), node);
        if (index->site_count > before)
            index->sites[before].type_position = true;
    } else if (kind_is(node, "type_identifier") &&
               kind_is(ts_node_parent(node), "type_parameter")) {
        /* A type parameter shadows a same-named type wherever it is visible. */
        add_name(index, node_text(index, node), 0, BINDING_BLOCKED | BINDING_TYPE_DECLARATION);
    } else if (kind_is(node, "type_identifier") && !state.import && !declaration_name) {
        size_t before = index->site_count;
        add_site(index, node_text(index, node), node);
        if (index->site_count > before)
            index->sites[before].type_position = true;
    }
    if (kind_is(node, "property_identifier") &&
        kind_is(ts_node_parent(node), "member_expression") &&
        ts_node_eq(node, field(ts_node_parent(node), "property")) &&
        js_plain_member_path(index, ts_node_parent(node))) {
        size_t before = index->site_count;
        add_site(index, node_text(index, node), node);
        if (index->site_count > before)
            index->sites[before].member_path = node_text(index, ts_node_parent(node));
    }
    if (strcmp(kind, "call_expression") == 0 || strcmp(kind, "new_expression") == 0) {
        note_call(index, node);
    } else if (jsx_component_site(index, node)) {
        add_site(index, node_text(index, field(node, "name")), node);
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
            import.uri_start_byte = ts_node_start_byte(child);
            import.uri_end_byte = ts_node_end_byte(child);
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
    sites[index->site_count++] = (binding_site_t){.name = name,
                                                  .start = ts_node_start_byte(callee),
                                                  .end = ts_node_end_byte(node),
                                                  .scope = 0};
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
                                         "impl_item",
                                         "declaration_list",
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
        state = python_child_context(parent_kind, field_name, state);
        index->current_scope = state.scope;
        index->current_pending = state.pending;
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
    if (!supported_node(index->language, kind) &&
        !(index->language == CBM_LANG_PYTHON && python_transparent_container(node, parent_kind))) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "unsupported_binding_syntax");
        return state;
    }
    if (index->language == CBM_LANG_PYTHON && state.scope == 0 && python_annotation_only(node) &&
        kind_is(field(node, "left"), "identifier")) {
        index->python_annotation_binding_preserved = true;
        if (!python_inert_annotation(field(node, "type")))
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                       "python_annotation_effects_unproven");
        /* Continue the ordinary walk: annotations cannot hide dynamic names,
         * nested writes or unsupported syntax from the source audit. */
    }
    if (index->language == CBM_LANG_PYTHON &&
        ((!strcmp(kind, "interpolation") || !strcmp(kind, "format_expression")) &&
         !python_literal_format_value(field(node, "expression"), 0))) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "python_format_value_effects_unproven");
        return state;
    }
    if (index->language == CBM_LANG_PYTHON && !strcmp(kind, "type_conversion")) {
        const char *conversion = node_text(index, node);
        if (!conversion ||
            (strcmp(conversion, "!s") && strcmp(conversion, "!r") && strcmp(conversion, "!a"))) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                       "python_format_conversion_unproven");
            return state;
        }
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
        uint32_t enclosing_scope = state.scope;
        state.pending = python_pending_declaration(index, node, enclosing_scope, state.pending);
        state.scope =
            python_child_scope(index, node, enclosing_scope, !strcmp(kind, "class_definition"));
        state.header_scope = enclosing_scope;
        state.conditional = false;
        index->current_scope = state.scope;
        index->current_pending = state.pending;
    }
    /* Annotation expressions read names; they do not bind the names inside
     * the type. The parameter name itself remains a shadowing declaration. */
    if (index->language == CBM_LANG_PYTHON && strcmp(kind, "type") == 0 && field_name &&
        strcmp(field_name, "type") == 0 && parent_kind &&
        (strcmp(parent_kind, "typed_parameter") == 0 ||
         strcmp(parent_kind, "typed_default_parameter") == 0))
        state.blocked = false;
    bool annotation_target = index->language == CBM_LANG_PYTHON && !strcmp(kind, "identifier") &&
                             field_name && !strcmp(field_name, "left") &&
                             python_annotation_only(ts_node_parent(node));
    if (index->language == CBM_LANG_PYTHON) {
        state.blocked = annotation_target
                            ? state.scope != 0
                            : python_target_role(kind, parent_kind, field_name, state.blocked);
    } else if (blocks_identifiers(kind)) {
        state.blocked = true;
    }
    if (strcmp(kind, "identifier") == 0) {
        const char *name = node_text(index, node);
        if (!name) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "identifier_unavailable");
            return state;
        }
        if (dynamic_name(index->language, name)) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dynamic_namespace_effect");
        }
        bool function_name = field_name && strcmp(field_name, "name") == 0 && parent_kind &&
                             (strcmp(parent_kind, "function_definition") == 0 ||
                              strcmp(parent_kind, "function_declaration") == 0);
        if (state.blocked && !function_name) {
            add_name(index, name, ts_node_start_byte(node), BINDING_BLOCKED);
        }
        if (!state.import && !function_name && !annotation_target) {
            add_site(index, name, node);
            if (index->language == CBM_LANG_PYTHON)
                note_python_argument_wrappers(index, name, node);
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
            if (exported && (name.flags & (BINDING_DECLARATION | BINDING_TYPE_DECLARATION |
                                           BINDING_VALUE_DECLARATION))) {
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

/* Rows of one (name, scope), [low, *high). */
static size_t js_name_range(const cbm_resolve_binding_index_t *index, const char *name,
                            size_t length, uint32_t scope, size_t *high) {
    size_t low = 0, top = index->name_count;
    while (low < top) {
        size_t mid = low + (top - low) / 2;
        int compare = strncmp(index->names[mid].name, name, length);
        if (!compare && index->names[mid].name[length])
            compare = 1;
        if (!compare)
            compare = (index->names[mid].scope > scope) - (index->names[mid].scope < scope);
        if (compare < 0)
            low = mid + 1;
        else
            top = mid;
    }
    size_t end = low;
    while (end < index->name_count && index->names[end].scope == scope &&
           strlen(index->names[end].name) == length &&
           !strncmp(index->names[end].name, name, length))
        end++;
    *high = end;
    return low;
}

/* TypeScript keeps value and type names apart, so `type Error` does not shadow
 * the value Error and `export const Info` + `export type Info` do not collide. */
static bool js_row_in_space(const binding_name_t *row, js_space_t space) {
    bool type_only =
        (row->flags & BINDING_TYPE_IMPORT) ||
        ((row->flags & BINDING_TYPE_DECLARATION) && !(row->flags & BINDING_VALUE_TYPE));
    /* A parameter or block-local name lives in the value space alone. */
    bool value_only =
        (row->flags & (BINDING_DECLARATION | BINDING_VALUE_DECLARATION)) != 0 ||
        ((row->flags & BINDING_BLOCKED) &&
         !(row->flags & (BINDING_TYPE_IMPORT | BINDING_TYPE_DECLARATION | BINDING_VALUE_TYPE)));
    if (space == JS_SPACE_VALUE)
        return !type_only;
    if (space == JS_SPACE_TYPE)
        return !value_only;
    return true;
}

static void js_apply_writes(cbm_resolve_binding_index_t *index) {
    size_t missing = 0;
    for (size_t w = 0; w < index->js_write_count; w++) {
        const js_write_t *write = &index->js_writes[w];
        size_t length = strlen(write->name);
        uint32_t scope = write->scope;
        bool found = false;
        for (unsigned depth = 0; depth < BINDING_MAX_DEPTH && !found; depth++) {
            size_t high = 0;
            size_t low = js_name_range(index, write->name, length, scope, &high);
            for (size_t r = low; r < high; r++) {
                binding_name_t *row = &index->names[r];
                if (!js_row_in_space(row, JS_SPACE_VALUE))
                    continue;
                found = true;
                row->flags |= BINDING_WRITTEN;
                /* A write never changes which declaration a variable names. It
                 * replaces a callable, an import or a const's only value. */
                if (!(row->flags & BINDING_VALUE_DECLARATION) ||
                    (row->flags & (BINDING_CONST_VALUE | BINDING_FUNCTION_VALUE)))
                    row->flags |= BINDING_BLOCKED;
            }
            if (found || scope == 0 || scope >= index->scope_count)
                break;
            scope = index->scopes[scope].parent;
        }
        if (!found)
            missing++;
    }
    /* A sloppy write to an undeclared name creates a global of that name. */
    if (missing) {
        uint32_t scope = index->current_scope;
        index->current_scope = 0;
        size_t count = index->js_write_count;
        for (size_t w = 0; w < count; w++) {
            const js_write_t *write = &index->js_writes[w];
            bool declared = false;
            uint32_t at = write->scope;
            for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
                size_t high = 0;
                size_t low = js_name_range(index, write->name, strlen(write->name), at, &high);
                declared |= low < high;
                if (declared || at == 0 || at >= index->scope_count)
                    break;
                at = index->scopes[at].parent;
            }
            if (!declared)
                add_name(index, write->name, 0, BINDING_BLOCKED);
        }
        index->current_scope = scope;
        qsort(index->names, index->name_count, sizeof(*index->names), compare_name);
    }
}

/* Join each source declaration to its extracted definition by name and exact
 * lines. A function const gains callable identity from that definition; a
 * nested declaration has no target except through this join. */
static void audit_javascript_values(cbm_resolve_binding_index_t *index) {
    for (int d = 0; d < index->result->defs.count; d++) {
        const CBMDefinition *definition = &index->result->defs.items[d];
        if (!definition->name || !definition->qualified_name)
            continue;
        binding_name_t key = {.name = definition->name};
        size_t low = 0, high = index->name_count;
        while (low < high) {
            size_t mid = low + (high - low) / 2;
            if (strcmp(index->names[mid].name, key.name) < 0)
                low = mid + 1;
            else
                high = mid;
        }
        for (size_t r = low;
             r < index->name_count && !strcmp(index->names[r].name, definition->name); r++) {
            binding_name_t *binding = &index->names[r];
            bool lines = definition->start_line == binding->value_start_line &&
                         definition->end_line == binding->value_end_line;
            if (binding->flags & BINDING_FUNCTION_VALUE) {
                if (lines && definition->label && !strcmp(definition->label, "Function")) {
                    binding->value_definitions++;
                    binding->target = definition->qualified_name;
                }
            } else if (binding->flags & BINDING_VALUE_DECLARATION) {
                if (!binding->target || strcmp(binding->target, definition->qualified_name))
                    continue;
                /* `export const Info` + `export type Info`: the type's definition
                 * shares this QN and backs the other row, not this one. */
                if (definition->label &&
                    (!strcmp(definition->label, "Type") || !strcmp(definition->label, "Interface")))
                    continue;
                binding->value_definitions++;
                if (!definition->label || strcmp(definition->label, "Variable") ||
                    definition->parent_class || !lines)
                    binding->flags |= BINDING_BLOCKED;
            } else if (binding->scope &&
                       (binding->flags & (BINDING_DECLARATION | BINDING_TYPE_DECLARATION)) &&
                       lines) {
                binding->value_definitions++;
                binding->target = definition->qualified_name;
            }
        }
    }
    for (size_t n = 0; n < index->name_count; n++) {
        binding_name_t *binding = &index->names[n];
        if (binding->flags & BINDING_VALUE_DECLARATION) {
            if (binding->value_definitions != 1)
                binding->flags |= BINDING_BLOCKED;
            else if (binding->flags & BINDING_FUNCTION_VALUE)
                binding->flags =
                    (binding->flags & ~(unsigned)BINDING_VALUE_DECLARATION) | BINDING_DECLARATION;
        } else if (binding->scope &&
                   (binding->flags & (BINDING_DECLARATION | BINDING_TYPE_DECLARATION)) &&
                   binding->value_definitions != 1) {
            binding->flags |= BINDING_BLOCKED;
        }
    }
}

static const binding_name_t *js_module_row(const cbm_resolve_binding_index_t *index,
                                           const char *name, js_space_t space, bool *conflict) {
    size_t high = 0;
    size_t low = js_name_range(index, name, strlen(name), 0, &high);
    const binding_name_t *found = NULL;
    for (size_t r = low; r < high; r++) {
        if (!js_row_in_space(&index->names[r], space))
            continue;
        if (found)
            *conflict = true;
        found = &index->names[r];
    }
    return found;
}

static int js_compare_definition(const void *left, const void *right) {
    const CBMDefinition *a = *(const CBMDefinition *const *)left;
    const CBMDefinition *b = *(const CBMDefinition *const *)right;
    int order = strcmp(a->name ? a->name : "", b->name ? b->name : "");
    if (order)
        return order;
    return (a->start_line > b->start_line) - (a->start_line < b->start_line);
}

/* An exported function value: its definition at exactly these lines, or none. */
static const char *js_function_target(cbm_resolve_binding_index_t *index,
                                      const CBMDefinition **sorted, const js_raw_export_t *raw) {
    if (!raw->function_name)
        return NULL;
    CBMDefinition probe = {.name = raw->function_name, .start_line = raw->start_line};
    const CBMDefinition *key = &probe;
    const CBMDefinition **found = bsearch(&key, sorted, (size_t)index->result->defs.count,
                                          sizeof(*sorted), js_compare_definition);
    if (!found || !(*found)->label ||
        (strcmp((*found)->label, "Function") && strcmp((*found)->label, "Class")) ||
        (*found)->end_line != raw->end_line)
        return NULL;
    return (*found)->qualified_name;
}

static bool js_owner_is_alias(const cbm_resolve_binding_index_t *index, const char *owner) {
    for (size_t a = 0; a < index->js_export_alias_count; a++)
        if (!strcmp(index->js_export_aliases[a], owner))
            return true;
    return false;
}

static void js_settle_identifier(cbm_resolve_binding_index_t *index, const js_raw_export_t *raw,
                                 cbm_resolve_js_export_t *out) {
    bool conflict = false;
    const binding_name_t *row = js_module_row(
        index, raw->local, raw->kind == CBM_RESOLVE_EXPORT_TYPE ? JS_SPACE_TYPE : JS_SPACE_VALUE,
        &conflict);
    /* A same-named value and type can leave together in one specifier. */
    if (!row && raw->kind != CBM_RESOLVE_EXPORT_TYPE)
        row = js_module_row(index, raw->local, JS_SPACE_TYPE, &conflict);
    if (!row || conflict || ((row->flags & BINDING_BLOCKED) && !(row->flags & BINDING_TYPE_IMPORT)))
        return;
    if (row->js_import) {
        out->known = row->js_import->source != NULL;
        out->source = row->js_import->source;
        out->member = row->js_import->member;
        out->default_import = row->js_import->is_default;
    } else if (row->target) {
        out->known = true;
        out->target = row->target;
        out->kind =
            ((row->flags & (BINDING_DECLARATION | BINDING_VALUE_TYPE | BINDING_VALUE_DECLARATION))
                 ? CBM_RESOLVE_EXPORT_VALUE
                 : 0) |
            ((row->flags & BINDING_TYPE_DECLARATION) ? CBM_RESOLVE_EXPORT_TYPE : 0);
    }
}

/* Settle the raw facts into exported-name -> proved value. A declaration that
 * also leaves under its own name via `export` is one more raw fact. */
static void js_settle_exports(cbm_resolve_binding_index_t *index) {
    size_t exported = 0;
    for (size_t n = 0; n < index->name_count; n++)
        if (index->names[n].scope == 0 &&
            (index->names[n].flags & (BINDING_EXPORTED | BINDING_TYPE_EXPORTED)))
            exported++;
    size_t capacity = index->js_raw_export_count + exported;
    if (!capacity)
        return;
    index->js_exports = cbm_arena_alloc(index->arena, capacity * sizeof(*index->js_exports));
    if (!index->js_exports) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_export_allocation");
        return;
    }
    const CBMDefinition **sorted = NULL;
    int definitions = index->result->defs.count;
    for (size_t r = 0; r < index->js_raw_export_count && !sorted && definitions; r++) {
        if (index->js_raw_exports[r].value == JS_VALUE_FUNCTION) {
            sorted = cbm_arena_alloc(index->arena, (size_t)definitions * sizeof(*sorted));
            if (!sorted) {
                fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_export_allocation");
                return;
            }
            for (int d = 0; d < definitions; d++)
                sorted[d] = &index->result->defs.items[d];
            qsort(sorted, (size_t)definitions, sizeof(*sorted), js_compare_definition);
        }
    }
    for (size_t n = 0; n < index->name_count; n++) {
        const binding_name_t *row = &index->names[n];
        if (row->scope || !(row->flags & (BINDING_EXPORTED | BINDING_TYPE_EXPORTED)))
            continue;
        cbm_resolve_js_export_t *out = &index->js_exports[index->js_export_count++];
        bool blocked = (row->flags & BINDING_BLOCKED) != 0;
        *out = (cbm_resolve_js_export_t){
            .name = row->name,
            .target = blocked ? NULL : row->target,
            .known = !blocked && row->target,
            .kind = ((row->flags &
                      (BINDING_DECLARATION | BINDING_VALUE_TYPE | BINDING_VALUE_DECLARATION)) &&
                             (row->flags & BINDING_EXPORTED)
                         ? CBM_RESOLVE_EXPORT_VALUE
                         : 0) |
                    ((row->flags & BINDING_TYPE_DECLARATION) ? CBM_RESOLVE_EXPORT_TYPE : 0)};
    }
    for (size_t r = 0; r < index->js_raw_export_count; r++) {
        const js_raw_export_t *raw = &index->js_raw_exports[r];
        if (raw->owner && !js_owner_is_alias(index, raw->owner))
            continue;
        cbm_resolve_js_export_t *out = &index->js_exports[index->js_export_count++];
        *out = (cbm_resolve_js_export_t){
            .name = raw->export_name, .kind = raw->kind, .whole = raw->whole};
        /* `module.exports = x` replaces the object a bare `exports.y` wrote to. */
        if (raw->bare_exports && index->js_whole_export && !index->js_exports_rebound)
            continue;
        switch (raw->value) {
        case JS_VALUE_IDENTIFIER:
            js_settle_identifier(index, raw, out);
            break;
        case JS_VALUE_FUNCTION:
            out->target = sorted ? js_function_target(index, sorted, raw) : NULL;
            out->known = out->target != NULL;
            out->kind = CBM_RESOLVE_EXPORT_VALUE;
            break;
        case JS_VALUE_REQUIRE:
            out->source = raw->source;
            out->member = raw->member;
            out->known = raw->source != NULL;
            break;
        case JS_VALUE_OBJECT:
            out->is_object = true;
            out->known = true;
            break;
        case JS_VALUE_UNKNOWN:
            /* `var app = module.exports = express()`: one value, two names. */
            if (raw->whole && index->js_export_alias_count == 1) {
                js_raw_export_t alias = *raw;
                alias.local = index->js_export_aliases[0];
                js_settle_identifier(index, &alias, out);
            }
            break;
        }
    }
}

static int js_compare_member(const void *left, const void *right) {
    const js_member_t *a = left, *b = right;
    int order = strcmp(a->owner, b->owner);
    if (!order)
        order = strcmp(a->member, b->member);
    return order ? order : (a->prototype > b->prototype) - (a->prototype < b->prototype);
}

/* Each member's declaration, by its extracted definition at exact lines or the
 * declaration an identifier names. Two rows for one member answer nothing. */
static void js_settle_members(cbm_resolve_binding_index_t *index) {
    if (!index->js_member_count || index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return;
    qsort(index->js_members, index->js_member_count, sizeof(*index->js_members), js_compare_member);
    int definitions = index->result->defs.count;
    const CBMDefinition **sorted =
        definitions ? cbm_arena_alloc(index->arena, (size_t)definitions * sizeof(*sorted)) : NULL;
    if (definitions && !sorted) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_member_allocation");
        return;
    }
    for (int d = 0; d < definitions; d++)
        sorted[d] = &index->result->defs.items[d];
    if (definitions)
        qsort(sorted, (size_t)definitions, sizeof(*sorted), js_compare_definition);
    for (size_t m = 0; m < index->js_member_count; m++) {
        js_member_t *row = &index->js_members[m];
        if (m + 1 < index->js_member_count && !js_compare_member(row, row + 1))
            row->conflict = row[1].conflict = true;
        if (row->conflict)
            continue;
        if (row->function_name && definitions) {
            js_raw_export_t raw = {.function_name = row->function_name,
                                   .start_line = row->start_line,
                                   .end_line = row->end_line};
            row->target = js_function_target(index, sorted, &raw);
        } else if (row->local) {
            bool conflict = false;
            const binding_name_t *binding =
                js_module_row(index, row->local, JS_SPACE_VALUE, &conflict);
            if (binding && !conflict && !(binding->flags & BINDING_BLOCKED) &&
                (binding->flags & BINDING_DECLARATION))
                row->target = binding->target;
        }
    }
}

bool cbm_resolve_binding_js_exports_complete(const cbm_resolve_binding_index_t *index) {
    return index && javascript_language(index->language) &&
           index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE && !index->js_exports_open;
}

bool cbm_resolve_binding_js_commonjs(const cbm_resolve_binding_index_t *index) {
    return index && index->js_commonjs;
}

size_t cbm_resolve_binding_js_export_count(const cbm_resolve_binding_index_t *index) {
    return index && index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE ? index->js_export_count : 0;
}

const cbm_resolve_js_export_t *cbm_resolve_binding_js_export_at(
    const cbm_resolve_binding_index_t *index, size_t offset) {
    return offset < cbm_resolve_binding_js_export_count(index) ? &index->js_exports[offset] : NULL;
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
        /* This walk is linear in the file. A node count would void the largest
         * ECMAScript sources for their size alone; only nesting has a real bound. */
        if ((++visited > BINDING_MAX_NODES && !javascript_language(index->language)) ||
            depth >= BINDING_MAX_DEPTH) {
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

bool cbm_resolve_binding_set_identity(cbm_resolve_binding_index_t *index, const char *project,
                                      const char *file) {
    if (!index || !index->observation)
        return false;
    cbm_resolve_proof_source_t source = index->observation->context.source;
    source.project = project;
    source.file = file;
    bool ok = cbm_resolve_lookup_context_init(&index->observation->context,
                                              &index->observation->pool, source);
    if (ok)
        index->observation->context.source =
            cbm_resolve_proof_view(index->observation->context.source_fact)->source;
    return ok;
}

cbm_resolve_lookup_context_t *cbm_resolve_binding_lookup_context(
    const cbm_resolve_binding_index_t *index) {
    return index && index->observation ? &index->observation->context : NULL;
}

const cbm_resolve_proof_t *cbm_resolve_binding_site_proof(const cbm_resolve_binding_index_t *index,
                                                          const char *name, uint32_t start,
                                                          uint32_t end, CBMSourceOrigin origin) {
    const binding_observation_t *value = index ? index->observation : NULL;
    return value && name && value->name && !strcmp(value->name, name) && value->start == start &&
                   value->end == end && value->origin == origin
               ? value->proof
               : NULL;
}

static void binding_query_begin(const cbm_resolve_binding_index_t *index,
                                cbm_resolve_lookup_t *query, const char *name, uint32_t start,
                                uint32_t end, CBMSourceOrigin origin) {
    cbm_resolve_lookup_context_t *context = cbm_resolve_binding_lookup_context(index);
    cbm_resolve_lookup_begin(query, context, "source_lexical_binding", name, start, end, origin);
    if (context)
        (void)cbm_resolve_lookup_observe(
            query, "source_binding_audit", name,
            index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE ? index->reason : NULL,
            index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE ? CBM_RESOLVE_BINDING_UNIQUE
                                                             : CBM_RESOLVE_BINDING_UNRESOLVED,
            index->analysis, 0, index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE, index->reason);
}

static cbm_resolve_result_t binding_query_finish(const cbm_resolve_binding_index_t *index,
                                                 cbm_resolve_lookup_t *query,
                                                 cbm_resolve_result_t result) {
    if (!index || !index->observation)
        return result;
    binding_observation_t *site = index->observation;
    site->name =
        query->key.lookup_key ? cbm_arena_strdup(index->arena, query->key.lookup_key) : NULL;
    site->start = query->key.start_byte;
    site->end = query->key.end_byte;
    site->origin = (CBMSourceOrigin)query->key.origin;
    cbm_resolve_result_t local = result;
    /* This is the target checked against the exact lexical binding above,
     * separate from a reusable exported semantic root retained in result. */
    query->key.binding_identity = local.target;
    site->proof = cbm_resolve_lookup_finish(query, CBM_PROOF_REGISTRY_LOOKUP, local);
    if (!site->proof) {
        result.analysis = site->pool.failure == CBM_RESOLVE_ANALYSIS_LIMITED
                              ? CBM_RESOLVE_ANALYSIS_LIMITED
                              : CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
        result.reason = "source_binding_proof_unavailable";
    } else if (cbm_resolve_proof_view(site->proof)->outcome.analysis !=
               CBM_RESOLVE_ANALYSIS_COMPLETE) {
        result.analysis = cbm_resolve_proof_view(site->proof)->outcome.analysis;
        result.facts &= ~CBM_RESOLVE_FACT_LOOKUP_COMPLETE;
    }
    /* Shared import roots stay in their owner pool. The coordinator consumes
     * this independent local site root alongside that original root. */
    if (!result.proof)
        result.proof = site->proof;
    return result;
}

/* A module value escaping to an unknown alias/callee can mutate its namespace.
 * This independent pass still visits syntax outside the positive-binding
 * subset. Imported-name queries are binary searches, never source rescans. */
static bool python_namespace_read(cbm_resolve_binding_index_t *index, TSNode node) {
    TSNode parent = ts_node_parent(node);
    if (kind_is(parent, "attribute") && ts_node_eq(node, field(parent, "attribute")))
        return true; /* Attribute spelling is not a lexical module reference. */
    if (kind_is(parent, "call") && ts_node_eq(node, field(parent, "function")))
        return true; /* Calling a module is an error, not an alias escape. */
    if (!kind_is(parent, "attribute") || !ts_node_eq(node, field(parent, "object")))
        return kind_is(parent, "expression_statement");
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        const char *member = node_text(index, field(parent, "attribute"));
        if (!member || !strcmp(member, "__dict__"))
            return false;
        node = parent;
        parent = ts_node_parent(node);
        if (kind_is(parent, "attribute") && ts_node_eq(node, field(parent, "object")))
            continue;
        return kind_is(parent, "expression_statement") ||
               (kind_is(parent, "call") && ts_node_eq(node, field(parent, "function")));
    }
    return false;
}
static int python_escape_compare(const void *left, const void *right) {
    return strcmp(*(const char *const *)left, *(const char *const *)right);
}
static void python_namespace_effects(cbm_resolve_binding_index_t *index, TSNode root) {
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    unsigned depth = 0;
    size_t visited = 0;
    index->python_effects_complete = true;
    for (;;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (++visited > BINDING_MAX_NODES || depth > BINDING_MAX_DEPTH) {
            index->python_effects_complete = false;
            break;
        }
        bool skip = kind_is(node, "import_statement") || kind_is(node, "import_from_statement");
        if (kind_is(node, "global_statement"))
            index->python_effects_complete = false;
        if (kind_is(node, "identifier")) {
            const char *name = node_text(index, node);
            if (name && (!strcmp(name, "exec") || !strcmp(name, "eval") ||
                         !strcmp(name, "__import__") || !strcmp(name, "globals")))
                index->python_effects_complete = false;
        }
        if (kind_is(node, "attribute")) {
            const char *name = node_text(index, field(node, "attribute"));
            if (name && (!strcmp(name, "modules") || !strcmp(name, "import_module")))
                index->python_effects_complete = false;
        }
        if (kind_is(node, "identifier") && !python_namespace_read(index, node)) {
            const char *name = node_text(index, node);
            const char **names =
                grow_array(index, index->python_escapes, index->python_escape_count,
                           &index->python_escape_capacity, sizeof(*names));
            if (!name || !names) {
                index->python_effects_complete = false;
                break;
            }
            index->python_escapes = names;
            names[index->python_escape_count++] = name;
        }
        if (!skip && ts_tree_cursor_goto_first_child(&cursor)) {
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
    if (index->python_escape_count > 1)
        qsort(index->python_escapes, index->python_escape_count, sizeof(*index->python_escapes),
              python_escape_compare);
}

/* Absence needs a stronger audit than a positive local binding. Module code
 * can install arbitrary names or __getattr__, including through imports and
 * class construction. Only inert statements and plain function declarations
 * with inert headers are closed here; function bodies are not executed. */
static bool python_inert_namespace(TSNode root) {
    uint32_t count = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode node = ts_node_named_child(root, i);
        if (kind_is(node, "comment") || kind_is(node, "pass_statement"))
            continue;
        if (kind_is(node, "expression_statement") && ts_node_named_child_count(node) == 1) {
            TSNode value = ts_node_named_child(node, 0);
            if (!kind_is(value, "string"))
                return false;
            for (uint32_t j = 0; j < ts_node_named_child_count(value); j++) {
                TSNode child = ts_node_named_child(value, j);
                if (!kind_is(child, "string_start") && !kind_is(child, "string_content") &&
                    !kind_is(child, "string_end"))
                    return false;
            }
            continue;
        }
        if (!kind_is(node, "function_definition") || !ts_node_is_null(field(node, "return_type")) ||
            !ts_node_is_null(field(node, "type_parameters")))
            return false;
        TSNode parameters = field(node, "parameters");
        if (ts_node_is_null(parameters))
            return false;
        for (uint32_t j = 0; j < ts_node_named_child_count(parameters); j++)
            if (!kind_is(ts_node_named_child(parameters, j), "identifier"))
                return false;
    }
    return true;
}

/* Symbol QNs omit a terminal index filename, while module QNs retain it.
 * Use the captured path and the same normalization as extraction: trimming
 * ".index" from a module string would corrupt public.index.ts identities. */
static const char *javascript_symbol_scope(cbm_resolve_binding_index_t *index) {
    const CBMFileResult *result = index->result;
    if (!result->source_project || !result->source_project[0] || !result->source_file ||
        !result->source_file[0] || !result->source_content_id[CBM_SOURCE_ORIGIN_RAW][0] ||
        !result->source_configuration_id[0]) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "javascript_source_identity_missing");
        return NULL;
    }
    const char *module = cbm_fqn_module(index->arena, result->source_project, result->source_file);
    char *scope = cbm_fqn_compute(index->arena, result->source_project, result->source_file,
                                  "__binding_scope__");
    if (!module || !scope) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "binding_symbol_scope_allocation");
        return NULL;
    }
    if (strcmp(module, result->module_qn)) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "javascript_source_identity_mismatch");
        return NULL;
    }
    /* The fixed nonempty member above always adds this final separator. */
    *strrchr(scope, '.') = '\0';
    return scope;
}

static void audit_http_default(cbm_resolve_binding_index_t *index, TSNode root);
static void audit_http_escapes(cbm_resolve_binding_index_t *index);

/* Language-level module signals, independent of file contents. A TypeScript
 * script can only redeclare a global implementation as a compile error. */
static bool javascript_module_by_source(const CBMFileResult *result, CBMLanguage language) {
    if (language == CBM_LANG_TYPESCRIPT || language == CBM_LANG_TSX || language == CBM_LANG_ARKTS)
        return true;
    const char *file = result ? result->source_file : NULL;
    const char *extension = file ? strrchr(file, '.') : NULL;
    return extension &&
           (!strcmp(extension, ".mjs") || !strcmp(extension, ".cjs") || !strcmp(extension, ".jsx"));
}

cbm_resolve_binding_index_t *cbm_resolve_binding_build_declared_module(
    CBMArena *arena, const CBMFileResult *result, CBMLanguage language, const char *source,
    size_t source_len, bool declared_module) {
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
    index->observation = cbm_arena_calloc(arena, sizeof(*index->observation));
    if (!index->observation)
        return NULL;
    /* One node per distinct fact of this file. A budget here would drop the later
     * occurrences of a large file for being later, not for being unproved. */
    cbm_resolve_proof_pool_init_project(&index->observation->pool, arena);
    cbm_resolve_proof_source_t captured = {
        .module = result ? result->module_qn : NULL,
        .scope = result ? result->module_qn : NULL,
        .content_identity = result ? result->source_content_id[CBM_SOURCE_ORIGIN_RAW] : NULL,
        .configuration_identity = result ? result->source_configuration_id : NULL,
        .source_origin = CBM_SOURCE_ORIGIN_RAW};
    if (!cbm_resolve_lookup_context_init(&index->observation->context, &index->observation->pool,
                                         captured))
        return NULL;
    if (result && result->source_content_id[CBM_SOURCE_ORIGIN_RAW][0] &&
        !cbm_result_source_matches(result, source, source_len, CBM_SOURCE_ORIGIN_RAW)) {
        index->reason = "binding_source_snapshot_mismatch";
        return index;
    }
    bool component = language == CBM_LANG_VUE || language == CBM_LANG_SVELTE;
    if (language != CBM_LANG_PYTHON && language != CBM_LANG_LUA && language != CBM_LANG_DART &&
        language != CBM_LANG_RUST && !javascript_language(language) && !component) {
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
    uint32_t tree_end = (uint32_t)source_len;
    if (component && tree) {
        TSRange range = {0};
        CBMExtractCtx host = {.arena = arena,
                              .result = (CBMFileResult *)result,
                              .source = source,
                              .source_len = source_len <= INT_MAX ? (int)source_len : -1,
                              .language = language,
                              .module_qn = result->module_qn,
                              .root = ts_tree_root_node(tree)};
        TSTree *script = cbm_component_binding_tree(&host, &language, &range);
        if (owned_tree)
            ts_tree_delete(owned_tree);
        tree = owned_tree = script;
        tree_end = range.end_byte;
        if (script) {
            index->language = language;
            index->javascript_module = true;
        }
    }
    if (!tree || ts_node_has_error(ts_tree_root_node(tree)) ||
        ts_node_end_byte(ts_tree_root_node(tree)) != tree_end) {
        index->reason = "incomplete_binding_tree";
    } else {
        index->analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        index->reason = "binding_complete_supported_subset";
        if (language == CBM_LANG_PYTHON || javascript_language(language)) {
            index->scopes =
                grow_array(index, NULL, 0, &index->scope_capacity, sizeof(*index->scopes));
            if (index->scopes) {
                const char *scope = result->module_qn;
                if (javascript_language(language)) {
                    scope = javascript_symbol_scope(index);
                } else {
                    size_t symbol_scope_len = cbm_fqn_symbol_scope_len(scope);
                    if (symbol_scope_len != strlen(scope))
                        scope = cbm_arena_strndup(arena, scope, symbol_scope_len);
                }
                if (!scope) {
                    if (index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE)
                        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED,
                                   "python_symbol_scope_allocation");
                } else {
                    index->scopes[0] =
                        (binding_scope_t){.qualified_name = scope, .function_scope = true};
                    index->scope_count = 1;
                }
            }
        }
        if (index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE) {
            if (javascript_language(language))
                index->javascript_module |=
                    declared_module || javascript_module_by_source(result, language);
            walk_tree(index, ts_tree_root_node(tree));
            if (language == CBM_LANG_PYTHON) {
                index->python_namespace_closed = python_inert_namespace(ts_tree_root_node(tree));
                python_namespace_effects(index, ts_tree_root_node(tree));
            }
        }
        if (index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE && javascript_language(language) &&
            !index->javascript_module) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "javascript_script_scope_unproven");
        }
    }
    if (index->name_count) {
        qsort(index->names, index->name_count, sizeof(*index->names), compare_name);
        apply_export_markers(index);
    }
    if (javascript_language(language)) {
        js_apply_writes(index);
        audit_javascript_values(index);
        js_settle_exports(index);
        js_settle_members(index);
    }
    /* An explicit replacement of annotation storage can invoke user code
     * even when the annotation itself is a bare name. Do not infer the
     * builtin mapping contract for a source-defined __annotations__. */
    if (index->python_annotation_binding_preserved && index->python_annotation_storage_bound)
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "python_annotation_storage_unproven");
    if (index->site_count) {
        qsort(index->sites, index->site_count, sizeof(*index->sites), compare_site);
    }
    if (tree && javascript_language(language) && index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE) {
        audit_http_escapes(index);
        audit_http_default(index, ts_tree_root_node(tree));
    }
    if (owned_tree)
        ts_tree_delete(owned_tree);
    return index;
}

cbm_resolve_binding_index_t *cbm_resolve_binding_build(CBMArena *arena, const CBMFileResult *result,
                                                       CBMLanguage language, const char *source,
                                                       size_t source_len) {
    return cbm_resolve_binding_build_declared_module(arena, result, language, source, source_len,
                                                     false);
}

static const binding_name_t *find_scoped_name(const cbm_resolve_binding_index_t *index,
                                              const char *name, size_t length, uint32_t scope,
                                              bool *present, js_space_t space) {
    if (space != JS_SPACE_ANY) {
        size_t top = 0;
        const binding_name_t *found = NULL;
        bool duplicate = false;
        for (size_t r = js_name_range(index, name, length, scope, &top); r < top; r++) {
            if (!js_row_in_space(&index->names[r], space))
                continue;
            duplicate |= found != NULL;
            found = &index->names[r];
        }
        *present = found != NULL;
        return duplicate ? NULL : found;
    }
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
    return find_scoped_name(index, name, length, 0, &present, JS_SPACE_ANY);
}

static void audit_http_escapes(cbm_resolve_binding_index_t *index) {
    for (size_t i = 0; i < index->site_count; i++) {
        binding_site_t *site = &index->sites[i];
        if (!site->http_escape && !site->http_factory_escape)
            continue;
        binding_name_t *binding =
            (binding_name_t *)find_name(index, site->name, strlen(site->name));
        if (binding) {
            binding->http_escaped |= site->http_escape;
            binding->http_factory_escaped |= site->http_factory_escape;
        }
    }
}

/* HTTP default values are a separate domain from named exports. This
 * deliberately small source form has no executable module statements other
 * than const initializers, and cannot certify an arbitrary default object. */
static bool http_literal(TSNode node) {
    return kind_is(node, "number") || kind_is(node, "string") || kind_is(node, "true") ||
           kind_is(node, "false") || kind_is(node, "null");
}

static TSNode http_default_value(TSNode root) {
    TSNode value = {0};
    uint32_t count = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode node = ts_node_named_child(root, i);
        if (kind_is(node, "comment") || kind_is(node, "import_statement") ||
            kind_is(node, "lexical_declaration"))
            continue;
        if (!kind_is(node, "export_statement") || !child_token(node, "default") ||
            !ts_node_is_null(value))
            return (TSNode){0};
        value = field(node, "value");
        if (!kind_is(value, "identifier"))
            return (TSNode){0};
    }
    return value;
}

static TSNode http_default_initializer(cbm_resolve_binding_index_t *index, TSNode root,
                                       const char *name) {
    TSNode selected = {0};
    uint32_t count = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode declaration = ts_node_named_child(root, i);
        if (!kind_is(declaration, "lexical_declaration"))
            continue;
        if (!child_token(declaration, "const"))
            return (TSNode){0};
        uint32_t variables = ts_node_named_child_count(declaration);
        for (uint32_t v = 0; v < variables; v++) {
            TSNode declarator = ts_node_named_child(declaration, v);
            if (kind_is(declarator, "comment"))
                continue;
            TSNode identifier = field(declarator, "name"), value = field(declarator, "value");
            if (!kind_is(declarator, "variable_declarator") || !kind_is(identifier, "identifier"))
                return (TSNode){0};
            const char *spelling = node_text(index, identifier);
            if (spelling && !strcmp(spelling, name)) {
                if (!ts_node_is_null(selected))
                    return (TSNode){0};
                selected = value;
            } else if (!http_literal(value)) {
                return (TSNode){0};
            }
        }
    }
    return selected;
}

bool cbm_resolve_binding_http_domain_complete(const cbm_resolve_binding_index_t *index) {
    return index && index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE && !index->http_domain_unknown;
}

size_t cbm_resolve_binding_http_dependency_count(const cbm_resolve_binding_index_t *index) {
    return index ? index->http_dependency_count : 0;
}

const cbm_resolve_http_dependency_t *cbm_resolve_binding_http_dependency_at(
    const cbm_resolve_binding_index_t *index, size_t offset) {
    return index && offset < index->http_dependency_count ? &index->http_dependencies[offset]
                                                          : NULL;
}

bool cbm_resolve_binding_http_import_safe(const cbm_resolve_binding_index_t *index,
                                          const char *name, bool axios_factory) {
    if (!name || !cbm_resolve_binding_import(index, name))
        return false;
    const binding_name_t *binding = find_name(index, name, strlen(name));
    return binding && binding->http_import_path &&
           !(axios_factory ? binding->http_factory_escaped : binding->http_escaped);
}

const char *cbm_resolve_binding_http_import_path(const cbm_resolve_binding_index_t *index,
                                                 const char *name) {
    if (!cbm_resolve_binding_import(index, name))
        return NULL;
    const binding_name_t *binding = find_name(index, name, strlen(name));
    return binding ? binding->http_import_path : NULL;
}

static bool http_axios_origin(const cbm_resolve_binding_index_t *index, const char *name) {
    const char *path = cbm_resolve_binding_http_import_path(index, name);
    return path && !strcmp(path, "axios");
}

static bool http_client_base(cbm_resolve_binding_index_t *index, TSNode call, const char **base) {
    TSNode args = field(call, "arguments");
    if (ts_node_is_null(args) || ts_node_named_child_count(args) != 1)
        return false;
    TSNode object = ts_node_named_child(args, 0);
    if (!kind_is(object, "object"))
        return false;
    bool seen = false;
    uint32_t count = ts_node_named_child_count(object);
    for (uint32_t i = 0; i < count; i++) {
        TSNode pair = ts_node_named_child(object, i);
        if (kind_is(pair, "comment"))
            continue;
        if (!kind_is(pair, "pair"))
            return false;
        const char *key = node_text(index, field(pair, "key"));
        TSNode value = field(pair, "value");
        if (key && !strcmp(key, "timeout") && kind_is(value, "number"))
            continue;
        if (!key || strcmp(key, "baseURL") || seen)
            return false;
        seen = true;
        if (kind_is(value, "string")) {
            const char *text = node_text(index, value);
            size_t length = text ? strlen(text) : 0;
            if (length >= 2 && !strchr(text, '\\'))
                *base = cbm_arena_strndup(index->arena, text + 1, length - 2);
        }
    }
    /* An absent/nonliteral base is unknown, not an empty literal base. */
    return true;
}

static void audit_http_default(cbm_resolve_binding_index_t *index, TSNode root) {
    TSNode value = http_default_value(root);
    const char *name = ts_node_is_null(value) ? NULL : node_text(index, value);
    const binding_name_t *binding = name ? find_name(index, name, strlen(name)) : NULL;
    if (!binding || (binding->flags & BINDING_BLOCKED) ||
        !(binding->flags & BINDING_VALUE_DECLARATION) || !binding->target ||
        ts_node_start_byte(value) < binding->at)
        return;
    TSNode call = http_default_initializer(index, root, name);
    TSNode function = field(call, "function");
    TSNode object = field(function, "object"), property = field(function, "property");
    const char *receiver = kind_is(object, "identifier") ? node_text(index, object) : NULL;
    const char *method = node_text(index, property);
    const char *base = NULL;
    if (!kind_is(call, "call_expression") || !kind_is(function, "member_expression") || !receiver ||
        !method || strcmp(method, "create") || !http_axios_origin(index, receiver) ||
        !http_client_base(index, call, &base))
        return;
    index->http_default_target = binding->target;
    index->http_default_base = base;
}

bool cbm_resolve_binding_http_default(const cbm_resolve_binding_index_t *index, const char **target,
                                      const char **base) {
    *target = *base = NULL;
    if (!index || index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !index->http_default_target)
        return false;
    *target = index->http_default_target;
    *base = index->http_default_base;
    return true;
}

static const binding_name_t *find_lexical_name(const cbm_resolve_binding_index_t *index,
                                               const char *name, size_t length, uint32_t scope,
                                               cbm_resolve_lookup_t *query, js_space_t space) {
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        bool present;
        const binding_name_t *found = find_scoped_name(index, name, length, scope, &present, space);
        /* An ECMAScript block that holds no such name adds nothing to the proof
         * beyond this file's own source fact. Blocks nest deeply; the occupied
         * scope and the module scope are what the lookup establishes. */
        if (space != JS_SPACE_ANY && !present && scope) {
            if (scope >= index->scope_count)
                return NULL;
            scope = index->scopes[scope].parent;
            continue;
        }
        const char *domain = scope < index->scope_count && index->scopes[scope].qualified_name
                                 ? index->scopes[scope].qualified_name
                                 : index->result->module_qn;
        const char *key = cbm_arena_strndup(index->arena, name, length);
        (void)cbm_resolve_lookup_observe(
            query, domain, key, found ? (found->target ? found->target : key) : NULL,
            found     ? CBM_RESOLVE_BINDING_UNIQUE
            : present ? CBM_RESOLVE_BINDING_AMBIGUOUS
                      : CBM_RESOLVE_BINDING_UNRESOLVED,
            CBM_RESOLVE_ANALYSIS_COMPLETE, found ? found->flags : 0, true,
            !present ? "lexical_scope_key_absent"
            : !found ? "lexical_scope_key_ambiguous"
                     : "lexical_scope_key_observed");
        if (present || !scope)
            return found;
        if (scope >= index->scope_count)
            return NULL;
        scope = index->scopes[scope].parent;
    }
    return NULL;
}

/* Query the exact AST occurrence and require its first visible binding to be
 * the unique module import. Parameter shadows stop lexical lookup; other
 * binders, writes and escapes retain the conservative module-wide veto. */
bool cbm_resolve_binding_import_occurrence(const cbm_resolve_binding_index_t *index,
                                           const CBMCall *call, const char *local_name) {
    if (!index || !call || !local_name || !call->callee_name || !call->enclosing_func_qn ||
        !call->first_string_arg || index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
        call->source_origin != CBM_SOURCE_ORIGIN_RAW ||
        call->site_end_byte <= call->site_start_byte ||
        !cbm_resolve_binding_import(index, local_name))
        return false;
    binding_site_t key = {
        .name = call->callee_name, .start = call->site_start_byte, .end = call->site_end_byte};
    const binding_site_t *site =
        index->sites && index->site_count
            ? bsearch(&key, index->sites, index->site_count, sizeof(*index->sites), compare_site)
            : NULL;
    const binding_name_t *binding = find_name(index, local_name, strlen(local_name));
    const binding_name_t *visible = site ? find_lexical_name(index, local_name, strlen(local_name),
                                                             site->scope, NULL, JS_SPACE_VALUE)
                                         : NULL;
    return site && site->invocation && binding && visible == binding && !binding->http_escaped &&
           site->http_scope && site->http_url &&
           !strcmp(site->http_scope, call->enclosing_func_qn) &&
           !strcmp(site->http_url, call->first_string_arg) && call->site_start_byte >= binding->at;
}

const cbm_resolve_js_import_t *cbm_resolve_binding_js_import(
    const cbm_resolve_binding_index_t *index, const char *name, uint32_t start, uint32_t end,
    CBMSourceOrigin origin, const char **path, bool *type_position) {
    if (!index || !javascript_language(index->language) ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !name ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start || !index->sites || !index->site_count)
        return NULL;
    binding_site_t key = {.name = name, .start = start, .end = end};
    const binding_site_t *site =
        bsearch(&key, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    if (!site)
        return NULL;
    const char *spelled = site->member_path ? site->member_path : name;
    if (path)
        *path = spelled;
    if (type_position)
        *type_position = site->type_position;
    const char *dot = strchr(spelled, '.');
    size_t root = dot ? (size_t)(dot - spelled) : strlen(spelled);
    const binding_name_t *binding =
        find_lexical_name(index, spelled, root, site->scope, NULL,
                          site->type_position ? JS_SPACE_TYPE : JS_SPACE_VALUE);
    if (!binding || !binding->js_import)
        return NULL;
    bool type_import = site->type_position && (binding->flags & BINDING_TYPE_IMPORT);
    return (binding->flags & BINDING_BLOCKED) && !type_import ? NULL : binding->js_import;
}

bool cbm_resolve_binding_js_declaration_lines(const cbm_resolve_binding_index_t *index,
                                              const char *name, uint32_t start, uint32_t end,
                                              CBMSourceOrigin origin, uint32_t *start_line,
                                              uint32_t *end_line) {
    if (!index || !javascript_language(index->language) ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !name || strchr(name, '.') ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start || !index->sites || !index->site_count)
        return false;
    binding_site_t key = {.name = name, .start = start, .end = end};
    const binding_site_t *site =
        bsearch(&key, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    if (!site || site->member_path)
        return false;
    const binding_name_t *binding =
        find_lexical_name(index, name, strlen(name), site->scope, NULL,
                          site->type_position ? JS_SPACE_TYPE : JS_SPACE_VALUE);
    if (!binding || (binding->flags & BINDING_BLOCKED) || !binding->value_start_line)
        return false;
    *start_line = binding->value_start_line;
    *end_line = binding->value_end_line;
    return true;
}

bool cbm_resolve_binding_jsx_occurrence(const cbm_resolve_binding_index_t *index, const char *name,
                                        uint32_t start, uint32_t end, CBMSourceOrigin origin) {
    if (!index || !name || !*name || !javascript_language(index->language) ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || origin != CBM_SOURCE_ORIGIN_RAW ||
        !index->sites || !index->site_count || end <= start)
        return false;
    binding_site_t key = {.name = name, .start = start, .end = end};
    const binding_site_t *site =
        bsearch(&key, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    return site && site->jsx_component && site->invocation;
}

cbm_resolve_result_t cbm_resolve_binding_validate(const cbm_resolve_binding_index_t *index,
                                                  const char *name, uint32_t start, uint32_t end,
                                                  CBMSourceOrigin origin,
                                                  cbm_resolve_result_t candidate) {
    cbm_resolve_lookup_t query;
    binding_query_begin(index, &query, name, start, end, origin);
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
        return binding_query_finish(index, &query, candidate);
    }
    binding_site_t site = {.name = name, .start = start, .end = end};
    const binding_site_t *located =
        index->sites && index->site_count
            ? bsearch(&site, index->sites, index->site_count, sizeof(*index->sites), compare_site)
            : NULL;
    (void)cbm_resolve_lookup_observe(
        &query, "source_occurrences", name, located ? name : NULL,
        located ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
        CBM_RESOLVE_ANALYSIS_COMPLETE, 0, true,
        located ? "exact_occurrence_observed" : "exact_occurrence_absent");
    if (!located) {
        candidate.reason = "binding_occurrence_missing";
        return binding_query_finish(index, &query, candidate);
    }
    const char *spelled = located->member_path ? located->member_path : name;
    const char *dot = strchr(spelled, '.');
    size_t root_length = dot ? (size_t)(dot - spelled) : strlen(spelled);
    bool ecma = javascript_language(index->language);
    js_space_t space = !ecma                    ? JS_SPACE_ANY
                       : located->type_position ? JS_SPACE_TYPE
                                                : JS_SPACE_VALUE;
    const binding_name_t *binding =
        find_lexical_name(index, spelled, root_length, located->scope, &query, space);
    /* `import type` names a type, so a type position may use it. */
    bool type_import = binding && located->type_position && (binding->flags & BINDING_TYPE_IMPORT);
    /* A later declaration binds a closure that runs after it, in any scope but
     * its own and never inside its own initializer. */
    bool too_early = binding && start < binding->at &&
                     (!ecma || located->scope == binding->scope || start >= binding->at_start);
    bool dart_import = index->language == CBM_LANG_DART && !dot && !binding &&
                       !cbm_resolve_binding_name_present(index, name) &&
                       candidate.rule == CBM_RESOLVE_RULE_IMPORT_BINDING &&
                       (prior_facts & CBM_RESOLVE_FACT_IMPORT_BOUND) &&
                       (prior_facts & CBM_RESOLVE_FACT_LOOKUP_COMPLETE);
    if (!dart_import &&
        (!binding || ((binding->flags & BINDING_BLOCKED) && !type_import) || too_early)) {
        candidate.reason = "binding_shadowed_or_unavailable";
        return binding_query_finish(index, &query, candidate);
    }
    for (const binding_pending_declaration_t *pending = located->pending; binding && pending;
         pending = pending->parent) {
        if (binding->scope == pending->scope && !strcmp(binding->name, pending->name)) {
            candidate.reason = "binding_declaration_not_yet_bound";
            return binding_query_finish(index, &query, candidate);
        }
    }
    if (binding && (binding->flags & BINDING_VALUE_DECLARATION) &&
        (located->invocation ||
         (prior_facts & (CBM_RESOLVE_FACT_CALLABLE_TYPED | CBM_RESOLVE_FACT_RECEIVER_TYPED |
                         CBM_RESOLVE_FACT_TYPE_BOUND | CBM_RESOLVE_FACT_CLASS_CALLABLE)))) {
        candidate.reason = "value_binding_does_not_prove_callable_or_type";
        return binding_query_finish(index, &query, candidate);
    }
    if (dart_import) {
        candidate.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    } else if (candidate.rule == CBM_RESOLVE_RULE_SCOPE_BINDING && !dot &&
               (binding->flags &
                (BINDING_DECLARATION | BINDING_VALUE_TYPE | BINDING_VALUE_DECLARATION)) &&
               binding->target && strcmp(binding->target, candidate.target) == 0) {
        candidate.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    } else if (candidate.rule == CBM_RESOLVE_RULE_IMPORT_BINDING &&
               ((binding->flags & BINDING_IMPORT) || type_import) &&
               (prior_facts & CBM_RESOLVE_FACT_IMPORT_BOUND) &&
               (prior_facts & CBM_RESOLVE_FACT_LOOKUP_COMPLETE)) {
        candidate.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    } else if (ecma && candidate.rule == CBM_RESOLVE_RULE_SCOPE_BINDING && !dot &&
               located->type_position && (binding->flags & BINDING_TYPE_DECLARATION) &&
               binding->target && strcmp(binding->target, candidate.target) == 0) {
        candidate.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    } else if (ecma && candidate.rule == CBM_RESOLVE_RULE_SCOPE_BINDING && dot &&
               (binding->flags & BINDING_VALUE_TYPE) && binding->target &&
               !strncmp(binding->target, candidate.target, strlen(binding->target)) &&
               !strcmp(candidate.target + strlen(binding->target), dot)) {
        /* `Service.create()`: the exact member of the class or namespace this
         * identifier lexically names. */
        candidate.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
    } else {
        candidate.reason = "binding_target_or_export_unproven";
        return binding_query_finish(index, &query, candidate);
    }
    if (index->language == CBM_LANG_PYTHON && candidate.rule == CBM_RESOLVE_RULE_SCOPE_BINDING &&
        (binding->flags & BINDING_TYPE_DECLARATION) && (binding->flags & BINDING_VALUE_TYPE))
        candidate.facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
    if (ecma && located->invocation && !dot && candidate.rule == CBM_RESOLVE_RULE_SCOPE_BINDING &&
        (binding->flags & BINDING_TYPE_DECLARATION) && (binding->flags & BINDING_VALUE_TYPE))
        candidate.facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
    candidate.facts |= CBM_RESOLVE_FACT_LOOKUP_COMPLETE | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    candidate.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
    candidate.reason = "exact_lexical_binding";
    return binding_query_finish(index, &query, candidate);
}

cbm_resolve_result_t cbm_resolve_binding_local(const cbm_resolve_binding_index_t *index,
                                               const char *name, uint32_t start, uint32_t end,
                                               CBMSourceOrigin origin) {
    cbm_resolve_lookup_t query;
    binding_query_begin(index, &query, name, start, end, origin);
    cbm_resolve_result_t result = {.reason = "local_binding_unavailable"};
    bool ecma = index && javascript_language(index->language);
    if (!index || (index->language != CBM_LANG_PYTHON && !ecma) ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !name || strchr(name, '.') ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start)
        return binding_query_finish(index, &query, result);
    binding_site_t site = {.name = name, .start = start, .end = end};
    const binding_site_t *located =
        index->sites && index->site_count
            ? bsearch(&site, index->sites, index->site_count, sizeof(*index->sites), compare_site)
            : NULL;
    (void)cbm_resolve_lookup_observe(
        &query, "source_occurrences", name, located ? name : NULL,
        located ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
        CBM_RESOLVE_ANALYSIS_COMPLETE, 0, true,
        located ? "exact_occurrence_observed" : "exact_occurrence_absent");
    js_space_t space = !ecma || !located        ? JS_SPACE_ANY
                       : located->type_position ? JS_SPACE_TYPE
                                                : JS_SPACE_VALUE;
    const binding_name_t *binding =
        located && !located->member_path
            ? find_lexical_name(index, name, strlen(name), located->scope, &query, space)
            : NULL;
    unsigned local_kinds = BINDING_DECLARATION | BINDING_VALUE_TYPE;
    if (ecma)
        local_kinds |= located && located->type_position ? BINDING_TYPE_DECLARATION
                                                         : BINDING_VALUE_DECLARATION;
    if (!binding || !binding->target || (binding->flags & BINDING_BLOCKED) ||
        !(binding->flags & local_kinds))
        return binding_query_finish(index, &query, result);
    result.binding = CBM_RESOLVE_BINDING_UNIQUE;
    result.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    result.rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
    result.facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_SCOPE_BOUND;
    result.target = binding->target;
    result.candidate_count = 1;
    return cbm_resolve_binding_validate(index, name, start, end, origin, result);
}

cbm_resolve_result_t cbm_resolve_binding_js_member(const cbm_resolve_binding_index_t *index,
                                                   const char *name, uint32_t start, uint32_t end,
                                                   CBMSourceOrigin origin) {
    cbm_resolve_lookup_t query;
    binding_query_begin(index, &query, name, start, end, origin);
    cbm_resolve_result_t result = {.reason = "js_member_unavailable"};
    const char *dot = name ? strchr(name, '.') : NULL;
    if (!index || !javascript_language(index->language) ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !dot || strchr(dot + 1, '.') ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start || !index->sites || !index->site_count)
        return binding_query_finish(index, &query, result);
    binding_site_t key = {.name = name, .start = start, .end = end};
    const binding_site_t *site =
        bsearch(&key, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    if (!site || !site->invocation)
        return binding_query_finish(index, &query, result);
    size_t root = (size_t)(dot - name);
    const char *owner = NULL;
    bool prototype = false;
    if (root == 4 && !strncmp(name, "this", 4)) {
        owner = site->this_owner;
        prototype = site->this_prototype;
    } else {
        const binding_name_t *binding =
            find_lexical_name(index, name, root, site->scope, &query, JS_SPACE_VALUE);
        if (binding && binding->scope == 0 && !binding->js_import)
            owner = binding->name;
    }
    if (!owner)
        return binding_query_finish(index, &query, result);
    bool conflict = false;
    const binding_name_t *holder = js_module_row(index, owner, JS_SPACE_VALUE, &conflict);
    if (!holder || conflict || (holder->flags & (BINDING_BLOCKED | BINDING_WRITTEN)) ||
        holder->js_import)
        return binding_query_finish(index, &query, result);
    js_member_t probe = {.owner = owner, .member = dot + 1, .prototype = prototype};
    const js_member_t *row = index->js_member_count
                                 ? bsearch(&probe, index->js_members, index->js_member_count,
                                           sizeof(*index->js_members), js_compare_member)
                                 : NULL;
    js_member_t computed = {.owner = owner, .member = JS_MEMBER_COMPUTED, .prototype = prototype};
    if (row && index->js_member_count &&
        bsearch(&computed, index->js_members, index->js_member_count, sizeof(*index->js_members),
                js_compare_member))
        row = NULL;
    (void)cbm_resolve_lookup_observe(
        &query, owner, dot + 1, row ? row->target : NULL,
        row && row->target ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
        CBM_RESOLVE_ANALYSIS_COMPLETE, 0, true,
        row && row->target ? "js_member_declaration_observed" : "js_member_declaration_absent");
    if (!row || !row->target || row->conflict)
        return binding_query_finish(index, &query, result);
    result = (cbm_resolve_result_t){
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_SCOPE_BOUND |
                 CBM_RESOLVE_FACT_LOOKUP_COMPLETE | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = row->target,
        .candidate_count = 1,
        .reason = "js_member_declaration"};
    return binding_query_finish(index, &query, result);
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
    if (javascript_language(index->language)) {
        /* `export const Info` and `export type Info` share one qualified name. */
        size_t top = 0;
        unsigned merged = 0;
        for (size_t r = js_name_range(index, name, strlen(name), 0, &top); r < top; r++) {
            const binding_name_t *row = &index->names[r];
            if (!row->target || strcmp(row->target, qualified_name) ||
                (row->flags & BINDING_BLOCKED) ||
                !(row->flags & (BINDING_EXPORTED | BINDING_TYPE_EXPORTED)))
                return 0;
            if ((row->flags &
                 (BINDING_DECLARATION | BINDING_VALUE_TYPE | BINDING_VALUE_DECLARATION)) &&
                (row->flags & BINDING_EXPORTED))
                merged |= CBM_RESOLVE_EXPORT_VALUE;
            if (row->flags & BINDING_TYPE_DECLARATION)
                merged |= CBM_RESOLVE_EXPORT_TYPE;
        }
        return merged;
    }
    const binding_name_t *binding = find_name(index, name, strlen(name));
    if (!binding || !binding->target || (binding->flags & BINDING_BLOCKED) ||
        strcmp(binding->target, qualified_name) != 0 ||
        ((javascript_language(index->language) || index->language == CBM_LANG_RUST) &&
         !(binding->flags & (BINDING_EXPORTED | BINDING_TYPE_EXPORTED)))) {
        return 0;
    }
    unsigned kind = 0;
    if ((binding->flags & (BINDING_DECLARATION | BINDING_VALUE_TYPE | BINDING_VALUE_DECLARATION)) &&
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

bool cbm_resolve_binding_python_effects_complete(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_PYTHON && index->python_effects_complete;
}

bool cbm_resolve_binding_python_import_escaped(const cbm_resolve_binding_index_t *index,
                                               const char *name) {
    if (!index || !name || !index->python_effects_complete)
        return true;
    return index->python_escape_count &&
           bsearch(&name, index->python_escapes, index->python_escape_count,
                   sizeof(*index->python_escapes), python_escape_compare) != NULL;
}

bool cbm_resolve_binding_python_namespace_closed(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_PYTHON &&
           index->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE && index->python_namespace_closed &&
           !cbm_resolve_binding_name_present(index, "__getattr__") &&
           !cbm_resolve_binding_name_present(index, "__getattribute__");
}
size_t cbm_resolve_binding_python_namespace_size(const cbm_resolve_binding_index_t *index) {
    return cbm_resolve_binding_python_namespace_closed(index) ? index->name_count : 0;
}
const char *cbm_resolve_binding_python_namespace_name(const cbm_resolve_binding_index_t *index,
                                                      size_t offset) {
    return cbm_resolve_binding_python_namespace_closed(index) && offset < index->name_count &&
                   index->names[offset].scope == 0
               ? index->names[offset].name
               : NULL;
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
