#include "binding.h"
#include "source_adapter.h"
#include "stats.h"
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
    BINDING_WRITTEN = 32768,
    /* Rebound by a decorator: blocked until the decorators are proven to keep
     * the declaration's identity. */
    BINDING_DECORATED = 65536
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
    /* A module-level class's own `constructor`, which `new` runs. */
    const char *js_constructor;
    /* One-based index of a Python import below module level or under a
     * statement: the extractor records only top-level imports. */
    uint32_t nested_import;
    /* What one Python binder received; see cbm_resolve_python_root_t. */
    cbm_resolve_python_root_kind_t value_kind;
    const char *value_chain;
    uint32_t value_scope;
    bool value_none;
    /* Python: from value_from to value_until every path reaches this binder. Zero until:
     * no such stretch. */
    uint32_t value_from, value_until;
    bool import_root;
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
    /* The `Ns` of a type annotation's `Ns.Type`: spelled as a value, used as a type. */
    bool type_path_root;
    /* A plain `a.b.c` member read. The occurrence is `c`; `a` is what binds. */
    const char *member_path;
    /* What `this` means here, when a member assignment or literal decides it. */
    const char *this_owner;
    bool this_prototype;
    bool http_escape, http_factory_escape;
    const char *http_scope, *http_url;
    /* Python receiver expression with call arguments dropped: `a.b().c`. */
    const char *chain;
    /* The direct-argument carrier spells `a.b` as b over the attribute's span.
     * The spelling is a member name here, never a lexical read of b. */
    bool attribute_terminal;
    /* Inside a comprehension written directly in a class body. */
    bool class_comprehension;
} binding_site_t;

typedef struct {
    const char *qualified_name;
    uint32_t parent;
    bool class_scope;
    bool function_scope; /* ECMAScript: the boundary a `var` hoists to. */
    bool namespace_scope;
    /* The class whose body directly declares this function, as scope + 1, and
     * the first parameter that carries its instance or class. */
    uint32_t method_of;
    const char *self_name;
    bool self_is_class;
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

typedef struct {
    uint32_t start, end;
    uint32_t parent; /* one-based enclosing loop */
} python_loop_t;

/* `if isinstance(E, C):` and its kin: in [from, until] E holds a C. */
typedef struct {
    const char *chain, *type;
    uint32_t from, until, scope;
    uint32_t parent; /* one-based enclosing narrowing */
} python_narrow_t;

typedef struct {
    const char *chain;
    uint32_t at; /* where the statement ends: its right side still reads what was there */
    uint32_t scope;
    const char *value; /* what a plain assignment stores, when followable */
    uint32_t value_at;
} python_store_t;

typedef struct {
    const char *name;   /* the local name the statement went through */
    const char *member; /* the dotted path it writes, NULL when any name can change */
} python_write_t;

typedef struct {
    const char *alias, *of;
} python_alias_t;

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
    bool python_annotation_binding_preserved;
    bool python_annotation_storage_bound;
    python_write_t *python_writes;  /* sorted by name */
    python_alias_t *python_aliases; /* `alias = name` statements */
    size_t python_alias_count, python_alias_capacity;
    size_t python_write_count, python_write_capacity;
    bool python_effects_complete;
    bool python_writes_unseen;       /* the audit stopped before the end of the file */
    bool python_name_handed;         /* __name__ goes to a call: that may build the module */
    bool python_namespace_open;      /* a computed globals() store can add any name */
    python_narrow_t *python_narrows; /* sorted by from, outermost first, once the walk ends */
    size_t python_narrow_count, python_narrow_capacity;
    python_store_t *python_stores; /* attribute stores, sorted by chain then position */
    size_t python_store_count, python_store_capacity;
    python_loop_t *python_loops; /* in start order: the walk's */
    size_t python_loop_count, python_loop_capacity;
    uint32_t python_open_loop; /* one-based innermost loop being walked */
    cbm_resolve_python_nested_import_t *python_nested;
    size_t python_nested_count, python_nested_capacity;
    cbm_resolve_python_star_t *python_stars;
    size_t python_star_count, python_star_capacity;
    cbm_resolve_python_class_t *python_classes;
    size_t python_class_count, python_class_capacity;
    cbm_resolve_python_member_t *python_members;
    size_t python_member_count, python_member_capacity;
    cbm_resolve_python_decorated_t *python_decorated;
    size_t python_decorated_count, python_decorated_capacity;
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
    bool js_commonjs, js_whole_export, js_exports_rebound, js_exports_detached, js_exports_open;
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
                 BINDING_VALUE_DECLARATION | BINDING_DECORATED)) {
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

static const char *python_import_canonical(const cbm_resolve_binding_index_t *index,
                                           const char *chain);
static const char *python_annotation_chain(cbm_resolve_binding_index_t *index, TSNode node,
                                           unsigned depth);

const char *cbm_resolve_python_typed_text(CBMArena *arena, const char *type, const char *rest) {
    const char *marker = strchr(type, '@');
    size_t head = marker ? (size_t)(marker - type) : strlen(type);
    if (marker && !strcmp(marker, "@type"))
        return cbm_arena_sprintf(arena, "%.*s%s", (int)head, type, rest);
    return cbm_arena_sprintf(arena, "%.*s()%s%s", (int)head, type, marker ? marker : "", rest);
}

static bool listed(const char *kind, const char *const *list);

/* The enclosing def declares this name as a parameter whose default is None. */
static bool python_parameter_defaults_none(cbm_resolve_binding_index_t *index, TSNode node,
                                           const char *name) {
    TSNode function = node;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH && !ts_node_is_null(function); depth++) {
        if (kind_is(function, "lambda") || kind_is(function, "class_definition"))
            return false;
        if (kind_is(function, "function_definition"))
            break;
        function = ts_node_parent(function);
    }
    TSNode parameters =
        kind_is(function, "function_definition") ? field(function, "parameters") : (TSNode){0};
    if (ts_node_is_null(parameters) || !name)
        return false;
    bool found = false;
    TSTreeCursor cursor = ts_tree_cursor_new(parameters);
    for (bool more = ts_tree_cursor_goto_first_child(&cursor); more && !found;
         more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode parameter = ts_tree_cursor_current_node(&cursor);
        if (!kind_is(parameter, "default_parameter") &&
            !kind_is(parameter, "typed_default_parameter"))
            continue;
        const char *spelled = node_text(index, field(parameter, "name"));
        found = spelled && !strcmp(spelled, name) && kind_is(field(parameter, "value"), "none");
    }
    ts_tree_cursor_delete(&cursor);
    return found;
}

/* The access path of a Python expression, as text: names, member steps and
 * calls with their arguments dropped. Only forms whose value can be followed
 * statically have one. Two-argument super and one-argument type keep their
 * identifiers, because those name the owner. */
static const char *python_chain(cbm_resolve_binding_index_t *index, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 32)
        return NULL;
    if (kind_is(node, "identifier"))
        return node_text(index, node);
    /* Awaiting a coroutine yields what its function returns; calling one without
     * await and then reading a member is not code that runs. */
    if (kind_is(node, "parenthesized_expression") || kind_is(node, "await"))
        return ts_node_named_child_count(node) == 1
                   ? python_chain(index, ts_node_named_child(node, 0), depth + 1)
                   : NULL;
    if (kind_is(node, "attribute")) {
        const char *object = python_chain(index, field(node, "object"), depth + 1);
        const char *member = node_text(index, field(node, "attribute"));
        return object && member ? cbm_arena_sprintf(index->arena, "%s.%s", object, member) : NULL;
    }
    /* x[k] and x[a:b]: what the container holds, read at evaluation. */
    if (kind_is(node, "subscript") && ts_node_named_child_count(node) == 2) {
        const char *object = python_chain(index, field(node, "value"), depth + 1);
        return object ? cbm_arena_sprintf(index->arena, "%s%s", object,
                                          kind_is(field(node, "subscript"), "slice") ? "[:]" : "[]")
                      : NULL;
    }
    /* `x or Y()` for a parameter defaulting to None: Y() is what the signature provides. */
    if (kind_is(node, "boolean_operator")) {
        const char *operator= node_text(index, field(node, "operator"));
        TSNode left = field(node, "left");

        if (operator&& !strcmp(operator, "or") && kind_is(left, "identifier") &&
            python_parameter_defaults_none(index, node, node_text(index, left)))
            return python_chain(index, field(node, "right"), depth + 1);
        return NULL;
    }
    /* `A() if c else None`: a None says nothing about the class a member read finds. */
    if (kind_is(node, "conditional_expression") && ts_node_named_child_count(node) == 3) {
        TSNode first = ts_node_named_child(node, 0), second = ts_node_named_child(node, 2);
        if (kind_is(first, "none") != kind_is(second, "none"))
            return python_chain(index, kind_is(first, "none") ? second : first, depth + 1);
        return NULL;
    }
    if (!kind_is(node, "call"))
        return NULL;
    TSNode function = field(node, "function"), args = field(node, "arguments");
    const char *callee = python_chain(index, function, depth + 1);
    if (!callee)
        return NULL;
    uint32_t count = ts_node_is_null(args) ? 0 : ts_node_named_child_count(args);
    /* partial(f, ...) keeps what it wraps: py_members checks the import. */
    if (count && (!strcmp(callee, "partial") || !strcmp(callee, "functools.partial"))) {
        const char *wrapped = python_chain(index, ts_node_named_child(args, 0), depth + 1);
        if (wrapped)
            return cbm_arena_sprintf(index->arena, "%s(%s)", callee, wrapped);
    }
    /* copy.copy(x) is an x, by the standard library's own signature. */
    size_t spelled = strlen(callee);
    if (count == 1 && ((spelled >= 4 && !strcmp(callee + spelled - 4, "copy")))) {
        const char *canonical = python_import_canonical(index, callee);
        if (canonical && (!strcmp(canonical, "copy.copy") || !strcmp(canonical, "copy.deepcopy")))
            return python_chain(index, ts_node_named_child(args, 0), depth + 1);
    }
    /* typing.cast(T, x) is a T: that is all it does. */
    if (count == 2 && spelled >= 4 && !strcmp(callee + spelled - 4, "cast")) {
        const char *canonical = python_import_canonical(index, callee);
        if (canonical &&
            (!strcmp(canonical, "typing.cast") || !strcmp(canonical, "typing_extensions.cast"))) {
            const char *type = python_annotation_chain(index, ts_node_named_child(args, 0), 0);
            return type ? cbm_resolve_python_typed_text(index->arena, type, "") : NULL;
        }
    }
    /* sorted(x) and friends hold what x holds; py_members checks the name is the builtin. */
    static const char *const passing[] = {"list",   "tuple",    "set",  "frozenset",
                                          "sorted", "reversed", "iter", NULL};
    if (count >= 1 && listed(callee, passing)) {
        const char *wrapped = python_chain(index, ts_node_named_child(args, 0), depth + 1);
        if (wrapped)
            return cbm_arena_sprintf(index->arena, "%s(%s)", callee, wrapped);
    }
    bool owner_form =
        (!strcmp(callee, "super") && count == 2) || (!strcmp(callee, "type") && count == 1);
    if (!owner_form)
        return cbm_arena_sprintf(index->arena, "%s()", callee);
    const char *first = node_text(index, ts_node_named_child(args, 0));
    const char *second = count == 2 ? node_text(index, ts_node_named_child(args, 1)) : NULL;
    if (!first || !kind_is(ts_node_named_child(args, 0), "identifier") ||
        (count == 2 && (!second || !kind_is(ts_node_named_child(args, 1), "identifier"))))
        return NULL;
    return count == 2 ? cbm_arena_sprintf(index->arena, "%s(%s,%s)", callee, first, second)
                      : cbm_arena_sprintf(index->arena, "%s(%s)", callee, first);
}

static bool python_in_class_comprehension(const cbm_resolve_binding_index_t *index, TSNode node) {
    if (!index->current_scope || !index->scopes[index->current_scope].class_scope)
        return false;
    for (unsigned depth = 0; !ts_node_is_null(node) && depth < 64; depth++) {
        if (kind_is(node, "list_comprehension") || kind_is(node, "set_comprehension") ||
            kind_is(node, "dictionary_comprehension") || kind_is(node, "generator_expression"))
            return true;
        if (kind_is(node, "class_definition") || kind_is(node, "function_definition") ||
            kind_is(node, "lambda"))
            return false;
        node = ts_node_parent(node);
    }
    return false;
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
    if (index->language == CBM_LANG_PYTHON && (kind_is(node, "call") || kind_is(node, "attribute")))
        index->sites[index->site_count - 1].chain =
            python_chain(index, kind_is(node, "call") ? field(node, "function") : node, 0);
    if (index->language == CBM_LANG_PYTHON)
        index->sites[index->site_count - 1].class_comprehension =
            python_in_class_comprehension(index, node);
}

/* The same carrier over a parenthesized direct argument, `f((a.b))`. */
static void python_note_terminal_wrappers(cbm_resolve_binding_index_t *index, const char *terminal,
                                          TSNode attribute) {
    const char *chain = python_chain(index, attribute, 0);
    TSNode value = attribute, parent = ts_node_parent(value);
    for (unsigned depth = 0; depth < 32 && kind_is(parent, "parenthesized_expression"); depth++) {
        size_t sites = index->site_count;
        add_site(index, terminal, parent);
        if (index->site_count > sites) {
            index->sites[index->site_count - 1].attribute_terminal = true;
            index->sites[index->site_count - 1].chain = chain;
        }
        value = parent;
        parent = ts_node_parent(value);
    }
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
    /* Expressions the ordinary walk audits through their children. */
    static const char *const python_expressions[] = {"list_splat",
                                                     "dictionary_splat",
                                                     "parenthesized_list_splat",
                                                     "ellipsis",
                                                     "except_group_clause",
                                                     "print_statement",
                                                     "chevron",
                                                     "union_type",
                                                     "member_type",
                                                     "constrained_type",
                                                     "splat_type",
                                                     "line_continuation",
                                                     NULL};
    /* Pattern identifiers bind, see python_target_role, as a type alias does. */
    static const char *const python_binders[] = {"match_statement",
                                                 "case_clause",
                                                 "case_pattern",
                                                 "dict_pattern",
                                                 "class_pattern",
                                                 "complex_pattern",
                                                 "splat_pattern",
                                                 "union_pattern",
                                                 "keyword_pattern",
                                                 "type_alias_statement",
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
    if (language == CBM_LANG_PYTHON)
        return listed(kind, python) || listed(kind, python_expressions) ||
               listed(kind, python_binders);
    return listed(kind, lua);
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
        return parent &&
               (!strcmp(parent, "generic_type") || !strcmp(parent, "function_definition") ||
                !strcmp(parent, "class_definition"));
    if (!strcmp(kind, "keyword_separator") || !strcmp(kind, "positional_separator"))
        return parent && (!strcmp(parent, "parameters") || !strcmp(parent, "lambda_parameters")) &&
               ts_node_named_child_count(node) == 0;
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
    if (!strcmp(kind, "as_pattern_target") || !strcmp(kind, "delete_statement") ||
        !strcmp(kind, "case_pattern"))
        target = true;
    /* def f[T](x: T) binds T; the alias name in `type X = int` is a binder too. */
    if (parent && field_name && !strcmp(field_name, "type_parameters"))
        target = true;
    if (parent && !strcmp(parent, "type_alias_statement") && field_name)
        target = !strcmp(field_name, "left");
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

static bool dynamic_name(CBMLanguage language, const char *name) {
    /* setattr/delattr/__dict__ change the object they are given. When that is
     * a module binding, python_namespace_effects records the escape and that
     * namespace is invalidated; otherwise they cannot rebind a name audited
     * here. __import__ loads a module and returns it. exec, eval, globals and
     * locals/vars are classified at the occurrence below. */
    static const char *const python[] = {"exec", "eval", "globals", NULL};
    static const char *const lua[] = {"exec",     "eval",    "globals",    "locals",   "vars",
                                      "setattr",  "delattr", "__import__", "__dict__", "load",
                                      "loadfile", "dofile",  "loadstring", "setfenv",  "getfenv",
                                      "debug",    "_ENV",    "_G",         "rawset",   NULL};
    /* Lua environment operations have no special meaning in Python. */
    return listed(name, language == CBM_LANG_LUA ? lua : python);
}

/* An identifier is a lexical read only outside the member and keyword roles:
 * `node.eval`, `f(eval=1)` and `def eval(self)` never touch the builtin, and
 * `value.helper` never refers to a module-level `helper`. */
static bool python_lexical_identifier(const char *parent, const char *field_name) {
    if (!parent || !field_name)
        return true;
    if (!strcmp(parent, "attribute") && !strcmp(field_name, "attribute"))
        return false;
    return strcmp(parent, "keyword_argument") || strcmp(field_name, "name");
}

/* A parameter or local spelled `vars` or `eval` is not the builtin. Source
 * order suffices: a parameter or earlier binder has already been recorded, and
 * this runs only for the few identifiers spelled like a dynamic builtin. */
static bool python_name_shadowed(const cbm_resolve_binding_index_t *index, const char *name) {
    uint32_t scope = index->current_scope;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        for (size_t i = 0; i < index->name_count; i++)
            if (index->names[i].scope == scope && !strcmp(index->names[i].name, name))
                return true;
        if (!scope || scope >= index->scope_count)
            return false;
        scope = index->scopes[scope].parent;
    }
    return true;
}

/* exec and eval write the namespace they run in, which an explicit globals
 * argument replaces. globals() is a write only if it can be written through: a
 * membership test, an item read, a read-only method or a splat into a new dict
 * leaves the module as it was. */
static bool python_namespace_call_writes(cbm_resolve_binding_index_t *index, TSNode node,
                                         const char *name) {
    TSNode call = ts_node_parent(node);
    if (!kind_is(call, "call") || !ts_node_eq(node, field(call, "function")))
        return true;
    if (strcmp(name, "globals")) {
        TSNode args = field(call, "arguments");
        uint32_t positional = 0;
        for (uint32_t i = 0; !ts_node_is_null(args) && i < ts_node_named_child_count(args); i++)
            positional += !kind_is(ts_node_named_child(args, i), "keyword_argument") &&
                          !kind_is(ts_node_named_child(args, i), "comment");
        return positional < 2;
    }
    TSNode parent = ts_node_parent(call);
    if (kind_is(parent, "comparison_operator") || kind_is(parent, "dictionary_splat"))
        return false;
    /* exec(code, globals(), d) stores in d. */
    if (kind_is(parent, "argument_list") && ts_node_named_child_count(parent) == 3 &&
        ts_node_eq(call, ts_node_named_child(parent, 1))) {
        const char *runner = node_text(index, field(ts_node_parent(parent), "function"));
        if (runner && (!strcmp(runner, "exec") || !strcmp(runner, "eval")))
            return false;
    }
    if (kind_is(parent, "subscript")) {
        TSNode user = ts_node_parent(parent), key = field(parent, "subscript");
        bool stored = (kind_is(user, "assignment") || kind_is(user, "augmented_assignment")) &&
                      ts_node_eq(parent, field(user, "left"));
        bool literal = kind_is(key, "string") && ts_node_named_child_count(key) == 3 &&
                       kind_is(ts_node_named_child(key, 1), "string_content");
        /* A computed key names no binding this audit saw. Names bound by statements stay what
         * those say, as celery's STATICA_HACK relies on; what the module lacks is unknowable. */
        if (stored && !literal) {
            index->python_namespace_open = true;
            return false;
        }
        return stored;
    }
    if (kind_is(parent, "attribute")) {
        const char *member = node_text(index, field(parent, "attribute"));
        return !member ||
               (strcmp(member, "get") && strcmp(member, "keys") && strcmp(member, "items") &&
                strcmp(member, "values") && strcmp(member, "copy"));
    }
    return true;
}

/* Module and class bodies expose their real namespace through locals()/vars().
 * A function's locals() is a snapshot: writing to it rebinds nothing, and vars(x)
 * reads x. Only the zero-argument form outside a function can write a binding;
 * any other read of those names passes the view on and stays unknown. */
static bool python_namespace_view(cbm_resolve_binding_index_t *index, TSNode node,
                                  const char *name) {
    if (strcmp(name, "locals") && strcmp(name, "vars"))
        return false;
    TSNode call = ts_node_parent(node);
    if (!kind_is(call, "call") || !ts_node_eq(node, field(call, "function")))
        return true;
    /* A read-only method of the view leaves the namespace as it was. */
    TSNode user = ts_node_parent(call);
    if (kind_is(user, "attribute") && ts_node_eq(call, field(user, "object"))) {
        const char *member = node_text(index, field(user, "attribute"));
        if (member &&
            (!strcmp(member, "get") || !strcmp(member, "keys") || !strcmp(member, "items") ||
             !strcmp(member, "values") || !strcmp(member, "copy")))
            return false;
    }
    bool in_function = index->current_scope && index->current_scope < index->scope_count &&
                       !index->scopes[index->current_scope].class_scope;
    TSNode args = field(call, "arguments");
    return !in_function && (ts_node_is_null(args) || ts_node_named_child_count(args) == 0);
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

/* A name bound by `if TYPE_CHECKING:` exists only for annotations; the
 * extractor keeps that distinction and this audit leaves such names blocked. */
static bool python_type_checking_import(cbm_resolve_binding_index_t *index, TSNode node) {
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        TSNode parent = ts_node_parent(node);
        if (ts_node_is_null(parent))
            return false;
        if (kind_is(parent, "if_statement") && ts_node_eq(node, field(parent, "consequence"))) {
            TSNode condition = field(parent, "condition");
            if (kind_is(condition, "attribute"))
                condition = field(condition, "attribute");
            const char *text =
                kind_is(condition, "identifier") ? node_text(index, condition) : NULL;
            if (text && !strcmp(text, "TYPE_CHECKING"))
                return true;
        }
        node = parent;
    }
    return false;
}

static const char *python_from_module(cbm_resolve_binding_index_t *index, TSNode node) {
    return node_text(index, field(node, "module_name"));
}

/* The same module-path spelling the extractor records for a top-level import. */
static void python_note_nested_import(cbm_resolve_binding_index_t *index, TSNode node, TSNode child,
                                      const char *local) {
    bool from = kind_is(node, "import_from_statement");
    TSNode alias = field(child, "alias");
    TSNode named = kind_is(child, "aliased_import") ? field(child, "name") : child;
    const char *spelled = node_text(index, named);
    const char *module = from ? python_from_module(index, node) : NULL;
    cbm_resolve_python_nested_import_t *items =
        grow_array(index, index->python_nested, index->python_nested_count,
                   &index->python_nested_capacity, sizeof(*items));
    if (!items || !spelled || (from && !module)) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_nested_import_allocation");
        return;
    }
    index->python_nested = items;
    size_t length = module ? strlen(module) : 0;
    const char *path = !from ? spelled
                       : length && module[length - 1] == '.'
                           ? cbm_arena_sprintf(index->arena, "%s%s", module, spelled)
                           : cbm_arena_sprintf(index->arena, "%s.%s", module, spelled);
    if (!path) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_nested_import_allocation");
        return;
    }
    cbm_resolve_python_nested_import_t *entry = &items[index->python_nested_count++];
    *entry = (cbm_resolve_python_nested_import_t){
        .import = {.local_name = local,
                   .module_path = path,
                   .kind = from                     ? CBM_IMPORT_KIND_PYTHON_FROM
                           : ts_node_is_null(alias) ? CBM_IMPORT_KIND_PYTHON_MODULE_ROOT
                                                    : CBM_IMPORT_KIND_PYTHON_MODULE_ALIAS,
                   .site_start_byte = ts_node_start_byte(child),
                   .site_end_byte = ts_node_end_byte(child),
                   .source_origin = CBM_SOURCE_ORIGIN_RAW},
        .at = ts_node_start_byte(child),
        .scope = index->current_scope};
    index->names[index->name_count - 1].nested_import = (uint32_t)index->python_nested_count;
}

static bool python_root_import_bound(const cbm_resolve_binding_index_t *index, const char *name) {
    for (size_t i = 0; name && i < index->name_count; i++)
        if (index->names[i].import_root && index->names[i].scope == index->current_scope &&
            !strcmp(index->names[i].name, name))
            return true;
    return false;
}

static void note_import(cbm_resolve_binding_index_t *index, TSNode node, bool nested);

/* Only annotation lookups see these names: runtime reads find a blocked binder. */
static void python_note_annotation_imports(cbm_resolve_binding_index_t *index, TSNode node) {
    size_t names = index->name_count, nested = index->python_nested_count;
    size_t stars = index->python_star_count;
    note_import(index, node, true);
    for (size_t i = names; i < index->name_count; i++)
        index->names[i].flags = BINDING_BLOCKED;
    for (size_t i = nested; i < index->python_nested_count; i++)
        index->python_nested[i].annotation_only = true;
    for (size_t i = stars; i < index->python_star_count; i++)
        index->python_stars[i].annotation_only = true;
}

static void note_import(cbm_resolve_binding_index_t *index, TSNode node, bool nested) {
    bool from = kind_is(node, "import_from_statement");
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (from && kind_is(child, "wildcard_import")) {
            index->python_wildcard_barrier = ts_node_end_byte(node);
            cbm_resolve_python_star_t *stars =
                grow_array(index, index->python_stars, index->python_star_count,
                           &index->python_star_capacity, sizeof(*stars));
            const char *module = python_from_module(index, node);
            if (!stars || !module) {
                fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_star_allocation");
                return;
            }
            index->python_stars = stars;
            stars[index->python_star_count++] =
                (cbm_resolve_python_star_t){.module_path = module, .at = ts_node_end_byte(node)};
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
        /* `import a.b` and `import a.c` bind the one package a, not two values. */
        if (!from && ts_node_is_null(alias) && python_root_import_bound(index, name))
            continue;
        size_t before = index->name_count;
        add_name(index, name, ts_node_start_byte(node), BINDING_IMPORT);
        if (!from && ts_node_is_null(alias) && index->name_count > before)
            index->names[index->name_count - 1].import_root = true;
        if (nested && index->name_count > before)
            python_note_nested_import(index, node, child, name);
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

/* `global x` and `nonlocal x` make x's enclosing binding writable from here.
 * That invalidates x, not the file: block exactly the listed names in the
 * scopes they can rebind, including this one. Lookup through any of them then
 * abstains, while every unrelated binding keeps its proof. */
static void python_note_rebinding_scope(cbm_resolve_binding_index_t *index, TSNode node,
                                        bool global) {
    uint32_t count = ts_node_named_child_count(node), declaring = index->current_scope;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!kind_is(child, "identifier"))
            continue;
        const char *name = node_text(index, child);
        add_name(index, name, ts_node_start_byte(node), BINDING_BLOCKED);
        uint32_t scope = declaring;
        for (unsigned depth = 0; scope && depth < BINDING_MAX_DEPTH; depth++) {
            scope = index->scopes[scope].parent;
            if (!global || !scope) {
                index->current_scope = scope;
                add_name(index, name, ts_node_start_byte(node), BINDING_BLOCKED);
            }
        }
        index->current_scope = declaring;
    }
}

/* `sys.modules[__name__]` hands out this module's own namespace object. That
 * is the one way the dynamic-attribute builtins reach a binding audited here
 * without a module import escaping. hasattr/getattr and a plain attribute read
 * only look at it. */
static bool python_own_module_entry(cbm_resolve_binding_index_t *index, TSNode subscript) {
    TSNode value = field(subscript, "value");
    if (!kind_is(value, "attribute"))
        return false;
    const char *member = node_text(index, field(value, "attribute"));
    if (!member || strcmp(member, "modules"))
        return false;
    TSNode key = field(subscript, "subscript");
    const char *text = kind_is(key, "identifier") ? node_text(index, key) : NULL;
    if (!text || strcmp(text, "__name__"))
        return false;
    TSNode parent = ts_node_parent(subscript);
    if (kind_is(parent, "attribute")) {
        TSNode user = ts_node_parent(parent);
        return kind_is(user, "assignment") && ts_node_eq(parent, field(user, "left"));
    }
    if (kind_is(parent, "argument_list")) {
        TSNode call = ts_node_parent(parent);
        const char *callee = node_text(index, field(call, "function"));
        return !callee || (strcmp(callee, "hasattr") && strcmp(callee, "getattr"));
    }
    return true;
}

/* The class annotation names, for the supported annotation forms: a dotted
 * name, its string spelling, and Optional[...] around one of those. */
static const char *python_import_canonical(const cbm_resolve_binding_index_t *index,
                                           const char *chain);
static const char *python_annotation_chain(cbm_resolve_binding_index_t *index, TSNode node,
                                           unsigned depth);

static bool python_annotation_is_none(TSNode node) {
    while (kind_is(node, "type") && ts_node_named_child_count(node) == 1)
        node = ts_node_named_child(node, 0);
    return kind_is(node, "none");
}

/* `X | None` says X. Two classes say neither. */
/* What a generic annotation says a value is. Its marker: @seq holds and yields X, @map holds
 * X by key, @type is the class X itself. A generic of the project's own is an instance of it. */
static const char *python_generic_chain(cbm_resolve_binding_index_t *index, TSNode head,
                                        const TSNode *args, uint32_t count, unsigned depth) {
    static const char *const seq[] = {"list",
                                      "set",
                                      "frozenset",
                                      "typing.List",
                                      "typing.Set",
                                      "typing.FrozenSet",
                                      "typing.Sequence",
                                      "typing.MutableSequence",
                                      "typing.Iterable",
                                      "typing.Iterator",
                                      "typing.Collection",
                                      "typing.Generator",
                                      "typing.AsyncIterator",
                                      "typing.AsyncIterable",
                                      "typing.AsyncGenerator",
                                      "typing.Deque",
                                      "typing.Reversible",
                                      "typing.AbstractSet",
                                      "typing.MutableSet",
                                      "collections.abc.Sequence",
                                      "collections.abc.MutableSequence",
                                      "collections.abc.Iterable",
                                      "collections.abc.Iterator",
                                      "collections.abc.Collection",
                                      "collections.abc.Generator",
                                      "collections.abc.AsyncIterator",
                                      "collections.abc.AsyncIterable",
                                      "collections.abc.AsyncGenerator",
                                      "collections.abc.Set",
                                      "collections.abc.MutableSet",
                                      "collections.deque",
                                      NULL};
    static const char *const map[] = {"dict",
                                      "typing.Dict",
                                      "typing.Mapping",
                                      "typing.MutableMapping",
                                      "typing.DefaultDict",
                                      "typing.OrderedDict",
                                      "collections.abc.Mapping",
                                      "collections.abc.MutableMapping",
                                      "collections.OrderedDict",
                                      "collections.defaultdict",
                                      NULL};
    static const char *const wrapper[] = {
        "typing.Optional",           "typing.Annotated",
        "typing.ClassVar",           "typing.Final",
        "typing.Awaitable",          "typing_extensions.Annotated",
        "collections.abc.Awaitable", NULL};
    const char *spelled = python_chain(index, head, 0);
    if (!spelled || !count)
        return NULL;
    const char *canonical = python_import_canonical(index, spelled);
    if (!canonical)
        canonical = spelled;
    const char *marker = NULL;
    uint32_t arg = 0;
    if (listed(canonical, wrapper)) {
        return python_annotation_chain(index, args[0], depth + 1);
    } else if (!strcmp(canonical, "typing.Coroutine") ||
               !strcmp(canonical, "collections.abc.Coroutine")) {
        return count == 3 ? python_annotation_chain(index, args[2], depth + 1) : NULL;
    } else if (listed(canonical, seq)) {
        marker = "@seq";
    } else if (!strcmp(canonical, "tuple") || !strcmp(canonical, "typing.Tuple")) {
        if (count != 2 || !kind_is(args[1], "type") || !ts_node_named_child_count(args[1]) ||
            !kind_is(ts_node_named_child(args[1], 0), "ellipsis"))
            return NULL;
        marker = "@seq";
    } else if (listed(canonical, map)) {
        if (count != 2)
            return NULL;
        marker = "@map";
        arg = 1;
    } else if (!strcmp(canonical, "type") || !strcmp(canonical, "typing.Type")) {
        marker = "@type";
    } else if (!strncmp(canonical, "typing.", 7) || !strncmp(canonical, "typing_extensions.", 18) ||
               !strncmp(canonical, "collections.", 12)) {
        return NULL;
    } else {
        return spelled; /* Repo[str] is a Repo. */
    }
    const char *element = python_annotation_chain(index, args[arg], depth + 1);
    /* One layer: list[list[X]] says nothing this model can carry. */
    return element && !strchr(element, '@')
               ? cbm_arena_sprintf(index->arena, "%s%s", element, marker)
               : NULL;
}

static const char *python_union_chain(cbm_resolve_binding_index_t *index, TSNode left, TSNode right,
                                      unsigned depth) {
    bool left_none = python_annotation_is_none(left), right_none = python_annotation_is_none(right);
    if (left_none == right_none)
        return NULL;
    return python_annotation_chain(index, left_none ? right : left, depth + 1);
}

static const char *python_annotation_chain(cbm_resolve_binding_index_t *index, TSNode node,
                                           unsigned depth) {
    if (ts_node_is_null(node) || depth > 8)
        return NULL;
    if (kind_is(node, "type") && ts_node_named_child_count(node) == 1)
        return python_annotation_chain(index, ts_node_named_child(node, 0), depth + 1);
    if (kind_is(node, "identifier") || kind_is(node, "attribute"))
        return python_chain(index, node, 0);
    if (kind_is(node, "string")) {
        const char *text = node_text(index, node);
        size_t length = text ? strlen(text) : 0;
        if (length < 3 || (text[0] != '"' && text[0] != '\'') || text[length - 1] != text[0])
            return NULL;
        for (size_t i = 1; i + 1 < length; i++)
            if (!(text[i] == '.' || text[i] == '_' || (text[i] >= '0' && text[i] <= '9') ||
                  (text[i] >= 'a' && text[i] <= 'z') || (text[i] >= 'A' && text[i] <= 'Z')))
                return NULL;
        return cbm_arena_strndup(index->arena, text + 1, length - 2);
    }
    if (kind_is(node, "generic_type") && ts_node_named_child_count(node) == 2) {
        TSNode arguments = ts_node_named_child(node, 1), args[3];
        memset(args, 0, sizeof(args));
        uint32_t count = ts_node_named_child_count(arguments);
        for (uint32_t i = 0; i < count && i < 3; i++)
            args[i] = ts_node_named_child(arguments, i);
        return count <= 3
                   ? python_generic_chain(index, ts_node_named_child(node, 0), args, count, depth)
                   : NULL;
    }
    /* The grammar reads a dotted generic, typing.Optional[X], as a subscript. */
    if (kind_is(node, "subscript")) {
        TSNode args[3];
        memset(args, 0, sizeof(args));
        uint32_t count = ts_node_named_child_count(node), used = 0;
        for (uint32_t i = 1; i < count && used < 3; i++)
            args[used++] = ts_node_named_child(node, i);
        return count >= 2 && count <= 4
                   ? python_generic_chain(index, field(node, "value"), args, used, depth)
                   : NULL;
    }
    if (kind_is(node, "union_type") && ts_node_named_child_count(node) == 2)
        return python_union_chain(index, ts_node_named_child(node, 0), ts_node_named_child(node, 1),
                                  depth);
    if (kind_is(node, "binary_operator")) {
        const char *operator= node_text(index, field(node, "operator"));
        return operator&& !strcmp(operator, "|")
                   ? python_union_chain(index, field(node, "left"), field(node, "right"), depth)
                   : NULL;
    }
    return NULL;
}

/* The top-level imports the extractor recorded, joined with the rest of a chain:
 * `functools.wraps` for both `import functools; functools.wraps` and
 * `from functools import wraps`. NULL when the root is not such an import. */
static const char *python_import_canonical(const cbm_resolve_binding_index_t *index,
                                           const char *chain) {
    size_t root = strcspn(chain, ".(");
    for (int i = 0; i < index->result->imports.count; i++) {
        const CBMImport *import = &index->result->imports.items[i];
        if (!import->local_name || !import->module_path || strlen(import->local_name) != root ||
            strncmp(import->local_name, chain, root))
            continue;
        if (import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT &&
            strcmp(import->module_path, import->local_name))
            return cbm_arena_strdup(index->arena, chain);
        return cbm_arena_sprintf(index->arena, "%s%s", import->module_path, chain + root);
    }
    return NULL;
}

/* Every path through the block ends in return or raise: it cannot fall off the
 * end, which would rebind the decorated name to None. */
static bool python_block_always_exits(TSNode block, unsigned depth) {
    if (ts_node_is_null(block) || depth > 16)
        return false;
    TSNode last = {0};
    for (uint32_t i = ts_node_named_child_count(block); i-- > 0;) {
        TSNode child = ts_node_named_child(block, i);
        if (!kind_is(child, "comment")) {
            last = child;
            break;
        }
    }
    if (kind_is(last, "return_statement") || kind_is(last, "raise_statement"))
        return true;
    if (!kind_is(last, "if_statement"))
        return false;
    if (!python_block_always_exits(field(last, "consequence"), depth + 1))
        return false;
    bool has_else = false;
    for (uint32_t i = 0; i < ts_node_named_child_count(last); i++) {
        TSNode branch = ts_node_named_child(last, i);
        if (kind_is(branch, "elif_clause") &&
            !python_block_always_exits(field(branch, "consequence"), depth + 1))
            return false;
        if (kind_is(branch, "else_clause")) {
            has_else = true;
            if (!python_block_always_exits(field(branch, "body"), depth + 1))
                return false;
        }
    }
    return has_else;
}

/* Every value this function returns, outside nested scopes. A bare return and
 * None say nothing about the class of the others. Returns false when one is not
 * an access path, or the function yields. */
static bool python_gather_returns(cbm_resolve_binding_index_t *index, TSNode node, const char **out,
                                  uint32_t *count, uint32_t room, unsigned depth) {
    if (depth > 48)
        return false;
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        if (kind_is(child, "function_definition") || kind_is(child, "class_definition") ||
            kind_is(child, "decorated_definition") || kind_is(child, "lambda"))
            continue;
        if (kind_is(child, "yield"))
            return false;
        if (kind_is(child, "return_statement")) {
            if (!ts_node_named_child_count(child))
                continue;
            TSNode value = ts_node_named_child(child, 0);
            if (kind_is(value, "none"))
                continue;
            const char *chain = python_chain(index, value, 0);
            if (!chain || *count >= room)
                return false;
            out[(*count)++] = chain;
            continue;
        }
        if (!python_gather_returns(index, child, out, count, room, depth + 1))
            return false;
    }
    return true;
}

/* Called once the function's own scope exists: its locals resolve the returns. */
static void python_note_returns(cbm_resolve_binding_index_t *index, TSNode function, size_t member,
                                uint32_t scope) {
    enum { RETURNS_MAX = 16 };
    const char *chains[RETURNS_MAX];
    uint32_t count = 0;
    /* `-> Self` and `def f(self: T) -> T` return the receiver, whichever class that is. */
    TSNode declared_return = field(function, "return_type");
    binding_scope_t *body_scope = scope < index->scope_count ? &index->scopes[scope] : NULL;
    if (member < index->python_member_count && !ts_node_is_null(declared_return) && body_scope &&
        body_scope->self_name) {
        const char *declared = python_annotation_chain(index, declared_return, 0);
        const char *canonical = declared ? python_import_canonical(index, declared) : NULL;
        TSNode parameters = field(function, "parameters");
        TSNode first = ts_node_named_child_count(parameters) ? ts_node_named_child(parameters, 0)
                                                             : (TSNode){0};
        const char *first_type = kind_is(first, "typed_parameter")
                                     ? python_annotation_chain(index, field(first, "type"), 0)
                                     : NULL;
        if (declared && ((canonical && (!strcmp(canonical, "typing.Self") ||
                                        !strcmp(canonical, "typing_extensions.Self"))) ||
                         (first_type && !strcmp(first_type, declared)))) {
            const char **kept = cbm_arena_alloc(index->arena, 2 * sizeof(*kept));
            if (!kept) {
                fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_member_allocation");
                return;
            }
            kept[0] = body_scope->self_is_class
                          ? cbm_arena_sprintf(index->arena, "%s()", body_scope->self_name)
                          : body_scope->self_name;
            kept[1] = NULL;
            index->python_members[member].return_chain = NULL;
            index->python_members[member].returned = kept;
            index->python_members[member].returned_scope = scope;
            return;
        }
    }
    /* What __new__ and friends answer with decides whether they defer to the base, whatever
     * they are annotated to return. */
    const char *name = node_text(index, field(function, "name"));
    bool special = name && (!strcmp(name, "__new__") || !strcmp(name, "__getattribute__") ||
                            !strcmp(name, "__class_getitem__"));
    if (member >= index->python_member_count ||
        (!special && !ts_node_is_null(field(function, "return_type"))) ||
        !python_gather_returns(index, field(function, "body"), chains, &count, RETURNS_MAX, 0) ||
        !count)
        return;
    const char **kept = cbm_arena_alloc(index->arena, (count + 1) * sizeof(*kept));
    if (!kept) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_member_allocation");
        return;
    }
    memcpy(kept, chains, count * sizeof(*kept));
    kept[count] = NULL;
    index->python_members[member].returned = kept;
    index->python_members[member].returned_scope = scope;
}

/* A definition under decorators: its name, until they are proven, is blocked. */
static void python_note_decorated(cbm_resolve_binding_index_t *index, TSNode definition,
                                  const char *target, const char *name, bool is_class) {
    TSNode decorated = ts_node_parent(definition);
    uint32_t count = ts_node_named_child_count(decorated);
    const char **chains = cbm_arena_alloc(index->arena, (count + 1) * sizeof(*chains));
    const char **canonical = cbm_arena_alloc(index->arena, (count + 1) * sizeof(*canonical));
    cbm_resolve_python_decorated_t *items =
        grow_array(index, index->python_decorated, index->python_decorated_count,
                   &index->python_decorated_capacity, sizeof(*items));
    if (!chains || !canonical || !items) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_decorator_allocation");
        return;
    }
    index->python_decorated = items;
    uint32_t used = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(decorated, i);
        if (!kind_is(child, "decorator"))
            continue;
        const char *chain = ts_node_named_child_count(child)
                                ? python_chain(index, ts_node_named_child(child, 0), 0)
                                : NULL;
        chains[used] = chain ? chain : "?";
        canonical[used] = chain ? python_import_canonical(index, chain) : NULL;
        used++;
    }
    chains[used] = NULL;
    canonical[used] = NULL;
    items[index->python_decorated_count++] =
        (cbm_resolve_python_decorated_t){.target = target,
                                         .name = name,
                                         .scope = index->current_scope,
                                         .is_class = is_class,
                                         .decorators = chains,
                                         .canonical = canonical};
}

static const char *python_decorator_chain(cbm_resolve_binding_index_t *index, TSNode decorator) {
    TSNode value =
        ts_node_named_child_count(decorator) ? ts_node_named_child(decorator, 0) : (TSNode){0};
    return python_chain(index, value, 0);
}

static bool python_overload_stub(cbm_resolve_binding_index_t *index, TSNode decorated) {
    uint32_t count = ts_node_named_child_count(decorated);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(decorated, i);
        if (!kind_is(child, "decorator") || !ts_node_named_child_count(child))
            continue;
        const char *chain = python_chain(index, ts_node_named_child(child, 0), 0);
        size_t length = chain ? strlen(chain) : 0;
        if (length < 8 || strcmp(chain + length - 8, "overload"))
            continue;
        const char *canonical = python_import_canonical(index, chain);
        if (canonical && (!strcmp(canonical, "typing.overload") ||
                          !strcmp(canonical, "typing_extensions.overload")))
            return true;
    }
    return false;
}

static void python_finish_narrows(cbm_resolve_binding_index_t *index);

static void python_add_narrow(cbm_resolve_binding_index_t *index, TSNode test, uint32_t from,
                              uint32_t until) {
    TSNode function = field(test, "function"), args = field(test, "arguments");
    const char *callee = kind_is(test, "call") && kind_is(function, "identifier")
                             ? node_text(index, function)
                             : NULL;
    if (!callee || strcmp(callee, "isinstance") || ts_node_is_null(args) ||
        ts_node_named_child_count(args) != 2 || until <= from)
        return;
    TSNode kind = ts_node_named_child(args, 1);
    const char *chain = python_chain(index, ts_node_named_child(args, 0), 0);
    /* A tuple of classes says only that it is one of them. */
    const char *type = kind_is(kind, "identifier") || kind_is(kind, "attribute")
                           ? python_chain(index, kind, 0)
                           : NULL;
    /* A call or subscript computes a new value each time. */
    if (!chain || !type || strpbrk(chain, "([@"))
        return;
    python_narrow_t *items = grow_array(index, index->python_narrows, index->python_narrow_count,
                                        &index->python_narrow_capacity, sizeof(*items));
    if (!items) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_narrow_allocation");
        return;
    }
    index->python_narrows = items;
    items[index->python_narrow_count++] = (python_narrow_t){
        .chain = chain, .type = type, .from = from, .until = until, .scope = index->current_scope};
}

/* Each conjunct of `a and b` holds in what follows it, up to the end of the block it guards. */
static void python_note_condition(cbm_resolve_binding_index_t *index, TSNode condition,
                                  uint32_t until, unsigned depth) {
    if (kind_is(condition, "parenthesized_expression") && ts_node_named_child_count(condition) == 1)
        condition = ts_node_named_child(condition, 0);
    const char *operator= kind_is(condition, "boolean_operator")
        ? node_text(index, field(condition, "operator"))
        : NULL;
    if (operator&& !strcmp(operator, "and") && depth < 8) {
        python_note_condition(index, field(condition, "left"), until, depth + 1);
        python_note_condition(index, field(condition, "right"), until, depth + 1);
        return;
    }
    python_add_narrow(index, condition, ts_node_end_byte(condition), until);
}

static uint32_t python_block_end(TSNode statement);

/* isinstance as if and elif conditions, as `if not isinstance: leave` and as an assert. */
static void python_note_narrowing(cbm_resolve_binding_index_t *index, TSNode node,
                                  const char *kind) {
    if (!strcmp(kind, "assert_statement")) {
        TSNode test = ts_node_named_child_count(node) ? ts_node_named_child(node, 0) : (TSNode){0};
        python_add_narrow(index, test, ts_node_end_byte(node), python_block_end(node));
        return;
    }
    TSNode condition = field(node, "condition"), body = field(node, "consequence");
    if (ts_node_is_null(condition) || ts_node_is_null(body))
        return;
    if (kind_is(condition, "not_operator")) {
        /* The other way to reach what follows left the block. */
        if (!strcmp(kind, "if_statement") && python_block_always_exits(body, 0) &&
            ts_node_is_null(field(node, "alternative")))
            python_add_narrow(index, field(condition, "argument"), ts_node_end_byte(node),
                              python_block_end(node));
        return;
    }
    python_note_condition(index, condition, ts_node_end_byte(body), 0);
}

/* Where the block holding this statement ends. Later statements of a block run only after
 * this one. A def or lambda in between means the statement is not in such a block. */
static uint32_t python_block_end(TSNode statement) {
    TSNode node = statement;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        node = ts_node_parent(node);
        if (ts_node_is_null(node) || kind_is(node, "lambda"))
            return 0;
        if (kind_is(node, "block") || kind_is(node, "module"))
            return ts_node_end_byte(node);
    }
    return 0;
}

/* `with X() as y`: y holds what __enter__ returns, from there to the end of the with
 * statement's block. `except E as e`: e holds an E, for that clause only. */
static void python_note_as_target(cbm_resolve_binding_index_t *index, binding_name_t *name,
                                  TSNode identifier) {
    TSNode pattern = ts_node_parent(ts_node_parent(identifier));
    TSNode holder = ts_node_parent(pattern);
    if (!kind_is(pattern, "as_pattern") || !ts_node_named_child_count(pattern))
        return;
    const char *chain = python_chain(index, ts_node_named_child(pattern, 0), 0);
    name->value_from = ts_node_end_byte(pattern);
    if (kind_is(holder, "except_clause") || kind_is(holder, "except_group_clause")) {
        name->value_chain = chain;
        name->value_kind = CBM_PY_ROOT_TYPED;
        name->value_until = ts_node_end_byte(holder);
    } else if (kind_is(holder, "with_item")) {
        TSNode statement = ts_node_parent(ts_node_parent(holder));
        bool awaited = kind_is(statement, "with_statement") && ts_node_child_count(statement) &&
                       kind_is(ts_node_child(statement, 0), "async");
        name->value_chain = chain ? cbm_arena_sprintf(index->arena, "%s.%s()", chain,
                                                      awaited ? "__aenter__" : "__enter__")
                                  : NULL;
        name->value_kind = CBM_PY_ROOT_ASSIGNED;
        name->value_until = python_block_end(statement);
    }
}

/* One name of a tuple a loop unpacks, when the iterable says what that position holds. */
static void python_note_unpacked_loop_target(cbm_resolve_binding_index_t *index,
                                             binding_name_t *name, TSNode identifier) {
    TSNode pattern = ts_node_parent(identifier), loop = ts_node_parent(pattern);
    if ((!kind_is(loop, "for_statement") && !kind_is(loop, "for_in_clause")) ||
        !ts_node_eq(pattern, field(loop, "left")))
        return;
    uint32_t position = 0, count = ts_node_named_child_count(pattern);
    bool located = false;
    TSTreeCursor cursor = ts_tree_cursor_new(pattern);
    for (bool more = ts_tree_cursor_goto_first_child(&cursor); more && !located;
         more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode child = ts_tree_cursor_current_node(&cursor);
        if (ts_node_eq(child, identifier))
            located = true;
        else if (ts_node_is_named(child))
            position++;
    }
    ts_tree_cursor_delete(&cursor);
    TSNode right = field(loop, "right");
    TSNode function = field(right, "function"), args = field(right, "arguments");
    if (!located || !kind_is(right, "call") || ts_node_is_null(args))
        return;
    uint32_t supplied = ts_node_named_child_count(args);
    const char *value = NULL;
    if (kind_is(function, "attribute")) {
        /* d.items() against an annotated map: the second is the value. */
        const char *member = node_text(index, field(function, "attribute"));
        const char *object = python_chain(index, field(function, "object"), 0);
        if (member && !strcmp(member, "items") && object && count == 2 && position == 1)
            value = cbm_arena_sprintf(index->arena, "%s@values@iter", object);
    } else if (kind_is(function, "identifier")) {
        const char *callee = node_text(index, function);
        const char *source = NULL;
        if (callee && !strcmp(callee, "enumerate") && count == 2 && position == 1 && supplied >= 1)
            source = python_chain(index, ts_node_named_child(args, 0), 0);
        else if (callee && !strcmp(callee, "zip") && supplied == count)
            source = python_chain(index, ts_node_named_child(args, position), 0);
        /* The name is checked to be the builtin when this is read. */
        value = source ? cbm_arena_sprintf(index->arena, "%s(%s)@iter", callee, source) : NULL;
    }
    if (!value)
        return;
    name->value_chain = value;
    name->value_kind = CBM_PY_ROOT_ASSIGNED;
    name->value_from = ts_node_end_byte(right);
    TSNode body = kind_is(loop, "for_statement") ? field(loop, "body") : ts_node_parent(loop);
    name->value_until = ts_node_is_null(body) ? 0 : ts_node_end_byte(body);
}

/* What the name just recorded was bound to, when that is statically followable.
 * Lookup uses it only while this stays the scope's one binder of the name. */
static void python_note_binder_value(cbm_resolve_binding_index_t *index, TSNode identifier,
                                     const char *parent, const char *field_name) {
    binding_name_t *name = &index->names[index->name_count - 1];
    uint32_t scope = index->current_scope;
    name->value_scope = scope;
    if (parent && !strcmp(parent, "as_pattern_target")) {
        python_note_as_target(index, name, identifier);
        return;
    }

    if (parent &&
        (!strcmp(parent, "parameters") || !strcmp(parent, "typed_parameter") ||
         !strcmp(parent, "typed_default_parameter") || !strcmp(parent, "default_parameter")))
        name->value_until = UINT32_MAX;
    /* `def make(self, Request=Request)`: what the signature says it holds unless told
     * otherwise, read where the def stands. */
    if (parent && !strcmp(parent, "default_parameter") && field_name &&
        !strcmp(field_name, "name") && scope) {
        name->value_chain = python_chain(index, field(ts_node_parent(identifier), "value"), 0);
        name->value_kind = name->value_chain ? CBM_PY_ROOT_ASSIGNED : name->value_kind;
        name->value_scope = index->scopes[scope].method_of ? index->scopes[scope].method_of - 1
                                                           : index->scopes[scope].parent;
        return;
    }
    /* `for k, v in d.items()`, `for i, x in enumerate(E)`, `for a, b in zip(A, B)`. */
    if (parent && !strcmp(parent, "pattern_list")) {
        python_note_unpacked_loop_target(index, name, identifier);
        return;
    }
    /* `if (m := Foo()):` */
    if (parent && !strcmp(parent, "named_expression")) {
        name->value_chain = python_chain(index, field(ts_node_parent(identifier), "value"), 0);
        name->value_kind = name->value_chain ? CBM_PY_ROOT_ASSIGNED : name->value_kind;
        return;
    }
    /* `for x in E`: x holds what E yields, for the loop's body. */
    if (parent && (!strcmp(parent, "for_statement") || !strcmp(parent, "for_in_clause")) &&
        field_name && !strcmp(field_name, "left")) {
        TSNode loop = ts_node_parent(identifier), right = field(loop, "right");
        const char *chain = python_chain(index, right, 0);
        TSNode body = !strcmp(parent, "for_statement") ? field(loop, "body") : ts_node_parent(loop);
        name->value_chain = chain ? cbm_arena_sprintf(index->arena, "%s@iter", chain) : NULL;
        name->value_kind = name->value_chain ? CBM_PY_ROOT_ASSIGNED : name->value_kind;
        name->value_from = ts_node_end_byte(right);
        name->value_until = ts_node_is_null(body) ? 0 : ts_node_end_byte(body);
        return;
    }
    if (parent && !strcmp(parent, "assignment") && field_name && !strcmp(field_name, "left")) {
        TSNode assignment = ts_node_parent(identifier);
        TSNode type = field(assignment, "type"), right = field(assignment, "right");
        name->value_from = ts_node_end_byte(assignment);
        name->value_until = python_block_end(assignment);
        if (!ts_node_is_null(type)) {
            name->value_chain = python_annotation_chain(index, type, 0);
            name->value_kind = CBM_PY_ROOT_TYPED;
        } else if (!ts_node_is_null(right)) {
            name->value_chain = python_chain(index, right, 0);
            name->value_kind = CBM_PY_ROOT_ASSIGNED;
            name->value_none = kind_is(right, "none");
        }
        if (name->value_chain && scope && index->scopes[scope].class_scope)
            name->value_scope = scope;
        return;
    }
    if (scope && parent && !strcmp(parent, "parameters") && index->scopes[scope].self_name &&
        !strcmp(index->scopes[scope].self_name, name->name) && index->scopes[scope].method_of) {
        name->value_kind = index->scopes[scope].self_is_class ? CBM_PY_ROOT_CLASS_PARAMETER
                                                              : CBM_PY_ROOT_SELF_PARAMETER;
        return;
    }
    if (parent &&
        (!strcmp(parent, "typed_parameter") || !strcmp(parent, "typed_default_parameter"))) {
        TSNode parameter = ts_node_parent(identifier);
        if (scope && index->scopes[scope].self_name && index->scopes[scope].method_of &&
            !strcmp(index->scopes[scope].self_name, name->name)) {
            name->value_kind = index->scopes[scope].self_is_class ? CBM_PY_ROOT_CLASS_PARAMETER
                                                                  : CBM_PY_ROOT_SELF_PARAMETER;
            return;
        }
        name->value_chain = python_annotation_chain(index, field(parameter, "type"), 0);
        name->value_kind = CBM_PY_ROOT_TYPED;
        /* Annotations evaluate in the namespace that declares the function. */
        name->value_scope = index->scopes[scope].method_of ? index->scopes[scope].method_of - 1
                                                           : index->scopes[scope].parent;
    }
}

static cbm_resolve_python_member_t *python_add_member(cbm_resolve_binding_index_t *index,
                                                      uint32_t owner, const char *name,
                                                      cbm_resolve_python_member_kind_t kind,
                                                      uint32_t at) {
    cbm_resolve_python_member_t *members =
        grow_array(index, index->python_members, index->python_member_count,
                   &index->python_member_capacity, sizeof(*members));
    if (!members || !name)
        return NULL;
    index->python_members = members;
    const char *qualified = index->scopes[owner].qualified_name;
    cbm_resolve_python_member_t *entry = &members[index->python_member_count++];
    *entry = (cbm_resolve_python_member_t){
        .owner_scope = owner,
        .owner = qualified,
        .name = name,
        .target = cbm_arena_sprintf(index->arena, "%s.%s", qualified, name),
        .kind = kind,
        .at = at};
    return entry->target ? entry : NULL;
}

static bool python_constant_value(TSNode value) {
    return kind_is(value, "integer") || kind_is(value, "float") || kind_is(value, "string") ||
           kind_is(value, "concatenated_string") || kind_is(value, "true") ||
           kind_is(value, "false") || kind_is(value, "none") || kind_is(value, "list") ||
           kind_is(value, "dictionary") || kind_is(value, "tuple") || kind_is(value, "set");
}

/* This body can only hand back a constant or nothing: a decorator like that discards the
 * function. A stub says nothing, and neither does a body that only raises. The walk stays
 * out of nested scopes, so across a file each node is visited once per enclosing def. */
static bool python_body_discards(TSNode body) {
    bool returned = false;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    bool discards = true, more = ts_tree_cursor_goto_first_child(&cursor);
    unsigned depth = 1;
    while (more) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        bool nested = kind_is(node, "function_definition") || kind_is(node, "class_definition") ||
                      kind_is(node, "decorated_definition") || kind_is(node, "lambda");
        if (kind_is(node, "yield")) {
            /* A generator returns a generator, whatever its return says. */
            ts_tree_cursor_delete(&cursor);
            return true;
        }
        if (kind_is(node, "return_statement")) {
            returned = true;
            if (ts_node_named_child_count(node) &&
                !python_constant_value(ts_node_named_child(node, 0))) {
                discards = false;
                break;
            }
        }
        if (!nested && !kind_is(node, "return_statement") &&
            ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (--depth == 0 || !ts_tree_cursor_goto_parent(&cursor)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cursor);
    if (!discards || returned)
        return discards;
    if (python_block_always_exits(body, 0))
        return false;
    uint32_t count = ts_node_named_child_count(body);
    TSNode last = count ? ts_node_named_child(body, count - 1) : (TSNode){0};
    return !(kind_is(last, "expression_statement") && ts_node_named_child_count(last) &&
             kind_is(ts_node_named_child(last, 0), "ellipsis"));
}

/* The name of a def's first parameter, when it has one a body could spell. */
static const char *python_first_parameter(cbm_resolve_binding_index_t *index, TSNode function) {
    TSNode parameters = field(function, "parameters");
    if (ts_node_is_null(parameters) || !ts_node_named_child_count(parameters))
        return NULL;
    TSNode first = ts_node_named_child(parameters, 0);
    if (kind_is(first, "typed_parameter") && ts_node_named_child_count(first))
        first = ts_node_named_child(first, 0);
    else if (kind_is(first, "default_parameter") || kind_is(first, "typed_default_parameter"))
        first = field(first, "name");
    return kind_is(first, "identifier") ? node_text(index, first) : NULL;
}

static bool python_spells(const cbm_resolve_binding_index_t *index, TSNode node, const char *word) {
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    size_t length = strlen(word);
    return end - start == length && end <= index->source_len &&
           !memcmp(index->source + start, word, length);
}

/* `def replace(function): return other`. A body that returns a value and never spells its
 * parameter cannot have built that value from the function. locals() and the like could reach
 * it by another name. Stops at the first mention, and only top-level and nested defs get here,
 * so across a file each node is visited once per enclosing def. */
static bool python_body_replaces(cbm_resolve_binding_index_t *index, TSNode function) {
    const char *parameter = python_first_parameter(index, function);
    TSNode body = field(function, "body");
    if (!parameter || ts_node_is_null(body))
        return false;
    bool returns = false, mentioned = false;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    unsigned depth = 1, nested = 0;
    while (more && !mentioned) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (kind_is(node, "identifier"))
            mentioned = python_spells(index, node, parameter) ||
                        python_spells(index, node, "locals") ||
                        python_spells(index, node, "vars") || python_spells(index, node, "eval") ||
                        python_spells(index, node, "exec");
        returns = returns ||
                  (!nested && kind_is(node, "return_statement") && ts_node_named_child_count(node));
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            nested += kind_is(node, "function_definition") || kind_is(node, "lambda");
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (--depth == 0 || !ts_tree_cursor_goto_parent(&cursor)) {
                more = false;
                break;
            }
            TSNode up = ts_tree_cursor_current_node(&cursor);
            nested -= nested && (kind_is(up, "function_definition") || kind_is(up, "lambda"));
        }
    }
    ts_tree_cursor_delete(&cursor);
    return returns && !mentioned;
}

static void python_note_member_shape(cbm_resolve_binding_index_t *index,
                                     cbm_resolve_python_member_t *member, TSNode function) {
    member->discards = python_body_discards(field(function, "body"));
    member->replaces = !member->discards && python_body_replaces(index, function);
}

/* A def directly in a class body. Its decorators are classified with the file's
 * imports once they are all known. */
static void python_note_method(cbm_resolve_binding_index_t *index, TSNode node, const char *parent,
                               uint32_t owner, bool conditional) {
    cbm_resolve_python_member_t *member = python_add_member(
        index, owner, node_text(index, field(node, "name")),
        conditional ? CBM_PY_MEMBER_OPAQUE : CBM_PY_MEMBER_FUNCTION, ts_node_start_byte(node));
    if (!member) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_member_allocation");
        return;
    }
    member->return_chain = python_annotation_chain(index, field(node, "return_type"), 0);
    member->chain_scope = owner;
    if (!index->scopes[owner].class_scope)
        python_note_member_shape(index, member, node);
    if (!parent || strcmp(parent, "decorated_definition"))
        return;
    TSNode decorated = ts_node_parent(node);
    uint32_t count = ts_node_named_child_count(decorated);
    const char **chains = cbm_arena_alloc(index->arena, (count + 1) * sizeof(*chains));
    if (!chains) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_member_allocation");
        return;
    }
    uint32_t used = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(decorated, i);
        if (!kind_is(child, "decorator"))
            continue;
        const char *chain = python_decorator_chain(index, child);
        chains[used++] = chain ? chain : "?";
    }
    chains[used] = NULL;
    member->decorators = chains;
}

/* A function outside a class matters to callers only through its annotated
 * return, or as a decorator its source proves. */
static void python_note_function(cbm_resolve_binding_index_t *index, TSNode node, uint32_t owner) {
    const char *return_chain = python_annotation_chain(index, field(node, "return_type"), 0);
    cbm_resolve_python_member_t *member =
        python_add_member(index, owner, node_text(index, field(node, "name")),
                          CBM_PY_MEMBER_FUNCTION, ts_node_start_byte(node));
    if (!member) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_member_allocation");
        return;
    }
    member->return_chain = return_chain;
    member->chain_scope = owner;
    python_note_member_shape(index, member, node);
}

/* `go = classmethod(go)` is the decorator spelled out: the callee, when this is that shape. */
static const char *python_spelled_decorator(cbm_resolve_binding_index_t *index, TSNode target,
                                            const char *name) {
    TSNode assignment = ts_node_parent(target);
    if (!kind_is(assignment, "assignment"))
        return NULL;
    TSNode right = field(assignment, "right");
    TSNode function = field(right, "function"), args = field(right, "arguments");
    if (!kind_is(right, "call") || !kind_is(function, "identifier") || ts_node_is_null(args) ||
        ts_node_named_child_count(args) != 1)
        return NULL;
    TSNode only = ts_node_named_child(args, 0);
    const char *callee = node_text(index, function);
    return kind_is(only, "identifier") && python_spells(index, only, name) && callee &&
                   (!strcmp(callee, "classmethod") || !strcmp(callee, "staticmethod"))
               ? callee
               : NULL;
}

/* Class-level assignment or declaration. No graph node carries it, but it stops
 * lookup: a base's method of the same name is not what this class exposes. */
static void python_note_class_value(cbm_resolve_binding_index_t *index, const char *name, TSNode at,
                                    const char *chain, bool typed, bool none, uint32_t owner) {
    cbm_resolve_python_member_t *member =
        python_add_member(index, owner, name, CBM_PY_MEMBER_VALUE, ts_node_start_byte(at));
    if (!member) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_member_allocation");
        return;
    }
    if (typed)
        member->type_chain = chain;
    else
        member->value_chain = chain;
    member->none_value = none;
    member->spelled_decorator = python_spelled_decorator(index, at, name);
    /* `ResultConsumer = ResultConsumer` in a class body reads the name from outside it: the
     * binding does not exist until the statement has run. */
    size_t root = chain ? strcspn(chain, ".(") : 0;
    bool self_named = root && strlen(name) == root && !strncmp(chain, name, root);
    member->chain_scope = self_named ? index->scopes[owner].parent : owner;
}

/* The method whose first parameter a name in this scope chain denotes. */
static uint32_t python_enclosing_method(const cbm_resolve_binding_index_t *index, uint32_t scope) {
    for (unsigned depth = 0; scope && scope < index->scope_count && depth < BINDING_MAX_DEPTH;
         depth++) {
        if (index->scopes[scope].method_of)
            return scope;
        scope = index->scopes[scope].parent;
    }
    return 0;
}

/* `self.x = value`, `self.x: T = value`: an instance attribute of the class the
 * method belongs to. Whatever else stores into it leaves its type unknown. */
static void python_note_instance_write(cbm_resolve_binding_index_t *index, TSNode attribute,
                                       const char *parent, const char *field_name) {
    TSNode object = field(attribute, "object");
    if (!kind_is(object, "identifier"))
        return;
    uint32_t method = python_enclosing_method(index, index->current_scope);
    if (!method)
        return;
    const char *receiver = node_text(index, object);
    const char *self = index->scopes[method].self_name;
    if (!receiver || !self || strcmp(receiver, self))
        return;
    uint32_t owner = index->scopes[method].method_of - 1;
    cbm_resolve_python_member_t *member =
        python_add_member(index, owner, node_text(index, field(attribute, "attribute")),
                          CBM_PY_MEMBER_INSTANCE, ts_node_start_byte(attribute));
    if (!member) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_member_allocation");
        return;
    }
    member->chain_scope = index->current_scope;
    bool direct =
        parent && !strcmp(parent, "assignment") && field_name && !strcmp(field_name, "left");
    if (!direct)
        return;
    TSNode assignment = ts_node_parent(attribute), type = field(assignment, "type"),
           right = field(assignment, "right");
    if (!ts_node_is_null(type)) {
        member->type_chain = python_annotation_chain(index, type, 0);
        return;
    }
    if (kind_is(right, "none"))
        member->none_value = true;
    else
        member->value_chain = python_chain(index, right, 0);
}

static void python_note_class(cbm_resolve_binding_index_t *index, TSNode node, uint32_t scope,
                              uint32_t header_scope) {
    cbm_resolve_python_class_t *classes =
        grow_array(index, index->python_classes, index->python_class_count,
                   &index->python_class_capacity, sizeof(*classes));
    if (!classes) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_class_allocation");
        return;
    }
    index->python_classes = classes;
    cbm_resolve_python_class_t *entry = &classes[index->python_class_count++];
    *entry = (cbm_resolve_python_class_t){.qualified_name = index->scopes[scope].qualified_name,
                                          .scope = scope,
                                          .header_scope = header_scope,
                                          .at = ts_node_start_byte(node)};
    TSNode bases = field(node, "superclasses");
    uint32_t count = ts_node_is_null(bases) ? 0 : ts_node_named_child_count(bases);
    entry->bases = count ? cbm_arena_alloc(index->arena, count * sizeof(*entry->bases)) : NULL;
    if (count && !entry->bases) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_class_allocation");
        return;
    }
    for (uint32_t i = 0; i < count; i++) {
        TSNode base = ts_node_named_child(bases, i);
        if (kind_is(base, "keyword_argument")) {
            continue;
        }
        if (kind_is(base, "comment"))
            continue;
        /* Generic[T] and Base[int] inherit the subscripted class. */
        bool subscripted = kind_is(base, "subscript");
        if (subscripted)
            base = field(base, "value");
        const char *chain = python_chain(index, base, 0);
        if (!chain || (subscripted && entry->base_count >= 64)) {
            entry->unknown_base = true;
            continue;
        }
        if (subscripted)
            entry->subscripted |= UINT64_C(1) << entry->base_count;
        /* These replace what the class body assigned: an Enum member reads as the enum. */
        const char *canonical = python_import_canonical(index, chain);
        entry->metaclass |= canonical && (!strncmp(canonical, "enum.", 5) ||
                                          !strcmp(canonical, "typing.NamedTuple"));
        entry->bases[entry->base_count++] = chain;
    }
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

/* A def in a class body receives its instance or class first. staticmethod
 * receives neither; __new__ and friends are implicitly class-level. */
static void python_note_method_scope(cbm_resolve_binding_index_t *index, TSNode node, uint32_t id,
                                     uint32_t owner) {
    binding_scope_t *scope = &index->scopes[id];
    TSNode decorated = ts_node_parent(node);
    bool is_class = false;
    if (kind_is(decorated, "decorated_definition")) {
        for (uint32_t i = 0; i < ts_node_named_child_count(decorated); i++) {
            TSNode child = ts_node_named_child(decorated, i);
            const char *chain =
                kind_is(child, "decorator") ? python_decorator_chain(index, child) : NULL;
            if (chain && !strcmp(chain, "staticmethod"))
                return;
            if (chain && !strcmp(chain, "classmethod"))
                is_class = true;
        }
    }
    const char *name = node_text(index, field(node, "name"));
    if (name && (!strcmp(name, "__new__") || !strcmp(name, "__init_subclass__") ||
                 !strcmp(name, "__class_getitem__")))
        is_class = true;
    TSNode parameters = field(node, "parameters");
    TSNode first = ts_node_is_null(parameters) || !ts_node_named_child_count(parameters)
                       ? (TSNode){0}
                       : ts_node_named_child(parameters, 0);
    if (kind_is(first, "typed_parameter") && ts_node_named_child_count(first))
        first = ts_node_named_child(first, 0);
    if (!kind_is(first, "identifier"))
        return;
    scope->method_of = owner + 1;
    scope->self_name = node_text(index, first);
    scope->self_is_class = is_class;
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
    uint32_t declaring = parent;
    /* A method closes over enclosing functions, never its class dictionary.
     * The qualified owner above still retains that class in graph identity. */
    if (!class_scope)
        while (parent && scopes[parent].class_scope)
            parent = scopes[parent].parent;
    uint32_t id = (uint32_t)index->scope_count++;
    scopes[id] =
        (binding_scope_t){.qualified_name = qn, .parent = parent, .class_scope = class_scope};
    if (kind_is(node, "function_definition") && scopes[declaring].class_scope && declaring)
        python_note_method_scope(index, node, id, declaring);
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
            /* `get m()` runs on the read; what a call then invokes is its return value. */
            if (row && (child_token(item, "get") || child_token(item, "set")))
                row->conflict = true;
            else if (row)
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
    /* Owners are later matched by name at module level, so a nested or parameter
     * `Owner` must not reach them. */
    if (state->scope != 0)
        return;
    TSNode parent = ts_node_parent(node);
    TSNode object = (TSNode){0};
    if (kind_is(node, "function_declaration")) {
        /* A constructor function's `this` is its instance. */
        state->js_this_owner = node_text(index, field(node, "name"));
        state->js_this_prototype = true;
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

/* `owner`, `owner.prototype` or `this` as the object whose members are named. */
static const char *js_member_owner(cbm_resolve_binding_index_t *index, TSNode object,
                                   const binding_walk_t *state, bool *prototype) {
    *prototype = false;
    if (kind_is(object, "this")) {
        *prototype = state->js_this_prototype;
        return state->js_this_owner;
    }
    if (kind_is(object, "identifier"))
        return node_text(index, object);
    if (kind_is(object, "member_expression") && kind_is(field(object, "object"), "identifier") &&
        kind_is(field(object, "property"), "property_identifier")) {
        const char *middle = node_text(index, field(object, "property"));
        if (middle && !strcmp(middle, "prototype")) {
            *prototype = true;
            return node_text(index, field(object, "object"));
        }
    }
    return NULL;
}

/* `owner[key] = v` and `owner.prototype[key] = v` can replace any member. */
static void note_javascript_computed_write(cbm_resolve_binding_index_t *index, TSNode left,
                                           const binding_walk_t *state) {
    bool prototype = false;
    const char *owner = js_member_owner(index, field(left, "object"), state, &prototype);
    if (owner)
        (void)js_member_row(index, owner, JS_MEMBER_COMPUTED, prototype);
}

/* A call that names its first argument the object it copies members into. */
static bool js_mutator_name(const char *name) {
    static const char *const names[] = {"assign",
                                        "defineProperty",
                                        "defineProperties",
                                        "deleteProperty",
                                        "extend",
                                        "merge",
                                        "mixin",
                                        "set"};
    for (size_t i = 0; name && i < sizeof(names) / sizeof(names[0]); i++) {
        if (!strcmp(name, names[i]))
            return true;
    }
    return false;
}

/* `Object.assign(owner, ...)`, `mixin(owner.prototype, ...)` and `delete owner.m` can
 * replace members no assignment names. `Reflect.set` is the one receiver-sensitive
 * name: a plain `map.set(owner, v)` mutates `map`, so `set` needs that receiver. */
static void note_javascript_member_mutation(cbm_resolve_binding_index_t *index, TSNode node,
                                            const binding_walk_t *state) {
    TSNode object = (TSNode){0};
    if (kind_is(node, "unary_expression") && child_token(node, "delete")) {
        TSNode target = field(node, "argument");
        if (kind_is(target, "member_expression") || kind_is(target, "subscript_expression"))
            object = field(target, "object");
    } else if (kind_is(node, "call_expression")) {
        TSNode callee = field(node, "function");
        bool member = kind_is(callee, "member_expression");
        const char *name = member
                               ? node_text(index, field(callee, "property"))
                               : (kind_is(callee, "identifier") ? node_text(index, callee) : NULL);
        if (!js_mutator_name(name))
            return;
        const char *receiver = member ? node_text(index, field(callee, "object")) : NULL;
        if (!strcmp(name, "set") && (!receiver || strcmp(receiver, "Reflect")))
            return;
        TSNode arguments = field(node, "arguments");
        if (kind_is(arguments, "arguments") && ts_node_named_child_count(arguments) > 0)
            object = ts_node_named_child(arguments, 0);
    }
    if (ts_node_is_null(object))
        return;
    bool prototype = false;
    const char *owner = js_member_owner(index, object, state, &prototype);
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
    bool prototype = false;
    const char *owner = js_member_owner(index, object, state, &prototype);
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
        /* `exports = module.exports = x` keeps them one object. `exports = x` does not. */
        bool chained = js_chain_writes_module_exports(index, right);
        index->js_exports_rebound |= chained;
        index->js_exports_detached |= !chained;
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
        /* Its own scope reads nothing before this line ran (TDZ, or undefined for
         * `var`). A closure, its own body included, runs later and may name it. */
        add_name(index, name, ts_node_end_byte(declarator),
                 BINDING_VALUE_DECLARATION | BINDING_FUNCTION_VALUE |
                     (state->named_export && module_level ? BINDING_EXPORTED : 0));
        js_set_lines(index, row, value);
        if (index->name_count > row)
            index->names[row].at_start = UINT32_MAX;
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
    note_javascript_member_mutation(index, node, &state);
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
        /* Field initializers and static blocks have the class's own `this`. */
        state.js_this_owner = NULL;
        state.js_this_prototype = false;
        index->current_this_owner = NULL;
        index->current_this_prototype = false;
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
            if (state.js_binder) {
                note_javascript_binder(index, node, name, &state);
            } else {
                size_t before = index->site_count;
                add_site(index, name, node);
                if (index->site_count > before && parent_kind &&
                    !strcmp(parent_kind, "nested_type_identifier"))
                    index->sites[before].type_path_root = true;
            }
        }
    } else if (kind_is(node, "nested_type_identifier") && !state.import) {
        size_t before = index->site_count;
        add_site(index, node_text(index, node), node);
        if (index->site_count > before)
            index->sites[before].type_position = true;
    } else if (kind_is(node, "type_identifier") &&
               (kind_is(ts_node_parent(node), "type_parameter") ||
                kind_is(ts_node_parent(node), "mapped_type_clause") ||
                kind_is(ts_node_parent(node), "infer_type"))) {
        /* `<T>`, `[K in Keys]` and `infer U` shadow a same-named type wherever they are
         * visible. The other names of a mapped clause are ordinary references. */
        if (!kind_is(ts_node_parent(node), "mapped_type_clause") ||
            ts_node_eq(node, field(ts_node_parent(node), "name"))) {
            add_name(index, node_text(index, node), 0, BINDING_BLOCKED | BINDING_TYPE_DECLARATION);
        } else if (!state.import) {
            size_t before = index->site_count;
            add_site(index, node_text(index, node), node);
            if (index->site_count > before)
                index->sites[before].type_position = true;
        }
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
            python_note_rebinding_scope(index, node, !strcmp(kind, "global_statement"));
            return state;
        }
        if (!strcmp(kind, "subscript") && python_own_module_entry(index, node)) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dynamic_namespace_effect");
            return state;
        }
        if (!strcmp(kind, "if_statement") || !strcmp(kind, "for_statement") ||
            !strcmp(kind, "while_statement") || !strcmp(kind, "try_statement") ||
            !strcmp(kind, "with_statement") || !strcmp(kind, "match_statement"))
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
        /* The annotation expression is an ordinary read, as it is on an
         * assignment. Continue the walk: it cannot hide dynamic names, nested
         * writes or unsupported syntax from the source audit. */
    }
    /* An interpolation is an ordinary read. Its __format__ can run user code,
     * exactly as any call or operator in this file can; neither rebinds a name
     * this audit sees. The walk still visits every nested expression, so a
     * walrus target or a dynamic name inside the braces keeps its own effect. */
    if (index->language == CBM_LANG_PYTHON && !strcmp(kind, "type_conversion")) {
        const char *conversion = node_text(index, node);
        if (!conversion ||
            (strcmp(conversion, "!s") && strcmp(conversion, "!r") && strcmp(conversion, "!a"))) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                       "python_format_conversion_unproven");
            return state;
        }
    }
    /* A star under TYPE_CHECKING states the module's names as its plain imports there do. */
    if (index->language == CBM_LANG_PYTHON && !strcmp(kind, "wildcard_import") &&
        (inherited.scope || (inherited.conditional && !python_type_checking_import(index, node)))) {
        fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "conditional_wildcard_import");
        return state;
    }
    if (index->language == CBM_LANG_PYTHON &&
        (!strcmp(kind, "if_statement") || !strcmp(kind, "elif_clause") ||
         !strcmp(kind, "assert_statement")))
        python_note_narrowing(index, node, kind);
    /* A store through an attribute ends what a narrowing said about that attribute. */
    if (index->language == CBM_LANG_PYTHON && kind_is(node, "attribute") && field_name &&
        !strcmp(field_name, "left") && parent_kind &&
        (!strcmp(parent_kind, "assignment") || !strcmp(parent_kind, "augmented_assignment"))) {
        const char *chain = python_chain(index, node, 0);
        python_store_t *stores =
            chain ? grow_array(index, index->python_stores, index->python_store_count,
                               &index->python_store_capacity, sizeof(*stores))
                  : NULL;
        if (stores) {
            index->python_stores = stores;
            stores[index->python_store_count++] = (python_store_t){
                .chain = chain,
                .at = ts_node_end_byte(ts_node_parent(node)),
                .scope = state.scope,
                .value = strcmp(parent_kind, "assignment")
                             ? NULL
                             : python_chain(index, field(ts_node_parent(node), "right"), 0),
                .value_at = ts_node_start_byte(node)};
        }
    }
    if (index->language == CBM_LANG_PYTHON &&
        (!strcmp(kind, "for_statement") || !strcmp(kind, "while_statement"))) {
        python_loop_t *loops = grow_array(index, index->python_loops, index->python_loop_count,
                                          &index->python_loop_capacity, sizeof(*loops));
        if (!loops) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_loop_allocation");
            return state;
        }
        index->python_loops = loops;
        uint32_t start = ts_node_start_byte(node);
        /* Loops nest or are disjoint, so the walk's order alone says which are still open. */
        while (index->python_open_loop && loops[index->python_open_loop - 1].end <= start)
            index->python_open_loop = loops[index->python_open_loop - 1].parent;
        loops[index->python_loop_count++] = (python_loop_t){
            .start = start, .end = ts_node_end_byte(node), .parent = index->python_open_loop};
        index->python_open_loop = (uint32_t)index->python_loop_count;
    }
    bool import = kind_is(node, "import_statement") || kind_is(node, "import_from_statement");
    if (import) {
        state.import = true;
        if (depth == 1) {
            note_import(index, node, false);
        } else if (index->language != CBM_LANG_PYTHON) {
            state.blocked = true;
        } else if (python_type_checking_import(index, node)) {
            /* Nothing binds at runtime, but an annotation still names the class. */
            python_note_annotation_imports(index, node);
        } else {
            /* `from a import b` binds b in the scope that runs it, and only b.
             * One binder is proof of identity wherever it is bound at all;
             * alternatives such as try/except ImportError stay ambiguous. */
            note_import(index, node, true);
        }
    }
    /* typing.overload stubs announce the def that follows: with it they are one declaration,
     * and they bind nothing of their own. */
    bool overload_stub = index->language == CBM_LANG_PYTHON &&
                         !strcmp(kind, "function_definition") && parent_kind &&
                         !strcmp(parent_kind, "decorated_definition") &&
                         python_overload_stub(index, ts_node_parent(node));
    if (index->language == CBM_LANG_PYTHON && strcmp(kind, "class_definition") == 0) {
        /* A class creates a binding in its enclosing namespace. Its members
         * do not authorize callable targets, but must not erase independent
         * module function/import proofs elsewhere in the file. */
        bool decorated = parent_kind && !strcmp(parent_kind, "decorated_definition");
        const char *class_name = node_text(index, field(node, "name"));
        add_name(index, class_name, ts_node_start_byte(node),
                 decorated ? BINDING_BLOCKED | BINDING_DECORATED
                           : BINDING_TYPE_DECLARATION | BINDING_VALUE_TYPE);
        if (decorated && index->name_count)
            python_note_decorated(index, node, index->names[index->name_count - 1].target,
                                  class_name, true);
    }
    if ((strcmp(kind, "function_definition") == 0 || strcmp(kind, "function_declaration") == 0) &&
        !overload_stub) {
        TSNode name = field(node, "name");
        /* One binder proves identity wherever it is bound at all, so a def under
         * a condition still counts; an alternative def makes two binders. */
        bool decorated = index->language == CBM_LANG_PYTHON && parent_kind &&
                         !strcmp(parent_kind, "decorated_definition");
        bool supported =
            kind_is(name, "identifier") && !decorated &&
            (index->language == CBM_LANG_PYTHON || (depth == 1 && lua_local_function(node)));
        if (!ts_node_is_null(name)) {
            const char *text = node_text(index, name);
            add_name(index, text, ts_node_start_byte(node),
                     supported   ? BINDING_DECLARATION
                     : decorated ? BINDING_BLOCKED | BINDING_DECORATED
                                 : BINDING_BLOCKED);
            if (decorated && index->name_count)
                python_note_decorated(index, node, index->names[index->name_count - 1].target, text,
                                      false);
        }
    }
    if (index->language == CBM_LANG_PYTHON &&
        (!strcmp(kind, "function_definition") || !strcmp(kind, "class_definition") ||
         !strcmp(kind, "lambda"))) {
        uint32_t enclosing_scope = state.scope;
        bool in_class = enclosing_scope && index->scopes[enclosing_scope].class_scope;
        size_t function_member = index->python_member_count;
        if (in_class && !strcmp(kind, "function_definition") && !overload_stub)
            python_note_method(index, node, parent_kind, enclosing_scope, state.conditional);
        else if (!strcmp(kind, "function_definition") && !overload_stub)
            python_note_function(index, node, enclosing_scope);
        if (in_class && !strcmp(kind, "class_definition"))
            python_note_class_value(index, node_text(index, field(node, "name")), node, NULL, false,
                                    false, enclosing_scope);
        state.pending = python_pending_declaration(index, node, enclosing_scope, state.pending);
        state.scope =
            python_child_scope(index, node, enclosing_scope, !strcmp(kind, "class_definition"));
        if (!strcmp(kind, "class_definition"))
            python_note_class(index, node, state.scope, enclosing_scope);
        if (!strcmp(kind, "function_definition") && !overload_stub)
            python_note_returns(index, node, function_member, state.scope);
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
        /* An attribute under a target role stores into its object's attribute. */
        bool attribute_target = !strcmp(kind, "attribute") &&
                                ((parent_kind && field_name && !strcmp(field_name, "left") &&
                                  (!strcmp(parent_kind, "assignment") ||
                                   !strcmp(parent_kind, "augmented_assignment"))) ||
                                 inherited.blocked);
        if (attribute_target)
            python_note_instance_write(index, node, parent_kind, field_name);
        if (!strcmp(kind, "attribute") && !attribute_target) {
            add_site(index, node_text(index, node), node);
            /* The site exists to reach the chain; lexical lookups refuse it. */
            const char *terminal = node_text(index, field(node, "attribute"));
            size_t sites = index->site_count;
            add_site(index, terminal, node);
            if (index->site_count > sites)
                index->sites[index->site_count - 1].attribute_terminal = true;
            python_note_terminal_wrappers(index, terminal, node);
        }
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
        bool python = index->language == CBM_LANG_PYTHON;
        bool lexical = !python || python_lexical_identifier(parent_kind, field_name);
        bool function_name = field_name && strcmp(field_name, "name") == 0 && parent_kind &&
                             (strcmp(parent_kind, "function_definition") == 0 ||
                              strcmp(parent_kind, "function_declaration") == 0);
        bool dynamic = python ? (dynamic_name(index->language, name) &&
                                 python_namespace_call_writes(index, node, name)) ||
                                    python_namespace_view(index, node, name)
                              : dynamic_name(index->language, name);
        if (dynamic && !python) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dynamic_namespace_effect");
        } else if (dynamic && lexical && !function_name && !state.blocked && !state.import &&
                   !python_name_shadowed(index, name)) {
            fail_index(index, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "dynamic_namespace_effect");
        }
        /* A simple module-level assignment declares that variable. One binder
         * proves a value identity only: invocation and member lookup through it
         * still abstain, see BINDING_VALUE_DECLARATION in validation. */
        bool module_value = python && state.blocked && state.scope == 0 && parent_kind &&
                            !strcmp(parent_kind, "assignment") && field_name &&
                            !strcmp(field_name, "left") && !annotation_target &&
                            !ts_node_is_null(field(ts_node_parent(node), "right"));
        if (state.blocked && !function_name && lexical) {
            size_t recorded = index->name_count;
            add_name(index, name, ts_node_start_byte(node),
                     module_value ? BINDING_VALUE_DECLARATION : BINDING_BLOCKED);
            bool binder = python && index->name_count > recorded;
            if (binder)
                python_note_binder_value(index, node, parent_kind, field_name);
            if (binder && state.scope && index->scopes[state.scope].class_scope &&
                !annotation_target)
                python_note_class_value(
                    index, name, node, index->names[index->name_count - 1].value_chain,
                    index->names[index->name_count - 1].value_kind == CBM_PY_ROOT_TYPED,
                    parent_kind && !strcmp(parent_kind, "assignment") &&
                        kind_is(field(ts_node_parent(node), "right"), "none"),
                    state.scope);
        }
        if (python && annotation_target && state.scope && index->scopes[state.scope].class_scope) {
            /* `x: T` in a class body types its instances' x without binding it. */
            cbm_resolve_python_member_t *member = python_add_member(
                index, state.scope, name, CBM_PY_MEMBER_INSTANCE, ts_node_start_byte(node));
            if (!member) {
                fail_index(index, CBM_RESOLVE_ANALYSIS_FAILED, "python_member_allocation");
                return state;
            }
            member->type_chain =
                python_annotation_chain(index, field(ts_node_parent(node), "type"), 0);
            member->chain_scope = state.scope;
        }
        if (!state.import && !function_name && !annotation_target && lexical) {
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
    size_t last_whole = 0;
    for (size_t r = 0; r < index->js_raw_export_count; r++) {
        if (index->js_raw_exports[r].whole)
            last_whole = r;
    }
    for (size_t r = 0; r < index->js_raw_export_count; r++) {
        const js_raw_export_t *raw = &index->js_raw_exports[r];
        if (raw->owner && !js_owner_is_alias(index, raw->owner))
            continue;
        cbm_resolve_js_export_t *out = &index->js_exports[index->js_export_count++];
        *out = (cbm_resolve_js_export_t){
            .name = raw->export_name, .kind = raw->kind, .whole = raw->whole};
        /* `module.exports = x` replaces the object a bare `exports.y` wrote to, and any
         * `exports.y` or `module.exports.y` written before it. A plain `exports = x`
         * detaches the name, so the bare writes never reach module.exports at all. */
        if (raw->bare_exports &&
            (index->js_exports_detached || (index->js_whole_export && !index->js_exports_rebound)))
            continue;
        if (!raw->whole && !raw->owner && r < last_whole)
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

/* `class X { constructor() {} }` at module level: what `new X()` runs. */
static void js_settle_constructors(cbm_resolve_binding_index_t *index) {
    if (index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return;
    for (int d = 0; d < index->result->defs.count; d++) {
        const CBMDefinition *definition = &index->result->defs.items[d];
        const char *qn = definition->qualified_name;
        if (!definition->name || strcmp(definition->name, "constructor") || !qn)
            continue;
        const char *dot = strrchr(qn, '.');
        const char *owner = dot ? dot : qn;
        while (owner > qn && owner[-1] != '.')
            owner--;
        if (!dot || owner == dot)
            continue;
        const char *name = cbm_arena_strndup(index->arena, owner, (size_t)(dot - owner));
        bool conflict = false;
        binding_name_t *row =
            name ? (binding_name_t *)js_module_row(index, name, JS_SPACE_VALUE, &conflict) : NULL;
        if (row && !conflict && row->target && strlen(row->target) == (size_t)(dot - qn) &&
            !strncmp(row->target, qn, (size_t)(dot - qn)))
            row->js_constructor = qn;
    }
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

typedef enum { PY_NS_READ, PY_NS_WRITE, PY_NS_ANY } python_namespace_use_t;

/* A plain string's text, NULL for anything computed. */
static const char *python_plain_string(cbm_resolve_binding_index_t *index, TSNode node) {
    if (!kind_is(node, "string") || ts_node_named_child_count(node) != 3)
        return NULL;
    TSNode content = ts_node_named_child(node, 1);
    return kind_is(content, "string_content") ? node_text(index, content) : NULL;
}

/* What one use of a name does to the module it may hold. A namespace changes by a
 * statement that stores or deletes through it, and that statement names what it changes.
 * Reading a member, calling, or handing the module to a call selects no other
 * declaration: what a callee does with it is not a static fact. __dict__, vars() and a
 * computed setattr can reach any name. Queries are binary searches, never rescans. */
static python_namespace_use_t python_namespace_use(cbm_resolve_binding_index_t *index, TSNode node,
                                                   const char **written) {
    TSNode parent = ts_node_parent(node);
    if (kind_is(parent, "argument_list")) {
        TSNode call = ts_node_parent(parent);
        const char *callee = node_text(index, field(call, "function"));
        if (callee && !strcmp(callee, "vars"))
            return PY_NS_ANY;
        if (!callee || (strcmp(callee, "setattr") && strcmp(callee, "delattr")) ||
            !ts_node_eq(node, ts_node_named_child(parent, 0)))
            return PY_NS_READ;

        *written = python_plain_string(index, ts_node_named_child(parent, 1));
        return *written ? PY_NS_WRITE : PY_NS_ANY;
    }
    if (!kind_is(parent, "attribute") || !ts_node_eq(node, field(parent, "object")))
        return PY_NS_READ;
    const char *path = NULL;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        const char *member = node_text(index, field(parent, "attribute"));
        if (!member || !strcmp(member, "__dict__"))
            return PY_NS_ANY;
        path = path ? cbm_arena_sprintf(index->arena, "%s.%s", path, member) : member;
        node = parent;
        parent = ts_node_parent(node);
        if (kind_is(parent, "attribute") && ts_node_eq(node, field(parent, "object")))
            continue;
        bool stored = ((kind_is(parent, "assignment") || kind_is(parent, "augmented_assignment")) &&
                       ts_node_eq(node, field(parent, "left"))) ||
                      kind_is(parent, "delete_statement") ||
                      (kind_is(parent, "expression_list") &&
                       kind_is(ts_node_parent(parent), "delete_statement")) ||
                      kind_is(parent, "pattern_list") || kind_is(parent, "as_pattern_target");
        if (!stored)
            return PY_NS_READ;
        *written = path;
        return path ? PY_NS_WRITE : PY_NS_ANY;
    }
    return PY_NS_ANY;
}

static int python_write_compare(const void *left, const void *right) {
    return strcmp(((const python_write_t *)left)->name, ((const python_write_t *)right)->name);
}

/* `alias = pkg; alias.child = None` writes through pkg. Two rounds follow an alias of an alias;
 * each is one search per alias statement. */
static void python_follow_write_aliases(cbm_resolve_binding_index_t *index) {
    for (unsigned round = 0; round < 2; round++) {
        size_t sorted = index->python_write_count;
        for (size_t a = 0; a < index->python_alias_count; a++) {
            const python_alias_t alias = index->python_aliases[a];
            size_t low = 0, high = sorted;
            while (low < high) {
                size_t middle = low + (high - low) / 2;
                if (strcmp(index->python_writes[middle].name, alias.alias) < 0)
                    low = middle + 1;
                else
                    high = middle;
            }
            for (; low < sorted && !strcmp(index->python_writes[low].name, alias.alias); low++) {
                const char *member = index->python_writes[low].member;
                python_write_t *writes =
                    grow_array(index, index->python_writes, index->python_write_count,
                               &index->python_write_capacity, sizeof(*writes));
                if (!writes) {
                    index->python_writes_unseen = true;
                    return;
                }
                index->python_writes = writes;
                writes[index->python_write_count++] =
                    (python_write_t){.name = alias.of, .member = member};
            }
        }
        if (index->python_write_count == sorted)
            return;
        qsort(index->python_writes, index->python_write_count, sizeof(*index->python_writes),
              python_write_compare);
    }
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
            index->python_writes_unseen = true;
            break;
        }
        bool skip = kind_is(node, "import_statement") || kind_is(node, "import_from_statement");
        TSNode parent = ts_node_parent(node);
        bool member =
            kind_is(node, "identifier") &&
            ((kind_is(parent, "attribute") && ts_node_eq(node, field(parent, "attribute"))) ||
             (kind_is(parent, "keyword_argument") && ts_node_eq(node, field(parent, "name"))));
        if (kind_is(node, "identifier") && !member) {
            const char *name = node_text(index, node);
            if (name && dynamic_name(CBM_LANG_PYTHON, name) &&
                python_namespace_call_writes(index, node, name))
                index->python_effects_complete = false;
        }
        if (kind_is(node, "identifier") && !member && kind_is(parent, "argument_list") &&
            python_spells(index, node, "__name__"))
            index->python_name_handed = true;
        if (kind_is(node, "identifier") && !member) {
            const char *written = NULL;
            python_namespace_use_t use = python_namespace_use(index, node, &written);
            const char *name = use != PY_NS_READ ? node_text(index, node) : NULL;
            python_write_t *writes =
                use != PY_NS_READ
                    ? grow_array(index, index->python_writes, index->python_write_count,
                                 &index->python_write_capacity, sizeof(*writes))
                    : NULL;
            if (use != PY_NS_READ && (!name || !writes)) {
                index->python_effects_complete = false;
                index->python_writes_unseen = true;
                break;
            }
            if (use != PY_NS_READ) {
                index->python_writes = writes;
                writes[index->python_write_count++] =
                    (python_write_t){.name = name, .member = use == PY_NS_WRITE ? written : NULL};
            } else if (kind_is(parent, "assignment") && ts_node_eq(node, field(parent, "right")) &&
                       kind_is(field(parent, "left"), "identifier")) {
                python_alias_t *aliases =
                    grow_array(index, index->python_aliases, index->python_alias_count,
                               &index->python_alias_capacity, sizeof(*aliases));
                const char *alias = node_text(index, field(parent, "left"));
                const char *of = node_text(index, node);
                if (!aliases || !alias || !of) {
                    index->python_effects_complete = false;
                    index->python_writes_unseen = true;
                    break;
                }
                index->python_aliases = aliases;
                aliases[index->python_alias_count++] = (python_alias_t){alias, of};
            }
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
    if (index->python_write_count > 1)
        qsort(index->python_writes, index->python_write_count, sizeof(*index->python_writes),
              python_write_compare);
    python_follow_write_aliases(index);
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

static const binding_name_t *find_scoped_name(const cbm_resolve_binding_index_t *index,
                                              const char *name, size_t length, uint32_t scope,
                                              bool *present, js_space_t space);

/* A comprehension body does not see the class it is written in, and has no scope of its
 * own here. So a name the class binds could be the comprehension's or the enclosing
 * scope's, and it abstains. Any other name reads on through the class as usual. */
static void python_refuse_class_comprehension_reads(cbm_resolve_binding_index_t *index) {
    for (size_t i = 0; i < index->site_count; i++) {
        binding_site_t *site = &index->sites[i];
        if (!site->class_comprehension)
            continue;
        bool present = false;
        (void)find_scoped_name(index, site->name, strcspn(site->name, ".("), site->scope, &present,
                               JS_SPACE_ANY);
        site->attribute_terminal |= present;
    }
}

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

static cbm_resolve_binding_index_t *binding_build(CBMArena *arena, const CBMFileResult *result,
                                                  CBMLanguage language, const char *source,
                                                  size_t source_len, bool declared_module,
                                                  bool adapter_sites_only) {
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
    /* These languages state their rules per occurrence in adapter_<lang>.c.
     * This index only attests the exact site through cbm_resolve_binding_source_site. */
    if (adapter_sites_only || (cbm_source_adapter_supported(language) &&
                               !cbm_source_adapter_has_binding_rules(language))) {
        index->analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        index->reason = "source_adapter_binding";
        return index;
    }
    bool component = language == CBM_LANG_VUE || language == CBM_LANG_SVELTE;
    if (language != CBM_LANG_PYTHON && language != CBM_LANG_LUA && language != CBM_LANG_DART &&
        language != CBM_LANG_RUST && !javascript_language(language) && !component) {
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
                python_namespace_effects(index, ts_tree_root_node(tree));
                python_finish_narrows(index);
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
        if (language == CBM_LANG_PYTHON)
            python_refuse_class_comprehension_reads(index);
    }
    if (javascript_language(language)) {
        js_apply_writes(index);
        audit_javascript_values(index);
        js_settle_exports(index);
        js_settle_members(index);
        js_settle_constructors(index);
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

cbm_resolve_binding_index_t *cbm_resolve_binding_build_declared_module(
    CBMArena *arena, const CBMFileResult *result, CBMLanguage language, const char *source,
    size_t source_len, bool declared_module) {
    return binding_build(arena, result, language, source, source_len, declared_module, false);
}

cbm_resolve_binding_index_t *cbm_resolve_binding_build(CBMArena *arena, const CBMFileResult *result,
                                                       CBMLanguage language, const char *source,
                                                       size_t source_len) {
    return binding_build(arena, result, language, source, source_len, false, false);
}

cbm_resolve_binding_index_t *cbm_resolve_binding_source_index(CBMArena *arena,
                                                              const CBMFileResult *result,
                                                              CBMLanguage language,
                                                              const char *source,
                                                              size_t source_len) {
    return binding_build(arena, result, language, source, source_len, false, true);
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

/* ts_node_named_child(node, i) walks from the first child on every call, so an index loop
 * is quadratic in the sibling count, and TypeScript's own baselines are files with that many
 * top-level statements. One cursor pass snapshots the children. */
static const TSNode *top_level_children(cbm_resolve_binding_index_t *index, TSNode root,
                                        uint32_t *count) {
    uint32_t expected = ts_node_named_child_count(root);
    TSNode *children =
        expected ? cbm_arena_alloc(index->arena, expected * sizeof(*children)) : NULL;
    uint32_t found = 0;
    if (children) {
        TSTreeCursor cursor = ts_tree_cursor_new(root);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (ts_node_is_named(child) && found < expected)
                    children[found++] = child;
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, found);
    *count = found;
    return children;
}

static TSNode http_default_value(const TSNode *children, uint32_t count) {
    TSNode value = {0};
    for (uint32_t i = 0; i < count; i++) {
        TSNode node = children[i];
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

static TSNode http_default_initializer(cbm_resolve_binding_index_t *index, const TSNode *children,
                                       uint32_t count, const char *name) {
    TSNode selected = {0};
    for (uint32_t i = 0; i < count; i++) {
        TSNode declaration = children[i];
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
    uint32_t count = 0;
    const TSNode *children = top_level_children(index, root, &count);
    TSNode value = http_default_value(children, count);
    const char *name = ts_node_is_null(value) ? NULL : node_text(index, value);
    const binding_name_t *binding = name ? find_name(index, name, strlen(name)) : NULL;
    if (!binding || (binding->flags & BINDING_BLOCKED) ||
        !(binding->flags & BINDING_VALUE_DECLARATION) || !binding->target ||
        ts_node_start_byte(value) < binding->at)
        return;
    TSNode call = http_default_initializer(index, children, count, name);
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

/* A function body runs when called, not where it is written, so a module-level
 * binder below the def is still the one it sees. Module and class bodies run in
 * order, so for them the source position decides. */
static bool python_deferred_site(const cbm_resolve_binding_index_t *index,
                                 const binding_site_t *site, const binding_name_t *binding) {
    if (index->language != CBM_LANG_PYTHON)
        return false;
    uint32_t scope = site->scope;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH && scope != binding->scope; depth++) {
        if (scope >= index->scope_count || !scope)
            return false;
        if (!index->scopes[scope].class_scope)
            return true;
        scope = index->scopes[scope].parent;
    }
    return false;
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
    if (!located || located->attribute_terminal) {
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
    /* Dart library members, and a Python name no scope binds that a closed star
     * import of this module provides: the export solver established the target. */
    bool implicit_language = index->language == CBM_LANG_DART ||
                             (index->language == CBM_LANG_PYTHON && index->python_star_count);
    bool dart_import = implicit_language && !dot && !binding &&
                       !cbm_resolve_binding_name_present(index, name) &&
                       candidate.rule == CBM_RESOLVE_RULE_IMPORT_BINDING &&
                       (prior_facts & CBM_RESOLVE_FACT_IMPORT_BOUND) &&
                       (prior_facts & CBM_RESOLVE_FACT_LOOKUP_COMPLETE);
    if (!dart_import && (!binding || ((binding->flags & BINDING_BLOCKED) && !type_import) ||
                         (too_early && !python_deferred_site(index, located, binding)))) {
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
        (binding->flags & BINDING_TYPE_DECLARATION) && (binding->flags & BINDING_VALUE_TYPE)) {
        if (binding->js_constructor)
            candidate.target = binding->js_constructor;
        else
            candidate.facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
    }
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
        located && !located->member_path && !located->attribute_terminal
            ? find_lexical_name(index, name, strlen(name), located->scope, &query, space)
            : NULL;
    unsigned local_kinds = BINDING_DECLARATION | BINDING_VALUE_TYPE;
    if (ecma)
        local_kinds |= located && located->type_position ? BINDING_TYPE_DECLARATION
                                                         : BINDING_VALUE_DECLARATION;
    else
        local_kinds |= BINDING_VALUE_DECLARATION;
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

bool cbm_resolve_binding_js_type_occurrence(const cbm_resolve_binding_index_t *index,
                                            const char *name, uint32_t start, uint32_t end,
                                            CBMSourceOrigin origin) {
    if (!index || !name || origin != CBM_SOURCE_ORIGIN_RAW || !index->sites ||
        !javascript_language(index->language))
        return false;
    binding_site_t key = {.name = name, .start = start, .end = end};
    const binding_site_t *site =
        bsearch(&key, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    return site && (site->type_position || site->type_path_root);
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

static uint32_t source_rule_fact(cbm_resolve_rule_t rule) {
    switch (rule) {
    case CBM_RESOLVE_RULE_SCOPE_BINDING:
        return CBM_RESOLVE_FACT_SCOPE_BOUND;
    case CBM_RESOLVE_RULE_IMPORT_BINDING:
        return CBM_RESOLVE_FACT_IMPORT_BOUND;
    case CBM_RESOLVE_RULE_TYPE_MEMBER:
        return CBM_RESOLVE_FACT_RECEIVER_TYPED;
    case CBM_RESOLVE_RULE_LANGUAGE_BINDING:
        return CBM_RESOLVE_FACT_LANGUAGE_BOUND;
    default:
        return 0;
    }
}

cbm_resolve_result_t cbm_resolve_binding_source_site(const cbm_resolve_binding_index_t *index,
                                                     const char *name, uint32_t start, uint32_t end,
                                                     CBMSourceOrigin origin, const char *domain,
                                                     const char *target, cbm_resolve_rule_t rule,
                                                     uint32_t facts, const char *decision) {
    cbm_resolve_lookup_t query;
    binding_query_begin(index, &query, name, start, end, origin);
    uint32_t fact = source_rule_fact(rule);
    cbm_resolve_result_t result = {.reason = decision};
    if (!index || index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !name || !*name ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start || !target || !*target || !fact)
        return binding_query_finish(index, &query, result);
    (void)cbm_resolve_lookup_observe(&query, "source_occurrences", name, name,
                                     CBM_RESOLVE_BINDING_UNIQUE, CBM_RESOLVE_ANALYSIS_COMPLETE, 0,
                                     true, "exact_occurrence_observed");
    (void)cbm_resolve_lookup_observe(&query, domain ? domain : "source_adapter", name, target,
                                     CBM_RESOLVE_BINDING_UNIQUE, CBM_RESOLVE_ANALYSIS_COMPLETE, 0,
                                     true, decision);
    result.binding = CBM_RESOLVE_BINDING_UNIQUE;
    result.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    result.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
    result.rule = rule;
    result.facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                   CBM_RESOLVE_FACT_OCCURRENCE_EXACT | fact | facts;
    result.target = target;
    result.candidate_count = 1;
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

bool cbm_resolve_binding_python_import_write(const cbm_resolve_binding_index_t *index,
                                             const char *name, size_t offset, const char **member) {
    *member = NULL;
    if (!index || !name || index->python_writes_unseen)
        return offset == 0;
    size_t low = 0, high = index->python_write_count;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (strcmp(index->python_writes[middle].name, name) < 0)
            low = middle + 1;
        else
            high = middle;
    }
    if (offset >= index->python_write_count - low ||
        strcmp(index->python_writes[low + offset].name, name))
        return false;
    *member = index->python_writes[low + offset].member;
    return true;
}

/* A completed audit saw every statement that binds at module level, including
 * imports and conditionals, so the name table is that namespace's static domain.
 * Module __getattr__ answers any absent name; a star import is judged later by
 * the shared resolver, which knows what its source module provides. */
bool cbm_resolve_binding_python_namespace_closed(const cbm_resolve_binding_index_t *index) {
    bool present = false;
    if (!index || index->language != CBM_LANG_PYTHON ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return false;
    (void)find_scoped_name(index, "__getattr__", strlen("__getattr__"), 0, &present, JS_SPACE_ANY);
    return !present && !index->python_namespace_open;
}
/* Names this module binds nothing for can still be there at run time: __getattr__, a computed
 * store into globals(), or handing its own __name__ on (celery's recreate_module,
 * lazy_loader.attach). */
bool cbm_resolve_binding_python_names_come_at_run_time(const cbm_resolve_binding_index_t *index) {
    return index &&
           (!cbm_resolve_binding_python_namespace_closed(index) || index->python_name_handed);
}
size_t cbm_resolve_binding_python_namespace_size(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_PYTHON ? index->name_count : 0;
}
const char *cbm_resolve_binding_python_namespace_name(const cbm_resolve_binding_index_t *index,
                                                      size_t offset, uint32_t *at) {
    if (!index || index->language != CBM_LANG_PYTHON || offset >= index->name_count ||
        index->names[offset].scope != 0)
        return NULL;
    if (at)
        *at = index->names[offset].at;
    return index->names[offset].name;
}
size_t cbm_resolve_binding_python_class_count(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_PYTHON ? index->python_class_count : 0;
}
const cbm_resolve_python_class_t *cbm_resolve_binding_python_class_at(
    const cbm_resolve_binding_index_t *index, size_t offset) {
    return offset < cbm_resolve_binding_python_class_count(index) ? &index->python_classes[offset]
                                                                  : NULL;
}
size_t cbm_resolve_binding_python_member_count(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_PYTHON ? index->python_member_count : 0;
}
const cbm_resolve_python_member_t *cbm_resolve_binding_python_member_at(
    const cbm_resolve_binding_index_t *index, size_t offset) {
    return offset < cbm_resolve_binding_python_member_count(index) ? &index->python_members[offset]
                                                                   : NULL;
}
bool cbm_resolve_binding_python_site(const cbm_resolve_binding_index_t *index, const char *name,
                                     uint32_t start, uint32_t end, CBMSourceOrigin origin,
                                     const char **chain, uint32_t *scope) {
    if (!index || index->language != CBM_LANG_PYTHON ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !name || !index->site_count ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start)
        return false;
    binding_site_t key = {.name = name, .start = start, .end = end};
    const binding_site_t *site =
        bsearch(&key, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    if (!site || !site->chain)
        return false;
    *chain = site->chain;
    *scope = site->scope;
    return true;
}
const char *cbm_resolve_binding_python_method_owner(const cbm_resolve_binding_index_t *index,
                                                    uint32_t scope) {
    if (!index || scope >= index->scope_count || !index->scopes[scope].method_of)
        return NULL;
    return index->scopes[index->scopes[scope].method_of - 1].qualified_name;
}
static int python_narrow_compare(const void *left, const void *right) {
    const python_narrow_t *a = left, *b = right;
    if (a->from != b->from)
        return a->from < b->from ? -1 : 1;
    return a->until == b->until ? 0 : a->until > b->until ? -1 : 1;
}

static int python_store_compare(const void *left, const void *right) {
    const python_store_t *a = left, *b = right;
    int order = strcmp(a->chain, b->chain);
    if (order)
        return order;
    return a->at == b->at ? 0 : a->at < b->at ? -1 : 1;
}

/* Sorted once after the walk. Regions nest or are disjoint, so one pass with the chain of open
 * regions finds each one's parent, and a site reaches its enclosing regions by those links. */
static void python_finish_narrows(cbm_resolve_binding_index_t *index) {
    if (index->python_narrow_count > 1)
        qsort(index->python_narrows, index->python_narrow_count, sizeof(*index->python_narrows),
              python_narrow_compare);
    if (index->python_store_count > 1)
        qsort(index->python_stores, index->python_store_count, sizeof(*index->python_stores),
              python_store_compare);
    uint32_t open = 0;
    for (size_t i = 0; i < index->python_narrow_count; i++) {
        python_narrow_t *narrow = &index->python_narrows[i];
        while (open && index->python_narrows[open - 1].until < narrow->from)
            open = index->python_narrows[open - 1].parent;
        narrow->parent = open;
        open = (uint32_t)i + 1;
    }
}

/* Any store of exactly this attribute chain between two positions. */
static bool python_attribute_stored(const cbm_resolve_binding_index_t *index, const char *chain,
                                    uint32_t from, uint32_t to) {
    size_t low = 0, high = index->python_store_count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        int order = strcmp(index->python_stores[mid].chain, chain);
        if (order < 0 || (!order && index->python_stores[mid].at < from))
            low = mid + 1;
        else
            high = mid;
    }
    return low < index->python_store_count && !strcmp(index->python_stores[low].chain, chain) &&
           index->python_stores[low].at <= to;
}

/* Any binder of this local between two positions. */
static bool python_local_rebound(const cbm_resolve_binding_index_t *index, const char *name,
                                 size_t length, uint32_t scope, uint32_t from, uint32_t to) {
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
    for (size_t i = low;
         i < index->name_count && index->names[i].scope == scope &&
         strlen(index->names[i].name) == length && !strncmp(index->names[i].name, name, length);
         i++)
        if (index->names[i].at >= from && index->names[i].at <= to)
            return true;
    return false;
}

/* A store of this exact chain finished before the site, in the scope holding the site or one
 * around it. Scopes nest as text does, so when the site's own scope has such a store the nearest
 * store before the site lies in it or inside it: one search, two walks up the nesting. */
static const python_store_t *python_stored_earlier(const cbm_resolve_binding_index_t *index,
                                                   const char *chain, size_t length, uint32_t scope,
                                                   uint32_t site) {
    size_t low = 0, high = index->python_store_count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        const python_store_t *store = &index->python_stores[mid];
        int order = strncmp(store->chain, chain, length);
        if (!order && store->chain[length])
            order = 1;
        if (order < 0 || (!order && store->at <= site))
            low = mid + 1;
        else
            high = mid;
    }
    if (!low)
        return NULL;
    const python_store_t *store = &index->python_stores[low - 1];
    if (strncmp(store->chain, chain, length) || store->chain[length])
        return NULL;
    uint32_t inside = store->scope, around = scope;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        if (inside == scope || around == store->scope)
            return store;
        if (!inside && !around)
            return NULL;
        inside = inside ? index->scopes[inside].parent : 0;
        around = around ? index->scopes[around].parent : 0;
    }
    return NULL;
}

/* `t.x = make()` says what t.x holds from there on, whatever class t.x is declared in, for what
 * is read through it: t.x itself stays the member it names. Stores through a method's own first
 * parameter are read as the class's instance attributes, so only what lies past its first
 * attribute counts for that root. The longest such prefix of the chain answers. */
bool cbm_resolve_binding_python_store_precedes(const cbm_resolve_binding_index_t *index,
                                               const char *chain, uint32_t scope, uint32_t site,
                                               size_t *prefix, const char **value,
                                               uint32_t *value_at, uint32_t *value_scope) {
    if (!index || !index->python_store_count)
        return false;
    size_t at = strcspn(chain, ".([@");
    cbm_resolve_python_root_kind_t root =
        cbm_resolve_binding_python_root(index, chain, at, scope, site).kind;
    bool own = root == CBM_PY_ROOT_SELF_PARAMETER || root == CBM_PY_ROOT_CLASS_PARAMETER;
    bool found = false;
    for (bool first = true; chain[at] == '.'; first = false) {
        at += 1 + strcspn(chain + at + 1, ".([@");
        const python_store_t *store = chain[at] && !(first && own)
                                          ? python_stored_earlier(index, chain, at, scope, site)
                                          : NULL;
        if (store) {
            found = true;
            *prefix = at;
            *value = store->value;
            *value_at = store->value_at;
            *value_scope = store->scope;
        }
    }
    return found;
}

const char *cbm_resolve_binding_python_narrowed(const cbm_resolve_binding_index_t *index,
                                                const char *chain, uint32_t scope, uint32_t site,
                                                size_t *consumed) {
    if (!index || !index->python_narrow_count)
        return NULL;
    size_t low = 0, high = index->python_narrow_count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        if (index->python_narrows[mid].from <= site)
            low = mid + 1;
        else
            high = mid;
    }
    for (uint32_t at = (uint32_t)low; at; at = index->python_narrows[at - 1].parent) {
        const python_narrow_t *narrow = &index->python_narrows[at - 1];
        size_t length = strlen(narrow->chain);
        if (narrow->until < site || narrow->scope != scope ||
            strncmp(chain, narrow->chain, length) ||
            (chain[length] && !strchr(".([@", chain[length])))
            continue;
        size_t root = strcspn(narrow->chain, ".([@");
        if (python_local_rebound(index, narrow->chain, root, scope, narrow->from, site) ||
            (narrow->chain[root] &&
             python_attribute_stored(index, narrow->chain, narrow->from, site)))
            return NULL;
        *consumed = length;
        return narrow->type;
    }
    return NULL;
}

/* A later binder inside a loop that holds the site but began after this binder comes back
 * round to the site without passing it. Nesting is what bounds the hops. */
static bool python_loop_comes_back(const cbm_resolve_binding_index_t *index, uint32_t binder,
                                   uint32_t site, uint32_t later) {
    size_t low = 0, high = index->python_loop_count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        if (index->python_loops[mid].start <= site)
            low = mid + 1;
        else
            high = mid;
    }
    for (uint32_t at = (uint32_t)low; at;) {
        const python_loop_t *loop = &index->python_loops[at - 1];
        if (loop->start <= binder)
            return false;
        if (loop->end >= site && loop->end >= later)
            return true;
        at = loop->parent;
    }
    return false;
}

cbm_resolve_python_flow_t cbm_resolve_binding_python_reaching(
    const cbm_resolve_binding_index_t *index, const char *name, size_t length, uint32_t scope,
    uint32_t site, const char **chain, uint32_t *at, uint32_t *chain_scope, bool *typed) {
    if (!index || index->language != CBM_LANG_PYTHON ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !scope || scope >= index->scope_count ||
        index->scopes[scope].class_scope)
        return CBM_PY_FLOW_NONE;
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
    const binding_name_t *best = NULL, *later = NULL;
    size_t count = 0;
    for (; low + count < index->name_count && index->names[low + count].scope == scope &&
           strlen(index->names[low + count].name) == length &&
           !strncmp(index->names[low + count].name, name, length);
         count++) {
        const binding_name_t *binder = &index->names[low + count];
        uint32_t from = binder->value_from ? binder->value_from : binder->at;
        if (from <= site) {
            if (!best || binder->at > best->at)
                best = binder;
        } else if (!later || binder->at < later->at) {
            later = binder;
        }
        if (count > 64)
            return CBM_PY_FLOW_NONE;
    }
    if (count < 2 || !best || best->value_until < site ||
        (later && python_loop_comes_back(index, best->at, site, later->at)))
        return CBM_PY_FLOW_NONE;
    if (best->flags != BINDING_BLOCKED || best->value_none || !best->value_chain ||
        (best->value_kind != CBM_PY_ROOT_ASSIGNED && best->value_kind != CBM_PY_ROOT_TYPED))
        return CBM_PY_FLOW_UNKNOWN;
    *chain = best->value_chain;
    *at = best->at;
    *chain_scope = best->value_scope;
    *typed = best->value_kind == CBM_PY_ROOT_TYPED;
    return CBM_PY_FLOW_VALUE;
}

/* A local rebound only by plain assignments: x = None, then x = Store(). A None
 * says nothing about the class a member read finds. Anything else that binds
 * the name leaves it unknown. */
static cbm_resolve_python_root_t python_assigned_root(const cbm_resolve_binding_index_t *index,
                                                      const char *name, size_t length,
                                                      uint32_t scope) {
    cbm_resolve_python_root_t root = {.kind = CBM_PY_ROOT_UNKNOWN};
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
    size_t count = 0;
    while (low + count < index->name_count && index->names[low + count].scope == scope &&
           strlen(index->names[low + count].name) == length &&
           !strncmp(index->names[low + count].name, name, length))
        count++;
    if (count < 2 || count > 16 || !scope)
        return root;
    const char **chains = cbm_arena_alloc(index->arena, count * sizeof(*chains));
    if (!chains)
        return root;
    uint32_t used = 0;
    for (size_t i = 0; i < count; i++) {
        const binding_name_t *binder = &index->names[low + i];
        if (binder->flags != BINDING_BLOCKED || binder->value_kind != CBM_PY_ROOT_ASSIGNED)
            return root;
        if (binder->value_none)
            continue;
        if (!binder->value_chain)
            return root;
        chains[used++] = binder->value_chain;
    }
    root.kind = CBM_PY_ROOT_ASSIGNED_MANY;
    root.chains = chains;
    root.chain_count = used;
    root.chain_scope = scope;
    return root;
}

cbm_resolve_python_root_t cbm_resolve_binding_python_root(const cbm_resolve_binding_index_t *index,
                                                          const char *name, size_t length,
                                                          uint32_t scope, uint32_t at) {
    cbm_resolve_python_root_t root = {.kind = CBM_PY_ROOT_UNKNOWN};
    if (!index || index->language != CBM_LANG_PYTHON ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return root;
    uint32_t current = scope;
    for (unsigned depth = 0; depth < BINDING_MAX_DEPTH; depth++) {
        bool present = false;
        const binding_name_t *found =
            find_scoped_name(index, name, length, current, &present, JS_SPACE_ANY);
        if (present) {
            if (!found)
                return python_assigned_root(index, name, length, current);
            if (at && found->at > at)
                return root;
            root.at = found->at;
            if (found->flags & BINDING_IMPORT) {
                root.kind = CBM_PY_ROOT_IMPORT;
                root.nested =
                    found->nested_import ? &index->python_nested[found->nested_import - 1] : NULL;
            } else if ((found->flags & BINDING_BLOCKED) && found->nested_import &&
                       index->python_nested[found->nested_import - 1].annotation_only) {
                root.kind = CBM_PY_ROOT_ANNOTATION_IMPORT;
                root.nested = &index->python_nested[found->nested_import - 1];
            } else if (found->flags & BINDING_DECORATED) {
                /* py_members decides whether the decorators keep this identity. */
                root.kind = CBM_PY_ROOT_DECLARATION;
                root.target = found->target;
            } else if (found->flags & BINDING_BLOCKED) {
                root.kind = found->value_kind ? found->value_kind : CBM_PY_ROOT_UNKNOWN;
                root.chain = found->value_chain;
                root.chain_scope = found->value_scope;
                if (found->value_kind == CBM_PY_ROOT_SELF_PARAMETER ||
                    found->value_kind == CBM_PY_ROOT_CLASS_PARAMETER)
                    root.owner = cbm_resolve_binding_python_method_owner(index, current);
            } else if (found->flags & BINDING_VALUE_DECLARATION) {
                root.kind = CBM_PY_ROOT_MODULE_VALUE;
                root.target = found->target;
                root.chain = found->value_chain;
                root.chain_scope = found->value_scope;
                root.value_typed = found->value_kind == CBM_PY_ROOT_TYPED;
            } else if (found->flags & (BINDING_DECLARATION | BINDING_VALUE_TYPE)) {
                root.kind = CBM_PY_ROOT_DECLARATION;
                root.target = found->target;
            }
            return root;
        }
        if (!current || current >= index->scope_count)
            break;
        current = index->scopes[current].parent;
    }
    root.kind = CBM_PY_ROOT_UNBOUND;
    return root;
}
/* Attach the audited member selection to this exact occurrence. The caller
 * established the class and MRO facts; this records them with the lexical root
 * that the receiver came from. */
cbm_resolve_result_t cbm_resolve_binding_python_member(const cbm_resolve_binding_index_t *index,
                                                       const char *name, uint32_t start,
                                                       uint32_t end, CBMSourceOrigin origin,
                                                       const char *owner, const char *target,
                                                       bool class_target, bool property_read) {
    cbm_resolve_lookup_t query;
    binding_query_begin(index, &query, name, start, end, origin);
    (void)cbm_resolve_lookup_observe(&query, "source_occurrences", name, name,
                                     CBM_RESOLVE_BINDING_UNIQUE, CBM_RESOLVE_ANALYSIS_COMPLETE, 0,
                                     true, "exact_occurrence_observed");
    (void)cbm_resolve_lookup_observe(&query, owner, name, target, CBM_RESOLVE_BINDING_UNIQUE,
                                     CBM_RESOLVE_ANALYSIS_COMPLETE, 0, true,
                                     "python_receiver_member_selected");
    cbm_resolve_result_t result = {
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_TYPE_MEMBER,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT |
                 (class_target ? CBM_RESOLVE_FACT_CLASS_CALLABLE : 0),
        .target = target,
        .candidate_count = 1,
        .reason = property_read ? CBM_RESOLVE_PYTHON_PROPERTY_READ : "python_receiver_member"};
    return binding_query_finish(index, &query, result);
}
size_t cbm_resolve_binding_python_decorated_count(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_PYTHON ? index->python_decorated_count : 0;
}
const cbm_resolve_python_decorated_t *cbm_resolve_binding_python_decorated_at(
    const cbm_resolve_binding_index_t *index, size_t offset) {
    return offset < cbm_resolve_binding_python_decorated_count(index)
               ? &index->python_decorated[offset]
               : NULL;
}
void cbm_resolve_binding_python_keep_decorated(cbm_resolve_binding_index_t *index,
                                               const cbm_resolve_python_decorated_t *item) {
    bool present = false;
    if (!index || !item)
        return;
    binding_name_t *binding = (binding_name_t *)find_scoped_name(
        index, item->name, strlen(item->name), item->scope, &present, JS_SPACE_ANY);
    if (binding && (binding->flags & BINDING_DECORATED))
        binding->flags =
            item->is_class ? BINDING_TYPE_DECLARATION | BINDING_VALUE_TYPE : BINDING_DECLARATION;
}
bool cbm_resolve_binding_python_import_is(const cbm_resolve_binding_index_t *index,
                                          const char *chain, const char *canonical) {
    const char *spelled = index && chain ? python_import_canonical(index, chain) : NULL;
    return spelled && !strcmp(spelled, canonical);
}
size_t cbm_resolve_binding_python_star_count(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_PYTHON ? index->python_star_count : 0;
}
const cbm_resolve_python_star_t *cbm_resolve_binding_python_star_at(
    const cbm_resolve_binding_index_t *index, size_t offset) {
    return offset < cbm_resolve_binding_python_star_count(index) ? &index->python_stars[offset]
                                                                 : NULL;
}
void cbm_resolve_binding_python_set_star_barrier(cbm_resolve_binding_index_t *index,
                                                 uint32_t barrier) {
    if (index && index->language == CBM_LANG_PYTHON)
        index->python_wildcard_barrier = barrier;
}
size_t cbm_resolve_binding_python_nested_count(const cbm_resolve_binding_index_t *index) {
    return index && index->language == CBM_LANG_PYTHON ? index->python_nested_count : 0;
}
const cbm_resolve_python_nested_import_t *cbm_resolve_binding_python_nested_at(
    const cbm_resolve_binding_index_t *index, size_t offset) {
    return offset < cbm_resolve_binding_python_nested_count(index) ? &index->python_nested[offset]
                                                                   : NULL;
}
bool cbm_resolve_binding_python_nested_audited(const cbm_resolve_binding_index_t *index,
                                               const cbm_resolve_python_nested_import_t *import) {
    bool present = false;
    if (!index || !import || index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return false;
    const char *name = import->import.local_name;
    const binding_name_t *binding =
        find_scoped_name(index, name, strlen(name), import->scope, &present, JS_SPACE_ANY);
    return binding && binding->nested_import &&
           &index->python_nested[binding->nested_import - 1] == import;
}
const cbm_resolve_python_nested_import_t *cbm_resolve_binding_python_nested_site(
    const cbm_resolve_binding_index_t *index, const char *name, uint32_t start, uint32_t end,
    CBMSourceOrigin origin) {
    if (!index || index->language != CBM_LANG_PYTHON || !index->python_nested_count ||
        index->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !name || !index->site_count ||
        origin != CBM_SOURCE_ORIGIN_RAW)
        return NULL;
    binding_site_t key = {.name = name, .start = start, .end = end};
    const binding_site_t *site =
        bsearch(&key, index->sites, index->site_count, sizeof(*index->sites), compare_site);
    const char *dot = strchr(name, '.');
    size_t root = dot ? (size_t)(dot - name) : strlen(name);
    const binding_name_t *binding =
        site && !site->attribute_terminal
            ? find_lexical_name(index, name, root, site->scope, NULL, JS_SPACE_ANY)
            : NULL;
    return binding && binding->flags == BINDING_IMPORT && binding->nested_import
               ? &index->python_nested[binding->nested_import - 1]
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
