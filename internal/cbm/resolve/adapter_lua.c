/* Lua binding rules, from the reference manual (§3.5 visibility, §2.2 environments, §3.3
 * statements, §6.3 modules).
 *
 * A `local` is lexical: it is visible from the statement after its declaration to the end of
 * its block, `local function f` also inside f, and nothing at runtime can rebind the name. A
 * name no local binds is a field of the environment, one table the whole program shares, so
 * `function foo()` and `function vim.api.foo()` declare in one global namespace that every
 * file reads. Tables are plain values: `function M.f()`, `M.f = v` and `{ f = v }` declare a
 * member of the table the name holds. `require('a.b')` is the value file a/b.lua returned,
 * through package.path: `./?.lua`, `./?/init.lua`, and a project's `lua/` root.
 *
 * Nothing here is a name match. A target is reached by following one value from a site: a
 * local's single assignment, a table's member, a file's single returned value, a function's
 * single returned value, a metatable's `__index`. Each step abstains when it is not exactly
 * one thing: a local written twice, a member with two declarations, two different returns.
 * A write through a computed key (`M[k] = v`) is taken to write no member it does not spell.
 *
 * What can change a binding is only what the manual says can: `local _ENV` and `setfenv`
 * replace where free names live, and `debug.setlocal` writes a local. `load`, `dofile` and
 * `_G.x = v` do not touch a lexical name, and the last is one more global declaration.
 *
 * The walk is the same in both phases, so a name binds the same declaration in both. Phase one
 * records values as encoded access paths, because another file's table is unknown until every
 * file was seen; phase two evaluates them, with a memo per file, so each distinct path costs
 * once and a cycle ends. Every lookup is a hash probe. */
#include "source_adapter_internal.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

/* Encoded value: a root, then steps. `G` the global table, `T:f\x1db` a table constructor,
 * `F:f\x1db` a function, `L:f\x1db` a local, `R:dir\x1dname` a require. A step is `.name`, `()`,
 * or `#n`: value n of a call, from 1 past the first. U+001E and U+001D cannot occur in an
 * identifier or in a repository path we index. */
#define LUA_STEP "\x1e"
#define LUA_PART "\x1d"
#define LUA_GLOBALS "G"
#define LUA_META "\x01meta"
#define LUA_LAZY "\x01lazy"
#define LUA_SHAPE "\x01shape"
#define LUA_MODULES "R:"
#define LUA_RETURNS "M:"
#define LUA_FLAGS "E:"
#define LUA_OWN_GLOBAL "H:"
#define LUA_CLASSES "C:"
#define LUA_ANNOTATED "N:"
#define LUA_PARENTS "P:"
#define LUA_CLASS_OF "\x01class"
#define LUA_RETURN_AT "\x01return"

enum {
    LUA_MAX_EVAL_DEPTH = 256,
    LUA_MAX_CANON_HOPS = 32,
    LUA_DOC_PARAMS = 32,
    LUA_VALUE_NAMES = 4,
    LUA_RETURN_VALUES = 8
};

typedef enum { LUA_UNKNOWN = 0, LUA_TABLE, LUA_FUNCTION } lua_kind_t;

typedef struct {
    lua_kind_t kind;
    const char *key; /* TABLE: its identity */
    /* TABLE: access paths that reached it. A write through any of them landed on it. */
    const char *names[LUA_VALUE_NAMES];
    const cbm_source_decl_t *function;
    const char *reason;
    bool absent; /* TABLE: a path nothing declares */
    bool pending;
} lua_value_t;

typedef struct lua_binder {
    const char *name;
    const char *identity;
    const char *qn; /* phase two: the graph node declared for it */
    struct lua_binder *shadowed;
    struct lua_binder *below;
    bool assigned; /* its declaration or first write gave it a value */
    bool global;   /* Lua 5.5 `global x`: the name is a global here */
} lua_binder_t;

typedef struct {
    const char *value;
    bool open;
    /* The values after the first: `return a, b`. */
    const char *later[LUA_RETURN_VALUES];
    bool later_open[LUA_RETURN_VALUES];
} lua_flow_t;

/* LuaCATS annotations on the statement that follows them. Only a type that is one name counts:
 * `string|nil` and `fun(): x` select no class. */
typedef struct {
    const char *class_name;
    const char *type_name;
    const char *return_type;
    const char *param_names[LUA_DOC_PARAMS];
    const char *param_types[LUA_DOC_PARAMS];
    unsigned param_count;
    uint32_t last_row;
    bool present;
} lua_doc_t;

typedef struct {
    cbm_source_walk_t *walk;
    CBMHashTable *visible;
    CBMHashTable *memo;
    lua_binder_t *top;
    lua_flow_t *flow;
    const char *directory;
    lua_doc_t *doc; /* phase one: what annotates the statement being visited */
    lua_binder_t *last_local;
    const char *module_table; /* a literal in this statement has `@module` fields */
    unsigned function_depth;
    bool collecting;
    bool environment_open; /* setfenv or `_ENV = x`: free names are not the globals */
} lua_ctx_t;

typedef struct {
    TSNode *items;
    uint32_t count;
} lua_nodes_t;

static void lua_visit(lua_ctx_t *ctx, TSNode node, unsigned depth);
static lua_value_t lua_eval(lua_ctx_t *ctx, const char *encoded, unsigned depth);

/* ── Syntax ────────────────────────────────────────────────────────── */

static bool lua_ignored(TSNode node) {
    return cbm_source_is(node, "comment") || cbm_source_is(node, "hash_bang_line");
}

static lua_nodes_t lua_children(lua_ctx_t *ctx, TSNode node, const char *field) {
    lua_nodes_t nodes = {0};
    uint32_t capacity = ts_node_is_null(node) ? 0 : ts_node_named_child_count(node);
    if (!capacity)
        return nodes;
    nodes.items = cbm_arena_alloc(ctx->walk->arena, capacity * sizeof(*nodes.items));
    if (!nodes.items) {
        cbm_source_fail(ctx->walk, "lua_scope_allocation");
        return nodes;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child) || lua_ignored(child))
                continue;
            if (field) {
                const char *name = ts_tree_cursor_current_field_name(&cursor);
                if (!name || strcmp(name, field))
                    continue;
            }
            nodes.items[nodes.count++] = child;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return nodes;
}

/* The text of a literal string with no escape in it; anything else is not spelled. */
static const char *lua_literal(lua_ctx_t *ctx, TSNode node) {
    if (!cbm_source_is(node, "string") || ts_node_named_child_count(node) != 1)
        return NULL;
    TSNode content = cbm_source_field(node, "content");
    return cbm_source_is(content, "string_content") ? cbm_source_text(ctx->walk, content) : NULL;
}

static bool lua_is_index(TSNode node) {
    return cbm_source_is(node, "dot_index_expression") ||
           cbm_source_is(node, "bracket_index_expression") ||
           cbm_source_is(node, "method_index_expression");
}

/* The member an index expression spells, NULL when it computes one. */
static const char *lua_index_member(lua_ctx_t *ctx, TSNode node) {
    if (cbm_source_is(node, "dot_index_expression"))
        return cbm_source_text(ctx->walk, cbm_source_field(node, "field"));
    if (cbm_source_is(node, "method_index_expression"))
        return cbm_source_text(ctx->walk, cbm_source_field(node, "method"));
    if (cbm_source_is(node, "bracket_index_expression"))
        return lua_literal(ctx, cbm_source_field(node, "field"));
    return NULL;
}

/* ── Scope ─────────────────────────────────────────────────────────── */

static const char *lua_identity(lua_ctx_t *ctx, char kind, uint32_t at) {
    return cbm_arena_sprintf(ctx->walk->arena, "%c:%s" LUA_PART "%u", kind, ctx->walk->file, at);
}

static lua_binder_t *lua_binder_of(const lua_ctx_t *ctx, const char *name) {
    lua_binder_t *binder = name ? cbm_ht_get(ctx->visible, name) : NULL;
    return binder && !binder->global ? binder : NULL;
}

/* `local _ENV` or `function f(_ENV)`: free names here are fields of that value. */
static bool lua_environment_replaced(const lua_ctx_t *ctx) {
    return ctx->environment_open || cbm_ht_get(ctx->visible, "_ENV") != NULL;
}

/* line is where the statement declaring it starts, when that statement can have a graph node:
 * 0 for a parameter or a loop variable, which never do. */
static lua_binder_t *lua_push(lua_ctx_t *ctx, const char *name, uint32_t at, uint32_t line) {
    lua_binder_t *binder = name ? cbm_arena_calloc(ctx->walk->arena, sizeof(*binder)) : NULL;
    if (!binder) {
        if (name)
            cbm_source_fail(ctx->walk, "lua_scope_allocation");
        return NULL;
    }
    binder->name = name;
    binder->identity = lua_identity(ctx, 'L', at);
    if (!ctx->collecting && line)
        binder->qn = cbm_source_def_qn_at(ctx->walk, name, line);
    binder->shadowed = cbm_ht_get(ctx->visible, name);
    binder->below = ctx->top;
    ctx->top = binder;
    cbm_ht_set(ctx->visible, name, binder);
    return binder;
}

static void lua_pop_to(lua_ctx_t *ctx, lua_binder_t *mark) {
    while (ctx->top != mark) {
        lua_binder_t *binder = ctx->top;
        ctx->top = binder->below;
        if (binder->shadowed)
            cbm_ht_set(ctx->visible, binder->name, binder->shadowed);
        else
            cbm_ht_delete(ctx->visible, binder->name);
    }
}

/* ── Phase one: facts ──────────────────────────────────────────────── */

static const cbm_source_decl_t *lua_fact(lua_ctx_t *ctx, const char *root) {
    const char *part = strchr(root, LUA_PART[0]);
    if (!part)
        return NULL;
    const char *owner = cbm_arena_strndup(ctx->walk->arena, root, (size_t)(part - root));
    return owner ? cbm_source_lookup(ctx->walk, owner, part + 1) : NULL;
}

static cbm_source_decl_t *lua_declare(lua_ctx_t *ctx, const char *owner, const char *name,
                                      const char *qn, const char *type) {
    cbm_source_decl_t *decl = cbm_source_declare(ctx->walk, owner, name, qn, 0, 0);
    if (decl && type)
        decl->type = cbm_source_table_intern(ctx->walk, type);
    return decl;
}

static void lua_declare_root(lua_ctx_t *ctx, const char *root, const char *qn, const char *type) {
    const char *part = strchr(root, LUA_PART[0]);
    const char *owner =
        part ? cbm_arena_strndup(ctx->walk->arena, root, (size_t)(part - root)) : NULL;
    if (owner)
        (void)lua_declare(ctx, owner, part + 1, qn, type);
}

/* `vim.iter = require('vim.iter')` written by two files in two directories names one module. */
static bool lua_same_value(const char *left, const char *right) {
    if (!left || !right)
        return false;
    if (!strcmp(left, right))
        return true;
    if (strncmp(left, LUA_MODULES, strlen(LUA_MODULES)) ||
        strncmp(right, LUA_MODULES, strlen(LUA_MODULES)))
        return false;
    const char *left_name = strchr(left, LUA_PART[0]);
    const char *right_name = strchr(right, LUA_PART[0]);
    return left_name && right_name && !strcmp(left_name, right_name);
}

/* One entry per (table, member). Declaring a second, different value marks it instead of
 * growing a list, so a member assigned many times stays one probe. Two functions on one QN
 * are one node of the graph, so they are one declaration here. A chunk's own statements declare
 * the table; a write inside a function happens when it runs, as a test's
 * `vim.fn.has = function() ... end` mock does, and does not compete with them. */
static void lua_member_fact(lua_ctx_t *ctx, const char *owner, const char *name, const char *qn,
                            const char *type) {
    bool runtime = ctx->function_depth > 0;
    cbm_source_decl_t *head = (cbm_source_decl_t *)cbm_source_lookup(ctx->walk, owner, name);
    if (!head) {
        cbm_source_decl_t *decl = lua_declare(ctx, owner, name, qn, type);
        if (decl && runtime)
            decl->flags |= CBM_SOURCE_DECL_F_CONDITIONAL;
        return;
    }
    bool head_runtime = (head->flags & CBM_SOURCE_DECL_F_CONDITIONAL) != 0;
    if (runtime && !head_runtime)
        return;
    bool same = (qn && head->qn && !strcmp(qn, head->qn)) || lua_same_value(type, head->type);
    if (same && runtime == head_runtime)
        return;
    if (!runtime && head_runtime) {
        head->qn = qn ? cbm_source_table_intern(ctx->walk, qn) : NULL;
        head->type = type ? cbm_source_table_intern(ctx->walk, type) : NULL;
        head->file = cbm_source_table_intern(ctx->walk, ctx->walk->file);
        head->flags &= ~(uint32_t)(CBM_SOURCE_DECL_F_CONDITIONAL | CBM_SOURCE_DECL_F_OPEN);
        return;
    }
    head->flags |= CBM_SOURCE_DECL_F_OPEN;
}

/* A global also gets an entry for its own file: two scripts of one project each declaring
 * `function usage()` are one name to the program, but each script reads its own. */
static void lua_write_member(lua_ctx_t *ctx, const char *owner, const char *name, const char *qn,
                             const char *type) {
    if (!owner || !name)
        return;
    lua_member_fact(ctx, owner, name, qn, type);
    if (owner[0] == LUA_GLOBALS[0]) {
        const char *own = cbm_arena_sprintf(ctx->walk->arena, LUA_OWN_GLOBAL "%s" LUA_PART "%s",
                                            ctx->walk->file, owner);
        if (own)
            lua_member_fact(ctx, own, name, qn, type);
    }
}

static bool lua_made_by_call(const char *encoded) {
    return strstr(encoded, LUA_STEP "()") || strstr(encoded, LUA_STEP "#");
}

/* The table a write through this path lands in: a local's value when it has one. */
static const char *lua_owner_key(lua_ctx_t *ctx, const char *encoded, unsigned hops) {
    if (!encoded || encoded[0] != 'L')
        return encoded;
    if (hops > LUA_MAX_CANON_HOPS)
        return NULL;
    const char *step = strchr(encoded, LUA_STEP[0]);
    size_t length = step ? (size_t)(step - encoded) : strlen(encoded);
    const char *root = cbm_arena_strndup(ctx->walk->arena, encoded, length);
    const cbm_source_decl_t *fact = root ? lua_fact(ctx, root) : NULL;
    if (!fact || !fact->type || (fact->flags & CBM_SOURCE_DECL_F_OPEN))
        return NULL;
    /* A call makes a new table each time, so what is written through this local is its own. */
    if (lua_made_by_call(fact->type))
        return encoded;
    const char *expanded =
        cbm_arena_sprintf(ctx->walk->arena, "%s%s", fact->type, step ? step : "");
    return lua_owner_key(ctx, expanded, hops + 1);
}

/* ── Encoding: what an expression's value is, as a path ────────────── */

static const char *lua_encode(lua_ctx_t *ctx, TSNode node, unsigned depth);

static const char *lua_encode_global(lua_ctx_t *ctx, const char *name) {
    if (lua_environment_replaced(ctx))
        return NULL;
    if (!strcmp(name, "_G") || !strcmp(name, "_ENV"))
        return LUA_GLOBALS;
    return cbm_arena_sprintf(ctx->walk->arena, LUA_GLOBALS LUA_STEP ".%s", name);
}

static bool lua_global_callee(const lua_ctx_t *ctx, TSNode callee, const char *name) {
    if (!cbm_source_is(callee, "identifier"))
        return false;
    const char *text = cbm_source_text(ctx->walk, callee);
    return text && !strcmp(text, name) && !cbm_ht_get(ctx->visible, name);
}

static const char *lua_encode_require(lua_ctx_t *ctx, TSNode literal) {
    const char *module = lua_literal(ctx, literal);
    if (!module || !*module)
        return NULL;
    return cbm_arena_sprintf(ctx->walk->arena, LUA_MODULES "%s" LUA_PART "%s", ctx->directory,
                             module);
}

static const char *lua_encode_call(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    TSNode callee = cbm_source_field(node, "name");
    lua_nodes_t args = lua_children(ctx, cbm_source_field(node, "arguments"), NULL);
    if (lua_global_callee(ctx, callee, "require"))
        return args.count == 1 ? lua_encode_require(ctx, args.items[0]) : NULL;
    /* setmetatable returns its first argument. */
    if (lua_global_callee(ctx, callee, "setmetatable"))
        return args.count == 2 ? lua_encode(ctx, args.items[0], depth + 1) : NULL;
    const char *encoded = lua_encode(ctx, callee, depth + 1);
    return encoded ? cbm_arena_sprintf(ctx->walk->arena, "%s" LUA_STEP "()", encoded) : NULL;
}

/* `a or {}` is a, or a table with nothing declared in it: only a can select a member. */
static const char *lua_encode_or(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    TSNode right = cbm_source_field(node, "right");
    if (!cbm_source_token(node, "or"))
        return NULL;
    if (cbm_source_is(right, "table_constructor") || cbm_source_is(right, "nil"))
        return lua_encode(ctx, cbm_source_field(node, "left"), depth + 1);
    return NULL;
}

static const char *lua_encode(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > LUA_MAX_EVAL_DEPTH)
        return NULL;
    cbm_source_walk_t *walk = ctx->walk;
    if (cbm_source_is(node, "identifier")) {
        const char *name = cbm_source_text(walk, node);
        if (!name)
            return NULL;
        const lua_binder_t *binder = lua_binder_of(ctx, name);
        return binder ? binder->identity : lua_encode_global(ctx, name);
    }
    if (lua_is_index(node)) {
        const char *member = lua_index_member(ctx, node);
        const char *table =
            member ? lua_encode(ctx, cbm_source_field(node, "table"), depth + 1) : NULL;
        return table ? cbm_arena_sprintf(walk->arena, "%s" LUA_STEP ".%s", table, member) : NULL;
    }
    if (cbm_source_is(node, "function_call"))
        return lua_encode_call(ctx, node, depth);
    if (cbm_source_is(node, "table_constructor"))
        return lua_identity(ctx, 'T', ts_node_start_byte(node));
    if (cbm_source_is(node, "function_definition"))
        return lua_identity(ctx, 'F', ts_node_start_byte(node));
    if (cbm_source_is(node, "parenthesized_expression")) {
        lua_nodes_t inner = lua_children(ctx, node, NULL);
        return inner.count == 1 ? lua_encode(ctx, inner.items[0], depth + 1) : NULL;
    }
    if (cbm_source_is(node, "binary_expression"))
        return lua_encode_or(ctx, node, depth);
    return NULL;
}

/* ── Phase two: evaluation ─────────────────────────────────────────── */

static lua_value_t lua_unknown(const char *reason) {
    return (lua_value_t){.kind = LUA_UNKNOWN, .reason = reason};
}

/* Names the host defines. A project may wrap one in its own file, but another file's
 * declaration does not decide what the name means everywhere. */
static bool lua_standard_global(const char *name) {
    static const char *const names[] = {
        "assert",     "collectgarbage", "coroutine", "debug",    "dofile",       "error",
        "getfenv",    "getmetatable",   "io",        "ipairs",   "load",         "loadfile",
        "loadstring", "math",           "module",    "next",     "os",           "package",
        "pairs",      "pcall",          "print",     "rawequal", "rawget",       "rawlen",
        "rawset",     "require",        "select",    "setfenv",  "setmetatable", "string",
        "table",      "tonumber",       "tostring",  "type",     "unpack",       "utf8",
        "warn",       "xpcall",         "bit32",     "bit",      "jit",          "ffi",
        NULL};
    for (size_t i = 0; names[i]; i++)
        if (!strcmp(name, names[i]))
            return true;
    return false;
}

/* The host preloads these in package.loaded: `require('os')` never reads an os.lua. */
static bool lua_preloaded_library(const char *module) {
    static const char *const names[] = {"string", "table", "math",    "io",        "os",
                                        "debug",  "utf8",  "package", "coroutine", "bit",
                                        "bit32",  "ffi",   "jit",     "_G",        NULL};
    for (size_t i = 0; names[i]; i++)
        if (!strcmp(module, names[i]))
            return true;
    return false;
}

static void lua_value_add_name(lua_value_t *value, const char *name) {
    if (!name || (value->key && !strcmp(value->key, name)))
        return;
    for (size_t i = 0; i < LUA_VALUE_NAMES; i++) {
        if (!value->names[i]) {
            value->names[i] = name;
            return;
        }
        if (!strcmp(value->names[i], name))
            return;
    }
}

/* A global's own-file entry first: `function usage()` in this script is what it reads. */
static const cbm_source_decl_t *lua_own_global(lua_ctx_t *ctx, const char *table,
                                               const char *name) {
    if (!table || table[0] != LUA_GLOBALS[0])
        return NULL;
    const char *own = cbm_arena_sprintf(ctx->walk->arena, LUA_OWN_GLOBAL "%s" LUA_PART "%s",
                                        ctx->walk->file, table);
    return own ? cbm_source_lookup(ctx->walk, own, name) : NULL;
}

static const cbm_source_decl_t *lua_member_decl(lua_ctx_t *ctx, const lua_value_t *table,
                                                const char *name) {
    const cbm_source_decl_t *mine = lua_own_global(ctx, table->key, name);
    for (size_t i = 0; !mine && i < LUA_VALUE_NAMES && table->names[i]; i++)
        mine = lua_own_global(ctx, table->names[i], name);
    if (mine)
        return mine;
    const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, table->key, name);
    for (size_t i = 0; !decl && i < LUA_VALUE_NAMES && table->names[i]; i++)
        decl = cbm_source_lookup(ctx->walk, table->names[i], name);
    if (decl && !strcmp(table->key, LUA_GLOBALS) && lua_standard_global(name))
        return NULL;
    return decl;
}

static lua_value_t lua_member(lua_ctx_t *ctx, lua_value_t base, const char *name, unsigned depth);

/* `__index = function(t, k) ... require('prefix.' .. k) ...`: a member t does not have is that
 * module, when the project has it. */
static bool lua_lazy_module(lua_ctx_t *ctx, const lua_value_t *base, const char *name,
                            unsigned depth, lua_value_t *out) {
    const cbm_source_decl_t *lazy = lua_member_decl(ctx, base, LUA_LAZY);
    if (!lazy || !lazy->type)
        return false;
    const char *root = cbm_arena_sprintf(ctx->walk->arena, LUA_MODULES "%s" LUA_PART "%s%s",
                                         ctx->directory, lazy->type, name);
    if (!root)
        return false;
    lua_value_t value = lua_eval(ctx, root, depth + 1);
    if (value.kind == LUA_UNKNOWN)
        return false;
    *out = value;
    return true;
}

/* `@class C: A, B`: a member C does not declare is the parents', when one of them declares it.
 * A parent the project has no table for could hold any name, so it leaves them all open. */
static bool lua_class_member(lua_ctx_t *ctx, const lua_value_t *base, const char *name,
                             unsigned depth, lua_value_t *out) {
    const char *class_name = NULL;
    if (!strncmp(base->key, LUA_CLASSES, strlen(LUA_CLASSES))) {
        class_name = base->key + strlen(LUA_CLASSES);
    } else {
        const cbm_source_decl_t *own = lua_member_decl(ctx, base, LUA_CLASS_OF);
        if (own && !(own->flags & CBM_SOURCE_DECL_F_OPEN))
            class_name = own->type;
    }
    if (!class_name)
        return false;
    lua_value_t chosen = {0};
    bool found = false;
    for (const cbm_source_decl_t *parent = cbm_source_lookup(ctx->walk, LUA_PARENTS, class_name);
         parent; parent = parent->next) {
        if (!parent->type)
            return false;
        /* Both through the memo: a hierarchy is walked once per (class, name), and a class that
         * reaches itself comes back unknown. */
        const char *root = cbm_arena_sprintf(ctx->walk->arena, LUA_CLASSES "%s", parent->type);
        const char *path =
            root ? cbm_arena_sprintf(ctx->walk->arena, "%s" LUA_STEP ".%s", root, name) : NULL;
        if (!path || lua_eval(ctx, root, depth + 1).kind != LUA_TABLE)
            return false;
        lua_value_t member = lua_eval(ctx, path, depth + 1);
        if (member.absent && member.reason && !strcmp(member.reason, "lua_member_not_declared"))
            continue;
        if (member.absent)
            return false;
        bool same = member.kind == chosen.kind &&
                    (member.kind == LUA_FUNCTION ? member.function == chosen.function
                     : member.kind == LUA_TABLE  ? !strcmp(member.key, chosen.key)
                                                 : false);
        if (found && !same)
            return false;
        chosen = member;
        found = true;
    }
    if (found)
        *out = chosen;
    return found;
}

/* `setmetatable(t, mt)`: a member t does not have comes from mt.__index. */
static bool lua_metatable_member(lua_ctx_t *ctx, const lua_value_t *base, const char *name,
                                 unsigned depth, lua_value_t *out) {
    const cbm_source_decl_t *meta = lua_member_decl(ctx, base, LUA_META);
    if (!meta || (meta->flags & CBM_SOURCE_DECL_F_OPEN) || !meta->type)
        return false;
    lua_value_t metatable = lua_eval(ctx, meta->type, depth + 1);
    if (metatable.kind != LUA_TABLE)
        return false;
    const cbm_source_decl_t *index = lua_member_decl(ctx, &metatable, "__index");
    if (!index || (index->flags & CBM_SOURCE_DECL_F_OPEN) || !index->type)
        return false;
    lua_value_t parent = lua_eval(ctx, index->type, depth + 1);
    if (parent.kind != LUA_TABLE)
        return false;
    lua_value_t found = lua_member(ctx, parent, name, depth + 1);
    if (found.kind == LUA_TABLE && found.absent)
        return false;
    *out = found;
    return true;
}

static bool lua_inherited(lua_ctx_t *ctx, const lua_value_t *base, const char *name, unsigned depth,
                          lua_value_t *out) {
    if (lua_lazy_module(ctx, base, name, depth, out))
        return true;
    const cbm_source_decl_t *shape = lua_member_decl(ctx, base, LUA_SHAPE);
    if (shape && !(shape->flags & CBM_SOURCE_DECL_F_OPEN) && shape->type) {
        lua_value_t found = lua_member(ctx, lua_eval(ctx, shape->type, depth + 1), name, depth + 1);
        if (found.kind != LUA_UNKNOWN && !found.absent) {
            *out = found;
            return true;
        }
    }
    return lua_metatable_member(ctx, base, name, depth, out) ||
           lua_class_member(ctx, base, name, depth, out);
}

static lua_value_t lua_member(lua_ctx_t *ctx, lua_value_t base, const char *name, unsigned depth) {
    if (base.kind != LUA_TABLE)
        return lua_unknown(base.kind == LUA_FUNCTION ? "lua_member_of_a_function" : base.reason);
    if (depth > LUA_MAX_EVAL_DEPTH)
        return lua_unknown("lua_value_chain_too_long");
    lua_value_t virtual_path = {.kind = LUA_TABLE, .absent = true};
    virtual_path.key = cbm_arena_sprintf(ctx->walk->arena, "%s" LUA_STEP ".%s", base.key, name);
    if (!virtual_path.key)
        return lua_unknown("lua_value_allocation");
    for (size_t i = 0; i < LUA_VALUE_NAMES && base.names[i]; i++)
        lua_value_add_name(&virtual_path, cbm_arena_sprintf(ctx->walk->arena, "%s" LUA_STEP ".%s",
                                                            base.names[i], name));
    const cbm_source_decl_t *decl = lua_member_decl(ctx, &base, name);
    if (decl) {
        if (decl->flags & CBM_SOURCE_DECL_F_OPEN) {
            /* Which value it holds is open; a member spelled through the path is not. */
            virtual_path.reason = "lua_member_not_one_declaration";
            return virtual_path;
        }
        if (!decl->type)
            return lua_unknown("lua_member_value_unknown");
        lua_value_t value = lua_eval(ctx, decl->type, depth + 1);
        if (value.kind == LUA_TABLE) {
            lua_value_add_name(&value, virtual_path.key);
            for (size_t i = 0; i < LUA_VALUE_NAMES && virtual_path.names[i]; i++)
                lua_value_add_name(&value, virtual_path.names[i]);
        }
        return value;
    }
    lua_value_t inherited;
    if (lua_inherited(ctx, &base, name, depth, &inherited))
        return inherited;
    virtual_path.reason = "lua_member_not_declared";
    return virtual_path;
}

/* What LuaCATS says this local or function holds, for when its value does not say. */
static bool lua_annotated(lua_ctx_t *ctx, const char *owner, const char *name, unsigned depth,
                          lua_value_t *out) {
    const char *key = cbm_arena_sprintf(ctx->walk->arena, LUA_ANNOTATED "%s", owner);
    const cbm_source_decl_t *fact = key ? cbm_source_lookup(ctx->walk, key, name) : NULL;
    if (!fact || !fact->type)
        return false;
    lua_value_t value = lua_eval(ctx, fact->type, depth + 1);
    if (value.kind == LUA_UNKNOWN)
        return false;
    *out = value;
    return true;
}

/* A table's `__call`, when it is called. */
static lua_value_t lua_callable_of(lua_ctx_t *ctx, lua_value_t value, unsigned depth) {
    if (value.kind != LUA_TABLE)
        return value;
    const cbm_source_decl_t *meta = lua_member_decl(ctx, &value, LUA_META);
    if (meta && !(meta->flags & CBM_SOURCE_DECL_F_OPEN) && meta->type) {
        lua_value_t metatable = lua_eval(ctx, meta->type, depth + 1);
        if (metatable.kind == LUA_TABLE) {
            const cbm_source_decl_t *call = lua_member_decl(ctx, &metatable, "__call");
            if (call && !(call->flags & CBM_SOURCE_DECL_F_OPEN) && call->type)
                return lua_eval(ctx, call->type, depth + 1);
        }
    }
    return lua_unknown(value.absent ? value.reason : "lua_call_of_a_table_value");
}

static lua_value_t lua_call_result(lua_ctx_t *ctx, lua_value_t value, unsigned depth) {
    lua_value_t callable = lua_callable_of(ctx, value, depth);
    if (callable.kind != LUA_FUNCTION)
        return lua_unknown(callable.reason);
    lua_value_t returned = callable.function->type
                               ? lua_eval(ctx, callable.function->type, depth + 1)
                               : lua_unknown("lua_return_value_unknown");
    lua_value_t annotated;
    if ((returned.kind == LUA_UNKNOWN || returned.absent) &&
        lua_annotated(ctx, callable.function->owner, callable.function->name, depth, &annotated))
        return annotated;
    return returned;
}

/* `local a, b = f()`: b is what every `return x, y` of f gives second. */
static lua_value_t lua_call_value_at(lua_ctx_t *ctx, lua_value_t value, const char *position,
                                     unsigned depth) {
    lua_value_t callable = lua_callable_of(ctx, value, depth);
    if (callable.kind != LUA_FUNCTION)
        return lua_unknown(callable.reason);
    const cbm_source_decl_t *function = callable.function;
    const char *owner =
        cbm_arena_sprintf(ctx->walk->arena, "%s" LUA_PART "%s", function->owner, function->name);
    const char *name = cbm_arena_sprintf(ctx->walk->arena, LUA_RETURN_AT "%s", position);
    const cbm_source_decl_t *fact =
        owner && name ? cbm_source_lookup(ctx->walk, owner, name) : NULL;
    if (!fact || (fact->flags & CBM_SOURCE_DECL_F_OPEN) || !fact->type)
        return lua_unknown("lua_return_value_unknown");
    return lua_eval(ctx, fact->type, depth + 1);
}

/* Every file that this literal can load by package.path, nearest first. The working directory
 * is the requiring file's or one of its ancestors; a `lua/` root is on the path by convention,
 * and is only taken when it alone offers the name. */
static const char *lua_required_file(lua_ctx_t *ctx, const char *directory, const char *module,
                                     const char **reason) {
    cbm_source_walk_t *walk = ctx->walk;
    if (lua_preloaded_library(module)) {
        *reason = "lua_require_of_a_preloaded_library";
        return NULL;
    }
    char *path = cbm_arena_strdup(walk->arena, module);
    char *current = cbm_arena_strdup(walk->arena, directory);
    if (!path || !current)
        return NULL;
    for (char *p = path; *p; p++)
        if (*p == '.')
            *p = '/';
    for (;;) {
        const char *base =
            cbm_arena_sprintf(walk->arena, "%s%s%s", current, *current ? "/" : "", path);
        const char *first = base ? cbm_arena_sprintf(walk->arena, "%s.lua", base) : NULL;
        const char *second = base ? cbm_arena_sprintf(walk->arena, "%s/init.lua", base) : NULL;
        const char *key1 = first ? cbm_arena_sprintf(walk->arena, LUA_RETURNS "%s", first) : NULL;
        const char *key2 = second ? cbm_arena_sprintf(walk->arena, LUA_RETURNS "%s", second) : NULL;
        if (key1 && cbm_source_lookup(walk, key1, ""))
            return first;
        if (key2 && cbm_source_lookup(walk, key2, ""))
            return second;
        if (!*current)
            break;
        char *slash = strrchr(current, '/');
        if (slash)
            *slash = '\0';
        else
            current[0] = '\0';
    }
    const cbm_source_decl_t *named = cbm_source_lookup(walk, LUA_MODULES, path);
    if (!named) {
        *reason = "lua_require_outside_project";
        return NULL;
    }
    for (const cbm_source_decl_t *other = named->next; other; other = other->next) {
        if (strcmp(other->file, named->file)) {
            *reason = "lua_require_several_project_files";
            return NULL;
        }
    }
    return named->file;
}

static lua_value_t lua_eval_root(lua_ctx_t *ctx, const char *root, unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    switch (root[0]) {
    case 'G':
        return (lua_value_t){.kind = LUA_TABLE, .key = LUA_GLOBALS};
    case 'T':
        return (lua_value_t){.kind = LUA_TABLE, .key = root};
    case 'F': {
        const cbm_source_decl_t *function = lua_fact(ctx, root);
        return function ? (lua_value_t){.kind = LUA_FUNCTION, .function = function}
                        : lua_unknown("lua_function_not_collected");
    }
    case 'C': {
        const char *class_name = root + strlen(LUA_CLASSES);
        const cbm_source_decl_t *declared = cbm_source_lookup(walk, LUA_CLASSES, class_name);
        if (!declared && cbm_source_lookup(walk, LUA_PARENTS, class_name))
            return (lua_value_t){.kind = LUA_TABLE, .key = root};
        if (!declared || (declared->flags & CBM_SOURCE_DECL_F_OPEN) || !declared->type)
            return lua_unknown("lua_annotated_class_has_no_table");
        return lua_eval(ctx, declared->type, depth + 1);
    }
    case 'L': {
        const char *part = strchr(root, LUA_PART[0]);
        const char *owner =
            part ? cbm_arena_strndup(walk->arena, root, (size_t)(part - root)) : NULL;
        const char *flags = part ? cbm_arena_sprintf(walk->arena, LUA_FLAGS "%.*s",
                                                     (int)(part - root - 2), root + 2)
                                 : NULL;
        if (!owner)
            return lua_unknown("lua_value_allocation");
        if (flags && cbm_source_lookup(walk, flags, "debug"))
            return lua_unknown("lua_debug_rewrites_locals");
        const cbm_source_decl_t *fact = cbm_source_lookup(walk, owner, part + 1);
        lua_value_t value = !fact ? lua_unknown("lua_local_value_unknown")
                            : (fact->flags & CBM_SOURCE_DECL_F_OPEN)
                                ? lua_unknown("lua_local_written_more_than_once")
                            : fact->type ? lua_eval(ctx, fact->type, depth + 1)
                                         : lua_unknown("lua_local_value_unknown");
        lua_value_t annotated;
        if ((value.kind == LUA_UNKNOWN || value.absent) &&
            lua_annotated(ctx, owner, part + 1, depth, &annotated))
            return annotated;
        /* A table a call made: this local's writes are the ones that apply to it. */
        if (value.kind == LUA_TABLE && fact && fact->type && lua_made_by_call(fact->type))
            lua_value_add_name(&value, root);
        return value;
    }
    case 'R': {
        const char *part = strchr(root, LUA_PART[0]);
        if (!part)
            return lua_unknown("lua_require_not_literal");
        if (cbm_source_lookup(walk, LUA_GLOBALS, "require"))
            return lua_unknown("lua_require_redefined");
        const char *directory = cbm_arena_strndup(walk->arena, root + strlen(LUA_MODULES),
                                                  (size_t)(part - root) - strlen(LUA_MODULES));
        const char *reason = "lua_require_outside_project";
        const char *file = directory ? lua_required_file(ctx, directory, part + 1, &reason) : NULL;
        if (!file)
            return lua_unknown(reason);
        const char *key = cbm_arena_sprintf(walk->arena, LUA_RETURNS "%s", file);
        const cbm_source_decl_t *module = key ? cbm_source_lookup(walk, key, "") : NULL;
        if (!module || (module->flags & CBM_SOURCE_DECL_F_OPEN) || !module->type)
            return lua_unknown("lua_module_return_unknown");
        lua_value_t value = lua_eval(ctx, module->type, depth + 1);
        /* A file can extend what it required: `function M.more()` on `require('a')`. */
        if (value.kind == LUA_TABLE)
            lua_value_add_name(&value, root);
        return value;
    }
    default:
        return lua_unknown("lua_value_not_modelled");
    }
}

static lua_value_t lua_eval_uncached(lua_ctx_t *ctx, const char *encoded, unsigned depth) {
    if (depth > LUA_MAX_EVAL_DEPTH)
        return lua_unknown("lua_value_chain_too_long");
    cbm_source_walk_t *walk = ctx->walk;
    const char *step = strchr(encoded, LUA_STEP[0]);
    const char *root =
        step ? cbm_arena_strndup(walk->arena, encoded, (size_t)(step - encoded)) : encoded;
    if (!root)
        return lua_unknown("lua_value_allocation");
    lua_value_t value = lua_eval_root(ctx, root, depth);
    while (step) {
        const char *next = strchr(step + 1, LUA_STEP[0]);
        const char *part =
            next ? cbm_arena_strndup(walk->arena, step + 1, (size_t)(next - step - 1)) : step + 1;
        if (!part)
            return lua_unknown("lua_value_allocation");
        value = part[0] == '.'   ? lua_member(ctx, value, part + 1, depth)
                : part[0] == '#' ? lua_call_value_at(ctx, value, part + 1, depth)
                                 : lua_call_result(ctx, value, depth);
        step = next;
    }
    return value;
}

/* The memo makes each distinct path cost once per file and ends a cycle: a path that needs
 * itself has no value. */
static lua_value_t lua_eval(lua_ctx_t *ctx, const char *encoded, unsigned depth) {
    lua_value_t *known = cbm_ht_get(ctx->memo, encoded);
    if (known)
        return known->pending ? lua_unknown("lua_value_needs_itself") : *known;
    lua_value_t *slot = cbm_arena_calloc(ctx->walk->arena, sizeof(*slot));
    const char *key = cbm_arena_strdup(ctx->walk->arena, encoded);
    if (!slot || !key)
        return lua_unknown("lua_value_allocation");
    slot->pending = true;
    cbm_ht_set(ctx->memo, key, slot);
    lua_value_t value = lua_eval_uncached(ctx, encoded, depth);
    *slot = value;
    slot->pending = false;
    return value;
}

/* ── Phase two: sites ──────────────────────────────────────────────── */

static cbm_resolve_rule_t lua_rule(const lua_ctx_t *ctx, const cbm_source_decl_t *function) {
    return strcmp(function->file, ctx->walk->file) ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                                   : CBM_RESOLVE_RULE_SCOPE_BINDING;
}

static void lua_emit_call(lua_ctx_t *ctx, TSNode call, TSNode callee) {
    cbm_source_walk_t *walk = ctx->walk;
    if (lua_global_callee(ctx, callee, "require")) {
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "lua_require_loads_a_module");
        return;
    }
    const char *encoded = lua_encode(ctx, callee, 0);
    if (!encoded) {
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "lua_callee_computed");
        return;
    }
    lua_value_t value = lua_callable_of(ctx, lua_eval(ctx, encoded, 0), 0);
    if (value.kind != LUA_FUNCTION) {
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             value.reason ? value.reason : "lua_callee_value_unknown");
        return;
    }
    const char *target = value.function->qn;
    cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, target, lua_rule(ctx, value.function),
                         value.function->file,
                         target ? "lua_value_flow_selected_declaration"
                                : "lua_function_has_no_graph_node");
}

static void lua_emit_value(lua_ctx_t *ctx, TSNode site, const char *encoded) {
    cbm_source_walk_t *walk = ctx->walk;
    lua_value_t value = encoded ? lua_eval(ctx, encoded, 0) : lua_unknown("lua_value_computed");
    if (value.kind == LUA_FUNCTION) {
        const char *target = value.function->qn;
        cbm_source_emit_node(walk, site, CBM_SOURCE_SITE_REFERENCE, target,
                             lua_rule(ctx, value.function), value.function->file,
                             target ? "lua_value_flow_selected_declaration"
                                    : "lua_function_has_no_graph_node");
        return;
    }
    cbm_source_emit_node(walk, site, CBM_SOURCE_SITE_REFERENCE, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                         value.reason ? value.reason : "lua_value_is_not_a_declaration");
}

/* An identifier read: the local its scope binds, or the global namespace. */
static void lua_emit_identifier(lua_ctx_t *ctx, TSNode node, bool callee) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *name = cbm_source_text(walk, node);
    if (!name)
        return;
    const lua_binder_t *binder = lua_binder_of(ctx, name);
    if (binder) {
        const char *target = binder->qn;
        /* Calling a function declared under this very name is the call, not a read. */
        if (target && callee) {
            lua_value_t value = lua_eval(ctx, binder->identity, 0);
            if (value.kind == LUA_FUNCTION && value.function->qn &&
                !strcmp(value.function->qn, target))
                target = NULL;
        }
        cbm_source_emit_node(walk, node, CBM_SOURCE_SITE_REFERENCE, target,
                             CBM_RESOLVE_RULE_SCOPE_BINDING, walk->file,
                             target       ? "lua_lexical_local"
                             : binder->qn ? "lua_callee_name_is_the_call"
                                          : "lua_local_has_no_graph_node");
        return;
    }
    if (callee) {
        cbm_source_emit_node(walk, node, CBM_SOURCE_SITE_REFERENCE, NULL, CBM_RESOLVE_RULE_NONE,
                             NULL, "lua_callee_name_is_the_call");
        return;
    }
    lua_emit_value(ctx, node, lua_encode_global(ctx, name));
}

/* ── The walk, shared by both phases ───────────────────────────────── */

static void lua_visit_children(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child) && !lua_ignored(child))
                lua_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void lua_visit_scoped(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    lua_binder_t *mark = ctx->top;
    lua_visit_children(ctx, node, depth);
    lua_pop_to(ctx, mark);
}

static void lua_annotate_root(lua_ctx_t *ctx, const char *root, const char *type) {
    const char *qualified = cbm_arena_sprintf(ctx->walk->arena, LUA_ANNOTATED "%s", root);
    const char *value = cbm_arena_sprintf(ctx->walk->arena, LUA_CLASSES "%s", type);
    if (qualified && value)
        lua_declare_root(ctx, qualified, NULL, value);
}

/* `@param client vim.lsp.Client`: the parameter holds an instance of that class. */
static void lua_bind_parameters(lua_ctx_t *ctx, TSNode parameters, const lua_doc_t *doc) {
    lua_nodes_t names = lua_children(ctx, parameters, "name");
    for (uint32_t i = 0; i < names.count; i++) {
        TSNode name = names.items[i];
        lua_binder_t *binder =
            lua_push(ctx, cbm_source_text(ctx->walk, name), ts_node_start_byte(name), 0);
        /* It holds what the caller passed, so `opts = opts or {}` is not its only value. */
        if (binder)
            binder->assigned = true;
        if (!binder || !doc)
            continue;
        for (unsigned p = 0; p < doc->param_count; p++) {
            if (strcmp(doc->param_names[p], binder->name))
                continue;
            lua_annotate_root(ctx, binder->identity, doc->param_types[p]);
            break;
        }
    }
}

/* A function's value facts and its body. spelled is the name its declaration or assignment
 * gives it, which is how extraction named its node; receiver is what `self` holds. */
static void lua_visit_function(lua_ctx_t *ctx, TSNode node, const char *spelled,
                               const char *receiver, uint32_t self_at, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    lua_flow_t flow = {0};
    lua_flow_t *outer = ctx->flow;
    const lua_doc_t *doc = ctx->doc;
    ctx->doc = NULL;
    ctx->flow = &flow;
    lua_binder_t *mark = ctx->top;
    ctx->function_depth++;
    /* `function a:m()` binds self, whatever a is. */
    if (self_at) {
        lua_binder_t *self = lua_push(ctx, "self", self_at, 0);
        if (self && receiver && ctx->collecting) {
            self->assigned = true;
            lua_declare_root(ctx, self->identity, NULL, receiver);
        }
    }
    lua_bind_parameters(ctx, cbm_source_field(node, "parameters"), doc);
    TSNode body = cbm_source_field(node, "body");
    if (!ts_node_is_null(body))
        lua_visit(ctx, body, depth + 1);
    lua_pop_to(ctx, mark);
    ctx->function_depth--;
    ctx->flow = outer;
    if (ctx->collecting) {
        const char *root = lua_identity(ctx, 'F', ts_node_start_byte(node));
        const char *qn =
            spelled ? cbm_source_def_qn_at(ctx->walk, spelled, ts_node_start_point(node).row + 1)
                    : NULL;
        if (root)
            lua_declare_root(ctx, root, qn, flow.open ? NULL : flow.value);
        for (unsigned i = 0; root && i < LUA_RETURN_VALUES; i++) {
            if (flow.later_open[i] || !flow.later[i])
                continue;
            const char *name = cbm_arena_sprintf(ctx->walk->arena, LUA_RETURN_AT "%u", i + 1);
            if (name)
                (void)lua_declare(ctx, root, name, NULL, flow.later[i]);
        }
        if (root && doc && doc->return_type)
            lua_annotate_root(ctx, root, doc->return_type);
    }
}

/* The QN a function assigned under this spelling will have. */
static const char *lua_function_qn(lua_ctx_t *ctx, TSNode value, const char *spelled) {
    return spelled && cbm_source_is(value, "function_definition")
               ? cbm_source_def_qn_at(ctx->walk, spelled, ts_node_start_point(value).row + 1)
               : NULL;
}

/* A write to a local: its first value if it had none, otherwise it holds more than one. */
static void lua_write_local(lua_ctx_t *ctx, lua_binder_t *binder, const char *encoded) {
    if (!binder->assigned) {
        binder->assigned = true;
        if (encoded)
            lua_declare_root(ctx, binder->identity, NULL, encoded);
        return;
    }
    /* `x = setmetatable(x, mt)` keeps x, and `screen = Screen.new()` in each test writes one
     * value many times. */
    if (encoded && !strcmp(encoded, binder->identity))
        return;
    cbm_source_decl_t *fact = (cbm_source_decl_t *)lua_fact(ctx, binder->identity);
    if (fact && !(encoded && fact->type && !strcmp(encoded, fact->type)))
        fact->flags |= CBM_SOURCE_DECL_F_OPEN;
}

/* `x = v` where x is not a local, or `M.x = v`, `M['x'] = v`. */
static void lua_write_target(lua_ctx_t *ctx, TSNode target, TSNode value, const char *encoded) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *spelled = cbm_source_text(walk, target);
    const char *qn = lua_function_qn(ctx, value, spelled);
    if (cbm_source_is(target, "identifier")) {
        lua_binder_t *binder = lua_binder_of(ctx, spelled);
        if (binder) {
            lua_write_local(ctx, binder, encoded);
            return;
        }
        if (spelled && !strcmp(spelled, "_ENV")) {
            (void)lua_declare(ctx, cbm_arena_sprintf(walk->arena, LUA_FLAGS "%s", walk->file),
                              "setfenv", NULL, NULL);
            return;
        }
        if (lua_environment_replaced(ctx) || !spelled)
            return;
        /* `vim = vim or {}` keeps what the name had. */
        const char *self = lua_encode_global(ctx, spelled);
        if (!encoded || !self || strcmp(encoded, self))
            lua_write_member(ctx, LUA_GLOBALS, spelled, qn, encoded);
        return;
    }
    const char *member = lua_index_member(ctx, target);
    const char *table = member ? lua_encode(ctx, cbm_source_field(target, "table"), 0) : NULL;
    if (table)
        lua_write_member(ctx, lua_owner_key(ctx, table, 0), member, qn, encoded);
}

/* `x = x or v` keeps x when it is set, so what the statement adds is v. */
static TSNode lua_unwrap_self_default(lua_ctx_t *ctx, TSNode target, TSNode value) {
    if (!cbm_source_is(value, "binary_expression") || !cbm_source_token(value, "or"))
        return value;
    const char *left = cbm_source_text(ctx->walk, cbm_source_field(value, "left"));
    const char *spelled = cbm_source_text(ctx->walk, target);
    return left && spelled && !strcmp(left, spelled) ? cbm_source_field(value, "right") : value;
}

/* `@class vim.lsp.Client` on `local Client = {}`: the class is this table. A value computed any
 * other way is not what the annotation is about. */
static void lua_note_class(lua_ctx_t *ctx, TSNode value, const char *table, const char *holder) {
    if (!ctx->collecting || !ctx->doc || !ctx->doc->class_name)
        return;
    if (cbm_source_is(value, "table_constructor") ||
        (cbm_source_is(value, "function_call") &&
         lua_global_callee(ctx, cbm_source_field(value, "name"), "setmetatable"))) {
        if (!table)
            return;
        lua_member_fact(ctx, LUA_CLASSES, ctx->doc->class_name, NULL, table);
        const char *owner = lua_owner_key(ctx, table, 0);
        if (owner)
            lua_member_fact(ctx, owner, LUA_CLASS_OF, NULL, ctx->doc->class_name);
        return;
    }
    /* `@class C: A, B` on `M = merge(M, other)`: what M holds now is stated, not computed. */
    if (holder && holder[0] == 'L' && !strchr(holder, LUA_STEP[0]))
        lua_annotate_root(ctx, holder, ctx->doc->class_name);
}

/* `a, b = f()`: the names past the expressions take the last call's later values. */
static const char *lua_encode_later_value(lua_ctx_t *ctx, const lua_nodes_t *values, uint32_t at) {
    if (!values->count || at < values->count)
        return NULL;
    TSNode last = values->items[values->count - 1];
    if (!cbm_source_is(last, "function_call"))
        return NULL;
    const char *callee = lua_encode(ctx, cbm_source_field(last, "name"), 0);
    return callee ? cbm_arena_sprintf(ctx->walk->arena, "%s" LUA_STEP "#%u", callee,
                                      at - values->count + 1)
                  : NULL;
}

static void lua_visit_assignment(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    lua_nodes_t targets = {0};
    TSNode list = (TSNode){0};
    lua_nodes_t parts = lua_children(ctx, node, NULL);
    for (uint32_t i = 0; i < parts.count; i++) {
        if (cbm_source_is(parts.items[i], "variable_list"))
            targets = lua_children(ctx, parts.items[i], "name");
        else if (cbm_source_is(parts.items[i], "expression_list"))
            list = parts.items[i];
    }
    lua_nodes_t values = ts_node_is_null(list) ? (lua_nodes_t){0} : lua_children(ctx, list, NULL);
    if (ctx->collecting) {
        for (uint32_t i = 0; i < targets.count; i++) {
            TSNode value = i < values.count ? values.items[i] : (TSNode){0};
            /* Reading a member of nil raises: a nil selects nothing and replaces nothing. */
            if (cbm_source_is(value, "nil"))
                continue;
            if (!ts_node_is_null(value))
                value = lua_unwrap_self_default(ctx, targets.items[i], value);
            const char *encoded = ts_node_is_null(value) ? lua_encode_later_value(ctx, &values, i)
                                                         : lua_encode(ctx, value, 0);
            /* `---@type uv` on `vim.uv = ...`: the value says nothing, the annotation does. */
            if (!encoded && !i && ctx->doc && ctx->doc->type_name)
                encoded = cbm_arena_sprintf(walk->arena, LUA_CLASSES "%s", ctx->doc->type_name);
            lua_write_target(ctx, targets.items[i], value, encoded);
            if (!i) {
                const char *written = lua_encode(ctx, targets.items[i], 0);
                lua_note_class(ctx, value, written, written);
            }
        }
    }
    /* A write target reads only the table it indexes. */
    for (uint32_t i = 0; i < targets.count; i++) {
        TSNode target = targets.items[i];
        if (lua_is_index(target)) {
            lua_visit(ctx, cbm_source_field(target, "table"), depth + 1);
            if (cbm_source_is(target, "bracket_index_expression"))
                lua_visit(ctx, cbm_source_field(target, "field"), depth + 1);
        }
        if (!ctx->collecting) {
            TSNode name = cbm_source_is(target, "dot_index_expression")
                              ? cbm_source_field(target, "field")
                              : target;
            if (cbm_source_is(name, "identifier"))
                cbm_source_emit_node(walk, name, CBM_SOURCE_SITE_REFERENCE, NULL,
                                     CBM_RESOLVE_RULE_NONE, NULL, "lua_assignment_target_written");
        }
    }
    for (uint32_t i = 0; i < values.count; i++) {
        TSNode value = values.items[i];
        const char *spelled = i < targets.count && cbm_source_is(value, "function_definition")
                                  ? cbm_source_text(walk, targets.items[i])
                                  : NULL;
        lua_doc_t *statement_doc = ctx->doc;
        if (spelled) {
            lua_visit_function(ctx, value, spelled, NULL, 0, depth + 1);
        } else {
            ctx->doc = NULL;
            lua_visit(ctx, value, depth + 1);
        }
        ctx->doc = statement_doc;
    }
}

/* `local old = M.f` above `function M.f() old() end`: the chunk runs in order, so this reads what
 * M.f held before, which is not the declaration further down. Only a table this chunk built can
 * say so; another module's table is complete by the time it is required. */
static const char *lua_chunk_read(lua_ctx_t *ctx, const char *encoded) {
    if (!encoded || ctx->function_depth)
        return encoded;
    const char *step = strrchr(encoded, LUA_STEP[0]);
    if (!step || step[1] != '.')
        return encoded;
    const char *table = cbm_arena_strndup(ctx->walk->arena, encoded, (size_t)(step - encoded));
    const char *owner = table ? lua_owner_key(ctx, table, 0) : NULL;
    if (!owner || owner[0] != 'T' || cbm_source_lookup(ctx->walk, owner, step + 2))
        return encoded;
    return NULL;
}

/* `local ok, m = pcall(require, 'm')`: on success, the second value is the module. */
static const char *lua_protected_require(lua_ctx_t *ctx, TSNode value) {
    if (!cbm_source_is(value, "function_call") ||
        !lua_global_callee(ctx, cbm_source_field(value, "name"), "pcall"))
        return NULL;
    lua_nodes_t args = lua_children(ctx, cbm_source_field(value, "arguments"), NULL);
    return args.count == 2 && lua_global_callee(ctx, args.items[0], "require")
               ? lua_encode_require(ctx, args.items[1])
               : NULL;
}

static void lua_visit_local(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    ctx->module_table = NULL;
    lua_nodes_t parts = lua_children(ctx, node, NULL);
    TSNode names = (TSNode){0}, list = (TSNode){0};
    for (uint32_t i = 0; i < parts.count; i++) {
        if (cbm_source_is(parts.items[i], "variable_list"))
            names = parts.items[i];
        else if (cbm_source_is(parts.items[i], "assignment_statement")) {
            lua_nodes_t inner = lua_children(ctx, parts.items[i], NULL);
            for (uint32_t j = 0; j < inner.count; j++) {
                if (cbm_source_is(inner.items[j], "variable_list"))
                    names = inner.items[j];
                else if (cbm_source_is(inner.items[j], "expression_list"))
                    list = inner.items[j];
            }
        }
    }
    lua_nodes_t binders =
        ts_node_is_null(names) ? (lua_nodes_t){0} : lua_children(ctx, names, "name");
    lua_nodes_t values = ts_node_is_null(list) ? (lua_nodes_t){0} : lua_children(ctx, list, NULL);
    /* The values are read before the names exist: `local x = x` reads the outer x. */
    const char **encoded = binders.count && ctx->collecting
                               ? cbm_arena_calloc(walk->arena, binders.count * sizeof(*encoded))
                               : NULL;
    if (encoded) {
        for (uint32_t i = 0; i < binders.count; i++) {
            if (i < values.count)
                encoded[i] = lua_chunk_read(ctx, lua_encode(ctx, values.items[i], 0));
            else if (i == 1 && values.count == 1)
                encoded[i] = lua_protected_require(ctx, values.items[0]);
            if (!encoded[i] && i >= values.count)
                encoded[i] = lua_chunk_read(ctx, lua_encode_later_value(ctx, &values, i));
        }
    }
    for (uint32_t i = 0; i < values.count; i++) {
        TSNode value = values.items[i];
        const char *spelled = i < binders.count && cbm_source_is(value, "function_definition")
                                  ? cbm_source_text(walk, binders.items[i])
                                  : NULL;
        lua_doc_t *statement_doc = ctx->doc;
        if (spelled) {
            lua_visit_function(ctx, value, spelled, NULL, 0, depth + 1);
        } else {
            ctx->doc = NULL;
            lua_visit(ctx, value, depth + 1);
        }
        ctx->doc = statement_doc;
    }
    for (uint32_t i = 0; i < binders.count; i++) {
        TSNode name = binders.items[i];
        /* Only a chunk-level statement has a Variable node; a function has a node anywhere. */
        TSNode initial = i < values.count ? values.items[i] : (TSNode){0};
        uint32_t line = !ctx->function_depth ? ts_node_start_point(node).row + 1
                        : cbm_source_is(initial, "function_definition")
                            ? ts_node_start_point(initial).row + 1
                            : 0;
        lua_binder_t *binder =
            lua_push(ctx, cbm_source_text(walk, name), ts_node_start_byte(name), line);
        if (!ctx->collecting)
            cbm_source_emit_node(walk, name, CBM_SOURCE_SITE_REFERENCE, NULL, CBM_RESOLVE_RULE_NONE,
                                 NULL, "lua_name_being_declared");
        if (!binder || !ctx->collecting)
            continue;
        TSNode value = i < values.count ? values.items[i] : (TSNode){0};
        binder->assigned =
            (!ts_node_is_null(value) && !cbm_source_is(value, "nil")) || (i > 0 && values.count);
        if (encoded && encoded[i]) {
            binder->assigned = true;
            lua_declare_root(ctx, binder->identity, NULL, encoded[i]);
        }
        if (!i && ctx->doc && ctx->doc->type_name)
            lua_annotate_root(ctx, binder->identity, ctx->doc->type_name);
        if (!i) {
            ctx->last_local = binder;
            /* The class is this statement's table, whatever the local holds later. */
            lua_note_class(ctx, value, encoded && encoded[i] ? encoded[i] : binder->identity,
                           binder->identity);
            if (ctx->module_table)
                lua_member_fact(ctx, binder->identity, LUA_SHAPE, NULL, ctx->module_table);
        }
    }
}

/* Lua 5.5 `global x = v`: the name is the global here, whatever a local says. */
static void lua_visit_global(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    lua_nodes_t parts = lua_children(ctx, node, NULL);
    for (uint32_t i = 0; i < parts.count; i++) {
        if (cbm_source_is(parts.items[i], "assignment_statement")) {
            lua_nodes_t inner = lua_children(ctx, parts.items[i], NULL);
            for (uint32_t j = 0; j < inner.count; j++) {
                if (cbm_source_is(inner.items[j], "variable_list")) {
                    lua_nodes_t names = lua_children(ctx, inner.items[j], "name");
                    for (uint32_t k = 0; k < names.count; k++) {
                        lua_binder_t *binder = lua_push(ctx, cbm_source_text(walk, names.items[k]),
                                                        ts_node_start_byte(names.items[k]), 0);
                        if (binder)
                            binder->global = true;
                    }
                }
            }
            lua_visit_assignment(ctx, parts.items[i], depth + 1);
        } else if (cbm_source_is(parts.items[i], "variable_list")) {
            lua_nodes_t names = lua_children(ctx, parts.items[i], "name");
            for (uint32_t k = 0; k < names.count; k++) {
                lua_binder_t *binder = lua_push(ctx, cbm_source_text(walk, names.items[k]),
                                                ts_node_start_byte(names.items[k]), 0);
                if (binder)
                    binder->global = true;
            }
        }
    }
}

static void lua_visit_function_declaration(lua_ctx_t *ctx, TSNode node, bool local,
                                           unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    TSNode name = cbm_source_field(node, "name");
    const char *spelled = cbm_source_text(walk, name);
    uint32_t line = ts_node_start_point(node).row + 1;
    const char *encoded = lua_identity(ctx, 'F', ts_node_start_byte(node));
    const char *qn = spelled && ctx->collecting ? cbm_source_def_qn_at(walk, spelled, line) : NULL;
    if (local) {
        /* Visible inside its own body. */
        lua_binder_t *binder = lua_push(ctx, spelled, ts_node_start_byte(name), line);
        if (binder)
            binder->assigned = true;
        if (binder && ctx->collecting)
            lua_declare_root(ctx, binder->identity, NULL, encoded);
        lua_visit_function(ctx, node, spelled, NULL, 0, depth);
        return;
    }
    const char *receiver = NULL;
    uint32_t self_at = 0;
    if (lua_is_index(name)) {
        lua_visit(ctx, cbm_source_field(name, "table"), depth + 1);
        if (cbm_source_is(name, "method_index_expression")) {
            receiver = lua_encode(ctx, cbm_source_field(name, "table"), 0);
            self_at = ts_node_start_byte(cbm_source_field(name, "method"));
        }
    }
    if (ctx->collecting) {
        if (cbm_source_is(name, "identifier")) {
            lua_binder_t *binder = lua_binder_of(ctx, spelled);
            if (binder)
                lua_write_local(ctx, binder, encoded);
            else if (spelled && !lua_environment_replaced(ctx))
                lua_write_member(ctx, LUA_GLOBALS, spelled, qn, encoded);
        } else if (lua_is_index(name)) {
            const char *member = lua_index_member(ctx, name);
            const char *table = member ? lua_encode(ctx, cbm_source_field(name, "table"), 0) : NULL;
            if (table)
                lua_write_member(ctx, lua_owner_key(ctx, table, 0), member, qn, encoded);
        }
    }
    lua_visit_function(ctx, node, spelled, receiver, self_at, depth);
}

/* Finds `require('prefix' .. key)` in a function, where key is its second parameter. */
static const char *lua_required_prefix(lua_ctx_t *ctx, TSNode node, const char *key,
                                       unsigned depth) {
    if (ts_node_is_null(node) || !cbm_source_step(ctx->walk, depth))
        return NULL;
    if (cbm_source_is(node, "function_call") &&
        lua_global_callee(ctx, cbm_source_field(node, "name"), "require")) {
        lua_nodes_t args = lua_children(ctx, cbm_source_field(node, "arguments"), NULL);
        if (args.count == 1 && cbm_source_is(args.items[0], "binary_expression")) {
            const char *prefix = lua_literal(ctx, cbm_source_field(args.items[0], "left"));
            const char *right =
                cbm_source_text(ctx->walk, cbm_source_field(args.items[0], "right"));
            if (prefix && right && !strcmp(right, key))
                return prefix;
        }
    }
    const char *found = NULL;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                found = lua_required_prefix(ctx, child, key, depth + 1);
        } while (!found && ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return found;
}

/* A metatable literal whose `__index` loads a module named after the key. */
static void lua_note_lazy_index(lua_ctx_t *ctx, const char *owner, TSNode metatable) {
    if (!cbm_source_is(metatable, "table_constructor"))
        return;
    lua_nodes_t fields = lua_children(ctx, metatable, NULL);
    for (uint32_t i = 0; i < fields.count; i++) {
        TSNode value = cbm_source_field(fields.items[i], "value");
        const char *name = cbm_source_text(ctx->walk, cbm_source_field(fields.items[i], "name"));
        if (!name || strcmp(name, "__index") || !cbm_source_is(value, "function_definition"))
            continue;
        lua_nodes_t parameters = lua_children(ctx, cbm_source_field(value, "parameters"), "name");
        const char *key =
            parameters.count == 2 ? cbm_source_text(ctx->walk, parameters.items[1]) : NULL;
        const char *prefix =
            key ? lua_required_prefix(ctx, cbm_source_field(value, "body"), key, 0) : NULL;
        if (prefix)
            lua_write_member(ctx, owner, LUA_LAZY, NULL, prefix);
    }
}

/* What a call does to the facts, whatever its value is used for. */
static void lua_call_effects(lua_ctx_t *ctx, TSNode callee, TSNode arguments) {
    cbm_source_walk_t *walk = ctx->walk;
    if (cbm_source_is(callee, "dot_index_expression")) {
        const char *member = lua_index_member(ctx, callee);
        if (member &&
            (!strcmp(member, "setlocal") || !strcmp(member, "setupvalue") ||
             !strcmp(member, "upvaluejoin")) &&
            lua_global_callee(ctx, cbm_source_field(callee, "table"), "debug")) {
            const char *flags = cbm_arena_sprintf(walk->arena, LUA_FLAGS "%s", walk->file);
            if (flags && !cbm_source_lookup(walk, flags, "debug"))
                (void)lua_declare(ctx, flags, "debug", NULL, NULL);
        }
        return;
    }
    if (lua_global_callee(ctx, callee, "setfenv")) {
        const char *flags = cbm_arena_sprintf(walk->arena, LUA_FLAGS "%s", walk->file);
        if (flags && !cbm_source_lookup(walk, flags, "setfenv"))
            (void)lua_declare(ctx, flags, "setfenv", NULL, NULL);
        return;
    }
    bool metatable = lua_global_callee(ctx, callee, "setmetatable");
    bool rawset = !metatable && lua_global_callee(ctx, callee, "rawset");
    if (!metatable && !rawset)
        return;
    lua_nodes_t args = lua_children(ctx, arguments, NULL);
    if (args.count < (rawset ? 3u : 2u))
        return;
    const char *table = lua_encode(ctx, args.items[0], 0);
    const char *owner = table ? lua_owner_key(ctx, table, 0) : NULL;
    if (!owner)
        return;
    if (metatable) {
        /* `setmetatable(o, self)` follows the receiver of each call, which one declaration
         * does not know: the method is called on subclasses too. */
        const char *spelled = cbm_source_is(args.items[1], "identifier")
                                  ? cbm_source_text(walk, args.items[1])
                                  : NULL;
        if (!spelled || strcmp(spelled, "self"))
            lua_write_member(ctx, owner, LUA_META, NULL, lua_encode(ctx, args.items[1], 0));
        lua_note_lazy_index(ctx, owner, args.items[1]);
        return;
    }
    const char *member = lua_literal(ctx, args.items[1]);
    if (member)
        lua_write_member(ctx, owner, member, NULL, lua_encode(ctx, args.items[2], 0));
}

static void lua_visit_call(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    TSNode callee = cbm_source_field(node, "name");
    TSNode arguments = cbm_source_field(node, "arguments");
    /* An annotation over a call statement is not about a callback passed to it. */
    ctx->doc = NULL;
    if (ctx->collecting)
        lua_call_effects(ctx, callee, arguments);
    else
        lua_emit_call(ctx, node, callee);
    if (cbm_source_is(callee, "identifier")) {
        if (!ctx->collecting)
            lua_emit_identifier(ctx, callee, true);
    } else if (cbm_source_is(callee, "dot_index_expression") ||
               cbm_source_is(callee, "method_index_expression")) {
        lua_visit(ctx, cbm_source_field(callee, "table"), depth + 1);
        if (!ctx->collecting) {
            TSNode member = cbm_source_field(
                callee, cbm_source_is(callee, "method_index_expression") ? "method" : "field");
            cbm_source_emit_node(ctx->walk, member, CBM_SOURCE_SITE_REFERENCE, NULL,
                                 CBM_RESOLVE_RULE_NONE, NULL, "lua_callee_name_is_the_call");
        }
    } else {
        lua_visit(ctx, callee, depth + 1);
    }
    if (!ts_node_is_null(arguments))
        lua_visit_children(ctx, arguments, depth);
}

/* `buf = ..., --- @module 'vim.lsp.buf'`: the field holds that module. */
static const char *lua_doc_module(lua_ctx_t *ctx, TSNode comment) {
    const char *text = cbm_source_text(ctx->walk, comment);
    const char *tag = text ? strstr(text, "@module") : NULL;
    const char *open = tag ? strpbrk(tag, "'\"") : NULL;
    const char *close = open ? strchr(open + 1, open[0]) : NULL;
    if (!close)
        return NULL;
    const char *module = cbm_arena_strndup(ctx->walk->arena, open + 1, (size_t)(close - open - 1));
    return module ? cbm_arena_sprintf(ctx->walk->arena, LUA_MODULES "%s" LUA_PART "%s",
                                      ctx->directory, module)
                  : NULL;
}

static void lua_visit_table(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *owner = ctx->collecting ? lua_identity(ctx, 'T', ts_node_start_byte(node)) : NULL;
    const char *pending = NULL;
    uint32_t pending_row = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode field = ts_tree_cursor_current_node(&cursor);
            if (cbm_source_is(field, "comment")) {
                const char *module = pending && ts_node_start_point(field).row == pending_row
                                         ? lua_doc_module(ctx, field)
                                         : NULL;
                if (module) {
                    lua_member_fact(ctx, owner, pending, NULL, module);
                    ctx->module_table = owner;
                }
                pending = NULL;
                continue;
            }
            if (!cbm_source_is(field, "field"))
                continue;
            pending = NULL;
            TSNode name = cbm_source_field(field, "name");
            TSNode value = cbm_source_field(field, "value");
            bool computed =
                !ts_node_is_null(name) && walk->source[ts_node_start_byte(field)] == '[';
            const char *member = NULL;
            if (!ts_node_is_null(name))
                member = computed ? lua_literal(ctx, name) : cbm_source_text(walk, name);
            if (ctx->collecting && member && owner && !ts_node_is_null(value)) {
                /* `name = ...` says nothing itself: the comment after it may. */
                if (cbm_source_is(value, "vararg_expression")) {
                    pending = member;
                    pending_row = ts_node_end_point(field).row;
                } else if (!cbm_source_is(value, "number") && !cbm_source_is(value, "string") &&
                           !cbm_source_is(value, "true") && !cbm_source_is(value, "false")) {
                    /* A literal selects no declaration, and a data table has many. */
                    lua_member_fact(ctx, owner, member, NULL, lua_encode(ctx, value, 0));
                }
            }
            if (computed)
                lua_visit(ctx, name, depth + 1);
            if (!ts_node_is_null(value))
                lua_visit(ctx, value, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* What a `return` gives past its first value. Only a last call or `...` can give more than it
 * spells, and a missing or nil one is the failure path, as it is for the first. */
static void lua_flow_later_values(lua_ctx_t *ctx, const lua_nodes_t *values) {
    lua_flow_t *flow = ctx->flow;
    for (uint32_t i = 1; i < values->count && i <= LUA_RETURN_VALUES; i++) {
        unsigned slot = i - 1;
        if (cbm_source_is(values->items[i], "nil"))
            continue;
        const char *encoded = lua_encode(ctx, values->items[i], 0);
        if (!encoded || (flow->later[slot] && strcmp(flow->later[slot], encoded)))
            flow->later_open[slot] = true;
        else
            flow->later[slot] = encoded;
    }
    TSNode last = values->items[values->count - 1];
    if (cbm_source_is(last, "function_call") || cbm_source_is(last, "vararg_expression"))
        for (uint32_t i = values->count; i <= LUA_RETURN_VALUES; i++)
            flow->later_open[i - 1] = true;
}

static void lua_visit_return(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    lua_nodes_t parts = lua_children(ctx, node, NULL);
    for (uint32_t i = 0; i < parts.count; i++) {
        if (!cbm_source_is(parts.items[i], "expression_list"))
            continue;
        lua_nodes_t values = lua_children(ctx, parts.items[i], NULL);
        if (ctx->collecting && ctx->flow && values.count) {
            /* `return nil, err` is the failure path: a member of nil raises. */
            if (!cbm_source_is(values.items[0], "nil")) {
                const char *encoded = lua_encode(ctx, values.items[0], 0);
                if (!encoded || (ctx->flow->value && strcmp(ctx->flow->value, encoded)))
                    ctx->flow->open = true;
                else
                    ctx->flow->value = encoded;
            }
            lua_flow_later_values(ctx, &values);
        }
        lua_visit_children(ctx, parts.items[i], depth);
    }
}

static void lua_visit_for(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    lua_binder_t *mark = ctx->top;
    TSNode clause = cbm_source_field(node, "clause");
    /* The clause's expressions are read outside the scope of the names it binds. */
    lua_nodes_t parts =
        ts_node_is_null(clause) ? (lua_nodes_t){0} : lua_children(ctx, clause, NULL);
    for (uint32_t i = 0; i < parts.count; i++) {
        TSNode part = parts.items[i];
        if (cbm_source_is(part, "variable_list"))
            continue;
        if (cbm_source_is(part, "identifier") && cbm_source_is(clause, "for_numeric_clause") &&
            ts_node_eq(part, cbm_source_field(clause, "name")))
            continue;
        lua_visit(ctx, part, depth + 1);
    }
    for (uint32_t i = 0; i < parts.count; i++) {
        TSNode part = parts.items[i];
        if (cbm_source_is(part, "variable_list")) {
            lua_nodes_t names = lua_children(ctx, part, "name");
            for (uint32_t j = 0; j < names.count; j++)
                (void)lua_push(ctx, cbm_source_text(ctx->walk, names.items[j]),
                               ts_node_start_byte(names.items[j]), 0);
        } else if (cbm_source_is(part, "identifier") &&
                   cbm_source_is(clause, "for_numeric_clause") &&
                   ts_node_eq(part, cbm_source_field(clause, "name"))) {
            (void)lua_push(ctx, cbm_source_text(ctx->walk, part), ts_node_start_byte(part), 0);
        }
    }
    TSNode body = cbm_source_field(node, "body");
    if (!ts_node_is_null(body))
        lua_visit(ctx, body, depth + 1);
    lua_pop_to(ctx, mark);
}

/* The `until` condition sees the locals of the body. */
static void lua_visit_repeat(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    lua_binder_t *mark = ctx->top;
    TSNode body = cbm_source_field(node, "body");
    TSNode condition = cbm_source_field(node, "condition");
    if (!ts_node_is_null(body))
        lua_visit_children(ctx, body, depth + 1);
    if (!ts_node_is_null(condition))
        lua_visit(ctx, condition, depth + 1);
    lua_pop_to(ctx, mark);
}

/* One whitespace-free word of an annotation, advancing past it. */
static const char *lua_doc_word(lua_ctx_t *ctx, const char **cursor) {
    const char *at = *cursor;
    while (*at == ' ' || *at == '\t')
        at++;
    const char *end = at;
    while (*end && *end != ' ' && *end != '\t' && *end != '\r' && *end != '\n')
        end++;
    *cursor = end;
    return end > at ? cbm_arena_strndup(ctx->walk->arena, at, (size_t)(end - at)) : NULL;
}

/* A type that names one class: `vim.lsp.Client`, `vim.lsp.Client?`. */
static const char *lua_doc_type(lua_ctx_t *ctx, const char *word) {
    if (!word)
        return NULL;
    size_t length = strlen(word);
    if (length && word[length - 1] == '?')
        length--;
    if (!length ||
        strspn(word, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.") != length)
        return NULL;
    return cbm_arena_strndup(ctx->walk->arena, word, length);
}

/* `a.b`, `a.b<T, U>`: the class named. Type arguments do not change which members it has. */
static const char *lua_doc_class_word(lua_ctx_t *ctx, const char **cursor) {
    const char *at = *cursor + strspn(*cursor, " \t");
    size_t length = strspn(at, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.");
    const char *end = at + length;
    if (*end == '<') {
        unsigned open = 0;
        do {
            if (*end == '<')
                open++;
            else if (*end == '>')
                open--;
            end++;
        } while (*end && open);
        if (open) {
            *cursor = end;
            return NULL;
        }
    }
    *cursor = end;
    return length ? cbm_arena_strndup(ctx->walk->arena, at, length) : NULL;
}

/* One entry per parent. An entry with no type is a parent this cannot read. */
static void lua_doc_parents(lua_ctx_t *ctx, const char *class_name, const char *at) {
    for (;;) {
        const char *parent = lua_doc_class_word(ctx, &at);
        char next = *at;
        bool ends = !next || next == ' ' || next == '\t' || next == '\r' || next == ',';
        (void)lua_declare(ctx, LUA_PARENTS, class_name, NULL, parent && ends ? parent : NULL);
        if (!parent || !ends)
            return;
        at += strspn(at, " \t");
        if (*at != ',')
            return;
        at++;
    }
}

static void lua_doc_read(lua_ctx_t *ctx, lua_doc_t *doc, TSNode comment) {
    const char *text = cbm_source_text(ctx->walk, comment);
    if (!text || strncmp(text, "---", 3))
        return;
    const char *at = text + 3;
    while (*at == '-' || *at == ' ' || *at == '\t')
        at++;
    uint32_t row = ts_node_end_point(comment).row;
    if (doc->present && doc->last_row + 1 != row)
        *doc = (lua_doc_t){0};
    doc->present = true;
    doc->last_row = row;
    if (*at != '@')
        return;
    const char *tag = lua_doc_word(ctx, &at);
    if (!tag)
        return;
    if (!strcmp(tag, "@class")) {
        at += strspn(at, " \t");
        if (*at == '(') {
            at = strchr(at, ')');
            if (!at)
                return;
            at++;
        }
        doc->class_name = lua_doc_class_word(ctx, &at);
        at += strspn(at, " \t");
        if (doc->class_name && *at == ':')
            lua_doc_parents(ctx, doc->class_name, at + 1);
    } else if (!strcmp(tag, "@type")) {
        doc->type_name = lua_doc_type(ctx, lua_doc_word(ctx, &at));
    } else if (!strcmp(tag, "@return")) {
        if (!doc->return_type)
            doc->return_type = lua_doc_type(ctx, lua_doc_word(ctx, &at));
    } else if (!strcmp(tag, "@param") && doc->param_count < LUA_DOC_PARAMS) {
        const char *name = lua_doc_word(ctx, &at);
        const char *type = lua_doc_type(ctx, lua_doc_word(ctx, &at));
        size_t length = name ? strlen(name) : 0;
        if (length && name[length - 1] == '?')
            name = cbm_arena_strndup(ctx->walk->arena, name, length - 1);
        if (name && type) {
            doc->param_names[doc->param_count] = name;
            doc->param_types[doc->param_count++] = type;
        }
    }
}

/* `local x = y --- @type T`: a type for the local this line just declared. */
static void lua_doc_trailing(lua_ctx_t *ctx, TSNode comment, uint32_t statement_row) {
    if (!ctx->last_local || ts_node_start_point(comment).row != statement_row)
        return;
    lua_doc_t trailing = {0};
    lua_doc_read(ctx, &trailing, comment);
    if (!trailing.type_name)
        return;
    lua_annotate_root(ctx, ctx->last_local->identity, trailing.type_name);
}

static void lua_visit_statements(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    /* `local` and `global` are field names on the statement, not kinds. */
    lua_doc_t doc = {0};
    lua_doc_t *outer = ctx->doc;
    uint32_t last_row = UINT32_MAX;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            if (cbm_source_is(child, "comment")) {
                if (ctx->collecting) {
                    lua_doc_trailing(ctx, child, last_row);
                    if (ts_node_start_point(child).row != last_row)
                        lua_doc_read(ctx, &doc, child);
                }
                continue;
            }
            if (lua_ignored(child))
                continue;
            /* An annotation is about the line right below it. */
            bool adjacent = doc.present && doc.last_row + 1 == ts_node_start_point(child).row;
            ctx->doc = ctx->collecting && adjacent ? &doc : NULL;
            ctx->last_local = NULL;
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            bool local = field && !strcmp(field, "local_declaration");
            bool global = field && !strcmp(field, "global_declaration");
            if (cbm_source_is(child, "function_declaration")) {
                if (cbm_source_step(ctx->walk, depth + 1))
                    lua_visit_function_declaration(ctx, child, local, depth + 1);
            } else if (cbm_source_is(child, "variable_declaration")) {
                if (cbm_source_step(ctx->walk, depth + 1)) {
                    if (global)
                        lua_visit_global(ctx, child, depth + 1);
                    else
                        lua_visit_local(ctx, child, depth + 1);
                }
            } else {
                lua_visit(ctx, child, depth + 1);
            }
            doc = (lua_doc_t){0};
            last_row = ts_node_end_point(child).row;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    ctx->doc = outer;
}

static void lua_visit(lua_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        if (!ctx->collecting)
            lua_emit_identifier(ctx, node, false);
        return;
    }
    if (cbm_source_is(node, "dot_index_expression")) {
        lua_visit(ctx, cbm_source_field(node, "table"), depth + 1);
        if (!ctx->collecting)
            lua_emit_value(ctx, cbm_source_field(node, "field"), lua_encode(ctx, node, 0));
        return;
    }
    if (cbm_source_is(node, "method_index_expression")) {
        lua_visit(ctx, cbm_source_field(node, "table"), depth + 1);
        return;
    }
    if (cbm_source_is(node, "bracket_index_expression")) {
        lua_visit(ctx, cbm_source_field(node, "table"), depth + 1);
        lua_visit(ctx, cbm_source_field(node, "field"), depth + 1);
        return;
    }
    if (cbm_source_is(node, "function_call")) {
        lua_visit_call(ctx, node, depth);
        return;
    }
    if (cbm_source_is(node, "block")) {
        lua_binder_t *mark = ctx->top;
        lua_visit_statements(ctx, node, depth);
        lua_pop_to(ctx, mark);
        return;
    }
    if (cbm_source_is(node, "function_definition")) {
        lua_visit_function(ctx, node, NULL, NULL, 0, depth);
        return;
    }
    if (cbm_source_is(node, "table_constructor")) {
        lua_visit_table(ctx, node, depth);
        return;
    }
    if (cbm_source_is(node, "assignment_statement")) {
        lua_visit_assignment(ctx, node, depth);
        return;
    }
    if (cbm_source_is(node, "return_statement")) {
        lua_visit_return(ctx, node, depth);
        return;
    }
    if (cbm_source_is(node, "for_statement")) {
        lua_visit_for(ctx, node, depth);
        return;
    }
    if (cbm_source_is(node, "repeat_statement")) {
        lua_visit_repeat(ctx, node, depth);
        return;
    }
    if (cbm_source_is(node, "goto_statement") || cbm_source_is(node, "label_statement") ||
        cbm_source_is(node, "attribute") || cbm_source_is(node, "string") ||
        cbm_source_is(node, "number"))
        return;
    lua_visit_scoped(ctx, node, depth);
}

/* ── Module names ──────────────────────────────────────────────────── */

/* Where a Lua project keeps what package.path points at: Neovim's and LuaRocks' `lua/`, and
 * LuaRocks' `src/` and `lib/` layouts. */
static bool lua_source_root_segment(const char *at, size_t size) {
    return (size == 3 &&
            (!strncmp(at, "lua", 3) || !strncmp(at, "src", 3) || !strncmp(at, "lib", 3)));
}

const char *cbm_lua_root_module(const char *rel_path, const char *from, size_t *length) {
    size_t size = strlen(rel_path);
    if (size < strlen(".lua") || strcmp(rel_path + size - strlen(".lua"), ".lua"))
        return NULL;
    const char *end = rel_path + size - strlen(".lua");
    if (size >= strlen("init.lua") && !strcmp(rel_path + size - strlen("init.lua"), "init.lua") &&
        (size == strlen("init.lua") || rel_path[size - strlen("init.lua") - 1] == '/'))
        end = size == strlen("init.lua") ? rel_path : rel_path + size - strlen("/init.lua");
    /* from is a name already returned: the next root starts after it. */
    const char *at = from ? from : rel_path;
    while (at < end) {
        const char *slash = strchr(at, '/');
        if (!slash || slash >= end)
            return NULL;
        if (lua_source_root_segment(at, (size_t)(slash - at))) {
            *length = (size_t)(end - (slash + 1));
            return slash + 1;
        }
        at = slash + 1;
    }
    return NULL;
}

/* A file a project's source root names. Those under the repository root or an ancestor of the
 * requiring file need no entry: they are probed by path. */
static void lua_register_module_names(lua_ctx_t *ctx) {
    cbm_source_walk_t *walk = ctx->walk;
    size_t length = 0;
    for (const char *name = cbm_lua_root_module(walk->file, NULL, &length); name;
         name = cbm_lua_root_module(walk->file, name, &length)) {
        const char *module = cbm_arena_strndup(walk->arena, name, length);
        if (module)
            (void)cbm_source_declare(walk, LUA_MODULES, module, NULL, 0, 0);
    }
}

static const char *lua_directory_of(cbm_source_walk_t *walk) {
    const char *slash = strrchr(walk->file, '/');
    return slash ? cbm_arena_strndup(walk->arena, walk->file, (size_t)(slash - walk->file)) : "";
}

static bool lua_ctx_init(lua_ctx_t *ctx, cbm_source_walk_t *walk, bool collecting) {
    *ctx = (lua_ctx_t){.walk = walk, .collecting = collecting};
    ctx->visible = cbm_ht_create(64);
    ctx->memo = collecting ? NULL : cbm_ht_create(64);
    ctx->directory = lua_directory_of(walk);
    if (!ctx->visible || (!collecting && !ctx->memo) || !ctx->directory) {
        cbm_source_fail(walk, "lua_scope_allocation");
        return false;
    }
    return true;
}

static void lua_ctx_free(lua_ctx_t *ctx) {
    cbm_ht_free(ctx->visible);
    cbm_ht_free(ctx->memo);
}

static void lua_collect(cbm_source_walk_t *walk) {
    /* Facts of the files before this one, and this file's so far, for a repeated write. */
    walk->table = walk->writable;
    lua_ctx_t ctx;
    if (!lua_ctx_init(&ctx, walk, true))
        return;
    lua_flow_t flow = {0};
    ctx.flow = &flow;
    lua_register_module_names(&ctx);
    lua_visit_statements(&ctx, walk->root, 0);
    const char *key = cbm_arena_sprintf(walk->arena, LUA_RETURNS "%s", walk->file);
    if (key)
        (void)lua_declare(&ctx, key, "", NULL, flow.open ? NULL : flow.value);
    lua_ctx_free(&ctx);
}

static void lua_resolve(cbm_source_walk_t *walk) {
    lua_ctx_t ctx;
    if (!lua_ctx_init(&ctx, walk, false))
        return;
    const char *flags = cbm_arena_sprintf(walk->arena, LUA_FLAGS "%s", walk->file);
    ctx.environment_open = flags && cbm_source_lookup(walk, flags, "setfenv");
    lua_visit_statements(&ctx, walk->root, 0);
    lua_ctx_free(&ctx);
}

const cbm_source_adapter_t cbm_source_adapter_lua = {.language = CBM_LANG_LUA,
                                                     .collect = lua_collect,
                                                     .resolve = lua_resolve,
                                                     .decides_references = true,
                                                     .one_name_one_node = true};
