/* Ruby binding rules. Methods are dynamic, so these rules are narrower than in
 * the other adapters, and each states why it still selects a declaration.
 *
 * A call with parentheses or arguments, `foo()` or `foo 1`, is a method call by
 * the grammar: Ruby only reads an identifier as a local when it has no arguments
 * and an assignment precedes it. That is why a same-file `target()` next to its
 * `def` is decidable. A top-level `def` defines a private method of Object, and
 * Object is on every class's ancestor chain.
 *
 * With an implicit or `self` receiver, lookup starts at the class of self. The
 * declaration static rules can select is the first on the lexical class's own
 * chain: prepended modules, the class, included modules in reverse order, the
 * superclass, finally Object. A subclass overriding it is runtime dispatch, which
 * CALLS does not model. In a `def self.x` or `class << self` body, or directly in
 * a class body, self is the class, so singleton methods are searched instead,
 * then extended modules, then the superclass's. `self.class.m` from an instance
 * searches that same chain, and `module_function` copies a module's instance
 * methods onto it. `Const.m` searches Const's singleton chain.
 *
 * `Const.new` allocates and runs `initialize`, unless the project's part of the
 * singleton chain declares its own `new`. It selects that `initialize`, or the
 * class itself when the project's part of the chain has none. An ancestor outside
 * the project may override `new`, but an override still constructs this class, so
 * leaving the project does not change what is selected.
 *
 * `a.b = c` is a call of `b=`, never of the reader `b`, and `a.b ||= c` calls both.
 *
 * Reopening is the language's normal mode. So all files declaring a method of the
 * same class count. Within one file Ruby runs top to bottom, so the last declaration
 * stands; across files the load order decides, so two declarations are no single
 * target, and neither is one that may not run. `define_method`, `attr_*`,
 * `undef_method` and `remove_method` with a literal name declare that name with no
 * node, which abstains if it is the one that stands. `alias` and `alias_method` name
 * the earlier method of the same file, and otherwise abstain.
 *
 * A block keeps the self it was written under unless the method it is handed to
 * rebinds that. So it keeps self only for a method known not to: a project method
 * this rule selected, or a core iterator, and in both cases only if no project
 * `def` of that name hands its block on to anything but a core iterator, or keeps
 * it. Everything else is the shape of a DSL and abstains. A lambda in a class body
 * or at the top of a file is handed to something the same way, and abstains too.
 * A `define_method` block is the body of an instance method of the lexical class.
 *
 * A constant resolves in each lexically enclosing scope's own table, innermost
 * first, then the ancestors of the innermost one, then the top level. Blocks never
 * change this, so a mention resolves under an unknown self as well. A mention of a
 * constant declared once is a USAGE of it; a reopened namespace is no single
 * declaration and abstains. A file-level local is the one local the graph has a node
 * for, so it alone can be read or written; every other bare identifier is a local
 * with no node or a call of self. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include "syntax_index.h"

enum { RUBY_MAX_NEST = 32 };

/* Declaration facts of this adapter, above the shared flags. */
enum {
    RUBY_DECL_ALIAS = 1u << 20,           /* `type` names the method this is another name of */
    RUBY_DECL_MODULE_FUNCTION = 1u << 21, /* `type` names the instance method this copies */
    RUBY_DECL_CONSTANT_ALIAS = 1u << 22   /* `type` is "cref|text" of the constant it holds */
};

#define RUBY_OBJECT "Object"
/* Name sets sharing the table: no constant path can spell these owners. One holds a
 * name for each `def` that can rebind its caller's block, with the method it forwards
 * that block to in `type` when forwarding is why. The same notes sit under the def's own
 * class, RUBY_REBINDING_OF + owner, for a call that selected that very def. The last set
 * holds every `def` name. */
#define RUBY_REBINDING_DEFS \
    "\x01"                  \
    "rebinding"
#define RUBY_REBINDING_OF \
    "\x01"                \
    "of:"
#define RUBY_ALL_DEFS \
    "\x01"            \
    "defs"

typedef enum {
    RUBY_SELF_UNKNOWN,
    RUBY_SELF_MAIN,     /* top of a file */
    RUBY_SELF_TOP_DEF,  /* body of a top-level def */
    RUBY_SELF_INSTANCE, /* body of a def in a class */
    RUBY_SELF_CLASS     /* class body, def self.x, a def in class << self */
} ruby_self_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *nest[RUBY_MAX_NEST];
    int depth;
    bool singleton;       /* inside class << self */
    bool module_function; /* after a bare module_function in this module body */
} ruby_ctx_t;

static const char *ruby_current(const ruby_ctx_t *ctx) {
    return ctx->depth ? ctx->nest[ctx->depth - 1] : "";
}

static bool ruby_push(ruby_ctx_t *ctx, const char *owner) {
    if (ctx->depth >= RUBY_MAX_NEST)
        return false;
    ctx->nest[ctx->depth++] = owner;
    return true;
}

/* `class A::B` spells its own path; written inside a nest it stays below it. */
static const char *ruby_declared_owner(ruby_ctx_t *ctx, TSNode name) {
    const char *text = cbm_source_text(ctx->walk, name);
    if (!text)
        return NULL;
    if (text[0] == ':' && text[1] == ':')
        return text + 2;
    return ctx->depth ? cbm_arena_sprintf(ctx->walk->arena, "%s::%s", ruby_current(ctx), text)
                      : text;
}

/* The lexically enclosing scopes as stored with a deferred constant, innermost last. */
static const char *ruby_cref(ruby_ctx_t *ctx) {
    const char *joined = "";
    for (int i = 0; joined && i < ctx->depth; i++)
        joined =
            i ? cbm_arena_sprintf(ctx->walk->arena, "%s;%s", joined, ctx->nest[i]) : ctx->nest[i];
    return joined;
}

static const char *ruby_literal_symbol(cbm_source_walk_t *walk, TSNode node) {
    if (cbm_source_is(node, "simple_symbol")) {
        const char *text = cbm_source_text(walk, node);
        return text && text[0] == ':' ? text + 1 : NULL;
    }
    if (cbm_source_is(node, "string") && ts_node_named_child_count(node) == 1 &&
        cbm_source_is(ts_node_named_child(node, 0), "string_content"))
        return cbm_source_text(walk, ts_node_named_child(node, 0));
    return NULL;
}

static const char *ruby_call_name(cbm_source_walk_t *walk, TSNode call) {
    if (!ts_node_is_null(cbm_source_field(call, "receiver")))
        return NULL;
    TSNode method = cbm_source_field(call, "method");
    return cbm_source_is(method, "identifier") ? cbm_source_text(walk, method) : NULL;
}

static bool ruby_name_in(const char *name, const char *const *names) {
    for (size_t i = 0; name && names[i]; i++)
        if (!strcmp(name, names[i]))
            return true;
    return false;
}

static bool ruby_rewrites_method(const char *name) {
    static const char *const names[] = {"define_method", "undef_method", "remove_method", NULL};
    return ruby_name_in(name, names);
}

static bool ruby_attribute_macro(const char *name) {
    static const char *const names[] = {"attr_reader", "attr_writer", "attr_accessor", "attr",
                                        NULL};
    return ruby_name_in(name, names);
}

/* Evaluates its block under another self. */
static bool ruby_rebinds_self(const char *name) {
    static const char *const names[] = {
        "instance_eval", "instance_exec", "class_eval",    "class_exec",
        "module_eval",   "module_exec",   "define_method", "define_singleton_method",
        "new",           "refine",        "included",      "extended",
        "prepended",     "inherited",     "class_methods", NULL};
    return ruby_name_in(name, names);
}

/* Ruby core's block-taking methods: Enumerable, Array, Hash, String, Integer, Kernel,
 * Comparable, IO, File, Dir, Mutex and Monitor. Each yields to its block and none
 * rebinds self. A project `def` of one of these names is audited separately. */
static bool ruby_core_block_method(const char *name) {
    static const char *const names[] = {"all?",
                                        "any?",
                                        "at_exit",
                                        "catch",
                                        "chdir",
                                        "chunk",
                                        "chunk_while",
                                        "collect",
                                        "collect!",
                                        "collect_concat",
                                        "count",
                                        "cycle",
                                        "delete_if",
                                        "detect",
                                        "downto",
                                        "drop_while",
                                        "each",
                                        "each_byte",
                                        "each_char",
                                        "each_cons",
                                        "each_entry",
                                        "each_index",
                                        "each_key",
                                        "each_line",
                                        "each_pair",
                                        "each_slice",
                                        "each_value",
                                        "each_with_index",
                                        "each_with_object",
                                        "fetch",
                                        "filter",
                                        "filter!",
                                        "filter_map",
                                        "find",
                                        "find_all",
                                        "find_index",
                                        "flat_map",
                                        "fork",
                                        "foreach",
                                        "group_by",
                                        "gsub",
                                        "gsub!",
                                        "index",
                                        "inject",
                                        "keep_if",
                                        "lambda",
                                        "loop",
                                        "map",
                                        "map!",
                                        "max",
                                        "max_by",
                                        "merge",
                                        "merge!",
                                        "min",
                                        "min_by",
                                        "minmax",
                                        "minmax_by",
                                        "mktmpdir",
                                        "mon_synchronize",
                                        "none?",
                                        "one?",
                                        "open",
                                        "partition",
                                        "popen",
                                        "popen2",
                                        "popen2e",
                                        "popen3",
                                        "proc",
                                        "reduce",
                                        "reject",
                                        "reject!",
                                        "reverse_each",
                                        "rindex",
                                        "scan",
                                        "select",
                                        "select!",
                                        "slice_after",
                                        "slice_before",
                                        "slice_when",
                                        "sort",
                                        "sort!",
                                        "sort_by",
                                        "sort_by!",
                                        "step",
                                        "sub",
                                        "sub!",
                                        "sum",
                                        "synchronize",
                                        "take_while",
                                        "tally_by",
                                        "tap",
                                        "then",
                                        "times",
                                        "to_h",
                                        "transform_keys",
                                        "transform_keys!",
                                        "transform_values",
                                        "transform_values!",
                                        "trap",
                                        "uniq",
                                        "uniq!",
                                        "update",
                                        "upto",
                                        "with_index",
                                        "with_object",
                                        "yield_self",
                                        "zip",
                                        NULL};
    return ruby_name_in(name, names);
}

/* `Thread.new { }` and these run their block as written; `Class.new { }` does not. */
static bool ruby_core_class_keeps_block_self(const char *name) {
    static const char *const names[] = {"Array", "Enumerator", "Fiber",  "Hash",
                                        "Proc",  "Set",        "Thread", NULL};
    return ruby_name_in(name, names);
}

/* ── Phase one ─────────────────────────────────────────────────────── */

static void ruby_declare_key(ruby_ctx_t *ctx, const char *name, uint32_t flags, const char *qn,
                             uint32_t line) {
    const char *owner = ctx->depth ? ruby_current(ctx) : RUBY_OBJECT;
    const char *key = ctx->singleton ? cbm_arena_sprintf(ctx->walk->arena, "self.%s", name) : name;
    if (key)
        (void)cbm_source_declare(ctx->walk, owner, key, qn, flags, line);
}

static void ruby_collect_method(ruby_ctx_t *ctx, TSNode method, bool conditional);
static void ruby_collect_singleton_method(ruby_ctx_t *ctx, TSNode method, bool conditional);

/* `alias_method :a, :b`, `alias a b`: a is whatever b was when this ran. */
static void ruby_declare_alias(ruby_ctx_t *ctx, const char *name, const char *original,
                               uint32_t line) {
    const char *owner = ctx->depth ? ruby_current(ctx) : RUBY_OBJECT;
    const char *key = ctx->singleton ? cbm_arena_sprintf(ctx->walk->arena, "self.%s", name) : name;
    cbm_source_decl_t *decl =
        key ? cbm_source_declare(ctx->walk, owner, key, NULL,
                                 original ? RUBY_DECL_ALIAS : CBM_SOURCE_DECL_F_OPEN, line)
            : NULL;
    if (decl && original)
        decl->type = cbm_source_table_intern(
            ctx->walk,
            ctx->singleton ? cbm_arena_sprintf(ctx->walk->arena, "self.%s", original) : original);
}

/* `module_function` copies the instance method onto the module. The copy is one
 * declaration with one node, so it names the method and carries no QN of its own. */
static void ruby_declare_module_function(ruby_ctx_t *ctx, const char *name, uint32_t line) {
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "self.%s", name);
    cbm_source_decl_t *decl = key && ctx->depth
                                  ? cbm_source_declare(ctx->walk, ruby_current(ctx), key, NULL,
                                                       RUBY_DECL_MODULE_FUNCTION, line)
                                  : NULL;
    if (decl)
        decl->type = cbm_source_table_intern(ctx->walk, name);
}

static void ruby_note_body_call(ruby_ctx_t *ctx, TSNode call, bool conditional) {
    const char *name = ruby_call_name(ctx->walk, call);
    if (!name)
        return;
    TSNode arguments = cbm_source_field(call, "arguments");
    if (ts_node_is_null(arguments))
        return;
    uint32_t line = ts_node_start_point(call).row + 1;
    bool attribute = ruby_attribute_macro(name);
    bool rewrite = ruby_rewrites_method(name);
    bool alias = !strcmp(name, "alias_method");
    bool module_function = !strcmp(name, "module_function");
    const char *first = NULL;
    uint32_t index = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(arguments);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode argument = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(argument))
                continue;
            /* `private def x`: the def runs in the class body like any other. */
            if (cbm_source_is(argument, "method")) {
                ruby_collect_method(ctx, argument, conditional);
                if (module_function) {
                    const char *text =
                        cbm_source_text(ctx->walk, cbm_source_field(argument, "name"));
                    if (text)
                        ruby_declare_module_function(ctx, text, line);
                }
            } else if (cbm_source_is(argument, "singleton_method")) {
                ruby_collect_singleton_method(ctx, argument, conditional);
            } else if (cbm_source_is(argument, "call")) {
                ruby_note_body_call(ctx, argument, conditional); /* `private attr_reader :x` */
            } else {
                const char *literal = ruby_literal_symbol(ctx->walk, argument);
                if (literal && module_function)
                    ruby_declare_module_function(ctx, literal, line);
                else if (literal && alias && index == 0)
                    first = literal;
                else if (literal && alias && index == 1 && first)
                    ruby_declare_alias(ctx, first, literal, line);
                else if (literal && (attribute || rewrite))
                    ruby_declare_key(ctx, literal, CBM_SOURCE_DECL_F_OPEN, NULL, line);
                if (literal && attribute &&
                    (!strcmp(name, "attr_accessor") || !strcmp(name, "attr_writer"))) {
                    const char *writer = cbm_arena_sprintf(ctx->walk->arena, "%s=", literal);
                    if (writer)
                        ruby_declare_key(ctx, writer, CBM_SOURCE_DECL_F_OPEN, NULL, line);
                }
            }
            index++;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    if (alias && first && index < 2)
        ruby_declare_alias(ctx, first, NULL, line); /* A computed original abstains. */
}

/* Each mixin keeps the lexical path that its constant is read under. */
static void ruby_gather_mixin(ruby_ctx_t *ctx, TSNode call, bool in_singleton_class,
                              cbm_source_names_t *bases) {
    const char *name = ruby_call_name(ctx->walk, call);
    char tag = 0;
    /* Inside `class << self`, include is what extend is outside it. */
    if (name && !strcmp(name, "include"))
        tag = in_singleton_class ? 'E' : 'I';
    else if (name && !strcmp(name, "prepend") && !in_singleton_class)
        tag = 'P';
    else if (name && !strcmp(name, "extend") && !in_singleton_class)
        tag = 'E';
    if (!tag)
        return;
    TSNode arguments = cbm_source_field(call, "arguments");
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < count; i++) {
        TSNode argument = ts_node_named_child(arguments, i);
        const char *text = NULL;
        if (cbm_source_is(argument, "constant") || cbm_source_is(argument, "scope_resolution") ||
            cbm_source_is(argument, "self"))
            text = cbm_source_text(ctx->walk, argument);
        /* A computed mixin is an unknown ancestor; the '?' keeps that visible. */
        const char *entry =
            cbm_arena_sprintf(ctx->walk->arena, "%c:%s|%s", tag, ruby_cref(ctx), text ? text : "?");
        if (!entry || !cbm_source_names_push(ctx->walk, bases, entry))
            cbm_source_fail(ctx->walk, "ruby_base_allocation");
    }
}

static void ruby_collect_body(ruby_ctx_t *ctx, TSNode body, bool conditional, unsigned depth);

static void ruby_collect_method(ruby_ctx_t *ctx, TSNode method, bool conditional) {
    TSNode name = cbm_source_field(method, "name");
    const char *text = cbm_source_text(ctx->walk, name);
    if (!text)
        return;
    uint32_t line = ts_node_start_point(method).row + 1;
    ruby_declare_key(ctx, text, conditional ? CBM_SOURCE_DECL_F_CONDITIONAL : 0,
                     cbm_source_def_qn(ctx->walk, text, line), line);
    if (ctx->module_function && !ctx->singleton)
        ruby_declare_module_function(ctx, text, line);
}

static void ruby_collect_singleton_method(ruby_ctx_t *ctx, TSNode method, bool conditional) {
    TSNode object = cbm_source_field(method, "object");
    if (!cbm_source_is(object, "self") || !ctx->depth)
        return; /* `def obj.x` targets a value this walk has no type for. */
    bool saved = ctx->singleton;
    ctx->singleton = true;
    ruby_collect_method(ctx, method, conditional);
    ctx->singleton = saved;
}

static void ruby_collect_class(ruby_ctx_t *ctx, TSNode node, bool conditional, unsigned depth) {
    const char *owner = ruby_declared_owner(ctx, cbm_source_field(node, "name"));
    if (!owner)
        return;
    uint32_t flags = CBM_SOURCE_DECL_F_TYPE;
    if (cbm_source_is(node, "module"))
        flags |= CBM_SOURCE_DECL_F_INTERFACE; /* Modules have no Object step. */
    uint32_t line = ts_node_start_point(node).row + 1;
    /* The class's node carries the name as its header spells it. */
    const char *spelled = cbm_source_text(ctx->walk, cbm_source_field(node, "name"));
    const char *qn = NULL;
    if (spelled && spelled[0] == ':' && spelled[1] == ':')
        spelled += 2;
    if (spelled)
        qn = cbm_source_def_qn(ctx->walk, spelled, line);
    cbm_source_decl_t *record = cbm_source_declare(ctx->walk, owner, "", qn, flags, line);
    cbm_source_names_t bases = {0};
    TSNode superclass = cbm_source_field(node, "superclass");
    if (!ts_node_is_null(superclass) && ts_node_named_child_count(superclass)) {
        TSNode parent = ts_node_named_child(superclass, 0);
        const char *text =
            cbm_source_is(parent, "constant") || cbm_source_is(parent, "scope_resolution")
                ? cbm_source_text(ctx->walk, parent)
                : NULL;
        /* Read in the enclosing nest: the class is not in scope for its own header. */
        const char *entry =
            cbm_arena_sprintf(ctx->walk->arena, "S:%s|%s", ruby_cref(ctx), text ? text : "?");
        if (!entry || !cbm_source_names_push(ctx->walk, &bases, entry))
            cbm_source_fail(ctx->walk, "ruby_base_allocation");
    }
    if (!ruby_push(ctx, owner)) {
        cbm_source_fail(ctx->walk, "ruby_nesting_limit");
        return;
    }
    bool saved = ctx->singleton;
    bool saved_function = ctx->module_function;
    ctx->singleton = false;
    ctx->module_function = false;
    TSNode body = cbm_source_field(node, "body");
    if (!ts_node_is_null(body)) {
        TSTreeCursor cursor = ts_tree_cursor_new(body);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (cbm_source_is(child, "call")) {
                    ruby_gather_mixin(ctx, child, false, &bases);
                } else if (cbm_source_is(child, "singleton_class") &&
                           cbm_source_is(cbm_source_field(child, "value"), "self")) {
                    TSNode inner = cbm_source_field(child, "body");
                    if (ts_node_is_null(inner))
                        continue;
                    TSTreeCursor walker = ts_tree_cursor_new(inner);
                    if (ts_tree_cursor_goto_first_child(&walker)) {
                        do {
                            TSNode statement = ts_tree_cursor_current_node(&walker);
                            if (cbm_source_is(statement, "call"))
                                ruby_gather_mixin(ctx, statement, true, &bases);
                        } while (ts_tree_cursor_goto_next_sibling(&walker));
                    }
                    ts_tree_cursor_delete(&walker);
                }
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }
    if (record) {
        const char **copy = cbm_source_table_alloc(ctx->walk, (bases.count + 1) * sizeof(*copy));
        if (copy) {
            for (size_t i = 0; i < bases.count; i++)
                copy[i] = cbm_source_table_intern(ctx->walk, bases.items[i]);
            copy[bases.count] = NULL;
            record->bases = copy;
        }
    }
    if (!ts_node_is_null(body))
        ruby_collect_body(ctx, body, conditional, depth + 1);
    ctx->singleton = saved;
    ctx->module_function = saved_function;
    ctx->depth--;
}

/* `NAME = value`. A constant holding another constant is that constant's second name. */
static void ruby_collect_constant(ruby_ctx_t *ctx, TSNode assignment, bool conditional) {
    TSNode left = cbm_source_field(assignment, "left");
    if (!cbm_source_is(left, "constant"))
        return;
    const char *name = cbm_source_text(ctx->walk, left);
    if (!name)
        return;
    const char *path =
        ctx->depth ? cbm_arena_sprintf(ctx->walk->arena, "%s::%s", ruby_current(ctx), name) : name;
    uint32_t line = ts_node_start_point(assignment).row + 1;
    if (!path)
        return;
    cbm_source_decl_t *decl =
        cbm_source_declare(ctx->walk, path, "", cbm_source_def_qn(ctx->walk, name, line),
                           conditional ? CBM_SOURCE_DECL_F_VALUE | CBM_SOURCE_DECL_F_CONDITIONAL
                                       : CBM_SOURCE_DECL_F_VALUE,
                           line);
    TSNode right = cbm_source_field(assignment, "right");
    const char *held = cbm_source_is(right, "constant") || cbm_source_is(right, "scope_resolution")
                           ? cbm_source_text(ctx->walk, right)
                           : NULL;
    if (decl && held && !conditional) {
        decl->flags |= RUBY_DECL_CONSTANT_ALIAS;
        decl->type = cbm_source_table_intern(
            ctx->walk, cbm_arena_sprintf(ctx->walk->arena, "%s|%s", ruby_cref(ctx), held));
    }
}

static bool ruby_block_node(TSNode node) {
    return cbm_source_is(node, "block") || cbm_source_is(node, "do_block") ||
           cbm_source_is(node, "lambda");
}

static void ruby_collect_body(ruby_ctx_t *ctx, TSNode body, bool conditional, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            if (cbm_source_is(child, "class") || cbm_source_is(child, "module")) {
                ruby_collect_class(ctx, child, conditional, depth);
            } else if (cbm_source_is(child, "singleton_class")) {
                if (!cbm_source_is(cbm_source_field(child, "value"), "self") || !ctx->depth)
                    continue;
                bool saved = ctx->singleton;
                ctx->singleton = true;
                TSNode inner = cbm_source_field(child, "body");
                if (!ts_node_is_null(inner))
                    ruby_collect_body(ctx, inner, conditional, depth + 1);
                ctx->singleton = saved;
            } else if (cbm_source_is(child, "method")) {
                ruby_collect_method(ctx, child, conditional);
            } else if (cbm_source_is(child, "singleton_method")) {
                ruby_collect_singleton_method(ctx, child, conditional);
            } else if (cbm_source_is(child, "alias")) {
                const char *name = cbm_source_text(ctx->walk, cbm_source_field(child, "name"));
                const char *original = cbm_source_text(ctx->walk, cbm_source_field(child, "alias"));
                if (name)
                    ruby_declare_alias(ctx, name, original, ts_node_start_point(child).row + 1);
            } else if (cbm_source_is(child, "call")) {
                ruby_note_body_call(ctx, child, conditional);
            } else if (cbm_source_is(child, "identifier")) {
                const char *word = ctx->depth ? cbm_source_text(ctx->walk, child) : NULL;
                if (word && !strcmp(word, "module_function"))
                    ctx->module_function = true;
            } else if (cbm_source_is(child, "assignment")) {
                ruby_collect_constant(ctx, child, conditional);
                ruby_collect_body(ctx, child, true, depth + 1);
            } else if (ruby_block_node(child)) {
                /* A def in a block belongs to whatever the block runs against. */
                continue;
            } else {
                /* `if`, `unless`, `begin` and friends hold conditional declarations. */
                ruby_collect_body(ctx, child, true, depth + 1);
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* ── Phase one: which defs can rebind their caller's block ─────────── */

typedef struct {
    ruby_ctx_t ctx;
    cbm_source_walk_t *walk;
    const char *block_param; /* the `&name` of the def being read */
    const char *def_name;
    const char *def_key;   /* the def's own table key */
    const char *def_owner; /* NULL for a def in a block, which no chain reaches */
    uint32_t def_line;
    bool in_def;
    bool in_block;
} ruby_def_audit_t;

static void ruby_note_rebinding(ruby_def_audit_t *audit, const char *forwarded_to) {
    cbm_source_decl_t *global = cbm_source_declare(audit->walk, RUBY_REBINDING_DEFS,
                                                   audit->def_name, NULL, 0, audit->def_line);
    const char *own = audit->def_owner ? cbm_arena_sprintf(audit->walk->arena, "%s%s",
                                                           RUBY_REBINDING_OF, audit->def_owner)
                                       : NULL;
    cbm_source_decl_t *local =
        own ? cbm_source_declare(audit->walk, own, audit->def_key, NULL, 0, audit->def_line) : NULL;
    if (forwarded_to) {
        if (global)
            global->type = cbm_source_table_intern(audit->walk, forwarded_to);
        if (local)
            local->type = cbm_source_table_intern(audit->walk, forwarded_to);
    }
}

/* Asking about a block or running it keeps self; anything else lets it be kept. */
static bool ruby_block_read_is_harmless(cbm_source_walk_t *walk, TSNode read, TSNode parent) {
    if (cbm_source_is(parent, "call") && ts_node_eq(cbm_source_field(parent, "receiver"), read)) {
        static const char *const names[] = {"call",    "yield",    "arity",  "parameters", "nil?",
                                            "lambda?", "present?", "blank?", NULL};
        TSNode method = cbm_source_field(parent, "method");
        return ts_node_is_null(method) || ruby_name_in(cbm_source_text(walk, method), names);
    }
    if (cbm_source_is(parent, "element_reference") || cbm_source_is(parent, "unary"))
        return true;
    if (cbm_source_is(parent, "binary") && cbm_source_token(parent, "&&"))
        return true;
    return ts_node_eq(cbm_source_field(parent, "condition"), read);
}

static void ruby_audit_block_use(ruby_def_audit_t *audit, TSNode node, TSNode parent, TSNode call) {
    cbm_source_walk_t *walk = audit->walk;
    if (cbm_source_is(node, "super")) {
        ruby_note_rebinding(audit, NULL); /* A bare super hands the block on, to whatever. */
    } else if (cbm_source_is(node, "block_argument")) {
        TSNode passed =
            ts_node_named_child_count(node) ? ts_node_named_child(node, 0) : (TSNode){0};
        const char *text =
            cbm_source_is(passed, "identifier") ? cbm_source_text(walk, passed) : NULL;
        bool forwarded = ts_node_is_null(passed) ||
                         (text && audit->block_param && !strcmp(text, audit->block_param));
        if (forwarded) {
            /* The block goes wherever the callee takes it. A callee on self can be read
             * in the project; on any other receiver it can be anything's. */
            const char *target = cbm_source_text(walk, cbm_source_field(call, "method"));
            TSNode receiver = cbm_source_field(call, "receiver");
            bool on_self = ts_node_is_null(receiver) || cbm_source_is(receiver, "self");
            if (!target || !ruby_core_block_method(target))
                ruby_note_rebinding(audit, on_self ? target : NULL);
        }
    } else if (cbm_source_is(node, "identifier") && audit->block_param &&
               !cbm_source_is(parent, "block_argument") &&
               !cbm_source_is(parent, "block_parameter")) {
        const char *text = cbm_source_text(walk, node);
        if (text && !strcmp(text, audit->block_param) &&
            !ruby_block_read_is_harmless(walk, node, parent))
            ruby_note_rebinding(audit, NULL);
    }
}

static const char *ruby_block_parameter(cbm_source_walk_t *walk, TSNode def) {
    TSNode parameters = cbm_source_field(def, "parameters");
    if (ts_node_is_null(parameters))
        return NULL;
    const char *name = NULL;
    TSTreeCursor cursor = ts_tree_cursor_new(parameters);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (cbm_source_is(child, "block_parameter"))
                name = cbm_source_text(walk, cbm_source_field(child, "name"));
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return name;
}

/* Every def counts, even one in a block no chain can reach: its name can rebind. */
static void ruby_audit_defs(ruby_def_audit_t *audit, TSNode node, TSNode parent, TSNode call,
                            unsigned depth) {
    if (!cbm_source_step(audit->walk, depth))
        return;
    bool def = cbm_source_is(node, "method") || cbm_source_is(node, "singleton_method");
    ruby_def_audit_t saved = *audit;
    if (def) {
        bool singleton = audit->ctx.singleton || cbm_source_is(node, "singleton_method");
        audit->def_name = cbm_source_text(audit->walk, cbm_source_field(node, "name"));
        audit->def_key = singleton && audit->def_name
                             ? cbm_arena_sprintf(audit->walk->arena, "self.%s", audit->def_name)
                             : audit->def_name;
        audit->def_owner = audit->in_block    ? NULL
                           : audit->ctx.depth ? ruby_current(&audit->ctx)
                                              : RUBY_OBJECT;
        audit->def_line = ts_node_start_point(node).row + 1;
        audit->in_def = audit->def_name != NULL;
        audit->block_param = ruby_block_parameter(audit->walk, node);
        if (audit->def_name)
            (void)cbm_source_declare(audit->walk, RUBY_ALL_DEFS, audit->def_name, NULL, 0,
                                     audit->def_line);
    } else if (audit->in_def) {
        ruby_audit_block_use(audit, node, parent, call);
    } else if (ruby_block_node(node)) {
        audit->in_block = true;
    } else if (cbm_source_is(node, "class") || cbm_source_is(node, "module")) {
        const char *owner = ruby_declared_owner(&audit->ctx, cbm_source_field(node, "name"));
        if (owner && ruby_push(&audit->ctx, owner))
            audit->ctx.singleton = false;
    } else if (cbm_source_is(node, "singleton_class")) {
        audit->ctx.singleton = cbm_source_is(cbm_source_field(node, "value"), "self");
    }
    TSNode next_call = cbm_source_is(node, "call") ? node : call;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                ruby_audit_defs(audit, child, node, next_call, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    /* A def's own notes are written as it goes; the rest of this state is its scope. */
    if (def || ruby_block_node(node) || cbm_source_is(node, "class") ||
        cbm_source_is(node, "module") || cbm_source_is(node, "singleton_class"))
        *audit = saved;
}

static void ruby_collect(cbm_source_walk_t *walk) {
    ruby_ctx_t ctx = {.walk = walk};
    ruby_collect_body(&ctx, walk->root, false, 0);
    ruby_def_audit_t audit = {.ctx = {.walk = walk}, .walk = walk};
    ruby_audit_defs(&audit, walk->root, (TSNode){0}, (TSNode){0}, 0);
}

/* ── Phase two: constants and ancestor chains ──────────────────────── */

enum { RUBY_CONSTANT_BUFFER = 1024 };

static const cbm_source_decl_t *ruby_constant_at(const ruby_ctx_t *ctx, const char *owner,
                                                 const char *name, size_t name_length) {
    char candidate[RUBY_CONSTANT_BUFFER];
    int length =
        owner && owner[0]
            ? snprintf(candidate, sizeof(candidate), "%s::%.*s", owner, (int)name_length, name)
            : snprintf(candidate, sizeof(candidate), "%.*s", (int)name_length, name);
    return length > 0 && (size_t)length < sizeof(candidate)
               ? cbm_source_lookup(ctx->walk, candidate, "")
               : NULL;
}

static bool ruby_is_type(const cbm_source_decl_t *records) {
    for (const cbm_source_decl_t *record = records; record; record = record->next)
        if (record->flags & CBM_SOURCE_DECL_F_TYPE)
            return true;
    return false;
}

static const cbm_source_decl_t *ruby_constant_path(const ruby_ctx_t *ctx, const char *const *nest,
                                                   int depth, const char *text, bool ancestors);

/* Splits a stored "a;a::b" in place; returns how many scopes it holds. */
static int ruby_split_cref(char *cref, const char **nest) {
    int depth = 0;
    for (char *part = cref; *part && depth < RUBY_MAX_NEST;) {
        nest[depth++] = part;
        char *end = strchr(part, ';');
        if (!end)
            break;
        *end = '\0';
        part = end + 1;
    }
    return depth;
}

/* A constant written once as another constant reads as that one. A cycle of these
 * names no class; the walk visits each name at most once. */
static const cbm_source_decl_t *ruby_follow_constant(const ruby_ctx_t *ctx,
                                                     const cbm_source_decl_t *records) {
    cbm_source_chain_t seen = {0};
    while (records && !records->next && (records->flags & RUBY_DECL_CONSTANT_ALIAS)) {
        if (!cbm_source_chain_first_visit(ctx->walk, &seen, records->owner))
            return NULL;
        const char *bar = strchr(records->type, '|');
        char cref[RUBY_CONSTANT_BUFFER];
        size_t length = bar ? (size_t)(bar - records->type) : sizeof(cref);
        if (length >= sizeof(cref))
            return NULL;
        memcpy(cref, records->type, length);
        cref[length] = '\0';
        const char *nest[RUBY_MAX_NEST];
        int depth = ruby_split_cref(cref, nest);
        records = ruby_constant_path(ctx, nest, depth, bar + 1, false);
    }
    return records;
}

/* Reads a stored "tag:cref|text" base. Lexical and top level only: looking through an
 * ancestor here would need the ancestors this is computing. */
static const char *ruby_base_owner(const ruby_ctx_t *ctx, const char *base) {
    const char *bar = strchr(base, '|');
    if (!bar || !base[1])
        return NULL;
    char cref[RUBY_CONSTANT_BUFFER];
    size_t length = (size_t)(bar - (base + 2));
    if (length >= sizeof(cref))
        return NULL;
    memcpy(cref, base + 2, length);
    cref[length] = '\0';
    const char *nest[RUBY_MAX_NEST];
    int depth = ruby_split_cref(cref, nest);
    const cbm_source_decl_t *records =
        ruby_follow_constant(ctx, ruby_constant_path(ctx, nest, depth, bar + 1, false));
    return ruby_is_type(records) ? records->owner : NULL;
}

/* A constant is inherited from prepended modules, included modules and the superclass.
 * Extending a class gives it methods, not constants. An unknown ancestor can only be
 * passed over: its table cannot be read. */
static const cbm_source_decl_t *ruby_constant_of_ancestors(const ruby_ctx_t *ctx, const char *owner,
                                                           const char *name, size_t length,
                                                           cbm_source_chain_t *chain);

static const cbm_source_decl_t *ruby_constant_above(const ruby_ctx_t *ctx, const char *owner,
                                                    const char *name, size_t length,
                                                    cbm_source_chain_t *chain) {
    static const char tags[] = {'P', 'I', 'S'};
    const cbm_source_decl_t *records = cbm_source_lookup(ctx->walk, owner, "");
    for (size_t t = 0; t < sizeof(tags); t++) {
        for (const cbm_source_decl_t *record = records; record; record = record->next) {
            size_t count = 0;
            while (record->bases && record->bases[count])
                count++;
            for (size_t i = count; i > 0; i--) {
                if (record->bases[i - 1][0] != tags[t])
                    continue;
                const char *parent = ruby_base_owner(ctx, record->bases[i - 1]);
                if (!parent)
                    continue;
                const cbm_source_decl_t *found = ruby_constant_at(ctx, parent, name, length);
                if (!found)
                    found = ruby_constant_of_ancestors(ctx, parent, name, length, chain);
                if (found)
                    return found;
            }
        }
    }
    return NULL;
}

static const cbm_source_decl_t *ruby_constant_of_ancestors(const ruby_ctx_t *ctx, const char *owner,
                                                           const char *name, size_t length,
                                                           cbm_source_chain_t *chain) {
    if (!cbm_source_chain_enter(ctx->walk, chain, owner))
        return NULL;
    const cbm_source_decl_t *found = ruby_constant_above(ctx, owner, name, length, chain);
    cbm_source_chain_leave(chain);
    return found;
}

/* Reading `::A`, `A` or `A::B::C` with these scopes enclosing it, innermost last. */
static const cbm_source_decl_t *ruby_constant_path(const ruby_ctx_t *ctx, const char *const *nest,
                                                   int depth, const char *text, bool ancestors) {
    if (!text || !text[0] || !strcmp(text, "?"))
        return NULL;
    bool absolute = text[0] == ':' && text[1] == ':';
    if (absolute)
        text += 2;
    const char *colons = strstr(text, "::");
    size_t first = colons ? (size_t)(colons - text) : strlen(text);
    const cbm_source_decl_t *found = NULL;
    if (!absolute) {
        for (int i = depth - 1; !found && i >= 0; i--)
            found = ruby_constant_at(ctx, nest[i], text, first);
        if (!found && ancestors && depth > 0) {
            cbm_source_chain_t chain = {0};
            found = ruby_constant_of_ancestors(ctx, nest[depth - 1], text, first, &chain);
        }
    }
    if (!found)
        found = ruby_constant_at(ctx, "", text, first);
    /* `class A::B` declares a path whose head can lie outside the project. */
    if (!found)
        return colons ? cbm_source_lookup(ctx->walk, text, "") : NULL;
    while (colons) {
        const char *rest = colons + 2;
        colons = strstr(rest, "::");
        size_t length = colons ? (size_t)(colons - rest) : strlen(rest);
        const cbm_source_decl_t *holder = ruby_follow_constant(ctx, found);
        if (!holder)
            return NULL;
        const cbm_source_decl_t *member = ruby_constant_at(ctx, holder->owner, rest, length);
        if (!member) {
            cbm_source_chain_t chain = {0};
            member = ruby_constant_of_ancestors(ctx, holder->owner, rest, length, &chain);
        }
        if (!member)
            return ruby_constant_at(ctx, holder->owner, rest, strlen(rest));
        found = member;
    }
    return found;
}

typedef enum { RUBY_FOUND, RUBY_ABSENT, RUBY_OPEN, RUBY_AMBIGUOUS } ruby_lookup_t;

/* The declaration of a name that stands once its file has run, if one is certain. */
static const cbm_source_decl_t *ruby_standing_declaration(const cbm_source_decl_t *first) {
    const cbm_source_decl_t *last = first;
    for (const cbm_source_decl_t *decl = first; decl; decl = decl->next) {
        if (strcmp(decl->file, first->file) || (decl->flags & CBM_SOURCE_DECL_F_CONDITIONAL))
            return NULL;
        if (decl->line >= last->line)
            last = decl;
    }
    return last;
}

static ruby_lookup_t ruby_own(const ruby_ctx_t *ctx, const char *owner, const char *key,
                              const cbm_source_decl_t **out) {
    const cbm_source_decl_t *first = cbm_source_lookup(ctx->walk, owner, key);
    if (!first)
        return RUBY_ABSENT;
    const cbm_source_decl_t *standing = ruby_standing_declaration(first);
    /* An alias must name an earlier method of its own file, which is what it copied.
     * Each step moves to an earlier line, so a chain of these ends. */
    while (standing && (standing->flags & (RUBY_DECL_ALIAS | RUBY_DECL_MODULE_FUNCTION))) {
        const cbm_source_decl_t *named = cbm_source_lookup(ctx->walk, owner, standing->type);
        const cbm_source_decl_t *original = named ? ruby_standing_declaration(named) : NULL;
        if (!original || strcmp(original->file, standing->file))
            return RUBY_AMBIGUOUS;
        if ((standing->flags & RUBY_DECL_ALIAS) && original->line >= standing->line)
            return RUBY_AMBIGUOUS;
        standing = original;
    }
    if (!standing || !standing->qn ||
        (standing->flags & (CBM_SOURCE_DECL_F_OPEN | CBM_SOURCE_DECL_F_QN_SHARED)))
        return RUBY_AMBIGUOUS;
    *out = standing;
    return RUBY_FOUND;
}

static ruby_lookup_t ruby_find_instance(const ruby_ctx_t *ctx, const char *owner, const char *name,
                                        cbm_source_chain_t *chain, const cbm_source_decl_t **out);

/* Mixins of one tag across every reopening, latest first as Ruby applies them. */
static ruby_lookup_t ruby_search_mixins(const ruby_ctx_t *ctx, const cbm_source_decl_t *records,
                                        char tag, const char *owner, const char *name,
                                        cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    for (const cbm_source_decl_t *record = records; record; record = record->next) {
        size_t count = 0;
        while (record->bases && record->bases[count])
            count++;
        for (size_t i = count; i > 0; i--) {
            const char *base = record->bases[i - 1];
            if (base[0] != tag)
                continue;
            const char *bar = strchr(base, '|');
            if (tag == 'E' && bar && !strcmp(bar + 1, "self")) {
                /* `extend self`: the module's instance methods are its own. */
                ruby_lookup_t own = ruby_own(ctx, owner, name, out);
                if (own != RUBY_ABSENT)
                    return own;
                continue;
            }
            const char *mixin = ruby_base_owner(ctx, base);
            if (!mixin)
                return RUBY_OPEN;
            ruby_lookup_t found = ruby_find_instance(ctx, mixin, name, chain, out);
            if (found != RUBY_ABSENT)
                return found;
        }
    }
    return RUBY_ABSENT;
}

static ruby_lookup_t ruby_search_superclass(const ruby_ctx_t *ctx, const cbm_source_decl_t *records,
                                            const char **superclass, bool *has_superclass) {
    *has_superclass = false;
    *superclass = NULL;
    for (const cbm_source_decl_t *record = records; record; record = record->next) {
        for (size_t i = 0; record->bases && record->bases[i]; i++) {
            if (record->bases[i][0] != 'S')
                continue;
            const char *parent = ruby_base_owner(ctx, record->bases[i]);
            if (!parent)
                return RUBY_OPEN;
            if (*has_superclass && strcmp(*superclass, parent))
                return RUBY_AMBIGUOUS;
            *has_superclass = true;
            *superclass = parent;
        }
    }
    return RUBY_ABSENT;
}

static ruby_lookup_t ruby_instance_of(const ruby_ctx_t *ctx, const char *owner, const char *name,
                                      cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (!strcmp(owner, RUBY_OBJECT))
        return ruby_own(ctx, RUBY_OBJECT, name, out);
    const cbm_source_decl_t *records = cbm_source_lookup(ctx->walk, owner, "");
    if (!records)
        return RUBY_OPEN; /* Outside the project: any method may be on it. */
    ruby_lookup_t found = ruby_search_mixins(ctx, records, 'P', owner, name, chain, out);
    if (found != RUBY_ABSENT)
        return found;
    found = ruby_own(ctx, owner, name, out);
    if (found != RUBY_ABSENT)
        return found;
    found = ruby_search_mixins(ctx, records, 'I', owner, name, chain, out);
    if (found != RUBY_ABSENT)
        return found;
    const char *superclass = NULL;
    bool has_superclass = false;
    found = ruby_search_superclass(ctx, records, &superclass, &has_superclass);
    if (found != RUBY_ABSENT)
        return found;
    if (has_superclass)
        return ruby_find_instance(ctx, superclass, name, chain, out);
    for (const cbm_source_decl_t *record = records; record; record = record->next)
        if (record->flags & CBM_SOURCE_DECL_F_INTERFACE)
            return RUBY_ABSENT; /* A module has no Object step of its own. */
    return ruby_own(ctx, RUBY_OBJECT, name, out);
}

static ruby_lookup_t ruby_find_instance(const ruby_ctx_t *ctx, const char *owner, const char *name,
                                        cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (!cbm_source_chain_enter(ctx->walk, chain, owner))
        return chain->overflow ? RUBY_OPEN : RUBY_ABSENT;
    ruby_lookup_t found = ruby_instance_of(ctx, owner, name, chain, out);
    cbm_source_chain_leave(chain);
    return found;
}

static ruby_lookup_t ruby_find_singleton_on(const ruby_ctx_t *ctx, const char *owner,
                                            const char *name, cbm_source_chain_t *chain,
                                            cbm_source_chain_t *extended,
                                            const cbm_source_decl_t **out);

static ruby_lookup_t ruby_singleton_of(const ruby_ctx_t *ctx, const char *owner, const char *name,
                                       cbm_source_chain_t *chain, cbm_source_chain_t *extended,
                                       const cbm_source_decl_t **out) {
    const cbm_source_decl_t *records = cbm_source_lookup(ctx->walk, owner, "");
    if (!records)
        return RUBY_OPEN;
    char key[512];
    int length = snprintf(key, sizeof(key), "self.%s", name);
    if (length < 0 || (size_t)length >= sizeof(key))
        return RUBY_OPEN;
    ruby_lookup_t found = ruby_own(ctx, owner, key, out);
    if (found != RUBY_ABSENT)
        return found;
    found = ruby_search_mixins(ctx, records, 'E', owner, name, extended, out);
    if (found != RUBY_ABSENT)
        return found;
    const char *superclass = NULL;
    bool has_superclass = false;
    found = ruby_search_superclass(ctx, records, &superclass, &has_superclass);
    if (found != RUBY_ABSENT)
        return found;
    if (has_superclass)
        return ruby_find_singleton_on(ctx, superclass, name, chain, extended, out);
    /* A class object is itself an Object, so a top-level def is reachable. */
    return ruby_own(ctx, RUBY_OBJECT, name, out);
}

static ruby_lookup_t ruby_find_singleton_on(const ruby_ctx_t *ctx, const char *owner,
                                            const char *name, cbm_source_chain_t *chain,
                                            cbm_source_chain_t *extended,
                                            const cbm_source_decl_t **out) {
    if (!cbm_source_chain_enter(ctx->walk, chain, owner))
        return chain->overflow ? RUBY_OPEN : RUBY_ABSENT;
    ruby_lookup_t found = ruby_singleton_of(ctx, owner, name, chain, extended, out);
    cbm_source_chain_leave(chain);
    return found;
}

static ruby_lookup_t ruby_find_singleton(const ruby_ctx_t *ctx, const char *owner, const char *name,
                                         const cbm_source_decl_t **out) {
    cbm_source_chain_t chain = {0}, extended = {0};
    return ruby_find_singleton_on(ctx, owner, name, &chain, &extended, out);
}

/* `super` in a method of owner: what follows owner's own step on its chain. Where a module's
 * method goes next depends on what includes it, so that stays unknown. */
static ruby_lookup_t ruby_find_super(const ruby_ctx_t *ctx, const char *owner, const char *name,
                                     bool singleton, const cbm_source_decl_t **out) {
    const cbm_source_decl_t *records = cbm_source_lookup(ctx->walk, owner, "");
    if (!records)
        return RUBY_OPEN;
    for (const cbm_source_decl_t *record = records; record; record = record->next)
        if (!singleton && (record->flags & CBM_SOURCE_DECL_F_INTERFACE))
            return RUBY_OPEN;
    cbm_source_chain_t chain = {0}, extended = {0};
    (void)cbm_source_chain_first_visit(ctx->walk, &chain, owner);
    ruby_lookup_t found = singleton
                              ? ruby_search_mixins(ctx, records, 'E', owner, name, &extended, out)
                              : ruby_search_mixins(ctx, records, 'I', owner, name, &chain, out);
    if (found != RUBY_ABSENT)
        return found;
    const char *superclass = NULL;
    bool has_superclass = false;
    found = ruby_search_superclass(ctx, records, &superclass, &has_superclass);
    if (found != RUBY_ABSENT)
        return found;
    if (!has_superclass)
        return RUBY_ABSENT;
    return singleton ? ruby_find_singleton_on(ctx, superclass, name, &chain, &extended, out)
                     : ruby_find_instance(ctx, superclass, name, &chain, out);
}

static ruby_lookup_t ruby_find_instance_from(const ruby_ctx_t *ctx, const char *owner,
                                             const char *name, const cbm_source_decl_t **out) {
    cbm_source_chain_t chain = {0};
    return ruby_find_instance(ctx, owner, name, &chain, out);
}

/* ── Phase two: locals and occurrences ─────────────────────────────── */

typedef struct ruby_local {
    const char *name;
    TSNode value;
    uint32_t available_from;
    uint32_t bound_from; /* where the first assignment's name is, as the parser sees it */
    uint32_t first_line; /* the line of that assignment */
    int writes;
    bool blocked;
    struct ruby_local *next;
} ruby_local_t;

typedef struct {
    ruby_ctx_t base;
    ruby_self_t self;
    ruby_local_t *locals;
    const char *scope;                 /* the enclosing def's QN; NULL in a class body or unknown */
    const char *def_name;              /* the def around this point, which `super` selects by */
    bool def_singleton;                /* and whether it is a method of the class itself */
    const cbm_source_decl_t *selected; /* what the call just visited selected, or NULL */
} ruby_visit_t;

static ruby_local_t *ruby_local(ruby_visit_t *visit, const char *name) {
    for (ruby_local_t *local = visit->locals; local; local = local->next)
        if (!strcmp(local->name, name))
            return local;
    ruby_local_t *local = cbm_arena_calloc(visit->base.walk->arena, sizeof(*local));
    if (!local) {
        cbm_source_fail(visit->base.walk, "ruby_scope_allocation");
        return NULL;
    }
    local->name = name;
    local->bound_from = UINT32_MAX;
    local->next = visit->locals;
    visit->locals = local;
    return local;
}

static void ruby_block_local_name(ruby_visit_t *visit, TSNode node) {
    const char *name = cbm_source_text(visit->base.walk, node);
    ruby_local_t *local = name ? ruby_local(visit, name) : NULL;
    if (local)
        local->blocked = true;
}

/* The names a parameter list, pattern or assignment target binds. A receiver or index
 * in `a.b, c[i] = 1, 2` is read rather than bound, and a default value runs as code. */
static void ruby_block_locals(ruby_visit_t *visit, TSNode node, unsigned depth) {
    if (!cbm_source_step(visit->base.walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        ruby_block_local_name(visit, node);
        return;
    }
    if (cbm_source_is(node, "call") || cbm_source_is(node, "element_reference"))
        return;
    if (cbm_source_is(node, "optional_parameter") || cbm_source_is(node, "keyword_parameter")) {
        ruby_block_local_name(visit, cbm_source_field(node, "name"));
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                ruby_block_locals(visit, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void ruby_audit_locals(ruby_visit_t *visit, TSNode node, unsigned depth) {
    if (!cbm_source_step(visit->base.walk, depth) || ts_node_is_null(node))
        return;
    if (cbm_source_is(node, "method") || cbm_source_is(node, "singleton_method") ||
        cbm_source_is(node, "class") || cbm_source_is(node, "module") ||
        cbm_source_is(node, "singleton_class"))
        return;
    if (cbm_source_is(node, "assignment")) {
        TSNode left = cbm_source_field(node, "left");
        if (cbm_source_is(left, "identifier")) {
            const char *name = cbm_source_text(visit->base.walk, left);
            ruby_local_t *local = name ? ruby_local(visit, name) : NULL;
            if (local) {
                local->writes++;
                local->value = cbm_source_field(node, "right");
                local->available_from = ts_node_end_byte(node);
                if (ts_node_start_byte(left) < local->bound_from) {
                    local->bound_from = ts_node_start_byte(left);
                    local->first_line = ts_node_start_point(left).row + 1;
                }
            }
        } else {
            ruby_block_locals(visit, left, depth + 1);
        }
    } else if (cbm_source_is(node, "operator_assignment") ||
               cbm_source_is(node, "block_parameters") ||
               cbm_source_is(node, "method_parameters") ||
               cbm_source_is(node, "exception_variable") ||
               cbm_source_is(node, "lambda_parameters")) {
        ruby_block_locals(
            visit,
            cbm_source_is(node, "operator_assignment") ? cbm_source_field(node, "left") : node,
            depth + 1);
    } else if (cbm_source_is(node, "for")) {
        ruby_block_locals(visit, cbm_source_field(node, "pattern"), depth + 1);
    } else if (strstr(ts_node_type(node), "pattern")) {
        /* `in {x:}` and `=> x` bind what they name. */
        ruby_block_locals(visit, node, depth + 1);
    } else if (cbm_source_is(node, "binary") &&
               cbm_source_is(cbm_source_field(node, "left"), "regex")) {
        /* `/(?<year>..)/ =~ s` binds year, by the literal's own text. */
        const char *text = cbm_source_text(visit->base.walk, cbm_source_field(node, "left"));
        for (const char *p = text ? strstr(text, "(?<") : NULL; p; p = strstr(p + 3, "(?<")) {
            const char *close = strchr(p + 3, '>');
            char *name =
                close ? cbm_arena_strndup(visit->base.walk->arena, p + 3, (size_t)(close - p - 3))
                      : NULL;
            ruby_local_t *local = name && name[0] ? ruby_local(visit, name) : NULL;
            if (local)
                local->blocked = true;
        }
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                ruby_audit_locals(visit, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void ruby_visit(ruby_visit_t *visit, TSNode node, unsigned depth);

static const cbm_source_decl_t *ruby_constant_records(ruby_visit_t *visit, TSNode node) {
    if (!cbm_source_is(node, "constant") && !cbm_source_is(node, "scope_resolution"))
        return NULL;
    /* A qualified constant may not have a project prefix at all. */
    return ruby_constant_path(&visit->base, visit->base.nest, visit->base.depth,
                              cbm_source_text(visit->base.walk, node), true);
}

/* The class a constant receiver names; a constant holding some value names none. */
static const char *ruby_constant_owner(ruby_visit_t *visit, TSNode node) {
    const cbm_source_decl_t *records =
        ruby_follow_constant(&visit->base, ruby_constant_records(visit, node));
    return ruby_is_type(records) ? records->owner : NULL;
}

static void ruby_emit(ruby_visit_t *visit, TSNode node, const char *target, cbm_resolve_rule_t rule,
                      const char *domain, const char *reason) {
    cbm_source_emit_node(visit->base.walk, node, CBM_SOURCE_SITE_CALL, target, rule, domain,
                         reason);
}

static bool ruby_emit_lookup(ruby_visit_t *visit, TSNode node, ruby_lookup_t found,
                             const cbm_source_decl_t *decl, const char *domain) {
    switch (found) {
    case RUBY_FOUND: {
        cbm_resolve_rule_t rule = decl->owner && domain && !strcmp(decl->owner, domain)
                                      ? CBM_RESOLVE_RULE_SCOPE_BINDING
                                      : CBM_RESOLVE_RULE_LANGUAGE_BINDING;
        ruby_emit(visit, node, decl->qn, rule, domain, "ruby_ancestor_chain_declaration");
        visit->selected = decl;
        return true;
    }
    case RUBY_ABSENT:
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                  "ruby_method_absent_from_known_chain");
        break;
    case RUBY_OPEN:
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                  "ruby_ancestor_chain_leaves_project");
        break;
    case RUBY_AMBIGUOUS:
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                  "ruby_method_not_one_static_declaration");
        break;
    }
    return false;
}

/* An occurrence extraction has no carrier for. */
static void ruby_extra_begin(ruby_visit_t *visit, TSNode node, const char *name) {
    cbm_source_walk_t *walk = visit->base.walk;
    /* With no enclosing def it belongs where a carrier's would: the file's module. */
    walk->extra_scope = visit->scope ? visit->scope : walk->result->module_qn;
    walk->extra_name = name;
    walk->extra_line = ts_node_start_point(node).row + 1;
}

static void ruby_extra_end(ruby_visit_t *visit) {
    cbm_source_walk_t *walk = visit->base.walk;
    walk->extra_scope = NULL;
    walk->extra_name = NULL;
    walk->extra_line = 0;
}

/* `Const.new`, as the comment at the top of this file states it. */
static bool ruby_visit_constructor(ruby_visit_t *visit, TSNode node, const char *owner) {
    const cbm_source_decl_t *decl = NULL;
    ruby_lookup_t custom = ruby_find_singleton(&visit->base, owner, "new", &decl);
    if (custom == RUBY_FOUND || custom == RUBY_AMBIGUOUS)
        return ruby_emit_lookup(visit, node, custom, decl, owner);
    ruby_lookup_t initialize = ruby_find_instance_from(&visit->base, owner, "initialize", &decl);
    if (initialize == RUBY_FOUND || initialize == RUBY_AMBIGUOUS)
        return ruby_emit_lookup(visit, node, initialize, decl, owner);
    const char *reason = NULL;
    const char *qn = cbm_source_unique_qn(cbm_source_lookup(visit->base.walk, owner, ""), &reason);
    if (!qn) {
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                  "ruby_method_not_one_static_declaration");
        return false;
    }
    cbm_source_emit_facts(visit->base.walk, ts_node_start_byte(node), ts_node_end_byte(node),
                          CBM_SOURCE_SITE_CALL, qn, CBM_RESOLVE_RULE_LANGUAGE_BINDING, owner,
                          "ruby_new_constructs_the_class", CBM_RESOLVE_FACT_CLASS_CALLABLE);
    return true;
}

static bool ruby_visit_implicit_self(ruby_visit_t *visit, TSNode node, const char *name) {
    const cbm_source_decl_t *decl = NULL;
    ruby_lookup_t found = RUBY_ABSENT;
    const char *current = ruby_current(&visit->base);
    switch (visit->self) {
    case RUBY_SELF_UNKNOWN:
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                  "ruby_block_can_run_with_another_self");
        return false;
    case RUBY_SELF_MAIN:
    case RUBY_SELF_TOP_DEF:
        current = RUBY_OBJECT;
        found = ruby_own(&visit->base, RUBY_OBJECT, name, &decl);
        break;
    case RUBY_SELF_INSTANCE:
        found = ruby_find_instance_from(&visit->base, current, name, &decl);
        break;
    case RUBY_SELF_CLASS:
        if (!strcmp(name, "new"))
            return ruby_visit_constructor(visit, node, current);
        found = ruby_find_singleton(&visit->base, current, name, &decl);
        break;
    }
    return ruby_emit_lookup(visit, node, found, decl, current);
}

/* The graph has a node for a local only at the top of a file. */
static const char *ruby_file_local_qn(ruby_visit_t *visit, const ruby_local_t *local) {
    if (visit->scope || visit->base.depth || visit->self != RUBY_SELF_MAIN || local->blocked)
        return NULL;
    return cbm_source_def_qn_label(visit->base.walk, local->name, local->first_line, "Variable");
}

static void ruby_emit_local(ruby_visit_t *visit, TSNode node, const ruby_local_t *local) {
    const char *qn = local ? ruby_file_local_qn(visit, local) : NULL;
    cbm_source_emit_node(visit->base.walk, node, CBM_SOURCE_SITE_REFERENCE, qn,
                         qn ? CBM_RESOLVE_RULE_SCOPE_BINDING : CBM_RESOLVE_RULE_NONE, NULL,
                         qn ? "ruby_file_level_local" : "ruby_local_variable_has_no_node");
}

/* Ruby's parser reads a bare identifier as a local only when an assignment to it
 * comes first in the scope, or it is a parameter. Otherwise it is a method call on
 * self with no arguments, whatever a later assignment does. */
static void ruby_visit_bare_identifier(ruby_visit_t *visit, TSNode node) {
    cbm_source_walk_t *walk = visit->base.walk;
    const char *name = cbm_source_text(walk, node);
    if (!name)
        return;
    uint32_t at = ts_node_start_byte(node);
    for (const ruby_local_t *local = visit->locals; local; local = local->next)
        if (!strcmp(local->name, name) && (local->blocked || at >= local->bound_from)) {
            ruby_emit_local(visit, node, local);
            return;
        }
    cbm_source_emit_node(walk, node, CBM_SOURCE_SITE_REFERENCE, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                         "ruby_bare_name_is_a_call");
    ruby_extra_begin(visit, node, name);
    (void)ruby_visit_implicit_self(visit, node, name);
    ruby_extra_end(visit);
}

/* A mention of a constant, whichever way it is then used. Each prefix of `A::B::C` is
 * read on the way, so each is a mention of its own. */
static void ruby_visit_constant(ruby_visit_t *visit, TSNode node, unsigned depth) {
    if (cbm_source_is(node, "scope_resolution")) {
        TSNode scope = cbm_source_field(node, "scope");
        if (!ts_node_is_null(scope)) {
            ruby_visit(visit, scope, depth + 1);
            /* `thing::Name` looks in a value this walk has no type for. */
            if (!cbm_source_is(scope, "constant") && !cbm_source_is(scope, "scope_resolution"))
                return;
        }
    }
    const cbm_source_decl_t *records = ruby_constant_records(visit, node);
    const char *reason = NULL;
    const char *qn = records ? cbm_source_unique_qn(records, &reason) : NULL;
    if (!qn)
        return;
    ruby_extra_begin(visit, node, cbm_source_text(visit->base.walk, node));
    cbm_source_emit_node(visit->base.walk, node, CBM_SOURCE_SITE_REFERENCE, qn,
                         CBM_RESOLVE_RULE_LANGUAGE_BINDING, records->owner, "ruby_constant_lookup");
    ruby_extra_end(visit);
}

static bool ruby_visit_constant_call(ruby_visit_t *visit, TSNode node, const char *owner,
                                     const char *method) {
    if (!strcmp(method, "new"))
        return ruby_visit_constructor(visit, node, owner);
    const cbm_source_decl_t *decl = NULL;
    ruby_lookup_t found = ruby_find_singleton(&visit->base, owner, method, &decl);
    return ruby_emit_lookup(visit, node, found, decl, owner);
}

/* The class `Const.new` makes, when that is what its `new` does. */
static const char *ruby_constructed_class(ruby_visit_t *visit, TSNode value) {
    if (!cbm_source_is(value, "call"))
        return NULL;
    const char *method = cbm_source_text(visit->base.walk, cbm_source_field(value, "method"));
    if (!method || strcmp(method, "new"))
        return NULL;
    const char *owner = ruby_constant_owner(visit, cbm_source_field(value, "receiver"));
    if (!owner)
        return NULL;
    /* A class can define its own `new`, which returns anything. */
    const cbm_source_decl_t *custom = NULL;
    ruby_lookup_t found = ruby_find_singleton(&visit->base, owner, "new", &custom);
    return found == RUBY_FOUND || found == RUBY_AMBIGUOUS ? NULL : owner;
}

static const char *ruby_local_class(ruby_visit_t *visit, const char *name, uint32_t at) {
    ruby_local_t *local = NULL;
    for (ruby_local_t *candidate = visit->locals; candidate; candidate = candidate->next)
        if (!strcmp(candidate->name, name))
            local = candidate;
    /* One assignment of `Const.new` is the only value the local can hold. */
    if (!local || local->blocked || local->writes != 1 || at < local->available_from)
        return NULL;
    return ruby_constructed_class(visit, local->value);
}

/* The exact class of a receiver's value, when one can be read off it. */
static const char *ruby_value_class(ruby_visit_t *visit, TSNode receiver, uint32_t at) {
    if (cbm_source_is(receiver, "identifier")) {
        const char *local = cbm_source_text(visit->base.walk, receiver);
        return local ? ruby_local_class(visit, local, at) : NULL;
    }
    return ruby_constructed_class(visit, receiver);
}

/* `self.class` from an instance. */
static bool ruby_receiver_is_own_class(ruby_visit_t *visit, TSNode receiver) {
    if (visit->self != RUBY_SELF_INSTANCE || !cbm_source_is(receiver, "call") ||
        !cbm_source_is(cbm_source_field(receiver, "receiver"), "self"))
        return false;
    const char *method = cbm_source_text(visit->base.walk, cbm_source_field(receiver, "method"));
    return method && !strcmp(method, "class");
}

/* One method name on one receiver. True when a project declaration was selected. */
static bool ruby_dispatch(ruby_visit_t *visit, TSNode node, TSNode receiver, const char *name) {
    if (ts_node_is_null(receiver) || cbm_source_is(receiver, "self"))
        return ruby_visit_implicit_self(visit, node, name);
    const char *owner = ruby_constant_owner(visit, receiver);
    if (owner)
        return ruby_visit_constant_call(visit, node, owner, name);
    if (cbm_source_is(receiver, "constant") || cbm_source_is(receiver, "scope_resolution")) {
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ruby_constant_not_in_project");
        return false;
    }
    if (ruby_receiver_is_own_class(visit, receiver))
        return ruby_visit_constant_call(visit, node, ruby_current(&visit->base), name);
    const char *cls = ruby_value_class(visit, receiver, ts_node_start_byte(node));
    if (cls) {
        const cbm_source_decl_t *decl = NULL;
        ruby_lookup_t found = ruby_find_instance_from(&visit->base, cls, name, &decl);
        if (found == RUBY_FOUND) {
            ruby_emit(visit, node, decl->qn, CBM_RESOLVE_RULE_TYPE_MEMBER, cls,
                      "ruby_receiver_class_from_new");
            visit->selected = decl;
            return true;
        }
        return ruby_emit_lookup(visit, node, found, decl, cls);
    }
    ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ruby_receiver_class_unknown");
    return false;
}

static bool ruby_visit_call(ruby_visit_t *visit, TSNode node) {
    TSNode method = cbm_source_field(node, "method");
    const char *name = cbm_source_is(method, "identifier") || cbm_source_is(method, "constant") ||
                               cbm_source_is(method, "operator")
                           ? cbm_source_text(visit->base.walk, method)
                           : NULL;
    if (!name) {
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ruby_dynamic_method_name");
        return false;
    }
    TSNode receiver = cbm_source_field(node, "receiver");
    /* Extraction keeps no call for a bare `new(...)`, which is self.new. */
    bool bare_new = ts_node_is_null(receiver) && !strcmp(name, "new");
    if (bare_new)
        ruby_extra_begin(visit, node, name);
    bool selected = ruby_dispatch(visit, node, receiver, name);
    if (bare_new)
        ruby_extra_end(visit);
    return selected;
}

/* `a[i]` calls [] and `a[i] = b` calls []=. Neither has a carrier. */
static void ruby_visit_index_operator(ruby_visit_t *visit, TSNode node, const char *method) {
    ruby_extra_begin(visit, node, method);
    (void)ruby_dispatch(visit, node, cbm_source_field(node, "object"), method);
    ruby_extra_end(visit);
}

static void ruby_visit_index_writer(ruby_visit_t *visit, TSNode left, TSNode site) {
    ruby_extra_begin(visit, site, "[]=");
    (void)ruby_dispatch(visit, site, cbm_source_field(left, "object"), "[]=");
    ruby_extra_end(visit);
}

/* `a.b = c` calls b=. The carrier extraction records for `a.b` is that call, so the
 * reader is not selected. `a.b ||= c` reads first, so the carrier is the reader and the
 * writer is a second occurrence, on the method's name. */
static void ruby_visit_attribute_target(ruby_visit_t *visit, TSNode left, bool reads,
                                        unsigned depth) {
    cbm_source_walk_t *walk = visit->base.walk;
    TSNode receiver = cbm_source_field(left, "receiver");
    TSNode method = cbm_source_field(left, "method");
    const char *name = cbm_source_is(method, "identifier") ? cbm_source_text(walk, method) : NULL;
    const char *writer = name ? cbm_arena_sprintf(walk->arena, "%s=", name) : NULL;
    if (!writer) {
        ruby_emit(visit, left, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ruby_dynamic_method_name");
    } else if (reads) {
        (void)ruby_dispatch(visit, left, receiver, name);
        ruby_extra_begin(visit, method, writer);
        (void)ruby_dispatch(visit, method, receiver, writer);
        ruby_extra_end(visit);
    } else {
        (void)ruby_dispatch(visit, left, receiver, writer);
    }
    ruby_visit(visit, receiver, depth + 1);
}

/* Follows forwarded blocks through the project's defs. A cycle adds nothing: the walk
 * reads each name once, so its cost is the forwarding defs reachable from this name. */
static bool ruby_name_may_rebind(const ruby_ctx_t *ctx, const char *name,
                                 cbm_source_chain_t *chain) {
    const cbm_source_decl_t *notes = cbm_source_lookup(ctx->walk, RUBY_REBINDING_DEFS, name);
    if (!notes)
        return false;
    if (!cbm_source_chain_enter(ctx->walk, chain, name))
        return chain->overflow;
    bool rebinds = false;
    for (const cbm_source_decl_t *note = notes; note && !rebinds; note = note->next)
        rebinds = !note->type || !cbm_source_lookup(ctx->walk, RUBY_ALL_DEFS, note->type) ||
                  ruby_name_may_rebind(ctx, note->type, chain);
    cbm_source_chain_leave(chain);
    return rebinds;
}

static bool ruby_method_may_rebind(const ruby_visit_t *visit, const char *name) {
    cbm_source_chain_t chain = {0};
    return ruby_name_may_rebind(&visit->base, name, &chain);
}

/* The same question about one def: what it forwards to is what its own chain selects.
 * Each def is read once, so the walk costs the defs reachable by forwarding. */
static bool ruby_decl_may_rebind(const ruby_ctx_t *ctx, const cbm_source_decl_t *decl,
                                 cbm_source_chain_t *chain) {
    char own[RUBY_CONSTANT_BUFFER];
    int length = snprintf(own, sizeof(own), "%s%s", RUBY_REBINDING_OF, decl->owner);
    if (length < 0 || (size_t)length >= sizeof(own))
        return true;
    const cbm_source_decl_t *notes = cbm_source_lookup(ctx->walk, own, decl->name);
    if (!notes)
        return false;
    if (!cbm_source_chain_enter(ctx->walk, chain, decl->qn ? decl->qn : decl->name))
        return chain->overflow;
    bool singleton = !strncmp(decl->name, "self.", 5);
    bool rebinds = false;
    for (const cbm_source_decl_t *note = notes; note && !rebinds; note = note->next) {
        const cbm_source_decl_t *next = NULL;
        ruby_lookup_t found =
            !note->type ? RUBY_OPEN
            : singleton ? ruby_find_singleton(ctx, decl->owner, note->type, &next)
                        : ruby_find_instance_from(ctx, decl->owner, note->type, &next);
        rebinds = found != RUBY_FOUND || ruby_decl_may_rebind(ctx, next, chain);
    }
    cbm_source_chain_leave(chain);
    return rebinds;
}

/* What self is inside the block handed to this call. */
static ruby_self_t ruby_block_self(ruby_visit_t *visit, TSNode call, const char *method,
                                   const cbm_source_decl_t *selected) {
    ruby_self_t self = visit->self;
    if (self == RUBY_SELF_UNKNOWN || !method)
        return RUBY_SELF_UNKNOWN;
    cbm_source_walk_t *walk = visit->base.walk;
    TSNode receiver = cbm_source_field(call, "receiver");
    bool own_receiver = ts_node_is_null(receiver) || cbm_source_is(receiver, "self");
    if (self == RUBY_SELF_CLASS && own_receiver && !strcmp(method, "define_method"))
        return RUBY_SELF_INSTANCE;
    if (self == RUBY_SELF_CLASS && own_receiver && !strcmp(method, "define_singleton_method"))
        return RUBY_SELF_CLASS;
    cbm_source_chain_t chain = {0};
    if (!strcmp(method, "new")) {
        const cbm_source_decl_t *records = ruby_constant_records(visit, receiver);
        if (records)
            return selected && !ruby_decl_may_rebind(&visit->base, selected, &chain)
                       ? self
                       : RUBY_SELF_UNKNOWN;
        const char *core =
            cbm_source_is(receiver, "constant") ? cbm_source_text(walk, receiver) : NULL;
        return core && ruby_core_class_keeps_block_self(core) ? self : RUBY_SELF_UNKNOWN;
    }
    if (ruby_rebinds_self(method))
        return RUBY_SELF_UNKNOWN;
    /* A proc made here is handed over like a lambda literal is. */
    if ((self == RUBY_SELF_CLASS || self == RUBY_SELF_MAIN) &&
        (!strcmp(method, "lambda") || !strcmp(method, "proc")))
        return RUBY_SELF_UNKNOWN;
    if (selected)
        return ruby_decl_may_rebind(&visit->base, selected, &chain) ? RUBY_SELF_UNKNOWN : self;
    return ruby_core_block_method(method) && !ruby_method_may_rebind(visit, method)
               ? self
               : RUBY_SELF_UNKNOWN;
}

static void ruby_visit_children(ruby_visit_t *visit, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                ruby_visit(visit, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void ruby_visit_def(ruby_visit_t *visit, TSNode node, ruby_self_t self, unsigned depth) {
    const char *saved_def = visit->def_name;
    bool saved_singleton = visit->def_singleton;
    /* A def inside a block has no known owner, so nothing follows it either. */
    visit->def_name = self == RUBY_SELF_UNKNOWN
                          ? NULL
                          : cbm_source_text(visit->base.walk, cbm_source_field(node, "name"));
    visit->def_singleton = self == RUBY_SELF_CLASS;
    ruby_self_t saved_self = visit->self;
    ruby_local_t *saved_locals = visit->locals;
    const char *saved_scope = visit->scope;
    TSNode def_name = cbm_source_field(node, "name");
    const char *def_text = cbm_source_text(visit->base.walk, def_name);
    visit->scope = def_text ? cbm_source_def_qn(visit->base.walk, def_text,
                                                ts_node_start_point(def_name).row + 1)
                            : NULL;
    visit->self = self;
    visit->locals = NULL;
    TSNode parameters = cbm_source_field(node, "parameters");
    TSNode body = cbm_source_field(node, "body");
    if (!ts_node_is_null(parameters)) {
        ruby_block_locals(visit, parameters, depth + 1);
        ruby_visit(visit, parameters, depth + 1);
    }
    if (!ts_node_is_null(body)) {
        ruby_audit_locals(visit, body, depth + 1);
        ruby_visit(visit, body, depth + 1);
    }
    visit->self = saved_self;
    visit->locals = saved_locals;
    visit->scope = saved_scope;
    visit->def_name = saved_def;
    visit->def_singleton = saved_singleton;
}

/* `super` selects by the def it stands in, whatever self is by then. */
static void ruby_visit_super(ruby_visit_t *visit, TSNode node) {
    if (!visit->def_name || !visit->base.depth)
        return;
    const char *owner = ruby_current(&visit->base);
    const cbm_source_decl_t *decl = NULL;
    ruby_lookup_t found =
        ruby_find_super(&visit->base, owner, visit->def_name, visit->def_singleton, &decl);
    ruby_extra_begin(visit, node, visit->def_name);
    (void)ruby_emit_lookup(visit, node, found, decl, owner);
    ruby_extra_end(visit);
}

static void ruby_visit(ruby_visit_t *visit, TSNode node, unsigned depth) {
    cbm_source_walk_t *walk = visit->base.walk;
    if (ts_node_is_null(node) || !cbm_source_step(walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        ruby_visit_bare_identifier(visit, node);
        return;
    }
    if (cbm_source_is(node, "super")) {
        ruby_visit_super(visit, node);
        return;
    }
    /* These hold names being bound or spelled, not values being read. */
    const char *syntax = ts_node_type(node);
    if (!strcmp(syntax, "left_assignment_list") || !strcmp(syntax, "exception_variable") ||
        !strcmp(syntax, "alias") || !strcmp(syntax, "undef") || strstr(syntax, "pattern") ||
        (!strcmp(syntax, "unary") && cbm_source_token(node, "defined?")))
        return;
    /* A parameter's name is bound; its default value runs. */
    if (!strcmp(syntax, "block_parameters") || !strcmp(syntax, "lambda_parameters") ||
        !strcmp(syntax, "method_parameters")) {
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (cbm_source_is(child, "optional_parameter") ||
                    cbm_source_is(child, "keyword_parameter"))
                    ruby_visit(visit, cbm_source_field(child, "value"), depth + 1);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
        return;
    }
    if (!strcmp(syntax, "assignment") || !strcmp(syntax, "operator_assignment")) {
        TSNode left = cbm_source_field(node, "left");
        if (cbm_source_is(left, "identifier")) {
            const char *name = cbm_source_text(walk, left);
            const ruby_local_t *found = NULL;
            for (const ruby_local_t *local = visit->locals; name && local; local = local->next)
                if (!strcmp(local->name, name))
                    found = local;
            ruby_emit_local(visit, left, found);
        } else if (cbm_source_is(left, "call")) {
            ruby_visit_attribute_target(visit, left, !strcmp(syntax, "operator_assignment"), depth);
        } else if (cbm_source_is(left, "element_reference")) {
            if (!strcmp(syntax, "operator_assignment")) {
                /* It reads and then writes, and one span holds one site: the writer goes on
                 * the bracket, as an attribute's goes on its name. */
                ruby_visit_index_operator(visit, left, "[]");
                TSNode bracket = ts_node_next_sibling(cbm_source_field(left, "object"));
                if (!ts_node_is_null(bracket))
                    ruby_visit_index_writer(visit, left, bracket);
            } else {
                ruby_visit_index_writer(visit, left, left);
            }
            ruby_visit_children(visit, left, depth);
        } else if (!cbm_source_is(left, "constant")) {
            ruby_visit(visit, left, depth + 1);
        }
        ruby_visit(visit, cbm_source_field(node, "right"), depth + 1);
        return;
    }
    if (!strcmp(syntax, "constant") || !strcmp(syntax, "scope_resolution")) {
        ruby_visit_constant(visit, node, depth);
        return;
    }
    if (!strcmp(syntax, "element_reference"))
        ruby_visit_index_operator(visit, node, "[]");
    if (!strcmp(syntax, "for")) {
        ruby_visit(visit, cbm_source_field(node, "value"), depth + 1);
        ruby_visit(visit, cbm_source_field(node, "body"), depth + 1);
        return;
    }
    /* A lambda in a class body or at the top of a file is handed to something. */
    if (!strcmp(syntax, "lambda") &&
        (visit->self == RUBY_SELF_CLASS || visit->self == RUBY_SELF_MAIN)) {
        ruby_self_t saved = visit->self;
        visit->self = RUBY_SELF_UNKNOWN;
        ruby_visit_children(visit, node, depth);
        visit->self = saved;
        return;
    }
    if (cbm_source_is(node, "class") || cbm_source_is(node, "module")) {
        const char *owner = ruby_declared_owner(&visit->base, cbm_source_field(node, "name"));
        TSNode body = cbm_source_field(node, "body");
        if (!owner || ts_node_is_null(body) || !ruby_push(&visit->base, owner))
            return;
        ruby_self_t saved = visit->self;
        bool saved_singleton = visit->base.singleton;
        ruby_local_t *saved_locals = visit->locals;
        const char *saved_scope = visit->scope;
        visit->self = RUBY_SELF_CLASS;
        visit->base.singleton = false;
        visit->scope = NULL;
        /* A class body is a new scope for locals. */
        visit->locals = NULL;
        ruby_audit_locals(visit, body, depth + 1);
        ruby_visit_children(visit, body, depth);
        visit->self = saved;
        visit->base.singleton = saved_singleton;
        visit->locals = saved_locals;
        visit->scope = saved_scope;
        visit->base.depth--;
        return;
    }
    if (cbm_source_is(node, "singleton_class")) {
        TSNode body = cbm_source_field(node, "body");
        bool literal = cbm_source_is(cbm_source_field(node, "value"), "self");
        if (ts_node_is_null(body))
            return;
        bool saved = visit->base.singleton;
        ruby_self_t saved_self = visit->self;
        visit->base.singleton = literal;
        visit->self = RUBY_SELF_UNKNOWN; /* self here is the singleton class itself. */
        ruby_visit_children(visit, body, depth);
        visit->base.singleton = saved;
        visit->self = saved_self;
        return;
    }
    if (cbm_source_is(node, "method")) {
        ruby_self_t self = !visit->base.depth      ? RUBY_SELF_TOP_DEF
                           : visit->base.singleton ? RUBY_SELF_CLASS
                                                   : RUBY_SELF_INSTANCE;
        if (visit->self == RUBY_SELF_UNKNOWN && !visit->base.singleton)
            self = RUBY_SELF_UNKNOWN; /* A def inside a block has no known owner. */
        ruby_visit_def(visit, node, self, depth);
        return;
    }
    if (cbm_source_is(node, "singleton_method")) {
        bool own = cbm_source_is(cbm_source_field(node, "object"), "self") && visit->base.depth &&
                   visit->self != RUBY_SELF_UNKNOWN;
        ruby_visit_def(visit, node, own ? RUBY_SELF_CLASS : RUBY_SELF_UNKNOWN, depth);
        return;
    }
    if (cbm_source_is(node, "call")) {
        visit->selected = NULL;
        (void)ruby_visit_call(visit, node);
        const cbm_source_decl_t *selected = visit->selected;
        TSNode block = cbm_source_field(node, "block");
        const char *method = cbm_source_text(walk, cbm_source_field(node, "method"));
        ruby_self_t saved = visit->self;
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (!ts_node_is_named(child))
                    continue;
                if (ts_node_eq(child, cbm_source_field(node, "method"))) {
                    if (cbm_source_is(child, "super"))
                        ruby_visit_super(visit, child);
                    continue;
                }
                if (!ts_node_is_null(block) && ts_node_eq(child, block))
                    visit->self = ruby_block_self(visit, node, method, selected);
                ruby_visit(visit, child, depth + 1);
                visit->self = saved;
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
        return;
    }
    ruby_visit_children(visit, node, depth);
}

static void ruby_resolve(cbm_source_walk_t *walk) {
    ruby_visit_t visit = {.base = {.walk = walk}, .self = RUBY_SELF_MAIN};
    ruby_audit_locals(&visit, walk->root, 0);
    ruby_visit_children(&visit, walk->root, 0);
}

/* `class A < B`: the constant in the header, read where the header is written. Only the
 * superclass, which is what the graph's INHERITS has always meant for Ruby. */
static const char *ruby_table_base(const cbm_source_table_t *table, const cbm_source_decl_t *decl,
                                   size_t offset) {
    if (offset || !(decl->flags & CBM_SOURCE_DECL_F_TYPE))
        return NULL;
    cbm_source_walk_t walk = {.table = table, .language = CBM_LANG_RUBY};
    ruby_ctx_t ctx = {.walk = &walk};
    for (const cbm_source_decl_t *record = cbm_source_lookup(&walk, decl->owner, ""); record;
         record = record->next) {
        for (size_t i = 0; record->bases && record->bases[i]; i++) {
            if (record->bases[i][0] != 'S')
                continue;
            const char *parent = ruby_base_owner(&ctx, record->bases[i]);
            const char *reason = NULL;
            return parent ? cbm_source_unique_qn(cbm_source_lookup(&walk, parent, ""), &reason)
                          : NULL;
        }
    }
    return NULL;
}

const cbm_source_adapter_t cbm_source_adapter_ruby = {.language = CBM_LANG_RUBY,
                                                      .collect = ruby_collect,
                                                      .resolve = ruby_resolve,
                                                      .base = ruby_table_base,
                                                      .decides_references = true,
                                                      .contains_parse_damage = true};
