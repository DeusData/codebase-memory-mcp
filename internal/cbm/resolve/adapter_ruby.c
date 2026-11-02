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
 * then extended modules, then the superclass's. `Const.m` searches Const's
 * singleton chain the same way and `Const.new` selects `initialize`.
 *
 * Reopening is the language's normal mode. So all files declaring a method of the
 * same class count, and two declarations of one method are no single target.
 * `define_method`, `alias_method`, `attr_*`, `undef_method` and `remove_method`
 * with a literal name replace that name, which then abstains. A block can run
 * with another self through instance_eval and friends, so anything in those, and
 * every block written directly in a class body or at the top of a file (the shape
 * of a DSL), abstains. A constant resolves lexically, innermost first, then at the
 * top level; constants inherited from an ancestor are a known limit. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>

enum { RUBY_MAX_NEST = 32, RUBY_MAX_BASES = 64 };

#define RUBY_OBJECT "Object"

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
    bool singleton; /* inside class << self */
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

/* ── Phase one ─────────────────────────────────────────────────────── */

static void ruby_declare_key(ruby_ctx_t *ctx, const char *name, uint32_t flags, const char *qn,
                             uint32_t line) {
    const char *owner = ctx->depth ? ruby_current(ctx) : RUBY_OBJECT;
    const char *key = ctx->singleton ? cbm_arena_sprintf(ctx->walk->arena, "self.%s", name) : name;
    if (key)
        (void)cbm_source_declare(ctx->walk, owner, key, qn, flags, line);
}

static void ruby_note_body_call(ruby_ctx_t *ctx, TSNode call) {
    const char *name = ruby_call_name(ctx->walk, call);
    if (!name)
        return;
    TSNode arguments = cbm_source_field(call, "arguments");
    uint32_t line = ts_node_start_point(call).row + 1;
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    bool attribute = ruby_attribute_macro(name);
    bool rewrite = ruby_rewrites_method(name);
    if (!attribute && !rewrite && strcmp(name, "alias_method") != 0)
        return;
    bool only_first = !strcmp(name, "alias_method");
    for (uint32_t i = 0; i < count && (!only_first || i == 0); i++) {
        const char *literal = ruby_literal_symbol(ctx->walk, ts_node_named_child(arguments, i));
        if (!literal)
            continue;
        ruby_declare_key(ctx, literal, CBM_SOURCE_DECL_F_OPEN, NULL, line);
        if (attribute && (!strcmp(name, "attr_accessor") || !strcmp(name, "attr_writer"))) {
            const char *writer = cbm_arena_sprintf(ctx->walk->arena, "%s=", literal);
            if (writer)
                ruby_declare_key(ctx, writer, CBM_SOURCE_DECL_F_OPEN, NULL, line);
        }
    }
}

/* Each mixin keeps the lexical path that its constant is read under. */
static void ruby_gather_mixin(ruby_ctx_t *ctx, TSNode call, const char **bases, size_t *used) {
    const char *name = ruby_call_name(ctx->walk, call);
    char tag = 0;
    if (name && !strcmp(name, "include"))
        tag = 'I';
    else if (name && !strcmp(name, "prepend"))
        tag = 'P';
    else if (name && !strcmp(name, "extend"))
        tag = 'E';
    if (!tag)
        return;
    TSNode arguments = cbm_source_field(call, "arguments");
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < count && *used < RUBY_MAX_BASES; i++) {
        TSNode argument = ts_node_named_child(arguments, i);
        const char *text = NULL;
        if (cbm_source_is(argument, "constant") || cbm_source_is(argument, "scope_resolution") ||
            cbm_source_is(argument, "self"))
            text = cbm_source_text(ctx->walk, argument);
        /* A computed mixin is an unknown ancestor; the '?' keeps that visible. */
        bases[(*used)++] = cbm_arena_sprintf(ctx->walk->arena, "%c:%s|%s", tag, ruby_current(ctx),
                                             text ? text : "?");
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
    cbm_source_decl_t *record = cbm_source_declare(ctx->walk, owner, "", NULL, flags, line);
    const char *bases[RUBY_MAX_BASES];
    size_t used = 0;
    TSNode superclass = cbm_source_field(node, "superclass");
    if (!ts_node_is_null(superclass) && ts_node_named_child_count(superclass)) {
        TSNode parent = ts_node_named_child(superclass, 0);
        const char *text =
            cbm_source_is(parent, "constant") || cbm_source_is(parent, "scope_resolution")
                ? cbm_source_text(ctx->walk, parent)
                : NULL;
        /* Read in the enclosing nest: the class is not in scope for its own header. */
        bases[used++] =
            cbm_arena_sprintf(ctx->walk->arena, "S:%s|%s", ruby_current(ctx), text ? text : "?");
    }
    if (!ruby_push(ctx, owner)) {
        cbm_source_fail(ctx->walk, "ruby_nesting_limit");
        return;
    }
    bool saved = ctx->singleton;
    ctx->singleton = false;
    TSNode body = cbm_source_field(node, "body");
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(body, i), "call"))
            ruby_gather_mixin(ctx, ts_node_named_child(body, i), bases, &used);
    if (record) {
        const char **copy = cbm_source_table_alloc(ctx->walk, (used + 1) * sizeof(*copy));
        if (copy) {
            for (size_t i = 0; i < used; i++)
                copy[i] = cbm_source_table_intern(ctx->walk, bases[i]);
            copy[used] = NULL;
            record->bases = copy;
        }
    }
    if (!ts_node_is_null(body))
        ruby_collect_body(ctx, body, conditional, depth + 1);
    ctx->singleton = saved;
    ctx->depth--;
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
                if (name)
                    ruby_declare_key(ctx, name, CBM_SOURCE_DECL_F_OPEN, NULL,
                                     ts_node_start_point(child).row + 1);
            } else if (cbm_source_is(child, "call")) {
                ruby_note_body_call(ctx, child);
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

static void ruby_collect(cbm_source_walk_t *walk) {
    ruby_ctx_t ctx = {.walk = walk};
    ruby_collect_body(&ctx, walk->root, false, 0);
}

/* ── Phase two: constants and ancestor chains ──────────────────────── */

static const char *ruby_resolve_constant_in(const ruby_ctx_t *ctx, const char *cref,
                                            const char *text) {
    if (!text || !text[0] || !strcmp(text, "?"))
        return NULL;
    if (text[0] == ':' && text[1] == ':')
        return cbm_source_lookup(ctx->walk, text + 2, "") ? text + 2 : NULL;
    const char *colons = strstr(text, "::");
    size_t first = colons ? (size_t)(colons - text) : strlen(text);
    char prefix[512];
    if (cref && strlen(cref) < sizeof(prefix)) {
        strcpy(prefix, cref);
        while (prefix[0]) {
            char candidate[1024];
            int length =
                snprintf(candidate, sizeof(candidate), "%s::%.*s", prefix, (int)first, text);
            if (length > 0 && (size_t)length < sizeof(candidate) &&
                cbm_source_lookup(ctx->walk, candidate, "")) {
                /* The first segment decides; the rest must exist under it. */
                const char *full = cbm_arena_sprintf(ctx->walk->arena, "%s::%s", prefix, text);
                return full && cbm_source_lookup(ctx->walk, full, "") ? full : NULL;
            }
            char *cut = strrchr(prefix, ':');
            if (!cut || cut == prefix)
                break;
            cut[-1] = '\0';
        }
    }
    char head[512];
    if (first < sizeof(head)) {
        memcpy(head, text, first);
        head[first] = '\0';
        if (cbm_source_lookup(ctx->walk, head, ""))
            return cbm_source_lookup(ctx->walk, text, "") ? text : NULL;
    }
    return NULL;
}

/* Reads a stored "tag:cref|text" base. */
static const char *ruby_base_owner(const ruby_ctx_t *ctx, const char *base) {
    const char *bar = strchr(base, '|');
    if (!bar || !base[1])
        return NULL;
    char cref[512];
    size_t length = (size_t)(bar - (base + 2));
    if (length >= sizeof(cref))
        return NULL;
    memcpy(cref, base + 2, length);
    cref[length] = '\0';
    return ruby_resolve_constant_in(ctx, cref, bar + 1);
}

typedef enum { RUBY_FOUND, RUBY_ABSENT, RUBY_OPEN, RUBY_AMBIGUOUS } ruby_lookup_t;

static ruby_lookup_t ruby_own(const ruby_ctx_t *ctx, const char *owner, const char *key,
                              const cbm_source_decl_t **out) {
    const cbm_source_decl_t *first = cbm_source_lookup(ctx->walk, owner, key);
    if (!first)
        return RUBY_ABSENT;
    const char *reason = NULL;
    if (first->next || !cbm_source_unique_qn(first, &reason))
        return RUBY_AMBIGUOUS;
    for (const cbm_source_decl_t *decl = first; decl; decl = decl->next)
        if (decl->flags & (CBM_SOURCE_DECL_F_CONDITIONAL | CBM_SOURCE_DECL_F_OPEN))
            return RUBY_AMBIGUOUS;
    *out = first;
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

static ruby_lookup_t ruby_find_instance(const ruby_ctx_t *ctx, const char *owner, const char *name,
                                        cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (!cbm_source_chain_first_visit(chain, owner))
        return chain->overflow ? RUBY_OPEN : RUBY_ABSENT;
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
    if (records->flags & CBM_SOURCE_DECL_F_INTERFACE)
        return RUBY_ABSENT; /* A module has no Object step of its own. */
    return ruby_own(ctx, RUBY_OBJECT, name, out);
}

static ruby_lookup_t ruby_find_singleton_on(const ruby_ctx_t *ctx, const char *owner,
                                            const char *name, cbm_source_chain_t *chain,
                                            cbm_source_chain_t *extended,
                                            const cbm_source_decl_t **out) {
    if (!cbm_source_chain_first_visit(chain, owner))
        return chain->overflow ? RUBY_OPEN : RUBY_ABSENT;
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

static ruby_lookup_t ruby_find_singleton(const ruby_ctx_t *ctx, const char *owner, const char *name,
                                         const cbm_source_decl_t **out) {
    cbm_source_chain_t chain = {0}, extended = {0};
    return ruby_find_singleton_on(ctx, owner, name, &chain, &extended, out);
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
    int writes;
    bool blocked;
    struct ruby_local *next;
} ruby_local_t;

typedef struct {
    ruby_ctx_t base;
    ruby_self_t self;
    ruby_local_t *locals;
    const char *scope; /* the enclosing def's QN; NULL in a class body or unknown */
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

static void ruby_block_locals(ruby_visit_t *visit, TSNode node, unsigned depth) {
    if (!cbm_source_step(visit->base.walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        const char *name = cbm_source_text(visit->base.walk, node);
        ruby_local_t *local = name ? ruby_local(visit, name) : NULL;
        if (local)
            local->blocked = true;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        ruby_block_locals(visit, ts_node_named_child(node, i), depth + 1);
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
                if (ts_node_start_byte(left) < local->bound_from)
                    local->bound_from = ts_node_start_byte(left);
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
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        ruby_audit_locals(visit, ts_node_named_child(node, i), depth + 1);
}

static const char *ruby_constant_owner(ruby_visit_t *visit, TSNode node) {
    if (!cbm_source_is(node, "constant") && !cbm_source_is(node, "scope_resolution"))
        return NULL;
    const char *text = cbm_source_text(visit->base.walk, node);
    /* A qualified constant may not have a project prefix at all. */
    return ruby_resolve_constant_in(&visit->base, ruby_current(&visit->base), text);
}

static void ruby_emit(ruby_visit_t *visit, TSNode node, const char *target, cbm_resolve_rule_t rule,
                      const char *domain, const char *reason) {
    cbm_source_emit_node(visit->base.walk, node, CBM_SOURCE_SITE_CALL, target, rule, domain,
                         reason);
}

static void ruby_emit_lookup(ruby_visit_t *visit, TSNode node, ruby_lookup_t found,
                             const cbm_source_decl_t *decl, const char *domain) {
    switch (found) {
    case RUBY_FOUND: {
        cbm_resolve_rule_t rule = decl->owner && domain && !strcmp(decl->owner, domain)
                                      ? CBM_RESOLVE_RULE_SCOPE_BINDING
                                      : CBM_RESOLVE_RULE_LANGUAGE_BINDING;
        ruby_emit(visit, node, decl->qn, rule, domain, "ruby_ancestor_chain_declaration");
        break;
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
}

static void ruby_visit_implicit_self(ruby_visit_t *visit, TSNode node, const char *name) {
    const cbm_source_decl_t *decl = NULL;
    ruby_lookup_t found = RUBY_ABSENT;
    const char *current = ruby_current(&visit->base);
    switch (visit->self) {
    case RUBY_SELF_UNKNOWN:
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                  "ruby_block_can_run_with_another_self");
        return;
    case RUBY_SELF_MAIN:
    case RUBY_SELF_TOP_DEF:
        current = RUBY_OBJECT;
        found = ruby_own(&visit->base, RUBY_OBJECT, name, &decl);
        break;
    case RUBY_SELF_INSTANCE:
        found = ruby_find_instance_from(&visit->base, current, name, &decl);
        break;
    case RUBY_SELF_CLASS:
        found = ruby_find_singleton(&visit->base, current, name, &decl);
        break;
    }
    ruby_emit_lookup(visit, node, found, decl, current);
}

/* Ruby's parser reads a bare identifier as a local only when an assignment to it
 * comes first in the scope, or it is a parameter. Otherwise it is a method call on
 * self with no arguments, whatever a later assignment does. */
static void ruby_visit_bare_identifier(ruby_visit_t *visit, TSNode node) {
    cbm_source_walk_t *walk = visit->base.walk;
    const char *name = cbm_source_text(walk, node);
    if (!name || (visit->self != RUBY_SELF_MAIN && !visit->scope))
        return;
    uint32_t at = ts_node_start_byte(node);
    for (const ruby_local_t *local = visit->locals; local; local = local->next)
        if (!strcmp(local->name, name) && (local->blocked || at >= local->bound_from))
            return;
    walk->extra_scope = visit->scope;
    walk->extra_name = name;
    ruby_visit_implicit_self(visit, node, name);
    walk->extra_scope = NULL;
    walk->extra_name = NULL;
}

static void ruby_visit_constant_call(ruby_visit_t *visit, TSNode node, const char *owner,
                                     const char *method) {
    const cbm_source_decl_t *decl = NULL;
    ruby_lookup_t found = ruby_find_singleton(&visit->base, owner, method, &decl);
    if (found == RUBY_ABSENT && !strcmp(method, "new")) {
        /* Class#new allocates and calls initialize; initialize is how the
         * declaration is reached. */
        found = ruby_find_instance_from(&visit->base, owner, "initialize", &decl);
    }
    ruby_emit_lookup(visit, node, found, decl, owner);
}

static const char *ruby_local_class(ruby_visit_t *visit, const char *name, uint32_t at) {
    ruby_local_t *local = NULL;
    for (ruby_local_t *candidate = visit->locals; candidate; candidate = candidate->next)
        if (!strcmp(candidate->name, name))
            local = candidate;
    /* One assignment of `Const.new` is the only value the local can hold. */
    if (!local || local->blocked || local->writes != 1 || at < local->available_from)
        return NULL;
    TSNode value = local->value;
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
    return ruby_find_singleton(&visit->base, owner, "new", &custom) == RUBY_ABSENT ? owner : NULL;
}

static void ruby_visit_call(ruby_visit_t *visit, TSNode node) {
    cbm_source_walk_t *walk = visit->base.walk;
    TSNode method = cbm_source_field(node, "method");
    TSNode receiver = cbm_source_field(node, "receiver");
    const char *name = cbm_source_is(method, "identifier") || cbm_source_is(method, "constant") ||
                               cbm_source_is(method, "operator")
                           ? cbm_source_text(walk, method)
                           : NULL;
    if (!name) {
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ruby_dynamic_method_name");
        return;
    }
    if (ts_node_is_null(receiver) || cbm_source_is(receiver, "self")) {
        ruby_visit_implicit_self(visit, node, name);
        return;
    }
    const char *owner = ruby_constant_owner(visit, receiver);
    if (owner) {
        ruby_visit_constant_call(visit, node, owner, name);
        return;
    }
    if (cbm_source_is(receiver, "constant") || cbm_source_is(receiver, "scope_resolution")) {
        ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ruby_constant_not_in_project");
        return;
    }
    if (cbm_source_is(receiver, "identifier")) {
        const char *local = cbm_source_text(walk, receiver);
        const char *cls = local ? ruby_local_class(visit, local, ts_node_start_byte(node)) : NULL;
        if (cls) {
            const cbm_source_decl_t *decl = NULL;
            ruby_lookup_t found = ruby_find_instance_from(&visit->base, cls, name, &decl);
            if (found == RUBY_FOUND) {
                ruby_emit(visit, node, decl->qn, CBM_RESOLVE_RULE_TYPE_MEMBER, cls,
                          "ruby_receiver_assigned_once_from_new");
                return;
            }
            ruby_emit_lookup(visit, node, found, decl, cls);
            return;
        }
    }
    ruby_emit(visit, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ruby_receiver_class_unknown");
}

static void ruby_visit(ruby_visit_t *visit, TSNode node, unsigned depth);

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
    if (!ts_node_is_null(parameters))
        ruby_block_locals(visit, parameters, depth + 1);
    if (!ts_node_is_null(body)) {
        ruby_audit_locals(visit, body, depth + 1);
        ruby_visit(visit, body, depth + 1);
    }
    visit->self = saved_self;
    visit->locals = saved_locals;
    visit->scope = saved_scope;
}

static void ruby_visit(ruby_visit_t *visit, TSNode node, unsigned depth) {
    cbm_source_walk_t *walk = visit->base.walk;
    if (ts_node_is_null(node) || !cbm_source_step(walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        ruby_visit_bare_identifier(visit, node);
        return;
    }
    /* These hold names being bound or spelled, not values being read. */
    const char *syntax = ts_node_type(node);
    if (!strcmp(syntax, "block_parameters") || !strcmp(syntax, "lambda_parameters") ||
        !strcmp(syntax, "method_parameters") || !strcmp(syntax, "left_assignment_list") ||
        !strcmp(syntax, "exception_variable") || !strcmp(syntax, "alias") ||
        !strcmp(syntax, "undef") || strstr(syntax, "pattern") ||
        (!strcmp(syntax, "unary") && cbm_source_token(node, "defined?")))
        return;
    if (!strcmp(syntax, "assignment") || !strcmp(syntax, "operator_assignment")) {
        TSNode left = cbm_source_field(node, "left");
        if (!cbm_source_is(left, "identifier"))
            ruby_visit(visit, left, depth + 1);
        ruby_visit(visit, cbm_source_field(node, "right"), depth + 1);
        return;
    }
    if (!strcmp(syntax, "scope_resolution")) {
        ruby_visit(visit, cbm_source_field(node, "scope"), depth + 1);
        return;
    }
    if (!strcmp(syntax, "for")) {
        ruby_visit(visit, cbm_source_field(node, "value"), depth + 1);
        ruby_visit(visit, cbm_source_field(node, "body"), depth + 1);
        return;
    }
    if (cbm_source_is(node, "class") || cbm_source_is(node, "module")) {
        const char *owner = ruby_declared_owner(&visit->base, cbm_source_field(node, "name"));
        TSNode body = cbm_source_field(node, "body");
        if (!owner || ts_node_is_null(body) || !ruby_push(&visit->base, owner))
            return;
        ruby_self_t saved = visit->self;
        bool saved_singleton = visit->base.singleton;
        visit->self = RUBY_SELF_CLASS;
        visit->base.singleton = false;
        ruby_visit_children(visit, body, depth);
        visit->self = saved;
        visit->base.singleton = saved_singleton;
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
        ruby_visit_call(visit, node);
        TSNode block = cbm_source_field(node, "block");
        const char *method = cbm_source_text(walk, cbm_source_field(node, "method"));
        ruby_self_t saved = visit->self;
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (!ts_node_is_named(child) ||
                    (ts_node_eq(child, cbm_source_field(node, "method"))))
                    continue;
                if (!ts_node_is_null(block) && ts_node_eq(child, block) &&
                    (visit->self == RUBY_SELF_CLASS || visit->self == RUBY_SELF_MAIN ||
                     (method && ruby_rebinds_self(method))))
                    visit->self = RUBY_SELF_UNKNOWN;
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

const cbm_source_adapter_t cbm_source_adapter_ruby = {
    .language = CBM_LANG_RUBY, .collect = ruby_collect, .resolve = ruby_resolve};
