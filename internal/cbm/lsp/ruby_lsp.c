/*
 * ruby_lsp.c — Ruby Light Semantic Pass.
 *
 * In-process type-aware call resolver for Ruby. Mirrors the perl_lsp.c /
 * php_lsp.c shape:
 *   1. Build a CBMTypeRegistry from file-local definitions + stdlib
 *      (Ruby core classes + curated Rails surface), with per-class method
 *      tables keyed by receiver QN. Singleton (class-side) methods are
 *      keyed "<class_qn>.self" — Ruby constants cannot be lowercase, so
 *      the suffix never collides with a real nested-constant path.
 *   2. ruby_lsp_process_file does a multi-pass walk:
 *        PASS 1 — collect class/module declarations (with lexical nesting),
 *          superclasses, include/prepend/extend mixins, and method tables
 *          (instance vs singleton classified from the AST — `def m` vs
 *          `def self.m` / `class << self`).
 *        PASS 1.5 — infer instance-variable types from `@x = Const.new`
 *          assignments inside method bodies (conflicting types latch to
 *          "conflicted" and resolve to nothing).
 *        PASS 2 — walk method bodies, track local variable types through
 *          assignments, and resolve call expressions into CBMResolvedCall
 *          edges via Ruby method lookup (prepends → own → includes →
 *          superclass chain; extends feed the singleton side).
 *
 * Verified tree-sitter-ruby node/field names (vendored compiled grammar at
 * internal/cbm/vendored/grammars/ruby/parser.c — ts_symbol_names and
 * ts_field_names tables):
 *   - call : fields `receiver`, `method`, `arguments`, `block`. Both
 *     `foo(x)` and command calls (`foo x`) surface as "call" nodes.
 *   - class : fields `name` (constant | scope_resolution), `superclass`
 *     (a `superclass` wrapper node), `body` (body_statement).
 *   - module : fields `name`, `body`.
 *   - method : fields `name`, `parameters`, `body`.
 *   - singleton_method : fields `object` (self | constant), `name`, `body`.
 *   - singleton_class : `class << self` — fields `value`, body children.
 *   - assignment : fields `left`, `right`.
 *   - scope_resolution : fields `scope` (may be absent for ::Foo), `name`.
 *   - leaves: constant, identifier, self, instance_variable, super,
 *     string, array, hash, integer, float, simple_symbol, regex.
 *
 * QN scheme (matches the structural extractor): Ruby HAS class node types
 * (class/module), so defs are named `module_qn.<ConstPath>.<method>` with
 * parent_class = `module_qn.<ConstPath>`. Constructor calls: the textual
 * extractor rewrites `Widget.new` to callee "Widget" (extract_calls.c), so
 * constructor rows emit the CLASS QN — the edge lands on the Class node,
 * exactly like Python's lsp_constructor.
 *
 * Zero-edge guarantee: if a receiver's type is unknown/unindexed, NO edge
 * is emitted (false edges are worse than missing edges). Dynamic dispatch
 * (`send`, `public_send`, `method_missing`, `define_method`) is
 * intentionally ignored.
 */

#include "ruby_lsp.h"
#include "../helpers.h"
#include "../arena.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Recursion cap for ruby_eval_expr_type — mirrors perl/php (unknown past 8). */
#define RUBY_EVAL_MAX_DEPTH 8

/* Confidence levels. */
#define RUBY_CONF_HIGH 0.95f  /* constructor / static constant dispatch */
#define RUBY_CONF_TYPED 0.90f /* inferred receiver type (locals, ivars, chains) */

/* Maximum AST-walk recursion depth (see scope.h rationale — graceful
 * degradation on pathologically nested sources, never a crash). */
#define CBM_LSP_RUBY_MAX_WALK_DEPTH 512

/* ── forward declarations ───────────────────────────────────────── */

static void ruby_resolve_calls_in_node(RubyLSPContext *ctx, TSNode node);
static void ruby_resolve_calls_in_node_inner(RubyLSPContext *ctx, TSNode node);
static void ruby_pass1_scan(RubyLSPContext *ctx, TSNode node);
static void ruby_pass1_scan_inner(RubyLSPContext *ctx, TSNode node);
static void ruby_ivar_scan(RubyLSPContext *ctx, TSNode node);
static void ruby_ivar_scan_inner(RubyLSPContext *ctx, TSNode node);
static const CBMType *ruby_eval_call_type(RubyLSPContext *ctx, TSNode call, bool emit);

/* ── helpers ────────────────────────────────────────────────────── */

static char *ruby_node_text(RubyLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

/* Convert a constant expression node (constant | scope_resolution) into a
 * dotted path ("Foo", "A.B.C"). Returns NULL for anything else. A leading
 * "::Foo" (scope_resolution without a scope) yields "Foo" — the resolver's
 * table probe covers the top-level meaning. */
static const char *ruby_const_path(RubyLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    const char *k = ts_node_type(node);
    if (strcmp(k, "constant") == 0)
        return ruby_node_text(ctx, node);
    if (strcmp(k, "scope_resolution") != 0)
        return NULL;
    TSNode scope = ts_node_child_by_field_name(node, "scope", 5);
    TSNode name = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name))
        return NULL;
    char *nm = ruby_node_text(ctx, name);
    if (!nm || !nm[0])
        return NULL;
    if (ts_node_is_null(scope))
        return nm; /* ::Foo */
    const char *sp = ruby_const_path(ctx, scope);
    if (!sp || !sp[0])
        return nm;
    return cbm_arena_sprintf(ctx->arena, "%s.%s", sp, nm);
}

/* Join two dotted path fragments; either side may be empty/NULL. */
static const char *ruby_path_join(CBMArena *a, const char *left, const char *right) {
    if (!left || !left[0])
        return right;
    if (!right || !right[0])
        return left;
    return cbm_arena_sprintf(a, "%s.%s", left, right);
}

/* True when a QN belongs to the indexed project (module_qn's first segment
 * is its prefix). Stdlib seed QNs ("String.upcase", "ActiveRecord.Base")
 * never carry the project prefix, so edges are only ever emitted at
 * project-defined targets. */
static bool ruby_is_project_qn(RubyLSPContext *ctx, const char *qn) {
    if (!qn || !ctx->module_qn || !ctx->module_qn[0])
        return false;
    const char *dot = strchr(ctx->module_qn, '.');
    size_t plen = dot ? (size_t)(dot - ctx->module_qn) : strlen(ctx->module_qn);
    return strncmp(qn, ctx->module_qn, plen) == 0 && qn[plen] == '.';
}

/* ── table growth helpers ───────────────────────────────────────── */

static RubyClassInfo *ruby_add_class(RubyLSPContext *ctx, const char *path, const char *qn,
                                     bool is_module) {
    if (!ctx || !path || !qn)
        return NULL;
    /* Reopened class/module: reuse the existing record. */
    for (int i = 0; i < ctx->class_count; i++) {
        if (strcmp(ctx->classes[i].path, path) == 0)
            return &ctx->classes[i];
    }
    if (ctx->class_count >= ctx->class_cap) {
        int newcap = ctx->class_cap ? ctx->class_cap * 2 : 8;
        RubyClassInfo *nc =
            (RubyClassInfo *)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(RubyClassInfo));
        if (!nc)
            return NULL;
        for (int i = 0; i < ctx->class_count; i++)
            nc[i] = ctx->classes[i];
        ctx->classes = nc;
        ctx->class_cap = newcap;
    }
    RubyClassInfo *ci = &ctx->classes[ctx->class_count++];
    memset(ci, 0, sizeof(*ci));
    ci->path = cbm_arena_strdup(ctx->arena, path);
    ci->qn = cbm_arena_strdup(ctx->arena, qn);
    ci->is_module = is_module;
    return ci;
}

static void ruby_add_mixin(RubyLSPContext *ctx, const char *owner_qn, const char *module_ref,
                           RubyMixinKind kind) {
    if (!ctx || !owner_qn || !module_ref)
        return;
    if (ctx->mixin_count >= ctx->mixin_cap) {
        int newcap = ctx->mixin_cap ? ctx->mixin_cap * 2 : 8;
        RubyMixinInfo *nm =
            (RubyMixinInfo *)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(RubyMixinInfo));
        if (!nm)
            return;
        for (int i = 0; i < ctx->mixin_count; i++)
            nm[i] = ctx->mixins[i];
        ctx->mixins = nm;
        ctx->mixin_cap = newcap;
    }
    RubyMixinInfo *mi = &ctx->mixins[ctx->mixin_count++];
    memset(mi, 0, sizeof(*mi));
    mi->owner_qn = cbm_arena_strdup(ctx->arena, owner_qn);
    mi->module_ref = cbm_arena_strdup(ctx->arena, module_ref);
    mi->kind = kind;
}

static void ruby_add_ivar(RubyLSPContext *ctx, const char *class_qn, const char *ivar_name,
                          const char *type_qn) {
    if (!ctx || !class_qn || !ivar_name || !type_qn)
        return;
    for (int i = 0; i < ctx->ivar_count; i++) {
        RubyIvarInfo *iv = &ctx->ivars[i];
        if (strcmp(iv->class_qn, class_qn) == 0 && strcmp(iv->ivar_name, ivar_name) == 0) {
            if (strcmp(iv->type_qn, type_qn) != 0)
                iv->conflicted = true; /* disagreeing assignments — no type */
            return;
        }
    }
    if (ctx->ivar_count >= ctx->ivar_cap) {
        int newcap = ctx->ivar_cap ? ctx->ivar_cap * 2 : 8;
        RubyIvarInfo *ni =
            (RubyIvarInfo *)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(RubyIvarInfo));
        if (!ni)
            return;
        for (int i = 0; i < ctx->ivar_count; i++)
            ni[i] = ctx->ivars[i];
        ctx->ivars = ni;
        ctx->ivar_cap = newcap;
    }
    RubyIvarInfo *iv = &ctx->ivars[ctx->ivar_count++];
    memset(iv, 0, sizeof(*iv));
    iv->class_qn = cbm_arena_strdup(ctx->arena, class_qn);
    iv->ivar_name = cbm_arena_strdup(ctx->arena, ivar_name);
    iv->type_qn = cbm_arena_strdup(ctx->arena, type_qn);
}

static const char *ruby_ivar_type(RubyLSPContext *ctx, const char *class_qn,
                                  const char *ivar_name) {
    if (!ctx || !class_qn || !ivar_name)
        return NULL;
    for (int i = 0; i < ctx->ivar_count; i++) {
        RubyIvarInfo *iv = &ctx->ivars[i];
        if (!iv->conflicted && strcmp(iv->class_qn, class_qn) == 0 &&
            strcmp(iv->ivar_name, ivar_name) == 0)
            return iv->type_qn;
    }
    return NULL;
}

static RubyClassInfo *ruby_class_by_qn(RubyLSPContext *ctx, const char *qn) {
    if (!qn)
        return NULL;
    for (int i = 0; i < ctx->class_count; i++) {
        if (strcmp(ctx->classes[i].qn, qn) == 0)
            return &ctx->classes[i];
    }
    return NULL;
}

/* Conventional Rails superclasses for stdlib-seeded types the class table
 * does not carry (per-file extraction cannot see app/models/
 * application_record.rb from another file; the convention is fixed). */
static const char *ruby_stdlib_superclass(const char *qn) {
    if (!qn)
        return NULL;
    if (strcmp(qn, "ApplicationRecord") == 0)
        return "ActiveRecord.Base";
    if (strcmp(qn, "ApplicationController") == 0)
        return "ActionController.Base";
    if (strcmp(qn, "ApplicationJob") == 0)
        return "ActiveJob.Base";
    if (strcmp(qn, "ApplicationMailer") == 0)
        return "ActionMailer.Base";
    return NULL;
}

/* Superclass QN of a class: table entry first, then the conventional
 * stdlib chain above. */
static const char *ruby_superclass_qn(RubyLSPContext *ctx, const char *class_qn) {
    RubyClassInfo *ci = ruby_class_by_qn(ctx, class_qn);
    if (ci)
        return ci->superclass_qn;
    return ruby_stdlib_superclass(class_qn);
}

/* ── constant resolution ────────────────────────────────────────── */

const char *ruby_resolve_constant(RubyLSPContext *ctx, const char *path) {
    if (!ctx || !path || !path[0])
        return NULL;
    /* Lexical nesting probe: for nesting "A.B" try "A.B.path", "A.path",
     * then "path" (innermost first — Ruby's constant lookup order). */
    const char *nest = ctx->nesting ? ctx->nesting : "";
    char prefix[512];
    size_t nlen = strlen(nest);
    if (nlen < sizeof(prefix)) {
        memcpy(prefix, nest, nlen + 1);
        while (1) {
            if (prefix[0]) {
                const char *cand = cbm_arena_sprintf(ctx->arena, "%s.%s", prefix, path);
                for (int i = 0; i < ctx->class_count; i++) {
                    if (strcmp(ctx->classes[i].path, cand) == 0)
                        return ctx->classes[i].qn;
                }
                char *last_dot = strrchr(prefix, '.');
                if (last_dot)
                    *last_dot = '\0';
                else
                    prefix[0] = '\0';
                continue;
            }
            break;
        }
    }
    for (int i = 0; i < ctx->class_count; i++) {
        if (strcmp(ctx->classes[i].path, path) == 0)
            return ctx->classes[i].qn;
    }
    /* Stdlib types are keyed by their dotted bare path. */
    const CBMRegisteredType *t = cbm_registry_lookup_type(ctx->registry, path);
    if (t)
        return t->qualified_name;
    return NULL;
}

/* ── method lookup (Ruby MRO approximation) ─────────────────────── */

/* Depth cap shared by the instance/singleton lookup walks. */
#define RUBY_LOOKUP_MAX_DEPTH CBM_LSP_MAX_LOOKUP_DEPTH

/* Own instance method of one exact class (no ancestry walk). */
static const CBMRegisteredFunc *ruby_own_instance_method(RubyLSPContext *ctx, const char *class_qn,
                                                         const char *method_name) {
    return cbm_registry_lookup_method(ctx->registry, class_qn, method_name);
}

/* Own singleton method of one exact class ("<qn>.self" receiver key). */
static const CBMRegisteredFunc *ruby_own_singleton_method(RubyLSPContext *ctx, const char *class_qn,
                                                          const char *method_name) {
    const char *key = cbm_arena_sprintf(ctx->arena, "%s.self", class_qn);
    return cbm_registry_lookup_method(ctx->registry, key, method_name);
}

static const CBMRegisteredFunc *ruby_lookup_instance_method_depth(RubyLSPContext *ctx,
                                                                  const char *class_qn,
                                                                  const char *method_name,
                                                                  int depth);

/* Search a mixin kind on `owner_qn` (most recent first — later mixins win),
 * recursing into each module's own include chain. */
static const CBMRegisteredFunc *ruby_lookup_in_mixins(RubyLSPContext *ctx, const char *owner_qn,
                                                      RubyMixinKind kind, const char *method_name,
                                                      int depth) {
    for (int i = ctx->mixin_count - 1; i >= 0; i--) {
        RubyMixinInfo *mi = &ctx->mixins[i];
        if (mi->kind != kind || strcmp(mi->owner_qn, owner_qn) != 0 || !mi->module_qn)
            continue;
        const CBMRegisteredFunc *f =
            ruby_lookup_instance_method_depth(ctx, mi->module_qn, method_name, depth + 1);
        if (f)
            return f;
    }
    return NULL;
}

static const CBMRegisteredFunc *ruby_lookup_instance_method_depth(RubyLSPContext *ctx,
                                                                  const char *class_qn,
                                                                  const char *method_name,
                                                                  int depth) {
    if (!ctx || !class_qn || !method_name || depth > RUBY_LOOKUP_MAX_DEPTH)
        return NULL;
    /* Prepended modules shadow the class's own methods. */
    const CBMRegisteredFunc *f =
        ruby_lookup_in_mixins(ctx, class_qn, RUBY_MIXIN_PREPEND, method_name, depth);
    if (f)
        return f;
    f = ruby_own_instance_method(ctx, class_qn, method_name);
    if (f)
        return f;
    f = ruby_lookup_in_mixins(ctx, class_qn, RUBY_MIXIN_INCLUDE, method_name, depth);
    if (f)
        return f;
    const char *sup = ruby_superclass_qn(ctx, class_qn);
    if (sup && strcmp(sup, class_qn) != 0)
        return ruby_lookup_instance_method_depth(ctx, sup, method_name, depth + 1);
    return NULL;
}

const CBMRegisteredFunc *ruby_lookup_instance_method(RubyLSPContext *ctx, const char *class_qn,
                                                     const char *method_name) {
    const CBMRegisteredFunc *f = ruby_lookup_instance_method_depth(ctx, class_qn, method_name, 0);
    if (f)
        return f;
    /* Universal receiver fallback (Object / ActiveSupport predicates) —
     * typing only; Object methods are stdlib entries, never project defs. */
    if (class_qn && strcmp(class_qn, "Object") != 0)
        return cbm_registry_lookup_method(ctx->registry, "Object", method_name);
    return NULL;
}

static const CBMRegisteredFunc *ruby_lookup_singleton_method_depth(RubyLSPContext *ctx,
                                                                   const char *class_qn,
                                                                   const char *method_name,
                                                                   int depth) {
    if (!ctx || !class_qn || !method_name || depth > RUBY_LOOKUP_MAX_DEPTH)
        return NULL;
    const CBMRegisteredFunc *f = ruby_own_singleton_method(ctx, class_qn, method_name);
    if (f)
        return f;
    /* `extend Mod` adds Mod's instance methods class-side. */
    f = ruby_lookup_in_mixins(ctx, class_qn, RUBY_MIXIN_EXTEND, method_name, depth);
    if (f)
        return f;
    const char *sup = ruby_superclass_qn(ctx, class_qn);
    if (sup && strcmp(sup, class_qn) != 0)
        return ruby_lookup_singleton_method_depth(ctx, sup, method_name, depth + 1);
    return NULL;
}

const CBMRegisteredFunc *ruby_lookup_singleton_method(RubyLSPContext *ctx, const char *class_qn,
                                                      const char *method_name) {
    return ruby_lookup_singleton_method_depth(ctx, class_qn, method_name, 0);
}

/* ── ActiveRecord model detection (typing special case) ─────────── */

/* True when class_qn's superclass chain reaches ActiveRecord::Base (or the
 * conventional ApplicationRecord intermediary, which per-file extraction
 * usually cannot see through). */
static bool ruby_is_ar_model(RubyLSPContext *ctx, const char *class_qn, int depth) {
    if (!class_qn || depth > RUBY_LOOKUP_MAX_DEPTH)
        return false;
    if (strcmp(class_qn, "ActiveRecord.Base") == 0 || strcmp(class_qn, "ApplicationRecord") == 0)
        return true;
    RubyClassInfo *ci = ruby_class_by_qn(ctx, class_qn);
    if (ci && ci->superclass_ref &&
        (strcmp(ci->superclass_ref, "ApplicationRecord") == 0 ||
         strcmp(ci->superclass_ref, "ActiveRecord.Base") == 0))
        return true;
    const char *sup = ruby_superclass_qn(ctx, class_qn);
    if (sup && strcmp(sup, class_qn) != 0)
        return ruby_is_ar_model(ctx, sup, depth + 1);
    return false;
}

/* ActiveRecord class-side query methods that return the model (or a
 * relation we approximate as the model, so chains like
 * `User.where(...).first` keep their type). */
static bool ruby_ar_returns_model(const char *m) {
    static const char *names[] = {"find",
                                  "find_by",
                                  "find_by!",
                                  "find_or_create_by",
                                  "find_or_initialize_by",
                                  "where",
                                  "all",
                                  "first",
                                  "last",
                                  "take",
                                  "create",
                                  "create!",
                                  "new",
                                  "order",
                                  "limit",
                                  "offset",
                                  "joins",
                                  "left_joins",
                                  "includes",
                                  "preload",
                                  "eager_load",
                                  "group",
                                  "having",
                                  "distinct",
                                  "none",
                                  "unscoped",
                                  "find_each",
                                  NULL};
    for (int i = 0; names[i]; i++) {
        if (strcmp(m, names[i]) == 0)
            return true;
    }
    return false;
}

/* ── emit ───────────────────────────────────────────────────────── */

static void ruby_emit_resolved(RubyLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence, TSNode site) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;
    /* Only project-defined targets become edges (stdlib rows would never
     * land on a node and merely add noise). */
    if (!ruby_is_project_qn(ctx, callee_qn))
        return;
    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    rc.reason = NULL;
    rc.kind = CBM_RESOLVED_INVOCATION;
    if (!ts_node_is_null(site)) {
        rc.site_start_byte = ts_node_start_byte(site);
        rc.site_end_byte = ts_node_end_byte(site);
    }
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

/* ── expression typing ──────────────────────────────────────────── */

/* Return type of a registered func's signature, or unknown. */
static const CBMType *ruby_func_return_type(const CBMRegisteredFunc *f) {
    if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC &&
        f->signature->data.func.return_types && f->signature->data.func.return_types[0]) {
        return f->signature->data.func.return_types[0];
    }
    return cbm_type_unknown();
}

const CBMType *ruby_eval_expr_type(RubyLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();
    if (ctx->eval_depth >= RUBY_EVAL_MAX_DEPTH)
        return cbm_type_unknown();
    ctx->eval_depth++;
    const CBMType *result = cbm_type_unknown();
    const char *k = ts_node_type(node);

    if (strcmp(k, "identifier") == 0) {
        char *txt = ruby_node_text(ctx, node);
        if (txt) {
            const CBMType *t = cbm_scope_lookup(ctx->current_scope, txt);
            if (t)
                result = t;
        }
    } else if (strcmp(k, "instance_variable") == 0) {
        char *txt = ruby_node_text(ctx, node);
        const char *tqn = ruby_ivar_type(ctx, ctx->enclosing_class_qn, txt);
        if (tqn)
            result = cbm_type_named(ctx->arena, tqn);
    } else if (strcmp(k, "self") == 0) {
        if (ctx->enclosing_class_qn && !ctx->in_singleton_method)
            result = cbm_type_named(ctx->arena, ctx->enclosing_class_qn);
    } else if (strcmp(k, "call") == 0) {
        result = ruby_eval_call_type(ctx, node, false);
    } else if (strcmp(k, "assignment") == 0) {
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(right))
            result = ruby_eval_expr_type(ctx, right);
    } else if (strcmp(k, "string") == 0 || strcmp(k, "string_content") == 0 ||
               strcmp(k, "heredoc_body") == 0 || strcmp(k, "chained_string") == 0) {
        result = cbm_type_named(ctx->arena, "String");
    } else if (strcmp(k, "array") == 0 || strcmp(k, "string_array") == 0 ||
               strcmp(k, "symbol_array") == 0) {
        result = cbm_type_named(ctx->arena, "Array");
    } else if (strcmp(k, "hash") == 0) {
        result = cbm_type_named(ctx->arena, "Hash");
    } else if (strcmp(k, "integer") == 0) {
        result = cbm_type_named(ctx->arena, "Integer");
    } else if (strcmp(k, "float") == 0) {
        result = cbm_type_named(ctx->arena, "Float");
    } else if (strcmp(k, "simple_symbol") == 0 || strcmp(k, "delimited_symbol") == 0) {
        result = cbm_type_named(ctx->arena, "Symbol");
    } else if (strcmp(k, "regex") == 0) {
        result = cbm_type_named(ctx->arena, "Regexp");
    } else if (strcmp(k, "parenthesized_statements") == 0 || strcmp(k, "begin") == 0) {
        /* Type of the last meaningful child. */
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = nc; i > 0; i--) {
            TSNode c = ts_node_named_child(node, i - 1);
            if (ts_node_is_null(c))
                continue;
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "comment") == 0)
                continue;
            result = ruby_eval_expr_type(ctx, c);
            break;
        }
    }

    ctx->eval_depth--;
    return result;
}

/* ── call resolution core ───────────────────────────────────────── */

/* Dynamic-dispatch method names the resolver must never guess through. */
static bool ruby_is_dynamic_dispatch(const char *m) {
    return strcmp(m, "send") == 0 || strcmp(m, "public_send") == 0 || strcmp(m, "__send__") == 0 ||
           strcmp(m, "method_missing") == 0 || strcmp(m, "define_method") == 0 ||
           strcmp(m, "instance_eval") == 0 || strcmp(m, "class_eval") == 0 ||
           strcmp(m, "module_eval") == 0 || strcmp(m, "instance_variable_get") == 0 ||
           strcmp(m, "instance_variable_set") == 0;
}

/* Class-body macro-ish names PASS 1 already interpreted (or that never
 * denote resolvable user calls). */
static bool ruby_is_body_macro(const char *m) {
    return strcmp(m, "include") == 0 || strcmp(m, "extend") == 0 || strcmp(m, "prepend") == 0 ||
           strcmp(m, "require") == 0 || strcmp(m, "require_relative") == 0 ||
           strcmp(m, "attr_accessor") == 0 || strcmp(m, "attr_reader") == 0 ||
           strcmp(m, "attr_writer") == 0 || strcmp(m, "private") == 0 || strcmp(m, "public") == 0 ||
           strcmp(m, "protected") == 0 || strcmp(m, "module_function") == 0 ||
           strcmp(m, "alias_method") == 0 || strcmp(m, "loop") == 0 || strcmp(m, "lambda") == 0 ||
           strcmp(m, "proc") == 0;
}

/* Resolve a singleton-side dispatch on a resolved class QN. Emits (when
 * `emit`) and returns the call's type. Also owns the ActiveRecord typing
 * special case (User.find -> User). */
static const CBMType *ruby_dispatch_singleton(RubyLSPContext *ctx, const char *class_qn,
                                              const char *mname, bool emit, TSNode site) {
    const CBMRegisteredFunc *f = ruby_lookup_singleton_method(ctx, class_qn, mname);
    if (f) {
        if (emit)
            ruby_emit_resolved(ctx, f->qualified_name, "ruby_class_method", RUBY_CONF_HIGH, site);
        const CBMType *rt = ruby_func_return_type(f);
        if (!cbm_type_is_unknown(rt))
            return rt;
        /* Stdlib AR query entries return unknown; substitute the model. */
        if (ruby_is_ar_model(ctx, class_qn, 0) && ruby_ar_returns_model(mname))
            return cbm_type_named(ctx->arena, class_qn);
        return rt;
    }
    if (ruby_is_ar_model(ctx, class_qn, 0) && ruby_ar_returns_model(mname))
        return cbm_type_named(ctx->arena, class_qn);
    return cbm_type_unknown();
}

/* Resolve an instance-side dispatch on a receiver type QN. */
static const CBMType *ruby_dispatch_instance(RubyLSPContext *ctx, const char *class_qn,
                                             const char *mname, const char *strategy, bool emit,
                                             TSNode site) {
    const CBMRegisteredFunc *f = ruby_lookup_instance_method(ctx, class_qn, mname);
    if (!f)
        return cbm_type_unknown();
    if (emit)
        ruby_emit_resolved(ctx, f->qualified_name, strategy, RUBY_CONF_TYPED, site);
    return ruby_func_return_type(f);
}

/* Resolve one `call` node. When `emit` is set, resolved project targets are
 * appended as CBMResolvedCall edges; the call's result type is returned
 * either way (so chains and assignments can type through it). */
static const CBMType *ruby_eval_call_type(RubyLSPContext *ctx, TSNode call, bool emit) {
    TSNode meth = ts_node_child_by_field_name(call, "method", 6);
    TSNode recv = ts_node_child_by_field_name(call, "receiver", 8);
    if (ts_node_is_null(meth))
        return cbm_type_unknown();

    /* `super(...)` is dispatched by the walker (ruby_resolve_super); as an
     * expression its type is not modeled. */
    if (strcmp(ts_node_type(meth), "super") == 0)
        return cbm_type_unknown();

    char *mname = ruby_node_text(ctx, meth);
    if (!mname || !mname[0])
        return cbm_type_unknown();
    if (ruby_is_dynamic_dispatch(mname))
        return cbm_type_unknown(); /* zero-edge guarantee */

    /* Constructor: Const.new / A::B.new — the textual extractor rewrites
     * the callee to the constant, so the row targets the CLASS QN. */
    if (strcmp(mname, "new") == 0 && !ts_node_is_null(recv)) {
        const char *cpath = ruby_const_path(ctx, recv);
        if (cpath) {
            const char *cqn = ruby_resolve_constant(ctx, cpath);
            if (cqn) {
                if (emit)
                    ruby_emit_resolved(ctx, cqn, "ruby_constructor", RUBY_CONF_HIGH, call);
                return cbm_type_named(ctx->arena, cqn);
            }
            return cbm_type_unknown();
        }
        /* Fall through: `expr.new` on a non-constant receiver. */
    }

    if (ts_node_is_null(recv)) {
        /* Bare call: self-dispatch inside methods, then file-level funcs. */
        if (ruby_is_body_macro(mname))
            return cbm_type_unknown();
        if (ctx->enclosing_class_qn) {
            const CBMRegisteredFunc *f =
                ctx->in_singleton_method
                    ? ruby_lookup_singleton_method(ctx, ctx->enclosing_class_qn, mname)
                    : ruby_lookup_instance_method_depth(ctx, ctx->enclosing_class_qn, mname, 0);
            if (f) {
                if (emit)
                    ruby_emit_resolved(ctx, f->qualified_name, "ruby_self_dispatch", RUBY_CONF_HIGH,
                                       call);
                return ruby_func_return_type(f);
            }
        }
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, mname);
        if (!f)
            f = cbm_registry_lookup_func(ctx->registry, mname); /* Kernel builtins */
        if (f) {
            if (emit)
                ruby_emit_resolved(ctx, f->qualified_name, "ruby_function_local", RUBY_CONF_HIGH,
                                   call);
            return ruby_func_return_type(f);
        }
        return cbm_type_unknown();
    }

    const char *rk = ts_node_type(recv);

    /* self.m — explicit self dispatch. */
    if (strcmp(rk, "self") == 0) {
        if (!ctx->enclosing_class_qn)
            return cbm_type_unknown();
        const CBMRegisteredFunc *f =
            ctx->in_singleton_method
                ? ruby_lookup_singleton_method(ctx, ctx->enclosing_class_qn, mname)
                : ruby_lookup_instance_method_depth(ctx, ctx->enclosing_class_qn, mname, 0);
        if (f) {
            if (emit)
                ruby_emit_resolved(ctx, f->qualified_name, "ruby_self_dispatch", RUBY_CONF_HIGH,
                                   call);
            return ruby_func_return_type(f);
        }
        return cbm_type_unknown();
    }

    /* Const.m / A::B.m — singleton dispatch on the resolved class. */
    if (strcmp(rk, "constant") == 0 || strcmp(rk, "scope_resolution") == 0) {
        const char *cpath = ruby_const_path(ctx, recv);
        const char *cqn = cpath ? ruby_resolve_constant(ctx, cpath) : NULL;
        if (!cqn)
            return cbm_type_unknown(); /* unknown constant — zero-edge */
        return ruby_dispatch_singleton(ctx, cqn, mname, emit, call);
    }

    /* Typed receiver: locals, ivars, chained calls, literals. */
    const CBMType *rt = ruby_eval_expr_type(ctx, recv);
    if (!rt || rt->kind != CBM_TYPE_NAMED)
        return cbm_type_unknown(); /* unknown receiver — zero-edge */
    const char *strategy = "ruby_method_typed";
    if (strcmp(rk, "instance_variable") == 0)
        strategy = "ruby_ivar_method";
    else if (strcmp(rk, "call") == 0)
        strategy = "ruby_method_chained";
    const CBMType *out =
        ruby_dispatch_instance(ctx, rt->data.named.qualified_name, mname, strategy, emit, call);
    if (!cbm_type_is_unknown(out))
        return out;
    /* AR relation approximation: chains on a model type keep the model. */
    if (ruby_is_ar_model(ctx, rt->data.named.qualified_name, 0) && ruby_ar_returns_model(mname))
        return rt;
    return out;
}

/* `super` / `super(...)`: resolve the enclosing method's name starting at
 * the enclosing class's ancestry (own def skipped — that IS the caller). */
static void ruby_resolve_super(RubyLSPContext *ctx, TSNode site) {
    if (!ctx->enclosing_class_qn || !ctx->enclosing_func_qn)
        return;
    const char *fq = ctx->enclosing_func_qn;
    const char *mname = strrchr(fq, '.');
    mname = mname ? mname + 1 : fq;
    const CBMRegisteredFunc *f = NULL;
    /* Search order mirrors Ruby: includes of the class, then superclass
     * chain — the class's OWN def is the caller and must be skipped. */
    f = ruby_lookup_in_mixins(ctx, ctx->enclosing_class_qn,
                              ctx->in_singleton_method ? RUBY_MIXIN_EXTEND : RUBY_MIXIN_INCLUDE,
                              mname, 0);
    if (!f) {
        const char *sup = ruby_superclass_qn(ctx, ctx->enclosing_class_qn);
        if (sup) {
            f = ctx->in_singleton_method ? ruby_lookup_singleton_method_depth(ctx, sup, mname, 0)
                                         : ruby_lookup_instance_method_depth(ctx, sup, mname, 0);
        }
    }
    if (f)
        ruby_emit_resolved(ctx, f->qualified_name, "ruby_method_super", RUBY_CONF_HIGH, site);
    /* No known parent or unresolved — zero-edge guarantee. */
}

/* ── assignment observer (scope binding) ────────────────────────── */

static void ruby_process_assignment(RubyLSPContext *ctx, TSNode assign) {
    TSNode left = ts_node_child_by_field_name(assign, "left", 4);
    TSNode right = ts_node_child_by_field_name(assign, "right", 5);
    if (ts_node_is_null(left) || ts_node_is_null(right))
        return;
    if (strcmp(ts_node_type(left), "identifier") != 0)
        return; /* only simple local targets are tracked */
    char *vname = ruby_node_text(ctx, left);
    if (!vname || !vname[0])
        return;
    const CBMType *rt = ruby_eval_expr_type(ctx, right);
    if (rt && rt->kind == CBM_TYPE_NAMED)
        cbm_scope_bind(ctx->current_scope, vname, rt);
}

/* ── PASS 2: resolution walk ────────────────────────────────────── */

/* Process one method/singleton_method definition node. */
static void ruby_process_method(RubyLSPContext *ctx, TSNode node, bool singleton) {
    CBMScope *saved_scope = ctx->current_scope;
    const char *saved_func = ctx->enclosing_func_qn;
    bool saved_singleton = ctx->in_singleton_method;

    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    ctx->in_singleton_method = singleton;

    TSNode name = ts_node_child_by_field_name(node, "name", 4);
    char *mname = ts_node_is_null(name) ? NULL : ruby_node_text(ctx, name);
    if (mname && mname[0]) {
        const char *owner = ctx->enclosing_class_qn ? ctx->enclosing_class_qn : ctx->module_qn;
        if (owner)
            ctx->enclosing_func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", owner, mname);
        else
            ctx->enclosing_func_qn = cbm_arena_strdup(ctx->arena, mname);
    }

    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (!ts_node_is_null(body))
        ruby_resolve_calls_in_node(ctx, body);

    ctx->current_scope = saved_scope;
    ctx->enclosing_func_qn = saved_func;
    ctx->in_singleton_method = saved_singleton;
}

/* Process a class/module node during PASS 2: update nesting + enclosing
 * class, then walk the body. */
static void ruby_process_class_body(RubyLSPContext *ctx, TSNode node) {
    TSNode name = ts_node_child_by_field_name(node, "name", 4);
    const char *cpath = ruby_const_path(ctx, name);
    if (!cpath)
        return;

    const char *saved_nesting = ctx->nesting;
    const char *saved_class = ctx->enclosing_class_qn;
    bool saved_singleton = ctx->in_singleton_method;

    ctx->nesting = ruby_path_join(ctx->arena, saved_nesting, cpath);
    const char *qn = ruby_resolve_constant(ctx, cpath);
    ctx->enclosing_class_qn = qn;
    ctx->in_singleton_method = false;

    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (!ts_node_is_null(body))
        ruby_resolve_calls_in_node(ctx, body);

    ctx->nesting = saved_nesting;
    ctx->enclosing_class_qn = saved_class;
    ctx->in_singleton_method = saved_singleton;
}

static void ruby_resolve_calls_in_node(RubyLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= CBM_LSP_RUBY_MAX_WALK_DEPTH)
        return;
    ctx->walk_depth++;
    ruby_resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}

static void ruby_resolve_calls_in_node_inner(RubyLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *k = ts_node_type(node);

    if (strcmp(k, "class") == 0 || strcmp(k, "module") == 0) {
        ruby_process_class_body(ctx, node);
        return;
    }
    if (strcmp(k, "singleton_class") == 0) {
        /* `class << self` — methods inside are singleton methods. */
        bool saved = ctx->in_singleton_method;
        ctx->in_singleton_method = true;
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++)
            ruby_resolve_calls_in_node(ctx, ts_node_named_child(node, i));
        ctx->in_singleton_method = saved;
        return;
    }
    if (strcmp(k, "method") == 0) {
        ruby_process_method(ctx, node, ctx->in_singleton_method);
        return;
    }
    if (strcmp(k, "singleton_method") == 0) {
        ruby_process_method(ctx, node, true);
        return;
    }

    if (strcmp(k, "assignment") == 0)
        ruby_process_assignment(ctx, node);

    if (strcmp(k, "call") == 0) {
        TSNode meth = ts_node_child_by_field_name(node, "method", 6);
        if (!ts_node_is_null(meth) && strcmp(ts_node_type(meth), "super") == 0)
            ruby_resolve_super(ctx, node);
        else
            ruby_eval_call_type(ctx, node, true);
        /* Recurse into receiver/arguments/blocks for nested calls. */
    } else if (strcmp(k, "super") == 0) {
        /* Bare `super` (no argument list) appears as a standalone node. */
        ruby_resolve_super(ctx, node);
        return;
    }

    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (!ts_node_is_null(c))
            ruby_resolve_calls_in_node(ctx, c);
    }
}

/* ── PASS 1: class/mixin/method collection ──────────────────────── */

/* Register one method into the registry with the proper receiver key. */
static void ruby_register_method(RubyLSPContext *ctx, CBMTypeRegistry *reg, const char *owner_qn,
                                 const char *mname, bool singleton) {
    if (!mname || !mname[0])
        return;
    CBMRegisteredFunc rf;
    memset(&rf, 0, sizeof(rf));
    rf.min_params = -1;
    const char *owner = owner_qn ? owner_qn : ctx->module_qn;
    rf.qualified_name = owner ? cbm_arena_sprintf(ctx->arena, "%s.%s", owner, mname)
                              : cbm_arena_strdup(ctx->arena, mname);
    rf.short_name = cbm_arena_strdup(ctx->arena, mname);
    if (owner_qn) {
        rf.receiver_type = singleton ? cbm_arena_sprintf(ctx->arena, "%s.self", owner_qn)
                                     : cbm_arena_strdup(ctx->arena, owner_qn);
    }
    const CBMType **rets = (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(*rets));
    if (rets) {
        rets[0] = cbm_type_unknown();
        rets[1] = NULL;
    }
    rf.signature = cbm_type_func(ctx->arena, NULL, NULL, rets);
    cbm_registry_add_func(reg, rf);
}

/* PASS-1 refs are nesting-scoped: the finalize step resolves superclass and
 * mixin references once the class table is complete, so PASS 1 records raw
 * dotted refs plus the nesting string active at the declaration site
 * (packed as "<nesting>|<ref>" when nesting is set). */
static const char *ruby_pack_ref(RubyLSPContext *ctx, const char *ref) {
    const char *nest = ctx->nesting ? ctx->nesting : "";
    if (!nest[0])
        return ref;
    return cbm_arena_sprintf(ctx->arena, "%s|%s", nest, ref);
}

/* Resolve a packed "<nesting>|<ref>" (or bare "<ref>") against the class
 * table + stdlib, honoring the recorded lexical nesting. */
static const char *ruby_resolve_packed_ref(RubyLSPContext *ctx, const char *packed) {
    if (!packed)
        return NULL;
    const char *bar = strchr(packed, '|');
    const char *saved_nesting = ctx->nesting;
    const char *ref = packed;
    if (bar) {
        ctx->nesting = cbm_arena_strndup(ctx->arena, packed, (size_t)(bar - packed));
        ref = bar + 1;
    } else {
        ctx->nesting = "";
    }
    const char *qn = ruby_resolve_constant(ctx, ref);
    ctx->nesting = saved_nesting;
    return qn;
}

/* Bare ref portion of a packed "<nesting>|<ref>" string. */
static const char *ruby_packed_bare_ref(const char *packed) {
    if (!packed)
        return NULL;
    const char *bar = strchr(packed, '|');
    return bar ? bar + 1 : packed;
}

/* Interpret a class-body `include X` / `prepend X` / `extend X` call,
 * recording the mixin with the nesting packed for finalize-time resolve. */
static void ruby_collect_mixin_call(RubyLSPContext *ctx, TSNode call) {
    if (!ctx->enclosing_class_qn)
        return;
    TSNode meth = ts_node_child_by_field_name(call, "method", 6);
    TSNode recv = ts_node_child_by_field_name(call, "receiver", 8);
    if (ts_node_is_null(meth))
        return;
    if (!ts_node_is_null(recv) && strcmp(ts_node_type(recv), "self") != 0)
        return; /* not a bare/self class-body macro */
    char *mname = ruby_node_text(ctx, meth);
    if (!mname)
        return;
    RubyMixinKind kind;
    if (strcmp(mname, "include") == 0)
        kind = RUBY_MIXIN_INCLUDE;
    else if (strcmp(mname, "prepend") == 0)
        kind = RUBY_MIXIN_PREPEND;
    else if (strcmp(mname, "extend") == 0)
        kind = RUBY_MIXIN_EXTEND;
    else
        return;
    TSNode args = ts_node_child_by_field_name(call, "arguments", 9);
    if (ts_node_is_null(args))
        return;
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t i = 0; i < nc; i++) {
        const char *mpath = ruby_const_path(ctx, ts_node_named_child(args, i));
        if (!mpath)
            continue; /* `extend self` etc. — not a constant */
        ruby_add_mixin(ctx, ctx->enclosing_class_qn, ruby_pack_ref(ctx, mpath), kind);
    }
}

static void ruby_pass1_class(RubyLSPContext *ctx, TSNode node, bool is_module) {
    TSNode name = ts_node_child_by_field_name(node, "name", 4);
    const char *cpath = ruby_const_path(ctx, name);
    if (!cpath)
        return;

    const char *saved_nesting = ctx->nesting;
    const char *full_path = ruby_path_join(ctx->arena, saved_nesting, cpath);
    const char *qn = ctx->module_qn
                         ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, full_path)
                         : full_path;
    RubyClassInfo *ci = ruby_add_class(ctx, full_path, qn, is_module);

    /* Superclass: `class C < Base` — the `superclass` field wraps the
     * expression after `<`. */
    if (ci && !is_module && !ci->superclass_ref) {
        TSNode sup = ts_node_child_by_field_name(node, "superclass", 10);
        if (!ts_node_is_null(sup)) {
            uint32_t sc = ts_node_named_child_count(sup);
            for (uint32_t i = 0; i < sc; i++) {
                const char *spath = ruby_const_path(ctx, ts_node_named_child(sup, i));
                if (spath) {
                    ci->superclass_ref = ruby_pack_ref(ctx, spath);
                    break;
                }
            }
        }
    }

    ctx->nesting = full_path;
    const char *saved_class = ctx->enclosing_class_qn;
    bool saved_singleton = ctx->in_singleton_method;
    ctx->enclosing_class_qn = qn;
    ctx->in_singleton_method = false;
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (!ts_node_is_null(body))
        ruby_pass1_scan(ctx, body);
    ctx->nesting = saved_nesting;
    ctx->enclosing_class_qn = saved_class;
    ctx->in_singleton_method = saved_singleton;
}

static void ruby_pass1_scan(RubyLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= CBM_LSP_RUBY_MAX_WALK_DEPTH)
        return;
    ctx->walk_depth++;
    ruby_pass1_scan_inner(ctx, node);
    ctx->walk_depth--;
}

/* Registry under construction during PASS 1 (methods are registered as
 * they are discovered). Held in a file-scope slot only for the duration of
 * cbm_run_ruby_lsp's pass-1 invocation — single-threaded per file. */
static void ruby_pass1_scan_inner(RubyLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *k = ts_node_type(node);

    if (strcmp(k, "class") == 0) {
        ruby_pass1_class(ctx, node, false);
        return;
    }
    if (strcmp(k, "module") == 0) {
        ruby_pass1_class(ctx, node, true);
        return;
    }
    if (strcmp(k, "singleton_class") == 0) {
        bool saved = ctx->in_singleton_method;
        ctx->in_singleton_method = true;
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++)
            ruby_pass1_scan(ctx, ts_node_named_child(node, i));
        ctx->in_singleton_method = saved;
        return;
    }
    if (strcmp(k, "method") == 0 || strcmp(k, "singleton_method") == 0) {
        bool singleton = ctx->in_singleton_method || strcmp(k, "singleton_method") == 0;
        TSNode name = ts_node_child_by_field_name(node, "name", 4);
        char *mname = ts_node_is_null(name) ? NULL : ruby_node_text(ctx, name);
        if (ctx->build_reg)
            ruby_register_method(ctx, ctx->build_reg, ctx->enclosing_class_qn, mname, singleton);
        return; /* method bodies are PASS-2 territory */
    }
    if (strcmp(k, "call") == 0) {
        TSNode meth = ts_node_child_by_field_name(node, "method", 6);
        if (!ts_node_is_null(meth)) {
            char *mn = ruby_node_text(ctx, meth);
            if (mn && (strcmp(mn, "include") == 0 || strcmp(mn, "prepend") == 0 ||
                       strcmp(mn, "extend") == 0)) {
                ruby_collect_mixin_call(ctx, node);
                return;
            }
        }
    }

    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (!ts_node_is_null(c))
            ruby_pass1_scan(ctx, c);
    }
}

/* Finalize PASS 1: resolve superclass + mixin references now that the class
 * table is complete. */
static void ruby_finalize_refs(RubyLSPContext *ctx) {
    for (int i = 0; i < ctx->class_count; i++) {
        RubyClassInfo *ci = &ctx->classes[i];
        if (ci->superclass_ref) {
            ci->superclass_qn = ruby_resolve_packed_ref(ctx, ci->superclass_ref);
            /* Keep the bare ref spelling for AR-model heuristics. */
            ci->superclass_ref = ruby_packed_bare_ref(ci->superclass_ref);
        }
    }
    for (int i = 0; i < ctx->mixin_count; i++) {
        RubyMixinInfo *mi = &ctx->mixins[i];
        mi->module_qn = ruby_resolve_packed_ref(ctx, mi->module_ref);
        mi->module_ref = ruby_packed_bare_ref(mi->module_ref);
    }
}

/* ── PASS 1.5: instance-variable type inference ─────────────────── */

/* Statically evaluate `Const.new` / `A::B.new` without scope context. */
static const char *ruby_static_ctor_type(RubyLSPContext *ctx, TSNode expr) {
    if (ts_node_is_null(expr) || strcmp(ts_node_type(expr), "call") != 0)
        return NULL;
    TSNode meth = ts_node_child_by_field_name(expr, "method", 6);
    TSNode recv = ts_node_child_by_field_name(expr, "receiver", 8);
    if (ts_node_is_null(meth) || ts_node_is_null(recv))
        return NULL;
    char *mname = ruby_node_text(ctx, meth);
    if (!mname || strcmp(mname, "new") != 0)
        return NULL;
    const char *cpath = ruby_const_path(ctx, recv);
    if (!cpath)
        return NULL;
    return ruby_resolve_constant(ctx, cpath);
}

static void ruby_ivar_scan(RubyLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= CBM_LSP_RUBY_MAX_WALK_DEPTH)
        return;
    ctx->walk_depth++;
    ruby_ivar_scan_inner(ctx, node);
    ctx->walk_depth--;
}

static void ruby_ivar_scan_inner(RubyLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *k = ts_node_type(node);

    if (strcmp(k, "class") == 0 || strcmp(k, "module") == 0) {
        TSNode name = ts_node_child_by_field_name(node, "name", 4);
        const char *cpath = ruby_const_path(ctx, name);
        if (!cpath)
            return;
        const char *saved_nesting = ctx->nesting;
        const char *saved_class = ctx->enclosing_class_qn;
        ctx->nesting = ruby_path_join(ctx->arena, saved_nesting, cpath);
        ctx->enclosing_class_qn = ruby_resolve_constant(ctx, cpath);
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        if (!ts_node_is_null(body))
            ruby_ivar_scan(ctx, body);
        ctx->nesting = saved_nesting;
        ctx->enclosing_class_qn = saved_class;
        return;
    }

    if (strcmp(k, "assignment") == 0 && ctx->enclosing_class_qn) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(left) && !ts_node_is_null(right) &&
            strcmp(ts_node_type(left), "instance_variable") == 0) {
            const char *tqn = ruby_static_ctor_type(ctx, right);
            if (tqn) {
                char *ivar = ruby_node_text(ctx, left);
                ruby_add_ivar(ctx, ctx->enclosing_class_qn, ivar, tqn);
            }
        }
    }

    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (!ts_node_is_null(c))
            ruby_ivar_scan(ctx, c);
    }
}

/* ── public API ─────────────────────────────────────────────────── */

void ruby_lsp_init(RubyLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                   const CBMTypeRegistry *registry, const char *module_qn,
                   CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->module_qn = module_qn;
    ctx->nesting = "";
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL);

    const char *dbg = getenv("CBM_LSP_DEBUG");
    ctx->debug = (dbg && dbg[0]);
}

/* ── entry: cbm_run_ruby_lsp ────────────────────────────────────── */

void cbm_run_ruby_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                      TSNode root) {
    if (!result || !arena || ts_node_is_null(root))
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    /* Phase A: stdlib (Ruby core + curated Rails surface). */
    cbm_ruby_stdlib_register(&reg, arena);

    const char *module_qn = result->module_qn;

    RubyLSPContext ctx;
    ruby_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, &result->resolved_calls);

    /* Phase B: PASS 1 — collect classes/modules, mixins, and methods from
     * the AST (methods register directly into `reg`; the AST is ground
     * truth for the instance-vs-singleton split the defs don't carry). */
    ctx.build_reg = &reg;
    ruby_pass1_scan(&ctx, root);
    ctx.build_reg = NULL;
    ruby_finalize_refs(&ctx);

    /* Finalize the registry for O(1) lookups — index allocations go to a
     * per-call scratch arena that dies with this call (see perl_lsp.c). */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    cbm_registry_finalize_into(&reg, &idx_arena);

    /* Phase B.5: PASS 1.5 — instance-variable types (needs the finished
     * class table + registry for constant resolution). */
    ctx.nesting = "";
    ctx.enclosing_class_qn = NULL;
    ruby_ivar_scan(&ctx, root);

    /* Phase C: PASS 2 — resolution walk. */
    ctx.nesting = "";
    ctx.enclosing_class_qn = NULL;
    ctx.enclosing_func_qn = NULL;
    ctx.in_singleton_method = false;
    ruby_resolve_calls_in_node(&ctx, root);

    if (ctx.debug) {
        fprintf(stderr, "[ruby_lsp] module_qn=%s defs=%d resolved=%d classes=%d mixins=%d\n",
                module_qn ? module_qn : "(null)", result->defs.count, result->resolved_calls.count,
                ctx.class_count, ctx.mixin_count);
        for (int i = 0; i < result->resolved_calls.count; i++) {
            CBMResolvedCall *r = &result->resolved_calls.items[i];
            fprintf(stderr, "[ruby_lsp]   %s -> %s [%s %.2f]\n", r->caller_qn, r->callee_qn,
                    r->strategy, r->confidence);
        }
    }

    cbm_arena_destroy(&idx_arena);
}

/* ── cross-file entry: cbm_run_ruby_lsp_cross ───────────────────── */

extern const TSLanguage *tree_sitter_ruby(void);

/* Derive the dotted constant path of a cross-file class def: the portion of
 * its QN after its module QN prefix ("proj.app.models.user.User" with
 * module "proj.app.models.user" → "User"). NULL when the shape is odd. */
static const char *ruby_cross_class_path(CBMArena *arena, const CBMLSPDef *d) {
    (void)arena;
    if (!d->qualified_name)
        return NULL;
    if (d->def_module_qn && d->def_module_qn[0]) {
        size_t mlen = strlen(d->def_module_qn);
        if (strncmp(d->qualified_name, d->def_module_qn, mlen) == 0 &&
            d->qualified_name[mlen] == '.') {
            return d->qualified_name + mlen + 1;
        }
    }
    return d->short_name;
}

void cbm_run_ruby_lsp_cross(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, CBMLSPDef *defs, int def_count,
                            const char **import_names, const char **import_qns, int import_count,
                            TSTree *cached_tree, CBMResolvedCallArray *out) {
    if (!arena || !source || !out)
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_ruby_stdlib_register(&reg, arena);

    RubyLSPContext ctx;
    ruby_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, out);
    ctx.import_names = import_names;
    ctx.import_qns = import_qns;
    ctx.import_count = import_count;

    /* Parse if the pipeline didn't hand us a cached tree. */
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        if (!parser)
            return;
        const TSLanguage *lang = tree_sitter_ruby();
        if (!ts_parser_set_language(parser, lang)) {
            ts_parser_delete(parser);
            return;
        }
        tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)source_len);
        ts_parser_delete(parser);
        if (!tree)
            return;
        owns_tree = true;
    }
    TSNode root = ts_tree_root_node(tree);

    /* PASS 1 over the local AST first (mixins + nesting-packed superclass
     * refs the defs don't carry). build_reg stays NULL — methods come from
     * the def registry below, which spans the whole project. */
    ruby_pass1_scan(&ctx, root);

    /* Register cross-file defs. Classes/modules go into the class table
     * (constant paths derived from their QNs — Ruby constants live in one
     * global namespace, so every project class is a resolution candidate);
     * ruby_add_class dedupes against the AST-scanned file-local entries.
     * Methods register under their parent-class receiver key. The
     * instance-vs-singleton split is not carried by CBMLSPDef, so
     * cross-file methods register on BOTH keys (an approximation the
     * per-file pass corrects for same-file dispatch). */
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;
        if (d->lang != CBM_LANG_RUBY)
            continue;
        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Module") == 0) {
            const char *path = ruby_cross_class_path(arena, d);
            if (!path)
                continue;
            RubyClassInfo *ci =
                ruby_add_class(&ctx, path, d->qualified_name, strcmp(d->label, "Module") == 0);
            if (ci && !ci->superclass_ref && d->embedded_types && d->embedded_types[0]) {
                /* First "|"-separated base class (Ruby has single
                 * inheritance). Normalize A::B to dotted form so the
                 * finalize step can resolve it; tolerate a legacy
                 * "< Base" spelling (raw superclass-node text). */
                const char *start = d->embedded_types;
                while (*start == '<' || *start == ' ' || *start == '\t')
                    start++;
                const char *bar = strchr(start, '|');
                char *ref = bar ? cbm_arena_strndup(arena, start, (size_t)(bar - start))
                                : cbm_arena_strdup(arena, start);
                if (ref) {
                    char *w = ref;
                    for (const char *p = ref; *p; p++) {
                        if (p[0] == ':' && p[1] == ':') {
                            *w++ = '.';
                            p++;
                        } else {
                            *w++ = *p;
                        }
                    }
                    *w = '\0';
                    ci->superclass_ref = ref;
                }
            }
        } else if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.min_params = -1;
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->short_name;
            rf.receiver_type = d->receiver_type;
            const CBMType **rets =
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
            if (rets) {
                rets[0] = cbm_type_unknown();
                rets[1] = NULL;
            }
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);
            cbm_registry_add_func(&reg, rf);
            /* Singleton-key twin (see block comment above). */
            if (d->receiver_type) {
                CBMRegisteredFunc rs = rf;
                rs.receiver_type = cbm_arena_sprintf(arena, "%s.self", d->receiver_type);
                cbm_registry_add_func(&reg, rs);
            }
        }
    }

    /* Resolve superclass + mixin refs against the completed class table. */
    ruby_finalize_refs(&ctx);

    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    cbm_registry_finalize_into(&reg, &idx_arena);

    /* PASS 1.5 + PASS 2. */
    ctx.nesting = "";
    ctx.enclosing_class_qn = NULL;
    ruby_ivar_scan(&ctx, root);

    ctx.nesting = "";
    ctx.enclosing_class_qn = NULL;
    ctx.enclosing_func_qn = NULL;
    ctx.in_singleton_method = false;
    ruby_resolve_calls_in_node(&ctx, root);

    if (owns_tree)
        ts_tree_delete(tree);
    cbm_arena_destroy(&idx_arena);
}
