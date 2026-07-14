/*
 * perl_lsp.c — Perl Light Semantic Pass.
 *
 * In-process type-aware call resolver for Perl. Mirrors the php_lsp.c /
 * go_lsp.c shape:
 *   1. Build a CBMTypeRegistry from file-local definitions + stdlib
 *      (perlfunc builtins + curated CPAN types) plus a per-package type entry
 *      carrying @ISA parents and the package's sub method table.
 *   2. perl_lsp_process_file does a TWO-PASS walk:
 *        PASS 1 — collect `package` declarations (a file may switch packages
 *          mid-file), @ISA / `use parent` / `use base` inheritance, and
 *          Exporter-style `use Foo qw(...)` imports.
 *        PASS 2 — walk each `subroutine_declaration_statement`, push a scope,
 *          bind the $self/$class invocant, track bless var→class, and resolve
 *          method/function call expressions into CBMResolvedCall edges.
 *
 * Verified tree-sitter-perl node/field names (Open Questions #1-3 in
 * 22-RESEARCH.md). These were confirmed against the vendored compiled grammar
 * at internal/cbm/vendored/grammars/perl/parser.c (ts_symbol_names and
 * ts_field_names tables — no node-types.json/grammar.js is vendored):
 *   - method_call_expression : fields `invocant` (receiver) and `method`
 *     (NOT `object`); arguments under field `arguments`.
 *   - function_call_expression / ambiguous_function_call_expression :
 *     field `function` (callee) and `arguments`.
 *   - package_statement : field `name` (the package name; "::"-separated).
 *   - use_statement : field `module` (the imported module) plus a
 *     `quoted_word_list` child for the `qw(...)` import/parent list.
 *   - assignment_expression : fields `left`, `operator`, `right`.
 *   - variable_declaration : holds an assignment_expression child for the
 *     `my $x = EXPR` initializer.
 *   - scalar/array/hash variables: node types `scalar`, `array`, `hash`
 *     (sigil included in node text, e.g. "$self", "@ISA").
 *   - string literals: `string_literal` / `interpolated_string_literal`;
 *     bare class names: `bareword` / `package` (autoquoted).
 *
 * QN scheme (verified against helpers.c cbm_enclosing_func_qn): Perl has no
 * class_node_types, so the structural extractor names every sub
 * `module_qn.subname` — the package is NOT woven into the sub QN. This module
 * therefore matches caller/callee edges by registering each file-local sub
 * under its extractor QN and resolving calls to those QNs by short name. A
 * per-package CBMRegisteredType (keyed by the package name) carries
 * method_names/method_qns + embedded_types (@ISA parents) so method dispatch
 * can walk the inheritance chain.
 *
 * Zero-edge guarantee: if a receiver's type is unknown/unindexed, NO edge is
 * emitted (false edges are worse than missing edges). Symbol-table aliasing
 * (*Foo::bar = \&...) is intentionally ignored.
 */

#include "perl_lsp.h"
#include "../helpers.h"
#include "../arena.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Recursion cap for perl_eval_expr_type — mirrors php_eval_expr_type's guard
 * (php returns unknown at depth >= 8). */
#define PERL_EVAL_MAX_DEPTH 8

/* bless / constructor confidence levels (22-RESEARCH.md §3). */
#define PERL_CONF_LITERAL 0.95f  /* bless($r, 'Literal'); resolved call */
#define PERL_CONF_INFERRED 0.75f /* ref($class)||$class idiom */

/* Maximum AST-walk recursion depth for the resolution/scan passes. Mirrors
 * java_lsp's JAVA_LSP_MAX_WALK_DEPTH: the per-child recursion of
 * perl_resolve_calls_in_node / perl_pass1_scan can stack-overflow on
 * pathologically nested real-world sources, the same failure mode that
 * produced documented SIGSEGVs in the Java/C++ walkers. Past the cap the
 * subtree is skipped — its calls stay unresolved (graceful degradation, not a
 * crash). The zero-edge guarantee is preserved: a skipped subtree emits no
 * edges, never a wrong one. */
#define CBM_LSP_PERL_MAX_WALK_DEPTH 512

/* ── forward declarations ───────────────────────────────────────── */

static void perl_resolve_calls_in_node(PerlLSPContext *ctx, TSNode node);
static void perl_resolve_calls_in_node_inner(PerlLSPContext *ctx, TSNode node);
static void process_subroutine(PerlLSPContext *ctx, TSNode node);
static void process_package_decl(PerlLSPContext *ctx, TSNode node);
static void perl_pass1_scan(PerlLSPContext *ctx, TSNode node);
static void perl_pass1_scan_inner(PerlLSPContext *ctx, TSNode node);
static const CBMType *perl_eval_function_call_type(PerlLSPContext *ctx, TSNode node);
static const CBMType *perl_eval_method_call_type(PerlLSPContext *ctx, TSNode node);
static const CBMType *perl_eval_new_type(PerlLSPContext *ctx, TSNode node);
static void perl_emit_resolved(PerlLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence);

/* ── helpers ────────────────────────────────────────────────────── */

/* Extract the source substring covered by a TSNode (arena-allocated). */
static char *perl_node_text(PerlLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

/* Perl qualified names use "." in the graph (project.path.module.pkg[.sub]).
 * Convert "Foo::Bar::Baz" to "Foo.Bar.Baz" so we can compose with module_qn
 * (which already uses ".") and look up registry entries. */
static char *perl_pkg_to_dot(CBMArena *a, const char *pkg) {
    if (!pkg)
        return NULL;
    size_t n = strlen(pkg);
    char *out = (char *)cbm_arena_alloc(a, n + 1);
    if (!out)
        return NULL;
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        if (pkg[i] == ':' && i + 1 < n && pkg[i + 1] == ':') {
            out[w++] = '.';
            i++; /* skip the second ':' */
        } else {
            out[w++] = pkg[i];
        }
    }
    out[w] = '\0';
    return out;
}

/* Strip a leading sigil ($ @ % & *) from a Perl variable's text. Returns a
 * pointer into the same string (no copy). */
static const char *perl_strip_sigil(const char *name) {
    if (!name)
        return NULL;
    if (name[0] == '$' || name[0] == '@' || name[0] == '%' || name[0] == '&' || name[0] == '*')
        return name + 1;
    return name;
}

/* Strip surrounding quotes from a string-literal node's text ('...' / "...").
 * Returns an arena copy of the inner content, or NULL if not quoted. */
static char *perl_unquote(CBMArena *a, const char *s) {
    if (!s || !s[0])
        return NULL;
    size_t n = strlen(s);
    if ((s[0] == '\'' || s[0] == '"') && n >= 2 && s[n - 1] == s[0]) {
        return cbm_arena_strndup(a, s + 1, n - 2);
    }
    return NULL;
}

/* Is this a string-literal-ish node? */
static bool perl_is_string_node(const char *k) {
    return strcmp(k, "string_literal") == 0 || strcmp(k, "interpolated_string_literal") == 0;
}

/* Is this a bareword / package-name node (e.g. a bare class name `Foo::Bar`)? */
static bool perl_is_bareword_node(const char *k) {
    return strcmp(k, "bareword") == 0 || strcmp(k, "package") == 0 ||
           strcmp(k, "autoquoted_bareword") == 0 || strcmp(k, "_bareword") == 0;
}

/* Extract the declared scalar from a variable_declaration (`my $x`). The
 * grammar exposes the target via the `variable` field (singular). Returns the
 * scalar/array/hash node, or the input unchanged if it is not a declaration. */
static TSNode perl_decl_target(TSNode node) {
    if (strcmp(ts_node_type(node), "variable_declaration") == 0) {
        TSNode v = ts_node_child_by_field_name(node, "variable", 8);
        if (!ts_node_is_null(v))
            return v;
    }
    return node;
}

/* Collect `node`'s children into a malloc'd array so callers get O(1) indexed
 * access on WIDE nodes: bare ts_node_child(node, i) is O(i), so an index loop
 * over a wide flat node is O(n^2) — the class of bug the ARM
 * `extract_wide_flat_file_is_linear` guard caught. Returns NULL for small nodes
 * (< PERL_CURSOR_MIN_CHILDREN) and on OOM; callers then fall back to
 * ts_node_child, which is cheaper at small child counts and merely
 * quadratic-but-correct on OOM. Mirrors wd_collect_children in extract_defs.c.
 * Caller frees. */
enum { PERL_CURSOR_MIN_CHILDREN = 64 };
static TSNode *perl_collect_children(TSNode node, uint32_t cc) {
    if (cc < PERL_CURSOR_MIN_CHILDREN)
        return NULL;
    TSNode *buf = (TSNode *)malloc((size_t)cc * sizeof(TSNode));
    if (!buf)
        return NULL;
    TSTreeCursor cur = ts_tree_cursor_new(node);
    uint32_t got = 0;
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            buf[got++] = ts_tree_cursor_current_node(&cur);
        } while (got < cc && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    if (got != cc) {
        /* Defensive: cursor and child_count disagree — fall back to indexed. */
        free(buf);
        return NULL;
    }
    return buf;
}

/* Find the first named child whose node type is `kind` (shallow). */
static TSNode perl_first_child_of_type(TSNode node, const char *kind) {
    uint32_t nc = ts_node_child_count(node);
    TSNode *kids = perl_collect_children(node, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(node, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        if (strcmp(ts_node_type(c), kind) == 0) {
            free(kids);
            return c;
        }
    }
    free(kids);
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

/* ── public API: init / use map ─────────────────────────────────── */

void perl_lsp_init(PerlLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                   const CBMTypeRegistry *registry, const char *module_qn,
                   CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->module_qn = module_qn;
    ctx->current_package_qn = "";
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL);

    const char *dbg = getenv("CBM_LSP_DEBUG");
    ctx->debug = (dbg && dbg[0]);
}

void perl_lsp_add_use(PerlLSPContext *ctx, const char *local_name, const char *target_qn) {
    if (!ctx || !local_name || !target_qn)
        return;
    if (ctx->use_count >= ctx->use_cap) {
        int newcap = ctx->use_cap ? ctx->use_cap * 2 : 8;
        const char **ln =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(char *));
        const char **tq =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(char *));
        if (!ln || !tq)
            return;
        for (int i = 0; i < ctx->use_count; i++) {
            ln[i] = ctx->use_local_names[i];
            tq[i] = ctx->use_target_qns[i];
        }
        ctx->use_local_names = ln;
        ctx->use_target_qns = tq;
        ctx->use_cap = newcap;
    }
    ctx->use_local_names[ctx->use_count] = cbm_arena_strdup(ctx->arena, local_name);
    ctx->use_target_qns[ctx->use_count] = cbm_arena_strdup(ctx->arena, target_qn);
    ctx->use_count++;
}

/* Look up an Exporter import: local symbol → target QN, or NULL. */
static const char *perl_find_import(PerlLSPContext *ctx, const char *local_name) {
    for (int i = 0; i < ctx->use_count; i++) {
        if (strcmp(ctx->use_local_names[i], local_name) == 0)
            return ctx->use_target_qns[i];
    }
    return NULL;
}

const char *perl_resolve_package_name(PerlLSPContext *ctx, const char *name) {
    if (!name || !name[0])
        return name;
    /* `__PACKAGE__` resolves to the enclosing package. */
    if (strcmp(name, "__PACKAGE__") == 0) {
        if (ctx->enclosing_package_qn && ctx->enclosing_package_qn[0])
            return ctx->enclosing_package_qn;
        return ctx->current_package_qn;
    }
    return name;
}

/* ── @ISA registry helpers ──────────────────────────────────────── */

/* Record `pkg inherits from parent` in the ctx ISA table. Both are package
 * names (e.g. "Derived", "Base"). */
static void perl_add_isa(PerlLSPContext *ctx, const char *pkg, const char *parent) {
    if (!ctx || !pkg || !parent || !pkg[0] || !parent[0])
        return;
    if (ctx->isa_count >= ctx->isa_cap) {
        int newcap = ctx->isa_cap ? ctx->isa_cap * 2 : 8;
        const char **pk =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(char *));
        const char **pa =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(char *));
        if (!pk || !pa)
            return;
        for (int i = 0; i < ctx->isa_count; i++) {
            pk[i] = ctx->isa_pkg_qns[i];
            pa[i] = ctx->isa_parent_qns[i];
        }
        ctx->isa_pkg_qns = pk;
        ctx->isa_parent_qns = pa;
        ctx->isa_cap = newcap;
    }
    ctx->isa_pkg_qns[ctx->isa_count] = cbm_arena_strdup(ctx->arena, pkg);
    ctx->isa_parent_qns[ctx->isa_count] = cbm_arena_strdup(ctx->arena, parent);
    ctx->isa_count++;
}

/* ── method lookup over the @ISA chain ──────────────────────────── */

/* Resolve a method on a package, searching the package's own subs first, then
 * walking parents (@ISA) depth-first. Returns the resolved sub's
 * CBMRegisteredFunc or NULL. Bounded by CBM_LSP_MAX_LOOKUP_DEPTH * 2 visited.
 *
 * package_qn is a package name (e.g. "Foo::Bar"). Methods are matched via the
 * registered type's method tables (populated in perl_attach_methods) or by a
 * direct receiver-keyed registry method (stdlib types). */
const CBMRegisteredFunc *perl_lookup_method(PerlLSPContext *ctx, const char *package_qn,
                                            const char *method_name) {
    if (!ctx || !package_qn || !method_name)
        return NULL;

    enum { CAP = CBM_LSP_MAX_LOOKUP_DEPTH * 2 };
    const char *frontier[CAP];
    int frontier_count = 0;
    const char *visited[CAP];
    int visited_count = 0;

    frontier[frontier_count++] = package_qn;

    while (frontier_count > 0 && visited_count < CAP) {
        const char *pkg = frontier[--frontier_count];
        bool seen = false;
        for (int v = 0; v < visited_count; v++) {
            if (strcmp(visited[v], pkg) == 0) {
                seen = true;
                break;
            }
        }
        if (seen)
            continue;
        visited[visited_count++] = pkg;

        const CBMRegisteredType *t = cbm_registry_lookup_type(ctx->registry, pkg);
        if (!t) {
            /* Even without a type entry, a stdlib receiver-keyed method may
             * exist (e.g. a curated CPAN class). */
            const CBMRegisteredFunc *direct =
                cbm_registry_lookup_method(ctx->registry, pkg, method_name);
            if (direct)
                return direct;
            continue;
        }

        /* Own methods (sub table built in perl_attach_methods). */
        if (t->method_names && t->method_qns) {
            for (int i = 0; t->method_names[i]; i++) {
                if (strcmp(t->method_names[i], method_name) == 0) {
                    const CBMRegisteredFunc *f =
                        cbm_registry_lookup_func(ctx->registry, t->method_qns[i]);
                    if (f)
                        return f;
                }
            }
        }
        /* Direct receiver-keyed method (stdlib types register this way). */
        const CBMRegisteredFunc *direct =
            cbm_registry_lookup_method(ctx->registry, pkg, method_name);
        if (direct)
            return direct;

        /* Push parents (@ISA) onto the frontier. */
        if (t->embedded_types) {
            for (int i = 0; t->embedded_types[i] && frontier_count < CAP; i++)
                frontier[frontier_count++] = t->embedded_types[i];
        }
    }
    return NULL;
}

/* ── expression typing ──────────────────────────────────────────── */

/* Detect a `bless` function call and return the blessed class type, or NULL if
 * this is not a bless call. Recognizes:
 *   bless($ref, 'Class')              → NAMED("Class")          (literal)
 *   bless({}, ref($class) || $class)  → enclosing package        (inferred)
 *   bless $ref, __PACKAGE__           → enclosing package
 *   bless({})                         → enclosing package (1-arg form) */
static const CBMType *perl_eval_bless(PerlLSPContext *ctx, TSNode call_node) {
    const char *k = ts_node_type(call_node);
    if (strcmp(k, "function_call_expression") != 0 &&
        strcmp(k, "ambiguous_function_call_expression") != 0)
        return NULL;

    TSNode fn = ts_node_child_by_field_name(call_node, "function", 8);
    if (ts_node_is_null(fn))
        return NULL;
    char *fname = perl_node_text(ctx, fn);
    if (!fname || strcmp(fname, "bless") != 0)
        return NULL;

    TSNode args = ts_node_child_by_field_name(call_node, "arguments", 9);
    if (ts_node_is_null(args))
        args = call_node; /* arguments may be inline children */

    /* Find the SECOND meaningful argument (the class). The first is the ref. */
    int seen = 0;
    TSNode class_arg;
    memset(&class_arg, 0, sizeof(class_arg));
    bool have_class = false;
    uint32_t nc = ts_node_child_count(args);
    TSNode *kids = perl_collect_children(args, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(args, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *ck = ts_node_type(c);
        /* Skip the literal "bless" callee if args==call_node. */
        if (strcmp(ck, "function") == 0)
            continue;
        seen++;
        if (seen == 2) {
            class_arg = c;
            have_class = true;
            break;
        }
    }
    free(kids);

    const char *pkg =
        ctx->enclosing_package_qn ? ctx->enclosing_package_qn : ctx->current_package_qn;

    if (!have_class) {
        /* 1-arg bless: blesses into the current package. */
        if (pkg && pkg[0])
            return cbm_type_named(ctx->arena, pkg);
        return cbm_type_unknown();
    }

    const char *ack = ts_node_type(class_arg);
    if (perl_is_string_node(ack)) {
        char *raw = perl_node_text(ctx, class_arg);
        char *inner = perl_unquote(ctx->arena, raw);
        if (inner && inner[0])
            return cbm_type_named(ctx->arena, perl_resolve_package_name(ctx, inner));
    } else if (perl_is_bareword_node(ack)) {
        char *bw = perl_node_text(ctx, class_arg);
        if (bw && strcmp(bw, "__PACKAGE__") == 0) {
            if (pkg && pkg[0])
                return cbm_type_named(ctx->arena, pkg);
        } else if (bw && bw[0]) {
            return cbm_type_named(ctx->arena, perl_resolve_package_name(ctx, bw));
        }
    } else {
        /* ref($class) || $class  /  $class → the enclosing sub's invocant
         * class. Bind to the enclosing package as the best static guess
         * (standard constructor idiom). */
        if (pkg && pkg[0])
            return cbm_type_named(ctx->arena, pkg);
    }
    return cbm_type_unknown();
}

const CBMType *perl_eval_expr_type(PerlLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();

    /* Recursion guard (mirrors php_eval_expr_type, cap PERL_EVAL_MAX_DEPTH). */
    if (ctx->eval_depth >= PERL_EVAL_MAX_DEPTH)
        return cbm_type_unknown();
    ctx->eval_depth++;
    const CBMType *result = cbm_type_unknown();

    const char *k = ts_node_type(node);

    if (strcmp(k, "scalar") == 0 || strcmp(k, "scalar_variable") == 0) {
        char *txt = perl_node_text(ctx, node);
        if (txt) {
            const char *bare = perl_strip_sigil(txt);
            const CBMType *t = cbm_scope_lookup(ctx->current_scope, bare);
            if (t)
                result = t;
        }
    } else if (strcmp(k, "method_call_expression") == 0) {
        result = perl_eval_method_call_type(ctx, node);
    } else if (strcmp(k, "function_call_expression") == 0 ||
               strcmp(k, "ambiguous_function_call_expression") == 0) {
        const CBMType *blessed = perl_eval_bless(ctx, node);
        if (blessed && !cbm_type_is_unknown(blessed))
            result = blessed;
        else
            result = perl_eval_function_call_type(ctx, node);
    } else if (strcmp(k, "assignment_expression") == 0) {
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(right))
            result = perl_eval_expr_type(ctx, right);
    } else if (strcmp(k, "variable_declaration") == 0) {
        /* `my $x = EXPR;` — the `=` is wrapped in an assignment_expression
         * child; recurse into it. */
        TSNode assign = perl_first_child_of_type(node, "assignment_expression");
        if (!ts_node_is_null(assign))
            result = perl_eval_expr_type(ctx, assign);
    } else if (strcmp(k, "parenthesized_expression") == 0 || strcmp(k, "list_expression") == 0) {
        /* Unwrap a single meaningful child. */
        uint32_t nc = ts_node_child_count(node);
        TSNode *kids = perl_collect_children(node, nc);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = kids ? kids[i] : ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            result = perl_eval_expr_type(ctx, c);
            break;
        }
        free(kids);
    }
    /* Hash/array deref of an unknown type → unknown (no edge). Anything we did
     * not recognize stays unknown. */

    ctx->eval_depth--;
    return result;
}

/* ClassName->new(...) returns ClassName. Handles the method_call_expression
 * where the invocant is a bareword/string class and the method is `new`.
 * Returns the constructed type, or NULL if this is not a constructor call. */
static const CBMType *perl_eval_new_type(PerlLSPContext *ctx, TSNode node) {
    TSNode inv = ts_node_child_by_field_name(node, "invocant", 8);
    TSNode meth = ts_node_child_by_field_name(node, "method", 6);
    if (ts_node_is_null(inv) || ts_node_is_null(meth))
        return NULL;
    char *mname = perl_node_text(ctx, meth);
    if (!mname || strcmp(mname, "new") != 0)
        return NULL;
    const char *ik = ts_node_type(inv);
    if (perl_is_bareword_node(ik)) {
        char *cls = perl_node_text(ctx, inv);
        if (cls && cls[0])
            return cbm_type_named(ctx->arena, perl_resolve_package_name(ctx, cls));
    } else if (perl_is_string_node(ik)) {
        char *raw = perl_node_text(ctx, inv);
        char *inner = perl_unquote(ctx->arena, raw);
        if (inner && inner[0])
            return cbm_type_named(ctx->arena, perl_resolve_package_name(ctx, inner));
    }
    return NULL;
}

/* func() in the current package, or Package::func() static call. Returns the
 * function's return type (for chaining), or unknown. */
static const CBMType *perl_eval_function_call_type(PerlLSPContext *ctx, TSNode node) {
    TSNode fn = ts_node_child_by_field_name(node, "function", 8);
    if (ts_node_is_null(fn))
        return cbm_type_unknown();
    char *name = perl_node_text(ctx, fn);
    if (!name || !name[0])
        return cbm_type_unknown();

    const CBMRegisteredFunc *f = NULL;

    /* Package::func() — qualified static call. Split on the LAST "::" so
     * multi-level packages keep their full name (Foo::Bar::sub -> pkg
     * "Foo::Bar", sub "sub"), mirroring perl_resolve_function_call. */
    char *colons = NULL;
    for (char *p = strstr(name, "::"); p; p = strstr(p + 2, "::"))
        colons = p;
    if (colons) {
        size_t plen = (size_t)(colons - name);
        char *pkg = cbm_arena_strndup(ctx->arena, name, plen);
        const char *shortn = colons + 2;
        f = perl_lookup_method(ctx, pkg, shortn);
        if (!f)
            f = cbm_registry_lookup_symbol(ctx->registry, pkg, shortn);
    } else {
        /* Bare func() — Exporter import map, then file-local/global func. */
        const char *imp = perl_find_import(ctx, name);
        if (imp)
            f = cbm_registry_lookup_func(ctx->registry, imp);
        if (!f)
            f = cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, name);
    }
    if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC &&
        f->signature->data.func.return_types && f->signature->data.func.return_types[0]) {
        return f->signature->data.func.return_types[0];
    }
    return cbm_type_unknown();
}

/* $obj->m / Class->m / $self->m — returns the method's return type. */
static const CBMType *perl_eval_method_call_type(PerlLSPContext *ctx, TSNode node) {
    /* ClassName->new returns ClassName (constructor). */
    const CBMType *ctor = perl_eval_new_type(ctx, node);
    if (ctor)
        return ctor;

    TSNode inv = ts_node_child_by_field_name(node, "invocant", 8);
    TSNode meth = ts_node_child_by_field_name(node, "method", 6);
    if (ts_node_is_null(meth))
        return cbm_type_unknown();
    char *mname = perl_node_text(ctx, meth);
    if (!mname || !mname[0])
        return cbm_type_unknown();

    const char *class_qn = NULL;
    if (!ts_node_is_null(inv)) {
        const char *ik = ts_node_type(inv);
        if (perl_is_bareword_node(ik)) {
            char *cls = perl_node_text(ctx, inv);
            if (cls && cls[0])
                class_qn = perl_resolve_package_name(ctx, cls);
        } else {
            const CBMType *recv = perl_eval_expr_type(ctx, inv);
            if (recv && recv->kind == CBM_TYPE_NAMED)
                class_qn = recv->data.named.qualified_name;
        }
    }
    if (!class_qn)
        return cbm_type_unknown();

    const CBMRegisteredFunc *f = perl_lookup_method(ctx, class_qn, mname);
    if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC &&
        f->signature->data.func.return_types && f->signature->data.func.return_types[0]) {
        return f->signature->data.func.return_types[0];
    }
    return cbm_type_unknown();
}

/* ── emit ───────────────────────────────────────────────────────── */

static void perl_emit_resolved(PerlLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    rc.reason = NULL;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

/* ── call/method dispatch (emit edges) ──────────────────────────── */

/* Resolve a function/static call and emit an edge if it lands on a registered
 * sub. Bare func(), Exporter func(), and Package::func() static calls. */
static void perl_resolve_function_call(PerlLSPContext *ctx, TSNode call) {
    TSNode fn = ts_node_child_by_field_name(call, "function", 8);
    if (ts_node_is_null(fn))
        return;
    char *name = perl_node_text(ctx, fn);
    if (!name || !name[0])
        return;
    /* `bless` is a typing primitive, not a resolvable user call. */
    if (strcmp(name, "bless") == 0)
        return;

    const CBMRegisteredFunc *f = NULL;
    /* Split on the LAST "::" so multi-level packages keep their full name
     * (Foo::Bar::sub -> pkg "Foo::Bar", sub "sub"). strstr would stop at the
     * first "::", yielding pkg "Foo" and a sub name that still contains "::" —
     * that never resolves, so the call falls through to the bare-name fallback,
     * which collapses distinct packages' same-named subs onto one winner. */
    char *colons = NULL;
    for (char *p = strstr(name, "::"); p; p = strstr(p + 2, "::"))
        colons = p;
    if (colons) {
        size_t plen = (size_t)(colons - name);
        char *pkg = cbm_arena_strndup(ctx->arena, name, plen);
        const char *shortn = colons + 2;
        f = perl_lookup_method(ctx, pkg, shortn);
        if (!f)
            f = cbm_registry_lookup_symbol(ctx->registry, pkg, shortn);
        if (f) {
            perl_emit_resolved(ctx, f->qualified_name, "perl_static_call", PERL_CONF_LITERAL);
            return;
        }
    } else {
        const char *imp = perl_find_import(ctx, name);
        if (imp) {
            f = cbm_registry_lookup_func(ctx->registry, imp);
            if (f) {
                perl_emit_resolved(ctx, f->qualified_name, "perl_imported_function",
                                   PERL_CONF_LITERAL);
                return;
            }
        }
        f = cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, name);
        if (f) {
            perl_emit_resolved(ctx, f->qualified_name, "perl_function_local", PERL_CONF_LITERAL);
            return;
        }
    }
    /* Unresolved — emit nothing (the unified extractor already records the raw
     * call edge; zero spurious edges). */
}

/* Resolve a method call and emit an edge if the receiver type is known AND the
 * method resolves through the @ISA chain. Unknown receiver → NO edge. */
static void perl_resolve_method_call(PerlLSPContext *ctx, TSNode call) {
    /* Class->new constructor: only meaningful for typing, not a callable user
     * sub unless the package actually defines new — fall through to lookup. */
    TSNode inv = ts_node_child_by_field_name(call, "invocant", 8);
    TSNode meth = ts_node_child_by_field_name(call, "method", 6);
    if (ts_node_is_null(meth))
        return;
    char *mname = perl_node_text(ctx, meth);
    if (!mname || !mname[0])
        return;

    /* $self->SUPER::method() — dispatch to the enclosing package's parent
     * (MRO root recorded in process_package_decl). Resolve `method` starting
     * at the parent so an overridden method in the child is skipped. No known
     * parent or unresolved method → no edge (zero-edge guarantee). */
    if (strncmp(mname, "SUPER::", 7) == 0) {
        const char *super_method = mname + 7;
        if (!super_method[0])
            return;
        const char *parent_qn = ctx->enclosing_parent_qn;
        if (!parent_qn || !parent_qn[0])
            return;
        const CBMRegisteredFunc *sf = perl_lookup_method(ctx, parent_qn, super_method);
        if (sf)
            perl_emit_resolved(ctx, sf->qualified_name, "perl_method_super", PERL_CONF_LITERAL);
        return;
    }

    const char *class_qn = NULL;
    const char *strategy = "perl_method_typed";
    if (!ts_node_is_null(inv)) {
        const char *ik = ts_node_type(inv);
        if (perl_is_bareword_node(ik)) {
            char *cls = perl_node_text(ctx, inv);
            if (cls && cls[0])
                class_qn = perl_resolve_package_name(ctx, cls);
            strategy = "perl_method_static";
        } else {
            const CBMType *recv = perl_eval_expr_type(ctx, inv);
            if (recv && recv->kind == CBM_TYPE_NAMED) {
                class_qn = recv->data.named.qualified_name;
                strategy = "perl_method_typed";
            }
        }
    }
    if (!class_qn)
        return; /* unknown receiver — zero-edge guarantee */

    const CBMRegisteredFunc *f = perl_lookup_method(ctx, class_qn, mname);
    if (f) {
        const char *strat = (f->receiver_type && strcmp(f->receiver_type, class_qn) == 0)
                                ? strategy
                                : "perl_method_inherited";
        perl_emit_resolved(ctx, f->qualified_name, strat, PERL_CONF_LITERAL);
        return;
    }
    /* Receiver typed but method not found in the indexed inheritance chain.
     * Per the zero-edge guarantee, emit nothing rather than a guessed edge. */
}

/* ── assignment observer (scope binding) ────────────────────────── */

/* Bind an LHS scalar to the RHS type. Handles `my $x = EXPR;` and `$x = EXPR;`.
 * Only single scalar targets are tracked (list assignment is skipped). */
static void perl_process_assignment(PerlLSPContext *ctx, TSNode assign) {
    TSNode left = ts_node_child_by_field_name(assign, "left", 4);
    TSNode right = ts_node_child_by_field_name(assign, "right", 5);
    if (ts_node_is_null(left) || ts_node_is_null(right))
        return;

    TSNode lhs_var = perl_decl_target(left);
    const char *lvk = ts_node_type(lhs_var);
    if (strcmp(lvk, "scalar") != 0 && strcmp(lvk, "scalar_variable") != 0)
        return;

    char *vtxt = perl_node_text(ctx, lhs_var);
    if (!vtxt)
        return;
    const char *bare = perl_strip_sigil(vtxt);
    if (!bare || !bare[0])
        return;

    const CBMType *rt = perl_eval_expr_type(ctx, right);
    if (rt && rt->kind == CBM_TYPE_NAMED)
        cbm_scope_bind(ctx->current_scope, bare, rt);
}

/* ── body walk ──────────────────────────────────────────────────── */

/* Depth-guarded entry: the AST walk recurses per nesting level and can stack-
 * overflow on pathologically nested sources (the same failure mode documented
 * for the Java/C++ walkers). Past CBM_LSP_PERL_MAX_WALK_DEPTH the subtree is
 * skipped — graceful degradation, never a wrong edge. */
static void perl_resolve_calls_in_node(PerlLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= CBM_LSP_PERL_MAX_WALK_DEPTH)
        return;
    ctx->walk_depth++;
    perl_resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}

static void perl_resolve_calls_in_node_inner(PerlLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *k = ts_node_type(node);

    /* Nested subs get their own scope via process_subroutine. */
    if (strcmp(k, "subroutine_declaration_statement") == 0 ||
        strcmp(k, "method_declaration_statement") == 0 ||
        strcmp(k, "anonymous_subroutine_expression") == 0) {
        process_subroutine(ctx, node);
        return;
    }
    /* A block-scoped package: `package Foo { ... }` updates package context. */
    if (strcmp(k, "package_statement") == 0) {
        process_package_decl(ctx, node);
        /* Continue walking children (block body may follow). */
    }

    /* Scope-binding observers. `my $x = bless(...)` is a variable_declaration
     * wrapping an assignment_expression; handle both forms. */
    if (strcmp(k, "assignment_expression") == 0) {
        perl_process_assignment(ctx, node);
    } else if (strcmp(k, "variable_declaration") == 0) {
        TSNode assign = perl_first_child_of_type(node, "assignment_expression");
        if (!ts_node_is_null(assign))
            perl_process_assignment(ctx, assign);
    }

    /* Call-resolution dispatch. */
    if (strcmp(k, "function_call_expression") == 0 ||
        strcmp(k, "ambiguous_function_call_expression") == 0) {
        perl_resolve_function_call(ctx, node);
    } else if (strcmp(k, "method_call_expression") == 0) {
        perl_resolve_method_call(ctx, node);
    }

    /* Recurse. */
    uint32_t nc = ts_node_child_count(node);
    TSNode *kids = perl_collect_children(node, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(node, i);
        if (!ts_node_is_null(c))
            perl_resolve_calls_in_node(ctx, c);
    }
    free(kids);
}

/* ── subroutine processing ──────────────────────────────────────── */

/* Find the sub's name via the `name` field. */
static char *perl_sub_name(PerlLSPContext *ctx, TSNode node) {
    TSNode name = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name))
        return NULL;
    return perl_node_text(ctx, name);
}

/* The invocant idiom is `my $self = shift;` / `shift @_` / `$_[0]`. Match
 * `shift` only at word boundaries so `shifty()` / `myshift` do not falsely bind
 * the receiver, and also accept the `$_[0]` positional form. */
static bool perl_rhs_is_invocant(const char *rtxt) {
    if (!rtxt)
        return false;
    if (strstr(rtxt, "$_[0]"))
        return true;
    for (const char *p = strstr(rtxt, "shift"); p; p = strstr(p + 5, "shift")) {
        char before = (p == rtxt) ? '\0' : p[-1];
        char after = p[5];
        bool lb = !(isalnum((unsigned char)before) || before == '_');
        bool rb = !(isalnum((unsigned char)after) || after == '_');
        if (lb && rb)
            return true;
    }
    return false;
}

/* Bind the invocant: in a method sub belonging to package P, the first
 * statement is typically `my $self = shift;` or `my $class = shift;`. Bind the
 * first such scalar to type P so $self->method() / $class->method() dispatch. */
static void perl_infer_self_type(PerlLSPContext *ctx, TSNode body) {
    const char *pkg =
        ctx->enclosing_package_qn ? ctx->enclosing_package_qn : ctx->current_package_qn;
    if (!pkg || !pkg[0])
        return;
    uint32_t nc = ts_node_child_count(body);
    TSNode *kids = perl_collect_children(body, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode stmt = kids ? kids[i] : ts_node_child(body, i);
        if (ts_node_is_null(stmt) || !ts_node_is_named(stmt))
            continue;

        TSNode assign;
        memset(&assign, 0, sizeof(assign));
        const char *sk = ts_node_type(stmt);
        if (strcmp(sk, "expression_statement") == 0) {
            TSNode a = perl_first_child_of_type(stmt, "assignment_expression");
            if (!ts_node_is_null(a)) {
                assign = a;
            } else {
                TSNode vd = perl_first_child_of_type(stmt, "variable_declaration");
                if (!ts_node_is_null(vd))
                    assign = perl_first_child_of_type(vd, "assignment_expression");
            }
        } else if (strcmp(sk, "variable_declaration") == 0) {
            assign = perl_first_child_of_type(stmt, "assignment_expression");
        } else if (strcmp(sk, "assignment_expression") == 0) {
            assign = stmt;
        }
        if (ts_node_is_null(assign))
            continue;

        TSNode left = ts_node_child_by_field_name(assign, "left", 4);
        TSNode right = ts_node_child_by_field_name(assign, "right", 5);
        if (ts_node_is_null(left) || ts_node_is_null(right))
            continue;
        TSNode lhs_var = perl_decl_target(left);
        const char *lvk = ts_node_type(lhs_var);
        if (strcmp(lvk, "scalar") != 0 && strcmp(lvk, "scalar_variable") != 0)
            continue;

        /* RHS must reference the invocant idiom (`shift` / `shift @_` / `$_[0]`). */
        char *rtxt = perl_node_text(ctx, right);
        if (!perl_rhs_is_invocant(rtxt))
            continue;

        char *vtxt = perl_node_text(ctx, lhs_var);
        if (!vtxt)
            continue;
        const char *bare = perl_strip_sigil(vtxt);
        if (bare && bare[0])
            cbm_scope_bind(ctx->current_scope, bare, cbm_type_named(ctx->arena, pkg));
        free(kids);
        return; /* only the first invocant binding */
    }
    free(kids);
}

static void process_subroutine(PerlLSPContext *ctx, TSNode node) {
    CBMScope *saved_scope = ctx->current_scope;
    const char *saved_func = ctx->enclosing_func_qn;

    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

    /* Sub QN = module_qn.subname (package is NOT woven in — see file header). */
    char *sname = perl_sub_name(ctx, node);
    if (sname && sname[0]) {
        if (ctx->module_qn)
            ctx->enclosing_func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, sname);
        else
            ctx->enclosing_func_qn = cbm_arena_strdup(ctx->arena, sname);
    }

    /* Locate the body block. */
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (ts_node_is_null(body))
        body = perl_first_child_of_type(node, "block");

    if (!ts_node_is_null(body)) {
        perl_infer_self_type(ctx, body);
        perl_resolve_calls_in_node(ctx, body);
    }

    ctx->current_scope = saved_scope;
    ctx->enclosing_func_qn = saved_func;
}

/* ── package + use collection (PASS 1) ──────────────────────────── */

/* Set the current package from a package_statement. */
static void process_package_decl(PerlLSPContext *ctx, TSNode node) {
    TSNode name = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name))
        name = perl_first_child_of_type(node, "package");
    if (ts_node_is_null(name))
        return;
    char *pkg = perl_node_text(ctx, name);
    if (!pkg || !pkg[0])
        return;
    ctx->current_package_qn = cbm_arena_strdup(ctx->arena, pkg);
    ctx->enclosing_package_qn = ctx->current_package_qn;

    /* Record the package's first @ISA parent for SUPER:: dispatch. The ISA
     * table is fully populated by PASS 1 before this runs in PASS 2, so the
     * MRO root is available here. NULL when the package has no known parent —
     * SUPER:: then resolves to nothing (zero-edge guarantee). */
    ctx->enclosing_parent_qn = NULL;
    for (int i = 0; i < ctx->isa_count; i++) {
        if (ctx->isa_pkg_qns[i] && strcmp(ctx->isa_pkg_qns[i], pkg) == 0) {
            ctx->enclosing_parent_qn = ctx->isa_parent_qns[i];
            break;
        }
    }
}

/* Parse the `qw(a b c)` list inside a node into the import map for module
 * `module_name`: each word W maps to `module_name::W`. */
static void perl_collect_qw_imports(PerlLSPContext *ctx, TSNode container,
                                    const char *module_name) {
    TSNode qw = perl_first_child_of_type(container, "quoted_word_list");
    if (ts_node_is_null(qw))
        return;
    uint32_t nc = ts_node_child_count(qw);
    TSNode *kids = perl_collect_children(qw, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode w = kids ? kids[i] : ts_node_child(qw, i);
        if (ts_node_is_null(w) || !ts_node_is_named(w))
            continue;
        char *word = perl_node_text(ctx, w);
        if (!word || !word[0])
            continue;
        const char *fn = perl_strip_sigil(word); /* allow &func imports */
        if (!fn || !fn[0] || !(isalpha((unsigned char)fn[0]) || fn[0] == '_'))
            continue;
        /* Registry QNs are fully dotted (e.g. "Scalar.Util.blessed"): the
         * module portion uses "." not "::". Dot the module so the import
         * target matches the registry key for exact-match lookup. */
        const char *module_dot = perl_pkg_to_dot(ctx->arena, module_name);
        if (!module_dot)
            module_dot = module_name;
        char *target = cbm_arena_sprintf(ctx->arena, "%s.%s", module_dot, fn);
        perl_lsp_add_use(ctx, fn, target);
    }
    free(kids);
}

/* Recursively collect parent package names from a subtree, registering each
 * as an @ISA parent of `child_pkg`. Accepts string literals, barewords, and
 * `quoted_word_list` words, descending through `list_expression` /
 * parenthesized wrappers. Skips the `-norequire` flag and the leading
 * `parent`/`base` module barewords. Bounded recursion depth. */
static void perl_collect_parents(PerlLSPContext *ctx, TSNode node, const char *child_pkg,
                                 int depth) {
    if (ts_node_is_null(node) || depth > 6)
        return;
    const char *k = ts_node_type(node);
    if (perl_is_string_node(k)) {
        char *raw = perl_node_text(ctx, node);
        char *inner = perl_unquote(ctx->arena, raw);
        if (inner && inner[0] && strcmp(inner, "-norequire") != 0)
            perl_add_isa(ctx, child_pkg, inner);
        return;
    }
    if (perl_is_bareword_node(k)) {
        char *bw = perl_node_text(ctx, node);
        if (bw && bw[0] && strcmp(bw, "parent") != 0 && strcmp(bw, "base") != 0 &&
            strcmp(bw, "-norequire") != 0 && bw[0] != '-')
            perl_add_isa(ctx, child_pkg, bw);
        return;
    }
    /* quoted_word_list words come through as named string-content children. */
    if (strcmp(k, "quoted_word_list") == 0) {
        uint32_t nc = ts_node_child_count(node);
        TSNode *kids = perl_collect_children(node, nc);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode w = kids ? kids[i] : ts_node_child(node, i);
            if (ts_node_is_null(w) || !ts_node_is_named(w))
                continue;
            char *pw = perl_node_text(ctx, w);
            if (pw && pw[0] && strcmp(pw, "-norequire") == 0)
                continue;
            if (pw && pw[0])
                perl_add_isa(ctx, child_pkg, pw);
        }
        free(kids);
        return;
    }
    /* list_expression / parenthesized: descend. */
    uint32_t nc = ts_node_child_count(node);
    TSNode *kids = perl_collect_children(node, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(node, i);
        if (!ts_node_is_null(c) && ts_node_is_named(c))
            perl_collect_parents(ctx, c, child_pkg, depth + 1);
    }
    free(kids);
}

/* Process a `use_statement`:
 *   use parent qw(Base);  / use parent 'Base';  → @ISA for current package
 *   use base   qw(Base);  / use base -norequire => 'Base';
 *   use Module qw(f1 f2); → Exporter import map (f1→Module::f1) */
static void perl_collect_use_statement(PerlLSPContext *ctx, TSNode node) {
    TSNode mod = ts_node_child_by_field_name(node, "module", 6);
    char *module_name = NULL;
    if (!ts_node_is_null(mod))
        module_name = perl_node_text(ctx, mod);
    if (!module_name || !module_name[0])
        return;

    bool is_parent = strcmp(module_name, "parent") == 0;
    bool is_base = strcmp(module_name, "base") == 0;

    if (is_parent || is_base) {
        const char *child_pkg = ctx->current_package_qn && ctx->current_package_qn[0]
                                    ? ctx->current_package_qn
                                    : "main";
        /* Parent package names appear as `use_statement` arguments — directly,
         * inside a `list_expression` (use parent -norequire, 'Base'), or in a
         * `quoted_word_list` (use parent qw(Base)). Scan every named child
         * except the leading `module` bareword (parent/base). */
        uint32_t nc = ts_node_child_count(node);
        TSNode *kids = perl_collect_children(node, nc);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = kids ? kids[i] : ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            /* Skip the module bareword itself (it equals "parent"/"base"). */
            if (ts_node_eq(c, mod))
                continue;
            perl_collect_parents(ctx, c, child_pkg, 0);
        }
        free(kids);
        return;
    }

    /* Generic Exporter import: use Module qw(f1 f2). */
    perl_collect_qw_imports(ctx, node, module_name);
}

/* Detect `our @ISA = (...)` / `@ISA = (...)` assignments, recording parents
 * for the current package. */
static void perl_collect_isa_assignment(PerlLSPContext *ctx, TSNode assign) {
    TSNode left = ts_node_child_by_field_name(assign, "left", 4);
    if (ts_node_is_null(left))
        return;
    TSNode lhs = perl_decl_target(left);
    char *ltxt = perl_node_text(ctx, lhs);
    if (!ltxt)
        return;
    const char *bare = perl_strip_sigil(ltxt);
    /* Match @ISA (bare) and qualified Pkg::ISA forms. */
    if (!bare)
        return;
    const char *tail = strstr(bare, "ISA");
    bool is_isa = (strcmp(bare, "ISA") == 0) ||
                  (tail && strcmp(tail, "ISA") == 0 && tail > bare && *(tail - 1) == ':');
    if (!is_isa)
        return;

    const char *child_pkg =
        ctx->current_package_qn && ctx->current_package_qn[0] ? ctx->current_package_qn : "main";

    /* Parents may be a quoted_word_list, a list_expression of string literals,
     * or a bare string literal — perl_collect_parents handles all of these.
     *
     * tree-sitter-perl flattens a parenthesized RHS (e.g. `= ('Base')`) so the
     * assignment's `right` field points at the `(` token while the parent
     * string literals are *sibling* children of the assignment. Relying on the
     * `right` field alone therefore misses `@ISA = ('Base')`. Instead, scan
     * every named child after the `=`, which covers both `@ISA = 'Base'` and
     * `@ISA = ('Base', 'Other')`. perl_collect_parents ignores the LHS
     * variable_declaration and the `parent`/`base`/`-norequire` barewords, so
     * scanning the RHS children is safe. */
    bool seen_eq = false;
    uint32_t nc = ts_node_child_count(assign);
    TSNode *kids = perl_collect_children(assign, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(assign, i);
        if (ts_node_is_null(c))
            continue;
        if (!ts_node_is_named(c)) {
            if (strcmp(ts_node_type(c), "=") == 0)
                seen_eq = true;
            continue;
        }
        /* Only collect from RHS children (after `=`); skip the LHS @ISA decl. */
        if (!seen_eq)
            continue;
        perl_collect_parents(ctx, c, child_pkg, 0);
    }
    free(kids);
}

/* Recursively scan (PASS 1) for package context, @ISA assignments, and `use`
 * statements. */
/* Depth-guarded entry (see perl_resolve_calls_in_node for the rationale). */
static void perl_pass1_scan(PerlLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= CBM_LSP_PERL_MAX_WALK_DEPTH)
        return;
    ctx->walk_depth++;
    perl_pass1_scan_inner(ctx, node);
    ctx->walk_depth--;
}

static void perl_pass1_scan_inner(PerlLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *k = ts_node_type(node);
    if (strcmp(k, "package_statement") == 0) {
        process_package_decl(ctx, node);
        /* Fall through: a block-scoped package's body follows as children. */
    } else if (strcmp(k, "use_statement") == 0) {
        perl_collect_use_statement(ctx, node);
        return;
    } else if (strcmp(k, "assignment_expression") == 0) {
        perl_collect_isa_assignment(ctx, node);
    }
    uint32_t nc = ts_node_child_count(node);
    TSNode *kids = perl_collect_children(node, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(node, i);
        if (!ts_node_is_null(c))
            perl_pass1_scan(ctx, c);
    }
    free(kids);
}

/* ── process_file: two-pass walk ────────────────────────────────── */

void perl_lsp_process_file(PerlLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;

    /* PASS 1: collect package context, @ISA inheritance, Exporter imports.
     * Reset the per-file maps first so this is idempotent even when a caller
     * (cbm_run_perl_lsp) has already run a pre-pass to build registry types. */
    ctx->current_package_qn = "";
    ctx->enclosing_package_qn = "";
    ctx->use_count = 0;
    ctx->isa_count = 0;
    perl_pass1_scan(ctx, root);

    /* PASS 2: walk subs in package order; resolve + emit call edges. */
    ctx->current_package_qn = "";
    ctx->enclosing_package_qn = "";
    uint32_t nc = ts_node_child_count(root);
    TSNode *kids = perl_collect_children(root, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(root, i);
        if (ts_node_is_null(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "package_statement") == 0) {
            process_package_decl(ctx, c);
            /* Walk the (possibly block-scoped) package body for nested subs. */
            uint32_t bn = ts_node_child_count(c);
            TSNode *bkids = perl_collect_children(c, bn);
            for (uint32_t bi = 0; bi < bn; bi++) {
                TSNode bc = bkids ? bkids[bi] : ts_node_child(c, bi);
                if (!ts_node_is_null(bc) && ts_node_is_named(bc))
                    perl_resolve_calls_in_node(ctx, bc);
            }
            free(bkids);
        } else if (strcmp(k, "subroutine_declaration_statement") == 0 ||
                   strcmp(k, "method_declaration_statement") == 0) {
            process_subroutine(ctx, c);
        } else {
            /* Top-level statements: walk for nested subs / block packages.
             * Edges outside an enclosing sub are suppressed (no caller QN). */
            perl_resolve_calls_in_node(ctx, c);
        }
    }
    free(kids);
}

/* ── registry: per-package types + method tables ────────────────── */

/* Register a per-package CBMRegisteredType for every package that participates
 * in @ISA (as child or parent), then attach @ISA parents (embedded_types). */
static void perl_register_packages(PerlLSPContext *ctx, CBMTypeRegistry *reg) {
    for (int i = 0; i < ctx->isa_count; i++) {
        const char *names[2] = {ctx->isa_pkg_qns[i], ctx->isa_parent_qns[i]};
        for (int s = 0; s < 2; s++) {
            const char *pkg = names[s];
            if (!pkg || !pkg[0] || cbm_registry_lookup_type(reg, pkg))
                continue;
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = cbm_arena_strdup(ctx->arena, pkg);
            rt.short_name = rt.qualified_name;
            cbm_registry_add_type(reg, rt);
        }
    }

    /* Attach @ISA parents (embedded_types) to each child package type. */
    for (int t = 0; t < reg->type_count; t++) {
        CBMRegisteredType *rt = &reg->types[t];
        if (!rt->qualified_name)
            continue;
        int pc = 0;
        for (int i = 0; i < ctx->isa_count; i++) {
            if (strcmp(ctx->isa_pkg_qns[i], rt->qualified_name) == 0)
                pc++;
        }
        if (pc == 0)
            continue;
        const char **parents =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)(pc + 1) * sizeof(char *));
        if (!parents)
            continue;
        int w = 0;
        for (int i = 0; i < ctx->isa_count; i++) {
            if (strcmp(ctx->isa_pkg_qns[i], rt->qualified_name) == 0)
                parents[w++] = ctx->isa_parent_qns[i];
        }
        parents[w] = NULL;
        rt->embedded_types = parents;
    }
}

/* One collected (package, short-name, sub-QN) mapping for the batch method-table
 * build. All three strings are arena-owned (they outlive the transient vector),
 * so the vector itself is a plain malloc'd scratch buffer freed in
 * perl_attach_methods. `order` is the source-encounter index — a stable
 * tiebreak so sorting by package preserves source order within a package
 * (first-defined wins on a same-name redefinition, matching the old
 * append-in-order behavior). */
typedef struct {
    const char *pkg;
    const char *short_name;
    const char *sub_qn;
    int order;
} PerlMethodEnt;

typedef struct {
    PerlMethodEnt *v;
    int cnt;
    int cap;
    bool oom;
} PerlMethodVec;

/* Append a mapping. Geometric growth → O(1) amortized (the old per-sub
 * perl_type_add_method rebuilt each package's whole method array on every add,
 * which is O(methods^2) on a wide flat single-package file). On OOM the vector
 * latches `oom` and drops further mappings: their method calls simply stay
 * unresolved (graceful degradation, never a wrong edge). */
static void perl_mvec_push(PerlMethodVec *mv, const char *pkg, const char *short_name,
                           const char *sub_qn) {
    if (mv->oom)
        return;
    if (mv->cnt == mv->cap) {
        int ncap = mv->cap ? mv->cap * 2 : 32;
        PerlMethodEnt *nv = (PerlMethodEnt *)realloc(mv->v, (size_t)ncap * sizeof(PerlMethodEnt));
        if (!nv) {
            mv->oom = true;
            return;
        }
        mv->v = nv;
        mv->cap = ncap;
    }
    PerlMethodEnt *e = &mv->v[mv->cnt];
    e->pkg = pkg;
    e->short_name = short_name;
    e->sub_qn = sub_qn;
    e->order = mv->cnt;
    mv->cnt++;
}

/* Sort key: package name, then source order within a package. */
static int perl_method_ent_cmp(const void *a, const void *b) {
    const PerlMethodEnt *ea = (const PerlMethodEnt *)a;
    const PerlMethodEnt *eb = (const PerlMethodEnt *)b;
    int c = strcmp(ea->pkg, eb->pkg);
    if (c != 0)
        return c;
    return ea->order - eb->order;
}

/* Build (or extend) a package type's method tables from a contiguous run of
 * `n` same-package mappings — one allocation for the run, not one per method.
 * Creating/finding the type is O(type_count) but happens once per DISTINCT
 * package, not once per sub. */
static void perl_type_set_methods(PerlLSPContext *ctx, CBMTypeRegistry *reg, const char *pkg,
                                  const PerlMethodEnt *ents, int n) {
    CBMRegisteredType *rt = NULL;
    for (int t = 0; t < reg->type_count; t++) {
        if (reg->types[t].qualified_name && strcmp(reg->types[t].qualified_name, pkg) == 0) {
            rt = &reg->types[t];
            break;
        }
    }
    if (!rt) {
        CBMRegisteredType nt;
        memset(&nt, 0, sizeof(nt));
        nt.qualified_name = cbm_arena_strdup(ctx->arena, pkg);
        nt.short_name = nt.qualified_name;
        cbm_registry_add_type(reg, nt);
        if (reg->type_count == 0)
            return; /* add failed (OOM) */
        rt = &reg->types[reg->type_count - 1];
    }

    int existing = 0;
    if (rt->method_names)
        while (rt->method_names[existing])
            existing++;
    int total = existing + n;
    const char **mn =
        (const char **)cbm_arena_alloc(ctx->arena, (size_t)(total + 1) * sizeof(char *));
    const char **mq =
        (const char **)cbm_arena_alloc(ctx->arena, (size_t)(total + 1) * sizeof(char *));
    if (!mn || !mq)
        return;
    for (int j = 0; j < existing; j++) {
        mn[j] = rt->method_names[j];
        mq[j] = rt->method_qns[j];
    }
    for (int j = 0; j < n; j++) {
        mn[existing + j] = ents[j].short_name;
        mq[existing + j] = ents[j].sub_qn;
    }
    mn[total] = NULL;
    mq[total] = NULL;
    rt->method_names = mn;
    rt->method_qns = mq;
}

/* Walk the top level mapping each sub to its enclosing package, registering the
 * sub's QN in that package's method table so method dispatch finds it. */
static void perl_attach_methods(PerlLSPContext *ctx, CBMTypeRegistry *reg, TSNode root) {
    const char *cur_pkg = "main";
    PerlMethodVec mv;
    memset(&mv, 0, sizeof(mv));
    uint32_t nc = ts_node_child_count(root);
    TSNode *kids = perl_collect_children(root, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(root, i);
        if (ts_node_is_null(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "package_statement") == 0) {
            TSNode name = ts_node_child_by_field_name(c, "name", 4);
            if (ts_node_is_null(name))
                name = perl_first_child_of_type(c, "package");
            if (!ts_node_is_null(name)) {
                char *p = perl_node_text(ctx, name);
                if (p && p[0])
                    cur_pkg = cbm_arena_strdup(ctx->arena, p);
            }
            /* Block-scoped package body: subs are nested children. */
            uint32_t bn = ts_node_child_count(c);
            TSNode *bkids = perl_collect_children(c, bn);
            for (uint32_t bi = 0; bi < bn; bi++) {
                TSNode bc = bkids ? bkids[bi] : ts_node_child(c, bi);
                if (ts_node_is_null(bc) || !ts_node_is_named(bc))
                    continue;
                if (strcmp(ts_node_type(bc), "subroutine_declaration_statement") != 0 &&
                    strcmp(ts_node_type(bc), "method_declaration_statement") != 0)
                    continue;
                TSNode bname = ts_node_child_by_field_name(bc, "name", 4);
                if (ts_node_is_null(bname))
                    continue;
                char *bsn = perl_node_text(ctx, bname);
                if (!bsn || !bsn[0])
                    continue;
                const char *bqn = ctx->module_qn
                                      ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, bsn)
                                      : cbm_arena_strdup(ctx->arena, bsn);
                perl_mvec_push(&mv, cur_pkg, bsn, bqn);
            }
            free(bkids);
            continue;
        }
        if (strcmp(k, "subroutine_declaration_statement") != 0 &&
            strcmp(k, "method_declaration_statement") != 0)
            continue;

        TSNode name = ts_node_child_by_field_name(c, "name", 4);
        if (ts_node_is_null(name))
            continue;
        char *sname = perl_node_text(ctx, name);
        if (!sname || !sname[0])
            continue;
        const char *sub_qn = ctx->module_qn
                                 ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, sname)
                                 : cbm_arena_strdup(ctx->arena, sname);
        perl_mvec_push(&mv, cur_pkg, sname, sub_qn);
    }
    free(kids);

    /* Build each package's method table once from the collected mappings:
     * sort by package (source order preserved within a package), then set each
     * contiguous same-package run in a single allocation. */
    if (mv.cnt > 0 && mv.v) {
        qsort(mv.v, (size_t)mv.cnt, sizeof(PerlMethodEnt), perl_method_ent_cmp);
        int s = 0;
        while (s < mv.cnt) {
            int e = s + 1;
            while (e < mv.cnt && strcmp(mv.v[e].pkg, mv.v[s].pkg) == 0)
                e++;
            perl_type_set_methods(ctx, reg, mv.v[s].pkg, &mv.v[s], e - s);
            s = e;
        }
    }
    free(mv.v);
}

/* ── entry: cbm_run_perl_lsp ────────────────────────────────────── */

void cbm_run_perl_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                      TSNode root) {
    if (!result || !arena || ts_node_is_null(root))
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    /* Phase A: register stdlib types/functions (perlfunc + curated CPAN). */
    cbm_perl_stdlib_register(&reg, arena);

    const char *module_qn = result->module_qn;

    /* Phase B: register file-local subs (label Function/Method). Return types
     * are unknown — Perl has no declared types; v1 infers via bless/new at the
     * call site, not from declarations. */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name || !d->label)
            continue;
        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;
            if (strcmp(d->label, "Method") == 0 && d->parent_class)
                rf.receiver_type = d->parent_class;
            const CBMType **rets =
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
            if (rets) {
                rets[0] = cbm_type_unknown();
                rets[1] = NULL;
            }
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);
            cbm_registry_add_func(&reg, rf);
        }
    }

    /* Phase B.1: pre-pass over the AST to populate the inheritance + import
     * maps and build per-package types + method tables. This must happen
     * before resolution (PASS 2) so method dispatch can walk @ISA. The
     * mutable `reg` lives here; perl_lsp_process_file later runs on the
     * finished (const) registry. */
    PerlLSPContext ctx;
    perl_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, &result->resolved_calls);

    ctx.current_package_qn = "";
    ctx.enclosing_package_qn = "";
    perl_pass1_scan(&ctx, root);
    perl_register_packages(&ctx, &reg);
    perl_attach_methods(&ctx, &reg, root);

    /* Finalize the registry for O(1) lookups during resolution — mirrors
     * php_lsp/java_lsp. Must come AFTER all registry mutations (stdlib, file
     * defs, packages, methods) and BEFORE resolution. reg's arena is the
     * pipeline-lifetime result arena, so per-file bucket allocations go to a
     * per-call scratch arena that dies with this call rather than accumulating
     * across a large repo. */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    cbm_registry_finalize_into(&reg, &idx_arena);

    /* Phase C: two-pass resolution walk (PASS 1 re-populates the per-file use
     * map + ISA context needed for the bless/$self idioms during PASS 2). */
    perl_lsp_process_file(&ctx, root);

    if (ctx.debug) {
        fprintf(stderr, "[perl_lsp] module_qn=%s defs=%d resolved=%d isa=%d types=%d\n",
                module_qn ? module_qn : "(null)", result->defs.count, result->resolved_calls.count,
                ctx.isa_count, reg.type_count);
        for (int i = 0; i < result->resolved_calls.count; i++) {
            CBMResolvedCall *r = &result->resolved_calls.items[i];
            fprintf(stderr, "[perl_lsp]   %s -> %s [%s %.2f]\n", r->caller_qn, r->callee_qn,
                    r->strategy, r->confidence);
        }
    }

    cbm_arena_destroy(&idx_arena);
}
