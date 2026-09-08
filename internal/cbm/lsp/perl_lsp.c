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
static const char *perl_func_return_class_qn(PerlLSPContext *ctx, const CBMRegisteredFunc *impf,
                                             const char *rtn);
static void perl_emit_resolved(PerlLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence, TSNode site);
static void perl_resolve_direct_coderef_arguments(PerlLSPContext *ctx, TSNode call);

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

/* Return-type inference stores multi-segment class spellings DOTTED
 * ("Mojo.File", perl_infer_return_types converts `::`→`.`), but the cross-file
 * used-module type table is keyed by the module name AS WRITTEN in the `use`
 * statement (colons, "Mojo::File"; perl_scan_used_modules) and
 * cbm_registry_lookup_type is exact-match. So a dotted multi-segment return
 * type never finds its colon-keyed method table — every `$obj->accessor->method`
 * chain whose accessor returns a multi-segment class silently fails (single-
 * segment "Widget" is dot==colon and masked the bug in fixtures). This yields
 * the colon variant of a dotted class QN so the typed-receiver lookup can retry.
 * NULL when there is no '.' to convert. */
static char *perl_class_qn_colon_variant(CBMArena *arena, const char *qn) {
    if (!qn || !strchr(qn, '.'))
        return NULL;
    size_t n = strlen(qn);
    char *out = (char *)cbm_arena_alloc(arena, n * 2 + 1);
    if (!out)
        return NULL;
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        if (qn[i] == '.') {
            out[w++] = ':';
            out[w++] = ':';
        } else {
            out[w++] = qn[i];
        }
    }
    out[w] = '\0';
    return out;
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

/* Grow-and-append for the paired (key, value) string tables added for
 * cross-file + Moose support. Returns false on OOM (entry dropped — graceful
 * degradation, the affected lookups just stay unresolved). */
static bool perl_pair_push(CBMArena *arena, const char ***keys, const char ***vals, int *count,
                           int *cap, const char *key, const char *val) {
    if (!arena || !keys || !vals || !key)
        return false;
    if (*count >= *cap) {
        int newcap = *cap ? *cap * 2 : 8;
        const char **nk = (const char **)cbm_arena_alloc(arena, (size_t)newcap * sizeof(char *));
        const char **nv = (const char **)cbm_arena_alloc(arena, (size_t)newcap * sizeof(char *));
        if (!nk || !nv)
            return false;
        for (int i = 0; i < *count; i++) {
            nk[i] = (*keys)[i];
            nv[i] = (*vals)[i];
        }
        *keys = nk;
        *vals = nv;
        *cap = newcap;
    }
    (*keys)[*count] = cbm_arena_strdup(arena, key);
    (*vals)[*count] = val ? cbm_arena_strdup(arena, val) : NULL;
    (*count)++;
    return true;
}

/* Cross-file package→module map lookup: "My::Util" → "test.lib.My.Util", or
 * NULL when the package has no resolved project module. */
static const char *perl_xmod_lookup(PerlLSPContext *ctx, const char *pkg) {
    if (!ctx || !pkg)
        return NULL;
    for (int i = 0; i < ctx->xmod_count; i++) {
        if (ctx->xmod_pkgs[i] && strcmp(ctx->xmod_pkgs[i], pkg) == 0)
            return ctx->xmod_qns[i];
    }
    return NULL;
}

/* Default-export ("@EXPORT") list for a resolved module QN, or NULL. */
static const char *perl_xexp_lookup(PerlLSPContext *ctx, const char *module_qn) {
    if (!ctx || !module_qn)
        return NULL;
    for (int i = 0; i < ctx->xexp_count; i++) {
        if (ctx->xexp_module_qns[i] && strcmp(ctx->xexp_module_qns[i], module_qn) == 0)
            return ctx->xexp_names[i];
    }
    return NULL;
}

/* ── Moose/Moo per-package mode + attribute tables ──────────────── */

static bool perl_pkg_is_moose(PerlLSPContext *ctx, const char *pkg) {
    if (!ctx || !pkg)
        return false;
    for (int i = 0; i < ctx->moose_pkg_count; i++) {
        if (ctx->moose_pkgs[i] && strcmp(ctx->moose_pkgs[i], pkg) == 0)
            return true;
    }
    return false;
}

static void perl_mark_moose_pkg(PerlLSPContext *ctx, const char *pkg) {
    if (!ctx || !pkg || !pkg[0] || perl_pkg_is_moose(ctx, pkg))
        return;
    if (ctx->moose_pkg_count >= ctx->moose_pkg_cap) {
        int newcap = ctx->moose_pkg_cap ? ctx->moose_pkg_cap * 2 : 4;
        const char **np =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(char *));
        if (!np)
            return;
        for (int i = 0; i < ctx->moose_pkg_count; i++)
            np[i] = ctx->moose_pkgs[i];
        ctx->moose_pkgs = np;
        ctx->moose_pkg_cap = newcap;
    }
    ctx->moose_pkgs[ctx->moose_pkg_count++] = cbm_arena_strdup(ctx->arena, pkg);
}

/* Record one Moose attribute (pkg, name, isa-or-NULL). `has '+attr'`
 * overrides an inherited attr: strip the '+' and do not mint a new name. */
static void perl_add_attr(PerlLSPContext *ctx, const char *pkg, const char *name,
                          const char *isa) {
    if (!ctx || !pkg || !name || !name[0])
        return;
    if (name[0] == '+')
        name++;
    if (!name[0])
        return;
    if (ctx->attr_count >= ctx->attr_cap) {
        int newcap = ctx->attr_cap ? ctx->attr_cap * 2 : 8;
        const char **np = (const char **)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(char *));
        const char **nn = (const char **)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(char *));
        const char **ni = (const char **)cbm_arena_alloc(ctx->arena, (size_t)newcap * sizeof(char *));
        if (!np || !nn || !ni)
            return;
        for (int i = 0; i < ctx->attr_count; i++) {
            np[i] = ctx->attr_pkgs[i];
            nn[i] = ctx->attr_names[i];
            ni[i] = ctx->attr_isa[i];
        }
        ctx->attr_pkgs = np;
        ctx->attr_names = nn;
        ctx->attr_isa = ni;
        ctx->attr_cap = newcap;
    }
    ctx->attr_pkgs[ctx->attr_count] = cbm_arena_strdup(ctx->arena, pkg);
    ctx->attr_names[ctx->attr_count] = cbm_arena_strdup(ctx->arena, name);
    ctx->attr_isa[ctx->attr_count] = isa && isa[0] ? cbm_arena_strdup(ctx->arena, isa) : NULL;
    ctx->attr_count++;
}

/* Attribute lookup on `pkg` and (Moose attrs inherit) its recorded parents.
 * Returns the isa type name, "" when the attr exists with unknown type, or
 * NULL when no such attribute is recorded. Bounded parent walk. */
static const char *perl_lookup_attr_isa(PerlLSPContext *ctx, const char *pkg,
                                        const char *attr_name) {
    if (!ctx || !pkg || !attr_name)
        return NULL;
    enum { CAP = CBM_LSP_MAX_LOOKUP_DEPTH * 2 };
    const char *frontier[CAP];
    int fc = 0;
    const char *visited[CAP];
    int vc = 0;
    frontier[fc++] = pkg;
    while (fc > 0 && vc < CAP) {
        const char *cur = frontier[--fc];
        bool seen = false;
        for (int v = 0; v < vc; v++) {
            if (strcmp(visited[v], cur) == 0) {
                seen = true;
                break;
            }
        }
        if (seen)
            continue;
        visited[vc++] = cur;
        for (int i = 0; i < ctx->attr_count; i++) {
            if (strcmp(ctx->attr_pkgs[i], cur) == 0 && strcmp(ctx->attr_names[i], attr_name) == 0)
                return ctx->attr_isa[i] ? ctx->attr_isa[i] : "";
        }
        for (int i = 0; i < ctx->isa_count && fc < CAP; i++) {
            if (strcmp(ctx->isa_pkg_qns[i], cur) == 0)
                frontier[fc++] = ctx->isa_parent_qns[i];
        }
    }
    return NULL;
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
    } else if (strcmp(k, "bareword") == 0) {
        /* A lowercase bareword that is an Exporter-imported function called with
         * NO parens/args — `my $dir = tempdir;` (tempdir/curfile/path factory).
         * tree-sitter parses the arg-less call as a plain bareword, so it never
         * reached perl_eval_function_call_type; type it from the function's
         * return type (resolving a `__PACKAGE__` factory return to its package)
         * so the bound var chains (`$dir->child`). Lowercase + import-map hit
         * distinguishes a function from a class name (zero-edge otherwise). */
        char *txt = perl_node_text(ctx, node);
        if (txt && txt[0] >= 'a' && txt[0] <= 'z') {
            const char *imp = perl_find_import(ctx, txt);
            const CBMRegisteredFunc *f = imp ? cbm_registry_lookup_func(ctx->registry, imp) : NULL;
            if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC &&
                f->signature->data.func.return_types && f->signature->data.func.return_types[0]) {
                const CBMType *rt = f->signature->data.func.return_types[0];
                if (rt->kind == CBM_TYPE_NAMED) {
                    const char *cq =
                        perl_func_return_class_qn(ctx, f, rt->data.named.qualified_name);
                    if (cq)
                        result = cbm_type_named(ctx->arena, cq);
                } else {
                    result = rt;
                }
            }
        }
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
        const CBMType *rt = f->signature->data.func.return_types[0];
        /* `func(...)->method` where func is a `__PACKAGE__->new` factory
         * (Mojo::File's `sub path { __PACKAGE__->new(@_) }`, curfile): resolve
         * the literal "__PACKAGE__" to func's own package so the chained method
         * dispatches — mirrors the bareword `func->method` path. */
        if (rt->kind == CBM_TYPE_NAMED) {
            const char *cq = perl_func_return_class_qn(ctx, f, rt->data.named.qualified_name);
            if (cq)
                return cbm_type_named(ctx->arena, cq);
            return cbm_type_unknown();
        }
        return rt;
    }
    return cbm_type_unknown();
}

/* Resolve a resolved-function's stored return-type spelling into a receiver
 * class QN usable by perl_lookup_method. A literal "__PACKAGE__" (from the
 * `sub f { __PACKAGE__->new(...) }` factory idiom — e.g. Mojo::File's curfile)
 * means the function returns its OWN package: map impf's package QN (its QN
 * minus the last segment) back through the xmod map to the colon-spelled module
 * name the cross-file used-module type table is keyed by, so the chained method
 * dispatches. Any other spelling passes through unchanged. NULL when a
 * __PACKAGE__ return can't be mapped (per-file mode has no xmod — the function
 * CALL edge is still emitted; only the chain is skipped). */
static const char *perl_func_return_class_qn(PerlLSPContext *ctx, const CBMRegisteredFunc *impf,
                                             const char *rtn) {
    if (!rtn)
        return NULL;
    if (strcmp(rtn, "__PACKAGE__") != 0)
        return rtn;
    if (!impf || !impf->qualified_name)
        return NULL;
    const char *dot = strrchr(impf->qualified_name, '.');
    if (!dot || dot == impf->qualified_name)
        return NULL;
    char *pkgqn =
        cbm_arena_strndup(ctx->arena, impf->qualified_name, (size_t)(dot - impf->qualified_name));
    if (!pkgqn)
        return NULL;
    for (int i = 0; i < ctx->xmod_count; i++) {
        if (ctx->xmod_qns[i] && strcmp(ctx->xmod_qns[i], pkgqn) == 0)
            return ctx->xmod_pkgs[i]; /* colon-spelled module name = used-module type key */
    }
    return NULL;
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
            /* `func->method` where func is an imported Exporter function: the
             * receiver's type is func's return type (mirrors the edge-emitting
             * path in perl_resolve_method_call). */
            const CBMRegisteredFunc *impf = NULL;
            if (cls && cls[0] >= 'a' && cls[0] <= 'z') {
                const char *imp = perl_find_import(ctx, cls);
                if (imp)
                    impf = cbm_registry_lookup_func(ctx->registry, imp);
            }
            const CBMType *rt =
                (impf && impf->signature && impf->signature->kind == CBM_TYPE_FUNC &&
                 impf->signature->data.func.return_types)
                    ? impf->signature->data.func.return_types[0]
                    : NULL;
            if (rt && rt->kind == CBM_TYPE_NAMED)
                class_qn = perl_func_return_class_qn(ctx, impf, rt->data.named.qualified_name);
            if (!class_qn && cls && cls[0])
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
    if (!f) {
        char *cv = perl_class_qn_colon_variant(ctx->arena, class_qn);
        if (cv)
            f = perl_lookup_method(ctx, cv, mname);
    }
    if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC &&
        f->signature->data.func.return_types && f->signature->data.func.return_types[0]) {
        return f->signature->data.func.return_types[0];
    }
    /* Moose/Moo synthetic accessor: `has engine => (isa => 'Engine')` makes
     * $self->engine return an Engine. TYPING ONLY — perl_resolve_method_call
     * still emits no edge for the accessor call itself (no indexed sub), but
     * the returned type lets the CHAINED call ($self->engine->start())
     * dispatch. Unknown/parameterized isa → unknown (zero-edge). */
    {
        const char *isa = perl_lookup_attr_isa(ctx, class_qn, mname);
        if (isa && isa[0])
            return cbm_type_named(ctx->arena, perl_resolve_package_name(ctx, isa));
    }
    return cbm_type_unknown();
}

/* ── emit ───────────────────────────────────────────────────────── */

static void perl_emit_resolved(PerlLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence, TSNode site) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
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

static void perl_emit_reference(PerlLSPContext *ctx, const char *callee_qn, TSNode site) {
    if (!ctx || !ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn ||
        ts_node_is_null(site)) {
        return;
    }
    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = "perl_coderef";
    rc.confidence = PERL_CONF_LITERAL;
    rc.kind = CBM_RESOLVED_CALL_REFERENCE;
    rc.site_start_byte = ts_node_start_byte(site);
    rc.site_end_byte = ts_node_end_byte(site);
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

/* Accept only the exact coderef spelling represented by one direct
 * refgen_expression. Conditional/list/map expressions are not examined, so
 * their constituent \&names remain ordinary usages. */
static char *perl_exact_coderef_name(PerlLSPContext *ctx, TSNode refgen) {
    if (ts_node_is_null(refgen) || strcmp(ts_node_type(refgen), "refgen_expression") != 0)
        return NULL;
    char *text = perl_node_text(ctx, refgen);
    if (!text)
        return NULL;
    const char *p = text;
    while (isspace((unsigned char)*p))
        p++;
    if (p[0] != '\\' || p[1] != '&')
        return NULL;
    p += 2;
    const char *start = p;
    if (!(isalpha((unsigned char)*p) || *p == '_'))
        return NULL;
    while (isalnum((unsigned char)*p) || *p == '_' || *p == ':')
        p++;
    const char *end = p;
    while (isspace((unsigned char)*p))
        p++;
    if (*p != '\0' || end == start)
        return NULL;
    return cbm_arena_strndup(ctx->arena, start, (size_t)(end - start));
}

static const CBMRegisteredFunc *perl_resolve_coderef_target(PerlLSPContext *ctx, char *name) {
    if (!ctx || !name || !name[0])
        return NULL;
    char *colons = NULL;
    for (char *p = strstr(name, "::"); p; p = strstr(p + 2, "::"))
        colons = p;
    if (colons) {
        char *pkg = cbm_arena_strndup(ctx->arena, name, (size_t)(colons - name));
        const char *short_name = colons + 2;
        const CBMRegisteredFunc *f = perl_lookup_method(ctx, pkg, short_name);
        return f ? f : cbm_registry_lookup_symbol(ctx->registry, pkg, short_name);
    }
    const char *import_qn = perl_find_import(ctx, name);
    if (import_qn) {
        const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, import_qn);
        if (f)
            return f;
    }
    return cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, name);
}

static void perl_resolve_one_coderef(PerlLSPContext *ctx, TSNode refgen) {
    char *name = perl_exact_coderef_name(ctx, refgen);
    const CBMRegisteredFunc *target = name ? perl_resolve_coderef_target(ctx, name) : NULL;
    if (target && target->qualified_name)
        perl_emit_reference(ctx, target->qualified_name, refgen);
}

static void perl_resolve_direct_coderef_arguments(PerlLSPContext *ctx, TSNode call) {
    TSNode args = ts_node_child_by_field_name(call, "arguments", 9);
    if (ts_node_is_null(args))
        return;
    if (strcmp(ts_node_type(args), "refgen_expression") == 0) {
        perl_resolve_one_coderef(ctx, args);
        return;
    }
    /* Multiple arguments are exposed through a flat list_expression. Do not
     * recurse into any other wrapper: in particular, a conditional argument
     * may contain refgen descendants but does not select one exact target. */
    if (strcmp(ts_node_type(args), "list_expression") != 0)
        return;
    uint32_t count = ts_node_named_child_count(args);
    for (uint32_t i = 0; i < count; i++) {
        TSNode arg = ts_node_named_child(args, i);
        if (strcmp(ts_node_type(arg), "refgen_expression") == 0)
            perl_resolve_one_coderef(ctx, arg);
    }
}

static bool perl_is_direct_coderef_argument(TSNode refgen) {
    TSNode container = ts_node_parent(refgen);
    if (ts_node_is_null(container))
        return false;
    if (strcmp(ts_node_type(container), "list_expression") != 0)
        container = refgen;
    TSNode call = ts_node_parent(container);
    if (ts_node_is_null(call))
        return false;
    const char *kind = ts_node_type(call);
    if (strcmp(kind, "function_call_expression") != 0 &&
        strcmp(kind, "ambiguous_function_call_expression") != 0) {
        return false;
    }
    TSNode args = ts_node_child_by_field_name(call, "arguments", 9);
    return !ts_node_is_null(args) && ts_node_eq(args, container);
}

static void perl_emit_non_direct_coderef_usage(PerlLSPContext *ctx, TSNode refgen) {
    if (!ctx || !ctx->usages || !ctx->enclosing_func_qn ||
        perl_is_direct_coderef_argument(refgen)) {
        return;
    }
    char *name = perl_exact_coderef_name(ctx, refgen);
    if (!name)
        return;
    uint32_t start = ts_node_start_byte(refgen);
    uint32_t end = ts_node_end_byte(refgen);
    for (int i = 0; i < ctx->usages->count; i++) {
        const CBMUsage *existing = &ctx->usages->items[i];
        if (existing->kind == CBM_USAGE_VALUE && existing->ref_name &&
            existing->enclosing_func_qn && strcmp(existing->ref_name, name) == 0 &&
            strcmp(existing->enclosing_func_qn, ctx->enclosing_func_qn) == 0 &&
            existing->site_start_byte == start && existing->site_end_byte == end) {
            return;
        }
    }
    CBMUsage usage = {0};
    usage.ref_name = name;
    usage.enclosing_func_qn = ctx->enclosing_func_qn;
    usage.kind = CBM_USAGE_VALUE;
    usage.site_start_byte = start;
    usage.site_end_byte = end;
    cbm_usages_push(ctx->usages, ctx->arena, usage);
}

/* ── call/method dispatch (emit edges) ──────────────────────────── */

/* Resolve a function/static call and emit an edge if it lands on a registered
 * sub. Bare func(), Exporter func(), and Package::func() static calls. */
static void perl_resolve_function_call(PerlLSPContext *ctx, TSNode call) {
    perl_resolve_direct_coderef_arguments(ctx, call);
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
            perl_emit_resolved(ctx, f->qualified_name, "perl_static_call", PERL_CONF_LITERAL, call);
            return;
        }
    } else {
        const char *imp = perl_find_import(ctx, name);
        if (imp) {
            f = cbm_registry_lookup_func(ctx->registry, imp);
            if (f) {
                perl_emit_resolved(ctx, f->qualified_name, "perl_imported_function",
                                   PERL_CONF_LITERAL, call);
                return;
            }
        }
        f = cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, name);
        if (f) {
            perl_emit_resolved(ctx, f->qualified_name, "perl_function_local", PERL_CONF_LITERAL,
                               call);
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
            perl_emit_resolved(ctx, sf->qualified_name, "perl_method_super", PERL_CONF_LITERAL,
                               call);
        return;
    }

    const char *class_qn = NULL;
    const char *strategy = "perl_method_typed";
    if (!ts_node_is_null(inv)) {
        const char *ik = ts_node_type(inv);
        if (perl_is_bareword_node(ik)) {
            char *cls = perl_node_text(ctx, inv);
            /* A lowercase bareword invocant that is an Exporter-imported function
             * (`use Mod qw(func)`) is a FUNCTION CALL used as `func->method`
             * (e.g. Mojo::File's `curfile->sibling(...)`), NOT a class name.
             * Perl spells classes CamelCase and functions lowercase, and the
             * import map only holds Exporter functions, so the two signals
             * together are unambiguous. Emit the call edge to the function, then
             * type the receiver from the function's return type so the chained
             * method dispatches. */
            const CBMRegisteredFunc *impf = NULL;
            if (cls && cls[0] >= 'a' && cls[0] <= 'z') {
                const char *imp = perl_find_import(ctx, cls);
                if (imp)
                    impf = cbm_registry_lookup_func(ctx->registry, imp);
            }
            if (impf) {
                perl_emit_resolved(ctx, impf->qualified_name, "perl_imported_function",
                                   PERL_CONF_LITERAL, inv);
                const CBMType *rt =
                    (impf->signature && impf->signature->kind == CBM_TYPE_FUNC &&
                     impf->signature->data.func.return_types)
                        ? impf->signature->data.func.return_types[0]
                        : NULL;
                if (rt && rt->kind == CBM_TYPE_NAMED)
                    class_qn = perl_func_return_class_qn(ctx, impf, rt->data.named.qualified_name);
                strategy = "perl_method_typed";
            } else {
                if (cls && cls[0])
                    class_qn = perl_resolve_package_name(ctx, cls);
                strategy = "perl_method_static";
            }
        } else {
            const CBMType *recv = perl_eval_expr_type(ctx, inv);
            if (recv && recv->kind == CBM_TYPE_NAMED) {
                class_qn = recv->data.named.qualified_name;
                strategy = "perl_method_typed";
            }
        }
    }
    if (!class_qn)
        return; /* unknown receiver — zero-edge guarantee (call edge, if any, already emitted) */

    const CBMRegisteredFunc *f = perl_lookup_method(ctx, class_qn, mname);
    if (!f) {
        char *cv = perl_class_qn_colon_variant(ctx->arena, class_qn);
        if (cv) {
            f = perl_lookup_method(ctx, cv, mname);
            if (f)
                class_qn = cv; /* the spelling that actually matched */
        }
    }
    if (f) {
        const char *strat = (f->receiver_type && strcmp(f->receiver_type, class_qn) == 0)
                                ? strategy
                                : "perl_method_inherited";
        perl_emit_resolved(ctx, f->qualified_name, strat, PERL_CONF_LITERAL, call);
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

    if (strcmp(k, "refgen_expression") == 0)
        perl_emit_non_direct_coderef_usage(ctx, node);

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

/* True when the text names a conventional invocant variable. The signature and
 * list-unpack forms are name-gated to $self/$class so a plain function's first
 * parameter never gains a spurious package type (zero-edge guarantee). */
static bool perl_is_invocant_name(const char *txt) {
    return txt && (strcmp(txt, "$self") == 0 || strcmp(txt, "$class") == 0);
}

/* First scalar descendant, caps on depth and per-level breadth: unwraps the
 * paren list in `my ($self, $x)` and the parameter wrapper in a signature. The
 * invocant is always leftmost, so the leftmost-first search returns after O(1)
 * nodes on real code; the caps bound pathological LHS shapes. */
static TSNode perl_first_scalar_desc(TSNode node, int depth) {
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    if (ts_node_is_null(node) || depth > 3)
        return null_node;
    const char *k = ts_node_type(node);
    if (strcmp(k, "scalar") == 0 || strcmp(k, "scalar_variable") == 0)
        return node;
    uint32_t nc = ts_node_named_child_count(node);
    if (nc > 8)
        nc = 8;
    for (uint32_t i = 0; i < nc; i++) {
        TSNode r = perl_first_scalar_desc(ts_node_named_child(node, i), depth + 1);
        if (!ts_node_is_null(r))
            return r;
    }
    return null_node;
}

/* Bind the invocant: in a method sub belonging to package P, the first
 * statement is typically `my $self = shift;` or `my $class = shift;`. Bind the
 * first such scalar to type P so $self->method() / $class->method() dispatch.
 * Also handles the classic list unpack `my ($self, $x) = @_;` (name-gated). */
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
        if (strcmp(lvk, "scalar") != 0 && strcmp(lvk, "scalar_variable") != 0) {
            /* List unpack `my ($self, $x) = @_;`: the invocant is the FIRST
             * scalar of the paren list when the whole RHS is @_. */
            char *lrtxt = perl_node_text(ctx, right);
            if (lrtxt && strcmp(lrtxt, "@_") == 0) {
                bool bound = false;
                TSNode sc = perl_first_scalar_desc(lhs_var, 0);
                char *vtxt = ts_node_is_null(sc) ? NULL : perl_node_text(ctx, sc);
                if (perl_is_invocant_name(vtxt)) {
                    const char *lbare = perl_strip_sigil(vtxt);
                    if (lbare && lbare[0]) {
                        cbm_scope_bind(ctx->current_scope, lbare, cbm_type_named(ctx->arena, pkg));
                        bound = true;
                    }
                }
                /* Mojolicious framework convention: inside a Mojolicious::* module
                 * a positional `$c` (ANY position, not just the 2nd) is the
                 * controller passed to a dispatch/render/route method or an
                 * around/hook callback, so `$c->render/stash/param/...` dispatches
                 * through Mojolicious::Controller (seeded into the chain-walk).
                 * Handles both `my ($self, $c) = @_` and `my ($next, $c) = @_`
                 * (around_action / before_dispatch, first positional is the
                 * continuation). Gated to the framework path — user code uses the
                 * signature form (perl_bind_routing_controller_param). The exact
                 * name `$c` is the strong Mojolicious convention keeping this
                 * zero-noise. */
                if (ctx->module_qn && strstr(ctx->module_qn, "Mojolicious")) {
                    uint32_t pn = ts_node_named_child_count(lhs_var);
                    for (uint32_t j = 0; j < pn && j < 8; j++) {
                        TSNode pv = perl_first_scalar_desc(ts_node_named_child(lhs_var, j), 0);
                        char *pt = ts_node_is_null(pv) ? NULL : perl_node_text(ctx, pv);
                        const char *pb = pt ? perl_strip_sigil(pt) : NULL;
                        if (pb && strcmp(pb, "c") == 0) {
                            cbm_scope_bind(ctx->current_scope, "c",
                                           cbm_type_named(ctx->arena, "Mojolicious::Controller"));
                            bound = true;
                            break;
                        }
                    }
                }
                if (bound) {
                    free(kids);
                    return; /* invocant / controller binding done */
                }
            }
            continue;
        }

        /* RHS must reference the invocant idiom (`shift` / `shift @_` / `$_[0]`). */
        char *rtxt = perl_node_text(ctx, right);
        if (!perl_rhs_is_invocant(rtxt))
            continue;

        char *vtxt = perl_node_text(ctx, lhs_var);
        if (!vtxt)
            continue;
        const char *bare = perl_strip_sigil(vtxt);
        if (bare && bare[0]) {
            /* Mojolicious `$c` convention: `my $c = shift` in a helper/hook
             * callback or controller action is the controller — NOT the
             * enclosing package, which for a helper callback (`$app->helper(x =>
             * sub { my $c = shift })`) is the plugin. Bind $c to
             * Mojolicious::Controller (chain-walk-seeded only in Mojo files, so
             * inert elsewhere — no false edges); every other invocant name binds
             * to the enclosing package as usual. */
            if (strcmp(bare, "c") == 0)
                cbm_scope_bind(ctx->current_scope, "c",
                               cbm_type_named(ctx->arena, "Mojolicious::Controller"));
            else
                cbm_scope_bind(ctx->current_scope, bare, cbm_type_named(ctx->arena, pkg));
        }
        free(kids);
        return; /* only the first invocant binding */
    }
    free(kids);
}

/* Modern signature form (`sub render ($self, $x) {...}`, stable since 5.36):
 * bind a leading $self/$class parameter to the enclosing package so the method
 * body dispatches. Other first parameters stay untyped (name gate). */
static void perl_bind_signature_invocant(PerlLSPContext *ctx, TSNode sub_node) {
    const char *pkg =
        ctx->enclosing_package_qn && ctx->enclosing_package_qn[0] ? ctx->enclosing_package_qn
                                                                  : ctx->current_package_qn;
    if (!pkg || !pkg[0])
        return;
    TSNode sig = perl_first_child_of_type(sub_node, "signature");
    if (ts_node_is_null(sig))
        return;
    TSNode first = ts_node_named_child(sig, 0);
    if (ts_node_is_null(first))
        return;
    /* Optional parameters carry defaults (`$x = 1`) whose scalar would pass the
     * name compare below; an optional invocant is nonsense, so gate on the
     * mandatory/bare forms only. */
    const char *fk = ts_node_type(first);
    if (strcmp(fk, "mandatory_parameter") != 0 && strcmp(fk, "scalar") != 0 &&
        strcmp(fk, "scalar_variable") != 0)
        return;
    TSNode sc = perl_first_scalar_desc(first, 0);
    char *ptxt = ts_node_is_null(sc) ? NULL : perl_node_text(ctx, sc);
    if (!perl_is_invocant_name(ptxt))
        return;
    const char *bare = perl_strip_sigil(ptxt);
    if (bare && bare[0])
        cbm_scope_bind(ctx->current_scope, bare, cbm_type_named(ctx->arena, pkg));
}

/* Mojolicious routing / hook DSL methods whose handler callback receives a
 * Mojolicious::Controller as `$c`. Both the function form (Mojolicious::Lite:
 * `get '/x' => sub ($c) {...}`) and the method form (`$r->under(...)->to(cb =>
 * sub ($c) {...})`, `$app->hook(before_dispatch => sub ($c) {...})`) route here. */
static bool perl_is_mojo_routing_method(const char *name) {
    if (!name || !name[0])
        return false;
    static const char *const kRoutes[] = {"get",   "post",      "put",  "del",   "delete",
                                          "patch", "options",   "any",  "under", "to",
                                          "websocket", "hook",   "group", "route", NULL};
    for (int i = 0; kRoutes[i]; i++)
        if (strcmp(name, kRoutes[i]) == 0)
            return true;
    return false;
}

/* True when `sub_node` (an anonymous sub) is the callback argument of a
 * Mojolicious routing/hook call — its `$c` param is then a controller. Walks up
 * at most a couple of wrapper levels (the sub sits inside the call's argument
 * list, possibly under a `=>` pair). Only a call to one of the DSL names
 * qualifies (zero-heuristic). */
static bool perl_sub_is_routing_callback(PerlLSPContext *ctx, TSNode sub_node) {
    TSNode n = sub_node;
    for (int up = 0; up < 4; up++) {
        TSNode parent = ts_node_parent(n);
        if (ts_node_is_null(parent))
            return false;
        const char *pk = ts_node_type(parent);
        if (strcmp(pk, "function_call_expression") == 0 ||
            strcmp(pk, "ambiguous_function_call_expression") == 0) {
            TSNode fn = ts_node_child_by_field_name(parent, "function", 8);
            char *fname = ts_node_is_null(fn) ? NULL : perl_node_text(ctx, fn);
            return perl_is_mojo_routing_method(fname);
        }
        if (strcmp(pk, "method_call_expression") == 0) {
            TSNode m = ts_node_child_by_field_name(parent, "method", 6);
            char *mname = ts_node_is_null(m) ? NULL : perl_node_text(ctx, m);
            return perl_is_mojo_routing_method(mname);
        }
        if (strcmp(pk, "list_expression") != 0 && strcmp(pk, "parenthesized_expression") != 0 &&
            strcmp(pk, "binary_expression") != 0 && strcmp(pk, "arguments") != 0)
            return false;
        n = parent;
    }
    return false;
}

/* In a Mojolicious routing/hook callback, bind a signature param named `$c` (the
 * framework convention for the invocant controller) to Mojolicious::Controller
 * so `$c->render/stash/param/...` dispatches through the controller's @ISA.
 * Double-gated (routing-call context AND the `$c` name) to stay
 * zero-false-positive. Mojolicious::Controller's method table is registered by
 * the cross pass's chain-walk (seeded when the file has such a callback). */
static void perl_bind_routing_controller_param(PerlLSPContext *ctx, TSNode sub_node) {
    if (!perl_sub_is_routing_callback(ctx, sub_node))
        return;
    TSNode sig = perl_first_child_of_type(sub_node, "signature");
    if (ts_node_is_null(sig))
        return;
    uint32_t nc = ts_node_named_child_count(sig);
    for (uint32_t i = 0; i < nc && i < 8; i++) {
        TSNode sc = perl_first_scalar_desc(ts_node_named_child(sig, i), 0);
        char *ptxt = ts_node_is_null(sc) ? NULL : perl_node_text(ctx, sc);
        const char *bare = ptxt ? perl_strip_sigil(ptxt) : NULL;
        if (bare && strcmp(bare, "c") == 0) {
            cbm_scope_bind(ctx->current_scope, "c",
                           cbm_type_named(ctx->arena, "Mojolicious::Controller"));
            return;
        }
    }
}

/* True if the file contains any Mojolicious routing/hook callback with a `$c`
 * controller param — the cross pass then seeds Mojolicious::Controller into the
 * inheritance chain-walk so its method table is attached from all_defs. */
static bool perl_scan_has_mojo_routing_cb(PerlLSPContext *ctx, TSNode node, int depth) {
    if (ts_node_is_null(node) || depth > 200)
        return false;
    if (strcmp(ts_node_type(node), "anonymous_subroutine_expression") == 0 &&
        perl_sub_is_routing_callback(ctx, node)) {
        TSNode sig = perl_first_child_of_type(node, "signature");
        if (!ts_node_is_null(sig)) {
            uint32_t sn = ts_node_named_child_count(sig);
            for (uint32_t i = 0; i < sn && i < 8; i++) {
                TSNode sc = perl_first_scalar_desc(ts_node_named_child(sig, i), 0);
                char *ptxt = ts_node_is_null(sc) ? NULL : perl_node_text(ctx, sc);
                const char *bare = ptxt ? perl_strip_sigil(ptxt) : NULL;
                if (bare && strcmp(bare, "c") == 0)
                    return true;
            }
        }
    }
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++)
        if (perl_scan_has_mojo_routing_cb(ctx, ts_node_named_child(node, i), depth + 1))
            return true;
    return false;
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

    perl_bind_signature_invocant(ctx, node);

    /* Mojolicious routing/hook callback: type its `$c` param to the controller. */
    if (strcmp(ts_node_type(node), "anonymous_subroutine_expression") == 0)
        perl_bind_routing_controller_param(ctx, node);

    /* Corinna methods (5.38 feature 'class') carry an implicit $self bound to
     * the enclosing class — no `= shift` or signature needed. */
    if (strcmp(ts_node_type(node), "method_declaration_statement") == 0) {
        const char *mpkg = ctx->enclosing_package_qn && ctx->enclosing_package_qn[0]
                               ? ctx->enclosing_package_qn
                               : ctx->current_package_qn;
        if (mpkg && mpkg[0])
            cbm_scope_bind(ctx->current_scope, "self", cbm_type_named(ctx->arena, mpkg));
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

/* Split a whitespace-separated word blob into arena-owned words, invoking
 * `fn(ctx, word, user)` for each. tree-sitter-perl exposes `qw(a b c)` as ONE
 * string_content node with text "a b c" — per-word children were an incorrect
 * assumption that silently broke every multi-symbol qw() list. */
typedef void (*perl_word_fn)(PerlLSPContext *ctx, const char *word, void *user);
static void perl_for_each_word(PerlLSPContext *ctx, const char *blob, perl_word_fn fn,
                               void *user) {
    if (!blob)
        return;
    const char *p = blob;
    while (*p) {
        while (*p && isspace((unsigned char)*p))
            p++;
        const char *start = p;
        while (*p && !isspace((unsigned char)*p))
            p++;
        if (p > start) {
            char *word = cbm_arena_strndup(ctx->arena, start, (size_t)(p - start));
            if (word && word[0])
                fn(ctx, word, user);
        }
    }
}

/* One qw-import word: map W → <resolved-or-dotted module>.W. */
static void perl_qw_import_word(PerlLSPContext *ctx, const char *word, void *user) {
    const char *module_dot = (const char *)user;
    const char *fn = perl_strip_sigil(word); /* allow &func imports */
    if (!fn || !fn[0] || !(isalpha((unsigned char)fn[0]) || fn[0] == '_'))
        return;
    /* Import tags (:all, :DEFAULT) are not symbols. */
    char *target = cbm_arena_sprintf(ctx->arena, "%s.%s", module_dot, fn);
    perl_lsp_add_use(ctx, fn, target);
}

/* Parse the `qw(a b c)` list inside a node into the import map for module
 * `module_name`: each word W maps to `<module>.W`. In cross-file mode the
 * module portion is the RESOLVED module QN from the package→module map
 * (test.lib.My.Util.helper); otherwise the naive dotted spelling, which can
 * only ever match stdlib registry entries (zero-edge safe). */
static void perl_collect_qw_imports(PerlLSPContext *ctx, TSNode container,
                                    const char *module_name) {
    TSNode qw = perl_first_child_of_type(container, "quoted_word_list");
    if (ts_node_is_null(qw))
        return;
    /* Registry QNs are fully dotted (e.g. "Scalar.Util.blessed"): the module
     * portion uses "." not "::". Prefer the cross-file resolved module QN. */
    const char *module_dot = perl_xmod_lookup(ctx, module_name);
    if (!module_dot)
        module_dot = perl_pkg_to_dot(ctx->arena, module_name);
    if (!module_dot)
        module_dot = module_name;
    uint32_t nc = ts_node_child_count(qw);
    TSNode *kids = perl_collect_children(qw, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode w = kids ? kids[i] : ts_node_child(qw, i);
        if (ts_node_is_null(w) || !ts_node_is_named(w))
            continue;
        char *blob = perl_node_text(ctx, w);
        perl_for_each_word(ctx, blob, perl_qw_import_word, (void *)module_dot);
    }
    free(kids);
}

/* One parent word from a qw() list: `-norequire` is a flag, not a parent. */
static void perl_parent_word(PerlLSPContext *ctx, const char *word, void *user) {
    const char *child_pkg = (const char *)user;
    if (!word || !word[0] || word[0] == '-')
        return;
    perl_add_isa(ctx, child_pkg, word);
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
    /* quoted_word_list: ONE string_content child carries the whole
     * space-separated word blob ("Base Other") — split it. */
    if (strcmp(k, "quoted_word_list") == 0) {
        uint32_t nc = ts_node_child_count(node);
        TSNode *kids = perl_collect_children(node, nc);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode w = kids ? kids[i] : ts_node_child(node, i);
            if (ts_node_is_null(w) || !ts_node_is_named(w))
                continue;
            char *blob = perl_node_text(ctx, w);
            perl_for_each_word(ctx, blob, perl_parent_word, (void *)child_pkg);
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

/* Collect @ISA parents from a `use Mojo::Base ...` argument subtree.
 * Mojo::Base is the Mojolicious base-class pragma and the single most common
 * inheritance idiom in real-world Perl — the entire Mojolicious ecosystem is
 * built on it, so an unquoted-string parent here is worth as much as `use
 * parent`. Semantics mirror `use parent`:
 *     use Mojo::Base 'Parent';              → @ISA = ('Parent')
 *     use Mojo::Base 'Parent', -signatures; → @ISA = ('Parent')
 *     use Mojo::Base -base;                 → @ISA = ('Mojo::Base')
 *     use Mojo::Base -role / -strict;       → no @ISA (role compose / pragma)
 * A quoted string names a parent class; the bare `-base` flag maps to
 * Mojo::Base itself; every other -flag (-signatures, -async_await, -strict,
 * -role, -norequire) contributes nothing. Args appear directly or inside a
 * `list_expression`. Bounded recursion. */
static void perl_collect_mojo_parents(PerlLSPContext *ctx, TSNode node,
                                      const char *child_pkg, int depth) {
    if (ts_node_is_null(node) || depth > 6)
        return;
    const char *k = ts_node_type(node);
    if (perl_is_string_node(k)) {
        char *raw = perl_node_text(ctx, node);
        char *inner = perl_unquote(ctx->arena, raw);
        if (inner && inner[0] && inner[0] != '-')
            perl_add_isa(ctx, child_pkg, inner);
        return;
    }
    if (perl_is_bareword_node(k)) {
        char *bw = perl_node_text(ctx, node);
        /* -base flag: this package IS a base, inheriting from Mojo::Base.
         * A bare (unquoted) parent class name — rare but legal — is honored. */
        if (bw && strcmp(bw, "-base") == 0)
            perl_add_isa(ctx, child_pkg, "Mojo::Base");
        else if (bw && bw[0] && bw[0] != '-')
            perl_add_isa(ctx, child_pkg, bw);
        return;
    }
    /* list_expression / parenthesized wrapper: descend. */
    uint32_t nc = ts_node_child_count(node);
    TSNode *kids = perl_collect_children(node, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(node, i);
        if (!ts_node_is_null(c) && ts_node_is_named(c))
            perl_collect_mojo_parents(ctx, c, child_pkg, depth + 1);
    }
    free(kids);
}

/* Process a `use_statement`:
 *   use parent qw(Base);  / use parent 'Base';  → @ISA for current package
 *   use base   qw(Base);  / use base -norequire => 'Base';
 *   use Mojo::Base 'Base'; / use Mojo::Base -base; → @ISA (Mojolicious idiom)
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

    /* Mojo::Base: Mojolicious base-class pragma (see perl_collect_mojo_parents).
     * `use Mojo::Base 'Parent'` establishes @ISA exactly like `use parent`, and
     * `-base` inherits from Mojo::Base itself. Scan every named argument child
     * except the leading `module` node. */
    if (strcmp(module_name, "Mojo::Base") == 0) {
        const char *child_pkg = ctx->current_package_qn && ctx->current_package_qn[0]
                                    ? ctx->current_package_qn
                                    : "main";
        uint32_t nc = ts_node_child_count(node);
        TSNode *kids = perl_collect_children(node, nc);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = kids ? kids[i] : ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c) || ts_node_eq(c, mod))
                continue;
            perl_collect_mojo_parents(ctx, c, child_pkg, 0);
        }
        free(kids);
        return;
    }

    /* Moose-family gate: has/extends/with become meaningful DSL keywords only
     * in packages that import a Moose-like module. Tracked PER PACKAGE so a
     * multi-package file with one Moose package does not treat a foreign
     * `has(...)` call as an attribute. Object::Pad is deliberately absent:
     * its `has $x;`/`field $x` take variables and ride the Corinna path. */
    if (strcmp(module_name, "Moose") == 0 || strcmp(module_name, "Moo") == 0 ||
        strcmp(module_name, "Mouse") == 0 || strcmp(module_name, "Moose::Role") == 0 ||
        strcmp(module_name, "Moo::Role") == 0 || strcmp(module_name, "Class::Accessor") == 0) {
        const char *pkg = ctx->current_package_qn && ctx->current_package_qn[0]
                              ? ctx->current_package_qn
                              : "main";
        perl_mark_moose_pkg(ctx, pkg);
        return;
    }

    /* Generic Exporter import: use Module qw(f1 f2). */
    perl_collect_qw_imports(ctx, node, module_name);

    /* `use Module;` with NO import list — the dominant style for internal
     * modules — imports the module's @EXPORT defaults. Cross-file mode knows
     * both the resolved module QN (package→module map) and its @EXPORT list
     * (collected at extraction); seed name → module_qn.name for each. A bare
     * pragma or unresolved module maps to nothing (zero-edge). "No import
     * list" means the statement has no named argument child beyond the module
     * field — `use Mod ();` (import NOTHING) and version/qw forms all carry
     * extra children and are excluded. */
    {
        bool has_args = false;
        uint32_t nc = ts_node_child_count(node);
        TSNode *kids = perl_collect_children(node, nc);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = kids ? kids[i] : ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c) || ts_node_eq(c, mod))
                continue;
            has_args = true;
            break;
        }
        free(kids);
        if (!has_args) {
            const char *resolved = perl_xmod_lookup(ctx, module_name);
            const char *exports = resolved ? perl_xexp_lookup(ctx, resolved) : NULL;
            if (exports && exports[0]) {
                /* '|'-separated names. */
                const char *p = exports;
                while (*p) {
                    const char *start = p;
                    while (*p && *p != '|')
                        p++;
                    if (p > start) {
                        char *name = cbm_arena_strndup(ctx->arena, start, (size_t)(p - start));
                        if (name && name[0]) {
                            char *target = cbm_arena_sprintf(ctx->arena, "%s.%s", resolved, name);
                            perl_lsp_add_use(ctx, name, target);
                        }
                    }
                    if (*p == '|')
                        p++;
                }
            }
        }
    }
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

/* Corinna (5.38 feature 'class'): `class Dog :isa(Animal) { ... }` — record
 * the :isa parent so inherited dispatch and SUPER:: work exactly like @ISA.
 * The attribute hangs off the class_statement as attribute_name "isa" with an
 * attribute_value carrying the parent name; scan shallow descendants (the
 * attributes precede the block, so the walk is tiny and depth-capped). */
static void perl_scan_isa_attribute(PerlLSPContext *ctx, TSNode node, const char *class_qn,
                                    bool *pending_isa, int depth) {
    if (ts_node_is_null(node) || depth > 4)
        return;
    const char *k = ts_node_type(node);
    if (strcmp(k, "block") == 0)
        return; /* attributes never live inside the class body */
    if (strcmp(k, "attribute_name") == 0) {
        char *t = perl_node_text(ctx, node);
        *pending_isa = t && strcmp(t, "isa") == 0;
        return;
    }
    if (strcmp(k, "attribute_value") == 0) {
        if (*pending_isa) {
            char *parent = perl_node_text(ctx, node);
            if (parent && parent[0])
                perl_add_isa(ctx, class_qn, parent);
            *pending_isa = false;
        }
        return;
    }
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc && i < 32; i++) {
        TSNode c = ts_node_child(node, i);
        if (!ts_node_is_null(c) && ts_node_is_named(c))
            perl_scan_isa_attribute(ctx, c, class_qn, pending_isa, depth + 1);
    }
}

static void perl_collect_class_isa(PerlLSPContext *ctx, TSNode class_node) {
    const char *class_qn = ctx->current_package_qn;
    if (!class_qn || !class_qn[0])
        return;
    bool pending = false;
    uint32_t nc = ts_node_child_count(class_node);
    for (uint32_t i = 0; i < nc && i < 32; i++) {
        TSNode c = ts_node_child(class_node, i);
        if (!ts_node_is_null(c) && ts_node_is_named(c))
            perl_scan_isa_attribute(ctx, c, class_qn, &pending, 0);
    }
}

/* Collect the Moose attribute name(s) from the FIRST argument of a `has`
 * call: 'name', bareword name, or ['a','b'] multi-attr arrayref. Strings only
 * — Object::Pad's `has $x;` takes a variable and is deliberately skipped. */
static void perl_collect_has_names(PerlLSPContext *ctx, TSNode node, const char *pkg,
                                   const char *isa, int depth) {
    if (ts_node_is_null(node) || depth > 3)
        return;
    const char *k = ts_node_type(node);
    if (perl_is_string_node(k)) {
        char *inner = perl_unquote(ctx->arena, perl_node_text(ctx, node));
        if (inner)
            perl_add_attr(ctx, pkg, inner, isa);
        return;
    }
    if (perl_is_bareword_node(k)) {
        char *bw = perl_node_text(ctx, node);
        if (bw)
            perl_add_attr(ctx, pkg, bw, isa);
        return;
    }
    if (strcmp(k, "anonymous_array_expression") == 0 || strcmp(k, "list_expression") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc && i < 16; i++)
            perl_collect_has_names(ctx, ts_node_named_child(node, i), pkg, isa, depth + 1);
    }
    /* scalar/other → variable-form has (Object::Pad) → skip. */
}

/* Find the `isa => 'Class::Name'` value inside a has() option list: scan the
 * flat key/value children for a bareword "isa" followed by a string/bareword
 * value. Parameterized types (ArrayRef[...]) return NULL (unknown). */
static const char *perl_find_has_isa(PerlLSPContext *ctx, TSNode node, int depth) {
    if (ts_node_is_null(node) || depth > 3)
        return NULL;
    uint32_t nc = ts_node_named_child_count(node);
    bool pending = false;
    for (uint32_t i = 0; i < nc && i < 64; i++) {
        TSNode c = ts_node_named_child(node, i);
        const char *ck = ts_node_type(c);
        if (perl_is_bareword_node(ck)) {
            char *t = perl_node_text(ctx, c);
            if (pending && t && t[0] && !strchr(t, '[')) {
                return cbm_arena_strdup(ctx->arena, t);
            }
            pending = t && strcmp(t, "isa") == 0;
            continue;
        }
        if (perl_is_string_node(ck)) {
            if (pending) {
                char *inner = perl_unquote(ctx->arena, perl_node_text(ctx, c));
                if (inner && inner[0] && !strchr(inner, '['))
                    return inner;
                return NULL;
            }
            continue;
        }
        if (strcmp(ck, "list_expression") == 0 || strcmp(ck, "parenthesized_expression") == 0) {
            const char *found = perl_find_has_isa(ctx, c, depth + 1);
            if (found)
                return found;
            continue;
        }
        pending = false; /* any other value node closes a dangling key */
    }
    return NULL;
}

/* PASS-1 observer for top-level DSL-ish calls:
 *   push @ISA, 'Base'; / unshift @ISA, ...; / push @Pkg::ISA, ... — the
 *     classic pre-parent.pm inheritance idiom (function_call with the ISA
 *     array as first argument).
 *   extends 'Base'; / with 'Role'; / has attr => (isa => 'T', ...) — Moose
 *     DSL, honored only in packages gated by perl_mark_moose_pkg. `extends`
 *     REPLACES @ISA in real Moose; appending is an accepted approximation for
 *     edge purposes, and `with` mapped to the ISA table is a sound flattening
 *     of role composition for method lookup. */
static void perl_pass1_scan_call(PerlLSPContext *ctx, TSNode call) {
    TSNode fn = ts_node_child_by_field_name(call, "function", 8);
    if (ts_node_is_null(fn))
        return;
    char *name = perl_node_text(ctx, fn);
    if (!name || !name[0])
        return;
    TSNode args = ts_node_child_by_field_name(call, "arguments", 9);

    if (strcmp(name, "push") == 0 || strcmp(name, "unshift") == 0) {
        if (ts_node_is_null(args))
            return;
        /* First named argument must be the @ISA array (bare or Pkg::ISA). */
        TSNode first = ts_node_named_child(args, 0);
        if (ts_node_is_null(first) || strcmp(ts_node_type(first), "array") != 0)
            return;
        char *atxt = perl_node_text(ctx, first);
        const char *aname = perl_strip_sigil(atxt);
        if (!aname)
            return;
        const char *child_pkg = NULL;
        if (strcmp(aname, "ISA") == 0) {
            child_pkg = ctx->current_package_qn && ctx->current_package_qn[0]
                            ? ctx->current_package_qn
                            : "main";
        } else {
            size_t alen = strlen(aname);
            if (alen > 5 && strcmp(aname + alen - 5, "::ISA") == 0)
                child_pkg = cbm_arena_strndup(ctx->arena, aname, alen - 5);
        }
        if (!child_pkg || !child_pkg[0])
            return;
        uint32_t nc = ts_node_named_child_count(args);
        for (uint32_t i = 1; i < nc && i < 32; i++)
            perl_collect_parents(ctx, ts_node_named_child(args, i), child_pkg, 0);
        return;
    }

    /* Moose DSL below — per-package gate. */
    const char *pkg =
        ctx->current_package_qn && ctx->current_package_qn[0] ? ctx->current_package_qn : "main";
    if (!perl_pkg_is_moose(ctx, pkg) || ts_node_is_null(args))
        return;

    if (strcmp(name, "extends") == 0 || strcmp(name, "with") == 0) {
        perl_collect_parents(ctx, args, pkg, 0);
        return;
    }
    if (strcmp(name, "has") == 0) {
        TSNode name_arg = args;
        if (strcmp(ts_node_type(args), "list_expression") == 0) {
            name_arg = ts_node_named_child(args, 0);
            if (ts_node_is_null(name_arg))
                return;
        }
        const char *isa = perl_find_has_isa(ctx, args, 0);
        perl_collect_has_names(ctx, name_arg, pkg, isa, 0);
        return;
    }
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
    } else if (strcmp(k, "class_statement") == 0) {
        /* Corinna class: package context + :isa parent (5.38 feature 'class').
         * The name field matches package_statement's shape. */
        process_package_decl(ctx, node);
        perl_collect_class_isa(ctx, node);
        /* Fall through: the class block's body follows as children. */
    } else if (strcmp(k, "use_statement") == 0) {
        perl_collect_use_statement(ctx, node);
        return;
    } else if (strcmp(k, "assignment_expression") == 0) {
        perl_collect_isa_assignment(ctx, node);
    } else if (strcmp(k, "function_call_expression") == 0 ||
               strcmp(k, "ambiguous_function_call_expression") == 0) {
        /* push/unshift @ISA and the Moose has/extends/with DSL. */
        perl_pass1_scan_call(ctx, node);
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
    ctx->use_count = ctx->use_floor; /* keep caller-seeded cross-file imports */
    ctx->isa_count = 0;
    ctx->moose_pkg_count = 0;
    ctx->attr_count = 0;
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
        if (strcmp(k, "package_statement") == 0 || strcmp(k, "class_statement") == 0) {
            process_package_decl(ctx, c);
            /* Walk the (possibly block-scoped) package/class body for nested
             * subs and methods. */
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
            /* Top-level statements (Mojolicious::Lite apps, .t scripts, script
             * bodies): attribute their calls to the FILE MODULE, exactly as the
             * unified extractor already does for the raw call rows it emits
             * (extract_unified.c: enclosing_func_qn = module_qn when there is no
             * enclosing sub). Without this the LSP left the caller NULL and
             * perl_emit_resolved dropped EVERY typed top-level call — a 109-file
             * Mojolicious test suite (12k `$var->method` sites: `$t->get_ok` on a
             * Test::Mojo typed via ->new, top-level `$obj->method` chains) emitted
             * ~0 edges. Matching the extractor's caller QN lets the LSP resolution
             * bind to the same source (the module node) so the edge survives. */
            const char *saved_tl = ctx->enclosing_func_qn;
            if (ctx->module_qn && ctx->module_qn[0])
                ctx->enclosing_func_qn = ctx->module_qn;
            perl_resolve_calls_in_node(ctx, c);
            ctx->enclosing_func_qn = saved_tl;
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
        if (strcmp(k, "package_statement") == 0 || strcmp(k, "class_statement") == 0) {
            TSNode name = ts_node_child_by_field_name(c, "name", 4);
            if (ts_node_is_null(name))
                name = perl_first_child_of_type(c, "package");
            if (!ts_node_is_null(name)) {
                char *p = perl_node_text(ctx, name);
                if (p && p[0])
                    cur_pkg = cbm_arena_strdup(ctx->arena, p);
            }
            /* Block-scoped package body: subs are DIRECT children of the
             * package_statement; a Corinna class_statement instead wraps its
             * methods in a `block` child — descend one level into it. */
            uint32_t bn = ts_node_child_count(c);
            TSNode *bkids = perl_collect_children(c, bn);
            for (uint32_t bi = 0; bi < bn; bi++) {
                TSNode bc = bkids ? bkids[bi] : ts_node_child(c, bi);
                if (ts_node_is_null(bc) || !ts_node_is_named(bc))
                    continue;
                const char *bk = ts_node_type(bc);
                TSNode subs_parent = c;
                uint32_t sn = 1;
                TSNode single = bc;
                TSNode *skids = NULL;
                if (strcmp(bk, "block") == 0) {
                    subs_parent = bc;
                    sn = ts_node_child_count(bc);
                    skids = perl_collect_children(bc, sn);
                }
                for (uint32_t si = 0; si < sn; si++) {
                    TSNode sc = (subs_parent.id == c.id)
                                    ? single
                                    : (skids ? skids[si] : ts_node_child(subs_parent, si));
                    if (ts_node_is_null(sc) || !ts_node_is_named(sc))
                        continue;
                    if (strcmp(ts_node_type(sc), "subroutine_declaration_statement") != 0 &&
                        strcmp(ts_node_type(sc), "method_declaration_statement") != 0)
                        continue;
                    TSNode bname = ts_node_child_by_field_name(sc, "name", 4);
                    if (ts_node_is_null(bname))
                        continue;
                    char *bsn = perl_node_text(ctx, bname);
                    if (!bsn || !bsn[0])
                        continue;
                    const char *bqn =
                        ctx->module_qn
                            ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, bsn)
                            : cbm_arena_strdup(ctx->arena, bsn);
                    perl_mvec_push(&mv, cur_pkg, bsn, bqn);
                }
                free(skids);
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
    ctx.usages = &result->usages;

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

/* ── cross-file LSP: cbm_run_perl_lsp_cross ─────────────────────── */

extern const TSLanguage *tree_sitter_perl(void);

/* Project-wide multi-level @ISA index lookup (defined in pass_lsp_cross.c):
 * a module QN → its own tagged parent spellings, or NULL. */
const char *const *cbm_perl_inherit_lookup(const struct CBMPerlInheritIndex *idx,
                                           const char *module_qn);

/* Register the caller-supplied CBMLSPDef[] as callable functions, mirroring
 * cbm_php_register_lsp_defs (php_lsp.c). Perl defs carry no declared types,
 * so signatures get an unknown return; receiver_type (when a def has one)
 * still gets its type auto-registered so perl_lookup_method's chain walk has
 * somewhere to land. Variable defs are skipped here — the EXPORT ones are
 * consumed separately for the default-export table. */
/* Register ONE Function/Method def as a callable func (unknown return). Skips
 * non-callable defs. Shared by the bulk registrar and the cross-file inheritance
 * chain-walk (which registers ancestor funcs pulled from the full def universe
 * so perl_lookup_method's cbm_registry_lookup_func succeeds on inherited
 * methods that the per-file def filter dropped). */
static void perl_register_lsp_func(CBMArena *arena, CBMTypeRegistry *reg, CBMLSPDef *d) {
    if (!d || !d->qualified_name || !d->short_name || !d->label)
        return;
    if (strcmp(d->label, "Function") != 0 && strcmp(d->label, "Method") != 0)
        return;
    CBMRegisteredFunc rf;
    memset(&rf, 0, sizeof(rf));
    rf.min_params = -1;
    rf.qualified_name = d->qualified_name;
    rf.short_name = d->short_name;
    const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
    if (rets) {
        /* Use the extraction-inferred return type when present (dotted package
         * spelling, e.g. "Mojo.Transaction", from perl_infer_return_types) so
         * `my $x = $obj->accessor` types $x and the chained call resolves; else
         * unknown (zero-edge, unchanged). d->return_types is a "|"-separated
         * text list — take the first entry. */
        const CBMType *ret = cbm_type_unknown();
        if (d->return_types && d->return_types[0]) {
            const char *bar = strchr(d->return_types, '|');
            size_t len = bar ? (size_t)(bar - d->return_types) : strlen(d->return_types);
            if (len > 0) {
                char *first = (char *)cbm_arena_alloc(arena, len + 1);
                if (first) {
                    memcpy(first, d->return_types, len);
                    first[len] = '\0';
                    ret = cbm_type_named(arena, first);
                }
            }
        }
        rets[0] = ret;
        rets[1] = NULL;
    }
    rf.signature = cbm_type_func(arena, NULL, NULL, rets);
    if (strcmp(d->label, "Method") == 0 && d->receiver_type && d->receiver_type[0]) {
        rf.receiver_type = d->receiver_type;
        if (!cbm_registry_lookup_type(reg, rf.receiver_type)) {
            CBMRegisteredType auto_t;
            memset(&auto_t, 0, sizeof(auto_t));
            auto_t.qualified_name = rf.receiver_type;
            const char *dot = strrchr(d->receiver_type, '.');
            auto_t.short_name = dot ? dot + 1 : rf.receiver_type;
            cbm_registry_add_type(reg, auto_t);
        }
    }
    cbm_registry_add_func(reg, rf);
}

static void cbm_perl_register_lsp_defs(CBMArena *arena, CBMTypeRegistry *reg, CBMLSPDef *defs,
                                       int def_count) {
    for (int i = 0; i < def_count; i++)
        perl_register_lsp_func(arena, reg, &defs[i]);
}

/* True when the dotted module QN `qn` ends with the dotted package path
 * `dotted` on a segment boundary ("test.lib.My.Util" matches "My.Util"). */
static bool perl_qn_tail_matches(const char *qn, const char *dotted) {
    if (!qn || !dotted || !dotted[0])
        return false;
    size_t ql = strlen(qn);
    size_t dl = strlen(dotted);
    if (ql < dl)
        return false;
    if (strcmp(qn + ql - dl, dotted) != 0)
        return false;
    return ql == dl || qn[ql - dl - 1] == '.';
}

/* Small collector for module names referenced by `use`/`require` — the
 * candidates for cross-file package→module mapping. Bounded. */
enum { PERL_XMOD_SCAN_CAP = 128 };
typedef struct {
    const char *names[PERL_XMOD_SCAN_CAP];
    int count;
} PerlUsedModules;

static void perl_used_modules_add(PerlLSPContext *ctx, PerlUsedModules *um, const char *name) {
    if (!name || !name[0] || um->count >= PERL_XMOD_SCAN_CAP)
        return;
    /* Pragmas and single lowercase words are never project modules worth a
     * convention lookup; still cheap to include, but skip the obvious ones. */
    for (int i = 0; i < um->count; i++) {
        if (strcmp(um->names[i], name) == 0)
            return;
    }
    um->names[um->count++] = cbm_arena_strdup(ctx->arena, name);
}

/* Whole-tree scan for use_statement modules and require_expression operands
 * (bareword `require Foo::Bar;` and string `require 'Foo/Bar.pm';`, wherever
 * they appear — the common patterns are conditional). Depth-capped. */
static void perl_scan_used_modules(PerlLSPContext *ctx, TSNode node, PerlUsedModules *um,
                                   int depth) {
    if (ts_node_is_null(node) || depth > 128 || um->count >= PERL_XMOD_SCAN_CAP)
        return;
    const char *k = ts_node_type(node);
    if (strcmp(k, "use_statement") == 0) {
        TSNode mod = ts_node_child_by_field_name(node, "module", 6);
        if (!ts_node_is_null(mod))
            perl_used_modules_add(ctx, um, perl_node_text(ctx, mod));
        return;
    }
    if (strcmp(k, "require_expression") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(node, i);
            const char *ck = ts_node_type(c);
            if (perl_is_bareword_node(ck)) {
                perl_used_modules_add(ctx, um, perl_node_text(ctx, c));
            } else if (perl_is_string_node(ck)) {
                /* 'Foo/Bar.pm' → Foo::Bar */
                char *inner = perl_unquote(ctx->arena, perl_node_text(ctx, c));
                if (inner) {
                    size_t n = strlen(inner);
                    if (n > 3 && strcmp(inner + n - 3, ".pm") == 0) {
                        inner[n - 3] = '\0';
                        /* '/' → "::" (grow: reuse dotted form later, keep :: here) */
                        size_t segs = 0;
                        for (char *p = inner; *p; p++)
                            if (*p == '/')
                                segs++;
                        char *pkg = (char *)cbm_arena_alloc(ctx->arena, n + segs + 1);
                        if (pkg) {
                            size_t w = 0;
                            for (char *p = inner; *p; p++) {
                                if (*p == '/') {
                                    pkg[w++] = ':';
                                    pkg[w++] = ':';
                                } else {
                                    pkg[w++] = *p;
                                }
                            }
                            pkg[w] = '\0';
                            perl_used_modules_add(ctx, um, pkg);
                        }
                    }
                }
            }
        }
        return;
    }
    uint32_t nc = ts_node_child_count(node);
    TSNode *kids = perl_collect_children(node, nc);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = kids ? kids[i] : ts_node_child(node, i);
        if (!ts_node_is_null(c) && ts_node_is_named(c))
            perl_scan_used_modules(ctx, c, um, depth + 1);
    }
    free(kids);
}

/* Resolve one used module name against (a) the caller-supplied import map
 * (values are gbuf-resolved module QNs) and (b) the filtered defs' own
 * def_module_qn tails (rel path ends Foo/Bar.pm — lib/ and t/lib/ roots fall
 * out of plain tail matching since "test.lib.My.Util" ends with ".My.Util").
 * Ambiguity (two DISTINCT module QNs match) → NULL, per the zero-edge
 * guarantee: no mapping, no edge. */
static const char *perl_resolve_used_module(PerlLSPContext *ctx, const char *pkg_name,
                                            CBMLSPDef *defs, int def_count,
                                            const char **import_names, const char **import_qns,
                                            int import_count) {
    const char *dotted = perl_pkg_to_dot(ctx->arena, pkg_name);
    if (!dotted || !dotted[0])
        return NULL;
    const char *found = NULL;
    /* (a) exact local-name match in the caller import map wins outright. */
    for (int i = 0; i < import_count; i++) {
        if (import_names && import_names[i] && import_qns && import_qns[i] &&
            strcmp(import_names[i], pkg_name) == 0) {
            return import_qns[i];
        }
    }
    /* (a') tail match over import map values. */
    for (int i = 0; i < import_count; i++) {
        const char *qn = import_qns ? import_qns[i] : NULL;
        if (!qn || !perl_qn_tail_matches(qn, dotted))
            continue;
        if (found && strcmp(found, qn) != 0)
            return NULL; /* ambiguous */
        found = qn;
    }
    if (found)
        return found;
    /* (b) tail match over the (filtered) defs' module QNs. */
    for (int i = 0; i < def_count; i++) {
        const char *qn = defs[i].def_module_qn;
        if (!qn || !perl_qn_tail_matches(qn, dotted))
            continue;
        if (found && strcmp(found, qn) != 0)
            return NULL; /* ambiguous */
        found = qn;
    }
    return found;
}

void cbm_run_perl_lsp_cross(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, CBMLSPDef *defs, int def_count,
                            const char **import_names, const char **import_qns, int import_count,
                            TSTree *cached_tree, CBMResolvedCallArray *out,
                            const struct CBMPerlInheritIndex *inherit_idx, CBMLSPDef *all_defs,
                            int all_def_count) {
    if (!arena || !source || source_len <= 0 || !out)
        return;
    /* The chain-walk resolves ANCESTOR modules (grandparent+), whose defs the
     * per-file filter drops; fall back to the filtered set when the caller has
     * no separate full universe (e.g. unit tests pass the same array). */
    if (!all_defs || all_def_count <= 0) {
        all_defs = defs;
        all_def_count = def_count;
    }

    TSParser *parser = NULL;
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return;
        ts_parser_set_language(parser, tree_sitter_perl());
        tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)source_len);
        owns_tree = true;
        if (!tree) {
            ts_parser_delete(parser);
            return;
        }
    }
    TSNode root = ts_tree_root_node(tree);

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_perl_stdlib_register(&reg, arena);
    cbm_perl_register_lsp_defs(arena, &reg, defs, def_count);

    PerlLSPContext ctx;
    perl_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, out);

    /* Caller-supplied import map seeds the use map; process_file's PASS-1
     * reset preserves the first use_floor entries. Module-shaped keys
     * ("My::Util") are harmless there — bare-call lookups never carry "::" —
     * and symbol-shaped keys (hand-built maps, future member imports)
     * resolve directly. */
    for (int i = 0; i < import_count; i++) {
        if (import_names && import_qns && import_names[i] && import_qns[i])
            perl_lsp_add_use(&ctx, import_names[i], import_qns[i]);
    }
    ctx.use_floor = ctx.use_count;

    /* Package→module map: every module named by a use/require anywhere in the
     * file, resolved against the import map + filtered defs (convention: rel
     * path ends Foo/Bar.pm, lib/ roots included by tail matching). Each
     * mapped package gets a CBMRegisteredType whose method table is that
     * module's Function/Method defs, so `Foo::Bar->new`, `$obj->m` chains and
     * `Foo::Bar::sub()` statics dispatch cross-file. No mapping → no entry →
     * no edge (zero-edge guarantee). */
    PerlUsedModules um;
    um.count = 0;
    perl_scan_used_modules(&ctx, root, &um, 0);
    for (int m = 0; m < um.count; m++) {
        const char *resolved = perl_resolve_used_module(&ctx, um.names[m], defs, def_count,
                                                        import_names, import_qns, import_count);
        if (!resolved || !resolved[0])
            continue;
        perl_pair_push(ctx.arena, &ctx.xmod_pkgs, &ctx.xmod_qns, &ctx.xmod_count, &ctx.xmod_cap,
                       um.names[m], resolved);
        /* Method table: the module's callable defs, keyed by short name. */
        PerlMethodVec mv;
        memset(&mv, 0, sizeof(mv));
        for (int i = 0; i < def_count; i++) {
            CBMLSPDef *d = &defs[i];
            if (!d->def_module_qn || strcmp(d->def_module_qn, resolved) != 0)
                continue;
            if (!d->label || (strcmp(d->label, "Function") != 0 && strcmp(d->label, "Method") != 0))
                continue;
            if (!d->short_name || !d->qualified_name)
                continue;
            perl_mvec_push(&mv, um.names[m], d->short_name, d->qualified_name);
        }
        if (mv.cnt > 0 && mv.v)
            perl_type_set_methods(&ctx, &reg, um.names[m], mv.v, mv.cnt);
        free(mv.v);
    }

    /* Default-export table from EXPORT Variable defs (perl-exports-model):
     * extraction stores the qw() word list on the def's return_type. Only
     * @EXPORT feeds `use Mod;` — @EXPORT_OK names must be requested via
     * qw(...), which the qw path already resolves against the module map. */
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->label || strcmp(d->label, "Variable") != 0 || !d->short_name)
            continue;
        if (strcmp(d->short_name, "EXPORT") != 0)
            continue;
        if (!d->def_module_qn || !d->return_types || !d->return_types[0])
            continue;
        perl_pair_push(ctx.arena, &ctx.xexp_module_qns, &ctx.xexp_names, &ctx.xexp_count,
                       &ctx.xexp_cap, d->def_module_qn, d->return_types);
    }

    /* Own-file packages: same Phase B.1 as the per-file entry point, so
     * same-file dispatch keeps working under the cross entry (results are
     * site-deduped on append). */
    ctx.current_package_qn = "";
    ctx.enclosing_package_qn = "";
    perl_pass1_scan(&ctx, root);
    perl_register_packages(&ctx, &reg);
    perl_attach_methods(&ctx, &reg, root);

    /* Cross-file MULTI-LEVEL inheritance: a class's @ISA parent (use parent /
     * use base / use Mojo::Base 'X') usually lives in ANOTHER file, so its
     * method table was never attached to the parent's (bare) registered type,
     * and its OWN parent (the grandparent) is invisible to this file's pass1
     * (which records only this file's packages' @ISA). Walk the ancestor chain:
     * seed with this file's direct parents; for each ancestor resolve it to a
     * module QN, attach that module's cross-file Function/Method defs to the
     * ancestor type, look up the ancestor's OWN parents in the project-wide
     * inherit index, set the ancestor type's embedded_types to them (so
     * perl_lookup_method's frontier walk recurses the rest of the chain), and
     * enqueue those grandparents. `$self->grandparent_method` then dispatches
     * across arbitrarily many files. Bounded by a seen-set + hard cap; diamonds
     * and cycles visit each ancestor once. inherit_idx == NULL degrades to the
     * one-level behaviour (direct parents only). REALLOC-SAFE: perl_type_set_
     * methods may grow reg.types, so no CBMRegisteredType* is held across it —
     * the type is always re-found by name. */
    {
        enum { PERL_CHAIN_CAP = 256 };
        const char *worklist[PERL_CHAIN_CAP];
        const char *seen[PERL_CHAIN_CAP];
        int wl_head = 0, wl_tail = 0, seen_count = 0;
        for (int i = 0; i < ctx.isa_count && wl_tail < PERL_CHAIN_CAP; i++) {
            const char *p = ctx.isa_parent_qns[i];
            if (p && p[0])
                worklist[wl_tail++] = p;
        }
        /* Also seed the USED-MODULE types (the packages named by `use`/
         * constructor: Mojo::IOLoop::Stream, Mojo::UserAgent, ...). The
         * used-module scan above attached each module's OWN methods, but NOT its
         * @ISA chain — so a constructor/return-typed receiver
         * `my $s = Mojo::IOLoop::Stream->new; $s->on(...)` could not reach an
         * INHERITED method (EventEmitter::on). Enqueuing the used-module packages
         * makes the walk set their embedded_types from the project inherit index
         * and attach ancestor methods, so inherited-method calls on any
         * constructor/used-module-typed receiver dispatch across files. Bounded
         * by PERL_CHAIN_CAP + the seen-set (each ancestor visited once). */
        for (int i = 0; i < ctx.xmod_count && wl_tail < PERL_CHAIN_CAP; i++) {
            const char *p = ctx.xmod_pkgs[i];
            if (p && p[0])
                worklist[wl_tail++] = p;
        }
        /* Also seed classes that appear as function/accessor RETURN TYPES
         * (`has res => sub { Mojo::Message::Response->new }` etc.). A receiver
         * typed via a return type ($tx->res->dom) may reach a class that this
         * file never `use`s, so it is absent from xmod and would carry no @ISA —
         * blocking the further-inherited method (Response inherits dom from
         * Mojo::Message). Return types are stored DOTTED ("Mojo.Message.Response",
         * first of a "|"-list); convert to the colon spelling the walk resolves.
         * Deduped against the worklist to respect the cap. */
        for (int i = 0; i < all_def_count && wl_tail < PERL_CHAIN_CAP; i++) {
            const char *rts = all_defs[i].return_types;
            if (!rts || !rts[0] || rts[0] == '_') /* skip empty + literal __PACKAGE__ */
                continue;
            size_t rlen = 0;
            while (rts[rlen] && rts[rlen] != '|')
                rlen++;
            if (rlen == 0 || !strchr(rts, '.')) /* single-segment/no-dot: xmod/own handles it */
                continue;
            char *colon = (char *)cbm_arena_alloc(ctx.arena, rlen * 2 + 1);
            if (!colon)
                continue;
            size_t w = 0;
            for (size_t r = 0; r < rlen; r++) {
                if (rts[r] == '.') {
                    colon[w++] = ':';
                    colon[w++] = ':';
                } else {
                    colon[w++] = rts[r];
                }
            }
            colon[w] = '\0';
            bool dup = false;
            for (int q = 0; q < wl_tail; q++) {
                if (worklist[q] && strcmp(worklist[q], colon) == 0) {
                    dup = true;
                    break;
                }
            }
            if (!dup)
                worklist[wl_tail++] = colon;
        }
        /* Mojolicious routing/hook callbacks type their `$c` param to
         * Mojolicious::Controller (perl_bind_routing_controller_param); seed that
         * class into the chain-walk so its method table (render/stash/param/...)
         * plus its own @ISA (Mojo::Base) get attached from all_defs. Only when the
         * file actually has such a callback — no callback, no seed, no edge. */
        if (wl_tail < PERL_CHAIN_CAP &&
            (perl_scan_has_mojo_routing_cb(&ctx, root, 0) ||
             (module_qn && strstr(module_qn, "Mojolicious"))))
            worklist[wl_tail++] = "Mojolicious::Controller";
        while (wl_head < wl_tail) {
            const char *parent = worklist[wl_head++];
            if (!parent || !parent[0])
                continue;
            bool already = false;
            for (int s = 0; s < seen_count; s++) {
                if (strcmp(seen[s], parent) == 0) {
                    already = true;
                    break;
                }
            }
            if (already)
                continue;
            if (seen_count < PERL_CHAIN_CAP)
                seen[seen_count++] = parent;

            /* Resolve + collect over the FULL def universe: a grandparent+ is not
             * in the current file's import map, so its module and methods are
             * absent from the per-file filtered `defs`. */
            const char *resolved = perl_resolve_used_module(&ctx, parent, all_defs, all_def_count,
                                                            import_names, import_qns, import_count);
            if (!resolved || !resolved[0])
                continue; /* external / unindexed ancestor: chain terminates here */

            /* Attach the ancestor module's methods to type[parent] unless it
             * already has them (same-file parent handled by perl_attach_methods).
             * Also REGISTER each ancestor sub as a func — otherwise
             * perl_lookup_method finds the name in the method table but
             * cbm_registry_lookup_func fails (the func was filtered out). */
            bool have_methods = false;
            for (int t = 0; t < reg.type_count; t++) {
                if (reg.types[t].qualified_name &&
                    strcmp(reg.types[t].qualified_name, parent) == 0) {
                    have_methods = reg.types[t].method_names && reg.types[t].method_names[0];
                    break;
                }
            }
            if (!have_methods) {
                PerlMethodVec pmv;
                memset(&pmv, 0, sizeof(pmv));
                for (int j = 0; j < all_def_count; j++) {
                    CBMLSPDef *d = &all_defs[j];
                    if (!d->def_module_qn || strcmp(d->def_module_qn, resolved) != 0)
                        continue;
                    if (!d->label ||
                        (strcmp(d->label, "Function") != 0 && strcmp(d->label, "Method") != 0))
                        continue;
                    if (!d->short_name || !d->qualified_name)
                        continue;
                    perl_register_lsp_func(ctx.arena, &reg, d);
                    perl_mvec_push(&pmv, parent, d->short_name, d->qualified_name);
                }
                if (pmv.cnt > 0 && pmv.v)
                    perl_type_set_methods(&ctx, &reg, parent, pmv.v, pmv.cnt); /* may realloc */
                free(pmv.v);
            }

            /* Grandparents: the ancestor module's OWN tagged @ISA parents. */
            const char *const *gps = cbm_perl_inherit_lookup(inherit_idx, resolved);
            if (!gps || !gps[0])
                continue;
            int gc = 0;
            while (gps[gc])
                gc++;
            /* Re-find type[parent] AFTER any set_methods realloc, then seed its
             * embedded_types (unless already set by perl_register_packages for a
             * same-file parent) so the frontier walk continues up the chain. */
            CBMRegisteredType *rt = NULL;
            for (int t = 0; t < reg.type_count; t++) {
                if (reg.types[t].qualified_name &&
                    strcmp(reg.types[t].qualified_name, parent) == 0) {
                    rt = &reg.types[t];
                    break;
                }
            }
            if (rt && !(rt->embedded_types && rt->embedded_types[0])) {
                const char **emb =
                    (const char **)cbm_arena_alloc(ctx.arena, (size_t)(gc + 1) * sizeof(char *));
                if (emb) {
                    for (int g = 0; g < gc; g++)
                        emb[g] = cbm_arena_strdup(ctx.arena, gps[g]);
                    emb[gc] = NULL;
                    rt->embedded_types = emb;
                }
            }
            for (int g = 0; g < gc && wl_tail < PERL_CHAIN_CAP; g++) {
                if (gps[g] && gps[g][0])
                    worklist[wl_tail++] = gps[g];
            }
        }
    }

    /* Finalize into a per-call scratch index arena (see cbm_run_perl_lsp). */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    cbm_registry_finalize_into(&reg, &idx_arena);

    perl_lsp_process_file(&ctx, root);

    cbm_arena_destroy(&idx_arena);
    if (owns_tree && tree)
        ts_tree_delete(tree);
    if (parser)
        ts_parser_delete(parser);
}
