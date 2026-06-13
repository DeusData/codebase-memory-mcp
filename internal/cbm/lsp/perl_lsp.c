/*
 * perl_lsp.c — Perl Light Semantic Pass (skeleton).
 *
 * In-process type-aware call resolver for Perl. Mirrors the php_lsp.c /
 * go_lsp.c shape:
 *   1. Build a CBMTypeRegistry from file-local definitions + stdlib
 *      (perlfunc builtins + curated CPAN types).
 *   2. Walk top-level: collect `package` declarations, @ISA / `use parent`
 *      inheritance, and Exporter-style `use` imports.
 *   3. Walk each sub body, push scope, track bless var→class, and resolve
 *      method/function call expressions.
 *
 * This file currently contains only the scaffold: perl_lsp_init plus an
 * inert cbm_run_perl_lsp that runs the three phases as safe no-ops. Real
 * call resolution lands in a later plan (22-03); cbm_run_perl_lsp here MUST
 * NOT emit any resolved-call edges.
 */

#include "perl_lsp.h"
#include "../helpers.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PERL_EVAL_MAX_DEPTH 32

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

/* ── public API ─────────────────────────────────────────────────── */

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
    /* TODO(plan 22-03): grow use_local_names/use_target_qns and record the
     * mapping. Inert for now so the skeleton emits no edges. */
    (void)ctx;
    (void)local_name;
    (void)target_qn;
}

void perl_lsp_process_file(PerlLSPContext *ctx, TSNode root) {
    /* TODO(plan 22-03): walk packages, @ISA, `use` imports, and sub bodies,
     * resolving calls into ctx->resolved_calls. Empty walk for the skeleton. */
    (void)ctx;
    (void)root;
}

const CBMType *perl_eval_expr_type(PerlLSPContext *ctx, TSNode node) {
    /* TODO(plan 22-03): evaluate expression types. */
    (void)ctx;
    (void)node;
    return cbm_type_unknown();
}

const char *perl_resolve_package_name(PerlLSPContext *ctx, const char *name) {
    /* TODO(plan 22-03): resolve via current package + export import map. */
    (void)ctx;
    return name;
}

const CBMRegisteredFunc *perl_lookup_method(PerlLSPContext *ctx, const char *package_qn,
                                            const char *method_name) {
    /* TODO(plan 22-03): walk the @ISA chain in the registry. */
    (void)ctx;
    (void)package_qn;
    (void)method_name;
    return NULL;
}

/* ── entry: cbm_run_perl_lsp ────────────────────────────────────── */

void cbm_run_perl_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                      TSNode root) {
    if (!result || !arena || ts_node_is_null(root))
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    /* Phase A: register stdlib types/functions. */
    cbm_perl_stdlib_register(&reg, arena);

    const char *module_qn = result->module_qn;

    /* Phase B: register functions/methods from this file's defs. Return types
     * are left unknown for the skeleton — real inference lands in plan 22-03. */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name || !d->label)
            continue;

        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;
            if (strcmp(d->label, "Method") == 0 && d->parent_class) {
                rf.receiver_type = d->parent_class;
            }
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

    /* Phase C: run the resolver. The skeleton initializes context and runs an
     * empty walk; no resolved-call edges are emitted yet (plan 22-03). */
    PerlLSPContext ctx;
    perl_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, &result->resolved_calls);
    perl_lsp_process_file(&ctx, root);

    /* Silence unused-helper warnings until plan 22-03 wires these in. */
    (void)perl_node_text;
    (void)perl_pkg_to_dot;
    (void)perl_lsp_add_use;
    (void)perl_eval_expr_type;
    (void)perl_resolve_package_name;
    (void)perl_lookup_method;
}
