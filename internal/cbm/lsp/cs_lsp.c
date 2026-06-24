/*
 * cs_lsp.c — C# Light Semantic Pass.
 *
 * Reverse-engineered from Roslyn (dotnet/roslyn). Roslyn's resolution
 * pipeline is built on a chain of `Binder` objects representing lexical
 * scopes (namespace, type, member, block). LookupSymbolsInternal walks
 * the chain inside-out and merges results. We model the same chain with
 * a CBMScope stack plus a CSLSPContext that tracks type/namespace/using
 * state that does not naturally live in the value scope.
 *
 * Resolution order for a bare identifier `X` inside member `M` in type
 * `T` in namespace `N`:
 *
 *    1. local variables / parameters in the current block          (CBMScope)
 *    2. type members of T (and base types, transitively)           (registry)
 *    3. type members of any enclosing-type chain                   (registry)
 *    4. members of N, then enclosing namespaces outward            (registry)
 *    5. types/aliases in `using` directives (in declaration order) (CSUsing)
 *    6. static members imported by `using static`                  (CSUsing)
 *    7. extension methods imported by `using` directives           (registry)
 *
 * All lookups eventually short-circuit to a registry.
 *
 * tree-sitter-c-sharp grammar reference (relevant node kinds):
 *   compilation_unit, namespace_declaration, file_scoped_namespace_declaration,
 *   using_directive, class_declaration, struct_declaration, record_declaration,
 *   interface_declaration, enum_declaration, base_list, type_parameter_list,
 *   method_declaration, constructor_declaration, property_declaration,
 *   indexer_declaration, field_declaration, event_declaration,
 *   parameter_list, parameter, type_parameter_constraints_clause,
 *   block, local_declaration_statement, variable_declaration,
 *   variable_declarator, expression_statement, return_statement,
 *   if_statement, switch_statement, for_statement, foreach_statement,
 *   identifier, qualified_name, generic_name, member_access_expression,
 *   invocation_expression, object_creation_expression, anonymous_object_creation_expression,
 *   implicit_object_creation_expression, lambda_expression, conditional_expression,
 *   tuple_expression, await_expression, cast_expression, as_expression,
 *   is_expression, predefined_type, nullable_type, array_type, pointer_type,
 *   tuple_type, function_pointer_type.
 */

#include "cs_lsp.h"
#include "lsp_node_iter.h"
#include "../helpers.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CS_EVAL_MAX_DEPTH 64
#define CS_USING_INITIAL_CAP 16
#define CS_NAMESPACE_INITIAL_CAP 8
#define CS_LSP_PARENT_WALK_MAX 32

extern const TSLanguage *tree_sitter_c_sharp(void);

/* ── forward decls ──────────────────────────────────────────────── */

static void cs_resolve_calls_in_node(CSLSPContext *ctx, TSNode node);
static void cs_process_function_like(CSLSPContext *ctx, TSNode node);
static void cs_process_type_decl(CSLSPContext *ctx, TSNode node);
static const CBMType *cs_eval_invocation_type(CSLSPContext *ctx, TSNode call);
static const CBMType *cs_eval_member_access_type(CSLSPContext *ctx, TSNode node);
static const CBMType *cs_eval_object_creation_type(CSLSPContext *ctx, TSNode node);
static const CBMType *cs_eval_identifier_type(CSLSPContext *ctx, TSNode node);
static const CBMType *cs_substitute_type_params(CBMArena *arena, const CBMType *t,
                                                 const char **param_names,
                                                 const CBMType **param_args);
static void cs_collect_imports(CSLSPContext *ctx, TSNode root);
static void cs_collect_namespace(CSLSPContext *ctx, TSNode ns_node, bool file_scoped);
static const char *cs_namespace_qn(CSLSPContext *ctx);
static void cs_register_type_decls(CSLSPContext *ctx, CBMTypeRegistry *reg, TSNode root);
static char *cs_node_text_cached(CSLSPContext *ctx, TSNode node);
static const CBMType *cs_unwrap_task(CSLSPContext *ctx, const CBMType *t);
static const CBMType *cs_unwrap_nullable(const CBMType *t);

/* ── small helpers ──────────────────────────────────────────────── */

static char *cs_node_text(CSLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

static char *cs_node_text_cached(CSLSPContext *ctx, TSNode node) {
    return cs_node_text(ctx, node);
}

static bool cs_node_is(TSNode n, const char *kind) {
    if (ts_node_is_null(n)) return false;
    return strcmp(ts_node_type(n), kind) == 0;
}

static TSNode cs_child_named_kind(TSNode parent, const char *kind) {
    if (ts_node_is_null(parent)) return parent;
    uint32_t nc = ts_node_child_count(parent);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(parent, i);
        if (!ts_node_is_null(c) && strcmp(ts_node_type(c), kind) == 0) return c;
    }
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

static TSNode cs_first_named_child(TSNode parent) {
    if (ts_node_is_null(parent)) return parent;
    uint32_t nc = ts_node_child_count(parent);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(parent, i);
        if (!ts_node_is_null(c) && ts_node_is_named(c)) return c;
    }
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

/* Last segment after '.'. */
static const char *cs_short_name(const char *qn) {
    if (!qn) return NULL;
    const char *last = qn;
    for (const char *p = qn; *p; p++) {
        if (*p == '.') last = p + 1;
    }
    return last;
}

/* C# uses '.' separators natively; no conversion needed for identifiers.
 * Strip leading "global::" / "::" if present. */
static const char *cs_strip_global(const char *name) {
    if (!name) return name;
    if (strncmp(name, "global::", 8) == 0) return name + 8;
    if (strncmp(name, "::", 2) == 0) return name + 2;
    return name;
}

static char *cs_normalize_name(CBMArena *a, const char *name) {
    if (!name) return NULL;
    name = cs_strip_global(name);
    /* Replace "::" with "." (C# scope qualifier in source forms like
     * `global::System.Foo` becomes `System.Foo`). */
    size_t n = strlen(name);
    char *out = (char *)cbm_arena_alloc(a, n + 1);
    if (!out) return NULL;
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        if (name[i] == ':' && i + 1 < n && name[i + 1] == ':') {
            out[w++] = '.';
            i++;
            continue;
        }
        out[w++] = name[i];
    }
    out[w] = '\0';
    return out;
}

/* Walk up from `name` past any leading '<' or whitespace. */
static char *cs_strip_generic_args(CBMArena *a, const char *name) {
    if (!name) return NULL;
    const char *lt = strchr(name, '<');
    if (!lt) return cbm_arena_strdup(a, name);
    size_t len = (size_t)(lt - name);
    return cbm_arena_strndup(a, name, len);
}

/* Predefined C# type aliases: int → System.Int32, etc. */
static const char *cs_predefined_alias(const char *name) {
    if (!name) return NULL;
    /* Roslyn predefined-type list: Binder_Symbols.cs BindPredefinedTypeSymbol */
    if (strcmp(name, "int") == 0)     return "System.Int32";
    if (strcmp(name, "uint") == 0)    return "System.UInt32";
    if (strcmp(name, "long") == 0)    return "System.Int64";
    if (strcmp(name, "ulong") == 0)   return "System.UInt64";
    if (strcmp(name, "short") == 0)   return "System.Int16";
    if (strcmp(name, "ushort") == 0)  return "System.UInt16";
    if (strcmp(name, "byte") == 0)    return "System.Byte";
    if (strcmp(name, "sbyte") == 0)   return "System.SByte";
    if (strcmp(name, "float") == 0)   return "System.Single";
    if (strcmp(name, "double") == 0)  return "System.Double";
    if (strcmp(name, "decimal") == 0) return "System.Decimal";
    if (strcmp(name, "bool") == 0)    return "System.Boolean";
    if (strcmp(name, "char") == 0)    return "System.Char";
    if (strcmp(name, "string") == 0)  return "System.String";
    if (strcmp(name, "object") == 0)  return "System.Object";
    if (strcmp(name, "nint") == 0)    return "System.IntPtr";
    if (strcmp(name, "nuint") == 0)   return "System.UIntPtr";
    if (strcmp(name, "void") == 0)    return "System.Void";
    if (strcmp(name, "dynamic") == 0) return "System.Object"; /* dynamic ≈ object */
    return NULL;
}

static bool cs_is_keyword_self(const char *name) {
    if (!name) return false;
    return strcmp(name, "this") == 0 || strcmp(name, "base") == 0;
}

/* ── init ───────────────────────────────────────────────────────── */

void cs_lsp_init(CSLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                 const CBMTypeRegistry *registry, const char *module_qn,
                 CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->module_qn = module_qn;
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL);

    /* Implicit `using System;` — C# always brings in System. We pretend
     * Program.cs has it explicitly so primitive aliases and System.Console
     * resolve out of the box. The real LSP picks this up from the project
     * file's <ImplicitUsings>; we just always include it. */
    cs_lsp_add_using(ctx, CBM_CS_USING_NAMESPACE, "", "System", false);

    const char *dbg = getenv("CBM_LSP_DEBUG");
    ctx->debug = (dbg && dbg[0]);
}

/* ── using management ───────────────────────────────────────────── */

void cs_lsp_add_using(CSLSPContext *ctx, CBMCSUsingKind kind, const char *local_name,
                      const char *target_qn, bool is_global) {
    if (!target_qn) return;
    /* Dedupe by (kind, local_name, target). */
    for (int i = 0; i < ctx->using_count; i++) {
        CBMCSUsing *u = &ctx->usings[i];
        if (u->kind != kind) continue;
        if (strcmp(u->target_qn, target_qn) != 0) continue;
        const char *a = local_name ? local_name : "";
        const char *b = u->local_name ? u->local_name : "";
        if (strcmp(a, b) != 0) continue;
        return;
    }
    if (ctx->using_count >= ctx->using_cap) {
        int new_cap = ctx->using_cap ? ctx->using_cap * 2 : CS_USING_INITIAL_CAP;
        CBMCSUsing *nu = (CBMCSUsing *)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*nu));
        if (!nu) return;
        for (int i = 0; i < ctx->using_count; i++) nu[i] = ctx->usings[i];
        ctx->usings = nu;
        ctx->using_cap = new_cap;
    }
    CBMCSUsing *slot = &ctx->usings[ctx->using_count++];
    slot->kind = kind;
    slot->local_name = local_name ? cbm_arena_strdup(ctx->arena, local_name) : "";
    slot->target_qn = cbm_arena_strdup(ctx->arena, target_qn);
    slot->is_global = is_global;
}

/* ── namespace stack ────────────────────────────────────────────── */

static void cs_namespace_push(CSLSPContext *ctx, const char *ns_name) {
    if (!ns_name || !*ns_name) return;
    if (ctx->namespace_count >= ctx->namespace_cap) {
        int new_cap = ctx->namespace_cap ? ctx->namespace_cap * 2 : CS_NAMESPACE_INITIAL_CAP;
        const char **arr = (const char **)cbm_arena_alloc(ctx->arena,
                                                          (size_t)new_cap * sizeof(*arr));
        if (!arr) return;
        for (int i = 0; i < ctx->namespace_count; i++) arr[i] = ctx->namespace_stack[i];
        ctx->namespace_stack = arr;
        ctx->namespace_cap = new_cap;
    }
    ctx->namespace_stack[ctx->namespace_count++] = cbm_arena_strdup(ctx->arena, ns_name);
}

static void cs_namespace_pop(CSLSPContext *ctx) {
    if (ctx->namespace_count > 0) ctx->namespace_count--;
}

/* Concatenate the namespace stack into a dotted QN (outer to inner). */
static const char *cs_namespace_qn(CSLSPContext *ctx) {
    if (ctx->namespace_count == 0) return "";
    /* Compute total length first. */
    size_t total = 0;
    for (int i = 0; i < ctx->namespace_count; i++) {
        total += strlen(ctx->namespace_stack[i]);
        if (i > 0) total += 1; /* dot */
    }
    char *out = (char *)cbm_arena_alloc(ctx->arena, total + 1);
    if (!out) return "";
    size_t w = 0;
    for (int i = 0; i < ctx->namespace_count; i++) {
        if (i > 0) out[w++] = '.';
        size_t len = strlen(ctx->namespace_stack[i]);
        memcpy(out + w, ctx->namespace_stack[i], len);
        w += len;
    }
    out[w] = '\0';
    return out;
}

/* ── type-name resolution ───────────────────────────────────────── */

static const CBMRegisteredType *cs_lookup_type_qn(CSLSPContext *ctx, const char *qn) {
    if (!ctx->registry || !qn) return NULL;
    return cbm_registry_lookup_type(ctx->registry, qn);
}

/* Try a candidate QN; if found, return it (interned in arena). */
static const char *cs_try_type_qn(CSLSPContext *ctx, const char *qn) {
    if (!qn) return NULL;
    if (cs_lookup_type_qn(ctx, qn)) return qn;
    return NULL;
}

/* Returns a fully-qualified type name resolved against:
 *   1. predefined alias (int -> System.Int32)
 *   2. type-parameter substitution map
 *   3. exact registry hit
 *   4. nested type / current type (for unqualified inside-class refs)
 *   5. each namespace prefix from innermost outward
 *   6. module-prefixed (file-local QN that the unified extractor produced)
 *   7. each `using namespace X` prefix
 *   8. each `using A = X` alias substitution
 *   9. fall back to bare name (registry will fail; pipeline drops the call)
 */
const char *cs_resolve_type_name(CSLSPContext *ctx, const char *raw) {
    if (!raw || !*raw) return NULL;
    char *name = cs_normalize_name(ctx->arena, raw);
    if (!name) return NULL;

    /* Strip generic args for lookup purposes; the caller can re-attach
     * a TEMPLATE wrapping. */
    char *bare = cs_strip_generic_args(ctx->arena, name);
    if (!bare) bare = name;

    /* 1. Predefined. */
    const char *pre = cs_predefined_alias(bare);
    if (pre) return pre;

    /* 2. Type-parameter substitution. */
    for (int i = 0; i < ctx->type_param_count; i++) {
        if (strcmp(ctx->type_param_names[i], bare) == 0) {
            const CBMType *arg = ctx->type_param_args[i];
            if (arg && arg->kind == CBM_TYPE_NAMED) return arg->data.named.qualified_name;
            /* Builtin or unknown — fall through. */
        }
    }

    /* 3. Exact registry hit. */
    if (cs_lookup_type_qn(ctx, bare)) return bare;

    /* 4. Inside the current class? Try enclosing class's nested type. */
    if (ctx->enclosing_class_qn) {
        const char *try_qn =
            cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->enclosing_class_qn, bare);
        if (cs_lookup_type_qn(ctx, try_qn)) return try_qn;
    }

    /* 5. Each namespace prefix from innermost outward. */
    if (ctx->namespace_count > 0) {
        const char *ns = cs_namespace_qn(ctx);
        for (;;) {
            if (!ns || !*ns) break;
            const char *try_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ns, bare);
            if (cs_lookup_type_qn(ctx, try_qn)) return try_qn;
            const char *dot = strrchr(ns, '.');
            if (!dot) break;
            char *trim = cbm_arena_strndup(ctx->arena, ns, (size_t)(dot - ns));
            ns = trim;
        }
    }

    /* 6. Module-prefixed (matches what unified extractor produces for file-
     * local types). */
    if (ctx->module_qn && ctx->module_qn[0]) {
        const char *try_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, bare);
        if (cs_lookup_type_qn(ctx, try_qn)) return try_qn;
    }

    /* 7. using namespace X — try X.bare. */
    for (int i = 0; i < ctx->using_count; i++) {
        const CBMCSUsing *u = &ctx->usings[i];
        if (u->kind != CBM_CS_USING_NAMESPACE) continue;
        const char *try_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", u->target_qn, bare);
        if (cs_lookup_type_qn(ctx, try_qn)) return try_qn;
    }

    /* 8. using A = X — substitute. The target may include generic args
     * (e.g. `using IL = ...List<int>`). The registry stores the bare
     * type name only, so we strip generic args before returning. */
    for (int i = 0; i < ctx->using_count; i++) {
        const CBMCSUsing *u = &ctx->usings[i];
        if (u->kind != CBM_CS_USING_ALIAS) continue;
        if (!u->local_name) continue;
        size_t alias_len = strlen(u->local_name);
        if (strncmp(bare, u->local_name, alias_len) == 0) {
            if (bare[alias_len] == '\0') {
                /* Whole name is the alias. Strip generic args from target. */
                char *target_bare = cs_strip_generic_args(ctx->arena, u->target_qn);
                if (target_bare && cs_lookup_type_qn(ctx, target_bare))
                    return target_bare;
                return cbm_arena_strdup(ctx->arena, u->target_qn);
            }
            if (bare[alias_len] == '.') {
                /* `Alias.Sub` -> `Target.Sub` */
                char *target_bare = cs_strip_generic_args(ctx->arena, u->target_qn);
                const char *base = target_bare ? target_bare : u->target_qn;
                const char *try_qn = cbm_arena_sprintf(ctx->arena, "%s%s", base,
                                                        bare + alias_len);
                if (cs_lookup_type_qn(ctx, try_qn)) return try_qn;
            }
        }
    }

    /* 9. Short-name fallback: scan the registry for any type whose
     * short_name equals `bare` and whose QN shares the longest namespace
     * prefix with the file's namespace stack. This recovers cross-file
     * lookups that the namespace heuristics miss when the file's
     * `namespace` declaration doesn't line up with the directory tree. */
    {
        const char *the_short = cs_short_name(bare);
        if (the_short && *the_short) {
            const CBMRegisteredType *best = NULL;
            int best_score = -1;
            const char *namespace_dotted = cs_namespace_qn(ctx);
            for (int i = 0; ctx->registry && i < ctx->registry->type_count; i++) {
                const CBMRegisteredType *cand = &ctx->registry->types[i];
                if (!cand->short_name || strcmp(cand->short_name, the_short) != 0) continue;
                int score = 0;
                if (cand->qualified_name && namespace_dotted && *namespace_dotted) {
                    const char *m = namespace_dotted;
                    const char *q = cand->qualified_name;
                    while (*m && *q && *m == *q) {
                        if (*m == '.') score++;
                        m++;
                        q++;
                    }
                }
                if (score > best_score) {
                    best_score = score;
                    best = cand;
                }
            }
            if (best) return best->qualified_name;
        }
    }

    /* 10. Last-resort: return bare so the caller can build a NAMED type even
     * if not registered. The registry-miss path simply drops the edge. */
    return bare;
}

/* Look up a method on a type, walking inheritance chain. */
const CBMRegisteredFunc *cs_lookup_method(CSLSPContext *ctx, const char *type_qn,
                                           const char *method_name) {
    if (!type_qn || !method_name) return NULL;

    const CBMRegisteredFunc *f =
        cbm_registry_lookup_method(ctx->registry, type_qn, method_name);
    if (f) return f;

    /* Walk inheritance chain (base + interfaces + transitive bases). */
    const CBMRegisteredType *t = cs_lookup_type_qn(ctx, type_qn);
    if (!t) return NULL;

    const char *visited[CS_LSP_PARENT_WALK_MAX];
    int visited_count = 0;
    const char *frontier[CS_LSP_PARENT_WALK_MAX];
    int frontier_count = 0;

    if (t->embedded_types) {
        for (int i = 0; t->embedded_types[i] && frontier_count < CS_LSP_PARENT_WALK_MAX; i++) {
            frontier[frontier_count++] = t->embedded_types[i];
        }
    }

    /* Always try System.Object as the universal root. */
    while (frontier_count > 0 && visited_count < CS_LSP_PARENT_WALK_MAX) {
        const char *parent = frontier[--frontier_count];
        bool seen = false;
        for (int v = 0; v < visited_count; v++) {
            if (strcmp(visited[v], parent) == 0) {
                seen = true;
                break;
            }
        }
        if (seen) continue;
        visited[visited_count++] = parent;

        f = cbm_registry_lookup_method(ctx->registry, parent, method_name);
        if (f) return f;

        const CBMRegisteredType *next = cs_lookup_type_qn(ctx, parent);
        if (!next) continue;
        if (next->embedded_types) {
            for (int i = 0;
                 next->embedded_types[i] && frontier_count < CS_LSP_PARENT_WALK_MAX; i++) {
                frontier[frontier_count++] = next->embedded_types[i];
            }
        }
    }

    /* Fall back to System.Object root members (ToString, Equals, GetHashCode). */
    if (strcmp(type_qn, "System.Object") != 0) {
        f = cbm_registry_lookup_method(ctx->registry, "System.Object", method_name);
        if (f) return f;
    }
    return NULL;
}

/* ── extension methods ──────────────────────────────────────────── */

/* Search for a static extension method `M(this U self, ...)` accessible
 * via using-imported namespaces. Returns the func entry or NULL. */
static const CBMRegisteredFunc *cs_lookup_extension(CSLSPContext *ctx,
                                                     const char *receiver_qn,
                                                     const char *method_name) {
    if (!ctx->registry || !receiver_qn || !method_name) return NULL;
    /* Walk every static class accessible in scope. We approximate accessibility
     * by checking that the function's qualified_name's first dotted prefixes
     * line up with one of: the file's namespace, an imported using namespace,
     * or the file's module. */
    for (int i = 0; i < ctx->registry->func_count; i++) {
        const CBMRegisteredFunc *cand = &ctx->registry->funcs[i];
        if (!cand->short_name || strcmp(cand->short_name, method_name) != 0) continue;
        if (cand->receiver_type) continue; /* must be a free static method */
        if (!cand->signature || cand->signature->kind != CBM_TYPE_FUNC) continue;
        const CBMType *sig = cand->signature;
        if (!sig->data.func.param_types) continue;
        const CBMType *first = sig->data.func.param_types[0];
        if (!first) continue;
        /* Only consider candidates marked as extensions (param[0] is named
         * "this" in our convention). param_names[0] == "this self" or "this". */
        if (!sig->data.func.param_names || !sig->data.func.param_names[0]) continue;
        const char *p0 = sig->data.func.param_names[0];
        if (strncmp(p0, "this", 4) != 0) continue;

        /* Receiver compatibility: NAMED receiver must match receiver_qn or
         * one of receiver_qn's bases. TEMPLATE receiver (e.g. IEnumerable<T>)
         * matches if receiver type or any base shares the template name. */
        bool match = false;
        if (first->kind == CBM_TYPE_NAMED && first->data.named.qualified_name) {
            if (strcmp(first->data.named.qualified_name, receiver_qn) == 0) match = true;
            if (!match && strcmp(first->data.named.qualified_name, "System.Object") == 0)
                match = true;
            /* Walk receiver's bases. */
            const CBMRegisteredType *rt = cs_lookup_type_qn(ctx, receiver_qn);
            if (rt && rt->embedded_types && !match) {
                for (int j = 0; rt->embedded_types[j]; j++) {
                    if (strcmp(rt->embedded_types[j],
                               first->data.named.qualified_name) == 0) {
                        match = true;
                        break;
                    }
                }
            }
        } else if (first->kind == CBM_TYPE_TEMPLATE) {
            const char *tn = first->data.template_type.template_name;
            const CBMRegisteredType *rt = cs_lookup_type_qn(ctx, receiver_qn);
            if (rt && rt->embedded_types) {
                for (int j = 0; rt->embedded_types[j]; j++) {
                    if (strcmp(rt->embedded_types[j], tn) == 0) {
                        match = true;
                        break;
                    }
                }
            }
            if (!match && strcmp(receiver_qn, tn) == 0) match = true;
        }
        if (!match) continue;

        /* Accessibility check: the candidate's namespace must be in
         * usings or namespace stack. We'll be lenient here — the user's
         * project tree generally has consistent namespaces. */
        return cand;
    }
    return NULL;
}

/* ── type AST parsing ───────────────────────────────────────────── */

const CBMType *cs_parse_type_node(CSLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node)) return cbm_type_unknown();
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "predefined_type") == 0) {
        char *t = cs_node_text(ctx, node);
        if (!t) return cbm_type_unknown();
        const char *aliased = cs_predefined_alias(t);
        if (aliased) return cbm_type_named(ctx->arena, aliased);
        return cbm_type_builtin(ctx->arena, t);
    }
    if (strcmp(kind, "nullable_type") == 0) {
        /* Recurse into the underlying. */
        TSNode inner = ts_node_child_by_field_name(node, "type", 4);
        if (ts_node_is_null(inner)) inner = cs_first_named_child(node);
        if (ts_node_is_null(inner)) return cbm_type_unknown();
        return cs_parse_type_node(ctx, inner);
    }
    if (strcmp(kind, "array_type") == 0) {
        TSNode inner = ts_node_child_by_field_name(node, "type", 4);
        if (ts_node_is_null(inner)) inner = cs_first_named_child(node);
        const CBMType *elem = cs_parse_type_node(ctx, inner);
        return cbm_type_template(ctx->arena, "System.Array",
                                 (const CBMType *[]){elem, NULL}, 1);
    }
    if (strcmp(kind, "pointer_type") == 0) {
        TSNode inner = ts_node_child_by_field_name(node, "type", 4);
        if (ts_node_is_null(inner)) inner = cs_first_named_child(node);
        return cs_parse_type_node(ctx, inner);
    }
    if (strcmp(kind, "tuple_type") == 0) {
        /* Build a TUPLE of element types. */
        uint32_t nc = ts_node_child_count(node);
        const CBMType **elems = (const CBMType **)cbm_arena_alloc(
            ctx->arena, (size_t)(nc + 1) * sizeof(*elems));
        if (!elems) return cbm_type_unknown();
        int count = 0;
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "tuple_element") != 0) continue;
            TSNode te = ts_node_child_by_field_name(c, "type", 4);
            if (ts_node_is_null(te)) te = cs_first_named_child(c);
            elems[count++] = cs_parse_type_node(ctx, te);
        }
        return cbm_type_tuple(ctx->arena, elems, count);
    }
    if (strcmp(kind, "generic_name") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (ts_node_is_null(name_node)) name_node = cs_child_named_kind(node, "identifier");
        TSNode args_node = ts_node_child_by_field_name(node, "type_arguments", 14);
        if (ts_node_is_null(args_node))
            args_node = cs_child_named_kind(node, "type_argument_list");
        if (ts_node_is_null(name_node)) return cbm_type_unknown();
        char *raw = cs_node_text(ctx, name_node);
        if (!raw) return cbm_type_unknown();
        const char *resolved = cs_resolve_type_name(ctx, raw);
        if (!resolved) return cbm_type_unknown();

        if (ts_node_is_null(args_node)) {
            return cbm_type_named(ctx->arena, resolved);
        }
        /* Parse args. */
        uint32_t nc = ts_node_child_count(args_node);
        const CBMType **args = (const CBMType **)cbm_arena_alloc(
            ctx->arena, (size_t)(nc + 1) * sizeof(*args));
        if (!args) return cbm_type_named(ctx->arena, resolved);
        int count = 0;
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(args_node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
            args[count++] = cs_parse_type_node(ctx, c);
        }
        args[count] = NULL;
        return cbm_type_template(ctx->arena, resolved, args, count);
    }
    if (strcmp(kind, "qualified_name") == 0 || strcmp(kind, "identifier") == 0 ||
        strcmp(kind, "alias_qualified_name") == 0 || strcmp(kind, "name") == 0) {
        char *t = cs_node_text(ctx, node);
        if (!t) return cbm_type_unknown();
        const char *resolved = cs_resolve_type_name(ctx, t);
        if (!resolved) return cbm_type_unknown();
        const char *pre = cs_predefined_alias(resolved);
        if (pre) return cbm_type_named(ctx->arena, pre);
        return cbm_type_named(ctx->arena, resolved);
    }
    if (strcmp(kind, "ref_type") == 0) {
        TSNode inner = ts_node_child_by_field_name(node, "type", 4);
        if (ts_node_is_null(inner)) inner = cs_first_named_child(node);
        const CBMType *t = cs_parse_type_node(ctx, inner);
        return cbm_type_reference(ctx->arena, t);
    }
    if (strcmp(kind, "implicit_type") == 0 || strcmp(kind, "var") == 0) {
        return cbm_type_unknown();
    }
    /* Fallback: read raw text + resolve. */
    char *t = cs_node_text(ctx, node);
    if (!t) return cbm_type_unknown();
    const char *resolved = cs_resolve_type_name(ctx, t);
    if (!resolved) return cbm_type_unknown();
    const char *pre = cs_predefined_alias(resolved);
    if (pre) return cbm_type_named(ctx->arena, pre);
    return cbm_type_named(ctx->arena, resolved);
}

/* ── expression evaluation ──────────────────────────────────────── */

static const CBMType *cs_unwrap_task(CSLSPContext *ctx, const CBMType *t) {
    (void)ctx;
    if (!t) return cbm_type_unknown();
    if (t->kind == CBM_TYPE_TEMPLATE) {
        const char *n = t->data.template_type.template_name;
        if (n && (strcmp(n, "System.Threading.Tasks.Task") == 0 ||
                   strcmp(n, "System.Threading.Tasks.ValueTask") == 0)) {
            if (t->data.template_type.template_args && t->data.template_type.template_args[0]) {
                return t->data.template_type.template_args[0];
            }
        }
    }
    if (t->kind == CBM_TYPE_NAMED) {
        const char *qn = t->data.named.qualified_name;
        if (qn && (strcmp(qn, "System.Threading.Tasks.Task") == 0 ||
                    strcmp(qn, "System.Threading.Tasks.ValueTask") == 0)) {
            return cbm_type_unknown();
        }
    }
    return t;
}

static const CBMType *cs_unwrap_nullable(const CBMType *t) {
    if (!t) return t;
    if (t->kind == CBM_TYPE_TEMPLATE) {
        const char *n = t->data.template_type.template_name;
        if (n && strcmp(n, "System.Nullable") == 0) {
            if (t->data.template_type.template_args && t->data.template_type.template_args[0]) {
                return t->data.template_type.template_args[0];
            }
        }
    }
    return t;
}

const CBMType *cs_eval_expr_type(CSLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node) || ctx->eval_depth >= CS_EVAL_MAX_DEPTH) {
        return cbm_type_unknown();
    }
    ctx->eval_depth++;
    const CBMType *result = cbm_type_unknown();
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "identifier") == 0) {
        result = cs_eval_identifier_type(ctx, node);
    } else if (strcmp(kind, "this_expression") == 0 || strcmp(kind, "this") == 0) {
        if (ctx->enclosing_class_qn) {
            result = cbm_type_named(ctx->arena, ctx->enclosing_class_qn);
        }
    } else if (strcmp(kind, "base_expression") == 0 || strcmp(kind, "base") == 0) {
        if (ctx->enclosing_base_qn) {
            result = cbm_type_named(ctx->arena, ctx->enclosing_base_qn);
        }
    } else if (strcmp(kind, "invocation_expression") == 0) {
        result = cs_eval_invocation_type(ctx, node);
    } else if (strcmp(kind, "member_access_expression") == 0 ||
               strcmp(kind, "conditional_access_expression") == 0) {
        result = cs_eval_member_access_type(ctx, node);
    } else if (strcmp(kind, "object_creation_expression") == 0 ||
               strcmp(kind, "implicit_object_creation_expression") == 0) {
        result = cs_eval_object_creation_type(ctx, node);
    } else if (strcmp(kind, "string_literal") == 0 ||
               strcmp(kind, "verbatim_string_literal") == 0 ||
               strcmp(kind, "interpolated_string_expression") == 0 ||
               strcmp(kind, "raw_string_literal") == 0) {
        result = cbm_type_named(ctx->arena, "System.String");
    } else if (strcmp(kind, "character_literal") == 0) {
        result = cbm_type_named(ctx->arena, "System.Char");
    } else if (strcmp(kind, "integer_literal") == 0) {
        result = cbm_type_named(ctx->arena, "System.Int32");
    } else if (strcmp(kind, "real_literal") == 0) {
        result = cbm_type_named(ctx->arena, "System.Double");
    } else if (strcmp(kind, "boolean_literal") == 0) {
        result = cbm_type_named(ctx->arena, "System.Boolean");
    } else if (strcmp(kind, "null_literal") == 0) {
        result = cbm_type_unknown();
    } else if (strcmp(kind, "parenthesized_expression") == 0) {
        TSNode c = cs_first_named_child(node);
        if (!ts_node_is_null(c)) result = cs_eval_expr_type(ctx, c);
    } else if (strcmp(kind, "cast_expression") == 0) {
        TSNode tnode = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(tnode)) result = cs_parse_type_node(ctx, tnode);
    } else if (strcmp(kind, "as_expression") == 0) {
        /* x as T → T */
        TSNode rhs = ts_node_child_by_field_name(node, "right", 5);
        if (ts_node_is_null(rhs)) {
            uint32_t nc = ts_node_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_child(node, i);
                if (!ts_node_is_null(c) && ts_node_is_named(c)) rhs = c;
            }
        }
        if (!ts_node_is_null(rhs)) result = cs_parse_type_node(ctx, rhs);
    } else if (strcmp(kind, "is_expression") == 0 || strcmp(kind, "is_pattern_expression") == 0) {
        result = cbm_type_named(ctx->arena, "System.Boolean");
    } else if (strcmp(kind, "await_expression") == 0) {
        TSNode inner = cs_first_named_child(node);
        if (!ts_node_is_null(inner)) {
            const CBMType *t = cs_eval_expr_type(ctx, inner);
            result = cs_unwrap_task(ctx, t);
        }
    } else if (strcmp(kind, "binary_expression") == 0 || strcmp(kind, "prefix_unary_expression") == 0 ||
               strcmp(kind, "postfix_unary_expression") == 0) {
        /* Best-effort: take left's type. For comparisons, return Boolean. */
        TSNode op = ts_node_child_by_field_name(node, "operator", 8);
        char *opt = ts_node_is_null(op) ? NULL : cs_node_text(ctx, op);
        if (opt && (strcmp(opt, "==") == 0 || strcmp(opt, "!=") == 0 ||
                     strcmp(opt, "<") == 0 || strcmp(opt, ">") == 0 ||
                     strcmp(opt, "<=") == 0 || strcmp(opt, ">=") == 0 ||
                     strcmp(opt, "&&") == 0 || strcmp(opt, "||") == 0 ||
                     strcmp(opt, "!") == 0)) {
            result = cbm_type_named(ctx->arena, "System.Boolean");
        } else {
            TSNode left = ts_node_child_by_field_name(node, "left", 4);
            if (ts_node_is_null(left)) left = cs_first_named_child(node);
            if (!ts_node_is_null(left)) result = cs_eval_expr_type(ctx, left);
        }
    } else if (strcmp(kind, "assignment_expression") == 0) {
        TSNode rhs = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(rhs)) result = cs_eval_expr_type(ctx, rhs);
    } else if (strcmp(kind, "conditional_expression") == 0 ||
               strcmp(kind, "switch_expression") == 0) {
        /* a ? b : c → take b's type. */
        TSNode b = ts_node_child_by_field_name(node, "consequence", 11);
        if (ts_node_is_null(b)) b = ts_node_child_by_field_name(node, "alternative", 11);
        if (ts_node_is_null(b)) {
            uint32_t nc = ts_node_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_child(node, i);
                if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                    const CBMType *t = cs_eval_expr_type(ctx, c);
                    if (t && t->kind != CBM_TYPE_UNKNOWN) {
                        result = t;
                        break;
                    }
                }
            }
        } else {
            result = cs_eval_expr_type(ctx, b);
        }
    } else if (strcmp(kind, "tuple_expression") == 0) {
        /* (a, b) -> tuple of element types. */
        uint32_t nc = ts_node_child_count(node);
        const CBMType **elems =
            (const CBMType **)cbm_arena_alloc(ctx->arena, (size_t)(nc + 1) * sizeof(*elems));
        if (!elems) {
            ctx->eval_depth--;
            return cbm_type_unknown();
        }
        int count = 0;
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
            elems[count++] = cs_eval_expr_type(ctx, c);
        }
        elems[count] = NULL;
        result = cbm_type_tuple(ctx->arena, elems, count);
    } else if (strcmp(kind, "lambda_expression") == 0 ||
               strcmp(kind, "anonymous_method_expression") == 0) {
        /* We approximate the lambda's type as System.Func<TR>; we don't
         * walk the body here. */
        result = cbm_type_unknown();
    } else if (strcmp(kind, "typeof_expression") == 0) {
        result = cbm_type_named(ctx->arena, "System.Type");
    } else if (strcmp(kind, "nameof_expression") == 0) {
        result = cbm_type_named(ctx->arena, "System.String");
    } else if (strcmp(kind, "default_expression") == 0) {
        TSNode tnode = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(tnode)) result = cs_parse_type_node(ctx, tnode);
    } else if (strcmp(kind, "element_access_expression") == 0) {
        /* arr[i] — return element type of receiver. */
        TSNode obj = ts_node_child_by_field_name(node, "expression", 10);
        if (ts_node_is_null(obj)) obj = cs_first_named_child(node);
        if (!ts_node_is_null(obj)) {
            const CBMType *recv = cs_eval_expr_type(ctx, obj);
            if (recv && recv->kind == CBM_TYPE_TEMPLATE) {
                const CBMType *const *args = recv->data.template_type.template_args;
                if (args) {
                    int n = 0;
                    while (args[n]) n++;
                    if (n >= 2 && args[1]) result = args[1];
                    else if (n >= 1 && args[0]) result = args[0];
                }
            }
        }
    } else if (strcmp(kind, "checked_expression") == 0 ||
               strcmp(kind, "unchecked_expression") == 0) {
        TSNode c = cs_first_named_child(node);
        if (!ts_node_is_null(c)) result = cs_eval_expr_type(ctx, c);
    } else if (strcmp(kind, "array_creation_expression") == 0) {
        TSNode tnode = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(tnode)) {
            const CBMType *elem = cs_parse_type_node(ctx, tnode);
            result = cbm_type_template(ctx->arena, "System.Array",
                                       (const CBMType *[]){elem, NULL}, 1);
        }
    } else if (strcmp(kind, "implicit_array_creation_expression") == 0) {
        result = cbm_type_template(ctx->arena, "System.Array",
                                   (const CBMType *[]){cbm_type_unknown(), NULL}, 1);
    }

    ctx->eval_depth--;
    return result ? result : cbm_type_unknown();
}

/* ── identifier resolution ──────────────────────────────────────── */

static const CBMType *cs_eval_identifier_type(CSLSPContext *ctx, TSNode node) {
    char *name = cs_node_text(ctx, node);
    if (!name) return cbm_type_unknown();

    /* Local / parameter scope. cbm_scope_lookup returns cbm_type_unknown()
     * (a non-NULL singleton) when the name isn't bound, so we need to
     * check the kind, not just the pointer. */
    const CBMType *bound = cbm_scope_lookup(ctx->current_scope, name);
    if (bound && bound->kind != CBM_TYPE_UNKNOWN) return bound;

    /* Implicit `this` member. Try to find a field on the enclosing class. */
    if (ctx->enclosing_class_qn) {
        const CBMRegisteredType *rt = cs_lookup_type_qn(ctx, ctx->enclosing_class_qn);
        if (rt && rt->field_names) {
            for (int i = 0; rt->field_names[i]; i++) {
                if (strcmp(rt->field_names[i], name) == 0) {
                    if (rt->field_types && rt->field_types[i]) return rt->field_types[i];
                    return cbm_type_unknown();
                }
            }
        }
    }

    /* Predefined alias: e.g. `int.Parse(...)` — but that's parsed as a
     * predefined_type, not identifier. Still: handle bare type names. */
    const char *resolved = cs_resolve_type_name(ctx, name);
    if (resolved) {
        const CBMRegisteredType *rt = cs_lookup_type_qn(ctx, resolved);
        if (rt) return cbm_type_named(ctx->arena, rt->qualified_name);
    }

    return cbm_type_unknown();
}

/* ── invocation ─────────────────────────────────────────────────── */

static int cs_count_args(TSNode args_node) {
    if (ts_node_is_null(args_node)) return 0;
    int count = 0;
    uint32_t nc = ts_node_child_count(args_node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(args_node, i);
        if (!ts_node_is_null(c) && ts_node_is_named(c)) {
            const char *k = ts_node_type(c);
            if (strcmp(k, "argument") == 0) count++;
        }
    }
    return count;
}

static const CBMType *cs_eval_invocation_type(CSLSPContext *ctx, TSNode call) {
    TSNode fn = ts_node_child_by_field_name(call, "function", 8);
    if (ts_node_is_null(fn)) {
        /* tree-sitter sometimes uses "expression" instead of "function". */
        uint32_t nc = ts_node_child_count(call);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(call, i);
            if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                const char *k = ts_node_type(c);
                if (strcmp(k, "argument_list") == 0) continue;
                fn = c;
                break;
            }
        }
    }
    if (ts_node_is_null(fn)) return cbm_type_unknown();
    const char *fk = ts_node_type(fn);

    /* Member call: `recv.Method` or `recv?.Method`. */
    if (strcmp(fk, "member_access_expression") == 0 ||
        strcmp(fk, "conditional_access_expression") == 0) {
        TSNode recv = ts_node_child_by_field_name(fn, "expression", 10);
        TSNode name = ts_node_child_by_field_name(fn, "name", 4);
        if (ts_node_is_null(recv)) recv = cs_first_named_child(fn);
        if (ts_node_is_null(name)) {
            uint32_t fnc = ts_node_child_count(fn);
            for (uint32_t i = 0; i < fnc; i++) {
                TSNode c = ts_node_child(fn, i);
                if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
                const char *k = ts_node_type(c);
                if (strcmp(k, "identifier") == 0 || strcmp(k, "generic_name") == 0) name = c;
            }
        }
        if (ts_node_is_null(name)) return cbm_type_unknown();
        char *mname = cs_node_text(ctx, name);
        if (!mname) return cbm_type_unknown();
        char *bare_name = cs_strip_generic_args(ctx->arena, mname);

        const CBMType *recv_type = cbm_type_unknown();
        if (!ts_node_is_null(recv)) recv_type = cs_eval_expr_type(ctx, recv);
        recv_type = cs_unwrap_nullable(recv_type);

        const char *type_qn = NULL;
        if (recv_type && recv_type->kind == CBM_TYPE_NAMED) {
            type_qn = recv_type->data.named.qualified_name;
        } else if (recv_type && recv_type->kind == CBM_TYPE_TEMPLATE) {
            type_qn = recv_type->data.template_type.template_name;
        }
        if (!type_qn) return cbm_type_unknown();

        const CBMRegisteredFunc *f = cs_lookup_method(ctx, type_qn, bare_name);
        if (!f) f = cs_lookup_extension(ctx, type_qn, bare_name);
        if (!f || !f->signature) return cbm_type_unknown();
        if (f->signature->kind == CBM_TYPE_FUNC && f->signature->data.func.return_types &&
            f->signature->data.func.return_types[0]) {
            const CBMType *ret = f->signature->data.func.return_types[0];
            /* If receiver is templated and method's return type contains type
             * parameters, substitute. */
            if (recv_type && recv_type->kind == CBM_TYPE_TEMPLATE) {
                const CBMRegisteredType *rt = cs_lookup_type_qn(ctx, type_qn);
                if (rt && rt->type_param_names &&
                    recv_type->data.template_type.template_args) {
                    return cs_substitute_type_params(
                        ctx->arena, ret, rt->type_param_names,
                        recv_type->data.template_type.template_args);
                }
            }
            return ret;
        }
        return cbm_type_unknown();
    }

    /* Bare invocation: `Method(args)`. */
    if (strcmp(fk, "identifier") == 0 || strcmp(fk, "generic_name") == 0) {
        char *fname = cs_node_text(ctx, fn);
        if (!fname) return cbm_type_unknown();
        char *bare = cs_strip_generic_args(ctx->arena, fname);

        /* Contextual keywords that tree-sitter parses as invocation
         * expressions but the C# language treats specially. */
        if (strcmp(bare, "nameof") == 0) {
            return cbm_type_named(ctx->arena, "System.String");
        }
        if (strcmp(bare, "typeof") == 0) {
            return cbm_type_named(ctx->arena, "System.Type");
        }

        /* Try enclosing class member. */
        if (ctx->enclosing_class_qn) {
            const CBMRegisteredFunc *f = cs_lookup_method(ctx, ctx->enclosing_class_qn, bare);
            if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC &&
                f->signature->data.func.return_types &&
                f->signature->data.func.return_types[0]) {
                return f->signature->data.func.return_types[0];
            }
        }
        /* Try free function in current namespace / using static targets. */
        const char *ns = cs_namespace_qn(ctx);
        if (ns && *ns) {
            const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ns, bare);
            const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, qn);
            if (f && f->signature && f->signature->data.func.return_types &&
                f->signature->data.func.return_types[0]) {
                return f->signature->data.func.return_types[0];
            }
        }
        for (int i = 0; i < ctx->using_count; i++) {
            const CBMCSUsing *u = &ctx->usings[i];
            if (u->kind != CBM_CS_USING_STATIC) continue;
            const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", u->target_qn, bare);
            const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, qn);
            if (f && f->signature && f->signature->data.func.return_types &&
                f->signature->data.func.return_types[0]) {
                return f->signature->data.func.return_types[0];
            }
        }
    }

    return cbm_type_unknown();
}

/* ── member access (no invocation) ──────────────────────────────── */

static const CBMType *cs_eval_member_access_type(CSLSPContext *ctx, TSNode node) {
    TSNode obj = ts_node_child_by_field_name(node, "expression", 10);
    TSNode name = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(obj)) obj = cs_first_named_child(node);
    if (ts_node_is_null(name)) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "identifier") == 0 || strcmp(k, "generic_name") == 0) name = c;
        }
    }
    if (ts_node_is_null(obj) || ts_node_is_null(name)) return cbm_type_unknown();
    char *fname = cs_node_text(ctx, name);
    if (!fname) return cbm_type_unknown();
    fname = cs_strip_generic_args(ctx->arena, fname);

    /* If obj is a type identifier → static member access */
    char *obj_text = cs_node_text(ctx, obj);
    if (obj_text) {
        const char *type_qn = cs_resolve_type_name(ctx, obj_text);
        if (type_qn && cs_lookup_type_qn(ctx, type_qn)) {
            const CBMRegisteredType *rt = cs_lookup_type_qn(ctx, type_qn);
            if (rt && rt->field_names) {
                for (int i = 0; rt->field_names[i]; i++) {
                    if (strcmp(rt->field_names[i], fname) == 0) {
                        if (rt->field_types && rt->field_types[i]) return rt->field_types[i];
                    }
                }
            }
            /* Try as method (returns its return type, which is what
             * `Math.Sqrt(...)` would propagate via invocation). */
            const CBMRegisteredFunc *f = cs_lookup_method(ctx, type_qn, fname);
            if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC &&
                f->signature->data.func.return_types &&
                f->signature->data.func.return_types[0]) {
                return f->signature->data.func.return_types[0];
            }
        }
    }

    const CBMType *recv = cs_eval_expr_type(ctx, obj);
    recv = cs_unwrap_nullable(recv);
    if (!recv) return cbm_type_unknown();

    const char *type_qn = NULL;
    if (recv->kind == CBM_TYPE_NAMED) {
        type_qn = recv->data.named.qualified_name;
    } else if (recv->kind == CBM_TYPE_TEMPLATE) {
        type_qn = recv->data.template_type.template_name;
    }
    if (!type_qn) return cbm_type_unknown();
    const CBMRegisteredType *rt = cs_lookup_type_qn(ctx, type_qn);
    if (rt && rt->field_names) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (strcmp(rt->field_names[i], fname) == 0) {
                if (rt->field_types && rt->field_types[i]) {
                    const CBMType *ft = rt->field_types[i];
                    if (recv->kind == CBM_TYPE_TEMPLATE && rt->type_param_names &&
                        recv->data.template_type.template_args) {
                        return cs_substitute_type_params(
                            ctx->arena, ft, rt->type_param_names,
                            recv->data.template_type.template_args);
                    }
                    return ft;
                }
            }
        }
    }
    /* Treat as property — call lookup_method with `get_<name>` or just `name`.
     * Properties are stored as methods in our registry with return type. */
    const CBMRegisteredFunc *f = cs_lookup_method(ctx, type_qn, fname);
    if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC &&
        f->signature->data.func.return_types && f->signature->data.func.return_types[0]) {
        const CBMType *ret = f->signature->data.func.return_types[0];
        if (recv->kind == CBM_TYPE_TEMPLATE && rt && rt->type_param_names &&
            recv->data.template_type.template_args) {
            return cs_substitute_type_params(ctx->arena, ret, rt->type_param_names,
                                             recv->data.template_type.template_args);
        }
        return ret;
    }

    return cbm_type_unknown();
}

/* ── object creation ────────────────────────────────────────────── */

static const CBMType *cs_eval_object_creation_type(CSLSPContext *ctx, TSNode node) {
    if (cs_node_is(node, "implicit_object_creation_expression")) {
        /* `new(...)` — target-typed; we don't have the target binding here. */
        return cbm_type_unknown();
    }
    TSNode tnode = ts_node_child_by_field_name(node, "type", 4);
    if (ts_node_is_null(tnode)) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "argument_list") == 0 || strcmp(k, "initializer_expression") == 0)
                continue;
            tnode = c;
            break;
        }
    }
    if (ts_node_is_null(tnode)) return cbm_type_unknown();
    return cs_parse_type_node(ctx, tnode);
}

/* ── generic substitution ───────────────────────────────────────── */

static const CBMType *cs_substitute_type_params(CBMArena *arena, const CBMType *t,
                                                 const char **param_names,
                                                 const CBMType **param_args) {
    if (!t || !param_names || !param_args) return t;
    if (t->kind == CBM_TYPE_NAMED) {
        const char *qn = t->data.named.qualified_name;
        if (!qn) return t;
        for (int i = 0; param_names[i]; i++) {
            if (strcmp(param_names[i], qn) == 0) {
                /* If the type is `T` and we have an arg for T, substitute. */
                return param_args[i] ? param_args[i] : t;
            }
        }
        return t;
    }
    if (t->kind == CBM_TYPE_TEMPLATE) {
        const CBMType *const *old_args = t->data.template_type.template_args;
        if (!old_args) return t;
        int n = 0;
        while (old_args[n]) n++;
        const CBMType **new_args = (const CBMType **)cbm_arena_alloc(
            arena, (size_t)(n + 1) * sizeof(*new_args));
        if (!new_args) return t;
        for (int i = 0; i < n; i++) {
            new_args[i] = cs_substitute_type_params(arena, old_args[i], param_names, param_args);
        }
        new_args[n] = NULL;
        return cbm_type_template(arena, t->data.template_type.template_name, new_args, n);
    }
    if (t->kind == CBM_TYPE_TYPE_PARAM) {
        const char *p = t->data.type_param.name;
        if (!p) return t;
        for (int i = 0; param_names[i]; i++) {
            if (strcmp(param_names[i], p) == 0) return param_args[i] ? param_args[i] : t;
        }
    }
    return t;
}

/* ── parameter binding ──────────────────────────────────────────── */

static void cs_bind_parameters(CSLSPContext *ctx, TSNode params_node, bool is_extension) {
    if (ts_node_is_null(params_node)) return;
    uint32_t nc = ts_node_child_count(params_node);
    int idx = 0;
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(params_node, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "parameter") != 0) continue;

        /* Detect `this` modifier (extension methods). */
        bool has_this = is_extension && (idx == 0);
        TSNode tnode = ts_node_child_by_field_name(c, "type", 4);
        TSNode nnode = ts_node_child_by_field_name(c, "name", 4);
        if (ts_node_is_null(tnode) || ts_node_is_null(nnode)) {
            uint32_t pc = ts_node_child_count(c);
            for (uint32_t j = 0; j < pc; j++) {
                TSNode cc = ts_node_child(c, j);
                if (ts_node_is_null(cc) || !ts_node_is_named(cc)) continue;
                const char *ck = ts_node_type(cc);
                if (strcmp(ck, "identifier") == 0 && ts_node_is_null(nnode)) nnode = cc;
                else if (ts_node_is_null(tnode) && strcmp(ck, "identifier") != 0) tnode = cc;
            }
        }
        if (ts_node_is_null(nnode)) {
            idx++;
            continue;
        }
        char *pname = cs_node_text(ctx, nnode);
        if (!pname) {
            idx++;
            continue;
        }
        const CBMType *ptype = cbm_type_unknown();
        if (!ts_node_is_null(tnode)) ptype = cs_parse_type_node(ctx, tnode);
        cbm_scope_bind(ctx->current_scope, pname, ptype);
        (void)has_this;
        idx++;
    }
}

/* ── statement processing ───────────────────────────────────────── */

static void cs_process_local_decl(CSLSPContext *ctx, TSNode node) {
    /* local_declaration_statement -> variable_declaration */
    TSNode vd = cs_child_named_kind(node, "variable_declaration");
    if (ts_node_is_null(vd)) return;
    TSNode tnode = ts_node_child_by_field_name(vd, "type", 4);
    if (ts_node_is_null(tnode)) tnode = cs_first_named_child(vd);
    /* For each declarator, bind the variable to (rhs_type or declared type). */
    uint32_t nc = ts_node_child_count(vd);
    bool is_var = false;
    if (!ts_node_is_null(tnode)) {
        char *tt = cs_node_text(ctx, tnode);
        if (tt && (strcmp(tt, "var") == 0 || strcmp(ts_node_type(tnode), "implicit_type") == 0)) {
            is_var = true;
        }
    }
    const CBMType *declared_type = ts_node_is_null(tnode) ? cbm_type_unknown()
                                                          : cs_parse_type_node(ctx, tnode);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(vd, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "variable_declarator") != 0) continue;
        TSNode nm = cs_child_named_kind(c, "identifier");
        if (ts_node_is_null(nm)) {
            nm = ts_node_child_by_field_name(c, "name", 4);
        }
        if (ts_node_is_null(nm)) continue;
        char *vname = cs_node_text(ctx, nm);
        if (!vname) continue;
        /* Find initializer (= rhs). variable_declarator children depend on
         * the grammar: tree-sitter-c-sharp emits `_identifier_or_global`
         * (the name) + optional `=` token + value-expression; the value
         * may also be wrapped in equals_value_clause in some grammar
         * variants. Try the field-name path first, then fall back to
         * walking named children for any expression after the identifier. */
        TSNode init = ts_node_child_by_field_name(c, "value", 5);
        const CBMType *rhs_t = NULL;
        if (!ts_node_is_null(init)) {
            if (strcmp(ts_node_type(init), "equals_value_clause") == 0) {
                TSNode rhs = cs_first_named_child(init);
                if (!ts_node_is_null(rhs)) rhs_t = cs_eval_expr_type(ctx, rhs);
            } else {
                rhs_t = cs_eval_expr_type(ctx, init);
            }
        }
        if (!rhs_t || rhs_t->kind == CBM_TYPE_UNKNOWN) {
            /* Walk named children: skip identifier (the name), evaluate
             * the next named child as the rhs expression. Also catch the
             * legacy equals_value_clause shape. */
            uint32_t cc = ts_node_child_count(c);
            int seen_named = 0;
            for (uint32_t j = 0; j < cc; j++) {
                TSNode cn = ts_node_child(c, j);
                if (ts_node_is_null(cn) || !ts_node_is_named(cn)) continue;
                const char *ck = ts_node_type(cn);
                if (strcmp(ck, "equals_value_clause") == 0) {
                    TSNode rhs = cs_first_named_child(cn);
                    if (!ts_node_is_null(rhs)) {
                        const CBMType *t = cs_eval_expr_type(ctx, rhs);
                        if (t && t->kind != CBM_TYPE_UNKNOWN) rhs_t = t;
                    }
                    break;
                }
                if (seen_named == 0) {
                    seen_named++;
                    continue; /* the identifier name */
                }
                if (seen_named >= 1) {
                    const CBMType *t = cs_eval_expr_type(ctx, cn);
                    if (t && t->kind != CBM_TYPE_UNKNOWN) {
                        rhs_t = t;
                        break;
                    }
                }
            }
        }
        const CBMType *bind = is_var ? (rhs_t ? rhs_t : declared_type) : declared_type;
        if (!bind) bind = cbm_type_unknown();
        cbm_scope_bind(ctx->current_scope, vname, bind);
    }
}

static void cs_process_foreach(CSLSPContext *ctx, TSNode node) {
    /* foreach_statement: type, identifier, expression. */
    TSNode tnode = ts_node_child_by_field_name(node, "type", 4);
    TSNode nnode = ts_node_child_by_field_name(node, "left", 4);
    if (ts_node_is_null(nnode)) nnode = cs_child_named_kind(node, "identifier");
    TSNode iter = ts_node_child_by_field_name(node, "right", 5);
    if (ts_node_is_null(iter)) {
        /* Fallback: look for an expression-shaped child after the type. */
        uint32_t nc = ts_node_child_count(node);
        bool past_id = false;
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "identifier") == 0 && !past_id) { past_id = true; continue; }
            if (past_id) {
                if (strcmp(k, "block") == 0) break;
                iter = c;
                break;
            }
        }
    }
    if (ts_node_is_null(nnode)) return;
    char *vname = cs_node_text(ctx, nnode);
    if (!vname) return;

    const CBMType *element_t = cbm_type_unknown();
    if (!ts_node_is_null(iter)) {
        const CBMType *iter_t = cs_eval_expr_type(ctx, iter);
        if (iter_t && iter_t->kind == CBM_TYPE_TEMPLATE) {
            const CBMType *const *args = iter_t->data.template_type.template_args;
            if (args && args[0]) element_t = args[0];
        }
    }
    if (!ts_node_is_null(tnode)) {
        const CBMType *declared = cs_parse_type_node(ctx, tnode);
        char *tt = cs_node_text(ctx, tnode);
        bool is_var = (tt && (strcmp(tt, "var") == 0));
        if (!is_var && declared && declared->kind != CBM_TYPE_UNKNOWN) element_t = declared;
    }
    cbm_scope_bind(ctx->current_scope, vname, element_t);
}

static void cs_process_using_statement(CSLSPContext *ctx, TSNode node) {
    /* `using (var x = expr) { ... }` — bind x. */
    TSNode vd = cs_child_named_kind(node, "variable_declaration");
    if (!ts_node_is_null(vd)) {
        /* Wrap as if it were a local_declaration_statement and dispatch. */
        TSNode wrapper;
        memset(&wrapper, 0, sizeof(wrapper));
        cs_process_local_decl(ctx, node);
    }
}

static void cs_process_assignment(CSLSPContext *ctx, TSNode node) {
    TSNode lhs = ts_node_child_by_field_name(node, "left", 4);
    TSNode rhs = ts_node_child_by_field_name(node, "right", 5);
    if (ts_node_is_null(lhs) || ts_node_is_null(rhs)) return;
    if (!cs_node_is(lhs, "identifier")) return;
    char *vname = cs_node_text(ctx, lhs);
    if (!vname) return;
    const CBMType *t = cs_eval_expr_type(ctx, rhs);
    if (!t || t->kind == CBM_TYPE_UNKNOWN) return;
    /* Don't override more-specific bindings with weaker ones. */
    const CBMType *existing = cbm_scope_lookup(ctx->current_scope, vname);
    if (existing && existing->kind == CBM_TYPE_NAMED && t->kind == CBM_TYPE_UNKNOWN) return;
    cbm_scope_bind(ctx->current_scope, vname, t);
}

/* ── call resolution + emit ─────────────────────────────────────── */

static void cs_emit_resolved(CSLSPContext *ctx, const char *callee_qn, const char *strategy,
                              float confidence) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn) return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    rc.reason = NULL;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

static void cs_resolve_invocation(CSLSPContext *ctx, TSNode call) {
    TSNode fn = ts_node_child_by_field_name(call, "function", 8);
    if (ts_node_is_null(fn)) {
        uint32_t nc = ts_node_child_count(call);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(call, i);
            if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                const char *k = ts_node_type(c);
                if (strcmp(k, "argument_list") == 0) continue;
                fn = c;
                break;
            }
        }
    }
    if (ts_node_is_null(fn)) return;
    const char *fk = ts_node_type(fn);

    /* Member call. */
    if (strcmp(fk, "member_access_expression") == 0 ||
        strcmp(fk, "conditional_access_expression") == 0) {
        TSNode recv = ts_node_child_by_field_name(fn, "expression", 10);
        TSNode name = ts_node_child_by_field_name(fn, "name", 4);
        if (ts_node_is_null(recv)) recv = cs_first_named_child(fn);
        if (ts_node_is_null(name)) {
            uint32_t fnc = ts_node_child_count(fn);
            for (uint32_t i = 0; i < fnc; i++) {
                TSNode c = ts_node_child(fn, i);
                if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                    const char *k = ts_node_type(c);
                    if (strcmp(k, "identifier") == 0 || strcmp(k, "generic_name") == 0) name = c;
                }
            }
        }
        if (ts_node_is_null(name)) return;
        char *mname = cs_node_text(ctx, name);
        if (!mname) return;
        char *bare = cs_strip_generic_args(ctx->arena, mname);

        /* Static member call: receiver is a type identifier. */
        char *recv_text = ts_node_is_null(recv) ? NULL : cs_node_text(ctx, recv);
        if (recv_text) {
            const char *type_qn = cs_resolve_type_name(ctx, recv_text);
            if (type_qn && cs_lookup_type_qn(ctx, type_qn)) {
                const CBMRegisteredFunc *f = cs_lookup_method(ctx, type_qn, bare);
                if (f) {
                    cs_emit_resolved(ctx, f->qualified_name, "cs_static_typed", 0.95f);
                    return;
                }
                /* Type known, method not in registry — synth a call. */
                cs_emit_resolved(ctx,
                                  cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, bare),
                                  "cs_static_typed_unindexed", 0.55f);
                return;
            }
        }

        /* Instance call. */
        const CBMType *recv_type = cbm_type_unknown();
        if (!ts_node_is_null(recv)) recv_type = cs_eval_expr_type(ctx, recv);
        recv_type = cs_unwrap_nullable(recv_type);
        const char *type_qn = NULL;
        if (recv_type && recv_type->kind == CBM_TYPE_NAMED) {
            type_qn = recv_type->data.named.qualified_name;
        } else if (recv_type && recv_type->kind == CBM_TYPE_TEMPLATE) {
            type_qn = recv_type->data.template_type.template_name;
        }
        if (!type_qn) return;
        const CBMRegisteredFunc *f = cs_lookup_method(ctx, type_qn, bare);
        if (f) {
            const char *strategy =
                (f->receiver_type && strcmp(f->receiver_type, type_qn) == 0)
                    ? "cs_method_typed"
                    : "cs_method_inherited";
            cs_emit_resolved(ctx, f->qualified_name, strategy, 0.95f);
            return;
        }
        /* Try extension method. */
        f = cs_lookup_extension(ctx, type_qn, bare);
        if (f) {
            cs_emit_resolved(ctx, f->qualified_name, "cs_extension_method", 0.90f);
            return;
        }
        /* Type known, method missing — emit unindexed marker so the textual
         * fallback in the pipeline is suppressed. */
        cs_emit_resolved(ctx,
                          cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, bare),
                          "cs_method_typed_unindexed", 0.55f);
        return;
    }

    /* Bare invocation: `Method()` */
    if (strcmp(fk, "identifier") == 0 || strcmp(fk, "generic_name") == 0) {
        char *fname = cs_node_text(ctx, fn);
        if (!fname) return;
        char *bare = cs_strip_generic_args(ctx->arena, fname);

        /* Try enclosing class member. */
        if (ctx->enclosing_class_qn) {
            const CBMRegisteredFunc *f = cs_lookup_method(ctx, ctx->enclosing_class_qn, bare);
            if (f) {
                cs_emit_resolved(ctx, f->qualified_name, "cs_self_method", 0.95f);
                return;
            }
        }
        /* Try base class chain explicitly (for `base.Foo()` style calls
         * happen via base_expression handled above; this is for inherited
         * methods invoked without `this.`). */
        if (ctx->enclosing_base_qn) {
            const CBMRegisteredFunc *f = cs_lookup_method(ctx, ctx->enclosing_base_qn, bare);
            if (f) {
                cs_emit_resolved(ctx, f->qualified_name, "cs_inherited_method", 0.92f);
                return;
            }
        }
        /* Try `using static` imports. */
        for (int i = 0; i < ctx->using_count; i++) {
            const CBMCSUsing *u = &ctx->usings[i];
            if (u->kind != CBM_CS_USING_STATIC) continue;
            const CBMRegisteredFunc *f = cs_lookup_method(ctx, u->target_qn, bare);
            if (f) {
                cs_emit_resolved(ctx, f->qualified_name, "cs_using_static", 0.90f);
                return;
            }
        }

        /* Free function in current namespace. */
        const char *ns = cs_namespace_qn(ctx);
        if (ns && *ns) {
            const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ns, bare);
            const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, qn);
            if (f) {
                cs_emit_resolved(ctx, f->qualified_name, "cs_namespace_func", 0.92f);
                return;
            }
        }
        /* Last resort: any free function with this short name in registry. */
        const CBMRegisteredFunc *best = NULL;
        int best_score = -1;
        for (int i = 0; ctx->registry && i < ctx->registry->func_count; i++) {
            const CBMRegisteredFunc *cand = &ctx->registry->funcs[i];
            if (cand->receiver_type) continue;
            if (!cand->short_name || strcmp(cand->short_name, bare) != 0) continue;
            int score = 0;
            if (cand->qualified_name && ctx->module_qn) {
                const char *m = ctx->module_qn;
                const char *q = cand->qualified_name;
                while (*m && *q && *m == *q) {
                    if (*m == '.') score++;
                    m++;
                    q++;
                }
            }
            if (score > best_score) {
                best_score = score;
                best = cand;
            }
        }
        if (best) {
            cs_emit_resolved(ctx, best->qualified_name, "cs_free_func_fallback", 0.65f);
            return;
        }
    }
}

static void cs_resolve_object_creation(CSLSPContext *ctx, TSNode call) {
    /* `new Foo(...)` adds an implicit Foo..ctor edge. We synth a constructor
     * call to give the pipeline a high-confidence target when Foo is known. */
    TSNode tnode = ts_node_child_by_field_name(call, "type", 4);
    if (ts_node_is_null(tnode)) return;
    const CBMType *t = cs_parse_type_node(ctx, tnode);
    const char *tqn = NULL;
    if (t && t->kind == CBM_TYPE_NAMED) tqn = t->data.named.qualified_name;
    else if (t && t->kind == CBM_TYPE_TEMPLATE) tqn = t->data.template_type.template_name;
    if (!tqn) return;
    const CBMRegisteredFunc *f = cs_lookup_method(ctx, tqn, ".ctor");
    if (f) {
        cs_emit_resolved(ctx, f->qualified_name, "cs_ctor", 0.95f);
        return;
    }
    /* Synthesize: Foo..ctor. */
    cs_emit_resolved(ctx, cbm_arena_sprintf(ctx->arena, "%s..ctor", tqn),
                      "cs_ctor_synthetic", 0.50f);
}

static void cs_resolve_calls_in_node(CSLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node)) return;
    const char *kind = ts_node_type(node);

    /* Scope-shaping nodes. */
    if (strcmp(kind, "block") == 0) {
        CBMScope *prev = ctx->current_scope;
        ctx->current_scope = cbm_scope_push(ctx->arena, prev);
        // Cursor walk (O(n)); ts_node_child(node,i) is O(i) → O(n²) on a wide block.
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                cs_resolve_calls_in_node(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
        ctx->current_scope = prev;
        return;
    }

    if (strcmp(kind, "local_declaration_statement") == 0) {
        cs_process_local_decl(ctx, node);
    } else if (strcmp(kind, "for_each_statement") == 0 || strcmp(kind, "foreach_statement") == 0) {
        cs_process_foreach(ctx, node);
    } else if (strcmp(kind, "using_statement") == 0) {
        cs_process_using_statement(ctx, node);
    } else if (strcmp(kind, "assignment_expression") == 0) {
        cs_process_assignment(ctx, node);
    } else if (strcmp(kind, "invocation_expression") == 0) {
        cs_resolve_invocation(ctx, node);
    } else if (strcmp(kind, "object_creation_expression") == 0) {
        cs_resolve_object_creation(ctx, node);
    }

    /* Recurse into children. We do NOT pre-bind anything that would only be
     * available after the child is processed — left-to-right walk is fine for
     * most C# constructs at a Light-Semantic-Pass level. */
    // Cursor walk (O(n)); ts_node_child(node,i) is O(i) → O(n²) on a wide node.
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode c = ts_tree_cursor_current_node(&cursor);
        const char *ck = ts_node_type(c);
        /* Don't recurse into nested type/method bodies — they're processed by
         * the pass-2 walker which rebuilds enclosing context. */
        if (strcmp(ck, "class_declaration") == 0 || strcmp(ck, "struct_declaration") == 0 ||
            strcmp(ck, "record_declaration") == 0 || strcmp(ck, "interface_declaration") == 0 ||
            strcmp(ck, "enum_declaration") == 0 || strcmp(ck, "method_declaration") == 0 ||
            strcmp(ck, "constructor_declaration") == 0 ||
            strcmp(ck, "destructor_declaration") == 0 ||
            strcmp(ck, "operator_declaration") == 0 ||
            strcmp(ck, "conversion_operator_declaration") == 0 ||
            strcmp(ck, "indexer_declaration") == 0 || strcmp(ck, "property_declaration") == 0 ||
            strcmp(ck, "event_declaration") == 0 ||
            strcmp(ck, "local_function_statement") == 0 ||
            strcmp(ck, "namespace_declaration") == 0 ||
            strcmp(ck, "file_scoped_namespace_declaration") == 0) {
            continue;
        }
        cs_resolve_calls_in_node(ctx, c);
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

/* ── method/constructor processing ──────────────────────────────── */

static void cs_collect_type_params(CSLSPContext *ctx, TSNode node, const char ***out_names,
                                    int *out_count) {
    *out_names = NULL;
    *out_count = 0;
    TSNode tplist = ts_node_child_by_field_name(node, "type_parameters", 15);
    if (ts_node_is_null(tplist)) tplist = cs_child_named_kind(node, "type_parameter_list");
    if (ts_node_is_null(tplist)) return;
    uint32_t nc = ts_node_child_count(tplist);
    int cap = 4;
    const char **arr = (const char **)cbm_arena_alloc(ctx->arena, (size_t)cap * sizeof(*arr));
    if (!arr) return;
    int n = 0;
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(tplist, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
        if (strcmp(ts_node_type(c), "type_parameter") != 0) continue;
        TSNode id = cs_first_named_child(c);
        if (ts_node_is_null(id)) continue;
        char *name = cs_node_text(ctx, id);
        if (!name) continue;
        if (n + 1 >= cap) {
            int new_cap = cap * 2;
            const char **ne =
                (const char **)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*ne));
            if (!ne) break;
            for (int j = 0; j < n; j++) ne[j] = arr[j];
            arr = ne;
            cap = new_cap;
        }
        arr[n++] = name;
    }
    arr[n] = NULL;
    *out_names = arr;
    *out_count = n;
}

static void cs_process_function_like(CSLSPContext *ctx, TSNode node) {
    const char *kind = ts_node_type(node);
    bool is_method = (strcmp(kind, "method_declaration") == 0 ||
                       strcmp(kind, "local_function_statement") == 0);
    bool is_ctor = strcmp(kind, "constructor_declaration") == 0;
    bool is_dtor = strcmp(kind, "destructor_declaration") == 0;
    bool is_property = strcmp(kind, "property_declaration") == 0;
    bool is_indexer = strcmp(kind, "indexer_declaration") == 0;
    bool is_op = strcmp(kind, "operator_declaration") == 0 ||
                 strcmp(kind, "conversion_operator_declaration") == 0;

    CBMScope *saved_scope = ctx->current_scope;
    const char *saved_func = ctx->enclosing_func_qn;
    const char **saved_tp = ctx->type_param_names;
    const CBMType **saved_tp_args = ctx->type_param_args;
    int saved_tp_count = ctx->type_param_count;

    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

    /* Determine func short name + QN. */
    const char *short_name = NULL;
    if (is_method) {
        TSNode nm = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(nm)) short_name = cs_node_text(ctx, nm);
    } else if (is_ctor) {
        short_name = ".ctor";
    } else if (is_dtor) {
        short_name = ".dtor";
    } else if (is_property) {
        TSNode nm = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(nm)) short_name = cs_node_text(ctx, nm);
    } else if (is_indexer) {
        short_name = "this[]";
    } else if (is_op) {
        TSNode op = ts_node_child_by_field_name(node, "operator", 8);
        if (!ts_node_is_null(op)) short_name = cs_node_text(ctx, op);
    }

    if (short_name) {
        if (ctx->enclosing_class_qn) {
            ctx->enclosing_func_qn =
                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->enclosing_class_qn, short_name);
        } else if (ctx->module_qn) {
            ctx->enclosing_func_qn =
                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, short_name);
        } else {
            ctx->enclosing_func_qn = cbm_arena_strdup(ctx->arena, short_name);
        }
    }

    /* Collect generic type parameters. */
    if (is_method) {
        const char **tp_names = NULL;
        int tp_count = 0;
        cs_collect_type_params(ctx, node, &tp_names, &tp_count);
        if (tp_count > 0) {
            const CBMType **args =
                (const CBMType **)cbm_arena_alloc(ctx->arena,
                                                  (size_t)(tp_count + 1) * sizeof(*args));
            if (args) {
                for (int i = 0; i < tp_count; i++) {
                    args[i] = cbm_type_type_param(ctx->arena, tp_names[i]);
                }
                args[tp_count] = NULL;
                ctx->type_param_names = tp_names;
                ctx->type_param_args = args;
                ctx->type_param_count = tp_count;
            }
        }
    }

    /* Bind parameters. Detect extension method (first param has `this`). */
    bool is_extension = false;
    if (is_method) {
        TSNode params = ts_node_child_by_field_name(node, "parameters", 10);
        if (ts_node_is_null(params)) params = cs_child_named_kind(node, "parameter_list");
        if (!ts_node_is_null(params)) {
            /* Detect `this` modifier on first parameter. */
            uint32_t nc = ts_node_child_count(params);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_child(params, i);
                if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
                if (strcmp(ts_node_type(c), "parameter") != 0) continue;
                /* Look for "this" keyword child. */
                uint32_t pc = ts_node_child_count(c);
                for (uint32_t j = 0; j < pc; j++) {
                    TSNode cc = ts_node_child(c, j);
                    if (ts_node_is_null(cc)) continue;
                    if (strcmp(ts_node_type(cc), "this") == 0) {
                        is_extension = true;
                        break;
                    }
                }
                break;
            }
            cs_bind_parameters(ctx, params, is_extension);
        }
    } else if (is_indexer) {
        TSNode params = cs_child_named_kind(node, "bracketed_parameter_list");
        if (!ts_node_is_null(params)) cs_bind_parameters(ctx, params, false);
    } else if (is_ctor || is_op) {
        TSNode params = cs_child_named_kind(node, "parameter_list");
        if (!ts_node_is_null(params)) cs_bind_parameters(ctx, params, false);
    }

    /* Walk body. */
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (ts_node_is_null(body)) {
        /* Expression-bodied: => expr; */
        TSNode arrow = cs_child_named_kind(node, "arrow_expression_clause");
        if (!ts_node_is_null(arrow)) {
            TSNode expr = cs_first_named_child(arrow);
            if (!ts_node_is_null(expr)) cs_resolve_calls_in_node(ctx, expr);
        }
    } else {
        cs_resolve_calls_in_node(ctx, body);
    }

    /* Property accessors */
    if (is_property || is_indexer) {
        TSNode accessors = cs_child_named_kind(node, "accessor_list");
        if (!ts_node_is_null(accessors)) {
            uint32_t nc = ts_node_child_count(accessors);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode a = ts_node_child(accessors, i);
                if (ts_node_is_null(a) || !ts_node_is_named(a)) continue;
                if (strcmp(ts_node_type(a), "accessor_declaration") != 0) continue;
                TSNode abody = ts_node_child_by_field_name(a, "body", 4);
                if (ts_node_is_null(abody)) abody = cs_child_named_kind(a, "block");
                if (!ts_node_is_null(abody)) cs_resolve_calls_in_node(ctx, abody);
                else {
                    TSNode arrow = cs_child_named_kind(a, "arrow_expression_clause");
                    if (!ts_node_is_null(arrow)) {
                        TSNode expr = cs_first_named_child(arrow);
                        if (!ts_node_is_null(expr)) cs_resolve_calls_in_node(ctx, expr);
                    }
                }
            }
        }
    }

    /* Restore. */
    ctx->current_scope = saved_scope;
    ctx->enclosing_func_qn = saved_func;
    ctx->type_param_names = saved_tp;
    ctx->type_param_args = saved_tp_args;
    ctx->type_param_count = saved_tp_count;
}

/* ── type declaration processing ────────────────────────────────── */

static void cs_process_type_decl(CSLSPContext *ctx, TSNode node) {
    const char *kind = ts_node_type(node);
    bool is_class = strcmp(kind, "class_declaration") == 0;
    bool is_struct = strcmp(kind, "struct_declaration") == 0;
    bool is_record = strcmp(kind, "record_declaration") == 0 ||
                       strcmp(kind, "record_struct_declaration") == 0;
    bool is_iface = strcmp(kind, "interface_declaration") == 0;
    bool is_enum = strcmp(kind, "enum_declaration") == 0;
    (void)is_struct; (void)is_record; (void)is_iface;

    TSNode nm = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(nm)) {
        ctx->debug && fprintf(stderr, "[cs_lsp] type decl missing name\n");
        return;
    }
    char *cname = cs_node_text(ctx, nm);
    if (!cname) return;

    const char *saved_class = ctx->enclosing_class_qn;
    const char *saved_base = ctx->enclosing_base_qn;
    const char **saved_ifs = ctx->enclosing_iface_qns;
    const char **saved_tp = ctx->type_param_names;
    const CBMType **saved_tp_args = ctx->type_param_args;
    int saved_tp_count = ctx->type_param_count;

    /* Compute QN: prefer module-qn-prefixed (matches unified extractor),
     * even though the C# `namespace` is also tracked for resolution. */
    if (ctx->module_qn) {
        ctx->enclosing_class_qn =
            cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cname);
    } else {
        ctx->enclosing_class_qn = cbm_arena_strdup(ctx->arena, cname);
    }

    /* Type parameters. */
    const char **tp_names = NULL;
    int tp_count = 0;
    cs_collect_type_params(ctx, node, &tp_names, &tp_count);
    if (tp_count > 0) {
        const CBMType **args = (const CBMType **)cbm_arena_alloc(
            ctx->arena, (size_t)(tp_count + 1) * sizeof(*args));
        if (args) {
            for (int i = 0; i < tp_count; i++) {
                args[i] = cbm_type_type_param(ctx->arena, tp_names[i]);
            }
            args[tp_count] = NULL;
            ctx->type_param_names = tp_names;
            ctx->type_param_args = args;
            ctx->type_param_count = tp_count;
        }
    }

    /* Base list -> enclosing_base_qn + enclosing_iface_qns. */
    {
        TSNode bl = cs_child_named_kind(node, "base_list");
        if (!ts_node_is_null(bl)) {
            uint32_t nc = ts_node_child_count(bl);
            int icap = 4, icount = 0;
            const char **ifs =
                (const char **)cbm_arena_alloc(ctx->arena, (size_t)icap * sizeof(*ifs));
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_child(bl, i);
                if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
                char *t = cs_node_text(ctx, c);
                if (!t) continue;
                /* Strip generic args for QN resolution. */
                char *bare = cs_strip_generic_args(ctx->arena, t);
                const char *qn = cs_resolve_type_name(ctx, bare ? bare : t);
                if (!qn) continue;
                /* Heuristic: first base whose registry says is_interface=true
                 * goes to interfaces; first non-interface goes to base. */
                const CBMRegisteredType *rt = cs_lookup_type_qn(ctx, qn);
                if (rt && rt->is_interface) {
                    if (icount + 1 >= icap) {
                        int nc2 = icap * 2;
                        const char **ne = (const char **)cbm_arena_alloc(
                            ctx->arena, (size_t)nc2 * sizeof(*ne));
                        if (!ne) continue;
                        for (int j = 0; j < icount; j++) ne[j] = ifs[j];
                        ifs = ne;
                        icap = nc2;
                    }
                    ifs[icount++] = qn;
                } else {
                    if (!ctx->enclosing_base_qn) ctx->enclosing_base_qn = qn;
                    else {
                        if (icount + 1 >= icap) {
                            int nc2 = icap * 2;
                            const char **ne = (const char **)cbm_arena_alloc(
                                ctx->arena, (size_t)nc2 * sizeof(*ne));
                            if (!ne) continue;
                            for (int j = 0; j < icount; j++) ne[j] = ifs[j];
                            ifs = ne;
                            icap = nc2;
                        }
                        ifs[icount++] = qn;
                    }
                }
            }
            ifs[icount] = NULL;
            ctx->enclosing_iface_qns = ifs;
        }
    }

    /* Walk body. Enums don't have methods worth resolving; skip recurse. */
    if (!is_enum) {
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        if (ts_node_is_null(body)) body = cs_child_named_kind(node, "declaration_list");
        if (!ts_node_is_null(body)) {
            uint32_t nc = ts_node_child_count(body);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_child(body, i);
                if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
                const char *k = ts_node_type(c);
                if (strcmp(k, "method_declaration") == 0 ||
                    strcmp(k, "constructor_declaration") == 0 ||
                    strcmp(k, "destructor_declaration") == 0 ||
                    strcmp(k, "operator_declaration") == 0 ||
                    strcmp(k, "conversion_operator_declaration") == 0 ||
                    strcmp(k, "indexer_declaration") == 0 ||
                    strcmp(k, "property_declaration") == 0 ||
                    strcmp(k, "event_declaration") == 0) {
                    cs_process_function_like(ctx, c);
                } else if (strcmp(k, "class_declaration") == 0 ||
                           strcmp(k, "struct_declaration") == 0 ||
                           strcmp(k, "record_declaration") == 0 ||
                           strcmp(k, "interface_declaration") == 0 ||
                           strcmp(k, "enum_declaration") == 0) {
                    /* Nested type. Recurse, but with the OUTER class's
                     * enclosing_class_qn replaced. */
                    cs_process_type_decl(ctx, c);
                }
            }
        }
        /* Records with primary constructor parameter list — bind in a synthetic
         * `.ctor` scope so any default-value expressions get resolved. */
        TSNode pctor = cs_child_named_kind(node, "parameter_list");
        if (!ts_node_is_null(pctor)) {
            CBMScope *prev = ctx->current_scope;
            ctx->current_scope = cbm_scope_push(ctx->arena, prev);
            const char *prev_func = ctx->enclosing_func_qn;
            ctx->enclosing_func_qn =
                cbm_arena_sprintf(ctx->arena, "%s..ctor", ctx->enclosing_class_qn);
            cs_bind_parameters(ctx, pctor, false);
            ctx->enclosing_func_qn = prev_func;
            ctx->current_scope = prev;
        }
    }
    (void)is_class;

    ctx->enclosing_class_qn = saved_class;
    ctx->enclosing_base_qn = saved_base;
    ctx->enclosing_iface_qns = saved_ifs;
    ctx->type_param_names = saved_tp;
    ctx->type_param_args = saved_tp_args;
    ctx->type_param_count = saved_tp_count;
}

/* ── using collection ───────────────────────────────────────────── */

static void cs_collect_imports(CSLSPContext *ctx, TSNode root) {
    /* Walk the entire tree once at top — using directives can appear at file
     * scope or inside namespace blocks. */
    TSNode stack[256];
    int top = 0;
    stack[top++] = root;
    while (top > 0) {
        TSNode n = stack[--top];
        if (ts_node_is_null(n)) continue;
        const char *k = ts_node_type(n);
        if (strcmp(k, "using_directive") == 0) {
            /* Inspect modifiers and target. tree-sitter-c-sharp uses the
             * `alias` field on using_directive for the alias name (in
             * `using A = X;` the field "alias" holds A). When the field
             * isn't populated (older grammar), we detect aliasing by the
             * presence of an `=` token between identifier children. */
            bool is_global = false;
            bool is_static = false;
            bool is_alias = false;
            const char *alias_name = NULL;
            const char *target = NULL;
            TSNode alias_node = ts_node_child_by_field_name(n, "alias", 5);
            if (!ts_node_is_null(alias_node)) {
                is_alias = true;
                char *t = cs_node_text(ctx, alias_node);
                if (t) alias_name = t;
            }
            /* Detect `=` token between named children to flag aliasing
             * even when the `alias` field isn't populated. */
            uint32_t nc = ts_node_child_count(n);
            bool seen_equals = false;
            TSNode pre_eq;
            memset(&pre_eq, 0, sizeof(pre_eq));
            TSNode post_eq;
            memset(&post_eq, 0, sizeof(post_eq));
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_child(n, i);
                if (ts_node_is_null(c)) continue;
                const char *ck = ts_node_type(c);
                /* Tokens (anonymous and named alike) for keywords + `=`. */
                if (strcmp(ck, "global") == 0) { is_global = true; continue; }
                if (strcmp(ck, "static") == 0) { is_static = true; continue; }
                if (strcmp(ck, "=") == 0) { seen_equals = true; continue; }
                if (!ts_node_is_named(c)) continue;
                if (strcmp(ck, "name_equals") == 0) {
                    /* Older grammar variant. */
                    is_alias = true;
                    TSNode id = cs_first_named_child(c);
                    if (!ts_node_is_null(id)) alias_name = cs_node_text(ctx, id);
                    continue;
                }
                /* identifier / qualified_name / generic_name. */
                if (!seen_equals && ts_node_is_null(pre_eq)) {
                    pre_eq = c;
                } else if (seen_equals && ts_node_is_null(post_eq)) {
                    post_eq = c;
                }
            }
            if (seen_equals && !ts_node_is_null(pre_eq) && !ts_node_is_null(post_eq)) {
                /* `using <pre_eq> = <post_eq>;` — aliasing. */
                is_alias = true;
                if (!alias_name) {
                    char *t = cs_node_text(ctx, pre_eq);
                    if (t) alias_name = t;
                }
                char *t = cs_node_text(ctx, post_eq);
                if (t) target = cs_normalize_name(ctx->arena, t);
            } else if (!ts_node_is_null(pre_eq) && !target) {
                /* Non-aliased: `using X;` — pre_eq is the target. */
                char *t = cs_node_text(ctx, pre_eq);
                if (t) target = cs_normalize_name(ctx->arena, t);
            }
            if (target) {
                if (is_alias) {
                    cs_lsp_add_using(ctx, CBM_CS_USING_ALIAS, alias_name ? alias_name : "",
                                      target, is_global);
                } else if (is_static) {
                    cs_lsp_add_using(ctx, CBM_CS_USING_STATIC, "", target, is_global);
                } else {
                    cs_lsp_add_using(ctx, CBM_CS_USING_NAMESPACE, "", target, is_global);
                }
            }
        }
        /* Push children for traversal. We don't recurse into method bodies
         * (using directives can't appear there), but namespace bodies may
         * contain more usings. */
        uint32_t cnc = ts_node_child_count(n);
        for (uint32_t i = 0; i < cnc && top < 256; i++) {
            TSNode c = ts_node_child(n, i);
            if (ts_node_is_null(c)) continue;
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "method_declaration") == 0 ||
                strcmp(ck, "constructor_declaration") == 0 ||
                strcmp(ck, "block") == 0) {
                continue;
            }
            stack[top++] = c;
        }
    }
}

/* ── namespace collection ───────────────────────────────────────── */

static void cs_collect_namespace(CSLSPContext *ctx, TSNode ns_node, bool file_scoped) {
    TSNode nm = ts_node_child_by_field_name(ns_node, "name", 4);
    if (ts_node_is_null(nm)) return;
    char *raw = cs_node_text(ctx, nm);
    if (!raw) return;
    const char *normalized = cs_normalize_name(ctx->arena, raw);
    cs_namespace_push(ctx, normalized);
    /* Walk body — file-scoped namespaces have body items as siblings of nm.
     * Block-style have a body node. */
    if (file_scoped) {
        /* The file-scoped namespace's siblings (in the compilation_unit, after
         * the namespace decl) are the contents — handled by the caller's
         * top-level walk. We just push the namespace and return. */
        return;
    }
    TSNode body = ts_node_child_by_field_name(ns_node, "body", 4);
    if (ts_node_is_null(body)) body = cs_child_named_kind(ns_node, "declaration_list");
    if (!ts_node_is_null(body)) {
        uint32_t nc = ts_node_child_count(body);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(body, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "namespace_declaration") == 0) {
                cs_collect_namespace(ctx, c, false);
            } else if (strcmp(ck, "file_scoped_namespace_declaration") == 0) {
                cs_collect_namespace(ctx, c, true);
            } else if (strcmp(ck, "class_declaration") == 0 ||
                       strcmp(ck, "struct_declaration") == 0 ||
                       strcmp(ck, "record_declaration") == 0 ||
                       strcmp(ck, "interface_declaration") == 0 ||
                       strcmp(ck, "enum_declaration") == 0) {
                cs_process_type_decl(ctx, c);
            }
        }
    }
    cs_namespace_pop(ctx);
}

/* ── top-level walk ─────────────────────────────────────────────── */

void cs_lsp_process_file(CSLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root)) return;

    /* Pass 1: collect using directives. */
    cs_collect_imports(ctx, root);

    /* Pass 2: walk top-level. file-scoped namespaces apply to ALL siblings,
     * so we keep the namespace pushed for the rest of the walk. */
    // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
    uint32_t kn = 0;
    TSNode *kids = cbm_lsp_collect_children(ctx->arena, root, &kn);
    bool file_scoped_active = false;
    for (uint32_t i = 0; i < kn; i++) {
        TSNode c = kids[i];
        if (!ts_node_is_named(c)) continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "using_directive") == 0) continue;
        if (strcmp(k, "file_scoped_namespace_declaration") == 0) {
            cs_collect_namespace(ctx, c, true);
            file_scoped_active = true;
        } else if (strcmp(k, "namespace_declaration") == 0) {
            cs_collect_namespace(ctx, c, false);
        } else if (strcmp(k, "class_declaration") == 0 ||
                   strcmp(k, "struct_declaration") == 0 ||
                   strcmp(k, "record_declaration") == 0 ||
                   strcmp(k, "interface_declaration") == 0 ||
                   strcmp(k, "enum_declaration") == 0) {
            cs_process_type_decl(ctx, c);
        } else if (strcmp(k, "global_statement") == 0 || strcmp(k, "expression_statement") == 0 ||
                   strcmp(k, "if_statement") == 0 || strcmp(k, "for_statement") == 0 ||
                   strcmp(k, "foreach_statement") == 0 ||
                   strcmp(k, "for_each_statement") == 0 ||
                   strcmp(k, "while_statement") == 0 ||
                   strcmp(k, "local_declaration_statement") == 0 ||
                   strcmp(k, "return_statement") == 0) {
            /* Top-level statements (Program.cs without explicit Main).
             * We treat them as a synthetic enclosing function `<top>` so
             * the resolved-call array carries the caller correctly. */
            const char *saved = ctx->enclosing_func_qn;
            if (!saved && ctx->module_qn) {
                ctx->enclosing_func_qn = ctx->module_qn;
            }
            cs_resolve_calls_in_node(ctx, c);
            ctx->enclosing_func_qn = saved;
        }
    }
    if (file_scoped_active && ctx->namespace_count > 0) cs_namespace_pop(ctx);
}

/* ── registry building from defs ─────────────────────────────────── */

/* Parse a parenthesized signature like `(int x, string s = "")` into
 * NULL-terminated arrays of param names + types. Best-effort: drops
 * default-value expressions, ignores ref/out/in modifiers. */
static void cs_parse_signature(CBMArena *arena, const char *signature,
                                CSLSPContext *ctx, const char ***out_names,
                                const CBMType ***out_types) {
    *out_names = NULL;
    *out_types = NULL;
    if (!signature) return;
    const char *p = signature;
    while (*p == ' ' || *p == '(') p++;
    /* Walk param-by-param. We split on top-level ',' (ignoring those inside
     * generic <> brackets). */
    int cap = 8;
    int count = 0;
    const char **names = (const char **)cbm_arena_alloc(arena, (size_t)cap * sizeof(*names));
    const CBMType **types = (const CBMType **)cbm_arena_alloc(arena, (size_t)cap * sizeof(*types));
    if (!names || !types) return;

    while (*p && *p != ')') {
        /* Skip leading whitespace. */
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        if (!*p || *p == ')') break;
        /* Skip param modifiers: ref, out, in, params, this. */
        const char *modifiers[] = {"ref ", "out ", "in ", "params ", "this "};
        bool ate;
        do {
            ate = false;
            for (int m = 0; m < 5; m++) {
                size_t ml = strlen(modifiers[m]);
                if (strncmp(p, modifiers[m], ml) == 0) {
                    p += ml;
                    ate = true;
                }
            }
        } while (ate);
        /* Read type tokens until we hit a name. The type may include '<...>'
         * or '[]'. The name is the last token before ',' or '=' or ')'. */
        const char *type_start = p;
        int depth = 0;
        const char *last_space = NULL;
        while (*p && (depth > 0 || (*p != ',' && *p != ')' && *p != '='))) {
            if (*p == '<') depth++;
            else if (*p == '>') depth--;
            else if (depth == 0 && (*p == ' ' || *p == '\t')) last_space = p;
            p++;
        }
        const char *name_end = p;
        while (name_end > type_start && (name_end[-1] == ' ' || name_end[-1] == '\t')) name_end--;
        if (!last_space) {
            /* No name — treat the whole token as the type with synthetic name. */
            char *type_text = cbm_arena_strndup(arena, type_start, (size_t)(name_end - type_start));
            const CBMType *t = cbm_type_unknown();
            if (ctx) t = cs_resolve_type_name(ctx, type_text)
                          ? cbm_type_named(ctx->arena, cs_resolve_type_name(ctx, type_text))
                          : cbm_type_unknown();
            (void)t;
            if (count + 1 >= cap) break;
            names[count] = cbm_arena_sprintf(arena, "_arg%d", count);
            types[count] = t;
            count++;
        } else {
            char *type_text = cbm_arena_strndup(arena, type_start, (size_t)(last_space - type_start));
            char *pname = cbm_arena_strndup(arena, last_space + 1, (size_t)(name_end - last_space - 1));
            const CBMType *t = cbm_type_unknown();
            if (ctx) {
                const char *resolved = cs_resolve_type_name(ctx, type_text);
                if (resolved) t = cbm_type_named(ctx->arena, resolved);
            }
            if (count + 1 >= cap) break;
            names[count] = pname;
            types[count] = t;
            count++;
        }
        /* Skip default value if any. */
        if (*p == '=') {
            int d = 0;
            while (*p && (d > 0 || (*p != ',' && *p != ')'))) {
                if (*p == '(' || *p == '<') d++;
                else if (*p == ')' || *p == '>') {
                    if (d == 0) break;
                    d--;
                }
                p++;
            }
        }
        if (*p == ',') p++;
    }
    if (count + 1 < cap) {
        names[count] = NULL;
        types[count] = NULL;
        *out_names = names;
        *out_types = types;
    }
}

static void cs_register_type_decls(CSLSPContext *ctx, CBMTypeRegistry *reg, TSNode root) {
    /* We rely on CBMFileResult.defs entries already being filled by the
     * unified extractor. This function is reserved for future expansions
     * (e.g. parsing field declarations directly from the AST). */
    (void)ctx; (void)reg; (void)root;
}

/* ── field/property collection from AST ─────────────────────────── */

typedef struct {
    const char *class_qn;
    const char **field_names;
    const CBMType **field_types;
    int count;
    int cap;
} cs_fields_t;

typedef struct {
    cs_fields_t *items;
    int count;
    int cap;
} cs_fields_table_t;

static cs_fields_t *cs_fields_get(CBMArena *arena, cs_fields_table_t *tab,
                                    const char *class_qn) {
    for (int i = 0; i < tab->count; i++) {
        if (strcmp(tab->items[i].class_qn, class_qn) == 0) return &tab->items[i];
    }
    if (tab->count + 1 >= tab->cap) {
        int new_cap = tab->cap ? tab->cap * 2 : 8;
        cs_fields_t *ne =
            (cs_fields_t *)cbm_arena_alloc(arena, (size_t)new_cap * sizeof(*ne));
        if (!ne) return NULL;
        for (int i = 0; i < tab->count; i++) ne[i] = tab->items[i];
        tab->items = ne;
        tab->cap = new_cap;
    }
    cs_fields_t *slot = &tab->items[tab->count++];
    memset(slot, 0, sizeof(*slot));
    slot->class_qn = class_qn;
    slot->cap = 8;
    slot->field_names =
        (const char **)cbm_arena_alloc(arena, (size_t)slot->cap * sizeof(*slot->field_names));
    slot->field_types =
        (const CBMType **)cbm_arena_alloc(arena, (size_t)slot->cap * sizeof(*slot->field_types));
    return slot;
}

static void cs_fields_add_debug(CBMArena *arena, cs_fields_t *f, const char *name,
                                 const CBMType *type, bool debug);

static void cs_fields_add(CBMArena *arena, cs_fields_t *f, const char *name,
                           const CBMType *type) {
    cs_fields_add_debug(arena, f, name, type, false);
}

static void cs_fields_add_debug(CBMArena *arena, cs_fields_t *f, const char *name,
                                 const CBMType *type, bool debug) {
    (void)debug;
    if (!f || !name) return;
    /* Dedupe. */
    for (int i = 0; i < f->count; i++) {
        if (strcmp(f->field_names[i], name) == 0) return;
    }
    if (f->count + 2 >= f->cap) {
        int new_cap = f->cap * 2;
        const char **nn = (const char **)cbm_arena_alloc(arena, (size_t)new_cap * sizeof(*nn));
        const CBMType **nt = (const CBMType **)cbm_arena_alloc(arena, (size_t)new_cap * sizeof(*nt));
        if (!nn || !nt) return;
        for (int i = 0; i < f->count; i++) {
            nn[i] = f->field_names[i];
            nt[i] = f->field_types[i];
        }
        f->field_names = nn;
        f->field_types = nt;
        f->cap = new_cap;
    }
    f->field_names[f->count] = cbm_arena_strdup(arena, name);
    f->field_types[f->count] = type ? type : cbm_type_unknown();
    f->count++;
    f->field_names[f->count] = NULL;
    f->field_types[f->count] = NULL;
}

static void cs_collect_class_fields(CSLSPContext *ctx, CBMTypeRegistry *reg, TSNode root,
                                     cs_fields_table_t *tab) {
    /* Walk the AST and collect field/property/event declarations into tab.
     * We need the namespace + using context to resolve types, so this runs
     * after cs_collect_imports. */
    TSNode stack[512];
    int top = 0;
    stack[top++] = root;
    /* Maintain enclosing class for each decl we visit. We simply re-derive it
     * via parent_chain inspection (limited to direct class parent). */
    while (top > 0) {
        TSNode n = stack[--top];
        if (ts_node_is_null(n)) continue;
        const char *k = ts_node_type(n);

        if (strcmp(k, "field_declaration") == 0 || strcmp(k, "property_declaration") == 0 ||
            strcmp(k, "event_field_declaration") == 0) {
            /* Find enclosing class/struct/record. */
            TSNode p = ts_node_parent(n);
            const char *cls_short = NULL;
            while (!ts_node_is_null(p)) {
                const char *pk = ts_node_type(p);
                if (strcmp(pk, "class_declaration") == 0 ||
                    strcmp(pk, "struct_declaration") == 0 ||
                    strcmp(pk, "record_declaration") == 0 ||
                    strcmp(pk, "record_struct_declaration") == 0 ||
                    strcmp(pk, "interface_declaration") == 0) {
                    TSNode nm = ts_node_child_by_field_name(p, "name", 4);
                    if (!ts_node_is_null(nm)) {
                        cls_short = cs_node_text(ctx, nm);
                    }
                    break;
                }
                p = ts_node_parent(p);
            }
            if (cls_short) {
                const char *cls_qn =
                    ctx->module_qn
                        ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cls_short)
                        : cbm_arena_strdup(ctx->arena, cls_short);
                cs_fields_t *f = cs_fields_get(ctx->arena, tab, cls_qn);
                if (strcmp(k, "field_declaration") == 0 ||
                    strcmp(k, "event_field_declaration") == 0) {
                    TSNode vd = cs_child_named_kind(n, "variable_declaration");
                    if (!ts_node_is_null(vd)) {
                        TSNode tnode = ts_node_child_by_field_name(vd, "type", 4);
                        if (ts_node_is_null(tnode)) tnode = cs_first_named_child(vd);
                        const CBMType *t = ts_node_is_null(tnode)
                                                ? cbm_type_unknown()
                                                : cs_parse_type_node(ctx, tnode);
                        uint32_t vc = ts_node_child_count(vd);
                        for (uint32_t i = 0; i < vc; i++) {
                            TSNode c = ts_node_child(vd, i);
                            if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
                            if (strcmp(ts_node_type(c), "variable_declarator") != 0) continue;
                            TSNode id = cs_child_named_kind(c, "identifier");
                            if (ts_node_is_null(id)) id = ts_node_child_by_field_name(c, "name", 4);
                            if (ts_node_is_null(id)) continue;
                            char *fn = cs_node_text(ctx, id);
                            if (fn) cs_fields_add_debug(ctx->arena, f, fn, t, ctx->debug);
                        }
                    }
                } else {
                    /* property */
                    TSNode tnode = ts_node_child_by_field_name(n, "type", 4);
                    TSNode nm = ts_node_child_by_field_name(n, "name", 4);
                    if (!ts_node_is_null(tnode) && !ts_node_is_null(nm)) {
                        const CBMType *t = cs_parse_type_node(ctx, tnode);
                        char *fn = cs_node_text(ctx, nm);
                        if (fn) cs_fields_add_debug(ctx->arena, f, fn, t, ctx->debug);
                    }
                }
            }
        }

        /* Primary constructor parameters of records / classes — register as fields */
        if (strcmp(k, "record_declaration") == 0 ||
            strcmp(k, "record_struct_declaration") == 0 ||
            strcmp(k, "class_declaration") == 0 ||
            strcmp(k, "struct_declaration") == 0) {
            TSNode params = cs_child_named_kind(n, "parameter_list");
            TSNode nm = ts_node_child_by_field_name(n, "name", 4);
            if (!ts_node_is_null(params) && !ts_node_is_null(nm)) {
                char *cls_short = cs_node_text(ctx, nm);
                if (cls_short) {
                    const char *cls_qn =
                        ctx->module_qn
                            ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cls_short)
                            : cbm_arena_strdup(ctx->arena, cls_short);
                    cs_fields_t *f = cs_fields_get(ctx->arena, tab, cls_qn);
                    uint32_t pc = ts_node_child_count(params);
                    for (uint32_t i = 0; i < pc; i++) {
                        TSNode c = ts_node_child(params, i);
                        if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
                        if (strcmp(ts_node_type(c), "parameter") != 0) continue;
                        TSNode tn = ts_node_child_by_field_name(c, "type", 4);
                        TSNode pn = ts_node_child_by_field_name(c, "name", 4);
                        if (ts_node_is_null(tn) || ts_node_is_null(pn)) continue;
                        const CBMType *t = cs_parse_type_node(ctx, tn);
                        char *pname = cs_node_text(ctx, pn);
                        if (pname) cs_fields_add(ctx->arena, f, pname, t);
                    }
                }
            }
        }

        uint32_t cnc = ts_node_child_count(n);
        for (uint32_t i = 0; i < cnc && top + 1 < 512; i++) {
            TSNode c = ts_node_child(n, i);
            if (ts_node_is_null(c)) continue;
            const char *ck = ts_node_type(c);
            /* Skip method/ctor bodies — fields can't be there. */
            if (strcmp(ck, "method_declaration") == 0 ||
                strcmp(ck, "constructor_declaration") == 0 ||
                strcmp(ck, "destructor_declaration") == 0 ||
                strcmp(ck, "operator_declaration") == 0 ||
                strcmp(ck, "indexer_declaration") == 0 ||
                strcmp(ck, "block") == 0) {
                continue;
            }
            stack[top++] = c;
        }
    }
    (void)reg;
}

/* ── extract method return types directly from AST ──────────────
 *
 * The unified extractor leaves CBMDefinition.return_type NULL for C#
 * methods (the C# code path takes a different branch). Without return
 * types, method-chain resolution like `a.GetB().Tag()` can't propagate
 * the result type from GetB into the next dispatch. To restore the
 * chain we do our own AST pass here, mapping method QNs to their
 * declared return type, then patching the registry's signatures.
 */

typedef struct {
    const char *qn;
    const CBMType *rt;
} cs_method_rt_entry_t;

typedef struct {
    cs_method_rt_entry_t *items;
    int count;
    int cap;
} cs_method_rt_table_t;

static void cs_method_rt_add(CBMArena *arena, cs_method_rt_table_t *tab,
                              const char *qn, const CBMType *rt) {
    if (!qn || !rt) return;
    for (int i = 0; i < tab->count; i++) {
        if (strcmp(tab->items[i].qn, qn) == 0) return;
    }
    if (tab->count + 1 >= tab->cap) {
        int new_cap = tab->cap ? tab->cap * 2 : 16;
        cs_method_rt_entry_t *ne = (cs_method_rt_entry_t *)cbm_arena_alloc(
            arena, (size_t)new_cap * sizeof(*ne));
        if (!ne) return;
        for (int i = 0; i < tab->count; i++) ne[i] = tab->items[i];
        tab->items = ne;
        tab->cap = new_cap;
    }
    tab->items[tab->count].qn = qn;
    tab->items[tab->count].rt = rt;
    tab->count++;
}

/* Walk the tree, finding method_declaration / property_declaration nodes
 * and recording (parent_class_qn + "." + method_name → return type). */
static void cs_collect_method_return_types(CSLSPContext *ctx, TSNode root,
                                            cs_method_rt_table_t *tab) {
    TSNode stack[512];
    int top = 0;
    stack[top++] = root;
    while (top > 0) {
        TSNode n = stack[--top];
        if (ts_node_is_null(n)) continue;
        const char *k = ts_node_type(n);

        bool is_method = (strcmp(k, "method_declaration") == 0);
        bool is_property = (strcmp(k, "property_declaration") == 0);
        bool is_indexer = (strcmp(k, "indexer_declaration") == 0);
        if (is_method || is_property || is_indexer) {
            /* Find enclosing class. */
            TSNode p = ts_node_parent(n);
            const char *cls_short = NULL;
            while (!ts_node_is_null(p)) {
                const char *pk = ts_node_type(p);
                if (strcmp(pk, "class_declaration") == 0 ||
                    strcmp(pk, "struct_declaration") == 0 ||
                    strcmp(pk, "record_declaration") == 0 ||
                    strcmp(pk, "record_struct_declaration") == 0 ||
                    strcmp(pk, "interface_declaration") == 0) {
                    TSNode pn = ts_node_child_by_field_name(p, "name", 4);
                    if (!ts_node_is_null(pn)) cls_short = cs_node_text(ctx, pn);
                    break;
                }
                p = ts_node_parent(p);
            }
            if (!cls_short) {
                uint32_t cnc = ts_node_child_count(n);
                for (uint32_t i = 0; i < cnc && top + 1 < 512; i++) {
                    TSNode c = ts_node_child(n, i);
                    if (!ts_node_is_null(c)) stack[top++] = c;
                }
                continue;
            }
            const char *cls_qn =
                ctx->module_qn
                    ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cls_short)
                    : cbm_arena_strdup(ctx->arena, cls_short);
            TSNode tnode = ts_node_child_by_field_name(n, "type", 4);
            TSNode nm = ts_node_child_by_field_name(n, "name", 4);
            /* tree-sitter-c-sharp doesn't always set the type field on
             * method_declaration. Walk named children to find the return
             * type: it's the first non-modifier non-attribute named child
             * whose kind isn't an identifier matching the method name. */
            if (ts_node_is_null(tnode)) {
                uint32_t cn = ts_node_child_count(n);
                for (uint32_t i = 0; i < cn; i++) {
                    TSNode c = ts_node_child(n, i);
                    if (ts_node_is_null(c) || !ts_node_is_named(c)) continue;
                    const char *ck = ts_node_type(c);
                    if (strcmp(ck, "modifier") == 0 || strcmp(ck, "attribute_list") == 0 ||
                        strcmp(ck, "type_parameter_list") == 0) continue;
                    /* The name comes after the type; skip it. */
                    if (!ts_node_is_null(nm) && ts_node_eq(c, nm)) continue;
                    if (strcmp(ck, "parameter_list") == 0 ||
                        strcmp(ck, "block") == 0 ||
                        strcmp(ck, "arrow_expression_clause") == 0 ||
                        strcmp(ck, "type_parameter_constraints_clause") == 0) break;
                    /* Heuristic: the first remaining named child is the
                     * return type. */
                    tnode = c;
                    break;
                }
            }
            if (is_indexer) {
                const char *short_name = "this[]";
                if (!ts_node_is_null(tnode)) {
                    const CBMType *rt = cs_parse_type_node(ctx, tnode);
                    cs_method_rt_add(ctx->arena, tab,
                                      cbm_arena_sprintf(ctx->arena, "%s.%s", cls_qn,
                                                         short_name),
                                      rt);
                }
            } else if (!ts_node_is_null(tnode) && !ts_node_is_null(nm)) {
                char *short_name = cs_node_text(ctx, nm);
                if (short_name) {
                    const CBMType *rt = cs_parse_type_node(ctx, tnode);
                    cs_method_rt_add(ctx->arena, tab,
                                      cbm_arena_sprintf(ctx->arena, "%s.%s", cls_qn,
                                                         short_name),
                                      rt);
                }
            }
        }

        /* Push children unless we're inside a method body. */
        uint32_t cnc = ts_node_child_count(n);
        for (uint32_t i = 0; i < cnc && top + 1 < 512; i++) {
            TSNode c = ts_node_child(n, i);
            if (ts_node_is_null(c)) continue;
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "block") == 0 || strcmp(ck, "arrow_expression_clause") == 0) {
                continue;
            }
            stack[top++] = c;
        }
    }
}

/* ── parse return type from CBMDefinition.return_type ──────────── */

static const CBMType *cs_parse_return_type_text(CSLSPContext *ctx, const char *text) {
    if (!text || !*text) return cbm_type_unknown();
    const char *p = text;
    while (*p == ' ' || *p == ':') p++;
    if (!*p) return cbm_type_unknown();
    /* Strip trailing whitespace + '?' nullability. */
    size_t n = strlen(p);
    while (n > 0 && (p[n - 1] == ' ' || p[n - 1] == '?')) n--;
    if (n == 0) return cbm_type_unknown();
    char *trimmed = cbm_arena_strndup(ctx->arena, p, n);
    const char *resolved = cs_resolve_type_name(ctx, trimmed);
    if (!resolved) return cbm_type_unknown();
    const char *pre = cs_predefined_alias(resolved);
    if (pre) return cbm_type_named(ctx->arena, pre);
    return cbm_type_named(ctx->arena, resolved);
}

/* ── single-file entry: cbm_run_cs_lsp ──────────────────────────── */

void cbm_run_cs_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                    TSNode root) {
    if (!result || !arena || ts_node_is_null(root)) return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    /* Phase A: stdlib. */
    cbm_csharp_stdlib_register(&reg, arena);

    const char *module_qn = result->module_qn;

    /* Phase B: register types + functions from this file's defs. */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name || !d->label) continue;

        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0 ||
            strcmp(d->label, "Struct") == 0 || strcmp(d->label, "Record") == 0 ||
            strcmp(d->label, "Enum") == 0 || strcmp(d->label, "Type") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->name;
            rt.is_interface = (strcmp(d->label, "Interface") == 0);
            if (d->base_classes) {
                int bc = 0;
                while (d->base_classes[bc]) bc++;
                if (bc > 0) {
                    const char **emb = (const char **)cbm_arena_alloc(
                        arena, (size_t)(bc + 1) * sizeof(const char *));
                    if (emb) {
                        for (int j = 0; j < bc; j++) {
                            const char *base = d->base_classes[j];
                            const char *q = base;
                            if (base[0] != '.' && !strchr(base, '.') && module_qn) {
                                q = cbm_arena_sprintf(arena, "%s.%s", module_qn, base);
                            }
                            emb[j] = q;
                        }
                        emb[bc] = NULL;
                        rt.embedded_types = emb;
                    }
                }
            }
            cbm_registry_add_type(&reg, rt);
        }

        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;
            rf.min_params = -1;
            if (strcmp(d->label, "Method") == 0 && d->parent_class) {
                rf.receiver_type = d->parent_class;
            }
            const CBMType *rt =
                d->return_type ? cbm_type_unknown() /* will refine below with ctx */
                               : cbm_type_unknown();
            const CBMType **rets =
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
            if (rets) {
                rets[0] = rt;
                rets[1] = NULL;
            }
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);
            cbm_registry_add_func(&reg, rf);
        }
    }

    /* Phase C: build context for type resolution + run the resolver. */
    CSLSPContext ctx;
    cs_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, &result->resolved_calls);

    /* Pass 1: collect imports + namespaces (for type-resolution context). */
    cs_collect_imports(&ctx, root);

    /* Now refine return types of registered funcs using ctx (which has using
     * directives). Also refine field types via collect_class_fields. */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->return_type) continue;
        if (strcmp(d->label, "Function") != 0 && strcmp(d->label, "Method") != 0) continue;
        const CBMType *rt = cs_parse_return_type_text(&ctx, d->return_type);
        /* Find the registered func and patch its signature. */
        for (int j = 0; j < reg.func_count; j++) {
            if (strcmp(reg.funcs[j].qualified_name, d->qualified_name) == 0) {
                const CBMType **rets =
                    (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
                if (rets) {
                    rets[0] = rt;
                    rets[1] = NULL;
                    reg.funcs[j].signature = cbm_type_func(arena, NULL, NULL, rets);
                }
                break;
            }
        }
    }

    /* Phase B.1.5: AST-driven return-type patch. The unified extractor
     * leaves CBMDefinition.return_type NULL for C# methods, so we walk the
     * AST ourselves to recover declared return types. */
    {
        cs_method_rt_table_t mtab = {0};
        cs_collect_method_return_types(&ctx, root, &mtab);
        for (int i = 0; i < mtab.count; i++) {
            const char *qn = mtab.items[i].qn;
            const CBMType *rt = mtab.items[i].rt;
            bool patched = false;
            for (int j = 0; j < reg.func_count; j++) {
                if (strcmp(reg.funcs[j].qualified_name, qn) == 0) {
                    const CBMType **rets =
                        (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
                    if (rets) {
                        rets[0] = rt;
                        rets[1] = NULL;
                        reg.funcs[j].signature = cbm_type_func(arena, NULL, NULL, rets);
                    }
                    patched = true;
                    break;
                }
            }
            (void)patched;
        }
    }

    /* Phase B.1: collect typed properties + fields. */
    {
        cs_fields_table_t tab = {0};
        cs_collect_class_fields(&ctx, &reg, root, &tab);
        for (int i = 0; i < tab.count; i++) {
            cs_fields_t *f = &tab.items[i];
            if (f->count == 0) continue;
            for (int t = 0; t < reg.type_count; t++) {
                if (strcmp(reg.types[t].qualified_name, f->class_qn) == 0) {
                    reg.types[t].field_names = f->field_names;
                    reg.types[t].field_types = f->field_types;
                    break;
                }
            }
        }
    }

    /* Re-bind ctx state since collect_class_fields may have temporarily
     * adjusted namespace stack (it doesn't, but be safe). */
    ctx.namespace_count = 0;
    /* Restore implicit `using System;`. */

    cs_lsp_process_file(&ctx, root);

    if (ctx.debug) {
        fprintf(stderr, "[cs_lsp] module=%s defs=%d types=%d funcs=%d resolved=%d\n",
                module_qn ? module_qn : "?", result->defs.count, reg.type_count,
                reg.func_count, result->resolved_calls.count);
        for (int i = 0; i < result->resolved_calls.count; i++) {
            const CBMResolvedCall *rc = &result->resolved_calls.items[i];
            fprintf(stderr, "[cs_lsp]   %s -> %s [%s %.2f]\n",
                    rc->caller_qn ? rc->caller_qn : "?",
                    rc->callee_qn ? rc->callee_qn : "?",
                    rc->strategy ? rc->strategy : "?", rc->confidence);
        }
    }
}

/* ── cross-file entry ───────────────────────────────────────────── */

/* Register one batch of CBMLSPDef[] into a registry. Shared by the
 * per-file cross-LSP path and the Tier 2 pre-built registry builder.
 * Def-driven (no per-file AST mutation) so deterministic per def set. */
static void cs_register_lsp_defs(CBMArena *arena, CBMTypeRegistry *reg,
                                 CBMLSPDef *defs, int def_count) {
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label) continue;
        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0 ||
            strcmp(d->label, "Struct") == 0 || strcmp(d->label, "Record") == 0 ||
            strcmp(d->label, "Enum") == 0 || strcmp(d->label, "Type") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->short_name;
            rt.is_interface = d->is_interface || (strcmp(d->label, "Interface") == 0);
            if (d->embedded_types && *d->embedded_types) {
                /* Parse "|"-separated list. */
                int n = 1;
                for (const char *p = d->embedded_types; *p; p++) if (*p == '|') n++;
                const char **arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(*arr));
                if (arr) {
                    int idx = 0;
                    const char *start = d->embedded_types;
                    for (const char *p = d->embedded_types; ; p++) {
                        if (*p == '|' || *p == '\0') {
                            size_t len = (size_t)(p - start);
                            if (len > 0) {
                                arr[idx++] = cbm_arena_strndup(arena, start, len);
                            }
                            if (*p == '\0') break;
                            start = p + 1;
                        }
                    }
                    arr[idx] = NULL;
                    rt.embedded_types = arr;
                }
            }
            cbm_registry_add_type(reg, rt);
        }
        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->short_name;
            rf.min_params = -1;
            if (strcmp(d->label, "Method") == 0 && d->receiver_type) {
                rf.receiver_type = d->receiver_type;
            }
            const CBMType **rets =
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
            if (rets) {
                rets[0] = cbm_type_unknown();
                rets[1] = NULL;
            }
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);
            cbm_registry_add_func(reg, rf);
        }
    }
}

/* Tier 2: build a project-wide C# registry ONCE from all defs (filters
 * by lang). Shared READ-ONLY across resolve workers. Def-driven →
 * identical entries to the per-file build, zero quality loss. */
CBMTypeRegistry *cbm_cs_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count) {
    if (!arena) return NULL;
    CBMTypeRegistry *reg = (CBMTypeRegistry *)cbm_arena_alloc(arena, sizeof(*reg));
    if (!reg) return NULL;
    cbm_registry_init(reg, arena);
    cbm_csharp_stdlib_register(reg, arena);
    for (int i = 0; i < def_count; i++) {
        if (defs[i].lang != CBM_LANG_CSHARP) continue;
        cs_register_lsp_defs(arena, reg, &defs[i], 1);
    }
    cbm_registry_finalize(reg);
    return reg;
}

void cbm_run_cs_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                        const char *module_qn, CBMTypeRegistry *reg,
                                        const char **using_targets, int using_count,
                                        TSTree *cached_tree, CBMResolvedCallArray *out) {
    if (!source || !arena || !out || !reg) return;

    TSTree *tree = cached_tree;
    bool owns = false;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        if (!parser) return;
        ts_parser_set_language(parser, tree_sitter_c_sharp());
        tree = ts_parser_parse_string(parser, NULL, source,
                                       source_len > 0 ? (uint32_t)source_len : (uint32_t)strlen(source));
        ts_parser_delete(parser);
        owns = true;
    }
    if (!tree) return;
    TSNode root = ts_tree_root_node(tree);

    CSLSPContext ctx;
    cs_lsp_init(&ctx, arena, source, source_len, reg, module_qn, out);
    for (int i = 0; i < using_count; i++) {
        if (using_targets[i]) {
            cs_lsp_add_using(&ctx, CBM_CS_USING_NAMESPACE, "", using_targets[i], false);
        }
    }
    cs_lsp_process_file(&ctx, root);

    if (owns) ts_tree_delete(tree);
}

void cbm_run_cs_lsp_cross(CBMArena *arena, const char *source, int source_len,
                           const char *module_qn, CBMLSPDef *defs, int def_count,
                           const char **using_targets, int using_count,
                           TSTree *cached_tree, CBMResolvedCallArray *out) {
    if (!source || !arena) return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_csharp_stdlib_register(&reg, arena);
    cs_register_lsp_defs(arena, &reg, defs, def_count);

    /* Parse if needed. */
    TSTree *tree = cached_tree;
    bool owns = false;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        if (!parser) return;
        ts_parser_set_language(parser, tree_sitter_c_sharp());
        tree = ts_parser_parse_string(parser, NULL, source,
                                       source_len > 0 ? (uint32_t)source_len : (uint32_t)strlen(source));
        ts_parser_delete(parser);
        owns = true;
    }
    if (!tree) return;
    TSNode root = ts_tree_root_node(tree);

    /* Finalize registry — O(1) lookups. See go_lsp.c "3c. Finalize"
     * comment for the rationale. */
    cbm_registry_finalize(&reg);

    CSLSPContext ctx;
    cs_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, out);

    /* Pre-populate using directives from the supplied list. */
    for (int i = 0; i < using_count; i++) {
        if (using_targets[i]) {
            cs_lsp_add_using(&ctx, CBM_CS_USING_NAMESPACE, "", using_targets[i], false);
        }
    }
    cs_lsp_process_file(&ctx, root);

    if (owns) ts_tree_delete(tree);
}

void cbm_batch_cs_lsp_cross(CBMArena *arena, CBMBatchCSLSPFile *files, int file_count,
                             CBMResolvedCallArray *out) {
    if (!arena || !files) return;
    for (int i = 0; i < file_count; i++) {
        CBMBatchCSLSPFile *f = &files[i];
        cbm_run_cs_lsp_cross(arena, f->source, f->source_len, f->module_qn, f->defs, f->def_count,
                              f->using_targets, f->using_count, f->cached_tree, &out[i]);
    }
}
