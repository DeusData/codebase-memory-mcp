/*
 * php_lsp.c — PHP Light Semantic Pass.
 *
 * In-process type-aware call resolver for PHP. Mirrors go_lsp.c / c_lsp.c
 * shape:
 *   1. Build a CBMTypeRegistry from file-local definitions + stdlib +
 *      composer PSR-4 mappings (when present).
 *   2. Walk top-level: collect namespace declaration and `use` clauses.
 *   3. Walk each function/method body, push scope, bind typed parameters
 *      and $this, resolve member/static/function call expressions.
 *
 * Scope is the collide-set attribution problem identified in the
 * pre-flight (see docs/PHP_LSP_PRE_FLIGHT.md). Specifically: when a
 * short name (e.g. value) exists as both a global helper function and a
 * method on a Laravel class, $x->value() must route to the method
 * variant whenever the type of $x is statically determinable, and bare
 * value() must route to the helper.
 */

#include "php_lsp.h"
#include "lsp_node_iter.h"
#include "../helpers.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PHP_EVAL_MAX_DEPTH 32
#define PHP_USE_INITIAL_CAP 16

extern const TSLanguage *tree_sitter_php_only(void);

/* Forward decls */
static void php_resolve_calls_in_node_inner(PHPLSPContext *ctx, TSNode node);

/* Depth-guarded entry for the AST call-resolution walk. The walk recurses once
 * per nesting level; a deeply-nested or cyclic file can overflow the native
 * stack (SIGSEGV) and take down the whole index. Past the cap the subtree is
 * skipped — its calls stay unresolved, which is graceful degradation, not a
 * crash. The cap is CBM_LSP_MAX_WALK_DEPTH, env-overridable via the same name.
 * The walk_depth-- runs after the inner returns, so early returns in the body
 * never leak the counter. */
static void php_resolve_calls_in_node(PHPLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= cbm_lsp_max_walk_depth())
        return;
    ctx->walk_depth++;
    php_resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}
static void process_function_like(PHPLSPContext *ctx, TSNode node);
static void process_class_decl(PHPLSPContext *ctx, TSNode node);
static const CBMType *php_substitute_template(CBMArena *arena, const CBMType *t,
                                              const char *const *param_names,
                                              const CBMType *const *args);
static const CBMType *eval_function_call_type(PHPLSPContext *ctx, TSNode call_node);
static const CBMType *eval_member_call_type(PHPLSPContext *ctx, TSNode call_node);
static const CBMType *eval_object_creation_type(PHPLSPContext *ctx, TSNode node);
static void bind_phpdoc_var(PHPLSPContext *ctx, const char *docstring);
static void parse_phpdoc_for_params(PHPLSPContext *ctx, const char *docstring, TSNode params);
static const CBMType *resolve_phpdoc_type(PHPLSPContext *ctx, const char *type_text);
static char *fetch_leading_phpdoc(PHPLSPContext *ctx, TSNode node);

/* ── helpers ────────────────────────────────────────────────────── */

static char *php_node_text(PHPLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

static bool node_is(TSNode n, const char *kind) {
    if (ts_node_is_null(n))
        return false;
    return strcmp(ts_node_type(n), kind) == 0;
}

static TSNode child_named(TSNode parent, const char *kind) {
    if (ts_node_is_null(parent))
        return parent;
    uint32_t nc = ts_node_child_count(parent);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(parent, i);
        if (!ts_node_is_null(c) && strcmp(ts_node_type(c), kind) == 0)
            return c;
    }
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

/* PHP qualified names use "." in the graph (project.path.module.class[.method]).
 * Convert "App\\Models\\User" to "App.Models.User" so we can compose with
 * module_qn (which already uses ".") and look up registry entries. */
static char *php_ns_to_dot(CBMArena *a, const char *ns) {
    if (!ns)
        return NULL;
    size_t n = strlen(ns);
    char *out = (char *)cbm_arena_alloc(a, n + 1);
    if (!out)
        return NULL;
    for (size_t i = 0; i < n; i++) {
        char c = ns[i];
        out[i] = (c == '\\') ? '.' : c;
    }
    out[n] = '\0';
    /* Trim leading dot from "\\Foo". */
    if (out[0] == '.')
        return out + 1;
    return out;
}

/* Return the substring after the last '.' or '\\'. */
static const char *php_short_name(const char *qn) {
    if (!qn)
        return NULL;
    const char *last = qn;
    for (const char *p = qn; *p; p++) {
        if (*p == '.' || *p == '\\')
            last = p + 1;
    }
    return last;
}

static bool php_is_builtin_type_name(const char *n) {
    if (!n)
        return false;
    static const char *const builtins[] = {
        "int",   "integer",  "float",    "double", "string", "bool",   "boolean",
        "array", "callable", "iterable", "object", "void",   "mixed",  "never",
        "null",  "true",     "false",    "self",   "static", "parent", NULL};
    for (int i = 0; builtins[i]; i++) {
        if (strcmp(n, builtins[i]) == 0)
            return true;
    }
    return false;
}

/* ── init / context ─────────────────────────────────────────────── */

void php_lsp_init(PHPLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                  const CBMTypeRegistry *registry, const char *module_qn,
                  CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->module_qn = module_qn;
    ctx->current_namespace_qn = "";
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL);

    const char *dbg = getenv("CBM_LSP_DEBUG");
    ctx->debug = (dbg && dbg[0]);
}

void php_lsp_add_use(PHPLSPContext *ctx, const char *local_name, const char *target_qn,
                     int use_kind) {
    if (!local_name || !target_qn)
        return;
    if (ctx->use_count >= ctx->use_cap) {
        int new_cap = ctx->use_cap ? ctx->use_cap * 2 : PHP_USE_INITIAL_CAP;
        const char **nl = (const char **)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*nl));
        const char **nq = (const char **)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*nq));
        int *nk = (int *)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(int));
        if (!nl || !nq || !nk)
            return;
        for (int i = 0; i < ctx->use_count; i++) {
            nl[i] = ctx->use_local_names[i];
            nq[i] = ctx->use_target_qns[i];
            nk[i] = (int)ctx->use_kinds[i];
        }
        ctx->use_local_names = nl;
        ctx->use_target_qns = nq;
        /* re-cast to the enum-typed pointer */
        ctx->use_kinds = (void *)nk;
        ctx->use_cap = new_cap;
    }
    ctx->use_local_names[ctx->use_count] = cbm_arena_strdup(ctx->arena, local_name);
    ctx->use_target_qns[ctx->use_count] = cbm_arena_strdup(ctx->arena, target_qn);
    ((int *)ctx->use_kinds)[ctx->use_count] = use_kind;
    ctx->use_count++;
}

static const char *find_use(PHPLSPContext *ctx, const char *local_name, int kind) {
    for (int i = 0; i < ctx->use_count; i++) {
        if ((int)ctx->use_kinds[i] != kind)
            continue;
        if (strcmp(ctx->use_local_names[i], local_name) == 0)
            return ctx->use_target_qns[i];
    }
    return NULL;
}

/* Resolve a class identifier to a fully-qualified registry key.
 *   - Fully-qualified ("\\Foo\\Bar"):       FOO.BAR — strip leading slash.
 *   - Aliased / single segment matches a use:  the use target.
 *   - First segment matches a use:               substitute and append remainder.
 *   - Otherwise:                                 prefix with current_namespace_qn (or module).
 *
 * Returns an arena-allocated string in dotted form. May return NULL for builtins. */
const char *php_resolve_class_name(PHPLSPContext *ctx, const char *name) {
    if (!name || !*name)
        return NULL;

    /* Self / static / parent are class-relative pseudo-types and must be
     * checked BEFORE the builtin-name check, since they're listed as
     * builtins above for type-parsing purposes. */
    if (strcmp(name, "self") == 0 || strcmp(name, "static") == 0) {
        return ctx->enclosing_class_qn;
    }
    if (strcmp(name, "parent") == 0) {
        return ctx->enclosing_parent_qn;
    }
    if (php_is_builtin_type_name(name))
        return NULL;

    /* Fully-qualified — leading backslash. */
    if (name[0] == '\\') {
        return php_ns_to_dot(ctx->arena, name + 1);
    }

    /* Split first segment. */
    const char *sep = name;
    while (*sep && *sep != '\\')
        sep++;
    char *first;
    if (*sep) {
        first = cbm_arena_strndup(ctx->arena, name, (size_t)(sep - name));
    } else {
        first = cbm_arena_strdup(ctx->arena, name);
    }

    const char *use_target = find_use(ctx, first, CBM_PHP_USE_CLASS);
    if (use_target) {
        if (*sep) {
            /* use App\\Models as M;  M\\User -> App.Models.User */
            return cbm_arena_sprintf(ctx->arena, "%s%s", use_target,
                                     php_ns_to_dot(ctx->arena, sep));
        }
        return use_target;
    }

    /* Fallback for a same-file class reference: prefer module_qn-prefixed QN
     * because that's what the unified extractor records. The PHP namespace
     * declaration is *not* incorporated into the unified extractor's QNs
     * (defs are keyed by file path + class name), so building from
     * current_namespace_qn here would diverge and miss every same-file
     * lookup. */
    if (ctx->module_qn && ctx->module_qn[0]) {
        return cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn,
                                 php_ns_to_dot(ctx->arena, name));
    }
    return php_ns_to_dot(ctx->arena, name);
}

/* Try to find a registered type for a "namespaced" QN.
 *
 * Lookup order:
 *   1. exact QN match
 *   2. module_qn + "." + qn (same-file class)
 *   3. project_root + "." + qn (drop trailing module segments one at a time)
 *   4. fall back to a short-name scan of the registry, preferring the
 *      candidate whose QN shares the longest dot-prefix with module_qn
 *      (cross-file class in the same project tree).
 *
 * Step 4 is critical for PHP because the unified extractor builds QNs from
 * file paths but `use App\\Models\\User` produces an "App.Models.User" key.
 * Without short-name fallback, every cross-file class resolution would miss. */
static const CBMRegisteredType *lookup_type_with_project(PHPLSPContext *ctx, const char *qn) {
    if (!qn)
        return NULL;
    const CBMRegisteredType *t = cbm_registry_lookup_type(ctx->registry, qn);
    if (t)
        return t;

    if (ctx->module_qn && ctx->module_qn[0]) {
        const char *combo = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, qn);
        t = cbm_registry_lookup_type(ctx->registry, combo);
        if (t)
            return t;

        const char *m = ctx->module_qn;
        const char *last_dot = strrchr(m, '.');
        while (last_dot) {
            char *prefix = cbm_arena_strndup(ctx->arena, m, (size_t)(last_dot - m));
            const char *try_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", prefix, qn);
            t = cbm_registry_lookup_type(ctx->registry, try_qn);
            if (t)
                return t;
            const char *prev = last_dot;
            last_dot = NULL;
            for (const char *p = m; p < prev; p++) {
                if (*p == '.')
                    last_dot = p;
            }
        }
    }

    /* Step 4: short-name fallback. */
    const char *short_name = qn;
    for (const char *p = qn; *p; p++) {
        if (*p == '.')
            short_name = p + 1;
    }
    if (!short_name || !*short_name)
        return NULL;

    const CBMRegisteredType *best = NULL;
    int best_score = -1;
    for (int i = 0; ctx->registry && i < ctx->registry->type_count; i++) {
        const CBMRegisteredType *cand = &ctx->registry->types[i];
        if (!cand->short_name || strcmp(cand->short_name, short_name) != 0)
            continue;
        int score = 0;
        if (cand->qualified_name && ctx->module_qn) {
            const char *m = ctx->module_qn;
            const char *q = cand->qualified_name;
            while (*m && *q && *m == *q) {
                if (*m == '.')
                    score++;
                m++;
                q++;
            }
        }
        if (score > best_score) {
            best_score = score;
            best = cand;
        }
    }
    return best;
}

/* ── method lookup with parent walk ─────────────────────────────── */

const CBMRegisteredFunc *php_lookup_method(PHPLSPContext *ctx, const char *class_qn,
                                           const char *method_name) {
    if (!class_qn || !method_name)
        return NULL;

    /* Direct lookup by the registry's receiver_qn -> method_name index. */
    const CBMRegisteredFunc *f = cbm_registry_lookup_method(ctx->registry, class_qn, method_name);
    if (f)
        return f;

    /* If the QN we have isn't the registry's canonical key, resolve the
     * type to its registered identity and retry. */
    const CBMRegisteredType *t = cbm_registry_lookup_type(ctx->registry, class_qn);
    if (!t)
        t = lookup_type_with_project(ctx, class_qn);
    if (!t)
        return NULL;
    if (strcmp(t->qualified_name, class_qn) != 0) {
        f = cbm_registry_lookup_method(ctx->registry, t->qualified_name, method_name);
        if (f)
            return f;
    }

    /* Walk the full ancestor chain. PHP only allows single inheritance for
     * `extends`, but a class may also pick up methods from `implements` /
     * traits, both of which are recorded in embedded_types. We iterate
     * across all entries and recurse into each branch with cycle-detection.
     *
     * Cap depth at 16 across all visited types to bound runtime. */
    const char *visited[32];
    int visited_count = 0;
    const char *frontier[32];
    int frontier_count = 0;
    if (t->embedded_types) {
        for (int i = 0; t->embedded_types[i] && frontier_count < 32; i++) {
            frontier[frontier_count++] = t->embedded_types[i];
        }
    }
    while (frontier_count > 0 && visited_count < 32) {
        const char *parent = frontier[--frontier_count];
        bool seen = false;
        for (int v = 0; v < visited_count; v++) {
            if (strcmp(visited[v], parent) == 0) {
                seen = true;
                break;
            }
        }
        if (seen)
            continue;
        visited[visited_count++] = parent;

        f = cbm_registry_lookup_method(ctx->registry, parent, method_name);
        if (f)
            return f;

        const CBMRegisteredType *next = cbm_registry_lookup_type(ctx->registry, parent);
        if (!next)
            next = lookup_type_with_project(ctx, parent);
        if (!next)
            continue;

        if (strcmp(next->qualified_name, parent) != 0) {
            f = cbm_registry_lookup_method(ctx->registry, next->qualified_name, method_name);
            if (f)
                return f;
        }
        if (next->method_qns && next->method_names) {
            for (int i = 0; next->method_names[i]; i++) {
                if (strcmp(next->method_names[i], method_name) == 0) {
                    const CBMRegisteredFunc *cand =
                        cbm_registry_lookup_func(ctx->registry, next->method_qns[i]);
                    if (cand)
                        return cand;
                }
            }
        }
        if (next->embedded_types) {
            for (int i = 0; next->embedded_types[i] && frontier_count < 32; i++) {
                frontier[frontier_count++] = next->embedded_types[i];
            }
        }
    }
    return NULL;
}

/* Detect a __call / __callStatic on a class chain. */
static bool class_has_magic_call(PHPLSPContext *ctx, const char *class_qn, bool is_static) {
    const char *magic = is_static ? "__callStatic" : "__call";
    return php_lookup_method(ctx, class_qn, magic) != NULL;
}

/* ── type parsing ───────────────────────────────────────────────── */

const CBMType *php_parse_type_node(PHPLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();

    const char *kind = ts_node_type(node);

    if (strcmp(kind, "primitive_type") == 0) {
        char *t = php_node_text(ctx, node);
        return cbm_type_builtin(ctx->arena, t ? t : "mixed");
    }
    if (strcmp(kind, "named_type") == 0 || strcmp(kind, "qualified_name") == 0 ||
        strcmp(kind, "name") == 0) {
        char *t = php_node_text(ctx, node);
        if (!t)
            return cbm_type_unknown();
        const char *resolved = php_resolve_class_name(ctx, t);
        if (!resolved)
            return cbm_type_unknown();
        return cbm_type_named(ctx->arena, resolved);
    }
    if (strcmp(kind, "optional_type") == 0) {
        /* ?Foo — strip the '?' and recurse. */
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                return php_parse_type_node(ctx, c);
            }
        }
        return cbm_type_unknown();
    }
    if (strcmp(kind, "union_type") == 0 || strcmp(kind, "intersection_type") == 0 ||
        strcmp(kind, "disjunctive_normal_form_type") == 0) {
        /* Pick the first concrete (non-null) named member as a heuristic;
         * full DNF dispatch is Phase 2.5 at best. */
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            const CBMType *t = php_parse_type_node(ctx, c);
            if (t && t->kind != CBM_TYPE_UNKNOWN) {
                if (t->kind == CBM_TYPE_BUILTIN && strcmp(t->data.builtin.name, "null") == 0) {
                    continue;
                }
                return t;
            }
        }
        return cbm_type_unknown();
    }
    /* Fallback: try to read raw text and treat as a name. */
    char *t = php_node_text(ctx, node);
    if (!t)
        return cbm_type_unknown();
    if (php_is_builtin_type_name(t))
        return cbm_type_builtin(ctx->arena, t);
    const char *resolved = php_resolve_class_name(ctx, t);
    if (!resolved)
        return cbm_type_unknown();
    return cbm_type_named(ctx->arena, resolved);
}

/* ── PHPDoc minimal parser ──────────────────────────────────────── */

/* Strip leading "/**", trailing "*​/", and per-line "*" prefixes. Returns a
 * mutable arena-allocated cleaned copy. */
static char *phpdoc_clean(CBMArena *a, const char *raw) {
    if (!raw)
        return NULL;
    size_t n = strlen(raw);
    char *out = (char *)cbm_arena_alloc(a, n + 1);
    if (!out)
        return NULL;
    size_t w = 0;
    size_t i = 0;
    /* Skip leading /* and ** */
    while (i < n && (raw[i] == '/' || raw[i] == '*' || raw[i] == ' ' || raw[i] == '\t'))
        i++;
    bool at_line_start = false;
    for (; i < n; i++) {
        char c = raw[i];
        if (c == '\n') {
            out[w++] = ' ';
            at_line_start = true;
            continue;
        }
        if (at_line_start) {
            if (c == ' ' || c == '\t' || c == '*')
                continue;
            at_line_start = false;
        }
        out[w++] = c;
    }
    /* Trim trailing */ if (w >= 2 && out[w - 1] == '/' && out[w - 2] == '*')
        w -= 2;
    out[w] = '\0';
    return out;
}

/* (next_tag helper not currently needed — phpdoc parsers use strstr inline.) */

/* Resolve a PHPDoc-style type spec (e.g. "App\\Foo|null", "?App\\Foo",
 * "Collection<User>", "string") to a CBMType. We strip nullables, take the
 * leftmost class in unions, drop generic <...> tails, then resolve. */
/* Parse a `<...>` argument list into an arena-allocated NULL-terminated
 * array of CBMType*. Caller has already skipped past the leading '<'.
 * Returns the number of args parsed; *out_args is the array; *out_close
 * is set to the position just after the matching '>'. */
static int parse_phpdoc_template_args(PHPLSPContext *ctx, const char *p, const CBMType ***out_args,
                                      const char **out_close) {
    /* Allocate a small buffer; grow if needed. */
    int cap = 4;
    int count = 0;
    const CBMType **buf =
        (const CBMType **)cbm_arena_alloc(ctx->arena, (size_t)(cap + 1) * sizeof(*buf));
    if (!buf) {
        *out_args = NULL;
        *out_close = p;
        return 0;
    }
    int depth = 1;
    while (*p && depth > 0) {
        while (*p == ' ' || *p == '\t' || *p == ',')
            p++;
        if (*p == '>')
            break;
        const char *arg_start = p;
        while (*p && depth > 0) {
            if (*p == '<')
                depth++;
            else if (*p == '>') {
                depth--;
                if (depth == 0)
                    break;
            } else if (*p == ',' && depth == 1) {
                break;
            }
            p++;
        }
        size_t arg_len = (size_t)(p - arg_start);
        while (arg_len > 0 && (arg_start[arg_len - 1] == ' ' || arg_start[arg_len - 1] == '\t')) {
            arg_len--;
        }
        if (arg_len > 0) {
            char *arg_text = cbm_arena_strndup(ctx->arena, arg_start, arg_len);
            const CBMType *arg_type = resolve_phpdoc_type(ctx, arg_text);
            if (count >= cap) {
                int new_cap = cap * 2;
                const CBMType **new_buf = (const CBMType **)cbm_arena_alloc(
                    ctx->arena, (size_t)(new_cap + 1) * sizeof(*new_buf));
                if (new_buf) {
                    for (int i = 0; i < count; i++)
                        new_buf[i] = buf[i];
                    buf = new_buf;
                    cap = new_cap;
                }
            }
            buf[count++] = arg_type;
        }
    }
    if (*p == '>')
        p++;
    buf[count] = NULL;
    *out_args = buf;
    *out_close = p;
    return count;
}

/* Add a @phpstan-type alias to ctx (idempotent — keeps first definition). */
static void php_add_phpstan_alias(PHPLSPContext *ctx, const char *name, const CBMType *type) {
    if (!ctx || !name || !type)
        return;
    for (int i = 0; i < ctx->phpstan_alias_count; i++) {
        if (strcmp(ctx->phpstan_alias_names[i], name) == 0)
            return;
    }
    if (ctx->phpstan_alias_count >= ctx->phpstan_alias_cap) {
        int new_cap = ctx->phpstan_alias_cap ? ctx->phpstan_alias_cap * 2 : 8;
        const char **nn =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)(new_cap + 1) * sizeof(*nn));
        const CBMType **nt =
            (const CBMType **)cbm_arena_alloc(ctx->arena, (size_t)(new_cap + 1) * sizeof(*nt));
        if (!nn || !nt)
            return;
        for (int i = 0; i < ctx->phpstan_alias_count; i++) {
            nn[i] = ctx->phpstan_alias_names[i];
            nt[i] = ctx->phpstan_alias_types[i];
        }
        ctx->phpstan_alias_names = nn;
        ctx->phpstan_alias_types = nt;
        ctx->phpstan_alias_cap = new_cap;
    }
    ctx->phpstan_alias_names[ctx->phpstan_alias_count] = cbm_arena_strdup(ctx->arena, name);
    ctx->phpstan_alias_types[ctx->phpstan_alias_count] = type;
    ctx->phpstan_alias_count++;
}

static const CBMType *php_lookup_phpstan_alias(PHPLSPContext *ctx, const char *name) {
    if (!ctx || !name)
        return NULL;
    for (int i = 0; i < ctx->phpstan_alias_count; i++) {
        if (strcmp(ctx->phpstan_alias_names[i], name) == 0) {
            return ctx->phpstan_alias_types[i];
        }
    }
    return NULL;
}

static const CBMType *resolve_phpdoc_type(PHPLSPContext *ctx, const char *type_text) {
    if (!type_text || !*type_text)
        return cbm_type_unknown();
    /* Skip whitespace + nullable */
    while (*type_text == ' ' || *type_text == '\t' || *type_text == '?')
        type_text++;
    /* Take portion up to first '|', ' ', '\t', '<' */
    size_t end = 0;
    while (type_text[end] && type_text[end] != '|' && type_text[end] != ' ' &&
           type_text[end] != '\t' && type_text[end] != '<' && type_text[end] != '\n' &&
           type_text[end] != ',' && type_text[end] != '>' && type_text[end] != '{') {
        end++;
    }
    if (end == 0)
        return cbm_type_unknown();
    char *first = cbm_arena_strndup(ctx->arena, type_text, end);
    if (!first)
        return cbm_type_unknown();
    if (strcmp(first, "null") == 0) {
        /* take next member */
        if (type_text[end] == '|')
            return resolve_phpdoc_type(ctx, type_text + end + 1);
        return cbm_type_unknown();
    }

    /* Generic? `Foo<Bar>` or `array<int, User>` */
    bool has_generic = (type_text[end] == '<');
    bool has_array_shape = (type_text[end] == '{');

    /* `array{...}` shape — treat as plain array for now. */
    if (has_array_shape)
        return cbm_type_builtin(ctx->arena, "array");

    /* @phpstan-type alias takes precedence over name resolution. */
    {
        const CBMType *aliased = php_lookup_phpstan_alias(ctx, first);
        if (aliased)
            return aliased;
    }

    if (php_is_builtin_type_name(first)) {
        if (has_generic) {
            /* `array<T>`, `iterable<T>`, `list<T>`, `callable<...>`. We
             * preserve the template form so foreach can extract elements
             * later. */
            const CBMType **args = NULL;
            const char *after = NULL;
            int n = parse_phpdoc_template_args(ctx, type_text + end + 1, &args, &after);
            return cbm_type_template(ctx->arena, first, args, n);
        }
        return cbm_type_builtin(ctx->arena, first);
    }

    const char *resolved = php_resolve_class_name(ctx, first);
    if (!resolved)
        return cbm_type_unknown();

    if (has_generic) {
        const CBMType **args = NULL;
        const char *after = NULL;
        int n = parse_phpdoc_template_args(ctx, type_text + end + 1, &args, &after);
        return cbm_type_template(ctx->arena, resolved, args, n);
    }
    return cbm_type_named(ctx->arena, resolved);
}

/* @param Type $name — rebind matching parameter. Tolerates generic
 * `<...>` in the type token. */
static void parse_phpdoc_for_params(PHPLSPContext *ctx, const char *docstring, TSNode params) {
    (void)params;
    if (!docstring || !ctx->current_scope)
        return;
    const char *p = docstring;
    while ((p = strstr(p, "@param")) != NULL) {
        p += 6;
        while (*p == ' ' || *p == '\t')
            p++;
        const char *type_start = p;
        int depth = 0;
        while (*p && *p != '\n') {
            if (*p == '<')
                depth++;
            else if (*p == '>')
                depth--;
            else if (depth == 0 && (*p == ' ' || *p == '\t' || *p == '$'))
                break;
            p++;
        }
        if (p == type_start)
            continue;
        char *type_text = cbm_arena_strndup(ctx->arena, type_start, (size_t)(p - type_start));
        while (*p == ' ' || *p == '\t')
            p++;
        if (*p != '$')
            continue;
        p++;
        const char *name_start = p;
        while (*p && (isalnum((unsigned char)*p) || *p == '_'))
            p++;
        if (p == name_start)
            continue;
        char *vname = cbm_arena_strndup(ctx->arena, name_start, (size_t)(p - name_start));
        const CBMType *t = resolve_phpdoc_type(ctx, type_text);
        if (vname && t && t->kind != CBM_TYPE_UNKNOWN) {
            cbm_scope_bind(ctx->current_scope, vname, t);
        }
    }
}

/* @var Type $name — bind a variable in current scope.
 * Generic types like `array<User>` may contain whitespace-free `<>` — we
 * tolerate those by including '<' and '>' in the type token. */
static void bind_phpdoc_var(PHPLSPContext *ctx, const char *docstring) {
    if (!docstring || !ctx->current_scope)
        return;
    const char *p = docstring;
    while ((p = strstr(p, "@var")) != NULL) {
        p += 4;
        while (*p == ' ' || *p == '\t')
            p++;
        const char *type_start = p;
        /* Take a type token: stop at whitespace or '$' (start of var name).
         * Allow '<', '>', ',', '|', '?' inside the type. */
        int depth = 0;
        while (*p && *p != '\n') {
            if (*p == '<')
                depth++;
            else if (*p == '>')
                depth--;
            else if (depth == 0 && (*p == ' ' || *p == '\t' || *p == '$'))
                break;
            p++;
        }
        if (p == type_start)
            continue;
        char *type_text = cbm_arena_strndup(ctx->arena, type_start, (size_t)(p - type_start));
        while (*p == ' ' || *p == '\t')
            p++;
        if (*p != '$')
            continue;
        p++;
        const char *name_start = p;
        while (*p && (isalnum((unsigned char)*p) || *p == '_'))
            p++;
        if (p == name_start)
            continue;
        char *vname = cbm_arena_strndup(ctx->arena, name_start, (size_t)(p - name_start));
        const CBMType *t = resolve_phpdoc_type(ctx, type_text);
        if (vname && t && t->kind != CBM_TYPE_UNKNOWN) {
            cbm_scope_bind(ctx->current_scope, vname, t);
        }
    }
}

/* Walk siblings backwards from `node` to find a leading PHPDoc comment
 * ("/**...*​/"). Returns cleaned doc text or NULL. */
static char *fetch_leading_phpdoc(PHPLSPContext *ctx, TSNode node) {
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent))
        return NULL;
    uint32_t nc = ts_node_child_count(parent);
    /* Find index of node within parent. */
    int idx = -1;
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(parent, i);
        if (ts_node_eq(c, node)) {
            idx = (int)i;
            break;
        }
    }
    if (idx <= 0)
        return NULL;
    for (int i = idx - 1; i >= 0; i--) {
        TSNode c = ts_node_child(parent, (uint32_t)i);
        if (ts_node_is_null(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "comment") == 0) {
            char *raw = php_node_text(ctx, c);
            if (raw && raw[0] == '/' && raw[1] == '*' && raw[2] == '*') {
                return phpdoc_clean(ctx->arena, raw);
            }
            return NULL;
        }
        /* Skip whitespace-only or attributes; stop at any other node. */
        if (strcmp(k, "attribute_list") == 0 || strcmp(k, "attribute_group") == 0)
            continue;
        return NULL;
    }
    return NULL;
}

/* ── expression evaluation ──────────────────────────────────────── */

/* Returns the type of a tree-sitter PHP expression node.
 * Coverage:
 *   - variable_name ($x):                          scope lookup ($this special-cased).
 *   - object_creation_expression (new X):          named X.
 *   - member_call_expression / nullsafe variant:   recurse into return type.
 *   - scoped_call_expression (X::m()):             recurse into return type.
 *   - function_call_expression:                    function return type.
 *   - assignment_expression ($a = ...):            type of RHS.
 *   - cast_expression:                             casted-to type.
 *   - parenthesized_expression:                    inner expression.
 *   - encapsed_string / string:                    builtin "string".
 *   - integer / float:                             builtin "int"/"float".
 *   - boolean / true / false:                      builtin "bool".
 *   - null:                                        builtin "null".
 *   - clone_expression:                            type of operand.
 */
const CBMType *php_eval_expr_type(PHPLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node) || ctx->eval_depth >= PHP_EVAL_MAX_DEPTH) {
        return cbm_type_unknown();
    }
    ctx->eval_depth++;
    const CBMType *result = cbm_type_unknown();
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "variable_name") == 0) {
        char *t = php_node_text(ctx, node);
        if (t) {
            /* Strip leading $. */
            const char *name = (t[0] == '$') ? t + 1 : t;
            if (strcmp(name, "this") == 0) {
                if (ctx->enclosing_class_qn) {
                    result = cbm_type_named(ctx->arena, ctx->enclosing_class_qn);
                }
            } else {
                const CBMType *bound = cbm_scope_lookup(ctx->current_scope, name);
                if (bound)
                    result = bound;
            }
        }
    } else if (strcmp(kind, "object_creation_expression") == 0) {
        result = eval_object_creation_type(ctx, node);
    } else if (strcmp(kind, "member_call_expression") == 0 ||
               strcmp(kind, "nullsafe_member_call_expression") == 0 ||
               strcmp(kind, "scoped_call_expression") == 0) {
        result = eval_member_call_type(ctx, node);
    } else if (strcmp(kind, "function_call_expression") == 0) {
        result = eval_function_call_type(ctx, node);
    } else if (strcmp(kind, "assignment_expression") == 0) {
        TSNode rhs = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(rhs))
            result = php_eval_expr_type(ctx, rhs);
    } else if (strcmp(kind, "cast_expression") == 0) {
        TSNode tnode = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(tnode))
            result = php_parse_type_node(ctx, tnode);
    } else if (strcmp(kind, "parenthesized_expression") == 0) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                result = php_eval_expr_type(ctx, c);
                break;
            }
        }
    } else if (strcmp(kind, "clone_expression") == 0) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                result = php_eval_expr_type(ctx, c);
                break;
            }
        }
    } else if (strcmp(kind, "class_constant_access_expression") == 0 ||
               strcmp(kind, "scoped_property_access_expression") == 0) {
        /* `Suit::Hearts` — if the LHS resolves to a registered class with a
         * field by that constant name, return the field's type. This makes
         * enum cases bind to the enum type. */
        TSNode lhs;
        memset(&lhs, 0, sizeof(lhs));
        TSNode rhs;
        memset(&rhs, 0, sizeof(rhs));
        uint32_t cnc = ts_node_child_count(node);
        int seen = 0;
        for (uint32_t i = 0; i < cnc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            if (seen == 0)
                lhs = c;
            else if (seen == 1)
                rhs = c;
            seen++;
        }
        if (!ts_node_is_null(lhs) && !ts_node_is_null(rhs)) {
            char *cls_text = php_node_text(ctx, lhs);
            char *member = php_node_text(ctx, rhs);
            if (cls_text && member) {
                const char *cls_qn = NULL;
                if (strcmp(cls_text, "self") == 0 || strcmp(cls_text, "static") == 0)
                    cls_qn = ctx->enclosing_class_qn;
                else if (strcmp(cls_text, "parent") == 0)
                    cls_qn = ctx->enclosing_parent_qn;
                else
                    cls_qn = php_resolve_class_name(ctx, cls_text);
                if (cls_qn) {
                    const CBMRegisteredType *t = cbm_registry_lookup_type(ctx->registry, cls_qn);
                    if (!t)
                        t = lookup_type_with_project(ctx, cls_qn);
                    if (t && t->field_names) {
                        for (int i = 0; t->field_names[i]; i++) {
                            if (strcmp(t->field_names[i], member) == 0) {
                                if (t->field_types && t->field_types[i]) {
                                    result = t->field_types[i];
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    } else if (strcmp(kind, "subscript_expression") == 0) {
        /* $arr[$k] — if $arr: TEMPLATE("array"|"list"|..., [V]) then we
         * return V (or the value-side of array<K, V>). NAMED collection
         * types fall back to UNKNOWN unless the registered type provides
         * an offsetGet method. */
        TSNode obj = ts_node_child_by_field_name(node, "dereferencable", 14);
        if (ts_node_is_null(obj)) {
            uint32_t nc = ts_node_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_child(node, i);
                if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                    obj = c;
                    break;
                }
            }
        }
        if (!ts_node_is_null(obj)) {
            const CBMType *recv = php_eval_expr_type(ctx, obj);
            if (recv && recv->kind == CBM_TYPE_TEMPLATE) {
                const CBMType *const *args = recv->data.template_type.template_args;
                if (args) {
                    int n = 0;
                    while (args[n])
                        n++;
                    if (n >= 2 && args[1])
                        result = args[1];
                    else if (n >= 1 && args[0])
                        result = args[0];
                }
            } else if (recv && recv->kind == CBM_TYPE_NAMED) {
                /* offsetGet on a registered ArrayAccess-like class. */
                const CBMRegisteredFunc *og =
                    php_lookup_method(ctx, recv->data.named.qualified_name, "offsetGet");
                if (og && og->signature && og->signature->kind == CBM_TYPE_FUNC &&
                    og->signature->data.func.return_types &&
                    og->signature->data.func.return_types[0]) {
                    result = og->signature->data.func.return_types[0];
                }
            }
        }
    } else if (strcmp(kind, "match_expression") == 0) {
        /* match($v) { ... => arm1, default => arm2 } — result is the
         * union of arm result expressions. We take the first arm result
         * with a known type, falling back to UNKNOWN. */
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "match_block") == 0) {
                uint32_t bnc = ts_node_child_count(c);
                for (uint32_t j = 0; j < bnc; j++) {
                    TSNode arm = ts_node_child(c, j);
                    if (ts_node_is_null(arm) || !ts_node_is_named(arm))
                        continue;
                    const char *ak = ts_node_type(arm);
                    if (strcmp(ak, "match_conditional_expression") != 0 &&
                        strcmp(ak, "match_default_expression") != 0)
                        continue;
                    /* The arm expression is the field "value" or the last
                     * named child after "=>". */
                    TSNode val = ts_node_child_by_field_name(arm, "return_expression", 17);
                    if (ts_node_is_null(val))
                        val = ts_node_child_by_field_name(arm, "value", 5);
                    if (ts_node_is_null(val)) {
                        uint32_t vnc = ts_node_child_count(arm);
                        for (uint32_t k2 = vnc; k2 > 0; k2--) {
                            TSNode vv = ts_node_child(arm, k2 - 1);
                            if (!ts_node_is_null(vv) && ts_node_is_named(vv)) {
                                val = vv;
                                break;
                            }
                        }
                    }
                    if (ts_node_is_null(val))
                        continue;
                    const CBMType *t = php_eval_expr_type(ctx, val);
                    if (t && t->kind != CBM_TYPE_UNKNOWN) {
                        result = t;
                        goto match_done;
                    }
                }
            }
        }
    match_done:;
    } else if (strcmp(kind, "conditional_expression") == 0) {
        /* $a ? $b : $c — result is type of $b (preferred) or $c. */
        TSNode then_n = ts_node_child_by_field_name(node, "body", 4);
        TSNode else_n = ts_node_child_by_field_name(node, "alternative", 11);
        if (ts_node_is_null(then_n) || ts_node_is_null(else_n)) {
            /* tree-sitter-php names: `consequence` / `alternative` */
            then_n = ts_node_child_by_field_name(node, "consequence", 11);
        }
        if (!ts_node_is_null(then_n)) {
            const CBMType *t = php_eval_expr_type(ctx, then_n);
            if (t && t->kind != CBM_TYPE_UNKNOWN) {
                result = t;
            }
        }
        if ((!result || result->kind == CBM_TYPE_UNKNOWN) && !ts_node_is_null(else_n)) {
            const CBMType *t = php_eval_expr_type(ctx, else_n);
            if (t && t->kind != CBM_TYPE_UNKNOWN)
                result = t;
        }
    } else if (strcmp(kind, "encapsed_string") == 0 || strcmp(kind, "string") == 0 ||
               strcmp(kind, "string_value") == 0 || strcmp(kind, "heredoc") == 0) {
        result = cbm_type_builtin(ctx->arena, "string");
    } else if (strcmp(kind, "integer") == 0) {
        result = cbm_type_builtin(ctx->arena, "int");
    } else if (strcmp(kind, "float") == 0) {
        result = cbm_type_builtin(ctx->arena, "float");
    } else if (strcmp(kind, "boolean") == 0 || strcmp(kind, "true") == 0 ||
               strcmp(kind, "false") == 0) {
        result = cbm_type_builtin(ctx->arena, "bool");
    } else if (strcmp(kind, "null") == 0) {
        result = cbm_type_builtin(ctx->arena, "null");
    } else if (strcmp(kind, "member_access_expression") == 0 ||
               strcmp(kind, "nullsafe_member_access_expression") == 0) {
        /* $x->prop — look up field type if known. */
        TSNode obj = ts_node_child_by_field_name(node, "object", 6);
        TSNode field = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(obj) && !ts_node_is_null(field)) {
            const CBMType *recv = php_eval_expr_type(ctx, obj);
            if (recv && recv->kind == CBM_TYPE_NAMED) {
                char *fname = php_node_text(ctx, field);
                const CBMRegisteredType *t =
                    cbm_registry_lookup_type(ctx->registry, recv->data.named.qualified_name);
                if (!t)
                    t = lookup_type_with_project(ctx, recv->data.named.qualified_name);
                if (t && t->field_names && fname) {
                    for (int i = 0; t->field_names[i]; i++) {
                        if (strcmp(t->field_names[i], fname) == 0) {
                            if (t->field_types && t->field_types[i]) {
                                result = t->field_types[i];
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    ctx->eval_depth--;
    return result ? result : cbm_type_unknown();
}

static const CBMType *eval_object_creation_type(PHPLSPContext *ctx, TSNode node) {
    /* Find the class identifier in `new X(...)`. */
    TSNode name_node;
    memset(&name_node, 0, sizeof(name_node));
    name_node = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name_node)) {
        /* Fallback: first named child that is a name/qualified_name. */
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "name") == 0 || strcmp(k, "qualified_name") == 0) {
                name_node = c;
                break;
            }
        }
    }
    if (ts_node_is_null(name_node))
        return cbm_type_unknown();
    char *name = php_node_text(ctx, name_node);
    if (!name)
        return cbm_type_unknown();
    const char *resolved = php_resolve_class_name(ctx, name);
    if (!resolved)
        return cbm_type_unknown();
    return cbm_type_named(ctx->arena, resolved);
}

static const CBMType *eval_function_call_type(PHPLSPContext *ctx, TSNode call_node) {
    TSNode fn = ts_node_child_by_field_name(call_node, "function", 8);
    if (ts_node_is_null(fn))
        return cbm_type_unknown();
    char *name = php_node_text(ctx, fn);
    if (!name)
        return cbm_type_unknown();
    /* Try `use function` map first. */
    const char *target = find_use(ctx, name, CBM_PHP_USE_FUNCTION);
    const char *qn = NULL;
    if (target) {
        qn = target;
    } else if (ctx->current_namespace_qn && ctx->current_namespace_qn[0]) {
        qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace_qn, name);
    } else {
        qn = cbm_arena_strdup(ctx->arena, name);
    }
    const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, qn);
    if (!f && ctx->module_qn) {
        f = cbm_registry_lookup_func(ctx->registry,
                                     cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name));
    }
    if (!f && ctx->registry) {
        /* Walk the symbol table by short_name as a last resort. */
        for (int i = 0; i < ctx->registry->func_count; i++) {
            const CBMRegisteredFunc *cand = &ctx->registry->funcs[i];
            if (cand->receiver_type)
                continue;
            if (cand->short_name && strcmp(cand->short_name, name) == 0) {
                f = cand;
                break;
            }
        }
    }
    if (!f || !f->signature)
        return cbm_type_unknown();
    /* PHP single-return convention. */
    const CBMType *sig = f->signature;
    if (sig->kind == CBM_TYPE_FUNC && sig->data.func.return_types &&
        sig->data.func.return_types[0]) {
        return sig->data.func.return_types[0];
    }
    return cbm_type_unknown();
}

/* Returns the return type of an instance/static method call expression. */
static const CBMType *eval_member_call_type(PHPLSPContext *ctx, TSNode call_node) {
    const char *kind = ts_node_type(call_node);
    bool is_static = strcmp(kind, "scoped_call_expression") == 0;

    TSNode recv_node;
    TSNode name_node;
    if (is_static) {
        recv_node = ts_node_child_by_field_name(call_node, "scope", 5);
        name_node = ts_node_child_by_field_name(call_node, "name", 4);
    } else {
        recv_node = ts_node_child_by_field_name(call_node, "object", 6);
        name_node = ts_node_child_by_field_name(call_node, "name", 4);
    }
    if (ts_node_is_null(name_node))
        return cbm_type_unknown();
    char *method_name = php_node_text(ctx, name_node);
    if (!method_name)
        return cbm_type_unknown();

    const char *class_qn = NULL;
    if (is_static) {
        if (!ts_node_is_null(recv_node)) {
            char *cls = php_node_text(ctx, recv_node);
            if (cls) {
                if (strcmp(cls, "self") == 0 || strcmp(cls, "static") == 0)
                    class_qn = ctx->enclosing_class_qn;
                else if (strcmp(cls, "parent") == 0)
                    class_qn = ctx->enclosing_parent_qn;
                else
                    class_qn = php_resolve_class_name(ctx, cls);
            }
        }
    } else {
        const CBMType *recv_type =
            ts_node_is_null(recv_node) ? cbm_type_unknown() : php_eval_expr_type(ctx, recv_node);
        if (recv_type && recv_type->kind == CBM_TYPE_NAMED) {
            class_qn = recv_type->data.named.qualified_name;
        } else if (recv_type && recv_type->kind == CBM_TYPE_TEMPLATE) {
            class_qn = recv_type->data.template_type.template_name;
        }
        if (recv_type && recv_type->kind == CBM_TYPE_TEMPLATE) {
            /* Apply generic substitution to method return type. */
            const CBMRegisteredFunc *f = php_lookup_method(ctx, class_qn, method_name);
            if (!f || !f->signature)
                return cbm_type_unknown();
            const CBMType *sig = f->signature;
            if (sig->kind == CBM_TYPE_FUNC && sig->data.func.return_types &&
                sig->data.func.return_types[0]) {
                const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, class_qn);
                if (!rt)
                    rt = lookup_type_with_project(ctx, class_qn);
                if (rt && rt->type_param_names && recv_type->data.template_type.template_args) {
                    return php_substitute_template(ctx->arena, sig->data.func.return_types[0],
                                                   rt->type_param_names,
                                                   recv_type->data.template_type.template_args);
                }
                return sig->data.func.return_types[0];
            }
            return cbm_type_unknown();
        }
    }
    if (!class_qn)
        return cbm_type_unknown();

    const CBMRegisteredFunc *f = php_lookup_method(ctx, class_qn, method_name);
    if (!f || !f->signature)
        return cbm_type_unknown();
    const CBMType *sig = f->signature;
    if (sig->kind == CBM_TYPE_FUNC && sig->data.func.return_types &&
        sig->data.func.return_types[0]) {
        return sig->data.func.return_types[0];
    }
    return cbm_type_unknown();
}

/* ── emit ───────────────────────────────────────────────────────── */

static void emit_resolved_reason(PHPLSPContext *ctx, const char *callee_qn, const char *strategy,
                                 float confidence, const char *reason) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    rc.reason = reason;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

static void emit_resolved(PHPLSPContext *ctx, const char *callee_qn, const char *strategy,
                          float confidence) {
    emit_resolved_reason(ctx, callee_qn, strategy, confidence, NULL);
}

static void emit_unresolved(PHPLSPContext *ctx, const char *expr_text, const char *reason) {
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = expr_text ? expr_text : "?";
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = reason;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

/* ── statement-level scope binding ──────────────────────────────── */

/* Recognise `$x = new Foo(...)` / `$x = Foo::create()` etc. and bind the LHS. */
static void process_assignment(PHPLSPContext *ctx, TSNode node) {
    TSNode lhs = ts_node_child_by_field_name(node, "left", 4);
    TSNode rhs = ts_node_child_by_field_name(node, "right", 5);
    if (ts_node_is_null(lhs) || ts_node_is_null(rhs))
        return;
    if (!node_is(lhs, "variable_name"))
        return;
    char *t = php_node_text(ctx, lhs);
    if (!t)
        return;
    const char *name = (t[0] == '$') ? t + 1 : t;
    if (strcmp(name, "this") == 0)
        return;
    const CBMType *rhs_type = php_eval_expr_type(ctx, rhs);
    if (rhs_type && rhs_type->kind != CBM_TYPE_UNKNOWN) {
        /* Don't override a more-specific (NAMED/TEMPLATE) binding with
         * a trivial null literal — preserves `@var T $x; $x = null;`. */
        if (rhs_type->kind == CBM_TYPE_BUILTIN && rhs_type->data.builtin.name &&
            strcmp(rhs_type->data.builtin.name, "null") == 0) {
            const CBMType *existing = cbm_scope_lookup(ctx->current_scope, name);
            if (existing &&
                (existing->kind == CBM_TYPE_NAMED || existing->kind == CBM_TYPE_TEMPLATE)) {
                return;
            }
        }
        cbm_scope_bind(ctx->current_scope, name, rhs_type);
    }
}

static void process_foreach(PHPLSPContext *ctx, TSNode node) {
    /* Try to extract the iterable expression's element type. tree-sitter-php
     * emits the iterable as the first sibling expression after `foreach (`,
     * and the loop var is the variable_name child after `as`.
     *
     * Element-type resolution rules:
     *   - iterable is array<T> / list<T> / iterable<T> (TEMPLATE)         → T
     *   - iterable is array<K, V> (TEMPLATE arg_count=2)                  → V (value)
     *   - iterable is a NAMED collection-like type whose `current()` method
     *     has a known return type (e.g. Iterator)                          → that
     *   - otherwise                                                         → UNKNOWN
     */
    TSNode iterable;
    memset(&iterable, 0, sizeof(iterable));
    TSNode loop_vars[4];
    int loop_var_count = 0;
    memset(loop_vars, 0, sizeof(loop_vars));

    /* tree-sitter-php emits foreach_statement children in source order:
     *   <iterable expression> ... `as` ... <loop var(s)> ... <body>
     * The iterable can be ANY expression: variable_name, member_call,
     * function_call_expression, member_access_expression, etc. The loop
     * vars are always variable_name. The body is compound_statement /
     * colon_block / a statement.
     *
     * We find the iterable as the first named child whose kind is an
     * expression-shape; subsequent variable_name children before the body
     * are the loop vars. */
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "compound_statement") == 0 || strcmp(k, "colon_block") == 0 ||
            strcmp(k, "expression_statement") == 0 || strcmp(k, "echo_statement") == 0 ||
            strcmp(k, "return_statement") == 0 || strcmp(k, "if_statement") == 0 ||
            strcmp(k, "for_statement") == 0 || strcmp(k, "while_statement") == 0 ||
            strcmp(k, "switch_statement") == 0 || strcmp(k, "try_statement") == 0) {
            break; /* reached body */
        }
        if (ts_node_is_null(iterable)) {
            iterable = c;
        } else if (strcmp(k, "variable_name") == 0 && loop_var_count < 4) {
            loop_vars[loop_var_count++] = c;
        } else if (strcmp(k, "pair") == 0) {
            /* `$k => $v` — extract both variable_names; the second is the
             * value, gets the elem_type. */
            uint32_t pnc = ts_node_child_count(c);
            for (uint32_t j = 0; j < pnc; j++) {
                TSNode pc = ts_node_child(c, j);
                if (!ts_node_is_null(pc) && ts_node_is_named(pc) &&
                    strcmp(ts_node_type(pc), "variable_name") == 0 && loop_var_count < 4) {
                    loop_vars[loop_var_count++] = pc;
                }
            }
        }
    }

    const CBMType *elem_type = cbm_type_unknown();
    if (!ts_node_is_null(iterable)) {
        const CBMType *it_type = php_eval_expr_type(ctx, iterable);
        if (it_type && it_type->kind == CBM_TYPE_TEMPLATE) {
            const CBMType *const *args = it_type->data.template_type.template_args;
            if (args) {
                int n = 0;
                while (args[n])
                    n++;
                if (n >= 2 && args[1])
                    elem_type = args[1];
                else if (n >= 1 && args[0])
                    elem_type = args[0];
            }
        } else if (it_type && it_type->kind == CBM_TYPE_NAMED) {
            const CBMRegisteredFunc *cur =
                php_lookup_method(ctx, it_type->data.named.qualified_name, "current");
            if (cur && cur->signature && cur->signature->kind == CBM_TYPE_FUNC &&
                cur->signature->data.func.return_types &&
                cur->signature->data.func.return_types[0] &&
                cur->signature->data.func.return_types[0]->kind != CBM_TYPE_UNKNOWN) {
                elem_type = cur->signature->data.func.return_types[0];
            }
        }
    }

    /* Bind the loop variable(s). PHP allows `foreach ($xs as $k => $v)` with
     * two: $k is the key, $v is the element. We bind elem_type to the LAST
     * variable_name (the value); earlier ones get UNKNOWN as approximation. */
    for (int i = 0; i < loop_var_count; i++) {
        char *t = php_node_text(ctx, loop_vars[i]);
        if (!t)
            continue;
        const char *name = (t[0] == '$') ? t + 1 : t;
        const CBMType *bind_type = (i == loop_var_count - 1) ? elem_type : cbm_type_unknown();
        cbm_scope_bind(ctx->current_scope, name, bind_type);
    }
}

static void process_catch(PHPLSPContext *ctx, TSNode node) {
    /* Bind $e to the caught exception type. */
    TSNode type_list;
    memset(&type_list, 0, sizeof(type_list));
    TSNode var_node;
    memset(&var_node, 0, sizeof(var_node));
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "type_list") == 0)
            type_list = c;
        if (strcmp(k, "variable_name") == 0)
            var_node = c;
    }
    if (ts_node_is_null(var_node))
        return;
    char *vt = php_node_text(ctx, var_node);
    if (!vt)
        return;
    const char *vname = (vt[0] == '$') ? vt + 1 : vt;
    const CBMType *exc_type = cbm_type_unknown();
    if (!ts_node_is_null(type_list)) {
        uint32_t tnc = ts_node_child_count(type_list);
        for (uint32_t i = 0; i < tnc; i++) {
            TSNode tc = ts_node_child(type_list, i);
            if (ts_node_is_null(tc) || !ts_node_is_named(tc))
                continue;
            exc_type = php_parse_type_node(ctx, tc);
            break;
        }
    }
    cbm_scope_bind(ctx->current_scope, vname, exc_type);
}

/* ── call resolution ────────────────────────────────────────────── */

static void resolve_function_call(PHPLSPContext *ctx, TSNode call) {
    TSNode fn = ts_node_child_by_field_name(call, "function", 8);
    if (ts_node_is_null(fn))
        return;
    char *name = php_node_text(ctx, fn);
    if (!name)
        return;

    /* `use function Foo\\bar;` mapping. */
    const char *target = find_use(ctx, name, CBM_PHP_USE_FUNCTION);
    if (target) {
        const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, target);
        if (f) {
            emit_resolved(ctx, f->qualified_name, "php_function_namespaced", 0.95f);
            return;
        }
    }

    /* Current namespace, then global fallback. */
    if (ctx->current_namespace_qn && ctx->current_namespace_qn[0]) {
        const char *ns_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace_qn, name);
        const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, ns_qn);
        if (f) {
            emit_resolved(ctx, f->qualified_name, "php_function_namespaced", 0.95f);
            return;
        }
    }

    /* Look for any free function with this short_name. Prefer the one in the
     * file's project (i.e. with the longest module-prefix overlap). */
    const CBMRegisteredFunc *best = NULL;
    int best_score = -1;
    for (int i = 0; ctx->registry && i < ctx->registry->func_count; i++) {
        const CBMRegisteredFunc *cand = &ctx->registry->funcs[i];
        if (cand->receiver_type)
            continue;
        if (!cand->short_name || strcmp(cand->short_name, name) != 0)
            continue;
        int score = 0;
        if (cand->qualified_name && ctx->module_qn) {
            const char *m = ctx->module_qn;
            const char *q = cand->qualified_name;
            while (*m && *q && *m == *q) {
                if (*m == '.')
                    score++;
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
        emit_resolved(ctx, best->qualified_name, "php_function_global_fallback", 0.70f);
        return;
    }
    /* Unresolved — but DO NOT emit unresolved noise here; the unified
     * extractor already records the call edge. We only contribute when we
     * can correct or refine attribution. */
    (void)emit_unresolved;
}

static void resolve_member_call(PHPLSPContext *ctx, TSNode call) {
    TSNode obj = ts_node_child_by_field_name(call, "object", 6);
    TSNode name_node = ts_node_child_by_field_name(call, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *method_name = php_node_text(ctx, name_node);
    if (!method_name)
        return;
    if (ts_node_is_null(obj))
        return;

    const CBMType *recv = php_eval_expr_type(ctx, obj);
    if (!recv)
        return;
    const char *class_qn = NULL;
    if (recv->kind == CBM_TYPE_NAMED) {
        class_qn = recv->data.named.qualified_name;
    } else if (recv->kind == CBM_TYPE_TEMPLATE) {
        /* `Collection<User>` resolves methods on `Collection`. */
        class_qn = recv->data.template_type.template_name;
    }
    if (!class_qn)
        return; /* unknown receiver; defer */
    const CBMRegisteredFunc *f = php_lookup_method(ctx, class_qn, method_name);
    if (f) {
        const char *strategy = (f->receiver_type && strcmp(f->receiver_type, class_qn) == 0)
                                   ? "php_method_typed"
                                   : "php_method_inherited";
        emit_resolved(ctx, f->qualified_name, strategy, 0.95f);
        return;
    }
    /* Receiver known but method missing — magic __call? The call dispatches to
     * the class's __call handler, so resolve to <class>.__call (a real node).
     * The textual callee is the dynamic method name (`anything`), not `__call`,
     * so stash it in reason for the join (lsp_resolve.h, php_method_dynamic).
     * Emit above the join's confidence floor — dispatch to __call is certain. */
    if (class_has_magic_call(ctx, class_qn, false)) {
        emit_resolved_reason(ctx, cbm_arena_sprintf(ctx->arena, "%s.__call", class_qn),
                             "php_method_dynamic", 0.85f, method_name);
        return;
    }
    /* Receiver known but class not in registry (e.g. vendor type not indexed,
     * or method genuinely missing). Emit a synthetic resolved call pointing
     * at "<class_qn>.<method>" so the pipeline bridge can BLOCK the unified
     * extractor's likely-incorrect short-name fallback. The bridge filters
     * unknown targets and yields no edge — better than a wrong edge. */
    emit_resolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, method_name),
                  "php_method_typed_unindexed", 0.55f);
}

static void resolve_static_call(PHPLSPContext *ctx, TSNode call) {
    TSNode scope = ts_node_child_by_field_name(call, "scope", 5);
    TSNode name_node = ts_node_child_by_field_name(call, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *method_name = php_node_text(ctx, name_node);
    if (!method_name)
        return;

    const char *class_qn = NULL;
    const char *strategy = "php_static_resolved";
    if (!ts_node_is_null(scope)) {
        char *cls = php_node_text(ctx, scope);
        if (cls) {
            if (strcmp(cls, "self") == 0 || strcmp(cls, "static") == 0) {
                class_qn = ctx->enclosing_class_qn;
                strategy = "php_self_static";
            } else if (strcmp(cls, "parent") == 0) {
                class_qn = ctx->enclosing_parent_qn;
                strategy = "php_self_static";
            } else {
                class_qn = php_resolve_class_name(ctx, cls);
            }
        }
    }
    if (!class_qn)
        return;

    const CBMRegisteredFunc *f = php_lookup_method(ctx, class_qn, method_name);
    if (f) {
        emit_resolved(ctx, f->qualified_name, strategy, 0.95f);
        return;
    }
    /* Facade detection: class has __callStatic in its chain. */
    if (class_has_magic_call(ctx, class_qn, true)) {
        emit_resolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, method_name),
                      "php_dynamic_unresolved", 0.10f);
        return;
    }
    /* Class resolved (e.g., from a `use` clause), method unknown — emit
     * synthetic resolved call so the pipeline bridge can suppress the
     * unified extractor's name-fallback misroute. */
    emit_resolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, method_name),
                  "php_static_unindexed", 0.55f);
}

/* ── type narrowing ─────────────────────────────────────────────────
 *
 * Recognise narrowing predicates inside an `if` / ternary / match condition
 * and a few common library calls that act as runtime asserts. When the
 * predicate proves a positive constraint about a variable's type, push a
 * child scope with that variable rebound to the narrower type before
 * walking the body, then pop. For `assert(<predicate>);` we narrow into
 * the *current* scope from that point onward, since assert() acts as a
 * sequential refinement.
 *
 * Patterns understood:
 *   $x instanceof Foo                 → $x: Foo
 *   is_string($x)                     → $x: string  (and is_int/is_float/...)
 *   is_array($x)                      → $x: array
 *   is_object($x)                     → $x: object
 *   is_callable($x)                   → $x: callable
 *   $x !== null / $x != null          → $x: <existing> minus null  (we drop null)
 *   $x instanceof FooInterface        → $x: FooInterface (interface narrows too)
 *
 * Negative branches and `else` are not narrowed in Phase 4a; PHP's
 * negative-narrowing semantics (e.g. `else` branch of `if (is_string($x))`
 * narrows to "not-string") would require subtractive types we don't model.
 */

typedef struct {
    const char *var_name; /* "x" without leading $ */
    const CBMType *type;  /* the narrowed type */
} php_narrowing_t;

static const char *is_func_to_builtin(const char *name) {
    if (!name)
        return NULL;
    if (strcmp(name, "is_string") == 0)
        return "string";
    if (strcmp(name, "is_int") == 0 || strcmp(name, "is_integer") == 0 ||
        strcmp(name, "is_long") == 0)
        return "int";
    if (strcmp(name, "is_float") == 0 || strcmp(name, "is_double") == 0 ||
        strcmp(name, "is_real") == 0)
        return "float";
    if (strcmp(name, "is_bool") == 0)
        return "bool";
    if (strcmp(name, "is_array") == 0)
        return "array";
    if (strcmp(name, "is_object") == 0)
        return "object";
    if (strcmp(name, "is_callable") == 0)
        return "callable";
    if (strcmp(name, "is_iterable") == 0)
        return "iterable";
    if (strcmp(name, "is_numeric") == 0)
        return "float";
    if (strcmp(name, "is_countable") == 0)
        return "iterable";
    if (strcmp(name, "is_resource") == 0)
        return "resource";
    return NULL;
}

/* Parse a narrowing predicate. On success fill out and return true.
 *
 * For multi-conjunction (`P1 && P2`), we accumulate ALL narrowings into
 * the *last out slot, but the caller-side pattern in process_if_statement
 * only takes a single php_narrowing_t. To support full conjunction we
 * additionally call parse_narrowing_collect which writes into a small
 * array. parse_narrowing itself returns the FIRST narrowing it finds. */
static int parse_narrowing_collect(PHPLSPContext *ctx, TSNode cond, php_narrowing_t *out,
                                   int max_out);

static bool parse_narrowing(PHPLSPContext *ctx, TSNode cond, php_narrowing_t *out) {
    php_narrowing_t buf[1] = {{0}};
    int n = parse_narrowing_collect(ctx, cond, buf, 1);
    if (n > 0) {
        *out = buf[0];
        return true;
    }
    return false;
}

/* Recursive collector: adds narrowings from the predicate tree into out[]
 * up to max_out. Returns the number filled. */
static int parse_narrowing_one(PHPLSPContext *ctx, TSNode cond, php_narrowing_t *out);

static int parse_narrowing_collect(PHPLSPContext *ctx, TSNode cond, php_narrowing_t *out,
                                   int max_out) {
    if (ts_node_is_null(cond) || max_out <= 0)
        return 0;
    const char *kind = ts_node_type(cond);

    /* Parenthesized: unwrap. */
    if (strcmp(kind, "parenthesized_expression") == 0) {
        uint32_t nc = ts_node_child_count(cond);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(cond, i);
            if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                return parse_narrowing_collect(ctx, c, out, max_out);
            }
        }
        return 0;
    }

    /* Conjunction `A && B` (or `A and B`) → recurse into both sides. */
    if (strcmp(kind, "binary_expression") == 0) {
        TSNode op = ts_node_child_by_field_name(cond, "operator", 8);
        char *opt = !ts_node_is_null(op) ? php_node_text(ctx, op) : NULL;
        if (opt && (strcmp(opt, "&&") == 0 || strcmp(opt, "and") == 0)) {
            TSNode left = ts_node_child_by_field_name(cond, "left", 4);
            TSNode right = ts_node_child_by_field_name(cond, "right", 5);
            int got = parse_narrowing_collect(ctx, left, out, max_out);
            got += parse_narrowing_collect(ctx, right, out + got, max_out - got);
            return got;
        }
    }

    /* Single-predicate path. */
    php_narrowing_t one = {0};
    if (parse_narrowing_one(ctx, cond, &one)) {
        out[0] = one;
        return 1;
    }
    return 0;
}

static int parse_narrowing_one(PHPLSPContext *ctx, TSNode cond, php_narrowing_t *out) {
    if (ts_node_is_null(cond))
        return 0;
    const char *kind = ts_node_type(cond);
    if (strcmp(kind, "parenthesized_expression") == 0) {
        uint32_t nc = ts_node_child_count(cond);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(cond, i);
            if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                return parse_narrowing_one(ctx, c, out);
            }
        }
        return 0;
    }

    /* `$x instanceof Foo` — emitted as binary_expression with operator
     * "instanceof" in tree-sitter-php. */
    if (strcmp(kind, "binary_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(cond, "left", 4);
        TSNode op = ts_node_child_by_field_name(cond, "operator", 8);
        TSNode right = ts_node_child_by_field_name(cond, "right", 5);
        if (ts_node_is_null(left) || ts_node_is_null(right))
            return false;
        char *opt = !ts_node_is_null(op) ? php_node_text(ctx, op) : NULL;
        if (opt && strcmp(opt, "instanceof") == 0) {
            if (!node_is(left, "variable_name"))
                return false;
            char *vt = php_node_text(ctx, left);
            if (!vt)
                return false;
            const char *vname = (vt[0] == '$') ? vt + 1 : vt;
            char *rt = php_node_text(ctx, right);
            if (!rt)
                return false;
            const char *resolved = php_resolve_class_name(ctx, rt);
            if (!resolved)
                return false;
            out->var_name = cbm_arena_strdup(ctx->arena, vname);
            out->type = cbm_type_named(ctx->arena, resolved);
            return true;
        }
        /* `$x !== null` / `$x != null` / `null !== $x` — narrow to "non-null":
         * we keep the existing scope type, so this is a no-op for type
         * purposes but suppresses null branches downstream. We don't
         * subtract nullable so just return false here. */
    }

    /* `is_string($x)` / `is_int($x)` / `array_key_exists(...)` /
     * `method_exists($x, 'foo')` / `is_a($x, Foo::class)` / ... */
    if (strcmp(kind, "function_call_expression") == 0) {
        TSNode fn = ts_node_child_by_field_name(cond, "function", 8);
        TSNode args = ts_node_child_by_field_name(cond, "arguments", 9);
        if (ts_node_is_null(fn))
            return false;
        char *name = php_node_text(ctx, fn);
        if (!name)
            return false;
        /* `is_a($x, Foo::class)` and `is_a($x, 'Foo')` narrow $x to Foo. */
        if (strcmp(name, "is_a") == 0 && !ts_node_is_null(args)) {
            uint32_t anc = ts_node_child_count(args);
            TSNode v;
            memset(&v, 0, sizeof(v));
            TSNode classref;
            memset(&classref, 0, sizeof(classref));
            int seen = 0;
            for (uint32_t i = 0; i < anc; i++) {
                TSNode a = ts_node_child(args, i);
                if (ts_node_is_null(a) || !ts_node_is_named(a))
                    continue;
                TSNode inner = a;
                if (strcmp(ts_node_type(a), "argument") == 0) {
                    uint32_t bnc = ts_node_child_count(a);
                    for (uint32_t j = 0; j < bnc; j++) {
                        TSNode b = ts_node_child(a, j);
                        if (!ts_node_is_null(b) && ts_node_is_named(b)) {
                            inner = b;
                            break;
                        }
                    }
                }
                if (seen == 0)
                    v = inner;
                else if (seen == 1)
                    classref = inner;
                seen++;
            }
            if (!ts_node_is_null(v) && !ts_node_is_null(classref) && node_is(v, "variable_name")) {
                char *vt = php_node_text(ctx, v);
                if (vt) {
                    const char *vname = (vt[0] == '$') ? vt + 1 : vt;
                    char *cls_text = php_node_text(ctx, classref);
                    if (cls_text) {
                        /* Strip `::class`, surrounding quotes. */
                        size_t cl = strlen(cls_text);
                        if (cl >= 8 && strcmp(cls_text + cl - 7, "::class") == 0) {
                            cls_text[cl - 7] = '\0';
                        } else if (cl >= 2 && (cls_text[0] == '"' || cls_text[0] == '\'')) {
                            cls_text[cl - 1] = '\0';
                            cls_text++;
                        }
                        const char *resolved = php_resolve_class_name(ctx, cls_text);
                        if (resolved) {
                            out->var_name = cbm_arena_strdup(ctx->arena, vname);
                            out->type = cbm_type_named(ctx->arena, resolved);
                            return true;
                        }
                    }
                }
            }
        }
        const char *builtin = is_func_to_builtin(name);
        if (!builtin)
            return false;
        /* First positional argument should be a variable_name. */
        if (ts_node_is_null(args))
            return false;
        uint32_t anc = ts_node_child_count(args);
        for (uint32_t i = 0; i < anc; i++) {
            TSNode a = ts_node_child(args, i);
            if (ts_node_is_null(a) || !ts_node_is_named(a))
                continue;
            const char *ak = ts_node_type(a);
            /* `argument` wrapper has variable_name child. */
            TSNode v = a;
            if (strcmp(ak, "argument") == 0) {
                uint32_t bnc = ts_node_child_count(a);
                for (uint32_t j = 0; j < bnc; j++) {
                    TSNode b = ts_node_child(a, j);
                    if (!ts_node_is_null(b) && ts_node_is_named(b) && node_is(b, "variable_name")) {
                        v = b;
                        break;
                    }
                }
            }
            if (!node_is(v, "variable_name"))
                return false;
            char *vt = php_node_text(ctx, v);
            if (!vt)
                return false;
            const char *vname = (vt[0] == '$') ? vt + 1 : vt;
            out->var_name = cbm_arena_strdup(ctx->arena, vname);
            out->type = cbm_type_builtin(ctx->arena, builtin);
            return true;
        }
    }
    return false;
}

/* `assert(<predicate>)` narrows the current scope sequentially. */
static void apply_assert_narrowing(PHPLSPContext *ctx, TSNode call) {
    TSNode fn = ts_node_child_by_field_name(call, "function", 8);
    if (ts_node_is_null(fn))
        return;
    char *name = php_node_text(ctx, fn);
    if (!name || strcmp(name, "assert") != 0)
        return;
    TSNode args = ts_node_child_by_field_name(call, "arguments", 9);
    if (ts_node_is_null(args))
        return;
    uint32_t anc = ts_node_child_count(args);
    for (uint32_t i = 0; i < anc; i++) {
        TSNode a = ts_node_child(args, i);
        if (ts_node_is_null(a) || !ts_node_is_named(a))
            continue;
        TSNode predicate = a;
        if (strcmp(ts_node_type(a), "argument") == 0) {
            uint32_t bnc = ts_node_child_count(a);
            for (uint32_t j = 0; j < bnc; j++) {
                TSNode b = ts_node_child(a, j);
                if (!ts_node_is_null(b) && ts_node_is_named(b)) {
                    predicate = b;
                    break;
                }
            }
        }
        php_narrowing_t nw = {0};
        if (parse_narrowing(ctx, predicate, &nw) && nw.var_name && nw.type) {
            cbm_scope_bind(ctx->current_scope, nw.var_name, nw.type);
        }
        return;
    }
}

/* Walk a body subtree under a narrowed scope (with up to N bindings), then
 * restore. */
static void walk_with_narrowings(PHPLSPContext *ctx, TSNode body, const php_narrowing_t *nws,
                                 int nw_count) {
    if (ts_node_is_null(body))
        return;
    if (!nws || nw_count <= 0) {
        php_resolve_calls_in_node(ctx, body);
        return;
    }
    CBMScope *saved = ctx->current_scope;
    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    for (int i = 0; i < nw_count; i++) {
        if (nws[i].var_name && nws[i].type) {
            cbm_scope_bind(ctx->current_scope, nws[i].var_name, nws[i].type);
        }
    }
    php_resolve_calls_in_node(ctx, body);
    ctx->current_scope = saved;
}

/* Backwards-compat single-binding wrapper. */
static void walk_with_narrowing(PHPLSPContext *ctx, TSNode body, const php_narrowing_t *nw) {
    walk_with_narrowings(ctx, body, nw, nw ? 1 : 0);
}

/* Detect whether a statement node is a "leaves the function" terminator:
 *   return;  return $x;  throw ...;  exit;  die;  continue (in a loop);
 *   break (in a loop). For our purposes any of these means the rest of the
 * enclosing function does NOT execute the inner narrowed-out branch, so we
 * can apply the negation of the inner branch as sequential narrowing. */
static bool stmt_is_terminator(TSNode stmt) {
    if (ts_node_is_null(stmt))
        return false;
    const char *k = ts_node_type(stmt);
    if (strcmp(k, "return_statement") == 0)
        return true;
    if (strcmp(k, "compound_statement") == 0) {
        /* Single-child compound that's a terminator. */
        uint32_t nc = ts_node_child_count(stmt);
        TSNode last;
        memset(&last, 0, sizeof(last));
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(stmt, i);
            if (!ts_node_is_null(c) && ts_node_is_named(c))
                last = c;
        }
        if (!ts_node_is_null(last))
            return stmt_is_terminator(last);
    }
    if (strcmp(k, "expression_statement") == 0) {
        uint32_t nc = ts_node_child_count(stmt);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(stmt, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "throw_expression") == 0)
                return true;
            if (strcmp(ck, "exit_statement") == 0)
                return true;
            /* exit() / die() */
            if (strcmp(ck, "function_call_expression") == 0) {
                TSNode fn = ts_node_child_by_field_name(c, "function", 8);
                if (!ts_node_is_null(fn)) {
                    /* Just take the raw text and check for exit/die — even
                     * with namespace prefixes these are global calls. */
                    /* Conservative: don't claim terminator on function calls. */
                }
            }
        }
    }
    if (strcmp(k, "throw_statement") == 0)
        return true;
    if (strcmp(k, "exit_statement") == 0)
        return true;
    return false;
}

/* Walk an `if_statement` (or `else_if_clause`) honoring narrowing.
 *
 * Tree-sitter-php's `if_statement` emits children as: an optional `if`
 * keyword, a parenthesized_expression (condition), and one or more
 * statement-shaped bodies (the if-body, optionally followed by
 * else_if_clause / else_clause). We narrow on the condition then walk
 * EVERY non-condition named child as the body — only the first
 * statement after the condition is the if-body and gets the narrowed
 * scope; else-branches walk without narrowing.
 *
 * Negative-narrowing pattern (Phase 4l):
 *   if (!$x instanceof Foo) { return; }
 *   $x->method();   // here $x is narrowed to Foo
 * If the if-body is a terminator AND the condition is a logical NOT of a
 * narrowable predicate, apply the (positive) narrowing into the CURRENT
 * scope sequentially for the remainder of the enclosing function.
 */
static void process_if_statement(PHPLSPContext *ctx, TSNode node) {
    /* Find condition: first parenthesized_expression child. */
    TSNode cond;
    memset(&cond, 0, sizeof(cond));
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        if (strcmp(ts_node_type(c), "parenthesized_expression") == 0) {
            cond = c;
            break;
        }
    }
    /* Collect all conjunctive narrowings: `$x instanceof Foo && is_int($y)`
     * narrows BOTH $x and $y in the if-body. */
    php_narrowing_t nws[8] = {{0}};
    int nw_count = ts_node_is_null(cond) ? 0 : parse_narrowing_collect(ctx, cond, nws, 8);
    bool has_nw = nw_count > 0;

    /* Detect negative narrowing: condition is `!P` and P is a narrowing
     * predicate. We unwrap one level of unary `!`. */
    php_narrowing_t neg_nw = {0};
    bool has_neg_nw = false;
    if (!ts_node_is_null(cond)) {
        TSNode inner;
        memset(&inner, 0, sizeof(inner));
        /* Unwrap parenthesized_expression. */
        TSNode probe = cond;
        for (int loop = 0; loop < 4; loop++) {
            if (strcmp(ts_node_type(probe), "parenthesized_expression") == 0) {
                uint32_t pnc = ts_node_child_count(probe);
                TSNode next;
                memset(&next, 0, sizeof(next));
                for (uint32_t i = 0; i < pnc; i++) {
                    TSNode c = ts_node_child(probe, i);
                    if (!ts_node_is_null(c) && ts_node_is_named(c)) {
                        next = c;
                        break;
                    }
                }
                if (ts_node_is_null(next))
                    break;
                probe = next;
            } else {
                break;
            }
        }
        if (strcmp(ts_node_type(probe), "unary_op_expression") == 0) {
            /* Find the operator child; if it's `!`, parse the inner expr. */
            TSNode op = ts_node_child_by_field_name(probe, "operator", 8);
            char *opt = !ts_node_is_null(op) ? php_node_text(ctx, op) : NULL;
            if (opt && strcmp(opt, "!") == 0) {
                TSNode operand = ts_node_child_by_field_name(probe, "operand", 7);
                if (ts_node_is_null(operand)) {
                    uint32_t unc = ts_node_child_count(probe);
                    for (uint32_t i = 0; i < unc; i++) {
                        TSNode c = ts_node_child(probe, i);
                        if (!ts_node_is_null(c) && ts_node_is_named(c) &&
                            strcmp(ts_node_type(c), "unary_op_expression") != 0) {
                            operand = c;
                            break;
                        }
                    }
                }
                if (!ts_node_is_null(operand)) {
                    has_neg_nw = parse_narrowing(ctx, operand, &neg_nw);
                }
            }
        }
    }

    /* Walk children. The first NON-condition, NON-else-clause named child
     * is the if-body; narrowed scope applies there. Subsequent
     * else_if_clause / else_clause walk without narrowing. */
    bool walked_body = false;
    bool past_cond = false;
    bool body_is_terminator = false;
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (!past_cond) {
            if (strcmp(k, "parenthesized_expression") == 0) {
                past_cond = true;
                /* Walk the condition expression so calls inside it (e.g.
                 * `$it->valid()` in `if ($it->valid())`) get resolved. */
                php_resolve_calls_in_node(ctx, c);
                continue;
            }
            /* Non-condition child encountered before the condition: keep walking. */
            php_resolve_calls_in_node(ctx, c);
            continue;
        }
        if (!walked_body) {
            body_is_terminator = stmt_is_terminator(c);
            walk_with_narrowings(ctx, c, has_nw ? nws : NULL, nw_count);
            walked_body = true;
        } else {
            /* else / elseif / colon_block */
            php_resolve_calls_in_node(ctx, c);
        }
    }

    /* Negative-narrowing apply: if the if-body unconditionally exits AND
     * the condition was `!P` for narrowable P, then for the rest of the
     * enclosing scope, P holds. */
    if (body_is_terminator && has_neg_nw && neg_nw.var_name && neg_nw.type) {
        cbm_scope_bind(ctx->current_scope, neg_nw.var_name, neg_nw.type);
    }
}

/* Walk a subtree, binding scope and resolving calls. */
static void php_resolve_calls_in_node_inner(PHPLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    /* Stop descending into nested function-likes — they are processed by
     * process_function_like with their own scope. */
    if (strcmp(kind, "function_definition") == 0 ||
        strcmp(kind, "function_static_declaration") == 0 ||
        strcmp(kind, "method_declaration") == 0 || strcmp(kind, "anonymous_function") == 0 ||
        strcmp(kind, "arrow_function") == 0) {
        process_function_like(ctx, node);
        return;
    }

    /* Statement-level scope-binding observers. */
    if (strcmp(kind, "assignment_expression") == 0) {
        process_assignment(ctx, node);
    } else if (strcmp(kind, "foreach_statement") == 0) {
        process_foreach(ctx, node);
    } else if (strcmp(kind, "catch_clause") == 0) {
        process_catch(ctx, node);
    } else if (strcmp(kind, "comment") == 0) {
        char *raw = php_node_text(ctx, node);
        if (raw && raw[0] == '/' && raw[1] == '*' && raw[2] == '*') {
            char *cleaned = phpdoc_clean(ctx->arena, raw);
            bind_phpdoc_var(ctx, cleaned);
        }
    } else if (strcmp(kind, "if_statement") == 0) {
        process_if_statement(ctx, node);
        return; /* process_if_statement already walked body under narrowing */
    }

    /* Call-resolution dispatch. */
    if (strcmp(kind, "function_call_expression") == 0) {
        resolve_function_call(ctx, node);
        /* assert(...) in current scope narrows sequentially. */
        apply_assert_narrowing(ctx, node);
    } else if (strcmp(kind, "member_call_expression") == 0 ||
               strcmp(kind, "nullsafe_member_call_expression") == 0) {
        resolve_member_call(ctx, node);
        /* Closure binding via $f->bindTo($obj). When $f is an inline
         * closure literal AND the bindTo call's first arg is a typed
         * variable, walk the closure body with $this rebound. We only
         * support this for the inline-literal pattern; tracking
         * Closure-as-value across assignments is deferred (closure
         * value-typing is Phase 6+ infrastructure). */
        TSNode obj = ts_node_child_by_field_name(node, "object", 6);
        TSNode mname = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(obj) && !ts_node_is_null(mname)) {
            char *mn = php_node_text(ctx, mname);
            const char *ok = ts_node_type(obj);
            if (mn && strcmp(mn, "bindTo") == 0 &&
                (strcmp(ok, "anonymous_function") == 0 || strcmp(ok, "arrow_function") == 0 ||
                 strcmp(ok, "parenthesized_expression") == 0)) {
                /* Resolve the second-arg's class. */
                TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
                if (!ts_node_is_null(args)) {
                    uint32_t anc = ts_node_child_count(args);
                    for (uint32_t i = 0; i < anc; i++) {
                        TSNode a = ts_node_child(args, i);
                        if (ts_node_is_null(a) || !ts_node_is_named(a))
                            continue;
                        TSNode v = a;
                        if (strcmp(ts_node_type(a), "argument") == 0) {
                            uint32_t bnc = ts_node_child_count(a);
                            for (uint32_t j = 0; j < bnc; j++) {
                                TSNode b = ts_node_child(a, j);
                                if (!ts_node_is_null(b) && ts_node_is_named(b)) {
                                    v = b;
                                    break;
                                }
                            }
                        }
                        const CBMType *t = php_eval_expr_type(ctx, v);
                        if (t && t->kind == CBM_TYPE_NAMED) {
                            const char *saved = ctx->enclosing_class_qn;
                            ctx->enclosing_class_qn = t->data.named.qualified_name;
                            php_resolve_calls_in_node(ctx, obj);
                            ctx->enclosing_class_qn = saved;
                        }
                        break;
                    }
                }
            }
        }
    } else if (strcmp(kind, "scoped_call_expression") == 0) {
        resolve_static_call(ctx, node);
        /* Closure binding via Closure::bind($f, $obj). Same shape as
         * bindTo: rebind $this when the first arg is a closure literal
         * and the second arg has a known class. */
        TSNode scope = ts_node_child_by_field_name(node, "scope", 5);
        TSNode mname2 = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(scope) && !ts_node_is_null(mname2)) {
            char *cls = php_node_text(ctx, scope);
            char *mn = php_node_text(ctx, mname2);
            bool is_closure_bind = mn && strcmp(mn, "bind") == 0 && cls &&
                                   (strcmp(cls, "Closure") == 0 || strcmp(cls, "\\Closure") == 0);
            if (is_closure_bind) {
                TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
                if (!ts_node_is_null(args)) {
                    TSNode closure_arg;
                    memset(&closure_arg, 0, sizeof(closure_arg));
                    TSNode obj_arg;
                    memset(&obj_arg, 0, sizeof(obj_arg));
                    int seen = 0;
                    uint32_t anc = ts_node_child_count(args);
                    for (uint32_t i = 0; i < anc; i++) {
                        TSNode a = ts_node_child(args, i);
                        if (ts_node_is_null(a) || !ts_node_is_named(a))
                            continue;
                        TSNode inner = a;
                        if (strcmp(ts_node_type(a), "argument") == 0) {
                            uint32_t bnc = ts_node_child_count(a);
                            for (uint32_t j = 0; j < bnc; j++) {
                                TSNode b = ts_node_child(a, j);
                                if (!ts_node_is_null(b) && ts_node_is_named(b)) {
                                    inner = b;
                                    break;
                                }
                            }
                        }
                        if (seen == 0)
                            closure_arg = inner;
                        else if (seen == 1)
                            obj_arg = inner;
                        seen++;
                    }
                    if (!ts_node_is_null(closure_arg) && !ts_node_is_null(obj_arg)) {
                        const char *ck = ts_node_type(closure_arg);
                        if (strcmp(ck, "anonymous_function") == 0 ||
                            strcmp(ck, "arrow_function") == 0) {
                            const CBMType *t = php_eval_expr_type(ctx, obj_arg);
                            if (t && t->kind == CBM_TYPE_NAMED) {
                                const char *saved = ctx->enclosing_class_qn;
                                ctx->enclosing_class_qn = t->data.named.qualified_name;
                                php_resolve_calls_in_node(ctx, closure_arg);
                                ctx->enclosing_class_qn = saved;
                            }
                        }
                    }
                }
            }
        }
    }

    /* Recurse via a cursor (O(n)); ts_node_child(node,i) is O(i) → O(n²) on a wide node. */
    {
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                php_resolve_calls_in_node(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }
}

/* ── function-like processing ───────────────────────────────────── */

static void bind_typed_parameters(PHPLSPContext *ctx, TSNode params) {
    if (ts_node_is_null(params))
        return;
    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_child(params, i);
        if (ts_node_is_null(p) || !ts_node_is_named(p))
            continue;
        const char *pk = ts_node_type(p);
        if (strcmp(pk, "simple_parameter") != 0 && strcmp(pk, "variadic_parameter") != 0 &&
            strcmp(pk, "property_promotion_parameter") != 0) {
            continue;
        }
        TSNode tnode = ts_node_child_by_field_name(p, "type", 4);
        const CBMType *ptype = cbm_type_unknown();
        if (!ts_node_is_null(tnode))
            ptype = php_parse_type_node(ctx, tnode);
        TSNode name_node = ts_node_child_by_field_name(p, "name", 4);
        if (ts_node_is_null(name_node)) {
            /* fallback: first variable_name child */
            uint32_t pnc = ts_node_child_count(p);
            for (uint32_t j = 0; j < pnc; j++) {
                TSNode c = ts_node_child(p, j);
                if (!ts_node_is_null(c) && strcmp(ts_node_type(c), "variable_name") == 0) {
                    name_node = c;
                    break;
                }
            }
        }
        if (ts_node_is_null(name_node))
            continue;
        char *vt = php_node_text(ctx, name_node);
        if (!vt)
            continue;
        const char *name = (vt[0] == '$') ? vt + 1 : vt;
        /* Variadic parameter — `string ...$args` has type array<string>;
         * we don't model array<T> in Phase 4h, so just bind as `array`. */
        if (strcmp(pk, "variadic_parameter") == 0) {
            ptype = cbm_type_builtin(ctx->arena, "array");
        }
        cbm_scope_bind(ctx->current_scope, name, ptype);
    }
}

static void process_function_like(PHPLSPContext *ctx, TSNode node) {
    const char *kind = ts_node_type(node);
    bool is_method = (strcmp(kind, "method_declaration") == 0);
    bool is_named = is_method || (strcmp(kind, "function_definition") == 0) ||
                    (strcmp(kind, "function_static_declaration") == 0);

    /* Save context. */
    CBMScope *saved_scope = ctx->current_scope;
    const char *saved_func = ctx->enclosing_func_qn;

    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

    /* Determine func QN. Mirror what the unified extractor produces: classes
     * and free functions are namespaced by file module_qn, NOT the PHP
     * namespace declaration (the unified extractor ignores `namespace`).
     * Method QN = enclosing_class_qn + "." + method_name. Free function QN =
     * module_qn + "." + name. */
    if (is_named) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(name_node)) {
            char *fname = php_node_text(ctx, name_node);
            if (fname) {
                if (is_method && ctx->enclosing_class_qn) {
                    ctx->enclosing_func_qn =
                        cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->enclosing_class_qn, fname);
                } else if (ctx->module_qn) {
                    ctx->enclosing_func_qn =
                        cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, fname);
                } else {
                    ctx->enclosing_func_qn = cbm_arena_strdup(ctx->arena, fname);
                }
            }
        }
    }

    /* Parameters. */
    TSNode params = ts_node_child_by_field_name(node, "parameters", 10);
    bind_typed_parameters(ctx, params);

    /* Closure `use ($a, $b)` clause: copy captured-variable types from the
     * parent scope into the closure scope. tree-sitter-php emits an
     * `anonymous_function_use_clause` child of `anonymous_function`. */
    if (strcmp(kind, "anonymous_function") == 0) {
        uint32_t fnc = ts_node_child_count(node);
        for (uint32_t i = 0; i < fnc; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            if (strcmp(ts_node_type(c), "anonymous_function_use_clause") != 0)
                continue;
            uint32_t unc = ts_node_child_count(c);
            for (uint32_t j = 0; j < unc; j++) {
                TSNode v = ts_node_child(c, j);
                if (ts_node_is_null(v) || !ts_node_is_named(v))
                    continue;
                if (strcmp(ts_node_type(v), "variable_name") != 0)
                    continue;
                char *vt = php_node_text(ctx, v);
                if (!vt)
                    continue;
                const char *vname = (vt[0] == '$') ? vt + 1 : vt;
                /* Look up in saved parent scope (captured at this point). */
                const CBMType *captured = cbm_scope_lookup(saved_scope, vname);
                if (captured) {
                    cbm_scope_bind(ctx->current_scope, vname, captured);
                }
            }
        }
    }

    /* PHPDoc @param overrides. */
    if (is_named) {
        char *doc = fetch_leading_phpdoc(ctx, node);
        if (doc)
            parse_phpdoc_for_params(ctx, doc, params);
    }

    /* Walk body. */
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (ts_node_is_null(body)) {
        /* Arrow function body is in field "body" too, but it's an expression.
         * Try generic recursion as a fallback. */
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(node, i);
            if (!ts_node_is_null(c))
                php_resolve_calls_in_node(ctx, c);
        }
    } else {
        php_resolve_calls_in_node(ctx, body);
    }

    /* Restore. */
    ctx->current_scope = saved_scope;
    ctx->enclosing_func_qn = saved_func;
}

/* Find first base class (extends X) in a class_declaration, resolved. */
static const char *find_extends_qn(PHPLSPContext *ctx, TSNode class_node) {
    uint32_t nc = ts_node_child_count(class_node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(class_node, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        if (strcmp(ts_node_type(c), "base_clause") != 0)
            continue;
        uint32_t bnc = ts_node_child_count(c);
        for (uint32_t j = 0; j < bnc; j++) {
            TSNode bc = ts_node_child(c, j);
            if (ts_node_is_null(bc) || !ts_node_is_named(bc))
                continue;
            const char *bk = ts_node_type(bc);
            if (strcmp(bk, "name") == 0 || strcmp(bk, "qualified_name") == 0) {
                char *t = php_node_text(ctx, bc);
                if (t)
                    return php_resolve_class_name(ctx, t);
            }
        }
    }
    return NULL;
}

static void process_class_decl(PHPLSPContext *ctx, TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *cname = php_node_text(ctx, name_node);
    if (!cname)
        return;

    const char *saved_class = ctx->enclosing_class_qn;
    const char *saved_parent = ctx->enclosing_parent_qn;

    /* enclosing_class_qn must match what the unified extractor records,
     * which uses module_qn (file-path-based), NOT the PHP `namespace`
     * declaration. */
    if (ctx->module_qn) {
        ctx->enclosing_class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cname);
    } else {
        ctx->enclosing_class_qn = cbm_arena_strdup(ctx->arena, cname);
    }
    ctx->enclosing_parent_qn = find_extends_qn(ctx, node);

    /* Walk class body — pick out method_declaration nodes. */
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (ts_node_is_null(body))
        body = child_named(node, "declaration_list");
    if (!ts_node_is_null(body)) {
        uint32_t nc = ts_node_child_count(body);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(body, i);
            if (ts_node_is_null(c))
                continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "method_declaration") == 0) {
                process_function_like(ctx, c);
            }
        }
    }

    ctx->enclosing_class_qn = saved_class;
    ctx->enclosing_parent_qn = saved_parent;
}

/* ── top-level pass ─────────────────────────────────────────────── */

/* Collect a single namespace_use_clause's local→target mapping and add.
 *
 * tree-sitter-php emits `use Vendor\X as Y;` either as a use_clause with
 * a `namespace_aliasing_clause` child wrapping the alias, or as a bare
 * sequence: qualified_name + literal `as` token + name. We accept both
 * shapes by treating the *second* name-like child as the alias when no
 * aliasing clause is present. */
static void collect_use_clause(PHPLSPContext *ctx, TSNode use_clause, int kind) {
    TSNode name_node;
    memset(&name_node, 0, sizeof(name_node));
    TSNode alias_node;
    memset(&alias_node, 0, sizeof(alias_node));
    int name_seen = 0;
    uint32_t nc = ts_node_child_count(use_clause);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(use_clause, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "qualified_name") == 0 || strcmp(k, "name") == 0) {
            if (name_seen == 0) {
                name_node = c;
            } else if (name_seen == 1 && ts_node_is_null(alias_node)) {
                /* Second name-shaped child = `as Alias`. */
                alias_node = c;
            }
            name_seen++;
        } else if (strcmp(k, "namespace_aliasing_clause") == 0) {
            uint32_t ac = ts_node_child_count(c);
            for (uint32_t j = 0; j < ac; j++) {
                TSNode ach = ts_node_child(c, j);
                if (!ts_node_is_null(ach) && ts_node_is_named(ach) &&
                    strcmp(ts_node_type(ach), "name") == 0) {
                    alias_node = ach;
                    break;
                }
            }
        }
    }
    if (ts_node_is_null(name_node))
        return;
    char *full = php_node_text(ctx, name_node);
    if (!full)
        return;
    char *dotted = php_ns_to_dot(ctx->arena, full);
    const char *local = NULL;
    if (!ts_node_is_null(alias_node)) {
        local = php_node_text(ctx, alias_node);
    }
    if (!local) {
        /* Default: last segment. */
        local = php_short_name(dotted);
    }
    php_lsp_add_use(ctx, local, dotted, kind);
}

static void collect_use_declaration(PHPLSPContext *ctx, TSNode use_decl) {
    /* Determine kind: function / const / class (default). */
    int kind = CBM_PHP_USE_CLASS;
    uint32_t nc = ts_node_child_count(use_decl);
    /* tree-sitter-php emits `function` or `const` keyword children. */
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(use_decl, i);
        if (ts_node_is_null(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "function") == 0)
            kind = CBM_PHP_USE_FUNCTION;
        if (strcmp(k, "const") == 0)
            kind = CBM_PHP_USE_CONST;
    }
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(use_decl, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "namespace_use_clause") == 0) {
            collect_use_clause(ctx, c, kind);
        } else if (strcmp(k, "namespace_use_group") == 0) {
            /* use App\\{Foo, Bar as B}; — recurse into clauses inside. */
            uint32_t gc = ts_node_child_count(c);
            for (uint32_t j = 0; j < gc; j++) {
                TSNode gch = ts_node_child(c, j);
                if (!ts_node_is_null(gch) && ts_node_is_named(gch) &&
                    strcmp(ts_node_type(gch), "namespace_use_clause") == 0) {
                    collect_use_clause(ctx, gch, kind);
                }
            }
        }
    }
}

static void set_namespace_from_decl(PHPLSPContext *ctx, TSNode ns_decl) {
    uint32_t nc = ts_node_child_count(ns_decl);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(ns_decl, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "namespace_name") == 0) {
            char *t = php_node_text(ctx, c);
            if (t) {
                ctx->current_namespace_qn = php_ns_to_dot(ctx->arena, t);
                return;
            }
        }
    }
}

void php_lsp_process_file(PHPLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;

    /* Pass 1: namespace + use declarations. */
    // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
    uint32_t kn = 0;
    TSNode *kids = cbm_lsp_collect_children(ctx->arena, root, &kn);
    for (uint32_t i = 0; i < kn; i++) {
        TSNode c = kids[i];
        const char *k = ts_node_type(c);
        if (strcmp(k, "namespace_definition") == 0) {
            set_namespace_from_decl(ctx, c);
        } else if (strcmp(k, "namespace_use_declaration") == 0) {
            collect_use_declaration(ctx, c);
        }
    }

    /* Pass 2: process classes and top-level functions. */
    for (uint32_t i = 0; i < kn; i++) {
        TSNode c = kids[i];
        const char *k = ts_node_type(c);
        if (strcmp(k, "class_declaration") == 0 || strcmp(k, "trait_declaration") == 0 ||
            strcmp(k, "interface_declaration") == 0 || strcmp(k, "enum_declaration") == 0) {
            process_class_decl(ctx, c);
        } else if (strcmp(k, "function_definition") == 0 ||
                   strcmp(k, "function_static_declaration") == 0) {
            process_function_like(ctx, c);
        } else if (strcmp(k, "namespace_definition") == 0) {
            /* Body-style: namespace App { ... }. Set namespace + collect
             * use clauses inside the block, then walk children. */
            set_namespace_from_decl(ctx, c);
            /* Collect any namespace_use_declaration inside the block. */
            uint32_t nc2 = ts_node_child_count(c);
            for (uint32_t i2 = 0; i2 < nc2; i2++) {
                TSNode ch = ts_node_child(c, i2);
                if (ts_node_is_null(ch) || !ts_node_is_named(ch))
                    continue;
                if (strcmp(ts_node_type(ch), "namespace_use_declaration") == 0) {
                    collect_use_declaration(ctx, ch);
                } else if (strcmp(ts_node_type(ch), "declaration_list") == 0) {
                    /* use clauses can also appear inside the declaration_list. */
                    uint32_t ncc = ts_node_child_count(ch);
                    for (uint32_t k3 = 0; k3 < ncc; k3++) {
                        TSNode cch = ts_node_child(ch, k3);
                        if (ts_node_is_null(cch) || !ts_node_is_named(cch))
                            continue;
                        if (strcmp(ts_node_type(cch), "namespace_use_declaration") == 0) {
                            collect_use_declaration(ctx, cch);
                        }
                    }
                }
            }
            /* The body of `namespace App { ... }` may be a `declaration_list`
             * or a `compound_statement` depending on tree-sitter-php
             * version. Iterate ALL named children of the namespace and
             * dispatch any class/function/etc. seen at any depth. */
            for (uint32_t j = 0; j < nc2; j++) {
                TSNode bc = ts_node_child(c, j);
                if (ts_node_is_null(bc) || !ts_node_is_named(bc))
                    continue;
                const char *bk = ts_node_type(bc);
                if (strcmp(bk, "class_declaration") == 0 || strcmp(bk, "trait_declaration") == 0 ||
                    strcmp(bk, "interface_declaration") == 0 ||
                    strcmp(bk, "enum_declaration") == 0) {
                    process_class_decl(ctx, bc);
                } else if (strcmp(bk, "function_definition") == 0) {
                    process_function_like(ctx, bc);
                } else if (strcmp(bk, "declaration_list") == 0 ||
                           strcmp(bk, "compound_statement") == 0) {
                    /* Walk children of the body node. */
                    uint32_t bn = ts_node_child_count(bc);
                    for (uint32_t bi = 0; bi < bn; bi++) {
                        TSNode bcc = ts_node_child(bc, bi);
                        if (ts_node_is_null(bcc) || !ts_node_is_named(bcc))
                            continue;
                        const char *bbk = ts_node_type(bcc);
                        if (strcmp(bbk, "class_declaration") == 0 ||
                            strcmp(bbk, "trait_declaration") == 0 ||
                            strcmp(bbk, "interface_declaration") == 0 ||
                            strcmp(bbk, "enum_declaration") == 0) {
                            process_class_decl(ctx, bcc);
                        } else if (strcmp(bbk, "function_definition") == 0) {
                            process_function_like(ctx, bcc);
                        } else if (strcmp(bbk, "expression_statement") == 0 ||
                                   strcmp(bbk, "echo_statement") == 0 ||
                                   strcmp(bbk, "if_statement") == 0) {
                            php_resolve_calls_in_node(ctx, bcc);
                        }
                    }
                }
            }
        } else if (strcmp(k, "expression_statement") == 0 || strcmp(k, "if_statement") == 0 ||
                   strcmp(k, "echo_statement") == 0) {
            /* Top-level script code outside any function/class. Without an
             * enclosing function we cannot emit resolved calls, but we still
             * walk statements to populate scope for any subsequent function
             * processing in this file. */
            php_resolve_calls_in_node(ctx, c);
        }
    }
}

/* ── parse return-type text from CBMDefinition.return_type ──────── */

static const CBMType *parse_return_type_text(CBMArena *arena, const char *text,
                                             const char *module_qn,
                                             const char *current_namespace_qn,
                                             const char **use_local_names,
                                             const char **use_target_qns, int use_count) {
    if (!text || !*text)
        return cbm_type_unknown();
    /* Skip leading ":?" / "?". */
    while (*text == ':' || *text == ' ' || *text == '?')
        text++;
    if (php_is_builtin_type_name(text))
        return cbm_type_builtin(arena, text);

    /* If union/intersection, take leftmost non-null. */
    const char *bar = strchr(text, '|');
    const char *amp = strchr(text, '&');
    const char *cut = NULL;
    if (bar && (!amp || bar < amp))
        cut = bar;
    else if (amp)
        cut = amp;
    char *first;
    if (cut) {
        first = cbm_arena_strndup(arena, text, (size_t)(cut - text));
    } else {
        first = cbm_arena_strdup(arena, text);
    }
    if (!first)
        return cbm_type_unknown();
    /* trim. */
    while (*first == ' ' || *first == '?')
        first++;
    char *end = first + strlen(first);
    while (end > first && (end[-1] == ' '))
        end--;
    *end = '\0';
    if (!*first || strcmp(first, "null") == 0)
        return cbm_type_unknown();
    if (php_is_builtin_type_name(first))
        return cbm_type_builtin(arena, first);

    /* Resolve via use map. */
    if (first[0] == '\\') {
        return cbm_type_named(arena, php_ns_to_dot(arena, first + 1));
    }
    /* Find first segment for use lookup. */
    const char *bs = first;
    while (*bs && *bs != '\\')
        bs++;
    char *first_seg = (*bs) ? cbm_arena_strndup(arena, first, (size_t)(bs - first))
                            : cbm_arena_strdup(arena, first);
    for (int i = 0; i < use_count; i++) {
        if (strcmp(use_local_names[i], first_seg) == 0) {
            const char *target = use_target_qns[i];
            if (*bs) {
                return cbm_type_named(
                    arena, cbm_arena_sprintf(arena, "%s%s", target, php_ns_to_dot(arena, bs)));
            }
            return cbm_type_named(arena, target);
        }
    }
    /* Fallback: current namespace prefix. */
    if (current_namespace_qn && *current_namespace_qn) {
        return cbm_type_named(arena, cbm_arena_sprintf(arena, "%s.%s", current_namespace_qn,
                                                       php_ns_to_dot(arena, first)));
    }
    if (module_qn) {
        return cbm_type_named(
            arena, cbm_arena_sprintf(arena, "%s.%s", module_qn, php_ns_to_dot(arena, first)));
    }
    return cbm_type_named(arena, php_ns_to_dot(arena, first));
}

/* ── property type extraction ───────────────────────────────────────
 *
 * Walks the AST top-level for class/trait/interface bodies and registers
 * typed properties on the corresponding CBMRegisteredType.field_names /
 * field_types. Handles:
 *   - typed `public Foo $bar;` declarations
 *   - constructor property promotion (the `public Foo $bar` parameter form)
 *   - `$this->bar = $bar` constructor-body inference, where $bar is a
 *     typed parameter
 *
 * After this pass `eval_expr_type` for member_access_expression $x->bar
 * returns the property's CBMType when known. */

typedef struct {
    const char *class_qn;
    const char **field_names; /* arena-backed, NULL-terminated */
    const CBMType **field_types;
    int count;
    int cap;
} php_class_fields_t;

typedef struct {
    php_class_fields_t *items;
    int count;
    int cap;
} php_class_field_table_t;

static php_class_fields_t *fields_for(php_class_field_table_t *tab, const char *class_qn,
                                      CBMArena *arena) {
    for (int i = 0; i < tab->count; i++) {
        if (strcmp(tab->items[i].class_qn, class_qn) == 0)
            return &tab->items[i];
    }
    if (tab->count >= tab->cap) {
        int new_cap = tab->cap ? tab->cap * 2 : 16;
        php_class_fields_t *next =
            (php_class_fields_t *)cbm_arena_alloc(arena, (size_t)new_cap * sizeof(*next));
        if (!next)
            return NULL;
        for (int i = 0; i < tab->count; i++)
            next[i] = tab->items[i];
        tab->items = next;
        tab->cap = new_cap;
    }
    php_class_fields_t *f = &tab->items[tab->count++];
    memset(f, 0, sizeof(*f));
    f->class_qn = cbm_arena_strdup(arena, class_qn);
    return f;
}

static void add_field(php_class_fields_t *f, CBMArena *arena, const char *name,
                      const CBMType *type) {
    if (!f || !name || !type)
        return;
    /* dedup: keep first declaration; constructor inference doesn't override
     * an explicit declaration. */
    for (int i = 0; i < f->count; i++) {
        if (strcmp(f->field_names[i], name) == 0)
            return;
    }
    if (f->count >= f->cap) {
        int new_cap = f->cap ? f->cap * 2 : 8;
        const char **nn =
            (const char **)cbm_arena_alloc(arena, (size_t)(new_cap + 1) * sizeof(*nn));
        const CBMType **nt =
            (const CBMType **)cbm_arena_alloc(arena, (size_t)(new_cap + 1) * sizeof(*nt));
        if (!nn || !nt)
            return;
        for (int i = 0; i < f->count; i++) {
            nn[i] = f->field_names[i];
            nt[i] = f->field_types[i];
        }
        f->field_names = nn;
        f->field_types = nt;
        f->cap = new_cap;
    }
    f->field_names[f->count] = cbm_arena_strdup(arena, name);
    f->field_types[f->count] = type;
    f->count++;
    f->field_names[f->count] = NULL;
    f->field_types[f->count] = NULL;
}

/* Compute the dotted class QN for a given class_declaration node, matching
 * how the unified extractor names defs (module_qn-prefixed). */
static const char *class_qn_for_node(PHPLSPContext *ctx, TSNode class_node) {
    TSNode name_node = ts_node_child_by_field_name(class_node, "name", 4);
    if (ts_node_is_null(name_node))
        return NULL;
    char *cname = php_node_text(ctx, name_node);
    if (!cname)
        return NULL;
    if (ctx->module_qn) {
        return cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cname);
    }
    return cbm_arena_strdup(ctx->arena, cname);
}

static void extract_property_decl(PHPLSPContext *ctx, TSNode prop_decl, php_class_fields_t *out) {
    TSNode tnode = ts_node_child_by_field_name(prop_decl, "type", 4);
    const CBMType *ptype = NULL;
    if (!ts_node_is_null(tnode))
        ptype = php_parse_type_node(ctx, tnode);
    if (!ptype || ptype->kind == CBM_TYPE_UNKNOWN) {
        /* Tree-sitter-php may put the type as a named child without field. */
        uint32_t nc = ts_node_child_count(prop_decl);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(prop_decl, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "named_type") == 0 || strcmp(k, "primitive_type") == 0 ||
                strcmp(k, "union_type") == 0 || strcmp(k, "intersection_type") == 0 ||
                strcmp(k, "optional_type") == 0) {
                ptype = php_parse_type_node(ctx, c);
                break;
            }
        }
    }
    if (!ptype || ptype->kind == CBM_TYPE_UNKNOWN)
        return;

    /* Find the property_element children (each has variable_name + maybe default). */
    uint32_t nc = ts_node_child_count(prop_decl);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(prop_decl, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "property_element") == 0) {
            uint32_t bnc = ts_node_child_count(c);
            for (uint32_t j = 0; j < bnc; j++) {
                TSNode b = ts_node_child(c, j);
                if (!ts_node_is_null(b) && ts_node_is_named(b) &&
                    strcmp(ts_node_type(b), "variable_name") == 0) {
                    char *vt = php_node_text(ctx, b);
                    if (!vt)
                        continue;
                    const char *name = (vt[0] == '$') ? vt + 1 : vt;
                    add_field(out, ctx->arena, name, ptype);
                    break;
                }
            }
        } else if (strcmp(k, "variable_name") == 0) {
            char *vt = php_node_text(ctx, c);
            if (vt) {
                const char *name = (vt[0] == '$') ? vt + 1 : vt;
                add_field(out, ctx->arena, name, ptype);
            }
        }
    }
}

static void extract_promoted_param(PHPLSPContext *ctx, TSNode param, php_class_fields_t *out) {
    /* property_promotion_parameter has: visibility/readonly modifiers,
     * type, variable_name. */
    TSNode tnode = ts_node_child_by_field_name(param, "type", 4);
    const CBMType *ptype = NULL;
    if (!ts_node_is_null(tnode))
        ptype = php_parse_type_node(ctx, tnode);
    if (!ptype || ptype->kind == CBM_TYPE_UNKNOWN) {
        uint32_t nc = ts_node_child_count(param);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(param, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "named_type") == 0 || strcmp(k, "primitive_type") == 0 ||
                strcmp(k, "union_type") == 0 || strcmp(k, "intersection_type") == 0 ||
                strcmp(k, "optional_type") == 0) {
                ptype = php_parse_type_node(ctx, c);
                break;
            }
        }
    }
    if (!ptype || ptype->kind == CBM_TYPE_UNKNOWN)
        return;
    TSNode vname = ts_node_child_by_field_name(param, "name", 4);
    if (ts_node_is_null(vname)) {
        uint32_t nc = ts_node_child_count(param);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(param, i);
            if (!ts_node_is_null(c) && strcmp(ts_node_type(c), "variable_name") == 0) {
                vname = c;
                break;
            }
        }
    }
    if (ts_node_is_null(vname))
        return;
    char *vt = php_node_text(ctx, vname);
    if (!vt)
        return;
    const char *n = (vt[0] == '$') ? vt + 1 : vt;
    add_field(out, ctx->arena, n, ptype);
}

/* Look at __construct: any `$this->X = $param;` where $param is a typed
 * parameter contributes a field type for X. */
static void extract_ctor_assignments(PHPLSPContext *ctx, TSNode ctor_method,
                                     php_class_fields_t *out) {
    TSNode params = ts_node_child_by_field_name(ctor_method, "parameters", 10);
    /* Build a small param-name -> type map for this constructor. */
    const char *names[32];
    const CBMType *types[32];
    int pc = 0;
    if (!ts_node_is_null(params)) {
        uint32_t nc = ts_node_child_count(params);
        for (uint32_t i = 0; i < nc && pc < 32; i++) {
            TSNode p = ts_node_child(params, i);
            if (ts_node_is_null(p) || !ts_node_is_named(p))
                continue;
            const char *pk = ts_node_type(p);
            if (strcmp(pk, "simple_parameter") != 0 &&
                strcmp(pk, "property_promotion_parameter") != 0) {
                continue;
            }
            TSNode tnode = ts_node_child_by_field_name(p, "type", 4);
            const CBMType *ptype = NULL;
            if (!ts_node_is_null(tnode))
                ptype = php_parse_type_node(ctx, tnode);
            if (!ptype || ptype->kind == CBM_TYPE_UNKNOWN)
                continue;
            TSNode name_node = ts_node_child_by_field_name(p, "name", 4);
            if (ts_node_is_null(name_node)) {
                uint32_t pnc = ts_node_child_count(p);
                for (uint32_t j = 0; j < pnc; j++) {
                    TSNode c = ts_node_child(p, j);
                    if (!ts_node_is_null(c) && strcmp(ts_node_type(c), "variable_name") == 0) {
                        name_node = c;
                        break;
                    }
                }
            }
            if (ts_node_is_null(name_node))
                continue;
            char *vt = php_node_text(ctx, name_node);
            if (!vt)
                continue;
            names[pc] = cbm_arena_strdup(ctx->arena, (vt[0] == '$') ? vt + 1 : vt);
            types[pc] = ptype;
            pc++;
        }
    }
    if (pc == 0)
        return;

    /* Walk the body, looking for `$this->X = $name;` patterns. */
    TSNode body = ts_node_child_by_field_name(ctor_method, "body", 4);
    if (ts_node_is_null(body))
        return;
    /* Iterative DFS. */
    TSNode stack[64];
    int top = 0;
    stack[top++] = body;
    while (top > 0) {
        TSNode n = stack[--top];
        if (ts_node_is_null(n))
            continue;
        const char *k = ts_node_type(n);
        if (strcmp(k, "assignment_expression") == 0) {
            TSNode lhs = ts_node_child_by_field_name(n, "left", 4);
            TSNode rhs = ts_node_child_by_field_name(n, "right", 5);
            if (!ts_node_is_null(lhs) && !ts_node_is_null(rhs) &&
                strcmp(ts_node_type(lhs), "member_access_expression") == 0 &&
                strcmp(ts_node_type(rhs), "variable_name") == 0) {
                /* lhs = $this->X */
                TSNode obj = ts_node_child_by_field_name(lhs, "object", 6);
                TSNode fname = ts_node_child_by_field_name(lhs, "name", 4);
                if (!ts_node_is_null(obj) && !ts_node_is_null(fname)) {
                    char *ot = php_node_text(ctx, obj);
                    char *fn = php_node_text(ctx, fname);
                    char *rt = php_node_text(ctx, rhs);
                    if (ot && fn && rt && (ot[0] == '$') && strcmp(ot + 1, "this") == 0) {
                        const char *rname = (rt[0] == '$') ? rt + 1 : rt;
                        for (int i = 0; i < pc; i++) {
                            if (strcmp(names[i], rname) == 0) {
                                add_field(out, ctx->arena, fn, types[i]);
                                break;
                            }
                        }
                    }
                }
            }
        }
        uint32_t cnt = ts_node_child_count(n);
        for (uint32_t i = 0; i < cnt && top < 64; i++) {
            TSNode c = ts_node_child(n, i);
            if (!ts_node_is_null(c))
                stack[top++] = c;
        }
    }
}

/* Parse a class-level docblock for @property / @method tags and register
 * them as virtual fields and methods on the class. Used heavily by
 * Eloquent and other dynamic-PHP frameworks.
 *
 *   @property Foo $bar               → field bar: Foo
 *   @property-read Foo $bar          → same (treat the same as @property)
 *   @method Bar foo()                → method foo() returning Bar
 *   @method static Bar foo()         → method foo() returning Bar (static)
 *
 * The signature parsing for @method is lenient — we only care about the
 * return type for downstream chain inference. Argument types in the
 * tag are ignored. */
/* Parse `@template T` / `@template T of Foo` lines and return the
 * NULL-terminated array of type-parameter names. */
static const char **parse_phpdoc_template_params(PHPLSPContext *ctx, const char *docstring) {
    if (!docstring)
        return NULL;
    const char *names[16];
    int n = 0;
    const char *p = docstring;
    while ((p = strstr(p, "@template")) != NULL) {
        p += 9;
        /* Skip variants that bind a type-param name with a kind suffix:
         *   @template-covariant T   → record T
         *   @template-contravariant T → record T
         * but NOT @template-extends / @template-implements (those declare
         * a parent class, not a type param). */
        bool is_param_variant = true;
        if (*p == '-') {
            const char *suffix = p + 1;
            if (strncmp(suffix, "covariant", 9) == 0) {
                p += 1 + 9;
            } else if (strncmp(suffix, "contravariant", 13) == 0) {
                p += 1 + 13;
            } else {
                /* @template-extends / -implements / -use / etc. — skip line. */
                is_param_variant = false;
                while (*p && *p != ' ' && *p != '\t' && *p != '\n')
                    p++;
            }
        }
        if (!is_param_variant)
            continue;
        while (*p == ' ' || *p == '\t')
            p++;
        const char *name_start = p;
        while (*p && (isalnum((unsigned char)*p) || *p == '_'))
            p++;
        if (p == name_start)
            continue;
        if (n < 16) {
            names[n++] = cbm_arena_strndup(ctx->arena, name_start, (size_t)(p - name_start));
        }
    }
    if (n == 0)
        return NULL;
    const char **out = (const char **)cbm_arena_alloc(ctx->arena, (size_t)(n + 1) * sizeof(*out));
    if (!out)
        return NULL;
    for (int i = 0; i < n; i++)
        out[i] = names[i];
    out[n] = NULL;
    return out;
}

/* Substitute the type parameters in `t` according to a parallel
 * (param_names, args) mapping, returning a new arena-allocated CBMType. */
static const CBMType *php_substitute_template(CBMArena *arena, const CBMType *t,
                                              const char *const *param_names,
                                              const CBMType *const *args) {
    if (!t || !param_names || !args)
        return t;
    if (t->kind == CBM_TYPE_NAMED && t->data.named.qualified_name) {
        const char *qn = t->data.named.qualified_name;
        for (int i = 0; param_names[i]; i++) {
            if (strcmp(qn, param_names[i]) == 0) {
                return args[i] ? args[i] : t;
            }
        }
        return t;
    }
    if (t->kind == CBM_TYPE_TEMPLATE) {
        /* Recurse into args. */
        int n = t->data.template_type.arg_count;
        if (n <= 0 || !t->data.template_type.template_args)
            return t;
        const CBMType **new_args =
            (const CBMType **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(*new_args));
        if (!new_args)
            return t;
        for (int i = 0; i < n; i++) {
            new_args[i] = php_substitute_template(arena, t->data.template_type.template_args[i],
                                                  param_names, args);
        }
        new_args[n] = NULL;
        return cbm_type_template(arena, t->data.template_type.template_name, new_args, n);
    }
    return t;
}

static void apply_phpdoc_class_tags(PHPLSPContext *ctx, CBMTypeRegistry *reg, const char *class_qn,
                                    const char *docstring, php_class_fields_t *out_fields) {
    if (!docstring || !class_qn)
        return;

    /* @template T  / @template T of Foo  — register on the class. */
    const char **tparams = parse_phpdoc_template_params(ctx, docstring);
    if (tparams) {
        for (int t = 0; t < reg->type_count; t++) {
            if (strcmp(reg->types[t].qualified_name, class_qn) == 0) {
                reg->types[t].type_param_names = tparams;
                break;
            }
        }
    }

    /* @phpstan-type Alias TYPE_EXPR  — register a per-file alias so
     * subsequent @var/@param/@return references to `Alias` resolve to the
     * aliased type. This is a phpstan/psalm convention used heavily for
     * shape descriptions and value objects.
     *
     * Also accept the equivalent @psalm-type and @phan-type spellings. */
    {
        const char *prefixes[] = {"@phpstan-type", "@psalm-type", "@phan-type", NULL};
        for (int pi = 0; prefixes[pi]; pi++) {
            const char *p = docstring;
            size_t plen = strlen(prefixes[pi]);
            while ((p = strstr(p, prefixes[pi])) != NULL) {
                p += plen;
                while (*p == ' ' || *p == '\t')
                    p++;
                /* alias name */
                const char *name_start = p;
                while (*p && (isalnum((unsigned char)*p) || *p == '_'))
                    p++;
                if (p == name_start)
                    continue;
                char *alias_name =
                    cbm_arena_strndup(ctx->arena, name_start, (size_t)(p - name_start));
                while (*p == ' ' || *p == '\t' || *p == '=')
                    p++;
                /* type expression, span <...> brackets */
                const char *type_start = p;
                int depth = 0;
                while (*p && *p != '\n') {
                    if (*p == '<')
                        depth++;
                    else if (*p == '>')
                        depth--;
                    else if (depth == 0 && (*p == ' ' || *p == '\t') && p > type_start) {
                        break;
                    }
                    p++;
                }
                if (p == type_start)
                    continue;
                char *type_text =
                    cbm_arena_strndup(ctx->arena, type_start, (size_t)(p - type_start));
                /* Strip trailing whitespace in case. */
                size_t tl = strlen(type_text);
                while (tl > 0 && (type_text[tl - 1] == ' ' || type_text[tl - 1] == '\t')) {
                    type_text[--tl] = '\0';
                }
                if (!*type_text)
                    continue;
                const CBMType *aliased = resolve_phpdoc_type(ctx, type_text);
                if (aliased && aliased->kind != CBM_TYPE_UNKNOWN) {
                    php_add_phpstan_alias(ctx, alias_name, aliased);
                }
            }
        }
    }

    /* @phpstan-import-type Alias from Other — best-effort. We don't have
     * cross-file alias indexing yet, but accept the syntax without
     * crashing. The alias becomes UNKNOWN until the source class's
     * @phpstan-type ends up in our table. */
    {
        const char *p = docstring;
        while ((p = strstr(p, "@phpstan-import-type")) != NULL) {
            p += 20;
            while (*p == ' ' || *p == '\t')
                p++;
            const char *name_start = p;
            while (*p && (isalnum((unsigned char)*p) || *p == '_'))
                p++;
            if (p == name_start)
                continue;
            (void)name_start; /* declared imported alias — silently accept */
        }
    }

    /* @extends ParentClass<X>  /  @template-extends ParentClass<X>
     *  — when a class generically extends a parametric ancestor, we
     * record that the ancestor's type-params are bound to the concrete
     * args. We model this by replacing the embedded_types entry with
     * a synthetic key class_qn + "@@" + parent_with_args, plus storing
     * the type-arg mapping in a side table. For Phase 4s we keep it
     * minimal: when method lookup walks the ancestor, eval_member_call
     * on the receiver doesn't yet know about this binding. The full
     * substitution happens in Phase 4s+ via a side table; for now we
     * record the @extends parent in embedded_types as a NAMED type so
     * inherited methods at least resolve (untyped). */
    {
        const char *p = docstring;
        while ((p = strstr(p, "@extends")) != NULL) {
            p += 8;
            if (*p == '-') {
                while (*p && *p != ' ' && *p != '\t' && *p != '\n')
                    p++;
                continue;
            }
            while (*p == ' ' || *p == '\t')
                p++;
            const char *parent_start = p;
            int depth = 0;
            while (*p && *p != '\n') {
                if (*p == '<')
                    depth++;
                else if (*p == '>') {
                    depth--;
                    if (depth == 0) {
                        p++;
                        break;
                    }
                } else if (depth == 0 && (*p == ' ' || *p == '\t'))
                    break;
                p++;
            }
            if (p == parent_start)
                continue;
            char *parent_text =
                cbm_arena_strndup(ctx->arena, parent_start, (size_t)(p - parent_start));
            const CBMType *parent_type = resolve_phpdoc_type(ctx, parent_text);
            const char *parent_qn = NULL;
            if (parent_type && parent_type->kind == CBM_TYPE_NAMED) {
                parent_qn = parent_type->data.named.qualified_name;
            } else if (parent_type && parent_type->kind == CBM_TYPE_TEMPLATE) {
                parent_qn = parent_type->data.template_type.template_name;
            }
            if (!parent_qn)
                continue;
            /* Append to embedded_types if not already present. */
            for (int t = 0; t < reg->type_count; t++) {
                if (strcmp(reg->types[t].qualified_name, class_qn) != 0)
                    continue;
                int existing = 0;
                if (reg->types[t].embedded_types) {
                    while (reg->types[t].embedded_types[existing])
                        existing++;
                }
                bool found = false;
                for (int e = 0; e < existing; e++) {
                    if (strcmp(reg->types[t].embedded_types[e], parent_qn) == 0) {
                        found = true;
                        break;
                    }
                }
                if (found)
                    break;
                const char **expanded = (const char **)cbm_arena_alloc(
                    ctx->arena, (size_t)(existing + 2) * sizeof(*expanded));
                if (!expanded)
                    break;
                for (int e = 0; e < existing; e++) {
                    expanded[e] = reg->types[t].embedded_types[e];
                }
                expanded[existing] = parent_qn;
                expanded[existing + 1] = NULL;
                reg->types[t].embedded_types = expanded;
                break;
            }
        }
    }

    /* @property TYPE $name */
    const char *p = docstring;
    while ((p = strstr(p, "@property")) != NULL) {
        p += 9;
        /* Skip "-read" / "-write" suffix variants. */
        if (*p == '-') {
            while (*p && *p != ' ' && *p != '\t')
                p++;
        }
        while (*p == ' ' || *p == '\t')
            p++;
        const char *type_start = p;
        while (*p && *p != ' ' && *p != '\t' && *p != '\n')
            p++;
        if (p == type_start)
            continue;
        char *type_text = cbm_arena_strndup(ctx->arena, type_start, (size_t)(p - type_start));
        while (*p == ' ' || *p == '\t')
            p++;
        if (*p != '$')
            continue;
        p++;
        const char *name_start = p;
        while (*p && (isalnum((unsigned char)*p) || *p == '_'))
            p++;
        if (p == name_start)
            continue;
        char *fname = cbm_arena_strndup(ctx->arena, name_start, (size_t)(p - name_start));
        const CBMType *t = resolve_phpdoc_type(ctx, type_text);
        if (out_fields && fname && t && t->kind != CBM_TYPE_UNKNOWN) {
            add_field(out_fields, ctx->arena, fname, t);
        }
    }

    /* @method [static] TYPE name(...) */
    p = docstring;
    while ((p = strstr(p, "@method")) != NULL) {
        p += 7;
        while (*p == ' ' || *p == '\t')
            p++;
        /* optional `static` */
        if (strncmp(p, "static", 6) == 0 && (p[6] == ' ' || p[6] == '\t')) {
            p += 6;
            while (*p == ' ' || *p == '\t')
                p++;
        }
        const char *type_start = p;
        while (*p && *p != ' ' && *p != '\t' && *p != '\n')
            p++;
        if (p == type_start)
            continue;
        char *type_text = cbm_arena_strndup(ctx->arena, type_start, (size_t)(p - type_start));
        while (*p == ' ' || *p == '\t')
            p++;
        const char *name_start = p;
        while (*p && (isalnum((unsigned char)*p) || *p == '_'))
            p++;
        if (p == name_start)
            continue;
        char *mname = cbm_arena_strndup(ctx->arena, name_start, (size_t)(p - name_start));

        /* Register as a virtual function whose receiver_type is class_qn.
         * Generates a synthetic QN class_qn + "." + mname. */
        CBMRegisteredFunc rf;
        memset(&rf, 0, sizeof(rf));
        rf.qualified_name = cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, mname);
        rf.short_name = mname;
        rf.receiver_type = class_qn;
        rf.min_params = -1;
        const CBMType *ret = resolve_phpdoc_type(ctx, type_text);
        const CBMType **rets = (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(*rets));
        if (rets) {
            rets[0] = ret;
            rets[1] = NULL;
        }
        rf.signature = cbm_type_func(ctx->arena, NULL, NULL, rets);
        cbm_registry_add_func(reg, rf);
    }
}

/* Flatten the methods of a trait into the using class's method table.
 *
 * Tree-sitter-php emits `use Trait1, Trait2;` inside a class body as a
 * `use_declaration` (distinct from the namespace-level
 * namespace_use_declaration). The optional conflict resolution block
 * uses `use_instead_of_clause` (for `Trait1::foo insteadof Trait2`) and
 * `use_as_clause` (for `Trait2::foo as fooAlt`).
 *
 * Phase 4e basic strategy:
 *   - copy each method on the trait into the class as a synthetic
 *     CBMRegisteredFunc with receiver_type = class_qn
 *   - if the same method exists from multiple traits, last write wins
 *     (suboptimal but matches PHP's "use the last `use` line" default
 *     when no insteadof clause exists)
 *   - handle `as` aliasing: register method under the alias name too
 *   - skip insteadof (rare in practice; full support deferred)
 */
static void flatten_trait_into_class(PHPLSPContext *ctx, CBMTypeRegistry *reg, const char *class_qn,
                                     const char *trait_qn, const char *alias_for_method,
                                     const char *only_method_name) {
    if (!class_qn || !trait_qn)
        return;
    /* Resolve trait through alias-style lookup if needed. */
    const CBMRegisteredType *t = cbm_registry_lookup_type(reg, trait_qn);
    if (!t)
        t = lookup_type_with_project(ctx, trait_qn);
    const char *canonical_trait_qn = t ? t->qualified_name : trait_qn;

    /* Iterate registry funcs whose receiver_type is the trait.
     *
     * Self-substitution: when the trait's method has a return type that
     * names the trait itself (e.g. `tap(): self` registered as
     * NAMED(trait_qn)), rewrite it to NAMED(using_class_qn) so chains
     * like `$c->tap()->classMethod()` resolve correctly. */
    for (int i = 0; i < reg->func_count; i++) {
        const CBMRegisteredFunc *src = &reg->funcs[i];
        if (!src->receiver_type || strcmp(src->receiver_type, canonical_trait_qn) != 0) {
            continue;
        }
        if (only_method_name && src->short_name && strcmp(src->short_name, only_method_name) != 0) {
            continue;
        }
        const char *new_short = alias_for_method ? alias_for_method : src->short_name;
        if (!new_short)
            continue;
        CBMRegisteredFunc rf;
        memset(&rf, 0, sizeof(rf));
        rf.qualified_name = cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, new_short);
        rf.short_name = cbm_arena_strdup(ctx->arena, new_short);
        rf.receiver_type = class_qn;
        rf.min_params = src->min_params;

        /* Self-substitute the return type: any NAMED(trait_qn) in the
         * signature's return becomes NAMED(class_qn). */
        const CBMType *new_sig = src->signature;
        if (src->signature && src->signature->kind == CBM_TYPE_FUNC &&
            src->signature->data.func.return_types) {
            const CBMType *ret0 = src->signature->data.func.return_types[0];
            if (ret0 && ret0->kind == CBM_TYPE_NAMED && ret0->data.named.qualified_name &&
                strcmp(ret0->data.named.qualified_name, canonical_trait_qn) == 0) {
                const CBMType **rets =
                    (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(*rets));
                if (rets) {
                    rets[0] = cbm_type_named(ctx->arena, class_qn);
                    rets[1] = NULL;
                    new_sig = cbm_type_func(ctx->arena, NULL, NULL, rets);
                }
            }
        }
        rf.signature = new_sig;
        cbm_registry_add_func(reg, rf);
    }
}

static void process_trait_use(PHPLSPContext *ctx, CBMTypeRegistry *reg, const char *class_qn,
                              TSNode use_decl) {
    /* Collect the named traits and any `as` alias clauses. */
    const char *trait_qns[16];
    int trait_count = 0;

    uint32_t nc = ts_node_child_count(use_decl);
    for (uint32_t i = 0; i < nc && trait_count < 16; i++) {
        TSNode c = ts_node_child(use_decl, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "name") == 0 || strcmp(k, "qualified_name") == 0) {
            char *tn = php_node_text(ctx, c);
            if (tn) {
                const char *resolved = php_resolve_class_name(ctx, tn);
                if (resolved)
                    trait_qns[trait_count++] = resolved;
            }
        }
    }

    /* First, do the default flatten for all traits. */
    for (int i = 0; i < trait_count; i++) {
        flatten_trait_into_class(ctx, reg, class_qn, trait_qns[i], NULL, NULL);
    }

    /* Process `use_list` block for `as` / `insteadof` clauses. The block is
     * either named "body" or a sibling. */
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(use_decl, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "use_list") != 0 && strcmp(k, "use_clause") != 0) {
            uint32_t bnc = ts_node_child_count(c);
            for (uint32_t j = 0; j < bnc; j++) {
                TSNode cc = ts_node_child(c, j);
                if (ts_node_is_null(cc) || !ts_node_is_named(cc))
                    continue;
                const char *kk = ts_node_type(cc);
                if (strcmp(kk, "use_as_clause") == 0) {
                    /* Trait::method as alias  OR  method as alias */
                    char *trait_text = NULL;
                    char *method_text = NULL;
                    char *alias_text = NULL;
                    uint32_t anc = ts_node_child_count(cc);
                    for (uint32_t a = 0; a < anc; a++) {
                        TSNode ch = ts_node_child(cc, a);
                        if (ts_node_is_null(ch) || !ts_node_is_named(ch))
                            continue;
                        const char *ck = ts_node_type(ch);
                        if (strcmp(ck, "class_constant_access_expression") == 0) {
                            /* Trait::method */
                            uint32_t cc2 = ts_node_child_count(ch);
                            for (uint32_t a2 = 0; a2 < cc2; a2++) {
                                TSNode subch = ts_node_child(ch, a2);
                                if (ts_node_is_null(subch) || !ts_node_is_named(subch))
                                    continue;
                                const char *sck = ts_node_type(subch);
                                if (strcmp(sck, "name") == 0 ||
                                    strcmp(sck, "qualified_name") == 0) {
                                    if (!trait_text)
                                        trait_text = php_node_text(ctx, subch);
                                    else
                                        method_text = php_node_text(ctx, subch);
                                }
                            }
                        } else if (strcmp(ck, "name") == 0) {
                            char *t = php_node_text(ctx, ch);
                            if (!method_text)
                                method_text = t;
                            else
                                alias_text = t;
                        }
                    }
                    if (alias_text && method_text) {
                        const char *trait_qn_resolved = NULL;
                        if (trait_text) {
                            trait_qn_resolved = php_resolve_class_name(ctx, trait_text);
                        } else if (trait_count > 0) {
                            trait_qn_resolved = trait_qns[0];
                        }
                        if (trait_qn_resolved) {
                            flatten_trait_into_class(ctx, reg, class_qn, trait_qn_resolved,
                                                     alias_text, method_text);
                        }
                    }
                }
                /* `use_instead_of_clause` is recognised but not yet
                 * implemented — last-flatten-wins semantics already match
                 * the common case where the resolution clause picks a
                 * specific implementation. */
            }
        }
    }
}

static void process_class_for_fields(PHPLSPContext *ctx, CBMTypeRegistry *reg, TSNode class_node,
                                     php_class_field_table_t *tab) {
    const char *cqn = class_qn_for_node(ctx, class_node);
    if (!cqn)
        return;
    php_class_fields_t *f = fields_for(tab, cqn, ctx->arena);
    if (!f)
        return;

    /* PHPDoc on the class itself: @property + @method tags. */
    char *class_doc = fetch_leading_phpdoc(ctx, class_node);
    if (class_doc) {
        apply_phpdoc_class_tags(ctx, reg, cqn, class_doc, f);
    }

    /* Belt-and-suspenders: walk the class declaration's `base_clause` and
     * `class_interface_clause` to gather extends/implements QNs and append
     * them to the registered type's embedded_types. The unified extractor
     * is supposed to populate base_classes for class_declaration but
     * may miss interface_declaration's `extends Base` chain or use a
     * raw form (with backslashes) that doesn't round-trip cleanly.
     * Doing this AST extraction here keeps the LSP self-sufficient. */
    {
        const char *parent_qns[16];
        int parent_count = 0;
        uint32_t cnc = ts_node_child_count(class_node);
        for (uint32_t i = 0; i < cnc && parent_count < 16; i++) {
            TSNode c = ts_node_child(class_node, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "base_clause") != 0 && strcmp(k, "class_interface_clause") != 0) {
                continue;
            }
            uint32_t bnc = ts_node_child_count(c);
            for (uint32_t j = 0; j < bnc && parent_count < 16; j++) {
                TSNode bc = ts_node_child(c, j);
                if (ts_node_is_null(bc) || !ts_node_is_named(bc))
                    continue;
                const char *bk = ts_node_type(bc);
                if (strcmp(bk, "name") != 0 && strcmp(bk, "qualified_name") != 0)
                    continue;
                char *t = php_node_text(ctx, bc);
                if (!t)
                    continue;
                const char *resolved = php_resolve_class_name(ctx, t);
                if (!resolved)
                    continue;
                parent_qns[parent_count++] = resolved;
            }
        }
        if (parent_count > 0) {
            for (int t = 0; t < reg->type_count; t++) {
                if (strcmp(reg->types[t].qualified_name, cqn) != 0)
                    continue;
                /* Merge with existing embedded_types — dedup. */
                int existing = 0;
                if (reg->types[t].embedded_types) {
                    while (reg->types[t].embedded_types[existing])
                        existing++;
                }
                int total = existing;
                for (int p = 0; p < parent_count; p++) {
                    bool seen = false;
                    for (int e = 0; e < existing; e++) {
                        if (strcmp(reg->types[t].embedded_types[e], parent_qns[p]) == 0) {
                            seen = true;
                            break;
                        }
                    }
                    if (!seen)
                        total++;
                }
                if (total == existing)
                    break;
                const char **expanded = (const char **)cbm_arena_alloc(
                    ctx->arena, (size_t)(total + 1) * sizeof(*expanded));
                if (!expanded)
                    break;
                int wi = 0;
                for (int e = 0; e < existing; e++)
                    expanded[wi++] = reg->types[t].embedded_types[e];
                for (int p = 0; p < parent_count; p++) {
                    bool seen = false;
                    for (int e = 0; e < existing; e++) {
                        if (strcmp(reg->types[t].embedded_types[e], parent_qns[p]) == 0) {
                            seen = true;
                            break;
                        }
                    }
                    if (!seen)
                        expanded[wi++] = parent_qns[p];
                }
                expanded[wi] = NULL;
                reg->types[t].embedded_types = expanded;
                break;
            }
        }
    }

    TSNode body = ts_node_child_by_field_name(class_node, "body", 4);
    if (ts_node_is_null(body))
        body = child_named(class_node, "declaration_list");
    if (ts_node_is_null(body))
        body = child_named(class_node, "enum_declaration_list");
    if (ts_node_is_null(body))
        return;

    uint32_t nc = ts_node_child_count(body);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(body, i);
        if (ts_node_is_null(c) || !ts_node_is_named(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "property_declaration") == 0) {
            extract_property_decl(ctx, c, f);
        } else if (strcmp(k, "enum_case") == 0) {
            /* Enum case: Suit::Hearts → typed as the enum itself. */
            TSNode case_name;
            memset(&case_name, 0, sizeof(case_name));
            uint32_t enc = ts_node_child_count(c);
            for (uint32_t j = 0; j < enc; j++) {
                TSNode cc = ts_node_child(c, j);
                if (!ts_node_is_null(cc) && ts_node_is_named(cc) &&
                    strcmp(ts_node_type(cc), "name") == 0) {
                    case_name = cc;
                    break;
                }
            }
            if (!ts_node_is_null(case_name)) {
                char *cn = php_node_text(ctx, case_name);
                if (cn) {
                    /* Register as a static field of the enum type. The
                     * field-name model already handles $this->prop; for
                     * static access Suit::Hearts the eval of class_constant_access
                     * handles below. */
                    add_field(f, ctx->arena, cn, cbm_type_named(ctx->arena, cqn));
                }
            }
        } else if (strcmp(k, "use_declaration") == 0) {
            /* Trait `use Foo;` inside a class body. */
            process_trait_use(ctx, reg, cqn, c);
        } else if (strcmp(k, "method_declaration") == 0) {
            /* Constructor: scan params for property_promotion_parameter and
             * scan body for $this->X = $param assignments. */
            TSNode mname = ts_node_child_by_field_name(c, "name", 4);
            char *mn = !ts_node_is_null(mname) ? php_node_text(ctx, mname) : NULL;
            bool is_ctor = mn && strcmp(mn, "__construct") == 0;
            TSNode params = ts_node_child_by_field_name(c, "parameters", 10);
            if (!ts_node_is_null(params)) {
                uint32_t pnc = ts_node_child_count(params);
                for (uint32_t j = 0; j < pnc; j++) {
                    TSNode p = ts_node_child(params, j);
                    if (ts_node_is_null(p) || !ts_node_is_named(p))
                        continue;
                    if (strcmp(ts_node_type(p), "property_promotion_parameter") == 0) {
                        extract_promoted_param(ctx, p, f);
                    }
                }
            }
            if (is_ctor)
                extract_ctor_assignments(ctx, c, f);

            /* Re-parse the declared return type via php_parse_type_node so
             * generic forms like `\Generator<int, User>` end up as
             * TEMPLATE types, not broken NAMED("Generator<int, User>")
             * that the simpler parse_return_type_text produced earlier.
             *
             * We temporarily set ctx->enclosing_class_qn so that
             * `self`/`static` in the return-type position resolve to the
             * current class. */
            TSNode rt_node = ts_node_child_by_field_name(c, "return_type", 11);
            if (!ts_node_is_null(rt_node) && mn) {
                const char *saved_class = ctx->enclosing_class_qn;
                const char *saved_parent = ctx->enclosing_parent_qn;
                ctx->enclosing_class_qn = cqn;
                /* ctx->enclosing_parent_qn left as-is (best-effort). */
                const CBMType *parsed = php_parse_type_node(ctx, rt_node);
                ctx->enclosing_class_qn = saved_class;
                ctx->enclosing_parent_qn = saved_parent;
                if (parsed && parsed->kind != CBM_TYPE_UNKNOWN) {
                    for (int fi = 0; fi < reg->func_count; fi++) {
                        CBMRegisteredFunc *rf = &reg->funcs[fi];
                        if (!rf->receiver_type || !rf->short_name)
                            continue;
                        if (strcmp(rf->receiver_type, cqn) != 0)
                            continue;
                        if (strcmp(rf->short_name, mn) != 0)
                            continue;
                        const CBMType **rets =
                            (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(*rets));
                        if (rets) {
                            rets[0] = parsed;
                            rets[1] = NULL;
                            rf->signature = cbm_type_func(ctx->arena, NULL, NULL, rets);
                        }
                        break;
                    }
                }
            }

            /* PHPDoc @return TYPE on the method's leading docblock — when
             * the method has no declared return type, this fills in the
             * registry's signature so chained calls can substitute through.
             * Runs after the declared-type pass so explicit @return wins
             * over an inferred declared type. */
            char *mdoc = fetch_leading_phpdoc(ctx, c);
            if (mdoc && mn) {
                const char *p = strstr(mdoc, "@return");
                if (p) {
                    p += 7;
                    while (*p == ' ' || *p == '\t')
                        p++;
                    const char *type_start = p;
                    int depth = 0;
                    while (*p && *p != '\n') {
                        if (*p == '<')
                            depth++;
                        else if (*p == '>')
                            depth--;
                        else if (depth == 0 && (*p == ' ' || *p == '\t'))
                            break;
                        p++;
                    }
                    if (p > type_start) {
                        char *type_text =
                            cbm_arena_strndup(ctx->arena, type_start, (size_t)(p - type_start));
                        const CBMType *ret_t = NULL;
                        /* `@return $this` / `@return static` / `@return self`
                         * → return the enclosing class type. Critical for
                         * fluent-builder chains. */
                        if (strcmp(type_text, "$this") == 0 || strcmp(type_text, "static") == 0 ||
                            strcmp(type_text, "self") == 0) {
                            ret_t = cbm_type_named(ctx->arena, cqn);
                        }
                        /* Else: does the raw token match a class @template
                         * param name? If so, build NAMED(name) so call-site
                         * substitution can rewrite it later. */
                        if (!ret_t)
                            for (int t = 0; t < reg->type_count; t++) {
                                if (strcmp(reg->types[t].qualified_name, cqn) != 0)
                                    continue;
                                if (!reg->types[t].type_param_names)
                                    break;
                                for (int j = 0; reg->types[t].type_param_names[j]; j++) {
                                    if (strcmp(reg->types[t].type_param_names[j], type_text) == 0) {
                                        ret_t = cbm_type_named(ctx->arena, type_text);
                                        break;
                                    }
                                }
                                break;
                            }
                        if (!ret_t)
                            ret_t = resolve_phpdoc_type(ctx, type_text);
                        if (ret_t && ret_t->kind != CBM_TYPE_UNKNOWN) {
                            /* Find the registered method by receiver_type=cqn,
                             * short_name=mn and patch its signature. */
                            for (int fi = 0; fi < reg->func_count; fi++) {
                                CBMRegisteredFunc *rf = &reg->funcs[fi];
                                if (!rf->receiver_type || !rf->short_name)
                                    continue;
                                if (strcmp(rf->receiver_type, cqn) != 0)
                                    continue;
                                if (strcmp(rf->short_name, mn) != 0)
                                    continue;
                                /* Replace the signature's return type. */
                                const CBMType **rets = (const CBMType **)cbm_arena_alloc(
                                    ctx->arena, 2 * sizeof(*rets));
                                if (rets) {
                                    rets[0] = ret_t;
                                    rets[1] = NULL;
                                    rf->signature = cbm_type_func(ctx->arena, NULL, NULL, rets);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

static void php_lsp_collect_class_fields(PHPLSPContext *ctx, CBMTypeRegistry *reg, TSNode root,
                                         php_class_field_table_t *tab) {
    if (ts_node_is_null(root))
        return;
    /* TWO PASSES so trait/interface declared-return-types get patched
     * BEFORE classes that use them — otherwise flatten_trait_into_class
     * sees stale (BUILTIN/UNKNOWN) signatures and can't perform the
     * trait `self` substitution.
     *
     * Pass 1: traits and interfaces (declarators that don't pull from
     * other classes via `use`).
     * Pass 2: classes and enums (which may `use Trait;` and need the
     * trait's signatures already patched).
     */
    for (int pass = 0; pass < 2; pass++) {
        TSNode stack[256];
        int top = 0;
        stack[top++] = root;
        while (top > 0) {
            TSNode n = stack[--top];
            if (ts_node_is_null(n))
                continue;
            const char *k = ts_node_type(n);
            bool is_trait_or_iface =
                (strcmp(k, "trait_declaration") == 0 || strcmp(k, "interface_declaration") == 0);
            bool is_class_or_enum =
                (strcmp(k, "class_declaration") == 0 || strcmp(k, "enum_declaration") == 0);
            if ((pass == 0 && is_trait_or_iface) || (pass == 1 && is_class_or_enum)) {
                process_class_for_fields(ctx, reg, n, tab);
            }
            uint32_t nc = ts_node_child_count(n);
            for (uint32_t i = 0; i < nc && top < 256; i++) {
                TSNode c = ts_node_child(n, i);
                if (!ts_node_is_null(c) && ts_node_is_named(c))
                    stack[top++] = c;
            }
        }
    }
}

/* ── entry: cbm_run_php_lsp ─────────────────────────────────────── */

void cbm_run_php_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                     TSNode root) {
    if (!result || !arena || ts_node_is_null(root))
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    /* Phase A: register stdlib types/functions. */
    cbm_php_stdlib_register(&reg, arena);

    const char *module_qn = result->module_qn;

    /* Phase B: register types and methods/functions from this file's defs.
     * We do not yet know the namespace mapping for the type's QN, so we
     * trust the QN that the unified extractor already produced. */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name || !d->label)
            continue;

        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0 ||
            strcmp(d->label, "Trait") == 0 || strcmp(d->label, "Enum") == 0 ||
            strcmp(d->label, "Type") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->name;
            rt.is_interface = (strcmp(d->label, "Interface") == 0);

            /* Hoist base_classes as embedded_types so php_lookup_method's
             * parent walk works. */
            if (d->base_classes) {
                int bc = 0;
                while (d->base_classes[bc])
                    bc++;
                if (bc > 0) {
                    const char **emb = (const char **)cbm_arena_alloc(
                        arena, (size_t)(bc + 1) * sizeof(const char *));
                    if (emb) {
                        for (int j = 0; j < bc; j++) {
                            const char *base = d->base_classes[j];
                            /* Try same-module, then bare. */
                            const char *qualified = base;
                            if (base[0] != '\\' && !strchr(base, '.')) {
                                qualified = cbm_arena_sprintf(arena, "%s.%s", module_qn, base);
                            } else if (base[0] == '\\') {
                                qualified = php_ns_to_dot(arena, base + 1);
                            } else {
                                qualified = php_ns_to_dot(arena, base);
                            }
                            emb[j] = qualified;
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
            if (strcmp(d->label, "Method") == 0 && d->parent_class) {
                rf.receiver_type = d->parent_class;
            }
            /* Build a minimal signature with just the return type. */
            const CBMType *ret_t =
                d->return_type
                    ? parse_return_type_text(arena, d->return_type, module_qn, NULL, NULL, NULL, 0)
                    : cbm_type_unknown();
            const CBMType **rets =
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
            if (rets) {
                rets[0] = ret_t;
                rets[1] = NULL;
            }
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);
            cbm_registry_add_func(&reg, rf);
        }
    }

    /* Phase C: run the resolver. */
    PHPLSPContext ctx;
    php_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, &result->resolved_calls);

    /* Phase B.1: collect typed properties (declared, promoted, ctor-assigned)
     * and merge them onto the registered types. Must run after stdlib +
     * file defs are registered but before the call walk so eval_expr_type
     * for $this->prop and $obj->prop sees the field types. */
    {
        php_class_field_table_t tab = {0};
        /* First pass to populate the use map (so type names in property
         * declarations resolve correctly). */
        /* Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²). */
        uint32_t pkn = 0;
        TSNode *pkids = cbm_lsp_collect_children(ctx.arena, root, &pkn);
        for (uint32_t i = 0; i < pkn; i++) {
            TSNode c = pkids[i];
            const char *k = ts_node_type(c);
            if (strcmp(k, "namespace_definition") == 0) {
                set_namespace_from_decl(&ctx, c);
            } else if (strcmp(k, "namespace_use_declaration") == 0) {
                collect_use_declaration(&ctx, c);
            }
        }
        php_lsp_collect_class_fields(&ctx, &reg, root, &tab);
        /* Reset namespace/use state — process_file will re-collect. */
        ctx.current_namespace_qn = "";
        ctx.use_count = 0;

        for (int i = 0; i < tab.count; i++) {
            php_class_fields_t *f = &tab.items[i];
            if (f->count == 0)
                continue;
            /* Mutate the existing entry — registered types are stored by
             * value in reg.types[], find the index and update it. */
            for (int t = 0; t < reg.type_count; t++) {
                if (strcmp(reg.types[t].qualified_name, f->class_qn) == 0) {
                    reg.types[t].field_names = f->field_names;
                    reg.types[t].field_types = f->field_types;
                    break;
                }
            }
        }
    }

    php_lsp_process_file(&ctx, root);

    if (ctx.debug) {
        fprintf(stderr, "[php_lsp] module=%s defs=%d types=%d funcs=%d resolved=%d\n",
                module_qn ? module_qn : "?", result->defs.count, reg.type_count, reg.func_count,
                result->resolved_calls.count);
        for (int i = 0; i < result->resolved_calls.count; i++) {
            const CBMResolvedCall *rc = &result->resolved_calls.items[i];
            fprintf(stderr, "[php_lsp]   %s -> %s [%s %.2f]\n", rc->caller_qn ? rc->caller_qn : "?",
                    rc->callee_qn ? rc->callee_qn : "?", rc->strategy ? rc->strategy : "?",
                    rc->confidence);
        }
    }
}

/* ── Cross-file + batch ───────────────────────────────────────── */

/* Split a "|"-separated list into a NULL-terminated array of arena copies. */
static const char **php_split_pipe(CBMArena *arena, const char *text) {
    if (!text || !text[0])
        return NULL;
    int count = 1;
    for (const char *p = text; *p; p++)
        if (*p == '|')
            count++;
    const char **out =
        (const char **)cbm_arena_alloc(arena, (size_t)(count + 1) * sizeof(const char *));
    if (!out)
        return NULL;
    int idx = 0;
    const char *start = text;
    for (const char *p = text;; p++) {
        if (*p == '|' || *p == '\0') {
            size_t n = (size_t)(p - start);
            char *s = (char *)cbm_arena_alloc(arena, n + 1);
            if (!s)
                return NULL;
            memcpy(s, start, n);
            s[n] = '\0';
            out[idx++] = s;
            if (*p == '\0')
                break;
            start = p + 1;
        }
    }
    out[idx] = NULL;
    return out;
}

/* Build a registry from caller-supplied CBMLSPDef[] — covers both the source
 * file's own defs and cross-file referenced defs. PHP labels recognised:
 * Class / Interface / Trait / Enum / Type for types; Function / Method for
 * functions. Return type strings are parsed via parse_return_type_text using
 * each def's def_module_qn so bare type names qualify against the module
 * where the def lives, not the importer's module. */
static void php_register_lsp_defs(CBMArena *arena, CBMArena *idx_arena, CBMTypeRegistry *reg,
                                  CBMLSPDef *defs, int def_count) {
    /* Pass 1: types only. The method pass below probes the registry per
     * Method def (receiver auto-registration); doing that against an
     * unfinalized registry is a LINEAR scan over the growing type table —
     * O(methods x types) per file, and PHP cross runs per file: symfony went
     * from ~25 s to 416 s on exactly this. Register all types first, build
     * the hash, then register funcs/methods with O(1) lookups (post-finalize
     * stub additions stay visible via the registry's tail-scan). */
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;

        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0 ||
            strcmp(d->label, "Trait") == 0 || strcmp(d->label, "Enum") == 0 ||
            strcmp(d->label, "Type") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name; /* borrowed — d outlives this call */
            rt.short_name = d->short_name;
            rt.is_interface = d->is_interface || strcmp(d->label, "Interface") == 0;
            rt.embedded_types = php_split_pipe(arena, d->embedded_types);
            if (d->method_names_str && d->method_names_str[0]) {
                rt.method_names = php_split_pipe(arena, d->method_names_str);
            }
            cbm_registry_add_type(reg, rt);
        }
    }
    /* idx_arena == NULL skips the mid-build finalize (callers that register
     * one def at a time would rebuild buckets per def — see py tier-2). */
    if (idx_arena) {
        cbm_registry_finalize_into(reg, idx_arena);
    }

    /* Pass 2: functions and methods. */
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;

        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name; /* borrowed */
            rf.short_name = d->short_name;

            /* Build FUNC type from "|"-separated return-type texts. Each piece
             * is the unqualified return-type expression as it appears in the
             * def-defining module's source. parse_return_type_text qualifies
             * bare type names against the def's own module_qn. */
            const char **ret_strs = php_split_pipe(arena, d->return_types);
            const CBMType **ret_types = NULL;
            if (ret_strs) {
                int n = 0;
                while (ret_strs[n])
                    n++;
                if (n > 0) {
                    ret_types = (const CBMType **)cbm_arena_alloc(
                        arena, (size_t)(n + 1) * sizeof(const CBMType *));
                    if (ret_types) {
                        const char *def_mod = d->def_module_qn ? d->def_module_qn : "";
                        for (int j = 0; j < n; j++) {
                            ret_types[j] = parse_return_type_text(arena, ret_strs[j], def_mod, NULL,
                                                                  NULL, NULL, 0);
                        }
                        ret_types[n] = NULL;
                    }
                }
            }
            rf.signature = cbm_type_func(arena, NULL, NULL, ret_types);

            if (strcmp(d->label, "Method") == 0 && d->receiver_type && d->receiver_type[0]) {
                rf.receiver_type = d->receiver_type; /* borrowed */
                /* Auto-register receiver type if cross-file def chain didn't
                 * include it explicitly, so php_lookup_method's chain walk
                 * has somewhere to land. */
                if (!cbm_registry_lookup_type(reg, rf.receiver_type)) {
                    CBMRegisteredType auto_t;
                    memset(&auto_t, 0, sizeof(auto_t));
                    auto_t.qualified_name = rf.receiver_type;
                    const char *dot = strrchr(d->receiver_type, '.');
                    auto_t.short_name = dot ? dot + 1 : rf.receiver_type; /* borrowed substring */
                    cbm_registry_add_type(reg, auto_t);
                }
            }
            cbm_registry_add_func(reg, rf);
        }
    }
}

void cbm_run_php_lsp_cross(CBMArena *arena, const char *source, int source_len,
                           const char *module_qn, CBMLSPDef *defs, int def_count,
                           const char **import_names, const char **import_qns, int import_count,
                           TSTree *cached_tree, CBMResolvedCallArray *out) {
    if (!arena || !source || source_len <= 0 || !out)
        return;

    TSParser *parser = NULL;
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return;
        ts_parser_set_language(parser, tree_sitter_php_only());
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
    cbm_php_stdlib_register(&reg, arena);
    /* Index allocations go to a per-call scratch arena: reg's arena is the
     * pipeline-lifetime result arena, and per-file bucket allocations there
     * accumulate GBs across a large repo. The scratch dies with this call. */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    php_register_lsp_defs(arena, &idx_arena, &reg, defs, def_count);

    /* Finalize registry — O(1) lookups. See go_lsp.c "3c. Finalize"
     * comment for the rationale. */
    cbm_registry_finalize_into(&reg, &idx_arena);

    PHPLSPContext ctx;
    php_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, out);

    /* Caller-supplied imports register first. process_file's own AST walk
     * adds file-internal `use` declarations on top of these. */
    for (int i = 0; i < import_count; i++) {
        if (import_names && import_qns && import_names[i] && import_qns[i]) {
            php_lsp_add_use(&ctx, import_names[i], import_qns[i], CBM_PHP_USE_CLASS);
        }
    }

    /* Class field collection — same flow as cbm_run_php_lsp. Walks the AST
     * to populate typed-property field maps so $this->prop and $obj->prop
     * resolve to the right type during the call walk. */
    {
        php_class_field_table_t tab = {0};
        /* Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²). */
        uint32_t pkn = 0;
        TSNode *pkids = cbm_lsp_collect_children(ctx.arena, root, &pkn);
        for (uint32_t i = 0; i < pkn; i++) {
            TSNode c = pkids[i];
            const char *k = ts_node_type(c);
            if (strcmp(k, "namespace_definition") == 0) {
                set_namespace_from_decl(&ctx, c);
            } else if (strcmp(k, "namespace_use_declaration") == 0) {
                collect_use_declaration(&ctx, c);
            }
        }
        php_lsp_collect_class_fields(&ctx, &reg, root, &tab);
        ctx.current_namespace_qn = "";
        /* Reset to caller-supplied uses only — process_file re-adds AST uses. */
        ctx.use_count = import_count;
        for (int i = 0; i < tab.count; i++) {
            php_class_fields_t *f = &tab.items[i];
            if (f->count == 0)
                continue;
            for (int t = 0; t < reg.type_count; t++) {
                if (strcmp(reg.types[t].qualified_name, f->class_qn) == 0) {
                    reg.types[t].field_names = f->field_names;
                    reg.types[t].field_types = f->field_types;
                    break;
                }
            }
        }
    }

    php_lsp_process_file(&ctx, root);
    cbm_arena_destroy(&idx_arena);

    if (owns_tree && tree)
        ts_tree_delete(tree);
    if (parser)
        ts_parser_delete(parser);
}

void cbm_batch_php_lsp_cross(CBMArena *arena, CBMBatchPHPLSPFile *files, int file_count,
                             CBMResolvedCallArray *out) {
    if (!arena || !files || file_count <= 0 || !out)
        return;

    for (int f = 0; f < file_count; f++) {
        CBMBatchPHPLSPFile *file = &files[f];
        memset(&out[f], 0, sizeof(CBMResolvedCallArray));
        if (!file->source || file->source_len <= 0)
            continue;

        CBMArena file_arena;
        cbm_arena_init(&file_arena);

        CBMResolvedCallArray file_out;
        memset(&file_out, 0, sizeof(file_out));

        cbm_run_php_lsp_cross(&file_arena, file->source, file->source_len, file->module_qn,
                              file->defs, file->def_count, file->import_names, file->import_qns,
                              file->import_count, file->cached_tree, &file_out);

        if (file_out.count > 0) {
            out[f].count = file_out.count;
            out[f].items = (CBMResolvedCall *)cbm_arena_alloc(arena, (size_t)file_out.count *
                                                                         sizeof(CBMResolvedCall));
            if (out[f].items) {
                for (int j = 0; j < file_out.count; j++) {
                    CBMResolvedCall *src = &file_out.items[j];
                    CBMResolvedCall *dst = &out[f].items[j];
                    dst->caller_qn =
                        src->caller_qn ? cbm_arena_strdup(arena, src->caller_qn) : NULL;
                    dst->callee_qn =
                        src->callee_qn ? cbm_arena_strdup(arena, src->callee_qn) : NULL;
                    dst->strategy = src->strategy ? cbm_arena_strdup(arena, src->strategy) : NULL;
                    dst->confidence = src->confidence;
                    dst->reason = src->reason ? cbm_arena_strdup(arena, src->reason) : NULL;
                }
            } else {
                out[f].count = 0;
            }
        }

        cbm_arena_destroy(&file_arena);
    }
}
