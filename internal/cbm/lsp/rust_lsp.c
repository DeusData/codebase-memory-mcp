/* rust_lsp.c — Type-aware call resolution for Rust source files.
 *
 * Mirrors the structure of `go_lsp.c` and reverse-engineers the relevant
 * pieces of `rust-analyzer` (`hir-def/resolver.rs`,
 * `hir-ty/method_resolution.rs`, `hir-ty/infer.rs`) into a per-file walk
 * driven by tree-sitter-rust.
 *
 * The compilation unit is split into clearly-labelled sections:
 *
 *   1.  Init + helpers            (~150 lines)
 *   2.  Builtin / prelude tables  (~100 lines)
 *   3.  Path & use resolution     (~250 lines)
 *   4.  Type-AST → CBMType        (~250 lines)
 *   5.  Generic substitution      (~150 lines)
 *   6.  Expression evaluator      (~700 lines)
 *   7.  Method dispatch           (~400 lines)
 *   8.  Macro handling            (~200 lines)
 *   9.  Statement / pattern bind  (~400 lines)
 *   10. Function & file walk      (~250 lines)
 *   11. Per-file entry            (~250 lines)
 *   12. Cross-file + batch        (~250 lines)
 *
 * Total ~3300 lines, matching the depth of go_lsp.c (2750) and py_lsp.c
 * (3188). The code is structured so each section has a single coherent
 * responsibility — there are no surprise back-edges between sections.
 */

#include "rust_lsp.h"
#include "rust_cargo.h"
#include "../helpers.h"
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ════════════════════════════════════════════════════════════════════
 * 1. Initialisation + arena helpers
 * ════════════════════════════════════════════════════════════════════ */

/* Forward declarations for early callers in the file. */
static void rust_resolve_calls_in_node(RustLSPContext *ctx, TSNode node);
static void rust_emit_resolved_call(RustLSPContext *ctx, const char *callee_qn,
                                    const char *strategy, float confidence);
static void rust_inject_syn_call(RustLSPContext *ctx, const char *callee_qn);
static void rust_emit_unresolved_call(RustLSPContext *ctx, const char *expr_text,
                                      const char *reason);
static const CBMType *rust_lookup_field(RustLSPContext *ctx, const char *type_qn,
                                        const char *field_name, int depth);
static const CBMRegisteredFunc *rust_lookup_method_in_trait(RustLSPContext *ctx,
                                                            const char *trait_qn,
                                                            const char *method_name);
static char *rust_node_text(RustLSPContext *ctx, TSNode node);
static const char *convert_path_to_qn(CBMArena *arena, const char *path);
static bool rust_type_derefs_to_first_arg(const char *type_qn);
static const char *rust_lookup_type_param_bound(RustLSPContext *ctx, const char *name);
static void rust_collect_bounds_from_text(RustLSPContext *ctx, const char *text);
static void rust_record_type_param_bound(RustLSPContext *ctx, const char *param_name,
                                         const char *trait_qn);

void rust_lsp_init(RustLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                   const CBMTypeRegistry *registry, const char *module_qn,
                   CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(RustLSPContext));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->module_qn = module_qn;
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL);

    const char *dbg = getenv("CBM_LSP_DEBUG");
    ctx->debug = (dbg && dbg[0]);
}

/* Doubling-array push of a `(local, full-path)` use entry. */
void rust_lsp_add_use(RustLSPContext *ctx, const char *local_name, const char *module_path) {
    if (!ctx || !local_name || !module_path) {
        return;
    }
    if (ctx->use_count % 32 == 0) {
        int new_cap = ctx->use_count + 32;
        const char **nl =
            (const char **)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(char *));
        const char **np =
            (const char **)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(char *));
        if (!nl || !np) {
            return;
        }
        if (ctx->use_local_names && ctx->use_count > 0) {
            memcpy(nl, ctx->use_local_names, ctx->use_count * sizeof(char *));
            memcpy(np, ctx->use_module_paths, ctx->use_count * sizeof(char *));
        }
        ctx->use_local_names = nl;
        ctx->use_module_paths = np;
    }
    ctx->use_local_names[ctx->use_count] = cbm_arena_strdup(ctx->arena, local_name);
    ctx->use_module_paths[ctx->use_count] = cbm_arena_strdup(ctx->arena, module_path);
    ctx->use_count++;
}

void rust_lsp_add_glob(RustLSPContext *ctx, const char *module_qn) {
    if (!ctx || !module_qn) {
        return;
    }
    if (ctx->glob_count % 16 == 0) {
        int new_cap = ctx->glob_count + 16;
        const char **ng =
            (const char **)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(char *));
        if (!ng) {
            return;
        }
        if (ctx->glob_module_qns && ctx->glob_count > 0) {
            memcpy(ng, ctx->glob_module_qns, ctx->glob_count * sizeof(char *));
        }
        ctx->glob_module_qns = ng;
    }
    ctx->glob_module_qns[ctx->glob_count++] = cbm_arena_strdup(ctx->arena, module_qn);
}

static char *rust_node_text(RustLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

/* ════════════════════════════════════════════════════════════════════
 * 2. Builtin / prelude tables
 * ════════════════════════════════════════════════════════════════════ */

/* Rust primitive types that the grammar reports as `primitive_type`. */
static const char *RUST_PRIMITIVES[] = {"i8",   "i16",  "i32", "i64",  "i128",  "isize", "u8",
                                        "u16",  "u32",  "u64", "u128", "usize", "f32",   "f64",
                                        "bool", "char", "str", "()",   "!",     NULL};

static bool is_rust_primitive(const char *name) {
    if (!name) {
        return false;
    }
    for (const char **p = RUST_PRIMITIVES; *p; p++) {
        if (strcmp(*p, name) == 0) {
            return true;
        }
    }
    return false;
}

/* Names of macros that behave like println-family: side effects only,
 * return type `()`. */
static bool is_void_macro(const char *name) {
    if (!name) {
        return false;
    }
    static const char *m[] = {"println",
                              "print",
                              "eprintln",
                              "eprint",
                              "panic",
                              "unimplemented",
                              "todo",
                              "unreachable",
                              "assert",
                              "assert_eq",
                              "assert_ne",
                              "debug_assert",
                              "debug_assert_eq",
                              "debug_assert_ne",
                              "writeln",
                              "write",
                              NULL};
    for (const char **p = m; *p; p++) {
        if (strcmp(*p, name) == 0) {
            return true;
        }
    }
    return false;
}

/* Names of macros that produce a `String` value. */
static bool is_string_macro(const char *name) {
    if (!name) {
        return false;
    }
    return strcmp(name, "format") == 0 || strcmp(name, "concat") == 0 ||
           strcmp(name, "stringify") == 0 || strcmp(name, "env") == 0 ||
           strcmp(name, "include_str") == 0;
}

/* Prelude trait names whose method short-names we treat as universally
 * available (for emit-on-best-effort when we cannot pin down the trait
 * impl). Borrowed from `core::prelude::v1`. */
static bool is_prelude_trait_method(const char *method_name) {
    if (!method_name) {
        return false;
    }
    static const char *m[] = {/* Clone / Copy / Default */
                              "clone", "default",
                              /* PartialEq / Eq / PartialOrd / Ord */
                              "eq", "ne", "cmp", "partial_cmp", "lt", "le", "gt", "ge",
                              /* Hash */
                              "hash",
                              /* Display / Debug */
                              "fmt", "to_string",
                              /* From / Into / TryFrom / TryInto */
                              "from", "into", "try_from", "try_into",
                              /* AsRef / AsMut / Borrow / BorrowMut */
                              "as_ref", "as_mut", "borrow", "borrow_mut",
                              /* Deref */
                              "deref", "deref_mut",
                              /* Drop */
                              "drop",
                              /* Iterator (most-used subset) */
                              "next", "iter", "iter_mut", "into_iter", "map", "filter", "fold",
                              "for_each", "collect", "count", "sum", "max", "min", "any", "all",
                              "find", "position", "enumerate", "zip", "chain", "take", "skip",
                              "rev", "cloned", "copied", "by_ref", "step_by", "flat_map", "flatten",
                              "filter_map", "peekable",
                              /* Future */
                              "poll", NULL};
    for (const char **p = m; *p; p++) {
        if (strcmp(*p, method_name) == 0) {
            return true;
        }
    }
    return false;
}

/* ════════════════════════════════════════════════════════════════════
 * 3. Path & use resolution
 * ════════════════════════════════════════════════════════════════════ */

/* Return the last `::`-separated segment of a Rust path (`std::io::Read` →
 * `Read`). Pointer aliases into `path` — caller does not own. */
static const char *path_last_segment(const char *path) {
    if (!path || !path[0]) {
        return path;
    }
    const char *p = path;
    const char *last = path;
    while (*p) {
        if (p[0] == ':' && p[1] == ':') {
            last = p + 2;
            p += 2;
            continue;
        }
        p++;
    }
    return last;
}

/* Convert a Rust path with `::` separators into our internal QN form using
 * `.` separators. Always allocates a fresh string. */
static const char *convert_path_to_qn(CBMArena *arena, const char *path) {
    if (!path || !path[0]) {
        return path;
    }
    size_t len = strlen(path);
    char *out = (char *)cbm_arena_alloc(arena, len + 1);
    if (!out) {
        return path;
    }
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        if (path[i] == ':' && i + 1 < len && path[i + 1] == ':') {
            out[j++] = '.';
            i++;
        } else {
            out[j++] = path[i];
        }
    }
    out[j] = '\0';
    return out;
}

/* In-place strip turbofish segments (`::<...>`) from a Rust path. The
 * grammar exposes paths like `Vec::<i32>::new` or `parse::<u32>(s)` —
 * the LSP cares about the underlying name, not the explicit type
 * arguments, so we collapse `head::<args>::tail` to `head::tail`.
 *
 * Modifies `path` in place. Safe on NULL. */
static void rust_strip_turbofish(char *path) {
    if (!path)
        return;
    char *read = path;
    char *write = path;
    while (*read) {
        if (read[0] == ':' && read[1] == ':' && read[2] == '<') {
            /* Skip ::< … > balanced. */
            int depth = 1;
            const char *p = read + 3;
            while (*p && depth > 0) {
                if (*p == '<')
                    depth++;
                else if (*p == '>')
                    depth--;
                p++;
            }
            read = (char *)p;
            continue;
        }
        *write++ = *read++;
    }
    *write = '\0';
}

/* Look up a `use` alias and return its fully-qualified module path,
 * or NULL if absent. The returned pointer aliases into the use map. */
static const char *rust_resolve_use(RustLSPContext *ctx, const char *local_name) {
    if (!ctx || !local_name) {
        return NULL;
    }
    for (int i = 0; i < ctx->use_count; i++) {
        if (strcmp(ctx->use_local_names[i], local_name) == 0) {
            return ctx->use_module_paths[i];
        }
    }
    return NULL;
}

/* The Rust prelude is auto-imported into every module. We map each name
 * to its canonical QN so bare references (`String`, `Vec::new`, …)
 * resolve without an explicit `use`. The list mirrors `core::prelude::v1`
 * + `alloc::prelude` + `std::prelude::v1`. The mapping is consulted before
 * the project-local fallback so prelude names always win. */
typedef struct {
    const char *name;
    const char *qn;
} RustPreludeEntry;

static const RustPreludeEntry RUST_PRELUDE[] = {{"String", "alloc.string.String"},
                                                {"ToString", "alloc.string.ToString"},
                                                {"Vec", "alloc.vec.Vec"},
                                                {"VecDeque", "alloc.collections.VecDeque"},
                                                {"HashMap", "alloc.collections.HashMap"},
                                                {"BTreeMap", "alloc.collections.BTreeMap"},
                                                {"HashSet", "alloc.collections.HashSet"},
                                                {"BTreeSet", "alloc.collections.BTreeSet"},
                                                {"Box", "alloc.boxed.Box"},
                                                {"Rc", "alloc.rc.Rc"},
                                                {"Arc", "alloc.sync.Arc"},
                                                {"Option", "core.option.Option"},
                                                {"Some", "core.option.Option.Some"},
                                                {"None", "core.option.Option.None"},
                                                {"Result", "core.result.Result"},
                                                {"Ok", "core.result.Result.Ok"},
                                                {"Err", "core.result.Result.Err"},
                                                {"Iterator", "core.iter.Iterator"},
                                                {"IntoIterator", "core.iter.IntoIterator"},
                                                {"Future", "core.future.Future"},
                                                {"Clone", "core.clone.Clone"},
                                                {"Copy", "core.marker.Copy"},
                                                {"Send", "core.marker.Send"},
                                                {"Sync", "core.marker.Sync"},
                                                {"Default", "core.default.Default"},
                                                {"PartialEq", "core.cmp.PartialEq"},
                                                {"Eq", "core.cmp.Eq"},
                                                {"PartialOrd", "core.cmp.PartialOrd"},
                                                {"Ord", "core.cmp.Ord"},
                                                {"Hash", "core.hash.Hash"},
                                                {"Display", "core.fmt.Display"},
                                                {"Debug", "core.fmt.Debug"},
                                                {"From", "core.convert.From"},
                                                {"Into", "core.convert.Into"},
                                                {"TryFrom", "core.convert.TryFrom"},
                                                {"TryInto", "core.convert.TryInto"},
                                                {"AsRef", "core.convert.AsRef"},
                                                {"AsMut", "core.convert.AsMut"},
                                                {"Borrow", "core.borrow.Borrow"},
                                                {"BorrowMut", "core.borrow.BorrowMut"},
                                                {"Deref", "core.ops.Deref"},
                                                {"DerefMut", "core.ops.DerefMut"},
                                                {"Drop", "core.ops.Drop"},
                                                {"RefCell", "core.cell.RefCell"},
                                                {"Cell", "core.cell.Cell"},
                                                {"Mutex", "std.sync.Mutex"},
                                                {"RwLock", "std.sync.RwLock"},
                                                {NULL, NULL}};

static const char *rust_lookup_prelude(const char *name) {
    if (!name)
        return NULL;
    for (const RustPreludeEntry *e = RUST_PRELUDE; e->name; e++) {
        if (strcmp(e->name, name) == 0)
            return e->qn;
    }
    return NULL;
}

/* Strip a leading `&` / `&mut` reference prefix from a textual type so we
 * can compare the inner head segment against builtins. */
static const char *skip_ref_prefix(const char *text) {
    if (!text) {
        return text;
    }
    while (*text == '&' || isspace((unsigned char)*text)) {
        text++;
    }
    if (strncmp(text, "mut ", 4) == 0) {
        text += 4;
        while (isspace((unsigned char)*text)) {
            text++;
        }
    }
    /* Also strip a single explicit lifetime: `'a ` */
    if (*text == '\'') {
        text++;
        while (*text && (isalnum((unsigned char)*text) || *text == '_')) {
            text++;
        }
        while (isspace((unsigned char)*text)) {
            text++;
        }
    }
    return text;
}

/* Resolve a Rust *path expression* (e.g. `Foo::bar` or `crate::x::y`)
 * into a canonical QN. The resolver cascades through these rules,
 * matching what `rust-analyzer`'s name resolver does at the path level:
 *
 *   1.  `Self::X` → `<self_type_qn>.X`
 *   2.  `crate::a::b` → `<root_module_qn>.a.b`
 *   3.  `super::a` → strip last segment of `module_qn` and prepend
 *   4.  Single-segment + matches a `use` local-name → `<full path>.X`
 *   5.  Multi-segment whose first segment is a `use` local → splice
 *   6.  Falls through unchanged (caller decides what to do).
 *
 * The returned string is arena-owned; in case (6) we return the input
 * with `::` already converted to `.`. */
static const char *rust_resolve_path_expr(RustLSPContext *ctx, const char *path) {
    if (!ctx || !path || !path[0]) {
        return path;
    }

    /* Self:: handling — we treat the receiver type's QN as the head. */
    if (strncmp(path, "Self::", 6) == 0 && ctx->self_type_qn) {
        return cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->self_type_qn,
                                 convert_path_to_qn(ctx->arena, path + 6));
    }
    if (strcmp(path, "Self") == 0 && ctx->self_type_qn) {
        return ctx->self_type_qn;
    }

    /* crate:: → <root>. We approximate the crate root as the first dotted
     * segment of `module_qn` after the project prefix. The pipeline
     * forms `module_qn` as `<project>.<crate>.<rel-path-segments>`, so
     * the first two segments are project + crate root. */
    if (strncmp(path, "crate::", 7) == 0 && ctx->module_qn) {
        const char *p = ctx->module_qn;
        int dots = 0;
        const char *second_dot = NULL;
        for (; *p; p++) {
            if (*p == '.') {
                if (++dots == 2) {
                    second_dot = p;
                    break;
                }
            }
        }
        size_t crate_len =
            second_dot ? (size_t)(second_dot - ctx->module_qn) : strlen(ctx->module_qn);
        char *crate_buf = cbm_arena_strndup(ctx->arena, ctx->module_qn, crate_len);
        return cbm_arena_sprintf(ctx->arena, "%s.%s", crate_buf,
                                 convert_path_to_qn(ctx->arena, path + 7));
    }

    /* super:: → drop last segment of module_qn. */
    if (strncmp(path, "super::", 7) == 0 && ctx->module_qn) {
        const char *dot = strrchr(ctx->module_qn, '.');
        if (dot) {
            char *parent =
                cbm_arena_strndup(ctx->arena, ctx->module_qn, (size_t)(dot - ctx->module_qn));
            return cbm_arena_sprintf(ctx->arena, "%s.%s", parent,
                                     convert_path_to_qn(ctx->arena, path + 7));
        }
    }

    /* Find first "::" — split into head + tail. */
    const char *sep = strstr(path, "::");
    if (!sep) {
        const char *full = rust_resolve_use(ctx, path);
        if (full) {
            return convert_path_to_qn(ctx->arena, full);
        }
        /* Prelude name (e.g. `String`, `Vec`)? */
        const char *prelude = rust_lookup_prelude(path);
        if (prelude) {
            return prelude;
        }
        /* Bare identifier — assume same module. */
        if (ctx->module_qn) {
            return cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, path);
        }
        return path;
    }

    char *head = cbm_arena_strndup(ctx->arena, path, (size_t)(sep - path));
    const char *tail = sep + 2;

    const char *full = rust_resolve_use(ctx, head);
    if (full) {
        /* The use-map's full path already includes `head` as the last
         * segment; concat its parent with the rest. */
        const char *full_dotted = convert_path_to_qn(ctx->arena, full);
        const char *tail_dotted = convert_path_to_qn(ctx->arena, tail);
        return cbm_arena_sprintf(ctx->arena, "%s.%s", full_dotted, tail_dotted);
    }

    /* Prelude head: `String::from` → `alloc.string.String.from`. */
    const char *prelude = rust_lookup_prelude(head);
    if (prelude) {
        return cbm_arena_sprintf(ctx->arena, "%s.%s", prelude,
                                 convert_path_to_qn(ctx->arena, tail));
    }

    /* Cargo-manifest aware routing — when a Cargo.toml has been parsed
     * and the path head matches either a declared dependency or a
     * workspace member, return the canonical form `<head>.<tail>` so
     * the resolver doesn't pollute the module-prefix space. */
    if (ctx->cargo_manifest) {
        const CBMCargoManifest *m = (const CBMCargoManifest *)ctx->cargo_manifest;
        const CBMCargoMember *mem = cbm_cargo_find_member(m, head);
        if (mem) {
            /* Workspace member: route to `<member_name>.<tail>` so the
             * pipeline's cross-crate resolution can match it. */
            return cbm_arena_sprintf(ctx->arena, "%s.%s", head,
                                     convert_path_to_qn(ctx->arena, tail));
        }
        if (cbm_cargo_is_known_dep(m, head)) {
            /* Declared dependency: same canonical form. The actual
             * methods may have been pre-seeded by rust_crates_seed.c;
             * otherwise the call is correctly attributed to an
             * external crate rather than fabricated locally. */
            return cbm_arena_sprintf(ctx->arena, "%s.%s", head,
                                     convert_path_to_qn(ctx->arena, tail));
        }
    }

    /* Treat unknown-head paths as absolute: `std::io::Read` → `std.io.Read`. */
    return convert_path_to_qn(ctx->arena, path);
}

/* ════════════════════════════════════════════════════════════════════
 * 4. Type-AST → CBMType
 * ════════════════════════════════════════════════════════════════════ */

/* Reconstruct the textual Rust path under a `scoped_type_identifier` /
 * `scoped_identifier` node. We deliberately walk the named children
 * rather than using the literal source text so we do not preserve
 * whitespace or trailing turbofish noise. */
static char *gather_scoped_path(RustLSPContext *ctx, TSNode node) {
    /* Fall back to the raw source text — the grammar already produces a
     * tight `path::to::name` literal under the node. */
    return rust_node_text(ctx, node);
}

/* Resolve a textual Rust path (with `::`) into a registered type's QN, or
 * NULL if no match. */
static const char *resolve_path_to_type_qn(RustLSPContext *ctx, const char *path) {
    if (!ctx || !path || !path[0]) {
        return NULL;
    }
    if (is_rust_primitive(path)) {
        return NULL;
    }
    const char *qn = rust_resolve_path_expr(ctx, path);
    if (!qn) {
        return NULL;
    }
    if (cbm_registry_lookup_type(ctx->registry, qn)) {
        return qn;
    }
    return qn; /* may not be registered yet but caller can still wrap as NAMED */
}

const CBMType *rust_parse_type_node(RustLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node)) {
        return cbm_type_unknown();
    }
    const char *kind = ts_node_type(node);

    /* primitive_type: i32, bool, char, str, … */
    if (strcmp(kind, "primitive_type") == 0) {
        char *name = rust_node_text(ctx, node);
        if (!name) {
            return cbm_type_unknown();
        }
        return cbm_type_builtin(ctx->arena, name);
    }

    /* type_identifier: simple named type */
    if (strcmp(kind, "type_identifier") == 0) {
        char *name = rust_node_text(ctx, node);
        if (!name) {
            return cbm_type_unknown();
        }
        if (is_rust_primitive(name)) {
            return cbm_type_builtin(ctx->arena, name);
        }
        if (strcmp(name, "Self") == 0 && ctx->self_type_qn) {
            return cbm_type_named(ctx->arena, ctx->self_type_qn);
        }
        const char *qn = rust_resolve_path_expr(ctx, name);
        return cbm_type_named(ctx->arena, qn);
    }

    /* scoped_type_identifier: A::B::C */
    if (strcmp(kind, "scoped_type_identifier") == 0) {
        char *path = gather_scoped_path(ctx, node);
        if (!path) {
            return cbm_type_unknown();
        }
        const char *qn = rust_resolve_path_expr(ctx, path);
        return cbm_type_named(ctx->arena, qn);
    }

    /* reference_type: &T or &mut T */
    if (strcmp(kind, "reference_type") == 0) {
        TSNode inner = ts_node_child_by_field_name(node, "type", 4);
        if (ts_node_is_null(inner)) {
            uint32_t nc = ts_node_named_child_count(node);
            if (nc > 0) {
                inner = ts_node_named_child(node, nc - 1);
            }
        }
        const CBMType *elem = rust_parse_type_node(ctx, inner);
        return cbm_type_reference(ctx->arena, elem);
    }

    /* pointer_type: *const T / *mut T */
    if (strcmp(kind, "pointer_type") == 0) {
        TSNode inner = ts_node_child_by_field_name(node, "type", 4);
        if (ts_node_is_null(inner)) {
            uint32_t nc = ts_node_named_child_count(node);
            if (nc > 0) {
                inner = ts_node_named_child(node, nc - 1);
            }
        }
        return cbm_type_pointer(ctx->arena, rust_parse_type_node(ctx, inner));
    }

    /* array_type: [T; N] — treated as slice T */
    if (strcmp(kind, "array_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem)) {
            elem = ts_node_child_by_field_name(node, "type", 4);
        }
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0) {
            elem = ts_node_named_child(node, 0);
        }
        return cbm_type_slice(ctx->arena, rust_parse_type_node(ctx, elem));
    }

    /* slice_type: [T] */
    if (strcmp(kind, "slice_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0) {
            elem = ts_node_named_child(node, 0);
        }
        return cbm_type_slice(ctx->arena, rust_parse_type_node(ctx, elem));
    }

    /* tuple_type: (T1, T2, …) */
    if (strcmp(kind, "tuple_type") == 0) {
        const CBMType *elems[16];
        int count = 0;
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc && count < 16; i++) {
            elems[count++] = rust_parse_type_node(ctx, ts_node_named_child(node, i));
        }
        if (count == 0) {
            return cbm_type_builtin(ctx->arena, "()");
        }
        if (count == 1) {
            return elems[0];
        }
        return cbm_type_tuple(ctx->arena, elems, count);
    }

    /* unit_type: () */
    if (strcmp(kind, "unit_type") == 0) {
        return cbm_type_builtin(ctx->arena, "()");
    }

    /* never_type: ! */
    if (strcmp(kind, "never_type") == 0) {
        return cbm_type_builtin(ctx->arena, "!");
    }

    /* generic_type: Foo<T1, T2, …> */
    if (strcmp(kind, "generic_type") == 0) {
        TSNode tn = ts_node_child_by_field_name(node, "type", 4);
        if (ts_node_is_null(tn) && ts_node_named_child_count(node) > 0) {
            tn = ts_node_named_child(node, 0);
        }
        char *head = rust_node_text(ctx, tn);
        if (!head) {
            return cbm_type_unknown();
        }
        const char *head_qn = rust_resolve_path_expr(ctx, head);

        /* Gather type_arguments. */
        TSNode args = ts_node_child_by_field_name(node, "type_arguments", 14);
        const CBMType *targs[16];
        int targ_count = 0;
        if (!ts_node_is_null(args)) {
            uint32_t anc = ts_node_named_child_count(args);
            for (uint32_t i = 0; i < anc && targ_count < 15; i++) {
                TSNode tc = ts_node_named_child(args, i);
                const char *tk = ts_node_type(tc);
                /* Skip lifetime arguments — we ignore lifetimes entirely. */
                if (strcmp(tk, "lifetime") == 0) {
                    continue;
                }
                targs[targ_count++] = rust_parse_type_node(ctx, tc);
            }
        }
        if (targ_count > 0) {
            return cbm_type_template(ctx->arena, head_qn, targs, targ_count);
        }
        return cbm_type_named(ctx->arena, head_qn);
    }

    /* function_type: fn(T1, T2) -> R */
    if (strcmp(kind, "function_type") == 0) {
        return cbm_type_func(ctx->arena, NULL, NULL, NULL);
    }

    /* dynamic_type: dyn Trait — record as named on the trait QN */
    if (strcmp(kind, "dynamic_type") == 0) {
        TSNode inner = ts_node_child_by_field_name(node, "trait", 5);
        if (ts_node_is_null(inner) && ts_node_named_child_count(node) > 0) {
            inner = ts_node_named_child(node, 0);
        }
        return rust_parse_type_node(ctx, inner);
    }

    /* abstract_type: impl Trait — best-effort same as dyn Trait */
    if (strcmp(kind, "abstract_type") == 0) {
        TSNode inner = ts_node_child_by_field_name(node, "trait", 5);
        if (ts_node_is_null(inner) && ts_node_named_child_count(node) > 0) {
            inner = ts_node_named_child(node, 0);
        }
        return rust_parse_type_node(ctx, inner);
    }

    /* bounded_type: T + Trait + 'a — take the first child */
    if (strcmp(kind, "bounded_type") == 0 && ts_node_named_child_count(node) > 0) {
        return rust_parse_type_node(ctx, ts_node_named_child(node, 0));
    }

    /* parenthesized_type or wrapped types */
    if (strcmp(kind, "parenthesized_type") == 0 && ts_node_named_child_count(node) > 0) {
        return rust_parse_type_node(ctx, ts_node_named_child(node, 0));
    }

    /* qualified_type: <T as Trait>::Item */
    if (strcmp(kind, "qualified_type") == 0) {
        return cbm_type_unknown();
    }

    return cbm_type_unknown();
}

/* Parse a textual Rust type (`Vec<String>`, `&mut Foo`, `Result<T, E>`)
 * into a CBMType. Used when we receive types as strings (return types of
 * extracted `CBMDefinition`s, cross-file `CBMRustLSPDef::return_types`,
 * stdlib seed entries).
 *
 * The parser is intentionally simple: it recognises the small surface
 * area that tree-sitter would produce in `rust_parse_type_node` but
 * without a parser. This is the same trade-off `cbm_rust_parse_return_type_text`
 * makes for Go. */
static const CBMType *parse_type_text_with_params(CBMArena *arena, const char *text,
                                                  const char *module_qn, const char **type_params) {
    if (!text || !text[0]) {
        return cbm_type_unknown();
    }
    /* Skip leading whitespace + lifetime + mut markers. */
    while (*text == ' ' || *text == '\t') {
        text++;
    }

    /* HRTB: `for<'a, 'b> Fn(&'a T) -> R` — strip the higher-rank
     * binder. We don't reason about explicit lifetimes anywhere, so
     * dropping it leaves the rest of the type untouched. */
    if (strncmp(text, "for<", 4) == 0) {
        const char *p = text + 4;
        int depth = 1;
        while (*p && depth > 0) {
            if (*p == '<')
                depth++;
            else if (*p == '>')
                depth--;
            p++;
        }
        while (*p == ' ')
            p++;
        text = p;
        if (!*text)
            return cbm_type_unknown();
    }

    /* Bare leading lifetime (e.g. `'a`) — accept and skip, treating
     * the rest of the text as the actual type. Rare outside of HRTBs
     * but cheap to handle. */
    if (text[0] == '\'' && (isalpha((unsigned char)text[1]) || text[1] == '_')) {
        const char *p = text + 1;
        while (*p && (isalnum((unsigned char)*p) || *p == '_'))
            p++;
        while (*p == ' ')
            p++;
        text = p;
        if (!*text)
            return cbm_type_unknown();
    }

    /* Reference: &T or &'a T or &mut T or &'a mut T */
    if (text[0] == '&') {
        const char *p = text + 1;
        if (*p == '\'') {
            p++;
            while (*p && (isalnum((unsigned char)*p) || *p == '_')) {
                p++;
            }
        }
        while (*p == ' ') {
            p++;
        }
        if (strncmp(p, "mut ", 4) == 0) {
            p += 4;
        }
        const CBMType *elem = parse_type_text_with_params(arena, p, module_qn, type_params);
        return cbm_type_reference(arena, elem);
    }

    /* Pointer: *const T / *mut T */
    if (text[0] == '*') {
        const char *p = text + 1;
        if (strncmp(p, "const ", 6) == 0) {
            p += 6;
        } else if (strncmp(p, "mut ", 4) == 0) {
            p += 4;
        }
        return cbm_type_pointer(arena,
                                parse_type_text_with_params(arena, p, module_qn, type_params));
    }

    /* Slice: [T] */
    if (text[0] == '[' && text[strlen(text) - 1] == ']') {
        const char *p = text + 1;
        const char *end = text + strlen(text) - 1;
        size_t inner_len = (size_t)(end - p);
        char *inner = cbm_arena_strndup(arena, p, inner_len);
        /* Array form `[T; N]` — strip the count. */
        char *semi = strchr(inner, ';');
        if (semi) {
            *semi = '\0';
            /* Trim trailing whitespace. */
            char *q = semi - 1;
            while (q > inner && isspace((unsigned char)*q)) {
                *q-- = '\0';
            }
        }
        return cbm_type_slice(arena,
                              parse_type_text_with_params(arena, inner, module_qn, type_params));
    }

    /* Unit / never */
    if (strcmp(text, "()") == 0) {
        return cbm_type_builtin(arena, "()");
    }
    if (strcmp(text, "!") == 0) {
        return cbm_type_builtin(arena, "!");
    }

    /* Tuple: (T1, T2, …) — only when not a single parenthesised type. */
    if (text[0] == '(' && text[strlen(text) - 1] == ')') {
        const char *p = text + 1;
        size_t inner_len = strlen(text) - 2;
        char *inner = cbm_arena_strndup(arena, p, inner_len);
        /* Detect comma at top level. */
        int depth = 0;
        bool has_comma = false;
        for (char *q = inner; *q; q++) {
            if (*q == '<' || *q == '(' || *q == '[')
                depth++;
            else if (*q == '>' || *q == ')' || *q == ']')
                depth--;
            else if (*q == ',' && depth == 0) {
                has_comma = true;
                break;
            }
        }
        if (!has_comma) {
            return parse_type_text_with_params(arena, inner, module_qn, type_params);
        }
        /* Split by top-level commas. */
        const CBMType *elems[16];
        int count = 0;
        char *start = inner;
        depth = 0;
        for (char *q = inner;; q++) {
            if (*q == '<' || *q == '(' || *q == '[')
                depth++;
            else if (*q == '>' || *q == ')' || *q == ']')
                depth--;
            if ((*q == ',' && depth == 0) || *q == '\0') {
                char save = *q;
                *q = '\0';
                /* Trim. */
                while (*start == ' ')
                    start++;
                if (count < 15 && *start) {
                    elems[count++] =
                        parse_type_text_with_params(arena, start, module_qn, type_params);
                }
                if (save == '\0')
                    break;
                start = q + 1;
            }
        }
        if (count == 0)
            return cbm_type_builtin(arena, "()");
        if (count == 1)
            return elems[0];
        return cbm_type_tuple(arena, elems, count);
    }

    /* Generic head: head<args> */
    const char *lt = strchr(text, '<');
    if (lt) {
        const char *gt = text + strlen(text) - 1;
        if (*gt == '>') {
            size_t head_len = (size_t)(lt - text);
            char *head = cbm_arena_strndup(arena, text, head_len);
            /* Recursive split of args by top-level commas. */
            const char *args = lt + 1;
            size_t args_len = (size_t)(gt - args);
            char *abuf = cbm_arena_strndup(arena, args, args_len);
            const CBMType *targs[16];
            int targ_count = 0;
            int depth = 0;
            char *start = abuf;
            for (char *q = abuf;; q++) {
                if (*q == '<' || *q == '(' || *q == '[')
                    depth++;
                else if (*q == '>' || *q == ')' || *q == ']')
                    depth--;
                if ((*q == ',' && depth == 0) || *q == '\0') {
                    char save = *q;
                    *q = '\0';
                    while (*start == ' ')
                        start++;
                    /* Skip lifetime args. */
                    if (*start != '\'' && *start && targ_count < 15) {
                        targs[targ_count++] =
                            parse_type_text_with_params(arena, start, module_qn, type_params);
                    }
                    if (save == '\0')
                        break;
                    start = q + 1;
                }
            }
            const char *head_qn = head;
            if (is_rust_primitive(head)) {
                /* Primitives don't take generics in practice except for str ref — pass through. */
                return cbm_type_builtin(arena, head);
            }
            /* Map a few well-known std type sugars. */
            return cbm_type_template(arena, head_qn, targs, targ_count);
        }
    }

    /* Bare identifier or path. */
    if (is_rust_primitive(text)) {
        return cbm_type_builtin(arena, text);
    }
    if (type_params) {
        for (int i = 0; type_params[i]; i++) {
            if (strcmp(text, type_params[i]) == 0) {
                return cbm_type_type_param(arena, text);
            }
        }
    }
    /* Self -> module-qualified placeholder caller will substitute. */
    if (strcmp(text, "Self") == 0) {
        return cbm_type_named(arena, "Self");
    }
    /* Has `::` → absolute path; treat dotted paths as already-qualified
     * QNs (cross-file callers pass module-qualified text directly). */
    if (strstr(text, "::")) {
        return cbm_type_named(arena, convert_path_to_qn(arena, text));
    }
    if (strchr(text, '.')) {
        return cbm_type_named(arena, text);
    }
    return cbm_type_named(arena, cbm_arena_sprintf(arena, "%s.%s", module_qn, text));
}

/* Public-ish helper used by the cross-file path. */
static const CBMType *rust_parse_return_type_text(CBMArena *arena, const char *text,
                                                  const char *module_qn) {
    return parse_type_text_with_params(arena, text, module_qn, NULL);
}

/* ════════════════════════════════════════════════════════════════════
 * 5. Generic substitution
 * ════════════════════════════════════════════════════════════════════ */

/* Recursively substitute every `TYPE_PARAM` reference in `t` whose name
 * matches `params[i]` with `args[i]`. Preserves structure for composite
 * types. */
static const CBMType *rust_substitute_type(CBMArena *arena, const CBMType *t, const char **params,
                                           const CBMType **args) {
    if (!t || !params || !args) {
        return t;
    }
    switch (t->kind) {
    case CBM_TYPE_TYPE_PARAM:
        for (int i = 0; params[i]; i++) {
            if (strcmp(t->data.type_param.name, params[i]) == 0) {
                return args[i];
            }
        }
        return t;
    case CBM_TYPE_REFERENCE:
        return cbm_type_reference(
            arena, rust_substitute_type(arena, t->data.reference.elem, params, args));
    case CBM_TYPE_POINTER:
        return cbm_type_pointer(arena,
                                rust_substitute_type(arena, t->data.pointer.elem, params, args));
    case CBM_TYPE_SLICE:
        return cbm_type_slice(arena, rust_substitute_type(arena, t->data.slice.elem, params, args));
    case CBM_TYPE_TEMPLATE: {
        const CBMType *new_args[16];
        int n = 0;
        for (; n < t->data.template_type.arg_count && n < 16; n++) {
            new_args[n] =
                rust_substitute_type(arena, t->data.template_type.template_args[n], params, args);
        }
        return cbm_type_template(arena, t->data.template_type.template_name, new_args, n);
    }
    case CBM_TYPE_TUPLE: {
        const CBMType *new_elems[16];
        int n = 0;
        for (; n < t->data.tuple.count && n < 16; n++) {
            new_elems[n] = rust_substitute_type(arena, t->data.tuple.elems[n], params, args);
        }
        return cbm_type_tuple(arena, new_elems, n);
    }
    default:
        return t;
    }
}

/* Naive Hindley-Milner-style type unification. Walks `param_type`
 * structurally against `arg_type`; whenever a `TYPE_PARAM` is bound for
 * the first time, store the corresponding `arg_type`. Subsequent
 * conflicting bindings are ignored (best-effort). */
static void rust_unify_type(const CBMType *param_type, const CBMType *arg_type,
                            const char **type_param_names, const CBMType **inferred,
                            int param_count) {
    if (!param_type || !arg_type || cbm_type_is_unknown(arg_type)) {
        return;
    }
    if (param_type->kind == CBM_TYPE_TYPE_PARAM) {
        for (int i = 0; i < param_count; i++) {
            if (strcmp(param_type->data.type_param.name, type_param_names[i]) == 0) {
                if (!inferred[i]) {
                    inferred[i] = arg_type;
                }
                return;
            }
        }
        return;
    }
    if (param_type->kind == CBM_TYPE_REFERENCE && arg_type->kind == CBM_TYPE_REFERENCE) {
        rust_unify_type(param_type->data.reference.elem, arg_type->data.reference.elem,
                        type_param_names, inferred, param_count);
        return;
    }
    if (param_type->kind == CBM_TYPE_REFERENCE) {
        rust_unify_type(param_type->data.reference.elem, arg_type, type_param_names, inferred,
                        param_count);
        return;
    }
    if (param_type->kind == CBM_TYPE_SLICE && arg_type->kind == CBM_TYPE_SLICE) {
        rust_unify_type(param_type->data.slice.elem, arg_type->data.slice.elem, type_param_names,
                        inferred, param_count);
        return;
    }
    if (param_type->kind == CBM_TYPE_TEMPLATE && arg_type->kind == CBM_TYPE_TEMPLATE) {
        if (param_type->data.template_type.arg_count == arg_type->data.template_type.arg_count) {
            int ac = param_type->data.template_type.arg_count;
            for (int i = 0; i < ac; i++) {
                rust_unify_type(param_type->data.template_type.template_args[i],
                                arg_type->data.template_type.template_args[i], type_param_names,
                                inferred, param_count);
            }
        }
        return;
    }
    /* TUPLE unification — needed for tuple-return generics like
     * `fn pair<A, B>(a: A, b: B) -> (A, B)`. */
    if (param_type->kind == CBM_TYPE_TUPLE && arg_type->kind == CBM_TYPE_TUPLE) {
        int pc = param_type->data.tuple.count;
        int ac = arg_type->data.tuple.count;
        int min_ = pc < ac ? pc : ac;
        for (int i = 0; i < min_; i++) {
            rust_unify_type(param_type->data.tuple.elems[i], arg_type->data.tuple.elems[i],
                            type_param_names, inferred, param_count);
        }
        return;
    }
    /* POINTER unification. */
    if (param_type->kind == CBM_TYPE_POINTER && arg_type->kind == CBM_TYPE_POINTER) {
        rust_unify_type(param_type->data.pointer.elem, arg_type->data.pointer.elem,
                        type_param_names, inferred, param_count);
        return;
    }
    /* Bidirectional fallback: if `arg_type` (rather than `param_type`)
     * carries the type-param marker, swap and retry. This lets
     * `unify(known_concrete, fresh_var)` solve the var. */
    if (arg_type->kind == CBM_TYPE_TYPE_PARAM) {
        rust_unify_type(arg_type, param_type, type_param_names, inferred, param_count);
        return;
    }
}

/* Apply a solved type-param environment to a type, recursively
 * substituting bound param names with their concrete types. Returns
 * the substituted type (arena-allocated when new structure is built).
 *
 * This is the post-solve step of HM-lite: after `rust_unify_type` has
 * filled the `inferred` array, this helper walks a target type and
 * rewrites every `TYPE_PARAM` reference. */
static const CBMType *rust_apply_subst(CBMArena *arena, const CBMType *t, const char **names,
                                       const CBMType **inferred, int count) {
    if (!t)
        return t;
    switch (t->kind) {
    case CBM_TYPE_TYPE_PARAM:
        for (int i = 0; i < count; i++) {
            if (inferred[i] && names[i] && strcmp(t->data.type_param.name, names[i]) == 0) {
                return inferred[i];
            }
        }
        return t;
    case CBM_TYPE_REFERENCE:
        return cbm_type_reference(
            arena, rust_apply_subst(arena, t->data.reference.elem, names, inferred, count));
    case CBM_TYPE_POINTER:
        return cbm_type_pointer(
            arena, rust_apply_subst(arena, t->data.pointer.elem, names, inferred, count));
    case CBM_TYPE_SLICE:
        return cbm_type_slice(arena,
                              rust_apply_subst(arena, t->data.slice.elem, names, inferred, count));
    case CBM_TYPE_TEMPLATE: {
        int n = t->data.template_type.arg_count;
        if (n <= 0)
            return t;
        const CBMType *new_args[16];
        if (n > 16)
            n = 16;
        for (int i = 0; i < n; i++) {
            new_args[i] = rust_apply_subst(arena, t->data.template_type.template_args[i], names,
                                           inferred, count);
        }
        return cbm_type_template(arena, t->data.template_type.template_name, new_args, n);
    }
    case CBM_TYPE_TUPLE: {
        int n = t->data.tuple.count;
        if (n <= 0)
            return t;
        const CBMType *new_elems[16];
        if (n > 16)
            n = 16;
        for (int i = 0; i < n; i++) {
            new_elems[i] = rust_apply_subst(arena, t->data.tuple.elems[i], names, inferred, count);
        }
        return cbm_type_tuple(arena, new_elems, n);
    }
    default:
        return t;
    }
}

/* ════════════════════════════════════════════════════════════════════
 * 6. Expression evaluator
 * ════════════════════════════════════════════════════════════════════ */

/* Evaluate the type of a literal child like `integer_literal`, `float_literal`,
 * `string_literal`, `char_literal`, `boolean_literal`. */
static const CBMType *rust_eval_literal_type(RustLSPContext *ctx, const char *kind, TSNode node) {
    if (strcmp(kind, "integer_literal") == 0) {
        char *text = rust_node_text(ctx, node);
        /* Look at suffix. */
        if (text) {
            /* Strip leading `-` if any. */
            const char *p = text;
            if (*p == '-')
                p++;
            /* Find suffix start (first non-digit/non-_/non-x/non-X/non-hex). */
            while (*p && (isdigit((unsigned char)*p) || *p == '_' || *p == '.' || *p == 'x' ||
                          *p == 'X' || *p == 'b' || *p == 'B' || *p == 'o' || *p == 'O' ||
                          (*p >= 'a' && *p <= 'f') || (*p >= 'A' && *p <= 'F'))) {
                p++;
            }
            if (*p) {
                return cbm_type_builtin(ctx->arena, p);
            }
        }
        return cbm_type_builtin(ctx->arena, "i32");
    }
    if (strcmp(kind, "float_literal") == 0) {
        char *text = rust_node_text(ctx, node);
        if (text) {
            const char *p = text;
            while (*p && (isdigit((unsigned char)*p) || *p == '.' || *p == 'e' || *p == 'E' ||
                          *p == '+' || *p == '-' || *p == '_')) {
                p++;
            }
            if (*p) {
                return cbm_type_builtin(ctx->arena, p);
            }
        }
        return cbm_type_builtin(ctx->arena, "f64");
    }
    if (strcmp(kind, "string_literal") == 0 || strcmp(kind, "raw_string_literal") == 0) {
        /* &'static str — represented as &str */
        return cbm_type_reference(ctx->arena, cbm_type_builtin(ctx->arena, "str"));
    }
    if (strcmp(kind, "char_literal") == 0) {
        return cbm_type_builtin(ctx->arena, "char");
    }
    if (strcmp(kind, "boolean_literal") == 0 || strcmp(kind, "true") == 0 ||
        strcmp(kind, "false") == 0) {
        return cbm_type_builtin(ctx->arena, "bool");
    }
    return cbm_type_unknown();
}

/* Look up the registered method or field type for a field/method-style
 * access. Order: inherent method → field → trait method (with single-impl
 * preference). */
static const CBMType *rust_eval_member_access(RustLSPContext *ctx, const CBMType *recv,
                                              const char *member);

const CBMType *rust_eval_expr_type(RustLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node)) {
        return cbm_type_unknown();
    }
    const char *kind = ts_node_type(node);

    /* Identifier: scope or registered symbol. */
    if (strcmp(kind, "identifier") == 0) {
        char *name = rust_node_text(ctx, node);
        if (!name) {
            return cbm_type_unknown();
        }
        const CBMType *t = cbm_scope_lookup(ctx->current_scope, name);
        if (!cbm_type_is_unknown(t)) {
            return t;
        }
        /* Module-level function. */
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, name);
        if (f && f->signature) {
            return f->signature;
        }
        /* Use-aliased symbol: resolve path then look up. */
        const char *full = rust_resolve_use(ctx, name);
        if (full) {
            const char *qn = convert_path_to_qn(ctx->arena, full);
            const CBMRegisteredFunc *uf = cbm_registry_lookup_func(ctx->registry, qn);
            if (uf && uf->signature) {
                return uf->signature;
            }
            const CBMRegisteredType *ut = cbm_registry_lookup_type(ctx->registry, qn);
            if (ut) {
                return cbm_type_named(ctx->arena, qn);
            }
        }
        /* Same-module type? */
        const char *type_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name);
        if (cbm_registry_lookup_type(ctx->registry, type_qn)) {
            return cbm_type_named(ctx->arena, type_qn);
        }
        return cbm_type_unknown();
    }

    /* self_parameter token: bound in scope as `self`. */
    if (strcmp(kind, "self") == 0 || strcmp(kind, "self_parameter") == 0) {
        return cbm_scope_lookup(ctx->current_scope, "self");
    }

    /* scoped_identifier: A::B::C — could be a function or a type. */
    if (strcmp(kind, "scoped_identifier") == 0) {
        char *path = rust_node_text(ctx, node);
        if (!path) {
            return cbm_type_unknown();
        }
        const char *qn = rust_resolve_path_expr(ctx, path);
        if (!qn) {
            return cbm_type_unknown();
        }
        const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, qn);
        if (f && f->signature) {
            return f->signature;
        }
        if (cbm_registry_lookup_type(ctx->registry, qn)) {
            return cbm_type_named(ctx->arena, qn);
        }
        return cbm_type_unknown();
    }

    /* generic_function: foo::<T> */
    if (strcmp(kind, "generic_function") == 0) {
        TSNode fn = ts_node_child_by_field_name(node, "function", 8);
        if (!ts_node_is_null(fn)) {
            return rust_eval_expr_type(ctx, fn);
        }
    }

    /* field_expression: obj.field or obj.0 (tuple) */
    if (strcmp(kind, "field_expression") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        TSNode field = ts_node_child_by_field_name(node, "field", 5);
        if (ts_node_is_null(value) || ts_node_is_null(field)) {
            return cbm_type_unknown();
        }
        const CBMType *recv = rust_eval_expr_type(ctx, value);
        const char *fk = ts_node_type(field);
        if (strcmp(fk, "integer_literal") == 0) {
            /* Tuple field access. */
            if (recv) {
                const CBMType *base = recv;
                while (base && base->kind == CBM_TYPE_REFERENCE) {
                    base = base->data.reference.elem;
                }
                if (base && base->kind == CBM_TYPE_TUPLE) {
                    char *idx_text = rust_node_text(ctx, field);
                    int idx = atoi(idx_text);
                    if (idx >= 0 && idx < base->data.tuple.count) {
                        return base->data.tuple.elems[idx];
                    }
                }
            }
            return cbm_type_unknown();
        }
        char *fname = rust_node_text(ctx, field);
        if (!fname) {
            return cbm_type_unknown();
        }
        return rust_eval_member_access(ctx, recv, fname);
    }

    /* call_expression: any callable invocation. */
    if (strcmp(kind, "call_expression") == 0) {
        TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
        TSNode args_node = ts_node_child_by_field_name(node, "arguments", 9);
        if (ts_node_is_null(func_node)) {
            return cbm_type_unknown();
        }
        const char *fk = ts_node_type(func_node);

        /* Constructor of a tuple struct, unit-like struct, or stdlib
         * factory invoked via path. Try the registry; on miss, also try
         * UFCS-style method lookup to catch `String::new()`,
         * `Vec::new()`, etc. */
        if (strcmp(fk, "identifier") == 0 || strcmp(fk, "scoped_identifier") == 0) {
            char *path = rust_node_text(ctx, func_node);
            if (path) {
                /* Strip ALL turbofish (`::<...>`) so the lookup below
                 * ignores explicit type arguments — handles forms like
                 * `Vec::<i32>::new` and `parse::<u32>`. */
                rust_strip_turbofish(path);
                const char *qn = rust_resolve_path_expr(ctx, path);
                if (qn) {
                    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, qn);
                    if (rt) {
                        return cbm_type_named(ctx->arena, qn);
                    }
                    const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, qn);
                    if (f && f->signature && f->signature->kind == CBM_TYPE_FUNC) {
                        const CBMType *const *rt_arr = f->signature->data.func.return_types;
                        if (rt_arr && rt_arr[0]) {
                            const CBMType *ret = rt_arr[0];
                            /* Constructor-style on a stdlib receiver with
                             * `unknown` return — substitute the receiver
                             * type so chains keep typing. The heuristic
                             * matches `new`, `default`, anything starting
                             * with `from_` / `with_`, plus a small list of
                             * common factory verbs.
                             *
                             * For smart-pointer factories (`Box::new`,
                             * `Rc::new`, `Arc::new`, `RefCell::new`,
                             * `Pin::new`, `Mutex::new`, `RwLock::new`)
                             * we also try to capture the first call
                             * argument's type as a TEMPLATE arg so the
                             * Deref chain has something to peel. */
                            if (f->receiver_type && cbm_type_is_unknown(ret) && f->short_name &&
                                (strcmp(f->short_name, "new") == 0 ||
                                 strcmp(f->short_name, "default") == 0 ||
                                 strcmp(f->short_name, "open") == 0 ||
                                 strcmp(f->short_name, "create") == 0 ||
                                 strcmp(f->short_name, "create_new") == 0 ||
                                 strcmp(f->short_name, "bind") == 0 ||
                                 strcmp(f->short_name, "connect") == 0 ||
                                 strcmp(f->short_name, "spawn") == 0 ||
                                 strcmp(f->short_name, "now") == 0 ||
                                 strncmp(f->short_name, "from_", 5) == 0 ||
                                 strncmp(f->short_name, "with_", 5) == 0 ||
                                 strcmp(f->short_name, "from") == 0)) {
                                /* Detect smart-pointer wrappers and
                                 * capture the first arg's type so
                                 * `let b = Box::new(x); b.method()`
                                 * dispatches via Deref. */
                                bool is_wrapper = strcmp(f->short_name, "new") == 0 &&
                                                  rust_type_derefs_to_first_arg(f->receiver_type);
                                if (is_wrapper && !ts_node_is_null(args_node)) {
                                    uint32_t anc = ts_node_named_child_count(args_node);
                                    if (anc > 0) {
                                        const CBMType *arg_t = rust_eval_expr_type(
                                            ctx, ts_node_named_child(args_node, 0));
                                        if (arg_t && !cbm_type_is_unknown(arg_t)) {
                                            return cbm_type_template(ctx->arena, f->receiver_type,
                                                                     &arg_t, 1);
                                        }
                                    }
                                }
                                return cbm_type_named(ctx->arena, f->receiver_type);
                            }
                            /* Self -> receiver_type substitution. */
                            if (f->receiver_type && ret->kind == CBM_TYPE_NAMED &&
                                strcmp(ret->data.named.qualified_name, "Self") == 0) {
                                return cbm_type_named(ctx->arena, f->receiver_type);
                            }
                            return ret;
                        }
                    }
                    /* UFCS path lookup: split off short name and try
                     * `cbm_registry_lookup_method`. */
                    const char *dot = strrchr(qn, '.');
                    if (dot) {
                        char *head = cbm_arena_strndup(ctx->arena, qn, (size_t)(dot - qn));
                        const char *short_name = dot + 1;
                        const CBMRegisteredFunc *m =
                            cbm_registry_lookup_method_aliased(ctx->registry, head, short_name);
                        if (!m && ctx->module_qn) {
                            const char *full_head =
                                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, head);
                            m = cbm_registry_lookup_method_aliased(ctx->registry, full_head,
                                                                   short_name);
                            if (m)
                                head = (char *)full_head;
                        }
                        if (m && m->signature && m->signature->kind == CBM_TYPE_FUNC &&
                            m->signature->data.func.return_types &&
                            m->signature->data.func.return_types[0]) {
                            const CBMType *ret = m->signature->data.func.return_types[0];
                            /* Substitute Self / unknown returns with the
                             * receiver type so chained calls keep typing. */
                            if (ret->kind == CBM_TYPE_NAMED &&
                                strcmp(ret->data.named.qualified_name, "Self") == 0) {
                                return cbm_type_named(ctx->arena, head);
                            }
                            if (cbm_type_is_unknown(ret) &&
                                (strcmp(short_name, "new") == 0 ||
                                 strcmp(short_name, "default") == 0 ||
                                 strcmp(short_name, "with_capacity") == 0 ||
                                 strcmp(short_name, "from") == 0)) {
                                /* Smart-pointer wrap: capture first arg
                                 * type into a TEMPLATE so Deref can peel. */
                                if (strcmp(short_name, "new") == 0 &&
                                    rust_type_derefs_to_first_arg(head) &&
                                    !ts_node_is_null(args_node)) {
                                    uint32_t anc = ts_node_named_child_count(args_node);
                                    if (anc > 0) {
                                        const CBMType *arg_t = rust_eval_expr_type(
                                            ctx, ts_node_named_child(args_node, 0));
                                        if (arg_t && !cbm_type_is_unknown(arg_t)) {
                                            return cbm_type_template(ctx->arena, head, &arg_t, 1);
                                        }
                                    }
                                }
                                return cbm_type_named(ctx->arena, head);
                            }
                            return ret;
                        }
                    }
                }
            }
        }

        /* Method call expressed as field_expression callee. */
        if (strcmp(fk, "field_expression") == 0) {
            TSNode value = ts_node_child_by_field_name(func_node, "value", 5);
            TSNode field = ts_node_child_by_field_name(func_node, "field", 5);
            if (!ts_node_is_null(value) && !ts_node_is_null(field)) {
                const CBMType *recv = rust_eval_expr_type(ctx, value);
                char *mname = rust_node_text(ctx, field);
                if (mname && recv) {
                    const CBMType *base = recv;
                    while (base && base->kind == CBM_TYPE_REFERENCE) {
                        base = base->data.reference.elem;
                    }
                    /* Template Vec<T> method handling */
                    if (base && base->kind == CBM_TYPE_TEMPLATE) {
                        const char *tname = base->data.template_type.template_name;
                        /* Iterator-producing methods on Vec/&[T]/Option/Result/HashMap. */
                        if (strstr(tname, "Vec") || strstr(tname, "VecDeque") ||
                            strstr(tname, "HashSet") || strstr(tname, "BTreeSet")) {
                            if (strcmp(mname, "iter") == 0 || strcmp(mname, "iter_mut") == 0 ||
                                strcmp(mname, "into_iter") == 0 || strcmp(mname, "drain") == 0) {
                                /* Iterator<Item=T> — represent loosely as the elem type for our
                                 * downstream chain calls. Even when the elem type is not
                                 * known, return Iterator (with no args) so further chain calls
                                 * can still attribute via Iterator's registered methods. */
                                if (base->data.template_type.arg_count > 0) {
                                    return cbm_type_template(
                                        ctx->arena, "core.iter.Iterator",
                                        &base->data.template_type.template_args[0], 1);
                                }
                                return cbm_type_template(ctx->arena, "core.iter.Iterator", NULL, 0);
                            }
                            /* Methods returning the element type directly. */
                            if (strcmp(mname, "remove") == 0 || strcmp(mname, "swap_remove") == 0) {
                                if (base->data.template_type.arg_count > 0) {
                                    return base->data.template_type.template_args[0];
                                }
                            }
                            if (strcmp(mname, "len") == 0 || strcmp(mname, "capacity") == 0) {
                                return cbm_type_builtin(ctx->arena, "usize");
                            }
                            if (strcmp(mname, "is_empty") == 0 || strcmp(mname, "contains") == 0) {
                                return cbm_type_builtin(ctx->arena, "bool");
                            }
                            if (strcmp(mname, "first") == 0 || strcmp(mname, "last") == 0 ||
                                strcmp(mname, "get") == 0 || strcmp(mname, "pop") == 0) {
                                if (base->data.template_type.arg_count > 0) {
                                    const CBMType *opt_args[1] = {
                                        base->data.template_type.template_args[0]};
                                    return cbm_type_template(ctx->arena, "core.option.Option",
                                                             opt_args, 1);
                                }
                            }
                            if (strcmp(mname, "as_slice") == 0) {
                                if (base->data.template_type.arg_count > 0) {
                                    return cbm_type_reference(
                                        ctx->arena,
                                        cbm_type_slice(ctx->arena,
                                                       base->data.template_type.template_args[0]));
                                }
                            }
                        }
                        if (strstr(tname, "Option")) {
                            if (strcmp(mname, "unwrap") == 0 || strcmp(mname, "expect") == 0 ||
                                strcmp(mname, "unwrap_or") == 0 ||
                                strcmp(mname, "unwrap_or_default") == 0 ||
                                strcmp(mname, "unwrap_or_else") == 0) {
                                if (base->data.template_type.arg_count > 0) {
                                    return base->data.template_type.template_args[0];
                                }
                            }
                            if (strcmp(mname, "is_some") == 0 || strcmp(mname, "is_none") == 0) {
                                return cbm_type_builtin(ctx->arena, "bool");
                            }
                            if (strcmp(mname, "as_ref") == 0) {
                                if (base->data.template_type.arg_count > 0) {
                                    const CBMType *arg0 = cbm_type_reference(
                                        ctx->arena, base->data.template_type.template_args[0]);
                                    return cbm_type_template(ctx->arena, "core.option.Option",
                                                             &arg0, 1);
                                }
                            }
                        }
                        if (strstr(tname, "Result")) {
                            if (strcmp(mname, "unwrap") == 0 || strcmp(mname, "expect") == 0 ||
                                strcmp(mname, "unwrap_or") == 0) {
                                if (base->data.template_type.arg_count > 0) {
                                    return base->data.template_type.template_args[0];
                                }
                            }
                            if (strcmp(mname, "ok") == 0 &&
                                base->data.template_type.arg_count > 0) {
                                const CBMType *a0 = base->data.template_type.template_args[0];
                                return cbm_type_template(ctx->arena, "core.option.Option", &a0, 1);
                            }
                            if (strcmp(mname, "err") == 0 &&
                                base->data.template_type.arg_count > 1) {
                                const CBMType *a1 = base->data.template_type.template_args[1];
                                return cbm_type_template(ctx->arena, "core.option.Option", &a1, 1);
                            }
                            if (strcmp(mname, "is_ok") == 0 || strcmp(mname, "is_err") == 0) {
                                return cbm_type_builtin(ctx->arena, "bool");
                            }
                        }
                        if (strstr(tname, "Iterator")) {
                            /* map/filter/take/skip/rev/chain → Iterator (with the relevant elem) */
                            if (strcmp(mname, "collect") == 0) {
                                /* Often Vec<Item>; without turbofish info we model as Vec<elem>. */
                                if (base->data.template_type.arg_count > 0) {
                                    return cbm_type_template(ctx->arena, "alloc.vec.Vec",
                                                             base->data.template_type.template_args,
                                                             base->data.template_type.arg_count);
                                }
                            }
                            if (strcmp(mname, "count") == 0 || strcmp(mname, "len") == 0) {
                                return cbm_type_builtin(ctx->arena, "usize");
                            }
                            if (strcmp(mname, "next") == 0 || strcmp(mname, "last") == 0 ||
                                strcmp(mname, "nth") == 0 || strcmp(mname, "find") == 0 ||
                                strcmp(mname, "max") == 0 || strcmp(mname, "min") == 0 ||
                                strcmp(mname, "max_by") == 0 || strcmp(mname, "min_by") == 0) {
                                if (base->data.template_type.arg_count > 0) {
                                    const CBMType *a0 = base->data.template_type.template_args[0];
                                    return cbm_type_template(ctx->arena, "core.option.Option", &a0,
                                                             1);
                                }
                            }
                            if (strcmp(mname, "filter") == 0 || strcmp(mname, "take") == 0 ||
                                strcmp(mname, "skip") == 0 || strcmp(mname, "rev") == 0 ||
                                strcmp(mname, "cloned") == 0 || strcmp(mname, "copied") == 0 ||
                                strcmp(mname, "step_by") == 0 || strcmp(mname, "fuse") == 0 ||
                                strcmp(mname, "peekable") == 0 || strcmp(mname, "by_ref") == 0 ||
                                strcmp(mname, "take_while") == 0 ||
                                strcmp(mname, "skip_while") == 0 || strcmp(mname, "inspect") == 0) {
                                return base; /* preserves Iterator<Item> */
                            }
                            if ((strcmp(mname, "sum") == 0 || strcmp(mname, "product") == 0 ||
                                 strcmp(mname, "fold") == 0 || strcmp(mname, "reduce") == 0) &&
                                base->data.template_type.arg_count > 0) {
                                return base->data.template_type.template_args[0];
                            }
                            if (strcmp(mname, "any") == 0 || strcmp(mname, "all") == 0) {
                                return cbm_type_builtin(ctx->arena, "bool");
                            }
                            if (strcmp(mname, "position") == 0) {
                                const CBMType *usize = cbm_type_builtin(ctx->arena, "usize");
                                return cbm_type_template(ctx->arena, "core.option.Option", &usize,
                                                         1);
                            }
                            if (strcmp(mname, "enumerate") == 0 &&
                                base->data.template_type.arg_count > 0) {
                                /* Iterator<(usize, T)>. */
                                const CBMType *pair[2] = {
                                    cbm_type_builtin(ctx->arena, "usize"),
                                    base->data.template_type.template_args[0]};
                                const CBMType *tup = cbm_type_tuple(ctx->arena, pair, 2);
                                return cbm_type_template(ctx->arena, "core.iter.Iterator", &tup, 1);
                            }
                        }
                        /* HashMap<K, V> / BTreeMap<K, V> generics. */
                        if (strstr(tname, "HashMap") || strstr(tname, "BTreeMap")) {
                            if (strcmp(mname, "len") == 0) {
                                return cbm_type_builtin(ctx->arena, "usize");
                            }
                            if (strcmp(mname, "is_empty") == 0 ||
                                strcmp(mname, "contains_key") == 0) {
                                return cbm_type_builtin(ctx->arena, "bool");
                            }
                            if (strcmp(mname, "get") == 0 || strcmp(mname, "get_mut") == 0 ||
                                strcmp(mname, "remove") == 0) {
                                if (base->data.template_type.arg_count > 1) {
                                    const CBMType *v = base->data.template_type.template_args[1];
                                    return cbm_type_template(ctx->arena, "core.option.Option", &v,
                                                             1);
                                }
                            }
                            if (strcmp(mname, "iter") == 0 || strcmp(mname, "iter_mut") == 0) {
                                /* Iterator<(K, V)>. */
                                if (base->data.template_type.arg_count > 1) {
                                    const CBMType *pair[2] = {
                                        base->data.template_type.template_args[0],
                                        base->data.template_type.template_args[1]};
                                    const CBMType *tup = cbm_type_tuple(ctx->arena, pair, 2);
                                    return cbm_type_template(ctx->arena, "core.iter.Iterator", &tup,
                                                             1);
                                }
                            }
                            if (strcmp(mname, "keys") == 0 &&
                                base->data.template_type.arg_count > 0) {
                                return cbm_type_template(ctx->arena, "core.iter.Iterator",
                                                         &base->data.template_type.template_args[0],
                                                         1);
                            }
                            if (strcmp(mname, "values") == 0 &&
                                base->data.template_type.arg_count > 1) {
                                return cbm_type_template(ctx->arena, "core.iter.Iterator",
                                                         &base->data.template_type.template_args[1],
                                                         1);
                            }
                        }
                    }
                    /* Fall through: registered method on the named type.
                     * Unwrap the resulting FUNC signature to its first
                     * return type so chains like
                     * `String::new().to_uppercase().len()` keep typing
                     * across each link. */
                    const CBMType *t = rust_eval_member_access(ctx, recv, mname);
                    if (t && t->kind == CBM_TYPE_FUNC && t->data.func.return_types &&
                        t->data.func.return_types[0]) {
                        const CBMType *ret = t->data.func.return_types[0];
                        /* Self -> receiver. */
                        if (ret->kind == CBM_TYPE_NAMED &&
                            strcmp(ret->data.named.qualified_name, "Self") == 0) {
                            const CBMType *rb = recv;
                            while (rb && rb->kind == CBM_TYPE_REFERENCE)
                                rb = rb->data.reference.elem;
                            if (rb && rb->kind == CBM_TYPE_NAMED) {
                                return cbm_type_named(ctx->arena, rb->data.named.qualified_name);
                            }
                        }
                        return ret;
                    }
                    return t;
                }
            }
        }

        /* Fallback: function expression's return type. */
        const CBMType *func_type = rust_eval_expr_type(ctx, func_node);
        if (func_type && func_type->kind == CBM_TYPE_FUNC && func_type->data.func.return_types &&
            func_type->data.func.return_types[0]) {
            return func_type->data.func.return_types[0];
        }
        if (func_type && func_type->kind == CBM_TYPE_NAMED) {
            return func_type;
        }
        return cbm_type_unknown();
    }

    /* macro_invocation: vec!, format!, … */
    if (strcmp(kind, "macro_invocation") == 0) {
        TSNode mname = ts_node_child_by_field_name(node, "macro", 5);
        if (ts_node_is_null(mname) && ts_node_named_child_count(node) > 0) {
            mname = ts_node_named_child(node, 0);
        }
        char *name = rust_node_text(ctx, mname);
        if (!name) {
            return cbm_type_unknown();
        }
        if (strcmp(name, "vec") == 0) {
            /* vec![T] — peek first argument's type. */
            TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
            if (!ts_node_is_null(args)) {
                /* args is a token_tree; skim its named children for the first
                 * expression token. */
                uint32_t nc = ts_node_named_child_count(args);
                for (uint32_t i = 0; i < nc; i++) {
                    TSNode c = ts_node_named_child(args, i);
                    const CBMType *elem = rust_eval_expr_type(ctx, c);
                    if (elem && !cbm_type_is_unknown(elem)) {
                        return cbm_type_template(ctx->arena, "alloc.vec.Vec", &elem, 1);
                    }
                }
            }
            return cbm_type_template(ctx->arena, "alloc.vec.Vec", NULL, 0);
        }
        if (is_string_macro(name)) {
            return cbm_type_named(ctx->arena, "alloc.string.String");
        }
        if (is_void_macro(name)) {
            return cbm_type_builtin(ctx->arena, "()");
        }
        return cbm_type_unknown();
    }

    /* reference_expression: &x or &mut x */
    if (strcmp(kind, "reference_expression") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(value) && ts_node_named_child_count(node) > 0) {
            value = ts_node_named_child(node, 0);
        }
        return cbm_type_reference(ctx->arena, rust_eval_expr_type(ctx, value));
    }

    /* unary_expression — *x dereferences, !x is bool, -x same as operand */
    if (strcmp(kind, "unary_expression") == 0) {
        TSNode operand = ts_node_named_child_count(node) > 0
                             ? ts_node_named_child(node, ts_node_named_child_count(node) - 1)
                             : (TSNode){0};
        char *op = NULL;
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode c = ts_node_child(node, i);
            if (!ts_node_is_named(c)) {
                op = rust_node_text(ctx, c);
                if (op)
                    break;
            }
        }
        const CBMType *inner =
            ts_node_is_null(operand) ? cbm_type_unknown() : rust_eval_expr_type(ctx, operand);
        if (op && strcmp(op, "*") == 0) {
            if (inner && inner->kind == CBM_TYPE_REFERENCE) {
                return inner->data.reference.elem;
            }
            if (inner && inner->kind == CBM_TYPE_POINTER) {
                return inner->data.pointer.elem;
            }
            return inner;
        }
        if (op && strcmp(op, "!") == 0) {
            return cbm_type_builtin(ctx->arena, "bool");
        }
        return inner;
    }

    /* binary_expression — comparisons → bool, logical → bool, arith → left */
    if (strcmp(kind, "binary_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_named(c))
                continue;
            char *op = rust_node_text(ctx, c);
            if (!op)
                continue;
            if (strcmp(op, "==") == 0 || strcmp(op, "!=") == 0 || strcmp(op, "<") == 0 ||
                strcmp(op, ">") == 0 || strcmp(op, "<=") == 0 || strcmp(op, ">=") == 0 ||
                strcmp(op, "&&") == 0 || strcmp(op, "||") == 0) {
                return cbm_type_builtin(ctx->arena, "bool");
            }
            break;
        }
        if (!ts_node_is_null(left)) {
            return rust_eval_expr_type(ctx, left);
        }
        return cbm_type_unknown();
    }

    /* index_expression */
    if (strcmp(kind, "index_expression") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(value) && ts_node_named_child_count(node) > 0) {
            value = ts_node_named_child(node, 0);
        }
        const CBMType *op_type = rust_eval_expr_type(ctx, value);
        const CBMType *base = op_type;
        while (base && base->kind == CBM_TYPE_REFERENCE)
            base = base->data.reference.elem;
        if (base && base->kind == CBM_TYPE_SLICE) {
            return base->data.slice.elem;
        }
        if (base && base->kind == CBM_TYPE_TEMPLATE) {
            const char *nm = base->data.template_type.template_name;
            if ((strstr(nm, "Vec") || strstr(nm, "VecDeque")) &&
                base->data.template_type.arg_count > 0) {
                return base->data.template_type.template_args[0];
            }
            if ((strstr(nm, "HashMap") || strstr(nm, "BTreeMap")) &&
                base->data.template_type.arg_count > 1) {
                return base->data.template_type.template_args[1];
            }
        }
        return cbm_type_unknown();
    }

    /* parenthesized_expression */
    if (strcmp(kind, "parenthesized_expression") == 0 && ts_node_named_child_count(node) > 0) {
        return rust_eval_expr_type(ctx, ts_node_named_child(node, 0));
    }

    /* try_expression: e? — peel one Result<T, _> / Option<T> layer. */
    if (strcmp(kind, "try_expression") == 0 && ts_node_named_child_count(node) > 0) {
        const CBMType *inner = rust_eval_expr_type(ctx, ts_node_named_child(node, 0));
        if (inner && inner->kind == CBM_TYPE_TEMPLATE) {
            const char *nm = inner->data.template_type.template_name;
            if ((strstr(nm, "Result") || strstr(nm, "Option")) &&
                inner->data.template_type.arg_count > 0) {
                return inner->data.template_type.template_args[0];
            }
        }
        return inner;
    }

    /* await_expression: future.await — peel one Future / Poll layer naively. */
    if (strcmp(kind, "await_expression") == 0 && ts_node_named_child_count(node) > 0) {
        const CBMType *inner = rust_eval_expr_type(ctx, ts_node_named_child(node, 0));
        if (inner && inner->kind == CBM_TYPE_TEMPLATE && inner->data.template_type.arg_count > 0) {
            return inner->data.template_type.template_args[0];
        }
        return inner;
    }

    /* type_cast_expression: x as T */
    if (strcmp(kind, "type_cast_expression") == 0) {
        TSNode tn = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(tn)) {
            return rust_parse_type_node(ctx, tn);
        }
    }

    /* tuple_expression */
    if (strcmp(kind, "tuple_expression") == 0) {
        const CBMType *elems[16];
        int count = 0;
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc && count < 16; i++) {
            elems[count++] = rust_eval_expr_type(ctx, ts_node_named_child(node, i));
        }
        if (count == 0) {
            return cbm_type_builtin(ctx->arena, "()");
        }
        if (count == 1) {
            return elems[0];
        }
        return cbm_type_tuple(ctx->arena, elems, count);
    }

    /* array_expression: [a, b, c] */
    if (strcmp(kind, "array_expression") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            const CBMType *elem = rust_eval_expr_type(ctx, ts_node_named_child(node, 0));
            return cbm_type_slice(ctx->arena, elem);
        }
        return cbm_type_unknown();
    }

    /* range_expression: a..b, a..=b, a.., ..b, .. — model as
     * Iterator<elem> so chains like `(0..n).map(...).count()` keep
     * typing. The element type is the type of the start/end expr. */
    if (strcmp(kind, "range_expression") == 0) {
        const CBMType *elem_type = NULL;
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(node, i);
            const CBMType *t = rust_eval_expr_type(ctx, c);
            if (t && !cbm_type_is_unknown(t)) {
                elem_type = t;
                break;
            }
        }
        if (elem_type) {
            return cbm_type_template(ctx->arena, "core.iter.Iterator", &elem_type, 1);
        }
        return cbm_type_template(ctx->arena, "core.iter.Iterator", NULL, 0);
    }

    /* unit_expression */
    if (strcmp(kind, "unit_expression") == 0) {
        return cbm_type_builtin(ctx->arena, "()");
    }

    /* struct_expression: Foo { … } */
    if (strcmp(kind, "struct_expression") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (ts_node_is_null(name_node) && ts_node_named_child_count(node) > 0) {
            name_node = ts_node_named_child(node, 0);
        }
        if (!ts_node_is_null(name_node)) {
            char *path = rust_node_text(ctx, name_node);
            if (path) {
                return cbm_type_named(ctx->arena, rust_resolve_path_expr(ctx, path));
            }
        }
        return cbm_type_unknown();
    }

    /* if_expression / match_expression / block / loop_expression — value of
     * the trailing expression in the consequence. */
    if (strcmp(kind, "if_expression") == 0) {
        TSNode cons = ts_node_child_by_field_name(node, "consequence", 11);
        if (!ts_node_is_null(cons)) {
            return rust_eval_expr_type(ctx, cons);
        }
    }
    if (strcmp(kind, "match_expression") == 0) {
        /* Take the type of the first arm's value if available. */
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        if (!ts_node_is_null(body)) {
            uint32_t nc = ts_node_named_child_count(body);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode arm = ts_node_named_child(body, i);
                if (strcmp(ts_node_type(arm), "match_arm") == 0) {
                    TSNode v = ts_node_child_by_field_name(arm, "value", 5);
                    if (!ts_node_is_null(v)) {
                        return rust_eval_expr_type(ctx, v);
                    }
                }
            }
        }
    }
    if (strcmp(kind, "block") == 0) {
        /* Find the last expression child (the trailing expression of the block). */
        uint32_t nc = ts_node_named_child_count(node);
        TSNode last = {0};
        bool found = false;
        for (uint32_t i = nc; i > 0; i--) {
            TSNode c = ts_node_named_child(node, i - 1);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "expression_statement") != 0 && strcmp(ck, "let_declaration") != 0 &&
                strcmp(ck, "empty_statement") != 0 && strcmp(ck, "line_comment") != 0 &&
                strcmp(ck, "block_comment") != 0) {
                last = c;
                found = true;
                break;
            }
        }
        if (found) {
            return rust_eval_expr_type(ctx, last);
        }
        return cbm_type_builtin(ctx->arena, "()");
    }
    if (strcmp(kind, "loop_expression") == 0) {
        return cbm_type_builtin(ctx->arena, "()");
    }

    /* closure_expression: |a, b| body — best effort. Return type comes from
     * the body's last expression if present. */
    if (strcmp(kind, "closure_expression") == 0) {
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        if (ts_node_is_null(body)) {
            return cbm_type_func(ctx->arena, NULL, NULL, NULL);
        }
        return cbm_type_func(ctx->arena, NULL, NULL, NULL);
    }

    /* Literals fall through. */
    if (strcmp(kind, "integer_literal") == 0 || strcmp(kind, "float_literal") == 0 ||
        strcmp(kind, "string_literal") == 0 || strcmp(kind, "raw_string_literal") == 0 ||
        strcmp(kind, "char_literal") == 0 || strcmp(kind, "boolean_literal") == 0 ||
        strcmp(kind, "true") == 0 || strcmp(kind, "false") == 0) {
        return rust_eval_literal_type(ctx, kind, node);
    }

    /* break / continue / return — expressions with no useful type for callers. */
    if (strcmp(kind, "return_expression") == 0 || strcmp(kind, "break_expression") == 0 ||
        strcmp(kind, "continue_expression") == 0 || strcmp(kind, "yield_expression") == 0) {
        return cbm_type_builtin(ctx->arena, "!");
    }

    return cbm_type_unknown();
}

/* Bidirectional wrapper. Evaluate `node` with an `expected` hint. The
 * hint is used post-synthesis: if synthesis returned an under-specified
 * type (unknown, or a TEMPLATE without args, or a NAMED that the hint
 * refines), we substitute the hint when it matches structurally. */
const CBMType *rust_eval_expr_typed(RustLSPContext *ctx, TSNode node, const CBMType *expected) {
    const CBMType *synth = rust_eval_expr_type(ctx, node);
    if (!expected)
        return synth;
    if (!synth || cbm_type_is_unknown(synth)) {
        return expected;
    }
    /* Template with same head and missing args -> use expected. */
    if (synth->kind == CBM_TYPE_TEMPLATE && expected->kind == CBM_TYPE_TEMPLATE &&
        synth->data.template_type.template_name && expected->data.template_type.template_name &&
        strcmp(synth->data.template_type.template_name,
               expected->data.template_type.template_name) == 0 &&
        synth->data.template_type.arg_count == 0 && expected->data.template_type.arg_count > 0) {
        return expected;
    }
    /* NAMED matches expected NAMED — no refinement needed. */
    if (synth->kind == CBM_TYPE_NAMED && expected->kind == CBM_TYPE_TEMPLATE &&
        synth->data.named.qualified_name && expected->data.template_type.template_name &&
        strcmp(synth->data.named.qualified_name, expected->data.template_type.template_name) == 0) {
        return expected; /* refine NAMED → TEMPLATE with args */
    }
    return synth;
}

/* Member access type: returns the CBMType for `recv.field_or_method`.
 * Methods return their FUNC signature; fields return the field type. */
static const CBMType *rust_eval_member_access(RustLSPContext *ctx, const CBMType *recv,
                                              const char *member) {
    if (!recv || !member)
        return cbm_type_unknown();
    /* Auto-deref through references. */
    const CBMType *base = recv;
    while (base && (base->kind == CBM_TYPE_REFERENCE || base->kind == CBM_TYPE_POINTER)) {
        base = (base->kind == CBM_TYPE_REFERENCE) ? base->data.reference.elem
                                                  : base->data.pointer.elem;
    }
    if (!base)
        return cbm_type_unknown();

    const char *type_qn = NULL;
    const CBMType **template_args = NULL;
    int template_count = 0;
    const char **template_params = NULL;

    if (base->kind == CBM_TYPE_NAMED) {
        type_qn = base->data.named.qualified_name;
    } else if (base->kind == CBM_TYPE_TEMPLATE) {
        type_qn = base->data.template_type.template_name;
        template_args = (const CBMType **)base->data.template_type.template_args;
        template_count = base->data.template_type.arg_count;
    } else if (base->kind == CBM_TYPE_BUILTIN) {
        /* Map a few primitives into stdlib QNs so registered methods on
         * `str`/`String`/integers are findable. */
        const char *nm = base->data.builtin.name;
        if (strcmp(nm, "str") == 0)
            type_qn = "core.str";
        else if (strcmp(nm, "String") == 0)
            type_qn = "alloc.string.String";
        else
            type_qn = nm;
    } else if (base->kind == CBM_TYPE_SLICE) {
        type_qn = "core.slice";
    } else {
        return cbm_type_unknown();
    }
    if (!type_qn)
        return cbm_type_unknown();

    /* Check inherent method first. */
    const CBMRegisteredFunc *method = rust_lookup_method(ctx, type_qn, member);
    if (method && method->signature) {
        if (template_args && method->type_param_names) {
            template_params = method->type_param_names;
            const CBMType *sub =
                rust_substitute_type(ctx->arena, method->signature, template_params, template_args);
            return sub;
        }
        /* Substitute any Self placeholder on the return type. */
        const CBMType *sig = method->signature;
        if (sig && sig->kind == CBM_TYPE_FUNC && sig->data.func.return_types &&
            sig->data.func.return_types[0]) {
            const CBMType *ret = sig->data.func.return_types[0];
            if (ret && ret->kind == CBM_TYPE_NAMED &&
                strcmp(ret->data.named.qualified_name, "Self") == 0) {
                return cbm_type_named(ctx->arena, type_qn);
            }
        }
        return method->signature;
    }

    /* Field on the type's RegisteredType. */
    const CBMType *ft = rust_lookup_field(ctx, type_qn, member, 0);
    if (ft) {
        if (template_args && template_params) {
            return rust_substitute_type(ctx->arena, ft, template_params, template_args);
        }
        return ft;
    }
    return cbm_type_unknown();
}

/* ════════════════════════════════════════════════════════════════════
 * 7. Method dispatch + field lookup
 * ════════════════════════════════════════════════════════════════════ */

static const CBMType *rust_lookup_field(RustLSPContext *ctx, const char *type_qn,
                                        const char *field_name, int depth) {
    if (!type_qn || !field_name || depth > CBM_LSP_MAX_LOOKUP_DEPTH)
        return NULL;
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt)
        return NULL;
    if (rt->alias_of)
        return rust_lookup_field(ctx, rt->alias_of, field_name, depth + 1);
    if (rt->field_names) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (strcmp(rt->field_names[i], field_name) == 0 && rt->field_types &&
                rt->field_types[i]) {
                return rt->field_types[i];
            }
        }
    }
    if (rt->embedded_types) {
        for (int i = 0; rt->embedded_types[i]; i++) {
            const CBMType *f = rust_lookup_field(ctx, rt->embedded_types[i], field_name, depth + 1);
            if (f)
                return f;
        }
    }
    return NULL;
}

/* Hardcoded Deref<Target=U> map for stdlib smart pointers + guards. The
 * format mirrors the de-facto rule: `<smart-pointer-QN-prefix>` →
 * `<inner-type position>` where inner is taken from the receiver's
 * template args. We don't store U literally; instead we let the call
 * site pass `template_args[idx]` as the new receiver type for retry. */
static bool rust_type_derefs_to_first_arg(const char *type_qn) {
    if (!type_qn)
        return false;
    static const char *derefable[] = {"alloc.boxed.Box",
                                      "std.boxed.Box",
                                      "alloc.rc.Rc",
                                      "std.rc.Rc",
                                      "alloc.sync.Arc",
                                      "std.sync.Arc",
                                      "core.cell.RefCell",
                                      "std.cell.RefCell",
                                      "core.cell.Cell",
                                      "std.cell.Cell",
                                      "std.sync.MutexGuard",
                                      "std.sync.RwLockReadGuard",
                                      "std.sync.RwLockWriteGuard",
                                      "core.pin.Pin",
                                      NULL};
    for (const char **p = derefable; *p; p++) {
        if (strcmp(*p, type_qn) == 0)
            return true;
    }
    return false;
}

/* Returns the inner type after one Deref step. For NAMED types we check
 * for a registered `embedded_types` entry whose tail is "Target=<U>" or,
 * for stdlib smart pointers, peel the first template arg. Returns NULL
 * if no Deref relationship is known. */
static const CBMType *rust_deref_step(RustLSPContext *ctx, const CBMType *t) {
    if (!t)
        return NULL;
    /* Smart pointers: Box<T>, Rc<T>, Arc<T>, etc. The grammar gives us a
     * TEMPLATE; the inner type is the first template arg. */
    if (t->kind == CBM_TYPE_TEMPLATE && t->data.template_type.template_name) {
        if (rust_type_derefs_to_first_arg(t->data.template_type.template_name)) {
            if (t->data.template_type.arg_count > 0) {
                return t->data.template_type.template_args[0];
            }
            return NULL;
        }
    }
    /* NAMED smart-pointer (rare — only when the user wrote `Rc` without
     * type arg) — no inner to peel. */
    if (t->kind == CBM_TYPE_NAMED && t->data.named.qualified_name) {
        if (rust_type_derefs_to_first_arg(t->data.named.qualified_name)) {
            return NULL;
        }
        /* Project type with a registered Deref impl. We approximate this
         * by checking `embedded_types` for an entry of the form
         * `<TraitQN>:DerefTarget:<U>` — set up by `impl Deref for X`
         * post-processing. (Currently not produced by extract_defs; the
         * call still works for stdlib types.) */
        const CBMRegisteredType *rt =
            cbm_registry_lookup_type(ctx->registry, t->data.named.qualified_name);
        if (rt && rt->embedded_types) {
            for (int i = 0; rt->embedded_types[i]; i++) {
                const char *e = rt->embedded_types[i];
                static const char prefix[] = "DerefTarget:";
                if (strncmp(e, prefix, sizeof(prefix) - 1) == 0) {
                    return cbm_type_named(ctx->arena, e + sizeof(prefix) - 1);
                }
            }
        }
    }
    return NULL;
}

static const CBMRegisteredFunc *rust_lookup_method_depth(RustLSPContext *ctx, const char *type_qn,
                                                         const char *member_name, int depth) {
    if (!type_qn || !member_name)
        return NULL;
    if (depth > CBM_LSP_MAX_LOOKUP_DEPTH)
        return NULL;

    /* Direct inherent method lookup. */
    const CBMRegisteredFunc *f =
        cbm_registry_lookup_method_aliased(ctx->registry, type_qn, member_name);
    if (f)
        return f;

    /* Follow alias / embedded chain. */
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (rt) {
        if (rt->alias_of) {
            f = rust_lookup_method_depth(ctx, rt->alias_of, member_name, depth + 1);
            if (f)
                return f;
        }
        if (rt->embedded_types) {
            for (int i = 0; rt->embedded_types[i]; i++) {
                /* Skip the synthetic DerefTarget marker — handled by the
                 * caller in the receiver-walk loop. */
                if (strncmp(rt->embedded_types[i], "DerefTarget:", 12) == 0)
                    continue;
                /* Skip bound markers used for type-param trait bound
                 * recording — those live under "Bound:<TraitQN>". */
                if (strncmp(rt->embedded_types[i], "Bound:", 6) == 0) {
                    /* Dispatch through the bound trait. */
                    f = rust_lookup_method_depth(ctx, rt->embedded_types[i] + 6, member_name,
                                                 depth + 1);
                    if (f)
                        return f;
                    continue;
                }
                f = rust_lookup_method_depth(ctx, rt->embedded_types[i], member_name, depth + 1);
                if (f)
                    return f;
            }
        }
    }
    return NULL;
}

const CBMRegisteredFunc *rust_lookup_method(RustLSPContext *ctx, const char *type_qn,
                                            const char *member_name) {
    return rust_lookup_method_depth(ctx, type_qn, member_name, 0);
}

/* Walk the registry for any method named `member_name` on a type that
 * implements `trait_qn`. We approximate trait impl membership by checking
 * whether the type's `embedded_types` contains `trait_qn` (we treat
 * `impl Trait for Type` as registering the trait QN as an embedded type
 * of the receiver). */
static const CBMRegisteredFunc *rust_lookup_method_in_trait(RustLSPContext *ctx,
                                                            const char *trait_qn,
                                                            const char *method_name) {
    if (!ctx || !trait_qn || !method_name)
        return NULL;
    const CBMTypeRegistry *reg = ctx->registry;
    /* Trait method is also registered on the trait itself with `receiver_type`
     * set to the trait QN (default impls / signatures). */
    return cbm_registry_lookup_method(reg, trait_qn, method_name);
}

/* For a method call where the receiver is a `dyn Trait` or `impl Trait`,
 * try to resolve through the trait's known impls. Returns the chosen
 * concrete method, the trait method (default impl), or NULL. */
static const CBMRegisteredFunc *rust_resolve_trait_method(RustLSPContext *ctx,
                                                          const char *receiver_type_qn,
                                                          const char *method_name,
                                                          int *out_impl_count) {
    if (out_impl_count)
        *out_impl_count = 0;
    if (!ctx || !receiver_type_qn || !method_name)
        return NULL;
    const CBMTypeRegistry *reg = ctx->registry;

    /* First try inherent method on the receiver itself, following any
     * type aliases (so `std.sync.Arc.clone` resolves through to
     * `alloc.sync.Arc.clone` in the registry). Stays BEFORE the memo check —
     * a colliding real hit is always found (collision guard). */
    const CBMRegisteredFunc *inh =
        cbm_registry_lookup_method_aliased(reg, receiver_type_qn, method_name);
    if (inh) {
        if (out_impl_count)
            *out_impl_count = 1;
        return inh;
    }

    /* Negative memo (sealed registry only): this whole cascade — embedded-
     * impl walk + trait-default tail — reads nothing but the registry and the
     * two query strings, so under read_only a miss is a pure fact. Macro-
     * expanded kernel rust asks the same failing (receiver, method) question
     * thousands of times per file; without the memo each repeat re-paid the
     * full walk (~63 s per trait-heavy file). Only the full miss (0 impls,
     * no trait default) is memoized, preserving out_impl_count fidelity for
     * the ambiguous (>=2 impls) case. */
    bool nm_active = reg && reg->read_only;
    uint64_t nm_key = 0;
    if (nm_active) {
        nm_key = cbm_negmemo_key(1, receiver_type_qn, method_name);
        if (cbm_negmemo_contains(&ctx->neg_memo, nm_key)) {
            return NULL;
        }
    }

    /* Look at every type whose embedded_types include the receiver_type_qn
     * (treated as a trait): pick the single-impl case. Prefilter to the types whose
     * embedded_types carry a matching BARE name via the registry index; the exact
     * full-QN check below is unchanged, so the result set is identical. */
    const CBMRegisteredFunc *unique = NULL;
    int impls = 0;
    const char *rdot = strrchr(receiver_type_qn, '.');
    const char *rbare = rdot ? rdot + 1 : receiver_type_qn;
    CBMTypeEmbedIter eit;
    cbm_registry_types_by_embedded_bare(reg, rbare, &eit);
    for (int ti; impls < 3 && (ti = cbm_type_embed_iter_next(&eit)) >= 0;) {
        const CBMRegisteredType *t = &reg->types[ti];
        if (!t->embedded_types)
            continue;
        for (int j = 0; t->embedded_types[j]; j++) {
            if (strcmp(t->embedded_types[j], receiver_type_qn) == 0) {
                const CBMRegisteredFunc *mf =
                    cbm_registry_lookup_method(reg, t->qualified_name, method_name);
                if (mf) {
                    impls++;
                    if (impls == 1)
                        unique = mf;
                }
                break;
            }
        }
    }
    if (out_impl_count)
        *out_impl_count = impls;
    if (impls == 1)
        return unique;
    const CBMRegisteredFunc *tm =
        rust_lookup_method_in_trait(ctx, receiver_type_qn, method_name);
    if (nm_active && !tm && impls == 0) {
        cbm_negmemo_insert(&ctx->neg_memo, ctx->arena, nm_key);
    }
    return tm;
}

// True if `type_qn` implements a trait that declares `method_name` — i.e. a
// method resolved inherently on the receiver is actually a trait-impl method
// (lsp_trait_dispatch) rather than a plain inherent one (lsp_method_dispatch).
// A struct's embedded_types are the traits it implements (the impl-link model
// rust_resolve_trait_method already relies on), so a declaring trait among them
// means the method came from `impl Trait for Type`.
static bool rust_method_is_trait_impl(RustLSPContext *ctx, const char *type_qn,
                                      const char *method_name) {
    if (!ctx || !type_qn || !method_name)
        return false;
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt || !rt->embedded_types)
        return false;
    for (int i = 0; rt->embedded_types[i]; i++) {
        if (cbm_registry_lookup_method(ctx->registry, rt->embedded_types[i], method_name))
            return true;
    }
    return false;
}

// Find the sole concrete implementer of trait `trait_qn` that declares
// `method_name`, returning that impl's method (NULL if none or 2+), setting
// *out_n to the count (capped at 2). Used for `Trait::method` UFCS so it
// resolves to the concrete impl rather than the trait's own abstract method.
// Matches the embedded (impl-link) entry by full QN OR bare name, since the
// link is recorded short in some registry entries and fully-qualified in
// others; dedups implementers by QN.
static const CBMRegisteredFunc *rust_find_sole_trait_impl(RustLSPContext *ctx, const char *trait_qn,
                                                          const char *method_name, int *out_n) {
    if (out_n)
        *out_n = 0;
    if (!ctx || !trait_qn || !method_name)
        return NULL;
    const CBMTypeRegistry *reg = ctx->registry;
    /* Negative memo (sealed registry only) — registry-pure cascade; only the
     * zero-implementer miss is memoized (out_n fidelity for the 2+ case). */
    bool nm_active = reg && reg->read_only;
    uint64_t nm_key = 0;
    if (nm_active) {
        nm_key = cbm_negmemo_key(2, trait_qn, method_name);
        if (cbm_negmemo_contains(&ctx->neg_memo, nm_key)) {
            return NULL;
        }
    }
    const char *tdot = strrchr(trait_qn, '.');
    const char *tbare = tdot ? tdot + 1 : trait_qn;
    const CBMRegisteredFunc *first = NULL;
    const char *first_qn = NULL;
    int n = 0;
    /* Prefilter to types whose embedded_types carry the trait's BARE name; the
     * exact (full-QN OR bare) check below is unchanged. tbare-keyed index captures
     * every original match (a full-QN match implies a bare-name match). */
    CBMTypeEmbedIter eit;
    cbm_registry_types_by_embedded_bare(reg, tbare, &eit);
    for (int ti; n < 2 && (ti = cbm_type_embed_iter_next(&eit)) >= 0;) {
        const CBMRegisteredType *t = &reg->types[ti];
        if (!t->embedded_types || !t->qualified_name)
            continue;
        bool impls = false;
        for (int j = 0; t->embedded_types[j]; j++) {
            const char *e = t->embedded_types[j];
            const char *edot = strrchr(e, '.');
            const char *ebare = edot ? edot + 1 : e;
            if (strcmp(e, trait_qn) == 0 || strcmp(ebare, tbare) == 0) {
                impls = true;
                break;
            }
        }
        if (!impls)
            continue;
        const CBMRegisteredFunc *mf =
            cbm_registry_lookup_method(reg, t->qualified_name, method_name);
        if (!mf)
            continue;
        if (!first_qn) {
            first = mf;
            first_qn = t->qualified_name;
            n = 1;
        } else if (strcmp(first_qn, t->qualified_name) != 0) {
            n = 2;
        }
    }
    if (out_n)
        *out_n = n;
    if (nm_active && n == 0) {
        cbm_negmemo_insert(&ctx->neg_memo, ctx->arena, nm_key);
    }
    return n == 1 ? first : NULL;
}

/* ════════════════════════════════════════════════════════════════════
 * 8. Macro handling
 * ════════════════════════════════════════════════════════════════════ */

/* Walk a `macro_invocation`'s `token_tree` looking for nested call/method
 * call expressions. We do this so calls inside `vec![foo()]`,
 * `assert_eq!(a.bar(), 0)`, and `dbg!(get_value())` still get attributed
 * to the enclosing function. */
static void rust_walk_macro_tokens(RustLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "call_expression") == 0 || strcmp(kind, "macro_invocation") == 0 ||
        strcmp(kind, "field_expression") == 0) {
        rust_resolve_calls_in_node(ctx, node);
        return;
    }
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (!ts_node_is_null(c))
            rust_walk_macro_tokens(ctx, c);
    }
}

/* Map a Rust infix/index operator token to the std::ops trait method that
 * the compiler desugars it to. `a + b` calls `Add::add`, `a[i]` calls
 * `Index::index`, etc. Returns NULL for operators with no overloadable
 * trait method (comparison/logical operators route through PartialEq /
 * PartialOrd whose methods we don't model here — sound to skip). */
static const char *rust_binop_trait_method(const char *op_text) {
    if (!op_text)
        return NULL;
    if (strcmp(op_text, "+") == 0)
        return "add";
    if (strcmp(op_text, "-") == 0)
        return "sub";
    if (strcmp(op_text, "*") == 0)
        return "mul";
    if (strcmp(op_text, "/") == 0)
        return "div";
    if (strcmp(op_text, "%") == 0)
        return "rem";
    if (strcmp(op_text, "&") == 0)
        return "bitand";
    if (strcmp(op_text, "|") == 0)
        return "bitor";
    if (strcmp(op_text, "^") == 0)
        return "bitxor";
    if (strcmp(op_text, "<<") == 0)
        return "shl";
    if (strcmp(op_text, ">>") == 0)
        return "shr";
    return NULL;
}

/* If `recv` is a user-defined NAMED type that defines operator method
 * `method` (via inherent impl or `impl <trait> for T`), emit a CALLS edge to
 * it. Models Rust operator-overload desugaring (`a + b` → T::add, `a[i]` →
 * T::index). Sound-only: we emit nothing when the operand type is unknown,
 * primitive, or the type has no such method registered — so we never guess on
 * built-in arithmetic. */
static void rust_emit_operator_call(RustLSPContext *ctx, const CBMType *recv, const char *method) {
    if (!recv || !method)
        return;
    const CBMType *base = recv;
    while (base && (base->kind == CBM_TYPE_REFERENCE || base->kind == CBM_TYPE_POINTER)) {
        base = (base->kind == CBM_TYPE_REFERENCE) ? base->data.reference.elem
                                                  : base->data.pointer.elem;
    }
    /* Only user-defined named types — built-in arithmetic must not emit. */
    if (!base || base->kind != CBM_TYPE_NAMED)
        return;
    const char *type_qn = base->data.named.qualified_name;
    if (!type_qn || is_rust_primitive(type_qn))
        return;
    int impl_count = 0;
    const CBMRegisteredFunc *m = rust_resolve_trait_method(ctx, type_qn, method, &impl_count);
    if (m && m->qualified_name) {
        rust_emit_resolved_call(ctx, m->qualified_name, "lsp_operator_trait",
                                CBM_RUST_CONF_OPERATOR);
        /* `a + b` is a binary_expression, never a syntactic call node, so the
         * extractor produced no CBMCall to pair with the resolved_call above.
         * Inject one so the pipeline emits the CALLS edge. */
        rust_inject_syn_call(ctx, m->qualified_name);
    }
}

/* ── User-defined macro_rules! support ────────────────────────────
 *
 * Strategy: collect every `macro_rules!` definition in the file
 * during the pre-walk, store each rule's transcriber text, and on
 * `macro_invocation` re-parse the transcriber as a synthetic Rust
 * function body so any calls inside the body are attributed to the
 * enclosing function of the invocation site.
 *
 * Recursion guard caps expansion depth at 8 (matches the rustc
 * default for macro recursion safety). */

typedef struct RustMacroRule {
    const char *macro_name;
    const char *pattern_text; /* left-hand side (without outer brackets) */
    int pattern_len;
    const char *transcriber_text;
    int transcriber_len;
} RustMacroRule;

/* Strip a single outer pair of matching brackets from a token-tree
 * text representation. Returns the inner span via out_text/out_len.
 * If no brackets, returns the original. */
static void rust_macro_strip_outer(const char *tt, int len, const char **out_text, int *out_len) {
    if (len >= 2 && ((tt[0] == '{' && tt[len - 1] == '}') || (tt[0] == '(' && tt[len - 1] == ')') ||
                     (tt[0] == '[' && tt[len - 1] == ']'))) {
        *out_text = tt + 1;
        *out_len = len - 2;
    } else {
        *out_text = tt;
        *out_len = len;
    }
}

static void rust_record_macro_rule(RustLSPContext *ctx, const char *macro_name, TSNode pattern,
                                   TSNode transcriber) {
    if (!ctx || !macro_name || ts_node_is_null(transcriber))
        return;
    if (ctx->macro_rules_count % 16 == 0) {
        int new_cap = ctx->macro_rules_count + 16;
        struct RustMacroRule **narr = (struct RustMacroRule **)cbm_arena_alloc(
            ctx->arena, new_cap * sizeof(struct RustMacroRule *));
        if (!narr)
            return;
        if (ctx->macro_rules_arr && ctx->macro_rules_count > 0) {
            memcpy(narr, ctx->macro_rules_arr,
                   ctx->macro_rules_count * sizeof(struct RustMacroRule *));
        }
        ctx->macro_rules_arr = narr;
    }
    RustMacroRule *r = (RustMacroRule *)cbm_arena_alloc(ctx->arena, sizeof(*r));
    if (!r)
        return;
    memset(r, 0, sizeof(*r));
    r->macro_name = cbm_arena_strdup(ctx->arena, macro_name);

    /* Cache pattern text for matching at invocation time. */
    if (!ts_node_is_null(pattern)) {
        char *pt = cbm_node_text(ctx->arena, pattern, ctx->source);
        if (pt) {
            const char *inner;
            int inner_len;
            rust_macro_strip_outer(pt, (int)strlen(pt), &inner, &inner_len);
            r->pattern_text = cbm_arena_strndup(ctx->arena, inner, (size_t)inner_len);
            r->pattern_len = inner_len;
        }
    }

    char *tt = cbm_node_text(ctx->arena, transcriber, ctx->source);
    if (tt) {
        int len = (int)strlen(tt);
        const char *inner;
        int inner_len;
        rust_macro_strip_outer(tt, len, &inner, &inner_len);
        r->transcriber_text = cbm_arena_strndup(ctx->arena, inner, (size_t)inner_len);
        r->transcriber_len = inner_len;
    }
    ctx->macro_rules_arr[ctx->macro_rules_count++] = r;
}

static void rust_collect_macro_rules(RustLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;
    uint32_t nc = ts_node_child_count(root);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(root, i);
        if (ts_node_is_null(c))
            continue;
        const char *k = ts_node_type(c);
        if (strcmp(k, "macro_definition") == 0) {
            TSNode name_node = ts_node_child_by_field_name(c, "name", 4);
            if (ts_node_is_null(name_node))
                continue;
            char *macro_name = rust_node_text(ctx, name_node);
            if (!macro_name)
                continue;
            uint32_t mnc = ts_node_child_count(c);
            for (uint32_t j = 0; j < mnc; j++) {
                TSNode rule = ts_node_child(c, j);
                if (ts_node_is_null(rule) || !ts_node_is_named(rule))
                    continue;
                if (strcmp(ts_node_type(rule), "macro_rule") != 0)
                    continue;
                TSNode left = ts_node_child_by_field_name(rule, "left", 4);
                TSNode right = ts_node_child_by_field_name(rule, "right", 5);
                if (!ts_node_is_null(right)) {
                    rust_record_macro_rule(ctx, macro_name, left, right);
                }
            }
        } else if (strcmp(k, "mod_item") == 0) {
            TSNode body = ts_node_child_by_field_name(c, "body", 4);
            if (!ts_node_is_null(body))
                rust_collect_macro_rules(ctx, body);
        }
    }
}

/* ── Metavar matching + substitution ─────────────────────────────
 *
 * Real `macro_rules!` semantics in a clean-room subset. We implement
 * the common shapes:
 *   $name:expr / $name:ty / $name:ident / $name:tt / $name:path /
 *   $name:pat / $name:literal / $name:block / $name:stmt
 *
 * And one form of repetition: `$(...)<sep><kind>` where kind is `*`,
 * `+`, or `?`. Repetitions inside repetitions are NOT supported.
 *
 * Substitution writes the bound values back into the transcriber and
 * re-parses the result so any calls inside are visible to the
 * resolver. */

#define RUST_MACRO_MAX_BINDINGS 32
#define RUST_MACRO_MAX_REPS 32

typedef struct {
    char name[32];     /* metavar name (without `$`) */
    const char *value; /* substring of the invocation args */
    int value_len;
} MacroBinding;

typedef struct {
    MacroBinding kv[RUST_MACRO_MAX_BINDINGS];
    int count;
    /* Repetition bindings: each metavar that appears inside `$(...)*`
     * collects an array of values (one per iteration). */
    struct {
        char name[32];
        const char **values; /* arena-allocated array of strings */
        int *lengths;
        int count;
    } reps[RUST_MACRO_MAX_REPS];
    int rep_count;
} MacroEnv;

/* Skip leading ASCII whitespace and Rust line comments. */
static int macro_skip_ws(const char *s, int len, int from) {
    while (from < len) {
        char c = s[from];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            from++;
            continue;
        }
        if (c == '/' && from + 1 < len && s[from + 1] == '/') {
            from += 2;
            while (from < len && s[from] != '\n')
                from++;
            continue;
        }
        break;
    }
    return from;
}

/* Consume an identifier (ASCII rust ident). Returns end position or
 * `from` if no identifier. */
static int macro_consume_ident(const char *s, int len, int from) {
    int p = from;
    if (p >= len)
        return from;
    char c = s[p];
    if (!(c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')))
        return from;
    p++;
    while (p < len) {
        c = s[p];
        if (c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9')) {
            p++;
        } else
            break;
    }
    return p;
}

/* Consume a balanced bracket group starting at `from` (which must
 * point at `(`, `[`, or `{`). Returns end position (just after the
 * close bracket) or `from` if not a bracket. */
static int macro_consume_balanced(const char *s, int len, int from) {
    if (from >= len)
        return from;
    char open = s[from];
    char close = 0;
    if (open == '(')
        close = ')';
    else if (open == '[')
        close = ']';
    else if (open == '{')
        close = '}';
    else
        return from;
    int depth = 1;
    int p = from + 1;
    while (p < len && depth > 0) {
        char c = s[p];
        if (c == '"') {
            /* Skip string literal. */
            p++;
            while (p < len && s[p] != '"') {
                if (s[p] == '\\' && p + 1 < len)
                    p += 2;
                else
                    p++;
            }
            if (p < len)
                p++;
            continue;
        }
        if (c == '\'') {
            /* Could be lifetime or char literal — accept char. */
            int q = p + 1;
            if (q < len && s[q] == '\\') {
                q++;
                if (q < len)
                    q++;
            } else if (q < len)
                q++;
            if (q < len && s[q] == '\'') {
                p = q + 1;
                continue;
            }
            /* otherwise treat as lifetime — skip until non-ident. */
            p = macro_consume_ident(s, len, p + 1);
            continue;
        }
        if (c == '(' || c == '[' || c == '{')
            depth++;
        else if (c == ')' || c == ']' || c == '}')
            depth--;
        p++;
    }
    return p;
}

/* Consume a fragment from the input matching the given fragment kind.
 * Returns end position or `from` if the consumption failed. */
static int macro_consume_fragment(const char *s, int len, int from, const char *frag) {
    from = macro_skip_ws(s, len, from);
    if (from >= len)
        return from;
    if (!frag)
        frag = "tt";

    if (strcmp(frag, "ident") == 0) {
        return macro_consume_ident(s, len, from);
    }
    if (strcmp(frag, "literal") == 0) {
        /* Numeric, string, char, bool literal. */
        char c = s[from];
        if (c == '"')
            return macro_consume_balanced(s, len, from);
        if (c == '\'') {
            int q = from + 1;
            if (q < len && s[q] == '\\') {
                q++;
                if (q < len)
                    q++;
            } else if (q < len)
                q++;
            if (q < len && s[q] == '\'')
                return q + 1;
            return from;
        }
        if (c == '-' || (c >= '0' && c <= '9')) {
            int p = from;
            if (c == '-')
                p++;
            while (p < len && ((s[p] >= '0' && s[p] <= '9') || s[p] == '_' || s[p] == '.' ||
                               s[p] == 'x' || s[p] == 'X' || s[p] == 'b' || s[p] == 'o' ||
                               (s[p] >= 'a' && s[p] <= 'f') || (s[p] >= 'A' && s[p] <= 'F')))
                p++;
            /* Allow type suffix. */
            int after = macro_consume_ident(s, len, p);
            return after;
        }
        return macro_consume_ident(s, len, from);
    }
    if (strcmp(frag, "tt") == 0) {
        char c = s[from];
        if (c == '(' || c == '[' || c == '{') {
            return macro_consume_balanced(s, len, from);
        }
        /* Single token: identifier, literal, or single-char punct. */
        if (c == '"' || c == '\'') {
            return macro_consume_fragment(s, len, from, "literal");
        }
        if (c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-') {
            int p = macro_consume_ident(s, len, from);
            if (p > from)
                return p;
            return macro_consume_fragment(s, len, from, "literal");
        }
        return from + 1;
    }
    /* expr / ty / path / pat / stmt / block — balance brackets, stop
     * at top-level `,` or end of input. */
    if (strcmp(frag, "block") == 0) {
        if (s[from] == '{')
            return macro_consume_balanced(s, len, from);
        return from;
    }
    /* For expr/ty/path/pat/stmt: greedy balanced span. */
    int depth = 0;
    int p = from;
    while (p < len) {
        char c = s[p];
        if (c == '"' || c == '\'') {
            int q = macro_consume_fragment(s, len, p, "literal");
            if (q == p) {
                p++;
                continue;
            }
            p = q;
            continue;
        }
        if (c == '(' || c == '[' || c == '{') {
            int q = macro_consume_balanced(s, len, p);
            if (q == p)
                break;
            p = q;
            continue;
        }
        if (c == ')' || c == ']' || c == '}')
            break;
        if (depth == 0 && c == ',')
            break;
        if (strcmp(frag, "stmt") == 0 && c == ';')
            break;
        p++;
    }
    return p;
}

/* Bind a metavar in the env. Returns false if the table is full or the
 * name doesn't fit. */
static bool macro_env_bind(MacroEnv *env, const char *name, int name_len, const char *val,
                           int val_len) {
    if (env->count >= RUST_MACRO_MAX_BINDINGS)
        return false;
    if (name_len <= 0 || name_len >= 32)
        return false;
    MacroBinding *b = &env->kv[env->count++];
    memcpy(b->name, name, name_len);
    b->name[name_len] = '\0';
    b->value = val;
    b->value_len = val_len;
    return true;
}

static const MacroBinding *macro_env_lookup(const MacroEnv *env, const char *name) {
    for (int i = 0; i < env->count; i++) {
        if (strcmp(env->kv[i].name, name) == 0)
            return &env->kv[i];
    }
    return NULL;
}

/* Match a pattern against an input. Pattern fragments like `$x:expr`
 * bind into `env`. Returns true if the pattern matches the whole input
 * (or up to the end of pattern, with trailing whitespace in input). */
static bool macro_pattern_match(const char *pat, int pat_len, const char *in, int in_len,
                                MacroEnv *env) {
    int pp = 0; /* pattern pos */
    int ip = 0; /* input pos */

    while (pp < pat_len) {
        pp = macro_skip_ws(pat, pat_len, pp);
        ip = macro_skip_ws(in, in_len, ip);
        if (pp >= pat_len)
            break;

        char pc = pat[pp];

        /* Metavar: $name:frag or $name */
        if (pc == '$') {
            pp++;
            /* Repetition group `$( ... )<sep><kind>` — for now we
             * treat any rep group as a wildcard accepting the rest of
             * the input (best-effort). We bind no names inside reps. */
            pp = macro_skip_ws(pat, pat_len, pp);
            if (pp < pat_len && pat[pp] == '(') {
                int after = macro_consume_balanced(pat, pat_len, pp);
                pp = after;
                /* Skip optional separator + kind (one char each). */
                if (pp < pat_len && pat[pp] != ' ' && pat[pp] != '$' && pat[pp] != '\n') {
                    pp++;
                }
                if (pp < pat_len && (pat[pp] == '*' || pat[pp] == '+' || pat[pp] == '?')) {
                    pp++;
                }
                /* Consume the rest of the input greedy until we hit
                 * the next literal pattern char. */
                int next_lit = pp;
                while (next_lit < pat_len && (pat[next_lit] == ' ' || pat[next_lit] == '\t' ||
                                              pat[next_lit] == '\n' || pat[next_lit] == '\r')) {
                    next_lit++;
                }
                if (next_lit >= pat_len) {
                    ip = in_len;
                } else {
                    char target = pat[next_lit];
                    while (ip < in_len && in[ip] != target)
                        ip++;
                }
                continue;
            }
            /* Metavar name. */
            int name_start = pp;
            pp = macro_consume_ident(pat, pat_len, pp);
            int name_end = pp;
            if (name_end == name_start)
                return false;
            const char *frag = "tt";
            if (pp < pat_len && pat[pp] == ':') {
                pp++;
                int frag_start = pp;
                pp = macro_consume_ident(pat, pat_len, pp);
                /* Copy frag into a small buffer. */
                static char frag_buf[16];
                int fl = pp - frag_start;
                if (fl > 0 && fl < (int)sizeof(frag_buf)) {
                    memcpy(frag_buf, pat + frag_start, fl);
                    frag_buf[fl] = '\0';
                    frag = frag_buf;
                }
            }
            /* Consume the fragment from input. */
            int val_start = ip;
            int val_end = macro_consume_fragment(in, in_len, ip, frag);
            if (val_end <= val_start) {
                /* For "expr"/"ty" the matcher may need to accept empty
                 * input (e.g. trailing `$($x:expr),*` form). */
                return false;
            }
            ip = val_end;
            if (!macro_env_bind(env, pat + name_start, name_end - name_start, in + val_start,
                                val_end - val_start)) {
                return false;
            }
            continue;
        }

        /* Literal: must match input verbatim. */
        if (ip >= in_len)
            return false;
        char ic = in[ip];
        if (pc != ic)
            return false;
        pp++;
        ip++;
    }
    /* Allow trailing whitespace in input. */
    ip = macro_skip_ws(in, in_len, ip);
    return ip == in_len;
}

/* Substitute env bindings into the transcriber text. Allocates a
 * fresh string in the arena. */
static char *macro_substitute(CBMArena *arena, const char *xs, int xs_len, const MacroEnv *env) {
    /* Estimate output size: each metavar expansion could be up to
     * ~256 bytes; bound the total at xs_len * 4 + 4KB. */
    int cap = xs_len * 4 + 4096;
    char *out = (char *)cbm_arena_alloc(arena, cap + 1);
    if (!out)
        return NULL;
    int op = 0;
    int xp = 0;
    while (xp < xs_len) {
        char c = xs[xp];
        if (c == '$' && xp + 1 < xs_len) {
            /* Skip rep groups: `$(...)<sep><kind>` — naive expansion:
             * emit the body once, no separator handling. */
            if (xs[xp + 1] == '(') {
                int body_start = xp + 2;
                int after = macro_consume_balanced(xs, xs_len, xp + 1);
                int body_end = after - 1;
                /* Emit the body recursively substituted. */
                char *inner = macro_substitute(arena, xs + body_start, body_end - body_start, env);
                if (inner) {
                    int il = (int)strlen(inner);
                    if (op + il < cap) {
                        memcpy(out + op, inner, il);
                        op += il;
                    }
                }
                xp = after;
                /* Skip optional separator + kind. */
                if (xp < xs_len && xs[xp] != ' ' && xs[xp] != '\n')
                    xp++;
                if (xp < xs_len && (xs[xp] == '*' || xs[xp] == '+' || xs[xp] == '?')) {
                    xp++;
                }
                continue;
            }
            /* $name reference. */
            int name_start = xp + 1;
            int name_end = macro_consume_ident(xs, xs_len, name_start);
            if (name_end > name_start) {
                char name_buf[32];
                int nl = name_end - name_start;
                if (nl < 32) {
                    memcpy(name_buf, xs + name_start, nl);
                    name_buf[nl] = '\0';
                    const MacroBinding *b = macro_env_lookup(env, name_buf);
                    if (b && b->value && op + b->value_len < cap) {
                        memcpy(out + op, b->value, b->value_len);
                        op += b->value_len;
                        xp = name_end;
                        continue;
                    }
                }
            }
        }
        if (op < cap)
            out[op++] = c;
        xp++;
    }
    out[op] = '\0';
    return out;
}

/* Re-parse a user macro's transcriber body as a synthetic Rust
 * function and walk it for calls. */
extern const TSLanguage *tree_sitter_rust(void);
static void rust_expand_user_macro(RustLSPContext *ctx, const char *mname, TSNode invocation) {
    if (!ctx || !mname || !ctx->macro_rules_arr)
        return;
    if (ctx->macro_expand_depth >= 8)
        return;

    /* Try each rule until one matches; first to match wins. */
    RustMacroRule *hit = NULL;
    MacroEnv env;
    memset(&env, 0, sizeof(env));

    /* Extract the invocation argument text. */
    const char *inv_args = NULL;
    int inv_args_len = 0;
    if (!ts_node_is_null(invocation)) {
        TSNode args_tt = ts_node_child_by_field_name(invocation, "arguments", 9);
        if (!ts_node_is_null(args_tt)) {
            char *at = cbm_node_text(ctx->arena, args_tt, ctx->source);
            if (at) {
                rust_macro_strip_outer(at, (int)strlen(at), &inv_args, &inv_args_len);
            }
        }
    }

    for (int i = 0; i < ctx->macro_rules_count; i++) {
        RustMacroRule *r = ctx->macro_rules_arr[i];
        if (strcmp(r->macro_name, mname) != 0)
            continue;
        memset(&env, 0, sizeof(env));
        if (r->pattern_text && inv_args &&
            macro_pattern_match(r->pattern_text, r->pattern_len, inv_args, inv_args_len, &env)) {
            hit = r;
            break;
        }
        /* Fall back to first-rule match without arg substitution if
         * the pattern doesn't match — still expand to walk the body. */
        if (!hit)
            hit = r;
    }
    if (!hit || !hit->transcriber_text || hit->transcriber_len <= 0)
        return;

    /* Substitute the bound metavars into the transcriber body. */
    char *substituted =
        macro_substitute(ctx->arena, hit->transcriber_text, hit->transcriber_len, &env);
    if (!substituted)
        return;

    /* Expansion memo (see RustLSPContext.macro_memo): reset per top-level
     * invocation, dedup identical (macro, substituted body) within the
     * recursion chain — kills the exponential breadth of self-recursive
     * macro_rules without losing any distinctly-attributed source site. */
    if (ctx->macro_expand_depth == 0 && ctx->macro_memo.slots) {
        memset(ctx->macro_memo.slots, 0, sizeof(uint64_t) * (size_t)ctx->macro_memo.cap);
        ctx->macro_memo.count = 0;
    }
    uint64_t mm_key = cbm_negmemo_key(3, mname, substituted);
    if (cbm_negmemo_contains(&ctx->macro_memo, mm_key)) {
        return;
    }
    cbm_negmemo_insert(&ctx->macro_memo, ctx->arena, mm_key);

    /* Wrap and parse. */
    char *wrapped = cbm_arena_sprintf(ctx->arena, "fn __cbm_macro_expand() { %s; }\n", substituted);
    if (!wrapped)
        return;

    TSParser *parser = ts_parser_new();
    if (!parser)
        return;
    ts_parser_set_language(parser, tree_sitter_rust());
    TSTree *tree = ts_parser_parse_string(parser, NULL, wrapped, (uint32_t)strlen(wrapped));
    if (tree) {
        ctx->macro_expand_depth++;
        TSNode root = ts_tree_root_node(tree);
        uint32_t rnc = ts_node_child_count(root);
        for (uint32_t i = 0; i < rnc; i++) {
            TSNode top = ts_node_child(root, i);
            if (ts_node_is_null(top))
                continue;
            if (strcmp(ts_node_type(top), "function_item") != 0)
                continue;
            TSNode body = ts_node_child_by_field_name(top, "body", 4);
            if (ts_node_is_null(body))
                continue;
            /* Swap source so node_text inside reads from `wrapped`. */
            const char *saved_source = ctx->source;
            int saved_len = ctx->source_len;
            ctx->source = wrapped;
            ctx->source_len = (int)strlen(wrapped);
            rust_resolve_calls_in_node(ctx, body);
            ctx->source = saved_source;
            ctx->source_len = saved_len;
        }
        ctx->macro_expand_depth--;
        ts_tree_delete(tree);
    }
    ts_parser_delete(parser);
}

/* Re-parse the argument list of a built-in expression-macro (format!,
 * println!, assert!, …) as ordinary Rust expressions and walk them for
 * calls. The tree-sitter-rust grammar tokenises macro arguments rather than
 * parsing them as expression AST, so a method call like `format!("{}",
 * d.label())` never appears as a call_expression/field_expression node and
 * rust_walk_macro_tokens cannot recover it. Because these macros DO evaluate
 * their arguments as normal expressions, re-parsing the argument text and
 * resolving calls in it is sound — it recovers exactly the calls the program
 * makes. We wrap the args in a block so each comma-separated argument parses
 * as its own statement; the current scope (params/locals) is preserved so
 * typed receivers still resolve. */
static void rust_resolve_macro_arg_exprs(RustLSPContext *ctx, TSNode invocation) {
    if (!ctx || ts_node_is_null(invocation))
        return;
    if (ctx->macro_expand_depth >= 8)
        return;

    /* The grammar exposes the argument list as a `token_tree` child rather
     * than via an `arguments` field, so locate it by node type. */
    TSNode args_tt = {0};
    uint32_t inc = ts_node_child_count(invocation);
    for (uint32_t i = 0; i < inc; i++) {
        TSNode c = ts_node_child(invocation, i);
        if (!ts_node_is_null(c) && strcmp(ts_node_type(c), "token_tree") == 0) {
            args_tt = c;
            break;
        }
    }
    if (ts_node_is_null(args_tt))
        return;
    char *at = cbm_node_text(ctx->arena, args_tt, ctx->source);
    if (!at)
        return;
    const char *inner;
    int inner_len;
    rust_macro_strip_outer(at, (int)strlen(at), &inner, &inner_len);
    if (inner_len <= 0)
        return;
    char *arg_text = cbm_arena_strndup(ctx->arena, inner, (size_t)inner_len);
    if (!arg_text)
        return;

    /* Same expansion memo as rust_expand_user_macro (site tag 4), but ONLY
     * inside a recursion chain (depth > 0): there the identical argument text
     * is re-parsed on every re-expansion of a self-recursive macro and has no
     * distinct source site. Top-level invocations (depth 0) always parse —
     * identical args in different enclosing functions attribute differently
     * and must all be walked. */
    if (ctx->macro_expand_depth > 0) {
        uint64_t am_key = cbm_negmemo_key(4, arg_text, NULL);
        if (cbm_negmemo_contains(&ctx->macro_memo, am_key)) {
            return;
        }
        cbm_negmemo_insert(&ctx->macro_memo, ctx->arena, am_key);
    }

    /* Wrap the comma-separated arguments in a tuple expression so the whole
     * thing parses as one valid expression (a trailing format-spec arg like
     * `width = w` would otherwise break statement parsing). Calls inside any
     * element are still walked. */
    char *wrapped =
        cbm_arena_sprintf(ctx->arena, "fn __cbm_macro_args() { let _ = (%s); }\n", arg_text);
    if (!wrapped)
        return;

    TSParser *parser = ts_parser_new();
    if (!parser)
        return;
    ts_parser_set_language(parser, tree_sitter_rust());
    TSTree *tree = ts_parser_parse_string(parser, NULL, wrapped, (uint32_t)strlen(wrapped));
    if (tree) {
        TSNode root = ts_tree_root_node(tree);
        /* Bail out if the synthetic source failed to parse cleanly — a
         * format-string-only arg, named args, or other non-expression token
         * soup must not produce bogus edges. */
        if (!ts_node_has_error(root)) {
            ctx->macro_expand_depth++;
            uint32_t rnc = ts_node_child_count(root);
            for (uint32_t i = 0; i < rnc; i++) {
                TSNode top = ts_node_child(root, i);
                if (ts_node_is_null(top))
                    continue;
                if (strcmp(ts_node_type(top), "function_item") != 0)
                    continue;
                TSNode body = ts_node_child_by_field_name(top, "body", 4);
                if (ts_node_is_null(body))
                    continue;
                const char *saved_source = ctx->source;
                int saved_len = ctx->source_len;
                ctx->source = wrapped;
                ctx->source_len = (int)strlen(wrapped);
                /* The syntactic extractor never produced call nodes for these
                 * macro-hidden expressions, so resolved_calls emitted here have
                 * no CBMCall to pair with. Inject matching synthetic calls. */
                ctx->inject_syn_calls++;
                rust_resolve_calls_in_node(ctx, body);
                ctx->inject_syn_calls--;
                ctx->source = saved_source;
                ctx->source_len = saved_len;
            }
            ctx->macro_expand_depth--;
        }
        ts_tree_delete(tree);
    }
    ts_parser_delete(parser);
}

/* ════════════════════════════════════════════════════════════════════
 * 9. Statement / pattern binding
 * ════════════════════════════════════════════════════════════════════ */

/* Recursively bind every identifier inside a pattern node to the given
 * fallback type. Handles tuple_pattern, struct_pattern, tuple_struct_pattern,
 * ref_pattern, mut_pattern, captured_pattern, identifier. */
static void rust_bind_pattern(RustLSPContext *ctx, TSNode pattern, const CBMType *type) {
    if (ts_node_is_null(pattern))
        return;
    const char *kind = ts_node_type(pattern);

    if (strcmp(kind, "identifier") == 0) {
        char *name = rust_node_text(ctx, pattern);
        if (name && strcmp(name, "_") != 0) {
            cbm_scope_bind(ctx->current_scope, name, type);
        }
        return;
    }
    if (strcmp(kind, "captured_pattern") == 0) {
        /* name @ subpattern */
        TSNode name_node = ts_node_child_by_field_name(pattern, "name", 4);
        if (!ts_node_is_null(name_node)) {
            char *name = rust_node_text(ctx, name_node);
            if (name && strcmp(name, "_") != 0) {
                cbm_scope_bind(ctx->current_scope, name, type);
            }
        }
        TSNode sub = ts_node_child_by_field_name(pattern, "pattern", 7);
        if (!ts_node_is_null(sub))
            rust_bind_pattern(ctx, sub, type);
        return;
    }
    if (strcmp(kind, "ref_pattern") == 0 || strcmp(kind, "mut_pattern") == 0 ||
        strcmp(kind, "reference_pattern") == 0) {
        if (ts_node_named_child_count(pattern) > 0) {
            rust_bind_pattern(ctx, ts_node_named_child(pattern, 0), type);
        }
        return;
    }
    if (strcmp(kind, "tuple_pattern") == 0) {
        const CBMType *base = type;
        while (base && base->kind == CBM_TYPE_REFERENCE)
            base = base->data.reference.elem;
        uint32_t nc = ts_node_named_child_count(pattern);
        for (uint32_t i = 0; i < nc; i++) {
            const CBMType *elem_t = cbm_type_unknown();
            if (base && base->kind == CBM_TYPE_TUPLE && (int)i < base->data.tuple.count) {
                elem_t = base->data.tuple.elems[i];
            }
            rust_bind_pattern(ctx, ts_node_named_child(pattern, i), elem_t);
        }
        return;
    }
    if (strcmp(kind, "tuple_struct_pattern") == 0) {
        /* Some(x), Ok(x), Err(e) — peel one Option/Result/template. */
        const CBMType *base = type;
        while (base && base->kind == CBM_TYPE_REFERENCE)
            base = base->data.reference.elem;
        const CBMType *inner = cbm_type_unknown();
        if (base && base->kind == CBM_TYPE_TEMPLATE && base->data.template_type.arg_count > 0) {
            inner = base->data.template_type.template_args[0];
        }
        uint32_t nc = ts_node_named_child_count(pattern);
        /* First named child is the path; subsequent are sub-patterns. */
        for (uint32_t i = 1; i < nc; i++) {
            rust_bind_pattern(ctx, ts_node_named_child(pattern, i), inner);
        }
        return;
    }
    if (strcmp(kind, "struct_pattern") == 0) {
        /* For each field_pattern, bind the local name to the field's type. */
        TSNode body = ts_node_child_by_field_name(pattern, "body", 4);
        TSNode iter = ts_node_is_null(body) ? pattern : body;
        const CBMType *base = type;
        while (base && base->kind == CBM_TYPE_REFERENCE)
            base = base->data.reference.elem;
        const char *type_qn = NULL;
        if (base && base->kind == CBM_TYPE_NAMED)
            type_qn = base->data.named.qualified_name;
        else if (base && base->kind == CBM_TYPE_TEMPLATE)
            type_qn = base->data.template_type.template_name;

        uint32_t nc = ts_node_named_child_count(iter);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode fp = ts_node_named_child(iter, i);
            const char *fk = ts_node_type(fp);
            if (strcmp(fk, "field_pattern") == 0) {
                TSNode name_node = ts_node_child_by_field_name(fp, "name", 4);
                TSNode pat_node = ts_node_child_by_field_name(fp, "pattern", 7);
                char *fname = rust_node_text(ctx, name_node);
                if (!fname)
                    continue;
                const CBMType *ft =
                    type_qn ? rust_lookup_field(ctx, type_qn, fname, 0) : cbm_type_unknown();
                if (!ft)
                    ft = cbm_type_unknown();
                if (!ts_node_is_null(pat_node)) {
                    rust_bind_pattern(ctx, pat_node, ft);
                } else {
                    cbm_scope_bind(ctx->current_scope, fname, ft);
                }
            } else if (strcmp(fk, "shorthand_field_identifier") == 0 ||
                       strcmp(fk, "identifier") == 0) {
                char *fname = rust_node_text(ctx, fp);
                if (fname && strcmp(fname, "_") != 0) {
                    const CBMType *ft =
                        type_qn ? rust_lookup_field(ctx, type_qn, fname, 0) : cbm_type_unknown();
                    cbm_scope_bind(ctx->current_scope, fname, ft ? ft : cbm_type_unknown());
                }
            }
        }
        return;
    }
    if (strcmp(kind, "or_pattern") == 0) {
        /* For an OR pattern we attempt to bind names from the first branch. */
        if (ts_node_named_child_count(pattern) > 0) {
            rust_bind_pattern(ctx, ts_node_named_child(pattern, 0), type);
        }
        return;
    }
    /* Other patterns we ignore for binding purposes. */
}

void rust_process_statement(RustLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    /* let_declaration: let pat: T = expr;
     *
     * Bidirectional inference: if the user wrote `let v: Vec<String> = …;`
     * we pass `Vec<String>` as the expected hint when synthesising the
     * RHS. That lets ambiguous calls like `Vec::new()` keep their full
     * template arguments through the chain. */
    if (strcmp(kind, "let_declaration") == 0) {
        TSNode pat = ts_node_child_by_field_name(node, "pattern", 7);
        TSNode tn = ts_node_child_by_field_name(node, "type", 4);
        TSNode val = ts_node_child_by_field_name(node, "value", 5);

        const CBMType *annotated = NULL;
        if (!ts_node_is_null(tn)) {
            annotated = rust_parse_type_node(ctx, tn);
        }
        const CBMType *let_type = annotated;
        if ((!let_type || cbm_type_is_unknown(let_type)) && !ts_node_is_null(val)) {
            /* Synthesis: evaluate the RHS with the (possibly NULL)
             * annotated type as a hint. */
            let_type = rust_eval_expr_typed(ctx, val, annotated);
        }
        if (!let_type)
            let_type = cbm_type_unknown();
        if (!ts_node_is_null(pat))
            rust_bind_pattern(ctx, pat, let_type);
        return;
    }

    /* const_item / static_item: const NAME: T = …; */
    if (strcmp(kind, "const_item") == 0 || strcmp(kind, "static_item") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        TSNode tn = ts_node_child_by_field_name(node, "type", 4);
        const CBMType *type =
            ts_node_is_null(tn) ? cbm_type_unknown() : rust_parse_type_node(ctx, tn);
        if (!ts_node_is_null(name_node)) {
            char *name = rust_node_text(ctx, name_node);
            if (name)
                cbm_scope_bind(ctx->current_scope, name, type);
        }
        return;
    }
}

/* ════════════════════════════════════════════════════════════════════
 * 10. Function & file walk
 * ════════════════════════════════════════════════════════════════════ */

/* Inject a synthetic CBMCall into result->calls so the downstream pipeline
 * (cbm_pipeline_find_lsp_resolution) can pair it with the resolved_call and
 * emit a CALLS edge. `callee_qn`'s last dot segment is used as the textual
 * callee_name, matching how the resolver's short-name comparison works. Only
 * used for calls the syntactic extractor cannot see (operator desugaring,
 * macro-hidden method calls). */
static void rust_inject_syn_call(RustLSPContext *ctx, const char *callee_qn) {
    if (!ctx || !ctx->syn_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;
    const char *dot = strrchr(callee_qn, '.');
    const char *short_name = dot ? dot + 1 : callee_qn;
    if (!short_name || !short_name[0])
        return;
    CBMCall call = {0};
    call.callee_name = cbm_arena_strdup(ctx->arena, short_name);
    call.enclosing_func_qn = ctx->enclosing_func_qn;
    cbm_calls_push(ctx->syn_calls, ctx->arena, call);
}

static void rust_emit_resolved_call(RustLSPContext *ctx, const char *callee_qn,
                                    const char *strategy, float confidence) {
    if (!ctx || !ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc = {
        .caller_qn = ctx->enclosing_func_qn,
        .callee_qn = callee_qn,
        .strategy = strategy,
        .confidence = confidence,
        .reason = NULL,
    };
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
    if (ctx->inject_syn_calls > 0) {
        rust_inject_syn_call(ctx, callee_qn);
    }
}

static void rust_emit_unresolved_call(RustLSPContext *ctx, const char *expr_text,
                                      const char *reason) {
    if (!ctx || !ctx->resolved_calls || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc = {
        .caller_qn = ctx->enclosing_func_qn,
        .callee_qn = expr_text ? expr_text : "?",
        .strategy = "lsp_unresolved",
        .confidence = 0.0f,
        .reason = reason,
    };
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

/* Entry hook: classify a call_expression and emit the best edge we can. */
static void rust_resolve_call_expression(RustLSPContext *ctx, TSNode node) {
    TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
    TSNode args_node = ts_node_child_by_field_name(node, "arguments", 9);
    if (ts_node_is_null(func_node))
        return;
    const char *fk = ts_node_type(func_node);

    /* Method call via field_expression. The grammar can also expose
     * `s.cast::<T>(x)` as a `generic_function` callee whose inner
     * `function` is a field_expression — peel that wrapper here. */
    if (strcmp(fk, "generic_function") == 0) {
        TSNode inner = ts_node_child_by_field_name(func_node, "function", 8);
        if (!ts_node_is_null(inner)) {
            func_node = inner;
            fk = ts_node_type(func_node);
        }
    }
    if (strcmp(fk, "field_expression") == 0) {
        TSNode value = ts_node_child_by_field_name(func_node, "value", 5);
        TSNode field = ts_node_child_by_field_name(func_node, "field", 5);
        if (ts_node_is_null(value) || ts_node_is_null(field))
            return;
        char *mname = rust_node_text(ctx, field);
        if (!mname)
            return;
        /* Strip turbofish from the method name in case the grammar
         * embedded it as part of the field token. */
        rust_strip_turbofish(mname);

        const CBMType *recv = rust_eval_expr_type(ctx, value);
        const CBMType *base = recv;
        while (base && (base->kind == CBM_TYPE_REFERENCE || base->kind == CBM_TYPE_POINTER)) {
            base = (base->kind == CBM_TYPE_REFERENCE) ? base->data.reference.elem
                                                      : base->data.pointer.elem;
        }

        const char *type_qn = NULL;
        if (base && base->kind == CBM_TYPE_NAMED)
            type_qn = base->data.named.qualified_name;
        else if (base && base->kind == CBM_TYPE_TEMPLATE)
            type_qn = base->data.template_type.template_name;
        else if (base && base->kind == CBM_TYPE_BUILTIN) {
            const char *nm = base->data.builtin.name;
            if (strcmp(nm, "str") == 0)
                type_qn = "core.str";
            else if (strcmp(nm, "String") == 0)
                type_qn = "alloc.string.String";
            else if (is_rust_primitive(nm)) {
                /* Primitive integer / float / char / bool / unit / never
                 * methods are registered under their primitive name. */
                type_qn = nm;
            }
        } else if (base && base->kind == CBM_TYPE_SLICE) {
            /* `&[T]` / `[T]` method dispatch. */
            type_qn = "core.slice";
        }

        if (type_qn) {
            int impl_count = 0;
            const CBMRegisteredFunc *m =
                rust_resolve_trait_method(ctx, type_qn, mname, &impl_count);
            if (m) {
                const char *strategy = "lsp_method_dispatch";
                float conf = CBM_RUST_CONF_METHOD;
                if (m->receiver_type && strcmp(m->receiver_type, type_qn) != 0) {
                    strategy = "lsp_trait_dispatch";
                    conf = (impl_count == 1) ? CBM_RUST_CONF_TRAIT_SOLE : CBM_RUST_CONF_TRAIT_AMB;
                } else if (rust_method_is_trait_impl(ctx, type_qn, mname)) {
                    // Inherently resolved, but the method comes from a trait impl
                    // (`impl Trait for Type`) → polymorphic trait dispatch.
                    strategy = "lsp_trait_dispatch";
                    conf = CBM_RUST_CONF_TRAIT_SOLE;
                }
                rust_emit_resolved_call(ctx, m->qualified_name, strategy, conf);
                (void)args_node;
                return;
            }

            /* Walk the Deref chain — `Box<T>::method` may live on `T`,
             * `Rc<RefCell<T>>::method` peels two levels, etc. We bound
             * the chain at 8 hops to mirror the rust-analyzer cap. */
            const CBMType *cur = base;
            for (int hop = 0; hop < 8; hop++) {
                const CBMType *next = rust_deref_step(ctx, cur);
                if (!next)
                    break;
                /* Unwrap reference layers introduced by deref. */
                while (next &&
                       (next->kind == CBM_TYPE_REFERENCE || next->kind == CBM_TYPE_POINTER)) {
                    next = (next->kind == CBM_TYPE_REFERENCE) ? next->data.reference.elem
                                                              : next->data.pointer.elem;
                }
                if (!next)
                    break;
                const char *next_qn = NULL;
                if (next->kind == CBM_TYPE_NAMED)
                    next_qn = next->data.named.qualified_name;
                else if (next->kind == CBM_TYPE_TEMPLATE)
                    next_qn = next->data.template_type.template_name;
                else if (next->kind == CBM_TYPE_BUILTIN) {
                    const char *nm = next->data.builtin.name;
                    if (strcmp(nm, "str") == 0)
                        next_qn = "core.str";
                    else if (strcmp(nm, "String") == 0)
                        next_qn = "alloc.string.String";
                }
                if (!next_qn)
                    break;
                int hop_impls = 0;
                const CBMRegisteredFunc *hm =
                    rust_resolve_trait_method(ctx, next_qn, mname, &hop_impls);
                if (hm) {
                    rust_emit_resolved_call(ctx, hm->qualified_name, "lsp_deref_dispatch",
                                            CBM_RUST_CONF_PROMOTED);
                    return;
                }
                cur = next;
            }

            /* Chalk-lite: receiver is typed as a NAMED with a name
             * that matches a current type-param bound. Resolve through
             * the bound trait. */
            {
                /* Just the local tail name. */
                const char *short_qn = type_qn;
                const char *dot = strrchr(type_qn, '.');
                if (dot)
                    short_qn = dot + 1;
                const char *bound = rust_lookup_type_param_bound(ctx, short_qn);
                if (bound) {
                    int bimpls = 0;
                    const CBMRegisteredFunc *bm =
                        rust_resolve_trait_method(ctx, bound, mname, &bimpls);
                    if (bm) {
                        rust_emit_resolved_call(ctx, bm->qualified_name, "lsp_bound_dispatch",
                                                CBM_RUST_CONF_TRAIT_AMB);
                        return;
                    }
                }
            }

            /* Prelude trait method best-effort. */
            if (is_prelude_trait_method(mname)) {
                rust_emit_resolved_call(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, mname),
                                        "lsp_prelude_trait", CBM_RUST_CONF_TRAIT_AMB);
                return;
            }
            rust_emit_unresolved_call(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, mname),
                                      "method_not_found");
            return;
        }

        /* Receiver type is unknown — record best-effort with the textual
         * receiver path so downstream can still see what we tried. */
        char *recv_text = rust_node_text(ctx, value);
        rust_emit_unresolved_call(
            ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", recv_text ? recv_text : "?", mname),
            "unknown_receiver_type");
        return;
    }

    /* Direct identifier or scoped path call. */
    if (strcmp(fk, "identifier") == 0 || strcmp(fk, "scoped_identifier") == 0 ||
        strcmp(fk, "generic_function") == 0) {
        TSNode actual_func = func_node;
        if (strcmp(fk, "generic_function") == 0) {
            TSNode inner = ts_node_child_by_field_name(actual_func, "function", 8);
            if (!ts_node_is_null(inner))
                actual_func = inner;
        }
        char *path = rust_node_text(ctx, actual_func);
        if (!path)
            return;
        /* Strip ALL turbofish (`Vec::<i32>::new` → `Vec::new`). */
        rust_strip_turbofish(path);

        const char *qn = rust_resolve_path_expr(ctx, path);
        if (!qn)
            return;

        /* Try registered free function first. Also try module-prefixed
         * fallback so `Logger::new` (which resolves to "Logger.new")
         * still finds the project's `<module>.Logger.new`. */
        if (cbm_registry_lookup_func(ctx->registry, qn)) {
            rust_emit_resolved_call(ctx, qn, "lsp_direct", CBM_RUST_CONF_DIRECT);
            return;
        }
        if (ctx->module_qn && strstr(qn, ".") == NULL) {
            const char *full = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, qn);
            if (cbm_registry_lookup_func(ctx->registry, full)) {
                rust_emit_resolved_call(ctx, full, "lsp_direct", CBM_RUST_CONF_DIRECT);
                return;
            }
        }

        /* UFCS form: T::method or trait_qn::method. */
        const char *dot = strrchr(qn, '.');
        if (dot) {
            char *head = cbm_arena_strndup(ctx->arena, qn, (size_t)(dot - qn));
            const char *short_name = dot + 1;
            /* If `head` is a trait, `Trait::method` UFCS resolves to the sole
             * concrete impl (lsp_trait_ufcs), NEVER the trait's own abstract
             * method that the inherent lookup below would find. Resolve the trait
             * QN (head or module-qualified) via its is_interface flag — set at
             * type-registration time, so it is reliable even on an early pass
             * before impl links are wired. When the impl isn't known yet, emit
             * nothing: a partial-pass lsp_ufcs to the abstract method would
             * otherwise outrank (higher conf) the real trait_ufcs from the
             * complete pass and win the join. */
            const char *trait_qn = NULL;
            const CBMRegisteredType *head_t = cbm_registry_lookup_type(ctx->registry, head);
            if (head_t && head_t->is_interface) {
                trait_qn = head;
            } else if (ctx->module_qn) {
                const char *fh = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, head);
                const CBMRegisteredType *ft = cbm_registry_lookup_type(ctx->registry, fh);
                if (ft && ft->is_interface)
                    trait_qn = fh;
            }
            if (trait_qn) {
                int tn = 0;
                const CBMRegisteredFunc *ti_m =
                    rust_find_sole_trait_impl(ctx, trait_qn, short_name, &tn);
                if (tn >= 1) {
                    rust_emit_resolved_call(
                        ctx,
                        ti_m ? ti_m->qualified_name
                             : cbm_arena_sprintf(ctx->arena, "%s.%s", trait_qn, short_name),
                        tn == 1 ? "lsp_trait_ufcs" : "lsp_trait_ufcs_amb",
                        tn == 1 ? CBM_RUST_CONF_TRAIT_SOLE : CBM_RUST_CONF_TRAIT_AMB);
                }
                return;
            }
            const CBMRegisteredFunc *m =
                cbm_registry_lookup_method_aliased(ctx->registry, head, short_name);
            if (!m && ctx->module_qn) {
                /* Fall back to module-qualified head: `Logger.new` →
                 * `<module>.Logger.new`. */
                const char *full_head =
                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, head);
                m = cbm_registry_lookup_method_aliased(ctx->registry, full_head, short_name);
            }
            if (m) {
                rust_emit_resolved_call(ctx, m->qualified_name,
                                        strcmp(short_name, "new") == 0 ? "lsp_constructor"
                                                                       : "lsp_ufcs",
                                        CBM_RUST_CONF_UFCS);
                return;
            }
            /* Trait method through single-impl dispatch. */
            int impls = 0;
            const CBMRegisteredFunc *tm = rust_resolve_trait_method(ctx, head, short_name, &impls);
            if (!tm && ctx->module_qn) {
                const char *full_head =
                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, head);
                tm = rust_resolve_trait_method(ctx, full_head, short_name, &impls);
            }
            if (tm) {
                rust_emit_resolved_call(
                    ctx, tm->qualified_name, impls == 1 ? "lsp_trait_ufcs" : "lsp_trait_ufcs_amb",
                    impls == 1 ? CBM_RUST_CONF_TRAIT_SOLE : CBM_RUST_CONF_TRAIT_AMB);
                return;
            }
        }

        const char *tail = strrchr(path, ':');
        if (tail && tail > path && tail[-1] == ':') {
            tail += 1;
        } else {
            tail = path;
        }

        /* Cross-crate workspace-member resolution (#56): when the call
         * path's head is a declared Cargo workspace member (e.g.
         * `crate_a::helper` from inside crate_b) we cannot rely on the
         * caller-crate-scoped fallback below — that filters by the
         * CALLER's module prefix and would resolve to a same-named local
         * function instead. Route to the function defined inside the
         * MEMBER crate by matching the registered QN's `.<member>.`
         * path segment plus the call tail. Requires a parsed manifest
         * (threaded through pass_lsp_cross.c); NULL manifest skips this. */
        if (ctx->cargo_manifest && tail && *tail) {
            const char *head_sep = strstr(path, "::");
            if (head_sep && head_sep > path) {
                char *head = cbm_arena_strndup(ctx->arena, path, (size_t)(head_sep - path));
                const CBMCargoManifest *m = (const CBMCargoManifest *)ctx->cargo_manifest;
                if (head && cbm_cargo_find_member(m, head)) {
                    /* `.crate_a.` — the member directory appears as a dotted
                     * QN segment for every def inside that crate. */
                    char *needle = cbm_arena_sprintf(ctx->arena, ".%s.", head);
                    const CBMRegisteredFunc *mem_unique = NULL;
                    int mem_matches = 0;
                    /* Iterate only free funcs whose short_name == tail via the index;
                     * the receiver/short_name/needle re-checks below are unchanged. */
                    CBMFreeFuncIter ffit;
                    cbm_registry_free_funcs_by_short_name(ctx->registry, tail, &ffit);
                    for (int i; mem_matches < 2 && (i = cbm_free_func_iter_next(&ffit)) >= 0;) {
                        const CBMRegisteredFunc *f = &ctx->registry->funcs[i];
                        if (!f->short_name || !f->qualified_name)
                            continue;
                        if (f->receiver_type)
                            continue; /* free functions only */
                        if (strcmp(f->short_name, tail) != 0)
                            continue;
                        if (!strstr(f->qualified_name, needle))
                            continue; /* not defined in the member crate */
                        mem_matches++;
                        if (mem_matches == 1)
                            mem_unique = f;
                    }
                    if (mem_matches == 1 && mem_unique) {
                        rust_emit_resolved_call(ctx, mem_unique->qualified_name, "lsp_cross_crate",
                                                CBM_RUST_CONF_DIRECT);
                        return;
                    }
                }
            }
        }

        /* Global short-name fallback: scan the registry for a unique
         * function whose short_name matches the path's tail and whose
         * QN starts with the current crate prefix. This gives `mod
         * foo; use foo::bar; bar()` a chance to resolve when the
         * intermediate module wasn't tracked through an explicit
         * use-map entry. */
        if (tail && *tail && ctx->module_qn) {
            /* Crate prefix is the first dotted segment of module_qn after
             * the project name, but for simplicity we just match on
             * "starts with first dot-segment". */
            const char *first_dot = strchr(ctx->module_qn, '.');
            size_t crate_len =
                first_dot ? (size_t)(first_dot - ctx->module_qn) : strlen(ctx->module_qn);
            const CBMRegisteredFunc *unique = NULL;
            int matches = 0;
            /* Iterate only free funcs whose short_name == tail via the index; the
             * receiver/short_name/crate-prefix re-checks below are unchanged. */
            CBMFreeFuncIter ffit;
            cbm_registry_free_funcs_by_short_name(ctx->registry, tail, &ffit);
            for (int i; matches < 2 && (i = cbm_free_func_iter_next(&ffit)) >= 0;) {
                const CBMRegisteredFunc *f = &ctx->registry->funcs[i];
                if (!f->short_name || !f->qualified_name)
                    continue;
                if (f->receiver_type)
                    continue; /* free functions only */
                if (strcmp(f->short_name, tail) != 0)
                    continue;
                /* Crate-scoped: QN must start with the same prefix. */
                if (strncmp(f->qualified_name, ctx->module_qn, crate_len) != 0)
                    continue;
                matches++;
                if (matches == 1)
                    unique = f;
            }
            if (matches == 1 && unique) {
                rust_emit_resolved_call(ctx, unique->qualified_name, "lsp_short_name_unique",
                                        CBM_RUST_CONF_PROMOTED);
                return;
            }
        }

        /* Last-ditch: emit with the resolved path. */
        rust_emit_unresolved_call(ctx, qn, "function_not_in_registry");
        return;
    }
}

/* Walk every node in a function body, recording calls and refining scope
 * for control-flow constructs that bind variables. */
#define CBM_RUST_EVAL_STEP_CAP 200000 /* per-file budget */
static void rust_resolve_calls_in_node(RustLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    /* Pathological-input guard: bail out once we've spent too many
     * eval steps on this file. Prevents hangs on adversarial input. */
    if (ctx->eval_step_count > CBM_RUST_EVAL_STEP_CAP)
        return;
    ctx->eval_step_count++;
    const char *kind = ts_node_type(node);

    /* Bind variables introduced by this statement. */
    rust_process_statement(ctx, node);

    /* Resolve a call expression. */
    if (strcmp(kind, "call_expression") == 0) {
        rust_resolve_call_expression(ctx, node);
        /* Closure-parameter inference: when the call is a known
         * iterator-style method that takes a closure of `Item`, stash
         * the receiver's element type so the closure_expression child
         * binds its first param to it. We compute this here (after
         * the call edge is emitted) so the recursion picks it up. */
        TSNode fn = ts_node_child_by_field_name(node, "function", 8);
        if (!ts_node_is_null(fn) && strcmp(ts_node_type(fn), "field_expression") == 0) {
            TSNode val = ts_node_child_by_field_name(fn, "value", 5);
            TSNode fld = ts_node_child_by_field_name(fn, "field", 5);
            if (!ts_node_is_null(val) && !ts_node_is_null(fld)) {
                char *mname = rust_node_text(ctx, fld);
                static const char *item_methods[] = {
                    "map",    "filter",     "for_each",   "find",        "position", "any",
                    "all",    "take_while", "skip_while", "filter_map",  "inspect",  "max_by",
                    "min_by", "max_by_key", "min_by_key", "sort_by_key", NULL};
                bool is_item_method = false;
                if (mname) {
                    for (const char **mm = item_methods; *mm; mm++) {
                        if (strcmp(mname, *mm) == 0) {
                            is_item_method = true;
                            break;
                        }
                    }
                }
                if (is_item_method) {
                    const CBMType *recv = rust_eval_expr_type(ctx, val);
                    const CBMType *base = recv;
                    while (base &&
                           (base->kind == CBM_TYPE_REFERENCE || base->kind == CBM_TYPE_POINTER)) {
                        base = (base->kind == CBM_TYPE_REFERENCE) ? base->data.reference.elem
                                                                  : base->data.pointer.elem;
                    }
                    if (base && base->kind == CBM_TYPE_TEMPLATE &&
                        base->data.template_type.arg_count > 0) {
                        const char *tn = base->data.template_type.template_name;
                        if (tn && (strstr(tn, "Iterator") || strstr(tn, "Vec") ||
                                   strstr(tn, "VecDeque") || strstr(tn, "Option") ||
                                   strstr(tn, "Result") || strstr(tn, "HashSet") ||
                                   strstr(tn, "BTreeSet") || strstr(tn, "Slice"))) {
                            ctx->pending_closure_param_type =
                                base->data.template_type.template_args[0];
                        }
                    } else if (base && base->kind == CBM_TYPE_SLICE) {
                        ctx->pending_closure_param_type = base->data.slice.elem;
                    }
                }
            }
        }
        /* Continue recursion so calls inside arguments are also seen. */
    }

    /* Operator-overload desugaring: `a + b` calls <T as Add>::add when the
     * left operand is a user-defined type T with that operator method;
     * `a[i]` calls T::index. The tree-sitter-rust grammar models these as
     * binary_expression / index_expression rather than call_expression, so
     * lang_specs.c's call-type whitelist never sees them — we recover the
     * call edge here. Sound-only via rust_emit_operator_call (no edge unless
     * the operand type actually defines the method). */
    if (strcmp(kind, "binary_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        if (!ts_node_is_null(left)) {
            for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
                TSNode c = ts_node_child(node, i);
                if (ts_node_is_named(c))
                    continue;
                char *op = rust_node_text(ctx, c);
                const char *method = rust_binop_trait_method(op);
                if (method) {
                    rust_emit_operator_call(ctx, rust_eval_expr_type(ctx, left), method);
                }
                break; /* operator is the sole anonymous child */
            }
        }
    } else if (strcmp(kind, "index_expression") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(value) && ts_node_named_child_count(node) > 0) {
            value = ts_node_named_child(node, 0);
        }
        if (!ts_node_is_null(value)) {
            rust_emit_operator_call(ctx, rust_eval_expr_type(ctx, value), "index");
        }
    }

    /* Macro invocation: walk inner tokens for nested calls and try the
     * macro-as-function mapping. */
    if (strcmp(kind, "macro_invocation") == 0) {
        TSNode mname_node = ts_node_child_by_field_name(node, "macro", 5);
        if (!ts_node_is_null(mname_node)) {
            char *mname = rust_node_text(ctx, mname_node);
            if (mname) {
                /* For known std macros emit a synthetic call under their
                 * canonical paths so trace tools can see the dependency. */
                const char *path = NULL;
                /* Macros whose arguments are ordinary Rust expressions — their
                 * call sites are lost to tree-sitter's macro tokenisation, so
                 * re-parse the args to recover calls like `format!("{}",
                 * d.label())`. */
                bool expr_arg_macro =
                    strcmp(mname, "println") == 0 || strcmp(mname, "eprintln") == 0 ||
                    strcmp(mname, "print") == 0 || strcmp(mname, "eprint") == 0 ||
                    strcmp(mname, "format") == 0 || strcmp(mname, "write") == 0 ||
                    strcmp(mname, "writeln") == 0 || strcmp(mname, "panic") == 0 ||
                    strcmp(mname, "assert") == 0 || strcmp(mname, "assert_eq") == 0 ||
                    strcmp(mname, "assert_ne") == 0 || strcmp(mname, "debug_assert") == 0 ||
                    strcmp(mname, "debug_assert_eq") == 0 ||
                    strcmp(mname, "debug_assert_ne") == 0 || strcmp(mname, "dbg") == 0;
                if (expr_arg_macro) {
                    rust_resolve_macro_arg_exprs(ctx, node);
                }
                if (strcmp(mname, "println") == 0 || strcmp(mname, "eprintln") == 0 ||
                    strcmp(mname, "print") == 0 || strcmp(mname, "eprint") == 0 ||
                    strcmp(mname, "format") == 0 || strcmp(mname, "write") == 0 ||
                    strcmp(mname, "writeln") == 0) {
                    path = cbm_arena_sprintf(ctx->arena, "std.macros.%s", mname);
                } else if (strcmp(mname, "vec") == 0) {
                    path = "alloc.vec.vec";
                } else if (strcmp(mname, "panic") == 0) {
                    path = "core.panicking.panic";
                } else if (strcmp(mname, "include") == 0) {
                    /* `include!` pulls another file in at compile time —
                     * we never see the included source. Emit a
                     * documentation edge so trace tools can flag it. */
                    path = "core.macros.include";
                } else if (strcmp(mname, "include_str") == 0 ||
                           strcmp(mname, "include_bytes") == 0) {
                    /* Equivalent for data inclusion. */
                    path = cbm_arena_sprintf(ctx->arena, "core.macros.%s", mname);
                } else if (strcmp(mname, "env") == 0 || strcmp(mname, "option_env") == 0) {
                    /* Compile-time env var read. Specifically: an
                     * `include!(concat!(env!("OUT_DIR"), …))` pattern
                     * indicates code generated by a build.rs that we
                     * cannot see. We surface the env! call so trace
                     * tools know to look for OUT_DIR. */
                    path = cbm_arena_sprintf(ctx->arena, "core.macros.%s", mname);
                }
                if (path) {
                    rust_emit_resolved_call(ctx, path, "lsp_macro", CBM_RUST_CONF_MACRO_KNOWN);
                } else {
                    /* User-defined macro: try expanding via macro_rules!. */
                    rust_expand_user_macro(ctx, mname, node);
                }
            }
        }
        TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
        if (!ts_node_is_null(args))
            rust_walk_macro_tokens(ctx, args);
        /* Don't recurse normally below — we already drilled into args. */
        return;
    }

    /* Push a fresh scope for blocks and constructs introducing new bindings. */
    bool push_scope =
        (strcmp(kind, "block") == 0 || strcmp(kind, "if_expression") == 0 ||
         strcmp(kind, "if_let_expression") == 0 || strcmp(kind, "while_expression") == 0 ||
         strcmp(kind, "while_let_expression") == 0 || strcmp(kind, "for_expression") == 0 ||
         strcmp(kind, "match_arm") == 0 || strcmp(kind, "closure_expression") == 0);

    CBMScope *saved = ctx->current_scope;
    if (push_scope) {
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    }

    /* if_let / while_let bind a pattern from the value's matched form.
     * Modern tree-sitter-rust parses `if let X = y { ... }` as
     * `if_expression` containing a `let_condition` child rather than as
     * the legacy `if_let_expression`. Same for `while let`. We handle
     * both shapes here. */
    if (strcmp(kind, "if_let_expression") == 0 || strcmp(kind, "while_let_expression") == 0) {
        TSNode pat = ts_node_child_by_field_name(node, "pattern", 7);
        TSNode val = ts_node_child_by_field_name(node, "value", 5);
        if (!ts_node_is_null(pat) && !ts_node_is_null(val)) {
            const CBMType *vt = rust_eval_expr_type(ctx, val);
            rust_bind_pattern(ctx, pat, vt);
        }
    }
    if (strcmp(kind, "if_expression") == 0 || strcmp(kind, "while_expression") == 0) {
        /* Look for a let_condition (or let_chain) anywhere in the
         * condition slot. */
        TSNode cond = ts_node_child_by_field_name(node, "condition", 9);
        if (!ts_node_is_null(cond)) {
            uint32_t nc2 = ts_node_named_child_count(cond);
            /* Walk one level down for let_condition / let_chain. */
            const char *ck = ts_node_type(cond);
            TSNode targets[8];
            int tcount = 0;
            if (strcmp(ck, "let_condition") == 0) {
                targets[tcount++] = cond;
            } else if (strcmp(ck, "let_chain") == 0) {
                for (uint32_t i = 0; i < nc2 && tcount < 8; i++) {
                    TSNode c2 = ts_node_named_child(cond, i);
                    if (strcmp(ts_node_type(c2), "let_condition") == 0) {
                        targets[tcount++] = c2;
                    }
                }
            }
            for (int t = 0; t < tcount; t++) {
                TSNode lc = targets[t];
                TSNode pat = ts_node_child_by_field_name(lc, "pattern", 7);
                TSNode val = ts_node_child_by_field_name(lc, "value", 5);
                if (!ts_node_is_null(pat) && !ts_node_is_null(val)) {
                    const CBMType *vt = rust_eval_expr_type(ctx, val);
                    rust_bind_pattern(ctx, pat, vt);
                }
            }
        }
    }

    /* for_expression: bind the loop variable from the iter's element type. */
    if (strcmp(kind, "for_expression") == 0) {
        TSNode pat = ts_node_child_by_field_name(node, "pattern", 7);
        TSNode val = ts_node_child_by_field_name(node, "value", 5);
        if (!ts_node_is_null(pat) && !ts_node_is_null(val)) {
            const CBMType *vt = rust_eval_expr_type(ctx, val);
            const CBMType *base = vt;
            while (base && base->kind == CBM_TYPE_REFERENCE)
                base = base->data.reference.elem;
            const CBMType *elem = cbm_type_unknown();
            if (base && base->kind == CBM_TYPE_SLICE) {
                elem = base->data.slice.elem;
            } else if (base && base->kind == CBM_TYPE_TEMPLATE) {
                const char *nm = base->data.template_type.template_name;
                if ((strstr(nm, "Vec") || strstr(nm, "VecDeque") || strstr(nm, "Iterator") ||
                     strstr(nm, "Range")) &&
                    base->data.template_type.arg_count > 0) {
                    elem = base->data.template_type.template_args[0];
                }
                if ((strstr(nm, "HashMap") || strstr(nm, "BTreeMap")) &&
                    base->data.template_type.arg_count > 1) {
                    /* Iter over (K, V) tuples. */
                    const CBMType *pair[2] = {base->data.template_type.template_args[0],
                                              base->data.template_type.template_args[1]};
                    elem = cbm_type_tuple(ctx->arena, pair, 2);
                }
            }
            rust_bind_pattern(ctx, pat, elem);
        }
    }

    /* match_expression: per-arm scope handled when we descend. */
    if (strcmp(kind, "match_arm") == 0) {
        /* The match value type is captured by the parent walker — best
         * effort: peek at the arm's pattern and let rust_bind_pattern do the
         * work using cbm_type_unknown() if we cannot derive it. */
    }

    /* closure_expression: bind closure parameters.
     *
     * Priority order for each param:
     *   1. Explicit type annotation (`|n: &i32|`) — use it directly.
     *   2. `ctx->pending_closure_param_type` for the FIRST param when the
     *      surrounding call resolver inferred one (`.map(|x| ...)` on
     *      Iterator<T>).
     *   3. Otherwise unknown.
     *
     * After binding, clear the pending type so it doesn't leak into a
     * sibling closure. */
    if (strcmp(kind, "closure_expression") == 0) {
        const CBMType *hint = ctx->pending_closure_param_type;
        ctx->pending_closure_param_type = NULL;
        TSNode params = ts_node_child_by_field_name(node, "parameters", 10);
        if (!ts_node_is_null(params)) {
            uint32_t pc = ts_node_named_child_count(params);
            for (uint32_t i = 0; i < pc; i++) {
                TSNode p = ts_node_named_child(params, i);
                /* For `parameter`-shaped nodes, peel off the type
                 * annotation if present; otherwise treat the whole node
                 * as the pattern. */
                TSNode pat = p;
                const CBMType *bound = (i == 0 && hint) ? hint : cbm_type_unknown();
                if (strcmp(ts_node_type(p), "parameter") == 0) {
                    TSNode tn = ts_node_child_by_field_name(p, "type", 4);
                    TSNode pn = ts_node_child_by_field_name(p, "pattern", 7);
                    if (!ts_node_is_null(pn))
                        pat = pn;
                    if (!ts_node_is_null(tn)) {
                        bound = rust_parse_type_node(ctx, tn);
                    }
                }
                rust_bind_pattern(ctx, pat, bound);
            }
        }
    }

    /* Recurse. */
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(node, i);
        if (!ts_node_is_null(c)) {
            rust_resolve_calls_in_node(ctx, c);
        }
    }

    if (push_scope) {
        ctx->current_scope = saved;
    }
}

/* Process a single function: bind parameters / `self`, then walk body. */
static void rust_process_function(RustLSPContext *ctx, TSNode func_node, const char *parent_qn) {
    TSNode name_node = ts_node_child_by_field_name(func_node, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *name = rust_node_text(ctx, name_node);
    if (!name || !name[0])
        return;

    const char *prefix = parent_qn ? parent_qn : ctx->module_qn;
    ctx->enclosing_func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", prefix, name);

    CBMScope *saved = ctx->current_scope;
    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

    /* Chalk-lite: capture the active function's type-parameter bounds
     * and where-clause bounds into ctx so dispatch through generic
     * receivers can route via the bound trait. */
    int saved_bound_count = ctx->type_param_bound_count;
    TSNode tp_list = ts_node_child_by_field_name(func_node, "type_parameters", 15);
    if (!ts_node_is_null(tp_list)) {
        char *tp_text = rust_node_text(ctx, tp_list);
        if (tp_text)
            rust_collect_bounds_from_text(ctx, tp_text);
    }
    TSNode where_clause = ts_node_child_by_field_name(func_node, "where_clause", 12);
    if (!ts_node_is_null(where_clause)) {
        char *wt = rust_node_text(ctx, where_clause);
        if (wt)
            rust_collect_bounds_from_text(ctx, wt);
    }

    /* Bind self for impl/trait methods. */
    TSNode params = ts_node_child_by_field_name(func_node, "parameters", 10);
    if (!ts_node_is_null(params)) {
        uint32_t pc = ts_node_named_child_count(params);
        for (uint32_t i = 0; i < pc; i++) {
            TSNode p = ts_node_named_child(params, i);
            const char *pk = ts_node_type(p);
            if (strcmp(pk, "self_parameter") == 0) {
                if (ctx->self_type_qn) {
                    /* Determine if &self / &mut self / self by reference. */
                    char *text = rust_node_text(ctx, p);
                    const CBMType *self_t = cbm_type_named(ctx->arena, ctx->self_type_qn);
                    if (text && strchr(text, '&')) {
                        self_t = cbm_type_reference(ctx->arena, self_t);
                    }
                    cbm_scope_bind(ctx->current_scope, "self", self_t);
                }
                continue;
            }
            if (strcmp(pk, "parameter") == 0) {
                TSNode pat = ts_node_child_by_field_name(p, "pattern", 7);
                TSNode tn = ts_node_child_by_field_name(p, "type", 4);
                const CBMType *pt =
                    ts_node_is_null(tn) ? cbm_type_unknown() : rust_parse_type_node(ctx, tn);
                if (!ts_node_is_null(pat))
                    rust_bind_pattern(ctx, pat, pt);
            }
        }
    }

    /* Walk function body. */
    TSNode body = ts_node_child_by_field_name(func_node, "body", 4);
    if (!ts_node_is_null(body)) {
        rust_resolve_calls_in_node(ctx, body);
    }

    ctx->current_scope = saved;
    ctx->enclosing_func_qn = NULL;
    /* Restore bound-env count so the caller's bounds are unaffected. */
    ctx->type_param_bound_count = saved_bound_count;
}

/* Walk an `impl_item`, processing each `function_item` inside its body
 * with the appropriate `self_type_qn` (and `self_trait_qn` for trait impls). */
/* Chalk-lite: record a `T: Trait` bound in the per-function bound
 * environment. The arrays grow by 8 to keep allocs cheap. */
static void rust_record_type_param_bound(RustLSPContext *ctx, const char *param_name,
                                         const char *trait_qn) {
    if (!ctx || !param_name || !trait_qn)
        return;
    /* Grow by 8s. */
    if (ctx->type_param_bound_count % 8 == 0) {
        int new_cap = ctx->type_param_bound_count + 8;
        void *narr = cbm_arena_alloc(ctx->arena, new_cap * sizeof(*ctx->type_param_bounds));
        if (!narr)
            return;
        if (ctx->type_param_bounds && ctx->type_param_bound_count > 0) {
            memcpy(narr, ctx->type_param_bounds,
                   ctx->type_param_bound_count * sizeof(*ctx->type_param_bounds));
        }
        ctx->type_param_bounds = narr;
    }
    ctx->type_param_bounds[ctx->type_param_bound_count].param_name =
        cbm_arena_strdup(ctx->arena, param_name);
    ctx->type_param_bounds[ctx->type_param_bound_count].trait_qn =
        cbm_arena_strdup(ctx->arena, trait_qn);
    ctx->type_param_bound_count++;
}

/* Look up the first trait bound for a given type-param name. Returns
 * NULL if `name` has no bound recorded. */
static const char *rust_lookup_type_param_bound(RustLSPContext *ctx, const char *name) {
    if (!ctx || !name)
        return NULL;
    for (int i = 0; i < ctx->type_param_bound_count; i++) {
        if (strcmp(ctx->type_param_bounds[i].param_name, name) == 0) {
            return ctx->type_param_bounds[i].trait_qn;
        }
    }
    return NULL;
}

/* Parse the impl/function's <T: Bound + Bound, U: Bound> + where clause
 * text into the per-context bound environment. We don't reason about
 * lifetimes; we record only trait bounds and associated-type bindings.
 *
 * Format we accept (simplified TOML-like grammar):
 *   `<T: Clone + Debug, U: Iterator<Item = V>>`
 *   `where T: Clone, U: Iterator<Item = V>`
 *
 * Multiple bounds are split on `+` (top-level), entries on `,`. */
static void rust_collect_bounds_from_text(RustLSPContext *ctx, const char *text) {
    if (!ctx || !text)
        return;
    /* Walk text, find segments separated by `,` at top depth. For each
     * segment, split on `:` to get (param, bound-list); split bounds on `+`
     * at top depth. Resolve each bound through the path resolver to its QN. */
    int len = (int)strlen(text);
    int from = 0;
    while (from < len) {
        /* Skip whitespace + leading 'where'/punct. */
        while (from < len && (text[from] == ' ' || text[from] == '\n' || text[from] == '<' ||
                              text[from] == ',' || text[from] == '>' || text[from] == 'w')) {
            if (text[from] == 'w' && from + 5 < len && strncmp(text + from, "where", 5) == 0) {
                from += 5;
            } else {
                from++;
            }
        }
        if (from >= len)
            break;

        /* Param name. */
        int name_start = from;
        if (text[from] == '\'') {
            /* Lifetime — skip. */
            from++;
            while (from < len && (isalnum((unsigned char)text[from]) || text[from] == '_'))
                from++;
            continue;
        }
        while (from < len && (isalnum((unsigned char)text[from]) || text[from] == '_'))
            from++;
        int name_end = from;
        if (name_end == name_start) {
            from++;
            continue;
        }
        char *pname =
            cbm_arena_strndup(ctx->arena, text + name_start, (size_t)(name_end - name_start));
        /* Look for `:`. */
        while (from < len && (text[from] == ' ' || text[from] == '\t'))
            from++;
        if (from >= len || text[from] != ':') {
            /* No bound; skip to next entry. */
            while (from < len && text[from] != ',' && text[from] != '>' && text[from] != '\n')
                from++;
            continue;
        }
        from++; /* consume `:` */

        /* Bound list: split on `+` at depth 0, terminated by `,` / `>` /
         * end of where clause. */
        int depth = 0;
        int bound_start = from;
        while (from < len) {
            char c = text[from];
            if (c == '<' || c == '(' || c == '[')
                depth++;
            else if (c == '>' || c == ')' || c == ']') {
                if (depth == 0)
                    break;
                depth--;
            } else if (depth == 0 && (c == '+' || c == ',' || c == '\n')) {
                /* End of one bound. */
                int b_end = from;
                /* Trim trailing whitespace. */
                while (b_end > bound_start && (text[b_end - 1] == ' ' || text[b_end - 1] == '\t')) {
                    b_end--;
                }
                /* Trim leading whitespace. */
                int b_start = bound_start;
                while (b_start < b_end && (text[b_start] == ' ' || text[b_start] == '\t')) {
                    b_start++;
                }
                if (b_end > b_start) {
                    char *btext =
                        cbm_arena_strndup(ctx->arena, text + b_start, (size_t)(b_end - b_start));
                    /* Strip any `<…>` associated-type suffix for the
                     * trait QN lookup — we keep the trait name only. */
                    char *langle = strchr(btext, '<');
                    if (langle)
                        *langle = '\0';
                    const char *qn = rust_resolve_path_expr(ctx, btext);
                    if (qn) {
                        rust_record_type_param_bound(ctx, pname, qn);
                    }
                }
                if (c == '+') {
                    from++;
                    bound_start = from;
                    continue;
                }
                /* End of entry. */
                break;
            }
            from++;
        }
        /* Advance past entry terminator. */
        if (from < len && (text[from] == ',' || text[from] == '\n'))
            from++;
    }
}

/* Helper: does `name` appear in the impl's `<T, U, ...>` type
 * parameter list? Used to detect blanket impls (impl<T: Trait> X for T). */
static bool rust_impl_has_type_param(RustLSPContext *ctx, TSNode impl_node, const char *name) {
    if (!name)
        return false;
    TSNode tp = ts_node_child_by_field_name(impl_node, "type_parameters", 15);
    if (ts_node_is_null(tp))
        return false;
    uint32_t nc = ts_node_named_child_count(tp);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(tp, i);
        const char *ck = ts_node_type(c);
        if (strcmp(ck, "type_identifier") == 0 || strcmp(ck, "constrained_type_parameter") == 0) {
            char *nm = rust_node_text(ctx, c);
            if (!nm)
                continue;
            /* For constrained_type_parameter, the name is the
             * first identifier-y child. */
            if (strcmp(ck, "constrained_type_parameter") == 0) {
                TSNode lhs = ts_node_child_by_field_name(c, "left", 4);
                if (!ts_node_is_null(lhs))
                    nm = rust_node_text(ctx, lhs);
            }
            if (nm && strcmp(nm, name) == 0)
                return true;
        }
    }
    return false;
}

static void rust_process_impl(RustLSPContext *ctx, TSNode impl_node) {
    TSNode type_node = ts_node_child_by_field_name(impl_node, "type", 4);
    if (ts_node_is_null(type_node))
        return;
    char *type_text = rust_node_text(ctx, type_node);
    if (!type_text)
        return;

    /* Detect blanket impl: `impl<T: Trait> ForeignTrait for T { ... }`
     * where type_text is a name that appears in the impl's type
     * parameters. In that case the receiver isn't a concrete type — it's
     * any T satisfying the bound. We register the methods on the trait
     * QN itself so dispatch through T: Trait finds them. */
    TSNode trait_node = ts_node_child_by_field_name(impl_node, "trait", 5);
    bool is_blanket =
        !ts_node_is_null(trait_node) && rust_impl_has_type_param(ctx, impl_node, type_text);
    const char *effective_recv = NULL;

    if (is_blanket) {
        char *tt = rust_node_text(ctx, trait_node);
        if (tt)
            effective_recv = rust_resolve_path_expr(ctx, tt);
    } else {
        effective_recv = rust_resolve_path_expr(ctx, type_text);
    }
    if (!effective_recv)
        return;

    const char *saved_self = ctx->self_type_qn;
    const char *saved_trait = ctx->self_trait_qn;
    ctx->self_type_qn = effective_recv;
    ctx->self_trait_qn = NULL;

    if (!ts_node_is_null(trait_node) && !is_blanket) {
        char *tt = rust_node_text(ctx, trait_node);
        if (tt)
            ctx->self_trait_qn = rust_resolve_path_expr(ctx, tt);
    }

    TSNode body = ts_node_child_by_field_name(impl_node, "body", 4);
    if (!ts_node_is_null(body)) {
        uint32_t nc = ts_node_child_count(body);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(body, i);
            if (ts_node_is_null(c) || !ts_node_is_named(c))
                continue;
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "function_item") == 0) {
                rust_process_function(ctx, c, effective_recv);
            }
        }
    }

    ctx->self_type_qn = saved_self;
    ctx->self_trait_qn = saved_trait;
}

void rust_lsp_process_file(RustLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;

    /* Pass 0: collect macro_rules! definitions before walking bodies so
     * macro_invocation handlers can expand user-defined macros. */
    rust_collect_macro_rules(ctx, root);

    /* Record bare `mod foo;` declarations (file links). The pipeline
     * uses these to know which sibling files to include in cross-file
     * resolution. We just store them as Imports with a `mod:` prefix
     * so the pipeline can distinguish them from `use` imports.
     *
     * `mod foo { ... }` (inline module) is NOT recorded — the body is
     * already in this file. Only bare `mod foo;` declarations are. */
    {
        uint32_t rnc = ts_node_child_count(root);
        for (uint32_t i = 0; i < rnc; i++) {
            TSNode c = ts_node_child(root, i);
            if (ts_node_is_null(c))
                continue;
            if (strcmp(ts_node_type(c), "mod_item") != 0)
                continue;
            /* Inline mod has a `body` field; bare decl does not. */
            TSNode body = ts_node_child_by_field_name(c, "body", 4);
            if (!ts_node_is_null(body))
                continue; /* inline */
            TSNode mname = ts_node_child_by_field_name(c, "name", 4);
            if (ts_node_is_null(mname))
                continue;
            char *name = rust_node_text(ctx, mname);
            if (!name)
                continue;
            /* Surface as a synthetic CALLS edge from "<module>" to
             * the sibling module so the cross-file pass picks it up
             * via short-name fallback. We attribute it to the file's
             * synthetic module-scope caller. */
            const char *save_caller = ctx->enclosing_func_qn;
            ctx->enclosing_func_qn = ctx->module_qn;
            rust_emit_resolved_call(ctx,
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name),
                                    "lsp_mod_decl", 0.70f);
            ctx->enclosing_func_qn = save_caller;
        }
    }

    /* Pass 1: bind module-level const/static so functions can see them. */
    uint32_t nc = ts_node_child_count(root);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(root, i);
        if (ts_node_is_null(c))
            continue;
        const char *ck = ts_node_type(c);
        if (strcmp(ck, "const_item") == 0 || strcmp(ck, "static_item") == 0) {
            rust_process_statement(ctx, c);
        }
    }

    /* Pass 2: walk every top-level item. */
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(root, i);
        if (ts_node_is_null(c))
            continue;
        const char *ck = ts_node_type(c);
        if (strcmp(ck, "function_item") == 0) {
            rust_process_function(ctx, c, NULL);
        } else if (strcmp(ck, "impl_item") == 0) {
            rust_process_impl(ctx, c);
        } else if (strcmp(ck, "mod_item") == 0) {
            /* Inline module — recurse into its declaration_list. */
            TSNode body = ts_node_child_by_field_name(c, "body", 4);
            if (!ts_node_is_null(body)) {
                uint32_t mnc = ts_node_child_count(body);
                for (uint32_t j = 0; j < mnc; j++) {
                    TSNode mc = ts_node_child(body, j);
                    if (ts_node_is_null(mc))
                        continue;
                    const char *mck = ts_node_type(mc);
                    if (strcmp(mck, "function_item") == 0) {
                        rust_process_function(ctx, mc, NULL);
                    } else if (strcmp(mck, "impl_item") == 0) {
                        rust_process_impl(ctx, mc);
                    }
                }
            }
        }
    }
}

/* ════════════════════════════════════════════════════════════════════
 * 11. Per-file entry: build registry + run
 * ════════════════════════════════════════════════════════════════════ */

/* Collect `use_declaration`s in the file and materialise our use map.
 * Tree-sitter-rust models the pattern as:
 *
 *   use_declaration → identifier | scoped_identifier | scoped_use_list |
 *                     use_list | use_as_clause | use_wildcard.
 *
 * We expand each of these into one or more (alias, full-path) entries. */
static void rust_collect_uses(RustLSPContext *ctx, TSNode root) {
    /* Recursive walker. */
    typedef struct stack_t {
        TSNode node;
        struct stack_t *prev;
    } stack_t;
    stack_t *top = (stack_t *)cbm_arena_alloc(ctx->arena, sizeof(stack_t));
    top->node = root;
    top->prev = NULL;
    while (top) {
        TSNode n = top->node;
        top = top->prev;
        if (ts_node_is_null(n))
            continue;
        const char *k = ts_node_type(n);
        if (strcmp(k, "use_declaration") == 0) {
            char *full = rust_node_text(ctx, n);
            if (full) {
                if (strncmp(full, "use ", 4) == 0)
                    full += 4;
                size_t len = strlen(full);
                if (len > 0 && full[len - 1] == ';')
                    full[len - 1] = '\0';
                /* Trim leading whitespace. */
                while (*full == ' ')
                    full++;
                /* Detect glob. */
                size_t flen = strlen(full);
                if (flen >= 3 && strcmp(full + flen - 3, "::*") == 0) {
                    char *mod = cbm_arena_strndup(ctx->arena, full, flen - 3);
                    rust_lsp_add_glob(ctx, convert_path_to_qn(ctx->arena, mod));
                } else if (flen >= 1 && full[flen - 1] == '}') {
                    /* Brace list: prefix::{a, b as c, d}. */
                    char *lbr = strchr(full, '{');
                    if (lbr) {
                        size_t prefix_len = (size_t)(lbr - full);
                        /* Strip trailing "::" from prefix. */
                        while (prefix_len >= 2 && full[prefix_len - 1] == ':' &&
                               full[prefix_len - 2] == ':') {
                            prefix_len -= 2;
                        }
                        char *prefix = cbm_arena_strndup(ctx->arena, full, prefix_len);
                        char *body = cbm_arena_strdup(ctx->arena, lbr + 1);
                        size_t blen = strlen(body);
                        if (blen > 0 && body[blen - 1] == '}')
                            body[blen - 1] = '\0';
                        char *save = NULL;
                        char *tok = strtok_r(body, ",", &save);
                        while (tok) {
                            while (*tok == ' ')
                                tok++;
                            char *eb = tok + strlen(tok) - 1;
                            while (eb > tok && *eb == ' ')
                                *eb-- = '\0';
                            if (*tok == '\0') {
                                tok = strtok_r(NULL, ",", &save);
                                continue;
                            }
                            /* `Read` or `Read as R`. */
                            char *asp = strstr(tok, " as ");
                            char *alias = NULL;
                            char *path_part = tok;
                            if (asp) {
                                *asp = '\0';
                                alias = asp + 4;
                                while (*alias == ' ')
                                    alias++;
                            } else {
                                alias = (char *)path_last_segment(tok);
                            }
                            char *full_path =
                                (strcmp(tok, "self") == 0)
                                    ? cbm_arena_strdup(ctx->arena, prefix)
                                    : cbm_arena_sprintf(ctx->arena, "%s::%s", prefix, path_part);
                            rust_lsp_add_use(ctx, alias, full_path);
                            tok = strtok_r(NULL, ",", &save);
                        }
                    }
                } else {
                    /* Single path; possibly followed by ` as X`. */
                    char *asp = strstr(full, " as ");
                    char *alias = NULL;
                    char *path_part = full;
                    if (asp) {
                        *asp = '\0';
                        alias = asp + 4;
                        while (*alias == ' ')
                            alias++;
                    } else {
                        alias = (char *)path_last_segment(full);
                    }
                    rust_lsp_add_use(ctx, alias, path_part);
                }
            }
        }
        /* Recurse into mod_item bodies so nested uses are captured too. */
        if (strcmp(k, "mod_item") == 0 || strcmp(k, "source_file") == 0 ||
            strcmp(k, "declaration_list") == 0) {
            uint32_t nc = ts_node_child_count(n);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_child(n, i);
                if (ts_node_is_null(c))
                    continue;
                stack_t *nx = (stack_t *)cbm_arena_alloc(ctx->arena, sizeof(stack_t));
                nx->node = c;
                nx->prev = top;
                top = nx;
            }
        }
    }
}

/* Build the registry from the per-file `result->defs`, `result->impl_traits`,
 * and a Rust prelude seed. */
static void rust_build_registry_from_defs(CBMArena *arena, CBMTypeRegistry *reg,
                                          CBMFileResult *result, const char *module_qn, TSNode root,
                                          const char *source) {

    cbm_registry_init(reg, arena);
    cbm_rust_stdlib_register(reg, arena);

    /* Phase A: register every Class/Type/Trait/Function/Method definition. */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name)
            continue;

        // Every type-like container (Class/Struct/Type/Interface/Trait/Enum).
        // Struct included so a Rust `struct Foo` (now labelled "Struct") registers
        // as a type and its `impl Foo` methods/fields resolve.
        if (cbm_label_is_type_like(d->label)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->name;
            rt.is_interface =
                (strcmp(d->label, "Interface") == 0 || strcmp(d->label, "Trait") == 0);
            cbm_registry_add_type(reg, rt);
        }

        if (d->label && (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0)) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;
            rf.min_params = -1;

            /* Build FUNC sig from return_types / param_types. */
            const CBMType **ret_types = NULL;
            if (d->return_types) {
                int count = 0;
                while (d->return_types[count])
                    count++;
                if (count > 0) {
                    ret_types = (const CBMType **)cbm_arena_alloc(
                        arena, (count + 1) * sizeof(const CBMType *));
                    for (int j = 0; j < count; j++) {
                        ret_types[j] =
                            rust_parse_return_type_text(arena, d->return_types[j], module_qn);
                    }
                    ret_types[count] = NULL;
                }
            } else if (d->return_type && d->return_type[0]) {
                ret_types = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
                ret_types[0] = rust_parse_return_type_text(arena, d->return_type, module_qn);
                ret_types[1] = NULL;
            }
            const CBMType **param_types = NULL;
            if (d->param_types) {
                int count = 0;
                while (d->param_types[count])
                    count++;
                if (count > 0) {
                    param_types = (const CBMType **)cbm_arena_alloc(
                        arena, (count + 1) * sizeof(const CBMType *));
                    for (int j = 0; j < count; j++) {
                        param_types[j] =
                            rust_parse_return_type_text(arena, d->param_types[j], module_qn);
                    }
                    param_types[count] = NULL;
                }
            }
            rf.signature = cbm_type_func(arena, d->param_names, param_types, ret_types);

            if (strcmp(d->label, "Method") == 0 && d->parent_class) {
                rf.receiver_type = d->parent_class;
                if (!cbm_registry_lookup_type(reg, rf.receiver_type)) {
                    CBMRegisteredType auto_t;
                    memset(&auto_t, 0, sizeof(auto_t));
                    auto_t.qualified_name = rf.receiver_type;
                    const char *dot = strrchr(rf.receiver_type, '.');
                    auto_t.short_name = dot ? cbm_arena_strdup(arena, dot + 1) : rf.receiver_type;
                    cbm_registry_add_type(reg, auto_t);
                }
            }

            cbm_registry_add_func(reg, rf);
        }
    }

    /* Phase B: walk the AST to extract struct fields + record `impl Trait
     * for Type` linkage as embedded types. */
    if (!ts_node_is_null(root)) {
        uint32_t nc = ts_node_child_count(root);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode top = ts_node_child(root, i);
            if (ts_node_is_null(top))
                continue;
            const char *tk = ts_node_type(top);

            if (strcmp(tk, "struct_item") == 0) {
                TSNode name_node = ts_node_child_by_field_name(top, "name", 4);
                TSNode body = ts_node_child_by_field_name(top, "body", 4);
                if (ts_node_is_null(name_node) || ts_node_is_null(body))
                    continue;
                char *tn = cbm_node_text(arena, name_node, source);
                if (!tn || !tn[0])
                    continue;
                const char *type_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, tn);

                /* Iterate field_declaration_list / ordered_field_declaration_list. */
                if (strcmp(ts_node_type(body), "field_declaration_list") == 0) {
                    uint32_t fc = ts_node_named_child_count(body);
                    const char *fld_names[64];
                    const CBMType *fld_types[64];
                    int fld_count = 0;
                    for (uint32_t j = 0; j < fc && fld_count < 63; j++) {
                        TSNode fd = ts_node_named_child(body, j);
                        if (strcmp(ts_node_type(fd), "field_declaration") != 0)
                            continue;
                        TSNode fn = ts_node_child_by_field_name(fd, "name", 4);
                        TSNode ft = ts_node_child_by_field_name(fd, "type", 4);
                        char *fname = cbm_node_text(arena, fn, source);
                        if (!fname)
                            continue;
                        /* Build a temporary context for parsing types. */
                        RustLSPContext tmp;
                        memset(&tmp, 0, sizeof(tmp));
                        tmp.arena = arena;
                        tmp.source = source;
                        tmp.source_len = (int)strlen(source);
                        tmp.registry = reg;
                        tmp.module_qn = module_qn;
                        const CBMType *ft_t = rust_parse_type_node(&tmp, ft);
                        fld_names[fld_count] = fname;
                        fld_types[fld_count] = ft_t;
                        fld_count++;
                    }
                    if (fld_count > 0) {
                        for (int ti = 0; ti < reg->type_count; ti++) {
                            if (reg->types[ti].qualified_name &&
                                strcmp(reg->types[ti].qualified_name, type_qn) == 0) {
                                const char **names = (const char **)cbm_arena_alloc(
                                    arena, (fld_count + 1) * sizeof(const char *));
                                const CBMType **types = (const CBMType **)cbm_arena_alloc(
                                    arena, (fld_count + 1) * sizeof(const CBMType *));
                                for (int fi = 0; fi < fld_count; fi++) {
                                    names[fi] = fld_names[fi];
                                    types[fi] = fld_types[fi];
                                }
                                names[fld_count] = NULL;
                                types[fld_count] = NULL;
                                reg->types[ti].field_names = names;
                                reg->types[ti].field_types = types;
                                break;
                            }
                        }
                    }
                }
            }

            if (strcmp(tk, "trait_item") == 0) {
                TSNode name_node = ts_node_child_by_field_name(top, "name", 4);
                TSNode body = ts_node_child_by_field_name(top, "body", 4);
                if (ts_node_is_null(name_node))
                    continue;
                char *tn = cbm_node_text(arena, name_node, source);
                if (!tn || !tn[0])
                    continue;
                const char *trait_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, tn);

                /* Mark as interface and collect method names. */
                for (int ti = 0; ti < reg->type_count; ti++) {
                    if (!reg->types[ti].qualified_name)
                        continue;
                    if (strcmp(reg->types[ti].qualified_name, trait_qn) == 0) {
                        reg->types[ti].is_interface = true;
                        if (!ts_node_is_null(body)) {
                            const char *methods[64];
                            int mc = 0;
                            uint32_t bc = ts_node_named_child_count(body);
                            for (uint32_t j = 0; j < bc && mc < 63; j++) {
                                TSNode item = ts_node_named_child(body, j);
                                const char *ik = ts_node_type(item);
                                if (strcmp(ik, "function_item") != 0 &&
                                    strcmp(ik, "function_signature_item") != 0)
                                    continue;
                                TSNode mn = ts_node_child_by_field_name(item, "name", 4);
                                if (ts_node_is_null(mn))
                                    continue;
                                char *mname = cbm_node_text(arena, mn, source);
                                if (mname)
                                    methods[mc++] = mname;
                            }
                            if (mc > 0) {
                                const char **arr = (const char **)cbm_arena_alloc(
                                    arena, (mc + 1) * sizeof(const char *));
                                for (int mi = 0; mi < mc; mi++)
                                    arr[mi] = methods[mi];
                                arr[mc] = NULL;
                                reg->types[ti].method_names = arr;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    /* Phase B1: walk top-level free `function_item`s to harvest their
     * return types into the registry — `extract_defs` does not fill
     * `return_type` for Rust free functions either, so a let-binding
     * like `let v = pair();` would otherwise know nothing about pair's
     * return tuple. */
    if (!ts_node_is_null(root)) {
        RustLSPContext tmp;
        memset(&tmp, 0, sizeof(tmp));
        tmp.arena = arena;
        tmp.source = source;
        tmp.source_len = (int)strlen(source);
        tmp.registry = reg;
        tmp.module_qn = module_qn;

        uint32_t rnc = ts_node_child_count(root);
        for (uint32_t i = 0; i < rnc; i++) {
            TSNode top = ts_node_child(root, i);
            if (ts_node_is_null(top) || strcmp(ts_node_type(top), "function_item") != 0)
                continue;
            TSNode mn = ts_node_child_by_field_name(top, "name", 4);
            TSNode rtn = ts_node_child_by_field_name(top, "return_type", 11);
            if (ts_node_is_null(mn) || ts_node_is_null(rtn))
                continue;
            char *fname = cbm_node_text(arena, mn, source);
            if (!fname)
                continue;
            const CBMType *ret = rust_parse_type_node(&tmp, rtn);
            const char *fn_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, fname);
            for (int k = 0; k < reg->func_count; k++) {
                CBMRegisteredFunc *rf = &reg->funcs[k];
                if (!rf->qualified_name)
                    continue;
                if (rf->receiver_type)
                    continue; /* free fns only */
                if (strcmp(rf->qualified_name, fn_qn) != 0)
                    continue;
                const CBMType **ret_arr =
                    (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
                ret_arr[0] = ret;
                ret_arr[1] = NULL;
                rf->signature = cbm_type_func(arena, NULL, NULL, ret_arr);
                break;
            }
        }
    }

    /* Phase A2: derive-macro synthesis.
     *
     * Real Rust code is saturated with `#[derive(Clone, Debug, …)]`.
     * Without expanding proc-macros we can still synthesize the trait
     * impl footprint that each well-known derive generates, so calls
     * like `x.clone()` / `format!("{:?}", x)` / `MyT::default()` on the
     * derived type actually resolve.
     *
     * We only synthesize the curated, high-frequency derives — anything
     * unknown is left alone (per the FOLLOWUP doc's "no false edge"
     * policy). Each synthesized impl:
     *   - registers a method (or static fn for `default`/`parse`) on
     *     the receiver type with the right short name and return type;
     *   - appends the trait's QN to the receiver's `embedded_types` so
     *     trait dispatch via `resolve_trait_method` walks it.
     */
    {
        /* Curated derive → (trait QN, [methods with sig sketch]) table. */
        struct DeriveMethod {
            const char *short_name;
            const char *return_type; /* QN or NULL for unknown */
            bool is_static;          /* no `self` (e.g. `default`, `parse`) */
        };
        struct DeriveImpl {
            const char *derive_name;
            const char *trait_qn;
            struct DeriveMethod methods[4]; /* NULL-terminated by empty short_name */
        };
        static const struct DeriveImpl derives[] = {
            {"Clone", "core.clone.Clone", {{"clone", NULL, false}, {NULL, NULL, false}}},
            {"Copy", "core.marker.Copy", {{NULL, NULL, false}}}, /* marker — no methods */
            {"Debug", "core.fmt.Debug", {{"fmt", NULL, false}, {NULL, NULL, false}}},
            {"Display", "core.fmt.Display", {{"fmt", NULL, false}, {NULL, NULL, false}}},
            {"Default", "core.default.Default", {{"default", NULL, true}, {NULL, NULL, false}}},
            {"PartialEq",
             "core.cmp.PartialEq",
             {{"eq", "bool", false}, {"ne", "bool", false}, {NULL, NULL, false}}},
            {"Eq", "core.cmp.Eq", {{NULL, NULL, false}}}, /* marker only */
            {"PartialOrd",
             "core.cmp.PartialOrd",
             {{"partial_cmp", NULL, false},
              {"lt", "bool", false},
              {"le", "bool", false},
              {NULL, NULL, false}}},
            {"Ord", "core.cmp.Ord", {{"cmp", NULL, false}, {NULL, NULL, false}}},
            {"Hash", "core.hash.Hash", {{"hash", "()", false}, {NULL, NULL, false}}},
            {"Send", "core.marker.Send", {{NULL, NULL, false}}},
            {"Sync", "core.marker.Sync", {{NULL, NULL, false}}},
            /* serde — extremely common. */
            {"Serialize", "serde.Serialize", {{"serialize", NULL, false}, {NULL, NULL, false}}},
            {"Deserialize",
             "serde.Deserialize",
             {{"deserialize", NULL, true}, {NULL, NULL, false}}},
            /* clap derive — synthesizes the Parser interface. */
            {"Parser",
             "clap.Parser",
             {{"parse", NULL, true},
              {"try_parse", NULL, true},
              {"parse_from", NULL, true},
              {"try_parse_from", NULL, true}}},
            {"Args", "clap.Args", {{NULL, NULL, false}}},
            {"Subcommand", "clap.Subcommand", {{NULL, NULL, false}}},
            {"ValueEnum", "clap.ValueEnum", {{NULL, NULL, false}}},
            /* thiserror — adds the Error impl. */
            {"Error", "core.error.Error", {{NULL, NULL, false}}},
        };
        const int derive_count = (int)(sizeof(derives) / sizeof(derives[0]));

        for (int i = 0; i < result->defs.count; i++) {
            CBMDefinition *d = &result->defs.items[i];
            if (!d->qualified_name || !d->name)
                continue;
            /* `#[derive(...)]` rides on type-like defs — most often a struct or
             * enum (now labelled "Struct"/"Enum"), also type aliases. Accept the
             * whole type-like set so a derive on a struct is not dropped. */
            if (!cbm_label_is_type_like(d->label))
                continue;
            if (!d->decorators)
                continue;

            /* Scan decorator strings for `#[derive(...)]`. */
            for (int di = 0; d->decorators[di]; di++) {
                const char *dec = d->decorators[di];
                const char *p = strstr(dec, "derive");
                if (!p)
                    continue;
                const char *lparen = strchr(p, '(');
                if (!lparen)
                    continue;
                const char *rparen = strchr(lparen, ')');
                if (!rparen)
                    continue;
                /* Now walk between the parens, splitting on comma. */
                const char *q = lparen + 1;
                while (q < rparen) {
                    while (q < rparen && (*q == ' ' || *q == ','))
                        q++;
                    /* Find the end of the identifier (may be qualified
                     * like `serde::Serialize`). We grab the trailing
                     * segment as the derive name. */
                    const char *tok_start = q;
                    while (q < rparen && *q != ',' && *q != ' ')
                        q++;
                    if (q == tok_start)
                        break;
                    /* Trailing-segment after the last `::`. */
                    const char *short_start = tok_start;
                    for (const char *r = tok_start; r < q - 1; r++) {
                        if (r[0] == ':' && r[1] == ':')
                            short_start = r + 2;
                    }
                    size_t name_len = (size_t)(q - short_start);
                    if (name_len == 0 || name_len > 64)
                        continue;
                    /* Look up in curated table. */
                    for (int di2 = 0; di2 < derive_count; di2++) {
                        const struct DeriveImpl *di_entry = &derives[di2];
                        size_t entry_len = strlen(di_entry->derive_name);
                        if (entry_len != name_len)
                            continue;
                        if (strncmp(di_entry->derive_name, short_start, name_len) != 0)
                            continue;

                        /* Found a matching curated derive. Register the
                         * trait QN as an embedded_type on the receiver
                         * AND synthesize the method entries. */
                        CBMRegisteredType *rt = NULL;
                        for (int ti = 0; ti < reg->type_count; ti++) {
                            if (reg->types[ti].qualified_name &&
                                strcmp(reg->types[ti].qualified_name, d->qualified_name) == 0) {
                                rt = &reg->types[ti];
                                break;
                            }
                        }
                        if (!rt)
                            break;

                        /* Append trait QN to embedded_types. */
                        int existing = 0;
                        if (rt->embedded_types) {
                            while (rt->embedded_types[existing])
                                existing++;
                        }
                        const char **new_arr = (const char **)cbm_arena_alloc(
                            arena, (existing + 2) * sizeof(const char *));
                        for (int k = 0; k < existing; k++) {
                            new_arr[k] = rt->embedded_types[k];
                        }
                        new_arr[existing] = di_entry->trait_qn;
                        new_arr[existing + 1] = NULL;
                        rt->embedded_types = new_arr;

                        /* Synthesize methods. Bound `mi < 4` BEFORE dereferencing
                         * methods[mi] so we never read methods[4] (OOB). */
                        for (int mi = 0; mi < 4 && di_entry->methods[mi].short_name; mi++) {
                            const struct DeriveMethod *dm = &di_entry->methods[mi];
                            CBMRegisteredFunc rf;
                            memset(&rf, 0, sizeof(rf));
                            rf.short_name = dm->short_name;
                            rf.qualified_name = cbm_arena_sprintf(arena, "%s.%s", d->qualified_name,
                                                                  dm->short_name);
                            /* Static methods (default/parse) have no
                             * receiver; method calls treat them as
                             * static path lookups via UFCS. */
                            rf.receiver_type = d->qualified_name;
                            rf.min_params = -1;
                            const CBMType *ret_t = cbm_type_unknown();
                            if (dm->return_type) {
                                if (strcmp(dm->return_type, "bool") == 0) {
                                    ret_t = cbm_type_builtin(arena, "bool");
                                } else if (strcmp(dm->return_type, "()") == 0) {
                                    ret_t = cbm_type_builtin(arena, "()");
                                }
                            } else if (dm->is_static) {
                                /* `default()`, `parse()` return Self. */
                                ret_t = cbm_type_named(arena, d->qualified_name);
                            }
                            const CBMType **ra = (const CBMType **)cbm_arena_alloc(
                                arena, 2 * sizeof(const CBMType *));
                            ra[0] = ret_t;
                            ra[1] = NULL;
                            rf.signature = cbm_type_func(arena, NULL, NULL, ra);
                            cbm_registry_add_func(reg, rf);
                        }
                        break;
                    }
                }
            }
        }
    }

    /* Phase B2: walk impl bodies to harvest each method's return type
     * from the AST. The unified `extract_defs` extractor does not fill
     * `return_type` for Rust impl methods, so without this pass our
     * registered functions have no return-type signature and chained
     * method calls (`File::open().read()`) break. */
    if (!ts_node_is_null(root)) {
        uint32_t rnc = ts_node_child_count(root);
        for (uint32_t i = 0; i < rnc; i++) {
            TSNode top = ts_node_child(root, i);
            if (ts_node_is_null(top) || strcmp(ts_node_type(top), "impl_item") != 0)
                continue;
            TSNode type_node = ts_node_child_by_field_name(top, "type", 4);
            TSNode body = ts_node_child_by_field_name(top, "body", 4);
            if (ts_node_is_null(type_node) || ts_node_is_null(body))
                continue;
            char *type_name = cbm_node_text(arena, type_node, source);
            if (!type_name || !type_name[0])
                continue;
            const char *type_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, type_name);

            RustLSPContext tmp;
            memset(&tmp, 0, sizeof(tmp));
            tmp.arena = arena;
            tmp.source = source;
            tmp.source_len = (int)strlen(source);
            tmp.registry = reg;
            tmp.module_qn = module_qn;
            tmp.self_type_qn = type_qn;

            uint32_t bnc = ts_node_child_count(body);
            for (uint32_t j = 0; j < bnc; j++) {
                TSNode item = ts_node_child(body, j);
                if (ts_node_is_null(item) || !ts_node_is_named(item))
                    continue;
                if (strcmp(ts_node_type(item), "function_item") != 0)
                    continue;
                TSNode mn = ts_node_child_by_field_name(item, "name", 4);
                TSNode rtn = ts_node_child_by_field_name(item, "return_type", 11);
                if (ts_node_is_null(mn) || ts_node_is_null(rtn))
                    continue;
                char *mname = cbm_node_text(arena, mn, source);
                if (!mname)
                    continue;
                const CBMType *ret = rust_parse_type_node(&tmp, rtn);
                /* Substitute Self -> receiver type so chains work. */
                if (ret && ret->kind == CBM_TYPE_NAMED &&
                    strcmp(ret->data.named.qualified_name, "Self") == 0) {
                    ret = cbm_type_named(arena, type_qn);
                }
                /* Patch the registered function's signature. */
                for (int k = 0; k < reg->func_count; k++) {
                    CBMRegisteredFunc *rf = &reg->funcs[k];
                    if (!rf->receiver_type || !rf->short_name)
                        continue;
                    if (strcmp(rf->receiver_type, type_qn) != 0)
                        continue;
                    if (strcmp(rf->short_name, mname) != 0)
                        continue;
                    const CBMType **ret_arr =
                        (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
                    ret_arr[0] = ret;
                    ret_arr[1] = NULL;
                    rf->signature = cbm_type_func(arena, NULL, NULL, ret_arr);
                    break;
                }
            }
        }
    }

    /* Phase C: encode `impl Trait for Type` as `embedded_types` on the
     * receiver type so trait dispatch can find them. */
    for (int i = 0; i < result->impl_traits.count; i++) {
        CBMImplTrait *it = &result->impl_traits.items[i];
        if (!it->struct_name || !it->trait_name)
            continue;
        const char *recv_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, it->struct_name);
        const char *trait_qn = strstr(it->trait_name, "::")
                                   ? convert_path_to_qn(arena, it->trait_name)
                                   : cbm_arena_sprintf(arena, "%s.%s", module_qn, it->trait_name);

        CBMRegisteredType *rt = NULL;
        for (int ti = 0; ti < reg->type_count; ti++) {
            if (reg->types[ti].qualified_name &&
                strcmp(reg->types[ti].qualified_name, recv_qn) == 0) {
                rt = &reg->types[ti];
                break;
            }
        }
        if (!rt) {
            CBMRegisteredType auto_t;
            memset(&auto_t, 0, sizeof(auto_t));
            auto_t.qualified_name = recv_qn;
            const char *dot = strrchr(recv_qn, '.');
            auto_t.short_name = dot ? dot + 1 : recv_qn;
            cbm_registry_add_type(reg, auto_t);
            rt = &reg->types[reg->type_count - 1];
        }
        /* Append trait_qn to embedded_types. */
        int existing = 0;
        if (rt->embedded_types) {
            while (rt->embedded_types[existing])
                existing++;
        }
        const char **new_arr =
            (const char **)cbm_arena_alloc(arena, (existing + 2) * sizeof(const char *));
        for (int j = 0; j < existing; j++)
            new_arr[j] = rt->embedded_types[j];
        new_arr[existing] = trait_qn;
        new_arr[existing + 1] = NULL;
        rt->embedded_types = new_arr;
    }
}

void cbm_run_rust_lsp_with_manifest(CBMArena *arena, CBMFileResult *result, const char *source,
                                    int source_len, TSNode root,
                                    const struct CBMCargoManifest *manifest) {
    if (!arena || !result || !source)
        return;
    const char *module_qn = result->module_qn ? result->module_qn : "rust";

    CBMTypeRegistry reg;
    rust_build_registry_from_defs(arena, &reg, result, module_qn, root, source);
    /* Finalize after all per-file adds so lookups during the walk
     * use the hash buckets. */
    cbm_registry_finalize(&reg);

    RustLSPContext ctx;
    rust_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, &result->resolved_calls);
    ctx.cargo_manifest = manifest;
    /* Let the resolver inject synthetic syntactic calls for operator/macro
     * desugaring so those recovered calls reach the CALLS-edge pipeline. */
    ctx.syn_calls = &result->calls;

    rust_collect_uses(&ctx, root);
    /* Bridge any extracted CBMImports the unified extractor saw. */
    for (int i = 0; i < result->imports.count; i++) {
        CBMImport *imp = &result->imports.items[i];
        if (imp->local_name && imp->module_path) {
            rust_lsp_add_use(&ctx, imp->local_name, imp->module_path);
        }
    }

    rust_lsp_process_file(&ctx, root);

    /* Curated attribute proc-macro synthesis (Option B of the
     * follow-up plan). Runs after the main walk so the synthetic
     * edges are appended without affecting in-walk attribution. */
    cbm_rust_synth_proc_macro_edges(arena, result);
}

void cbm_run_rust_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                      TSNode root) {
    cbm_run_rust_lsp_with_manifest(arena, result, source, source_len, root, NULL);
}

/* ════════════════════════════════════════════════════════════════════
 * 12. Cross-file + batch
 * ════════════════════════════════════════════════════════════════════ */

extern const TSLanguage *tree_sitter_rust(void);

/* Populate + finalize a Rust cross-file type registry from `defs`. Shared by the
 * per-file resolver (cbm_run_rust_lsp_cross_with_manifest) and the build-once shared
 * registry (cbm_rust_build_cross_registry) so both produce a byte-identical registry.
 * `module_qn` is ONLY the fallback used to qualify a def's return type when that def
 * carries no def_module_qn; pass NULL for the shared build (all_defs always carry
 * def_module_qn — verified: 0 NULL across the C + Rust kernel corpora). */
static void rust_populate_cross_registry(CBMTypeRegistry *reg, CBMArena *arena,
                                         CBMRustLSPDef *defs, int def_count,
                                         const char *module_qn) {
    cbm_registry_init(reg, arena);
    cbm_rust_stdlib_register(reg, arena);

    /* qn → (type index + 1), FIRST occurrence wins (mirrors the linear scans
     * this map replaces). Both in-loop registry probes below — the receiver
     * auto-registration check and the trait-linkage lookup — used to scan the
     * UNFINALIZED registry linearly (no buckets exist before finalize): the
     * checklist's lookup-in-registration-loop pattern. Invisible on small
     * per-file builds; on the shared all_defs build (~1.4M entries) those
     * scans were a constant ~63 s of the kernel run — and the sibling
     * null-filter files waited on the build once-guard for exactly that long,
     * which is why no resolution-side fix ever moved their wall time. Index,
     * not pointer, because reg->types reallocs as it grows. */
    CBMIdxMemo type_idx = {0};
    for (int ti = 0; ti < reg->type_count; ti++) {
        const char *qn = reg->types[ti].qualified_name;
        if (qn) {
            cbm_idxmemo_put_if_absent(&type_idx, arena, qn, ti);
        }
    }

    for (int i = 0; i < def_count; i++) {
        CBMRustLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;
        const char *def_mod = d->def_module_qn ? d->def_module_qn : module_qn;

        // Every type-like container (Type/Class/Struct/Interface/Trait/Enum).
        // Struct included so Rust structs (now labelled "Struct") register here.
        if (cbm_label_is_type_like(d->label)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = cbm_arena_strdup(arena, d->qualified_name);
            rt.short_name = cbm_arena_strdup(arena, d->short_name);
            rt.is_interface = d->is_interface || strcmp(d->label, "Trait") == 0 ||
                              strcmp(d->label, "Interface") == 0;
            cbm_registry_add_type(reg, rt);
            cbm_idxmemo_put_if_absent(&type_idx, arena, rt.qualified_name, reg->type_count - 1);
        }

        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = cbm_arena_strdup(arena, d->qualified_name);
            rf.short_name = cbm_arena_strdup(arena, d->short_name);
            rf.min_params = -1;

            /* Build sig from return_types text. */
            const CBMType **ret_types = NULL;
            if (d->return_types && d->return_types[0]) {
                int count = 1;
                for (const char *p = d->return_types; *p; p++) {
                    if (*p == '|')
                        count++;
                }
                ret_types =
                    (const CBMType **)cbm_arena_alloc(arena, (count + 1) * sizeof(const CBMType *));
                int idx = 0;
                char *buf = cbm_arena_strdup(arena, d->return_types);
                char *start = buf;
                for (char *p = buf;; p++) {
                    if (*p == '|' || *p == '\0') {
                        char save = *p;
                        *p = '\0';
                        if (start[0]) {
                            ret_types[idx++] = rust_parse_return_type_text(arena, start, def_mod);
                        }
                        if (save == '\0')
                            break;
                        start = p + 1;
                    }
                }
                ret_types[idx] = NULL;
            }
            rf.signature = cbm_type_func(arena, NULL, NULL, ret_types);

            if (strcmp(d->label, "Method") == 0 && d->receiver_type && d->receiver_type[0]) {
                rf.receiver_type = cbm_arena_strdup(arena, d->receiver_type);
                if (cbm_idxmemo_get(&type_idx, rf.receiver_type) < 0) {
                    CBMRegisteredType auto_t;
                    memset(&auto_t, 0, sizeof(auto_t));
                    auto_t.qualified_name = rf.receiver_type;
                    const char *dot = strrchr(d->receiver_type, '.');
                    auto_t.short_name = dot ? cbm_arena_strdup(arena, dot + 1) : rf.receiver_type;
                    cbm_registry_add_type(reg, auto_t);
                    cbm_idxmemo_put_if_absent(&type_idx, arena, auto_t.qualified_name,
                                              reg->type_count - 1);
                }
            }

            cbm_registry_add_func(reg, rf);

            /* If trait_qn set: encode embedded_type linkage on receiver. */
            if (rf.receiver_type && d->trait_qn && d->trait_qn[0]) {
                CBMRegisteredType *rt = NULL;
                int32_t tix = cbm_idxmemo_get(&type_idx, rf.receiver_type);
                if (tix >= 0) {
                    rt = &reg->types[tix];
                }
                if (rt) {
                    int existing = 0;
                    if (rt->embedded_types)
                        while (rt->embedded_types[existing])
                            existing++;
                    const char **new_arr = (const char **)cbm_arena_alloc(
                        arena, (existing + 2) * sizeof(const char *));
                    for (int j = 0; j < existing; j++)
                        new_arr[j] = rt->embedded_types[j];
                    new_arr[existing] = cbm_arena_strdup(arena, d->trait_qn);
                    new_arr[existing + 1] = NULL;
                    rt->embedded_types = new_arr;
                }
            }
        }
    }

    /* Finalise the cross-file registry now that all defs are added.
     * (type_idx is arena-owned — freed with the registry's arena.) */
    cbm_registry_finalize(reg);
}

/* Resolve one Rust file against an ALREADY-built (per-file or shared) registry. */
static void rust_resolve_against_registry(CBMArena *arena, const char *source, int source_len,
                                          const char *module_qn, const CBMTypeRegistry *reg,
                                          const char **import_names, const char **import_qns,
                                          int import_count, TSNode root,
                                          const struct CBMCargoManifest *manifest,
                                          CBMResolvedCallArray *out, CBMFileResult *result) {
    RustLSPContext ctx;
    rust_lsp_init(&ctx, arena, source, source_len, reg, module_qn, out);
    ctx.cargo_manifest = manifest;
    rust_collect_uses(&ctx, root);
    for (int i = 0; i < import_count; i++) {
        if (import_names[i] && import_qns[i]) {
            rust_lsp_add_use(&ctx, import_names[i], import_qns[i]);
        }
    }
    rust_lsp_process_file(&ctx, root);
    if (result)
        cbm_rust_synth_proc_macro_edges(arena, result);
}

/* Tier-2: build the Rust cross registry ONCE from all project defs, sealed
 * read-only, and shared across every Rust file's resolve (mirrors C/py/cs/ts).
 * Converts the pipeline's CBMLSPDef into CBMRustLSPDef inline (same field copy as
 * pass_lsp_cross.c's pxc_lspdefs_to_rust, incl. trait_qn=NULL). module_qn=NULL is
 * byte-identical because all_defs always carry def_module_qn. */
CBMTypeRegistry *cbm_rust_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count) {
    if (!arena)
        return NULL;
    CBMTypeRegistry *reg = (CBMTypeRegistry *)cbm_arena_alloc(arena, sizeof(*reg));
    if (!reg)
        return NULL;
    CBMRustLSPDef *rdefs = NULL;
    if (def_count > 0) {
        rdefs = (CBMRustLSPDef *)cbm_arena_alloc(arena, (size_t)def_count * sizeof(CBMRustLSPDef));
        if (!rdefs)
            return NULL;
        for (int i = 0; i < def_count; i++) {
            rdefs[i].qualified_name = defs[i].qualified_name;
            rdefs[i].short_name = defs[i].short_name;
            rdefs[i].label = defs[i].label;
            rdefs[i].receiver_type = defs[i].receiver_type;
            rdefs[i].def_module_qn = defs[i].def_module_qn;
            rdefs[i].return_types = defs[i].return_types;
            rdefs[i].embedded_types = defs[i].embedded_types;
            rdefs[i].field_defs = defs[i].field_defs;
            rdefs[i].method_names_str = defs[i].method_names_str;
            rdefs[i].trait_qn = NULL;
            rdefs[i].is_interface = defs[i].is_interface;
        }
    }
    rust_populate_cross_registry(reg, arena, rdefs, def_count, /*module_qn=*/NULL);
    reg->read_only = true; /* seal: shared Tier-2 registry is read-only during resolve */
    return reg;
}

/* Cross-file Rust resolve using a pre-built shared registry (Tier-2). Skips the
 * per-file registry build; just parse + resolve. Mirrors cbm_run_c_lsp_cross_with_registry. */
void cbm_run_rust_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                          const char *module_qn, const CBMTypeRegistry *reg,
                                          const char **import_names, const char **import_qns,
                                          int import_count, TSTree *cached_tree,
                                          const struct CBMCargoManifest *manifest,
                                          CBMResolvedCallArray *out, CBMFileResult *result) {
    if (!source || source_len <= 0 || !out || !reg)
        return;
    TSParser *parser = NULL;
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return;
        ts_parser_set_language(parser, tree_sitter_rust());
        tree = ts_parser_parse_string(parser, NULL, source, source_len);
        owns_tree = true;
        if (!tree) {
            ts_parser_delete(parser);
            return;
        }
    }
    TSNode root = ts_tree_root_node(tree);
    rust_resolve_against_registry(arena, source, source_len, module_qn, reg, import_names,
                                  import_qns, import_count, root, manifest, out, result);
    if (owns_tree) {
        ts_tree_delete(tree);
        if (parser)
            ts_parser_delete(parser);
    }
}

void cbm_run_rust_lsp_cross_with_manifest(CBMArena *arena, const char *source, int source_len,
                                          const char *module_qn, CBMRustLSPDef *defs, int def_count,
                                          const char **import_names, const char **import_qns,
                                          int import_count, TSTree *cached_tree,
                                          const struct CBMCargoManifest *manifest,
                                          CBMResolvedCallArray *out) {
    if (!source || source_len <= 0 || !out)
        return;

    TSParser *parser = NULL;
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return;
        ts_parser_set_language(parser, tree_sitter_rust());
        tree = ts_parser_parse_string(parser, NULL, source, source_len);
        owns_tree = true;
        if (!tree) {
            ts_parser_delete(parser);
            return;
        }
    }
    TSNode root = ts_tree_root_node(tree);

    /* Build registry from cross-file defs + stdlib (per-file). */
    CBMTypeRegistry reg;
    rust_populate_cross_registry(&reg, arena, defs, def_count, module_qn);

    RustLSPContext ctx;
    rust_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, out);
    /* Workspace/dependency awareness for cross-CRATE path routing (#56).
     * Mirrors the single-file path (cbm_run_rust_lsp_with_manifest). NULL
     * when no Cargo.toml was parsed — in-crate resolution is unaffected. */
    ctx.cargo_manifest = manifest;
    rust_collect_uses(&ctx, root);
    for (int i = 0; i < import_count; i++) {
        if (import_names[i] && import_qns[i]) {
            rust_lsp_add_use(&ctx, import_names[i], import_qns[i]);
        }
    }
    rust_lsp_process_file(&ctx, root);

    if (owns_tree) {
        ts_tree_delete(tree);
        if (parser)
            ts_parser_delete(parser);
    }
}

/* Manifest-free entry point. Preserves the pre-existing signature used by
 * the unit tests (test_rust_lsp.c) and the batch wrapper — delegates to
 * the manifest-aware variant with a NULL manifest. */
void cbm_run_rust_lsp_cross(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, CBMRustLSPDef *defs, int def_count,
                            const char **import_names, const char **import_qns, int import_count,
                            TSTree *cached_tree, CBMResolvedCallArray *out) {
    cbm_run_rust_lsp_cross_with_manifest(arena, source, source_len, module_qn, defs, def_count,
                                         import_names, import_qns, import_count, cached_tree, NULL,
                                         out);
}

void cbm_batch_rust_lsp_cross(CBMArena *arena, CBMBatchRustLSPFile *files, int file_count,
                              CBMResolvedCallArray *out) {
    if (!files || file_count <= 0 || !out)
        return;

    for (int f = 0; f < file_count; f++) {
        CBMBatchRustLSPFile *file = &files[f];
        memset(&out[f], 0, sizeof(CBMResolvedCallArray));
        if (!file->source || file->source_len <= 0)
            continue;

        CBMArena file_arena;
        cbm_arena_init(&file_arena);

        CBMResolvedCallArray file_out;
        memset(&file_out, 0, sizeof(file_out));

        cbm_run_rust_lsp_cross(&file_arena, file->source, file->source_len, file->module_qn,
                               file->defs, file->def_count, file->import_names, file->import_qns,
                               file->import_count, file->cached_tree, &file_out);

        if (file_out.count > 0) {
            out[f].count = file_out.count;
            out[f].items =
                (CBMResolvedCall *)cbm_arena_alloc(arena, file_out.count * sizeof(CBMResolvedCall));
            for (int j = 0; j < file_out.count; j++) {
                CBMResolvedCall *src = &file_out.items[j];
                CBMResolvedCall *dst = &out[f].items[j];
                dst->caller_qn = src->caller_qn ? cbm_arena_strdup(arena, src->caller_qn) : NULL;
                dst->callee_qn = src->callee_qn ? cbm_arena_strdup(arena, src->callee_qn) : NULL;
                dst->strategy = src->strategy ? cbm_arena_strdup(arena, src->strategy) : NULL;
                dst->confidence = src->confidence;
                dst->reason = src->reason ? cbm_arena_strdup(arena, src->reason) : NULL;
            }
        }

        cbm_arena_destroy(&file_arena);
    }
}
