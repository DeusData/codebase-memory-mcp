/*
 * ts_lsp.c — TypeScript / JavaScript / JSX / TSX hybrid LSP type resolver.
 *
 * Phase 2 v1.0 resolver. Covers:
 *   - Categories 1–4: param type inference, return type propagation, method chaining,
 *                     multi-return / destructuring.
 *   - Categories 7–10: object literal property typing, type aliases, class field/method
 *                      dispatch (including `this`/`super`), interface dispatch.
 *   - Category 11 (partial): explicit/implicit generic instantiation via TEMPLATE types.
 *   - Category 12 (partial): hand-curated stdlib seeds (Promise, Array, Map, Set, etc.).
 *   - Category 13: optional chaining `obj?.member()` propagates type through.
 *   - Category 14: `await p` unwraps `Promise<T>` → T.
 *   - Category 15 (partial): `typeof` narrowing inside `if`/ternary.
 *   - Category 16: union member access tries each branch.
 *   - Category 17: literal types (string/number/bool literal types map to BUILTIN).
 *   - Category 20: imports surface `module.symbol` resolution against the registry.
 *   - Category 25: emits unresolved-call diagnostics (confidence 0).
 *
 * Cross-file (Phase 3) merges defs project-wide before per-file resolution; see
 * cbm_batch_ts_lsp_cross.
 *
 * Conventions (mirrors c_lsp / go_lsp):
 *   - All allocations go through ctx->arena; no malloc / free.
 *   - ts_emit_resolved_call requires a non-NULL enclosing_func_qn — calls outside any
 *     function (module top-level) are not emitted.
 *   - eval_depth caps at TS_LSP_MAX_EVAL_DEPTH to defend against pathological recursion.
 */

#include "ts_lsp.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TS_LSP_MAX_EVAL_DEPTH 64
#define TS_LSP_FIELD_LEN(s) ((uint32_t)(sizeof(s) - 1))

// Tree-sitter grammar entry points for the three TS dialects (compiled into the binary).
extern const TSLanguage *tree_sitter_typescript(void);
extern const TSLanguage *tree_sitter_tsx(void);
extern const TSLanguage *tree_sitter_javascript(void);

// ── Forward declarations ──────────────────────────────────────────────────────

static const CBMType *parse_ts_type_text(CBMArena *arena, const char *text, const char *module_qn);
static void process_node(TSLSPContext *ctx, TSNode node);
static void process_function_body(TSLSPContext *ctx, TSNode body, const char *func_qn,
                                  const char *class_qn);
static const CBMType *type_of_identifier(TSLSPContext *ctx, const char *name);
static const CBMType *lookup_member_type(TSLSPContext *ctx, const CBMType *recv, const char *name);
static const CBMRegisteredFunc *lookup_method(TSLSPContext *ctx, const CBMType *recv,
                                              const char *method_name);
static char *node_text(TSLSPContext *ctx, TSNode node);

// Collect a node's children into an arena array via a single O(n) cursor pass.
// Returns NULL (and sets *out_n=0) for a childless node or on OOM. Use this in
// place of the `for (i=0; i<count; i++) ts_node_child(node, i)` idiom: in
// tree-sitter ts_node_child(node, i) is O(i), so that loop is O(n²) on a wide
// node — e.g. a program root holding hundreds of thousands of top-level nodes
// (the reallyLargeFile.ts fixture is 583K lines of comment markup → 583K
// children, which made the per-file LSP passes run for ~133 minutes).
static TSNode *collect_children(CBMArena *arena, TSNode node, uint32_t *out_n) {
    uint32_t nc = ts_node_child_count(node);
    *out_n = 0;
    if (nc == 0)
        return NULL;
    TSNode *kids = (TSNode *)cbm_arena_alloc(arena, (size_t)nc * sizeof(TSNode));
    if (!kids)
        return NULL;
    uint32_t kn = 0;
    TSTreeCursor cur = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            kids[kn++] = ts_tree_cursor_current_node(&cur);
        } while (kn < nc && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    *out_n = kn;
    return kids;
}
static const CBMType *simplify_type(TSLSPContext *ctx, const CBMType *t);
static const CBMType *unwrap_passthrough_template(const CBMType *t);

// ── Helpers ───────────────────────────────────────────────────────────────────

static char *node_text(TSLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

static bool node_kind_is(TSNode node, const char *kind) {
    if (ts_node_is_null(node))
        return false;
    return strcmp(ts_node_type(node), kind) == 0;
}

// Find the first named child of `node` whose kind matches one of the listed kinds.
static TSNode find_first_kind(TSNode node, const char *kind) {
    if (ts_node_is_null(node))
        return node;
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(node, i);
        if (!ts_node_is_null(c) && strcmp(ts_node_type(c), kind) == 0)
            return c;
    }
    TSNode null_node = {0};
    return null_node;
}

static void ts_emit_resolved_call(TSLSPContext *ctx, const char *callee_qn, const char *strategy,
                                  float confidence) {
    if (!ctx || !ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy ? strategy : "lsp_ts";
    rc.confidence = confidence;
    rc.reason = NULL;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

static void ts_emit_unresolved_call(TSLSPContext *ctx, const char *expr_text, const char *reason) {
    if (!ctx || !ctx->resolved_calls || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = expr_text ? expr_text : "?";
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = reason;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

// Parse a textual TS type into a CBMType. Pragmatic v1: handle the common cases
// (identifier, qualified.identifier, T[], T<U,V>, Promise<T>, T | U, T & U, predefined).
// Anything more complex falls back to NAMED with the original text.
//
// Trims leading whitespace, leading ":" (TS type-annotation prefix produced by
// `cbm_node_text` on a `type_annotation` AST node), and trailing whitespace/`;`.
static const CBMType *parse_ts_type_text(CBMArena *arena, const char *text, const char *module_qn) {
    if (!text || !text[0])
        return cbm_type_unknown();

    // Strip leading whitespace, then a leading ':' (type annotation), then more whitespace.
    while (*text == ' ' || *text == '\t' || *text == '\n' || *text == '\r')
        text++;
    if (*text == ':')
        text++;
    while (*text == ' ' || *text == '\t' || *text == '\n' || *text == '\r')
        text++;

    size_t len = strlen(text);
    while (len > 0 && (text[len - 1] == ' ' || text[len - 1] == '\t' || text[len - 1] == '\n' ||
                       text[len - 1] == '\r' || text[len - 1] == ';' || text[len - 1] == ','))
        len--;
    if (len == 0)
        return cbm_type_unknown();

    // Function type `(params) => returnType` (TS function type literal).
    if (text[0] == '(') {
        int depth = 0;
        size_t close = (size_t)-1;
        for (size_t i = 0; i < len; i++) {
            if (text[i] == '(')
                depth++;
            else if (text[i] == ')') {
                depth--;
                if (depth == 0) {
                    close = i;
                    break;
                }
            }
        }
        if (close == (size_t)-1)
            return cbm_type_unknown();
        size_t after = close + 1;
        while (after < len && (text[after] == ' ' || text[after] == '\t'))
            after++;
        if (after + 2 <= len && text[after] == '=' && text[after + 1] == '>') {
            // Parse return type.
            const char *ret_text = text + after + 2;
            const CBMType *ret = parse_ts_type_text(arena, ret_text, module_qn);

            // Parse params: split by top-level commas, each "name: type" or just "type".
            const char *fn_param_names[16] = {0};
            const CBMType *fn_param_types[16] = {0};
            int pc = 0;
            const char *params_text = text + 1;
            size_t params_len = close - 1;
            if (params_len > 0) {
                size_t start = 0;
                int pdepth = 0;
                for (size_t i = 0; i <= params_len && pc < 15; i++) {
                    char c = (i < params_len) ? params_text[i] : ',';
                    if (c == '(' || c == '<' || c == '[')
                        pdepth++;
                    else if (c == ')' || c == '>' || c == ']')
                        pdepth--;
                    else if (c == ',' && pdepth == 0) {
                        if (i > start) {
                            char *part = cbm_arena_strndup(arena, params_text + start, i - start);
                            char *colon = strchr(part, ':');
                            if (colon) {
                                *colon = '\0';
                                const char *nm = part;
                                while (*nm == ' ' || *nm == '\t')
                                    nm++;
                                size_t nl = strlen(nm);
                                while (nl > 0 && (nm[nl - 1] == ' ' || nm[nl - 1] == '?'))
                                    nl--;
                                fn_param_names[pc] = cbm_arena_strndup(arena, nm, nl);
                                fn_param_types[pc] =
                                    parse_ts_type_text(arena, colon + 1, module_qn);
                            } else {
                                fn_param_names[pc] = cbm_arena_sprintf(arena, "_%d", pc);
                                fn_param_types[pc] = parse_ts_type_text(arena, part, module_qn);
                            }
                            pc++;
                        }
                        start = i + 1;
                    }
                }
            }
            fn_param_names[pc] = NULL;
            fn_param_types[pc] = NULL;
            const CBMType *rets[2] = {ret, NULL};
            return cbm_type_func(arena, fn_param_names, fn_param_types, rets);
        }
        // Parens without arrow → return UNKNOWN.
        return cbm_type_unknown();
    }

    // Object type literal `{...}` and other unhandled — return UNKNOWN.
    if (text[0] == '{')
        return cbm_type_unknown();

    // TS polymorphic `this` return type: emit a TYPE_PARAM("this") sentinel that
    // ts_eval_expr_type / lookup_method substitute back to the actual receiver at
    // call site. Used by fluent-builder patterns: `class C { a(): this { ... } }`.
    if (len == 4 && memcmp(text, "this", 4) == 0) {
        return cbm_type_type_param(arena, "this");
    }

    // Conditional type `T extends U ? X : Y` — split on top-level " extends "
    // (followed downstream by " ? " and " : "). Detection: must have all three
    // keywords at depth 0, in order.
    {
        int depth = 0;
        size_t ext_pos = (size_t)-1;
        size_t q_pos = (size_t)-1;
        size_t colon_pos = (size_t)-1;
        for (size_t i = 0; i < len; i++) {
            char c = text[i];
            if (c == '<' || c == '[' || c == '(')
                depth++;
            else if (c == '>' || c == ']' || c == ')')
                depth--;
            if (depth != 0)
                continue;
            if (ext_pos == (size_t)-1 && i > 0 && text[i] == ' ' && i + 9 <= len &&
                memcmp(text + i, " extends ", 9) == 0) {
                ext_pos = i;
                i += 8;
                continue;
            }
            if (ext_pos != (size_t)-1 && q_pos == (size_t)-1 && i + 3 <= len && text[i] == ' ' &&
                text[i + 1] == '?' && text[i + 2] == ' ') {
                q_pos = i;
                i += 2;
                continue;
            }
            if (q_pos != (size_t)-1 && colon_pos == (size_t)-1 && i + 3 <= len && text[i] == ' ' &&
                text[i + 1] == ':' && text[i + 2] == ' ') {
                colon_pos = i;
                break;
            }
        }
        if (ext_pos != (size_t)-1 && q_pos != (size_t)-1 && colon_pos != (size_t)-1) {
            char *check_text = cbm_arena_strndup(arena, text, ext_pos);
            char *extends_text =
                cbm_arena_strndup(arena, text + ext_pos + 9, q_pos - (ext_pos + 9));
            char *true_text = cbm_arena_strndup(arena, text + q_pos + 3, colon_pos - (q_pos + 3));
            char *false_text =
                cbm_arena_strndup(arena, text + colon_pos + 3, len - (colon_pos + 3));
            const CBMType *check = parse_ts_type_text(arena, check_text, module_qn);
            const CBMType *extends = parse_ts_type_text(arena, extends_text, module_qn);
            const CBMType *true_branch = parse_ts_type_text(arena, true_text, module_qn);
            const CBMType *false_branch = parse_ts_type_text(arena, false_text, module_qn);
            return cbm_type_conditional(arena, check, extends, true_branch, false_branch);
        }
    }

    // `infer X` — emit CBM_TYPE_INFER. Only meaningful inside conditional `extends`
    // patterns; emitted standalone here and matched later by eval_conditional.
    if (len > 6 && memcmp(text, "infer ", 6) == 0) {
        const char *rest = text + 6;
        while (*rest == ' ' || *rest == '\t')
            rest++;
        const char *end = rest;
        while (*end && *end != ' ' && *end != '\t')
            end++;
        char *name = cbm_arena_strndup(arena, rest, (size_t)(end - rest));
        return cbm_type_infer(arena, name);
    }

    // `keyof T` — emit CBM_TYPE_KEYOF for downstream evaluation.
    if (len > 6 && memcmp(text, "keyof ", 6) == 0) {
        const char *rest = text + 6;
        while (*rest == ' ' || *rest == '\t')
            rest++;
        char *rest_term = cbm_arena_strdup(arena, rest);
        const CBMType *operand = parse_ts_type_text(arena, rest_term, module_qn);
        return cbm_type_keyof(arena, operand);
    }

    // `typeof X` in type position — emit CBM_TYPE_TYPEOF_QUERY.
    if (len > 7 && memcmp(text, "typeof ", 7) == 0) {
        const char *rest = text + 7;
        while (*rest == ' ' || *rest == '\t')
            rest++;
        return cbm_type_typeof_query(arena, rest);
    }

    // Tuple `[T, U]` — first char `[`, balanced. Build TUPLE.
    if (text[0] == '[' && len >= 2 && text[len - 1] == ']') {
        const char *inner_text = text + 1;
        size_t inner_len = len - 2;
        const CBMType *elems[16] = {0};
        int ec = 0;
        size_t start = 0;
        int depth = 0;
        for (size_t i = 0; i <= inner_len && ec < 15; i++) {
            char c = (i < inner_len) ? inner_text[i] : ',';
            if (c == '<' || c == '[')
                depth++;
            else if (c == '>' || c == ']')
                depth--;
            else if (c == ',' && depth == 0) {
                char *part = cbm_arena_strndup(arena, inner_text + start, i - start);
                elems[ec++] = parse_ts_type_text(arena, part, module_qn);
                start = i + 1;
            }
        }
        elems[ec] = NULL;
        return cbm_type_tuple(arena, elems, ec);
    }

    // Top-level UNION ` | ` or INTERSECTION ` & ` — split at depth 0 (outside <>, []).
    {
        int depth = 0;
        const CBMType *members[16] = {0};
        int mc = 0;
        size_t start = 0;
        bool is_union = false;
        bool is_inter = false;
        for (size_t i = 0; i < len; i++) {
            char c = text[i];
            if (c == '<' || c == '[')
                depth++;
            else if (c == '>' || c == ']')
                depth--;
            else if (depth == 0 && (c == '|' || c == '&') && i > 0 &&
                     (text[i - 1] == ' ' || text[i - 1] == '\t')) {
                if (mc < 15) {
                    char *part = cbm_arena_strndup(arena, text + start, i - start);
                    members[mc++] = parse_ts_type_text(arena, part, module_qn);
                }
                if (c == '|')
                    is_union = true;
                else
                    is_inter = true;
                start = i + 1;
            }
        }
        if (mc > 0 && (is_union || is_inter)) {
            // Cap mirrors the in-loop guard: members[16] holds at most 15
            // real entries + the trailing NULL sentinel. Without this guard,
            // a union/intersection with >=16 members overflows the array
            // (UBSan: index 16 out of bounds for 'const CBMType *[16]').
            if (mc < 15) {
                char *part = cbm_arena_strndup(arena, text + start, len - start);
                members[mc++] = parse_ts_type_text(arena, part, module_qn);
            }
            members[mc] = NULL;
            return is_union ? cbm_type_union(arena, members, mc)
                            : cbm_type_intersection(arena, members, mc);
        }
    }

    // Builtins / predefined type keywords.
    static const char *const builtins[] = {
        "string", "number", "boolean",   "bigint", "any",    "unknown", "void",
        "never",  "null",   "undefined", "object", "symbol", NULL,
    };
    for (int i = 0; builtins[i]; i++) {
        size_t bl = strlen(builtins[i]);
        if (len == bl && memcmp(text, builtins[i], bl) == 0) {
            return cbm_type_builtin(arena, builtins[i]);
        }
    }

    // Trailing `[]` → Array<elem>. Strip and recurse.
    if (len >= 2 && text[len - 1] == ']' && text[len - 2] == '[') {
        char *inner = cbm_arena_strndup(arena, text, len - 2);
        const CBMType *elem = parse_ts_type_text(arena, inner, module_qn);
        const CBMType *args[2] = {elem, NULL};
        return cbm_type_template(arena, "Array", args, 1);
    }

    // Generic instantiation `Foo<...>` — find balanced `<...>` at the end.
    if (len > 2 && text[len - 1] == '>') {
        int depth = 0;
        size_t open = (size_t)-1;
        for (size_t i = len; i-- > 0;) {
            char c = text[i];
            if (c == '>')
                depth++;
            else if (c == '<') {
                depth--;
                if (depth == 0) {
                    open = i;
                    break;
                }
            }
        }
        if (open != (size_t)-1 && open > 0) {
            char *base = cbm_arena_strndup(arena, text, open);
            const char *args_text = text + open + 1;
            size_t args_len = (len - 1) - (open + 1);
            // Split args by top-level commas.
            const CBMType *args[16] = {0};
            int arg_count = 0;
            size_t start = 0;
            int adepth = 0;
            for (size_t i = 0; i <= args_len && arg_count < 15; i++) {
                char c = (i < args_len) ? args_text[i] : ',';
                if (c == '<')
                    adepth++;
                else if (c == '>')
                    adepth--;
                else if (c == ',' && adepth == 0) {
                    char *part = cbm_arena_strndup(arena, args_text + start, i - start);
                    args[arg_count++] = parse_ts_type_text(arena, part, module_qn);
                    start = i + 1;
                }
            }
            args[arg_count] = NULL;
            // Qualify project-local generic bases against the current module so
            // `Container<number>` becomes TEMPLATE("test.main.Container", [number]).
            // Stdlib bases (Array, Promise, Map, Set, Object) stay bare so they match
            // the stdlib registration.
            const char *qualified_base = base;
            static const char *const stdlib_names[] = {
                "Array",
                "Promise",
                "Map",
                "Set",
                "WeakMap",
                "WeakSet",
                "ReadonlyArray",
                "Iterable",
                "Iterator",
                "Generator",
                "AsyncIterable",
                "AsyncIterator",
                "AsyncGenerator",
                "Object",
                "String",
                "Number",
                "Boolean",
                "BigInt",
                "Symbol",
                "Function",
                "Date",
                "RegExp",
                "Error",
                "Partial",
                "Required",
                "Readonly",
                "Pick",
                "Omit",
                "Record",
                "Exclude",
                "Extract",
                "NonNullable",
                "Parameters",
                "ReturnType",
                "Awaited",
                "ThisType",
                "InstanceType",
                "ConstructorParameters",
                "Uppercase",
                "Lowercase",
                "Capitalize",
                "Uncapitalize",
                "NodeList",
                "HTMLCollection",
                NULL,
            };
            bool is_stdlib = false;
            for (int sl = 0; stdlib_names[sl]; sl++) {
                if (strcmp(base, stdlib_names[sl]) == 0) {
                    is_stdlib = true;
                    break;
                }
            }
            if (!is_stdlib && module_qn && strchr(base, '.') == NULL && base[0] >= 'A' &&
                base[0] <= 'Z') {
                qualified_base = cbm_arena_sprintf(arena, "%s.%s", module_qn, base);
            }
            return cbm_type_template(arena, qualified_base, args, arg_count);
        }
    }

    // Qualified identifier (a.b.c) → NAMED with whatever text we have. Caller-side lookup
    // will check both the QN as-given and module-qualified variants.
    if (module_qn && strchr(text, '.') == NULL) {
        // Stdlib names stay bare so they match the stdlib registration QNs.
        static const char *const bare_stdlib_names[] = {
            "Array",
            "Promise",
            "Map",
            "Set",
            "WeakMap",
            "WeakSet",
            "ReadonlyArray",
            "Iterable",
            "Iterator",
            "Generator",
            "AsyncIterable",
            "AsyncIterator",
            "AsyncGenerator",
            "Object",
            "String",
            "Number",
            "Boolean",
            "BigInt",
            "Symbol",
            "Function",
            "Date",
            "RegExp",
            "Error",
            "Math",
            "JSON",
            "console",
            // DOM essentials
            "Element",
            "HTMLElement",
            "Document",
            "Window",
            "Node",
            "EventTarget",
            "Event",
            "Response",
            "Request",
            "Headers",
            "URL",
            "URLSearchParams",
            "FormData",
            "Blob",
            "File",
            "NodeList",
            "HTMLCollection",
            "JSX",
            NULL,
        };
        char *terminated = cbm_arena_strndup(arena, text, len);
        if (!terminated)
            return cbm_type_unknown();
        for (int i = 0; bare_stdlib_names[i]; i++) {
            if (strcmp(terminated, bare_stdlib_names[i]) == 0) {
                return cbm_type_named(arena, terminated);
            }
        }
        // Bare uppercase identifier — qualify against the current module if it looks
        // like a TypeIdent. Lower-case names are likely params/values, fall back.
        if ((terminated[0] >= 'A' && terminated[0] <= 'Z')) {
            const char *qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, terminated);
            return cbm_type_named(arena, qn);
        }
        return cbm_type_named(arena, terminated);
    }
    return cbm_type_named(arena, cbm_arena_strndup(arena, text, len));
}

// Return param-type CBMType array from a CBMDefinition's param_types text array.
static const CBMType **parse_param_types_array(CBMArena *arena, const char **texts,
                                               const char *module_qn) {
    if (!texts)
        return NULL;
    int count = 0;
    while (texts[count])
        count++;
    if (count == 0)
        return NULL;
    const CBMType **arr =
        (const CBMType **)cbm_arena_alloc(arena, (size_t)(count + 1) * sizeof(const CBMType *));
    if (!arr)
        return NULL;
    for (int i = 0; i < count; i++) {
        arr[i] = parse_ts_type_text(arena, texts[i], module_qn);
    }
    arr[count] = NULL;
    return arr;
}

// ── Partial structural subtyping (relater) ────────────────────────────────────
//
// Answers `is A assignable to B?` for the subset of TS types we support. Modeled
// loosely on typescript-go's `internal/checker/relater.go` but limited to the cases
// that matter for call-edge resolution. NOT a full type-checker — many edge cases
// return false rather than recursing into undecidable territory.
//
// Used by:
// - Conditional type evaluation (`T extends U ? X : Y`)
// - Generic constraint checks (`<T extends Animal>`)
// - Future overload-by-types refinement
//
// Rules implemented:
// - any/unknown/never on either side: liberal acceptance
// - NAMED == NAMED: equal QN OR walk the source's `embedded_types` (extends chain)
// - BUILTIN: identical name; LITERAL("string", X) ≤ BUILTIN("string"), etc.
// - TUPLE: same arity, element-wise
// - UNION source: every member assignable to target
// - UNION target: source assignable to any member
// - INTERSECTION source: any member assignable to target
// - INTERSECTION target: source assignable to all members
// - FUNC: param contravariance, return covariance, parameter-count tolerance
// - TEMPLATE: same name + each arg assignable
// - TYPE_PARAM: same name → true; otherwise check constraint when known
// - ALIAS: resolve and recurse
// - any UNKNOWN type: false
//
// Cycle/depth guard: max 64 nested calls. Pair-cache keyed on pointer identity
// (sufficient given arena-stable types within a single resolver pass).

#define TS_RELATER_MAX_DEPTH 64
#define TS_RELATER_CACHE_SIZE 256

typedef struct {
    const CBMType *a;
    const CBMType *b;
    int8_t result; // -1 = unknown, 0 = false, 1 = true
} TSRelaterCacheSlot;

static int ts_is_assignable_inner(TSLSPContext *ctx, const CBMType *a, const CBMType *b, int depth,
                                  TSRelaterCacheSlot *cache);

static const CBMType *ts_relater_unwrap(const CBMType *t) {
    if (!t)
        return t;
    if (t->kind == CBM_TYPE_ALIAS)
        return cbm_type_resolve_alias(t);
    return t;
}

static bool ts_builtin_compatible(const char *a, const char *b) {
    if (!a || !b)
        return false;
    if (strcmp(a, b) == 0)
        return true;
    // `any` and `unknown` accept anything (handled by caller before this).
    return false;
}

static bool ts_literal_promotes_to_builtin(const CBMType *lit, const CBMType *bi) {
    if (!lit || lit->kind != CBM_TYPE_TS_LITERAL || !lit->data.literal_ts.tag)
        return false;
    if (!bi || bi->kind != CBM_TYPE_BUILTIN || !bi->data.builtin.name)
        return false;
    return strcmp(lit->data.literal_ts.tag, bi->data.builtin.name) == 0;
}

static bool ts_class_extends(TSLSPContext *ctx, const char *sub_qn, const char *super_qn,
                             int depth) {
    if (!ctx || !sub_qn || !super_qn)
        return false;
    if (depth > TS_RELATER_MAX_DEPTH)
        return false;
    if (strcmp(sub_qn, super_qn) == 0)
        return true;
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, sub_qn);
    if (!rt || !rt->embedded_types)
        return false;
    for (int i = 0; rt->embedded_types[i]; i++) {
        if (ts_class_extends(ctx, rt->embedded_types[i], super_qn, depth + 1))
            return true;
    }
    return false;
}

static int ts_is_assignable_inner(TSLSPContext *ctx, const CBMType *a, const CBMType *b, int depth,
                                  TSRelaterCacheSlot *cache) {
    if (depth > TS_RELATER_MAX_DEPTH)
        return 0;
    if (!a || !b)
        return 0;

    // Cache probe (pointer identity is sufficient — arena-stable within a resolver pass).
    int slot = (int)((((uintptr_t)a >> 4) ^ ((uintptr_t)b >> 4)) & (TS_RELATER_CACHE_SIZE - 1));
    if (cache[slot].a == a && cache[slot].b == b && cache[slot].result >= 0) {
        return cache[slot].result;
    }

    a = ts_relater_unwrap(a);
    b = ts_relater_unwrap(b);
    if (!a || !b)
        return 0;

    // any/unknown/never short-circuits.
    if (b->kind == CBM_TYPE_BUILTIN && b->data.builtin.name) {
        if (strcmp(b->data.builtin.name, "any") == 0 ||
            strcmp(b->data.builtin.name, "unknown") == 0)
            return 1;
    }
    if (a->kind == CBM_TYPE_BUILTIN && a->data.builtin.name) {
        if (strcmp(a->data.builtin.name, "any") == 0)
            return 1;
        if (strcmp(a->data.builtin.name, "never") == 0)
            return 1;
    }

    int result = 0;

    // Source unions: every member must be assignable to target.
    if (a->kind == CBM_TYPE_UNION && a->data.union_type.members) {
        result = 1;
        for (int i = 0; i < a->data.union_type.count; i++) {
            if (!ts_is_assignable_inner(ctx, a->data.union_type.members[i], b, depth + 1, cache)) {
                result = 0;
                break;
            }
        }
        goto done;
    }
    // Target unions: source assignable to any member.
    if (b->kind == CBM_TYPE_UNION && b->data.union_type.members) {
        for (int i = 0; i < b->data.union_type.count; i++) {
            if (ts_is_assignable_inner(ctx, a, b->data.union_type.members[i], depth + 1, cache)) {
                result = 1;
                break;
            }
        }
        goto done;
    }
    // Source intersection: any member assignable to target.
    if (a->kind == CBM_TYPE_INTERSECTION && a->data.union_type.members) {
        for (int i = 0; i < a->data.union_type.count; i++) {
            if (ts_is_assignable_inner(ctx, a->data.union_type.members[i], b, depth + 1, cache)) {
                result = 1;
                break;
            }
        }
        goto done;
    }
    // Target intersection: source assignable to all members.
    if (b->kind == CBM_TYPE_INTERSECTION && b->data.union_type.members) {
        result = 1;
        for (int i = 0; i < b->data.union_type.count; i++) {
            if (!ts_is_assignable_inner(ctx, a, b->data.union_type.members[i], depth + 1, cache)) {
                result = 0;
                break;
            }
        }
        goto done;
    }

    if (a->kind != b->kind) {
        // Cross-kind rules: LITERAL → BUILTIN, NAMED-class → NAMED-class via extends.
        if (a->kind == CBM_TYPE_TS_LITERAL && b->kind == CBM_TYPE_BUILTIN) {
            result = ts_literal_promotes_to_builtin(a, b) ? 1 : 0;
            goto done;
        }
        // TYPE_PARAM source: check constraint if known.
        if (a->kind == CBM_TYPE_TYPE_PARAM) {
            // Without registered constraint info available here, treat as "unknown" (fail).
            result = 0;
            goto done;
        }
        result = 0;
        goto done;
    }

    // Same-kind cases.
    switch (a->kind) {
    case CBM_TYPE_NAMED: {
        const char *aqn = a->data.named.qualified_name;
        const char *bqn = b->data.named.qualified_name;
        if (!aqn || !bqn) {
            result = 0;
            break;
        }
        if (strcmp(aqn, bqn) == 0) {
            result = 1;
            break;
        }
        // class hierarchy: A extends B?
        result = ts_class_extends(ctx, aqn, bqn, 0) ? 1 : 0;
        break;
    }
    case CBM_TYPE_BUILTIN:
        result = ts_builtin_compatible(a->data.builtin.name, b->data.builtin.name) ? 1 : 0;
        break;
    case CBM_TYPE_TS_LITERAL:
        if (a->data.literal_ts.tag && b->data.literal_ts.tag &&
            strcmp(a->data.literal_ts.tag, b->data.literal_ts.tag) == 0 &&
            a->data.literal_ts.value && b->data.literal_ts.value &&
            strcmp(a->data.literal_ts.value, b->data.literal_ts.value) == 0) {
            result = 1;
        } else {
            result = 0;
        }
        break;
    case CBM_TYPE_TUPLE: {
        int ac = a->data.tuple.count;
        int bc = b->data.tuple.count;
        if (ac != bc) {
            result = 0;
            break;
        }
        result = 1;
        for (int i = 0; i < ac; i++) {
            if (!ts_is_assignable_inner(ctx, a->data.tuple.elems[i], b->data.tuple.elems[i],
                                        depth + 1, cache)) {
                result = 0;
                break;
            }
        }
        break;
    }
    case CBM_TYPE_TEMPLATE: {
        const char *an = a->data.template_type.template_name;
        const char *bn = b->data.template_type.template_name;
        if (!an || !bn || strcmp(an, bn) != 0) {
            result = 0;
            break;
        }
        int ac = a->data.template_type.arg_count;
        int bcount = b->data.template_type.arg_count;
        if (ac != bcount) {
            result = 0;
            break;
        }
        result = 1;
        for (int i = 0; i < ac; i++) {
            if (!ts_is_assignable_inner(ctx, a->data.template_type.template_args[i],
                                        b->data.template_type.template_args[i], depth + 1, cache)) {
                result = 0;
                break;
            }
        }
        break;
    }
    case CBM_TYPE_FUNC: {
        // Return covariance.
        const CBMType *aret = (a->data.func.return_types && a->data.func.return_types[0])
                                  ? a->data.func.return_types[0]
                                  : NULL;
        const CBMType *bret = (b->data.func.return_types && b->data.func.return_types[0])
                                  ? b->data.func.return_types[0]
                                  : NULL;
        if (aret && bret && !ts_is_assignable_inner(ctx, aret, bret, depth + 1, cache)) {
            result = 0;
            break;
        }
        // Param contravariance + arity tolerance.
        int ap = 0, bp = 0;
        if (a->data.func.param_types)
            while (a->data.func.param_types[ap])
                ap++;
        if (b->data.func.param_types)
            while (b->data.func.param_types[bp])
                bp++;
        if (ap > bp) {
            result = 0;
            break;
        } // source needs more params than target supplies
        result = 1;
        for (int i = 0; i < ap; i++) {
            if (!ts_is_assignable_inner(ctx, b->data.func.param_types[i],
                                        a->data.func.param_types[i], depth + 1, cache)) {
                result = 0;
                break;
            }
        }
        break;
    }
    case CBM_TYPE_TYPE_PARAM: {
        const char *an = a->data.type_param.name;
        const char *bn = b->data.type_param.name;
        result = (an && bn && strcmp(an, bn) == 0) ? 1 : 0;
        break;
    }
    default:
        result = 0;
        break;
    }

done:
    cache[slot].a = a;
    cache[slot].b = b;
    cache[slot].result = (int8_t)result;
    return result;
}

// Public entry point. Allocates a small per-call cache on the stack.
static bool ts_is_assignable(TSLSPContext *ctx, const CBMType *a, const CBMType *b) {
    if (!ctx || !a || !b)
        return false;
    TSRelaterCacheSlot cache[TS_RELATER_CACHE_SIZE];
    for (int i = 0; i < TS_RELATER_CACHE_SIZE; i++) {
        cache[i].a = NULL;
        cache[i].b = NULL;
        cache[i].result = -1;
    }
    return ts_is_assignable_inner(ctx, a, b, 0, cache) ? true : false;
}

// ── Type evaluation ───────────────────────────────────────────────────────────

// `infer X` constraint solver — pattern-matches `source` against `pattern` (which
// may contain CBM_TYPE_INFER nodes) and records bindings into a small fixed-size
// table. Returns true on full match, false otherwise. Bindings table is OWNED by
// caller (typically stack-allocated).
typedef struct {
    const char *name;
    const CBMType *binding;
} TSInferBinding;

#define TS_INFER_MAX 8

static bool match_with_infer(TSLSPContext *ctx, const CBMType *source, const CBMType *pattern,
                             TSInferBinding *binds, int *bind_count, int depth) {
    if (depth > 16 || !source || !pattern)
        return false;

    // Pattern is `infer X`: bind X to source.
    if (pattern->kind == CBM_TYPE_INFER) {
        if (*bind_count >= TS_INFER_MAX)
            return false;
        const char *nm = pattern->data.infer.name;
        if (!nm)
            return false;
        // Re-bind same name → require identical existing binding (else fail).
        for (int i = 0; i < *bind_count; i++) {
            if (binds[i].name && strcmp(binds[i].name, nm) == 0) {
                return ts_is_assignable(ctx, source, binds[i].binding) &&
                       ts_is_assignable(ctx, binds[i].binding, source);
            }
        }
        binds[*bind_count].name = nm;
        binds[*bind_count].binding = source;
        (*bind_count)++;
        return true;
    }

    // Same kind required for structural recursion.
    if (source->kind != pattern->kind) {
        // Non-INFER pattern with different kind: fall back to assignability.
        return ts_is_assignable(ctx, source, pattern);
    }

    switch (pattern->kind) {
    case CBM_TYPE_TEMPLATE: {
        const char *sn = source->data.template_type.template_name;
        const char *pn = pattern->data.template_type.template_name;
        if (!sn || !pn || strcmp(sn, pn) != 0)
            return false;
        int sc = source->data.template_type.arg_count;
        int pc = pattern->data.template_type.arg_count;
        if (sc != pc)
            return false;
        for (int i = 0; i < pc; i++) {
            if (!match_with_infer(ctx, source->data.template_type.template_args[i],
                                  pattern->data.template_type.template_args[i], binds, bind_count,
                                  depth + 1))
                return false;
        }
        return true;
    }
    case CBM_TYPE_TUPLE: {
        int sc = source->data.tuple.count;
        int pc = pattern->data.tuple.count;
        if (sc != pc)
            return false;
        for (int i = 0; i < pc; i++) {
            if (!match_with_infer(ctx, source->data.tuple.elems[i], pattern->data.tuple.elems[i],
                                  binds, bind_count, depth + 1))
                return false;
        }
        return true;
    }
    case CBM_TYPE_FUNC: {
        const CBMType *sret =
            source->data.func.return_types ? source->data.func.return_types[0] : NULL;
        const CBMType *pret =
            pattern->data.func.return_types ? pattern->data.func.return_types[0] : NULL;
        if (sret && pret && !match_with_infer(ctx, sret, pret, binds, bind_count, depth + 1))
            return false;
        // Param-by-param match (positionally).
        int sp = 0, pp = 0;
        if (source->data.func.param_types)
            while (source->data.func.param_types[sp])
                sp++;
        if (pattern->data.func.param_types)
            while (pattern->data.func.param_types[pp])
                pp++;
        if (sp != pp)
            return sp >= pp; // tolerate source having more params than pattern
        for (int i = 0; i < pp; i++) {
            if (!match_with_infer(ctx, source->data.func.param_types[i],
                                  pattern->data.func.param_types[i], binds, bind_count, depth + 1))
                return false;
        }
        return true;
    }
    default:
        return ts_is_assignable(ctx, source, pattern);
    }
}

// Substitute `infer X` bindings into a type tree (recursive).
static const CBMType *subst_infer_bindings(TSLSPContext *ctx, const CBMType *t,
                                           const TSInferBinding *binds, int bind_count, int depth) {
    if (!t || depth > 16 || bind_count == 0)
        return t;
    // TYPE_PARAM matching the bound name → replace with binding's type.
    if (t->kind == CBM_TYPE_TYPE_PARAM && t->data.type_param.name) {
        for (int i = 0; i < bind_count; i++) {
            if (binds[i].name && strcmp(binds[i].name, t->data.type_param.name) == 0) {
                return binds[i].binding;
            }
        }
        return t;
    }
    // NAMED matching the bound name (the parser may module-qualify single-letter
    // names like `U` to `module.U` since it doesn't know about active infer binds).
    // Match on the bare short name.
    if (t->kind == CBM_TYPE_NAMED && t->data.named.qualified_name) {
        const char *qn = t->data.named.qualified_name;
        const char *dot = strrchr(qn, '.');
        const char *bare = dot ? dot + 1 : qn;
        for (int i = 0; i < bind_count; i++) {
            if (binds[i].name && strcmp(binds[i].name, bare) == 0) {
                return binds[i].binding;
            }
        }
        return t;
    }
    if (t->kind == CBM_TYPE_TEMPLATE && t->data.template_type.template_args &&
        t->data.template_type.arg_count > 0) {
        int ac = t->data.template_type.arg_count;
        const CBMType **new_args = (const CBMType **)cbm_arena_alloc(
            ctx->arena, (size_t)(ac + 1) * sizeof(const CBMType *));
        if (!new_args)
            return t;
        for (int i = 0; i < ac; i++) {
            new_args[i] = subst_infer_bindings(ctx, t->data.template_type.template_args[i], binds,
                                               bind_count, depth + 1);
        }
        new_args[ac] = NULL;
        return cbm_type_template(ctx->arena, t->data.template_type.template_name, new_args, ac);
    }
    if (t->kind == CBM_TYPE_TUPLE && t->data.tuple.count > 0) {
        int n = t->data.tuple.count;
        const CBMType **new_elems = (const CBMType **)cbm_arena_alloc(
            ctx->arena, (size_t)(n + 1) * sizeof(const CBMType *));
        if (!new_elems)
            return t;
        for (int i = 0; i < n; i++) {
            new_elems[i] =
                subst_infer_bindings(ctx, t->data.tuple.elems[i], binds, bind_count, depth + 1);
        }
        new_elems[n] = NULL;
        return cbm_type_tuple(ctx->arena, new_elems, n);
    }
    return t;
}

// Walk a type tree to detect any CBM_TYPE_INFER nodes.
static bool contains_infer(const CBMType *t, int depth) {
    if (!t || depth > 32)
        return false;
    if (t->kind == CBM_TYPE_INFER)
        return true;
    if (t->kind == CBM_TYPE_TEMPLATE && t->data.template_type.template_args) {
        for (int i = 0; i < t->data.template_type.arg_count; i++) {
            if (contains_infer(t->data.template_type.template_args[i], depth + 1))
                return true;
        }
    }
    if (t->kind == CBM_TYPE_TUPLE && t->data.tuple.elems) {
        for (int i = 0; i < t->data.tuple.count; i++) {
            if (contains_infer(t->data.tuple.elems[i], depth + 1))
                return true;
        }
    }
    if (t->kind == CBM_TYPE_FUNC) {
        if (t->data.func.return_types) {
            for (int i = 0; t->data.func.return_types[i]; i++) {
                if (contains_infer(t->data.func.return_types[i], depth + 1))
                    return true;
            }
        }
        if (t->data.func.param_types) {
            for (int i = 0; t->data.func.param_types[i]; i++) {
                if (contains_infer(t->data.func.param_types[i], depth + 1))
                    return true;
            }
        }
    }
    return false;
}

// Evaluate a CBM_TYPE_CONDITIONAL `T extends U ? X : Y`. With distribution: when T is
// a UNION, evaluate per member and union the results. Uses the partial relater for
// the extends check.
static const CBMType *eval_conditional(TSLSPContext *ctx, const CBMType *t, int depth) {
    if (!t || t->kind != CBM_TYPE_CONDITIONAL || depth > 16)
        return t;
    const CBMType *check = t->data.conditional.check;
    const CBMType *extends = t->data.conditional.extends;
    const CBMType *tb = t->data.conditional.true_branch;
    const CBMType *fb = t->data.conditional.false_branch;
    if (!check || !extends)
        return t;

    // Distribution: when check is a UNION, evaluate per member and union results.
    if (check->kind == CBM_TYPE_UNION && check->data.union_type.members &&
        check->data.union_type.count > 0) {
        const CBMType **results = (const CBMType **)cbm_arena_alloc(
            ctx->arena, (size_t)(check->data.union_type.count + 1) * sizeof(const CBMType *));
        if (!results)
            return t;
        int n = 0;
        for (int i = 0; i < check->data.union_type.count; i++) {
            const CBMType *member = check->data.union_type.members[i];
            const CBMType *sub = cbm_type_conditional(ctx->arena, member, extends, tb, fb);
            results[n++] = eval_conditional(ctx, sub, depth + 1);
        }
        results[n] = NULL;
        if (n == 1)
            return results[0];
        return cbm_type_union(ctx->arena, results, n);
    }

    // `infer X` in extends pattern: pattern-match check against extends, capturing
    // bindings, then substitute them in the true_branch.
    if (contains_infer(extends, 0)) {
        TSInferBinding binds[TS_INFER_MAX] = {{0}};
        int bind_count = 0;
        if (match_with_infer(ctx, check, extends, binds, &bind_count, 0)) {
            return subst_infer_bindings(ctx, tb, binds, bind_count, 0);
        }
        return fb;
    }

    // Non-distributive: single resolution.
    bool yes = ts_is_assignable(ctx, check, extends);
    return yes ? tb : fb;
}

// Resolve a CBMType against the registry's alias chain when it's a NAMED type that's a
// pure alias (`type Foo = Bar`). Also evaluates inline conditional types.
static const CBMType *simplify_type(TSLSPContext *ctx, const CBMType *t) {
    if (!t || !ctx || !ctx->registry)
        return t;
    if (t->kind == CBM_TYPE_CONDITIONAL) {
        const CBMType *evaluated = eval_conditional(ctx, t, 0);
        if (evaluated && evaluated != t)
            return simplify_type(ctx, evaluated);
        return t;
    }
    if (t->kind == CBM_TYPE_ALIAS)
        return cbm_type_resolve_alias(t);
    if (t->kind != CBM_TYPE_NAMED)
        return t;
    for (int i = 0; i < 16; i++) {
        if (!t || t->kind != CBM_TYPE_NAMED)
            return t;
        const CBMRegisteredType *rt =
            cbm_registry_lookup_type(ctx->registry, t->data.named.qualified_name);
        if (!rt || !rt->alias_of)
            return t;
        t = cbm_type_named(ctx->arena, rt->alias_of);
    }
    return t;
}

static const CBMType *type_of_identifier(TSLSPContext *ctx, const char *name) {
    if (!ctx || !name)
        return cbm_type_unknown();

    // Scope lookup (params, locals).
    const CBMType *t = cbm_scope_lookup(ctx->current_scope, name);
    if (t && !cbm_type_is_unknown(t))
        return t;

    // Imports: bare identifier matching an import binding. Compute the full symbol QN
    // and look it up in the registry. If it's a registered func, return its signature;
    // if it's a registered type, return NAMED. Falls back to NAMED(module_qn) for
    // namespace-style imports where no specific symbol matches.
    for (int i = 0; i < ctx->import_count; i++) {
        const char *local = ctx->import_local_names ? ctx->import_local_names[i] : NULL;
        const char *mqn = ctx->import_module_qns ? ctx->import_module_qns[i] : NULL;
        if (!local || !mqn || strcmp(local, name) != 0)
            continue;

        // Construct the full symbol QN: if mqn already ends in ".name" use it; else
        // append ".name". This matches `resolve_type_with_imports`' convention.
        size_t mqn_len = strlen(mqn);
        size_t name_len = strlen(name);
        const char *full_qn;
        if (mqn_len > name_len + 1 && mqn[mqn_len - name_len - 1] == '.' &&
            strcmp(mqn + mqn_len - name_len, name) == 0) {
            full_qn = mqn;
        } else {
            full_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", mqn, name);
        }

        // Try the symbol lookup precedence: registered func, then registered type.
        const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, full_qn);
        if (f && f->signature)
            return f->signature;
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, full_qn);
        if (rt)
            return cbm_type_named(ctx->arena, full_qn);

        // No specific match — fall back to NAMED(module_qn), which is correct for
        // namespace-style imports (`import * as foo from './foo'`).
        return cbm_type_named(ctx->arena, mqn);
    }

    // Module-local function or class.
    if (ctx->module_qn) {
        const char *candidate = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name);
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, candidate);
        if (rt)
            return cbm_type_named(ctx->arena, candidate);
        const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, candidate);
        if (f && f->signature)
            return f->signature;
    }

    // Global / stdlib bare-name lookup (Promise, Array, Map, Set, console, JSON, Object, ...).
    // The TS stdlib registers these with QN equal to the bare name.
    {
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, name);
        if (rt)
            return cbm_type_named(ctx->arena, name);
        const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, name);
        if (f && f->signature)
            return f->signature;
    }

    return cbm_type_unknown();
}

// Map BUILTIN type names to their wrapper class for method dispatch.
// `"x".split(...)` works because TS treats string primitives as having String prototype.
static const char *builtin_wrapper_class(const char *builtin_name) {
    if (!builtin_name)
        return NULL;
    if (strcmp(builtin_name, "string") == 0)
        return "String";
    if (strcmp(builtin_name, "number") == 0)
        return "Number";
    if (strcmp(builtin_name, "boolean") == 0)
        return "Boolean";
    if (strcmp(builtin_name, "bigint") == 0)
        return "BigInt";
    if (strcmp(builtin_name, "symbol") == 0)
        return "Symbol";
    return NULL;
}

// Look up a property `name` on a receiver type. Returns the property type or UNKNOWN.
static const CBMType *lookup_member_type_inner(TSLSPContext *ctx, const CBMType *recv,
                                               const char *name);

#define TS_LSP_MAX_MEMBER_DEPTH 64

/* Depth-guarded entry: member lookup recurses through wrapper classes, union
 * members and registered-type expansion; cyclic type graphs in real-world TS
 * (microsoft/TypeScript reallyLargeFile.ts) recursed without bound — SIGBUS
 * stack overflow under endless lookup_member_type frames. Past the cap the
 * member resolves as unknown — graceful degradation, not a crash. */
static const CBMType *lookup_member_type(TSLSPContext *ctx, const CBMType *recv, const char *name) {
    if (!ctx || ctx->member_depth >= TS_LSP_MAX_MEMBER_DEPTH)
        return cbm_type_unknown();
    ctx->member_depth++;
    const CBMType *r = lookup_member_type_inner(ctx, recv, name);
    ctx->member_depth--;
    return r;
}

static const CBMType *lookup_member_type_inner(TSLSPContext *ctx, const CBMType *recv,
                                               const char *name) {
    if (!ctx || !recv || !name)
        return cbm_type_unknown();
    const CBMType *base = simplify_type(ctx, recv);
    if (!base)
        return cbm_type_unknown();
    base = unwrap_passthrough_template(base);
    if (!base)
        return cbm_type_unknown();

    // BUILTIN primitives delegate to their wrapper class (e.g. string → String).
    if (base->kind == CBM_TYPE_BUILTIN) {
        const char *wrap = builtin_wrapper_class(base->data.builtin.name);
        if (wrap) {
            const CBMType *wrapped = cbm_type_named(ctx->arena, wrap);
            return lookup_member_type(ctx, wrapped, name);
        }
        return cbm_type_unknown();
    }

    if (base->kind == CBM_TYPE_TEMPLATE) {
        // Look up on the template name (e.g., "Array", "Promise"); generic params are
        // applied via cbm_type_substitute when we know the registered type's params.
        const CBMRegisteredType *rt =
            cbm_registry_lookup_type(ctx->registry, base->data.template_type.template_name);
        if (rt) {
            // Field?
            if (rt->field_names && rt->field_types) {
                for (int i = 0; rt->field_names[i]; i++) {
                    if (strcmp(rt->field_names[i], name) == 0) {
                        return cbm_type_substitute(ctx->arena, rt->field_types[i],
                                                   rt->type_param_names,
                                                   base->data.template_type.template_args);
                    }
                }
            }
            // Method?
            if (rt->method_qns && rt->method_names) {
                for (int i = 0; rt->method_names[i]; i++) {
                    if (strcmp(rt->method_names[i], name) == 0) {
                        const CBMRegisteredFunc *f =
                            cbm_registry_lookup_func(ctx->registry, rt->method_qns[i]);
                        if (f && f->signature) {
                            return cbm_type_substitute(ctx->arena, f->signature,
                                                       rt->type_param_names,
                                                       base->data.template_type.template_args);
                        }
                    }
                }
            }
        }
        return cbm_type_unknown();
    }

    if (base->kind == CBM_TYPE_OBJECT_LIT) {
        if (base->data.object_lit.prop_names && base->data.object_lit.prop_types) {
            for (int i = 0; base->data.object_lit.prop_names[i]; i++) {
                if (strcmp(base->data.object_lit.prop_names[i], name) == 0) {
                    return base->data.object_lit.prop_types[i];
                }
            }
        }
        return cbm_type_unknown();
    }

    if (base->kind == CBM_TYPE_UNION) {
        // Try each branch; if any has the member, return that. (Pragmatic v1 — returns
        // first hit instead of building a union of results.)
        if (base->data.union_type.members) {
            for (int i = 0; i < base->data.union_type.count; i++) {
                const CBMType *m = lookup_member_type(ctx, base->data.union_type.members[i], name);
                if (!cbm_type_is_unknown(m))
                    return m;
            }
        }
        return cbm_type_unknown();
    }

    if (base->kind == CBM_TYPE_INTERSECTION) {
        if (base->data.union_type.members) {
            for (int i = 0; i < base->data.union_type.count; i++) {
                const CBMType *m = lookup_member_type(ctx, base->data.union_type.members[i], name);
                if (!cbm_type_is_unknown(m))
                    return m;
            }
        }
        return cbm_type_unknown();
    }

    if (base->kind != CBM_TYPE_NAMED)
        return cbm_type_unknown();

    const char *recv_qn = base->data.named.qualified_name;
    const CBMRegisteredType *rt = cbm_registry_resolve_alias(ctx->registry, recv_qn);
    if (!rt) {
        rt = cbm_registry_lookup_type(ctx->registry, recv_qn);
        if (!rt)
            return cbm_type_unknown();
    }

    // Direct field lookup.
    if (rt->field_names && rt->field_types) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (strcmp(rt->field_names[i], name) == 0)
                return rt->field_types[i];
        }
    }
    // Method lookup → return the method's FUNC type.
    if (rt->method_qns && rt->method_names) {
        for (int i = 0; rt->method_names[i]; i++) {
            if (strcmp(rt->method_names[i], name) == 0) {
                const CBMRegisteredFunc *f =
                    cbm_registry_lookup_func(ctx->registry, rt->method_qns[i]);
                if (f && f->signature)
                    return f->signature;
                return cbm_type_unknown();
            }
        }
    }
    // Walk extends/implements.
    if (rt->embedded_types) {
        for (int i = 0; rt->embedded_types[i]; i++) {
            const CBMType *parent = cbm_type_named(ctx->arena, rt->embedded_types[i]);
            const CBMType *m = lookup_member_type(ctx, parent, name);
            if (!cbm_type_is_unknown(m))
                return m;
        }
    }
    return cbm_type_unknown();
}

// TS utility types like Partial<T>, Readonly<T>, Required<T> are essentially
// transformations on T's properties — for call-edge resolution they pass through
// to T's own method/property surface. NonNullable<T> also unwraps to T.
static const CBMType *unwrap_passthrough_template(const CBMType *t) {
    if (!t || t->kind != CBM_TYPE_TEMPLATE)
        return t;
    const char *name = t->data.template_type.template_name;
    if (!name || !t->data.template_type.template_args || t->data.template_type.arg_count < 1)
        return t;
    // Single-arg utility types pass through to T's surface for method dispatch.
    if (strcmp(name, "Partial") == 0 || strcmp(name, "Required") == 0 ||
        strcmp(name, "Readonly") == 0 || strcmp(name, "NonNullable") == 0 ||
        strcmp(name, "Pick") == 0 || strcmp(name, "Omit") == 0 || strcmp(name, "Awaited") == 0) {
        return t->data.template_type.template_args[0];
    }
    // ReturnType<F>: when F is FUNC, surface F's return type for method dispatch.
    if (strcmp(name, "ReturnType") == 0) {
        const CBMType *f = t->data.template_type.template_args[0];
        if (f && f->kind == CBM_TYPE_FUNC && f->data.func.return_types &&
            f->data.func.return_types[0]) {
            return f->data.func.return_types[0];
        }
        return t;
    }
    // Exclude<T, U> / Extract<T, U>: T is a union; for method dispatch we treat the
    // result as T itself (any member's common methods still resolve via union dispatch).
    if (strcmp(name, "Exclude") == 0 || strcmp(name, "Extract") == 0) {
        return t->data.template_type.template_args[0];
    }
    // Promise<T> intentionally NOT unwrapped — methods (then/catch/finally) live on
    // the Promise registration. await-expression unwraps via ts_eval_expr_type.
    return t;
}

// Build a UNION of LITERAL string types representing keyof T, where T is a registered
// class/interface. Returns UNKNOWN if T has no fields or isn't registered.
static const CBMType *eval_keyof(TSLSPContext *ctx, const CBMType *operand) {
    if (!ctx || !operand)
        return cbm_type_unknown();
    const CBMType *base = simplify_type(ctx, operand);
    if (!base || base->kind != CBM_TYPE_NAMED)
        return cbm_type_unknown();
    const CBMRegisteredType *rt =
        cbm_registry_lookup_type(ctx->registry, base->data.named.qualified_name);
    if (!rt || !rt->field_names || !rt->field_names[0])
        return cbm_type_unknown();
    int fc = 0;
    while (rt->field_names[fc])
        fc++;
    const CBMType *members[64];
    int mc = 0;
    for (int i = 0; i < fc && mc < 63; i++) {
        members[mc++] = cbm_type_ts_literal(ctx->arena, "string", rt->field_names[i]);
    }
    members[mc] = NULL;
    if (mc == 0)
        return cbm_type_unknown();
    if (mc == 1)
        return members[0];
    return cbm_type_union(ctx->arena, members, mc);
}

// Evaluate T[K]: when K is a string literal, look up that field on T. When K is a
// type parameter (or `keyof T` itself), return UNKNOWN — pragmatic v1.
static const CBMType *eval_indexed_access(TSLSPContext *ctx, const CBMType *obj,
                                          const CBMType *key) {
    if (!ctx || !obj || !key)
        return cbm_type_unknown();
    if (key->kind == CBM_TYPE_TS_LITERAL && key->data.literal_ts.value &&
        key->data.literal_ts.tag && strcmp(key->data.literal_ts.tag, "string") == 0) {
        return lookup_member_type(ctx, obj, key->data.literal_ts.value);
    }
    return cbm_type_unknown();
}

// Look up a method on a receiver type — returns the registered func.
static const CBMRegisteredFunc *lookup_method(TSLSPContext *ctx, const CBMType *recv,
                                              const char *method_name) {
    if (!ctx || !recv || !method_name)
        return NULL;
    const CBMType *base = simplify_type(ctx, recv);
    if (!base)
        return NULL;
    base = unwrap_passthrough_template(base);
    if (!base)
        return NULL;

    // BUILTIN primitives → look up via the wrapper class (string → String, etc.).
    if (base->kind == CBM_TYPE_BUILTIN) {
        const char *wrap = builtin_wrapper_class(base->data.builtin.name);
        if (wrap) {
            const CBMType *wrapped = cbm_type_named(ctx->arena, wrap);
            return lookup_method(ctx, wrapped, method_name);
        }
        return NULL;
    }

    if (ctx->debug) {
        const char *base_qn = (base->kind == CBM_TYPE_NAMED) ? base->data.named.qualified_name
                              : (base->kind == CBM_TYPE_TEMPLATE)
                                  ? base->data.template_type.template_name
                                  : "<other>";
        fprintf(stderr, "[ts_lsp] lookup_method: recv_kind=%d recv_qn=%s method=%s\n",
                (int)base->kind, base_qn ? base_qn : "(null)", method_name);
    }

    if (base->kind == CBM_TYPE_TEMPLATE) {
        const CBMRegisteredType *rt =
            cbm_registry_lookup_type(ctx->registry, base->data.template_type.template_name);
        if (rt && rt->method_qns && rt->method_names) {
            for (int i = 0; rt->method_names[i]; i++) {
                if (strcmp(rt->method_names[i], method_name) == 0) {
                    return cbm_registry_lookup_func(ctx->registry, rt->method_qns[i]);
                }
            }
        }
    }

    if (base->kind == CBM_TYPE_UNION || base->kind == CBM_TYPE_INTERSECTION) {
        if (base->data.union_type.members) {
            for (int i = 0; i < base->data.union_type.count; i++) {
                const CBMRegisteredFunc *f =
                    lookup_method(ctx, base->data.union_type.members[i], method_name);
                if (f)
                    return f;
            }
        }
        return NULL;
    }

    if (base->kind != CBM_TYPE_NAMED)
        return NULL;
    const char *recv_qn = base->data.named.qualified_name;

    const CBMRegisteredFunc *f =
        cbm_registry_lookup_method_aliased(ctx->registry, recv_qn, method_name);
    if (f)
        return f;

    // Walk extends/implements via the registered type.
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, recv_qn);
    if (rt && rt->embedded_types) {
        for (int i = 0; rt->embedded_types[i]; i++) {
            const CBMType *parent = cbm_type_named(ctx->arena, rt->embedded_types[i]);
            const CBMRegisteredFunc *pf = lookup_method(ctx, parent, method_name);
            if (pf)
                return pf;
        }
    }
    return NULL;
}

// Pull the underlying return type out of a FUNC signature, collapsing single-element
// return arrays to that element.
static const CBMType *return_type_of(CBMArena *arena, const CBMType *sig) {
    if (!sig || sig->kind != CBM_TYPE_FUNC)
        return cbm_type_unknown();
    if (!sig->data.func.return_types || !sig->data.func.return_types[0])
        return cbm_type_unknown();
    if (!sig->data.func.return_types[1])
        return sig->data.func.return_types[0];
    int count = 0;
    while (sig->data.func.return_types[count])
        count++;
    // Multi-return → build a tuple. Needs a real arena: passing NULL here
    // crashed in cbm_arena_alloc when the cross-resolve path evaluated a
    // multi-return signature (only reachable once the per-file LSP O(n²) was
    // fixed and the resolve phase actually ran).
    return cbm_type_tuple(arena, sig->data.func.return_types, count);
}

const CBMType *ts_eval_expr_type(TSLSPContext *ctx, TSNode node) {
    if (!ctx || ts_node_is_null(node))
        return cbm_type_unknown();
    if (ctx->eval_depth > TS_LSP_MAX_EVAL_DEPTH)
        return cbm_type_unknown();
    ctx->eval_depth++;

    const CBMType *result = cbm_type_unknown();
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "identifier") == 0 || strcmp(kind, "shorthand_property_identifier") == 0) {
        char *name = node_text(ctx, node);
        if (name)
            result = type_of_identifier(ctx, name);
    } else if (strcmp(kind, "this") == 0) {
        if (ctx->enclosing_class_qn) {
            result = cbm_type_named(ctx->arena, ctx->enclosing_class_qn);
        }
    } else if (strcmp(kind, "super") == 0) {
        if (ctx->enclosing_class_qn) {
            const CBMRegisteredType *rt =
                cbm_registry_lookup_type(ctx->registry, ctx->enclosing_class_qn);
            if (rt && rt->embedded_types && rt->embedded_types[0]) {
                result = cbm_type_named(ctx->arena, rt->embedded_types[0]);
            }
        }
    } else if (strcmp(kind, "parenthesized_expression") == 0) {
        TSNode inner = ts_node_named_child(node, 0);
        result = ts_eval_expr_type(ctx, inner);
    } else if (strcmp(kind, "non_null_expression") == 0) {
        // `expr!` — strip the assertion, type unchanged.
        TSNode inner = ts_node_named_child(node, 0);
        result = ts_eval_expr_type(ctx, inner);
    } else if (strcmp(kind, "as_expression") == 0 || strcmp(kind, "satisfies_expression") == 0 ||
               strcmp(kind, "type_assertion") == 0) {
        // `expr as T` / `<T>expr` / `expr satisfies T`: type-position field gives the type.
        // For satisfies, we keep the asserted-type (TS narrows it but for resolution that's fine).
        TSNode type_node = ts_node_child_by_field_name(node, "type", TS_LSP_FIELD_LEN("type"));
        if (!ts_node_is_null(type_node)) {
            char *text = node_text(ctx, type_node);
            result = parse_ts_type_text(ctx->arena, text, ctx->module_qn);
        } else {
            // Last named child is the type for older grammars.
            uint32_t nc = ts_node_named_child_count(node);
            if (nc >= 2) {
                TSNode tn = ts_node_named_child(node, nc - 1);
                char *text = node_text(ctx, tn);
                result = parse_ts_type_text(ctx->arena, text, ctx->module_qn);
            }
        }
    } else if (strcmp(kind, "member_expression") == 0 ||
               strcmp(kind, "subscript_expression") == 0) {
        TSNode obj = ts_node_child_by_field_name(node, "object", TS_LSP_FIELD_LEN("object"));
        TSNode prop = ts_node_child_by_field_name(node, "property", TS_LSP_FIELD_LEN("property"));
        if (!ts_node_is_null(obj) && !ts_node_is_null(prop)) {
            const CBMType *recv = ts_eval_expr_type(ctx, obj);
            char *pname = node_text(ctx, prop);
            if (pname)
                result = lookup_member_type(ctx, recv, pname);
        }
    } else if (strcmp(kind, "new_expression") == 0) {
        TSNode ctor =
            ts_node_child_by_field_name(node, "constructor", TS_LSP_FIELD_LEN("constructor"));
        if (!ts_node_is_null(ctor)) {
            char *cname = node_text(ctx, ctor);
            if (cname) {
                // Bare class name → qualify against module.
                if (strchr(cname, '.') == NULL && ctx->module_qn) {
                    const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cname);
                    result = cbm_type_named(ctx->arena, qn);
                } else {
                    result = cbm_type_named(ctx->arena, cname);
                }
            }
        }
    } else if (strcmp(kind, "call_expression") == 0) {
        TSNode fn = ts_node_child_by_field_name(node, "function", TS_LSP_FIELD_LEN("function"));
        if (!ts_node_is_null(fn)) {
            const CBMType *fn_type = ts_eval_expr_type(ctx, fn);
            if (ctx->debug) {
                fprintf(stderr, "[ts_lsp] eval call_expression: fn_kind=%s fn_type_kind=%d\n",
                        ts_node_type(fn), fn_type ? (int)fn_type->kind : -1);
            }
            if (fn_type && fn_type->kind == CBM_TYPE_FUNC) {
                result = return_type_of(ctx->arena, fn_type);

                // Polymorphic `this` return type: when the registered signature uses
                // `this` (TYPE_PARAM "this"), substitute the actual receiver type
                // from the call site (fn is a member_expression — its `object` is
                // the receiver). Powers fluent-builder patterns.
                if (result && result->kind == CBM_TYPE_TYPE_PARAM && result->data.type_param.name &&
                    strcmp(result->data.type_param.name, "this") == 0 &&
                    strcmp(ts_node_type(fn), "member_expression") == 0) {
                    TSNode obj =
                        ts_node_child_by_field_name(fn, "object", TS_LSP_FIELD_LEN("object"));
                    if (!ts_node_is_null(obj)) {
                        const CBMType *recv = ts_eval_expr_type(ctx, obj);
                        if (recv && !cbm_type_is_unknown(recv))
                            result = recv;
                    }
                }

                // Generic inference at call site: if the function has type parameters
                // referenced in return / param types, infer T from concrete arg types
                // and substitute. This is the simplest form of typescript-go's
                // inferTypeArguments — single-pass, argument-driven.
                if (result && result->kind == CBM_TYPE_TYPE_PARAM &&
                    fn_type->data.func.param_types) {
                    TSNode args = ts_node_child_by_field_name(node, "arguments",
                                                              TS_LSP_FIELD_LEN("arguments"));
                    if (!ts_node_is_null(args)) {
                        // Build a list of inferred type-param names + arg types.
                        const char *inf_names[8] = {0};
                        const CBMType *inf_args[8] = {0};
                        int ic = 0;
                        uint32_t pc = 0;
                        while (fn_type->data.func.param_types[pc])
                            pc++;
                        uint32_t argc = ts_node_named_child_count(args);
                        for (uint32_t i = 0; i < pc && i < argc && ic < 7; i++) {
                            const CBMType *pt = fn_type->data.func.param_types[i];
                            if (!pt || pt->kind != CBM_TYPE_TYPE_PARAM)
                                continue;
                            TSNode arg = ts_node_named_child(args, i);
                            if (ts_node_is_null(arg))
                                continue;
                            const CBMType *at = ts_eval_expr_type(ctx, arg);
                            if (cbm_type_is_unknown(at))
                                continue;
                            inf_names[ic] = pt->data.type_param.name;
                            inf_args[ic] = at;
                            ic++;
                        }
                        if (ic > 0) {
                            inf_names[ic] = NULL;
                            inf_args[ic] = NULL;
                            const CBMType *sub =
                                cbm_type_substitute(ctx->arena, result, inf_names, inf_args);
                            if (sub && !cbm_type_is_unknown(sub))
                                result = sub;
                        }
                    }
                }

                if (ctx->debug) {
                    const char *res_qn = (result && result->kind == CBM_TYPE_NAMED)
                                             ? result->data.named.qualified_name
                                             : "<other>";
                    fprintf(stderr, "[ts_lsp]   returns kind=%d qn=%s\n",
                            result ? (int)result->kind : -1, res_qn ? res_qn : "(null)");
                }
            }
        }
    } else if (strcmp(kind, "await_expression") == 0) {
        TSNode inner = ts_node_named_child(node, 0);
        const CBMType *t = ts_eval_expr_type(ctx, inner);
        // Promise<T> → T.
        if (t && t->kind == CBM_TYPE_TEMPLATE && t->data.template_type.template_name &&
            strcmp(t->data.template_type.template_name, "Promise") == 0 &&
            t->data.template_type.arg_count >= 1 && t->data.template_type.template_args) {
            result = t->data.template_type.template_args[0];
        } else {
            result = t;
        }
    } else if (strcmp(kind, "string") == 0 || strcmp(kind, "template_string") == 0) {
        result = cbm_type_builtin(ctx->arena, "string");
    } else if (strcmp(kind, "number") == 0) {
        result = cbm_type_builtin(ctx->arena, "number");
    } else if (strcmp(kind, "true") == 0 || strcmp(kind, "false") == 0) {
        result = cbm_type_builtin(ctx->arena, "boolean");
    } else if (strcmp(kind, "null") == 0) {
        result = cbm_type_builtin(ctx->arena, "null");
    } else if (strcmp(kind, "undefined") == 0) {
        result = cbm_type_builtin(ctx->arena, "undefined");
    } else if (strcmp(kind, "array") == 0) {
        // Use first element's type as Array<T> argument.
        const CBMType *elem = cbm_type_unknown();
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(node, i);
            if (!ts_node_is_null(c)) {
                elem = ts_eval_expr_type(ctx, c);
                if (!cbm_type_is_unknown(elem))
                    break;
            }
        }
        const CBMType *args[2] = {elem, NULL};
        result = cbm_type_template(ctx->arena, "Array", args, 1);
    } else if (strcmp(kind, "object") == 0) {
        // Object literal — capture string-keyed property types.
        uint32_t nc = ts_node_named_child_count(node);
        const char *names[32] = {0};
        const CBMType *types[32] = {0};
        int count = 0;
        for (uint32_t i = 0; i < nc && count < 31; i++) {
            TSNode c = ts_node_named_child(node, i);
            if (ts_node_is_null(c))
                continue;
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "pair") == 0) {
                TSNode key = ts_node_child_by_field_name(c, "key", TS_LSP_FIELD_LEN("key"));
                TSNode val = ts_node_child_by_field_name(c, "value", TS_LSP_FIELD_LEN("value"));
                if (!ts_node_is_null(key) && !ts_node_is_null(val)) {
                    char *k = node_text(ctx, key);
                    if (k) {
                        // Strip surrounding quotes if string key.
                        size_t kl = strlen(k);
                        if (kl >= 2 && (k[0] == '"' || k[0] == '\'') && k[kl - 1] == k[0]) {
                            k[kl - 1] = '\0';
                            k++;
                        }
                        names[count] = k;
                        types[count] = ts_eval_expr_type(ctx, val);
                        count++;
                    }
                }
            } else if (strcmp(ck, "shorthand_property_identifier") == 0) {
                char *k = node_text(ctx, c);
                if (k) {
                    names[count] = k;
                    types[count] = type_of_identifier(ctx, k);
                    count++;
                }
            }
        }
        names[count] = NULL;
        types[count] = NULL;
        result = cbm_type_object_lit(ctx->arena, names, types, NULL, NULL);
    } else if (strcmp(kind, "ternary_expression") == 0) {
        TSNode cons =
            ts_node_child_by_field_name(node, "consequence", TS_LSP_FIELD_LEN("consequence"));
        TSNode alt =
            ts_node_child_by_field_name(node, "alternative", TS_LSP_FIELD_LEN("alternative"));
        const CBMType *a = ts_eval_expr_type(ctx, cons);
        const CBMType *b = ts_eval_expr_type(ctx, alt);
        if (cbm_type_is_unknown(a))
            result = b;
        else if (cbm_type_is_unknown(b))
            result = a;
        else if (a == b)
            result = a;
        else {
            const CBMType *members[3] = {a, b, NULL};
            result = cbm_type_union(ctx->arena, members, 2);
        }
    } else if (strcmp(kind, "binary_expression") == 0) {
        // String concatenation `a + b` where either side is string → string.
        TSNode left = ts_node_child_by_field_name(node, "left", TS_LSP_FIELD_LEN("left"));
        TSNode right = ts_node_child_by_field_name(node, "right", TS_LSP_FIELD_LEN("right"));
        if (!ts_node_is_null(left) && !ts_node_is_null(right)) {
            const CBMType *l = ts_eval_expr_type(ctx, left);
            const CBMType *r = ts_eval_expr_type(ctx, right);
            if ((l && l->kind == CBM_TYPE_BUILTIN && l->data.builtin.name &&
                 strcmp(l->data.builtin.name, "string") == 0) ||
                (r && r->kind == CBM_TYPE_BUILTIN && r->data.builtin.name &&
                 strcmp(r->data.builtin.name, "string") == 0)) {
                result = cbm_type_builtin(ctx->arena, "string");
            } else {
                result = cbm_type_builtin(ctx->arena, "number");
            }
        }
    }

    ctx->eval_depth--;
    return result;
}

// Walk a parsed type and replace NAMED references whose bare name matches a known
// import binding. Recurses through TEMPLATE / UNION / INTERSECTION compound kinds.
//
// Naming convention: ctx->import_module_qns[i] is the imported module's QN (e.g.
// "test.conn"); to get the full type QN, we append the local-binding name. Falls back
// to the original type if no match.
static const CBMType *resolve_type_with_imports(TSLSPContext *ctx, const CBMType *t) {
    if (!t || !ctx)
        return t;

    if (t->kind == CBM_TYPE_NAMED && t->data.named.qualified_name) {
        const char *qn = t->data.named.qualified_name;
        const char *dot = strrchr(qn, '.');
        const char *bare = dot ? dot + 1 : qn;

        // TS semantics: locally-declared types shadow ambient/stdlib types with the
        // same name. parse_ts_type_text leaves stdlib names bare to match the stdlib
        // registration QN; if the local module has a type with the same short name
        // (e.g. test files declaring their own `Response`/`File`/etc.), prefer that.
        if (!dot && ctx->module_qn) {
            const char *local_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, bare);
            const CBMRegisteredType *local_rt = cbm_registry_lookup_type(ctx->registry, local_qn);
            if (local_rt)
                return cbm_type_named(ctx->arena, local_qn);
        }

        for (int i = 0; i < ctx->import_count; i++) {
            const char *lname = ctx->import_local_names ? ctx->import_local_names[i] : NULL;
            const char *mqn = ctx->import_module_qns ? ctx->import_module_qns[i] : NULL;
            if (!lname || !mqn)
                continue;
            if (strcmp(lname, bare) != 0)
                continue;
            // Heuristic: if mqn already ends in ".bare" use as-is; otherwise append.
            size_t mqn_len = strlen(mqn);
            size_t bare_len = strlen(bare);
            if (mqn_len > bare_len + 1 && mqn[mqn_len - bare_len - 1] == '.' &&
                strcmp(mqn + mqn_len - bare_len, bare) == 0) {
                return cbm_type_named(ctx->arena, mqn);
            }
            return cbm_type_named(ctx->arena, cbm_arena_sprintf(ctx->arena, "%s.%s", mqn, bare));
        }
        return t;
    }
    if (t->kind == CBM_TYPE_TEMPLATE && t->data.template_type.template_args &&
        t->data.template_type.arg_count > 0) {
        int ac = t->data.template_type.arg_count;
        const CBMType **new_args = (const CBMType **)cbm_arena_alloc(
            ctx->arena, (size_t)(ac + 1) * sizeof(const CBMType *));
        if (!new_args)
            return t;
        for (int i = 0; i < ac; i++) {
            new_args[i] = resolve_type_with_imports(ctx, t->data.template_type.template_args[i]);
        }
        new_args[ac] = NULL;
        return cbm_type_template(ctx->arena, t->data.template_type.template_name, new_args, ac);
    }
    if (t->kind == CBM_TYPE_UNION || t->kind == CBM_TYPE_INTERSECTION) {
        if (!t->data.union_type.members || t->data.union_type.count == 0)
            return t;
        int mc = t->data.union_type.count;
        const CBMType **new_members = (const CBMType **)cbm_arena_alloc(
            ctx->arena, (size_t)(mc + 1) * sizeof(const CBMType *));
        if (!new_members)
            return t;
        for (int i = 0; i < mc; i++) {
            new_members[i] = resolve_type_with_imports(ctx, t->data.union_type.members[i]);
        }
        new_members[mc] = NULL;
        return t->kind == CBM_TYPE_UNION ? cbm_type_union(ctx->arena, new_members, mc)
                                         : cbm_type_intersection(ctx->arena, new_members, mc);
    }
    return t;
}

const CBMType *ts_parse_type_node(TSLSPContext *ctx, TSNode node) {
    if (!ctx || ts_node_is_null(node))
        return cbm_type_unknown();
    char *text = node_text(ctx, node);
    const CBMType *parsed = parse_ts_type_text(ctx->arena, text, ctx->module_qn);
    return resolve_type_with_imports(ctx, parsed);
}

// ── Statement processing ──────────────────────────────────────────────────────

// Bind a variable_declarator: `let x: T = init` / `const x = init` / `var x = ...`.
static void bind_variable_declarator(TSLSPContext *ctx, TSNode decl) {
    TSNode name = ts_node_child_by_field_name(decl, "name", TS_LSP_FIELD_LEN("name"));
    TSNode tann = ts_node_child_by_field_name(decl, "type", TS_LSP_FIELD_LEN("type"));
    TSNode value = ts_node_child_by_field_name(decl, "value", TS_LSP_FIELD_LEN("value"));
    if (ts_node_is_null(name))
        return;

    const CBMType *declared = NULL;
    if (!ts_node_is_null(tann)) {
        // type_annotation contains the actual type as a named child.
        TSNode tch = ts_node_named_child(tann, 0);
        if (!ts_node_is_null(tch))
            declared = ts_parse_type_node(ctx, tch);
    }
    const CBMType *inferred =
        ts_node_is_null(value) ? cbm_type_unknown() : ts_eval_expr_type(ctx, value);
    const CBMType *bound = (declared && !cbm_type_is_unknown(declared)) ? declared : inferred;

    if (node_kind_is(name, "identifier")) {
        char *nm = node_text(ctx, name);
        if (nm)
            cbm_scope_bind(ctx->current_scope, nm, bound);
    } else if (node_kind_is(name, "object_pattern")) {
        // Destructure object: `const { a, b } = obj` — bind a and b to obj.a and obj.b types.
        uint32_t nc = ts_node_named_child_count(name);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode pn = ts_node_named_child(name, i);
            if (ts_node_is_null(pn))
                continue;
            const char *pk = ts_node_type(pn);
            if (strcmp(pk, "shorthand_property_identifier_pattern") == 0) {
                char *pnm = node_text(ctx, pn);
                if (pnm)
                    cbm_scope_bind(ctx->current_scope, pnm, lookup_member_type(ctx, bound, pnm));
            } else if (strcmp(pk, "pair_pattern") == 0) {
                TSNode key = ts_node_child_by_field_name(pn, "key", TS_LSP_FIELD_LEN("key"));
                TSNode val = ts_node_child_by_field_name(pn, "value", TS_LSP_FIELD_LEN("value"));
                if (!ts_node_is_null(key) && !ts_node_is_null(val) &&
                    node_kind_is(val, "identifier")) {
                    char *knm = node_text(ctx, key);
                    char *vnm = node_text(ctx, val);
                    if (knm && vnm) {
                        cbm_scope_bind(ctx->current_scope, vnm,
                                       lookup_member_type(ctx, bound, knm));
                    }
                }
            }
        }
    } else if (node_kind_is(name, "array_pattern")) {
        // Destructure array / tuple: `const [a, b] = pair`.
        uint32_t nc = ts_node_named_child_count(name);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode pn = ts_node_named_child(name, i);
            if (ts_node_is_null(pn) || !node_kind_is(pn, "identifier"))
                continue;
            char *pnm = node_text(ctx, pn);
            if (!pnm)
                continue;
            const CBMType *et = cbm_type_unknown();
            if (bound && bound->kind == CBM_TYPE_TEMPLATE &&
                bound->data.template_type.template_args &&
                bound->data.template_type.arg_count >= 1) {
                et = bound->data.template_type.template_args[0];
            } else if (bound && bound->kind == CBM_TYPE_TUPLE && bound->data.tuple.elems &&
                       (int)i < bound->data.tuple.count) {
                et = bound->data.tuple.elems[i];
            }
            cbm_scope_bind(ctx->current_scope, pnm, et);
        }
    }
}

// Bind a single TS parameter into the current scope. Robust against grammar field-name
// variations: tries `pattern`, `name`, then falls back to first named child of the param.
static void bind_parameter(TSLSPContext *ctx, TSNode param) {
    const char *pk = ts_node_type(param);
    if (strcmp(pk, "required_parameter") != 0 && strcmp(pk, "optional_parameter") != 0 &&
        strcmp(pk, "tuple_parameter") != 0 && strcmp(pk, "optional_tuple_parameter") != 0)
        return;

    // Find pattern: try field "pattern", then "name", then first non-type named child.
    TSNode pattern = ts_node_child_by_field_name(param, "pattern", TS_LSP_FIELD_LEN("pattern"));
    if (ts_node_is_null(pattern)) {
        pattern = ts_node_child_by_field_name(param, "name", TS_LSP_FIELD_LEN("name"));
    }
    if (ts_node_is_null(pattern)) {
        uint32_t nc = ts_node_named_child_count(param);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(param, i);
            if (ts_node_is_null(c))
                continue;
            const char *k = ts_node_type(c);
            if (strcmp(k, "type_annotation") == 0)
                continue;
            if (strcmp(k, "decorator") == 0)
                continue;
            pattern = c;
            break;
        }
    }
    if (ts_node_is_null(pattern))
        return;

    // Find the type annotation: prefer field "type", fall back to descending into a child
    // of kind "type_annotation".
    TSNode tann = ts_node_child_by_field_name(param, "type", TS_LSP_FIELD_LEN("type"));
    if (ts_node_is_null(tann)) {
        uint32_t nc = ts_node_named_child_count(param);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(param, i);
            if (!ts_node_is_null(c) && strcmp(ts_node_type(c), "type_annotation") == 0) {
                tann = c;
                break;
            }
        }
    }

    const CBMType *t = cbm_type_unknown();
    if (!ts_node_is_null(tann)) {
        // The tree may give us either the type_annotation node or its inner type node.
        TSNode tch = (strcmp(ts_node_type(tann), "type_annotation") == 0)
                         ? ts_node_named_child(tann, 0)
                         : tann;
        if (!ts_node_is_null(tch)) {
            const char *tk = ts_node_type(tch);
            // Inline object type `{ a: T; b: U }` — walk members and build OBJECT_LIT
            // directly via AST. Falling back to text-parsing the node would yield UNKNOWN.
            if (strcmp(tk, "object_type") == 0) {
                const char *p_names[32] = {0};
                const CBMType *p_types[32] = {0};
                int pcount = 0;
                uint32_t omc = ts_node_named_child_count(tch);
                for (uint32_t oi = 0; oi < omc && pcount < 31; oi++) {
                    TSNode m = ts_node_named_child(tch, oi);
                    if (ts_node_is_null(m))
                        continue;
                    const char *mk = ts_node_type(m);
                    if (strcmp(mk, "property_signature") == 0) {
                        TSNode pname =
                            ts_node_child_by_field_name(m, "name", TS_LSP_FIELD_LEN("name"));
                        TSNode ptype =
                            ts_node_child_by_field_name(m, "type", TS_LSP_FIELD_LEN("type"));
                        if (ts_node_is_null(pname))
                            continue;
                        char *pnm_text = node_text(ctx, pname);
                        if (!pnm_text)
                            continue;
                        const CBMType *pt = cbm_type_unknown();
                        if (!ts_node_is_null(ptype)) {
                            TSNode ptch = (strcmp(ts_node_type(ptype), "type_annotation") == 0)
                                              ? ts_node_named_child(ptype, 0)
                                              : ptype;
                            if (!ts_node_is_null(ptch))
                                pt = ts_parse_type_node(ctx, ptch);
                        }
                        p_names[pcount] = pnm_text;
                        p_types[pcount] = pt;
                        pcount++;
                    }
                }
                p_names[pcount] = NULL;
                p_types[pcount] = NULL;
                t = cbm_type_object_lit(ctx->arena, p_names, p_types, NULL, NULL);
            } else {
                t = ts_parse_type_node(ctx, tch);
            }
        }
    }

    if (node_kind_is(pattern, "identifier")) {
        char *nm = node_text(ctx, pattern);
        if (nm)
            cbm_scope_bind(ctx->current_scope, nm, t);
    } else if (node_kind_is(pattern, "object_pattern")) {
        uint32_t nc = ts_node_named_child_count(pattern);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode p = ts_node_named_child(pattern, i);
            if (node_kind_is(p, "shorthand_property_identifier_pattern")) {
                char *pnm = node_text(ctx, p);
                if (pnm)
                    cbm_scope_bind(ctx->current_scope, pnm, lookup_member_type(ctx, t, pnm));
            } else if (node_kind_is(p, "pair_pattern")) {
                TSNode key = ts_node_child_by_field_name(p, "key", TS_LSP_FIELD_LEN("key"));
                TSNode val = ts_node_child_by_field_name(p, "value", TS_LSP_FIELD_LEN("value"));
                if (!ts_node_is_null(key) && !ts_node_is_null(val) &&
                    node_kind_is(val, "identifier")) {
                    char *knm = node_text(ctx, key);
                    char *vnm = node_text(ctx, val);
                    if (knm && vnm) {
                        cbm_scope_bind(ctx->current_scope, vnm, lookup_member_type(ctx, t, knm));
                    }
                }
            }
        }
    } else if (node_kind_is(pattern, "array_pattern")) {
        uint32_t nc = ts_node_named_child_count(pattern);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode p = ts_node_named_child(pattern, i);
            if (!node_kind_is(p, "identifier"))
                continue;
            char *pnm = node_text(ctx, p);
            if (!pnm)
                continue;
            const CBMType *et = cbm_type_unknown();
            if (t && t->kind == CBM_TYPE_TEMPLATE && t->data.template_type.template_args &&
                t->data.template_type.arg_count >= 1) {
                et = t->data.template_type.template_args[0];
            }
            cbm_scope_bind(ctx->current_scope, pnm, et);
        }
    }
}

void ts_process_statement(TSLSPContext *ctx, TSNode node) {
    if (!ctx || ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "lexical_declaration") == 0 || strcmp(kind, "variable_declaration") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(node, i);
            if (node_kind_is(c, "variable_declarator"))
                bind_variable_declarator(ctx, c);
        }
    } else if (strcmp(kind, "for_in_statement") == 0 || strcmp(kind, "for_of_statement") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", TS_LSP_FIELD_LEN("left"));
        TSNode right = ts_node_child_by_field_name(node, "right", TS_LSP_FIELD_LEN("right"));
        if (!ts_node_is_null(left) && !ts_node_is_null(right)) {
            const CBMType *iter = ts_eval_expr_type(ctx, right);
            const CBMType *elem = cbm_type_unknown();
            if (iter && iter->kind == CBM_TYPE_TEMPLATE && iter->data.template_type.template_args &&
                iter->data.template_type.arg_count >= 1) {
                elem = iter->data.template_type.template_args[0];
            }
            // left is typically `lexical_declaration` of `(let x of arr)` or bare identifier.
            if (node_kind_is(left, "lexical_declaration") ||
                node_kind_is(left, "variable_declaration")) {
                TSNode v = ts_node_named_child(left, 0);
                if (node_kind_is(v, "variable_declarator")) {
                    TSNode nm = ts_node_child_by_field_name(v, "name", TS_LSP_FIELD_LEN("name"));
                    if (node_kind_is(nm, "identifier")) {
                        char *s = node_text(ctx, nm);
                        if (s)
                            cbm_scope_bind(ctx->current_scope, s, elem);
                    }
                }
            } else if (node_kind_is(left, "identifier")) {
                char *s = node_text(ctx, left);
                if (s)
                    cbm_scope_bind(ctx->current_scope, s, elem);
            }
        }
    }
}

// ── Call resolution + tree walk ───────────────────────────────────────────────

static void resolve_call_at(TSLSPContext *ctx, TSNode call_node) {
    TSNode fn = ts_node_child_by_field_name(call_node, "function", TS_LSP_FIELD_LEN("function"));
    if (ts_node_is_null(fn))
        return;

    // Argument count.
    int arg_count = 0;
    TSNode args =
        ts_node_child_by_field_name(call_node, "arguments", TS_LSP_FIELD_LEN("arguments"));
    if (!ts_node_is_null(args))
        arg_count = (int)ts_node_named_child_count(args);

    const char *fk = ts_node_type(fn);

    // member_expression: obj.method(...)
    if (strcmp(fk, "member_expression") == 0) {
        TSNode obj = ts_node_child_by_field_name(fn, "object", TS_LSP_FIELD_LEN("object"));
        TSNode prop = ts_node_child_by_field_name(fn, "property", TS_LSP_FIELD_LEN("property"));
        if (!ts_node_is_null(obj) && !ts_node_is_null(prop)) {
            char *mname = node_text(ctx, prop);
            if (mname) {
                // Identifier as namespace import: `name.fn()` or `mod.fn()`.
                if (node_kind_is(obj, "identifier")) {
                    char *on = node_text(ctx, obj);
                    if (on) {
                        for (int i = 0; i < ctx->import_count; i++) {
                            const char *lname =
                                ctx->import_local_names ? ctx->import_local_names[i] : NULL;
                            const char *mqn =
                                ctx->import_module_qns ? ctx->import_module_qns[i] : NULL;
                            if (lname && mqn && strcmp(lname, on) == 0) {
                                const CBMRegisteredFunc *f = cbm_registry_lookup_symbol_by_args(
                                    ctx->registry, mqn, mname, arg_count);
                                if (f) {
                                    ts_emit_resolved_call(ctx, f->qualified_name,
                                                          "lsp_ts_namespace", 0.95f);
                                    return;
                                }
                                ts_emit_unresolved_call(
                                    ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", on, mname),
                                    "module_symbol_not_in_registry");
                                return;
                            }
                        }
                    }
                }
                // Type-based dispatch.
                const CBMType *recv = ts_eval_expr_type(ctx, obj);
                const CBMRegisteredFunc *f = lookup_method(ctx, recv, mname);

                // Overload resolution by arg types: when receiver is NAMED and the
                // registry has multiple overloads (same receiver+name), eval each
                // arg's type and let cbm_registry_lookup_method_by_types score them.
                // Falls back to the first match if scoring fails.
                if (f && recv) {
                    const CBMType *base = simplify_type(ctx, recv);
                    base = base ? unwrap_passthrough_template(base) : NULL;
                    if (base && base->kind == CBM_TYPE_NAMED && base->data.named.qualified_name &&
                        arg_count > 0) {
                        const CBMType *arg_types[16] = {0};
                        int n = arg_count > 16 ? 16 : arg_count;
                        for (int i = 0; i < n; i++) {
                            TSNode arg = ts_node_named_child(args, (uint32_t)i);
                            arg_types[i] = ts_eval_expr_type(ctx, arg);
                        }
                        const CBMRegisteredFunc *better = cbm_registry_lookup_method_by_types(
                            ctx->registry, base->data.named.qualified_name, mname, arg_types, n);
                        if (better)
                            f = better;
                    }
                }

                if (f) {
                    ts_emit_resolved_call(ctx, f->qualified_name, "lsp_ts_method", 0.95f);
                    return;
                }
                ts_emit_unresolved_call(ctx, mname, "method_not_in_registry");
            }
        }
        return;
    }

    // identifier: free function call.
    if (strcmp(fk, "identifier") == 0) {
        char *name = node_text(ctx, fn);
        if (!name)
            return;

        // Eval arg types up-front for type-aware overload resolution.
        const CBMType *arg_types[16] = {0};
        int typed_arg_n = 0;
        if (!ts_node_is_null(args) && arg_count > 0) {
            typed_arg_n = arg_count > 16 ? 16 : arg_count;
            for (int i = 0; i < typed_arg_n; i++) {
                TSNode arg = ts_node_named_child(args, (uint32_t)i);
                arg_types[i] = ts_eval_expr_type(ctx, arg);
            }
        }

        // Module-local function: prefer by-types when args are typed.
        if (ctx->module_qn) {
            const CBMRegisteredFunc *f = NULL;
            if (typed_arg_n > 0) {
                f = cbm_registry_lookup_symbol_by_types(ctx->registry, ctx->module_qn, name,
                                                        arg_types, typed_arg_n);
            }
            if (!f) {
                f = cbm_registry_lookup_symbol_by_args(ctx->registry, ctx->module_qn, name,
                                                       arg_count);
            }
            if (f) {
                ts_emit_resolved_call(ctx, f->qualified_name, "lsp_ts_local", 0.95f);
                return;
            }
        }
        // Imported default/named function.
        for (int i = 0; i < ctx->import_count; i++) {
            const char *lname = ctx->import_local_names ? ctx->import_local_names[i] : NULL;
            const char *mqn = ctx->import_module_qns ? ctx->import_module_qns[i] : NULL;
            if (lname && mqn && strcmp(lname, name) == 0) {
                const CBMRegisteredFunc *f =
                    cbm_registry_lookup_symbol_by_args(ctx->registry, mqn, name, arg_count);
                if (f) {
                    ts_emit_resolved_call(ctx, f->qualified_name, "lsp_ts_import", 0.95f);
                    return;
                }
                // Fall through to qualified_name fallback.
                const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", mqn, name);
                ts_emit_unresolved_call(ctx, qn, "import_symbol_not_in_registry");
                return;
            }
        }
        ts_emit_unresolved_call(ctx, name, "func_not_in_registry");
    }
}

// Process an arrow_function or function_expression as a callback whose param types are
// contextually typed by the enclosing call (e.g. `arr.map(x => x.length)` — `x` should
// be bound to `arr`'s element type via Array<T>.map's `(x: T) => U` signature). The
// caller passes `expected` = the FUNC type the callback must conform to.
static void process_callback_arrow(TSLSPContext *ctx, TSNode arrow, const CBMType *expected) {
    if (ts_node_is_null(arrow) || !expected || expected->kind != CBM_TYPE_FUNC)
        return;

    CBMScope *saved = ctx->current_scope;
    ctx->current_scope = cbm_scope_push(ctx->arena, saved);

    // Tree-sitter typescript shapes for arrow functions:
    //   `(x, y) => body`  → parameters: formal_parameters
    //   `x => body`       → parameter: identifier (singular field)
    TSNode params =
        ts_node_child_by_field_name(arrow, "parameters", TS_LSP_FIELD_LEN("parameters"));
    TSNode bare_param =
        ts_node_child_by_field_name(arrow, "parameter", TS_LSP_FIELD_LEN("parameter"));
    int idx = 0;
    if (!ts_node_is_null(bare_param)) {
        // Single bare param: `x => ...`.
        const char *pk = ts_node_type(bare_param);
        if (strcmp(pk, "identifier") == 0) {
            char *nm = node_text(ctx, bare_param);
            const CBMType *pt =
                (expected->data.func.param_types && expected->data.func.param_types[0])
                    ? expected->data.func.param_types[0]
                    : cbm_type_unknown();
            if (nm)
                cbm_scope_bind(ctx->current_scope, nm, pt);
        }
    } else if (!ts_node_is_null(params)) {
        uint32_t pc = ts_node_named_child_count(params);
        for (uint32_t i = 0; i < pc; i++) {
            TSNode p = ts_node_named_child(params, i);
            if (ts_node_is_null(p))
                continue;
            const char *pk = ts_node_type(p);
            const char *pname = NULL;
            const CBMType *expected_pt =
                (expected->data.func.param_types && expected->data.func.param_types[idx])
                    ? expected->data.func.param_types[idx]
                    : NULL;

            if (strcmp(pk, "identifier") == 0) {
                pname = node_text(ctx, p);
            } else {
                // required_parameter / optional_parameter — extract pattern.
                TSNode pp = ts_node_child_by_field_name(p, "pattern", TS_LSP_FIELD_LEN("pattern"));
                if (ts_node_is_null(pp)) {
                    pp = ts_node_child_by_field_name(p, "name", TS_LSP_FIELD_LEN("name"));
                }
                if (!ts_node_is_null(pp) && strcmp(ts_node_type(pp), "identifier") == 0) {
                    pname = node_text(ctx, pp);
                }
                // If the param has its own annotation, use that. Otherwise use expected.
                TSNode tann = ts_node_child_by_field_name(p, "type", TS_LSP_FIELD_LEN("type"));
                if (!ts_node_is_null(tann)) {
                    TSNode tch = (strcmp(ts_node_type(tann), "type_annotation") == 0)
                                     ? ts_node_named_child(tann, 0)
                                     : tann;
                    if (!ts_node_is_null(tch)) {
                        const CBMType *annotated = ts_parse_type_node(ctx, tch);
                        if (!cbm_type_is_unknown(annotated)) {
                            expected_pt = annotated;
                        }
                    }
                }
            }
            if (pname) {
                cbm_scope_bind(ctx->current_scope, pname,
                               expected_pt ? expected_pt : cbm_type_unknown());
            }
            idx++;
        }
    }

    TSNode body = ts_node_child_by_field_name(arrow, "body", TS_LSP_FIELD_LEN("body"));
    if (!ts_node_is_null(body)) {
        if (strcmp(ts_node_type(body), "statement_block") == 0) {
            TSTreeCursor cursor = ts_tree_cursor_new(body);
            if (ts_tree_cursor_goto_first_child(&cursor)) {
                do {
                    process_node(ctx, ts_tree_cursor_current_node(&cursor));
                } while (ts_tree_cursor_goto_next_sibling(&cursor));
            }
            ts_tree_cursor_delete(&cursor);
        } else {
            process_node(ctx, body);
        }
    }

    ctx->current_scope = saved;
}

// Try to extract a narrowing fact from an `if`-condition: `x instanceof Foo`,
// `typeof x === 'string'`, or `x.kind === 'lit'` (discriminated union).
// On success: writes the narrowed variable name and target type, plus whether the
// match polarity is `==` (true → consequence) or `!=` (true → alternative).
// Returns true if narrowing applies.
static bool extract_narrowing(TSLSPContext *ctx, TSNode condition, const char **out_var,
                              const CBMType **out_type, bool *out_inverted) {
    *out_var = NULL;
    *out_type = NULL;
    *out_inverted = false;
    if (ts_node_is_null(condition))
        return false;

    // Strip outer parens if any.
    while (!ts_node_is_null(condition) &&
           strcmp(ts_node_type(condition), "parenthesized_expression") == 0) {
        condition = ts_node_named_child(condition, 0);
    }
    if (ts_node_is_null(condition))
        return false;
    const char *k = ts_node_type(condition);

    // `x instanceof Foo`: binary_expression with operator "instanceof".
    if (strcmp(k, "binary_expression") == 0) {
        TSNode op =
            ts_node_child_by_field_name(condition, "operator", TS_LSP_FIELD_LEN("operator"));
        char *opt = ts_node_is_null(op) ? NULL : node_text(ctx, op);
        TSNode left = ts_node_child_by_field_name(condition, "left", TS_LSP_FIELD_LEN("left"));
        TSNode right = ts_node_child_by_field_name(condition, "right", TS_LSP_FIELD_LEN("right"));

        if (opt && strcmp(opt, "instanceof") == 0 && !ts_node_is_null(left) &&
            !ts_node_is_null(right) && strcmp(ts_node_type(left), "identifier") == 0 &&
            strcmp(ts_node_type(right), "identifier") == 0) {
            *out_var = node_text(ctx, left);
            char *class_name = node_text(ctx, right);
            if (class_name) {
                *out_type = parse_ts_type_text(ctx->arena, class_name, ctx->module_qn);
                *out_type = resolve_type_with_imports(ctx, *out_type);
            }
            return *out_var && *out_type;
        }

        // `typeof x === 'string'` / `typeof x !== 'number'`
        if (opt &&
            (strcmp(opt, "===") == 0 || strcmp(opt, "==") == 0 || strcmp(opt, "!==") == 0 ||
             strcmp(opt, "!=") == 0) &&
            !ts_node_is_null(left) && !ts_node_is_null(right)) {
            bool inverted = (strcmp(opt, "!==") == 0 || strcmp(opt, "!=") == 0);

            // typeof x === 'string'
            if (strcmp(ts_node_type(left), "unary_expression") == 0) {
                TSNode op2 =
                    ts_node_child_by_field_name(left, "operator", TS_LSP_FIELD_LEN("operator"));
                char *opt2 = ts_node_is_null(op2) ? NULL : node_text(ctx, op2);
                TSNode arg =
                    ts_node_child_by_field_name(left, "argument", TS_LSP_FIELD_LEN("argument"));
                if (opt2 && strcmp(opt2, "typeof") == 0 && !ts_node_is_null(arg) &&
                    strcmp(ts_node_type(arg), "identifier") == 0 &&
                    strcmp(ts_node_type(right), "string") == 0) {
                    *out_var = node_text(ctx, arg);
                    char *lit = node_text(ctx, right);
                    if (lit) {
                        // Strip surrounding quotes.
                        size_t ll = strlen(lit);
                        if (ll >= 2 && (lit[0] == '"' || lit[0] == '\'') && lit[ll - 1] == lit[0]) {
                            lit[ll - 1] = '\0';
                            lit++;
                        }
                        *out_type = cbm_type_builtin(ctx->arena, lit);
                    }
                    *out_inverted = inverted;
                    return *out_var && *out_type;
                }
            }
        }
    }

    return false;
}

// Look up a union member that satisfies a discriminant: when narrowed via `x.kind ===
// 'foo'`, find the member of x's type whose `kind` property is the literal 'foo'.
// Returns NULL if no match. Pragmatic v1: only handles `member_expression === string`.
static const CBMType *narrow_discriminated_union(TSLSPContext *ctx, TSNode condition,
                                                 const char **out_var) {
    *out_var = NULL;
    if (ts_node_is_null(condition))
        return NULL;
    while (!ts_node_is_null(condition) &&
           strcmp(ts_node_type(condition), "parenthesized_expression") == 0) {
        condition = ts_node_named_child(condition, 0);
    }
    if (ts_node_is_null(condition))
        return NULL;
    if (strcmp(ts_node_type(condition), "binary_expression") != 0)
        return NULL;

    TSNode op = ts_node_child_by_field_name(condition, "operator", TS_LSP_FIELD_LEN("operator"));
    char *opt = ts_node_is_null(op) ? NULL : node_text(ctx, op);
    if (!opt || (strcmp(opt, "===") != 0 && strcmp(opt, "==") != 0))
        return NULL;

    TSNode left = ts_node_child_by_field_name(condition, "left", TS_LSP_FIELD_LEN("left"));
    TSNode right = ts_node_child_by_field_name(condition, "right", TS_LSP_FIELD_LEN("right"));
    if (ts_node_is_null(left) || ts_node_is_null(right))
        return NULL;

    // left = `x.kind`, right = string literal.
    if (strcmp(ts_node_type(left), "member_expression") != 0 ||
        strcmp(ts_node_type(right), "string") != 0)
        return NULL;

    TSNode obj = ts_node_child_by_field_name(left, "object", TS_LSP_FIELD_LEN("object"));
    TSNode prop = ts_node_child_by_field_name(left, "property", TS_LSP_FIELD_LEN("property"));
    if (ts_node_is_null(obj) || strcmp(ts_node_type(obj), "identifier") != 0)
        return NULL;
    if (ts_node_is_null(prop))
        return NULL;
    char *var_name = node_text(ctx, obj);
    char *prop_name = node_text(ctx, prop);
    char *lit = node_text(ctx, right);
    if (!var_name || !prop_name || !lit)
        return NULL;
    size_t ll = strlen(lit);
    if (ll >= 2 && (lit[0] == '"' || lit[0] == '\'') && lit[ll - 1] == lit[0]) {
        lit[ll - 1] = '\0';
        lit++;
    }

    const CBMType *var_type = cbm_scope_lookup(ctx->current_scope, var_name);
    if (!var_type || var_type->kind != CBM_TYPE_UNION)
        return NULL;
    if (!var_type->data.union_type.members)
        return NULL;

    for (int i = 0; i < var_type->data.union_type.count; i++) {
        const CBMType *m = var_type->data.union_type.members[i];
        if (!m || m->kind != CBM_TYPE_NAMED)
            continue;
        const CBMRegisteredType *rt =
            cbm_registry_lookup_type(ctx->registry, m->data.named.qualified_name);
        if (!rt || !rt->field_names || !rt->field_types)
            continue;
        for (int j = 0; rt->field_names[j]; j++) {
            if (strcmp(rt->field_names[j], prop_name) != 0)
                continue;
            const CBMType *ft = rt->field_types[j];
            if (!ft)
                continue;
            // Discriminant fields are typically string-literal types (`'circle'`) or
            // the BUILTIN("string") fallback. For BUILTIN fallback we can't narrow
            // precisely, so skip unless the parsed text matches our literal.
            if (ft->kind == CBM_TYPE_TS_LITERAL && ft->data.literal_ts.value &&
                strcmp(ft->data.literal_ts.value, lit) == 0) {
                *out_var = var_name;
                return m;
            }
            // Some interfaces declare `kind: 'circle'` as NAMED("'circle'") via
            // text fall-through; handle that too.
            if (ft->kind == CBM_TYPE_NAMED && ft->data.named.qualified_name) {
                const char *qn = ft->data.named.qualified_name;
                size_t qnl = strlen(qn);
                if (qnl >= 2 && (qn[0] == '\'' || qn[0] == '"') && qn[qnl - 1] == qn[0]) {
                    if (strncmp(qn + 1, lit, qnl - 2) == 0 && strlen(lit) == qnl - 2) {
                        *out_var = var_name;
                        return m;
                    }
                }
            }
        }
    }
    return NULL;
}

// Resolve a JSX element opening — emits a resolved call edge for component invocations.
// `<Foo prop={x}/>` and `<Foo>...</Foo>` both invoke the component as a function.
// Lowercase first letter is treated as a JSX intrinsic (HTML element) and ignored.
static void resolve_jsx_element(TSLSPContext *ctx, TSNode element_node) {
    if (!ctx->jsx_mode)
        return;
    // The element's first named child is the opening element (or self-closing).
    const char *kind = ts_node_type(element_node);
    TSNode tag = element_node;
    if (strcmp(kind, "jsx_element") == 0) {
        TSNode opening = ts_node_named_child(element_node, 0);
        if (!ts_node_is_null(opening))
            tag = opening;
    }

    // Get the name from the opening / self_closing element. Field is "name".
    TSNode name_node = ts_node_child_by_field_name(tag, "name", TS_LSP_FIELD_LEN("name"));
    if (ts_node_is_null(name_node))
        return;
    char *tag_name = node_text(ctx, name_node);
    if (!tag_name || !tag_name[0])
        return;
    // Lowercase first → intrinsic (HTML element). Skip emission.
    if (tag_name[0] >= 'a' && tag_name[0] <= 'z')
        return;

    // Resolve as a free-function/component call.
    if (ctx->module_qn) {
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, tag_name);
        if (f) {
            ts_emit_resolved_call(ctx, f->qualified_name, "lsp_ts_jsx", 0.95f);
            return;
        }
    }
    // Try imports.
    for (int i = 0; i < ctx->import_count; i++) {
        const char *lname = ctx->import_local_names ? ctx->import_local_names[i] : NULL;
        const char *mqn = ctx->import_module_qns ? ctx->import_module_qns[i] : NULL;
        if (lname && mqn && strcmp(lname, tag_name) == 0) {
            const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", mqn, tag_name);
            ts_emit_resolved_call(ctx, qn, "lsp_ts_jsx_import", 0.85f);
            return;
        }
    }
    ts_emit_unresolved_call(ctx, tag_name, "jsx_component_not_in_registry");
}

static void process_node(TSLSPContext *ctx, TSNode node) {
    if (!ctx || ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    // Scope-affecting statements bind first, then we recurse.
    ts_process_statement(ctx, node);

    if (strcmp(kind, "call_expression") == 0) {
        resolve_call_at(ctx, node);

        // Contextual callback typing: when an arg is an arrow_function and the
        // corresponding param of the called function is itself a FUNC, propagate the
        // expected callback param types into the arrow's body before walking it.
        // Handle arg processing manually for this path to avoid double-walking via
        // default recurse below.
        do {
            TSNode fn_node =
                ts_node_child_by_field_name(node, "function", TS_LSP_FIELD_LEN("function"));
            if (ts_node_is_null(fn_node))
                break;
            const CBMType *fn_type = ts_eval_expr_type(ctx, fn_node);
            if (!fn_type || fn_type->kind != CBM_TYPE_FUNC)
                break;
            if (!fn_type->data.func.param_types)
                break;
            TSNode args =
                ts_node_child_by_field_name(node, "arguments", TS_LSP_FIELD_LEN("arguments"));
            if (ts_node_is_null(args))
                break;

            // Process the function child for nested call resolution.
            process_node(ctx, fn_node);

            // param_types is NULL-terminated (no count field). Measure its
            // length so we never index past the terminator: a call may pass
            // more args than the function declares params (e.g. excess/variadic
            // args), and the extra args simply have no expected type. Indexing
            // param_types[i] by the raw arg count read out of bounds → garbage
            // CBMType* → crash on expected->kind.
            uint32_t param_count = 0;
            while (fn_type->data.func.param_types[param_count])
                param_count++;

            uint32_t argc = ts_node_named_child_count(args);
            for (uint32_t i = 0; i < argc; i++) {
                TSNode arg = ts_node_named_child(args, i);
                if (ts_node_is_null(arg))
                    continue;
                const char *ak = ts_node_type(arg);
                const CBMType *expected =
                    (i < param_count) ? fn_type->data.func.param_types[i] : NULL;
                if ((strcmp(ak, "arrow_function") == 0 || strcmp(ak, "function_expression") == 0) &&
                    expected && expected->kind == CBM_TYPE_FUNC) {
                    process_callback_arrow(ctx, arg, expected);
                } else {
                    process_node(ctx, arg);
                }
            }
            return; // Skip default recurse since we processed children manually.
        } while (0);
    }

    // JSX element resolution (TSX / JSX modes).
    if (ctx->jsx_mode &&
        (strcmp(kind, "jsx_element") == 0 || strcmp(kind, "jsx_self_closing_element") == 0)) {
        resolve_jsx_element(ctx, node);
    }

    // Switch-statement narrowing: `switch (x.kind) { case 'foo': ... }` — narrow x
    // to the union member whose discriminant property matches the case literal.
    if (strcmp(kind, "switch_statement") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", TS_LSP_FIELD_LEN("value"));
        TSNode body = ts_node_child_by_field_name(node, "body", TS_LSP_FIELD_LEN("body"));
        // Walk the value expression so any nested calls resolve.
        if (!ts_node_is_null(value))
            process_node(ctx, value);

        // Identify discriminant: switch on `x.kind` member_expression.
        const char *disc_var = NULL;
        const char *disc_prop = NULL;
        const CBMType *var_union = NULL;
        if (!ts_node_is_null(value)) {
            // Strip parens.
            TSNode v = value;
            while (!ts_node_is_null(v) &&
                   strcmp(ts_node_type(v), "parenthesized_expression") == 0) {
                v = ts_node_named_child(v, 0);
            }
            if (!ts_node_is_null(v) && strcmp(ts_node_type(v), "member_expression") == 0) {
                TSNode obj = ts_node_child_by_field_name(v, "object", TS_LSP_FIELD_LEN("object"));
                TSNode prop =
                    ts_node_child_by_field_name(v, "property", TS_LSP_FIELD_LEN("property"));
                if (!ts_node_is_null(obj) && !ts_node_is_null(prop) &&
                    strcmp(ts_node_type(obj), "identifier") == 0) {
                    disc_var = node_text(ctx, obj);
                    disc_prop = node_text(ctx, prop);
                    if (disc_var) {
                        const CBMType *t = cbm_scope_lookup(ctx->current_scope, disc_var);
                        if (t && t->kind == CBM_TYPE_UNION)
                            var_union = t;
                    }
                }
            }
        }

        // Walk each switch_case in the body; narrow x in each case's scope.
        if (!ts_node_is_null(body)) {
            uint32_t bnc = ts_node_named_child_count(body);
            for (uint32_t i = 0; i < bnc; i++) {
                TSNode case_node = ts_node_named_child(body, i);
                if (ts_node_is_null(case_node))
                    continue;
                const char *ck = ts_node_type(case_node);
                if (strcmp(ck, "switch_case") != 0 && strcmp(ck, "switch_default") != 0) {
                    process_node(ctx, case_node);
                    continue;
                }

                // Find this case's literal value.
                const CBMType *narrowed = NULL;
                if (var_union && disc_prop && strcmp(ck, "switch_case") == 0) {
                    TSNode case_value =
                        ts_node_child_by_field_name(case_node, "value", TS_LSP_FIELD_LEN("value"));
                    if (!ts_node_is_null(case_value) &&
                        strcmp(ts_node_type(case_value), "string") == 0) {
                        char *lit = node_text(ctx, case_value);
                        if (lit) {
                            size_t ll = strlen(lit);
                            if (ll >= 2 && (lit[0] == '"' || lit[0] == '\'') &&
                                lit[ll - 1] == lit[0]) {
                                lit[ll - 1] = '\0';
                                lit++;
                            }
                            // Match member with matching discriminant field literal.
                            for (int mi = 0; mi < var_union->data.union_type.count; mi++) {
                                const CBMType *m = var_union->data.union_type.members[mi];
                                if (!m || m->kind != CBM_TYPE_NAMED)
                                    continue;
                                const CBMRegisteredType *rt = cbm_registry_lookup_type(
                                    ctx->registry, m->data.named.qualified_name);
                                if (!rt || !rt->field_names || !rt->field_types)
                                    continue;
                                for (int fj = 0; rt->field_names[fj]; fj++) {
                                    if (strcmp(rt->field_names[fj], disc_prop) != 0)
                                        continue;
                                    const CBMType *ft = rt->field_types[fj];
                                    if (!ft)
                                        continue;
                                    if (ft->kind == CBM_TYPE_TS_LITERAL &&
                                        ft->data.literal_ts.value &&
                                        strcmp(ft->data.literal_ts.value, lit) == 0) {
                                        narrowed = m;
                                        break;
                                    }
                                    if (ft->kind == CBM_TYPE_NAMED &&
                                        ft->data.named.qualified_name) {
                                        const char *qn = ft->data.named.qualified_name;
                                        size_t qnl = strlen(qn);
                                        if (qnl >= 2 && (qn[0] == '\'' || qn[0] == '"') &&
                                            qn[qnl - 1] == qn[0] &&
                                            strncmp(qn + 1, lit, qnl - 2) == 0 &&
                                            strlen(lit) == qnl - 2) {
                                            narrowed = m;
                                            break;
                                        }
                                    }
                                }
                                if (narrowed)
                                    break;
                            }
                        }
                    }
                }

                // Walk case body with narrowed scope if applicable.
                CBMScope *saved = ctx->current_scope;
                if (narrowed && disc_var) {
                    ctx->current_scope = cbm_scope_push(ctx->arena, saved);
                    cbm_scope_bind(ctx->current_scope, disc_var, narrowed);
                }
                // case_node's children include the value and the case body.
                uint32_t cnc = ts_node_child_count(case_node);
                for (uint32_t j = 0; j < cnc; j++) {
                    process_node(ctx, ts_node_child(case_node, j));
                }
                ctx->current_scope = saved;
            }
        }
        return;
    }

    // Flow-sensitive narrowing for `if (...) { consequence } else { alternative }`.
    // Recognises `x instanceof T`, `typeof x === 'string'`, and `x.kind === 'lit'`
    // (discriminated union). Narrowed binding is pushed into a child scope for the
    // matching branch; the other branch sees the original type.
    if (strcmp(kind, "if_statement") == 0) {
        TSNode condition =
            ts_node_child_by_field_name(node, "condition", TS_LSP_FIELD_LEN("condition"));
        TSNode consequence =
            ts_node_child_by_field_name(node, "consequence", TS_LSP_FIELD_LEN("consequence"));
        TSNode alt =
            ts_node_child_by_field_name(node, "alternative", TS_LSP_FIELD_LEN("alternative"));

        // Walk the condition itself so any nested calls there resolve too.
        if (!ts_node_is_null(condition))
            process_node(ctx, condition);

        // 1. Try simple narrowing (instanceof, typeof).
        const char *nv = NULL;
        const CBMType *nt = NULL;
        bool inverted = false;
        bool ok = extract_narrowing(ctx, condition, &nv, &nt, &inverted);

        // 2. Try discriminated-union narrowing if simple narrowing didn't apply.
        if (!ok) {
            const char *dv = NULL;
            const CBMType *dt = narrow_discriminated_union(ctx, condition, &dv);
            if (dv && dt) {
                nv = dv;
                nt = dt;
                inverted = false;
                ok = true;
            }
        }

        // Walk consequence with narrowed binding (for the truthy branch unless inverted).
        if (!ts_node_is_null(consequence)) {
            if (ok && nv && nt && !inverted) {
                CBMScope *saved = ctx->current_scope;
                ctx->current_scope = cbm_scope_push(ctx->arena, saved);
                cbm_scope_bind(ctx->current_scope, nv, nt);
                process_node(ctx, consequence);
                ctx->current_scope = saved;
            } else {
                process_node(ctx, consequence);
            }
        }

        // Walk alternative with negated narrowing for typeof case (instanceof+union
        // narrowing inverse is harder — skip).
        if (!ts_node_is_null(alt)) {
            if (ok && nv && nt && inverted) {
                CBMScope *saved = ctx->current_scope;
                ctx->current_scope = cbm_scope_push(ctx->arena, saved);
                cbm_scope_bind(ctx->current_scope, nv, nt);
                process_node(ctx, alt);
                ctx->current_scope = saved;
            } else {
                process_node(ctx, alt);
            }
        }

        return; // Skip default recurse.
    }

    // Function-introducing nodes get their own scope + enclosing_func_qn handling.
    if (strcmp(kind, "function_declaration") == 0 || strcmp(kind, "method_definition") == 0 ||
        strcmp(kind, "function_expression") == 0 || strcmp(kind, "arrow_function") == 0 ||
        strcmp(kind, "method_signature") == 0) {
        // Nested functions just get a fresh scope; top-level / class-method walks own
        // their enclosing_func_qn via process_function_body.
        CBMScope *saved = ctx->current_scope;
        ctx->current_scope = cbm_scope_push(ctx->arena, saved);
        TSNode params =
            ts_node_child_by_field_name(node, "parameters", TS_LSP_FIELD_LEN("parameters"));
        if (!ts_node_is_null(params)) {
            uint32_t nc = ts_node_named_child_count(params);
            for (uint32_t i = 0; i < nc; i++)
                bind_parameter(ctx, ts_node_named_child(params, i));
        }
        TSNode body = ts_node_child_by_field_name(node, "body", TS_LSP_FIELD_LEN("body"));
        if (!ts_node_is_null(body)) {
            // Arrow functions with a single-expression body: body IS the expression
            // (often a call_expression). Recursing into children skips it. Always
            // process_node(body) so resolve_call_at fires for that expression.
            process_node(ctx, body);
        }
        ctx->current_scope = saved;
        return;
    }

    // Class / interface declarations are walked by the registry pass; here we just dive
    // into method_definition bodies for inner call resolution.
    if (strcmp(kind, "class_declaration") == 0) {
        // Find the class name to set enclosing_class_qn for methods.
        TSNode name = ts_node_child_by_field_name(node, "name", TS_LSP_FIELD_LEN("name"));
        const char *class_qn = NULL;
        if (!ts_node_is_null(name) && ctx->module_qn) {
            char *cn = node_text(ctx, name);
            if (cn)
                class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cn);
        }
        TSNode body = ts_node_child_by_field_name(node, "body", TS_LSP_FIELD_LEN("body"));
        if (!ts_node_is_null(body) && class_qn) {
            uint32_t nc = ts_node_named_child_count(body);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode m = ts_node_named_child(body, i);
                if (!node_kind_is(m, "method_definition"))
                    continue;
                TSNode mname = ts_node_child_by_field_name(m, "name", TS_LSP_FIELD_LEN("name"));
                if (ts_node_is_null(mname))
                    continue;
                char *mn = node_text(ctx, mname);
                if (!mn)
                    continue;
                const char *mqn = strcmp(mn, "constructor") == 0
                                      ? cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, mn)
                                      : cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, mn);
                process_function_body(ctx, m, mqn, class_qn);
            }
        }
        return;
    }

    // Default: recurse into children via a cursor (O(n)). ts_node_child(node,i)
    // is O(i) in tree-sitter → O(n²) when `node` is a wide block/root.
    {
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                process_node(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }
}

static void process_function_body(TSLSPContext *ctx, TSNode func_node, const char *func_qn,
                                  const char *class_qn) {
    if (ts_node_is_null(func_node))
        return;
    CBMScope *saved_scope = ctx->current_scope;
    const char *saved_func = ctx->enclosing_func_qn;
    const char *saved_class = ctx->enclosing_class_qn;

    ctx->current_scope = cbm_scope_push(ctx->arena, saved_scope);
    ctx->enclosing_func_qn = func_qn;
    ctx->enclosing_class_qn = class_qn ? class_qn : saved_class;

    if (class_qn) {
        // Bind `this` to the class type for member resolution within methods.
        cbm_scope_bind(ctx->current_scope, "this", cbm_type_named(ctx->arena, class_qn));
    }

    TSNode params =
        ts_node_child_by_field_name(func_node, "parameters", TS_LSP_FIELD_LEN("parameters"));
    if (!ts_node_is_null(params)) {
        uint32_t nc = ts_node_named_child_count(params);
        for (uint32_t i = 0; i < nc; i++)
            bind_parameter(ctx, ts_node_named_child(params, i));

        // JSDoc / signature-derived param-type fallback: if bind_parameter left a
        // param bound to UNKNOWN (no inline TS annotation), use the registered func's
        // signature param_types[i] (which apply_jsdoc_signatures may have populated).
        if (func_qn) {
            const CBMRegisteredFunc *rf = cbm_registry_lookup_func(ctx->registry, func_qn);
            if (rf && rf->signature && rf->signature->kind == CBM_TYPE_FUNC &&
                rf->signature->data.func.param_names && rf->signature->data.func.param_types) {
                for (int i = 0; rf->signature->data.func.param_names[i] &&
                                rf->signature->data.func.param_types[i];
                     i++) {
                    const char *pname = rf->signature->data.func.param_names[i];
                    const CBMType *ptype = rf->signature->data.func.param_types[i];
                    if (cbm_type_is_unknown(ptype))
                        continue;
                    const CBMType *existing = cbm_scope_lookup(ctx->current_scope, pname);
                    if (existing && !cbm_type_is_unknown(existing))
                        continue;
                    cbm_scope_bind(ctx->current_scope, pname, ptype);
                }
            }
        }
    }

    TSNode body = ts_node_child_by_field_name(func_node, "body", TS_LSP_FIELD_LEN("body"));
    if (!ts_node_is_null(body)) {
        // statement_block: walk every child statement. Expression body (concise
        // arrow): walk the expression directly — process_node on that handles
        // call_expression resolution.
        if (strcmp(ts_node_type(body), "statement_block") == 0) {
            TSTreeCursor cursor = ts_tree_cursor_new(body);
            if (ts_tree_cursor_goto_first_child(&cursor)) {
                do {
                    process_node(ctx, ts_tree_cursor_current_node(&cursor));
                } while (ts_tree_cursor_goto_next_sibling(&cursor));
            }
            ts_tree_cursor_delete(&cursor);
        } else {
            process_node(ctx, body);
        }
    }

    ctx->enclosing_class_qn = saved_class;
    ctx->enclosing_func_qn = saved_func;
    ctx->current_scope = saved_scope;
}

void ts_lsp_process_file(TSLSPContext *ctx, TSNode root) {
    if (!ctx || ts_node_is_null(root))
        return;
    if (ctx->dts_mode)
        return;

    // Collect top-level children once (O(n)); both passes reuse the array.
    // See collect_children: indexing ts_node_child(root,i) here would be O(n²).
    uint32_t kn = 0;
    TSNode *kids = collect_children(ctx->arena, root, &kn);

    // Pass 1: bind module-level declarations into root scope.
    for (uint32_t i = 0; i < kn; i++) {
        TSNode child = kids[i];
        const char *kind = ts_node_type(child);
        if (strcmp(kind, "lexical_declaration") == 0 || strcmp(kind, "variable_declaration") == 0) {
            ts_process_statement(ctx, child);
        }
    }

    // Pass 2: process functions, methods, and class bodies.
    for (uint32_t i = 0; i < kn; i++) {
        TSNode child = kids[i];
        const char *kind = ts_node_type(child);

        if (strcmp(kind, "function_declaration") == 0) {
            TSNode name = ts_node_child_by_field_name(child, "name", TS_LSP_FIELD_LEN("name"));
            if (ts_node_is_null(name) || !ctx->module_qn)
                continue;
            char *fn = node_text(ctx, name);
            if (!fn)
                continue;
            const char *fqn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, fn);
            process_function_body(ctx, child, fqn, NULL);
        } else if (strcmp(kind, "class_declaration") == 0) {
            process_node(ctx, child);
        } else if (strcmp(kind, "export_statement") == 0) {
            // Walk the wrapped declaration.
            uint32_t enc = ts_node_named_child_count(child);
            for (uint32_t j = 0; j < enc; j++) {
                TSNode inner = ts_node_named_child(child, j);
                if (ts_node_is_null(inner))
                    continue;
                const char *ik = ts_node_type(inner);
                if (strcmp(ik, "function_declaration") == 0) {
                    TSNode name =
                        ts_node_child_by_field_name(inner, "name", TS_LSP_FIELD_LEN("name"));
                    if (ts_node_is_null(name) || !ctx->module_qn)
                        continue;
                    char *fn = node_text(ctx, name);
                    if (!fn)
                        continue;
                    const char *fqn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, fn);
                    process_function_body(ctx, inner, fqn, NULL);
                } else if (strcmp(ik, "class_declaration") == 0) {
                    process_node(ctx, inner);
                }
            }
        }
        // Top-level expression_statement bodies are not graphed as functions; skip.
    }
}

// ── Initialization ────────────────────────────────────────────────────────────

void ts_lsp_init(TSLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                 const CBMTypeRegistry *registry, const char *module_qn, bool js_mode,
                 bool jsx_mode, bool dts_mode, CBMResolvedCallArray *out) {
    if (!ctx)
        return;
    memset(ctx, 0, sizeof(TSLSPContext));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->module_qn = module_qn;
    ctx->resolved_calls = out;
    ctx->js_mode = js_mode;
    ctx->jsx_mode = jsx_mode;
    ctx->dts_mode = dts_mode;
    ctx->current_scope = arena ? cbm_scope_push(arena, NULL) : NULL;

    const char *debug_env = getenv("CBM_LSP_DEBUG");
    ctx->debug = (debug_env && debug_env[0]);
}

void ts_lsp_add_import(TSLSPContext *ctx, const char *local_name, const char *module_qn) {
    if (!ctx || !ctx->arena || !local_name || !module_qn)
        return;
    int new_count = ctx->import_count + 1;
    const char **names =
        (const char **)cbm_arena_alloc(ctx->arena, (size_t)new_count * sizeof(const char *));
    const char **qns =
        (const char **)cbm_arena_alloc(ctx->arena, (size_t)new_count * sizeof(const char *));
    if (!names || !qns)
        return;
    for (int i = 0; i < ctx->import_count; i++) {
        names[i] = ctx->import_local_names ? ctx->import_local_names[i] : NULL;
        qns[i] = ctx->import_module_qns ? ctx->import_module_qns[i] : NULL;
    }
    names[ctx->import_count] = cbm_arena_strdup(ctx->arena, local_name);
    qns[ctx->import_count] = cbm_arena_strdup(ctx->arena, module_qn);
    ctx->import_local_names = names;
    ctx->import_module_qns = qns;
    ctx->import_count = new_count;
}

// ── Stdlib seeds (Phase 5 will replace with generator) ────────────────────────

static const CBMType *tt_builtin(CBMArena *a, const char *n) {
    return cbm_type_builtin(a, n);
}
static const CBMType *tt_param(CBMArena *a, const char *n) {
    return cbm_type_type_param(a, n);
}

static void ts_reg_method(CBMTypeRegistry *reg, CBMArena *arena, const char *recv_qn,
                          const char *name, const CBMType *ret_type) {
    CBMRegisteredFunc rf;
    memset(&rf, 0, sizeof(rf));
    rf.qualified_name = cbm_arena_sprintf(arena, "%s.%s", recv_qn, name);
    rf.short_name = cbm_arena_strdup(arena, name);
    rf.receiver_type = cbm_arena_strdup(arena, recv_qn);
    rf.min_params = -1;
    const CBMType *rets[2] = {ret_type ? ret_type : cbm_type_unknown(), NULL};
    rf.signature = cbm_type_func(arena, NULL, NULL, rets);
    cbm_registry_add_func(reg, rf);
}

static void reg_type_with_methods(CBMTypeRegistry *reg, CBMArena *arena, const char *qn,
                                  const char *short_name, const char **type_params,
                                  const char **method_names, const CBMType **method_returns) {
    CBMRegisteredType rt;
    memset(&rt, 0, sizeof(rt));
    rt.qualified_name = cbm_arena_strdup(arena, qn);
    rt.short_name = cbm_arena_strdup(arena, short_name);
    rt.type_param_names = type_params;
    if (method_names) {
        int count = 0;
        while (method_names[count])
            count++;
        const char **mqns =
            (const char **)cbm_arena_alloc(arena, (size_t)(count + 1) * sizeof(const char *));
        const char **mnames_copy =
            (const char **)cbm_arena_alloc(arena, (size_t)(count + 1) * sizeof(const char *));
        if (mqns && mnames_copy) {
            for (int i = 0; i < count; i++) {
                mnames_copy[i] = cbm_arena_strdup(arena, method_names[i]);
                mqns[i] = cbm_arena_sprintf(arena, "%s.%s", qn, method_names[i]);
            }
            mnames_copy[count] = NULL;
            mqns[count] = NULL;
            rt.method_names = mnames_copy;
            rt.method_qns = mqns;
        }
    }
    cbm_registry_add_type(reg, rt);

    if (method_names && method_returns) {
        for (int i = 0; method_names[i]; i++) {
            ts_reg_method(reg, arena, qn, method_names[i], method_returns[i]);
        }
    }
}

void cbm_ts_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    if (!reg || !arena)
        return;

    const CBMType *t_string = tt_builtin(arena, "string");
    const CBMType *t_number = tt_builtin(arena, "number");
    const CBMType *t_boolean = tt_builtin(arena, "boolean");
    const CBMType *t_void = tt_builtin(arena, "void");
    const CBMType *t_unknown = cbm_type_unknown();

    // Array<T>: minimal method surface for v1.
    {
        const CBMType *tparam_T = tt_param(arena, "T");
        const CBMType *tparam_U = tt_param(arena, "U");
        const char **tparams = (const char **)cbm_arena_alloc(arena, 2 * sizeof(const char *));
        tparams[0] = "T";
        tparams[1] = NULL;
        static const char *methods[] = {
            "push",    "pop",      "shift",   "unshift", "slice",  "splice", "concat",    "join",
            "indexOf", "includes", "forEach", "map",     "filter", "find",   "findIndex", "reduce",
            "some",    "every",    "sort",    "reverse", "at",     "flat",   "flatMap",   NULL,
        };
        const CBMType *returns[24];
        const CBMType *t_T = tparam_T;
        const CBMType *t_arr_T_args[2] = {t_T, NULL};
        const CBMType *t_arr_T = cbm_type_template(arena, "Array", t_arr_T_args, 1);
        returns[0] = t_number;  // push
        returns[1] = t_T;       // pop
        returns[2] = t_T;       // shift
        returns[3] = t_number;  // unshift
        returns[4] = t_arr_T;   // slice
        returns[5] = t_arr_T;   // splice
        returns[6] = t_arr_T;   // concat
        returns[7] = t_string;  // join
        returns[8] = t_number;  // indexOf
        returns[9] = t_boolean; // includes
        returns[10] = t_void;   // forEach
        returns[11] =
            cbm_type_template(arena, "Array", (const CBMType *[]){tparam_U, NULL}, 1); // map<U>
        returns[12] = t_arr_T;                                                         // filter
        returns[13] = t_T;                                                             // find
        returns[14] = t_number;                                                        // findIndex
        returns[15] = tparam_U;                                                        // reduce<U>
        returns[16] = t_boolean;                                                       // some
        returns[17] = t_boolean;                                                       // every
        returns[18] = t_arr_T;                                                         // sort
        returns[19] = t_arr_T;                                                         // reverse
        returns[20] = t_T;                                                             // at
        returns[21] = t_arr_T;                                                         // flat
        returns[22] = t_arr_T;                                                         // flatMap
        returns[23] = NULL;
        reg_type_with_methods(reg, arena, "Array", "Array", tparams, methods, returns);

        // Patch the registered Array.{forEach,map,filter,find,findIndex,some,every,
        // flatMap,sort,reduce} signatures with a typed callback (x: T) => U so that
        // contextual typing can bind the callback's first param to T inside the
        // closure body. We rebuild rf.signature carrying through the existing return
        // type. This unlocks `arr.map(x => x.length)`-style resolution.
        const char *p_names[] = {"callback", NULL};
        struct {
            const char *method;
            const CBMType *ret;
            const CBMType *cb_param0;
            const CBMType *cb_ret;
        } cb_specs[] = {
            {"forEach", t_void, t_T, t_void},
            {"map", returns[11], t_T, tparam_U},
            {"filter", t_arr_T, t_T, t_boolean},
            {"find", t_T, t_T, t_boolean},
            {"findIndex", t_number, t_T, t_boolean},
            {"some", t_boolean, t_T, t_boolean},
            {"every", t_boolean, t_T, t_boolean},
            {"flatMap", t_arr_T, t_T, tparam_U},
            {"sort", t_arr_T, t_T, t_number},
            {"reduce", tparam_U, t_T, tparam_U},
            {NULL, NULL, NULL, NULL},
        };
        for (int ci = 0; cb_specs[ci].method; ci++) {
            // Build callback FUNC type: (x: T) => returnType
            const CBMType *cb_param_types[2] = {cb_specs[ci].cb_param0, NULL};
            const CBMType *cb_returns[2] = {cb_specs[ci].cb_ret, NULL};
            const char *cb_param_names[2] = {"x", NULL};
            const CBMType *cb_func =
                cbm_type_func(arena, cb_param_names, cb_param_types, cb_returns);

            // Rebuild Array.<method>'s signature with this callback as param 0.
            const CBMType *outer_param_types[2] = {cb_func, NULL};
            const CBMType *outer_returns[2] = {cb_specs[ci].ret, NULL};
            const CBMType *new_sig =
                cbm_type_func(arena, p_names, outer_param_types, outer_returns);

            // Find Array.<method> in the registry and patch its signature.
            char *qn = cbm_arena_sprintf(arena, "Array.%s", cb_specs[ci].method);
            for (int fi = 0; fi < reg->func_count; fi++) {
                if (reg->funcs[fi].qualified_name &&
                    strcmp(reg->funcs[fi].qualified_name, qn) == 0) {
                    reg->funcs[fi].signature = new_sig;
                    break;
                }
            }
        }
    }

    // Promise<T>
    {
        const CBMType *tparam_T = tt_param(arena, "T");
        const char **tparams = (const char **)cbm_arena_alloc(arena, 2 * sizeof(const char *));
        tparams[0] = "T";
        tparams[1] = NULL;
        static const char *methods[] = {"then", "catch", "finally", NULL};
        const CBMType *returns[4];
        const CBMType *t_promise_T_args[2] = {tparam_T, NULL};
        const CBMType *t_promise_T = cbm_type_template(arena, "Promise", t_promise_T_args, 1);
        returns[0] = t_promise_T;
        returns[1] = t_promise_T;
        returns[2] = t_promise_T;
        returns[3] = NULL;
        reg_type_with_methods(reg, arena, "Promise", "Promise", tparams, methods, returns);
    }

    // Map<K,V>
    {
        const char **tparams = (const char **)cbm_arena_alloc(arena, 3 * sizeof(const char *));
        tparams[0] = "K";
        tparams[1] = "V";
        tparams[2] = NULL;
        static const char *methods[] = {"get", "set", "has", "delete", "clear", "forEach", NULL};
        const CBMType *returns[7] = {
            tt_param(arena, "V"),
            cbm_type_template(arena, "Map",
                              (const CBMType *[]){tt_param(arena, "K"), tt_param(arena, "V"), NULL},
                              2),
            t_boolean,
            t_boolean,
            t_void,
            t_void,
            NULL,
        };
        reg_type_with_methods(reg, arena, "Map", "Map", tparams, methods, returns);
    }

    // Set<T>
    {
        const char **tparams = (const char **)cbm_arena_alloc(arena, 2 * sizeof(const char *));
        tparams[0] = "T";
        tparams[1] = NULL;
        static const char *methods[] = {"add", "has", "delete", "clear", "forEach", NULL};
        const CBMType *returns[6] = {
            cbm_type_template(arena, "Set", (const CBMType *[]){tt_param(arena, "T"), NULL}, 1),
            t_boolean,
            t_boolean,
            t_void,
            t_void,
            NULL,
        };
        reg_type_with_methods(reg, arena, "Set", "Set", tparams, methods, returns);
    }

    // Iterator<T> / Iterable<T>
    {
        const CBMType *tparam_T = tt_param(arena, "T");
        const char **tparams = (const char **)cbm_arena_alloc(arena, 2 * sizeof(const char *));
        tparams[0] = "T";
        tparams[1] = NULL;
        // IteratorResult<T> is loose — we fold it to T-or-undefined via the iterator method's
        // return.
        static const char *iter_methods[] = {"next", "return", "throw", NULL};
        const CBMType *iter_returns[4] = {tparam_T, tparam_T, tparam_T, NULL};
        reg_type_with_methods(reg, arena, "Iterator", "Iterator", tparams, iter_methods,
                              iter_returns);

        const CBMType *iter_t =
            cbm_type_template(arena, "Iterator", (const CBMType *[]){tparam_T, NULL}, 1);
        static const char *iterable_methods[] = {NULL}; // [Symbol.iterator] not modeled by name
        const CBMType *iterable_returns[1] = {NULL};
        (void)iter_t;
        reg_type_with_methods(reg, arena, "Iterable", "Iterable", tparams, iterable_methods,
                              iterable_returns);
    }

    // AsyncIterator<T> / AsyncIterable<T> — methods return Promise<T>
    {
        const CBMType *tparam_T = tt_param(arena, "T");
        const char **tparams = (const char **)cbm_arena_alloc(arena, 2 * sizeof(const char *));
        tparams[0] = "T";
        tparams[1] = NULL;
        const CBMType *prom_T =
            cbm_type_template(arena, "Promise", (const CBMType *[]){tparam_T, NULL}, 1);
        static const char *methods[] = {"next", "return", "throw", NULL};
        const CBMType *returns[4] = {prom_T, prom_T, prom_T, NULL};
        reg_type_with_methods(reg, arena, "AsyncIterator", "AsyncIterator", tparams, methods,
                              returns);
        reg_type_with_methods(reg, arena, "AsyncIterable", "AsyncIterable", tparams, methods,
                              returns);
    }

    // Generator<T> — extends Iterator<T> with yield-based methods.
    {
        const CBMType *tparam_T = tt_param(arena, "T");
        const char **tparams = (const char **)cbm_arena_alloc(arena, 2 * sizeof(const char *));
        tparams[0] = "T";
        tparams[1] = NULL;
        static const char *methods[] = {"next", "return", "throw", NULL};
        const CBMType *returns[4] = {tparam_T, tparam_T, tparam_T, NULL};
        reg_type_with_methods(reg, arena, "Generator", "Generator", tparams, methods, returns);
        reg_type_with_methods(reg, arena, "AsyncGenerator", "AsyncGenerator", tparams, methods,
                              returns);
    }

    // String prototype methods (most common).
    {
        static const char *methods[] = {
            "charAt",      "charCodeAt",  "concat",     "includes",  "indexOf",
            "lastIndexOf", "padStart",    "padEnd",     "repeat",    "replace",
            "slice",       "split",       "startsWith", "endsWith",  "substring",
            "toLowerCase", "toUpperCase", "trim",       "trimStart", "trimEnd",
            "match",       "matchAll",    "normalize",  "at",        NULL,
        };
        // Simplified: most string methods return string or boolean — fold to string for v1.
        int mc = 0;
        while (methods[mc])
            mc++;
        const CBMType **returns =
            (const CBMType **)cbm_arena_alloc(arena, (size_t)(mc + 1) * sizeof(const CBMType *));
        for (int i = 0; i < mc; i++) {
            // booleans: includes, startsWith, endsWith
            const char *m = methods[i];
            if (strcmp(m, "includes") == 0 || strcmp(m, "startsWith") == 0 ||
                strcmp(m, "endsWith") == 0) {
                returns[i] = t_boolean;
            } else if (strcmp(m, "indexOf") == 0 || strcmp(m, "lastIndexOf") == 0 ||
                       strcmp(m, "charCodeAt") == 0) {
                returns[i] = t_number;
            } else if (strcmp(m, "split") == 0) {
                returns[i] =
                    cbm_type_template(arena, "Array", (const CBMType *[]){t_string, NULL}, 1);
            } else {
                returns[i] = t_string;
            }
        }
        returns[mc] = NULL;
        reg_type_with_methods(reg, arena, "String", "String", NULL, methods, returns);
    }

    // console — used so frequently that resolving `console.log` directly is high value.
    {
        static const char *methods[] = {"log", "error", "warn", "info", "debug", NULL};
        const CBMType *returns[6] = {t_void, t_void, t_void, t_void, t_void, NULL};
        reg_type_with_methods(reg, arena, "console", "console", NULL, methods, returns);
    }

    // JSON namespace.
    {
        static const char *methods[] = {"parse", "stringify", NULL};
        const CBMType *returns[3] = {t_unknown, t_string, NULL};
        reg_type_with_methods(reg, arena, "JSON", "JSON", NULL, methods, returns);
    }

    // Object — constructor namespace methods that are commonly chained.
    {
        static const char *methods[] = {"keys",     "values", "entries",        "assign", "freeze",
                                        "isFrozen", "create", "getPrototypeOf", NULL};
        const CBMType *returns[9] = {
            cbm_type_template(arena, "Array", (const CBMType *[]){t_string, NULL}, 1),
            cbm_type_template(arena, "Array", (const CBMType *[]){t_unknown, NULL}, 1),
            cbm_type_template(arena, "Array", (const CBMType *[]){t_unknown, NULL}, 1),
            t_unknown,
            t_unknown,
            t_boolean,
            t_unknown,
            t_unknown,
            NULL,
        };
        reg_type_with_methods(reg, arena, "Object", "Object", NULL, methods, returns);
    }

    // Number wrapper class (so `(42).toString()` and similar resolve).
    {
        static const char *methods[] = {
            "toString", "toFixed", "toExponential", "toPrecision", "valueOf", NULL,
        };
        const CBMType *returns[6] = {t_string, t_string, t_string, t_string, t_number, NULL};
        reg_type_with_methods(reg, arena, "Number", "Number", NULL, methods, returns);
    }

    // Boolean wrapper class.
    {
        static const char *methods[] = {"toString", "valueOf", NULL};
        const CBMType *returns[3] = {t_string, t_boolean, NULL};
        reg_type_with_methods(reg, arena, "Boolean", "Boolean", NULL, methods, returns);
    }

    // Date wrapper class — common date operations.
    {
        static const char *methods[] = {
            "toISOString", "toString", "toJSON",      "toDateString", "toTimeString",
            "getTime",     "valueOf",  "getFullYear", "getMonth",     "getDate",
            "getDay",      "getHours", "getMinutes",  "getSeconds",   "getMilliseconds",
            "setFullYear", "setMonth", "setDate",     "setHours",     "setMinutes",
            "setSeconds",  NULL,
        };
        int mc = 0;
        while (methods[mc])
            mc++;
        const CBMType **returns =
            (const CBMType **)cbm_arena_alloc(arena, (size_t)(mc + 1) * sizeof(const CBMType *));
        for (int i = 0; i < mc; i++) {
            const char *m = methods[i];
            if (strcmp(m, "toISOString") == 0 || strcmp(m, "toString") == 0 ||
                strcmp(m, "toJSON") == 0 || strcmp(m, "toDateString") == 0 ||
                strcmp(m, "toTimeString") == 0) {
                returns[i] = t_string;
            } else {
                returns[i] = t_number;
            }
        }
        returns[mc] = NULL;
        reg_type_with_methods(reg, arena, "Date", "Date", NULL, methods, returns);
    }

    // RegExp wrapper class.
    {
        static const char *methods[] = {"test", "exec", "toString", "compile", NULL};
        const CBMType *returns[5] = {t_boolean, t_unknown, t_string, t_unknown, NULL};
        reg_type_with_methods(reg, arena, "RegExp", "RegExp", NULL, methods, returns);
    }

    // Error class — used heavily in `throw new Error(...)` and catch handlers.
    {
        static const char *methods[] = {"toString", NULL};
        const CBMType *returns[2] = {t_string, NULL};
        reg_type_with_methods(reg, arena, "Error", "Error", NULL, methods, returns);
    }

    // Math namespace — static math operations.
    {
        static const char *methods[] = {
            "floor", "ceil", "round", "trunc",  "abs",   "sqrt", "cbrt", "pow",  "exp",
            "log",   "log2", "log10", "sin",    "cos",   "tan",  "asin", "acos", "atan",
            "atan2", "min",  "max",   "random", "hypot", "sign", NULL,
        };
        int mc = 0;
        while (methods[mc])
            mc++;
        const CBMType **returns =
            (const CBMType **)cbm_arena_alloc(arena, (size_t)(mc + 1) * sizeof(const CBMType *));
        for (int i = 0; i < mc; i++)
            returns[i] = t_number;
        returns[mc] = NULL;
        reg_type_with_methods(reg, arena, "Math", "Math", NULL, methods, returns);
    }

    // Static methods that don't fit reg_type_with_methods cleanly:
    // Promise.resolve(x): Promise<T> — generic in T derived from arg type
    // Promise.all(promises): Promise<T[]>
    // Promise.reject(reason): Promise<never>
    // Array.from(iter): Array<T>
    // Array.isArray(x): boolean
    // Array.of(...items): Array<T>
    {
        const CBMType *tparam_T = tt_param(arena, "T");
        const CBMType *t_T = tparam_T;
        const CBMType *prom_T =
            cbm_type_template(arena, "Promise", (const CBMType *[]){t_T, NULL}, 1);
        const CBMType *arr_T = cbm_type_template(arena, "Array", (const CBMType *[]){t_T, NULL}, 1);
        const CBMType *prom_arr_T =
            cbm_type_template(arena, "Promise", (const CBMType *[]){arr_T, NULL}, 1);
        ts_reg_method(reg, arena, "Promise", "resolve", prom_T);
        ts_reg_method(reg, arena, "Promise", "all", prom_arr_T);
        ts_reg_method(reg, arena, "Promise", "reject", prom_T);
        ts_reg_method(reg, arena, "Promise", "race", prom_T);
        ts_reg_method(reg, arena, "Promise", "allSettled", prom_arr_T);

        ts_reg_method(reg, arena, "Array", "from", arr_T);
        ts_reg_method(reg, arena, "Array", "isArray", t_boolean);
        ts_reg_method(reg, arena, "Array", "of", arr_T);
    }

    // ── DOM essentials ───────────────────────────────────────────────────────
    // Element + Document + Window + Event + Response cover the most-used DOM
    // surface for browser TS code. Hand-curated subset; full lib.dom.d.ts would
    // be Phase 5 (generator).
    {
        // EventTarget base
        static const char *et_methods[] = {
            "addEventListener",
            "removeEventListener",
            "dispatchEvent",
            NULL,
        };
        const CBMType *et_returns[4] = {t_void, t_void, t_boolean, NULL};
        reg_type_with_methods(reg, arena, "EventTarget", "EventTarget", NULL, et_methods,
                              et_returns);

        // Node
        static const char *node_methods[] = {
            "appendChild",   "removeChild",      "replaceChild",        "cloneNode",     "contains",
            "hasChildNodes", "addEventListener", "removeEventListener", "dispatchEvent", NULL,
        };
        const CBMType *node_t = cbm_type_named(arena, "Node");
        const CBMType *node_returns[10] = {
            node_t, node_t, node_t, node_t, t_boolean, t_boolean, t_void, t_void, t_boolean, NULL,
        };
        reg_type_with_methods(reg, arena, "Node", "Node", NULL, node_methods, node_returns);

        // Element
        static const char *el_methods[] = {
            "getAttribute",
            "setAttribute",
            "removeAttribute",
            "hasAttribute",
            "querySelector",
            "querySelectorAll",
            "closest",
            "matches",
            "appendChild",
            "removeChild",
            "replaceChild",
            "addEventListener",
            "removeEventListener",
            "dispatchEvent",
            "getBoundingClientRect",
            "scrollIntoView",
            "focus",
            "blur",
            "click",
            "remove",
            NULL,
        };
        const CBMType *el_t = cbm_type_named(arena, "Element");
        const CBMType *nodelist_t =
            cbm_type_template(arena, "NodeList", (const CBMType *[]){el_t, NULL}, 1);
        const CBMType *el_returns[21] = {
            t_string,  t_void, t_void, t_boolean, el_t,   nodelist_t, el_t,
            t_boolean, el_t,   el_t,   el_t,      t_void, t_void,     t_boolean,
            t_unknown, t_void, t_void, t_void,    t_void, t_void,     NULL,
        };
        reg_type_with_methods(reg, arena, "Element", "Element", NULL, el_methods, el_returns);

        // HTMLElement (extends Element)
        static const char *html_methods[] = {
            "getAttribute",
            "setAttribute",
            "querySelector",
            "querySelectorAll",
            "closest",
            "addEventListener",
            "removeEventListener",
            "click",
            "focus",
            "blur",
            "remove",
            NULL,
        };
        const CBMType *html_t = cbm_type_named(arena, "HTMLElement");
        const CBMType *html_returns[12] = {
            t_string, t_void,
            html_t,   cbm_type_template(arena, "NodeList", (const CBMType *[]){html_t, NULL}, 1),
            html_t,   t_void,
            t_void,   t_void,
            t_void,   t_void,
            t_void,   NULL,
        };
        reg_type_with_methods(reg, arena, "HTMLElement", "HTMLElement", NULL, html_methods,
                              html_returns);

        // Document
        static const char *doc_methods[] = {
            "getElementById",
            "querySelector",
            "querySelectorAll",
            "getElementsByClassName",
            "getElementsByTagName",
            "createElement",
            "createTextNode",
            "createDocumentFragment",
            "addEventListener",
            "removeEventListener",
            NULL,
        };
        const CBMType *doc_returns[11] = {
            html_t,
            html_t,
            cbm_type_template(arena, "NodeList", (const CBMType *[]){html_t, NULL}, 1),
            cbm_type_template(arena, "HTMLCollection", (const CBMType *[]){html_t, NULL}, 1),
            cbm_type_template(arena, "HTMLCollection", (const CBMType *[]){html_t, NULL}, 1),
            html_t,
            node_t,
            node_t,
            t_void,
            t_void,
            NULL,
        };
        reg_type_with_methods(reg, arena, "Document", "Document", NULL, doc_methods, doc_returns);

        // Event
        static const char *event_methods[] = {
            "preventDefault", "stopPropagation", "stopImmediatePropagation", "composedPath", NULL,
        };
        const CBMType *event_returns[5] = {
            t_void,
            t_void,
            t_void,
            cbm_type_template(
                arena, "Array",
                (const CBMType *[]){
                    et_methods[0] ? cbm_type_named(arena, "EventTarget") : t_unknown, NULL},
                1),
            NULL,
        };
        reg_type_with_methods(reg, arena, "Event", "Event", NULL, event_methods, event_returns);

        // Response (fetch API)
        static const char *resp_methods[] = {
            "json", "text", "blob", "arrayBuffer", "formData", "clone", NULL,
        };
        const CBMType *resp_returns[7] = {
            cbm_type_template(arena, "Promise", (const CBMType *[]){t_unknown, NULL}, 1),
            cbm_type_template(arena, "Promise", (const CBMType *[]){t_string, NULL}, 1),
            cbm_type_template(arena, "Promise", (const CBMType *[]){t_unknown, NULL}, 1),
            cbm_type_template(arena, "Promise", (const CBMType *[]){t_unknown, NULL}, 1),
            cbm_type_template(arena, "Promise", (const CBMType *[]){t_unknown, NULL}, 1),
            cbm_type_named(arena, "Response"),
            NULL,
        };
        reg_type_with_methods(reg, arena, "Response", "Response", NULL, resp_methods, resp_returns);

        // Window
        static const char *win_methods[] = {
            "addEventListener",
            "removeEventListener",
            "alert",
            "confirm",
            "prompt",
            "fetch",
            "setTimeout",
            "clearTimeout",
            "setInterval",
            "clearInterval",
            "requestAnimationFrame",
            "cancelAnimationFrame",
            NULL,
        };
        const CBMType *win_returns[13] = {
            t_void,
            t_void,
            t_void,
            t_boolean,
            t_string,
            cbm_type_template(arena, "Promise",
                              (const CBMType *[]){cbm_type_named(arena, "Response"), NULL}, 1),
            t_number,
            t_void,
            t_number,
            t_void,
            t_number,
            t_void,
            NULL,
        };
        reg_type_with_methods(reg, arena, "Window", "Window", NULL, win_methods, win_returns);
    }
}

// ── Single-file entry point ───────────────────────────────────────────────────

// Build a registry from result->defs: register Class/Interface as types; Function/Method
// as funcs. Walks AST briefly for class fields/extends.
static void register_file_defs(CBMArena *arena, CBMTypeRegistry *reg, CBMFileResult *result,
                               const char *module_qn) {
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name || !d->label)
            continue;

        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->name;
            rt.is_interface = (strcmp(d->label, "Interface") == 0);
            // base_classes → embedded_types (extends/implements).
            if (d->base_classes) {
                int bc = 0;
                while (d->base_classes[bc])
                    bc++;
                if (bc > 0) {
                    const char **emb = (const char **)cbm_arena_alloc(
                        arena, (size_t)(bc + 1) * sizeof(const char *));
                    if (emb) {
                        for (int j = 0; j < bc; j++) {
                            const char *b = d->base_classes[j];
                            // For TS, extract_defs returns base text like "extends Animal"
                            // or "extends Foo<Bar>" (raw class_heritage node text). Strip
                            // the keyword and pull the first identifier.
                            if (b) {
                                while (*b == ' ' || *b == '\t' || *b == '\n')
                                    b++;
                                if (strncmp(b, "extends", 7) == 0 &&
                                    (b[7] == ' ' || b[7] == '\t')) {
                                    b += 7;
                                }
                                if (strncmp(b, "implements", 10) == 0 &&
                                    (b[10] == ' ' || b[10] == '\t')) {
                                    b += 10;
                                }
                                while (*b == ' ' || *b == '\t' || *b == '\n')
                                    b++;
                                size_t bl = 0;
                                while (b[bl] && b[bl] != ' ' && b[bl] != ',' && b[bl] != '<' &&
                                       b[bl] != '(' && b[bl] != '\n' && b[bl] != '\t')
                                    bl++;
                                if (bl == 0) {
                                    emb[j] = NULL;
                                    continue;
                                }
                                char *base_name = cbm_arena_strndup(arena, b, bl);
                                if (strchr(base_name, '.') == NULL) {
                                    emb[j] =
                                        cbm_arena_sprintf(arena, "%s.%s", module_qn, base_name);
                                } else {
                                    emb[j] = base_name;
                                }
                            } else {
                                emb[j] = NULL;
                            }
                        }
                        emb[bc] = NULL;
                        rt.embedded_types = emb;
                    }
                }
            }
            cbm_registry_add_type(reg, rt);
        } else if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;
            rf.min_params = -1;

            // Build return type list.
            //
            // For TS, prefer the RAW d->return_type text over d->return_types[]: the
            // latter is post-cleaned via extract_defs.c::clean_type_name which truncates
            // at the first '<' (so `Promise<number>` becomes `Promise`, losing the
            // generic args). The raw text preserves generics; parse_ts_type_text strips
            // the leading `:` annotation prefix.
            const CBMType **rets = NULL;
            if (d->return_type && d->return_type[0]) {
                rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
                if (rets) {
                    rets[0] = parse_ts_type_text(arena, d->return_type, module_qn);
                    rets[1] = NULL;
                }
            } else if (d->return_types) {
                int n = 0;
                while (d->return_types[n])
                    n++;
                if (n > 0) {
                    rets = (const CBMType **)cbm_arena_alloc(arena, (size_t)(n + 1) *
                                                                        sizeof(const CBMType *));
                    if (rets) {
                        for (int j = 0; j < n; j++) {
                            rets[j] = parse_ts_type_text(arena, d->return_types[j], module_qn);
                        }
                        rets[n] = NULL;
                    }
                }
            }
            const CBMType **params_t = parse_param_types_array(arena, d->param_types, module_qn);
            rf.signature = cbm_type_func(arena, d->param_names, params_t, rets);

            // Methods: deduce receiver from QN ("module.Class.method" → receiver "module.Class").
            if (strcmp(d->label, "Method") == 0) {
                const char *dot = strrchr(d->qualified_name, '.');
                if (dot && dot != d->qualified_name) {
                    char *recv = cbm_arena_strndup(arena, d->qualified_name,
                                                   (size_t)(dot - d->qualified_name));
                    rf.receiver_type = recv;

                    // Ensure the receiver type is registered (auto-create if missing).
                    const CBMRegisteredType *existing = cbm_registry_lookup_type(reg, recv);
                    if (!existing) {
                        CBMRegisteredType auto_rt;
                        memset(&auto_rt, 0, sizeof(auto_rt));
                        auto_rt.qualified_name = recv;
                        const char *short_name = strrchr(recv, '.');
                        auto_rt.short_name = short_name ? short_name + 1 : recv;
                        cbm_registry_add_type(reg, auto_rt);
                    }
                }
            }

            cbm_registry_add_func(reg, rf);
        }
    }

    // Second pass: attach method_names / method_qns to each registered type from
    // the registered methods. Lets dispatch find them via lookup_member_type.
    for (int ti = 0; ti < reg->type_count; ti++) {
        CBMRegisteredType *rt = &reg->types[ti];
        if (!rt->qualified_name)
            continue;

        // Count methods first.
        int mcount = 0;
        for (int fi = 0; fi < reg->func_count; fi++) {
            const CBMRegisteredFunc *f = &reg->funcs[fi];
            if (f->receiver_type && strcmp(f->receiver_type, rt->qualified_name) == 0) {
                mcount++;
            }
        }
        if (mcount == 0)
            continue;

        const char **mnames =
            (const char **)cbm_arena_alloc(arena, (size_t)(mcount + 1) * sizeof(const char *));
        const char **mqns =
            (const char **)cbm_arena_alloc(arena, (size_t)(mcount + 1) * sizeof(const char *));
        if (!mnames || !mqns)
            continue;
        int idx = 0;
        for (int fi = 0; fi < reg->func_count && idx < mcount; fi++) {
            const CBMRegisteredFunc *f = &reg->funcs[fi];
            if (f->receiver_type && strcmp(f->receiver_type, rt->qualified_name) == 0) {
                mnames[idx] = f->short_name;
                mqns[idx] = f->qualified_name;
                idx++;
            }
        }
        mnames[mcount] = NULL;
        mqns[mcount] = NULL;
        rt->method_names = mnames;
        rt->method_qns = mqns;
    }
}

// Find the first `return EXPR` inside a body and evaluate EXPR's type. Returns
// UNKNOWN if the body has no return or every return is unresolvable.
static const CBMType *infer_return_type_from_body(TSLSPContext *ctx, TSNode body) {
    if (ts_node_is_null(body))
        return cbm_type_unknown();

    // Iterative DFS using a small fixed-size stack to avoid C-stack blowup on big bodies.
    enum { STACK_CAP = 256 };
    TSNode stack[STACK_CAP];
    int top = 0;
    stack[top++] = body;
    while (top > 0) {
        TSNode n = stack[--top];
        if (ts_node_is_null(n))
            continue;
        const char *k = ts_node_type(n);
        if (strcmp(k, "return_statement") == 0) {
            // The first named child is the return expression (if any).
            uint32_t rnc = ts_node_named_child_count(n);
            if (rnc == 0)
                continue;
            TSNode expr = ts_node_named_child(n, 0);
            if (ts_node_is_null(expr))
                continue;
            const CBMType *t = ts_eval_expr_type(ctx, expr);
            if (t && !cbm_type_is_unknown(t))
                return t;
            continue;
        }
        // Don't recurse into nested function bodies — their returns belong to them.
        if (strcmp(k, "function_declaration") == 0 || strcmp(k, "function_expression") == 0 ||
            strcmp(k, "arrow_function") == 0 || strcmp(k, "method_definition") == 0)
            continue;

        uint32_t cnt = ts_node_child_count(n);
        for (uint32_t i = 0; i < cnt && top < STACK_CAP; i++) {
            stack[top++] = ts_node_child(n, i);
        }
    }
    return cbm_type_unknown();
}

// Rebuild a function's CBMRegisteredFunc signature from the AST directly. extract_defs
// only handles type_identifier / generic_type / predefined_type for params, so function
// types (`(x: Foo) => void`), object types, tuple types etc. are missing from
// def->param_types. This pass walks the AST and rebuilds signatures from scratch.
//
// Called after `register_file_defs` so the func entries already exist; we only mutate
// in place.
static void rebuild_signatures_from_ast(TSLSPContext *ctx, TSNode root, CBMTypeRegistry *reg) {
    if (ts_node_is_null(root) || !reg || !ctx->module_qn)
        return;

    enum { STACK_CAP = 256 };
    TSNode stack[STACK_CAP];
    int top = 0;
    uint32_t nc = ts_node_child_count(root);
    for (uint32_t i = 0; i < nc && top < STACK_CAP; i++)
        stack[top++] = ts_node_child(root, i);

    while (top > 0) {
        TSNode n = stack[--top];
        if (ts_node_is_null(n))
            continue;
        const char *k = ts_node_type(n);

        // Recurse into export_statement and class bodies.
        if (strcmp(k, "export_statement") == 0 || strcmp(k, "class_body") == 0) {
            uint32_t cnt = ts_node_child_count(n);
            for (uint32_t i = 0; i < cnt && top < STACK_CAP; i++) {
                stack[top++] = ts_node_child(n, i);
            }
            continue;
        }
        if (strcmp(k, "class_declaration") == 0) {
            TSNode body = ts_node_child_by_field_name(n, "body", TS_LSP_FIELD_LEN("body"));
            if (!ts_node_is_null(body) && top < STACK_CAP)
                stack[top++] = body;
            continue;
        }

        bool is_func = (strcmp(k, "function_declaration") == 0);
        bool is_method = (strcmp(k, "method_definition") == 0);
        if (!is_func && !is_method)
            continue;

        TSNode name_node = ts_node_child_by_field_name(n, "name", TS_LSP_FIELD_LEN("name"));
        if (ts_node_is_null(name_node))
            continue;
        char *fname = node_text(ctx, name_node);
        if (!fname)
            continue;

        // Resolve this declaration's FQN to find its registered func.
        const char *fqn = NULL;
        if (is_func) {
            fqn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, fname);
        } else {
            // Method: climb to enclosing class_declaration.
            TSNode parent = ts_node_parent(n);
            while (!ts_node_is_null(parent) &&
                   strcmp(ts_node_type(parent), "class_declaration") != 0) {
                parent = ts_node_parent(parent);
            }
            if (ts_node_is_null(parent))
                continue;
            TSNode cn = ts_node_child_by_field_name(parent, "name", TS_LSP_FIELD_LEN("name"));
            if (ts_node_is_null(cn))
                continue;
            char *cname = node_text(ctx, cn);
            if (!cname)
                continue;
            fqn = cbm_arena_sprintf(ctx->arena, "%s.%s.%s", ctx->module_qn, cname, fname);
        }

        // Rebuild parameters from AST.
        TSNode params =
            ts_node_child_by_field_name(n, "parameters", TS_LSP_FIELD_LEN("parameters"));
        const char *p_names[32] = {0};
        const CBMType *p_types[32] = {0};
        int pc = 0;
        if (!ts_node_is_null(params)) {
            uint32_t pnc = ts_node_named_child_count(params);
            for (uint32_t i = 0; i < pnc && pc < 31; i++) {
                TSNode p = ts_node_named_child(params, i);
                if (ts_node_is_null(p))
                    continue;
                const char *pk = ts_node_type(p);
                if (strcmp(pk, "required_parameter") != 0 && strcmp(pk, "optional_parameter") != 0)
                    continue;

                // Param name.
                TSNode pat = ts_node_child_by_field_name(p, "pattern", TS_LSP_FIELD_LEN("pattern"));
                if (ts_node_is_null(pat)) {
                    pat = ts_node_child_by_field_name(p, "name", TS_LSP_FIELD_LEN("name"));
                }
                if (ts_node_is_null(pat) || strcmp(ts_node_type(pat), "identifier") != 0)
                    continue;
                char *pn = node_text(ctx, pat);
                if (!pn)
                    continue;

                // Param type.
                const CBMType *pt = cbm_type_unknown();
                TSNode tann = ts_node_child_by_field_name(p, "type", TS_LSP_FIELD_LEN("type"));
                if (ts_node_is_null(tann)) {
                    // Fall back to kind-based search.
                    uint32_t pcnt = ts_node_named_child_count(p);
                    for (uint32_t j = 0; j < pcnt; j++) {
                        TSNode c = ts_node_named_child(p, j);
                        if (!ts_node_is_null(c) &&
                            strcmp(ts_node_type(c), "type_annotation") == 0) {
                            tann = c;
                            break;
                        }
                    }
                }
                if (!ts_node_is_null(tann)) {
                    TSNode tch = (strcmp(ts_node_type(tann), "type_annotation") == 0)
                                     ? ts_node_named_child(tann, 0)
                                     : tann;
                    if (!ts_node_is_null(tch))
                        pt = ts_parse_type_node(ctx, tch);
                }

                p_names[pc] = pn;
                p_types[pc] = pt;
                pc++;
            }
        }
        p_names[pc] = NULL;
        p_types[pc] = NULL;

        // Rebuild return type from AST.
        const CBMType *ret = cbm_type_unknown();
        TSNode rt_node =
            ts_node_child_by_field_name(n, "return_type", TS_LSP_FIELD_LEN("return_type"));
        if (!ts_node_is_null(rt_node)) {
            TSNode tch = (strcmp(ts_node_type(rt_node), "type_annotation") == 0)
                             ? ts_node_named_child(rt_node, 0)
                             : rt_node;
            if (!ts_node_is_null(tch))
                ret = ts_parse_type_node(ctx, tch);
        }

        // Build new signature and patch in registry.
        const CBMType *rets[2] = {ret, NULL};
        const CBMType *new_sig = cbm_type_func(ctx->arena, p_names, p_types, rets);
        for (int fi = 0; fi < reg->func_count; fi++) {
            if (!reg->funcs[fi].qualified_name)
                continue;
            if (strcmp(reg->funcs[fi].qualified_name, fqn) != 0)
                continue;
            // Preserve the ret type if AST gave UNKNOWN but the existing reg sig had one.
            if (cbm_type_is_unknown(ret) && reg->funcs[fi].signature &&
                reg->funcs[fi].signature->kind == CBM_TYPE_FUNC &&
                reg->funcs[fi].signature->data.func.return_types &&
                reg->funcs[fi].signature->data.func.return_types[0] &&
                !cbm_type_is_unknown(reg->funcs[fi].signature->data.func.return_types[0])) {
                const CBMType *preserved[2] = {reg->funcs[fi].signature->data.func.return_types[0],
                                               NULL};
                new_sig = cbm_type_func(ctx->arena, p_names, p_types, preserved);
            }
            reg->funcs[fi].signature = new_sig;
            break;
        }
    }
}

// Convert NAMED references to TYPE_PARAM for any function whose declaration has a
// `type_parameters` block. Without this, generic functions like `function id<T>(x: T): T`
// register T as NAMED("module.T") instead of TYPE_PARAM("T"), defeating substitution.
static void convert_signature_type_params(TSLSPContext *ctx, TSNode root, CBMTypeRegistry *reg) {
    if (ts_node_is_null(root) || !reg || !ctx->module_qn)
        return;

    // Walk: function_declaration, class_declaration { method_definition }, plus exported.
    enum { STACK_CAP = 256 };
    TSNode stack[STACK_CAP];
    int top = 0;
    uint32_t nc = ts_node_child_count(root);
    for (uint32_t i = 0; i < nc && top < STACK_CAP; i++)
        stack[top++] = ts_node_child(root, i);

    while (top > 0) {
        TSNode n = stack[--top];
        if (ts_node_is_null(n))
            continue;
        const char *k = ts_node_type(n);

        // Recurse into export_statement and class_body.
        if (strcmp(k, "export_statement") == 0 || strcmp(k, "class_body") == 0) {
            uint32_t cnt = ts_node_child_count(n);
            for (uint32_t i = 0; i < cnt && top < STACK_CAP; i++) {
                stack[top++] = ts_node_child(n, i);
            }
            continue;
        }
        if (strcmp(k, "class_declaration") == 0) {
            TSNode body = ts_node_child_by_field_name(n, "body", TS_LSP_FIELD_LEN("body"));
            if (!ts_node_is_null(body) && top < STACK_CAP)
                stack[top++] = body;
            continue;
        }

        bool is_func = (strcmp(k, "function_declaration") == 0);
        bool is_method = (strcmp(k, "method_definition") == 0);
        if (!is_func && !is_method)
            continue;

        TSNode tparams =
            ts_node_child_by_field_name(n, "type_parameters", TS_LSP_FIELD_LEN("type_parameters"));
        if (ts_node_is_null(tparams))
            continue;

        // Collect type param names.
        const char *names[16] = {0};
        const CBMType *args[16] = {0};
        int tpc = 0;
        uint32_t tnc = ts_node_named_child_count(tparams);
        for (uint32_t i = 0; i < tnc && tpc < 15; i++) {
            TSNode tp = ts_node_named_child(tparams, i);
            if (ts_node_is_null(tp))
                continue;
            // type_parameter has child name (type_identifier or identifier).
            TSNode tname = ts_node_child_by_field_name(tp, "name", TS_LSP_FIELD_LEN("name"));
            if (ts_node_is_null(tname)) {
                // First named child fallback.
                if (ts_node_named_child_count(tp) > 0) {
                    tname = ts_node_named_child(tp, 0);
                }
            }
            if (ts_node_is_null(tname))
                continue;
            char *nm = node_text(ctx, tname);
            if (!nm)
                continue;
            names[tpc] = nm;
            args[tpc] = cbm_type_type_param(ctx->arena, nm);
            tpc++;
        }
        names[tpc] = NULL;
        args[tpc] = NULL;
        if (tpc == 0)
            continue;

        // Resolve the function's QN.
        TSNode name_node = ts_node_child_by_field_name(n, "name", TS_LSP_FIELD_LEN("name"));
        if (ts_node_is_null(name_node))
            continue;
        char *fname = node_text(ctx, name_node);
        if (!fname)
            continue;

        // For methods, the receiver class must be in QN. For free functions, just module.
        const char *fqn = NULL;
        if (is_func) {
            fqn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, fname);
        } else {
            // Climb to enclosing class_declaration.
            TSNode parent = ts_node_parent(n);
            while (!ts_node_is_null(parent) &&
                   strcmp(ts_node_type(parent), "class_declaration") != 0) {
                parent = ts_node_parent(parent);
            }
            if (ts_node_is_null(parent))
                continue;
            TSNode cn = ts_node_child_by_field_name(parent, "name", TS_LSP_FIELD_LEN("name"));
            if (ts_node_is_null(cn))
                continue;
            char *cname = node_text(ctx, cn);
            if (!cname)
                continue;
            fqn = cbm_arena_sprintf(ctx->arena, "%s.%s.%s", ctx->module_qn, cname, fname);
        }

        // Patch the registered func's signature: rewrite NAMED → TYPE_PARAM.
        for (int fi = 0; fi < reg->func_count; fi++) {
            if (!reg->funcs[fi].qualified_name)
                continue;
            if (strcmp(reg->funcs[fi].qualified_name, fqn) != 0)
                continue;
            const CBMType *old_sig = reg->funcs[fi].signature;
            if (!old_sig)
                break;
            const CBMType *new_sig = cbm_type_substitute(ctx->arena, old_sig, names, args);
            if (new_sig)
                reg->funcs[fi].signature = new_sig;
            // Also store the type_param_names for later overload-by-types use.
            const char **stored = (const char **)cbm_arena_alloc(
                ctx->arena, (size_t)(tpc + 1) * sizeof(const char *));
            if (stored) {
                for (int ti = 0; ti < tpc; ti++)
                    stored[ti] = names[ti];
                stored[tpc] = NULL;
                reg->funcs[fi].type_param_names = stored;
            }
            break;
        }
    }
}

// Parse a JSDoc comment block for `@param {T} name` and `@returns {T}` tags. For each
// match, returns the type as a parsed CBMType. Used in `js_mode` to infer signatures
// for JavaScript files where there are no inline type annotations.
//
// Pragmatic v1: scans the comment text linearly. Handles:
//   * @param {T} name — description
//   * @returns {T} — description
//   * @return {T}
// Skips: nested braces in the type, complex JSDoc syntax (callback definitions,
//        @typedef, etc.). Falls back gracefully on UNKNOWN.
typedef struct {
    const char *name;
    const CBMType *type;
} JSDocParam;

static void parse_jsdoc_block(TSLSPContext *ctx, const char *text, size_t len,
                              JSDocParam *out_params, int *out_param_count, int max_params,
                              const CBMType **out_return) {
    *out_return = NULL;
    *out_param_count = 0;
    if (!text || len == 0)
        return;

    const char *end = text + len;
    const char *p = text;
    while (p < end) {
        // Find next `@` that introduces a tag.
        while (p < end && *p != '@')
            p++;
        if (p >= end)
            return;
        p++; // skip @

        // Match "param" or "returns"/"return".
        const char *tag_start = p;
        while (p < end && (*p >= 'a' && *p <= 'z'))
            p++;
        size_t tag_len = (size_t)(p - tag_start);
        bool is_param = (tag_len == 5 && memcmp(tag_start, "param", 5) == 0);
        bool is_return = ((tag_len == 7 && memcmp(tag_start, "returns", 7) == 0) ||
                          (tag_len == 6 && memcmp(tag_start, "return", 6) == 0));
        if (!is_param && !is_return)
            continue;

        // Skip whitespace.
        while (p < end && (*p == ' ' || *p == '\t'))
            p++;
        if (p >= end || *p != '{')
            continue;
        p++; // skip {
        const char *type_start = p;
        int depth = 1;
        while (p < end && depth > 0) {
            if (*p == '{')
                depth++;
            else if (*p == '}')
                depth--;
            if (depth > 0)
                p++;
        }
        if (p >= end || *p != '}')
            continue;
        size_t type_len = (size_t)(p - type_start);
        char *type_text = cbm_arena_strndup(ctx->arena, type_start, type_len);
        const CBMType *parsed = parse_ts_type_text(ctx->arena, type_text, ctx->module_qn);
        p++; // skip }

        if (is_return) {
            *out_return = parsed;
            continue;
        }

        // Param: skip whitespace then read name.
        while (p < end && (*p == ' ' || *p == '\t'))
            p++;
        const char *name_start = p;
        while (p < end && ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                           (*p >= '0' && *p <= '9') || *p == '_' || *p == '$' || *p == '.'))
            p++;
        size_t name_len = (size_t)(p - name_start);
        if (name_len == 0)
            continue;
        if (*out_param_count < max_params) {
            char *nm = cbm_arena_strndup(ctx->arena, name_start, name_len);
            out_params[*out_param_count].name = nm;
            out_params[*out_param_count].type = parsed;
            (*out_param_count)++;
        }
    }
}

// For each function_declaration in `js_mode` with a JSDoc-style comment immediately
// before it, parse the comment and patch the registered func's signature.
static void apply_jsdoc_signatures(TSLSPContext *ctx, TSNode root, CBMTypeRegistry *reg) {
    if (!ctx->js_mode || ts_node_is_null(root) || !reg || !ctx->module_qn)
        return;

    uint32_t kn = 0;
    TSNode *kids = collect_children(ctx->arena, root, &kn);
    for (uint32_t i = 0; i < kn; i++) {
        TSNode n = kids[i];
        if (strcmp(ts_node_type(n), "function_declaration") != 0)
            continue;

        // Find the immediate preceding comment sibling (skip whitespace nodes which
        // tree-sitter doesn't expose as named children, but unnamed children may include
        // comment nodes).
        TSNode prev = {0};
        for (int j = (int)i - 1; j >= 0; j--) {
            TSNode candidate = kids[j];
            const char *ck = ts_node_type(candidate);
            if (strcmp(ck, "comment") == 0) {
                prev = candidate;
                break;
            }
            // Anything else: stop — JSDoc must be immediately preceding.
            if (ts_node_is_named(candidate))
                break;
        }
        if (ts_node_is_null(prev))
            continue;

        // Get comment text and check JSDoc shape (`/** ... */`).
        char *ctext = node_text(ctx, prev);
        if (!ctext)
            continue;
        size_t clen = strlen(ctext);
        if (clen < 4 || ctext[0] != '/' || ctext[1] != '*' || ctext[2] != '*')
            continue;

        JSDocParam params[16];
        int pcount = 0;
        const CBMType *ret = NULL;
        parse_jsdoc_block(ctx, ctext + 3, clen - 3, params, &pcount, 16, &ret);

        // Resolve the function's QN and patch the registered signature.
        TSNode name_node = ts_node_child_by_field_name(n, "name", TS_LSP_FIELD_LEN("name"));
        if (ts_node_is_null(name_node))
            continue;
        char *fname = node_text(ctx, name_node);
        if (!fname)
            continue;
        const char *fqn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, fname);

        for (int fi = 0; fi < reg->func_count; fi++) {
            if (!reg->funcs[fi].qualified_name)
                continue;
            if (strcmp(reg->funcs[fi].qualified_name, fqn) != 0)
                continue;
            const CBMType *old_sig = reg->funcs[fi].signature;
            const CBMType **rets = NULL;
            if (ret && !cbm_type_is_unknown(ret)) {
                rets = (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(const CBMType *));
                if (rets) {
                    rets[0] = ret;
                    rets[1] = NULL;
                }
            } else if (old_sig && old_sig->kind == CBM_TYPE_FUNC) {
                rets = old_sig->data.func.return_types;
            }
            const CBMType **params_arr = NULL;
            const char **param_names_arr = NULL;
            if (pcount > 0) {
                params_arr = (const CBMType **)cbm_arena_alloc(
                    ctx->arena, (size_t)(pcount + 1) * sizeof(const CBMType *));
                param_names_arr = (const char **)cbm_arena_alloc(
                    ctx->arena, (size_t)(pcount + 1) * sizeof(const char *));
                if (params_arr && param_names_arr) {
                    for (int j = 0; j < pcount; j++) {
                        params_arr[j] = params[j].type;
                        param_names_arr[j] = params[j].name;
                    }
                    params_arr[pcount] = NULL;
                    param_names_arr[pcount] = NULL;
                }
            } else if (old_sig && old_sig->kind == CBM_TYPE_FUNC) {
                params_arr = old_sig->data.func.param_types;
                param_names_arr = old_sig->data.func.param_names;
            }
            reg->funcs[fi].signature = cbm_type_func(ctx->arena, param_names_arr, params_arr, rets);
            break;
        }
    }
}

// For each function_declaration whose registered signature has no return type, infer
// it from the body and rebuild the signature. This mirrors typescript-go's
// implicit return-type inference at a coarse level (single-return common case).
static void infer_implicit_returns(TSLSPContext *ctx, TSNode root, CBMTypeRegistry *reg) {
    if (ts_node_is_null(root) || !reg || !ctx->module_qn)
        return;
    uint32_t kn = 0;
    TSNode *kids = collect_children(ctx->arena, root, &kn);

    for (uint32_t i = 0; i < kn; i++) {
        TSNode n = kids[i];
        const char *k = ts_node_type(n);

        TSNode decl = n;
        if (strcmp(k, "export_statement") == 0) {
            TSNode d =
                ts_node_child_by_field_name(n, "declaration", TS_LSP_FIELD_LEN("declaration"));
            if (!ts_node_is_null(d)) {
                decl = d;
                k = ts_node_type(decl);
            }
        }

        if (strcmp(k, "function_declaration") != 0)
            continue;

        TSNode name_node = ts_node_child_by_field_name(decl, "name", TS_LSP_FIELD_LEN("name"));
        TSNode body = ts_node_child_by_field_name(decl, "body", TS_LSP_FIELD_LEN("body"));
        if (ts_node_is_null(name_node) || ts_node_is_null(body))
            continue;

        char *fname = node_text(ctx, name_node);
        if (!fname)
            continue;
        const char *fqn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, fname);
        const CBMRegisteredFunc *existing = cbm_registry_lookup_func(reg, fqn);
        if (!existing)
            continue;
        // Only infer when the existing signature lacks an explicit return type.
        if (existing->signature && existing->signature->kind == CBM_TYPE_FUNC &&
            existing->signature->data.func.return_types &&
            existing->signature->data.func.return_types[0] &&
            !cbm_type_is_unknown(existing->signature->data.func.return_types[0])) {
            continue;
        }

        const CBMType *inferred = infer_return_type_from_body(ctx, body);
        if (cbm_type_is_unknown(inferred))
            continue;

        // Build a new FUNC signature carrying over params from the existing one (if any).
        const char **param_names = NULL;
        const CBMType **param_types = NULL;
        if (existing->signature && existing->signature->kind == CBM_TYPE_FUNC) {
            param_names = existing->signature->data.func.param_names;
            param_types = existing->signature->data.func.param_types;
        }
        const CBMType *rets[2] = {inferred, NULL};
        const CBMType *new_sig = cbm_type_func(ctx->arena, param_names, param_types, rets);

        // Patch the registered func in place.
        for (int fi = 0; fi < reg->func_count; fi++) {
            if (reg->funcs[fi].qualified_name == fqn ||
                (reg->funcs[fi].qualified_name &&
                 strcmp(reg->funcs[fi].qualified_name, fqn) == 0)) {
                reg->funcs[fi].signature = new_sig;
                break;
            }
        }
    }
}

// AST sweep: walk class/interface bodies to collect field names+types and refine
// embedded_types / type_param_names.
static void ast_sweep_shapes(TSLSPContext *ctx, TSNode root, CBMTypeRegistry *reg) {
    if (ts_node_is_null(root) || !reg)
        return;
    uint32_t kn = 0;
    TSNode *kids = collect_children(ctx->arena, root, &kn);

    for (uint32_t i = 0; i < kn; i++) {
        TSNode n = kids[i];
        const char *k = ts_node_type(n);
        TSNode decl = n;
        if (strcmp(k, "export_statement") == 0) {
            // Look for declaration field.
            TSNode d =
                ts_node_child_by_field_name(n, "declaration", TS_LSP_FIELD_LEN("declaration"));
            if (!ts_node_is_null(d)) {
                decl = d;
                k = ts_node_type(decl);
            }
        }

        if (strcmp(k, "type_alias_declaration") == 0) {
            TSNode name = ts_node_child_by_field_name(decl, "name", TS_LSP_FIELD_LEN("name"));
            TSNode val = ts_node_child_by_field_name(decl, "value", TS_LSP_FIELD_LEN("value"));
            if (ts_node_is_null(name) || ts_node_is_null(val) || !ctx->module_qn)
                continue;
            char *nm = node_text(ctx, name);
            if (!nm)
                continue;
            const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, nm);
            // alias_of: resolved to QN if it's a NAMED text we can qualify.
            char *val_text = node_text(ctx, val);
            const char *alias_qn = NULL;
            if (val_text && val_text[0] && strchr(val_text, ' ') == NULL &&
                strchr(val_text, '|') == NULL && strchr(val_text, '&') == NULL) {
                if (strchr(val_text, '.') == NULL && val_text[0] >= 'A' && val_text[0] <= 'Z') {
                    alias_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, val_text);
                } else {
                    alias_qn = cbm_arena_strdup(ctx->arena, val_text);
                }
            }
            // Find or insert.
            bool found = false;
            for (int ti = 0; ti < reg->type_count; ti++) {
                if (reg->types[ti].qualified_name &&
                    strcmp(reg->types[ti].qualified_name, qn) == 0) {
                    if (alias_qn)
                        reg->types[ti].alias_of = alias_qn;
                    found = true;
                    break;
                }
            }
            if (!found) {
                CBMRegisteredType rt;
                memset(&rt, 0, sizeof(rt));
                rt.qualified_name = qn;
                rt.short_name = nm;
                rt.alias_of = alias_qn;
                cbm_registry_add_type(reg, rt);
            }
            continue;
        }

        if (strcmp(k, "class_declaration") != 0 && strcmp(k, "interface_declaration") != 0) {
            continue;
        }

        TSNode name_node = ts_node_child_by_field_name(decl, "name", TS_LSP_FIELD_LEN("name"));
        TSNode body = ts_node_child_by_field_name(decl, "body", TS_LSP_FIELD_LEN("body"));
        if (ts_node_is_null(name_node) || ts_node_is_null(body) || !ctx->module_qn)
            continue;
        char *cname = node_text(ctx, name_node);
        if (!cname)
            continue;
        const char *class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cname);

        // Find the registered type and augment with fields.
        CBMRegisteredType *rt = NULL;
        for (int ti = 0; ti < reg->type_count; ti++) {
            if (reg->types[ti].qualified_name &&
                strcmp(reg->types[ti].qualified_name, class_qn) == 0) {
                rt = &reg->types[ti];
                break;
            }
        }
        if (!rt)
            continue;

        const char *field_names[64] = {0};
        const CBMType *field_types[64] = {0};
        int field_count = 0;
        // Interface method signatures we need to register as separate funcs.
        const char *iface_method_names[64] = {0};
        const char *iface_method_qns[64] = {0};
        const CBMType *iface_method_sigs[64] = {0};
        int iface_method_count = 0;
        bool is_interface = (strcmp(k, "interface_declaration") == 0);

        uint32_t bnc = ts_node_named_child_count(body);
        for (uint32_t bi = 0; bi < bnc && field_count < 63; bi++) {
            TSNode m = ts_node_named_child(body, bi);
            if (ts_node_is_null(m))
                continue;
            const char *mk = ts_node_type(m);

            // public_field_definition: TS class fields.
            // property_signature: interface fields. Interface fields can be either
            // a bare property (string name) OR a callable property (function-type field).
            if (strcmp(mk, "public_field_definition") == 0 ||
                strcmp(mk, "property_signature") == 0) {
                TSNode fname = ts_node_child_by_field_name(m, "name", TS_LSP_FIELD_LEN("name"));
                TSNode ftype = ts_node_child_by_field_name(m, "type", TS_LSP_FIELD_LEN("type"));
                if (ts_node_is_null(fname))
                    continue;
                char *fnm = node_text(ctx, fname);
                if (!fnm)
                    continue;
                const CBMType *ft = cbm_type_unknown();
                if (!ts_node_is_null(ftype)) {
                    TSNode tch = ts_node_named_child(ftype, 0);
                    if (!ts_node_is_null(tch))
                        ft = ts_parse_type_node(ctx, tch);
                }
                field_names[field_count] = fnm;
                field_types[field_count] = ft;
                field_count++;
                continue;
            }

            // Interface method signature: `methodName(params): ReturnType;`
            // Or class method declaration in `.d.ts` ambient mode.
            if (is_interface && (strcmp(mk, "method_signature") == 0)) {
                if (iface_method_count >= 63)
                    continue;
                TSNode mname = ts_node_child_by_field_name(m, "name", TS_LSP_FIELD_LEN("name"));
                if (ts_node_is_null(mname))
                    continue;
                char *mnm = node_text(ctx, mname);
                if (!mnm)
                    continue;

                // Return type via field "return_type" — it's a type_annotation node.
                const CBMType *ret = cbm_type_unknown();
                TSNode rt_node =
                    ts_node_child_by_field_name(m, "return_type", TS_LSP_FIELD_LEN("return_type"));
                if (!ts_node_is_null(rt_node)) {
                    TSNode tch = (strcmp(ts_node_type(rt_node), "type_annotation") == 0)
                                     ? ts_node_named_child(rt_node, 0)
                                     : rt_node;
                    if (!ts_node_is_null(tch))
                        ret = ts_parse_type_node(ctx, tch);
                }
                const CBMType *rets[2] = {ret, NULL};
                const CBMType *sig = cbm_type_func(ctx->arena, NULL, NULL, rets);

                const char *mqn = cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, mnm);
                iface_method_names[iface_method_count] = mnm;
                iface_method_qns[iface_method_count] = mqn;
                iface_method_sigs[iface_method_count] = sig;
                iface_method_count++;

                // Also register a CBMRegisteredFunc so cbm_registry_lookup_method picks it up.
                CBMRegisteredFunc rf;
                memset(&rf, 0, sizeof(rf));
                rf.qualified_name = mqn;
                rf.short_name = mnm;
                rf.receiver_type = class_qn;
                rf.signature = sig;
                rf.min_params = -1;
                cbm_registry_add_func(reg, rf);
            }
        }
        if (field_count > 0) {
            const char **fn_arr = (const char **)cbm_arena_alloc(
                ctx->arena, (size_t)(field_count + 1) * sizeof(const char *));
            const CBMType **ft_arr = (const CBMType **)cbm_arena_alloc(
                ctx->arena, (size_t)(field_count + 1) * sizeof(const CBMType *));
            if (fn_arr && ft_arr) {
                for (int j = 0; j < field_count; j++) {
                    fn_arr[j] = field_names[j];
                    ft_arr[j] = field_types[j];
                }
                fn_arr[field_count] = NULL;
                ft_arr[field_count] = NULL;
                rt->field_names = fn_arr;
                rt->field_types = ft_arr;
            }
        }

        // Attach interface method names/qns to the registered type.
        if (iface_method_count > 0) {
            const char **mn_arr = (const char **)cbm_arena_alloc(
                ctx->arena, (size_t)(iface_method_count + 1) * sizeof(const char *));
            const char **mq_arr = (const char **)cbm_arena_alloc(
                ctx->arena, (size_t)(iface_method_count + 1) * sizeof(const char *));
            if (mn_arr && mq_arr) {
                for (int j = 0; j < iface_method_count; j++) {
                    mn_arr[j] = iface_method_names[j];
                    mq_arr[j] = iface_method_qns[j];
                }
                mn_arr[iface_method_count] = NULL;
                mq_arr[iface_method_count] = NULL;
                rt->method_names = mn_arr;
                rt->method_qns = mq_arr;
            }
            (void)iface_method_sigs; // sigs already attached via cbm_registry_add_func
        }
    }
}

void cbm_run_ts_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                    TSNode root, bool js_mode, bool jsx_mode, bool dts_mode) {
    if (!arena || !result || !source || ts_node_is_null(root))
        return;

    // Diagnostic / benchmarking knob: setting `CBM_LSP_DISABLED=1` skips the resolver.
    // This is used by the baseline-vs-LSP comparison tests to measure how many calls
    // the LSP-augmented path adds over plain tree-sitter extraction.
    const char *disabled = getenv("CBM_LSP_DISABLED");
    if (disabled && disabled[0] && disabled[0] != '0')
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_ts_stdlib_register(&reg, arena);
    register_file_defs(arena, &reg, result, result->module_qn);

    TSLSPContext ctx;
    ts_lsp_init(&ctx, arena, source, source_len, &reg, result->module_qn, js_mode, jsx_mode,
                dts_mode, &result->resolved_calls);

    // Add imports early so type-resolution passes can rewrite NAMED references that
    // refer to imported symbols. ts_parse_type_node consults these via
    // resolve_type_with_imports.
    for (int i = 0; i < result->imports.count; i++) {
        const CBMImport *imp = &result->imports.items[i];
        if (imp->local_name && imp->module_path) {
            ts_lsp_add_import(&ctx, imp->local_name, imp->module_path);
        }
    }

    ast_sweep_shapes(&ctx, root, &reg);
    rebuild_signatures_from_ast(&ctx, root, &reg);
    convert_signature_type_params(&ctx, root, &reg);
    apply_jsdoc_signatures(&ctx, root, &reg);
    infer_implicit_returns(&ctx, root, &reg);

    ts_lsp_process_file(&ctx, root);
}

// ── Cross-file entry point (Phase 3) ──────────────────────────────────────────

/* Register a batch of CBMLSPDef[] into a registry. Mirrors the inline
 * loop in cbm_run_ts_lsp_cross — KEEP IN SYNC. Shared by the Tier 2
 * pre-built registry builder + per-file overlay builder. Def-driven
 * (return/field/embedded/method info from def strings). */
static void ts_register_lsp_defs(CBMArena *arena, CBMTypeRegistry *reg, CBMLSPDef *defs,
                                 int def_count) {
    for (int i = 0; i < def_count; i++) {
        const CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;

        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->short_name;
            rt.is_interface = (strcmp(d->label, "Interface") == 0);
            if (d->embedded_types && d->embedded_types[0]) {
                int n = 1;
                for (const char *s = d->embedded_types; *s; s++)
                    if (*s == '|')
                        n++;
                const char **arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(const char *));
                if (arr) {
                    int idx = 0;
                    const char *start = d->embedded_types;
                    for (const char *s = d->embedded_types;; s++) {
                        if (*s == '|' || *s == '\0') {
                            arr[idx++] = cbm_arena_strndup(arena, start, (size_t)(s - start));
                            if (*s == '\0')
                                break;
                            start = s + 1;
                        }
                    }
                    arr[idx] = NULL;
                    rt.embedded_types = arr;
                }
            }
            if (d->field_defs && d->field_defs[0]) {
                int n = 1;
                for (const char *s = d->field_defs; *s; s++)
                    if (*s == '|')
                        n++;
                const char **fn_arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(const char *));
                const CBMType **ft_arr = (const CBMType **)cbm_arena_alloc(
                    arena, (size_t)(n + 1) * sizeof(const CBMType *));
                if (fn_arr && ft_arr) {
                    int idx = 0;
                    const char *start = d->field_defs;
                    for (const char *s = d->field_defs;; s++) {
                        if (*s == '|' || *s == '\0') {
                            char *pair = cbm_arena_strndup(arena, start, (size_t)(s - start));
                            char *colon = pair ? strchr(pair, ':') : NULL;
                            if (pair && colon) {
                                *colon = '\0';
                                fn_arr[idx] = pair;
                                ft_arr[idx] =
                                    parse_ts_type_text(arena, colon + 1, d->def_module_qn);
                                idx++;
                            }
                            if (*s == '\0')
                                break;
                            start = s + 1;
                        }
                    }
                    fn_arr[idx] = NULL;
                    ft_arr[idx] = NULL;
                    rt.field_names = fn_arr;
                    rt.field_types = ft_arr;
                }
            }
            cbm_registry_add_type(reg, rt);
            if (d->method_names_str && d->method_names_str[0]) {
                int n = 1;
                for (const char *s = d->method_names_str; *s; s++)
                    if (*s == '|')
                        n++;
                const char **mn_arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(const char *));
                const char **mqn_arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(const char *));
                if (mn_arr && mqn_arr) {
                    int idx = 0;
                    const char *start = d->method_names_str;
                    for (const char *s = d->method_names_str;; s++) {
                        if (*s == '|' || *s == '\0') {
                            char *m = cbm_arena_strndup(arena, start, (size_t)(s - start));
                            mn_arr[idx] = m;
                            mqn_arr[idx] = cbm_arena_sprintf(arena, "%s.%s", d->qualified_name, m);
                            idx++;
                            if (*s == '\0')
                                break;
                            start = s + 1;
                        }
                    }
                    mn_arr[idx] = NULL;
                    mqn_arr[idx] = NULL;
                    if (reg->type_count > 0) {
                        CBMRegisteredType *rt_just = &reg->types[reg->type_count - 1];
                        rt_just->method_names = mn_arr;
                        rt_just->method_qns = mqn_arr;
                    }
                }
            }
        } else if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->short_name;
            rf.min_params = -1;
            if (d->return_types && d->return_types[0]) {
                int n = 1;
                for (const char *s = d->return_types; *s; s++)
                    if (*s == '|')
                        n++;
                const CBMType **rets = (const CBMType **)cbm_arena_alloc(
                    arena, (size_t)(n + 1) * sizeof(const CBMType *));
                if (rets) {
                    int idx = 0;
                    const char *start = d->return_types;
                    for (const char *s = d->return_types;; s++) {
                        if (*s == '|' || *s == '\0') {
                            char *part = cbm_arena_strndup(arena, start, (size_t)(s - start));
                            rets[idx++] = parse_ts_type_text(arena, part, d->def_module_qn);
                            if (*s == '\0')
                                break;
                            start = s + 1;
                        }
                    }
                    rets[idx] = NULL;
                    rf.signature = cbm_type_func(arena, NULL, NULL, rets);
                }
            }
            if (strcmp(d->label, "Method") == 0 && d->receiver_type) {
                rf.receiver_type = d->receiver_type;
            }
            cbm_registry_add_func(reg, rf);
        }
    }
}

/* Tier 2: build a project-wide TS/JS/TSX registry ONCE from all defs
 * (filters by lang). Shared READ-ONLY *base* across resolve workers.
 * Per-file overlays (built by cbm_run_ts_lsp_cross_with_registry)
 * chain to this via the registry fallback pointer. */
CBMTypeRegistry *cbm_ts_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count) {
    if (!arena)
        return NULL;
    CBMTypeRegistry *reg = (CBMTypeRegistry *)cbm_arena_alloc(arena, sizeof(*reg));
    if (!reg)
        return NULL;
    cbm_registry_init(reg, arena);
    cbm_ts_stdlib_register(reg, arena);
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (d->lang != CBM_LANG_JAVASCRIPT && d->lang != CBM_LANG_TYPESCRIPT &&
            d->lang != CBM_LANG_TSX) {
            continue;
        }
        ts_register_lsp_defs(arena, reg, d, 1);
    }
    cbm_registry_finalize(reg);
    return reg;
}

/* Tier 2 per-file resolve. Builds a SMALL per-file overlay registry P
 * containing only this file's own-module defs (so the AST refinement
 * passes — which mutate type shapes/aliases of locally-declared types
 * — operate on P, not the shared base). P chains to the shared base
 * `reg` for cross-file + stdlib lookups. This preserves the per-file
 * AST refinement quality while avoiding re-registering imported
 * modules' defs in every file. */
void cbm_run_ts_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                        const char *module_qn, bool js_mode, bool jsx_mode,
                                        bool dts_mode, CBMTypeRegistry *reg, CBMLSPDef *defs,
                                        int def_count, const char **import_names,
                                        const char **import_qns, int import_count,
                                        TSTree *cached_tree, CBMResolvedCallArray *out) {
    if (!arena || !out || !reg)
        return;

    /* Per-file overlay: register only the file's own-module defs so the
     * AST passes can refine them. Imports/stdlib resolve via fallback. */
    CBMTypeRegistry overlay;
    cbm_registry_init(&overlay, arena);
    overlay.fallback = reg;
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        const char *dm = d->def_module_qn ? d->def_module_qn : "";
        if (module_qn && strcmp(dm, module_qn) == 0) {
            ts_register_lsp_defs(arena, &overlay, d, 1);
        }
    }

    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        if (!source || source_len <= 0)
            return;
        TSParser *parser = ts_parser_new();
        if (!parser)
            return;
        const TSLanguage *lang =
            jsx_mode ? (js_mode ? tree_sitter_javascript() : tree_sitter_tsx())
                     : (js_mode ? tree_sitter_javascript() : tree_sitter_typescript());
        ts_parser_set_language(parser, lang);
        tree = ts_parser_parse_string(parser, NULL, source, source_len);
        ts_parser_delete(parser);
        if (!tree)
            return;
        owns_tree = true;
    }
    TSNode root = ts_tree_root_node(tree);
    if (ts_node_is_null(root)) {
        if (owns_tree)
            ts_tree_delete(tree);
        return;
    }

    cbm_registry_finalize(&overlay);

    TSLSPContext ctx;
    ts_lsp_init(&ctx, arena, source, source_len, &overlay, module_qn, js_mode, jsx_mode, dts_mode,
                out);
    for (int i = 0; i < import_count; i++) {
        if (import_names && import_qns && import_names[i] && import_qns[i]) {
            ts_lsp_add_import(&ctx, import_names[i], import_qns[i]);
        }
    }

    ast_sweep_shapes(&ctx, root, &overlay);
    rebuild_signatures_from_ast(&ctx, root, &overlay);
    convert_signature_type_params(&ctx, root, &overlay);
    apply_jsdoc_signatures(&ctx, root, &overlay);
    infer_implicit_returns(&ctx, root, &overlay);

    ts_lsp_process_file(&ctx, root);

    if (owns_tree)
        ts_tree_delete(tree);
}

void cbm_run_ts_lsp_cross(CBMArena *arena, const char *source, int source_len,
                          const char *module_qn, bool js_mode, bool jsx_mode, bool dts_mode,
                          CBMLSPDef *defs, int def_count, const char **import_names,
                          const char **import_qns, int import_count, TSTree *cached_tree,
                          CBMResolvedCallArray *out) {
    if (!arena || !out)
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_ts_stdlib_register(&reg, arena);

    // Register cross-file defs.
    for (int i = 0; i < def_count; i++) {
        const CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;

        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->short_name;
            rt.is_interface = (strcmp(d->label, "Interface") == 0);
            // Embedded types (extends list) — pipe-separated.
            if (d->embedded_types && d->embedded_types[0]) {
                int n = 1;
                for (const char *s = d->embedded_types; *s; s++)
                    if (*s == '|')
                        n++;
                const char **arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(const char *));
                if (arr) {
                    int idx = 0;
                    const char *start = d->embedded_types;
                    for (const char *s = d->embedded_types;; s++) {
                        if (*s == '|' || *s == '\0') {
                            arr[idx++] = cbm_arena_strndup(arena, start, (size_t)(s - start));
                            if (*s == '\0')
                                break;
                            start = s + 1;
                        }
                    }
                    arr[idx] = NULL;
                    rt.embedded_types = arr;
                }
            }
            // Field defs (interface members, "name:type|name:type").
            if (d->field_defs && d->field_defs[0]) {
                int n = 1;
                for (const char *s = d->field_defs; *s; s++)
                    if (*s == '|')
                        n++;
                const char **fn_arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(const char *));
                const CBMType **ft_arr = (const CBMType **)cbm_arena_alloc(
                    arena, (size_t)(n + 1) * sizeof(const CBMType *));
                if (fn_arr && ft_arr) {
                    int idx = 0;
                    const char *start = d->field_defs;
                    for (const char *s = d->field_defs;; s++) {
                        if (*s == '|' || *s == '\0') {
                            char *pair = cbm_arena_strndup(arena, start, (size_t)(s - start));
                            char *colon = pair ? strchr(pair, ':') : NULL;
                            if (pair && colon) {
                                *colon = '\0';
                                fn_arr[idx] = pair;
                                ft_arr[idx] =
                                    parse_ts_type_text(arena, colon + 1, d->def_module_qn);
                                idx++;
                            }
                            if (*s == '\0')
                                break;
                            start = s + 1;
                        }
                    }
                    fn_arr[idx] = NULL;
                    ft_arr[idx] = NULL;
                    rt.field_names = fn_arr;
                    rt.field_types = ft_arr;
                }
            }
            cbm_registry_add_type(&reg, rt);
            // Method names from method_names_str.
            if (d->method_names_str && d->method_names_str[0]) {
                int n = 1;
                for (const char *s = d->method_names_str; *s; s++)
                    if (*s == '|')
                        n++;
                const char **mn_arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(const char *));
                const char **mqn_arr =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(const char *));
                if (mn_arr && mqn_arr) {
                    int idx = 0;
                    const char *start = d->method_names_str;
                    for (const char *s = d->method_names_str;; s++) {
                        if (*s == '|' || *s == '\0') {
                            char *m = cbm_arena_strndup(arena, start, (size_t)(s - start));
                            mn_arr[idx] = m;
                            mqn_arr[idx] = cbm_arena_sprintf(arena, "%s.%s", d->qualified_name, m);
                            idx++;
                            if (*s == '\0')
                                break;
                            start = s + 1;
                        }
                    }
                    mn_arr[idx] = NULL;
                    mqn_arr[idx] = NULL;
                    // Find the type we just inserted and attach.
                    if (reg.type_count > 0) {
                        CBMRegisteredType *rt_just = &reg.types[reg.type_count - 1];
                        rt_just->method_names = mn_arr;
                        rt_just->method_qns = mqn_arr;
                    }
                }
            }
        } else if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->short_name;
            rf.min_params = -1;
            // Return types from pipe-separated return_types text.
            if (d->return_types && d->return_types[0]) {
                int n = 1;
                for (const char *s = d->return_types; *s; s++)
                    if (*s == '|')
                        n++;
                const CBMType **rets = (const CBMType **)cbm_arena_alloc(
                    arena, (size_t)(n + 1) * sizeof(const CBMType *));
                if (rets) {
                    int idx = 0;
                    const char *start = d->return_types;
                    for (const char *s = d->return_types;; s++) {
                        if (*s == '|' || *s == '\0') {
                            char *part = cbm_arena_strndup(arena, start, (size_t)(s - start));
                            rets[idx++] = parse_ts_type_text(arena, part, d->def_module_qn);
                            if (*s == '\0')
                                break;
                            start = s + 1;
                        }
                    }
                    rets[idx] = NULL;
                    rf.signature = cbm_type_func(arena, NULL, NULL, rets);
                }
            }
            if (strcmp(d->label, "Method") == 0 && d->receiver_type) {
                rf.receiver_type = d->receiver_type;
            }
            cbm_registry_add_func(&reg, rf);
        }
    }

    // Use cached tree if available; otherwise parse internally and own it.
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        if (!source || source_len <= 0)
            return;
        TSParser *parser = ts_parser_new();
        if (!parser)
            return;
        const TSLanguage *lang =
            jsx_mode ? (js_mode ? tree_sitter_javascript() : tree_sitter_tsx())
                     : (js_mode ? tree_sitter_javascript() : tree_sitter_typescript());
        ts_parser_set_language(parser, lang);
        tree = ts_parser_parse_string(parser, NULL, source, source_len);
        ts_parser_delete(parser);
        if (!tree)
            return;
        owns_tree = true;
    }
    TSNode root = ts_tree_root_node(tree);
    if (ts_node_is_null(root)) {
        if (owns_tree)
            ts_tree_delete(tree);
        return;
    }

    // Build a faux CBMFileResult so register_file_defs's downstream paths work; we don't
    // need it here because cross-file defs already came in as CBMLSPDef.
    // Finalize registry — O(1) lookups. See go_lsp.c "3c. Finalize"
    // comment for the rationale.
    cbm_registry_finalize(&reg);

    TSLSPContext ctx;
    ts_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, js_mode, jsx_mode, dts_mode, out);

    // Add imports early so type passes can resolve imported types.
    for (int i = 0; i < import_count; i++) {
        if (import_names && import_qns && import_names[i] && import_qns[i]) {
            ts_lsp_add_import(&ctx, import_names[i], import_qns[i]);
        }
    }

    ast_sweep_shapes(&ctx, root, &reg);
    rebuild_signatures_from_ast(&ctx, root, &reg);
    convert_signature_type_params(&ctx, root, &reg);
    apply_jsdoc_signatures(&ctx, root, &reg);
    infer_implicit_returns(&ctx, root, &reg);

    ts_lsp_process_file(&ctx, root);

    if (owns_tree)
        ts_tree_delete(tree);
}

// ── Batch cross-file (Phase 3) ────────────────────────────────────────────────

void cbm_batch_ts_lsp_cross(CBMArena *arena, CBMBatchTSLSPFile *files, int file_count,
                            CBMResolvedCallArray *out) {
    if (!arena || !files || !out || file_count <= 0)
        return;
    // Project-scope merging happens implicitly: every file's CBMLSPDef set already
    // includes both same-file and cross-file defs (built by the caller). The plan §17
    // finding #4 fix ensures the *caller* (pass_calls.c) merges interface declarations
    // by QN before passing them in.
    for (int i = 0; i < file_count; i++) {
        CBMBatchTSLSPFile *f = &files[i];
        cbm_run_ts_lsp_cross(arena, f->source, f->source_len, f->module_qn, f->js_mode, f->jsx_mode,
                             f->dts_mode, f->defs, f->def_count, f->import_names, f->import_qns,
                             f->import_count, f->cached_tree, &out[i]);
    }
}
