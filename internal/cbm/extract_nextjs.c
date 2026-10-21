/*
 * extract_nextjs.c -- Next.js App Router route handlers (#759): an HTTP verb a
 * route file (`app/.../route.ts` or `route.js`, the app directory at the root
 * or under src/) exports serves the path its directories name.
 *
 * The handler is the Function definition extraction already made for the
 * export: this fills its route_path / route_method, so its Route node and
 * HANDLES edge come from the route pass exactly as a decorator route's do, on
 * a full and on an incremental index alike. The path is spelled as SvelteKit's
 * (sveltekit_route_path, pass_route_nodes.c): `[id]` is `:id`, `[...slug]` is
 * `*slug` and a `(group)` directory adds no segment.
 *
 * A directory this cannot spell makes the file no route at all: an optional
 * catch-all (`[[...slug]]`), a private folder (`_lib`), a parallel slot
 * (`@modal`), an intercepting route (`(.)photo`), a malformed bracket, or a
 * path that does not fit route_path. A handler is a Function of this file:
 * `export [async] function GET`, `export const GET = [async] (...) => ...`,
 * or `export { GET }` of such a declaration. A renamed
 * (`export { handler as GET }`), re-exported, destructured or wrapped
 * (`export const GET = auth(handler)`) handler, or a `function` expression
 * bound to a const, has no Function of its own here and is not routed. Each
 * file is judged alone: a src/app/ route file is routed even where a root
 * app/ directory makes Next.js ignore src/app/.
 */
#include "cbm.h"
#include "foundation/arena.h"
#include "helpers.h"
#include "tree_sitter/api.h"
#include "foundation/constants.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

enum {
    NX_PATH_MAX = CBM_SZ_256, /* the route pass rebuilds a Route from this much */
    NX_REST_LEN = 3,          /* "..." */
};

/* The path of a route file below its app directory, NULL for any other file. */
static const char *nx_app_relative(const char *rel_path) {
    const char *base = rel_path ? strrchr(rel_path, '/') : NULL;
    if (!base || (strcmp(base, "/route.ts") != 0 && strcmp(base, "/route.js") != 0)) {
        return NULL;
    }
    if (strncmp(rel_path, "app/", strlen("app/")) == 0) {
        return rel_path + strlen("app/");
    }
    if (strncmp(rel_path, "src/app/", strlen("src/app/")) == 0) {
        return rel_path + strlen("src/app/");
    }
    return NULL;
}

static bool nx_has_any(const char *s, size_t n, const char *chars) {
    for (size_t i = 0; i < n; i++) {
        if (strchr(chars, s[i])) {
            return true;
        }
    }
    return false;
}

/* Append directory segment seg (n bytes) to the route path out[*len]. False
 * when the segment cannot be spelled or the path would not fit. */
static bool nx_append_segment(char *out, size_t *len, const char *seg, size_t n) {
    if (n == 0 || seg[0] == '_' || seg[0] == '@') {
        return false;
    }
    if (seg[0] == '(') {
        /* A route group adds nothing; `(.)x`, `(..)x` and `(...)x` intercept. */
        return n > PAIR_LEN && seg[n - SKIP_ONE] == ')' && seg[SKIP_ONE] != '.' &&
               !nx_has_any(seg + SKIP_ONE, n - PAIR_LEN, "()");
    }
    char marker = '\0';
    if (seg[0] == '[') {
        if (seg[n - SKIP_ONE] != ']') {
            return false;
        }
        marker = ':';
        seg += SKIP_ONE;
        n -= PAIR_LEN;
        if (n >= NX_REST_LEN && strncmp(seg, "...", NX_REST_LEN) == 0) {
            marker = '*';
            seg += NX_REST_LEN;
            n -= NX_REST_LEN;
        }
        if (n == 0 || nx_has_any(seg, n, "[].")) {
            return false;
        }
    } else if (nx_has_any(seg, n, "[]")) {
        return false;
    }
    size_t need = SKIP_ONE + (marker ? SKIP_ONE : 0) + n;
    if (*len + need >= NX_PATH_MAX) {
        return false;
    }
    out[(*len)++] = '/';
    if (marker) {
        out[(*len)++] = marker;
    }
    memcpy(out + *len, seg, n);
    *len += n;
    out[*len] = '\0';
    return true;
}

/* The route path of the file being extracted, in the arena; NULL for none. */
static const char *nx_route_path(CBMExtractCtx *ctx) {
    const char *seg = nx_app_relative(ctx->rel_path);
    if (!seg) {
        return NULL;
    }
    const char *base = strrchr(ctx->rel_path, '/');
    char path[NX_PATH_MAX] = "/";
    size_t len = 0;
    while (seg < base) {
        const char *end = strchr(seg, '/');
        if (!nx_append_segment(path, &len, seg, (size_t)(end - seg))) {
            return NULL;
        }
        seg = end + SKIP_ONE;
    }
    return cbm_arena_strdup(ctx->arena, path);
}

static bool nx_is_method(const char *name) {
    static const char *const methods[] = {"GET",    "POST", "PUT",    "PATCH",
                                          "DELETE", "HEAD", "OPTIONS"};
    for (size_t i = 0; i < sizeof(methods) / sizeof(methods[0]); i++) {
        if (strcmp(name, methods[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool nx_is(TSNode n, const char *type) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), type) == 0;
}

/* Does a node carry the anonymous keyword child kw (`default`, `type`)? */
static bool nx_has_keyword(TSNode n, const char *kw) {
    uint32_t count = ts_node_child_count(n);
    for (uint32_t i = 0; i < count; i++) {
        TSNode c = ts_node_child(n, i);
        if (!ts_node_is_named(c) && strcmp(ts_node_type(c), kw) == 0) {
            return true;
        }
    }
    return false;
}

/* The name of a declarator whose value is an arrow function, the Function
 * extraction names after the binding (`GET = () => ...`); a null node for any
 * other declarator. */
static TSNode nx_arrow_binding(TSNode declarator) {
    TSNode value = ts_node_child_by_field_name(declarator, TS_FIELD("value"));
    TSNode name = ts_node_child_by_field_name(declarator, TS_FIELD("name"));
    if (!nx_is(name, "identifier") || !nx_is(value, "arrow_function")) {
        return (TSNode){0};
    }
    return name;
}

/* Route the Function definition named by name_node, declared inside decl. */
static void nx_route_handler(CBMExtractCtx *ctx, TSNode decl, TSNode name_node, const char *path) {
    const char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name || !nx_is_method(name)) {
        return;
    }
    const char *qn =
        cbm_fqn_compute_source_lang(ctx->arena, ctx->project, ctx->rel_path, name, ctx->language);
    uint32_t first = ts_node_start_point(decl).row + TS_LINE_OFFSET;
    uint32_t last = ts_node_end_point(decl).row + TS_LINE_OFFSET;
    for (int i = 0; qn && i < ctx->result->defs.count; i++) {
        CBMDefinition *def = &ctx->result->defs.items[i];
        if (!def->route_path && def->label && def->qualified_name &&
            strcmp(def->label, "Function") == 0 && strcmp(def->qualified_name, qn) == 0 &&
            def->start_line >= first && def->end_line <= last) {
            def->route_path = path;
            def->route_method = name;
            return;
        }
    }
}

/* Does the name node, if any, spell want? Any name does when want is NULL. */
static bool nx_wanted(CBMExtractCtx *ctx, TSNode name, const char *want) {
    if (ts_node_is_null(name)) {
        return false;
    }
    const char *text = want ? cbm_node_text(ctx->arena, name, ctx->source) : NULL;
    return !want || (text && strcmp(text, want) == 0);
}

/* Route the handlers a top-level declaration (function or const) defines.
 * Only names equal to want are considered when want is set. */
static void nx_route_declaration(CBMExtractCtx *ctx, TSNode decl, const char *path,
                                 const char *want) {
    if (nx_is(decl, "function_declaration")) {
        TSNode name = ts_node_child_by_field_name(decl, TS_FIELD("name"));
        if (nx_wanted(ctx, name, want)) {
            nx_route_handler(ctx, decl, name, path);
        }
        return;
    }
    if (!nx_is(decl, "lexical_declaration") && !nx_is(decl, "variable_declaration")) {
        return;
    }
    uint32_t count = ts_node_named_child_count(decl);
    for (uint32_t i = 0; i < count; i++) {
        TSNode declarator = ts_node_named_child(decl, i);
        TSNode name =
            nx_is(declarator, "variable_declarator") ? nx_arrow_binding(declarator) : (TSNode){0};
        if (nx_wanted(ctx, name, want)) {
            nx_route_handler(ctx, declarator, name, path);
        }
    }
}

/* `export { GET }`: route the file's own top-level declaration of each
 * unrenamed, non-type specifier. */
static void nx_route_export_clause(CBMExtractCtx *ctx, TSNode clause, const char *path) {
    uint32_t count = ts_node_named_child_count(clause);
    uint32_t top = ts_node_named_child_count(ctx->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode spec = ts_node_named_child(clause, i);
        TSNode name = ts_node_child_by_field_name(spec, TS_FIELD("name"));
        if (!nx_is(spec, "export_specifier") || !nx_is(name, "identifier") ||
            !ts_node_is_null(ts_node_child_by_field_name(spec, TS_FIELD("alias"))) ||
            nx_has_keyword(spec, "type") || nx_has_keyword(spec, "typeof")) {
            continue;
        }
        const char *want = cbm_node_text(ctx->arena, name, ctx->source);
        for (uint32_t j = 0; want && j < top; j++) {
            nx_route_declaration(ctx, ts_node_named_child(ctx->root, j), path, want);
        }
    }
}

void cbm_extract_nextjs_routes(CBMExtractCtx *ctx) {
    if (ctx->language != CBM_LANG_TYPESCRIPT && ctx->language != CBM_LANG_JAVASCRIPT) {
        return;
    }
    const char *path = nx_route_path(ctx);
    if (!path) {
        return;
    }
    uint32_t count = ts_node_named_child_count(ctx->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode node = ts_node_named_child(ctx->root, i);
        /* A runtime export of this file: not `export default`, not
         * `export type { ... }`, not a re-export `... from './x'`. */
        if (!nx_is(node, "export_statement") || nx_has_keyword(node, "default") ||
            nx_has_keyword(node, "type") ||
            !ts_node_is_null(ts_node_child_by_field_name(node, TS_FIELD("source")))) {
            continue;
        }
        TSNode decl = ts_node_child_by_field_name(node, TS_FIELD("declaration"));
        if (!ts_node_is_null(decl)) {
            nx_route_declaration(ctx, decl, path, NULL);
            continue;
        }
        uint32_t parts = ts_node_named_child_count(node);
        for (uint32_t j = 0; j < parts; j++) {
            TSNode part = ts_node_named_child(node, j);
            if (nx_is(part, "export_clause")) {
                nx_route_export_clause(ctx, part, path);
            }
        }
    }
}
