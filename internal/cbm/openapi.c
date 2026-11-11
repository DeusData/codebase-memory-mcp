/*
 * openapi.c -- OpenAPI and Swagger documents: each operation a Route, named by
 * the same QN as the route its handler registers in code (route_path.c), and
 * its `operationId` a reference to that handler.
 *
 * A Route is a definition of the document (its file DEFINES it). The
 * operationId is a doc-link record (CBM_DOCLINK_OPENAPI_OPERATION_ID) the
 * pipeline resolves as a qualified code name: Connexion's dotted paths
 * (`app.api.users.get_user`, or `x-openapi-router-controller` plus the id)
 * name one function; a bare id (`getUser`) names none for sure and stays
 * unresolved.
 *
 * The documents: `openapi.json` / `swagger.json` (CBM_LANG_OPENAPI, read with
 * the JSON grammar but no node per key), and any YAML or JSON file whose root
 * says `openapi:` or `swagger:` and has `paths`.
 */
#include "cbm.h"
#include "doclink.h"

#include "arena.h"
#include "foundation/constants.h"
#include "tree_sitter/api.h"

#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

enum {
    OA_SCALAR_MAX = 1024, /* a key, a path, an operationId */
    OA_UNWRAP_MAX = 8,    /* stream > document > block_node > mapping */
    OA_METHOD_MAX = 8,
};

static bool oa_is(TSNode n, const char *type) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), type) == 0;
}

/* The mapping a node holds (a YAML block or flow mapping, a JSON object),
 * through the wrappers around it; a null node for none. */
static TSNode oa_mapping(TSNode n) {
    for (int k = 0; k < OA_UNWRAP_MAX && !ts_node_is_null(n); k++) {
        if (oa_is(n, "block_mapping") || oa_is(n, "flow_mapping") || oa_is(n, "object")) {
            return n;
        }
        if (!oa_is(n, "stream") && !oa_is(n, "document") && !oa_is(n, "block_node") &&
            !oa_is(n, "flow_node")) {
            break;
        }
        TSNode next = {0};
        uint32_t nc = ts_node_named_child_count(n);
        for (uint32_t i = 0; i < nc && ts_node_is_null(next); i++) {
            TSNode c = ts_node_named_child(n, i);
            if (!oa_is(c, "comment") && !oa_is(c, "anchor") && !oa_is(c, "tag")) {
                next = c;
            }
        }
        n = next;
    }
    return (TSNode){0};
}

static bool oa_pair(TSNode n) {
    return oa_is(n, "block_mapping_pair") || oa_is(n, "flow_pair") || oa_is(n, "pair");
}

/* A scalar's text (quotes dropped) into out: its length, 0 for none. */
static size_t oa_scalar(const CBMExtractCtx *ctx, TSNode n, char *out, size_t cap) {
    for (int k = 0; k < OA_UNWRAP_MAX && (oa_is(n, "flow_node") || oa_is(n, "block_node")); k++) {
        n = ts_node_named_child(n, 0);
    }
    if (ts_node_is_null(n) || !ts_node_is_null(oa_mapping(n)) || oa_is(n, "block_sequence") ||
        oa_is(n, "flow_sequence") || oa_is(n, "array")) {
        out[0] = '\0';
        return 0;
    }
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    const char *s = ctx->source + b;
    size_t len = e - b;
    while (len && isspace((unsigned char)s[len - SKIP_ONE])) {
        len--;
    }
    if (len >= PAIR_LEN && (s[0] == '"' || s[0] == '\'') && s[len - SKIP_ONE] == s[0]) {
        s++;
        len -= PAIR_LEN;
    }
    if (len >= cap) {
        out[0] = '\0';
        return 0; /* longer than any key or reference read here */
    }
    memcpy(out, s, len);
    out[len] = '\0';
    return len;
}

/* The value of `key` in a mapping; a null node for none. */
static TSNode oa_get(const CBMExtractCtx *ctx, TSNode map, const char *key) {
    char k[OA_SCALAR_MAX];
    uint32_t nc = ts_node_is_null(map) ? 0 : ts_node_named_child_count(map);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_named_child(map, i);
        if (oa_pair(p) &&
            oa_scalar(ctx, ts_node_child_by_field_name(p, "key", CBM_SZ_3), k, sizeof(k)) &&
            strcmp(k, key) == 0) {
            return ts_node_child_by_field_name(p, "value", CBM_SZ_5);
        }
    }
    return (TSNode){0};
}

static const char *oa_dup_scalar(const CBMExtractCtx *ctx, TSNode n) {
    char v[OA_SCALAR_MAX];
    size_t len = oa_scalar(ctx, n, v, sizeof(v));
    return len ? cbm_arena_strndup(ctx->arena, v, len) : NULL;
}

/* Connexion's controller: on the operation, else its path. */
static const char *oa_controller(const CBMExtractCtx *ctx, TSNode op, TSNode path_item) {
    static const char *const keys[] = {"x-openapi-router-controller", "x-swagger-router-controller",
                                       NULL};
    for (int k = 0; keys[k]; k++) {
        const char *c = oa_dup_scalar(ctx, oa_get(ctx, op, keys[k]));
        c = c ? c : oa_dup_scalar(ctx, oa_get(ctx, path_item, keys[k]));
        if (c) {
            return c;
        }
    }
    return NULL;
}

/* One operation: its Route, and its operationId's reference. */
static void oa_operation(CBMExtractCtx *ctx, const char *path, const char *method, TSNode op,
                         TSNode path_item, TSNode pair) {
    CBMArena *a = ctx->arena;
    char upper[OA_METHOD_MAX];
    size_t ml = strlen(method);
    for (size_t i = 0; i < ml && i + SKIP_ONE < sizeof(upper); i++) {
        upper[i] = (char)toupper((unsigned char)method[i]);
        upper[i + SKIP_ONE] = '\0';
    }
    size_t pl = strlen(path);
    char *canon = (char *)cbm_arena_alloc(a, pl + SKIP_ONE);
    const char *qn = canon ? cbm_arena_sprintf(a, "__route__%s__%s", upper,
                                               cbm_route_canon_path(path, canon, pl + SKIP_ONE))
                           : NULL;
    const char *op_id = oa_dup_scalar(ctx, oa_get(ctx, op, "operationId"));
    const char **kv = (const char **)cbm_arena_alloc(a, CBM_SZ_7 * sizeof(char *));
    const char *m = cbm_arena_strdup(a, upper);
    if (!qn || !kv || !m) {
        return;
    }
    kv[0] = "method";
    kv[SKIP_ONE] = m;
    kv[PAIR_LEN] = "source";
    kv[CBM_SZ_3] = "openapi";
    kv[CBM_SZ_4] = op_id ? "operation_id" : NULL;
    kv[CBM_SZ_5] = op_id;
    kv[CBM_SZ_6] = NULL;
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = cbm_arena_strdup(a, path);
    def.qualified_name = qn;
    def.label = "Route";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(pair).row + SKIP_ONE;
    def.end_line = ts_node_end_point(pair).row + SKIP_ONE;
    def.extra_props = kv;
    if (!def.name) {
        return;
    }
    cbm_defs_push(&ctx->result->defs, a, def);
    if (!op_id) {
        return;
    }
    const char *controller = oa_controller(ctx, op, path_item);
    CBMDocLink link = {
        .source_qn = qn,
        .raw = controller ? cbm_arena_sprintf(a, "%s.%s", controller, op_id) : op_id,
        .line = def.start_line,
        .def_line = def.start_line,
        .syntax = CBM_DOCLINK_OPENAPI_OPERATION_ID,
    };
    if (link.raw) {
        cbm_doclinks_push(&ctx->result->doc_links, a, link);
    }
}

static bool oa_method(const char *k) {
    static const char *const methods[] = {"get",  "put",   "post",  "delete", "options",
                                          "head", "patch", "trace", NULL};
    for (int i = 0; methods[i]; i++) {
        if (strcmp(k, methods[i]) == 0) {
            return true;
        }
    }
    return false;
}

void cbm_extract_openapi(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || !ctx->source || ts_node_is_null(ctx->root) ||
        (ctx->language != CBM_LANG_OPENAPI && ctx->language != CBM_LANG_YAML &&
         ctx->language != CBM_LANG_JSON)) {
        return;
    }
    TSNode root = oa_mapping(ctx->root);
    if (ts_node_is_null(oa_get(ctx, root, "openapi")) &&
        ts_node_is_null(oa_get(ctx, root, "swagger"))) {
        return; /* no API description */
    }
    TSNode paths = oa_mapping(oa_get(ctx, root, "paths"));
    char path[OA_SCALAR_MAX];
    char key[OA_SCALAR_MAX];
    uint32_t np = ts_node_is_null(paths) ? 0 : ts_node_named_child_count(paths);
    for (uint32_t i = 0; i < np; i++) {
        TSNode pp = ts_node_named_child(paths, i);
        if (!oa_pair(pp) ||
            !oa_scalar(ctx, ts_node_child_by_field_name(pp, "key", CBM_SZ_3), path, sizeof(path)) ||
            path[0] != '/') {
            continue;
        }
        TSNode item = oa_mapping(ts_node_child_by_field_name(pp, "value", CBM_SZ_5));
        uint32_t nm = ts_node_is_null(item) ? 0 : ts_node_named_child_count(item);
        for (uint32_t j = 0; j < nm; j++) {
            TSNode mp = ts_node_named_child(item, j);
            if (!oa_pair(mp) ||
                !oa_scalar(ctx, ts_node_child_by_field_name(mp, "key", CBM_SZ_3), key,
                           sizeof(key)) ||
                !oa_method(key)) {
                continue;
            }
            TSNode op = oa_mapping(ts_node_child_by_field_name(mp, "value", CBM_SZ_5));
            if (!ts_node_is_null(op)) {
                oa_operation(ctx, path, key, op, item, mp);
            }
        }
    }
}
