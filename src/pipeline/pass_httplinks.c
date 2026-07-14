
/*
 * pass_httplinks.c — HTTP route discovery and cross-service linking.
 *
 * Port of Go internal/httplink package. Discovers HTTP route registrations
 * (Express, Flask, Gin, Spring, Ktor, Laravel, Actix, ASP.NET) and HTTP
 * call sites (fetch, http.Get, requests.post, etc.). Creates:
 *   - Route nodes with method + path
 *   - HANDLES edges: handler function → Route
 *   - HTTP_CALLS edges: caller → Route (cross-service calls)
 *   - ASYNC_CALLS edges: caller → Route (async dispatch)
 *   - CALLS edges with via=route_registration (registrar → Route)
 *
 * Operates on graph buffer (pre-flush): reads Function/Method/Module nodes,
 * parses decorator properties via yyjson, reads source from disk, and writes
 * Route nodes + edges back to the graph buffer.
 *
 * Depends on: pass_definitions, pass_calls (for cross-file prefix resolution)
 */
// NOLINTNEXTLINE(misc-include-cleaner) — pipeline.h included for interface contract
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/httplink.h"
#include "pipeline/worker_pool.h"
#include "graph_buffer/graph_buffer.h"
// NOLINTNEXTLINE(misc-include-cleaner) — platform.h included for worker count
#include "foundation/platform.h"
#include "foundation/log.h"
#include "foundation/profile.h"
#include "foundation/compat.h"
#include "foundation/compat_regex.h"

#include "yyjson/yyjson.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

/* ── Constants ────────────────────────────────────────────────── */
#define DOTTED_FRAG_BUF 260      /* buffer for slash-to-dot path conversion */
#define MIN_PATH_CONFIDENCE 0.25 /* minimum score to create HTTP_CALLS edge */
#define MODULE_WEIGHT 0.85       /* confidence weight for Module-sourced calls */

enum {
    HL_IMPORT_BINDING_MAX = 64,
    HL_GROUP_PREFIX_BINDING_MAX = 16,
    HL_BINDING_KEY_SIZE = 128,
    HL_BINDING_VALUE_SIZE = 256,
};

typedef struct {
    char key[HL_BINDING_KEY_SIZE];
    char value[HL_BINDING_VALUE_SIZE];
} hl_binding_t;

static bool hl_copy_regex_span(char *dst, size_t dst_sz, const char *base, cbm_regmatch_t match) {
    if (!dst || dst_sz == 0 || !base || match.rm_so < 0 || match.rm_eo < match.rm_so) {
        return false;
    }
    size_t len = (size_t)(match.rm_eo - match.rm_so);
    if (len >= dst_sz) {
        return false;
    }
    memcpy(dst, base + match.rm_so, len);
    dst[len] = '\0';
    return true;
}

static bool hl_binding_add(hl_binding_t *bindings, int *count, int max_count, const char *base,
                           cbm_regmatch_t key_match, cbm_regmatch_t value_match) {
    if (!bindings || !count || *count < 0 || *count >= max_count) {
        return false;
    }
    hl_binding_t entry;
    memset(&entry, 0, sizeof(entry));
    if (!hl_copy_regex_span(entry.key, sizeof(entry.key), base, key_match) ||
        !hl_copy_regex_span(entry.value, sizeof(entry.value), base, value_match)) {
        return false;
    }
    bindings[*count] = entry;
    (*count)++;
    return true;
}

static const char *hl_binding_lookup(const hl_binding_t *bindings, int count, const char *key) {
    if (!bindings || !key || !key[0]) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        if (strcmp(bindings[i].key, key) == 0) {
            return bindings[i].value;
        }
    }
    return NULL;
}

/* ── Format int to string for logging ──────────────────────────── */

static const char *itoa_hl(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Source cache helpers ──────────────────────────────────────── */

/* Read source lines from disk (used for route discovery which processes
 * few files filtered by language extension). */
static char *read_source_lines(const cbm_pipeline_ctx_t *ctx, const char *rel_path, int start_line,
                               int end_line) {
    return cbm_read_source_lines_disk(ctx->repo_path, rel_path, start_line, end_line);
}

static char *read_full_source(const cbm_pipeline_ctx_t *ctx, const char *rel_path) {
    return cbm_read_source_file_disk_limited(ctx->repo_path, rel_path,
                                             CBM_HTTPLINK_FULL_SOURCE_MAX_BYTES, NULL);
}

/* ── JSON helpers ──────────────────────────────────────────────── */

/* Extract the "decorators" array from a properties_json string.
 * Returns a NULL-terminated array of strings. Caller must free array and strings. */
static char **extract_decorators(const char *json, int *out_count) {
    *out_count = 0;
    if (!json) {
        return NULL;
    }

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return NULL;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *decs = yyjson_obj_get(root, "decorators");
    if (!decs || !yyjson_is_arr(decs)) {
        yyjson_doc_free(doc);
        return NULL;
    }

    size_t cnt = yyjson_arr_size(decs);
    if (cnt == 0) {
        yyjson_doc_free(doc);
        return NULL;
    }

    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char **out = calloc(cnt + 1, sizeof(char *));
    int idx = 0;
    yyjson_val *item;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(decs, &iter);
    while ((item = yyjson_arr_iter_next(&iter))) {
        if (yyjson_is_str(item)) {
            out[idx++] = cbm_strdup(yyjson_get_str(item));
        }
    }
    out[idx] = NULL;
    *out_count = idx;

    yyjson_doc_free(doc);
    if (idx > 0) {
        return out;
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(out);
    return NULL;
}

/* Check if a JSON properties string has is_test=true. */
static bool is_test_from_json(const char *json) {
    if (!json) {
        return false;
    }
    /* Fast path: substring search before full parse */
    if (!strstr(json, "\"is_test\"")) {
        return false;
    }

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *v = yyjson_obj_get(root, "is_test");
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool result = v && yyjson_is_bool(v) && yyjson_get_bool(v);
    yyjson_doc_free(doc);
    return result;
}

/* Check if node is from a test file (file path heuristic + is_test property). */
static bool is_test_node(const cbm_gbuf_node_t *n) {
    if (is_test_from_json(n->properties_json)) {
        return true;
    }
    if (!n->file_path) {
        return false;
    }
    return cbm_is_test_node_fp(n->file_path, false);
}

/* Update properties_json to set is_entry_point=true.
 * Returns a newly allocated JSON string. Caller must free(). */
static char *set_entry_point(const char *json) {
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *mroot;

    if (root && yyjson_is_obj(root)) {
        mroot = yyjson_val_mut_copy(mdoc, root);
    } else {
        mroot = yyjson_mut_obj(mdoc);
    }
    yyjson_mut_doc_set_root(mdoc, mroot);

    yyjson_mut_obj_remove_key(mroot, "is_entry_point");
    yyjson_mut_obj_add_bool(mdoc, mroot, "is_entry_point", true);

    char *result = yyjson_mut_write(mdoc, 0, NULL);
    yyjson_mut_doc_free(mdoc);
    if (doc) {
        yyjson_doc_free(doc);
    }
    return result;
}

static void free_decorators(char **decs) {
    if (!decs) {
        return;
    }
    for (int i = 0; decs[i]; i++) {
        free(decs[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(decs);
}

/* ── Suffix helpers ────────────────────────────────────────────── */

static bool has_suffix(const char *s, const char *suffix) {
    if (!s || !suffix) {
        return false;
    }
    size_t sl = strlen(s);
    size_t xl = strlen(suffix);
    if (xl > sl) {
        return false;
    }
    return strcmp(s + sl - xl, suffix) == 0;
}

static bool is_jsts_file(const char *path) {
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return has_suffix(path, ".js") || has_suffix(path, ".ts") || has_suffix(path, ".mjs") ||
           has_suffix(path, ".mts") || has_suffix(path, ".tsx");
}

static bool has_source_route_extractor(const char *path) {
    /* Keep this in lockstep with the source extractor dispatch below.
     * Decorator-based routes are handled before this gate and remain
     * language-driven by definition properties rather than file extension.
     * PHP Route:: registrations are handled by call extraction, which retains
     * enclosing prefix()->group() AST context (#952); rescanning them here with
     * the context-free Laravel regex would mint prefix-dropped duplicates. */
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return has_suffix(path, ".go") || is_jsts_file(path) || has_suffix(path, ".kt") ||
           has_suffix(path, ".kts");
}

/* ── Route discovery ───────────────────────────────────────────── */

/* Discover routes from a single Function/Method node. */
static int discover_node_routes(const cbm_gbuf_node_t *n, const cbm_pipeline_ctx_t *ctx,
                                cbm_route_handler_t *out, int max_out) {
    int total = 0;

    /* 1. Decorator-based routes (Python, Java, Rust, ASP.NET) */
    int ndec = 0;
    char **decs = extract_decorators(n->properties_json, &ndec);
    if (decs && ndec > 0) {
        int nr = cbm_extract_python_routes(n->name, n->qualified_name, (const char **)decs, ndec,
                                           out + total, max_out - total);
        total += nr;

        nr = cbm_extract_java_routes(n->name, n->qualified_name, (const char **)decs, ndec,
                                     out + total, max_out - total);
        total += nr;

        /* Rust Actix and C# ASP.NET also use decorator patterns —
         * these are handled by java_routes for similar decorator syntax
         * but we can add specific extractors here if needed */
    }
    free_decorators(decs);

    /* 2. Source-based routes — scoped by file extension to avoid
     * cross-framework false positives (e.g. Ktor regex matching PHP Cache::get) */
    const char *fp = n->file_path;
    if (has_source_route_extractor(fp) && n->start_line > 0 && n->end_line > 0 &&
        total < max_out) {
        char *source = read_source_lines(ctx, fp, n->start_line, n->end_line);
        if (source) {
            int nr;

            if (has_suffix(fp, ".go")) {
                nr = cbm_extract_go_routes(n->name, n->qualified_name, source, out + total,
                                           max_out - total);
                total += nr;
            }
            if (is_jsts_file(fp)) {
                nr = cbm_extract_express_routes(n->name, n->qualified_name, source, out + total,
                                                max_out - total);
                total += nr;
            }
            if (has_suffix(fp, ".kt") || has_suffix(fp, ".kts")) {
                nr = cbm_extract_ktor_routes(n->name, n->qualified_name, source, out + total,
                                             max_out - total);
                total += nr;
            }

            free(source);
        }
    }

    return total;
}

/* Discover module-level JS/TS Express routes. PHP Route facade calls are
 * already emitted with their AST-composed group prefixes during call passes. */
static int discover_module_routes(const cbm_gbuf_node_t *mod, const cbm_pipeline_ctx_t *ctx,
                                  cbm_route_handler_t *out, int max_out) {
    if (!mod->file_path) {
        return 0;
    }

    bool is_js = is_jsts_file(mod->file_path);
    if (!is_js) {
        return 0;
    }

    /* Read full file (from cache or disk) */
    char *source = read_full_source(ctx, mod->file_path);
    if (!source) {
        return 0;
    }

    int total = 0;
    total += cbm_extract_express_routes(mod->name, mod->qualified_name, source, out + total,
                                        max_out - total);
    free(source);
    return total;
}

/* ── Prefix resolution ─────────────────────────────────────────── */

/* Resolve FastAPI include_router prefixes.
 * Scans Python Module nodes for: app.include_router(var, prefix="/prefix")
 * and from ... import var. Prepends prefix to routes from matching modules. */
static void resolve_fastapi_prefixes(cbm_pipeline_ctx_t *ctx, cbm_route_handler_t *routes,
                                     int route_count) {
    const cbm_gbuf_node_t **modules = NULL;
    int mod_count = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Module", &modules, &mod_count) != 0) {
        return;
    }

    cbm_regex_t include_re;
    cbm_regex_t import_re;
    if (cbm_regcomp(
            &include_re,
            "\\.include_router\\(([[:alnum:]_]+)[[:space:]]*,[[:space:]]*prefix[[:space:]]*=[[:"
            "space:]]*[\"']([^\"']+)[\"']",
            CBM_REG_EXTENDED) != 0) {
        return;
    }
    if (cbm_regcomp(&import_re,
                    "from[[:space:]]+([[:alnum:]_.]+)[[:space:]]+import[[:space:]]+([[:alnum:]_]+)",
                    CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&include_re);
        return;
    }

    for (int m = 0; m < mod_count; m++) {
        const cbm_gbuf_node_t *mod = modules[m];
        if (!mod->file_path || !has_suffix(mod->file_path, ".py")) {
            continue;
        }

        /* Read full module source (from cache or disk) */
        char *source = read_full_source(ctx, mod->file_path);
        if (!source) {
            continue;
        }

        /* Build import map: var_name → dotted.module.path */
        hl_binding_t imports[HL_IMPORT_BINDING_MAX];
        memset(imports, 0, sizeof(imports));
        int import_count = 0;

        const char *p = source;
        cbm_regmatch_t pm[3];
        while (import_count < HL_IMPORT_BINDING_MAX &&
               cbm_regexec(&import_re, p, 3, pm, 0) == 0) {
            (void)hl_binding_add(imports, &import_count, HL_IMPORT_BINDING_MAX, p, pm[2], pm[1]);
            p += pm[0].rm_eo;
        }

        /* Find include_router calls */
        p = source;
        while (cbm_regexec(&include_re, p, 3, pm, 0) == 0) {
            char var_name[HL_BINDING_KEY_SIZE] = {0};
            char prefix[HL_BINDING_VALUE_SIZE] = {0};
            bool copied = hl_copy_regex_span(var_name, sizeof(var_name), p, pm[1]) &&
                          hl_copy_regex_span(prefix, sizeof(prefix), p, pm[2]);
            p += pm[0].rm_eo;
            if (!copied) {
                continue;
            }

            /* Find which module this var was imported from */
            const char *module_path = hl_binding_lookup(imports, import_count, var_name);
            if (!module_path) {
                continue;
            }

            /* Convert dotted module path to file fragment */
            char file_frag[HL_BINDING_VALUE_SIZE];
            snprintf(file_frag, sizeof(file_frag), "%s", module_path);
            for (char *c = file_frag; *c; c++) {
                if (*c == '.') {
                    *c = '/';
                }
            }

            /* Strip trailing slash from prefix */
            size_t pfx_len = strlen(prefix);
            while (pfx_len > 0 && prefix[pfx_len - 1] == '/') {
                prefix[--pfx_len] = '\0';
            }

            /* Apply prefix to matching routes */
            for (int r = 0; r < route_count; r++) {
                /* Skip routes that already have this prefix */
                if (strncmp(routes[r].path, prefix, pfx_len) == 0) {
                    continue;
                }

                /* Match routes whose QN contains the imported module path.
                 * QN uses dots (project.orders.routes.func), so match both:
                 *   - dotted module path ("orders.routes") against QN
                 *   - slash-based file fragment ("orders/routes") against file_path */
                if (strstr(routes[r].qualified_name, module_path) ||
                    (routes[r].function_name[0] && strstr(routes[r].qualified_name, file_frag))) {
                    char new_path[sizeof(routes[r].path)];
                    const char *old_path = routes[r].path;
                    while (*old_path == '/') {
                        old_path++;
                    }
                    snprintf(new_path, sizeof(new_path), "%s/%s", prefix, old_path);
                    snprintf(routes[r].path, sizeof(routes[r].path), "%s", new_path);
                }
            }
        }

        free(source);
    }

    cbm_regfree(&include_re);
    cbm_regfree(&import_re);
}

/* Resolve Express app.use("/prefix", routerVar) prefixes. */
static void resolve_express_prefixes(cbm_pipeline_ctx_t *ctx, cbm_route_handler_t *routes,
                                     int route_count) {
    const cbm_gbuf_node_t **modules = NULL;
    int mod_count = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Module", &modules, &mod_count) != 0) {
        return;
    }

    cbm_regex_t use_re;
    cbm_regex_t require_re;
    cbm_regex_t esimport_re;
    if (cbm_regcomp(
            &use_re,
            "\\.use\\([[:space:]]*[\"'`]([^\"'`]+)[\"'`][[:space:]]*,[[:space:]]*([[:alnum:]_]+)",
            CBM_REG_EXTENDED) != 0) {
        return;
    }
    if (cbm_regcomp(
            &require_re,
            "(const|let|var)[[:space:]]+([[:alnum:]_]+)[[:space:]]*=[[:space:]]*require\\([[:"
            "space:]]*[\"']([^\"']+)[\"']",
            CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&use_re);
        return;
    }
    if (cbm_regcomp(
            &esimport_re,
            "import[[:space:]]+([[:alnum:]_]+)[[:space:]]+from[[:space:]]+[\"']([^\"']+)[\"']",
            CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&use_re);
        cbm_regfree(&require_re);
        return;
    }

    for (int m = 0; m < mod_count; m++) {
        const cbm_gbuf_node_t *mod = modules[m];
        if (!mod->file_path || !is_jsts_file(mod->file_path)) {
            continue;
        }

        /* Read full module source (from cache or disk) */
        char *source = read_full_source(ctx, mod->file_path);
        if (!source) {
            continue;
        }

        /* Build import map: var_name → module_path */
        hl_binding_t imports[HL_IMPORT_BINDING_MAX];
        memset(imports, 0, sizeof(imports));
        int import_count = 0;

        const char *p = source;
        cbm_regmatch_t pm[4];
        while (import_count < HL_IMPORT_BINDING_MAX &&
               cbm_regexec(&require_re, p, 4, pm, 0) == 0) {
            (void)hl_binding_add(imports, &import_count, HL_IMPORT_BINDING_MAX, p, pm[2], pm[3]);
            p += pm[0].rm_eo;
        }
        p = source;
        while (import_count < HL_IMPORT_BINDING_MAX &&
               cbm_regexec(&esimport_re, p, 3, pm, 0) == 0) {
            (void)hl_binding_add(imports, &import_count, HL_IMPORT_BINDING_MAX, p, pm[1], pm[2]);
            p += pm[0].rm_eo;
        }

        /* Find .use("/prefix", var) calls */
        p = source;
        while (cbm_regexec(&use_re, p, 3, pm, 0) == 0) {
            char prefix[HL_BINDING_VALUE_SIZE] = {0};
            char var_name[HL_BINDING_KEY_SIZE] = {0};
            bool copied = hl_copy_regex_span(prefix, sizeof(prefix), p, pm[1]) &&
                          hl_copy_regex_span(var_name, sizeof(var_name), p, pm[2]);
            p += pm[0].rm_eo;
            if (!copied) {
                continue;
            }

            /* Resolve var → module path */
            const char *module_path = hl_binding_lookup(imports, import_count, var_name);
            if (!module_path) {
                continue;
            }

            /* Strip leading ./ and ../ from relative import */
            const char *file_frag = module_path;
            if (strncmp(file_frag, "./", 2) == 0) {
                file_frag += 2;
            }
            if (strncmp(file_frag, "../", 3) == 0) {
                file_frag += 3;
            }

            /* Strip trailing slash from prefix */
            size_t pfx_len = strlen(prefix);
            while (pfx_len > 0 && prefix[pfx_len - 1] == '/') {
                prefix[--pfx_len] = '\0';
            }

            /* Apply prefix to matching routes */
            for (int r = 0; r < route_count; r++) {
                if (strncmp(routes[r].path, prefix, pfx_len) == 0) {
                    continue;
                }

                /* Convert slash-based path to dots for QN matching */
                char dotted_frag[DOTTED_FRAG_BUF];
                snprintf(dotted_frag, sizeof(dotted_frag), "%s", file_frag);
                for (char *c = dotted_frag; *c; c++) {
                    if (*c == '/') {
                        *c = '.';
                    }
                }

                if (strstr(routes[r].qualified_name, dotted_frag) ||
                    strstr(routes[r].qualified_name, file_frag)) {
                    char new_path[sizeof(routes[r].path)];
                    const char *old_path = routes[r].path;
                    while (*old_path == '/') {
                        old_path++;
                    }
                    snprintf(new_path, sizeof(new_path), "%s/%s", prefix, old_path);
                    snprintf(routes[r].path, sizeof(routes[r].path), "%s", new_path);
                }
            }
        }

        free(source);
    }

    cbm_regfree(&use_re);
    cbm_regfree(&require_re);
    cbm_regfree(&esimport_re);
}

/* Resolve Go gin cross-file Group() prefixes.
 * Pattern: v1 := r.Group("/api"); RegisterRoutes(v1) */
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
static void resolve_cross_file_group_prefixes(cbm_pipeline_ctx_t *ctx, cbm_route_handler_t *routes,
                                              int route_count) {
    /* Build routesByFunc index: funcQN → (start_index, count) in routes array */
    typedef struct {
        const char *qn;
        int start;
        int count;
    } func_routes_t;
    func_routes_t func_map[1024];
    memset(func_map, 0, sizeof(func_map));
    int func_map_count = 0;

    for (int i = 0; i < route_count && func_map_count < 1024; i++) {
        /* Find or create entry */
        int found = -1;
        for (int j = 0; j < func_map_count; j++) {
            if (strcmp(func_map[j].qn, routes[i].qualified_name) == 0) {
                found = j;
                func_map[j].count++;
                break;
            }
        }
        if (found < 0) {
            func_map[func_map_count].qn = routes[i].qualified_name;
            func_map[func_map_count].start = i;
            func_map[func_map_count].count = 1;
            func_map_count++;
        }
    }

    cbm_regex_t group_direct_re;
    cbm_regex_t group_var_re;
    if (cbm_regcomp(
            &group_direct_re,
            "([[:alnum:]_]+)\\([[:space:]]*[[:alnum:]_]+\\.Group\\([[:space:]]*\"([^\"]+)\"",
            CBM_REG_EXTENDED) != 0) {
        return;
    }
    if (cbm_regcomp(
            &group_var_re,
            "([[:alnum:]_]+)[[:space:]]*:?=[[:space:]]*[[:alnum:]_]+\\.Group\\([[:space:]]*\"(["
            "^\"]+)\"",
            CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&group_direct_re);
        return;
    }

    for (int fi = 0; fi < func_map_count; fi++) {
        const char *func_qn = func_map[fi].qn;

        /* Find this function node in gbuf */
        const cbm_gbuf_node_t *func_node = cbm_gbuf_find_by_qn(ctx->gbuf, func_qn);
        if (!func_node) {
            continue;
        }

        /* Find CALLS edges targeting this function */
        const cbm_gbuf_edge_t **caller_edges = NULL;
        int caller_count = 0;
        if (cbm_gbuf_find_edges_by_target_type(ctx->gbuf, func_node->id, "CALLS", &caller_edges,
                                               &caller_count) != 0) {
            continue;
        }
        if (caller_count == 0) {
            continue;
        }

        for (int ci = 0; ci < caller_count; ci++) {
            const cbm_gbuf_node_t *caller =
                cbm_gbuf_find_by_id(ctx->gbuf, caller_edges[ci]->source_id);
            if (!caller || !caller->file_path || caller->start_line <= 0) {
                continue;
            }

            char *caller_source =
                read_source_lines(ctx, caller->file_path, caller->start_line, caller->end_line);
            if (!caller_source) {
                continue;
            }

            /* Pattern 1: RegisterRoutes(router.Group("/api")) */
            cbm_regmatch_t pm[3];
            const char *p = caller_source;
            while (cbm_regexec(&group_direct_re, p, 3, pm, 0) == 0) {
                char called_name[HL_BINDING_KEY_SIZE] = {0};
                char prefix[HL_BINDING_VALUE_SIZE] = {0};
                bool copied = hl_copy_regex_span(called_name, sizeof(called_name), p, pm[1]) &&
                              hl_copy_regex_span(prefix, sizeof(prefix), p, pm[2]);
                p += pm[0].rm_eo;
                if (!copied) {
                    continue;
                }

                if (strcmp(called_name, func_node->name) == 0) {
                    /* Apply prefix to routes of this function */
                    size_t pfx_len = strlen(prefix);
                    while (pfx_len > 0 && prefix[pfx_len - 1] == '/') {
                        prefix[--pfx_len] = '\0';
                    }
                    for (int r = 0; r < route_count; r++) {
                        if (strcmp(routes[r].qualified_name, func_qn) != 0) {
                            continue;
                        }
                        if (strncmp(routes[r].path, prefix, pfx_len) == 0) {
                            continue;
                        }
                        char new_path[sizeof(routes[r].path)];
                        const char *old = routes[r].path;
                        while (*old == '/') {
                            old++;
                        }
                        snprintf(new_path, sizeof(new_path), "%s/%s", prefix, old);
                        snprintf(routes[r].path, sizeof(routes[r].path), "%s", new_path);
                    }
                    break;
                }
            }

            /* Pattern 2: v1 := r.Group("/api"); RegisterRoutes(v1) */
            hl_binding_t var_pfx[HL_GROUP_PREFIX_BINDING_MAX];
            memset(var_pfx, 0, sizeof(var_pfx));
            int var_count = 0;

            p = caller_source;
            while (var_count < HL_GROUP_PREFIX_BINDING_MAX &&
                   cbm_regexec(&group_var_re, p, 3, pm, 0) == 0) {
                (void)hl_binding_add(var_pfx, &var_count, HL_GROUP_PREFIX_BINDING_MAX, p, pm[1],
                                     pm[2]);
                p += pm[0].rm_eo;
            }

            if (var_count > 0) {
                /* Build regex: funcName\s*\(\s*(\w+) */
                char call_pat[256];
                snprintf(call_pat, sizeof(call_pat), "%s[[:space:]]*\\([[:space:]]*([[:alnum:]_]+)",
                         func_node->name);
                cbm_regex_t call_re;
                if (cbm_regcomp(&call_re, call_pat, CBM_REG_EXTENDED) == 0) {
                    p = caller_source;
                    while (cbm_regexec(&call_re, p, 2, pm, 0) == 0) {
                        char arg_name[HL_BINDING_KEY_SIZE] = {0};
                        bool copied_arg =
                            hl_copy_regex_span(arg_name, sizeof(arg_name), p, pm[1]);
                        p += pm[0].rm_eo;
                        if (!copied_arg) {
                            continue;
                        }

                        for (int v = 0; v < var_count; v++) {
                            if (strcmp(var_pfx[v].key, arg_name) == 0) {
                                char *prefix = var_pfx[v].value;
                                size_t pfx_len = strlen(prefix);
                                while (pfx_len > 0 && prefix[pfx_len - 1] == '/') {
                                    prefix[--pfx_len] = '\0';
                                }
                                for (int r = 0; r < route_count; r++) {
                                    if (strcmp(routes[r].qualified_name, func_qn) != 0) {
                                        continue;
                                    }
                                    if (strncmp(routes[r].path, prefix, pfx_len) == 0) {
                                        continue;
                                    }
                                    char new_path[sizeof(routes[r].path)];
                                    const char *old = routes[r].path;
                                    while (*old == '/') {
                                        old++;
                                    }
                                    snprintf(new_path, sizeof(new_path), "%s/%s", prefix, old);
                                    snprintf(routes[r].path, sizeof(routes[r].path), "%s",
                                             new_path);
                                }
                                break;
                            }
                        }
                    }
                    cbm_regfree(&call_re);
                }
            }

            free(caller_source);
        }
    }

    cbm_regfree(&group_direct_re);
    cbm_regfree(&group_var_re);
}

/* ── Registration handler resolution ───────────────────────────── */

/* Resolve source-extracted handler references before canonical Route insertion.
 * The AST resolver already models registration as registrar → Route (CALLS) and
 * handler → Route (HANDLES). Do not recreate the pre-b00e1f74 direct
 * registrar → handler CALLS shape here; doing so splits route semantics and
 * duplicates registrations whenever both discovery paths recognize a call. */
static int resolve_registration_handlers(cbm_pipeline_ctx_t *ctx, cbm_route_handler_t *routes,
                                         int route_count) {
    int count = 0;
    for (int i = 0; i < route_count; i++) {
        if (routes[i].handler_ref[0] == '\0') {
            continue;
        }

        /* Resolve handler reference — strip receiver prefix (e.g., "h." from "h.CreateOrder") */
        const char *handler_name = routes[i].handler_ref;
        const char *dot = strrchr(handler_name, '.');
        if (dot) {
            handler_name = dot + 1;
        }

        /* Search for the handler function/method by name */
        const cbm_gbuf_node_t **handler_nodes = NULL;
        int handler_count = 0;
        if (cbm_gbuf_find_by_name(ctx->gbuf, handler_name, &handler_nodes, &handler_count) != 0) {
            continue;
        }
        if (handler_count == 0) {
            continue;
        }

        const cbm_gbuf_node_t *handler = handler_nodes[0];

        /* Store resolved handler QN for later use in insertRouteNodes */
        snprintf(routes[i].resolved_handler_qn, sizeof(routes[i].resolved_handler_qn), "%s",
                 handler->qualified_name);
        count++;
    }
    return count;
}

/* ── Route node insertion ──────────────────────────────────────── */

/* Insert source-discovered routes through the shared canonical identity.
 * Source regexes remain required fallbacks (for example handlerless Ktor), but
 * AST-resolved Go/Express registrations may already own the canonical Route.
 * In that case an existing HANDLES edge is stronger evidence than the source
 * scanner's registrar/module fallback, so retain it without adding clones. */
static int insert_route_nodes(cbm_pipeline_ctx_t *ctx, cbm_route_handler_t *routes,
                              int route_count) {
    int count = 0;
    for (int i = 0; i < route_count; i++) {
        cbm_route_handler_t *rh = &routes[i];

        const char *method = rh->method[0] ? rh->method : CBM_ROUTE_DEFAULT_METHOD;
        char route_qn[CBM_ROUTE_QN_SIZE];
        char route_props[CBM_SZ_256];
        if (!cbm_pipeline_build_service_route_identity(
                rh->path, CBM_SVC_HTTP, method, NULL, NULL, route_qn, sizeof(route_qn),
                route_props, sizeof(route_props))) {
            continue;
        }
        const cbm_gbuf_node_t *existing_route = cbm_gbuf_find_by_qn(ctx->gbuf, route_qn);
        int64_t existing_route_id = existing_route ? existing_route->id : 0;

        /* Use resolved handler QN if available */
        const char *handler_qn = rh->qualified_name;
        if (rh->resolved_handler_qn[0]) {
            handler_qn = rh->resolved_handler_qn;
        }

        /* Look up handler node for file_path and line range.
         * Copy all needed fields — gbuf pointers go stale after upsert. */
        const cbm_gbuf_node_t *handler_node = cbm_gbuf_find_by_qn(ctx->gbuf, handler_qn);
        char h_file[512] = "";
        char h_label[32] = "";
        char h_name[256] = "";
        char h_qn[512] = "";
        char h_props_json[2048] = "{}";
        int h_start = 0;
        int h_end = 0;
        // NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
        int64_t h_id = 0;
        if (handler_node) {
            if (handler_node->file_path) {
                snprintf(h_file, sizeof(h_file), "%s", handler_node->file_path);
            }
            if (handler_node->label) {
                snprintf(h_label, sizeof(h_label), "%s", handler_node->label);
            }
            if (handler_node->name) {
                snprintf(h_name, sizeof(h_name), "%s", handler_node->name);
            }
            if (handler_node->qualified_name) {
                snprintf(h_qn, sizeof(h_qn), "%s", handler_node->qualified_name);
            }
            if (handler_node->properties_json) {
                snprintf(h_props_json, sizeof(h_props_json), "%s", handler_node->properties_json);
            }
            h_start = handler_node->start_line;
            h_end = handler_node->end_line;
            h_id = handler_node->id;
        }
        /* handler_node pointer is NOT used below — only the copies above */

        char protocol[CBM_SZ_32] = "";
        if (rh->protocol[0]) {
            snprintf(protocol, sizeof(protocol), "%s", rh->protocol);
        } else if (h_id > 0 && h_file[0] && h_start > 0) {
            /* Detect protocol from handler source */
            char *hsource = read_source_lines(ctx, h_file, h_start, h_end);
            if (hsource) {
                const char *proto = cbm_detect_protocol(hsource);
                if (proto[0]) {
                    snprintf(protocol, sizeof(protocol), "%s", proto);
                }
                free(hsource);
            }
        }

        int64_t route_id = existing_route_id;
        if (route_id <= 0) {
            char props[CBM_SZ_512];
            char esc_handler[CBM_SZ_256];
            char esc_path[CBM_SZ_256];
            cbm_json_escape(esc_handler, sizeof(esc_handler), handler_qn);
            cbm_json_escape(esc_path, sizeof(esc_path), rh->path);
            int n = snprintf(props, sizeof(props),
                             "{\"method\":\"%s\",\"path\":\"%s\",\"handler\":\"%s\"",
                             method, esc_path, esc_handler);
            if (protocol[0] && n >= 0 && (size_t)n < sizeof(props)) {
                char esc_protocol[CBM_SZ_32];
                cbm_json_escape(esc_protocol, sizeof(esc_protocol), protocol);
                n += snprintf(props + n, sizeof(props) - (size_t)n, ",\"protocol\":\"%s\"",
                              esc_protocol);
            }
            if (n < 0 || (size_t)n >= sizeof(props) - SKIP_ONE) {
                continue;
            }
            snprintf(props + n, sizeof(props) - (size_t)n, "}");
            route_id = cbm_gbuf_upsert_node(ctx->gbuf, "Route", rh->path, route_qn, h_file,
                                            h_start, h_end, props);
        }
        if (route_id <= 0) {
            continue;
        }

        const cbm_gbuf_edge_t **existing_handles = NULL;
        int existing_handle_count = 0;
        cbm_gbuf_find_edges_by_target_type(ctx->gbuf, route_id, "HANDLES", &existing_handles,
                                           &existing_handle_count);

        /* Create HANDLES only when the canonical Route has no stronger AST or
         * decorator handler. This preserves regex-only fallback coverage while
         * preventing Function/Module scans from adding weaker pseudo-handlers. */
        if (h_id > 0) {
            bool has_handler = false;
            for (int eh = 0; eh < existing_handle_count; eh++) {
                if (existing_handles[eh]->source_id == h_id) {
                    has_handler = true;
                    break;
                }
            }
            if (existing_handle_count == 0 || has_handler) {
                cbm_gbuf_insert_edge(ctx->gbuf, h_id, route_id, "HANDLES", "{}");
            }

            /* Mark handler as entry point */
            if (existing_handle_count == 0 || has_handler) {
                char *new_props = set_entry_point(h_props_json);
                if (new_props) {
                    cbm_gbuf_upsert_node(ctx->gbuf, h_label, h_name, h_qn, h_file, h_start, h_end,
                                         new_props);
                    free(new_props);
                }
            }
        }

        /* Regex-only registrations with an explicit handler still retain a
         * registrar → Route edge. Existing AST registrations already have it,
         * and graph-buffer insertion deduplicates the identical edge. */
        if (rh->handler_ref[0] != '\0') {
            const cbm_gbuf_node_t *registrar =
                cbm_gbuf_find_by_qn(ctx->gbuf, rh->qualified_name);
            if (registrar) {
                cbm_gbuf_insert_edge(ctx->gbuf, registrar->id, route_id, "CALLS",
                                     "{\"via\":\"route_registration\"}");
            }
        }

        count++;
    }
    return count;
}

/* ── Match and link ────────────────────────────────────────────── */

/* Match call sites to routes and create HTTP_CALLS/ASYNC_CALLS edges. */
static int match_and_link(cbm_pipeline_ctx_t *ctx, cbm_route_handler_t *routes, int route_count,
                          cbm_http_call_site_t *sites, int site_count) {
    int link_count = 0;

    for (int si = 0; si < site_count; si++) {
        const cbm_http_call_site_t *cs = &sites[si];

        /* Find caller node */
        const cbm_gbuf_node_t *caller = cbm_gbuf_find_by_qn(ctx->gbuf, cs->source_qn);
        if (!caller) {
            continue;
        }

        for (int ri = 0; ri < route_count; ri++) {
            const cbm_route_handler_t *rh = &routes[ri];

            /* Skip same-service matches */
            if (cbm_same_service(cs->source_qn, rh->qualified_name)) {
                continue;
            }

            /* Skip excluded paths */
            if (cbm_is_path_excluded(rh->path, cbm_default_exclude_paths,
                                     cbm_default_exclude_paths_count)) {
                continue;
            }

            /* Score path match */
            double min_conf = ctx->httplink_min_confidence > 0.0 ? ctx->httplink_min_confidence
                                                                  : MIN_PATH_CONFIDENCE;
            double score = cbm_path_match_score(cs->path, rh->path);
            if (score < min_conf) {
                continue; /* minimum confidence threshold */
            }

            /* Apply source weight */
            double weight = 1.0;
            if (strcmp(cs->source_label, "Module") == 0) {
                weight = MODULE_WEIGHT;
            }
            score *= weight;

            if (score > 1.0) {
                score = 1.0;
            }

            /* Find handler node */
            const char *handler_qn = rh->qualified_name;
            if (rh->resolved_handler_qn[0]) {
                handler_qn = rh->resolved_handler_qn;
            }
            const cbm_gbuf_node_t *handler = cbm_gbuf_find_by_qn(ctx->gbuf, handler_qn);
            if (!handler) {
                continue;
            }

            /* Create edge */
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            const char *edge_type = cs->is_async ? "ASYNC_CALLS" : "HTTP_CALLS";
            const char *band = cbm_confidence_band(score);

            char edge_props[256];
            snprintf(edge_props, sizeof(edge_props),
                     "{\"url_path\":\"%s\",\"confidence\":%.3f,\"confidence_band\":\"%s\"}",
                     cs->path, score, band);

            cbm_gbuf_insert_edge(ctx->gbuf, caller->id, handler->id, edge_type, edge_props);
            link_count++;
        }
    }

    return link_count;
}

/* ── Parallel route discovery ──────────────────────────────────── */

/* Node entry for flat work array (tagged union of Function/Method/Module). */
typedef struct {
    const cbm_gbuf_node_t *node;
    bool is_module; /* true = Module, false = Function/Method */
} hl_work_item_t;

typedef struct {
    cbm_route_handler_t *routes;
    int count;
    int capacity;
} hl_route_buf_t;

/* Context for parallel route discovery. */
typedef struct {
    const hl_work_item_t *items;
    int item_count;
    const cbm_pipeline_ctx_t *ctx;
    hl_route_buf_t *worker_bufs; /* one per worker */
    int worker_count;
    _Atomic int next_idx;
    _Atomic int allocation_failed;
    _Atomic int *cancelled;
} hl_route_ctx_t;

static bool hl_reserve_items(void **items, int *capacity, int required, size_t item_size) {
    if (!items || !capacity || required < 0 || item_size == 0) {
        return false;
    }
    if (required <= *capacity) {
        return true;
    }
    int next_capacity = *capacity > 0 ? *capacity : CBM_SZ_8;
    while (next_capacity < required) {
        if (next_capacity > INT_MAX / 2) {
            next_capacity = required;
            break;
        }
        next_capacity *= 2;
    }
    if ((size_t)next_capacity > SIZE_MAX / item_size) {
        return false;
    }
    void *grown = realloc(*items, (size_t)next_capacity * item_size);
    if (!grown) {
        return false;
    }
    *items = grown;
    *capacity = next_capacity;
    return true;
}

static bool hl_append_items(void **items, int *count, int *capacity, const void *new_items,
                            int new_count, size_t item_size) {
    if (!items || !count || !capacity || new_count < 0 ||
        (new_count > 0 && !new_items) || new_count > INT_MAX - *count) {
        return false;
    }
    int required = *count + new_count;
    if (!hl_reserve_items(items, capacity, required, item_size)) {
        return false;
    }
    if (new_count > 0) {
        memcpy((char *)*items + (size_t)*count * item_size, new_items,
               (size_t)new_count * item_size);
    }
    *count = required;
    return true;
}

static bool hl_discover_item_routes(hl_route_buf_t *buf, const hl_work_item_t *item,
                                    const cbm_pipeline_ctx_t *ctx) {
    int first = buf->count;
    for (;;) {
        if (!hl_reserve_items((void **)&buf->routes, &buf->capacity, first + 1,
                              sizeof(*buf->routes))) {
            return false;
        }
        int available = buf->capacity - first;
        int discovered = item->is_module
                             ? discover_module_routes(item->node, ctx, buf->routes + first,
                                                      available)
                             : discover_node_routes(item->node, ctx, buf->routes + first,
                                                    available);
        if (discovered < available) {
            buf->count = first + discovered;
            return true;
        }
        if (buf->capacity == INT_MAX ||
            !hl_reserve_items((void **)&buf->routes, &buf->capacity, buf->capacity + 1,
                              sizeof(*buf->routes))) {
            return false;
        }
    }
}

static int hl_active_worker_count(int max_workers, int item_count) {
    if (item_count <= 0) {
        return 0;
    }
    if (max_workers < 1) {
        max_workers = 1;
    }
    return max_workers < item_count ? max_workers : item_count;
}

static void hl_route_worker(int worker_id, void *arg) {
    hl_route_ctx_t *rc = arg;
    hl_route_buf_t *buf = &rc->worker_bufs[worker_id];

    while (1) {
        if (atomic_load_explicit(&rc->allocation_failed, memory_order_relaxed)) {
            break;
        }
        int idx = atomic_fetch_add_explicit(&rc->next_idx, 1, memory_order_relaxed);
        if (idx >= rc->item_count) {
            break;
        }
        if (atomic_load_explicit(rc->cancelled, memory_order_relaxed)) {
            break;
        }

        const hl_work_item_t *item = &rc->items[idx];

        /* Skip test nodes */
        if (is_test_node(item->node)) {
            continue;
        }

        if (!hl_discover_item_routes(buf, item, rc->ctx)) {
            atomic_store_explicit(&rc->allocation_failed, 1, memory_order_relaxed);
            break;
        }
    }
}

typedef struct {
    cbm_http_call_site_t *sites;
    int count;
    int capacity;
} hl_site_buf_t;

/* Context for parallel call site discovery. */
typedef struct {
    const cbm_gbuf_node_t **nodes;
    const char **labels; /* "Function" or "Method" per node */
    int node_count;
    const cbm_pipeline_ctx_t *ctx;
    hl_site_buf_t *worker_bufs;
    int worker_count;
    _Atomic int next_idx;
    _Atomic int allocation_failed;
    _Atomic int *cancelled;
} hl_site_ctx_t;

static void hl_free_paths(char **paths, int path_count) {
    for (int path_index = 0; path_index < path_count; path_index++) {
        free(paths[path_index]);
    }
}

static bool hl_extract_all_url_paths(const char *source, char ***out_paths, int *out_count) {
    char **paths = NULL;
    int capacity = 0;
    if (!out_paths || !out_count) {
        return false;
    }
    *out_paths = NULL;
    *out_count = 0;
    for (;;) {
        if (!hl_reserve_items((void **)&paths, &capacity, capacity + 1, sizeof(*paths))) {
            free(paths);
            return false;
        }
        int path_count = cbm_extract_url_paths(source, paths, capacity);
        if (path_count < 0) {
            free(paths);
            return false;
        }
        if (path_count < capacity) {
            *out_paths = paths;
            *out_count = path_count;
            return true;
        }
        hl_free_paths(paths, path_count);
        if (capacity == INT_MAX) {
            free(paths);
            return false;
        }
    }
}

static void hl_site_worker(int worker_id, void *arg) {
    hl_site_ctx_t *sc = arg;
    hl_site_buf_t *buf = &sc->worker_bufs[worker_id];

    while (1) {
        if (atomic_load_explicit(&sc->allocation_failed, memory_order_relaxed)) {
            break;
        }
        int idx = atomic_fetch_add_explicit(&sc->next_idx, 1, memory_order_relaxed);
        if (idx >= sc->node_count) {
            break;
        }
        if (atomic_load_explicit(sc->cancelled, memory_order_relaxed)) {
            break;
        }

        const cbm_gbuf_node_t *n = sc->nodes[idx];
        if (!n->file_path || n->start_line <= 0 || n->end_line <= 0) {
            continue;
        }

        /* Skip Python dunder methods */
        if (n->name && strlen(n->name) > 4 && n->name[0] == '_' && n->name[1] == '_' &&
            n->name[strlen(n->name) - 1] == '_' && n->name[strlen(n->name) - 2] == '_') {
            continue;
        }

        char *source = read_source_lines(sc->ctx, n->file_path, n->start_line, n->end_line);
        if (!source) {
            continue;
        }

        /* Require at least one HTTP client or async dispatch keyword */
        bool has_http = false;
        for (int k = 0; k < cbm_http_client_keywords_count; k++) {
            if (strstr(source, cbm_http_client_keywords[k])) {
                has_http = true;
                break;
            }
        }
        bool has_async = false;
        for (int k = 0; k < cbm_async_dispatch_keywords_count; k++) {
            if (strstr(source, cbm_async_dispatch_keywords[k])) {
                has_async = true;
                break;
            }
        }

        if (!has_http && !has_async) {
            free(source);
            continue;
        }

        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        bool is_async = has_async && !has_http;

        char **paths = NULL;
        int path_count = 0;
        if (!hl_extract_all_url_paths(source, &paths, &path_count) ||
            path_count > INT_MAX - buf->count ||
            !hl_reserve_items((void **)&buf->sites, &buf->capacity, buf->count + path_count,
                              sizeof(*buf->sites))) {
            hl_free_paths(paths, path_count);
            free(paths);
            free(source);
            atomic_store_explicit(&sc->allocation_failed, 1, memory_order_relaxed);
            break;
        }
        for (int p = 0; p < path_count; p++) {
            cbm_http_call_site_t *site = &buf->sites[buf->count];
            snprintf(site->path, sizeof(site->path), "%s", paths[p]);
            site->method[0] = '\0';
            snprintf(site->source_name, sizeof(site->source_name), "%s", n->name);
            snprintf(site->source_qn, sizeof(site->source_qn), "%s", n->qualified_name);
            snprintf(site->source_label, sizeof(site->source_label), "%s", sc->labels[idx]);
            site->is_async = is_async;
            buf->count++;
        }
        hl_free_paths(paths, path_count);
        free(paths);
        free(source);
    }
}

/* ── Main pass entry point ─────────────────────────────────────── */

int cbm_pipeline_pass_httplinks(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("pass.start", "pass", "httplinks");

    if (cbm_pipeline_check_cancel(ctx)) {
        return -1;
    }

    int worker_count = cbm_default_worker_count(true);
    if (worker_count < 1) {
        worker_count = 1;
    }

    /* ── Phase 1: Route collection via parallel discovery ──
     * Decorator routes come from extracted node properties. The bounded source
     * fallbacks scan Function/Method/Module ranges in parallel for registrations
     * the call extractor cannot model completely (for example handlerless Ktor).
     * The fork's extraction-phase prescan cache was removed by the upstream
     * merge, so this pass owns that fallback work directly. */
    const char *route_labels[] = {"Function", "Method", "Module"};
    const cbm_gbuf_node_t **label_nodes[3] = {NULL, NULL, NULL};
    int label_counts[3] = {0, 0, 0};

    CBM_PROF_START(t_collect_nodes);
    for (int li = 0; li < 3; li++) {
        cbm_gbuf_find_by_label(ctx->gbuf, route_labels[li], &label_nodes[li], &label_counts[li]);
    }
    int total_label_nodes = label_counts[0] + label_counts[1] + label_counts[2];
    CBM_PROF_END_N("httplinks", "0_collect_nodes_seq", t_collect_nodes, total_label_nodes);

    cbm_route_handler_t *routes = NULL;
    int route_count = 0;
    int route_capacity = 0;

    {
        /* Parallel route discovery from disk. */
        CBM_PROF_START(t_routes);
        int total_route_nodes = 0;
        for (int li = 0; li < 3; li++) {
            total_route_nodes += label_counts[li];
        }

        hl_work_item_t *work_items = NULL;
        if (total_route_nodes > 0) {
            work_items = malloc((size_t)total_route_nodes * sizeof(hl_work_item_t));
        }

        int wi = 0;
        if (total_route_nodes == 0 || work_items) {
            for (int li = 0; li < 3; li++) {
                for (int i = 0; i < label_counts[li]; i++) {
                    work_items[wi].node = label_nodes[li][i];
                    work_items[wi].is_module = (li == 2);
                    wi++;
                }
            }
        }

        int route_workers = hl_active_worker_count(worker_count, wi);
        hl_route_buf_t *route_bufs =
            route_workers > 0 ? calloc((size_t)route_workers, sizeof(hl_route_buf_t)) : NULL;
        bool route_collection_failed =
            (total_route_nodes > 0 && !work_items) || (route_workers > 0 && !route_bufs);

        if (!route_collection_failed && route_workers > 0) {
            hl_route_ctx_t rc = {
                .items = work_items,
                .item_count = wi,
                .ctx = ctx,
                .worker_bufs = route_bufs,
                .worker_count = route_workers,
                .cancelled = ctx->cancelled,
            };
            atomic_init(&rc.next_idx, 0);
            atomic_init(&rc.allocation_failed, 0);

            cbm_parallel_for_opts_t opts = {.max_workers = route_workers, .force_pthreads = false};
            cbm_parallel_for(route_workers, hl_route_worker, &rc, opts);

            route_collection_failed =
                atomic_load_explicit(&rc.allocation_failed, memory_order_relaxed) != 0;
            if (!route_collection_failed) {
                for (int w = 0; w < route_workers; w++) {
                    if (!hl_append_items((void **)&routes, &route_count, &route_capacity,
                                         route_bufs[w].routes, route_bufs[w].count,
                                         sizeof(*routes))) {
                        route_collection_failed = true;
                        break;
                    }
                }
            }
        }
        free(work_items);
        if (route_bufs) {
            for (int worker_index = 0; worker_index < route_workers; worker_index++) {
                free(route_bufs[worker_index].routes);
            }
        }
        free(route_bufs);
        CBM_PROF_END_N("httplinks", "1_route_discovery_parallel", t_routes, wi);
        if (route_collection_failed) {
            free(routes);
            return -1;
        }
    }

    cbm_log_info("httplink.routes", "count", itoa_hl(route_count));
    if (route_count == 0) {
        /* No Route nodes or HTTP/ASYNC route edges can be emitted without a
         * discovered handler route. Avoid scanning every Function/Method body
         * for URL literals in route-free codebases; generated parser fixtures
         * otherwise spend seconds here only to produce zero links. */
        cbm_log_info("httplink.callsites", "count", "0");
        free(routes);
        cbm_log_info("pass.done", "pass", "httplinks", "routes", "0", "calls", "0");
        return 0;
    }

    /* ── Phase 2: Resolve cross-file prefixes (serial) ────────── */
    CBM_PROF_START(t_prefix);
    resolve_cross_file_group_prefixes(ctx, routes, route_count);
    resolve_fastapi_prefixes(ctx, routes, route_count);
    resolve_express_prefixes(ctx, routes, route_count);
    CBM_PROF_END_N("httplinks", "2_prefix_resolution_seq", t_prefix, route_count);

    /* ── Phase 3: Registration handler resolution (serial) ───── */
    CBM_PROF_START(t_registration);
    int handlers_resolved = resolve_registration_handlers(ctx, routes, route_count);
    CBM_PROF_END_N("httplinks", "3_handler_resolution_seq", t_registration, route_count);
    if (handlers_resolved > 0) {
        cbm_log_info("httplink.handlers_resolved", "count", itoa_hl(handlers_resolved));
    }

    /* ── Phase 4: Route nodes + HANDLES edges (serial) ────────── */
    CBM_PROF_START(t_insert_routes);
    int route_nodes = insert_route_nodes(ctx, routes, route_count);
    CBM_PROF_END_N("httplinks", "4_route_insert_seq", t_insert_routes, route_count);

    /* ── Phase 5: Call site collection via parallel disk scan ──────
     * Each Function/Method node's source is read from disk and scanned for
     * HTTP/async keywords and URL paths in parallel. (The fork's prescan
     * cache that pre-extracted these during extraction was removed by the
     * upstream merge, so we always compute call sites directly here.) */

    cbm_http_call_site_t *sites = NULL;
    int site_count = 0;
    int site_capacity = 0;
    int total_site_nodes = label_counts[0] + label_counts[1];

    CBM_PROF_START(t_sites);
    const char *site_labels[] = {"Function", "Method"};
    const cbm_gbuf_node_t **all_site_nodes = NULL;
    const char **all_site_labels = NULL;
    bool site_collection_failed = false;

    if (total_site_nodes > 0) {
        all_site_nodes = malloc((size_t)total_site_nodes * sizeof(cbm_gbuf_node_t *));
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        all_site_labels = malloc((size_t)total_site_nodes * sizeof(const char *));
        site_collection_failed = !all_site_nodes || !all_site_labels;
    }

    int site_node_count = 0;
    if (!site_collection_failed) {
        for (int li = 0; li < 2; li++) {
            for (int i = 0; i < label_counts[li]; i++) {
                all_site_nodes[site_node_count] = label_nodes[li][i];
                all_site_labels[site_node_count] = site_labels[li];
                site_node_count++;
            }
        }

        int site_workers = hl_active_worker_count(worker_count, site_node_count);
        hl_site_buf_t *site_bufs =
            site_workers > 0 ? calloc((size_t)site_workers, sizeof(hl_site_buf_t)) : NULL;
        site_collection_failed = site_workers > 0 && !site_bufs;
        if (!site_collection_failed && site_workers > 0) {
            hl_site_ctx_t sc = {
                .nodes = all_site_nodes,
                .labels = all_site_labels,
                .node_count = site_node_count,
                .ctx = ctx,
                .worker_bufs = site_bufs,
                .worker_count = site_workers,
                .cancelled = ctx->cancelled,
            };
            atomic_init(&sc.next_idx, 0);
            atomic_init(&sc.allocation_failed, 0);

            cbm_parallel_for_opts_t opts = {.max_workers = site_workers,
                                            .force_pthreads = false};
            cbm_parallel_for(site_workers, hl_site_worker, &sc, opts);

            site_collection_failed =
                atomic_load_explicit(&sc.allocation_failed, memory_order_relaxed) != 0;
            if (!site_collection_failed) {
                for (int w = 0; w < site_workers; w++) {
                    if (!hl_append_items((void **)&sites, &site_count, &site_capacity,
                                         site_bufs[w].sites, site_bufs[w].count,
                                         sizeof(*sites))) {
                        site_collection_failed = true;
                        break;
                    }
                }
            }
        }
        if (site_bufs) {
            for (int worker_index = 0; worker_index < site_workers; worker_index++) {
                free(site_bufs[worker_index].sites);
            }
        }
        free(site_bufs);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(all_site_nodes);
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(all_site_labels);
    CBM_PROF_END_N("httplinks", "5_callsite_scan_parallel", t_sites, total_site_nodes);
    if (site_collection_failed) {
        free(routes);
        free(sites);
        return -1;
    }

    cbm_log_info("httplink.callsites", "count", itoa_hl(site_count));

    /* ── Phase 6: Match and link (serial) ─────────────────────── */
    CBM_PROF_START(t_match);
    int link_count = 0;
    if (sites && site_count > 0 && route_count > 0) {
        link_count = match_and_link(ctx, routes, route_count, sites, site_count);
    }
    CBM_PROF_END_N("httplinks", "6_match_link_seq", t_match, site_count);

    free(routes);
    free(sites);

    cbm_log_info("pass.done", "pass", "httplinks", "routes", itoa_hl(route_nodes), "calls",
                 itoa_hl(link_count));
    return 0;
}
