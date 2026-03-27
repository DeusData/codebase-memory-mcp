
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
 *   - CALLS edges with via=route_registration (registrar → handler)
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
#include "foundation/compat.h"
#include "foundation/compat_regex.h"

#include "yyjson/yyjson.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

/* ── Constants ────────────────────────────────────────────────── */
#define DOTTED_FRAG_BUF 260      /* buffer for slash-to-dot path conversion */
#define MIN_PATH_CONFIDENCE 0.25 /* minimum score to create HTTP_CALLS edge */
#define MODULE_WEIGHT 0.85       /* confidence weight for Module-sourced calls */

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

/* Read full source file, using cache if available. Returns malloc'd copy (caller must free). */
/* Read full source from disk (used for module route discovery — few files,
 * filtered by .php/.js extension). */
static char *read_full_source(const cbm_pipeline_ctx_t *ctx, const char *rel_path) {
    char path_buf[2048];
    snprintf(path_buf, sizeof(path_buf), "%s/%s", ctx->repo_path, rel_path);
    FILE *f = fopen(path_buf, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (sz <= 0 || sz > (long)10 * 1024 * 1024) {
        (void)fclose(f);
        return NULL;
    }
    char *source = malloc((size_t)sz + 1);
    if (!source) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(source, 1, (size_t)sz, f);
    (void)fclose(f);
    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
    source[nread] = '\0';
    return source;
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
            // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
            out[idx++] = strdup(yyjson_get_str(item));
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

/* ── Route discovery ───────────────────────────────────────────── */

/* Max routes per pass */
#define MAX_ROUTES 4096
#define MAX_CALL_SITES 4096

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
    if (n->file_path && n->start_line > 0 && n->end_line > 0 && total < max_out) {
        char *source = read_source_lines(ctx, n->file_path, n->start_line, n->end_line);
        if (source) {
            const char *fp = n->file_path;
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
            if (has_suffix(fp, ".php")) {
                nr = cbm_extract_laravel_routes(n->name, n->qualified_name, source, out + total,
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

/* Discover module-level routes (PHP Laravel, JS/TS Express at top level). */
static int discover_module_routes(const cbm_gbuf_node_t *mod, const cbm_pipeline_ctx_t *ctx,
                                  cbm_route_handler_t *out, int max_out) {
    if (!mod->file_path) {
        return 0;
    }

    bool is_php = has_suffix(mod->file_path, ".php");
    bool is_js = is_jsts_file(mod->file_path);
    if (!is_php && !is_js) {
        return 0;
    }

    /* Read full file (from cache or disk) */
    char *source = read_full_source(ctx, mod->file_path);
    if (!source) {
        return 0;
    }

    int total = 0;
    if (is_php) {
        total += cbm_extract_laravel_routes(mod->name, mod->qualified_name, source, out + total,
                                            max_out - total);
    }
    if (is_js) {
        total += cbm_extract_express_routes(mod->name, mod->qualified_name, source, out + total,
                                            max_out - total);
    }
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
        typedef struct {
            char var[128];
            char module[256];
        } import_entry_t;
        import_entry_t imports[64];
        memset(imports, 0, sizeof(imports));
        int import_count = 0;

        const char *p = source;
        cbm_regmatch_t pm[3];
        while (import_count < 64 && cbm_regexec(&import_re, p, 3, pm, 0) == 0) {
            int mlen = (pm[1].rm_eo - pm[1].rm_so);
            int vlen = (pm[2].rm_eo - pm[2].rm_so);
            if (mlen < 256 && vlen < 128) {
                snprintf(imports[import_count].module, 256, "%.*s", mlen, p + pm[1].rm_so);
                snprintf(imports[import_count].var, 128, "%.*s", vlen, p + pm[2].rm_so);
                import_count++;
            }
            p += pm[0].rm_eo;
        }

        /* Find include_router calls */
        p = source;
        while (cbm_regexec(&include_re, p, 3, pm, 0) == 0) {
            char var_name[128] = {0};
            char prefix[256] = {0};
            int vlen = (pm[1].rm_eo - pm[1].rm_so);
            int plen = (pm[2].rm_eo - pm[2].rm_so);
            if (vlen < 128) {
                snprintf(var_name, 128, "%.*s", vlen, p + pm[1].rm_so);
            }
            if (plen < 256) {
                snprintf(prefix, 256, "%.*s", plen, p + pm[2].rm_so);
            }
            p += pm[0].rm_eo;

            /* Find which module this var was imported from */
            const char *module_path = NULL;
            for (int i = 0; i < import_count; i++) {
                if (strcmp(imports[i].var, var_name) == 0) {
                    module_path = imports[i].module;
                    break;
                }
            }
            if (!module_path) {
                continue;
            }

            /* Convert dotted module path to file fragment */
            char file_frag[256];
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
                    char new_path[256];
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
        typedef struct {
            char var[128];
            char module[256];
        } import_entry_t;
        import_entry_t imports[64];
        memset(imports, 0, sizeof(imports));
        int import_count = 0;

        const char *p = source;
        cbm_regmatch_t pm[4];
        while (import_count < 64 && cbm_regexec(&require_re, p, 4, pm, 0) == 0) {
            int vlen = (pm[2].rm_eo - pm[2].rm_so);
            int mlen = (pm[3].rm_eo - pm[3].rm_so);
            if (vlen < 128 && mlen < 256) {
                snprintf(imports[import_count].var, 128, "%.*s", vlen, p + pm[2].rm_so);
                snprintf(imports[import_count].module, 256, "%.*s", mlen, p + pm[3].rm_so);
                import_count++;
            }
            p += pm[0].rm_eo;
        }
        p = source;
        while (import_count < 64 && cbm_regexec(&esimport_re, p, 3, pm, 0) == 0) {
            int vlen = (pm[1].rm_eo - pm[1].rm_so);
            int mlen = (pm[2].rm_eo - pm[2].rm_so);
            if (vlen < 128 && mlen < 256) {
                snprintf(imports[import_count].var, 128, "%.*s", vlen, p + pm[1].rm_so);
                snprintf(imports[import_count].module, 256, "%.*s", mlen, p + pm[2].rm_so);
                import_count++;
            }
            p += pm[0].rm_eo;
        }

        /* Find .use("/prefix", var) calls */
        p = source;
        while (cbm_regexec(&use_re, p, 3, pm, 0) == 0) {
            char prefix[256] = {0};
            char var_name[128] = {0};
            int plen = (pm[1].rm_eo - pm[1].rm_so);
            int vlen = (pm[2].rm_eo - pm[2].rm_so);
            if (plen < 256) {
                snprintf(prefix, 256, "%.*s", plen, p + pm[1].rm_so);
            }
            if (vlen < 128) {
                snprintf(var_name, 128, "%.*s", vlen, p + pm[2].rm_so);
            }
            p += pm[0].rm_eo;

            /* Resolve var → module path */
            const char *module_path = NULL;
            for (int i = 0; i < import_count; i++) {
                if (strcmp(imports[i].var, var_name) == 0) {
                    module_path = imports[i].module;
                    break;
                }
            }
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
                    char new_path[256];
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
                char called_name[128] = {0};
                char prefix[256] = {0};
                int nlen = (pm[1].rm_eo - pm[1].rm_so);
                int plen = (pm[2].rm_eo - pm[2].rm_so);
                if (nlen < 128) {
                    snprintf(called_name, 128, "%.*s", nlen, p + pm[1].rm_so);
                }
                if (plen < 256) {
                    snprintf(prefix, 256, "%.*s", plen, p + pm[2].rm_so);
                }
                p += pm[0].rm_eo;

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
                        char new_path[256];
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
            typedef struct {
                char var[128];
                char prefix[256];
            } var_prefix_t;
            var_prefix_t var_pfx[16];
            memset(var_pfx, 0, sizeof(var_pfx));
            int var_count = 0;

            p = caller_source;
            while (var_count < 16 && cbm_regexec(&group_var_re, p, 3, pm, 0) == 0) {
                int vlen = (pm[1].rm_eo - pm[1].rm_so);
                int plen = (pm[2].rm_eo - pm[2].rm_so);
                if (vlen < 128 && plen < 256) {
                    snprintf(var_pfx[var_count].var, 128, "%.*s", vlen, p + pm[1].rm_so);
                    snprintf(var_pfx[var_count].prefix, 256, "%.*s", plen, p + pm[2].rm_so);
                    var_count++;
                }
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
                        char arg_name[128] = {0};
                        int alen = (pm[1].rm_eo - pm[1].rm_so);
                        if (alen < 128) {
                            snprintf(arg_name, 128, "%.*s", alen, p + pm[1].rm_so);
                        }
                        p += pm[0].rm_eo;

                        for (int v = 0; v < var_count; v++) {
                            if (strcmp(var_pfx[v].var, arg_name) == 0) {
                                char *prefix = var_pfx[v].prefix;
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
                                    char new_path[256];
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

/* ── Registration call edges ───────────────────────────────────── */

/* Create CALLS edges from route-registering functions to handler functions.
 * e.g., RegisterRoutes has .POST("/path", h.CreateOrder) → CALLS edge to CreateOrder. */
static int create_registration_call_edges(cbm_pipeline_ctx_t *ctx, cbm_route_handler_t *routes,
                                          int route_count) {
    int count = 0;
    for (int i = 0; i < route_count; i++) {
        if (routes[i].handler_ref[0] == '\0') {
            continue;
        }

        /* Find the registering function node */
        const cbm_gbuf_node_t *registrar = cbm_gbuf_find_by_qn(ctx->gbuf, routes[i].qualified_name);
        if (!registrar) {
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

        /* Create CALLS edge with via=route_registration */
        cbm_gbuf_insert_edge(ctx->gbuf, registrar->id, handler->id, "CALLS",
                             "{\"via\":\"route_registration\"}");
        count++;
    }
    return count;
}

/* ── Route node insertion ──────────────────────────────────────── */

/* Insert Route nodes and HANDLES edges for discovered routes.
 * Uses ResolvedHandlerQN if set (from createRegistrationCallEdges). */
static int insert_route_nodes(cbm_pipeline_ctx_t *ctx, cbm_route_handler_t *routes,
                              int route_count) {
    int count = 0;
    for (int i = 0; i < route_count; i++) {
        cbm_route_handler_t *rh = &routes[i];

        /* Reject obviously invalid route paths.
         * Vendored/minified JS files (e.g. tsc.js, typescript.js) inside non-JS
         * repos can produce false positives where JS operators/keywords get
         * matched as route paths by the Express extractor. */
        {
            const char *p = rh->path;
            /* Skip empty paths */
            if (!p || !*p) continue;

            /* Reject paths that are JS operators or keywords — not valid URL routes */
            static const char *const invalid_paths[] = {
                "!", "+", "++", "-", "--", ":", "~", "void", "null", "true",
                "false", "throw", "this", "typeof", "delete", "new", "return",
                "undefined", "NaN", "Infinity", "var", "let", "const",
                "function", "class", "if", "else", "for", "while", "do",
                "switch", "case", "break", "continue", "try", "catch",
                "finally", "with", "in", "of", "yield", "await", "async",
                "super", "import", "export", "default", "extends", "static",
                "_this", "self", "__proto__", "arguments", "range",
                NULL
            };
            bool rejected = false;
            /* Work with a trimmed copy for comparison */
            char trimmed[256];
            /* Trim leading whitespace */
            while (*p == ' ' || *p == '\t') p++;
            strncpy(trimmed, p, sizeof(trimmed) - 1);
            trimmed[sizeof(trimmed) - 1] = '\0';
            /* Trim trailing whitespace */
            size_t tlen = strlen(trimmed);
            while (tlen > 0 && (trimmed[tlen - 1] == ' ' || trimmed[tlen - 1] == '\t' ||
                                trimmed[tlen - 1] == '\n' || trimmed[tlen - 1] == '\r')) {
                trimmed[--tlen] = '\0';
            }
            for (int k = 0; invalid_paths[k]; k++) {
                if (strcmp(trimmed, invalid_paths[k]) == 0) {
                    rejected = true;
                    break;
                }
            }
            if (rejected) continue;

            /* Reject single-character non-slash paths (e.g. "*", "?", "#") */
            if (p[0] && !p[1] && p[0] != '/') continue;

            /* Reject paths that contain no alphanumeric or slash characters.
             * Valid routes like "/api/v1" always have at least one alnum. */
            bool has_alnum_or_slash = false;
            for (const char *c = p; *c; c++) {
                if ((*c >= 'a' && *c <= 'z') || (*c >= 'A' && *c <= 'Z') ||
                    (*c >= '0' && *c <= '9') || *c == '/') {
                    has_alnum_or_slash = true;
                    break;
                }
            }
            if (!has_alnum_or_slash) continue;
        }

        /* Build Route QN and name */
        char normal_method[16];
        snprintf(normal_method, sizeof(normal_method), "%s", rh->method[0] ? rh->method : "ANY");

        /* Normalize path for QN: replace / with _ */
        char normal_path[256];
        snprintf(normal_path, sizeof(normal_path), "%s", rh->path);
        for (char *c = normal_path; *c; c++) {
            if (*c == '/') {
                *c = '_';
            }
        }
        /* Trim leading/trailing underscores */
        char *np = normal_path;
        while (*np == '_') {
            np++;
        }
        size_t nplen = strlen(np);
        while (nplen > 0 && np[nplen - 1] == '_') {
            np[--nplen] = '\0';
        }

        char route_qn[1024];
        snprintf(route_qn, sizeof(route_qn), "%s.route.%s.%s", rh->qualified_name, normal_method,
                 np);

        char route_name[256];
        snprintf(route_name, sizeof(route_name), "%s %s", normal_method, rh->path);

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

        /* Build properties JSON */
        char props[512];
        int n =
            snprintf(props, sizeof(props), "{\"method\":\"%s\",\"path\":\"%s\",\"handler\":\"%s\"",
                     rh->method, rh->path, handler_qn);
        if (rh->protocol[0]) {
            n += snprintf(props + n, sizeof(props) - (size_t)n, ",\"protocol\":\"%s\"",
                          rh->protocol);
        } else if (h_id > 0 && h_file[0] && h_start > 0) {
            /* Detect protocol from handler source */
            char *hsource = read_source_lines(ctx, h_file, h_start, h_end);
            if (hsource) {
                const char *proto = cbm_detect_protocol(hsource);
                if (proto[0]) {
                    n += snprintf(props + n, sizeof(props) - (size_t)n, ",\"protocol\":\"%s\"",
                                  proto);
                }
                free(hsource);
            }
        }
        snprintf(props + n, sizeof(props) - (size_t)n, "}");

        /* Create Route node */
        int64_t route_id = cbm_gbuf_upsert_node(ctx->gbuf, "Route", route_name, route_qn, h_file,
                                                h_start, h_end, props);
        if (route_id <= 0) {
            continue;
        }

        /* Create HANDLES edge: handler → Route */
        if (h_id > 0) {
            int64_t source = h_id;
            int64_t target = route_id;
            cbm_gbuf_insert_edge(ctx->gbuf, source, target, "HANDLES", "{}");

            /* Mark handler as entry point */
            char *new_props = set_entry_point(h_props_json);
            if (new_props) {
                cbm_gbuf_upsert_node(ctx->gbuf, h_label, h_name, h_qn, h_file, h_start, h_end,
                                     new_props);
                free(new_props);
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
            double score = cbm_path_match_score(cs->path, rh->path);
            if (score < MIN_PATH_CONFIDENCE) {
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

/* Per-worker buffer for route discovery. */
#define HL_ROUTES_PER_WORKER 512
typedef struct {
    cbm_route_handler_t routes[HL_ROUTES_PER_WORKER];
    int count;
} hl_route_buf_t;

/* Context for parallel route discovery. */
typedef struct {
    const hl_work_item_t *items;
    int item_count;
    const cbm_pipeline_ctx_t *ctx;
    hl_route_buf_t *worker_bufs; /* one per worker */
    int worker_count;
    _Atomic int next_idx;
    _Atomic int *cancelled;
} hl_route_ctx_t;

static void hl_route_worker(int worker_id, void *arg) {
    hl_route_ctx_t *rc = arg;
    hl_route_buf_t *buf = &rc->worker_bufs[worker_id];

    while (1) {
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

        int space = HL_ROUTES_PER_WORKER - buf->count;
        if (space <= 0) {
            continue; /* worker buffer full */
        }

        int nr;
        if (item->is_module) {
            nr = discover_module_routes(item->node, rc->ctx, buf->routes + buf->count, space);
        } else {
            nr = discover_node_routes(item->node, rc->ctx, buf->routes + buf->count, space);
        }
        buf->count += nr;
    }
}

/* Per-worker buffer for call site discovery. */
#define HL_SITES_PER_WORKER 512
typedef struct {
    cbm_http_call_site_t sites[HL_SITES_PER_WORKER];
    int count;
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
    _Atomic int *cancelled;
} hl_site_ctx_t;

static void hl_site_worker(int worker_id, void *arg) {
    hl_site_ctx_t *sc = arg;
    hl_site_buf_t *buf = &sc->worker_bufs[worker_id];

    while (1) {
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

        /* Extract URL paths */
        char *paths[64];
        int path_count = cbm_extract_url_paths(source, paths, 64);

        int space = HL_SITES_PER_WORKER - buf->count;
        for (int p = 0; p < path_count && space > 0; p++) {
            cbm_http_call_site_t *site = &buf->sites[buf->count];
            snprintf(site->path, sizeof(site->path), "%s", paths[p]);
            site->method[0] = '\0';
            snprintf(site->source_name, sizeof(site->source_name), "%s", n->name);
            snprintf(site->source_qn, sizeof(site->source_qn), "%s", n->qualified_name);
            snprintf(site->source_label, sizeof(site->source_label), "%s", sc->labels[idx]);
            site->is_async = is_async;
            buf->count++;
            space--;
        }
        /* Free any remaining paths we couldn't store */
        for (int p = 0; p < path_count; p++) {
            free(paths[p]);
        }

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

    /* ── Phase 1: Route collection from prescan or parallel discovery ── */

    /* Need label_counts for fallback path */
    const char *route_labels[] = {"Function", "Method", "Module"};
    const cbm_gbuf_node_t **label_nodes[3] = {NULL, NULL, NULL};
    int label_counts[3] = {0, 0, 0};

    for (int li = 0; li < 3; li++) {
        cbm_gbuf_find_by_label(ctx->gbuf, route_labels[li], &label_nodes[li], &label_counts[li]);
    }

    cbm_route_handler_t *routes = calloc(MAX_ROUTES, sizeof(cbm_route_handler_t));
    if (!routes) {
        return -1;
    }
    int route_count = 0;

    if (ctx->prescan_cache) {
        /* Fast path: routes were extracted during the extraction phase.
         * Zero disk reads, zero JSON parsing, zero node iteration. */
        for (int fi = 0; fi < ctx->prescan_count; fi++) {
            const cbm_prescan_t *ps = &ctx->prescan_cache[fi];
            for (int ri = 0; ri < ps->route_count && route_count < MAX_ROUTES; ri++) {
                const cbm_prescan_route_t *src = &ps->routes[ri];
                cbm_route_handler_t *dst = &routes[route_count];
                snprintf(dst->path, sizeof(dst->path), "%s", src->path);
                snprintf(dst->method, sizeof(dst->method), "%s", src->method);
                snprintf(dst->function_name, sizeof(dst->function_name), "%s", src->function_name);
                snprintf(dst->qualified_name, sizeof(dst->qualified_name), "%s",
                         src->qualified_name);
                snprintf(dst->handler_ref, sizeof(dst->handler_ref), "%s", src->handler_ref);
                dst->resolved_handler_qn[0] = '\0';
                snprintf(dst->protocol, sizeof(dst->protocol), "%s", src->protocol);
                route_count++;
            }
        }
    } else {
        /* Fallback: parallel route discovery from disk (sequential pipeline path) */
        int total_route_nodes = 0;
        for (int li = 0; li < 3; li++) {
            total_route_nodes += label_counts[li];
        }

        hl_work_item_t *work_items = NULL;
        if (total_route_nodes > 0) {
            work_items = malloc((size_t)total_route_nodes * sizeof(hl_work_item_t));
        }

        int wi = 0;
        if (work_items) {
            for (int li = 0; li < 3; li++) {
                for (int i = 0; i < label_counts[li]; i++) {
                    work_items[wi].node = label_nodes[li][i];
                    work_items[wi].is_module = (li == 2);
                    wi++;
                }
            }
        }

        hl_route_buf_t *route_bufs = calloc((size_t)worker_count, sizeof(hl_route_buf_t));

        if (work_items && route_bufs && wi > 0) {
            hl_route_ctx_t rc = {
                .items = work_items,
                .item_count = wi,
                .ctx = ctx,
                .worker_bufs = route_bufs,
                .worker_count = worker_count,
                .cancelled = ctx->cancelled,
            };
            atomic_init(&rc.next_idx, 0);

            cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
            cbm_parallel_for(worker_count, hl_route_worker, &rc, opts);

            for (int w = 0; w < worker_count; w++) {
                int to_copy = route_bufs[w].count;
                if (to_copy > MAX_ROUTES - route_count) {
                    to_copy = MAX_ROUTES - route_count;
                }
                if (to_copy > 0) {
                    memcpy(routes + route_count, route_bufs[w].routes,
                           (size_t)to_copy * sizeof(cbm_route_handler_t));
                    route_count += to_copy;
                }
            }
        }
        free(work_items);
        free(route_bufs);
    }

    cbm_log_info("httplink.routes", "count", itoa_hl(route_count));

    /* ── Phase 2: Resolve cross-file prefixes (serial) ────────── */
    resolve_cross_file_group_prefixes(ctx, routes, route_count);
    resolve_fastapi_prefixes(ctx, routes, route_count);
    resolve_express_prefixes(ctx, routes, route_count);

    /* ── Phase 3: Registration edges (serial) ─────────────────── */
    int reg_edges = create_registration_call_edges(ctx, routes, route_count);
    if (reg_edges > 0) {
        cbm_log_info("httplink.registration_edges", "count", itoa_hl(reg_edges));
    }

    /* ── Phase 4: Route nodes + HANDLES edges (serial) ────────── */
    int route_nodes = insert_route_nodes(ctx, routes, route_count);

    /* ── Phase 5: Call site collection from prescan cache ────────── */
    /* During extraction, each file's source was scanned for HTTP/async
     * keywords and URL paths. Results are in ctx->prescan_cache[file_idx].
     * This eliminates 2M+ disk reads (the old approach read every function
     * from disk just to check for keywords, then discarded 99.9%). */

    cbm_http_call_site_t *sites = calloc(MAX_CALL_SITES, sizeof(cbm_http_call_site_t));
    int site_count = 0;

    if (sites && ctx->prescan_cache) {
        for (int fi = 0; fi < ctx->prescan_count; fi++) {
            const cbm_prescan_t *ps = &ctx->prescan_cache[fi];
            for (int si = 0; si < ps->http_site_count && site_count < MAX_CALL_SITES; si++) {
                const cbm_prescan_http_site_t *src = &ps->http_sites[si];
                cbm_http_call_site_t *dst = &sites[site_count];
                snprintf(dst->path, sizeof(dst->path), "%s", src->path);
                snprintf(dst->method, sizeof(dst->method), "%s", src->method);
                snprintf(dst->source_name, sizeof(dst->source_name), "%s", src->source_name);
                snprintf(dst->source_qn, sizeof(dst->source_qn), "%s", src->source_qn);
                snprintf(dst->source_label, sizeof(dst->source_label), "%s", src->source_label);
                dst->is_async = src->is_async;
                site_count++;
            }
        }
    } else if (sites) {
        /* Fallback: no prescan (sequential pipeline path).
         * Use the old parallel disk-reading approach. */
        const char *site_labels[] = {"Function", "Method"};
        const cbm_gbuf_node_t **all_site_nodes = NULL;
        const char **all_site_labels = NULL;
        int total_site_nodes = 0;

        for (int li = 0; li < 2; li++) {
            total_site_nodes += label_counts[li];
        }

        if (total_site_nodes > 0) {
            all_site_nodes = malloc((size_t)total_site_nodes * sizeof(cbm_gbuf_node_t *));
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            all_site_labels = malloc((size_t)total_site_nodes * sizeof(const char *));
        }

        if (all_site_nodes && all_site_labels) {
            int si2 = 0;
            for (int li = 0; li < 2; li++) {
                for (int i = 0; i < label_counts[li]; i++) {
                    all_site_nodes[si2] = label_nodes[li][i];
                    all_site_labels[si2] = site_labels[li];
                    si2++;
                }
            }

            hl_site_buf_t *site_bufs = calloc((size_t)worker_count, sizeof(hl_site_buf_t));
            if (site_bufs && si2 > 0) {
                hl_site_ctx_t sc = {
                    .nodes = all_site_nodes,
                    .labels = all_site_labels,
                    .node_count = si2,
                    .ctx = ctx,
                    .worker_bufs = site_bufs,
                    .worker_count = worker_count,
                    .cancelled = ctx->cancelled,
                };
                atomic_init(&sc.next_idx, 0);

                cbm_parallel_for_opts_t opts = {.max_workers = worker_count,
                                                .force_pthreads = false};
                cbm_parallel_for(worker_count, hl_site_worker, &sc, opts);

                for (int w = 0; w < worker_count; w++) {
                    int to_copy = site_bufs[w].count;
                    if (to_copy > MAX_CALL_SITES - site_count) {
                        to_copy = MAX_CALL_SITES - site_count;
                    }
                    if (to_copy > 0) {
                        memcpy(sites + site_count, site_bufs[w].sites,
                               (size_t)to_copy * sizeof(cbm_http_call_site_t));
                        site_count += to_copy;
                    }
                }
            }
            free(site_bufs);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(all_site_nodes);
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(all_site_labels);
    }

    cbm_log_info("httplink.callsites", "count", itoa_hl(site_count));

    /* ── Phase 6: Match and link (serial) ─────────────────────── */
    int link_count = 0;
    if (sites && site_count > 0 && route_count > 0) {
        link_count = match_and_link(ctx, routes, route_count, sites, site_count);
    }

    free(routes);
    free(sites);

    cbm_log_info("pass.done", "pass", "httplinks", "routes", itoa_hl(route_nodes), "calls",
                 itoa_hl(link_count));
    return 0;
}
