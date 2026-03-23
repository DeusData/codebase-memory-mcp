/*
 * pass_parallel.c — Three-phase parallel pipeline.
 *
 * Phase 3A: Parallel extract + create definition nodes (per-worker gbufs)
 * Phase 3B: Serial registry build + edge creation from cached results
 * Phase 4:  Parallel call/usage/semantic resolution (per-worker edge bufs)
 *
 * Each file is read and parsed ONCE (Phase 3A). The CBMFileResult is cached
 * and reused for resolution (Phase 4), eliminating 3x redundant I/O + parsing.
 *
 * Depends on: worker_pool, graph_buffer (shared IDs + merge), extraction (cbm.h)
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/worker_pool.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "graph_buffer/graph_buffer.h"
// NOLINTNEXTLINE(misc-include-cleaner) — platform.h included for interface contract
#include "foundation/platform.h"
#include "foundation/log.h"
#include "foundation/slab_alloc.h"
#include "foundation/mem.h"
#include "foundation/compat_regex.h"
#include "pipeline/httplink.h"
#include "cbm.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t extract_now_ns(void) {
    struct timespec ts;
    cbm_clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
}

/* ── Helpers (duplicated from pass files — kept static for isolation) ── */

/* Read file into a malloc'd buffer (= mimalloc in production). */
static char *read_file(const char *path, int *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > (long)100 * 1024 * 1024) {
        (void)fclose(f);
        return NULL;
    }
    char *buf = (char *)malloc((size_t)size + 1);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, 1, (size_t)size, f);
    (void)fclose(f);
    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* Free source buffer. */
static void free_source(char *buf) {
    free(buf);
}

static const char *itoa_log(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Append a JSON-escaped string value to buf at position *pos. */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void append_json_string(char *buf, size_t bufsize, size_t *pos, const char *key,
                               const char *val) {
    if (!val || !val[0]) {
        return;
    }
    if (*pos >= bufsize - 10) {
        return;
    }
    size_t p = *pos;
    int w = snprintf(buf + p, bufsize - p, ",\"%s\":\"", key);
    if (w <= 0 || (size_t)w >= bufsize - p) {
        return;
    }
    p += (size_t)w;
    for (const char *s = val; *s && p < bufsize - 3; s++) {
        switch (*s) {
        case '"':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = '"';
            }
            break;
        case '\\':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = '\\';
            }
            break;
        case '\n':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 'n';
            }
            break;
        case '\r':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 'r';
            }
            break;
        case '\t':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 't';
            }
            break;
        default:
            buf[p++] = *s;
            break;
        }
    }
    if (p < bufsize - 1) {
        buf[p++] = '"';
    }
    buf[p] = '\0';
    *pos = p;
}

static void build_def_props(char *buf, size_t bufsize, const CBMDefinition *def) {
    int n = snprintf(buf, bufsize,
                     "{\"complexity\":%d,\"lines\":%d,\"is_exported\":%s,"
                     "\"is_test\":%s,\"is_entry_point\":%s",
                     def->complexity, def->lines, def->is_exported ? "true" : "false",
                     def->is_test ? "true" : "false", def->is_entry_point ? "true" : "false");
    if (n <= 0 || (size_t)n >= bufsize) {
        buf[0] = '\0';
        return;
    }
    size_t pos = (size_t)n;
    append_json_string(buf, bufsize, &pos, "docstring", def->docstring);
    append_json_string(buf, bufsize, &pos, "signature", def->signature);
    if (def->decorators && def->decorators[0] && pos < bufsize - 2) {
        const char *prefix = ",\"decorators\":[";
        size_t plen = strlen(prefix);
        if (pos + plen < bufsize) {
            memcpy(buf + pos, prefix, plen);
            pos += plen;
        }
        for (int i = 0; def->decorators[i]; i++) {
            if (i > 0 && pos < bufsize - 1) {
                buf[pos++] = ',';
            }
            if (pos < bufsize - 1) {
                buf[pos++] = '"';
            }
            for (const char *s = def->decorators[i]; *s && pos < bufsize - 2; s++) {
                if (*s == '"' || *s == '\\') {
                    buf[pos++] = '\\';
                    if (pos >= bufsize - 2) {
                        break;
                    }
                }
                buf[pos++] = *s;
            }
            if (pos < bufsize - 1) {
                buf[pos++] = '"';
            }
        }
        if (pos < bufsize - 1) {
            buf[pos++] = ']';
        }
        buf[pos] = '\0';
    }
    if (pos < bufsize - 1) {
        buf[pos] = '}';
        buf[pos + 1] = '\0';
    }
}

/* Build import map from graph buffer IMPORTS edges (read-only access to gbuf). */
static int build_import_map(const cbm_gbuf_t *gbuf, const char *project_name, const char *rel_path,
                            // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                            const char ***out_keys, const char ***out_vals, int *out_count) {
    *out_keys = NULL;
    *out_vals = NULL;
    *out_count = 0;

    char *file_qn = cbm_pipeline_fqn_compute(project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(gbuf, file_qn);
    free(file_qn);
    if (!file_node) {
        return 0;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    int rc =
        cbm_gbuf_find_edges_by_source_type(gbuf, file_node->id, "IMPORTS", &edges, &edge_count);
    if (rc != 0 || edge_count == 0) {
        return 0;
    }

    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    const char **keys = calloc(edge_count, sizeof(const char *));
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    const char **vals = calloc(edge_count, sizeof(const char *));
    int count = 0;

    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_edge_t *e = edges[i];
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gbuf, e->target_id);
        if (!target || !e->properties_json) {
            continue;
        }
        const char *start = strstr(e->properties_json, "\"local_name\":\"");
        if (start) {
            start += strlen("\"local_name\":\"");
            const char *end = strchr(start, '"');
            if (end && end > start) {
                // NOLINTNEXTLINE(misc-include-cleaner) — strndup provided by standard header
                keys[count] = cbm_strndup(start, end - start);
                vals[count] = target->qualified_name;
                count++;
            }
        }
    }

    *out_keys = keys;
    *out_vals = vals;
    *out_count = count;
    return 0;
}

static void free_import_map(const char **keys, const char **vals, int count) {
    if (keys) {
        for (int i = 0; i < count; i++) {
            free((void *)keys[i]);
        }
        free((void *)keys);
    }
    if (vals) {
        free((void *)vals);
    }
}

static bool is_checked_exception(const char *name) {
    if (!name) {
        return false;
    }
    if (strstr(name, "Error") || strstr(name, "Panic") || strstr(name, "error") ||
        strstr(name, "panic")) {
        return false;
    }
    return true;
}

static const char *resolve_as_class(const cbm_registry_t *reg, const char *name,
                                    const char *module_qn, const char **imp_keys,
                                    const char **imp_vals, int imp_count) {
    cbm_resolution_t res =
        cbm_registry_resolve(reg, name, module_qn, imp_keys, imp_vals, imp_count);
    if (!res.qualified_name || res.qualified_name[0] == '\0') {
        return NULL;
    }
    const char *label = cbm_registry_label_of(reg, res.qualified_name);
    if (!label) {
        return NULL;
    }
    if (strcmp(label, "Class") != 0 && strcmp(label, "Interface") != 0 &&
        strcmp(label, "Type") != 0 && strcmp(label, "Enum") != 0) {
        return NULL;
    }
    return res.qualified_name;
}

static void extract_decorator_func(const char *dec, char *out, size_t outsz) {
    out[0] = '\0';
    if (!dec) {
        return;
    }
    const char *start = dec;
    if (*start == '@') {
        start++;
    }
    const char *paren = strchr(start, '(');
    size_t len = paren ? (size_t)(paren - start) : strlen(start);
    if (len == 0 || len >= outsz) {
        return;
    }
    memcpy(out, start, len);
    out[len] = '\0';
}

/* ── File sort for tail-latency reduction ────────────────────────── */

typedef struct {
    int idx;
    // NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
    int64_t size;
} file_sort_entry_t;

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int compare_by_size_desc(const void *a, const void *b) {
    const file_sort_entry_t *fa = a;
    const file_sort_entry_t *fb = b;
    if (fb->size > fa->size) {
        return 1;
    }
    if (fb->size < fa->size) {
        return -1;
    }
    return 0;
}

/* ── Phase 3A: Parallel Extract ──────────────────────────────────── */

#define CBM_CACHE_LINE 128

typedef struct __attribute__((aligned(CBM_CACHE_LINE))) {
    cbm_gbuf_t *local_gbuf;
    int nodes_created;
    int errors;
    char _pad[CBM_CACHE_LINE - sizeof(cbm_gbuf_t *) - (2 * sizeof(int))];
} extract_worker_state_t;

typedef struct {
    // NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
    const cbm_file_info_t *files;
    file_sort_entry_t *sorted;
    int file_count;
    const char *project_name;
    const char *repo_path;

    extract_worker_state_t *workers;
    int max_workers;
    _Atomic int next_worker_id;

    CBMFileResult **result_cache;
    cbm_prescan_t *prescan_cache;
    _Atomic int64_t *shared_ids;
    _Atomic int *cancelled;
    _Atomic int next_file_idx;
} extract_ctx_t;

/* ── Pre-scan: extract HTTP sites + config refs while source is in memory ── */

/* Extract lines [start_line..end_line] from source buffer as a malloc'd string. */
static char *extract_lines(const char *source, int source_len, int start_line, int end_line) {
    if (start_line <= 0 || end_line <= 0 || end_line < start_line) {
        return NULL;
    }
    int line = 1;
    const char *p = source;
    const char *end = source + source_len;
    const char *range_start = NULL;
    const char *range_end = NULL;

    while (p < end) {
        if (line == start_line) {
            range_start = p;
        }
        const char *nl = memchr(p, '\n', (size_t)(end - p));
        if (!nl) {
            nl = end;
        }
        if (line == end_line) {
            range_end = (nl < end) ? nl + 1 : end;
            break;
        }
        p = nl + 1;
        line++;
    }
    if (!range_start) {
        return NULL;
    }
    if (!range_end) {
        range_end = end;
    }
    size_t len = (size_t)(range_end - range_start);
    // NOLINTNEXTLINE(clang-analyzer-optin.taint.TaintedAlloc)
    char *out = malloc(len + 1);
    if (!out) {
        return NULL;
    }
    memcpy(out, range_start, len);
    out[len] = '\0';
    return out;
}

/* Scan function bodies for HTTP/async keywords, extract URL paths. */
static void prescan_http_sites(const char *source, int source_len, const CBMFileResult *result,
                               cbm_prescan_t *ps) {
    for (int d = 0; d < result->defs.count; d++) {
        const CBMDefinition *def = &result->defs.items[d];
        if (!def->qualified_name || !def->name || !def->label) {
            continue;
        }
        if (strcmp(def->label, "Function") != 0 && strcmp(def->label, "Method") != 0) {
            continue;
        }
        if (def->start_line == 0 || def->end_line == 0) {
            continue;
        }
        /* Skip Python dunder methods */
        size_t nlen = strlen(def->name);
        if (nlen > 4 && def->name[0] == '_' && def->name[1] == '_' && def->name[nlen - 1] == '_' &&
            def->name[nlen - 2] == '_') {
            continue;
        }

        char *func_src =
            extract_lines(source, source_len, (int)def->start_line, (int)def->end_line);
        if (!func_src) {
            continue;
        }

        /* Keyword pre-filter */
        bool has_http = false;
        for (int k = 0; k < cbm_http_client_keywords_count; k++) {
            if (strstr(func_src, cbm_http_client_keywords[k])) {
                has_http = true;
                break;
            }
        }
        bool has_async = false;
        for (int k = 0; k < cbm_async_dispatch_keywords_count; k++) {
            if (strstr(func_src, cbm_async_dispatch_keywords[k])) {
                has_async = true;
                break;
            }
        }

        if (!has_http && !has_async) {
            free(func_src);
            continue;
        }

        bool is_async = (has_async && !has_http) != 0;

        /* Extract URL paths */
        char *paths[64];
        int path_count = cbm_extract_url_paths(func_src, paths, 64);

        for (int p = 0; p < path_count; p++) {
            cbm_prescan_http_site_t *site =
                realloc(ps->http_sites,
                        (size_t)(ps->http_site_count + 1) * sizeof(cbm_prescan_http_site_t));
            if (!site) {
                free(paths[p]);
                continue;
            }
            ps->http_sites = site;
            cbm_prescan_http_site_t *s = &ps->http_sites[ps->http_site_count];
            snprintf(s->path, sizeof(s->path), "%s", paths[p]);
            s->method[0] = '\0';
            snprintf(s->source_name, sizeof(s->source_name), "%s", def->name);
            snprintf(s->source_qn, sizeof(s->source_qn), "%s", def->qualified_name);
            snprintf(s->source_label, sizeof(s->source_label), "%s", def->label);
            s->is_async = is_async;
            ps->http_site_count++;
            free(paths[p]);
        }
        /* Free any remaining paths we couldn't store */
        for (int p = path_count; p < path_count; p++) {
            free(paths[p]);
        }
        free(func_src);
    }
}

/* Helper: append a route to prescan results. */
static void prescan_add_route(cbm_prescan_t *ps, const cbm_route_handler_t *rh) {
    cbm_prescan_route_t *routes =
        realloc(ps->routes, (size_t)(ps->route_count + 1) * sizeof(cbm_prescan_route_t));
    if (!routes) {
        return;
    }
    ps->routes = routes;
    cbm_prescan_route_t *r = &routes[ps->route_count];
    snprintf(r->path, sizeof(r->path), "%s", rh->path);
    snprintf(r->method, sizeof(r->method), "%s", rh->method);
    snprintf(r->function_name, sizeof(r->function_name), "%s", rh->function_name);
    snprintf(r->qualified_name, sizeof(r->qualified_name), "%s", rh->qualified_name);
    snprintf(r->handler_ref, sizeof(r->handler_ref), "%s", rh->handler_ref);
    snprintf(r->protocol, sizeof(r->protocol), "%s", rh->protocol);
    ps->route_count++;
}

/* Check file extension for source-based route languages. */
static bool is_route_source_lang(const char *path) {
    if (!path) {
        return false;
    }
    size_t len = strlen(path);
    /* Go */
    if (len > 3 && strcmp(path + len - 3, ".go") == 0) {
        return true;
    }
    /* JavaScript/TypeScript */
    if (len > 3 && (strcmp(path + len - 3, ".js") == 0 || strcmp(path + len - 3, ".ts") == 0)) {
        return true;
    }
    if (len > 4 && (strcmp(path + len - 4, ".jsx") == 0 || strcmp(path + len - 4, ".tsx") == 0)) {
        return true;
    }
    /* PHP */
    if (len > 4 && strcmp(path + len - 4, ".php") == 0) {
        return true;
    }
    /* Kotlin */
    if (len > 3 && strcmp(path + len - 3, ".kt") == 0) {
        return true;
    }
    if (len > 4 && strcmp(path + len - 4, ".kts") == 0) {
        return true;
    }
    return false;
}

/* Scan function definitions for HTTP routes (decorator + source-based). */
static void prescan_routes(const char *source, int source_len, const CBMFileResult *result,
                           const char *rel_path, cbm_prescan_t *ps) {
    bool is_route_lang = is_route_source_lang(rel_path);

    for (int d = 0; d < result->defs.count; d++) {
        const CBMDefinition *def = &result->defs.items[d];
        if (!def->qualified_name || !def->name) {
            continue;
        }

        cbm_route_handler_t routes[16];
        int total = 0;

        /* Decorator-based routes (Python, Java) */
        if (def->decorators && def->decorators[0]) {
            int ndec = 0;
            while (def->decorators[ndec]) {
                ndec++;
            }
            int nr = cbm_extract_python_routes(def->name, def->qualified_name,
                                               (const char **)def->decorators, ndec, routes + total,
                                               16 - total);
            total += nr;
            nr = cbm_extract_java_routes(def->name, def->qualified_name,
                                         (const char **)def->decorators, ndec, routes + total,
                                         16 - total);
            total += nr;
        }

        /* Source-based routes (Go, Express, Laravel, Ktor) — only for matching languages */
        if (is_route_lang && def->start_line > 0 && def->end_line > 0 && total < 16) {
            char *func_src =
                extract_lines(source, source_len, (int)def->start_line, (int)def->end_line);
            if (func_src) {
                int nr = cbm_extract_go_routes(def->name, def->qualified_name, func_src,
                                               routes + total, 16 - total);
                total += nr;
                nr = cbm_extract_express_routes(def->name, def->qualified_name, func_src,
                                                routes + total, 16 - total);
                total += nr;
                nr = cbm_extract_laravel_routes(def->name, def->qualified_name, func_src,
                                                routes + total, 16 - total);
                total += nr;
                nr = cbm_extract_ktor_routes(def->name, def->qualified_name, func_src,
                                             routes + total, 16 - total);
                total += nr;
                free(func_src);
            }
        }

        for (int r = 0; r < total; r++) {
            prescan_add_route(ps, &routes[r]);
        }
    }

    /* Module-level routes (PHP Laravel, JS/TS Express) */
    if (is_route_lang && source_len > 0) {
        size_t plen = strlen(rel_path);
        bool is_php = (plen > 4 && strcmp(rel_path + plen - 4, ".php") == 0) != 0;
        bool is_js = ((plen > 3 && (strcmp(rel_path + plen - 3, ".js") == 0 ||
                                    strcmp(rel_path + plen - 3, ".ts") == 0)) ||
                      (plen > 4 && (strcmp(rel_path + plen - 4, ".jsx") == 0 ||
                                    strcmp(rel_path + plen - 4, ".tsx") == 0))) != 0;
        if (is_php || is_js) {
            /* Build a module name from rel_path */
            const char *slash = strrchr(rel_path, '/');
            const char *basename = slash ? slash + 1 : rel_path;
            cbm_route_handler_t mod_routes[16];
            int total = 0;
            if (is_php) {
                total += cbm_extract_laravel_routes(basename, "", source, mod_routes + total,
                                                    16 - total);
            }
            if (is_js) {
                total += cbm_extract_express_routes(basename, "", source, mod_routes + total,
                                                    16 - total);
            }
            for (int r = 0; r < total; r++) {
                prescan_add_route(ps, &mod_routes[r]);
            }
        }
    }
}

/* Scan full source for config file path references in string literals. */
static void prescan_config_refs(const char *source, cbm_prescan_t *ps) {
    /* Quick reject: no config extension substring in source at all */
    static const char *exts[] = {".toml", ".yaml", ".yml", ".ini", ".json",
                                 ".xml",  ".conf", ".cfg", ".env", NULL};
    bool has_any = false;
    for (int i = 0; exts[i]; i++) {
        if (strstr(source, exts[i])) {
            has_any = true;
            break;
        }
    }
    if (!has_any) {
        return;
    }

    cbm_regex_t re;
    if (cbm_regcomp(&re, "[\"']([^\"']*\\.(toml|yaml|yml|ini|json|xml|conf|cfg|env))[\"']",
                    CBM_REG_EXTENDED) != 0) {
        return;
    }

    cbm_regmatch_t match[3];
    const char *cursor = source;
    while (cbm_regexec(&re, cursor, 3, match, 0) == 0) {
        int start = match[1].rm_so;
        int end = match[1].rm_eo;
        int ref_len = end - start;

        if (ref_len > 0 && ref_len < 256) {
            cbm_prescan_config_ref_t *refs =
                realloc(ps->config_refs,
                        (size_t)(ps->config_ref_count + 1) * sizeof(cbm_prescan_config_ref_t));
            if (refs) {
                ps->config_refs = refs;
                memcpy(refs[ps->config_ref_count].ref_path, cursor + start, (size_t)ref_len);
                refs[ps->config_ref_count].ref_path[ref_len] = '\0';
                ps->config_ref_count++;
            }
        }
        cursor += match[0].rm_eo;
    }
    cbm_regfree(&re);
}

static void extract_worker(int worker_id, void *ctx_ptr) {
    extract_ctx_t *ec = ctx_ptr;
    extract_worker_state_t *ws = &ec->workers[worker_id];

    /* Lazy gbuf creation */
    if (!ws->local_gbuf) {
        ws->local_gbuf = cbm_gbuf_new_shared_ids(ec->project_name, ec->repo_path, ec->shared_ids);
    }

    /* Pull files from shared atomic counter */
    while (1) {
        int sort_pos = atomic_fetch_add_explicit(&ec->next_file_idx, 1, memory_order_relaxed);
        if (sort_pos >= ec->file_count) {
            break;
        }
        if (atomic_load_explicit(ec->cancelled, memory_order_relaxed)) {
            break;
        }

        int file_idx = ec->sorted[sort_pos].idx;
        const cbm_file_info_t *fi = &ec->files[file_idx];

        /* Read + extract */
        int source_len = 0;
        char *source = read_file(fi->path, &source_len);
        if (!source) {
            ws->errors++;
            continue;
        }

        /* Per-file start log: shows which file each worker is processing.
         * Critical for diagnosing stuck workers on large vendored files. */
        if (sort_pos < 24) { /* first 2 rounds of workers = most interesting */
            cbm_log_info("parallel.extract.file.start", "pos", itoa_log(sort_pos), "size_kb",
                         itoa_log(source_len / 1024), "path", fi->rel_path);
        }

        uint64_t file_t0 = extract_now_ns();

        CBMFileResult *result = cbm_extract_file(source, source_len, fi->language, ec->project_name,
                                                 fi->rel_path, CBM_EXTRACT_BUDGET, NULL, NULL);

        uint64_t file_elapsed_ms = (extract_now_ns() - file_t0) / 1000000ULL;

        if (!result) {
            if (sort_pos < 24) {
                cbm_log_warn("parallel.extract.file.fail", "pos", itoa_log(sort_pos), "elapsed_ms",
                             itoa_log((int)file_elapsed_ms), "path", fi->rel_path);
            }
            free_source(source);
            ws->errors++;
            continue;
        }

        /* Per-file completion log for large/slow files */
        if (sort_pos < 24 || file_elapsed_ms > 1000) {
            cbm_log_info("parallel.extract.file.done", "pos", itoa_log(sort_pos), "elapsed_ms",
                         itoa_log((int)file_elapsed_ms), "defs", itoa_log(result->defs.count),
                         "path", fi->rel_path);
        }

        /* Create definition nodes in local gbuf */
        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!def->qualified_name || !def->name) {
                continue;
            }

            char props[2048];
            build_def_props(props, sizeof(props), def);

            cbm_gbuf_upsert_node(ws->local_gbuf, def->label ? def->label : "Function", def->name,
                                 def->qualified_name,
                                 def->file_path ? def->file_path : fi->rel_path,
                                 (int)def->start_line, (int)def->end_line, props);
            ws->nodes_created++;
        }

        /* Free TSTree immediately — arena strings survive for registry+resolve.
         * This makes slab reset safe: tree-sitter's internal nodes (in slab)
         * are released before the slab is bulk-reclaimed. */
        cbm_free_tree(result);

        /* Pre-scan source for HTTP call sites and config file references
         * while the buffer is still in memory. Eliminates 2M+ disk reads
         * in httplinks and 62K+ disk reads in configlink. */
        if (ec->prescan_cache) {
            cbm_prescan_t *ps = &ec->prescan_cache[file_idx];
            prescan_routes(source, source_len, result, fi->rel_path, ps);
            prescan_http_sites(source, source_len, result, ps);
            prescan_config_refs(source, ps);
        }

        /* Free source buffer — prescan captured everything needed. */
        free_source(source);

        /* Cache result (arena + extracted data, no tree) for Phase 3B and Phase 4 */
        ec->result_cache[file_idx] = result;

        /* Progress logging: log every 10 files (atomic read, no contention) */
        if ((sort_pos + 1) % 10 == 0 || sort_pos + 1 == ec->file_count) {
            cbm_log_info("parallel.extract.progress", "done", itoa_log(sort_pos + 1), "total",
                         itoa_log(ec->file_count));
        }

        /* Reclaim all slab + tier2 memory between files.
         *
         * After cbm_free_tree(result), all tree nodes are on free lists.
         * We then destroy the parser (frees its internal allocations too),
         * leaving ZERO live slab/tier2 pointers. At that point, we can
         * safely munmap/free every page, bounding peak memory per-file
         * instead of accumulating across all 644 files.
         *
         * get_thread_parser() in cbm_extract_file will create a fresh
         * parser for the next file — cost is microseconds vs seconds
         * for parsing. This prevents unbounded memory accumulation and works
         * identically on macOS, Linux, and Windows. */
        cbm_destroy_thread_parser();
        cbm_slab_reclaim();
        cbm_mem_collect();
    }

    /* Final cleanup (parser already destroyed in loop, just slab state) */
    cbm_slab_destroy_thread();
}

int cbm_parallel_extract(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *files, int file_count,
                         CBMFileResult **result_cache, _Atomic int64_t *shared_ids,
                         int worker_count) {
    if (file_count == 0) {
        return 0;
    }

    cbm_log_info("parallel.extract.start", "files", itoa_log(file_count), "workers",
                 itoa_log(worker_count));

    /* Log per-worker memory budget */
    if (cbm_mem_budget() > 0) {
        size_t worker_budget = cbm_mem_worker_budget(worker_count);
        cbm_log_info("parallel.mem.budget", "total_mb",
                     itoa_log((int)(cbm_mem_budget() / (1024 * 1024))), "per_worker_mb",
                     itoa_log((int)(worker_budget / (1024 * 1024))));
    }

    /* Ensure extraction library is initialized */
    cbm_init();

    /* Slab allocator for tree-sitter (thread-safe via TLS).
     * Safe: extract_worker frees TSTree via cbm_free_tree() before
     * cbm_slab_reset_thread(), so no live tree pointers cross slab boundaries. */
    cbm_slab_install();

    /* Sort files by descending size for tail-latency reduction */
    file_sort_entry_t *sorted = malloc(file_count * sizeof(file_sort_entry_t));
    for (int i = 0; i < file_count; i++) {
        sorted[i].idx = i;
        sorted[i].size = files[i].size;
    }
    qsort(sorted, file_count, sizeof(file_sort_entry_t), compare_by_size_desc);

    /* Allocate per-worker state (cache-line aligned via posix_memalign) */
    extract_worker_state_t *workers = NULL;
    if (cbm_aligned_alloc((void **)&workers, CBM_CACHE_LINE,
                          (size_t)worker_count * sizeof(extract_worker_state_t)) != 0) {
        free(sorted);
        return -1;
    }
    memset(workers, 0, (size_t)worker_count * sizeof(extract_worker_state_t));

    extract_ctx_t ec = {
        .files = files,
        .sorted = sorted,
        .file_count = file_count,
        .project_name = ctx->project_name,
        .repo_path = ctx->repo_path,
        .workers = workers,
        .max_workers = worker_count,
        .result_cache = result_cache,
        .prescan_cache = ctx->prescan_cache,
        .shared_ids = shared_ids,
        .cancelled = ctx->cancelled,
    };
    atomic_init(&ec.next_worker_id, 0);
    atomic_init(&ec.next_file_idx, 0);

    /* Dispatch workers */
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, extract_worker, &ec, opts);

    /* Merge all local gbufs into main gbuf */
    int total_nodes = 0;
    int total_errors = 0;
    for (int i = 0; i < worker_count; i++) {
        if (workers[i].local_gbuf) {
            cbm_gbuf_merge(ctx->gbuf, workers[i].local_gbuf);
            total_nodes += workers[i].nodes_created;
            total_errors += workers[i].errors;
            cbm_gbuf_free(workers[i].local_gbuf);
        }
    }

    cbm_aligned_free(workers);
    free(sorted);

    if (atomic_load(ctx->cancelled)) {
        return -1;
    }

    /* RSS-based memory stats after extraction */
    if (cbm_mem_budget() > 0) {
        size_t rss_mb = cbm_mem_rss() / (1024 * 1024);
        size_t peak_mb = cbm_mem_peak_rss() / (1024 * 1024);
        size_t budget_mb = cbm_mem_budget() / (1024 * 1024);
        size_t worker_mb = cbm_mem_worker_budget(worker_count) / (1024 * 1024);
        cbm_log_info("parallel.extract.mem", "rss_mb", itoa_log((int)rss_mb), "peak_mb",
                     itoa_log((int)peak_mb), "budget_mb", itoa_log((int)budget_mb), "per_worker_mb",
                     itoa_log((int)worker_mb));
    }

    cbm_log_info("parallel.extract.done", "nodes", itoa_log(total_nodes), "errors",
                 itoa_log(total_errors));
    return 0;
}

/* ── Phase 3B: Serial Registry Build ─────────────────────────────── */

int cbm_build_registry_from_cache(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                  int file_count, CBMFileResult **result_cache) {
    cbm_log_info("parallel.registry.start", "files", itoa_log(file_count));

    int reg_entries = 0;
    int defines_edges = 0;
    int imports_edges = 0;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return -1;
        }

        CBMFileResult *result = result_cache[i];
        if (!result) {
            continue;
        }

        const char *rel = files[i].rel_path;

        /* Register callable symbols */
        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!def->name || !def->qualified_name || !def->label) {
                continue;
            }

            if (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0 ||
                strcmp(def->label, "Class") == 0) {
                cbm_registry_add(ctx->registry, def->name, def->qualified_name, def->label);
                reg_entries++;
            }

            /* DEFINES edge: File → Definition */
            char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
            const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
            const cbm_gbuf_node_t *def_node = cbm_gbuf_find_by_qn(ctx->gbuf, def->qualified_name);
            if (file_node && def_node) {
                cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, def_node->id, "DEFINES", "{}");
                defines_edges++;
            }
            free(file_qn);

            /* DEFINES_METHOD edge: Class → Method
             * MEMBER_OF reverse edge: Method → Class (enables PageRank to
             * propagate member importance back to the parent class) */
            if (def->parent_class && strcmp(def->label, "Method") == 0) {
                const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(ctx->gbuf, def->parent_class);
                if (parent && def_node) {
                    cbm_gbuf_insert_edge(ctx->gbuf, parent->id, def_node->id, "DEFINES_METHOD",
                                         "{}");
                    cbm_gbuf_insert_edge(ctx->gbuf, def_node->id, parent->id, "MEMBER_OF", "{}");
                }
            }
        }

        /* IMPORTS edges */
        for (int j = 0; j < result->imports.count; j++) {
            CBMImport *imp = &result->imports.items[j];
            if (!imp->module_path) {
                continue;
            }

            char *target_qn = cbm_pipeline_fqn_module(ctx->project_name, imp->module_path);
            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);

            char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
            const cbm_gbuf_node_t *source_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);

            if (source_node && target) {
                char imp_props[256];
                snprintf(imp_props, sizeof(imp_props), "{\"local_name\":\"%s\"}",
                         imp->local_name ? imp->local_name : "");
                cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, target->id, "IMPORTS", imp_props);
                imports_edges++;
            }
            free(target_qn);
            free(file_qn);
        }
    }

    cbm_log_info("parallel.registry.done", "entries", itoa_log(reg_entries), "defines",
                 itoa_log(defines_edges), "imports", itoa_log(imports_edges));
    return 0;
}

/* ── Phase 4: Parallel Resolution ────────────────────────────────── */

typedef struct __attribute__((aligned(CBM_CACHE_LINE))) {
    cbm_gbuf_t *local_edge_buf;
    int calls_resolved;
    int usages_resolved;
    int semantic_resolved;
    int errors;
    char _pad[CBM_CACHE_LINE - sizeof(cbm_gbuf_t *) - (4 * sizeof(int))];
} resolve_worker_state_t;

typedef struct {
    const cbm_file_info_t *files;
    int file_count;
    const char *project_name;
    const char *repo_path;

    resolve_worker_state_t *workers;
    int max_workers;

    CBMFileResult **result_cache;
    const cbm_gbuf_t *main_gbuf;    /* READ-ONLY during Phase 4 */
    const cbm_registry_t *registry; /* READ-ONLY during Phase 4 */
    _Atomic int64_t *shared_ids;
    _Atomic int *cancelled;
    _Atomic int next_file_idx;
} resolve_ctx_t;

static void resolve_worker(int worker_id, void *ctx_ptr) {
    resolve_ctx_t *rc = ctx_ptr;
    resolve_worker_state_t *ws = &rc->workers[worker_id];

    if (!ws->local_edge_buf) {
        ws->local_edge_buf =
            cbm_gbuf_new_shared_ids(rc->project_name, rc->repo_path, rc->shared_ids);
    }

    while (1) {
        int file_idx = atomic_fetch_add_explicit(&rc->next_file_idx, 1, memory_order_relaxed);
        if (file_idx >= rc->file_count) {
            break;
        }
        if (atomic_load_explicit(rc->cancelled, memory_order_relaxed)) {
            break;
        }

        CBMFileResult *result = rc->result_cache[file_idx];
        if (!result) {
            continue;
        }

        /* Skip files with nothing to resolve */
        if (result->calls.count == 0 && result->usages.count == 0 && result->throws.count == 0 &&
            result->rw.count == 0 && result->defs.count == 0 && result->impl_traits.count == 0) {
            continue;
        }

        const char *rel = rc->files[file_idx].rel_path;

        /* Build import map (read-only access to main_gbuf) */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(rc->main_gbuf, rc->project_name, rel, &imp_keys, &imp_vals, &imp_count);

        char *module_qn = cbm_pipeline_fqn_module(rc->project_name, rel);

        /* ── CALLS resolution ──────────────────────────────────── */
        for (int c = 0; c < result->calls.count; c++) {
            CBMCall *call = &result->calls.items[c];
            if (!call->callee_name) {
                continue;
            }

            const cbm_gbuf_node_t *source_node = NULL;
            if (call->enclosing_func_qn) {
                source_node = cbm_gbuf_find_by_qn(rc->main_gbuf, call->enclosing_func_qn);
            }
            if (!source_node) {
                char *file_qn = cbm_pipeline_fqn_compute(rc->project_name, rel, "__file__");
                source_node = cbm_gbuf_find_by_qn(rc->main_gbuf, file_qn);
                free(file_qn);
            }
            if (!source_node) {
                continue;
            }

            cbm_resolution_t res = cbm_registry_resolve(rc->registry, call->callee_name, module_qn,
                                                        imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *target_node =
                cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
            if (!target_node || source_node->id == target_node->id) {
                continue;
            }

            char props[256];
            snprintf(props, sizeof(props),
                     "{\"confidence\":%.2f,\"strategy\":\"%s\",\"candidates\":%d}", res.confidence,
                     res.strategy ? res.strategy : "unknown", res.candidate_count);
            cbm_gbuf_insert_edge(ws->local_edge_buf, source_node->id, target_node->id, "CALLS",
                                 props);
            ws->calls_resolved++;
        }

        /* ── USAGE resolution ──────────────────────────────────── */
        for (int u = 0; u < result->usages.count; u++) {
            CBMUsage *usage = &result->usages.items[u];
            if (!usage->ref_name) {
                continue;
            }

            const cbm_gbuf_node_t *src = NULL;
            if (usage->enclosing_func_qn) {
                src = cbm_gbuf_find_by_qn(rc->main_gbuf, usage->enclosing_func_qn);
            }
            if (!src) {
                char *file_qn = cbm_pipeline_fqn_compute(rc->project_name, rel, "__file__");
                src = cbm_gbuf_find_by_qn(rc->main_gbuf, file_qn);
                free(file_qn);
            }
            if (!src) {
                continue;
            }

            cbm_resolution_t res = cbm_registry_resolve(rc->registry, usage->ref_name, module_qn,
                                                        imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
            if (!tgt || src->id == tgt->id) {
                continue;
            }

            cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, "USAGE", "{}");
            ws->usages_resolved++;
        }

        /* ── THROWS / RAISES ───────────────────────────────────── */
        for (int t = 0; t < result->throws.count; t++) {
            CBMThrow *thr = &result->throws.items[t];
            if (!thr->exception_name || !thr->enclosing_func_qn) {
                continue;
            }

            const cbm_gbuf_node_t *src = cbm_gbuf_find_by_qn(rc->main_gbuf, thr->enclosing_func_qn);
            if (!src) {
                continue;
            }

            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            const char *edge_type = is_checked_exception(thr->exception_name) ? "THROWS" : "RAISES";
            cbm_resolution_t res = cbm_registry_resolve(rc->registry, thr->exception_name,
                                                        module_qn, imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
            if (!tgt || src->id == tgt->id) {
                continue;
            }

            cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, edge_type, "{}");
        }

        /* ── READS / WRITES ────────────────────────────────────── */
        for (int r = 0; r < result->rw.count; r++) {
            CBMReadWrite *rw = &result->rw.items[r];
            if (!rw->var_name) {
                continue;
            }

            const cbm_gbuf_node_t *src = NULL;
            if (rw->enclosing_func_qn) {
                src = cbm_gbuf_find_by_qn(rc->main_gbuf, rw->enclosing_func_qn);
            }
            if (!src) {
                char *file_qn = cbm_pipeline_fqn_compute(rc->project_name, rel, "__file__");
                src = cbm_gbuf_find_by_qn(rc->main_gbuf, file_qn);
                free(file_qn);
            }
            if (!src) {
                continue;
            }

            cbm_resolution_t res = cbm_registry_resolve(rc->registry, rw->var_name, module_qn,
                                                        imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
            if (!tgt || src->id == tgt->id) {
                continue;
            }

            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            const char *etype = rw->is_write ? "WRITES" : "READS";
            cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, etype, "{}");
        }

        /* ── INHERITS + DECORATES ──────────────────────────────── */
        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!def->qualified_name) {
                continue;
            }

            const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(rc->main_gbuf, def->qualified_name);
            if (!node) {
                continue;
            }

            /* INHERITS */
            if (def->base_classes) {
                for (int b = 0; def->base_classes[b]; b++) {
                    const char *base_qn =
                        resolve_as_class(rc->registry, def->base_classes[b], module_qn, imp_keys,
                                         imp_vals, imp_count);
                    if (!base_qn) {
                        continue;
                    }
                    const cbm_gbuf_node_t *base_node = cbm_gbuf_find_by_qn(rc->main_gbuf, base_qn);
                    if (!base_node || node->id == base_node->id) {
                        continue;
                    }
                    cbm_gbuf_insert_edge(ws->local_edge_buf, node->id, base_node->id, "INHERITS",
                                         "{}");
                    ws->semantic_resolved++;
                }
            }

            /* DECORATES */
            if (def->decorators) {
                for (int dc = 0; def->decorators[dc]; dc++) {
                    char func_name[256];
                    extract_decorator_func(def->decorators[dc], func_name, sizeof(func_name));
                    if (func_name[0] == '\0') {
                        continue;
                    }

                    cbm_resolution_t res = cbm_registry_resolve(rc->registry, func_name, module_qn,
                                                                imp_keys, imp_vals, imp_count);
                    if (!res.qualified_name || res.qualified_name[0] == '\0') {
                        continue;
                    }

                    const cbm_gbuf_node_t *dec_node =
                        cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
                    if (!dec_node || node->id == dec_node->id) {
                        continue;
                    }

                    char dprops[256];
                    snprintf(dprops, sizeof(dprops), "{\"decorator\":\"%s\"}", def->decorators[dc]);
                    cbm_gbuf_insert_edge(ws->local_edge_buf, node->id, dec_node->id, "DECORATES",
                                         dprops);
                    ws->semantic_resolved++;
                }
            }
        }

        /* ── IMPLEMENTS (Rust impl) ────────────────────────────── */
        for (int t = 0; t < result->impl_traits.count; t++) {
            CBMImplTrait *it = &result->impl_traits.items[t];
            if (!it->trait_name || !it->struct_name) {
                continue;
            }

            const char *trait_qn = resolve_as_class(rc->registry, it->trait_name, module_qn,
                                                    imp_keys, imp_vals, imp_count);
            if (!trait_qn) {
                continue;
            }
            const char *struct_qn = resolve_as_class(rc->registry, it->struct_name, module_qn,
                                                     imp_keys, imp_vals, imp_count);
            if (!struct_qn) {
                continue;
            }

            const cbm_gbuf_node_t *trait_node = cbm_gbuf_find_by_qn(rc->main_gbuf, trait_qn);
            const cbm_gbuf_node_t *struct_node = cbm_gbuf_find_by_qn(rc->main_gbuf, struct_qn);
            if (!trait_node || !struct_node || trait_node->id == struct_node->id) {
                continue;
            }

            cbm_gbuf_insert_edge(ws->local_edge_buf, struct_node->id, trait_node->id, "IMPLEMENTS",
                                 "{}");
            ws->semantic_resolved++;
        }

        free(module_qn);
        free_import_map(imp_keys, imp_vals, imp_count);
    }
}

int cbm_parallel_resolve(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                         CBMFileResult **result_cache, _Atomic int64_t *shared_ids,
                         int worker_count) {
    if (file_count == 0) {
        return 0;
    }

    cbm_log_info("parallel.resolve.start", "files", itoa_log(file_count), "workers",
                 itoa_log(worker_count));

    resolve_worker_state_t *workers = NULL;
    if (cbm_aligned_alloc((void **)&workers, CBM_CACHE_LINE,
                          (size_t)worker_count * sizeof(resolve_worker_state_t)) != 0) {
        return -1;
    }
    memset(workers, 0, (size_t)worker_count * sizeof(resolve_worker_state_t));

    resolve_ctx_t rc = {
        .files = files,
        .file_count = file_count,
        .project_name = ctx->project_name,
        .repo_path = ctx->repo_path,
        .workers = workers,
        .max_workers = worker_count,
        .result_cache = result_cache,
        .main_gbuf = ctx->gbuf,
        .registry = ctx->registry,
        .shared_ids = shared_ids,
        .cancelled = ctx->cancelled,
    };
    atomic_init(&rc.next_file_idx, 0);

    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, resolve_worker, &rc, opts);

    /* Merge all local edge bufs into main gbuf */
    int total_calls = 0;
    int total_usages = 0;
    int total_semantic = 0;
    for (int i = 0; i < worker_count; i++) {
        if (workers[i].local_edge_buf) {
            cbm_gbuf_merge(ctx->gbuf, workers[i].local_edge_buf);
            total_calls += workers[i].calls_resolved;
            total_usages += workers[i].usages_resolved;
            total_semantic += workers[i].semantic_resolved;
            cbm_gbuf_free(workers[i].local_edge_buf);
        }
    }

    cbm_aligned_free(workers);

    /* Go-style implicit interface satisfaction (needs full graph, serial) */
    int go_impl = cbm_pipeline_implements_go(ctx);

    if (atomic_load(ctx->cancelled)) {
        return -1;
    }

    cbm_log_info("parallel.resolve.done", "calls", itoa_log(total_calls), "usages",
                 itoa_log(total_usages), "semantic", itoa_log(total_semantic + go_impl));
    return 0;
}
