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
#include "foundation/constants.h"

enum {
    PP_RING = 4,
    PP_RING_MASK = 3,
    PP_JSON_MARGIN = 10,
    PP_ESC_MARGIN = 3,
    PP_ESC_SPACE = 2,
    PP_ARGS_MARGIN = 20,
    PP_LOG_THRESH = 24,
    PP_LOG_INTERVAL = 10,
    PP_TIMER_THRESH = 1000,
};
#define PP_NSEC_PER_SEC 1000000000ULL
#define PP_USEC_PER_MS 1000000ULL
#define PP_HALF_CONF 0.5
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/worker_pool.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "graph_buffer/graph_buffer.h"
#include "service_patterns.h"
#include "foundation/platform.h"
#include "foundation/log.h"
#include "foundation/slab_alloc.h"
#include "foundation/mem.h"
#include "foundation/str_util.h"
#include "foundation/profile.h"
#include "foundation/compat_regex.h"
#include "cbm.h"
#include "simhash/minhash.h"
#include "semantic/ast_profile.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t extract_now_ns(void) {
    struct timespec ts;
    cbm_clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * PP_NSEC_PER_SEC) + (uint64_t)ts.tv_nsec;
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
    if (size <= 0 || size > (long)CBM_PERCENT * CBM_SZ_1K * CBM_SZ_1K) {
        (void)fclose(f);
        return NULL;
    }
    char *buf = (char *)malloc((size_t)size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, SKIP_ONE, (size_t)size, f);
    (void)fclose(f);
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* Free source buffer. */
static void free_source(char *buf) {
    free(buf);
}

static const char *itoa_log(int val) {
    static CBM_TLS char bufs[PP_RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & PP_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Append a JSON-escaped string value to buf at position *pos. */
/* Escape one character for JSON. Returns bytes written (1 or 2). */
static int json_escape_char(char *buf, size_t avail, char ch) {
    char esc = 0;
    switch (ch) {
    case '"':
        esc = '"';
        break;
    case '\\':
        esc = '\\';
        break;
    case '\n':
        esc = 'n';
        break;
    case '\r':
        esc = 'r';
        break;
    case '\t':
        esc = 't';
        break;
    default:
        if (avail >= SKIP_ONE) {
            buf[0] = ch;
        }
        return SKIP_ONE;
    }
    if (avail >= PP_ESC_SPACE) {
        buf[0] = '\\';
        buf[SKIP_ONE] = esc;
    }
    return PP_ESC_SPACE;
}

static void append_json_string(char *buf, size_t bufsize, size_t *pos, const char *key,
                               const char *val) {
    if (!val || val[0] == '\0') {
        return;
    }
    if (*pos >= bufsize - PP_JSON_MARGIN) {
        return;
    }
    size_t p = *pos;
    int w = snprintf(buf + p, bufsize - p, ",\"%s\":\"", key);
    if (w <= 0 || (size_t)w >= bufsize - p) {
        return;
    }
    p += (size_t)w;
    for (const char *s = val; *s && p < bufsize - PP_ESC_MARGIN; s++) {
        int n = json_escape_char(buf + p, bufsize - p - PP_ESC_SPACE, *s);
        p += (size_t)n;
    }
    if (p < bufsize - SKIP_ONE) {
        buf[p++] = '"';
    }
    buf[p] = '\0';
    *pos = p;
}

/* Append a JSON array of strings: ,"key":["a","b","c"] */
static void append_json_str_array(char *buf, size_t bufsize, size_t *pos, const char *key,
                                  const char **arr) {
    if (!arr || !arr[0] || *pos >= bufsize - PP_JSON_MARGIN) {
        return;
    }
    size_t p = *pos;
    int n = snprintf(buf + p, bufsize - p, ",\"%s\":[", key);
    if (n <= 0 || p + (size_t)n >= bufsize - PP_ESC_SPACE) {
        return;
    }
    p += (size_t)n;
    for (int i = 0; arr[i]; i++) {
        if (i > 0 && p < bufsize - SKIP_ONE) {
            buf[p++] = ',';
        }
        if (p < bufsize - SKIP_ONE) {
            buf[p++] = '"';
        }
        for (const char *s = arr[i]; *s && p < bufsize - PP_ESC_SPACE; s++) {
            if (*s == '"' || *s == '\\') {
                buf[p++] = '\\';
                if (p >= bufsize - PP_ESC_SPACE) {
                    break;
                }
            }
            buf[p++] = *s;
        }
        if (p < bufsize - SKIP_ONE) {
            buf[p++] = '"';
        }
    }
    if (p < bufsize - SKIP_ONE) {
        buf[p++] = ']';
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
    append_json_string(buf, bufsize, &pos, "return_type", def->return_type);
    append_json_string(buf, bufsize, &pos, "parent_class", def->parent_class);
    append_json_str_array(buf, bufsize, &pos, "decorators", def->decorators);
    append_json_str_array(buf, bufsize, &pos, "base_classes", def->base_classes);
    append_json_str_array(buf, bufsize, &pos, "param_names", def->param_names);
    append_json_str_array(buf, bufsize, &pos, "param_types", def->param_types);
    append_json_string(buf, bufsize, &pos, "route_path", def->route_path);
    append_json_string(buf, bufsize, &pos, "route_method", def->route_method);

    /* MinHash fingerprint — append if present and buffer has room.
     * Hex-encoded K=64 uint32 = 512 chars + key/quotes ≈ 520 chars. */
    if (def->fingerprint && def->fingerprint_k > 0 &&
        pos + CBM_MINHASH_HEX_LEN + CBM_MINHASH_JSON_OVERHEAD < bufsize) {
        char fp_hex[CBM_MINHASH_HEX_BUF];
        cbm_minhash_to_hex((const cbm_minhash_t *)def->fingerprint, fp_hex, sizeof(fp_hex));
        append_json_string(buf, bufsize, &pos, "fp", fp_hex);
    }

    /* AST structural profile — append if present and buffer has room. */
    if (def->structural_profile && pos + CBM_AST_PROFILE_BUF < bufsize) {
        append_json_string(buf, bufsize, &pos, "sp", def->structural_profile);
    }

    /* Body tokens — raw identifiers from function body AST for semantic search. */
    if (def->body_tokens && pos + CBM_SZ_512 < bufsize) {
        append_json_string(buf, bufsize, &pos, "bt", def->body_tokens);
    }

    if (pos < bufsize - SKIP_ONE) {
        buf[pos] = '}';
        buf[pos + SKIP_ONE] = '\0';
    }
}

/* Build import map from graph buffer IMPORTS edges (read-only access to gbuf). */
static int build_import_map(const cbm_gbuf_t *gbuf, const char *project_name, const char *rel_path,
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

    const char **keys = calloc(edge_count, sizeof(const char *));
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
    int64_t size;
} file_sort_entry_t;

static int compare_by_size_desc(const void *a, const void *b) {
    const file_sort_entry_t *fa = a;
    const file_sort_entry_t *fb = b;
    if (fb->size > fa->size) {
        return SKIP_ONE;
    }
    if (fb->size < fa->size) {
        return CBM_NOT_FOUND;
    }
    return 0;
}

/* ── Phase 3A: Parallel Extract ──────────────────────────────────── */

#define CBM_CACHE_LINE CBM_SZ_128

typedef struct __attribute__((aligned(CBM_CACHE_LINE))) {
    cbm_gbuf_t *local_gbuf;
    int nodes_created;
    int errors;
    char _pad[CBM_CACHE_LINE - sizeof(cbm_gbuf_t *) - (PP_ESC_SPACE * sizeof(int))];
} extract_worker_state_t;

typedef struct {
    const cbm_file_info_t *files;
    file_sort_entry_t *sorted;
    int file_count;
    const char *project_name;
    const char *repo_path;

    extract_worker_state_t *workers;
    int max_workers;
    _Atomic int next_worker_id;

    CBMFileResult **result_cache;
    _Atomic int64_t *shared_ids;
    _Atomic int *cancelled;
    _Atomic int next_file_idx;
} extract_ctx_t;

/* Insert one definition node (and its route if present) into the local gbuf. */
static void insert_def_into_gbuf(extract_worker_state_t *ws, const cbm_file_info_t *fi,
                                 CBMDefinition *def) {
    char props[CBM_SZ_2K];
    build_def_props(props, sizeof(props), def);
    int64_t func_id =
        cbm_gbuf_upsert_node(ws->local_gbuf, def->label ? def->label : "Function", def->name,
                             def->qualified_name, def->file_path ? def->file_path : fi->rel_path,
                             (int)def->start_line, (int)def->end_line, props);
    ws->nodes_created++;
    if (def->route_path && def->route_path[0] != '\0') {
        const char *rm = def->route_method ? def->route_method : "ANY";
        char route_qn[CBM_ROUTE_QN_SIZE];
        snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", rm, def->route_path);
        char rprops[CBM_SZ_256];
        snprintf(rprops, sizeof(rprops), "{\"method\":\"%s\",\"source\":\"decorator\"}", rm);
        int64_t route_id =
            cbm_gbuf_upsert_node(ws->local_gbuf, "Route", def->route_path, route_qn,
                                 def->file_path ? def->file_path : fi->rel_path, 0, 0, rprops);
        char hprops[CBM_SZ_512];
        snprintf(hprops, sizeof(hprops), "{\"handler\":\"%s\"}", def->qualified_name);
        cbm_gbuf_insert_edge(ws->local_gbuf, func_id, route_id, "HANDLES", hprops);
    }
}

static void log_extract_fail(int pos, uint64_t ms, const char *path) {
    if (pos < PP_LOG_THRESH) {
        cbm_log_warn("parallel.extract.file.fail", "pos", itoa_log(pos), "elapsed_ms",
                     itoa_log((int)ms), "path", path);
    }
}

static void log_extract_done(int pos, uint64_t ms, int defs, const char *path) {
    if (pos < PP_LOG_THRESH || ms > PP_TIMER_THRESH) {
        cbm_log_info("parallel.extract.file.done", "pos", itoa_log(pos), "elapsed_ms",
                     itoa_log((int)ms), "defs", itoa_log(defs), "path", path);
    }
}

static void extract_worker(int worker_id, void *ctx_ptr) {
    extract_ctx_t *ec = ctx_ptr;
    extract_worker_state_t *ws = &ec->workers[worker_id];

    /* Lazy gbuf creation */
    if (!ws->local_gbuf) {
        ws->local_gbuf = cbm_gbuf_new_shared_ids(ec->project_name, ec->repo_path, ec->shared_ids);
    }

    /* Pull files from shared atomic counter */
    while (SKIP_ONE) {
        int sort_pos =
            atomic_fetch_add_explicit(&ec->next_file_idx, SKIP_ONE, memory_order_relaxed);
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
        if (sort_pos < PP_LOG_THRESH) { /* first 2 rounds of workers = most interesting */
            cbm_log_info("parallel.extract.file.start", "pos", itoa_log(sort_pos), "size_kb",
                         itoa_log(source_len / CBM_SZ_1K), "path", fi->rel_path);
        }

        uint64_t file_t0 = extract_now_ns();

        CBMFileResult *result = cbm_extract_file(source, source_len, fi->language, ec->project_name,
                                                 fi->rel_path, CBM_EXTRACT_BUDGET, NULL, NULL);

        uint64_t file_elapsed_ms = (extract_now_ns() - file_t0) / PP_USEC_PER_MS;

        if (!result) {
            log_extract_fail(sort_pos, file_elapsed_ms, fi->rel_path);
            free_source(source);
            ws->errors++;
            continue;
        }
        log_extract_done(sort_pos, file_elapsed_ms, result->defs.count, fi->rel_path);

        /* Create definition nodes in local gbuf */
        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (def->qualified_name && def->name) {
                insert_def_into_gbuf(ws, fi, def);
            }
        }

        /* Free TSTree immediately — arena strings survive for registry+resolve.
         * This makes slab reset safe: tree-sitter's internal nodes (in slab)
         * are released before the slab is bulk-reclaimed. */
        cbm_free_tree(result);

        /* Free source buffer — extraction captured everything needed. */
        free_source(source);

        /* Cache result (arena + extracted data, no tree) for Phase 3B and Phase 4 */
        ec->result_cache[file_idx] = result;

        /* Progress logging: log every 10 files (atomic read, no contention) */
        if ((sort_pos + SKIP_ONE) % PP_LOG_INTERVAL == 0 || sort_pos + SKIP_ONE == ec->file_count) {
            cbm_log_info("parallel.extract.progress", "done", itoa_log(sort_pos + SKIP_ONE),
                         "total", itoa_log(ec->file_count));
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

int cbm_parallel_extract(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
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
                     itoa_log((int)(cbm_mem_budget() / ((size_t)CBM_SZ_1K * CBM_SZ_1K))),
                     "per_worker_mb",
                     itoa_log((int)(worker_budget / ((size_t)CBM_SZ_1K * CBM_SZ_1K))));
    }

    /* Sub-phase: Ensure extraction library is initialized */
    CBM_PROF_START(t_init);
    cbm_init();

    /* Slab allocator for tree-sitter (thread-safe via TLS). */
    cbm_slab_install();
    CBM_PROF_END("parallel_extract", "1_init_libs", t_init);

    /* Sub-phase: Sort files by descending size for tail-latency reduction */
    CBM_PROF_START(t_sort);
    file_sort_entry_t *sorted = malloc(file_count * sizeof(file_sort_entry_t));
    for (int i = 0; i < file_count; i++) {
        sorted[i].idx = i;
        sorted[i].size = files[i].size;
    }
    qsort(sorted, file_count, sizeof(file_sort_entry_t), compare_by_size_desc);
    CBM_PROF_END_N("parallel_extract", "2_sort_files", t_sort, file_count);

    /* Allocate per-worker state (cache-line aligned via posix_memalign) */
    extract_worker_state_t *workers = NULL;
    if (cbm_aligned_alloc((void **)&workers, CBM_CACHE_LINE,
                          (size_t)worker_count * sizeof(extract_worker_state_t)) != 0) {
        free(sorted);
        return CBM_NOT_FOUND;
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
        .shared_ids = shared_ids,
        .cancelled = ctx->cancelled,
    };
    atomic_init(&ec.next_worker_id, 0);
    atomic_init(&ec.next_file_idx, 0);

    /* Sub-phase: Dispatch workers (parse + extract per file, PARALLEL) */
    CBM_PROF_START(t_dispatch);
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, extract_worker, &ec, opts);
    CBM_PROF_END_N("parallel_extract", "3_dispatch_workers_parallel", t_dispatch, file_count);

    /* Sub-phase: Merge all local gbufs into main gbuf (SEQUENTIAL, gbuf not thread-safe) */
    CBM_PROF_START(t_merge);
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
    CBM_PROF_END_N("parallel_extract", "4_merge_gbufs_seq", t_merge, total_nodes);

    cbm_aligned_free(workers);
    free(sorted);

    if (atomic_load(ctx->cancelled)) {
        return CBM_NOT_FOUND;
    }

    /* RSS-based memory stats after extraction */
    if (cbm_mem_budget() > 0) {
        size_t rss_mb = cbm_mem_rss() / ((size_t)CBM_SZ_1K * CBM_SZ_1K);
        size_t peak_mb = cbm_mem_peak_rss() / ((size_t)CBM_SZ_1K * CBM_SZ_1K);
        size_t budget_mb = cbm_mem_budget() / ((size_t)CBM_SZ_1K * CBM_SZ_1K);
        size_t worker_mb = cbm_mem_worker_budget(worker_count) / ((size_t)CBM_SZ_1K * CBM_SZ_1K);
        cbm_log_info("parallel.extract.mem", "rss_mb", itoa_log((int)rss_mb), "peak_mb",
                     itoa_log((int)peak_mb), "budget_mb", itoa_log((int)budget_mb), "per_worker_mb",
                     itoa_log((int)worker_mb));
    }

    cbm_log_info("parallel.extract.done", "nodes", itoa_log(total_nodes), "errors",
                 itoa_log(total_errors));
    return 0;
}

/* ── Phase 3B: Serial Registry Build ─────────────────────────────── */

/* Register one definition and create DEFINES + DEFINES_METHOD edges. Returns edge count. */
static int register_and_link_def(cbm_pipeline_ctx_t *ctx, const CBMDefinition *def, const char *rel,
                                 int *reg_entries) {
    int edges = 0;
    if (!def->name || !def->qualified_name || !def->label) {
        return 0;
    }
    /* Register callable symbols + Interface — see pass_definitions.c for rationale. */
    if (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0 ||
        strcmp(def->label, "Class") == 0 || strcmp(def->label, "Interface") == 0) {
        cbm_registry_add(ctx->registry, def->name, def->qualified_name, def->label);
        (*reg_entries)++;
    }
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
    const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
    const cbm_gbuf_node_t *def_node = cbm_gbuf_find_by_qn(ctx->gbuf, def->qualified_name);
    if (file_node && def_node) {
        cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, def_node->id, "DEFINES", "{}");
        edges++;
    }
    free(file_qn);
    if (def->parent_class && strcmp(def->label, "Method") == 0) {
        const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(ctx->gbuf, def->parent_class);
        if (parent && def_node) {
            cbm_gbuf_insert_edge(ctx->gbuf, parent->id, def_node->id, "DEFINES_METHOD", "{}");
        }
    }
    return edges;
}

/* Create IMPORTS edges for one file's imports (parallel path). */
static int create_imports_edges(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                const char *rel) {
    int count = 0;
    for (int j = 0; j < result->imports.count; j++) {
        CBMImport *imp = &result->imports.items[j];
        if (!imp->module_path) {
            continue;
        }
        char *target_qn = NULL;
        char *resolved = cbm_pipeline_resolve_relative_import(rel, imp->module_path);
        if (resolved) {
            target_qn = cbm_pipeline_fqn_module(ctx->project_name, resolved);
            free(resolved);
        } else {
            target_qn = cbm_pipeline_fqn_module(ctx->project_name, imp->module_path);
        }
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);
        char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
        const cbm_gbuf_node_t *source_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
        if (source_node && target) {
            char esc_ln[CBM_SZ_128];
            cbm_json_escape(esc_ln, sizeof(esc_ln), imp->local_name ? imp->local_name : "");
            char imp_props[CBM_SZ_256];
            snprintf(imp_props, sizeof(imp_props), "{\"local_name\":\"%s\"}", esc_ln);
            cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, target->id, "IMPORTS", imp_props);
            count++;
        }
        free(target_qn);
        free(file_qn);
    }
    return count;
}

/* Find channel source node (enclosing function or file). */
static const cbm_gbuf_node_t *find_channel_src(cbm_pipeline_ctx_t *ctx, const CBMChannel *ch,
                                               const char *rel) {
    const cbm_gbuf_node_t *node = NULL;
    if (ch->enclosing_func_qn && ch->enclosing_func_qn[0]) {
        node = cbm_gbuf_find_by_qn(ctx->gbuf, ch->enclosing_func_qn);
    }
    if (!node) {
        char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
        node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
        free(file_qn);
    }
    return node;
}

/* Create Channel nodes + EMITS/LISTENS_ON edges for one file. */
static void create_channel_edges(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                 const char *rel) {
    for (int j = 0; j < result->channels.count; j++) {
        CBMChannel *ch = &result->channels.items[j];
        if (!ch->channel_name || !ch->channel_name[0]) {
            continue;
        }
        char channel_qn[CBM_SZ_512];
        snprintf(channel_qn, sizeof(channel_qn), "__channel__%s__%s",
                 ch->transport ? ch->transport : "unknown", ch->channel_name);
        char esc_cn[CBM_SZ_256];
        cbm_json_escape(esc_cn, sizeof(esc_cn), ch->channel_name);
        char channel_props[CBM_SZ_512];
        snprintf(channel_props, sizeof(channel_props), "{\"transport\":\"%s\",\"name\":\"%s\"}",
                 ch->transport ? ch->transport : "unknown", esc_cn);
        int64_t channel_id = cbm_gbuf_upsert_node(ctx->gbuf, "Channel", ch->channel_name,
                                                  channel_qn, "", 0, 0, channel_props);
        const cbm_gbuf_node_t *src_node = find_channel_src(ctx, ch, rel);
        if (src_node && channel_id > 0) {
            const char *edge_type = ch->direction == CBM_CHANNEL_EMIT ? "EMITS" : "LISTENS_ON";
            char edge_props[CBM_SZ_128];
            snprintf(edge_props, sizeof(edge_props), "{\"transport\":\"%s\"}",
                     ch->transport ? ch->transport : "unknown");
            cbm_gbuf_insert_edge(ctx->gbuf, src_node->id, channel_id, edge_type, edge_props);
        }
    }
}

int cbm_build_registry_from_cache(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                  int file_count, CBMFileResult **result_cache) {
    cbm_log_info("parallel.registry.start", "files", itoa_log(file_count));

    int reg_entries = 0;
    int defines_edges = 0;
    int imports_edges = 0;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return CBM_NOT_FOUND;
        }

        CBMFileResult *result = result_cache[i];
        if (!result) {
            continue;
        }

        const char *rel = files[i].rel_path;

        /* Register callable symbols + DEFINES/DEFINES_METHOD edges */
        for (int d = 0; d < result->defs.count; d++) {
            defines_edges += register_and_link_def(ctx, &result->defs.items[d], rel, &reg_entries);
        }

        imports_edges += create_imports_edges(ctx, result, rel);
        create_channel_edges(ctx, result, rel);
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
    char _pad[CBM_CACHE_LINE - sizeof(cbm_gbuf_t *) - (PP_RING * sizeof(int))];
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

/* Minimum buffer space needed per arg JSON object */
#define CBM_ARG_JSON_GUARD CBM_SZ_32

/* Append arg data as JSON to edge properties: ,"args":[{"i":0,"e":"x","v":"val"},...]
 * Returns new position in buffer. */
/* Sanitize expression string for JSON (in-place). */
static void sanitize_expr(char *expr_buf, const char *expr) {
    if (expr) {
        snprintf(expr_buf, 128, "%.*s", 120, expr);
        for (char *p = expr_buf; *p; p++) {
            if (*p == '"') {
                *p = '\'';
            }
            if (*p == '\n' || *p == '\r') {
                *p = ' ';
            }
        }
    } else {
        expr_buf[0] = '\0';
    }
}

/* Format one call arg as JSON. Returns snprintf result. */
static int format_call_arg(char *buf, size_t bufsize, const CBMCallArg *a, const char *expr) {
    char esc_k[CBM_SZ_128];
    char esc_e[CBM_SZ_128];
    char esc_v[CBM_SZ_128];
    cbm_json_escape(esc_e, sizeof(esc_e), expr);
    if (a->keyword && a->value) {
        cbm_json_escape(esc_k, sizeof(esc_k), a->keyword);
        cbm_json_escape(esc_v, sizeof(esc_v), a->value);
        return snprintf(buf, bufsize, "{\"i\":%d,\"k\":\"%s\",\"e\":\"%s\",\"v\":\"%s\"}", a->index,
                        esc_k, esc_e, esc_v);
    }
    if (a->keyword) {
        cbm_json_escape(esc_k, sizeof(esc_k), a->keyword);
        return snprintf(buf, bufsize, "{\"i\":%d,\"k\":\"%s\",\"e\":\"%s\"}", a->index, esc_k,
                        esc_e);
    }
    if (a->value) {
        cbm_json_escape(esc_v, sizeof(esc_v), a->value);
        return snprintf(buf, bufsize, "{\"i\":%d,\"e\":\"%s\",\"v\":\"%s\"}", a->index, esc_e,
                        esc_v);
    }
    return snprintf(buf, bufsize, "{\"i\":%d,\"e\":\"%s\"}", a->index, esc_e);
}

static size_t append_args_json(char *buf, size_t bufsize, size_t pos, const CBMCall *call) {
    if (call->arg_count == 0 || pos >= bufsize - PP_ARGS_MARGIN) {
        return pos;
    }
    int n = snprintf(buf + pos, bufsize - pos, ",\"args\":[");
    if (n <= 0) {
        return pos;
    }
    pos += (size_t)n;
    for (int i = 0; i < call->arg_count && pos < bufsize - CBM_ARG_JSON_GUARD; i++) {
        const CBMCallArg *a = &call->args[i];
        if (i > 0 && pos < bufsize - SKIP_ONE) {
            buf[pos++] = ',';
        }
        char expr_buf[CBM_SZ_128];
        sanitize_expr(expr_buf, a->expr);
        n = format_call_arg(buf + pos, bufsize - pos, a, expr_buf);
        if (n > 0) {
            pos += (size_t)n;
        }
    }
    if (pos < bufsize - SKIP_ONE) {
        buf[pos++] = ']';
    }
    buf[pos] = '\0';
    return pos;
}

/* Scan call args for a URL-like route path and handler reference. */
static bool is_path_keyword(const char *keyword) {
    static const char *path_keywords[] = {"prefix",     "path",     "route", "pattern",
                                          "url",        "endpoint", "rule",  "mount_path",
                                          "route_path", "url_path", NULL};
    for (const char **kw = path_keywords; *kw; kw++) {
        if (strcmp(keyword, *kw) == 0) {
            return true;
        }
    }
    return false;
}

static const char *find_route_path_in_args(const CBMCall *call, const char **out_handler) {
    *out_handler = NULL;
    /* 1. First string arg starting with / */
    if (call->first_string_arg && call->first_string_arg[0] == '/') {
        *out_handler = call->second_arg_name;
        return call->first_string_arg;
    }
    /* 2. Keyword args (prefix=, path=, route=, etc.) */
    const char *found = NULL;
    for (int ai = 0; ai < call->arg_count && !found; ai++) {
        const CBMCallArg *ca = &call->args[ai];
        const char *val = ca->value ? ca->value : ca->expr;
        if (!val || val[0] != '/') {
            continue;
        }
        if ((ca->keyword && is_path_keyword(ca->keyword)) || (!ca->keyword && ca->index == 0)) {
            found = val;
        }
    }
    if (!found) {
        return NULL;
    }
    /* 3. Handler: first identifier arg that's not a path/keyword */
    for (int ai = 0; ai < call->arg_count; ai++) {
        const CBMCallArg *ca = &call->args[ai];
        if (!ca->expr || ca->expr[0] == '/' || ca->expr[0] == '"' || ca->expr[0] == '\'') {
            continue;
        }
        if (ca->keyword && (strcmp(ca->keyword, "prefix") == 0 ||
                            strcmp(ca->keyword, "name") == 0 || strcmp(ca->keyword, "tags") == 0)) {
            continue;
        }
        *out_handler = ca->expr;
        break;
    }
    return found;
}

/* Build props JSON, append args, close brace, emit edge. */
static void finalize_and_emit(cbm_gbuf_t *gbuf, int64_t src_id, int64_t tgt_id,
                              const char *edge_type, char *props, int n, const CBMCall *call) {
    if (n > 0 && (size_t)n < CBM_SZ_2K - PP_ESC_SPACE) {
        size_t pos = append_args_json(props, CBM_SZ_2K, (size_t)n, call);
        if (pos < CBM_SZ_2K - SKIP_ONE) {
            props[pos] = '}';
            props[pos + SKIP_ONE] = '\0';
        }
    }
    cbm_gbuf_insert_edge(gbuf, src_id, tgt_id, edge_type, props);
}

/* Build Route node QN and properties for HTTP/async service edges. */
static int64_t build_service_route(cbm_gbuf_t *gbuf, const char *arg, const char *method,
                                   const char *broker, cbm_svc_kind_t svc) {
    char route_qn[CBM_ROUTE_QN_SIZE];
    const char *prefix;
    if (svc == CBM_SVC_HTTP) {
        prefix = method ? method : "ANY";
    } else {
        prefix = broker ? broker : "async";
    }
    snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", prefix, arg);
    char route_props[CBM_SZ_256];
    if (method) {
        snprintf(route_props, sizeof(route_props), "{\"method\":\"%s\"}", method);
    } else if (broker) {
        snprintf(route_props, sizeof(route_props), "{\"broker\":\"%s\"}", broker);
    } else {
        snprintf(route_props, sizeof(route_props), "{}");
    }
    return cbm_gbuf_upsert_node(gbuf, "Route", arg, route_qn, "", 0, 0, route_props);
}

/* Emit HTTP_CALLS or ASYNC_CALLS edge via Route node. */
static void emit_http_async_service_edge(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source,
                                         const CBMCall *call, const cbm_resolution_t *res,
                                         cbm_svc_kind_t svc, const char *arg) {
    const char *edge_type = (svc == CBM_SVC_HTTP) ? "HTTP_CALLS" : "ASYNC_CALLS";
    const char *method =
        (svc == CBM_SVC_HTTP) ? cbm_service_pattern_http_method(call->callee_name) : NULL;
    const char *broker =
        (svc == CBM_SVC_ASYNC) ? cbm_service_pattern_broker(res->qualified_name) : NULL;

    int64_t route_id = build_service_route(gbuf, arg, method, broker, svc);

    char esc_c[CBM_SZ_256];
    char esc_a[CBM_SZ_256];
    cbm_json_escape(esc_c, sizeof(esc_c), call->callee_name);
    cbm_json_escape(esc_a, sizeof(esc_a), arg);
    char props[CBM_SZ_2K];
    int n = snprintf(props, sizeof(props), "{\"callee\":\"%s\",\"url_path\":\"%s\"", esc_c, esc_a);
    if (method) {
        n += snprintf(props + n, sizeof(props) - (size_t)n, ",\"method\":\"%s\"", method);
    }
    if (broker) {
        n += snprintf(props + n, sizeof(props) - (size_t)n, ",\"broker\":\"%s\"", broker);
    }
    finalize_and_emit(gbuf, source->id, route_id, edge_type, props, n, call);
}

/* Emit CONFIGURES edge. */
static void emit_config_edge(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source,
                             const cbm_gbuf_node_t *target, const CBMCall *call,
                             const cbm_resolution_t *res, const char *arg) {
    char esc_c[CBM_SZ_256];
    char esc_k[CBM_SZ_256];
    cbm_json_escape(esc_c, sizeof(esc_c), call->callee_name);
    cbm_json_escape(esc_k, sizeof(esc_k), arg ? arg : "");
    char props[CBM_SZ_2K];
    int n = snprintf(props, sizeof(props), "{\"callee\":\"%s\",\"key\":\"%s\",\"confidence\":%.2f",
                     esc_c, esc_k, res->confidence);
    finalize_and_emit(gbuf, source->id, target->id, "CONFIGURES", props, n, call);
}

/* Emit normal CALLS edge. */
static void emit_normal_calls_edge(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source,
                                   const cbm_gbuf_node_t *target, const CBMCall *call,
                                   const cbm_resolution_t *res) {
    char esc_c[CBM_SZ_256];
    cbm_json_escape(esc_c, sizeof(esc_c), call->callee_name);
    char props[CBM_SZ_2K];
    int n = snprintf(props, sizeof(props),
                     "{\"callee\":\"%s\",\"confidence\":%.2f,\"strategy\":\"%s\",\"candidates\":%d",
                     esc_c, res->confidence, res->strategy ? res->strategy : "unknown",
                     res->candidate_count);
    finalize_and_emit(gbuf, source->id, target->id, "CALLS", props, n, call);
}

/* Classify a resolved call by library identity and emit the appropriate edge. */
/* Create Route node + CALLS + HANDLES edges for a route registration call. */
static void emit_route_registration(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source,
                                    const CBMCall *call, const char *route_path,
                                    const char *handler_ref, const char *module_qn,
                                    const cbm_registry_t *registry, const cbm_gbuf_t *main_gbuf,
                                    const char **ik, const char **iv, int ic) {
    const char *method = cbm_service_pattern_route_method(call->callee_name);
    char rqn[CBM_ROUTE_QN_SIZE];
    snprintf(rqn, sizeof(rqn), "__route__%s__%s", method ? method : "ANY", route_path);
    char rp[CBM_SZ_256];
    snprintf(rp, sizeof(rp), "{\"method\":\"%s\"}", method ? method : "ANY");
    int64_t rid = cbm_gbuf_upsert_node(gbuf, "Route", route_path, rqn, "", 0, 0, rp);
    char props[CBM_SZ_512];
    snprintf(props, sizeof(props),
             "{\"callee\":\"%s\",\"url_path\":\"%s\",\"via\":\"route_registration\"}",
             call->callee_name, route_path);
    cbm_gbuf_insert_edge(gbuf, source->id, rid, "CALLS", props);
    if (handler_ref && handler_ref[0] != '\0') {
        cbm_resolution_t hres = cbm_registry_resolve(registry, handler_ref, module_qn, ik, iv, ic);
        if (hres.qualified_name && hres.qualified_name[0] != '\0') {
            const cbm_gbuf_node_t *h = cbm_gbuf_find_by_qn(main_gbuf, hres.qualified_name);
            if (h) {
                char hp[CBM_SZ_256];
                snprintf(hp, sizeof(hp), "{\"handler\":\"%s\"}", hres.qualified_name);
                cbm_gbuf_insert_edge(gbuf, h->id, rid, "HANDLES", hp);
            }
        }
    }
}

/* Reject regex metacharacters, spaces, double-slashes in URL candidates. */
static bool is_junk_url(const char *s) {
    for (int i = 0; s[i]; i++) {
        char ch = s[i];
        if (ch == '\\' || ch == '^' || ch == '$' || ch == '*' || ch == '+' || ch == '(' ||
            ch == ')' || ch == '[' || ch == ']' || ch == '|' || ch == ' ') {
            return true;
        }
        if (ch == '/' && i > 0 && s[i - SKIP_ONE] == '/') {
            return true;
        }
    }
    return false;
}

/* Normalize a template literal URL and reject junk patterns.
 * Returns true if norm contains a valid API path. */
static bool normalize_url_arg(const char *url, char *norm, int norm_sz) {
    int ni = 0;
    const char *p = url;
    if (*p == '`' || *p == '"' || *p == '\'') {
        p++;
    }
    if (*p != '/') {
        return false;
    }
    while (*p && ni < norm_sz - PAIR_LEN) {
        if (*p == '$' && *(p + SKIP_ONE) == '{') {
            norm[ni++] = ':';
            p += PAIR_LEN;
            while (*p && *p != '}' && ni < norm_sz - PAIR_LEN) {
                norm[ni++] = *p++;
            }
            if (*p == '}') {
                p++;
            }
        } else if (*p == '`' || *p == '"' || *p == '\'' || *p == '?') {
            break;
        } else {
            norm[ni++] = *p++;
        }
    }
    norm[ni] = '\0';
    enum { MIN_URL_LEN = 4 };
    if (ni < MIN_URL_LEN || !strchr(norm + SKIP_ONE, '/')) {
        return false;
    }
    return !is_junk_url(norm);
}

/* Detect API paths in call arguments and create HTTP_CALLS edges. */
static void detect_url_in_args(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source,
                               const CBMCall *call) {
    for (int ai = 0; ai < call->arg_count; ai++) {
        const CBMCallArg *ca = &call->args[ai];
        const char *url = ca->value ? ca->value : ca->expr;
        if (!url || (url[0] != '/' && url[0] != '`')) {
            continue;
        }
        char norm[CBM_SZ_256];
        if (!normalize_url_arg(url, norm, (int)sizeof(norm))) {
            continue;
        }
        char route_qn[CBM_ROUTE_QN_SIZE];
        snprintf(route_qn, sizeof(route_qn), "__route__ANY__%s", norm);
        int64_t route_id = cbm_gbuf_upsert_node(gbuf, "Route", norm, route_qn, "", 0, 0,
                                                "{\"source\":\"arg_url\"}");
        char esc_c[CBM_SZ_256];
        char esc_n[CBM_SZ_256];
        cbm_json_escape(esc_c, sizeof(esc_c), call->callee_name);
        cbm_json_escape(esc_n, sizeof(esc_n), norm);
        char eprops[CBM_SZ_512];
        snprintf(eprops, sizeof(eprops),
                 "{\"callee\":\"%s\",\"url_path\":\"%s\",\"via\":\"arg_url\"}", esc_c, esc_n);
        cbm_gbuf_insert_edge(gbuf, source->id, route_id, "HTTP_CALLS", eprops);
        break;
    }
}

static void emit_service_edge(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source,
                              const cbm_gbuf_node_t *target, const CBMCall *call,
                              const cbm_resolution_t *res, const char *module_qn,
                              const cbm_registry_t *registry, const cbm_gbuf_t *main_gbuf,
                              const char **imp_keys, const char **imp_vals, int imp_count) {
    cbm_svc_kind_t svc = cbm_service_pattern_match(res->qualified_name);
    const char *arg = call->first_string_arg;

    /* Also detect route registration by callee name suffix alone (handles unresolved
     * local variables like app.include_router where QN resolution fails). */
    if (svc == CBM_SVC_NONE && cbm_service_pattern_route_method(call->callee_name) != NULL) {
        svc = CBM_SVC_ROUTE_REG;
    }

    if (svc == CBM_SVC_ROUTE_REG) {
        const char *handler_ref = NULL;
        const char *route_path = find_route_path_in_args(call, &handler_ref);
        if (route_path) {
            emit_route_registration(gbuf, source, call, route_path, handler_ref, module_qn,
                                    registry, main_gbuf, imp_keys, imp_vals, imp_count);
            return;
        }
        /* No path found — fall through to normal CALLS edge */
    }

    bool has_url = (arg && arg[0] != '\0' && (arg[0] == '/' || strstr(arg, "://") != NULL));
    bool has_topic = (arg && arg[0] != '\0' && svc == CBM_SVC_ASYNC && strlen(arg) > PP_ESC_SPACE);

    if ((svc == CBM_SVC_HTTP || svc == CBM_SVC_ASYNC) && (has_url || has_topic)) {
        emit_http_async_service_edge(gbuf, source, call, res, svc, arg);
    } else if (svc == CBM_SVC_CONFIG) {
        emit_config_edge(gbuf, source, target, call, res, arg);
    } else {
        emit_normal_calls_edge(gbuf, source, target, call, res);
    }

    detect_url_in_args(gbuf, source, call);
}

/* Find the source node for an edge: enclosing function or file node. */
static const cbm_gbuf_node_t *find_source_node(const cbm_gbuf_t *gbuf, const char *project,
                                               const char *rel, const char *enclosing_qn) {
    const cbm_gbuf_node_t *src = NULL;
    if (enclosing_qn) {
        src = cbm_gbuf_find_by_qn(gbuf, enclosing_qn);
    }
    if (!src) {
        char *file_qn = cbm_pipeline_fqn_compute(project, rel, "__file__");
        src = cbm_gbuf_find_by_qn(gbuf, file_qn);
        free(file_qn);
    }
    return src;
}

/* Resolve calls for one file and emit CALLS/HTTP_CALLS/ASYNC_CALLS edges. */
static void resolve_file_calls(resolve_ctx_t *rc, resolve_worker_state_t *ws, CBMFileResult *result,
                               const char *rel, const char *module_qn, const char **imp_keys,
                               const char **imp_vals, int imp_count) {
    for (int c = 0; c < result->calls.count; c++) {
        CBMCall *call = &result->calls.items[c];
        if (!call->callee_name) {
            continue;
        }
        const cbm_gbuf_node_t *source_node =
            find_source_node(rc->main_gbuf, rc->project_name, rel, call->enclosing_func_qn);
        if (!source_node) {
            continue;
        }

        cbm_resolution_t res = cbm_registry_resolve(rc->registry, call->callee_name, module_qn,
                                                    imp_keys, imp_vals, imp_count);
        if (!res.qualified_name || res.qualified_name[0] == '\0') {
            if (cbm_service_pattern_route_method(call->callee_name) != NULL) {
                cbm_resolution_t fake_res = {.qualified_name = call->callee_name,
                                             .confidence = PP_HALF_CONF,
                                             .strategy = "callee_suffix"};
                emit_service_edge(ws->local_edge_buf, source_node, source_node, call, &fake_res,
                                  module_qn, rc->registry, rc->main_gbuf, imp_keys, imp_vals,
                                  imp_count);
            }
            continue;
        }
        const cbm_gbuf_node_t *target_node = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
        if (!target_node || source_node->id == target_node->id) {
            continue;
        }
        emit_service_edge(ws->local_edge_buf, source_node, target_node, call, &res, module_qn,
                          rc->registry, rc->main_gbuf, imp_keys, imp_vals, imp_count);
        ws->calls_resolved++;
    }
}

/* Resolve usages for one file. */
static void resolve_file_usages(resolve_ctx_t *rc, resolve_worker_state_t *ws,
                                CBMFileResult *result, const char *rel, const char *module_qn,
                                const char **imp_keys, const char **imp_vals, int imp_count) {
    for (int u = 0; u < result->usages.count; u++) {
        CBMUsage *usage = &result->usages.items[u];
        if (!usage->ref_name) {
            continue;
        }
        const cbm_gbuf_node_t *src =
            find_source_node(rc->main_gbuf, rc->project_name, rel, usage->enclosing_func_qn);
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
        char uprops[CBM_SZ_256];
        snprintf(uprops, sizeof(uprops), "{\"callee\":\"%s\"}", usage->ref_name);
        cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, "USAGE", uprops);
        ws->usages_resolved++;
    }
}

/* Resolve throws/raises for one file. */
static void resolve_file_throws(resolve_ctx_t *rc, resolve_worker_state_t *ws,
                                CBMFileResult *result, const char *module_qn, const char **imp_keys,
                                const char **imp_vals, int imp_count) {
    for (int t = 0; t < result->throws.count; t++) {
        CBMThrow *thr = &result->throws.items[t];
        if (!thr->exception_name || !thr->enclosing_func_qn) {
            continue;
        }
        const cbm_gbuf_node_t *src = cbm_gbuf_find_by_qn(rc->main_gbuf, thr->enclosing_func_qn);
        if (!src) {
            continue;
        }
        const char *edge_type = is_checked_exception(thr->exception_name) ? "THROWS" : "RAISES";
        cbm_resolution_t res = cbm_registry_resolve(rc->registry, thr->exception_name, module_qn,
                                                    imp_keys, imp_vals, imp_count);
        if (!res.qualified_name || res.qualified_name[0] == '\0') {
            continue;
        }
        const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
        if (!tgt || src->id == tgt->id) {
            continue;
        }
        cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, edge_type, "{}");
    }
}

/* Resolve reads/writes for one file. */
static void resolve_file_rw(resolve_ctx_t *rc, resolve_worker_state_t *ws, CBMFileResult *result,
                            const char *rel, const char *module_qn, const char **imp_keys,
                            const char **imp_vals, int imp_count) {
    for (int r = 0; r < result->rw.count; r++) {
        CBMReadWrite *rw = &result->rw.items[r];
        if (!rw->var_name) {
            continue;
        }
        const cbm_gbuf_node_t *src =
            find_source_node(rc->main_gbuf, rc->project_name, rel, rw->enclosing_func_qn);
        if (!src) {
            continue;
        }
        cbm_resolution_t res = cbm_registry_resolve(rc->registry, rw->var_name, module_qn, imp_keys,
                                                    imp_vals, imp_count);
        if (!res.qualified_name || res.qualified_name[0] == '\0') {
            continue;
        }
        const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
        if (!tgt || src->id == tgt->id) {
            continue;
        }
        const char *etype = rw->is_write ? "WRITES" : "READS";
        cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, etype, "{}");
    }
}

/* Resolve base_classes → INHERITS edges for one definition. */
static void resolve_def_inherits(resolve_ctx_t *rc, resolve_worker_state_t *ws,
                                 const CBMDefinition *def, const cbm_gbuf_node_t *node,
                                 const char *mq, const char **ik, const char **iv, int ic) {
    if (!def->base_classes) {
        return;
    }
    for (int b = 0; def->base_classes[b]; b++) {
        const char *bqn = resolve_as_class(rc->registry, def->base_classes[b], mq, ik, iv, ic);
        if (!bqn) {
            continue;
        }
        const cbm_gbuf_node_t *bn = cbm_gbuf_find_by_qn(rc->main_gbuf, bqn);
        if (bn && node->id != bn->id) {
            cbm_gbuf_insert_edge(ws->local_edge_buf, node->id, bn->id, "INHERITS", "{}");
            ws->semantic_resolved++;
        }
    }
}

/* Resolve decorators → DECORATES edges for one definition. */
static void resolve_def_decorators(resolve_ctx_t *rc, resolve_worker_state_t *ws,
                                   const CBMDefinition *def, const cbm_gbuf_node_t *node,
                                   const char *mq, const char **ik, const char **iv, int ic) {
    if (!def->decorators) {
        return;
    }
    for (int dc = 0; def->decorators[dc]; dc++) {
        char fn[CBM_SZ_256];
        extract_decorator_func(def->decorators[dc], fn, sizeof(fn));
        if (fn[0] == '\0') {
            continue;
        }
        cbm_resolution_t res = cbm_registry_resolve(rc->registry, fn, mq, ik, iv, ic);
        if (!res.qualified_name || res.qualified_name[0] == '\0') {
            continue;
        }
        const cbm_gbuf_node_t *dn = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
        if (dn && node->id != dn->id) {
            char dp[CBM_SZ_256];
            snprintf(dp, sizeof(dp), "{\"decorator\":\"%s\"}", def->decorators[dc]);
            cbm_gbuf_insert_edge(ws->local_edge_buf, node->id, dn->id, "DECORATES", dp);
            ws->semantic_resolved++;
        }
    }
}

/* Resolve INHERITS + DECORATES + IMPLEMENTS for one file. */
static void resolve_file_semantic(resolve_ctx_t *rc, resolve_worker_state_t *ws,
                                  CBMFileResult *result, const char *module_qn,
                                  const char **imp_keys, const char **imp_vals, int imp_count) {
    for (int d = 0; d < result->defs.count; d++) {
        CBMDefinition *def = &result->defs.items[d];
        if (!def->qualified_name) {
            continue;
        }
        const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(rc->main_gbuf, def->qualified_name);
        if (!node) {
            continue;
        }
        resolve_def_inherits(rc, ws, def, node, module_qn, imp_keys, imp_vals, imp_count);
        resolve_def_decorators(rc, ws, def, node, module_qn, imp_keys, imp_vals, imp_count);
    }
    for (int t = 0; t < result->impl_traits.count; t++) {
        CBMImplTrait *it = &result->impl_traits.items[t];
        if (!it->trait_name || !it->struct_name) {
            continue;
        }
        const char *tqn = resolve_as_class(rc->registry, it->trait_name, module_qn, imp_keys,
                                           imp_vals, imp_count);
        const char *sqn = resolve_as_class(rc->registry, it->struct_name, module_qn, imp_keys,
                                           imp_vals, imp_count);
        if (!tqn || !sqn) {
            continue;
        }
        const cbm_gbuf_node_t *tn = cbm_gbuf_find_by_qn(rc->main_gbuf, tqn);
        const cbm_gbuf_node_t *sn = cbm_gbuf_find_by_qn(rc->main_gbuf, sqn);
        if (tn && sn && tn->id != sn->id) {
            cbm_gbuf_insert_edge(ws->local_edge_buf, sn->id, tn->id, "IMPLEMENTS", "{}");
            ws->semantic_resolved++;
        }
    }
}

static void resolve_worker(int worker_id, void *ctx_ptr) {
    resolve_ctx_t *rc = ctx_ptr;
    resolve_worker_state_t *ws = &rc->workers[worker_id];

    if (!ws->local_edge_buf) {
        ws->local_edge_buf =
            cbm_gbuf_new_shared_ids(rc->project_name, rc->repo_path, rc->shared_ids);
    }

    while (SKIP_ONE) {
        int file_idx =
            atomic_fetch_add_explicit(&rc->next_file_idx, SKIP_ONE, memory_order_relaxed);
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
        resolve_file_calls(rc, ws, result, rel, module_qn, imp_keys, imp_vals, imp_count);

        /* ── USAGE resolution ──────────────────────────────────── */
        resolve_file_usages(rc, ws, result, rel, module_qn, imp_keys, imp_vals, imp_count);

        /* ── THROWS / RAISES ───────────────────────────────────── */
        resolve_file_throws(rc, ws, result, module_qn, imp_keys, imp_vals, imp_count);

        /* ── READS / WRITES ────────────────────────────────────── */
        resolve_file_rw(rc, ws, result, rel, module_qn, imp_keys, imp_vals, imp_count);

        /* ── INHERITS + DECORATES + IMPLEMENTS ──────────────────── */
        resolve_file_semantic(rc, ws, result, module_qn, imp_keys, imp_vals, imp_count);

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
        return CBM_NOT_FOUND;
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

    /* Sub-phase: Dispatch resolve workers (per-file call/usage resolution, PARALLEL) */
    CBM_PROF_START(t_resolve_dispatch);
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, resolve_worker, &rc, opts);
    CBM_PROF_END_N("parallel_resolve", "1_dispatch_workers_parallel", t_resolve_dispatch,
                   file_count);

    /* Sub-phase: Merge all local edge bufs into main gbuf (SEQUENTIAL) */
    CBM_PROF_START(t_resolve_merge);
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
    CBM_PROF_END_N("parallel_resolve", "2_merge_edge_bufs_seq", t_resolve_merge,
                   total_calls + total_usages);

    cbm_aligned_free(workers);

    /* Go-style implicit interface satisfaction (needs full graph, serial) */
    int go_impl = cbm_pipeline_implements_go(ctx);

    if (atomic_load(ctx->cancelled)) {
        return CBM_NOT_FOUND;
    }

    cbm_log_info("parallel.resolve.done", "calls", itoa_log(total_calls), "usages",
                 itoa_log(total_usages), "semantic", itoa_log(total_semantic + go_impl));
    return 0;
}
