/*
 * pipeline.c — Indexing pipeline orchestrator.
 *
 * Coordinates multi-pass indexing:
 *   1. Discover files
 *   2. Build structure (Project/Folder/Package/File nodes)
 *   3. Bulk load sources (read + LZ4 HC compress)
 *   4. Extract definitions (fused: extract + write nodes + build registry)
 *   5. Resolve imports, calls, usages, semantic edges
 *   6. Post-passes: tests, communities, HTTP links, git history
 *   7. Dump graph buffer to SQLite
 */
#include "foundation/constants.h"

enum { CBM_DIR_PERMS = 0755, PL_RING = 4, PL_RING_MASK = 3, PL_SEQ_PASSES = 6, PL_WAL_BUF = 1040 };
#define PL_NSEC_PER_SEC 1000000000LL
#include "pipeline/pipeline.h"
#include "pipeline/artifact.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/worker_pool.h"
#include "graph_buffer/graph_buffer.h"
#include "git/git_context.h"
#include "store/store.h"
#include "macro_table.h"
#include "arena.h"
#include "discover/discover.h"
#include "discover/userconfig.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "foundation/str_util.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/profile.h"
#include "foundation/mem.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <time.h>
#ifdef _WIN32
#include <process.h>
#define cbm_pipeline_getpid _getpid
#else
#include <unistd.h>
#define cbm_pipeline_getpid getpid
#endif

static inline void *intptr_to_ptr(intptr_t v) {
    void *p;
    memcpy(&p, &v, sizeof(p));
    return p;
}

/* ── Global index lock ─────────────────────────────────────────── */
/* Prevents concurrent pipeline runs on the same DB file.
 * Atomic spinlock: 0 = free, 1 = locked. */
static atomic_int g_pipeline_busy = 0;

#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
static atomic_bool g_persist_test_fail_after_stage_dump = false;
static atomic_bool g_persist_test_cancel_after_predump = false;
static atomic_bool g_persist_test_cancel_after_destination_prepare = false;
static atomic_bool g_persist_test_fail_adr_capture = false;
static cbm_pipeline_test_hook_fn g_persist_test_before_final_manifest = NULL;
static void *g_persist_test_before_final_manifest_userdata = NULL;

void cbm_pipeline_incremental_test_fail_after_stage_dump_once(void) {
    atomic_store(&g_persist_test_fail_after_stage_dump, true);
}

void cbm_pipeline_incremental_test_cancel_after_predump_once(void) {
    atomic_store(&g_persist_test_cancel_after_predump, true);
}

void cbm_pipeline_incremental_test_cancel_after_destination_prepare_once(void) {
    atomic_store(&g_persist_test_cancel_after_destination_prepare, true);
}

void cbm_pipeline_incremental_test_fail_adr_capture_once(void) {
    atomic_store(&g_persist_test_fail_adr_capture, true);
}

void cbm_pipeline_incremental_test_before_final_manifest_once(cbm_pipeline_test_hook_fn hook,
                                                              void *userdata) {
    g_persist_test_before_final_manifest = hook;
    g_persist_test_before_final_manifest_userdata = userdata;
}

void cbm_pipeline_persist_test_run_before_final_manifest(void) {
    cbm_pipeline_test_hook_fn hook = g_persist_test_before_final_manifest;
    void *userdata = g_persist_test_before_final_manifest_userdata;
    g_persist_test_before_final_manifest = NULL;
    g_persist_test_before_final_manifest_userdata = NULL;
    if (hook) {
        hook(userdata);
    }
}

bool cbm_pipeline_persist_test_take_failure_after_stage_dump(void) {
    return atomic_exchange(&g_persist_test_fail_after_stage_dump, false);
}

bool cbm_pipeline_persist_test_take_cancel_after_predump(void) {
    return atomic_exchange(&g_persist_test_cancel_after_predump, false);
}

bool cbm_pipeline_persist_test_take_cancel_after_destination_prepare(void) {
    return atomic_exchange(&g_persist_test_cancel_after_destination_prepare, false);
}

void cbm_pipeline_persist_test_reset_faults(void) {
    atomic_store(&g_persist_test_fail_after_stage_dump, false);
    atomic_store(&g_persist_test_cancel_after_predump, false);
    atomic_store(&g_persist_test_cancel_after_destination_prepare, false);
    atomic_store(&g_persist_test_fail_adr_capture, false);
    g_persist_test_before_final_manifest = NULL;
    g_persist_test_before_final_manifest_userdata = NULL;
}
#endif

bool cbm_pipeline_try_lock(void) {
    return atomic_exchange(&g_pipeline_busy, 1) == 0;
}

#define LOCK_SPIN_NS 100000000 /* 100ms between lock retries */

void cbm_pipeline_lock(void) {
    while (atomic_exchange(&g_pipeline_busy, 1) != 0) {
        struct timespec ts = {0, LOCK_SPIN_NS};
        cbm_nanosleep(&ts, NULL);
    }
}

void cbm_pipeline_unlock(void) {
    atomic_store(&g_pipeline_busy, 0);
}

/* ── Internal state ──────────────────────────────────────────────── */

struct cbm_pipeline {
    char *repo_path;
    char *db_path;
    char *project_name;
    cbm_git_context_t git_ctx;
    char *branch_qn;
    cbm_index_mode_t mode;
    atomic_int cancelled;
    bool persistence; /* write .codebase-memory/graph.db.zst after indexing */

    /* Indexing state (set during run) */
    cbm_gbuf_t *gbuf;
    cbm_registry_t *registry;

    /* Directory subtrees skipped during discovery (rel paths). Captured from
     * cbm_discover_ex so the MCP layer can report excluded subtrees (#411).
     * Owned by the pipeline; freed in cbm_pipeline_free. */
    char **excluded_dirs;
    int excluded_count;

    /* Individual files dropped by ignore rules during discovery (#963
     * "purposely not indexed" — by design, not failures). Stored entries are
     * capped in discovery; ignored_total keeps the uncapped count so
     * truncation stays explicit. Owned by the pipeline. */
    cbm_ignored_file_t *ignored_files;
    int ignored_count;
    int ignored_total;

    /* Per-file indexing failures (skipped files) surfaced via MCP/CLI/logfile
     * (Stage 2 / Track B). A skip is the expected handled outcome of a bad or
     * oversized file — the run still succeeds ("indexed"). Owned by the
     * pipeline; freed in cbm_pipeline_free. */
    cbm_file_error_t *file_errors;
    int file_errors_count;
    int file_errors_cap;

    /* User-defined extension overrides (loaded once per run) */
    cbm_userconfig_t *userconfig;

    /* Committed graph size at dump time (-1 = dump did not run). #334 gate axis. */
    int committed_nodes;
    int committed_edges;

    /* ADR (project_summaries) captured before a full-reindex DB delete, so it
     * can be restored after the rebuild. NULL when no ADR existed. Issue #516. */
    char *saved_adr;
};

/* ── Global pkgmap (one active pipeline at a time) ─────────────── */

static CBMHashTable *g_pkgmap = NULL;

CBMHashTable *cbm_pipeline_get_pkgmap(void) {
    return g_pkgmap;
}

void cbm_pipeline_set_pkgmap(CBMHashTable *map) {
    g_pkgmap = map;
}

/* ── Timing helper ──────────────────────────────────────────────── */

static double elapsed_ms(struct timespec start) {
    struct timespec now;
    cbm_clock_gettime(CLOCK_MONOTONIC, &now);
    return ((double)(now.tv_sec - start.tv_sec) * CBM_MS_PER_SEC) +
           ((double)(now.tv_nsec - start.tv_nsec) / CBM_US_PER_SEC_F);
}

/* Format int to string for logging. Thread-safe via TLS rotating buffers. */
static const char *itoa_buf(int val) {
    static CBM_TLS char bufs[PL_RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & PL_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Log current + peak RSS at a pipeline phase boundary (memory profiling). */
static void log_phase_mem(const char *phase) {
    enum { PL_BYTES_PER_MB = 1024 * 1024 };
    cbm_log_info("mem.phase", "phase", phase, "rss_mb",
                 itoa_buf((int)(cbm_mem_rss() / PL_BYTES_PER_MB)), "peak_mb",
                 itoa_buf((int)(cbm_mem_peak_rss() / PL_BYTES_PER_MB)));
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

cbm_pipeline_t *cbm_pipeline_new(const char *repo_path, const char *db_path,
                                 cbm_index_mode_t mode) {
    if (!repo_path) {
        return NULL;
    }

    cbm_pipeline_t *p = calloc(CBM_ALLOC_ONE, sizeof(cbm_pipeline_t));
    if (!p) {
        return NULL;
    }

    p->repo_path = strdup(repo_path);
    p->db_path = db_path ? strdup(db_path) : NULL;
    p->project_name = cbm_project_name_from_path(repo_path);
    (void)cbm_git_context_resolve(repo_path, &p->git_ctx);
    p->branch_qn = cbm_git_context_branch_qn(p->project_name, &p->git_ctx);
    p->mode = mode;
    p->persistence = false;
    p->committed_nodes = -1;
    p->committed_edges = -1;
    atomic_init(&p->cancelled, 0);

    return p;
}

static int pipeline_refresh_git_context(cbm_pipeline_t *p) {
    cbm_git_context_t fresh = {0};
    if (!p || cbm_git_context_resolve(p->repo_path, &fresh) != 0) {
        cbm_git_context_free(&fresh);
        return CBM_NOT_FOUND;
    }
    char *fresh_branch_qn = cbm_git_context_branch_qn(p->project_name, &fresh);
    if (!fresh_branch_qn) {
        cbm_git_context_free(&fresh);
        return CBM_NOT_FOUND;
    }
    cbm_git_context_free(&p->git_ctx);
    free(p->branch_qn);
    p->git_ctx = fresh;
    p->branch_qn = fresh_branch_qn;
    return 0;
}

void cbm_pipeline_set_persistence(cbm_pipeline_t *p, bool enabled) {
    if (p) {
        p->persistence = enabled;
    }
}

bool cbm_pipeline_set_project_name(cbm_pipeline_t *p, const char *name) {
    if (!p || !name || !name[0]) {
        return false;
    }

    char *normalized = cbm_project_name_from_path(name);
    if (!normalized) {
        return false;
    }
    if (!cbm_validate_project_name(normalized)) {
        free(normalized);
        return false;
    }

    free(p->project_name);
    p->project_name = normalized;
    free(p->branch_qn);
    p->branch_qn = cbm_git_context_branch_qn(p->project_name, &p->git_ctx);
    return true;
}

void cbm_pipeline_free(cbm_pipeline_t *p) {
    if (!p) {
        return;
    }
    free(p->repo_path);
    free(p->db_path);
    free(p->project_name);
    cbm_discover_free_excluded(p->excluded_dirs, p->excluded_count);
    p->excluded_dirs = NULL;
    p->excluded_count = 0;
    cbm_discover_free_ignored(p->ignored_files, p->ignored_count);
    p->ignored_files = NULL;
    p->ignored_count = 0;
    p->ignored_total = 0;
    for (int i = 0; i < p->file_errors_count; i++) {
        free(p->file_errors[i].path);
        free(p->file_errors[i].reason);
        free(p->file_errors[i].phase);
    }
    free(p->file_errors);
    p->file_errors = NULL;
    p->file_errors_count = 0;
    p->file_errors_cap = 0;
    free(p->branch_qn);
    free(p->saved_adr); /* freed here too: error paths can exit before the
                         * restore in dump_and_persist_hashes runs. Issue #516. */
    p->saved_adr = NULL;
    cbm_git_context_free(&p->git_ctx);
    /* gbuf, store, registry freed during/after run */
    /* Defensively free userconfig in case run() was never called or panicked */
    if (p->userconfig) {
        cbm_set_user_lang_config(NULL);
        cbm_userconfig_free(p->userconfig);
        p->userconfig = NULL;
    }
    free(p);
}

void cbm_pipeline_cancel(cbm_pipeline_t *p) {
    if (p) {
        atomic_store(&p->cancelled, 1);
    }
}

const char *cbm_pipeline_project_name(const cbm_pipeline_t *p) {
    return p ? p->project_name : NULL;
}

const char *cbm_pipeline_repo_path(const cbm_pipeline_t *p) {
    return p ? p->repo_path : NULL;
}

atomic_int *cbm_pipeline_cancelled_ptr(cbm_pipeline_t *p) {
    return p ? &p->cancelled : NULL;
}

int cbm_pipeline_get_mode(const cbm_pipeline_t *p) {
    return p ? (int)p->mode : 0;
}

void cbm_pipeline_get_excluded(const cbm_pipeline_t *p, char ***out, int *count) {
    if (out) {
        *out = p ? p->excluded_dirs : NULL;
    }
    if (count) {
        *count = p ? p->excluded_count : 0;
    }
}

/* NULL-safe heap strdup (avoids a strdup dependency + guards NULL inputs). */
static char *fe_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t n = strlen(s) + 1;
    char *d = (char *)malloc(n);
    if (d) {
        memcpy(d, s, n);
    }
    return d;
}

void cbm_pipeline_add_file_error(cbm_pipeline_t *p, const char *path, const char *reason,
                                 const char *phase) {
    if (!p) {
        return;
    }
    if (p->file_errors_count >= p->file_errors_cap) {
        int ncap = p->file_errors_cap ? p->file_errors_cap * 2 : 16;
        cbm_file_error_t *grown =
            (cbm_file_error_t *)realloc(p->file_errors, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            /* Never abort indexing just to record a skip — drop this record. */
            return;
        }
        p->file_errors = grown;
        p->file_errors_cap = ncap;
    }
    cbm_file_error_t *e = &p->file_errors[p->file_errors_count];
    e->path = fe_strdup(path);
    e->reason = fe_strdup(reason);
    e->phase = fe_strdup(phase);
    p->file_errors_count++;
}

void cbm_pipeline_get_file_errors(const cbm_pipeline_t *p, cbm_file_error_t **out, int *count) {
    if (out) {
        *out = p ? p->file_errors : NULL;
    }
    if (count) {
        *count = p ? p->file_errors_count : 0;
    }
}

void cbm_pipeline_get_ignored(const cbm_pipeline_t *p, cbm_ignored_file_t **out, int *count,
                              int *total) {
    if (out) {
        *out = p ? p->ignored_files : NULL;
    }
    if (count) {
        *count = p ? p->ignored_count : 0;
    }
    if (total) {
        *total = p ? p->ignored_total : 0;
    }
}

void cbm_pipeline_get_committed_counts(const cbm_pipeline_t *p, int *nodes, int *edges) {
    if (nodes) {
        *nodes = p ? p->committed_nodes : -1;
    }
    if (edges) {
        *edges = p ? p->committed_edges : -1;
    }
}

void cbm_pipeline_set_committed_counts(cbm_pipeline_t *p, int nodes, int edges) {
    if (p) {
        p->committed_nodes = nodes;
        p->committed_edges = edges;
    }
}

/* Effective worker count. The crash supervisor re-runs its worker single-
 * threaded (CBM_INDEX_SINGLE_THREAD=1) so a per-file marker can pin the EXACT
 * crasher; a parallel re-run would race the marker. Honour that override
 * everywhere the worker count drives the parallel/sequential decision, so the
 * whole extraction phase collapses to the deterministic sequential path. */
static int effective_worker_count(bool initial) {
    const char *st = getenv("CBM_INDEX_SINGLE_THREAD");
    if (st && st[0] == '1') {
        return 1;
    }
    return cbm_default_worker_count(initial);
}

/* Resolve the DB path for this pipeline. Caller must free(). */
static char *resolve_db_path(const cbm_pipeline_t *p) {
    char *path = malloc(CBM_SZ_1K);
    if (!path) {
        return NULL;
    }
    if (p->db_path) {
        snprintf(path, 1024, "%s", p->db_path);
    } else {
        snprintf(path, 1024, "%s/%s.db", cbm_resolve_cache_dir(), p->project_name);
    }
    return path;
}

static int check_cancel(const cbm_pipeline_t *p) {
    return atomic_load(&p->cancelled) ? CBM_NOT_FOUND : 0;
}

/* ── Hash table cleanup callback ─────────────────────────────────── */

static void free_seen_dir_key(const char *key, void *val, void *ud) {
    (void)val;
    (void)ud;
    free((void *)key);
}

/* ── Pass 1: Structure ──────────────────────────────────────────── */

/* Create Project, Folder/Package, and File nodes in the graph buffer. */
/* Walk directory chain upward, creating Folder nodes and CONTAINS_FOLDER edges. */
static void create_folder_chain(cbm_pipeline_t *p, const char *dir, CBMHashTable *seen_dirs) {
    char *walk = strdup(dir);
    while (walk[0] != '\0' && !cbm_ht_get(seen_dirs, walk)) {
        cbm_ht_set(seen_dirs, strdup(walk), intptr_to_ptr(SKIP_ONE));
        char *folder_qn = cbm_pipeline_fqn_folder(p->project_name, walk);
        const char *dir_base = strrchr(walk, '/');
        dir_base = dir_base ? dir_base + SKIP_ONE : walk;
        cbm_gbuf_upsert_node(p->gbuf, "Folder", dir_base, folder_qn, walk, 0, 0, "{}");

        char *pdir = strdup(walk);
        char *ps = strrchr(pdir, '/');
        if (ps) {
            *ps = '\0';
        } else {
            free(pdir);
            pdir = strdup("");
        }
        const char *pqn;
        char *pqn_heap = NULL;
        if (pdir[0] == '\0') {
            pqn = p->branch_qn ? p->branch_qn : p->project_name;
        } else {
            pqn_heap = cbm_pipeline_fqn_folder(p->project_name, pdir);
            pqn = pqn_heap;
        }
        const cbm_gbuf_node_t *fn = cbm_gbuf_find_by_qn(p->gbuf, folder_qn);
        const cbm_gbuf_node_t *pn = cbm_gbuf_find_by_qn(p->gbuf, pqn);
        if (fn && pn) {
            cbm_gbuf_insert_edge(p->gbuf, pn->id, fn->id, "CONTAINS_FOLDER", "{}");
        }
        free(folder_qn);
        free(pqn_heap);
        char *up = strrchr(walk, '/');
        if (up) {
            *up = '\0';
        } else {
            walk[0] = '\0';
        }
        free(pdir);
    }
    free(walk);
}

static int pass_structure(cbm_pipeline_t *p, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "structure", "files", itoa_buf(file_count));

    /* Project node */
    cbm_gbuf_upsert_node(p->gbuf, "Project", p->project_name, p->project_name, NULL, 0, 0, "{}");
    const char *branch_qn = p->branch_qn ? p->branch_qn : p->project_name;
    const char *branch_name = p->git_ctx.branch ? p->git_ctx.branch : "working-tree";
    char branch_props[CBM_SZ_2K];
    const char *branch_props_json = "{}";
    if (cbm_git_context_props_json(&p->git_ctx, branch_props, sizeof(branch_props)) > 0) {
        branch_props_json = branch_props;
    }
    if (p->branch_qn) {
        int64_t branch_id = cbm_gbuf_upsert_node(p->gbuf, "Branch", branch_name, branch_qn, NULL, 0,
                                                 0, branch_props_json);
        const cbm_gbuf_node_t *project_node = cbm_gbuf_find_by_qn(p->gbuf, p->project_name);
        if (project_node && branch_id > 0) {
            cbm_gbuf_insert_edge(p->gbuf, project_node->id, branch_id, "HAS_BRANCH",
                                 branch_props_json);
        }
    }

    /* Collect unique directories and create Folder/Package nodes */
    CBMHashTable *seen_dirs = cbm_ht_create(CBM_SZ_256);

    for (int i = 0; i < file_count; i++) {
        const char *rel = files[i].rel_path;
        if (!rel) {
            continue;
        }

        /* Create File node */
        char *file_qn = cbm_pipeline_fqn_compute(p->project_name, rel, "__file__");
        /* Extract basename */
        const char *slash = strrchr(rel, '/');
        const char *basename = slash ? slash + SKIP_ONE : rel;

        char props[CBM_SZ_256];
        const char *ext = strrchr(basename, '.');
        snprintf(props, sizeof(props), "{\"extension\":\"%s\"}", ext ? ext : "");

        const char *qualified_name = file_qn;
        const char *file_path = rel;
        cbm_gbuf_upsert_node(p->gbuf, "File", basename, qualified_name, file_path, 0, 0, props);

        /* CONTAINS_FILE edge: parent dir -> file */
        char *dir = strdup(rel);
        char *last_slash = strrchr(dir, '/');
        if (last_slash) {
            {
                *last_slash = '\0';
            }
        } else {
            free(dir);
            dir = strdup("");
        }

        const char *parent_qn;
        char *parent_qn_heap = NULL;
        if (dir[0] == '\0') {
            parent_qn = branch_qn;
        } else {
            parent_qn_heap = cbm_pipeline_fqn_folder(p->project_name, dir);
            parent_qn = parent_qn_heap;
        }

        /* Walk up directory chain, creating Folder nodes */
        create_folder_chain(p, dir, seen_dirs);

        /* Now create the CONTAINS_FILE edge */
        const cbm_gbuf_node_t *fnode = cbm_gbuf_find_by_qn(p->gbuf, file_qn);
        const cbm_gbuf_node_t *pnode = cbm_gbuf_find_by_qn(p->gbuf, parent_qn);
        if (fnode && pnode) {
            cbm_gbuf_insert_edge(p->gbuf, pnode->id, fnode->id, "CONTAINS_FILE", "{}");
        }

        free(file_qn);
        free(dir);
        free(parent_qn_heap);
    }

    /* Free seen_dirs keys */
    cbm_ht_foreach(seen_dirs, free_seen_dir_key, NULL);
    cbm_ht_free(seen_dirs);

    cbm_log_info("pass.done", "pass", "structure", "nodes", itoa_buf(cbm_gbuf_node_count(p->gbuf)),
                 "edges", itoa_buf(cbm_gbuf_edge_count(p->gbuf)));
    return 0;
}

/* ── Pass 2: Definitions ─────────────────────────────────────────── */

/* Implemented in pass_definitions.c via cbm_pipeline_pass_definitions() */

/* ── Githistory compute thread (for fused post-pass parallelism) ─── */

typedef struct {
    const char *repo_path;
    cbm_githistory_result_t *result;
} gh_compute_arg_t;

static void *gh_compute_thread_fn(void *arg) {
    gh_compute_arg_t *a = arg;
    cbm_pipeline_githistory_compute(a->repo_path, a->result);
    return NULL;
}

/* Extract Route nodes from URL strings found in config files (YAML, HCL, TOML).
 * These are infrastructure-defined endpoints (Cloud Scheduler, Terraform). */
/* Process infra bindings: topic→URL pairs from IaC configs.
 * Creates Route nodes for endpoints and HANDLES edges linking
 * topic Routes to endpoint Routes (bridging the gap). */
/* Process one infra binding: create Route node + INFRA_MAPS edge. */
static int process_one_infra_binding(cbm_gbuf_t *gbuf, const CBMInfraBinding *ib,
                                     const char *rel_path) {
    char url_route_qn[CBM_ROUTE_QN_SIZE];
    snprintf(url_route_qn, sizeof(url_route_qn), "__route__infra__%s", ib->target_url);
    int64_t url_route_id = cbm_gbuf_upsert_node(gbuf, "Route", ib->target_url, url_route_qn,
                                                rel_path, 0, 0, "{\"source\":\"infra\"}");
    char topic_route_qn[CBM_ROUTE_QN_SIZE];
    snprintf(topic_route_qn, sizeof(topic_route_qn), "__route__%s__%s",
             ib->broker ? ib->broker : "async", ib->source_name);
    const cbm_gbuf_node_t *topic_route = cbm_gbuf_find_by_qn(gbuf, topic_route_qn);
    int64_t topic_route_id;
    if (topic_route) {
        topic_route_id = topic_route->id;
    } else {
        /* The config file IS the declaration that the topic/queue/schedule exists;
         * upsert its Route node so the binding maps even when no code-side dispatch
         * call created the node first (e.g. a standalone scheduler/subscription
         * manifest). */
        topic_route_id = cbm_gbuf_upsert_node(gbuf, "Route", ib->source_name, topic_route_qn,
                                              rel_path, 0, 0, ib->broker ? ib->broker : "async");
        if (topic_route_id <= 0) {
            return 0;
        }
    }
    char props[CBM_SZ_512];
    snprintf(props, sizeof(props), "{\"broker\":\"%s\",\"topic\":\"%s\",\"endpoint\":\"%s\"}",
             ib->broker ? ib->broker : "async", ib->source_name, ib->target_url);
    cbm_gbuf_insert_edge(gbuf, topic_route_id, url_route_id, "INFRA_MAPS", props);
    return SKIP_ONE;
}

static void cbm_pipeline_process_infra_bindings(cbm_gbuf_t *gbuf, const cbm_file_info_t *files,
                                                CBMFileResult **result_cache, int file_count) {
    int bindings = 0;
    for (int i = 0; i < file_count; i++) {
        if (!result_cache[i]) {
            continue;
        }
        for (int bi = 0; bi < result_cache[i]->infra_bindings.count; bi++) {
            const CBMInfraBinding *ib = &result_cache[i]->infra_bindings.items[bi];
            if (ib->source_name && ib->target_url) {
                bindings += process_one_infra_binding(gbuf, ib, files[i].rel_path);
            }
        }
    }
    if (bindings > 0) {
        char buf[CBM_SZ_16];
        snprintf(buf, sizeof(buf), "%d", bindings);
        cbm_log_info("pass.infra_bindings", "linked", buf);
    }
}

static bool is_infra_file(const char *fp) {
    return fp != NULL &&
           (strstr(fp, ".yaml") != NULL || strstr(fp, ".yml") != NULL ||
            strstr(fp, ".tf") != NULL || strstr(fp, ".hcl") != NULL || strstr(fp, ".toml") != NULL);
}

/* CI/tooling configs describe the development TOOLCHAIN — their URLs are
 * repository/action/registry references, never endpoints this service
 * exposes. Minting infra Route nodes from them lets the route matcher's
 * root-service heuristic attach every handler of an ambiguous "/" route to
 * each tooling URL (junk HANDLES churn on plain pallets/flask, #999).
 * Deny by file identity, not URL shape: deployment configs (Cloud
 * Scheduler, compose) keep minting their genuine endpoints. */
static bool is_ci_tooling_config(const char *fp) {
    if (!fp) {
        return false;
    }
    if (strstr(fp, ".github/") != NULL || strstr(fp, ".gitlab/") != NULL ||
        strstr(fp, ".circleci/") != NULL) {
        return true;
    }
    const char *slash = strrchr(fp, '/');
    const char *base = slash ? slash + 1 : fp;
    static const char *const tooling[] = {".pre-commit-config.yaml",
                                          ".pre-commit-hooks.yaml",
                                          ".gitlab-ci.yml",
                                          ".travis.yml",
                                          "azure-pipelines.yml",
                                          "appveyor.yml",
                                          "bitbucket-pipelines.yml",
                                          ".readthedocs.yaml",
                                          ".readthedocs.yml",
                                          "codecov.yml",
                                          ".codecov.yml",
                                          ".goreleaser.yaml",
                                          ".goreleaser.yml",
                                          ".golangci.yml",
                                          ".golangci.yaml",
                                          NULL};
    for (int i = 0; tooling[i]; i++) {
        if (strcmp(base, tooling[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* True when a YAML key path denotes an UPSTREAM dependency, CONFIG value, or
 * HEALTHCHECK target rather than an endpoint this service exposes. Such URLs
 * (auth JWKS, downstream service base URLs, package-registry URLs, healthcheck
 * curl targets) are NOT routes the service serves and must not mint Route nodes
 * (#521). Exposed-endpoint keys (push_endpoint, post_url, callback, webhook)
 * are intentionally absent here so they still produce infra Route nodes. */
static bool is_upstream_config_key(const char *key_path) {
    if (!key_path) {
        /* No key context (e.g. flat string) — keep prior behaviour and mint. */
        return false;
    }
    static const char *const deny[] = {"jwks",     "registry",     "registries", "healthcheck",
                                       "upstream", "_service_url", "auth",       NULL};
    for (int i = 0; deny[i]; i++) {
        if (strstr(key_path, deny[i]) != NULL) {
            return true;
        }
    }
    return false;
}

/* Try to create an infra Route node from one string_ref. */
static void try_upsert_infra_route(cbm_gbuf_t *gbuf, const CBMStringRef *sr, const char *fp) {
    if (sr->kind != CBM_STRREF_URL || !sr->value || !strstr(sr->value, "://")) {
        return;
    }
    /* Skip upstream/config/healthcheck URLs — they are not exposed routes (#521). */
    if (is_upstream_config_key(sr->key_path)) {
        return;
    }
    char route_qn[CBM_ROUTE_QN_SIZE];
    snprintf(route_qn, sizeof(route_qn), "__route__infra__%s", sr->value);
    char route_props[CBM_SZ_512];
    if (sr->key_path) {
        snprintf(route_props, sizeof(route_props), "{\"source\":\"infra\",\"key_path\":\"%s\"}",
                 sr->key_path);
    } else {
        snprintf(route_props, sizeof(route_props), "{\"source\":\"infra\"}");
    }
    cbm_gbuf_upsert_node(gbuf, "Route", sr->value, route_qn, fp, 0, 0, route_props);
}

/* A URL string_ref that does NOT denote a route the service serves: a value
 * containing whitespace is a command/sentence with an embedded URL (e.g. a
 * Docker healthcheck `curl --fail http://... || exit 1`); a NULL key_path is a
 * context-less/duplicate ref; an upstream/config/healthcheck key is an external
 * dependency, not an exposed route. (#521) */
static bool route_sr_denied(const CBMStringRef *sr) {
    if (!sr->value || strchr(sr->value, ' ')) {
        return true;
    }
    if (!sr->key_path) {
        return true;
    }
    return is_upstream_config_key(sr->key_path);
}

static void cbm_pipeline_extract_infra_routes(cbm_gbuf_t *gbuf, const cbm_file_info_t *files,
                                              CBMFileResult **result_cache, int file_count) {
    /* DENY-WINS-BY-VALUE: the same URL is often extracted as several string_refs
     * at different key_path granularities (full path, leaf key, flat). The Route
     * node is keyed by VALUE, so it would be minted if ANY granularity passed the
     * per-ref guard — e.g. a denied full path `registries.terraform-registry.url`
     * is defeated by a sibling leaf `url`. So pass 1 collects every URL value
     * denied under ANY of its refs; pass 2 mints only values never denied. (#521) */
    CBMHashTable *denied = cbm_ht_create(16);
    for (int pass = 0; pass < 2; pass++) {
        for (int i = 0; i < file_count; i++) {
            if (!result_cache[i] || !is_infra_file(files[i].rel_path) ||
                is_ci_tooling_config(files[i].rel_path)) {
                continue;
            }
            for (int si = 0; si < result_cache[i]->string_refs.count; si++) {
                const CBMStringRef *sr = &result_cache[i]->string_refs.items[si];
                if (sr->kind != CBM_STRREF_URL || !sr->value || !strstr(sr->value, "://")) {
                    continue;
                }
                if (pass == 0) {
                    if (denied && route_sr_denied(sr)) {
                        cbm_ht_set(denied, sr->value, (void *)1);
                    }
                } else if (!denied || !cbm_ht_has(denied, sr->value)) {
                    try_upsert_infra_route(gbuf, sr, files[i].rel_path);
                }
            }
        }
    }
    cbm_ht_free(denied);
}

/* Run decorator_tags, configlink, and route matching passes. */
typedef void (*predump_pass_fn)(cbm_pipeline_ctx_t *);
static void predump_deco(cbm_pipeline_ctx_t *ctx) {
    cbm_pipeline_pass_decorator_tags(ctx->gbuf, ctx->project_name);
}
static void predump_route(cbm_pipeline_ctx_t *ctx) {
    cbm_pipeline_create_route_nodes(ctx->gbuf);
}
static void predump_sim(cbm_pipeline_ctx_t *ctx) {
    cbm_pipeline_pass_similarity(ctx);
}
static void predump_sem(cbm_pipeline_ctx_t *ctx) {
    cbm_pipeline_pass_semantic_edges(ctx);
}
static void predump_cfg(cbm_pipeline_ctx_t *ctx) {
    cbm_pipeline_pass_configlink(ctx);
}
static void predump_complexity(cbm_pipeline_ctx_t *ctx) {
    cbm_pipeline_pass_complexity(ctx);
}
static void run_predump_passes(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx) {
    static const struct {
        predump_pass_fn fn;
        const char *name;
        bool moderate_only; /* true = skip in fast mode */
    } passes[] = {
        {predump_deco, "decorator_tags", false}, {predump_cfg, "configlink", false},
        {predump_route, "route_match", false},   {predump_sim, "similarity", true},
        {predump_sem, "semantic_edges", true},   {predump_complexity, "complexity", false},
    };
    enum { PREDUMP_PASS_COUNT = 6 };
    struct timespec t;
    for (int i = 0; i < PREDUMP_PASS_COUNT && !check_cancel(p); i++) {
        /* "moderate_only" passes (similarity/semantic edges) run in FULL,
         * MODERATE and ADVANCED — they are skipped only in FAST. Compare
         * explicitly against FAST rather than `> MODERATE` so ADVANCED
         * (numerically 3) is not mistaken for a lighter mode than FULL. */
        if (passes[i].moderate_only && p->mode == CBM_MODE_FAST) {
            continue;
        }
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        passes[i].fn(ctx);
        cbm_log_info("pass.timing", "pass", passes[i].name, "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
    }
}

/* Adapter that lets cbm_pipeline_pass_lsp_cross slot into the seq_passes
 * dispatch table. The cross-file LSP needs the per-file CBMFileResult cache
 * to read defs/imports without re-extracting; in the sequential path that
 * cache is ctx->result_cache (set up by run_sequential_pipeline before
 * launching the dispatch loop). When the cache is unavailable (e.g. if the
 * pipeline opted out of caching), the pass becomes a no-op since there are
 * no extracted results to feed cross-file resolution. */
static int seq_pass_lsp_cross_dispatch(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                       int file_count) {
    if (!ctx || !ctx->result_cache)
        return 0;
    /* Cross-file LSP runs in every mode. */
    return cbm_pipeline_pass_lsp_cross(ctx, files, file_count, ctx->result_cache);
}

/* Run the sequential pipeline path: definitions, k8s, lsp_cross, calls, usages, semantic. */
/* Build the ObjectScript $$$macro table from .inc include files in the repo.
 * Returns NULL (and does no work) when no ObjectScript include files exist.
 * Caller owns the returned heap table (free via cbm_macro_table_free). */
CBMMacroTable *cbm_build_macro_table_from_files(const cbm_file_info_t *files, int count,
                                                const char *repo_path) {
    (void)repo_path;
    bool has_inc = false;
    for (int i = 0; i < count; i++) {
        if (files[i].language == CBM_LANG_OBJECTSCRIPT_ROUTINE && files[i].path &&
            (strrchr(files[i].path, '.') != NULL &&
             strcmp(strrchr(files[i].path, '.'), ".inc") == 0)) {
            has_inc = true;
            break;
        }
    }
    if (!has_inc) {
        return NULL;
    }

    CBMMacroTable *mt = (CBMMacroTable *)calloc(1, sizeof(CBMMacroTable));
    if (!mt) {
        return NULL;
    }

    cbm_arena_init(&mt->arena);
    cbm_macro_table_init_system(mt);

    for (int i = 0; i < count; i++) {
        if (files[i].language != CBM_LANG_OBJECTSCRIPT_ROUTINE) {
            continue;
        }
        if (!files[i].path || !(strrchr(files[i].path, '.') != NULL &&
                                strcmp(strrchr(files[i].path, '.'), ".inc") == 0)) {
            continue;
        }
        FILE *f = cbm_fopen(files[i].path, "rb");
        if (!f) {
            continue;
        }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        rewind(f);
        if (fsize > 0) {
            char *src = (char *)malloc((size_t)fsize + 1);
            if (src) {
                size_t nread = fread(src, 1, (size_t)fsize, f);
                src[nread] = '\0';
                cbm_parse_inc_file(mt, &mt->arena, src);
                free(src);
            }
        }
        (void)fclose(f);
    }
    return mt;
}

static int run_sequential_pipeline(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx,
                                   const cbm_file_info_t *files, int file_count,
                                   struct timespec *t) {
    cbm_log_info("pipeline.mode", "mode", "sequential", "files", itoa_buf(file_count));

    /* Build package map from manifest files (sequential: read manifests directly).
     * Use the repo-walking variant so manifests filtered out by the main
     * discoverer (package.json, composer.json) still feed pkgmap and let
     * workspace imports like `@my/pkg` resolve to their target Module. */
    cbm_pipeline_set_pkgmap(cbm_pkgmap_build_from_repo(ctx->repo_path, files, file_count,
                                                       ctx->project_name, ctx->excluded_dirs,
                                                       ctx->excluded_count));

    CBMFileResult **seq_cache = (CBMFileResult **)calloc(file_count, sizeof(CBMFileResult *));
    if (seq_cache) {
        ctx->result_cache = seq_cache;
    }

    /* ObjectScript: build the $$$macro table from .inc include files so that
     * pass_calls can resolve macro-mediated dispatch. NULL when not present. */
    CBMMacroTable *mt = cbm_build_macro_table_from_files(files, file_count, ctx->repo_path);
    if (mt) {
        ctx->macro_table = mt;
    }
    typedef int (*seq_pass_fn)(cbm_pipeline_ctx_t *, const cbm_file_info_t *, int);
    static const struct {
        seq_pass_fn fn;
        const char *name;
        bool ignore_err;
    } seq_passes[] = {
        {cbm_pipeline_pass_definitions, "definitions", false},
        {cbm_pipeline_pass_k8s, "k8s", true},
        {seq_pass_lsp_cross_dispatch, "lsp_cross", true},
        {cbm_pipeline_pass_calls, "calls", false},
        {cbm_pipeline_pass_usages, "usages", false},
        {cbm_pipeline_pass_semantic, "semantic", false},
    };
    int rc = 0;
    for (int si = 0; si < PL_SEQ_PASSES && rc == 0; si++) {
        cbm_clock_gettime(CLOCK_MONOTONIC, t);
        int pr = seq_passes[si].fn(ctx, files, file_count);
        if (pr != 0 && !seq_passes[si].ignore_err) {
            rc = pr;
        }
        cbm_log_info("pass.timing", "pass", seq_passes[si].name, "elapsed_ms",
                     itoa_buf((int)elapsed_ms(*t)));
        if (check_cancel(p)) {
            rc = CBM_NOT_FOUND;
        }
    }
    /* Consume infra bindings (YAML/HCL topic/queue/scheduler → endpoint) so
     * INFRA_MAPS edges also form on the sequential path, not just the parallel
     * one. process_one_infra_binding self-creates the topic Route node when no
     * code-side dispatch created it (e.g. a standalone scheduler manifest). */
    if (seq_cache && rc == 0) {
        cbm_pipeline_extract_infra_routes(p->gbuf, files, seq_cache, file_count);
        cbm_pipeline_process_infra_bindings(p->gbuf, files, seq_cache, file_count);
    }
    if (seq_cache) {
        for (int i = 0; i < file_count; i++) {
            if (seq_cache[i]) {
                cbm_free_result(seq_cache[i]);
            }
        }
        free(seq_cache);
        ctx->result_cache = NULL;
    }
    /* Release the lsp_cross pass's shared registries only now: resolved_calls
     * borrowed registry-owned strings that the calls pass read above. */
    if (ctx->seq_cross_arena_live) {
        cbm_arena_destroy(&ctx->seq_cross_arena);
        ctx->seq_cross_arena_live = false;
    }
    /* Destroy this thread's TLS parser: the sequential path parses on the
     * CALLING thread (usually main), and a parser left alive here was
     * allocated in the current tree-sitter allocator epoch. A later
     * parallel run switches the global ts allocator to the slab
     * (cbm_slab_install); destroying the stale parser then frees
     * mimalloc-epoch memory through slab_free -> plain free() and libmalloc
     * aborts — the #773 second-index SIGABRT. */
    cbm_destroy_thread_parser();
    /* ObjectScript: free the macro / return-type tables built for this run. */
    if (ctx->macro_table) {
        cbm_macro_table_free((CBMMacroTable *)ctx->macro_table);
        ctx->macro_table = NULL;
    }
    if (ctx->return_type_table) {
        for (int i = 0; i < ctx->return_type_table->count; i++) {
            free((void *)ctx->return_type_table->entries[i].return_type);
        }
        free((void *)ctx->return_type_table);
        ctx->return_type_table = NULL;
    }
    return rc;
}

/* Run the parallel pipeline path: extract, registry, resolve, infra, k8s. */
static int run_parallel_pipeline(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx,
                                 const cbm_file_info_t *files, int file_count, int worker_count,
                                 struct timespec *t) {
    cbm_log_info("pipeline.mode", "mode", "parallel", "workers", itoa_buf(worker_count), "files",
                 itoa_buf(file_count));
    _Atomic int64_t shared_ids;
    atomic_init(&shared_ids, cbm_gbuf_next_id(p->gbuf));
    CBMFileResult **cache = (CBMFileResult **)calloc(file_count, sizeof(CBMFileResult *));
    if (!cache) {
        cbm_log_error("pipeline.err", "phase", "cache_alloc");
        return CBM_NOT_FOUND;
    }
    cbm_clock_gettime(CLOCK_MONOTONIC, t);
    int rc = cbm_parallel_extract(ctx, files, file_count, cache, &shared_ids, worker_count);
    cbm_log_info("pass.timing", "pass", "parallel_extract", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(*t)));
    if (rc != 0 || check_cancel(p)) {
        for (int i = 0; i < file_count; i++) {
            cbm_free_result(cache[i]);
        }
        free(cache);
        return rc != 0 ? rc : CBM_NOT_FOUND;
    }
    cbm_gbuf_set_next_id(p->gbuf, atomic_load(&shared_ids));
    /* extract -> registry handoff: return the extract phase's freed-but-retained
     * allocator pages to the OS before registry_build allocates. On a 2x Linux
     * index the extract peak holds ~13 GB of reclaimable pages (peak_mb 20.7 vs
     * live rss_mb 7); not returning them pushed the process over the system
     * memory-pressure threshold and got it SIGKILLed at registry entry. */
    cbm_mem_collect();
    cbm_log_info("mem.collect", "phase", "post_extract", "rss_mb",
                 itoa_buf((int)(cbm_mem_rss() / (1024 * 1024))));
    cbm_clock_gettime(CLOCK_MONOTONIC, t);
    rc = cbm_build_registry_from_cache(ctx, files, file_count, cache);
    cbm_log_info("pass.timing", "pass", "registry_build", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(*t)));
    log_phase_mem("registry_build");
    if (rc != 0 || check_cancel(p)) {
        for (int i = 0; i < file_count; i++) {
            if (cache[i]) {
                cbm_free_result(cache[i]);
            }
        }
        free(cache);
        return rc != 0 ? rc : CBM_NOT_FOUND;
    }
    /* Registry consumers may materialize serial nodes (Channel, EnvVar, and
     * future carrier-derived resources) after parallel extraction established
     * the shared allocator watermark. Advance the atomic allocator before
     * resolve workers resume; otherwise their IDs and the later next-id reset
     * can collide with those nodes and orphan freshly inserted edges. */
    int64_t registry_next_id = cbm_gbuf_next_id(p->gbuf);
    if (registry_next_id > atomic_load(&shared_ids)) {
        atomic_store(&shared_ids, registry_next_id);
    }
    /* Cross-file LSP precondition: build a project-wide CBMLSPDef[]
     * once. The fused resolve_worker invokes cbm_pxc_run_one(_ts) per
     * file using these defs + the file's IMPORTS map, so cross-file
     * type-resolved CALLS land in result->resolved_calls before the
     * CALLS-edge emission. This replaces the old sequential
     * cbm_pipeline_pass_lsp_cross pass which re-read every source from
     * disk and re-parsed every tree on a single thread (~520s on
     * kubernetes). Soft-failure: NULL all_defs / NULL def_modules just
     * mean cross-file LSP no-ops; per-file LSP already ran during
     * extract. */
    cbm_clock_gettime(CLOCK_MONOTONIC, t);
    /* Cross-file LSP (type-aware call/usage resolution across files) — the
     * most expensive phase. CBM_DISABLE_LSP_CROSS=1 opts out (it can SIGSEGV
     * on large TS projects — see #340/#344); with cross-LSP off, all_defs
     * stays NULL and the fused resolver simply no-ops cross-file resolution
     * (per-file LSP already ran during extract). */
    char cbm_lsp_cross_env[CBM_SZ_16];
    const bool run_cross_lsp = cbm_safe_getenv("CBM_DISABLE_LSP_CROSS", cbm_lsp_cross_env,
                                               sizeof(cbm_lsp_cross_env), NULL) == NULL;
    if (!run_cross_lsp) {
        cbm_log_info("lsp_cross.skipped", "reason", "CBM_DISABLE_LSP_CROSS env set");
    }
    char **def_modules = NULL;
    int def_count = 0;
    CBMLSPDef *all_defs = NULL;
    if (run_cross_lsp) {
        def_modules = (char **)calloc((size_t)file_count, sizeof(char *));
        all_defs = def_modules
                       ? cbm_pxc_collect_all_defs(cache, files, file_count, ctx->project_name,
                                                  def_modules, &def_count)
                       : NULL;
    }
    /* Build inverted index: module_qn → defs. The fused resolve_worker
     * uses this to filter the global all_defs[] down to just the defs
     * each file actually needs (own_module + imported modules) — the
     * gopls "package summary" pattern. Drops per-file registry build
     * cost from O(all_defs) to O(relevant_defs), typically 50-100×
     * smaller per file. */
    CBMModuleDefIndex *module_def_index =
        all_defs ? cbm_pxc_build_module_def_index(all_defs, def_count) : NULL;
    /* Tier 2 full: pre-build per-language cross-LSP registries.
     * Built ONCE here; shared READ-ONLY across all files of that language
     * during resolve. Per-file work is then: parse + AST walk + O(1) lookups
     * — no registry build, no Phase 1b mutations. Languages added so far:
     * Go, Python. Others (C/C++, TS/JS, PHP, C#) fall back to per-file. */
    CBMArena cross_lsp_arena;
    cbm_arena_init(&cross_lsp_arena);
    CBMCrossLspRegistries cross_registries = {0};
    if (all_defs) {
        cross_registries.go = cbm_go_build_cross_registry(&cross_lsp_arena, all_defs, def_count);
        cross_registries.python =
            cbm_py_build_cross_registry(&cross_lsp_arena, all_defs, def_count);
        cross_registries.c = cbm_c_build_cross_registry(&cross_lsp_arena, all_defs, def_count);
        cross_registries.cs = cbm_cs_build_cross_registry(&cross_lsp_arena, all_defs, def_count);
        cross_registries.ts = cbm_ts_build_cross_registry(&cross_lsp_arena, all_defs, def_count);
        /* Rust: NOT built here. The shared all_defs registry is built LAZILY on the
         * first NULL-filter rust file (the amplifier files) inside cbm_parallel_resolve
         * — repos whose rust files all filter to subsets never pay the build/RSS. */
    }
    cbm_log_info("pass.timing", "pass", "lsp_cross_prepare", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(*t)));
    log_phase_mem("lsp_cross_prepare");
    cbm_clock_gettime(CLOCK_MONOTONIC, t);
    rc = cbm_parallel_resolve(ctx, files, file_count, cache, &shared_ids, worker_count, all_defs,
                              def_count, def_modules, module_def_index, &cross_registries);
    cbm_log_info("pass.timing", "pass", "parallel_resolve", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(*t)));
    log_phase_mem("parallel_resolve");
    cbm_pxc_free_module_def_index(module_def_index);
    cbm_arena_destroy(&cross_lsp_arena); /* releases all per-lang registries */
    free(all_defs);
    if (def_modules) {
        for (int i = 0; i < file_count; i++) {
            free(def_modules[i]);
        }
        free(def_modules);
    }
    cbm_gbuf_set_next_id(p->gbuf, atomic_load(&shared_ids));
    cbm_pipeline_extract_infra_routes(p->gbuf, files, cache, file_count);
    cbm_pipeline_process_infra_bindings(p->gbuf, files, cache, file_count);
    for (int i = 0; i < file_count; i++) {
        if (cache[i]) {
            cbm_free_result(cache[i]);
        }
    }
    free(cache);
    if (rc != 0) {
        return rc;
    }
    cbm_clock_gettime(CLOCK_MONOTONIC, t);
    cbm_pipeline_pass_k8s(ctx, files, file_count);
    cbm_log_info("pass.timing", "pass", "k8s", "elapsed_ms", itoa_buf((int)elapsed_ms(*t)));
    return check_cancel(p) ? CBM_NOT_FOUND : 0;
}

static int capture_existing_adr(cbm_pipeline_t *p, const char *db_path) {
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    if (atomic_exchange(&g_persist_test_fail_adr_capture, false)) {
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }
#endif
    cbm_store_t *adr_store = cbm_store_open_path_query(db_path);
    if (!adr_store) {
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }
    cbm_adr_t existing = {0};
    int adr_rc = cbm_store_adr_get(adr_store, p->project_name, &existing);
    if (adr_rc == CBM_STORE_NOT_FOUND) {
        cbm_store_close(adr_store);
        free(p->saved_adr);
        p->saved_adr = NULL;
        return 0;
    }
    if (adr_rc != CBM_STORE_OK || !existing.content) {
        cbm_store_adr_free(&existing);
        cbm_store_close(adr_store);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }
    char *saved = strdup(existing.content);
    cbm_store_adr_free(&existing);
    cbm_store_close(adr_store);
    if (!saved) {
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }
    free(p->saved_adr);
    p->saved_adr = saved;
    return 0;
}

/* Route an existing generation. Full rebuilds never delete the live DB here:
 * publication owns the eventual atomic replacement after every pass and
 * metadata write has succeeded. */
static int try_incremental_or_delete_db(cbm_pipeline_t *p, cbm_file_info_t *files, int file_count,
                                        const cbm_file_hash_t *baseline_manifest,
                                        int baseline_count) {
    char *db_path = resolve_db_path(p);
    if (!db_path) {
        return CBM_PIPELINE_FORCE_FULL_REINDEX;
    }
    struct stat db_st;
    if (stat(db_path, &db_st) != 0) {
        free(db_path);
        return CBM_PIPELINE_FORCE_FULL_REINDEX;
    }
    cbm_store_t *check_store = cbm_store_open_path_query(db_path);
    bool valid = check_store && cbm_store_check_integrity(check_store);
    if (check_store) {
        cbm_store_close(check_store);
    }
    if (!valid) {
        cbm_log_warn("pipeline.route", "path", "full", "reason", "invalid_existing_db");
        free(db_path);
        return CBM_PIPELINE_FORCE_FULL_REINDEX;
    }
    cbm_log_info("pipeline.route", "path", "incremental_manifest");
    int rc = cbm_pipeline_run_incremental(p, db_path, files, file_count, baseline_manifest,
                                          baseline_count);
    if (rc == CBM_PIPELINE_FORCE_FULL_REINDEX) {
        int adr_rc = capture_existing_adr(p, db_path);
        if (adr_rc != 0) {
            rc = adr_rc;
        }
    }
    free(db_path);
    return rc;
}

static const char *pipeline_mode_name(cbm_index_mode_t mode) {
    switch (mode) {
    case CBM_MODE_FULL:
        return "full";
    case CBM_MODE_MODERATE:
        return "moderate";
    case CBM_MODE_FAST:
        return "fast";
    default:
        return "unknown";
    }
}

static int pipeline_mode_coverage_rank(cbm_index_mode_t mode) {
    switch (mode) {
    case CBM_MODE_FULL:
        return 3;
    case CBM_MODE_MODERATE:
        return 2;
    case CBM_MODE_FAST:
        return 1;
    default:
        return 0;
    }
}

/* Index modes are additive: a cheaper run may refresh a fuller graph, but it
 * must never erase files that the cheaper discovery intentionally skips. The
 * exact-manifest pipeline therefore keeps the most comprehensive successfully
 * published mode and performs any changed rebuild at that coverage level. */
static void promote_mode_to_existing_coverage(cbm_pipeline_t *p) {
    if (!p || !p->project_name) {
        return;
    }
    char *db_path = resolve_db_path(p);
    if (!db_path) {
        return;
    }
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    free(db_path);
    if (!store) {
        return;
    }
    cbm_coverage_meta_t meta = {0};
    if (cbm_store_coverage_meta_get(store, p->project_name, &meta) == CBM_STORE_OK &&
        meta.index_mode) {
        cbm_index_mode_t stored_mode = p->mode;
        if (strcmp(meta.index_mode, "full") == 0) {
            stored_mode = CBM_MODE_FULL;
        } else if (strcmp(meta.index_mode, "moderate") == 0) {
            stored_mode = CBM_MODE_MODERATE;
        } else if (strcmp(meta.index_mode, "fast") == 0) {
            stored_mode = CBM_MODE_FAST;
        }
        if (pipeline_mode_coverage_rank(stored_mode) > pipeline_mode_coverage_rank(p->mode)) {
            cbm_log_info("pipeline.mode", "requested", pipeline_mode_name(p->mode), "effective",
                         pipeline_mode_name(stored_mode), "reason", "preserve_existing_coverage");
            p->mode = stored_mode;
        }
    }
    cbm_store_coverage_meta_clear(&meta);
    cbm_store_close(store);
}

int cbm_pipeline_refresh_artifact(cbm_pipeline_t *p, const char *db_path) {
    if (!p || !db_path || !p->repo_path || !p->project_name) {
        return 0;
    }
    bool existing = cbm_artifact_exists(p->repo_path);
    if (!p->persistence && !existing) {
        return 0;
    }
    int quality = p->persistence ? CBM_ARTIFACT_BEST : CBM_ARTIFACT_FAST;
    int rc = cbm_artifact_export(db_path, p->repo_path, p->project_name, quality);
    if (rc != 0) {
        const char *err = cbm_artifact_export_last_error();
        if (p->persistence) {
            cbm_log_error("pipeline.err", "phase", "artifact_export", "err", err ? err : "unknown");
            return rc;
        }
        cbm_log_warn("artifact.refresh_failed", "err", err ? err : "unknown");
    }
    return 0;
}

static atomic_ullong g_generation_stage_counter = 0;

static void discard_generation_stage(const char *stage_path) {
    if (!stage_path) {
        return;
    }
    cbm_unlink(stage_path);
    cbm_remove_db_sidecars(stage_path);
}

static int generation_rebuild_fts(cbm_store_t *store) {
    if (cbm_store_exec(store, "INSERT INTO nodes_fts(nodes_fts) VALUES('delete-all');") !=
        CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    if (cbm_store_exec(store,
                       "INSERT INTO nodes_fts(rowid, name, qualified_name, label, file_path) "
                       "SELECT id, cbm_camel_split(name), qualified_name, label, file_path "
                       "FROM nodes;") == CBM_STORE_OK) {
        return CBM_STORE_OK;
    }
    return cbm_store_exec(store,
                          "INSERT INTO nodes_fts(rowid, name, qualified_name, label, file_path) "
                          "SELECT id, name, qualified_name, label, file_path FROM nodes;");
}

typedef struct {
    bool quarantined;
    char backup_path[CBM_SZ_4K];
} cbm_replacement_prepare_t;

static int replacement_sidecar_path(char *out, size_t out_size, const char *base,
                                    const char *suffix) {
    int n = snprintf(out, out_size, "%s%s", base, suffix);
    return n > 0 && (size_t)n < out_size ? 0 : CBM_PIPELINE_PERSIST_FAILED;
}

static bool replacement_path_exists(const char *path) {
    cbm_path_info_t info;
    return cbm_path_info_utf8(path, &info) == 0;
}

static int rollback_quarantined_generation(const char *db_path,
                                           cbm_replacement_prepare_t *prepared) {
    if (!prepared || !prepared->quarantined) {
        return 0;
    }
    static const char *const suffixes[] = {"-wal", "-shm"};
    if (cbm_rename_noreplace(prepared->backup_path, db_path) != 0) {
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    for (size_t i = 0; i < sizeof(suffixes) / sizeof(suffixes[0]); i++) {
        char source[CBM_SZ_4K];
        char destination[CBM_SZ_4K];
        if (replacement_sidecar_path(source, sizeof(source), prepared->backup_path, suffixes[i]) !=
                0 ||
            replacement_sidecar_path(destination, sizeof(destination), db_path, suffixes[i]) != 0) {
            return CBM_PIPELINE_PERSIST_FAILED;
        }
        if (replacement_path_exists(source) && cbm_rename_noreplace(source, destination) != 0) {
            return CBM_PIPELINE_PERSIST_FAILED;
        }
    }
    prepared->quarantined = false;
    prepared->backup_path[0] = '\0';
    return 0;
}

static int quarantine_existing_generation(const char *db_path,
                                          cbm_replacement_prepare_t *prepared) {
    if (!db_path || !prepared) {
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    static const char *const suffixes[] = {"-wal", "-shm"};
    char candidate[CBM_SZ_4K];
    for (int attempt = 0; attempt < 10000; attempt++) {
        int n = attempt == 0
                    ? snprintf(candidate, sizeof(candidate), "%s.corrupt", db_path)
                    : snprintf(candidate, sizeof(candidate), "%s.corrupt.%d", db_path, attempt);
        if (n <= 0 || (size_t)n >= sizeof(candidate)) {
            return CBM_PIPELINE_PERSIST_FAILED;
        }
        bool available = !replacement_path_exists(candidate);
        for (size_t i = 0; available && i < sizeof(suffixes) / sizeof(suffixes[0]); i++) {
            char candidate_sidecar[CBM_SZ_4K];
            if (replacement_sidecar_path(candidate_sidecar, sizeof(candidate_sidecar), candidate,
                                         suffixes[i]) != 0) {
                return CBM_PIPELINE_PERSIST_FAILED;
            }
            available = !replacement_path_exists(candidate_sidecar);
        }
        if (!available) {
            continue;
        }
        if (cbm_rename_noreplace(db_path, candidate) != 0) {
            if (replacement_path_exists(candidate)) {
                continue;
            }
            return CBM_PIPELINE_PERSIST_FAILED;
        }

        snprintf(prepared->backup_path, sizeof(prepared->backup_path), "%s", candidate);
        prepared->quarantined = true;
        for (size_t i = 0; i < sizeof(suffixes) / sizeof(suffixes[0]); i++) {
            char source[CBM_SZ_4K];
            char destination[CBM_SZ_4K];
            if (replacement_sidecar_path(source, sizeof(source), db_path, suffixes[i]) != 0 ||
                replacement_sidecar_path(destination, sizeof(destination), candidate,
                                         suffixes[i]) != 0) {
                (void)rollback_quarantined_generation(db_path, prepared);
                return CBM_PIPELINE_PERSIST_FAILED;
            }
            if (replacement_path_exists(source) && cbm_rename_noreplace(source, destination) != 0) {
                (void)rollback_quarantined_generation(db_path, prepared);
                return CBM_PIPELINE_PERSIST_FAILED;
            }
        }
        return 0;
    }
    return CBM_PIPELINE_PERSIST_FAILED;
}

static int prepare_existing_generation_for_replace(const char *db_path,
                                                   cbm_replacement_prepare_t *prepared) {
    if (!prepared) {
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    memset(prepared, 0, sizeof(*prepared));
    cbm_path_info_t info;
    if (cbm_path_info_utf8(db_path, &info) == 0) {
        if (!info.is_regular || info.is_symlink) {
            return CBM_PIPELINE_PERSIST_FAILED;
        }
        int seal_rc = cbm_store_seal_existing_path_for_replace(db_path);
        if (seal_rc == CBM_STORE_NOT_FOUND) {
            return quarantine_existing_generation(db_path, prepared);
        }
        if (seal_rc != CBM_STORE_OK) {
            return CBM_PIPELINE_PERSIST_FAILED;
        }
    }
    return cbm_remove_db_sidecars_checked(db_path) == 0 ? 0 : CBM_PIPELINE_PERSIST_FAILED;
}

int cbm_pipeline_publish_generation(const cbm_pipeline_generation_t *generation) {
    if (!generation || !generation->gbuf || !generation->final_db_path || !generation->project ||
        generation->manifest_count < 0 ||
        (generation->manifest_count > 0 && !generation->manifest) ||
        generation->coverage_count < 0 ||
        (generation->coverage_count > 0 && !generation->coverage)) {
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    if (generation->cancelled && atomic_load(generation->cancelled)) {
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    size_t stage_size = strlen(generation->final_db_path) + 96;
    char *stage_path = malloc(stage_size);
    if (!stage_path) {
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    unsigned long long serial = atomic_fetch_add(&g_generation_stage_counter, 1) + 1;
    int path_n = snprintf(stage_path, stage_size, "%s.stage.%ld.%llu", generation->final_db_path,
                          (long)cbm_pipeline_getpid(), serial);
    if (path_n < 0 || (size_t)path_n >= stage_size) {
        free(stage_path);
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    discard_generation_stage(stage_path);

    int dump_rc = cbm_gbuf_dump_to_sqlite(generation->gbuf, stage_path);
    if (dump_rc != 0) {
        discard_generation_stage(stage_path);
        free(stage_path);
        return CBM_PIPELINE_PERSIST_FAILED;
    }
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    if (cbm_pipeline_persist_test_take_failure_after_stage_dump()) {
        discard_generation_stage(stage_path);
        free(stage_path);
        return CBM_PIPELINE_PERSIST_FAILED;
    }
#endif
    if (generation->cancelled && atomic_load(generation->cancelled)) {
        discard_generation_stage(stage_path);
        free(stage_path);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    cbm_store_t *store = cbm_store_open_path(stage_path);
    if (!store) {
        discard_generation_stage(stage_path);
        free(stage_path);
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    bool ok = cbm_store_exec(store, "PRAGMA synchronous=FULL;") == CBM_STORE_OK;
    ok = ok && cbm_store_delete_file_hashes(store, generation->project) == CBM_STORE_OK &&
         cbm_store_upsert_file_hash_batch(store, generation->manifest,
                                          generation->manifest_count) == CBM_STORE_OK;
    if (ok && generation->adr_content) {
        ok = cbm_store_adr_store(store, generation->project, generation->adr_content) ==
             CBM_STORE_OK;
    }

    cbm_project_t project_info = {0};
    bool have_project_info =
        cbm_store_get_project(store, generation->project, &project_info) == CBM_STORE_OK;
    cbm_coverage_meta_t meta = generation->coverage_meta;
    meta.generation = have_project_info ? project_info.indexed_at : NULL;
    meta.coverage_version = CBM_SEMANTIC_INDEX_VERSION;
    meta.hash_records_complete = true;
    if (!have_project_info ||
        cbm_store_coverage_replace_ex(store, generation->project, generation->coverage,
                                      generation->coverage_count, &meta) != CBM_STORE_OK) {
        ok = false;
    }
    if (have_project_info) {
        cbm_project_free_fields(&project_info);
    }
    if (generation_rebuild_fts(store) != CBM_STORE_OK || !cbm_store_check_integrity(store)) {
        ok = false;
    }
    if (ok && cbm_store_seal_for_atomic_publish(store) != CBM_STORE_OK) {
        ok = false;
    }
    cbm_store_close(store);
    if (ok && cbm_remove_db_sidecars_checked(stage_path) != 0) {
        ok = false;
    }
    if (!ok) {
        discard_generation_stage(stage_path);
        free(stage_path);
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    if (generation->cancelled && atomic_load(generation->cancelled)) {
        discard_generation_stage(stage_path);
        free(stage_path);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }
    cbm_replacement_prepare_t prepared = {0};
    if (prepare_existing_generation_for_replace(generation->final_db_path, &prepared) != 0) {
        discard_generation_stage(stage_path);
        free(stage_path);
        return CBM_PIPELINE_PERSIST_FAILED;
    }
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    if (cbm_pipeline_persist_test_take_cancel_after_destination_prepare() &&
        generation->cancelled) {
        atomic_store(generation->cancelled, true);
    }
#endif
    if (generation->cancelled && atomic_load(generation->cancelled)) {
        int rollback_rc = rollback_quarantined_generation(generation->final_db_path, &prepared);
        discard_generation_stage(stage_path);
        free(stage_path);
        return rollback_rc == 0 ? CBM_PIPELINE_ABORT_PRESERVE_DB : CBM_PIPELINE_PERSIST_FAILED;
    }
    if (cbm_rename_replace(stage_path, generation->final_db_path) != 0) {
        (void)rollback_quarantined_generation(generation->final_db_path, &prepared);
        discard_generation_stage(stage_path);
        free(stage_path);
        return CBM_PIPELINE_PERSIST_FAILED;
    }
    free(stage_path);
    return 0;
}

/* Dump graph to SQLite and persist file hashes for incremental indexing. */
static int dump_and_persist_hashes(cbm_pipeline_t *p, const cbm_file_hash_t *baseline_manifest,
                                   int baseline_count, struct timespec *t) {
    cbm_clock_gettime(CLOCK_MONOTONIC, t);
    char db_path[CBM_SZ_1K];
    if (p->db_path) {
        snprintf(db_path, sizeof(db_path), "%s", p->db_path);
    } else {
        const char *cdir = cbm_resolve_cache_dir();
        if (!cdir) {
            cdir = cbm_tmpdir();
        }
        snprintf(db_path, sizeof(db_path), "%s/%s.db", cdir, p->project_name);
    }
    char db_dir[CBM_SZ_1K];
    snprintf(db_dir, sizeof(db_dir), "%s", db_path);
    char *last_slash = strrchr(db_dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        cbm_mkdir_p(db_dir, CBM_DIR_PERMS);
    }

    cbm_file_hash_t *manifest = NULL;
    int manifest_count = 0;
#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    cbm_pipeline_persist_test_run_before_final_manifest();
#endif
    if (cbm_pipeline_build_fresh_semantic_manifest(p->project_name, p->repo_path, p->mode,
                                                   &manifest, &manifest_count) != 0) {
        cbm_log_error("pipeline.err", "phase", "semantic_manifest");
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }
    if (!cbm_pipeline_semantic_manifests_equal(baseline_manifest, baseline_count, manifest,
                                               manifest_count)) {
        cbm_log_warn("pipeline.abort", "reason", "semantic_inputs_changed");
        cbm_pipeline_free_semantic_manifest(manifest, manifest_count);
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    int cov_total = p->file_errors_count + p->excluded_count + p->ignored_count;
    cbm_coverage_row_t *cov = NULL;
    int cov_count = 0;
    bool coverage_rows_available = cov_total == 0;
    if (cov_total > 0) {
        cov = malloc((size_t)cov_total * sizeof(*cov));
        if (cov) {
            coverage_rows_available = true;
            for (int i = 0; i < p->file_errors_count; i++) {
                cov[cov_count++] = (cbm_coverage_row_t){.rel_path = p->file_errors[i].path,
                                                        .kind = p->file_errors[i].phase,
                                                        .detail = p->file_errors[i].reason};
            }
            for (int i = 0; i < p->excluded_count; i++) {
                cov[cov_count++] = (cbm_coverage_row_t){.rel_path = p->excluded_dirs[i],
                                                        .kind = "not_indexed_dir",
                                                        .detail = "excluded subtree"};
            }
            for (int i = 0; i < p->ignored_count; i++) {
                cov[cov_count++] = (cbm_coverage_row_t){.rel_path = p->ignored_files[i].rel_path,
                                                        .kind = "not_indexed_file",
                                                        .detail = p->ignored_files[i].reason};
            }
        }
    }
    cbm_pipeline_generation_t generation = {
        .gbuf = p->gbuf,
        .final_db_path = db_path,
        .project = p->project_name,
        .cancelled = &p->cancelled,
        .manifest = manifest,
        .manifest_count = manifest_count,
        .adr_content = p->saved_adr,
        .coverage = cov,
        .coverage_count = cov_count,
        .coverage_meta =
            {
                .index_mode = pipeline_mode_name(p->mode),
                .recording_status =
                    !coverage_rows_available
                        ? "unavailable"
                        : (p->ignored_total > p->ignored_count ? "truncated" : "complete"),
                .ignored_files_stored = p->ignored_count,
                .ignored_files_total = p->ignored_total,
                .coverage_version = CBM_SEMANTIC_INDEX_VERSION,
                .hash_records_complete = true,
            },
    };

    /* Capture counts before the dump releases graph-buffer indexes. */
    p->committed_nodes = cbm_gbuf_node_count(p->gbuf);
    p->committed_edges = cbm_gbuf_edge_count(p->gbuf);
    int rc = cbm_pipeline_publish_generation(&generation);
    free(cov);
    cbm_pipeline_free_semantic_manifest(manifest, manifest_count);
    if (rc != 0) {
        return rc;
    }
    cbm_log_info("pass.timing", "pass", "dump_and_persist", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(*t)), "files", itoa_buf(manifest_count));
    if (p->ignored_total > p->ignored_count) {
        cbm_log_warn("index.ignored_capped", "stored", itoa_buf(p->ignored_count), "total",
                     itoa_buf(p->ignored_total));
    }
    free(p->saved_adr);
    p->saved_adr = NULL;

    /* The SQLite generation is the commit point. Automatic refresh of an
     * existing artifact is best-effort, but an explicitly requested artifact
     * is caller-visible and must report an export failure. */
    return cbm_pipeline_refresh_artifact(p, db_path);
}

/* Run githistory pass. */
static int run_githistory(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx) {
    struct timespec t_gh;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t_gh);

    cbm_githistory_result_t gh_result = {0};
    cbm_thread_t gh_thread;
    bool gh_threaded = false;
    gh_compute_arg_t gh_arg = {.repo_path = ctx->repo_path, .result = &gh_result};

    if (p->mode != CBM_MODE_FAST) {
        if (effective_worker_count(true) > SKIP_ONE) {
            if (cbm_thread_create(&gh_thread, 0, gh_compute_thread_fn, &gh_arg) == 0) {
                gh_threaded = true;
            }
        }
        if (!gh_threaded) {
            cbm_pipeline_githistory_compute(ctx->repo_path, &gh_result);
            cbm_log_info("pass.timing", "pass", "githistory_compute", "elapsed_ms",
                         itoa_buf((int)elapsed_ms(t_gh)));
        }
    } else {
        cbm_log_info("pass.skip", "pass", "githistory", "reason", "fast_mode");
    }

    if (gh_threaded) {
        cbm_thread_join(&gh_thread);
        cbm_log_info("pass.timing", "pass", "githistory_compute", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t_gh)));
    }

    int gh_edges = 0;
    if (gh_result.count > 0 || gh_result.file_temporal_count > 0) {
        gh_edges = cbm_pipeline_githistory_apply(ctx, &gh_result);
    }
    cbm_log_info("pass.done", "pass", "githistory", "commits", itoa_buf(gh_result.commit_count),
                 "edges", itoa_buf(gh_edges));
    free(gh_result.couplings);
    free(gh_result.file_temporal);
    return 0;
}

/* ── Pipeline run ────────────────────────────────────────────────── */

/* Run tests + git history. Returns 0 on success. */
static int run_tests_and_history(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx,
                                 const cbm_file_info_t *files, int file_count) {
    struct timespec t;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    CBM_PROF_START(t_tests);
    int rc = cbm_pipeline_pass_tests(ctx, files, file_count);
    CBM_PROF_END_N("pipeline", "pass_tests", t_tests, file_count);
    cbm_log_info("pass.timing", "pass", "tests", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
    if (rc == 0 && !check_cancel(p)) {
        CBM_PROF_START(t_gh);
        rc = run_githistory(p, ctx);
        CBM_PROF_END("pipeline", "pass_githistory", t_gh);
    }
    if (check_cancel(p)) {
        return CBM_NOT_FOUND;
    }
    return rc;
}

/* Run tests, git history, predump passes, and dump+persist. */
static int run_post_extraction(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx,
                               const cbm_file_info_t *files, int file_count,
                               const cbm_file_hash_t *baseline_manifest, int baseline_count) {
    int rc = run_tests_and_history(p, ctx, files, file_count);
    if (rc != 0) {
        return rc;
    }

    CBM_PROF_START(t_predump);
    run_predump_passes(p, ctx);
    CBM_PROF_END("pipeline", "3_predump_passes_total", t_predump);

#if defined(CBM_INCREMENTAL_TEST_API) && CBM_INCREMENTAL_TEST_API
    if (cbm_pipeline_persist_test_take_cancel_after_predump()) {
        atomic_store(&p->cancelled, 1);
    }
#endif

    if (check_cancel(p)) {
        return CBM_PIPELINE_ABORT_PRESERVE_DB;
    }

    struct timespec t;
    CBM_PROF_START(t_dump);
    rc = dump_and_persist_hashes(p, baseline_manifest, baseline_count, &t);
    CBM_PROF_END("pipeline", "4_dump_and_persist", t_dump);
    return rc;
}

#define MIN_FILES_FOR_PARALLEL 50

/* Run structure + extraction passes (parallel or sequential). */
static int run_extraction_phase(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx,
                                const cbm_file_info_t *files, int file_count) {
    struct timespec t;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    CBM_PROF_START(t_struct);
    pass_structure(p, files, file_count);
    CBM_PROF_END_N("pipeline", "pass_structure", t_struct, file_count);
    cbm_log_info("pass.timing", "pass", "structure", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
    if (check_cancel(p)) {
        return CBM_NOT_FOUND;
    }

    int worker_count = effective_worker_count(true);
    CBM_PROF_START(t_extract_total);
    int rc = (worker_count > SKIP_ONE && file_count > MIN_FILES_FOR_PARALLEL)
                 ? run_parallel_pipeline(p, ctx, files, file_count, worker_count, &t)
                 : run_sequential_pipeline(p, ctx, files, file_count, &t);
    CBM_PROF_END_N("pipeline", "2_extraction_total", t_extract_total, file_count);
    if (check_cancel(p)) {
        return CBM_NOT_FOUND;
    }
    return rc;
}

int cbm_pipeline_run(cbm_pipeline_t *p) {
    if (!p) {
        return CBM_NOT_FOUND;
    }

    CBM_PROF_START(t_pipeline_total);
    struct timespec t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
    cbm_path_alias_collection_t *path_aliases = NULL;
    cbm_file_hash_t *baseline_manifest = NULL;
    int baseline_count = 0;

    promote_mode_to_existing_coverage(p);

    /* cbm_pipeline_new() may precede the actual run by an arbitrary interval.
     * Refresh once here, then use this exact snapshot for both Branch graph
     * construction and the baseline semantic manifest. */
    if (pipeline_refresh_git_context(p) != 0) {
        return CBM_NOT_FOUND;
    }

    /* C/C++ #define Macro nodes (#375) dominate extraction on macro-dense repos
     * (≈49% of nodes on the Linux kernel), so gate them to full mode — moderate
     * and fast skip them entirely. Set before any extraction dispatch. */
    cbm_set_macro_extraction(p->mode == CBM_MODE_FULL);

    /* Load user-defined extension overrides (fail-open: NULL on error) */
    CBM_PROF_START(t_userconfig);
    p->userconfig = cbm_userconfig_load(p->repo_path);
    cbm_set_user_lang_config(p->userconfig);
    CBM_PROF_END("pipeline", "0_userconfig_load", t_userconfig);

    /* Phase 1: Discover files */
    CBM_PROF_START(t_discover);
    cbm_discover_opts_t opts = {
        .mode = p->mode,
        .ignore_file = NULL,
        .max_file_size = 0,
    };
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    /* Capture skipped subtrees on the pipeline so the MCP layer can report
     * which directories were excluded (#411), plus the individually-ignored
     * files (#963 "purposely not indexed"). Replace any prior lists (e.g. a
     * re-run on the same pipeline) to avoid leaking the previous ones. */
    cbm_discover_free_excluded(p->excluded_dirs, p->excluded_count);
    p->excluded_dirs = NULL;
    p->excluded_count = 0;
    cbm_discover_free_ignored(p->ignored_files, p->ignored_count);
    p->ignored_files = NULL;
    p->ignored_count = 0;
    p->ignored_total = 0;
    int rc = cbm_discover_ex2(p->repo_path, &opts, &files, &file_count, &p->excluded_dirs,
                              &p->excluded_count, &p->ignored_files, &p->ignored_count,
                              &p->ignored_total);
    if (rc != 0) {
        cbm_log_error("pipeline.err", "phase", "discover", "rc", itoa_buf(rc));
    }
    CBM_PROF_END_N("pipeline", "1_discover", t_discover, file_count);
    cbm_log_info("pipeline.discover", "files", itoa_buf(file_count), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t0)));
    if (rc != 0 || check_cancel(p)) {
        rc = CBM_NOT_FOUND;
        goto cleanup;
    }

    /* Snapshot every semantic input once before routing/extraction. The same
     * bytes drive exact no-op comparison and are checked against a fresh
     * rediscovery immediately before any replacement is published. */
    rc = cbm_pipeline_build_semantic_manifest(p->project_name, p->repo_path, files, file_count,
                                              p->excluded_dirs, p->excluded_count, &p->git_ctx,
                                              p->userconfig, &baseline_manifest, &baseline_count);
    if (rc != 0) {
        rc = CBM_PIPELINE_ABORT_PRESERVE_DB;
        goto cleanup;
    }

    /* Check for existing DB → try incremental or delete for reindex */
    rc = try_incremental_or_delete_db(p, files, file_count, baseline_manifest, baseline_count);
    if (rc >= 0 || rc == CBM_PIPELINE_ABORT_PRESERVE_DB || rc == CBM_PIPELINE_PERSIST_FAILED) {
        goto cleanup;
    }
    if (rc != CBM_PIPELINE_FORCE_FULL_REINDEX) {
        goto cleanup;
    }
    cbm_log_info("pipeline.route", "path", "full");

    /* Phase 2: Create graph buffer and registry */
    p->gbuf = cbm_gbuf_new(p->project_name, p->repo_path);
    p->registry = cbm_registry_new();

    /* Phase 2b: Load build-tool path aliases (tsconfig/jsconfig today). NULL
     * when no usable configs are found — non-TS projects pay nothing. */
    path_aliases =
        cbm_load_path_aliases_excluded(p->repo_path, p->excluded_dirs, p->excluded_count);

    /* Build shared context for pass functions */
    cbm_pipeline_ctx_t ctx = {
        .project_name = p->project_name,
        .repo_path = p->repo_path,
        .gbuf = p->gbuf,
        .registry = p->registry,
        .cancelled = &p->cancelled,
        .pipeline = p, /* so passes can record per-file skips (Track B) */
        .mode = (int)p->mode,
        .path_aliases = path_aliases,
        .excluded_dirs = p->excluded_dirs,
        .excluded_count = p->excluded_count,
    };

    rc = run_extraction_phase(p, &ctx, files, file_count);
    if (rc != 0) {
        goto cleanup;
    }

    rc = run_post_extraction(p, &ctx, files, file_count, baseline_manifest, baseline_count);
    if (rc != 0) {
        goto cleanup;
    }

    cbm_log_info("pipeline.done", "nodes", itoa_buf(cbm_gbuf_node_count(p->gbuf)), "edges",
                 itoa_buf(cbm_gbuf_edge_count(p->gbuf)), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t0)));
    CBM_PROF_END("pipeline", "TOTAL", t_pipeline_total);

cleanup:
    cbm_pkgmap_free(cbm_pipeline_get_pkgmap());
    cbm_pipeline_set_pkgmap(NULL);
    cbm_discover_free(files, file_count);
    cbm_pipeline_free_semantic_manifest(baseline_manifest, baseline_count);
    cbm_gbuf_free(p->gbuf);
    p->gbuf = NULL;
    cbm_registry_free(p->registry);
    p->registry = NULL;
    cbm_path_alias_collection_free(path_aliases);
    /* Clear and free user extension config */
    cbm_set_user_lang_config(NULL);
    cbm_userconfig_free(p->userconfig);
    p->userconfig = NULL;
    return rc;
}
