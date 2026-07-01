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

enum { CBM_DIR_PERMS = 0755, PL_RING = 4, PL_RING_MASK = 3, PL_SEQ_PASSES = 6 };
#include "cli/cli.h"
#include "pipeline/pipeline.h"
#include "pipeline/artifact.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/worker_pool.h"
#include "graph_buffer/graph_buffer.h"
#include "git/git_context.h"
#include "store/store.h"
#include "discover/discover.h"
#include "discover/userconfig.h"
#include "depindex/depindex.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/profile.h"
#include "foundation/mem.h"
#include "foundation/str_util.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <time.h>

static inline void *intptr_to_ptr(intptr_t v) {
    void *p;
    memcpy(&p, &v, sizeof(p));
    return p;
}

static double pipeline_unit_threshold(double threshold) {
    return (threshold > 0.0 && threshold <= 1.0) ? threshold : 0.0;
}

/* ── Global index lock ─────────────────────────────────────────── */
/* Prevents concurrent pipeline runs on the same DB file.
 * Atomic spinlock: 0 = free, 1 = locked. */
static atomic_int g_pipeline_busy = 0;

bool cbm_pipeline_try_lock(void) {
    return atomic_exchange(&g_pipeline_busy, 1) == 0;
}

#define LOCK_SPIN_NS 100000000 /* 100ms between lock retries */

typedef enum {
    CBM_INCREMENTAL_REINDEX_FAST = 0,
    CBM_INCREMENTAL_REINDEX_ALWAYS,
    CBM_INCREMENTAL_REINDEX_OFF,
} cbm_incremental_reindex_policy_t;

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
    double similarity_threshold; /* Jaccard threshold for SIMILAR edges; <=0 = default (#41) */
    double httplink_min_confidence;
    double semantic_threshold;
    double githistory_min_coupling;
    double lsp_confidence_floor;
    cbm_incremental_reindex_policy_t incremental_reindex;
    atomic_int cancelled;
    cbm_store_t *flush_store; /* when set, use flush_to_store instead of dump_to_sqlite */
    bool persistence; /* write .codebase-memory/graph.db.zst after indexing */

    /* Indexing state (set during run) */
    cbm_gbuf_t *gbuf;
    cbm_registry_t *registry;

    /* Directory subtrees skipped during discovery (rel paths). Captured from
     * cbm_discover_ex so the MCP layer can report excluded subtrees (#411).
     * Owned by the pipeline; freed in cbm_pipeline_free. */
    char **excluded_dirs;
    int excluded_count;

    /* User-defined extension overrides (loaded once per run) */
    cbm_userconfig_t *userconfig;

    /* Committed graph size at dump time (-1 = dump did not run). #334 gate axis. */
    int committed_nodes;
    int committed_edges;
    bool graph_changed;
    cbm_pipeline_publish_kind_t publish_kind;
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
    p->similarity_threshold = 0.0; /* 0 = use CBM_MINHASH_JACCARD_THRESHOLD default */
    p->httplink_min_confidence = 0.0;
    p->semantic_threshold = 0.0;
    p->githistory_min_coupling = 0.0;
    p->lsp_confidence_floor = 0.0;
    p->incremental_reindex = CBM_INCREMENTAL_REINDEX_OFF;
    p->persistence = false;
    p->committed_nodes = -1;
    p->committed_edges = -1;
    p->graph_changed = false;
    p->publish_kind = CBM_PIPELINE_PUBLISH_NONE;
    atomic_init(&p->cancelled, 0);

    return p;
}

void cbm_pipeline_set_project_name(cbm_pipeline_t *p, const char *name) {
    if (!p || !name) return;
    free(p->project_name);
    p->project_name = strdup(name);
}

void cbm_pipeline_set_flush_store(cbm_pipeline_t *p, cbm_store_t *store) {
    if (!p) return;
    p->flush_store = store;
}

/* Set the Jaccard similarity threshold for SIMILAR-edge creation (pass_similarity).
 * Pass <=0 (or don't call) to use the CBM_MINHASH_JACCARD_THRESHOLD default.
 * Must be called before cbm_pipeline_run(). */
void cbm_pipeline_set_similarity_threshold(cbm_pipeline_t *p, double threshold) {
    if (p) {
        p->similarity_threshold = pipeline_unit_threshold(threshold);
    }
}

void cbm_pipeline_set_httplink_min_confidence(cbm_pipeline_t *p, double threshold) {
    if (p) {
        p->httplink_min_confidence = pipeline_unit_threshold(threshold);
    }
}

void cbm_pipeline_set_semantic_threshold(cbm_pipeline_t *p, double threshold) {
    if (p) {
        p->semantic_threshold = pipeline_unit_threshold(threshold);
    }
}

void cbm_pipeline_set_githistory_min_coupling(cbm_pipeline_t *p, double threshold) {
    if (p) {
        p->githistory_min_coupling = pipeline_unit_threshold(threshold);
    }
}

void cbm_pipeline_set_lsp_confidence_floor(cbm_pipeline_t *p, double threshold) {
    if (p) {
        p->lsp_confidence_floor = pipeline_unit_threshold(threshold);
    }
}

void cbm_pipeline_apply_config(cbm_pipeline_t *p, cbm_config_t *cfg) {
    if (!p || !cfg) {
        return;
    }

    double sim_thresh =
        cbm_config_get_double(cfg, CBM_CONFIG_SIMILARITY_THRESHOLD, 0.0);
    if (sim_thresh > 0.0) {
        cbm_pipeline_set_similarity_threshold(p, sim_thresh);
    }

    double httplink_min =
        cbm_config_get_double(cfg, CBM_CONFIG_HTTPLINK_MIN_CONFIDENCE, 0.0);
    if (httplink_min > 0.0) {
        cbm_pipeline_set_httplink_min_confidence(p, httplink_min);
    }

    double semantic_thresh =
        cbm_config_get_double(cfg, CBM_CONFIG_SEMANTIC_THRESHOLD, 0.0);
    if (semantic_thresh > 0.0) {
        cbm_pipeline_set_semantic_threshold(p, semantic_thresh);
    }

    double gh_min =
        cbm_config_get_double(cfg, CBM_CONFIG_GITHISTORY_MIN_COUPLING, 0.0);
    if (gh_min > 0.0) {
        cbm_pipeline_set_githistory_min_coupling(p, gh_min);
    }

    double lsp_floor =
        cbm_config_get_double(cfg, CBM_CONFIG_LSP_CONFIDENCE_FLOOR, 0.0);
    if (lsp_floor > 0.0) {
        cbm_pipeline_set_lsp_confidence_floor(p, lsp_floor);
    }

    const char *incremental = cbm_config_get(cfg, CBM_CONFIG_INCREMENTAL_REINDEX, "off");
    if (incremental && strcmp(incremental, "always") == 0) {
        p->incremental_reindex = CBM_INCREMENTAL_REINDEX_ALWAYS;
    } else if (incremental && strcmp(incremental, "fast") == 0) {
        p->incremental_reindex = CBM_INCREMENTAL_REINDEX_FAST;
    } else {
        p->incremental_reindex = CBM_INCREMENTAL_REINDEX_OFF;
    }
}

double cbm_pipeline_httplink_min_confidence(const cbm_pipeline_t *p) {
    return p ? p->httplink_min_confidence : 0.0;
}

double cbm_pipeline_similarity_threshold(const cbm_pipeline_t *p) {
    return p ? p->similarity_threshold : 0.0;
}

double cbm_pipeline_semantic_threshold(const cbm_pipeline_t *p) {
    return p ? p->semantic_threshold : 0.0;
}

double cbm_pipeline_githistory_min_coupling(const cbm_pipeline_t *p) {
    return p ? p->githistory_min_coupling : 0.0;
}

double cbm_pipeline_lsp_confidence_floor(const cbm_pipeline_t *p) {
    return p ? p->lsp_confidence_floor : 0.0;
}

int cbm_pipeline_current_pass_fingerprint(const cbm_pipeline_t *p, char *out, size_t out_sz) {
    if (!p) {
        return CBM_STORE_ERR;
    }
    return cbm_pipeline_format_file_delta_pass_fingerprint(
        out, out_sz, p->mode, p->similarity_threshold, p->httplink_min_confidence,
        p->semantic_threshold, p->githistory_min_coupling, p->lsp_confidence_floor);
}

void cbm_pipeline_set_persistence(cbm_pipeline_t *p, bool enabled) {
    if (p) {
        p->persistence = enabled;
    }
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
    free(p->branch_qn);
    cbm_git_context_free(&p->git_ctx);
    /* gbuf, store, registry freed during/after run. flush_store NOT owned by pipeline. */
    /* Defensively free userconfig in case run() was never called or panicked */
    if (p->userconfig) {
        cbm_set_user_lang_config(NULL);
        cbm_userconfig_free(p->userconfig);
        p->userconfig = NULL;
    }
    free(p);
}

void cbm_pipeline_global_cleanup(void) {
    /* Release lazily-compiled regex patterns held by pass_envscan.
     * These are compiled on first call to cbm_scan_project_env_urls() and
     * cached for the process lifetime.  Call this once at server shutdown,
     * after all pipelines and background indexing threads have finished. */
    cbm_envscan_free_patterns();
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

bool cbm_pipeline_graph_changed(const cbm_pipeline_t *p) {
    return p && p->graph_changed;
}

cbm_pipeline_publish_kind_t cbm_pipeline_publish_kind(const cbm_pipeline_t *p) {
    return p ? p->publish_kind : CBM_PIPELINE_PUBLISH_NONE;
}

const char *cbm_pipeline_publish_kind_name(cbm_pipeline_publish_kind_t kind) {
    switch (kind) {
    case CBM_PIPELINE_PUBLISH_NONE:
        return "none";
    case CBM_PIPELINE_PUBLISH_FULL:
        return "full";
    case CBM_PIPELINE_PUBLISH_INCREMENTAL_NOOP:
        return "incremental_noop";
    case CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT:
        return "incremental_exact";
    case CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT:
        return "incremental_containment";
    default:
        return "unknown";
    }
}

void cbm_pipeline_set_graph_changed(cbm_pipeline_t *p, bool changed) {
    if (p) {
        p->graph_changed = changed;
    }
}

void cbm_pipeline_set_publish_kind(cbm_pipeline_t *p, cbm_pipeline_publish_kind_t kind) {
    if (p) {
        p->publish_kind = kind;
    }
}

static bool resolve_db_path_buf(const cbm_pipeline_t *p, char *path, size_t path_sz) {
    if (!p || !path || path_sz == 0) {
        return false;
    }
    path[0] = '\0';
    if (p->db_path) {
        int n = snprintf(path, path_sz, "%s", p->db_path);
        return n > 0 && (size_t)n < path_sz;
    }

    const char *cdir = cbm_resolve_cache_dir();
    if (!cdir) {
        cdir = cbm_tmpdir();
    }
    if (!cdir || !p->project_name) {
        return false;
    }
    int n = snprintf(path, path_sz, "%s/%s.db", cdir, p->project_name);
    return n > 0 && (size_t)n < path_sz;
}

/* Resolve the DB path for this pipeline. Caller must free(). */
static char *resolve_db_path(const cbm_pipeline_t *p) {
    char path[CBM_PATH_MAX];
    if (!resolve_db_path_buf(p, path, sizeof(path))) {
        return NULL;
    }
    char *out = cbm_strdup(path);
    if (!out) {
        return NULL;
    }
    return out;
}

static bool pipeline_parent_dir(char *out, size_t out_sz, const char *path) {
    if (!out || out_sz == 0 || !path) {
        return false;
    }
    int n = snprintf(out, out_sz, "%s", path);
    if (n <= 0 || (size_t)n >= out_sz) {
        out[0] = '\0';
        return false;
    }
    char *last_slash = strrchr(out, '/');
#ifdef _WIN32
    char *last_bslash = strrchr(out, '\\');
    if (last_bslash && (!last_slash || last_bslash > last_slash)) {
        last_slash = last_bslash;
    }
#endif
    if (last_slash) {
        *last_slash = '\0';
    } else {
        out[0] = '\0';
    }
    return true;
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
static void create_folder_chain(cbm_gbuf_t *gbuf, const char *project, const char *root_qn,
                                const char *dir, CBMHashTable *seen_dirs) {
    char *walk = strdup(dir);
    if (!walk) {
        return;
    }
    while (walk[0] != '\0' && (!seen_dirs || !cbm_ht_get(seen_dirs, walk))) {
        if (seen_dirs) {
            char *seen_key = strdup(walk);
            if (!seen_key) {
                break;
            }
            cbm_ht_set(seen_dirs, seen_key, intptr_to_ptr(SKIP_ONE));
        }
        char *folder_qn = cbm_pipeline_fqn_folder(project, walk);
        if (!folder_qn) {
            break;
        }
        const char *dir_base = strrchr(walk, '/');
        dir_base = dir_base ? dir_base + SKIP_ONE : walk;
        cbm_gbuf_upsert_node(gbuf, "Folder", dir_base, folder_qn, walk, 0, 0, "{}");

        char *pdir = strdup(walk);
        if (!pdir) {
            free(folder_qn);
            break;
        }
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
            pqn = root_qn ? root_qn : project;
        } else {
            pqn_heap = cbm_pipeline_fqn_folder(project, pdir);
            pqn = pqn_heap;
        }
        const cbm_gbuf_node_t *fn = cbm_gbuf_find_by_qn(gbuf, folder_qn);
        const cbm_gbuf_node_t *pn = cbm_gbuf_find_by_qn(gbuf, pqn);
        if (fn && pn) {
            cbm_gbuf_insert_edge(gbuf, pn->id, fn->id, "CONTAINS_FOLDER", "{}");
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

int cbm_pipeline_ensure_file_structure(cbm_gbuf_t *gbuf, const char *project,
                                       const char *root_qn, const char *rel_path,
                                       CBMHashTable *seen_dirs) {
    if (!gbuf || !project || !rel_path) {
        return CBM_NOT_FOUND;
    }

    char *file_qn = cbm_pipeline_fqn_compute(project, rel_path, "__file__");
    if (!file_qn) {
        return CBM_NOT_FOUND;
    }

    const char *slash = strrchr(rel_path, '/');
    const char *basename = slash ? slash + SKIP_ONE : rel_path;
    char props[CBM_SZ_256];
    const char *ext = strrchr(basename, '.');
    snprintf(props, sizeof(props), "{\"extension\":\"%s\"}", ext ? ext : "");
    cbm_gbuf_upsert_node(gbuf, "File", basename, file_qn, rel_path, 0, 0, props);

    char *dir = strdup(rel_path);
    if (!dir) {
        free(file_qn);
        return CBM_NOT_FOUND;
    }
    char *last_slash = strrchr(dir, '/');
    if (last_slash) {
        *last_slash = '\0';
    } else {
        free(dir);
        dir = strdup("");
        if (!dir) {
            free(file_qn);
            return CBM_NOT_FOUND;
        }
    }

    const char *parent_qn;
    char *parent_qn_heap = NULL;
    if (dir[0] == '\0') {
        parent_qn = root_qn ? root_qn : project;
    } else {
        parent_qn_heap = cbm_pipeline_fqn_folder(project, dir);
        if (!parent_qn_heap) {
            free(file_qn);
            free(dir);
            return CBM_NOT_FOUND;
        }
        parent_qn = parent_qn_heap;
    }

    create_folder_chain(gbuf, project, root_qn, dir, seen_dirs);

    const cbm_gbuf_node_t *fnode = cbm_gbuf_find_by_qn(gbuf, file_qn);
    const cbm_gbuf_node_t *pnode = cbm_gbuf_find_by_qn(gbuf, parent_qn);
    if (fnode && pnode) {
        cbm_gbuf_insert_edge(gbuf, pnode->id, fnode->id, "CONTAINS_FILE", "{}");
    }

    free(file_qn);
    free(dir);
    free(parent_qn_heap);
    return 0;
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

        cbm_pipeline_ensure_file_structure(p->gbuf, p->project_name, branch_qn, rel, seen_dirs);
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
    double min_coupling_score;
} gh_compute_arg_t;

static void *gh_compute_thread_fn(void *arg) {
    gh_compute_arg_t *a = arg;
    cbm_pipeline_githistory_compute_with_threshold(a->repo_path, a->result,
                                                   a->min_coupling_score);
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
    char topic_route_props[CBM_SZ_256];
    if (!cbm_pipeline_build_service_route_identity(ib->source_name, CBM_SVC_ASYNC, NULL,
                                                   ib->broker, "infra", topic_route_qn,
                                                   sizeof(topic_route_qn), topic_route_props,
                                                   sizeof(topic_route_props))) {
        return 0;
    }
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
                                              rel_path, 0, 0, topic_route_props);
        if (topic_route_id <= 0) {
            return 0;
        }
    }
    char props[CBM_SZ_512];
    char esc_broker[CBM_SZ_128];
    char esc_topic[CBM_SZ_256];
    char esc_endpoint[CBM_SZ_256];
    cbm_json_escape(esc_broker, sizeof(esc_broker),
                    ib->broker ? ib->broker : CBM_ROUTE_DEFAULT_ASYNC_BROKER);
    cbm_json_escape(esc_topic, sizeof(esc_topic), ib->source_name);
    cbm_json_escape(esc_endpoint, sizeof(esc_endpoint), ib->target_url);
    snprintf(props, sizeof(props), "{\"broker\":\"%s\",\"topic\":\"%s\",\"endpoint\":\"%s\"}",
             esc_broker, esc_topic, esc_endpoint);
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
    if (cbm_is_manifest_path(fp)) {
        return false;
    }
    return fp != NULL &&
           (strstr(fp, ".yaml") != NULL || strstr(fp, ".yml") != NULL ||
            strstr(fp, ".tf") != NULL || strstr(fp, ".hcl") != NULL || strstr(fp, ".toml") != NULL);
}

/* True when an infra key path denotes an upstream dependency, config value, or
 * healthcheck target rather than an endpoint this service exposes. Exposed
 * endpoint keys such as push_endpoint, callback, and webhook are intentionally
 * absent so they can still produce infra Route nodes. */
static bool is_upstream_config_key(const char *key_path) {
    if (!key_path) {
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

/* The graph node is keyed by URL value, while extraction may emit several refs
 * for the same value at different key-path granularities. If any ref says the
 * value is upstream/config/healthcheck-only, suppress that URL globally. */
static bool route_sr_denied(const CBMStringRef *sr) {
    if (!sr || !sr->value || strpbrk(sr->value, " \t\r\n") != NULL) {
        return true;
    }
    if (!sr->key_path) {
        return true;
    }
    return is_upstream_config_key(sr->key_path);
}

static void cbm_pipeline_extract_infra_routes(cbm_gbuf_t *gbuf, const cbm_file_info_t *files,
                                              CBMFileResult **result_cache, int file_count) {
    enum { CBM_INFRA_ROUTE_DENY_PASS_COUNT = 2 };
    CBMHashTable *denied = cbm_ht_create(CBM_SZ_16);
    if (!denied) {
        cbm_log_warn("pass.infra_routes", "reason", "deny_set_alloc_failed");
        return;
    }
    for (int pass = 0; pass < CBM_INFRA_ROUTE_DENY_PASS_COUNT; pass++) {
        for (int i = 0; i < file_count; i++) {
            if (!result_cache[i] || !is_infra_file(files[i].rel_path)) {
                continue;
            }
            for (int si = 0; si < result_cache[i]->string_refs.count; si++) {
                const CBMStringRef *sr = &result_cache[i]->string_refs.items[si];
                if (sr->kind != CBM_STRREF_URL || !sr->value || !strstr(sr->value, "://")) {
                    continue;
                }
                if (pass == 0) {
                    if (route_sr_denied(sr)) {
                        cbm_ht_set(denied, sr->value, intptr_to_ptr(1));
                    }
                } else if (!cbm_ht_has(denied, sr->value)) {
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
static int run_sequential_pipeline(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx,
                                   const cbm_file_info_t *files, int file_count,
                                   struct timespec *t) {
    cbm_log_info("pipeline.mode", "mode", "sequential", "files", itoa_buf(file_count));

    /* Build package map from manifest files (sequential: read manifests directly).
     * Use the repo-walking variant so manifests filtered out by the main
     * discoverer (package.json, composer.json) still feed pkgmap and let
     * workspace imports like `@my/pkg` resolve to their target Module. */
    cbm_pipeline_set_pkgmap(
        cbm_pkgmap_build_from_repo(ctx->repo_path, files, file_count, ctx->project_name));

    CBMFileResult **seq_cache = (CBMFileResult **)calloc(file_count, sizeof(CBMFileResult *));
    if (seq_cache) {
        ctx->result_cache = seq_cache;
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
    /* Registry build runs on the main graph and can allocate import, channel,
     * and other support nodes after parallel_extract. Keep the worker ID source
     * in sync before parallel_resolve creates worker-local decorator nodes. */
    atomic_store_explicit(&shared_ids, cbm_gbuf_next_id(p->gbuf), memory_order_relaxed);
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

/* Try incremental pipeline or delete old DB for reindex.
 * Returns >= 0 if incremental was used (the return code), or -1 to proceed with full. */
static int try_incremental_or_reindex(cbm_pipeline_t *p, cbm_file_info_t *files, int file_count) {
    char *db_path = resolve_db_path(p);
    if (!db_path) {
        return CBM_NOT_FOUND;
    }
    struct stat db_st;
    if (stat(db_path, &db_st) != 0) {
        free(db_path);
        return CBM_NOT_FOUND;
    }
    if (p->incremental_reindex == CBM_INCREMENTAL_REINDEX_OFF ||
        (p->incremental_reindex == CBM_INCREMENTAL_REINDEX_FAST &&
         p->mode != CBM_MODE_FAST)) {
        cbm_log_info("pipeline.route", "path", "full", "reason",
                     p->incremental_reindex == CBM_INCREMENTAL_REINDEX_OFF
                         ? "incremental_reindex=off"
                         : "incremental_reindex=fast_requires_fast_mode");
        free(db_path);
        return CBM_NOT_FOUND;
    }
    cbm_store_t *check_store = cbm_store_open_path(db_path);
    if (check_store && cbm_store_check_integrity(check_store)) {
        cbm_file_hash_t *hashes = NULL;
        int hash_count = 0;
        cbm_store_get_file_hashes(check_store, p->project_name, &hashes, &hash_count);
        cbm_store_free_file_hashes(hashes, hash_count);
        cbm_store_close(check_store);
        if (hash_count > 0 && file_count <= hash_count + (hash_count / PAIR_LEN)) {
            cbm_log_info("pipeline.route", "path", "incremental", "stored_hashes",
                         itoa_buf(hash_count));
            int rc = cbm_pipeline_run_incremental(p, db_path, files, file_count);
            free(db_path);
            return rc;
        }
        if (hash_count > 0) {
            cbm_log_info("pipeline.route", "path", "mode_change_reindex", "stored_hashes",
                         itoa_buf(hash_count), "discovered", itoa_buf(file_count));
        }
    } else if (check_store) {
        cbm_store_close(check_store);
    }
    cbm_log_info("pipeline.route", "path", "reindex", "action", "atomic_rewrite");
    free(db_path);
    return CBM_NOT_FOUND;
}

/* mtime conversion is shared with incremental and exact-delta metadata so
 * file_hash classification cannot drift by platform path. */

/* Run githistory pass. */
static int run_githistory(cbm_pipeline_t *p, cbm_pipeline_ctx_t *ctx) {
    struct timespec t_gh;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t_gh);

    cbm_githistory_result_t gh_result = {0};
    cbm_thread_t gh_thread;
    bool gh_threaded = false;
    gh_compute_arg_t gh_arg = {
        .repo_path = ctx->repo_path,
        .result = &gh_result,
        .min_coupling_score = ctx->githistory_min_coupling,
    };

    if (p->mode != CBM_MODE_FAST) {
        if (cbm_default_worker_count(true) > SKIP_ONE) {
            if (cbm_thread_create(&gh_thread, 0, gh_compute_thread_fn, &gh_arg) == 0) {
                gh_threaded = true;
            }
        }
        if (!gh_threaded) {
            cbm_pipeline_githistory_compute_with_threshold(ctx->repo_path, &gh_result,
                                                           ctx->githistory_min_coupling);
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

    int worker_count = cbm_default_worker_count(true);
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
    p->graph_changed = false;
    p->publish_kind = CBM_PIPELINE_PUBLISH_NONE;
    p->committed_nodes = -1;
    p->committed_edges = -1;

    CBM_PROF_START(t_pipeline_total);
    struct timespec t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
    cbm_path_alias_collection_t *path_aliases = NULL;

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
     * which directories were excluded (#411). Replace any prior list (e.g. a
     * re-run on the same pipeline) to avoid leaking the previous one. */
    cbm_discover_free_excluded(p->excluded_dirs, p->excluded_count);
    p->excluded_dirs = NULL;
    p->excluded_count = 0;
    int rc = cbm_discover_ex(p->repo_path, &opts, &files, &file_count, &p->excluded_dirs,
                             &p->excluded_count);
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

    /* Check for existing DB → try incremental or delete for reindex.
     * Skip the DB routing entirely when flush_store is set: the dep-indexing
     * path uses an in-memory store and never writes a DB file, so there is no
     * old DB to consult or delete. */
    if (!p->flush_store) {
        rc = try_incremental_or_reindex(p, files, file_count);
        if (rc >= 0) {
            cbm_discover_free(files, file_count);
            return rc;
        }
    }
    cbm_log_info("pipeline.route", "path", "full");

    /* Phase 2: Create graph buffer and registry */
    p->gbuf = cbm_gbuf_new(p->project_name, p->repo_path);
    p->registry = cbm_registry_new();

    /* Phase 2b: Load build-tool path aliases (tsconfig/jsconfig today). NULL
     * when no usable configs are found — non-TS projects pay nothing. */
    path_aliases = cbm_load_path_aliases(p->repo_path);

    /* Build shared context for pass functions */
    cbm_pipeline_ctx_t ctx = {
        .project_name = p->project_name,
        .repo_path = p->repo_path,
        .gbuf = p->gbuf,
        .registry = p->registry,
        .cancelled = &p->cancelled,
        .mode = (int)p->mode,
        .similarity_threshold = p->similarity_threshold,
        .httplink_min_confidence = p->httplink_min_confidence,
        .semantic_threshold = p->semantic_threshold,
        .githistory_min_coupling = p->githistory_min_coupling,
        .lsp_confidence_floor = p->lsp_confidence_floor,
        .path_aliases = path_aliases,
    };

    /* Extraction phase: structure + (parallel|sequential) definitions/calls/
     * usages/semantic/k8s/LSP-cross/infra. Upstream's helper is the superset
     * (adds cross-file LSP, similarity prep, infra bindings); the fork's older
     * inline parallel/sequential blocks + prescan_cache are subsumed by it, so
     * they are NOT inlined here (would duplicate/conflict). */
    rc = run_extraction_phase(p, &ctx, files, file_count);
    if (rc != 0) {
        goto cleanup;
    }

    /* Post-extraction phase. Upstream factors tests+githistory into
     * run_tests_and_history() and the decorator/configlink/route/similarity/
     * semantic_edges/complexity passes into run_predump_passes(); both are
     * dump-free, so we call them here and then run the fork-only httplinks and
     * normalize passes before the fork's dump block (which carries the
     * flush_store branch the dep-index path needs). We intentionally do NOT
     * call upstream run_post_extraction()/dump_and_persist_hashes(): they have
     * no flush_store branch and would double-dump the sqlite path. */
    rc = run_tests_and_history(p, &ctx, files, file_count);
    if (rc != 0) {
        goto cleanup;
    }

    if (!check_cancel(p)) {
        CBM_PROF_START(t_predump);
        run_predump_passes(p, &ctx);
        CBM_PROF_END("pipeline", "3_predump_passes_total", t_predump);
    }

    struct timespec t; /* timing for fork-only httplinks/normalize/dump passes */

    /* httplinks: fork-only HTTP endpoint discovery pass. Upstream dropped this
     * feature, so it is not inside run_predump_passes(); run it here. */
    if (!check_cancel(p)) {
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_httplinks(&ctx);
        cbm_log_info("pass.timing", "pass", "httplinks", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        if (rc != 0) {
            goto cleanup;
        }
        if (check_cancel(p)) {
            rc = -1;
            goto cleanup;
        }
    }

    /* Normalization: enforce structural invariants (I2: Method->Class,
     * I3: Field->Class). Runs after ALL files processed so all Class nodes
     * exist in the gbuf. Fork-only; upstream has no equivalent. Runtime
     * O(M+F); latency <10ms. */
    if (!check_cancel(p)) {
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        cbm_pipeline_pass_normalize(p->gbuf);
        cbm_log_info("pass.timing", "pass", "normalize", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
    }

    /* Dump: when flush_store is set (dep indexing) flush to the in-memory
     * store; otherwise write the .db sqlite file and persist file hashes
     * for the next incremental run. The flush_store path skips hash
     * persistence (no DB file is written). */
    if (!check_cancel(p)) {
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);

        char db_path[CBM_PATH_MAX];
        if (!resolve_db_path_buf(p, db_path, sizeof(db_path))) {
            cbm_log_error("pipeline.err", "phase", "resolve_db_path", "reason", "path_too_long");
            rc = CBM_NOT_FOUND;
            goto cleanup;
        }

        /* Ensure parent directory exists (e.g. ~/.cache/codebase-memory-mcp/) */
        char db_dir[CBM_PATH_MAX];
        if (!pipeline_parent_dir(db_dir, sizeof(db_dir), db_path)) {
            cbm_log_error("pipeline.err", "phase", "resolve_db_dir", "reason", "path_too_long");
            rc = CBM_NOT_FOUND;
            goto cleanup;
        }
        if (db_dir[0]) {
            cbm_mkdir_p(db_dir, CBM_DIR_PERMS);
        }

        /* Record committed counts BEFORE the dump: cbm_gbuf_dump_to_sqlite /
         * cbm_gbuf_flush_to_store free the gbuf node index, so reading the
         * count afterward yields 0 and leaves the #334 plausibility gate
         * inert for the full path. Mirrors the incremental path's call. */
        cbm_pipeline_set_committed_counts(p, cbm_gbuf_node_count(p->gbuf),
                                          cbm_gbuf_edge_count(p->gbuf));

        if (p->flush_store) {
            rc = cbm_gbuf_flush_to_store(p->gbuf, p->flush_store);
        } else {
            rc = cbm_gbuf_dump_to_sqlite(p->gbuf, db_path);
        }
        if (rc != 0) {
            cbm_log_error("pipeline.err", "phase", "dump");
            goto cleanup;
        }
        cbm_pipeline_set_graph_changed(p, true);
        cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_FULL);
        cbm_log_info("pass.timing", "pass", "dump", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

        /* Persist file hashes so next run can use incremental path.
         * Skipped for dep indexing (flush_store): no DB file is written. */
        if (!p->flush_store) {
            cbm_store_t *hash_store = cbm_store_open_path(db_path);
            if (hash_store) {
                /* Batch upserts in one transaction: N files -> 1 COMMIT under WAL
                 * instead of N autocommit fsyncs. Falls back to autocommit if
                 * BEGIN fails. Matches persist_hashes() in pipeline_incremental.c. */
                bool hash_batched = (cbm_store_begin(hash_store) == CBM_STORE_OK);
                int delete_rc = cbm_store_delete_file_hashes(hash_store, p->project_name);
                if (delete_rc != CBM_STORE_OK) {
                    if (hash_batched) {
                        (void)cbm_store_rollback(hash_store);
                    }
                    cbm_log_error("pipeline.err", "phase", "persist_hashes_delete", "rc",
                                  itoa_buf(delete_rc));
                    cbm_store_close(hash_store);
                    rc = delete_rc;
                    goto cleanup;
                }
                int hash_failed = 0;
                for (int i = 0; i < file_count; i++) {
                    struct stat fst;
                    if (stat(files[i].path, &fst) == 0) {
                        int hash_rc =
                            cbm_store_upsert_file_hash(hash_store, p->project_name,
                                                       files[i].rel_path, "",
                                                       cbm_pipeline_stat_mtime_ns(&fst),
                                                       fst.st_size);
                        if (hash_rc != CBM_STORE_OK) {
                            hash_failed++;
                        }
                    }
                }
                if (hash_failed > 0) {
                    if (hash_batched) {
                        (void)cbm_store_rollback(hash_store);
                    }
                    cbm_log_error("pipeline.err", "phase", "persist_hashes", "failed",
                                  itoa_buf(hash_failed));
                    cbm_store_close(hash_store);
                    rc = CBM_STORE_ERR;
                    goto cleanup;
                }
                if (hash_batched) {
                    int commit_rc = cbm_store_commit(hash_store);
                    if (commit_rc != CBM_STORE_OK) {
                        cbm_log_error("pipeline.err", "phase", "persist_hashes_commit", "rc",
                                      itoa_buf(commit_rc));
                        cbm_store_close(hash_store);
                        rc = commit_rc;
                        goto cleanup;
                    }
                }
                char pass_fingerprint[CBM_SZ_256];
                int state_rc =
                    cbm_pipeline_current_pass_fingerprint(p, pass_fingerprint,
                                                          sizeof(pass_fingerprint));
                if (state_rc == CBM_STORE_OK) {
                    state_rc = cbm_pipeline_persist_file_states(
                        hash_store, p->project_name, files, file_count,
                        CBM_PIPELINE_COMPAT_GENERATION, pass_fingerprint);
                }
                if (state_rc != CBM_STORE_OK) {
                    cbm_log_error("pipeline.err", "phase", "persist_file_state", "rc",
                                  itoa_buf(state_rc));
                    cbm_store_close(hash_store);
                    rc = state_rc;
                    goto cleanup;
                }
                if (p->incremental_reindex != CBM_INCREMENTAL_REINDEX_OFF) {
                    int owner_rc = cbm_store_rebuild_file_delta_owners(
                        hash_store, p->project_name, CBM_PIPELINE_COMPAT_GENERATION);
                    if (owner_rc != CBM_STORE_OK) {
                        cbm_log_error("pipeline.err", "phase", "rebuild_file_delta_owners", "rc",
                                      itoa_buf(owner_rc));
                        cbm_store_close(hash_store);
                        rc = owner_rc;
                        goto cleanup;
                    }
                }
                cbm_store_close(hash_store);
                cbm_log_info("pass.timing", "pass", "persist_hashes", "files",
                             itoa_buf(file_count));
            }

            /* Export persistent .db.zst artifact when persistence is enabled.
             * Ported from upstream's dump_and_persist_hashes so the fork's
             * sqlite dump path keeps the upstream feature; the flush_store
             * (dep-index) path never writes a DB file so it never exports. */
            if (p->persistence) {
                int arc = cbm_artifact_export(db_path, p->repo_path, p->project_name,
                                              CBM_ARTIFACT_BEST);
                if (arc != 0) {
                    const char *err = cbm_artifact_export_last_error();
                    cbm_log_error("pipeline.err", "phase", "artifact_export",
                                  "err", err ? err : "unknown");
                    rc = arc;
                    goto cleanup;
                }
            }
        }
    }
    cbm_log_info("pipeline.done", "nodes", itoa_buf(cbm_gbuf_node_count(p->gbuf)), "edges",
                 itoa_buf(cbm_gbuf_edge_count(p->gbuf)), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t0)));
    CBM_PROF_END("pipeline", "TOTAL", t_pipeline_total);

cleanup:
    cbm_pkgmap_free(cbm_pipeline_get_pkgmap());
    cbm_pipeline_set_pkgmap(NULL);
    cbm_discover_free(files, file_count);
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
