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
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
// NOLINTNEXTLINE(misc-include-cleaner) — worker_pool.h included for interface contract
#include "pipeline/worker_pool.h"
#include "graph_buffer/graph_buffer.h"
#include "store/store.h"
#include "discover/discover.h"
#include "discover/userconfig.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <time.h>

/* ── Internal state ──────────────────────────────────────────────── */

struct cbm_pipeline {
    char *repo_path;
    char *db_path;
    char *project_name;
    cbm_index_mode_t mode;
    atomic_int cancelled;

    /* Indexing state (set during run) */
    cbm_gbuf_t *gbuf;
    cbm_registry_t *registry;

    /* User-defined extension overrides (loaded once per run) */
    cbm_userconfig_t *userconfig;
};

/* ── Timing helper ──────────────────────────────────────────────── */

static double elapsed_ms(struct timespec start) {
    struct timespec now;
    // NOLINTNEXTLINE(misc-include-cleaner) — cbm_clock_gettime provided by standard header
    cbm_clock_gettime(CLOCK_MONOTONIC, &now);
    return ((double)(now.tv_sec - start.tv_sec) * CBM_MS_PER_SEC) +
           ((double)(now.tv_nsec - start.tv_nsec) / CBM_US_PER_SEC_F);
}

/* Format int to string for logging. Thread-safe via TLS rotating buffers. */
static const char *itoa_buf(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

cbm_pipeline_t *cbm_pipeline_new(const char *repo_path, const char *db_path,
                                 cbm_index_mode_t mode) {
    if (!repo_path) {
        return NULL;
    }

    cbm_pipeline_t *p = calloc(1, sizeof(cbm_pipeline_t));
    if (!p) {
        return NULL;
    }

    // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
    p->repo_path = strdup(repo_path);
    p->db_path = db_path ? strdup(db_path) : NULL;
    p->project_name = cbm_project_name_from_path(repo_path);
    p->mode = mode;
    atomic_init(&p->cancelled, 0);

    return p;
}

void cbm_pipeline_free(cbm_pipeline_t *p) {
    if (!p) {
        return;
    }
    free(p->repo_path);
    free(p->db_path);
    free(p->project_name);
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

/* Resolve the DB path for this pipeline. Caller must free(). */
static char *resolve_db_path(const cbm_pipeline_t *p) {
    char *path = malloc(1024);
    if (!path) {
        return NULL;
    }
    if (p->db_path) {
        snprintf(path, 1024, "%s", p->db_path);
    } else {
        const char *home = cbm_get_home_dir();
        if (!home) {
            home = cbm_tmpdir();
        }
        snprintf(path, 1024, "%s/.cache/codebase-memory-mcp/%s.db", home, p->project_name);
    }
    return path;
}

static int check_cancel(const cbm_pipeline_t *p) {
    return atomic_load(&p->cancelled) ? -1 : 0;
}

/* ── Hash table cleanup callback ─────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void free_seen_dir_key(const char *key, void *val, void *ud) {
    (void)val;
    (void)ud;
    free((void *)key);
}

/* ── Pass 1: Structure ──────────────────────────────────────────── */

/* Create Project, Folder/Package, and File nodes in the graph buffer. */
static int pass_structure(cbm_pipeline_t *p, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "structure", "files", itoa_buf(file_count));

    /* Project node */
    cbm_gbuf_upsert_node(p->gbuf, "Project", p->project_name, p->project_name, NULL, 0, 0, "{}");

    /* Collect unique directories and create Folder/Package nodes */
    CBMHashTable *seen_dirs = cbm_ht_create(256);

    for (int i = 0; i < file_count; i++) {
        const char *rel = files[i].rel_path;
        if (!rel) {
            continue;
        }

        /* Create File node */
        char *file_qn = cbm_pipeline_fqn_compute(p->project_name, rel, "__file__");
        /* Extract basename */
        const char *slash = strrchr(rel, '/');
        const char *basename = slash ? slash + 1 : rel;

        char props[256];
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
            parent_qn = p->project_name;
        } else {
            parent_qn_heap = cbm_pipeline_fqn_folder(p->project_name, dir);
            parent_qn = parent_qn_heap;
        }

        /* Walk up directory chain, creating Folder nodes */
        char *walk_dir = strdup(dir);
        while (walk_dir[0] != '\0' && !cbm_ht_get(seen_dirs, walk_dir)) {
            cbm_ht_set(seen_dirs, strdup(walk_dir), (void *)1);

            char *folder_qn = cbm_pipeline_fqn_folder(p->project_name, walk_dir);
            const char *dir_base = strrchr(walk_dir, '/');
            dir_base = dir_base ? dir_base + 1 : walk_dir;

            cbm_gbuf_upsert_node(p->gbuf, "Folder", dir_base, folder_qn, walk_dir, 0, 0, "{}");

            /* CONTAINS_FOLDER edge: parent -> this dir */
            char *pdir = strdup(walk_dir);
            char *ps = strrchr(pdir, '/');
            if (ps) {
                {
                    *ps = '\0';
                }
            } else {
                free(pdir);
                pdir = strdup("");
            }

            const char *pqn;
            char *pqn_heap = NULL;
            if (pdir[0] == '\0') {
                pqn = p->project_name;
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

            /* Move up one level */
            char *up = strrchr(walk_dir, '/');
            if (up) {
                *up = '\0';
            } else {
                walk_dir[0] = '\0';
            }
            free(pdir);
        }

        /* Now create the CONTAINS_FILE edge */
        const cbm_gbuf_node_t *fnode = cbm_gbuf_find_by_qn(p->gbuf, file_qn);
        const cbm_gbuf_node_t *pnode = cbm_gbuf_find_by_qn(p->gbuf, parent_qn);
        if (fnode && pnode) {
            cbm_gbuf_insert_edge(p->gbuf, pnode->id, fnode->id, "CONTAINS_FILE", "{}");
        }

        free(file_qn);
        free(dir);
        free(walk_dir);
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

/* ── Pipeline run ────────────────────────────────────────────────── */

int cbm_pipeline_run(cbm_pipeline_t *p) {
    if (!p) {
        return -1;
    }

    struct timespec t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t0);

    /* Load user-defined extension overrides (fail-open: NULL on error) */
    p->userconfig = cbm_userconfig_load(p->repo_path);
    cbm_set_user_lang_config(p->userconfig);

    /* Phase 1: Discover files */
    cbm_discover_opts_t opts = {
        .mode = p->mode,
        .ignore_file = NULL,
        .max_file_size = 0,
    };
    cbm_file_info_t *files = NULL;
    int file_count = 0;
    int rc = cbm_discover(p->repo_path, &opts, &files, &file_count);
    if (rc != 0) {
        cbm_log_error("pipeline.err", "phase", "discover", "rc", itoa_buf(rc));
        cbm_discover_free(files, file_count);
        cbm_set_user_lang_config(NULL);
        cbm_userconfig_free(p->userconfig);
        p->userconfig = NULL;
        return -1;
    }
    cbm_log_info("pipeline.discover", "files", itoa_buf(file_count), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t0)));

    if (check_cancel(p)) {
        cbm_discover_free(files, file_count);
        cbm_set_user_lang_config(NULL);
        cbm_userconfig_free(p->userconfig);
        p->userconfig = NULL;
        return -1;
    }

    /* Check for existing DB with file hashes → incremental path */
    {
        char *db_path = resolve_db_path(p);
        if (db_path) {
            struct stat db_st;
            if (stat(db_path, &db_st) == 0) {
                /* DB exists — check if it has file hashes */
                cbm_store_t *check_store = cbm_store_open_path(db_path);
                if (check_store) {
                    cbm_file_hash_t *hashes = NULL;
                    int hash_count = 0;
                    cbm_store_get_file_hashes(check_store, p->project_name, &hashes, &hash_count);
                    cbm_store_free_file_hashes(hashes, hash_count);
                    cbm_store_close(check_store);

                    if (hash_count > 0) {
                        cbm_log_info("pipeline.route", "path", "incremental", "stored_hashes",
                                     itoa_buf(hash_count));
                        rc = cbm_pipeline_run_incremental(p, db_path, files, file_count);
                        cbm_discover_free(files, file_count);
                        free(db_path);
                        return rc;
                    }
                }
            }
            free(db_path);
        }
    }
    cbm_log_info("pipeline.route", "path", "full");

    /* Phase 2: Create graph buffer and registry */
    p->gbuf = cbm_gbuf_new(p->project_name, p->repo_path);
    p->registry = cbm_registry_new();

    /* Phase 3: Run passes */
    struct timespec t;

    /* Build shared context for pass functions */
    cbm_pipeline_ctx_t ctx = {
        .project_name = p->project_name,
        .repo_path = p->repo_path,
        .gbuf = p->gbuf,
        .registry = p->registry,
        .cancelled = &p->cancelled,
    };

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    rc = pass_structure(p, files, file_count);
    if (rc != 0) { // cppcheck-suppress knownConditionTrueFalse
        goto cleanup;
    }
    cbm_log_info("pass.timing", "pass", "structure", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
    if (check_cancel(p)) {
        rc = -1;
        goto cleanup;
    }

    /* Decide: parallel or sequential pipeline */
    int worker_count = cbm_default_worker_count(true);
#define MIN_FILES_FOR_PARALLEL 50
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool use_parallel = (worker_count > 1 && file_count > MIN_FILES_FOR_PARALLEL);

    if (use_parallel) {
        cbm_log_info("pipeline.mode", "mode", "parallel", "workers", itoa_buf(worker_count),
                     "files", itoa_buf(file_count));

        /* Shared atomic ID source — workers allocate globally unique IDs */
        // NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
        _Atomic int64_t shared_ids;
        int64_t gbuf_next = cbm_gbuf_next_id(p->gbuf);
        atomic_init(&shared_ids, gbuf_next);

        /* Allocate result cache: one CBMFileResult* per file */
        // NOLINTNEXTLINE(misc-include-cleaner)
        CBMFileResult **result_cache =
            (CBMFileResult **)calloc(file_count, sizeof(CBMFileResult *));
        if (!result_cache) {
            cbm_log_error("pipeline.err", "phase", "cache_alloc");
            rc = -1;
            goto cleanup;
        }

        /* Allocate prescan cache: HTTP sites + config refs extracted during
         * extraction phase while source is in memory. Eliminates all disk
         * re-reads in httplinks (2M+ reads) and configlink (62K+ reads). */
        cbm_prescan_t *prescan_cache = calloc(file_count, sizeof(cbm_prescan_t));
        ctx.prescan_cache = prescan_cache;
        ctx.prescan_count = file_count;

        /* Build path → file_idx map for prescan lookup by rel_path */
        CBMHashTable *prescan_map = cbm_ht_create(0);
        for (int i = 0; i < file_count; i++) {
            cbm_ht_set(prescan_map, files[i].rel_path, (void *)((intptr_t)i + 1));
        }
        ctx.prescan_path_map = prescan_map;

        /* Phase 3A: Parallel extract + definition nodes */
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_parallel_extract(&ctx, files, file_count, result_cache, &shared_ids, worker_count);
        cbm_log_info("pass.timing", "pass", "parallel_extract", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        if (rc != 0) {
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            free(result_cache);
            goto cleanup;
        }
        if (check_cancel(p)) {
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            free(result_cache);
            rc = -1;
            goto cleanup;
        }

        /* Sync gbuf ID counter after merge */
        cbm_gbuf_set_next_id(p->gbuf, atomic_load(&shared_ids));

        /* Phase 3B: Serial registry build + DEFINES/IMPORTS edges */
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_build_registry_from_cache(&ctx, files, file_count, result_cache);
        cbm_log_info("pass.timing", "pass", "registry_build", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        if (rc != 0) {
            for (int i = 0; i < file_count; i++) {
                if (result_cache[i]) {
                    // NOLINTNEXTLINE(misc-include-cleaner)
                    cbm_free_result(result_cache[i]);
                }
            }
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            free(result_cache);
            goto cleanup;
        }
        if (check_cancel(p)) {
            for (int i = 0; i < file_count; i++) {
                if (result_cache[i]) {
                    cbm_free_result(result_cache[i]);
                }
            }
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            free(result_cache);
            rc = -1;
            goto cleanup;
        }

        /* Phase 4: Parallel resolution (calls + usages + semantic, fused) */
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_parallel_resolve(&ctx, files, file_count, result_cache, &shared_ids, worker_count);
        cbm_log_info("pass.timing", "pass", "parallel_resolve", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));

        /* Sync gbuf ID counter after resolve merge */
        cbm_gbuf_set_next_id(p->gbuf, atomic_load(&shared_ids));

        /* Free cached extraction results */
        for (int i = 0; i < file_count; i++) {
            if (result_cache[i]) {
                cbm_free_result(result_cache[i]);
            }
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(result_cache);

        if (rc != 0) {
            goto cleanup;
        }
        if (check_cancel(p)) {
            rc = -1;
            goto cleanup;
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_k8s(&ctx, files, file_count);
        if (rc != 0) { /* log warning, continue */
        }
        cbm_log_info("pass.timing", "pass", "k8s", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
        if (check_cancel(p)) {
            rc = -1;
            goto cleanup;
        }
    } else {
        cbm_log_info("pipeline.mode", "mode", "sequential", "files", itoa_buf(file_count));

        /* Allocate result cache: pass_definitions stores results for reuse
         * by pass_calls/usages/semantic, avoiding 3x redundant file I/O + parsing */
        // NOLINTNEXTLINE(misc-include-cleaner)
        CBMFileResult **seq_cache = (CBMFileResult **)calloc(file_count, sizeof(CBMFileResult *));
        if (seq_cache) {
            ctx.result_cache = seq_cache;
        }

        /* Sequential fallback: original 4-pass chain */
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_definitions(&ctx, files, file_count);
        if (rc != 0) {
            goto seq_cleanup;
        }
        cbm_log_info("pass.timing", "pass", "definitions", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
        if (check_cancel(p)) {
            rc = -1;
            goto seq_cleanup;
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_k8s(&ctx, files, file_count);
        if (rc != 0) { /* log warning, continue */
        }
        cbm_log_info("pass.timing", "pass", "k8s", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
        if (check_cancel(p)) {
            rc = -1;
            goto seq_cleanup;
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_calls(&ctx, files, file_count);
        if (rc != 0) {
            goto seq_cleanup;
        }
        cbm_log_info("pass.timing", "pass", "calls", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
        if (check_cancel(p)) {
            rc = -1;
            goto seq_cleanup;
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_usages(&ctx, files, file_count);
        if (rc != 0) {
            goto seq_cleanup;
        }
        cbm_log_info("pass.timing", "pass", "usages", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
        if (check_cancel(p)) {
            rc = -1;
            goto seq_cleanup;
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_semantic(&ctx, files, file_count);
        if (rc != 0) {
            goto seq_cleanup;
        }
        cbm_log_info("pass.timing", "pass", "semantic", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
        if (check_cancel(p)) {
            rc = -1;
            goto seq_cleanup;
        }

    seq_cleanup:
        /* Free cached extraction results */
        if (seq_cache) {
            for (int i = 0; i < file_count; i++) {
                if (seq_cache[i]) {
                    cbm_free_result(seq_cache[i]);
                }
            }
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            free(seq_cache);
            ctx.result_cache = NULL;
        }
        if (rc != 0) {
            goto cleanup;
        }
    }

    /* Post-extraction passes (shared by both parallel and sequential) */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    rc = cbm_pipeline_pass_tests(&ctx, files, file_count);
    if (rc != 0) {
        goto cleanup;
    }
    cbm_log_info("pass.timing", "pass", "tests", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
    if (check_cancel(p)) {
        rc = -1;
        goto cleanup;
    }

    /* ── Fused post-passes: githistory (I/O) + httplinks (CPU) in parallel ── */
    {
        struct timespec t_gh;
        struct timespec t_hl;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t_gh);
        cbm_clock_gettime(CLOCK_MONOTONIC, &t_hl);

        cbm_githistory_result_t gh_result = {0};
        cbm_thread_t gh_thread;
        bool gh_threaded = false;
        gh_compute_arg_t gh_arg = {.repo_path = ctx.repo_path, .result = &gh_result};

        /* Skip githistory entirely in fast mode */
        if (p->mode != CBM_MODE_FAST) {

            /* Only parallelize if we have multiple cores */
            if (cbm_default_worker_count(true) > 1) {
                if (cbm_thread_create(&gh_thread, 0, gh_compute_thread_fn, &gh_arg) == 0) {
                    gh_threaded = true;
                }
            }

            /* If threading failed or single-core, run githistory serially first */
            if (!gh_threaded) {
                cbm_pipeline_githistory_compute(ctx.repo_path, &gh_result);
                cbm_log_info("pass.timing", "pass", "githistory_compute", "elapsed_ms",
                             itoa_buf((int)elapsed_ms(t_gh)));
            }
        } else {
            cbm_log_info("pass.skip", "pass", "githistory", "reason", "fast_mode");
        }

        /* Run httplinks on main thread (CPU-bound) */
        rc = cbm_pipeline_pass_httplinks(&ctx);
        cbm_log_info("pass.timing", "pass", "httplinks", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t_hl)));

        /* Wait for githistory thread to complete */
        if (gh_threaded) {
            cbm_thread_join(&gh_thread);
            cbm_log_info("pass.timing", "pass", "githistory_compute", "elapsed_ms",
                         itoa_buf((int)elapsed_ms(t_gh)));
        }

        if (rc != 0) {
            free(gh_result.couplings);
            goto cleanup;
        }
        if (check_cancel(p)) {
            free(gh_result.couplings);
            rc = -1;
            goto cleanup;
        }

        /* Apply githistory edges (serial, writes to gbuf) */
        int gh_edges = 0;
        if (gh_result.count > 0) {
            gh_edges = cbm_pipeline_githistory_apply(&ctx, &gh_result);
        }
        cbm_log_info("pass.done", "pass", "githistory", "commits", itoa_buf(gh_result.commit_count),
                     "edges", itoa_buf(gh_edges));
        free(gh_result.couplings);
    }

    /* Pre-dump passes (operate on graph buffer, not store) */
    if (!check_cancel(p)) {
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        cbm_pipeline_pass_decorator_tags(p->gbuf, p->project_name);
        cbm_log_info("pass.timing", "pass", "decorator_tags", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
    }

    if (!check_cancel(p)) {
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        cbm_pipeline_pass_configlink(&ctx);
        cbm_log_info("pass.timing", "pass", "configlink", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));
    }

    /* Free prescan cache — no longer needed after httplinks + configlink */
    if (ctx.prescan_cache) {
        for (int i = 0; i < ctx.prescan_count; i++) {
            free(ctx.prescan_cache[i].http_sites);
            free(ctx.prescan_cache[i].config_refs);
            free(ctx.prescan_cache[i].routes);
        }
        free(ctx.prescan_cache);
        ctx.prescan_cache = NULL;
    }
    if (ctx.prescan_path_map) {
        cbm_ht_free(ctx.prescan_path_map);
        ctx.prescan_path_map = NULL;
    }

    /* Direct dump: construct B-tree pages in C, fwrite() to .db file.
     * Zero SQLite library involvement — cbm_write_db() builds the binary
     * format directly from flat arrays. Atomic: writes .tmp then renames. */
    if (!check_cancel(p)) {
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);

        const char *home = cbm_get_home_dir();
        char db_path[1024];
        if (p->db_path) {
            snprintf(db_path, sizeof(db_path), "%s", p->db_path);
        } else {
            if (!home) {
                home = cbm_tmpdir();
            }
            snprintf(db_path, sizeof(db_path), "%s/.cache/codebase-memory-mcp/%s.db", home,
                     p->project_name);
        }

        /* Ensure parent directory exists (e.g. ~/.cache/codebase-memory-mcp/) */
        char db_dir[1024];
        snprintf(db_dir, sizeof(db_dir), "%s", db_path);
        char *last_slash = strrchr(db_dir, '/');
        if (last_slash) {
            *last_slash = '\0';
            cbm_mkdir_p(db_dir, 0755);
        }

        rc = cbm_gbuf_dump_to_sqlite(p->gbuf, db_path);
        if (rc != 0) {
            cbm_log_error("pipeline.err", "phase", "dump");
            goto cleanup;
        }
        cbm_log_info("pass.timing", "pass", "dump", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

        /* Persist file hashes so next run can use incremental path */
        cbm_store_t *hash_store = cbm_store_open_path(db_path);
        if (hash_store) {
            cbm_store_delete_file_hashes(hash_store, p->project_name);
            for (int i = 0; i < file_count; i++) {
                struct stat fst;
                if (stat(files[i].path, &fst) == 0) {
                    int64_t mtime_ns;
#ifdef __APPLE__
                    mtime_ns = ((int64_t)fst.st_mtimespec.tv_sec * 1000000000LL) +
                               (int64_t)fst.st_mtimespec.tv_nsec;
#elif defined(_WIN32)
                    mtime_ns = (int64_t)fst.st_mtime * 1000000000LL;
#else
                    mtime_ns =
                        ((int64_t)fst.st_mtim.tv_sec * 1000000000LL) + (int64_t)fst.st_mtim.tv_nsec;
#endif
                    cbm_store_upsert_file_hash(hash_store, p->project_name, files[i].rel_path, "",
                                               mtime_ns, fst.st_size);
                }
            }
            cbm_store_close(hash_store);
            cbm_log_info("pass.timing", "pass", "persist_hashes", "files", itoa_buf(file_count));
        }
    }

    cbm_log_info("pipeline.done", "nodes", itoa_buf(cbm_gbuf_node_count(p->gbuf)), "edges",
                 itoa_buf(cbm_gbuf_edge_count(p->gbuf)), "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t0)));

cleanup:
    /* Free prescan if not already freed */
    if (ctx.prescan_cache) {
        for (int i = 0; i < ctx.prescan_count; i++) {
            free(ctx.prescan_cache[i].http_sites);
            free(ctx.prescan_cache[i].config_refs);
            free(ctx.prescan_cache[i].routes);
        }
        free(ctx.prescan_cache);
        ctx.prescan_cache = NULL;
    }
    if (ctx.prescan_path_map) {
        cbm_ht_free(ctx.prescan_path_map);
        ctx.prescan_path_map = NULL;
    }
    cbm_discover_free(files, file_count);
    cbm_gbuf_free(p->gbuf);
    p->gbuf = NULL;
    cbm_registry_free(p->registry);
    p->registry = NULL;
    /* Clear and free user extension config */
    cbm_set_user_lang_config(NULL);
    cbm_userconfig_free(p->userconfig);
    p->userconfig = NULL;
    return rc;
}
