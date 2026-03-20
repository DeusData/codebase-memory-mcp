/*
 * pipeline_incremental.c — Disk-based incremental re-indexing.
 *
 * Operates on the existing SQLite DB directly (not RAM-first graph buffer).
 * Compares file mtime+size against stored hashes to classify changed/unchanged.
 * Deletes changed files' nodes (edges cascade via ON DELETE CASCADE),
 * re-parses only changed files through passes into a temp graph buffer,
 * then merges new nodes/edges into the disk DB. Persists updated hashes.
 *
 * Called from pipeline.c when a DB with stored hashes already exists.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <stdatomic.h>

/* ── Constants ───────────────────────────────────────────────────── */

#define CBM_MS_PER_SEC 1000.0
#define CBM_NS_PER_MS 1000000.0
#define CBM_NS_PER_SEC 1000000000LL

/* ── Timing helper (same as pipeline.c) ──────────────────────────── */

static double elapsed_ms(struct timespec start) {
    struct timespec now;
    cbm_clock_gettime(CLOCK_MONOTONIC, &now);
    double s = (double)(now.tv_sec - start.tv_sec);
    double ns = (double)(now.tv_nsec - start.tv_nsec);
    return (s * CBM_MS_PER_SEC) + (ns / CBM_NS_PER_MS);
}

/* itoa into static buffer — matches pipeline.c helper */
static const char *itoa_buf(int v) {
    static _Thread_local char buf[4][24];
    static _Thread_local int idx = 0;
    idx = (idx + 1) & 3;
    snprintf(buf[idx], sizeof(buf[idx]), "%d", v);
    return buf[idx];
}

/* ── Platform-portable mtime_ns ──────────────────────────────────── */

static int64_t stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * CBM_NS_PER_SEC) + (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * CBM_NS_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * CBM_NS_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

/* ── File classification ─────────────────────────────────────────── */

/* Classify discovered files against stored hashes using mtime+size.
 * Returns a boolean array: changed[i] = true if files[i] needs re-parsing.
 * Caller must free the returned array. */
static bool *classify_files(cbm_file_info_t *files, int file_count, cbm_file_hash_t *stored,
                            int stored_count, int *out_changed, int *out_unchanged) {
    bool *changed = calloc((size_t)file_count, sizeof(bool));
    if (!changed) {
        return NULL;
    }

    int n_changed = 0;
    int n_unchanged = 0;

    /* Build lookup: rel_path -> stored hash */
    CBMHashTable *ht = cbm_ht_create(stored_count > 0 ? (size_t)stored_count * 2 : 64);
    for (int i = 0; i < stored_count; i++) {
        cbm_ht_set(ht, stored[i].rel_path, &stored[i]);
    }

    for (int i = 0; i < file_count; i++) {
        cbm_file_hash_t *h = cbm_ht_get(ht, files[i].rel_path);
        if (!h) {
            /* New file */
            changed[i] = true;
            n_changed++;
            continue;
        }

        struct stat st;
        if (stat(files[i].path, &st) != 0) {
            changed[i] = true;
            n_changed++;
            continue;
        }

        if (stat_mtime_ns(&st) != h->mtime_ns || st.st_size != h->size) {
            changed[i] = true;
            n_changed++;
        } else {
            n_unchanged++;
        }
    }

    cbm_ht_free(ht);
    *out_changed = n_changed;
    *out_unchanged = n_unchanged;
    return changed;
}

/* Find stored files that no longer exist on disk. Returns count. */
static int find_deleted_files(cbm_file_info_t *files, int file_count, cbm_file_hash_t *stored,
                              int stored_count, char ***out_deleted) {
    CBMHashTable *current = cbm_ht_create((size_t)file_count * 2);
    for (int i = 0; i < file_count; i++) {
        cbm_ht_set(current, files[i].rel_path, &files[i]);
    }

    int count = 0;
    int cap = 64;
    char **deleted = malloc((size_t)cap * sizeof(char *));

    for (int i = 0; i < stored_count; i++) {
        if (!cbm_ht_get(current, stored[i].rel_path)) {
            if (count >= cap) {
                cap *= 2;
                char **tmp = realloc(deleted, (size_t)cap * sizeof(char *));
                if (!tmp) {
                    break;
                }
                deleted = tmp;
            }
            deleted[count++] = strdup(stored[i].rel_path);
        }
    }

    cbm_ht_free(current);
    *out_deleted = deleted;
    return count;
}

/* ── Persist file hashes ─────────────────────────────────────────── */

static void persist_hashes(cbm_store_t *store, const char *project, cbm_file_info_t *files,
                           int file_count) {
    for (int i = 0; i < file_count; i++) {
        struct stat st;
        if (stat(files[i].path, &st) != 0) {
            continue;
        }
        cbm_store_upsert_file_hash(store, project, files[i].rel_path, "", stat_mtime_ns(&st),
                                   st.st_size);
    }
}

/* ── Incremental pipeline entry point ────────────────────────────── */

int cbm_pipeline_run_incremental(cbm_pipeline_t *p, const char *db_path, cbm_file_info_t *files,
                                 int file_count) {
    struct timespec t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t0);

    const char *project = cbm_pipeline_project_name(p);

    /* Open existing disk DB */
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        cbm_log_error("incremental.err", "msg", "open_db_failed", "path", db_path);
        return -1;
    }

    /* Load stored file hashes */
    cbm_file_hash_t *stored = NULL;
    int stored_count = 0;
    cbm_store_get_file_hashes(store, project, &stored, &stored_count);

    /* Classify files */
    int n_changed = 0;
    int n_unchanged = 0;
    bool *is_changed =
        classify_files(files, file_count, stored, stored_count, &n_changed, &n_unchanged);

    /* Find deleted files */
    char **deleted = NULL;
    int deleted_count = find_deleted_files(files, file_count, stored, stored_count, &deleted);

    cbm_log_info("incremental.classify", "changed", itoa_buf(n_changed), "unchanged",
                 itoa_buf(n_unchanged), "deleted", itoa_buf(deleted_count));

    /* Fast path: nothing changed → skip */
    if (n_changed == 0 && deleted_count == 0) {
        cbm_log_info("incremental.noop", "reason", "no_changes");
        free(is_changed);
        free(deleted);
        cbm_store_free_file_hashes(stored, stored_count);
        cbm_store_close(store);
        return 0;
    }

    cbm_store_free_file_hashes(stored, stored_count);

    /* Delete changed files' nodes from disk DB (edges cascade) */
    cbm_store_begin(store);
    for (int i = 0; i < file_count; i++) {
        if (is_changed[i]) {
            cbm_store_delete_nodes_by_file(store, project, files[i].rel_path);
        }
    }

    /* Delete removed files' nodes + hashes */
    for (int i = 0; i < deleted_count; i++) {
        cbm_store_delete_nodes_by_file(store, project, deleted[i]);
        cbm_store_delete_file_hash(store, project, deleted[i]);
        cbm_log_info("incremental.removed", "file", deleted[i]);
        free(deleted[i]);
    }
    free(deleted);
    cbm_store_commit(store);

    /* Build list of changed files only */
    cbm_file_info_t *changed_files = malloc((size_t)n_changed * sizeof(cbm_file_info_t));
    int ci = 0;
    for (int i = 0; i < file_count; i++) {
        if (is_changed[i]) {
            changed_files[ci++] = files[i];
        }
    }
    free(is_changed);

    cbm_log_info("incremental.reparse", "files", itoa_buf(ci));

    /* Create temp graph buffer + registry for re-parsing changed files */
    cbm_gbuf_t *gbuf = cbm_gbuf_new(project, cbm_pipeline_repo_path(p));
    cbm_registry_t *registry = cbm_registry_new();

    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = cbm_pipeline_repo_path(p),
        .gbuf = gbuf,
        .registry = registry,
        .cancelled = cbm_pipeline_cancelled_ptr(p),
    };

    /* Run passes on changed files only */
    struct timespec t;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_pass_definitions(&ctx, changed_files, ci);
    cbm_log_info("pass.timing", "pass", "incr_definitions", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_pass_calls(&ctx, changed_files, ci);
    cbm_log_info("pass.timing", "pass", "incr_calls", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_pass_usages(&ctx, changed_files, ci);
    cbm_log_info("pass.timing", "pass", "incr_usages", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_pass_semantic(&ctx, changed_files, ci);
    cbm_log_info("pass.timing", "pass", "incr_semantic", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));

    /* Merge new nodes/edges from gbuf into disk DB */
    int new_nodes = cbm_gbuf_node_count(gbuf);
    int new_edges = cbm_gbuf_edge_count(gbuf);
    cbm_gbuf_merge_into_store(gbuf, store);

    cbm_log_info("incremental.merged", "nodes", itoa_buf(new_nodes), "edges", itoa_buf(new_edges));

    /* Persist updated file hashes for ALL files */
    persist_hashes(store, project, files, file_count);

    /* Cleanup */
    cbm_gbuf_free(gbuf);
    cbm_registry_free(registry);
    free(changed_files);
    cbm_store_close(store);

    cbm_log_info("incremental.done", "elapsed_ms", itoa_buf((int)elapsed_ms(t0)));
    return 0;
}
