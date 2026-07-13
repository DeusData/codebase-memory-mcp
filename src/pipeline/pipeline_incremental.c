/*
 * pipeline_incremental.c — Disk-based incremental re-indexing.
 *
 * Compares file metadata against stored hashes, hash-confirming metadata-equal
 * files when compatible file_state rows are available.
 * For non-noop changes, loads the existing SQLite graph into a graph buffer,
 * purges changed/deleted file paths, reparses changed files, and republishes
 * the full graph. This is correctness containment, not a true delta publish.
 *
 * Called from pipeline.c when a DB with stored hashes already exists.
 */
#include "foundation/constants.h"

enum { INCR_RING_BUF = 4, INCR_RING_MASK = 3, INCR_TS_BUF = 24 };
#include "pipeline/pipeline.h"
#include "pipeline/artifact.h"
#include <stdio.h>
#include <time.h>
#include "pipeline/pipeline_internal.h"
#include "pipeline/pass_lsp_cross.h"
#include "store/store.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "foundation/str_util.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"
#include "foundation/profile.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <stdatomic.h>
#include <stdint.h>

/* ── Timing helper (same as pipeline.c) ──────────────────────────── */

/* Fork renames this elapsed_ms_incr (vs pipeline.c's elapsed_ms) to avoid an
 * ODR/duplicate-symbol collision when both TUs are linked into the same binary. */
static double elapsed_ms_incr(struct timespec start) {
    struct timespec now;
    cbm_clock_gettime(CLOCK_MONOTONIC, &now);
    double s = (double)(now.tv_sec - start.tv_sec);
    double ns = (double)(now.tv_nsec - start.tv_nsec);
    return (s * (double)CBM_MSEC_PER_SEC) + (ns / (double)CBM_NSEC_PER_MSEC);
}

/* itoa into static buffer — matches pipeline.c helper. Fork renames to
 * itoa_buf_incr to avoid ODR collision with pipeline.c's itoa_buf. Uses
 * upstream's named constants (INCR_RING_BUF/INCR_TS_BUF/INCR_RING_MASK). */
static const char *itoa_buf_incr(int v) {
    static _Thread_local char buf[INCR_RING_BUF][INCR_TS_BUF];
    static _Thread_local int idx = 0;
    idx = (idx + SKIP_ONE) & INCR_RING_MASK;
    snprintf(buf[idx], sizeof(buf[idx]), "%d", v);
    return buf[idx];
}

static void free_mode_skipped(cbm_file_hash_t *ms, int count);
static void free_deleted_paths(char **deleted, int count);

static bool incr_changed_contains_c_family_header(const cbm_file_info_t *changed_files,
                                                  int changed_count) {
    if (!changed_files || changed_count <= 0) {
        return false;
    }
    for (int i = 0; i < changed_count; i++) {
        if (cbm_pipeline_is_c_family_header(changed_files[i].language,
                                            changed_files[i].rel_path)) {
            return true;
        }
    }
    return false;
}

static bool incr_changed_all_c_family_headers(const cbm_file_info_t *changed_files,
                                              int changed_count) {
    if (!changed_files || changed_count <= 0) {
        return false;
    }
    for (int i = 0; i < changed_count; i++) {
        if (!cbm_pipeline_is_c_family_header(changed_files[i].language,
                                             changed_files[i].rel_path)) {
            return false;
        }
    }
    return true;
}

static bool incr_changed_contains_c_family_source(const cbm_file_info_t *changed_files,
                                                  int changed_count) {
    if (!changed_files || changed_count <= 0) {
        return false;
    }
    for (int i = 0; i < changed_count; i++) {
        if (cbm_pipeline_is_c_family_source(changed_files[i].language,
                                            changed_files[i].rel_path)) {
            return true;
        }
    }
    return false;
}

static bool incr_language_has_scoped_overlay_parity(CBMLanguage lang) {
    switch (lang) {
    case CBM_LANG_GO:
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
        return true;
    default:
        return !cbm_pxc_has_cross_lsp(lang);
    }
}

static bool incr_changed_has_scoped_overlay_gap(const cbm_file_info_t *changed_files,
                                                int changed_count) {
    if (!changed_files || changed_count <= 0) {
        return false;
    }
    for (int i = 0; i < changed_count; i++) {
        if (!incr_language_has_scoped_overlay_parity(changed_files[i].language)) {
            return true;
        }
    }
    return false;
}

static bool incr_language_can_attempt_scoped_exact_gap(CBMLanguage lang) {
    return lang == CBM_LANG_PYTHON;
}

static bool incr_changed_has_unsupported_scoped_exact_gap(const cbm_file_info_t *changed_files,
                                                         int changed_count) {
    if (!changed_files || changed_count <= 0) {
        return false;
    }
    for (int i = 0; i < changed_count; i++) {
        CBMLanguage lang = changed_files[i].language;
        if (!incr_language_has_scoped_overlay_parity(lang) &&
            !incr_language_can_attempt_scoped_exact_gap(lang)) {
            return true;
        }
    }
    return false;
}

static bool incr_file_delta_has_type_like_node(const cbm_pipeline_file_delta_t *delta) {
    if (!delta || delta->change_kind == CBM_PIPELINE_DELTA_CHANGE_DELETE) {
        return false;
    }
    for (int i = 0; i < delta->delta.node_count; i++) {
        const cbm_node_t *node = &delta->delta.nodes[i];
        if (cbm_label_is_type_like(node->label)) {
            return true;
        }
    }
    return false;
}

static bool incr_same_stem_impl_exists(const char *path) {
    static const char *const impl_exts[] = {".c", ".cc", ".cpp", ".cxx", ".m", ".mm", ".cu"};
    if (!path || !path[0]) {
        return false;
    }
    const char *dot = strrchr(path, '.');
    if (!dot || dot == path) {
        return false;
    }
    size_t stem_len = (size_t)(dot - path);
    if (stem_len >= CBM_PATH_MAX || stem_len > (size_t)INT_MAX) {
        return false;
    }
    for (size_t i = 0; i < sizeof(impl_exts) / sizeof(impl_exts[0]); i++) {
        char candidate[CBM_PATH_MAX];
        int n = snprintf(candidate, sizeof(candidate), "%.*s%s", (int)stem_len, path,
                         impl_exts[i]);
        if (n < 0 || (size_t)n >= sizeof(candidate)) {
            continue;
        }
        if (cbm_file_exists(candidate)) {
            return true;
        }
    }
    return false;
}

static bool incr_header_overlay_has_type_impl_pair(const cbm_file_info_t *file,
                                                   const cbm_pipeline_file_delta_t *delta) {
    if (!file || !cbm_pipeline_is_c_family_header(file->language, file->rel_path)) {
        return false;
    }
    return incr_file_delta_has_type_like_node(delta) && incr_same_stem_impl_exists(file->path);
}

static bool incr_delta_node_qn_present(const cbm_node_t *nodes, int count, const char *qn) {
    if (!nodes || count <= 0 || !qn) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (nodes[i].qualified_name && strcmp(nodes[i].qualified_name, qn) == 0) {
            return true;
        }
    }
    return false;
}

static void incr_free_additive_delta(cbm_pipeline_file_delta_t *delta) {
    cbm_pipeline_file_delta_free(delta);
}

static int incr_build_additive_delta(const cbm_pipeline_file_delta_t *src,
                                     cbm_pipeline_file_delta_t *out) {
    if (!src || !out || src->change_kind != CBM_PIPELINE_DELTA_CHANGE_UPSERT) {
        return CBM_STORE_ERR;
    }
    memset(out, 0, sizeof(*out));
    out->delta = (cbm_store_file_delta_t){
        .project = src->delta.project,
        .rel_path = src->delta.rel_path,
        .generation = src->delta.generation,
        .file_hash = src->delta.file_hash,
        .file_state = src->delta.file_state,
        .derived_view_name = src->delta.derived_view_name,
        .derived_status = src->delta.derived_status,
    };
    out->change_kind = src->change_kind;

    if (src->delta.node_count > 0) {
        out->nodes = calloc((size_t)src->delta.node_count, sizeof(*out->nodes));
        if (!out->nodes) {
            return CBM_STORE_ERR;
        }
    }
    for (int i = 0; i < src->delta.node_count; i++) {
        const cbm_node_t *node = &src->delta.nodes[i];
        int rc = cbm_pipeline_copy_delta_node(node, &out->nodes[out->delta.node_count]);
        if (rc != CBM_STORE_OK) {
            incr_free_additive_delta(out);
            return rc;
        }
        out->delta.node_count++;
    }

    if (out->delta.node_count == 0) {
        incr_free_additive_delta(out);
        return CBM_STORE_NOT_FOUND;
    }
    if (src->delta.edge_count > 0) {
        out->edges = calloc((size_t)src->delta.edge_count, sizeof(*out->edges));
        if (!out->edges) {
            incr_free_additive_delta(out);
            return CBM_STORE_ERR;
        }
    }
    for (int i = 0; i < src->delta.edge_count; i++) {
        const cbm_store_delta_edge_t *edge = &src->delta.edges[i];
        if (!incr_delta_node_qn_present(out->nodes, out->delta.node_count, edge->source_qn) &&
            !incr_delta_node_qn_present(out->nodes, out->delta.node_count, edge->target_qn)) {
            continue;
        }
        int rc = cbm_pipeline_copy_delta_edge(edge, &out->edges[out->delta.edge_count]);
        if (rc != CBM_STORE_OK) {
            incr_free_additive_delta(out);
            return rc;
        }
        out->delta.edge_count++;
    }

    if (src->delta.export_count > 0) {
        out->exports = calloc((size_t)src->delta.export_count, sizeof(*out->exports));
        if (!out->exports) {
            incr_free_additive_delta(out);
            return CBM_STORE_ERR;
        }
    }
    for (int i = 0; i < src->delta.export_count; i++) {
        const char *qn = src->delta.exports[i].qualified_name;
        out->exports[out->delta.export_count].qualified_name = cbm_strdup(qn ? qn : "");
        if (!out->exports[out->delta.export_count].qualified_name) {
            incr_free_additive_delta(out);
            return CBM_STORE_ERR;
        }
        out->delta.export_count++;
    }

    out->delta.nodes = out->nodes;
    out->delta.edges = out->edges;
    out->delta.exports = out->exports;
    return CBM_STORE_OK;
}

/* ── File classification ─────────────────────────────────────────── */

/* Classify discovered files against stored metadata.
 * Returns a boolean array: changed[i] = true if files[i] needs re-parsing.
 * Caller must free the returned array. */
static bool *classify_files(cbm_store_t *store, const char *project, cbm_file_info_t *files,
                            int file_count, cbm_file_hash_t *stored, int stored_count,
                            const char *pass_fingerprint, int *out_changed, int *out_unchanged,
                            int *out_metadata_only) {
    bool *changed = calloc((size_t)file_count, sizeof(bool));
    if (!changed) {
        return NULL;
    }

    int n_changed = 0;
    int n_unchanged = 0;
    int n_metadata_only = 0;

    /* Build lookup: rel_path -> stored hash */
    CBMHashTable *ht =
        cbm_ht_create(stored_count > 0 ? (size_t)stored_count * PAIR_LEN : CBM_SZ_64);
    if (!ht) {
        free(changed);
        return NULL;
    }
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

        if (st.st_size != h->size) {
            changed[i] = true;
            n_changed++;
        } else if (cbm_pipeline_stat_mtime_ns(&st) != h->mtime_ns) {
            if (cbm_pipeline_file_state_content_matches_current(store, project, &files[i],
                                                                pass_fingerprint)) {
                n_unchanged++;
                n_metadata_only++;
            } else {
                changed[i] = true;
                n_changed++;
            }
        } else if (!cbm_pipeline_file_state_is_current_or_legacy(
                       store, project, &files[i], pass_fingerprint)) {
            changed[i] = true;
            n_changed++;
        } else {
            n_unchanged++;
        }
    }

    cbm_ht_free(ht);
    *out_changed = n_changed;
    *out_unchanged = n_unchanged;
    if (out_metadata_only) {
        *out_metadata_only = n_metadata_only;
    }
    return changed;
}

/* Classify stored files that are absent from current discovery. Returns
 * CBM_STORE_OK on complete classification. The count of truly-deleted files is
 * output via out_deleted_count, and mode-skipped files are collected into
 * out_mode_skipped. Caller frees both output arrays.
 *
 * A stored file is classified as:
 *   - "deleted"      — `stat()` returns ENOENT or ENOTDIR. Its nodes will
 *                       be purged and its hash row dropped.
 *   - "mode-skipped" — `stat()` succeeds. The file exists on disk but the
 *                       current discovery pass didn't visit it (e.g. excluded
 *                       by FAST_SKIP_DIRS in fast/moderate mode). Its nodes
 *                       must be preserved AND its hash row must be carried
 *                       forward into the new DB so subsequent reindexes can
 *                       still see it as "known" rather than treating it as
 *                       new-or-deleted.
 *
 * Without this distinction, a fast-mode reindex after a full-mode index
 * would silently purge every file under `tools/`, `scripts/`, `bin/`,
 * `build/`, `docs/`, `__tests__/`, etc. — see task
 * claude-connectors/codebase-memory-index-repository-is-destructive-...
 * and the 2026-04-13 Skyline incident (packages/mcp/src/tools/ vanished
 * from a live graph mid-session).
 *
 * Mode-skipped hash preservation is the second half of the additive-merge
 * contract: publish_and_persist re-upserts these hash rows so the next reindex
 * can correctly detect a real on-disk deletion of a mode-skipped file (as
 * opposed to seeing it as "never existed" → noop → orphaned graph nodes).
 *
 * Fail-safe rules (preserve nodes on uncertainty):
 *   - repo_path NULL → log error and preserve everything (return OK with 0
 *     deletions and empty mode_skipped). The caller contract is that
 *     repo_path is required; a NULL means a misconfigured pipeline,
 *     not a deletion signal.
 *   - snprintf truncation (combined path ≥ CBM_SZ_4K) → preserve. We can't
 *     reliably stat a truncated path. Treat as mode-skipped.
 *   - stat() errno != ENOENT/ENOTDIR (EACCES, EIO, ELOOP, transient NFS,
 *     etc.) → preserve. The file may exist; we just can't see it right now.
 *     Treat as mode-skipped.
 *
 * Allocation failure is not an uncertainty signal: return CBM_STORE_ERR so the
 * caller can avoid publishing a partial incremental classification.
 *
 * Note: we use stat() (not lstat()) on purpose. A symlink whose target was
 * deleted should be classified as deleted from the indexer's perspective
 * because the indexer follows symlinks during discovery — a stale symlink
 * has no source to parse. */
static int find_deleted_files(const char *repo_path, cbm_file_info_t *files, int file_count,
                              cbm_file_hash_t *stored, int stored_count, char ***out_deleted,
                              int *out_deleted_count, cbm_file_hash_t **out_mode_skipped,
                              int *out_mode_skipped_count) {
    *out_deleted = NULL;
    *out_deleted_count = 0;
    *out_mode_skipped = NULL;
    *out_mode_skipped_count = 0;

    if (!repo_path) {
        /* Misconfigured pipeline. Preserve everything rather than risk
         * silently re-introducing the destructive overwrite this function
         * was rewritten to prevent. */
        cbm_log_error("incremental.err", "msg", "find_deleted_files_null_repo_path");
        return CBM_STORE_OK;
    }

    CBMHashTable *current = cbm_ht_create((size_t)file_count * PAIR_LEN);
    if (!current) {
        cbm_log_error("incremental.err", "msg", "find_deleted_files_current_oom");
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < file_count; i++) {
        cbm_ht_set(current, files[i].rel_path, &files[i]);
    }

    int del_count = 0;
    int del_cap = CBM_SZ_64;
    char **deleted = malloc((size_t)del_cap * sizeof(char *));
    if (!deleted) {
        cbm_log_error("incremental.err", "msg", "find_deleted_files_oom");
        cbm_ht_free(current);
        return CBM_STORE_ERR;
    }

    int ms_count = 0;
    int ms_cap = CBM_SZ_64;
    cbm_file_hash_t *mode_skipped = malloc((size_t)ms_cap * sizeof(cbm_file_hash_t));
    if (!mode_skipped) {
        cbm_log_error("incremental.err", "msg", "find_deleted_files_oom_ms");
        free(deleted);
        cbm_ht_free(current);
        return CBM_STORE_ERR;
    }

    int rc = CBM_STORE_OK;
    if (cbm_pipeline_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_CLASSIFY_DELETED)) {
        cbm_log_error("incremental.err", "phase", CBM_TEST_FAIL_INCREMENTAL_CLASSIFY_DELETED,
                      "rc", itoa_buf_incr(CBM_STORE_ERR));
        rc = CBM_STORE_ERR;
    }
    for (int i = 0; i < stored_count; i++) {
        if (rc != CBM_STORE_OK) {
            break;
        }
        if (cbm_ht_get(current, stored[i].rel_path)) {
            continue; /* still visited by current pass */
        }
        /* Not in current discovery — check if it's truly deleted or just
         * mode-skipped (excluded by FAST_SKIP_DIRS etc.). */
        bool preserve = false;
        char abs_path[CBM_SZ_4K];
        int n = snprintf(abs_path, sizeof(abs_path), "%s/%s", repo_path, stored[i].rel_path);
        if (n < 0 || n >= (int)sizeof(abs_path)) {
            /* Truncation or encoding error — can't reliably stat. Preserve. */
            cbm_log_warn("incremental.path_truncated", "rel_path", stored[i].rel_path);
            preserve = true;
        } else {
            struct stat st;
            if (stat(abs_path, &st) == 0) {
                /* File exists on disk — mode-skipped, not deleted. */
                preserve = true;
            } else if (errno != ENOENT && errno != ENOTDIR) {
                /* Transient or permission error — fail safe by preserving.
                 * EACCES, EIO, ELOOP, ENAMETOOLONG, etc. */
                cbm_log_warn("incremental.stat_uncertain", "rel_path", stored[i].rel_path, "errno",
                             itoa_buf_incr(errno));
                preserve = true;
            }
        }

        if (preserve) {
            /* Carry forward the existing hash row so subsequent reindexes
             * can correctly classify this file. */
            if (ms_count >= ms_cap) {
                if (ms_cap > INT_MAX / PAIR_LEN) {
                    cbm_log_error("incremental.err", "msg", "find_deleted_files_cap_overflow_ms");
                    rc = CBM_STORE_ERR;
                    break;
                }
                int new_cap = ms_cap * PAIR_LEN;
                cbm_file_hash_t *tmp = realloc(mode_skipped, (size_t)new_cap * sizeof(*tmp));
                if (!tmp) {
                    cbm_log_error("incremental.err", "msg", "find_deleted_files_realloc_oom_ms");
                    rc = CBM_STORE_ERR;
                    break;
                }
                mode_skipped = tmp;
                ms_cap = new_cap;
            }
            char *rp = cbm_strdup(stored[i].rel_path);
            char *sh = stored[i].sha256 ? cbm_strdup(stored[i].sha256) : NULL;
            if (!rp || (stored[i].sha256 && !sh)) {
                /* OOM mid-record. Drop this entry rather than persist a
                 * row with a NULL rel_path that would silently fail the
                 * NOT NULL constraint in upsert and reintroduce the
                 * orphaned-node bug. */
                cbm_log_error("incremental.err", "msg", "find_deleted_files_strdup_oom", "rel_path",
                              stored[i].rel_path);
                free(rp);
                free(sh);
                rc = CBM_STORE_ERR;
                break;
            }
            mode_skipped[ms_count].project = NULL; /* unused by upsert API */
            mode_skipped[ms_count].rel_path = rp;
            mode_skipped[ms_count].sha256 = sh;
            mode_skipped[ms_count].mtime_ns = stored[i].mtime_ns;
            mode_skipped[ms_count].size = stored[i].size;
            ms_count++;
            continue;
        }

        /* File is truly gone — record for purge. */
        if (del_count >= del_cap) {
            if (del_cap > INT_MAX / PAIR_LEN) {
                cbm_log_error("incremental.err", "msg", "find_deleted_files_cap_overflow");
                rc = CBM_STORE_ERR;
                break;
            }
            int new_cap = del_cap * PAIR_LEN;
            char **tmp = realloc(deleted, (size_t)new_cap * sizeof(char *));
            if (!tmp) {
                cbm_log_error("incremental.err", "msg", "find_deleted_files_realloc_oom");
                rc = CBM_STORE_ERR;
                break;
            }
            deleted = tmp;
            del_cap = new_cap;
        }
        char *rp = cbm_strdup(stored[i].rel_path);
        if (!rp) {
            cbm_log_error("incremental.err", "msg", "find_deleted_files_strdup_oom", "rel_path",
                          stored[i].rel_path);
            rc = CBM_STORE_ERR;
            break;
        }
        deleted[del_count++] = rp;
    }

    cbm_ht_free(current);
    if (rc != CBM_STORE_OK) {
        free_deleted_paths(deleted, del_count);
        free_mode_skipped(mode_skipped, ms_count);
        return rc;
    }
    *out_deleted = deleted;
    *out_deleted_count = del_count;
    *out_mode_skipped = mode_skipped;
    *out_mode_skipped_count = ms_count;
    return CBM_STORE_OK;
}

/* Free a mode_skipped array allocated by find_deleted_files. */
static void free_mode_skipped(cbm_file_hash_t *ms, int count) {
    if (!ms) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free((void *)ms[i].rel_path);
        free((void *)ms[i].sha256);
    }
    free(ms);
}

static void free_deleted_paths(char **deleted, int count) {
    if (!deleted) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(deleted[i]);
    }
    free(deleted);
}

typedef struct {
    bool *is_changed;
    int n_changed;
    int n_unchanged;
    int n_metadata_only;
    char **deleted;
    int deleted_count;
    cbm_file_hash_t *mode_skipped;
    int mode_skipped_count;
    cbm_file_info_t *changed_files;
    int changed_file_count;
} cbm_incr_classification_t;

static void incr_observe_file_metadata(const cbm_file_info_t *file, int64_t *out_mtime_ns,
                                       int64_t *out_size) {
    if (out_mtime_ns) {
        *out_mtime_ns = 0;
    }
    if (out_size) {
        *out_size = 0;
    }
    if (!file || !file->path) {
        return;
    }
    struct stat st;
    if (stat(file->path, &st) == 0) {
        if (out_mtime_ns) {
            *out_mtime_ns = cbm_pipeline_stat_mtime_ns(&st);
        }
        if (out_size) {
            *out_size = st.st_size;
        }
    }
}

static int incr_mark_dirty_classification(cbm_store_t *store, const char *project,
                                          const cbm_incr_classification_t *cls) {
    if (!store || !project || !project[0] || !cls) {
        return CBM_STORE_ERR;
    }
    int rc = CBM_STORE_OK;
    for (int i = 0; i < cls->changed_file_count; i++) {
        int64_t mtime_ns = 0;
        int64_t size = 0;
        incr_observe_file_metadata(&cls->changed_files[i], &mtime_ns, &size);
        cbm_dirty_file_state_t dirty = {
            .project = project,
            .rel_path = cls->changed_files[i].rel_path,
            .observed_mtime_ns = mtime_ns,
            .observed_size = size,
            .observed_generation = CBM_PIPELINE_COMPAT_GENERATION,
            .source = CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX,
            .status = CBM_STORE_DIRTY_STATUS_PENDING,
        };
        if (cbm_store_upsert_dirty_file(store, &dirty) != CBM_STORE_OK) {
            rc = CBM_STORE_ERR;
        }
    }
    for (int i = 0; i < cls->deleted_count; i++) {
        cbm_dirty_file_state_t dirty = {
            .project = project,
            .rel_path = cls->deleted[i],
            .observed_generation = CBM_PIPELINE_COMPAT_GENERATION,
            .source = CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX,
            .status = CBM_STORE_DIRTY_STATUS_PENDING,
        };
        if (cbm_store_upsert_dirty_file(store, &dirty) != CBM_STORE_OK) {
            rc = CBM_STORE_ERR;
        }
    }
    return rc;
}

static int incr_clear_dirty_classification(cbm_store_t *store, const char *project,
                                           const cbm_incr_classification_t *cls) {
    if (!store || !project || !project[0] || !cls) {
        return CBM_STORE_ERR;
    }
    int rc = CBM_STORE_OK;
    for (int i = 0; i < cls->changed_file_count; i++) {
        if (cbm_store_clear_dirty_file(store, project, cls->changed_files[i].rel_path) !=
            CBM_STORE_OK) {
            rc = CBM_STORE_ERR;
        }
    }
    for (int i = 0; i < cls->deleted_count; i++) {
        if (cbm_store_clear_dirty_file(store, project, cls->deleted[i]) != CBM_STORE_OK) {
            rc = CBM_STORE_ERR;
        }
    }
    return rc;
}

static int incr_clear_dirty_classification_path(const char *db_path, const char *project,
                                                const cbm_incr_classification_t *cls) {
    if (!db_path || !project || !cls) {
        return CBM_STORE_ERR;
    }
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        return CBM_STORE_ERR;
    }
    int rc = incr_clear_dirty_classification(store, project, cls);
    cbm_store_close(store);
    return rc;
}

static void incr_classification_free(cbm_incr_classification_t *c) {
    if (!c) {
        return;
    }
    free(c->is_changed);
    free_deleted_paths(c->deleted, c->deleted_count);
    free_mode_skipped(c->mode_skipped, c->mode_skipped_count);
    free(c->changed_files);
    memset(c, 0, sizeof(*c));
}

static int incr_classification_build(cbm_pipeline_t *p, cbm_store_t *store, const char *project,
                                     cbm_file_info_t *files, int file_count,
                                     cbm_file_hash_t *stored, int stored_count,
                                     const char *pass_fingerprint,
                                     cbm_incr_classification_t *out) {
    if (!p || !out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));

    out->is_changed =
        classify_files(store, project, files, file_count, stored, stored_count, pass_fingerprint,
                       &out->n_changed, &out->n_unchanged, &out->n_metadata_only);
    if (!out->is_changed) {
        cbm_log_error("incremental.err", "msg", "classify_files_oom");
        return CBM_NOT_FOUND;
    }

    if (find_deleted_files(cbm_pipeline_repo_path(p), files, file_count, stored, stored_count,
                           &out->deleted, &out->deleted_count, &out->mode_skipped,
                           &out->mode_skipped_count) != CBM_STORE_OK) {
        cbm_log_error("incremental.err", "msg", "find_deleted_files_failed");
        incr_classification_free(out);
        return CBM_NOT_FOUND;
    }

    if (out->n_changed > 0) {
        out->changed_files = malloc((size_t)out->n_changed * sizeof(*out->changed_files));
        if (!out->changed_files) {
            cbm_log_error("incremental.err", "msg", "changed_files_oom");
            incr_classification_free(out);
            return CBM_NOT_FOUND;
        }
        for (int i = 0; i < file_count; i++) {
            if (out->is_changed[i]) {
                out->changed_files[out->changed_file_count++] = files[i];
            }
        }
    }

    return 0;
}

/* ── Inbound cross-file edge preservation (incremental correctness) ──
 *
 * The purge step (cbm_gbuf_delete_by_file) removes a changed file's nodes,
 * and the cascade then drops every edge referencing them — INCLUDING inbound
 * edges whose source lives in an UNCHANGED file (e.g. StudyService.grade ->
 * SM2.review, or a Folder -> File containment edge). Because incremental only
 * re-parses the changed files, the resolution passes never regenerate those
 * inbound edges, so the graph silently loses cross-file CALLS / USAGE /
 * CONTAINS_FILE / INHERITS / ... edges on every edit and diverges from a
 * clean full reindex (which resolves every file).
 *
 * Fix: snapshot the inbound cross-file edges into changed files BEFORE the
 * purge, keyed by endpoint qualified_name (stable across re-parse), then
 * re-link them AFTER re-resolution + post-passes. Notes:
 *   - Only edges whose target is in a changed file and whose source is NOT
 *     are snapshotted; edges out of a changed file are regenerated when that
 *     file is re-resolved.
 *   - Edge types recomputed wholesale by post-passes (SIMILAR_TO,
 *     SEMANTICALLY_RELATED) are skipped — re-linking a stale snapshot could
 *     add edges a full reindex would not produce.
 *   - cbm_gbuf_insert_edge dedups, so re-linking an edge the resolver already
 *     recreated is a harmless no-op.
 *   - A target whose qualified_name no longer exists (symbol deleted or
 *     renamed by the edit) is dropped — matching full-reindex semantics. */

typedef struct {
    char *source_qn;
    char *target_qn;
    char *type;
    char *props;
} cbm_saved_edge_t;

typedef struct {
    cbm_gbuf_t *gbuf;
    CBMHashTable *changed_paths; /* rel_path -> non-NULL sentinel (membership set) */
    cbm_saved_edge_t *items;
    int count;
    int cap;
} cbm_edge_capture_t;

/* Edge types that must NOT be re-linked from the pre-purge snapshot, because a
 * full reindex (re)computes them via a pass whose result can differ from the
 * snapshot — restoring a stale copy could leave wrong properties or even an
 * edge a full reindex would not produce:
 *   - SIMILAR_TO / SEMANTICALLY_RELATED: rebuilt wholesale by the incremental
 *     post-passes (similarity / semantic_edges) over a drifting corpus.
 *   - FILE_CHANGES_WITH (git-history coupling): produced only by the full
 *     githistory pass and not restored stale during incremental.
 *   - DATA_FLOWS (route data flow): rebuilt by the incremental route refresh,
 *     so stale pre-purge snapshots must not be re-linked afterward.
 * Every other edge type IS safe to re-link, by one of two routes that both
 * match a full reindex: edges re-emitted by the per-file resolution passes that
 * run incrementally (CALLS, USAGE, DEFINES, DEFINES_METHOD, INHERITS,
 * IMPLEMENTS) are deduped on re-link, while structural containment edges
 * (CONTAINS_FILE, CONTAINS_FOLDER) — which the full-only structure pass does
 * NOT regenerate incrementally — are preserved precisely by this snapshot. */
/* cbm_gbuf_foreach_edge visitor: snapshot inbound cross-file edges into
 * changed files so they survive the purge and can be re-linked afterward. */
static void incr_capture_inbound_edge(const cbm_gbuf_edge_t *edge, void *userdata) {
    cbm_edge_capture_t *cap = (cbm_edge_capture_t *)userdata;
    if (cbm_pipeline_delta_edge_type_is_recomputed(edge->type)) {
        return;
    }
    const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(cap->gbuf, edge->source_id);
    const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_id(cap->gbuf, edge->target_id);
    if (!src || !tgt || !src->qualified_name || !tgt->qualified_name || !src->file_path ||
        !tgt->file_path) {
        return;
    }
    /* Keep only edges that the purge would orphan permanently: target is in a
     * changed file (its node is deleted + re-created), source is NOT (its file
     * is never re-parsed, so the resolver won't regenerate the edge). */
    if (!cbm_ht_get(cap->changed_paths, tgt->file_path) ||
        cbm_ht_get(cap->changed_paths, src->file_path)) {
        return;
    }
    if (cap->count >= cap->cap) {
        int ncap = (cap->cap > 0) ? cap->cap * PAIR_LEN : CBM_SZ_64;
        cbm_saved_edge_t *tmp = realloc(cap->items, (size_t)ncap * sizeof(*tmp));
        if (!tmp) {
            cbm_log_warn("incremental.edge_snapshot_oom", "captured", itoa_buf_incr(cap->count));
            return; /* best-effort: stop capturing, keep what we have */
        }
        cap->items = tmp;
        cap->cap = ncap;
    }
    cbm_saved_edge_t *s = &cap->items[cap->count];
    s->source_qn = cbm_strdup(src->qualified_name);
    s->target_qn = cbm_strdup(tgt->qualified_name);
    s->type = cbm_strdup(edge->type);
    s->props = cbm_strdup(edge->properties_json ? edge->properties_json : "{}");
    if (!s->source_qn || !s->target_qn || !s->type || !s->props) {
        free(s->source_qn);
        free(s->target_qn);
        free(s->type);
        free(s->props);
        return;
    }
    cap->count++;
}

/* Re-link snapshotted inbound edges to the freshly re-created target nodes.
 * Returns the number of edges re-linked. */
static int incr_restore_inbound_edges(cbm_gbuf_t *gbuf, cbm_edge_capture_t *cap) {
    int restored = 0;
    for (int i = 0; i < cap->count; i++) {
        cbm_saved_edge_t *s = &cap->items[i];
        const cbm_gbuf_node_t *src = cbm_gbuf_find_by_qn(gbuf, s->source_qn);
        const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(gbuf, s->target_qn);
        if (src && tgt) {
            cbm_gbuf_insert_edge(gbuf, src->id, tgt->id, s->type, s->props);
            restored++;
        }
    }
    return restored;
}

static void incr_free_edge_capture(cbm_edge_capture_t *cap) {
    for (int i = 0; i < cap->count; i++) {
        free(cap->items[i].source_qn);
        free(cap->items[i].target_qn);
        free(cap->items[i].type);
        free(cap->items[i].props);
    }
    free(cap->items);
    cap->items = NULL;
    cap->count = 0;
    cap->cap = 0;
}

/* ── Persist file hashes ─────────────────────────────────────────── */

/* Persist file hash rows for the current discovery and any mode-skipped
 * files preserved from the previous DB.
 *
 * Partial-failure policy: continue writing all rows so one bad row does not
 * discard useful metadata for the others, but return an error summary so the
 * caller can fail closed instead of reporting a successful incremental run with
 * stale classification metadata. */
static int persist_hashes(cbm_store_t *store, const char *project, cbm_file_info_t *files,
                          int file_count, const cbm_file_hash_t *mode_skipped,
                          int mode_skipped_count) {
    int current_failed = 0;
    int ms_failed = 0;

    if (cbm_pipeline_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_HASH_PERSIST)) {
        cbm_log_error("incremental.err", "phase", CBM_TEST_FAIL_INCREMENTAL_HASH_PERSIST, "rc",
                      itoa_buf_incr(CBM_STORE_ERR));
        return CBM_STORE_ERR;
    }

    /* Batch all hash upserts in one transaction: N files -> 1 COMMIT under
     * WAL instead of N autocommit fsyncs (a 10k-file reindex did 10k separate
     * commits). If BEGIN fails (e.g. store busy), fall back to per-row
     * autocommit. The partial-failure policy below is unchanged. */
    bool batched = (cbm_store_begin(store) == CBM_STORE_OK);

    /* Current discovery: re-stat to capture any mtime/size that changed
     * during the run, and write fresh hash rows for visited files. */
    for (int i = 0; i < file_count; i++) {
        struct stat st;
        if (stat(files[i].path, &st) != 0) {
            continue;
        }
        int rc = cbm_store_upsert_file_hash(store, project, files[i].rel_path, "",
                                            cbm_pipeline_stat_mtime_ns(&st), st.st_size);
        if (rc != CBM_STORE_OK) {
            cbm_log_warn("incremental.persist_hash_failed", "scope", "current", "rel_path",
                         files[i].rel_path, "rc", itoa_buf_incr(rc));
            current_failed++;
        }
    }

    /* Mode-skipped (preserved): re-upsert hash rows from the previous DB
     * so the next reindex can still classify these files correctly. Without
     * this, an orphaned-node bug emerges where:
     *   - full mode indexes everything
     *   - fast mode runs and drops mode-skipped hash rows
     *   - file is then deleted on disk
     *   - next reindex's stored hashes don't include the file → noop or
     *     can't detect the deletion → graph nodes for the deleted file
     *     remain forever (or until a destructive rebuild).
     *
     * A failure here is more serious than a current-files failure because
     * it can revive the orphaned-node bug for that specific file. Logged
     * with scope=mode_skipped so the warning is searchable. */
    if (mode_skipped) {
        for (int i = 0; i < mode_skipped_count; i++) {
            int rc =
                cbm_store_upsert_file_hash(store, project, mode_skipped[i].rel_path,
                                           mode_skipped[i].sha256 ? mode_skipped[i].sha256 : "",
                                           mode_skipped[i].mtime_ns, mode_skipped[i].size);
            if (rc != CBM_STORE_OK) {
                cbm_log_warn("incremental.persist_hash_failed", "scope", "mode_skipped", "rel_path",
                             mode_skipped[i].rel_path, "rc", itoa_buf_incr(rc));
                ms_failed++;
            }
        }
    }

    if (batched) {
        int commit_rc = cbm_store_commit(store);
        if (commit_rc != CBM_STORE_OK) {
            cbm_log_warn("incremental.persist_summary", "commit_failed", itoa_buf_incr(commit_rc));
            return commit_rc;
        }
    }

    if (current_failed > 0 || ms_failed > 0) {
        cbm_log_warn("incremental.persist_summary", "current_failed", itoa_buf_incr(current_failed),
                     "mode_skipped_failed", itoa_buf_incr(ms_failed));
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* ── Registry seed visitor ────────────────────────────────────────── */

/* Callback for cbm_gbuf_foreach_node: seed the registry with the existing
 * project's definition symbols so the resolver can match cross-file symbols
 * during incremental. Mirrors the full-index registry contents exactly so an
 * incremental re-resolve picks the same nodes a full reindex would. */
static void registry_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    cbm_registry_t *r = (cbm_registry_t *)userdata;
    if (!cbm_pipeline_label_is_registry_symbol(node->label)) {
        return;
    }
    cbm_registry_add(r, node->name, node->qualified_name, node->label);
}

static void incr_free_result_cache(CBMFileResult **cache, int count) {
    if (!cache) {
        return;
    }
    for (int i = 0; i < count; i++) {
        if (cache[i]) {
            cbm_free_result(cache[i]);
        }
    }
    free(cache);
}

/* Run parallel or sequential extract+resolve for changed files. */
static int run_extract_resolve(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *changed_files, int ci) {
    struct timespec t;

    /* Per-file LSP always runs. Sequential scoped increments run the reusable
     * cross-LSP pass over the changed-file cache only; this is not equivalent
     * to full-repo FAST for languages whose receiver/type resolution needs
     * project-wide defs. Parallel scoped increments pass NULL cross registries
     * below, so their fused cross-LSP step is a no-op. */

#define MIN_FILES_FOR_PARALLEL_INCR 50
    int worker_count = cbm_default_worker_count(true);
    bool use_parallel = (worker_count > SKIP_ONE && ci > MIN_FILES_FOR_PARALLEL_INCR &&
                         !ctx->store_backed_node_lookup);

    if (use_parallel) {
        cbm_log_info("incremental.mode", "mode", "parallel", "workers", itoa_buf_incr(worker_count),
                     "changed", itoa_buf_incr(ci));

        _Atomic int64_t shared_ids;
        atomic_init(&shared_ids, cbm_gbuf_next_id(ctx->gbuf));

        CBMFileResult **cache = (CBMFileResult **)calloc(ci, sizeof(CBMFileResult *));
        if (cache) {
            int rc = 0;
            if (cbm_pipeline_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_EXTRACT)) {
                cbm_log_error("incremental.err", "phase", CBM_TEST_FAIL_INCREMENTAL_EXTRACT, "rc",
                              itoa_buf_incr(CBM_NOT_FOUND));
                incr_free_result_cache(cache, ci);
                return CBM_NOT_FOUND;
            }
            cbm_clock_gettime(CLOCK_MONOTONIC, &t);
            rc = cbm_parallel_extract(ctx, changed_files, ci, cache, &shared_ids, worker_count);
            cbm_gbuf_set_next_id(ctx->gbuf, atomic_load(&shared_ids));
            cbm_log_info("pass.timing", "pass", "incr_extract", "elapsed_ms",
                         itoa_buf_incr((int)elapsed_ms_incr(t)));
            if (rc != 0) {
                cbm_log_error("incremental.err", "phase", "incr_extract", "rc", itoa_buf_incr(rc));
                incr_free_result_cache(cache, ci);
                return rc;
            }

            if (cbm_pipeline_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_REGISTRY)) {
                cbm_log_error("incremental.err", "phase", CBM_TEST_FAIL_INCREMENTAL_REGISTRY, "rc",
                              itoa_buf_incr(CBM_NOT_FOUND));
                incr_free_result_cache(cache, ci);
                return CBM_NOT_FOUND;
            }
            cbm_clock_gettime(CLOCK_MONOTONIC, &t);
            rc = cbm_build_registry_from_cache(ctx, changed_files, ci, cache);
            cbm_log_info("pass.timing", "pass", "incr_registry", "elapsed_ms",
                         itoa_buf_incr((int)elapsed_ms_incr(t)));
            if (rc != 0) {
                cbm_log_error("incremental.err", "phase", "incr_registry", "rc",
                              itoa_buf_incr(rc));
                incr_free_result_cache(cache, ci);
                return rc;
            }
            /* Registry build allocates on the main graph after parallel_extract.
             * Refresh the shared worker ID source before parallel_resolve creates
             * worker-local nodes, or merged buffers can collide with main IDs. */
            atomic_store_explicit(&shared_ids, cbm_gbuf_next_id(ctx->gbuf),
                                  memory_order_relaxed);

            /* Incremental skips cross-file LSP precondition build — it
             * would need all_defs from the full project, not just the
             * changed slice. Per-file LSP (run inside cbm_extract_file)
             * still fires; cross-file resolution is deferred to the
             * next full re-index. Pass NULL/0/NULL to make the fused
             * step in resolve_worker a no-op. */
            if (cbm_pipeline_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_RESOLVE)) {
                cbm_log_error("incremental.err", "phase", CBM_TEST_FAIL_INCREMENTAL_RESOLVE, "rc",
                              itoa_buf_incr(CBM_NOT_FOUND));
                incr_free_result_cache(cache, ci);
                return CBM_NOT_FOUND;
            }
            cbm_clock_gettime(CLOCK_MONOTONIC, &t);
            rc = cbm_parallel_resolve(ctx, changed_files, ci, cache, &shared_ids, worker_count,
                                      NULL, 0, NULL, NULL /* module_def_index */,
                                      NULL /* cross_registries — incremental skips Tier 2 prebuild */);
            cbm_gbuf_set_next_id(ctx->gbuf, atomic_load(&shared_ids));
            cbm_log_info("pass.timing", "pass", "incr_resolve", "elapsed_ms",
                         itoa_buf_incr((int)elapsed_ms_incr(t)));

            incr_free_result_cache(cache, ci);
            if (rc != 0) {
                cbm_log_error("incremental.err", "phase", "incr_resolve", "rc",
                              itoa_buf_incr(rc));
                return rc;
            }
        } else {
            cbm_log_error("incremental.err", "phase", "incr_cache_alloc");
            return CBM_NOT_FOUND;
        }
    } else {
        int rc = 0;
        cbm_log_info("incremental.mode", "mode", "sequential", "changed", itoa_buf_incr(ci));
        if (cbm_pipeline_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_EXTRACT)) {
            cbm_log_error("incremental.err", "phase", CBM_TEST_FAIL_INCREMENTAL_EXTRACT, "rc",
                          itoa_buf_incr(CBM_NOT_FOUND));
            return CBM_NOT_FOUND;
        }
        rc = cbm_pipeline_pass_definitions(ctx, changed_files, ci);
        if (rc != 0) {
            cbm_log_error("incremental.err", "phase", "incr_definitions", "rc",
                          itoa_buf_incr(rc));
            return rc;
        }
        if (ctx->result_cache) {
            cbm_clock_gettime(CLOCK_MONOTONIC, &t);
            rc = cbm_pipeline_pass_lsp_cross(ctx, changed_files, ci, ctx->result_cache);
            cbm_log_info("pass.timing", "pass", "incr_lsp_cross", "elapsed_ms",
                         itoa_buf_incr((int)elapsed_ms_incr(t)));
            if (rc != 0) {
                cbm_log_error("incremental.err", "phase", "incr_lsp_cross", "rc",
                              itoa_buf_incr(rc));
                return rc;
            }
        }
        rc = cbm_pipeline_pass_calls(ctx, changed_files, ci);
        if (rc != 0) {
            cbm_log_error("incremental.err", "phase", "incr_calls", "rc", itoa_buf_incr(rc));
            return rc;
        }
        rc = cbm_pipeline_pass_usages(ctx, changed_files, ci);
        if (rc != 0) {
            cbm_log_error("incremental.err", "phase", "incr_usages", "rc", itoa_buf_incr(rc));
            return rc;
        }
        rc = cbm_pipeline_pass_semantic(ctx, changed_files, ci);
        if (rc != 0) {
            cbm_log_error("incremental.err", "phase", "incr_semantic", "rc", itoa_buf_incr(rc));
            return rc;
        }
    }
    return 0;
}

/* Run post-extraction passes (tests, decorator tags, configlink). */
static int run_postpasses(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *changed_files, int ci,
                          const char *project, bool refresh_global_semantic_edges) {
    struct timespec t;

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    int rc = cbm_pipeline_pass_tests(ctx, changed_files, ci);
    cbm_log_info("pass.timing", "pass", "incr_tests", "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t)));
    if (rc != 0) {
        cbm_log_error("incremental.err", "phase", "incr_tests", "rc", itoa_buf_incr(rc));
        return rc;
    }

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    (void)cbm_pipeline_pass_decorator_tags(ctx->gbuf, project);
    cbm_log_info("pass.timing", "pass", "incr_decorator_tags", "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    (void)cbm_pipeline_pass_configlink(ctx);
    cbm_log_info("pass.timing", "pass", "incr_configlink", "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_clear_route_derived_edges(ctx->gbuf);
    cbm_pipeline_create_route_nodes(ctx->gbuf);
    cbm_log_info("pass.timing", "pass", "incr_route_match", "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));

    /* SIMILAR_TO + SEMANTICALLY_RELATED edges only in moderate/full modes */
    if (refresh_global_semantic_edges && cbm_pipeline_mode_builds_global_semantic_edges(ctx->mode)) {
        /* These passes recompute global derived edge sets over the loaded graph.
         * Clear the previous run's rows first; otherwise repeated incremental
         * updates keep stale pairs whose node ids changed during purge/reparse. */
        cbm_gbuf_delete_edges_by_type(ctx->gbuf, "SIMILAR_TO");
        cbm_gbuf_delete_edges_by_type(ctx->gbuf, "SEMANTICALLY_RELATED");

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_similarity(ctx);
        cbm_log_info("pass.timing", "pass", "incr_similarity", "elapsed_ms",
                     itoa_buf_incr((int)elapsed_ms_incr(t)));
        if (rc != 0) {
            cbm_log_error("incremental.err", "phase", "incr_similarity", "rc",
                          itoa_buf_incr(rc));
            return rc;
        }

        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_pipeline_pass_semantic_edges(ctx);
        cbm_log_info("pass.timing", "pass", "incr_semantic_edges", "elapsed_ms",
                     itoa_buf_incr((int)elapsed_ms_incr(t)));
        if (rc != 0) {
            cbm_log_error("incremental.err", "phase", "incr_semantic_edges", "rc",
                          itoa_buf_incr(rc));
            return rc;
        }
    }
    return 0;
}

static const char *incremental_structure_root_qn(cbm_gbuf_t *gbuf, const char *project) {
    const cbm_gbuf_node_t **branches = NULL;
    int branch_count = 0;
    if (cbm_gbuf_find_by_label(gbuf, "Branch", &branches, &branch_count) == 0 &&
        branch_count > 0 && branches[0]->qualified_name) {
        return branches[0]->qualified_name;
    }
    return project;
}

static void incr_free_file_deltas(cbm_pipeline_file_delta_t *deltas, int count) {
    if (!deltas) {
        return;
    }
    for (int i = 0; i < count; i++) {
        cbm_pipeline_file_delta_free(&deltas[i]);
    }
    free(deltas);
}

static void incr_mark_generation_failed(cbm_store_t *store, const char *project,
                                        int64_t generation) {
    if (store && project && generation > 0) {
        (void)cbm_store_finish_index_generation(store, project, generation,
                                                CBM_STORE_INDEX_STATUS_FAILED);
    }
}

static int incr_try_exact_delete_route(cbm_pipeline_t *p, cbm_store_t *store, const char *db_path,
                                       const char *project, char **deleted, int deleted_count,
                                       int changed_count, int *applied) {
    if (applied) {
        *applied = 0;
    }
    if (!p || !store || !db_path || !project || !deleted || !applied) {
        return CBM_STORE_OK;
    }
    if (changed_count != 0 || deleted_count != 1 ||
        cbm_pipeline_get_mode(p) < CBM_MODE_FAST) {
        return CBM_STORE_OK;
    }
    cbm_pipeline_set_exact_delta_stats(p, deleted_count, -1, -1);

    const char *rel_path = deleted[0];
    if (!rel_path || !rel_path[0]) {
        cbm_pipeline_set_publish_reason(p, "missing_rel_path");
        cbm_log_info("incremental.exact.delete.fallback", "reason", "missing_rel_path");
        return CBM_STORE_OK;
    }

    enum {
        CBM_INCR_DELETE_DELTA_COUNT = 1,
        CBM_INCR_DELETE_PREFLIGHT_GENERATION = CBM_PIPELINE_COMPAT_GENERATION + 1,
    };
    cbm_pipeline_file_delta_t delta = {
        .delta = {.project = project,
                  .rel_path = rel_path,
                  .generation = CBM_INCR_DELETE_PREFLIGHT_GENERATION},
        .change_kind = CBM_PIPELINE_DELTA_CHANGE_DELETE,
    };
    const cbm_pipeline_file_delta_t *delta_ptrs[] = {&delta};
    cbm_pipeline_file_delta_plan_t plan = {0};
    int max_affected_paths = cbm_pipeline_exact_max_affected_paths(p);
    int rc = cbm_pipeline_plan_file_delta_batch(store, delta_ptrs, CBM_INCR_DELETE_DELTA_COUNT,
                                                max_affected_paths, &plan);
    if (rc != CBM_STORE_OK || plan.route != CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE) {
        cbm_pipeline_set_publish_reason(p, plan.reason ? plan.reason : "plan_error");
        cbm_log_info("incremental.exact.delete.fallback", "reason",
                     plan.reason ? plan.reason : "plan_error");
        cbm_pipeline_file_delta_plan_free(&plan);
        return CBM_STORE_OK;
    }
    cbm_pipeline_file_delta_plan_free(&plan);

    if (cbm_pipeline_overlay_publish_small_deltas(p)) {
        int64_t base_generation = 0;
        int64_t overlay_generation = 0;
        rc = cbm_store_latest_complete_index_generation(store, project, &base_generation);
        if (rc == CBM_STORE_OK) {
            CBM_PROF_START(t_overlay_publish);
            rc = cbm_pipeline_publish_overlay_file_delta_batch(
                store, delta_ptrs, CBM_INCR_DELETE_DELTA_COUNT, base_generation,
                CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX, &overlay_generation);
            CBM_PROF_END_N("incremental_exact", "1_delete_publish_overlay", t_overlay_publish,
                           CBM_INCR_DELETE_DELTA_COUNT);
        }
        if (rc == CBM_STORE_OK && overlay_generation > 0) {
            cbm_pipeline_set_committed_counts(p, cbm_store_count_nodes(store, project),
                                              cbm_store_count_edges(store, project));
            cbm_pipeline_set_graph_changed(p, false);
            cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY);
            cbm_pipeline_set_publish_reason(p, NULL);
            cbm_pipeline_set_exact_delta_stats(p, deleted_count, deleted_count, deleted_count);
            cbm_log_info("incremental.overlay.done", "files", "1");
            *applied = 1;
            return CBM_STORE_OK;
        }
        cbm_log_warn("incremental.overlay.fallback", "reason", "publish_error", "rc",
                     itoa_buf_incr(rc));
    }

    int64_t generation = 0;
    rc = cbm_store_reserve_index_generation(store, project, NULL, NULL, &generation);
    if (rc != CBM_STORE_OK || generation <= 0) {
        cbm_pipeline_set_publish_reason(p, "reserve_generation");
        cbm_log_info("incremental.exact.delete.fallback", "reason", "reserve_generation", "rc",
                     itoa_buf_incr(rc));
        return CBM_STORE_OK;
    }
    delta.delta.generation = generation;

    rc = cbm_pipeline_apply_file_delta_batch(store, delta_ptrs, CBM_INCR_DELETE_DELTA_COUNT,
                                             max_affected_paths, &plan);
    if (rc != CBM_STORE_OK || plan.route != CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE) {
        incr_mark_generation_failed(store, project, generation);
        cbm_pipeline_set_publish_reason(p, plan.reason ? plan.reason : "apply_error");
        cbm_log_info("incremental.exact.delete.fallback", "reason",
                     plan.reason ? plan.reason : "apply_error");
        cbm_pipeline_file_delta_plan_free(&plan);
        return CBM_STORE_OK;
    }
    cbm_pipeline_file_delta_plan_free(&plan);

    cbm_pipeline_set_committed_counts(p, cbm_store_count_nodes(store, project),
                                      cbm_store_count_edges(store, project));
    cbm_pipeline_set_graph_changed(p, true);
    cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT);
    cbm_pipeline_set_publish_reason(p, NULL);
    cbm_pipeline_set_exact_delta_stats(p, deleted_count, deleted_count, deleted_count);
    if (cbm_pipeline_repo_path(p) && cbm_artifact_exists(cbm_pipeline_repo_path(p))) {
        (void)cbm_artifact_export(db_path, cbm_pipeline_repo_path(p), project, CBM_ARTIFACT_FAST);
    }
    cbm_log_info("incremental.exact.delete.done", "files", "1");
    *applied = 1;
    return CBM_STORE_OK;
}

static bool incr_file_info_has_rel_path(const cbm_file_info_t *files, int count,
                                        const char *rel_path) {
    if (!files || count <= 0 || !rel_path || !rel_path[0]) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (files[i].rel_path && strcmp(files[i].rel_path, rel_path) == 0) {
            return true;
        }
    }
    return false;
}

static const cbm_file_info_t *incr_find_file_info_by_rel_path(const cbm_file_info_t *files,
                                                              int count,
                                                              const char *rel_path) {
    if (!files || count <= 0 || !rel_path || !rel_path[0]) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        if (files[i].rel_path && strcmp(files[i].rel_path, rel_path) == 0) {
            return &files[i];
        }
    }
    return NULL;
}

static bool incr_empty_source_inbound_edge_is_structure(const cbm_store_inbound_edge_t *edge) {
    return edge && edge->source_rel_path && edge->source_rel_path[0] == '\0' && edge->type &&
           strcmp(edge->type, CBM_PIPELINE_EDGE_CONTAINS_FILE) == 0;
}

static bool incr_inbound_edge_owner_is_exact_file(const cbm_store_inbound_edge_t *edge,
                                                  const cbm_file_info_t *exact_files,
                                                  int exact_count) {
    return edge && edge->source_rel_path && edge->source_rel_path[0] == '\0' &&
           incr_file_info_has_rel_path(exact_files, exact_count, edge->edge_rel_path);
}

static void incr_free_text_array(char **items, int count) {
    if (!items) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(items[i]);
    }
    free(items);
}

static int incr_append_exact_frontier_path(const char *source_rel_path,
                                           const cbm_file_info_t *all_files, int all_file_count,
                                           cbm_file_info_t *exact_files, int *exact_count,
                                           int max_exact_files, const char **out_reason) {
    if (!source_rel_path || !exact_files || !exact_count) {
        if (out_reason) {
            *out_reason = CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR;
        }
        return CBM_STORE_ERR;
    }
    if (incr_file_info_has_rel_path(exact_files, *exact_count, source_rel_path)) {
        return CBM_STORE_OK;
    }
    if (*exact_count >= max_exact_files) {
        if (out_reason) {
            *out_reason = CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE;
        }
        return CBM_STORE_NOT_FOUND;
    }
    const cbm_file_info_t *source_file =
        incr_find_file_info_by_rel_path(all_files, all_file_count, source_rel_path);
    if (!source_file) {
        if (out_reason) {
            *out_reason = CBM_PIPELINE_DELTA_REASON_FRONTIER_REQUIRES_BATCH;
        }
        return CBM_STORE_NOT_FOUND;
    }
    exact_files[(*exact_count)++] = *source_file;
    return CBM_STORE_OK;
}

static const char *incr_path_basename(const char *rel_path) {
    const char *slash = rel_path ? strrchr(rel_path, '/') : NULL;
    return slash ? slash + SKIP_ONE : rel_path;
}

static bool incr_basename_is_package_entry(const char *basename) {
    static const char init_stem[] = "__init__";
    static const char index_stem[] = "index";
    if (!basename || !basename[0]) {
        return false;
    }
    const char *dot = strrchr(basename, '.');
    size_t stem_len = dot ? (size_t)(dot - basename) : strlen(basename);
    return (stem_len == sizeof(init_stem) - SKIP_ONE &&
            memcmp(basename, init_stem, stem_len) == 0) ||
           (stem_len == sizeof(index_stem) - SKIP_ONE &&
            memcmp(basename, index_stem, stem_len) == 0);
}

static char *incr_frontier_import_target_qn(const char *project, const char *rel_path) {
    const char *basename = incr_path_basename(rel_path);
    if (basename && basename != rel_path && incr_basename_is_package_entry(basename)) {
        size_t dir_len = (size_t)(basename - rel_path);
        while (dir_len > 0 && rel_path[dir_len - SKIP_ONE] == '/') {
            dir_len--;
        }
        if (dir_len < CBM_PATH_MAX) {
            char dir[CBM_PATH_MAX];
            memcpy(dir, rel_path, dir_len);
            dir[dir_len] = '\0';
            return cbm_pipeline_fqn_folder(project, dir);
        }
        return NULL;
    }
    return cbm_pipeline_fqn_module(project, rel_path);
}

static int incr_expand_exact_inbound_frontier(cbm_store_t *store, const char *project,
                                              const cbm_file_info_t *all_files, int all_file_count,
                                              cbm_file_info_t *exact_files, int *exact_count,
                                              int max_exact_files, bool recursive,
                                              const char **out_reason) {
    if (out_reason) {
        *out_reason = NULL;
    }
    if (!store || !project || !all_files || all_file_count <= 0 || !exact_files || !exact_count ||
        *exact_count <= 0 || max_exact_files <= 0) {
        if (out_reason) {
            *out_reason = CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR;
        }
        return CBM_STORE_ERR;
    }

    const int original_exact_count = *exact_count;
    bool saw_batch_owned_empty_source_edge = false;
    for (int cursor = 0; cursor < *exact_count && (recursive || cursor < original_exact_count);
         cursor++) {
        const char *rel_path = exact_files[cursor].rel_path;
        cbm_store_inbound_edge_t *edges = NULL;
        int edge_count = 0;
        int rc = cbm_store_list_file_delta_inbound_edges(store, project, rel_path, &edges,
                                                         &edge_count);
        if (rc != CBM_STORE_OK) {
            if (out_reason) {
                *out_reason = CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR;
            }
            return rc;
        }
        if (recursive) {
            for (int i = 0; i < edge_count; i++) {
                const char *source_rel_path = edges[i].source_rel_path;
                if (!source_rel_path || source_rel_path[0] == '\0') {
                    if (incr_empty_source_inbound_edge_is_structure(&edges[i]) ||
                        incr_inbound_edge_owner_is_exact_file(&edges[i], exact_files,
                                                              *exact_count)) {
                        if (!incr_empty_source_inbound_edge_is_structure(&edges[i])) {
                            if (*exact_count > original_exact_count) {
                                if (out_reason) {
                                    *out_reason =
                                        CBM_PIPELINE_DELTA_REASON_INBOUND_EDGES_REQUIRE_FULL;
                                }
                                cbm_store_free_inbound_edges(edges, edge_count);
                                return CBM_STORE_NOT_FOUND;
                            }
                            saw_batch_owned_empty_source_edge = true;
                        }
                        continue;
                    }
                    if (out_reason) {
                        *out_reason = CBM_PIPELINE_DELTA_REASON_INBOUND_EDGES_REQUIRE_FULL;
                    }
                    cbm_store_free_inbound_edges(edges, edge_count);
                    return CBM_STORE_NOT_FOUND;
                }
                if (saw_batch_owned_empty_source_edge) {
                    if (out_reason) {
                        *out_reason = CBM_PIPELINE_DELTA_REASON_INBOUND_EDGES_REQUIRE_FULL;
                    }
                    cbm_store_free_inbound_edges(edges, edge_count);
                    return CBM_STORE_NOT_FOUND;
                }
                rc = incr_append_exact_frontier_path(source_rel_path, all_files, all_file_count,
                                                     exact_files, exact_count, max_exact_files,
                                                     out_reason);
                if (rc != CBM_STORE_OK) {
                    cbm_store_free_inbound_edges(edges, edge_count);
                    return rc;
                }
            }
        }
        cbm_store_free_inbound_edges(edges, edge_count);

        char *module_qn = incr_frontier_import_target_qn(project, rel_path);
        if (!module_qn) {
            if (out_reason) {
                *out_reason = CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR;
            }
            return CBM_STORE_ERR;
        }
        char **importers = NULL;
        int importer_count = 0;
        rc = cbm_store_list_import_edge_source_paths_by_target_qn(store, project, module_qn,
                                                                  &importers, &importer_count);
        free(module_qn);
        if (rc != CBM_STORE_OK) {
            if (out_reason) {
                *out_reason = CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR;
            }
            return rc;
        }
        for (int i = 0; i < importer_count; i++) {
            rc = incr_append_exact_frontier_path(importers[i], all_files, all_file_count,
                                                 exact_files, exact_count, max_exact_files,
                                                 out_reason);
            if (rc != CBM_STORE_OK) {
                incr_free_text_array(importers, importer_count);
                return rc;
            }
        }
        incr_free_text_array(importers, importer_count);
    }
    return CBM_STORE_OK;
}

static int incr_expand_regular_changed_frontier(cbm_store_t *store, const char *project,
                                                const cbm_file_info_t *all_files,
                                                int all_file_count,
                                                cbm_incr_classification_t *cls,
                                                bool allow_expansion) {
    if (!store || !project || !all_files || all_file_count <= 0 || !cls ||
        cls->changed_file_count <= 0) {
        return CBM_STORE_OK;
    }

    cbm_file_info_t *expanded = malloc((size_t)all_file_count * sizeof(*expanded));
    if (!expanded) {
        cbm_log_info("incremental.frontier.fallback", "reason", "alloc");
        return CBM_STORE_NOT_FOUND;
    }
    int expanded_count = cls->changed_file_count;
    for (int i = 0; i < cls->changed_file_count; i++) {
        expanded[i] = cls->changed_files[i];
    }

    for (int cursor = 0; cursor < expanded_count; cursor++) {
        const char *rel_path = expanded[cursor].rel_path;
        char *module_qn = incr_frontier_import_target_qn(project, rel_path);
        if (!module_qn) {
            cbm_log_info("incremental.frontier.fallback", "reason",
                         CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR);
            free(expanded);
            return CBM_STORE_NOT_FOUND;
        }
        char **importers = NULL;
        int importer_count = 0;
        int rc = cbm_store_list_import_edge_source_paths_by_target_qn(store, project, module_qn,
                                                                      &importers, &importer_count);
        free(module_qn);
        if (rc != CBM_STORE_OK) {
            cbm_log_info("incremental.frontier.fallback", "reason",
                         CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR);
            free(expanded);
            return CBM_STORE_NOT_FOUND;
        }
        const char *reason = NULL;
        for (int i = 0; i < importer_count; i++) {
            rc = incr_append_exact_frontier_path(importers[i], all_files, all_file_count, expanded,
                                                 &expanded_count, all_file_count, &reason);
            if (rc != CBM_STORE_OK) {
                cbm_log_info("incremental.frontier.fallback", "reason",
                             reason ? reason : CBM_PIPELINE_DELTA_REASON_FRONTIER_ERROR);
                incr_free_text_array(importers, importer_count);
                free(expanded);
                return CBM_STORE_NOT_FOUND;
            }
        }
        incr_free_text_array(importers, importer_count);
    }

    if (expanded_count > cls->changed_file_count) {
        if (!allow_expansion) {
            cbm_log_info("incremental.frontier.fallback", "reason", "global_derived_edges");
            free(expanded);
            return CBM_STORE_NOT_FOUND;
        }
        cbm_log_info("incremental.frontier", "changed", itoa_buf_incr(cls->changed_file_count),
                     "expanded", itoa_buf_incr(expanded_count));
        free(cls->changed_files);
        cls->changed_files = expanded;
        cls->changed_file_count = expanded_count;
        cls->n_changed = expanded_count;
    } else {
        free(expanded);
    }
    return CBM_STORE_OK;
}

static int incr_try_overlay_upsert_route(cbm_pipeline_t *p, cbm_store_t *store,
                                         const char *project, cbm_file_info_t *changed_files,
                                         int changed_count, int deleted_count,
                                         const char *pass_fingerprint, int *applied) {
    if (applied) {
        *applied = 0;
    }
    if (!p || !store || !project || !changed_files || changed_count <= 0 ||
        !pass_fingerprint || !applied) {
        return CBM_STORE_OK;
    }
    int max_affected_paths = cbm_pipeline_exact_max_affected_paths(p);
    if (!cbm_pipeline_overlay_publish_small_deltas(p) || deleted_count != 0 ||
        changed_count > cbm_pipeline_exact_max_changed_paths(p) ||
        cbm_pipeline_get_mode(p) < CBM_MODE_FAST || changed_count > max_affected_paths) {
        return CBM_STORE_OK;
    }
    if (incr_changed_has_scoped_overlay_gap(changed_files, changed_count)) {
        cbm_pipeline_set_publish_reason(p, "overlay_scoped_lsp_gap");
        cbm_log_info("incremental.overlay.fallback", "reason", "scoped_lsp_gap");
        return CBM_STORE_OK;
    }
    bool c_header_batch = changed_count > 1 &&
                          incr_changed_all_c_family_headers(changed_files, changed_count);
    bool mixed_header_batch = changed_count > 1 && !c_header_batch &&
                              incr_changed_contains_c_family_header(changed_files, changed_count);
    bool additive_header_overlay = c_header_batch;
    if (mixed_header_batch) {
        return CBM_STORE_OK;
    }
    int rc = CBM_STORE_OK;
    const char **changed_paths = NULL;
    cbm_gbuf_t *scratch = NULL;
    cbm_registry_t *registry = NULL;
    cbm_path_alias_collection_t *path_aliases = NULL;
    CBMHashTable *pkgmap = NULL;
    cbm_pipeline_file_delta_t *deltas = NULL;
    cbm_pipeline_file_delta_t *additive_deltas = NULL;
    const cbm_pipeline_file_delta_t **delta_ptrs = NULL;
    const cbm_pipeline_file_delta_t **additive_delta_ptrs = NULL;
    const cbm_store_file_delta_t **additive_store_delta_ptrs = NULL;
    CBMFileResult **result_cache = NULL;
    int64_t base_generation = 0;
    int64_t overlay_generation = 0;

    changed_paths = malloc((size_t)changed_count * sizeof(*changed_paths));
    deltas = calloc((size_t)changed_count, sizeof(*deltas));
    additive_deltas = calloc((size_t)changed_count, sizeof(*additive_deltas));
    delta_ptrs = malloc((size_t)changed_count * sizeof(*delta_ptrs));
    additive_delta_ptrs = malloc((size_t)changed_count * sizeof(*additive_delta_ptrs));
    additive_store_delta_ptrs =
        malloc((size_t)changed_count * sizeof(*additive_store_delta_ptrs));
    result_cache = calloc((size_t)changed_count, sizeof(*result_cache));
    scratch = cbm_gbuf_new(project, cbm_pipeline_repo_path(p));
    registry = cbm_registry_new();
    if (!changed_paths || !deltas || !additive_deltas || !delta_ptrs || !additive_delta_ptrs ||
        !additive_store_delta_ptrs || !result_cache || !scratch || !registry) {
        cbm_pipeline_set_publish_reason(p, "overlay_alloc");
        cbm_log_info("incremental.overlay.fallback", "reason", "alloc");
        goto cleanup;
    }
    for (int i = 0; i < changed_count; i++) {
        if (!changed_files[i].rel_path) {
            cbm_pipeline_set_publish_reason(p, "missing_rel_path");
            cbm_log_info("incremental.overlay.fallback", "reason", "missing_rel_path");
            goto cleanup;
        }
        changed_paths[i] = changed_files[i].rel_path;
    }

    CBM_PROF_START(t_overlay_seed);
    rc = cbm_pipeline_seed_file_delta_scratch_from_store(
        store, scratch, registry, project, changed_paths, changed_count);
    CBM_PROF_END_N("incremental_overlay", "1_seed_scratch", t_overlay_seed, changed_count);
    if (rc != CBM_STORE_OK) {
        cbm_pipeline_set_publish_reason(p, "overlay_scratch_seed");
        cbm_log_info("incremental.overlay.fallback", "reason", "scratch_seed", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }

    path_aliases = cbm_load_path_aliases(cbm_pipeline_repo_path(p));
    pkgmap = cbm_pkgmap_build_from_repo(cbm_pipeline_repo_path(p), changed_files, changed_count,
                                        project);
    cbm_pipeline_set_pkgmap(pkgmap);
    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = cbm_pipeline_repo_path(p),
        .gbuf = scratch,
        .registry = registry,
        .cancelled = cbm_pipeline_cancelled_ptr(p),
        .mode = cbm_pipeline_get_mode(p),
        .similarity_threshold = cbm_pipeline_similarity_threshold(p),
        .httplink_min_confidence = cbm_pipeline_httplink_min_confidence(p),
        .semantic_threshold = cbm_pipeline_semantic_threshold(p),
        .githistory_min_coupling = cbm_pipeline_githistory_min_coupling(p),
        .lsp_confidence_floor = cbm_pipeline_lsp_confidence_floor(p),
        .path_aliases = path_aliases,
        .pkgmap_preseeded = true,
        .result_cache = result_cache,
        .store_backed_node_lookup = store,
        .store_backed_changed_paths = changed_paths,
        .store_backed_changed_path_count = changed_count,
    };

    const char *structure_root_qn = incremental_structure_root_qn(scratch, project);
    for (int i = 0; i < changed_count; i++) {
        rc = cbm_pipeline_ensure_file_structure(scratch, project, structure_root_qn,
                                                changed_files[i].rel_path, NULL);
        if (rc != 0) {
            cbm_pipeline_set_publish_reason(p, "overlay_ensure_structure");
            cbm_log_info("incremental.overlay.fallback", "reason", "ensure_structure", "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
    }

    rc = run_extract_resolve(&ctx, changed_files, changed_count);
    if (rc != 0) {
        cbm_pipeline_set_publish_reason(p, "overlay_extract_resolve");
        cbm_log_info("incremental.overlay.fallback", "reason", "extract_resolve", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }
    rc = cbm_pipeline_pass_k8s(&ctx, changed_files, changed_count);
    if (rc != 0) {
        cbm_pipeline_set_publish_reason(p, "overlay_k8s");
        cbm_log_info("incremental.overlay.fallback", "reason", "k8s", "rc", itoa_buf_incr(rc));
        goto cleanup;
    }
    rc = run_postpasses(&ctx, changed_files, changed_count, project, true);
    if (rc != 0) {
        cbm_pipeline_set_publish_reason(p, "overlay_postpasses");
        cbm_log_info("incremental.overlay.fallback", "reason", "postpasses", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }
    cbm_pipeline_pass_complexity_for_paths(&ctx, changed_paths, changed_count);
    rc = cbm_pipeline_pass_httplinks(&ctx);
    if (rc != 0) {
        cbm_pipeline_set_publish_reason(p, "overlay_httplinks");
        cbm_log_info("incremental.overlay.fallback", "reason", "httplinks", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }
    cbm_pipeline_pass_normalize(scratch);

    for (int i = 0; i < changed_count; i++) {
        rc = cbm_pipeline_build_file_delta_from_gbuf(scratch, project, changed_files[i].rel_path,
                                                     CBM_PIPELINE_COMPAT_GENERATION, &deltas[i]);
        if (rc != CBM_STORE_OK) {
            cbm_pipeline_set_publish_reason(p, "overlay_build_delta");
            cbm_log_info("incremental.overlay.fallback", "reason", "build_delta", "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
        rc = cbm_pipeline_attach_file_delta_metadata_with_fingerprint(&deltas[i],
                                                                      &changed_files[i],
                                                                      pass_fingerprint);
        if (rc != CBM_STORE_OK) {
            cbm_pipeline_set_publish_reason(p, "overlay_metadata");
            cbm_log_info("incremental.overlay.fallback", "reason", "metadata", "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
        if (incr_header_overlay_has_type_impl_pair(&changed_files[i], &deltas[i])) {
            additive_header_overlay = true;
        }
        if (!additive_header_overlay) {
            int preserved = 0;
            rc = cbm_pipeline_file_delta_add_preserved_inbound_edges(store, &deltas[i], &preserved);
            if (rc != CBM_STORE_OK) {
                cbm_pipeline_set_publish_reason(p, "overlay_preserve_inbound");
                cbm_log_info("incremental.overlay.fallback", "reason", "preserve_inbound", "rc",
                             itoa_buf_incr(rc));
                goto cleanup;
            }
        }
        delta_ptrs[i] = &deltas[i];
    }

    if (additive_header_overlay) {
        for (int i = 0; i < changed_count; i++) {
            rc = incr_build_additive_delta(&deltas[i], &additive_deltas[i]);
            if (rc != CBM_STORE_OK) {
                const char *reason = rc == CBM_STORE_NOT_FOUND ? "overlay_no_additive_facts"
                                                               : "overlay_additive_filter";
                cbm_pipeline_set_publish_reason(p, reason);
                cbm_log_info("incremental.overlay.fallback", "reason", reason, "rc",
                             itoa_buf_incr(rc));
                goto cleanup;
            }
            additive_delta_ptrs[i] = &additive_deltas[i];
            additive_store_delta_ptrs[i] = &additive_deltas[i].delta;
        }
        bool preserves_owned_graph = false;
        rc = cbm_store_file_delta_batch_preserves_owned_graph(
            store, additive_store_delta_ptrs, changed_count, &preserves_owned_graph);
        if (rc != CBM_STORE_OK || !preserves_owned_graph) {
            const char *reason = rc == CBM_STORE_OK
                                     ? CBM_PIPELINE_DELTA_REASON_ADDITIVE_SUBSET_REQUIRED
                                     : CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR;
            cbm_pipeline_set_publish_reason(p, reason);
            cbm_log_info("incremental.overlay.fallback", "reason", reason, "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
    } else {
        for (int i = 0; i < changed_count; i++) {
            bool collision = false;
            rc = cbm_pipeline_file_delta_has_cross_file_node_qn_collision(store, &deltas[i],
                                                                          &collision);
            if (rc != CBM_STORE_OK || collision) {
                const char *reason =
                    collision ? CBM_PIPELINE_DELTA_REASON_CROSS_FILE_NODE_QN_COLLISION
                              : CBM_PIPELINE_DELTA_REASON_PREFLIGHT_ERROR;
                cbm_pipeline_set_publish_reason(p, reason);
                cbm_log_info("incremental.overlay.fallback", "reason", reason, "rc",
                             itoa_buf_incr(rc));
                goto cleanup;
            }
        }
    }

    rc = cbm_store_latest_complete_index_generation(store, project, &base_generation);
    if (rc == CBM_STORE_OK) {
        CBM_PROF_START(t_overlay_publish);
        rc = additive_header_overlay
                 ? cbm_pipeline_publish_overlay_file_delta_additions_batch(
                       store, additive_delta_ptrs, changed_count, base_generation,
                       CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX, &overlay_generation)
                 : cbm_pipeline_publish_overlay_file_delta_batch(
                       store, delta_ptrs, changed_count, base_generation,
                       CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX, &overlay_generation);
        CBM_PROF_END_N("incremental_overlay", "2_publish_overlay", t_overlay_publish,
                       changed_count);
    }
    if (rc == CBM_STORE_OK && overlay_generation > 0) {
        cbm_pipeline_set_committed_counts(p, cbm_store_count_nodes(store, project),
                                          cbm_store_count_edges(store, project));
        cbm_pipeline_set_graph_changed(p, false);
        cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY);
        cbm_pipeline_set_publish_reason(p, NULL);
        cbm_pipeline_set_exact_delta_stats(p, changed_count, changed_count, changed_count);
        cbm_log_info("incremental.overlay.done", "files", itoa_buf_incr(changed_count));
        *applied = 1;
        goto cleanup;
    }
    cbm_pipeline_set_publish_reason(p, "overlay_publish_error");
    cbm_log_warn("incremental.overlay.fallback", "reason", "publish_error", "rc",
                 itoa_buf_incr(rc));

cleanup:
    free(additive_store_delta_ptrs);
    free(additive_delta_ptrs);
    free(delta_ptrs);
    incr_free_file_deltas(additive_deltas, changed_count);
    incr_free_file_deltas(deltas, changed_count);
    incr_free_result_cache(result_cache, changed_count);
    cbm_path_alias_collection_free(path_aliases);
    if (cbm_pipeline_get_pkgmap() == pkgmap) {
        cbm_pipeline_set_pkgmap(NULL);
    }
    cbm_pkgmap_free(pkgmap);
    cbm_registry_free(registry);
    cbm_gbuf_free(scratch);
    free(changed_paths);
    return CBM_STORE_OK;
}

static void incr_report_exact_delta_plan_fallback(cbm_pipeline_t *p,
                                                  const cbm_pipeline_file_delta_plan_t *plan,
                                                  int input_path_count, int max_affected_paths,
                                                  const char *phase,
                                                  const char *default_reason) {
    const char *reason = plan && plan->reason ? plan->reason : default_reason;
    int affected_paths = (plan && plan->affected_count >= 0) ? plan->affected_count : -1;
    cbm_pipeline_set_exact_delta_stats_with_limit(p, input_path_count, affected_paths, -1,
                                                  max_affected_paths, false);
    cbm_pipeline_set_publish_reason(p, reason ? reason : "plan_error");
    if (affected_paths >= 0) {
        cbm_log_info("incremental.exact.fallback", "reason", reason ? reason : "plan_error",
                     "phase", phase ? phase : "plan", "affected", itoa_buf_incr(affected_paths),
                     "max_affected", itoa_buf_incr(max_affected_paths));
    } else {
        cbm_log_info("incremental.exact.fallback", "reason", reason ? reason : "plan_error",
                     "phase", phase ? phase : "plan");
    }
}

static int incr_try_exact_upsert_route(cbm_pipeline_t *p, cbm_store_t *store, const char *db_path,
                                       const char *project, cbm_file_info_t *changed_files,
                                       int changed_count, cbm_file_info_t *all_files,
                                       int all_file_count, char **deleted, int deleted_count,
                                       const char *pass_fingerprint, int *applied) {
    if (applied) {
        *applied = 0;
    }
    if (!p || !store || !db_path || !project || !changed_files || changed_count <= 0 ||
        !all_files || all_file_count <= 0 || !pass_fingerprint || !applied) {
        return CBM_STORE_OK;
    }
    int max_changed_paths = cbm_pipeline_exact_max_changed_paths(p);
    int max_affected_paths = cbm_pipeline_exact_max_affected_paths(p);
    int input_path_count = changed_count + deleted_count;
    cbm_pipeline_set_exact_delta_stats(p, input_path_count, -1, -1);
    bool scoped_overlay_gap = incr_changed_has_scoped_overlay_gap(changed_files, changed_count);
    bool scoped_exact_gap = scoped_overlay_gap;
    bool unsupported_scoped_exact_gap =
        incr_changed_has_unsupported_scoped_exact_gap(changed_files, changed_count);
    bool exact_deferred_global_derived =
        cbm_pipeline_get_mode(p) < CBM_MODE_FAST &&
        cbm_pipeline_incremental_derived_refresh_stale_on_exact(p);
    if (unsupported_scoped_exact_gap) {
        cbm_pipeline_set_exact_delta_stats_with_limit(
            p, input_path_count, input_path_count, -1, max_affected_paths, true);
        cbm_pipeline_set_publish_reason(p, CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE);
        cbm_log_info("incremental.exact.skip", "reason", "scoped_lsp_gap", "action",
                     "full_reindex");
        return CBM_STORE_OK;
    }
    if (deleted_count < 0 || changed_count > max_changed_paths ||
        (cbm_pipeline_get_mode(p) < CBM_MODE_FAST && !exact_deferred_global_derived) ||
        input_path_count > max_affected_paths) {
        const char *reason =
            changed_count > max_changed_paths
                ? "changed_batch_too_large"
                : (input_path_count > max_affected_paths
                       ? CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE
                       : "global_derived_edges");
        cbm_pipeline_set_publish_reason(p, reason);
        cbm_log_info("incremental.exact.skip", "reason", reason);
        return CBM_STORE_OK;
    }

    int exact_file_cap = max_affected_paths - deleted_count;
    if (exact_file_cap <= 0) {
        cbm_pipeline_set_publish_reason(p, CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE);
        cbm_log_info("incremental.exact.skip", "reason",
                     CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE);
        return CBM_STORE_OK;
    }
    cbm_file_info_t *exact_files = malloc((size_t)exact_file_cap * sizeof(*exact_files));
    if (!exact_files) {
        cbm_pipeline_set_publish_reason(p, "alloc");
        cbm_log_info("incremental.exact.fallback", "reason", "alloc");
        return CBM_STORE_OK;
    }
    int exact_count = changed_count;
    for (int i = 0; i < changed_count; i++) {
        exact_files[i] = changed_files[i];
    }
    const char *frontier_reason = NULL;
    int frontier_rc = incr_expand_exact_inbound_frontier(store, project, all_files, all_file_count,
                                                        exact_files, &exact_count, exact_file_cap,
                                                        !scoped_exact_gap, &frontier_reason);
    if (frontier_rc != CBM_STORE_OK) {
        bool frontier_truncated =
            frontier_reason && strcmp(frontier_reason,
                                      CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE) == 0;
        cbm_pipeline_set_exact_delta_stats_with_limit(
            p, input_path_count, exact_count + deleted_count, -1, max_affected_paths,
            frontier_truncated);
        cbm_pipeline_set_publish_reason(
            p, frontier_reason ? frontier_reason : CBM_PIPELINE_DELTA_REASON_FRONTIER_ERROR);
        if (frontier_truncated) {
            cbm_log_info("incremental.exact.fallback", "reason",
                         frontier_reason ? frontier_reason
                                         : CBM_PIPELINE_DELTA_REASON_FRONTIER_ERROR,
                         "affected", itoa_buf_incr(exact_count + deleted_count),
                         "max_affected", itoa_buf_incr(max_affected_paths), "truncated", "true");
        } else {
            cbm_log_info("incremental.exact.fallback", "reason",
                         frontier_reason ? frontier_reason
                                         : CBM_PIPELINE_DELTA_REASON_FRONTIER_ERROR);
        }
        free(exact_files);
        return CBM_STORE_OK;
    }
    if (exact_count != changed_count) {
        cbm_log_info("incremental.exact.frontier", "changed", itoa_buf_incr(changed_count),
                     "expanded", itoa_buf_incr(exact_count));
    }

    int delta_count = exact_count + deleted_count;
    cbm_pipeline_set_exact_delta_stats(p, input_path_count, delta_count, -1);
    int rc = CBM_STORE_OK;
    const char **changed_paths = NULL;
    cbm_gbuf_t *scratch = NULL;
    cbm_registry_t *registry = NULL;
    cbm_path_alias_collection_t *path_aliases = NULL;
    CBMHashTable *pkgmap = NULL;
    cbm_pipeline_file_delta_t *deltas = NULL;
    const cbm_pipeline_file_delta_t **delta_ptrs = NULL;
    const cbm_store_file_delta_t **store_delta_ptrs = NULL;
    CBMFileResult **result_cache = NULL;
    bool *frontier_noop_mask = NULL;
    cbm_pipeline_file_delta_plan_t plan = {0};
    int64_t generation = 0;
    bool graph_noop_candidate = false;
    int exact_publish_count = delta_count;

    changed_paths = malloc((size_t)exact_count * sizeof(*changed_paths));
    deltas = calloc((size_t)delta_count, sizeof(*deltas));
    delta_ptrs = malloc((size_t)delta_count * sizeof(*delta_ptrs));
    store_delta_ptrs = malloc((size_t)delta_count * sizeof(*store_delta_ptrs));
    result_cache = calloc((size_t)exact_count, sizeof(*result_cache));
    if (scoped_exact_gap) {
        frontier_noop_mask = calloc((size_t)delta_count, sizeof(*frontier_noop_mask));
    }
    scratch = cbm_gbuf_new(project, cbm_pipeline_repo_path(p));
    registry = cbm_registry_new();
    if (!changed_paths || !deltas || !delta_ptrs || !store_delta_ptrs || !result_cache ||
        (scoped_exact_gap && !frontier_noop_mask) || !scratch || !registry) {
        cbm_pipeline_set_publish_reason(p, "alloc");
        cbm_log_info("incremental.exact.fallback", "reason", "alloc");
        goto cleanup;
    }
    for (int i = 0; i < exact_count; i++) {
        if (!exact_files[i].rel_path) {
            cbm_pipeline_set_publish_reason(p, "missing_rel_path");
            cbm_log_info("incremental.exact.fallback", "reason", "missing_rel_path");
            goto cleanup;
        }
        changed_paths[i] = exact_files[i].rel_path;
    }
    for (int i = 0; i < deleted_count; i++) {
        if (!deleted || !deleted[i] || !deleted[i][0]) {
            cbm_pipeline_set_publish_reason(p, "missing_rel_path");
            cbm_log_info("incremental.exact.fallback", "reason", "missing_rel_path");
            goto cleanup;
        }
    }

    CBM_PROF_START(t_exact_seed);
    rc = cbm_pipeline_seed_file_delta_scratch_from_store(
        store, scratch, registry, project, changed_paths, exact_count);
    CBM_PROF_END_N("incremental_exact", "1_seed_scratch", t_exact_seed, exact_count);
    if (rc != CBM_STORE_OK) {
        cbm_pipeline_set_publish_reason(p, "scratch_seed");
        cbm_log_info("incremental.exact.fallback", "reason", "scratch_seed", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }

    path_aliases = cbm_load_path_aliases(cbm_pipeline_repo_path(p));
    pkgmap = cbm_pkgmap_build_from_repo(cbm_pipeline_repo_path(p), exact_files, exact_count,
                                        project);
    cbm_pipeline_set_pkgmap(pkgmap);
    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = cbm_pipeline_repo_path(p),
        .gbuf = scratch,
        .registry = registry,
        .cancelled = cbm_pipeline_cancelled_ptr(p),
        .mode = cbm_pipeline_get_mode(p),
        .similarity_threshold = cbm_pipeline_similarity_threshold(p),
        .httplink_min_confidence = cbm_pipeline_httplink_min_confidence(p),
        .semantic_threshold = cbm_pipeline_semantic_threshold(p),
        .githistory_min_coupling = cbm_pipeline_githistory_min_coupling(p),
        .lsp_confidence_floor = cbm_pipeline_lsp_confidence_floor(p),
        .path_aliases = path_aliases,
        .pkgmap_preseeded = true,
        .result_cache = result_cache,
        .store_backed_node_lookup = store,
        .store_backed_changed_paths = changed_paths,
        .store_backed_changed_path_count = exact_count,
        .store_backed_all_files = all_files,
        .store_backed_all_file_count = all_file_count,
        .store_backed_lsp_scope_cap = CBM_PIPELINE_STORE_BACKED_LSP_SCOPE_DEFAULT_CAP,
    };

    const char *structure_root_qn = incremental_structure_root_qn(scratch, project);
    CBM_PROF_START(t_exact_structure);
    for (int i = 0; i < exact_count; i++) {
        rc = cbm_pipeline_ensure_file_structure(scratch, project, structure_root_qn,
                                                exact_files[i].rel_path, NULL);
        if (rc != 0) {
            cbm_pipeline_set_publish_reason(p, "ensure_structure");
            cbm_log_info("incremental.exact.fallback", "reason", "ensure_structure", "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
    }
    CBM_PROF_END_N("incremental_exact", "2_ensure_structure", t_exact_structure, exact_count);
    CBM_PROF_START(t_exact_extract_resolve);
    rc = run_extract_resolve(&ctx, exact_files, exact_count);
    CBM_PROF_END_N("incremental_exact", "3_extract_resolve", t_exact_extract_resolve, exact_count);
    if (rc != 0) {
        cbm_pipeline_set_publish_reason(p, "extract_resolve");
        cbm_log_info("incremental.exact.fallback", "reason", "extract_resolve", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }
    CBM_PROF_START(t_exact_k8s);
    rc = cbm_pipeline_pass_k8s(&ctx, exact_files, exact_count);
    CBM_PROF_END_N("incremental_exact", "4_k8s", t_exact_k8s, exact_count);
    if (rc != 0) {
        cbm_pipeline_set_publish_reason(p, "k8s");
        cbm_log_info("incremental.exact.fallback", "reason", "k8s", "rc", itoa_buf_incr(rc));
        goto cleanup;
    }
    CBM_PROF_START(t_exact_postpasses);
    rc = run_postpasses(&ctx, exact_files, exact_count, project,
                        !exact_deferred_global_derived);
    CBM_PROF_END_N("incremental_exact", "5_postpasses", t_exact_postpasses, exact_count);
    if (rc != 0) {
        cbm_pipeline_set_publish_reason(p, "postpasses");
        cbm_log_info("incremental.exact.fallback", "reason", "postpasses", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }
    CBM_PROF_START(t_exact_complexity);
    cbm_pipeline_pass_complexity_for_paths(&ctx, changed_paths, exact_count);
    CBM_PROF_END_N("incremental_exact", "6_complexity", t_exact_complexity, exact_count);
    CBM_PROF_START(t_exact_httplinks);
    rc = cbm_pipeline_pass_httplinks(&ctx);
    CBM_PROF_END_N("incremental_exact", "7_httplinks", t_exact_httplinks, exact_count);
    if (rc != 0) {
        cbm_pipeline_set_publish_reason(p, "httplinks");
        cbm_log_info("incremental.exact.fallback", "reason", "httplinks", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }
    CBM_PROF_START(t_exact_normalize);
    cbm_pipeline_pass_normalize(scratch);
    CBM_PROF_END_N("incremental_exact", "8_normalize", t_exact_normalize, exact_count);

    CBM_PROF_START(t_exact_build_delta);
    for (int i = 0; i < exact_count; i++) {
        rc = cbm_pipeline_build_file_delta_from_gbuf(scratch, project, exact_files[i].rel_path,
                                                     CBM_PIPELINE_COMPAT_GENERATION, &deltas[i]);
        if (rc != CBM_STORE_OK) {
            cbm_pipeline_set_publish_reason(p, "build_delta");
            cbm_log_info("incremental.exact.fallback", "reason", "build_delta", "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
        rc = cbm_pipeline_attach_file_delta_metadata_with_fingerprint(&deltas[i],
                                                                      &exact_files[i],
                                                                      pass_fingerprint);
        if (rc != CBM_STORE_OK) {
            cbm_pipeline_set_publish_reason(p, "metadata");
            cbm_log_info("incremental.exact.fallback", "reason", "metadata", "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
        if (scoped_exact_gap && i < changed_count) {
            int preserved = 0;
            rc = cbm_pipeline_file_delta_add_preserved_inbound_edges(store, &deltas[i],
                                                                     &preserved);
            if (rc != CBM_STORE_OK) {
                cbm_pipeline_set_publish_reason(p, "preserve_inbound");
                cbm_log_info("incremental.exact.fallback", "reason", "preserve_inbound", "rc",
                             itoa_buf_incr(rc));
                goto cleanup;
            }
        }
        delta_ptrs[i] = &deltas[i];
        store_delta_ptrs[i] = &deltas[i].delta;
        if (scoped_exact_gap && i >= changed_count) {
            bool equal = false;
            const cbm_store_file_delta_t *single_delta = &deltas[i].delta;
            rc = cbm_store_file_delta_batch_graph_equal(store, &single_delta, 1, &equal);
            if (rc != CBM_STORE_OK) {
                cbm_pipeline_set_publish_reason(p, "graph_equal");
                cbm_log_info("incremental.exact.fallback", "reason", "graph_equal", "rc",
                             itoa_buf_incr(rc));
                goto cleanup;
            }
            frontier_noop_mask[i] = equal;
        }
    }
    if (scoped_exact_gap) {
        exact_publish_count = 0;
        for (int i = 0; i < delta_count; i++) {
            if (!frontier_noop_mask[i]) {
                exact_publish_count++;
            }
        }
    }
    for (int i = 0; i < deleted_count; i++) {
        int delta_index = exact_count + i;
        deltas[delta_index] =
            (cbm_pipeline_file_delta_t){.delta = {.project = project,
                                                  .rel_path = deleted[i],
                                                  .generation = CBM_PIPELINE_COMPAT_GENERATION + 1},
                                        .change_kind = CBM_PIPELINE_DELTA_CHANGE_DELETE};
        delta_ptrs[delta_index] = &deltas[delta_index];
        store_delta_ptrs[delta_index] = &deltas[delta_index].delta;
    }
    CBM_PROF_END_N("incremental_exact", "9_build_deltas", t_exact_build_delta, exact_count);

    if (deleted_count == 0) {
        CBM_PROF_START(t_exact_graph_equal);
        rc = cbm_store_file_delta_batch_graph_equal(store, store_delta_ptrs, delta_count,
                                                    &graph_noop_candidate);
        CBM_PROF_END_N("incremental_exact", "10b_graph_equal", t_exact_graph_equal, delta_count);
        if (rc != CBM_STORE_OK) {
            cbm_pipeline_set_publish_reason(p, "graph_equal");
            cbm_log_info("incremental.exact.fallback", "reason", "graph_equal", "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
    }

    if (!graph_noop_candidate) {
        CBM_PROF_START(t_exact_plan);
        rc = cbm_pipeline_plan_file_delta_batch_with_frontier_noop_mask(
            store, delta_ptrs, scoped_exact_gap ? frontier_noop_mask : NULL, delta_count,
            max_affected_paths, &plan);
        CBM_PROF_END_N("incremental_exact", "10_plan_delta", t_exact_plan, delta_count);
        if (rc != CBM_STORE_OK || plan.route != CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE) {
            incr_report_exact_delta_plan_fallback(p, &plan, input_path_count, max_affected_paths,
                                                  "plan", "plan_error");
            goto cleanup;
        }
        cbm_pipeline_file_delta_plan_free(&plan);
    }

    const char *prior_reason = cbm_pipeline_publish_reason(p);
    bool overlay_publish_already_failed =
        prior_reason && strcmp(prior_reason, "overlay_publish_error") == 0;
    bool header_overlay_unsafe = incr_changed_contains_c_family_header(changed_files, changed_count);
    if (!graph_noop_candidate && !header_overlay_unsafe && !scoped_overlay_gap &&
        cbm_pipeline_overlay_publish_small_deltas(p) &&
        !overlay_publish_already_failed) {
        int64_t base_generation = 0;
        int64_t overlay_generation = 0;
        rc = cbm_store_latest_complete_index_generation(store, project, &base_generation);
        if (rc == CBM_STORE_OK) {
            CBM_PROF_START(t_overlay_publish);
            rc = cbm_pipeline_publish_overlay_file_delta_batch(
                store, delta_ptrs, delta_count, base_generation,
                CBM_STORE_DIRTY_SOURCE_EXPLICIT_REINDEX, &overlay_generation);
            CBM_PROF_END_N("incremental_exact", "11_publish_overlay", t_overlay_publish,
                           delta_count);
        }
        if (rc == CBM_STORE_OK && overlay_generation > 0) {
            cbm_pipeline_set_committed_counts(p, cbm_store_count_nodes(store, project),
                                              cbm_store_count_edges(store, project));
            cbm_pipeline_set_graph_changed(p, false);
            cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY);
            cbm_pipeline_set_publish_reason(p, NULL);
            cbm_pipeline_set_exact_delta_stats(p, input_path_count, delta_count, delta_count);
            cbm_log_info("incremental.overlay.done", "files", itoa_buf_incr(delta_count));
            *applied = 1;
            goto cleanup;
        }
        cbm_log_warn("incremental.overlay.fallback", "reason", "publish_error", "rc",
                     itoa_buf_incr(rc));
    }

    CBM_PROF_START(t_exact_reserve);
    rc = cbm_store_reserve_index_generation(store, project, NULL, NULL, &generation);
    CBM_PROF_END("incremental_exact", "11_reserve_generation", t_exact_reserve);
    if (rc != CBM_STORE_OK || generation <= 0) {
        cbm_pipeline_set_publish_reason(p, "reserve_generation");
        cbm_log_info("incremental.exact.fallback", "reason", "reserve_generation", "rc",
                     itoa_buf_incr(rc));
        goto cleanup;
    }
    CBM_PROF_START(t_exact_stamp);
    for (int i = 0; i < delta_count; i++) {
        if (deltas[i].change_kind == CBM_PIPELINE_DELTA_CHANGE_DELETE) {
            deltas[i].delta.generation = generation;
        } else {
            rc = cbm_pipeline_file_delta_stamp_generation(&deltas[i], generation);
            if (rc != CBM_STORE_OK) {
                incr_mark_generation_failed(store, project, generation);
                cbm_pipeline_set_publish_reason(p, "stamp_generation");
                cbm_log_info("incremental.exact.fallback", "reason", "stamp_generation", "rc",
                             itoa_buf_incr(rc));
                goto cleanup;
            }
        }
    }
    CBM_PROF_END_N("incremental_exact", "12_stamp_generation", t_exact_stamp, delta_count);

    if (graph_noop_candidate) {
        CBM_PROF_START(t_exact_noop);
        rc = cbm_store_refresh_file_delta_metadata_batch_complete(store, store_delta_ptrs,
                                                                  delta_count);
        CBM_PROF_END_N("incremental_exact", "12b_refresh_metadata", t_exact_noop, delta_count);
        if (rc != CBM_STORE_OK) {
            incr_mark_generation_failed(store, project, generation);
            cbm_pipeline_set_publish_reason(p, "metadata_refresh");
            cbm_log_info("incremental.exact.fallback", "reason", "metadata_refresh", "rc",
                         itoa_buf_incr(rc));
            goto cleanup;
        }
        cbm_pipeline_set_committed_counts(p, cbm_store_count_nodes(store, project),
                                          cbm_store_count_edges(store, project));
        cbm_pipeline_set_graph_changed(p, false);
        cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_INCREMENTAL_NOOP);
        cbm_pipeline_set_publish_reason(p, NULL);
        cbm_pipeline_set_exact_delta_stats(p, input_path_count, delta_count, 0);
        cbm_log_info("incremental.exact.noop", "files", itoa_buf_incr(delta_count));
        *applied = 1;
        goto cleanup;
    }

    CBM_PROF_START(t_exact_apply);
    rc = cbm_pipeline_apply_file_delta_batch_with_frontier_noop_mask(
        store, delta_ptrs, scoped_exact_gap ? frontier_noop_mask : NULL, delta_count,
        max_affected_paths, &plan);
    CBM_PROF_END_N("incremental_exact", "13_apply_delta", t_exact_apply, delta_count);
    if (rc != CBM_STORE_OK || plan.route != CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE) {
        incr_mark_generation_failed(store, project, generation);
        incr_report_exact_delta_plan_fallback(p, &plan, input_path_count, max_affected_paths,
                                              "apply", "apply_error");
        goto cleanup;
    }

    cbm_pipeline_set_committed_counts(p, cbm_store_count_nodes(store, project),
                                      cbm_store_count_edges(store, project));
    cbm_pipeline_set_graph_changed(p, true);
    cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT);
    cbm_pipeline_set_publish_reason(p, NULL);
    cbm_pipeline_set_exact_delta_stats(p, input_path_count, delta_count, exact_publish_count);
    if (cbm_pipeline_repo_path(p) && cbm_artifact_exists(cbm_pipeline_repo_path(p))) {
        (void)cbm_artifact_export(db_path, cbm_pipeline_repo_path(p), project, CBM_ARTIFACT_FAST);
    }
    cbm_log_info("incremental.exact.done", "files", itoa_buf_incr(exact_publish_count));
    *applied = 1;

cleanup:
    cbm_pipeline_file_delta_plan_free(&plan);
    free(store_delta_ptrs);
    free(delta_ptrs);
    free(frontier_noop_mask);
    incr_free_file_deltas(deltas, delta_count);
    incr_free_result_cache(result_cache, exact_count);
    cbm_path_alias_collection_free(path_aliases);
    if (cbm_pipeline_get_pkgmap() == pkgmap) {
        cbm_pipeline_set_pkgmap(NULL);
    }
    cbm_pkgmap_free(pkgmap);
    cbm_registry_free(registry);
    cbm_gbuf_free(scratch);
    free(changed_paths);
    free(exact_files);
    return CBM_STORE_OK;
}

/* Transactionally publish the merged project graph + hashes.
 * Mode-skipped hash rows are preserved across the rebuild so subsequent
 * reindexes can correctly distinguish "never indexed" from "indexed but
 * not visited this pass". */
static int publish_and_persist(cbm_gbuf_t *gbuf, const char *db_path, const char *project,
                               cbm_file_info_t *files, int file_count,
                               const cbm_file_hash_t *mode_skipped, int mode_skipped_count,
                               const char *repo_path, const char *pass_fingerprint, int mode,
                               bool semantic_edges_refreshed) {
    struct timespec t;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);

    cbm_store_t *hash_store = cbm_store_open_path(db_path);
    if (hash_store) {
        int flush_rc = cbm_gbuf_flush_to_store(gbuf, hash_store);
        cbm_log_info("incremental.flush", "rc", itoa_buf_incr(flush_rc), "elapsed_ms",
                     itoa_buf_incr((int)elapsed_ms_incr(t)));
        if (flush_rc != 0) {
            cbm_log_error("incremental.err", "phase", "flush", "rc", itoa_buf_incr(flush_rc));
            cbm_store_close(hash_store);
            return flush_rc;
        }

        int hash_rc =
            persist_hashes(hash_store, project, files, file_count, mode_skipped, mode_skipped_count);
        int state_rc = CBM_STORE_OK;
        if (hash_rc == CBM_STORE_OK) {
            state_rc = cbm_pipeline_persist_file_states(hash_store, project, files, file_count,
                                                        CBM_PIPELINE_COMPAT_GENERATION,
                                                        pass_fingerprint);
        }
        if (hash_rc != CBM_STORE_OK) {
            cbm_store_close(hash_store);
            return hash_rc;
        }
        if (state_rc != CBM_STORE_OK) {
            cbm_log_error("incremental.err", "phase", "persist_file_state", "rc",
                          itoa_buf_incr(state_rc));
            cbm_store_close(hash_store);
            return state_rc;
        }

        int owner_rc =
            cbm_store_rebuild_file_delta_owners(hash_store, project, CBM_PIPELINE_COMPAT_GENERATION);
        if (owner_rc != CBM_STORE_OK) {
            cbm_log_error("incremental.err", "phase", "rebuild_file_delta_owners", "rc",
                          itoa_buf_incr(owner_rc));
            cbm_store_close(hash_store);
            return owner_rc;
        }

        /* Project replacement rewrites node IDs and the contentless FTS table
         * has no backing content to cascade from, so rebuild it after publish. */
        int fts_rc = cbm_store_rebuild_nodes_fts(hash_store);
        if (fts_rc != CBM_STORE_OK) {
            cbm_log_error("incremental.err", "phase", "rebuild_nodes_fts", "rc",
                          itoa_buf_incr(fts_rc));
            cbm_store_close(hash_store);
            return fts_rc;
        }
        int derived_rc =
            cbm_pipeline_mark_replacement_derived_views(hash_store, project, mode,
                                                        semantic_edges_refreshed);
        if (derived_rc != CBM_STORE_OK) {
            cbm_log_error("incremental.err", "phase", "mark_derived_views", "rc",
                          itoa_buf_incr(derived_rc));
            cbm_store_close(hash_store);
            return derived_rc;
        }

        cbm_store_close(hash_store);
    } else {
        cbm_log_error("incremental.err", "phase", "hash_store_open");
        return CBM_NOT_FOUND;
    }

    /* Auto-update artifact if one already exists (persistence was enabled previously) */
    if (repo_path && cbm_artifact_exists(repo_path)) {
        cbm_artifact_export(db_path, repo_path, project, CBM_ARTIFACT_FAST);
    }
    return 0;
}

/* ── Incremental pipeline entry point ────────────────────────────── */

int cbm_pipeline_run_incremental(cbm_pipeline_t *p, const char *db_path, cbm_file_info_t *files,
                                 int file_count) {
    struct timespec t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t0);

    const char *project = cbm_pipeline_project_name(p);
    char pass_fingerprint[CBM_SZ_256];
    if (cbm_pipeline_current_pass_fingerprint(p, pass_fingerprint, sizeof(pass_fingerprint)) !=
        CBM_STORE_OK) {
        cbm_log_error("incremental.err", "phase", "pass_fingerprint");
        return CBM_NOT_FOUND;
    }

    /* Open existing disk DB */
    CBM_PROF_START(t_incr_open_db);
    cbm_store_t *store = cbm_store_open_path(db_path);
    CBM_PROF_END("incremental", "1_open_db", t_incr_open_db);
    if (!store) {
        cbm_log_error("incremental.err", "msg", "open_db_failed", "path", db_path);
        return CBM_NOT_FOUND;
    }

    /* Load stored file hashes */
    CBM_PROF_START(t_incr_load_hashes);
    cbm_file_hash_t *stored = NULL;
    int stored_count = 0;
    cbm_store_get_file_hashes(store, project, &stored, &stored_count);
    CBM_PROF_END_N("incremental", "2_load_hashes", t_incr_load_hashes, stored_count);

    /* Classify stored/current files once. This shared result is the future
     * route-decision boundary for exact delta and the existing containment path. */
    CBM_PROF_START(t_incr_classify);
    cbm_incr_classification_t cls = {0};
    if (incr_classification_build(p, store, project, files, file_count, stored, stored_count,
                                  pass_fingerprint, &cls) != 0) {
        CBM_PROF_END_N("incremental", "3_classify_failed", t_incr_classify, file_count);
        cbm_store_free_file_hashes(stored, stored_count);
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }
    CBM_PROF_END_N("incremental", "3_classify", t_incr_classify, file_count);

    char changed_buf[INCR_TS_BUF];
    char unchanged_buf[INCR_TS_BUF];
    char deleted_buf[INCR_TS_BUF];
    char mode_skipped_buf[INCR_TS_BUF];
    char metadata_only_buf[INCR_TS_BUF];
    snprintf(changed_buf, sizeof(changed_buf), "%d", cls.n_changed);
    snprintf(unchanged_buf, sizeof(unchanged_buf), "%d", cls.n_unchanged);
    snprintf(deleted_buf, sizeof(deleted_buf), "%d", cls.deleted_count);
    snprintf(mode_skipped_buf, sizeof(mode_skipped_buf), "%d", cls.mode_skipped_count);
    snprintf(metadata_only_buf, sizeof(metadata_only_buf), "%d", cls.n_metadata_only);
    cbm_log_info("incremental.classify", "changed", changed_buf, "unchanged", unchanged_buf,
                 "deleted", deleted_buf, "mode_skipped", mode_skipped_buf, "metadata_only",
                 metadata_only_buf);

    /* Fast path: no graph changes. If only filesystem metadata drifted after a
     * hash-confirmed touch, refresh file_hash rows so future runs keep the
     * cheap metadata path. Refresh failure is nonfatal; graph state is already
     * current and a later run can hash-confirm again. */
    if (cls.n_changed == 0 && cls.deleted_count == 0) {
        CBM_PROF_START(t_incr_noop_refresh);
        if (cls.n_metadata_only > 0 &&
            persist_hashes(store, project, files, file_count, cls.mode_skipped,
                           cls.mode_skipped_count) != CBM_STORE_OK) {
            cbm_log_warn("incremental.noop_metadata_refresh_failed", "count",
                         itoa_buf_incr(cls.n_metadata_only));
        }
        CBM_PROF_END_N("incremental", "4_noop_metadata_refresh", t_incr_noop_refresh,
                       cls.n_metadata_only);
        cbm_log_info("incremental.noop", "reason", "no_changes");
        cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_INCREMENTAL_NOOP);
        incr_classification_free(&cls);
        cbm_store_free_file_hashes(stored, stored_count);
        cbm_store_close(store);
        CBM_PROF_END_N("incremental", "TOTAL", t0, file_count);
        return 0;
    }

    if (incr_mark_dirty_classification(store, project, &cls) != CBM_STORE_OK) {
        cbm_log_warn("incremental.dirty_ledger.warn", "phase", "mark");
    }

    cbm_store_free_file_hashes(stored, stored_count);

    cbm_file_info_t *changed_files = cls.changed_files;
    int ci = cls.changed_file_count;
    int exact_applied = 0;
    (void)incr_try_exact_delete_route(p, store, db_path, project, cls.deleted, cls.deleted_count,
                                      ci, &exact_applied);
    if (exact_applied) {
        if (cbm_pipeline_publish_kind(p) != CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY &&
            incr_clear_dirty_classification(store, project, &cls) != CBM_STORE_OK) {
            cbm_log_warn("incremental.dirty_ledger.warn", "phase", "clear_exact_delete");
        }
        incr_classification_free(&cls);
        cbm_store_close(store);
        cbm_log_info("incremental.done", "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t0)));
        return 0;
    }

    (void)incr_try_overlay_upsert_route(p, store, project, changed_files, ci, cls.deleted_count,
                                        pass_fingerprint, &exact_applied);
    if (exact_applied) {
        incr_classification_free(&cls);
        cbm_store_close(store);
        cbm_log_info("incremental.done", "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t0)));
        return 0;
    }

    (void)incr_try_exact_upsert_route(p, store, db_path, project, changed_files, ci, files,
                                      file_count, cls.deleted, cls.deleted_count,
                                      pass_fingerprint, &exact_applied);
    if (exact_applied) {
        if (cbm_pipeline_publish_kind(p) != CBM_PIPELINE_PUBLISH_INCREMENTAL_OVERLAY &&
            incr_clear_dirty_classification(store, project, &cls) != CBM_STORE_OK) {
            cbm_log_warn("incremental.dirty_ledger.warn", "phase", "clear_exact_upsert");
        }
        incr_classification_free(&cls);
        cbm_store_close(store);
        cbm_log_info("incremental.done", "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t0)));
        return 0;
    }
    const char *exact_reason = cbm_pipeline_publish_reason(p);
    if (strcmp(exact_reason ? exact_reason : "",
               CBM_PIPELINE_DELTA_REASON_INBOUND_EDGES_REQUIRE_FULL) == 0) {
        incr_classification_free(&cls);
        cbm_store_close(store);
        cbm_log_info("incremental.fallback", "reason",
                     CBM_PIPELINE_DELTA_REASON_INBOUND_EDGES_REQUIRE_FULL);
        return CBM_NOT_FOUND;
    }
    if (strcmp(exact_reason ? exact_reason : "",
               CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE) == 0 &&
        incr_changed_contains_c_family_header(changed_files, ci)) {
        incr_classification_free(&cls);
        cbm_store_close(store);
        cbm_log_info("incremental.fallback", "reason",
                     CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE, "scope",
                     CBM_PIPELINE_DELTA_SCOPE_C_FAMILY_HEADER);
        return CBM_NOT_FOUND;
    }
    if (strcmp(exact_reason ? exact_reason : "",
               CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE) == 0 &&
        incr_changed_contains_c_family_source(changed_files, ci)) {
        incr_classification_free(&cls);
        cbm_store_close(store);
        cbm_log_info("incremental.fallback", "reason",
                     CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE, "scope",
                     CBM_PIPELINE_DELTA_SCOPE_C_FAMILY_SOURCE);
        return CBM_NOT_FOUND;
    }
    if (strcmp(exact_reason ? exact_reason : "",
               CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE) == 0 &&
        incr_changed_has_scoped_overlay_gap(changed_files, ci)) {
        incr_classification_free(&cls);
        cbm_store_close(store);
        cbm_log_info("incremental.fallback", "reason", "scoped_lsp_gap", "exact_reason",
                     CBM_PIPELINE_DELTA_REASON_FRONTIER_TOO_LARGE);
        return CBM_NOT_FOUND;
    }

    bool regular_frontier_expansion_ok = cbm_pipeline_get_mode(p) >= CBM_MODE_FAST;
    if (incr_expand_regular_changed_frontier(store, project, files, file_count, &cls,
                                             regular_frontier_expansion_ok) != CBM_STORE_OK) {
        const char *reason = regular_frontier_expansion_ok
                                 ? CBM_PIPELINE_DELTA_REASON_FRONTIER_REQUIRES_BATCH
                                 : "global_derived_edges";
        incr_classification_free(&cls);
        cbm_store_close(store);
        cbm_log_info("incremental.fallback", "reason", reason);
        return CBM_NOT_FOUND;
    }
    changed_files = cls.changed_files;
    ci = cls.changed_file_count;

    struct timespec t;

    /* Step 1: Load existing graph into RAM */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_gbuf_t *existing = cbm_gbuf_new(project, cbm_pipeline_repo_path(p));
    if (!existing) {
        cbm_log_error("incremental.err", "msg", "gbuf_new_oom");
        incr_classification_free(&cls);
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }
    int load_rc = cbm_gbuf_load_from_db(existing, db_path, project);
    cbm_log_info("incremental.load_db", "rc", itoa_buf_incr(load_rc), "nodes",
                 itoa_buf_incr(cbm_gbuf_node_count(existing)), "edges",
                 itoa_buf_incr(cbm_gbuf_edge_count(existing)), "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));

    if (load_rc != 0) {
        cbm_log_error("incremental.err", "msg", "load_db_failed");
        cbm_gbuf_free(existing);
        incr_classification_free(&cls);
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }

    cbm_store_close(store);
    free(cls.is_changed);
    cls.is_changed = NULL;

    cbm_log_info("incremental.reparse", "files", itoa_buf_incr(ci));

    /* Snapshot inbound cross-file edges into changed files BEFORE purging, so
     * the cascade delete doesn't permanently drop edges whose source lives in
     * an unchanged (never-re-parsed) file. Re-linked after re-resolution. */
    cbm_edge_capture_t edge_cap = {0};
    edge_cap.gbuf = existing;
    {
        CBMHashTable *changed_paths = cbm_ht_create(ci > 0 ? (size_t)ci * PAIR_LEN : CBM_SZ_64);
        if (!changed_paths) {
            cbm_log_error("incremental.err", "msg", "changed_paths_oom");
            incr_free_edge_capture(&edge_cap);
            cbm_gbuf_free(existing);
            incr_classification_free(&cls);
            return CBM_NOT_FOUND;
        }
        for (int i = 0; i < ci; i++) {
            cbm_ht_set(changed_paths, changed_files[i].rel_path, &changed_files[i]);
        }
        edge_cap.changed_paths = changed_paths;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        cbm_gbuf_foreach_edge(existing, incr_capture_inbound_edge, &edge_cap);
        edge_cap.changed_paths = NULL;
        cbm_ht_free(changed_paths); /* keys borrowed from changed_files; not freed here */
    }
    cbm_log_info("incremental.edge_snapshot", "captured", itoa_buf_incr(edge_cap.count), "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));

    /* Step 2: Purge stale nodes — single pass over all changed+deleted paths
     * (O(N+E) total). Was O(C·(N+E)): one cbm_gbuf_delete_by_file call per file,
     * each a full nodes+edges scan (perf fork-origin #3). */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    {
        int purge_count = ci + cls.deleted_count;
        if (purge_count > 0) {
            const char **purge_paths = malloc((size_t)purge_count * sizeof(const char *));
            if (purge_paths) {
                int p = 0;
                for (int i = 0; i < ci; i++) purge_paths[p++] = changed_files[i].rel_path;
                for (int i = 0; i < cls.deleted_count; i++) purge_paths[p++] = cls.deleted[i];
                cbm_gbuf_delete_by_paths(existing, purge_paths, purge_count);
                free(purge_paths);
            } else {
                /* OOM fallback: per-file scan (correct, just slower) */
                for (int i = 0; i < ci; i++)
                    cbm_gbuf_delete_by_file(existing, changed_files[i].rel_path);
                for (int i = 0; i < cls.deleted_count; i++)
                    cbm_gbuf_delete_by_file(existing, cls.deleted[i]);
            }
        }
    }
    cbm_log_info("incremental.purge", "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t)));

    /* Step 3-5: Registry + extract + resolve */
    cbm_registry_t *registry = cbm_registry_new();
    if (!registry) {
        cbm_log_error("incremental.err", "msg", "registry_oom");
        incr_free_edge_capture(&edge_cap);
        cbm_gbuf_free(existing);
        incr_classification_free(&cls);
        return CBM_NOT_FOUND;
    }
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_gbuf_foreach_node(existing, registry_visitor, registry);
    cbm_log_info("incremental.registry_seed", "symbols", itoa_buf_incr(cbm_registry_size(registry)),
                 "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t)));

    cbm_path_alias_collection_t *path_aliases = cbm_load_path_aliases(cbm_pipeline_repo_path(p));

    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = cbm_pipeline_repo_path(p),
        .gbuf = existing,
        .registry = registry,
        .cancelled = cbm_pipeline_cancelled_ptr(p),
        .mode = cbm_pipeline_get_mode(p),
        .similarity_threshold = cbm_pipeline_similarity_threshold(p),
        .httplink_min_confidence = cbm_pipeline_httplink_min_confidence(p),
        .semantic_threshold = cbm_pipeline_semantic_threshold(p),
        .githistory_min_coupling = cbm_pipeline_githistory_min_coupling(p),
        .lsp_confidence_floor = cbm_pipeline_lsp_confidence_floor(p),
        .path_aliases = path_aliases,
    };

    const char *structure_root_qn = incremental_structure_root_qn(existing, project);
    int pipeline_rc = 0;
    for (int i = 0; i < ci; i++) {
        pipeline_rc = cbm_pipeline_ensure_file_structure(existing, project, structure_root_qn,
                                                         changed_files[i].rel_path, NULL);
        if (pipeline_rc != 0) {
            cbm_log_error("incremental.err", "phase", "ensure_file_structure", "rc",
                          itoa_buf_incr(pipeline_rc));
            break;
        }
    }

    if (pipeline_rc == 0) {
        pipeline_rc = run_extract_resolve(&ctx, changed_files, ci);
    }
    if (pipeline_rc == 0) {
        pipeline_rc = cbm_pipeline_pass_k8s(&ctx, changed_files, ci);
        if (pipeline_rc != 0) {
            cbm_log_error("incremental.err", "phase", "incr_k8s", "rc",
                          itoa_buf_incr(pipeline_rc));
        }
    }
    if (pipeline_rc == 0) {
        if (cbm_pipeline_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_POSTPASS)) {
            cbm_log_error("incremental.err", "phase", CBM_TEST_FAIL_INCREMENTAL_POSTPASS, "rc",
                          itoa_buf_incr(CBM_NOT_FOUND));
            pipeline_rc = CBM_NOT_FOUND;
        } else {
            bool refresh_global_semantic_edges =
                !cbm_pipeline_incremental_derived_refresh_stale_on_incremental(p);
            pipeline_rc = run_postpasses(&ctx, changed_files, ci, project,
                                         refresh_global_semantic_edges);
        }
    }

    if (pipeline_rc != 0) {
        cbm_registry_free(registry);
        cbm_path_alias_collection_free(path_aliases);
        incr_free_edge_capture(&edge_cap);
        incr_classification_free(&cls);
        cbm_gbuf_free(existing);
        return pipeline_rc;
    }

    /* Re-link inbound cross-file edges that the purge orphaned. Runs after
     * re-resolution AND post-passes so the freshly re-created target nodes
     * exist and nothing downstream clobbers the restored edges; insert_edge
     * dedups, so any edge the resolver already recreated is a no-op. */
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    int relinked = incr_restore_inbound_edges(existing, &edge_cap);
    cbm_log_info("incremental.edge_relink", "relinked", itoa_buf_incr(relinked), "captured",
                 itoa_buf_incr(edge_cap.count), "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t)));
    incr_free_edge_capture(&edge_cap);

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_pass_complexity(&ctx);
    cbm_log_info("pass.timing", "pass", "incr_complexity", "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    int httplink_rc = cbm_pipeline_pass_httplinks(&ctx);
    cbm_log_info("pass.timing", "pass", "incr_httplinks", "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));
    cbm_registry_free(registry);
    cbm_path_alias_collection_free(path_aliases);
    if (httplink_rc != 0) {
        cbm_log_error("incremental.err", "phase", "incr_httplinks", "rc",
                      itoa_buf_incr(httplink_rc));
        incr_classification_free(&cls);
        cbm_gbuf_free(existing);
        return httplink_rc;
    }

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    cbm_pipeline_pass_normalize(existing);
    cbm_log_info("pass.timing", "pass", "incr_normalize", "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));

    /* Step 7: Publish the merged project graph (preserves mode-skipped hash
     * rows so the next reindex can correctly classify those files instead of
     * seeing them as never-existed; also exports a fast-mode artifact when one
     * is already present alongside the repo). */
    /* Record committed counts before publishing so the #334 plausibility gate
     * covers incremental reindexes, not just full publishes. */
    cbm_pipeline_set_committed_counts(p, cbm_gbuf_node_count(existing),
                                      cbm_gbuf_edge_count(existing));
    bool semantic_edges_refreshed =
        !cbm_pipeline_incremental_derived_refresh_stale_on_incremental(p);
    int persist_rc = publish_and_persist(existing, db_path, project, files, file_count,
                                         cls.mode_skipped, cls.mode_skipped_count,
                                         cbm_pipeline_repo_path(p), pass_fingerprint,
                                         cbm_pipeline_get_mode(p), semantic_edges_refreshed);
    if (persist_rc == 0) {
        cbm_pipeline_set_graph_changed(p, true);
        cbm_pipeline_set_publish_kind(p, CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT);
        if (!cbm_pipeline_publish_reason(p)) {
            cbm_pipeline_set_publish_reason(p, "containment_rebuild");
        }
        if (incr_clear_dirty_classification_path(db_path, project, &cls) != CBM_STORE_OK) {
            cbm_log_warn("incremental.dirty_ledger.warn", "phase", "clear_containment");
        }
    }
    incr_classification_free(&cls);
    cbm_gbuf_free(existing);
    if (persist_rc != 0) {
        return persist_rc;
    }

    cbm_log_info("incremental.done", "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t0)));
    return 0;
}
