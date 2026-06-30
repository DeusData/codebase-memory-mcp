/*
 * pipeline_incremental.c — Disk-based incremental re-indexing.
 *
 * Compares file mtime+size against stored hashes to classify changed/unchanged.
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
#include "store/store.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <stdatomic.h>
#include <stdint.h>

/* ── Timing helper (same as pipeline.c) ──────────────────────────── */

static const char cbm_incr_test_env_disabled[] = "0";

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

static bool incr_test_fail_phase_enabled(const char *phase) {
    char buf[CBM_SZ_64];
    const char *val =
        cbm_safe_getenv(CBM_TEST_FAIL_INCREMENTAL_PHASE, buf, sizeof(buf), NULL);
    return val && val[0] != '\0' && strcmp(val, cbm_incr_test_env_disabled) != 0 &&
           phase && strcmp(val, phase) == 0;
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
    CBMHashTable *ht =
        cbm_ht_create(stored_count > 0 ? (size_t)stored_count * PAIR_LEN : CBM_SZ_64);
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

        if (cbm_pipeline_stat_mtime_ns(&st) != h->mtime_ns || st.st_size != h->size) {
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

/* Classify stored files that are absent from current discovery. Returns the
 * count of truly-deleted files (output via out_deleted) and ALSO collects
 * mode-skipped files into out_mode_skipped (caller frees both).
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
 * contract: dump_and_persist re-upserts these hash rows so the next reindex
 * can correctly detect a real on-disk deletion of a mode-skipped file (as
 * opposed to seeing it as "never existed" → noop → orphaned graph nodes).
 *
 * Fail-safe rules (preserve nodes on uncertainty):
 *   - repo_path NULL → log error and preserve everything (return 0
 *     deletions, empty mode_skipped). The caller contract is that
 *     repo_path is required; a NULL means a misconfigured pipeline,
 *     not a deletion signal.
 *   - snprintf truncation (combined path ≥ CBM_SZ_4K) → preserve. We can't
 *     reliably stat a truncated path. Treat as mode-skipped.
 *   - stat() errno != ENOENT/ENOTDIR (EACCES, EIO, ELOOP, transient NFS,
 *     etc.) → preserve. The file may exist; we just can't see it right now.
 *     Treat as mode-skipped.
 *
 * Note: we use stat() (not lstat()) on purpose. A symlink whose target was
 * deleted should be classified as deleted from the indexer's perspective
 * because the indexer follows symlinks during discovery — a stale symlink
 * has no source to parse. */
static int find_deleted_files(const char *repo_path, cbm_file_info_t *files, int file_count,
                              cbm_file_hash_t *stored, int stored_count, char ***out_deleted,
                              cbm_file_hash_t **out_mode_skipped, int *out_mode_skipped_count) {
    *out_deleted = NULL;
    *out_mode_skipped = NULL;
    *out_mode_skipped_count = 0;

    if (!repo_path) {
        /* Misconfigured pipeline. Preserve everything rather than risk
         * silently re-introducing the destructive overwrite this function
         * was rewritten to prevent. */
        cbm_log_error("incremental.err", "msg", "find_deleted_files_null_repo_path");
        return 0;
    }

    CBMHashTable *current = cbm_ht_create((size_t)file_count * PAIR_LEN);
    if (!current) {
        cbm_log_error("incremental.err", "msg", "find_deleted_files_current_oom");
        return 0;
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
        return 0;
    }

    int ms_count = 0;
    int ms_cap = CBM_SZ_64;
    cbm_file_hash_t *mode_skipped = malloc((size_t)ms_cap * sizeof(cbm_file_hash_t));
    if (!mode_skipped) {
        cbm_log_error("incremental.err", "msg", "find_deleted_files_oom_ms");
        free(deleted);
        cbm_ht_free(current);
        return 0;
    }

    for (int i = 0; i < stored_count; i++) {
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
                ms_cap *= PAIR_LEN;
                cbm_file_hash_t *tmp = realloc(mode_skipped, (size_t)ms_cap * sizeof(*tmp));
                if (!tmp) {
                    cbm_log_error("incremental.err", "msg", "find_deleted_files_realloc_oom_ms");
                    break;
                }
                mode_skipped = tmp;
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
            del_cap *= PAIR_LEN;
            char **tmp = realloc(deleted, (size_t)del_cap * sizeof(char *));
            if (!tmp) {
                cbm_log_error("incremental.err", "msg", "find_deleted_files_realloc_oom");
                break;
            }
            deleted = tmp;
        }
        char *rp = cbm_strdup(stored[i].rel_path);
        if (!rp) {
            cbm_log_error("incremental.err", "msg", "find_deleted_files_strdup_oom", "rel_path",
                          stored[i].rel_path);
            break;
        }
        deleted[del_count++] = rp;
    }

    cbm_ht_free(current);
    *out_deleted = deleted;
    *out_mode_skipped = mode_skipped;
    *out_mode_skipped_count = ms_count;
    return del_count;
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
    char **deleted;
    int deleted_count;
    cbm_file_hash_t *mode_skipped;
    int mode_skipped_count;
    cbm_file_info_t *changed_files;
    int changed_file_count;
} cbm_incr_classification_t;

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

static int incr_classification_build(cbm_pipeline_t *p, cbm_file_info_t *files, int file_count,
                                     cbm_file_hash_t *stored, int stored_count,
                                     cbm_incr_classification_t *out) {
    if (!p || !out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));

    out->is_changed =
        classify_files(files, file_count, stored, stored_count, &out->n_changed, &out->n_unchanged);
    if (!out->is_changed) {
        cbm_log_error("incremental.err", "msg", "classify_files_oom");
        return CBM_NOT_FOUND;
    }

    out->deleted_count =
        find_deleted_files(cbm_pipeline_repo_path(p), files, file_count, stored, stored_count,
                           &out->deleted, &out->mode_skipped, &out->mode_skipped_count);

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
static bool incr_edge_type_is_recomputed(const char *type) {
    return type && (strcmp(type, "SIMILAR_TO") == 0 || strcmp(type, "SEMANTICALLY_RELATED") == 0 ||
                    strcmp(type, "FILE_CHANGES_WITH") == 0 || strcmp(type, "DATA_FLOWS") == 0);
}

/* cbm_gbuf_foreach_edge visitor: snapshot inbound cross-file edges into
 * changed files so they survive the purge and can be re-linked afterward. */
static void incr_capture_inbound_edge(const cbm_gbuf_edge_t *edge, void *userdata) {
    cbm_edge_capture_t *cap = (cbm_edge_capture_t *)userdata;
    if (incr_edge_type_is_recomputed(edge->type)) {
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
    s->source_qn = strdup(src->qualified_name);
    s->target_qn = strdup(tgt->qualified_name);
    s->type = strdup(edge->type);
    s->props = strdup(edge->properties_json ? edge->properties_json : "{}");
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

    if (incr_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_HASH_PERSIST)) {
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

/* Labels the full-index definition pass seeds into the registry
 * (pass_definitions.c — KEEP IN SYNC). Incremental re-resolution must see the
 * SAME symbol set, or it diverges from a clean full reindex: seeding extra
 * container nodes (File / Module / Folder / ...) lets a type usage like `Word`
 * resolve to the same-named Module node instead of the Class node. Only
 * callable / declared symbols belong in the registry. */
static bool incr_label_is_registry_symbol(const char *label) {
    return label && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0 ||
                     strcmp(label, "Class") == 0 || strcmp(label, "Interface") == 0 ||
                     strcmp(label, "Variable") == 0 || strcmp(label, "Field") == 0);
}

/* Callback for cbm_gbuf_foreach_node: seed the registry with the existing
 * project's definition symbols so the resolver can match cross-file symbols
 * during incremental. Mirrors the full-index registry contents exactly so an
 * incremental re-resolve picks the same nodes a full reindex would. */
static void registry_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    cbm_registry_t *r = (cbm_registry_t *)userdata;
    if (!incr_label_is_registry_symbol(node->label)) {
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

    /* Per-file LSP always runs (every mode). Cross-file LSP stays disabled in
     * incremental regardless (cbm_parallel_resolve is called with NULL
     * cross_registries below). */

#define MIN_FILES_FOR_PARALLEL_INCR 50
    int worker_count = cbm_default_worker_count(true);
    bool use_parallel = (worker_count > SKIP_ONE && ci > MIN_FILES_FOR_PARALLEL_INCR);

    if (use_parallel) {
        cbm_log_info("incremental.mode", "mode", "parallel", "workers", itoa_buf_incr(worker_count),
                     "changed", itoa_buf_incr(ci));

        _Atomic int64_t shared_ids;
        atomic_init(&shared_ids, cbm_gbuf_next_id(ctx->gbuf));

        CBMFileResult **cache = (CBMFileResult **)calloc(ci, sizeof(CBMFileResult *));
        if (cache) {
            int rc = 0;
            if (incr_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_EXTRACT)) {
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

            if (incr_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_REGISTRY)) {
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
            if (incr_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_RESOLVE)) {
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
        rc = cbm_pipeline_pass_definitions(ctx, changed_files, ci);
        if (rc != 0) {
            cbm_log_error("incremental.err", "phase", "incr_definitions", "rc",
                          itoa_buf_incr(rc));
            return rc;
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
                          const char *project) {
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
    if (ctx->mode <= CBM_MODE_MODERATE) {
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

/* Atomically dump merged graph + hashes to disk.
 * Mode-skipped hash rows are preserved across the rebuild so subsequent
 * reindexes can correctly distinguish "never indexed" from "indexed but
 * not visited this pass". */
static int dump_and_persist(cbm_gbuf_t *gbuf, const char *db_path, const char *project,
                            cbm_file_info_t *files, int file_count,
                            const cbm_file_hash_t *mode_skipped, int mode_skipped_count,
                            const char *repo_path) {
    struct timespec t;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t);

    int dump_rc = cbm_gbuf_dump_to_sqlite(gbuf, db_path);
    cbm_log_info("incremental.dump", "rc", itoa_buf_incr(dump_rc), "elapsed_ms",
                 itoa_buf_incr((int)elapsed_ms_incr(t)));
    if (dump_rc != 0) {
        cbm_log_error("incremental.err", "phase", "dump", "rc", itoa_buf_incr(dump_rc));
        return dump_rc;
    }

    cbm_store_t *hash_store = cbm_store_open_path(db_path);
    if (hash_store) {
        int hash_rc =
            persist_hashes(hash_store, project, files, file_count, mode_skipped, mode_skipped_count);

        /* FTS5 rebuild after incremental dump.  The btree dump path bypasses
         * any triggers that could have kept nodes_fts synchronized, so we
         * rebuild from the nodes table here.  See the full-dump path in
         * pipeline.c for the matching logic. */
        cbm_store_exec(hash_store, "INSERT INTO nodes_fts(nodes_fts) VALUES('delete-all');");
        if (cbm_store_exec(hash_store,
                           "INSERT INTO nodes_fts(rowid, name, qualified_name, label, file_path) "
                           "SELECT id, cbm_camel_split(name), qualified_name, label, file_path "
                           "FROM nodes;") != CBM_STORE_OK) {
            cbm_store_exec(hash_store,
                           "INSERT INTO nodes_fts(rowid, name, qualified_name, label, file_path) "
                           "SELECT id, name, qualified_name, label, file_path FROM nodes;");
        }

        cbm_store_close(hash_store);
        if (hash_rc != CBM_STORE_OK) {
            return hash_rc;
        }
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

    /* Open existing disk DB */
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        cbm_log_error("incremental.err", "msg", "open_db_failed", "path", db_path);
        return CBM_NOT_FOUND;
    }

    /* Load stored file hashes */
    cbm_file_hash_t *stored = NULL;
    int stored_count = 0;
    cbm_store_get_file_hashes(store, project, &stored, &stored_count);

    /* Classify stored/current files once. This shared result is the future
     * route-decision boundary for exact delta and the existing containment path. */
    cbm_incr_classification_t cls = {0};
    if (incr_classification_build(p, files, file_count, stored, stored_count, &cls) != 0) {
        cbm_store_free_file_hashes(stored, stored_count);
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }

    cbm_log_info("incremental.classify", "changed", itoa_buf_incr(cls.n_changed), "unchanged",
                 itoa_buf_incr(cls.n_unchanged), "deleted", itoa_buf_incr(cls.deleted_count),
                 "mode_skipped", itoa_buf_incr(cls.mode_skipped_count));

    /* Fast path: nothing changed → skip. The on-disk DB is left untouched,
     * which means existing hash rows (including for any mode-skipped files
     * that were already preserved by an earlier run) remain intact. */
    if (cls.n_changed == 0 && cls.deleted_count == 0) {
        cbm_log_info("incremental.noop", "reason", "no_changes");
        incr_classification_free(&cls);
        cbm_store_free_file_hashes(stored, stored_count);
        cbm_store_close(store);
        return 0;
    }

    cbm_store_free_file_hashes(stored, stored_count);

    cbm_file_info_t *changed_files = cls.changed_files;
    int ci = cls.changed_file_count;

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
    free_deleted_paths(cls.deleted, cls.deleted_count);
    cls.deleted = NULL;
    cls.deleted_count = 0;
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
        if (incr_test_fail_phase_enabled(CBM_TEST_FAIL_INCREMENTAL_POSTPASS)) {
            cbm_log_error("incremental.err", "phase", CBM_TEST_FAIL_INCREMENTAL_POSTPASS, "rc",
                          itoa_buf_incr(CBM_NOT_FOUND));
            pipeline_rc = CBM_NOT_FOUND;
        } else {
            pipeline_rc = run_postpasses(&ctx, changed_files, ci, project);
        }
    }

    free(cls.changed_files);
    cls.changed_files = NULL;
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

    /* Step 7: Dump to disk (preserves mode-skipped hash rows so the next
     * reindex can correctly classify those files instead of seeing them
     * as never-existed; also exports a fast-mode artifact when one is
     * already present alongside the repo). */
    /* Record committed counts before dump_and_persist (whose dump frees the
     * gbuf node index, zeroing the count) so the #334 plausibility gate also
     * covers incremental reindexes, not just full ones. */
    cbm_pipeline_set_committed_counts(p, cbm_gbuf_node_count(existing),
                                      cbm_gbuf_edge_count(existing));
    int persist_rc = dump_and_persist(existing, db_path, project, files, file_count, cls.mode_skipped,
                                      cls.mode_skipped_count, cbm_pipeline_repo_path(p));
    incr_classification_free(&cls);
    cbm_gbuf_free(existing);
    if (persist_rc != 0) {
        return persist_rc;
    }

    cbm_log_info("incremental.done", "elapsed_ms", itoa_buf_incr((int)elapsed_ms_incr(t0)));
    return 0;
}
