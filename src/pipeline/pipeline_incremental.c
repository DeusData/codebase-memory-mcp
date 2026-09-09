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
#include "foundation/constants.h"

enum {
    INCR_RING_BUF = 4,
    INCR_RING_MASK = 3,
    INCR_TS_BUF = 24,
    INCR_NODE_DIGEST_COLS = 8,
    INCR_EDGE_DIGEST_COLS = 7,
    INCR_START_LINE_COL = 4,
    INCR_END_LINE_COL = 5,
    INCR_CONFIDENCE_COL = 5,
};
#include "pipeline/pipeline.h"
#include "pipeline/artifact.h"
#include "pipeline/content_hash.h"
#include "pipeline/worker_pool.h"
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
#include <sqlite3.h>

/* ── Constants ───────────────────────────────────────────────────── */

#define CBM_MS_PER_SEC 1000.0
#define CBM_NS_PER_MS 1000000.0
#define CBM_NS_PER_SEC 1000000000LL

bool cbm_pipeline_incremental_requires_rebuild(int current_count, int stored_count,
                                               int changed_count, int deleted_count) {
    int baseline = current_count > stored_count ? current_count : stored_count;
    int affected = changed_count + deleted_count;
    if (baseline <= 0 || affected <= 0) {
        return false;
    }
    return (int64_t)affected * 5 > (int64_t)baseline;
}

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
    static _Thread_local char buf[INCR_RING_BUF][INCR_TS_BUF];
    static _Thread_local int idx = 0;
    idx = (idx + SKIP_ONE) & INCR_RING_MASK;
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

/* Classify discovered files against stored metadata and XXH3-128 hashes.
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

        bool metadata_changed = stat_mtime_ns(&st) != h->mtime_ns || st.st_size != h->size;
        char content_hash[CBM_CONTENT_HASH_SIZE] = {0};
        bool hash_changed =
            !h->sha256 || h->sha256[0] == '\0' ||
            cbm_content_hash_file(files[i].path, content_hash) != 0 ||
            strcmp(content_hash, h->sha256) != 0;
        if (metadata_changed || hash_changed) {
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
                             itoa_buf(errno));
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
            char *rp = strdup(stored[i].rel_path);
            char *sh = stored[i].sha256 ? strdup(stored[i].sha256) : NULL;
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
        deleted[del_count++] = strdup(stored[i].rel_path);
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

/* ── Delta working sets ──────────────────────────────────────────── */

typedef struct {
    cbm_file_info_t *items; /* borrowed path/rel_path strings */
    int count;
    int capacity;
} incr_file_vec_t;

typedef struct {
    int64_t temp_id;
    int64_t real_id;
} seed_id_pair_t;

typedef struct {
    seed_id_pair_t *items;
    int count;
    int capacity;
} seed_id_vec_t;

static bool file_vec_contains(const incr_file_vec_t *vec, const char *rel_path) {
    for (int i = 0; i < vec->count; i++) {
        if (strcmp(vec->items[i].rel_path, rel_path) == 0) {
            return true;
        }
    }
    return false;
}

static int file_vec_append(incr_file_vec_t *vec, const cbm_file_info_t *file) {
    if (!file || !file->rel_path || file_vec_contains(vec, file->rel_path)) {
        return 0;
    }
    if (vec->count >= vec->capacity) {
        int next = vec->capacity > 0 ? vec->capacity * PAIR_LEN : CBM_SZ_16;
        cbm_file_info_t *grown = realloc(vec->items, (size_t)next * sizeof(*grown));
        if (!grown) {
            return CBM_NOT_FOUND;
        }
        vec->items = grown;
        vec->capacity = next;
    }
    vec->items[vec->count++] = *file;
    return 0;
}

static const cbm_file_info_t *find_current_file(cbm_file_info_t *files, int file_count,
                                                const char *rel_path) {
    for (int i = 0; i < file_count; i++) {
        if (strcmp(files[i].rel_path, rel_path) == 0) {
            return &files[i];
        }
    }
    return NULL;
}

static int compare_file_info(const void *lhs, const void *rhs) {
    const cbm_file_info_t *a = lhs;
    const cbm_file_info_t *b = rhs;
    return strcmp(a->rel_path, b->rel_path);
}

static bool path_is_affected(const incr_file_vec_t *files, char **deleted, int deleted_count,
                             const char *file_path) {
    if (!file_path || file_path[0] == '\0') {
        return false;
    }
    if (file_vec_contains(files, file_path)) {
        return true;
    }
    for (int i = 0; i < deleted_count; i++) {
        if (strcmp(deleted[i], file_path) == 0) {
            return true;
        }
    }
    return false;
}

static int seed_id_append(seed_id_vec_t *vec, int64_t temp_id, int64_t real_id) {
    if (vec->count >= vec->capacity) {
        int next = vec->capacity > 0 ? vec->capacity * PAIR_LEN : CBM_SZ_64;
        seed_id_pair_t *grown = realloc(vec->items, (size_t)next * sizeof(*grown));
        if (!grown) {
            return CBM_NOT_FOUND;
        }
        vec->items = grown;
        vec->capacity = next;
    }
    vec->items[vec->count++] = (seed_id_pair_t){.temp_id = temp_id, .real_id = real_id};
    return 0;
}

static int append_neighbor_file(cbm_store_t *store, int64_t node_id, cbm_file_info_t *files,
                                int file_count, incr_file_vec_t *parse_files) {
    cbm_node_t node = {0};
    if (cbm_store_find_node_by_id(store, node_id, &node) != CBM_STORE_OK) {
        return 0;
    }
    const cbm_file_info_t *file =
        node.file_path ? find_current_file(files, file_count, node.file_path) : NULL;
    int rc = file ? file_vec_append(parse_files, file) : 0;
    cbm_node_free_fields(&node);
    return rc;
}

/* Add one inbound/outbound dependency hop around the files that actually
 * changed. This restores edges whose unchanged endpoint would otherwise be
 * removed by ON DELETE CASCADE when an affected symbol is replaced. */
static int expand_dependency_cone(cbm_store_t *store, const char *project,
                                  cbm_file_info_t *files, int file_count,
                                  const incr_file_vec_t *changed_files, char **deleted,
                                  int deleted_count, incr_file_vec_t *parse_files) {
    int base_count = changed_files->count + deleted_count;
    for (int bi = 0; bi < base_count; bi++) {
        const char *rel_path =
            bi < changed_files->count ? changed_files->items[bi].rel_path
                                      : deleted[bi - changed_files->count];
        cbm_node_t *nodes = NULL;
        int node_count = 0;
        if (cbm_store_find_nodes_by_file(store, project, rel_path, &nodes, &node_count) !=
            CBM_STORE_OK) {
            return CBM_NOT_FOUND;
        }
        for (int ni = 0; ni < node_count; ni++) {
            cbm_edge_t *inbound = NULL;
            cbm_edge_t *outbound = NULL;
            int inbound_count = 0;
            int outbound_count = 0;
            if (cbm_store_find_edges_by_target(store, nodes[ni].id, &inbound, &inbound_count) !=
                    CBM_STORE_OK ||
                cbm_store_find_edges_by_source(store, nodes[ni].id, &outbound, &outbound_count) !=
                    CBM_STORE_OK) {
                cbm_store_free_edges(inbound, inbound_count);
                cbm_store_free_edges(outbound, outbound_count);
                cbm_store_free_nodes(nodes, node_count);
                return CBM_NOT_FOUND;
            }
            for (int ei = 0; ei < inbound_count; ei++) {
                if (append_neighbor_file(store, inbound[ei].source_id, files, file_count,
                                         parse_files) != 0) {
                    cbm_store_free_edges(inbound, inbound_count);
                    cbm_store_free_edges(outbound, outbound_count);
                    cbm_store_free_nodes(nodes, node_count);
                    return CBM_NOT_FOUND;
                }
            }
            for (int ei = 0; ei < outbound_count; ei++) {
                if (append_neighbor_file(store, outbound[ei].target_id, files, file_count,
                                         parse_files) != 0) {
                    cbm_store_free_edges(inbound, inbound_count);
                    cbm_store_free_edges(outbound, outbound_count);
                    cbm_store_free_nodes(nodes, node_count);
                    return CBM_NOT_FOUND;
                }
            }
            cbm_store_free_edges(inbound, inbound_count);
            cbm_store_free_edges(outbound, outbound_count);
        }
        cbm_store_free_nodes(nodes, node_count);
    }
    qsort(parse_files->items, (size_t)parse_files->count, sizeof(*parse_files->items),
          compare_file_info);
    return 0;
}

/* ── Registry seed visitor ────────────────────────────────────────── */

/* Callback for cbm_gbuf_foreach_node: add each node to the registry
 * so the resolver can find cross-file symbols during incremental. */
static void registry_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    cbm_registry_t *r = (cbm_registry_t *)userdata;
    cbm_registry_add(r, node->name, node->qualified_name, node->label);
}

static int seed_existing_nodes(cbm_store_t *store, const char *project, cbm_gbuf_t *slice,
                               const incr_file_vec_t *parse_files, char **deleted,
                               int deleted_count, seed_id_vec_t *seed_ids) {
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    if (cbm_store_find_nodes_by_project(store, project, &nodes, &node_count) != CBM_STORE_OK) {
        return CBM_NOT_FOUND;
    }
    int rc = 0;
    for (int i = 0; i < node_count; i++) {
        cbm_node_t *node = &nodes[i];
        if (path_is_affected(parse_files, deleted, deleted_count, node->file_path)) {
            continue;
        }
        cbm_gbuf_node_spec_t spec = {
            .label = node->label,
            .name = node->name,
            .qualified_name = node->qualified_name,
            .file_path = node->file_path,
            .start_line = node->start_line,
            .end_line = node->end_line,
            .properties_json = node->properties_json,
            .symbol_id = node->symbol_id,
            .language = node->language,
            .signature = node->signature,
            .origin = node->origin,
            .confidence = node->confidence,
        };
        int64_t temp_id = cbm_gbuf_upsert_node_v2(slice, &spec);
        if (temp_id <= 0 || seed_id_append(seed_ids, temp_id, node->id) != 0) {
            rc = CBM_NOT_FOUND;
            break;
        }
    }
    cbm_store_free_nodes(nodes, node_count);
    return rc;
}

/* Build only the structural chain needed by a changed/new file. Existing
 * Project/Folder nodes were seeded with their real IDs; new directories are
 * inserted top-down so every CONTAINS_FOLDER edge is emitted deterministically. */
static int add_file_structure(cbm_gbuf_t *slice, const char *project, const char *rel_path) {
    if (!rel_path || rel_path[0] == '\0') {
        return CBM_NOT_FOUND;
    }
    if (cbm_gbuf_upsert_node(slice, "Project", project, project, "", 0, 0, "{}") <= 0) {
        return CBM_NOT_FOUND;
    }

    char *rel_copy = strdup(rel_path);
    if (!rel_copy) {
        return CBM_NOT_FOUND;
    }
    char *slash = strrchr(rel_copy, '/');
    if (slash) {
        *slash = '\0';
    } else {
        rel_copy[0] = '\0';
    }

    char *walk = strdup(rel_copy);
    if (!walk) {
        free(rel_copy);
        return CBM_NOT_FOUND;
    }
    char prefix[CBM_SZ_4K] = {0};
    char *save = NULL;
    char *segment = strtok_r(walk, "/", &save);
    const char *parent_qn = project;
    char *parent_owned = NULL;
    while (segment) {
        size_t used = strlen(prefix);
        int written = snprintf(prefix + used, sizeof(prefix) - used, "%s%s",
                               used > 0 ? "/" : "", segment);
        if (written < 0 || (size_t)written >= sizeof(prefix) - used) {
            free(parent_owned);
            free(walk);
            free(rel_copy);
            return CBM_NOT_FOUND;
        }
        char *folder_qn = cbm_pipeline_fqn_folder(project, prefix);
        if (!folder_qn ||
            cbm_gbuf_upsert_node(slice, "Folder", segment, folder_qn, prefix, 0, 0, "{}") <= 0) {
            free(folder_qn);
            free(parent_owned);
            free(walk);
            free(rel_copy);
            return CBM_NOT_FOUND;
        }
        const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(slice, parent_qn);
        const cbm_gbuf_node_t *folder = cbm_gbuf_find_by_qn(slice, folder_qn);
        if (!parent || !folder ||
            cbm_gbuf_insert_edge(slice, parent->id, folder->id, "CONTAINS_FOLDER", "{}") <= 0) {
            free(folder_qn);
            free(parent_owned);
            free(walk);
            free(rel_copy);
            return CBM_NOT_FOUND;
        }
        free(parent_owned);
        parent_owned = folder_qn;
        parent_qn = parent_owned;
        segment = strtok_r(NULL, "/", &save);
    }

    const char *basename = strrchr(rel_path, '/');
    basename = basename ? basename + SKIP_ONE : rel_path;
    const char *extension = strrchr(basename, '.');
    char props[CBM_SZ_256];
    snprintf(props, sizeof(props), "{\"extension\":\"%s\"}", extension ? extension : "");
    char *file_qn = cbm_pipeline_fqn_compute(project, rel_path, "__file__");
    int rc = CBM_NOT_FOUND;
    if (file_qn &&
        cbm_gbuf_upsert_node(slice, "File", basename, file_qn, rel_path, 0, 0, props) > 0) {
        const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(slice, parent_qn);
        const cbm_gbuf_node_t *file = cbm_gbuf_find_by_qn(slice, file_qn);
        if (parent && file &&
            cbm_gbuf_insert_edge(slice, parent->id, file->id, "CONTAINS_FILE", "{}") > 0) {
            rc = 0;
        }
    }
    free(file_qn);
    free(parent_owned);
    free(walk);
    free(rel_copy);
    return rc;
}

/* Run parallel or sequential extract+resolve for the affected dependency cone. */
static int run_extract_resolve(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *changed_files, int ci) {
    struct timespec t;

    /* Per-file LSP always runs (every mode). Cross-file LSP stays disabled in
     * incremental regardless (cbm_parallel_resolve is called with NULL
     * cross_registries below). */

#define MIN_FILES_FOR_PARALLEL_INCR 50
    int worker_count = cbm_worker_count_for_files(ci, false);
    bool use_parallel = (worker_count > SKIP_ONE && ci > MIN_FILES_FOR_PARALLEL_INCR);

    if (use_parallel) {
        cbm_log_info("incremental.mode", "mode", "parallel", "workers", itoa_buf(worker_count),
                     "changed", itoa_buf(ci));

        _Atomic int64_t shared_ids;
        atomic_init(&shared_ids, cbm_gbuf_next_id(ctx->gbuf));

        CBMFileResult **cache = (CBMFileResult **)calloc(ci, sizeof(CBMFileResult *));
        if (!cache) {
            return CBM_NOT_FOUND;
        }
        int rc = 0;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t);
        rc = cbm_parallel_extract(ctx, changed_files, ci, cache, &shared_ids, worker_count);
        cbm_gbuf_set_next_id(ctx->gbuf, atomic_load(&shared_ids));
        cbm_log_info("pass.timing", "pass", "incr_extract", "elapsed_ms",
                     itoa_buf((int)elapsed_ms(t)));

        if (rc == 0) {
            cbm_clock_gettime(CLOCK_MONOTONIC, &t);
            rc = cbm_build_registry_from_cache(ctx, changed_files, ci, cache);
            cbm_log_info("pass.timing", "pass", "incr_registry", "elapsed_ms",
                         itoa_buf((int)elapsed_ms(t)));
        }

        /* The parallel delta path keeps precise per-file static resolution
         * and structural fallback. Cross-file static resolver state is not
         * rebuilt globally; the immediate old dependency cone is reparsed. */
        if (rc == 0) {
            cbm_clock_gettime(CLOCK_MONOTONIC, &t);
            rc = cbm_parallel_resolve(ctx, changed_files, ci, cache, &shared_ids, worker_count,
                                      NULL, 0, NULL, NULL, NULL);
            cbm_gbuf_set_next_id(ctx->gbuf, atomic_load(&shared_ids));
            cbm_log_info("pass.timing", "pass", "incr_resolve", "elapsed_ms",
                         itoa_buf((int)elapsed_ms(t)));
        }

        for (int j = 0; j < ci; j++) {
            if (cache[j]) {
                cbm_free_result(cache[j]);
            }
        }
        free(cache);
        return rc;
    } else {
        cbm_log_info("incremental.mode", "mode", "sequential", "changed", itoa_buf(ci));
        CBMFileResult **cache = calloc((size_t)ci, sizeof(*cache));
        if (!cache) {
            return CBM_NOT_FOUND;
        }
        ctx->result_cache = cache;
        int rc = cbm_pipeline_pass_definitions(ctx, changed_files, ci);
        if (rc == 0) {
            rc = cbm_pipeline_pass_lsp_cross(ctx, changed_files, ci, cache);
        }
        if (rc == 0) {
            rc = cbm_pipeline_pass_calls(ctx, changed_files, ci);
        }
        if (rc == 0) {
            rc = cbm_pipeline_pass_usages(ctx, changed_files, ci);
        }
        if (rc == 0) {
            rc = cbm_pipeline_pass_semantic(ctx, changed_files, ci);
        }
        ctx->result_cache = NULL;
        for (int i = 0; i < ci; i++) {
            if (cache[i]) {
                cbm_free_result(cache[i]);
            }
        }
        free(cache);
        return rc;
    }
}

/* Run only publication-blocking structural post-passes. Similarity and semantic
 * edges belong to a derived generation and must not delay a fresh structural
 * delta. Existing derived edges touching replaced nodes disappear by cascade. */
static int run_postpasses(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *changed_files, int ci,
                          const char *project) {
    struct timespec t;

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    int rc = cbm_pipeline_pass_tests(ctx, changed_files, ci);
    cbm_log_info("pass.timing", "pass", "incr_tests", "elapsed_ms", itoa_buf((int)elapsed_ms(t)));
    if (rc != 0) {
        return rc;
    }

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    rc = cbm_pipeline_pass_decorator_tags(ctx->gbuf, project);
    cbm_log_info("pass.timing", "pass", "incr_decorator_tags", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));
    if (rc != 0) {
        return rc;
    }

    cbm_clock_gettime(CLOCK_MONOTONIC, &t);
    rc = cbm_pipeline_pass_configlink(ctx);
    cbm_log_info("pass.timing", "pass", "incr_configlink", "elapsed_ms",
                 itoa_buf((int)elapsed_ms(t)));
    return rc;
}

typedef struct {
    const cbm_gbuf_node_t **items;
    int count;
    int capacity;
    bool failed;
} node_ptr_vec_t;

typedef struct {
    const cbm_gbuf_edge_t **items;
    int count;
    int capacity;
    bool failed;
} edge_ptr_vec_t;

static void collect_node_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    node_ptr_vec_t *vec = userdata;
    if (vec->failed) {
        return;
    }
    if (vec->count >= vec->capacity) {
        int next = vec->capacity > 0 ? vec->capacity * PAIR_LEN : CBM_SZ_64;
        const cbm_gbuf_node_t **grown =
            realloc(vec->items, (size_t)next * sizeof(*grown));
        if (!grown) {
            vec->failed = true;
            return;
        }
        vec->items = grown;
        vec->capacity = next;
    }
    vec->items[vec->count++] = node;
}

static void collect_edge_visitor(const cbm_gbuf_edge_t *edge, void *userdata) {
    edge_ptr_vec_t *vec = userdata;
    if (vec->failed) {
        return;
    }
    if (vec->count >= vec->capacity) {
        int next = vec->capacity > 0 ? vec->capacity * PAIR_LEN : CBM_SZ_64;
        const cbm_gbuf_edge_t **grown =
            realloc(vec->items, (size_t)next * sizeof(*grown));
        if (!grown) {
            vec->failed = true;
            return;
        }
        vec->items = grown;
        vec->capacity = next;
    }
    vec->items[vec->count++] = edge;
}

static int compare_nullable_string(const char *a, const char *b) {
    return strcmp(a ? a : "", b ? b : "");
}

static int compare_node_ptr(const void *lhs, const void *rhs) {
    const cbm_gbuf_node_t *a = *(const cbm_gbuf_node_t *const *)lhs;
    const cbm_gbuf_node_t *b = *(const cbm_gbuf_node_t *const *)rhs;
    int c = compare_nullable_string(a->symbol_id, b->symbol_id);
    if (c == 0) {
        c = compare_nullable_string(a->qualified_name, b->qualified_name);
    }
    if (c == 0) {
        c = compare_nullable_string(a->file_path, b->file_path);
    }
    if (c == 0 && a->start_line != b->start_line) {
        c = a->start_line < b->start_line ? CBM_NOT_FOUND : SKIP_ONE;
    }
    if (c == 0) {
        c = compare_nullable_string(a->label, b->label);
    }
    return c;
}

static int compare_edge_ptr(const void *lhs, const void *rhs) {
    const cbm_gbuf_edge_t *a = *(const cbm_gbuf_edge_t *const *)lhs;
    const cbm_gbuf_edge_t *b = *(const cbm_gbuf_edge_t *const *)rhs;
    if (a->source_id != b->source_id) {
        return a->source_id < b->source_id ? CBM_NOT_FOUND : SKIP_ONE;
    }
    if (a->target_id != b->target_id) {
        return a->target_id < b->target_id ? CBM_NOT_FOUND : SKIP_ONE;
    }
    int c = compare_nullable_string(a->type, b->type);
    if (c == 0) {
        c = compare_nullable_string(a->properties_json, b->properties_json);
    }
    return c;
}

static int fts_apply_node(cbm_store_t *store, const cbm_node_t *node, bool remove) {
    sqlite3 *db = cbm_store_get_db(store);
    if (!db || !node) {
        return CBM_STORE_ERR;
    }
    const char *sql =
        remove ? "INSERT INTO nodes_fts(nodes_fts,rowid,name,qualified_name,label,file_path) "
                 "VALUES('delete',?1,cbm_camel_split(?2),?3,?4,?5);"
               : "INSERT INTO nodes_fts(rowid,name,qualified_name,label,file_path) "
                 "VALUES(?1,cbm_camel_split(?2),?3,?4,?5);";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_int64(stmt, SKIP_ONE, node->id);
    sqlite3_bind_text(stmt, PAIR_LEN, node->name ? node->name : "", CBM_NOT_FOUND,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, CBM_SZ_3, node->qualified_name ? node->qualified_name : "",
                      CBM_NOT_FOUND, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, CBM_SZ_4, node->label ? node->label : "", CBM_NOT_FOUND,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, CBM_SZ_5, node->file_path ? node->file_path : "", CBM_NOT_FOUND,
                      SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt) == SQLITE_DONE ? CBM_STORE_OK : CBM_STORE_ERR;
    sqlite3_finalize(stmt);
    return rc;
}

static int purge_file_nodes(cbm_store_t *store, const char *project, const char *file_path) {
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    if (cbm_store_find_nodes_by_file(store, project, file_path, &nodes, &node_count) !=
        CBM_STORE_OK) {
        return CBM_NOT_FOUND;
    }
    int rc = 0;
    for (int i = 0; i < node_count; i++) {
        if (fts_apply_node(store, &nodes[i], true) != CBM_STORE_OK) {
            rc = CBM_NOT_FOUND;
            break;
        }
    }
    cbm_store_free_nodes(nodes, node_count);
    if (rc == 0 && cbm_store_delete_nodes_by_file(store, project, file_path) != CBM_STORE_OK) {
        rc = CBM_NOT_FOUND;
    }
    return rc;
}

static int persist_changed_hashes(cbm_store_t *store, const char *project,
                                  const incr_file_vec_t *changed_files) {
    for (int i = 0; i < changed_files->count; i++) {
        const cbm_file_info_t *file = &changed_files->items[i];
        struct stat before;
        struct stat after;
        char hash[CBM_CONTENT_HASH_SIZE];
        if (stat(file->path, &before) != 0 ||
            cbm_content_hash_file(file->path, hash) != 0 ||
            stat(file->path, &after) != 0 ||
            stat_mtime_ns(&before) != stat_mtime_ns(&after) ||
            before.st_size != after.st_size ||
            cbm_store_upsert_file_hash(store, project, file->rel_path, hash,
                                       stat_mtime_ns(&after), after.st_size) != CBM_STORE_OK) {
            return CBM_NOT_FOUND;
        }
    }
    return 0;
}

static int merge_slice_into_transaction(cbm_store_t *store, cbm_gbuf_t *slice,
                                        const seed_id_vec_t *seed_ids) {
    node_ptr_vec_t nodes = {0};
    edge_ptr_vec_t edges = {0};
    cbm_gbuf_foreach_node(slice, collect_node_visitor, &nodes);
    cbm_gbuf_foreach_edge(slice, collect_edge_visitor, &edges);
    if (nodes.failed || edges.failed) {
        free(nodes.items);
        free(edges.items);
        return CBM_NOT_FOUND;
    }
    qsort(nodes.items, (size_t)nodes.count, sizeof(*nodes.items), compare_node_ptr);
    qsort(edges.items, (size_t)edges.count, sizeof(*edges.items), compare_edge_ptr);

    int64_t map_count = cbm_gbuf_next_id(slice) + SKIP_ONE;
    int64_t *temp_to_real = calloc((size_t)map_count, sizeof(*temp_to_real));
    if (!temp_to_real) {
        free(nodes.items);
        free(edges.items);
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < seed_ids->count; i++) {
        if (seed_ids->items[i].temp_id > 0 && seed_ids->items[i].temp_id < map_count) {
            temp_to_real[seed_ids->items[i].temp_id] = seed_ids->items[i].real_id;
        }
    }

    int rc = 0;
    for (int i = 0; i < nodes.count && rc == 0; i++) {
        const cbm_gbuf_node_t *node = nodes.items[i];
        if (node->id <= 0 || node->id >= map_count) {
            rc = CBM_NOT_FOUND;
            break;
        }
        if (temp_to_real[node->id] > 0) {
            continue;
        }
        cbm_node_t stored = {
            .project = node->project,
            .label = node->label,
            .name = node->name,
            .qualified_name = node->qualified_name,
            .file_path = node->file_path,
            .start_line = node->start_line,
            .end_line = node->end_line,
            .properties_json = node->properties_json,
            .symbol_id = node->symbol_id,
            .language = node->language,
            .signature = node->signature,
            .origin = node->origin,
            .confidence = node->confidence,
        };
        int64_t real_id = cbm_store_upsert_node(store, &stored);
        if (real_id <= 0) {
            rc = CBM_NOT_FOUND;
            break;
        }
        temp_to_real[node->id] = real_id;
        stored.id = real_id;
        if (fts_apply_node(store, &stored, false) != CBM_STORE_OK) {
            rc = CBM_NOT_FOUND;
        }
    }

    for (int i = 0; i < edges.count && rc == 0; i++) {
        const cbm_gbuf_edge_t *edge = edges.items[i];
        if (edge->source_id <= 0 || edge->source_id >= map_count || edge->target_id <= 0 ||
            edge->target_id >= map_count) {
            rc = CBM_NOT_FOUND;
            break;
        }
        int64_t source_id = temp_to_real[edge->source_id];
        int64_t target_id = temp_to_real[edge->target_id];
        if (source_id <= 0 || target_id <= 0) {
            rc = CBM_NOT_FOUND;
            break;
        }
        cbm_edge_t stored = {
            .project = edge->project,
            .source_id = source_id,
            .target_id = target_id,
            .type = edge->type,
            .properties_json = edge->properties_json,
            .origin = edge->origin,
            .confidence = edge->confidence,
            .evidence_json = edge->evidence_json,
        };
        if (cbm_store_insert_edge(store, &stored) <= 0) {
            rc = CBM_NOT_FOUND;
        }
    }

    free(temp_to_real);
    free(nodes.items);
    free(edges.items);
    return rc;
}

static bool incremental_fault_is(const char *stage) {
    const char *fault = getenv("CBM_TEST_INCREMENTAL_FAULT");
    return fault && strcmp(fault, stage) == 0;
}

static uint64_t digest_text(uint64_t hash, const unsigned char *text) {
    static const uint64_t prime = UINT64_C(1099511628211);
    if (text) {
        while (*text) {
            hash ^= *text++;
            hash *= prime;
        }
    }
    hash ^= UINT8_C(0xff);
    return hash * prime;
}

static int compute_structural_digest(cbm_store_t *store, const char *project,
                                     char out[CBM_SZ_64]) {
    sqlite3 *db = cbm_store_get_db(store);
    sqlite3_stmt *stmt = NULL;
    uint64_t hash = UINT64_C(1469598103934665603);
    const char *node_sql =
        "SELECT symbol_id,label,qualified_name,file_path,start_line,end_line,signature,origin "
        "FROM nodes WHERE project=?1 ORDER BY symbol_id;";
    if (!db || sqlite3_prepare_v2(db, node_sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        return CBM_NOT_FOUND;
    }
    sqlite3_bind_text(stmt, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        for (int col = 0; col < INCR_NODE_DIGEST_COLS; col++) {
            if (col == INCR_START_LINE_COL || col == INCR_END_LINE_COL) {
                char number[CBM_SZ_32];
                snprintf(number, sizeof(number), "%d", sqlite3_column_int(stmt, col));
                hash = digest_text(hash, (const unsigned char *)number);
            } else {
                hash = digest_text(hash, sqlite3_column_text(stmt, col));
            }
        }
    }
    sqlite3_finalize(stmt);

    const char *edge_sql =
        "SELECT source.symbol_id,target.symbol_id,e.type,e.properties,e.origin,e.confidence,"
        "e.evidence FROM edges e JOIN nodes source ON source.id=e.source_id "
        "JOIN nodes target ON target.id=e.target_id WHERE e.project=?1 "
        "AND e.type NOT IN ('SIMILAR_TO','SEMANTICALLY_RELATED','CO_CHANGES_WITH') "
        "ORDER BY source.symbol_id,target.symbol_id,e.type,e.properties,e.origin,e.evidence;";
    if (sqlite3_prepare_v2(db, edge_sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        return CBM_NOT_FOUND;
    }
    sqlite3_bind_text(stmt, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        for (int col = 0; col < INCR_EDGE_DIGEST_COLS; col++) {
            if (col == INCR_CONFIDENCE_COL) {
                char number[CBM_SZ_32];
                snprintf(number, sizeof(number), "%.9g", sqlite3_column_double(stmt, col));
                hash = digest_text(hash, (const unsigned char *)number);
            } else {
                hash = digest_text(hash, sqlite3_column_text(stmt, col));
            }
        }
    }
    sqlite3_finalize(stmt);
    snprintf(out, CBM_SZ_64, "fnv1a64:%016llx", (unsigned long long)hash);
    return 0;
}

static void utc_now(char out[CBM_SZ_32]) {
    time_t now = time(NULL);
    struct tm utc = {0};
#ifdef _WIN32
    gmtime_s(&utc, &now);
#else
    gmtime_r(&now, &utc);
#endif
    strftime(out, CBM_SZ_32, "%Y-%m-%dT%H:%M:%SZ", &utc);
}

static int advance_structural_generation(cbm_store_t *store, const char *project) {
    cbm_project_t previous = {0};
    if (cbm_store_get_project(store, project, &previous) != CBM_STORE_OK) {
        return CBM_NOT_FOUND;
    }
    char digest[CBM_SZ_64];
    char indexed_at[CBM_SZ_32];
    utc_now(indexed_at);
    int rc = compute_structural_digest(store, project, digest);
    if (rc == 0) {
        rc = cbm_store_set_project_freshness(
            store, project, previous.generation + SKIP_ONE, previous.commit_hash,
            previous.dirty_fingerprint, indexed_at, previous.derived_indexed_at);
    }
    if (rc == CBM_STORE_OK) {
        rc = cbm_store_set_project_structural_metadata(
            store, project, digest,
            previous.parser_version && previous.parser_version[0] ? previous.parser_version
                                                                  : "schema2-structural-v1");
    }
    cbm_project_free_fields(&previous);
    return rc == CBM_STORE_OK ? 0 : CBM_NOT_FOUND;
}

static int apply_delta_transaction(cbm_store_t *store, const char *project, cbm_gbuf_t *slice,
                                   const seed_id_vec_t *seed_ids,
                                   const incr_file_vec_t *changed_files,
                                   const incr_file_vec_t *parse_files, char **deleted,
                                   int deleted_count, const _Atomic int *cancelled) {
    if (cbm_store_begin(store) != CBM_STORE_OK) {
        return CBM_NOT_FOUND;
    }
    int rc = 0;
    for (int i = 0; i < parse_files->count && rc == 0; i++) {
        rc = purge_file_nodes(store, project, parse_files->items[i].rel_path);
    }
    for (int i = 0; i < deleted_count && rc == 0; i++) {
        if (!file_vec_contains(parse_files, deleted[i])) {
            rc = purge_file_nodes(store, project, deleted[i]);
        }
        if (rc == 0 && cbm_store_delete_file_hash(store, project, deleted[i]) != CBM_STORE_OK) {
            rc = CBM_NOT_FOUND;
        }
    }
    if (rc == 0 && incremental_fault_is("after_delete")) {
        cbm_store_rollback(store);
        return SKIP_ONE; /* explicit test fault: do not trigger full fallback */
    }
    if (rc == 0) {
        rc = merge_slice_into_transaction(store, slice, seed_ids);
    }
    if (rc == 0) {
        rc = persist_changed_hashes(store, project, changed_files);
    }
    if (rc == 0) {
        rc = advance_structural_generation(store, project);
    }
    if (rc == 0 && incremental_fault_is("before_commit")) {
        cbm_store_rollback(store);
        return SKIP_ONE;
    }
    if (rc == 0 && cancelled && atomic_load(cancelled)) {
        rc = CBM_NOT_FOUND;
    }
    if (rc != 0) {
        cbm_store_rollback(store);
        return rc;
    }
    if (cbm_store_commit(store) != CBM_STORE_OK) {
        cbm_store_rollback(store);
        return CBM_NOT_FOUND;
    }
    return 0;
}

/* ── Incremental pipeline entry point ────────────────────────────── */

int cbm_pipeline_run_incremental(cbm_pipeline_t *p, const char *db_path, cbm_file_info_t *files,
                                 int file_count) {
    struct timespec t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &t0);

    const char *project = cbm_pipeline_project_name(p);
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        cbm_log_error("incremental.err", "msg", "open_db_failed", "path", db_path);
        return CBM_NOT_FOUND;
    }

    cbm_file_hash_t *stored = NULL;
    int stored_count = 0;
    bool *is_changed = NULL;
    char **deleted = NULL;
    cbm_file_hash_t *mode_skipped = NULL;
    int mode_skipped_count = 0;
    incr_file_vec_t changed_files = {0};
    incr_file_vec_t parse_files = {0};
    seed_id_vec_t seed_ids = {0};
    cbm_gbuf_t *slice = NULL;
    cbm_registry_t *registry = NULL;
    cbm_path_alias_collection_t *path_aliases = NULL;
    int deleted_count = 0;
    int result = CBM_NOT_FOUND;

    if (cbm_store_get_file_hashes(store, project, &stored, &stored_count) != CBM_STORE_OK) {
        cbm_log_error("incremental.err", "msg", "read_hashes_failed");
        goto cleanup;
    }

    int n_changed = 0;
    int n_unchanged = 0;
    is_changed = classify_files(files, file_count, stored, stored_count, &n_changed, &n_unchanged);
    if (file_count > 0 && !is_changed) {
        goto cleanup;
    }

    deleted_count =
        find_deleted_files(cbm_pipeline_repo_path(p), files, file_count, stored, stored_count,
                           &deleted, &mode_skipped, &mode_skipped_count);

    cbm_log_info("incremental.classify", "changed", itoa_buf(n_changed), "unchanged",
                 itoa_buf(n_unchanged), "deleted", itoa_buf(deleted_count), "mode_skipped",
                 itoa_buf(mode_skipped_count));

    /* Explicit enrichment needs the whole graph: structural slices cannot
     * discover new similarity edges against unchanged files. */
    if (cbm_pipeline_get_mode(p) == CBM_MODE_FULL) {
        result = CBM_NOT_FOUND;
        goto cleanup;
    }

    if (n_changed == 0 && deleted_count == 0) {
        cbm_log_info("incremental.noop", "reason", "no_changes");
        result = 0;
        goto cleanup;
    }

    /* A large delta is deliberately routed back to pipeline.c. That path
     * builds a validated sibling DB and atomically publishes it, preserving
     * the current live index until the full candidate is ready. */
    if (mode_skipped_count == 0 &&
        cbm_pipeline_incremental_requires_rebuild(file_count, stored_count, n_changed,
                                                  deleted_count)) {
        cbm_log_info("incremental.route", "path", "full_rebuild", "reason", "over_20_percent");
        result = CBM_NOT_FOUND;
        goto cleanup;
    }

    for (int i = 0; i < file_count; i++) {
        if (is_changed[i] && (file_vec_append(&changed_files, &files[i]) != 0 ||
                              file_vec_append(&parse_files, &files[i]) != 0)) {
            goto cleanup;
        }
    }

    if (expand_dependency_cone(store, project, files, file_count, &changed_files, deleted,
                               deleted_count, &parse_files) != 0) {
        goto cleanup;
    }
    if (mode_skipped_count == 0 &&
        cbm_pipeline_incremental_requires_rebuild(file_count, stored_count, parse_files.count,
                                                  deleted_count)) {
        cbm_log_info("incremental.route", "path", "full_rebuild", "reason",
                     "dependency_cone_over_20_percent");
        result = CBM_NOT_FOUND;
        goto cleanup;
    }

    cbm_log_info("incremental.reparse", "changed", itoa_buf(changed_files.count), "cone",
                 itoa_buf(parse_files.count));

    slice = cbm_gbuf_new(project, cbm_pipeline_repo_path(p));
    registry = cbm_registry_new();
    if (!slice || !registry ||
        seed_existing_nodes(store, project, slice, &parse_files, deleted, deleted_count,
                            &seed_ids) != 0) {
        goto cleanup;
    }

    for (int i = 0; i < parse_files.count; i++) {
        if (add_file_structure(slice, project, parse_files.items[i].rel_path) != 0) {
            goto cleanup;
        }
    }
    cbm_gbuf_foreach_node(slice, registry_visitor, registry);
    cbm_log_info("incremental.registry_seed", "symbols", itoa_buf(cbm_registry_size(registry)),
                 "seed_nodes", itoa_buf(seed_ids.count));

    path_aliases = cbm_load_path_aliases(cbm_pipeline_repo_path(p));
    cbm_pipeline_set_pkgmap(cbm_pkgmap_build_from_repo(cbm_pipeline_repo_path(p), files, file_count,
                                                       project));

    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = cbm_pipeline_repo_path(p),
        .gbuf = slice,
        .registry = registry,
        .cancelled = cbm_pipeline_cancelled_ptr(p),
        .mode = cbm_pipeline_get_mode(p),
        .path_aliases = path_aliases,
    };

    if (parse_files.count > 0) {
        if (run_extract_resolve(&ctx, parse_files.items, parse_files.count) != 0 ||
            cbm_pipeline_pass_k8s(&ctx, parse_files.items, parse_files.count) != 0 ||
            run_postpasses(&ctx, parse_files.items, parse_files.count, project) != 0) {
            goto cleanup;
        }
    }

    result = apply_delta_transaction(store, project, slice, &seed_ids, &changed_files,
                                     &parse_files, deleted, deleted_count,
                                     cbm_pipeline_cancelled_ptr(p));
    if (result == 0) {
        if (!cbm_store_check_integrity(store)) {
            cbm_log_error("incremental.err", "msg", "post_commit_integrity_failed");
            result = CBM_NOT_FOUND;
        } else if (cbm_store_checkpoint(store) != CBM_STORE_OK) {
            cbm_log_warn("incremental.checkpoint", "status", "deferred");
        }
        if (result == 0 && cbm_artifact_exists(cbm_pipeline_repo_path(p))) {
            cbm_artifact_export(db_path, cbm_pipeline_repo_path(p), project, CBM_ARTIFACT_FAST);
        }
    }

cleanup:
    cbm_pkgmap_free(cbm_pipeline_get_pkgmap());
    cbm_pipeline_set_pkgmap(NULL);
    cbm_path_alias_collection_free(path_aliases);
    cbm_registry_free(registry);
    cbm_gbuf_free(slice);
    free(seed_ids.items);
    free(changed_files.items);
    free(parse_files.items);
    free(is_changed);
    for (int i = 0; i < deleted_count; i++) {
        free(deleted[i]);
    }
    free(deleted);
    free_mode_skipped(mode_skipped, mode_skipped_count);
    cbm_store_free_file_hashes(stored, stored_count);
    cbm_store_close(store);

    if (result == 0) {
        cbm_log_info("incremental.done", "elapsed_ms", itoa_buf((int)elapsed_ms(t0)));
    }
    return result;
}
