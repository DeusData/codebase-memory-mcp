/*
 * pass_githistory.c — Analyze git log to find change coupling.
 *
 * Runs `git log --name-only --since=6 months ago` and computes
 * file pairs that change together frequently. Creates FILE_CHANGES_WITH
 * edges between File nodes with coupling_score properties.
 *
 * Skips commits with >20 files (refactoring/merge noise).
 * Requires minimum 3 co-changes for an edge.
 *
 * Depends on: pass_structure having created File nodes
 */
#include "foundation/constants.h"

enum { GH_RING = 4, GH_RING_MASK = 3, GH_INIT_CAP = 16, GH_MIN_COMMITS = 3 };

#define SLEN(s) (sizeof(s) - 1)
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/str_util.h"

/* Minimum coupling score to create an edge */
#define MIN_COUPLING_SCORE 0.3

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *itoa_log(int val) {
    static CBM_TLS char bufs[GH_RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & GH_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

static bool ends_with(const char *s, size_t slen, const char *suffix) {
    size_t sflen = strlen(suffix);
    return slen >= sflen && strcmp(s + slen - sflen, suffix) == 0;
}

bool cbm_is_trackable_file(const char *path) {
    if (!path) {
        return false;
    }
    /* Skip directory prefixes */
#define LEN_NODE_MODULES_SLASH 13 /* strlen("node_modules/") */
    if (strncmp(path, ".git/", SLEN(".git/")) == 0 ||
        strncmp(path, "node_modules/", LEN_NODE_MODULES_SLASH) == 0 ||
        strncmp(path, "vendor/", SLEN("vendor/")) == 0 ||
        strncmp(path, "__pycache__/", SLEN("__pycache__/")) == 0 ||
        strncmp(path, ".cache/", SLEN(".cache/")) == 0) {
        return false;
    }
    /* Skip lock/generated file names */
    const char *base = strrchr(path, '/');
    base = base ? base + SKIP_ONE : path;
    if (strcmp(base, "package-lock.json") == 0 || strcmp(base, "yarn.lock") == 0 ||
        strcmp(base, "pnpm-lock.yaml") == 0 || strcmp(base, "Cargo.lock") == 0 ||
        strcmp(base, "poetry.lock") == 0 || strcmp(base, "composer.lock") == 0 ||
        strcmp(base, "Gemfile.lock") == 0 || strcmp(base, "Pipfile.lock") == 0) {
        return false;
    }
    /* Skip non-source file extensions */
    size_t len = strlen(path);
    if (ends_with(path, len, ".lock") || ends_with(path, len, ".sum") ||
        ends_with(path, len, ".min.js") || ends_with(path, len, ".min.css") ||
        ends_with(path, len, ".map") || ends_with(path, len, ".wasm") ||
        ends_with(path, len, ".png") || ends_with(path, len, ".jpg") ||
        ends_with(path, len, ".gif") || ends_with(path, len, ".ico") ||
        ends_with(path, len, ".svg")) {
        return false;
    }
    return true;
}

/* ── Commit parsing ───────────────────────────────────────────────── */

typedef struct {
    char **files;
    int count;
    int cap;
    long long timestamp; /* unix epoch of this commit; 0 when unknown */
} commit_t;

static void commit_add_file(commit_t *c, const char *file) {
    if (c->count >= c->cap) {
        c->cap = c->cap ? c->cap * PAIR_LEN : GH_INIT_CAP;
        c->files = safe_realloc(c->files, c->cap * sizeof(char *));
    }
    c->files[c->count++] = strdup(file);
}

static void commit_free(commit_t *c) {
    for (int i = 0; i < c->count; i++) {
        free(c->files[i]);
    }
    free(c->files);
}

/* ── git log parsing (popen "git log") ────────────────────────────── */

static int parse_git_log(const char *repo_path, commit_t **out, int *out_count) {
    *out = NULL;
    *out_count = 0;

    if (!cbm_validate_shell_path_arg(repo_path)) {
        return CBM_NOT_FOUND;
    }

    char cmd[CBM_SZ_1K];
#ifdef _WIN32
    /* cmd.exe does not recognize single quotes, and '/dev/null' is a POSIX path. */
    const char *null_dev = "NUL";
#else
    const char *null_dev = "/dev/null";
#endif
    /* git -C "<path>" works on both cmd.exe and POSIX shells. Double quotes are
     * safe here because cbm_validate_shell_arg (above) rejects ", $, `, \ and the
     * other shell metacharacters that would otherwise be active inside them. */
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" log --name-only --pretty=format:COMMIT:%%H:%%ct "
             "--since=\"1 year ago\" --max-count=" CBM_STRINGIFY(
                 CBM_GITHISTORY_HISTORY_COMMIT_LIMIT) " 2>%s",
             repo_path, null_dev);

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }

    int cap = CBM_SZ_64;
    commit_t *commits = malloc(cap * sizeof(commit_t));
    int count = 0;
    commit_t current = {0};

    char line[CBM_SZ_1K];
    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        if (strncmp(line, "COMMIT:", SLEN("COMMIT:")) == 0) {
            if (current.count > 0) {
                if (count >= cap) {
                    cap *= PAIR_LEN;
                    commits = safe_realloc(commits, cap * sizeof(commit_t));
                }
                commits[count++] = current;
                memset(&current, 0, sizeof(current));
            }
            /* Parse the unix timestamp from "COMMIT:<hash>:<unix_epoch>".
             * Older callers / stripped-down git output without %ct land on 0. */
            const char *hash_end = strchr(line + SLEN("COMMIT:"), ':');
            if (hash_end) {
                current.timestamp = strtoll(hash_end + 1, NULL, 10);
            }
            continue;
        }

        if (cbm_is_trackable_file(line)) {
            commit_add_file(&current, line);
        }
    }
    if (current.count > 0) {
        if (count >= cap) {
            cap *= PAIR_LEN;
            commits = safe_realloc(commits, cap * sizeof(commit_t));
        }
        commits[count++] = current;
    } else {
        commit_free(&current);
    }

    cbm_pclose(fp);
    *out = commits;
    *out_count = count;
    return 0;
}

/* Callback to free hash table entries. */
static void free_counter(const char *key, void *val, void *ud) {
    (void)ud;
    safe_str_free(&key);
    free(val);
}

static void free_index_value(const char *key, void *val, void *ud) {
    (void)key;
    (void)ud;
    free(val);
}

/* ── Standalone coupling computation (testable) ──────────────────── */

typedef struct {
    char *file_a; /* borrowed from commits through collection */
    char *file_b;
    int co_change_count;
    long long last_co_change;
} coupling_pair_t;

/* Context for collect_coupling_result callback. */
typedef struct {
    CBMHashTable *file_counts;
    cbm_change_coupling_t *out;
    int max_out;
    double min_coupling_score;
    cbm_change_coupling_result_t result;
} collect_coupling_ctx_t;

/* Positive means a is preferred over b. Coupling strength leads, then
 * observation support and recency; lexical order makes exact ties canonical. */
static int coupling_quality_compare(const cbm_change_coupling_t *a,
                                    const cbm_change_coupling_t *b) {
    if (a->coupling_score != b->coupling_score) {
        return a->coupling_score > b->coupling_score ? 1 : -1;
    }
    if (a->co_change_count != b->co_change_count) {
        return a->co_change_count > b->co_change_count ? 1 : -1;
    }
    if (a->last_co_change != b->last_co_change) {
        return a->last_co_change > b->last_co_change ? 1 : -1;
    }
    int cmp = strcmp(a->file_a, b->file_a);
    if (cmp != 0) {
        return cmp < 0 ? 1 : -1;
    }
    cmp = strcmp(a->file_b, b->file_b);
    return cmp == 0 ? 0 : (cmp < 0 ? 1 : -1);
}

static void coupling_swap(cbm_change_coupling_t *a, cbm_change_coupling_t *b) {
    cbm_change_coupling_t tmp = *a;
    *a = *b;
    *b = tmp;
}

/* Maintain a worst-first heap so a bounded output buffer always retains the
 * strongest candidates seen across the complete hash-table traversal. */
static void coupling_heap_push(collect_coupling_ctx_t *ctx,
                               const cbm_change_coupling_t *candidate) {
    if (ctx->max_out <= 0 || !ctx->out) {
        return;
    }

    if (ctx->result.written < ctx->max_out) {
        int child = ctx->result.written++;
        ctx->out[child] = *candidate;
        while (child > 0) {
            int parent = (child - 1) / 2;
            if (coupling_quality_compare(&ctx->out[child], &ctx->out[parent]) >= 0) {
                break;
            }
            coupling_swap(&ctx->out[child], &ctx->out[parent]);
            child = parent;
        }
        return;
    }

    if (coupling_quality_compare(candidate, &ctx->out[0]) <= 0) {
        return;
    }
    ctx->out[0] = *candidate;
    int parent = 0;
    for (;;) {
        int left = parent * 2 + 1;
        if (left >= ctx->result.written) {
            break;
        }
        int right = left + 1;
        int worse = left;
        if (right < ctx->result.written &&
            coupling_quality_compare(&ctx->out[right], &ctx->out[left]) < 0) {
            worse = right;
        }
        if (coupling_quality_compare(&ctx->out[parent], &ctx->out[worse]) <= 0) {
            break;
        }
        coupling_swap(&ctx->out[parent], &ctx->out[worse]);
        parent = worse;
    }
}

static int coupling_best_first_qsort(const void *lhs, const void *rhs) {
    int cmp = coupling_quality_compare(lhs, rhs);
    return cmp > 0 ? -1 : (cmp < 0 ? 1 : 0);
}

static void collect_coupling_cb(const char *pair_key, void *val, void *ud) {
    (void)pair_key;
    collect_coupling_ctx_t *cctx = ud;
    coupling_pair_t *pair = val;
    if (pair->co_change_count < GH_MIN_COMMITS) {
        return;
    }

    int *count_a = cbm_ht_get(cctx->file_counts, pair->file_a);
    int *count_b = cbm_ht_get(cctx->file_counts, pair->file_b);
    if (!count_a || !count_b) {
        return;
    }
    int min_total = *count_a < *count_b ? *count_a : *count_b;
    if (min_total == 0) {
        return;
    }

    double score = (double)pair->co_change_count / (double)min_total;
    double min_score =
        cctx->min_coupling_score > 0.0 ? cctx->min_coupling_score : MIN_COUPLING_SCORE;
    if (score < min_score) {
        return;
    }
    cctx->result.eligible++;

    cbm_change_coupling_t candidate = {
        .file_a = pair->file_a,
        .file_b = pair->file_b,
        .co_change_count = pair->co_change_count,
        .coupling_score = score,
        .last_co_change = pair->last_co_change,
    };
    coupling_heap_push(cctx, &candidate);
}

void cbm_change_coupling_paths_free(cbm_change_coupling_t *items, int count) {
    if (!items || count <= 0) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(items[i].file_a);
        free(items[i].file_b);
        items[i].file_a = NULL;
        items[i].file_b = NULL;
    }
}

/* The selection heap borrows input paths so traversal performs no per-candidate
 * string allocation. Duplicate only the final K rows after ranking: O(K) string
 * allocations and O(total selected path bytes), with exact paths at any length. */
static bool coupling_selected_paths_duplicate(cbm_change_coupling_t *items, int count) {
    for (int i = 0; i < count; i++) {
        char *file_a = cbm_strdup(items[i].file_a);
        char *file_b = cbm_strdup(items[i].file_b);
        if (!file_a || !file_b) {
            free(file_a);
            free(file_b);
            cbm_change_coupling_paths_free(items, i);
            for (int j = i; j < count; j++) {
                items[j].file_a = NULL;
                items[j].file_b = NULL;
            }
            return false;
        }
        items[i].file_a = file_a;
        items[i].file_b = file_b;
    }
    return true;
}

cbm_change_coupling_result_t cbm_compute_change_coupling_result(const cbm_commit_files_t *commits,
                                                                int commit_count,
                                                                cbm_change_coupling_t *out,
                                                                int max_out,
                                                                double min_coupling_score) {
    /* Aggregation remains expected O(file observations + pair observations).
     * The bounded strongest-first selection adds O(E log K) time for E
     * eligible pairs and output budget K, with O(K + selected path bytes)
     * caller-owned output.
     * Keeping timestamp and path references in the pair value avoids the
     * former second hash table and its duplicate key allocation. */
    cbm_change_coupling_result_t result = {0};
    if (commit_count <= 0 || !commits) {
        return result;
    }
    if (max_out > 0 && !out) {
        result.allocation_failed = 1;
        return result;
    }

    CBMHashTable *file_counts = cbm_ht_create(CBM_SZ_1K);
    CBMHashTable *pair_counts = cbm_ht_create(CBM_SZ_2K);
    if (!file_counts || !pair_counts) {
        cbm_ht_free(file_counts);
        cbm_ht_free(pair_counts);
        result.allocation_failed = 1;
        return result;
    }

    for (int c = 0; c < commit_count; c++) {
        if (commits[c].count > CBM_GITHISTORY_MAX_FILES_PER_COMMIT) {
            continue;
        }

        for (int i = 0; i < commits[c].count; i++) {
            int *val = cbm_ht_get(file_counts, commits[c].files[i]);
            if (val) {
                (*val)++;
            } else {
                int *nv = malloc(sizeof(int));
                char *key = cbm_strdup(commits[c].files[i]);
                if (!nv || !key) {
                    free(nv);
                    free(key);
                    goto allocation_failed;
                }
                *nv = SKIP_ONE;
                cbm_ht_set(file_counts, key, nv);
                if (cbm_ht_get(file_counts, key) != nv) {
                    free(nv);
                    free(key);
                    goto allocation_failed;
                }
            }
        }

        for (int i = 0; i < commits[c].count; i++) {
            for (int j = i + SKIP_ONE; j < commits[c].count; j++) {
                char *a = commits[c].files[i];
                char *b = commits[c].files[j];
                if (strcmp(a, b) > 0) {
                    char *t = a;
                    a = b;
                    b = t;
                }
                size_t la = strlen(a);
                size_t lb = strlen(b);
                if (lb > SIZE_MAX - 2 || la > SIZE_MAX - lb - 2) {
                    goto allocation_failed;
                }
                size_t pk_len = la + SKIP_ONE + lb + SKIP_ONE;
                char *pk = malloc(pk_len);
                if (!pk) {
                    goto allocation_failed;
                }
                memcpy(pk, a, la);
                pk[la] = '\x01';
                memcpy(pk + la + SKIP_ONE, b, lb + SKIP_ONE);

                coupling_pair_t *pair = cbm_ht_get(pair_counts, pk);
                if (pair) {
                    pair->co_change_count++;
                    if (commits[c].timestamp > pair->last_co_change) {
                        pair->last_co_change = commits[c].timestamp;
                    }
                    free(pk);
                } else {
                    pair = malloc(sizeof(*pair));
                    if (!pair) {
                        free(pk);
                        goto allocation_failed;
                    }
                    *pair = (coupling_pair_t){
                        .file_a = a,
                        .file_b = b,
                        .co_change_count = 1,
                        .last_co_change = commits[c].timestamp,
                    };
                    cbm_ht_set(pair_counts, pk, pair);
                    if (cbm_ht_get(pair_counts, pk) != pair) {
                        free(pair);
                        free(pk);
                        goto allocation_failed;
                    }
                }
            }
        }
    }

    collect_coupling_ctx_t cctx = {
        .file_counts = file_counts,
        .out = out,
        .max_out = max_out > 0 ? max_out : 0,
        .min_coupling_score = min_coupling_score,
    };
    cbm_ht_foreach(pair_counts, collect_coupling_cb, &cctx);
    result = cctx.result;
    result.omitted = result.eligible - result.written;
    if (result.written > 1) {
        qsort(out, (size_t)result.written, sizeof(*out), coupling_best_first_qsort);
    }
    if (!coupling_selected_paths_duplicate(out, result.written)) {
        goto allocation_failed;
    }
    goto cleanup;

allocation_failed:
    result = (cbm_change_coupling_result_t){.allocation_failed = 1};
cleanup:
    cbm_ht_foreach(pair_counts, free_counter, NULL);
    cbm_ht_free(pair_counts);
    cbm_ht_foreach(file_counts, free_counter, NULL);
    cbm_ht_free(file_counts);

    return result;
}

int cbm_compute_change_coupling_with_threshold(const cbm_commit_files_t *commits, int commit_count,
                                               cbm_change_coupling_t *out, int max_out,
                                               double min_coupling_score) {
    return cbm_compute_change_coupling_result(commits, commit_count, out, max_out,
                                              min_coupling_score)
        .written;
}

int cbm_compute_change_coupling(const cbm_commit_files_t *commits, int commit_count,
                                cbm_change_coupling_t *out, int max_out) {
    return cbm_compute_change_coupling_with_threshold(commits, commit_count, out, max_out, 0.0);
}

/* ── Split pass: compute (I/O-bound) + apply (gbuf writes) ───────── */

void cbm_file_temporal_free(cbm_file_temporal_t *items, int count) {
    if (!items) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(items[i].file_path);
    }
    free(items);
}

int cbm_compute_file_temporal(const cbm_commit_files_t *commits, int commit_count,
                              cbm_file_temporal_t **out, int *out_count) {
    if (!out || !out_count || commit_count < 0 || (commit_count > 0 && !commits)) {
        return CBM_NOT_FOUND;
    }
    *out = NULL;
    *out_count = 0;
    if (commit_count == 0) {
        return 0;
    }

    CBMHashTable *file_idx = cbm_ht_create(CBM_SZ_1K);
    if (!file_idx) {
        return CBM_NOT_FOUND;
    }

    cbm_file_temporal_t *items = NULL;
    int count = 0;
    int capacity = 0;
    for (int c = 0; c < commit_count; c++) {
        if (commits[c].count > CBM_GITHISTORY_MAX_FILES_PER_COMMIT) {
            continue;
        }
        for (int f = 0; f < commits[c].count; f++) {
            const char *path = commits[c].files[f];
            if (!path) {
                continue;
            }
            int *idx = cbm_ht_get(file_idx, path);
            if (idx) {
                if (*idx < 0 || *idx >= count) {
                    goto fail;
                }
                items[*idx].change_count++;
                if (commits[c].timestamp > items[*idx].last_modified) {
                    items[*idx].last_modified = commits[c].timestamp;
                }
                continue;
            }

            if (count == capacity) {
                if (capacity > INT_MAX / 2) {
                    goto fail;
                }
                int next_capacity = capacity == 0 ? CBM_SZ_256 : capacity * 2;
                if ((size_t)next_capacity > SIZE_MAX / sizeof(*items)) {
                    goto fail;
                }
                cbm_file_temporal_t *grown = realloc(items, (size_t)next_capacity * sizeof(*items));
                if (!grown) {
                    goto fail;
                }
                items = grown;
                capacity = next_capacity;
            }

            char *owned_path = cbm_strdup(path);
            int *new_idx = malloc(sizeof(*new_idx));
            if (!owned_path || !new_idx) {
                free(owned_path);
                free(new_idx);
                goto fail;
            }
            *new_idx = count;
            cbm_ht_set(file_idx, owned_path, new_idx);
            if (cbm_ht_get(file_idx, owned_path) != new_idx) {
                free(owned_path);
                free(new_idx);
                goto fail;
            }

            items[count].file_path = owned_path;
            items[count].change_count = 1;
            items[count].last_modified = commits[c].timestamp;
            count++;
        }
    }

    cbm_ht_foreach(file_idx, free_index_value, NULL);
    cbm_ht_free(file_idx);
    if (count == 0) {
        free(items);
        items = NULL;
    }
    *out = items;
    *out_count = count;
    return 0;

fail:
    cbm_ht_foreach(file_idx, free_index_value, NULL);
    cbm_ht_free(file_idx);
    cbm_file_temporal_free(items, count);
    return CBM_NOT_FOUND;
}

/* Compute change couplings without touching the graph buffer.
 * Can run on a separate thread while other passes use the gbuf. */
static int coupling_output_capacity(const commit_t *commits, int commit_count, int requested) {
    int capacity = 0;
    for (int i = 0; i < commit_count && capacity < requested; i++) {
        int file_count = commits[i].count;
        if (file_count < 2 || file_count > CBM_GITHISTORY_MAX_FILES_PER_COMMIT) {
            continue;
        }
        int pair_count = file_count * (file_count - 1) / 2;
        capacity = pair_count >= requested - capacity ? requested : capacity + pair_count;
    }
    return capacity;
}

int cbm_pipeline_githistory_compute_with_limits(const char *repo_path,
                                                cbm_githistory_result_t *result,
                                                double min_coupling_score, int max_couplings) {
    result->couplings = NULL;
    result->count = 0;
    result->commit_count = 0;
    result->file_temporal = NULL;
    result->file_temporal_count = 0;

    commit_t *commits = NULL;
    int commit_count = 0;
    int rc = parse_git_log(repo_path, &commits, &commit_count);
    if (rc != 0 || commit_count == 0) {
        free(commits);
        return 0;
    }

    result->commit_count = commit_count;

    /* Convert to testable format */
    cbm_commit_files_t *cf = calloc((size_t)commit_count, sizeof(cbm_commit_files_t));
    if (!cf) {
        for (int c = 0; c < commit_count; c++) {
            commit_free(&commits[c]);
        }
        free(commits);
        return 0;
    }
    for (int c = 0; c < commit_count; c++) {
        cf[c].files = commits[c].files;
        cf[c].count = commits[c].count;
        cf[c].timestamp = commits[c].timestamp;
    }

    if (max_couplings <= 0) {
        max_couplings = CBM_GITHISTORY_DEFAULT_MAX_COUPLINGS;
    } else if (max_couplings > CBM_GITHISTORY_MAX_COUPLINGS_LIMIT) {
        max_couplings = CBM_GITHISTORY_MAX_COUPLINGS_LIMIT;
    }
    /* A wide configured budget must not reserve space for pairs the parsed
     * history cannot possibly contain. This O(commits) bound is saturated at
     * the requested K, adds constant memory, and preserves exact selection. */
    int coupling_capacity = coupling_output_capacity(commits, commit_count, max_couplings);
    cbm_change_coupling_t *couplings =
        coupling_capacity > 0 ? malloc((size_t)coupling_capacity * sizeof(cbm_change_coupling_t))
                              : NULL;
    cbm_change_coupling_result_t coupling_result = {0};
    if (couplings) {
        coupling_result = cbm_compute_change_coupling_result(cf, commit_count, couplings,
                                                             coupling_capacity, min_coupling_score);
    } else if (coupling_capacity > 0) {
        coupling_result.allocation_failed = 1;
    }
    if (coupling_result.allocation_failed) {
        cbm_log_error("pass.githistory.alloc_failed", "phase", "couplings");
    } else if (coupling_result.omitted > 0) {
        cbm_log_warn("pass.githistory.couplings_partial", "written",
                     itoa_log(coupling_result.written), "eligible",
                     itoa_log(coupling_result.eligible), "omitted",
                     itoa_log(coupling_result.omitted), "path_too_long",
                     itoa_log(coupling_result.path_too_long));
    }
    int coupling_count = coupling_result.written;

    /* Exact per-file temporal aggregation remains expected O(observations)
     * while storing O(unique paths + path bytes). Unlike the former fixed
     * array, no valid tail entries or long paths are silently truncated. */
    if (cbm_compute_file_temporal(cf, commit_count, &result->file_temporal,
                                  &result->file_temporal_count) != 0) {
        cbm_log_error("pass.githistory.alloc_failed", "phase", "file_temporal");
    }

    free(cf);
    for (int c = 0; c < commit_count; c++) {
        commit_free(&commits[c]);
    }
    free(commits);

    result->couplings = couplings;
    result->count = coupling_count;
    return 0;
}

int cbm_pipeline_githistory_compute(const char *repo_path, cbm_githistory_result_t *result) {
    return cbm_pipeline_githistory_compute_with_limits(repo_path, result, 0.0,
                                                       CBM_GITHISTORY_DEFAULT_MAX_COUPLINGS);
}

/* Apply pre-computed couplings to the graph buffer (must be on main thread). */
int cbm_pipeline_githistory_apply(cbm_pipeline_ctx_t *ctx, const cbm_githistory_result_t *result) {
    int edge_count = 0;

    for (int i = 0; i < result->count; i++) {
        const cbm_change_coupling_t *cc = &result->couplings[i];

        char *qn_a = cbm_pipeline_fqn_compute(ctx->project_name, cc->file_a, "__file__");
        char *qn_b = cbm_pipeline_fqn_compute(ctx->project_name, cc->file_b, "__file__");

        const cbm_gbuf_node_t *node_a = cbm_gbuf_find_by_qn(ctx->gbuf, qn_a);
        const cbm_gbuf_node_t *node_b = cbm_gbuf_find_by_qn(ctx->gbuf, qn_b);

        free(qn_a);
        free(qn_b);

        if (!node_a || !node_b || node_a->id == node_b->id) {
            continue;
        }

        char props[CBM_SZ_128];
        snprintf(props, sizeof(props),
                 "{\"co_changes\":%d,\"coupling_score\":%.2f,\"last_co_change\":%lld}",
                 cc->co_change_count, cc->coupling_score, cc->last_co_change);

        cbm_gbuf_insert_edge(ctx->gbuf, node_a->id, node_b->id, "FILE_CHANGES_WITH", props);
        edge_count++;
    }

    /* Apply per-file temporal metadata to existing File nodes so callers
     * can query change_count / last_modified for hotspot analysis. The
     * extension is re-derived and JSON-escaped to keep the props blob
     * well-formed even for paths with quotes or backslashes. */
    for (int i = 0; i < result->file_temporal_count; i++) {
        const cbm_file_temporal_t *ft = &result->file_temporal[i];
        char *qn = cbm_pipeline_fqn_compute(ctx->project_name, ft->file_path, "__file__");
        const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(ctx->gbuf, qn);
        free(qn);
        if (!node) {
            continue;
        }

        const char *base = strrchr(ft->file_path, '/');
        base = base ? base + SKIP_ONE : ft->file_path;
        const char *ext = strrchr(base, '.');
        char ext_escaped[CBM_SZ_64];
        cbm_json_escape(ext_escaped, (int)sizeof(ext_escaped), ext ? ext : "");

        char props[CBM_SZ_256];
        snprintf(props, sizeof(props),
                 "{\"extension\":\"%s\",\"last_modified\":%lld,\"change_count\":%d}", ext_escaped,
                 ft->last_modified, ft->change_count);

        cbm_gbuf_upsert_node(ctx->gbuf, node->label, node->name, node->qualified_name,
                             node->file_path, node->start_line, node->end_line, props);
    }

    return edge_count;
}

/* ── Main pass (original serial interface) ───────────────────────── */

int cbm_pipeline_pass_githistory(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("pass.start", "pass", "githistory");

    cbm_githistory_result_t result = {0};
    cbm_pipeline_githistory_compute_with_limits(
        ctx->repo_path, &result, ctx->githistory_min_coupling, ctx->githistory_max_couplings);

    int edge_count = 0;
    if (result.count > 0 || result.file_temporal_count > 0) {
        edge_count = cbm_pipeline_githistory_apply(ctx, &result);
    }

    cbm_change_coupling_paths_free(result.couplings, result.count);
    free(result.couplings);
    cbm_file_temporal_free(result.file_temporal, result.file_temporal_count);

    cbm_log_info("pass.done", "pass", "githistory", "commits", itoa_log(result.commit_count),
                 "edges", itoa_log(edge_count));
    return 0;
}
