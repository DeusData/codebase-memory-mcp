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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *itoa_log(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

static bool ends_with(const char *s, size_t slen, const char *suffix) {
    size_t sflen = strlen(suffix);
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return slen >= sflen && strcmp(s + slen - sflen, suffix) == 0;
}

bool cbm_is_trackable_file(const char *path) {
    if (!path) {
        return false;
    }
    /* Skip directory prefixes */
#define LEN_NODE_MODULES_SLASH 13 /* strlen("node_modules/") */
    if (strncmp(path, ".git/", 5) == 0 ||
        strncmp(path, "node_modules/", LEN_NODE_MODULES_SLASH) == 0 ||
        strncmp(path, "vendor/", 7) == 0 || strncmp(path, "__pycache__/", 12) == 0 ||
        strncmp(path, ".cache/", 7) == 0) {
        return false;
    }
    /* Skip lock/generated file names */
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
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
} commit_t;

static void commit_add_file(commit_t *c, const char *file) {
    if (c->count >= c->cap) {
        c->cap = c->cap ? c->cap * 2 : 16;
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        c->files = safe_realloc(c->files, c->cap * sizeof(char *));
    }
    // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
    c->files[c->count++] = strdup(file);
}

static void commit_free(commit_t *c) {
    for (int i = 0; i < c->count; i++) {
        free(c->files[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(c->files);
}

/* ── libgit2-based git log parsing (preferred) ────────────────────── */

#ifdef HAVE_LIBGIT2
#include <git2.h>
#include <time.h>

static int parse_git_log(const char *repo_path, commit_t **out, int *out_count) {
    *out = NULL;
    *out_count = 0;

    git_libgit2_init();

    git_repository *repo = NULL;
    if (git_repository_open(&repo, repo_path) != 0) {
        git_libgit2_shutdown();
        return -1;
    }

    /* Walk from HEAD, sorted chronologically */
    git_revwalk *walker = NULL;
    if (git_revwalk_new(&walker, repo) != 0) {
        git_repository_free(repo);
        git_libgit2_shutdown();
        return -1;
    }
    git_revwalk_sorting(walker, GIT_SORT_TIME);
    git_revwalk_push_head(walker);

    /* 1 year cutoff, max 10k commits */
    time_t cutoff = time(NULL) - (365L * 24 * 3600);
    int max_commits = 10000;

    int cap = 64;
    commit_t *commits = malloc(cap * sizeof(commit_t));
    int count = 0;

    git_oid oid;
    while (git_revwalk_next(&oid, walker) == 0 && count < max_commits) {
        git_commit *commit = NULL;
        if (git_commit_lookup(&commit, repo, &oid) != 0) {
            continue;
        }

        /* Check if commit is within the 6-month window */
        git_time_t ct = git_commit_time(commit);
        if ((time_t)ct < cutoff) {
            git_commit_free(commit);
            break; /* sorted by time — all subsequent commits are older */
        }

        /* Get commit tree and parent tree for diff */
        git_tree *tree = NULL;
        git_tree *parent_tree = NULL;
        git_commit_tree(&tree, commit);

        unsigned int nparents = git_commit_parentcount(commit);
        if (nparents > 0) {
            git_commit *parent = NULL;
            if (git_commit_parent(&parent, commit, 0) == 0) {
                git_commit_tree(&parent_tree, parent);
                git_commit_free(parent);
            }
        }

        /* Diff parent_tree → tree to find changed files */
        git_diff *diff = NULL;
        git_diff_options diff_opts;
        git_diff_options_init(&diff_opts, GIT_DIFF_OPTIONS_VERSION);
        if (git_diff_tree_to_tree(&diff, repo, parent_tree, tree, &diff_opts) == 0) {
            commit_t current = {0};

            size_t ndeltas = git_diff_num_deltas(diff);
            for (size_t d = 0; d < ndeltas; d++) {
                const git_diff_delta *delta = git_diff_get_delta(diff, d);
                const char *path = delta->new_file.path;
                if (path && cbm_is_trackable_file(path)) {
                    commit_add_file(&current, path);
                }
            }

            if (current.count > 0) {
                if (count >= cap) {
                    cap *= 2;
                    commits = safe_realloc(commits, cap * sizeof(commit_t));
                }
                commits[count++] = current;
            } else {
                commit_free(&current);
            }
            git_diff_free(diff);
        }

        if (parent_tree) {
            git_tree_free(parent_tree);
        }
        git_tree_free(tree);
        git_commit_free(commit);
    }

    git_revwalk_free(walker);
    git_repository_free(repo);
    git_libgit2_shutdown();

    *out = commits;
    *out_count = count;
    return 0;
}

#else /* !HAVE_LIBGIT2 — popen fallback */

static int parse_git_log(const char *repo_path, commit_t **out, int *out_count) {
    *out = NULL;
    *out_count = 0;

    if (!cbm_validate_shell_arg(repo_path)) {
        return -1;
    }

    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "cd '%s' && git log --name-only --pretty=format:COMMIT:%%H "
             "--since='1 year ago' --max-count=10000 2>/dev/null",
             repo_path);

    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return -1;
    }

    int cap = 64;
    commit_t *commits = malloc(cap * sizeof(commit_t));
    int count = 0;
    commit_t current = {0};

    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        if (strncmp(line, "COMMIT:", 7) == 0) {
            if (current.count > 0) {
                if (count >= cap) {
                    cap *= 2;
                    commits = safe_realloc(commits, cap * sizeof(commit_t));
                }
                commits[count++] = current;
                memset(&current, 0, sizeof(current));
            }
            continue;
        }

        if (cbm_is_trackable_file(line)) {
            commit_add_file(&current, line);
        }
    }
    if (current.count > 0) {
        if (count >= cap) {
            cap *= 2;
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

#endif /* HAVE_LIBGIT2 */

/* Callback to free hash table entries. */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void free_counter(const char *key, void *val, void *ud) {
    (void)ud;
    free((void *)key);
    free(val);
}

/* ── Standalone coupling computation (testable) ──────────────────── */

/* Context for collect_coupling_result callback. */
typedef struct {
    CBMHashTable *file_counts;
    cbm_change_coupling_t *out;
    int out_count;
    int max_out;
} collect_coupling_ctx_t;

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void collect_coupling_cb(const char *pair_key, void *val, void *ud) {
    collect_coupling_ctx_t *cctx = ud;
    int co_count = *(int *)val;
    if (co_count < 3) {
        return;
    }
    if (cctx->out_count >= cctx->max_out) {
        return;
    }

    const char *sep = strchr(pair_key, '\x01');
    if (!sep) {
        return;
    }
    size_t la = sep - pair_key;
    const char *file_b = sep + 1;

    char file_a_buf[512];
    if (la >= sizeof(file_a_buf)) {
        return;
    }
    memcpy(file_a_buf, pair_key, la);
    file_a_buf[la] = '\0';

    int *count_a = cbm_ht_get(cctx->file_counts, file_a_buf);
    int *count_b = cbm_ht_get(cctx->file_counts, file_b);
    if (!count_a || !count_b) {
        return;
    }

    int min_total = *count_a < *count_b ? *count_a : *count_b;
    if (min_total == 0) {
        return;
    }

    double score = (double)co_count / (double)min_total;
    if (score < MIN_COUPLING_SCORE) {
        return;
    }

    cbm_change_coupling_t *cc = &cctx->out[cctx->out_count++];
    snprintf(cc->file_a, sizeof(cc->file_a), "%s", file_a_buf);
    snprintf(cc->file_b, sizeof(cc->file_b), "%s", file_b);
    cc->co_change_count = co_count;
    cc->coupling_score = score;
}

int cbm_compute_change_coupling(const cbm_commit_files_t *commits, int commit_count,
                                cbm_change_coupling_t *out, int max_out) {
    CBMHashTable *file_counts = cbm_ht_create(1024);
    CBMHashTable *pair_counts = cbm_ht_create(2048);

    for (int c = 0; c < commit_count; c++) {
        if (commits[c].count > 20) {
            continue;
        }

        for (int i = 0; i < commits[c].count; i++) {
            int *val = cbm_ht_get(file_counts, commits[c].files[i]);
            if (val) {
                (*val)++;
            } else {
                int *nv = malloc(sizeof(int));
                *nv = 1;
                cbm_ht_set(file_counts, strdup(commits[c].files[i]), nv);
            }
        }

        for (int i = 0; i < commits[c].count; i++) {
            for (int j = i + 1; j < commits[c].count; j++) {
                const char *a = commits[c].files[i];
                const char *b = commits[c].files[j];
                if (strcmp(a, b) > 0) {
                    const char *t = a;
                    a = b;
                    b = t;
                }
                size_t la = strlen(a);
                size_t lb = strlen(b);
                char *pk = malloc(la + 1 + lb + 1);
                memcpy(pk, a, la);
                pk[la] = '\x01';
                memcpy(pk + la + 1, b, lb + 1);

                int *val = cbm_ht_get(pair_counts, pk);
                if (val) {
                    (*val)++;
                    free(pk);
                } else {
                    int *nv = malloc(sizeof(int));
                    *nv = 1;
                    cbm_ht_set(pair_counts, pk, nv);
                }
            }
        }
    }

    collect_coupling_ctx_t cctx = {
        .file_counts = file_counts,
        .out = out,
        .out_count = 0,
        .max_out = max_out,
    };
    cbm_ht_foreach(pair_counts, collect_coupling_cb, &cctx);

    cbm_ht_foreach(pair_counts, free_counter, NULL);
    cbm_ht_free(pair_counts);
    cbm_ht_foreach(file_counts, free_counter, NULL);
    cbm_ht_free(file_counts);

    return cctx.out_count;
}

/* ── Split pass: compute (I/O-bound) + apply (gbuf writes) ───────── */

/* Pre-computed coupling result buffer for fused post-pass parallelism. */
#define MAX_COUPLINGS 8192

/* Compute change couplings without touching the graph buffer.
 * Can run on a separate thread while other passes use the gbuf. */
int cbm_pipeline_githistory_compute(const char *repo_path, cbm_githistory_result_t *result) {
    result->couplings = NULL;
    result->count = 0;
    result->commit_count = 0;

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
    }

    cbm_change_coupling_t *couplings = malloc(MAX_COUPLINGS * sizeof(cbm_change_coupling_t));
    int coupling_count = cbm_compute_change_coupling(cf, commit_count, couplings, MAX_COUPLINGS);

    free(cf);
    for (int c = 0; c < commit_count; c++) {
        commit_free(&commits[c]);
    }
    free(commits);

    result->couplings = couplings;
    result->count = coupling_count;
    return 0;
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

        char props[128];
        snprintf(props, sizeof(props), "{\"co_changes\":%d,\"coupling_score\":%.2f}",
                 cc->co_change_count, cc->coupling_score);

        cbm_gbuf_insert_edge(ctx->gbuf, node_a->id, node_b->id, "FILE_CHANGES_WITH", props);
        edge_count++;
    }

    return edge_count;
}

/* ── Main pass (original serial interface) ───────────────────────── */

int cbm_pipeline_pass_githistory(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("pass.start", "pass", "githistory");

    cbm_githistory_result_t result = {0};
    cbm_pipeline_githistory_compute(ctx->repo_path, &result);

    int edge_count = 0;
    if (result.count > 0) {
        edge_count = cbm_pipeline_githistory_apply(ctx, &result);
    }

    free(result.couplings);

    cbm_log_info("pass.done", "pass", "githistory", "commits", itoa_log(result.commit_count),
                 "edges", itoa_log(edge_count));
    return 0;
}
