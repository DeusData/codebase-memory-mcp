#ifndef CBM_GIT_CONTEXT_H
#define CBM_GIT_CONTEXT_H

#include <stdbool.h>

typedef struct {
    bool is_git;
    bool is_worktree;
    bool is_detached;
    bool root_exists;
    char *input_path;
    char *worktree_root;
    char *git_dir;
    char *git_common_dir;
    char *canonical_root;
    char *branch;
    char *branch_slug;
    char *head_sha;
    char *base_sha;
} cbm_git_context_t;

int cbm_git_context_resolve(const char *path, cbm_git_context_t *out);
void cbm_git_context_free(cbm_git_context_t *ctx);
char *cbm_git_context_branch_qn(const char *project_name, const cbm_git_context_t *ctx);
int cbm_git_context_props_json(const cbm_git_context_t *ctx, char *buf, int buf_size);

/* Bounded snapshot of the git worktree status for one validated repo root.
 *
 * available is true ONLY when `git status --porcelain=v1 -z --untracked-files=all`
 * exited 0 AND every NUL-delimited record parsed cleanly. Any other outcome —
 * shell-unsafe path, spawn failure, nonzero exit, malformed/oversized record,
 * trailing partial record — leaves available=false with zero counts and no
 * samples. Callers MUST treat !available as "status unavailable", never as a
 * clean worktree.
 *
 * The entire stream is counted, but only max_samples paths per class (tracked
 * / untracked) are ever retained, in stream order; exceeding the cap sets the
 * matching *_truncated flag. Rename/copy records are counted once (their two
 * NUL-separated paths are consumed without counting the second), and the
 * sampled path is the record's NEW path. Ignored files never appear because
 * --ignored is not requested.
 *
 * cbm_git_worktree_status returns 0 when *out has been filled (available tells
 * whether the snapshot succeeded) and CBM_NOT_FOUND for invalid arguments
 * (NULL root/out or max_samples < 0). The caller owns the returned samples via
 * cbm_git_worktree_status_free. */
typedef struct {
    bool available;
    int tracked_count;
    int untracked_count;
    char **tracked_paths; /* heap-owned samples, tracked_sample_count entries */
    int tracked_sample_count;
    bool tracked_truncated;
    char **untracked_paths;
    int untracked_sample_count;
    bool untracked_truncated;
} cbm_worktree_status_t;

int cbm_git_worktree_status(const char *validated_root, int max_samples,
                            cbm_worktree_status_t *out);
void cbm_git_worktree_status_free(cbm_worktree_status_t *st);

#endif
