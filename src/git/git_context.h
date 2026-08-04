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

/* True when path is the root of a LINKED git worktree (`git worktree add`).
 *
 * Plumbing-only, no subprocess: <path>/.git must be a regular file holding a
 * "gitdir: <path>" pointer AND that gitdir must contain a `commondir` file.
 * The commondir check is what separates a linked worktree from a submodule —
 * a submodule's .git is also a gitlink file, but its gitdir
 * (<super>/.git/modules/<name>) has no commondir entry.
 *
 * Callers run this on every session start, so it stays fork-free; the richer
 * cbm_git_context_resolve() shells out to git and is not usable on that path. */
bool cbm_git_is_linked_worktree(const char *path);

int cbm_git_context_resolve(const char *path, cbm_git_context_t *out);
void cbm_git_context_free(cbm_git_context_t *ctx);
char *cbm_git_context_branch_qn(const char *project_name, const cbm_git_context_t *ctx);
int cbm_git_context_props_json(const cbm_git_context_t *ctx, char *buf, int buf_size);

#endif
