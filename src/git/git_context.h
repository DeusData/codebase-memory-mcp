#ifndef CBM_GIT_CONTEXT_H
#define CBM_GIT_CONTEXT_H

#include <stdbool.h>
#include <stddef.h>

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
/* Only the is_git and branch fields of cbm_git_context_resolve, from the same
 * two git commands it uses for them (2 git processes instead of 7). branch
 * gets "" when path is not in a git work tree; pass CBM_GIT_BRANCH_MAX bytes
 * to read exactly what cbm_git_context_resolve reads. */
#define CBM_GIT_BRANCH_MAX 4096
int cbm_git_branch_resolve(const char *path, bool *is_git, char *branch, size_t branch_size);
void cbm_git_context_free(cbm_git_context_t *ctx);
char *cbm_git_context_branch_qn(const char *project_name, const cbm_git_context_t *ctx);
int cbm_git_context_props_json(const cbm_git_context_t *ctx, char *buf, int buf_size);

#endif
