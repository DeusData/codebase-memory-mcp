#ifndef CBM_PROJECT_RESOLVE_H
#define CBM_PROJECT_RESOLVE_H

#include <stdbool.h>
#include <stddef.h>

/* Canonicalize path (realpath / _fullpath). Returns false if path is invalid. */
bool cbm_path_canonicalize(const char *path, char *out, size_t out_sz);

/* Stable per-worktree identity: canonicalized git worktree root when available,
 * else canonical filesystem path. Distinct linked worktrees stay distinct. */
bool cbm_project_identity_key(const char *repo_path, char *out, size_t out_sz);

/* Return heap-allocated existing project name when repo_path matches a cached
 * index (exact worktree identity or a subdirectory of an indexed root).
 * Caller frees; NULL if no match. Uses a read-only scan cached until invalidate. */
char *cbm_find_existing_project_name(const char *repo_path);

/* Drop the cached identity scan (call after indexing completes). */
void cbm_project_identity_cache_invalidate(void);

#endif
