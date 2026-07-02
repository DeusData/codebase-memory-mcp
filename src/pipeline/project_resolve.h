#ifndef CBM_PROJECT_RESOLVE_H
#define CBM_PROJECT_RESOLVE_H

#include <stdbool.h>
#include <stddef.h>

/* Canonicalize path (realpath / _fullpath). Returns false if path is invalid. */
bool cbm_path_canonicalize(const char *path, char *out, size_t out_sz);

/* Stable identity for dedup: git canonical_root when available, else canonical path. */
bool cbm_project_identity_key(const char *repo_path, char *out, size_t out_sz);

/* Return heap-allocated existing project name when repo_path matches a cached index
 * (same identity or nested under an indexed root). Caller frees; NULL if no match. */
char *cbm_find_existing_project_name(const char *repo_path);

#endif
