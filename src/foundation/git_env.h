/*
 * git_env.h — the environment cbm hands to its OWN git child processes.
 *
 * Git exports repository-local variables into hooks and some editors export
 * them into their terminals (GIT_DIR, GIT_WORK_TREE, GIT_INDEX_FILE, ...). They
 * take precedence over `git -C <dir>`: a cbm started from such an environment
 * would otherwise resolve every indexed project against the CALLER's repository
 * (a non-git directory reported as is_git=true, history and branch data from
 * the wrong repo). Every git spawn therefore gets a child environment with
 * exactly the variables `git rev-parse --local-env-vars` prints removed. The
 * scrub is per spawn: cbm's own environment is never modified (unsetenv in the
 * parent would race every other thread reading the environment).
 */
#ifndef CBM_GIT_ENV_H
#define CBM_GIT_ENV_H

#include <stdbool.h>
#include <stddef.h>

#ifdef _WIN32
#include <wchar.h>
#endif

/* `git rev-parse --local-env-vars` (git 2.50) — the single source of truth. */
enum { CBM_GIT_REPO_ENV_VAR_COUNT = 15 };
extern const char *const cbm_git_repo_env_vars[CBM_GIT_REPO_ENV_VAR_COUNT];

/* True when a "NAME=value" environment entry names one of the variables above.
 * The name compare is case-insensitive on Windows (its environment is). */
bool cbm_git_env_entry_is_repo_local(const char *entry);

#ifdef _WIN32
/* A CREATE_UNICODE_ENVIRONMENT block: the current environment minus the
 * repository-local git variables. One allocation; release with cbm_git_child_env_free().
 * NULL on allocation/snapshot failure. */
wchar_t *cbm_git_child_env_block(void);
#else
/* A NULL-terminated envp: the current environment minus the repository-local
 * git variables. Pointers and strings live in ONE allocation; release with
 * cbm_git_child_env_free(). NULL on allocation failure. */
char **cbm_git_child_envp(void);
#endif

/* Release a block from cbm_git_child_env_block / cbm_git_child_envp. NULL is a no-op. */
void cbm_git_child_env_free(void *env);

#endif /* CBM_GIT_ENV_H */
