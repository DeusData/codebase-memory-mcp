/*
 * git_env.c — child environment for cbm's git spawns. See git_env.h.
 */
#include "foundation/git_env.h"
#include "foundation/mem_core.h"

#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
extern char **environ;
#endif

const char *const cbm_git_repo_env_vars[CBM_GIT_REPO_ENV_VAR_COUNT] = {
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CONFIG",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_COUNT",
    "GIT_OBJECT_DIRECTORY",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_IMPLICIT_WORK_TREE",
    "GIT_GRAFT_FILE",
    "GIT_INDEX_FILE",
    "GIT_NO_REPLACE_OBJECTS",
    "GIT_REPLACE_REF_BASE",
    "GIT_PREFIX",
    "GIT_SHALLOW_FILE",
    "GIT_COMMON_DIR",
};

static char git_env_fold(char c) {
#ifdef _WIN32
    return (c >= 'a' && c <= 'z') ? (char)(c - 'a' + 'A') : c;
#else
    return c;
#endif
}

/* Does the name part of `entry` (up to '=' or `name_len` chars) equal `var`? */
static bool git_env_name_equals(const char *entry, size_t name_len, const char *var) {
    size_t var_len = strlen(var);
    if (name_len != var_len) {
        return false;
    }
    for (size_t i = 0; i < var_len; i++) {
        if (git_env_fold(entry[i]) != var[i]) {
            return false;
        }
    }
    return true;
}

static bool git_env_name_is_repo_local(const char *name, size_t name_len) {
    for (size_t i = 0; i < CBM_GIT_REPO_ENV_VAR_COUNT; i++) {
        if (git_env_name_equals(name, name_len, cbm_git_repo_env_vars[i])) {
            return true;
        }
    }
    return false;
}

bool cbm_git_env_entry_is_repo_local(const char *entry) {
    if (!entry) {
        return false;
    }
    const char *eq = strchr(entry, '=');
    size_t name_len = eq ? (size_t)(eq - entry) : strlen(entry);
    return git_env_name_is_repo_local(entry, name_len);
}

/* Explicit diff flags must not be overridden by the caller's environment.
 * Keep the public repository-local classification and its Git-derived list
 * unchanged; this extra removal belongs only to child construction. */
static bool git_env_name_should_strip(const char *name, size_t name_len) {
    return git_env_name_is_repo_local(name, name_len) ||
           git_env_name_equals(name, name_len, "GIT_DIFF_OPTS");
}

#ifdef _WIN32

/* Wide entry name → should the child omit it? Names we match are ASCII, so any
 * non-ASCII code unit is an immediate mismatch. Windows keeps per-drive cwd
 * entries ("=C:=C:\\dir") whose name starts with '='; the search for the
 * separator starts at index 1 so they are kept untouched. */
static bool git_env_wide_entry_should_strip(const wchar_t *entry) {
    char name[64] = {0};
    size_t n = 0;
    for (size_t i = 0; entry[i] && (i == 0 || entry[i] != L'='); i++) {
        if (entry[i] > 0x7F || n + 1 >= sizeof(name)) {
            return false;
        }
        name[n++] = (char)entry[i];
    }
    return git_env_name_should_strip(name, n);
}

wchar_t *cbm_git_child_env_block(void) {
    wchar_t *current = GetEnvironmentStringsW();
    if (!current) {
        return NULL;
    }
    size_t kept = 0;
    for (const wchar_t *e = current; *e; e += wcslen(e) + 1) {
        if (!git_env_wide_entry_should_strip(e)) {
            kept += wcslen(e) + 1;
        }
    }
    /* +2: a block that keeps nothing still needs its double terminator. */
    wchar_t *block = (wchar_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (kept + 2) * sizeof(wchar_t));
    if (!block) {
        FreeEnvironmentStringsW(current);
        return NULL;
    }
    size_t pos = 0;
    for (const wchar_t *e = current; *e; e += wcslen(e) + 1) {
        if (!git_env_wide_entry_should_strip(e)) {
            size_t len = wcslen(e) + 1;
            memcpy(block + pos, e, len * sizeof(wchar_t));
            pos += len;
        }
    }
    block[pos] = L'\0';
    if (pos == 0) {
        block[1] = L'\0';
    }
    FreeEnvironmentStringsW(current);
    return block;
}

#else

static bool git_env_entry_should_strip(const char *entry) {
    const char *eq = strchr(entry, '=');
    size_t name_len = eq ? (size_t)(eq - entry) : strlen(entry);
    return git_env_name_should_strip(entry, name_len);
}

char **cbm_git_child_envp(void) {
    size_t count = 0;
    size_t bytes = 0;
    for (char **e = environ; e && *e; e++) {
        if (!git_env_entry_should_strip(*e)) {
            count++;
            bytes += strlen(*e) + 1;
        }
    }
    size_t table = (count + 1) * sizeof(char *);
    char **envp = (char **)cbm_alloc(CBM_MEM_CLASS_OTHER, table + bytes);
    if (!envp) {
        return NULL;
    }
    char *strings = (char *)envp + table;
    size_t slot = 0;
    for (char **e = environ; e && *e && slot < count; e++) {
        if (!git_env_entry_should_strip(*e)) {
            size_t len = strlen(*e) + 1;
            memcpy(strings, *e, len);
            envp[slot++] = strings;
            strings += len;
        }
    }
    envp[slot] = NULL;
    return envp;
}

#endif

void cbm_git_child_env_free(void *env) {
    cbm_free(CBM_MEM_CLASS_OTHER, env);
}
