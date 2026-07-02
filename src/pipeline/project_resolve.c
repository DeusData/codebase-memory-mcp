/*
 * project_resolve.c — Canonical path identity and duplicate-index prevention.
 */
#include "pipeline/project_resolve.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/str_util.h"
#include "git/git_context.h"
#include "store/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *identity_key;
    char *project_name;
} proj_identity_entry_t;

typedef struct {
    bool loaded;
    char cache_dir[1024];
    proj_identity_entry_t *entries;
    size_t count;
} proj_identity_cache_t;

static proj_identity_cache_t g_identity_cache;

bool cbm_path_canonicalize(const char *path, char *out, size_t out_sz) {
    if (!path || !out || out_sz == 0) {
        return false;
    }
    out[0] = '\0';
#ifdef _WIN32
    if (!_fullpath(out, path, out_sz)) {
        return false;
    }
    cbm_normalize_path_sep(out);
#else
    if (!realpath(path, real)) {
        return false;
    }
#endif
    return out[0] != '\0';
}

bool cbm_project_identity_key(const char *repo_path, char *out, size_t out_sz) {
    if (!repo_path || !out || out_sz == 0) {
        return false;
    }

    cbm_git_context_t ctx = {0};
    if (cbm_git_context_resolve(repo_path, &ctx) == 0 && ctx.is_git && ctx.worktree_root &&
        ctx.worktree_root[0]) {
        bool ok = cbm_path_canonicalize(ctx.worktree_root, out, out_sz);
        cbm_git_context_free(&ctx);
        return ok;
    }
    cbm_git_context_free(&ctx);
    return cbm_path_canonicalize(repo_path, out, out_sz);
}

static bool identity_is_child(const char *child, const char *parent) {
    if (!child[0] || !parent[0]) {
        return false;
    }
    if (strcmp(child, parent) == 0) {
        return true;
    }
    size_t plen = strlen(parent);
    if (strncmp(child, parent, plen) != 0) {
        return false;
    }
    return child[plen] == '/';
}

static bool is_project_db_file(const char *name, size_t len) {
    if (len < 5 || strcmp(name + len - 3, ".db") != 0) {
        return false;
    }
    if (name[0] == '_') {
        return false;
    }
    return true;
}

static void identity_cache_free(void) {
    for (size_t i = 0; i < g_identity_cache.count; i++) {
        free(g_identity_cache.entries[i].identity_key);
        free(g_identity_cache.entries[i].project_name);
    }
    free(g_identity_cache.entries);
    memset(&g_identity_cache, 0, sizeof(g_identity_cache));
}

void cbm_project_identity_cache_invalidate(void) {
    identity_cache_free();
}

static bool identity_cache_load(void) {
    const char *cache_dir = cbm_resolve_cache_dir();
    if (g_identity_cache.loaded &&
        strcmp(g_identity_cache.cache_dir, cache_dir) == 0) {
        return true;
    }

    identity_cache_free();

    cbm_dir_t *d = cbm_opendir(cache_dir);
    if (!d) {
        snprintf(g_identity_cache.cache_dir, sizeof(g_identity_cache.cache_dir), "%s", cache_dir);
        g_identity_cache.loaded = true;
        return true;
    }

    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *name = entry->name;
        size_t len = strlen(name);
        if (!is_project_db_file(name, len)) {
            continue;
        }

        char db_path[2048];
        snprintf(db_path, sizeof(db_path), "%s/%s", cache_dir, name);

        cbm_store_t *store = cbm_store_open_path_query(db_path);
        if (!store) {
            continue;
        }

        char project_name[1024];
        snprintf(project_name, sizeof(project_name), "%.*s", (int)(len - 3), name);

        cbm_project_t proj = {0};
        if (cbm_store_get_project(store, project_name, &proj) != CBM_STORE_OK || !proj.root_path) {
            safe_str_free(&proj.name);
            safe_str_free(&proj.indexed_at);
            safe_str_free(&proj.root_path);
            cbm_store_close(store);
            continue;
        }

        char indexed_key[4096];
        bool has_key = cbm_project_identity_key(proj.root_path, indexed_key, sizeof(indexed_key));

        safe_str_free(&proj.name);
        safe_str_free(&proj.indexed_at);
        safe_str_free(&proj.root_path);
        cbm_store_close(store);

        if (!has_key) {
            continue;
        }

        proj_identity_entry_t row = {
            .identity_key = strdup(indexed_key),
            .project_name = strdup(project_name),
        };
        if (!row.identity_key || !row.project_name) {
            free(row.identity_key);
            free(row.project_name);
            continue;
        }

        proj_identity_entry_t *grown =
            realloc(g_identity_cache.entries,
                    (g_identity_cache.count + 1) * sizeof(*g_identity_cache.entries));
        if (!grown) {
            free(row.identity_key);
            free(row.project_name);
            continue;
        }
        g_identity_cache.entries = grown;
        g_identity_cache.entries[g_identity_cache.count++] = row;
    }

    cbm_closedir(d);
    snprintf(g_identity_cache.cache_dir, sizeof(g_identity_cache.cache_dir), "%s", cache_dir);
    g_identity_cache.loaded = true;
    return true;
}

char *cbm_find_existing_project_name(const char *repo_path) {
    if (!repo_path || !repo_path[0]) {
        return NULL;
    }

    char query_key[4096];
    if (!cbm_project_identity_key(repo_path, query_key, sizeof(query_key))) {
        return NULL;
    }

    if (!identity_cache_load()) {
        return NULL;
    }

    char *best_name = NULL;
    size_t best_root_len = 0;

    for (size_t i = 0; i < g_identity_cache.count; i++) {
        const char *indexed_key = g_identity_cache.entries[i].identity_key;
        const char *project_name = g_identity_cache.entries[i].project_name;
        if (!indexed_key || !project_name) {
            continue;
        }

        if (!identity_is_child(query_key, indexed_key)) {
            continue;
        }

        size_t root_len = strlen(indexed_key);
        if (!best_name || root_len > best_root_len) {
            free(best_name);
            best_name = strdup(project_name);
            best_root_len = root_len;
        }
    }

    return best_name;
}
