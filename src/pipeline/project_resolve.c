/*
 * project_resolve.c — Canonical path identity and duplicate-index prevention.
 */
#include "pipeline/project_resolve.h"
#include "pipeline/pipeline.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "git/git_context.h"
#include "store/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    if (!realpath(path, out)) {
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
    if (cbm_git_context_resolve(repo_path, &ctx) == 0 && ctx.canonical_root &&
        ctx.canonical_root[0]) {
        snprintf(out, out_sz, "%s", ctx.canonical_root);
        cbm_normalize_path_sep(out);
        cbm_git_context_free(&ctx);
        return true;
    }
    cbm_git_context_free(&ctx);
    return cbm_path_canonicalize(repo_path, out, out_sz);
}

static bool identity_nested(const char *child, const char *parent) {
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

char *cbm_find_existing_project_name(const char *repo_path) {
    if (!repo_path || !repo_path[0]) {
        return NULL;
    }

    char query_key[4096];
    if (!cbm_project_identity_key(repo_path, query_key, sizeof(query_key))) {
        return NULL;
    }

    char cache_dir[1024];
    snprintf(cache_dir, sizeof(cache_dir), "%s", cbm_resolve_cache_dir());

    cbm_dir_t *d = cbm_opendir(cache_dir);
    if (!d) {
        return NULL;
    }

    char *best_name = NULL;
    size_t best_root_len = 0;

    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *name = entry->name;
        size_t len = strlen(name);
        if (!is_project_db_file(name, len)) {
            continue;
        }

        char db_path[2048];
        snprintf(db_path, sizeof(db_path), "%s/%s", cache_dir, name);

        cbm_store_t *store = cbm_store_open_path(db_path);
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

        if (strcmp(query_key, indexed_key) == 0 || identity_nested(query_key, indexed_key) ||
            identity_nested(indexed_key, query_key)) {
            size_t root_len = strlen(indexed_key);
            if (!best_name || root_len > best_root_len) {
                free(best_name);
                best_name = strdup(project_name);
                best_root_len = root_len;
            }
        }
    }

    cbm_closedir(d);
    return best_name;
}
