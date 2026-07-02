#include "personal_memory/memory.h"

#include "cli/cli.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { MEMORY_DIR_PERMS = 0755 };

bool cbm_memory_enabled(struct cbm_config *cfg) {
    if (!cfg) {
        return true;
    }
    return cbm_config_get_bool(cfg, CBM_CONFIG_MEMORY_ENABLED, true);
}

const char *cbm_memory_resolve_dir(struct cbm_config *cfg) {
    if (cfg) {
        const char *configured = cbm_config_get(cfg, CBM_CONFIG_MEMORY_DIR, "");
        if (configured && configured[0]) {
            return configured;
        }
    }
    return cbm_resolve_memory_dir();
}

char *cbm_memory_db_path(struct cbm_config *cfg, bool create_dir) {
    const char *dir = cbm_memory_resolve_dir(cfg);
    if (!dir || !dir[0]) {
        return NULL;
    }
    if (create_dir && !cbm_mkdir_p(dir, MEMORY_DIR_PERMS)) {
        return NULL;
    }
    int n = snprintf(NULL, 0, "%s/memory.db", dir);
    if (n < 0) {
        return NULL;
    }
    char *path = malloc((size_t)n + 1);
    if (!path) {
        return NULL;
    }
    snprintf(path, (size_t)n + 1, "%s/memory.db", dir);
    return path;
}

cbm_store_t *cbm_memory_open(struct cbm_config *cfg, char **out_path) {
    char *path = cbm_memory_db_path(cfg, true);
    if (!path) {
        return NULL;
    }
    cbm_store_t *store = cbm_store_open_path(path);
    if (out_path) {
        *out_path = path;
    } else {
        free(path);
    }
    return store;
}

cbm_store_t *cbm_memory_open_query(struct cbm_config *cfg, char **out_path) {
    char *path = cbm_memory_db_path(cfg, false);
    if (!path) {
        return NULL;
    }
    cbm_store_t *store = cbm_store_open_path_query(path);
    if (out_path) {
        *out_path = path;
    } else {
        free(path);
    }
    return store;
}

const char *cbm_memory_default_branch(const cbm_git_context_t *ctx) {
    if (ctx && ctx->branch && ctx->branch[0] && strcmp(ctx->branch, "DETACHED") != 0 &&
        (strcmp(ctx->branch, "main") == 0 || strcmp(ctx->branch, "master") == 0)) {
        return ctx->branch;
    }
    return "main";
}

const char *cbm_memory_current_branch(const cbm_git_context_t *ctx) {
    if (ctx && ctx->branch && ctx->branch[0]) {
        return ctx->branch;
    }
    return "working-tree";
}

char *cbm_memory_repo_id(const char *project, const char *root_path, const cbm_git_context_t *ctx) {
    const char *identity = NULL;
    if (ctx && ctx->is_git && ctx->canonical_root && ctx->canonical_root[0]) {
        identity = ctx->canonical_root;
    } else if (root_path && root_path[0]) {
        identity = root_path;
    } else {
        identity = project && project[0] ? project : "project";
    }
    int n = snprintf(NULL, 0, "repo:%s", identity);
    if (n < 0) {
        return NULL;
    }
    char *out = malloc((size_t)n + 1);
    if (!out) {
        return NULL;
    }
    snprintf(out, (size_t)n + 1, "repo:%s", identity);
    return out;
}

char *cbm_memory_doc_key(const char *repo_id, const char *branch, const char *doc_type) {
    const char *rid = repo_id && repo_id[0] ? repo_id : "repo:unknown";
    const char *br = branch && branch[0] ? branch : "working-tree";
    const char *dt = doc_type && doc_type[0] ? doc_type : "adr";
    int n = snprintf(NULL, 0, "%s::branch:%s::doc:%s", rid, br, dt);
    if (n < 0) {
        return NULL;
    }
    char *out = malloc((size_t)n + 1);
    if (!out) {
        return NULL;
    }
    snprintf(out, (size_t)n + 1, "%s::branch:%s::doc:%s", rid, br, dt);
    return out;
}

void cbm_memory_add_settings_json(struct cbm_config *cfg, yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  const char *db_path) {
    const char *cache_dir_resolved = cbm_resolve_cache_dir();
    const char *memory_dir_resolved = cbm_memory_resolve_dir(cfg);
    char memory_dir_buf[CBM_SZ_1K];
    snprintf(memory_dir_buf, sizeof(memory_dir_buf), "%s",
             memory_dir_resolved ? memory_dir_resolved : "");
    const char *default_scope =
        cfg ? cbm_config_get(cfg, CBM_CONFIG_MEMORY_DEFAULT_SCOPE, "personal") : "personal";
    char default_scope_buf[CBM_SZ_256];
    snprintf(default_scope_buf, sizeof(default_scope_buf), "%s",
             default_scope ? default_scope : "");
    yyjson_mut_obj_add_str(doc, root, "storage", "personal");
    yyjson_mut_obj_add_bool(doc, root, "enabled", cbm_memory_enabled(cfg));
    yyjson_mut_obj_add_strcpy(doc, root, "default_scope", default_scope_buf);
    yyjson_mut_obj_add_str(doc, root, "cache_env", "CBM_CACHE_DIR");
    yyjson_mut_obj_add_str(doc, root, "memory_env", "CBM_MEMORY_DIR");
    yyjson_mut_obj_add_str(doc, root, "cache_dir", cache_dir_resolved ? cache_dir_resolved : "");
    yyjson_mut_obj_add_strcpy(doc, root, "memory_dir", memory_dir_buf);
    yyjson_mut_obj_add_strcpy(doc, root, "memory_db", db_path ? db_path : "");
    yyjson_mut_obj_add_str(doc, root, "repo_upload", "disabled");
}

void cbm_memory_add_list_json(cbm_store_t *store, const char *repo_id, yyjson_mut_doc *doc,
                              yyjson_mut_val *root) {
    yyjson_mut_val *items = yyjson_mut_arr(doc);
    int count = 0;
    if (store && repo_id) {
        sqlite3 *db = cbm_store_get_db(store);
        sqlite3_stmt *stmt = NULL;
        const char *sql = "SELECT project, updated_at FROM project_summaries WHERE project LIKE ?1 "
                          "ORDER BY updated_at DESC";
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            char pattern[CBM_SZ_2K];
            snprintf(pattern, sizeof(pattern), "%s::branch:%%::doc:%%", repo_id);
            sqlite3_bind_text(stmt, 1, pattern, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *key = (const char *)sqlite3_column_text(stmt, 0);
                const char *updated_at = (const char *)sqlite3_column_text(stmt, 1);
                const char *branch = key ? strstr(key, "::branch:") : NULL;
                const char *doc_type = key ? strstr(key, "::doc:") : NULL;
                yyjson_mut_val *item = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, item, "key", key ? key : "");
                if (branch && doc_type && doc_type > branch) {
                    branch += strlen("::branch:");
                    yyjson_mut_obj_add_strncpy(doc, item, "branch", branch,
                                               (size_t)(doc_type - branch));
                    yyjson_mut_obj_add_strcpy(doc, item, "doc_type", doc_type + strlen("::doc:"));
                }
                yyjson_mut_obj_add_strcpy(doc, item, "updated_at", updated_at ? updated_at : "");
                yyjson_mut_arr_add_val(items, item);
                count++;
            }
        }
        sqlite3_finalize(stmt);
    }
    yyjson_mut_obj_add_val(doc, root, "items", items);
    yyjson_mut_obj_add_int(doc, root, "count", count);
}
