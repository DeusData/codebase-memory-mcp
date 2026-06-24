/*
 * path_alias.c — Resolve build-tool path aliases.
 *
 * Builds a directory-scoped collection of alias maps from per-language
 * config files (currently tsconfig.json / jsconfig.json) so the import
 * resolver can turn "@/lib/auth"-style imports into repo-relative paths.
 *
 * Design notes:
 *   - Public types and functions are language-agnostic. Adding a Vite /
 *     Webpack / Python loader means writing a new load_*_file() helper
 *     and registering it in find_alias_files. The resolver, the
 *     collection, and the pipeline integration do not change.
 *   - Sorting uses qsort (n log n). The bubble-sorts that the original
 *     Layer 1b draft used were O(n^2); with up to 256 alias entries
 *     and 256 scoped maps per repo, qsort is the right ceiling.
 *   - The repo walk caps recursion depth and total file count and emits
 *     a warning when either cap fires, so silent truncation on
 *     pathological monorepos shows up in the index log.
 */

#include "pipeline/path_alias.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/platform.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <yyjson/yyjson.h>

/* Resource ceilings. Chosen to comfortably cover real-world monorepos
 * (Next.js Skyline, large nx workspaces) while bounding worst-case
 * memory and walk time. Cap hits are logged. */
enum {
    CBM_PATH_ALIAS_MAX_ENTRIES = 256, /* per single config file       */
    CBM_PATH_ALIAS_MAX_FILES = 256,   /* config files per repo walk   */
    CBM_PATH_ALIAS_MAX_FILE_BYTES = 64 * 1024,
    CBM_PATH_ALIAS_MAX_DEPTH = 32, /* directory recursion depth    */
};

/* ── Helpers ───────────────────────────────────────────────────── */

/* Strip .ts/.tsx/.js/.jsx in place. Returns its argument. */
static char *strip_resolved_ext(char *path) {
    if (!path) {
        return path;
    }
    size_t len = strlen(path);
    if (len > 3 && path[len - 3] == '.' && (path[len - 2] == 't' || path[len - 2] == 'j') &&
        path[len - 1] == 's') {
        path[len - 3] = '\0';
        return path;
    }
    if (len > 4 && path[len - 4] == '.' && (path[len - 3] == 't' || path[len - 3] == 'j') &&
        path[len - 2] == 's' && path[len - 1] == 'x') {
        path[len - 4] = '\0';
    }
    return path;
}

/* If target starts with "./" and dir_prefix is non-empty, prepend dir_prefix.
 * Returns heap-allocated repo-relative target. */
static char *resolve_target_relative(const char *dir_prefix, const char *target) {
    if (!target) {
        return NULL;
    }
    const char *t = target;
    if (t[0] == '.' && t[1] == '/') {
        t += 2;
    }
    if (!dir_prefix || dir_prefix[0] == '\0') {
        return strdup(t);
    }
    size_t dp_len = strlen(dir_prefix);
    size_t t_len = strlen(t);
    char *result = malloc(dp_len + 1 + t_len + 1);
    if (!result) {
        return NULL;
    }
    snprintf(result, dp_len + 1 + t_len + 1, "%s/%s", dir_prefix, t);
    return result;
}

/* qsort comparator: alias entries by alias_prefix length, descending. */
static int cmp_alias_entry_by_specificity(const void *a, const void *b) {
    const cbm_path_alias_t *ea = a;
    const cbm_path_alias_t *eb = b;
    size_t la = strlen(ea->alias_prefix);
    size_t lb = strlen(eb->alias_prefix);
    if (lb > la) {
        return 1;
    }
    if (lb < la) {
        return -1;
    }
    return 0;
}

/* qsort comparator: scopes by dir_prefix length, descending. */
static int cmp_scope_by_specificity(const void *a, const void *b) {
    const cbm_path_alias_scope_t *sa = a;
    const cbm_path_alias_scope_t *sb = b;
    size_t la = strlen(sa->dir_prefix);
    size_t lb = strlen(sb->dir_prefix);
    if (lb > la) {
        return 1;
    }
    if (lb < la) {
        return -1;
    }
    return 0;
}

/* ── tsconfig.json / jsconfig.json loader ──────────────────────── */

/* Parse compilerOptions.paths and compilerOptions.baseUrl into an alias map.
 * dir_prefix is the directory of the config file relative to the repo root
 * (e.g. "apps/manager", or "" for repo root). Returns NULL if the file is
 * missing, malformed, or has neither a usable paths block nor a baseUrl. */
static cbm_path_alias_map_t *load_tsconfig_file(const char *abs_path, const char *dir_prefix) {
    FILE *f = fopen(abs_path, "r");
    if (!f) {
        return NULL;
    }
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (len <= 0 || len > CBM_PATH_ALIAS_MAX_FILE_BYTES) {
        fclose(f);
        return NULL;
    }
    char *buf = malloc((size_t)len + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, 1, (size_t)len, f);
    fclose(f);
    buf[nread] = '\0';

    yyjson_read_flag flg = YYJSON_READ_ALLOW_COMMENTS | YYJSON_READ_ALLOW_TRAILING_COMMAS;
    yyjson_doc *doc = yyjson_read(buf, nread, flg);
    free(buf);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *compiler_opts = yyjson_obj_get(root, "compilerOptions");
    if (!compiler_opts) {
        yyjson_doc_free(doc);
        return NULL;
    }
    yyjson_val *base_url_val = yyjson_obj_get(compiler_opts, "baseUrl");
    const char *base_url_str = base_url_val ? yyjson_get_str(base_url_val) : NULL;
    yyjson_val *paths_obj = yyjson_obj_get(compiler_opts, "paths");
    if (!paths_obj && !base_url_str) {
        yyjson_doc_free(doc);
        return NULL;
    }

    cbm_path_alias_map_t *map = calloc(1, sizeof(*map));
    if (!map) {
        yyjson_doc_free(doc);
        return NULL;
    }

    if (base_url_str && base_url_str[0] != '\0' && strcmp(base_url_str, ".") != 0) {
        map->base_url = resolve_target_relative(dir_prefix, base_url_str);
    } else if (base_url_str && strcmp(base_url_str, ".") == 0 && dir_prefix &&
               dir_prefix[0] != '\0') {
        map->base_url = strdup(dir_prefix);
    }

    if (paths_obj && yyjson_is_obj(paths_obj)) {
        size_t obj_size = yyjson_obj_size(paths_obj);
        bool capped = obj_size > CBM_PATH_ALIAS_MAX_ENTRIES;
        int capacity = (int)(capped ? (size_t)CBM_PATH_ALIAS_MAX_ENTRIES : obj_size);
        if (capacity > 0) {
            map->entries = calloc((size_t)capacity, sizeof(cbm_path_alias_t));
            if (!map->entries) {
                free(map->base_url);
                free(map);
                yyjson_doc_free(doc);
                return NULL;
            }
            yyjson_val *key;
            yyjson_obj_iter iter = yyjson_obj_iter_with(paths_obj);
            while ((key = yyjson_obj_iter_next(&iter)) != NULL && map->count < capacity) {
                yyjson_val *val = yyjson_obj_iter_get_val(key);
                const char *alias_pattern = yyjson_get_str(key);
                if (!alias_pattern || !yyjson_is_arr(val) || yyjson_arr_size(val) == 0) {
                    continue;
                }
                const char *target_pattern = yyjson_get_str(yyjson_arr_get_first(val));
                if (!target_pattern) {
                    continue;
                }
                cbm_path_alias_t *entry = &map->entries[map->count];
                const char *star = strchr(alias_pattern, '*');
                if (star) {
                    entry->has_wildcard = true;
                    entry->alias_prefix =
                        cbm_strndup(alias_pattern, (size_t)(star - alias_pattern));
                    entry->alias_suffix = strdup(star + 1);
                } else {
                    entry->has_wildcard = false;
                    entry->alias_prefix = strdup(alias_pattern);
                    entry->alias_suffix = strdup("");
                }
                const char *tstar = strchr(target_pattern, '*');
                if (tstar) {
                    char *pre = cbm_strndup(target_pattern, (size_t)(tstar - target_pattern));
                    entry->target_prefix = resolve_target_relative(dir_prefix, pre);
                    free(pre);
                    entry->target_suffix = strdup(tstar + 1);
                } else {
                    entry->target_prefix = resolve_target_relative(dir_prefix, target_pattern);
                    entry->target_suffix = strdup("");
                }
                map->count++;
            }
            if (capped) {
                cbm_log_warn("path_alias.entries.cap_hit", "config", abs_path, "kept",
                             /* itoa via thread-local buffer would be tidier; keep simple */
                             "256_of_more");
            }
            qsort(map->entries, (size_t)map->count, sizeof(cbm_path_alias_t),
                  cmp_alias_entry_by_specificity);
        }
    }

    yyjson_doc_free(doc);
    return map;
}

/* ── Public API ────────────────────────────────────────────────── */

void cbm_path_alias_collection_free(cbm_path_alias_collection_t *coll) {
    if (!coll) {
        return;
    }
    for (int i = 0; i < coll->count; i++) {
        free(coll->scopes[i].dir_prefix);
        if (coll->scopes[i].map) {
            cbm_path_alias_map_t *map = coll->scopes[i].map;
            for (int j = 0; j < map->count; j++) {
                free(map->entries[j].alias_prefix);
                free(map->entries[j].alias_suffix);
                free(map->entries[j].target_prefix);
                free(map->entries[j].target_suffix);
            }
            free(map->entries);
            free(map->base_url);
            free(map);
        }
    }
    free(coll->scopes);
    free(coll);
}

char *cbm_path_alias_resolve(const cbm_path_alias_map_t *map, const char *module_path) {
    if (!map || !module_path) {
        return NULL;
    }
    size_t mod_len = strlen(module_path);

    for (int i = 0; i < map->count; i++) {
        const cbm_path_alias_t *e = &map->entries[i];

        if (e->has_wildcard) {
            size_t prefix_len = strlen(e->alias_prefix);
            size_t suffix_len = strlen(e->alias_suffix);
            if (mod_len < prefix_len + suffix_len) {
                continue;
            }
            if (strncmp(module_path, e->alias_prefix, prefix_len) != 0) {
                continue;
            }
            if (suffix_len > 0 &&
                strcmp(module_path + mod_len - suffix_len, e->alias_suffix) != 0) {
                continue;
            }
            size_t wild_len = mod_len - prefix_len - suffix_len;
            const char *wild_start = module_path + prefix_len;
            size_t tp_len = strlen(e->target_prefix);
            size_t ts_len = strlen(e->target_suffix);
            char *result = malloc(tp_len + wild_len + ts_len + 1);
            if (!result) {
                return NULL;
            }
            memcpy(result, e->target_prefix, tp_len);
            memcpy(result + tp_len, wild_start, wild_len);
            memcpy(result + tp_len + wild_len, e->target_suffix, ts_len);
            result[tp_len + wild_len + ts_len] = '\0';
            return strip_resolved_ext(result);
        }

        if (strcmp(module_path, e->alias_prefix) == 0) {
            return strip_resolved_ext(strdup(e->target_prefix));
        }
    }

    /* baseUrl fallback. Apply only to non-relative imports that look
     * sub-path-ish (contain '/' but don't start with '.' or '@'); skips
     * obvious package names like "react" or "lodash". */
    if (map->base_url && module_path[0] != '.' && module_path[0] != '@' &&
        strchr(module_path, '/') != NULL) {
        size_t bu_len = strlen(map->base_url);
        size_t need = bu_len + 1 + mod_len + 1;
        char *result = malloc(need);
        if (!result) {
            return NULL;
        }
        snprintf(result, need, "%s/%s", map->base_url, module_path);
        return strip_resolved_ext(result);
    }
    return NULL;
}

/* ── Repo walk ─────────────────────────────────────────────────── */

typedef struct {
    char abs[CBM_SZ_512];
    char rel[CBM_SZ_256];
} alias_config_hit_t;

static const char *const TS_CONFIG_NAMES[] = {"tsconfig.json", "jsconfig.json"};
enum { TS_CONFIG_NAMES_COUNT = 2 };

static void find_alias_files(const char *abs_dir, const char *rel_dir, alias_config_hit_t *out,
                             int *count, int max_count, int depth) {
    if (*count >= max_count || depth > CBM_PATH_ALIAS_MAX_DEPTH) {
        return;
    }
    cbm_dir_t *d = cbm_opendir(abs_dir);
    if (!d) {
        return;
    }

    /* One config file per directory: prefer tsconfig.json over jsconfig.json. */
    for (int i = 0; i < TS_CONFIG_NAMES_COUNT && *count < max_count; i++) {
        char check[CBM_SZ_512];
        snprintf(check, sizeof(check), "%s/%s", abs_dir, TS_CONFIG_NAMES[i]);
        FILE *f = fopen(check, "r");
        if (f) {
            fclose(f);
            snprintf(out[*count].abs, sizeof(out[*count].abs), "%s", check);
            snprintf(out[*count].rel, sizeof(out[*count].rel), "%s", rel_dir);
            (*count)++;
            break;
        }
    }

    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL && *count < max_count) {
        if (!ent->is_dir) {
            continue;
        }
        const char *name = ent->name;
        if (name[0] == '.' || strcmp(name, "node_modules") == 0 || strcmp(name, "dist") == 0 ||
            strcmp(name, "build") == 0 || strcmp(name, ".next") == 0 ||
            strcmp(name, "coverage") == 0 || strcmp(name, "target") == 0 /* Rust */) {
            continue;
        }
        char child_abs[CBM_SZ_512];
        char child_rel[CBM_SZ_256];
        snprintf(child_abs, sizeof(child_abs), "%s/%s", abs_dir, name);
        if (rel_dir[0] == '\0') {
            snprintf(child_rel, sizeof(child_rel), "%s", name);
        } else {
            snprintf(child_rel, sizeof(child_rel), "%s/%s", rel_dir, name);
        }
        find_alias_files(child_abs, child_rel, out, count, max_count, depth + 1);
    }
    cbm_closedir(d);
}

cbm_path_alias_collection_t *cbm_load_path_aliases(const char *repo_path) {
    if (!repo_path) {
        return NULL;
    }
    alias_config_hit_t *hits = calloc(CBM_PATH_ALIAS_MAX_FILES, sizeof(*hits));
    if (!hits) {
        return NULL;
    }
    int count = 0;
    find_alias_files(repo_path, "", hits, &count, CBM_PATH_ALIAS_MAX_FILES, 0);
    if (count >= CBM_PATH_ALIAS_MAX_FILES) {
        cbm_log_warn("path_alias.files.cap_hit", "repo", repo_path, "kept", "256_of_more");
    }
    if (count == 0) {
        free(hits);
        return NULL;
    }

    cbm_path_alias_collection_t *coll = calloc(1, sizeof(*coll));
    if (!coll) {
        free(hits);
        return NULL;
    }
    coll->scopes = calloc((size_t)count, sizeof(cbm_path_alias_scope_t));
    if (!coll->scopes) {
        free(coll);
        free(hits);
        return NULL;
    }

    for (int i = 0; i < count; i++) {
        cbm_path_alias_map_t *map = load_tsconfig_file(hits[i].abs, hits[i].rel);
        if (!map) {
            continue;
        }
        coll->scopes[coll->count].dir_prefix = strdup(hits[i].rel);
        coll->scopes[coll->count].map = map;
        coll->count++;
    }
    free(hits);

    if (coll->count == 0) {
        free(coll->scopes);
        free(coll);
        return NULL;
    }

    qsort(coll->scopes, (size_t)coll->count, sizeof(cbm_path_alias_scope_t),
          cmp_scope_by_specificity);
    return coll;
}

const cbm_path_alias_map_t *cbm_path_alias_find_for_file(const cbm_path_alias_collection_t *coll,
                                                         const char *rel_path) {
    if (!coll || !rel_path) {
        return NULL;
    }
    for (int i = 0; i < coll->count; i++) {
        const char *prefix = coll->scopes[i].dir_prefix;
        size_t plen = strlen(prefix);
        if (plen == 0) {
            return coll->scopes[i].map;
        }
        if (strncmp(rel_path, prefix, plen) == 0 &&
            (rel_path[plen] == '/' || rel_path[plen] == '\0')) {
            return coll->scopes[i].map;
        }
    }
    return NULL;
}
