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

#include "pipeline/pipeline_internal.h"

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
    CBM_PATH_ALIAS_MAX_DEPTH = 32,   /* directory recursion depth    */
    CBM_PATH_ALIAS_MAX_EXTENDS = 16, /* `extends` links of one config, against a cycle */
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

/* Join dir_prefix with target, collapsing "." and ".." segments so aliases
 * that climb out of their tsconfig's directory (the common monorepo
 * pattern: a tsconfig at apps/web/tsconfig.json pointing an alias at a
 * wildcard target like "../../packages/shared/src/" + wildcard) resolve
 * to a real repo-relative path. Naive concatenation left literal ".."
 * components in the target, which never match a module's FQN since
 * cbm_pipeline_fqn_module tokenizes on '/' without collapsing them
 * (#730). A trailing '/' on target (the usual case right before a
 * wildcard) is preserved so the caller's later wildcard-substring
 * concat still lines up. Returns heap-allocated
 * repo-relative target. */
static char *resolve_target_relative(const char *dir_prefix, const char *target) {
    if (!target) {
        return NULL;
    }
    size_t dp_len = (dir_prefix && dir_prefix[0] != '\0') ? strlen(dir_prefix) : 0;
    size_t t_len = strlen(target);
    char *buf = malloc(dp_len + t_len + 2);
    if (!buf) {
        return NULL;
    }
    buf[0] = '\0';
    if (dp_len > 0) {
        memcpy(buf, dir_prefix, dp_len);
        buf[dp_len] = '\0';
    }

    bool trailing_slash = t_len > 0 && target[t_len - 1] == '/';

    const char *p = target;
    while (*p) {
        while (*p == '/') {
            p++;
        }
        if (!*p) {
            break;
        }
        const char *seg_start = p;
        while (*p && *p != '/') {
            p++;
        }
        size_t seg_len = (size_t)(p - seg_start);
        if (seg_len == 1 && seg_start[0] == '.') {
            continue;
        }
        if (seg_len == 2 && seg_start[0] == '.' && seg_start[1] == '.') {
            char *last = strrchr(buf, '/');
            if (last) {
                *last = '\0';
            } else {
                buf[0] = '\0';
            }
            continue;
        }
        size_t cur = strlen(buf);
        if (cur > 0) {
            buf[cur++] = '/';
        }
        memcpy(buf + cur, seg_start, seg_len);
        buf[cur + seg_len] = '\0';
    }

    if (trailing_slash) {
        size_t cur = strlen(buf);
        buf[cur] = '/';
        buf[cur + 1] = '\0';
    }
    return buf;
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

/* What a config and the configs it extends say. TypeScript merges compilerOptions down the
 * chain, so the nearest config setting `paths` answers for it, and likewise `baseUrl`. */
typedef struct {
    yyjson_doc *paths_doc; /* owns paths */
    yyjson_val *paths;
    char *paths_dir;            /* repo-relative directory of the config holding paths */
    char *base_root;            /* repo-relative baseUrl; NULL when no config sets one */
    yyjson_doc *conditions_doc; /* owns conditions */
    yyjson_val *conditions;
    cbm_path_alias_collection_t *collection;
    int bases_read;
} tsconfig_options_t;

static yyjson_doc *read_tsconfig_doc(const char *abs_path) {
    FILE *f = cbm_fopen(abs_path, "r");
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
    return doc;
}

/* The repo-relative file an `extends` value names, or false for anything else: a package
 * name is a dependency's config, and a path climbing out of the repo is not ours to read. */
static bool extends_rel_path(const char *dir_prefix, const char *value, char *out, size_t cap) {
    if (!value || (strncmp(value, "./", 2) && strncmp(value, "../", 3))) {
        return false;
    }
    int depth = dir_prefix && dir_prefix[0] ? 1 : 0;
    for (const char *c = dir_prefix; c && *c; c++) {
        depth += *c == '/';
    }
    for (const char *at = value; *at;) {
        const char *slash = strchr(at, '/');
        size_t length = slash ? (size_t)(slash - at) : strlen(at);
        if (length == 2 && !strncmp(at, "..", 2)) {
            if (--depth < 0) {
                return false;
            }
        } else if (length && !(length == 1 && at[0] == '.')) {
            depth++;
        }
        at += length + (slash ? 1 : 0);
    }
    char *joined = resolve_target_relative(dir_prefix, value);
    if (!joined) {
        return false;
    }
    size_t length = strlen(joined);
    /* `"extends": "./base"` reads ./base.json, like TypeScript. */
    bool suffixed = length > 5 && !strcmp(joined + length - 5, ".json");
    int n = snprintf(out, cap, "%s%s", joined, suffixed ? "" : ".json");
    free(joined);
    return n > 0 && (size_t)n < cap;
}

static bool tsconfig_read_chain(const char *repo_path, const char *abs_path, const char *dir_prefix,
                                int depth, tsconfig_options_t *out);

static void tsconfig_note_base(cbm_path_alias_collection_t *coll, const char *rel) {
    if (!coll) {
        return;
    }
    char **grown = realloc(coll->bases, ((size_t)coll->base_count + 1) * sizeof(*grown));
    char *copy = grown ? strdup(rel) : NULL;
    if (grown) {
        coll->bases = grown;
    }
    if (copy) {
        coll->bases[coll->base_count++] = copy;
    }
}

/* One relative `extends` entry. A missing or linked base supplies nothing. */
static void tsconfig_follow(const char *repo_path, const char *dir_prefix, const char *value,
                            int depth, tsconfig_options_t *out) {
    char rel[CBM_SZ_512];
    char abs[CBM_SZ_1K];
    if (!extends_rel_path(dir_prefix, value, rel, sizeof(rel))) {
        return;
    }
    tsconfig_note_base(out->collection, rel);
    /* `extends` arrays in a cycle would branch at every level. No real config reads this many. */
    enum { TSCONFIG_MAX_BASES_READ = 64 };
    if (++out->bases_read > TSCONFIG_MAX_BASES_READ) {
        return;
    }
    int n = snprintf(abs, sizeof(abs), "%s/%s", repo_path, rel);
    cbm_path_info_t info;
    if (n <= 0 || (size_t)n >= sizeof(abs) || cbm_path_info_utf8(abs, &info) != CBM_PATH_INFO_OK ||
        info.is_symlink) {
        return;
    }
    char *slash = strrchr(rel, '/');
    if (slash) {
        *slash = '\0';
    } else {
        rel[0] = '\0';
    }
    (void)tsconfig_read_chain(repo_path, abs, rel, depth + 1, out);
}

/* False when this config itself cannot be read, which says nothing about its scope. */
static bool tsconfig_read_chain(const char *repo_path, const char *abs_path, const char *dir_prefix,
                                int depth, tsconfig_options_t *out) {
    yyjson_doc *doc = read_tsconfig_doc(abs_path);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *options = yyjson_obj_get(root, "compilerOptions");
    bool keep = false;
    yyjson_val *conditions = yyjson_obj_get(options, "customConditions");
    bool keep_conditions = false;
    if (!out->conditions && yyjson_is_arr(conditions)) {
        out->conditions = conditions;
        out->conditions_doc = doc;
        keep_conditions = true;
    }
    yyjson_val *paths = yyjson_obj_get(options, "paths");
    if (!out->paths && yyjson_is_obj(paths)) {
        out->paths = paths;
        out->paths_doc = doc;
        out->paths_dir = strdup(dir_prefix);
        keep = true;
    }
    const char *base_url = yyjson_get_str(yyjson_obj_get(options, "baseUrl"));
    if (!out->base_root && base_url && base_url[0]) {
        out->base_root = resolve_target_relative(dir_prefix, base_url);
        size_t length = out->base_root ? strlen(out->base_root) : 0;
        if (length && out->base_root[length - 1] == '/') {
            out->base_root[length - 1] = '\0';
        }
    }
    yyjson_val *extends = yyjson_obj_get(root, "extends");
    if ((!out->paths || !out->base_root || !out->conditions) && extends &&
        depth < CBM_PATH_ALIAS_MAX_EXTENDS) {
        if (yyjson_is_str(extends)) {
            tsconfig_follow(repo_path, dir_prefix, yyjson_get_str(extends), depth, out);
        } else if (yyjson_is_arr(extends)) {
            /* A later entry of the array overrides an earlier one. */
            for (size_t i = yyjson_arr_size(extends); i > 0; i--) {
                tsconfig_follow(repo_path, dir_prefix,
                                yyjson_get_str(yyjson_arr_get(extends, i - 1)), depth, out);
            }
        }
    }
    /* The same document can hold both, so it is freed once, through paths_doc. */
    if (keep && keep_conditions) {
        out->conditions_doc = NULL;
    } else if (!keep && !keep_conditions) {
        yyjson_doc_free(doc);
    }
    return true;
}

/* `"files": []` with no `include`, the shape of a solution config that only lists references,
 * compiles nothing itself. It governs no file, so it does not stop an ancestor's scope. */
static bool tsconfig_owns_no_file(const char *abs_path) {
    yyjson_doc *doc = read_tsconfig_doc(abs_path);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *files = yyjson_obj_get(root, "files");
    yyjson_val *include = yyjson_obj_get(root, "include");
    bool none = yyjson_is_arr(files) && yyjson_arr_size(files) == 0 &&
                (!include || (yyjson_is_arr(include) && yyjson_arr_size(include) == 0));
    yyjson_doc_free(doc);
    return none;
}

/* Parse compilerOptions.paths and compilerOptions.baseUrl, through relative `extends`, into
 * an alias map. A config that sets neither still governs its directory, so it yields an
 * empty map: TypeScript never reads an ancestor config it was not told to extend. Returns
 * NULL only when the config cannot be read, which says nothing about its directory. */
static cbm_path_alias_map_t *load_tsconfig_file(const char *repo_path, const char *abs_path,
                                                const char *dir_prefix,
                                                cbm_path_alias_collection_t *collection) {
    tsconfig_options_t options = {.collection = collection};
    if (tsconfig_owns_no_file(abs_path) ||
        !tsconfig_read_chain(repo_path, abs_path, dir_prefix, 0, &options)) {
        return NULL;
    }
    cbm_path_alias_map_t *map = calloc(1, sizeof(*map));
    if (!map) {
        yyjson_doc_free(options.paths_doc);
        yyjson_doc_free(options.conditions_doc);
        free(options.paths_dir);
        free(options.base_root);
        return NULL;
    }
    if (options.base_root && options.base_root[0]) {
        map->base_url = strdup(options.base_root);
    }
    size_t conditions = options.conditions ? yyjson_arr_size(options.conditions) : 0;
    map->custom_conditions = conditions ? calloc(conditions, sizeof(char *)) : NULL;
    for (size_t i = 0; map->custom_conditions && i < conditions; i++) {
        const char *condition = yyjson_get_str(yyjson_arr_get(options.conditions, i));
        if (condition) {
            map->custom_conditions[map->custom_condition_count++] = strdup(condition);
        }
    }
    yyjson_doc_free(options.conditions_doc);
    options.conditions_doc = NULL;
    /* A target is relative to baseUrl when one is set, else to the config declaring it. */
    const char *target_root = options.base_root ? options.base_root : options.paths_dir;

    if (options.paths) {
        yyjson_val *paths_obj = options.paths;
        size_t obj_size = yyjson_obj_size(paths_obj);
        bool capped = obj_size > CBM_PATH_ALIAS_MAX_ENTRIES;
        int capacity = (int)(capped ? (size_t)CBM_PATH_ALIAS_MAX_ENTRIES : obj_size);
        if (capacity > 0) {
            map->entries = calloc((size_t)capacity, sizeof(cbm_path_alias_t));
            if (!map->entries) {
                free(map->base_url);
                free(map);
                yyjson_doc_free(options.paths_doc);
                free(options.paths_dir);
                free(options.base_root);
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
                    entry->target_prefix = resolve_target_relative(target_root, pre);
                    free(pre);
                    entry->target_suffix = strdup(tstar + 1);
                } else {
                    entry->target_prefix = resolve_target_relative(target_root, target_pattern);
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

    yyjson_doc_free(options.paths_doc);
    free(options.paths_dir);
    free(options.base_root);
    return map;
}

/* ── Public API ────────────────────────────────────────────────── */

static void free_alias_map(cbm_path_alias_map_t *map) {
    if (!map) {
        return;
    }
    for (int j = 0; j < map->count; j++) {
        free(map->entries[j].alias_prefix);
        free(map->entries[j].alias_suffix);
        free(map->entries[j].target_prefix);
        free(map->entries[j].target_suffix);
    }
    free(map->entries);
    free(map->base_url);
    for (int j = 0; j < map->custom_condition_count; j++) {
        free(map->custom_conditions[j]);
    }
    free(map->custom_conditions);
    free(map);
}

void cbm_path_alias_collection_free(cbm_path_alias_collection_t *coll) {
    if (!coll) {
        return;
    }
    for (int i = 0; i < coll->count; i++) {
        free(coll->scopes[i].dir_prefix);
        free(coll->scopes[i].source_rel_path);
        free_alias_map(coll->scopes[i].map);
    }
    for (int i = 0; i < coll->base_count; i++) {
        free(coll->bases[i]);
    }
    free(coll->bases);
    free(coll->scopes);
    free(coll);
}

bool cbm_path_alias_resolve_into(const cbm_path_alias_map_t *map, const char *module_path,
                                 char *out, size_t cap) {
    if (!map || !module_path || !out || cap == 0) {
        return false;
    }
    size_t mod_len = strlen(module_path);
    int n = CBM_NOT_FOUND;

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
            int wild_len = (int)(mod_len - prefix_len - suffix_len);
            n = snprintf(out, cap, "%s%.*s%s", e->target_prefix, wild_len, module_path + prefix_len,
                         e->target_suffix);
            break;
        }

        if (strcmp(module_path, e->alias_prefix) == 0) {
            n = snprintf(out, cap, "%s", e->target_prefix);
            break;
        }
    }

    /* baseUrl fallback. Apply only to non-relative imports that look
     * sub-path-ish (contain '/' but don't start with '.' or '@'); skips
     * obvious package names like "react" or "lodash". */
    if (n < 0 && map->base_url && module_path[0] != '.' && module_path[0] != '@' &&
        strchr(module_path, '/') != NULL) {
        n = snprintf(out, cap, "%s/%s", map->base_url, module_path);
    }
    if (n < 0 || (size_t)n >= cap) {
        return false;
    }
    strip_resolved_ext(out);
    return true;
}

char *cbm_path_alias_resolve(const cbm_path_alias_map_t *map, const char *module_path) {
    char buf[CBM_PATH_ALIAS_RESULT_MAX];
    return cbm_path_alias_resolve_into(map, module_path, buf, sizeof(buf)) ? strdup(buf) : NULL;
}

/* ── Repo walk ─────────────────────────────────────────────────── */

typedef struct {
    char abs[CBM_SZ_512];
    char rel[CBM_SZ_256];
} alias_config_hit_t;

static const char *const TS_CONFIG_NAMES[] = {"tsconfig.json", "jsconfig.json"};
enum { TS_CONFIG_NAMES_COUNT = 2 };

static void find_alias_files(const char *abs_dir, const char *rel_dir, alias_config_hit_t *out,
                             int *count, int max_count, int depth, char **excluded_dirs,
                             int excluded_count) {
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
        FILE *f = cbm_fopen(check, "r");
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
        if (cbm_pipeline_relpath_is_excluded(child_rel, excluded_dirs, excluded_count)) {
            continue;
        }
        /* A link is not followed, as discovery does not follow one. is_dir is
         * also true for a junction or a directory symbolic link on Windows,
         * and either can lead out of the repository. A child whose metadata
         * cannot be read is skipped the same way. */
        cbm_path_info_t child_info;
        if (cbm_path_info_utf8(child_abs, &child_info) != CBM_PATH_INFO_OK ||
            child_info.is_symlink) {
            continue;
        }
        find_alias_files(child_abs, child_rel, out, count, max_count, depth + 1, excluded_dirs,
                         excluded_count);
    }
    cbm_closedir(d);
}

static int cmp_base_path(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

bool cbm_path_alias_is_extended_base(const cbm_path_alias_collection_t *coll,
                                     const char *rel_path) {
    if (!coll || !rel_path || !coll->base_count) {
        return false;
    }
    return bsearch(&rel_path, coll->bases, (size_t)coll->base_count, sizeof(*coll->bases),
                   cmp_base_path) != NULL;
}

cbm_path_alias_collection_t *cbm_load_path_aliases_excluded(const char *repo_path,
                                                            char **excluded_dirs,
                                                            int excluded_count) {
    if (!repo_path) {
        return NULL;
    }
    alias_config_hit_t *hits = calloc(CBM_PATH_ALIAS_MAX_FILES, sizeof(*hits));
    if (!hits) {
        return NULL;
    }
    int count = 0;
    find_alias_files(repo_path, "", hits, &count, CBM_PATH_ALIAS_MAX_FILES, 0, excluded_dirs,
                     excluded_count);
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
        cbm_path_alias_map_t *map = load_tsconfig_file(repo_path, hits[i].abs, hits[i].rel, coll);
        if (!map) {
            continue;
        }
        const char *base = strrchr(hits[i].abs, '/');
        base = base ? base + 1 : hits[i].abs;
        char selected_rel[CBM_SZ_512];
        if (hits[i].rel[0]) {
            snprintf(selected_rel, sizeof(selected_rel), "%s/%s", hits[i].rel, base);
        } else {
            snprintf(selected_rel, sizeof(selected_rel), "%s", base);
        }
        char *dir_prefix = strdup(hits[i].rel);
        char *source_rel_path = strdup(selected_rel);
        if (!dir_prefix || !source_rel_path) {
            free(dir_prefix);
            free(source_rel_path);
            free_alias_map(map);
            continue;
        }
        coll->scopes[coll->count].dir_prefix = dir_prefix;
        coll->scopes[coll->count].source_rel_path = source_rel_path;
        coll->scopes[coll->count].map = map;
        coll->count++;
    }
    free(hits);

    /* Configs that alias nothing leave no scope worth a collection, unless a base config could
     * start to: the planner must still recognize a change to it. */
    bool any_alias = coll->base_count > 0;
    for (int i = 0; i < coll->count && !any_alias; i++) {
        any_alias = coll->scopes[i].map->count > 0 || coll->scopes[i].map->base_url ||
                    coll->scopes[i].map->custom_condition_count > 0;
    }
    if (!any_alias) {
        cbm_path_alias_collection_free(coll);
        return NULL;
    }
    if (coll->base_count) {
        qsort(coll->bases, (size_t)coll->base_count, sizeof(*coll->bases), cmp_base_path);
    }

    qsort(coll->scopes, (size_t)coll->count, sizeof(cbm_path_alias_scope_t),
          cmp_scope_by_specificity);
    return coll;
}

cbm_path_alias_collection_t *cbm_load_path_aliases(const char *repo_path) {
    return cbm_load_path_aliases_excluded(repo_path, NULL, 0);
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
