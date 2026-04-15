/*
 * fqn.c — Fully Qualified Name computation for graph nodes.
 *
 * Implements the FQN scheme: project.dir.parts.name
 * Handles Python __init__.py, JS/TS index.{js,ts}, path separators.
 * Also handles tsconfig.json path alias resolution for TypeScript projects.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"

#include <stdbool.h>
#include <stddef.h> // NULL
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // strdup
#include <yyjson/yyjson.h>

/* Maximum path segments in a FQN (CBM_SZ_256 slots total, -2 for project + name) */
#define FQN_MAX_PATH_SEGS 254
#define FQN_MAX_DIR_SEGS 255

/* ── Internal helpers ─────────────────────────────────────────────── */

/* Build a dot-joined string from segments. Returns heap-allocated string. */
static char *join_segments(const char **segments, int count) {
    if (count == 0) {
        return strdup("");
    }
    size_t total = 0;
    for (int i = 0; i < count; i++) {
        total += strlen(segments[i]);
        if (i > 0) {
            total++; /* dot separator */
        }
    }
    char *result = malloc(total + SKIP_ONE);
    if (!result) {
        return NULL;
    }
    char *p = result;
    for (int i = 0; i < count; i++) {
        if (i > 0) {
            *p++ = '.';
        }
        size_t len = strlen(segments[i]);
        memcpy(p, segments[i], len);
        p += len;
    }
    *p = '\0';
    return result;
}

/* Strip file extension from the last path component. */
static void strip_file_extension(char *path) {
    char *last_slash = strrchr(path, '/');
    char *start = last_slash ? last_slash + SKIP_ONE : path;
    char *ext = strrchr(start, '.');
    if (ext) {
        *ext = '\0';
    }
}

/* Tokenize path by '/' into segments array. Returns number of segments added. */
static int tokenize_path(char *path, const char **segments, int max_segs) {
    int count = 0;
    if (path[0] == '\0') {
        return 0;
    }
    char *tok = path;
    while (tok && *tok && count < max_segs) {
        char *slash = strchr(tok, '/');
        if (slash) {
            *slash = '\0';
        }
        if (tok[0] != '\0') {
            segments[count++] = tok;
        }
        tok = slash ? slash + SKIP_ONE : NULL;
    }
    return count;
}

/* Strip __init__ (Python) / index (JS/TS) from the last segment when a
 * symbol name is provided. Keeps it when no name is given to avoid QN
 * collision with Folder nodes for the same directory. */
static void strip_init_or_index(const char **segments, int *seg_count, const char *name) {
    if (*seg_count <= SKIP_ONE) {
        return;
    }
    const char *last = segments[*seg_count - SKIP_ONE];
    if (strcmp(last, "__init__") != 0 && strcmp(last, "index") != 0) {
        return;
    }
    if (name && name[0] != '\0') {
        (*seg_count)--;
    }
}

/* ── Public API ──────────────────────────────────────────────────── */

char *cbm_pipeline_fqn_compute(const char *project, const char *rel_path, const char *name) {
    if (!project) {
        return strdup("");
    }

    char *path = strdup(rel_path ? rel_path : "");
    cbm_normalize_path_sep(path);
    strip_file_extension(path);

    const char *segments[CBM_SZ_256];
    int seg_count = 0;
    segments[seg_count++] = project;
    seg_count += tokenize_path(path, segments + seg_count, FQN_MAX_PATH_SEGS);

    strip_init_or_index(segments, &seg_count, name);

    if (name && name[0] != '\0') {
        segments[seg_count++] = name;
    }

    char *result = join_segments(segments, seg_count);
    free(path);
    return result;
}

char *cbm_pipeline_fqn_module(const char *project, const char *rel_path) {
    return cbm_pipeline_fqn_compute(project, rel_path, NULL);
}

enum {
    FQN_PATH_BUF = 1024,
    FQN_SEP_LEN = 1, /* one byte for the '/' separator */
    FQN_NUL_LEN = 1, /* one byte for the terminating NUL */
    FQN_DOTDOT_LEN = 2,
    FQN_MIN_PY_DOTS = 1, /* first leading dot is "current package", not a pop */
    FQN_REL_KIND_NONE = 0,
    FQN_REL_KIND_PYTHON = 1,
    FQN_REL_KIND_JS = 2,
};

/* Append a single path segment to a mutable buffer that already holds a
 * normalized slash-separated path.  Adds a '/' separator when needed,
 * returns false if the buffer would overflow. */
static bool path_append_segment(char *buf, size_t buf_size, const char *seg, size_t seg_len) {
    size_t cur = strlen(buf);
    size_t need = cur + (cur > 0 ? FQN_SEP_LEN : 0) + seg_len + FQN_NUL_LEN;
    if (need > buf_size) {
        return false;
    }
    if (cur > 0) {
        buf[cur++] = '/';
    }
    memcpy(buf + cur, seg, seg_len);
    buf[cur + seg_len] = '\0';
    return true;
}

/* Pop the last segment from a mutable slash-separated path. */
static void path_pop_segment(char *buf) {
    char *last = strrchr(buf, '/');
    if (last) {
        *last = '\0';
    } else {
        buf[0] = '\0';
    }
}

/* Seed `buf` with the source file's directory (strip the basename) and
 * normalize backslashes. */
static void seed_source_dir(char *buf, size_t buf_size, const char *source_rel) {
    snprintf(buf, buf_size, "%s", source_rel ? source_rel : "");
    for (char *p = buf; *p; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
    char *last = strrchr(buf, '/');
    if (last) {
        *last = '\0';
    } else {
        buf[0] = '\0';
    }
}

/* Detect the flavor of relative import based on the leading characters.
 * Returns 1 for Python dotted form (e.g. ".foo" or "..bar.baz"),
 *         2 for JS/TS slash form (e.g. "./foo" or "../bar/baz"),
 *         0 for anything not relative (caller should skip). */
static int classify_relative_import(const char *module_path) {
    if (!module_path || module_path[0] != '.') {
        return FQN_REL_KIND_NONE;
    }
    bool has_slash = strchr(module_path, '/') != NULL;
    bool js_like = module_path[FQN_SEP_LEN] == '/' ||
                   (module_path[FQN_SEP_LEN] == '.' && module_path[FQN_DOTDOT_LEN] == '/');
    if (has_slash || js_like) {
        return FQN_REL_KIND_JS;
    }
    return FQN_REL_KIND_PYTHON;
}

/* Python relative import: ".foo", "..bar.baz" → resolve against source dir. */
static char *resolve_python_relative(char *buf, size_t buf_size, const char *module_path) {
    const char *p = module_path;
    int dot_count = 0;
    while (*p == '.') {
        dot_count++;
        p++;
    }
    for (int i = FQN_MIN_PY_DOTS; i < dot_count; i++) {
        path_pop_segment(buf);
    }
    while (*p) {
        const char *seg_start = p;
        while (*p && *p != '.') {
            p++;
        }
        size_t seg_len = (size_t)(p - seg_start);
        if (seg_len > 0 && !path_append_segment(buf, buf_size, seg_start, seg_len)) {
            return NULL;
        }
        if (*p == '.') {
            p++;
        }
    }
    return strdup(buf);
}

/* Strip a trailing file extension from a segment (e.g. "helpers.ts" → "helpers").
 * Returns the new segment length. */
static size_t strip_ext(const char *seg_start, size_t seg_len) {
    const char *seg_end = seg_start + seg_len;
    const char *dot = NULL;
    for (const char *d = seg_end - FQN_SEP_LEN; d >= seg_start; d--) {
        if (*d == '.') {
            dot = d;
            break;
        }
    }
    if (dot && dot > seg_start) {
        return (size_t)(dot - seg_start);
    }
    return seg_len;
}

/* JS/TS relative import: "./foo", "../bar/baz" → resolve against source dir. */
static char *resolve_js_relative(char *buf, size_t buf_size, const char *module_path) {
    const char *p = module_path;
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
        if (seg_len == FQN_SEP_LEN && seg_start[0] == '.') {
            continue;
        }
        if (seg_len == FQN_DOTDOT_LEN && seg_start[0] == '.' && seg_start[FQN_SEP_LEN] == '.') {
            path_pop_segment(buf);
            continue;
        }
        if (*p == '\0') {
            seg_len = strip_ext(seg_start, seg_len);
        }
        if (seg_len > 0 && !path_append_segment(buf, buf_size, seg_start, seg_len)) {
            return NULL;
        }
    }
    return strdup(buf);
}

char *cbm_pipeline_resolve_relative_import(const char *source_rel, const char *module_path) {
    int kind = classify_relative_import(module_path);
    if (kind == FQN_REL_KIND_NONE) {
        return NULL;
    }
    char buf[FQN_PATH_BUF];
    seed_source_dir(buf, sizeof(buf), source_rel);
    if (kind == FQN_REL_KIND_PYTHON) {
        return resolve_python_relative(buf, sizeof(buf), module_path);
    }
    return resolve_js_relative(buf, sizeof(buf), module_path);
}

char *cbm_pipeline_fqn_folder(const char *project, const char *rel_dir) {
    if (!project) {
        return strdup("");
    }

    /* Work on mutable copy */
    char *dir = strdup(rel_dir ? rel_dir : "");
    cbm_normalize_path_sep(dir);

    const char *segments[CBM_SZ_256];
    int seg_count = 0;
    segments[seg_count++] = project;

    if (dir[0] != '\0') {
        char *tok = dir;
        while (tok && *tok && seg_count < FQN_MAX_DIR_SEGS) {
            char *slash = strchr(tok, '/');
            if (slash) {
                *slash = '\0';
            }
            if (tok[0] != '\0') {
                segments[seg_count++] = tok;
            }
            tok = slash ? slash + SKIP_ONE : NULL;
        }
    }

    char *result = join_segments(segments, seg_count);
    free(dir);
    return result;
}

char *cbm_project_name_from_path(const char *abs_path) {
    if (!abs_path || !abs_path[0]) {
        return strdup("root");
    }

    /* Work on mutable copy */
    char *path = strdup(abs_path);
    size_t len = strlen(path);

    /* Normalize path separators */
    cbm_normalize_path_sep(path);

    /* Replace / and : with - */
    for (size_t i = 0; i < len; i++) {
        if (path[i] == '/' || path[i] == ':') {
            path[i] = '-';
        }
    }

    /* Collapse consecutive dashes */
    char *dst = path;
    char prev = 0;
    for (size_t i = 0; i < len; i++) {
        if (path[i] == '-' && prev == '-') {
            continue;
        }
        *dst++ = path[i];
        prev = path[i];
    }
    *dst = '\0';

    /* Trim leading dashes */
    char *start = path;
    while (*start == '-') {
        start++;
    }

    /* Trim trailing dashes */
    size_t slen = strlen(start);
    while (slen > 0 && start[slen - SKIP_ONE] == '-') {
        start[--slen] = '\0';
    }

    if (*start == '\0') {
        free(path);
        return strdup("root");
    }

    char *result = strdup(start);
    free(path);
    return result;
}

/* ── tsconfig.json path alias resolution ────────────────────────── */

/* Maximum number of path alias entries we'll parse */
#define MAX_PATH_ALIASES 64

/* Maximum tsconfig.json file size (64 KB — tsconfigs are small) */
#define MAX_TSCONFIG_SIZE (64 * 1024)

/* Strip a trailing file extension from a resolved path (in-place).
 * Returns the string for convenience. Only strips common JS/TS extensions. */
static char *strip_resolved_ext(char *path) {
    if (!path) {
        return path;
    }
    size_t len = strlen(path);
    /* .ts, .js */
    if (len > 3 && path[len - 3] == '.' &&
        ((path[len - 2] == 't' || path[len - 2] == 'j') && path[len - 1] == 's')) {
        path[len - 3] = '\0';
        return path;
    }
    /* .tsx, .jsx */
    if (len > 4 && path[len - 4] == '.' &&
        ((path[len - 3] == 't' || path[len - 3] == 'j') && path[len - 2] == 's' &&
         path[len - 1] == 'x')) {
        path[len - 4] = '\0';
        return path;
    }
    return path;
}

/* Internal: resolve a target path pattern relative to a tsconfig directory.
 * If target starts with "./" and dir_prefix is non-empty, prepend dir_prefix.
 * E.g. dir_prefix="apps/manager", target="./src/foo" -> "apps/manager/src/foo"
 * Returns heap-allocated string. */
static char *resolve_target_relative(const char *dir_prefix, const char *target) {
    if (!target) {
        return NULL;
    }
    /* Strip leading "./" from target */
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

/* Internal: load and parse a single tsconfig/jsconfig file.
 * abs_path is the full filesystem path to the JSON file.
 * dir_prefix is the tsconfig's directory relative to repo root (e.g. "apps/manager").
 * Target paths in the alias map are resolved relative to dir_prefix. */
static cbm_path_alias_map_t *load_tsconfig_file(const char *abs_path, const char *dir_prefix) {
    FILE *f = fopen(abs_path, "r");
    if (!f) {
        return NULL;
    }

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (len <= 0 || len > MAX_TSCONFIG_SIZE) {
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

    cbm_path_alias_map_t *map = calloc(1, sizeof(cbm_path_alias_map_t));
    if (!map) {
        yyjson_doc_free(doc);
        return NULL;
    }

    /* Resolve baseUrl relative to tsconfig directory */
    if (base_url_str && base_url_str[0] != '\0' && strcmp(base_url_str, ".") != 0) {
        map->base_url = resolve_target_relative(dir_prefix, base_url_str);
    } else if (base_url_str && strcmp(base_url_str, ".") == 0 &&
               dir_prefix && dir_prefix[0] != '\0') {
        /* baseUrl="." in a subdirectory means that subdirectory */
        map->base_url = strdup(dir_prefix);
    }

    if (paths_obj && yyjson_is_obj(paths_obj)) {
        size_t obj_size = yyjson_obj_size(paths_obj);
        int capacity = (int)(obj_size < MAX_PATH_ALIASES ? obj_size : MAX_PATH_ALIASES);
        map->entries = calloc((size_t)capacity, sizeof(cbm_path_alias_t));
        if (!map->entries) {
            free(map->base_url);
            free(map);
            yyjson_doc_free(doc);
            return NULL;
        }

        yyjson_val *key, *val;
        yyjson_obj_iter iter = yyjson_obj_iter_with(paths_obj);
        while ((key = yyjson_obj_iter_next(&iter)) != NULL && map->count < capacity) {
            val = yyjson_obj_iter_get_val(key);
            const char *alias_pattern = yyjson_get_str(key);
            if (!alias_pattern || !yyjson_is_arr(val) || yyjson_arr_size(val) == 0) {
                continue;
            }
            yyjson_val *first_target = yyjson_arr_get_first(val);
            const char *target_pattern = yyjson_get_str(first_target);
            if (!target_pattern) {
                continue;
            }

            cbm_path_alias_t *entry = &map->entries[map->count];

            /* Split alias pattern at '*' */
            const char *star = strchr(alias_pattern, '*');
            if (star) {
                entry->has_wildcard = true;
                entry->alias_prefix = strndup(alias_pattern, (size_t)(star - alias_pattern));
                entry->alias_suffix = strdup(star + 1);
            } else {
                entry->has_wildcard = false;
                entry->alias_prefix = strdup(alias_pattern);
                entry->alias_suffix = strdup("");
            }

            /* Split target pattern at '*' and resolve relative to dir_prefix */
            const char *tstar = strchr(target_pattern, '*');
            if (tstar) {
                char *pre = strndup(target_pattern, (size_t)(tstar - target_pattern));
                entry->target_prefix = resolve_target_relative(dir_prefix, pre);
                free(pre);
                entry->target_suffix = strdup(tstar + 1);
            } else {
                entry->target_prefix = resolve_target_relative(dir_prefix, target_pattern);
                entry->target_suffix = strdup("");
            }

            map->count++;
        }

        /* Sort by alias_prefix length descending (most specific first) */
        for (int i = 0; i < map->count - 1; i++) {
            for (int j = i + 1; j < map->count; j++) {
                size_t li = strlen(map->entries[i].alias_prefix);
                size_t lj = strlen(map->entries[j].alias_prefix);
                if (lj > li) {
                    cbm_path_alias_t tmp = map->entries[i];
                    map->entries[i] = map->entries[j];
                    map->entries[j] = tmp;
                }
            }
        }
    }

    yyjson_doc_free(doc);
    return map;
}

/* Public: load aliases from the root tsconfig.json (backwards compat) */
cbm_path_alias_map_t *cbm_load_tsconfig_paths(const char *repo_path) {
    if (!repo_path) {
        return NULL;
    }
    static const char *config_names[] = {"tsconfig.json", "jsconfig.json"};
    char path_buf[CBM_SZ_512];
    for (int i = 0; i < 2; i++) {
        snprintf(path_buf, sizeof(path_buf), "%s/%s", repo_path, config_names[i]);
        cbm_path_alias_map_t *map = load_tsconfig_file(path_buf, "");
        if (map) {
            return map;
        }
    }
    return NULL;
}

void cbm_path_alias_map_free(cbm_path_alias_map_t *map) {
    if (!map) {
        return;
    }
    for (int i = 0; i < map->count; i++) {
        free(map->entries[i].alias_prefix);
        free(map->entries[i].alias_suffix);
        free(map->entries[i].target_prefix);
        free(map->entries[i].target_suffix);
    }
    free(map->entries);
    free(map->base_url);
    free(map);
}

char *cbm_resolve_path_alias(const cbm_path_alias_map_t *map, const char *module_path) {
    if (!map || !module_path) {
        return NULL;
    }

    size_t mod_len = strlen(module_path);

    /* Try each alias entry */
    for (int i = 0; i < map->count; i++) {
        const cbm_path_alias_t *e = &map->entries[i];
        size_t prefix_len = strlen(e->alias_prefix);
        size_t suffix_len = strlen(e->alias_suffix);

        if (e->has_wildcard) {
            /* Wildcard match: check prefix and suffix, extract the * part */
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
            /* Extract the wildcard portion */
            size_t wild_len = mod_len - prefix_len - suffix_len;
            const char *wild_start = module_path + prefix_len;

            /* Build result: target_prefix + wildcard + target_suffix */
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

        /* Exact match (no wildcard) */
        if (strcmp(module_path, e->alias_prefix) == 0) {
            char *result = strdup(e->target_prefix);
            return strip_resolved_ext(result);
        }
    }

    /* No alias matched — try baseUrl fallback.
     * If baseUrl is set, non-relative imports resolve relative to it.
     * E.g. baseUrl="src", import "lib/auth" → "src/lib/auth" */
    if (map->base_url) {
        /* Only apply to non-relative, non-package imports.
         * A simple heuristic: if it contains '/' and doesn't start with '.',
         * it might be a baseUrl-relative import. Also skip obvious packages
         * (no '/' at all, like "react" or "lodash"). */
        if (module_path[0] != '.' && strchr(module_path, '/') != NULL) {
            size_t bu_len = strlen(map->base_url);
            size_t needed = bu_len + 1 + mod_len + 1;
            char *result = malloc(needed);
            if (!result) {
                return NULL;
            }
            snprintf(result, needed, "%s/%s", map->base_url, module_path);
            return strip_resolved_ext(result);
        }
    }

    return NULL;
}

/* ── Monorepo tsconfig collection ──────────────────────────────── */

#define MAX_TSCONFIG_FILES 64

/* Recursive directory walker that finds tsconfig.json/jsconfig.json files. */
static void find_tsconfig_files(const char *abs_dir, const char *rel_dir,
                                char found[][CBM_SZ_512], char rels[][CBM_SZ_256],
                                int *count, int max_count) {
    if (*count >= max_count) {
        return;
    }
    cbm_dir_t *d = cbm_opendir(abs_dir);
    if (!d) {
        return;
    }

    /* First check if this directory has a tsconfig.json or jsconfig.json */
    static const char *cfg_names[] = {"tsconfig.json", "jsconfig.json"};
    for (int i = 0; i < 2 && *count < max_count; i++) {
        char check_path[CBM_SZ_512];
        snprintf(check_path, sizeof(check_path), "%s/%s", abs_dir, cfg_names[i]);
        FILE *f = fopen(check_path, "r");
        if (f) {
            fclose(f);
            snprintf(found[*count], CBM_SZ_512, "%s", check_path);
            snprintf(rels[*count], CBM_SZ_256, "%s", rel_dir);
            (*count)++;
            break; /* only take one per directory */
        }
    }

    /* Recurse into subdirectories (skip node_modules, .git, dist, build) */
    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) && *count < max_count) {
        if (!ent->is_dir) {
            continue;
        }
        const char *name = ent->name;
        if (name[0] == '.' || strcmp(name, "node_modules") == 0 ||
            strcmp(name, "dist") == 0 || strcmp(name, "build") == 0 ||
            strcmp(name, ".next") == 0 || strcmp(name, "coverage") == 0) {
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
        find_tsconfig_files(child_abs, child_rel, found, rels, count, max_count);
    }

    cbm_closedir(d);
}

cbm_tsconfig_collection_t *cbm_load_all_tsconfig_paths(const char *repo_path) {
    if (!repo_path) {
        return NULL;
    }

    char (*found)[CBM_SZ_512] = calloc(MAX_TSCONFIG_FILES, CBM_SZ_512);
    char (*rels)[CBM_SZ_256] = calloc(MAX_TSCONFIG_FILES, CBM_SZ_256);
    if (!found || !rels) {
        free(found);
        free(rels);
        return NULL;
    }

    int file_count = 0;
    find_tsconfig_files(repo_path, "", found, rels, &file_count, MAX_TSCONFIG_FILES);

    if (file_count == 0) {
        free(found);
        free(rels);
        return NULL;
    }

    cbm_tsconfig_collection_t *coll = calloc(1, sizeof(cbm_tsconfig_collection_t));
    if (!coll) {
        free(found);
        free(rels);
        return NULL;
    }
    coll->entries = calloc((size_t)file_count, sizeof(cbm_tsconfig_entry_t));
    if (!coll->entries) {
        free(coll);
        free(found);
        free(rels);
        return NULL;
    }

    for (int i = 0; i < file_count; i++) {
        cbm_path_alias_map_t *map = load_tsconfig_file(found[i], rels[i]);
        if (map) {
            coll->entries[coll->count].dir_prefix = strdup(rels[i]);
            coll->entries[coll->count].map = map;
            coll->count++;
        }
    }

    free(found);
    free(rels);

    if (coll->count == 0) {
        free(coll->entries);
        free(coll);
        return NULL;
    }

    /* Sort by dir_prefix length descending (most specific directory first) */
    for (int i = 0; i < coll->count - 1; i++) {
        for (int j = i + 1; j < coll->count; j++) {
            size_t li = strlen(coll->entries[i].dir_prefix);
            size_t lj = strlen(coll->entries[j].dir_prefix);
            if (lj > li) {
                cbm_tsconfig_entry_t tmp = coll->entries[i];
                coll->entries[i] = coll->entries[j];
                coll->entries[j] = tmp;
            }
        }
    }

    return coll;
}

void cbm_tsconfig_collection_free(cbm_tsconfig_collection_t *coll) {
    if (!coll) {
        return;
    }
    for (int i = 0; i < coll->count; i++) {
        free(coll->entries[i].dir_prefix);
        cbm_path_alias_map_free(coll->entries[i].map);
    }
    free(coll->entries);
    free(coll);
}

const cbm_path_alias_map_t *cbm_find_path_aliases(const cbm_tsconfig_collection_t *coll,
                                                   const char *rel_path) {
    if (!coll || !rel_path) {
        return NULL;
    }

    /* Find the nearest ancestor tsconfig: the longest dir_prefix that is
     * a prefix of the file's path. Entries are sorted longest-first. */
    for (int i = 0; i < coll->count; i++) {
        const char *prefix = coll->entries[i].dir_prefix;
        size_t plen = strlen(prefix);

        /* Root tsconfig (empty prefix) matches everything */
        if (plen == 0) {
            return coll->entries[i].map;
        }

        /* Check if the file's path starts with this directory */
        if (strncmp(rel_path, prefix, plen) == 0 &&
            (rel_path[plen] == '/' || rel_path[plen] == '\0')) {
            return coll->entries[i].map;
        }
    }

    return NULL;
}
