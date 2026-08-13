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
 *   - Sorting uses qsort (n log n). Alias/config counts are derived from
 *     input with checked geometric storage; they are not semantic limits.
 *   - The repo walk uses exact reusable paths and an iterative active-directory
 *     stack, so finite depth does not remove aliases or grow the C call stack.
 */

#include "pipeline/path_alias.h"

#include "pipeline/pipeline_internal.h"
#include "pipeline/walk_path.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/win_utf8.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <yyjson/yyjson.h>

enum {
    PATH_ALIAS_GROWTH_FACTOR = 2,
    PATH_ALIAS_INITIAL_CAPACITY = 16,
    PATH_ALIAS_IDENTITY_HEX_LEN = (int)(sizeof(uint64_t) * 2U),
    PATH_ALIAS_IDENTITY_KEY_LEN = PATH_ALIAS_IDENTITY_HEX_LEN * 2 + 1,
    PATH_ALIAS_IDENTITY_KEY_BUFSZ = PATH_ALIAS_IDENTITY_KEY_LEN + 1,
    PATH_ALIAS_ALLOC_NONE = 0,
    PATH_ALIAS_ALLOC_CONFIG_HIT,
    PATH_ALIAS_ALLOC_SCOPE_PREFIX,
};

#ifdef CBM_ENABLE_TEST_SEAMS
_Static_assert((int)CBM_PATH_ALIAS_TEST_ALLOC_NONE == PATH_ALIAS_ALLOC_NONE,
               "path-alias allocation test-site values must match");
_Static_assert((int)CBM_PATH_ALIAS_TEST_ALLOC_CONFIG_HIT == PATH_ALIAS_ALLOC_CONFIG_HIT,
               "path-alias config-hit test site must match");
_Static_assert((int)CBM_PATH_ALIAS_TEST_ALLOC_SCOPE_PREFIX == PATH_ALIAS_ALLOC_SCOPE_PREFIX,
               "path-alias scope-prefix test site must match");
static CBM_TLS int g_path_alias_test_alloc_site = PATH_ALIAS_ALLOC_NONE;
static CBM_TLS int g_path_alias_test_alloc_successes_before_failure = -1;

void cbm_path_alias_test_fail_allocation(cbm_path_alias_test_alloc_site_t site,
                                         int successful_before) {
    g_path_alias_test_alloc_site = (int)site;
    g_path_alias_test_alloc_successes_before_failure = successful_before;
}
#endif

static bool path_alias_test_allocation_should_fail(int site) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (g_path_alias_test_alloc_site != site ||
        g_path_alias_test_alloc_successes_before_failure < 0) {
        return false;
    }
    if (g_path_alias_test_alloc_successes_before_failure == 0) {
        g_path_alias_test_alloc_site = PATH_ALIAS_ALLOC_NONE;
        g_path_alias_test_alloc_successes_before_failure = -1;
        return true;
    }
    g_path_alias_test_alloc_successes_before_failure--;
#else
    (void)site;
#endif
    return false;
}

static void path_alias_mark_resource_failure(bool *resource_failure) {
    if (resource_failure) {
        *resource_failure = true;
    }
}

/* ── Helpers ───────────────────────────────────────────────────── */

static bool path_alias_size_add(size_t *total, size_t amount) {
    if (!total || amount > SIZE_MAX - *total) {
        return false;
    }
    *total += amount;
    return true;
}

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
    if (dp_len > SIZE_MAX - t_len || dp_len + t_len > SIZE_MAX - 2U) {
        return NULL;
    }
    size_t capacity = dp_len + t_len + 2U;
    char *buf = malloc(capacity);
    if (!buf) {
        return NULL;
    }
    buf[0] = '\0';
    if (dp_len > 0) {
        memcpy(buf, dir_prefix, dp_len);
        buf[dp_len] = '\0';
    }
    size_t path_len = dp_len;

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
            while (path_len > 0 && buf[path_len - 1U] != '/') {
                path_len--;
            }
            if (path_len > 0) {
                path_len--;
            }
            buf[path_len] = '\0';
            continue;
        }
        size_t separator = path_len > 0 ? 1U : 0U;
        if (path_len >= capacity || separator > capacity - path_len - 1U ||
            seg_len > capacity - path_len - 1U - separator) {
            free(buf);
            return NULL;
        }
        if (separator > 0) {
            buf[path_len++] = '/';
        }
        memcpy(buf + path_len, seg_start, seg_len);
        path_len += seg_len;
        buf[path_len] = '\0';
    }

    if (trailing_slash) {
        if (path_len >= capacity - 1U) {
            free(buf);
            return NULL;
        }
        buf[path_len++] = '/';
        buf[path_len] = '\0';
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
    if (ea->has_wildcard != eb->has_wildcard) {
        return ea->has_wildcard ? 1 : -1;
    }
    int suffix_order = strcmp(eb->alias_suffix, ea->alias_suffix);
    if (suffix_order != 0) {
        return suffix_order;
    }
    return strcmp(ea->alias_prefix, eb->alias_prefix);
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
    return strcmp(sa->dir_prefix, sb->dir_prefix);
}

static void path_alias_entry_free(cbm_path_alias_t *entry) {
    if (!entry) {
        return;
    }
    free(entry->alias_prefix);
    free(entry->alias_suffix);
    free(entry->target_prefix);
    free(entry->target_suffix);
    memset(entry, 0, sizeof(*entry));
}

static void path_alias_map_free(cbm_path_alias_map_t *map) {
    if (!map) {
        return;
    }
    for (int i = 0; i < map->count; i++) {
        path_alias_entry_free(&map->entries[i]);
    }
    free(map->entries);
    free(map->base_url);
    free(map);
}

static void path_alias_log_file_failure(const char *path, const char *reason, long size,
                                        long limit) {
    char size_buf[CBM_SZ_32];
    char limit_buf[CBM_SZ_32];
    snprintf(size_buf, sizeof(size_buf), "%ld", size);
    snprintf(limit_buf, sizeof(limit_buf), "%ld", limit);
    cbm_log_warn("path_alias.config_skipped", "path", path ? path : "", "reason", reason, "bytes",
                 size_buf, "limit", limit_buf);
}

/* ── tsconfig.json / jsconfig.json loader ──────────────────────── */

/* Parse compilerOptions.paths and compilerOptions.baseUrl into an alias map.
 * dir_prefix is the directory of the config file relative to the repo root
 * (e.g. "apps/manager", or "" for repo root). Returns NULL if the file is
 * missing, malformed, or has neither a usable paths block nor a baseUrl;
 * resource_failure distinguishes allocation/representation failure so the
 * caller can discard the whole collection instead of publishing partial
 * resolution state. */
static cbm_path_alias_map_t *load_tsconfig_file(const char *abs_path, const char *dir_prefix,
                                                bool *resource_failure) {
    if (resource_failure) {
        *resource_failure = false;
    }
    /* Read exact repository bytes. Windows text mode translates CRLF while
     * ftell() reports the physical extent, so an exact-size read would reject
     * valid native-line-ending configs as short. Binary mode keeps the
     * existing O(N) runtime/O(N) peak-buffer contract platform-independent. */
    FILE *f = cbm_fopen(abs_path, "rb");
    if (!f) {
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        path_alias_log_file_failure(abs_path, "seek_end_failed", -1, cbm_max_file_bytes());
        return NULL;
    }
    long len = ftell(f);
    if (len < 0 || fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        path_alias_log_file_failure(abs_path, "size_or_seek_start_failed", len,
                                    cbm_max_file_bytes());
        return NULL;
    }
    long file_limit = cbm_max_file_bytes();
    if (len > file_limit) {
        fclose(f);
        path_alias_log_file_failure(abs_path, "oversized", len, file_limit);
        return NULL;
    }
    if ((uintmax_t)len > (uintmax_t)SIZE_MAX - 1U) {
        fclose(f);
        path_alias_mark_resource_failure(resource_failure);
        path_alias_log_file_failure(abs_path, "allocation_size_overflow", len, file_limit);
        return NULL;
    }
    char *buf = malloc((size_t)len + 1U);
    if (!buf) {
        fclose(f);
        path_alias_mark_resource_failure(resource_failure);
        path_alias_log_file_failure(abs_path, "out_of_memory", len, file_limit);
        return NULL;
    }
    size_t nread = fread(buf, 1, (size_t)len, f);
    fclose(f);
    if (nread != (size_t)len) {
        free(buf);
        path_alias_log_file_failure(abs_path, "short_read", (long)nread, len);
        return NULL;
    }
    buf[nread] = '\0';

    yyjson_read_flag flg = YYJSON_READ_ALLOW_COMMENTS | YYJSON_READ_ALLOW_TRAILING_COMMAS;
    yyjson_read_err read_error = {0};
    yyjson_doc *doc = yyjson_read_opts(buf, nread, flg, NULL, &read_error);
    free(buf);
    if (!doc) {
        if (read_error.code == YYJSON_READ_ERROR_MEMORY_ALLOCATION) {
            path_alias_mark_resource_failure(resource_failure);
        }
        path_alias_log_file_failure(abs_path, "invalid_json", len, file_limit);
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
        path_alias_mark_resource_failure(resource_failure);
        yyjson_doc_free(doc);
        return NULL;
    }

    bool has_base_url = base_url_str && base_url_str[0] != '\0';
    if (has_base_url && strcmp(base_url_str, ".") != 0) {
        map->base_url = resolve_target_relative(dir_prefix, base_url_str);
    } else if (has_base_url) {
        map->base_url = cbm_strdup(dir_prefix ? dir_prefix : "");
    }
    if (has_base_url && !map->base_url) {
        path_alias_mark_resource_failure(resource_failure);
        path_alias_map_free(map);
        yyjson_doc_free(doc);
        return NULL;
    }

    if (paths_obj && yyjson_is_obj(paths_obj)) {
        size_t obj_size = yyjson_obj_size(paths_obj);
        if (obj_size > INT_MAX || obj_size > SIZE_MAX / sizeof(cbm_path_alias_t)) {
            path_alias_mark_resource_failure(resource_failure);
            path_alias_log_file_failure(abs_path, "entry_count_overflow", (long)obj_size, INT_MAX);
            path_alias_map_free(map);
            yyjson_doc_free(doc);
            return NULL;
        }
        int capacity = (int)obj_size;
        if (capacity > 0) {
            map->entries = calloc((size_t)capacity, sizeof(cbm_path_alias_t));
            if (!map->entries) {
                path_alias_mark_resource_failure(resource_failure);
                path_alias_map_free(map);
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
                const char *target_root = has_base_url ? map->base_url : dir_prefix;
                if (tstar) {
                    char *pre = cbm_strndup(target_pattern, (size_t)(tstar - target_pattern));
                    entry->target_prefix = resolve_target_relative(target_root, pre);
                    free(pre);
                    entry->target_suffix = strdup(tstar + 1);
                } else {
                    entry->target_prefix = resolve_target_relative(target_root, target_pattern);
                    entry->target_suffix = strdup("");
                }
                if (!entry->alias_prefix || !entry->alias_suffix || !entry->target_prefix ||
                    !entry->target_suffix) {
                    path_alias_mark_resource_failure(resource_failure);
                    path_alias_entry_free(entry);
                    path_alias_map_free(map);
                    yyjson_doc_free(doc);
                    return NULL;
                }
                map->count++;
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
        free(coll->scopes[i].source_rel_path);
        path_alias_map_free(coll->scopes[i].map);
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
            if (prefix_len > mod_len || suffix_len > mod_len - prefix_len) {
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
            size_t allocation_size = tp_len;
            if (!path_alias_size_add(&allocation_size, wild_len) ||
                !path_alias_size_add(&allocation_size, ts_len) ||
                !path_alias_size_add(&allocation_size, 1U)) {
                return NULL;
            }
            size_t result_len = allocation_size - 1U;
            char *result = malloc(allocation_size);
            if (!result) {
                return NULL;
            }
            memcpy(result, e->target_prefix, tp_len);
            memcpy(result + tp_len, wild_start, wild_len);
            memcpy(result + tp_len + wild_len, e->target_suffix, ts_len);
            result[result_len] = '\0';
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
        return strip_resolved_ext(resolve_target_relative(map->base_url, module_path));
    }
    return NULL;
}

/* ── Repo walk ─────────────────────────────────────────────────── */

typedef struct {
    char *abs;
    char *rel;
} alias_config_hit_t;

static const char *const TS_CONFIG_NAMES[] = {"tsconfig.json", "jsconfig.json"};
enum { TS_CONFIG_NAMES_COUNT = 2 };

typedef struct {
    alias_config_hit_t *items;
    size_t count;
    size_t capacity;
} alias_config_hits_t;

typedef cbm_walk_path_t path_alias_walk_path_t;

typedef struct {
    cbm_dir_t *dir;
    size_t abs_parent_len;
    size_t rel_parent_len;
    char *identity_key;
    bool configs_checked;
} path_alias_walk_frame_t;

typedef struct {
    path_alias_walk_frame_t *frames;
    size_t count;
    size_t capacity;
    CBMHashTable *active_identities;
} path_alias_walk_stack_t;

static char path_alias_identity_present;

#define path_alias_walk_path_init cbm_walk_path_init
#define path_alias_walk_path_append cbm_walk_path_append
#define path_alias_walk_path_restore cbm_walk_path_restore
#define path_alias_walk_path_free cbm_walk_path_free

static bool path_alias_plain_directory_identity(const char *path, cbm_file_identity_t *identity) {
    if (!path || !identity) {
        return false;
    }
    *identity = (cbm_file_identity_t){0};
#ifdef _WIN32
    wchar_t *wide_path = cbm_path_to_wide(path);
    if (!wide_path) {
        return false;
    }
    DWORD attrs = GetFileAttributesW(wide_path);
    free(wide_path);
    if (attrs == INVALID_FILE_ATTRIBUTES || (attrs & FILE_ATTRIBUTE_DIRECTORY) == 0 ||
        (attrs & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
        return false;
    }
    return cbm_file_identity_read(path, identity);
#else
    struct stat state;
    if (lstat(path, &state) != 0 || S_ISLNK(state.st_mode) || !S_ISDIR(state.st_mode)) {
        return false;
    }
    identity->volume = (uint64_t)state.st_dev;
    identity->file = (uint64_t)state.st_ino;
    identity->valid = true;
    return true;
#endif
}

static bool path_alias_identity_key(const cbm_file_identity_t *identity,
                                    char key[PATH_ALIAS_IDENTITY_KEY_BUFSZ]) {
    if (!identity || !identity->valid) {
        return false;
    }
    int written = snprintf(key, PATH_ALIAS_IDENTITY_KEY_BUFSZ, "%0*" PRIx64 ":%0*" PRIx64,
                           PATH_ALIAS_IDENTITY_HEX_LEN, identity->volume,
                           PATH_ALIAS_IDENTITY_HEX_LEN, identity->file);
    return written == PATH_ALIAS_IDENTITY_KEY_LEN;
}

static int path_alias_walk_stack_push(path_alias_walk_stack_t *stack, cbm_dir_t *dir,
                                      size_t abs_parent_len, size_t rel_parent_len,
                                      const cbm_file_identity_t *identity) {
    char key[PATH_ALIAS_IDENTITY_KEY_BUFSZ];
    if (!stack || !dir || !stack->active_identities || !path_alias_identity_key(identity, key)) {
        return -1;
    }
    if (cbm_ht_has(stack->active_identities, key)) {
        return 0;
    }
    char *owned_key = cbm_strdup(key);
    if (!owned_key) {
        return -1;
    }
    if (stack->count == stack->capacity) {
        size_t new_capacity = stack->capacity == 0 ? PATH_ALIAS_INITIAL_CAPACITY
                                                   : stack->capacity * PATH_ALIAS_GROWTH_FACTOR;
        if (new_capacity < stack->capacity ||
            new_capacity > SIZE_MAX / sizeof(path_alias_walk_frame_t)) {
            free(owned_key);
            return -1;
        }
        path_alias_walk_frame_t *grown = realloc(stack->frames, new_capacity * sizeof(*grown));
        if (!grown) {
            free(owned_key);
            return -1;
        }
        stack->frames = grown;
        stack->capacity = new_capacity;
    }
    cbm_ht_set(stack->active_identities, owned_key, &path_alias_identity_present);
    if (!cbm_ht_has(stack->active_identities, owned_key)) {
        free(owned_key);
        return -1;
    }
    stack->frames[stack->count++] = (path_alias_walk_frame_t){
        .dir = dir,
        .abs_parent_len = abs_parent_len,
        .rel_parent_len = rel_parent_len,
        .identity_key = owned_key,
        .configs_checked = false,
    };
    return 1;
}

static void path_alias_walk_stack_pop(path_alias_walk_stack_t *stack,
                                      path_alias_walk_path_t *abs_path,
                                      path_alias_walk_path_t *rel_path) {
    if (!stack || stack->count == 0) {
        return;
    }
    path_alias_walk_frame_t *frame = &stack->frames[stack->count - 1U];
    cbm_closedir(frame->dir);
    (void)cbm_ht_delete(stack->active_identities, frame->identity_key);
    free(frame->identity_key);
    path_alias_walk_path_restore(abs_path, frame->abs_parent_len);
    path_alias_walk_path_restore(rel_path, frame->rel_parent_len);
    stack->count--;
}

static void path_alias_walk_stack_free(path_alias_walk_stack_t *stack,
                                       path_alias_walk_path_t *abs_path,
                                       path_alias_walk_path_t *rel_path) {
    if (!stack) {
        return;
    }
    while (stack->count > 0) {
        path_alias_walk_stack_pop(stack, abs_path, rel_path);
    }
    cbm_ht_free(stack->active_identities);
    free(stack->frames);
    memset(stack, 0, sizeof(*stack));
}

static bool alias_config_hits_push(alias_config_hits_t *hits, const char *abs, const char *rel) {
    if (!hits || !abs || !rel) {
        return false;
    }
    if (hits->count >= INT_MAX) {
        return false;
    }
    if (hits->count == hits->capacity) {
        size_t new_capacity = hits->capacity == 0 ? PATH_ALIAS_INITIAL_CAPACITY
                                                  : hits->capacity * PATH_ALIAS_GROWTH_FACTOR;
        if (new_capacity < hits->capacity || new_capacity > INT_MAX ||
            new_capacity > SIZE_MAX / sizeof(alias_config_hit_t)) {
            return false;
        }
        alias_config_hit_t *grown = realloc(hits->items, new_capacity * sizeof(*grown));
        if (!grown) {
            return false;
        }
        hits->items = grown;
        hits->capacity = new_capacity;
    }
    if (path_alias_test_allocation_should_fail(PATH_ALIAS_ALLOC_CONFIG_HIT)) {
        return false;
    }
    char *owned_abs = cbm_strdup(abs);
    char *owned_rel = cbm_strdup(rel);
    if (!owned_abs || !owned_rel) {
        free(owned_rel);
        free(owned_abs);
        return false;
    }
    hits->items[hits->count++] = (alias_config_hit_t){.abs = owned_abs, .rel = owned_rel};
    return true;
}

static void alias_config_hits_free(alias_config_hits_t *hits) {
    if (!hits) {
        return;
    }
    for (size_t i = 0; i < hits->count; i++) {
        free(hits->items[i].abs);
        free(hits->items[i].rel);
    }
    free(hits->items);
    memset(hits, 0, sizeof(*hits));
}

static bool path_alias_probe_config(path_alias_walk_path_t *abs_path,
                                    const path_alias_walk_path_t *rel_path,
                                    alias_config_hits_t *hits) {
    size_t parent_len = abs_path->length;
    for (int i = 0; i < TS_CONFIG_NAMES_COUNT; i++) {
        if (!path_alias_walk_path_append(abs_path, TS_CONFIG_NAMES[i])) {
            path_alias_walk_path_restore(abs_path, parent_len);
            return false;
        }
        FILE *file = cbm_fopen(abs_path->data, "r");
        if (file) {
            fclose(file);
            bool stored = alias_config_hits_push(hits, abs_path->data, rel_path->data);
            path_alias_walk_path_restore(abs_path, parent_len);
            return stored;
        }
        path_alias_walk_path_restore(abs_path, parent_len);
    }
    return true;
}

/* Exact iterative DFS. Each accepted descent is a plain-directory edge:
 * symlinks/reparse points fail closed and an expected-O(1) active identity set
 * rejects alias/bind cycles. Runtime is expected O(E + N), plus filesystem and
 * exclusion costs; reusable paths cost O(P), and frames/handles/identities cost
 * O(D), where E is entries, N name bytes, P longest path, and D active depth. */
static bool find_alias_files(const char *repo_path, alias_config_hits_t *hits, char **excluded_dirs,
                             int excluded_count) {
    path_alias_walk_path_t abs_path = {0};
    path_alias_walk_path_t rel_path = {0};
    path_alias_walk_stack_t stack = {0};
    if (!path_alias_walk_path_init(&abs_path, repo_path) ||
        !path_alias_walk_path_init(&rel_path, "")) {
        path_alias_walk_path_free(&rel_path);
        path_alias_walk_path_free(&abs_path);
        return false;
    }
    stack.active_identities = cbm_ht_create(PATH_ALIAS_INITIAL_CAPACITY);
    cbm_file_identity_t root_identity = {0};
    cbm_dir_t *root = NULL;
    if (!stack.active_identities ||
        !path_alias_plain_directory_identity(abs_path.data, &root_identity) ||
        !(root = cbm_opendir(abs_path.data)) ||
        path_alias_walk_stack_push(&stack, root, abs_path.length, rel_path.length,
                                   &root_identity) != 1) {
        if (root) {
            cbm_closedir(root);
        }
        cbm_log_warn("path_alias.walk_skipped", "path", repo_path, "reason",
                     "root_open_identity_or_stack_failed");
        path_alias_walk_stack_free(&stack, &abs_path, &rel_path);
        path_alias_walk_path_free(&rel_path);
        path_alias_walk_path_free(&abs_path);
        return false;
    }

    bool complete = true;
    while (stack.count > 0 && complete) {
        path_alias_walk_frame_t *frame = &stack.frames[stack.count - 1U];
        if (!frame->configs_checked) {
            frame->configs_checked = true;
            if (!path_alias_probe_config(&abs_path, &rel_path, hits)) {
                cbm_log_warn("path_alias.walk_skipped", "path", abs_path.data, "reason",
                             "config_path_or_collection_allocation_failed");
                complete = false;
                break;
            }
        }
        cbm_dirent_t *entry = cbm_readdir(frame->dir);
        if (!entry) {
            path_alias_walk_stack_pop(&stack, &abs_path, &rel_path);
            continue;
        }
        const char *name = entry->name;
        if (name[0] == '.' || cbm_should_skip_dir(name, CBM_MODE_FULL)) {
            continue;
        }
        size_t abs_parent_len = abs_path.length;
        size_t rel_parent_len = rel_path.length;
        if (!path_alias_walk_path_append(&abs_path, name) ||
            !path_alias_walk_path_append(&rel_path, name)) {
            path_alias_walk_path_restore(&abs_path, abs_parent_len);
            path_alias_walk_path_restore(&rel_path, rel_parent_len);
            cbm_log_warn("path_alias.walk_entry_skipped", "dir", abs_path.data, "entry", name,
                         "reason", "path_allocation_failed");
            complete = false;
            break;
        }
        cbm_file_identity_t identity = {0};
        bool descend =
            !cbm_pipeline_relpath_is_excluded(rel_path.data, excluded_dirs, excluded_count) &&
            path_alias_plain_directory_identity(abs_path.data, &identity);
        if (descend) {
            cbm_dir_t *child = cbm_opendir(abs_path.data);
            if (child) {
                int pushed = path_alias_walk_stack_push(&stack, child, abs_parent_len,
                                                        rel_parent_len, &identity);
                if (pushed == 1) {
                    continue;
                }
                cbm_closedir(child);
                if (pushed < 0) {
                    cbm_log_warn("path_alias.walk_skipped", "path", abs_path.data, "reason",
                                 "walk_stack_allocation_failed");
                    complete = false;
                    break;
                }
                cbm_log_warn("path_alias.walk_entry_skipped", "path", abs_path.data, "reason",
                             "directory_cycle");
            } else {
                cbm_log_warn("path_alias.walk_entry_skipped", "path", abs_path.data, "reason",
                             "directory_open_failed");
            }
        }
        path_alias_walk_path_restore(&abs_path, abs_parent_len);
        path_alias_walk_path_restore(&rel_path, rel_parent_len);
    }
    path_alias_walk_stack_free(&stack, &abs_path, &rel_path);
    path_alias_walk_path_free(&rel_path);
    path_alias_walk_path_free(&abs_path);
    return complete;
}

cbm_path_alias_collection_t *cbm_load_path_aliases_excluded(const char *repo_path,
                                                            char **excluded_dirs,
                                                            int excluded_count) {
    if (!repo_path) {
        return NULL;
    }
    alias_config_hits_t hits = {0};
    if (!find_alias_files(repo_path, &hits, excluded_dirs, excluded_count)) {
        alias_config_hits_free(&hits);
        return NULL;
    }
    if (hits.count == 0) {
        alias_config_hits_free(&hits);
        return NULL;
    }

    cbm_path_alias_collection_t *coll = calloc(1, sizeof(*coll));
    if (!coll) {
        alias_config_hits_free(&hits);
        return NULL;
    }
    coll->scopes = calloc(hits.count, sizeof(cbm_path_alias_scope_t));
    if (!coll->scopes) {
        free(coll);
        alias_config_hits_free(&hits);
        return NULL;
    }

    for (size_t i = 0; i < hits.count; i++) {
        bool resource_failure = false;
        cbm_path_alias_map_t *map =
            load_tsconfig_file(hits.items[i].abs, hits.items[i].rel, &resource_failure);
        if (resource_failure) {
            cbm_log_warn("path_alias.collection_failed", "path", hits.items[i].abs, "reason",
                         "config_resource_failure");
            alias_config_hits_free(&hits);
            cbm_path_alias_collection_free(coll);
            return NULL;
        }
        if (!map) {
            continue;
        }
        const char *base = strrchr(hits.items[i].abs, '/');
        base = base ? base + 1 : hits.items[i].abs;
        size_t selected_size = strlen(base) + 1U;
        if (hits.items[i].rel[0] &&
            (!path_alias_size_add(&selected_size, strlen(hits.items[i].rel)) ||
             !path_alias_size_add(&selected_size, 1U))) {
            path_alias_map_free(map);
            alias_config_hits_free(&hits);
            cbm_path_alias_collection_free(coll);
            return NULL;
        }
        char *dir_prefix = path_alias_test_allocation_should_fail(PATH_ALIAS_ALLOC_SCOPE_PREFIX)
                               ? NULL
                               : cbm_strdup(hits.items[i].rel);
        char *source_rel_path = malloc(selected_size);
        if (source_rel_path) {
            snprintf(source_rel_path, selected_size, "%s%s%s", hits.items[i].rel,
                     hits.items[i].rel[0] ? "/" : "", base);
        }
        if (!dir_prefix || !source_rel_path) {
            free(dir_prefix);
            free(source_rel_path);
            path_alias_map_free(map);
            cbm_log_warn("path_alias.collection_failed", "path", hits.items[i].abs, "reason",
                         "scope_allocation_failed");
            alias_config_hits_free(&hits);
            cbm_path_alias_collection_free(coll);
            return NULL;
        }
        coll->scopes[coll->count].dir_prefix = dir_prefix;
        coll->scopes[coll->count].source_rel_path = source_rel_path;
        coll->scopes[coll->count].map = map;
        coll->count++;
    }
    alias_config_hits_free(&hits);

    if (coll->count == 0) {
        free(coll->scopes);
        free(coll);
        return NULL;
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
