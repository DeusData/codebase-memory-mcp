/*
 * pass_pkgmap.c — Package specifier resolution for JS/TS monorepos.
 *
 * Builds a hash table mapping npm package names to resolved module QNs
 * by scanning the repo for package.json files. Used by cbm_pipeline_resolve_module()
 * to convert bare specifiers like "@myorg/utils" into graph node QNs.
 *
 * Functions:
 *   cbm_pipeline_build_pkg_map()   — walk repo, parse package.json, build map
 *   cbm_pipeline_free_pkg_map()    — free map and all entries
 *   cbm_pipeline_resolve_module()  — resolve specifier via map with fallback
 */
#include "pipeline/pipeline_internal.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "yyjson/yyjson.h"

#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Internal helpers ────────────────────────────────────────────── */

/* Read entire file into heap-allocated buffer. Returns NULL on error.
 * Caller must free(). Sets *out_len to byte count. */
static char *pkgmap_read_file(const char *path, int *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }

    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);

    if (size <= 0 || size > (long)1024 * 1024) { /* 1 MB sanity limit for package.json */
        (void)fclose(f);
        return NULL;
    }

    char *buf = malloc((size_t)size + 1);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }

    size_t nread = fread(buf, 1, (size_t)size, f);
    (void)fclose(f);

    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* Compute relative path from repo_root to abs_path.
 * repo_root must be a prefix of abs_path.
 * Returns pointer into abs_path (not heap-allocated). */
static const char *make_rel_path(const char *abs_path, const char *repo_root) {
    size_t root_len = strlen(repo_root);
    if (strncmp(abs_path, repo_root, root_len) != 0) {
        return abs_path;
    }
    const char *rel = abs_path + root_len;
    /* Skip leading slash */
    if (*rel == '/') {
        rel++;
    }
    return rel;
}

/* Resolve entry point candidate string from package.json.
 * Strips leading "./" and tries stat() on the resulting path.
 * pkg_dir: absolute path to package directory.
 * repo_root: absolute repo root.
 * Returns heap-allocated repo-relative path on success, NULL on failure.
 * Caller must free(). */
static char *try_entry_point(const char *candidate, const char *pkg_dir, const char *repo_root,
                             const char *project_name) {
    if (!candidate || !candidate[0]) {
        return NULL;
    }

    /* Strip leading "./" */
    if (candidate[0] == '.' && candidate[1] == '/') {
        candidate += 2;
    }

    char full_path[1024];
    snprintf(full_path, sizeof(full_path), "%s/%s", pkg_dir, candidate);

    struct stat st;
    if (stat(full_path, &st) == 0) {
        const char *rel = make_rel_path(full_path, repo_root);
        return cbm_pipeline_fqn_module(project_name, rel);
    }

    return NULL;
}

/* Process a single package.json file.
 * pkg_dir: absolute path to the directory containing package.json.
 * repo_root: absolute repo root.
 * project_name: pipeline project name.
 * pkg_map: hash table to insert into. */
static void process_package_json(const char *pkg_dir, const char *repo_root,
                                 const char *project_name, CBMHashTable *pkg_map) {
    char pkg_json_path[1024];
    snprintf(pkg_json_path, sizeof(pkg_json_path), "%s/package.json", pkg_dir);

    int len = 0;
    char *content = pkgmap_read_file(pkg_json_path, &len);
    if (!content) {
        return;
    }

    yyjson_doc *doc = yyjson_read(content, (size_t)len, 0);
    free(content);
    if (!doc) {
        return;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!root || !yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return;
    }

    /* Extract "name" field */
    yyjson_val *name_val = yyjson_obj_get(root, "name");
    if (!name_val || !yyjson_is_str(name_val)) {
        yyjson_doc_free(doc);
        return;
    }
    const char *pkg_name = yyjson_get_str(name_val);
    if (!pkg_name || !pkg_name[0]) {
        yyjson_doc_free(doc);
        return;
    }

    /* Resolve entry point — try in priority order */
    char *module_qn = NULL;

    /* 1. Check "exports" field */
    yyjson_val *exports_val = yyjson_obj_get(root, "exports");
    if (exports_val && !module_qn) {
        const char *candidate = NULL;
        if (yyjson_is_str(exports_val)) {
            /* "exports": "./src/index.ts" */
            candidate = yyjson_get_str(exports_val);
        } else if (yyjson_is_obj(exports_val)) {
            /* "exports": { ".": ... } */
            yyjson_val *dot_val = yyjson_obj_get(exports_val, ".");
            if (dot_val) {
                if (yyjson_is_str(dot_val)) {
                    candidate = yyjson_get_str(dot_val);
                } else if (yyjson_is_obj(dot_val)) {
                    /* Try "import", then "default", then "require" */
                    static const char *keys[] = {"import", "default", "require", NULL};
                    for (int ki = 0; keys[ki] && !candidate; ki++) {
                        yyjson_val *v = yyjson_obj_get(dot_val, keys[ki]);
                        if (v && yyjson_is_str(v)) {
                            candidate = yyjson_get_str(v);
                        }
                    }
                }
            }
        }
        if (candidate) {
            module_qn = try_entry_point(candidate, pkg_dir, repo_root, project_name);
        }
    }

    /* 2. Check "main" field */
    if (!module_qn) {
        yyjson_val *main_val = yyjson_obj_get(root, "main");
        if (main_val && yyjson_is_str(main_val)) {
            const char *candidate = yyjson_get_str(main_val);
            if (candidate && candidate[0]) {
                /* Try as-is first */
                module_qn = try_entry_point(candidate, pkg_dir, repo_root, project_name);
                /* If no extension and stat failed, try appending extensions */
                if (!module_qn) {
                    static const char *ext_suffixes[] = {".ts", ".js", ".tsx", ".jsx", NULL};
                    /* Strip "./" prefix for the base path */
                    const char *base = (candidate[0] == '.' && candidate[1] == '/') ? candidate + 2
                                                                                    : candidate;
                    for (int si = 0; ext_suffixes[si] && !module_qn; si++) {
                        char with_ext[512];
                        snprintf(with_ext, sizeof(with_ext), "%s%s", base, ext_suffixes[si]);
                        module_qn = try_entry_point(with_ext, pkg_dir, repo_root, project_name);
                    }
                }
            }
        }
    }

    /* 3. Fallback: try common entry points */
    if (!module_qn) {
        static const char *fallbacks[] = {"src/index.ts", "src/index.js", "index.ts", "index.js",
                                          NULL};
        for (int fi = 0; fallbacks[fi] && !module_qn; fi++) {
            module_qn = try_entry_point(fallbacks[fi], pkg_dir, repo_root, project_name);
        }
    }

    if (!module_qn) {
        /* No resolvable entry point — skip this package */
        yyjson_doc_free(doc);
        return;
    }

    /* Compute relative package directory */
    const char *pkg_rel_dir = make_rel_path(pkg_dir, repo_root);

    cbm_pkg_entry_t *entry = (cbm_pkg_entry_t *)malloc(sizeof(cbm_pkg_entry_t));
    if (!entry) {
        free(module_qn);
        yyjson_doc_free(doc);
        return;
    }
    entry->module_qn = module_qn; /* takes ownership */
    // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
    entry->pkg_dir = cbm_strdup(pkg_rel_dir);

    /* Check for existing entry before inserting to manage memory correctly.
     * cbm_ht_set stores the key pointer directly (slot->key = supplied key), so
     * each key must be a separate heap allocation owned by the table.
     * On eviction we reuse the existing strdup'd key rather than allocating a new
     * one — this avoids orphaning the old key, which would be a leak. */
    cbm_pkg_entry_t *prev = (cbm_pkg_entry_t *)cbm_ht_get(pkg_map, pkg_name);
    if (prev) {
        /* Duplicate package name: free old entry fields, then overwrite value.
         * The key allocation in the slot remains valid and is intentionally reused;
         * allocating a fresh strdup here would orphan it (cbm_ht_set does not free
         * the displaced key pointer). */
        free(prev->module_qn);
        free(prev->pkg_dir);
        free(prev);
        const char *existing_key = cbm_ht_get_key(pkg_map, pkg_name);
        cbm_ht_set(pkg_map, existing_key, entry);
    } else {
        /* New package: strdup the key so the table owns a stable heap allocation. */
        // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
        char *key = cbm_strdup(pkg_name);
        cbm_ht_set(pkg_map, key, entry);
    }

    yyjson_doc_free(doc);
}

/* Recursive directory scan helper.
 * Uses an explicit stack to avoid deep recursion on large monorepos. */
#define PKGMAP_STACK_MAX 512

static void scan_dir_for_packages(const char *repo_root, const char *project_name,
                                  CBMHashTable *pkg_map) {
    /* Stack of absolute directory paths to visit */
    char **stack = (char **)malloc(PKGMAP_STACK_MAX * sizeof(char *));
    if (!stack) {
        return;
    }
    int top = 0;

    // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
    stack[top++] = cbm_strdup(repo_root);

    while (top > 0) {
        char *dir_path = stack[--top];

        cbm_dir_t *d = cbm_opendir(dir_path);
        if (!d) {
            free(dir_path);
            continue;
        }

        bool found_pkg_json = false;
        cbm_dirent_t *ent;
        while ((ent = cbm_readdir(d)) != NULL) {
            if (!ent->is_dir) {
                if (strcmp(ent->name, "package.json") == 0) {
                    found_pkg_json = true;
                }
                continue;
            }

            /* Skip standard ignored directories */
            if (cbm_should_skip_dir(ent->name, CBM_MODE_FULL)) {
                continue;
            }

            if (top < PKGMAP_STACK_MAX) {
                char child_path[1024];
                snprintf(child_path, sizeof(child_path), "%s/%s", dir_path, ent->name);
                // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
                stack[top++] = cbm_strdup(child_path);
            }
        }
        cbm_closedir(d);

        if (found_pkg_json) {
            process_package_json(dir_path, repo_root, project_name, pkg_map);
        }

        free(dir_path);
    }

    free(stack);
}

/* ── Public API ──────────────────────────────────────────────────── */

CBMHashTable *cbm_pipeline_build_pkg_map(const char *repo_path, const char *project_name) {
    if (!repo_path || !project_name) {
        return NULL;
    }

    CBMHashTable *pkg_map = cbm_ht_create(64);
    if (!pkg_map) {
        return NULL;
    }

    scan_dir_for_packages(repo_path, project_name, pkg_map);

    if (cbm_ht_count(pkg_map) == 0) {
        cbm_ht_free(pkg_map);
        return NULL;
    }

    return pkg_map;
}

/* Free callback for cbm_ht_foreach */
static void free_pkg_entry(const char *key, void *value, void *userdata) {
    (void)userdata;
    free((char *)(uintptr_t)key);
    cbm_pkg_entry_t *entry = (cbm_pkg_entry_t *)value;
    if (entry) {
        free(entry->module_qn);
        free(entry->pkg_dir);
        free(entry);
    }
}

void cbm_pipeline_free_pkg_map(CBMHashTable *pkg_map) {
    if (!pkg_map) {
        return;
    }
    cbm_ht_foreach(pkg_map, free_pkg_entry, NULL);
    cbm_ht_free(pkg_map);
}

/* Resolve a subpath import relative to a package directory.
 * pkg_dir: repo-relative path to the package directory.
 * subpath: the portion after the package name (e.g. "utils" from "@pkg/utils").
 * Returns heap-allocated QN string, or NULL if not found. Caller must free(). */
static char *resolve_subpath(const cbm_pipeline_ctx_t *ctx, const char *pkg_dir,
                             const char *subpath) {
    static const char *suffixes[] = {".ts", ".js", ".tsx", ".jsx", "/index.ts",
                                     "/index.js", NULL};
    for (int si = 0; suffixes[si]; si++) {
        char candidate_rel[1024];
        snprintf(candidate_rel, sizeof(candidate_rel), "%s/%s%s", pkg_dir, subpath,
                 suffixes[si]);

        char full_path[2048];
        snprintf(full_path, sizeof(full_path), "%s/%s", ctx->repo_path, candidate_rel);

        struct stat st;
        if (stat(full_path, &st) == 0) {
            return cbm_pipeline_fqn_module(ctx->project_name, candidate_rel);
        }
    }

    /* Try without any suffix (subpath may already include extension) */
    char candidate_rel[1024];
    snprintf(candidate_rel, sizeof(candidate_rel), "%s/%s", pkg_dir, subpath);

    char full_path[2048];
    snprintf(full_path, sizeof(full_path), "%s/%s", ctx->repo_path, candidate_rel);

    struct stat st;
    if (stat(full_path, &st) == 0) {
        return cbm_pipeline_fqn_module(ctx->project_name, candidate_rel);
    }

    return NULL;
}

char *cbm_pipeline_resolve_module(const cbm_pipeline_ctx_t *ctx, const char *module_path) {
    if (!ctx || !module_path) {
        return cbm_pipeline_fqn_module(ctx ? ctx->project_name : NULL, module_path);
    }

    /* No package map — fall through to fqn_module for all specifiers */
    if (!ctx->pkg_map) {
        return cbm_pipeline_fqn_module(ctx->project_name, module_path);
    }

    /* Relative or absolute paths are not package specifiers */
    if (module_path[0] == '.' || module_path[0] == '/' || module_path[0] == '\\') {
        return cbm_pipeline_fqn_module(ctx->project_name, module_path);
    }

    /* 1. Exact match (bare specifier or full scoped name) */
    cbm_pkg_entry_t *entry = (cbm_pkg_entry_t *)cbm_ht_get(ctx->pkg_map, module_path);
    if (entry) {
        // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
        return cbm_strdup(entry->module_qn);
    }

    /* 2. Subpath: extract package name prefix and remainder */
    const char *pkg_name = NULL;
    const char *subpath = NULL;
    size_t pkg_name_len = 0;

    if (module_path[0] == '@') {
        /* Scoped: @scope/name/sub/path */
        const char *first_slash = strchr(module_path, '/');
        if (first_slash) {
            const char *second_slash = strchr(first_slash + 1, '/');
            if (second_slash) {
                pkg_name = module_path;
                pkg_name_len = (size_t)(second_slash - module_path);
                subpath = second_slash + 1;
            }
        }
    } else {
        /* Unscoped: name/sub/path */
        const char *first_slash = strchr(module_path, '/');
        if (first_slash) {
            pkg_name = module_path;
            pkg_name_len = (size_t)(first_slash - module_path);
            subpath = first_slash + 1;
        }
    }

    if (pkg_name && subpath && pkg_name_len > 0) {
        char pkg_name_buf[256];
        size_t copy_len = pkg_name_len < sizeof(pkg_name_buf) - 1 ? pkg_name_len
                                                                    : sizeof(pkg_name_buf) - 1;
        memcpy(pkg_name_buf, pkg_name, copy_len);
        pkg_name_buf[copy_len] = '\0';

        entry = (cbm_pkg_entry_t *)cbm_ht_get(ctx->pkg_map, pkg_name_buf);
        if (entry && entry->pkg_dir) {
            char *resolved = resolve_subpath(ctx, entry->pkg_dir, subpath);
            if (resolved) {
                return resolved;
            }
        }
    }

    /* 3. Fallback: unknown package — fqn_module produces a QN that matches no node,
     * same as before this fix. No regression. */
    return cbm_pipeline_fqn_module(ctx->project_name, module_path);
}
