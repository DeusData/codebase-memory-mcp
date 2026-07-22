/*
 * depindex.c — Dependency/reference API indexing implementation.
 *
 * Package resolution, ecosystem detection, dep discovery, auto-indexing,
 * and cross-boundary edge creation for dependency source code.
 */
#include "depindex/depindex.h"
#include "cli/cli.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "foundation/log.h"
#include "foundation/compat_fs.h"
#include "foundation/compat.h"
#include "foundation/hash_table.h"
#include "foundation/platform.h"

#include <yyjson/yyjson.h>

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    CBM_DEP_MANIFEST_MAX_BYTES = 1024 * 1024,
    CBM_DEP_DISCOVERY_INITIAL_CAPACITY = 16,
    /* Usage-ranked selection (depindex.c: cbm_discover_installed_deps) needs
     * to see more candidates than the final cap so the most-imported
     * packages can be picked instead of merely the first discovered.
     * CBM_DEP_DISCOVERY_OVERFETCH_MULTIPLIER scales the cap; the result is
     * clamped to CBM_DEP_DISCOVERY_OVERFETCH_MAX regardless of multiplier.
     * Bound rationale: each retained cbm_dep_discovered_t candidate owns
     * three heap strings (package, path, version), so unbounded over-fetch
     * is a real memory cost, not just extra compute. */
    CBM_DEP_DISCOVERY_OVERFETCH_MULTIPLIER = 5,
    CBM_DEP_DISCOVERY_OVERFETCH_MAX = 500,
};

/* Upper bound for fetching project Variable/import-reference nodes, shared by
 * cbm_dep_link_cross_edges() (matches project imports to already-indexed dep
 * Module nodes) and rank_by_import_usage() below (counts project imports per
 * not-yet-indexed candidate package). Both consumers match project import
 * references to package names by exact node name over the same node source,
 * so they share one fetch bound and one query shape rather than inventing a
 * second matching scheme (repo CLAUDE.md: prefer extending the established
 * path). */
#define CBM_DEP_PROJECT_IMPORT_FETCH_LIMIT 500

/* ── Package Manager Parse/String ──────────────────────────────── */

cbm_pkg_manager_t cbm_parse_pkg_manager(const char *s) {
    if (!s) return CBM_PKG_COUNT;
    static const struct {
        const char *name;
        cbm_pkg_manager_t val;
    } table[] = {
        {"uv", CBM_PKG_UV},         {"pip", CBM_PKG_UV},
        {"poetry", CBM_PKG_UV},     {"pdm", CBM_PKG_UV},
        {"python", CBM_PKG_UV},     {"cargo", CBM_PKG_CARGO},
        {"npm", CBM_PKG_NPM},       {"yarn", CBM_PKG_NPM},
        {"pnpm", CBM_PKG_NPM},      {"bun", CBM_PKG_BUN},
        {"go", CBM_PKG_GO},         {"jvm", CBM_PKG_JVM},
        {"maven", CBM_PKG_JVM},     {"gradle", CBM_PKG_JVM},
        {"dotnet", CBM_PKG_DOTNET}, {"nuget", CBM_PKG_DOTNET},
        {"ruby", CBM_PKG_RUBY},     {"bundler", CBM_PKG_RUBY},
        {"php", CBM_PKG_PHP},       {"composer", CBM_PKG_PHP},
        {"swift", CBM_PKG_SWIFT},   {"dart", CBM_PKG_DART},
        {"pub", CBM_PKG_DART},      {"mix", CBM_PKG_MIX},
        {"hex", CBM_PKG_MIX},       {"make", CBM_PKG_MAKE},
        {"cmake", CBM_PKG_CMAKE},   {"meson", CBM_PKG_MESON},
        {"conan", CBM_PKG_CONAN},   {"custom", CBM_PKG_CUSTOM},
        {NULL, CBM_PKG_COUNT},
    };
    for (int i = 0; table[i].name; i++) {
        if (strcmp(s, table[i].name) == 0) return table[i].val;
    }
    return CBM_PKG_COUNT;
}

const char *cbm_pkg_manager_str(cbm_pkg_manager_t mgr) {
    static const char *names[] = {
        "uv",    "cargo",  "npm",   "bun",   "go",
        "jvm",   "dotnet", "ruby",  "php",   "swift",
        "dart",  "mix",    "make",  "cmake", "meson",
        "conan", "custom"
    };
    return mgr < CBM_PKG_COUNT ? names[mgr] : "unknown";
}

/* ── Dep Naming Helpers ────────────────────────────────────────── */

char *cbm_dep_project_name(const char *project, const char *package_name) {
    if (!project || !package_name) return NULL;
    char buf[CBM_DEP_PATH_MAX];
    snprintf(buf, sizeof(buf), "%s" CBM_DEP_SEPARATOR "%s", project, package_name);
    return cbm_strdup(buf);
}

bool cbm_is_dep_project(const char *project_name, const char *session_project) {
    if (!project_name) return false;
    /* Check session-specific match first (e.g., "myapp.dep.pandas" with session "myapp") */
    if (session_project && session_project[0]) {
        size_t sp_len = strlen(session_project);
        if (strncmp(project_name, session_project, sp_len) == 0 &&
            strncmp(project_name + sp_len, CBM_DEP_SEPARATOR,
                    CBM_DEP_SEPARATOR_LEN) == 0) {
            return true;
        }
    }
    /* Generic fallback: any project containing ".dep." or starting with "dep." is a dep.
     * Handles cross-project queries where session_project doesn't match. */
    return strstr(project_name, CBM_DEP_SEPARATOR) != NULL ||
           strncmp(project_name, "dep.", 4) == 0;
}

/* Check if a file path ends with a known manifest file name.
 * Uses the shared CBM_MANIFEST_FILES list from depindex.h for DRY. */
bool cbm_is_manifest_path(const char *file_path) {
    if (!file_path) return false;
    for (int i = 0; CBM_MANIFEST_FILES[i]; i++) {
        if (strstr(file_path, CBM_MANIFEST_FILES[i])) return true;
    }
    return false;
}

/* ── Ecosystem Detection ───────────────────────────────────────── */

/* Scan project_root directory for a file matching any of the given basenames.
 * Returns true if any match found — used for wildcard-like detection (e.g. *.csproj). */
static bool dir_contains_suffix(const char *project_root, const char *suffix) {
    cbm_dir_t *d = cbm_opendir(project_root);
    if (!d) return false;
    cbm_dirent_t *ent;
    size_t slen = strlen(suffix);
    while ((ent = cbm_readdir(d)) != NULL) {
        size_t nlen = strlen(ent->name);
        if (nlen >= slen && strcmp(ent->name + nlen - slen, suffix) == 0) {
            cbm_closedir(d);
            return true;
        }
    }
    cbm_closedir(d);
    return false;
}

/* Check for a vendored dependency directory (vendor/, vendored/, third_party/, etc.).
 * Returns true if any conventional vendor dir exists with at least one subdirectory. */
static bool has_vendored_deps_dir(const char *project_root) {
    static const char *vendor_dirs[] = {
        "vendor", "vendored", "third_party", "thirdparty",
        "deps", "external", "ext", "contrib", "lib",
        "_vendor", "submodules", NULL
    };
    char path[CBM_DEP_PATH_MAX];
    for (int i = 0; vendor_dirs[i]; i++) {
        snprintf(path, sizeof(path), "%s/%s", project_root, vendor_dirs[i]);
        cbm_dir_t *d = cbm_opendir(path);
        if (!d) continue;
        cbm_dirent_t *ent;
        bool has_subdir = false;
        while ((ent = cbm_readdir(d)) != NULL) {
            if (ent->name[0] == '.') continue;
            char sub[CBM_DEP_PATH_MAX];
            snprintf(sub, sizeof(sub), "%s/%s", path, ent->name);
            if (cbm_is_dir(sub)) { has_subdir = true; break; }
        }
        cbm_closedir(d);
        if (has_subdir) return true;
    }
    return false;
}

cbm_pkg_manager_t cbm_detect_ecosystem(const char *project_root) {
    if (!project_root) return CBM_PKG_COUNT;
    char path[CBM_DEP_PATH_MAX];

/* Macro: check file exists → return manager */
#define CHECK(file, mgr) \
    do { snprintf(path, sizeof(path), "%s/" file, project_root); \
         if (cbm_file_exists(path)) return (mgr); } while (0)

    /* Interpreted-language ecosystems (highest confidence — unique lockfiles/manifests) */
    CHECK("bun.lockb",       CBM_PKG_BUN);   /* bun before npm: more specific */
    CHECK("pyproject.toml",  CBM_PKG_UV);
    CHECK("setup.py",        CBM_PKG_UV);
    CHECK("requirements.txt",CBM_PKG_UV);
    CHECK("Pipfile",         CBM_PKG_UV);
    CHECK("Cargo.toml",      CBM_PKG_CARGO);
    CHECK("go.mod",          CBM_PKG_GO);
    CHECK("pom.xml",         CBM_PKG_JVM);
    CHECK("build.gradle",    CBM_PKG_JVM);
    CHECK("build.gradle.kts",CBM_PKG_JVM);
    CHECK("package.json",    CBM_PKG_NPM);
    CHECK("Gemfile",         CBM_PKG_RUBY);
    CHECK("composer.json",   CBM_PKG_PHP);
    CHECK("Package.swift",   CBM_PKG_SWIFT);
    CHECK("pubspec.yaml",    CBM_PKG_DART);
    CHECK("mix.exs",         CBM_PKG_MIX);

    /* .NET: check well-known files first, then scan for *.csproj / *.fsproj */
    CHECK("global.json",            CBM_PKG_DOTNET);
    CHECK("Directory.Build.props",  CBM_PKG_DOTNET);
    CHECK("NuGet.Config",           CBM_PKG_DOTNET);
    if (dir_contains_suffix(project_root, ".csproj") ||
        dir_contains_suffix(project_root, ".fsproj") ||
        dir_contains_suffix(project_root, ".vbproj")) return CBM_PKG_DOTNET;

    /* C/C++ build systems */
    CHECK("conanfile.txt",   CBM_PKG_CONAN);  /* Conan before CMake: conanfile may coexist */
    CHECK("conanfile.py",    CBM_PKG_CONAN);
    CHECK("vcpkg.json",      CBM_PKG_CMAKE);  /* vcpkg always used with CMake */
    CHECK("CMakeLists.txt",  CBM_PKG_CMAKE);
    CHECK("meson.build",     CBM_PKG_MESON);
    CHECK("Makefile",        CBM_PKG_MAKE);
    CHECK("GNUmakefile",     CBM_PKG_MAKE);
    CHECK("BSDmakefile",     CBM_PKG_MAKE);
    CHECK("Makefile.cbm",    CBM_PKG_MAKE); /* non-standard but used by codebase-memory-mcp itself */

#undef CHECK

    /* Generic: vendored deps in vendor/ vendored/ etc. (any language with bundled deps) */
    if (has_vendored_deps_dir(project_root)) return CBM_PKG_CUSTOM;

    return CBM_PKG_COUNT;
}

/* ── Package Resolution ────────────────────────────────────────── */

void cbm_dep_resolved_free(cbm_dep_resolved_t *r) {
    if (!r) return;
    free((void *)r->path);
    free((void *)r->version);
    r->path = NULL;
    r->version = NULL;
}

static const char *get_home_dir(char *buf, size_t buf_sz) {
#ifdef _WIN32
    const char *home = cbm_safe_getenv("USERPROFILE", buf, buf_sz, NULL);
    if (!home) home = cbm_safe_getenv("HOME", buf, buf_sz, NULL);
#else
    const char *home = cbm_safe_getenv("HOME", buf, buf_sz, NULL);
#endif
    return home ? home : "/tmp";
}

/* Resolve Python package in .venv or venv site-packages.
 * Runtime: O(N_python_versions) where N is typically 1.
 * Memory: O(1) stack buffers only. */
static int resolve_uv(const char *package_name, const char *project_root,
                       cbm_dep_resolved_t *out) {
    char probe[CBM_DEP_PATH_MAX];
    char underscore_name[CBM_NAME_MAX];
    snprintf(underscore_name, sizeof(underscore_name), "%s", package_name);
    for (char *c = underscore_name; *c; c++) {
        if (*c == '-') *c = '_';
    }

    const char *variants[3] = {package_name, NULL, NULL};
    if (strcmp(underscore_name, package_name) != 0) {
        variants[1] = underscore_name;
    }

    /* Try .venv/ and venv/ prefixes */
    static const char *venv_prefixes[] = {".venv", "venv", NULL};

    for (int v = 0; variants[v]; v++) {
        for (int p = 0; venv_prefixes[p]; p++) {
            snprintf(probe, sizeof(probe), "%s/%s/lib", project_root, venv_prefixes[p]);
            cbm_dir_t *d = cbm_opendir(probe);
            if (!d) continue;
            cbm_dirent_t *ent;
            while ((ent = cbm_readdir(d)) != NULL) {
                if (strncmp(ent->name, "python", 6) != 0) continue;
                snprintf(probe, sizeof(probe), "%s/%s/lib/%s/site-packages/%s",
                         project_root, venv_prefixes[p], ent->name, variants[v]);
                if (cbm_file_exists(probe)) {
                    out->path = cbm_strdup(probe);
                    cbm_closedir(d);
                    return 0;
                }
            }
            cbm_closedir(d);
        }
    }
    return -1;
}

/* Resolve Rust crate from cargo registry.
 * Runtime: O(N_registry_dirs * N_crate_dirs). Typically 1 registry * ~100 crates.
 * Memory: O(1) stack buffers only. */
static int resolve_cargo(const char *package_name, const char *project_root,
                          cbm_dep_resolved_t *out) {
    (void)project_root;
    char home_buf[CBM_DEP_PATH_MAX];
    const char *home = get_home_dir(home_buf, sizeof(home_buf));
    char cargo_home_buf[CBM_DEP_PATH_MAX];
    const char *cargo_home =
        cbm_safe_getenv("CARGO_HOME", cargo_home_buf, sizeof(cargo_home_buf), NULL);
    char registry_base[CBM_DEP_PATH_MAX];
    if (cargo_home) {
        snprintf(registry_base, sizeof(registry_base), "%s/registry/src", cargo_home);
    } else {
        snprintf(registry_base, sizeof(registry_base), "%s/.cargo/registry/src", home);
    }

    cbm_dir_t *d = cbm_opendir(registry_base);
    if (!d) return -1;

    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        if (strncmp(ent->name, "index.crates.io-", 16) != 0) continue;
        char reg_path[CBM_DEP_PATH_MAX];
        snprintf(reg_path, sizeof(reg_path), "%s/%s", registry_base, ent->name);
        cbm_dir_t *rd = cbm_opendir(reg_path);
        if (!rd) continue;
        cbm_dirent_t *rent;
        while ((rent = cbm_readdir(rd)) != NULL) {
            size_t pkg_len = strlen(package_name);
            if (strncmp(rent->name, package_name, pkg_len) == 0 &&
                rent->name[pkg_len] == '-') {
                char full[CBM_DEP_PATH_MAX];
                snprintf(full, sizeof(full), "%s/%s", reg_path, rent->name);
                out->path = cbm_strdup(full);
                out->version = cbm_strdup(rent->name + pkg_len + 1);
                cbm_closedir(rd);
                cbm_closedir(d);
                return 0;
            }
        }
        cbm_closedir(rd);
    }
    cbm_closedir(d);
    return -1;
}

/* Resolve npm/bun package from node_modules.
 * Runtime: O(1) — direct path check.
 * Memory: O(1) stack buffer. */
static int resolve_npm(const char *package_name, const char *project_root,
                        cbm_dep_resolved_t *out) {
    char probe[CBM_DEP_PATH_MAX];
    snprintf(probe, sizeof(probe), "%s/node_modules/%s", project_root, package_name);
    if (cbm_file_exists(probe)) {
        out->path = cbm_strdup(probe);
        return 0;
    }
    return -1;
}

int cbm_resolve_pkg_source(cbm_pkg_manager_t mgr, const char *package_name,
                           const char *project_root, cbm_dep_resolved_t *out) {
    if (!package_name || !project_root || !out) return -1;
    out->path = NULL;
    out->version = NULL;

    switch (mgr) {
    case CBM_PKG_UV:
        return resolve_uv(package_name, project_root, out);
    case CBM_PKG_CARGO:
        return resolve_cargo(package_name, project_root, out);
    case CBM_PKG_NPM:
    case CBM_PKG_BUN:
        return resolve_npm(package_name, project_root, out);
    case CBM_PKG_CUSTOM:
        return -1; /* source_paths[] provides path directly */
    default:
        return -1;
    }
}

/* ── Dep Discovery ─────────────────────────────────────────────── */

void cbm_dep_discovered_free(cbm_dep_discovered_t *deps, int count) {
    if (!deps) return;
    for (int i = 0; i < count; i++) {
        free((void *)deps[i].package);
        free((void *)deps[i].path);
        free((void *)deps[i].version);
    }
    free(deps);
}

static char *read_dependency_manifest(const char *path, size_t *out_len) {
    if (out_len)
        *out_len = 0;
    FILE *fp = cbm_fopen(path, "rb");
    if (!fp)
        return NULL;
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    long size = ftell(fp);
    if (size <= 0 || size > CBM_DEP_MANIFEST_MAX_BYTES || fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }
    char *contents = malloc((size_t)size + 1);
    if (!contents) {
        fclose(fp);
        return NULL;
    }
    size_t read_len = fread(contents, 1, (size_t)size, fp);
    fclose(fp);
    if (read_len != (size_t)size) {
        free(contents);
        return NULL;
    }
    contents[read_len] = '\0';
    if (out_len)
        *out_len = read_len;
    return contents;
}

/* package.json extraction does not preserve the `dependencies` object in each
 * child's qualified name, so querying graph QNs for "dependencies" misses real
 * package names. Parse the bounded manifest directly and resolve only packages
 * that actually exist under node_modules. Runtime is O(manifest bytes + declared
 * packages); retained memory is O(min(installed packages, max_results)). */
static int discover_npm_deps(cbm_pkg_manager_t mgr, const char *project_root,
                             cbm_dep_discovered_t **out, int *count, int max_results) {
    *out = NULL;
    *count = 0;
    char manifest_path[CBM_DEP_PATH_MAX];
    int path_len = snprintf(manifest_path, sizeof(manifest_path), "%s/package.json", project_root);
    if (path_len <= 0 || (size_t)path_len >= sizeof(manifest_path))
        return 0;

    size_t source_len = 0;
    char *source = read_dependency_manifest(manifest_path, &source_len);
    if (!source)
        return 0;
    yyjson_doc *doc = yyjson_read(source, source_len, 0);
    free(source);
    if (!doc)
        return 0;
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return 0;
    }

    int capacity = max_results < CBM_DEP_DISCOVERY_INITIAL_CAPACITY
                       ? max_results
                       : CBM_DEP_DISCOVERY_INITIAL_CAPACITY;
    cbm_dep_discovered_t *results = calloc((size_t)capacity, sizeof(*results));
    CBMHashTable *seen = cbm_ht_create((uint32_t)capacity);
    if (!results || !seen) {
        free(results);
        cbm_ht_free(seen);
        yyjson_doc_free(doc);
        return -1;
    }

    static const char *const sections[] = {
        "dependencies", "devDependencies", "optionalDependencies", "peerDependencies", NULL,
    };
    int result_count = 0;
    for (int section_index = 0; sections[section_index] && result_count < max_results;
         section_index++) {
        yyjson_val *section = yyjson_obj_get(root, sections[section_index]);
        if (!yyjson_is_obj(section))
            continue;
        yyjson_obj_iter iter = yyjson_obj_iter_with(section);
        yyjson_val *key = NULL;
        while ((key = yyjson_obj_iter_next(&iter)) != NULL && result_count < max_results) {
            const char *package = yyjson_get_str(key);
            if (!package || !package[0] || cbm_ht_has(seen, package))
                continue;
            cbm_ht_set(seen, package, (void *)(uintptr_t)1);

            cbm_dep_resolved_t resolved = {0};
            if (cbm_resolve_pkg_source(mgr, package, project_root, &resolved) != 0)
                continue;
            if (result_count == capacity) {
                int next_capacity = capacity > max_results / 2 ? max_results : capacity * 2;
                cbm_dep_discovered_t *grown =
                    realloc(results, (size_t)next_capacity * sizeof(*results));
                if (!grown) {
                    cbm_dep_resolved_free(&resolved);
                    cbm_dep_discovered_free(results, result_count);
                    cbm_ht_free(seen);
                    yyjson_doc_free(doc);
                    return -1;
                }
                memset(grown + capacity, 0, (size_t)(next_capacity - capacity) * sizeof(*results));
                results = grown;
                capacity = next_capacity;
            }
            results[result_count].package = cbm_strdup(package);
            if (!results[result_count].package) {
                cbm_dep_resolved_free(&resolved);
                cbm_dep_discovered_free(results, result_count);
                cbm_ht_free(seen);
                yyjson_doc_free(doc);
                return -1;
            }
            results[result_count].path = resolved.path;
            results[result_count].version = resolved.version;
            result_count++;
        }
    }

    cbm_ht_free(seen);
    yyjson_doc_free(doc);
    *out = results;
    *count = result_count;
    return 0;
}

/* Discover vendored dependencies by scanning conventional vendor directories.
 * Used for C/C++ build systems (Make, CMake, Meson, Conan) and generic CBM_PKG_CUSTOM.
 * Each named subdirectory in vendor/ vendored/ third_party/ etc. becomes a dep entry. */
static int discover_vendored_deps(const char *project_root, cbm_dep_discovered_t **out,
                                  int *count, int max_results) {
    static const char *vendor_dirs[] = {
        "vendor", "vendored", "third_party", "thirdparty",
        "deps", "external", "ext", "contrib", "lib",
        "_vendor", "submodules", NULL
    };

    *out = calloc((size_t)max_results, sizeof(cbm_dep_discovered_t));
    if (!*out) return -1;
    *count = 0;

    char dir_path[CBM_DEP_PATH_MAX];
    for (int vi = 0; vendor_dirs[vi] && *count < max_results; vi++) {
        snprintf(dir_path, sizeof(dir_path), "%s/%s", project_root, vendor_dirs[vi]);
        cbm_dir_t *d = cbm_opendir(dir_path);
        if (!d) continue;
        cbm_dirent_t *ent;
        while ((ent = cbm_readdir(d)) != NULL && *count < max_results) {
            if (ent->name[0] == '.') continue;
            char sub[CBM_DEP_PATH_MAX];
            snprintf(sub, sizeof(sub), "%s/%s", dir_path, ent->name);
            if (!cbm_is_dir(sub)) continue;
            (*out)[*count].package = cbm_strdup(ent->name);
            (*out)[*count].path    = cbm_strdup(sub);
            (*count)++;
        }
        cbm_closedir(d);
    }
    return 0;
}

/* Sort key for rank_by_import_usage: pairs a discovered candidate with how
 * many distinct project import references name it. A plain array of
 * cbm_dep_discovered_t can't carry a sort key without either mutating the
 * public struct or threading context through qsort (not portable in C11),
 * so this local wrapper is the established pattern in this codebase for
 * qsort-by-derived-key (see path_alias.c comparators). */
typedef struct {
    cbm_dep_discovered_t entry;
    int64_t import_count;
} cbm_dep_rank_entry_t;

/* Descending by import_count; ties broken by package name ascending so
 * selection is deterministic and reproducible across runs. */
static int cmp_dep_rank_entry(const void *a, const void *b) {
    const cbm_dep_rank_entry_t *ea = a;
    const cbm_dep_rank_entry_t *eb = b;
    if (ea->import_count != eb->import_count)
        return (ea->import_count < eb->import_count) ? 1 : -1;
    const char *na = ea->entry.package ? ea->entry.package : "";
    const char *nb = eb->entry.package ? eb->entry.package : "";
    return strcmp(na, nb);
}

/* Rank discovered candidates by how many distinct project import references
 * name each package, descending; tiebreak by package name ascending.
 * Reorders candidates[] in place.
 *
 * Reuses the exact node source cbm_dep_link_cross_edges() matches against
 * dep Module nodes (project-scoped Variable-label nodes, matched by exact
 * name) instead of inventing a second import-matching scheme, per repo
 * CLAUDE.md.
 *
 * Returns 0 on success. Returns -1 if the store query failed; callers MUST
 * fail OPEN on -1 (keep discovery order) rather than fail the whole
 * auto-index — ranking is a refinement, not a correctness requirement. */
static int rank_by_import_usage(cbm_store_t *store, const char *project_name,
                                cbm_dep_discovered_t *candidates, int candidate_count) {
    if (!store || !project_name || !candidates || candidate_count <= 0) return -1;

    cbm_search_params_t params = {0};
    params.project = project_name;
    params.project_exact = true;
    params.label = "Variable";
    params.limit = CBM_DEP_PROJECT_IMPORT_FETCH_LIMIT;

    cbm_search_output_t out = {0};
    if (cbm_store_search(store, &params, &out) != 0) {
        cbm_store_search_free(&out);
        return -1;
    }

    CBMHashTable *counts = cbm_ht_create((uint32_t)(out.count > 0 ? out.count : 1));
    if (!counts) {
        cbm_store_search_free(&out);
        return -1;
    }
    for (int i = 0; i < out.count; i++) {
        const char *name = out.results[i].node.name;
        if (!name || !name[0]) continue;
        intptr_t cur = (intptr_t)cbm_ht_get(counts, name);
        cbm_ht_set(counts, name, (void *)(cur + 1));
    }

    cbm_dep_rank_entry_t *ranked = calloc((size_t)candidate_count, sizeof(*ranked));
    if (!ranked) {
        cbm_ht_free(counts);
        cbm_store_search_free(&out);
        return -1;
    }
    for (int i = 0; i < candidate_count; i++) {
        ranked[i].entry = candidates[i];
        ranked[i].import_count = candidates[i].package
                                      ? (int64_t)(intptr_t)cbm_ht_get(counts, candidates[i].package)
                                      : 0;
    }
    qsort(ranked, (size_t)candidate_count, sizeof(*ranked), cmp_dep_rank_entry);
    for (int i = 0; i < candidate_count; i++) {
        candidates[i] = ranked[i].entry;
    }

    free(ranked);
    cbm_ht_free(counts);
    cbm_store_search_free(&out);
    return 0;
}

/* Discover installed deps by querying the graph for Variable nodes
 * in manifest files under dependency sections.
 * Runtime: O(search_limit) for query + O(N) for filtering + O(N) for resolution.
 * Memory: O(fetch_limit) for the results array before ranking truncates it to
 * max_results (see CBM_DEP_DISCOVERY_OVERFETCH_MULTIPLIER/_MAX). */
int cbm_discover_installed_deps(cbm_pkg_manager_t mgr, const char *project_root,
                                cbm_store_t *store, const char *project_name,
                                cbm_dep_discovered_t **out, int *count,
                                int max_results) {
    if (!store || !project_name || !out || !count) return -1;
    *out = NULL;
    *count = 0;
    if (max_results <= 0) max_results = CBM_DEFAULT_AUTO_DEP_LIMIT;

    /* Over-fetch so usage ranking has more candidates to choose from than the
     * final cap. When the caller's cap already meets or exceeds the ceiling,
     * fetch exactly what was asked (no headroom to rank within, but never
     * less than requested). */
    int fetch_limit = max_results;
    if (max_results < INT_MAX / CBM_DEP_DISCOVERY_OVERFETCH_MULTIPLIER) {
        int overfetch = max_results * CBM_DEP_DISCOVERY_OVERFETCH_MULTIPLIER;
        fetch_limit = overfetch < CBM_DEP_DISCOVERY_OVERFETCH_MAX ? overfetch
                                                                   : CBM_DEP_DISCOVERY_OVERFETCH_MAX;
        if (fetch_limit < max_results) fetch_limit = max_results;
    }

    int rc;
    if (mgr == CBM_PKG_NPM || mgr == CBM_PKG_BUN) {
        rc = discover_npm_deps(mgr, project_root, out, count, fetch_limit);
    } else if (mgr == CBM_PKG_MAKE || mgr == CBM_PKG_CMAKE || mgr == CBM_PKG_MESON ||
               mgr == CBM_PKG_CONAN || mgr == CBM_PKG_CUSTOM) {
        /* C/C++ build systems and generic vendored deps: scan vendor directories
         * directly. These don't have a registry/lockfile to parse; deps live in
         * the source tree. */
        rc = discover_vendored_deps(project_root, out, count, fetch_limit);
    } else {
        cbm_search_params_t params = {0};
        params.project = project_name;
        params.label = "Variable";
        params.qn_pattern = "dependencies|require";
        /* Raw-hit window stays tied to max_results (not fetch_limit): this is
         * the SQL fetch bound for filtering, distinct from the retention cap
         * below. Keeping it unscaled by fetch_limit means the underlying
         * query and its ordering never change, so results are byte-identical
         * to today whenever the true distinct-package count is within
         * max_results — only the retention cap grows to let ranking see more
         * of the SAME raw window. */
        params.limit = max_results * 5; /* over-fetch since we filter post-query */

        cbm_search_output_t search_out = {0};
        rc = cbm_store_search(store, &params, &search_out);
        if (rc != 0) {
            cbm_store_search_free(&search_out);
            return -1;
        }

        cbm_dep_discovered_t *results = calloc((size_t)fetch_limit, sizeof(cbm_dep_discovered_t));
        if (!results) {
            cbm_store_search_free(&search_out);
            return -1;
        }

        int n = 0;
        for (int i = 0; i < search_out.count && n < fetch_limit; i++) {
            const char *fp = search_out.results[i].node.file_path;
            const char *name = search_out.results[i].node.name;
            if (!fp || !name || !name[0]) continue;

            /* Filter to manifest files only (DRY via CBM_MANIFEST_FILES) */
            if (!cbm_is_manifest_path(fp)) continue;

            cbm_dep_resolved_t resolved = {0};
            if (cbm_resolve_pkg_source(mgr, name, project_root, &resolved) == 0) {
                results[n].package = cbm_strdup(name);
                results[n].path = resolved.path;
                results[n].version = resolved.version;
                n++;
            }
        }

        cbm_store_search_free(&search_out);
        *out = results;
        *count = n;
        rc = 0;
    }
    if (rc != 0) return rc;

    /* Rank-then-truncate only when discovery actually found more than the
     * cap; otherwise the result is byte-identical to today regardless of the
     * (larger) internal fetch_limit used above — see the callers of this
     * function in cbm_dep_auto_index_effective(). */
    if (*count > max_results) {
        if (rank_by_import_usage(store, project_name, *out, *count) != 0) {
            /* Fail open: store query failed, keep discovery order (today's
             * semantics) rather than fail the whole auto-index. */
        }
        for (int i = max_results; i < *count; i++) {
            free((void *)(*out)[i].package);
            free((void *)(*out)[i].path);
            free((void *)(*out)[i].version);
        }
        *count = max_results;
    }

    return 0;
}

/* ── Auto-Index ────────────────────────────────────────────────── */

int cbm_dep_normalize_configured_limit(int limit, int default_limit) {
    if (limit == 0) {
        return -1;
    }
    if (limit > 0 && limit <= CBM_MAX_AUTO_DEP_LIMIT) {
        return limit;
    }
    return default_limit > 0 && default_limit <= CBM_MAX_AUTO_DEP_LIMIT
               ? default_limit
               : CBM_DEFAULT_AUTO_DEP_LIMIT;
}

int cbm_dep_auto_index_effective_limit(cbm_config_t *cfg, int default_limit) {
    if (!cbm_config_get_bool(cfg, CBM_CONFIG_AUTO_INDEX_DEPS, CBM_DEFAULT_AUTO_INDEX_DEPS)) {
        return 0;
    }

    int limit = cbm_config_get_int(cfg, CBM_CONFIG_AUTO_DEP_LIMIT, default_limit);
    /* The direct API keeps max_deps=0 as disabled. The config registry documents
     * auto_dep_limit=0 as unlimited, so map configured callers to -1 here. */
    return cbm_dep_normalize_configured_limit(limit, default_limit);
}

/* Auto-detect ecosystem, discover deps, index each via flush_to_store.
 * Runtime: O(N_deps * pipeline_run) where pipeline_run is O(files * parse_time).
 * With max 1000 files/dep at ~1ms/file: ~1s/dep * 20 deps = ~20s worst case.
 * Memory: O(symbols_per_dep) peak per dep pipeline, freed between iterations. */
int cbm_dep_auto_index_effective(const char *project_name, const char *project_root,
                                 cbm_store_t *store, int effective_max_deps,
                                 cbm_config_t *cfg) {
    if (effective_max_deps == 0) return 0;
    int effective_max = (effective_max_deps < 0) ? INT_MAX : effective_max_deps;
    cbm_pkg_manager_t mgr = cbm_detect_ecosystem(project_root);
    if (mgr == CBM_PKG_COUNT) return 0;

    cbm_dep_discovered_t *deps = NULL;
    int dep_count = 0;
    if (cbm_discover_installed_deps(mgr, project_root, store, project_name,
                                    &deps, &dep_count, effective_max) != 0) {
        return 0;
    }

    int reindexed = 0;
    for (int i = 0; i < dep_count; i++) {
        if (!deps[i].path || !deps[i].package || !deps[i].package[0]) continue;
        char *dep_proj = cbm_dep_project_name(project_name, deps[i].package);
        if (!dep_proj) continue;

        cbm_pipeline_t *dp = cbm_pipeline_new(deps[i].path, NULL, CBM_MODE_DEP);
        if (dp) {
            cbm_pipeline_apply_config(dp, cfg);
            cbm_pipeline_set_project_name(dp, dep_proj);
            bool current = false;
            int current_rc = cbm_pipeline_store_project_current(dp, store, &current);
            if (current_rc == CBM_STORE_OK && current) {
                cbm_pipeline_free(dp);
                free(dep_proj);
                continue;
            }
            cbm_pipeline_set_flush_store(dp, store);
            if (cbm_pipeline_run(dp) == 0) reindexed++;
            cbm_pipeline_free(dp);
        }
        free(dep_proj);
    }
    cbm_dep_discovered_free(deps, dep_count);

    if (dep_count > 0) {
        cbm_dep_link_cross_edges(store, project_name);
    }
    if (reindexed > 0) {
        int fts_rc = cbm_store_rebuild_nodes_fts(store);
        if (fts_rc != CBM_STORE_OK) {
            char rc_str[CBM_NAME_MAX];
            snprintf(rc_str, sizeof(rc_str), "%d", fts_rc);
            cbm_log_error("dep.auto_index", "phase", "rebuild_nodes_fts", "rc", rc_str);
        }
    }

    return reindexed;
}

int cbm_dep_auto_index(const char *project_name, const char *project_root,
                       cbm_store_t *store, int max_deps, cbm_config_t *cfg) {
    int effective_max_deps = cfg ? cbm_dep_auto_index_effective_limit(cfg, max_deps) : max_deps;
    return cbm_dep_auto_index_effective(project_name, project_root, store, effective_max_deps, cfg);
}

/* ── Cross-Boundary Edges ──────────────────────────────────────── */

/* Cross-boundary edge creation links project IMPORTS nodes to dep Module nodes.
 *
 * For each IMPORTS node in the project, check if a matching Module node exists
 * in any dep project (project_name.dep.*). If so, create an IMPORTS edge from
 * the project's import node to the dep's module node.
 *
 * This enables trace_path to follow imports across the project/dep boundary. */
/* Upper bound for the one-shot bulk fetch of dep Module nodes when linking
 * cross-boundary IMPORTS edges. Named (not magic) — per the no-magic-values
 * convention. Dep linking is index-time, so a generous fetch is fine. */
#define CBM_DEP_LINK_MODULE_FETCH 100000

int cbm_dep_link_cross_edges(cbm_store_t *store, const char *project_name) {
    if (!store || !project_name || !project_name[0]) return 0;

    /* Find all IMPORTS nodes in the main project */
    cbm_search_params_t params = {0};
    params.project = project_name;
    params.project_exact = true;
    params.label = "Variable";  /* import statements are typically Variable nodes */
    params.limit = CBM_DEP_PROJECT_IMPORT_FETCH_LIMIT;

    cbm_search_output_t out = {0};
    int rc = cbm_store_search(store, &params, &out);
    if (rc != 0 || out.count == 0) {
        cbm_store_search_free(&out);
        return 0;
    }

    /* Perf #8: was N+1 — one cbm_store_search PER import (up to 500) to find a
     * matching dep Module. Now ONE bulk fetch of all dep Module nodes + an
     * in-memory name→id hash, then O(1) per import. Behavior preserved: first
     * Module matching the name wins (hash set only if absent), matching the old
     * limit=1 first-result semantics. */
    char dep_pattern[CBM_NAME_MAX];
    snprintf(dep_pattern, sizeof(dep_pattern), "%s" CBM_DEP_SEPARATOR "%%",
             project_name);

    cbm_search_params_t mod_params = {0};
    mod_params.project_pattern = dep_pattern;
    mod_params.label = "Module";
    mod_params.limit = CBM_DEP_LINK_MODULE_FETCH;
    cbm_search_output_t mod_out = {0};
    CBMHashTable *mod_by_name = NULL;
    /* node ids are >= 1, so (void*)(intptr_t)id is non-NULL for present modules
     * and cbm_ht_get returns NULL for absent names — a clean presence test. */
    if (cbm_store_search(store, &mod_params, &mod_out) == 0) {
        mod_by_name = cbm_ht_create((uint32_t)mod_out.count + 8);
        for (int i = 0; i < mod_out.count; i++) {
            const char *mname = mod_out.results[i].node.name;
            if (!mname || !mname[0]) continue;
            if (cbm_ht_get(mod_by_name, mname)) continue; /* first-wins */
            int64_t mid = mod_out.results[i].node.id;
            cbm_ht_set(mod_by_name, mname, (void *)(intptr_t)mid);
        }
    }

    int linked = 0;
    for (int i = 0; i < out.count; i++) {
        const char *import_name = out.results[i].node.name;
        if (!import_name || !import_name[0]) continue;

        void *hit = cbm_ht_get(mod_by_name, import_name);
        if (!hit) continue;

        cbm_edge_t edge = {
            .source_id = out.results[i].node.id,
            .target_id = (int64_t)(intptr_t)hit,
            .type = "IMPORTS",
            .project = project_name,
        };
        int64_t edge_id = cbm_store_insert_edge(store, &edge);
        if (edge_id <= 0) {
            continue;
        }
        linked++;
        if (out.results[i].node.file_path && out.results[i].node.file_path[0] &&
            cbm_store_upsert_edge_owner(store, project_name, edge_id,
                                        out.results[i].node.file_path, NULL,
                                        CBM_PIPELINE_FILE_DELTA_GENERATION) != CBM_STORE_OK) {
            cbm_log_warn("dep.cross_edges.owner", "project", project_name, "file",
                         out.results[i].node.file_path);
        }
    }

    if (mod_by_name) cbm_ht_free(mod_by_name); /* keys borrowed from mod_out, not freed */
    cbm_store_search_free(&mod_out);
    cbm_store_search_free(&out);

    if (linked > 0) {
        char linked_str[16];
        snprintf(linked_str, sizeof(linked_str), "%d", linked);
        cbm_log_info("dep.cross_edges", "project", project_name,
                     "linked", linked_str);
    }

    return linked;
}
