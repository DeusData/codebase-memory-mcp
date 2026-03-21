/*
 * depindex.h — Dependency/reference API indexing.
 *
 * Provides package resolution, ecosystem detection, and auto-indexing
 * for dependency source code. Dependencies are stored in the SAME db
 * as project code with "{project}.dep.{package}" project names.
 *
 * Primary interface: source_paths[] (works for all 78 languages).
 * Convenience shortcuts: package_manager for uv/cargo/npm/bun.
 *
 * Depends on: pipeline, store, foundation
 */
#ifndef CBM_DEPINDEX_H
#define CBM_DEPINDEX_H

#include <stdbool.h>
#include <stddef.h>

/* Forward declarations */
typedef struct cbm_store cbm_store_t;

/* ── Constants ─────────────────────────────────────────────────── */

#define CBM_PATH_MAX 4096
#define CBM_NAME_MAX 512
#define CBM_DEP_SEPARATOR ".dep."
#define CBM_DEP_SEPARATOR_LEN 5

/* DRY manifest file list — used by depindex, pass_configlink, and dep discovery.
 * These are the basenames of files that declare project dependencies.
 * When adding a new manifest file, add it here — all consumers pick it up. */
static const char *CBM_MANIFEST_FILES[] = {
    "Cargo.toml", "pyproject.toml", "package.json", "go.mod",
    "requirements.txt", "Gemfile", "build.gradle", "pom.xml",
    "composer.json", "pubspec.yaml", "mix.exs", "Package.swift",
    "setup.py", "Pipfile", NULL
};

/* Default limits (convention: -1=unlimited, 0=disabled, >0=limit) */
#define CBM_DEFAULT_AUTO_DEP_LIMIT 20
#define CBM_DEFAULT_DEP_MAX_FILES 1000

/* Config key strings */
#define CBM_CONFIG_AUTO_INDEX_DEPS "auto_index_deps"
#define CBM_CONFIG_AUTO_DEP_LIMIT "auto_dep_limit"
#define CBM_CONFIG_DEP_MAX_FILES "dep_max_files"

/* ── Package Manager Enum ──────────────────────────────────────── */

typedef enum {
    CBM_PKG_UV = 0,
    CBM_PKG_CARGO,
    CBM_PKG_NPM,
    CBM_PKG_BUN,
    CBM_PKG_GO,
    CBM_PKG_JVM,
    CBM_PKG_DOTNET,
    CBM_PKG_RUBY,
    CBM_PKG_PHP,
    CBM_PKG_SWIFT,
    CBM_PKG_DART,
    CBM_PKG_MIX,
    CBM_PKG_CUSTOM,
    CBM_PKG_COUNT /* sentinel / invalid */
} cbm_pkg_manager_t;

/* Parse "uv"/"cargo"/"npm"/"bun"/etc → enum. Returns CBM_PKG_COUNT if unknown. */
cbm_pkg_manager_t cbm_parse_pkg_manager(const char *s);

/* Manager enum → short string ("uv", "cargo", etc.) */
const char *cbm_pkg_manager_str(cbm_pkg_manager_t mgr);

/* ── Dep Naming Helpers ────────────────────────────────────────── */

/* Build dep project name: "{project}.dep.{package}". Caller must free(). */
char *cbm_dep_project_name(const char *project, const char *package_name);

/* Check if a project name is a dependency.
 * session_project non-NULL: precise prefix check "{session}.dep.".
 * session_project NULL: fallback strstr check. */
bool cbm_is_dep_project(const char *project_name, const char *session_project);

/* Check if a file path contains a known manifest file name.
 * Uses the shared CBM_MANIFEST_FILES list. */
bool cbm_is_manifest_path(const char *file_path);

/* ── Ecosystem Detection ───────────────────────────────────────── */

/* Detect ecosystem from project root by checking marker files.
 * Returns CBM_PKG_COUNT if no ecosystem detected. */
cbm_pkg_manager_t cbm_detect_ecosystem(const char *project_root);

/* ── Package Resolution ────────────────────────────────────────── */

typedef struct {
    const char *path;    /* absolute path to package source (heap) */
    const char *version; /* detected version, or NULL (heap) */
} cbm_dep_resolved_t;

void cbm_dep_resolved_free(cbm_dep_resolved_t *r);

/* Resolve package source directory and version on disk.
 * Returns 0 on success, -1 if package source not found. */
int cbm_resolve_pkg_source(cbm_pkg_manager_t mgr, const char *package_name,
                           const char *project_root, cbm_dep_resolved_t *out);

/* ── Dep Discovery ─────────────────────────────────────────────── */

typedef struct {
    const char *package; /* package name (heap) */
    const char *path;    /* absolute source path (heap) */
    const char *version; /* version or NULL (heap) */
} cbm_dep_discovered_t;

/* Discover installed deps by querying the indexed graph.
 * store: open store with freshly indexed project.
 * Returns 0 on success. Caller must call cbm_dep_discovered_free(). */
int cbm_discover_installed_deps(cbm_pkg_manager_t mgr, const char *project_root,
                                cbm_store_t *store, const char *project_name,
                                cbm_dep_discovered_t **out, int *count,
                                int max_results);
void cbm_dep_discovered_free(cbm_dep_discovered_t *deps, int count);

/* ── Auto-Index (DRY helper for all 3 re-index paths) ──────────── */

/* Detect ecosystem, discover deps from fresh graph, index via flush.
 * Called AFTER dump_to_sqlite by index_repository, watcher, autoindex.
 * Returns number of deps indexed, or 0 if none. */
int cbm_dep_auto_index(const char *project_name, const char *project_root,
                       cbm_store_t *store, int max_deps);

/* ── Cross-Boundary Edges ──────────────────────────────────────── */

/* Create IMPORTS edges from project code to dep modules.
 * Called AFTER all dep flushes complete.
 * Returns number of edges created. */
int cbm_dep_link_cross_edges(cbm_store_t *store, const char *project_name);

#endif /* CBM_DEPINDEX_H */
