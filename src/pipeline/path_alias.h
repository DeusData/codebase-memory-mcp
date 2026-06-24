#ifndef CBM_PATH_ALIAS_H
#define CBM_PATH_ALIAS_H

/*
 * path_alias.h — Generic build-tool path alias resolution.
 *
 * Resolves module paths that the indexer would otherwise leave as bare
 * imports (e.g. "@/lib/auth" or "~/components/Button") into repo-relative
 * paths the existing module-FQN logic can consume. The alias source is
 * pluggable per language: the loader walks the repo, recognises supported
 * config files, and adds each one's aliases to a single collection scoped
 * by directory.
 *
 * Currently implemented sources: TypeScript / JavaScript (tsconfig.json
 * and jsconfig.json compilerOptions.paths + baseUrl). The data model and
 * resolver below are deliberately language-agnostic so further loaders
 * (Vite/Webpack alias config, Python tool.* hints, ...) can register
 * additional scopes without touching the resolver or the pipeline.
 *
 * Lookup is two-step:
 *   1. cbm_path_alias_find_for_file picks the nearest ancestor scope for
 *      a given source file (longest matching dir_prefix wins).
 *   2. cbm_path_alias_resolve runs the chosen scope's alias map against
 *      the import path, returning a heap-allocated repo-relative target
 *      (or NULL if no alias matches).
 */

#include <stdbool.h>
#include <stddef.h>

/* Single alias entry: prefix-pattern → target-pattern, optionally with a
 * single '*' wildcard. The pattern is split at the wildcard so resolution
 * can match the prefix/suffix and slot the wildcard portion into the
 * target. Patterns without '*' are exact-match. */
typedef struct {
    char *alias_prefix;  /* portion before '*' in the key   (e.g. "@/")    */
    char *alias_suffix;  /* portion after  '*' in the key   (usually "")   */
    char *target_prefix; /* portion before '*' in the value (e.g. "src/")  */
    char *target_suffix; /* portion after  '*' in the value (usually "")   */
    bool has_wildcard;   /* true when the alias pattern contained '*'      */
} cbm_path_alias_t;

/* Alias map for a single config source (one tsconfig.json, one webpack
 * alias block, ...). Entries are sorted by alias_prefix length descending
 * so longer (more specific) prefixes are matched first, matching the
 * resolution semantics of every build tool we know of. */
typedef struct {
    cbm_path_alias_t *entries;
    int count;
    char *base_url; /* compilerOptions.baseUrl or equivalent; NULL if unset */
} cbm_path_alias_map_t;

/* A directory-scoped alias map. dir_prefix is the path of the config file
 * relative to the repo root ("" for a root-level config). Source files
 * pick up the map of the nearest ancestor scope. */
typedef struct {
    char *dir_prefix;
    cbm_path_alias_map_t *map;
} cbm_path_alias_scope_t;

/* Aggregated alias scopes for a whole repository. Scopes are sorted by
 * dir_prefix length descending so the nearest-ancestor lookup is a single
 * linear scan. */
typedef struct {
    cbm_path_alias_scope_t *scopes;
    int count;
} cbm_path_alias_collection_t;

/* Walk repo_path for known build-config files and build a collection of
 * scoped alias maps. Currently picks up tsconfig.json and jsconfig.json;
 * additional loaders register here. Returns NULL when no usable configs
 * are found (also NULL on out-of-memory). Caller frees with
 * cbm_path_alias_collection_free. */
cbm_path_alias_collection_t *cbm_load_path_aliases(const char *repo_path);

/* Free a collection produced by cbm_load_path_aliases. NULL-safe. */
void cbm_path_alias_collection_free(cbm_path_alias_collection_t *coll);

/* Pick the nearest ancestor scope for a file path that is relative to the
 * repo root. Returns the scope's alias map, or NULL if no scope applies.
 * The returned pointer is owned by the collection. */
const cbm_path_alias_map_t *cbm_path_alias_find_for_file(const cbm_path_alias_collection_t *coll,
                                                         const char *rel_path);

/* Resolve module_path against map. Returns a heap-allocated repo-relative
 * path (caller frees), or NULL when no alias entry matches and no
 * baseUrl fallback applies. Common JS/TS extensions (.ts, .tsx, .js,
 * .jsx) are stripped from the resolved path so the existing module-FQN
 * pipeline can consume it. */
char *cbm_path_alias_resolve(const cbm_path_alias_map_t *map, const char *module_path);

#endif /* CBM_PATH_ALIAS_H */
