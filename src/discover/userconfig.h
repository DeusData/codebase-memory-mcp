/*
 * userconfig.h — User-defined file extension → language mappings.
 *
 * Reads extra_extensions from two optional JSON config files:
 *   Global:  $XDG_CONFIG_HOME/codebase-memory-mcp/config.json
 *            (falls back to ~/.config/codebase-memory-mcp/config.json)
 *   Project: {repo_root}/.codebase-memory.json
 *
 * Project config wins over global. Unknown language values warn and are
 * skipped (fail-open). Missing files are silently ignored.
 *
 * Format:
 *   {"extra_extensions": {".blade.php": "php", ".mjs": "javascript"}}
 *
 * The language string matching is case-insensitive.
 */
#ifndef CBM_USERCONFIG_H
#define CBM_USERCONFIG_H

#include "cbm.h" /* CBMLanguage */

/* ── Types ──────────────────────────────────────────────────────── */

typedef struct {
    char *ext;        /* file extension including dot, e.g. ".blade.php" */
    CBMLanguage lang; /* resolved language enum */
} cbm_userext_t;

typedef struct {
    cbm_userext_t *entries; /* heap-allocated array */
    int count;              /* number of entries */
} cbm_userconfig_t;

/* ── API ────────────────────────────────────────────────────────── */

/*
 * Load user config from global + project files, merge (project wins).
 * repo_path: absolute path to the repository root (for project config).
 * Returns a heap-allocated cbm_userconfig_t (caller must free via
 * cbm_userconfig_free). Returns NULL only on allocation failure.
 * Missing config files are silently ignored.
 */
cbm_userconfig_t *cbm_userconfig_load(const char *repo_path);

/*
 * Look up a file extension in the user config.
 * ext: extension including dot, e.g. ".blade.php"
 * Returns the mapped CBMLanguage, or CBM_LANG_COUNT if not found.
 */
CBMLanguage cbm_userconfig_lookup(const cbm_userconfig_t *cfg, const char *ext);

/* Free a cbm_userconfig_t returned by cbm_userconfig_load. NULL-safe. */
void cbm_userconfig_free(cbm_userconfig_t *cfg);

/* ── Integration hook ───────────────────────────────────────────── */

/*
 * Set the process-global user config that cbm_language_for_extension()
 * will consult before the built-in table.
 * cfg may be NULL to clear the override.
 * Not thread-safe — call before spawning worker threads.
 */
void cbm_set_user_lang_config(const cbm_userconfig_t *cfg);

/*
 * Get the currently active process-global user config.
 * Returns NULL if none has been set.
 * Called internally by cbm_language_for_extension().
 */
const cbm_userconfig_t *cbm_get_user_lang_config(void);

#endif /* CBM_USERCONFIG_H */
