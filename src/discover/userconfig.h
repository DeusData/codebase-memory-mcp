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
#include "foundation/sha256.h"

/* ── Types ──────────────────────────────────────────────────────── */

typedef struct {
    char *ext;        /* file extension including dot, e.g. ".blade.php" */
    CBMLanguage lang; /* resolved language enum */
} cbm_userext_t;

typedef struct cbm_userconfig_source cbm_userconfig_source_t;
typedef enum {
    CBM_USERCONFIG_SOURCE_ERROR = 0,
    CBM_USERCONFIG_SOURCE_ABSENT,
    CBM_USERCONFIG_SOURCE_PRESENT
} cbm_userconfig_source_state_t;

typedef struct {
    cbm_userext_t *entries;                  /* heap-allocated array */
    int count;                               /* number of entries */
    cbm_userconfig_source_t *project_source; /* optional owned project snapshot */
    /* Digests of the exact bytes/state consumed by cbm_userconfig_load(). */
    char global_source_sha256[CBM_SHA256_HEX_LEN + 1];
    char project_source_sha256[CBM_SHA256_HEX_LEN + 1];
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

/* Additive indexing loader: retain exactly the project bytes used for extension
 * parsing and project_source_sha256. Global configuration keeps legacy behavior.
 * At most 64 KiB; actual absence is distinct from nonregular/read/close/size errors.
 * NULL means allocation failure; inspect the snapshot state for other failures.
 * Empty or malformed JSON remains PRESENT for downstream strict validation. */
cbm_userconfig_t *cbm_userconfig_load_with_source(const char *repo_path);

/* The PRESENT byte view belongs to cfg and remains valid until free. Outputs
 * are cleared for ABSENT/ERROR. A legacy loader result has no snapshot: ERROR. */
cbm_userconfig_source_state_t cbm_userconfig_project_source(const cbm_userconfig_t *cfg,
                                                            const char **bytes, size_t *len);

/*
 * Look up a file extension in the user config.
 * ext: extension including dot, e.g. ".blade.php"
 * Returns the mapped CBMLanguage, or CBM_LANG_COUNT if not found.
 */
CBMLanguage cbm_userconfig_lookup(const cbm_userconfig_t *cfg, const char *ext);

/* Free a cbm_userconfig_t returned by cbm_userconfig_load. NULL-safe. */
void cbm_userconfig_free(cbm_userconfig_t *cfg);

/* Pure project-only snapshot; never reads global/home/project paths. ABSENT
 * requires NULL/0; PRESENT requires a strict object of 1..65536 bytes. Consumed
 * extra_extensions are strict (duplicates/types/NUL rejected, known languages).
 * Copies bytes/entries into an owned arena; release through cbm_userconfig_free.
 * Clears *out before validation and never exposes a partial config. */
typedef enum {
    CBM_USERCONFIG_SNAPSHOT_OK = 0,
    CBM_USERCONFIG_SNAPSHOT_INVALID,
    CBM_USERCONFIG_SNAPSHOT_UNSUPPORTED,
    CBM_USERCONFIG_SNAPSHOT_LIMIT,
    CBM_USERCONFIG_SNAPSHOT_OOM
} cbm_userconfig_snapshot_status_t;

cbm_userconfig_snapshot_status_t cbm_userconfig_from_project_bytes(
    cbm_userconfig_source_state_t state, const void *bytes, size_t len, cbm_userconfig_t **out);

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
