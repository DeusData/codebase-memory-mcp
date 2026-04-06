/*
 * discover.h — File discovery, language detection, and gitignore matching.
 *
 * Provides:
 *   - Language detection from filename/extension (CBM_SZ_64 languages)
 *   - .m file disambiguation (Objective-C vs Magma vs MATLAB)
 *   - Gitignore-style pattern parsing and matching
 *   - Recursive directory walk with hardcoded + gitignore filtering
 *
 * Depends on: foundation (platform.h for file ops), cbm.h (CBMLanguage enum)
 */
#ifndef CBM_DISCOVER_H
#define CBM_DISCOVER_H

#include <stdbool.h>
#include <stdint.h>

/* Use the existing CBMLanguage enum from extraction layer */
#include "cbm.h"

/* ── Language detection ──────────────────────────────────────────── */

/* Detect language from a filename (basename only, not full path).
 * Checks special filenames first (Makefile, CMakeLists.txt, etc.),
 * then falls back to extension-based lookup.
 * Returns CBM_LANG_COUNT if unknown. */
CBMLanguage cbm_language_for_filename(const char *filename);

/* Detect language from a file extension (including the dot, e.g. ".go").
 * Returns CBM_LANG_COUNT if unknown. */
CBMLanguage cbm_language_for_extension(const char *ext);

/* Get the human-readable name for a language enum value.
 * Returns "Unknown" for CBM_LANG_COUNT or out-of-range values. */
const char *cbm_language_name(CBMLanguage lang);

/* Disambiguate .m files by reading first 4KB of content.
 * Returns CBM_LANG_OBJC, CBM_LANG_MAGMA, or CBM_LANG_MATLAB.
 * On read failure, defaults to CBM_LANG_MATLAB. */
CBMLanguage cbm_disambiguate_m(const char *path);

/* ── Gitignore pattern matching ──────────────────────────────────── */

typedef struct cbm_gitignore cbm_gitignore_t;

/* Parse gitignore patterns from a file. Returns NULL on error (file not found, etc.).
 * Caller must call cbm_gitignore_free(). */
cbm_gitignore_t *cbm_gitignore_load(const char *path);

/* Parse gitignore patterns from a string (for testing).
 * Caller must call cbm_gitignore_free(). */
cbm_gitignore_t *cbm_gitignore_parse(const char *content);

/* Check if a relative path matches any gitignore pattern.
 * rel_path should use '/' separators. is_dir indicates if path is a directory. */
bool cbm_gitignore_matches(const cbm_gitignore_t *gi, const char *rel_path, bool is_dir);

/* Free a gitignore matcher. NULL-safe. */
void cbm_gitignore_free(cbm_gitignore_t *gi);

/* ── Directory skip / suffix filters ─────────────────────────────── */

/* Index mode controls filtering aggressiveness.
 * IMPORTANT: these values MUST match pipeline.h exactly.  A previous
 * mismatch (this header had FAST=1, pipeline.h has FAST=2) caused
 * fast-mode filtering to silently no-op depending on include order —
 * the pipeline passed value 2, discover.c compared against 1, and no
 * files got filtered. */
#ifndef CBM_INDEX_MODE_T_DEFINED
#define CBM_INDEX_MODE_T_DEFINED
typedef enum {
    CBM_MODE_FULL = 0,     /* parse everything supported */
    CBM_MODE_MODERATE = 1, /* aggressive filtering + similarity/semantic edges */
    CBM_MODE_FAST = 2,     /* aggressive filtering + no similarity/semantic edges */
} cbm_index_mode_t;
#endif

/* Check if a directory name should always be skipped (e.g. .git, node_modules).
 * Only checks the basename, not the full path. */
bool cbm_should_skip_dir(const char *dirname, cbm_index_mode_t mode);

/* Check if a file has a suffix that should be skipped (e.g. .pyc, .png). */
bool cbm_has_ignored_suffix(const char *filename, cbm_index_mode_t mode);

/* Check if a specific filename should be skipped in fast mode (e.g. LICENSE, go.sum). */
bool cbm_should_skip_filename(const char *filename, cbm_index_mode_t mode);

/* Check if a path matches fast-mode substring patterns (e.g. .d.ts, .pb.go). */
bool cbm_matches_fast_pattern(const char *filename, cbm_index_mode_t mode);

/* ── File discovery ──────────────────────────────────────────────── */

typedef struct {
    char *path;           /* absolute path (heap-allocated) */
    char *rel_path;       /* relative to repo root (heap-allocated) */
    CBMLanguage language; /* detected language */
    int64_t size;         /* file size in bytes */
} cbm_file_info_t;

typedef struct {
    cbm_index_mode_t mode;   /* CBM_MODE_FULL or CBM_MODE_FAST */
    const char *ignore_file; /* path to .cbmignore file, or NULL */
    int64_t max_file_size;   /* 0 = no limit */
} cbm_discover_opts_t;

/* Walk a repository directory tree and discover all source files.
 * Applies hardcoded filters, gitignore patterns, and language detection.
 * Returns 0 on success, -1 on error.
 * Caller must call cbm_discover_free() on the results. */
int cbm_discover(const char *repo_path, const cbm_discover_opts_t *opts, cbm_file_info_t **out,
                 int *count);

/* Free an array of file info results. NULL-safe. */
void cbm_discover_free(cbm_file_info_t *files, int count);

#endif /* CBM_DISCOVER_H */
