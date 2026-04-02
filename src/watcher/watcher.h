/*
 * watcher.h — File change watcher for auto-reindexing.
 *
 * Polls indexed projects for git changes (HEAD movement or dirty working tree)
 * and triggers re-indexing via a callback. Uses adaptive polling intervals
 * based on project size (5s base + 1s per 500 files, capped at 60s).
 *
 * Depends on: foundation, store (for project metadata)
 */
#ifndef CBM_WATCHER_H
#define CBM_WATCHER_H

#include <stdbool.h>
#include <stdint.h>

/* Forward declarations */
typedef struct cbm_store cbm_store_t;

/* ── Opaque handle ──────────────────────────────────────────────── */

typedef struct cbm_watcher cbm_watcher_t;

/* ── Index callback ─────────────────────────────────────────────── */

/* Called when file changes are detected. Return 0 on success, -1 on error.
 * project_name: project identifier
 * root_path: absolute path to the repository root */
typedef int (*cbm_index_fn)(const char *project_name, const char *root_path, void *user_data);

/* ── Lifecycle ──────────────────────────────────────────────────── */

/* Create a new watcher. store is used for project metadata lookups.
 * index_fn is called when file changes are detected.
 * user_data is passed to index_fn. */
cbm_watcher_t *cbm_watcher_new(cbm_store_t *store, cbm_index_fn index_fn, void *user_data);

/* Free the watcher and all per-project state. NULL-safe. */
void cbm_watcher_free(cbm_watcher_t *w);

/* ── Watch list management ──────────────────────────────────────── */

/* Add a project to the watch list. root_path is copied. */
void cbm_watcher_watch(cbm_watcher_t *w, const char *project_name, const char *root_path);

/* Remove a project from the watch list. */
void cbm_watcher_unwatch(cbm_watcher_t *w, const char *project_name);

/* Refresh a project's timestamp (resets adaptive backoff). */
void cbm_watcher_touch(cbm_watcher_t *w, const char *project_name);

/* ── Polling ────────────────────────────────────────────────────── */

/* Run a single poll cycle — check each watched project for changes.
 * Returns the number of projects that were reindexed. */
int cbm_watcher_poll_once(cbm_watcher_t *w);

/* Run the blocking poll loop. Polls every base_interval_ms until
 * cbm_watcher_stop() is called. Returns 0 on clean shutdown. */
int cbm_watcher_run(cbm_watcher_t *w, int base_interval_ms);

/* Request the run loop to stop (thread-safe). */
void cbm_watcher_stop(cbm_watcher_t *w);

/* ── Introspection (for testing) ────────────────────────────────── */

/* Return the number of projects in the watch list. */
int cbm_watcher_watch_count(cbm_watcher_t *w);

/* Return the adaptive poll interval (ms) for a given file count. */
int cbm_watcher_poll_interval_ms(int file_count);

#endif /* CBM_WATCHER_H */
