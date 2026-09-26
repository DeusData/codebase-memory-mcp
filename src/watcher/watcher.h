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

/* Called when file changes are detected. Return 0 on success, a POSITIVE
 * value when the reindex was skipped and should be retried on the next poll
 * (e.g. another pipeline holds the lock), negative on error. Only a 0 return
 * commits the watcher's change baselines — a skipped or failed reindex keeps
 * the change pending so it is retried, never silently lost (#937).
 * project_name: project identifier
 * root_path: absolute path to the repository root */
typedef int (*cbm_index_fn)(const char *project_name, const char *root_path, void *user_data);

/* Optional daemon coordination for destructive stale-root pruning. begin is
 * non-blocking: a false result preserves the watch and retries on a later
 * poll. A successful begin is paired with end. pruned is called after the
 * physical watch and cached DB have been removed so the daemon can invalidate
 * its logical subscriptions. All callbacks use the same borrowed context. */
typedef bool (*cbm_watcher_project_mutation_begin_fn)(void *context, const char *project);
typedef void (*cbm_watcher_project_mutation_end_fn)(void *context, const char *project);
typedef void (*cbm_watcher_project_pruned_fn)(void *context, const char *project);

/* ── Lifecycle ──────────────────────────────────────────────────── */

/* Create a new watcher. store is used for project metadata lookups.
 * index_fn is called when file changes are detected.
 * user_data is passed to index_fn. */
cbm_watcher_t *cbm_watcher_new(cbm_store_t *store, cbm_index_fn index_fn, void *user_data);

/* Free the watcher and all per-project state. NULL-safe.
 * Precondition: cbm_watcher_stop() + thread join must have completed. */
void cbm_watcher_free(cbm_watcher_t *w);

/* Install or clear daemon-owned stale-root coordination. Passing NULL for
 * begin/end clears all callbacks. The setter waits for an in-flight prune
 * callback to finish before returning. */
void cbm_watcher_set_project_mutation_guard(cbm_watcher_t *w,
                                            cbm_watcher_project_mutation_begin_fn begin,
                                            cbm_watcher_project_mutation_end_fn end,
                                            cbm_watcher_project_pruned_fn pruned, void *context);

/* ── Watch list management ──────────────────────────────────────── */

/* Add a project to the watch list. root_path is copied. Returns true only when
 * the physical registration exists (including an identical existing watch).
 * A stopped watcher rejects new registrations. */
bool cbm_watcher_watch(cbm_watcher_t *w, const char *project_name, const char *root_path);

/* Remove a project from the watch list. Any not-yet-admitted callback in the
 * current poll snapshot is invalidated before this function returns. */
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

/* Return a watched project's consecutive hard-index-failure count, or -1 when
 * it is not watched. Exposed so the failure state machine (increment on a
 * hard error, reset on success) can be asserted directly rather than inferred
 * from the poll deadline it feeds.
 *
 * Memory visibility: this reads under projects_lock, but poll_project WRITES
 * the counter outside that lock — it runs against a state snapshot taken
 * while the lock was held, which is the same discipline the other
 * poll-mutated fields here already follow (last_head, last_dirty_sig,
 * interval_ms, next_poll_ns, missing_root_count). It is NOT the discipline of
 * every field: active_git is serialized by projects_lock and registered is an
 * atomic_bool. Reading concurrently with a live poll is therefore formally a
 * data race and may observe a stale value; it is a diagnostic and test
 * accessor, not a synchronisation point.
 * Single-threaded callers (the tests, and any caller between poll cycles)
 * always see the current value. Do not build scheduling decisions on it
 * without first giving the counter atomic accessors. */
int cbm_watcher_index_failure_count(cbm_watcher_t *w, const char *project_name);

/* Return the adaptive poll interval (ms) for a given file count. */
int cbm_watcher_poll_interval_ms(int file_count);

/* Return the delay (ms) before the next index attempt for a project with
 * `consecutive_failures` consecutive hard index failures. Zero failures
 * yields `interval_ms` unchanged; each further failure doubles the delay, so
 * a permanently failing project stops re-forking a worker at the poll cadence
 * without ever being abandoned.
 *
 * Doubling stops at a fixed shift cap, so the delay plateaus at
 * `interval_ms << INDEX_FAIL_SHIFT_MAX` — which is the ceiling only when
 * `interval_ms` is large enough to reach it (>= 4688 ms for the current cap
 * and ceiling). Below that the plateau sits strictly under the ceiling. Every
 * interval this watcher generates is >= POLL_BASE_MS, so in practice the
 * ceiling is always reached; the distinction is stated because this is an
 * exported function and a caller may pass a smaller interval.
 *
 * The result is clamped at BOTH ends: never above the ceiling, and never
 * below `interval_ms`, so backing off can only ever delay the next attempt,
 * never bring it forward. That lower clamp matters only for an `interval_ms`
 * already above the ceiling, which the current constants cannot produce
 * (POLL_MAX_MS < the ceiling) — it is stated because this is an exported
 * function and the guarantee should hold for any argument a caller passes,
 * not only for the ones today's constants generate.
 *
 * Negative inputs are treated as zero. */
int cbm_watcher_index_backoff_ms(int interval_ms, int consecutive_failures);

/* Classify a stat() errno observed on a watched project root: returns true
 * only for values that mean the root itself is gone (ENOENT, ENOTDIR) and
 * may count toward stale-root pruning (#286). Any other failure (EACCES,
 * EIO, transient mounts, macOS TCC revocation) must NOT count — the cached
 * DB holds user-authored data and is unrecoverable once pruned. Exposed
 * for direct unit testing with injected errno values. */
bool cbm_watcher_root_missing_errno(int err);

#endif /* CBM_WATCHER_H */
