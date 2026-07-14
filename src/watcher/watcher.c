/*
 * watcher.c — Git-based file change watcher.
 *
 * Strategy: git status + HEAD tracking (the most reliable approach).
 * For non-git projects, the watcher skips polling (no fsnotify/dirmtime yet).
 *
 *
 * Per-project state tracks:
 *   - Last git HEAD hash (detects commits, checkout, pull)
 *   - Dirty-state signature (#937): porcelain entries plus file metadata, so
 *     a persistently dirty tree reindexes once per distinct state
 *   - Last poll time + adaptive interval
 *   - Whether the project is a git repo
 *
 * Baselines are committed only after a successful reindex. Busy or failed
 * runs retain the old baseline so the same change is retried.
 *
 * Adaptive interval: 5s base + 1s per 500 files, capped at 60s.
 * Matches the Go watcher's `pollInterval()` logic.
 */
#include <stdint.h>
#include "watcher/watcher.h"
#include "git/git_command.h"
#include "git/git_snapshot.h"
#include "store/store.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <sys/stat.h>

/* ── Constants ─────────────────────────────────────────────────── */

/* Adaptive poll interval parameters (ms) */
#define POLL_BASE_MS 5000
#define POLL_FILE_STEP 500 /* add 1s per this many files */
#define POLL_MAX_MS 60000

/* Stale-root pruning (#286) requires both repeated missing observations and
 * a grace period because deleting the cache can also delete user-authored ADRs. */
#define MISSING_ROOT_DELETE_AFTER 3
#define PRUNE_GRACE_DEFAULT_S 600

/* Sleep chunk for responsive shutdown (ms) */
#define SLEEP_CHUNK_MS 500

/* ── Per-project state ──────────────────────────────────────────── */

typedef struct {
    char *project_name;
    char *root_path;
    char last_head[CBM_SZ_64]; /* git HEAD hash */
    char last_dirty_hash[CBM_GIT_DIRTY_HASH_BUFSZ]; /* git status hash */
    bool is_git;               /* false → skip polling */
    bool baseline_done;        /* true after first poll */
    int file_count;            /* approximate, for interval calc */
    int interval_ms;           /* adaptive poll interval */
    int64_t next_poll_ns;      /* next poll time (monotonic ns) */
    int missing_root_count;
    uint64_t first_missing_ms;
    uint64_t version;          /* increments when external calls replace or refresh state */
} project_state_t;

typedef struct {
    char *project_name;
    char *root_path;
    char last_head[CBM_SZ_64];
    char last_dirty_hash[CBM_GIT_DIRTY_HASH_BUFSZ];
    char pending_head[CBM_SZ_64];
    char pending_dirty_hash[CBM_GIT_DIRTY_HASH_BUFSZ];
    bool is_git;
    bool baseline_done;
    int file_count;
    int interval_ms;
    int64_t next_poll_ns;
    int missing_root_count;
    uint64_t first_missing_ms;
    int64_t observed_next_poll_ns;
    uint64_t observed_version;
} project_snapshot_t;

/* ── Watcher struct ─────────────────────────────────────────────── */

struct cbm_watcher {
    cbm_store_t *store;
    cbm_index_fn index_fn;
    void *user_data;
    CBMHashTable *projects; /* name → project_state_t* */
    cbm_mutex_t projects_lock;
    uint64_t next_version;
    atomic_int stopped;
    int poll_base_ms;  /* 0 = use POLL_BASE_MS default */
    int poll_max_ms;   /* 0 = use POLL_MAX_MS default */
};

/* ── Time helper ────────────────────────────────────────────────── */

static int64_t now_ns(void) {
    struct timespec ts;
    cbm_clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((int64_t)ts.tv_sec * (int64_t)CBM_NSEC_PER_SEC) + ts.tv_nsec;
}

/* ── Adaptive interval ──────────────────────────────────────────── */

int cbm_watcher_poll_interval_ms(int file_count, int base_ms, int max_ms) {
    if (base_ms <= 0) base_ms = POLL_BASE_MS;
    if (max_ms  <= 0) max_ms  = POLL_MAX_MS;
    int ms = base_ms + ((file_count / POLL_FILE_STEP) * (int)CBM_MSEC_PER_SEC);
    if (ms > max_ms) {
        ms = max_ms;
    }
    return ms;
}

/* ── Git helpers ────────────────────────────────────────────────── */

static bool watcher_git_path_supported(const char *root_path) {
    return cbm_git_snapshot_path_supported(root_path);
}

static bool watcher_validate_path(const char *event, const char *project_name, const char *root_path) {
    if (!cbm_git_validate_repo_path(root_path)) {
        cbm_log_warn(event, "project", project_name, "reason",
                     "path contains shell metacharacters");
        return false;
    }
    if (!watcher_git_path_supported(root_path)) {
        cbm_log_warn(event, "project", project_name, "reason", "path too long for git command");
        return false;
    }
    return true;
}

/* ── Project state lifecycle ────────────────────────────────────── */

static project_state_t *state_new(const char *name, const char *root_path) {
    project_state_t *s = calloc(CBM_ALLOC_ONE, sizeof(*s));
    if (!s) {
        return NULL;
    }
    s->project_name = cbm_strdup(name);
    s->root_path = cbm_strdup(root_path);
    if (!s->project_name || !s->root_path) {
        free(s->project_name);
        free(s->root_path);
        free(s);
        return NULL;
    }
    s->interval_ms = POLL_BASE_MS;
    return s;
}

static void state_free(project_state_t *s) {
    if (!s) {
        return;
    }
    free(s->project_name);
    free(s->root_path);
    free(s);
}

static bool snapshot_from_state(project_snapshot_t *dst, const project_state_t *src) {
    if (!dst || !src) {
        return false;
    }
    memset(dst, 0, sizeof(*dst));
    dst->project_name = cbm_strdup(src->project_name);
    dst->root_path = cbm_strdup(src->root_path);
    if (!dst->project_name || !dst->root_path) {
        free(dst->project_name);
        free(dst->root_path);
        memset(dst, 0, sizeof(*dst));
        return false;
    }
    memcpy(dst->last_head, src->last_head, sizeof(dst->last_head));
    memcpy(dst->last_dirty_hash, src->last_dirty_hash, sizeof(dst->last_dirty_hash));
    dst->is_git = src->is_git;
    dst->baseline_done = src->baseline_done;
    dst->file_count = src->file_count;
    dst->interval_ms = src->interval_ms;
    dst->next_poll_ns = src->next_poll_ns;
    dst->missing_root_count = src->missing_root_count;
    dst->first_missing_ms = src->first_missing_ms;
    dst->observed_next_poll_ns = src->next_poll_ns;
    dst->observed_version = src->version;
    return true;
}

static bool snapshot_from_project(project_snapshot_t *dst, const char *project_name,
                                  const char *root_path) {
    if (!dst || !project_name || !root_path) {
        return false;
    }
    memset(dst, 0, sizeof(*dst));
    dst->project_name = cbm_strdup(project_name);
    dst->root_path = cbm_strdup(root_path);
    if (!dst->project_name || !dst->root_path) {
        free(dst->project_name);
        free(dst->root_path);
        memset(dst, 0, sizeof(*dst));
        return false;
    }
    dst->interval_ms = POLL_BASE_MS;
    return true;
}

static void snapshot_free(project_snapshot_t *s) {
    if (!s) {
        return;
    }
    free(s->project_name);
    free(s->root_path);
    memset(s, 0, sizeof(*s));
}

static void state_apply_snapshot(project_state_t *dst, const project_snapshot_t *src) {
    bool touched_during_poll =
        src->observed_next_poll_ns != 0 && dst->next_poll_ns == 0 && src->next_poll_ns != 0;
    memcpy(dst->last_head, src->last_head, sizeof(dst->last_head));
    memcpy(dst->last_dirty_hash, src->last_dirty_hash, sizeof(dst->last_dirty_hash));
    dst->is_git = src->is_git;
    dst->baseline_done = src->baseline_done;
    dst->file_count = src->file_count;
    dst->interval_ms = src->interval_ms;
    dst->next_poll_ns = touched_during_poll ? 0 : src->next_poll_ns;
    dst->missing_root_count = src->missing_root_count;
    dst->first_missing_ms = src->first_missing_ms;
}

bool cbm_watcher_root_missing_errno(int err) {
    /* Permission, I/O, symlink, and transient mount failures are uncertainty,
     * never evidence that author-owned data may be deleted (#286). */
    return err == ENOENT || err == ENOTDIR;
}

typedef enum {
    ROOT_PRESENT = 0,
    ROOT_MISSING,
    ROOT_UNCERTAIN,
} root_status_t;

static root_status_t watcher_root_status(const char *root_path, int *out_errno) {
    *out_errno = 0;
    if (!root_path) {
        return ROOT_UNCERTAIN;
    }
    struct stat st;
    if (stat(root_path, &st) == 0) {
        return S_ISDIR(st.st_mode) ? ROOT_PRESENT : ROOT_MISSING;
    }
    *out_errno = errno;
    return cbm_watcher_root_missing_errno(errno) ? ROOT_MISSING : ROOT_UNCERTAIN;
}

static long watcher_prune_grace_s(void) {
    const char *raw = getenv("CBM_WATCHER_PRUNE_GRACE_S");
    if (raw && raw[0]) {
        errno = 0;
        char *end = NULL;
        long value = strtol(raw, &end, 10);
        if (errno == 0 && end != raw && *end == '\0' && value >= 0) {
            return value;
        }
    }
    return PRUNE_GRACE_DEFAULT_S;
}

static void watcher_delete_cached_project_db(const char *project_name) {
    if (!cbm_validate_project_name(project_name)) {
        return;
    }
    const char *cache_dir = cbm_resolve_cache_dir();
    if (!cache_dir) {
        return;
    }
    char path[CBM_SZ_1K];
    char wal[CBM_SZ_1K];
    char shm[CBM_SZ_1K];
    snprintf(path, sizeof(path), "%s/%s.db", cache_dir, project_name);
    snprintf(wal, sizeof(wal), "%s-wal", path);
    snprintf(shm, sizeof(shm), "%s-shm", path);
    (void)cbm_unlink(path);
    (void)cbm_unlink(wal);
    (void)cbm_unlink(shm);
}

/* Hash table foreach callback to free state entries */
static void free_state_entry(const char *key, void *val, void *ud) {
    (void)key;
    (void)ud;
    state_free(val);
}

/* ── Watcher lifecycle ──────────────────────────────────────────── */

cbm_watcher_t *cbm_watcher_new(cbm_store_t *store, cbm_index_fn index_fn, void *user_data) {
    cbm_watcher_t *w = calloc(CBM_ALLOC_ONE, sizeof(*w));
    if (!w) {
        return NULL;
    }
    w->store = store;
    w->index_fn = index_fn;
    w->user_data = user_data;
    w->next_version = 1;
    w->projects = cbm_ht_create(CBM_SZ_32);
    if (!w->projects) {
        free(w);
        return NULL;
    }
    cbm_mutex_init(&w->projects_lock);
    atomic_init(&w->stopped, 0);
    return w;
}

void cbm_watcher_free(cbm_watcher_t *w) {
    if (!w) {
        return;
    }
    cbm_mutex_lock(&w->projects_lock);
    cbm_ht_foreach(w->projects, free_state_entry, NULL);
    cbm_ht_free(w->projects);
    cbm_mutex_unlock(&w->projects_lock);
    cbm_mutex_destroy(&w->projects_lock);
    free(w);
}

/* ── Watch list management ──────────────────────────────────────── */

static void init_baseline(project_snapshot_t *s, const cbm_watcher_t *w,
                          bool indexed_baseline);

void cbm_watcher_watch(cbm_watcher_t *w, const char *project_name, const char *root_path) {
    if (!w || !project_name || !root_path) {
        return;
    }

    /* Reject paths with shell metacharacters: all git helpers use cbm_popen(). */
    if (!watcher_validate_path("watcher.watch.reject", project_name, root_path)) {
        return;
    }

    cbm_mutex_lock(&w->projects_lock);

    /* If already watching this project at the same path, preserve existing state.
     * This prevents unnecessary baseline resets when resolve_store() calls us on
     * every project switch — which would discard the accumulated HEAD/dirty hashes
     * and trigger redundant git commands on the next poll cycle. */
    project_state_t *existing = cbm_ht_get(w->projects, project_name);
    if (existing && strcmp(existing->root_path, root_path) == 0) {
        cbm_mutex_unlock(&w->projects_lock);
        return; /* idempotent — state preserved */
    }

    /* Path changed or first watch: replace old entry */
    if (existing) {
        cbm_ht_delete(w->projects, project_name);
        state_free(existing);
    }

    project_state_t *s = state_new(project_name, root_path);
    if (!s) {
        cbm_mutex_unlock(&w->projects_lock);
        cbm_log_warn("watcher.watch.oom", "project", project_name, "path", root_path);
        return;
    }
    s->version = w->next_version++;
    cbm_ht_set(w->projects, s->project_name, s);
    cbm_mutex_unlock(&w->projects_lock);
    cbm_log_info("watcher.watch", "project", project_name, "path", root_path);
}

void cbm_watcher_mark_indexed(cbm_watcher_t *w, const char *project_name, const char *root_path) {
    if (!w || !project_name || !root_path) {
        return;
    }
    if (!watcher_validate_path("watcher.indexed.reject", project_name, root_path)) {
        return;
    }

    project_snapshot_t snap = {0};
    if (!snapshot_from_project(&snap, project_name, root_path)) {
        cbm_log_warn("watcher.indexed.oom", "project", project_name, "path", root_path);
        return;
    }
    init_baseline(&snap, w, true);

    cbm_mutex_lock(&w->projects_lock);
    project_state_t *cur = cbm_ht_get(w->projects, project_name);
    if (cur && strcmp(cur->root_path, root_path) != 0) {
        cbm_ht_delete(w->projects, project_name);
        state_free(cur);
        cur = NULL;
    }
    if (!cur) {
        cur = state_new(project_name, root_path);
        if (!cur) {
            cbm_mutex_unlock(&w->projects_lock);
            snapshot_free(&snap);
            cbm_log_warn("watcher.indexed.oom", "project", project_name, "path", root_path);
            return;
        }
        cbm_ht_set(w->projects, cur->project_name, cur);
    }
    state_apply_snapshot(cur, &snap);
    cur->version = w->next_version++;
    cbm_mutex_unlock(&w->projects_lock);

    cbm_log_info("watcher.indexed", "project", project_name, "path", root_path);
    snapshot_free(&snap);
}

void cbm_watcher_unwatch(cbm_watcher_t *w, const char *project_name) {
    if (!w || !project_name) {
        return;
    }
    bool removed = false;
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *s = cbm_ht_get(w->projects, project_name);
    if (s) {
        cbm_ht_delete(w->projects, project_name);
        state_free(s);
        removed = true;
    }
    cbm_mutex_unlock(&w->projects_lock);
    if (removed) {
        cbm_log_info("watcher.unwatch", "project", project_name);
    }
}

void cbm_watcher_touch(cbm_watcher_t *w, const char *project_name) {
    if (!w || !project_name) {
        return;
    }
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *s = cbm_ht_get(w->projects, project_name);
    if (s) {
        /* Reset backoff — poll immediately on next cycle */
        s->next_poll_ns = 0;
        s->version = w->next_version++;
    }
    cbm_mutex_unlock(&w->projects_lock);
}

int cbm_watcher_watch_count(cbm_watcher_t *w) {
    if (!w) {
        return 0;
    }
    cbm_mutex_lock(&w->projects_lock);
    int count = (int)cbm_ht_count(w->projects);
    cbm_mutex_unlock(&w->projects_lock);
    return count;
}

/* ── Single poll cycle ──────────────────────────────────────────── */

/* Init baseline for a project: check if git, get HEAD, count files */
static void init_baseline(project_snapshot_t *s, const cbm_watcher_t *w,
                          bool indexed_baseline) {
    if (!cbm_file_exists(s->root_path)) {
        cbm_log_warn("watcher.root_gone", "project", s->project_name, "path", s->root_path);
        s->baseline_done = true;
        s->is_git = false;
        return;
    }

    cbm_git_snapshot_t snap = {0};
    if (cbm_git_snapshot_read(s->root_path,
                              CBM_GIT_SNAPSHOT_HEAD | CBM_GIT_SNAPSHOT_DIRTY |
                                  CBM_GIT_SNAPSHOT_FILE_COUNT,
                              &snap) != 0) {
        s->baseline_done = true;
        s->is_git = false;
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "none");
        s->next_poll_ns = now_ns() + ((int64_t)s->interval_ms * (int64_t)CBM_NSEC_PER_MSEC);
        return;
    }
    s->is_git = snap.is_git;
    s->baseline_done = true;

    if (s->is_git) {
        memcpy(s->last_head, snap.head, sizeof(s->last_head));
        if (indexed_baseline || snap.dirty_bytes <= 0) {
            memcpy(s->last_dirty_hash, snap.dirty_hash, sizeof(s->last_dirty_hash));
        } else {
            /* A pre-existing dirty tree may not be represented in a restored
             * artifact. Reindex it once, then #937 suppresses idle repeats. */
            s->last_dirty_hash[0] = '\0';
        }
        s->file_count = snap.file_count;
        s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count, w->poll_base_ms, w->poll_max_ms);
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "git", "files",
                     s->file_count > 0 ? "yes" : "0");
    } else {
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "none");
    }

    s->next_poll_ns = now_ns() + ((int64_t)s->interval_ms * (int64_t)CBM_NSEC_PER_MSEC);
}

/* Stage the observed state without changing the committed baseline (#937).
 * The caller commits pending_* only after index_fn succeeds. */
static bool check_changes(project_snapshot_t *s) {
    if (!s->is_git) {
        return false;
    }

    cbm_git_snapshot_t snap = {0};
    if (cbm_git_snapshot_read(s->root_path, CBM_GIT_SNAPSHOT_HEAD | CBM_GIT_SNAPSHOT_DIRTY,
                              &snap) != 0 || !snap.is_git) {
        return false;
    }

    memcpy(s->pending_head, snap.head, sizeof(s->pending_head));
    memcpy(s->pending_dirty_hash, snap.dirty_hash, sizeof(s->pending_dirty_hash));
    bool head_changed = snap.head[0] != '\0' && s->last_head[0] != '\0' &&
                        strcmp(snap.head, s->last_head) != 0;
    bool dirty_changed = strcmp(snap.dirty_hash, s->last_dirty_hash) != 0;
    return head_changed || dirty_changed;
}

static bool watcher_snapshot_current(cbm_watcher_t *w, const project_snapshot_t *snap) {
    bool current = false;
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *cur = cbm_ht_get(w->projects, snap->project_name);
    if (cur && strcmp(cur->root_path, snap->root_path) == 0 &&
        cur->version == snap->observed_version) {
        current = true;
    }
    cbm_mutex_unlock(&w->projects_lock);
    return current;
}

static bool watcher_store_has_project(cbm_store_t *store, const char *project_name) {
    if (!store || !project_name) {
        return false;
    }
    cbm_project_t project = {0};
    int rc = cbm_store_get_project(store, project_name, &project);
    if (rc == CBM_STORE_OK) {
        cbm_project_free_fields(&project);
        return true;
    }
    if (rc != CBM_STORE_NOT_FOUND) {
        cbm_log_warn("watcher.dirty_ledger.warn", "project", project_name, "phase",
                     "get_project");
    }
    return false;
}

static void watcher_mark_dirty_paths(cbm_watcher_t *w, const project_snapshot_t *s) {
    if (!w || !w->store || !s || !s->project_name || !s->root_path) {
        return;
    }
    if (!watcher_store_has_project(w->store, s->project_name)) {
        return;
    }
    char **paths = NULL;
    int path_count = 0;
    if (cbm_git_status_paths(s->root_path, &paths, &path_count) != 0) {
        cbm_log_warn("watcher.dirty_ledger.warn", "project", s->project_name, "phase",
                     "git_status_paths");
        return;
    }
    for (int i = 0; i < path_count; i++) {
        cbm_dirty_file_state_t dirty = {
            .project = s->project_name,
            .rel_path = paths[i],
            .observed_hash = s->last_dirty_hash,
            .observed_generation = 0,
            .source = CBM_STORE_DIRTY_SOURCE_WATCHER,
            .status = CBM_STORE_DIRTY_STATUS_PENDING,
        };
        if (cbm_store_upsert_dirty_file(w->store, &dirty) != CBM_STORE_OK) {
            cbm_log_warn("watcher.dirty_ledger.warn", "project", s->project_name, "phase",
                         "upsert_dirty_file");
        }
    }
    cbm_git_status_paths_free(paths, path_count);
}

/* Context for poll_once foreach callback */
typedef struct {
    cbm_watcher_t *w;
    int64_t now;
    int reindexed;
} poll_ctx_t;

static void prune_missing_project(cbm_watcher_t *w, const project_snapshot_t *snapshot) {
    bool removed = false;
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *current = cbm_ht_get(w->projects, snapshot->project_name);
    if (current && current->version == snapshot->observed_version &&
        strcmp(current->root_path, snapshot->root_path) == 0) {
        cbm_ht_delete(w->projects, snapshot->project_name);
        state_free(current);
        removed = true;
    }
    cbm_mutex_unlock(&w->projects_lock);
    if (removed) {
        watcher_delete_cached_project_db(snapshot->project_name);
        cbm_log_info("watcher.root_pruned", "project", snapshot->project_name);
    }
}

static void poll_project(const char *key, void *val, void *ud) {
    (void)key;
    poll_ctx_t *ctx = ud;
    project_snapshot_t *s = val;
    if (!s) {
        return;
    }

    /* Check roots before Git/baseline/backoff gates so non-Git projects are
     * also pruned after sustained, unambiguous absence (#286). */
    int stat_errno = 0;
    root_status_t root = watcher_root_status(s->root_path, &stat_errno);
    if (root == ROOT_UNCERTAIN) {
        s->missing_root_count = 0;
        s->first_missing_ms = 0;
        cbm_log_warn("watcher.root_stat_error", "project", s->project_name);
        return;
    }
    if (root == ROOT_MISSING) {
        uint64_t now_ms = cbm_now_ms();
        if (s->missing_root_count == 0) {
            s->first_missing_ms = now_ms;
        }
        s->missing_root_count++;
        if (s->missing_root_count >= MISSING_ROOT_DELETE_AFTER &&
            now_ms - s->first_missing_ms >=
                (uint64_t)watcher_prune_grace_s() * (uint64_t)CBM_MSEC_PER_SEC) {
            prune_missing_project(ctx->w, s);
        }
        return;
    }
    if (s->missing_root_count > 0) {
        cbm_log_info("watcher.root_restored", "project", s->project_name);
        s->missing_root_count = 0;
        s->first_missing_ms = 0;
    }

    /* Initialize baseline on first poll */
    if (!s->baseline_done) {
        init_baseline(s, ctx->w, false);
        return;
    }

    /* Skip non-git projects */
    if (!s->is_git) {
        return;
    }

    /* Respect adaptive interval */
    if (ctx->now < s->next_poll_ns) {
        return;
    }

    /* Check for changes */
    bool changed = check_changes(s);
    if (!changed) {
        s->next_poll_ns = ctx->now + ((int64_t)s->interval_ms * (int64_t)CBM_NSEC_PER_MSEC);
        return;
    }

    /* Trigger reindex */
    if (!watcher_snapshot_current(ctx->w, s)) {
        return;
    }
    cbm_log_info("watcher.changed", "project", s->project_name, "strategy", "git");
    watcher_mark_dirty_paths(ctx->w, s);
    if (ctx->w->index_fn) {
        int rc = ctx->w->index_fn(s->project_name, s->root_path, ctx->w->user_data);
        if (rc == 0) {
            ctx->reindexed++;
            /* Commit the state that was actually indexed. A later edit during
             * index_fn remains different and is retried next poll (#937). */
            memcpy(s->last_head, s->pending_head, sizeof(s->last_head));
            memcpy(s->last_dirty_hash, s->pending_dirty_hash,
                   sizeof(s->last_dirty_hash));
            cbm_git_snapshot_t post = {0};
            if (cbm_git_snapshot_read(s->root_path, CBM_GIT_SNAPSHOT_FILE_COUNT, &post) == 0 &&
                post.is_git) {
                s->file_count = post.file_count;
                s->interval_ms = cbm_watcher_poll_interval_ms(
                    s->file_count, ctx->w->poll_base_ms, ctx->w->poll_max_ms);
            }
        } else if (rc > 0) {
            cbm_log_info("watcher.index.retry", "project", s->project_name);
        } else {
            cbm_log_warn("watcher.index.err", "project", s->project_name);
        }
    }

    s->next_poll_ns = ctx->now + ((int64_t)s->interval_ms * (int64_t)CBM_NSEC_PER_MSEC);
}

/* Callback to snapshot project state values into an array. */
typedef struct {
    project_snapshot_t *items;
    int count;
    int cap;
    bool oom;
} snapshot_ctx_t;

static void snapshot_project(const char *key, void *val, void *ud) {
    (void)key;
    snapshot_ctx_t *sc = ud;
    if (val && sc->count < sc->cap) {
        if (snapshot_from_state(&sc->items[sc->count], val)) {
            sc->count++;
        } else {
            sc->oom = true;
        }
    }
}

static void watcher_apply_snapshot(cbm_watcher_t *w, const project_snapshot_t *snap) {
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *cur = cbm_ht_get(w->projects, snap->project_name);
    if (cur && strcmp(cur->root_path, snap->root_path) == 0 &&
        cur->version == snap->observed_version) {
        state_apply_snapshot(cur, snap);
    }
    cbm_mutex_unlock(&w->projects_lock);
}

int cbm_watcher_poll_once(cbm_watcher_t *w) {
    if (!w) {
        return 0;
    }

    /* Snapshot project state under lock, then poll without holding it.
     * Write-back is conditional on the same project/path still being watched.
     * This keeps git I/O and index_fn outside the watcher mutex without using
     * raw state pointers that watch/unwatch could free mid-poll. */
    cbm_mutex_lock(&w->projects_lock);
    int n = cbm_ht_count(w->projects);
    if (n == 0) {
        cbm_mutex_unlock(&w->projects_lock);
        return 0;
    }
    project_snapshot_t *snap = calloc((size_t)n, sizeof(*snap));
    if (!snap) {
        cbm_mutex_unlock(&w->projects_lock);
        return 0;
    }
    snapshot_ctx_t sc = {.items = snap, .count = 0, .cap = n};
    cbm_ht_foreach(w->projects, snapshot_project, &sc);
    cbm_mutex_unlock(&w->projects_lock);
    if (sc.oom) {
        for (int i = 0; i < sc.count; i++) {
            snapshot_free(&snap[i]);
        }
        free(snap);
        return 0;
    }

    poll_ctx_t ctx = {
        .w = w,
        .now = now_ns(),
        .reindexed = 0,
    };
    for (int i = 0; i < sc.count; i++) {
        poll_project(NULL, &snap[i], &ctx);
        watcher_apply_snapshot(w, &snap[i]);
        snapshot_free(&snap[i]);
    }
    free(snap);
    return ctx.reindexed;
}

/* ── Blocking run loop ──────────────────────────────────────────── */

void cbm_watcher_stop(cbm_watcher_t *w) {
    if (w) {
        atomic_store(&w->stopped, 1);
    }
}

int cbm_watcher_run(cbm_watcher_t *w, int base_ms, int max_ms) {
    if (!w) {
        return CBM_NOT_FOUND;
    }
    int base_interval_ms = (base_ms > 0) ? base_ms : POLL_BASE_MS;
    w->poll_base_ms = base_interval_ms;
    w->poll_max_ms  = (max_ms  > 0) ? max_ms  : POLL_MAX_MS;

    cbm_log_info("watcher.start", "interval_ms", base_interval_ms > 999 ? "multi-sec" : "fast");

    while (!atomic_load(&w->stopped)) {
        cbm_watcher_poll_once(w);

        /* Sleep in small increments to allow responsive shutdown */
        int slept = 0;
        while (slept < base_interval_ms && !atomic_load(&w->stopped)) {
            int chunk = base_interval_ms - slept;
            if (chunk > SLEEP_CHUNK_MS) {
                chunk = SLEEP_CHUNK_MS;
            }
            cbm_usleep((unsigned)chunk * CBM_MSEC_PER_SEC);
            slept += chunk;
        }
    }

    cbm_log_info("watcher.stop");
    return 0;
}
