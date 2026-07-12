/*
 * watcher.c — Git-based file change watcher.
 *
 * Strategy: git status + HEAD tracking (the most reliable approach).
 * For non-git projects, the watcher skips polling (no fsnotify/dirmtime yet).
 *
 *
 * Per-project state tracks:
 *   - Last git HEAD hash (detects commits, checkout, pull)
 *   - Dirty-state signature (#937): porcelain entries + per-file size/mtime,
 *     so a persistently dirty tree reindexes once per distinct state instead
 *     of on every poll (write amplification)
 *   - Last poll time + adaptive interval
 *   - Whether the project is a git repo
 *
 * Baselines are committed only after a successful reindex; busy-skips and
 * failed runs leave them untouched so the change is retried, never lost.
 *
 * Adaptive interval: 5s base + 1s per 500 files, capped at 60s.
 * Matches the Go watcher's `pollInterval()` logic.
 */
#include <stdint.h>
#include "watcher/watcher.h"
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

/* ── Per-project state ──────────────────────────────────────────── */

typedef struct {
    char *project_name;
    char *root_path;
    char last_head[CBM_SZ_64]; /* git HEAD hash (committed baseline) */
    bool is_git;               /* false → skip polling */
    bool baseline_done;        /* true after first poll */
    int missing_root_count;    /* consecutive polls where root was missing (ENOENT/ENOTDIR) */
    uint64_t first_missing_ms; /* cbm_now_ms() of the streak's first miss (0 = no streak) */
    int file_count;            /* approximate, for interval calc */
    int interval_ms;           /* adaptive poll interval */
    int64_t next_poll_ns;      /* next poll time (monotonic ns) */
    /* Dirty-state signature (#937): a persistently dirty worktree must
     * reindex once per DISTINCT dirty state, not on every poll. Baselines
     * are committed only after a SUCCESSFUL reindex (busy-skips and failed
     * runs retry); check_changes stages its observations in the pending_*
     * fields. 0 = clean tree. */
    uint64_t last_dirty_sig;      /* committed dirty-state signature */
    uint64_t pending_dirty_sig;   /* observed at check time */
    char pending_head[CBM_SZ_64]; /* HEAD observed at check time */
} project_state_t;

/* ── Watcher struct ─────────────────────────────────────────────── */

struct cbm_watcher {
    cbm_store_t *store;
    cbm_index_fn index_fn;
    void *user_data;
    CBMHashTable *projects; /* name → project_state_t* */
    cbm_mutex_t projects_lock;
    atomic_int stopped;
    /* Deferred-free list: freed after the next poll_once. */
    project_state_t **pending_free;
    int pending_free_count;
    int pending_free_cap;
};

/* ── Constants ─────────────────────────────────────────────────── */

/* Time unit conversions */
#define NS_PER_SEC 1000000000LL
#define US_PER_MS 1000000LL

/* Adaptive poll interval parameters (ms) */
#define POLL_BASE_MS 5000
#define POLL_FILE_STEP 500 /* add 1s per this many files */
#define POLL_MAX_MS 60000

/* Stale-root pruning (#286): a watched project whose root directory stays
 * missing is pruned — its cached DB is deleted and the watch entry removed.
 * Deletion is destructive (the DB can hold user-authored data such as the
 * ADR), so it requires BOTH a streak of consecutive missing polls AND a
 * sustained-absence grace window measured from the streak's first miss. */
#define MISSING_ROOT_DELETE_AFTER 3
#define PRUNE_GRACE_DEFAULT_S 600 /* 10 min; override: CBM_WATCHER_PRUNE_GRACE_S */

/* Sleep chunk for responsive shutdown (ms) */
#define SLEEP_CHUNK_MS 500

/* ── Time helper ────────────────────────────────────────────────── */

static int64_t now_ns(void) {
    struct timespec ts;
    cbm_clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((int64_t)ts.tv_sec * NS_PER_SEC) + ts.tv_nsec;
}

/* ── Adaptive interval ──────────────────────────────────────────── */

int cbm_watcher_poll_interval_ms(int file_count) {
    int ms = POLL_BASE_MS + ((file_count / POLL_FILE_STEP) * CBM_MSEC_PER_SEC);
    if (ms > POLL_MAX_MS) {
        ms = POLL_MAX_MS;
    }
    return ms;
}

/* ── Git helpers ────────────────────────────────────────────────── */

/* Portable command pieces: cbm_popen runs through cmd.exe on Windows, which does
 * NOT strip single quotes (git would receive a literal-quoted path → "cannot find
 * the path") and has no /dev/null. Use double quotes (stripped by both cmd.exe and
 * POSIX sh) and the platform null device. */
#if defined(_WIN32)
#define WATCHER_NULDEV "NUL"
#else
#define WATCHER_NULDEV "/dev/null"
#endif

static bool is_git_repo(const char *root_path) {
    char cmd[CBM_SZ_1K];
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" rev-parse --git-dir 2>%s", root_path, WATCHER_NULDEV);
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return false;
    }
    /* Drain output so pclose gets a clean exit status. */
    char drain[CBM_SZ_128];
    while (fgets(drain, (int)sizeof(drain), fp)) { /* discard */
    }
    int rc = cbm_pclose(fp);
    return rc == 0;
}

static int git_head(const char *root_path, char *out, size_t out_size) {
    char cmd[CBM_SZ_1K];
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" rev-parse HEAD 2>%s", root_path, WATCHER_NULDEV);
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }

    if (fgets(out, (int)out_size, fp)) {
        size_t len = strlen(out);
        while (len > 0 && (out[len - SKIP_ONE] == '\n' || out[len - SKIP_ONE] == '\r')) {
            out[--len] = '\0';
        }
        cbm_pclose(fp);
        return 0;
    }
    cbm_pclose(fp);
    return CBM_NOT_FOUND;
}

/* ── Dirty-state signature (#937) ───────────────────────────────── */

#define SIG_FNV_OFFSET 1469598103934665603ULL
#define SIG_FNV_PRIME 1099511628211ULL

static uint64_t sig_fold(uint64_t h, const void *data, size_t len) {
    const unsigned char *p = data;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= SIG_FNV_PRIME;
    }
    return h;
}

/* Platform-portable mtime_ns (mirrors pipeline_incremental.c). */
static int64_t sig_stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * NS_PER_SEC) + (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * NS_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * NS_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

/* Fold a listed path's (size, mtime) into the signature so an in-place edit
 * of an already-dirty file still produces a new signature. A failed stat
 * (deleted file, quoting artifact) degrades to the entry text alone — the
 * deletion itself is represented by the porcelain status. */
static uint64_t sig_fold_path_stat(uint64_t h, const char *root_path, const char *rel) {
    char abs[CBM_SZ_4K];
    snprintf(abs, sizeof(abs), "%s/%s", root_path, rel);
    struct stat st;
    if (stat(abs, &st) == 0) {
        int64_t mt = sig_stat_mtime_ns(&st);
        int64_t sz = (int64_t)st.st_size;
        h = sig_fold(h, &mt, sizeof(mt));
        h = sig_fold(h, &sz, sizeof(sz));
    }
    return h;
}

/* Signature of the current dirty state: FNV-1a over the entries of
 * `git status --porcelain -uall -z` plus each listed path's (size, mtime).
 * Returns 0 for a clean tree. Two polls over an untouched dirty tree yield
 * the same value; editing a dirty file, adding/removing one, or reverting
 * the tree to clean each yield a new one. -uall lists files inside
 * untracked directories individually (a nested addition under `?? dir/`
 * would otherwise be invisible); -z gives unquoted NUL-separated paths that
 * hash identically across polls and stat cleanly. */
static uint64_t git_dirty_signature(const char *root_path) {
    char cmd[CBM_SZ_1K];
    snprintf(cmd, sizeof(cmd), "git --no-optional-locks -C \"%s\" status --porcelain -uall -z 2>%s",
             root_path, WATCHER_NULDEV);
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return 0;
    }

    uint64_t h = SIG_FNV_OFFSET;
    bool any = false;
    char entry[CBM_SZ_4K];
    size_t elen = 0;
    bool overflow = false;
    /* Rename/copy entries are followed by a second NUL token (the origin
     * path) that carries no XY prefix — fold it as text, don't stat it. */
    bool origin_token = false;
    int c;
    for (;;) {
        c = fgetc(fp);
        if (c != EOF && c != '\0') {
            if (elen + 1 < sizeof(entry)) {
                entry[elen] = (char)c;
            } else {
                overflow = true; /* keep hashing prefix; skip stat for entry */
            }
            elen++;
            continue;
        }
        size_t stored = elen < sizeof(entry) ? elen : sizeof(entry) - 1;
        entry[stored] = '\0';
        if (stored > 0) {
            any = true;
            h = sig_fold(h, entry, stored);
            h = sig_fold(h, "", 1); /* token separator */
            if (origin_token) {
                origin_token = false;
            } else if (!overflow && stored > 3 && entry[2] == ' ') {
                if (entry[0] == 'R' || entry[0] == 'C') {
                    origin_token = true;
                }
                h = sig_fold_path_stat(h, root_path, entry + 3);
            }
        }
        elen = 0;
        overflow = false;
        if (c == EOF) {
            break;
        }
    }
    cbm_pclose(fp);

#if !defined(_WIN32)
    /* Submodules (POSIX-only, mirroring git_is_dirty): fold each submodule's
     * porcelain output with paths re-rooted at the superproject so they stat
     * correctly. Line mode here (foreach output is line-oriented); rename
     * lines fail the stat and degrade to text-only. */
    snprintf(cmd, sizeof(cmd),
             "git --no-optional-locks -C '%s' submodule foreach --quiet --recursive "
             "'git status --porcelain -uall 2>/dev/null | "
             "sed -e \"s@^\\(..\\) @\\1 $displaypath/@\"' 2>/dev/null",
             root_path);
    fp = cbm_popen(cmd, "r");
    if (fp) {
        char line[CBM_SZ_4K];
        while (fgets(line, sizeof(line), fp)) {
            size_t len = strlen(line);
            while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
                line[--len] = '\0';
            }
            if (len == 0) {
                continue;
            }
            any = true;
            h = sig_fold(h, line, len);
            h = sig_fold(h, "", 1);
            if (len > 3 && line[2] == ' ') {
                h = sig_fold_path_stat(h, root_path, line + 3);
            }
        }
        cbm_pclose(fp);
    }
#endif

    if (!any) {
        return 0;
    }
    return h ? h : 1; /* reserve 0 for "clean" */
}

/* Count tracked files via git ls-files */
static int git_file_count(const char *root_path) {
    char cmd[CBM_SZ_1K];
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" ls-files 2>%s", root_path, WATCHER_NULDEV);
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return 0;
    }

    /* Count newlines (one tracked file per line). `wc -l` is unavailable on
     * Windows, so count in C, robust to paths longer than the read buffer. */
    int count = 0;
    char buf[CBM_SZ_1K];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (buf[i] == '\n') {
                count++;
            }
        }
    }
    cbm_pclose(fp);
    return count;
}

/* ── Project state lifecycle ────────────────────────────────────── */

static project_state_t *state_new(const char *name, const char *root_path) {
    project_state_t *s = calloc(CBM_ALLOC_ONE, sizeof(*s));
    if (!s) {
        return NULL;
    }
    s->project_name = strdup(name);
    s->root_path = strdup(root_path);
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

/* Move a state onto the deferred-free list (caller holds projects_lock).
 * The state may still be referenced by a poll_once snapshot; poll_once
 * drains the list at the start of its next cycle. Returns false when
 * growing the list fails (OOM): the state is left untouched and the
 * caller must keep it registered — freeing it immediately here could be
 * a use-after-free against an in-flight poll snapshot. */
static bool defer_state_free(cbm_watcher_t *w, project_state_t *s) {
    if (w->pending_free_count >= w->pending_free_cap) {
        int new_cap = w->pending_free_cap ? w->pending_free_cap * 2 : 8;
        project_state_t **tmp =
            realloc(w->pending_free, (size_t)new_cap * sizeof(project_state_t *));
        if (!tmp) {
            cbm_log_warn("watcher.unwatch.oom", "project", s->project_name);
            return false;
        }
        w->pending_free = tmp;
        w->pending_free_cap = new_cap;
    }
    w->pending_free[w->pending_free_count++] = s;
    return true;
}

/* ── Stale-root pruning (#286) ──────────────────────────────────── */

bool cbm_watcher_root_missing_errno(int err) {
    /* Only ENOENT/ENOTDIR mean the root itself is gone. Anything else
     * (EACCES, EIO, ELOOP, a transient network mount, macOS TCC permission
     * revocation) is uncertainty: the directory may still exist even though
     * we cannot see it right now — never treat it as a deletion signal.
     * Windows (mingw/UCRT) maps ERROR_FILE_NOT_FOUND / ERROR_PATH_NOT_FOUND
     * to ENOENT, so the same check holds there (same convention as
     * find_deleted_files in pipeline_incremental.c). */
    return err == ENOENT || err == ENOTDIR;
}

typedef enum {
    ROOT_PRESENT = 0, /* stat succeeded and the root is a directory */
    ROOT_MISSING,     /* genuinely gone: ENOENT/ENOTDIR (or replaced by a non-directory) */
    ROOT_UNCERTAIN,   /* any other stat failure — must NOT count toward pruning */
} root_status_t;

static root_status_t root_status(const char *root_path, int *out_errno) {
    *out_errno = 0;
    if (!root_path) {
        return ROOT_UNCERTAIN;
    }
    struct stat st;
    if (stat(root_path, &st) == 0) {
        /* Exists but is no longer a directory → the root directory is gone. */
        return S_ISDIR(st.st_mode) ? ROOT_PRESENT : ROOT_MISSING;
    }
    *out_errno = errno;
    return cbm_watcher_root_missing_errno(errno) ? ROOT_MISSING : ROOT_UNCERTAIN;
}

/* Sustained-absence window (seconds) before a missing root may be pruned.
 * Generous default: 10 minutes. Override with CBM_WATCHER_PRUNE_GRACE_S
 * (>= 0; 0 prunes as soon as the missing-poll streak is reached). Read on
 * each call so tests/operators can adjust via setenv without a restart —
 * same convention as cbm_max_file_bytes in limits.c. */
static long prune_grace_s(void) {
    const char *raw = getenv("CBM_WATCHER_PRUNE_GRACE_S");
    if (raw && raw[0]) {
        errno = 0;
        char *end = NULL;
        long v = strtol(raw, &end, 10);
        if (errno == 0 && end != raw && *end == '\0' && v >= 0) {
            return v;
        }
        /* Unparseable / negative → fall through to the safe default. */
    }
    return PRUNE_GRACE_DEFAULT_S;
}

/* Format int to string for logging (poll thread only, one use per call). */
static const char *itoa_buf(int v) {
    static CBM_TLS char buf[CBM_SZ_32];
    snprintf(buf, sizeof(buf), "%d", v);
    return buf;
}

static void delete_cached_project_db(const char *project_name) {
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
    /* Safety net: ensure stopped is set before draining pending_free.
     * In production the caller should cbm_watcher_stop() + join first. */
    atomic_store(&w->stopped, 1);
    cbm_mutex_lock(&w->projects_lock);
    cbm_ht_foreach(w->projects, free_state_entry, NULL);
    cbm_ht_free(w->projects);
    for (int i = 0; i < w->pending_free_count; i++) {
        state_free(w->pending_free[i]);
    }
    free(w->pending_free);
    cbm_mutex_unlock(&w->projects_lock);
    cbm_mutex_destroy(&w->projects_lock);
    free(w);
}

/* ── Watch list management ──────────────────────────────────────── */

void cbm_watcher_watch(cbm_watcher_t *w, const char *project_name, const char *root_path) {
    if (!w || !project_name || !root_path) {
        return;
    }

    /* Reject paths with shell metacharacters — all git helpers use popen/system */
    if (!cbm_validate_shell_arg(root_path)) {
        cbm_log_warn("watcher.watch.reject", "project", project_name, "reason",
                     "path contains shell metacharacters");
        return;
    }

    /* Remove old entry first (key points to state's project_name) */
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *old = cbm_ht_get(w->projects, project_name);
    if (old) {
        cbm_ht_delete(w->projects, project_name);
        state_free(old);
    }

    project_state_t *s = state_new(project_name, root_path);
    if (!s) {
        cbm_mutex_unlock(&w->projects_lock);
        cbm_log_warn("watcher.watch.oom", "project", project_name, "path", root_path);
        return;
    }
    cbm_ht_set(w->projects, s->project_name, s);
    cbm_mutex_unlock(&w->projects_lock);
    cbm_log_info("watcher.watch", "project", project_name, "path", root_path);
}

void cbm_watcher_unwatch(cbm_watcher_t *w, const char *project_name) {
    if (!w || !project_name) {
        return;
    }
    bool removed = false;
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *s = cbm_ht_get(w->projects, project_name);
    if (s && defer_state_free(w, s)) {
        /* The entry leaves the table only once its state is safely on
         * the deferred-free list; on OOM the watch stays registered. */
        cbm_ht_delete(w->projects, project_name);
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
static void init_baseline(project_state_t *s) {
    struct stat st;
    if (stat(s->root_path, &st) != 0) {
        cbm_log_warn("watcher.root_gone", "project", s->project_name, "path", s->root_path);
        s->baseline_done = true;
        s->is_git = false;
        return;
    }

    s->is_git = is_git_repo(s->root_path);
    s->baseline_done = true;

    if (s->is_git) {
        git_head(s->root_path, s->last_head, sizeof(s->last_head));
        /* last_dirty_sig stays 0 ("clean known"): a tree that is ALREADY
         * dirty at baseline reindexes once on the first poll — the watcher
         * cannot know whether that state made it into the DB (e.g. server
         * restart with a stale artifact). At-least-once, then the signature
         * gates further polls (#937). */
        s->file_count = git_file_count(s->root_path);
        s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count);
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "git", "files",
                     s->file_count > 0 ? "yes" : "0");
    } else {
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "none");
    }

    s->next_poll_ns = now_ns() + ((int64_t)s->interval_ms * US_PER_MS);
}

/* Check if a project has changes. Returns true if reindex needed.
 * Must NOT mutate the committed baselines (last_head, last_dirty_sig):
 * poll_project commits them only after a SUCCESSFUL reindex so that
 * busy-skips and failed runs retry instead of silently losing the change
 * (#937). Observations are staged in the pending_* fields. */
static bool check_changes(project_state_t *s) {
    if (!s->is_git) {
        return false;
    }

    bool changed = false;

    /* Check HEAD movement (commit, checkout, pull) */
    s->pending_head[0] = '\0';
    char head[CBM_SZ_64] = {0};
    if (git_head(s->root_path, head, sizeof(head)) == 0) {
        if (s->last_head[0] == '\0') {
            /* First observed HEAD: adopt as baseline, not a change. */
            snprintf(s->last_head, sizeof(s->last_head), "%s", head);
        } else if (strcmp(head, s->last_head) != 0) {
            changed = true;
        }
        snprintf(s->pending_head, sizeof(s->pending_head), "%s", head);
    }

    /* Working tree: reindex only when the DIRTY STATE ITSELF changed —
     * a persistently dirty tree polled while idle must not re-trigger
     * full reindex/write cycles (#937 write amplification). */
    uint64_t sig = git_dirty_signature(s->root_path);
    if (sig != s->last_dirty_sig) {
        changed = true;
    }
    s->pending_dirty_sig = sig;

    return changed;
}

/* Context for poll_once foreach callback */
typedef struct {
    cbm_watcher_t *w;
    int64_t now;
    int reindexed;
} poll_ctx_t;

static void prune_missing_project(cbm_watcher_t *w, project_state_t *s) {
    if (!w || !s || !s->project_name) {
        return;
    }

    char project_name[CBM_SZ_1K];
    snprintf(project_name, sizeof(project_name), "%s", s->project_name);

    bool removed = false;
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *current = cbm_ht_get(w->projects, project_name);
    /* Deferred free (same discipline as cbm_watcher_unwatch): this state
     * is referenced by the poll_once snapshot iterating us. On OOM the
     * watch stays registered and pruning retries on the next cycle. */
    if (current == s && defer_state_free(w, s)) {
        delete_cached_project_db(project_name);
        cbm_ht_delete(w->projects, project_name);
        removed = true;
    }
    cbm_mutex_unlock(&w->projects_lock);

    if (removed) {
        cbm_log_info("watcher.root_pruned", "project", project_name);
    }
}

static void poll_project(const char *key, void *val, void *ud) {
    (void)key;
    poll_ctx_t *ctx = ud;
    project_state_t *s = val;
    if (!s) {
        return;
    }

    /* Stale-root pruning (#286): classify the root BEFORE the baseline /
     * is_git / interval gates so vanished roots are noticed even for
     * non-git projects and regardless of adaptive backoff. */
    int stat_errno = 0;
    root_status_t rs = root_status(s->root_path, &stat_errno);
    if (rs == ROOT_UNCERTAIN) {
        /* EACCES / EIO / network blip / TCC revocation — the root may still
         * exist. Never count toward pruning; restart the streak so only an
         * uninterrupted run of genuine ENOENT/ENOTDIR observations can
         * delete user data. */
        if (s->missing_root_count > 0) {
            s->missing_root_count = 0;
            s->first_missing_ms = 0;
        }
        cbm_log_warn("watcher.root_stat_error", "project", s->project_name, "path", s->root_path,
                     "errno", itoa_buf(stat_errno));
        return;
    }
    if (rs == ROOT_MISSING) {
        uint64_t now_ms = cbm_now_ms();
        if (s->missing_root_count == 0) {
            s->first_missing_ms = now_ms;
        }
        s->missing_root_count++;
        cbm_log_warn("watcher.root_missing", "project", s->project_name, "path", s->root_path,
                     "polls", itoa_buf(s->missing_root_count));
        if (s->missing_root_count >= MISSING_ROOT_DELETE_AFTER &&
            now_ms - s->first_missing_ms >= (uint64_t)prune_grace_s() * CBM_MSEC_PER_SEC) {
            prune_missing_project(ctx->w, s);
        }
        return;
    }
    if (s->missing_root_count > 0) {
        cbm_log_info("watcher.root_restored", "project", s->project_name, "path", s->root_path);
        s->missing_root_count = 0;
        s->first_missing_ms = 0;
    }

    /* Initialize baseline on first poll */
    if (!s->baseline_done) {
        init_baseline(s);
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
        s->next_poll_ns = ctx->now + ((int64_t)s->interval_ms * US_PER_MS);
        return;
    }

    /* Trigger reindex */
    cbm_log_info("watcher.changed", "project", s->project_name, "strategy", "git");
    if (ctx->w->index_fn) {
        int rc = ctx->w->index_fn(s->project_name, s->root_path, ctx->w->user_data);
        if (rc == 0) {
            ctx->reindexed++;
            /* Commit the baselines OBSERVED AT CHECK TIME — the state whose
             * reindex just succeeded. A commit/edit landing during the
             * reindex is deliberately not absorbed: the next poll sees it
             * as a new delta (at-least-once, never lost). */
            if (s->pending_head[0] != '\0') {
                snprintf(s->last_head, sizeof(s->last_head), "%s", s->pending_head);
            }
            s->last_dirty_sig = s->pending_dirty_sig;
            /* Refresh file count for interval */
            s->file_count = git_file_count(s->root_path);
            s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count);
        } else if (rc > 0) {
            /* Busy-skip: baseline stays uncommitted, next poll retries. */
            cbm_log_info("watcher.index.retry", "project", s->project_name);
        } else {
            cbm_log_warn("watcher.index.err", "project", s->project_name);
        }
    }

    s->next_poll_ns = ctx->now + ((int64_t)s->interval_ms * US_PER_MS);
}

/* Callback to snapshot project state pointers into an array. */
typedef struct {
    project_state_t **items;
    int count;
    int cap;
} snapshot_ctx_t;

static void snapshot_project(const char *key, void *val, void *ud) {
    (void)key;
    snapshot_ctx_t *sc = ud;
    if (val && sc->count < sc->cap) {
        sc->items[sc->count++] = val;
    }
}

int cbm_watcher_poll_once(cbm_watcher_t *w) {
    if (!w) {
        return 0;
    }

    /* Snapshot project pointers under lock, then poll without holding it.
     * This keeps the critical section small — poll_project does git I/O
     * and may invoke index_fn which runs the full pipeline. */
    cbm_mutex_lock(&w->projects_lock);

    /* Free deferred entries from the previous cycle. */
    for (int i = 0; i < w->pending_free_count; i++) {
        state_free(w->pending_free[i]);
    }
    w->pending_free_count = 0;

    int n = cbm_ht_count(w->projects);
    if (n == 0) {
        cbm_mutex_unlock(&w->projects_lock);
        return 0;
    }
    project_state_t **snap = malloc(n * sizeof(project_state_t *));
    if (!snap) {
        cbm_mutex_unlock(&w->projects_lock);
        return 0;
    }
    snapshot_ctx_t sc = {.items = snap, .count = 0, .cap = n};
    cbm_ht_foreach(w->projects, snapshot_project, &sc);
    cbm_mutex_unlock(&w->projects_lock);

    poll_ctx_t ctx = {
        .w = w,
        .now = now_ns(),
        .reindexed = 0,
    };
    for (int i = 0; i < sc.count; i++) {
        poll_project(NULL, snap[i], &ctx);
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

int cbm_watcher_run(cbm_watcher_t *w, int base_interval_ms) {
    if (!w) {
        return CBM_NOT_FOUND;
    }
    if (base_interval_ms <= 0) {
        base_interval_ms = POLL_BASE_MS;
    }

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
