/*
 * watcher.c — Git-based file change watcher.
 *
 * Strategy: git status + HEAD tracking (the most reliable approach).
 * For non-git projects, the watcher skips polling (no fsnotify/dirmtime yet).
 *
 *
 * Per-project state tracks:
 *   - Last successfully indexed worktree fingerprint
 *     (HEAD + porcelain status + changed-file content hashes)
 *   - Last poll time + adaptive interval
 *   - Whether the project is a git repo
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
#include "foundation/str_util.h"
#include "pipeline/content_hash.h"

#include <inttypes.h>
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
    char last_fingerprint[17]; /* HEAD + sorted status + changed-file contents */
    bool is_git;               /* false → skip polling */
    bool baseline_done;        /* true after first poll */
    int file_count;            /* approximate, for interval calc */
    int interval_ms;           /* adaptive poll interval */
    atomic_int_fast64_t next_poll_ns; /* shared with touch() */
    atomic_int references;          /* map ownership + active snapshots */
} project_state_t;

/* ── Watcher struct ─────────────────────────────────────────────── */

struct cbm_watcher {
    cbm_store_t *store;
    cbm_index_fn index_fn;
    void *user_data;
    CBMHashTable *projects; /* name → project_state_t* */
    cbm_mutex_t projects_lock;
    atomic_int stopped;
    atomic_bool polling;
};

/* ── Constants ─────────────────────────────────────────────────── */

/* Time unit conversions */
#define NS_PER_SEC 1000000000LL
#define US_PER_MS 1000000LL

/* Adaptive poll interval parameters (ms) */
#define POLL_BASE_MS 5000
#define POLL_FILE_STEP 500 /* add 1s per this many files */
#define POLL_MAX_MS 60000

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

/* Small deterministic combiner for the worktree fingerprint. File contents
 * themselves use XXH3-128 via cbm_content_hash_file(). */
static uint64_t fingerprint_update(uint64_t hash, const void *data, size_t len) {
    const unsigned char *bytes = data;
    for (size_t i = 0; i < len; i++) {
        hash ^= bytes[i];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static void fingerprint_status_record(const char *root_path, const char *record, size_t len,
                                      bool *expect_rename_source, uint64_t *hash) {
    bool has_status = !*expect_rename_source;
    const char *rel_path = record;
    if (has_status) {
        if (len < 3) {
            return;
        }
        rel_path = record + 3;
        *expect_rename_source =
            record[0] == 'R' || record[0] == 'C' || record[1] == 'R' || record[1] == 'C';
    } else {
        *expect_rename_source = false;
    }

    unsigned char marker = has_status ? 1 : 2;
    *hash = fingerprint_update(*hash, &marker, sizeof(marker));
    *hash = fingerprint_update(*hash, record, len);

    size_t root_len = strlen(root_path);
    size_t rel_len = strlen(rel_path);
    char *full_path = malloc(root_len + rel_len + 2);
    if (!full_path) {
        static const char oom[] = "<hash-oom>";
        *hash = fingerprint_update(*hash, oom, sizeof(oom));
        return;
    }
    snprintf(full_path, root_len + rel_len + 2, "%s/%s", root_path, rel_path);

    char content_hash[CBM_CONTENT_HASH_SIZE];
    if (cbm_content_hash_file(full_path, content_hash) == 0) {
        *hash = fingerprint_update(*hash, content_hash, strlen(content_hash));
    } else {
        /* Deleted paths and dirty submodule directories cannot be opened as
         * regular files. Their status record still participates in the
         * fingerprint, with an explicit marker to avoid ambiguity. */
        static const char unavailable[] = "<content-unavailable>";
        *hash = fingerprint_update(*hash, unavailable, sizeof(unavailable));
    }
    free(full_path);
}

/* Build a deterministic fingerprint from HEAD, Git's path-sorted porcelain
 * status, and XXH3-128 content hashes for every changed/untracked path. */
int cbm_watcher_worktree_state(const char *root_path, char *commit_out,
                               size_t commit_out_size, char fingerprint_out[17],
                               bool *dirty_out) {
    if (!root_path || !commit_out || commit_out_size == 0 || !fingerprint_out ||
        !dirty_out || !cbm_validate_shell_arg(root_path)) {
        return CBM_NOT_FOUND;
    }

    char head[CBM_SZ_64] = {0};
    if (git_head(root_path, head, sizeof(head)) != 0) {
        return CBM_NOT_FOUND;
    }

    char cmd[CBM_SZ_1K];
    snprintf(cmd, sizeof(cmd),
             "git --no-optional-locks -C \"%s\" status --porcelain=v1 -z "
             "--untracked-files=all 2>%s",
             root_path, WATCHER_NULDEV);
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }

    uint64_t hash = UINT64_C(1469598103934665603);
    hash = fingerprint_update(hash, head, strlen(head));
    bool dirty = false;
    bool expect_rename_source = false;
    size_t cap = CBM_SZ_1K;
    size_t len = 0;
    char *record = malloc(cap);
    if (!record) {
        cbm_pclose(fp);
        return CBM_NOT_FOUND;
    }

    int ch;
    while ((ch = fgetc(fp)) != EOF) {
        if (ch != '\0') {
            if (len + 1 >= cap) {
                size_t next_cap = cap * PAIR_LEN;
                char *grown = realloc(record, next_cap);
                if (!grown) {
                    free(record);
                    cbm_pclose(fp);
                    return CBM_NOT_FOUND;
                }
                record = grown;
                cap = next_cap;
            }
            record[len++] = (char)ch;
            continue;
        }

        record[len] = '\0';
        if (len > 0) {
            dirty = true;
            fingerprint_status_record(root_path, record, len, &expect_rename_source, &hash);
        }
        len = 0;
    }
    if (len > 0) {
        record[len] = '\0';
        dirty = true;
        fingerprint_status_record(root_path, record, len, &expect_rename_source, &hash);
    }
    bool read_ok = !ferror(fp);
    free(record);
    int status_rc = cbm_pclose(fp);
    if (!read_ok || status_rc != 0) {
        return CBM_NOT_FOUND;
    }

    snprintf(commit_out, commit_out_size, "%s", head);
    snprintf(fingerprint_out, 17, "%016" PRIx64, hash);
    *dirty_out = dirty;
    return 0;
}

static int git_worktree_fingerprint(const char *root_path, char out[17], bool *out_dirty) {
    char commit[CBM_SZ_64];
    return cbm_watcher_worktree_state(root_path, commit, sizeof(commit), out, out_dirty);
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
    if (!s->project_name || !s->root_path) {
        free(s->project_name);
        free(s->root_path);
        free(s);
        return NULL;
    }
    atomic_init(&s->references, 1);
    atomic_init(&s->next_poll_ns, 0);
    s->interval_ms = POLL_BASE_MS;
    return s;
}

static void state_free(project_state_t *s) {
    if (!s) {
        return;
    }
    if (atomic_fetch_sub(&s->references, 1) != 1) {
        return;
    }
    free(s->project_name);
    free(s->root_path);
    free(s);
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
    atomic_init(&w->polling, false);
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
        s->baseline_done = false;
        s->is_git = false;
        s->next_poll_ns = now_ns() + ((int64_t)s->interval_ms * US_PER_MS);
        return;
    }

    s->is_git = is_git_repo(s->root_path);
    s->baseline_done = s->is_git;

    if (s->is_git) {
        bool dirty = false;
        char fingerprint[sizeof(s->last_fingerprint)] = {0};
        if (git_worktree_fingerprint(s->root_path, fingerprint, &dirty) == 0 && !dirty) {
            memcpy(s->last_fingerprint, fingerprint, sizeof(s->last_fingerprint));
        }
        s->file_count = git_file_count(s->root_path);
        s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count);
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "git", "files",
                     s->file_count > 0 ? "yes" : "0");
    } else {
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "none");
    }

    s->next_poll_ns = now_ns() + ((int64_t)s->interval_ms * US_PER_MS);
}

/* Check if a project has changes without mutating the committed fingerprint. */
static bool check_changes(project_state_t *s, char candidate[17]) {
    if (!s->is_git) {
        return false;
    }

    bool dirty = false;
    if (git_worktree_fingerprint(s->root_path, candidate, &dirty) != 0) {
        return false;
    }

    /* A dirty baseline is intentionally processed on the first real poll.
     * A clean baseline can be learned without invoking the callback. */
    if (s->last_fingerprint[0] == '\0') {
        if (!dirty) {
            memcpy(s->last_fingerprint, candidate, sizeof(s->last_fingerprint));
            return false;
        }
        return true;
    }
    return strcmp(candidate, s->last_fingerprint) != 0;
}

/* Context for poll_once foreach callback */
typedef struct {
    cbm_watcher_t *w;
    int64_t now;
    int reindexed;
} poll_ctx_t;

static void poll_project(const char *key, void *val, void *ud) {
    (void)key;
    poll_ctx_t *ctx = ud;
    project_state_t *s = val;
    if (!s) {
        return;
    }

    /* Respect the interval even when initialization must be retried. */
    if (ctx->now < s->next_poll_ns) {
        return;
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
    char candidate[sizeof(s->last_fingerprint)] = {0};
    bool changed = check_changes(s, candidate);
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
            /* Commit the exact pre-index snapshot only after success. If the
             * worktree changes during indexing, the next poll sees a new
             * fingerprint and schedules another pass. */
            memcpy(s->last_fingerprint, candidate, sizeof(s->last_fingerprint));
            /* Refresh file count for interval */
            s->file_count = git_file_count(s->root_path);
            s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count);
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
        project_state_t *s = val;
        atomic_fetch_add(&s->references, 1);
        sc->items[sc->count++] = val;
    }
}

int cbm_watcher_poll_once(cbm_watcher_t *w) {
    if (!w) {
        return 0;
    }

    /* Only one poll owns mutable baseline state; callbacks may reenter. */
    if (atomic_exchange(&w->polling, true)) {
        return 0;
    }

    /* Snapshot project pointers under lock, then poll without holding it.
     * This keeps the critical section small — poll_project does git I/O
     * and may invoke index_fn which runs the full pipeline. */
    cbm_mutex_lock(&w->projects_lock);
    int n = cbm_ht_count(w->projects);
    if (n == 0) {
        cbm_mutex_unlock(&w->projects_lock);
        atomic_store(&w->polling, false);
        return 0;
    }
    project_state_t **snap = malloc(n * sizeof(project_state_t *));
    if (!snap) {
        cbm_mutex_unlock(&w->projects_lock);
        atomic_store(&w->polling, false);
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
        state_free(snap[i]);
    }
    free(snap);
    atomic_store(&w->polling, false);
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
