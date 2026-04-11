/*
 * watcher.c — Git-based file change watcher.
 *
 * Strategy: git status + HEAD tracking (the most reliable approach).
 * For non-git projects, the watcher skips polling (no fsnotify/dirmtime yet).
 *
 *
 * Per-project state tracks:
 *   - Last git HEAD hash (detects commits, checkout, pull)
 *   - Last dirty-tree hash (djb2 of git status --porcelain; prevents reindex
 *     loop when tree is permanently dirty — only reindexes when content changes)
 *   - Last poll time + adaptive interval
 *   - Whether the project is a git repo
 *
 * Adaptive interval: 5s base + 1s per 500 files, capped at 60s.
 * Matches the Go watcher's `pollInterval()` logic.
 */
#include "watcher/watcher.h"
#include "store/store.h"
#include "foundation/log.h"
#include "foundation/hash_table.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/str_util.h"

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
    char last_head[64];       /* git HEAD hash */
    char last_dirty_hash[17]; /* djb2 hex of git status --porcelain output */
    bool is_git;        /* false → skip polling */
    bool baseline_done; /* true after first poll */
    int file_count;     /* approximate, for interval calc */
    int interval_ms;    /* adaptive poll interval */
    // NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
    int64_t next_poll_ns; /* next poll time (monotonic ns) */
} project_state_t;

/* ── Watcher struct ─────────────────────────────────────────────── */

struct cbm_watcher {
    cbm_store_t *store;
    cbm_index_fn index_fn;
    void *user_data;
    CBMHashTable *projects; /* name → project_state_t* */
    atomic_int stopped;
    int poll_base_ms;  /* 0 = use POLL_BASE_MS default */
    int poll_max_ms;   /* 0 = use POLL_MAX_MS default */
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
    // NOLINTNEXTLINE(misc-include-cleaner) — cbm_clock_gettime provided by standard header
    cbm_clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((int64_t)ts.tv_sec * NS_PER_SEC) + ts.tv_nsec;
}

/* ── Adaptive interval ──────────────────────────────────────────── */

int cbm_watcher_poll_interval_ms(int file_count, int base_ms, int max_ms) {
    if (base_ms <= 0) base_ms = POLL_BASE_MS;
    if (max_ms  <= 0) max_ms  = POLL_MAX_MS;
    int ms = base_ms + ((file_count / POLL_FILE_STEP) * 1000);
    if (ms > max_ms) {
        ms = max_ms;
    }
    return ms;
}

/* ── Git helpers ────────────────────────────────────────────────── */

static bool is_git_repo(const char *root_path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "git -C '%s' rev-parse --git-dir >/dev/null 2>&1", root_path);
    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c,concurrency-mt-unsafe)
    return system(cmd) == 0;
}

static int git_head(const char *root_path, char *out, size_t out_size) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "git -C '%s' rev-parse HEAD 2>/dev/null", root_path);
    // NOLINTNEXTLINE(misc-include-cleaner,bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return -1;
    }

    if (fgets(out, (int)out_size, fp)) {
        size_t len = strlen(out);
        while (len > 0 && (out[len - 1] == '\n' || out[len - 1] == '\r')) {
            out[--len] = '\0';
        }
        cbm_pclose(fp);
        return 0;
    }
    cbm_pclose(fp);
    return -1;
}

/* djb2 hash over a string — non-cryptographic, fast, good distribution */
static uint64_t djb2(const char *s) {
    uint64_t h = 5381;
    while (*s) {
        h = ((h << 5) + h) ^ (unsigned char)*s++;
    }
    return h;
}

/* Read full git status --porcelain output into a 16-char hex hash.
 * Returns the number of bytes read (>0 means dirty), -1 on popen failure.
 * out_hex17 must be at least 17 bytes. */
static int git_dirty_hash(const char *root_path, char *out_hex17) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "git --no-optional-locks -C '%s' status --porcelain "
             "--untracked-files=normal 2>/dev/null",
             root_path);
    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        strcpy(out_hex17, "0000000000000000");
        return -1;
    }
    char buf[4096] = {0};
    // NOLINTNEXTLINE(bugprone-not-null-terminated-result) — buf has extra NUL byte
    size_t n = fread(buf, 1, sizeof(buf) - 1, fp);
    buf[n] = '\0';
    cbm_pclose(fp);
    uint64_t h = djb2(n > 0 ? buf : "");
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    snprintf(out_hex17, 17, "%016llx", (unsigned long long)h);
    return (int)n; /* >0 means dirty */
}

/* Count tracked files via git ls-files */
static int git_file_count(const char *root_path) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "git -C '%s' ls-files 2>/dev/null | wc -l", root_path);
    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return 0;
    }

    int count = 0;
    char line[64];
    if (fgets(line, sizeof(line), fp)) {
        count = (int)strtol(line, NULL, 10);
    }
    cbm_pclose(fp);
    return count;
}

/* ── Project state lifecycle ────────────────────────────────────── */

static project_state_t *state_new(const char *name, const char *root_path) {
    project_state_t *s = calloc(1, sizeof(*s));
    if (!s) {
        return NULL;
    }
    // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
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

/* Hash table foreach callback to free state entries */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void free_state_entry(const char *key, void *val, void *ud) {
    (void)key;
    (void)ud;
    state_free(val);
}

/* ── Watcher lifecycle ──────────────────────────────────────────── */

cbm_watcher_t *cbm_watcher_new(cbm_store_t *store, cbm_index_fn index_fn, void *user_data) {
    cbm_watcher_t *w = calloc(1, sizeof(*w));
    if (!w) {
        return NULL;
    }
    w->store = store;
    w->index_fn = index_fn;
    w->user_data = user_data;
    w->projects = cbm_ht_create(32);
    atomic_init(&w->stopped, 0);
    return w;
}

void cbm_watcher_free(cbm_watcher_t *w) {
    if (!w) {
        return;
    }
    cbm_ht_foreach(w->projects, free_state_entry, NULL);
    cbm_ht_free(w->projects);
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

    /* If already watching this project at the same path, preserve existing state.
     * This prevents unnecessary baseline resets when resolve_store() calls us on
     * every project switch — which would discard the accumulated HEAD/dirty hashes
     * and trigger redundant git commands on the next poll cycle. */
    project_state_t *existing = cbm_ht_get(w->projects, project_name);
    if (existing && strcmp(existing->root_path, root_path) == 0) {
        return; /* idempotent — state preserved */
    }

    /* Path changed or first watch: replace old entry */
    if (existing) {
        cbm_ht_delete(w->projects, project_name);
        state_free(existing);
    }

    project_state_t *s = state_new(project_name, root_path);
    cbm_ht_set(w->projects, s->project_name, s);
    cbm_log_info("watcher.watch", "project", project_name, "path", root_path);
}

void cbm_watcher_unwatch(cbm_watcher_t *w, const char *project_name) {
    if (!w || !project_name) {
        return;
    }
    project_state_t *s = cbm_ht_get(w->projects, project_name);
    if (s) {
        cbm_ht_delete(w->projects, project_name);
        state_free(s);
        cbm_log_info("watcher.unwatch", "project", project_name);
    }
}

void cbm_watcher_touch(cbm_watcher_t *w, const char *project_name) {
    if (!w || !project_name) {
        return;
    }
    project_state_t *s = cbm_ht_get(w->projects, project_name);
    if (s) {
        /* Reset backoff — poll immediately on next cycle */
        s->next_poll_ns = 0;
    }
}

int cbm_watcher_watch_count(const cbm_watcher_t *w) {
    if (!w) {
        return 0;
    }
    return (int)cbm_ht_count(w->projects);
}

/* ── Single poll cycle ──────────────────────────────────────────── */

/* Init baseline for a project: check if git, get HEAD, count files */
static void init_baseline(project_state_t *s, const cbm_watcher_t *w) {
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
        s->file_count = git_file_count(s->root_path);
        s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count, w->poll_base_ms, w->poll_max_ms);
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "git", "files",
                     s->file_count > 0 ? "yes" : "0");
    } else {
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "none");
    }

    s->next_poll_ns = now_ns() + ((int64_t)s->interval_ms * US_PER_MS);
}

/* Check if a project has changes. Returns true if reindex needed. */
static bool check_changes(project_state_t *s) {
    if (!s->is_git) {
        return false;
    }

    /* Check HEAD movement */
    char head[64] = {0};
    if (git_head(s->root_path, head, sizeof(head)) == 0) {
        if (s->last_head[0] != '\0' && strcmp(head, s->last_head) != 0) {
            /* HEAD moved — commit, checkout, pull */
            strncpy(s->last_head, head, sizeof(s->last_head) - 1);
            s->last_dirty_hash[0] = '\0'; /* HEAD moved: clear hash to force recheck */
            return true;
        }
        strncpy(s->last_head, head, sizeof(s->last_head) - 1);
    }

    /* Check working tree — only reindex if content actually changed since last poll */
    char new_hash[17];
    int dirty = git_dirty_hash(s->root_path, new_hash);
    if (dirty <= 0) {
        /* Clean tree — clear hash so future dirt is always caught */
        s->last_dirty_hash[0] = '\0';
        return false;
    }
    if (strcmp(new_hash, s->last_dirty_hash) == 0) {
        return false; /* same dirty state as last check — no new changes */
    }
    strncpy(s->last_dirty_hash, new_hash, sizeof(s->last_dirty_hash) - 1);
    return true;
}

/* Context for poll_once foreach callback */
typedef struct {
    cbm_watcher_t *w;
    int64_t now;
    int reindexed;
} poll_ctx_t;

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void poll_project(const char *key, void *val, void *ud) {
    (void)key;
    poll_ctx_t *ctx = ud;
    project_state_t *s = val;
    if (!s) {
        return;
    }

    /* Initialize baseline on first poll */
    if (!s->baseline_done) {
        init_baseline(s, ctx->w);
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
            /* Update HEAD after successful reindex */
            git_head(s->root_path, s->last_head, sizeof(s->last_head));
            /* Refresh dirty hash so same uncommitted changes don't retrigger */
            git_dirty_hash(s->root_path, s->last_dirty_hash);
            /* Refresh file count for interval */
            s->file_count = git_file_count(s->root_path);
            s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count, ctx->w->poll_base_ms, ctx->w->poll_max_ms);
        } else {
            cbm_log_warn("watcher.index.err", "project", s->project_name);
        }
    }

    s->next_poll_ns = ctx->now + ((int64_t)s->interval_ms * US_PER_MS);
}

int cbm_watcher_poll_once(cbm_watcher_t *w) {
    if (!w) {
        return 0;
    }

    poll_ctx_t ctx = {
        .w = w,
        .now = now_ns(),
        .reindexed = 0,
    };
    cbm_ht_foreach(w->projects, poll_project, &ctx);
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
        return -1;
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
            cbm_usleep((unsigned)chunk * 1000);
            slept += chunk;
        }
    }

    cbm_log_info("watcher.stop");
    return 0;
}
