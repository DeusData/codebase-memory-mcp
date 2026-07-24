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
#include "foundation/subprocess.h"
#ifdef _WIN32
#include "foundation/win_utf8.h"
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#include <errno.h>
#include <limits.h>
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
    /* poll_once snapshots retain state pointers after projects_lock is
     * released. Unwatch/replacement tombstones a removed state so a later
     * snapshot entry cannot admit new indexing work. */
    atomic_bool registered;
    /* Published only while one supervised Git command is active. Access is
     * serialized by watcher->projects_lock; cancellation itself is atomic. */
    cbm_subprocess_t *active_git;
    /* Room for Git's 64-hex SHA-256 object ID plus newline/NUL while reading. */
    char last_head[CBM_SZ_128]; /* git HEAD hash (committed baseline) */
    bool is_git;                /* false → skip polling */
    bool baseline_done;         /* true after first poll */
    int missing_root_count;     /* consecutive polls where root was missing (ENOENT/ENOTDIR) */
    uint64_t first_missing_ms;  /* cbm_now_ms() of the streak's first miss (0 = no streak) */
    int file_count;             /* approximate, for interval calc */
    int interval_ms;            /* adaptive poll interval */
    int64_t next_poll_ns;       /* next poll time (monotonic ns) */
    /* Dirty-state signature (#937): a persistently dirty worktree must
     * reindex once per DISTINCT dirty state, not on every poll. Baselines
     * are committed only after a SUCCESSFUL reindex (busy-skips and failed
     * runs retry); check_changes stages its observations in the pending_*
     * fields. 0 = clean tree. */
    uint64_t last_dirty_sig;       /* committed dirty-state signature */
    uint64_t pending_dirty_sig;    /* observed at check time */
    char pending_head[CBM_SZ_128]; /* HEAD observed at check time */
} project_state_t;

/* ── Watcher struct ─────────────────────────────────────────────── */

struct cbm_watcher {
    cbm_store_t *store;
    cbm_index_fn index_fn;
    void *user_data;
    CBMHashTable *projects; /* name → project_state_t* */
    cbm_mutex_t projects_lock;
    /* Serializes callback replacement with the entire destructive prune
     * transaction so a borrowed daemon context cannot be freed mid-callback. */
    cbm_mutex_t coordination_lock;
    cbm_watcher_project_mutation_begin_fn mutation_begin;
    cbm_watcher_project_mutation_end_fn mutation_end;
    cbm_watcher_project_pruned_fn project_pruned;
    void *mutation_context;
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

/* Git is external and repository-controlled configuration may activate slow
 * helpers (for example fsmonitor). Every invocation therefore has both a hard
 * wall-clock deadline and a finite capture budget. */
#define WATCHER_GIT_DEADLINE_MS 30000
#define WATCHER_GIT_OUTPUT_MAX (64U * 1024U * 1024U)
#define WATCHER_GIT_HEAD_MAX 4096U
#define WATCHER_GIT_POLL_US 10000U

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

typedef struct {
    char path[CBM_SZ_4K];
    size_t size;
} watcher_git_output_t;

typedef enum {
    WATCHER_GIT_OK = 0,
    WATCHER_GIT_COMMAND_FAILED,
    WATCHER_GIT_CANCELLED,
    WATCHER_GIT_DEADLINE,
    WATCHER_GIT_OUTPUT_LIMIT,
    WATCHER_GIT_SUPERVISION_FAILED,
} watcher_git_status_t;

static void watcher_git_output_cleanup(watcher_git_output_t *output) {
    if (output && output->path[0]) {
        (void)cbm_unlink(output->path);
        output->path[0] = '\0';
        output->size = 0;
    }
}

static bool watcher_git_output_create(watcher_git_output_t *output) {
    if (!output) {
        return false;
    }
    memset(output, 0, sizeof(*output));
    int written =
        snprintf(output->path, sizeof(output->path), "%s/cbm-watcher-git-XXXXXX", cbm_tmpdir());
    if (written <= 0 || written >= (int)sizeof(output->path)) {
        output->path[0] = '\0';
        return false;
    }
    int descriptor = cbm_mkstemp(output->path);
    if (descriptor < 0) {
        output->path[0] = '\0';
        return false;
    }
#ifdef _WIN32
    bool closed = _close(descriptor) == 0;
#else
    bool closed = close(descriptor) == 0;
#endif
    if (!closed) {
        watcher_git_output_cleanup(output);
    }
    return closed;
}

#ifdef _WIN32
/* CreateProcessW does not search PATH when lpApplicationName is non-NULL.
 * Resolve Git ourselves, and deliberately accept only absolute PATH entries:
 * empty/relative entries would reintroduce Windows' current-directory search. */
static bool watcher_windows_path_absolute(const wchar_t *path) {
    if (!path || wcslen(path) < 3U) {
        return false;
    }
    bool drive = ((path[0] >= L'A' && path[0] <= L'Z') || (path[0] >= L'a' && path[0] <= L'z')) &&
                 path[1] == L':' && (path[2] == L'\\' || path[2] == L'/');
    bool unc = (path[0] == L'\\' || path[0] == L'/') && (path[1] == L'\\' || path[1] == L'/') &&
               path[2] != L'\0' && path[2] != L'\\' && path[2] != L'/';
    return drive || unc;
}

static bool watcher_windows_git_candidate(const wchar_t *entry, size_t entry_length,
                                          char output[CBM_SZ_4K]) {
    while (entry_length > 0U && (entry[0] == L' ' || entry[0] == L'\t')) {
        entry++;
        entry_length--;
    }
    while (entry_length > 0U &&
           (entry[entry_length - 1U] == L' ' || entry[entry_length - 1U] == L'\t')) {
        entry_length--;
    }
    if (entry_length >= 2U && entry[0] == L'"' && entry[entry_length - 1U] == L'"') {
        entry++;
        entry_length -= 2U;
    }
    while (entry_length > 0U && (entry[0] == L' ' || entry[0] == L'\t')) {
        entry++;
        entry_length--;
    }
    while (entry_length > 0U &&
           (entry[entry_length - 1U] == L' ' || entry[entry_length - 1U] == L'\t')) {
        entry_length--;
    }
    if (entry_length == 0U || entry_length >= CBM_SZ_4K) {
        return false;
    }
    wchar_t directory[CBM_SZ_4K];
    memcpy(directory, entry, entry_length * sizeof(*directory));
    directory[entry_length] = L'\0';
    if (!watcher_windows_path_absolute(directory) || wcschr(directory, L'"') != NULL) {
        return false;
    }

    bool separator = directory[entry_length - 1U] == L'\\' || directory[entry_length - 1U] == L'/';
    wchar_t candidate[CBM_SZ_4K];
    int written =
        swprintf(candidate, CBM_SZ_4K, separator ? L"%lsgit.exe" : L"%ls\\git.exe", directory);
    if (written <= 0 || written >= CBM_SZ_4K) {
        return false;
    }
    DWORD required = GetFullPathNameW(candidate, 0U, NULL, NULL);
    wchar_t *normalized =
        required > 0U ? malloc(((size_t)required + 1U) * sizeof(*normalized)) : NULL;
    DWORD normalized_length =
        normalized ? GetFullPathNameW(candidate, required + 1U, normalized, NULL) : 0U;
    if (!normalized || normalized_length == 0U || normalized_length > required ||
        !watcher_windows_path_absolute(normalized)) {
        free(normalized);
        return false;
    }
    HANDLE file = CreateFileW(normalized, GENERIC_READ | FILE_READ_ATTRIBUTES,
                              FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                              OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    BY_HANDLE_FILE_INFORMATION information;
    bool regular = file != INVALID_HANDLE_VALUE && GetFileType(file) == FILE_TYPE_DISK &&
                   GetFileInformationByHandle(file, &information) != 0 &&
                   (information.dwFileAttributes &
                    (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0;
    if (file != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(file);
    }
    char *utf8 = regular ? cbm_wide_to_utf8(normalized) : NULL;
    size_t utf8_length = utf8 ? strlen(utf8) : 0U;
    bool valid = utf8 && utf8_length > 0U && utf8_length < CBM_SZ_4K;
    if (valid) {
        memcpy(output, utf8, utf8_length + 1U);
    }
    free(utf8);
    free(normalized);
    return valid;
}

static bool watcher_resolve_git_executable(char output[CBM_SZ_4K]) {
    output[0] = '\0';
    DWORD required = GetEnvironmentVariableW(L"PATH", NULL, 0U);
    wchar_t *path = required > 0U ? malloc((size_t)required * sizeof(*path)) : NULL;
    DWORD length = path ? GetEnvironmentVariableW(L"PATH", path, required) : 0U;
    if (!path || length == 0U || length >= required) {
        free(path);
        return false;
    }
    const wchar_t *entry = path;
    for (const wchar_t *cursor = path;; cursor++) {
        if (*cursor != L';' && *cursor != L'\0') {
            continue;
        }
        if (watcher_windows_git_candidate(entry, (size_t)(cursor - entry), output)) {
            free(path);
            return true;
        }
        if (*cursor == L'\0') {
            break;
        }
        entry = cursor + 1;
    }
    free(path);
    return false;
}
#endif

/* Run one literal argv vector in a contained process tree. active_git is
 * published under projects_lock before supervision starts, so stop/unwatch can
 * request cancellation without racing destruction of the handle. */
static watcher_git_status_t watcher_git_run(cbm_watcher_t *w, project_state_t *state,
                                            const char *const *argv, size_t output_limit,
                                            watcher_git_output_t *output) {
    if (!w || !state || !argv || !argv[0]) {
        return WATCHER_GIT_SUPERVISION_FAILED;
    }
    if (output) {
        memset(output, 0, sizeof(*output));
    }
    if (output_limit > 0 && (!output || !watcher_git_output_create(output))) {
        return WATCHER_GIT_SUPERVISION_FAILED;
    }

#ifdef _WIN32
    char git_executable[CBM_SZ_4K];
    if (!watcher_resolve_git_executable(git_executable)) {
        watcher_git_output_cleanup(output);
        return WATCHER_GIT_SUPERVISION_FAILED;
    }
#endif

    cbm_proc_opts_t options = {
#ifdef _WIN32
        .bin = git_executable,
#else
        .bin = argv[0],
#endif
        .argv = argv,
        .log_file = output_limit > 0 ? output->path : NULL,
        /* A wall deadline below is authoritative. Binary `status -z` output
         * deliberately has no line-based quiet-timeout semantics. */
        .quiet_timeout_ms = 0,
        .cancel_grace_ms = CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS,
        .delete_log_on_exit = false,
    };
    cbm_subprocess_t *process = NULL;
    if (cbm_subprocess_spawn(&options, &process) != 0) {
        watcher_git_output_cleanup(output);
        return WATCHER_GIT_SUPERVISION_FAILED;
    }

    bool cancelled = false;
    bool deadline_expired = false;
    bool output_exceeded = false;
    bool publication_conflict = false;
    cbm_mutex_lock(&w->projects_lock);
    if (state->active_git) {
        publication_conflict = true;
        cancelled = cbm_subprocess_request_cancel(process);
    } else {
        state->active_git = process;
    }
    if (!cancelled && (atomic_load_explicit(&w->stopped, memory_order_acquire) ||
                       !atomic_load_explicit(&state->registered, memory_order_acquire))) {
        cancelled = cbm_subprocess_request_cancel(process);
    }
    cbm_mutex_unlock(&w->projects_lock);

    uint64_t started_at = cbm_now_ms();
    cbm_proc_result_t result = {0};
    for (;;) {
        uint64_t now = cbm_now_ms();
        if (!cancelled && (atomic_load_explicit(&w->stopped, memory_order_acquire) ||
                           !atomic_load_explicit(&state->registered, memory_order_acquire))) {
            cancelled = cbm_subprocess_request_cancel(process);
        }
        if (!cancelled && now - started_at >= WATCHER_GIT_DEADLINE_MS) {
            deadline_expired = true;
            cancelled = cbm_subprocess_request_cancel(process);
        }
        if (!cancelled && output_limit > 0) {
            int64_t observed_size = cbm_file_size(output->path);
            if (observed_size > 0 && (uint64_t)observed_size > output_limit) {
                output_exceeded = true;
                cancelled = cbm_subprocess_request_cancel(process);
            }
        }
        cbm_proc_poll_t polled = cbm_subprocess_poll(process, &result);
        if (polled == CBM_PROC_POLL_TERMINAL) {
            break;
        }
        if (polled == CBM_PROC_POLL_ERROR) {
            /* Valid owned handles have no ERROR transition. Fail closed, but
             * continue polling so the contained tree is never abandoned. */
            cancelled = cbm_subprocess_request_cancel(process) || cancelled;
        }
        cbm_usleep(WATCHER_GIT_POLL_US);
    }

    cbm_mutex_lock(&w->projects_lock);
    if (state->active_git == process) {
        state->active_git = NULL;
    }
    cbm_mutex_unlock(&w->projects_lock);
    cbm_subprocess_destroy(process);

    watcher_git_status_t status = WATCHER_GIT_OK;
    if (publication_conflict || !result.tree_quiesced || result.supervision_failed) {
        status = WATCHER_GIT_SUPERVISION_FAILED;
    } else if (output_exceeded) {
        status = WATCHER_GIT_OUTPUT_LIMIT;
    } else if (deadline_expired) {
        status = WATCHER_GIT_DEADLINE;
    } else if (result.cancellation_requested || cancelled) {
        status = WATCHER_GIT_CANCELLED;
    } else if (result.outcome != CBM_PROC_CLEAN) {
        status = WATCHER_GIT_COMMAND_FAILED;
    }

    if (status == WATCHER_GIT_OK && output_limit > 0) {
        int64_t final_size = cbm_file_size(output->path);
        if (final_size < 0) {
            status = WATCHER_GIT_SUPERVISION_FAILED;
        } else if ((uint64_t)final_size > output_limit) {
            status = WATCHER_GIT_OUTPUT_LIMIT;
        } else {
            output->size = (size_t)final_size;
        }
    }
    if (status != WATCHER_GIT_OK) {
        watcher_git_output_cleanup(output);
    }
    if (status == WATCHER_GIT_DEADLINE || status == WATCHER_GIT_OUTPUT_LIMIT ||
        status == WATCHER_GIT_SUPERVISION_FAILED) {
        cbm_log_warn("watcher.git.failed", "project", state->project_name, "reason",
                     status == WATCHER_GIT_DEADLINE
                         ? "deadline"
                         : (status == WATCHER_GIT_OUTPUT_LIMIT ? "output_limit" : "supervision"));
    }
    return status;
}

static watcher_git_status_t git_repo_status(cbm_watcher_t *w, project_state_t *state) {
    const char *argv[] = {"git", "-C", state->root_path, "rev-parse", "--git-dir", NULL};
    return watcher_git_run(w, state, argv, 0, NULL);
}

static watcher_git_status_t git_head(cbm_watcher_t *w, project_state_t *state, char *out,
                                     size_t out_size) {
    if (!out || out_size < 2) {
        return WATCHER_GIT_SUPERVISION_FAILED;
    }
    out[0] = '\0';
    const char *argv[] = {"git", "-C", state->root_path, "rev-parse", "HEAD", NULL};
    watcher_git_output_t output;
    watcher_git_status_t status = watcher_git_run(w, state, argv, WATCHER_GIT_HEAD_MAX, &output);
    if (status != WATCHER_GIT_OK) {
        return status;
    }
    FILE *file = cbm_fopen(output.path, "rb");
    bool read = file && fgets(out, (int)out_size, file) != NULL;
    int extra = file ? fgetc(file) : EOF;
    if (file) {
        read = fclose(file) == 0 && read;
    }
    watcher_git_output_cleanup(&output);
    if (!read || extra != EOF) {
        out[0] = '\0';
        return WATCHER_GIT_SUPERVISION_FAILED;
    }
    size_t len = strlen(out);
    while (len > 0 && (out[len - SKIP_ONE] == '\n' || out[len - SKIP_ONE] == '\r')) {
        out[--len] = '\0';
    }
    return len > 0 ? WATCHER_GIT_OK : WATCHER_GIT_COMMAND_FAILED;
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
static watcher_git_status_t git_dirty_signature(cbm_watcher_t *w, project_state_t *state,
                                                uint64_t *signature_out) {
    if (!signature_out) {
        return WATCHER_GIT_SUPERVISION_FAILED;
    }
    *signature_out = 0;
    const char *status_argv[] = {"git",    "--no-optional-locks", "-C",    state->root_path,
                                 "status", "--porcelain",         "-uall", "-z",
                                 NULL};
    watcher_git_output_t output;
    watcher_git_status_t status =
        watcher_git_run(w, state, status_argv, WATCHER_GIT_OUTPUT_MAX, &output);
    if (status != WATCHER_GIT_OK) {
        return status;
    }
    FILE *fp = cbm_fopen(output.path, "rb");
    if (!fp) {
        watcher_git_output_cleanup(&output);
        return WATCHER_GIT_SUPERVISION_FAILED;
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
                h = sig_fold_path_stat(h, state->root_path, entry + 3);
            }
        }
        elen = 0;
        overflow = false;
        if (c == EOF) {
            break;
        }
    }
    bool parsed = !ferror(fp) && fclose(fp) == 0;
    watcher_git_output_cleanup(&output);
    if (!parsed) {
        return WATCHER_GIT_SUPERVISION_FAILED;
    }

#if !defined(_WIN32)
    /* `submodule foreach` necessarily evaluates its final fixed argument in
     * Git's documented POSIX-shell environment. No repository/user text is
     * interpolated into that argument; the outer process is still argv-spawned
     * and its complete descendant tree remains supervised. */
    const char *submodule_argv[] = {
        "git",
        "--no-optional-locks",
        "-C",
        state->root_path,
        "submodule",
        "foreach",
        "--quiet",
        "--recursive",
        "git status --porcelain -uall 2>/dev/null | sed -e \"s@^\\(..\\) @\\1 $displaypath/@\"",
        NULL,
    };
    watcher_git_status_t submodule_status =
        watcher_git_run(w, state, submodule_argv, WATCHER_GIT_OUTPUT_MAX, &output);
    if (submodule_status == WATCHER_GIT_OK) {
        fp = cbm_fopen(output.path, "rb");
        if (!fp) {
            watcher_git_output_cleanup(&output);
            return WATCHER_GIT_SUPERVISION_FAILED;
        }
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
                h = sig_fold_path_stat(h, state->root_path, line + 3);
            }
        }
        parsed = !ferror(fp) && fclose(fp) == 0;
        watcher_git_output_cleanup(&output);
        if (!parsed) {
            return WATCHER_GIT_SUPERVISION_FAILED;
        }
    } else if (submodule_status != WATCHER_GIT_COMMAND_FAILED) {
        return submodule_status;
    }
#endif

    *signature_out = any ? (h ? h : 1) : 0; /* reserve 0 for "clean" */
    return WATCHER_GIT_OK;
}

/* Count tracked files via git ls-files */
static watcher_git_status_t git_file_count(cbm_watcher_t *w, project_state_t *state,
                                           int *count_out) {
    if (!count_out) {
        return WATCHER_GIT_SUPERVISION_FAILED;
    }
    *count_out = 0;
    const char *argv[] = {"git", "-C", state->root_path, "ls-files", "-z", NULL};
    watcher_git_output_t output;
    watcher_git_status_t status = watcher_git_run(w, state, argv, WATCHER_GIT_OUTPUT_MAX, &output);
    if (status != WATCHER_GIT_OK) {
        return status;
    }
    FILE *fp = cbm_fopen(output.path, "rb");
    if (!fp) {
        watcher_git_output_cleanup(&output);
        return WATCHER_GIT_SUPERVISION_FAILED;
    }

    /* NUL mode is unambiguous even for tracked filenames containing newlines. */
    int count = 0;
    char buf[CBM_SZ_1K];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (buf[i] == '\0' && count < INT_MAX) {
                count++;
            }
        }
    }
    if (ferror(fp) || fclose(fp) != 0) {
        watcher_git_output_cleanup(&output);
        return WATCHER_GIT_SUPERVISION_FAILED;
    }
    watcher_git_output_cleanup(&output);
    *count_out = count;
    return WATCHER_GIT_OK;
}

/* ── Project state lifecycle ────────────────────────────────────── */

static void state_free(project_state_t *s);

static project_state_t *state_new(const char *name, const char *root_path) {
    project_state_t *s = calloc(CBM_ALLOC_ONE, sizeof(*s));
    if (!s) {
        return NULL;
    }
    s->project_name = strdup(name);
    s->root_path = strdup(root_path);
    if (!s->project_name || !s->root_path) {
        state_free(s);
        return NULL;
    }
    atomic_init(&s->registered, true);
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

static bool watcher_unlink_cached_file(const char *project_name, const char *path,
                                       const char *artifact) {
    if (cbm_unlink(path) == 0 || errno == ENOENT) {
        return true;
    }
    int unlink_errno = errno;
    char errno_text[CBM_SZ_32];
    snprintf(errno_text, sizeof(errno_text), "%d", unlink_errno);
    cbm_log_warn("watcher.root_prune_delete_failed", "project", project_name, "artifact", artifact,
                 "path", path, "errno", errno_text);
    return false;
}

static bool delete_cached_project_db(const char *project_name) {
    if (!cbm_validate_project_name(project_name)) {
        return false;
    }

    const char *cache_dir = cbm_resolve_cache_dir();
    if (!cache_dir) {
        return false;
    }

    size_t cache_length = strlen(cache_dir);
    size_t project_length = strlen(project_name);
    if (cache_length > SIZE_MAX - project_length - sizeof("/.db")) {
        return false;
    }
    size_t path_capacity = cache_length + project_length + sizeof("/.db");
    char *path = malloc(path_capacity);
    char *sidecar = malloc(path_capacity + sizeof("-wal"));
    if (!path || !sidecar) {
        free(path);
        free(sidecar);
        return false;
    }
    int written = snprintf(path, path_capacity, "%s/%s.db", cache_dir, project_name);
    bool formatted = written > 0 && (size_t)written < path_capacity;
    bool removed = formatted && watcher_unlink_cached_file(project_name, path, "database");
    /* If the main DB could not be removed, preserve WAL/SHM: together they
     * may be the only recoverable committed generation. */
    if (removed) {
        written = snprintf(sidecar, path_capacity + sizeof("-wal"), "%s-wal", path);
        removed = written > 0 && (size_t)written < path_capacity + sizeof("-wal") &&
                  watcher_unlink_cached_file(project_name, sidecar, "wal");
    }
    if (removed) {
        written = snprintf(sidecar, path_capacity + sizeof("-wal"), "%s-shm", path);
        removed = written > 0 && (size_t)written < path_capacity + sizeof("-wal") &&
                  watcher_unlink_cached_file(project_name, sidecar, "shm");
    }
    free(path);
    free(sidecar);
    return removed;
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
    cbm_mutex_init(&w->coordination_lock);
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
    cbm_mutex_lock(&w->coordination_lock);
    cbm_mutex_lock(&w->projects_lock);
    cbm_ht_foreach(w->projects, free_state_entry, NULL);
    cbm_ht_free(w->projects);
    for (int i = 0; i < w->pending_free_count; i++) {
        state_free(w->pending_free[i]);
    }
    free(w->pending_free);
    cbm_mutex_unlock(&w->projects_lock);
    cbm_mutex_unlock(&w->coordination_lock);
    cbm_mutex_destroy(&w->projects_lock);
    cbm_mutex_destroy(&w->coordination_lock);
    free(w);
}

void cbm_watcher_set_project_mutation_guard(cbm_watcher_t *w,
                                            cbm_watcher_project_mutation_begin_fn begin,
                                            cbm_watcher_project_mutation_end_fn end,
                                            cbm_watcher_project_pruned_fn pruned, void *context) {
    if (!w) {
        return;
    }
    cbm_mutex_lock(&w->coordination_lock);
    bool valid = begin && end;
    w->mutation_begin = valid ? begin : NULL;
    w->mutation_end = valid ? end : NULL;
    w->project_pruned = valid ? pruned : NULL;
    w->mutation_context = valid ? context : NULL;
    cbm_mutex_unlock(&w->coordination_lock);
}

/* ── Watch list management ──────────────────────────────────────── */

bool cbm_watcher_watch(cbm_watcher_t *w, const char *project_name, const char *root_path) {
    if (!w || !project_name || !project_name[0] || !root_path || !root_path[0] ||
        atomic_load_explicit(&w->stopped, memory_order_acquire)) {
        return false;
    }

    project_state_t *s = state_new(project_name, root_path);
    if (!s) {
        cbm_log_warn("watcher.watch.oom", "project", project_name, "path", root_path);
        return false;
    }

    cbm_mutex_lock(&w->projects_lock);
    if (atomic_load_explicit(&w->stopped, memory_order_acquire)) {
        cbm_mutex_unlock(&w->projects_lock);
        state_free(s);
        return false;
    }
    project_state_t *old = cbm_ht_get(w->projects, project_name);
    if (old && strcmp(old->root_path, s->root_path) == 0) {
        /* Multiple daemon sessions may subscribe to the same canonical
         * project/root. Re-registering that identical watch must not discard
         * its git baseline, pending dirty signature, or immediate-touch state. */
        cbm_mutex_unlock(&w->projects_lock);
        state_free(s);
        return true;
    }
    if (old) {
        /* A poll snapshot may still be using the old state, including while
         * its index callback calls back into this function. Queue it before
         * removing the table entry; on OOM, preserve the existing watch. */
        if (!defer_state_free(w, old)) {
            cbm_mutex_unlock(&w->projects_lock);
            state_free(s);
            return false;
        }
        cbm_ht_delete(w->projects, project_name);
    }
    cbm_ht_set(w->projects, s->project_name, s);
    bool registered = cbm_ht_get(w->projects, project_name) == s;
    if (!registered && old) {
        /* Restore the original table entry and remove the deferred-free slot.
         * The lock keeps this rollback invisible to poll/watch callers. */
        cbm_ht_set(w->projects, old->project_name, old);
        if (cbm_ht_get(w->projects, project_name) == old) {
            w->pending_free[--w->pending_free_count] = NULL;
        } else {
            atomic_store_explicit(&old->registered, false, memory_order_release);
            if (old->active_git) {
                (void)cbm_subprocess_request_cancel(old->active_git);
            }
        }
    }
    if (registered && old) {
        atomic_store_explicit(&old->registered, false, memory_order_release);
        if (old->active_git) {
            (void)cbm_subprocess_request_cancel(old->active_git);
        }
    }
    cbm_mutex_unlock(&w->projects_lock);
    if (!registered) {
        state_free(s);
        cbm_log_warn("watcher.watch.failed", "project", project_name, "reason", "registration");
        return false;
    }
    cbm_log_info("watcher.watch", "project", project_name, "path", root_path);
    return true;
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
        atomic_store_explicit(&s->registered, false, memory_order_release);
        if (s->active_git) {
            (void)cbm_subprocess_request_cancel(s->active_git);
        }
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
static bool init_baseline(cbm_watcher_t *w, project_state_t *s) {
    struct stat st;
    if (stat(s->root_path, &st) != 0) {
        cbm_log_warn("watcher.root_gone", "project", s->project_name, "path", s->root_path);
        s->baseline_done = true;
        s->is_git = false;
        return true;
    }

    watcher_git_status_t repository_status = git_repo_status(w, s);
    if (repository_status != WATCHER_GIT_OK && repository_status != WATCHER_GIT_COMMAND_FAILED) {
        return false;
    }
    s->is_git = repository_status == WATCHER_GIT_OK;
    s->baseline_done = true;

    if (s->is_git) {
        watcher_git_status_t head_status = git_head(w, s, s->last_head, sizeof(s->last_head));
        if (head_status != WATCHER_GIT_OK && head_status != WATCHER_GIT_COMMAND_FAILED) {
            s->baseline_done = false;
            return false;
        }
        /* last_dirty_sig stays 0 ("clean known"): a tree that is ALREADY
         * dirty at baseline reindexes once on the first poll — the watcher
         * cannot know whether that state made it into the DB (e.g. server
         * restart with a stale artifact). At-least-once, then the signature
         * gates further polls (#937). */
        int file_count = 0;
        watcher_git_status_t file_count_status = git_file_count(w, s, &file_count);
        if (file_count_status != WATCHER_GIT_OK &&
            file_count_status != WATCHER_GIT_COMMAND_FAILED) {
            s->baseline_done = false;
            return false;
        }
        s->file_count = file_count;
        s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count);
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "git", "files",
                     s->file_count > 0 ? "yes" : "0");
    } else {
        cbm_log_info("watcher.baseline", "project", s->project_name, "strategy", "none");
    }

    s->next_poll_ns = now_ns() + ((int64_t)s->interval_ms * US_PER_MS);
    return true;
}

/* Check if a project has changes. Returns true if reindex needed.
 * Must NOT mutate the committed baselines (last_head, last_dirty_sig):
 * poll_project commits them only after a SUCCESSFUL reindex so that
 * busy-skips and failed runs retry instead of silently losing the change
 * (#937). Observations are staged in the pending_* fields. */
static bool check_changes(cbm_watcher_t *w, project_state_t *s, bool *changed_out) {
    if (!changed_out) {
        return false;
    }
    *changed_out = false;
    if (!s->is_git) {
        return true;
    }

    bool changed = false;

    /* Check HEAD movement (commit, checkout, pull) */
    s->pending_head[0] = '\0';
    char head[CBM_SZ_128] = {0};
    watcher_git_status_t head_status = git_head(w, s, head, sizeof(head));
    if (head_status == WATCHER_GIT_OK) {
        if (s->last_head[0] == '\0') {
            /* First observed HEAD: adopt as baseline, not a change. */
            snprintf(s->last_head, sizeof(s->last_head), "%s", head);
        } else if (strcmp(head, s->last_head) != 0) {
            changed = true;
        }
        snprintf(s->pending_head, sizeof(s->pending_head), "%s", head);
    } else if (head_status != WATCHER_GIT_COMMAND_FAILED) {
        return false;
    }

    /* Working tree: reindex only when the DIRTY STATE ITSELF changed —
     * a persistently dirty tree polled while idle must not re-trigger
     * full reindex/write cycles (#937 write amplification). */
    uint64_t sig = 0;
    watcher_git_status_t signature_status = git_dirty_signature(w, s, &sig);
    if (signature_status == WATCHER_GIT_COMMAND_FAILED) {
        /* The repository may have been removed between baseline and poll.
         * Preserve committed observations and wait for a later clean probe. */
        return true;
    }
    if (signature_status != WATCHER_GIT_OK) {
        return false;
    }
    if (sig != s->last_dirty_sig) {
        changed = true;
    }
    s->pending_dirty_sig = sig;

    *changed_out = changed;
    return true;
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

    char *project_name = cbm_strdup(s->project_name);
    char *root_path = cbm_strdup(s->root_path);
    if (!project_name || !root_path) {
        free(project_name);
        free(root_path);
        return;
    }

    bool removed = false;
    cbm_mutex_lock(&w->coordination_lock);
    bool mutation_acquired =
        !w->mutation_begin || w->mutation_begin(w->mutation_context, project_name);
    if (!mutation_acquired) {
        cbm_mutex_unlock(&w->coordination_lock);
        free(project_name);
        free(root_path);
        return;
    }
    cbm_mutex_lock(&w->projects_lock);
    project_state_t *current = cbm_ht_get(w->projects, project_name);
    /* Deferred free (same discipline as cbm_watcher_unwatch): this state
     * is referenced by the poll_once snapshot iterating us. On OOM the
     * watch stays registered and pruning retries on the next cycle. Re-stat
     * under the mutation lease so a root restored after the poll snapshot is
     * never pruned. */
    int stat_errno = 0;
    uint64_t now_ms = cbm_now_ms();
    bool still_eligible =
        current == s && strcmp(current->root_path, root_path) == 0 &&
        current->missing_root_count >= MISSING_ROOT_DELETE_AFTER && current->first_missing_ms > 0 &&
        now_ms - current->first_missing_ms >= (uint64_t)prune_grace_s() * CBM_MSEC_PER_SEC &&
        root_status(root_path, &stat_errno) == ROOT_MISSING;
    if (still_eligible && defer_state_free(w, s)) {
        if (delete_cached_project_db(project_name)) {
            atomic_store_explicit(&s->registered, false, memory_order_release);
            cbm_ht_delete(w->projects, project_name);
            removed = true;
        } else {
            /* The state stays registered; undo the just-added deferred-free
             * entry so the next poll can safely retry the failed deletion. */
            w->pending_free[--w->pending_free_count] = NULL;
        }
    }
    cbm_mutex_unlock(&w->projects_lock);

    if (removed && w->project_pruned) {
        w->project_pruned(w->mutation_context, project_name);
    }
    if (w->mutation_begin) {
        w->mutation_end(w->mutation_context, project_name);
    }
    cbm_mutex_unlock(&w->coordination_lock);

    if (removed) {
        cbm_log_info("watcher.root_pruned", "project", project_name);
    }
    free(project_name);
    free(root_path);
}

static void poll_project(const char *key, void *val, void *ud) {
    (void)key;
    poll_ctx_t *ctx = ud;
    project_state_t *s = val;
    if (!s || !atomic_load_explicit(&s->registered, memory_order_acquire)) {
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
        (void)init_baseline(ctx->w, s);
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
    bool changed = false;
    if (!check_changes(ctx->w, s, &changed)) {
        return;
    }
    if (!changed) {
        s->next_poll_ns = ctx->now + ((int64_t)s->interval_ms * US_PER_MS);
        return;
    }

    /* Trigger reindex */
    if (!atomic_load_explicit(&s->registered, memory_order_acquire)) {
        /* Git inspection can take long enough for the final owning daemon
         * session to disconnect. Unwatch tombstones the snapshot state; do
         * not admit an index callback after that ownership boundary. */
        return;
    }
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
            int file_count = 0;
            if (git_file_count(ctx->w, s, &file_count) == WATCHER_GIT_OK) {
                s->file_count = file_count;
                s->interval_ms = cbm_watcher_poll_interval_ms(s->file_count);
            }
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

static void cancel_active_git_entry(const char *key, void *value, void *user_data) {
    (void)key;
    (void)user_data;
    project_state_t *state = value;
    if (state && state->active_git) {
        (void)cbm_subprocess_request_cancel(state->active_git);
    }
}

void cbm_watcher_stop(cbm_watcher_t *w) {
    if (w) {
        atomic_store_explicit(&w->stopped, 1, memory_order_release);
        cbm_mutex_lock(&w->projects_lock);
        cbm_ht_foreach(w->projects, cancel_active_git_entry, NULL);
        for (int i = 0; i < w->pending_free_count; i++) {
            project_state_t *state = w->pending_free[i];
            if (state && state->active_git) {
                (void)cbm_subprocess_request_cancel(state->active_git);
            }
        }
        cbm_mutex_unlock(&w->projects_lock);
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
