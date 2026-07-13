/*
 * diagnostics.c — Periodic diagnostics file writer.
 *
 * Writes JSON to /tmp/cbm-diagnostics-<pid>.json every 5 seconds.
 * Atomic: writes .tmp then renames to avoid partial reads.
 */
#include "foundation/constants.h"
#include "foundation/diagnostics.h"
#include <stdatomic.h>
#include "foundation/mem.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <mimalloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <dirent.h>
#include <unistd.h>
#endif

/* ── Globals ─────────────────────────────────────────────────────── */

cbm_query_stats_t g_query_stats = {0};
static atomic_int g_diag_stop = 0;
static cbm_thread_t g_diag_thread;
static bool g_diag_started = false;
static time_t g_start_time = 0;
static char g_diag_path[CBM_SZ_256] = "";
/* Persistent NDJSON time-series (the memory TRAJECTORY users send us to diagnose
 * slow leaks like #581). Unlike g_diag_path (a latest-snapshot file overwritten
 * every interval and deleted on stop), this is appended-to and KEPT on exit. */
static char g_diag_ndjson_path[CBM_SZ_256] = "";
static size_t g_diag_ndjson_size = 0;
#define DIAG_NDJSON_CAP_BYTES (8u * 1024u * 1024u) /* rotate to .1 past this */

/* ── Query stats ─────────────────────────────────────────────────── */

void cbm_diag_record_query(long long duration_us, bool is_error) {
    atomic_fetch_add(&g_query_stats.count, 1);
    atomic_fetch_add(&g_query_stats.time_us, duration_us);
    if (is_error) {
        atomic_fetch_add(&g_query_stats.errors, 1);
    }
    /* Update max (lock-free CAS loop) */
    long long old_max = atomic_load(&g_query_stats.max_us);
    while (duration_us > old_max) {
        if (atomic_compare_exchange_weak(&g_query_stats.max_us, &old_max, duration_us)) {
            break;
        }
    }
}

/* ── FD count (platform-specific) ────────────────────────────────── */

static int count_open_fds(void) {
#ifdef __linux__
    struct dirent **entries = NULL;
    int n = scandir("/proc/self/fd", &entries, NULL, NULL);
    if (n < 0) {
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < n; i++) {
        free(entries[i]);
    }
    free(entries);
    return n - PAIR_LEN; /* . and .. */
#elif defined(__APPLE__)
    /* Count via /dev/fd using scandir (MT-safe) */
    struct dirent **entries = NULL;
    int n = scandir("/dev/fd", &entries, NULL, NULL);
    if (n < 0) {
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < n; i++) {
        free(entries[i]);
    }
    free(entries);
    return n - PAIR_LEN; /* . and .. */
#else
    return CBM_NOT_FOUND; /* Not available on Windows */
#endif
}

/* ── Writer ──────────────────────────────────────────────────────── */

#define DIAG_INTERVAL_S 5
#define DIAG_PATH_EXTRA 24 /* ".tmp" + safety margin */

/* Append one compact JSON line to the persistent NDJSON trajectory, rotating to
 * <path>.1 once it passes the cap. Best-effort: a failed append never disrupts
 * the server. The trajectory (a monotonic rss/committed climb over hours) is
 * what reveals a slow leak like #581 — the single latest-snapshot file cannot. */
static void append_trajectory(long uptime, size_t rss, size_t peak_rss, size_t commit,
                              size_t peak_commit, size_t page_faults, int fds, int qcount) {
    if (g_diag_ndjson_path[0] == '\0') {
        return;
    }
    if (g_diag_ndjson_size > DIAG_NDJSON_CAP_BYTES) {
        char rot[sizeof(g_diag_ndjson_path) + DIAG_PATH_EXTRA];
        snprintf(rot, sizeof(rot), "%s.1", g_diag_ndjson_path);
        (void)rename(g_diag_ndjson_path, rot); /* keep one previous generation */
        g_diag_ndjson_size = 0;
    }
    FILE *f = fopen(g_diag_ndjson_path, "a");
    if (!f) {
        return;
    }
    int n = fprintf(f,
                    "{\"uptime_s\":%ld,\"rss\":%zu,\"peak_rss\":%zu,\"committed\":%zu,"
                    "\"peak_committed\":%zu,\"page_faults\":%zu,\"fd\":%d,\"queries\":%d}\n",
                    uptime, rss, peak_rss, commit, peak_commit, page_faults, fds, qcount);
    if (n > 0) {
        g_diag_ndjson_size += (size_t)n;
    }
    (void)fclose(f);
}

static void write_diagnostics(void) {
    /* Collect mimalloc stats */
    size_t elapsed_ms = 0;
    size_t user_ms = 0;
    size_t sys_ms = 0;
    size_t current_rss = 0;
    size_t peak_rss = 0;
    size_t current_commit = 0;
    size_t peak_commit = 0;
    size_t page_faults = 0;
    mi_process_info(&elapsed_ms, &user_ms, &sys_ms, &current_rss, &peak_rss, &current_commit,
                    &peak_commit, &page_faults);

    /* Fallback RSS for ASan builds */
    if (current_rss == 0) {
        current_rss = cbm_mem_rss();
    }

    int fds = count_open_fds();
    time_t now = time(NULL);
    long uptime = (long)(now - g_start_time);

    int qcount = atomic_load(&g_query_stats.count);
    int qerrors = atomic_load(&g_query_stats.errors);
    long long qtime = atomic_load(&g_query_stats.time_us);
    long long qmax = atomic_load(&g_query_stats.max_us);
    long long qavg = qcount > 0 ? qtime / qcount : 0;

    /* Write to .tmp then rename (atomic) */
    char tmp_path[sizeof(g_diag_path) + DIAG_PATH_EXTRA];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", g_diag_path);

    FILE *f = fopen(tmp_path, "w");
    if (!f) {
        return;
    }

    if (fprintf(f,
                "{\n"
                "  \"uptime_s\": %ld,\n"
                "  \"rss_bytes\": %zu,\n"
                "  \"peak_rss_bytes\": %zu,\n"
                "  \"heap_committed_bytes\": %zu,\n"
                "  \"peak_committed_bytes\": %zu,\n"
                "  \"page_faults\": %zu,\n"
                "  \"fd_count\": %d,\n"
                "  \"query_count\": %d,\n"
                "  \"query_errors\": %d,\n"
                "  \"query_total_us\": %lld,\n"
                "  \"query_avg_us\": %lld,\n"
                "  \"query_max_us\": %lld,\n"
                "  \"pid\": %d\n"
                "}\n",
                uptime, current_rss, peak_rss, current_commit, peak_commit, page_faults, fds,
                qcount, qerrors, qtime, qavg, qmax, (int)getpid()) < 0) {
        (void)fclose(f);
        return;
    }
    if (fclose(f) != 0) {
        return;
    }
    (void)rename(tmp_path, g_diag_path);

    /* Also append to the persistent trajectory (kept on exit for users to send). */
    append_trajectory(uptime, current_rss, peak_rss, current_commit, peak_commit, page_faults, fds,
                      qcount);
}

static void *diag_thread_fn(void *arg) {
    (void)arg;
    while (!atomic_load(&g_diag_stop)) {
        write_diagnostics();
        struct timespec ts = {DIAG_INTERVAL_S, 0};
        cbm_nanosleep(&ts, NULL);
    }
    /* Final write before exit */
    write_diagnostics();
    return NULL;
}

/* ── Public API ──────────────────────────────────────────────────── */

bool cbm_diag_start(void) {
    char env_buf[CBM_SZ_32] = "";
    cbm_safe_getenv("CBM_DIAGNOSTICS", env_buf, sizeof(env_buf), NULL);
    if (env_buf[0] == '\0' || (strcmp(env_buf, "1") != 0 && strcmp(env_buf, "true") != 0)) {
        return false;
    }

    g_start_time = time(NULL);
    atomic_store(&g_diag_stop, 0);

    snprintf(g_diag_path, sizeof(g_diag_path), "%s/cbm-diagnostics-%d.json", cbm_tmpdir(),
             (int)getpid());
    snprintf(g_diag_ndjson_path, sizeof(g_diag_ndjson_path), "%s/cbm-diagnostics-%d.ndjson",
             cbm_tmpdir(), (int)getpid());
    g_diag_ndjson_size = 0;

    if (cbm_thread_create(&g_diag_thread, 0, diag_thread_fn, NULL) != 0) {
        return false;
    }

    g_diag_started = true;
    (void)fprintf(stderr,
                  "level=info msg=diagnostics.start snapshot=%s trajectory=%s interval=%ds\n",
                  g_diag_path, g_diag_ndjson_path, DIAG_INTERVAL_S);
    return true;
}

void cbm_diag_stop(void) {
    if (!g_diag_started) {
        return;
    }
    atomic_store(&g_diag_stop, 1);
    cbm_thread_join(&g_diag_thread);
    g_diag_started = false;

    /* Remove the live latest-snapshot file (and its .tmp), but KEEP the
     * trajectory NDJSON so the user can send it after the server exits. */
    cbm_unlink(g_diag_path);
    char tmp_path[sizeof(g_diag_path) + DIAG_PATH_EXTRA];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", g_diag_path);
    cbm_unlink(tmp_path);
    if (g_diag_ndjson_path[0] != '\0') {
        (void)fprintf(stderr, "level=info msg=diagnostics.trajectory_kept path=%s\n",
                      g_diag_ndjson_path);
    }
}
