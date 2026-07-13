/*
 * index_supervisor.c — see index_supervisor.h.
 */
#include "index_supervisor.h"

#include "foundation/compat.h"    /* cbm_setenv, cbm_unsetenv */
#include "foundation/compat_fs.h" /* cbm_mkdir_p, cbm_fopen */
#include "foundation/log.h"
#include "foundation/platform.h" /* cbm_resolve_cache_dir */
#include "foundation/profile.h"  /* cbm_profile_active (keep worker log under CBM_PROFILE) */
#include "ui/http_server.h"      /* cbm_http_server_resolve_binary_path */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <process.h> /* _getpid */
#define cbm_getpid _getpid
#else
#include <unistd.h> /* getpid */
#define cbm_getpid getpid
#endif

/* ── Worker-role state ────────────────────────────────────────────── */

static bool g_worker_active = false;
static char g_worker_response_out[1024] = {0};

void cbm_index_set_worker_role(bool is_worker, const char *response_out) {
    g_worker_active = is_worker;
    if (response_out && response_out[0]) {
        snprintf(g_worker_response_out, sizeof(g_worker_response_out), "%s", response_out);
    } else {
        g_worker_response_out[0] = '\0';
    }
}

bool cbm_index_worker_active(void) {
    return g_worker_active;
}

const char *cbm_index_worker_response_out(void) {
    return g_worker_response_out[0] ? g_worker_response_out : NULL;
}

/* Test hook (#845): counts spawn ATTEMPTS (entry to cbm_index_spawn_worker),
 * including ones that fail to resolve the self binary — an embedder must never
 * even try to spawn. */
static int g_spawn_count = 0;

int cbm_index_supervisor_spawn_count(void) {
    return g_spawn_count;
}

/* Test hook: counts SINGLE-THREADED spawns. Production recovery is parallel-
 * only (there are no sequential production runs); this must stay ZERO on
 * every supervised path — any nonzero count means a recovery/probe regressed
 * to the sequential crawl that ground an 81k-file TS corpus for hours. */
static int g_spawn_st_count = 0;

int cbm_index_supervisor_spawn_st_count(void) {
    return g_spawn_st_count;
}

/* #845: opt-in host mark — see the header. Set once from the real binary's
 * main(); embedders never set it, so should_wrap() stays false for them. */
static bool g_host_marked = false;

void cbm_index_supervisor_mark_host(void) {
    g_host_marked = true;
}

bool cbm_index_supervisor_should_wrap(void) {
    if (!g_host_marked) {
        return false; /* embedder (#845): never spawn `<self> cli --index-worker` */
    }
    if (g_worker_active) {
        return false; /* I am the worker — run in-process, never re-supervise */
    }
    const char *sv = getenv("CBM_INDEX_SUPERVISOR");
    if (sv && strcmp(sv, "0") == 0) {
        return false; /* kill switch → in-process */
    }
    return true;
}

/* Quiet-timeout (ms) for a supervised worker: killed + reported as a hang if it
 * emits no NEW log line within the window. This is a NO-PROGRESS timeout — every
 * completed log line the worker tails (per-batch parallel.extract.progress every
 * 10 files, plus each pass boundary) resets it — NOT a total-time cap, so a large
 * repo that keeps making progress is never falsely killed. Default: 15 min (a
 * genuinely stuck file emits nothing, so this fires only on a real hang). The
 * CBM_INDEX_WORKER_TIMEOUT_S override (seconds → ms) tightens it for tests. */
static int worker_quiet_timeout_ms(void) {
    enum { DEFAULT_QUIET_TIMEOUT_MS = 900000 }; /* 15 min with no progress */
    const char *e = getenv("CBM_INDEX_WORKER_TIMEOUT_S");
    if (e && e[0]) {
        long s = atol(e);
        if (s > 0) {
            return (int)(s * 1000);
        }
    }
    return DEFAULT_QUIET_TIMEOUT_MS;
}

/* Read an entire file into a heap string (NUL-terminated). NULL on error. */
static char *slurp_file(const char *path) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        (void)fclose(f);
        return NULL;
    }
    long n = ftell(f);
    if (n < 0) {
        (void)fclose(f);
        return NULL;
    }
    (void)fseek(f, 0, SEEK_SET);
    char *buf = (char *)malloc((size_t)n + 1);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t rd = fread(buf, 1, (size_t)n, f);
    (void)fclose(f);
    buf[rd] = '\0';
    return buf;
}

/* Resolve a per-run temp path <cache_dir>/logs/.worker-<pid><suffix>. */
static void worker_tmp_path(char *out, size_t out_sz, int pid, const char *suffix) {
    const char *cdir = cbm_resolve_cache_dir();
    if (cdir && cdir[0]) {
        char dir[900];
        snprintf(dir, sizeof(dir), "%s/logs", cdir);
        cbm_mkdir_p(dir, 0755);
        snprintf(out, out_sz, "%s/.worker-%d%s", dir, pid, suffix);
    } else {
        snprintf(out, out_sz, ".worker-%d%s", pid, suffix);
    }
}

int cbm_index_spawn_worker(const char *args_json, bool single_thread, const char *marker_file,
                           const char *quarantine_file, cbm_index_worker_result_t *result) {
    g_spawn_count++; /* test hook (#845) — see cbm_index_supervisor_spawn_count */
    if (single_thread) {
        g_spawn_st_count++; /* test hook — must stay 0: recovery is parallel-only */
    }
    result->outcome = CBM_PROC_SPAWN_FAILED;
    result->exit_code = -1;
    result->term_signal = 0;
    result->response = NULL;

    char self[1024] = {0};
    if (!cbm_http_server_resolve_binary_path(NULL, self, sizeof(self)) || !self[0]) {
        cbm_log_warn("index.supervisor.no_self_path", "action", "degrade_in_process");
        return -1;
    }

    int pid = (int)cbm_getpid();
    char resp_path[1024];
    char log_path[1024];
    worker_tmp_path(resp_path, sizeof(resp_path), pid, ".response");
    worker_tmp_path(log_path, sizeof(log_path), pid, ".log");
    (void)remove(resp_path); /* clear any stale file */

    /* No --progress: the worker's DEFAULT structured logging already provides the
     * no-progress heartbeat (INFO parallel.extract.progress every 10 files + each
     * pass boundary — all newline-terminated → tailed → reset the quiet-timeout).
     * --progress would be strictly worse here: it installs a REPLACE-mode sink that
     * suppresses those default lines and emits per-file extraction as a carriage-
     * return in-place update (no trailing '\n'), which cbm_tail_log does not count
     * as progress. (It would not corrupt the response either — that goes to the
     * separate --response-out file, not stdout.) */
    const char *argv[8];
    int n = 0;
    argv[n++] = self;
    argv[n++] = "cli";
    argv[n++] = "--index-worker";
    argv[n++] = "index_repository";
    argv[n++] = args_json;
    argv[n++] = "--response-out";
    argv[n++] = resp_path;
    argv[n] = NULL;

    /* Recovery-run probe knobs → inherited env for the child. Spawns are
     * sequential, so mutating the parent's environment around a single spawn is
     * safe. Set only the requested knobs; unset them all again after reaping so
     * a later attempt (or the caller) starts from a clean environment. */
    if (single_thread) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    }
    if (marker_file && marker_file[0]) {
        cbm_setenv("CBM_INDEX_MARKER_FILE", marker_file, 1);
    }
    if (quarantine_file && quarantine_file[0]) {
        cbm_setenv("CBM_INDEX_QUARANTINE_FILE", quarantine_file, 1);
    }

    cbm_proc_opts_t opts = {0};
    opts.bin = self;
    opts.argv = argv;
    opts.log_file = log_path;
    opts.quiet_timeout_ms = worker_quiet_timeout_ms();
    /* We manage log deletion ourselves after reaping (below): keep it on failure
     * for post-mortem, delete it only on a clean run. See the observability
     * note at the reap site. */
    opts.delete_log_on_exit = false;

    cbm_proc_result_t r;
    int run_rc = cbm_subprocess_run(&opts, &r);

    if (single_thread) {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    if (marker_file && marker_file[0]) {
        cbm_unsetenv("CBM_INDEX_MARKER_FILE");
    }
    if (quarantine_file && quarantine_file[0]) {
        cbm_unsetenv("CBM_INDEX_QUARANTINE_FILE");
    }

    if (run_rc != 0) {
        (void)remove(resp_path);
        (void)remove(log_path); /* empty/partial log from a failed spawn — nothing to keep */
        cbm_log_warn("index.supervisor.spawn_failed", "action", "degrade_in_process");
        return -1;
    }

    result->outcome = r.outcome;
    result->exit_code = r.exit_code;
    result->term_signal = r.term_signal;
    if (r.outcome == CBM_PROC_CLEAN) {
        result->response = slurp_file(resp_path);
    }
    (void)remove(resp_path);

    char sig[16];
    char exit_buf[16];
    snprintf(sig, sizeof(sig), "%d", r.term_signal);
    snprintf(exit_buf, sizeof(exit_buf), "%d", r.exit_code);
    cbm_log_info("index.supervisor.reap", "outcome", cbm_proc_outcome_str(r.outcome), "exit_code",
                 exit_buf, "signal", sig);

    /* Observability: on a CLEAN run the worker log is noise → delete it. On
     * ANY failure keep it and surface its path + raw exit code, so the worker's own
     * stdout/stderr (pipeline logs, any assert/abort text, the exact exit code) is
     * available post-mortem instead of vanishing. Previously the log was ALWAYS
     * deleted and only outcome+signal were logged, so a worker that exited non-zero
     * left nothing to diagnose — the CI blind spot that hid this bug (a mangled JSON
     * arg → "repo_path is required" exit) behind a generic "crashed on a file".
     *
     * Exception: under CBM_PROFILE the log IS the deliverable — the worker's
     * msg=prof pass/sub-phase report is only written there, and deleting it on
     * success made profiling clean runs impossible. Keep it and say where it is. */
    if (r.outcome == CBM_PROC_CLEAN && !cbm_profile_active) {
        (void)remove(log_path);
    } else if (r.outcome == CBM_PROC_CLEAN) {
        cbm_log_info("index.supervisor.profile_log", "log", log_path);
    } else {
        cbm_log_warn("index.supervisor.worker_failed", "outcome", cbm_proc_outcome_str(r.outcome),
                     "exit_code", exit_buf, "log", log_path);
    }
    return 0;
}

void cbm_index_worker_result_free(cbm_index_worker_result_t *result) {
    if (result) {
        free(result->response);
        result->response = NULL;
    }
}
