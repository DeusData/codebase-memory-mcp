/* RED contract for daemon-owned asynchronous index workers. */
#include "test_framework.h"
#include "test_helpers.h"

#include "cli/progress_sink.h"
#include "daemon/bootstrap.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"
#include "foundation/profile.h"
#include "mcp/index_supervisor.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

enum {
    INDEX_SUPERVISOR_TEST_PATH_CAP = 1024,
    /* Worker startup re-hashes the entire executable image for the
     * exact-build fingerprint before its first write. For the ASan
     * test-runner that is a multi-hundred-MB read+SHA-256, which on the
     * 3-core CI runner VMs with cold IO takes several seconds — the CI
     * macOS legs showed workers alive with created-but-EMPTY logs at the
     * old 5 s/3 s deadlines. These are hang guards, not benchmarks: a
     * genuinely wedged worker still fails loudly here, and the failure
     * dumps the (empty) worker log as proof.  */
    INDEX_SUPERVISOR_TEST_TERMINAL_MS = 90000,
    INDEX_SUPERVISOR_TEST_READY_MS = 60000,
    INDEX_SUPERVISOR_TEST_BACKLOG_LINES = 1024,
};

static void index_supervisor_test_pause(void) {
    const struct timespec pause = {0, 10000000L};
    (void)cbm_nanosleep(&pause, NULL);
}

static bool index_supervisor_test_poll_terminal(cbm_index_worker_handle_t *handle,
                                                uint32_t timeout_ms,
                                                const cbm_index_worker_result_t **result_out) {
    uint64_t deadline = cbm_now_ms() + timeout_ms;
    do {
        cbm_index_worker_poll_t state = cbm_index_worker_poll(handle, result_out);
        if (state == CBM_INDEX_WORKER_POLL_TERMINAL) {
            return result_out && *result_out;
        }
        if (state == CBM_INDEX_WORKER_POLL_ERROR) {
            return false;
        }
        index_supervisor_test_pause();
    } while (cbm_now_ms() < deadline);
    return cbm_index_worker_poll(handle, result_out) == CBM_INDEX_WORKER_POLL_TERMINAL;
}

static bool index_supervisor_test_wait_file(cbm_index_worker_handle_t *handle, const char *path,
                                            char *out, size_t out_size) {
    uint64_t deadline = cbm_now_ms() + INDEX_SUPERVISOR_TEST_READY_MS;
    do {
        FILE *file = cbm_fopen(path, "rb");
        if (file) {
            size_t used = fread(out, 1, out_size - 1, file);
            out[used] = '\0';
            (void)fclose(file);
            if (used > 0) {
                return true;
            }
        }
        const cbm_index_worker_result_t *unexpected = NULL;
        if (cbm_index_worker_poll(handle, &unexpected) != CBM_INDEX_WORKER_POLL_RUNNING) {
            return false;
        }
        index_supervisor_test_pause();
    } while (cbm_now_ms() < deadline);
    return false;
}

/* On failure, the worker's own log/marker files carry the exit reason; they
 * are deleted during teardown before the asserts run, so dump them at the
 * failure site or the evidence is gone by the time CI prints the FAIL. */
static void index_supervisor_test_dump(const char *label, const char *path) {
    (void)fprintf(stderr, "  [worker-dump] %s: %s\n", label, path && path[0] ? path : "<none>");
    FILE *file = path && path[0] ? cbm_fopen(path, "rb") : NULL;
    if (!file) {
        (void)fprintf(stderr, "  [worker-dump] (missing or unreadable)\n");
        return;
    }
    char content[4096];
    size_t used = fread(content, 1, sizeof(content) - 1, file);
    content[used] = '\0';
    (void)fclose(file);
    (void)fprintf(stderr, "%s%s", content, used > 0 && content[used - 1] == '\n' ? "" : "\n");
}

static bool index_supervisor_test_wait_file_text(const char *path, const char *needle,
                                                 uint32_t timeout_ms) {
    uint64_t deadline = cbm_now_ms() + timeout_ms;
    do {
        FILE *file = cbm_fopen(path, "rb");
        if (file) {
            char content[4096] = {0};
            size_t used = fread(content, 1, sizeof(content) - 1, file);
            content[used] = '\0';
            (void)fclose(file);
            if (strstr(content, needle)) {
                return true;
            }
        }
        index_supervisor_test_pause();
    } while (cbm_now_ms() < deadline);
    return false;
}

static bool index_supervisor_test_append_log(const char *path, const char *line) {
    FILE *file = cbm_fopen(path, "ab");
    if (!file) {
        return false;
    }
    bool written = fputs(line, file) >= 0 && fflush(file) == 0;
    return fclose(file) == 0 && written;
}

static bool index_supervisor_test_append_terminal_backlog(const char *path) {
    FILE *file = cbm_fopen(path, "ab");
    if (!file) {
        return false;
    }
    bool written =
        fputs("level=info msg=pipeline.discover files=7\n", file) >= 0 &&
        fputs("{\"level\":\"info\",\"event\":\"pass.start\",\"pass\":\"structure\"}\n", file) >= 0;
    for (int i = 0; written && i < INDEX_SUPERVISOR_TEST_BACKLOG_LINES; i++) {
        written = fprintf(file, "terminal-backlog-%04d\n", i) > 0;
    }
    written = written && fputs("terminal-backlog-sentinel\n", file) >= 0 && fflush(file) == 0;
    return fclose(file) == 0 && written;
}

static void index_supervisor_test_cleanup_handle(cbm_index_worker_handle_t *handle) {
    if (!handle) {
        return;
    }
    (void)cbm_index_worker_request_cancel(handle);
    const cbm_index_worker_result_t *result = NULL;
    if (index_supervisor_test_poll_terminal(handle, INDEX_SUPERVISOR_TEST_TERMINAL_MS, &result)) {
        cbm_index_worker_destroy(handle);
    }
}

static void index_supervisor_test_restore_env(const char *name, char *saved) {
    if (saved) {
        (void)cbm_setenv(name, saved, 1);
    } else {
        (void)cbm_unsetenv(name);
    }
    free(saved);
}

static bool index_supervisor_test_owner_file(const char *path) {
#ifdef _WIN32
    return cbm_file_size(path) >= 0;
#else
    struct stat status;
    return path && stat(path, &status) == 0 && S_ISREG(status.st_mode) &&
           (status.st_mode & 0777) == 0600;
#endif
}

typedef struct {
    int delivered;
    bool saw_clean_probe;
    bool saw_request_a;
    bool saw_request_b;
    bool saw_structured_text;
    bool saw_structured_json;
    bool saw_backlog_sentinel;
    int backlog_lines;
    bool render_progress;
} index_supervisor_log_capture_t;

static void index_supervisor_test_capture_log(const char *line, void *context) {
    index_supervisor_log_capture_t *capture = context;
    if (!capture || !line) {
        return;
    }
    capture->delivered++;
    if (strstr(line, "async worker clean probe")) {
        capture->saw_clean_probe = true;
    }
    if (strcmp(line, "request-a-only") == 0) {
        capture->saw_request_a = true;
    }
    if (strcmp(line, "request-b-only") == 0) {
        capture->saw_request_b = true;
    }
    if (strcmp(line, "level=info msg=pipeline.discover files=7") == 0) {
        capture->saw_structured_text = true;
    }
    if (strcmp(line, "{\"level\":\"info\",\"event\":\"pass.start\",\"pass\":\"structure\"}") == 0) {
        capture->saw_structured_json = true;
    }
    if (strncmp(line, "terminal-backlog-", strlen("terminal-backlog-")) == 0) {
        capture->backlog_lines++;
    }
    if (strcmp(line, "terminal-backlog-sentinel") == 0) {
        capture->saw_backlog_sentinel = true;
    }
    if (capture->render_progress) {
        cbm_progress_sink_fn(line);
    }
}

TEST(index_supervisor_worker_argv_requires_exact_build_bound_grammar) {
    const char *captured = cbm_index_supervisor_build_fingerprint();
    ASSERT_NOT_NULL(captured);
    ASSERT_EQ(strlen(captured), CBM_INDEX_WORKER_BUILD_FINGERPRINT_LENGTH);

    char mismatched[CBM_INDEX_WORKER_BUILD_FINGERPRINT_SIZE];
    (void)snprintf(mismatched, sizeof(mismatched), "%s", captured);
    mismatched[0] = mismatched[0] == '0' ? '1' : '0';

    char *valid[] = {
        "test-runner",
        "cli",
        "--index-worker",
        CBM_INDEX_WORKER_BUILD_ARG,
        (char *)captured,
        "index_repository",
        "{\"__cbm_test_worker\":\"hang-tree\"}",
        "--response-out",
        "/tmp/r",
        CBM_INDEX_WORKER_MEMORY_BUDGET_ARG,
        "1024",
        CBM_INDEX_WORKER_SINGLE_THREAD_ARG,
        CBM_INDEX_WORKER_MARKER_ARG,
        "/tmp/m",
        CBM_INDEX_WORKER_QUARANTINE_ARG,
        "/tmp/q",
        NULL,
    };
    cbm_index_worker_invocation_t invocation;
    ASSERT_EQ(cbm_index_worker_parse_process_argv(16, valid, &invocation),
              CBM_INDEX_WORKER_ARGV_VALID);
    ASSERT_EQ(cbm_daemon_process_role(16, valid), CBM_DAEMON_PROCESS_WORKER);
    ASSERT_STR_EQ(invocation.args_json, valid[6]);
    ASSERT_STR_EQ(invocation.response_out, valid[8]);
    ASSERT_EQ(invocation.memory_budget_bytes, 1024);
    ASSERT_TRUE(invocation.single_thread);
    ASSERT_STR_EQ(invocation.marker_file, valid[13]);
    ASSERT_STR_EQ(invocation.quarantine_file, valid[15]);

    char *missing_build[] = {"test-runner",      "cli", "--index-worker",
                             "index_repository", "{}",  "--response-out",
                             "/tmp/r",           NULL};
    char *wrong_build[] = {"test-runner",
                           "cli",
                           "--index-worker",
                           CBM_INDEX_WORKER_BUILD_ARG,
                           mismatched,
                           "index_repository",
                           "{}",
                           "--response-out",
                           "/tmp/r",
                           NULL};
    char *reordered[] = {"test-runner",
                         "cli",
                         "--index-worker",
                         CBM_INDEX_WORKER_BUILD_ARG,
                         (char *)captured,
                         "index_repository",
                         "{}",
                         "--response-out",
                         "/tmp/r",
                         CBM_INDEX_WORKER_QUARANTINE_ARG,
                         "/tmp/q",
                         CBM_INDEX_WORKER_MARKER_ARG,
                         "/tmp/m",
                         NULL};
    char *trailing[] = {"test-runner",
                        "cli",
                        "--index-worker",
                        CBM_INDEX_WORKER_BUILD_ARG,
                        (char *)captured,
                        "index_repository",
                        "{}",
                        "--response-out",
                        "/tmp/r",
                        "unexpected",
                        NULL};
    char *zero_budget[] = {"test-runner",
                           "cli",
                           "--index-worker",
                           CBM_INDEX_WORKER_BUILD_ARG,
                           (char *)captured,
                           "index_repository",
                           "{}",
                           "--response-out",
                           "/tmp/r",
                           CBM_INDEX_WORKER_MEMORY_BUDGET_ARG,
                           "0",
                           NULL};
    char *overflow_budget[] = {"test-runner",
                               "cli",
                               "--index-worker",
                               CBM_INDEX_WORKER_BUILD_ARG,
                               (char *)captured,
                               "index_repository",
                               "{}",
                               "--response-out",
                               "/tmp/r",
                               CBM_INDEX_WORKER_MEMORY_BUDGET_ARG,
                               "184467440737095516160",
                               NULL};
    char *user_value[] = {"test-runner", "cli", "search_code", "--query", "--index-worker", NULL};
    ASSERT_EQ(cbm_index_worker_parse_process_argv(7, missing_build, &invocation),
              CBM_INDEX_WORKER_ARGV_INVALID);
    ASSERT_EQ(cbm_daemon_process_role(7, missing_build), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(cbm_index_worker_parse_process_argv(9, wrong_build, &invocation),
              CBM_INDEX_WORKER_ARGV_BUILD_MISMATCH);
    ASSERT_EQ(cbm_index_worker_parse_process_argv(13, reordered, &invocation),
              CBM_INDEX_WORKER_ARGV_INVALID);
    ASSERT_EQ(cbm_index_worker_parse_process_argv(10, trailing, &invocation),
              CBM_INDEX_WORKER_ARGV_INVALID);
    ASSERT_EQ(cbm_index_worker_parse_process_argv(11, zero_budget, &invocation),
              CBM_INDEX_WORKER_ARGV_INVALID);
    ASSERT_EQ(cbm_daemon_process_role(11, zero_budget), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(cbm_index_worker_parse_process_argv(11, overflow_budget, &invocation),
              CBM_INDEX_WORKER_ARGV_INVALID);
    ASSERT_EQ(cbm_daemon_process_role(11, overflow_budget), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(cbm_index_worker_parse_process_argv(5, user_value, &invocation),
              CBM_INDEX_WORKER_ARGV_INVALID);
    ASSERT_EQ(cbm_daemon_process_role(5, user_value), CBM_DAEMON_PROCESS_INVALID);
    PASS();
}

TEST(index_supervisor_async_jobs_are_isolated_cancellable_and_terminal_cached) {
    const char *captured = cbm_index_supervisor_build_fingerprint();
    ASSERT_NOT_NULL(captured);
    char *worker_argv[] = {
        "test-runner",
        "cli",
        "--index-worker",
        CBM_INDEX_WORKER_BUILD_ARG,
        (char *)captured,
        "index_repository",
        "{\"__cbm_test_worker\":\"hang-tree\"}",
        "--response-out",
        "/tmp/r",
        CBM_INDEX_WORKER_SINGLE_THREAD_ARG,
        CBM_INDEX_WORKER_MARKER_ARG,
        "/tmp/m",
        CBM_INDEX_WORKER_QUARANTINE_ARG,
        "/tmp/q",
        NULL,
    };
    ASSERT_EQ(cbm_daemon_process_role(14, worker_argv), CBM_DAEMON_PROCESS_WORKER);

    char cache[INDEX_SUPERVISOR_TEST_PATH_CAP];
    (void)snprintf(cache, sizeof(cache), "%s/cbm-index-async-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));
    char marker_a[INDEX_SUPERVISOR_TEST_PATH_CAP];
    char marker_b[INDEX_SUPERVISOR_TEST_PATH_CAP];
    char quarantine_a[INDEX_SUPERVISOR_TEST_PATH_CAP];
    char quarantine_b[INDEX_SUPERVISOR_TEST_PATH_CAP];
    (void)snprintf(marker_a, sizeof(marker_a), "%s/marker-a", cache);
    (void)snprintf(marker_b, sizeof(marker_b), "%s/marker-b", cache);
    (void)snprintf(quarantine_a, sizeof(quarantine_a), "%s/quarantine-a", cache);
    (void)snprintf(quarantine_b, sizeof(quarantine_b), "%s/quarantine-b", cache);

    const char *old_cache = getenv("CBM_CACHE_DIR");
    const char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    const char *old_marker = getenv("CBM_INDEX_MARKER_FILE");
    const char *old_quarantine = getenv("CBM_INDEX_QUARANTINE_FILE");
    char *saved_cache = old_cache ? cbm_strdup(old_cache) : NULL;
    char *saved_single = old_single ? cbm_strdup(old_single) : NULL;
    char *saved_marker = old_marker ? cbm_strdup(old_marker) : NULL;
    char *saved_quarantine = old_quarantine ? cbm_strdup(old_quarantine) : NULL;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);
    (void)cbm_setenv("CBM_INDEX_SINGLE_THREAD", "parent-single", 1);
    (void)cbm_setenv("CBM_INDEX_MARKER_FILE", "parent-marker", 1);
    (void)cbm_setenv("CBM_INDEX_QUARANTINE_FILE", "parent-quarantine", 1);

    const char args[] = "{\"__cbm_test_worker\":\"hang-tree\"}";
    cbm_index_worker_handle_t *first = NULL;
    cbm_index_worker_handle_t *second = NULL;
    index_supervisor_log_capture_t first_capture = {0};
    index_supervisor_log_capture_t second_capture = {0};
    uint64_t start_before = cbm_now_ms();
    int first_rc =
        cbm_index_worker_start_with_log(args, 4096, true, marker_a, quarantine_a,
                                        index_supervisor_test_capture_log, &first_capture, &first);
    int second_rc = cbm_index_worker_start_with_log(args, 8192, true, marker_b, quarantine_b,
                                                    index_supervisor_test_capture_log,
                                                    &second_capture, &second);
    uint64_t start_elapsed = cbm_now_ms() - start_before;

    char response_a[INDEX_SUPERVISOR_TEST_PATH_CAP] = {0};
    char response_b[INDEX_SUPERVISOR_TEST_PATH_CAP] = {0};
    char log_a[INDEX_SUPERVISOR_TEST_PATH_CAP] = {0};
    char log_b[INDEX_SUPERVISOR_TEST_PATH_CAP] = {0};
    if (first) {
        (void)snprintf(response_a, sizeof(response_a), "%s", cbm_index_worker_response_path(first));
        (void)snprintf(log_a, sizeof(log_a), "%s", cbm_index_worker_log_path(first));
    }
    if (second) {
        (void)snprintf(response_b, sizeof(response_b), "%s",
                       cbm_index_worker_response_path(second));
        (void)snprintf(log_b, sizeof(log_b), "%s", cbm_index_worker_log_path(second));
    }
    bool unique = response_a[0] && response_b[0] && log_a[0] && log_b[0] &&
                  strcmp(response_a, response_b) != 0 && strcmp(log_a, log_b) != 0 &&
                  strcmp(response_a, log_a) != 0 && strcmp(response_b, log_b) != 0;
    bool owner_only = index_supervisor_test_owner_file(response_a) &&
                      index_supervisor_test_owner_file(response_b) &&
                      index_supervisor_test_owner_file(log_a) &&
                      index_supervisor_test_owner_file(log_b);

    char ready_a[2048] = {0};
    char ready_b[2048] = {0};
    bool first_ready =
        first && index_supervisor_test_wait_file(first, marker_a, ready_a, sizeof(ready_a));
    bool second_ready =
        second && index_supervisor_test_wait_file(second, marker_b, ready_b, sizeof(ready_b));
    bool child_options = strstr(ready_a, "single=1") && strstr(ready_a, marker_a) &&
                         strstr(ready_a, quarantine_a) && strstr(ready_a, "budget=4096") &&
                         strstr(ready_b, "single=1") && strstr(ready_b, marker_b) &&
                         strstr(ready_b, quarantine_b) && strstr(ready_b, "budget=8192");
    bool parent_unchanged = strcmp(getenv("CBM_INDEX_SINGLE_THREAD"), "parent-single") == 0 &&
                            strcmp(getenv("CBM_INDEX_MARKER_FILE"), "parent-marker") == 0 &&
                            strcmp(getenv("CBM_INDEX_QUARANTINE_FILE"), "parent-quarantine") == 0;

    bool worker_logs_ready =
        index_supervisor_test_wait_file_text(log_a, "async worker hang-tree probe",
                                             INDEX_SUPERVISOR_TEST_READY_MS) &&
        index_supervisor_test_wait_file_text(log_b, "async worker hang-tree probe",
                                             INDEX_SUPERVISOR_TEST_READY_MS);
    bool callback_lines_injected = worker_logs_ready &&
                                   index_supervisor_test_append_log(log_a, "request-a-only\n") &&
                                   index_supervisor_test_append_log(log_b, "request-b-only\n");

    const cbm_index_worker_result_t *premature = (const cbm_index_worker_result_t *)1;
    uint64_t poll_before = cbm_now_ms();
    cbm_index_worker_poll_t running =
        first ? cbm_index_worker_poll(first, &premature) : CBM_INDEX_WORKER_POLL_ERROR;
    uint64_t poll_elapsed = cbm_now_ms() - poll_before;
    uint64_t cancel_before = cbm_now_ms();
    bool first_cancel = first && cbm_index_worker_request_cancel(first);
    bool second_cancel = second && cbm_index_worker_request_cancel(second);
    uint64_t cancel_elapsed = cbm_now_ms() - cancel_before;

    const cbm_index_worker_result_t *first_result = NULL;
    const cbm_index_worker_result_t *second_result = NULL;
    bool first_terminal = first && index_supervisor_test_poll_terminal(
                                       first, INDEX_SUPERVISOR_TEST_TERMINAL_MS, &first_result);
    bool second_terminal = second && index_supervisor_test_poll_terminal(
                                         second, INDEX_SUPERVISOR_TEST_TERMINAL_MS, &second_result);
    const cbm_index_worker_result_t *cached = NULL;
    int first_callbacks_at_terminal = first_capture.delivered;
    bool cached_result = first_terminal &&
                         cbm_index_worker_poll(first, &cached) == CBM_INDEX_WORKER_POLL_TERMINAL &&
                         cached == first_result;
    bool callback_terminal_stable = first_capture.delivered == first_callbacks_at_terminal;
    bool callback_isolation = callback_lines_injected && first_capture.saw_request_a &&
                              !first_capture.saw_request_b && second_capture.saw_request_b &&
                              !second_capture.saw_request_a;
    bool forced_as_expected = true;
#ifndef _WIN32
    forced_as_expected =
        first_result && second_result && first_result->forced && second_result->forced;
#endif
    bool contained = first_result && second_result && first_result->cancellation_requested &&
                     second_result->cancellation_requested && forced_as_expected &&
                     first_result->tree_quiesced && second_result->tree_quiesced &&
                     !first_result->supervision_failed && !second_result->supervision_failed;
    bool terminal_cancel_rejected = first && !cbm_index_worker_request_cancel(first);
    bool response_cleaned = cbm_file_size(response_a) < 0 && cbm_file_size(response_b) < 0;
    bool failure_logs_kept = cbm_file_size(log_a) >= 0 && cbm_file_size(log_b) >= 0;

    if (first_terminal) {
        cbm_index_worker_destroy(first);
    } else {
        index_supervisor_test_cleanup_handle(first);
    }
    if (second_terminal) {
        cbm_index_worker_destroy(second);
    } else {
        index_supervisor_test_cleanup_handle(second);
    }
    if (!first_ready || !second_ready) {
        index_supervisor_test_dump("worker-a log", log_a);
        index_supervisor_test_dump("worker-b log", log_b);
        index_supervisor_test_dump("worker-a response", response_a);
        index_supervisor_test_dump("worker-b response", response_b);
    }
    (void)cbm_unlink(log_a);
    (void)cbm_unlink(log_b);
    (void)cbm_unlink(marker_a);
    (void)cbm_unlink(marker_b);
    index_supervisor_test_restore_env("CBM_CACHE_DIR", saved_cache);
    index_supervisor_test_restore_env("CBM_INDEX_SINGLE_THREAD", saved_single);
    index_supervisor_test_restore_env("CBM_INDEX_MARKER_FILE", saved_marker);
    index_supervisor_test_restore_env("CBM_INDEX_QUARANTINE_FILE", saved_quarantine);
    (void)th_rmtree(cache);

    ASSERT_EQ(first_rc, 0);
    ASSERT_EQ(second_rc, 0);
    ASSERT_TRUE(start_elapsed < 2000);
    ASSERT_TRUE(unique);
    ASSERT_TRUE(owner_only);
    ASSERT_TRUE(first_ready);
    ASSERT_TRUE(second_ready);
    ASSERT_TRUE(child_options);
    ASSERT_TRUE(parent_unchanged);
    ASSERT_EQ(running, CBM_INDEX_WORKER_POLL_RUNNING);
    ASSERT_NULL(premature);
    ASSERT_TRUE(poll_elapsed < 100);
    ASSERT_TRUE(first_cancel);
    ASSERT_TRUE(second_cancel);
    ASSERT_TRUE(cancel_elapsed < 100);
    ASSERT_TRUE(first_terminal);
    ASSERT_TRUE(second_terminal);
    ASSERT_TRUE(cached_result);
    ASSERT_TRUE(callback_terminal_stable);
    ASSERT_TRUE(callback_isolation);
    ASSERT_TRUE(contained);
    ASSERT_TRUE(terminal_cancel_rejected);
    ASSERT_TRUE(response_cleaned);
    ASSERT_TRUE(failure_logs_kept);
    PASS();
}

TEST(index_supervisor_sync_wrapper_forwards_cancel_and_drains_tree) {
    char cache[INDEX_SUPERVISOR_TEST_PATH_CAP];
    (void)snprintf(cache, sizeof(cache), "%s/cbm-index-sync-cancel-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));
    const char *old_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache = old_cache ? cbm_strdup(old_cache) : NULL;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);

    atomic_int cancel_requested;
    atomic_init(&cancel_requested, 1);
    cbm_index_worker_result_t result = {0};
    int run_status =
        cbm_index_spawn_worker_with_log_cancel("{\"__cbm_test_worker\":\"hang-tree\"}", false, NULL,
                                               NULL, NULL, NULL, &cancel_requested, &result);
    bool contained = run_status == 0 && result.cancellation_requested && result.tree_quiesced &&
                     !result.supervision_failed && result.response == NULL;
    cbm_index_worker_result_free(&result);
    index_supervisor_test_restore_env("CBM_CACHE_DIR", saved_cache);
    (void)th_rmtree(cache);

    ASSERT_TRUE(contained);
    PASS();
}

static bool index_supervisor_test_run_probe(const char *mode, bool profiling,
                                            cbm_proc_outcome_t *outcome_out, bool *has_response_out,
                                            bool *log_exists_out, bool *response_path_exists_out) {
    char args[128];
    (void)snprintf(args, sizeof(args), "{\"__cbm_test_worker\":\"%s\"}", mode);
    cbm_profile_active = profiling;
    cbm_index_worker_handle_t *handle = NULL;
    if (cbm_index_worker_start(args, 0, false, NULL, NULL, &handle) != 0 || !handle) {
        return false;
    }
    char log_path[INDEX_SUPERVISOR_TEST_PATH_CAP];
    char response_path[INDEX_SUPERVISOR_TEST_PATH_CAP];
    (void)snprintf(log_path, sizeof(log_path), "%s", cbm_index_worker_log_path(handle));
    (void)snprintf(response_path, sizeof(response_path), "%s",
                   cbm_index_worker_response_path(handle));
    const cbm_index_worker_result_t *result = NULL;
    bool terminal =
        index_supervisor_test_poll_terminal(handle, INDEX_SUPERVISOR_TEST_TERMINAL_MS, &result);
    if (terminal && result) {
        *outcome_out = result->outcome;
        *has_response_out = result->response != NULL;
        *log_exists_out = cbm_file_size(log_path) >= 0;
        *response_path_exists_out = cbm_file_size(response_path) >= 0;
        cbm_index_worker_destroy(handle);
    } else {
        index_supervisor_test_dump("probe worker log", log_path);
        index_supervisor_test_dump("probe worker response", response_path);
        index_supervisor_test_cleanup_handle(handle);
    }
    if (cbm_file_size(log_path) >= 0) {
        (void)cbm_unlink(log_path);
    }
    return terminal;
}

TEST(index_supervisor_terminal_log_lifecycle_matches_outcome_and_profiling) {
    char cache[INDEX_SUPERVISOR_TEST_PATH_CAP];
    (void)snprintf(cache, sizeof(cache), "%s/cbm-index-logs-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));
    const char *old_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache = old_cache ? cbm_strdup(old_cache) : NULL;
    bool saved_profile = cbm_profile_active;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_proc_outcome_t clean_outcome = CBM_PROC_SPAWN_FAILED;
    cbm_proc_outcome_t profile_outcome = CBM_PROC_SPAWN_FAILED;
    cbm_proc_outcome_t crash_outcome = CBM_PROC_SPAWN_FAILED;
    bool clean_response = false;
    bool clean_log = true;
    bool clean_response_file = true;
    bool profile_response = false;
    bool profile_log = false;
    bool profile_response_file = true;
    bool crash_response = true;
    bool crash_log = false;
    bool crash_response_file = true;
    bool clean_terminal = index_supervisor_test_run_probe(
        "clean", false, &clean_outcome, &clean_response, &clean_log, &clean_response_file);
    bool profile_terminal = index_supervisor_test_run_probe(
        "clean", true, &profile_outcome, &profile_response, &profile_log, &profile_response_file);
    bool crash_terminal = index_supervisor_test_run_probe(
        "crash", false, &crash_outcome, &crash_response, &crash_log, &crash_response_file);
#ifdef _WIN32
    bool crash_classified_failure = crash_outcome != CBM_PROC_CLEAN;
#else
    bool crash_classified_failure = crash_outcome == CBM_PROC_CRASH;
#endif

    cbm_profile_active = saved_profile;
    index_supervisor_test_restore_env("CBM_CACHE_DIR", saved_cache);
    (void)th_rmtree(cache);

    ASSERT_TRUE(clean_terminal);
    ASSERT_EQ(clean_outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(clean_response);
    ASSERT_FALSE(clean_log);
    ASSERT_FALSE(clean_response_file);
    ASSERT_TRUE(profile_terminal);
    ASSERT_EQ(profile_outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(profile_response);
    ASSERT_TRUE(profile_log);
    ASSERT_FALSE(profile_response_file);
    ASSERT_TRUE(crash_terminal);
    ASSERT_TRUE(crash_classified_failure);
    ASSERT_FALSE(crash_response);
    ASSERT_TRUE(crash_log);
    ASSERT_FALSE(crash_response_file);
    PASS();
}

TEST(index_supervisor_drains_terminal_backlog_into_request_progress_callback) {
    char cache[INDEX_SUPERVISOR_TEST_PATH_CAP];
    (void)snprintf(cache, sizeof(cache), "%s/cbm-index-relay-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));
    const char *old_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache = old_cache ? cbm_strdup(old_cache) : NULL;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);

    FILE *progress = tmpfile();
    ASSERT_NOT_NULL(progress);
    index_supervisor_log_capture_t capture = {.render_progress = true};
    cbm_progress_sink_init(progress);
    cbm_index_worker_handle_t *handle = NULL;
    int start_rc =
        cbm_index_worker_start_with_log("{\"__cbm_test_worker\":\"clean\"}", 0, false, NULL, NULL,
                                        index_supervisor_test_capture_log, &capture, &handle);
    char log_path[INDEX_SUPERVISOR_TEST_PATH_CAP] = {0};
    if (handle) {
        (void)snprintf(log_path, sizeof(log_path), "%s", cbm_index_worker_log_path(handle));
    }
    bool worker_logged =
        log_path[0] && index_supervisor_test_wait_file_text(log_path, "async worker clean probe",
                                                            INDEX_SUPERVISOR_TEST_READY_MS);
    if (worker_logged) {
        /* Seeing the flushed probe places the child immediately before _Exit.
         * Give it time to become waitable without polling: the regression is a
         * backlog already present when the first terminal poll occurs. */
        cbm_usleep(250000);
    }
    bool backlog_written = worker_logged && index_supervisor_test_append_terminal_backlog(log_path);
    const cbm_index_worker_result_t *result = NULL;
    bool terminal = handle && index_supervisor_test_poll_terminal(
                                  handle, INDEX_SUPERVISOR_TEST_TERMINAL_MS, &result);
    cbm_proc_outcome_t outcome = result ? result->outcome : CBM_PROC_SPAWN_FAILED;
    int callbacks_at_terminal = capture.delivered;
    const cbm_index_worker_result_t *cached = NULL;
    bool terminal_cached =
        terminal && cbm_index_worker_poll(handle, &cached) == CBM_INDEX_WORKER_POLL_TERMINAL &&
        cached == result && capture.delivered == callbacks_at_terminal;
    cbm_progress_sink_fini();
    bool progress_rewound = fseek(progress, 0, SEEK_SET) == 0;
    char rendered[1024] = {0};
    size_t rendered_size =
        progress_rewound ? fread(rendered, 1, sizeof(rendered) - 1, progress) : 0;
    (void)fclose(progress);
    if (terminal) {
        cbm_index_worker_destroy(handle);
    } else {
        index_supervisor_test_cleanup_handle(handle);
    }

    if (!worker_logged || !terminal) {
        index_supervisor_test_dump("backlog worker log", log_path);
    }
    index_supervisor_test_restore_env("CBM_CACHE_DIR", saved_cache);
    (void)th_rmtree(cache);

    ASSERT_EQ(start_rc, 0);
    ASSERT_TRUE(worker_logged);
    ASSERT_TRUE(backlog_written);
    ASSERT_TRUE(terminal);
    ASSERT_EQ(outcome, CBM_PROC_CLEAN);
    ASSERT_TRUE(terminal_cached);
    ASSERT_TRUE(capture.saw_clean_probe);
    ASSERT_TRUE(capture.saw_structured_text);
    ASSERT_TRUE(capture.saw_structured_json);
    ASSERT_EQ(capture.backlog_lines, INDEX_SUPERVISOR_TEST_BACKLOG_LINES + 1);
    ASSERT_TRUE(capture.saw_backlog_sentinel);
    ASSERT_TRUE(progress_rewound);
    ASSERT_TRUE(rendered_size > 0);
    ASSERT_NOT_NULL(strstr(rendered, "Discovering files (7 found)"));
    ASSERT_NOT_NULL(strstr(rendered, "[1/9] Building file structure"));
    PASS();
}

TEST(index_supervisor_oversized_response_is_contained_and_log_is_retained) {
    char cache[INDEX_SUPERVISOR_TEST_PATH_CAP];
    (void)snprintf(cache, sizeof(cache), "%s/cbm-index-oversize-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));
    const char *old_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache = old_cache ? cbm_strdup(old_cache) : NULL;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_index_worker_handle_t *handle = NULL;
    int start_rc = cbm_index_worker_start("{\"__cbm_test_worker\":\"oversize\"}", 0, false, NULL,
                                          NULL, &handle);
    char log_path[INDEX_SUPERVISOR_TEST_PATH_CAP] = {0};
    char response_path[INDEX_SUPERVISOR_TEST_PATH_CAP] = {0};
    if (handle) {
        (void)snprintf(log_path, sizeof(log_path), "%s", cbm_index_worker_log_path(handle));
        (void)snprintf(response_path, sizeof(response_path), "%s",
                       cbm_index_worker_response_path(handle));
    }
    const cbm_index_worker_result_t *result = NULL;
    bool terminal = handle && index_supervisor_test_poll_terminal(
                                  handle, INDEX_SUPERVISOR_TEST_TERMINAL_MS, &result);
    bool rejected = terminal && result && result->response_rejected && !result->response &&
                    result->outcome == CBM_PROC_EXIT_NONZERO && result->tree_quiesced &&
                    !result->supervision_failed;
    bool log_retained = log_path[0] && cbm_file_size(log_path) >= 0;
    bool response_removed = response_path[0] && cbm_file_size(response_path) < 0;
    if (terminal) {
        cbm_index_worker_destroy(handle);
    } else {
        index_supervisor_test_dump("oversize worker log", log_path);
        index_supervisor_test_cleanup_handle(handle);
    }
    (void)cbm_unlink(log_path);
    index_supervisor_test_restore_env("CBM_CACHE_DIR", saved_cache);
    (void)th_rmtree(cache);

    ASSERT_EQ(start_rc, 0);
    ASSERT_TRUE(terminal);
    ASSERT_TRUE(rejected);
    ASSERT_TRUE(log_retained);
    ASSERT_TRUE(response_removed);
    PASS();
}

SUITE(index_supervisor) {
    RUN_TEST(index_supervisor_worker_argv_requires_exact_build_bound_grammar);
    RUN_TEST(index_supervisor_async_jobs_are_isolated_cancellable_and_terminal_cached);
    RUN_TEST(index_supervisor_sync_wrapper_forwards_cancel_and_drains_tree);
    RUN_TEST(index_supervisor_terminal_log_lifecycle_matches_outcome_and_profiling);
    RUN_TEST(index_supervisor_drains_terminal_backlog_into_request_progress_callback);
    RUN_TEST(index_supervisor_oversized_response_is_contained_and_log_is_retained);
}
