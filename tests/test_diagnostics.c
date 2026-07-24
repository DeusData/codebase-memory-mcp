/*
 * test_diagnostics.c — Security and lifecycle guards for the optional
 * periodic diagnostics writer.
 */
#include "test_framework.h"

#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/diagnostics.h"
#include "../src/foundation/log.h"
#include "../src/foundation/platform.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>

#include <aclapi.h>
#include <process.h>
#define getpid _getpid
#else
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

typedef struct {
    char directory[1024];
    char snapshot[1024];
    char trajectory[1024];
} diagnostics_paths_t;

static void diagnostics_wait_ms(unsigned int milliseconds) {
    struct timespec delay = {
        .tv_sec = (time_t)(milliseconds / 1000U),
        .tv_nsec = (long)((milliseconds % 1000U) * 1000000U),
    };
    (void)cbm_nanosleep(&delay, NULL);
}

#ifndef _WIN32
static bool diagnostics_file_equals(const char *path, const char *expected) {
    char contents[128] = "";
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return false;
    }
    size_t length = fread(contents, 1, sizeof(contents) - 1, file);
    bool closed = fclose(file) == 0;
    contents[length] = '\0';
    return closed && strcmp(contents, expected) == 0;
}
#endif

static bool diagnostics_capture_paths(diagnostics_paths_t *paths) {
    return cbm_diag_test_copy_paths(paths->directory, sizeof(paths->directory), paths->snapshot,
                                    sizeof(paths->snapshot), paths->trajectory,
                                    sizeof(paths->trajectory));
}

static bool diagnostics_wait_for_file(const char *path, unsigned int timeout_ms) {
    uint64_t deadline = cbm_now_ms() + timeout_ms;
    while (cbm_now_ms() < deadline) {
        if (cbm_file_exists(path)) {
            return true;
        }
        diagnostics_wait_ms(10);
    }
    return cbm_file_exists(path);
}

static char *diagnostics_read_file(const char *path) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file || fseek(file, 0, SEEK_END) != 0) {
        if (file) {
            (void)fclose(file);
        }
        return NULL;
    }
    long length = ftell(file);
    if (length < 0 || fseek(file, 0, SEEK_SET) != 0) {
        (void)fclose(file);
        return NULL;
    }
    char *contents = malloc((size_t)length + 1);
    if (!contents) {
        (void)fclose(file);
        return NULL;
    }
    size_t read_length = fread(contents, 1, (size_t)length, file);
    bool closed = fclose(file) == 0;
    bool complete = read_length == (size_t)length && closed;
    contents[read_length] = '\0';
    if (!complete) {
        free(contents);
        return NULL;
    }
    return contents;
}

static void diagnostics_cleanup_outputs(const diagnostics_paths_t *paths) {
    char temporary[1100];
    char rotated[1100];
    if (snprintf(temporary, sizeof(temporary), "%s.tmp", paths->snapshot) > 0) {
        (void)cbm_unlink(temporary);
    }
    if (snprintf(rotated, sizeof(rotated), "%s.1", paths->trajectory) > 0) {
        (void)cbm_unlink(rotated);
    }
    (void)cbm_unlink(paths->snapshot);
    (void)cbm_unlink(paths->trajectory);
    (void)cbm_rmdir(paths->directory);
}

#ifndef _WIN32
TEST(diagnostics_rejects_predictable_tmp_symlinks) {
    char snapshot_link[256];
    char trajectory_link[256];
    char snapshot_target[256];
    char trajectory_target[256];
    const char *snapshot_seed = "snapshot sentinel\n";
    const char *trajectory_seed = "trajectory sentinel\n";
    int pid = (int)getpid();

    ASSERT_LT(
        snprintf(snapshot_link, sizeof(snapshot_link), "/tmp/cbm-diagnostics-%d.json.tmp", pid),
        (int)sizeof(snapshot_link));
    ASSERT_LT(
        snprintf(trajectory_link, sizeof(trajectory_link), "/tmp/cbm-diagnostics-%d.ndjson", pid),
        (int)sizeof(trajectory_link));
    ASSERT_LT(snprintf(snapshot_target, sizeof(snapshot_target),
                       "/tmp/cbm-diagnostics-snapshot-sentinel-%d", pid),
              (int)sizeof(snapshot_target));
    ASSERT_LT(snprintf(trajectory_target, sizeof(trajectory_target),
                       "/tmp/cbm-diagnostics-trajectory-sentinel-%d", pid),
              (int)sizeof(trajectory_target));

    (void)cbm_unlink(snapshot_link);
    (void)cbm_unlink(trajectory_link);
    (void)cbm_unlink(snapshot_target);
    (void)cbm_unlink(trajectory_target);

    FILE *snapshot = cbm_fopen(snapshot_target, "wb");
    ASSERT_NOT_NULL(snapshot);
    ASSERT_GTE(fputs(snapshot_seed, snapshot), 0);
    ASSERT_EQ(fclose(snapshot), 0);
    FILE *trajectory = cbm_fopen(trajectory_target, "wb");
    ASSERT_NOT_NULL(trajectory);
    ASSERT_GTE(fputs(trajectory_seed, trajectory), 0);
    ASSERT_EQ(fclose(trajectory), 0);
    ASSERT_EQ(symlink(snapshot_target, snapshot_link), 0);
    ASSERT_EQ(symlink(trajectory_target, trajectory_link), 0);

    ASSERT_EQ(cbm_setenv("CBM_DIAGNOSTICS", "1", 1), 0);
    ASSERT_TRUE(cbm_diag_start());
    diagnostics_paths_t outputs;
    bool captured = diagnostics_capture_paths(&outputs);
    bool emitted = captured && diagnostics_wait_for_file(outputs.snapshot, 1000);
    cbm_diag_stop();
    ASSERT_EQ(cbm_unsetenv("CBM_DIAGNOSTICS"), 0);

    bool snapshot_untouched = diagnostics_file_equals(snapshot_target, snapshot_seed);
    bool trajectory_untouched = diagnostics_file_equals(trajectory_target, trajectory_seed);
    (void)cbm_unlink(snapshot_link);
    (void)cbm_unlink(trajectory_link);
    (void)cbm_unlink(snapshot_target);
    (void)cbm_unlink(trajectory_target);
    if (captured) {
        diagnostics_cleanup_outputs(&outputs);
    }

    ASSERT_TRUE(captured);
    ASSERT_TRUE(emitted);
    ASSERT_TRUE(snapshot_untouched);
    ASSERT_TRUE(trajectory_untouched);
    PASS();
}
#endif

TEST(diagnostics_stop_interrupts_periodic_wait) {
    char legacy_snapshot[256];
    char legacy_trajectory[256];
    int pid = (int)getpid();
    ASSERT_LT(snprintf(legacy_snapshot, sizeof(legacy_snapshot), "%s/cbm-diagnostics-%d.json",
                       cbm_tmpdir(), pid),
              (int)sizeof(legacy_snapshot));
    ASSERT_LT(snprintf(legacy_trajectory, sizeof(legacy_trajectory), "%s/cbm-diagnostics-%d.ndjson",
                       cbm_tmpdir(), pid),
              (int)sizeof(legacy_trajectory));
    (void)cbm_unlink(legacy_snapshot);
    (void)cbm_unlink(legacy_trajectory);

    ASSERT_EQ(cbm_setenv("CBM_DIAGNOSTICS", "1", 1), 0);
    ASSERT_TRUE(cbm_diag_start());

    diagnostics_paths_t outputs;
    bool captured = diagnostics_capture_paths(&outputs);
    /* A visible snapshot proves the writer has reached its periodic wait. */
    bool emitted = captured && diagnostics_wait_for_file(outputs.snapshot, 1000);
    uint64_t started_ms = cbm_now_ms();
    cbm_diag_stop();
    uint64_t elapsed_ms = cbm_now_ms() - started_ms;
    ASSERT_EQ(cbm_unsetenv("CBM_DIAGNOSTICS"), 0);
    (void)cbm_unlink(legacy_snapshot);
    (void)cbm_unlink(legacy_trajectory);
    if (captured) {
        diagnostics_cleanup_outputs(&outputs);
    }

    ASSERT_TRUE(captured);
    ASSERT_TRUE(emitted);
    ASSERT_LT(elapsed_ms, UINT64_C(1000));
    PASS();
}

#ifndef _WIN32
TEST(diagnostics_outputs_are_owner_private_regular_files) {
    ASSERT_EQ(cbm_setenv("CBM_DIAGNOSTICS", "1", 1), 0);
    ASSERT_TRUE(cbm_diag_start());
    diagnostics_paths_t outputs;
    bool captured = diagnostics_capture_paths(&outputs);
    bool snapshot_emitted = captured && diagnostics_wait_for_file(outputs.snapshot, 1000);
    bool trajectory_emitted = captured && diagnostics_wait_for_file(outputs.trajectory, 1000);

    struct stat directory_state;
    struct stat snapshot_state;
    struct stat trajectory_state;
    bool secure = captured && snapshot_emitted && trajectory_emitted &&
                  lstat(outputs.directory, &directory_state) == 0 &&
                  S_ISDIR(directory_state.st_mode) && directory_state.st_uid == geteuid() &&
                  (directory_state.st_mode & 07777) == 0700 &&
                  lstat(outputs.snapshot, &snapshot_state) == 0 &&
                  S_ISREG(snapshot_state.st_mode) && snapshot_state.st_uid == geteuid() &&
                  snapshot_state.st_nlink == 1 && (snapshot_state.st_mode & 07777) == 0600 &&
                  lstat(outputs.trajectory, &trajectory_state) == 0 &&
                  S_ISREG(trajectory_state.st_mode) && trajectory_state.st_uid == geteuid() &&
                  trajectory_state.st_nlink == 1 && (trajectory_state.st_mode & 07777) == 0600;

    cbm_diag_stop();
    ASSERT_EQ(cbm_unsetenv("CBM_DIAGNOSTICS"), 0);
    if (captured) {
        diagnostics_cleanup_outputs(&outputs);
    }
    ASSERT_TRUE(secure);
    PASS();
}
#endif

TEST(diagnostics_stalled_writer_cannot_retain_shutdown) {
    cbm_diag_test_hold_writer(true);
    ASSERT_EQ(cbm_setenv("CBM_DIAGNOSTICS", "1", 1), 0);
    if (!cbm_diag_start()) {
        cbm_diag_test_hold_writer(false);
        (void)cbm_unsetenv("CBM_DIAGNOSTICS");
        FAIL("failed to start held diagnostics writer");
    }
    diagnostics_paths_t outputs;
    bool captured = diagnostics_capture_paths(&outputs);
    uint64_t reach_deadline = cbm_now_ms() + 1000;
    while (!cbm_diag_test_writer_reached() && cbm_now_ms() < reach_deadline) {
        diagnostics_wait_ms(10);
    }
    bool reached = cbm_diag_test_writer_reached();
    if (!captured || !reached) {
        cbm_diag_test_hold_writer(false);
        cbm_diag_stop();
        (void)cbm_unsetenv("CBM_DIAGNOSTICS");
        if (captured) {
            diagnostics_cleanup_outputs(&outputs);
        }
        FAIL("diagnostics writer did not reach the held I/O boundary");
    }

    uint64_t started_ms = cbm_now_ms();
    cbm_diag_stop();
    uint64_t elapsed_ms = cbm_now_ms() - started_ms;
    bool abandoned = cbm_diag_test_abandoned();

    cbm_diag_test_hold_writer(false);
    bool reset = false;
    uint64_t reset_deadline = cbm_now_ms() + 1000;
    while (!reset && cbm_now_ms() < reset_deadline) {
        reset = cbm_diag_test_reset_abandoned();
        if (!reset) {
            diagnostics_wait_ms(10);
        }
    }
    (void)cbm_unsetenv("CBM_DIAGNOSTICS");
    diagnostics_cleanup_outputs(&outputs);

    ASSERT_LT(elapsed_ms, UINT64_C(1500));
    ASSERT_TRUE(abandoned);
    ASSERT_TRUE(reset);
    PASS();
}

TEST(diagnostics_soak_discovers_daemon_emitted_paths) {
    char *script = diagnostics_read_file("scripts/soak-test.sh");
    ASSERT_NOT_NULL(script);
    bool parses_start_event = strstr(script, "\"event\":\"diagnostics.start\"") != NULL;
    bool isolates_daemon = strstr(script, "CBM_CACHE_DIR=\"$SOAK_CACHE_DIR_VALUE\"") != NULL;
    bool legacy_predictable_path = strstr(script, "/tmp/cbm-diagnostics-${SERVER_PID}") != NULL;
    free(script);

    ASSERT_TRUE(parses_start_event);
    ASSERT_TRUE(isolates_daemon);
    ASSERT_FALSE(legacy_predictable_path);
    PASS();
}


/* ── Discovery record contract ───────────────────────────────────────── */

static char diagnostics_discovery_line[2048];
static int diagnostics_discovery_records;

static void diagnostics_discovery_sink(const char *line) {
    if (!line || !strstr(line, "\"event\":\"diagnostics.start\"")) {
        return;
    }
    diagnostics_discovery_records++;
    (void)snprintf(diagnostics_discovery_line, sizeof(diagnostics_discovery_line), "%s", line);
}

static bool diagnostics_discovery_value(const char *key, char *out, size_t out_size) {
    char marker[64];
    if (snprintf(marker, sizeof(marker), "\"%s\":\"", key) >= (int)sizeof(marker)) {
        return false;
    }
    const char *cursor = strstr(diagnostics_discovery_line, marker);
    if (!cursor) {
        return false;
    }
    cursor += strlen(marker);
    size_t used = 0;
    while (*cursor && *cursor != '"' && used + 1 < out_size) {
        if (*cursor == '\\' && cursor[1] != '\0') {
            cursor++;
            char decoded = *cursor;
            if (decoded == 'n') {
                decoded = '\n';
            } else if (decoded == 't') {
                decoded = '\t';
            } else if (decoded == 'r') {
                decoded = '\r';
            }
            out[used++] = decoded;
            cursor++;
            continue;
        }
        out[used++] = *cursor++;
    }
    out[used] = '\0';
    return *cursor == '"';
}

TEST(diagnostics_discovery_record_survives_suppressed_log_level) {
    diagnostics_discovery_records = 0;
    diagnostics_discovery_line[0] = '\0';
    cbm_log_set_level(CBM_LOG_NONE);
    cbm_log_set_sink_ex(diagnostics_discovery_sink, CBM_LOG_SINK_REPLACE);
    ASSERT_EQ(cbm_setenv("CBM_DIAGNOSTICS", "1", 1), 0);
    bool started = cbm_diag_start();
    diagnostics_paths_t outputs;
    bool captured = started && diagnostics_capture_paths(&outputs);
    cbm_diag_stop();
    (void)cbm_unsetenv("CBM_DIAGNOSTICS");
    cbm_log_set_sink(NULL);
    cbm_log_set_level(CBM_LOG_INFO);

    char recorded_snapshot[1024] = "";
    char recorded_trajectory[1024] = "";
    bool decoded =
        diagnostics_discovery_value("snapshot", recorded_snapshot, sizeof(recorded_snapshot)) &&
        diagnostics_discovery_value("trajectory", recorded_trajectory,
                                    sizeof(recorded_trajectory));
    if (captured) {
        diagnostics_cleanup_outputs(&outputs);
    }
    ASSERT_TRUE(started);
    ASSERT_TRUE(captured);
    /* Exactly one discovery record, delivered despite CBM_LOG_LEVEL=none, and
     * its decoded paths are byte-exact against the writer's own paths. */
    ASSERT_EQ(diagnostics_discovery_records, 1);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(strcmp(recorded_snapshot, outputs.snapshot), 0);
    ASSERT_EQ(strcmp(recorded_trajectory, outputs.trajectory), 0);
    PASS();
}

#ifndef _WIN32
TEST(diagnostics_placement_honors_tmpdir_with_spaces) {
    char saved_tmpdir[1024] = "";
    const char *current_tmpdir = getenv("TMPDIR");
    bool had_tmpdir = current_tmpdir != NULL;
    if (had_tmpdir) {
        ASSERT_LT(snprintf(saved_tmpdir, sizeof(saved_tmpdir), "%s", current_tmpdir),
                  (int)sizeof(saved_tmpdir));
    }
    char base[256];
    ASSERT_LT(snprintf(base, sizeof(base), "/tmp/cbm diag spaces %d", (int)getpid()),
              (int)sizeof(base));
    (void)cbm_rmdir(base);
    ASSERT_EQ(mkdir(base, 0700), 0);
    ASSERT_EQ(cbm_setenv("TMPDIR", base, 1), 0);
    diagnostics_discovery_records = 0;
    diagnostics_discovery_line[0] = '\0';
    cbm_log_set_sink_ex(diagnostics_discovery_sink, CBM_LOG_SINK_REPLACE);
    ASSERT_EQ(cbm_setenv("CBM_DIAGNOSTICS", "1", 1), 0);
    bool started = cbm_diag_start();
    diagnostics_paths_t outputs;
    bool captured = started && diagnostics_capture_paths(&outputs);
    bool emitted = captured && diagnostics_wait_for_file(outputs.snapshot, 1000);
    cbm_diag_stop();
    (void)cbm_unsetenv("CBM_DIAGNOSTICS");
    cbm_log_set_sink(NULL);
    if (had_tmpdir) {
        (void)cbm_setenv("TMPDIR", saved_tmpdir, 1);
    } else {
        (void)cbm_unsetenv("TMPDIR");
    }

    char recorded[1024] = "";
    bool decoded = diagnostics_discovery_value("snapshot", recorded, sizeof(recorded));
    bool under_base = captured && strncmp(outputs.directory, base, strlen(base)) == 0;
    if (captured) {
        diagnostics_cleanup_outputs(&outputs);
    }
    (void)cbm_rmdir(base);
    ASSERT_TRUE(started);
    ASSERT_TRUE(captured);
    ASSERT_TRUE(emitted);
    /* The documented contract: honor $TMPDIR (with /tmp fallback), and the
     * discovery record must round-trip a path containing spaces exactly. */
    ASSERT_TRUE(under_base);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(strcmp(recorded, outputs.snapshot), 0);
    PASS();
}
#endif

#ifdef _WIN32
static wchar_t *diagnostics_windows_utf8_to_wide(const char *value) {
    int needed = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, NULL, 0);
    wchar_t *wide = needed > 0 ? calloc((size_t)needed, sizeof(*wide)) : NULL;
    if (!wide || MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, wide, needed) <= 0) {
        free(wide);
        return NULL;
    }
    return wide;
}

static bool diagnostics_windows_current_user_sid(void **information_out, PSID *sid_out) {
    *information_out = NULL;
    *sid_out = NULL;
    HANDLE token = NULL;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        return false;
    }
    DWORD needed = 0;
    (void)GetTokenInformation(token, TokenUser, NULL, 0, &needed);
    void *information = needed > 0 ? calloc(1, needed) : NULL;
    bool ok =
        information && GetTokenInformation(token, TokenUser, information, needed, &needed) != 0;
    (void)CloseHandle(token);
    if (!ok) {
        free(information);
        return false;
    }
    PSID sid = ((TOKEN_USER *)information)->User.Sid;
    if (!sid || !IsValidSid(sid)) {
        free(information);
        return false;
    }
    *information_out = information;
    *sid_out = sid;
    return true;
}

/* Owner is the current user AND the protected DACL grants access to that
 * owner alone — the Windows equivalent of the POSIX 0700/0600 assertions. */
static bool diagnostics_windows_path_owner_private(const char *path, bool directory) {
    void *user_information = NULL;
    PSID user_sid = NULL;
    wchar_t *wide_path = diagnostics_windows_utf8_to_wide(path);
    DWORD flags = FILE_FLAG_OPEN_REPARSE_POINT | (directory ? FILE_FLAG_BACKUP_SEMANTICS : 0);
    HANDLE handle = wide_path
                        ? CreateFileW(wide_path, READ_CONTROL,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                      OPEN_EXISTING, flags, NULL)
                        : INVALID_HANDLE_VALUE;
    PSID owner = NULL;
    PACL dacl = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    SECURITY_DESCRIPTOR_CONTROL control = 0;
    DWORD revision = 0;
    bool ok = handle != INVALID_HANDLE_VALUE &&
              diagnostics_windows_current_user_sid(&user_information, &user_sid) &&
              GetSecurityInfo(handle, SE_FILE_OBJECT,
                              OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION, &owner, NULL,
                              &dacl, NULL, &descriptor) == ERROR_SUCCESS &&
              owner && IsValidSid(owner) && EqualSid(owner, user_sid) &&
              GetSecurityDescriptorControl(descriptor, &control, &revision) != 0 &&
              (control & SE_DACL_PROTECTED) != 0 && dacl && dacl->AceCount == 1;
    if (ok) {
        void *ace = NULL;
        ok = GetAce(dacl, 0, &ace) != 0 &&
             ((ACE_HEADER *)ace)->AceType == ACCESS_ALLOWED_ACE_TYPE &&
             EqualSid((PSID)&((ACCESS_ALLOWED_ACE *)ace)->SidStart, user_sid);
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    if (handle != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(handle);
    }
    free(wide_path);
    free(user_information);
    return ok;
}

static bool diagnostics_windows_output_contract(const char *path) {
    /* Mirrors production's diag_win_handle_valid: files are secured by their
     * owner-private parent directory; the file itself must be a plain local
     * regular file — no reparse, no extra links. Owner/DACL are asserted on
     * the directory, where the security boundary actually lives (file owners
     * follow the machine's default-owner policy, e.g. Administrators on
     * GitHub-runner-class images). */
    wchar_t *wide_path = diagnostics_windows_utf8_to_wide(path);
    HANDLE handle = wide_path
                        ? CreateFileW(wide_path, GENERIC_READ,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                      OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL)
                        : INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION information;
    bool ok = handle != INVALID_HANDLE_VALUE && GetFileType(handle) == FILE_TYPE_DISK &&
              GetFileInformationByHandle(handle, &information) != 0 &&
              (information.dwFileAttributes &
               (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
              information.nNumberOfLinks == 1;
    if (handle != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(handle);
    }
    free(wide_path);
    return ok;
}

TEST(diagnostics_outputs_are_owner_private_windows) {
    ASSERT_EQ(cbm_setenv("CBM_DIAGNOSTICS", "1", 1), 0);
    ASSERT_TRUE(cbm_diag_start());
    diagnostics_paths_t outputs;
    bool captured = diagnostics_capture_paths(&outputs);
    bool snapshot_emitted = captured && diagnostics_wait_for_file(outputs.snapshot, 1000);
    bool trajectory_emitted = captured && diagnostics_wait_for_file(outputs.trajectory, 1000);
    bool secure = captured && snapshot_emitted && trajectory_emitted &&
                  diagnostics_windows_path_owner_private(outputs.directory, true) &&
                  diagnostics_windows_output_contract(outputs.snapshot) &&
                  diagnostics_windows_output_contract(outputs.trajectory);
    cbm_diag_stop();
    ASSERT_EQ(cbm_unsetenv("CBM_DIAGNOSTICS"), 0);
    if (captured) {
        diagnostics_cleanup_outputs(&outputs);
    }
    ASSERT_TRUE(secure);
    PASS();
}
#endif

SUITE(diagnostics) {
#ifndef _WIN32
    RUN_TEST(diagnostics_rejects_predictable_tmp_symlinks);
#endif
    RUN_TEST(diagnostics_stop_interrupts_periodic_wait);
#ifndef _WIN32
    RUN_TEST(diagnostics_outputs_are_owner_private_regular_files);
    RUN_TEST(diagnostics_placement_honors_tmpdir_with_spaces);
#endif
#ifdef _WIN32
    RUN_TEST(diagnostics_outputs_are_owner_private_windows);
#endif
    RUN_TEST(diagnostics_discovery_record_survives_suppressed_log_level);
    RUN_TEST(diagnostics_stalled_writer_cannot_retain_shutdown);
    RUN_TEST(diagnostics_soak_discovers_daemon_emitted_paths);
}
