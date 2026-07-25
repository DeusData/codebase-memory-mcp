#include "git/git_command.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"
#ifdef _WIN32
#include "foundation/win_utf8.h"
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <wchar.h>
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

bool cbm_git_validate_repo_path(const char *repo_path) {
    return repo_path && repo_path[0] != '\0';
}

#ifdef _WIN32
static bool git_windows_path_absolute(const wchar_t *path) {
    if (!path || wcslen(path) < 3U) {
        return false;
    }
    bool drive = ((path[0] >= L'A' && path[0] <= L'Z') ||
                  (path[0] >= L'a' && path[0] <= L'z')) &&
                 path[1] == L':' && (path[2] == L'\\' || path[2] == L'/');
    bool unc = (path[0] == L'\\' || path[0] == L'/') &&
               (path[1] == L'\\' || path[1] == L'/') && path[2] != L'\0' &&
               path[2] != L'\\' && path[2] != L'/';
    return drive || unc;
}

static bool git_windows_candidate(const wchar_t *entry, size_t entry_length,
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
    if (!git_windows_path_absolute(directory) || wcschr(directory, L'"') != NULL) {
        return false;
    }

    bool separator = directory[entry_length - 1U] == L'\\' ||
                     directory[entry_length - 1U] == L'/';
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
        !git_windows_path_absolute(normalized)) {
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
static bool git_resolve_windows(char output[CBM_SZ_4K]) {
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
        if (git_windows_candidate(entry, (size_t)(cursor - entry), output)) {
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

bool cbm_git_resolve_executable(char out[CBM_SZ_4K]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
#ifdef _WIN32
    return git_resolve_windows(out);
#else
    memcpy(out, "git", sizeof("git"));
    return true;
#endif
}

void cbm_git_output_cleanup(cbm_git_output_t *output) {
    if (!output) {
        return;
    }
    if (output->path[0] != '\0') {
        (void)cbm_unlink(output->path);
    }
    memset(output, 0, sizeof(*output));
}

static bool git_output_create(cbm_git_output_t *output) {
    memset(output, 0, sizeof(*output));
    int written =
        snprintf(output->path, sizeof(output->path), "%s/cbm-git-XXXXXX", cbm_tmpdir());
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
        cbm_git_output_cleanup(output);
    }
    return closed;
}

static const char **git_build_argv(const char *repo_path, const char *const git_args[]) {
    size_t count = 0;
    while (git_args[count]) {
        if (count == SIZE_MAX - 5U) {
            return NULL;
        }
        count++;
    }
    const char **argv = malloc((count + 5U) * sizeof(*argv));
    if (!argv) {
        return NULL;
    }
    argv[0] = "git";
    argv[1] = "--no-optional-locks";
    argv[2] = "-C";
    argv[3] = repo_path;
    for (size_t i = 0; i < count; i++) {
        argv[4U + i] = git_args[i];
    }
    argv[4U + count] = NULL;
    return argv;
}

int cbm_git_run_argv(const char *repo_path, const char *const git_args[],
                     const cbm_git_run_opts_t *opts, cbm_git_output_t *output,
                     cbm_proc_result_t *result) {
    cbm_proc_result_t local_result = {
        .outcome = CBM_PROC_SPAWN_FAILED,
        .exit_code = -1,
    };
    if (!result) {
        result = &local_result;
    } else {
        *result = local_result;
    }
    if (output) {
        memset(output, 0, sizeof(*output));
    }
    if (!cbm_git_validate_repo_path(repo_path) || !git_args || !git_args[0] ||
        (output && !git_output_create(output))) {
        return CBM_NOT_FOUND;
    }

    char executable[CBM_SZ_4K];
    const char **argv = git_build_argv(repo_path, git_args);
    if (!argv || !cbm_git_resolve_executable(executable)) {
        free(argv);
        cbm_git_output_cleanup(output);
        return CBM_NOT_FOUND;
    }
    cbm_proc_opts_t process_opts = {
        .bin = executable,
        .argv = argv,
        .log_file = output ? output->path : NULL,
        .discard_stderr = true,
        .quiet_timeout_ms = 0,
        .cancel_grace_ms = CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS,
        .delete_log_on_exit = false,
    };
    cbm_subprocess_t *process = NULL;
    int spawn_rc = cbm_subprocess_spawn(&process_opts, &process);
    free(argv);
    if (spawn_rc != 0) {
        cbm_git_output_cleanup(output);
        return CBM_NOT_FOUND;
    }

    bool cancel_sent = false;
    cbm_proc_poll_t state;
    for (;;) {
        if (!cancel_sent && opts && opts->cancel_requested &&
            opts->cancel_requested(opts->cancel_context)) {
            cancel_sent = cbm_subprocess_request_cancel(process);
        }
        state = cbm_subprocess_poll(process, result);
        if (state == CBM_PROC_POLL_TERMINAL) {
            break;
        }
        if (state == CBM_PROC_POLL_ERROR) {
            cancel_sent = cbm_subprocess_request_cancel(process) || cancel_sent;
        }
        cbm_usleep(10000);
    }
    bool contained = result->tree_quiesced && !result->supervision_failed;
    cbm_subprocess_destroy(process);
    if (!contained) {
        cbm_git_output_cleanup(output);
        return CBM_NOT_FOUND;
    }
    if (output) {
        int64_t size = cbm_file_size(output->path);
        if (size < 0 || (uint64_t)size > SIZE_MAX) {
            cbm_git_output_cleanup(output);
            return CBM_NOT_FOUND;
        }
        output->size = (size_t)size;
    }
    return 0;
}

static void git_trim_newlines(char *s) {
    if (!s) {
        return;
    }
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == '\n' || s[n - 1] == '\r')) {
        s[--n] = '\0';
    }
}

int cbm_git_capture_first_line_buf(const char *repo_path, const char *const git_args[],
                                   char *out, size_t out_size) {
    if (!out || out_size == 0) {
        return CBM_NOT_FOUND;
    }
    out[0] = '\0';

    cbm_git_output_t output;
    cbm_proc_result_t result;
    if (cbm_git_run_argv(repo_path, git_args, NULL, &output, &result) != 0 ||
        result.outcome != CBM_PROC_CLEAN) {
        cbm_git_output_cleanup(&output);
        return CBM_NOT_FOUND;
    }
    FILE *fp = cbm_fopen(output.path, "rb");
    if (!fp) {
        cbm_git_output_cleanup(&output);
        return CBM_NOT_FOUND;
    }

    bool got_line = fgets(out, (int)out_size, fp) != NULL;
    size_t len = got_line ? strlen(out) : 0;
    bool truncated = got_line && len > 0 && out[len - 1] != '\n' && !feof(fp);
    git_trim_newlines(out);

    int rc = fclose(fp);
    cbm_git_output_cleanup(&output);
    if (!got_line || truncated || rc != 0 || out[0] == '\0') {
        out[0] = '\0';
        return CBM_NOT_FOUND;
    }
    return 0;
}

int cbm_git_capture_first_line(const char *repo_path, const char *const git_args[], char **out) {
    if (!out) {
        return CBM_NOT_FOUND;
    }
    *out = NULL;
    char buf[CBM_GIT_OUTPUT_BUFSZ];
    if (cbm_git_capture_first_line_buf(repo_path, git_args, buf, sizeof(buf)) != 0) {
        return CBM_NOT_FOUND;
    }
    *out = cbm_strdup(buf);
    return *out ? 0 : CBM_NOT_FOUND;
}

int cbm_git_run_first_line_buf(const char *repo_path, const char *const git_args[],
                               char *out, size_t out_size, int *out_exit_code) {
    if (!out || out_size == 0 || !out_exit_code) {
        return CBM_NOT_FOUND;
    }
    out[0] = '\0';
    *out_exit_code = CBM_NOT_FOUND;
    cbm_git_output_t output;
    cbm_proc_result_t result;
    if (cbm_git_run_argv(repo_path, git_args, NULL, &output, &result) != 0) {
        return CBM_NOT_FOUND;
    }
    FILE *fp = cbm_fopen(output.path, "rb");
    if (!fp) {
        cbm_git_output_cleanup(&output);
        return CBM_NOT_FOUND;
    }

    char line[CBM_GIT_OUTPUT_BUFSZ];
    bool got_line = fgets(line, (int)sizeof(line), fp) != NULL;
    size_t line_len = got_line ? strlen(line) : 0;
    bool truncated = got_line && line_len > 0 && line[line_len - 1] != '\n' && !feof(fp);
    bool output_fits = true;
    if (got_line) {
        git_trim_newlines(line);
        size_t value_len = strlen(line);
        if (value_len >= out_size) {
            output_fits = false;
        } else {
            memcpy(out, line, value_len + 1);
        }
    }
    char drain[CBM_SZ_128];
    while (fgets(drain, (int)sizeof(drain), fp)) {
    }
    int close_rc = fclose(fp);
    *out_exit_code = result.exit_code;
    cbm_git_output_cleanup(&output);
    if (close_rc != 0 || truncated || !output_fits) {
        out[0] = '\0';
        return CBM_NOT_FOUND;
    }
    return 0;
}

int cbm_git_drain_command(const char *repo_path, const char *const git_args[]) {
    cbm_proc_result_t result;
    return cbm_git_run_argv(repo_path, git_args, NULL, NULL, &result) == 0 &&
                   result.outcome == CBM_PROC_CLEAN
               ? 0
               : CBM_NOT_FOUND;
}
