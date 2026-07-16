#include "git/git_command.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/str_util.h"

#include <stdio.h>
#include <string.h>

const char *cbm_git_null_device(void) {
#if defined(_WIN32)
    return "NUL";
#else
    return "/dev/null";
#endif
}

bool cbm_git_validate_repo_path(const char *repo_path) {
    if (!cbm_validate_shell_arg(repo_path)) {
        return false;
    }
#ifdef _WIN32
    for (const char *p = repo_path; *p; p++) {
        if (*p == '%' || *p == '!' || *p == '^') {
            return false;
        }
    }
#endif
    return true;
}

bool cbm_git_command_fits(int n, size_t cmd_size) {
    return n >= 0 && (size_t)n < cmd_size;
}

bool cbm_git_format_command(char *cmd, size_t cmd_size, const char *repo_path,
                            const char *git_args) {
    if (!cmd || cmd_size == 0 || !repo_path || !git_args ||
        !cbm_git_validate_repo_path(repo_path)) {
        return false;
    }
    /* Double quotes work for POSIX shells and cmd.exe. cbm_git_validate_repo_path()
     * rejects shell metacharacters before interpolation. */
    int n = snprintf(cmd, cmd_size, "git -C \"%s\" %s 2>%s", repo_path, git_args,
                     cbm_git_null_device());
    return cbm_git_command_fits(n, cmd_size);
}

bool cbm_git_format_status_command(char *cmd, size_t cmd_size, const char *repo_path) {
    if (!cmd || cmd_size == 0 || !repo_path || !cbm_git_validate_repo_path(repo_path)) {
        return false;
    }
    int n = snprintf(cmd, cmd_size,
                     "git --no-optional-locks -C \"%s\" status --porcelain "
                     "--untracked-files=all 2>%s",
                     repo_path, cbm_git_null_device());
    return cbm_git_command_fits(n, cmd_size);
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

int cbm_git_capture_first_line_buf(const char *repo_path, const char *git_args,
                                   char *out, size_t out_size) {
    if (!out || out_size == 0) {
        return CBM_NOT_FOUND;
    }
    out[0] = '\0';

    char cmd[CBM_GIT_CMD_BUFSZ];
    if (!cbm_git_format_command(cmd, sizeof(cmd), repo_path, git_args)) {
        return CBM_NOT_FOUND;
    }

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }

    bool got_line = fgets(out, (int)out_size, fp) != NULL;
    size_t len = got_line ? strlen(out) : 0;
    bool truncated = got_line && len > 0 && out[len - 1] != '\n' && !feof(fp);
    git_trim_newlines(out);

    int rc = cbm_pclose(fp);
    if (!got_line || truncated || rc != 0 || out[0] == '\0') {
        out[0] = '\0';
        return CBM_NOT_FOUND;
    }
    return 0;
}

int cbm_git_capture_first_line(const char *repo_path, const char *git_args, char **out) {
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

int cbm_git_run_first_line_buf(const char *repo_path, const char *git_args,
                               char *out, size_t out_size, int *out_exit_code) {
    if (!out || out_size == 0 || !out_exit_code) {
        return CBM_NOT_FOUND;
    }
    out[0] = '\0';
    *out_exit_code = CBM_NOT_FOUND;
    char cmd[CBM_GIT_CMD_BUFSZ];
    if (!cbm_git_format_command(cmd, sizeof(cmd), repo_path, git_args)) {
        return CBM_NOT_FOUND;
    }
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
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
    *out_exit_code = cbm_pclose_exit_code(fp);
    if (truncated || !output_fits) {
        out[0] = '\0';
        return CBM_NOT_FOUND;
    }
    return 0;
}

int cbm_git_drain_command(const char *repo_path, const char *git_args) {
    char cmd[CBM_GIT_CMD_BUFSZ];
    if (!cbm_git_format_command(cmd, sizeof(cmd), repo_path, git_args)) {
        return CBM_NOT_FOUND;
    }
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }
    char drain[CBM_SZ_128];
    while (fgets(drain, (int)sizeof(drain), fp)) {
    }
    return cbm_pclose(fp) == 0 ? 0 : CBM_NOT_FOUND;
}
