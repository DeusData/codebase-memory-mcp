#ifndef CBM_GIT_COMMAND_H
#define CBM_GIT_COMMAND_H

#include <stdbool.h>
#include <stddef.h>

#include "foundation/constants.h"

enum {
    CBM_GIT_CMD_BUFSZ = CBM_SZ_1K,
    CBM_GIT_OUTPUT_BUFSZ = CBM_SZ_4K,
};

const char *cbm_git_null_device(void);
bool cbm_git_validate_repo_path(const char *repo_path);
bool cbm_git_command_fits(int n, size_t cmd_size);
bool cbm_git_format_command(char *cmd, size_t cmd_size, const char *repo_path,
                            const char *git_args);
bool cbm_git_format_status_command(char *cmd, size_t cmd_size, const char *repo_path);
int cbm_git_capture_first_line_buf(const char *repo_path, const char *git_args,
                                   char *out, size_t out_size);
int cbm_git_capture_first_line(const char *repo_path, const char *git_args, char **out);
int cbm_git_run_first_line_buf(const char *repo_path, const char *git_args,
                               char *out, size_t out_size, int *out_exit_code);
int cbm_git_drain_command(const char *repo_path, const char *git_args);

#endif
