#ifndef CBM_GIT_COMMAND_H
#define CBM_GIT_COMMAND_H

#include <stdbool.h>
#include <stddef.h>

#include "foundation/constants.h"
#include "foundation/subprocess.h"

enum {
    CBM_GIT_OUTPUT_BUFSZ = CBM_SZ_4K,
};

typedef bool (*cbm_git_cancel_requested_fn)(void *context);

typedef struct {
    cbm_git_cancel_requested_fn cancel_requested;
    void *cancel_context;
} cbm_git_run_opts_t;

typedef struct {
    char path[CBM_PATH_MAX];
    size_t size;
} cbm_git_output_t;

/* Git receives repo_path as one argv element, so spaces and shell
 * metacharacters are literal. Only NULL and the empty path are unsupported. */
bool cbm_git_validate_repo_path(const char *repo_path);

/* Resolve the Git executable without a shell. POSIX deliberately returns the
 * literal PATH name for execvp; Windows accepts only an absolute PATH entry so
 * CreateProcessW cannot fall back to the current directory. */
bool cbm_git_resolve_executable(char out[CBM_SZ_4K]);

/* Run {"git", "--no-optional-locks", "-C", repo_path, git_args...} in an
 * owned process tree. stdout is captured in output when non-NULL; stderr is
 * discarded independently. A cancellation callback is sampled while polling
 * and converted into an owned-handle cancellation request. */
int cbm_git_run_argv(const char *repo_path, const char *const git_args[],
                     const cbm_git_run_opts_t *opts, cbm_git_output_t *output,
                     cbm_proc_result_t *result);
void cbm_git_output_cleanup(cbm_git_output_t *output);

/* Remove trailing LF/CRLF bytes from one captured Git output line. */
void cbm_git_trim_newlines(char *line);

int cbm_git_capture_first_line_buf(const char *repo_path, const char *const git_args[],
                                   char *out, size_t out_size);
int cbm_git_capture_first_line(const char *repo_path, const char *const git_args[], char **out);
int cbm_git_run_first_line_buf(const char *repo_path, const char *const git_args[],
                               char *out, size_t out_size, int *out_exit_code);
int cbm_git_drain_command(const char *repo_path, const char *const git_args[]);

#endif
