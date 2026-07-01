#include "git/git_snapshot.h"

#include "git/git_command.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static const char CBM_GIT_EMPTY_DIRTY_HASH[CBM_GIT_DIRTY_HASH_BUFSZ] = "0000000000000000";
static const uint64_t CBM_GIT_DIRTY_HASH_SEED = 5381u;

static uint64_t git_dirty_hash_update(uint64_t h, const unsigned char *buf, size_t n) {
    for (size_t i = 0; i < n; i++) {
        h = ((h << 5) + h) ^ buf[i];
    }
    return h;
}

static int git_hash_command_output(const char *cmd, uint64_t *hash, int *bytes_read) {
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }
    unsigned char buf[CBM_SZ_1K];
    int total = 0;
    size_t n = 0;
    while ((n = fread(buf, CBM_ALLOC_ONE, sizeof(buf), fp)) > 0) {
        *hash = git_dirty_hash_update(*hash, buf, n);
        if (total <= INT32_MAX - (int)n) {
            total += (int)n;
        } else {
            total = INT32_MAX;
        }
    }
    bool read_error = ferror(fp) != 0;
    int rc = cbm_pclose(fp);
    if (bytes_read) {
        *bytes_read += total;
    }
    return rc == 0 && !read_error ? 0 : CBM_NOT_FOUND;
}

#if !defined(_WIN32)
static bool git_format_submodule_status_command(char *cmd, size_t cmd_size, const char *repo_path) {
    if (!cmd || cmd_size == 0 || !repo_path || !cbm_git_validate_repo_path(repo_path)) {
        return false;
    }
    int n = snprintf(cmd, cmd_size,
                     "git --no-optional-locks -C \"%s\" submodule foreach --quiet --recursive "
                     "\"git status --porcelain --untracked-files=normal 2>/dev/null\" "
                     "2>/dev/null",
                     repo_path);
    return cbm_git_command_fits(n, cmd_size);
}
#endif

bool cbm_git_snapshot_path_supported(const char *repo_path) {
    char cmd[CBM_GIT_CMD_BUFSZ];
    return cbm_git_format_command(cmd, sizeof(cmd), repo_path, "rev-parse --git-dir") &&
           cbm_git_format_command(cmd, sizeof(cmd), repo_path, "rev-parse HEAD") &&
           cbm_git_format_status_command(cmd, sizeof(cmd), repo_path) &&
           cbm_git_format_command(cmd, sizeof(cmd), repo_path, "ls-files");
}

static int git_dirty_hash(const char *repo_path, char *out_hash, size_t out_size) {
    if (!out_hash || out_size < CBM_GIT_DIRTY_HASH_BUFSZ) {
        return CBM_NOT_FOUND;
    }
    memcpy(out_hash, CBM_GIT_EMPTY_DIRTY_HASH, sizeof(CBM_GIT_EMPTY_DIRTY_HASH));

    char cmd[CBM_GIT_CMD_BUFSZ];
    if (!cbm_git_format_status_command(cmd, sizeof(cmd), repo_path)) {
        return CBM_NOT_FOUND;
    }

    uint64_t h = CBM_GIT_DIRTY_HASH_SEED;
    int bytes = 0;
    if (git_hash_command_output(cmd, &h, &bytes) != 0) {
        return CBM_NOT_FOUND;
    }

#if !defined(_WIN32)
    if (git_format_submodule_status_command(cmd, sizeof(cmd), repo_path)) {
        (void)git_hash_command_output(cmd, &h, &bytes);
    }
#endif
    if (bytes <= 0) {
        return 0;
    }

    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    int n = snprintf(out_hash, out_size, "%016llx", (unsigned long long)h);
    return n == CBM_GIT_DIRTY_HASH_HEX_LEN ? bytes : CBM_NOT_FOUND;
}

static int git_file_count(const char *repo_path) {
    char cmd[CBM_GIT_CMD_BUFSZ];
    if (!cbm_git_format_command(cmd, sizeof(cmd), repo_path, "ls-files")) {
        return 0;
    }
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return 0;
    }

    int count = 0;
    char buf[CBM_SZ_1K];
    size_t n = 0;
    while ((n = fread(buf, CBM_ALLOC_ONE, sizeof(buf), fp)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (buf[i] == '\n') {
                count++;
            }
        }
    }
    bool read_error = ferror(fp) != 0;
    int rc = cbm_pclose(fp);
    return rc == 0 && !read_error ? count : 0;
}

int cbm_git_snapshot_read(const char *repo_path, unsigned flags, cbm_git_snapshot_t *out) {
    if (!out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));
    memcpy(out->dirty_hash, CBM_GIT_EMPTY_DIRTY_HASH, sizeof(CBM_GIT_EMPTY_DIRTY_HASH));

    out->path_supported = cbm_git_snapshot_path_supported(repo_path);
    if (!out->path_supported) {
        return CBM_NOT_FOUND;
    }

    out->is_git = cbm_git_drain_command(repo_path, "rev-parse --git-dir") == 0;
    if (!out->is_git) {
        return 0;
    }

    if ((flags & CBM_GIT_SNAPSHOT_HEAD) != 0) {
        (void)cbm_git_capture_first_line_buf(repo_path, "rev-parse HEAD", out->head,
                                             sizeof(out->head));
    }
    if ((flags & CBM_GIT_SNAPSHOT_DIRTY) != 0) {
        out->dirty_bytes = git_dirty_hash(repo_path, out->dirty_hash, sizeof(out->dirty_hash));
        if (out->dirty_bytes < 0) {
            out->dirty_bytes = 0;
            memcpy(out->dirty_hash, CBM_GIT_EMPTY_DIRTY_HASH, sizeof(CBM_GIT_EMPTY_DIRTY_HASH));
        }
    }
    if ((flags & CBM_GIT_SNAPSHOT_FILE_COUNT) != 0) {
        out->file_count = git_file_count(repo_path);
    }
    return 0;
}
