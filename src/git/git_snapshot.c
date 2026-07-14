#include "git/git_snapshot.h"

#include "git/git_command.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static const char CBM_GIT_EMPTY_DIRTY_HASH[CBM_GIT_DIRTY_HASH_BUFSZ] = "0000000000000000";
static const uint64_t CBM_GIT_DIRTY_HASH_SEED = 5381u;
enum { CBM_GIT_STATUS_PREFIX_LEN = 3 };

static int git_status_paths_parse_z(const char *buf, size_t len, char ***out_paths,
                                    int *out_count);

static uint64_t git_dirty_hash_update(uint64_t h, const unsigned char *buf, size_t n) {
    for (size_t i = 0; i < n; i++) {
        h = ((h << 5) + h) ^ buf[i];
    }
    return h;
}

static int64_t git_snapshot_mtime_ns(const struct stat *st) {
#if defined(__APPLE__)
    return ((int64_t)st->st_mtimespec.tv_sec * (int64_t)CBM_NSEC_PER_SEC) +
           (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * (int64_t)CBM_NSEC_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * (int64_t)CBM_NSEC_PER_SEC) +
           (int64_t)st->st_mtim.tv_nsec;
#endif
}

static void git_dirty_hash_file_metadata(const char *repo_path, char **paths, int path_count,
                                         uint64_t *hash) {
    for (int i = 0; i < path_count; i++) {
        char abs_path[CBM_PATH_MAX];
        int n = snprintf(abs_path, sizeof(abs_path), "%s/%s", repo_path, paths[i]);
        if (n < 0 || (size_t)n >= sizeof(abs_path)) {
            continue;
        }
        struct stat st;
        if (stat(abs_path, &st) == 0) {
            int64_t mtime_ns = git_snapshot_mtime_ns(&st);
            int64_t size = (int64_t)st.st_size;
            *hash = git_dirty_hash_update(*hash, (const unsigned char *)&mtime_ns,
                                          sizeof(mtime_ns));
            *hash = git_dirty_hash_update(*hash, (const unsigned char *)&size, sizeof(size));
        }
    }
}

static int git_capture_command(const char *cmd, char **out, size_t *out_len) {
    *out = NULL;
    *out_len = 0;
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    char chunk[CBM_SZ_4K];
    size_t got = 0;
    while ((got = fread(chunk, CBM_ALLOC_ONE, sizeof(chunk), fp)) > 0) {
        if (got > SIZE_MAX - len - 1) {
            free(buf);
            (void)cbm_pclose(fp);
            return CBM_NOT_FOUND;
        }
        if (len + got + 1 > cap) {
            size_t new_cap = cap > 0 ? cap : CBM_SZ_4K;
            while (new_cap < len + got + 1) {
                if (new_cap > SIZE_MAX / PAIR_LEN) {
                    free(buf);
                    (void)cbm_pclose(fp);
                    return CBM_NOT_FOUND;
                }
                new_cap *= PAIR_LEN;
            }
            char *tmp = safe_realloc(buf, new_cap);
            if (!tmp) {
                free(buf);
                (void)cbm_pclose(fp);
                return CBM_NOT_FOUND;
            }
            buf = tmp;
            cap = new_cap;
        }
        memcpy(buf + len, chunk, got);
        len += got;
    }
    bool read_error = ferror(fp) != 0;
    int rc = cbm_pclose(fp);
    if (read_error || rc != 0) {
        free(buf);
        return CBM_NOT_FOUND;
    }
    if (buf) {
        buf[len] = '\0';
    }
    *out = buf;
    *out_len = len;
    return 0;
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
    int command_len = snprintf(cmd, sizeof(cmd),
                               "git --no-optional-locks -C \"%s\" status --porcelain=v1 -z "
                               "--untracked-files=all 2>%s",
                               repo_path, cbm_git_null_device());
    if (!cbm_git_command_fits(command_len, sizeof(cmd))) {
        return CBM_NOT_FOUND;
    }

    uint64_t h = CBM_GIT_DIRTY_HASH_SEED;
    char *status = NULL;
    size_t status_len = 0;
    if (git_capture_command(cmd, &status, &status_len) != 0) {
        return CBM_NOT_FOUND;
    }
    h = git_dirty_hash_update(h, (const unsigned char *)status, status_len);
    int bytes = status_len > (size_t)INT32_MAX ? INT32_MAX : (int)status_len;

    char **paths = NULL;
    int path_count = 0;
    if (status_len > 0 &&
        git_status_paths_parse_z(status, status_len, &paths, &path_count) != 0) {
        free(status);
        return CBM_NOT_FOUND;
    }
    free(status);

#if !defined(_WIN32)
    if (git_format_submodule_status_command(cmd, sizeof(cmd), repo_path)) {
        (void)git_hash_command_output(cmd, &h, &bytes);
    }
#endif
    if (bytes <= 0) {
        cbm_git_status_paths_free(paths, path_count);
        return 0;
    }

    /* #937: porcelain text alone is stable while an already-dirty file keeps
     * changing. Fold metadata so each distinct dirty state is indexed once. */
    git_dirty_hash_file_metadata(repo_path, paths, path_count, &h);
    cbm_git_status_paths_free(paths, path_count);

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

static int git_status_paths_add(char ***paths, int *count, int *cap, const char *path) {
    if (!path || !path[0]) {
        return 0;
    }
    if (*count >= *cap) {
        if (*cap > INT32_MAX / PAIR_LEN) {
            return CBM_NOT_FOUND;
        }
        int new_cap = *cap > 0 ? *cap * PAIR_LEN : CBM_SZ_16;
        if ((size_t)new_cap > SIZE_MAX / sizeof(**paths)) {
            return CBM_NOT_FOUND;
        }
        char **tmp = (char **)safe_realloc(*paths, (size_t)new_cap * sizeof(**paths));
        if (!tmp) {
            return CBM_NOT_FOUND;
        }
        *paths = tmp;
        *cap = new_cap;
    }
    (*paths)[*count] = cbm_strdup(path);
    if (!(*paths)[*count]) {
        return CBM_NOT_FOUND;
    }
    (*count)++;
    return 0;
}

static size_t git_status_record_len(const char *s, size_t max_len) {
    size_t len = 0;
    while (len < max_len && s[len] != '\0') {
        len++;
    }
    return len;
}

static int git_status_paths_parse_z(const char *buf, size_t len, char ***out_paths,
                                    int *out_count) {
    char **paths = NULL;
    int count = 0;
    int cap = 0;
    size_t pos = 0;
    while (pos < len) {
        const char *rec = buf + pos;
        size_t rec_len = git_status_record_len(rec, len - pos);
        if (rec_len == 0) {
            pos++;
            continue;
        }
        if (rec_len >= CBM_GIT_STATUS_PREFIX_LEN && rec[PAIR_LEN] == ' ') {
            bool has_extra_path = rec[0] == 'R' || rec[0] == 'C' || rec[1] == 'R' ||
                                  rec[1] == 'C';
            if (git_status_paths_add(&paths, &count, &cap, rec + CBM_GIT_STATUS_PREFIX_LEN) != 0) {
                cbm_git_status_paths_free(paths, count);
                return CBM_NOT_FOUND;
            }
            pos += rec_len + 1;
            if (has_extra_path && pos < len) {
                const char *old_path = buf + pos;
                size_t old_len = git_status_record_len(old_path, len - pos);
                if (old_len > 0 &&
                    git_status_paths_add(&paths, &count, &cap, old_path) != 0) {
                    cbm_git_status_paths_free(paths, count);
                    return CBM_NOT_FOUND;
                }
                pos += old_len + 1;
            }
            continue;
        }
        if (git_status_paths_add(&paths, &count, &cap, rec) != 0) {
            cbm_git_status_paths_free(paths, count);
            return CBM_NOT_FOUND;
        }
        pos += rec_len + 1;
    }
    *out_paths = paths;
    *out_count = count;
    return 0;
}

void cbm_git_status_paths_free(char **paths, int count) {
    if (!paths) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(paths[i]);
    }
    free(paths);
}

int cbm_git_status_paths(const char *repo_path, char ***out_paths, int *out_count) {
    if (out_paths) {
        *out_paths = NULL;
    }
    if (out_count) {
        *out_count = 0;
    }
    if (!repo_path || !out_paths || !out_count || !cbm_git_validate_repo_path(repo_path)) {
        return CBM_NOT_FOUND;
    }

    char cmd[CBM_GIT_CMD_BUFSZ];
    int n = snprintf(cmd, sizeof(cmd),
                     "git --no-optional-locks -C \"%s\" status --porcelain=v1 -z "
                     "--untracked-files=all 2>%s",
                     repo_path, cbm_git_null_device());
    if (!cbm_git_command_fits(n, sizeof(cmd))) {
        return CBM_NOT_FOUND;
    }

    char *buf = NULL;
    size_t len = 0;
    int rc = git_capture_command(cmd, &buf, &len);
    if (rc != 0) {
        return CBM_NOT_FOUND;
    }
    if (!buf) {
        return 0;
    }
    rc = git_status_paths_parse_z(buf, len, out_paths, out_count);
    free(buf);
    return rc;
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
