/*
 * config_toml_edit.c — Fail-closed edits for agent TOML configuration.
 */
#include "cli/config_toml_edit.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include "foundation/win_utf8.h"

#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#define toml_close _close
#define toml_fdopen _fdopen
#define TOML_SYNC _commit
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#define toml_close close
#define toml_fdopen fdopen
#define TOML_SYNC fsync
#endif

#define TOML_EDIT_OK 0
#define TOML_EDIT_FOREIGN 1
#define TOML_EDIT_ERR (-1)
#define TOML_EDIT_MAX_BYTES (16U * 1024U * 1024U)
#define TOML_EDIT_MAX_PATH_BYTES 32768U

#ifdef CBM_TOML_EDIT_ENABLE_TEST_API
static CBM_TLS cbm_toml_precommit_test_hook_t toml_precommit_test_hook = NULL;
static CBM_TLS void *toml_precommit_test_context = NULL;
static CBM_TLS cbm_toml_precommit_test_hook_t toml_prepublish_test_hook = NULL;
static CBM_TLS void *toml_prepublish_test_context = NULL;
#endif

typedef struct {
    int exists;
#ifdef _WIN32
    DWORD volume_serial;
    DWORD file_index_high;
    DWORD file_index_low;
    DWORD attributes;
    DWORD link_count;
    FILETIME creation_time;
    FILETIME write_time;
    uint64_t size;
#else
    dev_t device;
    ino_t inode;
    mode_t mode;
    nlink_t link_count;
    uid_t owner;
    gid_t group;
    off_t size;
    int64_t modified_sec;
    long modified_nsec;
    int64_t changed_sec;
    long changed_nsec;
#endif
} toml_file_snapshot_t;

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} toml_buffer_t;

typedef struct {
    size_t start;
    size_t content_end;
    size_t full_end;
} toml_line_t;

typedef struct {
    char *data;
    size_t len;
    size_t consumed;
} toml_string_t;

typedef struct {
    char *data;
    size_t len;
    size_t count;
} toml_key_path_t;

typedef struct {
    int present;
    int array;
    int target;
    size_t edit_start;
    toml_key_path_t path;
} toml_header_t;

typedef struct {
    int present;
    int multiline_value;
    toml_key_path_t key;
    size_t value_start;
    size_t value_end;
} toml_assignment_t;

typedef struct {
    int matching_count;
    size_t start;
    size_t header_end;
    size_t direct_end;
    size_t edit_end;
} toml_table_scan_t;

typedef struct {
    int active;
    int descendants;
    int identity_count;
    int identity_matches;
    size_t start;
    size_t header_end;
    size_t direct_end;
    size_t direct_significant_end;
    size_t last_significant_end;
} toml_target_table_t;

typedef struct {
    toml_key_path_t key;
    toml_line_t line;
} toml_body_entry_t;

typedef struct {
    toml_body_entry_t *entries;
    size_t count;
    size_t capacity;
} toml_body_spec_t;

static int toml_managed_block_conflicts(const char *existing, size_t existing_len,
                                        size_t exclude_start, size_t exclude_end, const char *block,
                                        size_t block_len);

static void toml_buffer_dispose(toml_buffer_t *buffer) {
    if (!buffer) {
        return;
    }
    free(buffer->data);
    buffer->data = NULL;
    buffer->len = 0;
    buffer->cap = 0;
}

static int toml_buffer_reserve(toml_buffer_t *buffer, size_t additional) {
    if (!buffer || buffer->len == SIZE_MAX || additional > SIZE_MAX - buffer->len - 1) {
        return TOML_EDIT_ERR;
    }
    size_t needed = buffer->len + additional + 1;
    if (needed > (size_t)TOML_EDIT_MAX_BYTES + 1U) {
        return TOML_EDIT_ERR;
    }
    if (needed <= buffer->cap) {
        return TOML_EDIT_OK;
    }

    size_t cap = buffer->cap ? buffer->cap : 128;
    while (cap < needed) {
        if (cap > SIZE_MAX / 2) {
            cap = needed;
            break;
        }
        cap *= 2;
    }
    char *grown = (char *)realloc(buffer->data, cap);
    if (!grown) {
        return TOML_EDIT_ERR;
    }
    buffer->data = grown;
    buffer->cap = cap;
    return TOML_EDIT_OK;
}

static int toml_buffer_append(toml_buffer_t *buffer, const char *data, size_t len) {
    if ((!data && len != 0) || toml_buffer_reserve(buffer, len) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    if (len != 0) {
        memcpy(buffer->data + buffer->len, data, len);
        buffer->len += len;
    }
    buffer->data[buffer->len] = '\0';
    return TOML_EDIT_OK;
}

static int toml_buffer_append_char(toml_buffer_t *buffer, char value) {
    return toml_buffer_append(buffer, &value, 1);
}

static int toml_buffer_append_cstr(toml_buffer_t *buffer, const char *value) {
    return value ? toml_buffer_append(buffer, value, strlen(value)) : TOML_EDIT_ERR;
}

static int toml_utf8_is_valid(const char *text, size_t len) {
    size_t pos = 0;
    while (pos < len) {
        unsigned char first = (unsigned char)text[pos++];
        if (first <= 0x7f) {
            continue;
        }
        size_t continuation_count;
        uint32_t codepoint;
        if (first >= 0xc2 && first <= 0xdf) {
            continuation_count = 1;
            codepoint = first & 0x1f;
        } else if (first >= 0xe0 && first <= 0xef) {
            continuation_count = 2;
            codepoint = first & 0x0f;
        } else if (first >= 0xf0 && first <= 0xf4) {
            continuation_count = 3;
            codepoint = first & 0x07;
        } else {
            return 0;
        }
        if (continuation_count > len - pos) {
            return 0;
        }
        for (size_t i = 0; i < continuation_count; ++i) {
            unsigned char next = (unsigned char)text[pos++];
            if ((next & 0xc0) != 0x80) {
                return 0;
            }
            codepoint = (codepoint << 6) | (uint32_t)(next & 0x3f);
        }
        if ((continuation_count == 1 && codepoint < 0x80) ||
            (continuation_count == 2 && codepoint < 0x800) ||
            (continuation_count == 3 && codepoint < 0x10000) || codepoint > 0x10ffff ||
            (codepoint >= 0xd800 && codepoint <= 0xdfff)) {
            return 0;
        }
    }
    return 1;
}

static int toml_text_is_safe(const char *text, size_t len, int multiline) {
    if (!text && len != 0) {
        return 0;
    }
    for (size_t i = 0; i < len; ++i) {
        unsigned char ch = (unsigned char)text[i];
        if (ch == 0 || ch == 0x7f) {
            return 0;
        }
        if (ch < 0x20 && !(multiline && (ch == '\t' || ch == '\n' || ch == '\r'))) {
            return 0;
        }
        if (ch == '\r' && (!multiline || i + 1U >= len || text[i + 1U] != '\n')) {
            return 0;
        }
    }
    return toml_utf8_is_valid(text, len);
}

static int toml_bounded_length(const char *text, size_t maximum, size_t *length_out) {
    if (!text || !length_out) {
        return TOML_EDIT_ERR;
    }
    size_t length = 0U;
    while (length <= maximum && text[length] != '\0') {
        length++;
    }
    if (length > maximum) {
        return TOML_EDIT_ERR;
    }
    *length_out = length;
    return TOML_EDIT_OK;
}

static int toml_valid_path(const char *path) {
    size_t len = 0U;
    return path && path[0] != '\0' &&
           toml_bounded_length(path, TOML_EDIT_MAX_PATH_BYTES, &len) == TOML_EDIT_OK &&
           toml_text_is_safe(path, len, 0);
}

static int toml_is_bare_key_char(unsigned char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
           ch == '_' || ch == '-';
}

static int toml_valid_identifier(const char *identifier) {
    size_t length = 0U;
    if (!identifier || identifier[0] == '\0' ||
        toml_bounded_length(identifier, 4096U, &length) != TOML_EDIT_OK) {
        return 0;
    }
    for (size_t i = 0U; i < length; ++i) {
        if (!toml_is_bare_key_char((unsigned char)identifier[i])) {
            return 0;
        }
    }
    return 1;
}

static int toml_valid_marker(const char *marker) {
    if (!marker || marker[0] == '\0') {
        return 0;
    }
    size_t len = 0U;
    if (toml_bounded_length(marker, 4096U, &len) != TOML_EDIT_OK) {
        return 0;
    }
    return toml_text_is_safe(marker, len, 0) && !strchr(marker, '\n') && !strchr(marker, '\r');
}

static int toml_snapshot_equal(const toml_file_snapshot_t *left,
                               const toml_file_snapshot_t *right) {
    if (left->exists != right->exists) {
        return 0;
    }
    if (!left->exists) {
        return 1;
    }
#ifdef _WIN32
    return left->volume_serial == right->volume_serial &&
           left->file_index_high == right->file_index_high &&
           left->file_index_low == right->file_index_low && left->attributes == right->attributes &&
           left->link_count == right->link_count &&
           left->creation_time.dwLowDateTime == right->creation_time.dwLowDateTime &&
           left->creation_time.dwHighDateTime == right->creation_time.dwHighDateTime &&
           left->write_time.dwLowDateTime == right->write_time.dwLowDateTime &&
           left->write_time.dwHighDateTime == right->write_time.dwHighDateTime &&
           left->size == right->size;
#else
    return left->device == right->device && left->inode == right->inode &&
           left->mode == right->mode && left->link_count == right->link_count &&
           left->owner == right->owner && left->group == right->group &&
           left->size == right->size && left->modified_sec == right->modified_sec &&
           left->modified_nsec == right->modified_nsec && left->changed_sec == right->changed_sec &&
           left->changed_nsec == right->changed_nsec;
#endif
}

#ifdef _WIN32
static int toml_snapshot_from_handle(HANDLE handle, toml_file_snapshot_t *snapshot) {
    BY_HANDLE_FILE_INFORMATION info;
    if (GetFileType(handle) != FILE_TYPE_DISK || !GetFileInformationByHandle(handle, &info) ||
        (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0 ||
        info.nNumberOfLinks != 1U || (info.nFileIndexHigh == 0U && info.nFileIndexLow == 0U)) {
        return TOML_EDIT_ERR;
    }
    uint64_t size = ((uint64_t)info.nFileSizeHigh << 32U) | (uint64_t)info.nFileSizeLow;
    if (size > TOML_EDIT_MAX_BYTES) {
        return TOML_EDIT_ERR;
    }
    *snapshot = (toml_file_snapshot_t){
        .exists = 1,
        .volume_serial = info.dwVolumeSerialNumber,
        .file_index_high = info.nFileIndexHigh,
        .file_index_low = info.nFileIndexLow,
        .attributes = info.dwFileAttributes,
        .link_count = info.nNumberOfLinks,
        .creation_time = info.ftCreationTime,
        .write_time = info.ftLastWriteTime,
        .size = size,
    };
    return TOML_EDIT_OK;
}
#else
static int toml_snapshot_from_stat(const struct stat *state, toml_file_snapshot_t *snapshot) {
    if (!S_ISREG(state->st_mode) || state->st_ino == 0 || state->st_nlink != 1U ||
        state->st_size < 0 || (uint64_t)state->st_size > TOML_EDIT_MAX_BYTES ||
        (state->st_mode & (S_ISUID | S_ISGID | S_ISVTX)) != 0) {
        return TOML_EDIT_ERR;
    }
    *snapshot = (toml_file_snapshot_t){
        .exists = 1,
        .device = state->st_dev,
        .inode = state->st_ino,
        .mode = state->st_mode,
        .link_count = state->st_nlink,
        .owner = state->st_uid,
        .group = state->st_gid,
        .size = state->st_size,
#ifdef __APPLE__
        .modified_sec = state->st_mtimespec.tv_sec,
        .modified_nsec = state->st_mtimespec.tv_nsec,
        .changed_sec = state->st_ctimespec.tv_sec,
        .changed_nsec = state->st_ctimespec.tv_nsec,
#else
        .modified_sec = state->st_mtim.tv_sec,
        .modified_nsec = state->st_mtim.tv_nsec,
        .changed_sec = state->st_ctim.tv_sec,
        .changed_nsec = state->st_ctim.tv_nsec,
#endif
    };
    return TOML_EDIT_OK;
}
#endif

static int toml_read_file(const char *path, char **out_data, size_t *out_len,
                          toml_file_snapshot_t *snapshot_out) {
    if (!path || !out_data || !out_len || !snapshot_out) {
        return TOML_EDIT_ERR;
    }
    *out_data = NULL;
    *out_len = 0;
    memset(snapshot_out, 0, sizeof(*snapshot_out));

#ifdef _WIN32
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_path) {
        return TOML_EDIT_ERR;
    }
    HANDLE handle = CreateFileW(
        wide_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    free(wide_path);
    if (handle == INVALID_HANDLE_VALUE) {
        DWORD error = GetLastError();
        if (error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND) {
            return TOML_EDIT_ERR;
        }
        char *empty = (char *)malloc(1);
        if (!empty) {
            return TOML_EDIT_ERR;
        }
        empty[0] = '\0';
        *out_data = empty;
        return TOML_EDIT_OK;
    }
    toml_file_snapshot_t before;
    if (toml_snapshot_from_handle(handle, &before) != TOML_EDIT_OK) {
        CloseHandle(handle);
        return TOML_EDIT_ERR;
    }
    size_t size = (size_t)before.size;
    char *data = (char *)malloc(size + 1U);
    if (!data) {
        CloseHandle(handle);
        return TOML_EDIT_ERR;
    }
    DWORD read_count = 0U;
    BOOL read_ok = ReadFile(handle, data, (DWORD)size, &read_count, NULL);
    toml_file_snapshot_t after;
    int after_result = toml_snapshot_from_handle(handle, &after);
    BOOL close_ok = CloseHandle(handle);
    if (!read_ok || read_count != (DWORD)size || after_result != TOML_EDIT_OK || !close_ok ||
        !toml_snapshot_equal(&before, &after)) {
        free(data);
        return TOML_EDIT_ERR;
    }
#else
#ifndef O_NOFOLLOW
    return TOML_EDIT_ERR;
#else
    int flags = O_RDONLY | O_NOFOLLOW | O_NONBLOCK;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int fd = open(path, flags);
    if (fd < 0) {
        if (errno != ENOENT) {
            return TOML_EDIT_ERR;
        }
        struct stat path_state;
        if (lstat(path, &path_state) == 0 || errno != ENOENT) {
            return TOML_EDIT_ERR;
        }
        char *empty = (char *)malloc(1U);
        if (!empty) {
            return TOML_EDIT_ERR;
        }
        empty[0] = '\0';
        *out_data = empty;
        return TOML_EDIT_OK;
    }
    struct stat before_state;
    toml_file_snapshot_t before;
    if (fstat(fd, &before_state) != 0 ||
        toml_snapshot_from_stat(&before_state, &before) != TOML_EDIT_OK) {
        toml_close(fd);
        return TOML_EDIT_ERR;
    }
    FILE *file = toml_fdopen(fd, "rb");
    if (!file) {
        toml_close(fd);
        return TOML_EDIT_ERR;
    }
    size_t size = (size_t)before.size;
    char *data = (char *)malloc(size + 1U);
    if (!data) {
        (void)fclose(file);
        return TOML_EDIT_ERR;
    }
    size_t read_count = size ? fread(data, 1, size, file) : 0;
    int read_error = ferror(file);
    struct stat after_state;
    toml_file_snapshot_t after;
    int after_result = fstat(cbm_fileno(file), &after_state) == 0
                           ? toml_snapshot_from_stat(&after_state, &after)
                           : TOML_EDIT_ERR;
    int close_error = fclose(file);
    if (read_count != size || read_error || close_error != 0 || after_result != TOML_EDIT_OK ||
        !toml_snapshot_equal(&before, &after)) {
        free(data);
        return TOML_EDIT_ERR;
    }
#endif
#endif
    data[size] = '\0';
    *out_data = data;
    *out_len = size;
    *snapshot_out = before;
    return TOML_EDIT_OK;
}

#ifndef _WIN32
static char *toml_parent_directory(const char *path) {
    const char *separator = strrchr(path, '/');
    if (!separator) {
        return cbm_strdup(".");
    }
    if (separator == path) {
        return cbm_strdup("/");
    }
    return cbm_strndup(path, (size_t)(separator - path));
}
#endif

static int toml_snapshot_matches_path(const char *path, const char *old_data, size_t old_len,
                                      const toml_file_snapshot_t *expected) {
    char *current = NULL;
    size_t current_len = 0U;
    toml_file_snapshot_t current_snapshot;
    if (toml_read_file(path, &current, &current_len, &current_snapshot) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    int matches = expected->exists == current_snapshot.exists &&
                  toml_snapshot_equal(expected, &current_snapshot) && current_len == old_len &&
                  (old_len == 0U || memcmp(current, old_data, old_len) == 0);
    free(current);
    return matches ? TOML_EDIT_OK : TOML_EDIT_ERR;
}

#ifndef _WIN32
static int toml_sync_parent_directory(const char *path) {
    char *parent = toml_parent_directory(path);
    if (!parent) {
        return TOML_EDIT_ERR;
    }
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int fd = open(parent, flags);
    free(parent);
    if (fd < 0) {
        return TOML_EDIT_ERR;
    }
    struct stat state;
    int result = fstat(fd, &state) == 0 && S_ISDIR(state.st_mode) && fsync(fd) == 0 ? TOML_EDIT_OK
                                                                                    : TOML_EDIT_ERR;
    if (toml_close(fd) != 0) {
        result = TOML_EDIT_ERR;
    }
    return result;
}
#endif

static int toml_replace_atomic(const char *temp_path, const char *path, int existed) {
#ifdef _WIN32
    wchar_t *wide_temp = cbm_utf8_to_wide(temp_path);
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_temp || !wide_path) {
        free(wide_temp);
        free(wide_path);
        return TOML_EDIT_ERR;
    }
    /* ReplaceFileW preserves the destination ACL and other mergeable metadata;
     * merge failures stay fatal so metadata is never silently discarded. */
    BOOL replaced =
        existed ? ReplaceFileW(wide_path, wide_temp, NULL, REPLACEFILE_WRITE_THROUGH, NULL, NULL)
                : MoveFileExW(wide_temp, wide_path, MOVEFILE_WRITE_THROUGH);
    free(wide_temp);
    free(wide_path);
    return replaced ? TOML_EDIT_OK : TOML_EDIT_ERR;
#else
    if (!existed) {
        if (link(temp_path, path) != 0) {
            return TOML_EDIT_ERR;
        }
        if (cbm_unlink(temp_path) != 0) {
            return TOML_EDIT_ERR;
        }
        return toml_sync_parent_directory(path);
    }
    if (rename(temp_path, path) != 0) {
        return TOML_EDIT_ERR;
    }
    return toml_sync_parent_directory(path);
#endif
}

static int toml_write_atomic(const char *path, const char *old_data, size_t old_len,
                             const char *new_data, size_t new_len,
                             const toml_file_snapshot_t *snapshot) {
    if (old_len > TOML_EDIT_MAX_BYTES || new_len > TOML_EDIT_MAX_BYTES) {
        return TOML_EDIT_ERR;
    }
    if (old_len == new_len && (old_len == 0 || memcmp(old_data, new_data, old_len) == 0)) {
        return TOML_EDIT_OK;
    }
    size_t path_len = strlen(path);
    static const char suffix[] = ".XXXXXX";
    if (path_len > SIZE_MAX - sizeof(suffix)) {
        return TOML_EDIT_ERR;
    }
    char *temp_path = (char *)malloc(path_len + sizeof(suffix));
    if (!temp_path) {
        return TOML_EDIT_ERR;
    }
    memcpy(temp_path, path, path_len);
    memcpy(temp_path + path_len, suffix, sizeof(suffix));

    int fd = cbm_mkstemp(temp_path);
    if (fd < 0) {
        free(temp_path);
        return TOML_EDIT_ERR;
    }
    FILE *file = toml_fdopen(fd, "wb");
    if (!file) {
        (void)toml_close(fd);
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TOML_EDIT_ERR;
    }

    int failed = new_len != 0 && fwrite(new_data, 1, new_len, file) != new_len;
    if (!failed && fflush(file) != 0) {
        failed = 1;
    }
#ifndef _WIN32
    if (!failed && snapshot->exists &&
        fchown(cbm_fileno(file), snapshot->owner, snapshot->group) != 0) {
        failed = 1;
    }
    mode_t mode = snapshot->exists ? snapshot->mode & 0777U : 0600U;
    if (!failed && fchmod(cbm_fileno(file), mode) != 0) {
        failed = 1;
    }
#endif
    if (!failed && TOML_SYNC(cbm_fileno(file)) != 0) {
        failed = 1;
    }
    if (fclose(file) != 0) {
        failed = 1;
    }
    if (failed) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TOML_EDIT_ERR;
    }
    char *temp_data = NULL;
    size_t temp_len = 0U;
    toml_file_snapshot_t temp_snapshot;
    if (toml_read_file(temp_path, &temp_data, &temp_len, &temp_snapshot) != TOML_EDIT_OK ||
        !temp_snapshot.exists || temp_len != new_len ||
        (new_len != 0U && memcmp(temp_data, new_data, new_len) != 0)) {
        free(temp_data);
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TOML_EDIT_ERR;
    }
    free(temp_data);
#ifdef CBM_TOML_EDIT_ENABLE_TEST_API
    if (toml_precommit_test_hook) {
        toml_precommit_test_hook(path, toml_precommit_test_context);
    }
#endif
    if (toml_snapshot_matches_path(path, old_data, old_len, snapshot) != TOML_EDIT_OK) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TOML_EDIT_ERR;
    }
#ifdef CBM_TOML_EDIT_ENABLE_TEST_API
    if (toml_prepublish_test_hook) {
        toml_prepublish_test_hook(path, toml_prepublish_test_context);
    }
#endif
    if (toml_snapshot_matches_path(path, old_data, old_len, snapshot) != TOML_EDIT_OK ||
        toml_snapshot_matches_path(temp_path, new_data, new_len, &temp_snapshot) != TOML_EDIT_OK ||
        toml_replace_atomic(temp_path, path, snapshot->exists) != TOML_EDIT_OK) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TOML_EDIT_ERR;
    }
    free(temp_path);
    return TOML_EDIT_OK;
}

#ifdef CBM_TOML_EDIT_ENABLE_TEST_API
void cbm_toml_set_precommit_hook_for_testing(cbm_toml_precommit_test_hook_t hook, void *context) {
    toml_precommit_test_hook = hook;
    toml_precommit_test_context = context;
}

void cbm_toml_set_prepublish_hook_for_testing(cbm_toml_precommit_test_hook_t hook, void *context) {
    toml_prepublish_test_hook = hook;
    toml_prepublish_test_context = context;
}
#endif

static int toml_next_line(const char *data, size_t len, size_t *cursor, toml_line_t *line) {
    if (!data || !cursor || !line || *cursor >= len) {
        return 0;
    }
    size_t pos = *cursor;
    line->start = pos;
    while (pos < len && data[pos] != '\n') {
        ++pos;
    }
    line->content_end = pos;
    if (line->content_end > line->start && data[line->content_end - 1] == '\r') {
        --line->content_end;
    }
    line->full_end = pos < len ? pos + 1 : pos;
    *cursor = line->full_end;
    return 1;
}

static int toml_line_equals(const char *data, const toml_line_t *line, const char *value) {
    size_t start = line->start;
    if (start == 0U && line->content_end >= 3U && (unsigned char)data[0] == 0xefU &&
        (unsigned char)data[1] == 0xbbU && (unsigned char)data[2] == 0xbfU) {
        start = 3U;
    }
    size_t line_len = line->content_end - start;
    size_t value_len = strlen(value);
    return line_len == value_len && memcmp(data + start, value, value_len) == 0;
}

enum {
    TOML_STRING_NONE = 0,
    TOML_STRING_MULTILINE_BASIC = 1,
    TOML_STRING_MULTILINE_LITERAL = 2,
};

static int toml_scan_line_strings(const char *data, const toml_line_t *line, int *multiline_state) {
    size_t pos = line->start;
    if (line->start == 0 && line->content_end >= 3U && (unsigned char)data[0] == 0xefU &&
        (unsigned char)data[1] == 0xbbU && (unsigned char)data[2] == 0xbfU) {
        pos = 3U;
    }
    while (pos < line->content_end) {
        if (*multiline_state != TOML_STRING_NONE) {
            char quote = *multiline_state == TOML_STRING_MULTILINE_BASIC ? '"' : '\'';
            if (*multiline_state == TOML_STRING_MULTILINE_BASIC && data[pos] == '\\') {
                pos += pos + 1U < line->content_end ? 2U : 1U;
                continue;
            }
            if (pos + 2U < line->content_end && data[pos] == quote && data[pos + 1U] == quote &&
                data[pos + 2U] == quote) {
                *multiline_state = TOML_STRING_NONE;
                pos += 3U;
                continue;
            }
            pos++;
            continue;
        }

        if (data[pos] == '#') {
            return TOML_EDIT_OK;
        }
        if (data[pos] != '"' && data[pos] != '\'') {
            pos++;
            continue;
        }
        char quote = data[pos];
        if (pos + 2U < line->content_end && data[pos + 1U] == quote && data[pos + 2U] == quote) {
            *multiline_state =
                quote == '"' ? TOML_STRING_MULTILINE_BASIC : TOML_STRING_MULTILINE_LITERAL;
            pos += 3U;
            continue;
        }
        pos++;
        int closed = 0;
        while (pos < line->content_end) {
            if (quote == '"' && data[pos] == '\\') {
                if (pos + 1U >= line->content_end) {
                    return TOML_EDIT_ERR;
                }
                pos += 2U;
                continue;
            }
            if (data[pos] == quote) {
                pos++;
                closed = 1;
                break;
            }
            pos++;
        }
        if (!closed) {
            return TOML_EDIT_ERR;
        }
    }
    return TOML_EDIT_OK;
}

static int toml_validate_lexical_strings(const char *data, size_t len) {
    size_t cursor = 0U;
    toml_line_t line;
    int multiline_state = TOML_STRING_NONE;
    while (toml_next_line(data, len, &cursor, &line)) {
        if (toml_scan_line_strings(data, &line, &multiline_state) != TOML_EDIT_OK) {
            return TOML_EDIT_ERR;
        }
    }
    return multiline_state == TOML_STRING_NONE ? TOML_EDIT_OK : TOML_EDIT_ERR;
}

static int toml_find_markers(const char *data, size_t len, const char *begin_marker,
                             const char *end_marker, toml_line_t *begin_line, toml_line_t *end_line,
                             int *has_pair) {
    int begin_count = 0;
    int end_count = 0;
    size_t cursor = 0;
    toml_line_t line;
    int multiline_state = TOML_STRING_NONE;
    while (toml_next_line(data, len, &cursor, &line)) {
        int line_in_multiline = multiline_state != TOML_STRING_NONE;
        if (!line_in_multiline && toml_line_equals(data, &line, begin_marker)) {
            ++begin_count;
            *begin_line = line;
        }
        if (!line_in_multiline && toml_line_equals(data, &line, end_marker)) {
            ++end_count;
            *end_line = line;
        }
        if (toml_scan_line_strings(data, &line, &multiline_state) != TOML_EDIT_OK) {
            return TOML_EDIT_ERR;
        }
    }
    if (multiline_state != TOML_STRING_NONE) {
        return TOML_EDIT_ERR;
    }
    if (begin_count == 0 && end_count == 0) {
        *has_pair = 0;
        return TOML_EDIT_OK;
    }
    if (begin_count != 1 || end_count != 1 || begin_line->start >= end_line->start) {
        return TOML_EDIT_ERR;
    }
    *has_pair = 1;
    return TOML_EDIT_OK;
}

static const char *toml_newline_style(const char *data, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        if (data[i] == '\n') {
            return i > 0U && data[i - 1U] == '\r' ? "\r\n" : "\n";
        }
    }
    return "\n";
}

static int toml_append_normalized_text(toml_buffer_t *output, const char *text, size_t len,
                                       const char *newline) {
    size_t cursor = 0U;
    toml_line_t line;
    while (toml_next_line(text, len, &cursor, &line)) {
        if (toml_buffer_append(output, text + line.start, line.content_end - line.start) !=
            TOML_EDIT_OK) {
            return TOML_EDIT_ERR;
        }
        if (line.full_end > line.content_end &&
            toml_buffer_append_cstr(output, newline) != TOML_EDIT_OK) {
            return TOML_EDIT_ERR;
        }
    }
    return TOML_EDIT_OK;
}

static int toml_contains_marker_line(const char *data, size_t len, const char *begin_marker,
                                     const char *end_marker) {
    size_t cursor = 0U;
    toml_line_t line;
    while (toml_next_line(data, len, &cursor, &line)) {
        if (toml_line_equals(data, &line, begin_marker) ||
            toml_line_equals(data, &line, end_marker)) {
            return 1;
        }
    }
    return 0;
}

static int toml_append_managed(toml_buffer_t *output, const char *begin_marker,
                               const char *end_marker, const char *block, const char *newline) {
    size_t block_len = strlen(block);
    if (toml_buffer_append_cstr(output, begin_marker) != TOML_EDIT_OK ||
        toml_buffer_append_cstr(output, newline) != TOML_EDIT_OK ||
        toml_append_normalized_text(output, block, block_len, newline) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    if (block_len != 0 && block[block_len - 1] != '\n' &&
        toml_buffer_append_cstr(output, newline) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    return toml_buffer_append_cstr(output, end_marker) == TOML_EDIT_OK &&
                   toml_buffer_append_cstr(output, newline) == TOML_EDIT_OK
               ? TOML_EDIT_OK
               : TOML_EDIT_ERR;
}

int cbm_toml_escape_basic_string(const char *input, char *out, size_t out_size) {
    if (!input || !out || out_size == 0) {
        return TOML_EDIT_ERR;
    }
    out[0] = '\0';
    size_t input_len = 0U;
    if (toml_bounded_length(input, TOML_EDIT_MAX_BYTES, &input_len) != TOML_EDIT_OK ||
        !toml_utf8_is_valid(input, input_len)) {
        return TOML_EDIT_ERR;
    }
    static const char hex[] = "0123456789ABCDEF";
    size_t used = 0;
    for (const unsigned char *p = (const unsigned char *)input; *p; ++p) {
        const char *escape = NULL;
        char unicode_escape[7];
        switch (*p) {
        case '\b':
            escape = "\\b";
            break;
        case '\t':
            escape = "\\t";
            break;
        case '\n':
            escape = "\\n";
            break;
        case '\f':
            escape = "\\f";
            break;
        case '\r':
            escape = "\\r";
            break;
        case '"':
            escape = "\\\"";
            break;
        case '\\':
            escape = "\\\\";
            break;
        default:
            if (*p < 0x20 || *p == 0x7f) {
                unicode_escape[0] = '\\';
                unicode_escape[1] = 'u';
                unicode_escape[2] = '0';
                unicode_escape[3] = '0';
                unicode_escape[4] = hex[*p >> 4];
                unicode_escape[5] = hex[*p & 0x0f];
                unicode_escape[6] = '\0';
                escape = unicode_escape;
            }
            break;
        }
        size_t add = escape ? strlen(escape) : 1;
        if (add > out_size - used - 1) {
            out[0] = '\0';
            return TOML_EDIT_ERR;
        }
        if (escape) {
            memcpy(out + used, escape, add);
        } else {
            out[used] = (char)*p;
        }
        used += add;
    }
    out[used] = '\0';
    return TOML_EDIT_OK;
}

int cbm_toml_upsert_managed_block(const char *file_path, const char *begin_marker,
                                  const char *end_marker, const char *block) {
    size_t block_len = 0U;
    if (!toml_valid_path(file_path) || !toml_valid_marker(begin_marker) ||
        !toml_valid_marker(end_marker) || strcmp(begin_marker, end_marker) == 0 || !block ||
        toml_bounded_length(block, TOML_EDIT_MAX_BYTES, &block_len) != TOML_EDIT_OK ||
        !toml_text_is_safe(block, block_len, 1) ||
        toml_contains_marker_line(block, block_len, begin_marker, end_marker) ||
        toml_validate_lexical_strings(block, block_len) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }

    char *existing = NULL;
    size_t existing_len = 0;
    toml_file_snapshot_t snapshot;
    if (toml_read_file(file_path, &existing, &existing_len, &snapshot) != TOML_EDIT_OK ||
        !toml_text_is_safe(existing, existing_len, 1)) {
        free(existing);
        return TOML_EDIT_ERR;
    }

    toml_line_t begin_line = {0};
    toml_line_t end_line = {0};
    int has_pair = 0;
    if (toml_find_markers(existing, existing_len, begin_marker, end_marker, &begin_line, &end_line,
                          &has_pair) != TOML_EDIT_OK) {
        free(existing);
        return TOML_EDIT_ERR;
    }
    size_t exclude_start = has_pair ? begin_line.start : SIZE_MAX;
    size_t exclude_end = has_pair ? end_line.full_end : SIZE_MAX;
    if (toml_managed_block_conflicts(existing, existing_len, exclude_start, exclude_end, block,
                                     block_len) != TOML_EDIT_OK) {
        free(existing);
        return TOML_EDIT_ERR;
    }

    toml_buffer_t output = {0};
    size_t prefix_len = has_pair ? begin_line.start : existing_len;
    if (has_pair && prefix_len == 0U && existing_len >= 3U && (unsigned char)existing[0] == 0xefU &&
        (unsigned char)existing[1] == 0xbbU && (unsigned char)existing[2] == 0xbfU) {
        prefix_len = 3U;
    }
    const char *newline = toml_newline_style(existing, existing_len);
    size_t payload_start = existing_len >= 3U && (unsigned char)existing[0] == 0xefU &&
                                   (unsigned char)existing[1] == 0xbbU &&
                                   (unsigned char)existing[2] == 0xbfU
                               ? 3U
                               : 0U;
    if (toml_buffer_append(&output, existing, prefix_len) != TOML_EDIT_OK ||
        (!has_pair && existing_len > payload_start && existing[existing_len - 1] != '\n' &&
         toml_buffer_append_cstr(&output, newline) != TOML_EDIT_OK) ||
        toml_append_managed(&output, begin_marker, end_marker, block, newline) != TOML_EDIT_OK ||
        (has_pair && toml_buffer_append(&output, existing + end_line.full_end,
                                        existing_len - end_line.full_end) != TOML_EDIT_OK)) {
        toml_buffer_dispose(&output);
        free(existing);
        return TOML_EDIT_ERR;
    }

    int result =
        toml_write_atomic(file_path, existing, existing_len, output.data, output.len, &snapshot);
    toml_buffer_dispose(&output);
    free(existing);
    return result;
}

int cbm_toml_remove_managed_block(const char *file_path, const char *begin_marker,
                                  const char *end_marker) {
    if (!toml_valid_path(file_path) || !toml_valid_marker(begin_marker) ||
        !toml_valid_marker(end_marker) || strcmp(begin_marker, end_marker) == 0) {
        return TOML_EDIT_ERR;
    }
    char *existing = NULL;
    size_t existing_len = 0;
    toml_file_snapshot_t snapshot;
    if (toml_read_file(file_path, &existing, &existing_len, &snapshot) != TOML_EDIT_OK ||
        !toml_text_is_safe(existing, existing_len, 1)) {
        free(existing);
        return TOML_EDIT_ERR;
    }

    toml_line_t begin_line = {0};
    toml_line_t end_line = {0};
    int has_pair = 0;
    if (toml_find_markers(existing, existing_len, begin_marker, end_marker, &begin_line, &end_line,
                          &has_pair) != TOML_EDIT_OK) {
        free(existing);
        return TOML_EDIT_ERR;
    }
    if (!has_pair) {
        free(existing);
        return TOML_EDIT_OK;
    }

    toml_buffer_t output = {0};
    size_t prefix_len = begin_line.start;
    if (prefix_len == 0U && existing_len >= 3U && (unsigned char)existing[0] == 0xefU &&
        (unsigned char)existing[1] == 0xbbU && (unsigned char)existing[2] == 0xbfU) {
        prefix_len = 3U;
    }
    if (toml_buffer_append(&output, existing, prefix_len) != TOML_EDIT_OK ||
        toml_buffer_append(&output, existing + end_line.full_end,
                           existing_len - end_line.full_end) != TOML_EDIT_OK) {
        toml_buffer_dispose(&output);
        free(existing);
        return TOML_EDIT_ERR;
    }
    int result =
        toml_write_atomic(file_path, existing, existing_len, output.data, output.len, &snapshot);
    toml_buffer_dispose(&output);
    free(existing);
    return result;
}

static int toml_hex_digit(unsigned char ch) {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }
    if (ch >= 'A' && ch <= 'F') {
        return ch - 'A' + 10;
    }
    return -1;
}

static int toml_append_codepoint(toml_buffer_t *buffer, uint32_t codepoint) {
    char encoded[4];
    size_t len = 0;
    if (codepoint == 0 || codepoint > 0x10ffff || (codepoint >= 0xd800 && codepoint <= 0xdfff)) {
        return TOML_EDIT_ERR;
    }
    if (codepoint <= 0x7f) {
        encoded[0] = (char)codepoint;
        len = 1;
    } else if (codepoint <= 0x7ff) {
        encoded[0] = (char)(0xc0 | (codepoint >> 6));
        encoded[1] = (char)(0x80 | (codepoint & 0x3f));
        len = 2;
    } else if (codepoint <= 0xffff) {
        encoded[0] = (char)(0xe0 | (codepoint >> 12));
        encoded[1] = (char)(0x80 | ((codepoint >> 6) & 0x3f));
        encoded[2] = (char)(0x80 | (codepoint & 0x3f));
        len = 3;
    } else {
        encoded[0] = (char)(0xf0 | (codepoint >> 18));
        encoded[1] = (char)(0x80 | ((codepoint >> 12) & 0x3f));
        encoded[2] = (char)(0x80 | ((codepoint >> 6) & 0x3f));
        encoded[3] = (char)(0x80 | (codepoint & 0x3f));
        len = 4;
    }
    return toml_buffer_append(buffer, encoded, len);
}

static int toml_parse_unicode_escape(const char *text, size_t len, size_t digits,
                                     uint32_t *out_codepoint) {
    if (len < digits) {
        return TOML_EDIT_ERR;
    }
    uint32_t value = 0;
    for (size_t i = 0; i < digits; ++i) {
        int digit = toml_hex_digit((unsigned char)text[i]);
        if (digit < 0) {
            return TOML_EDIT_ERR;
        }
        value = (value << 4) | (uint32_t)digit;
    }
    *out_codepoint = value;
    return TOML_EDIT_OK;
}

static int toml_parse_string(const char *text, size_t len, toml_string_t *parsed) {
    if (!text || !parsed || len < 2 || (text[0] != '"' && text[0] != '\'')) {
        return TOML_EDIT_ERR;
    }
    char quote = text[0];
    if (len >= 3 && text[1] == quote && text[2] == quote) {
        return TOML_EDIT_ERR;
    }
    toml_buffer_t value = {0};
    size_t pos = 1;
    while (pos < len) {
        unsigned char ch = (unsigned char)text[pos++];
        if (ch == (unsigned char)quote) {
            if (!value.data && toml_buffer_reserve(&value, 0) != TOML_EDIT_OK) {
                return TOML_EDIT_ERR;
            }
            parsed->data = value.data;
            parsed->len = value.len;
            parsed->consumed = pos;
            return TOML_EDIT_OK;
        }
        if (ch < 0x20 && ch != '\t') {
            toml_buffer_dispose(&value);
            return TOML_EDIT_ERR;
        }
        if (ch == 0x7f) {
            toml_buffer_dispose(&value);
            return TOML_EDIT_ERR;
        }
        if (quote == '\'' || ch != '\\') {
            if (toml_buffer_append_char(&value, (char)ch) != TOML_EDIT_OK) {
                toml_buffer_dispose(&value);
                return TOML_EDIT_ERR;
            }
            continue;
        }
        if (pos >= len) {
            toml_buffer_dispose(&value);
            return TOML_EDIT_ERR;
        }
        unsigned char escaped = (unsigned char)text[pos++];
        char decoded;
        switch (escaped) {
        case 'b':
            decoded = '\b';
            break;
        case 't':
            decoded = '\t';
            break;
        case 'n':
            decoded = '\n';
            break;
        case 'f':
            decoded = '\f';
            break;
        case 'r':
            decoded = '\r';
            break;
        case '"':
            decoded = '"';
            break;
        case '\\':
            decoded = '\\';
            break;
        case 'u':
        case 'U': {
            size_t digits = escaped == 'u' ? 4 : 8;
            uint32_t codepoint = 0;
            if (toml_parse_unicode_escape(text + pos, len - pos, digits, &codepoint) !=
                    TOML_EDIT_OK ||
                toml_append_codepoint(&value, codepoint) != TOML_EDIT_OK) {
                toml_buffer_dispose(&value);
                return TOML_EDIT_ERR;
            }
            pos += digits;
            continue;
        }
        default:
            toml_buffer_dispose(&value);
            return TOML_EDIT_ERR;
        }
        if (toml_buffer_append_char(&value, decoded) != TOML_EDIT_OK) {
            toml_buffer_dispose(&value);
            return TOML_EDIT_ERR;
        }
    }
    toml_buffer_dispose(&value);
    return TOML_EDIT_ERR;
}

static void toml_string_dispose(toml_string_t *value) {
    if (!value) {
        return;
    }
    free(value->data);
    value->data = NULL;
    value->len = 0;
    value->consumed = 0;
}

static void toml_key_path_dispose(toml_key_path_t *path) {
    if (!path) {
        return;
    }
    free(path->data);
    memset(path, 0, sizeof(*path));
}

static int toml_parse_key_path(const char *data, size_t start, size_t end, toml_key_path_t *path) {
    memset(path, 0, sizeof(*path));
    toml_buffer_t encoded = {0};
    size_t pos = start;
    while (pos < end) {
        while (pos < end && (data[pos] == ' ' || data[pos] == '\t')) {
            pos++;
        }
        if (pos >= end || path->count >= 64U) {
            toml_buffer_dispose(&encoded);
            return TOML_EDIT_ERR;
        }
        if (data[pos] == '"' || data[pos] == '\'') {
            toml_string_t segment = {0};
            if (toml_parse_string(data + pos, end - pos, &segment) != TOML_EDIT_OK ||
                toml_buffer_append(&encoded, segment.data, segment.len) != TOML_EDIT_OK ||
                toml_buffer_append_char(&encoded, '\0') != TOML_EDIT_OK) {
                toml_string_dispose(&segment);
                toml_buffer_dispose(&encoded);
                return TOML_EDIT_ERR;
            }
            pos += segment.consumed;
            toml_string_dispose(&segment);
        } else {
            size_t segment_start = pos;
            while (pos < end && toml_is_bare_key_char((unsigned char)data[pos])) {
                pos++;
            }
            if (pos == segment_start ||
                toml_buffer_append(&encoded, data + segment_start, pos - segment_start) !=
                    TOML_EDIT_OK ||
                toml_buffer_append_char(&encoded, '\0') != TOML_EDIT_OK) {
                toml_buffer_dispose(&encoded);
                return TOML_EDIT_ERR;
            }
        }
        path->count++;
        while (pos < end && (data[pos] == ' ' || data[pos] == '\t')) {
            pos++;
        }
        if (pos == end) {
            break;
        }
        if (data[pos] != '.') {
            toml_buffer_dispose(&encoded);
            memset(path, 0, sizeof(*path));
            return TOML_EDIT_ERR;
        }
        pos++;
    }
    if (path->count == 0U) {
        toml_buffer_dispose(&encoded);
        return TOML_EDIT_ERR;
    }
    path->data = encoded.data;
    path->len = encoded.len;
    return TOML_EDIT_OK;
}

static const char *toml_key_path_segment(const toml_key_path_t *path, size_t index) {
    const char *segment = path->data;
    for (size_t i = 0U; i < index; ++i) {
        segment += strlen(segment) + 1U;
    }
    return segment;
}

static int toml_key_path_equal(const toml_key_path_t *left, const toml_key_path_t *right) {
    return left->count == right->count && left->len == right->len &&
           (left->len == 0U || memcmp(left->data, right->data, left->len) == 0);
}

static int toml_key_path_has_prefix(const toml_key_path_t *path, const toml_key_path_t *prefix) {
    if (path->count < prefix->count) {
        return 0;
    }
    for (size_t i = 0U; i < prefix->count; ++i) {
        if (strcmp(toml_key_path_segment(path, i), toml_key_path_segment(prefix, i)) != 0) {
            return 0;
        }
    }
    return 1;
}

static int toml_key_path_is_single(const toml_key_path_t *path, const char *name) {
    return path->count == 1U && strcmp(toml_key_path_segment(path, 0U), name) == 0;
}

static int toml_key_path_join(const toml_key_path_t *prefix, const toml_key_path_t *suffix,
                              toml_key_path_t *joined) {
    memset(joined, 0, sizeof(*joined));
    if (prefix->len > SIZE_MAX - suffix->len) {
        return TOML_EDIT_ERR;
    }
    joined->len = prefix->len + suffix->len;
    joined->data = (char *)malloc(joined->len + 1U);
    if (!joined->data) {
        return TOML_EDIT_ERR;
    }
    if (prefix->len != 0U) {
        memcpy(joined->data, prefix->data, prefix->len);
    }
    if (suffix->len != 0U) {
        memcpy(joined->data + prefix->len, suffix->data, suffix->len);
    }
    joined->data[joined->len] = '\0';
    joined->count = prefix->count + suffix->count;
    return TOML_EDIT_OK;
}

static void toml_trim(const char *data, size_t *start, size_t *end) {
    while (*start < *end && (data[*start] == ' ' || data[*start] == '\t')) {
        ++*start;
    }
    while (*end > *start && (data[*end - 1] == ' ' || data[*end - 1] == '\t')) {
        --*end;
    }
}

static int toml_parse_header(const char *data, const toml_line_t *line, const char *target_name,
                             toml_header_t *header) {
    memset(header, 0, sizeof(*header));
    size_t start = line->start;
    size_t end = line->content_end;
    header->edit_start = line->start;
    if (line->start == 0 && end - start >= 3 && (unsigned char)data[start] == 0xef &&
        (unsigned char)data[start + 1] == 0xbb && (unsigned char)data[start + 2] == 0xbf) {
        start += 3;
        header->edit_start = start;
    }
    while (start < end && (data[start] == ' ' || data[start] == '\t')) {
        ++start;
    }
    if (start >= end || data[start] != '[') {
        return TOML_EDIT_OK;
    }

    int array = start + 1 < end && data[start + 1] == '[';
    size_t inner_start = start + (array ? 2u : 1u);
    size_t pos = inner_start;
    char quote = '\0';
    int escaped = 0;
    size_t close_start = SIZE_MAX;
    size_t close_end = SIZE_MAX;
    while (pos < end) {
        char ch = data[pos];
        if (quote) {
            if (quote == '"' && !escaped && ch == '\\') {
                escaped = 1;
            } else {
                if (!escaped && ch == quote) {
                    quote = '\0';
                }
                escaped = 0;
            }
            ++pos;
            continue;
        }
        if (ch == '"' || ch == '\'') {
            quote = ch;
            ++pos;
            continue;
        }
        if (ch == ']' && (!array || (pos + 1 < end && data[pos + 1] == ']'))) {
            close_start = pos;
            close_end = pos + (array ? 2u : 1u);
            break;
        }
        ++pos;
    }
    if (quote || close_start == SIZE_MAX) {
        return TOML_EDIT_ERR;
    }
    size_t inner_end = close_start;
    toml_trim(data, &inner_start, &inner_end);
    if (inner_start == inner_end) {
        return TOML_EDIT_ERR;
    }
    pos = close_end;
    while (pos < end && (data[pos] == ' ' || data[pos] == '\t')) {
        ++pos;
    }
    if (pos < end && data[pos] != '#') {
        return TOML_EDIT_ERR;
    }

    header->present = 1;
    header->array = array;
    if (toml_parse_key_path(data, inner_start, inner_end, &header->path) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    header->target = array && toml_key_path_is_single(&header->path, target_name);
    return TOML_EDIT_OK;
}

static void toml_header_dispose(toml_header_t *header) {
    if (header) {
        toml_key_path_dispose(&header->path);
    }
}

static int toml_line_is_blank_or_comment(const char *data, const toml_line_t *line) {
    size_t pos = line->start;
    while (pos < line->content_end && (data[pos] == ' ' || data[pos] == '\t')) {
        ++pos;
    }
    return pos == line->content_end || data[pos] == '#';
}

static int toml_parse_assignment(const char *data, const toml_line_t *line,
                                 toml_assignment_t *assignment) {
    memset(assignment, 0, sizeof(*assignment));
    size_t start = line->start;
    size_t end = line->content_end;
    if (start == 0U && end >= 3U && (unsigned char)data[0] == 0xefU &&
        (unsigned char)data[1] == 0xbbU && (unsigned char)data[2] == 0xbfU) {
        start = 3U;
    }
    while (start < end && (data[start] == ' ' || data[start] == '\t')) {
        start++;
    }
    if (start == end || data[start] == '#') {
        return TOML_EDIT_OK;
    }

    size_t equals = SIZE_MAX;
    char quote = '\0';
    int escaped = 0;
    for (size_t pos = start; pos < end; ++pos) {
        char ch = data[pos];
        if (quote) {
            if (quote == '"' && !escaped && ch == '\\') {
                escaped = 1;
            } else {
                if (!escaped && ch == quote) {
                    quote = '\0';
                }
                escaped = 0;
            }
            continue;
        }
        if (ch == '"' || ch == '\'') {
            quote = ch;
        } else if (ch == '=') {
            equals = pos;
            break;
        } else if (ch == '#') {
            break;
        }
    }
    if (quote || equals == SIZE_MAX) {
        return TOML_EDIT_OK;
    }
    size_t key_end = equals;
    toml_trim(data, &start, &key_end);
    if (toml_parse_key_path(data, start, key_end, &assignment->key) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }

    size_t value_start = equals + 1U;
    while (value_start < end && (data[value_start] == ' ' || data[value_start] == '\t')) {
        value_start++;
    }
    size_t value_end = end;
    quote = '\0';
    escaped = 0;
    for (size_t pos = value_start; pos < end; ++pos) {
        char ch = data[pos];
        if (quote) {
            if (quote == '"' && !escaped && ch == '\\') {
                escaped = 1;
            } else {
                if (!escaped && ch == quote) {
                    quote = '\0';
                }
                escaped = 0;
            }
            continue;
        }
        if ((ch == '"' || ch == '\'') && pos + 2U < end && data[pos + 1U] == ch &&
            data[pos + 2U] == ch) {
            assignment->multiline_value = 1;
            break;
        }
        if (ch == '"' || ch == '\'') {
            quote = ch;
        } else if (ch == '#') {
            value_end = pos;
            break;
        }
    }
    toml_trim(data, &value_start, &value_end);
    if (value_start == value_end) {
        toml_key_path_dispose(&assignment->key);
        return TOML_EDIT_ERR;
    }
    assignment->present = 1;
    assignment->value_start = value_start;
    assignment->value_end = value_end;
    return TOML_EDIT_OK;
}

static void toml_assignment_dispose(toml_assignment_t *assignment) {
    if (assignment) {
        toml_key_path_dispose(&assignment->key);
    }
}

static int toml_assignment_string_equals(const char *data, const toml_assignment_t *assignment,
                                         const char *expected, int *matches) {
    *matches = 0;
    if (!assignment->present || assignment->multiline_value) {
        return TOML_EDIT_ERR;
    }
    toml_string_t value = {0};
    if (toml_parse_string(data + assignment->value_start,
                          assignment->value_end - assignment->value_start,
                          &value) != TOML_EDIT_OK ||
        value.consumed != assignment->value_end - assignment->value_start) {
        toml_string_dispose(&value);
        return TOML_EDIT_ERR;
    }
    size_t expected_len = strlen(expected);
    *matches = value.len == expected_len && memcmp(value.data, expected, expected_len) == 0;
    toml_string_dispose(&value);
    return TOML_EDIT_OK;
}

static int toml_block_has_prior_table(const char *block, size_t block_len, size_t stop,
                                      const toml_key_path_t *desired) {
    size_t cursor = 0U;
    toml_line_t line;
    int multiline_state = TOML_STRING_NONE;
    toml_key_path_t scope = {0};
    while (toml_next_line(block, block_len, &cursor, &line) && line.start < stop) {
        int line_in_multiline = multiline_state != TOML_STRING_NONE;
        if (!line_in_multiline) {
            toml_header_t header;
            if (toml_parse_header(block, &line, "", &header) != TOML_EDIT_OK) {
                toml_key_path_dispose(&scope);
                return TOML_EDIT_ERR;
            }
            int duplicate =
                header.present && !header.array && toml_key_path_has_prefix(&header.path, desired);
            if (header.present) {
                toml_key_path_dispose(&scope);
                scope = header.path;
                memset(&header.path, 0, sizeof(header.path));
            } else {
                toml_assignment_t assignment;
                if (toml_parse_assignment(block, &line, &assignment) != TOML_EDIT_OK) {
                    toml_header_dispose(&header);
                    toml_key_path_dispose(&scope);
                    return TOML_EDIT_ERR;
                }
                if (assignment.present) {
                    toml_key_path_t full_key;
                    if (toml_key_path_join(&scope, &assignment.key, &full_key) != TOML_EDIT_OK) {
                        toml_assignment_dispose(&assignment);
                        toml_header_dispose(&header);
                        toml_key_path_dispose(&scope);
                        return TOML_EDIT_ERR;
                    }
                    duplicate = toml_key_path_has_prefix(&full_key, desired) ||
                                toml_key_path_has_prefix(desired, &full_key);
                    toml_key_path_dispose(&full_key);
                }
                toml_assignment_dispose(&assignment);
            }
            toml_header_dispose(&header);
            if (duplicate) {
                toml_key_path_dispose(&scope);
                return TOML_EDIT_ERR;
            }
        }
        if (toml_scan_line_strings(block, &line, &multiline_state) != TOML_EDIT_OK) {
            toml_key_path_dispose(&scope);
            return TOML_EDIT_ERR;
        }
    }
    toml_key_path_dispose(&scope);
    return TOML_EDIT_OK;
}

static int toml_existing_conflicts_with_table(const char *existing, size_t existing_len,
                                              size_t exclude_start, size_t exclude_end,
                                              const toml_key_path_t *desired) {
    size_t cursor = 0U;
    toml_line_t line;
    int multiline_state = TOML_STRING_NONE;
    toml_key_path_t scope = {0};
    while (toml_next_line(existing, existing_len, &cursor, &line)) {
        int line_in_multiline = multiline_state != TOML_STRING_NONE;
        int excluded =
            exclude_start != SIZE_MAX && line.start >= exclude_start && line.start < exclude_end;
        if (!line_in_multiline) {
            toml_header_t header;
            if (toml_parse_header(existing, &line, "", &header) != TOML_EDIT_OK) {
                toml_key_path_dispose(&scope);
                return TOML_EDIT_ERR;
            }
            if (header.present) {
                int conflict = !excluded && toml_key_path_has_prefix(&header.path, desired);
                toml_key_path_dispose(&scope);
                scope = header.path;
                memset(&header.path, 0, sizeof(header.path));
                toml_header_dispose(&header);
                if (conflict) {
                    toml_key_path_dispose(&scope);
                    return TOML_EDIT_ERR;
                }
            } else if (!excluded) {
                toml_assignment_t assignment;
                if (toml_parse_assignment(existing, &line, &assignment) != TOML_EDIT_OK) {
                    toml_header_dispose(&header);
                    toml_key_path_dispose(&scope);
                    return TOML_EDIT_ERR;
                }
                if (assignment.present) {
                    toml_key_path_t full_key;
                    if (toml_key_path_join(&scope, &assignment.key, &full_key) != TOML_EDIT_OK) {
                        toml_assignment_dispose(&assignment);
                        toml_header_dispose(&header);
                        toml_key_path_dispose(&scope);
                        return TOML_EDIT_ERR;
                    }
                    int conflict = toml_key_path_has_prefix(&full_key, desired) ||
                                   toml_key_path_has_prefix(desired, &full_key);
                    toml_key_path_dispose(&full_key);
                    toml_assignment_dispose(&assignment);
                    if (conflict) {
                        toml_header_dispose(&header);
                        toml_key_path_dispose(&scope);
                        return TOML_EDIT_ERR;
                    }
                }
            }
            toml_header_dispose(&header);
        }
        if (toml_scan_line_strings(existing, &line, &multiline_state) != TOML_EDIT_OK) {
            toml_key_path_dispose(&scope);
            return TOML_EDIT_ERR;
        }
    }
    toml_key_path_dispose(&scope);
    return multiline_state == TOML_STRING_NONE ? TOML_EDIT_OK : TOML_EDIT_ERR;
}

static int toml_managed_block_conflicts(const char *existing, size_t existing_len,
                                        size_t exclude_start, size_t exclude_end, const char *block,
                                        size_t block_len) {
    size_t cursor = 0U;
    toml_line_t line;
    int multiline_state = TOML_STRING_NONE;
    while (toml_next_line(block, block_len, &cursor, &line)) {
        int line_in_multiline = multiline_state != TOML_STRING_NONE;
        if (!line_in_multiline) {
            toml_header_t header;
            if (toml_parse_header(block, &line, "", &header) != TOML_EDIT_OK) {
                return TOML_EDIT_ERR;
            }
            if (header.present && !header.array &&
                (toml_block_has_prior_table(block, block_len, line.start, &header.path) !=
                     TOML_EDIT_OK ||
                 toml_existing_conflicts_with_table(existing, existing_len, exclude_start,
                                                    exclude_end, &header.path) != TOML_EDIT_OK)) {
                toml_header_dispose(&header);
                return TOML_EDIT_ERR;
            }
            toml_header_dispose(&header);
        }
        if (toml_scan_line_strings(block, &line, &multiline_state) != TOML_EDIT_OK) {
            return TOML_EDIT_ERR;
        }
    }
    return multiline_state == TOML_STRING_NONE ? TOML_EDIT_OK : TOML_EDIT_ERR;
}

static int toml_finish_target_table(toml_target_table_t *current, toml_table_scan_t *result) {
    if (!current->active) {
        return TOML_EDIT_OK;
    }
    if (current->identity_count != 1) {
        return TOML_EDIT_ERR;
    }
    if (current->identity_matches) {
        ++result->matching_count;
        if (result->matching_count > 1) {
            return TOML_EDIT_ERR;
        }
        result->start = current->start;
        result->header_end = current->header_end;
        result->direct_end = current->direct_end;
        result->edit_end = current->last_significant_end;
    }
    memset(current, 0, sizeof(*current));
    return TOML_EDIT_OK;
}

static int toml_scan_named_tables(const char *data, size_t len, const char *table_name,
                                  const char *identity_key, const char *identity_value,
                                  toml_table_scan_t *result) {
    memset(result, 0, sizeof(*result));
    toml_key_path_t root_path;
    if (toml_parse_key_path(table_name, 0U, strlen(table_name), &root_path) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    toml_target_table_t current = {0};
    size_t cursor = 0;
    toml_line_t line;
    int multiline_state = TOML_STRING_NONE;
    int at_root = 1;
    while (toml_next_line(data, len, &cursor, &line)) {
        int line_in_multiline = multiline_state != TOML_STRING_NONE;
        int handled_header = 0;
        if (!line_in_multiline) {
            toml_header_t header;
            if (toml_parse_header(data, &line, table_name, &header) != TOML_EDIT_OK) {
                toml_key_path_dispose(&root_path);
                return TOML_EDIT_ERR;
            }
            if (header.present) {
                handled_header = 1;
                at_root = 0;
                int exact_root = toml_key_path_equal(&header.path, &root_path);
                int descendant = header.path.count > root_path.count &&
                                 toml_key_path_has_prefix(&header.path, &root_path);
                if (exact_root) {
                    if (!header.array) {
                        toml_header_dispose(&header);
                        toml_key_path_dispose(&root_path);
                        return TOML_EDIT_ERR;
                    }
                    if (current.active && current.direct_end == 0U) {
                        current.direct_end = current.direct_significant_end;
                    }
                    if (toml_finish_target_table(&current, result) != TOML_EDIT_OK) {
                        toml_header_dispose(&header);
                        toml_key_path_dispose(&root_path);
                        return TOML_EDIT_ERR;
                    }
                    current.active = 1;
                    current.start = header.edit_start;
                    current.header_end = line.full_end;
                    current.direct_significant_end = line.full_end;
                    current.last_significant_end = line.full_end;
                } else if (descendant) {
                    if (!current.active) {
                        toml_header_dispose(&header);
                        toml_key_path_dispose(&root_path);
                        return TOML_EDIT_ERR;
                    }
                    if (!current.descendants) {
                        current.direct_end = current.direct_significant_end;
                        current.descendants = 1;
                    }
                    current.last_significant_end = line.full_end;
                } else {
                    if (current.active && current.direct_end == 0U) {
                        current.direct_end = current.direct_significant_end;
                    }
                    if (toml_finish_target_table(&current, result) != TOML_EDIT_OK) {
                        toml_header_dispose(&header);
                        toml_key_path_dispose(&root_path);
                        return TOML_EDIT_ERR;
                    }
                }
            }
            toml_header_dispose(&header);
        }

        if (!handled_header && current.active) {
            int significant = !toml_line_is_blank_or_comment(data, &line);
            if (significant) {
                current.last_significant_end = line.full_end;
                if (!current.descendants) {
                    current.direct_significant_end = line.full_end;
                }
            }
            if (!line_in_multiline) {
                toml_assignment_t assignment;
                if (toml_parse_assignment(data, &line, &assignment) != TOML_EDIT_OK) {
                    toml_key_path_dispose(&root_path);
                    return TOML_EDIT_ERR;
                }
                if (significant && !assignment.present) {
                    toml_assignment_dispose(&assignment);
                    toml_key_path_dispose(&root_path);
                    return TOML_EDIT_ERR;
                }
                if (!current.descendants && assignment.present &&
                    toml_key_path_is_single(&assignment.key, identity_key)) {
                    int identity_matches = 0;
                    if (toml_assignment_string_equals(data, &assignment, identity_value,
                                                      &identity_matches) != TOML_EDIT_OK) {
                        toml_assignment_dispose(&assignment);
                        toml_key_path_dispose(&root_path);
                        return TOML_EDIT_ERR;
                    }
                    current.identity_count++;
                    if (current.identity_count > 1) {
                        toml_assignment_dispose(&assignment);
                        toml_key_path_dispose(&root_path);
                        return TOML_EDIT_ERR;
                    }
                    current.identity_matches = identity_matches;
                }
                toml_assignment_dispose(&assignment);
            }
        } else if (!handled_header && !current.active && at_root && !line_in_multiline) {
            toml_assignment_t assignment;
            if (toml_parse_assignment(data, &line, &assignment) != TOML_EDIT_OK) {
                toml_key_path_dispose(&root_path);
                return TOML_EDIT_ERR;
            }
            if (assignment.present && (toml_key_path_has_prefix(&assignment.key, &root_path) ||
                                       toml_key_path_has_prefix(&root_path, &assignment.key))) {
                toml_assignment_dispose(&assignment);
                toml_key_path_dispose(&root_path);
                return TOML_EDIT_ERR;
            }
            toml_assignment_dispose(&assignment);
        }
        if (toml_scan_line_strings(data, &line, &multiline_state) != TOML_EDIT_OK) {
            toml_key_path_dispose(&root_path);
            return TOML_EDIT_ERR;
        }
    }
    if (multiline_state != TOML_STRING_NONE) {
        toml_key_path_dispose(&root_path);
        return TOML_EDIT_ERR;
    }
    if (current.active && current.direct_end == 0U) {
        current.direct_end = current.direct_significant_end;
    }
    int finish = toml_finish_target_table(&current, result);
    toml_key_path_dispose(&root_path);
    return finish;
}

static void toml_body_spec_dispose(toml_body_spec_t *spec) {
    if (!spec) {
        return;
    }
    for (size_t i = 0U; i < spec->count; ++i) {
        toml_key_path_dispose(&spec->entries[i].key);
    }
    free(spec->entries);
    memset(spec, 0, sizeof(*spec));
}

static int toml_body_spec_add(toml_body_spec_t *spec, toml_assignment_t *assignment,
                              const toml_line_t *line) {
    for (size_t i = 0U; i < spec->count; ++i) {
        if (toml_key_path_equal(&spec->entries[i].key, &assignment->key)) {
            return TOML_EDIT_ERR;
        }
    }
    if (spec->count == spec->capacity) {
        size_t capacity = spec->capacity ? spec->capacity * 2U : 8U;
        if (capacity < spec->count || capacity > SIZE_MAX / sizeof(*spec->entries)) {
            return TOML_EDIT_ERR;
        }
        toml_body_entry_t *grown =
            (toml_body_entry_t *)realloc(spec->entries, capacity * sizeof(*spec->entries));
        if (!grown) {
            return TOML_EDIT_ERR;
        }
        spec->entries = grown;
        spec->capacity = capacity;
    }
    spec->entries[spec->count].key = assignment->key;
    spec->entries[spec->count].line = *line;
    memset(&assignment->key, 0, sizeof(assignment->key));
    spec->count++;
    return TOML_EDIT_OK;
}

static int toml_validate_table_body(const char *body, size_t body_len, const char *identity_key,
                                    const char *identity_value, toml_body_spec_t *spec) {
    memset(spec, 0, sizeof(*spec));
    if (toml_validate_lexical_strings(body, body_len) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    int identity_count = 0;
    size_t cursor = 0U;
    toml_line_t line;
    while (toml_next_line(body, body_len, &cursor, &line)) {
        toml_header_t header;
        if (toml_parse_header(body, &line, "", &header) != TOML_EDIT_OK) {
            toml_body_spec_dispose(spec);
            return TOML_EDIT_ERR;
        }
        if (header.present) {
            toml_header_dispose(&header);
            toml_body_spec_dispose(spec);
            return TOML_EDIT_ERR;
        }
        toml_header_dispose(&header);
        if (toml_line_is_blank_or_comment(body, &line)) {
            continue;
        }
        toml_assignment_t assignment;
        if (toml_parse_assignment(body, &line, &assignment) != TOML_EDIT_OK ||
            !assignment.present || assignment.multiline_value || assignment.key.count != 1U) {
            toml_assignment_dispose(&assignment);
            toml_body_spec_dispose(spec);
            return TOML_EDIT_ERR;
        }
        if (toml_key_path_is_single(&assignment.key, identity_key)) {
            int matches = 0;
            if (toml_assignment_string_equals(body, &assignment, identity_value, &matches) !=
                    TOML_EDIT_OK ||
                !matches) {
                toml_assignment_dispose(&assignment);
                toml_body_spec_dispose(spec);
                return TOML_EDIT_ERR;
            }
            identity_count++;
        }
        if (toml_body_spec_add(spec, &assignment, &line) != TOML_EDIT_OK) {
            toml_assignment_dispose(&assignment);
            toml_body_spec_dispose(spec);
            return TOML_EDIT_ERR;
        }
        toml_assignment_dispose(&assignment);
    }
    if (identity_count != 1 || spec->count == 0U) {
        toml_body_spec_dispose(spec);
        return TOML_EDIT_ERR;
    }
    return TOML_EDIT_OK;
}

static size_t toml_body_spec_find(const toml_body_spec_t *spec, const toml_key_path_t *key) {
    for (size_t i = 0U; i < spec->count; ++i) {
        if (toml_key_path_equal(&spec->entries[i].key, key)) {
            return i;
        }
    }
    return SIZE_MAX;
}

static int toml_append_body_entry(toml_buffer_t *output, const char *body,
                                  const toml_body_entry_t *entry, const char *newline) {
    return toml_buffer_append(output, body + entry->line.start,
                              entry->line.content_end - entry->line.start) == TOML_EDIT_OK &&
                   toml_buffer_append_cstr(output, newline) == TOML_EDIT_OK
               ? TOML_EDIT_OK
               : TOML_EDIT_ERR;
}

static int toml_merge_named_table(const char *existing, size_t existing_len,
                                  const toml_table_scan_t *scan, const char *body,
                                  const toml_body_spec_t *spec, const char *newline,
                                  toml_buffer_t *output) {
    if (toml_buffer_append(output, existing, scan->header_end) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    unsigned char *emitted = (unsigned char *)calloc(spec->count, 1U);
    if (!emitted) {
        return TOML_EDIT_ERR;
    }
    size_t cursor = scan->header_end;
    toml_line_t line;
    int multiline_state = TOML_STRING_NONE;
    while (cursor < scan->direct_end && toml_next_line(existing, existing_len, &cursor, &line)) {
        if (line.start >= scan->direct_end) {
            break;
        }
        int replaced = 0;
        if (multiline_state == TOML_STRING_NONE) {
            toml_assignment_t assignment;
            if (toml_parse_assignment(existing, &line, &assignment) != TOML_EDIT_OK) {
                free(emitted);
                return TOML_EDIT_ERR;
            }
            if (assignment.present) {
                size_t desired = toml_body_spec_find(spec, &assignment.key);
                if (desired != SIZE_MAX) {
                    if (emitted[desired] || assignment.multiline_value ||
                        toml_append_body_entry(output, body, &spec->entries[desired], newline) !=
                            TOML_EDIT_OK) {
                        toml_assignment_dispose(&assignment);
                        free(emitted);
                        return TOML_EDIT_ERR;
                    }
                    emitted[desired] = 1U;
                    replaced = 1;
                }
            }
            toml_assignment_dispose(&assignment);
        }
        if (!replaced && toml_buffer_append(output, existing + line.start,
                                            line.full_end - line.start) != TOML_EDIT_OK) {
            free(emitted);
            return TOML_EDIT_ERR;
        }
        if (toml_scan_line_strings(existing, &line, &multiline_state) != TOML_EDIT_OK) {
            free(emitted);
            return TOML_EDIT_ERR;
        }
    }
    if (multiline_state != TOML_STRING_NONE) {
        free(emitted);
        return TOML_EDIT_ERR;
    }
    for (size_t i = 0U; i < spec->count; ++i) {
        if (emitted[i]) {
            continue;
        }
        if (output->len != 0U && output->data[output->len - 1U] != '\n' &&
            toml_buffer_append_cstr(output, newline) != TOML_EDIT_OK) {
            free(emitted);
            return TOML_EDIT_ERR;
        }
        if (toml_append_body_entry(output, body, &spec->entries[i], newline) != TOML_EDIT_OK) {
            free(emitted);
            return TOML_EDIT_ERR;
        }
    }
    free(emitted);
    return toml_buffer_append(output, existing + scan->direct_end, existing_len - scan->direct_end);
}

static int toml_append_named_table(toml_buffer_t *output, const char *table_name,
                                   const char *table_body, const char *newline) {
    size_t body_len = strlen(table_body);
    if (toml_buffer_append_cstr(output, "[[") != TOML_EDIT_OK ||
        toml_buffer_append_cstr(output, table_name) != TOML_EDIT_OK ||
        toml_buffer_append_cstr(output, "]]") != TOML_EDIT_OK ||
        toml_buffer_append_cstr(output, newline) != TOML_EDIT_OK ||
        toml_append_normalized_text(output, table_body, body_len, newline) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    return body_len == 0 || table_body[body_len - 1] == '\n' ||
                   toml_buffer_append_cstr(output, newline) == TOML_EDIT_OK
               ? TOML_EDIT_OK
               : TOML_EDIT_ERR;
}

static int toml_named_inputs_valid(const char *file_path, const char *table_name,
                                   const char *identity_key, const char *identity_value) {
    size_t identity_len = 0U;
    return toml_valid_path(file_path) && toml_valid_identifier(table_name) &&
           toml_valid_identifier(identity_key) && identity_value &&
           toml_bounded_length(identity_value, 4096U, &identity_len) == TOML_EDIT_OK &&
           toml_text_is_safe(identity_value, identity_len, 0);
}

int cbm_toml_upsert_named_array_table(const char *file_path, const char *table_name,
                                      const char *identity_key, const char *identity_value,
                                      const char *table_body) {
    size_t body_len = 0U;
    if (!toml_named_inputs_valid(file_path, table_name, identity_key, identity_value) ||
        !table_body ||
        toml_bounded_length(table_body, TOML_EDIT_MAX_BYTES, &body_len) != TOML_EDIT_OK ||
        !toml_text_is_safe(table_body, body_len, 1)) {
        return TOML_EDIT_ERR;
    }
    toml_body_spec_t spec;
    if (toml_validate_table_body(table_body, body_len, identity_key, identity_value, &spec) !=
        TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }
    char *existing = NULL;
    size_t existing_len = 0;
    toml_file_snapshot_t snapshot;
    if (toml_read_file(file_path, &existing, &existing_len, &snapshot) != TOML_EDIT_OK ||
        !toml_text_is_safe(existing, existing_len, 1)) {
        toml_body_spec_dispose(&spec);
        free(existing);
        return TOML_EDIT_ERR;
    }
    toml_table_scan_t scan;
    if (toml_scan_named_tables(existing, existing_len, table_name, identity_key, identity_value,
                               &scan) != TOML_EDIT_OK) {
        toml_body_spec_dispose(&spec);
        free(existing);
        return TOML_EDIT_ERR;
    }

    toml_buffer_t output = {0};
    const char *newline = toml_newline_style(existing, existing_len);
    int edit_result = TOML_EDIT_OK;
    if (scan.matching_count == 1) {
        edit_result = toml_merge_named_table(existing, existing_len, &scan, table_body, &spec,
                                             newline, &output);
    } else {
        size_t payload_start = existing_len >= 3U && (unsigned char)existing[0] == 0xefU &&
                                       (unsigned char)existing[1] == 0xbbU &&
                                       (unsigned char)existing[2] == 0xbfU
                                   ? 3U
                                   : 0U;
        edit_result = toml_buffer_append(&output, existing, existing_len);
        if (edit_result == TOML_EDIT_OK && existing_len > payload_start) {
            if (existing[existing_len - 1U] != '\n') {
                edit_result = toml_buffer_append_cstr(&output, newline);
            }
            if (edit_result == TOML_EDIT_OK &&
                (output.len < strlen(newline) * 2U ||
                 memcmp(output.data + output.len - strlen(newline) * 2U, newline,
                        strlen(newline)) != 0)) {
                edit_result = toml_buffer_append_cstr(&output, newline);
            }
        }
        if (edit_result == TOML_EDIT_OK) {
            edit_result = toml_append_named_table(&output, table_name, table_body, newline);
        }
    }
    if (edit_result != TOML_EDIT_OK) {
        toml_buffer_dispose(&output);
        toml_body_spec_dispose(&spec);
        free(existing);
        return TOML_EDIT_ERR;
    }
    int result =
        toml_write_atomic(file_path, existing, existing_len, output.data, output.len, &snapshot);
    toml_buffer_dispose(&output);
    toml_body_spec_dispose(&spec);
    free(existing);
    return result;
}

int cbm_toml_remove_named_array_table(const char *file_path, const char *table_name,
                                      const char *identity_key, const char *identity_value) {
    if (!toml_named_inputs_valid(file_path, table_name, identity_key, identity_value)) {
        return TOML_EDIT_ERR;
    }
    char *existing = NULL;
    size_t existing_len = 0;
    toml_file_snapshot_t snapshot;
    if (toml_read_file(file_path, &existing, &existing_len, &snapshot) != TOML_EDIT_OK ||
        !toml_text_is_safe(existing, existing_len, 1)) {
        free(existing);
        return TOML_EDIT_ERR;
    }
    toml_table_scan_t scan;
    if (toml_scan_named_tables(existing, existing_len, table_name, identity_key, identity_value,
                               &scan) != TOML_EDIT_OK) {
        free(existing);
        return TOML_EDIT_ERR;
    }
    if (scan.matching_count == 0) {
        free(existing);
        return TOML_EDIT_OK;
    }

    toml_buffer_t output = {0};
    if (toml_buffer_append(&output, existing, scan.start) != TOML_EDIT_OK ||
        toml_buffer_append(&output, existing + scan.edit_end, existing_len - scan.edit_end) !=
            TOML_EDIT_OK) {
        toml_buffer_dispose(&output);
        free(existing);
        return TOML_EDIT_ERR;
    }
    int result =
        toml_write_atomic(file_path, existing, existing_len, output.data, output.len, &snapshot);
    toml_buffer_dispose(&output);
    free(existing);
    return result;
}

static int toml_owned_table_is_canonical(const char *existing, size_t existing_len,
                                         const toml_table_scan_t *scan, const char *table_name,
                                         const char *canonical_body, const char *newline,
                                         int *is_canonical) {
    *is_canonical = 0;
    if (scan->matching_count != 1 || scan->start > scan->direct_end ||
        scan->direct_end > scan->edit_end || scan->edit_end > existing_len) {
        return TOML_EDIT_ERR;
    }
    if (scan->direct_end != scan->edit_end) {
        return TOML_EDIT_OK;
    }

    toml_buffer_t canonical = {0};
    if (toml_append_named_table(&canonical, table_name, canonical_body, newline) != TOML_EDIT_OK) {
        toml_buffer_dispose(&canonical);
        return TOML_EDIT_ERR;
    }
    size_t existing_table_len = scan->direct_end - scan->start;
    *is_canonical = existing_table_len == canonical.len &&
                    memcmp(existing + scan->start, canonical.data, canonical.len) == 0;
    toml_buffer_dispose(&canonical);
    return TOML_EDIT_OK;
}

static int toml_build_owned_table_insert(toml_buffer_t *output, const char *existing,
                                         size_t existing_len, const char *table_name,
                                         const char *canonical_body, const char *newline) {
    size_t payload_start = existing_len >= 3U && (unsigned char)existing[0] == 0xefU &&
                                   (unsigned char)existing[1] == 0xbbU &&
                                   (unsigned char)existing[2] == 0xbfU
                               ? 3U
                               : 0U;
    int result = toml_buffer_append(output, existing, existing_len);
    if (result == TOML_EDIT_OK && existing_len > payload_start &&
        existing[existing_len - 1U] != '\n') {
        result = toml_buffer_append_cstr(output, newline);
    }
    size_t newline_len = strlen(newline);
    if (result == TOML_EDIT_OK && existing_len > payload_start &&
        (output->len < newline_len * 2U ||
         memcmp(output->data + output->len - newline_len * 2U, newline, newline_len) != 0)) {
        result = toml_buffer_append_cstr(output, newline);
    }
    return result == TOML_EDIT_OK
               ? toml_append_named_table(output, table_name, canonical_body, newline)
               : TOML_EDIT_ERR;
}

static int toml_edit_owned_named_array_table(const char *file_path, const char *table_name,
                                             const char *identity_key, const char *identity_value,
                                             const char *canonical_body, int remove) {
    size_t body_len = 0U;
    if (!toml_named_inputs_valid(file_path, table_name, identity_key, identity_value) ||
        !canonical_body ||
        toml_bounded_length(canonical_body, TOML_EDIT_MAX_BYTES, &body_len) != TOML_EDIT_OK ||
        !toml_text_is_safe(canonical_body, body_len, 1)) {
        return CBM_TOML_OWNED_EDIT_ERROR;
    }
    toml_body_spec_t spec;
    if (toml_validate_table_body(canonical_body, body_len, identity_key, identity_value, &spec) !=
        TOML_EDIT_OK) {
        return CBM_TOML_OWNED_EDIT_ERROR;
    }
    toml_body_spec_dispose(&spec);

    char *existing = NULL;
    size_t existing_len = 0U;
    toml_file_snapshot_t snapshot;
    if (toml_read_file(file_path, &existing, &existing_len, &snapshot) != TOML_EDIT_OK ||
        !toml_text_is_safe(existing, existing_len, 1)) {
        free(existing);
        return CBM_TOML_OWNED_EDIT_ERROR;
    }
    toml_table_scan_t scan;
    if (toml_scan_named_tables(existing, existing_len, table_name, identity_key, identity_value,
                               &scan) != TOML_EDIT_OK) {
        free(existing);
        return CBM_TOML_OWNED_EDIT_ERROR;
    }

    const char *newline = toml_newline_style(existing, existing_len);
    if (scan.matching_count == 1) {
        int is_canonical = 0;
        if (toml_owned_table_is_canonical(existing, existing_len, &scan, table_name, canonical_body,
                                          newline, &is_canonical) != TOML_EDIT_OK) {
            free(existing);
            return CBM_TOML_OWNED_EDIT_ERROR;
        }
        if (!is_canonical) {
            free(existing);
            return CBM_TOML_OWNED_EDIT_FOREIGN;
        }
        if (!remove) {
            free(existing);
            return CBM_TOML_OWNED_EDIT_OK;
        }
    } else if (remove) {
        free(existing);
        return CBM_TOML_OWNED_EDIT_OK;
    }

    toml_buffer_t output = {0};
    int build_result =
        scan.matching_count == 1
            ? (toml_buffer_append(&output, existing, scan.start) == TOML_EDIT_OK &&
                       toml_buffer_append(&output, existing + scan.edit_end,
                                          existing_len - scan.edit_end) == TOML_EDIT_OK
                   ? TOML_EDIT_OK
                   : TOML_EDIT_ERR)
            : toml_build_owned_table_insert(&output, existing, existing_len, table_name,
                                            canonical_body, newline);
    int result = build_result == TOML_EDIT_OK
                     ? toml_write_atomic(file_path, existing, existing_len, output.data, output.len,
                                         &snapshot)
                     : TOML_EDIT_ERR;
    toml_buffer_dispose(&output);
    free(existing);
    return result == TOML_EDIT_OK ? CBM_TOML_OWNED_EDIT_OK : CBM_TOML_OWNED_EDIT_ERROR;
}

int cbm_toml_upsert_owned_named_array_table(const char *file_path, const char *table_name,
                                            const char *identity_key, const char *identity_value,
                                            const char *canonical_body) {
    return toml_edit_owned_named_array_table(file_path, table_name, identity_key, identity_value,
                                             canonical_body, 0);
}

int cbm_toml_remove_owned_named_array_table(const char *file_path, const char *table_name,
                                            const char *identity_key, const char *identity_value,
                                            const char *canonical_body) {
    return toml_edit_owned_named_array_table(file_path, table_name, identity_key, identity_value,
                                             canonical_body, 1);
}

static int toml_legacy_command_is_owned(const char *data, const toml_assignment_t *assignment,
                                        int *owned) {
    *owned = 0;
    if (!assignment->present || assignment->multiline_value) {
        return TOML_EDIT_ERR;
    }
    toml_string_t value = {0};
    size_t value_len = assignment->value_end - assignment->value_start;
    if (toml_parse_string(data + assignment->value_start, value_len, &value) != TOML_EDIT_OK ||
        value.consumed != value_len) {
        toml_string_dispose(&value);
        return TOML_EDIT_ERR;
    }
    size_t basename_start = 0U;
    for (size_t i = 0U; i < value.len; ++i) {
        if (value.data[i] == '/' || value.data[i] == '\\') {
            basename_start = i + 1U;
        }
    }
    static const char binary_name[] = "codebase-memory-mcp";
    static const char windows_binary_name[] = "codebase-memory-mcp.exe";
    size_t basename_len = value.len - basename_start;
    *owned = (basename_len == sizeof(binary_name) - 1U &&
              memcmp(value.data + basename_start, binary_name, basename_len) == 0) ||
             (basename_len == sizeof(windows_binary_name) - 1U &&
              memcmp(value.data + basename_start, windows_binary_name, basename_len) == 0);
    toml_string_dispose(&value);
    return TOML_EDIT_OK;
}

static int toml_legacy_args_are_empty(const char *data, const toml_assignment_t *assignment) {
    if (!assignment->present || assignment->multiline_value) {
        return 0;
    }
    size_t pos = assignment->value_start;
    size_t end = assignment->value_end;
    if (pos >= end || data[pos++] != '[') {
        return 0;
    }
    while (pos < end && (data[pos] == ' ' || data[pos] == '\t')) {
        pos++;
    }
    if (pos >= end || data[pos++] != ']') {
        return 0;
    }
    while (pos < end && (data[pos] == ' ' || data[pos] == '\t')) {
        pos++;
    }
    return pos == end;
}

static int toml_legacy_schema_is_owned(int command_count, int command_owned, int args_count,
                                       int args_empty) {
    return command_count == 1 && command_owned && args_count <= 1 &&
           (args_count == 0 || args_empty);
}

int cbm_toml_remove_legacy_table(const char *file_path, const char *table_name,
                                 const char *begin_marker, const char *end_marker) {
    if (!toml_valid_path(file_path) || !table_name || !toml_valid_marker(begin_marker) ||
        !toml_valid_marker(end_marker) || strcmp(begin_marker, end_marker) == 0) {
        return TOML_EDIT_ERR;
    }
    toml_key_path_t desired;
    if (toml_parse_key_path(table_name, 0U, strlen(table_name), &desired) != TOML_EDIT_OK) {
        return TOML_EDIT_ERR;
    }

    char *existing = NULL;
    size_t existing_len = 0U;
    toml_file_snapshot_t snapshot;
    if (toml_read_file(file_path, &existing, &existing_len, &snapshot) != TOML_EDIT_OK ||
        !toml_text_is_safe(existing, existing_len, 1)) {
        toml_key_path_dispose(&desired);
        free(existing);
        return TOML_EDIT_ERR;
    }

    toml_line_t begin_line = {0};
    toml_line_t end_line = {0};
    int has_managed_pair = 0;
    if (toml_find_markers(existing, existing_len, begin_marker, end_marker, &begin_line, &end_line,
                          &has_managed_pair) != TOML_EDIT_OK) {
        toml_key_path_dispose(&desired);
        free(existing);
        return TOML_EDIT_ERR;
    }
    if (has_managed_pair) {
        toml_key_path_dispose(&desired);
        free(existing);
        return TOML_EDIT_OK;
    }

    size_t cursor = 0U;
    toml_line_t line;
    int multiline_state = TOML_STRING_NONE;
    int target_active = 0;
    int target_count = 0;
    int target_foreign = 0;
    int target_array_seen = 0;
    int target_regular_seen = 0;
    int command_count = 0;
    int command_owned = 0;
    int args_count = 0;
    int args_empty = 0;
    size_t edit_start = SIZE_MAX;
    size_t edit_end = existing_len;
    while (toml_next_line(existing, existing_len, &cursor, &line)) {
        int line_in_multiline = multiline_state != TOML_STRING_NONE;
        int handled_header = 0;
        if (!line_in_multiline) {
            toml_header_t header;
            if (toml_parse_header(existing, &line, "", &header) != TOML_EDIT_OK) {
                toml_key_path_dispose(&desired);
                free(existing);
                return TOML_EDIT_ERR;
            }
            if (header.present) {
                handled_header = 1;
                int exact = toml_key_path_equal(&header.path, &desired);
                int descendant = header.path.count > desired.count &&
                                 toml_key_path_has_prefix(&header.path, &desired);
                if (exact) {
                    if (header.array) {
                        if (target_regular_seen) {
                            toml_header_dispose(&header);
                            toml_key_path_dispose(&desired);
                            free(existing);
                            return TOML_EDIT_ERR;
                        }
                        target_array_seen = 1;
                        target_foreign = 1;
                        target_count++;
                    } else if (target_array_seen || target_regular_seen) {
                        toml_header_dispose(&header);
                        toml_key_path_dispose(&desired);
                        free(existing);
                        return TOML_EDIT_ERR;
                    } else {
                        target_regular_seen = 1;
                        target_count = 1;
                    }
                    target_active = 1;
                    command_count = 0;
                    command_owned = 0;
                    args_count = 0;
                    args_empty = 0;
                    edit_start = header.edit_start;
                } else if (target_active) {
                    if (descendant || !toml_legacy_schema_is_owned(command_count, command_owned,
                                                                   args_count, args_empty)) {
                        target_foreign = 1;
                    }
                    edit_end = header.edit_start;
                    target_active = 0;
                }
            }
            toml_header_dispose(&header);
        }
        if (target_active && !handled_header && !line_in_multiline &&
            !toml_line_is_blank_or_comment(existing, &line)) {
            toml_assignment_t assignment;
            if (toml_parse_assignment(existing, &line, &assignment) != TOML_EDIT_OK ||
                !assignment.present) {
                toml_assignment_dispose(&assignment);
                toml_key_path_dispose(&desired);
                free(existing);
                return TOML_EDIT_ERR;
            }
            if (assignment.key.count != 1U) {
                target_foreign = 1;
            } else if (toml_key_path_is_single(&assignment.key, "command")) {
                if (++command_count > 1 ||
                    toml_legacy_command_is_owned(existing, &assignment, &command_owned) !=
                        TOML_EDIT_OK) {
                    toml_assignment_dispose(&assignment);
                    toml_key_path_dispose(&desired);
                    free(existing);
                    return TOML_EDIT_ERR;
                }
                if (!command_owned) {
                    target_foreign = 1;
                }
            } else if (toml_key_path_is_single(&assignment.key, "args")) {
                args_count++;
                args_empty = toml_legacy_args_are_empty(existing, &assignment);
                if (args_count > 1) {
                    toml_assignment_dispose(&assignment);
                    toml_key_path_dispose(&desired);
                    free(existing);
                    return TOML_EDIT_ERR;
                }
                if (!args_empty) {
                    target_foreign = 1;
                }
            } else {
                target_foreign = 1;
            }
            toml_assignment_dispose(&assignment);
        }
        if (toml_scan_line_strings(existing, &line, &multiline_state) != TOML_EDIT_OK) {
            toml_key_path_dispose(&desired);
            free(existing);
            return TOML_EDIT_ERR;
        }
    }
    toml_key_path_dispose(&desired);
    if (multiline_state != TOML_STRING_NONE) {
        free(existing);
        return TOML_EDIT_ERR;
    }
    if (target_active &&
        !toml_legacy_schema_is_owned(command_count, command_owned, args_count, args_empty)) {
        target_foreign = 1;
    }
    if (target_foreign) {
        free(existing);
        return TOML_EDIT_FOREIGN;
    }
    if (target_count == 0) {
        free(existing);
        return TOML_EDIT_OK;
    }

    toml_buffer_t output = {0};
    if (toml_buffer_append(&output, existing, edit_start) != TOML_EDIT_OK ||
        toml_buffer_append(&output, existing + edit_end, existing_len - edit_end) != TOML_EDIT_OK) {
        toml_buffer_dispose(&output);
        free(existing);
        return TOML_EDIT_ERR;
    }
    int result =
        toml_write_atomic(file_path, existing, existing_len, output.data, output.len, &snapshot);
    toml_buffer_dispose(&output);
    free(existing);
    return result;
}
