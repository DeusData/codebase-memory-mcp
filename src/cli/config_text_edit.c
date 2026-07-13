/*
 * config_text_edit.c — Fail-closed edits for managed instruction text.
 */
#include "cli/config_text_edit.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include "foundation/win_utf8.h"

#include <fcntl.h>
#include <io.h>
#include <process.h>
#include <sys/stat.h>
#define text_close _close
#define text_fdopen _fdopen
#define TEXT_PROCESS_ID _getpid
#define TEXT_SYNC _commit
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#define text_close close
#define text_fdopen fdopen
#define TEXT_PROCESS_ID getpid
#define TEXT_SYNC fsync
#endif

#define TEXT_OK 0
#define TEXT_ERROR (-1)
#define TEXT_UNOWNED 1
#define TEXT_MAX_BYTES (16U * 1024U * 1024U)
#define TEXT_MAX_PATH_BYTES 32768U
#define TEXT_MAX_MARKER_BYTES 4096U
#define TEXT_TEMP_SUFFIX_BYTES 80U
#define TEXT_TEMP_ATTEMPTS 64U

#ifdef CBM_TEXT_EDIT_ENABLE_TEST_API
static CBM_TLS cbm_text_precommit_test_hook_t text_precommit_test_hook = NULL;
static CBM_TLS void *text_precommit_test_context = NULL;
static CBM_TLS cbm_text_precommit_test_hook_t text_prepublish_test_hook = NULL;
static CBM_TLS void *text_prepublish_test_context = NULL;
#endif

static atomic_uint text_temp_sequence = 1U;

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
} text_file_snapshot_t;

typedef struct {
    char *data;
    size_t len;
    size_t capacity;
} text_buffer_t;

typedef struct {
    int present;
    size_t begin_start;
    size_t begin_text_end;
    size_t begin_full_end;
    size_t end_start;
    size_t end_text_end;
    size_t end_full_end;
} text_managed_region_t;

static void text_buffer_dispose(text_buffer_t *buffer) {
    if (!buffer) {
        return;
    }
    free(buffer->data);
    buffer->data = NULL;
    buffer->len = 0U;
    buffer->capacity = 0U;
}

static int text_buffer_reserve(text_buffer_t *buffer, size_t additional) {
    if (!buffer || additional > SIZE_MAX - buffer->len - 1U) {
        return TEXT_ERROR;
    }
    size_t required = buffer->len + additional + 1U;
    if (required > (size_t)TEXT_MAX_BYTES + 1U) {
        return TEXT_ERROR;
    }
    if (required <= buffer->capacity) {
        return TEXT_OK;
    }
    size_t capacity = buffer->capacity ? buffer->capacity : 256U;
    while (capacity < required) {
        if (capacity > SIZE_MAX / 2U) {
            capacity = required;
            break;
        }
        capacity *= 2U;
    }
    char *grown = (char *)realloc(buffer->data, capacity);
    if (!grown) {
        return TEXT_ERROR;
    }
    buffer->data = grown;
    buffer->capacity = capacity;
    return TEXT_OK;
}

static int text_buffer_append(text_buffer_t *buffer, const char *data, size_t len) {
    if ((!data && len != 0U) || text_buffer_reserve(buffer, len) != TEXT_OK) {
        return TEXT_ERROR;
    }
    if (len != 0U) {
        memcpy(buffer->data + buffer->len, data, len);
        buffer->len += len;
    }
    buffer->data[buffer->len] = '\0';
    return TEXT_OK;
}

static int text_bounded_strlen(const char *text, size_t maximum, size_t *len_out) {
    if (!text || !len_out) {
        return TEXT_ERROR;
    }
    size_t len = 0U;
    while (len <= maximum && text[len] != '\0') {
        len++;
    }
    if (len > maximum) {
        return TEXT_ERROR;
    }
    *len_out = len;
    return TEXT_OK;
}

static int text_decode_utf8(const unsigned char *text, size_t remaining, uint32_t *codepoint,
                            size_t *byte_count) {
    if (!text || !codepoint || !byte_count || remaining == 0U) {
        return TEXT_ERROR;
    }
    unsigned char first = text[0];
    if (first <= 0x7fU) {
        *codepoint = first;
        *byte_count = 1U;
        return TEXT_OK;
    }

    size_t count;
    uint32_t value;
    uint32_t minimum;
    if (first >= 0xc2U && first <= 0xdfU) {
        count = 2U;
        value = first & 0x1fU;
        minimum = 0x80U;
    } else if (first >= 0xe0U && first <= 0xefU) {
        count = 3U;
        value = first & 0x0fU;
        minimum = 0x800U;
    } else if (first >= 0xf0U && first <= 0xf4U) {
        count = 4U;
        value = first & 0x07U;
        minimum = 0x10000U;
    } else {
        return TEXT_ERROR;
    }
    if (count > remaining) {
        return TEXT_ERROR;
    }
    for (size_t i = 1U; i < count; i++) {
        unsigned char next = text[i];
        if ((next & 0xc0U) != 0x80U) {
            return TEXT_ERROR;
        }
        value = (value << 6U) | (uint32_t)(next & 0x3fU);
    }
    if (value < minimum || value > 0x10ffffU || (value >= 0xd800U && value <= 0xdfffU)) {
        return TEXT_ERROR;
    }
    *codepoint = value;
    *byte_count = count;
    return TEXT_OK;
}

static int text_validate_bytes(const char *data, size_t len, int allow_initial_bom) {
    if (!data && len != 0U) {
        return TEXT_ERROR;
    }
    size_t pos = 0U;
    while (pos < len) {
        uint32_t codepoint = 0U;
        size_t byte_count = 0U;
        if (text_decode_utf8((const unsigned char *)data + pos, len - pos, &codepoint,
                             &byte_count) != TEXT_OK) {
            return TEXT_ERROR;
        }
        if (codepoint == 0xfeffU) {
            if (!(allow_initial_bom && pos == 0U && byte_count == 3U)) {
                return TEXT_ERROR;
            }
        } else if (codepoint < 0x20U) {
            if (codepoint != '\t' && codepoint != '\n' && codepoint != '\r') {
                return TEXT_ERROR;
            }
            if (codepoint == '\r' && (pos + 1U >= len || data[pos + 1U] != '\n')) {
                return TEXT_ERROR;
            }
        } else if (codepoint == 0x7fU || (codepoint >= 0x80U && codepoint <= 0x9fU)) {
            return TEXT_ERROR;
        }
        pos += byte_count;
    }
    return TEXT_OK;
}

static int text_valid_path(const char *path) {
    size_t len = 0U;
    return path && path[0] != '\0' &&
           text_bounded_strlen(path, TEXT_MAX_PATH_BYTES, &len) == TEXT_OK &&
           text_validate_bytes(path, len, 0) == TEXT_OK && !strchr(path, '\n') &&
           !strchr(path, '\r');
}

static int text_valid_marker(const char *marker, size_t *len_out) {
    size_t len = 0U;
    if (!marker || marker[0] == '\0' ||
        text_bounded_strlen(marker, TEXT_MAX_MARKER_BYTES, &len) != TEXT_OK ||
        text_validate_bytes(marker, len, 0) != TEXT_OK || strchr(marker, '\n') ||
        strchr(marker, '\r')) {
        return TEXT_ERROR;
    }
    *len_out = len;
    return TEXT_OK;
}

static int text_snapshot_equal(const text_file_snapshot_t *left,
                               const text_file_snapshot_t *right) {
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
static int text_snapshot_from_handle(HANDLE handle, text_file_snapshot_t *snapshot) {
    BY_HANDLE_FILE_INFORMATION info;
    if (GetFileType(handle) != FILE_TYPE_DISK || !GetFileInformationByHandle(handle, &info) ||
        (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0 ||
        info.nNumberOfLinks != 1U || (info.nFileIndexHigh == 0U && info.nFileIndexLow == 0U)) {
        return TEXT_ERROR;
    }
    uint64_t size = ((uint64_t)info.nFileSizeHigh << 32U) | (uint64_t)info.nFileSizeLow;
    if (size > TEXT_MAX_BYTES) {
        return TEXT_ERROR;
    }
    *snapshot = (text_file_snapshot_t){
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
    return TEXT_OK;
}
#else
static int text_snapshot_from_stat(const struct stat *state, text_file_snapshot_t *snapshot) {
    if (!S_ISREG(state->st_mode) || state->st_ino == 0 || state->st_nlink != 1U ||
        state->st_size < 0 || (uint64_t)state->st_size > TEXT_MAX_BYTES ||
        (state->st_mode & (S_ISUID | S_ISGID | S_ISVTX)) != 0) {
        return TEXT_ERROR;
    }
    *snapshot = (text_file_snapshot_t){
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
    return TEXT_OK;
}
#endif

static int text_read_file(const char *path, char **data_out, size_t *len_out,
                          text_file_snapshot_t *snapshot_out) {
    if (!path || !data_out || !len_out || !snapshot_out) {
        return TEXT_ERROR;
    }
    *data_out = NULL;
    *len_out = 0U;
    memset(snapshot_out, 0, sizeof(*snapshot_out));

#ifdef _WIN32
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_path) {
        return TEXT_ERROR;
    }
    HANDLE handle = CreateFileW(
        wide_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    free(wide_path);
    if (handle == INVALID_HANDLE_VALUE) {
        DWORD error = GetLastError();
        if (error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND) {
            return TEXT_ERROR;
        }
        char *empty = (char *)calloc(1U, 1U);
        if (!empty) {
            return TEXT_ERROR;
        }
        *data_out = empty;
        return TEXT_OK;
    }
    text_file_snapshot_t before;
    if (text_snapshot_from_handle(handle, &before) != TEXT_OK) {
        CloseHandle(handle);
        return TEXT_ERROR;
    }
    size_t len = (size_t)before.size;
    char *data = (char *)malloc(len + 1U);
    if (!data) {
        CloseHandle(handle);
        return TEXT_ERROR;
    }
    DWORD read_count = 0U;
    BOOL read_ok = ReadFile(handle, data, (DWORD)len, &read_count, NULL);
    text_file_snapshot_t after;
    int after_result = text_snapshot_from_handle(handle, &after);
    BOOL close_ok = CloseHandle(handle);
    if (!read_ok || read_count != (DWORD)len || after_result != TEXT_OK || !close_ok ||
        !text_snapshot_equal(&before, &after)) {
        free(data);
        return TEXT_ERROR;
    }
#else
#ifndef O_NOFOLLOW
    (void)path;
    return TEXT_ERROR;
#else
    int flags = O_RDONLY | O_NOFOLLOW | O_NONBLOCK;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int descriptor = open(path, flags);
    if (descriptor < 0) {
        if (errno != ENOENT) {
            return TEXT_ERROR;
        }
        struct stat path_state;
        if (lstat(path, &path_state) == 0 || errno != ENOENT) {
            return TEXT_ERROR;
        }
        char *empty = (char *)calloc(1U, 1U);
        if (!empty) {
            return TEXT_ERROR;
        }
        *data_out = empty;
        return TEXT_OK;
    }
    struct stat before_state;
    text_file_snapshot_t before;
    if (fstat(descriptor, &before_state) != 0 ||
        text_snapshot_from_stat(&before_state, &before) != TEXT_OK) {
        text_close(descriptor);
        return TEXT_ERROR;
    }
    FILE *file = text_fdopen(descriptor, "rb");
    if (!file) {
        text_close(descriptor);
        return TEXT_ERROR;
    }
    size_t len = (size_t)before.size;
    char *data = (char *)malloc(len + 1U);
    if (!data) {
        fclose(file);
        return TEXT_ERROR;
    }
    size_t read_count = len == 0U ? 0U : fread(data, 1U, len, file);
    int read_failed = ferror(file);
    struct stat after_state;
    text_file_snapshot_t after;
    int after_result = fstat(cbm_fileno(file), &after_state) == 0
                           ? text_snapshot_from_stat(&after_state, &after)
                           : TEXT_ERROR;
    int close_failed = fclose(file);
    if (read_count != len || read_failed || close_failed != 0 || after_result != TEXT_OK ||
        !text_snapshot_equal(&before, &after)) {
        free(data);
        return TEXT_ERROR;
    }
#endif
#endif
    data[len] = '\0';
    *data_out = data;
    *len_out = len;
    *snapshot_out = before;
    return TEXT_OK;
}

#ifndef _WIN32
static char *text_parent_directory(const char *path) {
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

static int text_snapshot_matches_path(const char *path, const char *expected_data,
                                      size_t expected_len,
                                      const text_file_snapshot_t *expected_snapshot) {
    char *current_data = NULL;
    size_t current_len = 0U;
    text_file_snapshot_t current_snapshot;
    if (text_read_file(path, &current_data, &current_len, &current_snapshot) != TEXT_OK) {
        return TEXT_ERROR;
    }
    int matches = expected_snapshot->exists == current_snapshot.exists &&
                  text_snapshot_equal(expected_snapshot, &current_snapshot) &&
                  current_len == expected_len &&
                  (expected_len == 0U || memcmp(current_data, expected_data, expected_len) == 0);
    free(current_data);
    return matches ? TEXT_OK : TEXT_ERROR;
}

#ifndef _WIN32
static int text_sync_parent_directory(const char *path) {
    char *parent = text_parent_directory(path);
    if (!parent) {
        return TEXT_ERROR;
    }
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int descriptor = open(parent, flags);
    free(parent);
    if (descriptor < 0) {
        return TEXT_ERROR;
    }
    struct stat state;
    int result = fstat(descriptor, &state) == 0 && S_ISDIR(state.st_mode) && fsync(descriptor) == 0
                     ? TEXT_OK
                     : TEXT_ERROR;
    if (text_close(descriptor) != 0) {
        result = TEXT_ERROR;
    }
    return result;
}
#endif

static int text_replace_file(const char *temp_path, const char *path, int destination_exists) {
#ifdef _WIN32
    wchar_t *wide_temp = cbm_utf8_to_wide(temp_path);
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_temp || !wide_path) {
        free(wide_temp);
        free(wide_path);
        return TEXT_ERROR;
    }
    BOOL replaced = destination_exists ? ReplaceFileW(wide_path, wide_temp, NULL,
                                                      REPLACEFILE_WRITE_THROUGH, NULL, NULL)
                                       : MoveFileExW(wide_temp, wide_path, MOVEFILE_WRITE_THROUGH);
    free(wide_temp);
    free(wide_path);
    return replaced ? TEXT_OK : TEXT_ERROR;
#else
    if (!destination_exists) {
        if (link(temp_path, path) != 0) {
            return TEXT_ERROR;
        }
        if (cbm_unlink(temp_path) != 0) {
            return TEXT_ERROR;
        }
        return text_sync_parent_directory(path);
    }
    if (rename(temp_path, path) != 0) {
        return TEXT_ERROR;
    }
    return text_sync_parent_directory(path);
#endif
}

static int text_write_atomic(const char *path, const char *new_data, size_t new_len,
                             const char *old_data, size_t old_len,
                             const text_file_snapshot_t *snapshot) {
    if (new_len > TEXT_MAX_BYTES || old_len > TEXT_MAX_BYTES) {
        return TEXT_ERROR;
    }
    if (new_len == old_len && (new_len == 0U || memcmp(new_data, old_data, new_len) == 0)) {
        return TEXT_OK;
    }
    size_t path_len = 0U;
    if (text_bounded_strlen(path, TEXT_MAX_PATH_BYTES, &path_len) != TEXT_OK ||
        path_len > SIZE_MAX - TEXT_TEMP_SUFFIX_BYTES - 1U) {
        return TEXT_ERROR;
    }
    size_t capacity = path_len + TEXT_TEMP_SUFFIX_BYTES + 1U;
    char *temp_path = (char *)malloc(capacity);
    if (!temp_path) {
        return TEXT_ERROR;
    }

    FILE *file = NULL;
    for (unsigned attempt = 0U; attempt < TEXT_TEMP_ATTEMPTS; attempt++) {
        unsigned sequence =
            atomic_fetch_add_explicit(&text_temp_sequence, 1U, memory_order_relaxed);
        int written = snprintf(temp_path, capacity, "%s.cbm-text-%ld-%u.tmp", path,
                               (long)TEXT_PROCESS_ID(), sequence);
        if (written < 0 || (size_t)written >= capacity) {
            free(temp_path);
            return TEXT_ERROR;
        }
        errno = 0;
#ifdef _WIN32
        file = cbm_fopen(temp_path, "wbx");
#else
#ifndef O_NOFOLLOW
        free(temp_path);
        return TEXT_ERROR;
#else
        int flags = O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW;
#ifdef O_CLOEXEC
        flags |= O_CLOEXEC;
#endif
        int descriptor = open(temp_path, flags, 0600);
        if (descriptor >= 0) {
            file = text_fdopen(descriptor, "wb");
            if (!file) {
                int saved_error = errno;
                text_close(descriptor);
                (void)cbm_unlink(temp_path);
                errno = saved_error;
            }
        }
#endif
#endif
        if (file) {
            break;
        }
        if (errno != EEXIST) {
            free(temp_path);
            return TEXT_ERROR;
        }
    }
    if (!file) {
        free(temp_path);
        return TEXT_ERROR;
    }

    int failed = new_len != 0U && fwrite(new_data, 1U, new_len, file) != new_len;
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
    if (!failed && TEXT_SYNC(cbm_fileno(file)) != 0) {
        failed = 1;
    }
    if (fclose(file) != 0) {
        failed = 1;
    }
    if (failed) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TEXT_ERROR;
    }
    char *temp_data = NULL;
    size_t temp_len = 0U;
    text_file_snapshot_t temp_snapshot;
    if (text_read_file(temp_path, &temp_data, &temp_len, &temp_snapshot) != TEXT_OK ||
        !temp_snapshot.exists || temp_len != new_len ||
        (new_len != 0U && memcmp(temp_data, new_data, new_len) != 0)) {
        free(temp_data);
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TEXT_ERROR;
    }
    free(temp_data);

#ifdef CBM_TEXT_EDIT_ENABLE_TEST_API
    if (text_precommit_test_hook) {
        text_precommit_test_hook(path, text_precommit_test_context);
    }
#endif
    if (text_snapshot_matches_path(path, old_data, old_len, snapshot) != TEXT_OK) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TEXT_ERROR;
    }
#ifdef CBM_TEXT_EDIT_ENABLE_TEST_API
    if (text_prepublish_test_hook) {
        text_prepublish_test_hook(path, text_prepublish_test_context);
    }
#endif
    if (text_snapshot_matches_path(path, old_data, old_len, snapshot) != TEXT_OK ||
        text_snapshot_matches_path(temp_path, new_data, new_len, &temp_snapshot) != TEXT_OK ||
        text_replace_file(temp_path, path, snapshot->exists) != TEXT_OK) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return TEXT_ERROR;
    }
    free(temp_path);
    return TEXT_OK;
}

static int text_delete_file(const char *path, const char *old_data, size_t old_len,
                            const text_file_snapshot_t *snapshot) {
#ifdef CBM_TEXT_EDIT_ENABLE_TEST_API
    if (text_precommit_test_hook) {
        text_precommit_test_hook(path, text_precommit_test_context);
    }
#endif
    if (text_snapshot_matches_path(path, old_data, old_len, snapshot) != TEXT_OK) {
        return TEXT_ERROR;
    }
#ifdef CBM_TEXT_EDIT_ENABLE_TEST_API
    if (text_prepublish_test_hook) {
        text_prepublish_test_hook(path, text_prepublish_test_context);
    }
#endif
    if (text_snapshot_matches_path(path, old_data, old_len, snapshot) != TEXT_OK) {
        return TEXT_ERROR;
    }
#ifdef _WIN32
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_path) {
        return TEXT_ERROR;
    }
    BOOL removed = DeleteFileW(wide_path);
    free(wide_path);
    return removed ? TEXT_OK : TEXT_ERROR;
#else
    if (unlink(path) != 0) {
        return TEXT_ERROR;
    }
    return text_sync_parent_directory(path);
#endif
}

#ifdef CBM_TEXT_EDIT_ENABLE_TEST_API
void cbm_text_set_precommit_hook_for_testing(cbm_text_precommit_test_hook_t hook, void *context) {
    text_precommit_test_hook = hook;
    text_precommit_test_context = context;
}

void cbm_text_set_prepublish_hook_for_testing(cbm_text_precommit_test_hook_t hook, void *context) {
    text_prepublish_test_hook = hook;
    text_prepublish_test_context = context;
}
#endif

static int text_bytes_equal(const char *data, size_t start, size_t end, const char *value,
                            size_t value_len) {
    return end >= start && end - start == value_len &&
           (value_len == 0U || memcmp(data + start, value, value_len) == 0);
}

static size_t text_count_occurrences(const char *data, size_t len, const char *needle,
                                     size_t needle_len) {
    size_t count = 0U;
    if (needle_len == 0U || needle_len > len) {
        return 0U;
    }
    for (size_t pos = 0U; pos <= len - needle_len;) {
        if (memcmp(data + pos, needle, needle_len) == 0) {
            count++;
            pos += needle_len;
        } else {
            pos++;
        }
    }
    return count;
}

static int text_scan_managed_region(const char *data, size_t len, size_t bom_len,
                                    const char *begin_marker, size_t begin_len,
                                    const char *end_marker, size_t end_len,
                                    text_managed_region_t *region) {
    memset(region, 0, sizeof(*region));
    size_t begin_count = 0U;
    size_t end_count = 0U;
    size_t cursor = bom_len;
    while (cursor < len) {
        size_t line_start = cursor;
        while (cursor < len && data[cursor] != '\n') {
            cursor++;
        }
        size_t text_end = cursor;
        if (text_end > line_start && data[text_end - 1U] == '\r') {
            text_end--;
        }
        size_t full_end = cursor < len ? cursor + 1U : cursor;
        if (text_bytes_equal(data, line_start, text_end, begin_marker, begin_len)) {
            begin_count++;
            if (begin_count == 1U) {
                region->begin_start = line_start;
                region->begin_text_end = text_end;
                region->begin_full_end = full_end;
            }
        }
        if (text_bytes_equal(data, line_start, text_end, end_marker, end_len)) {
            end_count++;
            if (end_count == 1U) {
                region->end_start = line_start;
                region->end_text_end = text_end;
                region->end_full_end = full_end;
            }
        }
        cursor = full_end;
    }
    size_t raw_begin_count =
        text_count_occurrences(data + bom_len, len - bom_len, begin_marker, begin_len);
    size_t raw_end_count =
        text_count_occurrences(data + bom_len, len - bom_len, end_marker, end_len);
    if (raw_begin_count != begin_count || raw_end_count != end_count || begin_count > 1U ||
        end_count > 1U || begin_count != end_count) {
        return TEXT_ERROR;
    }
    if (begin_count == 0U) {
        return TEXT_OK;
    }
    if (region->begin_start >= region->end_start ||
        region->begin_full_end <= region->begin_text_end ||
        region->begin_full_end > region->end_start) {
        return TEXT_ERROR;
    }
    region->present = 1;
    return TEXT_OK;
}

static void text_detect_eol(const char *data, size_t len, const char **eol_out,
                            size_t *eol_len_out) {
    for (size_t pos = 0U; pos < len; pos++) {
        if (data[pos] == '\n') {
            if (pos > 0U && data[pos - 1U] == '\r') {
                *eol_out = "\r\n";
                *eol_len_out = 2U;
            } else {
                *eol_out = "\n";
                *eol_len_out = 1U;
            }
            return;
        }
    }
    *eol_out = "\n";
    *eol_len_out = 1U;
}

static int text_append_normalized(text_buffer_t *buffer, const char *content, size_t content_len,
                                  const char *eol, size_t eol_len) {
    size_t segment_start = 0U;
    size_t pos = 0U;
    while (pos < content_len) {
        if (content[pos] == '\n') {
            size_t segment_end = pos;
            if (segment_end > segment_start && content[segment_end - 1U] == '\r') {
                segment_end--;
            }
            if (text_buffer_append(buffer, content + segment_start, segment_end - segment_start) !=
                    TEXT_OK ||
                text_buffer_append(buffer, eol, eol_len) != TEXT_OK) {
                return TEXT_ERROR;
            }
            pos++;
            segment_start = pos;
        } else {
            pos++;
        }
    }
    return text_buffer_append(buffer, content + segment_start, content_len - segment_start);
}

static int text_content_ends_eol(const char *content, size_t len) {
    return len != 0U && content[len - 1U] == '\n';
}

static int text_append_managed_block(text_buffer_t *buffer, const char *begin_marker,
                                     size_t begin_len, const char *end_marker, size_t end_len,
                                     const char *content, size_t content_len, const char *eol,
                                     size_t eol_len, const char *trailing, size_t trailing_len) {
    if (text_buffer_append(buffer, begin_marker, begin_len) != TEXT_OK ||
        text_buffer_append(buffer, eol, eol_len) != TEXT_OK ||
        text_append_normalized(buffer, content, content_len, eol, eol_len) != TEXT_OK) {
        return TEXT_ERROR;
    }
    if (content_len != 0U && !text_content_ends_eol(content, content_len) &&
        text_buffer_append(buffer, eol, eol_len) != TEXT_OK) {
        return TEXT_ERROR;
    }
    return text_buffer_append(buffer, end_marker, end_len) == TEXT_OK &&
                   text_buffer_append(buffer, trailing, trailing_len) == TEXT_OK
               ? TEXT_OK
               : TEXT_ERROR;
}

static size_t text_bom_length(const char *data, size_t len) {
    static const unsigned char bom[] = {0xefU, 0xbbU, 0xbfU};
    return len >= sizeof(bom) && memcmp(data, bom, sizeof(bom)) == 0 ? sizeof(bom) : 0U;
}

static int text_validate_markers(const char *begin_marker, const char *end_marker,
                                 size_t *begin_len, size_t *end_len) {
    if (text_valid_marker(begin_marker, begin_len) != TEXT_OK ||
        text_valid_marker(end_marker, end_len) != TEXT_OK ||
        (*begin_len == *end_len && memcmp(begin_marker, end_marker, *begin_len) == 0) ||
        text_count_occurrences(begin_marker, *begin_len, end_marker, *end_len) != 0U ||
        text_count_occurrences(end_marker, *end_len, begin_marker, *begin_len) != 0U) {
        return TEXT_ERROR;
    }
    return TEXT_OK;
}

static int text_upsert_managed_block(const char *file_path, const char *begin_marker,
                                     const char *end_marker, const char *owned_content,
                                     size_t max_document_bytes) {
    size_t begin_len = 0U;
    size_t end_len = 0U;
    size_t content_len = 0U;
    if (max_document_bytes == 0U || max_document_bytes > TEXT_MAX_BYTES ||
        !text_valid_path(file_path) ||
        text_validate_markers(begin_marker, end_marker, &begin_len, &end_len) != TEXT_OK ||
        text_bounded_strlen(owned_content, TEXT_MAX_BYTES, &content_len) != TEXT_OK ||
        text_validate_bytes(owned_content, content_len, 0) != TEXT_OK ||
        text_count_occurrences(owned_content, content_len, begin_marker, begin_len) != 0U ||
        text_count_occurrences(owned_content, content_len, end_marker, end_len) != 0U) {
        return TEXT_ERROR;
    }

    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK ||
        text_validate_bytes(old_data, old_len, 1) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    size_t bom_len = text_bom_length(old_data, old_len);
    text_managed_region_t region;
    if (text_scan_managed_region(old_data, old_len, bom_len, begin_marker, begin_len, end_marker,
                                 end_len, &region) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }

    text_buffer_t updated = {0};
    int result = TEXT_ERROR;
    if (region.present) {
        size_t eol_start = region.begin_text_end;
        size_t eol_len = region.begin_full_end - region.begin_text_end;
        const char *eol = old_data + eol_start;
        size_t trailing_len = region.end_full_end - region.end_text_end;
        if (text_buffer_append(&updated, old_data, region.begin_start) == TEXT_OK &&
            text_append_managed_block(&updated, begin_marker, begin_len, end_marker, end_len,
                                      owned_content, content_len, eol, eol_len,
                                      old_data + region.end_text_end, trailing_len) == TEXT_OK &&
            text_buffer_append(&updated, old_data + region.end_full_end,
                               old_len - region.end_full_end) == TEXT_OK &&
            updated.len <= max_document_bytes) {
            result = text_write_atomic(file_path, updated.data, updated.len, old_data, old_len,
                                       &snapshot);
        }
    } else {
        const char *eol = NULL;
        size_t eol_len = 0U;
        text_detect_eol(old_data + bom_len, old_len - bom_len, &eol, &eol_len);
        int meaningful_empty = old_len == bom_len;
        int had_final_eol = old_len > bom_len && old_data[old_len - 1U] == '\n';
        if (text_buffer_append(&updated, old_data, old_len) == TEXT_OK &&
            (!meaningful_empty && !had_final_eol ? text_buffer_append(&updated, eol, eol_len)
                                                 : TEXT_OK) == TEXT_OK &&
            text_append_managed_block(
                &updated, begin_marker, begin_len, end_marker, end_len, owned_content, content_len,
                eol, eol_len, meaningful_empty || had_final_eol ? eol : "",
                meaningful_empty || had_final_eol ? eol_len : 0U) == TEXT_OK &&
            updated.len <= max_document_bytes) {
            result = text_write_atomic(file_path, updated.data, updated.len, old_data, old_len,
                                       &snapshot);
        }
    }
    text_buffer_dispose(&updated);
    free(old_data);
    return result;
}

int cbm_text_upsert_managed_block(const char *file_path, const char *begin_marker,
                                  const char *end_marker, const char *owned_content) {
    return text_upsert_managed_block(file_path, begin_marker, end_marker, owned_content,
                                     TEXT_MAX_BYTES);
}

int cbm_text_upsert_managed_block_limited(const char *file_path, const char *begin_marker,
                                          const char *end_marker, const char *owned_content,
                                          size_t max_document_bytes) {
    return text_upsert_managed_block(file_path, begin_marker, end_marker, owned_content,
                                     max_document_bytes);
}

int cbm_text_remove_managed_block(const char *file_path, const char *begin_marker,
                                  const char *end_marker) {
    size_t begin_len = 0U;
    size_t end_len = 0U;
    if (!text_valid_path(file_path) ||
        text_validate_markers(begin_marker, end_marker, &begin_len, &end_len) != TEXT_OK) {
        return TEXT_ERROR;
    }
    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK ||
        text_validate_bytes(old_data, old_len, 1) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    size_t bom_len = text_bom_length(old_data, old_len);
    text_managed_region_t region;
    if (text_scan_managed_region(old_data, old_len, bom_len, begin_marker, begin_len, end_marker,
                                 end_len, &region) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    if (!region.present) {
        free(old_data);
        return TEXT_OK;
    }

    size_t remove_start = region.begin_start;
    if (region.end_full_end == old_len && region.end_full_end == region.end_text_end &&
        remove_start > bom_len && old_data[remove_start - 1U] == '\n') {
        remove_start--;
        if (remove_start > bom_len && old_data[remove_start - 1U] == '\r') {
            remove_start--;
        }
    }
    text_buffer_t updated = {0};
    int result = TEXT_ERROR;
    if (text_buffer_append(&updated, old_data, remove_start) == TEXT_OK &&
        text_buffer_append(&updated, old_data + region.end_full_end,
                           old_len - region.end_full_end) == TEXT_OK) {
        result =
            text_write_atomic(file_path, updated.data, updated.len, old_data, old_len, &snapshot);
    }
    text_buffer_dispose(&updated);
    free(old_data);
    return result;
}

int cbm_text_write_owned_document(const char *file_path, const char *owned_content) {
    size_t content_len = 0U;
    if (!text_valid_path(file_path) ||
        text_bounded_strlen(owned_content, TEXT_MAX_BYTES, &content_len) != TEXT_OK ||
        text_validate_bytes(owned_content, content_len, 1) != TEXT_OK) {
        return TEXT_ERROR;
    }
    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK ||
        text_validate_bytes(old_data, old_len, 1) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    int result =
        text_write_atomic(file_path, owned_content, content_len, old_data, old_len, &snapshot);
    free(old_data);
    return result;
}

int cbm_text_write_owned_document_if_unchanged(const char *file_path, const char *owned_content,
                                               const char *expected_content,
                                               size_t expected_length) {
    size_t content_len = 0U;
    if (!text_valid_path(file_path) ||
        text_bounded_strlen(owned_content, TEXT_MAX_BYTES, &content_len) != TEXT_OK ||
        text_validate_bytes(owned_content, content_len, 1) != TEXT_OK ||
        (!expected_content && expected_length != 0U) || expected_length > TEXT_MAX_BYTES ||
        (expected_content &&
         text_validate_bytes(expected_content, expected_length, 1) != TEXT_OK)) {
        return TEXT_ERROR;
    }
    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK ||
        text_validate_bytes(old_data, old_len, 1) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    int expected_missing = expected_content == NULL;
    int matches = expected_missing ? !snapshot.exists
                                   : snapshot.exists && old_len == expected_length &&
                                         (expected_length == 0U ||
                                          memcmp(old_data, expected_content, expected_length) == 0);
    if (!matches) {
        free(old_data);
        return TEXT_ERROR;
    }
    int result =
        text_write_atomic(file_path, owned_content, content_len, old_data, old_len, &snapshot);
    free(old_data);
    return result;
}

int cbm_text_create_owned_document(const char *file_path, const char *owned_content) {
    size_t content_len = 0U;
    if (!text_valid_path(file_path) ||
        text_bounded_strlen(owned_content, TEXT_MAX_BYTES, &content_len) != TEXT_OK ||
        text_validate_bytes(owned_content, content_len, 1) != TEXT_OK) {
        return TEXT_ERROR;
    }
    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    if (snapshot.exists) {
        free(old_data);
        return TEXT_ERROR;
    }
    int result =
        text_write_atomic(file_path, owned_content, content_len, old_data, old_len, &snapshot);
    free(old_data);
    return result;
}

int cbm_text_ensure_owned_document(const char *file_path, const char *owned_content) {
    size_t content_len = 0U;
    if (!text_valid_path(file_path) ||
        text_bounded_strlen(owned_content, TEXT_MAX_BYTES, &content_len) != TEXT_OK ||
        text_validate_bytes(owned_content, content_len, 1) != TEXT_OK) {
        return TEXT_ERROR;
    }
    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    if (snapshot.exists) {
        int result = old_len == content_len &&
                             (old_len == 0U || memcmp(old_data, owned_content, old_len) == 0)
                         ? TEXT_OK
                         : TEXT_ERROR;
        free(old_data);
        return result;
    }
    int result =
        text_write_atomic(file_path, owned_content, content_len, old_data, old_len, &snapshot);
    free(old_data);
    return result;
}

static int text_matches_candidate(const char *data, size_t data_len, const char *candidate,
                                  bool *matches) {
    size_t candidate_len = 0U;
    if (!matches || text_bounded_strlen(candidate, TEXT_MAX_BYTES, &candidate_len) != TEXT_OK ||
        text_validate_bytes(candidate, candidate_len, 1) != TEXT_OK) {
        return TEXT_ERROR;
    }
    *matches =
        data_len == candidate_len && (data_len == 0U || memcmp(data, candidate, data_len) == 0);
    return TEXT_OK;
}

int cbm_text_migrate_owned_document(const char *file_path, const char *current_content,
                                    const char *const *released_contents, size_t released_count) {
    size_t current_len = 0U;
    if (!text_valid_path(file_path) ||
        text_bounded_strlen(current_content, TEXT_MAX_BYTES, &current_len) != TEXT_OK ||
        text_validate_bytes(current_content, current_len, 1) != TEXT_OK ||
        (released_count > 0U && !released_contents)) {
        return TEXT_ERROR;
    }
    for (size_t i = 0U; i < released_count; i++) {
        bool ignored = false;
        if (text_matches_candidate("", 0U, released_contents[i], &ignored) != TEXT_OK) {
            return TEXT_ERROR;
        }
    }

    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK ||
        text_validate_bytes(old_data, old_len, 1) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    if (!snapshot.exists) {
        int result = text_write_atomic(file_path, current_content, current_len, old_data, old_len,
                                       &snapshot);
        free(old_data);
        return result;
    }

    bool matches = false;
    if (text_matches_candidate(old_data, old_len, current_content, &matches) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    if (matches) {
        free(old_data);
        return TEXT_OK;
    }
    for (size_t i = 0U; i < released_count; i++) {
        if (text_matches_candidate(old_data, old_len, released_contents[i], &matches) != TEXT_OK) {
            free(old_data);
            return TEXT_ERROR;
        }
        if (matches) {
            int result = text_write_atomic(file_path, current_content, current_len, old_data,
                                           old_len, &snapshot);
            free(old_data);
            return result;
        }
    }
    free(old_data);
    return TEXT_UNOWNED;
}

int cbm_text_remove_owned_document(const char *file_path, const char *expected_owned_content) {
    size_t expected_len = 0U;
    if (!text_valid_path(file_path) ||
        text_bounded_strlen(expected_owned_content, TEXT_MAX_BYTES, &expected_len) != TEXT_OK ||
        text_validate_bytes(expected_owned_content, expected_len, 1) != TEXT_OK) {
        return TEXT_ERROR;
    }
    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK ||
        text_validate_bytes(old_data, old_len, 1) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    if (!snapshot.exists) {
        free(old_data);
        return TEXT_OK;
    }
    if (old_len != expected_len ||
        (old_len != 0U && memcmp(old_data, expected_owned_content, old_len) != 0)) {
        free(old_data);
        return TEXT_UNOWNED;
    }
    int result = text_delete_file(file_path, old_data, old_len, &snapshot);
    free(old_data);
    return result;
}

int cbm_text_remove_owned_document_any(const char *file_path, const char *current_content,
                                       const char *const *released_contents,
                                       size_t released_count) {
    if (!text_valid_path(file_path) || (released_count > 0U && !released_contents)) {
        return TEXT_ERROR;
    }
    bool ignored = false;
    if (text_matches_candidate("", 0U, current_content, &ignored) != TEXT_OK) {
        return TEXT_ERROR;
    }
    for (size_t i = 0U; i < released_count; i++) {
        if (text_matches_candidate("", 0U, released_contents[i], &ignored) != TEXT_OK) {
            return TEXT_ERROR;
        }
    }

    char *old_data = NULL;
    size_t old_len = 0U;
    text_file_snapshot_t snapshot;
    if (text_read_file(file_path, &old_data, &old_len, &snapshot) != TEXT_OK ||
        text_validate_bytes(old_data, old_len, 1) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    if (!snapshot.exists) {
        free(old_data);
        return TEXT_OK;
    }

    bool matches = false;
    if (text_matches_candidate(old_data, old_len, current_content, &matches) != TEXT_OK) {
        free(old_data);
        return TEXT_ERROR;
    }
    for (size_t i = 0U; !matches && i < released_count; i++) {
        if (text_matches_candidate(old_data, old_len, released_contents[i], &matches) != TEXT_OK) {
            free(old_data);
            return TEXT_ERROR;
        }
    }
    if (!matches) {
        free(old_data);
        return TEXT_UNOWNED;
    }
    int result = text_delete_file(file_path, old_data, old_len, &snapshot);
    free(old_data);
    return result;
}
