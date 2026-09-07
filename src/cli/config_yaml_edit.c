/*
 * config_yaml_edit.c — Conservative edits for small agent YAML configs.
 *
 * This is deliberately not a general YAML parser. It recognizes only the
 * top-level mapping and flat string-list shapes used by the CLI integrations,
 * and fails closed before writing when the target uses ambiguous YAML syntax.
 */
#include "cli/config_yaml_edit.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifdef _WIN32
#include "foundation/win_utf8.h"

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <io.h>
#include <process.h>
#include <windows.h>
#define YAML_PROCESS_ID _getpid
#define YAML_SYNC _commit
#else
#include <sys/file.h>
#include <sys/types.h>
#include <unistd.h>
#define YAML_PROCESS_ID getpid
#define YAML_SYNC fsync
#endif

enum {
    YAML_ERROR = -1,
    YAML_OK = 0,
    YAML_UNIT = 1,
    YAML_MATCH = 1,
    YAML_BYTES_PER_KIB = 1024,
    YAML_INPUT_MIB = 16,
    YAML_OUTPUT_MIB = 32,
    YAML_BLOCK_MIB = 1,
    YAML_ITEM_KIB = 64,
    YAML_KEY_LIMIT = 255,
    YAML_TMP_SUFFIX_LIMIT = 80,
    YAML_LOCK_SUFFIX_LIMIT = 32,
    YAML_TMP_ATTEMPTS = 100,
    YAML_BUFFER_INITIAL_CAPACITY = 256,
    YAML_GROWTH_FACTOR = 2,
    YAML_UTF8_BOM_LEN = 3,
    YAML_ENTRY_INDENT = 2,
    YAML_VALUE_INDENT = 4,
    YAML_ITEM_INITIAL_CAPACITY = 8,
    YAML_SEQUENCE_PATH_LIMIT = 16,
    YAML_DOC_MARKER_LEN = 3,
    YAML_QUOTED_MIN_LEN = 2,
    YAML_HEX_DIGIT_COUNT = 2,
    YAML_HEX_ALPHA_OFFSET = 10,
    YAML_NIBBLE_SHIFT = 4,
    YAML_CONTROL_LIMIT = 0x20,
    YAML_DELETE_BYTE = 0x7f,
    YAML_ESCAPE_BYTE = 0x1b,
    YAML_BOM_BYTE_0 = 0xef,
    YAML_BOM_BYTE_1 = 0xbb,
    YAML_BOM_BYTE_2 = 0xbf,
    YAML_LOCK_FILE_MODE = 0600,
    YAML_NEW_FILE_MODE = 0600,
    YAML_PERMISSION_MASK = 0777,
};

#define YAML_INPUT_MAX ((size_t)YAML_INPUT_MIB * YAML_BYTES_PER_KIB * YAML_BYTES_PER_KIB)
#define YAML_OUTPUT_MAX ((size_t)YAML_OUTPUT_MIB * YAML_BYTES_PER_KIB * YAML_BYTES_PER_KIB)
#define YAML_BLOCK_MAX ((size_t)YAML_BLOCK_MIB * YAML_BYTES_PER_KIB * YAML_BYTES_PER_KIB)
#define YAML_ITEM_MAX ((size_t)YAML_ITEM_KIB * YAML_BYTES_PER_KIB)
#define YAML_KEY_MAX ((size_t)YAML_KEY_LIMIT)
#define YAML_TMP_SUFFIX_MAX ((size_t)YAML_TMP_SUFFIX_LIMIT)
#define YAML_LOCK_SUFFIX_MAX ((size_t)YAML_LOCK_SUFFIX_LIMIT)
#define YAML_LITERAL_LEN(value) (sizeof(value) - YAML_UNIT)

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} yaml_buf_t;

typedef struct {
    size_t start;
    size_t text_end;
    size_t end;
    size_t indent;
    bool blank;
    bool comment;
    /* Double-quoted scalars may continue across lines via a trailing `\`
     * (#1631). dquote_opens: this line ends inside an open double quote whose
     * last byte is the continuation backslash. dquote_cont: this line is the
     * textual continuation of such a scalar — VALUE BYTES, not structure;
     * every structural walker must skip it. */
    bool dquote_opens;
    bool dquote_cont;
} yaml_line_t;

typedef struct {
    const char *data;
    size_t len;
    yaml_line_t *lines;
    size_t line_count;
    const char *eol;
    size_t eol_len;
} yaml_doc_t;

typedef struct {
    bool section_found;
    bool section_inline_empty;
    size_t section_line;
    size_t section_colon;
    size_t section_end;
    bool entry_found;
    size_t entry_line;
    size_t entry_end;
    size_t entry_count;
} yaml_mapping_target_t;

typedef struct {
    bool exists;
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
} yaml_file_snapshot_t;

typedef struct {
    char *path;
#ifdef _WIN32
    HANDLE handle;
    DWORD volume_serial;
    DWORD file_index_high;
    DWORD file_index_low;
#else
    int descriptor;
    dev_t device;
    ino_t inode;
    uid_t owner;
#endif
} yaml_config_lock_t;

typedef struct {
    size_t raw_start;
    size_t raw_end;
    size_t line_start;
    size_t line_end;
    char *value;
} yaml_item_t;

typedef struct {
    yaml_item_t *items;
    size_t len;
    size_t cap;
} yaml_item_vec_t;

typedef enum {
    YAML_LIST_ABSENT = 0,
    YAML_LIST_SCALAR,
    YAML_LIST_FLOW,
    YAML_LIST_BLOCK,
} yaml_list_kind_t;

typedef struct {
    yaml_list_kind_t kind;
    size_t key_line;
    size_t section_end;
    size_t value_start;
    size_t value_end;
    yaml_item_vec_t items;
} yaml_list_target_t;

static atomic_uint yaml_temp_sequence = ATOMIC_VAR_INIT(0);
#ifdef CBM_YAML_ENABLE_TEST_API
static cbm_yaml_precommit_test_hook_t yaml_precommit_test_hook = NULL;
static void *yaml_precommit_test_context = NULL;
static cbm_yaml_precommit_test_hook_t yaml_prepublish_test_hook = NULL;
static void *yaml_prepublish_test_context = NULL;
static cbm_yaml_lock_postcreate_test_hook_t yaml_lock_postcreate_test_hook = NULL;
static void *yaml_lock_postcreate_test_context = NULL;
#endif

static int yaml_decode_scalar(const char *data, size_t start, size_t end, char **out_value);
static int yaml_find_mapping_colon(const char *data, size_t start, size_t end, size_t *out_colon);
static int yaml_validate_root_mapping(const yaml_doc_t *doc);
static int yaml_validate_utf8(const char *value, size_t len);

static void yaml_buf_free(yaml_buf_t *buf) {
    free(buf->data);
    memset(buf, 0, sizeof(*buf));
}

static int yaml_buf_reserve(yaml_buf_t *buf, size_t extra) {
    if (extra > YAML_OUTPUT_MAX || buf->len > YAML_OUTPUT_MAX - extra) {
        return YAML_ERROR;
    }
    size_t needed = buf->len + extra;
    if (needed == SIZE_MAX) {
        return YAML_ERROR;
    }
    needed++;
    if (needed <= buf->cap) {
        return 0;
    }

    size_t cap = buf->cap ? buf->cap : YAML_BUFFER_INITIAL_CAPACITY;
    while (cap < needed) {
        if (cap > YAML_OUTPUT_MAX / YAML_GROWTH_FACTOR) {
            cap = YAML_OUTPUT_MAX + YAML_UNIT;
            break;
        }
        cap *= YAML_GROWTH_FACTOR;
    }
    if (cap < needed || cap > YAML_OUTPUT_MAX + YAML_UNIT) {
        return YAML_ERROR;
    }
    char *grown = (char *)realloc(buf->data, cap);
    if (!grown) {
        return YAML_ERROR;
    }
    buf->data = grown;
    buf->cap = cap;
    return 0;
}

static int yaml_buf_append(yaml_buf_t *buf, const char *data, size_t len) {
    if (len == 0U) {
        return 0;
    }
    if (!data || yaml_buf_reserve(buf, len) != 0) {
        return YAML_ERROR;
    }
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    buf->data[buf->len] = '\0';
    return 0;
}

static int yaml_buf_append_char(yaml_buf_t *buf, char value) {
    return yaml_buf_append(buf, &value, YAML_UNIT);
}

static int yaml_bounded_strlen(const char *value, size_t max_len, size_t *out_len) {
    if (!value || !out_len) {
        return YAML_ERROR;
    }
    for (size_t i = 0; i <= max_len; i++) {
        if (value[i] == '\0') {
            *out_len = i;
            return 0;
        }
    }
    return YAML_ERROR;
}

static int yaml_validate_key(const char *key, size_t *out_len) {
    size_t len = 0;
    if (yaml_bounded_strlen(key, YAML_KEY_MAX, &len) != 0 || len == 0U) {
        return YAML_ERROR;
    }
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)key[i];
        if (!(isalnum(c) || c == '_' || c == '-' || c == '.')) {
            return YAML_ERROR;
        }
    }
    *out_len = len;
    return 0;
}

static int yaml_validate_text_bytes_from(const char *data, size_t len, size_t from);

static int yaml_validate_text_bytes(const char *data, size_t len) {
    if (len >= YAML_UTF8_BOM_LEN && (unsigned char)data[0] == YAML_BOM_BYTE_0 &&
        (unsigned char)data[YAML_UNIT] == YAML_BOM_BYTE_1 &&
        (unsigned char)data[YAML_ENTRY_INDENT] == YAML_BOM_BYTE_2) {
        return YAML_ERROR;
    }
    return yaml_validate_text_bytes_from(data, len, 0U);
}

/* Body of the byte validation, entered past any prologue. */
static int yaml_validate_text_bytes_from(const char *data, size_t len, size_t from) {
    for (size_t i = from; i < len; i++) {
        unsigned char c = (unsigned char)data[i];
        if (c == '\t' || c == '\0' || c == YAML_DELETE_BYTE) {
            return YAML_ERROR;
        }
        if (c == '\r') {
            if (i + YAML_UNIT >= len || data[i + YAML_UNIT] != '\n') {
                return YAML_ERROR;
            }
            continue;
        }
        if (c < YAML_CONTROL_LIMIT && c != '\n') {
            return YAML_ERROR;
        }
    }
    return yaml_validate_utf8(data, len);
}

static int yaml_build_lock_path(const char *path, char **out_path) {
    static const char suffix[] = ".cbm-yaml.lock";
    size_t path_len = 0U;
    if (yaml_bounded_strlen(path, YAML_OUTPUT_MAX, &path_len) != 0 || path_len == 0U ||
        path_len > SIZE_MAX - YAML_LOCK_SUFFIX_MAX - YAML_UNIT) {
        return YAML_ERROR;
    }
    size_t capacity = path_len + YAML_LOCK_SUFFIX_MAX + YAML_UNIT;
    char *lock_path = (char *)malloc(capacity);
    if (!lock_path) {
        return YAML_ERROR;
    }
    int written = snprintf(lock_path, capacity, "%s%s", path, suffix);
    if (written < 0 || (size_t)written >= capacity) {
        free(lock_path);
        return YAML_ERROR;
    }
    *out_path = lock_path;
    return 0;
}

#ifdef _WIN32
static void yaml_remove_open_lock_directory(HANDLE handle) {
    FILE_DISPOSITION_INFO disposition = {.DeleteFile = TRUE};
    (void)SetFileInformationByHandle(handle, FileDispositionInfo, &disposition,
                                     sizeof(disposition));
    (void)CloseHandle(handle);
}
#else
static bool yaml_lock_file_state_is_safe(const struct stat *state) {
    return S_ISREG(state->st_mode) && state->st_nlink == 1 && state->st_uid == geteuid() &&
           (state->st_mode & YAML_PERMISSION_MASK) == YAML_LOCK_FILE_MODE &&
           (state->st_mode & (S_ISUID | S_ISGID | S_ISVTX)) == 0;
}

static bool yaml_lock_file_state_matches(const struct stat *left, const struct stat *right) {
    return left->st_dev == right->st_dev && left->st_ino == right->st_ino &&
           left->st_mode == right->st_mode && left->st_nlink == right->st_nlink &&
           left->st_uid == right->st_uid;
}

static int yaml_flock_nointr(int descriptor, int operation) {
    int result = 0;
    do {
        result = flock(descriptor, operation);
    } while (result != 0 && errno == EINTR);
    return result == 0 ? 0 : YAML_ERROR;
}
#endif

static int yaml_lock_acquire(const char *path, yaml_config_lock_t *lock) {
    memset(lock, 0, sizeof(*lock));
#ifdef _WIN32
    lock->handle = INVALID_HANDLE_VALUE;
#else
    lock->descriptor = -1;
#endif
    if (yaml_build_lock_path(path, &lock->path) != 0) {
        return YAML_ERROR;
    }

#ifdef _WIN32
    wchar_t *wide_path = cbm_utf8_to_wide(lock->path);
    if (!wide_path) {
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }
    if (!CreateDirectoryW(wide_path, NULL)) {
        free(wide_path);
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }
    HANDLE handle = CreateFileW(wide_path, DELETE | FILE_READ_ATTRIBUTES,
                                FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
                                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (handle == INVALID_HANDLE_VALUE) {
        (void)RemoveDirectoryW(wide_path);
        free(wide_path);
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }
    BY_HANDLE_FILE_INFORMATION created_info;
    if (!GetFileInformationByHandle(handle, &created_info) ||
        (created_info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0 ||
        (created_info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
        yaml_remove_open_lock_directory(handle);
        free(wide_path);
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }
#ifdef CBM_YAML_ENABLE_TEST_API
    if (yaml_lock_postcreate_test_hook) {
        yaml_lock_postcreate_test_hook(lock->path, yaml_lock_postcreate_test_context);
    }
#endif
    BY_HANDLE_FILE_INFORMATION info;
    if (!GetFileInformationByHandle(handle, &info) ||
        (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0 ||
        (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0 ||
        info.dwVolumeSerialNumber != created_info.dwVolumeSerialNumber ||
        info.nFileIndexHigh != created_info.nFileIndexHigh ||
        info.nFileIndexLow != created_info.nFileIndexLow) {
        yaml_remove_open_lock_directory(handle);
        free(wide_path);
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }
    free(wide_path);
    lock->handle = handle;
    lock->volume_serial = info.dwVolumeSerialNumber;
    lock->file_index_high = info.nFileIndexHigh;
    lock->file_index_low = info.nFileIndexLow;
#else
#ifndef O_NOFOLLOW
    free(lock->path);
    lock->path = NULL;
    return YAML_ERROR;
#else
    int flags = O_RDWR | O_NOFOLLOW | O_NONBLOCK;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    bool created = false;
    int descriptor = open(lock->path, flags | O_CREAT | O_EXCL, YAML_LOCK_FILE_MODE);
    if (descriptor >= 0) {
        created = true;
    } else if (errno == EEXIST) {
        descriptor = open(lock->path, flags);
    }
    if (descriptor < 0 || (created && fchmod(descriptor, YAML_LOCK_FILE_MODE) != 0)) {
        if (descriptor >= 0) {
            (void)close(descriptor);
        }
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }

    struct stat opened_state;
    if (fstat(descriptor, &opened_state) != 0 || !yaml_lock_file_state_is_safe(&opened_state)) {
        (void)close(descriptor);
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }
#ifdef CBM_YAML_ENABLE_TEST_API
    if (yaml_lock_postcreate_test_hook) {
        yaml_lock_postcreate_test_hook(lock->path, yaml_lock_postcreate_test_context);
    }
#endif
    if (yaml_flock_nointr(descriptor, LOCK_EX | LOCK_NB) != 0) {
        (void)close(descriptor);
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }

    struct stat handle_state;
    struct stat path_state;
    bool safe = fstat(descriptor, &handle_state) == 0 && lstat(lock->path, &path_state) == 0 &&
                yaml_lock_file_state_is_safe(&handle_state) &&
                yaml_lock_file_state_is_safe(&path_state) &&
                yaml_lock_file_state_matches(&opened_state, &handle_state) &&
                yaml_lock_file_state_matches(&handle_state, &path_state);
    if (!safe) {
        (void)yaml_flock_nointr(descriptor, LOCK_UN);
        (void)close(descriptor);
        free(lock->path);
        lock->path = NULL;
        return YAML_ERROR;
    }
    lock->descriptor = descriptor;
    lock->device = handle_state.st_dev;
    lock->inode = handle_state.st_ino;
    lock->owner = handle_state.st_uid;
#endif
#endif
    return 0;
}

static int yaml_lock_release(yaml_config_lock_t *lock) {
    int result = YAML_ERROR;
#ifdef _WIN32
    if (lock->handle != INVALID_HANDLE_VALUE) {
        BY_HANDLE_FILE_INFORMATION info;
        FILE_DISPOSITION_INFO disposition = {.DeleteFile = TRUE};
        bool same = GetFileInformationByHandle(lock->handle, &info) &&
                    (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                    (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
                    info.dwVolumeSerialNumber == lock->volume_serial &&
                    info.nFileIndexHigh == lock->file_index_high &&
                    info.nFileIndexLow == lock->file_index_low;
        bool removed = same && SetFileInformationByHandle(lock->handle, FileDispositionInfo,
                                                          &disposition, sizeof(disposition));
        BOOL closed = CloseHandle(lock->handle);
        result = removed && closed ? 0 : YAML_ERROR;
        lock->handle = INVALID_HANDLE_VALUE;
    }
#else
    if (lock->descriptor >= 0) {
        struct stat handle_state;
        struct stat path_state;
        bool same =
            fstat(lock->descriptor, &handle_state) == 0 && lstat(lock->path, &path_state) == 0 &&
            yaml_lock_file_state_is_safe(&handle_state) &&
            yaml_lock_file_state_is_safe(&path_state) && handle_state.st_dev == lock->device &&
            handle_state.st_ino == lock->inode && handle_state.st_uid == lock->owner &&
            yaml_lock_file_state_matches(&handle_state, &path_state);
        int unlocked = yaml_flock_nointr(lock->descriptor, LOCK_UN);
        int closed = close(lock->descriptor);
        result = same && unlocked == 0 && closed == 0 ? 0 : YAML_ERROR;
        lock->descriptor = -1;
    }
#endif
    free(lock->path);
    lock->path = NULL;
    return result;
}

static bool yaml_snapshot_state_equal(const yaml_file_snapshot_t *left,
                                      const yaml_file_snapshot_t *right) {
    if (left->exists != right->exists) {
        return false;
    }
    if (!left->exists) {
        return true;
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
static int yaml_snapshot_from_handle(HANDLE handle, yaml_file_snapshot_t *snapshot) {
    BY_HANDLE_FILE_INFORMATION info;
    if (GetFileType(handle) != FILE_TYPE_DISK || !GetFileInformationByHandle(handle, &info) ||
        (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0 ||
        info.nNumberOfLinks != 1U) {
        return YAML_ERROR;
    }
    uint64_t size = ((uint64_t)info.nFileSizeHigh << 32U) | (uint64_t)info.nFileSizeLow;
    if (size > YAML_INPUT_MAX) {
        return YAML_ERROR;
    }
    *snapshot = (yaml_file_snapshot_t){
        .exists = true,
        .volume_serial = info.dwVolumeSerialNumber,
        .file_index_high = info.nFileIndexHigh,
        .file_index_low = info.nFileIndexLow,
        .attributes = info.dwFileAttributes,
        .link_count = info.nNumberOfLinks,
        .creation_time = info.ftCreationTime,
        .write_time = info.ftLastWriteTime,
        .size = size,
    };
    return 0;
}
#else
static int yaml_snapshot_from_stat(const struct stat *state, yaml_file_snapshot_t *snapshot) {
    if (!S_ISREG(state->st_mode) || state->st_nlink != 1U || state->st_size < 0 ||
        (uint64_t)state->st_size > YAML_INPUT_MAX ||
        (state->st_mode & (S_ISUID | S_ISGID | S_ISVTX)) != 0) {
        return YAML_ERROR;
    }
    *snapshot = (yaml_file_snapshot_t){
        .exists = true,
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
    return 0;
}
#endif

static int yaml_read_file(const char *path, char **out_data, size_t *out_len,
                          yaml_file_snapshot_t *out_snapshot) {
    *out_data = NULL;
    *out_len = 0U;
    memset(out_snapshot, 0, sizeof(*out_snapshot));
#ifdef _WIN32
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_path) {
        return YAML_ERROR;
    }
    HANDLE handle = CreateFileW(
        wide_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    free(wide_path);
    if (handle == INVALID_HANDLE_VALUE) {
        DWORD error = GetLastError();
        if (error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND) {
            return YAML_ERROR;
        }
        char *empty = (char *)calloc(YAML_UNIT, YAML_UNIT);
        if (!empty) {
            return YAML_ERROR;
        }
        *out_data = empty;
        return 0;
    }
    yaml_file_snapshot_t before;
    if (yaml_snapshot_from_handle(handle, &before) != 0) {
        CloseHandle(handle);
        return YAML_ERROR;
    }
    size_t len = (size_t)before.size;
    char *data = (char *)malloc(len + YAML_UNIT);
    if (!data) {
        CloseHandle(handle);
        return YAML_ERROR;
    }
    DWORD read_count = 0U;
    BOOL read_ok = ReadFile(handle, data, (DWORD)len, &read_count, NULL);
    yaml_file_snapshot_t after;
    int after_result = yaml_snapshot_from_handle(handle, &after);
    BOOL close_ok = CloseHandle(handle);
    if (!read_ok || read_count != (DWORD)len || after_result != 0 || !close_ok ||
        !yaml_snapshot_state_equal(&before, &after)) {
        free(data);
        return YAML_ERROR;
    }
#else
#ifndef O_NOFOLLOW
    (void)path;
    return YAML_ERROR;
#else
    int flags = O_RDONLY | O_NOFOLLOW | O_NONBLOCK;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int descriptor = open(path, flags);
    if (descriptor < 0) {
        if (errno != ENOENT) {
            return YAML_ERROR;
        }
        struct stat path_state;
        if (lstat(path, &path_state) == 0 || errno != ENOENT) {
            return YAML_ERROR;
        }
        char *empty = (char *)calloc(YAML_UNIT, YAML_UNIT);
        if (!empty) {
            return YAML_ERROR;
        }
        *out_data = empty;
        return 0;
    }
    struct stat before_state;
    yaml_file_snapshot_t before;
    if (fstat(descriptor, &before_state) != 0 ||
        yaml_snapshot_from_stat(&before_state, &before) != 0) {
        close(descriptor);
        return YAML_ERROR;
    }
    FILE *file = fdopen(descriptor, "rb");
    if (!file) {
        close(descriptor);
        return YAML_ERROR;
    }
    size_t len = (size_t)before.size;
    char *data = (char *)malloc(len + YAML_UNIT);
    if (!data) {
        fclose(file);
        return YAML_ERROR;
    }
    size_t read_count = fread(data, YAML_UNIT, len, file);
    int read_failed = ferror(file);
    struct stat after_state;
    yaml_file_snapshot_t after;
    int after_result = fstat(cbm_fileno(file), &after_state) == 0
                           ? yaml_snapshot_from_stat(&after_state, &after)
                           : YAML_ERROR;
    int close_failed = fclose(file);
    if (read_count != len || read_failed || after_result != 0 || close_failed != 0 ||
        !yaml_snapshot_state_equal(&before, &after)) {
        free(data);
        return YAML_ERROR;
    }
#endif
#endif
    data[len] = '\0';
    /* Documents may open with a UTF-8 BOM (#1656) — validated past it here,
     * treated as a prologue by yaml_doc_init, preserved verbatim on write.
     * Non-document inputs (keys, blocks, identity scalars) keep the strict
     * no-BOM rule via yaml_validate_text_bytes. */
    {
        size_t prologue = 0U;
        if (len >= YAML_UTF8_BOM_LEN && (unsigned char)data[0] == YAML_BOM_BYTE_0 &&
            (unsigned char)data[YAML_UNIT] == YAML_BOM_BYTE_1 &&
            (unsigned char)data[YAML_ENTRY_INDENT] == YAML_BOM_BYTE_2) {
            prologue = YAML_UTF8_BOM_LEN;
        }
        if (yaml_validate_text_bytes_from(data, len, prologue) != 0) {
            free(data);
            return YAML_ERROR;
        }
    }
    *out_data = data;
    *out_len = len;
    *out_snapshot = before;
    return 0;
}

#ifndef _WIN32
static char *yaml_parent_directory(const char *path) {
    const char *separator = strrchr(path, '/');
    if (!separator) {
        char *current = (char *)malloc(YAML_LITERAL_LEN(".") + YAML_UNIT);
        if (current) {
            memcpy(current, ".", YAML_LITERAL_LEN(".") + YAML_UNIT);
        }
        return current;
    }
    size_t len = separator == path ? YAML_UNIT : (size_t)(separator - path);
    char *parent = (char *)malloc(len + YAML_UNIT);
    if (!parent) {
        return NULL;
    }
    memcpy(parent, path, len);
    parent[len] = '\0';
    return parent;
}
#endif

static int yaml_snapshot_matches_path(const char *path, const char *expected_data,
                                      size_t expected_len,
                                      const yaml_file_snapshot_t *expected_snapshot) {
    char *current_data = NULL;
    size_t current_len = 0U;
    yaml_file_snapshot_t current_snapshot;
    if (yaml_read_file(path, &current_data, &current_len, &current_snapshot) != 0) {
        return YAML_ERROR;
    }
    bool matches = false;
    if (!expected_snapshot->exists) {
        matches = !current_snapshot.exists;
    } else {
        matches = current_snapshot.exists && current_len == expected_len &&
                  yaml_snapshot_state_equal(expected_snapshot, &current_snapshot) &&
                  (expected_len == 0U || memcmp(current_data, expected_data, expected_len) == 0);
    }
    free(current_data);
    return matches ? 0 : YAML_ERROR;
}

#ifndef _WIN32
static int yaml_sync_parent_directory(const char *path) {
    char *parent = yaml_parent_directory(path);
    if (!parent) {
        return YAML_ERROR;
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
        return YAML_ERROR;
    }
    struct stat state;
    int result = fstat(descriptor, &state) == 0 && S_ISDIR(state.st_mode) && fsync(descriptor) == 0
                     ? 0
                     : YAML_ERROR;
    if (close(descriptor) != 0) {
        result = YAML_ERROR;
    }
    return result;
}
#endif

static int yaml_replace_file(const char *temp_path, const char *path, bool destination_exists) {
#ifdef _WIN32
    wchar_t *wide_temp = cbm_utf8_to_wide(temp_path);
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_temp || !wide_path) {
        free(wide_temp);
        free(wide_path);
        return YAML_ERROR;
    }
    BOOL replaced = destination_exists ? ReplaceFileW(wide_path, wide_temp, NULL,
                                                      REPLACEFILE_WRITE_THROUGH, NULL, NULL)
                                       : MoveFileExW(wide_temp, wide_path, MOVEFILE_WRITE_THROUGH);
    free(wide_temp);
    free(wide_path);
    return replaced ? 0 : YAML_ERROR;
#else
    if (!destination_exists) {
        if (link(temp_path, path) != 0) {
            return YAML_ERROR;
        }
        if (cbm_unlink(temp_path) != 0) {
            return YAML_ERROR;
        }
        return yaml_sync_parent_directory(path);
    }
    if (rename(temp_path, path) != 0) {
        return YAML_ERROR;
    }
    return yaml_sync_parent_directory(path);
#endif
}

static int yaml_write_atomic(const char *path, const char *data, size_t len,
                             const char *expected_data, size_t expected_len,
                             const yaml_file_snapshot_t *expected_snapshot) {
    size_t path_len = 0U;
    if (yaml_bounded_strlen(path, YAML_OUTPUT_MAX, &path_len) != 0 ||
        path_len > SIZE_MAX - YAML_TMP_SUFFIX_MAX - YAML_UNIT) {
        return YAML_ERROR;
    }
    size_t capacity = path_len + YAML_TMP_SUFFIX_MAX + YAML_UNIT;
    char *temp_path = (char *)malloc(capacity);
    if (!temp_path) {
        return YAML_ERROR;
    }

    FILE *file = NULL;
    for (unsigned attempt = 0U; attempt < YAML_TMP_ATTEMPTS; attempt++) {
        unsigned sequence =
            atomic_fetch_add_explicit(&yaml_temp_sequence, YAML_UNIT, memory_order_relaxed);
        int written = snprintf(temp_path, capacity, "%s.cbm-yaml-%ld-%u.tmp", path,
                               (long)YAML_PROCESS_ID(), sequence);
        if (written < 0 || (size_t)written >= capacity) {
            free(temp_path);
            return YAML_ERROR;
        }
        errno = 0;
#ifdef _WIN32
        file = cbm_fopen(temp_path, "wbx");
#else
        int flags = O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW;
#ifdef O_CLOEXEC
        flags |= O_CLOEXEC;
#endif
        int descriptor = open(temp_path, flags, YAML_NEW_FILE_MODE);
        if (descriptor >= 0) {
            file = fdopen(descriptor, "wb");
            if (!file) {
                (void)close(descriptor);
                (void)cbm_unlink(temp_path);
                free(temp_path);
                return YAML_ERROR;
            }
        }
#endif
        if (file) {
            break;
        }
        if (errno != EEXIST) {
            free(temp_path);
            return YAML_ERROR;
        }
    }
    if (!file) {
        free(temp_path);
        return YAML_ERROR;
    }

    bool failed = fwrite(data, YAML_UNIT, len, file) != len;
    if (!failed && fflush(file) != 0) {
        failed = true;
    }
#ifndef _WIN32
    if (!failed && expected_snapshot->exists &&
        fchown(cbm_fileno(file), expected_snapshot->owner, expected_snapshot->group) != 0) {
        failed = true;
    }
    mode_t mode = expected_snapshot->exists ? expected_snapshot->mode & YAML_PERMISSION_MASK
                                            : YAML_NEW_FILE_MODE;
    if (!failed && fchmod(cbm_fileno(file), mode) != 0) {
        failed = true;
    }
#endif
    if (!failed && YAML_SYNC(cbm_fileno(file)) != 0) {
        failed = true;
    }
    if (fclose(file) != 0) {
        failed = true;
    }
    if (failed) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return YAML_ERROR;
    }
    char *temp_data = NULL;
    size_t temp_len = 0U;
    yaml_file_snapshot_t temp_snapshot;
    if (yaml_read_file(temp_path, &temp_data, &temp_len, &temp_snapshot) != 0 ||
        !temp_snapshot.exists || temp_len != len ||
        (len != 0U && memcmp(temp_data, data, len) != 0)) {
        free(temp_data);
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return YAML_ERROR;
    }
    free(temp_data);

#ifdef CBM_YAML_ENABLE_TEST_API
    if (yaml_precommit_test_hook) {
        yaml_precommit_test_hook(path, yaml_precommit_test_context);
    }
#endif
    if (yaml_snapshot_matches_path(path, expected_data, expected_len, expected_snapshot) != 0) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return YAML_ERROR;
    }
#ifdef CBM_YAML_ENABLE_TEST_API
    if (yaml_prepublish_test_hook) {
        yaml_prepublish_test_hook(path, yaml_prepublish_test_context);
    }
#endif
    if (yaml_snapshot_matches_path(path, expected_data, expected_len, expected_snapshot) != 0 ||
        yaml_snapshot_matches_path(temp_path, data, len, &temp_snapshot) != 0 ||
        yaml_replace_file(temp_path, path, expected_snapshot->exists) != 0) {
        (void)cbm_unlink(temp_path);
        free(temp_path);
        return YAML_ERROR;
    }
    free(temp_path);
    return 0;
}

#ifdef CBM_YAML_ENABLE_TEST_API
void cbm_yaml_set_precommit_hook_for_testing(cbm_yaml_precommit_test_hook_t hook, void *context) {
    yaml_precommit_test_hook = hook;
    yaml_precommit_test_context = context;
}

void cbm_yaml_set_prepublish_hook_for_testing(cbm_yaml_precommit_test_hook_t hook, void *context) {
    yaml_prepublish_test_hook = hook;
    yaml_prepublish_test_context = context;
}

void cbm_yaml_set_lock_postcreate_hook_for_testing(cbm_yaml_lock_postcreate_test_hook_t hook,
                                                   void *context) {
    yaml_lock_postcreate_test_hook = hook;
    yaml_lock_postcreate_test_context = context;
}
#endif

static int yaml_commit_if_changed(const char *path, const char *old_data, size_t old_len,
                                  const yaml_file_snapshot_t *snapshot, const yaml_buf_t *updated) {
    if (old_len == updated->len &&
        (old_len == 0U || memcmp(old_data, updated->data, old_len) == 0)) {
        return 0;
    }
    return yaml_write_atomic(path, updated->data ? updated->data : "", updated->len, old_data,
                             old_len, snapshot);
}

static size_t yaml_count_lines(const yaml_doc_t *doc) {
    if (doc->len == 0U) {
        return 0U;
    }
    size_t count = YAML_UNIT;
    for (size_t i = 0; i + YAML_UNIT < doc->len; i++) {
        if (doc->data[i] == '\n') {
            count++;
        }
    }
    return count;
}

static size_t yaml_parse_line(const yaml_doc_t *doc, size_t start, yaml_line_t *line) {
    size_t end = start;
    while (end < doc->len && doc->data[end] != '\n') {
        end++;
    }
    size_t full_end = end < doc->len ? end + YAML_UNIT : end;
    size_t text_end = end;
    if (text_end > start && doc->data[text_end - YAML_UNIT] == '\r') {
        text_end--;
    }
    size_t indent = start;
    while (indent < text_end && doc->data[indent] == ' ') {
        indent++;
    }
    line->start = start;
    line->text_end = text_end;
    line->end = full_end;
    line->indent = indent - start;
    line->blank = indent == text_end;
    line->comment = !line->blank && doc->data[indent] == '#';
    return full_end;
}

static void yaml_detect_eol(yaml_doc_t *doc) {
    doc->eol = "\n";
    doc->eol_len = YAML_UNIT;
    for (size_t i = 0; i < doc->line_count; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->end > line->text_end && line->end - line->text_end == YAML_ENTRY_INDENT &&
            doc->data[line->text_end] == '\r') {
            doc->eol = "\r\n";
            doc->eol_len = YAML_LITERAL_LEN("\r\n");
            break;
        }
        if (line->end > line->text_end) {
            break;
        }
    }
}

static int yaml_build_lines(yaml_doc_t *doc) {
    size_t count = yaml_count_lines(doc);
    if (count == 0U) {
        yaml_detect_eol(doc);
        return 0;
    }
    if (count > SIZE_MAX / sizeof(yaml_line_t)) {
        return YAML_ERROR;
    }
    yaml_line_t *lines = (yaml_line_t *)calloc(count, sizeof(*lines));
    if (!lines) {
        return YAML_ERROR;
    }

    size_t line_index = 0U;
    size_t start = 0U;
    while (start < doc->len && line_index < count) {
        start = yaml_parse_line(doc, start, &lines[line_index++]);
    }
    if (start != doc->len || line_index != count) {
        free(lines);
        return YAML_ERROR;
    }
    doc->lines = lines;
    doc->line_count = count;
    yaml_detect_eol(doc);
    return 0;
}

static void yaml_doc_free(yaml_doc_t *doc) {
    free(doc->lines);
    memset(doc, 0, sizeof(*doc));
}

/* Precompute the double-quote continuation flags (#1631): a line whose scan
 * ends inside a double-quoted scalar with a trailing `\` opens a continuation;
 * the following line is value text and may itself close, or re-open, the
 * scalar. Any OTHER way of ending a line inside a quote stays an error at the
 * validators. Returns YAML_ERROR only when the document ENDS inside an open
 * continuation (the quote can never close). */
static int yaml_mark_dquote_continuations(yaml_doc_t *doc) {
    bool cont = false;
    for (size_t i = 0; i < doc->line_count; i++) {
        yaml_line_t *line = &doc->lines[i];
        line->dquote_cont = cont;
        line->dquote_opens = false;
        if (!cont && (line->blank || line->comment)) {
            continue;
        }
        size_t start = cont ? line->start : line->start + line->indent;
        char quote = cont ? '"' : '\0';
        bool opens = false;
        char previous = '\0';
        size_t j = start;
        while (j < line->text_end) {
            char c = doc->data[j];
            if (quote == '"') {
                if (c == '\\') {
                    if (j + YAML_UNIT == line->text_end) {
                        opens = true;
                        break;
                    }
                    j += YAML_ENTRY_INDENT;
                    continue;
                }
                if (c == '"') {
                    quote = '\0';
                }
                j++;
                continue;
            }
            if (quote == '\'') {
                if (c == '\'') {
                    if (j + YAML_UNIT < line->text_end && doc->data[j + YAML_UNIT] == '\'') {
                        j += YAML_ENTRY_INDENT;
                    } else {
                        quote = '\0';
                        j++;
                    }
                    continue;
                }
                j++;
                continue;
            }
            if ((c == '"' || c == '\'') &&
                (previous == '\0' || previous == ':' || previous == '-')) {
                quote = c;
                j++;
                continue;
            }
            if (c == '#' && (j == start || doc->data[j - YAML_UNIT] == ' ')) {
                break;
            }
            if (c != ' ' && c != '\t') {
                previous = c;
            }
            j++;
        }
        line->dquote_opens = opens;
        cont = opens;
    }
    return cont ? YAML_ERROR : 0;
}

static int yaml_doc_init(yaml_doc_t *doc, const char *data, size_t len) {
    memset(doc, 0, sizeof(*doc));
    doc->data = data;
    doc->len = len;
    if (yaml_build_lines(doc) != 0) {
        return YAML_ERROR;
    }
    /* A UTF-8 BOM is a prologue, not content (#1656): Windows-authored
     * configs routinely carry one (PowerShell 5.1 Set-Content -Encoding UTF8
     * writes it), and treating its bytes as the first key made every edit op
     * fail content-independently. Skip it for structure; edits splice ranges
     * at line offsets, so the BOM survives every write byte-for-byte. */
    if (doc->line_count > 0U && doc->len >= YAML_DOC_MARKER_LEN &&
        (unsigned char)data[0] == 0xEFU && (unsigned char)data[1] == 0xBBU &&
        (unsigned char)data[2] == 0xBFU) {
        yaml_line_t *first = &doc->lines[0];
        if (first->start == 0U && !first->blank) {
            first->start = YAML_DOC_MARKER_LEN;
            if (first->start + first->indent >= first->text_end) {
                first->blank = true;
            }
        }
    }
    if (yaml_mark_dquote_continuations(doc) != 0) {
        yaml_doc_free(doc);
        return YAML_ERROR;
    }
    for (size_t i = 0; i < doc->line_count; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->indent == 0U && !line->blank && !line->comment && !line->dquote_cont) {
            size_t len_no_eol = line->text_end - line->start;
            if ((len_no_eol == YAML_DOC_MARKER_LEN &&
                 memcmp(doc->data + line->start, "---", YAML_DOC_MARKER_LEN) == 0) ||
                (len_no_eol == YAML_DOC_MARKER_LEN &&
                 memcmp(doc->data + line->start, "...", YAML_DOC_MARKER_LEN) == 0)) {
                yaml_doc_free(doc);
                return YAML_ERROR;
            }
        }
    }
    if (yaml_validate_root_mapping(doc) != 0) {
        yaml_doc_free(doc);
        return YAML_ERROR;
    }
    return 0;
}

static size_t yaml_skip_spaces(const char *data, size_t pos, size_t end) {
    while (pos < end && data[pos] == ' ') {
        pos++;
    }
    return pos;
}

static size_t yaml_trim_spaces_end(const char *data, size_t start, size_t end) {
    while (end > start && data[end - YAML_UNIT] == ' ') {
        end--;
    }
    return end;
}

static int yaml_line_matches_key(const yaml_doc_t *doc, const yaml_line_t *line, size_t indent,
                                 const char *key, size_t key_len, size_t *out_colon) {
    if (line->blank || line->comment || line->dquote_cont || line->indent != indent) {
        return 0;
    }
    size_t start = line->start + indent;
    size_t end = line->text_end;
    if (start >= end) {
        return 0;
    }
    /* A block-sequence item at this indent is a known non-key line (#1631):
     * YAML puts items at the same indent as their mapping key, so a column-0
     * `- item` is ordinary structure, not a malformed key. The root-mapping
     * walker already validated that items only follow a value-less key. */
    if (doc->data[start] == '-' &&
        (start + YAML_UNIT == end || doc->data[start + YAML_UNIT] == ' ')) {
        return 0;
    }
    size_t colon = 0U;
    if (yaml_find_mapping_colon(doc->data, start, end, &colon) != 0) {
        return YAML_ERROR;
    }
    size_t key_end = yaml_trim_spaces_end(doc->data, start, colon);
    if (key_end == start) {
        return YAML_ERROR;
    }

    if (doc->data[start] != '\'' && doc->data[start] != '"') {
        if (key_end - start != key_len || memcmp(doc->data + start, key, key_len) != 0) {
            return 0;
        }
        *out_colon = colon;
        return YAML_MATCH;
    }

    char quote = doc->data[start];
    bool canonical = key_end - start == key_len + YAML_QUOTED_MIN_LEN &&
                     doc->data[key_end - YAML_UNIT] == quote &&
                     memcmp(doc->data + start + YAML_UNIT, key, key_len) == 0;
    char *decoded = NULL;
    if (yaml_decode_scalar(doc->data, start, key_end, &decoded) != 0) {
        return YAML_ERROR;
    }
    bool semantic_match = strcmp(decoded, key) == 0;
    free(decoded);
    if (!semantic_match) {
        return 0;
    }
    if (!canonical) {
        return YAML_ERROR;
    }
    *out_colon = colon;
    return YAML_MATCH;
}

static int yaml_find_unique_key(const yaml_doc_t *doc, size_t indent, const char *key,
                                size_t key_len, bool *out_found, size_t *out_line,
                                size_t *out_colon) {
    size_t count = 0U;
    for (size_t i = 0; i < doc->line_count; i++) {
        size_t colon = 0U;
        int match = yaml_line_matches_key(doc, &doc->lines[i], indent, key, key_len, &colon);
        if (match < 0) {
            return YAML_ERROR;
        }
        if (match == YAML_MATCH) {
            count++;
            *out_line = i;
            *out_colon = colon;
        }
    }
    if (count > YAML_UNIT) {
        return YAML_ERROR;
    }
    *out_found = count == YAML_UNIT;
    return 0;
}

static size_t yaml_top_level_section_end(const yaml_doc_t *doc, size_t header_line) {
    for (size_t i = header_line + YAML_UNIT; i < doc->line_count; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (!line->blank && !line->comment && !line->dquote_cont && line->indent == 0U) {
            return i;
        }
    }
    return doc->line_count;
}

static int yaml_scan_quote(const char *data, size_t end, size_t *pos, char *quote, bool *is_plain) {
    char c = data[*pos];
    *is_plain = false;
    if (*quote == '\'') {
        if (c == '\'') {
            if (*pos + YAML_UNIT < end && data[*pos + YAML_UNIT] == '\'') {
                (*pos)++;
            } else {
                *quote = '\0';
            }
        }
        return 0;
    }
    if (*quote == '"') {
        if (c == '\\') {
            if (*pos + YAML_UNIT >= end) {
                return YAML_ERROR;
            }
            (*pos)++;
        } else if (c == '"') {
            *quote = '\0';
        }
        return 0;
    }
    if (c == '\'' || c == '"') {
        *quote = c;
        return 0;
    }
    *is_plain = true;
    return 0;
}

/* Positional variant (#1631): a quote character OPENS a scalar only where a
 * node begins — at the range start, after a mapping colon, or after a block-
 * sequence dash. Mid-word apostrophes (`LET'S`) and inch marks are ordinary
 * text, exactly like the `&`/`*` indicator rule above. Closing behaviour for
 * an already-open quote is unchanged. */
static int yaml_scan_quote_positional(const char *data, size_t end, size_t *pos, char *quote,
                                      bool *is_plain, char previous) {
    if (*quote == '\0') {
        char c = data[*pos];
        if ((c == '\'' || c == '"') && !(previous == '\0' || previous == ':' || previous == '-')) {
            *is_plain = true;
            return 0;
        }
    }
    return yaml_scan_quote(data, end, pos, quote, is_plain);
}

static int yaml_find_comment(const char *data, size_t start, size_t end, size_t *out_comment) {
    char quote = '\0';
    for (size_t i = start; i < end; i++) {
        bool is_plain = false;
        if (yaml_scan_quote(data, end, &i, &quote, &is_plain) != 0) {
            return YAML_ERROR;
        }
        if (!is_plain) {
            continue;
        }
        if (data[i] == '#' && (i == start || data[i - YAML_UNIT] == ' ')) {
            *out_comment = i;
            return 0;
        }
    }
    if (quote != '\0') {
        return YAML_ERROR;
    }
    *out_comment = end;
    return 0;
}

/* `&` and `*` are anchor/alias indicators only where a NODE begins. Once a
 * plain scalar has started, they are ordinary characters.
 *
 * This used to reject them anywhere in the range, so a value containing prose
 * asterisks — `use *emphasis* here`, a kaomoji, a glob in a description — was
 * refused as if it were an alias. Reported against a real 16 KB Hermes config
 * where this was one of four constructs that made the file permanently
 * un-editable by us (#1631, root-caused by @rg6304 with an isolated repro).
 *
 * The test is positional and deliberately conservative: an indicator is only
 * honoured as one when nothing has appeared before it in the value. `key: *a`
 * is still an alias and still refused; `key: 2 * 3` and `key: a*b` are text.
 * `{`/`}` keep their existing treatment here; mapping-body validation admits
 * exact empty flow mappings through a context-specific exception.
 *
 * allow_open_dquote: the caller established (via the doc-level continuation
 * flags, #1631) that this range legitimately ends inside a double-quoted
 * scalar continued on the next line — an open `"` at range end is then not an
 * error. Every other caller keeps the fail-closed unterminated-quote check. */
static int yaml_range_has_unsupported_ex(const char *data, size_t start, size_t end,
                                         bool allow_open_dquote);

static int yaml_range_has_unsupported(const char *data, size_t start, size_t end) {
    return yaml_range_has_unsupported_ex(data, start, end, false);
}

static int yaml_range_has_unsupported_ex(const char *data, size_t start, size_t end,
                                         bool allow_open_dquote) {
    char quote = '\0';
    /* Last significant character seen, so an indicator can be judged by what
     * PRECEDES it. The scanned range covers a whole line including its key, so
     * "first character in the range" is not the same as "start of a node" —
     * in `command: &shared` the `&` is an anchor even though `command:` came
     * first. A node begins at the range start, after a mapping colon, or after
     * a block-sequence dash. */
    char previous = '\0';
    /* A continuation range ends with the escaping backslash itself; keep it
     * out of the scan so yaml_scan_quote never sees a dangling escape. */
    if (allow_open_dquote && end > start && data[end - YAML_UNIT] == '\\') {
        end--;
    }
    for (size_t i = start; i < end; i++) {
        bool is_plain = false;
        if (yaml_scan_quote_positional(data, end, &i, &quote, &is_plain, previous) != 0) {
            return YAML_MATCH;
        }
        if (!is_plain) {
            previous = 'q';
            continue;
        }
        char c = data[i];
        if (c == '#' && (i == start || data[i - YAML_UNIT] == ' ')) {
            break;
        }
        if (c == '{' || c == '}') {
            return YAML_MATCH;
        }
        if (c == '&' || c == '*') {
            bool begins_node = previous == '\0' || previous == ':' || previous == '-';
            if (begins_node) {
                return YAML_MATCH;
            }
        }
        if (c != ' ' && c != '\t') {
            previous = c;
        }
        if (c == '<' && i + YAML_ENTRY_INDENT < end && data[i + YAML_UNIT] == '<' &&
            data[i + YAML_ENTRY_INDENT] == ':') {
            return YAML_MATCH;
        }
    }
    if (quote == '"' && allow_open_dquote) {
        return 0;
    }
    return quote != '\0';
}

static int yaml_find_mapping_colon(const char *data, size_t start, size_t end, size_t *out_colon) {
    char quote = '\0';
    for (size_t i = start; i < end; i++) {
        bool is_plain = false;
        if (yaml_scan_quote(data, end, &i, &quote, &is_plain) != 0) {
            return YAML_ERROR;
        }
        if (!is_plain) {
            continue;
        }
        if (data[i] == ':' &&
            (i + YAML_UNIT == end || data[i + YAML_UNIT] == ' ' || data[i + YAML_UNIT] == '#')) {
            if (i == start) {
                return YAML_ERROR;
            }
            *out_colon = i;
            return 0;
        }
    }
    return YAML_ERROR;
}

static int yaml_validate_root_mapping(const yaml_doc_t *doc) {
    bool have_top_level_entry = false;
    /* YAML allows a block sequence at the same indent as its mapping key
     * (#1631): `agents:` followed by `- alpha` at column 0. Item lines are
     * only legal directly after a value-less top-level key or another item. */
    bool in_column_zero_sequence = false;
    for (size_t i = 0U; i < doc->line_count; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->blank || line->comment || line->dquote_cont) {
            continue;
        }
        if (line->indent != 0U) {
            if (!have_top_level_entry) {
                return YAML_ERROR;
            }
            continue;
        }

        size_t start = line->start;
        bool is_item = doc->data[start] == '-' &&
                       (start + YAML_UNIT == line->text_end || doc->data[start + YAML_UNIT] == ' ');
        if (is_item) {
            if (!in_column_zero_sequence) {
                return YAML_ERROR;
            }
            continue;
        }
        if (doc->data[start] == '%' || doc->data[start] == '?' || doc->data[start] == ':') {
            return YAML_ERROR;
        }
        size_t colon = 0U;
        if (yaml_find_mapping_colon(doc->data, start, line->text_end, &colon) != 0) {
            return YAML_ERROR;
        }
        size_t key_end = yaml_trim_spaces_end(doc->data, start, colon);
        char *decoded_key = NULL;
        if (yaml_decode_scalar(doc->data, start, key_end, &decoded_key) != 0) {
            return YAML_ERROR;
        }
        bool merge_key = strcmp(decoded_key, "<<") == 0;
        free(decoded_key);
        if (merge_key) {
            return YAML_ERROR;
        }
        have_top_level_entry = true;
        size_t tail = yaml_skip_spaces(doc->data, colon + YAML_UNIT, line->text_end);
        in_column_zero_sequence = tail == line->text_end || doc->data[tail] == '#';
    }
    return 0;
}

static int yaml_tail_is_empty(const char *data, size_t colon, size_t end) {
    size_t pos = yaml_skip_spaces(data, colon + YAML_UNIT, end);
    return pos == end || data[pos] == '#';
}

static int yaml_tail_is_explicit_empty_mapping(const char *data, size_t colon, size_t end,
                                               bool *out_empty) {
    size_t comment = 0U;
    if (yaml_find_comment(data, colon + YAML_UNIT, end, &comment) != 0) {
        return YAML_ERROR;
    }
    size_t value_start = yaml_skip_spaces(data, colon + YAML_UNIT, comment);
    size_t value_end = yaml_trim_spaces_end(data, value_start, comment);
    *out_empty = value_end - value_start == YAML_QUOTED_MIN_LEN && data[value_start] == '{' &&
                 data[value_start + YAML_UNIT] == '}';
    return 0;
}

/* Exact empty flow collection (`{}` or `[]`) as the whole value, optionally
 * followed by a comment. Used ONLY by the document VALIDATORS (#1631/#1673):
 * an empty flow collection is preserved opaque user content. Section-header
 * semantics keep the mapping-only helper above — `section: []` is a sequence,
 * never an empty mapping section. */
static int yaml_tail_is_explicit_empty_flow(const char *data, size_t colon, size_t end,
                                            bool *out_empty) {
    size_t comment = 0U;
    if (yaml_find_comment(data, colon + YAML_UNIT, end, &comment) != 0) {
        return YAML_ERROR;
    }
    size_t value_start = yaml_skip_spaces(data, colon + YAML_UNIT, comment);
    size_t value_end = yaml_trim_spaces_end(data, value_start, comment);
    *out_empty = value_end - value_start == YAML_QUOTED_MIN_LEN &&
                 ((data[value_start] == '{' && data[value_start + YAML_UNIT] == '}') ||
                  (data[value_start] == '[' && data[value_start + YAML_UNIT] == ']'));
    return 0;
}

static int yaml_mapping_body_line_has_unsupported(const yaml_doc_t *doc, const yaml_line_t *line) {
    size_t start = line->start + line->indent;
    size_t colon = 0U;
    bool empty_flow = false;
    if (yaml_find_mapping_colon(doc->data, start, line->text_end, &colon) == 0 &&
        yaml_tail_is_explicit_empty_flow(doc->data, colon, line->text_end, &empty_flow) == 0 &&
        empty_flow) {
        return yaml_range_has_unsupported(doc->data, start, colon + YAML_UNIT);
    }
    return yaml_range_has_unsupported_ex(doc->data, start, line->text_end, line->dquote_opens);
}

static int yaml_value_starts_multiline(const char *data, size_t colon, size_t end) {
    size_t pos = yaml_skip_spaces(data, colon + YAML_UNIT, end);
    return pos < end && (data[pos] == '|' || data[pos] == '>');
}

static int yaml_validate_mapping_body(const yaml_doc_t *doc, size_t first_line, size_t end_line,
                                      const char *entry_key, size_t entry_len,
                                      yaml_mapping_target_t *target) {
    size_t target_count = 0U;
    size_t mapping_count = 0U;
    bool have_entry = false;
    for (size_t i = first_line; i < end_line; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->blank || line->comment || line->dquote_cont) {
            continue;
        }
        if (line->indent < YAML_ENTRY_INDENT || (line->indent & YAML_UNIT) != 0U ||
            yaml_mapping_body_line_has_unsupported(doc, line)) {
            return YAML_ERROR;
        }
        size_t colon = 0U;
        if (line->indent == YAML_ENTRY_INDENT) {
            if (yaml_find_mapping_colon(doc->data, line->start + YAML_ENTRY_INDENT, line->text_end,
                                        &colon) != 0 ||
                yaml_value_starts_multiline(doc->data, colon, line->text_end)) {
                return YAML_ERROR;
            }
            have_entry = true;
            mapping_count++;
            size_t target_colon = 0U;
            int match = yaml_line_matches_key(doc, line, YAML_ENTRY_INDENT, entry_key, entry_len,
                                              &target_colon);
            if (match < 0) {
                return YAML_ERROR;
            }
            if (match == YAML_MATCH) {
                target_count++;
                target->entry_line = i;
                if (!yaml_tail_is_empty(doc->data, target_colon, line->text_end)) {
                    return YAML_ERROR;
                }
            }
            continue;
        }
        if (!have_entry || (yaml_find_mapping_colon(doc->data, line->start + line->indent,
                                                    line->text_end, &colon) == 0 &&
                            yaml_value_starts_multiline(doc->data, colon, line->text_end))) {
            return YAML_ERROR;
        }
    }
    if (target_count > YAML_UNIT) {
        return YAML_ERROR;
    }
    target->entry_found = target_count == YAML_UNIT;
    target->entry_count = mapping_count;
    return 0;
}

static size_t yaml_mapping_entry_end(const yaml_doc_t *doc, size_t entry_line, size_t end_line,
                                     size_t section_end) {
    size_t pending_boundary = SIZE_MAX;
    for (size_t i = entry_line + YAML_UNIT; i < end_line; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->blank || (line->comment && line->indent <= YAML_ENTRY_INDENT)) {
            if (pending_boundary == SIZE_MAX) {
                pending_boundary = line->start;
            }
            continue;
        }
        if (line->indent <= YAML_ENTRY_INDENT) {
            return pending_boundary == SIZE_MAX ? line->start : pending_boundary;
        }
        pending_boundary = SIZE_MAX;
    }
    return pending_boundary == SIZE_MAX ? section_end : pending_boundary;
}

static int yaml_analyze_mapping(const yaml_doc_t *doc, const char *section_key, size_t section_len,
                                const char *entry_key, size_t entry_len,
                                yaml_mapping_target_t *target) {
    memset(target, 0, sizeof(*target));
    size_t section_colon = 0U;
    if (yaml_find_unique_key(doc, 0U, section_key, section_len, &target->section_found,
                             &target->section_line, &section_colon) != 0) {
        return YAML_ERROR;
    }
    if (!target->section_found) {
        return 0;
    }
    target->section_colon = section_colon;

    const yaml_line_t *section = &doc->lines[target->section_line];
    if (yaml_tail_is_explicit_empty_mapping(doc->data, section_colon, section->text_end,
                                            &target->section_inline_empty) != 0) {
        return YAML_ERROR;
    }
    size_t end_line = yaml_top_level_section_end(doc, target->section_line);
    target->section_end = end_line < doc->line_count ? doc->lines[end_line].start : doc->len;
    if (target->section_inline_empty) {
        for (size_t i = target->section_line + YAML_UNIT; i < end_line; i++) {
            if (!doc->lines[i].blank && !doc->lines[i].comment) {
                return YAML_ERROR;
            }
        }
        return 0;
    }
    if (yaml_range_has_unsupported(doc->data, section_colon + YAML_UNIT, section->text_end) ||
        !yaml_tail_is_empty(doc->data, section_colon, section->text_end)) {
        return YAML_ERROR;
    }

    if (yaml_validate_mapping_body(doc, target->section_line + YAML_UNIT, end_line, entry_key,
                                   entry_len, target) != 0) {
        return YAML_ERROR;
    }
    if (!target->entry_found) {
        return 0;
    }
    target->entry_end =
        yaml_mapping_entry_end(doc, target->entry_line, end_line, target->section_end);
    return 0;
}

static int yaml_validate_entry_block_line(const char *block, size_t start, size_t text_end) {
    size_t indent = start;
    while (indent < text_end && block[indent] == ' ') {
        indent++;
    }
    if (indent == text_end) {
        return 0;
    }
    size_t width = indent - start;
    if (width < YAML_VALUE_INDENT || (width & YAML_UNIT) != 0U) {
        return YAML_ERROR;
    }
    if (block[indent] == '#') {
        return 0;
    }
    size_t comment = 0U;
    if (yaml_find_comment(block, indent, text_end, &comment) != 0 || comment < text_end) {
        return YAML_ERROR;
    }
    if (yaml_range_has_unsupported(block, indent, text_end)) {
        return YAML_ERROR;
    }
    size_t colon = 0U;
    if (yaml_find_mapping_colon(block, indent, text_end, &colon) == 0 &&
        yaml_value_starts_multiline(block, colon, text_end)) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_validate_entry_block(const char *block, size_t *out_len) {
    size_t len = 0U;
    if (yaml_bounded_strlen(block, YAML_BLOCK_MAX, &len) != 0 ||
        yaml_validate_text_bytes(block, len) != 0) {
        return YAML_ERROR;
    }
    size_t pos = 0U;
    while (pos < len) {
        size_t end = pos;
        while (end < len && block[end] != '\n') {
            end++;
        }
        size_t text_end = end;
        if (text_end > pos && block[text_end - YAML_UNIT] == '\r') {
            text_end--;
        }
        if (yaml_validate_entry_block_line(block, pos, text_end) != 0) {
            return YAML_ERROR;
        }
        pos = end < len ? end + YAML_UNIT : end;
    }
    *out_len = len;
    return 0;
}

static int yaml_append_normalized_block(yaml_buf_t *buf, const char *block, size_t block_len,
                                        const yaml_doc_t *doc) {
    size_t pos = 0U;
    while (pos < block_len) {
        size_t end = pos;
        while (end < block_len && block[end] != '\n') {
            end++;
        }
        size_t text_end = end;
        if (text_end > pos && block[text_end - YAML_UNIT] == '\r') {
            text_end--;
        }
        if (yaml_buf_append(buf, block + pos, text_end - pos) != 0 ||
            yaml_buf_append(buf, doc->eol, doc->eol_len) != 0) {
            return YAML_ERROR;
        }
        pos = end < block_len ? end + YAML_UNIT : end;
    }
    return 0;
}

static int yaml_build_mapping_entry(yaml_buf_t *buf, const yaml_doc_t *doc,
                                    const yaml_line_t *existing_header, const char *entry_key,
                                    size_t entry_len, const char *block, size_t block_len) {
    if (existing_header) {
        if (yaml_buf_append(buf, doc->data + existing_header->start,
                            existing_header->end - existing_header->start) != 0) {
            return YAML_ERROR;
        }
        if (existing_header->end == existing_header->text_end &&
            yaml_buf_append(buf, doc->eol, doc->eol_len) != 0) {
            return YAML_ERROR;
        }
    } else {
        if (yaml_buf_append(buf, "  ", YAML_LITERAL_LEN("  ")) != 0 ||
            yaml_buf_append(buf, entry_key, entry_len) != 0 ||
            yaml_buf_append_char(buf, ':') != 0 ||
            yaml_buf_append(buf, doc->eol, doc->eol_len) != 0) {
            return YAML_ERROR;
        }
    }
    return yaml_append_normalized_block(buf, block, block_len, doc);
}

static int yaml_splice(yaml_buf_t *out, const yaml_doc_t *doc, size_t start, size_t end,
                       const char *replacement, size_t replacement_len) {
    if (start > end || end > doc->len || yaml_buf_append(out, doc->data, start) != 0 ||
        yaml_buf_append(out, replacement, replacement_len) != 0 ||
        yaml_buf_append(out, doc->data + end, doc->len - end) != 0) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_append_separator_if_needed(yaml_buf_t *buf, const yaml_doc_t *doc, size_t offset) {
    if (offset > 0U && doc->data[offset - YAML_UNIT] != '\n') {
        return yaml_buf_append(buf, doc->eol, doc->eol_len);
    }
    return 0;
}

static int yaml_hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + YAML_HEX_ALPHA_OFFSET;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + YAML_HEX_ALPHA_OFFSET;
    }
    return YAML_ERROR;
}

static int yaml_decode_single_quoted(const char *data, size_t start, size_t end,
                                     yaml_buf_t *decoded) {
    if (end - start < YAML_QUOTED_MIN_LEN || data[end - YAML_UNIT] != '\'') {
        return YAML_ERROR;
    }
    for (size_t i = start + YAML_UNIT; i < end - YAML_UNIT; i++) {
        if (data[i] == '\'') {
            if (i + YAML_UNIT >= end - YAML_UNIT || data[i + YAML_UNIT] != '\'') {
                return YAML_ERROR;
            }
            i++;
        }
        if (yaml_buf_append_char(decoded, data[i]) != 0) {
            return YAML_ERROR;
        }
    }
    return 0;
}

static int yaml_decode_hex_escape(const char *data, size_t end, size_t *pos, char *out_value) {
    if (*pos + YAML_HEX_DIGIT_COUNT >= end - YAML_UNIT) {
        return YAML_ERROR;
    }
    int high = yaml_hex_value(data[*pos + YAML_UNIT]);
    int low = yaml_hex_value(data[*pos + YAML_HEX_DIGIT_COUNT]);
    if (high < 0 || low < 0) {
        return YAML_ERROR;
    }
    *out_value = (char)((high << YAML_NIBBLE_SHIFT) | low);
    *pos += YAML_HEX_DIGIT_COUNT;
    return 0;
}

static int yaml_decode_escape(const char *data, size_t end, size_t *pos, char *out_value) {
    char escaped = data[*pos];
    switch (escaped) {
    case '0':
        *out_value = '\0';
        return 0;
    case 'a':
        *out_value = '\a';
        return 0;
    case 'b':
        *out_value = '\b';
        return 0;
    case 't':
        *out_value = '\t';
        return 0;
    case 'n':
        *out_value = '\n';
        return 0;
    case 'v':
        *out_value = '\v';
        return 0;
    case 'f':
        *out_value = '\f';
        return 0;
    case 'r':
        *out_value = '\r';
        return 0;
    case 'e':
        *out_value = YAML_ESCAPE_BYTE;
        return 0;
    case ' ':
        *out_value = ' ';
        return 0;
    case '/':
        *out_value = '/';
        return 0;
    case '"':
        *out_value = '"';
        return 0;
    case '\\':
        *out_value = '\\';
        return 0;
    case 'x':
        return yaml_decode_hex_escape(data, end, pos, out_value);
    default:
        return YAML_ERROR;
    }
}

static int yaml_decode_double_quoted(const char *data, size_t start, size_t end,
                                     yaml_buf_t *decoded) {
    if (end - start < YAML_QUOTED_MIN_LEN || data[end - YAML_UNIT] != '"') {
        return YAML_ERROR;
    }
    for (size_t i = start + YAML_UNIT; i < end - YAML_UNIT; i++) {
        char value = data[i];
        if (value == '"') {
            return YAML_ERROR;
        }
        if (value == '\\') {
            i++;
            if (i >= end - YAML_UNIT || yaml_decode_escape(data, end, &i, &value) != 0) {
                return YAML_ERROR;
            }
        }
        if ((unsigned char)value < YAML_CONTROL_LIMIT || value == YAML_DELETE_BYTE ||
            yaml_buf_append_char(decoded, value) != 0) {
            return YAML_ERROR;
        }
    }
    return 0;
}

static int yaml_decode_plain(const char *data, size_t start, size_t end, yaml_buf_t *decoded) {
    const char *indicators = "-?:,[]{}#&*!|>@`";
    if (strchr(indicators, data[start]) != NULL) {
        return YAML_ERROR;
    }
    for (size_t i = start; i < end; i++) {
        char c = data[i];
        if ((c == ':' && i + YAML_UNIT < end && data[i + YAML_UNIT] == ' ') ||
            (c == '#' && i > start && data[i - YAML_UNIT] == ' ')) {
            return YAML_ERROR;
        }
    }
    return yaml_buf_append(decoded, data + start, end - start);
}

static int yaml_decode_scalar(const char *data, size_t start, size_t end, char **out_value) {
    start = yaml_skip_spaces(data, start, end);
    end = yaml_trim_spaces_end(data, start, end);
    if (start == end || end - start > YAML_ITEM_MAX) {
        return YAML_ERROR;
    }

    yaml_buf_t decoded = {0};
    int rc = 0;
    if (data[start] == '\'') {
        rc = yaml_decode_single_quoted(data, start, end, &decoded);
    } else if (data[start] == '"') {
        rc = yaml_decode_double_quoted(data, start, end, &decoded);
    } else {
        rc = yaml_decode_plain(data, start, end, &decoded);
    }

    if (rc != 0 || decoded.len == 0U || yaml_validate_utf8(decoded.data, decoded.len) != 0) {
        yaml_buf_free(&decoded);
        return YAML_ERROR;
    }
    *out_value = decoded.data;
    return 0;
}

static void yaml_item_vec_free(yaml_item_vec_t *items) {
    for (size_t i = 0; i < items->len; i++) {
        free(items->items[i].value);
    }
    free(items->items);
    memset(items, 0, sizeof(*items));
}

static int yaml_item_vec_push(yaml_item_vec_t *items, const yaml_item_t *item) {
    if (items->len == items->cap) {
        size_t next = items->cap ? items->cap * YAML_GROWTH_FACTOR : YAML_ITEM_INITIAL_CAPACITY;
        if (next < items->cap || next > YAML_INPUT_MAX / sizeof(*items->items)) {
            return YAML_ERROR;
        }
        yaml_item_t *grown = (yaml_item_t *)realloc(items->items, next * sizeof(*items->items));
        if (!grown) {
            return YAML_ERROR;
        }
        items->items = grown;
        items->cap = next;
    }
    items->items[items->len++] = *item;
    return 0;
}

static int yaml_add_item_range(yaml_item_vec_t *items, const char *data, size_t start, size_t end,
                               size_t line_start, size_t line_end) {
    start = yaml_skip_spaces(data, start, end);
    end = yaml_trim_spaces_end(data, start, end);
    yaml_item_t item = {
        .raw_start = start,
        .raw_end = end,
        .line_start = line_start,
        .line_end = line_end,
        .value = NULL,
    };
    if (yaml_decode_scalar(data, start, end, &item.value) != 0) {
        return YAML_ERROR;
    }
    if (yaml_item_vec_push(items, &item) != 0) {
        free(item.value);
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_finish_flow_items(const yaml_doc_t *doc, size_t segment, size_t close_pos,
                                  const yaml_line_t *line, yaml_item_vec_t *items,
                                  bool saw_content) {
    size_t trimmed_start = segment;
    while (trimmed_start < close_pos && doc->data[trimmed_start] == ' ') {
        trimmed_start++;
    }
    if (trimmed_start == close_pos) {
        return items->len == 0U && !saw_content ? 0 : YAML_ERROR;
    }
    return yaml_add_item_range(items, doc->data, segment, close_pos, line->start, line->end);
}

static int yaml_parse_flow_items(const yaml_doc_t *doc, size_t open_pos, size_t close_pos,
                                 const yaml_line_t *line, yaml_item_vec_t *items) {
    size_t segment = open_pos + YAML_UNIT;
    char quote = '\0';
    bool saw_content = false;
    for (size_t i = segment; i < close_pos; i++) {
        char quote_before = quote;
        bool is_plain = false;
        char c = doc->data[i];
        if (yaml_scan_quote(doc->data, close_pos, &i, &quote, &is_plain) != 0) {
            return YAML_ERROR;
        }
        if (!is_plain) {
            if (quote_before == '\0' && quote != '\0') {
                saw_content = true;
            }
            continue;
        }
        if (c == '[' || c == ']') {
            return YAML_ERROR;
        }
        if (c == ',') {
            if (yaml_add_item_range(items, doc->data, segment, i, line->start, line->end) != 0) {
                return YAML_ERROR;
            }
            segment = i + YAML_UNIT;
            saw_content = false;
            continue;
        }
        if (c != ' ') {
            saw_content = true;
        }
    }
    if (quote != '\0') {
        return YAML_ERROR;
    }
    return yaml_finish_flow_items(doc, segment, close_pos, line, items, saw_content);
}

static int yaml_parse_block_list(const yaml_doc_t *doc, yaml_list_target_t *target) {
    size_t end_line = yaml_top_level_section_end(doc, target->key_line);
    target->section_end = end_line < doc->line_count ? doc->lines[end_line].start : doc->len;
    for (size_t i = target->key_line + YAML_UNIT; i < end_line; i++) {
        const yaml_line_t *child = &doc->lines[i];
        if (child->blank || child->comment) {
            continue;
        }
        if (child->indent != YAML_ENTRY_INDENT) {
            return YAML_ERROR;
        }
        size_t dash = child->start + YAML_ENTRY_INDENT;
        if (dash >= child->text_end || doc->data[dash] != '-' ||
            dash + YAML_UNIT >= child->text_end || doc->data[dash + YAML_UNIT] != ' ') {
            return YAML_ERROR;
        }
        size_t item_start = yaml_skip_spaces(doc->data, dash + YAML_ENTRY_INDENT, child->text_end);
        size_t item_comment = 0U;
        if (yaml_find_comment(doc->data, item_start, child->text_end, &item_comment) != 0 ||
            yaml_range_has_unsupported(doc->data, item_start, item_comment) ||
            yaml_add_item_range(&target->items, doc->data, item_start, item_comment, child->start,
                                child->end) != 0) {
            return YAML_ERROR;
        }
    }
    return 0;
}

static int yaml_analyze_list(const yaml_doc_t *doc, const char *key, size_t key_len,
                             yaml_list_target_t *target) {
    memset(target, 0, sizeof(*target));
    size_t colon = 0U;
    bool key_found = false;
    if (yaml_find_unique_key(doc, 0U, key, key_len, &key_found, &target->key_line, &colon) != 0) {
        return YAML_ERROR;
    }
    if (!key_found) {
        target->kind = YAML_LIST_ABSENT;
        return 0;
    }

    const yaml_line_t *line = &doc->lines[target->key_line];
    if (yaml_range_has_unsupported(doc->data, colon + YAML_UNIT, line->text_end)) {
        return YAML_ERROR;
    }
    size_t comment = 0U;
    if (yaml_find_comment(doc->data, colon + YAML_UNIT, line->text_end, &comment) != 0) {
        return YAML_ERROR;
    }
    size_t value_start = yaml_skip_spaces(doc->data, colon + YAML_UNIT, comment);
    size_t value_end = yaml_trim_spaces_end(doc->data, value_start, comment);
    target->value_start = value_start;
    target->value_end = value_end;

    if (value_start == value_end) {
        target->kind = YAML_LIST_BLOCK;
        return yaml_parse_block_list(doc, target);
    }

    if (doc->data[value_start] == '|' || doc->data[value_start] == '>') {
        return YAML_ERROR;
    }
    if (doc->data[value_start] == '[') {
        if (value_end - value_start < YAML_QUOTED_MIN_LEN ||
            doc->data[value_end - YAML_UNIT] != ']') {
            return YAML_ERROR;
        }
        target->kind = YAML_LIST_FLOW;
        return yaml_parse_flow_items(doc, value_start, value_end - YAML_UNIT, line, &target->items);
    }
    target->kind = YAML_LIST_SCALAR;
    return yaml_add_item_range(&target->items, doc->data, value_start, value_end, line->start,
                               line->end);
}

static size_t yaml_item_match_count(const yaml_item_vec_t *items, const char *item) {
    size_t count = 0U;
    for (size_t i = 0; i < items->len; i++) {
        if (strcmp(items->items[i].value, item) == 0) {
            count++;
        }
    }
    return count;
}

static int yaml_validate_utf8(const char *value, size_t len) {
    const unsigned char *bytes = (const unsigned char *)value;
    for (size_t i = 0U; i < len;) {
        unsigned char first = bytes[i];
        if (first < 0x80U) {
            i++;
            continue;
        }
        if (first >= 0xC2U && first <= 0xDFU) {
            if (i + YAML_UNIT >= len || (bytes[i + YAML_UNIT] & 0xC0U) != 0x80U) {
                return YAML_ERROR;
            }
            i += YAML_ENTRY_INDENT;
            continue;
        }
        if (first >= 0xE0U && first <= 0xEFU) {
            if (i + YAML_ENTRY_INDENT >= len || (bytes[i + YAML_UNIT] & 0xC0U) != 0x80U ||
                (bytes[i + YAML_ENTRY_INDENT] & 0xC0U) != 0x80U ||
                (first == 0xE0U && bytes[i + YAML_UNIT] < 0xA0U) ||
                (first == 0xEDU && bytes[i + YAML_UNIT] >= 0xA0U)) {
                return YAML_ERROR;
            }
            i += YAML_DOC_MARKER_LEN;
            continue;
        }
        if (first >= 0xF0U && first <= 0xF4U) {
            if (i + YAML_DOC_MARKER_LEN >= len || (bytes[i + YAML_UNIT] & 0xC0U) != 0x80U ||
                (bytes[i + YAML_ENTRY_INDENT] & 0xC0U) != 0x80U ||
                (bytes[i + YAML_DOC_MARKER_LEN] & 0xC0U) != 0x80U ||
                (first == 0xF0U && bytes[i + YAML_UNIT] < 0x90U) ||
                (first == 0xF4U && bytes[i + YAML_UNIT] >= 0x90U)) {
                return YAML_ERROR;
            }
            i += YAML_VALUE_INDENT;
            continue;
        }
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_validate_scalar_value(const char *value, size_t *out_len) {
    size_t len = 0U;
    if (yaml_bounded_strlen(value, YAML_ITEM_MAX, &len) != 0 || len == 0U) {
        return YAML_ERROR;
    }
    for (size_t i = 0U; i < len; i++) {
        unsigned char byte = (unsigned char)value[i];
        if (byte < YAML_CONTROL_LIMIT || byte == YAML_DELETE_BYTE) {
            return YAML_ERROR;
        }
    }
    if (yaml_validate_utf8(value, len) != 0) {
        return YAML_ERROR;
    }
    *out_len = len;
    return 0;
}

static int yaml_append_quoted(yaml_buf_t *buf, const char *item, size_t item_len) {
    if (yaml_buf_append_char(buf, '"') != 0) {
        return YAML_ERROR;
    }
    for (size_t i = 0; i < item_len; i++) {
        if ((item[i] == '"' || item[i] == '\\') && yaml_buf_append_char(buf, '\\') != 0) {
            return YAML_ERROR;
        }
        if (yaml_buf_append_char(buf, item[i]) != 0) {
            return YAML_ERROR;
        }
    }
    return yaml_buf_append_char(buf, '"');
}

int cbm_yaml_encode_double_quoted_scalar(const char *value, char **encoded_out) {
    if (!encoded_out) {
        return YAML_ERROR;
    }
    *encoded_out = NULL;
    size_t len = 0U;
    if (yaml_validate_scalar_value(value, &len) != 0) {
        return YAML_ERROR;
    }
    yaml_buf_t encoded = {0};
    if (yaml_append_quoted(&encoded, value, len) != 0) {
        yaml_buf_free(&encoded);
        return YAML_ERROR;
    }
    *encoded_out = encoded.data;
    return 0;
}

static int yaml_validate_item(const char *item, size_t *out_len) {
    return yaml_validate_scalar_value(item, out_len);
}

static int yaml_build_flow_replacement(yaml_buf_t *replacement, const yaml_doc_t *doc,
                                       const yaml_list_target_t *target, const char *add_item,
                                       size_t add_len, const char *remove_item) {
    if (target->key_line >= doc->line_count ||
        (target->kind == YAML_LIST_SCALAR && target->items.len == 0U)) {
        return YAML_ERROR;
    }
    const yaml_line_t *line = &doc->lines[target->key_line];
    if (yaml_buf_append(replacement, doc->data + line->start, target->value_start - line->start) !=
            0 ||
        yaml_buf_append_char(replacement, '[') != 0) {
        return YAML_ERROR;
    }

    size_t emitted = 0U;
    for (size_t i = 0; i < target->items.len; i++) {
        const yaml_item_t *item = &target->items.items[i];
        if (remove_item && strcmp(item->value, remove_item) == 0) {
            continue;
        }
        if (emitted++ > 0U && yaml_buf_append(replacement, ", ", YAML_LITERAL_LEN(", ")) != 0) {
            return YAML_ERROR;
        }
        if (yaml_buf_append(replacement, doc->data + item->raw_start,
                            item->raw_end - item->raw_start) != 0) {
            return YAML_ERROR;
        }
    }
    if (add_item) {
        if (emitted > 0U && yaml_buf_append(replacement, ", ", YAML_LITERAL_LEN(", ")) != 0) {
            return YAML_ERROR;
        }
        if (yaml_append_quoted(replacement, add_item, add_len) != 0) {
            return YAML_ERROR;
        }
    }
    if (yaml_buf_append_char(replacement, ']') != 0) {
        return YAML_ERROR;
    }

    size_t suffix =
        target->kind == YAML_LIST_FLOW ? target->value_end : target->items.items[0].raw_end;
    return yaml_buf_append(replacement, doc->data + suffix, line->end - suffix);
}

static int yaml_remove_block_items(yaml_buf_t *out, const yaml_doc_t *doc,
                                   const yaml_list_target_t *target, const char *remove_item,
                                   size_t match_count) {
    size_t remaining = target->items.len - match_count;
    size_t item_index = 0U;
    for (size_t i = 0; i < doc->line_count; i++) {
        const yaml_line_t *line = &doc->lines[i];
        bool skip = remaining == 0U && i == target->key_line;
        while (item_index < target->items.len &&
               target->items.items[item_index].line_start < line->start) {
            item_index++;
        }
        if (item_index < target->items.len &&
            target->items.items[item_index].line_start == line->start &&
            strcmp(target->items.items[item_index].value, remove_item) == 0) {
            skip = true;
        }
        if (!skip && yaml_buf_append(out, doc->data + line->start, line->end - line->start) != 0) {
            return YAML_ERROR;
        }
    }
    return 0;
}

static int yaml_append_list_item_line(yaml_buf_t *out, const yaml_doc_t *doc, const char *item,
                                      size_t item_len) {
    if (yaml_buf_append(out, "  - ", YAML_LITERAL_LEN("  - ")) != 0 ||
        yaml_append_quoted(out, item, item_len) != 0 ||
        yaml_buf_append(out, doc->eol, doc->eol_len) != 0) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_build_absent_list(yaml_buf_t *out, const yaml_doc_t *doc, const char *key,
                                  size_t key_len, const char *item, size_t item_len) {
    if (yaml_buf_append(out, doc->data, doc->len) != 0 ||
        yaml_append_separator_if_needed(out, doc, doc->len) != 0 ||
        yaml_buf_append(out, key, key_len) != 0 || yaml_buf_append_char(out, ':') != 0 ||
        yaml_buf_append(out, doc->eol, doc->eol_len) != 0 ||
        yaml_append_list_item_line(out, doc, item, item_len) != 0) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_build_block_list_add(yaml_buf_t *out, const yaml_doc_t *doc,
                                     const yaml_list_target_t *target, const char *item,
                                     size_t item_len) {
    if (yaml_buf_append(out, doc->data, target->section_end) != 0 ||
        yaml_append_separator_if_needed(out, doc, target->section_end) != 0 ||
        yaml_append_list_item_line(out, doc, item, item_len) != 0 ||
        yaml_buf_append(out, doc->data + target->section_end, doc->len - target->section_end) !=
            0) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_build_inline_list_add(yaml_buf_t *out, const yaml_doc_t *doc,
                                      const yaml_list_target_t *target, const char *item,
                                      size_t item_len) {
    if (target->key_line >= doc->line_count) {
        return YAML_ERROR;
    }
    yaml_buf_t replacement = {0};
    int rc = yaml_build_flow_replacement(&replacement, doc, target, item, item_len, NULL);
    if (rc == 0) {
        const yaml_line_t *line = &doc->lines[target->key_line];
        rc = yaml_splice(out, doc, line->start, line->end, replacement.data, replacement.len);
    }
    yaml_buf_free(&replacement);
    return rc;
}

static int yaml_build_list_add(yaml_buf_t *out, const yaml_doc_t *doc,
                               const yaml_list_target_t *target, const char *key, size_t key_len,
                               const char *item, size_t item_len) {
    if (target->kind == YAML_LIST_ABSENT) {
        return yaml_build_absent_list(out, doc, key, key_len, item, item_len);
    }
    if (target->kind == YAML_LIST_BLOCK) {
        return yaml_build_block_list_add(out, doc, target, item, item_len);
    }
    return yaml_build_inline_list_add(out, doc, target, item, item_len);
}

static int yaml_build_mapping_section_add(yaml_buf_t *out, const yaml_doc_t *doc,
                                          const yaml_mapping_target_t *target,
                                          const yaml_buf_t *entry) {
    if (yaml_buf_append(out, doc->data, target->section_end) != 0 ||
        yaml_append_separator_if_needed(out, doc, target->section_end) != 0 ||
        yaml_buf_append(out, entry->data, entry->len) != 0 ||
        yaml_buf_append(out, doc->data + target->section_end, doc->len - target->section_end) !=
            0) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_build_new_mapping_section(yaml_buf_t *out, const yaml_doc_t *doc,
                                          const char *section_key, size_t section_len,
                                          const yaml_buf_t *entry) {
    if (yaml_buf_append(out, doc->data, doc->len) != 0 ||
        yaml_append_separator_if_needed(out, doc, doc->len) != 0 ||
        yaml_buf_append(out, section_key, section_len) != 0 ||
        yaml_buf_append_char(out, ':') != 0 || yaml_buf_append(out, doc->eol, doc->eol_len) != 0 ||
        yaml_buf_append(out, entry->data, entry->len) != 0) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_build_inline_empty_mapping_section_add(yaml_buf_t *out, const yaml_doc_t *doc,
                                                       const yaml_mapping_target_t *target,
                                                       const yaml_buf_t *entry) {
    if (target->section_line >= doc->line_count || target->section_end > doc->len) {
        return YAML_ERROR;
    }
    const yaml_line_t *section = &doc->lines[target->section_line];
    size_t comment = 0U;
    if (yaml_find_comment(doc->data, target->section_colon + YAML_UNIT, section->text_end,
                          &comment) != 0 ||
        target->section_end < section->end) {
        return YAML_ERROR;
    }
    if (yaml_buf_append(out, doc->data, section->start) != 0 ||
        yaml_buf_append(out, doc->data + section->start,
                        target->section_colon + YAML_UNIT - section->start) != 0) {
        return YAML_ERROR;
    }
    if (comment < section->text_end) {
        if (yaml_buf_append_char(out, ' ') != 0 ||
            yaml_buf_append(out, doc->data + comment, section->end - comment) != 0) {
            return YAML_ERROR;
        }
    } else if (yaml_buf_append(out, doc->data + section->text_end,
                               section->end - section->text_end) != 0) {
        return YAML_ERROR;
    }
    if (yaml_buf_append(out, doc->data + section->end, target->section_end - section->end) != 0 ||
        (out->len > 0U && out->data[out->len - YAML_UNIT] != '\n' &&
         yaml_buf_append(out, doc->eol, doc->eol_len) != 0) ||
        yaml_buf_append(out, entry->data, entry->len) != 0 ||
        yaml_buf_append(out, doc->data + target->section_end, doc->len - target->section_end) !=
            0) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_build_mapping_update(yaml_buf_t *out, const yaml_doc_t *doc,
                                     const yaml_mapping_target_t *target, const yaml_line_t *header,
                                     const char *section_key, size_t section_len,
                                     const yaml_buf_t *entry) {
    if (target->entry_found) {
        if (!header) {
            return YAML_ERROR;
        }
        return yaml_splice(out, doc, header->start, target->entry_end, entry->data, entry->len);
    }
    if (target->section_found) {
        if (target->section_inline_empty) {
            return yaml_build_inline_empty_mapping_section_add(out, doc, target, entry);
        }
        return yaml_build_mapping_section_add(out, doc, target, entry);
    }
    return yaml_build_new_mapping_section(out, doc, section_key, section_len, entry);
}

static int yaml_build_last_mapping_entry_removal(yaml_buf_t *out, const yaml_doc_t *doc,
                                                 const yaml_mapping_target_t *target) {
    if (target->section_line >= doc->line_count || target->entry_line >= doc->line_count ||
        target->section_colon >= doc->lines[target->section_line].text_end) {
        return YAML_ERROR;
    }
    const yaml_line_t *section = &doc->lines[target->section_line];
    const yaml_line_t *entry = &doc->lines[target->entry_line];
    size_t comment = 0U;
    if (yaml_find_comment(doc->data, target->section_colon + YAML_UNIT, section->text_end,
                          &comment) != 0 ||
        entry->start < section->end || target->entry_end < entry->end ||
        target->entry_end > doc->len) {
        return YAML_ERROR;
    }

    if (yaml_buf_append(out, doc->data, section->start) != 0 ||
        yaml_buf_append(out, doc->data + section->start,
                        target->section_colon + YAML_UNIT - section->start) != 0 ||
        yaml_buf_append(out, " {}", YAML_LITERAL_LEN(" {}")) != 0) {
        return YAML_ERROR;
    }
    if (comment < section->text_end) {
        if (yaml_buf_append_char(out, ' ') != 0 ||
            yaml_buf_append(out, doc->data + comment, section->end - comment) != 0) {
            return YAML_ERROR;
        }
    } else if (yaml_buf_append(out, doc->data + section->text_end,
                               section->end - section->text_end) != 0) {
        return YAML_ERROR;
    }
    if (yaml_buf_append(out, doc->data + section->end, entry->start - section->end) != 0 ||
        yaml_buf_append(out, doc->data + target->entry_end, doc->len - target->entry_end) != 0) {
        return YAML_ERROR;
    }
    return 0;
}

typedef struct {
    char **values;
    size_t len;
    size_t cap;
} yaml_sequence_key_vec_t;

typedef struct {
    bool sequence_found;
    size_t missing_index;
    size_t insert_offset;
    size_t item_count;
    bool identity_found;
    size_t identity_start;
    size_t identity_end;
} yaml_mapping_sequence_target_t;

static void yaml_sequence_key_vec_free(yaml_sequence_key_vec_t *keys) {
    for (size_t i = 0U; i < keys->len; i++) {
        free(keys->values[i]);
    }
    free(keys->values);
    memset(keys, 0, sizeof(*keys));
}

/* Takes ownership of value on every path. Duplicate mapping keys are an
 * ambiguity, not an overwrite opportunity. */
static int yaml_sequence_key_vec_add(yaml_sequence_key_vec_t *keys, char *value) {
    for (size_t i = 0U; i < keys->len; i++) {
        if (strcmp(keys->values[i], value) == 0) {
            free(value);
            return YAML_ERROR;
        }
    }
    if (keys->len == keys->cap) {
        size_t next = keys->cap ? keys->cap * YAML_GROWTH_FACTOR : YAML_ITEM_INITIAL_CAPACITY;
        if (next < keys->cap || next > YAML_INPUT_MAX / sizeof(*keys->values)) {
            free(value);
            return YAML_ERROR;
        }
        char **grown = (char **)realloc(keys->values, next * sizeof(*keys->values));
        if (!grown) {
            free(value);
            return YAML_ERROR;
        }
        keys->values = grown;
        keys->cap = next;
    }
    keys->values[keys->len++] = value;
    return 0;
}

static size_t yaml_sequence_line_offset(const yaml_doc_t *doc, size_t line_index) {
    return line_index < doc->line_count ? doc->lines[line_index].start : doc->len;
}

static size_t yaml_sequence_nested_end(const yaml_doc_t *doc, size_t header_line,
                                       size_t parent_end) {
    size_t indent = doc->lines[header_line].indent;
    for (size_t i = header_line + YAML_UNIT; i < parent_end; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (!line->blank && !line->comment && !line->dquote_cont && line->indent <= indent) {
            return i;
        }
    }
    return parent_end;
}

static int yaml_sequence_line_has_unsupported(const yaml_doc_t *doc, const yaml_line_t *line) {
    size_t start = line->start + line->indent;
    /* Exact empty flow collections as values (`envs: {}`, `plugins: []`) are
     * preserved opaque user content (#1631/#1673): validate only the key. */
    size_t flow_colon = 0U;
    bool empty_flow = false;
    if (yaml_find_mapping_colon(doc->data, start, line->text_end, &flow_colon) == 0 &&
        yaml_tail_is_explicit_empty_flow(doc->data, flow_colon, line->text_end, &empty_flow) == 0 &&
        empty_flow) {
        return yaml_range_has_unsupported(doc->data, start, flow_colon + YAML_UNIT);
    }
    size_t scan_end = line->text_end;
    if (line->dquote_opens && scan_end > start && doc->data[scan_end - YAML_UNIT] == '\\') {
        scan_end--;
    }
    if (yaml_range_has_unsupported_ex(doc->data, start, line->text_end, line->dquote_opens)) {
        return YAML_MATCH;
    }
    char quote = '\0';
    char previous = '\0';
    for (size_t i = start; i < scan_end; i++) {
        bool is_plain = false;
        if (yaml_scan_quote_positional(doc->data, scan_end, &i, &quote, &is_plain, previous) != 0) {
            return YAML_MATCH;
        }
        if (!is_plain) {
            previous = 'q';
            continue;
        }
        char value = doc->data[i];
        if (value == '#' && (i == start || doc->data[i - YAML_UNIT] == ' ')) {
            break;
        }
        if (value == '[' || value == ']' || value == '|' || value == '>') {
            return YAML_MATCH;
        }
        if (value != ' ' && value != '\t') {
            previous = value;
        }
    }
    if (quote == '"' && line->dquote_opens) {
        return 0;
    }
    return quote != '\0' ? YAML_MATCH : 0;
}

static int yaml_sequence_validate_document(const yaml_doc_t *doc) {
    for (size_t i = 0U; i < doc->line_count; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->blank || line->comment || line->dquote_cont) {
            continue;
        }
        if ((line->indent & YAML_UNIT) != 0U || yaml_sequence_line_has_unsupported(doc, line)) {
            return YAML_ERROR;
        }
    }
    return 0;
}

static int yaml_sequence_decode_field_key(const yaml_doc_t *doc, size_t start, size_t end,
                                          size_t *out_colon, char **out_key) {
    size_t colon = 0U;
    if (yaml_find_mapping_colon(doc->data, start, end, &colon) != 0) {
        return YAML_ERROR;
    }
    size_t key_end = yaml_trim_spaces_end(doc->data, start, colon);
    char *key = NULL;
    if (key_end == start || yaml_decode_scalar(doc->data, start, key_end, &key) != 0 ||
        strcmp(key, "<<") == 0) {
        free(key);
        return YAML_ERROR;
    }
    *out_colon = colon;
    *out_key = key;
    return 0;
}

static int yaml_sequence_validate_mapping_range(const yaml_doc_t *doc, size_t begin_line,
                                                size_t end_line, size_t direct_indent) {
    bool have_direct = false;
    /* YAML allows a block sequence at the same indent as its mapping key
     * (#1631): a dash line directly after a value-less key at this indent is
     * that key's opaque foreign value, not malformed structure. */
    bool prev_key_open = false;
    yaml_sequence_key_vec_t keys = {0};
    int result = 0;
    for (size_t i = begin_line; i < end_line; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->blank || line->comment || line->dquote_cont) {
            continue;
        }
        if (line->indent < direct_indent || (line->indent > direct_indent && !have_direct)) {
            result = YAML_ERROR;
            break;
        }
        if (line->indent != direct_indent) {
            continue;
        }
        size_t start = line->start + direct_indent;
        if (start >= line->text_end) {
            result = YAML_ERROR;
            break;
        }
        if (doc->data[start] == '-') {
            bool is_item =
                start + YAML_UNIT == line->text_end || doc->data[start + YAML_UNIT] == ' ';
            if (!is_item || !prev_key_open) {
                result = YAML_ERROR;
                break;
            }
            continue;
        }
        size_t colon = 0U;
        char *key = NULL;
        if (yaml_sequence_decode_field_key(doc, start, line->text_end, &colon, &key) != 0 ||
            yaml_sequence_key_vec_add(&keys, key) != 0) {
            result = YAML_ERROR;
            break;
        }
        have_direct = true;
        prev_key_open = yaml_tail_is_empty(doc->data, colon, line->text_end) != 0;
    }
    yaml_sequence_key_vec_free(&keys);
    return result;
}

static int yaml_sequence_find_key(const yaml_doc_t *doc, size_t begin_line, size_t end_line,
                                  size_t indent, const char *key, size_t key_len, bool *out_found,
                                  size_t *out_line, size_t *out_colon) {
    size_t count = 0U;
    for (size_t i = begin_line; i < end_line; i++) {
        size_t colon = 0U;
        int match = yaml_line_matches_key(doc, &doc->lines[i], indent, key, key_len, &colon);
        if (match < 0) {
            return YAML_ERROR;
        }
        if (match == YAML_MATCH) {
            count++;
            *out_line = i;
            *out_colon = colon;
        }
    }
    if (count > YAML_UNIT) {
        return YAML_ERROR;
    }
    *out_found = count == YAML_UNIT;
    return 0;
}

static int yaml_sequence_parse_item_field(const yaml_doc_t *doc, const yaml_line_t *line,
                                          size_t field_start, const char *identity_key,
                                          const char *identity_value, yaml_sequence_key_vec_t *keys,
                                          bool *out_identity_match) {
    size_t colon = 0U;
    char *key = NULL;
    if (yaml_sequence_decode_field_key(doc, field_start, line->text_end, &colon, &key) != 0) {
        return YAML_ERROR;
    }
    bool identity_field = strcmp(key, identity_key) == 0;
    if (yaml_sequence_key_vec_add(keys, key) != 0) {
        return YAML_ERROR;
    }

    size_t comment = 0U;
    if (yaml_find_comment(doc->data, colon + YAML_UNIT, line->text_end, &comment) != 0) {
        return YAML_ERROR;
    }
    size_t value_start = yaml_skip_spaces(doc->data, colon + YAML_UNIT, comment);
    size_t value_end = yaml_trim_spaces_end(doc->data, value_start, comment);
    if (value_start == value_end) {
        return identity_field ? YAML_ERROR : 0;
    }
    if (yaml_value_starts_multiline(doc->data, colon, line->text_end)) {
        return YAML_ERROR;
    }
    char *decoded = NULL;
    if (yaml_decode_scalar(doc->data, value_start, value_end, &decoded) != 0) {
        return YAML_ERROR;
    }
    if (identity_field) {
        *out_identity_match = strcmp(decoded, identity_value) == 0;
    }
    free(decoded);
    return 0;
}

static int yaml_sequence_parse_item(const yaml_doc_t *doc, size_t start_line, size_t end_line,
                                    size_t item_indent, const char *identity_key,
                                    const char *identity_value, bool *out_identity_match) {
    yaml_sequence_key_vec_t keys = {0};
    bool parsed_field = false;
    bool identity_match = false;
    int result = 0;
    for (size_t i = start_line; i < end_line; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->blank || line->comment || line->dquote_cont) {
            continue;
        }
        size_t field_start = 0U;
        if (i == start_line) {
            size_t dash = line->start + item_indent;
            if (line->indent != item_indent || dash + YAML_UNIT >= line->text_end ||
                doc->data[dash] != '-' || doc->data[dash + YAML_UNIT] != ' ') {
                result = YAML_ERROR;
                break;
            }
            field_start = dash + YAML_ENTRY_INDENT;
        } else if (line->indent == item_indent + YAML_ENTRY_INDENT) {
            field_start = line->start + line->indent;
        } else if (line->indent <= item_indent + YAML_ENTRY_INDENT) {
            result = YAML_ERROR;
            break;
        } else {
            if (!parsed_field) {
                result = YAML_ERROR;
                break;
            }
            continue;
        }
        if (yaml_sequence_parse_item_field(doc, line, field_start, identity_key, identity_value,
                                           &keys, &identity_match) != 0) {
            result = YAML_ERROR;
            break;
        }
        parsed_field = true;
    }
    yaml_sequence_key_vec_free(&keys);
    if (result == 0 && !parsed_field) {
        result = YAML_ERROR;
    }
    *out_identity_match = result == 0 && identity_match;
    return result;
}

static int yaml_sequence_parse_block(const yaml_doc_t *doc, size_t begin_line, size_t end_line,
                                     size_t item_indent, const char *identity_key,
                                     const char *identity_value,
                                     yaml_mapping_sequence_target_t *target) {
    size_t range_len = end_line - begin_line;
    /* Minimum one element: a NULL-for-empty-range starts is only safe through
     * the loop-bound == alloc-bound invariant, which a path-sensitive
     * analyzer (rightly) refuses to assume across expressions. */
    size_t *starts = (size_t *)calloc(range_len ? range_len : 1, sizeof(*starts));
    if (!starts) {
        return YAML_ERROR;
    }
    size_t start_count = 0U;
    for (size_t i = begin_line; i < end_line; i++) {
        const yaml_line_t *line = &doc->lines[i];
        if (line->blank || line->comment || line->dquote_cont) {
            continue;
        }
        if (line->indent == item_indent) {
            size_t dash = line->start + item_indent;
            if (dash + YAML_UNIT >= line->text_end || doc->data[dash] != '-' ||
                doc->data[dash + YAML_UNIT] != ' ') {
                free(starts);
                return YAML_ERROR;
            }
            starts[start_count++] = i;
        } else if (line->indent < item_indent || start_count == 0U) {
            free(starts);
            return YAML_ERROR;
        }
    }

    size_t identity_count = 0U;
    for (size_t i = 0U; i < start_count; i++) {
        size_t item_end_line = i + YAML_UNIT < start_count ? starts[i + YAML_UNIT] : end_line;
        bool identity_match = false;
        if (yaml_sequence_parse_item(doc, starts[i], item_end_line, item_indent, identity_key,
                                     identity_value, &identity_match) != 0) {
            free(starts);
            return YAML_ERROR;
        }
        if (identity_match) {
            identity_count++;
            target->identity_start = doc->lines[starts[i]].start;
            target->identity_end = yaml_sequence_line_offset(doc, item_end_line);
        }
    }
    free(starts);
    if (identity_count > YAML_UNIT) {
        return YAML_ERROR;
    }
    target->item_count = start_count;
    target->identity_found = identity_count == YAML_UNIT;
    return 0;
}

static int yaml_sequence_analyze(const yaml_doc_t *doc, const char *const *sequence_path,
                                 const size_t *path_lengths, size_t sequence_path_len,
                                 const char *identity_key, const char *identity_value,
                                 yaml_mapping_sequence_target_t *target) {
    memset(target, 0, sizeof(*target));
    if (yaml_sequence_validate_document(doc) != 0) {
        return YAML_ERROR;
    }
    size_t parent_begin = 0U;
    size_t parent_end = doc->line_count;
    for (size_t depth = 0U; depth < sequence_path_len; depth++) {
        if (depth > SIZE_MAX / YAML_ENTRY_INDENT) {
            return YAML_ERROR;
        }
        size_t indent = depth * YAML_ENTRY_INDENT;
        if (yaml_sequence_validate_mapping_range(doc, parent_begin, parent_end, indent) != 0) {
            return YAML_ERROR;
        }
        bool found = false;
        size_t key_line = 0U;
        size_t colon = 0U;
        if (yaml_sequence_find_key(doc, parent_begin, parent_end, indent, sequence_path[depth],
                                   path_lengths[depth], &found, &key_line, &colon) != 0) {
            return YAML_ERROR;
        }
        if (!found) {
            target->missing_index = depth;
            target->insert_offset = yaml_sequence_line_offset(doc, parent_end);
            return 0;
        }
        const yaml_line_t *header = &doc->lines[key_line];
        if (!yaml_tail_is_empty(doc->data, colon, header->text_end)) {
            return YAML_ERROR;
        }
        size_t child_end = yaml_sequence_nested_end(doc, key_line, parent_end);
        if (depth + YAML_UNIT == sequence_path_len) {
            target->sequence_found = true;
            target->missing_index = sequence_path_len;
            target->insert_offset = yaml_sequence_line_offset(doc, child_end);
            return yaml_sequence_parse_block(doc, key_line + YAML_UNIT, child_end,
                                             indent + YAML_ENTRY_INDENT, identity_key,
                                             identity_value, target);
        }
        parent_begin = key_line + YAML_UNIT;
        parent_end = child_end;
    }
    return YAML_ERROR;
}

static int yaml_sequence_append_spaces(yaml_buf_t *out, size_t count) {
    static const char spaces[] = "                                ";
    while (count > 0U) {
        size_t chunk = count < YAML_LITERAL_LEN(spaces) ? count : YAML_LITERAL_LEN(spaces);
        if (yaml_buf_append(out, spaces, chunk) != 0) {
            return YAML_ERROR;
        }
        count -= chunk;
    }
    return 0;
}

static int yaml_sequence_render_item(yaml_buf_t *out, const char *canonical_item,
                                     size_t canonical_len, size_t indent, const char *eol,
                                     size_t eol_len) {
    if (canonical_len == 0U || yaml_validate_text_bytes(canonical_item, canonical_len) != 0) {
        return YAML_ERROR;
    }
    size_t line_index = 0U;
    size_t start = 0U;
    while (start < canonical_len) {
        size_t end = start;
        while (end < canonical_len && canonical_item[end] != '\n') {
            end++;
        }
        size_t text_end = end;
        if (text_end > start && canonical_item[text_end - YAML_UNIT] == '\r') {
            text_end--;
        }
        size_t source_indent = start;
        while (source_indent < text_end && canonical_item[source_indent] == ' ') {
            source_indent++;
        }
        source_indent -= start;
        bool valid_first = line_index == 0U && source_indent == 0U && text_end - start >= 2U &&
                           canonical_item[start] == '-' && canonical_item[start + YAML_UNIT] == ' ';
        bool valid_continuation = line_index > 0U && source_indent == YAML_ENTRY_INDENT &&
                                  start + source_indent < text_end;
        if ((!valid_first && !valid_continuation) ||
            yaml_sequence_append_spaces(out, indent) != 0 ||
            yaml_buf_append(out, canonical_item + start, text_end - start) != 0 ||
            yaml_buf_append(out, eol, eol_len) != 0) {
            return YAML_ERROR;
        }
        line_index++;
        start = end < canonical_len ? end + YAML_UNIT : end;
    }
    return line_index > 0U ? 0 : YAML_ERROR;
}

static int yaml_sequence_validate_canonical(const char *canonical_item, size_t canonical_len,
                                            const char *identity_key, const char *identity_value) {
    yaml_buf_t rendered = {0};
    yaml_buf_t wrapped = {0};
    int result = yaml_sequence_render_item(&rendered, canonical_item, canonical_len,
                                           YAML_VALUE_INDENT, "\n", YAML_UNIT);
    if (result == 0 && (yaml_buf_append(&wrapped, "root:\n  sequence:\n",
                                        YAML_LITERAL_LEN("root:\n  sequence:\n")) != 0 ||
                        yaml_buf_append(&wrapped, rendered.data, rendered.len) != 0)) {
        result = YAML_ERROR;
    }
    yaml_doc_t doc;
    memset(&doc, 0, sizeof(doc));
    if (result == 0 && yaml_doc_init(&doc, wrapped.data, wrapped.len) != 0) {
        result = YAML_ERROR;
    }
    if (result == 0) {
        const char *const path[] = {"root", "sequence"};
        const size_t lengths[] = {YAML_LITERAL_LEN("root"), YAML_LITERAL_LEN("sequence")};
        yaml_mapping_sequence_target_t target;
        if (yaml_sequence_analyze(&doc, path, lengths, 2U, identity_key, identity_value, &target) !=
                0 ||
            !target.sequence_found || target.item_count != YAML_UNIT || !target.identity_found ||
            target.identity_end - target.identity_start != rendered.len ||
            memcmp(doc.data + target.identity_start, rendered.data, rendered.len) != 0) {
            result = YAML_ERROR;
        }
    }
    yaml_doc_free(&doc);
    yaml_buf_free(&wrapped);
    yaml_buf_free(&rendered);
    return result;
}

static int yaml_sequence_build_insert(yaml_buf_t *out, const yaml_doc_t *doc,
                                      const yaml_mapping_sequence_target_t *target,
                                      const char *const *sequence_path, const size_t *path_lengths,
                                      size_t sequence_path_len, const yaml_buf_t *rendered_item) {
    if (target->insert_offset > doc->len ||
        yaml_buf_append(out, doc->data, target->insert_offset) != 0 ||
        yaml_append_separator_if_needed(out, doc, target->insert_offset) != 0) {
        return YAML_ERROR;
    }
    if (!target->sequence_found) {
        for (size_t depth = target->missing_index; depth < sequence_path_len; depth++) {
            size_t indent = depth * YAML_ENTRY_INDENT;
            if (yaml_sequence_append_spaces(out, indent) != 0 ||
                yaml_buf_append(out, sequence_path[depth], path_lengths[depth]) != 0 ||
                yaml_buf_append_char(out, ':') != 0 ||
                yaml_buf_append(out, doc->eol, doc->eol_len) != 0) {
                return YAML_ERROR;
            }
        }
    }
    if (yaml_buf_append(out, rendered_item->data, rendered_item->len) != 0 ||
        yaml_buf_append(out, doc->data + target->insert_offset, doc->len - target->insert_offset) !=
            0) {
        return YAML_ERROR;
    }
    return 0;
}

static int yaml_edit_mapping_sequence_item_locked(const char *file_path,
                                                  const char *const *sequence_path,
                                                  size_t sequence_path_len,
                                                  const char *identity_key,
                                                  const char *identity_scalar,
                                                  const char *canonical_item, bool remove) {
    size_t file_path_len = 0U;
    size_t identity_key_len = 0U;
    size_t identity_scalar_len = 0U;
    size_t canonical_len = 0U;
    if (yaml_bounded_strlen(file_path, YAML_OUTPUT_MAX, &file_path_len) != 0 ||
        file_path_len == 0U || !sequence_path || sequence_path_len == 0U ||
        sequence_path_len > YAML_SEQUENCE_PATH_LIMIT ||
        yaml_validate_key(identity_key, &identity_key_len) != 0 ||
        yaml_bounded_strlen(identity_scalar, YAML_ITEM_MAX, &identity_scalar_len) != 0 ||
        yaml_validate_text_bytes(identity_scalar, identity_scalar_len) != 0 ||
        yaml_bounded_strlen(canonical_item, YAML_BLOCK_MAX, &canonical_len) != 0) {
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    (void)identity_key_len;

    size_t *path_lengths = (size_t *)calloc(sequence_path_len, sizeof(*path_lengths));
    if (!path_lengths) {
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    for (size_t i = 0U; i < sequence_path_len; i++) {
        if (yaml_validate_key(sequence_path[i], &path_lengths[i]) != 0) {
            free(path_lengths);
            return CBM_YAML_IDENTITY_EDIT_ERROR;
        }
    }
    char *identity_value = NULL;
    if (yaml_decode_scalar(identity_scalar, 0U, identity_scalar_len, &identity_value) != 0 ||
        yaml_sequence_validate_canonical(canonical_item, canonical_len, identity_key,
                                         identity_value) != 0) {
        free(identity_value);
        free(path_lengths);
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }

    char *data = NULL;
    size_t len = 0U;
    yaml_file_snapshot_t snapshot;
    if (yaml_read_file(file_path, &data, &len, &snapshot) != 0) {
        free(identity_value);
        free(path_lengths);
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    yaml_doc_t doc;
    if (yaml_doc_init(&doc, data, len) != 0) {
        free(data);
        free(identity_value);
        free(path_lengths);
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    yaml_mapping_sequence_target_t target;
    if (yaml_sequence_analyze(&doc, sequence_path, path_lengths, sequence_path_len, identity_key,
                              identity_value, &target) != 0) {
        yaml_doc_free(&doc);
        free(data);
        free(identity_value);
        free(path_lengths);
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }

    yaml_buf_t rendered = {0};
    size_t item_indent = sequence_path_len * YAML_ENTRY_INDENT;
    if (yaml_sequence_render_item(&rendered, canonical_item, canonical_len, item_indent, doc.eol,
                                  doc.eol_len) != 0) {
        yaml_buf_free(&rendered);
        yaml_doc_free(&doc);
        free(data);
        free(identity_value);
        free(path_lengths);
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }

    int result = CBM_YAML_IDENTITY_EDIT_OK;
    if (target.identity_found) {
        size_t existing_len = target.identity_end - target.identity_start;
        if (existing_len != rendered.len ||
            memcmp(doc.data + target.identity_start, rendered.data, rendered.len) != 0) {
            result = CBM_YAML_IDENTITY_EDIT_FOREIGN;
        } else if (remove) {
            yaml_buf_t out = {0};
            if (yaml_splice(&out, &doc, target.identity_start, target.identity_end, NULL, 0U) !=
                    0 ||
                yaml_commit_if_changed(file_path, data, len, &snapshot, &out) != 0) {
                result = CBM_YAML_IDENTITY_EDIT_ERROR;
            }
            yaml_buf_free(&out);
        }
    } else if (!remove) {
        yaml_buf_t out = {0};
        if (yaml_sequence_build_insert(&out, &doc, &target, sequence_path, path_lengths,
                                       sequence_path_len, &rendered) != 0 ||
            yaml_commit_if_changed(file_path, data, len, &snapshot, &out) != 0) {
            result = CBM_YAML_IDENTITY_EDIT_ERROR;
        }
        yaml_buf_free(&out);
    }

    yaml_buf_free(&rendered);
    yaml_doc_free(&doc);
    free(data);
    free(identity_value);
    free(path_lengths);
    return result;
}

static int yaml_upsert_mapping_entry_locked(const char *file_path, const char *section_key,
                                            const char *entry_key, const char *entry_block) {
    size_t section_len = 0U;
    size_t entry_len = 0U;
    size_t block_len = 0U;
    size_t path_len = 0U;
    if (yaml_bounded_strlen(file_path, YAML_OUTPUT_MAX, &path_len) != 0 || path_len == 0U ||
        yaml_validate_key(section_key, &section_len) != 0 ||
        yaml_validate_key(entry_key, &entry_len) != 0 ||
        yaml_validate_entry_block(entry_block, &block_len) != 0) {
        return YAML_ERROR;
    }

    char *data = NULL;
    size_t len = 0U;
    yaml_file_snapshot_t snapshot;
    if (yaml_read_file(file_path, &data, &len, &snapshot) != 0) {
        return YAML_ERROR;
    }
    yaml_doc_t doc;
    if (yaml_doc_init(&doc, data, len) != 0) {
        free(data);
        return YAML_ERROR;
    }
    yaml_mapping_target_t target;
    if (yaml_analyze_mapping(&doc, section_key, section_len, entry_key, entry_len, &target) != 0) {
        yaml_doc_free(&doc);
        free(data);
        return YAML_ERROR;
    }

    yaml_buf_t entry = {0};
    const yaml_line_t *header = NULL;
    if (target.entry_found) {
        if (target.entry_line >= doc.line_count) {
            yaml_doc_free(&doc);
            free(data);
            return YAML_ERROR;
        }
        header = &doc.lines[target.entry_line];
    }
    if (yaml_build_mapping_entry(&entry, &doc, header, entry_key, entry_len, entry_block,
                                 block_len) != 0) {
        yaml_buf_free(&entry);
        yaml_doc_free(&doc);
        free(data);
        return YAML_ERROR;
    }
    yaml_buf_t out = {0};
    int build_rc =
        yaml_build_mapping_update(&out, &doc, &target, header, section_key, section_len, &entry);

    int rc =
        build_rc == 0 ? yaml_commit_if_changed(file_path, data, len, &snapshot, &out) : YAML_ERROR;
    yaml_buf_free(&out);
    yaml_buf_free(&entry);
    yaml_doc_free(&doc);
    free(data);
    return rc;
}

static int yaml_remove_mapping_entry_locked(const char *file_path, const char *section_key,
                                            const char *entry_key) {
    size_t section_len = 0U;
    size_t entry_len = 0U;
    size_t path_len = 0U;
    if (yaml_bounded_strlen(file_path, YAML_OUTPUT_MAX, &path_len) != 0 || path_len == 0U ||
        yaml_validate_key(section_key, &section_len) != 0 ||
        yaml_validate_key(entry_key, &entry_len) != 0) {
        return YAML_ERROR;
    }

    char *data = NULL;
    size_t len = 0U;
    yaml_file_snapshot_t snapshot;
    if (yaml_read_file(file_path, &data, &len, &snapshot) != 0) {
        return YAML_ERROR;
    }
    yaml_doc_t doc;
    if (yaml_doc_init(&doc, data, len) != 0) {
        free(data);
        return YAML_ERROR;
    }
    yaml_mapping_target_t target;
    if (yaml_analyze_mapping(&doc, section_key, section_len, entry_key, entry_len, &target) != 0) {
        yaml_doc_free(&doc);
        free(data);
        return YAML_ERROR;
    }
    if (!target.entry_found) {
        yaml_doc_free(&doc);
        free(data);
        return 0;
    }
    if (target.entry_line >= doc.line_count) {
        yaml_doc_free(&doc);
        free(data);
        return YAML_ERROR;
    }

    yaml_buf_t out = {0};
    const yaml_line_t *header = &doc.lines[target.entry_line];
    int rc = target.entry_count == YAML_UNIT
                 ? yaml_build_last_mapping_entry_removal(&out, &doc, &target)
                 : yaml_splice(&out, &doc, header->start, target.entry_end, NULL, 0U);
    if (rc == 0) {
        rc = yaml_commit_if_changed(file_path, data, len, &snapshot, &out);
    }
    yaml_buf_free(&out);
    yaml_doc_free(&doc);
    free(data);
    return rc;
}

/* #1631 / goose upgrades: an existing entry under OUR key whose bytes differ
 * from today's canonical is still OURS — and repairable — when it parses as a
 * shape a previous release wrote AND its command value's basename is our
 * binary. Recognized prior shapes, nothing else:
 *   header `  <key>:` then EITHER exactly one `    command: V` line (the YAML
 *   stdio schema; early releases wrote V unquoted), OR the goose block
 *   `type/cmd/args/enabled` in order with an optional leading `name:` (the
 *   pre-#1675 block had no name). V may be plain or double-quoted; its
 *   basename must be codebase-memory-mcp[.exe]. Anything else stays FOREIGN
 *   (fail-closed). */
static bool yaml_owned_value_is_our_binary(const char *v, size_t vlen) {
    if (vlen >= 2U && v[0] == '"' && v[vlen - 1U] == '"') {
        v++;
        vlen -= 2U;
    }
    size_t base = 0U;
    for (size_t i = 0; i < vlen; i++) {
        if (v[i] == '/' || v[i] == '\\') {
            base = i + 1U;
        }
    }
    const char *name = v + base;
    size_t name_len = vlen - base;
    static const char plain[] = "codebase-memory-mcp";
    static const char exe[] = "codebase-memory-mcp.exe";
    return (name_len == sizeof(plain) - 1U && memcmp(name, plain, name_len) == 0) ||
           (name_len == sizeof(exe) - 1U && memcmp(name, exe, name_len) == 0);
}

static bool yaml_owned_entry_is_prior_shape(const char *data, size_t len, const char *entry_key,
                                            size_t entry_key_len) {
    /* Split into lines, tolerating CRLF. */
    enum { PRIOR_MAX_LINES = 8 };
    struct {
        const char *text;
        size_t len;
    } lines[PRIOR_MAX_LINES];
    size_t count = 0U;
    size_t pos = 0U;
    while (pos < len) {
        const char *nl = memchr(data + pos, '\n', len - pos);
        size_t line_len = nl ? (size_t)(nl - (data + pos)) : len - pos;
        size_t trimmed = line_len;
        while (trimmed > 0U && (data[pos + trimmed - 1U] == '\r')) {
            trimmed--;
        }
        if (trimmed > 0U) {
            if (count == PRIOR_MAX_LINES) {
                return false;
            }
            lines[count].text = data + pos;
            lines[count].len = trimmed;
            count++;
        }
        pos += line_len + (nl ? 1U : 0U);
    }
    if (count < 2U) {
        return false;
    }
    /* Header: `  <key>:` exactly. */
    if (lines[0].len != entry_key_len + 3U || memcmp(lines[0].text, "  ", 2U) != 0 ||
        memcmp(lines[0].text + 2U, entry_key, entry_key_len) != 0 ||
        lines[0].text[2U + entry_key_len] != ':') {
        return false;
    }
    /* Body lines: 4-space indent, known fields only. */
    static const char cmd_prefix[] = "    command: ";
    static const char goose_name[] = "    name: codebase-memory-mcp";
    static const char goose_type[] = "    type: stdio";
    static const char goose_cmd[] = "    cmd: ";
    static const char goose_args[] = "    args: []";
    static const char goose_enabled[] = "    enabled: true";
    if (count == 2U && lines[1].len > sizeof(cmd_prefix) - 1U &&
        memcmp(lines[1].text, cmd_prefix, sizeof(cmd_prefix) - 1U) == 0) {
        return yaml_owned_value_is_our_binary(lines[1].text + sizeof(cmd_prefix) - 1U,
                                              lines[1].len - (sizeof(cmd_prefix) - 1U));
    }
    size_t i = 1U;
    if (i < count && lines[i].len == sizeof(goose_name) - 1U &&
        memcmp(lines[i].text, goose_name, lines[i].len) == 0) {
        i++;
    }
    if (i + 4U != count) {
        return false;
    }
    if (lines[i].len != sizeof(goose_type) - 1U ||
        memcmp(lines[i].text, goose_type, lines[i].len) != 0) {
        return false;
    }
    i++;
    if (lines[i].len <= sizeof(goose_cmd) - 1U ||
        memcmp(lines[i].text, goose_cmd, sizeof(goose_cmd) - 1U) != 0 ||
        !yaml_owned_value_is_our_binary(lines[i].text + sizeof(goose_cmd) - 1U,
                                        lines[i].len - (sizeof(goose_cmd) - 1U))) {
        return false;
    }
    i++;
    if (lines[i].len != sizeof(goose_args) - 1U ||
        memcmp(lines[i].text, goose_args, lines[i].len) != 0) {
        return false;
    }
    i++;
    return lines[i].len == sizeof(goose_enabled) - 1U &&
           memcmp(lines[i].text, goose_enabled, lines[i].len) == 0;
}

static int yaml_edit_owned_mapping_entry_locked(const char *file_path, const char *section_key,
                                                const char *entry_key,
                                                const char *canonical_entry_block, bool remove) {
    size_t section_len = 0U;
    size_t entry_len = 0U;
    size_t block_len = 0U;
    size_t path_len = 0U;
    if (yaml_bounded_strlen(file_path, YAML_OUTPUT_MAX, &path_len) != 0 || path_len == 0U ||
        yaml_validate_key(section_key, &section_len) != 0 ||
        yaml_validate_key(entry_key, &entry_len) != 0 ||
        yaml_validate_entry_block(canonical_entry_block, &block_len) != 0) {
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }

    char *data = NULL;
    size_t len = 0U;
    yaml_file_snapshot_t snapshot;
    if (yaml_read_file(file_path, &data, &len, &snapshot) != 0) {
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    yaml_doc_t doc;
    if (yaml_doc_init(&doc, data, len) != 0) {
        free(data);
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    yaml_mapping_target_t target;
    if (yaml_analyze_mapping(&doc, section_key, section_len, entry_key, entry_len, &target) != 0) {
        yaml_doc_free(&doc);
        free(data);
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }

    yaml_buf_t canonical = {0};
    if (yaml_build_mapping_entry(&canonical, &doc, NULL, entry_key, entry_len,
                                 canonical_entry_block, block_len) != 0) {
        yaml_buf_free(&canonical);
        yaml_doc_free(&doc);
        free(data);
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }

    const yaml_line_t *header = NULL;
    size_t entry_start = 0U;
    if (target.entry_found) {
        if (target.entry_line >= doc.line_count) {
            yaml_buf_free(&canonical);
            yaml_doc_free(&doc);
            free(data);
            return CBM_YAML_IDENTITY_EDIT_ERROR;
        }
        header = &doc.lines[target.entry_line];
        if (header->start > target.entry_end || target.entry_end > doc.len) {
            yaml_buf_free(&canonical);
            yaml_doc_free(&doc);
            free(data);
            return CBM_YAML_IDENTITY_EDIT_ERROR;
        }
        entry_start = header->start;
        size_t existing_entry_len = target.entry_end - entry_start;
        bool canonical_match = existing_entry_len == canonical.len &&
                               memcmp(doc.data + entry_start, canonical.data, canonical.len) == 0;
        if (!canonical_match &&
            !yaml_owned_entry_is_prior_shape(doc.data + entry_start, existing_entry_len, entry_key,
                                             entry_len)) {
            yaml_buf_free(&canonical);
            yaml_doc_free(&doc);
            free(data);
            return CBM_YAML_IDENTITY_EDIT_FOREIGN;
        }
        if (!remove && canonical_match) {
            yaml_buf_free(&canonical);
            yaml_doc_free(&doc);
            free(data);
            return CBM_YAML_IDENTITY_EDIT_OK;
        }
    } else if (remove) {
        yaml_buf_free(&canonical);
        yaml_doc_free(&doc);
        free(data);
        return CBM_YAML_IDENTITY_EDIT_OK;
    }

    yaml_buf_t out = {0};
    int build_result = 0;
    if (remove) {
        build_result = target.entry_count == YAML_UNIT
                           ? yaml_build_last_mapping_entry_removal(&out, &doc, &target)
                           : yaml_splice(&out, &doc, entry_start, target.entry_end, NULL, 0U);
    } else {
        build_result = yaml_build_mapping_update(&out, &doc, &target, header, section_key,
                                                 section_len, &canonical);
    }
    int result = build_result == 0 ? yaml_commit_if_changed(file_path, data, len, &snapshot, &out)
                                   : YAML_ERROR;
    yaml_buf_free(&out);
    yaml_buf_free(&canonical);
    yaml_doc_free(&doc);
    free(data);
    return result == 0 ? CBM_YAML_IDENTITY_EDIT_OK : CBM_YAML_IDENTITY_EDIT_ERROR;
}

static int yaml_upsert_string_list_item_locked(const char *file_path, const char *key,
                                               const char *item) {
    size_t key_len = 0U;
    size_t item_len = 0U;
    size_t path_len = 0U;
    if (yaml_bounded_strlen(file_path, YAML_OUTPUT_MAX, &path_len) != 0 || path_len == 0U ||
        yaml_validate_key(key, &key_len) != 0 || yaml_validate_item(item, &item_len) != 0) {
        return YAML_ERROR;
    }

    char *data = NULL;
    size_t len = 0U;
    yaml_file_snapshot_t snapshot;
    if (yaml_read_file(file_path, &data, &len, &snapshot) != 0) {
        return YAML_ERROR;
    }
    yaml_doc_t doc;
    if (yaml_doc_init(&doc, data, len) != 0) {
        free(data);
        return YAML_ERROR;
    }
    yaml_list_target_t target;
    if (yaml_analyze_list(&doc, key, key_len, &target) != 0) {
        yaml_item_vec_free(&target.items);
        yaml_doc_free(&doc);
        free(data);
        return YAML_ERROR;
    }
    if (yaml_item_match_count(&target.items, item) > 0U) {
        yaml_item_vec_free(&target.items);
        yaml_doc_free(&doc);
        free(data);
        return 0;
    }

    yaml_buf_t out = {0};
    int build_rc = yaml_build_list_add(&out, &doc, &target, key, key_len, item, item_len);

    int rc =
        build_rc == 0 ? yaml_commit_if_changed(file_path, data, len, &snapshot, &out) : YAML_ERROR;
    yaml_buf_free(&out);
    yaml_item_vec_free(&target.items);
    yaml_doc_free(&doc);
    free(data);
    return rc;
}

static int yaml_remove_string_list_item_locked(const char *file_path, const char *key,
                                               const char *item) {
    size_t key_len = 0U;
    size_t item_len = 0U;
    size_t path_len = 0U;
    if (yaml_bounded_strlen(file_path, YAML_OUTPUT_MAX, &path_len) != 0 || path_len == 0U ||
        yaml_validate_key(key, &key_len) != 0 || yaml_validate_item(item, &item_len) != 0) {
        return YAML_ERROR;
    }
    (void)item_len;

    char *data = NULL;
    size_t len = 0U;
    yaml_file_snapshot_t snapshot;
    if (yaml_read_file(file_path, &data, &len, &snapshot) != 0) {
        return YAML_ERROR;
    }
    yaml_doc_t doc;
    if (yaml_doc_init(&doc, data, len) != 0) {
        free(data);
        return YAML_ERROR;
    }
    yaml_list_target_t target;
    if (yaml_analyze_list(&doc, key, key_len, &target) != 0) {
        yaml_item_vec_free(&target.items);
        yaml_doc_free(&doc);
        free(data);
        return YAML_ERROR;
    }
    size_t matches = yaml_item_match_count(&target.items, item);
    if (matches == 0U) {
        yaml_item_vec_free(&target.items);
        yaml_doc_free(&doc);
        free(data);
        return 0;
    }
    if (target.key_line >= doc.line_count) {
        yaml_item_vec_free(&target.items);
        yaml_doc_free(&doc);
        free(data);
        return YAML_ERROR;
    }

    yaml_buf_t out = {0};
    int build_rc = 0;
    const yaml_line_t *line = &doc.lines[target.key_line];
    if (target.kind == YAML_LIST_SCALAR ||
        (target.kind == YAML_LIST_FLOW && matches == target.items.len)) {
        build_rc = yaml_splice(&out, &doc, line->start, line->end, NULL, 0U);
    } else if (target.kind == YAML_LIST_FLOW) {
        yaml_buf_t replacement = {0};
        build_rc = yaml_build_flow_replacement(&replacement, &doc, &target, NULL, 0U, item);
        if (build_rc == 0) {
            build_rc =
                yaml_splice(&out, &doc, line->start, line->end, replacement.data, replacement.len);
        }
        yaml_buf_free(&replacement);
    } else {
        build_rc = yaml_remove_block_items(&out, &doc, &target, item, matches);
    }

    int rc =
        build_rc == 0 ? yaml_commit_if_changed(file_path, data, len, &snapshot, &out) : YAML_ERROR;
    yaml_buf_free(&out);
    yaml_item_vec_free(&target.items);
    yaml_doc_free(&doc);
    free(data);
    return rc;
}

int cbm_yaml_upsert_mapping_entry(const char *file_path, const char *section_key,
                                  const char *entry_key, const char *entry_block) {
    yaml_config_lock_t lock;
    if (yaml_lock_acquire(file_path, &lock) != 0) {
        return YAML_ERROR;
    }
    int result = yaml_upsert_mapping_entry_locked(file_path, section_key, entry_key, entry_block);
    int release_result = yaml_lock_release(&lock);
    return result == 0 && release_result == 0 ? 0 : YAML_ERROR;
}

/* Removal is idempotent: a config that does not exist has nothing to remove.
 * The JSON removers already answer OK here (cbm_json_like_read_document's
 * "missing" result); the YAML removers instead took the lock first, and the
 * lock directory is created beside the target — so with the parent directory
 * absent (`~/.hermes/` never created because Hermes was detected by its
 * binary on PATH, not by its home) every uninstall reported
 * "does not exist or cannot be inspected" and refused to remove the
 * executable. Only a true ENOENT short-circuits: a dangling symlink still
 * reaches the reader and its byte-identical rejection. */
static bool yaml_remove_target_absent(const char *path) {
    if (!path) {
        return false;
    }
#ifdef _WIN32
    /* FILE_FLAG_OPEN_REPARSE_POINT, as in toml_read_file: opens the link
     * itself rather than following it, so a dangling symlink does NOT count as
     * absent and still reaches the reader — matching the POSIX branch, where
     * lstat does not follow either. */
    wchar_t *wide = cbm_utf8_to_wide(path);
    if (!wide) {
        return false;
    }
    HANDLE handle = CreateFileW(
        wide, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    DWORD error = handle == INVALID_HANDLE_VALUE ? GetLastError() : 0;
    free(wide);
    if (handle != INVALID_HANDLE_VALUE) {
        CloseHandle(handle);
        return false;
    }
    return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND;
#else
    struct stat path_state;
    return lstat(path, &path_state) != 0 && errno == ENOENT;
#endif
}

int cbm_yaml_remove_mapping_entry(const char *file_path, const char *section_key,
                                  const char *entry_key) {
    if (yaml_remove_target_absent(file_path)) {
        return 0;
    }
    yaml_config_lock_t lock;
    if (yaml_lock_acquire(file_path, &lock) != 0) {
        return YAML_ERROR;
    }
    int result = yaml_remove_mapping_entry_locked(file_path, section_key, entry_key);
    int release_result = yaml_lock_release(&lock);
    return result == 0 && release_result == 0 ? 0 : YAML_ERROR;
}

int cbm_yaml_upsert_owned_mapping_entry(const char *file_path, const char *section_key,
                                        const char *entry_key, const char *canonical_entry_block) {
    yaml_config_lock_t lock;
    if (yaml_lock_acquire(file_path, &lock) != 0) {
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    int result = yaml_edit_owned_mapping_entry_locked(file_path, section_key, entry_key,
                                                      canonical_entry_block, false);
    int release_result = yaml_lock_release(&lock);
    return release_result == 0 ? result : CBM_YAML_IDENTITY_EDIT_ERROR;
}

int cbm_yaml_remove_owned_mapping_entry(const char *file_path, const char *section_key,
                                        const char *entry_key, const char *canonical_entry_block) {
    if (yaml_remove_target_absent(file_path)) {
        return CBM_YAML_IDENTITY_EDIT_OK;
    }
    yaml_config_lock_t lock;
    if (yaml_lock_acquire(file_path, &lock) != 0) {
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    int result = yaml_edit_owned_mapping_entry_locked(file_path, section_key, entry_key,
                                                      canonical_entry_block, true);
    int release_result = yaml_lock_release(&lock);
    return release_result == 0 ? result : CBM_YAML_IDENTITY_EDIT_ERROR;
}

int cbm_yaml_upsert_mapping_sequence_item(const char *file_path, const char *const *sequence_path,
                                          size_t sequence_path_len, const char *identity_key,
                                          const char *identity_scalar, const char *canonical_item) {
    yaml_config_lock_t lock;
    if (yaml_lock_acquire(file_path, &lock) != 0) {
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    int result = yaml_edit_mapping_sequence_item_locked(file_path, sequence_path, sequence_path_len,
                                                        identity_key, identity_scalar,
                                                        canonical_item, false);
    int release_result = yaml_lock_release(&lock);
    return release_result == 0 ? result : CBM_YAML_IDENTITY_EDIT_ERROR;
}

int cbm_yaml_remove_mapping_sequence_item(const char *file_path, const char *const *sequence_path,
                                          size_t sequence_path_len, const char *identity_key,
                                          const char *identity_scalar, const char *canonical_item) {
    if (yaml_remove_target_absent(file_path)) {
        return CBM_YAML_IDENTITY_EDIT_OK;
    }
    yaml_config_lock_t lock;
    if (yaml_lock_acquire(file_path, &lock) != 0) {
        return CBM_YAML_IDENTITY_EDIT_ERROR;
    }
    int result =
        yaml_edit_mapping_sequence_item_locked(file_path, sequence_path, sequence_path_len,
                                               identity_key, identity_scalar, canonical_item, true);
    int release_result = yaml_lock_release(&lock);
    return release_result == 0 ? result : CBM_YAML_IDENTITY_EDIT_ERROR;
}

int cbm_yaml_upsert_string_list_item(const char *file_path, const char *key, const char *item) {
    yaml_config_lock_t lock;
    if (yaml_lock_acquire(file_path, &lock) != 0) {
        return YAML_ERROR;
    }
    int result = yaml_upsert_string_list_item_locked(file_path, key, item);
    int release_result = yaml_lock_release(&lock);
    return result == 0 && release_result == 0 ? 0 : YAML_ERROR;
}

int cbm_yaml_remove_string_list_item(const char *file_path, const char *key, const char *item) {
    if (yaml_remove_target_absent(file_path)) {
        return 0;
    }
    yaml_config_lock_t lock;
    if (yaml_lock_acquire(file_path, &lock) != 0) {
        return YAML_ERROR;
    }
    int result = yaml_remove_string_list_item_locked(file_path, key, item);
    int release_result = yaml_lock_release(&lock);
    return result == 0 && release_result == 0 ? 0 : YAML_ERROR;
}
