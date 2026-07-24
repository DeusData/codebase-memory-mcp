/*
 * config.c — Persistent UI configuration (JSON via yyjson).
 *
 * Config file: ~/.cache/codebase-memory-mcp/config.json
 * Format: {"ui_enabled": false, "ui_port": 9749}
 */
#include "foundation/constants.h"
#include "ui/config.h"
#include "ui/embedded_assets.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/compat.h"

#include <yyjson/yyjson.h>

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include "foundation/win_utf8.h"

#include <wchar.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

/* ── Path ────────────────────────────────────────────────────── */

void cbm_ui_config_path(char *buf, int bufsz) {
    const char *dir = cbm_resolve_cache_dir();
    if (!dir) {
        dir = cbm_tmpdir();
    }
    snprintf(buf, (size_t)bufsz, "%s/config.json", dir);
}

/* ── Load ────────────────────────────────────────────────────── */

static char *config_read_file(const char *path, size_t *length_out, bool *opened_out) {
    if (length_out) {
        *length_out = 0;
    }
    if (opened_out) {
        *opened_out = false;
    }
    if (!path || !length_out || !opened_out) {
        return NULL;
    }
#ifdef _WIN32
    wchar_t *wide_path = cbm_path_to_wide(path);
    if (!wide_path) {
        return NULL;
    }
    HANDLE file = CreateFileW(wide_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_DELETE, NULL,
                              OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    free(wide_path);
    if (file == INVALID_HANDLE_VALUE) {
        return NULL;
    }
    *opened_out = true;
    LARGE_INTEGER size;
    if (!GetFileSizeEx(file, &size) || size.QuadPart <= 0 || size.QuadPart > 4096) {
        (void)CloseHandle(file);
        return NULL;
    }
    size_t length = (size_t)size.QuadPart;
    char *buffer = malloc(length + 1U);
    DWORD read_length = 0;
    bool read_ok = buffer && ReadFile(file, buffer, (DWORD)length, &read_length, NULL) &&
                   read_length == (DWORD)length;
    (void)CloseHandle(file);
    if (!read_ok) {
        free(buffer);
        return NULL;
    }
#else
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return NULL;
    }
    *opened_out = true;
    if (fseek(file, 0, SEEK_END) != 0) {
        (void)fclose(file);
        return NULL;
    }
    long file_length = ftell(file);
    if (file_length <= 0 || file_length > 4096 || fseek(file, 0, SEEK_SET) != 0) {
        (void)fclose(file);
        return NULL;
    }
    size_t length = (size_t)file_length;
    char *buffer = malloc(length + 1U);
    bool read_ok = buffer && fread(buffer, 1, length, file) == length;
    (void)fclose(file);
    if (!read_ok) {
        free(buffer);
        return NULL;
    }
#endif
    buffer[length] = '\0';
    *length_out = length;
    return buffer;
}

void cbm_ui_config_load(cbm_ui_config_t *cfg) {
    if (!cfg) {
        return;
    }
    cfg->ui_enabled = CBM_UI_DEFAULT_ENABLED;
    cfg->ui_port = CBM_UI_DEFAULT_PORT;

    char path[CBM_SZ_1K];
    cbm_ui_config_path(path, (int)sizeof(path));

    size_t length = 0;
    bool opened = false;
    char *buffer = config_read_file(path, &length, &opened);
    if (!opened) {
        /* No config file — auto-enable UI if binary has embedded assets */
        if (CBM_EMBEDDED_FILE_COUNT > 0) {
            cfg->ui_enabled = true;
        }
        return;
    }
    if (!buffer) {
        return;
    }

    yyjson_doc *doc = yyjson_read(buffer, length, 0);
    free(buffer);
    if (!doc) {
        cbm_log_warn("ui.config.corrupt", "path", path);
        return; /* corrupt JSON → defaults */
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return;
    }

    yyjson_val *v_enabled = yyjson_obj_get(root, "ui_enabled");
    if (yyjson_is_bool(v_enabled)) {
        cfg->ui_enabled = yyjson_get_bool(v_enabled);
    }

    yyjson_val *v_port = yyjson_obj_get(root, "ui_port");
    if (yyjson_is_int(v_port)) {
        int64_t port = yyjson_get_int(v_port);
        if (port > 0 && port <= 65535) {
            cfg->ui_port = (int)port;
        }
    }

    yyjson_doc_free(doc);
}

/* ── Save ────────────────────────────────────────────────────── */

static bool config_parent_directory(const char *path, char *directory, size_t directory_size) {
    int written = snprintf(directory, directory_size, "%s", path ? path : "");
    if (written <= 0 || (size_t)written >= directory_size) {
        return false;
    }
    char *slash = strrchr(directory, '/');
    char *backslash = strrchr(directory, '\\');
    if (backslash && (!slash || backslash > slash)) {
        slash = backslash;
    }
    if (!slash) {
        return snprintf(directory, directory_size, ".") == 1;
    }
    if (slash == directory) {
        slash[1] = '\0';
    } else {
        *slash = '\0';
    }
    return true;
}

#ifdef _WIN32
static volatile LONG g_config_temp_sequence = 0;

typedef struct {
    DWORD Flags;
    HANDLE RootDirectory;
    DWORD FileNameLength;
    WCHAR FileName[1];
} config_file_rename_info_ex_t;

#define CONFIG_FILE_RENAME_INFO_EX_CLASS ((FILE_INFO_BY_HANDLE_CLASS)22)
#define CONFIG_FILE_RENAME_REPLACE 0x00000001U
#define CONFIG_FILE_RENAME_POSIX 0x00000002U

/* Publish the still-open temp file over the destination with POSIX rename
 * semantics: MoveFileExW's legacy replace cannot supersede a destination that
 * a reader holds open (its name lingers until the last handle closes, even
 * with FILE_SHARE_DELETE), so a config save racing an open reader failed.
 * The POSIX form frees the name immediately. The rename target must be the
 * bare drive path — the NT layer rejects the \\?\ prefix here — and the name
 * buffer is NUL-terminated in an over-allocated buffer because filter
 * drivers read FileName as NUL-terminated despite FileNameLength. */
static bool config_posix_rename_handle(HANDLE file, const wchar_t *target_path) {
    size_t chars = wcslen(target_path);
    if (chars >= 4U && wcsncmp(target_path, L"\\\\?\\", 4) == 0) {
        target_path += 4;
        chars -= 4U;
    }
    if (chars == 0U || chars > (size_t)UINT32_MAX / sizeof(wchar_t)) {
        return false;
    }
    size_t bytes = chars * sizeof(wchar_t);
    size_t allocation = offsetof(config_file_rename_info_ex_t, FileName) + bytes + sizeof(wchar_t);
    config_file_rename_info_ex_t *rename = calloc(1U, allocation);
    if (!rename) {
        return false;
    }
    rename->Flags = CONFIG_FILE_RENAME_POSIX | CONFIG_FILE_RENAME_REPLACE;
    rename->RootDirectory = NULL;
    rename->FileNameLength = (DWORD)bytes;
    memcpy(rename->FileName, target_path, bytes);
    rename->FileName[chars] = L'\0';
    bool renamed = SetFileInformationByHandle(file, CONFIG_FILE_RENAME_INFO_EX_CLASS, rename,
                                              (DWORD)allocation) != 0;
    free(rename);
    return renamed;
}

static bool config_write_atomic(const char *path, const char *json, size_t json_length) {
    wchar_t *wide_path = cbm_path_to_wide(path);
    if (!wide_path) {
        return false;
    }
    size_t temporary_capacity = wcslen(wide_path) + 40U;
    wchar_t *temporary = calloc(temporary_capacity, sizeof(*temporary));
    HANDLE file = INVALID_HANDLE_VALUE;
    if (temporary) {
        for (unsigned int attempt = 0; attempt < 128U; attempt++) {
            ULONG sequence = (ULONG)InterlockedIncrement(&g_config_temp_sequence);
            int written = swprintf(temporary, temporary_capacity, L"%ls.tmp.%08lX.%08lX", wide_path,
                                   (unsigned long)GetCurrentProcessId(), (unsigned long)sequence);
            if (written <= 0 || (size_t)written >= temporary_capacity) {
                break;
            }
            /* DELETE access on the temp handle is what permits the
             * rename-by-handle publish below. */
            file = CreateFileW(temporary, GENERIC_WRITE | DELETE, 0, NULL, CREATE_NEW,
                               FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH, NULL);
            if (file != INVALID_HANDLE_VALUE || GetLastError() != ERROR_FILE_EXISTS) {
                break;
            }
        }
    }
    bool ok = file != INVALID_HANDLE_VALUE;
    size_t offset = 0;
    while (ok && offset < json_length) {
        size_t remaining = json_length - offset;
        DWORD chunk = remaining > UINT32_MAX ? UINT32_MAX : (DWORD)remaining;
        DWORD written = 0;
        ok = WriteFile(file, json + offset, chunk, &written, NULL) != 0 && written > 0;
        offset += written;
    }
    if (ok) {
        ok = FlushFileBuffers(file) != 0;
    }
    bool published = ok && config_posix_rename_handle(file, wide_path);
    if (file != INVALID_HANDLE_VALUE && !CloseHandle(file)) {
        ok = false;
    }
    if (ok && !published) {
        /* Error-driven fallback, not a version probe: POSIX rename needs
         * NTFS-class filesystem support, so exFAT/SMB-homed configs land
         * here, keeping the pre-POSIX behavior (replace can fail while a
         * reader holds the destination open). */
        published = MoveFileExW(temporary, wide_path,
                                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) != 0;
    }
    ok = ok && published;
    if (!ok && temporary) {
        (void)DeleteFileW(temporary);
    }
    free(temporary);
    free(wide_path);
    return ok;
}
#else
static bool config_write_all(int descriptor, const char *json, size_t json_length) {
    size_t offset = 0;
    while (offset < json_length) {
        ssize_t written = write(descriptor, json + offset, json_length - offset);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return false;
        }
        offset += (size_t)written;
    }
    return true;
}

static bool config_sync_descriptor(int descriptor) {
    int result;
    do {
        result = fsync(descriptor);
    } while (result != 0 && errno == EINTR);
    return result == 0;
}

static bool config_sync_parent_directory(const char *path) {
    char directory[CBM_SZ_1K];
    if (!config_parent_directory(path, directory, sizeof(directory))) {
        return false;
    }
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    int descriptor = open(directory, flags);
    if (descriptor < 0) {
        return false;
    }
    bool synced = config_sync_descriptor(descriptor);
    int sync_error = synced ? 0 : errno;
    (void)close(descriptor);
#ifdef ENOTSUP
    if (!synced && sync_error == ENOTSUP) {
        synced = true;
    }
#endif
#ifdef EOPNOTSUPP
    if (!synced && sync_error == EOPNOTSUPP) {
        synced = true;
    }
#endif
    if (!synced && sync_error == EINVAL) {
        /* Some POSIX filesystems do not implement directory fsync. The temp
         * file itself was already synced and rename publication is atomic. */
        synced = true;
    }
    return synced;
}

static bool config_write_atomic(const char *path, const char *json, size_t json_length) {
    char temporary[CBM_SZ_2K];
    int written = snprintf(temporary, sizeof(temporary), "%s.tmp.XXXXXX", path);
    if (written <= 0 || (size_t)written >= sizeof(temporary)) {
        return false;
    }
    int descriptor = cbm_mkstemp(temporary);
    if (descriptor < 0) {
        return false;
    }
    bool ok = fchmod(descriptor, 0600) == 0;
    int descriptor_flags = fcntl(descriptor, F_GETFD);
    ok = ok && descriptor_flags >= 0 &&
         fcntl(descriptor, F_SETFD, descriptor_flags | FD_CLOEXEC) == 0;
    ok =
        ok && config_write_all(descriptor, json, json_length) && config_sync_descriptor(descriptor);
    if (close(descriptor) != 0) {
        ok = false;
    }
    if (ok) {
        ok = rename(temporary, path) == 0;
    }
    if (ok) {
        ok = config_sync_parent_directory(path);
    }
    if (!ok) {
        (void)cbm_unlink(temporary);
    }
    return ok;
}
#endif

bool cbm_ui_config_save(const cbm_ui_config_t *cfg) {
    if (!cfg || cfg->ui_port <= 0 || cfg->ui_port > 65535) {
        cbm_log_error("ui.config.write_fail", "reason", "invalid_config");
        return false;
    }
    char path[CBM_SZ_1K];
    cbm_ui_config_path(path, (int)sizeof(path));

    /* Ensure directory exists (recursive) */
    char dir[CBM_SZ_1K];
    bool directory_ready = config_parent_directory(path, dir, sizeof(dir));
    if (directory_ready && !cbm_is_dir(dir)) {
        directory_ready = cbm_mkdir_p(dir, 0750) || cbm_is_dir(dir);
    }
    if (!directory_ready) {
        cbm_log_error("ui.config.write_fail", "path", path, "reason", "create_directory");
        return false;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    bool serialized = doc && root;
    if (serialized) {
        yyjson_mut_doc_set_root(doc, root);
        serialized = yyjson_mut_obj_add_bool(doc, root, "ui_enabled", cfg->ui_enabled) &&
                     yyjson_mut_obj_add_int(doc, root, "ui_port", cfg->ui_port);
    }

    size_t json_len = 0;
    char *json = serialized ? yyjson_mut_write(doc, YYJSON_WRITE_PRETTY, &json_len) : NULL;
    if (doc) {
        yyjson_mut_doc_free(doc);
    }

    if (!json) {
        cbm_log_error("ui.config.write_fail", "reason", "serialize");
        return false;
    }

    bool saved = config_write_atomic(path, json, json_len);
    free(json);
    if (!saved) {
        cbm_log_error("ui.config.write_fail", "path", path, "reason", "atomic_publish");
        return false;
    }

    cbm_log_debug("ui.config.saved", "path", path);
    return true;
}
