/*
 * test_helpers.h — Cross-platform test fixture helpers.
 *
 * Replaces system("rm -rf"), system("mkdir -p && echo >"), chmod(),
 * and hardcoded /tmp/ paths with portable C implementations.
 * Uses compat.h/compat_fs.h underneath — same abstraction layer
 * as production code.
 *
 * All functions are static inline to avoid linker issues when
 * included from multiple test files.
 */
#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/constants.h"
#include "../src/foundation/platform.h"

#include <stdio.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "../src/foundation/win_utf8.h"
#else
#include <fcntl.h>
#endif

/* ── Path building ────────────────────────────────────────────── */

/* Build a path from base + relative. Uses a static buffer — not reentrant.
 * Usage: th_write_file(TH_PATH(base, "src/main.go"), "content"); */
#define TH_PATH(base, rel) th_path_join(base, rel)

static inline const char *th_path_join(const char *base, const char *rel) {
    static char buf[4][1024];
    static int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(buf[i], sizeof(buf[i]), "%s/%s", base, rel);
    return buf[i];
}

/* ── File writing ─────────────────────────────────────────────── */

/* Write content to a file, creating parent directories as needed. */
static inline int th_write_file(const char *path, const char *content) {
    /* Create parent directories */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *last_slash = strrchr(dir, '/');
#ifdef _WIN32
    char *last_bslash = strrchr(dir, '\\');
    if (last_bslash && (!last_slash || last_bslash > last_slash)) {
        last_slash = last_bslash;
    }
#endif
    if (last_slash) {
        *last_slash = '\0';
        cbm_mkdir_p(dir, 0755);
    }

    FILE *f = cbm_fopen(path, "wb");
    if (!f) {
        return -1;
    }
    if (content && content[0]) {
        fputs(content, f);
    }
    fclose(f);
    return 0;
}

/* Append content to a file. */
static inline int th_append_file(const char *path, const char *content) {
    FILE *f = cbm_fopen(path, "ab");
    if (!f) {
        return -1;
    }
    if (content && content[0]) {
        fputs(content, f);
    }
    fclose(f);
    return 0;
}

/* Write a config row without public-setter validation. Tests use this only to
 * model retained databases from older builds or manual edits. */
static inline int th_set_raw_config_value(const char *cache_dir, const char *key,
                                          const char *value) {
    char path[CBM_PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/_config.db", cache_dir);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        return -1;
    }
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_open(path, &db);
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO config (key, value) VALUES (?1, ?2)",
                                -1, &stmt, NULL);
    }
    if (rc == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, value, -1, SQLITE_TRANSIENT);
        rc = sqlite3_step(stmt) == SQLITE_DONE ? SQLITE_OK : sqlite3_errcode(db);
    }
    sqlite3_finalize(stmt);
    if (db && sqlite3_close(db) != SQLITE_OK) {
        rc = SQLITE_BUSY;
    }
    return rc == SQLITE_OK ? 0 : -1;
}

/* ── Directory creation ───────────────────────────────────────── */

/* Create a directory and all parents. Returns 0 on success. */
static inline int th_mkdir_p(const char *path) {
    return cbm_mkdir_p(path, 0755) ? 0 : -1;
}

/* ── Recursive directory removal ──────────────────────────────── */

/* Git for Windows marks loose objects read-only. Match rm -rf semantics in
 * test cleanup without broadening production cbm_unlink behavior. */
static inline int th_unlink_force(const char *path) {
    int result = cbm_unlink(path);
#ifdef _WIN32
    if (result != 0) {
        wchar_t *wide = cbm_path_to_wide(path);
        DWORD attributes = wide ? GetFileAttributesW(wide) : INVALID_FILE_ATTRIBUTES;
        if (attributes != INVALID_FILE_ATTRIBUTES && (attributes & FILE_ATTRIBUTE_READONLY) != 0U &&
            SetFileAttributesW(wide, attributes & ~((DWORD)FILE_ATTRIBUTE_READONLY))) {
            result = cbm_unlink(path);
        }
        free(wide);
    }
#endif
    return result;
}

/* Remove a file or directory tree recursively. Cross-platform rm -rf. */
static inline int th_rmtree(const char *path) {
    /* The platform wrappers preserve UTF-8 and add the extended-length prefix
     * on Windows; narrow CRT stat() silently treats paths beyond MAX_PATH as
     * absent and leaves their parents nonempty. */
    if (!cbm_file_exists(path)) {
        return 0; /* doesn't exist — success */
    }

    if (!cbm_is_dir(path)) {
        return th_unlink_force(path);
    }

    /* Directory — recurse into children */
    cbm_dir_t *d = cbm_opendir(path);
    if (!d) {
        return -1;
    }

    cbm_dirent_t *entry;
    int rc = 0;
    while ((entry = cbm_readdir(d)) != NULL) {
        if (strcmp(entry->name, ".") == 0 || strcmp(entry->name, "..") == 0) {
            continue;
        }
        char child[1024];
        snprintf(child, sizeof(child), "%s/%s", path, entry->name);
        if (entry->is_dir) {
            if (th_rmtree(child) != 0) {
                rc = -1;
            }
        } else {
            if (th_unlink_force(child) != 0) {
                rc = -1;
            }
        }
    }
    cbm_closedir(d);
    if (cbm_rmdir(path) != 0) {
        rc = -1;
    }
    return rc;
}

/* Put a fixture's write time unambiguously before a cache created immediately
 * afterward. This avoids sleeps and remains deterministic on coarse-timestamp
 * filesystems used by containers and Windows test environments. */
static inline bool th_backdate_file_for_cache_test(const char *path) {
    enum {
        TH_CACHE_TIMESTAMP_SETTLE_SECONDS = 2,
        TH_WINDOWS_FILETIME_TICKS_PER_SECOND = 10000000,
    };
    if (!path) {
        return false;
    }
#ifdef _WIN32
    wchar_t *wide = cbm_path_to_wide(path);
    if (!wide) {
        return false;
    }
    HANDLE file = CreateFileW(wide, FILE_WRITE_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE |
                                                               FILE_SHARE_DELETE,
                              NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    free(wide);
    FILETIME now;
    GetSystemTimeAsFileTime(&now);
    uint64_t ticks = ((uint64_t)now.dwHighDateTime << 32U) | now.dwLowDateTime;
    uint64_t delta = (uint64_t)TH_CACHE_TIMESTAMP_SETTLE_SECONDS *
                     TH_WINDOWS_FILETIME_TICKS_PER_SECOND;
    bool ok = file != INVALID_HANDLE_VALUE && ticks > delta;
    if (ok) {
        ticks -= delta;
        FILETIME earlier = {
            .dwLowDateTime = (DWORD)ticks,
            .dwHighDateTime = (DWORD)(ticks >> 32U),
        };
        ok = SetFileTime(file, NULL, NULL, &earlier) != 0;
    }
    if (file != INVALID_HANDLE_VALUE && CloseHandle(file) == 0) {
        ok = false;
    }
    return ok;
#else
    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) != 0 ||
        now.tv_sec <= TH_CACHE_TIMESTAMP_SETTLE_SECONDS) {
        return false;
    }
    struct timespec times[2] = {
        {.tv_sec = now.tv_sec - TH_CACHE_TIMESTAMP_SETTLE_SECONDS, .tv_nsec = now.tv_nsec},
        {.tv_sec = now.tv_sec - TH_CACHE_TIMESTAMP_SETTLE_SECONDS, .tv_nsec = now.tv_nsec},
    };
    return utimensat(AT_FDCWD, path, times, 0) == 0;
#endif
}

/* ── Temp directory creation ──────────────────────────────────── */

/* Create a temporary directory. Returns static buffer with path.
 * Pattern: prefix is used as part of the dirname. */
static inline char *th_mktempdir(const char *prefix) {
    static char buf[256];
#ifdef _WIN32
    const char *tmp = getenv("TEMP");
    if (!tmp)
        tmp = getenv("TMP");
    if (!tmp)
        tmp = "C:\\Temp";
    snprintf(buf, sizeof(buf), "%s\\%s_XXXXXX", tmp, prefix);
#else
    snprintf(buf, sizeof(buf), "/tmp/%s_XXXXXX", prefix);
#endif
    if (!cbm_mkdtemp(buf)) {
        return NULL;
    }
    return buf;
}

/* Runtime IPC validates the complete Windows directory ancestry, so an MSYS2
 * TEMP rooted under C:/msys64/tmp is intentionally unsuitable even when the
 * leaf made by cbm_mkdtemp() has a private DACL. Keep these security-sensitive
 * fixtures under LocalAppData on Windows; preserve the ordinary temporary-root
 * behavior on POSIX. */
static inline bool th_secure_runtime_parent_new(char *out, size_t out_cap, const char *tag) {
    if (!out || out_cap == 0 || !tag || !tag[0]) {
        return false;
    }
#ifdef _WIN32
    const char *base = cbm_app_local_dir();
#else
    const char *base = cbm_tmpdir();
#endif
    if (!base || !base[0]) {
        out[0] = '\0';
        return false;
    }
    int written = snprintf(out, out_cap, "%s/cbm-runtime-%s-XXXXXX", base, tag);
    if (written <= 0 || (size_t)written >= out_cap) {
        out[0] = '\0';
        return false;
    }
    return cbm_mkdtemp(out) != NULL;
}

/* ── File permissions (no-op on Windows) ──────────────────────── */

/* Make a file executable. On Windows this is a no-op (all files are
 * "executable" if they have the right extension). */
static inline void th_make_executable(const char *path) {
#ifndef _WIN32
    chmod(path, 0755);
#else
    (void)path;
#endif
}

/* ── Cleanup helper ───────────────────────────────────────────── */

/* Remove a temp directory tree. Safe to call with NULL. */
static inline void th_cleanup(const char *path) {
    if (path && path[0]) {
        th_rmtree(path);
    }
}

#endif /* TEST_HELPERS_H */
