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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

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

    FILE *f = fopen(path, "w");
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
    FILE *f = fopen(path, "a");
    if (!f) {
        return -1;
    }
    if (content && content[0]) {
        fputs(content, f);
    }
    fclose(f);
    return 0;
}

/* ── Directory creation ───────────────────────────────────────── */

/* Create a directory and all parents. Returns 0 on success. */
static inline int th_mkdir_p(const char *path) {
    return cbm_mkdir_p(path, 0755) ? 0 : -1;
}

/* ── Recursive directory removal ──────────────────────────────── */

/* Remove a file or directory tree recursively. Cross-platform rm -rf. */
static inline int th_rmtree(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        return 0; /* doesn't exist — success */
    }

    if (!S_ISDIR(st.st_mode)) {
        return cbm_unlink(path);
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
            if (cbm_unlink(child) != 0) {
                rc = -1;
            }
        }
    }
    cbm_closedir(d);
    cbm_rmdir(path);
    return rc;
}

/* ── Temp directory creation ──────────────────────────────────── */

/* Create a temporary directory. Returns static buffer with path.
 * Pattern: prefix is used as part of the dirname. */
static inline char *th_mktempdir(const char *prefix) {
    static char buf[256];
#ifdef _WIN32
    const char *tmp = getenv("TEMP");
    if (!tmp) tmp = getenv("TMP");
    if (!tmp) tmp = "C:\\Temp";
    snprintf(buf, sizeof(buf), "%s\\%s_XXXXXX", tmp, prefix);
#else
    snprintf(buf, sizeof(buf), "/tmp/%s_XXXXXX", prefix);
#endif
    if (!cbm_mkdtemp(buf)) {
        return NULL;
    }
    return buf;
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
