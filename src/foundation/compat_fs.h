/*
 * compat_fs.h — Portable directory iteration, popen, and file operations.
 *
 * POSIX: thin wrappers around opendir/readdir, popen/pclose, mkdir, unlink.
 * Windows: FindFirstFile/FindNextFile, _popen/_pclose, _mkdir, _unlink.
 */
#ifndef CBM_COMPAT_FS_H
#define CBM_COMPAT_FS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

#include "foundation/constants.h"

/* ── Directory iteration ──────────────────────────────────────── */

/* Max filename length (MAX_PATH on Windows, NAME_MAX on POSIX). */
#define CBM_DIRENT_NAME_MAX (CBM_SZ_256 + CBM_SZ_4)

typedef struct cbm_dir cbm_dir_t;

typedef struct {
    char name[CBM_DIRENT_NAME_MAX];
    bool is_dir;
    unsigned char d_type; /* DT_REG, DT_DIR, DT_LNK, etc. (POSIX only, 0 on Windows) */
} cbm_dirent_t;

/* Open a directory for iteration. Returns NULL on error. */
cbm_dir_t *cbm_opendir(const char *path);

/* Read next entry. Returns NULL when done. The returned pointer is
 * valid until the next cbm_readdir call on the same handle. */
cbm_dirent_t *cbm_readdir(cbm_dir_t *d);

/* True when name can be represented exactly in cbm_dirent_t.name. */
bool cbm_dirent_name_fits(const char *name);

/* Close directory handle. */
void cbm_closedir(cbm_dir_t *d);

/* ── Portable popen/pclose ────────────────────────────────────── */

FILE *cbm_popen(const char *cmd, const char *mode);
int cbm_pclose(FILE *f);

/* ── File operations ──────────────────────────────────────────── */

/* Create directory (and parents). mode is ignored on Windows. Returns true on success. */
bool cbm_mkdir_p(const char *path, int mode);

/* Delete a file. Returns 0 on success. */
int cbm_unlink(const char *path);
/* Remove <db_path>-wal/-shm. MUST be called by any path installing a fresh
 * DB file where a previous generation lived — a leftover WAL is otherwise
 * replayed on top of the new file at the next open (#897). */
void cbm_remove_db_sidecars(const char *db_path);
/* rename() that replaces an existing destination on every platform
 * (Windows rename fails with EEXIST; this uses MoveFileExW there). */
int cbm_rename_replace(const char *src, const char *dst);
/* Canonicalize an EXISTING path (realpath / wide GetFullPathNameW). Locale-
 * independent on Windows — never routes UTF-8 through the ANSI CRT (#973).
 * out must be >= 4096 bytes. Returns 1 on success, 0 otherwise. */
int cbm_canonical_path(const char *path, char *out, size_t out_sz);

/* Delete an empty directory. Returns 0 on success. */
int cbm_rmdir(const char *path);

/* Atomically replace dest_path with tmp_path when the platform supports it.
 * tmp_path must already contain the complete new file. Returns 0 on success.
 * POSIX: rename(). Windows: MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH). */
int cbm_replace_file(const char *tmp_path, const char *dest_path);

/* Move src_path to dest_path only when dest_path does not already exist.
 * Returns 0 on success and leaves src_path in place on destination conflicts.
 * Best for sibling temp/quarantine paths: POSIX uses link()+unlink(), so a
 * cross-device move fails instead of silently copying or replacing. */
int cbm_move_file_no_replace(const char *src_path, const char *dest_path);

/* Same as cbm_replace_file(), but returns the platform-native failure code via
 * platform_error: errno on POSIX, GetLastError() on Windows. */
int cbm_replace_file_ex(const char *tmp_path, const char *dest_path, int *platform_error);

typedef struct {
    const char *stage; /* path_too_long, open_temp, write_temp, close_temp, rename_temp */
    int code;          /* errno/GetLastError() when available, or 0 for validation errors */
} cbm_atomic_file_error_t;

/* Write data to a unique temp sibling, close it, then atomically replace dest_path.
 * Binary-safe. Returns 0 on success and fills out_err on failure when provided. */
int cbm_write_file_atomic(const char *dest_path, const void *data, size_t len,
                          cbm_atomic_file_error_t *out_err);

/* Open a file by UTF-8 path.
 * On Windows, converts to wide-char and calls _wfopen so paths with
 * non-ASCII characters (accents, CJK, etc.) are handled correctly.
 * On POSIX, delegates to fopen. mode must be an ASCII string. */
FILE *cbm_fopen(const char *path, const char *mode);

/* Execute a command without shell interpretation.
 * argv is a NULL-terminated array: {"cmd", "arg1", "arg2", NULL}.
 * Returns the process exit code, or -1 on fork/exec failure.
 * POSIX: fork() + execvp(). Windows: CreateProcess with proper quoting. */
int cbm_exec_no_shell(const char *const *argv);

#endif /* CBM_COMPAT_FS_H */
