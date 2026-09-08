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
#include <stdint.h>
#include <stdio.h>

/* ── Directory iteration ──────────────────────────────────────── */

/* Max filename length (MAX_PATH on Windows, NAME_MAX on POSIX). */
#define CBM_DIRENT_NAME_MAX 1024

typedef struct cbm_dir cbm_dir_t;

typedef struct {
    char name[CBM_DIRENT_NAME_MAX];
    bool is_dir;
    unsigned char d_type; /* DT_REG, DT_DIR, DT_LNK, etc. (POSIX only, 0 on Windows) */
} cbm_dirent_t;

/* Locale-independent metadata for a UTF-8 path. Symlinks/reparse points are
 * reported rather than followed so semantic-input walkers cannot leave the
 * repository through an alias. mtime_ns is Unix-epoch nanoseconds. */
typedef struct {
    bool is_regular;
    bool is_directory;
    bool is_symlink;
    int64_t size;
    int64_t mtime_ns;
} cbm_path_info_t;

enum {
    CBM_PATH_INFO_OK = 0,
    CBM_PATH_INFO_UNAVAILABLE = -1,
    CBM_PATH_INFO_ABSENT = -2,
};

/* Distinguishes a proven absent path from permission, encoding, and transient
 * inspection failures. Existing callers that only test zero/nonzero retain
 * their behavior. */
int cbm_path_info_utf8(const char *path, cbm_path_info_t *out);

/* Open a directory for iteration. Returns NULL on error. */
cbm_dir_t *cbm_opendir(const char *path);

/* Read next entry. Returns NULL when done. The returned pointer is
 * valid until the next cbm_readdir call on the same handle. */
cbm_dirent_t *cbm_readdir(cbm_dir_t *d);

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
/* Remove <db_path>-wal/-shm/-journal. MUST be called by any path installing a fresh
 * DB file where a previous generation lived — a leftover WAL is otherwise
 * replayed on top of the new file at the next open (#897). Returns 0 when
 * every artifact is absent, -1 when cleanup could not be safely completed. */
int cbm_remove_db_sidecars(const char *db_path);
/* rename() that replaces an existing destination on every platform
 * (Windows rename fails with EEXIST; this uses write-through MoveFileExW). */
int cbm_rename_replace(const char *src, const char *dst);

/* Copy src to dst (dst truncated/created), preferring an instant
 * copy-on-write clone where the filesystem supports one: clonefile(2) on
 * APFS, FICLONE on Linux reflink filesystems. Falls back to a streamed
 * copy. Returns 0 on success. The delta-repair path stages a multi-GB
 * database this way, so the clone fast path is the difference between
 * milliseconds and seconds there. */
int cbm_clone_or_copy_file(const char *src, const char *dst);
/* Move a regular file only when dst does not exist. Never overwrites dst.
 * Used for collision-safe evidence quarantine beside a database. */
int cbm_rename_noreplace(const char *src, const char *dst);
/* Canonicalize an EXISTING path and resolve links/junctions (realpath / wide
 * GetFinalPathNameByHandleW). Locale-independent on Windows — never routes
 * UTF-8 through the ANSI CRT (#973). out must be >= 4096 bytes. Returns 1 on
 * success, 0 otherwise. */
int cbm_canonical_path(const char *path, char *out, size_t out_sz);

/* Delete an empty directory. Returns 0 on success. */
int cbm_rmdir(const char *path);

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
