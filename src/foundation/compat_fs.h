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
#include <sys/stat.h>

#include "foundation/constants.h"

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

/* Returns 0 on success and -1 when the path cannot be inspected. */
int cbm_path_info_utf8(const char *path, cbm_path_info_t *out);

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
/* Close/reap a popen stream and return a platform-independent child exit code. */
int cbm_pclose_exit_code(FILE *f);

/* ── File operations ──────────────────────────────────────────── */

/* Read metadata for a UTF-8 filesystem path. POSIX delegates to stat();
 * Windows converts once to UTF-16 and uses _wstat64 so non-ASCII and extended
 * paths never pass through the ANSI CRT. The Windows conversion costs O(P)
 * runtime and O(P) transient memory for P path bytes; POSIX remains O(1)
 * wrapper overhead. Returns 0 on success and -1 with errno preserved on
 * failure. */
int cbm_stat(const char *path, struct stat *out);

enum {
    CBM_FILE_CONTENT_HASH_HEX_LEN = (int)(sizeof(uint64_t) * PAIR_LEN),
    CBM_FILE_CONTENT_HASH_BUFSZ = CBM_FILE_CONTENT_HASH_HEX_LEN + 1,
};

/* Compute the canonical XXH3-64 content hash used by file-state and dirty-file
 * metadata. out must provide CBM_FILE_CONTENT_HASH_BUFSZ bytes. */
int cbm_file_content_hash(const char *path, char *out, size_t out_sz);

/* Stable identity of one filesystem object. This distinguishes atomic path
 * replacement from in-place metadata changes and is valid across processes. */
typedef struct {
    uint64_t volume;
    uint64_t file;
    bool valid;
} cbm_file_identity_t;

/* Read the object identity currently named by path. Returns false when the
 * path is missing or its identity cannot be read. */
bool cbm_file_identity_read(const char *path, cbm_file_identity_t *out);
bool cbm_file_identity_equal(const cbm_file_identity_t *left, const cbm_file_identity_t *right);

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
/* Canonicalize an existing path into exact heap storage. Caller frees. */
char *cbm_canonical_path_alloc(const char *path);

/* Delete an empty directory. Returns 0 on success. */
int cbm_rmdir(const char *path);

/* Atomically replace dest_path with tmp_path when the platform supports it.
 * tmp_path must already contain the complete new file. Returns 0 on success.
 * POSIX: one atomic name replacement. Windows: write-through MoveFileExW on
 * the ordinary path, with a handle-based FileRenameInfoEx fallback when an
 * open reader retains the previous destination generation. */
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
