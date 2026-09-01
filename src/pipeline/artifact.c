/*
 * artifact.c — Persistent artifact export/import for team sharing.
 *
 * Export: strip indexes → VACUUM INTO temp → zstd compress → write .zst + metadata
 * Import: decompress → write to cache → open (auto-creates indexes) → integrity check
 */
#include "foundation/constants.h"

enum {
    ART_DIR_PERMS = 0755,
    ART_ZSTD_FAST = 3,
    ART_ZSTD_BEST = 9,
    ART_RATIO_SCALE = 10,        /* multiply ratio by 10 for integer logging */
    ART_NUL = 1,                 /* NUL terminator byte */
    ART_NS_PER_SEC = 1000000000, /* nanoseconds per second (mtime_ns helper) */
    ART_OID_SHA1_LEN = 40,       /* hex length of a SHA-1 git object id */
    ART_OID_SHA256_LEN = 64,     /* hex length of a SHA-256 git object id */
};
#define ART_BYTES_PER_MB ((size_t)1024 * 1024)

/* Generous ceiling on an imported artifact's decompressed size. Real indexes
 * (a full Linux-kernel DB is ~14 GB) fit comfortably; a frame that declares
 * more than this is rejected before any allocation so a crafted content size
 * can neither trigger a runaway allocation nor be used to desync the decoder
 * capacity from the destination buffer. */
#define ART_MAX_DECOMPRESSED_BYTES ((size_t)64 * 1024 * ART_BYTES_PER_MB)

#include "pipeline/artifact.h"
#include "store/store.h"
#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "foundation/compat.h"
#include "foundation/log.h"
#include "foundation/str_util.h"   /* cbm_validate_shell_arg — git shell-out hardening */
#include "foundation/hash_table.h" /* CBMHashTable — reconcile membership sets */

#include "zstd_store.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#ifdef _WIN32
#include <windows.h>
#include "foundation/win_utf8.h" /* cbm_path_to_wide */
#endif

/* ── Helpers ──────────────────────────────────────────────────────── */

/* Thread-local rotating buffers for small int→string conversions (logging).
 * Rotating allows multiple itoa_buf() calls in a single log statement. */
enum { ART_RING = 4, ART_RING_MASK = 3 };
static _Thread_local char g_export_error[CBM_SZ_512];

static const char *itoa_buf(int v) {
    static _Thread_local char bufs[ART_RING][CBM_SZ_32];
    static _Thread_local int idx = 0;
    int i = idx;
    idx = (idx + ART_NUL) & ART_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", v);
    return bufs[i];
}

/* Same rotating-buffer trick for 64-bit values. mtime_ns does not fit an int,
 * and a diagnostic that truncates the number it is there to explain is worse
 * than no diagnostic. */
static const char *i64_buf(int64_t v) {
    static _Thread_local char bufs[ART_RING][CBM_SZ_32];
    static _Thread_local int idx = 0;
    int i = idx;
    idx = (idx + ART_NUL) & ART_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%lld", (long long)v);
    return bufs[i];
}

const char *cbm_artifact_export_last_error(void) {
    return g_export_error[0] ? g_export_error : NULL;
}

static void clear_export_error(void) {
    g_export_error[0] = '\0';
}

static int artifact_export_fail(const char *stage, const char *path, const char *err, int err_no) {
    const char *safe_stage = stage ? stage : "unknown";
    const char *safe_err = err ? err : "unknown";

    if (path && err_no != 0) {
        snprintf(g_export_error, sizeof(g_export_error), "%s: %s errno=%d path=%s", safe_stage,
                 safe_err, err_no, path);
    } else if (path) {
        snprintf(g_export_error, sizeof(g_export_error), "%s: %s path=%s", safe_stage, safe_err,
                 path);
    } else if (err_no != 0) {
        snprintf(g_export_error, sizeof(g_export_error), "%s: %s errno=%d", safe_stage, safe_err,
                 err_no);
    } else {
        snprintf(g_export_error, sizeof(g_export_error), "%s: %s", safe_stage, safe_err);
    }

    if (path && err_no != 0) {
        cbm_log_error("artifact.export", "stage", safe_stage, "err", safe_err, "errno",
                      itoa_buf(err_no), "path", path);
    } else if (path) {
        cbm_log_error("artifact.export", "stage", safe_stage, "err", safe_err, "path", path);
    } else if (err_no != 0) {
        cbm_log_error("artifact.export", "stage", safe_stage, "err", safe_err, "errno",
                      itoa_buf(err_no));
    } else {
        cbm_log_error("artifact.export", "stage", safe_stage, "err", safe_err);
    }
    return CBM_NOT_FOUND;
}

typedef struct {
    const char *err;
    int err_no;
} artifact_file_error_t;

static void file_error_clear(artifact_file_error_t *out) {
    if (out) {
        out->err = NULL;
        out->err_no = 0;
    }
}

static void file_error_set(artifact_file_error_t *out, const char *err, int err_no) {
    if (out) {
        out->err = err;
        out->err_no = err_no;
    }
}

/* Build path: <repo>/.codebase-memory/<name> into caller-owned buf. */
static bool artifact_path(char *buf, size_t bufsz, const char *repo_path, const char *name) {
    int n = snprintf(buf, bufsz, "%s/%s/%s", repo_path, CBM_ARTIFACT_DIR, name);
    return n >= 0 && (size_t)n < bufsz;
}

/* Read entire file into malloc'd buffer. Sets *out_len. Returns NULL on error. */
static char *read_file_alloc(const char *path, size_t *out_len) {
    FILE *fp = cbm_fopen(path, "rb");
    if (!fp) {
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    if (sz <= 0) {
        (void)fclose(fp);
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_SET);
    char *buf = malloc((size_t)sz);
    if (!buf) {
        (void)fclose(fp);
        return NULL;
    }
    size_t rd = fread(buf, ART_NUL, (size_t)sz, fp);
    (void)fclose(fp);
    if ((long)rd != sz) {
        free(buf);
        return NULL;
    }
    *out_len = (size_t)sz;
    return buf;
}

/* Write buffer to file atomically (write to tmp, rename). Returns 0 on success. */
static int write_file_atomic(const char *path, const char *data, size_t len,
                             artifact_file_error_t *out_err) {
    file_error_clear(out_err);

    char tmp[CBM_SZ_4K];
    int n = snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    if (n < 0 || (size_t)n >= sizeof(tmp)) {
        file_error_set(out_err, "path_too_long", 0);
        return CBM_NOT_FOUND;
    }

    FILE *fp = fopen(tmp, "wb");
    if (!fp) {
        file_error_set(out_err, "open_temp", errno);
        return CBM_NOT_FOUND;
    }

    size_t wr = fwrite(data, ART_NUL, len, fp);
    if (wr != len) {
        int saved_errno = ferror(fp) ? errno : 0;
        (void)fclose(fp);
        cbm_unlink(tmp);
        file_error_set(out_err, "write_temp", saved_errno);
        return CBM_NOT_FOUND;
    }

    if (fclose(fp) != 0) {
        int saved_errno = errno;
        cbm_unlink(tmp);
        file_error_set(out_err, "close_temp", saved_errno);
        return CBM_NOT_FOUND;
    }

#ifdef _WIN32
    /* MoveFileEx replace approach suggested by @Ayush7Ranjan in #492. */
    if (!MoveFileExA(tmp, path, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        DWORD saved_error = GetLastError();
        cbm_unlink(tmp);
        file_error_set(out_err, "rename_temp", (int)saved_error);
        return CBM_NOT_FOUND;
    }
#else
    if (rename(tmp, path) != 0) {
        int saved_errno = errno;
        cbm_unlink(tmp);
        file_error_set(out_err, "rename_temp", saved_errno);
        return CBM_NOT_FOUND;
    }
#endif
    return 0;
}

#ifdef _WIN32
#define ARTIFACT_NULL_DEV "NUL"
#else
#define ARTIFACT_NULL_DEV "/dev/null"
#endif

/* See artifact.h. Mirrors git_context.c's git_validate_repo_path (the best-hardened
 * git shell-out): cbm_validate_shell_arg rejects quote / backslash / substitution
 * metacharacters, and on Windows we also reject the cmd.exe expansion metacharacters
 * % ! ^. Callers then use DOUBLE quotes (honored by both POSIX sh and cmd.exe, unlike
 * single quotes on cmd.exe), so a repo path may legitimately contain spaces. */
bool cbm_artifact_repo_path_is_shell_safe(const char *repo_path) {
    return cbm_validate_shell_path_arg(repo_path);
}

/* Get current git HEAD hash. buf must be >= CBM_SZ_64. Returns false on error. */
static bool git_head_hash(const char *repo_path, char *buf, size_t bufsz) {
    char cmd[CBM_SZ_1K];
    if (!cbm_artifact_repo_path_is_shell_safe(repo_path)) {
        buf[0] = '\0';
        return false;
    }
    int n =
        snprintf(cmd, sizeof(cmd), "git -C \"%s\" rev-parse HEAD 2>" ARTIFACT_NULL_DEV, repo_path);
    if (n < 0 || (size_t)n >= sizeof(cmd)) {
        buf[0] = '\0'; /* truncated command → don't run a malformed shell string (parity with
                          git_context.c) */
        return false;
    }
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        buf[0] = '\0';
        return false;
    }
    buf[0] = '\0';
    if (fgets(buf, (int)bufsz, fp)) {
        /* Strip trailing newline */
        size_t len = strlen(buf);
        while (len > 0 && (buf[len - ART_NUL] == '\n' || buf[len - ART_NUL] == '\r')) {
            buf[--len] = '\0';
        }
    }
    (void)cbm_pclose(fp);
    return buf[0] != '\0';
}

/* Generate ISO 8601 timestamp into buf. */
static void iso_timestamp(char *buf, size_t bufsz) {
    time_t now = time(NULL);
    struct tm tm;
#ifdef _WIN32
    gmtime_s(&tm, &now);
#else
    gmtime_r(&now, &tm);
#endif
    (void)strftime(buf, bufsz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

/* ── Git + trust helpers for bootstrap reconciliation ─────────────
 *
 * SHELL QUOTING RULE (see cbm_artifact_repo_path_is_shell_safe above): every
 * git command built here is run through cbm_popen, which on Windows executes
 * `cmd.exe /c <cmd>`. cmd.exe does NOT honor single quotes — it passes them
 * through to git as literal characters — so a single-quoted argument silently
 * becomes part of the value and the command misbehaves rather than failing
 * loudly. DOUBLE quotes are honored by both POSIX sh and cmd.exe and are the
 * only form used below. For the same reason no argument here contains `^`:
 * that is cmd.exe's escape character. */

/* Non-NULL sentinel value stored in a membership hash set (key presence is all
 * that matters; the value is never dereferenced). */
static int g_reconcile_sentinel;

/* Validate s is a hex git object id: 40 chars (SHA-1) or 64 chars (SHA-256).
 * Repo-controlled strings reach this check, so it is a hard gate before any
 * commit value is interpolated into a git command string. */
static bool is_hex_oid(const char *s) {
    if (!s) {
        return false;
    }
    size_t len = strlen(s);
    if (len != ART_OID_SHA1_LEN && len != ART_OID_SHA256_LEN) {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
            return false;
        }
    }
    return true;
}

/* Portable mtime in nanoseconds. MUST stay bit-identical to
 * pipeline_incremental.c's stat_mtime_ns: that is the function the incremental
 * classifier compares a restamped row against, and a differing encoding would
 * make every restamped row look changed. */
static int64_t art_stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * ART_NS_PER_SEC) + (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * ART_NS_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * ART_NS_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

/* Stat a path, refusing symlinks. Returns 0 on success, CBM_NOT_FOUND to skip.
 * Mirrors discover.c's safe_stat / pass_pkgmap.c's pkgmap_safe_stat.
 *
 * Symlinks are refused rather than followed because git tracks a symlink's
 * LINK TEXT, not its target's content: "unchanged" from git means the link
 * still points at the same name, which says nothing about the bytes the
 * indexer would actually parse. Following it would stamp the target's mtime
 * into the row and let a changed target read as unchanged. Discovery already
 * skips symlinks (discover.c safe_stat), so in practice no such row exists —
 * this keeps the invariant true even if that ever changes.
 *
 * On Windows the wide stat also keeps non-ASCII repo paths off the ANSI CRT
 * (the cbm_fopen rule applied to stat). */
static int reconcile_stat_no_symlink(const char *abs_path, struct stat *st) {
#ifdef _WIN32
    wchar_t *wpath = cbm_path_to_wide(abs_path);
    if (!wpath) {
        return CBM_NOT_FOUND;
    }
    DWORD attr = GetFileAttributesW(wpath);
    if (attr != INVALID_FILE_ATTRIBUTES && (attr & FILE_ATTRIBUTE_REPARSE_POINT)) {
        free(wpath);
        return CBM_NOT_FOUND; /* junction / symlink — same escape hatch */
    }
    struct _stat64 wst;
    int ret = _wstat64(wpath, &wst);
    free(wpath);
    if (ret != 0) {
        return CBM_NOT_FOUND;
    }
    st->st_mode = wst.st_mode;
    st->st_size = wst.st_size;
    st->st_mtime = wst.st_mtime;
    return 0;
#else
    if (lstat(abs_path, st) != 0) {
        return CBM_NOT_FOUND;
    }
    if (S_ISLNK(st->st_mode)) {
        return CBM_NOT_FOUND;
    }
    return 0;
#endif
}

/* Build `git -C "<repo>" <args> 2><null>` with a shell-validated repo path.
 * repo_path is the only untrusted component; args is a trusted literal or a
 * hex-validated commit string (never arbitrary data). Returns false on
 * shell-arg validation failure or truncation. */
static bool build_git_cmd(char *buf, size_t bufsz, const char *repo_path, const char *args) {
    if (!cbm_artifact_repo_path_is_shell_safe(repo_path)) {
        return false;
    }
    int n = snprintf(buf, bufsz, "git -C \"%s\" %s 2>" ARTIFACT_NULL_DEV, repo_path, args);
    return n >= 0 && (size_t)n < bufsz;
}

/* Run a git command; return true iff it exits 0. Output is drained (not kept)
 * so `diff --quiet` semantics work and large outputs don't SIGPIPE. */
static bool git_run_ok(const char *repo_path, const char *args) {
    char cmd[CBM_SZ_2K];
    if (!build_git_cmd(cmd, sizeof(cmd), repo_path, args)) {
        return false;
    }
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return false;
    }
    char drain[CBM_SZ_4K];
    while (fread(drain, 1, sizeof(drain), fp) > 0) {}
    return cbm_pclose(fp) == 0;
}

/* Run a git command and capture the FULL stdout (NUL bytes preserved) into a
 * growing malloc'd buffer. Empty output is success with *out_len = 0. Returns
 * 0 on success, CBM_NOT_FOUND on popen / non-zero-exit / OOM. Caller frees *out. */
static int git_capture_full(const char *repo_path, const char *args, char **out, size_t *out_len) {
    *out = NULL;
    *out_len = 0;
    char cmd[CBM_SZ_2K];
    if (!build_git_cmd(cmd, sizeof(cmd), repo_path, args)) {
        return CBM_NOT_FOUND;
    }
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }
    size_t cap = CBM_SZ_4K;
    size_t len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        (void)cbm_pclose(fp);
        return CBM_NOT_FOUND;
    }
    size_t n;
    while ((n = fread(buf + len, 1, cap - len, fp)) > 0) {
        len += n;
        if (len == cap) {
            size_t ncap = cap * PAIR_LEN;
            char *tmp = realloc(buf, ncap);
            if (!tmp) {
                free(buf);
                (void)cbm_pclose(fp);
                return CBM_NOT_FOUND;
            }
            buf = tmp;
            cap = ncap;
        }
    }
    int rc = cbm_pclose(fp);
    if (rc != 0) {
        free(buf);
        return CBM_NOT_FOUND;
    }
    *out = buf;
    *out_len = len;
    return 0;
}

/* True iff `commit` names an object that exists locally AND is a commit
 * (shallow/partial-clone guard).
 *
 * Deliberately `cat-file -t <oid>` rather than `cat-file -e <oid>^{commit}`:
 * the peel suffix contains `^`, which is cmd.exe's escape character. Quoting
 * cannot be relied on to neutralise it across both shells, and a mangled
 * argument here would silently disable reconciliation on Windows. `-t` needs
 * no metacharacters at all — the oid is already hex-validated. */
static bool git_commit_exists(const char *repo_path, const char *commit) {
    char args[CBM_SZ_128];
    int n = snprintf(args, sizeof(args), "cat-file -t %s", commit);
    if (n < 0 || (size_t)n >= sizeof(args)) {
        return false;
    }
    char *type = NULL;
    size_t type_len = 0;
    if (git_capture_full(repo_path, args, &type, &type_len) != 0) {
        return false;
    }
    static const char kCommit[] = "commit";
    size_t want = sizeof(kCommit) - ART_NUL;
    bool is_commit = type_len >= want && memcmp(type, kCommit, want) == 0 &&
                     (type_len == want || type[want] == '\n' || type[want] == '\r');
    free(type);
    return is_commit;
}

/* True iff the working tree has no tracked/staged/untracked changes outside
 * .codebase-memory/. The export itself writes .codebase-memory/, so a blanket
 * dirty check would always fail; excluding that subtree is what makes the
 * "clean export" invariant checkable.
 *
 * The `:(exclude)` pathspec is DOUBLE-quoted: its parentheses are shell
 * grouping metacharacters under POSIX sh and cmd.exe alike, and double quotes
 * are the one form both honor. (Single quotes here were the Windows bug: under
 * cmd.exe git received a literal leading `'`, no path ever matched the
 * pathspec, `diff --quiet` reported dirty, and the clean-basis marker was
 * never written on Windows.) */
static bool tree_clean_for_reconcile(const char *repo_path) {
    /* Tracked (staged + unstaged) changes vs HEAD, excluding .codebase-memory. */
    if (!git_run_ok(repo_path, "diff --quiet HEAD -- . \":(exclude).codebase-memory\"")) {
        return false;
    }
    /* Untracked (non-ignored) files outside .codebase-memory. */
    static const char *const ls_args =
        "ls-files -z --others --exclude-standard -- . \":(exclude).codebase-memory\"";
    char *untracked = NULL;
    size_t un_len = 0;
    if (git_capture_full(repo_path, ls_args, &untracked, &un_len) != 0) {
        return false;
    }
    bool clean = (un_len == 0);
    free(untracked);
    return clean;
}

/* True for file_hashes rows that are not repository files. The pipeline stores
 * synthetic "semantic input" digests under .codebase-memory/.semantic-input/
 * (pipeline_internal.h's CBM_SEMANTIC_INPUT_PREFIX), and the artifact itself
 * lives in the same directory. Neither is a plain file the indexer parsed, so
 * neither can be stat()ed or vouched for by git — find_deleted_files skips the
 * same class for the same reason.
 *
 * Testing the whole .codebase-memory/ subtree (rather than the semantic-input
 * prefix alone) keeps this in lockstep with the ":(exclude).codebase-memory"
 * pathspec used by the clean-tree probe: both halves of the trust check must
 * agree on what counts as a repository file, or export can never mark a basis
 * it would then refuse to act on. */
static bool reconcile_is_synthetic_row(const char *rel_path) {
    static const char prefix[] = CBM_ARTIFACT_DIR "/";
    return rel_path && strncmp(rel_path, prefix, sizeof(prefix) - ART_NUL) == 0;
}

/* True iff every file_hashes row for project has an on-disk file whose
 * mtime_ns + size matches the stored stamp. Any stat failure, path overflow,
 * or mismatch makes the artifact untrusted for reconciliation — this is the
 * belt-and-suspenders that catches a stale/swapped DB even when the tree looks
 * clean.
 *
 * The disk side MUST be read with cbm_path_info_utf8, because that is the
 * function that WROTE the rows being compared: the semantic manifest stamps
 * every row via pipeline_incremental.c's semantic_manifest_hash_file, which
 * calls cbm_path_info_utf8. On POSIX any lstat-based helper agrees with it,
 * but on Windows cbm_path_info_utf8 derives mtime_ns from ftLastWriteTime
 * (100-ns FILETIME ticks) while a _wstat64 st_mtime carries WHOLE SECONDS —
 * so a seconds-truncated re-stat can only ever equal the stored value when a
 * file's write time lands exactly on a second boundary. It essentially never
 * does, which made this check fail for every row on Windows and only on
 * Windows, suppressing the reconcile_basis marker there. Compare like with
 * like: same function on both sides, no encoding to keep in sync.
 *
 * cbm_path_info_utf8 reports symlinks/reparse points rather than following
 * them, so the is_symlink/is_regular guard below preserves exactly the
 * refusal reconcile_stat_no_symlink provides (git tracks a symlink's link
 * text, not its target's bytes). */
static bool db_hashes_match_disk(const char *repo_path, const char *db_path, const char *project,
                                 char *detail, size_t detail_sz) {
    snprintf(detail, detail_sz, "store_unreadable");
    cbm_store_t *s = cbm_store_open_path(db_path);
    if (!s) {
        return false;
    }
    cbm_file_hash_t *hashes = NULL;
    int count = 0;
    bool match = true;
    if (cbm_store_get_file_hashes(s, project, &hashes, &count) != CBM_STORE_OK) {
        snprintf(detail, detail_sz, "file_hashes_unreadable project=%s", project);
        cbm_store_close(s);
        return false;
    }
    detail[0] = '\0';
    for (int i = 0; i < count; i++) {
        if (reconcile_is_synthetic_row(hashes[i].rel_path)) {
            continue;
        }
        char abs[CBM_SZ_4K];
        int n = snprintf(abs, sizeof(abs), "%s/%s", repo_path, hashes[i].rel_path);
        if (n < 0 || n >= (int)sizeof(abs)) {
            match = false;
            snprintf(detail, detail_sz, "path=%s reason=path_too_long", hashes[i].rel_path);
            break;
        }
        cbm_path_info_t info;
        if (cbm_path_info_utf8(abs, &info) != 0 || !info.is_regular || info.is_symlink) {
            match = false;
            snprintf(detail, detail_sz, "path=%s reason=not_a_readable_regular_file",
                     hashes[i].rel_path);
            break;
        }
        if (info.mtime_ns != hashes[i].mtime_ns || info.size != hashes[i].size) {
            match = false;
            /* BOTH stamps are reported: a stale/swapped DB and a stamp-encoding
             * mismatch look identical at the boolean, and the numbers are the
             * only thing that separates them on a platform we cannot attach a
             * debugger to. */
            snprintf(detail, detail_sz,
                     "path=%s reason=%s stored_mtime_ns=%s disk_mtime_ns=%s stored_size=%s "
                     "disk_size=%s",
                     hashes[i].rel_path,
                     info.size != hashes[i].size ? "size_differs" : "mtime_differs",
                     i64_buf(hashes[i].mtime_ns), i64_buf(info.mtime_ns), i64_buf(hashes[i].size),
                     i64_buf(info.size));
            break;
        }
    }
    cbm_store_free_file_hashes(hashes, count);
    cbm_store_close(s);
    return match;
}

/* Read the optional reconcile_basis marker from artifact.json. True only when it
 * is exactly "git-clean-head" (the sole trusted basis this code emits). */
static bool read_metadata_reconcile_trusted(const char *repo_path) {
    char meta_path[CBM_SZ_4K];
    if (!artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META)) {
        return false;
    }
    size_t len = 0;
    char *json = read_file_alloc(meta_path, &len);
    if (!json) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(json, len, 0);
    free(json);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, "reconcile_basis");
    bool trusted = false;
    if (val) {
        const char *s = yyjson_get_str(val);
        trusted = (s && strcmp(s, "git-clean-head") == 0);
    }
    yyjson_doc_free(doc);
    return trusted;
}

/* Description of why the last export on THIS thread withheld the clean-basis
 * marker; empty when it wrote one. See cbm_artifact_reconcile_basis_last_blocker. */
static _Thread_local char g_basis_blocker[CBM_SZ_512];

const char *cbm_artifact_reconcile_basis_last_blocker(void) {
    return g_basis_blocker[0] ? g_basis_blocker : NULL;
}

/* Evaluate export's four clean-basis preconditions, recording the first one
 * that fails in g_basis_blocker. Returns true iff all four hold.
 *
 * Split out of cbm_artifact_export deliberately: as one short-circuiting &&
 * chain the four gates collapsed into a single bool, and a marker that went
 * missing on one platform gave no way to tell "HEAD unresolved" from "tree
 * dirty" from "DB does not match disk" — which is exactly how a Windows-only
 * stamp-encoding bug hid behind a suspected quoting bug for two diagnoses.
 * The reason is recorded rather than merely logged so a caller can quote it
 * verbatim: recomputing it after the fact reports a DIFFERENT gate, because
 * export's own ensure_gitattributes leaves an untracked .gitattributes behind
 * and every later evaluation then answers tree_not_clean. */
static bool reconcile_basis_trusted(const char *repo_path, const char *db_path,
                                    const char *project_name, const char *commit, bool has_commit) {
    g_basis_blocker[0] = '\0';
    if (!has_commit) {
        snprintf(g_basis_blocker, sizeof(g_basis_blocker), "head_unresolved");
        return false;
    }
    if (!is_hex_oid(commit)) {
        snprintf(g_basis_blocker, sizeof(g_basis_blocker), "head_not_hex_oid");
        return false;
    }
    if (!tree_clean_for_reconcile(repo_path)) {
        snprintf(g_basis_blocker, sizeof(g_basis_blocker), "tree_not_clean");
        return false;
    }
    char detail[CBM_SZ_256] = "";
    if (!db_hashes_match_disk(repo_path, db_path, project_name, detail, sizeof(detail))) {
        snprintf(g_basis_blocker, sizeof(g_basis_blocker), "db_hashes_differ_from_disk %s", detail);
        return false;
    }
    return true;
}

/* ── Metadata read/write ─────────────────────────────────────────── */

/* Read schema_version from artifact.json. Returns -1 if missing/invalid. */
static int read_metadata_version(const char *repo_path) {
    char meta_path[CBM_SZ_4K];
    artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META);

    size_t len = 0;
    char *json = read_file_alloc(meta_path, &len);
    if (!json) {
        return CBM_NOT_FOUND;
    }

    yyjson_doc *doc = yyjson_read(json, len, 0);
    free(json);
    if (!doc) {
        return CBM_NOT_FOUND;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *ver = yyjson_obj_get(root, "schema_version");
    int version = ver ? yyjson_get_int(ver) : CBM_NOT_FOUND;
    yyjson_doc_free(doc);
    return version;
}

/* Read original_size from artifact.json. Returns 0 on error. */
static size_t read_metadata_original_size(const char *repo_path) {
    char meta_path[CBM_SZ_4K];
    artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META);

    size_t len = 0;
    char *json = read_file_alloc(meta_path, &len);
    if (!json) {
        return 0;
    }

    yyjson_doc *doc = yyjson_read(json, len, 0);
    free(json);
    if (!doc) {
        return 0;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, "original_size");
    size_t result = val ? (size_t)yyjson_get_uint(val) : 0;
    yyjson_doc_free(doc);
    return result;
}

/* Write artifact.json metadata. */
static int write_metadata(const char *repo_path, const char *project_name, const char *commit,
                          int nodes, int edges, size_t original_size, size_t compressed_size,
                          int compression_level, bool reconcile_trusted) {
    char ts[CBM_SZ_64];
    iso_timestamp(ts, sizeof(ts));

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_int(doc, root, "schema_version", CBM_ARTIFACT_SCHEMA_VERSION);
    yyjson_mut_obj_add_str(doc, root, "commit", commit);
    yyjson_mut_obj_add_str(doc, root, "indexed_at", ts);
    yyjson_mut_obj_add_str(doc, root, "project", project_name);
    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);
    yyjson_mut_obj_add_uint(doc, root, "original_size", (uint64_t)original_size);
    yyjson_mut_obj_add_uint(doc, root, "compressed_size", (uint64_t)compressed_size);
    yyjson_mut_obj_add_int(doc, root, "compression_level", compression_level);
    /* Optional clean-basis marker: present only when export verified the DB
     * matches a clean checked-out tree at `commit` (see cbm_artifact_export).
     * Older binaries ignore this unknown field, so no schema_version bump. */
    if (reconcile_trusted) {
        yyjson_mut_obj_add_str(doc, root, "reconcile_basis", "git-clean-head");
    }

    size_t json_len = 0;
    char *json = yyjson_mut_write(doc, YYJSON_WRITE_PRETTY, &json_len);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return artifact_export_fail("write_metadata", NULL, "json_encode", 0);
    }

    char meta_path[CBM_SZ_4K];
    if (!artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META)) {
        free(json);
        return artifact_export_fail("write_metadata", repo_path, "path_too_long", 0);
    }
    artifact_file_error_t ioerr;
    int rc = write_file_atomic(meta_path, json, json_len, &ioerr);
    free(json);
    if (rc != 0) {
        return artifact_export_fail("write_metadata", meta_path, ioerr.err, ioerr.err_no);
    }
    return rc;
}

/* ── .gitattributes setup ────────────────────────────────────────── */

static void ensure_gitattributes(const char *repo_path) {
    char ga_path[CBM_SZ_4K];
    artifact_path(ga_path, sizeof(ga_path), repo_path, ".gitattributes");

    /* Atomic create-only-if-absent: O_EXCL closes the TOCTOU window
     * between checking existence and writing. If the file exists, open
     * fails with EEXIST and we leave it untouched. */
    int fd = open(ga_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
        if (errno != EEXIST) {
            cbm_log_warn("artifact.gitattributes.open path=%s err=%s", ga_path, strerror(errno));
        }
        /* fall through to merge driver setup either way */
    } else {
        FILE *fp = fdopen(fd, "w");
        if (fp) {
            /* Order matters: attributes apply left to right and the `binary`
             * macro expands to `-diff -merge -text`, so a trailing `binary`
             * unsets `merge=ours` and the conflict prevention this file
             * exists for never engages. The macro must come first. */
            (void)fputs("# Auto-generated by codebase-memory-mcp\n"
                        "# Prevent merge conflicts on compressed artifact\n" CBM_ARTIFACT_FILENAME
                        " binary merge=ours\n",
                        fp);
            (void)fclose(fp);
        } else {
            (void)close(fd);
        }
    }

    /* Best-effort: configure merge driver */
    if (!cbm_artifact_repo_path_is_shell_safe(repo_path)) {
        return;
    }
    char cmd[CBM_SZ_1K];
    int n = snprintf(cmd, sizeof(cmd),
                     "git -C \"%s\" config merge.ours.driver true 2>" ARTIFACT_NULL_DEV, repo_path);
    if (n < 0 || (size_t)n >= sizeof(cmd)) {
        return; /* truncated command → skip (parity with git_context.c) */
    }
    FILE *p = cbm_popen(cmd, "r");
    if (p) {
        (void)cbm_pclose(p);
    }
}

/* ── Index stripping ─────────────────────────────────────────────── */

/* SQL to drop all user-created indexes (not autoindexes, not FTS5). */
static const char *DROP_INDEXES_SQL = "DROP INDEX IF EXISTS idx_nodes_label;"
                                      "DROP INDEX IF EXISTS idx_nodes_name;"
                                      "DROP INDEX IF EXISTS idx_nodes_file;"
                                      "DROP INDEX IF EXISTS idx_edges_source;"
                                      "DROP INDEX IF EXISTS idx_edges_target;"
                                      "DROP INDEX IF EXISTS idx_edges_type;"
                                      "DROP INDEX IF EXISTS idx_edges_target_type;"
                                      "DROP INDEX IF EXISTS idx_edges_source_type;"
                                      "DROP INDEX IF EXISTS idx_edges_url_path;";

/* ── Export helpers ───────────────────────────────────────────────── */

/* Owner-private scratch directory holding the snapshot copy.
 *
 * VACUUM INTO refuses to write a destination that already exists, so the
 * destination file cannot be pre-created with exclusive semantics the way
 * cbm_mkstemp would — sqlite has to be the one that creates it. Containing it in
 * a directory only this user can enter buys the same protection: cbm_mkdtemp
 * creates with 0700 on POSIX and an explicit owner-only DACL on Windows, and the
 * XXXXXX suffix makes the path unguessable. The old fixed
 * "<tmp>/cbm_artifact_tmp.db" was vulnerable on both counts — another local user
 * could pre-plant a symlink there to redirect the copy, and two concurrent
 * exports collided on the one name. */
typedef struct {
    char dir[CBM_SZ_512]; /* cbm_mkdtemp copies its result back into this buffer */
    char db[CBM_SZ_4K];
} artifact_snapshot_tmp_t;

static bool artifact_snapshot_tmp_open(artifact_snapshot_tmp_t *tmp) {
    tmp->dir[0] = '\0';
    tmp->db[0] = '\0';
    int written = snprintf(tmp->dir, sizeof(tmp->dir), "%s/cbm-artifact-XXXXXX", cbm_tmpdir());
    if (written <= 0 || (size_t)written >= sizeof(tmp->dir) || !cbm_mkdtemp(tmp->dir)) {
        tmp->dir[0] = '\0';
        return false;
    }
    written = snprintf(tmp->db, sizeof(tmp->db), "%s/snapshot.db", tmp->dir);
    if (written <= 0 || (size_t)written >= sizeof(tmp->db)) {
        (void)cbm_rmdir(tmp->dir);
        tmp->dir[0] = '\0';
        return false;
    }
    return true;
}

/* Anchored cleanup — every exit from prepare_snapshot_db runs this, so neither
 * the copy nor its WAL/SHM sidecars outlive the export. Removing the directory
 * last doubles as the check that nothing was left inside it: rmdir only
 * succeeds once it is empty. */
static void artifact_snapshot_tmp_close(artifact_snapshot_tmp_t *tmp) {
    if (tmp->dir[0] == '\0') {
        return;
    }
    static const char *const suffixes[] = {"-wal", "-shm"};
    for (size_t i = 0; i < sizeof(suffixes) / sizeof(suffixes[0]); i++) {
        char sidecar[CBM_SZ_4K];
        int written = snprintf(sidecar, sizeof(sidecar), "%s%s", tmp->db, suffixes[i]);
        if (written > 0 && (size_t)written < sizeof(sidecar)) {
            (void)cbm_unlink(sidecar);
        }
    }
    (void)cbm_unlink(tmp->db);
    (void)cbm_rmdir(tmp->dir);
    tmp->dir[0] = '\0';
}

/* Prepare a stripped DB copy for best-quality export.
 * VACUUM INTO → (optionally) drop indexes → VACUUM. Returns malloc'd buffer
 * or NULL. VACUUM INTO runs on BOTH quality levels: it is the consistent
 * snapshot — the store runs in WAL mode, so raw main-file bytes miss
 * committed transactions still in the -wal and can be mid-checkpoint torn
 * (#895). Only the index-stripping is BEST-only. */
static char *prepare_snapshot_db(const char *db_path, size_t *out_size, bool strip_indexes) {
    artifact_snapshot_tmp_t tmp;
    if (!artifact_snapshot_tmp_open(&tmp)) {
        artifact_export_fail("prepare_snapshot_dir", cbm_tmpdir(), "private_tmpdir_failed", errno);
        return NULL;
    }
    /* Fresh private directory ⇒ the destination is absent by construction, which
     * is exactly what VACUUM INTO requires. The old unlink-the-stale-file step
     * is gone with the fixed name it existed to clear. */
    const char *tmp_path = tmp.db;

    /* VACUUM INTO: clean compacted copy. Use raw sqlite3 to bypass store authorizer
     * (which blocks ATTACH, used internally by VACUUM INTO). */
    sqlite3 *raw_db = NULL;
    if (sqlite3_open_v2(db_path, &raw_db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) {
        const char *err = raw_db ? sqlite3_errmsg(raw_db) : "sqlite_open";
        artifact_export_fail("open_source_db", db_path, err, 0);
        sqlite3_close(raw_db);
        artifact_snapshot_tmp_close(&tmp);
        return NULL;
    }

    char vacuum_sql[CBM_SZ_4K];
    snprintf(vacuum_sql, sizeof(vacuum_sql), "VACUUM INTO '%s';", tmp_path);
    char *errmsg = NULL;
    int vrc = sqlite3_exec(raw_db, vacuum_sql, NULL, NULL, &errmsg);
    sqlite3_close(raw_db);

    if (vrc != SQLITE_OK) {
        artifact_export_fail("vacuum_into", tmp_path, errmsg ? errmsg : sqlite3_errstr(vrc), 0);
        sqlite3_free(errmsg);
        artifact_snapshot_tmp_close(&tmp);
        return NULL;
    }

    /* Strip indexes from the copy for better compression (BEST only). */
    if (strip_indexes) {
        sqlite3 *tmp_db = NULL;
        if (sqlite3_open_v2(tmp_path, &tmp_db, SQLITE_OPEN_READWRITE, NULL) == SQLITE_OK) {
            sqlite3_exec(tmp_db, DROP_INDEXES_SQL, NULL, NULL, NULL);
            sqlite3_exec(tmp_db, "VACUUM;", NULL, NULL, NULL);
            sqlite3_close(tmp_db);
        }
    }

    /* Reopened by path rather than held on a descriptor across VACUUM INTO,
     * because sqlite owns the create. The private directory is what makes that
     * safe: an attacker who cannot enter it cannot swap the file underneath. */
    char *data = read_file_alloc(tmp_path, out_size);
    if (!data || *out_size == 0) {
        artifact_export_fail("read_stripped_db", tmp_path, "empty_or_unreadable", errno);
    }
    /* Removes the copy, its WAL/SHM sidecars, and the private directory. */
    artifact_snapshot_tmp_close(&tmp);
    return data;
}

/* ── Export ───────────────────────────────────────────────────────── */

int cbm_artifact_export(const char *db_path, const char *repo_path, const char *project_name,
                        int quality) {
    clear_export_error();

    if (!db_path || !repo_path || !project_name) {
        return artifact_export_fail("validate_args", NULL, "missing_argument", 0);
    }

    /* Ensure .codebase-memory/ directory exists */
    char art_dir[CBM_SZ_4K];
    int dir_len = snprintf(art_dir, sizeof(art_dir), "%s/%s", repo_path, CBM_ARTIFACT_DIR);
    if (dir_len < 0 || (size_t)dir_len >= sizeof(art_dir)) {
        return artifact_export_fail("prepare_artifact_dir", repo_path, "path_too_long", 0);
    }
    errno = 0;
    if (!cbm_mkdir_p(art_dir, ART_DIR_PERMS)) {
        return artifact_export_fail("prepare_artifact_dir", art_dir, "mkdir_or_not_directory",
                                    errno);
    }
    if (!cbm_is_dir(art_dir)) {
        return artifact_export_fail("prepare_artifact_dir", art_dir, "not_directory", 0);
    }

    size_t db_size = 0;
    char *db_data = NULL;
    int compression_level = ART_ZSTD_FAST;

    if (quality == CBM_ARTIFACT_BEST) {
        compression_level = ART_ZSTD_BEST;
        db_data = prepare_snapshot_db(db_path, &db_size, true);
    } else {
        /* FAST keeps zstd-3 and its indexes, but still snapshots via
         * VACUUM INTO: the raw main-file bytes of a live WAL store are a
         * torn copy (#895). */
        db_data = prepare_snapshot_db(db_path, &db_size, false);
    }

    if (!db_data || db_size == 0) {
        free(db_data);
        if (cbm_artifact_export_last_error()) {
            return CBM_NOT_FOUND;
        }
        return artifact_export_fail("read_db", db_path, "empty_or_unreadable", errno);
    }

    /* Compress with zstd */
    size_t bound = cbm_zstd_compress_bound(db_size);
    char *compressed = malloc(bound);
    if (!compressed) {
        free(db_data);
        return artifact_export_fail("compress", NULL, "alloc_compressed_buffer", 0);
    }

    int64_t clen = cbm_zstd_compress(db_data, db_size, compressed, bound, compression_level);
    free(db_data);

    if (clen <= 0) {
        free(compressed);
        return artifact_export_fail("compress", NULL, "zstd_compress", 0);
    }

    /* Write compressed artifact */
    char zst_path[CBM_SZ_4K];
    if (!artifact_path(zst_path, sizeof(zst_path), repo_path, CBM_ARTIFACT_FILENAME)) {
        free(compressed);
        return artifact_export_fail("write_artifact", repo_path, "path_too_long", 0);
    }
    artifact_file_error_t ioerr;
    int wrc = write_file_atomic(zst_path, compressed, (size_t)clen, &ioerr);
    free(compressed);

    if (wrc != 0) {
        return artifact_export_fail("write_artifact", zst_path, ioerr.err, ioerr.err_no);
    }

    /* Get node/edge counts for metadata */
    int nodes = 0;
    int edges = 0;
    cbm_store_t *count_store = cbm_store_open_path_query(db_path);
    if (count_store) {
        nodes = cbm_store_count_nodes(count_store, project_name);
        edges = cbm_store_count_edges(count_store, project_name);
        cbm_store_close(count_store);
    }

    /* Compute the optional clean-basis trust marker. An imported DB can be
     * fast-reconciled against git only if export can prove it was built from a
     * clean checked-out tree at a known commit. Any doubt omits the marker and
     * bootstrap falls back to today's slow-safe full re-parse. */
    char commit[CBM_SZ_64] = "";
    bool has_commit = git_head_hash(repo_path, commit, sizeof(commit));
    bool reconcile_trusted =
        reconcile_basis_trusted(repo_path, db_path, project_name, commit, has_commit);
    if (!reconcile_trusted) {
        cbm_log_info("artifact.reconcile_basis_omitted", "reason", g_basis_blocker);
    }

    /* Write metadata */
    if (write_metadata(repo_path, project_name, commit, nodes, edges, db_size, (size_t)clen,
                       compression_level, reconcile_trusted) != 0) {
        cbm_unlink(zst_path);
        return CBM_NOT_FOUND;
    }

    /* Ensure .gitattributes for merge conflict prevention */
    ensure_gitattributes(repo_path);

    double ratio = db_size > 0 ? (double)db_size / (double)clen : 0.0;
    cbm_log_info("artifact.export", "quality", quality == CBM_ARTIFACT_BEST ? "best" : "fast",
                 "original_mb", itoa_buf((int)(db_size / ART_BYTES_PER_MB)), "compressed_mb",
                 itoa_buf((int)((size_t)clen / ART_BYTES_PER_MB)), "ratio",
                 itoa_buf((int)(ratio * ART_RATIO_SCALE)));

    return 0;
}

/* ── Import ──────────────────────────────────────────────────────── */

int cbm_artifact_import(const char *repo_path, const char *cache_db_path) {
    if (!repo_path || !cache_db_path) {
        return CBM_NOT_FOUND;
    }

    /* Check schema version compatibility */
    int version = read_metadata_version(repo_path);
    if (version < 0 || version > CBM_ARTIFACT_SCHEMA_VERSION) {
        cbm_log_info("artifact.import", "skip", "schema_version_mismatch", "artifact_ver",
                     itoa_buf(version), "current_ver", itoa_buf(CBM_ARTIFACT_SCHEMA_VERSION));
        return CBM_NOT_FOUND;
    }

    /* Get original_size for decompression buffer */
    size_t original_size = read_metadata_original_size(repo_path);
    if (original_size == 0) {
        cbm_log_error("artifact.import", "err", "missing_original_size");
        return CBM_NOT_FOUND;
    }

    /* Read compressed artifact */
    char zst_path[CBM_SZ_4K];
    artifact_path(zst_path, sizeof(zst_path), repo_path, CBM_ARTIFACT_FILENAME);

    size_t clen = 0;
    char *compressed = read_file_alloc(zst_path, &clen);
    if (!compressed) {
        cbm_log_error("artifact.import", "err", "read_artifact");
        return CBM_NOT_FOUND;
    }

    /* Decompress */
    /* Size the destination from the zstd frame's own content-size header, not
     * from the separately-stored (attacker-controllable) original_size field.
     * The allocation and the decoder capacity are then the SAME size_t value,
     * so a crafted size can never make the capacity exceed the real buffer
     * (the int-truncation that used to do exactly that is gone with the size_t
     * signature). Require the metadata field to agree, and cap the total. */
    size_t frame_size = cbm_zstd_frame_content_size(compressed, clen);
    if (frame_size == 0 || frame_size > ART_MAX_DECOMPRESSED_BYTES || frame_size != original_size) {
        free(compressed);
        cbm_log_error("artifact.import", "err", "bad_decompressed_size");
        return CBM_NOT_FOUND;
    }

    char *decompressed = malloc(frame_size);
    if (!decompressed) {
        free(compressed);
        return CBM_NOT_FOUND;
    }

    int64_t dlen = cbm_zstd_decompress(compressed, clen, decompressed, frame_size);
    free(compressed);

    if (dlen <= 0 || (size_t)dlen != frame_size) {
        free(decompressed);
        cbm_log_error("artifact.import", "err", "zstd_decompress");
        return CBM_NOT_FOUND;
    }

    /* Write to temp file, then rename for atomicity */
    char tmp_path[CBM_SZ_4K];
    snprintf(tmp_path, sizeof(tmp_path), "%s.import_tmp", cache_db_path);

    /* Ensure cache directory exists */
    char cache_dir[CBM_SZ_1K];
    snprintf(cache_dir, sizeof(cache_dir), "%s", cache_db_path);
    char *last_slash = strrchr(cache_dir, '/');
    if (last_slash) {
        *last_slash = '\0';
        cbm_mkdir_p(cache_dir, ART_DIR_PERMS);
    }

    artifact_file_error_t ioerr;
    int wrc = write_file_atomic(tmp_path, decompressed, (size_t)dlen, &ioerr);
    free(decompressed);

    if (wrc != 0) {
        if (ioerr.err_no != 0) {
            cbm_log_error("artifact.import", "err", "write_temp_db", "detail", ioerr.err, "errno",
                          itoa_buf(ioerr.err_no), "path", tmp_path);
        } else {
            cbm_log_error("artifact.import", "err", "write_temp_db", "detail", ioerr.err, "path",
                          tmp_path);
        }
        return CBM_NOT_FOUND;
    }

    /* Open with cbm_store_open_path to auto-create missing indexes + FTS5 */
    cbm_store_t *store = cbm_store_open_path(tmp_path);
    if (!store) {
        cbm_log_error("artifact.import", "err", "open_imported_db");
        cbm_unlink(tmp_path);
        return CBM_NOT_FOUND;
    }

    /* Deep integrity check — refuse corrupted artifacts. The shallow check
     * only sanity-checks the projects table, so page-corrupted (torn)
     * artifacts installed cleanly (#895); quick_check catches them. */
    if (!cbm_store_check_integrity_deep(store)) {
        cbm_log_error("artifact.import", "err", "integrity_check_failed");
        cbm_store_close(store);
        cbm_unlink(tmp_path);
        return CBM_NOT_FOUND;
    }

    cbm_store_close(store);

    /* Atomic rename to final path. Drop the DESTINATION's leftover
     * -wal/-shm first: the import cleans the tmp file's sidecars, but a
     * stale WAL next to the cache path would be replayed on top of the
     * imported file at the next open (#897). */
    cbm_remove_db_sidecars(cache_db_path);
    if (rename(tmp_path, cache_db_path) != 0) {
        cbm_log_error("artifact.import", "err", "rename_to_cache");
        cbm_unlink(tmp_path);
        return CBM_NOT_FOUND;
    }

    /* Clean up any stale WAL/SHM from the temp open */
    char wal[CBM_SZ_4K];
    char shm[CBM_SZ_4K];
    snprintf(wal, sizeof(wal), "%s-wal", tmp_path);
    snprintf(shm, sizeof(shm), "%s-shm", tmp_path);
    cbm_unlink(wal);
    cbm_unlink(shm);

    cbm_log_info("artifact.import", "db", cache_db_path, "size_mb",
                 itoa_buf((int)((size_t)dlen / ART_BYTES_PER_MB)));

    return 0;
}

/* ── Existence check ─────────────────────────────────────────────── */

bool cbm_artifact_exists(const char *repo_path) {
    if (!repo_path) {
        return false;
    }

    char zst_path[CBM_SZ_4K];
    artifact_path(zst_path, sizeof(zst_path), repo_path, CBM_ARTIFACT_FILENAME);

    struct stat st;
    if (stat(zst_path, &st) != 0 || st.st_size == 0) {
        return false;
    }

    /* Check schema version is compatible */
    int version = read_metadata_version(repo_path);
    return version >= 0 && version <= CBM_ARTIFACT_SCHEMA_VERSION;
}

/* ── Commit hash extraction ──────────────────────────────────────── */

char *cbm_artifact_commit(const char *repo_path) {
    if (!repo_path) {
        return NULL;
    }

    char meta_path[CBM_SZ_4K];
    artifact_path(meta_path, sizeof(meta_path), repo_path, CBM_ARTIFACT_META);

    size_t len = 0;
    char *json = read_file_alloc(meta_path, &len);
    if (!json) {
        return NULL;
    }

    yyjson_doc *doc = yyjson_read(json, len, 0);
    free(json);
    if (!doc) {
        return NULL;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, "commit");
    char *result = NULL;
    if (val) {
        const char *s = yyjson_get_str(val);
        if (s && s[0]) {
            size_t slen = strlen(s);
            result = malloc(slen + ART_NUL);
            if (result) {
                memcpy(result, s, slen + ART_NUL);
            }
        }
    }
    yyjson_doc_free(doc);
    return result;
}

/* ── Bootstrap reconciliation ─────────────────────────────────────── */

/* The three git outputs plus the two membership sets built from them.
 * The sets BORROW their keys from the buffers, so the tables must be freed
 * before the buffers — reconcile_sets_free() is the single place that
 * ordering is expressed. */
typedef struct {
    char *diff_out;
    size_t diff_len;
    char *ls_out;
    size_t ls_len;
    char *tracked_out;
    size_t tracked_len;
    CBMHashTable *changed; /* paths modified since the artifact commit */
    CBMHashTable *tracked; /* paths tracked AT the artifact commit */
} reconcile_sets_t;

static void reconcile_sets_free(reconcile_sets_t *s) {
    /* Tables FIRST: every key points into the buffers freed just below. */
    cbm_ht_free(s->changed);
    cbm_ht_free(s->tracked);
    s->changed = NULL;
    s->tracked = NULL;
    free(s->diff_out);
    free(s->ls_out);
    free(s->tracked_out);
    s->diff_out = NULL;
    s->ls_out = NULL;
    s->tracked_out = NULL;
}

/* Add every NUL-delimited entry in buf (length len) to the membership set ht.
 * git -z output NUL-terminates every entry including the last, so each non-empty
 * entry buf+i is already a C string terminated by its trailing NUL. Keys are
 * borrowed from buf (the table does not copy keys), so buf must outlive ht.
 * Empty entries (consecutive NULs) are skipped.
 *
 * Returns false if ANY entry failed to land. cbm_ht_set discards the return of
 * the underlying insert (hash_table.c), so an allocation failure drops an entry
 * SILENTLY — and a dropped entry in `changed` means a genuinely modified file
 * reads as unchanged and gets restamped, i.e. a silently stale graph. Presence
 * is therefore verified per entry and any failure aborts the whole
 * reconciliation (fail closed). */
static bool reconcile_add_nul_entries(CBMHashTable *ht, const char *buf, size_t len) {
    if (!ht) {
        return false;
    }
    if (!buf || len == 0) {
        return true;
    }
    size_t i = 0;
    while (i < len) {
        const char *entry = buf + i;
        size_t j = i;
        while (j < len && buf[j] != '\0') {
            j++;
        }
        if (j > i) {
            cbm_ht_set(ht, entry, &g_reconcile_sentinel);
            if (!cbm_ht_has(ht, entry)) {
                return false;
            }
        }
        i = (j < len) ? j + 1 : len;
    }
    return true;
}

/* Capture the git outputs and build the changed/tracked membership sets.
 * Returns true only if every step succeeded; on false the caller must still
 * call reconcile_sets_free (partial state is freed correctly). */
static bool reconcile_sets_build(const char *repo_path, const char *commit,
                                 reconcile_sets_t *sets) {
    /* diff: content changed between the artifact commit and the working tree.
     * ls-files --others: untracked files (git cannot vouch for them).
     * ls-tree: the paths tracked AT the artifact commit — the eligibility set.
     *
     * NUL-delimited (-z) output is parsed directly; line-oriented parsing
     * cannot handle paths containing newlines or quotes. The tracked set
     * exists because git diff/ls-files are blind to files git IGNORES: a
     * gitignored-yet-indexed file (a .cbmignore negation un-skipping a
     * generated dir, #500) would otherwise be restamped as "unchanged" while
     * its content is not under git's control at all. */
    char diff_args[CBM_SZ_256];
    char lstree_args[CBM_SZ_256];
    int dn =
        snprintf(diff_args, sizeof(diff_args), "diff -z --name-only --no-renames %s --", commit);
    int ln = snprintf(lstree_args, sizeof(lstree_args), "ls-tree -r -z --name-only %s --", commit);
    if (dn < 0 || (size_t)dn >= sizeof(diff_args) || ln < 0 || (size_t)ln >= sizeof(lstree_args)) {
        return false;
    }
    if (git_capture_full(repo_path, diff_args, &sets->diff_out, &sets->diff_len) != 0) {
        return false;
    }
    if (git_capture_full(repo_path, "ls-files -z --others --exclude-standard --", &sets->ls_out,
                         &sets->ls_len) != 0) {
        return false;
    }
    if (git_capture_full(repo_path, lstree_args, &sets->tracked_out, &sets->tracked_len) != 0) {
        return false;
    }

    /* Parse-invariant guard: git -z NUL-terminates every entry including the
     * last; a non-empty buffer not ending in NUL means truncated/corrupt output
     * → untrusted → skip rather than risk misclassifying a changed file as
     * unchanged (graph corruption). */
    if ((sets->diff_len > 0 && sets->diff_out[sets->diff_len - ART_NUL] != '\0') ||
        (sets->ls_len > 0 && sets->ls_out[sets->ls_len - ART_NUL] != '\0') ||
        (sets->tracked_len > 0 && sets->tracked_out[sets->tracked_len - ART_NUL] != '\0')) {
        return false;
    }

    /* cbm_ht_create returns NULL on OOM. Checking BOTH is load-bearing: with a
     * NULL `changed` and a live `tracked`, every tracked row would look
     * unchanged and get restamped — including genuinely modified files. Note
     * the polarity difference from classify_files, which uses the same
     * primitive where a dropped entry merely means "re-parse" (safe); here it
     * means "unchanged" (unsafe). Fail closed. */
    sets->changed = cbm_ht_create(CBM_SZ_64);
    sets->tracked = cbm_ht_create(CBM_SZ_64);
    if (!sets->changed || !sets->tracked) {
        return false;
    }
    return reconcile_add_nul_entries(sets->changed, sets->diff_out, sets->diff_len) &&
           reconcile_add_nul_entries(sets->changed, sets->ls_out, sets->ls_len) &&
           reconcile_add_nul_entries(sets->tracked, sets->tracked_out, sets->tracked_len);
}

/* Restamp every row that is tracked at the artifact commit and not in the
 * changed set, with local stat() values. Returns the number of rows restamped,
 * or CBM_NOT_FOUND if the store could not be read/written. */
static int reconcile_restamp_rows(const char *repo_path, const char *cache_db_path,
                                  const char *project_name, const reconcile_sets_t *sets,
                                  int *out_skipped) {
    *out_skipped = 0;
    cbm_store_t *store = cbm_store_open_path(cache_db_path);
    if (!store) {
        return CBM_NOT_FOUND;
    }
    cbm_file_hash_t *stored = NULL;
    int stored_count = 0;
    if (cbm_store_get_file_hashes(store, project_name, &stored, &stored_count) != CBM_STORE_OK) {
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }
    if (stored_count <= 0) {
        cbm_store_free_file_hashes(stored, stored_count);
        cbm_store_close(store);
        return 0;
    }
    cbm_file_hash_t *batch = malloc((size_t)stored_count * sizeof(*batch));
    if (!batch) {
        cbm_store_free_file_hashes(stored, stored_count);
        cbm_store_close(store);
        return CBM_NOT_FOUND;
    }

    int batch_n = 0;
    int skipped = 0;
    for (int i = 0; i < stored_count; i++) {
        if (reconcile_is_synthetic_row(stored[i].rel_path)) {
            continue; /* synthetic semantic input — not a repository file */
        }
        if (!cbm_ht_get(sets->tracked, stored[i].rel_path) ||
            cbm_ht_get(sets->changed, stored[i].rel_path)) {
            continue; /* untracked or changed → stays foreign → re-parsed */
        }
        char abs[CBM_SZ_4K];
        int n = snprintf(abs, sizeof(abs), "%s/%s", repo_path, stored[i].rel_path);
        if (n < 0 || n >= (int)sizeof(abs)) {
            skipped++;
            continue;
        }
        struct stat st;
        if (reconcile_stat_no_symlink(abs, &st) != 0) {
            skipped++;
            continue; /* missing locally → find_deleted_files purges the row */
        }
        /* Borrow project from the caller and rel_path/sha256 from the row; all
         * outlive the batch upsert below, which is issued before `stored` is
         * freed. */
        batch[batch_n].project = project_name;
        batch[batch_n].rel_path = stored[i].rel_path;
        batch[batch_n].sha256 = stored[i].sha256;
        /* art_stat_mtime_ns, NOT cbm_path_info_utf8 (which db_hashes_match_disk
         * uses): the two disagree on Windows and each side must match ITS OWN
         * consumer. A restamped row exists to be read by the incremental
         * classifier (pipeline_incremental.c classify_files) and by
         * check_index_coverage (mcp.c coverage_path_freshness) — both stat()
         * the file and encode whole seconds on Windows. Stamping a 100-ns
         * FILETIME value here would make every restamped row read as changed
         * there and reconcile would buy nothing. Do not unify these two call
         * sites without changing those consumers first. */
        batch[batch_n].mtime_ns = art_stat_mtime_ns(&st);
        batch[batch_n].size = (int64_t)st.st_size;
        batch_n++;
    }

    int restamped = 0;
    if (batch_n > 0) {
        restamped = (cbm_store_upsert_file_hash_batch(store, batch, batch_n) == CBM_STORE_OK)
                        ? batch_n
                        : CBM_NOT_FOUND;
    }
    free(batch);
    cbm_store_free_file_hashes(stored, stored_count);
    cbm_store_close(store);
    *out_skipped = skipped;
    return restamped;
}

/* See artifact.h for the contract and the trust trade-off this implements.
 *
 * Windows/autocrlf note: on-disk bytes may differ from the exporter's while git
 * reports "unchanged"; line numbers and parse results are equivalent, so
 * re-stamping by git's diff is correct. */
int cbm_artifact_reconcile_hashes(const char *repo_path, const char *cache_db_path,
                                  const char *project_name) {
    if (!repo_path || !cache_db_path || !project_name) {
        return CBM_NOT_FOUND;
    }

    /* 1. The producer-written clean-basis marker must be present. This alone is
     *    NOT trust (see artifact.h) — it only opens the door to the git checks
     *    below, each of which is evaluated against the LOCAL repository. */
    if (!read_metadata_reconcile_trusted(repo_path)) {
        return CBM_NOT_FOUND;
    }
    char *commit = cbm_artifact_commit(repo_path);
    if (!commit || !is_hex_oid(commit)) {
        /* Hard gate: repo-controlled metadata never reaches a git command
         * unless it is pure hex. */
        free(commit);
        return CBM_NOT_FOUND;
    }

    /* 2. That commit must exist locally as a commit (shallow-clone guard). */
    if (!git_commit_exists(repo_path, commit)) {
        free(commit);
        return CBM_NOT_FOUND;
    }

    /* 3. Build the changed / tracked-at-commit sets from git. */
    reconcile_sets_t sets = {0};
    bool built = reconcile_sets_build(repo_path, commit, &sets);
    free(commit);
    if (!built) {
        reconcile_sets_free(&sets);
        cbm_log_info("artifact.reconcile_skipped", "reason", "git_sets_unavailable");
        return CBM_NOT_FOUND;
    }

    /* 4. Restamp the eligible rows. */
    int skipped = 0;
    int restamped = reconcile_restamp_rows(repo_path, cache_db_path, project_name, &sets, &skipped);
    int changed_count = (int)cbm_ht_count(sets.changed);
    reconcile_sets_free(&sets);

    if (restamped < 0) {
        cbm_log_info("artifact.reconcile_skipped", "reason", "store_unavailable");
        return CBM_NOT_FOUND;
    }
    cbm_log_info("artifact.reconcile", "restamped", itoa_buf(restamped), "changed",
                 itoa_buf(changed_count), "skipped", itoa_buf(skipped));
    return restamped;
}
