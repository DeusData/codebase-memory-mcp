/*
 * config_edit_path.c — follow a config symlink only when the agent-config
 * writer opted in, the invoking user owns both the link and its regular-file
 * target, and the target's parent directory can be pinned. See the header
 * for the decision, the sequence and its boundaries (#1954).
 */
#include "cli/config_edit_path.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

enum { EDIT_PATH_MAX_ROOTS = 4 };

static int edit_path_copy(const char *source, char *out, size_t out_size) {
    int written = snprintf(out, out_size, "%s", source);
    return written >= 0 && (size_t)written < out_size ? 0 : -1;
}

static void edit_path_target_reset(cbm_config_edit_target_t *target) {
    target->status = CBM_CONFIG_EDIT_PATH_DIRECT;
    target->dirfd = -1;
    target->fd = -1;
    target->base[0] = '\0';
    target->path[0] = '\0';
}

#ifndef _WIN32
/* Only the POSIX symlink-follow resolver records a refusal reason; the Windows
 * build never follows, so guard this with the same condition as its callers to
 * avoid -Werror,-Wunused-function (#2110 CI). */
static void edit_path_note(char *reason, size_t reason_size, const char *text) {
    if (reason && reason_size > 0U) {
        (void)snprintf(reason, reason_size, "%s", text);
    }
}
#endif /* !_WIN32 */

void cbm_config_edit_target_close(cbm_config_edit_target_t *target) {
    if (!target) {
        return;
    }
    int saved_errno = errno;
#ifndef _WIN32
    if (target->fd >= 0) {
        (void)close(target->fd);
    }
    if (target->dirfd >= 0) {
        (void)close(target->dirfd);
    }
#endif
    target->fd = -1;
    target->dirfd = -1;
    errno = saved_errno;
}

#ifndef _WIN32

/* The opted-in follow-roots are per-thread (test isolation), but the storage
 * is a heap buffer behind a thread-local POINTER, not a thread-local array.
 * A `_Thread_local char[4][4096]` sits in the binary's static TLS block, and
 * glibc carves that block out of every thread's stack allocation — including
 * the parent-death watchdog's deliberately tiny 64 KiB stack (main.c). The
 * 16 KiB array pushed the watchdog's pthread_create over the static-TLS +
 * guard budget, so it failed with EINVAL on glibc/x86-64 (green on macOS,
 * whose TLS is not stack-carved): #2110 CI. The pointer keeps the static TLS
 * block at 8 bytes; the buffer is allocated on first add and freed by clear. */
static CBM_TLS char (*edit_path_roots)[CBM_CONFIG_EDIT_PATH_MAX] = NULL;
static CBM_TLS size_t edit_path_root_count = 0U;

#ifdef CBM_CONFIG_EDIT_PATH_ENABLE_TEST_API
static CBM_TLS int edit_path_uid_override_set = 0;
static CBM_TLS uid_t edit_path_uid_override = 0;
static CBM_TLS int edit_path_identity_skew = 0;

void cbm_config_edit_path_set_invoking_uid_for_test(unsigned uid, int set) {
    edit_path_uid_override_set = set != 0;
    edit_path_uid_override = (uid_t)uid;
}

void cbm_config_edit_path_set_identity_skew_for_test(int set) {
    edit_path_identity_skew = set != 0;
}
#endif

static uid_t edit_path_invoking_uid(void) {
#ifdef CBM_CONFIG_EDIT_PATH_ENABLE_TEST_API
    if (edit_path_uid_override_set) {
        return edit_path_uid_override;
    }
#endif
    return geteuid();
}

int cbm_config_edit_path_follow_add_root(const char *root) {
    if (!root || !root[0] || edit_path_root_count >= EDIT_PATH_MAX_ROOTS) {
        return -1;
    }
    char canonical[CBM_CONFIG_EDIT_PATH_MAX];
    if (!cbm_canonical_path(root, canonical, sizeof(canonical))) {
        return -1;
    }
    if (!edit_path_roots) {
        edit_path_roots = calloc(EDIT_PATH_MAX_ROOTS, sizeof(*edit_path_roots));
        if (!edit_path_roots) {
            return -1;
        }
    }
    if (edit_path_copy(canonical, edit_path_roots[edit_path_root_count],
                       sizeof(edit_path_roots[0])) != 0) {
        return -1;
    }
    edit_path_root_count++;
    return 0;
}

void cbm_config_edit_path_follow_clear(void) {
    edit_path_root_count = 0U;
    free(edit_path_roots);
    edit_path_roots = NULL;
}

static int edit_path_under_root(const char *canonical) {
    for (size_t i = 0U; i < edit_path_root_count; i++) {
        const char *root = edit_path_roots[i];
        size_t root_length = strlen(root);
        if (strcmp(canonical, root) == 0) {
            return 1;
        }
        if (strncmp(canonical, root, root_length) == 0 &&
            (canonical[root_length] == '/' || root[root_length - 1U] == '/')) {
            return 1;
        }
    }
    return 0;
}

/* Splits a canonical absolute path into its parent and its final name. */
static int edit_path_split(const char *canonical, char *parent, size_t parent_size, char *base,
                           size_t base_size) {
    const char *slash = strrchr(canonical, '/');
    if (!slash || slash[1] == '\0') {
        return -1;
    }
    size_t parent_length = slash == canonical ? 1U : (size_t)(slash - canonical);
    if (parent_length >= parent_size || edit_path_copy(slash + 1, base, base_size) != 0) {
        return -1;
    }
    memcpy(parent, canonical, parent_length);
    parent[parent_length] = '\0';
    return 0;
}

/* The link's OWN directory must sit under a registered root: that is what
 * makes the follow an opt-in of the agent-configuration writers rather than
 * a property of every editor caller. */
static int edit_path_link_dir_under_root(const char *path) {
    if (edit_path_root_count == 0U) {
        return 0;
    }
    char parent[CBM_CONFIG_EDIT_PATH_MAX];
    if (edit_path_copy(path, parent, sizeof(parent)) != 0) {
        return 0;
    }
    char *slash = strrchr(parent, '/');
    if (!slash) {
        parent[0] = '.';
        parent[1] = '\0';
    } else if (slash == parent) {
        parent[1] = '\0';
    } else {
        *slash = '\0';
    }
    char canonical[CBM_CONFIG_EDIT_PATH_MAX];
    if (!cbm_canonical_path(parent, canonical, sizeof(canonical))) {
        return 0;
    }
    return edit_path_under_root(canonical);
}

static void edit_path_note_owner(char *reason, size_t reason_size, const char *what,
                                 unsigned long owner, unsigned long self) {
    if (reason && reason_size > 0U) {
        (void)snprintf(reason, reason_size, "%s owned by uid %lu, not the invoking user (uid %lu)",
                       what, owner, self);
    }
}

/* One classification serves both the resolver and the diagnostic, so the
 * reason a user reads is exactly the rule that refused the link. On REFUSED
 * every descriptor opened along the way is closed again. */
static int edit_path_classify(const char *path, cbm_config_edit_target_t *target, char *reason,
                              size_t reason_size) {
    edit_path_target_reset(target);
    if (reason && reason_size > 0U) {
        reason[0] = '\0';
    }
    if (!path || !path[0]) {
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    struct stat link_state;
    if (lstat(path, &link_state) != 0 || !S_ISLNK(link_state.st_mode)) {
        return edit_path_copy(path, target->path, sizeof(target->path)) == 0
                   ? CBM_CONFIG_EDIT_PATH_DIRECT
                   : CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    if (!edit_path_link_dir_under_root(path)) {
        edit_path_note(reason, reason_size,
                       edit_path_root_count == 0U
                           ? "symlinks are not followed by this command"
                           : "symlink outside the opted-in configuration roots");
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    uid_t self = edit_path_invoking_uid();
    if (self == 0 && link_state.st_uid != 0) {
        if (reason && reason_size > 0U) {
            (void)snprintf(reason, reason_size,
                           "symlink owned by uid %lu is not followed by a root process",
                           (unsigned long)link_state.st_uid);
        }
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    if (link_state.st_uid != self) {
        edit_path_note_owner(reason, reason_size, "symlink", (unsigned long)link_state.st_uid,
                             (unsigned long)self);
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    char resolved[CBM_CONFIG_EDIT_PATH_MAX];
    struct stat target_state;
    if (!cbm_canonical_path(path, resolved, sizeof(resolved)) ||
        stat(resolved, &target_state) != 0) {
        edit_path_note(reason, reason_size, "dangling symlink");
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    if (!S_ISREG(target_state.st_mode)) {
        edit_path_note(reason, reason_size,
                       S_ISDIR(target_state.st_mode) ? "symlink to a directory"
                                                     : "symlink to a special file");
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    if (target_state.st_uid != self) {
        edit_path_note_owner(reason, reason_size, "symlink target",
                             (unsigned long)target_state.st_uid, (unsigned long)self);
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
#if CBM_CONFIG_EDIT_TARGET_UNDER_ROOT
    if (!edit_path_under_root(resolved)) {
        edit_path_note(reason, reason_size, "symlink target outside the configuration roots");
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
#endif
    char parent[CBM_CONFIG_EDIT_PATH_MAX];
    if (edit_path_split(resolved, parent, sizeof(parent), target->base, sizeof(target->base)) !=
            0 ||
        edit_path_copy(resolved, target->path, sizeof(target->path)) != 0) {
        edit_path_note(reason, reason_size, "symlink target path is too long");
        edit_path_target_reset(target);
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }

    int dir_flags = O_RDONLY | O_NOFOLLOW;
#ifdef O_DIRECTORY
    dir_flags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
    dir_flags |= O_CLOEXEC;
#endif
    target->dirfd = open(parent, dir_flags);
    struct stat dir_state;
    if (target->dirfd < 0 || fstat(target->dirfd, &dir_state) != 0 || !S_ISDIR(dir_state.st_mode)) {
        edit_path_note(reason, reason_size,
                       "parent directory of the symlink target cannot be opened");
        cbm_config_edit_target_close(target);
        edit_path_target_reset(target);
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    if (dir_state.st_uid != self && dir_state.st_uid != 0) {
        if (reason && reason_size > 0U) {
            (void)snprintf(reason, reason_size,
                           "parent directory of the symlink target is owned by uid %lu",
                           (unsigned long)dir_state.st_uid);
        }
        cbm_config_edit_target_close(target);
        edit_path_target_reset(target);
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    if ((dir_state.st_mode & S_IWOTH) != 0 && (dir_state.st_mode & S_ISVTX) == 0) {
        edit_path_note(reason, reason_size,
                       "parent directory of the symlink target is writable by others");
        cbm_config_edit_target_close(target);
        edit_path_target_reset(target);
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }

    int file_flags = O_RDONLY | O_NOFOLLOW | O_NONBLOCK;
#ifdef O_CLOEXEC
    file_flags |= O_CLOEXEC;
#endif
    target->fd = openat(target->dirfd, target->base, file_flags);
    struct stat opened_state;
    if (target->fd < 0 || fstat(target->fd, &opened_state) != 0) {
        edit_path_note(reason, reason_size, "symlink target cannot be opened beside its parent");
        cbm_config_edit_target_close(target);
        edit_path_target_reset(target);
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    const char *mismatch = NULL;
    if (!S_ISREG(opened_state.st_mode)) {
        mismatch = "symlink target is not a regular file";
    } else if (opened_state.st_uid != self) {
        mismatch = "symlink target changed owner while it was being checked";
    } else if (opened_state.st_nlink != 1U) {
        mismatch = "symlink target is hard-linked";
    } else if (opened_state.st_dev != target_state.st_dev ||
               opened_state.st_ino != target_state.st_ino) {
        mismatch = "symlink target changed while it was being checked";
    }
#ifdef CBM_CONFIG_EDIT_PATH_ENABLE_TEST_API
    if (!mismatch && edit_path_identity_skew) {
        mismatch = "symlink target changed while it was being checked";
    }
#endif
    if (mismatch) {
        edit_path_note(reason, reason_size, mismatch);
        cbm_config_edit_target_close(target);
        edit_path_target_reset(target);
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    target->status = CBM_CONFIG_EDIT_PATH_FOLLOWED;
    return CBM_CONFIG_EDIT_PATH_FOLLOWED;
}

int cbm_config_edit_target_create_temp(const cbm_config_edit_target_t *target, const char *name,
                                       unsigned mode) {
    if (!target || target->status != CBM_CONFIG_EDIT_PATH_FOLLOWED || target->dirfd < 0 || !name ||
        !name[0] || strchr(name, '/')) {
        errno = EINVAL;
        return -1;
    }
    int flags = O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    return openat(target->dirfd, name, flags, (mode_t)mode);
}

int cbm_config_edit_target_commit(const cbm_config_edit_target_t *target, const char *name) {
    if (!target || target->status != CBM_CONFIG_EDIT_PATH_FOLLOWED || target->dirfd < 0 || !name ||
        !name[0] || strchr(name, '/')) {
        errno = EINVAL;
        return -1;
    }
    if (renameat(target->dirfd, name, target->dirfd, target->base) != 0) {
        return -1;
    }
    return fsync(target->dirfd) == 0 ? 0 : -1;
}

int cbm_config_edit_target_unlink(const cbm_config_edit_target_t *target, const char *name) {
    if (!target || target->status != CBM_CONFIG_EDIT_PATH_FOLLOWED || target->dirfd < 0 || !name ||
        !name[0] || strchr(name, '/')) {
        errno = EINVAL;
        return -1;
    }
    return unlinkat(target->dirfd, name, 0);
}

#else /* _WIN32 */

#ifdef CBM_CONFIG_EDIT_PATH_ENABLE_TEST_API
void cbm_config_edit_path_set_invoking_uid_for_test(unsigned uid, int set) {
    (void)uid;
    (void)set;
}

void cbm_config_edit_path_set_identity_skew_for_test(int set) {
    (void)set;
}
#endif

int cbm_config_edit_path_follow_add_root(const char *root) {
    (void)root;
    return 0;
}

void cbm_config_edit_path_follow_clear(void) {}

/* Reparse points are never followed by the editors on Windows; the path is
 * handed back unchanged and FILE_FLAG_OPEN_REPARSE_POINT keeps refusing. */
static int edit_path_classify(const char *path, cbm_config_edit_target_t *target, char *reason,
                              size_t reason_size) {
    edit_path_target_reset(target);
    if (reason && reason_size > 0U) {
        reason[0] = '\0';
    }
    if (!path || !path[0]) {
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    return edit_path_copy(path, target->path, sizeof(target->path)) == 0
               ? CBM_CONFIG_EDIT_PATH_DIRECT
               : CBM_CONFIG_EDIT_PATH_REFUSED;
}

int cbm_config_edit_target_create_temp(const cbm_config_edit_target_t *target, const char *name,
                                       unsigned mode) {
    (void)target;
    (void)name;
    (void)mode;
    errno = EINVAL;
    return -1;
}

int cbm_config_edit_target_commit(const cbm_config_edit_target_t *target, const char *name) {
    (void)target;
    (void)name;
    errno = EINVAL;
    return -1;
}

int cbm_config_edit_target_unlink(const cbm_config_edit_target_t *target, const char *name) {
    (void)target;
    (void)name;
    errno = EINVAL;
    return -1;
}

#endif /* _WIN32 */

int cbm_config_edit_target_open(const char *path, cbm_config_edit_target_t *target) {
    if (!target) {
        return CBM_CONFIG_EDIT_PATH_REFUSED;
    }
    int status = edit_path_classify(path, target, NULL, 0U);
    target->status = status;
    return status;
}

int cbm_config_edit_path_refusal(const char *path, char *out, size_t out_size) {
    if (!out || out_size == 0U) {
        return 0;
    }
    cbm_config_edit_target_t target;
    (void)edit_path_classify(path, &target, out, out_size);
    cbm_config_edit_target_close(&target);
    return out[0] != '\0' ? 1 : 0;
}
