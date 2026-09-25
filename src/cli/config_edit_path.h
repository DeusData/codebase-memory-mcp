/*
 * config_edit_path.h — the path a configuration editor may operate on when
 * the path it was given is a symlink.
 *
 * Every config editor (JSON-like, TOML, YAML, text) opens its file with
 * O_NOFOLLOW and refuses symlinks: a link planted at ~/.cursor/mcp.json could
 * otherwise point a privileged writer at any file its planter can name. Users,
 * however, keep exactly those files as symlinks into a dotfiles checkout, and
 * a blanket refusal turned `uninstall` into data loss (#1954).
 *
 * Decision C (2026-09-07): a symlink the INVOKING user owns, resolving to a
 * regular file the same user owns, is followed — but only as an explicit
 * OPT-IN of the agent-configuration writers, scoped to configuration roots
 * (HOME / XDG dirs) they register with cbm_config_edit_path_follow_add_root().
 * With no root registered the editors behave exactly as before; a path whose
 * link lives outside every registered root is never followed, so repository
 * paths stay refused whatever the caller.
 *
 * Race-safe sequence for a followed link (POSIX):
 *   lstat(link) → link owner == invoking uid (a root process follows only
 *   root-owned links) → realpath → open the target's PARENT DIRECTORY as a
 *   dirfd (O_DIRECTORY|O_NOFOLLOW; owned by the invoking user or root, not
 *   writable by others unless sticky) → openat(dirfd, base, O_NOFOLLOW) →
 *   fstat: regular file, owned by the invoking user, link count 1, and the
 *   same (st_dev, st_ino) as stat(realpath).
 * Everything else is refused with an observable reason
 * (cbm_config_edit_path_refusal): another owner, a root process over a
 * non-root link, a dangling link, a link to a directory or special file, a
 * hard-linked or swapped target, a foreign or world-writable parent. The
 * editor then reads through the validated descriptor, stages its temp file
 * with openat(dirfd, …, O_CREAT|O_EXCL) and commits with renameat on the same
 * dirfd, so the link itself is never replaced. Windows never follows reparse
 * points here: the editors keep FILE_FLAG_OPEN_REPARSE_POINT and the helper
 * hands the path back unchanged.
 *
 * The check runs at every open and before every commit, never cached: a link
 * swapped mid-edit resolves to a different identity, which also fails the
 * editors' own pre-publish snapshot comparison on the resolved path.
 */
#ifndef CBM_CONFIG_EDIT_PATH_H
#define CBM_CONFIG_EDIT_PATH_H

#include <stddef.h>

enum {
    /* Callers pass a buffer of at least this size (cbm_canonical_path's floor). */
    CBM_CONFIG_EDIT_PATH_MAX = 4096,
    CBM_CONFIG_EDIT_NAME_MAX = 256,
};

/* Pending user decision (#1954): must the resolved TARGET also stay under a
 * registered root (1), or may it live anywhere the invoking user owns, such
 * as a dotfiles checkout on an external volume (0, the default)? One switch. */
#ifndef CBM_CONFIG_EDIT_TARGET_UNDER_ROOT
#define CBM_CONFIG_EDIT_TARGET_UNDER_ROOT 0
#endif

typedef enum {
    CBM_CONFIG_EDIT_PATH_REFUSED = -1, /* a link that must not be followed */
    CBM_CONFIG_EDIT_PATH_DIRECT = 0,   /* not a link (or absent): path is used as given */
    CBM_CONFIG_EDIT_PATH_FOLLOWED = 1, /* user-owned link inside a root: the target is used */
} cbm_config_edit_path_status_t;

typedef struct {
    int status; /* cbm_config_edit_path_status_t */
    int dirfd;  /* FOLLOWED: the target's parent directory, pinned; else -1 */
    int fd;     /* FOLLOWED: the validated target, O_RDONLY|O_NOFOLLOW; else -1 */
    char base[CBM_CONFIG_EDIT_NAME_MAX]; /* FOLLOWED: the target's name inside dirfd */
    char path[CBM_CONFIG_EDIT_PATH_MAX]; /* the path the editor operates on */
} cbm_config_edit_target_t;

/* Opt-in scope (per thread): links under these canonical roots may be
 * followed. Up to four roots; returns 0 when registered. Clear when the
 * agent-configuration work is done. */
int cbm_config_edit_path_follow_add_root(const char *root);
void cbm_config_edit_path_follow_clear(void);

/* Classifies path and, for a followed link, pins its parent directory and
 * validates the target. Returns the status; on REFUSED the target holds no
 * descriptors and an empty path, and the editor fails closed as before.
 * Always pair with cbm_config_edit_target_close(). */
int cbm_config_edit_target_open(const char *path, cbm_config_edit_target_t *target);
void cbm_config_edit_target_close(cbm_config_edit_target_t *target);

/* FOLLOWED targets only (return -1 with errno = EINVAL otherwise):
 * create a temp file `name` beside the target (openat O_CREAT|O_EXCL|
 * O_NOFOLLOW; EEXIST when the name is taken), publish it over the target with
 * renameat on the pinned directory (which is then fsync'ed), or discard it. */
int cbm_config_edit_target_create_temp(const cbm_config_edit_target_t *target, const char *name,
                                       unsigned mode);
int cbm_config_edit_target_commit(const cbm_config_edit_target_t *target, const char *name);
int cbm_config_edit_target_unlink(const cbm_config_edit_target_t *target, const char *name);

/* Observed-fact reason a symlink at path is refused, for diagnostics
 * ("symlink owned by uid 0, not the invoking user (uid 501)", "dangling
 * symlink", …). Writes "" and returns 0 when path is not a refused link, 1
 * when a reason was written. Never reports errno — it may be stale (#1537). */
int cbm_config_edit_path_refusal(const char *path, char *out, size_t out_size);

#ifdef CBM_CONFIG_EDIT_PATH_ENABLE_TEST_API
/* Test seams (POSIX; no-ops on Windows). A test cannot create a link owned by
 * another user without root, so the foreign-owner and root-run refusals move
 * the observer instead of the file; the identity skew makes the validated
 * descriptor disagree with stat(realpath), the swapped-target refusal. */
void cbm_config_edit_path_set_invoking_uid_for_test(unsigned uid, int set);
void cbm_config_edit_path_set_identity_skew_for_test(int set);
#endif

#endif /* CBM_CONFIG_EDIT_PATH_H */
