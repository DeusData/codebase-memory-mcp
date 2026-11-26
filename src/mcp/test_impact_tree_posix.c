#include "mcp/test_impact_tree_internal.h"
#ifndef _WIN32
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

bool tpt_posix_error(tpt_context *c, int e, bool creating) {
    if (creating && e == EEXIST)
        return tpt_fail(c, CBM_PINNED_TREE_COLLISION, "exclusive native name already exists");
    if (e == ENOMEM)
        return tpt_fail(c, CBM_PINNED_TREE_OOM, "native allocation failed");
    if (e == ENAMETOOLONG || e == EILSEQ || e == ENOSYS || e == ENOTSUP || e == EINVAL)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "unsupported native representation");
    return tpt_fail(c, CBM_PINNED_TREE_IO, "native filesystem operation failed");
}

bool tpt_native_id_equal(tpt_identity a, tpt_identity b) {
    return a.device == b.device && a.inode == b.inode;
}

static tpt_identity tpt_stat_id(const struct stat *st) {
    return (tpt_identity){st->st_dev, st->st_ino};
}

static bool tpt_kind(const struct stat *st, bool directory) {
    return directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode) && st->st_nlink == 1;
}

static int tpt_stat_path(cbm_pinned_tree_t *t, tpt_object *o, struct stat *st) {
    if (o == &t->parent)
        return lstat(t->parent_path, st);
    return fstatat(tpt_parent(t, o)->handle, tpt_leaf(o), st, AT_SYMLINK_NOFOLLOW);
}

static bool tpt_open_object(tpt_context *c, tpt_object *o) {
    if (o->handle != TPT_INVALID_HANDLE)
        return true;
    int flags = O_RDONLY | O_NOFOLLOW | O_CLOEXEC;
    if (o->directory)
        flags |= O_DIRECTORY;
    o->handle = openat(tpt_parent(c->tree, o)->handle, tpt_leaf(o), flags);
    return o->handle >= 0 || tpt_posix_error(c, errno, false);
}

bool tpt_native_identity(tpt_context *c, tpt_object *o, bool *absent) {
    *absent = false;
    struct stat st;
    if (tpt_stat_path(c->tree, o, &st) != 0) {
        if (errno == ENOENT) {
            *absent = true;
            return true;
        }
        return tpt_posix_error(c, errno, false);
    }
    if (!o->identified || !tpt_kind(&st, o->directory) ||
        !tpt_native_id_equal(tpt_stat_id(&st), o->identity))
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "tracked native identity replaced");
    if (o->directory && o != &c->tree->parent && !tpt_open_object(c, o))
        return false;
    if (o->handle != TPT_INVALID_HANDLE) {
        if (fstat(o->handle, &st) != 0)
            return tpt_posix_error(c, errno, false);
        if (!tpt_kind(&st, o->directory) || !tpt_native_id_equal(tpt_stat_id(&st), o->identity))
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "tracked native handle changed");
    }
    return true;
}

static unsigned tpt_mode(const cbm_pinned_tree_t *t, const tpt_object *o, bool writing) {
    if (o->directory)
        return 0700;
    if (writing)
        return 0600;
    return t->files[o->file].git_mode == 0100755 ? 0500 : 0400;
}

static bool tpt_posix_policy(tpt_context *c, tpt_object *o, const struct stat *st, bool writing) {
    unsigned mode = tpt_mode(c->tree, o, writing);
    if (!tpt_native_id_equal(tpt_stat_id(st), o->identity))
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "opened object identity mismatch");
    if (st->st_uid != c->tree->uid || (st->st_mode & 07777) != mode || !tpt_kind(st, o->directory))
        return tpt_fail(c, writing ? CBM_PINNED_TREE_UNSUPPORTED : CBM_PINNED_TREE_CHANGED,
                        "native owner, mode or link policy mismatch");
    if (!o->directory && !writing &&
        (st->st_size < 0 || (uint64_t)st->st_size != c->tree->files[o->file].content_length))
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "native file length changed");
    return true;
}

bool tpt_native_check(tpt_context *c, tpt_object *o, bool writing) {
    if (geteuid() != c->tree->uid)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "effective user changed");
    bool absent = false;
    if (!tpt_native_identity(c, o, &absent))
        return false;
    if (absent)
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "expected native object missing");
    struct stat st;
    if (tpt_stat_path(c->tree, o, &st) != 0)
        return tpt_posix_error(c, errno, false);
    if (!tpt_posix_policy(c, o, &st, writing))
        return false;
    if (o != &c->tree->parent && !tpt_open_object(c, o))
        return false;
    if (fstat(o->handle, &st) != 0)
        return tpt_posix_error(c, errno, false);
    return tpt_posix_policy(c, o, &st, writing) &&
           tpt_native_acl(c, o, tpt_mode(c->tree, o, writing), !writing);
}

bool tpt_native_prepare(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    size_t parent_length = 0;
    if (!tpt_length(c, t->parent_path, TPT_PATH_CAP - 1, &parent_length))
        return false;
    while (parent_length > 1 && t->parent_path[parent_length - 1] == '/')
        t->parent_path[--parent_length] = 0;
    struct stat before;
    if (lstat(t->parent_path, &before) != 0)
        return tpt_posix_error(c, errno, false);
    if (!S_ISDIR(before.st_mode) || before.st_uid != geteuid() || (before.st_mode & 07777) != 0700)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "parent must be a private owned directory");
    t->parent.handle = open(t->parent_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (t->parent.handle < 0)
        return tpt_posix_error(c, errno, false);
    t->uid = geteuid();
    struct stat st;
    if (fstat(t->parent.handle, &st) != 0)
        return tpt_posix_error(c, errno, false);
    t->parent.identity = tpt_stat_id(&st);
    t->parent.identified = true;
    if (!tpt_native_id_equal(tpt_stat_id(&before), t->parent.identity))
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "parent changed while opening");
    if (!realpath(t->parent_path, t->path_scratch))
        return tpt_posix_error(c, errno, false);
    size_t n = 0;
    if (!tpt_length(c, t->path_scratch, TPT_PATH_CAP - 1, &n))
        return false;
    memcpy(t->parent_path, t->path_scratch, n + 1);
    t->parent.path = t->parent_path;
    t->parent.length = n;
    return tpt_native_check(c, &t->parent, true);
}

bool tpt_native_create(tpt_context *c, tpt_object *o) {
    if (!tpt_poll(c) || !tpt_chain(c, o))
        return false;
    int parent = tpt_parent(c->tree, o)->handle;
    const char *name = tpt_leaf(o);
    if (o->directory) {
        if (mkdirat(parent, name, 0700) != 0)
            return tpt_posix_error(c, errno, true);
        o->created = true;
    } else {
        o->handle = openat(parent, name, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
        if (o->handle < 0)
            return tpt_posix_error(c, errno, true);
        o->created = true;
    }
    struct stat st;
    if (fstatat(parent, name, &st, AT_SYMLINK_NOFOLLOW) != 0)
        return tpt_posix_error(c, errno, false);
    if (!tpt_kind(&st, o->directory))
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "created object type changed");
    if (!tpt_record_identity(c, o, tpt_stat_id(&st)) || !tpt_open_object(c, o))
        return false;
    if (fchmod(o->handle, o->directory ? 0700 : 0600) != 0)
        return tpt_posix_error(c, errno, false);
    return tpt_native_check(c, o, true);
}

bool tpt_native_close(tpt_context *c, tpt_handle *handle) {
    if (*handle == TPT_INVALID_HANDLE)
        return true;
    int fd = *handle;
    *handle = TPT_INVALID_HANDLE;
    if (close(fd) == 0)
        return true;
    /* Never retry an ambiguously closed POSIX fd: another thread may reuse it. */
    c->tree->close_uncertain = true;
    c->close_failed = true;
    return tpt_fail(c, CBM_PINNED_TREE_IO, "native close could not be established");
}

bool tpt_native_remove(tpt_context *c, tpt_object *o) {
    if (!tpt_chain(c, o))
        return false;
    bool absent = false;
    if (!tpt_native_identity(c, o, &absent))
        return false;
    if (absent && !tpt_absent_disposable(c, o))
        return false;
    if (!tpt_native_close(c, &o->handle))
        return false;
    if (!absent &&
        unlinkat(tpt_parent(c->tree, o)->handle, tpt_leaf(o), o->directory ? AT_REMOVEDIR : 0) != 0)
        return tpt_posix_error(c, errno, false);
    o->created = false;
    return true;
}
#endif
