#include "mcp/test_impact_tree_internal.h"
#ifndef _WIN32
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef __APPLE__
#include <sys/acl.h>
#endif

bool tpt_native_write(tpt_context *c, tpt_object *o, cbm_git_bytes_t bytes) {
    size_t offset = 0;
    while (offset < bytes.length) {
        if (!tpt_poll(c))
            return false;
        size_t n = bytes.length - offset;
        if (n > TPT_CHUNK)
            n = TPT_CHUNK;
        ssize_t written = write(o->handle, bytes.data + offset, n);
        if (written < 0 && errno == EINTR)
            continue;
        if (written <= 0)
            return tpt_fail(c, CBM_PINNED_TREE_IO, "native payload write failed");
        offset += (size_t)written;
    }
    return true;
}

bool tpt_native_read_bounded(tpt_context *c, tpt_object *o, size_t request, size_t *count) {
    *count = 0;
    if (!request || request > TPT_CHUNK)
        return tpt_fail(c, CBM_PINNED_TREE_INVALID, "invalid native read bound");
    for (;;) {
        if (!tpt_poll(c))
            return false;
        ssize_t n = read(o->handle, c->tree->io_scratch, request);
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0)
            return tpt_posix_error(c, errno, false);
        *count = (size_t)n;
        return true;
    }
}

bool tpt_native_read(tpt_context *c, tpt_object *o, size_t *count) {
    return tpt_native_read_bounded(c, o, TPT_CHUNK, count);
}

bool tpt_native_finish(tpt_context *c, tpt_object *o) {
    mode_t mode = c->tree->files[o->file].git_mode == 0100755 ? 0500 : 0400;
    if (fchmod(o->handle, mode) != 0)
        return tpt_posix_error(c, errno, false);
    return tpt_native_check(c, o, false) && tpt_native_close(c, &o->handle);
}

static bool tpt_directory_close(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    if (!t->enumeration)
        return true;
    DIR *dir = t->enumeration;
    t->enumeration = NULL;
    if (closedir(dir) == 0)
        return true;
    t->close_uncertain = true;
    return tpt_fail(c, CBM_PINNED_TREE_IO, "directory close failed");
}

bool tpt_native_enumerate(tpt_context *c, size_t directory) {
    cbm_pinned_tree_t *t = c->tree;
    t->enumeration_fd =
        openat(t->objects[directory].handle, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (t->enumeration_fd < 0)
        return tpt_posix_error(c, errno, false);
    t->enumeration = fdopendir(t->enumeration_fd);
    if (!t->enumeration)
        return tpt_posix_error(c, errno, false);
    t->enumeration_fd = -1;
    for (;;) {
        if (!tpt_poll(c))
            return false;
        errno = 0;
        struct dirent *entry = readdir(t->enumeration);
        if (!entry) {
            if (errno)
                return tpt_posix_error(c, errno, false);
            return tpt_directory_close(c);
        }
        size_t n = 0;
        if (!tpt_length(c, entry->d_name, TPT_PATH_CAP - 1, &n))
            return false;
        if ((n == 1 && entry->d_name[0] == '.') ||
            (n == 2 && entry->d_name[0] == '.' && entry->d_name[1] == '.'))
            continue;
        if (!tpt_entry(c, directory, entry->d_name, n))
            return false;
    }
}

bool tpt_native_release(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    bool ok = tpt_directory_close(c);
    if (!tpt_native_close(c, &t->enumeration_fd))
        ok = false;
#ifdef __APPLE__
    if (t->pending_acl) {
        if (acl_free(t->pending_acl) != 0) {
            tpt_fail(c, CBM_PINNED_TREE_IO, "pending ACL release failed");
            ok = false;
        } else {
            t->pending_acl = NULL;
        }
    }
#endif
    bool owned = false;
    for (size_t i = 0; t->objects && i < t->object_count; i++) {
        tpt_object *o = &t->objects[i];
        if (!o->created)
            continue;
        owned = true;
        if (!o->directory && !tpt_native_close(c, &o->handle))
            ok = false;
    }
    if (!owned && !tpt_native_close(c, &t->parent.handle))
        ok = false;
    if (t->close_uncertain) {
        tpt_fail(c, CBM_PINNED_TREE_IO, "a native close remains indeterminate");
        ok = false;
    }
    return ok;
}
#endif
