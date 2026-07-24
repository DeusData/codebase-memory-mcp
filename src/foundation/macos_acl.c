/* macos_acl.c — Darwin extended-ACL operations anchored to an open fd. */
#include "foundation/macos_acl.h"

#include <stddef.h>

#ifdef __APPLE__

#include <errno.h>
#include <sys/types.h>
#include <sys/acl.h>

bool cbm_macos_extended_acl_fd_is_empty(int fd) {
    if (fd < 0) {
        return false;
    }

    errno = 0;
    acl_t acl = acl_get_fd_np(fd, ACL_TYPE_EXTENDED);
    if (!acl) {
        /* Darwin reports an object with no extended ACL as ENOENT on some
         * filesystems.  All other retrieval failures are fail-closed. */
        return errno == ENOENT;
    }

    acl_entry_t entry = NULL;
    errno = 0;
    int entry_result = acl_get_entry(acl, ACL_FIRST_ENTRY, &entry);
    int entry_error = errno;
    int free_result = acl_free(acl);

    /* Darwin returns 0 for an entry and -1/EINVAL when an empty ACL (or the
     * end of an ACL) is reached.  Looking only at the first entry makes every
     * nonempty ACL unsafe, independent of its allow/deny permissions. */
    return entry_result == -1 && entry_error == EINVAL && free_result == 0;
}

bool cbm_macos_extended_acl_fd_is_deny_only(int fd) {
    if (fd < 0) {
        return false;
    }

    errno = 0;
    acl_t acl = acl_get_fd_np(fd, ACL_TYPE_EXTENDED);
    if (!acl) {
        /* As with the empty predicate, ENOENT is Darwin's ACL-less result on
         * some filesystems. Retrieval errors other than ENOENT are unsafe. */
        return errno == ENOENT;
    }

    bool safe = true;
    acl_entry_t entry = NULL;
    int entry_id = ACL_FIRST_ENTRY;
    for (;;) {
        errno = 0;
        int entry_result = acl_get_entry(acl, entry_id, &entry);
        int entry_error = errno;
        if (entry_result == -1) {
            safe = entry_error == EINVAL;
            break;
        }
        if (entry_result != 0 || !entry) {
            safe = false;
            break;
        }
        acl_tag_t tag = (acl_tag_t)0;
        if (acl_get_tag_type(entry, &tag) != 0 || tag != ACL_EXTENDED_DENY) {
            safe = false;
            break;
        }
        entry_id = ACL_NEXT_ENTRY;
    }

    int free_result = acl_free(acl);
    return safe && free_result == 0;
}

bool cbm_macos_extended_acl_fd_clear(int fd) {
    if (fd < 0) {
        return false;
    }
    acl_t empty = acl_init(0);
    if (!empty) {
        return false;
    }
    bool cleared = acl_set_fd_np(fd, empty, ACL_TYPE_EXTENDED) == 0;
    bool freed = acl_free(empty) == 0;
    return cleared && freed && cbm_macos_extended_acl_fd_is_empty(fd);
}

#else

bool cbm_macos_extended_acl_fd_is_empty(int fd) {
    return fd >= 0;
}

bool cbm_macos_extended_acl_fd_is_deny_only(int fd) {
    return fd >= 0;
}

bool cbm_macos_extended_acl_fd_clear(int fd) {
    return fd >= 0;
}

#endif
