#include "mcp/test_impact_tree_internal.h"
#ifndef _WIN32
#include <errno.h>
#if defined(__APPLE__)
#include <sys/acl.h>

bool tpt_native_acl(tpt_context *c, tpt_object *o, unsigned mode, bool changed) {
    (void)mode;
    acl_t acl = acl_get_fd_np(o->handle, ACL_TYPE_EXTENDED);
    if (!acl) {
        int error = errno;
        /* macOS reports an absent ACL as ENOENT on a valid descriptor. The sole
         * caller verifies the named object and fstat identity/policy of this held
         * descriptor immediately before this query; no pathname lookup occurs here. */
        if (error == ENOENT)
            return true;
        return tpt_fail(c, error == ENOMEM ? CBM_PINNED_TREE_OOM : CBM_PINNED_TREE_UNSUPPORTED,
                        "required extended ACL inspection failed");
    }
    c->tree->pending_acl = acl;
    bool valid = acl_valid(acl) == 0;
    int validation_error = errno;
    if (!valid)
        tpt_fail(c, validation_error == ENOMEM ? CBM_PINNED_TREE_OOM : CBM_PINNED_TREE_UNSUPPORTED,
                 "required extended ACL validation failed");
    acl_entry_t entry;
    errno = 0;
    int result = valid ? acl_get_entry(acl, ACL_FIRST_ENTRY, &entry) : -1;
    int terminal = errno;
    bool empty = valid && result == -1 && terminal == EINVAL;
    if (acl_free(acl) != 0)
        return tpt_fail(c, CBM_PINNED_TREE_IO, "native ACL release failed");
    c->tree->pending_acl = NULL;
    if (!valid)
        return false;
    if (result == 0)
        return tpt_fail(c, changed ? CBM_PINNED_TREE_CHANGED : CBM_PINNED_TREE_UNSUPPORTED,
                        "extended ACL entry rejected");
    return empty || tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "ACL absence not established");
}
#elif defined(__linux__)
#include <sys/xattr.h>

static uint32_t tpt_little(const unsigned char *p, size_t n) {
    uint32_t value = 0;
    for (size_t i = 0; i < n; i++)
        value |= (uint32_t)p[i] << (8 * i);
    return value;
}

static int tpt_linux_access(tpt_context *c, const unsigned char *p, size_t n, unsigned mode,
                            bool is_default) {
    /* Linux UAPI posix_acl_xattr: LE32 version 2; LE16 tag/perm + LE32 id. */
    if (n < 4 || (n - 4) % 8 || tpt_little(p, 4) != 2)
        return -1;
    if (is_default)
        return n == 4 ? 1 : 0;
    unsigned seen = 0;
    bool extended = false;
    for (size_t i = 4; i < n; i += 8) {
        if (!tpt_poll(c))
            return -1;
        uint32_t tag = tpt_little(p + i, 2), perm = tpt_little(p + i + 2, 2);
        uint32_t id = tpt_little(p + i + 4, 4);
        if (perm > 7)
            return -1;
        if (tag == 2 || tag == 8 || tag == 16) {
            extended = true;
            continue;
        }
        unsigned bit, expected;
        if (tag == 1) {
            bit = 1;
            expected = (mode >> 6) & 7;
        } else if (tag == 4) {
            bit = 2;
            expected = (mode >> 3) & 7;
        } else if (tag == 32) {
            bit = 4;
            expected = mode & 7;
        } else {
            return -1;
        }
        if (id != UINT32_MAX)
            return -1;
        if ((seen & bit) || perm != expected)
            extended = true;
        seen |= bit;
    }
    return !extended && seen == 7 && n == 28 ? 1 : 0;
}

static bool tpt_linux_acl(tpt_context *c, tpt_object *o, unsigned mode, bool is_default,
                          bool changed) {
    const char *name = is_default ? "system.posix_acl_default" : "system.posix_acl_access";
    ssize_t n = fgetxattr(o->handle, name, c->tree->security_scratch, TPT_SECURITY_CAP);
    if (n < 0) {
        if (errno == ENODATA)
            return true;
        return tpt_fail(c, errno == ENOMEM ? CBM_PINNED_TREE_OOM : CBM_PINNED_TREE_UNSUPPORTED,
                        "required POSIX ACL xattr inspection failed");
    }
    const unsigned char *p = c->tree->security_scratch;
    int allowed = tpt_linux_access(c, p, (size_t)n, mode, is_default);
    if (allowed < 0)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "undecodable POSIX ACL representation");
    return allowed == 1 ||
           tpt_fail(c, changed ? CBM_PINNED_TREE_CHANGED : CBM_PINNED_TREE_UNSUPPORTED,
                    "extended or inherited POSIX ACL rejected");
}

bool tpt_native_acl(tpt_context *c, tpt_object *o, unsigned mode, bool changed) {
    return tpt_linux_acl(c, o, mode, false, changed) &&
           (!o->directory || tpt_linux_acl(c, o, mode, true, changed));
}
#else
bool tpt_native_acl(tpt_context *c, tpt_object *o, unsigned mode, bool changed) {
    (void)o;
    (void)mode;
    (void)changed;
    return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "required native ACL interface unavailable");
}
#endif
#endif
