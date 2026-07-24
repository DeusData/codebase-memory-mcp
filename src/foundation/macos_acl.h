/* macos_acl.h — Anchored extended-ACL checks for private filesystem objects. */
#ifndef CBM_FOUNDATION_MACOS_ACL_H
#define CBM_FOUNDATION_MACOS_ACL_H

#include <stdbool.h>

/* On macOS, succeeds only when fd has no extended ACL entries.  Other
 * platforms have no Darwin extended ACL surface, so a valid fd is sufficient. */
bool cbm_macos_extended_acl_fd_is_empty(int fd);

/* On macOS, succeeds when fd has no extended ACL or every extended ACL entry
 * is an explicit DENY entry.  This is the ancestor-directory predicate: deny
 * entries cannot grant another account mutation authority, while any ALLOW or
 * unrecognized entry fails closed.  Other platforms only validate fd. */
bool cbm_macos_extended_acl_fd_is_deny_only(int fd);

/* On macOS, replaces fd's extended ACL with an empty ACL and verifies the
 * result through the same anchored descriptor.  Other platforms are a no-op. */
bool cbm_macos_extended_acl_fd_clear(int fd);

#endif
