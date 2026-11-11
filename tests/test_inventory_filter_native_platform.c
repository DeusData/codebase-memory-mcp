#include "test_inventory_filter_native_internal.h"
#ifdef _WIN32
#include <aclapi.h>
static bool if_native_windows_parent(const char *path) {
    HANDLE token = NULL;
    union {
        max_align_t align;
        unsigned char bytes[2048];
    } user;
    DWORD size = sizeof(user.bytes);
    union {
        max_align_t alignment;
        unsigned char bytes[1024];
    } acl_storage;
    wchar_t wide[IF_NATIVE_PATH];
    SECURITY_DESCRIPTOR descriptor;
    ACL *acl = (ACL *)acl_storage.bytes;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token))
        return false;
    bool ok = GetTokenInformation(token, TokenUser, user.bytes, size, &size) != 0;
    ok = CloseHandle(token) != 0 && ok;
    if (!ok ||
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wide, IF_NATIVE_PATH) <= 0)
        return false;
    PSID owner = ((TOKEN_USER *)user.bytes)->User.Sid;
    return InitializeAcl(acl, sizeof(acl_storage.bytes), ACL_REVISION) &&
           AddAccessAllowedAceEx(acl, ACL_REVISION, OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
                                 FILE_ALL_ACCESS, owner) &&
           InitializeSecurityDescriptor(&descriptor, SECURITY_DESCRIPTOR_REVISION) &&
           SetSecurityDescriptorOwner(&descriptor, owner, FALSE) &&
           SetSecurityDescriptorDacl(&descriptor, TRUE, acl, FALSE) &&
           SetSecurityDescriptorControl(&descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED) &&
           SetFileSecurityW(wide,
                            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION |
                                PROTECTED_DACL_SECURITY_INFORMATION,
                            &descriptor);
}
#endif
bool if_native_private_parent(if_native *n) {
    /* The helper may fill a template even when creation fails. Publish only a
     * successfully created owned path; subsequent ACL failure retains ownership. */
    char candidate[IF_NATIVE_PATH] = {0};
    if (!th_secure_runtime_parent_new(candidate, sizeof(candidate), "inventory-filter"))
        return false;
    strcpy(n->parent, candidate);
#ifdef _WIN32
    return if_native_windows_parent(n->parent);
#else
    return chmod(n->parent, 0700) == 0;
#endif
}
