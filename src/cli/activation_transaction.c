/* Transactional binary activation. See activation_transaction.h. */
#include "cli/activation_transaction.h"
#include "foundation/macos_acl.h"

#include <errno.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <sys/stat.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <aclapi.h>
#include <sddl.h>
#include <windows.h>
#else
#include <fcntl.h>
#ifdef __APPLE__
#include <sys/acl.h>
#include <sys/stdio.h>
#elif defined(__linux__)
#include <sys/syscall.h>
#endif
#include <unistd.h>
#endif

typedef enum {
    ACTIVATION_REPLACE = 0,
    ACTIVATION_REMOVE = 1,
} activation_action_t;

typedef enum {
    ACTIVATION_STAGED = 0,
    ACTIVATION_COMMITTED = 1,
    ACTIVATION_ROLLED_BACK = 2,
    ACTIVATION_FINALIZED = 3,
    ACTIVATION_FINALIZED_DEFERRED = 4,
    ACTIVATION_RECOVERY_NEEDED = 5,
} activation_state_t;

typedef struct {
#ifdef _WIN32
    DWORD volume_serial;
    DWORD index_high;
    DWORD index_low;
#else
    dev_t device;
    ino_t inode;
#endif
} activation_file_identity_t;

struct cbm_activation_transaction {
    activation_action_t action;
    activation_state_t state;
    char *target_path;
    char *directory_path;
    char *target_name;
    char *staged_path;
    char *staged_name;
    char *backup_path;
    char *backup_name;
    bool target_existed;
    bool staged_exists;
    bool backup_exists;
    bool backup_contains_target;
    bool deferred_cleanup;
    activation_file_identity_t target_identity;
    activation_file_identity_t directory_identity;
    activation_file_identity_t staged_identity;
    activation_file_identity_t backup_identity;
#ifndef _WIN32
    int directory_fd;
#endif
};

/* The transaction's security predicates refuse by returning false without a
 * usable OS last-error, so every caller used to report the same blind
 * "status -3, os 0" for five distinct refusal classes (ancestor open,
 * owner, DACL, target snapshot, staged create). The first predicate that
 * refuses records what refused here; callers append the note to their error
 * line. First-wins so the deepest (root) refusal survives the unwind.
 * Single-threaded by contract: CLI activation runs on one thread. */
static char g_activation_refusal_note[512];

/* The validated object's path, set by each validator for the duration of its
 * predicate calls so the note can say WHAT was refused, not just why. Points
 * into the caller's storage; never dereferenced after the validator returns. */
static const char *g_activation_refusal_object = NULL;

static void activation_refusal_clear(void) {
    g_activation_refusal_note[0] = '\0';
}

static void activation_note_refusal(const char *predicate, unsigned long os_error) {
    if (g_activation_refusal_note[0] != '\0') {
        return;
    }
    (void)snprintf(g_activation_refusal_note, sizeof(g_activation_refusal_note), "%s (os %lu)%s%s",
                   predicate, os_error, g_activation_refusal_object ? " at " : "",
                   g_activation_refusal_object ? g_activation_refusal_object : "");
}

const char *cbm_activation_transaction_refusal_note(void) {
    return g_activation_refusal_note;
}

#ifdef _WIN32
typedef HANDLE activation_native_file_t;
#define ACTIVATION_INVALID_FILE INVALID_HANDLE_VALUE
#else
typedef int activation_native_file_t;
#define ACTIVATION_INVALID_FILE (-1)
#endif

static atomic_uint_fast64_t activation_unique_sequence = ATOMIC_VAR_INIT(0);
static cbm_activation_transaction_before_absent_publish_for_test_fn
    activation_before_absent_publish_for_test;
static void *activation_before_absent_publish_context_for_test;

void cbm_activation_transaction_set_before_absent_publish_for_test(
    cbm_activation_transaction_before_absent_publish_for_test_fn hook, void *context) {
    activation_before_absent_publish_context_for_test = context;
    activation_before_absent_publish_for_test = hook;
}

static char *activation_string_copy(const char *value) {
    if (!value) {
        return NULL;
    }
    size_t length = strlen(value);
    char *copy = malloc(length + 1U);
    if (copy) {
        memcpy(copy, value, length + 1U);
    }
    return copy;
}

static char *activation_string_span(const char *value, size_t length) {
    char *copy = malloc(length + 1U);
    if (copy) {
        memcpy(copy, value, length);
        copy[length] = '\0';
    }
    return copy;
}

static bool activation_target_parts(const char *target_path, char **directory_out,
                                    char **name_out) {
    *directory_out = NULL;
    *name_out = NULL;
    if (!target_path || !target_path[0]) {
        return false;
    }
    size_t length = strlen(target_path);
    if (target_path[length - 1U] == '/' || target_path[length - 1U] == '\\') {
        return false;
    }
    const char *separator = strrchr(target_path, '/');
#ifdef _WIN32
    const char *backslash = strrchr(target_path, '\\');
    if (backslash && (!separator || backslash > separator)) {
        separator = backslash;
    }
#endif
    const char *base = separator ? separator + 1 : target_path;
    if (!base[0] || strcmp(base, ".") == 0 || strcmp(base, "..") == 0) {
        return false;
    }
    if (!separator) {
        *directory_out = activation_string_copy(".");
    } else if (separator == target_path) {
        *directory_out = activation_string_span(target_path, 1U);
#ifdef _WIN32
    } else if (separator == target_path + 2 && target_path[1] == ':') {
        *directory_out = activation_string_span(target_path, 3U);
#endif
    } else {
        *directory_out = activation_string_span(target_path, (size_t)(separator - target_path));
    }
    if (*directory_out) {
        *name_out = activation_string_copy(base);
    }
    if (!*directory_out || !*name_out) {
        free(*directory_out);
        free(*name_out);
        *directory_out = NULL;
        *name_out = NULL;
        return false;
    }
    return true;
}

static char *activation_path_name_copy(const char *path) {
    const char *slash = strrchr(path, '/');
#ifdef _WIN32
    const char *backslash = strrchr(path, '\\');
    if (backslash && (!slash || backslash > slash)) {
        slash = backslash;
    }
#endif
    return activation_string_copy(slash ? slash + 1 : path);
}

static char *activation_unique_path(const char *directory, const char *tag) {
    uint64_t sequence =
        atomic_fetch_add_explicit(&activation_unique_sequence, 1U, memory_order_relaxed) + 1U;
#ifdef _WIN32
    unsigned long process_id = (unsigned long)GetCurrentProcessId();
#else
    unsigned long process_id = (unsigned long)getpid();
#endif
    size_t directory_length = strlen(directory);
    size_t needed = directory_length + strlen(tag) + 80U;
    char *path = malloc(needed);
    if (!path) {
        return NULL;
    }
    /* Windows joins use the backslash: extended-length (\\\\?\\) directories
     * reach this composer and that namespace performs no forward-slash
     * translation. POSIX keeps the slash. */
#ifdef _WIN32
    const char *separator = "\\";
#else
    const char *separator = "/";
#endif
    int written =
        snprintf(path, needed, "%s%s.cbm-%s-%lu-%" PRIu64, directory,
                 directory[directory_length - 1U] == '/' || directory[directory_length - 1U] == '\\'
                     ? ""
                     : separator,
                 tag, process_id, sequence);
    if (written <= 0 || (size_t)written >= needed) {
        free(path);
        return NULL;
    }
    return path;
}

static bool activation_identity_equal(const activation_file_identity_t *left,
                                      const activation_file_identity_t *right) {
#ifdef _WIN32
    return left->volume_serial == right->volume_serial && left->index_high == right->index_high &&
           left->index_low == right->index_low;
#else
    return left->device == right->device && left->inode == right->inode;
#endif
}

#ifdef _WIN32

typedef struct {
    void *token_information;
    PSID user_sid;
    PACL acl;
    SECURITY_DESCRIPTOR descriptor;
    SECURITY_ATTRIBUTES attributes;
} activation_windows_security_t;

static wchar_t *activation_utf8_to_wide_plain(const char *value) {
    if (!value) {
        return NULL;
    }
    int needed = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, NULL, 0);
    if (needed <= 0) {
        return NULL;
    }
    wchar_t *wide = malloc((size_t)needed * sizeof(*wide));
    if (!wide || MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, wide, needed) <= 0) {
        free(wide);
        return NULL;
    }
    return wide;
}

/* File-API path conversion: an absolute drive path beyond the legacy limit is
 * canonicalized and given the extended-length \\?\ prefix so every Win32
 * file call accepts it. Managed-install generation stores and CI-runner paths
 * routinely exceed 260 chars; the prefix also disables the \.\..\ and
 * MAX_PATH parsing that the ancestry validator separately enforces. Non-file
 * strings must not use this. */
static wchar_t *activation_utf8_to_wide(const char *value) {
    wchar_t *wide = activation_utf8_to_wide_plain(value);
    if (!wide) {
        return NULL;
    }
    size_t length = wcslen(wide);
    bool drive_absolute =
        length >= 3 &&
        ((wide[0] >= L'A' && wide[0] <= L'Z') || (wide[0] >= L'a' && wide[0] <= L'z')) &&
        wide[1] == L':' && (wide[2] == L'\\' || wide[2] == L'/');
    if (!drive_absolute || length < 240U) {
        return wide;
    }
    DWORD needed = GetFullPathNameW(wide, 0, NULL, NULL);
    wchar_t *prefixed = needed > 0 ? malloc(((size_t)needed + 5U) * sizeof(*prefixed)) : NULL;
    if (!prefixed) {
        free(wide);
        return NULL;
    }
    wmemcpy(prefixed, L"\\\\?\\", 4);
    DWORD copied = GetFullPathNameW(wide, needed, prefixed + 4, NULL);
    free(wide);
    if (copied == 0 || copied >= needed) {
        free(prefixed);
        return NULL;
    }
    return prefixed;
}

static bool activation_windows_user(void **information_out, PSID *sid_out) {
    *information_out = NULL;
    *sid_out = NULL;
    HANDLE token = NULL;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        return false;
    }
    DWORD needed = 0;
    (void)GetTokenInformation(token, TokenUser, NULL, 0, &needed);
    if (needed == 0 || GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        (void)CloseHandle(token);
        return false;
    }
    void *information = calloc(1, needed);
    bool ok =
        information && GetTokenInformation(token, TokenUser, information, needed, &needed) != 0;
    (void)CloseHandle(token);
    if (!ok) {
        free(information);
        return false;
    }
    PSID sid = ((TOKEN_USER *)information)->User.Sid;
    if (!sid || !IsValidSid(sid)) {
        free(information);
        return false;
    }
    *information_out = information;
    *sid_out = sid;
    return true;
}

static bool activation_windows_security_init(activation_windows_security_t *security) {
    memset(security, 0, sizeof(*security));
    if (!activation_windows_user(&security->token_information, &security->user_sid)) {
        return false;
    }
    EXPLICIT_ACCESSW access;
    memset(&access, 0, sizeof(access));
    access.grfAccessPermissions = GENERIC_ALL;
    access.grfAccessMode = SET_ACCESS;
    access.grfInheritance = NO_INHERITANCE;
    access.Trustee.TrusteeForm = TRUSTEE_IS_SID;
    access.Trustee.TrusteeType = TRUSTEE_IS_USER;
    access.Trustee.ptstrName = (LPWSTR)security->user_sid;
    bool ok =
        SetEntriesInAclW(1, &access, NULL, &security->acl) == ERROR_SUCCESS &&
        InitializeSecurityDescriptor(&security->descriptor, SECURITY_DESCRIPTOR_REVISION) &&
        SetSecurityDescriptorDacl(&security->descriptor, TRUE, security->acl, FALSE) &&
        /* Stamp the owner explicitly to the token user. Without this the file
         * created with these attributes inherits the token's DEFAULT owner,
         * which under an Administrators-default-owner policy (standard on
         * Windows Server and GitHub's elevated runners) is BUILTIN\
         * Administrators, not this user. Every staged file is then validated
         * with a strict owner-is-current-user check (activation_windows_owner_
         * is_current), so an unstamped file is rejected and private staging
         * fails. The daemon stamps the owner identically for the same reason
         * (see src/daemon/ipc.c). Setting the owner to our own token user
         * needs no extra privilege. */
        SetSecurityDescriptorOwner(&security->descriptor, security->user_sid, FALSE) &&
        SetSecurityDescriptorControl(&security->descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED);
    if (!ok) {
        if (security->acl) {
            (void)LocalFree(security->acl);
        }
        free(security->token_information);
        memset(security, 0, sizeof(*security));
        return false;
    }
    security->attributes.nLength = sizeof(security->attributes);
    security->attributes.lpSecurityDescriptor = &security->descriptor;
    security->attributes.bInheritHandle = FALSE;
    return true;
}

static void activation_windows_security_destroy(activation_windows_security_t *security) {
    if (security->acl) {
        (void)LocalFree(security->acl);
    }
    free(security->token_information);
    memset(security, 0, sizeof(*security));
}

/* Trusted-owner acceptance for SOURCE-side objects: a downloaded release
 * bundle is owned by whatever the machine's default-owner policy dictates
 * (Administrators on GitHub-runner-class images). Its integrity is enforced
 * by identity snapshot/recheck plus the caller's build-fingerprint check,
 * not by exact ownership. Staging targets created by this transaction keep
 * the exact-current-owner rule. */
static bool activation_windows_owner_is_trusted(HANDLE handle) {
    void *information = NULL;
    PSID user_sid = NULL;
    if (!activation_windows_user(&information, &user_sid)) {
        return false;
    }
    PSID owner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD result = GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &owner, NULL,
                                   NULL, NULL, &descriptor);
    bool trusted = result == ERROR_SUCCESS && owner && IsValidSid(owner) &&
                   (EqualSid(owner, user_sid) || IsWellKnownSid(owner, WinLocalSystemSid) ||
                    IsWellKnownSid(owner, WinBuiltinAdministratorsSid));
    if (!trusted) {
        activation_note_refusal("owner-not-trusted", result);
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    free(information);
    return trusted;
}

static bool activation_windows_owner_is_current(HANDLE handle) {
    void *information = NULL;
    PSID user_sid = NULL;
    if (!activation_windows_user(&information, &user_sid)) {
        return false;
    }
    PSID owner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD result = GetSecurityInfo(handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &owner, NULL,
                                   NULL, NULL, &descriptor);
    bool same = result == ERROR_SUCCESS && owner && IsValidSid(owner) && EqualSid(owner, user_sid);
    if (!same) {
        activation_note_refusal("owner-not-current-user", result);
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    free(information);
    return same;
}

static bool activation_windows_acl_check(HANDLE handle, DWORD tolerated_untrusted_mask);

static bool activation_windows_acl_secure(HANDLE handle) {
    return activation_windows_acl_check(handle, 0U);
}

static bool activation_windows_acl_check(HANDLE handle, DWORD tolerated_untrusted_mask) {
    void *information = NULL;
    PSID user_sid = NULL;
    if (!activation_windows_user(&information, &user_sid)) {
        return false;
    }
    PACL dacl = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD result = GetSecurityInfo(handle, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, NULL, NULL,
                                   &dacl, NULL, &descriptor);
    ACL_SIZE_INFORMATION acl_information;
    memset(&acl_information, 0, sizeof(acl_information));
    bool secure =
        result == ERROR_SUCCESS && descriptor && dacl && IsValidAcl(dacl) != 0 &&
        GetAclInformation(dacl, &acl_information, sizeof(acl_information), AclSizeInformation) != 0;
    const DWORD mutation_rights = GENERIC_ALL | GENERIC_WRITE | FILE_WRITE_DATA | FILE_APPEND_DATA |
                                  FILE_ADD_FILE | FILE_ADD_SUBDIRECTORY | FILE_DELETE_CHILD |
                                  FILE_WRITE_EA | FILE_WRITE_ATTRIBUTES | DELETE | WRITE_DAC |
                                  WRITE_OWNER;
    enum {
        ACTIVATION_WINDOWS_ACE_ALLOW = 0x00,
        ACTIVATION_WINDOWS_ACE_DENY = 0x01,
        ACTIVATION_WINDOWS_ACE_DENY_OBJECT = 0x06,
        ACTIVATION_WINDOWS_ACE_DENY_CALLBACK = 0x0a,
        ACTIVATION_WINDOWS_ACE_DENY_CALLBACK_OBJECT = 0x0c,
    };
    for (DWORD index = 0; secure && index < acl_information.AceCount; index++) {
        void *opaque_ace = NULL;
        if (!GetAce(dacl, index, &opaque_ace) || !opaque_ace) {
            secure = false;
            break;
        }
        ACE_HEADER *header = opaque_ace;
        if (header->AceType == ACTIVATION_WINDOWS_ACE_DENY ||
            header->AceType == ACTIVATION_WINDOWS_ACE_DENY_OBJECT ||
            header->AceType == ACTIVATION_WINDOWS_ACE_DENY_CALLBACK ||
            header->AceType == ACTIVATION_WINDOWS_ACE_DENY_CALLBACK_OBJECT) {
            continue;
        }
        if (header->AceType != ACTIVATION_WINDOWS_ACE_ALLOW) {
            /* Only the fixed-layout ACCESS_ALLOWED_ACE is parsed below.  Every
             * callback/object/compound allow form and every unknown future
             * form is rejected so payloads cannot hide mutation grants. */
            secure = false;
            break;
        }
        const size_t sid_offset = offsetof(ACCESS_ALLOWED_ACE, SidStart);
        const size_t sid_header_size = offsetof(SID, SubAuthority);
        if ((size_t)header->AceSize < sid_offset + sid_header_size) {
            secure = false;
            break;
        }
        ACCESS_ALLOWED_ACE *ace = opaque_ace;
        if ((ace->Mask & mutation_rights & ~tolerated_untrusted_mask) == 0) {
            continue;
        }
        PSID sid = (PSID)&ace->SidStart;
        size_t sid_capacity = (size_t)header->AceSize - sid_offset;
        DWORD sid_length = GetSidLengthRequired(((SID *)sid)->SubAuthorityCount);
        bool trusted = sid_length <= sid_capacity && IsValidSid(sid) &&
                       GetLengthSid(sid) == sid_length &&
                       (EqualSid(sid, user_sid) || IsWellKnownSid(sid, WinLocalSystemSid) ||
                        IsWellKnownSid(sid, WinBuiltinAdministratorsSid) ||
                        /* OWNER RIGHTS modulates whoever owns the object; the
                         * owner is separately validated in every chain that
                         * reaches here (same tolerance as the daemon IPC and
                         * launcher ancestry validators). */
                        IsWellKnownSid(sid, WinCreatorOwnerRightsSid));
        if (!trusted) {
            char label[128];
            LPSTR sid_text = NULL;
            (void)snprintf(label, sizeof(label), "acl-grants-cross-account-mutation to %s",
                           ConvertSidToStringSidA(sid, &sid_text) && sid_text ? sid_text
                                                                              : "unparsable-sid");
            if (sid_text) {
                (void)LocalFree(sid_text);
            }
            activation_note_refusal(label, 0UL);
            secure = false;
        }
    }
    if (!secure) {
        activation_note_refusal("acl-unreadable-or-malformed", result);
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    free(information);
    return secure;
}

static bool activation_windows_identity(HANDLE handle, activation_file_identity_t *identity_out,
                                        bool require_regular) {
    BY_HANDLE_FILE_INFORMATION information;
    if (handle == INVALID_HANDLE_VALUE || GetFileType(handle) != FILE_TYPE_DISK ||
        !GetFileInformationByHandle(handle, &information)) {
        return false;
    }
    if (require_regular && ((information.dwFileAttributes &
                             (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0 ||
                            information.nNumberOfLinks != 1)) {
        return false;
    }
    identity_out->volume_serial = information.dwVolumeSerialNumber;
    identity_out->index_high = information.nFileIndexHigh;
    identity_out->index_low = information.nFileIndexLow;
    return true;
}

static HANDLE activation_windows_directory_open_no_reparse(const char *directory) {
    /* Canonicalize on the PLAIN wide form so GetFullPathNameW resolves any
     * \.\..\ and slash forms; each ancestor is then opened through the
     * extended-length prefix so a deep component past MAX_PATH still opens. */
    wchar_t *input = activation_utf8_to_wide_plain(directory);
    if (!input) {
        return INVALID_HANDLE_VALUE;
    }
    /* Managed-install callers hand in extended-length paths already; drop
     * their prefix for canonicalization and the drive-shape check below —
     * the component walk re-adds it for every open. */
    wchar_t *shape = wcsncmp(input, L"\\\\?\\", 4) == 0 ? input + 4 : input;
    DWORD needed = GetFullPathNameW(shape, 0, NULL, NULL);
    wchar_t *path = needed > 0 ? calloc((size_t)needed + 1U, sizeof(*path)) : NULL;
    DWORD length = path ? GetFullPathNameW(shape, needed + 1U, path, NULL) : 0;
    free(input);
    if (!path || length < 3 || length > needed || path[1] != L':' ||
        (path[2] != L'\\' && path[2] != L'/')) {
        free(path);
        return INVALID_HANDLE_VALUE;
    }
    for (DWORD index = 0; index < length; index++) {
        if (path[index] == L'/') {
            path[index] = L'\\';
        }
    }
    /* prefixed = \\?\ + canonical path; each open truncates this buffer. */
    wchar_t *prefixed = malloc(((size_t)length + 5U) * sizeof(*prefixed));
    if (!prefixed) {
        free(path);
        return INVALID_HANDLE_VALUE;
    }
    wmemcpy(prefixed, L"\\\\?\\", 4);
    wmemcpy(prefixed + 4, path, (size_t)length + 1U);
    HANDLE final_handle = INVALID_HANDLE_VALUE;
    for (DWORD boundary = 3; boundary <= length; boundary++) {
        if (boundary < length && path[boundary] != L'\\') {
            continue;
        }
        if (boundary < length && boundary > 0 && path[boundary - 1] == L'\\') {
            free(prefixed);
            free(path);
            return INVALID_HANDLE_VALUE;
        }
        wchar_t saved = prefixed[boundary + 4U];
        prefixed[boundary + 4U] = L'\0';
        HANDLE handle =
            CreateFileW(prefixed, FILE_READ_ATTRIBUTES | READ_CONTROL,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
        DWORD open_error = handle == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
        prefixed[boundary + 4U] = saved;
        BY_HANDLE_FILE_INFORMATION information;
        bool valid = handle != INVALID_HANDLE_VALUE && GetFileType(handle) == FILE_TYPE_DISK &&
                     GetFileInformationByHandle(handle, &information) &&
                     (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                     (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
        if (!valid) {
            activation_note_refusal(handle == INVALID_HANDLE_VALUE
                                        ? "ancestor-component-open"
                                        : "ancestor-component-not-plain-directory",
                                    open_error);
            if (handle != INVALID_HANDLE_VALUE) {
                (void)CloseHandle(handle);
            }
            free(prefixed);
            free(path);
            return INVALID_HANDLE_VALUE;
        }
        if (boundary == length) {
            final_handle = handle;
        } else {
            (void)CloseHandle(handle);
        }
    }
    free(prefixed);
    free(path);
    return final_handle;
}

/* SOURCE-side directory rule: trusted owner, and cross-account grants are
 * tolerated only for pure add rights (FILE_ADD_FILE/FILE_ADD_SUBDIRECTORY,
 * standard on Downloads-class locations) — they cannot modify the existing
 * source file, whose bytes are pinned by identity recheck plus the staged
 * copy's build-fingerprint validation. */
static bool activation_source_directory_secure(const char *directory,
                                               activation_file_identity_t *identity_out) {
    g_activation_refusal_object = directory;
    HANDLE handle = activation_windows_directory_open_no_reparse(directory);
    BY_HANDLE_FILE_INFORMATION information;
    bool open_ok = handle != INVALID_HANDLE_VALUE &&
                   GetFileInformationByHandle(handle, &information) &&
                   (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                   (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
                   activation_windows_identity(handle, identity_out, false);
    bool owner_ok = open_ok && activation_windows_owner_is_trusted(handle);
    bool acl_ok = owner_ok && activation_windows_acl_check(
                                  handle, FILE_ADD_FILE | (DWORD)FILE_ADD_SUBDIRECTORY);
    if (handle != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(handle);
    }
    g_activation_refusal_object = NULL;
    return acl_ok;
}

static bool activation_directory_secure(const char *directory, int *unused,
                                        activation_file_identity_t *identity_out) {
    (void)unused;
    g_activation_refusal_object = directory;
    HANDLE handle = activation_windows_directory_open_no_reparse(directory);
    BY_HANDLE_FILE_INFORMATION information;
    bool open_ok = handle != INVALID_HANDLE_VALUE &&
                   GetFileInformationByHandle(handle, &information) &&
                   (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                   (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
                   activation_windows_identity(handle, identity_out, false);
    bool owner_ok = open_ok && activation_windows_owner_is_current(handle);
    bool acl_ok = owner_ok && activation_windows_acl_secure(handle);
    if (handle != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(handle);
    }
    g_activation_refusal_object = NULL;
    return acl_ok;
}

#else

static bool activation_posix_acl_empty(int descriptor) {
    return cbm_macos_extended_acl_fd_is_empty(descriptor);
}

static char *activation_posix_walk_path(const char *directory) {
#ifdef __APPLE__
    static const char *const aliases[] = {"/tmp", "/var"};
    for (size_t index = 0; index < sizeof(aliases) / sizeof(aliases[0]); index++) {
        const char *alias = aliases[index];
        size_t alias_length = strlen(alias);
        if (strncmp(directory, alias, alias_length) != 0 ||
            (directory[alias_length] != '\0' && directory[alias_length] != '/')) {
            continue;
        }
        struct stat alias_status;
        char resolved[4096];
        if (lstat(alias, &alias_status) != 0 || !S_ISLNK(alias_status.st_mode) ||
            alias_status.st_uid != 0 || !realpath(alias, resolved)) {
            return NULL;
        }
        struct stat resolved_status;
        if (lstat(resolved, &resolved_status) != 0 || !S_ISDIR(resolved_status.st_mode) ||
            resolved_status.st_uid != 0) {
            return NULL;
        }
        size_t needed = strlen(resolved) + strlen(directory + alias_length) + 1U;
        char *mapped = malloc(needed);
        if (!mapped) {
            return NULL;
        }
        int written = snprintf(mapped, needed, "%s%s", resolved, directory + alias_length);
        if (written <= 0 || (size_t)written >= needed) {
            free(mapped);
            return NULL;
        }
        return mapped;
    }
#endif
    return activation_string_copy(directory);
}

static bool activation_posix_intermediate_secure(const struct stat *status) {
    bool trusted_owner = status->st_uid == 0 || status->st_uid == geteuid();
    bool private_permissions = (status->st_mode & 0022) == 0;
    bool root_sticky = status->st_uid == 0 && (status->st_mode & S_ISVTX) != 0;
    return S_ISDIR(status->st_mode) && trusted_owner && (private_permissions || root_sticky);
}

static bool activation_directory_secure(const char *directory, int *directory_fd_out,
                                        activation_file_identity_t *identity_out) {
    int flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    char *walk_path = activation_posix_walk_path(directory);
    if (!walk_path) {
        return false;
    }
    bool absolute = walk_path[0] == '/';
    int descriptor = open(absolute ? "/" : ".", flags);
    bool ok = descriptor >= 0;
    char *cursor = walk_path;
    while (*cursor == '/') {
        cursor++;
    }
    if (ok && *cursor) {
        struct stat initial_status;
        ok = fstat(descriptor, &initial_status) == 0 &&
             activation_posix_intermediate_secure(&initial_status);
    }
    while (ok && *cursor) {
        char *component = cursor;
        while (*cursor && *cursor != '/') {
            cursor++;
        }
        char saved = *cursor;
        *cursor = '\0';
        if (strcmp(component, ".") == 0) {
            /* Harmless explicit current-directory component. */
        } else if (strcmp(component, "..") == 0 || !component[0]) {
            ok = false;
        } else {
            int next = openat(descriptor, component, flags);
            struct stat next_status;
            bool next_ok = next >= 0 && fstat(next, &next_status) == 0;
            char *remaining = cursor + (saved ? 1 : 0);
            while (*remaining == '/') {
                remaining++;
            }
            if (next_ok && *remaining && !activation_posix_intermediate_secure(&next_status)) {
                next_ok = false;
            }
            if (next_ok) {
                (void)close(descriptor);
                descriptor = next;
            } else {
                if (next >= 0) {
                    (void)close(next);
                }
                ok = false;
            }
        }
        *cursor = saved;
        while (*cursor == '/') {
            cursor++;
        }
    }
    struct stat status;
    ok = ok && fstat(descriptor, &status) == 0 && S_ISDIR(status.st_mode) &&
         status.st_uid == geteuid() && (status.st_mode & 0022) == 0 &&
         activation_posix_acl_empty(descriptor);
    free(walk_path);
    if (!ok) {
        if (descriptor >= 0) {
            (void)close(descriptor);
        }
        return false;
    }
    identity_out->device = status.st_dev;
    identity_out->inode = status.st_ino;
    *directory_fd_out = descriptor;
    return true;
}

#endif

static bool activation_directory_still_valid(const cbm_activation_transaction_t *transaction) {
#ifdef _WIN32
    int ignored = 0;
    activation_file_identity_t current;
    return activation_directory_secure(transaction->directory_path, &ignored, &current) &&
           activation_identity_equal(&current, &transaction->directory_identity);
#else
    struct stat status;
    if (transaction->directory_fd < 0 || fstat(transaction->directory_fd, &status) != 0 ||
        !S_ISDIR(status.st_mode) || status.st_uid != geteuid() || (status.st_mode & 0022) != 0 ||
        !activation_posix_acl_empty(transaction->directory_fd)) {
        return false;
    }
    activation_file_identity_t current = {
        .device = status.st_dev,
        .inode = status.st_ino,
    };
    if (!activation_identity_equal(&current, &transaction->directory_identity)) {
        return false;
    }
    int path_fd = -1;
    activation_file_identity_t path_identity;
    bool path_same =
        activation_directory_secure(transaction->directory_path, &path_fd, &path_identity) &&
        activation_identity_equal(&path_identity, &current);
    if (path_fd >= 0) {
        (void)close(path_fd);
    }
    return path_same;
#endif
}

static bool activation_native_close(activation_native_file_t file) {
#ifdef _WIN32
    return file != INVALID_HANDLE_VALUE && CloseHandle(file) != 0;
#else
    return file >= 0 && close(file) == 0;
#endif
}

static bool activation_native_sync(activation_native_file_t file) {
#ifdef _WIN32
    return FlushFileBuffers(file) != 0;
#else
    int result;
    do {
        result = fsync(file);
    } while (result != 0 && errno == EINTR);
    return result == 0 || errno == EINVAL || errno == ENOTSUP || errno == EROFS;
#endif
}

static bool activation_native_write_all(activation_native_file_t file, const void *data,
                                        size_t length) {
    const unsigned char *cursor = data;
    while (length > 0) {
#ifdef _WIN32
        DWORD chunk = length > (size_t)UINT32_MAX ? UINT32_MAX : (DWORD)length;
        DWORD written = 0;
        if (!WriteFile(file, cursor, chunk, &written, NULL) || written == 0) {
            return false;
        }
        size_t count = (size_t)written;
#else
        ssize_t result;
        do {
            result = write(file, cursor, length);
        } while (result < 0 && errno == EINTR);
        if (result <= 0) {
            return false;
        }
        size_t count = (size_t)result;
#endif
        cursor += count;
        length -= count;
    }
    return true;
}

typedef enum {
    ACTIVATION_CREATE_OK = 0,
    ACTIVATION_CREATE_EXISTS = 1,
    ACTIVATION_CREATE_ERROR = 2,
} activation_create_status_t;

static activation_create_status_t activation_private_file_create(
    const cbm_activation_transaction_t *transaction, const char *path, const char *name,
    activation_native_file_t *file_out, activation_file_identity_t *identity_out) {
    *file_out = ACTIVATION_INVALID_FILE;
    if (!activation_directory_still_valid(transaction)) {
        return ACTIVATION_CREATE_ERROR;
    }
#ifdef _WIN32
    wchar_t *wide = activation_utf8_to_wide(path);
    activation_windows_security_t security;
    if (!wide || !activation_windows_security_init(&security)) {
        free(wide);
        return ACTIVATION_CREATE_ERROR;
    }
    g_activation_refusal_object = path;
    SetLastError(ERROR_SUCCESS);
    HANDLE file = CreateFileW(wide, GENERIC_READ | GENERIC_WRITE | READ_CONTROL | DELETE,
                              FILE_SHARE_READ, &security.attributes, CREATE_NEW,
                              FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    DWORD error = file == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
    free(wide);
    bool valid = file != INVALID_HANDLE_VALUE &&
                 SetHandleInformation(file, HANDLE_FLAG_INHERIT, 0) != 0 &&
                 activation_windows_identity(file, identity_out, true) &&
                 activation_windows_owner_is_current(file) && activation_windows_acl_secure(file);
    activation_windows_security_destroy(&security);
    if (!valid) {
        if (file == INVALID_HANDLE_VALUE && error != ERROR_FILE_EXISTS &&
            error != ERROR_ALREADY_EXISTS) {
            activation_note_refusal("staged-file-create", error);
        }
        g_activation_refusal_object = NULL;
        if (file != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(file);
        }
        return error == ERROR_FILE_EXISTS || error == ERROR_ALREADY_EXISTS
                   ? ACTIVATION_CREATE_EXISTS
                   : ACTIVATION_CREATE_ERROR;
    }
    g_activation_refusal_object = NULL;
    *file_out = file;
    return ACTIVATION_CREATE_OK;
#else
    int flags = O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    int file = openat(transaction->directory_fd, name, flags, 0700);
    int open_error = errno;
    struct stat status;
    bool valid = file >= 0 && fchmod(file, 0700) == 0 && fstat(file, &status) == 0 &&
                 S_ISREG(status.st_mode) && status.st_uid == geteuid() && status.st_nlink == 1 &&
                 (status.st_mode & 0777) == 0700 && activation_posix_acl_empty(file);
    if (!valid) {
        if (file >= 0) {
            (void)close(file);
        }
        return file < 0 && open_error == EEXIST ? ACTIVATION_CREATE_EXISTS
                                                : ACTIVATION_CREATE_ERROR;
    }
    identity_out->device = status.st_dev;
    identity_out->inode = status.st_ino;
    *file_out = file;
    return ACTIVATION_CREATE_OK;
#endif
}

static cbm_activation_transaction_status_t activation_create_unique(
    const cbm_activation_transaction_t *transaction, const char *tag, char **path_out,
    char **name_out, activation_native_file_t *file_out, activation_file_identity_t *identity_out) {
    *path_out = NULL;
    *name_out = NULL;
    *file_out = ACTIVATION_INVALID_FILE;
    for (unsigned int attempt = 0; attempt < 1024U; attempt++) {
        char *candidate = activation_unique_path(transaction->directory_path, tag);
        if (!candidate) {
            return CBM_ACTIVATION_TRANSACTION_NO_MEMORY;
        }
        char *name = activation_path_name_copy(candidate);
        if (!name) {
            free(candidate);
            return CBM_ACTIVATION_TRANSACTION_NO_MEMORY;
        }
        activation_create_status_t created =
            activation_private_file_create(transaction, candidate, name, file_out, identity_out);
        if (created == ACTIVATION_CREATE_OK) {
            *path_out = candidate;
            *name_out = name;
            return CBM_ACTIVATION_TRANSACTION_OK;
        }
        free(candidate);
        free(name);
        if (created != ACTIVATION_CREATE_EXISTS) {
            return CBM_ACTIVATION_TRANSACTION_IO;
        }
    }
    return CBM_ACTIVATION_TRANSACTION_IO;
}

#ifdef _WIN32
static bool activation_external_snapshot_with_owner(const char *path, bool require_current_owner,
                                                    bool *exists_out,
                                                    activation_file_identity_t *identity_out) {
    *exists_out = false;
    wchar_t *wide = activation_utf8_to_wide(path);
    if (!wide) {
        return false;
    }
    DWORD attributes = GetFileAttributesW(wide);
    if (attributes == INVALID_FILE_ATTRIBUTES) {
        DWORD error = GetLastError();
        free(wide);
        return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND;
    }
    if ((attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0) {
        free(wide);
        return false;
    }
    HANDLE handle = CreateFileW(wide, FILE_READ_ATTRIBUTES | READ_CONTROL,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    free(wide);
    bool valid = handle != INVALID_HANDLE_VALUE &&
                 activation_windows_identity(handle, identity_out, true) &&
                 (require_current_owner ? activation_windows_owner_is_current(handle)
                                        : activation_windows_owner_is_trusted(handle)) &&
                 activation_windows_acl_secure(handle);
    if (handle != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(handle);
    }
    if (!valid) {
        return false;
    }
    *exists_out = true;
    return true;
}
#endif

#ifndef _WIN32
static bool activation_posix_entry_snapshot_with_links(
    const cbm_activation_transaction_t *transaction, const char *name, nlink_t required_links,
    bool *exists_out, activation_file_identity_t *identity_out) {
    *exists_out = false;
    if (!activation_directory_still_valid(transaction)) {
        return false;
    }
    struct stat before;
    if (fstatat(transaction->directory_fd, name, &before, AT_SYMLINK_NOFOLLOW) != 0) {
        return errno == ENOENT;
    }
    if (!S_ISREG(before.st_mode) || before.st_uid != geteuid() ||
        before.st_nlink != required_links || (before.st_mode & 0022) != 0) {
        return false;
    }
    int flags = O_RDONLY | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    int file = openat(transaction->directory_fd, name, flags);
    struct stat opened;
    struct stat after;
    bool valid = file >= 0 && fstat(file, &opened) == 0 && S_ISREG(opened.st_mode) &&
                 opened.st_uid == geteuid() && opened.st_nlink == required_links &&
                 (opened.st_mode & 0022) == 0 && opened.st_dev == before.st_dev &&
                 opened.st_ino == before.st_ino && activation_posix_acl_empty(file) &&
                 fstatat(transaction->directory_fd, name, &after, AT_SYMLINK_NOFOLLOW) == 0 &&
                 S_ISREG(after.st_mode) && after.st_uid == geteuid() &&
                 after.st_nlink == required_links && (after.st_mode & 0022) == 0 &&
                 after.st_dev == opened.st_dev && after.st_ino == opened.st_ino;
    bool closed = file >= 0 && close(file) == 0;
    if (!valid || !closed) {
        return false;
    }
    identity_out->device = opened.st_dev;
    identity_out->inode = opened.st_ino;
    *exists_out = true;
    return true;
}
#endif

static bool activation_entry_snapshot(const cbm_activation_transaction_t *transaction,
                                      const char *path, const char *name, bool *exists_out,
                                      activation_file_identity_t *identity_out) {
#ifdef _WIN32
    return activation_directory_still_valid(transaction) &&
           activation_external_snapshot_with_owner(path, true, exists_out, identity_out);
#else
    (void)path;
    return activation_posix_entry_snapshot_with_links(transaction, name, (nlink_t)1, exists_out,
                                                      identity_out);
#endif
}

static bool activation_path_matches(const cbm_activation_transaction_t *transaction,
                                    const char *path, const char *name,
                                    const activation_file_identity_t *expected, bool *exists_out) {
    activation_file_identity_t actual;
    bool exists = false;
    if (!activation_entry_snapshot(transaction, path, name, &exists, &actual)) {
        return false;
    }
    *exists_out = exists;
    return !exists || activation_identity_equal(&actual, expected);
}

static bool activation_sync_directory(const cbm_activation_transaction_t *transaction) {
#ifdef _WIN32
    /* MoveFileExW(MOVEFILE_WRITE_THROUGH) is the strongest portable
     * directory-entry durability primitive available here. */
    (void)transaction;
    return true;
#else
    int result;
    do {
        result = fsync(transaction->directory_fd);
    } while (result != 0 && errno == EINTR);
    return result == 0 || errno == EINVAL || errno == ENOTSUP || errno == EROFS;
#endif
}

static bool activation_rename(const cbm_activation_transaction_t *transaction, const char *source,
                              const char *source_name, const char *destination,
                              const char *destination_name, bool replace_destination) {
    if (!activation_directory_still_valid(transaction)) {
        return false;
    }
#ifdef _WIN32
    wchar_t *wide_source = activation_utf8_to_wide(source);
    wchar_t *wide_destination = activation_utf8_to_wide(destination);
    DWORD flags = MOVEFILE_WRITE_THROUGH | (replace_destination ? MOVEFILE_REPLACE_EXISTING : 0);
    bool ok = wide_source && wide_destination &&
              MoveFileExW(wide_source, wide_destination, flags) != 0 &&
              activation_directory_still_valid(transaction);
    free(wide_source);
    free(wide_destination);
    return ok;
#else
    (void)source;
    (void)destination;
    (void)replace_destination;
    int result;
    do {
        result = renameat(transaction->directory_fd, source_name, transaction->directory_fd,
                          destination_name);
    } while (result != 0 && errno == EINTR);
    return result == 0;
#endif
}

typedef enum {
    ACTIVATION_UNLINK_OK = 0,
    ACTIVATION_UNLINK_DEFERRED = 1,
    ACTIVATION_UNLINK_ERROR = 2,
} activation_unlink_status_t;

static activation_unlink_status_t activation_unlink_expected(
    const cbm_activation_transaction_t *transaction, const char *path, const char *name,
    const activation_file_identity_t *expected, bool allow_windows_deferred) {
    bool exists = false;
    if (!activation_path_matches(transaction, path, name, expected, &exists)) {
        return ACTIVATION_UNLINK_ERROR;
    }
    if (!exists) {
        return ACTIVATION_UNLINK_OK;
    }
#ifdef _WIN32
    wchar_t *wide = activation_utf8_to_wide(path);
    if (!wide) {
        return ACTIVATION_UNLINK_ERROR;
    }
    if (DeleteFileW(wide)) {
        free(wide);
        return ACTIVATION_UNLINK_OK;
    }
    DWORD error = GetLastError();
    bool deferred = allow_windows_deferred &&
                    (error == ERROR_ACCESS_DENIED || error == ERROR_SHARING_VIOLATION ||
                     error == ERROR_LOCK_VIOLATION) &&
                    MoveFileExW(wide, NULL, MOVEFILE_DELAY_UNTIL_REBOOT) != 0;
    free(wide);
    return deferred ? ACTIVATION_UNLINK_DEFERRED : ACTIVATION_UNLINK_ERROR;
#else
    (void)allow_windows_deferred;
    int result;
    do {
        result = unlinkat(transaction->directory_fd, name, 0);
    } while (result != 0 && errno == EINTR);
    return result == 0 || errno == ENOENT ? ACTIVATION_UNLINK_OK : ACTIVATION_UNLINK_ERROR;
#endif
}

static void activation_transaction_destroy(cbm_activation_transaction_t *transaction) {
    if (!transaction) {
        return;
    }
#ifndef _WIN32
    if (transaction->directory_fd >= 0) {
        (void)close(transaction->directory_fd);
    }
#endif
    free(transaction->target_path);
    free(transaction->directory_path);
    free(transaction->target_name);
    free(transaction->staged_path);
    free(transaction->staged_name);
    free(transaction->backup_path);
    free(transaction->backup_name);
    free(transaction);
}

static cbm_activation_transaction_status_t activation_discard_staged_assets(
    cbm_activation_transaction_t *transaction) {
    bool ok = true;
    if (transaction->staged_exists) {
        activation_unlink_status_t removed = activation_unlink_expected(
            transaction, transaction->staged_path, transaction->staged_name,
            &transaction->staged_identity, false);
        ok = removed == ACTIVATION_UNLINK_OK && ok;
        if (removed == ACTIVATION_UNLINK_OK) {
            transaction->staged_exists = false;
        }
    }
    if (transaction->backup_exists && !transaction->backup_contains_target) {
        activation_unlink_status_t removed = activation_unlink_expected(
            transaction, transaction->backup_path, transaction->backup_name,
            &transaction->backup_identity, false);
        ok = removed == ACTIVATION_UNLINK_OK && ok;
        if (removed == ACTIVATION_UNLINK_OK) {
            transaction->backup_exists = false;
        }
    }
    ok = activation_sync_directory(transaction) && ok;
    return ok ? CBM_ACTIVATION_TRANSACTION_OK : CBM_ACTIVATION_TRANSACTION_IO;
}

static cbm_activation_transaction_status_t activation_transaction_prepare(
    const char *target_path, activation_action_t action,
    cbm_activation_transaction_t **transaction_out) {
    *transaction_out = NULL;
    activation_refusal_clear();
    cbm_activation_transaction_t *transaction = calloc(1, sizeof(*transaction));
    if (!transaction) {
        return CBM_ACTIVATION_TRANSACTION_NO_MEMORY;
    }
#ifndef _WIN32
    transaction->directory_fd = -1;
#endif
    transaction->action = action;
    transaction->state = ACTIVATION_STAGED;
    transaction->target_path = activation_string_copy(target_path);
    if (!transaction->target_path) {
        activation_transaction_destroy(transaction);
        return CBM_ACTIVATION_TRANSACTION_NO_MEMORY;
    }
    if (!activation_target_parts(target_path, &transaction->directory_path,
                                 &transaction->target_name)) {
        activation_transaction_destroy(transaction);
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
#ifdef _WIN32
    int ignored = 0;
    if (!activation_directory_secure(transaction->directory_path, &ignored,
                                     &transaction->directory_identity)) {
#else
    if (!activation_directory_secure(transaction->directory_path, &transaction->directory_fd,
                                     &transaction->directory_identity)) {
#endif
        activation_transaction_destroy(transaction);
        return CBM_ACTIVATION_TRANSACTION_IO;
    }
    if (!activation_entry_snapshot(transaction, transaction->target_path, transaction->target_name,
                                   &transaction->target_existed, &transaction->target_identity)) {
        activation_note_refusal("target-entry-snapshot", 0UL);
        activation_transaction_destroy(transaction);
        return CBM_ACTIVATION_TRANSACTION_IO;
    }
    if (transaction->target_existed) {
        activation_native_file_t reservation = ACTIVATION_INVALID_FILE;
        cbm_activation_transaction_status_t status = activation_create_unique(
            transaction, "backup", &transaction->backup_path, &transaction->backup_name,
            &reservation, &transaction->backup_identity);
        if (status != CBM_ACTIVATION_TRANSACTION_OK) {
            activation_transaction_destroy(transaction);
            return status;
        }
        transaction->backup_exists = true;
        bool durable = activation_native_sync(reservation);
        bool closed = activation_native_close(reservation);
        if (!durable || !closed || !activation_sync_directory(transaction)) {
            (void)activation_discard_staged_assets(transaction);
            activation_transaction_destroy(transaction);
            return CBM_ACTIVATION_TRANSACTION_IO;
        }
    }
    *transaction_out = transaction;
    return CBM_ACTIVATION_TRANSACTION_OK;
}

static void activation_failed_stage_cleanup(cbm_activation_transaction_t *transaction) {
    if (transaction) {
        (void)activation_discard_staged_assets(transaction);
        activation_transaction_destroy(transaction);
    }
}

cbm_activation_transaction_status_t cbm_activation_transaction_stage_bytes(
    const char *target_path, const void *candidate, size_t candidate_size,
    cbm_activation_transaction_t **transaction_out) {
    if (transaction_out) {
        *transaction_out = NULL;
    }
    if (!target_path || !candidate || candidate_size == 0 || !transaction_out) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    cbm_activation_transaction_t *transaction = NULL;
    cbm_activation_transaction_status_t status =
        activation_transaction_prepare(target_path, ACTIVATION_REPLACE, &transaction);
    if (status != CBM_ACTIVATION_TRANSACTION_OK) {
        return status;
    }
    activation_native_file_t staged = ACTIVATION_INVALID_FILE;
    status =
        activation_create_unique(transaction, "stage", &transaction->staged_path,
                                 &transaction->staged_name, &staged, &transaction->staged_identity);
    if (status != CBM_ACTIVATION_TRANSACTION_OK) {
        activation_failed_stage_cleanup(transaction);
        return status;
    }
    transaction->staged_exists = true;
    bool written = activation_native_write_all(staged, candidate, candidate_size);
    bool durable = written && activation_native_sync(staged);
    bool closed = activation_native_close(staged);
    if (!written || !durable || !closed || !activation_sync_directory(transaction)) {
        activation_failed_stage_cleanup(transaction);
        return CBM_ACTIVATION_TRANSACTION_IO;
    }
    *transaction_out = transaction;
    return CBM_ACTIVATION_TRANSACTION_OK;
}

static bool activation_source_open(const char *path, activation_native_file_t *file_out) {
    *file_out = ACTIVATION_INVALID_FILE;
    char *directory = NULL;
    char *name = NULL;
    if (!activation_target_parts(path, &directory, &name)) {
        return false;
    }
#ifdef _WIN32
    activation_file_identity_t directory_identity;
    activation_file_identity_t expected;
    bool exists = false;
    bool src_dir_ok = activation_source_directory_secure(directory, &directory_identity);
    /* The source (a downloaded release bundle) follows the machine's
     * default-owner policy; its bytes are pinned by identity recheck plus the
     * staged copy's build-fingerprint validation, so a trusted owner
     * (user/Administrators/SYSTEM) is sufficient here. Targets keep the
     * exact-current-owner rule. */
    bool snapshot_ok =
        src_dir_ok && activation_external_snapshot_with_owner(path, false, &exists, &expected);
    if (!src_dir_ok || !snapshot_ok || !exists) {
        free(directory);
        free(name);
        return false;
    }
    wchar_t *wide = activation_utf8_to_wide(path);
    if (!wide) {
        free(directory);
        free(name);
        return false;
    }
    HANDLE file =
        CreateFileW(wide, GENERIC_READ | FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    free(wide);
    activation_file_identity_t actual;
    activation_file_identity_t directory_now;
    bool valid = file != INVALID_HANDLE_VALUE && activation_windows_identity(file, &actual, true) &&
                 activation_windows_owner_is_trusted(file) && activation_windows_acl_secure(file) &&
                 activation_identity_equal(&actual, &expected) &&
                 activation_source_directory_secure(directory, &directory_now) &&
                 activation_identity_equal(&directory_now, &directory_identity);
    if (!valid) {
        if (file != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(file);
        }
        free(directory);
        free(name);
        return false;
    }
#else
    int directory_fd = -1;
    activation_file_identity_t directory_identity;
    if (!activation_directory_secure(directory, &directory_fd, &directory_identity)) {
        free(directory);
        free(name);
        return false;
    }
    struct stat before;
    bool before_valid = fstatat(directory_fd, name, &before, AT_SYMLINK_NOFOLLOW) == 0 &&
                        S_ISREG(before.st_mode) && before.st_uid == geteuid() &&
                        before.st_nlink == 1 && (before.st_mode & 0022) == 0;
    int flags = O_RDONLY | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    int file = before_valid ? openat(directory_fd, name, flags) : -1;
    struct stat information;
    bool valid = file >= 0 && fstat(file, &information) == 0 && S_ISREG(information.st_mode) &&
                 information.st_uid == geteuid() && information.st_nlink == 1 &&
                 (information.st_mode & 0022) == 0 && activation_posix_acl_empty(file) &&
                 information.st_dev == before.st_dev && information.st_ino == before.st_ino;
    (void)close(directory_fd);
    if (!valid) {
        if (file >= 0) {
            (void)close(file);
        }
        free(directory);
        free(name);
        return false;
    }
#endif
    free(directory);
    free(name);
    *file_out = file;
    return true;
}

static bool activation_native_read(activation_native_file_t file, void *buffer, size_t capacity,
                                   size_t *read_out) {
#ifdef _WIN32
    DWORD amount = 0;
    DWORD request = capacity > (size_t)UINT32_MAX ? UINT32_MAX : (DWORD)capacity;
    if (!ReadFile(file, buffer, request, &amount, NULL)) {
        return false;
    }
    *read_out = (size_t)amount;
    return true;
#else
    ssize_t result;
    do {
        result = read(file, buffer, capacity);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
        return false;
    }
    *read_out = (size_t)result;
    return true;
#endif
}

cbm_activation_transaction_status_t cbm_activation_transaction_stage_file(
    const char *target_path, const char *candidate_path,
    cbm_activation_transaction_t **transaction_out) {
    if (transaction_out) {
        *transaction_out = NULL;
    }
    if (!target_path || !candidate_path || !candidate_path[0] || !transaction_out) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    cbm_activation_transaction_t *transaction = NULL;
    cbm_activation_transaction_status_t status =
        activation_transaction_prepare(target_path, ACTIVATION_REPLACE, &transaction);
    if (status != CBM_ACTIVATION_TRANSACTION_OK) {
        return status;
    }
    activation_native_file_t source = ACTIVATION_INVALID_FILE;
    if (!activation_source_open(candidate_path, &source)) {
        activation_failed_stage_cleanup(transaction);
        return CBM_ACTIVATION_TRANSACTION_IO;
    }
    activation_native_file_t staged = ACTIVATION_INVALID_FILE;
    status =
        activation_create_unique(transaction, "stage", &transaction->staged_path,
                                 &transaction->staged_name, &staged, &transaction->staged_identity);
    if (status != CBM_ACTIVATION_TRANSACTION_OK) {
        (void)activation_native_close(source);
        activation_failed_stage_cleanup(transaction);
        return status;
    }
    transaction->staged_exists = true;
    unsigned char buffer[64U * 1024U];
    size_t total = 0;
    bool copied = true;
    for (;;) {
        size_t amount = 0;
        if (!activation_native_read(source, buffer, sizeof(buffer), &amount)) {
            copied = false;
            break;
        }
        if (amount == 0) {
            break;
        }
        if (SIZE_MAX - total < amount || !activation_native_write_all(staged, buffer, amount)) {
            copied = false;
            break;
        }
        total += amount;
    }
    bool durable = copied && total > 0 && activation_native_sync(staged);
    bool source_closed = activation_native_close(source);
    bool staged_closed = activation_native_close(staged);
    if (!copied || total == 0 || !durable || !source_closed || !staged_closed ||
        !activation_sync_directory(transaction)) {
        activation_failed_stage_cleanup(transaction);
        return CBM_ACTIVATION_TRANSACTION_IO;
    }
    *transaction_out = transaction;
    return CBM_ACTIVATION_TRANSACTION_OK;
}

cbm_activation_transaction_status_t cbm_activation_transaction_stage_removal(
    const char *target_path, cbm_activation_transaction_t **transaction_out) {
    if (transaction_out) {
        *transaction_out = NULL;
    }
    if (!target_path || !transaction_out) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    return activation_transaction_prepare(target_path, ACTIVATION_REMOVE, transaction_out);
}

#ifdef _WIN32
static bool activation_windows_copy_target_to_backup(cbm_activation_transaction_t *transaction) {
    if (!activation_directory_still_valid(transaction)) {
        return false;
    }
    wchar_t *target_path = activation_utf8_to_wide(transaction->target_path);
    wchar_t *backup_path = activation_utf8_to_wide(transaction->backup_path);
    if (!target_path || !backup_path) {
        free(target_path);
        free(backup_path);
        return false;
    }
    HANDLE target =
        CreateFileW(target_path, GENERIC_READ | FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    HANDLE backup =
        CreateFileW(backup_path, GENERIC_READ | GENERIC_WRITE | FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    free(target_path);
    free(backup_path);
    activation_file_identity_t target_identity;
    activation_file_identity_t backup_identity;
    LARGE_INTEGER beginning = {.QuadPart = 0};
    bool valid = target != INVALID_HANDLE_VALUE && backup != INVALID_HANDLE_VALUE &&
                 activation_windows_identity(target, &target_identity, true) &&
                 activation_windows_identity(backup, &backup_identity, true) &&
                 activation_windows_owner_is_current(target) &&
                 activation_windows_owner_is_current(backup) &&
                 activation_windows_acl_secure(target) && activation_windows_acl_secure(backup) &&
                 activation_identity_equal(&target_identity, &transaction->target_identity) &&
                 activation_identity_equal(&backup_identity, &transaction->backup_identity) &&
                 SetFilePointerEx(backup, beginning, NULL, FILE_BEGIN) != 0 &&
                 SetEndOfFile(backup) != 0;
    unsigned char buffer[64U * 1024U];
    while (valid) {
        DWORD amount = 0;
        valid = ReadFile(target, buffer, (DWORD)sizeof(buffer), &amount, NULL) != 0;
        if (!valid || amount == 0) {
            break;
        }
        valid = activation_native_write_all(backup, buffer, (size_t)amount);
    }
    valid = valid && activation_native_sync(backup);
    bool target_closed = target != INVALID_HANDLE_VALUE && CloseHandle(target) != 0;
    bool backup_closed = backup != INVALID_HANDLE_VALUE && CloseHandle(backup) != 0;
    return valid && target_closed && backup_closed && activation_directory_still_valid(transaction);
}
#endif

typedef enum {
    ACTIVATION_PUBLISH_OK = 0,
    ACTIVATION_PUBLISH_UNCHANGED_ERROR = 1,
    ACTIVATION_PUBLISH_CHANGED_ERROR = 2,
} activation_publish_status_t;

#if defined(__APPLE__) || (defined(__linux__) && defined(SYS_renameat2))
static bool activation_noreplace_primitive_unavailable(int error) {
    bool unavailable = error == ENOSYS || error == EINVAL;
#ifdef ENOTSUP
    unavailable = unavailable || error == ENOTSUP;
#endif
#ifdef EOPNOTSUPP
    unavailable = unavailable || error == EOPNOTSUPP;
#endif
    return unavailable;
}
#endif

#ifndef _WIN32
/* Portable last resort for platforms/filesystems without a no-replace rename.
 * linkat() atomically claims an absent destination.  Until the staging link is
 * removed both names deliberately retain the same verified inode, and the
 * transaction's staged_exists flag records that partial publication so
 * rollback can remove only the target link. */
static activation_publish_status_t activation_publish_absent_link_fallback(
    cbm_activation_transaction_t *transaction) {
    if (!activation_directory_still_valid(transaction)) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    int linked;
    do {
        linked = linkat(transaction->directory_fd, transaction->staged_name,
                        transaction->directory_fd, transaction->target_name, 0);
    } while (linked != 0 && errno == EINTR);
    if (linked != 0) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }

    activation_file_identity_t staged_identity;
    activation_file_identity_t target_identity;
    bool staged_exists = false;
    bool target_exists = false;
    bool linked_pair =
        activation_posix_entry_snapshot_with_links(transaction, transaction->staged_name,
                                                   (nlink_t)2, &staged_exists, &staged_identity) &&
        staged_exists &&
        activation_identity_equal(&staged_identity, &transaction->staged_identity) &&
        activation_posix_entry_snapshot_with_links(transaction, transaction->target_name,
                                                   (nlink_t)2, &target_exists, &target_identity) &&
        target_exists && activation_identity_equal(&target_identity, &transaction->staged_identity);
    if (!linked_pair) {
        return ACTIVATION_PUBLISH_CHANGED_ERROR;
    }

    int removed;
    do {
        removed = unlinkat(transaction->directory_fd, transaction->staged_name, 0);
    } while (removed != 0 && errno == EINTR);
    if (removed != 0 && errno != ENOENT) {
        return ACTIVATION_PUBLISH_CHANGED_ERROR;
    }
    transaction->staged_exists = false;

    activation_file_identity_t published_identity;
    bool published_exists = false;
    if (!activation_posix_entry_snapshot_with_links(transaction, transaction->target_name,
                                                    (nlink_t)1, &published_exists,
                                                    &published_identity) ||
        !published_exists ||
        !activation_identity_equal(&published_identity, &transaction->staged_identity)) {
        return ACTIVATION_PUBLISH_CHANGED_ERROR;
    }
    return ACTIVATION_PUBLISH_OK;
}
#endif

static activation_publish_status_t activation_finish_absent_publish(
    cbm_activation_transaction_t *transaction) {
    transaction->staged_exists = false;
    bool target_exists = false;
    return activation_path_matches(transaction, transaction->target_path, transaction->target_name,
                                   &transaction->staged_identity, &target_exists) &&
                   target_exists
               ? ACTIVATION_PUBLISH_OK
               : ACTIVATION_PUBLISH_CHANGED_ERROR;
}

static activation_publish_status_t activation_publish_absent_replacement(
    cbm_activation_transaction_t *transaction) {
    if (!activation_directory_still_valid(transaction)) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    bool staged_exists = false;
    if (!activation_path_matches(transaction, transaction->staged_path, transaction->staged_name,
                                 &transaction->staged_identity, &staged_exists) ||
        !staged_exists) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
#ifdef _WIN32
    wchar_t *source = activation_utf8_to_wide(transaction->staged_path);
    wchar_t *destination = activation_utf8_to_wide(transaction->target_path);
    bool published =
        source && destination && MoveFileExW(source, destination, MOVEFILE_WRITE_THROUGH) != 0;
    free(source);
    free(destination);
    if (!published) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    return activation_finish_absent_publish(transaction);
#elif defined(__APPLE__)
    int renamed;
    do {
        renamed = renameatx_np(transaction->directory_fd, transaction->staged_name,
                               transaction->directory_fd, transaction->target_name, RENAME_EXCL);
    } while (renamed != 0 && errno == EINTR);
    if (renamed == 0) {
        return activation_finish_absent_publish(transaction);
    }
    if (!activation_noreplace_primitive_unavailable(errno)) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    return activation_publish_absent_link_fallback(transaction);
#elif defined(__linux__) && defined(SYS_renameat2)
    const unsigned int rename_noreplace = 1U;
    long renamed;
    do {
        renamed = syscall(SYS_renameat2, transaction->directory_fd, transaction->staged_name,
                          transaction->directory_fd, transaction->target_name, rename_noreplace);
    } while (renamed != 0 && errno == EINTR);
    if (renamed == 0) {
        return activation_finish_absent_publish(transaction);
    }
    if (!activation_noreplace_primitive_unavailable(errno)) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    return activation_publish_absent_link_fallback(transaction);
#else
    return activation_publish_absent_link_fallback(transaction);
#endif
}

#ifndef _WIN32
static bool activation_linked_backup_pair_valid(const cbm_activation_transaction_t *transaction) {
    activation_file_identity_t target_identity;
    activation_file_identity_t backup_identity;
    bool target_exists = false;
    bool backup_exists = false;
    return activation_posix_entry_snapshot_with_links(transaction, transaction->target_name,
                                                      (nlink_t)2, &target_exists,
                                                      &target_identity) &&
           target_exists &&
           activation_identity_equal(&target_identity, &transaction->target_identity) &&
           activation_posix_entry_snapshot_with_links(transaction, transaction->backup_name,
                                                      (nlink_t)2, &backup_exists,
                                                      &backup_identity) &&
           backup_exists &&
           activation_identity_equal(&backup_identity, &transaction->target_identity);
}

static bool activation_remove_linked_backup(cbm_activation_transaction_t *transaction) {
    if (!activation_linked_backup_pair_valid(transaction)) {
        return false;
    }
    int result;
    do {
        result = unlinkat(transaction->directory_fd, transaction->backup_name, 0);
    } while (result != 0 && errno == EINTR);
    if (result == 0) {
        transaction->backup_exists = false;
        transaction->backup_contains_target = false;
    }
    return result == 0;
}
#endif

/* Publish over an existing target without a disappearance window. POSIX
 * retains the old inode through a same-directory hard link before renameat.
 * Windows copies the verified old bytes into the already-private backup, then
 * MoveFileExW atomically replaces the target with the private staged file. */
static activation_publish_status_t activation_publish_existing_replacement(
    cbm_activation_transaction_t *transaction) {
#ifdef _WIN32
    transaction->backup_contains_target = false;
    if (!activation_windows_copy_target_to_backup(transaction)) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    if (!activation_rename(transaction, transaction->staged_path, transaction->staged_name,
                           transaction->target_path, transaction->target_name, true)) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    transaction->staged_exists = false;
    transaction->backup_contains_target = true;
    bool target_exists = false;
    if (!activation_path_matches(transaction, transaction->target_path, transaction->target_name,
                                 &transaction->staged_identity, &target_exists) ||
        !target_exists) {
        return ACTIVATION_PUBLISH_CHANGED_ERROR;
    }
#else
    activation_unlink_status_t reservation_removed =
        activation_unlink_expected(transaction, transaction->backup_path, transaction->backup_name,
                                   &transaction->backup_identity, false);
    if (reservation_removed != ACTIVATION_UNLINK_OK) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    transaction->backup_exists = false;
    if (!activation_directory_still_valid(transaction) ||
        linkat(transaction->directory_fd, transaction->target_name, transaction->directory_fd,
               transaction->backup_name, 0) != 0) {
        return ACTIVATION_PUBLISH_UNCHANGED_ERROR;
    }
    transaction->backup_exists = true;
    transaction->backup_contains_target = true;
    transaction->backup_identity = transaction->target_identity;
    bool staged_exists = false;
    if (!activation_linked_backup_pair_valid(transaction) ||
        !activation_path_matches(transaction, transaction->staged_path, transaction->staged_name,
                                 &transaction->staged_identity, &staged_exists) ||
        !staged_exists) {
        return activation_remove_linked_backup(transaction) ? ACTIVATION_PUBLISH_UNCHANGED_ERROR
                                                            : ACTIVATION_PUBLISH_CHANGED_ERROR;
    }
    if (!activation_sync_directory(transaction)) {
        return activation_remove_linked_backup(transaction) ? ACTIVATION_PUBLISH_UNCHANGED_ERROR
                                                            : ACTIVATION_PUBLISH_CHANGED_ERROR;
    }
    if (!activation_rename(transaction, transaction->staged_path, transaction->staged_name,
                           transaction->target_path, transaction->target_name, true)) {
        return activation_remove_linked_backup(transaction) ? ACTIVATION_PUBLISH_UNCHANGED_ERROR
                                                            : ACTIVATION_PUBLISH_CHANGED_ERROR;
    }
    transaction->staged_exists = false;
#endif
    return ACTIVATION_PUBLISH_OK;
}

static bool activation_target_still_original(const cbm_activation_transaction_t *transaction) {
    activation_file_identity_t current;
    bool exists = false;
    if (!activation_entry_snapshot(transaction, transaction->target_path, transaction->target_name,
                                   &exists, &current) ||
        exists != transaction->target_existed) {
        return false;
    }
    return !exists || activation_identity_equal(&current, &transaction->target_identity);
}

static bool activation_absent_target_snapshot(const cbm_activation_transaction_t *transaction,
                                              bool *exists_out, bool *linked_pair_out) {
    *exists_out = false;
    *linked_pair_out = false;
    if (activation_path_matches(transaction, transaction->target_path, transaction->target_name,
                                &transaction->staged_identity, exists_out)) {
        return true;
    }
#ifndef _WIN32
    if (!transaction->staged_exists) {
        return false;
    }
    activation_file_identity_t staged_identity;
    activation_file_identity_t target_identity;
    bool staged_exists = false;
    bool target_exists = false;
    if (!activation_posix_entry_snapshot_with_links(transaction, transaction->target_name,
                                                    (nlink_t)2, &target_exists, &target_identity) ||
        !target_exists ||
        !activation_identity_equal(&target_identity, &transaction->staged_identity) ||
        !activation_posix_entry_snapshot_with_links(transaction, transaction->staged_name,
                                                    (nlink_t)2, &staged_exists, &staged_identity) ||
        !staged_exists ||
        !activation_identity_equal(&staged_identity, &transaction->staged_identity)) {
        return false;
    }
    *exists_out = true;
    *linked_pair_out = true;
    return true;
#else
    return false;
#endif
}

static bool activation_remove_absent_published_target(cbm_activation_transaction_t *transaction,
                                                      bool linked_pair) {
    if (!linked_pair) {
        return activation_unlink_expected(transaction, transaction->target_path,
                                          transaction->target_name, &transaction->staged_identity,
                                          false) == ACTIVATION_UNLINK_OK;
    }
#ifndef _WIN32
    bool target_exists = false;
    bool still_linked = false;
    if (!activation_absent_target_snapshot(transaction, &target_exists, &still_linked) ||
        !target_exists || !still_linked) {
        return false;
    }
    int removed;
    do {
        removed = unlinkat(transaction->directory_fd, transaction->target_name, 0);
    } while (removed != 0 && errno == EINTR);
    if (removed != 0) {
        return false;
    }
    activation_file_identity_t staged_identity;
    bool staged_exists = false;
    activation_file_identity_t absent_identity;
    bool target_remains = false;
    return activation_posix_entry_snapshot_with_links(transaction, transaction->staged_name,
                                                      (nlink_t)1, &staged_exists,
                                                      &staged_identity) &&
           staged_exists &&
           activation_identity_equal(&staged_identity, &transaction->staged_identity) &&
           activation_posix_entry_snapshot_with_links(transaction, transaction->target_name,
                                                      (nlink_t)1, &target_remains,
                                                      &absent_identity) &&
           !target_remains;
#else
    return false;
#endif
}

static cbm_activation_transaction_status_t activation_rollback_internal(
    cbm_activation_transaction_t *transaction) {
    if (transaction->target_existed) {
        bool backup_exists = false;
        if (!transaction->backup_contains_target ||
            !activation_path_matches(transaction, transaction->backup_path,
                                     transaction->backup_name, &transaction->backup_identity,
                                     &backup_exists) ||
            !backup_exists) {
            transaction->state = ACTIVATION_RECOVERY_NEEDED;
            return CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
        }
        bool target_exists = false;
        activation_file_identity_t current_target;
        if (!activation_entry_snapshot(transaction, transaction->target_path,
                                       transaction->target_name, &target_exists, &current_target)) {
            transaction->state = ACTIVATION_RECOVERY_NEEDED;
            return CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
        }
        if (target_exists &&
            (transaction->action != ACTIVATION_REPLACE ||
             !activation_identity_equal(&current_target, &transaction->staged_identity))) {
            transaction->state = ACTIVATION_RECOVERY_NEEDED;
            return CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
        }
        if (!activation_rename(transaction, transaction->backup_path, transaction->backup_name,
                               transaction->target_path, transaction->target_name, target_exists)) {
            transaction->state = ACTIVATION_RECOVERY_NEEDED;
            return CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
        }
        transaction->backup_exists = false;
        transaction->backup_contains_target = false;
    } else if (transaction->action == ACTIVATION_REPLACE) {
        bool target_exists = false;
        bool linked_pair = false;
        if (!activation_absent_target_snapshot(transaction, &target_exists, &linked_pair)) {
            transaction->state = ACTIVATION_RECOVERY_NEEDED;
            return CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
        }
        if (target_exists && !activation_remove_absent_published_target(transaction, linked_pair)) {
            transaction->state = ACTIVATION_RECOVERY_NEEDED;
            return CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
        }
    }
    transaction->state = ACTIVATION_ROLLED_BACK;
    return activation_sync_directory(transaction) ? CBM_ACTIVATION_TRANSACTION_OK
                                                  : CBM_ACTIVATION_TRANSACTION_IO;
}

cbm_activation_transaction_status_t cbm_activation_transaction_commit(
    cbm_activation_transaction_t *transaction, cbm_activation_transaction_validator_fn validator,
    void *validator_context) {
    if (!transaction) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    if (transaction->state != ACTIVATION_STAGED) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_STATE;
    }
    if (!activation_target_still_original(transaction)) {
        return CBM_ACTIVATION_TRANSACTION_IO;
    }
    if (transaction->action == ACTIVATION_REPLACE) {
        bool staged_exists = false;
        if (!transaction->staged_exists ||
            !activation_path_matches(transaction, transaction->staged_path,
                                     transaction->staged_name, &transaction->staged_identity,
                                     &staged_exists) ||
            !staged_exists) {
            return CBM_ACTIVATION_TRANSACTION_IO;
        }
    }
    if (transaction->target_existed) {
        if (transaction->action == ACTIVATION_REPLACE) {
            activation_publish_status_t published =
                activation_publish_existing_replacement(transaction);
            if (published == ACTIVATION_PUBLISH_UNCHANGED_ERROR) {
                return CBM_ACTIVATION_TRANSACTION_IO;
            }
            if (published == ACTIVATION_PUBLISH_CHANGED_ERROR) {
                transaction->state = ACTIVATION_RECOVERY_NEEDED;
                cbm_activation_transaction_status_t restored =
                    activation_rollback_internal(transaction);
                return restored == CBM_ACTIVATION_TRANSACTION_OK
                           ? CBM_ACTIVATION_TRANSACTION_IO
                           : CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
            }
        } else {
            bool reservation_exists = false;
            if (!transaction->backup_exists ||
                !activation_path_matches(transaction, transaction->backup_path,
                                         transaction->backup_name, &transaction->backup_identity,
                                         &reservation_exists) ||
                !reservation_exists ||
                !activation_rename(transaction, transaction->target_path, transaction->target_name,
                                   transaction->backup_path, transaction->backup_name, true)) {
                return CBM_ACTIVATION_TRANSACTION_IO;
            }
            transaction->backup_identity = transaction->target_identity;
            transaction->backup_contains_target = true;
        }
    }
    if (transaction->action == ACTIVATION_REPLACE && !transaction->target_existed) {
        if (activation_before_absent_publish_for_test) {
            activation_before_absent_publish_for_test(
                transaction->target_path, activation_before_absent_publish_context_for_test);
        }
        activation_publish_status_t published = activation_publish_absent_replacement(transaction);
        if (published == ACTIVATION_PUBLISH_UNCHANGED_ERROR) {
            return CBM_ACTIVATION_TRANSACTION_IO;
        }
        if (published == ACTIVATION_PUBLISH_CHANGED_ERROR) {
            transaction->state = ACTIVATION_RECOVERY_NEEDED;
            cbm_activation_transaction_status_t restored =
                activation_rollback_internal(transaction);
            return restored == CBM_ACTIVATION_TRANSACTION_OK
                       ? CBM_ACTIVATION_TRANSACTION_IO
                       : CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
        }
    }
    transaction->state = ACTIVATION_COMMITTED;
    if (!activation_directory_still_valid(transaction) || !activation_sync_directory(transaction)) {
        cbm_activation_transaction_status_t restored = activation_rollback_internal(transaction);
        return restored == CBM_ACTIVATION_TRANSACTION_OK
                   ? CBM_ACTIVATION_TRANSACTION_IO
                   : CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
    }
    if (validator && !validator(transaction->target_path, validator_context)) {
        cbm_activation_transaction_status_t restored = activation_rollback_internal(transaction);
        return restored == CBM_ACTIVATION_TRANSACTION_OK
                   ? CBM_ACTIVATION_TRANSACTION_VALIDATION_FAILED
                   : CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
    }
    return CBM_ACTIVATION_TRANSACTION_OK;
}

cbm_activation_transaction_status_t cbm_activation_transaction_rollback(
    cbm_activation_transaction_t *transaction) {
    if (!transaction) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    if (transaction->state != ACTIVATION_COMMITTED &&
        transaction->state != ACTIVATION_RECOVERY_NEEDED) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_STATE;
    }
    return activation_rollback_internal(transaction);
}

cbm_activation_transaction_status_t cbm_activation_transaction_finalize(
    cbm_activation_transaction_t *transaction) {
    if (!transaction) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    if (transaction->state != ACTIVATION_COMMITTED) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_STATE;
    }
    if (!activation_directory_still_valid(transaction)) {
        return CBM_ACTIVATION_TRANSACTION_IO;
    }
    if (transaction->backup_contains_target) {
        activation_unlink_status_t removed = activation_unlink_expected(
            transaction, transaction->backup_path, transaction->backup_name,
            &transaction->backup_identity, true);
        if (removed == ACTIVATION_UNLINK_ERROR) {
            return CBM_ACTIVATION_TRANSACTION_IO;
        }
        if (removed == ACTIVATION_UNLINK_DEFERRED) {
            transaction->deferred_cleanup = true;
            transaction->state = ACTIVATION_FINALIZED_DEFERRED;
            return CBM_ACTIVATION_TRANSACTION_DEFERRED;
        }
        transaction->backup_exists = false;
        transaction->backup_contains_target = false;
    }
    if (!activation_sync_directory(transaction)) {
        return CBM_ACTIVATION_TRANSACTION_IO;
    }
    transaction->state = ACTIVATION_FINALIZED;
    return CBM_ACTIVATION_TRANSACTION_OK;
}

cbm_activation_transaction_status_t cbm_activation_transaction_close(
    cbm_activation_transaction_t **transaction_io) {
    if (!transaction_io || !*transaction_io) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    cbm_activation_transaction_t *transaction = *transaction_io;
    if (transaction->state == ACTIVATION_COMMITTED ||
        transaction->state == ACTIVATION_RECOVERY_NEEDED) {
        cbm_activation_transaction_status_t status = activation_rollback_internal(transaction);
        if (status != CBM_ACTIVATION_TRANSACTION_OK) {
            return status;
        }
    }
    if (transaction->state == ACTIVATION_STAGED || transaction->state == ACTIVATION_ROLLED_BACK) {
        cbm_activation_transaction_status_t status = activation_discard_staged_assets(transaction);
        if (status != CBM_ACTIVATION_TRANSACTION_OK) {
            return status;
        }
    }
    activation_transaction_destroy(transaction);
    *transaction_io = NULL;
    return CBM_ACTIVATION_TRANSACTION_OK;
}

const char *cbm_activation_transaction_target_path(
    const cbm_activation_transaction_t *transaction) {
    return transaction ? transaction->target_path : NULL;
}

const char *cbm_activation_transaction_staged_path(
    const cbm_activation_transaction_t *transaction) {
    return transaction ? transaction->staged_path : NULL;
}

const char *cbm_activation_transaction_backup_path(
    const cbm_activation_transaction_t *transaction) {
    return transaction ? transaction->backup_path : NULL;
}

const char *cbm_activation_transaction_deferred_path(
    const cbm_activation_transaction_t *transaction) {
    return transaction && transaction->deferred_cleanup ? transaction->backup_path : NULL;
}

const char *cbm_activation_transaction_status_message(cbm_activation_transaction_status_t status) {
    switch (status) {
    case CBM_ACTIVATION_TRANSACTION_OK:
        return "activation transaction completed";
    case CBM_ACTIVATION_TRANSACTION_DEFERRED:
        return "activation completed; old executable cleanup was deferred";
    case CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT:
        return "invalid activation transaction argument";
    case CBM_ACTIVATION_TRANSACTION_NO_MEMORY:
        return "activation transaction allocation failed";
    case CBM_ACTIVATION_TRANSACTION_IO:
        return "activation transaction I/O failed";
    case CBM_ACTIVATION_TRANSACTION_INVALID_STATE:
        return "activation transaction is in the wrong state";
    case CBM_ACTIVATION_TRANSACTION_VALIDATION_FAILED:
        return "activated candidate failed validation and was rolled back";
    case CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED:
        return "activation failed and the retained backup could not be restored";
    default:
        return "unknown activation transaction status";
    }
}
