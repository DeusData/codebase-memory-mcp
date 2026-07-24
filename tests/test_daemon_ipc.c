/*
 * test_daemon_ipc.c — RED guards for the local CBM daemon transport.
 *
 * These tests deliberately exercise the public transport boundary rather than
 * platform socket/pipe primitives.  The same API is expected to use Unix
 * domain sockets on POSIX and current-user named pipes on Windows.
 */
#include "test_framework.h"
#include "test_helpers.h"

#include "daemon/daemon.h"
#include "daemon/ipc.h"
#include "daemon/ipc_internal.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"
#include "foundation/private_file_lock_internal.h"
#include "foundation/subprocess.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include "foundation/win_utf8.h"
#include <aclapi.h>
#include <direct.h>
#include <windows.h>
#ifndef PIPE_REJECT_REMOTE_CLIENTS
#define PIPE_REJECT_REMOTE_CLIENTS 0x00000008
#endif
#define ipc_test_chdir _chdir
#define ipc_test_getcwd _getcwd
#else
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __APPLE__
#include <membership.h>
#include <sys/acl.h>
#endif
#define ipc_test_chdir chdir
#define ipc_test_getcwd getcwd
#endif

enum { TEST_PATH_CAP = 1024 };

#ifndef _WIN32
static bool ipc_test_write_byte(const char *path, unsigned char byte);
#endif

static bool ipc_test_parent_new(char out[TEST_PATH_CAP], const char *tag) {
    /* Endpoint parents must carry production-shaped ancestry: the runtime
     * ancestry validation (correctly) refuses temp roots whose ancestors
     * grant mutation rights to Authenticated Users, e.g. C:/msys64/tmp and
     * GitHub-runner work directories. th_secure_runtime_parent_new anchors
     * under LocalAppData on Windows, like the production default parent. */
    return th_secure_runtime_parent_new(out, TEST_PATH_CAP, tag);
}

static void ipc_test_copy_path(char out[TEST_PATH_CAP], const char *path) {
    if (!path) {
        out[0] = '\0';
        return;
    }
    (void)snprintf(out, TEST_PATH_CAP, "%s", path);
}

static bool ipc_test_full_path(char out[TEST_PATH_CAP], const char *path) {
#ifdef _WIN32
    return path && _fullpath(out, path, TEST_PATH_CAP) != NULL;
#else
    return path && realpath(path, out) != NULL;
#endif
}

static bool ipc_test_path_is_absolute(const char *path) {
    if (!path || path[0] == '\0') {
        return false;
    }
#ifdef _WIN32
    return (path[0] == '\\' && path[1] == '\\') ||
           (((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
            path[1] == ':' && (path[2] == '/' || path[2] == '\\'));
#else
    return path[0] == '/';
#endif
}

static bool ipc_test_path_has_parent(const char *path, const char *parent) {
    if (!path || !parent) {
        return false;
    }
    size_t parent_length = strlen(parent);
    if (parent_length == 0) {
        return false;
    }
    for (size_t i = 0; i < parent_length; i++) {
        char actual = path[i];
        char expected = parent[i];
#ifdef _WIN32
        if (actual == '/') {
            actual = '\\';
        }
        if (expected == '/') {
            expected = '\\';
        }
        if (actual >= 'A' && actual <= 'Z') {
            actual = (char)(actual - 'A' + 'a');
        }
        if (expected >= 'A' && expected <= 'Z') {
            expected = (char)(expected - 'A' + 'a');
        }
#endif
        if (actual == '\0' || actual != expected) {
            return false;
        }
    }
    if (parent[parent_length - 1] == '/' || parent[parent_length - 1] == '\\') {
        return true;
    }
    return path[parent_length] == '/' || path[parent_length] == '\\';
}

/* Endpoint state is intentionally persistent while clients come and go.  Tests
 * use isolated parents, so remove any lock/socket artifacts after all handles
 * have closed without following symlinks or spawning a cleanup shell. */
static void ipc_test_remove_flat_dir(const char *path) {
    if (!path || path[0] == '\0') {
        return;
    }
#ifdef _WIN32
    char pattern[TEST_PATH_CAP];
    char child[TEST_PATH_CAP];
    WIN32_FIND_DATAA entry;
    (void)snprintf(pattern, sizeof(pattern), "%s/*", path);
    HANDLE find = FindFirstFileA(pattern, &entry);
    if (find != INVALID_HANDLE_VALUE) {
        do {
            if (strcmp(entry.cFileName, ".") == 0 || strcmp(entry.cFileName, "..") == 0) {
                continue;
            }
            (void)snprintf(child, sizeof(child), "%s/%s", path, entry.cFileName);
            if ((entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0) {
                (void)RemoveDirectoryA(child);
            } else {
                (void)DeleteFileA(child);
            }
        } while (FindNextFileA(find, &entry));
        (void)FindClose(find);
    }
    (void)RemoveDirectoryA(path);
#else
    DIR *dir = opendir(path);
    if (dir) {
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }
            char child[TEST_PATH_CAP];
            struct stat st;
            (void)snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
            if (lstat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
                (void)rmdir(child);
            } else {
                (void)unlink(child);
            }
        }
        (void)closedir(dir);
    }
    (void)rmdir(path);
#endif
}

static void ipc_test_remove_tree(const char *runtime_dir, const char *parent) {
    if (runtime_dir && parent && strcmp(runtime_dir, parent) != 0) {
        ipc_test_remove_flat_dir(runtime_dir);
    }
    ipc_test_remove_flat_dir(parent);
}

#ifdef __APPLE__
/* Return 1 when installed, 0 only when the backing filesystem does not support
 * Darwin extended ACLs, and -1 for every other fixture failure. */
static int ipc_test_macos_set_directory_acl(const char *path, bool inheritable, acl_tag_t tag) {
    if (!path || path[0] == '\0') {
        errno = EINVAL;
        return -1;
    }

    acl_t acl = acl_init(1);
    acl_entry_t entry = NULL;
    acl_permset_t permissions = NULL;
    acl_flagset_t flags = NULL;
    bool built = acl && acl_create_entry(&acl, &entry) == 0 && entry &&
                 (tag == ACL_EXTENDED_ALLOW || tag == ACL_EXTENDED_DENY) &&
                 acl_set_tag_type(entry, tag) == 0;

    /* wheel (0) and daemon (1) are stable real macOS system groups.  Use the
     * one that is not the process's effective group so the fixture grants a
     * distinct principal directory-mutation rights. */
    gid_t foreign_group = getegid() == (gid_t)0 ? (gid_t)1 : (gid_t)0;
    uuid_t foreign_group_uuid = {0};
    int membership_result = built ? mbr_gid_to_uuid(foreign_group, foreign_group_uuid) : 0;
    if (built && membership_result != 0) {
        errno = membership_result;
        built = false;
    }
    built = built && acl_set_qualifier(entry, foreign_group_uuid) == 0 &&
            acl_get_permset(entry, &permissions) == 0 && permissions &&
            acl_clear_perms(permissions) == 0 && acl_add_perm(permissions, ACL_ADD_FILE) == 0 &&
            acl_add_perm(permissions, ACL_ADD_SUBDIRECTORY) == 0 &&
            acl_add_perm(permissions, ACL_DELETE_CHILD) == 0 &&
            acl_set_permset(entry, permissions) == 0;

    if (built && inheritable) {
        built = acl_get_flagset_np(entry, &flags) == 0 && flags && acl_clear_flags_np(flags) == 0 &&
                acl_add_flag_np(flags, ACL_ENTRY_FILE_INHERIT) == 0 &&
                acl_add_flag_np(flags, ACL_ENTRY_DIRECTORY_INHERIT) == 0 &&
                acl_set_flagset_np(entry, flags) == 0;
    }
    built = built && acl_valid(acl) == 0;

    errno = built ? 0 : errno;
    int result = built ? acl_set_file(path, ACL_TYPE_EXTENDED, acl) : -1;
    int saved_error = errno;
    if (acl && acl_free(acl) != 0 && result == 0) {
        result = -1;
        saved_error = errno;
    }
    errno = saved_error;
    if (result == 0) {
        return 1;
    }
    return saved_error == ENOTSUP || saved_error == EOPNOTSUPP ? 0 : -1;
}

static int ipc_test_macos_set_mutating_acl(const char *path, bool inheritable) {
    return ipc_test_macos_set_directory_acl(path, inheritable, ACL_EXTENDED_ALLOW);
}

static int ipc_test_macos_set_deny_acl(const char *path, bool inheritable) {
    return ipc_test_macos_set_directory_acl(path, inheritable, ACL_EXTENDED_DENY);
}

/* An ACL-less existing path is reported by Darwin either as an empty ACL or,
 * on some filesystems, ENOENT.  Validate the path first so ENOENT cannot hide
 * a missing runtime directory. */
static int ipc_test_macos_extended_acl_entry_count(const char *path) {
    struct stat status;
    if (!path || lstat(path, &status) != 0) {
        return -1;
    }

    errno = 0;
    acl_t acl = acl_get_file(path, ACL_TYPE_EXTENDED);
    if (!acl) {
        return errno == ENOENT ? 0 : -1;
    }

    int count = 0;
    acl_entry_t entry = NULL;
    int entry_result = acl_get_entry(acl, ACL_FIRST_ENTRY, &entry);
    /* Darwin's acl_get_entry returns 0 for each entry and -1/EINVAL after
     * ACL_NEXT_ENTRY advances beyond the final entry (unlike the 1/0 return
     * convention used by several other POSIX ACL implementations). */
    while (entry_result == 0) {
        count++;
        errno = 0;
        entry_result = acl_get_entry(acl, ACL_NEXT_ENTRY, &entry);
    }
    int entry_error = errno;
    int free_result = acl_free(acl);
    return entry_result == -1 && entry_error == EINVAL && free_result == 0 ? count : -1;
}
#endif

#ifdef _WIN32
typedef struct {
    wchar_t *value;
    bool present;
} ipc_test_win_env_t;

static bool ipc_test_win_grant_everyone_rights(const char *path, DWORD rights) {
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    BYTE world_buffer[SECURITY_MAX_SID_SIZE];
    DWORD world_size = sizeof(world_buffer);
    PACL existing = NULL;
    PACL replacement = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    bool ok = wide_path && CreateWellKnownSid(WinWorldSid, NULL, world_buffer, &world_size) != 0 &&
              GetNamedSecurityInfoW(wide_path, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, NULL,
                                    NULL, &existing, NULL, &descriptor) == ERROR_SUCCESS;
    EXPLICIT_ACCESSW access;
    memset(&access, 0, sizeof(access));
    access.grfAccessPermissions = rights;
    access.grfAccessMode = GRANT_ACCESS;
    access.grfInheritance = NO_INHERITANCE;
    access.Trustee.TrusteeForm = TRUSTEE_IS_SID;
    access.Trustee.TrusteeType = TRUSTEE_IS_WELL_KNOWN_GROUP;
    access.Trustee.ptstrName = (LPWSTR)world_buffer;
    if (ok) {
        ok = SetEntriesInAclW(1U, &access, existing, &replacement) == ERROR_SUCCESS &&
             replacement &&
             SetNamedSecurityInfoW(wide_path, SE_FILE_OBJECT,
                                   DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                                   NULL, NULL, replacement, NULL) == ERROR_SUCCESS;
    }
    if (replacement) {
        (void)LocalFree(replacement);
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    free(wide_path);
    return ok;
}

static bool ipc_test_win_grant_everyone_mutation(const char *path) {
    return ipc_test_win_grant_everyone_rights(path, FILE_ADD_FILE | FILE_ADD_SUBDIRECTORY |
                                                        FILE_DELETE_CHILD | DELETE | WRITE_DAC |
                                                        WRITE_OWNER);
}

static bool ipc_test_win_env_capture(const wchar_t *name, ipc_test_win_env_t *saved) {
    if (!name || !saved) {
        return false;
    }
    memset(saved, 0, sizeof(*saved));
    SetLastError(ERROR_SUCCESS);
    DWORD needed = GetEnvironmentVariableW(name, NULL, 0);
    if (needed == 0) {
        DWORD error = GetLastError();
        return error == ERROR_ENVVAR_NOT_FOUND || error == ERROR_SUCCESS;
    }
    saved->value = malloc((size_t)needed * sizeof(*saved->value));
    if (!saved->value || GetEnvironmentVariableW(name, saved->value, needed) + 1U != needed) {
        free(saved->value);
        saved->value = NULL;
        return false;
    }
    saved->present = true;
    return true;
}

static bool ipc_test_win_env_restore(const wchar_t *name, ipc_test_win_env_t *saved) {
    if (!name || !saved) {
        return false;
    }
    bool restored = SetEnvironmentVariableW(name, saved->present ? saved->value : NULL) != 0;
    free(saved->value);
    memset(saved, 0, sizeof(*saved));
    return restored;
}

static bool ipc_test_win_temp_set(const char *path) {
    wchar_t *wide = cbm_utf8_to_wide(path);
    bool changed = wide && SetEnvironmentVariableW(L"TMP", wide) != 0 &&
                   SetEnvironmentVariableW(L"TEMP", wide) != 0;
    free(wide);
    return changed;
}

static int ipc_test_win_lock_child(const char *kind, const char *key, const char *parent) {
    char self[MAX_PATH];
    DWORD self_length = GetModuleFileNameA(NULL, self, sizeof(self));
    if (!kind || !key || !parent || self_length == 0 || self_length >= sizeof(self)) {
        return -1;
    }
    const char *const argv[] = {
        self, "__cbm_daemon_ipc_lock_probe", kind, key, parent, NULL,
    };
    cbm_proc_opts_t options = {
        .bin = self,
        .argv = argv,
        .quiet_timeout_ms = 5000,
    };
    cbm_proc_result_t result;
    return cbm_subprocess_run(&options, &result) == 0 ? result.exit_code : -1;
}

typedef struct {
    const cbm_daemon_ipc_endpoint_t *endpoint;
    int result;
} ipc_test_win_startup_call_t;

static void *ipc_test_win_startup_call(void *opaque) {
    ipc_test_win_startup_call_t *call = opaque;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    call->result = cbm_daemon_ipc_startup_lock_try_acquire(call->endpoint, &startup);
    if (call->result == 1 && !cbm_daemon_ipc_startup_lock_prepare_handoff(startup)) {
        call->result = 0;
    }
    cbm_daemon_ipc_startup_lock_release(&startup);
    return NULL;
}

static void ipc_test_win_lock_release(cbm_private_file_lock_t **lock_io) {
    while (lock_io && *lock_io &&
           cbm_private_file_lock_release(lock_io) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
}

static bool ipc_test_win_lock_busy(cbm_private_lock_directory_t *directory, const char *base_name) {
    cbm_private_file_lock_t *probe = NULL;
    cbm_private_file_lock_status_t status =
        cbm_private_file_lock_try_acquire(directory, base_name, CBM_PRIVATE_FILE_LOCK_SH, &probe);
    ipc_test_win_lock_release(&probe);
    return status == CBM_PRIVATE_FILE_LOCK_BUSY;
}
#endif

typedef struct {
    cbm_ipc_pending_wait_status_t wait_status;
    cbm_ipc_pending_finish_status_t finish_status;
    uint32_t finish_bytes;
    char calls[8];
    size_t call_count;
    bool finish_was_blocking;
} ipc_pending_fake_t;

static void ipc_pending_fake_record(ipc_pending_fake_t *fake, char call) {
    if (fake->call_count + 1 < sizeof(fake->calls)) {
        fake->calls[fake->call_count++] = call;
        fake->calls[fake->call_count] = '\0';
    }
}

static cbm_ipc_pending_wait_status_t ipc_pending_fake_wait(void *opaque, uint32_t timeout_ms) {
    ipc_pending_fake_t *fake = (ipc_pending_fake_t *)opaque;
    (void)timeout_ms;
    ipc_pending_fake_record(fake, 'W');
    return fake->wait_status;
}

static void ipc_pending_fake_cancel(void *opaque) {
    ipc_pending_fake_t *fake = (ipc_pending_fake_t *)opaque;
    ipc_pending_fake_record(fake, 'C');
}

static cbm_ipc_pending_finish_status_t ipc_pending_fake_finish(void *opaque, bool blocking,
                                                               uint32_t *transferred_out) {
    ipc_pending_fake_t *fake = (ipc_pending_fake_t *)opaque;
    ipc_pending_fake_record(fake, 'D');
    fake->finish_was_blocking = blocking;
    if (fake->finish_status == CBM_IPC_PENDING_FINISH_COMPLETED) {
        *transferred_out = fake->finish_bytes;
    }
    return fake->finish_status;
}

static int ipc_pending_fake_run(ipc_pending_fake_t *fake, uint32_t *transferred_out) {
    cbm_ipc_pending_ops_t ops = {
        .context = fake,
        .wait = ipc_pending_fake_wait,
        .cancel = ipc_pending_fake_cancel,
        .finish = ipc_pending_fake_finish,
    };
    return cbm_daemon_ipc_wait_pending(&ops, 17, transferred_out);
}

TEST(daemon_ipc_pending_timeout_race_returns_completed_io) {
    ipc_pending_fake_t fake = {
        .wait_status = CBM_IPC_PENDING_WAIT_TIMEOUT,
        .finish_status = CBM_IPC_PENDING_FINISH_COMPLETED,
        .finish_bytes = 6,
    };
    uint32_t transferred = 0;
    int result = ipc_pending_fake_run(&fake, &transferred);

    ASSERT_EQ(result, 1);
    ASSERT_EQ(transferred, 6);
    ASSERT_STR_EQ(fake.calls, "WCD");
    ASSERT_TRUE(fake.finish_was_blocking);
    PASS();
}

TEST(daemon_ipc_pending_wait_failure_cancels_and_drains) {
    ipc_pending_fake_t fake = {
        .wait_status = CBM_IPC_PENDING_WAIT_FAILED,
        .finish_status = CBM_IPC_PENDING_FINISH_CANCELLED,
    };
    uint32_t transferred = 0;
    int result = ipc_pending_fake_run(&fake, &transferred);

    ASSERT_EQ(result, -1);
    ASSERT_STR_EQ(fake.calls, "WCD");
    ASSERT_TRUE(fake.finish_was_blocking);
    PASS();
}

TEST(daemon_ipc_pending_timeout_cancelled_returns_timeout) {
    ipc_pending_fake_t fake = {
        .wait_status = CBM_IPC_PENDING_WAIT_TIMEOUT,
        .finish_status = CBM_IPC_PENDING_FINISH_CANCELLED,
    };
    uint32_t transferred = 0;
    int result = ipc_pending_fake_run(&fake, &transferred);

    ASSERT_EQ(result, 0);
    ASSERT_STR_EQ(fake.calls, "WCD");
    ASSERT_TRUE(fake.finish_was_blocking);
    PASS();
}

TEST(daemon_ipc_windows_generation_address_binds_account_key_and_nonce) {
    /* S-1-5-21-305419896-2596069104-267242409 */
    static const uint8_t sid_a[] = {
        1,    4,    0,    0,    0,    0,    0,    5,    21,   0,    0,    0,
        0x78, 0x56, 0x34, 0x12, 0xf0, 0xde, 0xbc, 0x9a, 0xa9, 0xcb, 0xed, 0x0f,
    };
    /* Same account shape, different final sub-authority. */
    static const uint8_t sid_b[] = {
        1,    4,    0,    0,    0,    0,    0,    5,    21,   0,    0,    0,
        0x78, 0x56, 0x34, 0x12, 0xf0, 0xde, 0xbc, 0x9a, 0xaa, 0xcb, 0xed, 0x0f,
    };
    static const char key[] = "0123456789abcdef";
    static const char other_key[] = "fedcba9876543210";
    static const uint8_t nonce_a[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE] = {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
        0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
        0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    };
    static const uint8_t nonce_b[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE] = {
        0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a,
        0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35,
        0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f,
    };
    char address_a[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char address_same[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char address_b[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char address_other_key[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char address_other_nonce[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};

    bool first =
        cbm_daemon_ipc_windows_generation_address(sid_a, sizeof(sid_a), key, nonce_a, address_a);
    bool repeated =
        cbm_daemon_ipc_windows_generation_address(sid_a, sizeof(sid_a), key, nonce_a, address_same);
    bool other_user =
        cbm_daemon_ipc_windows_generation_address(sid_b, sizeof(sid_b), key, nonce_a, address_b);
    bool changed_key = cbm_daemon_ipc_windows_generation_address(sid_a, sizeof(sid_a), other_key,
                                                                 nonce_a, address_other_key);
    bool changed_nonce = cbm_daemon_ipc_windows_generation_address(sid_a, sizeof(sid_a), key,
                                                                   nonce_b, address_other_nonce);

    ASSERT_TRUE(first);
    ASSERT_TRUE(repeated);
    ASSERT_TRUE(other_user);
    ASSERT_TRUE(changed_key);
    ASSERT_TRUE(changed_nonce);
    ASSERT_STR_EQ(address_a, address_same);
    ASSERT_TRUE(strcmp(address_a, address_b) != 0);
    ASSERT_TRUE(strcmp(address_a, address_other_key) != 0);
    ASSERT_TRUE(strcmp(address_a, address_other_nonce) != 0);
    ASSERT_STR_EQ(address_a, "\\\\.\\pipe\\cbm-daemon-"
                             "e861648d9f8bc786dce31bbb16eda2ab"
                             "ffa330a770752832ab5f2e4feaa506f1");
    ASSERT_TRUE(strstr(address_a, "S-1-") == NULL);
    ASSERT_TRUE(strstr(address_a, key) == NULL);
    PASS();
}

TEST(daemon_ipc_windows_legacy_names_are_frozen_for_migration) {
    char pipe_name[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char startup_name[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    bool derived = cbm_daemon_ipc_windows_legacy_names("C:\\Users\\Alice\\AppData\\Local",
                                                       "0123456789abcdef", pipe_name, startup_name);

    ASSERT_TRUE(derived);
    ASSERT_STR_EQ(pipe_name, "\\\\.\\pipe\\cbm-daemon-72380d6ef7f19c0c-"
                             "0123456789abcdef");
    ASSERT_STR_EQ(startup_name, "Local\\cbm-daemon-72380d6ef7f19c0c-"
                                "0123456789abcdef-startup");
    PASS();
}

TEST(daemon_ipc_windows_rendezvous_record_is_exact_and_canonical) {
    static const uint8_t nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE] = {
        0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a,
        0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95,
        0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
    };
    static const char address[] = "\\\\.\\pipe\\cbm-daemon-"
                                  "0123456789abcdef0123456789abcdef"
                                  "0123456789abcdef0123456789abcdef";
    uint8_t record[CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE];
    uint8_t decoded_nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE] = {0};
    char decoded_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};

    bool encoded = cbm_daemon_ipc_windows_rendezvous_record_encode(nonce, address, record);
    bool decoded = encoded && cbm_daemon_ipc_windows_rendezvous_record_decode(
                                  record, sizeof(record), decoded_nonce, decoded_address);
    ASSERT_TRUE(encoded);
    ASSERT_TRUE(decoded);
    ASSERT_TRUE(memcmp(decoded_nonce, nonce, sizeof(nonce)) == 0);
    ASSERT_STR_EQ(decoded_address, address);

    uint8_t corrupt[sizeof(record)];
    memcpy(corrupt, record, sizeof(corrupt));
    corrupt[0] ^= 0xffU;
    ASSERT_FALSE(cbm_daemon_ipc_windows_rendezvous_record_decode(corrupt, sizeof(corrupt),
                                                                 decoded_nonce, decoded_address));
    ASSERT_FALSE(cbm_daemon_ipc_windows_rendezvous_record_decode(record, sizeof(record) - 1U,
                                                                 decoded_nonce, decoded_address));

    memcpy(corrupt, record, sizeof(corrupt));
    corrupt[sizeof(corrupt) - 1U] = 1U;
    ASSERT_FALSE(cbm_daemon_ipc_windows_rendezvous_record_decode(corrupt, sizeof(corrupt),
                                                                 decoded_nonce, decoded_address));

    memcpy(corrupt, record, sizeof(corrupt));
    const size_t address_offset = 8U + CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE;
    corrupt[address_offset + strlen("\\\\.\\pipe\\cbm-daemon-")] = 'A';
    ASSERT_FALSE(cbm_daemon_ipc_windows_rendezvous_record_decode(corrupt, sizeof(corrupt),
                                                                 decoded_nonce, decoded_address));
    PASS();
}

#ifdef _WIN32
TEST(daemon_ipc_windows_default_endpoint_ignores_temp_environment) {
    char key[17];
    char parent_a[TEST_PATH_CAP] = {0};
    char parent_b[TEST_PATH_CAP] = {0};
    char runtime_a[TEST_PATH_CAP] = {0};
    char runtime_b[TEST_PATH_CAP] = {0};
    char address_a[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char address_b[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    ipc_test_win_env_t saved_tmp = {0};
    ipc_test_win_env_t saved_temp = {0};
    cbm_daemon_ipc_endpoint_t *endpoint_a = NULL;
    cbm_daemon_ipc_endpoint_t *endpoint_b = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    unsigned long long unique =
        ((unsigned long long)GetCurrentProcessId() << 32U) ^ GetTickCount64();
    bool key_ok = snprintf(key, sizeof(key), "%016llx", unique) == 16;
    bool parents_ok =
        ipc_test_parent_new(parent_a, "win-temp-a") && ipc_test_parent_new(parent_b, "win-temp-b");
    bool tmp_saved = ipc_test_win_env_capture(L"TMP", &saved_tmp);
    bool temp_saved = ipc_test_win_env_capture(L"TEMP", &saved_temp);
    bool saved = tmp_saved && temp_saved;
    bool first_temp = parents_ok && saved && ipc_test_win_temp_set(parent_a);
    if (key_ok && first_temp) {
        endpoint_a = cbm_daemon_ipc_endpoint_new(key, NULL);
    }
    if (endpoint_a) {
        int startup_status = cbm_daemon_ipc_startup_lock_try_acquire(endpoint_a, &startup);
        /* Publish the generation record: the address under test exists only
         * after a startup owner prepares the handoff. */
        if (startup_status == 1 && !cbm_daemon_ipc_startup_lock_prepare_handoff(startup)) {
            startup_status = -1;
        }
        cbm_daemon_ipc_startup_lock_release(&startup);
        startup = NULL;
        if (startup_status != 1) {
            cbm_daemon_ipc_endpoint_free(endpoint_a);
            endpoint_a = NULL;
        }
    }
    if (endpoint_a) {
        ipc_test_copy_path(runtime_a, cbm_daemon_ipc_endpoint_runtime_dir(endpoint_a));
        ipc_test_copy_path(address_a, cbm_daemon_ipc_endpoint_address(endpoint_a));
    }
    bool second_temp = endpoint_a && ipc_test_win_temp_set(parent_b);
    if (second_temp) {
        endpoint_b = cbm_daemon_ipc_endpoint_new(key, NULL);
    }
    if (endpoint_b) {
        ipc_test_copy_path(runtime_b, cbm_daemon_ipc_endpoint_runtime_dir(endpoint_b));
        ipc_test_copy_path(address_b, cbm_daemon_ipc_endpoint_address(endpoint_b));
    }
    bool stable = endpoint_a && endpoint_b && strcmp(address_a, address_b) == 0 &&
                  address_a[0] != '\0' && strcmp(runtime_a, runtime_b) == 0 &&
                  !ipc_test_path_has_parent(runtime_a, parent_a) &&
                  !ipc_test_path_has_parent(runtime_a, parent_b);

    bool restored_tmp = tmp_saved && ipc_test_win_env_restore(L"TMP", &saved_tmp);
    bool restored_temp = temp_saved && ipc_test_win_env_restore(L"TEMP", &saved_temp);
    cbm_daemon_ipc_endpoint_free(endpoint_b);
    cbm_daemon_ipc_endpoint_free(endpoint_a);
    if (runtime_b[0] != '\0' && strcmp(runtime_b, runtime_a) != 0) {
        ipc_test_remove_flat_dir(runtime_b);
    }
    ipc_test_remove_flat_dir(runtime_a);
    ipc_test_remove_flat_dir(parent_b);
    ipc_test_remove_flat_dir(parent_a);

    ASSERT_TRUE(key_ok);
    ASSERT_TRUE(parents_ok);
    ASSERT_TRUE(saved);
    ASSERT_TRUE(first_temp);
    ASSERT_TRUE(second_temp);
    ASSERT_TRUE(restored_tmp);
    ASSERT_TRUE(restored_temp);
    ASSERT_TRUE(stable);
    PASS();
}

TEST(daemon_ipc_windows_private_directory_rejects_untrusted_ancestor_acl) {
    char parent[TEST_PATH_CAP] = {0};
    char unsafe[TEST_PATH_CAP] = {0};
    char cache[TEST_PATH_CAP] = {0};
    bool paths_ok = false;
    bool unsafe_created = false;
    bool acl_injected = false;
    bool rejected = false;
    bool cache_absent = false;

    if (ipc_test_parent_new(parent, "win-unsafe-ancestor")) {
        int unsafe_written = snprintf(unsafe, sizeof(unsafe), "%s/unsafe", parent);
        int cache_written = snprintf(cache, sizeof(cache), "%s/cache", unsafe);
        paths_ok = unsafe_written > 0 && unsafe_written < (int)sizeof(unsafe) &&
                   cache_written > 0 && cache_written < (int)sizeof(cache);
    }
    if (paths_ok) {
        unsafe_created = CreateDirectoryA(unsafe, NULL) != 0;
    }
    if (unsafe_created) {
        acl_injected = ipc_test_win_grant_everyone_mutation(unsafe);
    }
    if (acl_injected) {
        rejected = !cbm_daemon_ipc_private_directory_secure(cache);
        SetLastError(ERROR_SUCCESS);
        DWORD attributes = GetFileAttributesA(cache);
        DWORD error = GetLastError();
        cache_absent = attributes == INVALID_FILE_ATTRIBUTES &&
                       (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND);
    }

    (void)RemoveDirectoryA(cache);
    (void)RemoveDirectoryA(unsafe);
    ipc_test_remove_flat_dir(parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(unsafe_created);
    ASSERT_TRUE(acl_injected);
    ASSERT_TRUE(rejected);
    ASSERT_TRUE(cache_absent);
    PASS();
}

TEST(daemon_ipc_windows_private_directory_allows_add_subdirectory_only_ancestor) {
    char parent[TEST_PATH_CAP] = {0};
    char add_only[TEST_PATH_CAP] = {0};
    char cache[TEST_PATH_CAP] = {0};
    bool paths_ok = false;
    bool ancestor_created = false;
    bool acl_injected = false;
    bool secured = false;
    bool cache_created = false;

    if (ipc_test_parent_new(parent, "win-add-only-ancestor")) {
        int ancestor_written = snprintf(add_only, sizeof(add_only), "%s/add-only", parent);
        int cache_written = snprintf(cache, sizeof(cache), "%s/cache", add_only);
        paths_ok = ancestor_written > 0 && ancestor_written < (int)sizeof(add_only) &&
                   cache_written > 0 && cache_written < (int)sizeof(cache);
    }
    if (paths_ok) {
        ancestor_created = CreateDirectoryA(add_only, NULL) != 0;
    }
    if (ancestor_created) {
        acl_injected = ipc_test_win_grant_everyone_rights(add_only, FILE_ADD_SUBDIRECTORY);
    }
    if (acl_injected) {
        secured = cbm_daemon_ipc_private_directory_secure(cache);
        DWORD attributes = GetFileAttributesA(cache);
        cache_created = attributes != INVALID_FILE_ATTRIBUTES &&
                        (attributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                        (attributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
    }

    (void)RemoveDirectoryA(cache);
    (void)RemoveDirectoryA(add_only);
    ipc_test_remove_flat_dir(parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(ancestor_created);
    ASSERT_TRUE(acl_injected);
    ASSERT_TRUE(secured);
    ASSERT_TRUE(cache_created);
    PASS();
}

TEST(daemon_ipc_windows_legacy_bridge_covers_handoff_and_lifetime) {
    static const char key[] = "91a2b3c4d5e6f708";
    char parent[TEST_PATH_CAP] = {0};
    char canonical_parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char legacy_pipe[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char legacy_startup[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_participant_guard_t *participant = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *lifetime = NULL;
    HANDLE old_pipe = INVALID_HANDLE_VALUE;

    bool parent_ok = ipc_test_parent_new(parent, "win-legacy-bridge") &&
                     ipc_test_full_path(canonical_parent, parent);
    bool names_ok = parent_ok && cbm_daemon_ipc_windows_legacy_names(canonical_parent, key,
                                                                     legacy_pipe, legacy_startup);
    if (names_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int absent_before = endpoint ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    int startup_status =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int visible_during_startup = startup ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    bool handoff = startup && cbm_daemon_ipc_startup_lock_prepare_handoff(startup);
    int visible_during_handoff = handoff ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    int participant_status =
        handoff ? cbm_daemon_ipc_participant_guard_try_join(endpoint, &participant) : -1;
    int lifetime_status = participant_status == 1
                              ? cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &lifetime)
                              : -1;
    int visible_during_lifetime = lifetime ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    lifetime = NULL;
    /* Native Windows teardown is deliberately ordered through startup-v2.
     * A participant must remain retryable while a bootstrap handoff owner
     * retains that transition; releasing the bootstrap then unblocks it. */
    bool participant_blocked_by_startup =
        participant && !cbm_daemon_ipc_participant_guard_release(&participant);
    bool participant_retained = participant != NULL;
    bool startup_released = cbm_daemon_ipc_startup_lock_release(&startup);
    bool participant_released = cbm_daemon_ipc_participant_guard_release(&participant);
    int absent_after_lifetime = endpoint ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;

    typedef BOOL(WINAPI * ipc_test_initialize_sd_fn)(PSECURITY_DESCRIPTOR, DWORD);
    typedef BOOL(WINAPI * ipc_test_set_dacl_fn)(PSECURITY_DESCRIPTOR, BOOL, PACL, BOOL);
    HMODULE advapi = LoadLibraryW(L"advapi32.dll");
    ipc_test_initialize_sd_fn initialize_sd =
        advapi ? (ipc_test_initialize_sd_fn)GetProcAddress(advapi, "InitializeSecurityDescriptor")
               : NULL;
    ipc_test_set_dacl_fn set_dacl =
        advapi ? (ipc_test_set_dacl_fn)GetProcAddress(advapi, "SetSecurityDescriptorDacl") : NULL;
    SECURITY_DESCRIPTOR unsafe_descriptor;
    bool unsafe_descriptor_ok = initialize_sd && set_dacl &&
                                initialize_sd(&unsafe_descriptor, SECURITY_DESCRIPTOR_REVISION) &&
                                set_dacl(&unsafe_descriptor, TRUE, NULL, FALSE);
    SECURITY_ATTRIBUTES unsafe_attributes = {
        .nLength = sizeof(unsafe_attributes),
        .lpSecurityDescriptor = unsafe_descriptor_ok ? &unsafe_descriptor : NULL,
        .bInheritHandle = FALSE,
    };
    wchar_t *legacy_startup_wide = names_ok ? cbm_utf8_to_wide(legacy_startup) : NULL;
    HANDLE unsafe_mutex = unsafe_descriptor_ok && legacy_startup_wide
                              ? CreateMutexW(&unsafe_attributes, FALSE, legacy_startup_wide)
                              : NULL;
    free(legacy_startup_wide);
    int unsafe_probe = unsafe_mutex ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : 0;
    cbm_daemon_ipc_startup_lock_t *unsafe_startup = NULL;
    int unsafe_startup_status =
        unsafe_mutex ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &unsafe_startup) : 0;
    cbm_daemon_ipc_startup_lock_release(&unsafe_startup);
    if (unsafe_mutex) {
        (void)CloseHandle(unsafe_mutex);
    }
    if (advapi) {
        (void)FreeLibrary(advapi);
    }
    int absent_after_unsafe = endpoint ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;

    wchar_t *old_name = names_ok ? cbm_utf8_to_wide(legacy_pipe) : NULL;
    if (old_name) {
        old_pipe = CreateNamedPipeW(old_name, PIPE_ACCESS_DUPLEX | FILE_FLAG_FIRST_PIPE_INSTANCE,
                                    PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT |
                                        PIPE_REJECT_REMOTE_CLIENTS,
                                    1, 4096, 4096, 0, NULL);
    }
    free(old_name);
    int visible_legacy_pipe =
        old_pipe != INVALID_HANDLE_VALUE ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    if (old_pipe != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(old_pipe);
    }
    int absent_after_pipe = endpoint ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;

    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    (void)cbm_daemon_ipc_participant_guard_release(&participant);
    cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(names_ok);
    ASSERT_EQ(absent_before, 0);
    ASSERT_EQ(startup_status, 1);
    ASSERT_EQ(visible_during_startup, 1);
    ASSERT_TRUE(handoff);
    ASSERT_EQ(visible_during_handoff, 1);
    ASSERT_EQ(participant_status, 1);
    ASSERT_EQ(lifetime_status, 1);
    ASSERT_EQ(visible_during_lifetime, 1);
    ASSERT_TRUE(participant_blocked_by_startup);
    ASSERT_TRUE(participant_retained);
    ASSERT_TRUE(startup_released);
    ASSERT_NULL(startup);
    ASSERT_EQ(absent_after_lifetime, 0);
    ASSERT_TRUE(participant_released);
    ASSERT_NULL(participant);
    ASSERT_TRUE(unsafe_descriptor_ok);
    ASSERT_TRUE(unsafe_mutex != NULL);
    ASSERT_EQ(unsafe_probe, -1);
    ASSERT_EQ(unsafe_startup_status, -1);
    ASSERT_EQ(absent_after_unsafe, 0);
    ASSERT_TRUE(old_pipe != INVALID_HANDLE_VALUE);
    ASSERT_EQ(visible_legacy_pipe, 1);
    ASSERT_EQ(absent_after_pipe, 0);
    PASS();
}

TEST(daemon_ipc_windows_local_transition_atomically_reserves_legacy_pipe) {
    static const char key[] = "71a2b3c4d5e6f809";
    char parent[TEST_PATH_CAP] = {0};
    char canonical_parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char legacy_pipe[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char legacy_startup[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_local_transition_t *transition_wins = NULL;
    cbm_daemon_ipc_local_transition_t *legacy_wins = NULL;
    HANDLE old_after_sentinel = INVALID_HANDLE_VALUE;
    HANDLE old_first = INVALID_HANDLE_VALUE;

    bool parent_ok = ipc_test_parent_new(parent, "win-local-transition") &&
                     ipc_test_full_path(canonical_parent, parent);
    bool names_ok = parent_ok && cbm_daemon_ipc_windows_legacy_names(canonical_parent, key,
                                                                     legacy_pipe, legacy_startup);
    if (names_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int transition_wins_result =
        endpoint ? cbm_daemon_ipc_local_transition_try_acquire(endpoint, &transition_wins) : -1;
    int sentinel_result = transition_wins_result == 1
                              ? cbm_daemon_ipc_local_transition_seal_legacy(transition_wins)
                              : -1;
    int sentinel_visible =
        sentinel_result == 1 ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    wchar_t *legacy_name = names_ok ? cbm_utf8_to_wide(legacy_pipe) : NULL;
    if (legacy_name && sentinel_result == 1) {
        old_after_sentinel = CreateNamedPipeW(
            legacy_name, PIPE_ACCESS_DUPLEX | FILE_FLAG_FIRST_PIPE_INSTANCE,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS, 1, 4096,
            4096, 0, NULL);
    }
    bool work_begun =
        sentinel_result == 1 && cbm_daemon_ipc_local_transition_begin_work(transition_wins);
    bool transition_wins_released = cbm_daemon_ipc_local_transition_release(&transition_wins);
    int absent_after_sentinel = endpoint ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;

    if (legacy_name) {
        old_first = CreateNamedPipeW(
            legacy_name, PIPE_ACCESS_DUPLEX | FILE_FLAG_FIRST_PIPE_INSTANCE,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS, 1, 4096,
            4096, 0, NULL);
    }
    int legacy_wins_result =
        old_first != INVALID_HANDLE_VALUE
            ? cbm_daemon_ipc_local_transition_try_acquire(endpoint, &legacy_wins)
            : -1;
    int rejected_after_legacy_win =
        legacy_wins_result == 1 ? cbm_daemon_ipc_local_transition_seal_legacy(legacy_wins) : -1;
    bool legacy_wins_released = cbm_daemon_ipc_local_transition_release(&legacy_wins);
    if (old_after_sentinel != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(old_after_sentinel);
    }
    if (old_first != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(old_first);
    }
    free(legacy_name);
    int absent_after_old = endpoint ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;

    (void)cbm_daemon_ipc_local_transition_release(&legacy_wins);
    (void)cbm_daemon_ipc_local_transition_release(&transition_wins);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(names_ok);
    ASSERT_EQ(transition_wins_result, 1);
    ASSERT_EQ(sentinel_result, 1);
    ASSERT_EQ(sentinel_visible, 1);
    ASSERT_TRUE(old_after_sentinel == INVALID_HANDLE_VALUE);
    ASSERT_TRUE(work_begun);
    ASSERT_TRUE(transition_wins_released);
    ASSERT_NULL(transition_wins);
    ASSERT_EQ(absent_after_sentinel, 0);
    ASSERT_TRUE(old_first != INVALID_HANDLE_VALUE);
    ASSERT_EQ(legacy_wins_result, 1);
    ASSERT_EQ(rejected_after_legacy_win, 0);
    ASSERT_TRUE(legacy_wins_released);
    ASSERT_NULL(legacy_wins);
    ASSERT_EQ(absent_after_old, 0);
    PASS();
}

TEST(daemon_ipc_windows_startup_retries_transient_rendezvous_reader) {
    static const char key[] = "81b2c3d4e5f60719";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_private_lock_directory_t *directory = NULL;
    cbm_private_file_lock_t *record_reader = NULL;
    cbm_thread_t thread;
    bool thread_started = false;
    bool startup_observed = false;
    int join_status = -1;
    ipc_test_win_startup_call_t call = {.result = -1};

    bool parent_ok = ipc_test_parent_new(parent, "win-record-reader");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        call.endpoint = endpoint;
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    cbm_private_file_lock_status_t directory_status =
        endpoint ? cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t record_status =
        directory_status == CBM_PRIVATE_FILE_LOCK_OK
            ? cbm_private_file_lock_try_acquire(directory, CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_FILE,
                                                CBM_PRIVATE_FILE_LOCK_SH, &record_reader)
            : CBM_PRIVATE_FILE_LOCK_IO;
    if (record_status == CBM_PRIVATE_FILE_LOCK_OK) {
        thread_started = cbm_thread_create(&thread, 0, ipc_test_win_startup_call, &call) == 0;
    }
    for (size_t attempt = 0; thread_started && attempt < 200U; attempt++) {
        if (ipc_test_win_lock_busy(directory, "cbm-startup-v2.lock")) {
            startup_observed = true;
            break;
        }
        cbm_usleep(1000);
    }
    ipc_test_win_lock_release(&record_reader);
    if (thread_started) {
        join_status = cbm_thread_join(&thread);
    }
    if (endpoint) {
        ipc_test_copy_path(address, cbm_daemon_ipc_endpoint_address(endpoint));
    }

    ipc_test_win_lock_release(&record_reader);
    cbm_private_lock_directory_close(directory);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(directory_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(record_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(thread_started);
    ASSERT_TRUE(startup_observed);
    ASSERT_EQ(join_status, 0);
    ASSERT_EQ(call.result, 1);
    ASSERT_TRUE(address[0] != '\0');
    PASS();
}

TEST(daemon_ipc_windows_rendezvous_bridges_concurrent_lifetime_owner) {
    static const char key[] = "71b2c3d4e5f60729";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char before[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char after[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *initial_startup = NULL;
    cbm_private_lock_directory_t *directory = NULL;
    cbm_private_file_lock_t *record_reader = NULL;
    cbm_private_file_lock_t *lifetime_owner = NULL;
    cbm_thread_t thread;
    bool thread_started = false;
    bool startup_observed = false;
    int join_status = -1;
    ipc_test_win_startup_call_t call = {.result = -1};

    bool parent_ok = ipc_test_parent_new(parent, "win-lifetime-bridge");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        call.endpoint = endpoint;
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int initial_status =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &initial_startup) : -1;
    if (initial_status == 1 && !cbm_daemon_ipc_startup_lock_prepare_handoff(initial_startup)) {
        initial_status = -1;
    }
    if (initial_status == 1) {
        ipc_test_copy_path(before, cbm_daemon_ipc_endpoint_address(endpoint));
    }
    cbm_daemon_ipc_startup_lock_release(&initial_startup);
    initial_startup = NULL;

    cbm_private_file_lock_status_t directory_status =
        endpoint ? cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t record_status =
        directory_status == CBM_PRIVATE_FILE_LOCK_OK
            ? cbm_private_file_lock_try_acquire(directory, CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_FILE,
                                                CBM_PRIVATE_FILE_LOCK_SH, &record_reader)
            : CBM_PRIVATE_FILE_LOCK_IO;
    if (record_status == CBM_PRIVATE_FILE_LOCK_OK) {
        thread_started = cbm_thread_create(&thread, 0, ipc_test_win_startup_call, &call) == 0;
    }
    for (size_t attempt = 0; thread_started && attempt < 200U; attempt++) {
        if (ipc_test_win_lock_busy(directory, "cbm-startup-v2.lock")) {
            startup_observed = true;
            break;
        }
        cbm_usleep(1000);
    }
    cbm_private_file_lock_status_t lifetime_status =
        startup_observed
            ? cbm_private_file_lock_try_acquire(directory, "cbm-lifetime.lock",
                                                CBM_PRIVATE_FILE_LOCK_EX, &lifetime_owner)
            : CBM_PRIVATE_FILE_LOCK_IO;
    ipc_test_win_lock_release(&record_reader);
    if (thread_started) {
        join_status = cbm_thread_join(&thread);
    }
    if (endpoint) {
        ipc_test_copy_path(after, cbm_daemon_ipc_endpoint_address(endpoint));
    }
    ipc_test_win_lock_release(&lifetime_owner);

    ipc_test_win_lock_release(&record_reader);
    ipc_test_win_lock_release(&lifetime_owner);
    cbm_private_lock_directory_close(directory);
    cbm_daemon_ipc_startup_lock_release(&initial_startup);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(initial_status, 1);
    ASSERT_TRUE(before[0] != '\0');
    ASSERT_EQ(directory_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(record_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(thread_started);
    ASSERT_TRUE(startup_observed);
    ASSERT_EQ(lifetime_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(join_status, 0);
    ASSERT_EQ(call.result, 0);
    ASSERT_STR_EQ(after, before);
    PASS();
}

TEST(daemon_ipc_windows_generation_rotates_and_escapes_occupied_old_pipe) {
    static const char key[] = "a1b2c3d4e5f60718";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char first_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char first_peer_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char second_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char second_peer_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_endpoint_t *peer = NULL;
    cbm_daemon_ipc_startup_lock_t *first_startup = NULL;
    cbm_daemon_ipc_startup_lock_t *second_startup = NULL;
    cbm_daemon_ipc_participant_guard_t *participant = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *lifetime = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    HANDLE old_pipe = INVALID_HANDLE_VALUE;

    bool parent_ok = ipc_test_parent_new(parent, "win-generation");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
        peer = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int first_status =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &first_startup) : -1;
    if (first_status == 1 && !cbm_daemon_ipc_startup_lock_prepare_handoff(first_startup)) {
        first_status = -1;
    }
    if (first_status == 1) {
        ipc_test_copy_path(first_address, cbm_daemon_ipc_endpoint_address(endpoint));
        ipc_test_copy_path(first_peer_address, cbm_daemon_ipc_endpoint_address(peer));
    }
    cbm_daemon_ipc_startup_lock_release(&first_startup);
    first_startup = NULL;

    wchar_t *old_name = first_address[0] ? cbm_utf8_to_wide(first_address) : NULL;
    if (old_name) {
        old_pipe = CreateNamedPipeW(old_name, PIPE_ACCESS_DUPLEX | FILE_FLAG_FIRST_PIPE_INSTANCE,
                                    PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT |
                                        PIPE_REJECT_REMOTE_CLIENTS,
                                    1, 4096, 4096, 0, NULL);
    }
    free(old_name);
    int second_status =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &second_startup) : -1;
    bool handoff_prepared =
        second_status == 1 && cbm_daemon_ipc_startup_lock_prepare_handoff(second_startup);
    int participant_status =
        handoff_prepared ? cbm_daemon_ipc_participant_guard_try_join(endpoint, &participant) : -1;
    int lifetime_status = participant_status == 1
                              ? cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &lifetime)
                              : -1;
    if (handoff_prepared) {
        ipc_test_copy_path(second_address, cbm_daemon_ipc_endpoint_address(endpoint));
        ipc_test_copy_path(second_peer_address, cbm_daemon_ipc_endpoint_address(peer));
        listener =
            lifetime_status == 1 ? cbm_daemon_ipc_listen_reserved(endpoint, &lifetime) : NULL;
    }
    cbm_daemon_ipc_startup_lock_release(&second_startup);
    second_startup = NULL;

    bool first_shared = first_address[0] && strcmp(first_address, first_peer_address) == 0;
    bool rotated = second_address[0] && strcmp(first_address, second_address) != 0;
    bool second_shared = strcmp(second_address, second_peer_address) == 0;
    bool old_occupied = old_pipe != INVALID_HANDLE_VALUE;
    bool new_listened = listener != NULL;

    cbm_daemon_ipc_listener_close(listener);
    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    bool participant_released = cbm_daemon_ipc_participant_guard_release(&participant);
    if (old_pipe != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(old_pipe);
    }
    cbm_daemon_ipc_startup_lock_release(&second_startup);
    cbm_daemon_ipc_startup_lock_release(&first_startup);
    cbm_daemon_ipc_endpoint_free(peer);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(first_status, 1);
    ASSERT_TRUE(first_shared);
    ASSERT_TRUE(old_occupied);
    ASSERT_EQ(second_status, 1);
    ASSERT_TRUE(handoff_prepared);
    ASSERT_EQ(participant_status, 1);
    ASSERT_EQ(lifetime_status, 1);
    ASSERT_TRUE(rotated);
    ASSERT_TRUE(second_shared);
    ASSERT_TRUE(new_listened);
    ASSERT_TRUE(participant_released);
    PASS();
}

TEST(daemon_ipc_windows_corrupt_rendezvous_fails_closed_until_startup_repairs) {
    static const char key[] = "b1c2d3e4f5061728";
    static const uint8_t partial[] = {'C', 'B', 'M', 'R', 'D', 'V', '1'};
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char original_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char repaired_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    char rebound_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_endpoint_t *reader = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_private_lock_directory_t *directory = NULL;
    cbm_private_file_lock_t *record_lock = NULL;
    cbm_private_file_lock_status_t directory_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t record_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t write_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t read_status = CBM_PRIVATE_FILE_LOCK_IO;
    uint8_t readback[CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE] = {0};
    size_t readback_length = 0;

    bool parent_ok = ipc_test_parent_new(parent, "win-corrupt-record");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int initial_status =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    if (initial_status == 1 && !cbm_daemon_ipc_startup_lock_prepare_handoff(startup)) {
        initial_status = -1;
    }
    if (initial_status == 1) {
        ipc_test_copy_path(original_address, cbm_daemon_ipc_endpoint_address(endpoint));
    }
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;

    if (endpoint) {
        directory_status = cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory);
    }
    if (directory_status == CBM_PRIVATE_FILE_LOCK_OK) {
        record_status =
            cbm_private_file_lock_try_acquire(directory, CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_FILE,
                                              CBM_PRIVATE_FILE_LOCK_EX, &record_lock);
    }
    if (record_status == CBM_PRIVATE_FILE_LOCK_OK) {
        write_status = cbm_private_file_lock_payload_write(record_lock, partial, sizeof(partial));
    }
    if (record_lock) {
        (void)cbm_private_file_lock_release(&record_lock);
    }
    cbm_private_lock_directory_close(directory);
    directory = NULL;

    if (write_status == CBM_PRIVATE_FILE_LOCK_OK) {
        reader = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    cbm_daemon_ipc_connection_t *corrupt_connection =
        reader ? cbm_daemon_ipc_connect(reader, 1) : NULL;
    bool failed_closed =
        reader && cbm_daemon_ipc_endpoint_address(reader) == NULL && corrupt_connection == NULL;
    cbm_daemon_ipc_connection_close(corrupt_connection);

    if (reader &&
        cbm_daemon_ipc_private_lock_directory_new(reader, &directory) == CBM_PRIVATE_FILE_LOCK_OK &&
        cbm_private_file_lock_try_acquire(directory, CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_FILE,
                                          CBM_PRIVATE_FILE_LOCK_SH,
                                          &record_lock) == CBM_PRIVATE_FILE_LOCK_OK) {
        read_status = cbm_private_file_lock_payload_read(record_lock, readback, sizeof(readback),
                                                         &readback_length);
    }
    if (record_lock) {
        (void)cbm_private_file_lock_release(&record_lock);
    }
    cbm_private_lock_directory_close(directory);
    directory = NULL;
    bool reader_did_not_repair = read_status == CBM_PRIVATE_FILE_LOCK_OK &&
                                 readback_length == sizeof(partial) &&
                                 memcmp(readback, partial, sizeof(partial)) == 0;

    int repair_status = reader ? cbm_daemon_ipc_startup_lock_try_acquire(reader, &startup) : -1;
    if (repair_status == 1 && !cbm_daemon_ipc_startup_lock_prepare_handoff(startup)) {
        repair_status = -1;
    }
    if (repair_status == 1) {
        ipc_test_copy_path(repaired_address, cbm_daemon_ipc_endpoint_address(reader));
    }
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;

    bool repaired = repaired_address[0] && strcmp(original_address, repaired_address) != 0;

    uint8_t mismatched_nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE] = {0};
    char mismatched_address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP] = {0};
    size_t valid_record_length = 0;
    cbm_private_file_lock_status_t mismatch_read = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t mismatch_write = CBM_PRIVATE_FILE_LOCK_IO;
    bool mismatch_encoded = false;
    if (reader &&
        cbm_daemon_ipc_private_lock_directory_new(reader, &directory) == CBM_PRIVATE_FILE_LOCK_OK &&
        cbm_private_file_lock_try_acquire(directory, CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_FILE,
                                          CBM_PRIVATE_FILE_LOCK_EX,
                                          &record_lock) == CBM_PRIVATE_FILE_LOCK_OK) {
        mismatch_read = cbm_private_file_lock_payload_read(record_lock, readback, sizeof(readback),
                                                           &valid_record_length);
        bool decoded = mismatch_read == CBM_PRIVATE_FILE_LOCK_OK &&
                       cbm_daemon_ipc_windows_rendezvous_record_decode(
                           readback, valid_record_length, mismatched_nonce, mismatched_address);
        if (decoded) {
            mismatched_nonce[0] ^= 0x80U;
            mismatch_encoded = cbm_daemon_ipc_windows_rendezvous_record_encode(
                mismatched_nonce, mismatched_address, readback);
        }
        if (mismatch_encoded) {
            mismatch_write =
                cbm_private_file_lock_payload_write(record_lock, readback, sizeof(readback));
        }
    }
    ipc_test_win_lock_release(&record_lock);
    cbm_private_lock_directory_close(directory);
    directory = NULL;
    bool mismatch_failed_closed = mismatch_write == CBM_PRIVATE_FILE_LOCK_OK &&
                                  cbm_daemon_ipc_endpoint_address(reader) == NULL;
    int rebound_status = reader ? cbm_daemon_ipc_startup_lock_try_acquire(reader, &startup) : -1;
    if (rebound_status == 1 && !cbm_daemon_ipc_startup_lock_prepare_handoff(startup)) {
        rebound_status = -1;
    }
    if (rebound_status == 1) {
        ipc_test_copy_path(rebound_address, cbm_daemon_ipc_endpoint_address(reader));
    }
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;
    bool rebound = rebound_address[0] && strcmp(rebound_address, repaired_address) != 0;

    cbm_daemon_ipc_startup_lock_release(&startup);
    if (record_lock) {
        (void)cbm_private_file_lock_release(&record_lock);
    }
    cbm_private_lock_directory_close(directory);
    cbm_daemon_ipc_endpoint_free(reader);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(initial_status, 1);
    ASSERT_TRUE(original_address[0] != '\0');
    ASSERT_EQ(directory_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(record_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(write_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(failed_closed);
    ASSERT_TRUE(reader_did_not_repair);
    ASSERT_EQ(repair_status, 1);
    ASSERT_TRUE(repaired);
    ASSERT_EQ(mismatch_read, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(mismatch_encoded);
    ASSERT_EQ(mismatch_write, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(mismatch_failed_closed);
    ASSERT_EQ(rebound_status, 1);
    ASSERT_TRUE(rebound);
    PASS();
}

TEST(daemon_ipc_windows_startup_and_lifetime_locks_are_cross_process) {
    static const char key[] = "c1d2e3f405162738";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_startup_lock_t *lifetime_startup = NULL;
    cbm_daemon_ipc_participant_guard_t *participant = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *lifetime = NULL;

    bool parent_ok = ipc_test_parent_new(parent, "win-process-locks");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int startup_status =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int child_startup_while_held =
        startup_status == 1 ? ipc_test_win_lock_child("startup", key, parent) : -1;
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;
    int child_startup_after_release =
        endpoint ? ipc_test_win_lock_child("startup", key, parent) : -1;

    int lifetime_startup_status =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &lifetime_startup) : -1;
    bool lifetime_prepared = lifetime_startup_status == 1 &&
                             cbm_daemon_ipc_startup_lock_prepare_handoff(lifetime_startup);
    int participant_status =
        lifetime_prepared ? cbm_daemon_ipc_participant_guard_try_join(endpoint, &participant) : -1;
    int lifetime_status = participant_status == 1
                              ? cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &lifetime)
                              : -1;
    cbm_daemon_ipc_startup_lock_release(&lifetime_startup);
    lifetime_startup = NULL;
    int child_lifetime_while_held =
        lifetime_status == 1 ? ipc_test_win_lock_child("lifetime", key, parent) : -1;
    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    lifetime = NULL;
    int child_lifetime_after_release =
        endpoint ? ipc_test_win_lock_child("lifetime", key, parent) : -1;
    bool participant_released = cbm_daemon_ipc_participant_guard_release(&participant);

    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_startup_lock_release(&lifetime_startup);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(startup_status, 1);
    ASSERT_EQ(child_startup_while_held, 20);
    ASSERT_EQ(child_startup_after_release, 0);
    ASSERT_EQ(lifetime_startup_status, 1);
    ASSERT_TRUE(lifetime_prepared);
    ASSERT_EQ(participant_status, 1);
    ASSERT_EQ(lifetime_status, 1);
    ASSERT_EQ(child_lifetime_while_held, 20);
    ASSERT_EQ(child_lifetime_after_release, 0);
    ASSERT_TRUE(participant_released);
    ASSERT_NULL(participant);
    PASS();
}
#endif

TEST(daemon_ipc_endpoint_is_namespaced_by_instance_key) {
    static const char key_a[] = "0123456789abcdef";
    static const char key_b[] = "fedcba9876543210";
    char parent[TEST_PATH_CAP] = {0};
    char canonical_parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char same_runtime_dir[TEST_PATH_CAP] = {0};
    char other_runtime_dir[TEST_PATH_CAP] = {0};
    bool same_key_same_address = false;
    bool other_key_other_address = false;
    bool address_contains_key = false;
    bool runtime_is_private_child = false;
    bool invalid_key_rejected = false;

    cbm_daemon_ipc_endpoint_t *a = NULL;
    cbm_daemon_ipc_endpoint_t *same = NULL;
    cbm_daemon_ipc_endpoint_t *other = NULL;
    cbm_daemon_ipc_endpoint_t *invalid = NULL;
    cbm_daemon_ipc_startup_lock_t *a_startup = NULL;
    cbm_daemon_ipc_startup_lock_t *other_startup = NULL;

    bool parent_ok =
        ipc_test_parent_new(parent, "namespace") && ipc_test_full_path(canonical_parent, parent);
    if (parent_ok) {
        a = cbm_daemon_ipc_endpoint_new(key_a, parent);
        same = cbm_daemon_ipc_endpoint_new(key_a, parent);
        other = cbm_daemon_ipc_endpoint_new(key_b, parent);
        invalid = cbm_daemon_ipc_endpoint_new("../../not-a-key", parent);
        invalid_key_rejected = invalid == NULL;
    }

#ifdef _WIN32
    int a_startup_status = a ? cbm_daemon_ipc_startup_lock_try_acquire(a, &a_startup) : -1;
    int other_startup_status =
        other ? cbm_daemon_ipc_startup_lock_try_acquire(other, &other_startup) : -1;
    /* Windows addresses are generation-bound and exist only once a startup
     * owner publishes the rendezvous record; drive the documented flow. */
    if (a_startup_status == 1 && !cbm_daemon_ipc_startup_lock_prepare_handoff(a_startup)) {
        a_startup_status = -1;
    }
    if (other_startup_status == 1 &&
        !cbm_daemon_ipc_startup_lock_prepare_handoff(other_startup)) {
        other_startup_status = -1;
    }
    cbm_daemon_ipc_startup_lock_release(&a_startup);
    cbm_daemon_ipc_startup_lock_release(&other_startup);
    a_startup = NULL;
    other_startup = NULL;
    if (a_startup_status != 1 || other_startup_status != 1) {
        cbm_daemon_ipc_endpoint_free(other);
        cbm_daemon_ipc_endpoint_free(same);
        other = NULL;
        same = NULL;
    }
#endif

    if (a && same && other) {
        const char *a_address = cbm_daemon_ipc_endpoint_address(a);
        const char *same_address = cbm_daemon_ipc_endpoint_address(same);
        const char *other_address = cbm_daemon_ipc_endpoint_address(other);
        const char *runtime = cbm_daemon_ipc_endpoint_runtime_dir(a);
        same_key_same_address = a_address && same_address && strcmp(a_address, same_address) == 0;
        other_key_other_address =
            a_address && other_address && strcmp(a_address, other_address) != 0;
#ifdef _WIN32
        address_contains_key = a_address &&
                               strncmp(a_address, "\\\\.\\pipe\\cbm-daemon-",
                                       strlen("\\\\.\\pipe\\cbm-daemon-")) == 0 &&
                               strstr(a_address, key_a) == NULL && other_address &&
                               strstr(other_address, key_b) == NULL;
#else
        address_contains_key = a_address && strstr(a_address, key_a) != NULL && other_address &&
                               strstr(other_address, key_b) != NULL;
#endif
        runtime_is_private_child = runtime && strcmp(runtime, canonical_parent) != 0 &&
                                   ipc_test_path_has_parent(runtime, canonical_parent);
        ipc_test_copy_path(runtime_dir, runtime);
        ipc_test_copy_path(same_runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(same));
        ipc_test_copy_path(other_runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(other));
    }

    cbm_daemon_ipc_endpoint_free(invalid);
    cbm_daemon_ipc_startup_lock_release(&other_startup);
    cbm_daemon_ipc_startup_lock_release(&a_startup);
    cbm_daemon_ipc_endpoint_free(other);
    cbm_daemon_ipc_endpoint_free(same);
    cbm_daemon_ipc_endpoint_free(a);
    if (same_runtime_dir[0] != '\0' && strcmp(same_runtime_dir, runtime_dir) != 0) {
        ipc_test_remove_flat_dir(same_runtime_dir);
    }
    if (other_runtime_dir[0] != '\0' && strcmp(other_runtime_dir, runtime_dir) != 0 &&
        strcmp(other_runtime_dir, same_runtime_dir) != 0) {
        ipc_test_remove_flat_dir(other_runtime_dir);
    }
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(same_key_same_address);
    ASSERT_TRUE(other_key_other_address);
    ASSERT_TRUE(address_contains_key);
    ASSERT_TRUE(runtime_is_private_child);
    ASSERT_TRUE(invalid_key_rejected); /* only exact 16-hex keys enter endpoint names */
    PASS();
}

TEST(daemon_ipc_relative_runtime_parent_is_canonical_and_stable) {
    static const char key[] = "0a1b2c3d4e5f6071";
    char parent[TEST_PATH_CAP] = {0};
    char canonical_parent[TEST_PATH_CAP] = {0};
    char saved_cwd[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char cleanup_runtime_dir[TEST_PATH_CAP] = {0};
    char address[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_connection_t *client = NULL;
    bool parent_ok = false;
    bool cwd_saved = false;
    bool entered_parent = false;
    bool cwd_restored = false;
    bool runtime_canonical = false;
    bool address_absolute = false;
    bool usable_after_chdir = false;

    parent_ok = ipc_test_parent_new(parent, "relative-parent") &&
                ipc_test_full_path(canonical_parent, parent);
    cwd_saved = parent_ok && ipc_test_getcwd(saved_cwd, sizeof(saved_cwd)) != NULL;
    entered_parent = cwd_saved && ipc_test_chdir(parent) == 0;
    if (entered_parent) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, ".");
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        (void)ipc_test_full_path(cleanup_runtime_dir, runtime_dir);
        runtime_canonical = ipc_test_path_is_absolute(runtime_dir) &&
                            ipc_test_path_has_parent(runtime_dir, canonical_parent);
    }
    if (entered_parent) {
        cwd_restored = ipc_test_chdir(saved_cwd) == 0;
    }
    if (endpoint && cwd_restored) {
        listener = cbm_daemon_ipc_listen(endpoint);
        ipc_test_copy_path(address, cbm_daemon_ipc_endpoint_address(endpoint));
#ifdef _WIN32
        address_absolute = strncmp(address, "\\\\.\\pipe\\", 9) == 0;
#else
        address_absolute = ipc_test_path_is_absolute(address);
#endif
        if (listener) {
            client = cbm_daemon_ipc_connect(endpoint, 500);
        }
        usable_after_chdir = listener != NULL && client != NULL;
    }

    cbm_daemon_ipc_connection_close(client);
    cbm_daemon_ipc_listener_close(listener);
    cbm_daemon_ipc_endpoint_free(endpoint);
    if (entered_parent && !cwd_restored) {
        cwd_restored = ipc_test_chdir(saved_cwd) == 0;
    }
    ipc_test_remove_tree(cleanup_runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(cwd_saved);
    ASSERT_TRUE(entered_parent);
    ASSERT_TRUE(cwd_restored);
    ASSERT_TRUE(runtime_canonical);
    ASSERT_TRUE(address_absolute);
    ASSERT_TRUE(usable_after_chdir);
    PASS();
}

TEST(daemon_ipc_rejects_uppercase_instance_key) {
    static const char uppercase_key[] = "ABCDEF0123456789";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;

    bool parent_ok = ipc_test_parent_new(parent, "uppercase-key");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(uppercase_key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    bool rejected = endpoint == NULL;

    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(daemon_ipc_no_spawn_probe_distinguishes_absent_active_and_busy) {
    static const char key[] = "2468ace02468ace0";
    enum { PROBE_CLIENT_CAP = 64 };
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_connection_t *clients[PROBE_CLIENT_CAP] = {0};
    size_t client_count = 0;
    int absent_before = -1;
    int active_or_busy = -1;
    int absent_after = -1;

    if (ipc_test_parent_new(parent, "no-spawn-probe")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        absent_before = cbm_daemon_ipc_endpoint_probe(endpoint, 1);
        listener = cbm_daemon_ipc_listen(endpoint);
    }
    /* Do not accept: on Windows the sole pipe instance becomes BUSY; on
     * POSIX this fills the listen backlog. Probe must still report ACTIVE. */
    while (listener && client_count < PROBE_CLIENT_CAP) {
        cbm_daemon_ipc_connection_t *client = cbm_daemon_ipc_connect(endpoint, 1);
        if (!client) {
            break;
        }
        clients[client_count++] = client;
    }
    if (listener) {
        active_or_busy = cbm_daemon_ipc_endpoint_probe(endpoint, 1);
    }
    for (size_t i = 0; i < client_count; i++) {
        cbm_daemon_ipc_connection_close(clients[i]);
    }
    cbm_daemon_ipc_listener_close(listener);
    listener = NULL;
    if (endpoint) {
        absent_after = cbm_daemon_ipc_endpoint_probe(endpoint, 1);
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_EQ(absent_before, 0);
    ASSERT(client_count > 0);
    ASSERT_EQ(active_or_busy, 1);
    ASSERT_EQ(absent_after, 0);
    PASS();
}

TEST(daemon_ipc_lifetime_reservation_survives_saturated_second_listen) {
    static const char key[] = "13579bdf13579bdf";
    enum { PROBE_CLIENT_CAP = 64 };
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *first = NULL;
    cbm_daemon_ipc_listener_t *second = NULL;
    cbm_daemon_ipc_listener_t *after_close = NULL;
    cbm_daemon_ipc_connection_t *clients[PROBE_CLIENT_CAP] = {0};
    size_t client_count = 0;
    bool first_started = false;
    bool after_close_started = false;
    int free_before = -1;
    int held_while_listening = -1;
    int held_after_second_attempt = -1;
    int endpoint_after_second_attempt = -1;
    int free_after_close = -1;

    if (ipc_test_parent_new(parent, "lifetime-reservation")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        free_before = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        first = cbm_daemon_ipc_listen(endpoint);
        first_started = first != NULL;
    }
    if (first) {
        held_while_listening = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
    }
    /* Keep the first listener from accepting and fill as much transport
     * capacity as the platform exposes. On BSD-derived kernels a subsequent
     * connect can report ECONNREFUSED even though this listener is live. */
    while (first && client_count < PROBE_CLIENT_CAP) {
        cbm_daemon_ipc_connection_t *client = cbm_daemon_ipc_connect(endpoint, 1);
        if (!client) {
            break;
        }
        clients[client_count++] = client;
    }
    if (first) {
        second = cbm_daemon_ipc_listen(endpoint);
        held_after_second_attempt = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        endpoint_after_second_attempt = cbm_daemon_ipc_endpoint_probe(endpoint, 1);
    }
    for (size_t i = 0; i < client_count; i++) {
        cbm_daemon_ipc_connection_close(clients[i]);
    }
    cbm_daemon_ipc_listener_close(second);
    cbm_daemon_ipc_listener_close(first);
    first = NULL;
    if (endpoint) {
        free_after_close = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        after_close = cbm_daemon_ipc_listen(endpoint);
        after_close_started = after_close != NULL;
    }
    cbm_daemon_ipc_listener_close(after_close);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_EQ(free_before, 0);
    ASSERT_TRUE(first_started);
    ASSERT_TRUE(client_count > 0);
    ASSERT_EQ(held_while_listening, 1);
    ASSERT_TRUE(second == NULL);
    ASSERT_EQ(held_after_second_attempt, 1);
    ASSERT_EQ(endpoint_after_second_attempt, 1);
    ASSERT_EQ(free_after_close, 0);
    ASSERT_TRUE(after_close_started);
    PASS();
}

TEST(daemon_ipc_lifetime_reservation_transfers_without_unlock_window) {
    static const char key[] = "1029384756abcdef";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_participant_guard_t *participant = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *reservation = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_listener_t *contender = NULL;
    cbm_daemon_ipc_connection_t *client = NULL;
    int acquired = -1;
    int held_before_transfer = -1;
    int held_after_transfer = -1;
    int free_after_close = -1;
    bool transfer_consumed = false;
    bool connected = false;

    if (ipc_test_parent_new(parent, "lifetime-transfer")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        int startup_result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup);
        bool prepared = startup_result == 1 && cbm_daemon_ipc_startup_lock_prepare_handoff(startup);
        int participant_result =
            prepared ? cbm_daemon_ipc_participant_guard_try_join(endpoint, &participant) : -1;
        acquired = participant_result == 1
                       ? cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &reservation)
                       : -1;
        cbm_daemon_ipc_startup_lock_release(&startup);
        startup = NULL;
    }
    if (reservation) {
        held_before_transfer = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        contender = cbm_daemon_ipc_listen(endpoint);
        listener = cbm_daemon_ipc_listen_reserved(endpoint, &reservation);
        transfer_consumed = listener != NULL && reservation == NULL;
    }
    if (listener) {
        held_after_transfer = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        client = cbm_daemon_ipc_connect(endpoint, 500);
        connected = client != NULL;
    }

    cbm_daemon_ipc_connection_close(client);
    cbm_daemon_ipc_listener_close(contender);
    cbm_daemon_ipc_listener_close(listener);
    listener = NULL;
    cbm_daemon_ipc_lifetime_reservation_release(reservation);
    reservation = NULL;
    bool participant_released = cbm_daemon_ipc_participant_guard_release(&participant);
    if (endpoint) {
        free_after_close = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_EQ(acquired, 1);
    ASSERT_EQ(held_before_transfer, 1);
    ASSERT_TRUE(contender == NULL);
    ASSERT_TRUE(transfer_consumed);
    ASSERT_TRUE(connected);
    ASSERT_EQ(held_after_transfer, 1);
    ASSERT_EQ(free_after_close, 0);
    ASSERT_TRUE(participant_released);
    ASSERT_NULL(participant);
    PASS();
}

typedef struct {
    cbm_daemon_ipc_listener_t *listener;
    int result;
    uint64_t peer_pid;
} ipc_roundtrip_server_t;

static uint64_t ipc_test_process_id(void) {
#ifdef _WIN32
    return (uint64_t)GetCurrentProcessId();
#else
    return (uint64_t)getpid();
#endif
}

static void *ipc_roundtrip_server(void *opaque) {
    static const uint8_t expected_request[] = {0x00, 'r', 'e', 'q', 0xff};
    static const uint8_t response[] = {'o', 'k', 0x00, 0x7f};
    ipc_roundtrip_server_t *server = (ipc_roundtrip_server_t *)opaque;
    cbm_daemon_ipc_connection_t *connection = NULL;
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;

    server->result = 1;
    if (cbm_daemon_ipc_accept(server->listener, 2000, &connection) != 1 || !connection) {
        goto done;
    }
    server->peer_pid = cbm_daemon_ipc_connection_peer_pid(connection);
    if (server->peer_pid == 0) {
        goto done;
    }
    if (cbm_daemon_ipc_receive_frame(connection, 2000, &frame, &payload) != 1) {
        goto done;
    }
    if (frame.type != CBM_DAEMON_FRAME_REQUEST || frame.flags != 0x1234 ||
        frame.length != sizeof(expected_request) || !payload ||
        memcmp(payload, expected_request, sizeof(expected_request)) != 0) {
        goto done;
    }
    if (!cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_RESPONSE, 0x00a5, response,
                                   (uint32_t)sizeof(response))) {
        goto done;
    }
    server->result = 0;

done:
    free(payload); /* receive_frame returns a malloc-owned payload */
    cbm_daemon_ipc_connection_close(connection);
    return NULL;
}

TEST(daemon_ipc_local_frame_roundtrip) {
    static const char key[] = "1111222233334444";
    static const uint8_t request[] = {0x00, 'r', 'e', 'q', 0xff};
    static const uint8_t expected_response[] = {'o', 'k', 0x00, 0x7f};
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_connection_t *client = NULL;
    cbm_thread_t thread;
    bool thread_started = false;
    ipc_roundtrip_server_t server = {0};
    cbm_daemon_frame_t response_frame = {0};
    uint8_t *response_payload = NULL;
    uint64_t client_peer_pid = 0;
    int result = 1;

    if (!ipc_test_parent_new(parent, "roundtrip")) {
        goto cleanup;
    }
    endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    if (!endpoint) {
        goto cleanup;
    }
    ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    listener = cbm_daemon_ipc_listen(endpoint);
    if (!listener) {
        goto cleanup;
    }
    server.listener = listener;
    server.result = 1;
    if (cbm_thread_create(&thread, 0, ipc_roundtrip_server, &server) != 0) {
        goto cleanup;
    }
    thread_started = true;
    client = cbm_daemon_ipc_connect(endpoint, 2000);
    client_peer_pid = cbm_daemon_ipc_connection_peer_pid(client);
    if (!client || client_peer_pid == 0 ||
        !cbm_daemon_ipc_send_frame(client, CBM_DAEMON_FRAME_REQUEST, 0x1234, request,
                                   (uint32_t)sizeof(request)) ||
        cbm_daemon_ipc_receive_frame(client, 2000, &response_frame, &response_payload) != 1) {
        goto cleanup;
    }
    if (response_frame.type != CBM_DAEMON_FRAME_RESPONSE || response_frame.flags != 0x00a5 ||
        response_frame.length != sizeof(expected_response) || !response_payload ||
        memcmp(response_payload, expected_response, sizeof(expected_response)) != 0) {
        goto cleanup;
    }
    result = 0;

cleanup:
    free(response_payload);
    cbm_daemon_ipc_connection_close(client);
    if (thread_started) {
        int join_result = cbm_thread_join(&thread);
        if (join_result != 0 || server.result != 0) {
            result = 1;
        }
    }
    cbm_daemon_ipc_listener_close(listener);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_EQ(result, 0);
    ASSERT_EQ(client_peer_pid, ipc_test_process_id());
    ASSERT_EQ(server.peer_pid, ipc_test_process_id());
    PASS();
}

TEST(daemon_ipc_bounded_receive_rejects_oversize_before_payload) {
    static const char key[] = "2021222324252627";
    static const uint8_t oversized_payload[] = {'o', 'v', 'e', 'r', '!'};
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_connection_t *client = NULL;
    cbm_daemon_ipc_connection_t *server = NULL;
    cbm_daemon_frame_t frame = {0};
    uint8_t *received_payload = NULL;
    bool parent_ok = ipc_test_parent_new(parent, "bounded-frame");
    int accepted = -1;
    bool sent = false;
    int bounded_result = 1;
    int reuse_result = 1;
    bool bounded_payload_absent = false;
    bool bounded_frame_empty = false;

    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        listener = cbm_daemon_ipc_listen(endpoint);
    }
    if (listener) {
        client = cbm_daemon_ipc_connect(endpoint, 500);
    }
    if (client) {
        accepted = cbm_daemon_ipc_accept(listener, 500, &server);
    }
    if (accepted == 1 && server) {
        sent = cbm_daemon_ipc_send_frame(client, CBM_DAEMON_FRAME_REQUEST, 0x2021,
                                         oversized_payload, (uint32_t)sizeof(oversized_payload));
    }
    if (sent) {
        bounded_result = cbm_daemon_ipc_receive_frame_bounded(
            server, 500, (uint32_t)sizeof(oversized_payload) - 1, &frame, &received_payload);
        bounded_payload_absent = received_payload == NULL;
        bounded_frame_empty = frame.length == 0;
        free(received_payload);
        received_payload = NULL;
        reuse_result = cbm_daemon_ipc_receive_frame(server, 1, &frame, &received_payload);
    }

    free(received_payload);
    cbm_daemon_ipc_connection_close(server);
    cbm_daemon_ipc_connection_close(client);
    cbm_daemon_ipc_listener_close(listener);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(accepted, 1);
    ASSERT_TRUE(sent);
    ASSERT_EQ(bounded_result, -1);
    ASSERT_TRUE(bounded_payload_absent);
    ASSERT_TRUE(bounded_frame_empty);
    ASSERT_EQ(reuse_result, -1);
    PASS();
}

typedef struct {
    cbm_daemon_ipc_listener_t *listener;
    cbm_daemon_ipc_connection_t *connection;
    atomic_bool waiting;
    int receive_result;
} ipc_forever_wait_server_t;

static void *ipc_forever_wait_server(void *opaque) {
    ipc_forever_wait_server_t *server = (ipc_forever_wait_server_t *)opaque;
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    server->receive_result = -2;
    if (cbm_daemon_ipc_accept(server->listener, 2000, &server->connection) == 1 &&
        server->connection) {
        atomic_store_explicit(&server->waiting, true, memory_order_release);
        server->receive_result = cbm_daemon_ipc_receive_frame(
            server->connection, CBM_DAEMON_IPC_WAIT_FOREVER, &frame, &payload);
    }
    free(payload);
    cbm_daemon_ipc_connection_close(server->connection);
    server->connection = NULL;
    return NULL;
}

TEST(daemon_ipc_wait_forever_is_interruptible) {
    static const char key[] = "1212121212121212";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_connection_t *client = NULL;
    cbm_thread_t thread;
    bool thread_started = false;
    bool reached_wait = false;
    int join_result = -1;
    ipc_forever_wait_server_t server = {0};
    atomic_init(&server.waiting, false);

    if (ipc_test_parent_new(parent, "wait-forever")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        listener = cbm_daemon_ipc_listen(endpoint);
    }
    if (listener) {
        server.listener = listener;
        thread_started = cbm_thread_create(&thread, 0, ipc_forever_wait_server, &server) == 0;
    }
    if (thread_started) {
        client = cbm_daemon_ipc_connect(endpoint, 2000);
    }
    uint64_t deadline = cbm_now_ms() + 2000;
    while (client && cbm_now_ms() < deadline &&
           !atomic_load_explicit(&server.waiting, memory_order_acquire)) {
        cbm_usleep(1000);
    }
    reached_wait = atomic_load_explicit(&server.waiting, memory_order_acquire);
    if (reached_wait) {
        cbm_daemon_ipc_connection_interrupt(server.connection);
    }
    if (thread_started) {
        join_result = cbm_thread_join(&thread);
    }
    cbm_daemon_ipc_connection_close(client);
    cbm_daemon_ipc_listener_close(listener);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(thread_started);
    ASSERT_TRUE(client != NULL);
    ASSERT_TRUE(reached_wait);
    ASSERT_EQ(join_result, 0);
    ASSERT_EQ(server.receive_result, -1);
    PASS();
}

typedef struct {
    const cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_daemon_ipc_listener_t *listener;
    atomic_bool delay_started;
} ipc_delayed_listener_t;

static void *ipc_delayed_listener_start(void *opaque) {
    ipc_delayed_listener_t *delayed = (ipc_delayed_listener_t *)opaque;
    struct timespec delay = {.tv_sec = 0, .tv_nsec = 150000000};
    atomic_store_explicit(&delayed->delay_started, true, memory_order_release);
    (void)cbm_nanosleep(&delay, NULL);
    delayed->listener = cbm_daemon_ipc_listen(delayed->endpoint);
    return NULL;
}

TEST(daemon_ipc_connect_waits_for_delayed_listener) {
    static const char key[] = "2222333344445555";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_connection_t *client = NULL;
    cbm_thread_t thread;
    bool thread_started = false;
    bool delay_observed = false;
    int join_result = -1;
    ipc_delayed_listener_t delayed = {0};
    atomic_init(&delayed.delay_started, false);

    bool parent_ok = ipc_test_parent_new(parent, "delayed-listener");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        delayed.endpoint = endpoint;
        thread_started = cbm_thread_create(&thread, 0, ipc_delayed_listener_start, &delayed) == 0;
    }
    if (thread_started) {
        struct timespec poll_delay = {.tv_sec = 0, .tv_nsec = 1000000};
        for (size_t i = 0; i < 2000; i++) {
            if (atomic_load_explicit(&delayed.delay_started, memory_order_acquire)) {
                delay_observed = true;
                break;
            }
            (void)cbm_nanosleep(&poll_delay, NULL);
        }
    }
    if (delay_observed) {
        client = cbm_daemon_ipc_connect(endpoint, 1500);
    }
    if (thread_started) {
        join_result = cbm_thread_join(&thread);
    }
    bool connected_after_wait = client != NULL && delayed.listener != NULL;

    cbm_daemon_ipc_connection_close(client);
    cbm_daemon_ipc_listener_close(delayed.listener);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(thread_started);
    ASSERT_TRUE(delay_observed);
    ASSERT_EQ(join_result, 0);
    ASSERT_TRUE(connected_after_wait);
    PASS();
}

typedef struct {
    const cbm_daemon_ipc_endpoint_t *endpoint;
    int result;
} ipc_lock_contender_t;

static void *ipc_lock_contender(void *opaque) {
    ipc_lock_contender_t *contender = (ipc_lock_contender_t *)opaque;
    cbm_daemon_ipc_startup_lock_t *lock = NULL;
    contender->result = cbm_daemon_ipc_startup_lock_try_acquire(contender->endpoint, &lock);
    cbm_daemon_ipc_startup_lock_release(&lock);
    return NULL;
}

TEST(daemon_ipc_startup_lock_has_one_winner) {
    static const char key[] = "5555666677778888";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *first = NULL;
    cbm_daemon_ipc_startup_lock_t *after_release = NULL;
    cbm_thread_t thread;
    bool thread_started = false;
    ipc_lock_contender_t contender = {0};
    int first_result = -1;
    int reacquire_result = -1;
    int join_result = -1;

    if (ipc_test_parent_new(parent, "startup-lock")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        first_result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &first);
    }
    if (first_result == 1 && first) {
        contender.endpoint = endpoint;
        contender.result = -1;
        if (cbm_thread_create(&thread, 0, ipc_lock_contender, &contender) == 0) {
            thread_started = true;
            join_result = cbm_thread_join(&thread);
            thread_started = false;
        }
    }
    cbm_daemon_ipc_startup_lock_release(&first);
    first = NULL;
    if (endpoint) {
        reacquire_result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &after_release);
    }
    cbm_daemon_ipc_startup_lock_release(&after_release);
    if (thread_started) {
        (void)cbm_thread_join(&thread);
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_EQ(first_result, 1);
    ASSERT_EQ(join_result, 0);
    ASSERT_EQ(contender.result, 0); /* held, not an OS/error failure */
    ASSERT_EQ(reacquire_result, 1);
    PASS();
}

TEST(daemon_ipc_activation_probe_ignores_matching_startup_claim_only) {
    static const char key[] = "5c5c6d6d7e7e8f8f";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_endpoint_t *wrong_endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;

    bool parent_ok = ipc_test_parent_new(parent, "activation-probe");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
        wrong_endpoint = cbm_daemon_ipc_endpoint_new("5c5c6d6d7e7e8f90", parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int without_lock =
        endpoint ? cbm_daemon_ipc_generation_probe_under_startup_lock(endpoint, NULL) : 0;
    int startup_result =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int generic_self = startup ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    int matching =
        startup ? cbm_daemon_ipc_generation_probe_under_startup_lock(endpoint, startup) : -2;
    int mismatched =
        startup && wrong_endpoint
            ? cbm_daemon_ipc_generation_probe_under_startup_lock(wrong_endpoint, startup)
            : -2;
    bool prepared = startup && cbm_daemon_ipc_startup_lock_prepare_handoff(startup);
    int after_prepare =
        prepared ? cbm_daemon_ipc_generation_probe_under_startup_lock(endpoint, startup) : -2;
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;
    int after_release = endpoint ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    listener = endpoint ? cbm_daemon_ipc_listen(endpoint) : NULL;
    bool listener_started = listener != NULL;
    int active_startup =
        listener ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int active_generation =
        active_startup == 1 ? cbm_daemon_ipc_generation_probe_under_startup_lock(endpoint, startup)
                            : -1;
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;
    cbm_daemon_ipc_listener_close(listener);

    cbm_daemon_ipc_endpoint_free(wrong_endpoint);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(without_lock, -1);
    ASSERT_EQ(startup_result, 1);
    ASSERT_EQ(generic_self, 1);
    ASSERT_EQ(matching, 0);
    ASSERT_EQ(mismatched, -1);
    ASSERT_TRUE(prepared);
    ASSERT_EQ(after_prepare, -1);
    ASSERT_EQ(after_release, 0);
    ASSERT_TRUE(listener_started);
    ASSERT_EQ(active_startup, 1);
    ASSERT_EQ(active_generation, 1);
    PASS();
}

TEST(daemon_ipc_local_transition_coexists_with_active_daemon_lifetime) {
    static const char key[] = "5d5d6e6e7f7f8080";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_startup_lock_t *startup_during_local = NULL;
    cbm_daemon_ipc_startup_lock_t *startup_after = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *lifetime = NULL;
    cbm_daemon_ipc_participant_guard_t *daemon_participant = NULL;
    cbm_daemon_ipc_local_transition_t *transition = NULL;

    bool parent_ok = ipc_test_parent_new(parent, "local-transition");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int startup_result =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    bool handoff = startup_result == 1 && cbm_daemon_ipc_startup_lock_prepare_handoff(startup);
    int participant_result =
        handoff ? cbm_daemon_ipc_participant_guard_try_join(endpoint, &daemon_participant) : -1;
    int lifetime_result = participant_result == 1
                              ? cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &lifetime)
                              : -1;
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;

    int transition_result = lifetime_result == 1
                                ? cbm_daemon_ipc_local_transition_try_acquire(endpoint, &transition)
                                : -1;
    int unsealed_lifetime =
        transition_result == 1
            ? cbm_daemon_ipc_local_transition_lifetime_probe(endpoint, transition)
            : 0;
    int seal_result =
        transition_result == 1 ? cbm_daemon_ipc_local_transition_seal_legacy(transition) : -1;
    int sealed_lifetime = seal_result == 1
                              ? cbm_daemon_ipc_local_transition_lifetime_probe(endpoint, transition)
                              : -1;
    bool work_begun =
        sealed_lifetime == 1 && cbm_daemon_ipc_local_transition_begin_work(transition);
    int startup_during_result =
        work_begun ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup_during_local) : -1;
    cbm_daemon_ipc_startup_lock_release(&startup_during_local);
    startup_during_local = NULL;
    bool transition_released = cbm_daemon_ipc_local_transition_release(&transition);
    int lifetime_after_transition =
        endpoint ? cbm_daemon_ipc_lifetime_reservation_probe(endpoint) : -1;

    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    lifetime = NULL;
    bool participant_released = cbm_daemon_ipc_participant_guard_release(&daemon_participant);
    int startup_after_result =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup_after) : -1;
    cbm_daemon_ipc_startup_lock_release(&startup_after);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(startup_result, 1);
    ASSERT_TRUE(handoff);
    ASSERT_EQ(participant_result, 1);
    ASSERT_EQ(lifetime_result, 1);
    ASSERT_EQ(transition_result, 1);
    ASSERT_EQ(unsealed_lifetime, -1);
    ASSERT_EQ(seal_result, 1);
    ASSERT_EQ(sealed_lifetime, 1);
    ASSERT_TRUE(work_begun);
    ASSERT_EQ(startup_during_result, 1);
    ASSERT_TRUE(transition_released);
    ASSERT_NULL(transition);
    ASSERT_EQ(lifetime_after_transition, 1);
    ASSERT_TRUE(participant_released);
    ASSERT_NULL(daemon_participant);
    ASSERT_EQ(startup_after_result, 1);
    PASS();
}

TEST(daemon_ipc_local_participants_overlap_and_allow_modern_startup) {
    static const char key[] = "5e5e6f6f70708181";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_local_transition_t *first = NULL;
    cbm_daemon_ipc_local_transition_t *second = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;

    bool parent_ok = ipc_test_parent_new(parent, "local-overlap");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int first_acquired =
        endpoint ? cbm_daemon_ipc_local_transition_try_acquire(endpoint, &first) : -1;
    int first_sealed =
        first_acquired == 1 ? cbm_daemon_ipc_local_transition_seal_legacy(first) : -1;
    int first_presence =
        first_sealed == 1 ? cbm_daemon_ipc_local_transition_lifetime_probe(endpoint, first) : -1;
    bool first_begun = first_presence == 0 && cbm_daemon_ipc_local_transition_begin_work(first);

    int second_acquired =
        first_begun ? cbm_daemon_ipc_local_transition_try_acquire(endpoint, &second) : -1;
    int second_sealed =
        second_acquired == 1 ? cbm_daemon_ipc_local_transition_seal_legacy(second) : -1;
    int second_presence =
        second_sealed == 1 ? cbm_daemon_ipc_local_transition_lifetime_probe(endpoint, second) : -1;
    bool second_begun = second_presence == 0 && cbm_daemon_ipc_local_transition_begin_work(second);
    int startup_during =
        second_begun ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;
    int legacy_while_both = second_begun ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    bool first_released = cbm_daemon_ipc_local_transition_release(&first);
    int legacy_while_second =
        first_released ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    bool second_released = cbm_daemon_ipc_local_transition_release(&second);
    int legacy_after = second_released ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;

    (void)cbm_daemon_ipc_local_transition_release(&second);
    (void)cbm_daemon_ipc_local_transition_release(&first);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(first_acquired, 1);
    ASSERT_EQ(first_sealed, 1);
    ASSERT_EQ(first_presence, 0);
    ASSERT_TRUE(first_begun);
    ASSERT_EQ(second_acquired, 1);
    ASSERT_EQ(second_sealed, 1);
    ASSERT_EQ(second_presence, 0);
    ASSERT_TRUE(second_begun);
    ASSERT_EQ(startup_during, 1);
    ASSERT_EQ(legacy_while_both, 1);
    ASSERT_TRUE(first_released);
    ASSERT_EQ(legacy_while_second, 1);
    ASSERT_TRUE(second_released);
    ASSERT_EQ(legacy_after, 0);
    PASS();
}

TEST(daemon_ipc_windows_local_transition_release_retries_retained_mutex) {
#ifndef _WIN32
    PASS();
#else
    static const char key[] = "5f5f606071718282";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_local_transition_t *transition = NULL;
    cbm_daemon_ipc_startup_lock_t *after = NULL;

    bool parent_ok = ipc_test_parent_new(parent, "local-release-retry");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int acquired =
        endpoint ? cbm_daemon_ipc_local_transition_try_acquire(endpoint, &transition) : -1;
    int sealed = acquired == 1 ? cbm_daemon_ipc_local_transition_seal_legacy(transition) : -1;
    int lifetime =
        sealed == 1 ? cbm_daemon_ipc_local_transition_lifetime_probe(endpoint, transition) : -1;
    bool begun = lifetime == 0 && cbm_daemon_ipc_local_transition_begin_work(transition);
    if (begun) {
        cbm_daemon_ipc_windows_legacy_guard_release_failures_set_for_test(1);
    }
    bool first_release = cbm_daemon_ipc_local_transition_release(&transition);
    bool retained_after_failure = transition != NULL;
    bool retry_release = cbm_daemon_ipc_local_transition_release(&transition);
    cbm_daemon_ipc_windows_legacy_guard_release_failures_set_for_test(0);
    int legacy_after = endpoint ? cbm_daemon_ipc_legacy_generation_probe(endpoint) : -1;
    int startup_after = endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &after) : -1;
    cbm_daemon_ipc_startup_lock_release(&after);
    (void)cbm_daemon_ipc_local_transition_release(&transition);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(acquired, 1);
    ASSERT_EQ(sealed, 1);
    ASSERT_EQ(lifetime, 0);
    ASSERT_TRUE(begun);
    ASSERT_FALSE(first_release);
    ASSERT_TRUE(retained_after_failure);
    ASSERT_TRUE(retry_release);
    ASSERT_NULL(transition);
    ASSERT_EQ(legacy_after, 0);
    ASSERT_EQ(startup_after, 1);
    PASS();
#endif
}

TEST(daemon_ipc_windows_startup_release_retains_retry_authority) {
#ifndef _WIN32
    PASS();
#else
    static const char key[] = "6f6f707081819292";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_startup_lock_t *blocked = NULL;
    cbm_daemon_ipc_startup_lock_t *after = NULL;

    bool parent_ok = ipc_test_parent_new(parent, "startup-release-retry");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int acquired = endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    if (acquired == 1) {
        cbm_daemon_ipc_windows_legacy_guard_release_failures_set_for_test(1);
    }
    bool first_release = cbm_daemon_ipc_startup_lock_release(&startup);
    bool retained_after_failure = startup != NULL;
    int blocked_while_retained =
        retained_after_failure ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &blocked) : -1;
    bool retry_release = cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_windows_legacy_guard_release_failures_set_for_test(0);
    int acquired_after =
        retry_release ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &after) : -1;
    bool after_released = cbm_daemon_ipc_startup_lock_release(&after);
    (void)cbm_daemon_ipc_startup_lock_release(&blocked);
    (void)cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(acquired, 1);
    ASSERT_FALSE(first_release);
    ASSERT_TRUE(retained_after_failure);
    ASSERT_EQ(blocked_while_retained, 0);
    ASSERT_TRUE(retry_release);
    ASSERT_NULL(startup);
    ASSERT_EQ(acquired_after, 1);
    ASSERT_TRUE(after_released);
    PASS();
#endif
}

/* A frame deadline is a fail-stop boundary.  In particular, Windows
 * overlapped I/O can complete while cancellation races a timeout, so an
 * offset-zero timeout does not prove that the byte stream is untouched.
 * Reusing that connection could interpret trailing bytes as a new header. */
TEST(daemon_ipc_frame_timeout_poisons_connection) {
    static const char key[] = "5a5a6b6b7c7c8d8d";
    static const uint8_t payload[] = {'a', 'f', 't', 'e', 'r'};
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_connection_t *client = NULL;
    cbm_daemon_ipc_connection_t *server = NULL;
    cbm_daemon_frame_t frame = {0};
    uint8_t *received_payload = NULL;
    int accept_result = -1;
    int timeout_result = -1;
    int reuse_result = 1;
    bool sent_after_timeout = false;

    bool parent_ok = ipc_test_parent_new(parent, "frame-timeout");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        listener = cbm_daemon_ipc_listen(endpoint);
    }
    if (listener) {
        client = cbm_daemon_ipc_connect(endpoint, 500);
    }
    if (client) {
        accept_result = cbm_daemon_ipc_accept(listener, 500, &server);
    }
    if (accept_result == 1 && server) {
        timeout_result = cbm_daemon_ipc_receive_frame(server, 20, &frame, &received_payload);
    }
    free(received_payload);
    received_payload = NULL;
    memset(&frame, 0, sizeof(frame));
    if (timeout_result == 0) {
        sent_after_timeout = cbm_daemon_ipc_send_frame(client, CBM_DAEMON_FRAME_REQUEST, 0x1357,
                                                       payload, (uint32_t)sizeof(payload));
    }
    if (sent_after_timeout) {
        reuse_result = cbm_daemon_ipc_receive_frame(server, 500, &frame, &received_payload);
    }

    free(received_payload);
    cbm_daemon_ipc_connection_close(server);
    cbm_daemon_ipc_connection_close(client);
    cbm_daemon_ipc_listener_close(listener);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(accept_result, 1);
    ASSERT_EQ(timeout_result, 0);
    ASSERT_TRUE(sent_after_timeout);
    ASSERT_EQ(reuse_result, -1);
    PASS();
}

#ifndef _WIN32

static bool ipc_test_fd_write_all(int fd, const void *buffer, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t written = write(fd, (const uint8_t *)buffer + offset, length - offset);
        if (written > 0) {
            offset += (size_t)written;
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

static bool ipc_test_fd_read_all(int fd, void *buffer, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t received = read(fd, (uint8_t *)buffer + offset, length - offset);
        if (received > 0) {
            offset += (size_t)received;
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

static bool ipc_test_socket_send_all(int fd, const void *buffer, size_t length) {
    size_t offset = 0;
    while (offset < length) {
#ifdef MSG_NOSIGNAL
        ssize_t written = send(fd, (const uint8_t *)buffer + offset, length - offset, MSG_NOSIGNAL);
#else
        ssize_t written = send(fd, (const uint8_t *)buffer + offset, length - offset, 0);
#endif
        if (written > 0) {
            offset += (size_t)written;
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

static bool ipc_test_unix_address_set(struct sockaddr_un *address, const char *path,
                                      socklen_t *length_out) {
    if (!address || !path || !length_out) {
        return false;
    }
    size_t path_length = strlen(path);
    if (path_length == 0 || path_length >= sizeof(address->sun_path)) {
        return false;
    }
    memset(address, 0, sizeof(*address));
    address->sun_family = AF_UNIX;
    memcpy(address->sun_path, path, path_length + 1);
    socklen_t address_length =
        (socklen_t)(offsetof(struct sockaddr_un, sun_path) + path_length + 1);
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    address->sun_len = (uint8_t)address_length;
#endif
    *length_out = address_length;
    return true;
}

static bool ipc_test_socket_identity_path(char out[TEST_PATH_CAP], const char *socket_path) {
    int written = socket_path ? snprintf(out, TEST_PATH_CAP, "%s.identity", socket_path) : -1;
    return written > 0 && written < TEST_PATH_CAP;
}

static bool ipc_test_socket_anchor_path(char out[TEST_PATH_CAP], const char *runtime_dir,
                                        const char *key) {
    int written =
        runtime_dir && key ? snprintf(out, TEST_PATH_CAP, "%s/cbm-%s.anc", runtime_dir, key) : -1;
    return written > 0 && written < TEST_PATH_CAP;
}

static bool ipc_test_socket_pending_path(char out[TEST_PATH_CAP], const char *socket_path) {
    int written = socket_path ? snprintf(out, TEST_PATH_CAP, "%s.pending", socket_path) : -1;
    return written > 0 && written < TEST_PATH_CAP;
}

#ifndef _WIN32
static bool ipc_test_record_temp_path(char out[TEST_PATH_CAP], const char *runtime_dir,
                                      const char *record_path) {
    if (!out || !runtime_dir || !record_path) {
        return false;
    }
    out[0] = '\0';
    const char *base = strrchr(record_path, '/');
    base = base ? base + 1 : record_path;
    char prefix[TEST_PATH_CAP];
    int prefix_length = snprintf(prefix, sizeof(prefix), "%s.tmp", base);
    if (prefix_length <= 0 || prefix_length >= (int)sizeof(prefix)) {
        return false;
    }
    DIR *directory = opendir(runtime_dir);
    if (!directory) {
        return false;
    }
    bool found = false;
    bool ambiguous = false;
    struct dirent *entry;
    while ((entry = readdir(directory)) != NULL) {
        if (strncmp(entry->d_name, prefix, (size_t)prefix_length) != 0) {
            continue;
        }
        if (found) {
            ambiguous = true;
            break;
        }
        int written = snprintf(out, TEST_PATH_CAP, "%s/%s", runtime_dir, entry->d_name);
        found = written > 0 && written < TEST_PATH_CAP;
        if (!found) {
            break;
        }
    }
    (void)closedir(directory);
    if (ambiguous || !found) {
        out[0] = '\0';
        return false;
    }
    return true;
}
#endif

static void ipc_test_publication_crash_hook(cbm_daemon_ipc_posix_publication_stage_t stage,
                                            void *opaque) {
    const cbm_daemon_ipc_posix_publication_stage_t *target = opaque;
    if (target && stage == *target) {
        _exit(40 + (int)stage);
    }
}

static int ipc_test_cross_process_lock_child(const cbm_daemon_ipc_endpoint_t *endpoint,
                                             int command_fd, int result_fd) {
    for (int attempt = 0; attempt < 2; attempt++) {
        uint8_t command = 0;
        if (!ipc_test_fd_read_all(command_fd, &command, sizeof(command)) ||
            command != (uint8_t)(attempt + 1)) {
            return 10 + attempt;
        }
        cbm_daemon_ipc_startup_lock_t *lock = NULL;
        int result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &lock);
        cbm_daemon_ipc_startup_lock_release(&lock);
        if (!ipc_test_fd_write_all(result_fd, &result, sizeof(result))) {
            return 20 + attempt;
        }
    }
    return 0;
}

TEST(daemon_ipc_posix_startup_lock_is_cross_process) {
    static const char key[] = "6666777788889999";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *parent_lock = NULL;
    int commands[2] = {-1, -1};
    int results[2] = {-1, -1};
    pid_t child = -1;
    int child_status = -1;
    int parent_result = -1;
    int child_while_held = -1;
    int child_after_release = -1;
    bool pipes_ok = false;
    bool first_exchange_ok = false;
    bool second_exchange_ok = false;

    bool parent_ok = ipc_test_parent_new(parent, "process-lock");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        if (pipe(commands) == 0) {
            if (pipe(results) == 0) {
                pipes_ok = true;
            } else {
                (void)close(commands[0]);
                (void)close(commands[1]);
                commands[0] = commands[1] = -1;
            }
        }
    }
    if (pipes_ok) {
        child = fork();
    }
    if (child == 0) {
        (void)close(commands[1]);
        (void)close(results[0]);
        int child_result = ipc_test_cross_process_lock_child(endpoint, commands[0], results[1]);
        (void)close(commands[0]);
        (void)close(results[1]);
        _exit(child_result);
    }
    if (child > 0) {
        (void)close(commands[0]);
        commands[0] = -1;
        (void)close(results[1]);
        results[1] = -1;

        parent_result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &parent_lock);
        uint8_t command = 1;
        first_exchange_ok =
            ipc_test_fd_write_all(commands[1], &command, sizeof(command)) &&
            ipc_test_fd_read_all(results[0], &child_while_held, sizeof(child_while_held));
        cbm_daemon_ipc_startup_lock_release(&parent_lock);
        parent_lock = NULL;

        if (first_exchange_ok) {
            command = 2;
            second_exchange_ok =
                ipc_test_fd_write_all(commands[1], &command, sizeof(command)) &&
                ipc_test_fd_read_all(results[0], &child_after_release, sizeof(child_after_release));
        }
        (void)close(commands[1]);
        commands[1] = -1;
        (void)close(results[0]);
        results[0] = -1;
        while (waitpid(child, &child_status, 0) < 0 && errno == EINTR) {}
    }

    cbm_daemon_ipc_startup_lock_release(&parent_lock);
    for (size_t i = 0; i < 2; i++) {
        if (commands[i] >= 0) {
            (void)close(commands[i]);
        }
        if (results[i] >= 0) {
            (void)close(results[i]);
        }
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(pipes_ok);
    ASSERT_GT(child, 0);
    ASSERT_EQ(parent_result, 1);
    ASSERT_TRUE(first_exchange_ok);
    ASSERT_EQ(child_while_held, 0);
    ASSERT_TRUE(second_exchange_ok);
    ASSERT_EQ(child_after_release, 1);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    PASS();
}

TEST(daemon_ipc_posix_lifetime_reservation_rejects_fork_inheritance) {
    static const char key[] = "6a6a7b7b8c8c9d9d";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *reservation = NULL;
    cbm_daemon_ipc_listener_t *parent_listener = NULL;
    int result_pipe[2] = {-1, -1};
    pid_t child = -1;
    uint8_t child_result = 0;
    int child_status = -1;
    int acquired = -1;
    int held_after_child = -1;
    bool parent_transfer_consumed = false;
    int free_after_close = -1;

    if (ipc_test_parent_new(parent, "forked-lifetime")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        acquired = cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &reservation);
    }
    if (reservation && pipe(result_pipe) == 0) {
        child = fork();
    }
    if (child == 0) {
        (void)close(result_pipe[0]);
        cbm_daemon_ipc_lifetime_reservation_t *inherited = reservation;
        cbm_daemon_ipc_listener_t *unexpected =
            cbm_daemon_ipc_listen_reserved(endpoint, &inherited);
        child_result = unexpected == NULL && inherited == reservation ? 1 : 0;
        cbm_daemon_ipc_listener_close(unexpected);
        cbm_daemon_ipc_lifetime_reservation_release(inherited);
        bool reported = ipc_test_fd_write_all(result_pipe[1], &child_result, sizeof(child_result));
        (void)close(result_pipe[1]);
        _exit(reported && child_result == 1 ? 0 : 1);
    }
    if (child > 0) {
        (void)close(result_pipe[1]);
        result_pipe[1] = -1;
        bool received = ipc_test_fd_read_all(result_pipe[0], &child_result, sizeof(child_result));
        (void)close(result_pipe[0]);
        result_pipe[0] = -1;
        if (!received) {
            child_result = 0;
        }
        while (waitpid(child, &child_status, 0) < 0 && errno == EINTR) {}
    }
    if (child_result == 1 && WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0) {
        held_after_child = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        parent_listener = cbm_daemon_ipc_listen_reserved(endpoint, &reservation);
        parent_transfer_consumed = parent_listener != NULL && reservation == NULL;
    }
    cbm_daemon_ipc_listener_close(parent_listener);
    cbm_daemon_ipc_lifetime_reservation_release(reservation);
    if (endpoint) {
        free_after_close = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
    }
    for (size_t index = 0; index < 2; index++) {
        if (result_pipe[index] >= 0) {
            (void)close(result_pipe[index]);
        }
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_EQ(acquired, 1);
    ASSERT_GT(child, 0);
    ASSERT_EQ(child_result, 1);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    ASSERT_EQ(held_after_child, 1);
    ASSERT_TRUE(parent_transfer_consumed);
    ASSERT_EQ(free_after_close, 0);
    PASS();
}

TEST(daemon_ipc_posix_child_participant_handoff_retains_legacy_bridge) {
#ifdef _WIN32
    PASS();
#else
    static const char key[] = "6b6b7c7c8d8d9e9e";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char legacy_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    int parent_to_child[2] = {-1, -1};
    int child_to_parent[2] = {-1, -1};
    pid_t child = -1;
    int child_status = -1;
    uint8_t joined = 0;
    uint8_t released = 0;
    int legacy_fd = -1;
    int blocked_while_child = -1;
    int acquired_after_child = -1;

    bool parent_ok = ipc_test_parent_new(parent, "participant-handoff");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        int path_length =
            snprintf(legacy_path, sizeof(legacy_path), "%s/cbm-%s.lock", runtime_dir, key);
        parent_ok = path_length > 0 && path_length < (int)sizeof(legacy_path);
    }
    int startup_result =
        parent_ok ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    bool prepared = startup_result == 1 && cbm_daemon_ipc_startup_lock_prepare_handoff(startup);
    bool pipes_ok = prepared && pipe(parent_to_child) == 0 && pipe(child_to_parent) == 0;
    if (pipes_ok) {
        child = fork();
    }
    if (child == 0) {
        (void)close(parent_to_child[1]);
        (void)close(child_to_parent[0]);
        cbm_daemon_ipc_participant_guard_t *guard = NULL;
        int join_result = cbm_daemon_ipc_participant_guard_try_join(endpoint, &guard);
        uint8_t join_byte = join_result == 1 && guard ? 1 : 0;
        bool reported = ipc_test_fd_write_all(child_to_parent[1], &join_byte, sizeof(join_byte));
        uint8_t command = 0;
        bool commanded =
            reported && ipc_test_fd_read_all(parent_to_child[0], &command, sizeof(command));
        bool guard_released = cbm_daemon_ipc_participant_guard_release(&guard);
        uint8_t release_byte = commanded && command == 1 && guard_released && !guard ? 1 : 0;
        bool release_reported =
            ipc_test_fd_write_all(child_to_parent[1], &release_byte, sizeof(release_byte));
        (void)close(parent_to_child[0]);
        (void)close(child_to_parent[1]);
        _exit(join_byte == 1 && release_reported && release_byte == 1 ? 0 : 1);
    }
    if (child > 0) {
        (void)close(parent_to_child[0]);
        parent_to_child[0] = -1;
        (void)close(child_to_parent[1]);
        child_to_parent[1] = -1;
        bool joined_read = ipc_test_fd_read_all(child_to_parent[0], &joined, sizeof(joined));
        cbm_daemon_ipc_startup_lock_release(&startup);
        startup = NULL;
        legacy_fd = open(legacy_path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
        if (joined_read && joined == 1 && legacy_fd >= 0) {
            blocked_while_child = flock(legacy_fd, LOCK_EX | LOCK_NB) == 0
                                      ? 0
                                      : (errno == EWOULDBLOCK || errno == EAGAIN ? 1 : -1);
        }
        uint8_t command = 1;
        bool commanded = ipc_test_fd_write_all(parent_to_child[1], &command, sizeof(command));
        bool released_read =
            commanded && ipc_test_fd_read_all(child_to_parent[0], &released, sizeof(released));
        (void)close(parent_to_child[1]);
        parent_to_child[1] = -1;
        (void)close(child_to_parent[0]);
        child_to_parent[0] = -1;
        while (waitpid(child, &child_status, 0) < 0 && errno == EINTR) {}
        if (released_read && released == 1 && WIFEXITED(child_status) && legacy_fd >= 0) {
            acquired_after_child = flock(legacy_fd, LOCK_EX | LOCK_NB) == 0 ? 1 : 0;
        }
    }

    cbm_daemon_ipc_startup_lock_release(&startup);
    if (legacy_fd >= 0) {
        (void)flock(legacy_fd, LOCK_UN);
        (void)close(legacy_fd);
    }
    for (size_t index = 0; index < 2; index++) {
        if (parent_to_child[index] >= 0) {
            (void)close(parent_to_child[index]);
        }
        if (child_to_parent[index] >= 0) {
            (void)close(child_to_parent[index]);
        }
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(startup_result, 1);
    ASSERT_TRUE(prepared);
    ASSERT_TRUE(pipes_ok);
    ASSERT_GT(child, 0);
    ASSERT_EQ(joined, 1);
    ASSERT_EQ(blocked_while_child, 1);
    ASSERT_EQ(released, 1);
    ASSERT_EQ(acquired_after_child, 1);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    PASS();
#endif
}

TEST(daemon_ipc_posix_publication_boundaries_recover_from_crash) {
#ifdef _WIN32
    PASS();
#else
    static const char key[] = "9f8e7d6c5b4a3210";
    static cbm_daemon_ipc_posix_publication_stage_t stages[] = {
        CBM_DAEMON_IPC_POSIX_PUBLICATION_ANCHOR_DURABLE,
        CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_DURABLE,
        CBM_DAEMON_IPC_POSIX_PUBLICATION_STABLE_DURABLE,
        CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_DURABLE,
        CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_REMOVED,
    };
    bool stage_ok[sizeof(stages) / sizeof(stages[0])] = {0};

    for (size_t index = 0; index < sizeof(stages) / sizeof(stages[0]); index++) {
        char parent[TEST_PATH_CAP] = {0};
        char runtime_dir[TEST_PATH_CAP] = {0};
        char socket_path[TEST_PATH_CAP] = {0};
        char anchor_path[TEST_PATH_CAP] = {0};
        char identity_path[TEST_PATH_CAP] = {0};
        char pending_path[TEST_PATH_CAP] = {0};
        cbm_daemon_ipc_endpoint_t *endpoint = NULL;
        cbm_daemon_ipc_startup_lock_t *startup = NULL;
        struct stat socket_status = {0};
        struct stat anchor_status = {0};
        struct stat record_status = {0};
        pid_t child = -1;
        int child_status = -1;

        bool parent_ok = ipc_test_parent_new(parent, "publish-boundary");
        if (parent_ok) {
            endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
        }
        if (endpoint) {
            ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
            ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
        }
        bool paths_ok = endpoint && ipc_test_socket_anchor_path(anchor_path, runtime_dir, key) &&
                        ipc_test_socket_identity_path(identity_path, socket_path) &&
                        ipc_test_socket_pending_path(pending_path, socket_path);
        if (paths_ok) {
            cbm_daemon_ipc_posix_publication_hook_set_for_test(ipc_test_publication_crash_hook,
                                                               &stages[index]);
            child = fork();
        }
        if (child == 0) {
            cbm_daemon_ipc_listener_t *listener = cbm_daemon_ipc_listen(endpoint);
            _exit(listener ? 90 : 91);
        }
        cbm_daemon_ipc_posix_publication_hook_set_for_test(NULL, NULL);
        if (child > 0) {
            while (waitpid(child, &child_status, 0) < 0 && errno == EINTR) {}
        }

        bool crashed_at_boundary = child > 0 && WIFEXITED(child_status) &&
                                   WEXITSTATUS(child_status) == 40 + (int)stages[index];
        bool anchor_present =
            lstat(anchor_path, &anchor_status) == 0 && S_ISSOCK(anchor_status.st_mode);
        bool stable_present =
            lstat(socket_path, &socket_status) == 0 && S_ISSOCK(socket_status.st_mode);
        bool pending_present =
            lstat(pending_path, &record_status) == 0 && S_ISREG(record_status.st_mode);
        bool marker_present =
            lstat(identity_path, &record_status) == 0 && S_ISREG(record_status.st_mode);
        bool linked_shape = stages[index] < CBM_DAEMON_IPC_POSIX_PUBLICATION_STABLE_DURABLE
                                ? anchor_present && anchor_status.st_nlink == 1 && !stable_present
                                : anchor_present && stable_present && anchor_status.st_nlink == 2 &&
                                      socket_status.st_nlink == 2 &&
                                      anchor_status.st_dev == socket_status.st_dev &&
                                      anchor_status.st_ino == socket_status.st_ino;
        bool expected_records =
            pending_present == (stages[index] >= CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_DURABLE &&
                                stages[index] < CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_REMOVED) &&
            marker_present == (stages[index] >= CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_DURABLE);

        int startup_result =
            crashed_at_boundary ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
        int cleanup_result =
            startup_result == 1 ? cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup) : -1;
        cbm_daemon_ipc_startup_lock_release(&startup);
        errno = 0;
        bool stable_removed = lstat(socket_path, &record_status) != 0 && errno == ENOENT;
        errno = 0;
        bool anchor_removed = lstat(anchor_path, &record_status) != 0 && errno == ENOENT;
        errno = 0;
        bool pending_removed = lstat(pending_path, &record_status) != 0 && errno == ENOENT;
        errno = 0;
        bool marker_removed = lstat(identity_path, &record_status) != 0 && errno == ENOENT;
        stage_ok[index] = parent_ok && paths_ok && crashed_at_boundary && linked_shape &&
                          expected_records && startup_result == 1 && cleanup_result == 1 &&
                          stable_removed && anchor_removed && pending_removed && marker_removed;

        cbm_daemon_ipc_endpoint_free(endpoint);
        ipc_test_remove_tree(runtime_dir, parent);
    }

    for (size_t index = 0; index < sizeof(stage_ok) / sizeof(stage_ok[0]); index++) {
        ASSERT_TRUE(stage_ok[index]);
    }
    PASS();
#endif
}

TEST(daemon_ipc_posix_record_publication_windows_recover_from_crash) {
#ifdef _WIN32
    PASS();
#else
    static const char key[] = "1a2b3c4d5e6f7081";
    static const struct {
        cbm_daemon_ipc_posix_publication_stage_t stage;
        bool marker;
        bool linked;
    } cases[] = {
        {CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_TEMP_SYNCED, false, false},
        {CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_RECORD_LINKED, false, true},
        {CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_TEMP_SYNCED, true, false},
        {CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_RECORD_LINKED, true, true},
    };
    bool case_ok[sizeof(cases) / sizeof(cases[0])] = {0};

    for (size_t index = 0; index < sizeof(cases) / sizeof(cases[0]); index++) {
        char parent[TEST_PATH_CAP] = {0};
        char runtime_dir[TEST_PATH_CAP] = {0};
        char socket_path[TEST_PATH_CAP] = {0};
        char anchor_path[TEST_PATH_CAP] = {0};
        char identity_path[TEST_PATH_CAP] = {0};
        char pending_path[TEST_PATH_CAP] = {0};
        char temp_path[TEST_PATH_CAP] = {0};
        cbm_daemon_ipc_endpoint_t *endpoint = NULL;
        cbm_daemon_ipc_startup_lock_t *startup = NULL;
        struct stat socket_status = {0};
        struct stat anchor_status = {0};
        struct stat pending_status = {0};
        struct stat marker_status = {0};
        struct stat record_status = {0};
        struct stat temp_status = {0};
        struct stat absent_status = {0};
        pid_t child = -1;
        int child_status = -1;

        bool parent_ok = ipc_test_parent_new(parent, "record-window");
        if (parent_ok) {
            endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
        }
        if (endpoint) {
            ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
            ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
        }
        bool paths_ok = endpoint && ipc_test_socket_anchor_path(anchor_path, runtime_dir, key) &&
                        ipc_test_socket_identity_path(identity_path, socket_path) &&
                        ipc_test_socket_pending_path(pending_path, socket_path);
        if (paths_ok) {
            cbm_daemon_ipc_posix_publication_hook_set_for_test(ipc_test_publication_crash_hook,
                                                               (void *)&cases[index].stage);
            child = fork();
        }
        if (child == 0) {
            cbm_daemon_ipc_listener_t *listener = cbm_daemon_ipc_listen(endpoint);
            _exit(listener ? 90 : 91);
        }
        cbm_daemon_ipc_posix_publication_hook_set_for_test(NULL, NULL);
        if (child > 0) {
            while (waitpid(child, &child_status, 0) < 0 && errno == EINTR) {}
        }

        bool crashed = child > 0 && WIFEXITED(child_status) &&
                       WEXITSTATUS(child_status) == 40 + (int)cases[index].stage;
        const char *record_path = cases[index].marker ? identity_path : pending_path;
        bool temp_found = crashed &&
                          ipc_test_record_temp_path(temp_path, runtime_dir, record_path) &&
                          lstat(temp_path, &temp_status) == 0 && S_ISREG(temp_status.st_mode) &&
                          temp_status.st_uid == geteuid() && (temp_status.st_mode & 0777) == 0600 &&
                          temp_status.st_nlink == (cases[index].linked ? 2 : 1);
        errno = 0;
        bool record_shape = cases[index].linked
                                ? lstat(record_path, &record_status) == 0 &&
                                      S_ISREG(record_status.st_mode) &&
                                      record_status.st_nlink == 2 && temp_found &&
                                      record_status.st_dev == temp_status.st_dev &&
                                      record_status.st_ino == temp_status.st_ino
                                : lstat(record_path, &absent_status) != 0 && errno == ENOENT;
        bool socket_shape =
            cases[index].marker
                ? lstat(socket_path, &socket_status) == 0 && S_ISSOCK(socket_status.st_mode) &&
                      socket_status.st_nlink == 2 && lstat(anchor_path, &anchor_status) == 0 &&
                      S_ISSOCK(anchor_status.st_mode) && anchor_status.st_nlink == 2 &&
                      socket_status.st_dev == anchor_status.st_dev &&
                      socket_status.st_ino == anchor_status.st_ino
                : lstat(anchor_path, &anchor_status) == 0 && S_ISSOCK(anchor_status.st_mode) &&
                      anchor_status.st_nlink == 1 && lstat(socket_path, &absent_status) != 0 &&
                      errno == ENOENT;
        errno = 0;
        bool preceding_record_shape =
            cases[index].marker
                ? lstat(pending_path, &pending_status) == 0 && S_ISREG(pending_status.st_mode) &&
                      pending_status.st_nlink == 1
                : lstat(identity_path, &marker_status) != 0 && errno == ENOENT;

        int startup_result =
            crashed ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
        int cleanup_result =
            startup_result == 1 ? cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup) : -1;
        cbm_daemon_ipc_startup_lock_release(&startup);
        errno = 0;
        bool stable_removed = lstat(socket_path, &absent_status) != 0 && errno == ENOENT;
        errno = 0;
        bool anchor_removed = lstat(anchor_path, &absent_status) != 0 && errno == ENOENT;
        errno = 0;
        bool pending_removed = lstat(pending_path, &absent_status) != 0 && errno == ENOENT;
        errno = 0;
        bool marker_removed = lstat(identity_path, &absent_status) != 0 && errno == ENOENT;
        errno = 0;
        bool temp_removed =
            temp_path[0] != '\0' && lstat(temp_path, &absent_status) != 0 && errno == ENOENT;
        case_ok[index] = parent_ok && paths_ok && crashed && temp_found && record_shape &&
                         socket_shape && preceding_record_shape && startup_result == 1 &&
                         cleanup_result == 1 && stable_removed && anchor_removed &&
                         pending_removed && marker_removed && temp_removed;

        cbm_daemon_ipc_endpoint_free(endpoint);
        ipc_test_remove_tree(runtime_dir, parent);
    }

    for (size_t index = 0; index < sizeof(case_ok) / sizeof(case_ok[0]); index++) {
        ASSERT_TRUE(case_ok[index]);
    }
    PASS();
#endif
}

TEST(daemon_ipc_posix_unknown_record_temp_pair_is_preserved) {
#ifdef _WIN32
    PASS();
#else
    static const char key[] = "2b3c4d5e6f708192";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char socket_path[TEST_PATH_CAP] = {0};
    char pending_path[TEST_PATH_CAP] = {0};
    char temp_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    struct stat pending_before = {0};
    struct stat temp_before = {0};
    struct stat pending_after = {0};
    struct stat temp_after = {0};

    bool parent_ok = ipc_test_parent_new(parent, "unknown-record-temp");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
    }
    bool pending_path_ok = endpoint && ipc_test_socket_pending_path(pending_path, socket_path);
    int temp_written =
        pending_path_ok ? snprintf(temp_path, sizeof(temp_path), "%s.tmp", pending_path) : -1;
    bool paths_ok = pending_path_ok && temp_written > 0 && temp_written < (int)sizeof(temp_path);
    bool pair_created = paths_ok && ipc_test_write_byte(temp_path, 0x5a) &&
                        chmod(temp_path, 0600) == 0 && link(temp_path, pending_path) == 0 &&
                        lstat(temp_path, &temp_before) == 0 &&
                        lstat(pending_path, &pending_before) == 0 && S_ISREG(temp_before.st_mode) &&
                        temp_before.st_nlink == 2 && pending_before.st_dev == temp_before.st_dev &&
                        pending_before.st_ino == temp_before.st_ino;
    int startup_result =
        pair_created ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int cleanup_result =
        startup_result == 1 ? cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup) : -1;
    cbm_daemon_ipc_startup_lock_release(&startup);
    bool pair_preserved =
        lstat(temp_path, &temp_after) == 0 && lstat(pending_path, &pending_after) == 0 &&
        temp_after.st_dev == temp_before.st_dev && temp_after.st_ino == temp_before.st_ino &&
        temp_after.st_nlink == 2 && pending_after.st_nlink == 2 &&
        pending_after.st_dev == pending_before.st_dev &&
        pending_after.st_ino == pending_before.st_ino && temp_after.st_size == 1 &&
        pending_after.st_size == 1;

    (void)unlink(pending_path);
    (void)unlink(temp_path);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(pair_created);
    ASSERT_EQ(startup_result, 1);
    ASSERT_EQ(cleanup_result, 0);
    ASSERT_TRUE(pair_preserved);
    PASS();
#endif
}

TEST(daemon_ipc_posix_recovery_preserves_replaced_stable_socket) {
#ifdef _WIN32
    PASS();
#else
    static const char key[] = "8e7d6c5b4a392817";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char socket_path[TEST_PATH_CAP] = {0};
    char anchor_path[TEST_PATH_CAP] = {0};
    char identity_path[TEST_PATH_CAP] = {0};
    char pending_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_posix_publication_stage_t crash_stage =
        CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_REMOVED;
    struct stat replacement_before = {0};
    struct stat replacement_after = {0};
    struct stat status = {0};
    struct sockaddr_un address;
    socklen_t address_length = 0;
    pid_t child = -1;
    int child_status = -1;
    int replacement = -1;

    bool parent_ok = ipc_test_parent_new(parent, "stable-replacement");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
    }
    bool paths_ok = endpoint && ipc_test_socket_anchor_path(anchor_path, runtime_dir, key) &&
                    ipc_test_socket_identity_path(identity_path, socket_path) &&
                    ipc_test_socket_pending_path(pending_path, socket_path) &&
                    ipc_test_unix_address_set(&address, socket_path, &address_length);
    if (paths_ok) {
        cbm_daemon_ipc_posix_publication_hook_set_for_test(ipc_test_publication_crash_hook,
                                                           &crash_stage);
        child = fork();
    }
    if (child == 0) {
        cbm_daemon_ipc_listener_t *listener = cbm_daemon_ipc_listen(endpoint);
        _exit(listener ? 90 : 91);
    }
    cbm_daemon_ipc_posix_publication_hook_set_for_test(NULL, NULL);
    if (child > 0) {
        while (waitpid(child, &child_status, 0) < 0 && errno == EINTR) {}
    }
    bool crashed_committed =
        child > 0 && WIFEXITED(child_status) && WEXITSTATUS(child_status) == 40 + (int)crash_stage;
    bool stable_unlinked = crashed_committed && unlink(socket_path) == 0;
    if (stable_unlinked) {
        replacement = socket(AF_UNIX, SOCK_STREAM, 0);
    }
    bool replacement_ready =
        replacement >= 0 &&
        bind(replacement, (const struct sockaddr *)&address, address_length) == 0 &&
        chmod(socket_path, 0600) == 0 && listen(replacement, 1) == 0 &&
        lstat(socket_path, &replacement_before) == 0 && S_ISSOCK(replacement_before.st_mode);
    int startup_result =
        replacement_ready ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int cleanup_result =
        startup_result == 1 ? cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup) : -1;
    cbm_daemon_ipc_startup_lock_release(&startup);
    bool replacement_preserved = lstat(socket_path, &replacement_after) == 0 &&
                                 S_ISSOCK(replacement_after.st_mode) &&
                                 replacement_after.st_dev == replacement_before.st_dev &&
                                 replacement_after.st_ino == replacement_before.st_ino;
    errno = 0;
    bool anchor_removed = lstat(anchor_path, &status) != 0 && errno == ENOENT;
    errno = 0;
    bool pending_removed = lstat(pending_path, &status) != 0 && errno == ENOENT;
    errno = 0;
    bool marker_removed = lstat(identity_path, &status) != 0 && errno == ENOENT;

    if (replacement >= 0) {
        (void)close(replacement);
    }
    (void)unlink(socket_path);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(crashed_committed);
    ASSERT_TRUE(stable_unlinked);
    ASSERT_TRUE(replacement_ready);
    ASSERT_EQ(startup_result, 1);
    ASSERT_EQ(cleanup_result, 0);
    ASSERT_TRUE(replacement_preserved);
    ASSERT_TRUE(anchor_removed);
    ASSERT_TRUE(pending_removed);
    ASSERT_TRUE(marker_removed);
    PASS();
#endif
}

TEST(daemon_ipc_posix_pending_without_anchor_never_deletes_stable) {
#ifdef _WIN32
    PASS();
#else
    static const char key[] = "7d6c5b4a39281706";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char socket_path[TEST_PATH_CAP] = {0};
    char anchor_path[TEST_PATH_CAP] = {0};
    char pending_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_posix_publication_stage_t crash_stage =
        CBM_DAEMON_IPC_POSIX_PUBLICATION_STABLE_DURABLE;
    struct stat stable_before = {0};
    struct stat stable_after = {0};
    struct stat status = {0};
    pid_t child = -1;
    int child_status = -1;

    bool parent_ok = ipc_test_parent_new(parent, "pending-no-anchor");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
    }
    bool paths_ok = endpoint && ipc_test_socket_anchor_path(anchor_path, runtime_dir, key) &&
                    ipc_test_socket_pending_path(pending_path, socket_path);
    if (paths_ok) {
        cbm_daemon_ipc_posix_publication_hook_set_for_test(ipc_test_publication_crash_hook,
                                                           &crash_stage);
        child = fork();
    }
    if (child == 0) {
        cbm_daemon_ipc_listener_t *listener = cbm_daemon_ipc_listen(endpoint);
        _exit(listener ? 90 : 91);
    }
    cbm_daemon_ipc_posix_publication_hook_set_for_test(NULL, NULL);
    if (child > 0) {
        while (waitpid(child, &child_status, 0) < 0 && errno == EINTR) {}
    }
    bool crashed_before_marker =
        child > 0 && WIFEXITED(child_status) && WEXITSTATUS(child_status) == 40 + (int)crash_stage;
    bool anchor_removed = crashed_before_marker && lstat(socket_path, &stable_before) == 0 &&
                          S_ISSOCK(stable_before.st_mode) && unlink(anchor_path) == 0;
    int startup_result =
        anchor_removed ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int cleanup_result =
        startup_result == 1 ? cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup) : -1;
    cbm_daemon_ipc_startup_lock_release(&startup);
    bool stable_preserved =
        lstat(socket_path, &stable_after) == 0 && S_ISSOCK(stable_after.st_mode) &&
        stable_after.st_dev == stable_before.st_dev && stable_after.st_ino == stable_before.st_ino;
    errno = 0;
    bool pending_removed = lstat(pending_path, &status) != 0 && errno == ENOENT;

    (void)unlink(socket_path);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(crashed_before_marker);
    ASSERT_TRUE(anchor_removed);
    ASSERT_EQ(startup_result, 1);
    ASSERT_EQ(cleanup_result, 0);
    ASSERT_TRUE(stable_preserved);
    ASSERT_TRUE(pending_removed);
    PASS();
#endif
}

TEST(daemon_ipc_posix_current_generation_crash_cleanup_requires_startup_lock) {
    static const char key[] = "a1b2c3d4e5f60718";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char socket_path[TEST_PATH_CAP] = {0};
    char anchor_path[TEST_PATH_CAP] = {0};
    char identity_path[TEST_PATH_CAP] = {0};
    char pending_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_endpoint_t *wrong_endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_startup_lock_t *wrong_startup = NULL;
    int ready_pipe[2] = {-1, -1};
    pid_t child = -1;
    int child_status = -1;
    uint8_t ready = 0;
    struct stat status = {0};
    bool paths_ok = false;
    bool child_ready = false;
    bool artifacts_survived_crash = false;
    int lifetime_after_crash = -1;
    int cleanup_without_lock = -2;
    int wrong_startup_result = -1;
    int cleanup_with_wrong_lock = -2;
    int startup_result = -1;
    int cleanup_result = -1;
    bool artifacts_removed = false;

    if (ipc_test_parent_new(parent, "crash-identity")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
        paths_ok = ipc_test_socket_anchor_path(anchor_path, runtime_dir, key) &&
                   ipc_test_socket_identity_path(identity_path, socket_path) &&
                   ipc_test_socket_pending_path(pending_path, socket_path) && pipe(ready_pipe) == 0;
    }
    if (paths_ok) {
        child = fork();
    }
    if (child == 0) {
        (void)close(ready_pipe[0]);
        cbm_daemon_ipc_listener_t *listener = cbm_daemon_ipc_listen(endpoint);
        uint8_t result = listener ? 'R' : 'E';
        bool reported = ipc_test_fd_write_all(ready_pipe[1], &result, sizeof(result));
        (void)close(ready_pipe[1]);
        /* Deliberately bypass listener_close: the kernel releases descriptors
         * and locks, while the current-generation socket identity remains. */
        _exit(listener && reported ? 0 : 1);
    }
    if (child > 0) {
        (void)close(ready_pipe[1]);
        ready_pipe[1] = -1;
        child_ready = ipc_test_fd_read_all(ready_pipe[0], &ready, sizeof(ready)) && ready == 'R';
        (void)close(ready_pipe[0]);
        ready_pipe[0] = -1;
        while (waitpid(child, &child_status, 0) < 0 && errno == EINTR) {}
    }
    if (child_ready && WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0) {
        artifacts_survived_crash = lstat(socket_path, &status) == 0 && S_ISSOCK(status.st_mode) &&
                                   status.st_nlink == 2 && lstat(anchor_path, &status) == 0 &&
                                   S_ISSOCK(status.st_mode) && status.st_nlink == 2 &&
                                   lstat(identity_path, &status) == 0 && S_ISREG(status.st_mode);
        lifetime_after_crash = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        cleanup_without_lock = cbm_daemon_ipc_stale_generation_cleanup(endpoint, NULL);
        wrong_endpoint = cbm_daemon_ipc_endpoint_new("a1b2c3d4e5f60719", parent);
        wrong_startup_result =
            wrong_endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(wrong_endpoint, &wrong_startup)
                           : -1;
        cleanup_with_wrong_lock =
            wrong_startup ? cbm_daemon_ipc_stale_generation_cleanup(endpoint, wrong_startup) : -2;
        cbm_daemon_ipc_startup_lock_release(&wrong_startup);
        wrong_startup = NULL;
        startup_result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup);
    }
    if (startup) {
        cleanup_result = cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup);
        errno = 0;
        bool socket_removed = lstat(socket_path, &status) != 0 && errno == ENOENT;
        errno = 0;
        bool identity_removed = lstat(identity_path, &status) != 0 && errno == ENOENT;
        errno = 0;
        bool anchor_removed = lstat(anchor_path, &status) != 0 && errno == ENOENT;
        errno = 0;
        bool pending_removed = lstat(pending_path, &status) != 0 && errno == ENOENT;
        artifacts_removed = socket_removed && anchor_removed && identity_removed && pending_removed;
    }

    cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_startup_lock_release(&wrong_startup);
    for (size_t index = 0; index < 2; index++) {
        if (ready_pipe[index] >= 0) {
            (void)close(ready_pipe[index]);
        }
    }
    cbm_daemon_ipc_endpoint_free(wrong_endpoint);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_GT(child, 0);
    ASSERT_TRUE(child_ready);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    ASSERT_TRUE(artifacts_survived_crash);
    ASSERT_EQ(lifetime_after_crash, 0);
    ASSERT_EQ(cleanup_without_lock, -1);
    ASSERT_EQ(wrong_startup_result, 1);
    ASSERT_EQ(cleanup_with_wrong_lock, -1);
    ASSERT_EQ(startup_result, 1);
    ASSERT_EQ(cleanup_result, 1);
    ASSERT_TRUE(artifacts_removed);
    PASS();
}

TEST(daemon_ipc_posix_unknown_socket_without_identity_refuses_cleanup) {
    static const char key[] = "b1c2d3e4f5061728";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char socket_path[TEST_PATH_CAP] = {0};
    char identity_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_local_transition_t *transition = NULL;
    int raw_listener = -1;
    struct sockaddr_un address;
    socklen_t address_length = 0;
    struct stat before = {0};
    struct stat after = {0};
    bool paths_ok = false;
    bool unknown_created = false;
    int startup_result = -1;
    int cleanup_result = -1;
    int transition_result = -1;
    int transition_seal_result = -1;
    bool unknown_unchanged = false;
    bool identity_absent = false;

    if (ipc_test_parent_new(parent, "unknown-socket")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
        paths_ok = ipc_test_socket_identity_path(identity_path, socket_path) &&
                   ipc_test_unix_address_set(&address, socket_path, &address_length);
    }
    if (paths_ok) {
        raw_listener = socket(AF_UNIX, SOCK_STREAM, 0);
    }
    if (raw_listener >= 0) {
        unknown_created =
            bind(raw_listener, (const struct sockaddr *)&address, address_length) == 0 &&
            chmod(socket_path, 0600) == 0 && listen(raw_listener, 1) == 0 &&
            lstat(socket_path, &before) == 0 && S_ISSOCK(before.st_mode) &&
            before.st_uid == geteuid() && (before.st_mode & 0777) == 0600;
        (void)close(raw_listener);
        raw_listener = -1;
    }
    if (unknown_created) {
        startup_result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup);
    }
    if (startup) {
        cleanup_result = cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup);
        unknown_unchanged = lstat(socket_path, &after) == 0 && S_ISSOCK(after.st_mode) &&
                            after.st_dev == before.st_dev && after.st_ino == before.st_ino;
        errno = 0;
        identity_absent = lstat(identity_path, &after) != 0 && errno == ENOENT;
    }

    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;
    if (unknown_unchanged) {
        transition_result = cbm_daemon_ipc_local_transition_try_acquire(endpoint, &transition);
    }
    if (transition) {
        transition_seal_result = cbm_daemon_ipc_local_transition_seal_legacy(transition);
    }
    (void)cbm_daemon_ipc_local_transition_release(&transition);
    if (raw_listener >= 0) {
        (void)close(raw_listener);
    }
    (void)unlink(socket_path);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(unknown_created);
    ASSERT_EQ(startup_result, 1);
    ASSERT_EQ(cleanup_result, 0);
    ASSERT_EQ(transition_result, 1);
    ASSERT_EQ(transition_seal_result, 0);
    ASSERT_TRUE(unknown_unchanged);
    ASSERT_TRUE(identity_absent);
    PASS();
}

TEST(daemon_ipc_posix_active_listener_is_never_cleaned_under_queue_pressure) {
    static const char key[] = "c1d2e3f405162738";
    enum { CLIENT_CAP = 64 };
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char socket_path[TEST_PATH_CAP] = {0};
    char anchor_path[TEST_PATH_CAP] = {0};
    char identity_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_connection_t *clients[CLIENT_CAP] = {0};
    size_t client_count = 0;
    struct stat socket_before = {0};
    struct stat anchor_before = {0};
    struct stat identity_before = {0};
    struct stat socket_after = {0};
    struct stat anchor_after = {0};
    struct stat identity_after = {0};
    bool paths_ok = false;
    bool artifacts_before = false;
    int startup_result = -1;
    int cleanup_result = -1;
    bool artifacts_unchanged = false;
    int endpoint_after_cleanup = -1;

    if (ipc_test_parent_new(parent, "active-cleanup")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
        paths_ok = ipc_test_socket_anchor_path(anchor_path, runtime_dir, key) &&
                   ipc_test_socket_identity_path(identity_path, socket_path);
        listener = cbm_daemon_ipc_listen(endpoint);
    }
    if (listener) {
        artifacts_before =
            lstat(socket_path, &socket_before) == 0 && S_ISSOCK(socket_before.st_mode) &&
            lstat(anchor_path, &anchor_before) == 0 && S_ISSOCK(anchor_before.st_mode) &&
            anchor_before.st_dev == socket_before.st_dev &&
            anchor_before.st_ino == socket_before.st_ino &&
            lstat(identity_path, &identity_before) == 0 && S_ISREG(identity_before.st_mode);
    }
    /* Never accept: pressure the listen queue into the BSD case where a live
     * listener can reject further connects with ECONNREFUSED. Cleanup must
     * rely only on the retained lifetime reservation, never on connect(). */
    while (listener && client_count < CLIENT_CAP) {
        cbm_daemon_ipc_connection_t *client = cbm_daemon_ipc_connect(endpoint, 1);
        if (!client) {
            break;
        }
        clients[client_count++] = client;
    }
    if (listener) {
        startup_result = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup);
    }
    if (startup) {
        cleanup_result = cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup);
        artifacts_unchanged =
            lstat(socket_path, &socket_after) == 0 && S_ISSOCK(socket_after.st_mode) &&
            socket_after.st_dev == socket_before.st_dev &&
            socket_after.st_ino == socket_before.st_ino && lstat(anchor_path, &anchor_after) == 0 &&
            S_ISSOCK(anchor_after.st_mode) && anchor_after.st_dev == anchor_before.st_dev &&
            anchor_after.st_ino == anchor_before.st_ino &&
            lstat(identity_path, &identity_after) == 0 && S_ISREG(identity_after.st_mode) &&
            identity_after.st_dev == identity_before.st_dev &&
            identity_after.st_ino == identity_before.st_ino;
        endpoint_after_cleanup = cbm_daemon_ipc_endpoint_probe(endpoint, 1);
    }

    cbm_daemon_ipc_startup_lock_release(&startup);
    for (size_t index = 0; index < client_count; index++) {
        cbm_daemon_ipc_connection_close(clients[index]);
    }
    cbm_daemon_ipc_listener_close(listener);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(artifacts_before);
    ASSERT_TRUE(client_count > 0);
    ASSERT_EQ(startup_result, 1);
    ASSERT_EQ(cleanup_result, 0);
    ASSERT_TRUE(artifacts_unchanged);
    ASSERT_EQ(endpoint_after_cleanup, 1);
    PASS();
}

TEST(daemon_ipc_posix_partial_frame_timeout_poisons_connection) {
    static const char key[] = "777788889999aaaa";
    static const uint8_t payload[] = {'n', 'e', 'w'};
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    cbm_daemon_ipc_connection_t *server_connection = NULL;
    int raw_client = -1;
    uint8_t partial_header[CBM_DAEMON_FRAME_HEADER_SIZE] = {0};
    uint8_t full_header[CBM_DAEMON_FRAME_HEADER_SIZE] = {0};
    cbm_daemon_frame_t frame = {0};
    uint8_t *received_payload = NULL;
    bool raw_connected = false;
    bool partial_sent = false;
    bool full_frame_sent = false;
    int accept_result = -1;
    int partial_receive_result = -1;
    int reuse_result = 1;

    bool parent_ok = ipc_test_parent_new(parent, "partial-frame");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        listener = cbm_daemon_ipc_listen(endpoint);
    }
    if (listener) {
        raw_client = socket(AF_UNIX, SOCK_STREAM, 0);
    }
    if (raw_client >= 0) {
#ifdef SO_NOSIGPIPE
        int no_sigpipe = 1;
        (void)setsockopt(raw_client, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe));
#endif
        struct sockaddr_un address;
        socklen_t address_length = 0;
        if (ipc_test_unix_address_set(&address, cbm_daemon_ipc_endpoint_address(endpoint),
                                      &address_length)) {
            int connect_result;
            do {
                connect_result =
                    connect(raw_client, (const struct sockaddr *)&address, address_length);
            } while (connect_result != 0 && errno == EINTR);
            raw_connected = connect_result == 0;
        }
    }
    if (raw_connected) {
        accept_result = cbm_daemon_ipc_accept(listener, 500, &server_connection);
    }
    if (accept_result == 1 && server_connection &&
        cbm_daemon_frame_header_encode(partial_header, CBM_DAEMON_FRAME_REQUEST, 0, 0)) {
        partial_sent =
            ipc_test_socket_send_all(raw_client, partial_header, CBM_DAEMON_FRAME_HEADER_SIZE / 2);
    }
    if (partial_sent) {
        partial_receive_result =
            cbm_daemon_ipc_receive_frame(server_connection, 50, &frame, &received_payload);
    }
    free(received_payload);
    received_payload = NULL;
    memset(&frame, 0, sizeof(frame));
    if (partial_receive_result == 0 &&
        cbm_daemon_frame_header_encode(full_header, CBM_DAEMON_FRAME_REQUEST, 0x55aa,
                                       (uint32_t)sizeof(payload))) {
        full_frame_sent = ipc_test_socket_send_all(raw_client, full_header, sizeof(full_header)) &&
                          ipc_test_socket_send_all(raw_client, payload, sizeof(payload));
    }
    if (full_frame_sent) {
        reuse_result =
            cbm_daemon_ipc_receive_frame(server_connection, 500, &frame, &received_payload);
    }

    free(received_payload);
    if (raw_client >= 0) {
        (void)close(raw_client);
    }
    cbm_daemon_ipc_connection_close(server_connection);
    cbm_daemon_ipc_listener_close(listener);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(raw_connected);
    ASSERT_EQ(accept_result, 1);
    ASSERT_TRUE(partial_sent);
    ASSERT_EQ(partial_receive_result, 0);
    ASSERT_TRUE(full_frame_sent);
    ASSERT_EQ(reuse_result, -1); /* a timed-out partial frame permanently poisons the stream */
    PASS();
}

#ifdef __APPLE__
TEST(daemon_ipc_macos_clears_inherited_deny_acl_from_new_runtime) {
    static const char key[] = "aa11bb22cc33dd44";
    char parent[TEST_PATH_CAP] = {0};
    char control[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    struct stat parent_status = {0};
    struct stat control_status = {0};
    struct stat runtime_status = {0};
    bool parent_ok = ipc_test_parent_new(parent, "mac-acl-inherit");
    int acl_fixture = parent_ok ? ipc_test_macos_set_deny_acl(parent, true) : -1;
    if (acl_fixture == 0) {
        ipc_test_remove_flat_dir(parent);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }

    bool parent_mode_stayed_private = acl_fixture == 1 && lstat(parent, &parent_status) == 0 &&
                                      S_ISDIR(parent_status.st_mode) &&
                                      (parent_status.st_mode & 0777) == 0700;
    int control_written = parent_mode_stayed_private
                              ? snprintf(control, sizeof(control), "%s/inheritance-control", parent)
                              : -1;
    bool control_created =
        control_written > 0 && control_written < (int)sizeof(control) && mkdir(control, 0700) == 0;
    bool control_mode_stayed_private = control_created && lstat(control, &control_status) == 0 &&
                                       S_ISDIR(control_status.st_mode) &&
                                       (control_status.st_mode & 0777) == 0700;
    int control_acl_entries =
        control_created ? ipc_test_macos_extended_acl_entry_count(control) : -1;
    bool control_removed = !control_created || rmdir(control) == 0;

    /* A deny-only ancestor is safe to traverse, but its inherited deny entry
     * still must be stripped from CBM's final private directory.  The control
     * child proves that this filesystem propagated the ACL. */
    if (control_removed && control_acl_entries > 0) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    bool runtime_mode_stayed_private =
        runtime_dir[0] != '\0' && lstat(runtime_dir, &runtime_status) == 0 &&
        S_ISDIR(runtime_status.st_mode) && (runtime_status.st_mode & 0777) == 0700;
    int runtime_acl_entries =
        runtime_dir[0] != '\0' ? ipc_test_macos_extended_acl_entry_count(runtime_dir) : -1;

    bool endpoint_created = endpoint != NULL;
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(acl_fixture, 1);
    ASSERT_TRUE(parent_mode_stayed_private);
    ASSERT_TRUE(control_created);
    ASSERT_TRUE(control_mode_stayed_private);
    ASSERT_GT(control_acl_entries, 0);
    ASSERT_TRUE(control_removed);
    ASSERT_TRUE(endpoint_created);
    ASSERT_TRUE(runtime_mode_stayed_private);
    ASSERT_EQ(runtime_acl_entries, 0);
    PASS();
}

TEST(daemon_ipc_macos_rejects_allow_acl_on_ancestor_without_mutation) {
    static const char key[] = "bb22cc33dd44ee55";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    struct stat before = {0};
    struct stat after = {0};

    bool parent_ok = ipc_test_parent_new(parent, "mac-acl-ancestor-allow");
    int runtime_written = parent_ok
                              ? snprintf(runtime_dir, sizeof(runtime_dir), "%s/cbm-daemon-%lu",
                                         parent, (unsigned long)geteuid())
                              : -1;
    int acl_fixture = parent_ok ? ipc_test_macos_set_mutating_acl(parent, false) : -1;
    if (acl_fixture == 0) {
        ipc_test_remove_flat_dir(parent);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    int entries_before = acl_fixture == 1 ? ipc_test_macos_extended_acl_entry_count(parent) : -1;
    bool snapshot_before = entries_before > 0 && lstat(parent, &before) == 0;

    cbm_daemon_ipc_endpoint_t *endpoint =
        snapshot_before ? cbm_daemon_ipc_endpoint_new(key, parent) : NULL;

    int entries_after = ipc_test_macos_extended_acl_entry_count(parent);
    bool parent_unchanged = lstat(parent, &after) == 0 && before.st_dev == after.st_dev &&
                            before.st_ino == after.st_ino &&
                            (before.st_mode & 07777) == (after.st_mode & 07777) &&
                            entries_after == entries_before;
    errno = 0;
    bool runtime_absent = runtime_written > 0 && runtime_written < (int)sizeof(runtime_dir) &&
                          lstat(runtime_dir, &after) != 0 && errno == ENOENT;

    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_flat_dir(parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(acl_fixture, 1);
    ASSERT_TRUE(snapshot_before);
    ASSERT_NULL(endpoint);
    ASSERT_TRUE(parent_unchanged);
    ASSERT_TRUE(runtime_absent);
    PASS();
}

TEST(daemon_ipc_macos_repairs_or_rejects_existing_runtime_mutating_acl) {
    static const char key[] = "ee55aa66bb77cc88";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *seed = NULL;
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    struct stat runtime_status = {0};

    bool parent_ok = ipc_test_parent_new(parent, "mac-acl-existing");
    if (parent_ok) {
        seed = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (seed) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(seed));
    }
    bool runtime_seeded = runtime_dir[0] != '\0';
    cbm_daemon_ipc_endpoint_free(seed);
    seed = NULL;

    int acl_fixture = runtime_seeded ? ipc_test_macos_set_mutating_acl(runtime_dir, false) : -1;
    if (acl_fixture == 0) {
        ipc_test_remove_tree(runtime_dir, parent);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    bool unsafe_mode_stayed_private =
        acl_fixture == 1 && lstat(runtime_dir, &runtime_status) == 0 &&
        S_ISDIR(runtime_status.st_mode) && (runtime_status.st_mode & 0777) == 0700;
    int acl_entries_before =
        acl_fixture == 1 ? ipc_test_macos_extended_acl_entry_count(runtime_dir) : -1;

    if (acl_entries_before > 0) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    int startup_status =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int acl_entries_after = ipc_test_macos_extended_acl_entry_count(runtime_dir);

    /* A constructor rejection or a lock-use rejection is fail-closed.  If use
     * succeeds, the dedicated directory must already have had its ACL cleared
     * and revalidated.  Mode 0700 alone is deliberately insufficient. */
    bool rejected_before_use = endpoint == NULL || startup_status == -1;
    bool repaired_before_use = endpoint != NULL && startup_status == 1 && acl_entries_after == 0;

    cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(runtime_seeded);
    ASSERT_EQ(acl_fixture, 1);
    ASSERT_TRUE(unsafe_mode_stayed_private);
    ASSERT_GT(acl_entries_before, 0);
    ASSERT_TRUE(rejected_before_use || repaired_before_use);
    PASS();
}

TEST(daemon_ipc_macos_runtime_acl_injection_invalidates_existing_endpoint) {
    static const char key[] = "aa77bb88cc99dd00";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    struct stat runtime_status = {0};

    bool parent_ok = ipc_test_parent_new(parent, "mac-acl-injected");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int acl_fixture =
        runtime_dir[0] != '\0' ? ipc_test_macos_set_mutating_acl(runtime_dir, false) : -1;
    if (acl_fixture == 0) {
        cbm_daemon_ipc_endpoint_free(endpoint);
        ipc_test_remove_tree(runtime_dir, parent);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    bool mode_stayed_private = acl_fixture == 1 && lstat(runtime_dir, &runtime_status) == 0 &&
                               S_ISDIR(runtime_status.st_mode) &&
                               (runtime_status.st_mode & 07777) == 0700;
    int startup_status =
        mode_stayed_private ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : 0;

    cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_TRUE(runtime_dir[0] != '\0');
    ASSERT_EQ(acl_fixture, 1);
    ASSERT_TRUE(mode_stayed_private);
    ASSERT_EQ(startup_status, -1);
    ASSERT_NULL(startup);
    PASS();
}

TEST(daemon_ipc_macos_lock_acl_injection_invalidates_retained_startup) {
    static const char key[] = "ee11dd22cc33bb44";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char startup_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    struct stat startup_status = {0};

    bool parent_ok = ipc_test_parent_new(parent, "mac-acl-lock");
    if (parent_ok) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
    }
    int startup_result =
        endpoint ? cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) : -1;
    int path_written = startup_result == 1 ? snprintf(startup_path, sizeof(startup_path),
                                                      "%s/cbm-%s.startup-v2.lock", runtime_dir, key)
                                           : -1;
    bool path_ok = path_written > 0 && path_written < (int)sizeof(startup_path);
    int acl_fixture = path_ok ? ipc_test_macos_set_mutating_acl(startup_path, false) : -1;
    if (acl_fixture == 0) {
        cbm_daemon_ipc_startup_lock_release(&startup);
        cbm_daemon_ipc_endpoint_free(endpoint);
        ipc_test_remove_tree(runtime_dir, parent);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    bool mode_stayed_private = acl_fixture == 1 && lstat(startup_path, &startup_status) == 0 &&
                               S_ISREG(startup_status.st_mode) &&
                               (startup_status.st_mode & 07777) == 0600;
    bool prepared = mode_stayed_private && cbm_daemon_ipc_startup_lock_prepare_handoff(startup);

    cbm_daemon_ipc_startup_lock_release(&startup);
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(parent_ok);
    ASSERT_EQ(startup_result, 1);
    ASSERT_TRUE(path_ok);
    ASSERT_EQ(acl_fixture, 1);
    ASSERT_TRUE(mode_stayed_private);
    ASSERT_FALSE(prepared);
    PASS();
}
#endif

TEST(daemon_ipc_posix_runtime_and_socket_are_owner_only) {
    static const char key[] = "9999aaaabbbbcccc";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char socket_path[TEST_PATH_CAP] = {0};
    char anchor_path[TEST_PATH_CAP] = {0};
    char identity_path[TEST_PATH_CAP] = {0};
    char pending_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *listener = NULL;
    struct stat runtime_stat = {0};
    struct stat socket_stat = {0};
    struct stat anchor_stat = {0};
    struct stat identity_stat = {0};
    bool runtime_ok = false;
    bool socket_ok = false;
    bool anchor_ok = false;
    bool identity_ok = false;
    bool socket_removed = false;
    bool anchor_removed = false;
    bool pending_absent = false;
    bool identity_removed = false;

    if (ipc_test_parent_new(parent, "permissions")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
        (void)ipc_test_socket_anchor_path(anchor_path, runtime_dir, key);
        (void)ipc_test_socket_identity_path(identity_path, socket_path);
        (void)ipc_test_socket_pending_path(pending_path, socket_path);
        runtime_ok = lstat(runtime_dir, &runtime_stat) == 0 && S_ISDIR(runtime_stat.st_mode) &&
                     (runtime_stat.st_mode & 0777) == 0700 && runtime_stat.st_uid == geteuid();
        listener = cbm_daemon_ipc_listen(endpoint);
    }
    if (listener) {
        socket_ok = lstat(socket_path, &socket_stat) == 0 && S_ISSOCK(socket_stat.st_mode) &&
                    (socket_stat.st_mode & 0777) == 0600 && socket_stat.st_uid == geteuid() &&
                    socket_stat.st_nlink == 2;
        anchor_ok = lstat(anchor_path, &anchor_stat) == 0 && S_ISSOCK(anchor_stat.st_mode) &&
                    (anchor_stat.st_mode & 0777) == 0600 && anchor_stat.st_uid == geteuid() &&
                    anchor_stat.st_nlink == 2 && anchor_stat.st_dev == socket_stat.st_dev &&
                    anchor_stat.st_ino == socket_stat.st_ino;
        identity_ok = lstat(identity_path, &identity_stat) == 0 && S_ISREG(identity_stat.st_mode) &&
                      (identity_stat.st_mode & 0777) == 0600 && identity_stat.st_uid == geteuid() &&
                      identity_stat.st_nlink == 1;
        errno = 0;
        pending_absent = lstat(pending_path, &identity_stat) != 0 && errno == ENOENT;
    }
    cbm_daemon_ipc_listener_close(listener);
    if (socket_path[0] != '\0') {
        errno = 0;
        socket_removed = lstat(socket_path, &socket_stat) != 0 && errno == ENOENT;
    }
    if (identity_path[0] != '\0') {
        errno = 0;
        identity_removed = lstat(identity_path, &identity_stat) != 0 && errno == ENOENT;
    }
    if (anchor_path[0] != '\0') {
        errno = 0;
        anchor_removed = lstat(anchor_path, &anchor_stat) != 0 && errno == ENOENT;
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(runtime_ok);
    ASSERT_TRUE(socket_ok);
    ASSERT_TRUE(anchor_ok);
    ASSERT_TRUE(identity_ok);
    ASSERT_TRUE(pending_absent);
    ASSERT_TRUE(socket_removed);
    ASSERT_TRUE(anchor_removed);
    ASSERT_TRUE(identity_removed);
    PASS();
}

static bool ipc_test_write_byte(const char *path, unsigned char byte) {
    FILE *file = fopen(path, "wb");
    if (!file) {
        return false;
    }
    bool ok = fwrite(&byte, 1, 1, file) == 1;
    return fclose(file) == 0 && ok;
}

TEST(daemon_ipc_posix_private_log_creates_first_ever_cache_tree_safely) {
    char parent[TEST_PATH_CAP] = {0};
    char first[TEST_PATH_CAP] = {0};
    char cache[TEST_PATH_CAP] = {0};
    char logs[TEST_PATH_CAP] = {0};
    char log_path[TEST_PATH_CAP] = {0};
    FILE *log = NULL;
    struct stat status = {0};
    bool paths_ok = false;
    bool absent_before = false;
    bool opened = false;
    bool tree_private = false;
    bool file_private = false;

    if (ipc_test_parent_new(parent, "private-log-first-cache")) {
        int first_written = snprintf(first, sizeof(first), "%s/first", parent);
        int cache_written = snprintf(cache, sizeof(cache), "%s/cache", first);
        int logs_written = snprintf(logs, sizeof(logs), "%s/logs", cache);
        int log_written = snprintf(log_path, sizeof(log_path), "%s/daemon.log", logs);
        paths_ok = first_written > 0 && first_written < (int)sizeof(first) && cache_written > 0 &&
                   cache_written < (int)sizeof(cache) && logs_written > 0 &&
                   logs_written < (int)sizeof(logs) && log_written > 0 &&
                   log_written < (int)sizeof(log_path);
    }
    if (paths_ok) {
        errno = 0;
        absent_before = lstat(first, &status) != 0 && errno == ENOENT;
        log = cbm_daemon_ipc_private_log_open(logs, "daemon.log", 4096);
        opened = log != NULL;
    }
    if (log) {
        (void)fputs("first startup\n", log);
        (void)fclose(log);
        log = NULL;
    }
    if (opened) {
        struct stat first_status;
        struct stat cache_status;
        struct stat logs_status;
        tree_private = lstat(first, &first_status) == 0 && S_ISDIR(first_status.st_mode) &&
                       (first_status.st_mode & 0777) == 0700 && lstat(cache, &cache_status) == 0 &&
                       S_ISDIR(cache_status.st_mode) && (cache_status.st_mode & 0777) == 0700 &&
                       lstat(logs, &logs_status) == 0 && S_ISDIR(logs_status.st_mode) &&
                       (logs_status.st_mode & 0777) == 0700;
        file_private = lstat(log_path, &status) == 0 && S_ISREG(status.st_mode) &&
                       status.st_uid == geteuid() && (status.st_mode & 0777) == 0600;
    }

    (void)unlink(log_path);
    (void)rmdir(logs);
    (void)rmdir(cache);
    (void)rmdir(first);
    ipc_test_remove_flat_dir(parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(absent_before);
    ASSERT_TRUE(opened);
    ASSERT_TRUE(tree_private);
    ASSERT_TRUE(file_private);
    PASS();
}

TEST(daemon_ipc_posix_private_directory_hardens_existing_cache_root) {
    char parent[TEST_PATH_CAP] = {0};
    char cache[TEST_PATH_CAP] = {0};
    struct stat status = {0};
    bool paths_ok = false;
    bool deliberately_public = false;
    bool hardened = false;

    if (ipc_test_parent_new(parent, "private-cache-root")) {
        int cache_written = snprintf(cache, sizeof(cache), "%s/cache", parent);
        paths_ok =
            cache_written > 0 && cache_written < (int)sizeof(cache) && mkdir(cache, 0700) == 0;
    }
    if (paths_ok) {
        deliberately_public = chmod(cache, 0755) == 0;
    }
    if (deliberately_public && cbm_daemon_ipc_private_directory_secure(cache)) {
        hardened = lstat(cache, &status) == 0 && S_ISDIR(status.st_mode) &&
                   status.st_uid == geteuid() && (status.st_mode & 07777) == 0700;
    }

    (void)rmdir(cache);
    ipc_test_remove_flat_dir(parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(deliberately_public);
    ASSERT_TRUE(hardened);
    PASS();
}

TEST(daemon_ipc_posix_private_directory_rejects_world_writable_ancestor) {
    char parent[TEST_PATH_CAP] = {0};
    char unsafe[TEST_PATH_CAP] = {0};
    char cache[TEST_PATH_CAP] = {0};
    struct stat status = {0};
    bool paths_ok = false;
    bool unsafe_mode_set = false;
    bool rejected = false;
    bool ancestor_unchanged = false;
    bool cache_absent = false;

    if (ipc_test_parent_new(parent, "private-cache-unsafe-ancestor")) {
        int unsafe_written = snprintf(unsafe, sizeof(unsafe), "%s/unsafe", parent);
        int cache_written = snprintf(cache, sizeof(cache), "%s/cache", unsafe);
        paths_ok = unsafe_written > 0 && unsafe_written < (int)sizeof(unsafe) &&
                   cache_written > 0 && cache_written < (int)sizeof(cache) &&
                   mkdir(unsafe, 0700) == 0;
    }
    if (paths_ok) {
        unsafe_mode_set = chmod(unsafe, 0777) == 0;
    }
    if (unsafe_mode_set) {
        rejected = !cbm_daemon_ipc_private_directory_secure(cache);
        ancestor_unchanged = lstat(unsafe, &status) == 0 && S_ISDIR(status.st_mode) &&
                             (status.st_mode & 07777) == 0777;
        errno = 0;
        cache_absent = lstat(cache, &status) != 0 && errno == ENOENT;
    }

    (void)rmdir(cache);
    (void)chmod(unsafe, 0700);
    (void)rmdir(unsafe);
    ipc_test_remove_flat_dir(parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(unsafe_mode_set);
    ASSERT_TRUE(rejected);
    ASSERT_TRUE(ancestor_unchanged);
    ASSERT_TRUE(cache_absent);
    PASS();
}

TEST(daemon_ipc_posix_private_log_rejects_symlinks_and_is_owner_only) {
    char parent[TEST_PATH_CAP] = {0};
    char logs[TEST_PATH_CAP] = {0};
    char log_path[TEST_PATH_CAP] = {0};
    char victim_dir[TEST_PATH_CAP] = {0};
    char victim_file[TEST_PATH_CAP] = {0};
    FILE *log = NULL;
    struct stat status = {0};
    bool paths_ok = false;
    bool directory_symlink_created = false;
    bool directory_symlink_rejected = false;
    bool secure_log_opened = false;
    bool directory_owner_only = false;
    bool file_owner_only = false;
    bool file_symlink_created = false;
    bool file_symlink_rejected = false;
    bool victim_unchanged = false;

    if (ipc_test_parent_new(parent, "private-log")) {
        int logs_written = snprintf(logs, sizeof(logs), "%s/logs", parent);
        int log_written = snprintf(log_path, sizeof(log_path), "%s/daemon.log", logs);
        int victim_dir_written = snprintf(victim_dir, sizeof(victim_dir), "%s/victim", parent);
        int victim_file_written = snprintf(victim_file, sizeof(victim_file), "%s/sentinel", parent);
        paths_ok = logs_written > 0 && logs_written < (int)sizeof(logs) && log_written > 0 &&
                   log_written < (int)sizeof(log_path) && victim_dir_written > 0 &&
                   victim_dir_written < (int)sizeof(victim_dir) && victim_file_written > 0 &&
                   victim_file_written < (int)sizeof(victim_file) && mkdir(victim_dir, 0700) == 0 &&
                   ipc_test_write_byte(victim_file, 0x4d);
    }
    if (paths_ok) {
        directory_symlink_created = symlink(victim_dir, logs) == 0;
    }
    if (directory_symlink_created) {
        log = cbm_daemon_ipc_private_log_open(logs, "daemon.log", 4096);
        directory_symlink_rejected = log == NULL;
        if (log) {
            (void)fclose(log);
            log = NULL;
        }
        (void)unlink(logs);
    }
    if (directory_symlink_rejected) {
        log = cbm_daemon_ipc_private_log_open(logs, "daemon.log", 4096);
        secure_log_opened = log != NULL;
        if (log) {
            (void)fputs("secure\n", log);
            (void)fclose(log);
            log = NULL;
        }
        directory_owner_only = lstat(logs, &status) == 0 && S_ISDIR(status.st_mode) &&
                               status.st_uid == geteuid() && (status.st_mode & 0777) == 0700;
        file_owner_only = lstat(log_path, &status) == 0 && S_ISREG(status.st_mode) &&
                          status.st_uid == geteuid() && (status.st_mode & 0777) == 0600;
    }
    if (file_owner_only) {
        (void)unlink(log_path);
        file_symlink_created = symlink(victim_file, log_path) == 0;
    }
    if (file_symlink_created) {
        log = cbm_daemon_ipc_private_log_open(logs, "daemon.log", 4096);
        file_symlink_rejected = log == NULL;
        if (log) {
            (void)fclose(log);
            log = NULL;
        }
        status.st_size = -1;
        victim_unchanged =
            stat(victim_file, &status) == 0 && S_ISREG(status.st_mode) && status.st_size == 1;
    }

    (void)unlink(log_path);
    (void)unlink(victim_file);
    (void)rmdir(logs);
    (void)rmdir(victim_dir);
    ipc_test_remove_flat_dir(parent);

    ASSERT_TRUE(paths_ok);
    ASSERT_TRUE(directory_symlink_created);
    ASSERT_TRUE(directory_symlink_rejected);
    ASSERT_TRUE(secure_log_opened);
    ASSERT_TRUE(directory_owner_only);
    ASSERT_TRUE(file_owner_only);
    ASSERT_TRUE(file_symlink_created);
    ASSERT_TRUE(file_symlink_rejected);
    ASSERT_TRUE(victim_unchanged);
    PASS();
}

TEST(daemon_ipc_posix_rejects_non_socket_and_symlink_endpoints) {
    static const char key[] = "ddddeeeeffff0000";
    char parent[TEST_PATH_CAP] = {0};
    char runtime_dir[TEST_PATH_CAP] = {0};
    char socket_path[TEST_PATH_CAP] = {0};
    char sentinel_path[TEST_PATH_CAP] = {0};
    cbm_daemon_ipc_endpoint_t *endpoint = NULL;
    cbm_daemon_ipc_listener_t *unexpected_listener = NULL;
    struct stat st = {0};
    bool regular_created = false;
    bool regular_rejected_unchanged = false;
    bool sentinel_created = false;
    bool symlink_created = false;
    bool symlink_rejected_unchanged = false;
    bool sentinel_unchanged = false;

    if (ipc_test_parent_new(parent, "stale-path")) {
        endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    }
    if (endpoint) {
        ipc_test_copy_path(runtime_dir, cbm_daemon_ipc_endpoint_runtime_dir(endpoint));
        ipc_test_copy_path(socket_path, cbm_daemon_ipc_endpoint_address(endpoint));
        (void)snprintf(sentinel_path, sizeof(sentinel_path), "%s/sentinel", parent);
        regular_created = ipc_test_write_byte(socket_path, 0x42);
    }
    if (regular_created) {
        unexpected_listener = cbm_daemon_ipc_listen(endpoint);
        regular_rejected_unchanged = unexpected_listener == NULL && lstat(socket_path, &st) == 0 &&
                                     S_ISREG(st.st_mode) && st.st_size == 1;
        cbm_daemon_ipc_listener_close(unexpected_listener);
        unexpected_listener = NULL;
        (void)unlink(socket_path);
        sentinel_created = ipc_test_write_byte(sentinel_path, 0x7e);
    }
    if (sentinel_created) {
        symlink_created = symlink(sentinel_path, socket_path) == 0;
    }
    if (symlink_created) {
        unexpected_listener = cbm_daemon_ipc_listen(endpoint);
        symlink_rejected_unchanged =
            unexpected_listener == NULL && lstat(socket_path, &st) == 0 && S_ISLNK(st.st_mode);
        sentinel_unchanged =
            stat(sentinel_path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size == 1;
        cbm_daemon_ipc_listener_close(unexpected_listener);
    }

    if (socket_path[0] != '\0') {
        (void)unlink(socket_path);
    }
    if (sentinel_path[0] != '\0') {
        (void)unlink(sentinel_path);
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    ipc_test_remove_tree(runtime_dir, parent);

    ASSERT_TRUE(regular_created);
    ASSERT_TRUE(regular_rejected_unchanged);
    ASSERT_TRUE(sentinel_created);
    ASSERT_TRUE(symlink_created);
    ASSERT_TRUE(symlink_rejected_unchanged);
    ASSERT_TRUE(sentinel_unchanged);
    PASS();
}

#endif /* !_WIN32 */

SUITE(daemon_ipc) {
    RUN_TEST(daemon_ipc_pending_timeout_race_returns_completed_io);
    RUN_TEST(daemon_ipc_pending_wait_failure_cancels_and_drains);
    RUN_TEST(daemon_ipc_pending_timeout_cancelled_returns_timeout);
    RUN_TEST(daemon_ipc_windows_generation_address_binds_account_key_and_nonce);
    RUN_TEST(daemon_ipc_windows_legacy_names_are_frozen_for_migration);
    RUN_TEST(daemon_ipc_windows_rendezvous_record_is_exact_and_canonical);
#ifdef _WIN32
    RUN_TEST(daemon_ipc_windows_default_endpoint_ignores_temp_environment);
    RUN_TEST(daemon_ipc_windows_private_directory_rejects_untrusted_ancestor_acl);
    RUN_TEST(daemon_ipc_windows_private_directory_allows_add_subdirectory_only_ancestor);
    RUN_TEST(daemon_ipc_windows_legacy_bridge_covers_handoff_and_lifetime);
    RUN_TEST(daemon_ipc_windows_local_transition_atomically_reserves_legacy_pipe);
    RUN_TEST(daemon_ipc_windows_startup_retries_transient_rendezvous_reader);
    RUN_TEST(daemon_ipc_windows_rendezvous_bridges_concurrent_lifetime_owner);
    RUN_TEST(daemon_ipc_windows_generation_rotates_and_escapes_occupied_old_pipe);
    RUN_TEST(daemon_ipc_windows_corrupt_rendezvous_fails_closed_until_startup_repairs);
    RUN_TEST(daemon_ipc_windows_startup_and_lifetime_locks_are_cross_process);
#endif
    RUN_TEST(daemon_ipc_endpoint_is_namespaced_by_instance_key);
    RUN_TEST(daemon_ipc_relative_runtime_parent_is_canonical_and_stable);
    RUN_TEST(daemon_ipc_rejects_uppercase_instance_key);
    RUN_TEST(daemon_ipc_no_spawn_probe_distinguishes_absent_active_and_busy);
    RUN_TEST(daemon_ipc_lifetime_reservation_survives_saturated_second_listen);
    RUN_TEST(daemon_ipc_lifetime_reservation_transfers_without_unlock_window);
    RUN_TEST(daemon_ipc_local_frame_roundtrip);
    RUN_TEST(daemon_ipc_bounded_receive_rejects_oversize_before_payload);
    RUN_TEST(daemon_ipc_wait_forever_is_interruptible);
    RUN_TEST(daemon_ipc_connect_waits_for_delayed_listener);
    RUN_TEST(daemon_ipc_startup_lock_has_one_winner);
    RUN_TEST(daemon_ipc_activation_probe_ignores_matching_startup_claim_only);
    RUN_TEST(daemon_ipc_local_transition_coexists_with_active_daemon_lifetime);
    RUN_TEST(daemon_ipc_local_participants_overlap_and_allow_modern_startup);
    RUN_TEST(daemon_ipc_windows_local_transition_release_retries_retained_mutex);
    RUN_TEST(daemon_ipc_windows_startup_release_retains_retry_authority);
    RUN_TEST(daemon_ipc_frame_timeout_poisons_connection);
#ifndef _WIN32
    RUN_TEST(daemon_ipc_posix_startup_lock_is_cross_process);
    RUN_TEST(daemon_ipc_posix_lifetime_reservation_rejects_fork_inheritance);
    RUN_TEST(daemon_ipc_posix_child_participant_handoff_retains_legacy_bridge);
    RUN_TEST(daemon_ipc_posix_publication_boundaries_recover_from_crash);
    RUN_TEST(daemon_ipc_posix_record_publication_windows_recover_from_crash);
    RUN_TEST(daemon_ipc_posix_unknown_record_temp_pair_is_preserved);
    RUN_TEST(daemon_ipc_posix_recovery_preserves_replaced_stable_socket);
    RUN_TEST(daemon_ipc_posix_pending_without_anchor_never_deletes_stable);
    RUN_TEST(daemon_ipc_posix_current_generation_crash_cleanup_requires_startup_lock);
    RUN_TEST(daemon_ipc_posix_unknown_socket_without_identity_refuses_cleanup);
    RUN_TEST(daemon_ipc_posix_active_listener_is_never_cleaned_under_queue_pressure);
    RUN_TEST(daemon_ipc_posix_partial_frame_timeout_poisons_connection);
#ifdef __APPLE__
    RUN_TEST(daemon_ipc_macos_clears_inherited_deny_acl_from_new_runtime);
    RUN_TEST(daemon_ipc_macos_rejects_allow_acl_on_ancestor_without_mutation);
    RUN_TEST(daemon_ipc_macos_repairs_or_rejects_existing_runtime_mutating_acl);
    RUN_TEST(daemon_ipc_macos_runtime_acl_injection_invalidates_existing_endpoint);
    RUN_TEST(daemon_ipc_macos_lock_acl_injection_invalidates_retained_startup);
#endif
    RUN_TEST(daemon_ipc_posix_runtime_and_socket_are_owner_only);
    RUN_TEST(daemon_ipc_posix_private_log_creates_first_ever_cache_tree_safely);
    RUN_TEST(daemon_ipc_posix_private_directory_hardens_existing_cache_root);
    RUN_TEST(daemon_ipc_posix_private_directory_rejects_world_writable_ancestor);
    RUN_TEST(daemon_ipc_posix_private_log_rejects_symlinks_and_is_owner_only);
    RUN_TEST(daemon_ipc_posix_rejects_non_socket_and_symlink_endpoints);
#endif
}
