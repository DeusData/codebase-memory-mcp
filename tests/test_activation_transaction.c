/* Transactional install/update/uninstall binary activation contract. */
#include "test_framework.h"
#include "test_helpers.h"

#include "cli/activation_transaction.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <aclapi.h>
#include <windows.h>
#endif

#ifdef __APPLE__
#include <errno.h>
#include <membership.h>
#include <sys/acl.h>
#endif

#ifndef _WIN32
#include <unistd.h>
#endif

enum { ACTIVATION_TEST_PATH_CAP = 1024, ACTIVATION_TEST_CONTENT_CAP = 256 };

static bool activation_test_fixture(char out[ACTIVATION_TEST_PATH_CAP]) {
    int written = snprintf(out, ACTIVATION_TEST_PATH_CAP, "%s/cbm-activation-transaction-XXXXXX",
                           cbm_tmpdir());
    return written > 0 && written < ACTIVATION_TEST_PATH_CAP && cbm_mkdtemp(out) != NULL;
}

static bool activation_test_path(char out[ACTIVATION_TEST_PATH_CAP], const char *directory,
                                 const char *name) {
    int written = snprintf(out, ACTIVATION_TEST_PATH_CAP, "%s/%s", directory, name);
    return written > 0 && written < ACTIVATION_TEST_PATH_CAP;
}

static bool activation_test_read(const char *path, char out[ACTIVATION_TEST_CONTENT_CAP]) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return false;
    }
    size_t used = fread(out, 1, ACTIVATION_TEST_CONTENT_CAP - 1, file);
    bool complete = !ferror(file) && feof(file);
    out[used] = '\0';
    return fclose(file) == 0 && complete;
}

static bool activation_test_write(const char *path, const char *contents) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file) {
        return false;
    }
    size_t length = strlen(contents);
    bool ok = fwrite(contents, 1, length, file) == length;
    ok = fclose(file) == 0 && ok;
#ifndef _WIN32
    ok = chmod(path, 0700) == 0 && ok;
#endif
    return ok;
}

static bool activation_test_exists(const char *path) {
    struct stat status;
    return path && stat(path, &status) == 0;
}

typedef struct {
    bool expect_absent;
    const char *expected_contents;
} activation_test_validation_t;

static bool activation_test_validate(const char *target_path, void *opaque) {
    const activation_test_validation_t *validation = opaque;
    if (validation->expect_absent) {
        return !activation_test_exists(target_path);
    }
    char contents[ACTIVATION_TEST_CONTENT_CAP];
    return activation_test_read(target_path, contents) &&
           strcmp(contents, validation->expected_contents) == 0;
}

static bool activation_test_reject(const char *target_path, void *opaque) {
    (void)target_path;
    (void)opaque;
    return false;
}

#ifdef _WIN32
#ifndef ACCESS_ALLOWED_CALLBACK_ACE_TYPE
#define ACCESS_ALLOWED_CALLBACK_ACE_TYPE (0x9)
#endif

typedef struct {
    ACE_HEADER header;
    ACCESS_MASK mask;
    DWORD sid_start;
} activation_test_callback_allow_ace_t;

static wchar_t *activation_test_windows_utf8_to_wide(const char *value) {
    int needed = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, NULL, 0);
    wchar_t *wide = needed > 0 ? calloc((size_t)needed, sizeof(*wide)) : NULL;
    if (!wide || MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, wide, needed) <= 0) {
        free(wide);
        return NULL;
    }
    return wide;
}

static bool activation_test_windows_current_user_sid(void **information_out, PSID *sid_out) {
    *information_out = NULL;
    *sid_out = NULL;
    HANDLE token = NULL;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        return false;
    }
    DWORD needed = 0;
    (void)GetTokenInformation(token, TokenUser, NULL, 0, &needed);
    void *information = needed > 0 ? calloc(1, needed) : NULL;
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

static bool activation_test_windows_set_directory_acl(const char *path,
                                                      bool include_untrusted_callback) {
    void *user_information = NULL;
    PSID user_sid = NULL;
    if (!activation_test_windows_current_user_sid(&user_information, &user_sid)) {
        return false;
    }
    unsigned char world_sid_storage[SECURITY_MAX_SID_SIZE];
    DWORD world_sid_size = sizeof(world_sid_storage);
    PSID world_sid = world_sid_storage;
    bool ok = CreateWellKnownSid(WinWorldSid, NULL, world_sid, &world_sid_size) != 0;
    DWORD world_sid_length = ok ? GetLengthSid(world_sid) : 0U;
    DWORD user_ace_size =
        (DWORD)(sizeof(ACCESS_ALLOWED_ACE) - sizeof(DWORD)) + GetLengthSid(user_sid);
    DWORD callback_ace_size =
        (DWORD)(sizeof(activation_test_callback_allow_ace_t) - sizeof(DWORD)) + world_sid_length;
    DWORD acl_size =
        (DWORD)sizeof(ACL) + user_ace_size + (include_untrusted_callback ? callback_ace_size : 0U);
    PACL acl = ok ? calloc(1, acl_size) : NULL;
    ok = acl && InitializeAcl(acl, acl_size, ACL_REVISION) != 0 &&
         AddAccessAllowedAceEx(acl, ACL_REVISION, 0, GENERIC_ALL, user_sid) != 0;
    unsigned char *callback_storage =
        include_untrusted_callback ? calloc(1, callback_ace_size) : NULL;
    if (ok && include_untrusted_callback) {
        activation_test_callback_allow_ace_t *callback =
            (activation_test_callback_allow_ace_t *)callback_storage;
        ok = callback && callback_ace_size <= UINT16_MAX;
        if (ok) {
            callback->header.AceType = ACCESS_ALLOWED_CALLBACK_ACE_TYPE;
            callback->header.AceFlags = 0;
            callback->header.AceSize = (WORD)callback_ace_size;
            callback->mask = FILE_ADD_FILE | FILE_DELETE_CHILD;
            ok = CopySid(world_sid_length, &callback->sid_start, world_sid) != 0 &&
                 AddAce(acl, ACL_REVISION, MAXDWORD, callback, callback_ace_size) != 0;
        }
    }
    wchar_t *wide_path = ok ? activation_test_windows_utf8_to_wide(path) : NULL;
    HANDLE directory =
        wide_path ? CreateFileW(wide_path, READ_CONTROL | WRITE_DAC,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                OPEN_EXISTING,
                                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL)
                  : INVALID_HANDLE_VALUE;
    ok = directory != INVALID_HANDLE_VALUE &&
         SetSecurityInfo(directory, SE_FILE_OBJECT,
                         DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION, NULL,
                         NULL, acl, NULL) == ERROR_SUCCESS;
    if (directory != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(directory);
    }
    free(wide_path);
    free(callback_storage);
    free(acl);
    free(user_information);
    return ok;
}

static bool activation_test_windows_directory_owned_by_current_user(const char *path) {
    void *user_information = NULL;
    PSID user_sid = NULL;
    wchar_t *wide_path = activation_test_windows_utf8_to_wide(path);
    HANDLE directory =
        wide_path ? CreateFileW(wide_path, READ_CONTROL,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                OPEN_EXISTING,
                                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL)
                  : INVALID_HANDLE_VALUE;
    PSID owner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    bool ok = directory != INVALID_HANDLE_VALUE &&
              activation_test_windows_current_user_sid(&user_information, &user_sid) &&
              GetSecurityInfo(directory, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &owner, NULL,
                              NULL, NULL, &descriptor) == ERROR_SUCCESS &&
              owner && IsValidSid(owner) && EqualSid(owner, user_sid);
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    if (directory != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(directory);
    }
    free(wide_path);
    free(user_information);
    return ok;
}
#endif

#ifndef _WIN32
typedef struct {
    const char *contents;
    bool created;
} activation_test_competing_target_t;

static void activation_test_create_competing_target(const char *target_path, void *opaque) {
    activation_test_competing_target_t *competing = opaque;
    competing->created = activation_test_write(target_path, competing->contents);
}
#endif

#ifdef __APPLE__
typedef enum {
    ACTIVATION_TEST_ACL_OK = 0,
    ACTIVATION_TEST_ACL_UNSUPPORTED = 1,
    ACTIVATION_TEST_ACL_ERROR = 2,
} activation_test_acl_status_t;

static activation_test_acl_status_t activation_test_install_mutating_acl(const char *path) {
    acl_t acl = acl_init(1);
    if (!acl) {
        return ACTIVATION_TEST_ACL_ERROR;
    }
    acl_entry_t entry = NULL;
    acl_permset_t permissions = NULL;
    gid_t foreign_group = getegid() == (gid_t)0 ? (gid_t)1 : (gid_t)0;
    uuid_t foreign_group_uuid;
    bool valid = acl_create_entry(&acl, &entry) == 0 && entry &&
                 acl_set_tag_type(entry, ACL_EXTENDED_ALLOW) == 0 &&
                 mbr_gid_to_uuid(foreign_group, foreign_group_uuid) == 0 &&
                 acl_set_qualifier(entry, foreign_group_uuid) == 0 &&
                 acl_get_permset(entry, &permissions) == 0 && permissions &&
                 acl_clear_perms(permissions) == 0 &&
                 acl_add_perm(permissions, ACL_ADD_FILE) == 0 &&
                 acl_add_perm(permissions, ACL_ADD_SUBDIRECTORY) == 0 &&
                 acl_add_perm(permissions, ACL_DELETE_CHILD) == 0 &&
                 acl_set_permset(entry, permissions) == 0 && acl_valid(acl) == 0;
    activation_test_acl_status_t status = ACTIVATION_TEST_ACL_ERROR;
    if (valid) {
        errno = 0;
        if (acl_set_file(path, ACL_TYPE_EXTENDED, acl) == 0) {
            status = ACTIVATION_TEST_ACL_OK;
        } else if (errno == ENOTSUP || errno == EOPNOTSUPP) {
            status = ACTIVATION_TEST_ACL_UNSUPPORTED;
        }
    }
    if (acl_free(acl) != 0) {
        status = ACTIVATION_TEST_ACL_ERROR;
    }
    return status;
}

static bool activation_test_clear_extended_acl(const char *path) {
    acl_t empty = acl_init(0);
    if (!empty) {
        return false;
    }
    bool cleared = acl_set_file(path, ACL_TYPE_EXTENDED, empty) == 0;
    return acl_free(empty) == 0 && cleared;
}

static bool activation_test_clear_extended_acl_if_exists(const char *path) {
    return !path || !path[0] || !activation_test_exists(path) ||
           activation_test_clear_extended_acl(path);
}

static bool activation_test_acl_metadata_acceptable(const char *path, bool expect_directory) {
    struct stat status;
    if (lstat(path, &status) != 0 || status.st_uid != geteuid() ||
        (status.st_mode & 0777) != 0700) {
        return false;
    }
    return expect_directory ? S_ISDIR(status.st_mode)
                            : S_ISREG(status.st_mode) && status.st_nlink == 1;
}
#endif

TEST(activation_transaction_stages_same_directory_private_executable_and_aborts) {
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "candidate", strlen("candidate"),
                                                     &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NOT_NULL(transaction);
    const char *staged = cbm_activation_transaction_staged_path(transaction);
    ASSERT_NOT_NULL(staged);
    ASSERT_TRUE(strncmp(staged, directory, strlen(directory)) == 0);
    /* The invariant is containment in the target's directory, not a specific
     * separator: Windows composes the staged path with '\' because the \\?\
     * namespace performs no forward-slash translation. */
    ASSERT_TRUE(staged[strlen(directory)] == '/' || staged[strlen(directory)] == '\\');
    ASSERT_FALSE(activation_test_exists(target));

    char contents[ACTIVATION_TEST_CONTENT_CAP];
    ASSERT_TRUE(activation_test_read(staged, contents));
    ASSERT_STR_EQ(contents, "candidate");
#ifndef _WIN32
    struct stat status;
    ASSERT_EQ(lstat(staged, &status), 0);
    ASSERT_TRUE(S_ISREG(status.st_mode));
    ASSERT_EQ(status.st_uid, geteuid());
    ASSERT_EQ(status.st_mode & 0777, 0700);
#endif

    char staged_copy[ACTIVATION_TEST_PATH_CAP];
    (void)snprintf(staged_copy, sizeof(staged_copy), "%s", staged);
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
    ASSERT_FALSE(activation_test_exists(staged_copy));
    ASSERT_FALSE(activation_test_exists(target));
    ASSERT_EQ(th_rmtree(directory), 0);
    PASS();
}

TEST(activation_transaction_commit_keeps_backup_until_finalize) {
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(target, "old"));

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "new", strlen("new"), &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    const char *backup = cbm_activation_transaction_backup_path(transaction);
    ASSERT_NOT_NULL(backup);
    char backup_copy[ACTIVATION_TEST_PATH_CAP];
    (void)snprintf(backup_copy, sizeof(backup_copy), "%s", backup);

    activation_test_validation_t validation = {
        .expect_absent = false,
        .expected_contents = "new",
    };
    ASSERT_EQ(cbm_activation_transaction_commit(transaction, activation_test_validate, &validation),
              CBM_ACTIVATION_TRANSACTION_OK);
    char contents[ACTIVATION_TEST_CONTENT_CAP];
    ASSERT_TRUE(activation_test_read(target, contents));
    ASSERT_STR_EQ(contents, "new");
    ASSERT_TRUE(activation_test_read(backup_copy, contents));
    ASSERT_STR_EQ(contents, "old");

    ASSERT_EQ(cbm_activation_transaction_finalize(transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_FALSE(activation_test_exists(backup_copy));
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(th_rmtree(directory), 0);
    PASS();
}

TEST(activation_transaction_validation_failure_restores_previous_target) {
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(target, "old"));

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "bad", strlen("bad"), &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    const char *backup = cbm_activation_transaction_backup_path(transaction);
    ASSERT_NOT_NULL(backup);
    char backup_copy[ACTIVATION_TEST_PATH_CAP];
    (void)snprintf(backup_copy, sizeof(backup_copy), "%s", backup);
    ASSERT_EQ(cbm_activation_transaction_commit(transaction, activation_test_reject, NULL),
              CBM_ACTIVATION_TRANSACTION_VALIDATION_FAILED);

    char contents[ACTIVATION_TEST_CONTENT_CAP];
    ASSERT_TRUE(activation_test_read(target, contents));
    ASSERT_STR_EQ(contents, "old");
    ASSERT_FALSE(activation_test_exists(backup_copy));
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(th_rmtree(directory), 0);
    PASS();
}

TEST(activation_transaction_explicit_rollback_restores_previous_target) {
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(target, "old"));

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "new", strlen("new"), &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    activation_test_validation_t validation = {
        .expect_absent = false,
        .expected_contents = "new",
    };
    ASSERT_EQ(cbm_activation_transaction_commit(transaction, activation_test_validate, &validation),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_rollback(transaction), CBM_ACTIVATION_TRANSACTION_OK);
    char contents[ACTIVATION_TEST_CONTENT_CAP];
    ASSERT_TRUE(activation_test_read(target, contents));
    ASSERT_STR_EQ(contents, "old");
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(th_rmtree(directory), 0);
    PASS();
}

TEST(activation_transaction_stage_file_installs_new_target) {
    char directory[ACTIVATION_TEST_PATH_CAP];
    char source[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(source, directory, "downloaded-cbm"));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(source, "downloaded"));

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_file(target, source, &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    activation_test_validation_t validation = {
        .expect_absent = false,
        .expected_contents = "downloaded",
    };
    ASSERT_EQ(cbm_activation_transaction_commit(transaction, activation_test_validate, &validation),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_finalize(transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
    char contents[ACTIVATION_TEST_CONTENT_CAP];
    ASSERT_TRUE(activation_test_read(target, contents));
    ASSERT_STR_EQ(contents, "downloaded");
    ASSERT_EQ(th_rmtree(directory), 0);
    PASS();
}

TEST(activation_transaction_stage_file_survives_long_target_path) {
    /* Managed installs and CI runners place the generation store far below a
     * deep profile path; the full store path routinely exceeds the legacy
     * 260-char limit. The activation transaction's own file operations must
     * carry those paths (extended-length form on Windows). */
    char base[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(th_secure_runtime_parent_new(base, sizeof(base), "act-longpath"));
    static const char segment[] = "gen-abcdefghijklmnopqrstuvwxyz0123456789-abcdefghij";
    char deep[ACTIVATION_TEST_PATH_CAP];
    int written = snprintf(deep, sizeof(deep), "%s", base);
    ASSERT_TRUE(written > 0 && written < (int)sizeof(deep));
    /* Append segments until the store path comfortably exceeds the legacy
     * 260-char Windows limit, regardless of the (platform-dependent) base. */
    while (strlen(deep) <= 320U) {
        size_t used = strlen(deep);
        written = snprintf(deep + used, sizeof(deep) - used, "/%s", segment);
        ASSERT_TRUE(written > 0 && (size_t)written < sizeof(deep) - used);
    }
    ASSERT_TRUE(strlen(deep) > 300);
    ASSERT_TRUE(cbm_mkdir_p(deep, 0700));

    char source[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_path(source, deep, "downloaded-cbm"));
    ASSERT_TRUE(activation_test_path(target, deep, "cbm"));
    ASSERT_TRUE(activation_test_write(source, "downloaded"));

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_file(target, source, &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    activation_test_validation_t validation = {
        .expect_absent = false,
        .expected_contents = "downloaded",
    };
    ASSERT_EQ(cbm_activation_transaction_commit(transaction, activation_test_validate, &validation),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_finalize(transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
    char contents[ACTIVATION_TEST_CONTENT_CAP];
    ASSERT_TRUE(activation_test_read(target, contents));
    ASSERT_STR_EQ(contents, "downloaded");
    ASSERT_EQ(th_rmtree(base), 0);
    PASS();
}

TEST(activation_transaction_removal_can_rollback_or_finalize) {
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(target, "old"));
    activation_test_validation_t absent = {
        .expect_absent = true,
        .expected_contents = NULL,
    };

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_removal(target, &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_commit(transaction, activation_test_validate, &absent),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_FALSE(activation_test_exists(target));
    ASSERT_EQ(cbm_activation_transaction_rollback(transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_TRUE(activation_test_exists(target));
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);

    ASSERT_EQ(cbm_activation_transaction_stage_removal(target, &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    const char *backup = cbm_activation_transaction_backup_path(transaction);
    ASSERT_NOT_NULL(backup);
    char backup_copy[ACTIVATION_TEST_PATH_CAP];
    (void)snprintf(backup_copy, sizeof(backup_copy), "%s", backup);
    ASSERT_EQ(cbm_activation_transaction_commit(transaction, activation_test_validate, &absent),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_finalize(transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_FALSE(activation_test_exists(target));
    ASSERT_FALSE(activation_test_exists(backup_copy));
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(th_rmtree(directory), 0);
    PASS();
}

TEST(activation_transaction_rejects_cross_account_writable_target_directory) {
#ifndef _WIN32
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_EQ(chmod(directory, 0777), 0);
    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "candidate", strlen("candidate"),
                                                     &transaction),
              CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_NULL(transaction);
    ASSERT_EQ(chmod(directory, 0700), 0);
    ASSERT_EQ(th_rmtree(directory), 0);
#endif
    PASS();
}

TEST(activation_transaction_rejects_windows_callback_allow_directory_ace) {
#ifdef _WIN32
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_windows_set_directory_acl(directory, true));
    ASSERT_TRUE(activation_test_windows_directory_owned_by_current_user(directory));

    cbm_activation_transaction_t *transaction = NULL;
    cbm_activation_transaction_status_t stage_status = cbm_activation_transaction_stage_bytes(
        target, "candidate", strlen("candidate"), &transaction);
    bool transaction_was_created = transaction != NULL;
    cbm_activation_transaction_status_t close_status =
        transaction ? cbm_activation_transaction_close(&transaction)
                    : CBM_ACTIVATION_TRANSACTION_OK;
    bool acl_restored = activation_test_windows_set_directory_acl(directory, false);
    int cleanup_status = th_rmtree(directory);

    ASSERT_EQ(stage_status, CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_FALSE(transaction_was_created);
    ASSERT_EQ(close_status, CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
    ASSERT_TRUE(acl_restored);
    ASSERT_EQ(cleanup_status, 0);
#endif
    PASS();
}

TEST(activation_transaction_rejects_symlink_candidate_target_and_parent) {
#ifndef _WIN32
    char directory[ACTIVATION_TEST_PATH_CAP];
    char candidate[ACTIVATION_TEST_PATH_CAP];
    char candidate_link[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    char target_link[ACTIVATION_TEST_PATH_CAP];
    char real_parent[ACTIVATION_TEST_PATH_CAP];
    char real_nested[ACTIVATION_TEST_PATH_CAP];
    char real_nested_candidate[ACTIVATION_TEST_PATH_CAP];
    char parent_link[ACTIVATION_TEST_PATH_CAP];
    char linked_parent_target[ACTIVATION_TEST_PATH_CAP];
    char linked_nested_target[ACTIVATION_TEST_PATH_CAP];
    char linked_nested_candidate[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(candidate, directory, "candidate"));
    ASSERT_TRUE(activation_test_path(candidate_link, directory, "candidate-link"));
    ASSERT_TRUE(activation_test_path(target, directory, "target"));
    ASSERT_TRUE(activation_test_path(target_link, directory, "target-link"));
    ASSERT_TRUE(activation_test_path(real_parent, directory, "real-parent"));
    ASSERT_TRUE(activation_test_path(real_nested, real_parent, "nested"));
    ASSERT_TRUE(activation_test_path(real_nested_candidate, real_nested, "candidate"));
    ASSERT_TRUE(activation_test_path(parent_link, directory, "parent-link"));
    ASSERT_TRUE(activation_test_path(linked_parent_target, parent_link, "cbm"));
    ASSERT_TRUE(activation_test_path(linked_nested_target, parent_link, "nested/cbm"));
    ASSERT_TRUE(activation_test_path(linked_nested_candidate, parent_link, "nested/candidate"));
    ASSERT_TRUE(activation_test_write(candidate, "candidate"));
    ASSERT_EQ(symlink(candidate, candidate_link), 0);
    ASSERT_EQ(symlink(candidate, target_link), 0);
    ASSERT_TRUE(cbm_mkdir_p(real_parent, 0700));
    ASSERT_TRUE(cbm_mkdir_p(real_nested, 0700));
    ASSERT_TRUE(activation_test_write(real_nested_candidate, "candidate"));
    ASSERT_EQ(symlink(real_parent, parent_link), 0);

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_file(target, candidate_link, &transaction),
              CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_NULL(transaction);
    ASSERT_EQ(cbm_activation_transaction_stage_file(target, linked_nested_candidate, &transaction),
              CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_NULL(transaction);
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target_link, "replacement",
                                                     strlen("replacement"), &transaction),
              CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_NULL(transaction);
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(linked_parent_target, "replacement",
                                                     strlen("replacement"), &transaction),
              CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_NULL(transaction);
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(linked_nested_target, "replacement",
                                                     strlen("replacement"), &transaction),
              CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_NULL(transaction);

    char contents[ACTIVATION_TEST_CONTENT_CAP];
    ASSERT_TRUE(activation_test_read(candidate, contents));
    ASSERT_STR_EQ(contents, "candidate");
    ASSERT_EQ(unlink(candidate_link), 0);
    ASSERT_EQ(unlink(target_link), 0);
    ASSERT_EQ(unlink(parent_link), 0);
    ASSERT_EQ(th_rmtree(directory), 0);
#endif
    PASS();
}

TEST(activation_transaction_fails_closed_if_target_directory_is_replaced) {
    char root[ACTIVATION_TEST_PATH_CAP];
    char active[ACTIVATION_TEST_PATH_CAP];
    char moved[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(root));
    ASSERT_TRUE(activation_test_path(active, root, "active"));
    ASSERT_TRUE(activation_test_path(moved, root, "moved"));
    ASSERT_TRUE(cbm_mkdir_p(active, 0700));
    ASSERT_TRUE(activation_test_path(target, active, "cbm"));

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "candidate", strlen("candidate"),
                                                     &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(rename(active, moved), 0);
    ASSERT_TRUE(cbm_mkdir_p(active, 0700));
    ASSERT_EQ(cbm_activation_transaction_commit(transaction, NULL, NULL),
              CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_FALSE(activation_test_exists(target));
    ASSERT_EQ(cbm_rmdir(active), 0);
    ASSERT_EQ(rename(moved, active), 0);
    ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
    ASSERT_EQ(th_rmtree(root), 0);
    PASS();
}

TEST(activation_transaction_does_not_replace_target_created_at_publish_boundary) {
#ifndef _WIN32
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));

    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "candidate", strlen("candidate"),
                                                     &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    const char *staged = cbm_activation_transaction_staged_path(transaction);
    ASSERT_NOT_NULL(staged);
    char staged_copy[ACTIVATION_TEST_PATH_CAP];
    (void)snprintf(staged_copy, sizeof(staged_copy), "%s", staged);

    activation_test_competing_target_t competing = {
        .contents = "external",
        .created = false,
    };
    cbm_activation_transaction_set_before_absent_publish_for_test(
        activation_test_create_competing_target, &competing);
    cbm_activation_transaction_status_t commit_status =
        cbm_activation_transaction_commit(transaction, NULL, NULL);
    cbm_activation_transaction_set_before_absent_publish_for_test(NULL, NULL);

    char before_close[ACTIVATION_TEST_CONTENT_CAP];
    bool target_survived_commit = activation_test_read(target, before_close);
    bool stage_survived_commit = activation_test_exists(staged_copy);
    cbm_activation_transaction_status_t close_status =
        cbm_activation_transaction_close(&transaction);
    bool stage_survived_close = activation_test_exists(staged_copy);
    char after_close[ACTIVATION_TEST_CONTENT_CAP];
    bool target_survived_close = activation_test_read(target, after_close);

    if (activation_test_exists(target)) {
        ASSERT_EQ(cbm_unlink(target), 0);
    }
    ASSERT_EQ(th_rmtree(directory), 0);

    ASSERT_TRUE(competing.created);
    ASSERT_EQ(commit_status, CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_TRUE(target_survived_commit);
    ASSERT_STR_EQ(before_close, "external");
    ASSERT_TRUE(stage_survived_commit);
    ASSERT_EQ(close_status, CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
    ASSERT_FALSE(stage_survived_close);
    ASSERT_TRUE(target_survived_close);
    ASSERT_STR_EQ(after_close, "external");
#endif
    PASS();
}

TEST(activation_transaction_rejects_macos_mutating_extended_acl) {
#ifdef __APPLE__
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_EQ(chmod(directory, 0700), 0);

    activation_test_acl_status_t acl_status = activation_test_install_mutating_acl(directory);
    if (acl_status == ACTIVATION_TEST_ACL_UNSUPPORTED) {
        ASSERT_EQ(th_rmtree(directory), 0);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    ASSERT_EQ(acl_status, ACTIVATION_TEST_ACL_OK);

    struct stat status;
    ASSERT_EQ(stat(directory, &status), 0);
    ASSERT_EQ(status.st_mode & 0777, 0700);

    cbm_activation_transaction_t *transaction = NULL;
    cbm_activation_transaction_status_t stage_status = cbm_activation_transaction_stage_bytes(
        target, "candidate", strlen("candidate"), &transaction);
    bool transaction_was_created = transaction != NULL;
    cbm_activation_transaction_status_t close_status =
        transaction ? cbm_activation_transaction_close(&transaction)
                    : CBM_ACTIVATION_TRANSACTION_OK;
    ASSERT_EQ(th_rmtree(directory), 0);

    ASSERT_EQ(stage_status, CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_FALSE(transaction_was_created);
    ASSERT_EQ(close_status, CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
#endif
    PASS();
}

TEST(activation_transaction_rejects_macos_existing_target_mutating_acl) {
#ifdef __APPLE__
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(target, "old"));

    activation_test_acl_status_t acl_status = activation_test_install_mutating_acl(target);
    if (acl_status == ACTIVATION_TEST_ACL_UNSUPPORTED) {
        ASSERT_EQ(th_rmtree(directory), 0);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    ASSERT_EQ(acl_status, ACTIVATION_TEST_ACL_OK);
    bool metadata_acceptable = activation_test_acl_metadata_acceptable(target, false);

    cbm_activation_transaction_t *transaction = NULL;
    cbm_activation_transaction_status_t stage_status = cbm_activation_transaction_stage_bytes(
        target, "candidate", strlen("candidate"), &transaction);
    char staged[ACTIVATION_TEST_PATH_CAP] = {0};
    char backup[ACTIVATION_TEST_PATH_CAP] = {0};
    if (transaction) {
        const char *staged_path = cbm_activation_transaction_staged_path(transaction);
        const char *backup_path = cbm_activation_transaction_backup_path(transaction);
        if (staged_path) {
            (void)snprintf(staged, sizeof(staged), "%s", staged_path);
        }
        if (backup_path) {
            (void)snprintf(backup, sizeof(backup), "%s", backup_path);
        }
    }
    cbm_activation_transaction_status_t commit_status = CBM_ACTIVATION_TRANSACTION_INVALID_STATE;
    if (stage_status == CBM_ACTIVATION_TRANSACTION_OK && transaction) {
        commit_status = cbm_activation_transaction_commit(transaction, NULL, NULL);
    }
    char observed[ACTIVATION_TEST_CONTENT_CAP];
    bool target_unchanged = activation_test_read(target, observed) && strcmp(observed, "old") == 0;
    bool acl_cleared = activation_test_clear_extended_acl_if_exists(target) &&
                       activation_test_clear_extended_acl_if_exists(staged) &&
                       activation_test_clear_extended_acl_if_exists(backup);
    cbm_activation_transaction_status_t close_status =
        transaction ? cbm_activation_transaction_close(&transaction)
                    : CBM_ACTIVATION_TRANSACTION_OK;
    char restored[ACTIVATION_TEST_CONTENT_CAP];
    bool target_restored = activation_test_read(target, restored) && strcmp(restored, "old") == 0;
    int cleanup_status = th_rmtree(directory);

    ASSERT_TRUE(metadata_acceptable);
    ASSERT_TRUE(stage_status == CBM_ACTIVATION_TRANSACTION_IO ||
                (stage_status == CBM_ACTIVATION_TRANSACTION_OK &&
                 commit_status == CBM_ACTIVATION_TRANSACTION_IO));
    ASSERT_TRUE(target_unchanged);
    ASSERT_TRUE(acl_cleared);
    ASSERT_EQ(close_status, CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
    ASSERT_TRUE(target_restored);
    ASSERT_EQ(cleanup_status, 0);
#endif
    PASS();
}

TEST(activation_transaction_revalidates_macos_directory_acl_before_commit) {
#ifdef __APPLE__
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(target, "old"));
    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "candidate", strlen("candidate"),
                                                     &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);

    activation_test_acl_status_t acl_status = activation_test_install_mutating_acl(directory);
    if (acl_status == ACTIVATION_TEST_ACL_UNSUPPORTED) {
        ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
        ASSERT_EQ(th_rmtree(directory), 0);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    ASSERT_EQ(acl_status, ACTIVATION_TEST_ACL_OK);
    bool metadata_acceptable = activation_test_acl_metadata_acceptable(directory, true);
    cbm_activation_transaction_status_t commit_status =
        cbm_activation_transaction_commit(transaction, NULL, NULL);
    char observed[ACTIVATION_TEST_CONTENT_CAP];
    bool target_unchanged = activation_test_read(target, observed) && strcmp(observed, "old") == 0;
    bool acl_cleared = activation_test_clear_extended_acl(directory);
    cbm_activation_transaction_status_t close_status =
        cbm_activation_transaction_close(&transaction);
    int cleanup_status = th_rmtree(directory);

    ASSERT_TRUE(metadata_acceptable);
    ASSERT_EQ(commit_status, CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_TRUE(target_unchanged);
    ASSERT_TRUE(acl_cleared);
    ASSERT_EQ(close_status, CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
    ASSERT_EQ(cleanup_status, 0);
#endif
    PASS();
}

TEST(activation_transaction_revalidates_macos_staged_file_acl_before_commit) {
#ifdef __APPLE__
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(target, "old"));
    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "candidate", strlen("candidate"),
                                                     &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    const char *staged_path = cbm_activation_transaction_staged_path(transaction);
    ASSERT_NOT_NULL(staged_path);
    char staged[ACTIVATION_TEST_PATH_CAP];
    (void)snprintf(staged, sizeof(staged), "%s", staged_path);

    activation_test_acl_status_t acl_status = activation_test_install_mutating_acl(staged);
    if (acl_status == ACTIVATION_TEST_ACL_UNSUPPORTED) {
        ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
        ASSERT_EQ(th_rmtree(directory), 0);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    ASSERT_EQ(acl_status, ACTIVATION_TEST_ACL_OK);
    bool metadata_acceptable = activation_test_acl_metadata_acceptable(staged, false);
    cbm_activation_transaction_status_t commit_status =
        cbm_activation_transaction_commit(transaction, NULL, NULL);
    char observed[ACTIVATION_TEST_CONTENT_CAP];
    bool target_unchanged = activation_test_read(target, observed) && strcmp(observed, "old") == 0;
    bool acl_cleared = activation_test_clear_extended_acl_if_exists(staged) &&
                       activation_test_clear_extended_acl_if_exists(target);
    cbm_activation_transaction_status_t close_status =
        cbm_activation_transaction_close(&transaction);
    int cleanup_status = th_rmtree(directory);

    ASSERT_TRUE(metadata_acceptable);
    ASSERT_EQ(commit_status, CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_TRUE(target_unchanged);
    ASSERT_TRUE(acl_cleared);
    ASSERT_EQ(close_status, CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
    ASSERT_EQ(cleanup_status, 0);
#endif
    PASS();
}

TEST(activation_transaction_revalidates_macos_target_file_acl_before_commit) {
#ifdef __APPLE__
    char directory[ACTIVATION_TEST_PATH_CAP];
    char target[ACTIVATION_TEST_PATH_CAP];
    ASSERT_TRUE(activation_test_fixture(directory));
    ASSERT_TRUE(activation_test_path(target, directory, "cbm"));
    ASSERT_TRUE(activation_test_write(target, "old"));
    cbm_activation_transaction_t *transaction = NULL;
    ASSERT_EQ(cbm_activation_transaction_stage_bytes(target, "candidate", strlen("candidate"),
                                                     &transaction),
              CBM_ACTIVATION_TRANSACTION_OK);
    const char *backup_path = cbm_activation_transaction_backup_path(transaction);
    ASSERT_NOT_NULL(backup_path);
    char backup[ACTIVATION_TEST_PATH_CAP];
    (void)snprintf(backup, sizeof(backup), "%s", backup_path);

    activation_test_acl_status_t acl_status = activation_test_install_mutating_acl(target);
    if (acl_status == ACTIVATION_TEST_ACL_UNSUPPORTED) {
        ASSERT_EQ(cbm_activation_transaction_close(&transaction), CBM_ACTIVATION_TRANSACTION_OK);
        ASSERT_EQ(th_rmtree(directory), 0);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    ASSERT_EQ(acl_status, ACTIVATION_TEST_ACL_OK);
    bool metadata_acceptable = activation_test_acl_metadata_acceptable(target, false);
    cbm_activation_transaction_status_t commit_status =
        cbm_activation_transaction_commit(transaction, NULL, NULL);
    char observed[ACTIVATION_TEST_CONTENT_CAP];
    bool target_unchanged = activation_test_read(target, observed) && strcmp(observed, "old") == 0;
    bool acl_cleared = activation_test_clear_extended_acl_if_exists(target) &&
                       activation_test_clear_extended_acl_if_exists(backup);
    cbm_activation_transaction_status_t close_status =
        cbm_activation_transaction_close(&transaction);
    int cleanup_status = th_rmtree(directory);

    ASSERT_TRUE(metadata_acceptable);
    ASSERT_EQ(commit_status, CBM_ACTIVATION_TRANSACTION_IO);
    ASSERT_TRUE(target_unchanged);
    ASSERT_TRUE(acl_cleared);
    ASSERT_EQ(close_status, CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_NULL(transaction);
    ASSERT_EQ(cleanup_status, 0);
#endif
    PASS();
}

SUITE(activation_transaction) {
    RUN_TEST(activation_transaction_stages_same_directory_private_executable_and_aborts);
    RUN_TEST(activation_transaction_commit_keeps_backup_until_finalize);
    RUN_TEST(activation_transaction_validation_failure_restores_previous_target);
    RUN_TEST(activation_transaction_explicit_rollback_restores_previous_target);
    RUN_TEST(activation_transaction_stage_file_installs_new_target);
    RUN_TEST(activation_transaction_stage_file_survives_long_target_path);
    RUN_TEST(activation_transaction_removal_can_rollback_or_finalize);
    RUN_TEST(activation_transaction_rejects_cross_account_writable_target_directory);
    RUN_TEST(activation_transaction_rejects_windows_callback_allow_directory_ace);
    RUN_TEST(activation_transaction_rejects_symlink_candidate_target_and_parent);
    RUN_TEST(activation_transaction_fails_closed_if_target_directory_is_replaced);
    RUN_TEST(activation_transaction_does_not_replace_target_created_at_publish_boundary);
    RUN_TEST(activation_transaction_rejects_macos_mutating_extended_acl);
    RUN_TEST(activation_transaction_rejects_macos_existing_target_mutating_acl);
    RUN_TEST(activation_transaction_revalidates_macos_directory_acl_before_commit);
    RUN_TEST(activation_transaction_revalidates_macos_staged_file_acl_before_commit);
    RUN_TEST(activation_transaction_revalidates_macos_target_file_acl_before_commit);
}
