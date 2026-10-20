/* Independent acceptance for the frozen pinned-tree contract.
 * All repository mutations below belong to private fixture repositories. */
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>
#include <errno.h>
#include <foundation/arena.h>
#include <foundation/sha256.h>
#include <foundation/subprocess.h>
#include <mcp/test_impact_tree.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <aclapi.h>
#include <wchar.h>
#include <winioctl.h>
#else
#include <dirent.h>
#include <fcntl.h>
#include <pwd.h>
#include <unistd.h>
#ifdef __APPLE__
#include <membership.h>
#include <sys/acl.h>
#else
#include <sys/xattr.h>
#endif
#endif

enum { PT_PATH = 4096, PT_ROWS = 12, PT_LARGE = 131073 };
static const unsigned char pt_binary[] = {'A', 0, '\r', '\n', 255, 'Z'};
static const unsigned char pt_shared[] = "shared\n";
static const unsigned char pt_before[] = "before\n";
static const unsigned char pt_after[] = "after!\n";

typedef struct {
    const char *path;
    uint32_t mode;
    const unsigned char *bytes;
    size_t length;
    char oid[65];
} pt_row;

typedef struct {
    CBMArena arena;
    char home[PT_PATH], repo[PT_PATH], parent[PT_PATH], git[PT_PATH];
    char capture[PT_PATH], log[PT_PATH], input[PT_PATH];
    char a[65], p[65], q[65], head[65], base[65], before_oid[65];
    unsigned width;
    unsigned char *large;
    pt_row rows[PT_ROWS];
    size_t count;
    cbm_git_facts_t *facts;
    cbm_pinned_tree_t *tree, *second;
} pt_fixture;

#define PT_CHECK(condition)                                                                       \
    do {                                                                                          \
        if (!(condition)) {                                                                       \
            fprintf(stderr, "pinned-tree assertion %s:%d: %s\n", __FILE__, __LINE__, #condition); \
            goto done;                                                                            \
        }                                                                                         \
    } while (0)

/* Diagnostics preserve the operation's result and its last-error state. */
static bool pt_setup_step(const char *stage, bool ok) {
    if (ok)
        return true;
    int saved_errno = errno;
#ifdef _WIN32
    DWORD saved_error = GetLastError();
    fprintf(stderr, "pinned-tree fixture stage=%s errno=%d win32=%lu\n", stage, saved_errno,
            (unsigned long)saved_error);
    SetLastError(saved_error);
#else
    fprintf(stderr, "pinned-tree fixture stage=%s errno=%d\n", stage, saved_errno);
#endif
    errno = saved_errno;
    return false;
}

static void pt_diag_bytes(const unsigned char *bytes, size_t length) {
    for (size_t i = 0; i < length; i++) {
        unsigned char c = bytes[i];
        if (c >= 32 && c <= 126 && c != '\\' && c != '\'')
            fputc(c, stderr);
        else
            fprintf(stderr, "\\x%02x", (unsigned)c);
    }
}

static void pt_diag_git_tail(const char *const *tail) {
    fputs("pinned-tree fixture git tail:", stderr);
    size_t i = 0;
    for (; i < 12 && tail[i]; i++) {
        size_t length = 0;
        while (length < 64 && tail[i][length])
            length++;
        fputs(" '", stderr);
        pt_diag_bytes((const unsigned char *)tail[i], length);
        fputs(length == 64 ? "'[possibly truncated]" : "'", stderr);
    }
    fputs(i == 12 ? " [remaining arguments omitted]\n" : "\n", stderr);
}

static void pt_diag_git_stderr(const char *path) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        fprintf(stderr, "pinned-tree fixture stderr unavailable errno=%d\n", errno);
        return;
    }
    unsigned char bytes[2048];
    bool seek_ok = fseek(file, 0, SEEK_END) == 0;
    long end = seek_ok ? ftell(file) : -1;
    long start = end > (long)sizeof(bytes) ? end - (long)sizeof(bytes) : 0;
    if (end < 0 || fseek(file, start, SEEK_SET) != 0) {
        fprintf(stderr, "pinned-tree fixture stderr seek failed errno=%d\n", errno);
        (void)fclose(file);
        return;
    }
    size_t length = fread(bytes, 1, sizeof(bytes), file);
    int read_error = ferror(file);
    fprintf(stderr, "pinned-tree fixture stderr tail offset=%ld bytes=%zu read_error=%d: '", start,
            length, read_error);
    pt_diag_bytes(bytes, length);
    fputs("' (capture may predate an unsuccessful spawn)\n", stderr);
    if (fclose(file) != 0)
        fprintf(stderr, "pinned-tree fixture stderr close failed errno=%d\n", errno);
}

static bool pt_git_failed(const pt_fixture *fx, const char *const *tail, const char *stage,
                          int run_rc, const cbm_proc_result_t *result) {
    int saved_errno = errno;
#ifdef _WIN32
    DWORD saved_error = GetLastError();
#endif
    fprintf(stderr,
            "pinned-tree fixture git failure stage=%s run_rc=%d outcome=%d exit=%d "
            "quiesced=%d supervision_failed=%d errno=%d\n",
            stage, run_rc, result ? (int)result->outcome : -1, result ? result->exit_code : -1,
            result ? (int)result->tree_quiesced : 0, result ? (int)result->supervision_failed : 0,
            saved_errno);
    pt_diag_git_tail(tail);
    pt_diag_git_stderr(fx->log);
#ifdef _WIN32
    SetLastError(saved_error);
#endif
    errno = saved_errno;
    return false;
}

static bool pt_join(char out[PT_PATH], const char *root, const char *leaf) {
    int n = snprintf(out, PT_PATH, "%s/%s", root, leaf);
    return n > 0 && n < PT_PATH;
}

static bool pt_write(const char *path, const void *bytes, size_t length) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file)
        return false;
    bool ok = fwrite(bytes, 1, length, file) == length;
    return fclose(file) == 0 && ok;
}

static bool pt_equal_file(const char *path, const void *bytes, size_t length) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file)
        return false;
    unsigned char buffer[4096];
    size_t offset = 0;
    bool ok = true;
    while (offset < length && ok) {
        size_t amount = length - offset;
        if (amount > sizeof(buffer))
            amount = sizeof(buffer);
        ok = fread(buffer, 1, amount, file) == amount &&
             memcmp(buffer, (const unsigned char *)bytes + offset, amount) == 0;
        offset += amount;
    }
    ok = ok && fgetc(file) == EOF && !ferror(file);
    return fclose(file) == 0 && ok;
}

#ifdef _WIN32
static bool pt_wide(const char *path, wchar_t out[PT_PATH]) {
    return MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, out, PT_PATH) > 0;
}

static PSID pt_current_sid(pt_fixture *fx) {
    HANDLE token = NULL;
    DWORD length = 0;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token))
        return NULL;
    (void)GetTokenInformation(token, TokenUser, NULL, 0, &length);
    TOKEN_USER *user = length ? cbm_arena_alloc(&fx->arena, length) : NULL;
    bool ok = user && GetTokenInformation(token, TokenUser, user, length, &length);
    ok = CloseHandle(token) && ok;
    return ok ? user->User.Sid : NULL;
}

static bool pt_windows_acl(pt_fixture *fx, const char *path, bool extra) {
    PSID owner = pt_current_sid(fx);
    wchar_t wide[PT_PATH];
    unsigned char acl_bytes[1024], world_bytes[SECURITY_MAX_SID_SIZE];
    DWORD world_length = sizeof(world_bytes);
    ACL *acl = (ACL *)acl_bytes;
    SECURITY_DESCRIPTOR descriptor;
    if (!owner || !pt_wide(path, wide) || !InitializeAcl(acl, sizeof(acl_bytes), ACL_REVISION) ||
        !AddAccessAllowedAceEx(acl, ACL_REVISION, OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
                               FILE_ALL_ACCESS, owner))
        return false;
    if (extra && (!CreateWellKnownSid(WinWorldSid, NULL, world_bytes, &world_length) ||
                  !AddAccessAllowedAceEx(acl, ACL_REVISION, 0, FILE_GENERIC_READ, world_bytes)))
        return false;
    return InitializeSecurityDescriptor(&descriptor, SECURITY_DESCRIPTOR_REVISION) &&
           SetSecurityDescriptorOwner(&descriptor, owner, FALSE) &&
           SetSecurityDescriptorDacl(&descriptor, TRUE, acl, FALSE) &&
           SetSecurityDescriptorControl(&descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED) &&
           SetFileSecurityW(wide,
                            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION |
                                PROTECTED_DACL_SECURITY_INFORMATION,
                            &descriptor);
}

/* The encoding Windows stores for an inheritable generic grant: an effective
 * full-access entry for the owner plus an inherit-only one, here naming
 * `inherited` (the owner, or another principal for the negative). */
static bool pt_windows_split_acl(pt_fixture *fx, const char *path, PSID inherited) {
    PSID owner = pt_current_sid(fx);
    wchar_t wide[PT_PATH];
    unsigned char acl_bytes[1024];
    ACL *acl = (ACL *)acl_bytes;
    SECURITY_DESCRIPTOR descriptor;
    return owner && inherited && pt_wide(path, wide) &&
           InitializeAcl(acl, sizeof(acl_bytes), ACL_REVISION) &&
           AddAccessAllowedAceEx(acl, ACL_REVISION, 0, FILE_ALL_ACCESS, owner) &&
           AddAccessAllowedAceEx(acl, ACL_REVISION,
                                 OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE | INHERIT_ONLY_ACE,
                                 FILE_ALL_ACCESS, inherited) &&
           InitializeSecurityDescriptor(&descriptor, SECURITY_DESCRIPTOR_REVISION) &&
           SetSecurityDescriptorOwner(&descriptor, owner, FALSE) &&
           SetSecurityDescriptorDacl(&descriptor, TRUE, acl, FALSE) &&
           SetSecurityDescriptorControl(&descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED) &&
           SetFileSecurityW(wide,
                            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION |
                                PROTECTED_DACL_SECURITY_INFORMATION,
                            &descriptor);
}

static bool pt_owner_is_current(PSID owner) {
    HANDLE token = NULL;
    union {
        max_align_t alignment;
        unsigned char bytes[1024];
    } buffer;
    DWORD size = sizeof(buffer.bytes);
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token))
        return false;
    bool ok = GetTokenInformation(token, TokenUser, buffer.bytes, size, &size) &&
              EqualSid(owner, ((TOKEN_USER *)buffer.bytes)->User.Sid);
    return CloseHandle(token) && ok;
}

static bool pt_policy(const char *path, bool directory, bool executable) {
    wchar_t wide[PT_PATH];
    if (!pt_wide(path, wide))
        return false;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    PSID owner = NULL;
    PACL acl = NULL;
    DWORD result = GetNamedSecurityInfoW(wide, SE_FILE_OBJECT,
                                         OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                                         &owner, NULL, &acl, NULL, &descriptor);
    SECURITY_DESCRIPTOR_CONTROL control = 0;
    DWORD revision = 0;
    bool ok = result == ERROR_SUCCESS && owner && pt_owner_is_current(owner) && acl &&
              acl->AceCount == 1 && GetSecurityDescriptorControl(descriptor, &control, &revision) &&
              (control & SE_DACL_PROTECTED) != 0;
    void *raw = NULL;
    if (ok)
        ok = GetAce(acl, 0, &raw) && raw;
    if (ok) {
        ACCESS_ALLOWED_ACE *ace = raw;
        ok = ace->Header.AceType == ACCESS_ALLOWED_ACE_TYPE && EqualSid(owner, &ace->SidStart) &&
             (ace->Mask & FILE_ALL_ACCESS) == FILE_ALL_ACCESS;
    }
    DWORD attributes = GetFileAttributesW(wide);
    ok = ok && attributes != INVALID_FILE_ATTRIBUTES &&
         !(attributes & FILE_ATTRIBUTE_REPARSE_POINT) &&
         (((attributes & FILE_ATTRIBUTE_DIRECTORY) != 0) == directory) &&
         (directory || (attributes & FILE_ATTRIBUTE_READONLY));
    (void)executable;
    if (descriptor)
        LocalFree(descriptor);
    return ok;
}

static bool pt_make_writable(const char *path) {
    wchar_t wide[PT_PATH];
    if (!pt_wide(path, wide))
        return false;
    DWORD attributes = GetFileAttributesW(wide);
    return attributes != INVALID_FILE_ATTRIBUTES &&
           SetFileAttributesW(wide, attributes & ~(DWORD)FILE_ATTRIBUTE_READONLY);
}
#else
#ifdef __APPLE__
static bool pt_mac_same_object(const struct stat *a, const struct stat *b) {
    return a->st_dev == b->st_dev && a->st_ino == b->st_ino && a->st_mode == b->st_mode &&
           a->st_uid == b->st_uid && a->st_nlink == b->st_nlink && a->st_nlink > 0;
}

/* Native probing confirms absent extended ACLs can yield ENOENT through an fd.
 * Only a no-follow descriptor tied to this still-existing object may interpret
 * that result as absence. Path lookup failure is never an absent ACL. */
static bool pt_mac_read_acl(const char *path, acl_t *out, bool *absent) {
    *out = NULL;
    *absent = false;
    struct stat named, before, after, final;
    if (lstat(path, &named) != 0 || !(S_ISDIR(named.st_mode) || S_ISREG(named.st_mode)))
        return false;
    int fd = open(path, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        return false;
    bool ok = fstat(fd, &before) == 0 && pt_mac_same_object(&named, &before);
    acl_t acl = NULL;
    int acl_error = 0;
    if (ok) {
        errno = 0;
        acl = acl_get_fd_np(fd, ACL_TYPE_EXTENDED);
        acl_error = errno;
        ok = acl != NULL || acl_error == ENOENT;
    }
    if (ok)
        ok = fstat(fd, &after) == 0 && lstat(path, &final) == 0 &&
             pt_mac_same_object(&before, &after) && pt_mac_same_object(&after, &final);
    int close_rc = close(fd);
    if (!ok || close_rc != 0) {
        int saved_errno = errno;
        fprintf(stderr,
                "pinned-tree verified-fd ACL failed null=%d acl_errno=%d "
                "object_ok=%d close_rc=%d errno=%d\n",
                acl == NULL, acl_error, (int)ok, close_rc, saved_errno);
        if (acl)
            (void)acl_free(acl);
        errno = saved_errno;
        return false;
    }
    *out = acl;
    *absent = acl == NULL;
    return true;
}
#endif

static bool pt_acl_absent(const char *path, bool directory) {
#ifdef __APPLE__
    acl_t acl = NULL;
    bool absent = false;
    if (!pt_mac_read_acl(path, &acl, &absent))
        return false;
    if (absent)
        return true;
    acl_entry_t entry;
    errno = 0;
    int result = acl_get_entry(acl, ACL_FIRST_ENTRY, &entry);
    int entry_errno = errno;
    bool ok = result == -1 && errno == EINVAL;
    (void)directory;
    int free_rc = acl_free(acl);
    int free_errno = errno;
    if (!ok || free_rc != 0) {
        fprintf(stderr,
                "pinned-tree native ACL get_null=0 entry_result=%d "
                "entry_errno=%d free_rc=%d free_errno=%d\n",
                result, entry_errno, free_rc, free_errno);
        errno = free_errno;
    }
    return free_rc == 0 && ok;
#else
    unsigned char data[64];
    ssize_t n = getxattr(path, "system.posix_acl_access", data, sizeof(data));
    bool ok = n == -1 && errno == ENODATA;
    if (n == 28) {
        static const unsigned char tags[3] = {1, 4, 32};
        struct stat st;
        ok = data[0] == 2 && data[1] == 0 && data[2] == 0 && data[3] == 0 && stat(path, &st) == 0;
        for (size_t i = 0; i < 3 && ok; i++) {
            const unsigned char *p = data + 4 + 8 * i;
            unsigned expected = ((unsigned)st.st_mode >> (6U - 3U * (unsigned)i)) & 7U;
            ok = p[0] == tags[i] && p[1] == 0 && p[2] == expected && p[3] == 0 && p[4] == 255 &&
                 p[5] == 255 && p[6] == 255 && p[7] == 255;
        }
    }
    if (directory && ok) {
        n = getxattr(path, "system.posix_acl_default", data, sizeof(data));
        ok = (n == -1 && errno == ENODATA) || n == 0 ||
             (n == 4 && data[0] == 2 && data[1] == 0 && data[2] == 0 && data[3] == 0);
    }
    return ok;
#endif
}

static bool pt_policy(const char *path, bool directory, bool executable) {
    struct stat st;
    if (lstat(path, &st) != 0)
        return false;
    mode_t expected = directory ? 0700 : executable ? 0500 : 0400;
    return st.st_uid == geteuid() && (st.st_mode & 07777) == expected &&
           (directory ? S_ISDIR(st.st_mode) : S_ISREG(st.st_mode) && st.st_nlink == 1) &&
           pt_acl_absent(path, directory);
}

static bool pt_make_writable(const char *path) {
    return chmod(path, 0600) == 0;
}
#endif

#ifndef _WIN32
static void pt_parent_policy_diagnostic(const char *path) {
    int saved_errno = errno;
    struct stat st;
    int rc = lstat(path, &st);
    if (rc == 0)
        fprintf(stderr,
                "pinned-tree parent policy mode=%lo uid=%lu euid=%lu nlink=%lu "
                "directory=%d expected_mode=700\n",
                (unsigned long)st.st_mode, (unsigned long)st.st_uid, (unsigned long)geteuid(),
                (unsigned long)st.st_nlink, (int)S_ISDIR(st.st_mode));
    else
        fprintf(stderr, "pinned-tree parent policy lstat_rc=%d errno=%d\n", rc, errno);
    errno = saved_errno;
}
#endif

static bool pt_private_parent(pt_fixture *fx) {
    if (!pt_setup_step(
            "private-parent:create",
            th_secure_runtime_parent_new(fx->parent, sizeof(fx->parent), "pinned-parent")))
        return false;
#ifdef _WIN32
    return pt_setup_step("private-parent:native-ACL", pt_windows_acl(fx, fx->parent, false)) &&
           pt_setup_step("private-parent:native-policy", pt_policy(fx->parent, true, false));
#else
    if (!pt_setup_step("private-parent:chmod", chmod(fx->parent, 0700) == 0))
        return false;
    bool ok = pt_policy(fx->parent, true, false);
    if (!ok)
        pt_parent_policy_diagnostic(fx->parent);
    return pt_setup_step("private-parent:native-policy", ok);
#endif
}

/* A checked native iterator is the test's own no-leftover witness. */
static bool pt_parent_only_sentinel(const char *parent) {
    size_t count = 0;
    bool ok = true;
#ifdef _WIN32
    wchar_t pattern[PT_PATH];
    char joined[PT_PATH];
    if (!pt_join(joined, parent, "*") || !pt_wide(joined, pattern))
        return false;
    WIN32_FIND_DATAW entry;
    HANDLE handle = FindFirstFileW(pattern, &entry);
    if (handle == INVALID_HANDLE_VALUE)
        return false;
    do {
        if (!wcscmp(entry.cFileName, L".") || !wcscmp(entry.cFileName, L".."))
            continue;
        count++;
        if (wcscmp(entry.cFileName, L"sentinel"))
            ok = false;
    } while (FindNextFileW(handle, &entry));
    ok = GetLastError() == ERROR_NO_MORE_FILES && ok;
    return FindClose(handle) && ok && count == 1;
#else
    DIR *directory = opendir(parent);
    if (!directory)
        return false;
    for (;;) {
        errno = 0;
        struct dirent *entry = readdir(directory);
        if (!entry) {
            if (errno)
                ok = false;
            break;
        }
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
            continue;
        count++;
        if (strcmp(entry->d_name, "sentinel"))
            ok = false;
    }
    return closedir(directory) == 0 && ok && count == 1;
#endif
}

static bool pt_git(pt_fixture *fx, const char *const *tail, char *output, size_t capacity) {
    const char *argv[48] = {
        fx->git, "-C", fx->repo, "-c", "commit.gpgSign=false", "-c", "core.autocrlf=false"};
    size_t used = 7;
    for (size_t i = 0; tail[i]; i++) {
        if (used + 1 >= sizeof(argv) / sizeof(argv[0]))
            return pt_git_failed(fx, tail, "argv-capacity", -1, NULL);
        argv[used++] = tail[i];
    }
    argv[used] = NULL;
    cbm_proc_opts_t options = {.bin = fx->git,
                               .argv = argv,
                               .stdout_file = fx->capture,
                               .log_file = fx->log,
                               .quiet_timeout_ms = 10000,
                               .strip_git_repo_env = true};
    cbm_proc_result_t result = {0};
    int run_rc = cbm_subprocess_run(&options, &result);
    if (run_rc != 0 || result.outcome != CBM_PROC_CLEAN || result.exit_code != 0 ||
        !result.tree_quiesced || result.supervision_failed)
        return pt_git_failed(fx, tail, "run", run_rc, &result);
    if (!output)
        return true;
    FILE *file = cbm_fopen(fx->capture, "rb");
    if (!file || !capacity) {
        if (file)
            (void)fclose(file);
        return pt_git_failed(fx, tail, "capture-open-or-capacity", run_rc, &result);
    }
    size_t length = fread(output, 1, capacity - 1, file);
    bool ok = fgetc(file) == EOF && !ferror(file);
    output[length] = 0;
    int close_rc = fclose(file);
    if (close_rc != 0 || !ok)
        return pt_git_failed(fx, tail, "capture-read-EOF-or-close", run_rc, &result);
    return true;
}

static bool pt_oid(pt_fixture *fx, const char *const *argv, char out[65]) {
    char bytes[80];
    if (!pt_git(fx, argv, bytes, sizeof(bytes)))
        return false;
    size_t n = strlen(bytes);
    if (n && bytes[n - 1] == '\n')
        bytes[--n] = 0;
    if (n && bytes[n - 1] == '\r')
        bytes[--n] = 0;
    if (n != fx->width)
        return false;
    for (size_t i = 0; i < n; i++)
        if (!((bytes[i] >= '0' && bytes[i] <= '9') || (bytes[i] >= 'a' && bytes[i] <= 'f')))
            return false;
    memcpy(out, bytes, n + 1);
    return true;
}

static bool pt_object(pt_fixture *fx, const char *type, const void *bytes, size_t length,
                      char out[65]) {
    const char *argv[] = {"hash-object", "-w", "--no-filters", "-t", type, "--", fx->input, NULL};
    return pt_write(fx->input, bytes, length) && pt_oid(fx, argv, out);
}

static bool pt_commit(pt_fixture *fx, const char *tree, const char *one, const char *two,
                      const char *name, char out[65]) {
    char bytes[1024];
    int n = snprintf(bytes, sizeof(bytes),
                     "tree %s\n%s%s%s%s%s%s"
                     "author Tree Fixture <fixture@example.invalid> 946684800 +0000\n"
                     "committer Tree Fixture <fixture@example.invalid> 946684800 "
                     "+0000\n\n%s\n",
                     tree, one ? "parent " : "", one ? one : "", one ? "\n" : "",
                     two ? "parent " : "", two ? two : "", two ? "\n" : "", name);
    return n > 0 && (size_t)n < sizeof(bytes) && pt_object(fx, "commit", bytes, (size_t)n, out);
}

static bool pt_ref(pt_fixture *fx, const char *name, const char *oid) {
    const char *args[] = {"update-ref", name, oid, NULL};
    return pt_git(fx, args, NULL, 0);
}

static bool pt_index_tree(pt_fixture *fx, pt_row *rows, size_t count, char out[65]) {
    const char *clear[] = {"read-tree", "--empty", NULL};
    const char *write[] = {"write-tree", NULL};
    if (!pt_git(fx, clear, NULL, 0))
        return false;
    for (size_t i = 0; i < count; i++) {
        char entry[PT_PATH + 80];
        int n = snprintf(entry, sizeof(entry), "%o,%s,%s", rows[i].mode, rows[i].oid, rows[i].path);
        const char *add[] = {"update-index", "--add", "--cacheinfo", entry, NULL};
        if (n <= 0 || (size_t)n >= sizeof(entry) || !pt_git(fx, add, NULL, 0))
            return false;
    }
    return pt_oid(fx, write, out);
}

static bool pt_repo_config(pt_fixture *fx) {
    const char *keys[] = {"core.protectNTFS", "core.protectHFS", "core.ignorecase"};
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {
        const char *args[] = {"config", "--local", keys[i], "false", NULL};
        if (!pt_git(fx, args, NULL, 0))
            return false;
    }
    const char *mode[] = {"config", "--local", "core.filemode", "true", NULL};
    return pt_git(fx, mode, NULL, 0);
}

static bool pt_init(pt_fixture *fx, unsigned width) {
    memset(fx, 0, sizeof(*fx));
    cbm_arena_init(&fx->arena);
    fx->width = width;
    const char *home = th_mktempdir("cbm-pinned-tree");
    if (!home || strlen(home) >= sizeof(fx->home))
        return pt_setup_step("init:home", false);
    strcpy(fx->home, home);
    /* Owned fixture home prevents an unrelated user directory probe. */
    const char *git = cbm_find_cli("git", fx->home);
    if (!git || strlen(git) >= sizeof(fx->git))
        return pt_setup_step("init:git-resolution", false);
#ifdef _WIN32
    if (!((git[0] && git[1] == ':' && (git[2] == '/' || git[2] == '\\')) ||
          (git[0] == '\\' && git[1] == '\\')))
        return pt_setup_step("init:git-absolute-path", false);
#else
    if (git[0] != '/')
        return pt_setup_step("init:git-absolute-path", false);
#endif
    strcpy(fx->git, git);
    if (!pt_join(fx->repo, fx->home, "repo") || !pt_join(fx->capture, fx->home, "stdout") ||
        !pt_join(fx->log, fx->home, "stderr") || !pt_join(fx->input, fx->home, "input") ||
        th_mkdir_p(fx->repo) != 0)
        return pt_setup_step("init:path-and-mkdir", false);
    if (!pt_private_parent(fx))
        return pt_setup_step("init:private-parent", false);
    const char *init[] = {"-c",
                          "init.templateDir=",
                          "init",
                          "--quiet",
                          width == 40 ? "--object-format=sha1" : "--object-format=sha256",
                          "--initial-branch=topic",
                          NULL};
    char sentinel[PT_PATH];
    return pt_setup_step("init:git-init", pt_git(fx, init, NULL, 0)) &&
           pt_setup_step("init:git-config", pt_repo_config(fx)) &&
           pt_setup_step("init:sentinel-path", pt_join(sentinel, fx->parent, "sentinel")) &&
           pt_setup_step("init:sentinel-write", pt_write(sentinel, "parent-owned\n", 13)) &&
           pt_setup_step("init:sentinel-audit", pt_parent_only_sentinel(fx->parent));
}

static bool pt_populate(pt_fixture *fx) {
    fx->large = cbm_arena_alloc(&fx->arena, PT_LARGE);
    if (!fx->large)
        return false;
    for (size_t i = 0; i < PT_LARGE; i++)
        fx->large[i] = (unsigned char)(i % 251U);
    fx->rows[0] = (pt_row){
        .path = "binary.bin", .mode = 0100644, .bytes = pt_binary, .length = sizeof(pt_binary)};
    fx->rows[1] = (pt_row){
        .path = "copy.bin", .mode = 0100644, .bytes = pt_shared, .length = sizeof(pt_shared) - 1};
    fx->rows[2] =
        (pt_row){.path = "empty", .mode = 0100644, .bytes = (const unsigned char *)"", .length = 0};
    fx->rows[3] =
        (pt_row){.path = "large", .mode = 0100644, .bytes = fx->large, .length = PT_LARGE};
    fx->rows[4] = (pt_row){.path = "nested/source.c",
                           .mode = 0100644,
                           .bytes = pt_after,
                           .length = sizeof(pt_after) - 1};
    fx->rows[5] = (pt_row){
        .path = "run", .mode = 0100755, .bytes = pt_shared, .length = sizeof(pt_shared) - 1};
    fx->count = 6;
    for (size_t i = 0; i < fx->count; i++)
        if (!pt_object(fx, "blob", fx->rows[i].bytes, fx->rows[i].length, fx->rows[i].oid))
            return false;
    pt_row ancestor[PT_ROWS];
    memcpy(ancestor, fx->rows, sizeof(ancestor));
    ancestor[4].bytes = pt_before;
    ancestor[4].length = sizeof(pt_before) - 1;
    if (!pt_object(fx, "blob", pt_before, sizeof(pt_before) - 1, ancestor[4].oid))
        return false;
    strcpy(fx->before_oid, ancestor[4].oid);
    char empty[65], before[65], after[65];
    return pt_index_tree(fx, NULL, 0, empty) && pt_commit(fx, empty, NULL, NULL, "A", fx->a) &&
           pt_commit(fx, empty, fx->a, NULL, "P", fx->p) &&
           pt_index_tree(fx, ancestor, fx->count, before) &&
           pt_commit(fx, before, fx->a, NULL, "Q", fx->q) &&
           pt_index_tree(fx, fx->rows, fx->count, after) &&
           pt_commit(fx, after, fx->p, fx->q, "H", fx->head) &&
           pt_commit(fx, before, fx->q, NULL, "B", fx->base) &&
           pt_ref(fx, "refs/heads/topic", fx->head) && pt_ref(fx, "refs/heads/base", fx->base);
}

static bool pt_open_facts(pt_fixture *fx) {
    cbm_git_facts_options_t options = {.root = fx->repo,
                                       .base_ref = "refs/heads/base",
                                       .git_executable = fx->git,
                                       .expected_head = fx->head,
                                       .deadline_ms = cbm_now_ms() + 120000U,
                                       .command_limit = 4096,
                                       .stdout_limit = 4 * 1024 * 1024,
                                       .stderr_limit = 65536,
                                       .total_output_limit = 64 * 1024 * 1024};
    cbm_git_facts_error_t error;
    fx->facts = cbm_git_facts_open(&options, &error);
    const cbm_git_facts_identity_t *id = cbm_git_facts_identity(fx->facts);
    return fx->facts && error.status == CBM_GIT_FACTS_OK && id && id->oid_hex_length == fx->width &&
           !strcmp(id->head, fx->head) && !strcmp(id->base, fx->base) &&
           !strcmp(id->merge_base, fx->q);
}

static cbm_pinned_tree_options_t pt_options(pt_fixture *fx, cbm_git_revision_t revision) {
    return (cbm_pinned_tree_options_t){
        .facts = fx->facts,
        .revision = revision,
        .private_parent = fx->parent,
        .limits = {.max_files = 64,
                   .max_directories = 64,
                   .max_total_content_bytes = 4 * 1024 * 1024,
                   .max_relative_path_bytes = 1024,
                   .max_arena_bytes = 16 * 1024 * 1024,
                   .blob_batch = {.max_entries = 64,
                                  .max_input_bytes = 16384,
                                  .max_arena_bytes = 16 * 1024 * 1024}},
        .control = {.deadline_ms = cbm_now_ms() + 120000U}};
}

static bool pt_close_one(cbm_pinned_tree_t **tree) {
    if (!*tree)
        return true;
    cbm_pinned_tree_error_t error;
    return cbm_pinned_tree_close(tree, &error) == CBM_PINNED_TREE_OK && !*tree;
}

static int pt_finish(pt_fixture *fx, int result) {
    bool closed = pt_close_one(&fx->tree) && pt_close_one(&fx->second);
    cbm_git_facts_free(fx->facts);
    if (!closed) {
        fprintf(stderr, "pinned-tree fixture retained for disposition: %s\n", fx->parent);
        return 1;
    }
    if (fx->parent[0] && th_rmtree(fx->parent) != 0)
        result = 1;
    if (fx->home[0] && th_rmtree(fx->home) != 0)
        result = 1;
    cbm_arena_destroy(&fx->arena);
    return result;
}

static size_t pt_total(const pt_fixture *fx) {
    size_t total = 0;
    for (size_t i = 0; i < fx->count; i++)
        total += fx->rows[i].length;
    return total;
}

static bool pt_fact_control(pt_fixture *fx) {
    cbm_git_tree_inventory_t inventory;
    cbm_git_facts_error_t error;
    if (!cbm_git_facts_inventory(fx->facts, CBM_GIT_REV_HEAD, &inventory, &error) ||
        inventory.count != fx->count)
        return false;
    for (size_t i = 0; i < fx->count; i++) {
        const cbm_git_tree_entry_t *entry = &inventory.entries[i];
        if (entry->path_length != strlen(fx->rows[i].path) ||
            memcmp(entry->path, fx->rows[i].path, entry->path_length) ||
            entry->mode != fx->rows[i].mode || strcmp(entry->oid, fx->rows[i].oid))
            return false;
    }
    cbm_git_blob_t blob;
    return cbm_git_facts_read_blob(fx->facts, CBM_GIT_REV_HEAD, "binary.bin", 10, &blob, &error) ==
               CBM_GIT_BLOB_FOUND &&
           blob.bytes.length == sizeof(pt_binary) &&
           !memcmp(blob.bytes.data, pt_binary, sizeof(pt_binary));
}

static void pt_hash(const void *bytes, size_t length, unsigned char out[32]) {
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    cbm_sha256_update(&hash, bytes, length);
    cbm_sha256_final(&hash, out);
}

static bool pt_view_matches(pt_fixture *fx, const cbm_pinned_tree_view_t *view, bool ancestor) {
    if (!view || view->file_count != fx->count || view->directory_count != 2 ||
        view->total_content_bytes != pt_total(fx) || !view->identity ||
        strcmp(view->commit, ancestor ? fx->q : fx->head) ||
        view->revision != (ancestor ? CBM_GIT_REV_MERGE_BASE : CBM_GIT_REV_HEAD) ||
        !pt_policy(view->root, true, false))
        return false;
    for (size_t i = 0; i < fx->count; i++) {
        const cbm_pinned_tree_file_t *row = &view->files[i];
        const unsigned char *bytes = ancestor && i == 4 ? pt_before : fx->rows[i].bytes;
        unsigned char digest[32];
        char path[PT_PATH];
        pt_hash(bytes, fx->rows[i].length, digest);
        if (row->path_length != strlen(fx->rows[i].path) ||
            memcmp(row->path, fx->rows[i].path, row->path_length + 1) ||
            row->git_mode != fx->rows[i].mode || row->content_length != fx->rows[i].length ||
            memcmp(row->content_sha256, digest, 32) ||
            !pt_join(path, view->root, fx->rows[i].path) ||
            !pt_equal_file(path, bytes, fx->rows[i].length) ||
            !pt_policy(path, false, fx->rows[i].mode == 0100755))
            return false;
        if (strcmp(row->oid, ancestor && i == 4 ? fx->before_oid : fx->rows[i].oid))
            return false;
    }
    char nested[PT_PATH];
    return pt_join(nested, view->root, "nested") && pt_policy(nested, true, false);
}

typedef struct {
    unsigned char bytes[8192];
    size_t used;
    bool ok;
} pt_wire;

static void pt_wbytes(pt_wire *wire, const void *bytes, size_t length) {
    if (!wire->ok || length > sizeof(wire->bytes) - wire->used) {
        wire->ok = false;
        return;
    }
    if (length)
        memcpy(wire->bytes + wire->used, bytes, length);
    wire->used += length;
}

static void pt_wnumber(pt_wire *wire, uint64_t number, size_t width) {
    unsigned char bytes[8];
    for (size_t i = 0; i < width; i++)
        bytes[width - 1 - i] = (unsigned char)(number >> (i * 8));
    pt_wbytes(wire, bytes, width);
}

static void pt_wstring(pt_wire *wire, const char *value) {
    size_t length = strlen(value);
    pt_wnumber(wire, length, 8);
    pt_wbytes(wire, value, length);
}

/* Expected transcript consumes only independently constructed fixture rows and
 * the already checked Git facts identity; no materializer encoder/view input.
 */
static bool pt_expected_digest(const cbm_git_facts_identity_t *identity, const pt_row *rows,
                               size_t count, size_t directories, cbm_git_revision_t revision,
                               unsigned char out[32]) {
    pt_wire wire = {.ok = true};
    static const char domain[] = "cbm.pinned-tree.v1";
    const char *selected = revision == CBM_GIT_REV_HEAD ? identity->head : identity->merge_base;
    uint64_t total = 0;
    for (size_t i = 0; i < count; i++)
        total += rows[i].length;
    pt_wbytes(&wire, domain, sizeof(domain));
    pt_wnumber(&wire, identity->oid_hex_length == 40 ? 1 : 2, 1);
    pt_wstring(&wire, identity->root);
    pt_wstring(&wire, identity->git_dir);
    pt_wstring(&wire, identity->common_dir);
    pt_wstring(&wire, identity->head);
    pt_wstring(&wire, identity->base);
    pt_wstring(&wire, identity->merge_base);
    pt_wnumber(&wire, revision == CBM_GIT_REV_HEAD ? 1 : 2, 1);
    pt_wstring(&wire, selected);
    pt_wnumber(&wire, count, 8);
    pt_wnumber(&wire, directories, 8);
    pt_wnumber(&wire, total, 8);
    for (size_t i = 0; i < count; i++) {
        unsigned char content[32];
        pt_hash(rows[i].bytes, rows[i].length, content);
        pt_wnumber(&wire, 'F', 1);
        pt_wstring(&wire, rows[i].path);
        pt_wnumber(&wire, rows[i].mode, 4);
        pt_wstring(&wire, rows[i].oid);
        pt_wnumber(&wire, rows[i].length, 8);
        pt_wbytes(&wire, content, sizeof(content));
    }
    if (wire.ok)
        pt_hash(wire.bytes, wire.used, out);
    return wire.ok;
}

static void pt_hex(const unsigned char digest[32], char out[65]) {
    static const char digits[] = "0123456789abcdef";
    for (size_t i = 0; i < 32; i++) {
        out[2 * i] = digits[digest[i] >> 4];
        out[2 * i + 1] = digits[digest[i] & 15];
    }
    out[64] = 0;
}

static bool pt_golden_control(void) {
    cbm_git_facts_identity_t id = {
        .root = "/repo", .git_dir = "/repo/.git", .common_dir = "/repo/.git", .oid_hex_length = 40};
    memset(id.head, '1', 40);
    memset(id.base, '2', 40);
    memset(id.merge_base, '3', 40);
    pt_row row = {.path = "a",
                  .mode = 0100644,
                  .bytes = (const unsigned char *)"",
                  .length = 0,
                  .oid = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"};
    unsigned char digest[32];
    char hex[65];
    if (!pt_expected_digest(&id, &row, 1, 1, CBM_GIT_REV_HEAD, digest))
        return false;
    pt_hex(digest, hex);
    return !strcmp(hex, "cde0e3e17a8263cd24439742f6af0f265217607aa7b1bd8b912fcad055e05e46");
}

static bool pt_independent_files(const char *root) {
    char first[PT_PATH], second[PT_PATH];
    if (!pt_join(first, root, "copy.bin") || !pt_join(second, root, "run"))
        return false;
#ifdef _WIN32
    wchar_t a[PT_PATH], b[PT_PATH];
    if (!pt_wide(first, a) || !pt_wide(second, b))
        return false;
    HANDLE one =
        CreateFileW(a, FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    HANDLE two =
        CreateFileW(b, FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    BY_HANDLE_FILE_INFORMATION ia, ib;
    bool ok = one != INVALID_HANDLE_VALUE && two != INVALID_HANDLE_VALUE &&
              GetFileInformationByHandle(one, &ia) && GetFileInformationByHandle(two, &ib) &&
              ia.nNumberOfLinks == 1 && ib.nNumberOfLinks == 1 &&
              (ia.nFileIndexHigh != ib.nFileIndexHigh || ia.nFileIndexLow != ib.nFileIndexLow ||
               ia.dwVolumeSerialNumber != ib.dwVolumeSerialNumber);
    if (one != INVALID_HANDLE_VALUE)
        ok = CloseHandle(one) && ok;
    if (two != INVALID_HANDLE_VALUE)
        ok = CloseHandle(two) && ok;
    return ok;
#else
    struct stat a, b;
    return lstat(first, &a) == 0 && lstat(second, &b) == 0 && a.st_nlink == 1 && b.st_nlink == 1 &&
           (a.st_dev != b.st_dev || a.st_ino != b.st_ino);
#endif
}

static int pt_pinned_round(unsigned width) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, width)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)));
    PT_CHECK(pt_setup_step("setup:control", pt_fact_control(&fx)));
    const cbm_git_facts_identity_t *id = cbm_git_facts_identity(fx.facts);
    PT_CHECK(strcmp(id->merge_base, fx.p) != 0);
    unsigned char expected_head[32], expected_merge[32];
    pt_row ancestor_rows[PT_ROWS];
    memcpy(ancestor_rows, fx.rows, sizeof(ancestor_rows));
    ancestor_rows[4].bytes = pt_before;
    strcpy(ancestor_rows[4].oid, fx.before_oid);
    PT_CHECK(pt_expected_digest(id, fx.rows, fx.count, 2, CBM_GIT_REV_HEAD, expected_head));
    PT_CHECK(
        pt_expected_digest(id, ancestor_rows, fx.count, 2, CBM_GIT_REV_MERGE_BASE, expected_merge));
    char *original_root = cbm_arena_strdup(&fx.arena, id->root);
    char *original_git = cbm_arena_strdup(&fx.arena, id->git_dir);
    char *original_common = cbm_arena_strdup(&fx.arena, id->common_dir);
    PT_CHECK(original_root && original_git && original_common);
    const char *empty_index[] = {"read-tree", "--empty", NULL};
    char dirty[PT_PATH];
    PT_CHECK(pt_ref(&fx, "refs/heads/topic", fx.a) && pt_ref(&fx, "refs/heads/base", fx.a));
    PT_CHECK(pt_git(&fx, empty_index, NULL, 0) && pt_join(dirty, fx.repo, "binary.bin"));
    PT_CHECK(pt_write(dirty, "dirty", 5));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    char parent_copy[PT_PATH];
    strcpy(parent_copy, fx.parent);
    options.private_parent = parent_copy;
    cbm_pinned_tree_error_t error;
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    memset(parent_copy, 0xa5, sizeof(parent_copy));
    PT_CHECK(pt_view_matches(&fx, cbm_pinned_tree_view(fx.tree), false));
    PT_CHECK(pt_independent_files(cbm_pinned_tree_view(fx.tree)->root));
    options = pt_options(&fx, CBM_GIT_REV_MERGE_BASE);
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.second, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_view_matches(&fx, cbm_pinned_tree_view(fx.second), true));
    PT_CHECK(!memcmp(cbm_pinned_tree_view(fx.tree)->manifest_sha256, expected_head, 32));
    PT_CHECK(!memcmp(cbm_pinned_tree_view(fx.second)->manifest_sha256, expected_merge, 32));
    PT_CHECK(memcmp(expected_head, expected_merge, 32) != 0);
    cbm_git_facts_free(fx.facts);
    fx.facts = NULL;
    memset(&options, 0xa5, sizeof(options));
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(fx.tree);
    PT_CHECK(view && !strcmp(view->identity->root, original_root) &&
             !strcmp(view->identity->git_dir, original_git) &&
             !strcmp(view->identity->common_dir, original_common) &&
             !strcmp(view->identity->head, fx.head) && !strcmp(view->identity->base, fx.base));
    cbm_pinned_tree_control_t control = {.deadline_ms = cbm_now_ms() + 120000U};
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &control, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_view_matches(&fx, cbm_pinned_tree_view(fx.tree), false));
    PT_CHECK(pt_close_one(&fx.tree) && pt_close_one(&fx.second));
    PT_CHECK(pt_parent_only_sentinel(fx.parent) && pt_equal_file(dirty, "dirty", 5));
    result = 0;
done:
    return pt_finish(&fx, result);
}

TEST(tree_a_pinned_head_merge_base_and_owned_lifetime) {
    ASSERT_EQ(pt_pinned_round(40), 0);
    ASSERT_EQ(pt_pinned_round(64), 0);
    PASS();
}

TEST(tree_b_manifest_golden_and_empty_inventory) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(pt_golden_control());
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, 40)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)));
    PT_CHECK(pt_setup_step("setup:control", pt_fact_control(&fx)));
    unsigned char expected[32];
    PT_CHECK(pt_expected_digest(cbm_git_facts_identity(fx.facts), fx.rows, fx.count, 2,
                                CBM_GIT_REV_HEAD, expected));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    cbm_pinned_tree_error_t error;
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.second, &error) == CBM_PINNED_TREE_OK);
    const cbm_pinned_tree_view_t *one = cbm_pinned_tree_view(fx.tree);
    const cbm_pinned_tree_view_t *two = cbm_pinned_tree_view(fx.second);
    PT_CHECK(one && two && strcmp(one->root, two->root) &&
             !memcmp(one->manifest_sha256, expected, 32) &&
             !memcmp(two->manifest_sha256, expected, 32));
    PT_CHECK(pt_close_one(&fx.tree) && pt_close_one(&fx.second));
    cbm_git_facts_free(fx.facts);
    fx.facts = NULL;
    strcpy(fx.head, fx.a);
    strcpy(fx.base, fx.a);
    strcpy(fx.q, fx.a);
    PT_CHECK(pt_ref(&fx, "refs/heads/topic", fx.a) && pt_ref(&fx, "refs/heads/base", fx.a));
    PT_CHECK(pt_open_facts(&fx));
    cbm_git_tree_inventory_t inventory;
    cbm_git_facts_error_t git_error;
    PT_CHECK(cbm_git_facts_inventory(fx.facts, CBM_GIT_REV_HEAD, &inventory, &git_error));
    PT_CHECK(!inventory.entries && inventory.count == 0);
    PT_CHECK(pt_expected_digest(cbm_git_facts_identity(fx.facts), NULL, 0, 1, CBM_GIT_REV_HEAD,
                                expected));
    options = pt_options(&fx, CBM_GIT_REV_HEAD);
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    one = cbm_pinned_tree_view(fx.tree);
    PT_CHECK(one && !one->files && one->file_count == 0 && one->directory_count == 1 &&
             one->total_content_bytes == 0 && !memcmp(one->manifest_sha256, expected, 32));
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &options.control, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_close_one(&fx.tree) && pt_parent_only_sentinel(fx.parent));
    result = 0;
done:
    return pt_finish(&fx, result);
}

static unsigned char pt_unhex(char c) {
    return (unsigned char)(c <= '9' ? c - '0' : c - 'a' + 10);
}

/* Raw Git tree objects allow Windows-inexpressible Git names without creating
 * them in the fixture working tree. Caller supplies Git's canonical flat order.
 */
static bool pt_flat_tree(pt_fixture *fx, pt_row *rows, size_t count, char out[65]) {
    unsigned char bytes[8192];
    size_t used = 0;
    for (size_t i = 0; i < count; i++) {
        char mode[16];
        int n = snprintf(mode, sizeof(mode), "%o ", rows[i].mode);
        size_t length = strlen(rows[i].path);
        if (n <= 0 || used + (size_t)n + length + 1 + fx->width / 2 > sizeof(bytes))
            return false;
        memcpy(bytes + used, mode, (size_t)n);
        used += (size_t)n;
        memcpy(bytes + used, rows[i].path, length + 1);
        used += length + 1;
        for (size_t j = 0; j < fx->width; j += 2)
            bytes[used++] =
                (unsigned char)((pt_unhex(rows[i].oid[j]) << 4) | pt_unhex(rows[i].oid[j + 1]));
    }
    return pt_object(fx, "tree", bytes, used, out);
}

static bool pt_replace_tree(pt_fixture *fx, pt_row *rows, size_t count) {
    cbm_git_facts_free(fx->facts);
    fx->facts = NULL;
    char tree[65];
    if (!pt_flat_tree(fx, rows, count, tree) ||
        !pt_commit(fx, tree, fx->a, NULL, "special", fx->head))
        return false;
    strcpy(fx->base, fx->head);
    strcpy(fx->q, fx->head);
    if (!pt_ref(fx, "refs/heads/topic", fx->head) || !pt_ref(fx, "refs/heads/base", fx->head) ||
        !pt_open_facts(fx))
        return false;
    cbm_git_tree_inventory_t inventory;
    cbm_git_facts_error_t error;
    if (!cbm_git_facts_inventory(fx->facts, CBM_GIT_REV_HEAD, &inventory, &error) ||
        inventory.count != count)
        return false;
    for (size_t i = 0; i < count; i++)
        if (strcmp(inventory.entries[i].path, rows[i].path) ||
            inventory.entries[i].mode != rows[i].mode)
            return false;
    return true;
}

static bool pt_clean_failure(pt_fixture *fx, cbm_pinned_tree_options_t *options,
                             cbm_pinned_tree_status_t expected) {
    cbm_pinned_tree_error_t error;
    memset(&error, 0xa5, sizeof(error));
    cbm_pinned_tree_status_t result = cbm_pinned_tree_create(options, &fx->tree, &error);
    return result == expected && error.status == expected && error.cause == expected && !fx->tree &&
           pt_parent_only_sentinel(fx->parent);
}

#ifndef _WIN32
/* Checked native spelling witness; 0 exact,1 changed spelling,-1 I/O. */
static int pt_posix_names(const char *root, const char *first, const char *second) {
    DIR *dir = opendir(root);
    if (!dir)
        return -1;
    bool seen_first = false, seen_second = second == NULL, ok = true;
    for (;;) {
        errno = 0;
        struct dirent *entry = readdir(dir);
        if (!entry) {
            ok = errno == 0;
            break;
        }
        if (!strcmp(entry->d_name, first))
            seen_first = true;
        if (second && !strcmp(entry->d_name, second))
            seen_second = true;
    }
    if (closedir(dir) != 0)
        ok = false;
    if (!ok)
        return -1;
    return seen_first && seen_second ? 0 : 1;
}

/* Native refusal/spelling is observed separately from the feature result. */
static int pt_raw_name_support(pt_fixture *fx, const char *name) {
    char root[PT_PATH], path[PT_PATH];
    if (!pt_join(root, fx->parent, "raw-probe") || cbm_mkdir(root) != 0 ||
        !pt_join(path, root, name))
        return -1;
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    int result = -1;
    if (fd < 0) {
        if (errno == EINVAL || errno == EILSEQ || errno == ENAMETOOLONG)
            result = 1;
    } else if (close(fd) == 0)
        result = pt_posix_names(root, name, NULL);
    if (th_rmtree(root) != 0)
        result = -1;
    return result;
}
#endif

TEST(tree_c_complete_inventory_and_native_names) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, 40)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)) &&
             pt_setup_step("setup:control", pt_fact_control(&fx)));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    cbm_pinned_tree_error_t error;
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_close_one(&fx.tree));
    pt_row rows[2] = {fx.rows[0], fx.rows[0]};
    /* A symlink or a submodule link is left out and counted, as discovery
     * never indexes a symlink; the regular file beside it is pinned. The link
     * sorts first (rows are in git's order), so the pinned file is not at its
     * position in git's own list. */
    rows[0].path = "a-link";
    rows[1].path = "good";
    const uint32_t modes[] = {0120000, 0160000};
    for (size_t i = 0; i < 2; i++) {
        rows[0].mode = modes[i];
        strcpy(rows[0].oid, i == 0 ? fx.rows[0].oid : fx.a);
        PT_CHECK(pt_replace_tree(&fx, rows, 2));
        options = pt_options(&fx, CBM_GIT_REV_HEAD);
        PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
        const cbm_pinned_tree_view_t *linked = cbm_pinned_tree_view(fx.tree);
        PT_CHECK(linked && linked->file_count == 1 && linked->skipped_link_count == 1 &&
                 strcmp((const char *)linked->files[0].path, "good") == 0);
        PT_CHECK(pt_close_one(&fx.tree));
    }
    rows[1] = fx.rows[0];
#ifdef _WIN32
    /* WHY: these are Win32 encoding/device restrictions. POSIX raw-name cases
     * below are the tried positive counterpart; no platform is silently skipped.
     */
    const char *names[] = {"CON.txt", "LPT1",      "bad:stream", "bad\\slash", "bad.",
                           "bad ",    "bad\tname", "bad*name",   "bad\xff",    "COM\xc2\xb9.txt"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        pt_row one = fx.rows[0];
        one.path = names[i];
        PT_CHECK(pt_replace_tree(&fx, &one, 1));
        options = pt_options(&fx, CBM_GIT_REV_HEAD);
        PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_UNSUPPORTED));
    }
#else
    const char *names[] = {"-dash",      ":colon",    "[glob]*", "back\\slash",
                           "line\nname", "tab\tname", "z\xff"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        int support = pt_raw_name_support(&fx, names[i]);
        PT_CHECK(support >= 0);
        fprintf(stderr, "pinned-tree POSIX raw-name native support=%d at case%zu\n", support, i);
        pt_row one = fx.rows[0];
        one.path = names[i];
        PT_CHECK(pt_replace_tree(&fx, &one, 1));
        options = pt_options(&fx, CBM_GIT_REV_HEAD);
        if (support != 0) {
            PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_UNSUPPORTED));
            continue;
        }
        PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
        const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(fx.tree);
        char path[PT_PATH];
        PT_CHECK(view && view->file_count == 1 && view->files[0].path_length == strlen(names[i]) &&
                 !memcmp(view->files[0].path, names[i], strlen(names[i]) + 1));
        PT_CHECK(pt_join(path, view->root, names[i]) &&
                 pt_equal_file(path, pt_binary, sizeof(pt_binary)));
        PT_CHECK(pt_close_one(&fx.tree));
    }
#endif
    PT_CHECK(pt_parent_only_sentinel(fx.parent));
    result = 0;
done:
    return pt_finish(&fx, result);
}

/* 0: both exact names; 1: native alias; 2: spelling changed; -1: setup error.
 */
static int pt_native_pair(pt_fixture *fx, const char *first, const char *second) {
    char root[PT_PATH], a[PT_PATH], b[PT_PATH];
    if (!pt_join(root, fx->parent, "native-probe") || cbm_mkdir(root) != 0 ||
        !pt_join(a, root, first) || !pt_join(b, root, second))
        return -1;
    int result = -1;
#ifdef _WIN32
    wchar_t wa[PT_PATH], wb[PT_PATH];
    if (!pt_wide(a, wa) || !pt_wide(b, wb))
        goto cleanup;
    HANDLE one = CreateFileW(wa, GENERIC_WRITE, 0, NULL, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, NULL);
    if (one == INVALID_HANDLE_VALUE)
        goto cleanup;
    if (!CloseHandle(one))
        goto cleanup;
    HANDLE two = CreateFileW(wb, GENERIC_WRITE, 0, NULL, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, NULL);
    if (two == INVALID_HANDLE_VALUE) {
        DWORD error = GetLastError();
        if (error == ERROR_FILE_EXISTS || error == ERROR_ALREADY_EXISTS)
            result = 1;
    } else if (CloseHandle(two))
        result = 0;
#else
    int one = open(a, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (one < 0)
        goto cleanup;
    if (close(one) != 0)
        goto cleanup;
    int two = open(b, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (two < 0) {
        if (errno == EEXIST)
            result = 1;
    } else if (close(two) == 0)
        result = 0;
    if (result == 0) {
        int spelling = pt_posix_names(root, first, second);
        result = spelling < 0 ? -1 : spelling == 0 ? 0 : 2;
    }
#endif
cleanup:
    if (th_rmtree(root) != 0)
        result = -1;
    return result;
}

static bool pt_pair_case(pt_fixture *fx, const char *first, const char *second) {
    int native = pt_native_pair(fx, first, second);
    if (native < 0 || !pt_parent_only_sentinel(fx->parent))
        return false;
    fprintf(stderr, "pinned-tree native pair classification=%d for %s / %s\n", native, first,
            second);
    pt_row rows[2] = {fx->rows[0], fx->rows[1]};
    rows[0].path = first;
    rows[1].path = second;
    if (strcmp(first, second) > 0) {
        pt_row temporary = rows[0];
        rows[0] = rows[1];
        rows[1] = temporary;
    }
    if (!pt_replace_tree(fx, rows, 2))
        return false;
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    if (native != 0)
        return pt_clean_failure(
            fx, &options, native == 1 ? CBM_PINNED_TREE_COLLISION : CBM_PINNED_TREE_UNSUPPORTED);
    cbm_pinned_tree_error_t error;
    if (cbm_pinned_tree_create(&options, &fx->tree, &error) != CBM_PINNED_TREE_OK)
        return false;
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(fx->tree);
    bool ok = view && view->file_count == 2;
    for (size_t i = 0; i < 2 && ok; i++) {
        char path[PT_PATH];
        ok = !strcmp((const char *)view->files[i].path, rows[i].path) &&
             pt_join(path, view->root, rows[i].path) &&
             pt_equal_file(path, rows[i].bytes, rows[i].length);
    }
    return pt_close_one(&fx->tree) && ok;
}

#ifdef _WIN32
static bool pt_short_alias_case(pt_fixture *fx) {
    static const char long_name[] = "Long fixture filename for alias.bin";
    char root[PT_PATH], path[PT_PATH];
    wchar_t wide[PT_PATH], short_path[PT_PATH];
    if (!pt_join(root, fx->parent, "short-probe") || cbm_mkdir(root) != 0 ||
        !pt_join(path, root, long_name) || !pt_write(path, "probe", 5) || !pt_wide(path, wide))
        return false;
    DWORD length = GetShortPathNameW(wide, short_path, PT_PATH);
    if (!length || length >= PT_PATH) {
        (void)th_rmtree(root);
        return false;
    }
    const wchar_t *leaf = short_path;
    for (const wchar_t *p = short_path; *p; p++)
        if (*p == L'\\' || *p == L'/')
            leaf = p + 1;
    char alias[512];
    int encoded = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, leaf, -1, alias, sizeof(alias),
                                      NULL, NULL);
    if (th_rmtree(root) != 0 || encoded <= 0)
        return false;
    if (!strcmp(alias, long_name)) {
        fprintf(stderr, "pinned-tree alias applicability: native volume allocated "
                        "no short alias; "
                        "WHY no distinct alias exists; tried GetShortPathNameW on "
                        "an actual created file\n");
        return true;
    }
    pt_row rows[2] = {fx->rows[0], fx->rows[1]};
    rows[0].path = long_name;
    rows[1].path = alias;
    if (strcmp(rows[0].path, rows[1].path) > 0) {
        pt_row swap = rows[0];
        rows[0] = rows[1];
        rows[1] = swap;
    }
    if (!pt_replace_tree(fx, rows, 2))
        return false;
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    cbm_pinned_tree_error_t error;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_create(&options, &fx->tree, &error);
    if (status == CBM_PINNED_TREE_COLLISION)
        return !fx->tree && error.status == status && pt_parent_only_sentinel(fx->parent);
    if (status != CBM_PINNED_TREE_OK)
        return false;
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(fx->tree);
    bool ok = view && view->file_count == 2;
    for (size_t i = 0; i < 2 && ok; i++)
        ok = !strcmp((const char *)view->files[i].path, rows[i].path) &&
             pt_join(path, view->root, rows[i].path) &&
             pt_equal_file(path, rows[i].bytes, rows[i].length);
    /* Creation order may reserve the explicit short-looking name first, making
     * Git's two spellings independently representable. Both must then retain
     * different expected bytes, so accidental alias reuse cannot pass. */
    return pt_close_one(&fx->tree) && ok;
}
#endif

TEST(tree_d_native_collision_and_exact_spelling) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, 40)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)) &&
             pt_setup_step("setup:control", pt_fact_control(&fx)));
    PT_CHECK(pt_pair_case(&fx, "exact-one", "exact-two"));
    PT_CHECK(pt_pair_case(&fx, "Case", "case"));
#ifdef _WIN32
    PT_CHECK(pt_short_alias_case(&fx));
#endif
    PT_CHECK(pt_pair_case(&fx, "e\xcc\x81", "\xc3\xa9"));
    /* The same observed case rule also applies when an expected file would
     * otherwise reuse the native directory of a different Git prefix. */
    int native = pt_native_pair(&fx, "A", "a");
    PT_CHECK(native == 0 || native == 1);
    cbm_git_facts_free(fx.facts);
    fx.facts = NULL;
    pt_row rows[2] = {fx.rows[0], fx.rows[1]};
    rows[0].path = "A";
    rows[1].path = "a/leaf";
    char tree[65];
    PT_CHECK(pt_index_tree(&fx, rows, 2, tree) &&
             pt_commit(&fx, tree, fx.a, NULL, "prefix", fx.head));
    strcpy(fx.base, fx.head);
    strcpy(fx.q, fx.head);
    PT_CHECK(pt_ref(&fx, "refs/heads/topic", fx.head) && pt_ref(&fx, "refs/heads/base", fx.head));
    PT_CHECK(pt_open_facts(&fx));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    cbm_pinned_tree_error_t error;
    if (native == 1)
        PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_COLLISION));
    else {
        PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
        PT_CHECK(cbm_pinned_tree_view(fx.tree)->file_count == 2);
        PT_CHECK(pt_close_one(&fx.tree));
    }
    PT_CHECK(pt_parent_only_sentinel(fx.parent));
    result = 0;
done:
    return pt_finish(&fx, result);
}

#ifndef _WIN32
static bool pt_passing_posix_mode(const char *path, mode_t mode) {
    struct stat st;
    return stat(path, &st) == 0 && st.st_uid == geteuid() && (st.st_mode & 07777) == mode;
}
#ifdef __APPLE__
static bool pt_mac_named_acl(const char *path, bool install) {
    acl_t acl = acl_init(1);
    if (!acl)
        return false;
    bool ok = true;
    if (install) {
        struct passwd *principal = getpwnam("nobody");
        uuid_t uuid;
        acl_entry_t entry;
        acl_permset_t permissions;
        acl_flagset_t flags;
        ok = principal && principal->pw_uid != geteuid() &&
             mbr_uid_to_uuid(principal->pw_uid, uuid) == 0 && acl_create_entry(&acl, &entry) == 0 &&
             acl_set_tag_type(entry, ACL_EXTENDED_ALLOW) == 0 &&
             acl_set_qualifier(entry, uuid) == 0 && acl_get_permset(entry, &permissions) == 0 &&
             acl_clear_perms(permissions) == 0 &&
             acl_add_perm(permissions, ACL_LIST_DIRECTORY) == 0 &&
             acl_add_perm(permissions, ACL_SEARCH) == 0 &&
             acl_set_permset(entry, permissions) == 0 && acl_get_flagset_np(entry, &flags) == 0 &&
             acl_clear_flags_np(flags) == 0 &&
             acl_add_flag_np(flags, ACL_ENTRY_FILE_INHERIT) == 0 &&
             acl_add_flag_np(flags, ACL_ENTRY_DIRECTORY_INHERIT) == 0 &&
             acl_set_flagset_np(entry, flags) == 0;
    }
    if (ok)
        ok = acl_set_file(path, ACL_TYPE_EXTENDED, acl) == 0;
    return acl_free(acl) == 0 && ok;
}

static bool pt_mac_named_observed(const char *path) {
    struct passwd *principal = getpwnam("nobody");
    uuid_t expected;
    if (!principal || principal->pw_uid == geteuid() ||
        mbr_uid_to_uuid(principal->pw_uid, expected) != 0)
        return false;
    acl_t acl = acl_get_file(path, ACL_TYPE_EXTENDED);
    if (!acl)
        return false;
    acl_entry_t entry;
    acl_tag_t tag;
    acl_permset_t permissions;
    acl_flagset_t flags;
    bool ok =
        acl_get_entry(acl, ACL_FIRST_ENTRY, &entry) == 0 && acl_get_tag_type(entry, &tag) == 0 &&
        tag == ACL_EXTENDED_ALLOW && acl_get_permset(entry, &permissions) == 0 &&
        acl_get_perm_np(permissions, ACL_LIST_DIRECTORY) == 1 &&
        acl_get_perm_np(permissions, ACL_SEARCH) == 1 && acl_get_flagset_np(entry, &flags) == 0 &&
        acl_get_flag_np(flags, ACL_ENTRY_FILE_INHERIT) == 1 &&
        acl_get_flag_np(flags, ACL_ENTRY_DIRECTORY_INHERIT) == 1;
    void *qualifier = ok ? acl_get_qualifier(entry) : NULL;
    ok = ok && qualifier && !memcmp(qualifier, expected, sizeof(expected));
    if (qualifier)
        ok = acl_free(qualifier) == 0 && ok;
    return acl_free(acl) == 0 && ok;
}
#else
static void pt_le16(unsigned char *out, unsigned n) {
    out[0] = (unsigned char)n;
    out[1] = (unsigned char)(n >> 8);
}
static void pt_le32(unsigned char *out, uint32_t n) {
    for (size_t i = 0; i < 4; i++)
        out[i] = (unsigned char)(n >> (8 * i));
}

static bool pt_linux_acl(const char *path, bool defaults, bool install) {
    const char *key = defaults ? "system.posix_acl_default" : "system.posix_acl_access";
    if (!install)
        return removexattr(path, key) == 0 || errno == ENODATA;
    unsigned char bytes[44] = {0};
    const unsigned access_tags[5] = {1, 2, 4, 16, 32};
    const unsigned default_tags[3] = {1, 4, 32};
    const unsigned *tags = defaults ? default_tags : access_tags;
    size_t count = defaults ? 3 : 5;
    struct passwd *principal = getpwnam("nobody");
    if (!principal || principal->pw_uid == geteuid())
        return false;
    pt_le32(bytes, 2);
    for (size_t i = 0; i < count; i++) {
        unsigned char *entry = bytes + 4 + 8 * i;
        unsigned permission = tags[i] == 1 || tags[i] == 2 ? 7 : 0;
        pt_le16(entry, tags[i]);
        pt_le16(entry + 2, permission);
        pt_le32(entry + 4, tags[i] == 2 ? (uint32_t)principal->pw_uid : UINT32_MAX);
    }
    size_t length = 4 + 8 * count;
    if (setxattr(path, key, bytes, length, 0) != 0)
        return false;
    unsigned char observed[44];
    ssize_t n = getxattr(path, key, observed, sizeof(observed));
    return n == (ssize_t)length && !memcmp(bytes, observed, length);
}
#endif
#endif

/* Full native parent-security snapshots for family E. The test serialization
 * contains semantic fields or documented external ACL bytes, never pointers or
 * an in-memory security descriptor's padding. Unknown/oversized state fails the
 * preservation assertion; no successful prefix is accepted. */
#ifdef _WIN32
static bool pt_acl_windows_sid(pt_wire *snapshot, PSID sid) {
    pt_wnumber(snapshot, sid != NULL, 1);
    if (!sid)
        return snapshot->ok;
    if (!IsValidSid(sid))
        return false;
    DWORD length = GetLengthSid(sid);
    pt_wnumber(snapshot, length, 4);
    pt_wbytes(snapshot, sid, length);
    return snapshot->ok;
}

static bool pt_acl_windows_ace(pt_wire *snapshot, const ACE_HEADER *header) {
    /* The fixture installs ordinary allow ACEs. Deny ACEs are also completely
     * represented so changing the type cannot hide behind a policy predicate.
     * Any other post-call ACE form is itself a failed preservation witness. */
    if (header->AceType != ACCESS_ALLOWED_ACE_TYPE && header->AceType != ACCESS_DENIED_ACE_TYPE)
        return false;
    size_t offset = offsetof(ACCESS_ALLOWED_ACE, SidStart);
    if (header->AceSize < offset + 8)
        return false;
    const unsigned char *bytes = (const unsigned char *)header;
    const unsigned char *sid_bytes = bytes + offset;
    size_t sid_length = 8 + 4 * (size_t)sid_bytes[1];
    if (sid_length > (size_t)header->AceSize - offset)
        return false;
    DWORD rights;
    memcpy(&rights, bytes + offsetof(ACCESS_ALLOWED_ACE, Mask), sizeof(rights));
    pt_wnumber(snapshot, header->AceType, 1);
    pt_wnumber(snapshot, header->AceFlags, 1);
    pt_wnumber(snapshot, rights, 4);
    return pt_acl_windows_sid(snapshot, (PSID)sid_bytes);
}

static bool pt_acl_windows_descriptor(pt_wire *snapshot, PSECURITY_DESCRIPTOR descriptor) {
    SECURITY_DESCRIPTOR_CONTROL control;
    DWORD revision;
    PSID owner = NULL;
    PACL dacl = NULL;
    BOOL owner_defaulted = FALSE, present = FALSE, defaulted = FALSE;
    if (!IsValidSecurityDescriptor(descriptor) ||
        !GetSecurityDescriptorControl(descriptor, &control, &revision) ||
        !GetSecurityDescriptorOwner(descriptor, &owner, &owner_defaulted) ||
        !GetSecurityDescriptorDacl(descriptor, &present, &dacl, &defaulted))
        return false;
    /* SELF_RELATIVE describes storage form, not a security-policy difference. */
    pt_wnumber(snapshot, control & ~(unsigned)SE_SELF_RELATIVE, 4);
    pt_wnumber(snapshot, revision, 4);
    pt_wnumber(snapshot, owner_defaulted != FALSE, 1);
    if (!pt_acl_windows_sid(snapshot, owner))
        return false;
    pt_wnumber(snapshot, present != FALSE, 1);
    pt_wnumber(snapshot, defaulted != FALSE, 1);
    pt_wnumber(snapshot, dacl != NULL, 1);
    if (!dacl)
        return snapshot->ok;
    if (!IsValidAcl(dacl))
        return false;
    pt_wnumber(snapshot, dacl->AclRevision, 1);
    pt_wnumber(snapshot, dacl->AceCount, 4);
    for (DWORD i = 0; i < dacl->AceCount; i++) {
        void *entry = NULL;
        if (!GetAce(dacl, i, &entry) || !entry || !pt_acl_windows_ace(snapshot, entry))
            return false;
    }
    return snapshot->ok;
}

static bool pt_acl_snapshot(pt_fixture *fx, const char *path, pt_wire *snapshot) {
    *snapshot = (pt_wire){.ok = true};
    wchar_t wide[PT_PATH];
    if (!pt_wide(path, wide))
        return false;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD status = GetNamedSecurityInfoW(wide, SE_FILE_OBJECT,
                                         OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                                         NULL, NULL, NULL, NULL, &descriptor);
    bool ok =
        status == ERROR_SUCCESS && descriptor && pt_acl_windows_descriptor(snapshot, descriptor);
    if (descriptor && LocalFree(descriptor) != NULL)
        ok = false;
    (void)fx;
    return ok;
}
#elif defined(__APPLE__)
static bool pt_acl_mac_entries(acl_t acl, pt_wire *snapshot) {
    size_t count = 0;
    int cursor = ACL_FIRST_ENTRY;
    for (;;) {
        acl_entry_t entry;
        errno = 0;
        if (acl_get_entry(acl, cursor, &entry) != 0) {
            if (errno != EINVAL)
                return false;
            break;
        }
        if (++count > ACL_MAX_ENTRIES)
            return false;
        cursor = ACL_NEXT_ENTRY;
        acl_tag_t tag;
        acl_permset_mask_t permissions;
        if (acl_get_tag_type(entry, &tag) != 0 || acl_get_permset_mask_np(entry, &permissions) != 0)
            return false;
        void *principal = acl_get_qualifier(entry);
        if (!principal)
            return false;
        pt_wnumber(snapshot, (uint32_t)tag, 4);
        pt_wnumber(snapshot, permissions, 8);
        pt_wbytes(snapshot, principal, sizeof(uuid_t));
        if (acl_free(principal) != 0 || !snapshot->ok)
            return false;
    }
    pt_wnumber(snapshot, count, 4);
    return snapshot->ok;
}

static bool pt_acl_snapshot(pt_fixture *fx, const char *path, pt_wire *snapshot) {
    *snapshot = (pt_wire){.ok = true};
    acl_t acl = NULL;
    bool absent = false;
    if (!pt_mac_read_acl(path, &acl, &absent))
        return false;
    /* Preserve absent versus present native state; a returned empty ACL still
     * retains its complete external bytes and flags below. */
    pt_wnumber(snapshot, !absent, 1);
    if (absent)
        return snapshot->ok;
    ssize_t length = acl_size(acl);
    bool ok = length > 0 && (size_t)length <= sizeof(snapshot->bytes);
    unsigned char *external = ok ? cbm_arena_calloc(&fx->arena, (size_t)length) : NULL;
    ok = external && acl_copy_ext_native(external, acl, length) == length;
    if (ok) {
        /* Public, serializable external representation binds ALL ACL/ACE flags,
         * including reserved native bits. Its internal format is not assumed. */
        pt_wnumber(snapshot, (uint64_t)length, 8);
        pt_wbytes(snapshot, external, (size_t)length);
        ok = pt_acl_mac_entries(acl, snapshot);
    }
    return acl_free(acl) == 0 && ok && snapshot->ok;
}
#else
static bool pt_acl_linux_attribute(pt_fixture *fx, const char *path, const char *key,
                                   pt_wire *snapshot) {
    errno = 0;
    ssize_t length = getxattr(path, key, NULL, 0);
    if (length < 0) {
        if (errno != ENODATA)
            return false;
        pt_wnumber(snapshot, 0, 1);
        return snapshot->ok;
    }
    if ((size_t)length > 4096)
        return false;
    size_t capacity = length ? (size_t)length : 1;
    unsigned char *bytes = cbm_arena_alloc(&fx->arena, capacity);
    if (!bytes || getxattr(path, key, bytes, capacity) != length)
        return false;
    pt_wnumber(snapshot, 1, 1);
    pt_wnumber(snapshot, (uint64_t)length, 8);
    pt_wbytes(snapshot, bytes, (size_t)length);
    return snapshot->ok;
}

static bool pt_acl_snapshot(pt_fixture *fx, const char *path, pt_wire *snapshot) {
    *snapshot = (pt_wire){.ok = true};
    return pt_acl_linux_attribute(fx, path, "system.posix_acl_access", snapshot) &&
           pt_acl_linux_attribute(fx, path, "system.posix_acl_default", snapshot);
}
#endif

static bool pt_acl_preserved(pt_fixture *fx, const char *path, const pt_wire *before) {
    pt_wire after;
    return before->ok && pt_acl_snapshot(fx, path, &after) && before->used == after.used &&
           !memcmp(before->bytes, after.bytes, before->used);
}

TEST(tree_e_native_permissions_and_acl_policy) {
    pt_fixture fx = {0};
    int result = 1;
    bool parent_acl = false, tracked_acl = false;
    char tracked[PT_PATH] = {0};
    pt_wire parent_before;
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, 40)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)) &&
             pt_setup_step("setup:control", pt_fact_control(&fx)));
    PT_CHECK(pt_policy(fx.parent, true, false));
#ifdef __APPLE__
    char missing_acl[PT_PATH];
    struct stat missing_stat;
    pt_wire missing_snapshot;
    PT_CHECK(pt_join(missing_acl, fx.parent, "acl-nonexistent-control"));
    errno = 0;
    PT_CHECK(lstat(missing_acl, &missing_stat) == -1 && errno == ENOENT);
    PT_CHECK(!pt_acl_absent(missing_acl, true));
    PT_CHECK(!pt_acl_snapshot(&fx, missing_acl, &missing_snapshot));
#endif
    PT_CHECK(pt_acl_snapshot(&fx, fx.parent, &parent_before));
    PT_CHECK(pt_acl_preserved(&fx, fx.parent, &parent_before));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    cbm_pinned_tree_error_t error;
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_view_matches(&fx, cbm_pinned_tree_view(fx.tree), false));
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &options.control, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_close_one(&fx.tree));
    PT_CHECK(pt_acl_preserved(&fx, fx.parent, &parent_before));
#ifdef _WIN32
    /* WHY: Win32 DACL semantics have no POSIX-mode equivalent. The native
     * owner-only positive above precedes an observed extra-world ACE. */
    parent_acl = true;
    PT_CHECK(pt_windows_acl(&fx, fx.parent, true));
    PT_CHECK(!pt_policy(fx.parent, true, false));
    PT_CHECK(pt_acl_snapshot(&fx, fx.parent, &parent_before));
    PT_CHECK(pt_acl_preserved(&fx, fx.parent, &parent_before));
    PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_UNSUPPORTED));
    PT_CHECK(pt_acl_preserved(&fx, fx.parent, &parent_before));
    PT_CHECK(!pt_policy(fx.parent, true, false));
    PT_CHECK(pt_windows_acl(&fx, fx.parent, false));
    parent_acl = false;
#elif defined(__APPLE__)
    PT_CHECK(pt_passing_posix_mode(fx.parent, 0700));
    parent_acl = true;
    PT_CHECK(pt_mac_named_acl(fx.parent, true));
    PT_CHECK(pt_passing_posix_mode(fx.parent, 0700) && pt_mac_named_observed(fx.parent));
    PT_CHECK(pt_acl_snapshot(&fx, fx.parent, &parent_before));
    PT_CHECK(pt_acl_preserved(&fx, fx.parent, &parent_before));
    PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_UNSUPPORTED));
    PT_CHECK(pt_acl_preserved(&fx, fx.parent, &parent_before));
    PT_CHECK(pt_passing_posix_mode(fx.parent, 0700) && pt_mac_named_observed(fx.parent));
    PT_CHECK(pt_mac_named_acl(fx.parent, false));
    parent_acl = false;
#else
    for (unsigned defaults = 0; defaults < 2; defaults++) {
        parent_acl = true;
        PT_CHECK(pt_linux_acl(fx.parent, defaults != 0, true));
        PT_CHECK(pt_passing_posix_mode(fx.parent, 0700));
        PT_CHECK(pt_acl_snapshot(&fx, fx.parent, &parent_before));
        PT_CHECK(pt_acl_preserved(&fx, fx.parent, &parent_before));
        PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_UNSUPPORTED));
        PT_CHECK(pt_acl_preserved(&fx, fx.parent, &parent_before));
        PT_CHECK(pt_linux_acl(fx.parent, defaults != 0, false));
        parent_acl = false;
    }
#endif
    PT_CHECK(pt_policy(fx.parent, true, false));
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_join(tracked, cbm_pinned_tree_view(fx.tree)->root, "nested"));
#ifdef _WIN32
    tracked_acl = true;
    PT_CHECK(pt_windows_acl(&fx, tracked, true));
#elif defined(__APPLE__)
    tracked_acl = true;
    PT_CHECK(pt_mac_named_acl(tracked, true));
    PT_CHECK(pt_passing_posix_mode(tracked, 0700) && pt_mac_named_observed(tracked));
#else
    tracked_acl = true;
    PT_CHECK(pt_linux_acl(tracked, false, true));
    PT_CHECK(pt_passing_posix_mode(tracked, 0700));
#endif
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &options.control, &error) == CBM_PINNED_TREE_CHANGED);
    PT_CHECK(!cbm_pinned_tree_view(fx.tree));
    result = 0;
done:
#ifdef _WIN32
    if (parent_acl && !pt_windows_acl(&fx, fx.parent, false))
        result = 1;
    if (tracked_acl && !pt_windows_acl(&fx, tracked, false))
        result = 1;
#elif defined(__APPLE__)
    if (parent_acl && !pt_mac_named_acl(fx.parent, false))
        result = 1;
    if (tracked_acl && !pt_mac_named_acl(tracked, false))
        result = 1;
#else
    if (parent_acl &&
        (!pt_linux_acl(fx.parent, false, false) || !pt_linux_acl(fx.parent, true, false)))
        result = 1;
    if (tracked_acl && !pt_linux_acl(tracked, false, false))
        result = 1;
#endif
    return pt_finish(&fx, result);
}

/* The work parent production makes is owner-only: cbm_mkdir_p stamps a fresh
 * directory with one inheritable grant for the owner, which Windows stores as
 * an effective entry plus an inherit-only one. The policy refused that form,
 * so the engine on Windows ran everything (pinning HEAD failed). The same
 * split naming another principal is still refused. */
TEST(tree_e_windows_split_owner_dacl_is_owner_only) {
#ifndef _WIN32
    SKIP_PLATFORM("Win32 DACL encoding");
#else
    pt_fixture fx = {0};
    int result = 1;
    char stamped[PT_PATH] = {0};
    unsigned char world[SECURITY_MAX_SID_SIZE];
    DWORD world_length = sizeof(world);
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, 40)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)) &&
             pt_setup_step("setup:control", pt_fact_control(&fx)));
    PT_CHECK(pt_join(stamped, fx.home, "stamped-parent") && cbm_mkdir_p(stamped, 0700));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    options.private_parent = stamped;
    cbm_pinned_tree_error_t error;
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_close_one(&fx.tree));
    PT_CHECK(pt_windows_split_acl(&fx, stamped, pt_current_sid(&fx)));
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_close_one(&fx.tree));
    PT_CHECK(CreateWellKnownSid(WinWorldSid, NULL, world, &world_length));
    PT_CHECK(pt_windows_split_acl(&fx, stamped, world));
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_UNSUPPORTED &&
             !fx.tree);
    result = 0;
done:
    if (stamped[0] && !pt_windows_split_acl(&fx, stamped, pt_current_sid(&fx)))
        result = 1;
    return pt_finish(&fx, result);
#endif
}

typedef struct {
    size_t calls, stop;
} pt_cancel;
static bool pt_cancelled(void *context) {
    pt_cancel *control = context;
    control->calls++;
    return control->stop && control->calls >= control->stop;
}

static bool pt_create_ok(pt_fixture *fx, cbm_pinned_tree_options_t *options) {
    cbm_pinned_tree_error_t error;
    return cbm_pinned_tree_create(options, &fx->tree, &error) == CBM_PINNED_TREE_OK &&
           error.status == CBM_PINNED_TREE_OK && error.cause == CBM_PINNED_TREE_OK &&
           pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false);
}

static bool pt_limit_pairs(pt_fixture *fx) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    size_t *fields[] = {&options.limits.max_files, &options.limits.max_directories,
                        &options.limits.max_total_content_bytes,
                        &options.limits.max_relative_path_bytes};
    const size_t exact[] = {fx->count, 2, pt_total(fx), strlen("nested/source.c")};
    for (size_t i = 0; i < 4; i++) {
        options = pt_options(fx, CBM_GIT_REV_HEAD);
        *fields[i] = exact[i];
        if (!pt_create_ok(fx, &options) || !pt_close_one(&fx->tree))
            return false;
        *fields[i] = exact[i] - 1;
        if (!pt_clean_failure(fx, &options, CBM_PINNED_TREE_LIMIT))
            return false;
    }
    options = pt_options(fx, CBM_GIT_REV_HEAD);
    options.limits.max_arena_bytes = 1;
    if (!pt_clean_failure(fx, &options, CBM_PINNED_TREE_LIMIT))
        return false;
    /* Find this platform's logical tariff without encoding allocation layout.
     * Upper cap and at most24 bisections are explicit and bounded. */
    size_t low = 1, high = 16U * 1024U * 1024U;
    options.limits.max_arena_bytes = high;
    if (!pt_create_ok(fx, &options) || !pt_close_one(&fx->tree))
        return false;
    for (unsigned step = 0; low < high && step < 24; step++) {
        size_t middle = low + (high - low) / 2;
        options.limits.max_arena_bytes = middle;
        cbm_pinned_tree_error_t error;
        cbm_pinned_tree_status_t status = cbm_pinned_tree_create(&options, &fx->tree, &error);
        if (status == CBM_PINNED_TREE_OK) {
            if (!pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false) ||
                !pt_close_one(&fx->tree))
                return false;
            high = middle;
        } else {
            if (status != CBM_PINNED_TREE_LIMIT || error.status != status || fx->tree ||
                !pt_parent_only_sentinel(fx->parent))
                return false;
            low = middle + 1;
        }
    }
    if (low != high || low <= 1)
        return false;
    options.limits.max_arena_bytes = low;
    if (!pt_create_ok(fx, &options) || !pt_close_one(&fx->tree))
        return false;
    options.limits.max_arena_bytes = low - 1;
    return pt_clean_failure(fx, &options, CBM_PINNED_TREE_LIMIT);
}

static bool pt_cancel_create_cases(pt_fixture *fx) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    pt_cancel counter = {0};
    options.control.cancelled = pt_cancelled;
    options.control.cancel_context = &counter;
    if (!pt_create_ok(fx, &options) || !counter.calls || !pt_close_one(&fx->tree))
        return false;
    size_t total = counter.calls;
    size_t stops[] = {1, (total + 1) / 2, total, total + 1};
    size_t cancelled = 0;
    for (size_t i = 0; i < 4; i++) {
        counter = (pt_cancel){.stop = stops[i]};
        cbm_pinned_tree_error_t error;
        cbm_pinned_tree_status_t status = cbm_pinned_tree_create(&options, &fx->tree, &error);
        if (status == CBM_PINNED_TREE_OK) {
            if (counter.calls >= counter.stop ||
                !pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false) ||
                !pt_close_one(&fx->tree))
                return false;
        } else {
            if (status != CBM_PINNED_TREE_CANCELLED || error.status != status ||
                counter.calls < counter.stop || fx->tree || !pt_parent_only_sentinel(fx->parent))
                return false;
            cancelled++;
        }
    }
    options.control.cancelled = NULL;
    options.control.cancel_context = NULL;
    return cancelled && pt_create_ok(fx, &options) && pt_close_one(&fx->tree);
}

static bool pt_cancel_verify_cases(pt_fixture *fx) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    pt_cancel counter = {0};
    cbm_pinned_tree_control_t control = options.control;
    control.cancelled = pt_cancelled;
    control.cancel_context = &counter;
    cbm_pinned_tree_error_t error;
    if (!pt_create_ok(fx, &options) ||
        cbm_pinned_tree_verify(fx->tree, &control, &error) != CBM_PINNED_TREE_OK ||
        !counter.calls || !pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false) ||
        !pt_close_one(&fx->tree))
        return false;
    size_t total = counter.calls;
    const size_t stops[] = {1, (total + 1) / 2, total, total + 1};
    size_t cancelled = 0;
    for (size_t i = 0; i < 4; i++) {
        if (!pt_create_ok(fx, &options))
            return false;
        counter = (pt_cancel){.stop = stops[i]};
        cbm_pinned_tree_status_t status = cbm_pinned_tree_verify(fx->tree, &control, &error);
        if (status == CBM_PINNED_TREE_OK) {
            if (counter.calls >= counter.stop ||
                !pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false))
                return false;
        } else {
            if (status != CBM_PINNED_TREE_CANCELLED || counter.calls < counter.stop ||
                cbm_pinned_tree_view(fx->tree))
                return false;
            cancelled++;
        }
        if (!pt_close_one(&fx->tree) || !pt_parent_only_sentinel(fx->parent))
            return false;
    }
    return cancelled != 0;
}

TEST(tree_f_resource_limits_and_local_control) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, 40)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)) &&
             pt_setup_step("setup:control", pt_fact_control(&fx)));
    PT_CHECK(pt_limit_pairs(&fx));
    PT_CHECK(pt_cancel_create_cases(&fx) && pt_cancel_verify_cases(&fx));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    PT_CHECK(pt_create_ok(&fx, &options) && pt_close_one(&fx.tree));
    options.control.deadline_ms = 1;
    PT_CHECK(cbm_now_ms() > 1 && pt_clean_failure(&fx, &options, CBM_PINNED_TREE_DEADLINE));
    options = pt_options(&fx, CBM_GIT_REV_HEAD);
    options.limits.max_files = 0;
    PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_INVALID));
    options = pt_options(&fx, CBM_GIT_REV_HEAD);
    options.limits.max_relative_path_bytes = 4096;
    PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_INVALID));
    options = pt_options(&fx, CBM_GIT_REV_BASE);
    PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_INVALID));
    options = pt_options(&fx, CBM_GIT_REV_HEAD);
    options.control.deadline_ms = 0;
    PT_CHECK(pt_clean_failure(&fx, &options, CBM_PINNED_TREE_INVALID));
    options = pt_options(&fx, CBM_GIT_REV_HEAD);
    options.limits.blob_batch.max_entries = 1;
    cbm_pinned_tree_error_t error;
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_GIT);
    PT_CHECK(!fx.tree && error.status == CBM_PINNED_TREE_GIT &&
             error.git.status == CBM_GIT_FACTS_LIMIT && pt_parent_only_sentinel(fx.parent));
    result = 0;
done:
    return pt_finish(&fx, result);
}

static bool pt_restore_readonly(const char *path, bool executable) {
#ifdef _WIN32
    wchar_t wide[PT_PATH];
    if (!pt_wide(path, wide))
        return false;
    DWORD attributes = GetFileAttributesW(wide);
    (void)executable;
    return attributes != INVALID_FILE_ATTRIBUTES &&
           SetFileAttributesW(wide, attributes | FILE_ATTRIBUTE_READONLY);
#else
    return chmod(path, executable ? 0500 : 0400) == 0;
#endif
}

/* Each mutation happens between synchronous calls, with no worker or child
 * using the tree. Type/link cases have explicit native counterparts below. */
static bool pt_mutation_case(pt_fixture *fx, unsigned kind) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    if (!pt_create_ok(fx, &options))
        return false;
    char path[PT_PATH], other[PT_PATH];
    const char *root = cbm_pinned_tree_view(fx->tree)->root;
    if (!pt_join(path, root, "binary.bin") || !pt_join(other, root, "unexpected"))
        return false;
    bool ok = false;
    if (kind == 0)
        ok = pt_write(other, "new", 3);
    else if (kind == 1)
        ok = pt_make_writable(path) && cbm_unlink(path) == 0;
    else if (kind == 2)
        ok = cbm_rename_noreplace(path, other) == 0;
    else if (kind == 3)
        ok = pt_make_writable(path) && pt_write(path, pt_binary, sizeof(pt_binary) - 1) &&
             pt_restore_readonly(path, false);
    else if (kind == 4) {
        unsigned char changed[sizeof(pt_binary)];
        memcpy(changed, pt_binary, sizeof(changed));
        changed[0] = 'B';
        ok = pt_make_writable(path) && pt_write(path, changed, sizeof(changed)) &&
             pt_restore_readonly(path, false);
    } else if (kind == 5)
        ok = pt_make_writable(path);
    if (!ok)
        return false;
    cbm_pinned_tree_error_t error;
    ok = cbm_pinned_tree_verify(fx->tree, &options.control, &error) == CBM_PINNED_TREE_CHANGED &&
         error.status == CBM_PINNED_TREE_CHANGED && !cbm_pinned_tree_view(fx->tree);
    if (kind == 0)
        ok = cbm_unlink(other) == 0 && ok;
    if (kind == 2)
        ok = cbm_rename_noreplace(other, path) == 0 && ok;
    if (kind == 5)
        ok = pt_restore_readonly(path, false) && ok;
    /* A failed owner cannot be turned into a newly approved snapshot. */
    ok = cbm_pinned_tree_verify(fx->tree, &options.control, &error) != CBM_PINNED_TREE_OK &&
         !cbm_pinned_tree_view(fx->tree) && ok;
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
}

TEST(tree_g_verify_state_and_complete_inventory) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, 40)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)) &&
             pt_setup_step("setup:control", pt_fact_control(&fx)));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    PT_CHECK(pt_create_ok(&fx, &options));
    cbm_pinned_tree_error_t error;
    unsigned char digest[32];
    memcpy(digest, cbm_pinned_tree_view(fx.tree)->manifest_sha256, sizeof(digest));
    cbm_pinned_tree_control_t invalid = {.deadline_ms = 0};
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &invalid, &error) == CBM_PINNED_TREE_INVALID);
    PT_CHECK(pt_view_matches(&fx, cbm_pinned_tree_view(fx.tree), false));
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &options.control, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(!memcmp(cbm_pinned_tree_view(fx.tree)->manifest_sha256, digest, sizeof(digest)));
    PT_CHECK(pt_close_one(&fx.tree));
    for (unsigned kind = 0; kind < 6; kind++)
        PT_CHECK(pt_mutation_case(&fx, kind));
    PT_CHECK(pt_create_ok(&fx, &options));
    cbm_pinned_tree_control_t expired = {.deadline_ms = 1};
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &expired, &error) == CBM_PINNED_TREE_DEADLINE);
    PT_CHECK(!cbm_pinned_tree_view(fx.tree) && pt_close_one(&fx.tree));
    PT_CHECK(cbm_pinned_tree_close(NULL, &error) == CBM_PINNED_TREE_INVALID);
    PT_CHECK(cbm_pinned_tree_close(&fx.tree, &error) == CBM_PINNED_TREE_OK && !fx.tree);
    result = 0;
done:
    return pt_finish(&fx, result);
}

/* Windows may retain a directory handle without delete-sharing. This is an
 * observed native applicability branch, never a whole-family platform escape.
 */
/* These paths belong exclusively to this fixture; no concurrent namespace
 * writers are admitted. POSIX checks an absent destination before rename under
 * that custody assumption. This is not a general atomic no-replace primitive. */
static bool pt_move_owned_directory(const char *source, const char *destination) {
#ifdef _WIN32
    wchar_t from[PT_PATH], to[PT_PATH];
    return pt_wide(source, from) && pt_wide(destination, to) && MoveFileExW(from, to, 0) != 0;
#else
    struct stat st;
    if (lstat(source, &st) != 0)
        return false;
    if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return false;
    }
    if (lstat(destination, &st) == 0) {
        errno = EEXIST;
        return false;
    }
    if (errno != ENOENT)
        return false;
    return rename(source, destination) == 0;
#endif
}

static bool pt_relocate_directory(const char *source, const char *destination, bool *blocked) {
    *blocked = false;
#ifdef _WIN32
    wchar_t from[PT_PATH], to[PT_PATH];
    if (!pt_wide(source, from) || !pt_wide(destination, to))
        return false;
    if (MoveFileExW(from, to, 0))
        return true;
    DWORD error = GetLastError();
    if (error != ERROR_SHARING_VIOLATION && error != ERROR_ACCESS_DENIED)
        return false;
    fprintf(stderr,
            "pinned-tree relocation applicability: WHY native directory mutation "
            "denied; "
            "tried MoveFileExW, observed error=%lu; unchanged-tree verification "
            "follows\n",
            (unsigned long)error);
    *blocked = true;
    return true;
#else
    return pt_move_owned_directory(source, destination);
#endif
}

/* H-only diagnostics: each predicate and API call retains its original order. */
static bool pt_h_tree_step(const char *helper, const char *stage, bool ok,
                           cbm_pinned_tree_status_t status, const cbm_pinned_tree_error_t *error) {
    if (ok)
        return true;
    int saved_errno = errno;
#ifdef _WIN32
    DWORD saved_error = GetLastError();
#endif
    fprintf(stderr,
            "pinned-tree H helper=%s stage=%s return=%d error.status=%d cause=%d "
            "errno=%d diagnostic='",
            helper, stage, (int)status, (int)error->status, (int)error->cause, saved_errno);
    size_t length = 0;
    while (length < sizeof(error->diagnostic) && error->diagnostic[length])
        length++;
    pt_diag_bytes((const unsigned char *)error->diagnostic, length);
    fputs("'\n", stderr);
#ifdef _WIN32
    SetLastError(saved_error);
#endif
    errno = saved_errno;
    return false;
}

static bool pt_h_status(const char *helper, const char *stage, cbm_pinned_tree_status_t actual,
                        cbm_pinned_tree_status_t expected, const cbm_pinned_tree_error_t *error) {
    return pt_h_tree_step(helper, stage, actual == expected, actual, error);
}

static bool pt_h_create_ok(const char *helper, pt_fixture *fx, cbm_pinned_tree_options_t *options) {
    cbm_pinned_tree_error_t error;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_create(options, &fx->tree, &error);
    return pt_h_status(helper, "create", status, CBM_PINNED_TREE_OK, &error) &&
           pt_h_tree_step(helper, "create-error-state",
                          error.status == CBM_PINNED_TREE_OK && error.cause == CBM_PINNED_TREE_OK,
                          status, &error) &&
           pt_h_tree_step(helper, "create-view",
                          pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false), status,
                          &error);
}

static bool pt_h_close_one(const char *helper, cbm_pinned_tree_t **tree) {
    if (!*tree)
        return true;
    cbm_pinned_tree_error_t error;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_close(tree, &error);
    return pt_h_tree_step(helper, "final-close", status == CBM_PINNED_TREE_OK && !*tree, status,
                          &error);
}

static bool pt_unchanged_after_blocked_move(const char *helper, pt_fixture *fx,
                                            cbm_pinned_tree_options_t *options) {
    cbm_pinned_tree_error_t error;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_verify(fx->tree, &options->control, &error);
    bool ok =
        pt_h_status(helper, "blocked-move-verify", status, CBM_PINNED_TREE_OK, &error) &&
        pt_h_tree_step(helper, "blocked-move-view",
                       pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false), status, &error);
    return pt_h_close_one(helper, &fx->tree) &&
           pt_setup_step("H:blocked-move-parent-preserve", pt_parent_only_sentinel(fx->parent)) &&
           ok;
}

#ifdef _WIN32
/* A mount-point reparse fixture uses an owned empty directory and an owned
 * target. It does not depend on symbolic-link privilege or developer mode. */
static bool pt_junction(const char *path, const char *target) {
    wchar_t wide[PT_PATH], destination[PT_PATH];
    if (!pt_wide(path, wide) || !pt_wide(target, destination))
        return false;
    for (wchar_t *p = destination; *p; p++)
        if (*p == L'/')
            *p = L'\\';
    if (!destination[0] || destination[1] != L':')
        return false;
    struct {
        DWORD tag;
        WORD data_length, reserved, substitute_offset, substitute_length, print_offset,
            print_length;
        WCHAR path[PT_PATH + 8];
    } buffer = {0};
    size_t length = wcslen(destination);
    if (length + 5 >= sizeof(buffer.path) / sizeof(buffer.path[0]))
        return false;
    buffer.tag = IO_REPARSE_TAG_MOUNT_POINT;
    memcpy(buffer.path, L"\\??\\", 4 * sizeof(WCHAR));
    memcpy(buffer.path + 4, destination, (length + 1) * sizeof(WCHAR));
    buffer.substitute_length = (WORD)((length + 4) * sizeof(WCHAR));
    buffer.print_offset = (WORD)(buffer.substitute_length + sizeof(WCHAR));
    buffer.print_length = 0;
    buffer.path[length + 5] = 0;
    buffer.data_length = (WORD)(8 + buffer.substitute_length + 2 * sizeof(WCHAR));
    HANDLE directory = CreateFileW(wide, GENERIC_WRITE, 0, NULL, OPEN_EXISTING,
                                   FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (directory == INVALID_HANDLE_VALUE)
        return false;
    DWORD returned = 0;
    bool ok = DeviceIoControl(directory, FSCTL_SET_REPARSE_POINT, &buffer,
                              (DWORD)(8 + buffer.data_length), NULL, 0, &returned, NULL) != 0;
    return CloseHandle(directory) && ok;
}
#endif

static bool pt_link_case(pt_fixture *fx) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    if (!pt_h_create_ok("link", fx, &options))
        return false;
    char root[PT_PATH], tracked[PT_PATH], saved[PT_PATH], outside[PT_PATH], sentinel[PT_PATH];
    strcpy(root, cbm_pinned_tree_view(fx->tree)->root);
    if (!pt_join(tracked, root, "nested") || !pt_join(saved, root, "parked-dir") ||
        !pt_join(outside, fx->home, "link-target") || cbm_mkdir(outside) != 0 ||
        !pt_join(sentinel, outside, "keep") || !pt_write(sentinel, "target", 6))
        return pt_setup_step("H:link:paths-outside-sentinel", false);
    bool blocked = false;
    if (!pt_relocate_directory(tracked, saved, &blocked))
        return pt_setup_step("H:link:relocate", false);
    if (blocked)
        return pt_unchanged_after_blocked_move("link", fx, &options) &&
               pt_setup_step("H:link:blocked-preserve", pt_equal_file(sentinel, "target", 6));
    bool installed = false, ok = false;
#ifdef _WIN32
    if (!pt_setup_step("H:link:install-mkdir", cbm_mkdir(tracked) == 0))
        goto restore;
    installed = true;
    if (!pt_setup_step("H:link:install-junction", pt_junction(tracked, outside)))
        goto restore;
#else
    if (!pt_setup_step("H:link:install-symlink", symlink(outside, tracked) == 0))
        goto restore;
    installed = true;
#endif
    cbm_pinned_tree_error_t error;
    ok = pt_h_status("link", "verify", cbm_pinned_tree_verify(fx->tree, &options.control, &error),
                     CBM_PINNED_TREE_CHANGED, &error) &&
         pt_setup_step("H:link:verify-view-unavailable", !cbm_pinned_tree_view(fx->tree)) &&
         pt_h_status("link", "close", cbm_pinned_tree_close(&fx->tree, &error),
                     CBM_PINNED_TREE_CLEANUP_REQUIRED, &error) &&
         pt_setup_step("H:link:close-owner-retained", fx->tree != NULL) &&
         pt_setup_step("H:link:close-view-unavailable", !cbm_pinned_tree_view(fx->tree)) &&
         pt_setup_step("H:link:preserve-target", pt_equal_file(sentinel, "target", 6));
restore:
    if (installed) {
#ifdef _WIN32
        wchar_t wide[PT_PATH];
        if (!pt_wide(tracked, wide) || !RemoveDirectoryW(wide)) {
            (void)pt_setup_step("H:link:restore-remove-junction", false);
            ok = false;
        }
#else
        if (!pt_setup_step("H:link:restore-remove-symlink", cbm_unlink(tracked) == 0))
            ok = false;
#endif
    }
    if (!pt_setup_step("H:link:restore-directory", pt_move_owned_directory(saved, tracked)))
        ok = false;
    return pt_h_close_one("link", &fx->tree) &&
           pt_setup_step("H:link:final-preserve-target", pt_equal_file(sentinel, "target", 6)) &&
           ok;
}

static bool pt_root_replacement_case(pt_fixture *fx) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    if (!pt_h_create_ok("root-replacement", fx, &options))
        return false;
    char original[PT_PATH], moved[PT_PATH], marker[PT_PATH], preserved[PT_PATH];
    strcpy(original, cbm_pinned_tree_view(fx->tree)->root);
    bool blocked = false;
    if (!pt_join(moved, fx->parent, "parked-root") ||
        !pt_relocate_directory(original, moved, &blocked))
        return pt_setup_step("H:root-replacement:relocate", false);
    if (blocked)
        return pt_unchanged_after_blocked_move("root-replacement", fx, &options);
    bool replacement =
        pt_setup_step("H:root-replacement:install-directory", cbm_mkdir(original) == 0);
    bool ok =
        replacement &&
        pt_setup_step("H:root-replacement:marker-path", pt_join(marker, original, "other-owner")) &&
        pt_setup_step("H:root-replacement:marker-write", pt_write(marker, "keep", 4)) &&
        pt_setup_step("H:root-replacement:preserved-path", pt_join(preserved, moved, "binary.bin"));
    cbm_pinned_tree_error_t error;
    if (ok)
        ok =
            pt_h_status("root-replacement", "close", cbm_pinned_tree_close(&fx->tree, &error),
                        CBM_PINNED_TREE_CLEANUP_REQUIRED, &error) &&
            pt_setup_step("H:root-replacement:close-owner-retained", fx->tree != NULL) &&
            pt_setup_step("H:root-replacement:close-view-unavailable",
                          !cbm_pinned_tree_view(fx->tree)) &&
            pt_setup_step("H:root-replacement:cleanup-path",
                          !strcmp(error.cleanup_path, original)) &&
            pt_setup_step("H:root-replacement:preserve-marker", pt_equal_file(marker, "keep", 4)) &&
            pt_setup_step("H:root-replacement:preserve-original",
                          pt_equal_file(preserved, pt_binary, sizeof(pt_binary)));
    if (replacement &&
        !pt_setup_step("H:root-replacement:remove-replacement", th_rmtree(original) == 0))
        ok = false;
    if (!pt_setup_step("H:root-replacement:restore-original",
                       pt_move_owned_directory(moved, original)))
        ok = false;
    return pt_h_close_one("root-replacement", &fx->tree) &&
           pt_setup_step("H:root-replacement:final-parent-preserve",
                         pt_parent_only_sentinel(fx->parent)) &&
           ok;
}

static bool pt_hardlink_case(pt_fixture *fx) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    if (!pt_h_create_ok("hardlink", fx, &options))
        return false;
    char tracked[PT_PATH], alias[PT_PATH];
    if (!pt_join(tracked, cbm_pinned_tree_view(fx->tree)->root, "binary.bin") ||
        !pt_join(alias, fx->home, "outside-hardlink"))
        return pt_setup_step("H:hardlink:paths", false);
#ifdef _WIN32
    wchar_t a[PT_PATH], b[PT_PATH];
    if (!pt_wide(alias, a) || !pt_wide(tracked, b) || !CreateHardLinkW(a, b, NULL))
        return pt_setup_step("H:hardlink:install", false);
#else
    if (!pt_setup_step("H:hardlink:install", link(tracked, alias) == 0))
        return false;
#endif
    cbm_pinned_tree_error_t error;
    bool ok =
        pt_h_status("hardlink", "verify",
                    cbm_pinned_tree_verify(fx->tree, &options.control, &error),
                    CBM_PINNED_TREE_CHANGED, &error) &&
        pt_setup_step("H:hardlink:verify-view-unavailable", !cbm_pinned_tree_view(fx->tree)) &&
        pt_setup_step("H:hardlink:preserve-alias",
                      pt_equal_file(alias, pt_binary, sizeof(pt_binary)));
    /* Explicit fixture removal of the alias restores the owned file's single
     * link identity before the materializer is asked to dispose it. */
#ifdef _WIN32
    if (!pt_setup_step("H:hardlink:restore-writable", pt_make_writable(alias)))
        ok = false;
#endif
    if (!pt_setup_step("H:hardlink:restore-unlink", cbm_unlink(alias) == 0))
        ok = false;
    return pt_h_close_one("hardlink", &fx->tree) && ok;
}

#ifdef _WIN32
/* No handle acquired by these fixture checks survives a native rename attempt. */
static bool pt_retry_directory_info(const char *path, BY_HANDLE_FILE_INFORMATION *info) {
    wchar_t wide[PT_PATH];
    if (!pt_wide(path, wide))
        return false;
    HANDLE handle = CreateFileW(
        wide, FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
        OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (handle == INVALID_HANDLE_VALUE)
        return false;
    memset(info, 0, sizeof(*info));
    bool ok = GetFileInformationByHandle(handle, info) != 0;
    if (!CloseHandle(handle))
        ok = false;
    return ok && (info->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
           (info->dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
}

static bool pt_retry_path_absent(const wchar_t *path) {
    if (GetFileAttributesW(path) != INVALID_FILE_ATTRIBUTES)
        return false;
    DWORD error = GetLastError();
    return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND;
}

static bool pt_retry_same_id(const BY_HANDLE_FILE_INFORMATION *a,
                             const BY_HANDLE_FILE_INFORMATION *b) {
    return a->dwVolumeSerialNumber == b->dwVolumeSerialNumber &&
           a->nFileIndexHigh == b->nFileIndexHigh && a->nFileIndexLow == b->nFileIndexLow;
}

static bool pt_retry_directory_is(const char *path, const BY_HANDLE_FILE_INFORMATION *expected) {
    BY_HANDLE_FILE_INFORMATION actual;
    return pt_retry_directory_info(path, &actual) && pt_retry_same_id(&actual, expected);
}

typedef struct {
    char root[PT_PATH], moved[PT_PATH], child[PT_PATH], moved_child[PT_PATH];
    BY_HANDLE_FILE_INFORMATION original;
    cbm_pinned_tree_t *owner;
} pt_cleanup_retry;

static bool pt_retry_prepare(pt_fixture *fx, pt_cleanup_retry *retry) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    if (!pt_create_ok(fx, &options))
        return false;
    strcpy(retry->root, cbm_pinned_tree_view(fx->tree)->root);
    retry->owner = fx->tree;
    if (!pt_join(retry->moved, fx->parent, "retry-parked-root") ||
        !pt_join(retry->child, retry->root, "retry-unexpected.bin") ||
        !pt_join(retry->moved_child, retry->moved, "retry-unexpected.bin") ||
        !pt_retry_directory_info(retry->root, &retry->original) ||
        !pt_write(retry->child, "caller-retained", 15))
        return false;
    cbm_pinned_tree_error_t error;
    return cbm_pinned_tree_close(&fx->tree, &error) == CBM_PINNED_TREE_CLEANUP_REQUIRED &&
           error.status == CBM_PINNED_TREE_CLEANUP_REQUIRED && fx->tree == retry->owner &&
           !cbm_pinned_tree_view(fx->tree) && !strcmp(error.cleanup_path, retry->root) &&
           pt_retry_directory_is(retry->root, &retry->original) &&
           pt_equal_file(retry->child, "caller-retained", 15);
}

static bool pt_retry_first_move(const pt_cleanup_retry *retry, bool *blocked) {
    wchar_t from[PT_PATH], to[PT_PATH];
    *blocked = false;
    if (!pt_wide(retry->root, from) || !pt_wide(retry->moved, to))
        return false;
    if (MoveFileExW(from, to, 0))
        return true;
    DWORD error = GetLastError();
    if (error != ERROR_SHARING_VIOLATION && error != ERROR_ACCESS_DENIED)
        return false;
    fprintf(stderr,
            "pinned-tree cleanup retry applicability: WHY first native relocation "
            "denied; tried MoveFileExW after nonempty close, observed error=%lu; "
            "retained native identity and child are checked before recovery\n",
            (unsigned long)error);
    *blocked = true;
    return true;
}

static bool pt_retry_replacement(pt_fixture *fx, const pt_cleanup_retry *retry) {
    wchar_t root[PT_PATH], moved[PT_PATH];
    BY_HANDLE_FILE_INFORMATION replacement;
    if (!pt_wide(retry->root, root) || !pt_wide(retry->moved, moved) ||
        !pt_retry_directory_is(retry->moved, &retry->original) || !pt_retry_path_absent(root) ||
        cbm_mkdir(retry->root) != 0 || !pt_retry_directory_info(retry->root, &replacement) ||
        pt_retry_same_id(&replacement, &retry->original))
        return false;
    cbm_pinned_tree_error_t error;
    if (cbm_pinned_tree_close(&fx->tree, &error) != CBM_PINNED_TREE_CLEANUP_REQUIRED ||
        error.status != CBM_PINNED_TREE_CLEANUP_REQUIRED || fx->tree != retry->owner ||
        cbm_pinned_tree_view(fx->tree) || strcmp(error.cleanup_path, retry->root) ||
        !pt_retry_directory_is(retry->root, &replacement) ||
        !pt_retry_directory_is(retry->moved, &retry->original) ||
        !pt_equal_file(retry->moved_child, "caller-retained", 15))
        return false;
    /* This removal MUST succeed. Unlike the first relocation, a refusal here
     * is a regression: failed close may not retain custody of a replacement. */
    if (!RemoveDirectoryW(root)) {
        fprintf(stderr,
                "pinned-tree replacement removal failed after close retry: error=%lu; "
                "original retained at %s\n",
                (unsigned long)GetLastError(), retry->moved);
        return false;
    }
    return MoveFileExW(moved, root, 0) != 0;
}

static bool pt_windows_cleanup_retry_case(pt_fixture *fx) {
    pt_cleanup_retry retry = {0};
    if (!pt_retry_prepare(fx, &retry))
        return false;
    bool blocked = false;
    if (!pt_retry_first_move(&retry, &blocked))
        return false;
    if (blocked) {
        wchar_t moved[PT_PATH];
        if (!pt_wide(retry.moved, moved) || !pt_retry_path_absent(moved))
            return false;
    } else if (!pt_retry_replacement(fx, &retry)) {
        fprintf(stderr, "pinned-tree cleanup retry retained fixture: original=%s moved=%s\n",
                retry.root, retry.moved);
        return false;
    }
    /* After the initial close, READY verification is forbidden. These native
     * checks prove the retained original/child are intact in either branch. */
    if (fx->tree != retry.owner || cbm_pinned_tree_view(fx->tree) ||
        !pt_retry_directory_is(retry.root, &retry.original) ||
        !pt_equal_file(retry.child, "caller-retained", 15) || cbm_unlink(retry.child) != 0)
        return false;
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent);
}
#endif

TEST(tree_h_cleanup_owner_and_unrelated_resources) {
    pt_fixture fx = {0};
    int result = 1;
    char root[PT_PATH] = {0}, unknown[PT_PATH] = {0}, tracked[PT_PATH] = {0}, moved[PT_PATH] = {0};
    bool have_unknown = false, replacement = false;
    PT_CHECK(pt_setup_step("setup:init", pt_init(&fx, 40)) &&
             pt_setup_step("setup:populate", pt_populate(&fx)) &&
             pt_setup_step("setup:facts", pt_open_facts(&fx)) &&
             pt_setup_step("setup:control", pt_fact_control(&fx)));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_HEAD);
    PT_CHECK(pt_create_ok(&fx, &options));
    strcpy(root, cbm_pinned_tree_view(fx.tree)->root);
    PT_CHECK(pt_join(unknown, root, "unrelated.bin") && pt_write(unknown, "unrelated", 9));
    have_unknown = true;
    cbm_pinned_tree_t *identity = fx.tree;
    cbm_pinned_tree_error_t error;
    PT_CHECK(cbm_pinned_tree_close(&fx.tree, &error) == CBM_PINNED_TREE_CLEANUP_REQUIRED);
    PT_CHECK(fx.tree == identity && !cbm_pinned_tree_view(fx.tree) &&
             error.status == CBM_PINNED_TREE_CLEANUP_REQUIRED && !strcmp(error.cleanup_path, root));
    PT_CHECK(pt_equal_file(unknown, "unrelated", 9));
    cbm_git_facts_free(fx.facts);
    fx.facts = NULL;
    memset(&options, 0xa5, sizeof(options));
    PT_CHECK(cbm_pinned_tree_close(&fx.tree, &error) == CBM_PINNED_TREE_CLEANUP_REQUIRED);
    PT_CHECK(pt_equal_file(unknown, "unrelated", 9));
    PT_CHECK(cbm_unlink(unknown) == 0);
    have_unknown = false;
    PT_CHECK(pt_close_one(&fx.tree) && pt_parent_only_sentinel(fx.parent));
    PT_CHECK(pt_open_facts(&fx) && pt_fact_control(&fx));
    options = pt_options(&fx, CBM_GIT_REV_HEAD);
    PT_CHECK(pt_create_ok(&fx, &options));
    strcpy(root, cbm_pinned_tree_view(fx.tree)->root);
    PT_CHECK(pt_join(tracked, root, "binary.bin") && pt_join(moved, root, "saved-original"));
    PT_CHECK(cbm_rename_noreplace(tracked, moved) == 0);
    replacement = true;
    PT_CHECK(pt_write(tracked, "replacement", 11));
    identity = fx.tree;
    PT_CHECK(cbm_pinned_tree_close(&fx.tree, &error) == CBM_PINNED_TREE_CLEANUP_REQUIRED);
    PT_CHECK(fx.tree == identity && !cbm_pinned_tree_view(fx.tree) &&
             error.cause == CBM_PINNED_TREE_CHANGED);
    PT_CHECK(pt_equal_file(tracked, "replacement", 11) &&
             pt_equal_file(moved, pt_binary, sizeof(pt_binary)));
    PT_CHECK(cbm_unlink(tracked) == 0 && cbm_rename_noreplace(moved, tracked) == 0);
    replacement = false;
    PT_CHECK(pt_close_one(&fx.tree) && pt_parent_only_sentinel(fx.parent));
    PT_CHECK(pt_link_case(&fx));
    PT_CHECK(pt_hardlink_case(&fx));
    PT_CHECK(pt_root_replacement_case(&fx));
#ifdef _WIN32
    PT_CHECK(pt_windows_cleanup_retry_case(&fx));
#else
    /* WHY native Windows handle recovery is inapplicable here; tried the POSIX
     * replacement/unexpected-child paths above, which retain their assertions. */
#endif
    /* Git and caller-owned parent sentinels are still usable after retries. */
    PT_CHECK(pt_setup_step("setup:control", pt_fact_control(&fx)));
    char sentinel[PT_PATH];
    PT_CHECK(pt_join(sentinel, fx.parent, "sentinel") &&
             pt_equal_file(sentinel, "parent-owned\n", 13));
    result = 0;
done:
    if (have_unknown && cbm_unlink(unknown) != 0)
        result = 1;
    if (replacement) {
        if (cbm_file_exists(tracked) && th_unlink_force(tracked) != 0)
            result = 1;
        if (cbm_rename_noreplace(moved, tracked) != 0)
            result = 1;
    }
    return pt_finish(&fx, result);
}

SUITE(test_impact_tree) {
    RUN_TEST(tree_a_pinned_head_merge_base_and_owned_lifetime);
    RUN_TEST(tree_b_manifest_golden_and_empty_inventory);
    RUN_TEST(tree_c_complete_inventory_and_native_names);
    RUN_TEST(tree_d_native_collision_and_exact_spelling);
    RUN_TEST(tree_e_native_permissions_and_acl_policy);
    RUN_TEST(tree_e_windows_split_owner_dacl_is_owner_only);
    RUN_TEST(tree_f_resource_limits_and_local_control);
    RUN_TEST(tree_g_verify_state_and_complete_inventory);
    RUN_TEST(tree_h_cleanup_owner_and_unrelated_resources);
}

/* Independent prefix-read acceptance: no publisher/admission claims. All
 * mutations are synchronous and restricted to the fixture's owned namespace. */
static bool ptr_setup(pt_fixture *fx, unsigned width) {
    return pt_setup_step("read:init", pt_init(fx, width)) &&
           pt_setup_step("read:populate", pt_populate(fx)) &&
           pt_setup_step("read:facts", pt_open_facts(fx)) &&
           pt_setup_step("read:control", pt_fact_control(fx));
}

static cbm_pinned_tree_control_t ptr_control(void) {
    return (cbm_pinned_tree_control_t){.deadline_ms = cbm_now_ms() + 120000U};
}

static bool ptr_start(pt_fixture *fx) {
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    return pt_create_ok(fx, &options);
}

static bool ptr_status(const char *stage, cbm_pinned_tree_status_t actual,
                       cbm_pinned_tree_status_t expected, const cbm_pinned_tree_error_t *error) {
    return pt_h_tree_step("prefix-read", stage, actual == expected && error->status == expected,
                          actual, error);
}

static bool ptr_canary(const unsigned char *bytes, size_t begin, size_t end) {
    for (size_t i = begin; i < end; i++)
        if (bytes[i] != 0xa5)
            return false;
    return true;
}

static bool ptr_read_ok(pt_fixture *fx, cbm_pinned_tree_t *tree, size_t index, size_t capacity,
                        bool ancestor) {
    const pt_row *row = &fx->rows[index];
    const unsigned char *expected = ancestor && index == 4 ? pt_before : row->bytes;
    unsigned char *bytes = cbm_arena_alloc(&fx->arena, capacity + 16);
    if (!bytes)
        return false;
    memset(bytes, 0xa5, capacity + 16);
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(tree);
    if (!view)
        return false;
    unsigned char digest[32];
    memcpy(digest, view->manifest_sha256, sizeof(digest));
    cbm_pinned_tree_control_t control = ptr_control();
    cbm_pinned_tree_error_t error;
    memset(&error, 0xa5, sizeof(error));
    size_t copied = SIZE_MAX;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_read_prefix(
        tree, index, row->length ? row->length : 1, bytes, capacity, &copied, &control, &error);
    size_t wanted = capacity < row->length ? capacity : row->length;
    view = cbm_pinned_tree_view(tree);
    return ptr_status("positive", status, CBM_PINNED_TREE_OK, &error) &&
           error.cause == CBM_PINNED_TREE_OK && copied == wanted &&
           (!wanted || !memcmp(bytes, expected, wanted)) &&
           ptr_canary(bytes, wanted, capacity + 16) && view &&
           !memcmp(digest, view->manifest_sha256, sizeof(digest));
}

static bool ptr_zero_null(cbm_pinned_tree_t *tree) {
    cbm_pinned_tree_control_t control = ptr_control();
    size_t copied = SIZE_MAX;
    return cbm_pinned_tree_read_prefix(tree, 3, PT_LARGE, NULL, 0, &copied, &control, NULL) ==
               CBM_PINNED_TREE_OK &&
           copied == 0 && cbm_pinned_tree_view(tree);
}

static int ptr_bytes_width(unsigned width) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(ptr_setup(&fx, width) && ptr_start(&fx));
    for (size_t i = 0; i < fx.count; i++)
        PT_CHECK(ptr_read_ok(&fx, fx.tree, i, fx.rows[i].length + 7, false));
    PT_CHECK(ptr_read_ok(&fx, fx.tree, 3, 37, false)); /* Includes raw Ctrl-Z. */
    PT_CHECK(ptr_read_ok(&fx, fx.tree, 0, 2, false));  /* Includes raw NUL. */
    PT_CHECK(ptr_read_ok(&fx, fx.tree, 3, 0, false) && ptr_zero_null(fx.tree));
    cbm_pinned_tree_control_t control = ptr_control();
    cbm_pinned_tree_error_t error;
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &control, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_close_one(&fx.tree) && pt_parent_only_sentinel(fx.parent));
    cbm_pinned_tree_options_t options = pt_options(&fx, CBM_GIT_REV_MERGE_BASE);
    PT_CHECK(cbm_pinned_tree_create(&options, &fx.tree, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_view_matches(&fx, cbm_pinned_tree_view(fx.tree), true));
    cbm_git_facts_free(fx.facts);
    fx.facts = NULL;
    PT_CHECK(ptr_read_ok(&fx, fx.tree, 4, 32, true));
    PT_CHECK(ptr_read_ok(&fx, fx.tree, 3, PT_LARGE, true));
    PT_CHECK(cbm_pinned_tree_verify(fx.tree, &control, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(pt_close_one(&fx.tree) && pt_parent_only_sentinel(fx.parent));
    result = 0;
done:
    return pt_finish(&fx, result);
}

TEST(tree_read_a_bytes_and_lifetime) {
    if (ptr_bytes_width(40) != 0)
        return 1;
    return ptr_bytes_width(64);
}

/* Disposed owners cannot publish another read or be made READY by verify. */
static bool ptr_disposal(cbm_pinned_tree_t *tree) {
    cbm_pinned_tree_control_t control = ptr_control();
    pt_cancel counter = {0};
    control.cancelled = pt_cancelled;
    control.cancel_context = &counter;
    cbm_pinned_tree_error_t error;
    unsigned char byte = 0xa5;
    size_t copied = SIZE_MAX;
    cbm_pinned_tree_status_t status =
        cbm_pinned_tree_read_prefix(tree, 0, 6, &byte, 1, &copied, &control, &error);
    return ptr_status("disposal-preflight", status, CBM_PINNED_TREE_INVALID, &error) &&
           copied == 0 && byte == 0xa5 && counter.calls == 0 && !cbm_pinned_tree_view(tree) &&
           !cbm_pinned_tree_test_set_read_fault(tree, CBM_PINNED_TREE_READ_FAULT_NONE) &&
           cbm_pinned_tree_verify(tree, &control, &error) != CBM_PINNED_TREE_OK &&
           !cbm_pinned_tree_view(tree);
}

/* All bad operands are valid memory; the test does not exercise alias UB. */
static bool ptr_preflight_case(pt_fixture *fx, unsigned kind) {
    cbm_pinned_tree_t *tree = kind == 0 ? NULL : fx->tree;
    size_t index = kind == 1 ? SIZE_MAX : kind == 2 ? fx->count : 0;
    uint64_t cap = kind == 3 ? 0 : kind == 8 ? sizeof(pt_binary) - 1 : sizeof(pt_binary);
    unsigned char byte = 0xa5;
    unsigned char *output = kind == 4 ? NULL : &byte;
    size_t copied = SIZE_MAX;
    size_t *copied_arg = kind == 5 ? NULL : &copied;
    cbm_pinned_tree_control_t control = ptr_control();
    pt_cancel counter = {0};
    control.cancelled = pt_cancelled;
    control.cancel_context = &counter;
    if (kind == 7)
        control.deadline_ms = 0;
    const cbm_pinned_tree_control_t *control_arg = kind == 6 ? NULL : &control;
    cbm_pinned_tree_error_t error;
    memset(&error, 0xa5, sizeof(error));
    cbm_pinned_tree_status_t wanted = kind == 8 ? CBM_PINNED_TREE_LIMIT : CBM_PINNED_TREE_INVALID;
    cbm_pinned_tree_status_t status =
        cbm_pinned_tree_read_prefix(tree, index, cap, output, 1, copied_arg, control_arg, &error);
    return ptr_status("argument-preflight", status, wanted, &error) && (kind == 5 || copied == 0) &&
           byte == 0xa5 && counter.calls == 0 &&
           pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false);
}

TEST(tree_read_b_preflight_and_pending_fault) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(ptr_setup(&fx, 40) && ptr_start(&fx));
    PT_CHECK(ptr_read_ok(&fx, fx.tree, 0, sizeof(pt_binary), false));
    PT_CHECK(!cbm_pinned_tree_test_set_read_fault(NULL, CBM_PINNED_TREE_READ_FAULT_READ));
    PT_CHECK(cbm_pinned_tree_test_set_read_fault(fx.tree, CBM_PINNED_TREE_READ_FAULT_READ));
    PT_CHECK(!cbm_pinned_tree_test_set_read_fault(fx.tree, (cbm_pinned_tree_read_fault_t)999));
    for (unsigned kind = 0; kind < 9; kind++)
        PT_CHECK(ptr_preflight_case(&fx, kind));
    cbm_pinned_tree_control_t control = ptr_control();
    cbm_pinned_tree_error_t error;
    unsigned char byte = 0xa5;
    size_t copied = SIZE_MAX;
    cbm_pinned_tree_status_t status =
        cbm_pinned_tree_read_prefix(fx.tree, 0, 6, &byte, 1, &copied, &control, &error);
    PT_CHECK(ptr_status("pending-read-fault", status, CBM_PINNED_TREE_IO, &error));
    PT_CHECK(copied == 0 && ptr_disposal(fx.tree));
    PT_CHECK(pt_close_one(&fx.tree) && pt_parent_only_sentinel(fx.parent));
    PT_CHECK(ptr_start(&fx));
    PT_CHECK(ptr_preflight_case(&fx, 8));
    PT_CHECK(ptr_read_ok(&fx, fx.tree, 0, 1, false)); /* Exact cap, after LIMIT. */
    result = 0;
done:
    return pt_finish(&fx, result);
}

static bool ptr_cancel_once(pt_fixture *fx, size_t stop, bool *cancelled) {
    if (!ptr_start(fx))
        return false;
    cbm_pinned_tree_control_t control = ptr_control();
    pt_cancel counter = {.stop = stop};
    control.cancelled = pt_cancelled;
    control.cancel_context = &counter;
    cbm_pinned_tree_error_t error;
    unsigned char prefix[37];
    size_t copied = SIZE_MAX;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_read_prefix(
        fx->tree, 3, PT_LARGE, prefix, sizeof(prefix), &copied, &control, &error);
    bool ok = error.status == status;
    if (status == CBM_PINNED_TREE_OK)
        ok = ok && counter.calls < stop && copied == sizeof(prefix) &&
             !memcmp(prefix, fx->large, sizeof(prefix)) &&
             pt_view_matches(fx, cbm_pinned_tree_view(fx->tree), false);
    else {
        ok = ok && status == CBM_PINNED_TREE_CANCELLED && counter.calls >= stop && copied == 0 &&
             ptr_disposal(fx->tree);
        *cancelled = true;
    }
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
}

TEST(tree_read_c_cancellation_and_deadline) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(ptr_setup(&fx, 40) && ptr_start(&fx));
    PT_CHECK(ptr_read_ok(&fx, fx.tree, 3, 37, false));
    cbm_pinned_tree_control_t control = ptr_control();
    pt_cancel counter = {0};
    control.cancelled = pt_cancelled;
    control.cancel_context = &counter;
    unsigned char prefix[37];
    cbm_pinned_tree_error_t error;
    size_t copied = SIZE_MAX;
    PT_CHECK(cbm_pinned_tree_read_prefix(fx.tree, 3, PT_LARGE, prefix, sizeof(prefix), &copied,
                                         &control, &error) == CBM_PINNED_TREE_OK);
    PT_CHECK(copied == sizeof(prefix) && !memcmp(prefix, fx.large, sizeof(prefix)));
    PT_CHECK(counter.calls > 0 && counter.calls < SIZE_MAX / 8 && pt_close_one(&fx.tree));
    size_t total = counter.calls;
    bool cancelled = false;
    for (size_t i = 0; i < 7; i++) {
        size_t stop = i == 6 ? total + 1 : 1 + (total - 1) * i / 5;
        PT_CHECK(ptr_cancel_once(&fx, stop, &cancelled));
    }
    PT_CHECK(cancelled && ptr_start(&fx));
    control = ptr_control();
    control.deadline_ms = 1;
    copied = SIZE_MAX;
    PT_CHECK(cbm_now_ms() > 1);
    cbm_pinned_tree_status_t status = cbm_pinned_tree_read_prefix(
        fx.tree, 0, 6, prefix, sizeof(prefix), &copied, &control, &error);
    PT_CHECK(ptr_status("expired", status, CBM_PINNED_TREE_DEADLINE, &error));
    PT_CHECK(copied == 0 && ptr_disposal(fx.tree));
    PT_CHECK(pt_close_one(&fx.tree) && pt_parent_only_sentinel(fx.parent));
    PT_CHECK(ptr_start(&fx) && ptr_read_ok(&fx, fx.tree, 3, PT_LARGE, false));
    result = 0;
done:
    return pt_finish(&fx, result);
}

static bool ptr_changed(pt_fixture *fx, size_t index, bool missing) {
    cbm_pinned_tree_control_t control = ptr_control();
    cbm_pinned_tree_error_t error;
    unsigned char byte = 0xa5;
    size_t copied = SIZE_MAX;
    cbm_pinned_tree_status_t status =
        cbm_pinned_tree_read_prefix(fx->tree, index, PT_LARGE, &byte, 1, &copied, &control, &error);
    bool typed = status == CBM_PINNED_TREE_CHANGED || (missing && status == CBM_PINNED_TREE_IO);
    return pt_h_tree_step("prefix-read", "native-change", typed && error.status == status, status,
                          &error) &&
           copied == 0 && ptr_disposal(fx->tree);
}

static bool ptr_change_suffix(const char *path) {
    if (!pt_make_writable(path))
        return false;
    FILE *file = cbm_fopen(path, "r+b");
    if (!file)
        return false;
    unsigned char changed = (unsigned char)(((PT_LARGE - 1) % 251) ^ 0xff);
    bool ok = fseek(file, PT_LARGE - 1, SEEK_SET) == 0 && fwrite(&changed, 1, 1, file) == 1;
    if (fclose(file) != 0)
        ok = false;
    return pt_restore_readonly(path, false) && ok;
}

static bool ptr_content_case(pt_fixture *fx, unsigned kind) {
    if (!ptr_start(fx))
        return false;
    size_t index = kind == 0 ? 3 : 0;
    if (!ptr_read_ok(fx, fx->tree, index, 1, false))
        return false;
    char path[PT_PATH];
    if (!pt_join(path, cbm_pinned_tree_view(fx->tree)->root, fx->rows[index].path))
        return false;
    bool changed;
    if (kind == 0)
        changed = ptr_change_suffix(path);
    else {
        unsigned char bytes[sizeof(pt_binary) + 1];
        memcpy(bytes, pt_binary, sizeof(pt_binary));
        bytes[sizeof(pt_binary)] = 0xee;
        size_t length = kind == 1 ? sizeof(pt_binary) - 1 : sizeof(bytes);
        changed = pt_make_writable(path) && pt_write(path, bytes, length) &&
                  pt_restore_readonly(path, false);
    }
    bool ok = changed && ptr_changed(fx, index, false);
    /* These remain owned identities. No byte restoration is needed for disposal. */
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
}

static bool ptr_file_identity_case(pt_fixture *fx, bool replacement) {
    if (!ptr_start(fx) || !ptr_read_ok(fx, fx->tree, 0, 1, false))
        return false;
    char path[PT_PATH], saved[PT_PATH];
    if (!pt_join(path, cbm_pinned_tree_view(fx->tree)->root, "binary.bin") ||
        !pt_join(saved, fx->parent, "read-parked-file") || cbm_rename_noreplace(path, saved) != 0)
        return false;
    bool installed = false;
    bool ready = true;
    if (replacement) {
        installed = pt_write(path, pt_binary, sizeof(pt_binary));
        ready = installed && pt_restore_readonly(path, false);
    }
    bool ok = ready && ptr_changed(fx, 0, !replacement) &&
              pt_equal_file(saved, pt_binary, sizeof(pt_binary));
    if (installed && (!pt_make_writable(path) || cbm_unlink(path) != 0))
        return false;
    if (cbm_rename_noreplace(saved, path) != 0)
        return false;
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
}

static bool ptr_make_link(const char *path, const char *alias) {
#ifdef _WIN32
    wchar_t a[PT_PATH], b[PT_PATH];
    return pt_wide(alias, a) && pt_wide(path, b) && CreateHardLinkW(a, b, NULL);
#else
    return link(path, alias) == 0;
#endif
}

static bool ptr_hardlink_case(pt_fixture *fx) {
    if (!ptr_start(fx) || !ptr_read_ok(fx, fx->tree, 0, 1, false))
        return false;
    char path[PT_PATH], alias[PT_PATH];
    if (!pt_join(path, cbm_pinned_tree_view(fx->tree)->root, "binary.bin") ||
        !pt_join(alias, fx->parent, "read-outside-link") || !ptr_make_link(path, alias))
        return false;
    bool ok = ptr_changed(fx, 0, false) && pt_equal_file(alias, pt_binary, sizeof(pt_binary));
#ifdef _WIN32
    if (!pt_make_writable(alias))
        return false;
#endif
    if (cbm_unlink(alias) != 0 || !pt_restore_readonly(path, false))
        return false;
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
}

TEST(tree_read_d_selected_file_mutations) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(ptr_setup(&fx, 40));
    for (unsigned kind = 0; kind < 3; kind++)
        PT_CHECK(ptr_content_case(&fx, kind));
    PT_CHECK(ptr_file_identity_case(&fx, false));
    PT_CHECK(ptr_file_identity_case(&fx, true));
    PT_CHECK(ptr_hardlink_case(&fx));
    result = 0;
done:
    return pt_finish(&fx, result);
}

static bool ptr_ancestor_path(pt_fixture *fx, unsigned level, char path[PT_PATH]) {
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(fx->tree);
    if (!view)
        return false;
    if (level == 2)
        return pt_join(path, view->root, "nested");
    strcpy(path, level == 0 ? fx->parent : view->root);
    return true;
}

static bool ptr_set_acl(pt_fixture *fx, const char *path, bool add) {
#ifdef _WIN32
    return pt_windows_acl(fx, path, add);
#elif defined(__APPLE__)
    (void)fx;
    return pt_mac_named_acl(path, add);
#else
    (void)fx;
    return pt_linux_acl(path, false, add);
#endif
}

static bool ptr_ancestor_policy(pt_fixture *fx, unsigned level, bool acl) {
    if (!ptr_start(fx) || !ptr_read_ok(fx, fx->tree, 4, 1, false))
        return false;
    char path[PT_PATH];
    if (!ptr_ancestor_path(fx, level, path))
        return false;
    bool installed = false;
    if (acl)
        installed = ptr_set_acl(fx, path, true);
#ifndef _WIN32
    else
        installed = chmod(path, 0750) == 0;
#endif
    pt_wire before;
    bool observed = installed;
    if (acl)
        observed =
            observed && pt_acl_snapshot(fx, path, &before) && pt_acl_preserved(fx, path, &before);
#ifndef _WIN32
    if (!acl)
        observed = observed && pt_passing_posix_mode(path, 0750);
#endif
    bool ok = observed && ptr_changed(fx, 4, false);
    if (acl && observed)
        ok = pt_acl_preserved(fx, path, &before) && ok;
#ifndef _WIN32
    if (!acl && observed)
        ok = pt_passing_posix_mode(path, 0750) && ok;
#endif
    bool restored = false;
    if (acl)
        restored = ptr_set_acl(fx, path, false);
#ifndef _WIN32
    else
        restored = chmod(path, 0700) == 0;
#endif
    if (!restored || !pt_policy(path, true, false))
        return false;
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
}

static bool ptr_ancestor_replacement(pt_fixture *fx, unsigned level) {
    if (!ptr_start(fx) || !ptr_read_ok(fx, fx->tree, 4, 1, false))
        return false;
    char original[PT_PATH], saved[PT_PATH], marker[PT_PATH];
    if (!ptr_ancestor_path(fx, level, original) ||
        !pt_join(saved, fx->parent, "read-parked-directory"))
        return false;
    bool blocked = false;
    if (!pt_relocate_directory(original, saved, &blocked))
        return false;
    if (blocked) {
        /* WHY/observed native error are recorded by the relocation helper. */
        bool ok = ptr_read_ok(fx, fx->tree, 4, 7, false);
        cbm_pinned_tree_control_t control = ptr_control();
        cbm_pinned_tree_error_t error;
        ok = cbm_pinned_tree_verify(fx->tree, &control, &error) == CBM_PINNED_TREE_OK && ok;
        return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
    }
    bool installed = cbm_mkdir(original) == 0;
    bool marked =
        installed && pt_join(marker, original, "caller-owned") && pt_write(marker, "keep", 4);
    bool ok = marked && ptr_changed(fx, 4, false) && pt_equal_file(marker, "keep", 4);
    if (marked && cbm_unlink(marker) != 0)
        return false;
    if (installed && th_rmtree(original) != 0)
        return false;
    if (!pt_move_owned_directory(saved, original))
        return false;
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
}

TEST(tree_read_e_ancestor_policy_and_identity) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(ptr_setup(&fx, 40));
    for (unsigned level = 0; level < 3; level++) {
        PT_CHECK(ptr_ancestor_policy(&fx, level, true));
#ifndef _WIN32
        PT_CHECK(ptr_ancestor_policy(&fx, level, false));
#else
        fprintf(stderr, "prefix-read mode applicability: WHY Windows has no POSIX mode policy; "
                        "tried the native protected-DACL mutation on this same ancestor\n");
#endif
    }
    PT_CHECK(ptr_ancestor_replacement(&fx, 1));
    PT_CHECK(ptr_ancestor_replacement(&fx, 2));
    result = 0;
done:
    return pt_finish(&fx, result);
}

static bool ptr_present(const char *path) {
#ifdef _WIN32
    wchar_t wide[PT_PATH];
    return pt_wide(path, wide) && GetFileAttributesW(wide) != INVALID_FILE_ATTRIBUTES;
#else
    struct stat st;
    return lstat(path, &st) == 0;
#endif
}

static bool ptr_fault_case(pt_fixture *fx, cbm_pinned_tree_read_fault_t fault, bool corrupt) {
    if (!ptr_start(fx))
        return false;
    size_t index = fault == CBM_PINNED_TREE_READ_FAULT_EOF ? 2 : 3;
    if (!ptr_read_ok(fx, fx->tree, index, 1, false))
        return false;
    char root[PT_PATH], path[PT_PATH];
    strcpy(root, cbm_pinned_tree_view(fx->tree)->root);
    if (!pt_join(path, root, fx->rows[index].path) ||
        !cbm_pinned_tree_test_set_read_fault(fx->tree, fault))
        return false;
    if (corrupt && !ptr_change_suffix(path))
        return false;
    cbm_pinned_tree_control_t control = ptr_control();
    cbm_pinned_tree_error_t error;
    unsigned char prefix[37];
    size_t copied = SIZE_MAX;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_read_prefix(
        fx->tree, index, PT_LARGE, prefix, sizeof(prefix), &copied, &control, &error);
    cbm_pinned_tree_status_t expected = fault == CBM_PINNED_TREE_READ_FAULT_CLOSE
                                            ? CBM_PINNED_TREE_CLEANUP_REQUIRED
                                            : CBM_PINNED_TREE_IO;
    bool ok = ptr_status("simulated-boundary", status, expected, &error) && copied == 0 &&
              error.cause == (corrupt ? CBM_PINNED_TREE_CHANGED : CBM_PINNED_TREE_IO) &&
              !cbm_pinned_tree_view(fx->tree) && fx->tree;
    if (fault == CBM_PINNED_TREE_READ_FAULT_CLOSE)
        ok = ok && !strcmp(error.cleanup_path, root);
    /* Namespace persists; CLOSE retains a known handle, not a real failed POSIX
     * close. Ordinary disposal is the required recovery, with no rewrite. */
    ok = ok && ptr_present(root) && ptr_present(path) && ptr_disposal(fx->tree);
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent) && ok;
}

static bool ptr_fault_consumption(pt_fixture *fx) {
    if (!ptr_start(fx) || !ptr_read_ok(fx, fx->tree, 2, 1, false))
        return false;
    if (!cbm_pinned_tree_test_set_read_fault(fx->tree, CBM_PINNED_TREE_READ_FAULT_READ) ||
        !ptr_read_ok(fx, fx->tree, 2, 1, false) || !ptr_read_ok(fx, fx->tree, 0, 6, false))
        return false;
    if (!cbm_pinned_tree_test_set_read_fault(fx->tree, CBM_PINNED_TREE_READ_FAULT_EOF) ||
        !cbm_pinned_tree_test_set_read_fault(fx->tree, CBM_PINNED_TREE_READ_FAULT_NONE) ||
        !ptr_read_ok(fx, fx->tree, 0, 6, false))
        return false;
    return pt_close_one(&fx->tree) && pt_parent_only_sentinel(fx->parent);
}

static bool ptr_owner_isolation(pt_fixture *fx) {
    if (!ptr_start(fx) || !ptr_read_ok(fx, fx->tree, 0, 6, false))
        return false;
    cbm_pinned_tree_options_t options = pt_options(fx, CBM_GIT_REV_HEAD);
    cbm_pinned_tree_error_t error;
    if (cbm_pinned_tree_create(&options, &fx->second, &error) != CBM_PINNED_TREE_OK ||
        !pt_view_matches(fx, cbm_pinned_tree_view(fx->second), false) ||
        !cbm_pinned_tree_test_set_read_fault(fx->tree, CBM_PINNED_TREE_READ_FAULT_EOF) ||
        !ptr_read_ok(fx, fx->second, 3, PT_LARGE, false))
        return false;
    cbm_pinned_tree_control_t control = ptr_control();
    size_t copied = SIZE_MAX;
    cbm_pinned_tree_status_t status =
        cbm_pinned_tree_read_prefix(fx->tree, 2, 1, NULL, 0, &copied, &control, &error);
    bool ok = ptr_status("per-owner-fault", status, CBM_PINNED_TREE_IO, &error) && copied == 0 &&
              ptr_disposal(fx->tree) && ptr_read_ok(fx, fx->second, 0, 6, false);
    return pt_close_one(&fx->tree) && pt_close_one(&fx->second) &&
           pt_parent_only_sentinel(fx->parent) && ok;
}

TEST(tree_read_f_faults_and_owner_isolation) {
    pt_fixture fx = {0};
    int result = 1;
    PT_CHECK(ptr_setup(&fx, 40));
    PT_CHECK(ptr_fault_consumption(&fx));
    PT_CHECK(ptr_fault_case(&fx, CBM_PINNED_TREE_READ_FAULT_READ, false));
    PT_CHECK(ptr_fault_case(&fx, CBM_PINNED_TREE_READ_FAULT_EOF, false));
    PT_CHECK(ptr_fault_case(&fx, CBM_PINNED_TREE_READ_FAULT_CLOSE, false));
    PT_CHECK(ptr_fault_case(&fx, CBM_PINNED_TREE_READ_FAULT_CLOSE, true));
    PT_CHECK(ptr_owner_isolation(&fx));
    result = 0;
done:
    return pt_finish(&fx, result);
}

SUITE(test_impact_tree_read) {
    RUN_TEST(tree_read_a_bytes_and_lifetime);
    RUN_TEST(tree_read_b_preflight_and_pending_fault);
    RUN_TEST(tree_read_c_cancellation_and_deadline);
    RUN_TEST(tree_read_d_selected_file_mutations);
    RUN_TEST(tree_read_e_ancestor_policy_and_identity);
    RUN_TEST(tree_read_f_faults_and_owner_isolation);
}
