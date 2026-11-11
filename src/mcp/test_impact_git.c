#include "mcp/test_impact_git.h"
#include "mcp/test_impact.h"

#include "foundation/arena.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"
#include "foundation/secure_random.h"
#include "foundation/subprocess.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <aclapi.h>
#include "foundation/win_utf8.h"
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

enum {
    GF_ARENA_BLOCK = 8192,
    GF_PATH_CAP = 4096,
    GF_ARG_CAP = 96,
    GF_FLAG_CAP = 32,
    GF_INPUT_MAX = 65536,
    GF_POLL_US = 10000
};

typedef struct {
    size_t arena_limit, arena_used;
    size_t input_limit, input_used;
} gf_batch_budget_t;

typedef struct {
    const char *commit;
    cbm_git_tree_inventory_t inventory;
} gf_inventory_cache_t;

struct cbm_git_facts {
    CBMArena arena;
    cbm_git_facts_options_t options;
    cbm_git_facts_identity_t identity;
    const char
        *command_git_dir; /* worktree directory while resolving refs, common directory later */
    const char *git_pins[GF_FLAG_CAP];
    int git_pin_count;
    const char *diff_flags[GF_FLAG_CAP];
    int diff_flag_count;
    const char *graft_paths[2];
    const char *shallow_paths[2];
    unsigned commands;
    size_t output_bytes;
    cbm_git_facts_error_t stopped;
    gf_batch_budget_t *batch_budget; /* borrowed stack state during one serialized call */
    gf_inventory_cache_t inventory_cache[2];
    bool no_lazy_fetch; /* the executable takes --no-lazy-fetch (gf_probe_no_lazy_fetch) */
};

typedef struct {
    cbm_git_bytes_t out;
    cbm_git_bytes_t err;
    int exit_code;
} gf_result_t;

typedef struct {
    char directory[GF_PATH_CAP];
    char out[GF_PATH_CAP];
    char err[GF_PATH_CAP];
    char in[GF_PATH_CAP];
} gf_capture_t;

static void gf_error_clear(cbm_git_facts_error_t *error) {
    memset(error, 0, sizeof(*error));
    error->exit_code = -1;
}

static bool gf_error(cbm_git_facts_error_t *error, cbm_git_facts_status_t status,
                     const char *message) {
    error->status = status;
    (void)snprintf(error->diagnostic, sizeof(error->diagnostic), "%s", message);
    return false;
}

/* Exhausting any query budget makes the handle terminal. In particular, an
 * observed over-cap capture cannot be retried without charging its bytes. */
static bool gf_limit(cbm_git_facts_t *facts, cbm_git_facts_error_t *error, const char *message) {
    gf_error(error, CBM_GIT_FACTS_LIMIT, message);
    facts->stopped = *error;
    return false;
}

static size_t gf_input_length(const char *text) {
    if (!text)
        return SIZE_MAX;
    size_t n = strnlen(text, (size_t)GF_INPUT_MAX + 1);
    return n <= GF_INPUT_MAX ? n : SIZE_MAX;
}

static bool gf_absolute(const char *path) {
    if (!path || !path[0])
        return false;
#ifdef _WIN32
    bool has_prefix = path[1] && path[2];
    bool drive = has_prefix &&
                 ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
                 path[1] == ':' && (path[2] == '/' || path[2] == '\\');
    bool unc = has_prefix && (path[0] == '/' || path[0] == '\\') &&
               (path[1] == '/' || path[1] == '\\') && path[2] != '/' && path[2] != '\\';
    return drive || unc;
#else
    return path[0] == '/';
#endif
}

static bool gf_native_executable(const char *path) {
    if (!gf_absolute(path))
        return false;
#ifdef _WIN32
    size_t n = strlen(path);
    return n >= 4 && path[n - 4] == '.' && (path[n - 3] == 'e' || path[n - 3] == 'E') &&
           (path[n - 2] == 'x' || path[n - 2] == 'X') && (path[n - 1] == 'e' || path[n - 1] == 'E');
#else
    return true; /* trusted internal resolution, not an arbitrary user program */
#endif
}

static bool gf_gate(cbm_git_facts_t *facts, cbm_git_facts_error_t *error) {
    if (facts->stopped.status != CBM_GIT_FACTS_OK) {
        *error = facts->stopped;
        return false;
    }
    if (facts->options.cancelled && facts->options.cancelled(facts->options.cancel_context)) {
        gf_error(error, CBM_GIT_FACTS_CANCELLED, "Git facts request cancelled");
        facts->stopped = *error;
        return false;
    }
    if (cbm_now_ms() >= facts->options.deadline_ms)
        return gf_error(error, CBM_GIT_FACTS_DEADLINE, "Git facts deadline expired");
    return true;
}

/* A batch charges logical requests before allocating, including allocations
 * in inherited guards. Legacy calls retain their original allocation policy. */
static void *gf_allocate(cbm_git_facts_t *facts, CBMArena *arena, size_t size,
                         cbm_git_facts_error_t *error) {
    gf_batch_budget_t *budget = facts->batch_budget;
    if (budget) {
        if (!gf_gate(facts, error))
            return NULL;
        if (size > budget->arena_limit - budget->arena_used) {
            gf_limit(facts, error, "Git batch arena limit exceeded");
            return NULL;
        }
        budget->arena_used += size;
    }
    void *out = cbm_arena_alloc(arena, size);
    if (!out) {
        gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain Git facts data");
        return NULL;
    }
    if (budget && !gf_gate(facts, error))
        return NULL;
    return out;
}

static bool gf_metadata_equal(cbm_git_facts_t *facts, const char *a, const char *b, bool *equal,
                              cbm_git_facts_error_t *error) {
    if (!facts->batch_budget) {
        *equal = strcmp(a, b) == 0;
        return true;
    }
    size_t at = 0;
    for (;;) {
        if ((at & 65535u) == 0 && !gf_gate(facts, error))
            return false;
        unsigned char x = (unsigned char)a[at], y = (unsigned char)b[at];
        if (x != y || !x) {
            *equal = x == y;
            return true;
        }
        at++;
    }
}

/* These are compile-time project constants containing simple space-separated
 * argv tokens, not user command text. No shell quoting or expansion is used. */
static bool gf_split_flags(cbm_git_facts_t *facts, const char *literal, const char **tokens,
                           int *count) {
    *count = 0;
    char *copy = cbm_arena_strdup(&facts->arena, literal);
    if (!copy)
        return false;
    char *p = copy;
    while (*p) {
        while (*p == ' ')
            p++;
        if (!*p)
            break;
        if (*count == GF_FLAG_CAP)
            return false;
        tokens[(*count)++] = p;
        while (*p && *p != ' ') {
            if (*p == '\t' || *p == '\n' || *p == '\r' || *p == '\'' || *p == '"')
                return false;
            p++;
        }
        if (*p)
            *p++ = '\0';
    }
    return true;
}

#ifdef _WIN32
/* Same owner-only, protected, inheritable ACL as compat.c's private helper,
 * but deliberately no inherited-security fallback. TOKEN_USER storage is
 * arena-owned; Windows owns the ACL returned by SetEntriesInAclW. */
static bool gf_private_directory(cbm_git_facts_t *facts, char path[GF_PATH_CAP],
                                 cbm_git_facts_error_t *error) {
    CBMArena scratch;
    cbm_arena_init_lazy(&scratch, 1024);
    HANDLE token = NULL;
    TOKEN_USER *user = NULL;
    PACL acl = NULL;
    DWORD needed = 0;
    bool created = false;
    const char *tmp = cbm_tmpdir();
    if (!gf_absolute(tmp)) {
        gf_error(error, CBM_GIT_FACTS_IO, "An absolute private temp location is required");
        goto done;
    }
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token) ||
        GetTokenInformation(token, TokenUser, NULL, 0, &needed) ||
        GetLastError() != ERROR_INSUFFICIENT_BUFFER || needed == 0 || needed > 65536) {
        gf_error(error, CBM_GIT_FACTS_IO, "Cannot establish private capture owner");
        goto done;
    }
    user = gf_allocate(facts, &scratch, needed, error);
    if (!user) {
        if (error->status == CBM_GIT_FACTS_OOM)
            gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain private capture owner");
        goto done;
    }
    if (!GetTokenInformation(token, TokenUser, user, needed, &needed) || !user->User.Sid ||
        !IsValidSid(user->User.Sid)) {
        gf_error(error, CBM_GIT_FACTS_IO, "Cannot establish private capture owner");
        goto done;
    }
    EXPLICIT_ACCESSW access;
    memset(&access, 0, sizeof(access));
    access.grfAccessPermissions = GENERIC_ALL;
    access.grfAccessMode = SET_ACCESS;
    access.grfInheritance = SUB_CONTAINERS_AND_OBJECTS_INHERIT;
    access.Trustee.TrusteeForm = TRUSTEE_IS_SID;
    access.Trustee.TrusteeType = TRUSTEE_IS_USER;
    access.Trustee.ptstrName = (LPWSTR)user->User.Sid;
    SECURITY_DESCRIPTOR descriptor;
    if (SetEntriesInAclW(1, &access, NULL, &acl) != ERROR_SUCCESS ||
        !InitializeSecurityDescriptor(&descriptor, SECURITY_DESCRIPTOR_REVISION) ||
        !SetSecurityDescriptorDacl(&descriptor, TRUE, acl, FALSE) ||
        !SetSecurityDescriptorOwner(&descriptor, user->User.Sid, FALSE) ||
        !SetSecurityDescriptorControl(&descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED)) {
        gf_error(error, CBM_GIT_FACTS_IO, "Cannot establish private capture permissions");
        goto done;
    }
    SECURITY_ATTRIBUTES attributes = {sizeof(attributes), &descriptor, FALSE};
    static const char hex[] = "0123456789abcdef";
    for (int attempt = 0; attempt < 16; attempt++) {
        if (!gf_gate(facts, error))
            goto done;
        unsigned char random[16];
        char suffix[33];
        if (!cbm_secure_random(random, sizeof(random)))
            break;
        for (size_t i = 0; i < sizeof(random); i++) {
            suffix[i * 2] = hex[random[i] >> 4];
            suffix[i * 2 + 1] = hex[random[i] & 15];
        }
        suffix[32] = '\0';
        int n = snprintf(path, GF_PATH_CAP, "%s/cbm-git-facts-%s", tmp, suffix);
        if (n < 0 || n >= GF_PATH_CAP)
            break;
        wchar_t *wide = cbm_path_to_wide_in(CBM_MEM_CLASS_OTHER, path);
        if (!wide)
            break;
        created = CreateDirectoryW(wide, &attributes) != 0;
        DWORD why = GetLastError();
        cbm_free(CBM_MEM_CLASS_OTHER, wide);
        if (created || (why != ERROR_ALREADY_EXISTS && why != ERROR_FILE_EXISTS))
            break;
    }
    if (!created)
        gf_error(error, CBM_GIT_FACTS_IO, "Private capture directory creation failed");
done:
    if (acl)
        (void)LocalFree(acl);
    if (token)
        (void)CloseHandle(token);
    cbm_arena_destroy(&scratch);
    if (!created)
        path[0] = '\0';
    return created;
}
#else
static bool gf_private_directory(cbm_git_facts_t *facts, char path[GF_PATH_CAP],
                                 cbm_git_facts_error_t *error) {
    if (!gf_gate(facts, error))
        return false;
    int n = snprintf(path, GF_PATH_CAP, "%s/cbm-git-facts-XXXXXX", cbm_tmpdir());
    if (n < 0 || n >= GF_PATH_CAP || !cbm_mkdtemp(path)) {
        path[0] = '\0';
        return gf_error(error, CBM_GIT_FACTS_IO, "Private capture directory creation failed");
    }
    return true;
}
#endif

static bool gf_capture_cleanup(gf_capture_t *capture) {
    bool ok = true;
    if (capture->out[0] && cbm_unlink(capture->out) != 0 && errno != ENOENT)
        ok = false;
    if (capture->err[0] && cbm_unlink(capture->err) != 0 && errno != ENOENT)
        ok = false;
    if (capture->in[0] && cbm_unlink(capture->in) != 0 && errno != ENOENT)
        ok = false;
    if (capture->directory[0] && cbm_rmdir(capture->directory) != 0 && errno != ENOENT)
        ok = false;
    return ok;
}

static bool gf_capture_create(cbm_git_facts_t *facts, gf_capture_t *capture,
                              cbm_git_facts_error_t *error) {
    memset(capture, 0, sizeof(*capture));
    if (!gf_private_directory(facts, capture->directory, error))
        return false;
    int a = snprintf(capture->out, sizeof(capture->out), "%s/stdout", capture->directory);
    int b = snprintf(capture->err, sizeof(capture->err), "%s/stderr", capture->directory);
    if (a < 0 || b < 0 || (size_t)a >= sizeof(capture->out) || (size_t)b >= sizeof(capture->err)) {
        capture->out[0] = capture->err[0] = '\0';
        (void)gf_capture_cleanup(capture);
        return gf_error(error, CBM_GIT_FACTS_IO, "Capture path exceeds supported length");
    }
    return true;
}

/* The private writer is closed before spawn. Only canonical OID lines reach
 * this transport; the child opens its independent read-only binary handle. */
static bool gf_capture_input(cbm_git_facts_t *facts, gf_capture_t *capture,
                             const cbm_git_bytes_t *input, cbm_git_facts_error_t *error) {
    if (!input)
        return true;
    gf_batch_budget_t *budget = facts->batch_budget;
    if (!budget || (input->length && !input->data))
        return gf_error(error, CBM_GIT_FACTS_INVALID, "Invalid internal Git batch input");
    if (!gf_gate(facts, error))
        return false;
    if (input->length > budget->input_limit - budget->input_used)
        return gf_limit(facts, error, "Git batch stdin limit exceeded");
    int n = snprintf(capture->in, sizeof(capture->in), "%s/stdin", capture->directory);
    if (n < 0 || (size_t)n >= sizeof(capture->in)) {
        capture->in[0] = '\0';
        return gf_error(error, CBM_GIT_FACTS_IO, "Batch input path exceeds supported length");
    }
    FILE *file = cbm_fopen(capture->in, "wb");
    if (!file)
        return gf_error(error, CBM_GIT_FACTS_IO, "Cannot create private Git batch input");
    bool ok = true;
    size_t at = 0;
    while (at < input->length) {
        if (!gf_gate(facts, error)) {
            ok = false;
            break;
        }
        size_t chunk = input->length - at;
        if (chunk > 65536)
            chunk = 65536;
        size_t written = fwrite(input->data + at, 1, chunk, file);
        at += written;
        budget->input_used += written;
        if (written != chunk) {
            ok = gf_error(error, CBM_GIT_FACTS_IO, "Cannot write private Git batch input");
            break;
        }
    }
    if (fclose(file) != 0 && ok)
        ok = gf_error(error, CBM_GIT_FACTS_IO, "Cannot close private Git batch input");
    return ok && gf_gate(facts, error);
}

static bool gf_capture_sizes(cbm_git_facts_t *facts, const gf_capture_t *capture, size_t *out_size,
                             size_t *err_size, cbm_git_facts_error_t *error) {
    int64_t a = cbm_file_size(capture->out), b = cbm_file_size(capture->err);
    if (a < 0 || b < 0)
        return gf_error(error, CBM_GIT_FACTS_IO, "Cannot inspect captured output");
    if ((uint64_t)a > facts->options.stdout_limit || (uint64_t)b > facts->options.stderr_limit ||
        (uint64_t)a > SIZE_MAX - (uint64_t)b)
        return gf_limit(facts, error, "Git command output limit exceeded");
    size_t total = (size_t)a + (size_t)b;
    if (facts->output_bytes > facts->options.total_output_limit ||
        total > facts->options.total_output_limit - facts->output_bytes)
        return gf_limit(facts, error, "Git request output limit exceeded");
    if (out_size)
        *out_size = (size_t)a;
    if (err_size)
        *err_size = (size_t)b;
    return true;
}

static bool gf_read_capture(cbm_git_facts_t *facts, const char *path, size_t length,
                            cbm_git_bytes_t *out, cbm_git_facts_error_t *error) {
    *out = (cbm_git_bytes_t){0};
    if (length == SIZE_MAX)
        return gf_limit(facts, error, "Captured output is too large");
    unsigned char *bytes = gf_allocate(facts, &facts->arena, length + 1, error);
    if (!bytes) {
        if (error->status == CBM_GIT_FACTS_OOM)
            gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain captured output");
        return false;
    }
    FILE *file = cbm_fopen(path, "rb");
    if (!file)
        return gf_error(error, CBM_GIT_FACTS_IO, "Cannot read captured output");
    bool ok = true;
    size_t used = 0;
    while (used < length) {
        if (!gf_gate(facts, error)) {
            ok = false;
            break;
        }
        size_t chunk = length - used;
        if (chunk > 65536)
            chunk = 65536;
        size_t got = fread(bytes + used, 1, chunk, file);
        used += got;
        if (got != chunk) {
            ok = gf_error(error, CBM_GIT_FACTS_IO, "Captured output was truncated or unreadable");
            break;
        }
    }
    if (ok && (fgetc(file) != EOF || ferror(file)))
        ok = gf_error(error, CBM_GIT_FACTS_IO, "Captured output changed while being read");
    if (fclose(file) != 0 && ok)
        ok = gf_error(error, CBM_GIT_FACTS_IO, "Cannot close captured output");
    if (ok)
        ok = gf_gate(facts, error);
    if (!ok)
        return false;
    bytes[length] = '\0'; /* convenience sentinel; length remains authoritative */
    *out = (cbm_git_bytes_t){bytes, length};
    return true;
}

static void gf_command_diagnostic(cbm_git_facts_error_t *error, const gf_result_t *result) {
    error->exit_code = result->exit_code;
    size_t at = strlen(error->diagnostic);
    if (result->err.length && at + 2 < sizeof(error->diagnostic)) {
        error->diagnostic[at++] = ':';
        error->diagnostic[at++] = ' ';
    }
    for (size_t i = 0; i < result->err.length && at + 1 < sizeof(error->diagnostic); i++) {
        unsigned char c = result->err.data[i];
        error->diagnostic[at++] = c >= 32 && c < 127 ? (char)c : ' ';
    }
    error->diagnostic[at] = '\0';
}

/* Observed file-size limits request cancellation. They are not instantaneous
 * disk quotas; final retained bytes are bounded independently after quiescence. */
static bool gf_run_input(cbm_git_facts_t *facts, const char *const *arguments, int count,
                         bool allow_exit_one, const cbm_git_bytes_t *input, gf_result_t *out,
                         cbm_git_facts_error_t *error) {
    memset(out, 0, sizeof(*out));
    out->exit_code = -1;
    if (!gf_gate(facts, error))
        return false;
    if (facts->commands >= facts->options.command_limit)
        return gf_limit(facts, error, "Git command-count limit exceeded");
    const char *argv[GF_ARG_CAP];
    int argc = 0;
    const char *prefix[] = {facts->options.git_executable,
                            "--no-replace-objects",
                            "--no-pager",
                            "--literal-pathspecs",
                            "-C",
                            facts->options.root,
                            "-c",
                            "protocol.allow=never",
                            "-c",
                            "core.fsmonitor=false"};
    size_t prefix_count = sizeof(prefix) / sizeof(prefix[0]);
    size_t directory_count = facts->command_git_dir ? 4 : 0;
    if (count < 1 || count >= GF_ARG_CAP ||
        (size_t)count + prefix_count + directory_count + (size_t)facts->git_pin_count + 2 >
            GF_ARG_CAP)
        return gf_error(error, CBM_GIT_FACTS_INVALID, "Invalid internal Git command");
    for (size_t i = 0; i < prefix_count; i++)
        argv[argc++] = prefix[i];
    if (facts->no_lazy_fetch)
        argv[argc++] = "--no-lazy-fetch";
    if (facts->command_git_dir) {
        argv[argc++] = "--git-dir";
        argv[argc++] = facts->command_git_dir;
        argv[argc++] = "--work-tree";
        argv[argc++] = facts->identity.root;
    }
    for (int i = 0; i < facts->git_pin_count; i++)
        argv[argc++] = facts->git_pins[i];
    for (int i = 0; i < count; i++)
        argv[argc++] = arguments[i];
    argv[argc] = NULL;
    gf_capture_t capture;
    if (!gf_capture_create(facts, &capture, error))
        return false;
    bool ok = gf_gate(facts, error) && gf_capture_input(facts, &capture, input, error);
    cbm_subprocess_t *process = NULL;
    cbm_proc_result_t result = {0};
    if (ok) {
        cbm_proc_opts_t options = {.bin = facts->options.git_executable,
                                   .argv = argv,
                                   .log_file = capture.err,
                                   .quiet_timeout_ms = 0,
                                   .cancel_grace_ms = CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS,
                                   .strip_git_repo_env = true,
                                   .stdout_file = capture.out,
                                   .stdin_file = input ? capture.in : NULL};
        facts->commands++;
        if (cbm_subprocess_spawn(&options, &process) != 0)
            ok = gf_error(error, CBM_GIT_FACTS_SPAWN, "Cannot spawn native Git process");
    }
    if (process) {
        for (;;) {
            if (ok)
                ok = gf_gate(facts, error) && gf_capture_sizes(facts, &capture, NULL, NULL, error);
            if (!ok)
                (void)cbm_subprocess_request_cancel(process);
            cbm_proc_poll_t polled = cbm_subprocess_poll(process, &result);
            if (polled == CBM_PROC_POLL_TERMINAL)
                break;
            if (polled == CBM_PROC_POLL_ERROR && ok)
                ok = gf_error(error, CBM_GIT_FACTS_SUPERVISION, "Git process supervision failed");
            /* Valid owned handles never have an ERROR transition. Continue
             * driving cancellation rather than abandon a possibly live tree. */
            cbm_usleep(GF_POLL_US);
        }
        cbm_subprocess_destroy(process);
        if (!result.tree_quiesced || result.supervision_failed) {
            gf_error(error, CBM_GIT_FACTS_SUPERVISION, "Git process tree did not quiesce");
            (void)snprintf(error->diagnostic, sizeof(error->diagnostic),
                           "Git containment failed; retained capture directory: %.400s",
                           capture.directory);
            facts->stopped = *error;
            return false; /* do not claim files with possible writers were cleaned */
        }
        if (ok && (result.cancellation_requested || result.forced))
            ok = gf_error(error, CBM_GIT_FACTS_CANCELLED, "Git process was cancelled");
    }
    size_t stdout_size = 0, stderr_size = 0;
    if (ok)
        ok = gf_gate(facts, error) &&
             gf_capture_sizes(facts, &capture, &stdout_size, &stderr_size, error);
    gf_result_t captured = {.exit_code = result.exit_code};
    if (ok) {
        facts->output_bytes += stdout_size + stderr_size;
        ok = gf_read_capture(facts, capture.err, stderr_size, &captured.err, error) &&
             gf_read_capture(facts, capture.out, stdout_size, &captured.out, error);
    }
    /* New batches classify a known command failure before cleanup. Cleanup
     * must not replace an established error; legacy calls keep their ordering. */
    if (ok && facts->batch_budget && !(result.outcome == CBM_PROC_CLEAN && result.exit_code == 0) &&
        !(allow_exit_one && result.outcome == CBM_PROC_EXIT_NONZERO && result.exit_code == 1)) {
        ok = gf_error(error, CBM_GIT_FACTS_COMMAND, "Git command failed");
        gf_command_diagnostic(error, &captured);
    }
    bool cleaned = gf_capture_cleanup(&capture);
    if (!cleaned && ok) {
        ok = gf_error(error, CBM_GIT_FACTS_IO, "Cannot clean captured Git output");
        (void)snprintf(error->diagnostic, sizeof(error->diagnostic),
                       "Cannot clean retained capture directory: %.400s", capture.directory);
    }
    if (ok)
        ok = gf_gate(facts, error);
    if (!ok)
        return false;
    bool success = result.outcome == CBM_PROC_CLEAN && result.exit_code == 0;
    bool one = allow_exit_one && result.outcome == CBM_PROC_EXIT_NONZERO && result.exit_code == 1;
    if (!success && !one) {
        gf_error(error, CBM_GIT_FACTS_COMMAND, "Git command failed");
        gf_command_diagnostic(error, &captured);
        return false;
    }
    *out = captured;
    return true;
}

static bool gf_run(cbm_git_facts_t *facts, const char *const *arguments, int count,
                   bool allow_exit_one, gf_result_t *out, cbm_git_facts_error_t *error) {
    return gf_run_input(facts, arguments, count, allow_exit_one, NULL, out, error);
}

static bool gf_line(const cbm_git_bytes_t *bytes, const unsigned char **text, size_t *length) {
    if (!bytes->length || bytes->data[bytes->length - 1] != '\n')
        return false;
    size_t n = bytes->length - 1;
#ifdef _WIN32
    if (n && bytes->data[n - 1] == '\r')
        n--;
#endif
    if (memchr(bytes->data, '\0', n) || memchr(bytes->data, '\n', n) ||
        memchr(bytes->data, '\r', n))
        return false;
    *text = bytes->data;
    *length = n;
    return true;
}

static bool gf_active_line(cbm_git_facts_t *facts, const cbm_git_bytes_t *bytes,
                           const unsigned char **text, size_t *length,
                           cbm_git_facts_error_t *error) {
    if (!facts->batch_budget)
        return gf_line(bytes, text, length) ||
               gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git metadata line");
    if (!bytes->length || bytes->data[bytes->length - 1] != '\n')
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git metadata line");
    size_t n = bytes->length - 1;
#ifdef _WIN32
    if (n && bytes->data[n - 1] == '\r')
        n--;
#endif
    for (size_t at = 0; at < n; at++) {
        if ((at & 65535u) == 0 && !gf_gate(facts, error))
            return false;
        unsigned char c = bytes->data[at];
        if (!c || c == '\n' || c == '\r')
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git metadata line");
    }
    *text = bytes->data;
    *length = n;
    return gf_gate(facts, error);
}

static bool gf_owned_line(cbm_git_facts_t *facts, const cbm_git_bytes_t *bytes, const char **out,
                          cbm_git_facts_error_t *error) {
    const unsigned char *text;
    size_t n;
    if (!gf_active_line(facts, bytes, &text, &n, error))
        return false;
    if (!facts->batch_budget) {
        *out = cbm_arena_strndup(&facts->arena, (const char *)text, n);
        return *out || gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain Git metadata");
    }
    if (n == SIZE_MAX)
        return gf_limit(facts, error, "Git metadata allocation overflow");
    char *copy = gf_allocate(facts, &facts->arena, n + 1, error);
    if (!copy)
        return false;
    for (size_t at = 0; at < n;) {
        if (!gf_gate(facts, error))
            return false;
        size_t chunk = n - at;
        if (chunk > 65536)
            chunk = 65536;
        memcpy(copy + at, text + at, chunk);
        at += chunk;
    }
    copy[n] = '\0';
    if (!gf_gate(facts, error))
        return false;
    *out = copy;
    return true;
}

static bool gf_oid(char out[65], const unsigned char *text, size_t length, unsigned width) {
    if ((width != 40 && width != 64) || length != width)
        return false;
    for (unsigned i = 0; i < width; i++) {
        unsigned char c = text[i];
        if (c >= 'A' && c <= 'F')
            c = (unsigned char)(c + ('a' - 'A'));
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return false;
        out[i] = (char)c;
    }
    out[width] = '\0';
    return true;
}

static bool gf_oid_line(cbm_git_facts_t *facts, const cbm_git_bytes_t *bytes, char out[65],
                        cbm_git_facts_error_t *error) {
    const unsigned char *text;
    size_t n;
    if (!gf_line(bytes, &text, &n) || !gf_oid(out, text, n, facts->identity.oid_hex_length))
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Git returned an invalid full object ID");
    return true;
}

static bool gf_resolve(cbm_git_facts_t *facts, const char *ref, char out[65],
                       cbm_git_facts_error_t *error) {
    size_t n = gf_input_length(ref);
    if (n == SIZE_MAX || n == 0 || ref[0] == '-')
        return gf_error(error, CBM_GIT_FACTS_INVALID, "A bounded commit reference is required");
    char *peel = cbm_arena_alloc(&facts->arena, n + sizeof("^{commit}"));
    if (!peel)
        return gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain commit reference");
    memcpy(peel, ref, n);
    memcpy(peel + n, "^{commit}", sizeof("^{commit}"));
    const char *args[] = {"rev-parse", "--verify", "--end-of-options", peel};
    gf_result_t result;
    return gf_run(facts, args, 4, false, &result, error) &&
           gf_oid_line(facts, &result.out, out, error);
}

#ifdef _WIN32
/* MSYS2 and Cygwin git print absolute paths in their own namespace:
 * /c/Users/x (Cygwin: /cygdrive/c/Users/x). The drive form maps one to one
 * onto C:/Users/x, which the native file checks below need. Any other path of
 * that namespace (/tmp, /usr) needs the runtime's mount table, so it stays
 * unmapped and is refused as not absolute. */
static const char *gf_native_metadata_path(cbm_git_facts_t *facts, const char *path) {
    const char *drive = strncmp(path, "/cygdrive/", 10) == 0 ? path + 9 : path;
    bool letter = (drive[1] >= 'A' && drive[1] <= 'Z') || (drive[1] >= 'a' && drive[1] <= 'z');
    if (drive[0] != '/' || !letter || (drive[2] != '/' && drive[2] != '\0'))
        return path;
    char upper = (char)(drive[1] >= 'a' ? drive[1] - 'a' + 'A' : drive[1]);
    return cbm_arena_sprintf(&facts->arena, "%c:/%s", upper, drive[2] ? drive + 3 : "");
}
#endif

static bool gf_rev_metadata(cbm_git_facts_t *facts, const char *option, bool absolute,
                            const char **out, cbm_git_facts_error_t *error) {
    const char *args[] = {"rev-parse", "--path-format=absolute", option};
    gf_result_t result;
    if (!gf_run(facts, args, 3, false, &result, error) ||
        !gf_owned_line(facts, &result.out, out, error))
        return false;
#ifdef _WIN32
    if (absolute && !(*out = gf_native_metadata_path(facts, *out)))
        return gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain Git metadata path");
#endif
    if (absolute && !gf_absolute(*out))
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Git metadata path is not absolute");
    return true;
}

/* Metadata queries call gf_run directly; the executor must not call this guard.
 * This checks persistent topology consistency, not an atomic filesystem snapshot.
 * Explicit directory selection prevents a root gitfile from redirecting later
 * commands; the check also rejects a new commondir indirection at that selection. */
static bool gf_topology_guard(cbm_git_facts_t *facts, cbm_git_facts_error_t *error) {
    if (!gf_gate(facts, error))
        return false;
    const char *common_dir;
    if (!facts->command_git_dir || !facts->identity.common_dir)
        return gf_error(error, CBM_GIT_FACTS_INVALID, "Git directory identity is not established");
    if (!gf_rev_metadata(facts, "--git-common-dir", true, &common_dir, error))
        return false;
    bool equal;
    if (!gf_metadata_equal(facts, common_dir, facts->identity.common_dir, &equal, error))
        return false;
    if (!equal)
        return gf_error(error, CBM_GIT_FACTS_IDENTITY_MISMATCH,
                        "Git common directory identity changed");
    return gf_gate(facts, error);
}

/* --no-replace-objects is not treated as a promise about legacy info/grafts.
 * Check graft and shallow state in both per-worktree and common Git locations.
 * No graft parsing or environment mutation; uncertain files block proof. */
static bool gf_history_file_absent_or_empty(const char *path, cbm_git_facts_error_t *error) {
#ifdef _WIN32
    wchar_t *wide = cbm_path_to_wide_in(CBM_MEM_CLASS_OTHER, path);
    if (!wide)
        return gf_error(error, CBM_GIT_FACTS_IO, "Cannot inspect Git history override path");
    HANDLE file =
        CreateFileW(wide, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    DWORD open_error = GetLastError();
    cbm_free(CBM_MEM_CLASS_OTHER, wide);
    if (file == INVALID_HANDLE_VALUE) {
        if (open_error == ERROR_FILE_NOT_FOUND || open_error == ERROR_PATH_NOT_FOUND)
            return true;
        return gf_error(error, CBM_GIT_FACTS_IO, "Cannot inspect Git history override state");
    }
    BY_HANDLE_FILE_INFORMATION info;
    bool ok =
        GetFileType(file) == FILE_TYPE_DISK && GetFileInformationByHandle(file, &info) != 0 &&
        (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
        info.nFileSizeHigh == 0 && info.nFileSizeLow == 0;
    bool closed = CloseHandle(file) != 0;
#else
    int flags = O_RDONLY | O_NONBLOCK;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    int file = open(path, flags);
    if (file < 0) {
        if (errno == ENOENT)
            return true;
        return gf_error(error, CBM_GIT_FACTS_IO, "Cannot inspect Git history override state");
    }
    struct stat info;
    bool ok = fstat(file, &info) == 0 && S_ISREG(info.st_mode) && info.st_size == 0;
    bool closed = close(file) == 0;
#endif
    if (!ok)
        return gf_error(error, CBM_GIT_FACTS_UNSUPPORTED,
                        "Graft or shallow state prevents history certification");
    return closed ||
           gf_error(error, CBM_GIT_FACTS_IO, "Cannot close Git history override inspection");
}

static bool gf_history_guard(cbm_git_facts_t *facts, cbm_git_facts_error_t *error) {
    if (!gf_topology_guard(facts, error))
        return false;
    for (int i = 0; i < 2; i++) {
        if (i == 1) {
            bool equal;
            if (!gf_metadata_equal(facts, facts->graft_paths[0], facts->graft_paths[1], &equal,
                                   error))
                return false;
            if (equal)
                continue;
        }
        if (!gf_history_file_absent_or_empty(facts->graft_paths[i], error) ||
            !gf_history_file_absent_or_empty(facts->shallow_paths[i], error))
            return false;
    }
    return gf_gate(facts, error);
}

static bool gf_open_identity(cbm_git_facts_t *facts, cbm_git_facts_error_t *error) {
    const char *prefix;
    if (!gf_rev_metadata(facts, "--show-prefix", false, &prefix, error))
        return false;
    if (prefix[0])
        return gf_error(error, CBM_GIT_FACTS_INVALID, "Git root must be a worktree top-level");
    if (!gf_rev_metadata(facts, "--show-toplevel", true, &facts->identity.root, error))
        return false;
    facts->options.root = facts->identity.root;
    if (!gf_rev_metadata(facts, "--absolute-git-dir", true, &facts->identity.git_dir, error))
        return false;
    /* HEAD and worktree-local refs must be resolved in this worktree's Git
     * directory, even if its root gitfile is repointed after discovery. */
    facts->command_git_dir = facts->identity.git_dir;
    if (!gf_rev_metadata(facts, "--git-common-dir", true, &facts->identity.common_dir, error))
        return false;
    facts->graft_paths[0] =
        cbm_arena_sprintf(&facts->arena, "%s/info/grafts", facts->identity.git_dir);
    facts->graft_paths[1] =
        cbm_arena_sprintf(&facts->arena, "%s/info/grafts", facts->identity.common_dir);
    facts->shallow_paths[0] =
        cbm_arena_sprintf(&facts->arena, "%s/shallow", facts->identity.git_dir);
    facts->shallow_paths[1] =
        cbm_arena_sprintf(&facts->arena, "%s/shallow", facts->identity.common_dir);
    if (!facts->graft_paths[0] || !facts->graft_paths[1] || !facts->shallow_paths[0] ||
        !facts->shallow_paths[1])
        return gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain Git history guard paths");
    if (!gf_history_guard(facts, error))
        return false;
    const char *format, *shallow;
    if (!gf_rev_metadata(facts, "--show-object-format=storage", false, &format, error))
        return false;
    if (strcmp(format, "sha1") == 0)
        facts->identity.oid_hex_length = 40;
    else if (strcmp(format, "sha256") == 0)
        facts->identity.oid_hex_length = 64;
    else
        return gf_error(error, CBM_GIT_FACTS_UNSUPPORTED, "Unsupported Git object format");
    if (!gf_rev_metadata(facts, "--is-shallow-repository", false, &shallow, error))
        return false;
    if (strcmp(shallow, "false") != 0)
        return gf_error(error, CBM_GIT_FACTS_UNSUPPORTED, "Complete local Git history is required");
    if (!gf_resolve(facts, "HEAD", facts->identity.head, error) ||
        !gf_resolve(facts, facts->options.base_ref, facts->identity.base, error))
        return false;
    /* Reject a persistent commondir redirect during ref resolution before
     * switching to OID-only access through the captured common directory. */
    if (!gf_history_guard(facts, error))
        return false;
    if (facts->options.expected_head) {
        char expected[65];
        if (!gf_oid(expected, (const unsigned char *)facts->options.expected_head,
                    strlen(facts->options.expected_head), facts->identity.oid_hex_length))
            return gf_error(error, CBM_GIT_FACTS_INVALID, "Expected HEAD must be a full object ID");
        if (strcmp(expected, facts->identity.head) != 0)
            return gf_error(error, CBM_GIT_FACTS_IDENTITY_MISMATCH,
                            "HEAD does not match expected identity");
    }
    facts->command_git_dir = facts->identity.common_dir;
    const char *args[] = {"merge-base", "--all", facts->identity.base, facts->identity.head};
    gf_result_t result;
    if (!gf_history_guard(facts, error) || !gf_run(facts, args, 4, true, &result, error) ||
        !gf_history_guard(facts, error))
        return false;
    if (result.exit_code == 1 && result.out.length == 0)
        return gf_error(error, CBM_GIT_FACTS_NO_MERGE_BASE, "No local common ancestor exists");
    if (result.exit_code != 0 || result.out.length == 0)
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed common ancestor result");
    /* Parse every line before classifying multiple ancestors as ambiguous;
     * stderr never participates in the list. No arbitrary first base wins. */
    size_t at = 0;
    unsigned bases = 0;
    char candidate[65];
    while (at < result.out.length) {
        const unsigned char *nl = memchr(result.out.data + at, '\n', result.out.length - at);
        if (!nl)
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed common ancestor list");
        size_t n = (size_t)(nl - (result.out.data + at)) + 1;
        cbm_git_bytes_t line = {result.out.data + at, n};
        if (!gf_oid_line(facts, &line, candidate, error))
            return false;
        if (bases == 0)
            memcpy(facts->identity.merge_base, candidate, facts->identity.oid_hex_length + 1);
        if (bases < 2)
            bases++;
        at += n;
    }
    if (bases != 1)
        return gf_error(error, CBM_GIT_FACTS_AMBIGUOUS_MERGE_BASE,
                        "Multiple common ancestors are unsupported");
    return gf_gate(facts, error);
}

/* --no-lazy-fetch (git 2.44) keeps an object missing from a partial clone
 * from being fetched. Older git rejects the option, which failed every engine
 * command (Ubuntu 24.04 ships git 2.43). There `-c protocol.allow=never`
 * already turns such a fetch into an error, which is what the option asks
 * for, so it is passed only when the executable accepts it: one probe per
 * facts session, outside the command budget, output discarded. */
static bool gf_probe_no_lazy_fetch(const char *git) {
    const char *argv[] = {git, "--no-lazy-fetch", "--version", NULL};
    cbm_proc_opts_t options = {.bin = git,
                               .argv = argv,
                               .cancel_grace_ms = CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS,
                               .strip_git_repo_env = true};
    cbm_proc_result_t result = {0};
    return cbm_subprocess_run(&options, &result) == 0 && result.outcome == CBM_PROC_CLEAN;
}

cbm_git_facts_t *cbm_git_facts_open(const cbm_git_facts_options_t *options,
                                    cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    if (!options || gf_input_length(options->root) == SIZE_MAX ||
        gf_input_length(options->base_ref) == SIZE_MAX ||
        gf_input_length(options->git_executable) == SIZE_MAX ||
        (options->expected_head && gf_input_length(options->expected_head) == SIZE_MAX) ||
        !gf_absolute(options->root) || !gf_native_executable(options->git_executable) ||
        !options->base_ref[0] || options->base_ref[0] == '-' || !options->deadline_ms ||
        !options->command_limit || !options->stdout_limit || !options->stderr_limit ||
        !options->total_output_limit) {
        gf_error(error, CBM_GIT_FACTS_INVALID,
                 "Invalid Git facts options or missing finite limits");
        return NULL;
    }
    if (options->expected_head) {
        size_t width = strlen(options->expected_head);
        char checked[65];
        if ((width != 40 && width != 64) ||
            !gf_oid(checked, (const unsigned char *)options->expected_head, width,
                    (unsigned)width)) {
            gf_error(error, CBM_GIT_FACTS_INVALID, "Expected HEAD must be a full object ID");
            return NULL;
        }
    }
    CBMArena arena;
    cbm_arena_init_sized(&arena, GF_ARENA_BLOCK);
    cbm_git_facts_t *facts = cbm_arena_calloc(&arena, sizeof(*facts));
    if (!facts) {
        cbm_arena_destroy(&arena);
        gf_error(error, CBM_GIT_FACTS_OOM, "Cannot allocate Git facts");
        return NULL;
    }
    facts->arena = arena;
    facts->options = *options;
    facts->options.root = cbm_arena_strdup(&facts->arena, options->root);
    facts->options.base_ref = cbm_arena_strdup(&facts->arena, options->base_ref);
    facts->options.git_executable = cbm_arena_strdup(&facts->arena, options->git_executable);
    facts->options.expected_head =
        options->expected_head ? cbm_arena_strdup(&facts->arena, options->expected_head) : NULL;
    if (!facts->options.root || !facts->options.base_ref || !facts->options.git_executable ||
        (options->expected_head && !facts->options.expected_head) ||
        !gf_split_flags(facts, CBM_TEST_IMPACT_GIT_PINS, facts->git_pins, &facts->git_pin_count) ||
        !gf_split_flags(facts, CBM_TEST_IMPACT_DIFF_FLAGS, facts->diff_flags,
                        &facts->diff_flag_count)) {
        gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain Git facts options");
        cbm_git_facts_free(facts);
        return NULL;
    }
    facts->no_lazy_fetch = gf_probe_no_lazy_fetch(facts->options.git_executable);
    if (!gf_open_identity(facts, error)) {
        cbm_git_facts_free(facts);
        return NULL;
    }
    return facts;
}

void cbm_git_facts_free(cbm_git_facts_t *facts) {
    if (facts) {
        CBMArena arena = facts->arena;
        cbm_arena_destroy(&arena);
    }
}

const cbm_git_facts_identity_t *cbm_git_facts_identity(const cbm_git_facts_t *facts) {
    return facts ? &facts->identity : NULL;
}

cbm_git_ancestry_t cbm_git_facts_is_ancestor(cbm_git_facts_t *facts, const char *ancestor_oid,
                                             const char *descendant_oid,
                                             cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    char ancestor[65], descendant[65];
    if (!facts || !ancestor_oid || !descendant_oid ||
        !gf_oid(ancestor, (const unsigned char *)ancestor_oid, gf_input_length(ancestor_oid),
                facts->identity.oid_hex_length) ||
        !gf_oid(descendant, (const unsigned char *)descendant_oid, gf_input_length(descendant_oid),
                facts->identity.oid_hex_length)) {
        gf_error(error, CBM_GIT_FACTS_INVALID, "Ancestry requires two full object IDs");
        return CBM_GIT_ANCESTRY_ERROR;
    }
    const char *args[] = {"merge-base", "--is-ancestor", ancestor, descendant};
    gf_result_t result;
    if (!gf_history_guard(facts, error) || !gf_run(facts, args, 4, true, &result, error) ||
        !gf_history_guard(facts, error))
        return CBM_GIT_ANCESTRY_ERROR;
    if (result.out.length) {
        gf_error(error, CBM_GIT_FACTS_COMMAND, "Unexpected ancestry stdout");
        return CBM_GIT_ANCESTRY_ERROR;
    }
    return result.exit_code == 0 ? CBM_GIT_ANCESTRY_YES : CBM_GIT_ANCESTRY_NO;
}

static bool gf_relative_path(const char *path, size_t n) {
    if (!path || !n || n > GF_INPUT_MAX || path[0] == '/' || memchr(path, '\0', n))
        return false;
#ifdef _WIN32
    /* Windows Git normalizes backslashes before lookup. Reject that spelling
     * rather than silently selecting a different Git tree path. */
    if (memchr(path, '\\', n) || (n >= 2 && path[1] == ':') ||
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, (int)n, NULL, 0) == 0)
        return false;
#endif
    size_t begin = 0;
    for (size_t i = 0; i <= n; i++) {
        if (i != n && path[i] != '/')
            continue;
        size_t length = i - begin;
        if (!length || (length == 1 && path[begin] == '.') ||
            (length == 2 && path[begin] == '.' && path[begin + 1] == '.'))
            return false;
        begin = i + 1;
    }
    return true;
}

static const char *gf_revision(const cbm_git_facts_t *facts, cbm_git_revision_t revision) {
    switch (revision) {
    case CBM_GIT_REV_HEAD:
        return facts->identity.head;
    case CBM_GIT_REV_BASE:
        return facts->identity.base;
    case CBM_GIT_REV_MERGE_BASE:
        return facts->identity.merge_base;
    default:
        return NULL;
    }
}

/* Inventory paths are data, never argv or filesystem paths. Unlike blob
 * lookup, this validates only tree-path structure, retaining every literal
 * byte spelling so an unsupported relevant entry cannot silently disappear. */
static bool gf_inventory_path(cbm_git_facts_t *facts, const unsigned char *path, size_t n,
                              cbm_git_facts_error_t *error) {
    if (!n || path[0] == '/')
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed tracked-tree path");
    size_t begin = 0;
    for (size_t i = 0; i <= n; i++) {
        if ((i & 65535u) == 0 && !gf_gate(facts, error))
            return false;
        if (i != n && path[i] != '/')
            continue;
        size_t length = i - begin;
        if (!length || (length == 1 && path[begin] == '.') ||
            (length == 2 && path[begin] == '.' && path[begin + 1] == '.'))
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed tracked-tree path");
        begin = i + 1;
    }
    return true;
}

/* Parse one exact <mode> <type> <full-oid> TAB <path> NUL record. Bounds
 * come from captured length, never a guessed line or a strlen(path). */
static bool gf_inventory_record(cbm_git_facts_t *facts, const cbm_git_bytes_t *bytes,
                                size_t *offset, cbm_git_tree_entry_t *out,
                                cbm_git_facts_error_t *error) {
    const unsigned char *end = NULL;
    size_t scan = *offset;
    while (scan < bytes->length) {
        if (!gf_gate(facts, error))
            return false;
        size_t chunk = bytes->length - scan;
        if (chunk > 65536)
            chunk = 65536;
        end = memchr(bytes->data + scan, '\0', chunk);
        if (end)
            break;
        scan += chunk;
    }
    if (!end)
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Truncated tracked-tree record");
    const unsigned char *record = bytes->data + *offset;
    size_t length = (size_t)(end - record);
    if (length < 8 || record[6] != ' ')
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed tracked-tree header");
    cbm_git_tree_entry_t entry = {0};
    for (size_t i = 0; i < 6; i++) {
        if (record[i] < '0' || record[i] > '7')
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed tracked-tree mode");
        entry.mode = entry.mode * 8 + (uint32_t)(record[i] - '0');
    }
    const char *type;
    size_t type_length;
    if (entry.mode == 0100644 || entry.mode == 0100755 || entry.mode == 0120000) {
        entry.object_type = CBM_GIT_TREE_BLOB;
        type = "blob";
        type_length = 4;
    } else if (entry.mode == 0160000) {
        entry.object_type = CBM_GIT_TREE_COMMIT;
        type = "commit";
        type_length = 6;
    } else {
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Invalid tracked-tree mode/type");
    }
    size_t oid_at = 7 + type_length + 1;
    size_t path_at = oid_at + facts->identity.oid_hex_length + 1;
    if (length <= path_at || memcmp(record + 7, type, type_length) != 0 ||
        record[7 + type_length] != ' ' || record[path_at - 1] != '\t' ||
        !gf_oid(entry.oid, record + oid_at, facts->identity.oid_hex_length,
                facts->identity.oid_hex_length))
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed tracked-tree identity/type");
    entry.path = (const char *)record + path_at;
    entry.path_length = length - path_at;
    if (!gf_inventory_path(facts, record + path_at, entry.path_length, error))
        return false;
    *offset = (size_t)(end - bytes->data) + 1;
    if (out)
        *out = entry;
    return true;
}

static bool gf_inventory_compare(cbm_git_facts_t *facts, const cbm_git_tree_entry_t *a,
                                 const cbm_git_tree_entry_t *b, int *comparison,
                                 cbm_git_facts_error_t *error) {
    size_t common = a->path_length < b->path_length ? a->path_length : b->path_length;
    for (size_t at = 0; at < common;) {
        if (!gf_gate(facts, error))
            return false;
        size_t chunk = common - at;
        if (chunk > 65536)
            chunk = 65536;
        int cmp = memcmp(a->path + at, b->path + at, chunk);
        if (cmp) {
            *comparison = cmp < 0 ? -1 : 1;
            return true;
        }
        at += chunk;
    }
    *comparison = a->path_length < b->path_length ? -1 : a->path_length > b->path_length ? 1 : 0;
    return true;
}

/* In-place heapsort: deterministic byte order, no recursive stack or hidden
 * allocation, and request gates during comparisons and each extraction. */
static bool gf_inventory_sift(cbm_git_facts_t *facts, cbm_git_tree_entry_t *entries, size_t root,
                              size_t count, cbm_git_facts_error_t *error) {
    while (root < count / 2) {
        size_t child = root * 2 + 1;
        int cmp;
        if (child + 1 < count) {
            if (!gf_inventory_compare(facts, &entries[child], &entries[child + 1], &cmp, error))
                return false;
            if (cmp < 0)
                child++;
        }
        if (!gf_inventory_compare(facts, &entries[root], &entries[child], &cmp, error))
            return false;
        if (cmp >= 0)
            return true;
        cbm_git_tree_entry_t swap = entries[root];
        entries[root] = entries[child];
        entries[child] = swap;
        root = child;
    }
    return true;
}

static bool gf_inventory_sort(cbm_git_facts_t *facts, cbm_git_tree_entry_t *entries, size_t count,
                              cbm_git_facts_error_t *error) {
    for (size_t i = count / 2; i > 0; i--)
        if (!gf_inventory_sift(facts, entries, i - 1, count, error))
            return false;
    for (size_t end = count; end > 1; end--) {
        if (!gf_gate(facts, error))
            return false;
        cbm_git_tree_entry_t swap = entries[0];
        entries[0] = entries[end - 1];
        entries[end - 1] = swap;
        if (!gf_inventory_sift(facts, entries, 0, end - 1, error))
            return false;
    }
    for (size_t i = 1; i < count; i++) {
        int cmp;
        if (!gf_inventory_compare(facts, &entries[i - 1], &entries[i], &cmp, error))
            return false;
        if (cmp >= 0)
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Duplicate tracked-tree path");
    }
    return gf_gate(facts, error);
}

bool cbm_git_facts_inventory(cbm_git_facts_t *facts, cbm_git_revision_t revision,
                             cbm_git_tree_inventory_t *out, cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    if (out)
        memset(out, 0, sizeof(*out));
    if (!facts || !out || (revision != CBM_GIT_REV_HEAD && revision != CBM_GIT_REV_MERGE_BASE))
        return gf_error(error, CBM_GIT_FACTS_INVALID,
                        "Inventory requires pinned HEAD or actual merge base");
    if (!gf_topology_guard(facts, error))
        return false;
    const char *args[] = {"ls-tree", "-r", "-z", "--full-tree", gf_revision(facts, revision)};
    gf_result_t result;
    if (!gf_run(facts, args, 5, false, &result, error))
        return false;
    /* Validate before allocating rows: malformed NUL-heavy output cannot turn
     * a small capture into a much larger metadata allocation. Every accepted
     * row requires at least one full-OID record in the already charged capture. */
    size_t offset = 0, count = 0;
    while (offset < result.out.length) {
        if (!gf_inventory_record(facts, &result.out, &offset, NULL, error))
            return false;
        if (count == SIZE_MAX / sizeof(cbm_git_tree_entry_t))
            return gf_limit(facts, error, "Tracked-tree inventory allocation limit exceeded");
        count++;
    }
    if (!gf_gate(facts, error))
        return false;
    cbm_git_tree_entry_t *entries = NULL;
    if (count) {
        entries = gf_allocate(facts, &facts->arena, count * sizeof(*entries), error);
        if (!entries) {
            if (error->status == CBM_GIT_FACTS_OOM)
                gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain tracked-tree inventory");
            return false;
        }
        offset = 0;
        for (size_t i = 0; i < count; i++)
            if (!gf_inventory_record(facts, &result.out, &offset, &entries[i], error))
                return false;
        if (!gf_inventory_sort(facts, entries, count, error))
            return false;
    }
    if (!gf_topology_guard(facts, error))
        return false;
    *out = (cbm_git_tree_inventory_t){.entries = entries, .count = count};
    return true;
}

static bool gf_decimal_size(const cbm_git_bytes_t *bytes, size_t *size) {
    const unsigned char *text;
    size_t n;
    if (!gf_line(bytes, &text, &n) || !n)
        return false;
    size_t value = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = text[i];
        if (c < '0' || c > '9' || value > (SIZE_MAX - (size_t)(c - '0')) / 10)
            return false;
        value = value * 10 + (size_t)(c - '0');
    }
    *size = value;
    return true;
}

cbm_git_blob_status_t cbm_git_facts_read_blob(cbm_git_facts_t *facts, cbm_git_revision_t revision,
                                              const char *path, size_t path_length,
                                              cbm_git_blob_t *out, cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    if (out)
        memset(out, 0, sizeof(*out));
    const char *commit = facts ? gf_revision(facts, revision) : NULL;
    if (!facts || !out || !commit || !gf_relative_path(path, path_length)) {
        gf_error(error, CBM_GIT_FACTS_INVALID,
                 "Blob lookup requires a pinned revision and literal relative path");
        return CBM_GIT_BLOB_ERROR;
    }
    if (!gf_topology_guard(facts, error))
        return CBM_GIT_BLOB_ERROR;
    const char *owned_path = cbm_arena_strndup(&facts->arena, path, path_length);
    if (!owned_path) {
        gf_error(error, CBM_GIT_FACTS_OOM, "Cannot retain literal Git path");
        return CBM_GIT_BLOB_ERROR;
    }
    const char *tree_args[] = {"ls-tree", "-z", "--full-tree", commit, "--", owned_path};
    gf_result_t result;
    if (!gf_run(facts, tree_args, 6, false, &result, error))
        return CBM_GIT_BLOB_ERROR;
    if (!result.out.length)
        return gf_topology_guard(facts, error) ? CBM_GIT_BLOB_ABSENT : CBM_GIT_BLOB_ERROR;
    const unsigned char *data = result.out.data;
    size_t length = result.out.length;
    const unsigned char *tab = memchr(data, '\t', length);
    const unsigned char *end = memchr(data, '\0', length);
    if (!tab || !end || end != data + length - 1 || tab >= end ||
        (size_t)(end - tab - 1) != path_length || memcmp(tab + 1, path, path_length) != 0 ||
        (size_t)(tab - data) < 8 || data[6] != ' ') {
        gf_error(error, CBM_GIT_FACTS_COMMAND,
                 "Git tree lookup did not return one exact literal path");
        return CBM_GIT_BLOB_ERROR;
    }
    uint32_t mode = 0;
    for (int i = 0; i < 6; i++) {
        if (data[i] < '0' || data[i] > '7') {
            gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git tree mode");
            return CBM_GIT_BLOB_ERROR;
        }
        mode = mode * 8 + (uint32_t)(data[i] - '0');
    }
    const unsigned char *separator = memchr(data + 7, ' ', (size_t)(tab - data - 7));
    if (!separator) {
        gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git tree object type");
        return CBM_GIT_BLOB_ERROR;
    }
    cbm_git_blob_t blob = {.mode = mode};
    if (!gf_oid(blob.oid, separator + 1, (size_t)(tab - separator - 1),
                facts->identity.oid_hex_length)) {
        gf_error(error, CBM_GIT_FACTS_COMMAND, "Invalid Git blob identity");
        return CBM_GIT_BLOB_ERROR;
    }
    if ((size_t)(separator - (data + 7)) != 4 || memcmp(data + 7, "blob", 4) != 0 ||
        (mode != 0100644 && mode != 0100755)) {
        gf_error(error, CBM_GIT_FACTS_UNSUPPORTED,
                 "Only regular committed source blobs are supported");
        return CBM_GIT_BLOB_ERROR;
    }
    const char *size_args[] = {"cat-file", "-s", blob.oid};
    size_t expected;
    if (!gf_run(facts, size_args, 3, false, &result, error))
        return CBM_GIT_BLOB_ERROR;
    if (!gf_decimal_size(&result.out, &expected)) {
        gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git blob size");
        return CBM_GIT_BLOB_ERROR;
    }
    if (expected > facts->options.stdout_limit ||
        expected > facts->options.total_output_limit - facts->output_bytes) {
        gf_limit(facts, error, "Committed blob exceeds the remaining output budget");
        return CBM_GIT_BLOB_ERROR;
    }
    const char *blob_args[] = {"cat-file", "blob", blob.oid};
    if (!gf_run(facts, blob_args, 3, false, &result, error))
        return CBM_GIT_BLOB_ERROR;
    if (result.out.length != expected) {
        gf_error(error, CBM_GIT_FACTS_COMMAND,
                 "Git blob bytes disagree with the immutable object size");
        return CBM_GIT_BLOB_ERROR;
    }
    if (!gf_topology_guard(facts, error))
        return CBM_GIT_BLOB_ERROR;
    blob.bytes = result.out;
    *out = blob;
    return CBM_GIT_BLOB_FOUND;
}

static bool gf_names_valid(const cbm_git_bytes_t *bytes) {
    size_t at = 0;
    while (at < bytes->length) {
        if (bytes->length - at < 3 || bytes->data[at + 1] != '\0')
            return false;
        unsigned char status = bytes->data[at];
        if (status != 'A' && status != 'D' && status != 'M' && status != 'T')
            return false;
        at += 2;
        const unsigned char *end = memchr(bytes->data + at, '\0', bytes->length - at);
        if (!end ||
            !gf_relative_path((const char *)bytes->data + at, (size_t)(end - (bytes->data + at))))
            return false;
        at = (size_t)(end - bytes->data) + 1;
    }
    return true;
}

bool cbm_git_facts_diff(cbm_git_facts_t *facts, cbm_git_diff_t *out, cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    if (out)
        memset(out, 0, sizeof(*out));
    if (!facts || !out)
        return gf_error(error, CBM_GIT_FACTS_INVALID,
                        "A Git snapshot and diff output are required");
    if (!gf_topology_guard(facts, error))
        return false;
    const char *names[] = {"diff",
                           "--name-status",
                           "-z",
                           "--no-renames",
                           "--no-ext-diff",
                           "--no-textconv",
                           "--no-color",
                           "--no-relative",
                           "--ignore-submodules=none",
                           "--no-exit-code",
                           facts->identity.merge_base,
                           facts->identity.head,
                           "--"};
    gf_result_t names_result, patch_result;
    if (!gf_run(facts, names, (int)(sizeof(names) / sizeof(names[0])), false, &names_result, error))
        return false;
    if (!gf_names_valid(&names_result.out))
        return gf_error(error, CBM_GIT_FACTS_COMMAND,
                        "Malformed or unsupported changed-path metadata");
    const char *args[GF_ARG_CAP];
    int count = 0;
    args[count++] = "diff";
    for (int i = 0; i < facts->diff_flag_count; i++)
        args[count++] = facts->diff_flags[i];
    const char *extra[] = {"--patch",
                           "--no-relative",
                           "--ignore-submodules=none",
                           "--submodule=short",
                           "--no-exit-code",
                           "--no-indent-heuristic",
                           "--output-indicator-new=+",
                           "--output-indicator-old=-",
                           "--output-indicator-context= ",
                           facts->identity.merge_base,
                           facts->identity.head,
                           "--"};
    for (size_t i = 0; i < sizeof(extra) / sizeof(extra[0]); i++)
        args[count++] = extra[i];
    if (!gf_run(facts, args, count, false, &patch_result, error))
        return false;
    if ((names_result.out.length == 0) != (patch_result.out.length == 0))
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Changed-path metadata and patch disagree");
    if (!gf_topology_guard(facts, error))
        return false;
    *out = (cbm_git_diff_t){.patch = patch_result.out, .name_status = names_result.out};
    return true;
}

/* A-to-H names are output data, never blob lookup arguments. Keep this reader
 * separate from gf_names_valid so legacy M-to-H behavior remains unchanged. */
static bool gf_ancestor_names_valid(cbm_git_facts_t *facts, const cbm_git_bytes_t *bytes,
                                    cbm_git_facts_error_t *error) {
    if (bytes->length && !bytes->data)
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Missing ancestor changed-path capture");
    size_t at = 0;
    while (at < bytes->length) {
        if (!gf_gate(facts, error))
            return false;
        if (bytes->length - at < 3 || bytes->data[at + 1] != '\0')
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed ancestor changed-path record");
        unsigned char status = bytes->data[at];
        if (status != 'A' && status != 'M' && status != 'D' && status != 'T')
            return gf_error(error, CBM_GIT_FACTS_COMMAND,
                            "Unsupported ancestor changed-path status");
        at += 2;
        size_t start = at;
        size_t end = at;
        bool terminated = false;
        while (at < bytes->length) {
            if (!gf_gate(facts, error))
                return false;
            size_t chunk = bytes->length - at;
            if (chunk > 65536)
                chunk = 65536;
            const unsigned char *nul = memchr(bytes->data + at, '\0', chunk);
            if (nul) {
                end = at + (size_t)(nul - (bytes->data + at));
                at = end + 1;
                terminated = true;
                break;
            }
            at += chunk;
        }
        if (!terminated)
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Truncated ancestor changed-path record");
        if (!gf_gate(facts, error) ||
            !gf_inventory_path(facts, bytes->data + start, end - start, error))
            return false;
    }
    return gf_gate(facts, error);
}

/* merge-base accepts peeled tags. Check the object itself before ancestry so
 * a full tag OID cannot silently stand in for the admitted artifact commit. */
static bool gf_ancestor_commit_type(cbm_git_facts_t *facts, const char *oid,
                                    cbm_git_facts_error_t *error) {
    const char *args[] = {"cat-file", "-t", oid};
    gf_result_t result;
    if (!gf_run(facts, args, 3, false, &result, error))
        return false;
    const unsigned char *type;
    size_t length;
    /* The longest supported response is "commit\r\n". Reject a large malformed
     * response without an unbounded scan through gf_line's line checks. */
    if (result.out.length > 8 || !gf_line(&result.out, &type, &length))
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed artifact object-type response");
    if (length == 6 && memcmp(type, "commit", 6) == 0)
        return true;
    if ((length == 4 && (memcmp(type, "tree", 4) == 0 || memcmp(type, "blob", 4) == 0)) ||
        (length == 3 && memcmp(type, "tag", 3) == 0))
        return gf_error(error, CBM_GIT_FACTS_INVALID,
                        "Artifact object ID must identify a commit itself");
    return gf_error(error, CBM_GIT_FACTS_COMMAND, "Unknown artifact object-type response");
}

cbm_git_ancestor_changes_status_t cbm_git_facts_ancestor_changes(cbm_git_facts_t *facts,
                                                                 const char *artifact_oid,
                                                                 size_t artifact_oid_length,
                                                                 cbm_git_bytes_t *name_status,
                                                                 cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    if (name_status)
        *name_status = (cbm_git_bytes_t){0};
    char ancestor[65];
    if (!facts || !artifact_oid || !name_status ||
        !gf_oid(ancestor, (const unsigned char *)artifact_oid, artifact_oid_length,
                facts->identity.oid_hex_length)) {
        gf_error(error, CBM_GIT_FACTS_INVALID,
                 "Ancestor changes require a snapshot, full artifact OID and output");
        return CBM_GIT_ANCESTOR_CHANGES_ERROR;
    }
    if (!gf_history_guard(facts, error))
        return CBM_GIT_ANCESTOR_CHANGES_ERROR;
    if (strcmp(ancestor, facts->identity.merge_base) != 0) {
        if (!gf_ancestor_commit_type(facts, ancestor, error))
            return CBM_GIT_ANCESTOR_CHANGES_ERROR;
        const char *args[] = {"merge-base", "--is-ancestor", ancestor, facts->identity.merge_base};
        gf_result_t result;
        if (!gf_run(facts, args, 4, true, &result, error))
            return CBM_GIT_ANCESTOR_CHANGES_ERROR;
        if (result.out.length) {
            gf_error(error, CBM_GIT_FACTS_COMMAND, "Unexpected artifact ancestry stdout");
            return CBM_GIT_ANCESTOR_CHANGES_ERROR;
        }
        if (result.exit_code == 1) {
            if (!gf_history_guard(facts, error) || !gf_gate(facts, error))
                return CBM_GIT_ANCESTOR_CHANGES_ERROR;
            return CBM_GIT_ANCESTOR_CHANGES_NOT_ANCESTOR;
        }
    }
    const char *names[] = {"diff",
                           "--name-status",
                           "-z",
                           "--no-renames",
                           "--no-ext-diff",
                           "--no-textconv",
                           "--no-color",
                           "--no-relative",
                           "--ignore-submodules=none",
                           "--no-exit-code",
                           ancestor,
                           facts->identity.head,
                           "--"};
    gf_result_t result;
    if (!gf_run(facts, names, (int)(sizeof(names) / sizeof(names[0])), false, &result, error) ||
        !gf_ancestor_names_valid(facts, &result.out, error) || !gf_history_guard(facts, error) ||
        !gf_gate(facts, error))
        return CBM_GIT_ANCESTOR_CHANGES_ERROR;
    *name_status = result.out;
    return CBM_GIT_ANCESTOR_CHANGES_OK;
}

/* Pinned blob batches retain inventory identity separately from payload bytes.
 * Per-call objects are sorted by OID; result items retain caller ordering. */
typedef struct {
    const char *oid;
    size_t size, frame_length;
    cbm_git_bytes_t bytes;
} gf_batch_object_t;

typedef struct {
    const cbm_git_tree_entry_t *entry;
    size_t inventory_index, position, object_index;
} gf_batch_pick_t;

typedef struct {
    size_t width, count;
    const char *const *oids; /* test wrapper view, never production ownership */
    const size_t *sizes;
    gf_batch_object_t *objects; /* optional production output and expected identity */
    bool has_payload;
} gf_batch_frames_t;

typedef struct {
    cbm_git_facts_t *facts;
    const cbm_git_blob_batch_request_t *request;
    cbm_git_tree_inventory_t inventory;
    gf_batch_pick_t *picks;
    gf_batch_object_t *objects;
    size_t unique_count, line_length;
    unsigned char *lines;
} gf_batch_work_t;

/* NULL facts is used only by the pure parser wrappers. Both paths share every
 * grammar/overflow/frame check; the wrapper has no transport or owner state. */
static bool gf_batch_gate(cbm_git_facts_t *facts, cbm_git_facts_error_t *error) {
    return !facts || gf_gate(facts, error);
}

static bool gf_batch_limit(cbm_git_facts_t *facts, cbm_git_facts_error_t *error,
                           const char *message) {
    return facts ? gf_limit(facts, error, message) : gf_error(error, CBM_GIT_FACTS_LIMIT, message);
}

static bool gf_batch_canonical_oid(const char *oid, size_t width) {
    if (!oid || (width != 40 && width != 64))
        return false;
    for (size_t i = 0; i < width; i++) {
        unsigned char c = (unsigned char)oid[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return false;
    }
    return oid[width] == '\0';
}

typedef struct {
    size_t size, length;
} gf_batch_header_t;

/* Continue validating digits after overflow so a malformed suffix is never
 * mistaken for a valid huge size. Header length includes its terminal LF. */
static bool gf_batch_header(cbm_git_facts_t *facts, cbm_git_bytes_t capture, const char *oid,
                            size_t width, gf_batch_header_t *header, cbm_git_facts_error_t *error) {
    *header = (gf_batch_header_t){0};
    if (!gf_batch_gate(facts, error))
        return false;
    size_t begin = width + 6;
    if (capture.length < begin + 2 || memcmp(capture.data, oid, width) != 0 ||
        memcmp(capture.data + width, " blob ", 6) != 0)
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git batch identity/type");
    size_t value = 0, at = begin;
    bool overflow = false;
    for (; at < capture.length && capture.data[at] != '\n'; at++) {
        if (((at - begin) & 65535u) == 0 && !gf_batch_gate(facts, error))
            return false;
        unsigned char c = capture.data[at];
        if (c < '0' || c > '9' || (at > begin && capture.data[begin] == '0'))
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git batch object size");
        if (value > (SIZE_MAX - (size_t)(c - '0')) / 10)
            overflow = true;
        if (!overflow)
            value = value * 10 + (size_t)(c - '0');
    }
    if (at == begin || at == capture.length)
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Truncated Git batch header");
    if (overflow || at == SIZE_MAX - 1 || value > SIZE_MAX - (at + 1) - 1)
        return gf_batch_limit(facts, error, "Git batch object frame size overflow");
    if (!gf_batch_gate(facts, error))
        return false;
    *header = (gf_batch_header_t){.size = value, .length = at + 1};
    return true;
}

static bool gf_batch_payload(cbm_git_facts_t *facts, cbm_git_bytes_t capture, size_t *offset,
                             size_t size, cbm_git_bytes_t *bytes, cbm_git_facts_error_t *error) {
    if (size >= capture.length - *offset)
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Truncated Git batch payload");
    size_t start = *offset;
    for (size_t left = size; left;) {
        if (!gf_batch_gate(facts, error))
            return false;
        size_t chunk = left > 65536 ? 65536 : left;
        *offset += chunk;
        left -= chunk;
    }
    if (!gf_batch_gate(facts, error))
        return false;
    if (capture.data[*offset] != '\n')
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Malformed Git batch payload separator");
    (*offset)++;
    *bytes = (cbm_git_bytes_t){capture.data + start, size};
    return true;
}

/* One exact capture driver for production and deterministic parser tests. */
static bool gf_batch_capture(cbm_git_facts_t *facts, cbm_git_bytes_t capture,
                             const gf_batch_frames_t *frames, cbm_git_facts_error_t *error) {
    size_t at = 0;
    for (size_t i = 0; i < frames->count; i++) {
        if (!gf_batch_gate(facts, error))
            return false;
        const char *oid = frames->objects ? frames->objects[i].oid : frames->oids[i];
        cbm_git_bytes_t remaining = {capture.data ? capture.data + at : NULL, capture.length - at};
        gf_batch_header_t header;
        if (!gf_batch_header(facts, remaining, oid, frames->width, &header, error))
            return false;
        size_t size = header.size;
        bool check_size = frames->sizes || (frames->objects && frames->has_payload);
        size_t expected = frames->sizes     ? frames->sizes[i]
                          : frames->objects ? frames->objects[i].size
                                            : 0;
        if (check_size && size != expected)
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Git batch size changed after preflight");
        at += header.length;
        cbm_git_bytes_t bytes = {0};
        if (frames->has_payload && !gf_batch_payload(facts, capture, &at, size, &bytes, error))
            return false;
        if (frames->objects) {
            frames->objects[i].size = size;
            frames->objects[i].frame_length = header.length + size + 1;
            if (frames->has_payload)
                frames->objects[i].bytes = bytes;
        }
    }
    if (at != capture.length)
        return gf_error(error, CBM_GIT_FACTS_COMMAND, "Unexpected bytes after Git batch frames");
    return gf_batch_gate(facts, error);
}

#ifdef CBM_ENABLE_TEST_SEAMS
bool cbm_git_facts_test_batch_header(cbm_git_bytes_t capture, const char *expected_oid,
                                     size_t oid_hex_length, size_t *object_size,
                                     size_t *header_length, cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    if (object_size)
        *object_size = 0;
    if (header_length)
        *header_length = 0;
    if (!object_size || !header_length || (capture.length && !capture.data) ||
        !gf_batch_canonical_oid(expected_oid, oid_hex_length))
        return gf_error(error, CBM_GIT_FACTS_INVALID, "Invalid Git batch header test input");
    gf_batch_header_t header;
    if (!gf_batch_header(NULL, capture, expected_oid, oid_hex_length, &header, error))
        return false;
    *object_size = header.size;
    *header_length = header.length;
    return true;
}

bool cbm_git_facts_test_batch_capture(cbm_git_bytes_t capture, size_t oid_hex_length,
                                      const char *const *expected_oids,
                                      const size_t *expected_sizes, size_t count, bool has_payload,
                                      cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    if ((capture.length && !capture.data) || (oid_hex_length != 40 && oid_hex_length != 64) ||
        (count && (!expected_oids || (has_payload && !expected_sizes))) ||
        count > SIZE_MAX / sizeof(*expected_oids) || count > SIZE_MAX / sizeof(*expected_sizes))
        return gf_error(error, CBM_GIT_FACTS_INVALID, "Invalid Git batch capture test input");
    for (size_t i = 0; i < count; i++)
        if (!gf_batch_canonical_oid(expected_oids[i], oid_hex_length))
            return gf_error(error, CBM_GIT_FACTS_INVALID, "Invalid expected Git batch object ID");
    gf_batch_frames_t frames = {.width = oid_hex_length,
                                .count = count,
                                .oids = expected_oids,
                                .sizes = expected_sizes,
                                .has_payload = has_payload};
    return gf_batch_capture(NULL, capture, &frames, error);
}
#endif

static void *gf_batch_array(cbm_git_facts_t *facts, size_t count, size_t size,
                            cbm_git_facts_error_t *error) {
    if (count > SIZE_MAX / size) {
        gf_limit(facts, error, "Git batch array size overflow");
        return NULL;
    }
    return gf_allocate(facts, &facts->arena, count * size, error);
}

static bool gf_batch_inventory(gf_batch_work_t *work, cbm_git_facts_error_t *error) {
    cbm_git_facts_t *facts = work->facts;
    const char *commit = gf_revision(facts, work->request->revision);
    for (size_t i = 0; i < 2; i++) {
        if (facts->inventory_cache[i].commit &&
            strcmp(facts->inventory_cache[i].commit, commit) == 0) {
            work->inventory = facts->inventory_cache[i].inventory;
            return gf_gate(facts, error);
        }
    }
    if (!cbm_git_facts_inventory(facts, work->request->revision, &work->inventory, error))
        return false;
    for (size_t i = 0; i < 2; i++) {
        if (!facts->inventory_cache[i].commit) {
            facts->inventory_cache[i] = (gf_inventory_cache_t){commit, work->inventory};
            return true;
        }
    }
    return gf_error(error, CBM_GIT_FACTS_INVALID, "Invalid internal pinned inventory cache");
}

static int gf_batch_pick_compare(const gf_batch_pick_t *a, const gf_batch_pick_t *b, size_t width) {
    int cmp = memcmp(a->entry->oid, b->entry->oid, width);
    if (cmp)
        return cmp;
    return a->position < b->position ? -1 : a->position > b->position ? 1 : 0;
}

static bool gf_batch_sift(gf_batch_work_t *work, size_t root, size_t count,
                          cbm_git_facts_error_t *error) {
    gf_batch_pick_t *picks = work->picks;
    size_t width = work->facts->identity.oid_hex_length;
    while (root < count / 2) {
        if (!gf_gate(work->facts, error))
            return false;
        size_t child = root * 2 + 1;
        if (child + 1 < count && gf_batch_pick_compare(&picks[child], &picks[child + 1], width) < 0)
            child++;
        if (gf_batch_pick_compare(&picks[root], &picks[child], width) >= 0)
            return true;
        gf_batch_pick_t swap = picks[root];
        picks[root] = picks[child];
        picks[child] = swap;
        root = child;
    }
    return true;
}

static bool gf_batch_sort(gf_batch_work_t *work, cbm_git_facts_error_t *error) {
    size_t count = work->request->count;
    for (size_t i = count / 2; i > 0; i--)
        if (!gf_batch_sift(work, i - 1, count, error))
            return false;
    for (size_t end = count; end > 1; end--) {
        if (!gf_gate(work->facts, error))
            return false;
        gf_batch_pick_t swap = work->picks[0];
        work->picks[0] = work->picks[end - 1];
        work->picks[end - 1] = swap;
        if (!gf_batch_sift(work, 0, end - 1, error))
            return false;
    }
    return gf_gate(work->facts, error);
}

/* Validate the entire selection before allocating its maps or querying any
 * object. Index-vector multiplication was checked at the public boundary. */
static bool gf_batch_selection(gf_batch_work_t *work, cbm_git_facts_error_t *error) {
    size_t count = work->request->count;
    for (size_t i = 0; i < count; i++) {
        if (!gf_gate(work->facts, error))
            return false;
        size_t index = work->request->indices[i];
        if (index >= work->inventory.count)
            return gf_error(error, CBM_GIT_FACTS_INVALID,
                            "Git batch inventory index is out of range");
        const cbm_git_tree_entry_t *entry = &work->inventory.entries[index];
        if (entry->object_type != CBM_GIT_TREE_BLOB ||
            (entry->mode != 0100644 && entry->mode != 0100755))
            return gf_error(error, CBM_GIT_FACTS_UNSUPPORTED, "Git batch requires regular blobs");
    }
    work->picks = gf_batch_array(work->facts, count, sizeof(*work->picks), error);
    if (!work->picks)
        return false;
    for (size_t i = 0; i < count; i++) {
        if (!gf_gate(work->facts, error))
            return false;
        size_t index = work->request->indices[i];
        work->picks[i] = (gf_batch_pick_t){
            .entry = &work->inventory.entries[index], .inventory_index = index, .position = i};
    }
    return gf_batch_sort(work, error);
}

static bool gf_batch_unique(gf_batch_work_t *work, cbm_git_facts_error_t *error) {
    size_t count = work->request->count, width = work->facts->identity.oid_hex_length;
    for (size_t i = 0; i < count; i++) {
        if (!gf_gate(work->facts, error))
            return false;
        if (!i || memcmp(work->picks[i - 1].entry->oid, work->picks[i].entry->oid, width) != 0)
            work->unique_count++;
        work->picks[i].object_index = work->unique_count - 1;
    }
    work->objects = gf_batch_array(work->facts, work->unique_count, sizeof(*work->objects), error);
    if (!work->objects)
        return false;
    work->line_length = width + 1;
    if (work->unique_count > SIZE_MAX / work->line_length / 2 ||
        work->unique_count * work->line_length * 2 > work->facts->batch_budget->input_limit)
        return gf_limit(work->facts, error, "Git batch cumulative stdin limit exceeded");
    work->lines = gf_batch_array(work->facts, work->unique_count, work->line_length, error);
    if (!work->lines)
        return false;
    for (size_t i = 0; i < count; i++) {
        if (!gf_gate(work->facts, error))
            return false;
        size_t object = work->picks[i].object_index;
        if (i && object == work->picks[i - 1].object_index)
            continue;
        const char *oid = work->picks[i].entry->oid;
        if (!gf_batch_canonical_oid(oid, width))
            return gf_error(error, CBM_GIT_FACTS_COMMAND, "Invalid owned Git batch object ID");
        work->objects[object] = (gf_batch_object_t){.oid = oid};
        unsigned char *line = work->lines + object * work->line_length;
        memcpy(line, oid, width);
        line[width] = '\n';
    }
    return gf_gate(work->facts, error);
}

static size_t gf_batch_digits(size_t value) {
    size_t digits = 1;
    while (value >= 10) {
        value /= 10;
        digits++;
    }
    return digits;
}

static bool gf_batch_command(gf_batch_work_t *work, size_t first, size_t count, bool payload,
                             cbm_git_facts_error_t *error) {
    const char *args[] = {"cat-file",
                          payload ? "--batch=%(objectname) %(objecttype) %(objectsize)"
                                  : "--batch-check=%(objectname) %(objecttype) %(objectsize)"};
    cbm_git_bytes_t input = {work->lines + first * work->line_length, count * work->line_length};
    gf_result_t result;
    if (!gf_run_input(work->facts, args, 2, false, &input, &result, error))
        return false;
    gf_batch_frames_t frames = {.width = work->facts->identity.oid_hex_length,
                                .count = count,
                                .objects = work->objects + first,
                                .has_payload = payload};
    return gf_batch_capture(work->facts, result.out, &frames, error);
}

static bool gf_batch_preflight(gf_batch_work_t *work, cbm_git_facts_error_t *error) {
    size_t worst_header = work->facts->identity.oid_hex_length + 7 + gf_batch_digits(SIZE_MAX);
    size_t batch = work->facts->options.stdout_limit / worst_header;
    if (!batch)
        batch = 1;
    for (size_t first = 0; first < work->unique_count;) {
        if (!gf_gate(work->facts, error))
            return false;
        size_t count = work->unique_count - first;
        if (count > batch)
            count = batch;
        if (!gf_batch_command(work, first, count, false, error))
            return false;
        first += count;
    }
    size_t total = 0;
    for (size_t i = 0; i < work->unique_count; i++) {
        if (!gf_gate(work->facts, error))
            return false;
        size_t frame = work->objects[i].frame_length;
        if (frame > work->facts->options.stdout_limit || frame > SIZE_MAX - total)
            return gf_limit(work->facts, error, "Git batch payload frame exceeds output limit");
        total += frame;
    }
    if (total > work->facts->options.total_output_limit - work->facts->output_bytes)
        return gf_limit(work->facts, error, "Git batch payloads exceed remaining output limit");
    return gf_gate(work->facts, error);
}

static bool gf_batch_read_payloads(gf_batch_work_t *work, cbm_git_facts_error_t *error) {
    for (size_t first = 0; first < work->unique_count;) {
        size_t count = 0, total = 0;
        while (first + count < work->unique_count) {
            if (!gf_gate(work->facts, error))
                return false;
            size_t frame = work->objects[first + count].frame_length;
            if (frame > work->facts->options.stdout_limit - total)
                break;
            total += frame;
            count++;
        }
        if (!count || total > work->facts->options.total_output_limit - work->facts->output_bytes)
            return gf_limit(work->facts, error, "Git batch payloads exceed remaining output limit");
        if (!gf_batch_command(work, first, count, true, error))
            return false;
        first += count;
    }
    return gf_gate(work->facts, error);
}

static bool gf_batch_publish(gf_batch_work_t *work, cbm_git_blob_batch_t *out,
                             cbm_git_facts_error_t *error) {
    size_t count = work->request->count;
    cbm_git_blob_batch_item_t *items = NULL;
    if (count) {
        items = gf_batch_array(work->facts, count, sizeof(*items), error);
        if (!items)
            return false;
        for (size_t i = 0; i < count; i++) {
            if (!gf_gate(work->facts, error))
                return false;
            const gf_batch_pick_t *pick = &work->picks[i];
            items[pick->position] =
                (cbm_git_blob_batch_item_t){.inventory_index = pick->inventory_index,
                                            .entry = pick->entry,
                                            .bytes = work->objects[pick->object_index].bytes};
        }
    }
    if (!gf_history_guard(work->facts, error) || !gf_gate(work->facts, error))
        return false;
    *out = (cbm_git_blob_batch_t){.revision = work->request->revision,
                                  .commit = gf_revision(work->facts, work->request->revision),
                                  .items = items,
                                  .count = count};
    return true;
}

static bool gf_batch_execute(cbm_git_facts_t *facts, const cbm_git_blob_batch_request_t *request,
                             const cbm_git_blob_batch_limits_t *limits, cbm_git_blob_batch_t *out,
                             cbm_git_facts_error_t *error) {
    if (!gf_history_guard(facts, error))
        return false;
    if (request->count > limits->max_entries || request->count > SIZE_MAX / sizeof(size_t) ||
        request->count > SIZE_MAX / sizeof(gf_batch_pick_t) ||
        request->count > SIZE_MAX / sizeof(cbm_git_blob_batch_item_t))
        return gf_limit(facts, error, "Git batch entry limit exceeded");
    gf_batch_work_t work = {.facts = facts, .request = request};
    if (!gf_batch_inventory(&work, error))
        return false;
    if (request->count &&
        (!gf_batch_selection(&work, error) || !gf_batch_unique(&work, error) ||
         !gf_batch_preflight(&work, error) || !gf_batch_read_payloads(&work, error)))
        return false;
    return gf_batch_publish(&work, out, error);
}

bool cbm_git_facts_read_blob_batch(cbm_git_facts_t *facts,
                                   const cbm_git_blob_batch_request_t *request,
                                   const cbm_git_blob_batch_limits_t *limits,
                                   cbm_git_blob_batch_t *out, cbm_git_facts_error_t *error) {
    cbm_git_facts_error_t local;
    if (!error)
        error = &local;
    gf_error_clear(error);
    if (out)
        memset(out, 0, sizeof(*out));
    if (!facts || !request || !limits || !out || !limits->max_entries || !limits->max_input_bytes ||
        !limits->max_arena_bytes ||
        (request->revision != CBM_GIT_REV_HEAD && request->revision != CBM_GIT_REV_MERGE_BASE) ||
        (request->count && !request->indices) || facts->batch_budget)
        return gf_error(error, CBM_GIT_FACTS_INVALID, "Invalid pinned Git blob batch request");
    gf_batch_budget_t budget = {.arena_limit = limits->max_arena_bytes,
                                .input_limit = limits->max_input_bytes};
    facts->batch_budget = &budget;
    bool ok = gf_batch_execute(facts, request, limits, out, error);
    facts->batch_budget = NULL;
    if (!ok && error->status == CBM_GIT_FACTS_CANCELLED)
        facts->stopped = *error;
    return ok;
}
