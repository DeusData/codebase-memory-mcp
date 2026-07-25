#include "cli/windows_launcher_state.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const uint8_t current_v1_magic[8] = {
    'C', 'B', 'M', 'C', 'U', 'R', '1', '\0',
};

static const uint8_t release_descriptor_v1_magic[8] = {
    'C', 'B', 'M', 'W', 'R', 'D', '1', '\0',
};

static uint32_t read_u32_le(const uint8_t *input) {
    return (uint32_t)input[0] | ((uint32_t)input[1] << 8U) | ((uint32_t)input[2] << 16U) |
           ((uint32_t)input[3] << 24U);
}

static uint64_t read_u64_le(const uint8_t *input) {
    uint64_t value = 0;
    for (unsigned int index = 0; index < 8U; index++) {
        value |= (uint64_t)input[index] << (index * 8U);
    }
    return value;
}

static void write_u32_le(uint8_t *output, uint32_t value) {
    for (unsigned int index = 0; index < 4U; index++) {
        output[index] = (uint8_t)(value >> (index * 8U));
    }
}

static void write_u64_le(uint8_t *output, uint64_t value) {
    for (unsigned int index = 0; index < 8U; index++) {
        output[index] = (uint8_t)(value >> (index * 8U));
    }
}

static bool lowercase_sha256_valid(const char *digest) {
    if (!digest) {
        return false;
    }
    for (size_t index = 0; index < 64U; index++) {
        char value = digest[index];
        if (!((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f'))) {
            return false;
        }
    }
    return digest[64] == '\0';
}

static bool current_v1_state_valid(const cbm_windows_current_v1_t *state) {
    return state && state->launcher_abi_min > 0U &&
           state->launcher_abi_min <= state->launcher_abi_max && state->payload_size > 0U &&
           lowercase_sha256_valid(state->payload_sha256);
}

static bool release_descriptor_v1_valid(const cbm_windows_release_descriptor_v1_t *descriptor) {
    return descriptor && descriptor->launcher_abi > 0U &&
           descriptor->payload_launcher_abi_min > 0U &&
           descriptor->payload_launcher_abi_min <= descriptor->payload_launcher_abi_max &&
           descriptor->launcher_abi >= descriptor->payload_launcher_abi_min &&
           descriptor->launcher_abi <= descriptor->payload_launcher_abi_max &&
           descriptor->payload_size > 0U && lowercase_sha256_valid(descriptor->payload_sha256);
}

bool cbm_windows_current_v1_encode(const cbm_windows_current_v1_t *state,
                                   uint8_t out[CBM_WINDOWS_CURRENT_V1_SIZE]) {
    if (!current_v1_state_valid(state) || !out) {
        return false;
    }
    memset(out, 0, CBM_WINDOWS_CURRENT_V1_SIZE);
    memcpy(out, current_v1_magic, sizeof(current_v1_magic));
    write_u32_le(out + 8U, 1U);
    write_u32_le(out + 12U, CBM_WINDOWS_CURRENT_V1_SIZE);
    write_u32_le(out + 16U, state->launcher_abi_min);
    write_u32_le(out + 20U, state->launcher_abi_max);
    write_u64_le(out + 24U, state->payload_size);
    memcpy(out + 32U, state->payload_sha256, 64U);
    return true;
}

bool cbm_windows_current_v1_decode(const uint8_t *record, size_t record_size,
                                   cbm_windows_current_v1_t *state_out) {
    if (state_out) {
        memset(state_out, 0, sizeof(*state_out));
    }
    if (!record || !state_out || record_size != CBM_WINDOWS_CURRENT_V1_SIZE ||
        memcmp(record, current_v1_magic, sizeof(current_v1_magic)) != 0 ||
        read_u32_le(record + 8U) != 1U ||
        read_u32_le(record + 12U) != CBM_WINDOWS_CURRENT_V1_SIZE) {
        return false;
    }
    for (size_t index = 96U; index < CBM_WINDOWS_CURRENT_V1_SIZE; index++) {
        if (record[index] != 0U) {
            return false;
        }
    }
    cbm_windows_current_v1_t decoded;
    memset(&decoded, 0, sizeof(decoded));
    decoded.launcher_abi_min = read_u32_le(record + 16U);
    decoded.launcher_abi_max = read_u32_le(record + 20U);
    decoded.payload_size = read_u64_le(record + 24U);
    memcpy(decoded.payload_sha256, record + 32U, 64U);
    decoded.payload_sha256[64] = '\0';
    if (!current_v1_state_valid(&decoded)) {
        return false;
    }
    *state_out = decoded;
    return true;
}

bool cbm_windows_current_v1_supports_launcher_abi(const cbm_windows_current_v1_t *state,
                                                  uint32_t launcher_abi) {
    return current_v1_state_valid(state) && launcher_abi > 0U &&
           launcher_abi >= state->launcher_abi_min && launcher_abi <= state->launcher_abi_max;
}

bool cbm_windows_release_descriptor_v1_encode(const cbm_windows_release_descriptor_v1_t *descriptor,
                                              uint8_t out[CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE]) {
    if (!release_descriptor_v1_valid(descriptor) || !out) {
        return false;
    }
    memset(out, 0, CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE);
    memcpy(out, release_descriptor_v1_magic, sizeof(release_descriptor_v1_magic));
    write_u32_le(out + 8U, 1U);
    write_u32_le(out + 12U, CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE);
    write_u32_le(out + 16U, descriptor->launcher_abi);
    write_u32_le(out + 20U, descriptor->payload_launcher_abi_min);
    write_u32_le(out + 24U, descriptor->payload_launcher_abi_max);
    write_u32_le(out + 28U, 0U);
    write_u64_le(out + 32U, descriptor->payload_size);
    memcpy(out + 40U, descriptor->payload_sha256, 64U);
    return true;
}

bool cbm_windows_release_descriptor_v1_decode(const uint8_t *record, size_t record_size,
                                              cbm_windows_release_descriptor_v1_t *descriptor_out) {
    if (descriptor_out) {
        memset(descriptor_out, 0, sizeof(*descriptor_out));
    }
    if (!record || !descriptor_out || record_size != CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE ||
        memcmp(record, release_descriptor_v1_magic, sizeof(release_descriptor_v1_magic)) != 0 ||
        read_u32_le(record + 8U) != 1U ||
        read_u32_le(record + 12U) != CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE ||
        read_u32_le(record + 28U) != 0U) {
        return false;
    }
    for (size_t index = 104U; index < CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE; index++) {
        if (record[index] != 0U) {
            return false;
        }
    }
    cbm_windows_release_descriptor_v1_t decoded;
    memset(&decoded, 0, sizeof(decoded));
    decoded.launcher_abi = read_u32_le(record + 16U);
    decoded.payload_launcher_abi_min = read_u32_le(record + 20U);
    decoded.payload_launcher_abi_max = read_u32_le(record + 24U);
    decoded.payload_size = read_u64_le(record + 32U);
    memcpy(decoded.payload_sha256, record + 40U, 64U);
    decoded.payload_sha256[64] = '\0';
    if (!release_descriptor_v1_valid(&decoded)) {
        return false;
    }
    *descriptor_out = decoded;
    return true;
}

cbm_windows_transition_plan_t cbm_windows_transition_plan(
    const cbm_windows_current_v1_t *current, const cbm_windows_release_descriptor_v1_t *candidate) {
    if (!current_v1_state_valid(current) || !release_descriptor_v1_valid(candidate)) {
        return CBM_WINDOWS_TRANSITION_INCOMPATIBLE;
    }
    if (cbm_windows_current_v1_supports_launcher_abi(current, candidate->launcher_abi)) {
        return CBM_WINDOWS_TRANSITION_LAUNCHER_FIRST;
    }
    if (candidate->payload_launcher_abi_min <= current->launcher_abi_min &&
        candidate->payload_launcher_abi_max >= current->launcher_abi_max) {
        return CBM_WINDOWS_TRANSITION_CURRENT_FIRST;
    }
    return CBM_WINDOWS_TRANSITION_INCOMPATIBLE;
}

static bool windows_generation_file_path(const wchar_t *canonical_launcher_path,
                                         const char payload_sha256[65], const wchar_t *leaf,
                                         wchar_t *path_out, size_t path_capacity) {
    if (path_out && path_capacity > 0U) {
        path_out[0] = L'\0';
    }
    if (!canonical_launcher_path || !lowercase_sha256_valid(payload_sha256) || !leaf || !leaf[0] ||
        wcschr(leaf, L'\\') || wcschr(leaf, L'/') || !path_out || path_capacity == 0U) {
        return false;
    }
    const wchar_t *last_backslash = wcsrchr(canonical_launcher_path, L'\\');
    const wchar_t *last_slash = wcsrchr(canonical_launcher_path, L'/');
    const wchar_t *separator = last_backslash;
    if (!separator || (last_slash && last_slash > separator)) {
        separator = last_slash;
    }
    if (!separator || separator == canonical_launcher_path ||
        (separator == canonical_launcher_path + 2 && canonical_launcher_path[1] != L':')) {
        return false;
    }
    size_t directory_length = (size_t)(separator - canonical_launcher_path);
    if (directory_length == 2U && canonical_launcher_path[1] == L':') {
        directory_length = 3U; /* Preserve C:\ rather than producing C:.cbm. */
    }
    static const wchar_t middle[] = L".cbm\\generations\\";
    size_t middle_length = sizeof(middle) / sizeof(middle[0]) - 1U;
    size_t leaf_length = wcslen(leaf);
    bool root = directory_length == 3U && canonical_launcher_path[1] == L':' &&
                (canonical_launcher_path[2] == L'\\' || canonical_launcher_path[2] == L'/');
    size_t separator_count = root ? 0U : 1U;
    size_t needed =
        directory_length + separator_count + middle_length + 64U + 1U + leaf_length + 1U;
    if (needed > path_capacity) {
        return false;
    }
    size_t offset = 0U;
    memcpy(path_out + offset, canonical_launcher_path, directory_length * sizeof(*path_out));
    offset += directory_length;
    if (!root) {
        path_out[offset++] = L'\\';
    } else {
        path_out[2] = L'\\';
    }
    memcpy(path_out + offset, middle, middle_length * sizeof(*path_out));
    offset += middle_length;
    for (size_t index = 0; index < 64U; index++) {
        path_out[offset++] = (wchar_t)(unsigned char)payload_sha256[index];
    }
    path_out[offset++] = L'\\';
    memcpy(path_out + offset, leaf, (leaf_length + 1U) * sizeof(*path_out));
    return true;
}

bool cbm_windows_generation_payload_path(const wchar_t *canonical_launcher_path,
                                         const char payload_sha256[65], wchar_t *path_out,
                                         size_t path_capacity) {
    return windows_generation_file_path(canonical_launcher_path, payload_sha256,
                                        L"codebase-memory-mcp.payload.exe", path_out,
                                        path_capacity);
}

bool cbm_windows_generation_launcher_path(const wchar_t *canonical_launcher_path,
                                          const char payload_sha256[65], wchar_t *path_out,
                                          size_t path_capacity) {
    return windows_generation_file_path(canonical_launcher_path, payload_sha256,
                                        L"codebase-memory-mcp.exe", path_out, path_capacity);
}

bool cbm_windows_retired_state_path(const wchar_t *canonical_launcher_path,
                                    const char payload_sha256[65], uint32_t payload_pid,
                                    wchar_t *path_out, size_t path_capacity) {
    if (path_out && path_capacity > 0U) {
        path_out[0] = L'\0';
    }
    if (!canonical_launcher_path || !lowercase_sha256_valid(payload_sha256) || payload_pid == 0U ||
        !path_out || path_capacity == 0U) {
        return false;
    }
    const wchar_t *last_backslash = wcsrchr(canonical_launcher_path, L'\\');
    const wchar_t *last_slash = wcsrchr(canonical_launcher_path, L'/');
    const wchar_t *separator = last_backslash;
    if (!separator || (last_slash && last_slash > separator)) {
        separator = last_slash;
    }
    if (!separator || separator == canonical_launcher_path ||
        (separator == canonical_launcher_path + 2 && canonical_launcher_path[1] != L':')) {
        return false;
    }
    size_t directory_length = (size_t)(separator - canonical_launcher_path);
    if (directory_length == 2U && canonical_launcher_path[1] == L':') {
        directory_length = 3U;
    }
    bool root = directory_length == 3U && canonical_launcher_path[1] == L':' &&
                (canonical_launcher_path[2] == L'\\' || canonical_launcher_path[2] == L'/');
    static const wchar_t retired_prefix[] = L".cbm-retired-v1-";
    wchar_t pid_text[16];
    int pid_written = swprintf(pid_text, sizeof(pid_text) / sizeof(pid_text[0]), L"%u",
                               (unsigned int)payload_pid);
    size_t prefix_length = sizeof(retired_prefix) / sizeof(retired_prefix[0]) - 1U;
    if (pid_written <= 0 || (size_t)pid_written >= sizeof(pid_text) / sizeof(pid_text[0])) {
        return false;
    }
    size_t leaf_length = prefix_length + CBM_WINDOWS_RETIRED_TAG_HEX + 1U + (size_t)pid_written;
    wchar_t leaf[128];
    if (leaf_length + 1U > sizeof(leaf) / sizeof(leaf[0])) {
        return false;
    }
    memcpy(leaf, retired_prefix, prefix_length * sizeof(*leaf));
    size_t leaf_offset = prefix_length;
    /* A 16-hex-char (64-bit) prefix of the payload digest keeps the retired
     * directory uniquely identifiable while staying inside the directory-
     * rename length limit for the deepest managed-install ancestries. The
     * managed uninstall renames the state tree with FileRenameInfoEx on an
     * already-DELETE-opened handle (MoveFileExW cannot: it needs
     * FILE_DELETE_CHILD on the parent, which the add-only managed ancestor
     * withholds), and that call caps the absolute target near MAX_PATH. */
    for (size_t index = 0U; index < CBM_WINDOWS_RETIRED_TAG_HEX; index++) {
        leaf[leaf_offset++] = (wchar_t)(unsigned char)payload_sha256[index];
    }
    leaf[leaf_offset++] = L'-';
    memcpy(leaf + leaf_offset, pid_text, ((size_t)pid_written + 1U) * sizeof(*leaf));
    size_t separator_count = root ? 0U : 1U;
    size_t needed = directory_length + separator_count + leaf_length + 1U;
    if (needed > path_capacity) {
        return false;
    }
    memcpy(path_out, canonical_launcher_path, directory_length * sizeof(*path_out));
    size_t offset = directory_length;
    if (!root) {
        path_out[offset++] = L'\\';
    } else {
        path_out[2] = L'\\';
    }
    memcpy(path_out + offset, leaf, (leaf_length + 1U) * sizeof(*path_out));
    return true;
}

static bool argument_is(const char *argument, const char *expected) {
    return argument && expected && strcmp(argument, expected) == 0;
}

cbm_windows_launcher_action_t cbm_windows_launcher_classify_action(int argc,
                                                                   const char *const argv[]) {
    if (argc <= 1 || !argv) {
        return CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY;
    }
    for (int index = 1; index < argc; index++) {
        const char *argument = argv[index];
        if (argument_is(argument, "cli") || argument_is(argument, "hook-augment") ||
            argument_is(argument, "config") || argument_is(argument, "install") ||
            argument_is(argument, "--help") || argument_is(argument, "-h") ||
            argument_is(argument, "--version")) {
            return CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY;
        }
        if (argument_is(argument, "update")) {
            return CBM_WINDOWS_LAUNCHER_ACTION_UPDATE;
        }
        if (argument_is(argument, "uninstall")) {
            return CBM_WINDOWS_LAUNCHER_ACTION_UNINSTALL;
        }
    }
    return CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY;
}

bool cbm_windows_launcher_action_allowed(cbm_windows_launcher_action_t action, bool managed) {
    switch (action) {
    case CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY:
        return true;
    case CBM_WINDOWS_LAUNCHER_ACTION_UPDATE:
    case CBM_WINDOWS_LAUNCHER_ACTION_UNINSTALL:
        return managed;
    default:
        return false;
    }
}

static void launcher_error(char *error, size_t error_size, const char *message) {
    if (error && error_size > 0U) {
        (void)snprintf(error, error_size, "%s", message ? message : "error");
    }
}

#ifndef _WIN32

bool cbm_windows_launcher_context_consume(cbm_windows_launcher_context_t *context_out, char *error,
                                          size_t error_size) {
    if (context_out) {
        memset(context_out, 0, sizeof(*context_out));
    }
    if (!context_out) {
        launcher_error(error, error_size, "invalid launcher context output");
        return false;
    }
    if (error && error_size > 0U) {
        error[0] = '\0';
    }
    return true;
}

bool cbm_windows_launcher_context_complete(cbm_windows_launcher_context_t *context, bool accepted,
                                           char *error, size_t error_size) {
    (void)accepted;
    if (error && error_size > 0U)
        error[0] = '\0';
    if (!context || context->_authority_handle != 0U) {
        launcher_error(error, error_size, "invalid launcher context completion");
        return false;
    }
    return true;
}

bool cbm_windows_launcher_capability_probe(const wchar_t *target_directory,
                                           const wchar_t *launcher_candidate, char *error,
                                           size_t error_size) {
    (void)target_directory;
    (void)launcher_candidate;
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_launcher_file_secure(const wchar_t *launcher_path, char *error,
                                      size_t error_size) {
    (void)launcher_path;
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_managed_launcher_backing(const wchar_t *canonical_launcher_path,
                                          wchar_t *backing_path_out, size_t backing_path_capacity,
                                          char *error, size_t error_size) {
    (void)canonical_launcher_path;
    if (backing_path_out && backing_path_capacity > 0U) {
        backing_path_out[0] = L'\0';
    }
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_release_descriptor_probe(const wchar_t *launcher_candidate,
                                          cbm_windows_release_descriptor_v1_t *descriptor_out,
                                          char *error, size_t error_size) {
    (void)launcher_candidate;
    if (descriptor_out) {
        memset(descriptor_out, 0, sizeof(*descriptor_out));
    }
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_current_v1_write_atomic(const wchar_t *canonical_launcher_path,
                                         const cbm_windows_current_v1_t *state, char *error,
                                         size_t error_size) {
    (void)canonical_launcher_path;
    (void)state;
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_launcher_replace_atomic(const wchar_t *target_path, const wchar_t *backing_path,
                                         char *error, size_t error_size) {
    (void)target_path;
    (void)backing_path;
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_launcher_remove_posix(const wchar_t *target_path, char *error, size_t error_size) {
    (void)target_path;
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_launcher_uninstall_commit(const wchar_t *canonical_launcher_path,
                                           const char payload_sha256[65], char *error,
                                           size_t error_size) {
    (void)canonical_launcher_path;
    (void)payload_sha256;
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_generation_rollback_if_unreferenced(const wchar_t *canonical_launcher_path,
                                                     const char payload_sha256[65],
                                                     bool created_by_activation, char *error,
                                                     size_t error_size) {
    (void)canonical_launcher_path;
    (void)payload_sha256;
    (void)created_by_activation;
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

bool cbm_windows_generations_prune(const wchar_t *canonical_launcher_path, size_t *removed_out,
                                   char *error, size_t error_size) {
    (void)canonical_launcher_path;
    if (removed_out) {
        *removed_out = 0U;
    }
    launcher_error(error, error_size, "Windows launcher support is unavailable");
    return false;
}

#else

/* The Windows implementation follows below.  Keep the byte codec above free
 * of platform conditionals so all hosts continuously test the release ABI. */

#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0A00
#endif
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <aclapi.h>
#include <sddl.h>

#ifndef PROC_THREAD_ATTRIBUTE_JOB_LIST
#define PROC_THREAD_ATTRIBUTE_JOB_LIST ((DWORD_PTR)0x0002000dU)
#endif

#define CBM_LAUNCH_CONTEXT_ENV L"CBM_WINDOWS_LAUNCH_CONTEXT_HANDLE_V1"
#define CBM_LAUNCH_CONTEXT_HEADER_SIZE 128U
#define CBM_LAUNCH_CONTEXT_FLAG_MANAGED 0x00000001U
#define CBM_LAUNCH_CONTEXT_FLAG_PRIVATE 0x00000002U

static const uint8_t launch_context_magic[8] = {
    'C', 'B', 'M', 'L', 'C', 'T', '1', '\0',
};

/* The launcher source writes this exact explicit byte layout. */
static bool launch_context_header_decode(const uint8_t input[CBM_LAUNCH_CONTEXT_HEADER_SIZE],
                                         uint32_t *flags_out,
                                         cbm_windows_launcher_action_t *action_out,
                                         DWORD *server_pid_out, FILETIME *creation_out,
                                         uint64_t *payload_size_out, char digest_out[65],
                                         uint32_t *path_chars_out) {
    if (memcmp(input, launch_context_magic, sizeof(launch_context_magic)) != 0 ||
        read_u32_le(input + 8U) != 1U ||
        read_u32_le(input + 12U) != CBM_LAUNCH_CONTEXT_HEADER_SIZE) {
        return false;
    }
    uint32_t flags = read_u32_le(input + 16U);
    uint32_t action = read_u32_le(input + 20U);
    uint32_t server_pid = read_u32_le(input + 24U);
    uint32_t path_chars = read_u32_le(input + 28U);
    uint64_t creation = read_u64_le(input + 32U);
    uint64_t payload_size = read_u64_le(input + 40U);
    char digest[65];
    memcpy(digest, input + 48U, 64U);
    digest[64] = '\0';
    for (size_t index = 112U; index < CBM_LAUNCH_CONTEXT_HEADER_SIZE; index++) {
        if (input[index] != 0U) {
            return false;
        }
    }
    bool managed = (flags & CBM_LAUNCH_CONTEXT_FLAG_MANAGED) != 0U;
    bool digest_zero = true;
    for (size_t index = 0U; index < 64U; index++) {
        if (input[48U + index] != 0U) {
            digest_zero = false;
        }
    }
    if ((flags & ~(CBM_LAUNCH_CONTEXT_FLAG_MANAGED | CBM_LAUNCH_CONTEXT_FLAG_PRIVATE)) != 0U ||
        (!managed && (flags & CBM_LAUNCH_CONTEXT_FLAG_PRIVATE) != 0U) ||
        action > (uint32_t)CBM_WINDOWS_LAUNCHER_ACTION_UNINSTALL || server_pid == 0U ||
        path_chars < 4U || path_chars >= CBM_WINDOWS_LAUNCHER_PATH_CAP || creation == 0U ||
        (managed && (payload_size == 0U || !lowercase_sha256_valid(digest))) ||
        (!managed && (payload_size != 0U || !digest_zero))) {
        return false;
    }
    *flags_out = flags;
    *action_out = (cbm_windows_launcher_action_t)action;
    *server_pid_out = server_pid;
    creation_out->dwLowDateTime = (DWORD)creation;
    creation_out->dwHighDateTime = (DWORD)(creation >> 32U);
    *payload_size_out = payload_size;
    memcpy(digest_out, digest, sizeof(digest));
    *path_chars_out = path_chars;
    return true;
}

static bool windows_read_exact(HANDLE file, void *buffer, size_t size) {
    size_t offset = 0U;
    while (offset < size) {
        DWORD chunk = size - offset > (size_t)MAXDWORD ? MAXDWORD : (DWORD)(size - offset);
        DWORD amount = 0U;
        if (!ReadFile(file, (uint8_t *)buffer + offset, chunk, &amount, NULL) || amount == 0U) {
            return false;
        }
        offset += amount;
    }
    return true;
}

static bool windows_pipe_wait_available(HANDLE pipe, size_t needed, uint64_t deadline) {
    if (needed > (size_t)MAXDWORD)
        return false;
    while (GetTickCount64() < deadline) {
        DWORD available = 0U;
        if (!PeekNamedPipe(pipe, NULL, 0U, NULL, &available, NULL)) {
            return false;
        }
        if ((size_t)available >= needed)
            return true;
        Sleep(2U);
    }
    return false;
}

static bool windows_file_information(HANDLE file, BY_HANDLE_FILE_INFORMATION *information) {
    return file && file != INVALID_HANDLE_VALUE && GetFileType(file) == FILE_TYPE_DISK &&
           GetFileInformationByHandle(file, information) != 0 &&
           (information->dwFileAttributes &
            (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0;
}

static bool windows_file_identity_links(HANDLE file, BY_HANDLE_FILE_INFORMATION *information,
                                        DWORD expected_links) {
    return windows_file_information(file, information) &&
           information->nNumberOfLinks == expected_links;
}

static bool windows_file_identity(HANDLE file, BY_HANDLE_FILE_INFORMATION *information) {
    return windows_file_identity_links(file, information, 1U);
}

static bool windows_same_identity(const BY_HANDLE_FILE_INFORMATION *first,
                                  const BY_HANDLE_FILE_INFORMATION *second) {
    return first->dwVolumeSerialNumber == second->dwVolumeSerialNumber &&
           first->nFileIndexHigh == second->nFileIndexHigh &&
           first->nFileIndexLow == second->nFileIndexLow;
}

static uint32_t windows_sid_read_u32_le(const uint8_t *bytes) {
    return (uint32_t)bytes[0] | ((uint32_t)bytes[1] << 8U) | ((uint32_t)bytes[2] << 16U) |
           ((uint32_t)bytes[3] << 24U);
}

static bool windows_sid_is_trusted_installer(PSID sid) {
    static const uint32_t subauthorities[] = {
        80U, 956008885U, 3418522649U, 1831038044U, 1853292631U, 2271478464U,
    };
    if (!sid || !IsValidSid(sid)) {
        return false;
    }
    DWORD sid_length = GetLengthSid(sid);
    const uint8_t *bytes = (const uint8_t *)sid;
    if (sid_length != 32U || bytes[0] != 1U || bytes[1] != 6U || bytes[2] != 0U || bytes[3] != 0U ||
        bytes[4] != 0U || bytes[5] != 0U || bytes[6] != 0U || bytes[7] != 5U) {
        return false;
    }
    for (size_t index = 0U; index < sizeof(subauthorities) / sizeof(subauthorities[0]); index++) {
        if (windows_sid_read_u32_le(bytes + 8U + index * 4U) != subauthorities[index]) {
            return false;
        }
    }
    return true;
}

static bool windows_sid_is_trusted(PSID sid, PSID current_user) {
    return sid && current_user && IsValidSid(sid) &&
           (EqualSid(sid, current_user) || IsWellKnownSid(sid, WinLocalSystemSid) ||
            IsWellKnownSid(sid, WinBuiltinAdministratorsSid) ||
            windows_sid_is_trusted_installer(sid));
}

static bool windows_bounded_ace_sid_is_trusted(const ACE_HEADER *header, PSID current_user) {
    size_t sid_offset = offsetof(ACCESS_ALLOWED_ACE, SidStart);
    if (!header || (size_t)header->AceSize < sid_offset + 8U) {
        return false;
    }
    const ACCESS_ALLOWED_ACE *ace = (const ACCESS_ALLOWED_ACE *)header;
    const uint8_t *sid = (const uint8_t *)&ace->SidStart;
    size_t sid_capacity = (size_t)header->AceSize - sid_offset;
    if (sid[0] != 1U || sid[1] > 15U) {
        return false;
    }
    size_t sid_length = 8U + (size_t)sid[1] * 4U;
    return sid_length <= sid_capacity && IsValidSid((PSID)sid) &&
           GetLengthSid((PSID)sid) == (DWORD)sid_length &&
           (windows_sid_is_trusted((PSID)sid, current_user) ||
            (((header->AceFlags & INHERIT_ONLY_ACE) != 0U) &&
             IsWellKnownSid((PSID)sid, WinCreatorOwnerSid)) ||
            /* OWNER RIGHTS (S-1-3-4) modulates the rights of whoever OWNS the
             * object; the owner is separately validated by
             * windows_owner_secure in every chain that reaches here, so such
             * an ACE only ever grants to an already-trusted identity. Default
             * Windows profile/temp ACLs (and GitHub runner profiles) carry
             * it; the daemon IPC ancestry validator documents the same
             * tolerance. */
            IsWellKnownSid((PSID)sid, WinCreatorOwnerRightsSid));
}

static bool windows_owner_secure(HANDLE file, bool require_current_user) {
    HANDLE token = NULL;
    DWORD token_size = 0U;
    PTOKEN_USER user = NULL;
    PSID owner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    bool ok = OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token) != 0;
    if (ok) {
        (void)GetTokenInformation(token, TokenUser, NULL, 0U, &token_size);
        user = token_size ? (PTOKEN_USER)malloc(token_size) : NULL;
        ok = user && GetTokenInformation(token, TokenUser, user, token_size, &token_size) != 0;
    }
    if (ok) {
        ok = GetSecurityInfo(file, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &owner, NULL, NULL,
                             NULL, &descriptor) == ERROR_SUCCESS &&
             owner && IsValidSid(owner) &&
             (require_current_user ? EqualSid(owner, user->User.Sid) != 0
                                   : windows_sid_is_trusted(owner, user->User.Sid));
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    free(user);
    if (token) {
        (void)CloseHandle(token);
    }
    return ok;
}

static bool windows_owner_is_current(HANDLE file) {
    return windows_owner_secure(file, true);
}

static DWORD windows_private_mutation_rights(void) {
    return GENERIC_ALL | GENERIC_WRITE | FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_ADD_FILE |
           FILE_ADD_SUBDIRECTORY | FILE_DELETE_CHILD | FILE_WRITE_EA | FILE_WRITE_ATTRIBUTES |
           DELETE | WRITE_DAC | WRITE_OWNER | ACCESS_SYSTEM_SECURITY;
}

static bool windows_acl_secure_for_mutation(HANDLE file, DWORD mutation) {
    HANDLE token = NULL;
    DWORD token_size = 0U;
    PTOKEN_USER user = NULL;
    PACL dacl = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    bool secure = OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token) != 0;
    if (secure) {
        (void)GetTokenInformation(token, TokenUser, NULL, 0U, &token_size);
        user = token_size ? (PTOKEN_USER)malloc(token_size) : NULL;
        secure = user && GetTokenInformation(token, TokenUser, user, token_size, &token_size) != 0;
    }
    DWORD status = secure ? GetSecurityInfo(file, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION, NULL,
                                            NULL, &dacl, NULL, &descriptor)
                          : ERROR_ACCESS_DENIED;
    ACL_SIZE_INFORMATION information;
    memset(&information, 0, sizeof(information));
    secure = secure && status == ERROR_SUCCESS && descriptor && dacl && IsValidAcl(dacl) != 0 &&
             GetAclInformation(dacl, &information, sizeof(information), AclSizeInformation) != 0;
    enum {
        CBM_ACE_ALLOW = 0x00,
        CBM_ACE_DENY = 0x01,
        CBM_ACE_DENY_OBJECT = 0x06,
        CBM_ACE_DENY_CALLBACK = 0x0a,
        CBM_ACE_DENY_CALLBACK_OBJECT = 0x0c,
    };
    for (DWORD index = 0U; secure && index < information.AceCount; index++) {
        void *opaque = NULL;
        if (!GetAce(dacl, index, &opaque) || !opaque) {
            secure = false;
            break;
        }
        ACE_HEADER *header = opaque;
        if (header->AceType == CBM_ACE_DENY || header->AceType == CBM_ACE_DENY_OBJECT ||
            header->AceType == CBM_ACE_DENY_CALLBACK ||
            header->AceType == CBM_ACE_DENY_CALLBACK_OBJECT) {
            continue;
        }
        if (header->AceType != CBM_ACE_ALLOW ||
            (size_t)header->AceSize <
                offsetof(ACCESS_ALLOWED_ACE, SidStart) + offsetof(SID, SubAuthority)) {
            secure = false;
            break;
        }
        ACCESS_ALLOWED_ACE *ace = opaque;
        if ((ace->Mask & mutation) == 0U) {
            continue;
        }
        if (!windows_bounded_ace_sid_is_trusted(header, user->User.Sid)) {
            secure = false;
        }
    }
    if (descriptor)
        (void)LocalFree(descriptor);
    free(user);
    if (token)
        (void)CloseHandle(token);
    return secure;
}

static bool windows_acl_secure(HANDLE file) {
    return windows_acl_secure_for_mutation(file, windows_private_mutation_rights());
}

static bool windows_path_tree_plain(const wchar_t *file_path) {
    size_t length = file_path ? wcslen(file_path) : 0U;
    /* Accept the extended-length prefix for local drive paths: launchers may
     * legitimately resolve their image through \\?\C:\... (>MAX_PATH-safe
     * form). The prefix is dropped for shape checks and the component walk;
     * \\?\UNC\... stays rejected with every other non-local namespace. */
    if (length >= 4U && file_path[0] == L'\\' && file_path[1] == L'\\' && file_path[2] == L'?' &&
        file_path[3] == L'\\') {
        file_path += 4;
        length -= 4U;
    }
    if (length < 4U || length >= CBM_WINDOWS_LAUNCHER_PATH_CAP || file_path[1] != L':' ||
        (file_path[2] != L'\\' && file_path[2] != L'/')) {
        return false;
    }
    /* Walk in the extended-length namespace: the cumulative ancestor paths of
     * a >MAX_PATH target overflow the legacy limit even though the full path
     * itself opens, so every component open below carries the \\?\\ prefix.
     * Boundary offsets stay relative to the plain form (component index i is
     * prefixed-buffer index i + 4). */
    wchar_t *path = malloc((length + 5U) * sizeof(*path));
    if (!path) {
        return false;
    }
    wmemcpy(path, L"\\\\?\\", 4);
    memcpy(path + 4, file_path, (length + 1U) * sizeof(*path));
    for (size_t index = 0U; index < length; index++) {
        if (path[4U + index] == L'/')
            path[4U + index] = L'\\';
    }
    wchar_t *last = wcsrchr(path + 4, L'\\');
    if (!last || last <= path + 6) {
        free(path);
        return false;
    }
    *last = L'\0';
    size_t directory_length = wcslen(path + 4);
    bool valid = true;
    for (size_t index = 3U; valid && index <= directory_length; index++) {
        if (index < directory_length && path[4U + index] != L'\\')
            continue;
        wchar_t saved = path[4U + index];
        path[4U + index] = L'\0';
        HANDLE component =
            CreateFileW(path, FILE_READ_ATTRIBUTES | READ_CONTROL,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
        BY_HANDLE_FILE_INFORMATION information;
        DWORD mutation = windows_private_mutation_rights();
        if (index < directory_length) {
            /* Default C:\\Users ACLs allow sibling-directory creation. That
             * cannot replace the existing next component. The executable's
             * immediate parent remains fully private. */
            mutation &= ~((DWORD)FILE_ADD_SUBDIRECTORY);
        }
        bool open_ok =
            component != INVALID_HANDLE_VALUE && GetFileType(component) == FILE_TYPE_DISK;
        bool info_ok = open_ok && GetFileInformationByHandle(component, &information) != 0 &&
                       (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                       (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
        bool owner_ok = info_ok && windows_owner_secure(component, false);
        bool acl_ok = owner_ok && windows_acl_secure_for_mutation(component, mutation);
        valid = acl_ok;
        if (component != INVALID_HANDLE_VALUE)
            (void)CloseHandle(component);
        path[4U + index] = saved;
    }
    free(path);
    return valid;
}

static HANDLE windows_open_regular_no_reparse_links(const wchar_t *path, DWORD access,
                                                    DWORD expected_links) {
    HANDLE file =
        CreateFileW(path, access | FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    BY_HANDLE_FILE_INFORMATION information;
    if (!windows_path_tree_plain(path) || !windows_file_information(file, &information) ||
        (expected_links != 0U && information.nNumberOfLinks != expected_links) ||
        !windows_owner_is_current(file) || !windows_acl_secure(file)) {
        if (file != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(file);
        }
        return INVALID_HANDLE_VALUE;
    }
    return file;
}

static HANDLE windows_open_regular_no_reparse(const wchar_t *path, DWORD access) {
    return windows_open_regular_no_reparse_links(path, access, 1U);
}

/* The launcher may be owned by the installing user or by one of the bounded
 * privileged principals accepted by the permanent launcher.  Keep this
 * context-only opener separate from the exact-current-owner opener above:
 * mutable transaction files must never inherit this broader owner policy. */
static HANDLE windows_open_launcher_context_file(const wchar_t *path, DWORD expected_links) {
    HANDLE file =
        CreateFileW(path, GENERIC_READ | FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    BY_HANDLE_FILE_INFORMATION information;
    bool tree_ok = windows_path_tree_plain(path);
    bool links_ok = tree_ok && windows_file_identity_links(file, &information, expected_links);
    bool owner_ok = links_ok && windows_owner_secure(file, false);
    bool acl_ok = owner_ok && windows_acl_secure(file);
    if (!acl_ok) {
        if (file != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(file);
        }
        return INVALID_HANDLE_VALUE;
    }
    return file;
}

static bool windows_create_directory_private(const wchar_t *path);
static bool windows_stamp_current_owner_private(HANDLE file);

static HANDLE windows_open_directory_secure_access(const wchar_t *path, DWORD access) {
    HANDLE directory =
        CreateFileW(path, access | FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    BY_HANDLE_FILE_INFORMATION information;
    bool valid = directory != INVALID_HANDLE_VALUE && GetFileType(directory) == FILE_TYPE_DISK &&
                 GetFileInformationByHandle(directory, &information) != 0 &&
                 (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                 (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
                 windows_owner_is_current(directory) && windows_acl_secure(directory);
    if (!valid) {
        if (directory != INVALID_HANDLE_VALUE)
            (void)CloseHandle(directory);
        return INVALID_HANDLE_VALUE;
    }
    return directory;
}

static HANDLE windows_open_directory_secure(const wchar_t *path) {
    return windows_open_directory_secure_access(path, 0U);
}

static bool windows_process_creation(HANDLE process, FILETIME *creation_out) {
    FILETIME exit_time;
    FILETIME kernel_time;
    FILETIME user_time;
    return GetProcessTimes(process, creation_out, &exit_time, &kernel_time, &user_time) != 0;
}

static bool windows_filetime_equal(const FILETIME *first, const FILETIME *second) {
    return first->dwLowDateTime == second->dwLowDateTime &&
           first->dwHighDateTime == second->dwHighDateTime;
}

bool cbm_windows_launcher_context_consume(cbm_windows_launcher_context_t *context_out, char *error,
                                          size_t error_size) {
    if (context_out) {
        memset(context_out, 0, sizeof(*context_out));
    }
    if (error && error_size > 0U) {
        error[0] = '\0';
    }
    if (!context_out) {
        launcher_error(error, error_size, "invalid launcher context output");
        return false;
    }
    wchar_t encoded[32];
    DWORD length = GetEnvironmentVariableW(CBM_LAUNCH_CONTEXT_ENV, encoded,
                                           (DWORD)(sizeof(encoded) / sizeof(encoded[0])));
    if (length == 0U && GetLastError() == ERROR_ENVVAR_NOT_FOUND) {
        return true;
    }
    /* Scrub before validation: malformed authority must never reach a child. */
    (void)SetEnvironmentVariableW(CBM_LAUNCH_CONTEXT_ENV, NULL);
    if (length == 0U || length >= sizeof(encoded) / sizeof(encoded[0])) {
        launcher_error(error, error_size, "invalid inherited launcher context handle");
        return false;
    }
    wchar_t *end = NULL;
    unsigned long long raw = wcstoull(encoded, &end, 16);
    if (!end || *end != L'\0' || raw == 0ULL || raw > (unsigned long long)(uintptr_t)UINTPTR_MAX) {
        launcher_error(error, error_size, "invalid inherited launcher context handle");
        return false;
    }
    HANDLE pipe = (HANDLE)(uintptr_t)raw;
    uint8_t header[CBM_LAUNCH_CONTEXT_HEADER_SIZE];
    uint32_t flags = 0U;
    cbm_windows_launcher_action_t action = CBM_WINDOWS_LAUNCHER_ACTION_ORDINARY;
    DWORD claimed_pid = 0U;
    FILETIME claimed_creation;
    uint64_t payload_size = 0U;
    char digest[65];
    uint32_t path_chars = 0U;
    ULONG actual_pid = 0U;
    uint64_t context_now = GetTickCount64();
    uint64_t context_deadline = UINT64_MAX - context_now < 5000U ? UINT64_MAX : context_now + 5000U;
    bool valid =
        GetFileType(pipe) == FILE_TYPE_PIPE &&
        GetNamedPipeServerProcessId(pipe, &actual_pid) != 0 &&
        windows_pipe_wait_available(pipe, sizeof(header), context_deadline) &&
        windows_read_exact(pipe, header, sizeof(header)) &&
        launch_context_header_decode(header, &flags, &action, &claimed_pid, &claimed_creation,
                                     &payload_size, digest, &path_chars) &&
        actual_pid == claimed_pid;
    wchar_t claimed_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    memset(claimed_path, 0, sizeof(claimed_path));
    valid = valid &&
            windows_pipe_wait_available(pipe, (size_t)path_chars * sizeof(*claimed_path),
                                        context_deadline) &&
            windows_read_exact(pipe, claimed_path, (size_t)path_chars * sizeof(*claimed_path)) &&
            claimed_path[path_chars - 1U] == L'\0' &&
            wmemchr(claimed_path, L'\0', path_chars) == claimed_path + path_chars - 1U;
    DWORD trailing_count = 0U;
    valid = valid && PeekNamedPipe(pipe, NULL, 0U, NULL, &trailing_count, NULL) != 0 &&
            trailing_count == 0U;

    HANDLE server =
        valid ? OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, actual_pid) : NULL;
    FILETIME actual_creation;
    wchar_t actual_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    DWORD actual_path_length = CBM_WINDOWS_LAUNCHER_PATH_CAP;
    valid = valid && server && windows_process_creation(server, &actual_creation) &&
            windows_filetime_equal(&actual_creation, &claimed_creation) &&
            QueryFullProcessImageNameW(server, 0U, actual_path, &actual_path_length) != 0;
    if (server) {
        (void)CloseHandle(server);
    }
    const char *context_error = "invalid or unauthenticated Windows launcher context";
    DWORD expected_launcher_links = (flags & CBM_LAUNCH_CONTEXT_FLAG_MANAGED) != 0U ? 2U : 1U;
    HANDLE actual_file = INVALID_HANDLE_VALUE;
    HANDLE claimed_file = INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION actual_info;
    BY_HANDLE_FILE_INFORMATION claimed_info;
    if (valid) {
        actual_file = windows_open_launcher_context_file(actual_path, expected_launcher_links);
        if (actual_file == INVALID_HANDLE_VALUE) {
            context_error = "Windows launcher context actual image failed secure open";
            valid = false;
        }
    }
    if (valid) {
        claimed_file = windows_open_launcher_context_file(claimed_path, expected_launcher_links);
        if (claimed_file == INVALID_HANDLE_VALUE) {
            context_error = "Windows launcher context claimed path failed secure open";
            valid = false;
        }
    }
    if (valid && !windows_file_identity_links(actual_file, &actual_info, expected_launcher_links)) {
        context_error = "Windows launcher context actual image failed secure open";
        valid = false;
    }
    if (valid &&
        !windows_file_identity_links(claimed_file, &claimed_info, expected_launcher_links)) {
        context_error = "Windows launcher context claimed path failed secure open";
        valid = false;
    }
    if (valid && !windows_same_identity(&actual_info, &claimed_info)) {
        context_error = "Windows launcher context actual/claimed file identity mismatch";
        valid = false;
    }
    if (actual_file != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(actual_file);
    }
    if (claimed_file != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(claimed_file);
    }
    if (!valid) {
        (void)CloseHandle(pipe);
        launcher_error(error, error_size, context_error);
        memset(context_out, 0, sizeof(*context_out));
        return false;
    }
    context_out->present = true;
    context_out->managed = (flags & CBM_LAUNCH_CONTEXT_FLAG_MANAGED) != 0U;
    context_out->private_activation = (flags & CBM_LAUNCH_CONTEXT_FLAG_PRIVATE) != 0U;
    context_out->action = action;
    context_out->payload_size = payload_size;
    memcpy(context_out->expected_payload_sha256, digest, sizeof(digest));
    memcpy(context_out->canonical_launcher_path, claimed_path,
           (size_t)path_chars * sizeof(*claimed_path));
    context_out->_authority_handle = (uintptr_t)pipe;
    return true;
}

static bool windows_pipe_read_byte_until(HANDLE pipe, uint8_t *value_out, uint64_t deadline) {
    while (GetTickCount64() < deadline) {
        DWORD available = 0U;
        if (!PeekNamedPipe(pipe, NULL, 0U, NULL, &available, NULL)) {
            return false;
        }
        if (available > 0U) {
            DWORD received = 0U;
            return ReadFile(pipe, value_out, 1U, &received, NULL) != 0 && received == 1U;
        }
        Sleep(2U);
    }
    return false;
}

bool cbm_windows_launcher_context_complete(cbm_windows_launcher_context_t *context, bool accepted,
                                           char *error, size_t error_size) {
    if (error && error_size > 0U)
        error[0] = '\0';
    if (!context) {
        launcher_error(error, error_size, "invalid launcher context completion");
        return false;
    }
    if (!context->present) {
        return context->_authority_handle == 0U;
    }
    HANDLE pipe = (HANDLE)context->_authority_handle;
    context->_authority_handle = 0U;
    if (!pipe || pipe == INVALID_HANDLE_VALUE) {
        launcher_error(error, error_size, "launcher context authority was already consumed");
        return false;
    }
    uint8_t ready = accepted ? (uint8_t)'R' : (uint8_t)'X';
    DWORD written = 0U;
    bool ok = WriteFile(pipe, &ready, 1U, &written, NULL) != 0 && written == 1U;
    if (ok && accepted) {
        uint8_t result = 0U;
        uint64_t now = GetTickCount64();
        uint64_t deadline = UINT64_MAX - now < 30000U ? UINT64_MAX : now + 30000U;
        ok = windows_pipe_read_byte_until(pipe, &result, deadline) && result == (uint8_t)'G';
    }
    (void)CloseHandle(pipe);
    if (!ok) {
        launcher_error(error, error_size,
                       "launcher rejected or timed out completing payload authentication");
    }
    return ok;
}

/* Remaining native transaction/probe helpers are below the launcher-facing
 * context code to keep their private Windows structures out of the ABI. */

typedef struct {
    DWORD Flags;
} cbm_file_disposition_info_ex_t;

typedef struct {
    DWORD Flags;
    HANDLE RootDirectory;
    DWORD FileNameLength;
    WCHAR FileName[1];
} cbm_file_rename_info_ex_t;

#define CBM_FILE_DISPOSITION_INFO_EX_CLASS ((FILE_INFO_BY_HANDLE_CLASS)21)
#define CBM_FILE_RENAME_INFO_EX_CLASS ((FILE_INFO_BY_HANDLE_CLASS)22)
#define CBM_FILE_DISPOSITION_DELETE 0x00000001U
#define CBM_FILE_DISPOSITION_POSIX 0x00000002U
#define CBM_FILE_RENAME_REPLACE 0x00000001U
#define CBM_FILE_RENAME_POSIX 0x00000002U

static bool windows_posix_remove_handle(HANDLE file) {
    cbm_file_disposition_info_ex_t disposition = {
        .Flags = CBM_FILE_DISPOSITION_DELETE | CBM_FILE_DISPOSITION_POSIX,
    };
    return SetFileInformationByHandle(file, CBM_FILE_DISPOSITION_INFO_EX_CLASS, &disposition,
                                      sizeof(disposition)) != 0;
}

static bool windows_posix_rename_handle_flags(HANDLE file, const wchar_t *target_path,
                                              bool replace_existing) {
    /* The rename target travels to NtSetInformationFile. A bare drive path is
     * converted internally with the legacy MAX_PATH bound (deep targets fail
     * with ERROR_FILENAME_EXCED_RANGE), and the Win32 \\\\?\\ prefix is not a
     * valid NT name — the NT namespace form \\??\\ is, at any length. */
    size_t chars = wcslen(target_path);
    if (chars == 0U || chars > (size_t)UINT32_MAX / sizeof(wchar_t)) {
        return false;
    }

    /* The rename TARGET must be the bare Win32 path — harness-proven on this
     * kernel: FileRenameInfoEx submits the name to NtSetInformationFile, which
     * rejects the \\\\?\\ extended-length prefix (ERROR_INVALID_NAME, os 123)
     * AND the NT \\??\\ form (os 123), while accepting a bare drive path up to
     * the legacy limit. Strip any inherited prefix; never add one. */
    if (chars >= 4U && wcsncmp(target_path, L"\\\\?\\", 4) == 0) {
        target_path += 4;
        chars -= 4U;
    }
    size_t bytes = chars * sizeof(wchar_t);
    /* Allocate one extra WCHAR and NUL-terminate the name. FileNameLength governs
     * per the contract, but antivirus/filter drivers on this kernel read the
     * FileName as NUL-terminated: with an exactly-sized buffer they append
     * adjacent heap bytes to the created name (a flaky, garbage-suffixed rename
     * target). The terminator bounds them to the intended name. */
    size_t allocation = offsetof(cbm_file_rename_info_ex_t, FileName) + bytes + sizeof(wchar_t);
    cbm_file_rename_info_ex_t *rename = calloc(1U, allocation);
    if (!rename) {
        return false;
    }
    rename->Flags = CBM_FILE_RENAME_POSIX | (replace_existing ? CBM_FILE_RENAME_REPLACE : 0U);
    rename->RootDirectory = NULL;
    rename->FileNameLength = (DWORD)bytes;
    memcpy(rename->FileName, target_path, chars * sizeof(wchar_t));
    rename->FileName[chars] = L'\0';
    /* First-touch antivirus scanning (Defender filter drivers) transiently
     * vetoes renames of trees holding a just-created executable — observed as
     * ERROR_INVALID_NAME on a name that is provably valid moments later, and
     * classically as ACCESS_DENIED/SHARING_VIOLATION. Freshly built or
     * downloaded binaries are always cold, so first-run installs must absorb
     * the scan window with a bounded backoff. */
    bool renamed = false;
    DWORD last_error = ERROR_SUCCESS;
    for (DWORD wait_ms = 0U, waited_ms = 0U;; waited_ms += wait_ms) {
        renamed = SetFileInformationByHandle(file, CBM_FILE_RENAME_INFO_EX_CLASS, rename,
                                             (DWORD)allocation) != 0;
        if (renamed) {
            break;
        }
        last_error = GetLastError();
        bool transient = last_error == ERROR_INVALID_NAME || last_error == ERROR_ACCESS_DENIED ||
                         last_error == ERROR_SHARING_VIOLATION;
        if (!transient || waited_ms >= 2000U) {
            break;
        }
        wait_ms = wait_ms == 0U ? 100U : (wait_ms < 1000U ? wait_ms * 2U : wait_ms);
        Sleep(wait_ms);
    }
    free(rename);
    if (!renamed) {
        SetLastError(last_error);
    }
    return renamed;
}

static bool windows_posix_rename_handle(HANDLE file, const wchar_t *target_path) {
    return windows_posix_rename_handle_flags(file, target_path, true);
}

static bool windows_posix_rename_handle_no_replace(HANDLE file, const wchar_t *target_path) {
    return windows_posix_rename_handle_flags(file, target_path, false);
}

static bool windows_parent_path(const wchar_t *path, wchar_t *parent, size_t capacity) {
    if (!path || !parent || capacity == 0U) {
        return false;
    }
    size_t length = wcslen(path);
    if (length + 1U > capacity) {
        return false;
    }
    memcpy(parent, path, (length + 1U) * sizeof(*parent));
    wchar_t *separator = wcsrchr(parent, L'\\');
    wchar_t *slash = wcsrchr(parent, L'/');
    if (!separator || (slash && slash > separator)) {
        separator = slash;
    }
    if (!separator || separator <= parent + 2) {
        return false;
    }
    *separator = L'\0';
    return true;
}

static bool windows_copy_flush_private(const wchar_t *candidate, const wchar_t *stage) {
    /* Capability probes may copy the already-managed canonical launcher
     * (exact two links); candidate publication validation separately requires
     * an unpublished generation launcher to have exactly one link. */
    HANDLE source = windows_open_regular_no_reparse_links(candidate, GENERIC_READ, 0U);
    if (source == INVALID_HANDLE_VALUE || !windows_path_tree_plain(stage) ||
        !CopyFileW(candidate, stage, TRUE)) {
        if (source != INVALID_HANDLE_VALUE)
            (void)CloseHandle(source);
        return false;
    }
    (void)CloseHandle(source);
    HANDLE file =
        CreateFileW(stage, GENERIC_READ | GENERIC_WRITE | DELETE | WRITE_OWNER | WRITE_DAC,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH, NULL);
    BY_HANDLE_FILE_INFORMATION information;
    /* CopyFileW's destination inherits the machine's default owner (e.g.
     * Administrators on runner-class images); re-stamp the exact current user
     * as owner and re-apply the owner-only protected DACL so the probe's own
     * exact-owner validation accepts the freshly staged image. */
    bool stamped = windows_stamp_current_owner_private(file);
    bool ok = stamped && windows_file_identity(file, &information) &&
              windows_owner_is_current(file) && windows_acl_secure(file) &&
              FlushFileBuffers(file) != 0;
    if (file != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(file);
    }
    if (!ok) {
        (void)DeleteFileW(stage);
    }
    return ok;
}

static bool windows_unique_sibling(const wchar_t *target, const wchar_t *tag, wchar_t *path,
                                   size_t capacity) {
    static volatile LONG counter = 0;
    LONG sequence = InterlockedIncrement(&counter);
    int written = swprintf(path, capacity, L"%ls.cbm-%ls-%lu-%ld.tmp", target, tag,
                           (unsigned long)GetCurrentProcessId(), (long)sequence);
    return written > 0 && (size_t)written < capacity;
}

static bool windows_generation_layout_exact(const wchar_t *generation_directory,
                                            DWORD expected_launcher_links,
                                            BY_HANDLE_FILE_INFORMATION *launcher_information_out);

static bool windows_backing_generation_layout_exact(const wchar_t *backing_path,
                                                    DWORD expected_launcher_links) {
    wchar_t generation_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    return windows_parent_path(backing_path, generation_directory, CBM_WINDOWS_LAUNCHER_PATH_CAP) &&
           windows_generation_layout_exact(generation_directory, expected_launcher_links, NULL);
}

bool cbm_windows_launcher_replace_atomic(const wchar_t *target_path, const wchar_t *backing_path,
                                         char *error, size_t error_size) {
    if (error && error_size > 0U)
        error[0] = '\0';
    if (!target_path || !backing_path || !target_path[0] || !backing_path[0]) {
        launcher_error(error, error_size, "invalid launcher replacement paths");
        return false;
    }
    HANDLE backing = windows_open_regular_no_reparse_links(backing_path, GENERIC_READ, 0U);
    DWORD target_attributes = GetFileAttributesW(target_path);
    DWORD target_error =
        target_attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
    HANDLE target = target_attributes == INVALID_FILE_ATTRIBUTES
                        ? INVALID_HANDLE_VALUE
                        : windows_open_regular_no_reparse_links(target_path, GENERIC_READ, 0U);
    bool target_absent =
        target_attributes == INVALID_FILE_ATTRIBUTES &&
        (target_error == ERROR_FILE_NOT_FOUND || target_error == ERROR_PATH_NOT_FOUND);
    BY_HANDLE_FILE_INFORMATION backing_before;
    BY_HANDLE_FILE_INFORMATION target_before;
    bool backing_valid = windows_file_information(backing, &backing_before);
    bool target_valid = target_absent || windows_file_information(target, &target_before);
    bool already_published = backing_valid && target_valid && !target_absent &&
                             windows_same_identity(&backing_before, &target_before) &&
                             backing_before.nNumberOfLinks == 2U &&
                             target_before.nNumberOfLinks == 2U;
    if (already_published) {
        bool exact_layout = windows_backing_generation_layout_exact(backing_path, 2U);
        (void)CloseHandle(backing);
        (void)CloseHandle(target);
        if (!exact_layout) {
            launcher_error(error, error_size,
                           "published launcher generation has extra or missing entries");
        }
        return exact_layout;
    }
    if (!backing_valid || backing_before.nNumberOfLinks != 1U || !target_valid ||
        (!target_absent && target_before.nNumberOfLinks != 2U) ||
        !windows_path_tree_plain(target_path) ||
        !windows_backing_generation_layout_exact(backing_path, 1U)) {
        if (backing != INVALID_HANDLE_VALUE)
            (void)CloseHandle(backing);
        if (target != INVALID_HANDLE_VALUE)
            (void)CloseHandle(target);
        launcher_error(error, error_size,
                       "launcher backing or existing managed target has an unsafe link layout");
        return false;
    }
    (void)CloseHandle(backing);
    if (target != INVALID_HANDLE_VALUE)
        (void)CloseHandle(target);
    wchar_t stage[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    if (!windows_unique_sibling(target_path, L"hardlink", stage, CBM_WINDOWS_LAUNCHER_PATH_CAP) ||
        !CreateHardLinkW(stage, backing_path, NULL)) {
        launcher_error(error, error_size, "could not create launcher hard-link stage");
        return false;
    }
    HANDLE stage_file = windows_open_regular_no_reparse_links(stage, DELETE, 2U);
    HANDLE backing_after_link =
        windows_open_regular_no_reparse_links(backing_path, GENERIC_READ, 2U);
    BY_HANDLE_FILE_INFORMATION stage_information;
    BY_HANDLE_FILE_INFORMATION backing_information;
    bool ok = windows_file_identity_links(stage_file, &stage_information, 2U) &&
              windows_file_identity_links(backing_after_link, &backing_information, 2U) &&
              windows_same_identity(&stage_information, &backing_information) &&
              windows_posix_rename_handle(stage_file, target_path);
    if (stage_file != INVALID_HANDLE_VALUE)
        (void)CloseHandle(stage_file);
    if (backing_after_link != INVALID_HANDLE_VALUE)
        (void)CloseHandle(backing_after_link);
    if (!ok) {
        (void)DeleteFileW(stage);
        launcher_error(error, error_size,
                       "atomic hard-link launcher replacement is unsupported or failed");
        return false;
    }

    HANDLE canonical_check = windows_open_regular_no_reparse_links(target_path, GENERIC_READ, 2U);
    HANDLE backing_check = windows_open_regular_no_reparse_links(backing_path, GENERIC_READ, 2U);
    BY_HANDLE_FILE_INFORMATION canonical_information;
    BY_HANDLE_FILE_INFORMATION final_backing_information;
    bool committed = windows_file_identity_links(canonical_check, &canonical_information, 2U) &&
                     windows_file_identity_links(backing_check, &final_backing_information, 2U) &&
                     windows_same_identity(&canonical_information, &final_backing_information);
    if (canonical_check != INVALID_HANDLE_VALUE)
        (void)CloseHandle(canonical_check);
    if (backing_check != INVALID_HANDLE_VALUE)
        (void)CloseHandle(backing_check);
    if (!committed) {
        launcher_error(error, error_size,
                       "published launcher did not retain its exact hard-link identity");
    }
    return committed;
}

static bool cbm_windows_launcher_remove_posix_links(const wchar_t *target_path,
                                                    DWORD expected_links, char *error,
                                                    size_t error_size) {
    if (error && error_size > 0U)
        error[0] = '\0';
    if (!target_path || !target_path[0]) {
        launcher_error(error, error_size, "invalid launcher removal path");
        return false;
    }
    HANDLE original =
        windows_open_regular_no_reparse_links(target_path, GENERIC_READ, expected_links);
    if (original == INVALID_HANDLE_VALUE) {
        DWORD attributes = GetFileAttributesW(target_path);
        DWORD remove_error = attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
        if (attributes == INVALID_FILE_ATTRIBUTES &&
            (remove_error == ERROR_FILE_NOT_FOUND || remove_error == ERROR_PATH_NOT_FOUND)) {
            return true;
        }
        launcher_error(error, error_size, "could not securely open launcher for removal");
        return false;
    }
    (void)CloseHandle(original);

    /* Windows refuses a delete disposition on a mapped image.  Uninstall uses
     * the same transaction proven by update: replace the mapped canonical
     * link with an unmapped two-link tombstone, then unlink only that fresh
     * name.  The mapped generation backing remains as its safe sole link. */
    wchar_t tombstone[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t stage[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    if (!windows_unique_sibling(target_path, L"unlink-backing", tombstone,
                                CBM_WINDOWS_LAUNCHER_PATH_CAP) ||
        !windows_unique_sibling(target_path, L"unlink-stage", stage,
                                CBM_WINDOWS_LAUNCHER_PATH_CAP)) {
        launcher_error(error, error_size, "launcher unlink staging path is too long");
        return false;
    }
    HANDLE tombstone_file = CreateFileW(
        tombstone,
        GENERIC_READ | GENERIC_WRITE | DELETE | WRITE_OWNER | WRITE_DAC | FILE_READ_ATTRIBUTES |
            READ_CONTROL,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, CREATE_NEW,
        FILE_ATTRIBUTE_HIDDEN | FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH, NULL);
    bool tombstone_created = tombstone_file != INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION tombstone_information;
    /* CREATE_NEW inherits the parent directory's default owner (Administrators
     * on runner-class images); re-stamp the exact current user so the unlink
     * transaction's own owner validation accepts this fresh tombstone. */
    bool tombstone_ready = windows_stamp_current_owner_private(tombstone_file) &&
                           windows_file_identity(tombstone_file, &tombstone_information) &&
                           windows_owner_is_current(tombstone_file) &&
                           windows_acl_secure(tombstone_file) && FlushFileBuffers(tombstone_file);
    if (tombstone_file != INVALID_HANDLE_VALUE)
        (void)CloseHandle(tombstone_file);
    bool stage_created = tombstone_ready && CreateHardLinkW(stage, tombstone, NULL) != 0;
    HANDLE stage_file = stage_created ? windows_open_regular_no_reparse_links(stage, DELETE, 2U)
                                      : INVALID_HANDLE_VALUE;
    HANDLE tombstone_link = stage_created
                                ? windows_open_regular_no_reparse_links(tombstone, GENERIC_READ, 2U)
                                : INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION stage_information;
    BY_HANDLE_FILE_INFORMATION tombstone_link_information;
    bool renamed = windows_file_identity_links(stage_file, &stage_information, 2U) &&
                   windows_file_identity_links(tombstone_link, &tombstone_link_information, 2U) &&
                   windows_same_identity(&stage_information, &tombstone_link_information) &&
                   windows_posix_rename_handle(stage_file, target_path);
    if (stage_file != INVALID_HANDLE_VALUE)
        (void)CloseHandle(stage_file);
    if (tombstone_link != INVALID_HANDLE_VALUE)
        (void)CloseHandle(tombstone_link);

    HANDLE replacement = renamed ? windows_open_regular_no_reparse_links(target_path, DELETE, 2U)
                                 : INVALID_HANDLE_VALUE;
    HANDLE tombstone_after =
        renamed ? windows_open_regular_no_reparse_links(tombstone, GENERIC_READ, 2U)
                : INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION replacement_information;
    BY_HANDLE_FILE_INFORMATION tombstone_after_information;
    bool target_removed =
        windows_file_identity_links(replacement, &replacement_information, 2U) &&
        windows_file_identity_links(tombstone_after, &tombstone_after_information, 2U) &&
        windows_same_identity(&replacement_information, &tombstone_after_information) &&
        windows_posix_remove_handle(replacement);
    if (replacement != INVALID_HANDLE_VALUE)
        (void)CloseHandle(replacement);
    if (tombstone_after != INVALID_HANDLE_VALUE)
        (void)CloseHandle(tombstone_after);

    HANDLE tombstone_final =
        target_removed ? windows_open_regular_no_reparse(tombstone, DELETE) : INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION tombstone_final_information;
    bool tombstone_removed = windows_file_identity(tombstone_final, &tombstone_final_information) &&
                             windows_posix_remove_handle(tombstone_final);
    if (tombstone_final != INVALID_HANDLE_VALUE)
        (void)CloseHandle(tombstone_final);

    if (!renamed && stage_created) {
        (void)DeleteFileW(stage);
    } else if (!target_removed) {
        /* Rename already committed: target now names only our unmapped
         * tombstone, never the original launcher, so cleanup is safe. */
        (void)DeleteFileW(target_path);
    }
    if (!tombstone_removed && tombstone_created)
        (void)DeleteFileW(tombstone);
    DWORD attributes = GetFileAttributesW(target_path);
    DWORD remove_error = attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
    bool removed = renamed && target_removed && tombstone_removed &&
                   attributes == INVALID_FILE_ATTRIBUTES &&
                   (remove_error == ERROR_FILE_NOT_FOUND || remove_error == ERROR_PATH_NOT_FOUND);
    if (!removed) {
        launcher_error(error, error_size,
                       "transactional POSIX launcher unlink is unsupported or failed");
    }
    return removed;
}

bool cbm_windows_launcher_remove_posix(const wchar_t *target_path, char *error, size_t error_size) {
    /* The canonical launcher is the managed two-link file (canonical name +
     * generation backing). Callers that have not intentionally dropped the
     * backing keep that invariant. */
    return cbm_windows_launcher_remove_posix_links(target_path, 2U, error, error_size);
}

/* 1 = valid current, 0 = absent, -1 = unsafe/corrupt. */
static int windows_current_v1_read(const wchar_t *canonical_launcher_path,
                                   cbm_windows_current_v1_t *state_out) {
    if (state_out)
        memset(state_out, 0, sizeof(*state_out));
    wchar_t directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t state_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t current[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    if (!canonical_launcher_path || !state_out ||
        !windows_parent_path(canonical_launcher_path, directory, CBM_WINDOWS_LAUNCHER_PATH_CAP)) {
        return -1;
    }
    int state_written =
        swprintf(state_directory, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\.cbm", directory);
    int current_written =
        swprintf(current, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\current-v1", state_directory);
    if (state_written <= 0 || current_written <= 0 ||
        (size_t)state_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP ||
        (size_t)current_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
        return -1;
    }
    HANDLE file = windows_open_regular_no_reparse(current, GENERIC_READ);
    if (file == INVALID_HANDLE_VALUE) {
        DWORD attributes = GetFileAttributesW(current);
        DWORD open_error = attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
        return attributes == INVALID_FILE_ATTRIBUTES &&
                       (open_error == ERROR_FILE_NOT_FOUND || open_error == ERROR_PATH_NOT_FOUND)
                   ? 0
                   : -1;
    }
    LARGE_INTEGER size;
    uint8_t record[CBM_WINDOWS_CURRENT_V1_SIZE];
    uint8_t trailing = 0U;
    DWORD trailing_count = 0U;
    bool valid = GetFileSizeEx(file, &size) != 0 && size.QuadPart == CBM_WINDOWS_CURRENT_V1_SIZE &&
                 windows_read_exact(file, record, sizeof(record)) &&
                 ReadFile(file, &trailing, 1U, &trailing_count, NULL) != 0 &&
                 trailing_count == 0U &&
                 cbm_windows_current_v1_decode(record, sizeof(record), state_out);
    (void)CloseHandle(file);
    return valid ? 1 : -1;
}

/* Relocate every generation backing executable OUT of the managed state tree so
 * the tree can be renamed aside during uninstall. The active generation's
 * launcher backing is hardlinked to the still-running managed launcher image; a
 * mapped file pins its directory against rename. A running image's LAST
 * directory name cannot be deleted, but the image CAN be renamed — so we MOVE
 * each backing to an `activation-<pid>-N.retired` tombstone beside the install.
 * That unpins the state tree while keeping the canonical the exact two-link
 * managed file for its own unlink; the moved name survives as the mapped
 * inode's link until this process exits, and the launcher's liveness-guarded
 * stale sweep reclaims the tombstone afterwards. Best-effort by design. */
/* A record of one generation backing moved aside to an activation-*.retired
 * tombstone during an unpin relocation. A FAILED uninstall must be able to
 * reverse every such move so the restored .cbm keeps its generation backings.
 * The full extended-length path cap is too large to hold on the stack per
 * record, so each path is heap-duplicated. */
typedef struct {
    wchar_t *from; /* the activation-*.retired tombstone the backing now lives at */
    wchar_t *to;   /* the original generation backing path to restore it to */
} windows_backing_move_t;

typedef struct {
    windows_backing_move_t *moves;
    size_t count;
    size_t capacity;
} windows_backing_relocation_log_t;

static void windows_backing_relocation_log_record(windows_backing_relocation_log_t *log,
                                                  const wchar_t *from, const wchar_t *to) {
    if (!log) {
        return;
    }
    if (log->count == log->capacity) {
        size_t next_capacity = log->capacity == 0U ? 8U : log->capacity * 2U;
        windows_backing_move_t *grown =
            (windows_backing_move_t *)realloc(log->moves, next_capacity * sizeof(*grown));
        if (!grown) {
            return; /* best-effort: an unrecorded move cannot be reversed, but the
                     * commit still proceeds correctly on the success path */
        }
        log->moves = grown;
        log->capacity = next_capacity;
    }
    size_t from_bytes = (wcslen(from) + 1U) * sizeof(wchar_t);
    size_t to_bytes = (wcslen(to) + 1U) * sizeof(wchar_t);
    wchar_t *from_copy = (wchar_t *)malloc(from_bytes);
    wchar_t *to_copy = (wchar_t *)malloc(to_bytes);
    if (!from_copy || !to_copy) {
        free(from_copy);
        free(to_copy);
        return;
    }
    (void)memcpy(from_copy, from, from_bytes);
    (void)memcpy(to_copy, to, to_bytes);
    log->moves[log->count].from = from_copy;
    log->moves[log->count].to = to_copy;
    log->count++;
}

/* Reverse every recorded unpin relocation, moving each tombstone back to its
 * original generation backing path. Called only on the uninstall failure paths
 * after .cbm has been restored, so the target generation directories exist
 * again. The generation backing path routinely exceeds MAX_PATH, which the
 * handle-based FileRenameInfoEx cannot reach (its bare full-path target is
 * bounded by the legacy limit, and SetFileInformationByHandle rejects a
 * RootDirectory-relative form with ERROR_INVALID_PARAMETER, os 87 —
 * VM-proven). MoveFileExW converts both arguments through the DOS→NT path
 * routine itself, so the \\?\ extended-length form works at any depth, and a
 * mapped image may be renamed (only deletion is blocked) — the same property
 * the relocation itself relies on. Without MOVEFILE_REPLACE_EXISTING this
 * keeps the no-replace semantics of the outbound move. Best-effort per move:
 * a rename the kernel refuses leaves that tombstone for the launcher's
 * liveness-guarded stale sweep rather than aborting the restore. */
static void windows_restore_relocated_backings(const windows_backing_relocation_log_t *log) {
    if (!log) {
        return;
    }
    for (size_t index = 0U; index < log->count; index++) {
        (void)MoveFileExW(log->moves[index].from, log->moves[index].to, 0U);
    }
}

static void windows_backing_relocation_log_free(windows_backing_relocation_log_t *log) {
    if (!log) {
        return;
    }
    for (size_t index = 0U; index < log->count; index++) {
        free(log->moves[index].from);
        free(log->moves[index].to);
    }
    free(log->moves);
    log->moves = NULL;
    log->count = 0U;
    log->capacity = 0U;
}

static void windows_relocate_generation_backings(const wchar_t *state_directory,
                                                 windows_backing_relocation_log_t *log) {
    if (!state_directory || !state_directory[0]) {
        return;
    }
    /* A generation backing path — a 64-hex generation directory nested under a
     * possibly-deep managed install — routinely exceeds MAX_PATH. Open it (and
     * enumerate) in the extended-length namespace, or CreateFileW fails with
     * ERROR_PATH_NOT_FOUND and the pin is never dropped. Normalize to a single
     * \\?\ prefix regardless of the caller's form. */
    const wchar_t *bare_state =
        wcsncmp(state_directory, L"\\\\?\\", 4) == 0 ? state_directory + 4 : state_directory;
    wchar_t install_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    if (!windows_parent_path(bare_state, install_directory, CBM_WINDOWS_LAUNCHER_PATH_CAP)) {
        return;
    }
    wchar_t pattern[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int written =
        swprintf(pattern, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"\\\\?\\%ls\\generations\\*", bare_state);
    if (written <= 0 || (size_t)written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
        return;
    }
    WIN32_FIND_DATAW entry;
    HANDLE search = FindFirstFileW(pattern, &entry);
    if (search == INVALID_HANDLE_VALUE) {
        return;
    }
    static const wchar_t *const backings[] = {L"codebase-memory-mcp.exe",
                                              L"codebase-memory-mcp.payload.exe"};
    unsigned int tombstone_index = 0U;
    do {
        if (wcscmp(entry.cFileName, L".") == 0 || wcscmp(entry.cFileName, L"..") == 0 ||
            (entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0) {
            continue;
        }
        for (size_t index = 0U; index < sizeof(backings) / sizeof(backings[0]); index++) {
            wchar_t backing_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
            int backing_written = swprintf(backing_path, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                           L"\\\\?\\%ls\\generations\\%ls\\%ls", bare_state,
                                           entry.cFileName, backings[index]);
            if (backing_written <= 0 || (size_t)backing_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
                continue;
            }
            HANDLE handle = CreateFileW(backing_path, DELETE,
                                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                        NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
            if (handle == INVALID_HANDLE_VALUE) {
                continue; /* absent (e.g. no payload backing) or already relocated */
            }
            wchar_t tombstone[CBM_WINDOWS_LAUNCHER_PATH_CAP];
            int tombstone_written = swprintf(
                tombstone, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"\\\\?\\%ls\\activation-%lu-%u.retired",
                install_directory, (unsigned long)GetCurrentProcessId(), tombstone_index++);
            if (tombstone_written > 0 &&
                (size_t)tombstone_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
                windows_posix_rename_handle_no_replace(handle, tombstone)) {
                windows_backing_relocation_log_record(log, tombstone, backing_path);
            }
            (void)CloseHandle(handle);
        }
    } while (FindNextFileW(search, &entry));
    (void)FindClose(search);
}

bool cbm_windows_launcher_uninstall_commit(const wchar_t *canonical_launcher_path,
                                           const char payload_sha256[65], char *error,
                                           size_t error_size) {
    if (error && error_size > 0U)
        error[0] = '\0';
    cbm_windows_current_v1_t current;
    wchar_t backing[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    char backing_error[128] = {0};
    if (!canonical_launcher_path || !lowercase_sha256_valid(payload_sha256) ||
        windows_current_v1_read(canonical_launcher_path, &current) != 1 ||
        strcmp(current.payload_sha256, payload_sha256) != 0 ||
        !cbm_windows_managed_launcher_backing(canonical_launcher_path, backing,
                                              CBM_WINDOWS_LAUNCHER_PATH_CAP, backing_error,
                                              sizeof(backing_error))) {
        launcher_error(error, error_size,
                       "managed launcher/current/backing validation failed before uninstall");
        return false;
    }

    wchar_t install_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t state_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t retired_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int state_written = windows_parent_path(canonical_launcher_path, install_directory,
                                            CBM_WINDOWS_LAUNCHER_PATH_CAP)
                            ? swprintf(state_directory, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\.cbm",
                                       install_directory)
                            : -1;
    if (state_written <= 0 || (size_t)state_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP ||
        !cbm_windows_retired_state_path(canonical_launcher_path, payload_sha256,
                                        (uint32_t)GetCurrentProcessId(), retired_directory,
                                        CBM_WINDOWS_LAUNCHER_PATH_CAP)) {
        launcher_error(error, error_size, "managed retired-state path could not be derived");
        return false;
    }
    /* Normalize both paths to the bare Win32 form. FileRenameInfoEx accepts
     * only a bare target (the \\?\\ prefix is rejected NT-side), so the
     * retirement rename lands the tree at the bare path; every secure open and
     * restore below must resolve the same bare path, not the \\?\\ form the
     * canonical launcher path was canonicalized into. These paths are always
     * within the legacy limit. */
    if (wcsncmp(state_directory, L"\\\\?\\", 4) == 0) {
        (void)wmemmove(state_directory, state_directory + 4, wcslen(state_directory + 4) + 1U);
    }
    if (wcsncmp(retired_directory, L"\\\\?\\", 4) == 0) {
        (void)wmemmove(retired_directory, retired_directory + 4,
                       wcslen(retired_directory + 4) + 1U);
    }
    DWORD retired_attributes = GetFileAttributesW(retired_directory);
    DWORD retired_error =
        retired_attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
    if (retired_attributes != INVALID_FILE_ATTRIBUTES ||
        (retired_error != ERROR_FILE_NOT_FOUND && retired_error != ERROR_PATH_NOT_FOUND)) {
        launcher_error(error, error_size,
                       "unique retired-state destination already exists or is inaccessible");
        return false;
    }

    HANDLE original_backing = windows_open_regular_no_reparse_links(backing, GENERIC_READ, 2U);
    BY_HANDLE_FILE_INFORMATION original_backing_information;
    if (!windows_file_identity_links(original_backing, &original_backing_information, 2U)) {
        if (original_backing != INVALID_HANDLE_VALUE)
            (void)CloseHandle(original_backing);
        launcher_error(error, error_size,
                       "managed launcher backing changed before state retirement");
        return false;
    }
    HANDLE state = windows_open_directory_secure_access(state_directory, DELETE);
    bool rename_succeeded = false;
    /* Records every backing an unpin relocation moves aside, so a failed
     * uninstall can reverse the moves and hand back a .cbm with its generation
     * backings intact. Empty (and its restore/free no-ops) when the rename never
     * needed to unpin. */
    windows_backing_relocation_log_t reloc_log = {0};
    if (state != INVALID_HANDLE_VALUE) {
        /* Retire the state tree by renaming it aside. Two transient forces can
         * veto the rename; both are absorbed inside one bounded budget:
         *   1. When the running managed launcher maps its generation backings,
         *      those hardlinks pin the tree against rename (ACCESS_DENIED). Drop
         *      that pin ONCE, only on the first failure, by relocating the
         *      backings to activation-*.retired tombstones (a mapped image can be
         *      renamed but not deleted; the inode survives via the canonical
         *      link). Relocating only when the rename actually needs it keeps a
         *      recoverable, unpinned tree undisturbed until the commit is
         *      certain — a FAILED uninstall below must restore .cbm with its
         *      generation backings intact.
         *   2. First-touch antivirus scanning transiently vetoes a provably valid
         *      target as ERROR_INVALID_NAME; the loop keeps retrying. */
        DWORD settled_total_ms = 0U;
        bool backings_released = false;
        for (DWORD settle_ms = 0U;; settled_total_ms += settle_ms) {
            rename_succeeded = windows_posix_rename_handle_no_replace(state, retired_directory);
            if (rename_succeeded || settled_total_ms >= 30000U) {
                break;
            }
            if (!backings_released) {
                windows_relocate_generation_backings(state_directory, &reloc_log);
                backings_released = true;
            }
            settle_ms = settle_ms == 0U ? 200U : (settle_ms < 1600U ? settle_ms * 2U : settle_ms);
            Sleep(settle_ms);
        }
    }
    DWORD state_attributes = rename_succeeded ? GetFileAttributesW(state_directory) : 0U;
    DWORD state_error = rename_succeeded && state_attributes == INVALID_FILE_ATTRIBUTES
                            ? GetLastError()
                            : ERROR_SUCCESS;
    HANDLE retired_check =
        rename_succeeded ? windows_open_directory_secure(retired_directory) : INVALID_HANDLE_VALUE;
    bool retired = rename_succeeded && state_attributes == INVALID_FILE_ATTRIBUTES &&
                   (state_error == ERROR_FILE_NOT_FOUND || state_error == ERROR_PATH_NOT_FOUND) &&
                   retired_check != INVALID_HANDLE_VALUE;
    if (retired_check != INVALID_HANDLE_VALUE)
        (void)CloseHandle(retired_check);
    if (!retired) {
        bool restored =
            rename_succeeded && windows_posix_rename_handle_no_replace(state, state_directory);
        /* .cbm is back at its canonical path when the rename never moved it or was
         * moved back — reverse any unpin relocation so its generation backings
         * return. If the tree is stuck aside (restored == false), the tombstones
         * stay for manual recovery alongside it. */
        if (!rename_succeeded || restored) {
            windows_restore_relocated_backings(&reloc_log);
        }
        windows_backing_relocation_log_free(&reloc_log);
        if (state != INVALID_HANDLE_VALUE)
            (void)CloseHandle(state);
        (void)CloseHandle(original_backing);
        launcher_error(error, error_size,
                       restored ? "managed state retirement validation failed and was restored"
                                : "managed state could not be retired atomically; inspect the "
                                  "retired state directory before retrying");
        return false;
    }

    /* The canonical is still the exact two-link managed file — its second link is
     * the generation backing inside the retired tree, or the activation-*.retired
     * tombstone it was relocated to if the tree had to be unpinned — so its
     * unlink follows the proven two-link transaction and the surviving link keeps
     * the mapped inode alive until the process exits. */
    char unlink_error[256] = {0};
    bool unlinked = cbm_windows_launcher_remove_posix(canonical_launcher_path, unlink_error,
                                                      sizeof(unlink_error));
    if (!unlinked) {
        HANDLE canonical =
            windows_open_regular_no_reparse_links(canonical_launcher_path, GENERIC_READ, 2U);
        BY_HANDLE_FILE_INFORMATION canonical_information;
        bool original_canonical_survived =
            windows_file_identity_links(canonical, &canonical_information, 2U) &&
            windows_same_identity(&canonical_information, &original_backing_information);
        if (canonical != INVALID_HANDLE_VALUE)
            (void)CloseHandle(canonical);
        bool restored = original_canonical_survived &&
                        windows_posix_rename_handle_no_replace(state, state_directory);
        (void)CloseHandle(state);
        (void)CloseHandle(original_backing);
        if (restored) {
            /* .cbm is back — reverse any unpin relocation so the restored tree
             * keeps its generation backings. */
            windows_restore_relocated_backings(&reloc_log);
            if (error && error_size > 0U) {
                (void)snprintf(error, error_size,
                               "canonical unlink failed; retired state was restored: %s",
                               unlink_error[0] ? unlink_error : "transaction failed");
            }
        } else {
            launcher_error(error, error_size,
                           "canonical unlink failed after state retirement; retired state was kept "
                           "for manual recovery");
        }
        windows_backing_relocation_log_free(&reloc_log);
        return false;
    }
    (void)CloseHandle(state);
    (void)CloseHandle(original_backing);
    /* Commit succeeded. Relocate any generation backings still inside the retired
     * tree to activation-*.retired tombstones beside the install so the tree is
     * shallow enough for the detached cleanup's non-long-path-aware rd — a 64-hex
     * generation path exceeds MAX_PATH. Deferred to the commit path on purpose: a
     * failed uninstall restores .cbm with its backings intact and recoverable. */
    windows_relocate_generation_backings(retired_directory, NULL);
    windows_backing_relocation_log_free(&reloc_log);
    return true;
}

static bool windows_generation_directory_path(
    const wchar_t *canonical_launcher_path, const char payload_sha256[65],
    wchar_t directory_out[CBM_WINDOWS_LAUNCHER_PATH_CAP]) {
    wchar_t payload[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    return cbm_windows_generation_payload_path(canonical_launcher_path, payload_sha256, payload,
                                               CBM_WINDOWS_LAUNCHER_PATH_CAP) &&
           windows_parent_path(payload, directory_out, CBM_WINDOWS_LAUNCHER_PATH_CAP);
}

bool cbm_windows_launcher_file_secure(const wchar_t *launcher_path, char *error,
                                      size_t error_size) {
    if (error && error_size > 0U)
        error[0] = '\0';
    HANDLE launcher = launcher_path
                          ? windows_open_regular_no_reparse_links(launcher_path, GENERIC_READ, 1U)
                          : INVALID_HANDLE_VALUE;
    if (launcher == INVALID_HANDLE_VALUE) {
        launcher_error(error, error_size, "launcher path, owner, or access policy is unsafe");
        return false;
    }
    (void)CloseHandle(launcher);
    return true;
}

static bool windows_generation_name_valid(const wchar_t *name) {
    if (!name || wcslen(name) != 64U)
        return false;
    for (size_t index = 0U; index < 64U; index++) {
        wchar_t value = name[index];
        if (!((value >= L'0' && value <= L'9') || (value >= L'a' && value <= L'f'))) {
            return false;
        }
    }
    return true;
}

static bool windows_generation_layout_exact(const wchar_t *generation_directory,
                                            DWORD expected_launcher_links,
                                            BY_HANDLE_FILE_INFORMATION *launcher_information_out) {
    HANDLE directory = windows_open_directory_secure(generation_directory);
    if (directory == INVALID_HANDLE_VALUE) {
        return false;
    }
    (void)CloseHandle(directory);

    wchar_t pattern[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int pattern_written =
        swprintf(pattern, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\*", generation_directory);
    if (pattern_written <= 0 || (size_t)pattern_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
        return false;
    }
    WIN32_FIND_DATAW entry;
    HANDLE search = FindFirstFileW(pattern, &entry);
    if (search == INVALID_HANDLE_VALUE) {
        return false;
    }
    bool exact = true;
    size_t payload_entries = 0U;
    size_t launcher_entries = 0U;
    do {
        if (wcscmp(entry.cFileName, L".") == 0 || wcscmp(entry.cFileName, L"..") == 0) {
            continue;
        }
        bool regular = (entry.dwFileAttributes &
                        (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0;
        if (regular && wcscmp(entry.cFileName, L"codebase-memory-mcp.payload.exe") == 0) {
            payload_entries++;
        } else if (regular && wcscmp(entry.cFileName, L"codebase-memory-mcp.exe") == 0) {
            launcher_entries++;
        } else {
            exact = false;
            break;
        }
    } while (FindNextFileW(search, &entry));
    DWORD find_error = GetLastError();
    (void)FindClose(search);
    if (!exact || find_error != ERROR_NO_MORE_FILES || payload_entries != 1U ||
        launcher_entries != 1U) {
        return false;
    }

    wchar_t payload_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t launcher_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int payload_written = swprintf(payload_path, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                   L"%ls\\codebase-memory-mcp.payload.exe", generation_directory);
    int launcher_written = swprintf(launcher_path, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                    L"%ls\\codebase-memory-mcp.exe", generation_directory);
    if (payload_written <= 0 || launcher_written <= 0 ||
        (size_t)payload_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP ||
        (size_t)launcher_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
        return false;
    }
    HANDLE payload = windows_open_regular_no_reparse(payload_path, GENERIC_READ);
    HANDLE launcher =
        windows_open_regular_no_reparse_links(launcher_path, GENERIC_READ, expected_launcher_links);
    BY_HANDLE_FILE_INFORMATION payload_information;
    BY_HANDLE_FILE_INFORMATION launcher_information;
    bool valid =
        windows_file_identity(payload, &payload_information) &&
        windows_file_identity_links(launcher, &launcher_information, expected_launcher_links);
    if (valid && launcher_information_out) {
        *launcher_information_out = launcher_information;
    }
    if (payload != INVALID_HANDLE_VALUE)
        (void)CloseHandle(payload);
    if (launcher != INVALID_HANDLE_VALUE)
        (void)CloseHandle(launcher);
    return valid;
}

bool cbm_windows_managed_launcher_backing(const wchar_t *canonical_launcher_path,
                                          wchar_t *backing_path_out, size_t backing_path_capacity,
                                          char *error, size_t error_size) {
    if (backing_path_out && backing_path_capacity > 0U) {
        backing_path_out[0] = L'\0';
    }
    if (error && error_size > 0U) {
        error[0] = '\0';
    }
    if (!canonical_launcher_path || !backing_path_out || backing_path_capacity == 0U) {
        launcher_error(error, error_size, "invalid managed launcher validation request");
        return false;
    }
    HANDLE canonical =
        windows_open_regular_no_reparse_links(canonical_launcher_path, GENERIC_READ, 2U);
    BY_HANDLE_FILE_INFORMATION canonical_information;
    if (!windows_file_identity_links(canonical, &canonical_information, 2U)) {
        if (canonical != INVALID_HANDLE_VALUE)
            (void)CloseHandle(canonical);
        launcher_error(error, error_size, "canonical launcher is not an exact two-link file");
        return false;
    }

    wchar_t install_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t generations_root[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int root_written = windows_parent_path(canonical_launcher_path, install_directory,
                                           CBM_WINDOWS_LAUNCHER_PATH_CAP)
                           ? swprintf(generations_root, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                      L"%ls\\.cbm\\generations", install_directory)
                           : -1;
    HANDLE root = root_written > 0 && (size_t)root_written < CBM_WINDOWS_LAUNCHER_PATH_CAP
                      ? windows_open_directory_secure(generations_root)
                      : INVALID_HANDLE_VALUE;
    if (root == INVALID_HANDLE_VALUE) {
        (void)CloseHandle(canonical);
        launcher_error(error, error_size, "managed generation root is missing or unsafe");
        return false;
    }
    (void)CloseHandle(root);

    wchar_t pattern[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int pattern_written =
        swprintf(pattern, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\*", generations_root);
    WIN32_FIND_DATAW entry;
    HANDLE search = pattern_written > 0 && (size_t)pattern_written < CBM_WINDOWS_LAUNCHER_PATH_CAP
                        ? FindFirstFileW(pattern, &entry)
                        : INVALID_HANDLE_VALUE;
    if (search == INVALID_HANDLE_VALUE) {
        (void)CloseHandle(canonical);
        launcher_error(error, error_size, "managed generation root could not be enumerated");
        return false;
    }

    size_t matches = 0U;
    wchar_t match[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    match[0] = L'\0';
    do {
        if (!windows_generation_name_valid(entry.cFileName) ||
            (entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0 ||
            (entry.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
            continue;
        }
        wchar_t candidate[CBM_WINDOWS_LAUNCHER_PATH_CAP];
        int candidate_written =
            swprintf(candidate, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\%ls\\codebase-memory-mcp.exe",
                     generations_root, entry.cFileName);
        if (candidate_written <= 0 || (size_t)candidate_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
            continue;
        }
        HANDLE backing = windows_open_regular_no_reparse_links(candidate, GENERIC_READ, 2U);
        BY_HANDLE_FILE_INFORMATION backing_information;
        bool same = windows_file_identity_links(backing, &backing_information, 2U) &&
                    windows_same_identity(&canonical_information, &backing_information);
        if (backing != INVALID_HANDLE_VALUE)
            (void)CloseHandle(backing);
        if (!same) {
            continue;
        }
        size_t candidate_length = wcslen(candidate);
        if (++matches != 1U || candidate_length + 1U > backing_path_capacity) {
            continue;
        }
        memcpy(match, candidate, (candidate_length + 1U) * sizeof(*match));
    } while (FindNextFileW(search, &entry));
    DWORD find_error = GetLastError();
    (void)FindClose(search);
    (void)CloseHandle(canonical);

    wchar_t generation_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    BY_HANDLE_FILE_INFORMATION backing_information;
    bool valid = find_error == ERROR_NO_MORE_FILES && matches == 1U && match[0] != L'\0' &&
                 windows_parent_path(match, generation_directory, CBM_WINDOWS_LAUNCHER_PATH_CAP) &&
                 windows_generation_layout_exact(generation_directory, 2U, &backing_information) &&
                 windows_same_identity(&canonical_information, &backing_information);
    if (!valid) {
        launcher_error(error, error_size,
                       "canonical launcher has no unique exact generation backing");
        return false;
    }
    size_t match_length = wcslen(match);
    memcpy(backing_path_out, match, (match_length + 1U) * sizeof(*backing_path_out));
    return true;
}

typedef enum {
    WINDOWS_GENERATION_REMOVE_INVALID = 0,
    WINDOWS_GENERATION_REMOVE_REMOVED = 1,
    WINDOWS_GENERATION_REMOVE_BUSY = 2,
} windows_generation_remove_result_t;

static windows_generation_remove_result_t windows_remove_generation_directory(
    const wchar_t *generation_directory) {
    DWORD attributes = GetFileAttributesW(generation_directory);
    if (attributes == INVALID_FILE_ATTRIBUTES) {
        DWORD missing_error = GetLastError();
        return missing_error == ERROR_FILE_NOT_FOUND || missing_error == ERROR_PATH_NOT_FOUND
                   ? WINDOWS_GENERATION_REMOVE_REMOVED
                   : WINDOWS_GENERATION_REMOVE_INVALID;
    }
    if (!windows_generation_layout_exact(generation_directory, 1U, NULL))
        return WINDOWS_GENERATION_REMOVE_INVALID;

    wchar_t payload_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t launcher_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int payload_written = swprintf(payload_path, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                   L"%ls\\codebase-memory-mcp.payload.exe", generation_directory);
    int launcher_written = swprintf(launcher_path, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                    L"%ls\\codebase-memory-mcp.exe", generation_directory);
    if (payload_written <= 0 || launcher_written <= 0 ||
        (size_t)payload_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP ||
        (size_t)launcher_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
        return WINDOWS_GENERATION_REMOVE_INVALID;
    }

    /* Acquire DELETE handles for both exact-one-link files before mutating
     * either. Their symmetric share checks prevent a new deny-delete handle
     * from racing between the two dispositions. A live old launcher image
     * predictably refuses the first disposition, leaving the exact pair
     * untouched for a later prune. */
    HANDLE launcher = windows_open_regular_no_reparse(launcher_path, DELETE);
    DWORD launcher_open_error = launcher == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
    HANDLE payload = windows_open_regular_no_reparse(payload_path, DELETE);
    DWORD payload_open_error = payload == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
    if (launcher == INVALID_HANDLE_VALUE || payload == INVALID_HANDLE_VALUE) {
        if (launcher != INVALID_HANDLE_VALUE)
            (void)CloseHandle(launcher);
        if (payload != INVALID_HANDLE_VALUE)
            (void)CloseHandle(payload);
        DWORD open_error =
            launcher == INVALID_HANDLE_VALUE ? launcher_open_error : payload_open_error;
        return open_error == ERROR_ACCESS_DENIED || open_error == ERROR_SHARING_VIOLATION ||
                       open_error == ERROR_LOCK_VIOLATION
                   ? WINDOWS_GENERATION_REMOVE_BUSY
                   : WINDOWS_GENERATION_REMOVE_INVALID;
    }
    bool launcher_removed = windows_posix_remove_handle(launcher);
    DWORD launcher_error_code = launcher_removed ? ERROR_SUCCESS : GetLastError();
    bool payload_removed = launcher_removed && windows_posix_remove_handle(payload);
    (void)CloseHandle(launcher);
    (void)CloseHandle(payload);
    if (!launcher_removed) {
        return launcher_error_code == ERROR_ACCESS_DENIED ||
                       launcher_error_code == ERROR_SHARING_VIOLATION ||
                       launcher_error_code == ERROR_LOCK_VIOLATION
                   ? WINDOWS_GENERATION_REMOVE_BUSY
                   : WINDOWS_GENERATION_REMOVE_INVALID;
    }
    return payload_removed && RemoveDirectoryW(generation_directory) != 0
               ? WINDOWS_GENERATION_REMOVE_REMOVED
               : WINDOWS_GENERATION_REMOVE_INVALID;
}

bool cbm_windows_generation_rollback_if_unreferenced(const wchar_t *canonical_launcher_path,
                                                     const char payload_sha256[65],
                                                     bool created_by_activation, char *error,
                                                     size_t error_size) {
    if (error && error_size > 0U)
        error[0] = '\0';
    if (!created_by_activation)
        return true;
    if (!canonical_launcher_path || !lowercase_sha256_valid(payload_sha256)) {
        launcher_error(error, error_size, "invalid generation rollback request");
        return false;
    }
    cbm_windows_current_v1_t current;
    int current_status = windows_current_v1_read(canonical_launcher_path, &current);
    if (current_status < 0) {
        launcher_error(error, error_size, "current-v1 is unsafe during generation rollback");
        return false;
    }
    if (current_status == 1 && strcmp(current.payload_sha256, payload_sha256) == 0) {
        return true;
    }
    wchar_t generation[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t canonical_backing[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    char backing_error[128] = {0};
    DWORD canonical_attributes = GetFileAttributesW(canonical_launcher_path);
    DWORD canonical_error =
        canonical_attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
    bool canonical_absent =
        canonical_attributes == INVALID_FILE_ATTRIBUTES &&
        (canonical_error == ERROR_FILE_NOT_FOUND || canonical_error == ERROR_PATH_NOT_FOUND);
    bool backing_valid = canonical_absent ||
                         cbm_windows_managed_launcher_backing(
                             canonical_launcher_path, canonical_backing,
                             CBM_WINDOWS_LAUNCHER_PATH_CAP, backing_error, sizeof(backing_error));
    wchar_t backing_generation[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    bool is_backing_generation =
        !canonical_absent && backing_valid &&
        windows_parent_path(canonical_backing, backing_generation, CBM_WINDOWS_LAUNCHER_PATH_CAP) &&
        windows_generation_directory_path(canonical_launcher_path, payload_sha256, generation) &&
        _wcsicmp(generation, backing_generation) == 0;
    if (is_backing_generation) {
        return true;
    }
    if (!backing_valid ||
        !windows_generation_directory_path(canonical_launcher_path, payload_sha256, generation) ||
        windows_remove_generation_directory(generation) != WINDOWS_GENERATION_REMOVE_REMOVED) {
        launcher_error(error, error_size,
                       "new unreferenced generation could not be rolled back safely");
        return false;
    }
    return true;
}

bool cbm_windows_generations_prune(const wchar_t *canonical_launcher_path, size_t *removed_out,
                                   char *error, size_t error_size) {
    if (removed_out)
        *removed_out = 0U;
    if (error && error_size > 0U)
        error[0] = '\0';
    cbm_windows_current_v1_t current;
    if (!canonical_launcher_path ||
        windows_current_v1_read(canonical_launcher_path, &current) != 1) {
        launcher_error(error, error_size, "current-v1 is unsafe during generation pruning");
        return false;
    }
    wchar_t current_generation[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t generations_root[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    if (!windows_generation_directory_path(canonical_launcher_path, current.payload_sha256,
                                           current_generation) ||
        !windows_parent_path(current_generation, generations_root, CBM_WINDOWS_LAUNCHER_PATH_CAP)) {
        launcher_error(error, error_size, "generation root path could not be resolved");
        return false;
    }
    wchar_t canonical_backing[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t backing_generation[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    char backing_error[128] = {0};
    if (!cbm_windows_managed_launcher_backing(canonical_launcher_path, canonical_backing,
                                              CBM_WINDOWS_LAUNCHER_PATH_CAP, backing_error,
                                              sizeof(backing_error)) ||
        !windows_parent_path(canonical_backing, backing_generation,
                             CBM_WINDOWS_LAUNCHER_PATH_CAP)) {
        launcher_error(error, error_size, "canonical launcher backing is missing during pruning");
        return false;
    }
    bool current_is_backing = _wcsicmp(current_generation, backing_generation) == 0;
    if (!windows_generation_layout_exact(current_generation, current_is_backing ? 2U : 1U, NULL)) {
        launcher_error(error, error_size, "current generation layout is incomplete or unsafe");
        return false;
    }
    HANDLE root = windows_open_directory_secure(generations_root);
    if (root == INVALID_HANDLE_VALUE) {
        launcher_error(error, error_size, "generation root is missing or unsafe");
        return false;
    }
    (void)CloseHandle(root);

    wchar_t pattern[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int pattern_written =
        swprintf(pattern, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\*", generations_root);
    if (pattern_written <= 0 || (size_t)pattern_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
        launcher_error(error, error_size, "generation enumeration path is too long");
        return false;
    }
    WIN32_FIND_DATAW entry;
    HANDLE search = FindFirstFileW(pattern, &entry);
    if (search == INVALID_HANDLE_VALUE) {
        launcher_error(error, error_size, "generation root could not be enumerated");
        return false;
    }
    wchar_t current_name[65];
    for (size_t index = 0U; index < 64U; index++) {
        current_name[index] = (wchar_t)(unsigned char)current.payload_sha256[index];
    }
    current_name[64] = L'\0';
    const wchar_t *backing_name = wcsrchr(backing_generation, L'\\');
    const wchar_t *backing_slash = wcsrchr(backing_generation, L'/');
    if (!backing_name || (backing_slash && backing_slash > backing_name)) {
        backing_name = backing_slash;
    }
    backing_name = backing_name ? backing_name + 1 : backing_generation;
    bool ok = true;
    size_t removed = 0U;
    do {
        if (wcscmp(entry.cFileName, L".") == 0 || wcscmp(entry.cFileName, L"..") == 0) {
            continue;
        }
        if (_wcsicmp(entry.cFileName, current_name) == 0) {
            bool canonical_current = wcscmp(entry.cFileName, current_name) == 0 &&
                                     (entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                                     (entry.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
            if (!canonical_current)
                ok = false;
            continue;
        }
        if (_wcsicmp(entry.cFileName, backing_name) == 0) {
            bool backing_entry_valid = wcscmp(entry.cFileName, backing_name) == 0 &&
                                       (entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                                       (entry.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
            if (!backing_entry_valid)
                ok = false;
            continue;
        }
        if (!windows_generation_name_valid(entry.cFileName) ||
            (entry.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0 ||
            (entry.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
            ok = false;
            continue;
        }
        wchar_t generation[CBM_WINDOWS_LAUNCHER_PATH_CAP];
        int generation_written = swprintf(generation, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\%ls",
                                          generations_root, entry.cFileName);
        windows_generation_remove_result_t removal =
            generation_written > 0 && (size_t)generation_written < CBM_WINDOWS_LAUNCHER_PATH_CAP
                ? windows_remove_generation_directory(generation)
                : WINDOWS_GENERATION_REMOVE_INVALID;
        if (removal == WINDOWS_GENERATION_REMOVE_BUSY) {
            /* A launcher mapped by an older live session keeps its sole
             * generation link until that process exits.  Retaining the exact
             * layout is safe; the next activation prunes it. */
            continue;
        }
        if (removal != WINDOWS_GENERATION_REMOVE_REMOVED) {
            ok = false;
            continue;
        }
        removed++;
    } while (FindNextFileW(search, &entry));
    DWORD find_error = GetLastError();
    (void)FindClose(search);
    ok = ok && find_error == ERROR_NO_MORE_FILES;
    if (removed_out)
        *removed_out = removed;
    if (!ok) {
        launcher_error(error, error_size,
                       "one or more non-current generations were unsafe or could not be pruned");
    }
    return ok;
}

bool cbm_windows_current_v1_write_atomic(const wchar_t *canonical_launcher_path,
                                         const cbm_windows_current_v1_t *state, char *error,
                                         size_t error_size) {
    if (error && error_size > 0U)
        error[0] = '\0';
    uint8_t record[CBM_WINDOWS_CURRENT_V1_SIZE];
    wchar_t directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    HANDLE canonical =
        canonical_launcher_path
            ? windows_open_regular_no_reparse_links(canonical_launcher_path, GENERIC_READ, 2U)
            : INVALID_HANDLE_VALUE;
    if (!canonical_launcher_path || canonical == INVALID_HANDLE_VALUE ||
        !cbm_windows_current_v1_encode(state, record) ||
        !windows_parent_path(canonical_launcher_path, directory, CBM_WINDOWS_LAUNCHER_PATH_CAP)) {
        if (canonical != INVALID_HANDLE_VALUE)
            (void)CloseHandle(canonical);
        launcher_error(error, error_size, "invalid current-v1 write request");
        return false;
    }
    (void)CloseHandle(canonical);
    wchar_t state_directory[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    wchar_t current[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int state_written =
        swprintf(state_directory, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\.cbm", directory);
    int current_written =
        swprintf(current, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"%ls\\current-v1", state_directory);
    if (state_written <= 0 || current_written <= 0 ||
        (size_t)state_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP ||
        (size_t)current_written >= CBM_WINDOWS_LAUNCHER_PATH_CAP ||
        (!windows_create_directory_private(state_directory) &&
         GetLastError() != ERROR_ALREADY_EXISTS)) {
        launcher_error(error, error_size, "could not create launcher state directory");
        return false;
    }
    HANDLE state_handle = windows_open_directory_secure(state_directory);
    DWORD current_attributes = GetFileAttributesW(current);
    DWORD current_error =
        current_attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
    HANDLE existing = current_attributes == INVALID_FILE_ATTRIBUTES
                          ? INVALID_HANDLE_VALUE
                          : windows_open_regular_no_reparse(current, GENERIC_READ);
    bool current_absent =
        current_attributes == INVALID_FILE_ATTRIBUTES &&
        (current_error == ERROR_FILE_NOT_FOUND || current_error == ERROR_PATH_NOT_FOUND);
    if (state_handle == INVALID_HANDLE_VALUE ||
        (!current_absent && existing == INVALID_HANDLE_VALUE)) {
        if (state_handle != INVALID_HANDLE_VALUE)
            (void)CloseHandle(state_handle);
        if (existing != INVALID_HANDLE_VALUE)
            (void)CloseHandle(existing);
        launcher_error(error, error_size, "launcher state directory or current-v1 is unsafe");
        return false;
    }
    (void)CloseHandle(state_handle);
    if (existing != INVALID_HANDLE_VALUE)
        (void)CloseHandle(existing);
    wchar_t stage[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    if (!windows_unique_sibling(current, L"current", stage, CBM_WINDOWS_LAUNCHER_PATH_CAP)) {
        launcher_error(error, error_size, "current-v1 stage path is too long");
        return false;
    }
    HANDLE file = CreateFileW(
        stage,
        GENERIC_WRITE | DELETE | FILE_READ_ATTRIBUTES | READ_CONTROL | WRITE_OWNER | WRITE_DAC,
        FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, CREATE_NEW,
        FILE_ATTRIBUTE_HIDDEN | FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH, NULL);
    DWORD written = 0U;
    BY_HANDLE_FILE_INFORMATION information;
    /* Stamp before the owner check: under an Administrators-default-owner
     * policy (GitHub runners, Windows Server) a NULL-security CREATE_NEW file
     * is Administrators-owned and the exact-owner validation below would
     * refuse our own staged record. */
    bool ok = file != INVALID_HANDLE_VALUE && windows_stamp_current_owner_private(file) &&
              WriteFile(file, record, sizeof(record), &written, NULL) != 0 &&
              written == sizeof(record) && FlushFileBuffers(file) != 0 &&
              windows_file_identity(file, &information) && windows_owner_is_current(file) &&
              windows_acl_secure(file) && windows_posix_rename_handle(file, current);
    if (file != INVALID_HANDLE_VALUE)
        (void)CloseHandle(file);
    if (!ok) {
        (void)DeleteFileW(stage);
        launcher_error(error, error_size, "atomic current-v1 publication is unsupported or failed");
    }
    return ok;
}

#define CBM_RELEASE_DESCRIPTOR_ARG L"__cbm_windows_release_descriptor_v1"

bool cbm_windows_release_descriptor_probe(const wchar_t *launcher_candidate,
                                          cbm_windows_release_descriptor_v1_t *descriptor_out,
                                          char *error, size_t error_size) {
    if (descriptor_out) {
        memset(descriptor_out, 0, sizeof(*descriptor_out));
    }
    if (error && error_size > 0U)
        error[0] = '\0';
    HANDLE candidate = launcher_candidate
                           ? windows_open_regular_no_reparse(launcher_candidate, GENERIC_READ)
                           : INVALID_HANDLE_VALUE;
    if (!descriptor_out || candidate == INVALID_HANDLE_VALUE) {
        if (candidate != INVALID_HANDLE_VALUE)
            (void)CloseHandle(candidate);
        launcher_error(error, error_size, "launcher descriptor candidate is unsafe");
        return false;
    }
    (void)CloseHandle(candidate);

    SECURITY_ATTRIBUTES security = {
        .nLength = sizeof(security),
        .lpSecurityDescriptor = NULL,
        .bInheritHandle = TRUE,
    };
    HANDLE read_pipe = NULL;
    HANDLE write_pipe = NULL;
    HANDLE null_input = INVALID_HANDLE_VALUE;
    HANDLE null_error = INVALID_HANDLE_VALUE;
    HANDLE job = NULL;
    PROCESS_INFORMATION child;
    memset(&child, 0, sizeof(child));
    bool ready = CreatePipe(&read_pipe, &write_pipe, &security, 4096U) != 0 &&
                 SetHandleInformation(read_pipe, HANDLE_FLAG_INHERIT, 0U) != 0;
    null_input = ready ? CreateFileW(L"NUL", GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                                     &security, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL)
                       : INVALID_HANDLE_VALUE;
    null_error = ready ? CreateFileW(L"NUL", GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                                     &security, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL)
                       : INVALID_HANDLE_VALUE;
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits;
    memset(&limits, 0, sizeof(limits));
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    job = ready ? CreateJobObjectW(NULL, NULL) : NULL;
    ready =
        ready && null_input != INVALID_HANDLE_VALUE && null_error != INVALID_HANDLE_VALUE && job &&
        SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits, sizeof(limits)) !=
            0;

    HANDLE inherited[3] = {write_pipe, null_input, null_error};
    SIZE_T attribute_size = 0U;
    (void)InitializeProcThreadAttributeList(NULL, 2U, 0U, &attribute_size);
    LPPROC_THREAD_ATTRIBUTE_LIST attributes =
        ready && attribute_size ? malloc(attribute_size) : NULL;
    bool initialized =
        attributes && InitializeProcThreadAttributeList(attributes, 2U, 0U, &attribute_size) != 0;
    ready = ready && initialized &&
            UpdateProcThreadAttribute(attributes, 0U, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherited,
                                      sizeof(inherited), NULL, NULL) != 0 &&
            UpdateProcThreadAttribute(attributes, 0U, PROC_THREAD_ATTRIBUTE_JOB_LIST, &job,
                                      sizeof(job), NULL, NULL) != 0;

    wchar_t command[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int command_length = launcher_candidate
                             ? swprintf(command, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"\"%ls\" %ls",
                                        launcher_candidate, CBM_RELEASE_DESCRIPTOR_ARG)
                             : -1;
    STARTUPINFOEXW startup;
    memset(&startup, 0, sizeof(startup));
    startup.StartupInfo.cb = sizeof(startup);
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = null_input;
    startup.StartupInfo.hStdOutput = write_pipe;
    startup.StartupInfo.hStdError = null_error;
    startup.lpAttributeList = attributes;
    bool spawned = ready && command_length > 0 &&
                   (size_t)command_length < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
                   CreateProcessW(launcher_candidate, command, NULL, NULL, TRUE,
                                  CREATE_NO_WINDOW | EXTENDED_STARTUPINFO_PRESENT, NULL, NULL,
                                  &startup.StartupInfo, &child) != 0;
    if (initialized)
        DeleteProcThreadAttributeList(attributes);
    free(attributes);
    if (write_pipe) {
        (void)CloseHandle(write_pipe);
        write_pipe = NULL;
    }
    if (null_input != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(null_input);
        null_input = INVALID_HANDLE_VALUE;
    }
    if (null_error != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(null_error);
        null_error = INVALID_HANDLE_VALUE;
    }

    uint8_t record[CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE + 1U];
    size_t received_total = 0U;
    bool pipe_closed = false;
    uint64_t now = GetTickCount64();
    uint64_t deadline = UINT64_MAX - now < 30000U ? UINT64_MAX : now + 30000U;
    while (spawned && GetTickCount64() < deadline && !pipe_closed) {
        DWORD available = 0U;
        if (!PeekNamedPipe(read_pipe, NULL, 0U, NULL, &available, NULL)) {
            pipe_closed = GetLastError() == ERROR_BROKEN_PIPE;
            break;
        }
        if (available > 0U) {
            DWORD capacity = (DWORD)(sizeof(record) - received_total);
            DWORD request = available < capacity ? available : capacity;
            DWORD amount = 0U;
            if (request == 0U ||
                !ReadFile(read_pipe, record + received_total, request, &amount, NULL) ||
                amount == 0U) {
                break;
            }
            received_total += amount;
            if (received_total == sizeof(record)) {
                break;
            }
            continue;
        }
        Sleep(2U);
    }
    DWORD wait = spawned ? WaitForSingleObject(child.hProcess, 0U) : WAIT_FAILED;
    if (spawned && wait != WAIT_OBJECT_0) {
        (void)TerminateJobObject(job, 1U);
        (void)WaitForSingleObject(child.hProcess, 5000U);
    }
    DWORD exit_code = 1U;
    bool valid =
        spawned && pipe_closed && received_total == CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE &&
        WaitForSingleObject(child.hProcess, 0U) == WAIT_OBJECT_0 &&
        GetExitCodeProcess(child.hProcess, &exit_code) != 0 && exit_code == 0U &&
        cbm_windows_release_descriptor_v1_decode(record, CBM_WINDOWS_RELEASE_DESCRIPTOR_V1_SIZE,
                                                 descriptor_out);
    if (child.hThread)
        (void)CloseHandle(child.hThread);
    if (child.hProcess)
        (void)CloseHandle(child.hProcess);
    if (read_pipe)
        (void)CloseHandle(read_pipe);
    if (job)
        (void)CloseHandle(job);
    if (!valid) {
        memset(descriptor_out, 0, sizeof(*descriptor_out));
        launcher_error(error, error_size,
                       "launcher release descriptor was missing, malformed, or timed out");
    }
    return valid;
}

/* Probe subprocess protocol.  The standalone launcher recognizes this private
 * mode, signals ready, and remains mapped until the release event is set. */
#define CBM_LAUNCHER_PROBE_ARG L"__cbm_launcher_capability_probe_v1"

static bool windows_probe_volume(const wchar_t *directory, char *error, size_t error_size) {
    wchar_t volume[MAX_PATH + 1U];
    wchar_t filesystem[MAX_PATH + 1U];
    if (!GetVolumePathNameW(directory, volume, MAX_PATH + 1U) ||
        GetDriveTypeW(volume) != DRIVE_FIXED ||
        !GetVolumeInformationW(volume, NULL, 0U, NULL, NULL, NULL, filesystem, MAX_PATH + 1U) ||
        _wcsicmp(filesystem, L"NTFS") != 0) {
        launcher_error(error, error_size, "managed launcher requires a local fixed NTFS volume");
        return false;
    }
    return true;
}

static bool windows_probe_spawn(const wchar_t *image, HANDLE ready, HANDLE release,
                                PROCESS_INFORMATION *child) {
    wchar_t command[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int written = swprintf(command, CBM_WINDOWS_LAUNCHER_PATH_CAP, L"\"%ls\" %ls %llx %llx", image,
                           CBM_LAUNCHER_PROBE_ARG, (unsigned long long)(uintptr_t)ready,
                           (unsigned long long)(uintptr_t)release);
    if (written <= 0 || (size_t)written >= CBM_WINDOWS_LAUNCHER_PATH_CAP) {
        return false;
    }
    HANDLE inherited[2] = {ready, release};
    SIZE_T attribute_size = 0U;
    (void)InitializeProcThreadAttributeList(NULL, 1U, 0U, &attribute_size);
    LPPROC_THREAD_ATTRIBUTE_LIST attributes = attribute_size ? malloc(attribute_size) : NULL;
    bool initialized =
        attributes && InitializeProcThreadAttributeList(attributes, 1U, 0U, &attribute_size) != 0;
    bool ready_attributes =
        initialized && UpdateProcThreadAttribute(attributes, 0U, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                                                 inherited, sizeof(inherited), NULL, NULL) != 0;
    STARTUPINFOEXW startup;
    memset(&startup, 0, sizeof(startup));
    memset(child, 0, sizeof(*child));
    startup.StartupInfo.cb = sizeof(startup);
    startup.lpAttributeList = attributes;
    bool spawned =
        ready_attributes && CreateProcessW(image, command, NULL, NULL, TRUE,
                                           CREATE_NO_WINDOW | EXTENDED_STARTUPINFO_PRESENT, NULL,
                                           NULL, &startup.StartupInfo, child) != 0;
    if (initialized)
        DeleteProcThreadAttributeList(attributes);
    free(attributes);
    return spawned;
}

static void windows_probe_child_stop(PROCESS_INFORMATION *child, HANDLE release) {
    if (release)
        (void)SetEvent(release);
    if (child->hProcess) {
        if (WaitForSingleObject(child->hProcess, 5000U) != WAIT_OBJECT_0) {
            (void)TerminateProcess(child->hProcess, 1U);
            (void)WaitForSingleObject(child->hProcess, 5000U);
        }
        (void)CloseHandle(child->hProcess);
    }
    if (child->hThread)
        (void)CloseHandle(child->hThread);
    memset(child, 0, sizeof(*child));
}

typedef struct {
    wchar_t **paths;
    size_t count;
    size_t capacity;
} windows_created_directories_t;

static void windows_created_directories_close(windows_created_directories_t *created,
                                              bool remove_directories) {
    if (!created)
        return;
    for (size_t index = 0U; index < created->count; index++) {
        if (remove_directories)
            (void)RemoveDirectoryW(created->paths[index]);
        free(created->paths[index]);
    }
    free(created->paths);
    memset(created, 0, sizeof(*created));
}

static bool windows_created_directories_push(windows_created_directories_t *created,
                                             const wchar_t *path) {
    if (created->count == created->capacity) {
        size_t next_capacity = created->capacity == 0U ? 8U : created->capacity * 2U;
        if (next_capacity < created->capacity || next_capacity > CBM_WINDOWS_LAUNCHER_PATH_CAP) {
            return false;
        }
        wchar_t **next = realloc(created->paths, next_capacity * sizeof(*next));
        if (!next)
            return false;
        created->paths = next;
        created->capacity = next_capacity;
    }
    size_t length = wcslen(path);
    wchar_t *copy = malloc((length + 1U) * sizeof(*copy));
    if (!copy)
        return false;
    memcpy(copy, path, (length + 1U) * sizeof(*copy));
    created->paths[created->count++] = copy;
    return true;
}

/* Directories this launcher creates must be owner-stamped explicitly: under
 * the Administrators-default-owner policy (GitHub runners, mirrored by the
 * local VM), CreateDirectoryW with a NULL descriptor yields an
 * Administrators-owned directory that this file's own exact-owner
 * validation then rejects. Same pattern as win_mkdtemp_private_create. */
static bool windows_stamp_current_owner_private(HANDLE file) {
    if (file == INVALID_HANDLE_VALUE) {
        return false;
    }
    HANDLE token = NULL;
    TOKEN_USER *user = NULL;
    PACL acl = NULL;
    DWORD needed = 0U;
    bool stamped = false;
    if (OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token) &&
        !GetTokenInformation(token, TokenUser, NULL, 0U, &needed) &&
        GetLastError() == ERROR_INSUFFICIENT_BUFFER && (user = malloc(needed)) != NULL &&
        GetTokenInformation(token, TokenUser, user, needed, &needed) && user->User.Sid &&
        IsValidSid(user->User.Sid)) {
        EXPLICIT_ACCESSW access;
        memset(&access, 0, sizeof(access));
        access.grfAccessPermissions = GENERIC_ALL;
        access.grfAccessMode = SET_ACCESS;
        access.grfInheritance = NO_INHERITANCE;
        access.Trustee.TrusteeForm = TRUSTEE_IS_SID;
        access.Trustee.TrusteeType = TRUSTEE_IS_USER;
        access.Trustee.ptstrName = (LPWSTR)user->User.Sid;
        if (SetEntriesInAclW(1U, &access, NULL, &acl) == ERROR_SUCCESS &&
            SetSecurityInfo(file, SE_FILE_OBJECT,
                            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION |
                                PROTECTED_DACL_SECURITY_INFORMATION,
                            user->User.Sid, NULL, acl, NULL) == ERROR_SUCCESS) {
            stamped = true;
        }
    }
    if (acl) {
        (void)LocalFree(acl);
    }
    free(user);
    if (token) {
        (void)CloseHandle(token);
    }
    return stamped;
}

static bool windows_create_directory_private(const wchar_t *path) {
    bool created = false;
    HANDLE token = NULL;
    TOKEN_USER *user = NULL;
    PACL acl = NULL;
    DWORD needed = 0U;
    if (OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token) &&
        !GetTokenInformation(token, TokenUser, NULL, 0U, &needed) &&
        GetLastError() == ERROR_INSUFFICIENT_BUFFER && (user = malloc(needed)) != NULL &&
        GetTokenInformation(token, TokenUser, user, needed, &needed) && user->User.Sid &&
        IsValidSid(user->User.Sid)) {
        EXPLICIT_ACCESSW access;
        memset(&access, 0, sizeof(access));
        access.grfAccessPermissions = GENERIC_ALL;
        access.grfAccessMode = SET_ACCESS;
        access.grfInheritance = SUB_CONTAINERS_AND_OBJECTS_INHERIT;
        access.Trustee.TrusteeForm = TRUSTEE_IS_SID;
        access.Trustee.TrusteeType = TRUSTEE_IS_USER;
        access.Trustee.ptstrName = (LPWSTR)user->User.Sid;
        SECURITY_DESCRIPTOR descriptor;
        if (SetEntriesInAclW(1U, &access, NULL, &acl) == ERROR_SUCCESS &&
            InitializeSecurityDescriptor(&descriptor, SECURITY_DESCRIPTOR_REVISION) &&
            SetSecurityDescriptorDacl(&descriptor, TRUE, acl, FALSE) &&
            SetSecurityDescriptorOwner(&descriptor, user->User.Sid, FALSE) &&
            SetSecurityDescriptorControl(&descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED)) {
            SECURITY_ATTRIBUTES attributes;
            attributes.nLength = sizeof(attributes);
            attributes.lpSecurityDescriptor = &descriptor;
            attributes.bInheritHandle = FALSE;
            created = CreateDirectoryW(path, &attributes) != 0;
        }
    }
    DWORD create_error = created ? ERROR_SUCCESS : GetLastError();
    if (acl) {
        (void)LocalFree(acl);
    }
    free(user);
    if (token) {
        (void)CloseHandle(token);
    }
    /* Callers distinguish ERROR_ALREADY_EXISTS; keep the creation error alive
     * across the cleanup calls above. */
    SetLastError(create_error);
    return created;
}

static bool windows_prepare_probe_directory(const wchar_t *target,
                                            windows_created_directories_t *created) {
    memset(created, 0, sizeof(*created));
    size_t length = target ? wcslen(target) : 0U;
    /* Managed-install targets arrive in the extended-length form; the walk
     * keeps the prefix on every cumulative open and only the drive-shape
     * check and root guards shift by its four characters. */
    size_t root = 0U;
    if (length >= 4U && target[0] == L'\\' && target[1] == L'\\' && target[2] == L'?' &&
        target[3] == L'\\') {
        root = 4U;
    }
    if (length < root + 3U || length >= CBM_WINDOWS_LAUNCHER_PATH_CAP ||
        target[root + 1U] != L':' || (target[root + 2U] != L'\\' && target[root + 2U] != L'/')) {
        return false;
    }
    wchar_t *cursor = malloc((length + 1U) * sizeof(*cursor));
    if (!cursor)
        return false;
    memcpy(cursor, target, (length + 1U) * sizeof(*cursor));
    for (size_t index = root; index < length; index++) {
        if (cursor[index] == L'/')
            cursor[index] = L'\\';
    }
    while (length > root + 3U && cursor[length - 1U] == L'\\') {
        cursor[--length] = L'\0';
    }
    bool valid = true;
    for (;;) {
        DWORD attributes = GetFileAttributesW(cursor);
        if (attributes != INVALID_FILE_ATTRIBUTES) {
            HANDLE ancestor = CreateFileW(
                cursor, FILE_READ_ATTRIBUTES | READ_CONTROL,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
            BY_HANDLE_FILE_INFORMATION information;
            /* Sibling-directory creation cannot replace an existing path
             * component at any ancestry level, and the descendant chain this
             * probe creates below here uses CreateDirectoryW without
             * OPEN_ALWAYS — a squatted component fails creation instead of
             * being adopted. Standard profile ancestors (and the launcher
             * guard's fixture) legitimately carry cross-account
             * add-subdirectory grants. */
            DWORD mutation = windows_private_mutation_rights() & ~((DWORD)FILE_ADD_SUBDIRECTORY);
            valid = ancestor != INVALID_HANDLE_VALUE && GetFileType(ancestor) == FILE_TYPE_DISK &&
                    GetFileInformationByHandle(ancestor, &information) != 0 &&
                    (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                    (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
                    windows_owner_secure(ancestor, false) &&
                    windows_acl_secure_for_mutation(ancestor, mutation);
            if (ancestor != INVALID_HANDLE_VALUE)
                (void)CloseHandle(ancestor);
            break;
        }
        DWORD error = GetLastError();
        if ((error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND) ||
            !windows_created_directories_push(created, cursor)) {
            valid = false;
            break;
        }
        wchar_t *separator = wcsrchr(cursor + root, L'\\');
        if (!separator || separator <= cursor + root + 2) {
            valid = false;
            break;
        }
        *separator = L'\0';
    }
    for (size_t remaining = created->count; valid && remaining > 0U; remaining--) {
        const wchar_t *directory = created->paths[remaining - 1U];
        if (!windows_create_directory_private(directory)) {
            valid = false;
            break;
        }
        HANDLE handle = windows_open_directory_secure(directory);
        valid = handle != INVALID_HANDLE_VALUE;
        if (handle != INVALID_HANDLE_VALUE)
            (void)CloseHandle(handle);
    }
    free(cursor);
    if (!valid) {
        windows_created_directories_close(created, true);
    }
    return valid;
}

static bool windows_launcher_capability_probe_once(const wchar_t *target_directory,
                                                   const wchar_t *launcher_candidate, char *error,
                                                   size_t error_size) {
    if (error && error_size > 0U)
        error[0] = '\0';
    if (!target_directory || !target_directory[0] || !launcher_candidate ||
        !launcher_candidate[0]) {
        launcher_error(error, error_size, "invalid launcher capability probe request");
        return false;
    }
    wchar_t forced[16];
    if (GetEnvironmentVariableW(L"CBM_TEST_WINDOWS_LAUNCHER_CAPABILITY_PROBE", forced,
                                (DWORD)(sizeof(forced) / sizeof(forced[0]))) > 0U &&
        _wcsicmp(forced, L"fail") == 0) {
        launcher_error(error, error_size, "Windows launcher capability probe was forced to fail");
        return false;
    }
    windows_created_directories_t created;
    if (!windows_prepare_probe_directory(target_directory, &created)) {
        launcher_error(error, error_size, "could not safely prepare capability probe directory");
        return false;
    }
    wchar_t probe_tree_path[CBM_WINDOWS_LAUNCHER_PATH_CAP];
    int tree_written = swprintf(probe_tree_path, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                L"%ls\\.cbm-probe-path-check", target_directory);
    HANDLE target_handle = windows_open_directory_secure(target_directory);
    bool ok = tree_written > 0 && (size_t)tree_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
              windows_path_tree_plain(probe_tree_path) && target_handle != INVALID_HANDLE_VALUE &&
              windows_probe_volume(target_directory, error, error_size);
    if (target_handle != INVALID_HANDLE_VALUE)
        (void)CloseHandle(target_handle);
    HANDLE mutex =
        ok ? CreateMutexW(NULL, FALSE, L"Local\\CBMWindowsLauncherCapabilityProbe-v1") : NULL;
    DWORD mutex_wait = mutex ? WaitForSingleObject(mutex, 30000U) : WAIT_FAILED;
    ok = ok && mutex_wait == WAIT_OBJECT_0;
    unsigned long probe_pid = (unsigned long)GetCurrentProcessId();
    unsigned long long probe_nonce = (unsigned long long)GetTickCount64();
    enum { WINDOWS_PROBE_PATH_COUNT = 12 };
    wchar_t *path_storage =
        calloc(WINDOWS_PROBE_PATH_COUNT * CBM_WINDOWS_LAUNCHER_PATH_CAP, sizeof(*path_storage));
    wchar_t *canonical = path_storage;
    wchar_t *old_generation = path_storage ? path_storage + CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *old_payload = path_storage ? path_storage + 2U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *old_backing = path_storage ? path_storage + 3U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *new_generation =
        path_storage ? path_storage + 4U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *new_payload = path_storage ? path_storage + 5U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *new_backing = path_storage ? path_storage + 6U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *state_directory =
        path_storage ? path_storage + 7U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *retired_directory =
        path_storage ? path_storage + 8U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *mapped_image = path_storage ? path_storage + 9U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *retired_mapped_image =
        path_storage ? path_storage + 10U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    wchar_t *state_mapped_link =
        path_storage ? path_storage + 11U * CBM_WINDOWS_LAUNCHER_PATH_CAP : NULL;
    int canonical_written = path_storage
                                ? swprintf(canonical, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                           L"%ls\\.cbm-launcher-probe-canonical-%lu-%llu.exe",
                                           target_directory, probe_pid, probe_nonce)
                                : -1;
    int old_generation_written = path_storage
                                     ? swprintf(old_generation, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                                L"%ls\\.cbm-launcher-probe-old-generation-%lu-%llu",
                                                target_directory, probe_pid, probe_nonce)
                                     : -1;
    int old_payload_written =
        path_storage ? swprintf(old_payload, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                L"%ls\\codebase-memory-mcp.payload.exe", old_generation)
                     : -1;
    int old_backing_written = path_storage
                                  ? swprintf(old_backing, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                             L"%ls\\codebase-memory-mcp.exe", old_generation)
                                  : -1;
    int new_generation_written = path_storage
                                     ? swprintf(new_generation, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                                L"%ls\\.cbm-launcher-probe-new-generation-%lu-%llu",
                                                target_directory, probe_pid, probe_nonce)
                                     : -1;
    int new_payload_written =
        path_storage ? swprintf(new_payload, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                L"%ls\\codebase-memory-mcp.payload.exe", new_generation)
                     : -1;
    int new_backing_written = path_storage
                                  ? swprintf(new_backing, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                             L"%ls\\codebase-memory-mcp.exe", new_generation)
                                  : -1;
    int state_directory_written = path_storage
                                      ? swprintf(state_directory, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                                 L"%ls\\.cbm-launcher-probe-state-%lu-%llu",
                                                 target_directory, probe_pid, probe_nonce)
                                      : -1;
    int retired_directory_written = path_storage
                                        ? swprintf(retired_directory, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                                   L"%ls\\.cbm-launcher-probe-retired-%lu-%llu",
                                                   target_directory, probe_pid, probe_nonce)
                                        : -1;
    int mapped_image_written = path_storage
                                   ? swprintf(mapped_image, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                              L"%ls\\.cbm-launcher-probe-mapped-%lu-%llu.exe",
                                              target_directory, probe_pid, probe_nonce)
                                   : -1;
    int retired_mapped_image_written =
        path_storage ? swprintf(retired_mapped_image, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                L"%ls\\mapped-launcher.exe", retired_directory)
                     : -1;
    int state_mapped_link_written = path_storage
                                        ? swprintf(state_mapped_link, CBM_WINDOWS_LAUNCHER_PATH_CAP,
                                                   L"%ls\\mapped-launcher.exe", state_directory)
                                        : -1;
    ok = ok && canonical_written > 0 && old_generation_written > 0 && old_payload_written > 0 &&
         old_backing_written > 0 && new_generation_written > 0 && new_payload_written > 0 &&
         new_backing_written > 0 && state_directory_written > 0 && retired_directory_written > 0 &&
         mapped_image_written > 0 && retired_mapped_image_written > 0 &&
         state_mapped_link_written > 0 &&
         (size_t)state_mapped_link_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)canonical_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)old_generation_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)old_payload_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)old_backing_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)new_generation_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)new_payload_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)new_backing_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)state_directory_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)retired_directory_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)mapped_image_written < CBM_WINDOWS_LAUNCHER_PATH_CAP &&
         (size_t)retired_mapped_image_written < CBM_WINDOWS_LAUNCHER_PATH_CAP;
    bool old_generation_created = false;
    bool old_payload_created = false;
    bool old_backing_created = false;
    bool new_generation_created = false;
    bool new_payload_created = false;
    bool new_backing_created = false;
    bool canonical_created = false;
    if (ok)
        old_generation_created = windows_create_directory_private(old_generation);
    ok = ok && old_generation_created;
    if (ok)
        new_generation_created = windows_create_directory_private(new_generation);
    ok = ok && new_generation_created;
    if (ok)
        old_payload_created = windows_copy_flush_private(launcher_candidate, old_payload);
    ok = ok && old_payload_created;
    if (ok)
        old_backing_created = windows_copy_flush_private(launcher_candidate, old_backing);
    ok = ok && old_backing_created;
    if (ok)
        new_payload_created = windows_copy_flush_private(launcher_candidate, new_payload);
    ok = ok && new_payload_created;
    if (ok)
        new_backing_created = windows_copy_flush_private(launcher_candidate, new_backing);
    ok = ok && new_backing_created && windows_generation_layout_exact(old_generation, 1U, NULL) &&
         windows_generation_layout_exact(new_generation, 1U, NULL);
    if (ok)
        canonical_created = CreateHardLinkW(canonical, old_backing, NULL) != 0;
    ok = ok && canonical_created && windows_generation_layout_exact(old_generation, 2U, NULL);
    if (!ok) {
        (void)fprintf(stderr, "error: probe middle stage %d refused (os %lu)\n", 1,
                      (unsigned long)GetLastError());
    }

    HANDLE canonical_before =
        ok ? windows_open_regular_no_reparse_links(canonical, GENERIC_READ, 2U)
           : INVALID_HANDLE_VALUE;
    HANDLE old_before = ok ? windows_open_regular_no_reparse_links(old_backing, GENERIC_READ, 2U)
                           : INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION canonical_before_information;
    BY_HANDLE_FILE_INFORMATION old_before_information;
    ok = ok && windows_file_identity_links(canonical_before, &canonical_before_information, 2U) &&
         windows_file_identity_links(old_before, &old_before_information, 2U) &&
         windows_same_identity(&canonical_before_information, &old_before_information);
    if (!ok) {
        (void)fprintf(stderr, "error: probe M1-identity-before refused (os %lu)\n",
                      (unsigned long)GetLastError());
    }
    if (canonical_before != INVALID_HANDLE_VALUE)
        (void)CloseHandle(canonical_before);
    if (old_before != INVALID_HANDLE_VALUE)
        (void)CloseHandle(old_before);

    SECURITY_ATTRIBUTES security = {
        .nLength = sizeof(security),
        .lpSecurityDescriptor = NULL,
        .bInheritHandle = TRUE,
    };
    HANDLE ready_a = ok ? CreateEventW(&security, TRUE, FALSE, NULL) : NULL;
    HANDLE release_a = ok ? CreateEventW(&security, TRUE, FALSE, NULL) : NULL;
    PROCESS_INFORMATION child_a;
    memset(&child_a, 0, sizeof(child_a));
    ok = ok && ready_a && release_a &&
         windows_probe_spawn(canonical, ready_a, release_a, &child_a) &&
         WaitForSingleObject(ready_a, 30000U) == WAIT_OBJECT_0;
    if (!ok) {
        (void)fprintf(stderr, "error: probe M2-spawnA refused (os %lu)\n",
                      (unsigned long)GetLastError());
    }

    char transaction_error[256] = {0};
    bool replace_ok =
        ok && cbm_windows_launcher_replace_atomic(canonical, new_backing, transaction_error,
                                                  sizeof(transaction_error));
    bool child_a_alive = replace_ok && WaitForSingleObject(child_a.hProcess, 0U) == WAIT_TIMEOUT;
    if (ok && !replace_ok) {
        (void)fprintf(stderr, "error: probe M3 replace_atomic refused (os %lu): %s\n",
                      (unsigned long)GetLastError(),
                      transaction_error[0] ? transaction_error : "no detail");
    }
    if (replace_ok && !child_a_alive) {
        DWORD child_code = 0U;
        (void)GetExitCodeProcess(child_a.hProcess, &child_code);
        (void)fprintf(stderr, "error: probe M3 child A exited early (code %lu)\n",
                      (unsigned long)child_code);
    }
    ok = child_a_alive;

    HANDLE canonical_after = ok ? windows_open_regular_no_reparse_links(canonical, GENERIC_READ, 2U)
                                : INVALID_HANDLE_VALUE;
    HANDLE new_after = ok ? windows_open_regular_no_reparse_links(new_backing, GENERIC_READ, 2U)
                          : INVALID_HANDLE_VALUE;
    HANDLE old_after =
        ok ? windows_open_regular_no_reparse(old_backing, GENERIC_READ) : INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION canonical_after_information;
    BY_HANDLE_FILE_INFORMATION new_after_information;
    BY_HANDLE_FILE_INFORMATION old_after_information;
    ok = ok && windows_file_identity_links(canonical_after, &canonical_after_information, 2U) &&
         windows_file_identity_links(new_after, &new_after_information, 2U) &&
         windows_same_identity(&canonical_after_information, &new_after_information) &&
         windows_file_identity(old_after, &old_after_information) &&
         !windows_same_identity(&old_after_information, &new_after_information);
    if (!ok) {
        (void)fprintf(stderr, "error: probe M4-identity-after refused (os %lu)\n",
                      (unsigned long)GetLastError());
    }
    if (canonical_after != INVALID_HANDLE_VALUE)
        (void)CloseHandle(canonical_after);
    if (new_after != INVALID_HANDLE_VALUE)
        (void)CloseHandle(new_after);
    if (old_after != INVALID_HANDLE_VALUE)
        (void)CloseHandle(old_after);

    /* Unlink the fresh, unmapped canonical name while the old generation's
     * sole backing remains mapped.  This is the real managed transaction;
     * direct deletion of the mapped image is intentionally never probed. */
    ok = ok &&
         cbm_windows_launcher_remove_posix(canonical, transaction_error, sizeof(transaction_error));
    if (!ok) {
        (void)fprintf(stderr, "error: probe M5-remove refused (os %lu)\n",
                      (unsigned long)GetLastError());
    }
    DWORD canonical_attributes = ok ? GetFileAttributesW(canonical) : INVALID_FILE_ATTRIBUTES;
    DWORD canonical_error =
        canonical_attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
    HANDLE new_final =
        ok ? windows_open_regular_no_reparse(new_backing, GENERIC_READ) : INVALID_HANDLE_VALUE;
    BY_HANDLE_FILE_INFORMATION new_final_information;
    ok = ok && canonical_attributes == INVALID_FILE_ATTRIBUTES &&
         (canonical_error == ERROR_FILE_NOT_FOUND || canonical_error == ERROR_PATH_NOT_FOUND) &&
         windows_file_identity(new_final, &new_final_information) &&
         WaitForSingleObject(child_a.hProcess, 0U) == WAIT_TIMEOUT;
    if (!ok) {
        (void)fprintf(stderr, "error: probe M6-final refused (os %lu)\n",
                      (unsigned long)GetLastError());
    }
    if (new_final != INVALID_HANDLE_VALUE)
        (void)CloseHandle(new_final);

    windows_probe_child_stop(&child_a, release_a);
    if (ready_a)
        (void)CloseHandle(ready_a);
    if (release_a)
        (void)CloseHandle(release_a);

    /* Also prove that a private state tree containing a mapped launcher can
     * be retired to a no-replace sibling and restored before any sessions are
     * drained. Uninstall relies on exactly this namespace transaction. */
    bool state_directory_created = false;
    bool mapped_image_created = false;
    if (ok) {
        state_directory_created = windows_create_directory_private(state_directory);
        if (!state_directory_created) {
            (void)fprintf(stderr, "error: probe state directory creation refused (os %lu): %ls\n",
                          (unsigned long)GetLastError(), state_directory);
        }
    }
    ok = ok && state_directory_created;
    if (ok)
        mapped_image_created = windows_copy_flush_private(launcher_candidate, mapped_image);
    ok = ok && mapped_image_created;
    /* Mirror the real uninstall layout: the EXECUTING name lives outside the
     * state tree (like the canonical launcher and the retired staged copy);
     * the tree carries only a hardlink (like a generation backing). Renaming
     * a tree that merely links a mapped image is the capability uninstall
     * actually needs — and first-touch antivirus scanning pins only the
     * executing path, so this shape is immune to the cold-scan veto that a
     * directly-mapped inside file provokes. */
    bool state_link_created = ok && CreateHardLinkW(state_mapped_link, mapped_image, NULL) != 0;
    ok = ok && state_link_created;
    HANDLE state =
        ok ? windows_open_directory_secure_access(state_directory, DELETE) : INVALID_HANDLE_VALUE;
    HANDLE ready_b = ok ? CreateEventW(&security, TRUE, FALSE, NULL) : NULL;
    HANDLE release_b = ok ? CreateEventW(&security, TRUE, FALSE, NULL) : NULL;
    PROCESS_INFORMATION child_b;
    memset(&child_b, 0, sizeof(child_b));
    bool probe_spawned = ok && state != INVALID_HANDLE_VALUE && ready_b && release_b &&
                         windows_probe_spawn(mapped_image, ready_b, release_b, &child_b);
    bool probe_ready = probe_spawned && WaitForSingleObject(ready_b, 30000U) == WAIT_OBJECT_0;
    bool probe_retired =
        probe_ready && windows_posix_rename_handle_no_replace(state, retired_directory);
    if (ok && !probe_retired) {
        (void)fprintf(stderr,
                      "error: capability probe stage failed: spawned=%d ready=%d "
                      "retire-rename refused (os %lu)\n",
                      probe_spawned ? 1 : 0, probe_ready ? 1 : 0, (unsigned long)GetLastError());
        (void)fprintf(stderr, "error: probe retire target was: %ls\n", retired_directory);
        (void)fprintf(stderr, "error: probe state source was: %ls\n", state_directory);
    }
    ok = probe_retired;
    DWORD state_attributes = ok ? GetFileAttributesW(state_directory) : 0U;
    DWORD state_missing_error =
        ok && state_attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
    HANDLE retired_check =
        ok ? windows_open_directory_secure(retired_directory) : INVALID_HANDLE_VALUE;
    ok = ok && state_attributes == INVALID_FILE_ATTRIBUTES &&
         (state_missing_error == ERROR_FILE_NOT_FOUND ||
          state_missing_error == ERROR_PATH_NOT_FOUND) &&
         retired_check != INVALID_HANDLE_VALUE &&
         WaitForSingleObject(child_b.hProcess, 0U) == WAIT_TIMEOUT &&
         windows_posix_rename_handle_no_replace(state, state_directory);
    if (retired_check != INVALID_HANDLE_VALUE)
        (void)CloseHandle(retired_check);
    DWORD retired_attributes = ok ? GetFileAttributesW(retired_directory) : 0U;
    DWORD retired_missing_error =
        ok && retired_attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
    HANDLE state_check = ok ? windows_open_directory_secure(state_directory) : INVALID_HANDLE_VALUE;
    ok = ok && retired_attributes == INVALID_FILE_ATTRIBUTES &&
         (retired_missing_error == ERROR_FILE_NOT_FOUND ||
          retired_missing_error == ERROR_PATH_NOT_FOUND) &&
         state_check != INVALID_HANDLE_VALUE &&
         WaitForSingleObject(child_b.hProcess, 0U) == WAIT_TIMEOUT;
    if (state_check != INVALID_HANDLE_VALUE)
        (void)CloseHandle(state_check);
    windows_probe_child_stop(&child_b, release_b);
    if (ready_b)
        (void)CloseHandle(ready_b);
    if (release_b)
        (void)CloseHandle(release_b);
    if (state != INVALID_HANDLE_VALUE)
        (void)CloseHandle(state);

    if (canonical_created)
        (void)DeleteFileW(canonical);
    if (old_backing_created)
        (void)DeleteFileW(old_backing);
    if (old_payload_created)
        (void)DeleteFileW(old_payload);
    if (old_generation_created)
        (void)RemoveDirectoryW(old_generation);
    if (new_backing_created)
        (void)DeleteFileW(new_backing);
    if (new_payload_created)
        (void)DeleteFileW(new_payload);
    if (new_generation_created)
        (void)RemoveDirectoryW(new_generation);
    if (mapped_image_created) {
        (void)DeleteFileW(mapped_image);
        (void)DeleteFileW(retired_mapped_image);
        (void)DeleteFileW(state_mapped_link);
    }
    if (state_directory_created) {
        (void)RemoveDirectoryW(state_directory);
        (void)RemoveDirectoryW(retired_directory);
    }
    if (mutex_wait == WAIT_OBJECT_0)
        (void)ReleaseMutex(mutex);
    if (mutex)
        (void)CloseHandle(mutex);
    free(path_storage);
    windows_created_directories_close(&created, true);
    if (!ok) {
        /* Failure-path observability: name the phase that refused. */
        (void)fprintf(stderr,
                      "error: capability probe flags: oldgen=%d newgen=%d oldpay=%d "
                      "oldback=%d newpay=%d newback=%d canon=%d statedir=%d mapped=%d link=%d\n",
                      old_generation_created ? 1 : 0, new_generation_created ? 1 : 0,
                      old_payload_created ? 1 : 0, old_backing_created ? 1 : 0,
                      new_payload_created ? 1 : 0, new_backing_created ? 1 : 0,
                      canonical_created ? 1 : 0, state_directory_created ? 1 : 0,
                      mapped_image_created ? 1 : 0, state_link_created ? 1 : 0);
    }
    if (!ok && (!error || error_size == 0U || error[0] == '\0')) {
        launcher_error(error, error_size,
                       "mapped-image hard-link and state-retirement capability probe failed");
    }
    return ok;
}

bool cbm_windows_launcher_capability_probe(const wchar_t *target_directory,
                                           const wchar_t *launcher_candidate, char *error,
                                           size_t error_size) {
    /* First-touch antivirus scanning pins a never-seen executable for the
     * lifetime of the process mapped from it, which vetoes the probe's
     * state-retirement rename no matter how long that one call retries. The
     * first attempt's staged copies get scanned while it runs, so a single
     * full re-run is warm by construction. Freshly built or downloaded
     * binaries — every first install — hit this. */
    for (int attempt = 0;; attempt++) {
        if (windows_launcher_capability_probe_once(target_directory, launcher_candidate, error,
                                                   error_size)) {
            return true;
        }
        if (attempt >= 1) {
            return false;
        }
        Sleep(500U);
    }
}

#endif /* _WIN32 */
