/* asset_pack.c — Hash-bound loader for the external UI frontend pack. */
#include "ui/asset_pack.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include "foundation/sha256.h"
#ifdef _WIN32
#include "foundation/win_utf8.h"
#endif

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

enum {
    UI_PACK_HEADER_BYTES = 80,
    UI_PACK_ENTRY_BYTES = 24,
    UI_PACK_VERSION = 1,
    UI_PACK_MAX_FILES = 1024,
    UI_PACK_MAX_PATH_BYTES = 255,
    UI_ASSETS_DIR_PERM = 0755,
    UI_ASSET_FILE_PERM = 0644,
    UI_PATH_CAP = 4096,
};

#define UI_PACK_MAX_BYTES ((size_t)CBM_SZ_64K * CBM_SZ_1K)

typedef struct {
    unsigned char *bytes;
    size_t length;
    cbm_ui_asset_t *assets;
    size_t asset_count;
    char *paths;
} ui_asset_snapshot_t;

static ui_asset_snapshot_t g_snapshot;
static atomic_int g_state = CBM_UI_ASSETS_COLD;
static atomic_bool g_cancel_requested = false;
static char g_binary_path[UI_PATH_CAP];

#ifdef CBM_CLI_ENABLE_TEST_API
static bool g_test_manifest_set;
static char g_test_pack_name[UI_PATH_CAP];
static char g_test_sha256[CBM_SHA256_HEX_LEN + 1U];
static uint64_t g_test_size;
#endif

static const char *ui_manifest_name(void) {
#ifdef CBM_CLI_ENABLE_TEST_API
    if (g_test_manifest_set) {
        return g_test_pack_name;
    }
#endif
    return CBM_UI_ASSET_PACK_NAME;
}

static const char *ui_manifest_sha256(void) {
#ifdef CBM_CLI_ENABLE_TEST_API
    if (g_test_manifest_set) {
        return g_test_sha256;
    }
#endif
    return CBM_UI_ASSET_SHA256;
}

static uint64_t ui_manifest_size(void) {
#ifdef CBM_CLI_ENABLE_TEST_API
    if (g_test_manifest_set) {
        return g_test_size;
    }
#endif
    return CBM_UI_ASSET_SIZE;
}

static bool ui_hex_sha256(const char *value) {
    if (!value || strlen(value) != CBM_SHA256_HEX_LEN) {
        return false;
    }
    for (size_t index = 0; index < CBM_SHA256_HEX_LEN; index++) {
        char byte = value[index];
        if (!((byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f'))) {
            return false;
        }
    }
    return true;
}

static bool ui_content_addressed_name(const char *name, const char *sha256) {
    static const char prefix[] = "cbm-ui-";
    static const char suffix[] = ".pack";
    if (!name || !sha256 || strncmp(name, prefix, sizeof(prefix) - 1U) != 0) {
        return false;
    }
    size_t expected_length = (sizeof(prefix) - 1U) + CBM_SHA256_HEX_LEN + (sizeof(suffix) - 1U);
    return strlen(name) == expected_length &&
           strncmp(name + sizeof(prefix) - 1U, sha256, CBM_SHA256_HEX_LEN) == 0 &&
           strcmp(name + expected_length - (sizeof(suffix) - 1U), suffix) == 0;
}

bool cbm_ui_assets_supported(void) {
    const char *name = ui_manifest_name();
    const char *sha256 = ui_manifest_sha256();
    uint64_t size = ui_manifest_size();
    return size >= UI_PACK_HEADER_BYTES && size <= UI_PACK_MAX_BYTES && ui_hex_sha256(sha256) &&
           ui_content_addressed_name(name, sha256);
}

const char *cbm_ui_assets_current_pack_name(void) {
    return cbm_ui_assets_supported() ? ui_manifest_name() : NULL;
}

void cbm_ui_assets_set_binary_path(const char *path) {
    if (!path) {
        g_binary_path[0] = '\0';
        return;
    }
    int written = snprintf(g_binary_path, sizeof(g_binary_path), "%s", path);
    if (written <= 0 || (size_t)written >= sizeof(g_binary_path)) {
        g_binary_path[0] = '\0';
    }
}

static void ui_fill_error(char *err, size_t err_sz, const char *detail) {
    if (!err || err_sz == 0U) {
        return;
    }
    (void)snprintf(err, err_sz,
                   "UI assets missing or modified - reinstall the matching release "
                   "(expected %s%s%s)",
                   ui_manifest_name(), detail && detail[0] ? ": " : "", detail ? detail : "");
}

static uint16_t ui_read_u16(const unsigned char *bytes) {
    return (uint16_t)bytes[0] | ((uint16_t)bytes[1] << 8U);
}

static uint32_t ui_read_u32(const unsigned char *bytes) {
    return (uint32_t)bytes[0] | ((uint32_t)bytes[1] << 8U) | ((uint32_t)bytes[2] << 16U) |
           ((uint32_t)bytes[3] << 24U);
}

static uint64_t ui_read_u64(const unsigned char *bytes) {
    uint64_t value = 0U;
    for (size_t index = 0U; index < sizeof(uint64_t); index++) {
        value |= (uint64_t)bytes[index] << (index * 8U);
    }
    return value;
}

static const char *ui_mime_type(unsigned int mime_id) {
    static const char *const types[] = {
        NULL,
        "text/html; charset=utf-8",
        "application/javascript; charset=utf-8",
        "text/css; charset=utf-8",
        "application/json; charset=utf-8",
        "image/svg+xml",
        "image/png",
        "image/jpeg",
        "image/webp",
        "image/avif",
        "image/x-icon",
        "font/woff2",
        "font/woff",
        "application/wasm",
    };
    return mime_id < sizeof(types) / sizeof(types[0]) ? types[mime_id] : NULL;
}

static unsigned int ui_expected_mime(const char *path) {
    const char *dot = strrchr(path, '.');
    if (!dot) {
        return 0U;
    }
    if (strcmp(dot, ".html") == 0)
        return 1U;
    if (strcmp(dot, ".js") == 0 || strcmp(dot, ".mjs") == 0)
        return 2U;
    if (strcmp(dot, ".css") == 0)
        return 3U;
    if (strcmp(dot, ".json") == 0)
        return 4U;
    if (strcmp(dot, ".svg") == 0)
        return 5U;
    if (strcmp(dot, ".png") == 0)
        return 6U;
    if (strcmp(dot, ".jpg") == 0 || strcmp(dot, ".jpeg") == 0)
        return 7U;
    if (strcmp(dot, ".webp") == 0)
        return 8U;
    if (strcmp(dot, ".avif") == 0)
        return 9U;
    if (strcmp(dot, ".ico") == 0)
        return 10U;
    if (strcmp(dot, ".woff2") == 0)
        return 11U;
    if (strcmp(dot, ".woff") == 0)
        return 12U;
    if (strcmp(dot, ".wasm") == 0)
        return 13U;
    return 0U;
}

static bool ui_path_valid(const unsigned char *path, size_t length) {
    if (length < 2U || length > UI_PACK_MAX_PATH_BYTES || path[0] != '/' ||
        path[length - 1U] == '/') {
        return false;
    }
    for (size_t index = 0U; index < length; index++) {
        unsigned char byte = path[index];
        bool allowed = (byte >= 'A' && byte <= 'Z') || (byte >= 'a' && byte <= 'z') ||
                       (byte >= '0' && byte <= '9') || byte == '.' || byte == '_' || byte == '-' ||
                       byte == '/';
        if (!allowed || (byte == '/' && index + 1U < length && path[index + 1U] == '/')) {
            return false;
        }
    }
    for (size_t start = 1U; start < length;) {
        size_t end = start;
        while (end < length && path[end] != '/') {
            end++;
        }
        size_t segment = end - start;
        if ((segment == 1U && path[start] == '.') ||
            (segment == 2U && path[start] == '.' && path[start + 1U] == '.')) {
            return false;
        }
        start = end + 1U;
    }
    static const unsigned char index_path[] = "/index.html";
    static const unsigned char asset_prefix[] = "/assets/";
    return (length == sizeof(index_path) - 1U &&
            memcmp(path, index_path, sizeof(index_path) - 1U) == 0) ||
           (length > sizeof(asset_prefix) - 1U &&
            memcmp(path, asset_prefix, sizeof(asset_prefix) - 1U) == 0);
}

static int ui_path_compare(const unsigned char *left, size_t left_length,
                           const unsigned char *right, size_t right_length) {
    size_t common = left_length < right_length ? left_length : right_length;
    int compared = memcmp(left, right, common);
    if (compared != 0) {
        return compared;
    }
    return left_length < right_length ? -1 : left_length > right_length ? 1 : 0;
}

static void ui_snapshot_free(ui_asset_snapshot_t *snapshot) {
    if (!snapshot) {
        return;
    }
    free(snapshot->paths);
    free(snapshot->assets);
    free(snapshot->bytes);
    memset(snapshot, 0, sizeof(*snapshot));
}

static bool ui_parse_pack(unsigned char *bytes, size_t length, ui_asset_snapshot_t *out) {
    /* Keep the seven format bytes as separate comparisons. A contiguous
     * native-binary copy would make the composition gate unable to
     * distinguish this parser constant from an accidentally embedded pack. */
    bool valid_magic = bytes && length >= 8U && bytes[0] == 'C' && bytes[1] == 'B' &&
                       bytes[2] == 'M' && bytes[3] == 'U' && bytes[4] == 'I' && bytes[5] == 'P' &&
                       bytes[6] == 'K' && bytes[7] == 0U;
    if (!bytes || !out || length < UI_PACK_HEADER_BYTES || !valid_magic ||
        ui_read_u16(bytes + 8U) != UI_PACK_VERSION ||
        ui_read_u16(bytes + 10U) != UI_PACK_HEADER_BYTES || ui_read_u32(bytes + 12U) != 0U) {
        return false;
    }
    uint32_t file_count = ui_read_u32(bytes + 16U);
    uint32_t entry_bytes = ui_read_u32(bytes + 20U);
    uint64_t index_offset = ui_read_u64(bytes + 24U);
    uint64_t index_bytes = ui_read_u64(bytes + 32U);
    uint64_t paths_offset = ui_read_u64(bytes + 40U);
    uint64_t paths_bytes = ui_read_u64(bytes + 48U);
    uint64_t payload_offset = ui_read_u64(bytes + 56U);
    uint64_t payload_bytes = ui_read_u64(bytes + 64U);
    uint64_t total_bytes = ui_read_u64(bytes + 72U);
    uint64_t length_u64 = (uint64_t)length;
    if (file_count == 0U || file_count > UI_PACK_MAX_FILES || entry_bytes != UI_PACK_ENTRY_BYTES ||
        index_offset != UI_PACK_HEADER_BYTES ||
        index_bytes != (uint64_t)file_count * UI_PACK_ENTRY_BYTES || index_offset > length_u64 ||
        index_bytes > length_u64 - index_offset || paths_offset != index_offset + index_bytes ||
        paths_offset > length_u64 || paths_bytes > length_u64 - paths_offset ||
        payload_offset != paths_offset + paths_bytes || payload_offset > length_u64 ||
        payload_bytes > length_u64 - payload_offset ||
        total_bytes != payload_offset + payload_bytes || total_bytes != length_u64 ||
        paths_bytes > SIZE_MAX - file_count || payload_bytes > SIZE_MAX) {
        return false;
    }
    cbm_ui_asset_t *assets = calloc(file_count, sizeof(*assets));
    char *paths = malloc((size_t)paths_bytes + file_count);
    if (!assets || !paths) {
        free(assets);
        free(paths);
        return false;
    }
    uint64_t expected_path_offset = 0U;
    uint64_t expected_data_offset = 0U;
    size_t path_storage_offset = 0U;
    bool has_index = false;
    const unsigned char *previous_path = NULL;
    size_t previous_length = 0U;
    for (uint32_t index = 0U; index < file_count; index++) {
        const unsigned char *entry =
            bytes + (size_t)index_offset + (size_t)index * UI_PACK_ENTRY_BYTES;
        uint32_t path_offset = ui_read_u32(entry);
        uint16_t path_length = ui_read_u16(entry + 4U);
        unsigned int mime_id = entry[6U];
        unsigned int cache_id = entry[7U];
        uint64_t data_offset = ui_read_u64(entry + 8U);
        uint64_t data_length = ui_read_u64(entry + 16U);
        if (path_offset != expected_path_offset || path_length == 0U ||
            (uint64_t)path_length > paths_bytes - expected_path_offset ||
            data_offset != expected_data_offset || data_length == 0U ||
            data_length > payload_bytes - expected_data_offset) {
            free(assets);
            free(paths);
            return false;
        }
        const unsigned char *path_bytes = bytes + (size_t)paths_offset + path_offset;
        if (!ui_path_valid(path_bytes, path_length) ||
            (previous_path &&
             ui_path_compare(previous_path, previous_length, path_bytes, path_length) >= 0)) {
            free(assets);
            free(paths);
            return false;
        }
        char *stored_path = paths + path_storage_offset;
        memcpy(stored_path, path_bytes, path_length);
        stored_path[path_length] = '\0';
        const char *mime = ui_mime_type(mime_id);
        bool index_asset = strcmp(stored_path, "/index.html") == 0;
        if (!mime || ui_expected_mime(stored_path) != mime_id ||
            (index_asset ? cache_id != CBM_UI_ASSET_REVALIDATE
                         : cache_id != CBM_UI_ASSET_IMMUTABLE) ||
            (index_asset && has_index)) {
            free(assets);
            free(paths);
            return false;
        }
        has_index = has_index || index_asset;
        assets[index] = (cbm_ui_asset_t){
            .path = stored_path,
            .data = bytes + (size_t)payload_offset + (size_t)data_offset,
            .size = (size_t)data_length,
            .content_type = mime,
            .cache = (cbm_ui_asset_cache_t)cache_id,
        };
        previous_path = path_bytes;
        previous_length = path_length;
        expected_path_offset += path_length;
        expected_data_offset += data_length;
        path_storage_offset += path_length + 1U;
    }
    if (!has_index || expected_path_offset != paths_bytes ||
        expected_data_offset != payload_bytes) {
        free(assets);
        free(paths);
        return false;
    }
    *out = (ui_asset_snapshot_t){
        .bytes = bytes,
        .length = length,
        .assets = assets,
        .asset_count = file_count,
        .paths = paths,
    };
    return true;
}

static bool ui_read_regular_file(const char *path, unsigned char **bytes_out, size_t *length_out) {
    *bytes_out = NULL;
    *length_out = 0U;
    cbm_path_info_t info;
    uint64_t expected = ui_manifest_size();
    if (!path || cbm_path_info_utf8(path, &info) != 0 || !info.is_regular || info.is_symlink ||
        info.size <= 0 || (uint64_t)info.size != expected || expected > UI_PACK_MAX_BYTES) {
        return false;
    }
    unsigned char *bytes = malloc((size_t)expected);
    if (!bytes) {
        return false;
    }
#ifdef _WIN32
    wchar_t *wide = cbm_path_to_wide(path);
    HANDLE file = wide ? CreateFileW(wide, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_DELETE, NULL,
                                     OPEN_EXISTING,
                                     FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL)
                       : INVALID_HANDLE_VALUE;
    free(wide);
    if (file == INVALID_HANDLE_VALUE) {
        free(bytes);
        return false;
    }
    BY_HANDLE_FILE_INFORMATION handle_info;
    bool handle_info_ok = GetFileInformationByHandle(file, &handle_info) != 0;
    uint64_t handle_size = handle_info_ok ? ((uint64_t)handle_info.nFileSizeHigh << 32U) |
                                                (uint64_t)handle_info.nFileSizeLow
                                          : UINT64_MAX;
    bool ok = handle_info_ok && handle_size == expected &&
              !(handle_info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) &&
              !(handle_info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
    size_t offset = 0U;
    while (ok && offset < (size_t)expected &&
           !atomic_load_explicit(&g_cancel_requested, memory_order_acquire)) {
        DWORD chunk =
            (DWORD)(((size_t)expected - offset) > CBM_SZ_64K ? CBM_SZ_64K
                                                             : ((size_t)expected - offset));
        DWORD got = 0U;
        ok = ReadFile(file, bytes + offset, chunk, &got, NULL) && got == chunk;
        offset += got;
    }
    (void)CloseHandle(file);
    ok = ok && offset == (size_t)expected;
#else
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    int file = open(path, flags);
    struct stat opened_info;
    bool ok = file >= 0 && fstat(file, &opened_info) == 0 && S_ISREG(opened_info.st_mode) &&
              (uint64_t)opened_info.st_size == expected;
    size_t offset = 0U;
    while (ok && offset < (size_t)expected &&
           !atomic_load_explicit(&g_cancel_requested, memory_order_acquire)) {
        size_t chunk = (size_t)expected - offset;
        if (chunk > CBM_SZ_64K) {
            chunk = CBM_SZ_64K;
        }
        ssize_t got = read(file, bytes + offset, chunk);
        if (got < 0 && errno == EINTR) {
            continue;
        }
        if (got <= 0) {
            ok = false;
            break;
        }
        offset += (size_t)got;
    }
    if (file >= 0) {
        (void)close(file);
    }
    ok = ok && offset == (size_t)expected;
#endif
    if (!ok || atomic_load_explicit(&g_cancel_requested, memory_order_acquire)) {
        free(bytes);
        return false;
    }
    *bytes_out = bytes;
    *length_out = (size_t)expected;
    return true;
}

static bool ui_verify_bytes(const unsigned char *bytes, size_t length) {
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    for (size_t offset = 0U; offset < length; offset += CBM_SZ_64K) {
        if (atomic_load_explicit(&g_cancel_requested, memory_order_acquire)) {
            return false;
        }
        size_t chunk = length - offset;
        if (chunk > CBM_SZ_64K) {
            chunk = CBM_SZ_64K;
        }
        cbm_sha256_update(&hash, bytes + offset, chunk);
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&hash, digest);
    static const char hex_chars[] = "0123456789abcdef";
    char hex[CBM_SHA256_HEX_LEN + 1U];
    for (size_t index = 0U; index < sizeof(digest); index++) {
        hex[index * 2U] = hex_chars[digest[index] >> 4U];
        hex[index * 2U + 1U] = hex_chars[digest[index] & 0x0fU];
    }
    hex[CBM_SHA256_HEX_LEN] = '\0';
    return strcmp(hex, ui_manifest_sha256()) == 0;
}

static bool ui_candidate_path(const char *directory, char out[UI_PATH_CAP]) {
    if (!directory || !directory[0]) {
        return false;
    }
    int written = snprintf(out, UI_PATH_CAP, "%s/%s", directory, ui_manifest_name());
    return written > 0 && written < UI_PATH_CAP;
}

static bool ui_binary_directory(char out[UI_PATH_CAP]) {
    int written = snprintf(out, UI_PATH_CAP, "%s", g_binary_path);
    if (written <= 0 || written >= UI_PATH_CAP) {
        return false;
    }
    char *slash = strrchr(out, '/');
#ifdef _WIN32
    char *backslash = strrchr(out, '\\');
    if (backslash && (!slash || backslash > slash)) {
        slash = backslash;
    }
#endif
    if (!slash || slash == out) {
        return false;
    }
    *slash = '\0';
    return true;
}

static bool ui_try_path(const char *path, ui_asset_snapshot_t *snapshot) {
    unsigned char *bytes = NULL;
    size_t length = 0U;
    if (!ui_read_regular_file(path, &bytes, &length)) {
        return false;
    }
    if (!ui_verify_bytes(bytes, length) || !ui_parse_pack(bytes, length, snapshot)) {
        free(bytes);
        return false;
    }
    return true;
}

bool cbm_ui_assets_verify_file(const char *path) {
    if (!cbm_ui_assets_supported() || !path || !path[0]) {
        return false;
    }
    ui_asset_snapshot_t verified = {0};
    bool ok = ui_try_path(path, &verified);
    ui_snapshot_free(&verified);
    return ok;
}

static bool ui_resolve_snapshot(const char *home, ui_asset_snapshot_t *snapshot) {
    static const char missing[] = "\x1f"
                                  "CBM_UI_ASSETS_DIR_MISSING"
                                  "\x1f";
    char env_buffer[UI_PATH_CAP];
    char candidate[UI_PATH_CAP];
    const char *override =
        cbm_safe_getenv("CBM_UI_ASSETS_DIR", env_buffer, sizeof(env_buffer), missing);
    if (!override) {
        return false;
    }
    if (strcmp(override, missing) != 0 && override[0]) {
        return ui_candidate_path(override, candidate) && ui_try_path(candidate, snapshot);
    }
    char binary_directory[UI_PATH_CAP];
    if (ui_binary_directory(binary_directory)) {
        if (ui_candidate_path(binary_directory, candidate)) {
            cbm_path_info_t info;
            if (cbm_path_info_utf8(candidate, &info) == 0) {
                return ui_try_path(candidate, snapshot);
            }
        }
        int written = snprintf(candidate, sizeof(candidate), "%s/../share/codebase-memory-mcp/%s",
                               binary_directory, ui_manifest_name());
        if (written > 0 && (size_t)written < sizeof(candidate)) {
            cbm_path_info_t info;
            if (cbm_path_info_utf8(candidate, &info) == 0) {
                return ui_try_path(candidate, snapshot);
            }
        }
    }
    (void)home;
    return false;
}

bool cbm_ui_assets_warm(const char *home, char *err, size_t err_sz) {
    if (!cbm_ui_assets_supported()) {
        return true;
    }
    int expected = CBM_UI_ASSETS_COLD;
    if (!atomic_compare_exchange_strong_explicit(&g_state, &expected, CBM_UI_ASSETS_LOADING,
                                                 memory_order_acq_rel, memory_order_acquire)) {
        if (expected == CBM_UI_ASSETS_READY) {
            return true;
        }
        const char *detail = expected == CBM_UI_ASSETS_LOADING     ? "load already in progress"
                             : expected == CBM_UI_ASSETS_CANCELLED ? "load cancelled"
                                                                   : "load previously failed";
        ui_fill_error(err, err_sz, detail);
        return false;
    }
    ui_asset_snapshot_t snapshot = {0};
    bool loaded = ui_resolve_snapshot(home, &snapshot);
    if (atomic_load_explicit(&g_cancel_requested, memory_order_acquire)) {
        ui_snapshot_free(&snapshot);
        atomic_store_explicit(&g_state, CBM_UI_ASSETS_CANCELLED, memory_order_release);
        ui_fill_error(err, err_sz, "load cancelled");
        return false;
    }
    if (!loaded) {
        ui_snapshot_free(&snapshot);
        atomic_store_explicit(&g_state, CBM_UI_ASSETS_FAILED, memory_order_release);
        ui_fill_error(err, err_sz, "verified pack not found");
        return false;
    }
    g_snapshot = snapshot;
    atomic_store_explicit(&g_state, CBM_UI_ASSETS_READY, memory_order_release);
    return true;
}

void cbm_ui_assets_request_cancel(void) {
    atomic_store_explicit(&g_cancel_requested, true, memory_order_release);
    int expected = CBM_UI_ASSETS_COLD;
    (void)atomic_compare_exchange_strong_explicit(&g_state, &expected, CBM_UI_ASSETS_CANCELLED,
                                                  memory_order_acq_rel, memory_order_acquire);
}

cbm_ui_assets_state_t cbm_ui_assets_state(void) {
    if (!cbm_ui_assets_supported()) {
        return CBM_UI_ASSETS_UNAVAILABLE;
    }
    return (cbm_ui_assets_state_t)atomic_load_explicit(&g_state, memory_order_acquire);
}

const cbm_ui_asset_t *cbm_ui_asset_lookup(const char *path) {
    if (!path || cbm_ui_assets_state() != CBM_UI_ASSETS_READY) {
        return NULL;
    }
    size_t low = 0U;
    size_t high = g_snapshot.asset_count;
    while (low < high) {
        size_t middle = low + (high - low) / 2U;
        int compared = strcmp(path, g_snapshot.assets[middle].path);
        if (compared == 0) {
            return &g_snapshot.assets[middle];
        }
        if (compared < 0) {
            high = middle;
        } else {
            low = middle + 1U;
        }
    }
    return NULL;
}

bool cbm_ui_assets_stage_install(const char *install_dir,
                                 cbm_activation_transaction_t **transaction_out, char *err,
                                 size_t err_sz) {
    if (transaction_out) {
        *transaction_out = NULL;
    }
    if (!cbm_ui_assets_supported()) {
        return true;
    }
    if (!transaction_out || !install_dir || !install_dir[0] ||
        !cbm_ui_assets_warm(NULL, err, err_sz)) {
        if (!install_dir || !install_dir[0]) {
            ui_fill_error(err, err_sz, "install directory unavailable");
        }
        return false;
    }
    char stored[UI_PATH_CAP];
    if (!ui_candidate_path(install_dir, stored)) {
        ui_fill_error(err, err_sz, "install path is too long");
        return false;
    }
    if (!cbm_mkdir_p(install_dir, UI_ASSETS_DIR_PERM)) {
        ui_fill_error(err, err_sz, "cannot create the install directory");
        return false;
    }
    cbm_activation_transaction_status_t status = cbm_activation_transaction_stage_bytes(
        stored, g_snapshot.bytes, g_snapshot.length, transaction_out);
    if (status != CBM_ACTIVATION_TRANSACTION_OK || !*transaction_out) {
        if (*transaction_out) {
            (void)cbm_activation_transaction_close(transaction_out);
        }
        ui_fill_error(err, err_sz, "cannot stage the verified pack");
        return false;
    }
    return true;
}

static bool ui_install_validator(const char *target_path, void *context) {
    (void)context;
    return cbm_ui_assets_verify_file(target_path);
}

cbm_activation_transaction_status_t cbm_ui_assets_commit_install(
    cbm_activation_transaction_t *transaction) {
    if (!transaction) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    cbm_activation_transaction_status_t status =
        cbm_activation_transaction_commit(transaction, ui_install_validator, NULL);
#ifndef _WIN32
    if (status == CBM_ACTIVATION_TRANSACTION_OK) {
        const char *target = cbm_activation_transaction_target_path(transaction);
        if (!target || chmod(target, UI_ASSET_FILE_PERM) != 0) {
            cbm_activation_transaction_status_t rollback =
                cbm_activation_transaction_rollback(transaction);
            return rollback == CBM_ACTIVATION_TRANSACTION_OK
                       ? CBM_ACTIVATION_TRANSACTION_IO
                       : CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
        }
    }
#endif
    return status;
}

cbm_activation_transaction_status_t cbm_ui_assets_commit_removal(
    cbm_activation_transaction_t *transaction) {
    if (!transaction) {
        return CBM_ACTIVATION_TRANSACTION_INVALID_ARGUMENT;
    }
    cbm_activation_transaction_status_t status =
        cbm_activation_transaction_commit(transaction, NULL, NULL);
    const char *retained = cbm_activation_transaction_backup_path(transaction);
    if (status != CBM_ACTIVATION_TRANSACTION_OK) {
        return status;
    }
    if (retained && cbm_ui_assets_verify_file(retained)) {
        return CBM_ACTIVATION_TRANSACTION_OK;
    }
    cbm_activation_transaction_status_t rollback =
        cbm_activation_transaction_rollback(transaction);
    return rollback == CBM_ACTIVATION_TRANSACTION_OK
               ? CBM_ACTIVATION_TRANSACTION_VALIDATION_FAILED
               : CBM_ACTIVATION_TRANSACTION_ROLLBACK_FAILED;
}

#ifdef CBM_CLI_ENABLE_TEST_API
bool cbm_ui_assets_install(const char *install_dir, bool dry_run, char *err, size_t err_sz) {
    if (!cbm_ui_assets_supported()) {
        return true;
    }
    if (!install_dir || !install_dir[0] || !cbm_ui_assets_warm(NULL, err, err_sz)) {
        if (!install_dir || !install_dir[0]) {
            ui_fill_error(err, err_sz, "install directory unavailable");
        }
        return false;
    }
    if (dry_run) {
        return true;
    }
    cbm_activation_transaction_t *transaction = NULL;
    if (!cbm_ui_assets_stage_install(install_dir, &transaction, err, err_sz)) {
        return false;
    }
    cbm_activation_transaction_status_t status = cbm_ui_assets_commit_install(transaction);
    if (status != CBM_ACTIVATION_TRANSACTION_OK) {
        (void)cbm_activation_transaction_close(&transaction);
        ui_fill_error(err, err_sz, "cannot publish the verified pack");
        return false;
    }
    status = cbm_activation_transaction_finalize(transaction);
    bool finalized =
        status == CBM_ACTIVATION_TRANSACTION_OK || status == CBM_ACTIVATION_TRANSACTION_DEFERRED;
    bool closed = cbm_activation_transaction_close(&transaction) == CBM_ACTIVATION_TRANSACTION_OK;
    if (!finalized || !closed) {
        ui_fill_error(err, err_sz, "cannot finalize the verified pack");
        return false;
    }
    return true;
}
#endif

bool cbm_ui_assets_remove(const char *install_dir, bool dry_run, char *err, size_t err_sz) {
    if (!cbm_ui_assets_supported()) {
        return true;
    }
    char path[UI_PATH_CAP];
    if (!ui_candidate_path(install_dir, path)) {
        ui_fill_error(err, err_sz, "uninstall path is too long");
        return false;
    }
    cbm_path_info_t info;
    if (cbm_path_info_utf8(path, &info) != 0) {
        return true;
    }
    ui_asset_snapshot_t verified = {0};
    if (!ui_try_path(path, &verified)) {
        ui_fill_error(err, err_sz, "adjacent pack is not owned; preserved");
        return false;
    }
    ui_snapshot_free(&verified);
    if (dry_run) {
        return true;
    }
    cbm_activation_transaction_t *transaction = NULL;
    cbm_activation_transaction_status_t status =
        cbm_activation_transaction_stage_removal(path, &transaction);
    if (status == CBM_ACTIVATION_TRANSACTION_OK && transaction) {
        status = cbm_ui_assets_commit_removal(transaction);
    }
    if (status != CBM_ACTIVATION_TRANSACTION_OK || !transaction) {
        (void)cbm_activation_transaction_close(&transaction);
        ui_fill_error(err, err_sz,
                      status == CBM_ACTIVATION_TRANSACTION_VALIDATION_FAILED
                          ? "adjacent pack changed before removal; preserved"
                          : "cannot remove the owned adjacent pack");
        return false;
    }
    status = cbm_activation_transaction_finalize(transaction);
    bool finalized =
        status == CBM_ACTIVATION_TRANSACTION_OK || status == CBM_ACTIVATION_TRANSACTION_DEFERRED;
    bool closed = cbm_activation_transaction_close(&transaction) == CBM_ACTIVATION_TRANSACTION_OK;
    if (!finalized || !closed) {
        ui_fill_error(err, err_sz, "cannot finalize the owned adjacent pack removal");
        return false;
    }
    return true;
}

bool cbm_ui_assets_stage_remove(const char *install_dir,
                                cbm_activation_transaction_t **transaction_out,
                                bool *foreign_preserved_out, char *err, size_t err_sz) {
    if (transaction_out) {
        *transaction_out = NULL;
    }
    if (foreign_preserved_out) {
        *foreign_preserved_out = false;
    }
    if (!transaction_out) {
        ui_fill_error(err, err_sz, "uninstall transaction output unavailable");
        return false;
    }
    if (!cbm_ui_assets_supported()) {
        return true;
    }
    char path[UI_PATH_CAP];
    if (!install_dir || !install_dir[0] || !ui_candidate_path(install_dir, path)) {
        ui_fill_error(err, err_sz, "uninstall path is too long");
        return false;
    }
    cbm_path_info_t info;
    if (cbm_path_info_utf8(path, &info) != 0) {
        return true;
    }
    if (!info.is_regular || info.is_symlink || !cbm_ui_assets_verify_file(path)) {
        if (foreign_preserved_out) {
            *foreign_preserved_out = true;
        }
        return true;
    }
    cbm_activation_transaction_status_t status =
        cbm_activation_transaction_stage_removal(path, transaction_out);
    if (status != CBM_ACTIVATION_TRANSACTION_OK || !*transaction_out) {
        ui_fill_error(err, err_sz, cbm_activation_transaction_status_message(status));
        if (*transaction_out) {
            (void)cbm_activation_transaction_close(transaction_out);
        }
        return false;
    }
    /* Revalidate after the transaction captured target identity. Commit will
     * refuse if that identity changes before removal. */
    if (!cbm_ui_assets_verify_file(path)) {
        if (cbm_activation_transaction_close(transaction_out) != CBM_ACTIVATION_TRANSACTION_OK) {
            ui_fill_error(err, err_sz, "cannot clean up a changed pack removal transaction");
            return false;
        }
        if (foreign_preserved_out) {
            *foreign_preserved_out = true;
        }
    }
    return true;
}

#ifdef CBM_CLI_ENABLE_TEST_API
void cbm_ui_assets_set_manifest_for_testing(const char *name, const char *sha256, uint64_t size) {
    int name_written = snprintf(g_test_pack_name, sizeof(g_test_pack_name), "%s", name ? name : "");
    int hash_written = snprintf(g_test_sha256, sizeof(g_test_sha256), "%s", sha256 ? sha256 : "");
    g_test_manifest_set = name_written > 0 && (size_t)name_written < sizeof(g_test_pack_name) &&
                          hash_written == CBM_SHA256_HEX_LEN;
    g_test_size = size;
}

void cbm_ui_assets_reset_for_testing(void) {
    ui_snapshot_free(&g_snapshot);
    atomic_store_explicit(&g_state, CBM_UI_ASSETS_COLD, memory_order_release);
    atomic_store_explicit(&g_cancel_requested, false, memory_order_release);
    g_binary_path[0] = '\0';
    g_test_manifest_set = false;
    g_test_pack_name[0] = '\0';
    g_test_sha256[0] = '\0';
    g_test_size = 0U;
}
#endif
