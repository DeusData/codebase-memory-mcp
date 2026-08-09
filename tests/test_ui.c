/*
 * test_ui.c — Tests for the graph visualization UI module.
 *
 * Covers: config persistence, external asset-pack validation, layout engine.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "foundation/sha256.h"
#include "ui/asset_pack.h"
#include "ui/config.h"
#include "ui/layout3d.h"
#include "store/store.h"
#ifdef _WIN32
#include "foundation/win_utf8.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#include <sys/wait.h>
#endif

/* ── Config tests ─────────────────────────────────────────────── */

TEST(config_load_defaults) {
    /* Loading with no config file should give defaults */
    cbm_ui_config_t cfg;
    cfg.ui_enabled = true; /* set non-default to verify load overwrites */
    cfg.ui_port = 1234;

    /* Use a temp HOME to avoid touching real config */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    cbm_ui_config_load(&cfg);

    ASSERT_FALSE(cfg.ui_enabled);
    ASSERT_EQ(cfg.ui_port, 9749);

    /* Restore HOME */
    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_save_and_reload) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Save */
    cbm_ui_config_t cfg = {.ui_enabled = true, .ui_port = 8080};
    ASSERT_TRUE(cbm_ui_config_save(&cfg));

    /* Reload */
    cbm_ui_config_t loaded;
    cbm_ui_config_load(&loaded);

    ASSERT_TRUE(loaded.ui_enabled);
    ASSERT_EQ(loaded.ui_port, 8080);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_save_atomically_replaces_a_complete_generation) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_atomic_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_cache = getenv("CBM_CACHE_DIR") ? strdup(getenv("CBM_CACHE_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", td, 1), 0);

    cbm_ui_config_t old_generation = {
        .ui_enabled = false,
        .ui_port = 11111,
    };
    ASSERT_TRUE(cbm_ui_config_save(&old_generation));

    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));
#ifdef _WIN32
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    ASSERT_NOT_NULL(wide_path);
    HANDLE old_handle =
        CreateFileW(wide_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    free(wide_path);
    ASSERT_TRUE(old_handle != INVALID_HANDLE_VALUE);
#else
    FILE *old_handle = cbm_fopen(path, "rb");
    ASSERT_NOT_NULL(old_handle);
#endif

    cbm_ui_config_t new_generation = {
        .ui_enabled = true,
        .ui_port = 22222,
    };
    /* Capture everything, clean up, and only then assert: an assert firing
     * before the env restore leaks CBM_CACHE_DIR into every later test in the
     * process, turning one regression into a cascade. */
    bool saved = cbm_ui_config_save(&new_generation);

    char old_bytes[512] = {0};
#ifdef _WIN32
    DWORD old_length = 0;
    bool old_read =
        ReadFile(old_handle, old_bytes, (DWORD)sizeof(old_bytes) - 1U, &old_length, NULL) != 0 &&
        old_length > 0;
    bool old_closed = CloseHandle(old_handle) != 0;
#else
    size_t old_length = fread(old_bytes, 1, sizeof(old_bytes) - 1, old_handle);
    bool old_read = old_length > 0;
    bool old_closed = fclose(old_handle) == 0;
#endif

    cbm_ui_config_t loaded = {0};
    cbm_ui_config_load(&loaded);

    if (old_cache) {
        (void)cbm_setenv("CBM_CACHE_DIR", old_cache, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(old_cache);
    (void)th_rmtree(td);

    ASSERT_TRUE(saved);
    ASSERT_TRUE(old_read);
    ASSERT_TRUE(old_closed);
    /* An in-place truncate/rewrite mutates the already-open handle. Atomic
     * replacement leaves it attached to the complete prior generation. */
    ASSERT_NOT_NULL(strstr(old_bytes, "11111"));
    ASSERT_NULL(strstr(old_bytes, "22222"));
    ASSERT_TRUE(loaded.ui_enabled);
    ASSERT_EQ(loaded.ui_port, 22222);
    PASS();
}

TEST(config_overwrite) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Save with ui_enabled=true */
    cbm_ui_config_t cfg1 = {.ui_enabled = true, .ui_port = 9749};
    ASSERT_TRUE(cbm_ui_config_save(&cfg1));

    /* Overwrite with ui_enabled=false */
    cbm_ui_config_t cfg2 = {.ui_enabled = false, .ui_port = 9749};
    ASSERT_TRUE(cbm_ui_config_save(&cfg2));

    /* Reload should show false */
    cbm_ui_config_t loaded;
    cbm_ui_config_load(&loaded);
    ASSERT_FALSE(loaded.ui_enabled);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_corrupt_file) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Write garbage to config path */
    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));

    /* Ensure directory exists (portable — no system("mkdir -p")) */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/.cache/codebase-memory-mcp", td);
    cbm_mkdir_p(dir, 0755);

    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "this is not json!!!");
    fclose(f);

    /* Should load defaults, not crash */
    cbm_ui_config_t cfg;
    cbm_ui_config_load(&cfg);
    ASSERT_FALSE(cfg.ui_enabled);
    ASSERT_EQ(cfg.ui_port, 9749);

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

TEST(config_missing_fields) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_test_config_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    /* Write JSON with only ui_port */
    char path[1024];
    cbm_ui_config_path(path, (int)sizeof(path));

    char dir[1024];
    snprintf(dir, sizeof(dir), "%s/.cache/codebase-memory-mcp", td);
    cbm_mkdir_p(dir, 0755);

    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "{\"ui_port\": 5555}");
    fclose(f);

    cbm_ui_config_t cfg;
    cbm_ui_config_load(&cfg);
    ASSERT_FALSE(cfg.ui_enabled); /* defaults for missing field */
    ASSERT_EQ(cfg.ui_port, 5555); /* present field loaded */

    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }

    PASS();
}

/* ── External UI asset-pack tests ────────────────────────────── */

enum {
    TEST_UI_PACK_HEADER_BYTES = 80,
    TEST_UI_PACK_ENTRY_BYTES = 24,
};

static void ui_test_write_u16(unsigned char *bytes, uint16_t value) {
    bytes[0] = (unsigned char)value;
    bytes[1] = (unsigned char)(value >> 8U);
}

static void ui_test_write_u32(unsigned char *bytes, uint32_t value) {
    for (size_t index = 0U; index < sizeof(value); index++) {
        bytes[index] = (unsigned char)(value >> (index * 8U));
    }
}

static void ui_test_write_u64(unsigned char *bytes, uint64_t value) {
    for (size_t index = 0U; index < sizeof(value); index++) {
        bytes[index] = (unsigned char)(value >> (index * 8U));
    }
}

static unsigned char *ui_test_pack(size_t *length_out, bool malformed_index) {
    static const char asset_path[] = "/assets/app.js";
    static const char index_path[] = "/index.html";
    static const unsigned char asset_data[] = "console.log(1);";
    static const unsigned char index_data[] = "<h1>CBM</h1>";
    size_t paths_length = sizeof(asset_path) - 1U + sizeof(index_path) - 1U;
    size_t payload_length = sizeof(asset_data) - 1U + sizeof(index_data) - 1U;
    size_t index_length = TEST_UI_PACK_ENTRY_BYTES * 2U;
    size_t paths_offset = TEST_UI_PACK_HEADER_BYTES + index_length;
    size_t payload_offset = paths_offset + paths_length;
    size_t total = payload_offset + payload_length;
    unsigned char *pack = calloc(total, 1U);
    if (!pack) {
        return NULL;
    }
    pack[0] = 'C';
    pack[1] = 'B';
    pack[2] = 'M';
    pack[3] = 'U';
    pack[4] = 'I';
    pack[5] = 'P';
    pack[6] = 'K';
    ui_test_write_u16(pack + 8U, 1U);
    ui_test_write_u16(pack + 10U, TEST_UI_PACK_HEADER_BYTES);
    ui_test_write_u32(pack + 16U, 2U);
    ui_test_write_u32(pack + 20U, TEST_UI_PACK_ENTRY_BYTES);
    ui_test_write_u64(pack + 24U, TEST_UI_PACK_HEADER_BYTES);
    ui_test_write_u64(pack + 32U, index_length);
    ui_test_write_u64(pack + 40U, paths_offset);
    ui_test_write_u64(pack + 48U, paths_length);
    ui_test_write_u64(pack + 56U, payload_offset);
    ui_test_write_u64(pack + 64U, payload_length);
    ui_test_write_u64(pack + 72U, total);

    unsigned char *asset_entry = pack + TEST_UI_PACK_HEADER_BYTES;
    ui_test_write_u32(asset_entry, malformed_index ? 1U : 0U);
    ui_test_write_u16(asset_entry + 4U, sizeof(asset_path) - 1U);
    asset_entry[6U] = 2U;
    asset_entry[7U] = CBM_UI_ASSET_IMMUTABLE;
    ui_test_write_u64(asset_entry + 8U, 0U);
    ui_test_write_u64(asset_entry + 16U, sizeof(asset_data) - 1U);

    unsigned char *index_entry = asset_entry + TEST_UI_PACK_ENTRY_BYTES;
    ui_test_write_u32(index_entry, sizeof(asset_path) - 1U);
    ui_test_write_u16(index_entry + 4U, sizeof(index_path) - 1U);
    index_entry[6U] = 1U;
    index_entry[7U] = CBM_UI_ASSET_REVALIDATE;
    ui_test_write_u64(index_entry + 8U, sizeof(asset_data) - 1U);
    ui_test_write_u64(index_entry + 16U, sizeof(index_data) - 1U);

    memcpy(pack + paths_offset, asset_path, sizeof(asset_path) - 1U);
    memcpy(pack + paths_offset + sizeof(asset_path) - 1U, index_path, sizeof(index_path) - 1U);
    memcpy(pack + payload_offset, asset_data, sizeof(asset_data) - 1U);
    memcpy(pack + payload_offset + sizeof(asset_data) - 1U, index_data, sizeof(index_data) - 1U);
    *length_out = total;
    return pack;
}

static bool ui_test_publish_pack(const char *directory, unsigned char *pack, size_t length,
                                 char path_out[1024]) {
    char hash[CBM_SHA256_HEX_LEN + 1U];
    cbm_sha256_hex(pack, length, hash);
    char name[128];
    int name_length = snprintf(name, sizeof(name), "cbm-ui-%s.pack", hash);
    int path_length = snprintf(path_out, 1024U, "%s/%s", directory, name);
    if (name_length <= 0 || (size_t)name_length >= sizeof(name) || path_length <= 0 ||
        path_length >= 1024) {
        return false;
    }
    FILE *file = fopen(path_out, "wb");
    bool written = file && fwrite(pack, 1U, length, file) == length;
    if (file && fclose(file) != 0) {
        written = false;
    }
    if (written) {
        cbm_ui_assets_set_manifest_for_testing(name, hash, length);
    }
    return written;
}

static void ui_test_restore_assets_env(char *old_value) {
    if (old_value) {
        (void)cbm_setenv("CBM_UI_ASSETS_DIR", old_value, 1);
        free(old_value);
    } else {
        (void)cbm_unsetenv("CBM_UI_ASSETS_DIR");
    }
}

TEST(ui_asset_pack_validates_before_publication) {
    char directory[] = "/tmp/cbm_ui_pack_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(directory));
    size_t length = 0U;
    unsigned char *pack = ui_test_pack(&length, false);
    ASSERT_NOT_NULL(pack);
    char pack_path[1024];
    cbm_ui_assets_reset_for_testing();
    ASSERT_TRUE(ui_test_publish_pack(directory, pack, length, pack_path));
    free(pack);
    char *old_assets = getenv("CBM_UI_ASSETS_DIR") ? strdup(getenv("CBM_UI_ASSETS_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_UI_ASSETS_DIR", directory, 1), 0);

    char error[512];
    ASSERT_TRUE(cbm_ui_assets_supported());
    ASSERT_EQ(cbm_ui_assets_state(), CBM_UI_ASSETS_COLD);
    ASSERT_TRUE(cbm_ui_assets_warm(NULL, error, sizeof(error)));
    ASSERT_EQ(cbm_ui_assets_state(), CBM_UI_ASSETS_READY);
    const cbm_ui_asset_t *asset = cbm_ui_asset_lookup("/assets/app.js");
    ASSERT_NOT_NULL(asset);
    ASSERT_EQ(asset->cache, CBM_UI_ASSET_IMMUTABLE);
    ASSERT_STR_EQ(asset->content_type, "application/javascript; charset=utf-8");
    ASSERT_EQ(asset->size, sizeof("console.log(1);") - 1U);
    ASSERT_NULL(cbm_ui_asset_lookup("/api/processes"));

    cbm_ui_assets_reset_for_testing();
    ui_test_restore_assets_env(old_assets);
    (void)cbm_unlink(pack_path);
    (void)cbm_rmdir(directory);
    PASS();
}

TEST(ui_asset_pack_wrong_hash_fails_closed) {
    char directory[] = "/tmp/cbm_ui_hash_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(directory));
    size_t length = 0U;
    unsigned char *pack = ui_test_pack(&length, false);
    ASSERT_NOT_NULL(pack);
    char pack_path[1024];
    cbm_ui_assets_reset_for_testing();
    ASSERT_TRUE(ui_test_publish_pack(directory, pack, length, pack_path));
    pack[length - 1U] ^= 1U;
    FILE *file = fopen(pack_path, "wb");
    ASSERT_NOT_NULL(file);
    ASSERT_EQ(fwrite(pack, 1U, length, file), length);
    ASSERT_EQ(fclose(file), 0);
    free(pack);
    char *old_assets = getenv("CBM_UI_ASSETS_DIR") ? strdup(getenv("CBM_UI_ASSETS_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_UI_ASSETS_DIR", directory, 1), 0);

    char error[512];
    ASSERT_FALSE(cbm_ui_assets_warm(NULL, error, sizeof(error)));
    ASSERT_EQ(cbm_ui_assets_state(), CBM_UI_ASSETS_FAILED);
    ASSERT_NULL(cbm_ui_asset_lookup("/index.html"));

    cbm_ui_assets_reset_for_testing();
    ui_test_restore_assets_env(old_assets);
    (void)cbm_unlink(pack_path);
    (void)cbm_rmdir(directory);
    PASS();
}

TEST(ui_asset_pack_structural_corruption_fails_after_valid_hash) {
    char directory[] = "/tmp/cbm_ui_format_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(directory));
    size_t length = 0U;
    unsigned char *pack = ui_test_pack(&length, true);
    ASSERT_NOT_NULL(pack);
    char pack_path[1024];
    cbm_ui_assets_reset_for_testing();
    ASSERT_TRUE(ui_test_publish_pack(directory, pack, length, pack_path));
    free(pack);
    char *old_assets = getenv("CBM_UI_ASSETS_DIR") ? strdup(getenv("CBM_UI_ASSETS_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_UI_ASSETS_DIR", directory, 1), 0);

    char error[512];
    ASSERT_FALSE(cbm_ui_assets_warm(NULL, error, sizeof(error)));
    ASSERT_EQ(cbm_ui_assets_state(), CBM_UI_ASSETS_FAILED);
    ASSERT_NULL(cbm_ui_asset_lookup("/index.html"));

    cbm_ui_assets_reset_for_testing();
    ui_test_restore_assets_env(old_assets);
    (void)cbm_unlink(pack_path);
    (void)cbm_rmdir(directory);
    PASS();
}

/* A daemon can be asked to stop immediately after creating the warmup thread,
 * before that worker enters cbm_ui_assets_warm(). The cancellation must latch
 * across that scheduling window instead of being cleared by worker startup. */
TEST(ui_asset_pack_prestart_cancellation_is_not_lost) {
    char directory[] = "/tmp/cbm_ui_cancel_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(directory));
    size_t length = 0U;
    unsigned char *pack = ui_test_pack(&length, false);
    ASSERT_NOT_NULL(pack);
    char pack_path[1024];
    cbm_ui_assets_reset_for_testing();
    ASSERT_TRUE(ui_test_publish_pack(directory, pack, length, pack_path));
    free(pack);
    char *old_assets = getenv("CBM_UI_ASSETS_DIR") ? strdup(getenv("CBM_UI_ASSETS_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_UI_ASSETS_DIR", directory, 1), 0);

    cbm_ui_assets_request_cancel();
    char error[512];
    bool warmed = cbm_ui_assets_warm(NULL, error, sizeof(error));
    cbm_ui_assets_state_t state = cbm_ui_assets_state();

    cbm_ui_assets_reset_for_testing();
    ui_test_restore_assets_env(old_assets);
    (void)cbm_unlink(pack_path);
    (void)cbm_rmdir(directory);

    ASSERT_FALSE(warmed);
    ASSERT_EQ(state, CBM_UI_ASSETS_CANCELLED);
    PASS();
}

TEST(ui_asset_pack_install_never_follows_a_predictable_temp_symlink) {
#ifdef _WIN32
    SKIP_PLATFORM(
        "Windows reparse-point creation requires privileges; installer contracts cover it");
#else
    static const char sentinel_content[] = "foreign sentinel must survive\n";
    char source_directory[] = "/tmp/cbm_ui_install_source_XXXXXX";
    char install_directory[] = "/tmp/cbm_ui_install_target_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(source_directory));
    ASSERT_NOT_NULL(cbm_mkdtemp(install_directory));

    size_t pack_length = 0U;
    unsigned char *pack = ui_test_pack(&pack_length, false);
    ASSERT_NOT_NULL(pack);
    char source_pack[1024];
    cbm_ui_assets_reset_for_testing();
    ASSERT_TRUE(ui_test_publish_pack(source_directory, pack, pack_length, source_pack));
    free(pack);

    const char *pack_name = strrchr(source_pack, '/');
    ASSERT_NOT_NULL(pack_name);
    pack_name++;
    char installed_pack[1024];
    char old_temporary[1024];
    char sentinel_path[1024];
    ASSERT_LT(
        snprintf(installed_pack, sizeof(installed_pack), "%s/%s", install_directory, pack_name),
        (int)sizeof(installed_pack));
    ASSERT_LT(snprintf(old_temporary, sizeof(old_temporary), "%s.tmp", installed_pack),
              (int)sizeof(old_temporary));
    ASSERT_LT(snprintf(sentinel_path, sizeof(sentinel_path), "%s/foreign.txt", install_directory),
              (int)sizeof(sentinel_path));
    ASSERT_EQ(th_write_file(sentinel_path, sentinel_content), 0);
    ASSERT_EQ(symlink(sentinel_path, old_temporary), 0);

    char *old_assets = getenv("CBM_UI_ASSETS_DIR") ? strdup(getenv("CBM_UI_ASSETS_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_UI_ASSETS_DIR", source_directory, 1), 0);
    char error[512] = {0};
    bool installed = cbm_ui_assets_install(install_directory, false, error, sizeof(error));

    char sentinel_after[sizeof(sentinel_content)] = {0};
    FILE *sentinel_file = cbm_fopen(sentinel_path, "rb");
    size_t sentinel_length =
        sentinel_file ? fread(sentinel_after, 1U, sizeof(sentinel_after), sentinel_file) : 0U;
    bool sentinel_closed = !sentinel_file || fclose(sentinel_file) == 0;
    struct stat old_temp_info;
    bool old_temp_is_still_symlink =
        lstat(old_temporary, &old_temp_info) == 0 && S_ISLNK(old_temp_info.st_mode);
    cbm_path_info_t installed_info;
    bool installed_is_regular = cbm_path_info_utf8(installed_pack, &installed_info) == 0 &&
                                installed_info.is_regular && !installed_info.is_symlink;
    bool removed = cbm_ui_assets_remove(install_directory, false, error, sizeof(error));

    cbm_ui_assets_reset_for_testing();
    ui_test_restore_assets_env(old_assets);
    (void)cbm_unlink(old_temporary);
    (void)cbm_unlink(installed_pack);
    (void)cbm_unlink(sentinel_path);
    (void)cbm_unlink(source_pack);
    (void)cbm_rmdir(install_directory);
    (void)cbm_rmdir(source_directory);

    ASSERT_TRUE(installed);
    ASSERT_TRUE(sentinel_closed);
    ASSERT_EQ(sentinel_length, sizeof(sentinel_content) - 1U);
    ASSERT_MEM_EQ(sentinel_after, sentinel_content, sizeof(sentinel_content) - 1U);
    ASSERT_TRUE(old_temp_is_still_symlink);
    ASSERT_TRUE(installed_is_regular);
    ASSERT_TRUE(removed);
    PASS();
#endif
}

TEST(ui_asset_pack_install_remove_is_hash_bound_and_dry_run_safe) {
    char source_directory[] = "/tmp/cbm_ui_lifecycle_source_XXXXXX";
    char install_directory[] = "/tmp/cbm_ui_lifecycle_target_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(source_directory));
    ASSERT_NOT_NULL(cbm_mkdtemp(install_directory));

    size_t pack_length = 0U;
    unsigned char *pack = ui_test_pack(&pack_length, false);
    ASSERT_NOT_NULL(pack);
    char source_pack[1024];
    cbm_ui_assets_reset_for_testing();
    ASSERT_TRUE(ui_test_publish_pack(source_directory, pack, pack_length, source_pack));
    const char *pack_name = strrchr(source_pack, '/');
    ASSERT_NOT_NULL(pack_name);
    pack_name++;
    char installed_pack[1024];
    ASSERT_LT(
        snprintf(installed_pack, sizeof(installed_pack), "%s/%s", install_directory, pack_name),
        (int)sizeof(installed_pack));

    char *old_assets = getenv("CBM_UI_ASSETS_DIR") ? strdup(getenv("CBM_UI_ASSETS_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_UI_ASSETS_DIR", source_directory, 1), 0);
    char error[512] = {0};
    bool dry_install = cbm_ui_assets_install(install_directory, true, error, sizeof(error));
    cbm_path_info_t info;
    bool absent_after_dry_install = cbm_path_info_utf8(installed_pack, &info) != 0;
    bool installed = cbm_ui_assets_install(install_directory, false, error, sizeof(error));

    unsigned char *installed_bytes = malloc(pack_length);
    FILE *installed_file = cbm_fopen(installed_pack, "rb");
    size_t installed_length = installed_file && installed_bytes
                                  ? fread(installed_bytes, 1U, pack_length, installed_file)
                                  : 0U;
    bool installed_closed = !installed_file || fclose(installed_file) == 0;
    bool exact_copy = installed_bytes && installed_length == pack_length &&
                      memcmp(installed_bytes, pack, pack_length) == 0;
    free(installed_bytes);

    bool dry_remove = cbm_ui_assets_remove(install_directory, true, error, sizeof(error));
    bool present_after_dry_remove = cbm_path_info_utf8(installed_pack, &info) == 0;
    bool removed = cbm_ui_assets_remove(install_directory, false, error, sizeof(error));
    bool absent_after_remove = cbm_path_info_utf8(installed_pack, &info) != 0;

    bool reinstalled = cbm_ui_assets_install(install_directory, false, error, sizeof(error));
    FILE *tampered_file = cbm_fopen(installed_pack, "r+b");
    bool tampered = tampered_file && fseek(tampered_file, -1L, SEEK_END) == 0;
    int original_last_byte = tampered ? fgetc(tampered_file) : EOF;
    tampered = tampered && original_last_byte != EOF && fseek(tampered_file, -1L, SEEK_END) == 0 &&
               fputc(original_last_byte ^ 1, tampered_file) != EOF;
    if (tampered_file && fclose(tampered_file) != 0) {
        tampered = false;
    }
    bool refused_tampered_remove =
        !cbm_ui_assets_remove(install_directory, false, error, sizeof(error));
    bool tampered_preserved = cbm_path_info_utf8(installed_pack, &info) == 0;
    bool preservation_explained = strstr(error, "not owned; preserved") != NULL;

    cbm_ui_assets_reset_for_testing();
    ui_test_restore_assets_env(old_assets);
    (void)cbm_unlink(installed_pack);
    (void)cbm_unlink(source_pack);
    (void)cbm_rmdir(install_directory);
    (void)cbm_rmdir(source_directory);
    free(pack);

    ASSERT_TRUE(dry_install);
    ASSERT_TRUE(absent_after_dry_install);
    ASSERT_TRUE(installed);
    ASSERT_TRUE(installed_closed);
    ASSERT_TRUE(exact_copy);
    ASSERT_TRUE(dry_remove);
    ASSERT_TRUE(present_after_dry_remove);
    ASSERT_TRUE(removed);
    ASSERT_TRUE(absent_after_remove);
    ASSERT_TRUE(reinstalled);
    ASSERT_TRUE(tampered);
    ASSERT_TRUE(refused_tampered_remove);
    ASSERT_TRUE(tampered_preserved);
    ASSERT_TRUE(preservation_explained);
    PASS();
}

TEST(ui_asset_pack_staged_removal_is_owned_and_transactional) {
    char source_directory[] = "/tmp/cbm_ui_staged_remove_source_XXXXXX";
    char install_directory[] = "/tmp/cbm_ui_staged_remove_target_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(source_directory));
    ASSERT_NOT_NULL(cbm_mkdtemp(install_directory));
    size_t pack_length = 0U;
    unsigned char *pack = ui_test_pack(&pack_length, false);
    ASSERT_NOT_NULL(pack);
    char source_pack[1024];
    cbm_ui_assets_reset_for_testing();
    ASSERT_TRUE(ui_test_publish_pack(source_directory, pack, pack_length, source_pack));
    const char *pack_name = strrchr(source_pack, '/');
    ASSERT_NOT_NULL(pack_name);
    pack_name++;
    char installed_pack[1024];
    ASSERT_LT(
        snprintf(installed_pack, sizeof(installed_pack), "%s/%s", install_directory, pack_name),
        (int)sizeof(installed_pack));
    char *old_assets = getenv("CBM_UI_ASSETS_DIR") ? strdup(getenv("CBM_UI_ASSETS_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_UI_ASSETS_DIR", source_directory, 1), 0);
    char error[512] = {0};
    ASSERT_TRUE(cbm_ui_assets_install(install_directory, false, error, sizeof(error)));

    cbm_activation_transaction_t *removal = NULL;
    bool foreign = false;
    bool staged =
        cbm_ui_assets_stage_remove(install_directory, &removal, &foreign, error, sizeof(error));
    ASSERT_TRUE(staged);
    ASSERT_FALSE(foreign);
    ASSERT_NOT_NULL(removal);
    ASSERT_EQ(cbm_activation_transaction_commit(removal, NULL, NULL),
              CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_finalize(removal), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_EQ(cbm_activation_transaction_close(&removal), CBM_ACTIVATION_TRANSACTION_OK);
    cbm_path_info_t info;
    bool exact_removed = cbm_path_info_utf8(installed_pack, &info) != 0;

    ASSERT_TRUE(cbm_ui_assets_install(install_directory, false, error, sizeof(error)));
    FILE *tampered_file = cbm_fopen(installed_pack, "r+b");
    ASSERT_NOT_NULL(tampered_file);
    ASSERT_EQ(fseek(tampered_file, -1L, SEEK_END), 0);
    int last_byte = fgetc(tampered_file);
    ASSERT_TRUE(last_byte != EOF);
    ASSERT_EQ(fseek(tampered_file, -1L, SEEK_END), 0);
    ASSERT_TRUE(fputc(last_byte ^ 1, tampered_file) != EOF);
    ASSERT_EQ(fclose(tampered_file), 0);
    removal = NULL;
    foreign = false;
    bool foreign_staged =
        cbm_ui_assets_stage_remove(install_directory, &removal, &foreign, error, sizeof(error));
    bool foreign_preserved = cbm_path_info_utf8(installed_pack, &info) == 0;

    cbm_ui_assets_reset_for_testing();
    ui_test_restore_assets_env(old_assets);
    (void)cbm_activation_transaction_close(&removal);
    (void)cbm_unlink(installed_pack);
    (void)cbm_unlink(source_pack);
    (void)cbm_rmdir(install_directory);
    (void)cbm_rmdir(source_directory);
    free(pack);

    ASSERT_TRUE(exact_removed);
    ASSERT_TRUE(foreign_staged);
    ASSERT_TRUE(foreign);
    ASSERT_NULL(removal);
    ASSERT_TRUE(foreign_preserved);
    PASS();
}

/* File identity alone does not detect a truncate/rewrite through an already
 * open handle. Publication must validate the exact bytes at the public name,
 * and removal must validate the exact object after moving it to the private
 * retained name. Both mutations below preserve the inode on POSIX. */
TEST(ui_asset_pack_commit_rejects_in_place_rewrite_after_stage) {
    char source_directory[] = "/tmp/cbm_ui_commit_source_XXXXXX";
    char install_directory[] = "/tmp/cbm_ui_commit_target_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(source_directory));
    ASSERT_NOT_NULL(cbm_mkdtemp(install_directory));
    size_t pack_length = 0U;
    unsigned char *pack = ui_test_pack(&pack_length, false);
    ASSERT_NOT_NULL(pack);
    char source_pack[1024];
    cbm_ui_assets_reset_for_testing();
    ASSERT_TRUE(ui_test_publish_pack(source_directory, pack, pack_length, source_pack));
    const char *pack_name = strrchr(source_pack, '/');
    ASSERT_NOT_NULL(pack_name);
    pack_name++;
    char installed_pack[1024];
    ASSERT_LT(
        snprintf(installed_pack, sizeof(installed_pack), "%s/%s", install_directory, pack_name),
        (int)sizeof(installed_pack));
    char *old_assets = getenv("CBM_UI_ASSETS_DIR") ? strdup(getenv("CBM_UI_ASSETS_DIR")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_UI_ASSETS_DIR", source_directory, 1), 0);
    char error[512] = {0};
    ASSERT_TRUE(cbm_ui_assets_install(install_directory, false, error, sizeof(error)));
    ASSERT_TRUE(cbm_ui_assets_verify_file(installed_pack));

    cbm_activation_transaction_t *publication = NULL;
    ASSERT_TRUE(
        cbm_ui_assets_stage_install(install_directory, &publication, error, sizeof(error)));
    ASSERT_NOT_NULL(publication);
    const char *staged = cbm_activation_transaction_staged_path(publication);
    ASSERT_NOT_NULL(staged);
    FILE *staged_file = cbm_fopen(staged, "r+b");
    ASSERT_NOT_NULL(staged_file);
    ASSERT_EQ(fseek(staged_file, -1L, SEEK_END), 0);
    int staged_last = fgetc(staged_file);
    ASSERT_TRUE(staged_last != EOF);
    ASSERT_EQ(fseek(staged_file, -1L, SEEK_END), 0);
    ASSERT_TRUE(fputc(staged_last ^ 1, staged_file) != EOF);
    ASSERT_EQ(fclose(staged_file), 0);
    ASSERT_EQ(cbm_ui_assets_commit_install(publication),
              CBM_ACTIVATION_TRANSACTION_VALIDATION_FAILED);
    ASSERT_EQ(cbm_activation_transaction_close(&publication), CBM_ACTIVATION_TRANSACTION_OK);
    ASSERT_TRUE(cbm_ui_assets_verify_file(installed_pack));

    cbm_activation_transaction_t *removal = NULL;
    bool foreign = false;
    ASSERT_TRUE(cbm_ui_assets_stage_remove(install_directory, &removal, &foreign, error,
                                           sizeof(error)));
    ASSERT_FALSE(foreign);
    ASSERT_NOT_NULL(removal);
    FILE *target_file = cbm_fopen(installed_pack, "r+b");
    ASSERT_NOT_NULL(target_file);
    ASSERT_EQ(fseek(target_file, -1L, SEEK_END), 0);
    int target_last = fgetc(target_file);
    ASSERT_TRUE(target_last != EOF);
    ASSERT_EQ(fseek(target_file, -1L, SEEK_END), 0);
    ASSERT_TRUE(fputc(target_last ^ 1, target_file) != EOF);
    ASSERT_EQ(fclose(target_file), 0);
    ASSERT_EQ(cbm_ui_assets_commit_removal(removal),
              CBM_ACTIVATION_TRANSACTION_VALIDATION_FAILED);
    ASSERT_EQ(cbm_activation_transaction_close(&removal), CBM_ACTIVATION_TRANSACTION_OK);
    cbm_path_info_t preserved_info;
    ASSERT_EQ(cbm_path_info_utf8(installed_pack, &preserved_info), 0);
    ASSERT_TRUE(preserved_info.is_regular);
    ASSERT_FALSE(cbm_ui_assets_verify_file(installed_pack));

    cbm_ui_assets_reset_for_testing();
    ui_test_restore_assets_env(old_assets);
    (void)cbm_unlink(installed_pack);
    (void)cbm_unlink(source_pack);
    (void)cbm_rmdir(install_directory);
    (void)cbm_rmdir(source_directory);
    free(pack);
    PASS();
}

TEST(ui_asset_standard_manifest_has_no_capability) {
    cbm_ui_assets_reset_for_testing();
    ASSERT_FALSE(cbm_ui_assets_supported());
    ASSERT_EQ(cbm_ui_assets_state(), CBM_UI_ASSETS_UNAVAILABLE);
    ASSERT_NULL(cbm_ui_asset_lookup("/index.html"));
    PASS();
}

/* ── Layout tests ─────────────────────────────────────────────── */

TEST(layout_empty_graph) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    /* No nodes in store → empty result */
    cbm_layout_result_t *r =
        cbm_layout_compute(store, "test-project", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 0);
    ASSERT_EQ(r->edge_count, 0);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_single_node) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");
    cbm_node_t node = {
        .project = "test",
        .label = "Function",
        .name = "main",
        .qualified_name = "test::main",
        .file_path = "main.c",
        .start_line = 1,
        .end_line = 10,
    };
    int64_t id = cbm_store_upsert_node(store, &node);
    ASSERT_GT(id, 0);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 1);
    ASSERT_STR_EQ(r->nodes[0].name, "main");
    ASSERT_EQ(r->total_nodes, 1);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_two_connected) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "foo",
                     .qualified_name = "test::foo",
                     .file_path = "a.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "bar",
                     .qualified_name = "test::bar",
                     .file_path = "b.c",
                     .start_line = 1,
                     .end_line = 5};
    int64_t id1 = cbm_store_upsert_node(store, &n1);
    int64_t id2 = cbm_store_upsert_node(store, &n2);

    cbm_edge_t edge = {.project = "test", .source_id = id1, .target_id = id2, .type = "CALLS"};
    cbm_store_insert_edge(store, &edge);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, 2);

    /* Nodes should be positioned apart (not at same point) */
    float dx = r->nodes[0].x - r->nodes[1].x;
    float dy = r->nodes[0].y - r->nodes[1].y;
    float dz = r->nodes[0].z - r->nodes[1].z;
    float dist = sqrtf(dx * dx + dy * dy + dz * dz);
    ASSERT_GT((long long)(dist * 100), 0);

    ASSERT_EQ(r->edge_count, 1);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_respects_max_nodes) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    /* Insert 20 nodes */
    for (int i = 0; i < 20; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::fn%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "a.c",
                        .start_line = i,
                        .end_line = i + 1};
        cbm_store_upsert_node(store, &n);
    }

    /* max_nodes=5 should return at most 5 */
    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 5);
    ASSERT_NOT_NULL(r);
    ASSERT_LTE(r->node_count, 5);
    ASSERT_EQ(r->total_nodes, 20);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_clamps_render_cap_from_env) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    const char *old_raw = getenv("CBM_UI_MAX_RENDER_NODES");
    char *old_cap = old_raw ? strdup(old_raw) : NULL;
    cbm_setenv("CBM_UI_MAX_RENDER_NODES", "25", 1);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    for (int i = 0; i < 40; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::fn%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "a.c",
                        .start_line = i,
                        .end_line = i + 1};
        cbm_store_upsert_node(store, &n);
    }

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 50000);
    ASSERT_NOT_NULL(r);
    ASSERT_LTE(r->node_count, 25);
    ASSERT_EQ(r->total_nodes, 40);

    cbm_layout_free(r);
    cbm_store_close(store);
    if (old_cap) {
        cbm_setenv("CBM_UI_MAX_RENDER_NODES", old_cap, 1);
        free(old_cap);
    } else {
        cbm_unsetenv("CBM_UI_MAX_RENDER_NODES");
    }
    PASS();
}

/* A caller-requested budget above the default must be honored (up to the hard
 * ceiling) when no env cap is set — the default is a default, not a ceiling. */
TEST(layout_honors_budget_above_default) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    const char *old_raw = getenv("CBM_UI_MAX_RENDER_NODES");
    char *old_cap = old_raw ? strdup(old_raw) : NULL;
    cbm_unsetenv("CBM_UI_MAX_RENDER_NODES");

    cbm_store_upsert_project(store, "test", "/tmp/test");

    enum { BUDGET_NODES = 5100 };
    for (int i = 0; i < BUDGET_NODES; i++) {
        char name[32], qn[64];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::fn%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "a.c",
                        .start_line = i,
                        .end_line = i + 1};
        cbm_store_upsert_node(store, &n);
    }

    cbm_layout_result_t *r =
        cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, BUDGET_NODES);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->node_count, BUDGET_NODES);
    ASSERT_EQ(r->total_nodes, BUDGET_NODES);

    cbm_layout_free(r);
    cbm_store_close(store);
    if (old_cap) {
        cbm_setenv("CBM_UI_MAX_RENDER_NODES", old_cap, 1);
        free(old_cap);
    }
    PASS();
}

TEST(layout_deterministic) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n1 = {.project = "test",
                     .label = "Function",
                     .name = "alpha",
                     .qualified_name = "test::alpha",
                     .file_path = "a.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_node_t n2 = {.project = "test",
                     .label = "Function",
                     .name = "beta",
                     .qualified_name = "test::beta",
                     .file_path = "b.c",
                     .start_line = 1,
                     .end_line = 5};
    cbm_store_upsert_node(store, &n1);
    cbm_store_upsert_node(store, &n2);

    /* Run twice, check positions match */
    cbm_layout_result_t *r1 = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    cbm_layout_result_t *r2 = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(r2);
    ASSERT_EQ(r1->node_count, r2->node_count);

    for (int i = 0; i < r1->node_count; i++) {
        ASSERT_FLOAT_EQ(r1->nodes[i].x, r2->nodes[i].x, 0.001);
        ASSERT_FLOAT_EQ(r1->nodes[i].y, r2->nodes[i].y, 0.001);
        ASSERT_FLOAT_EQ(r1->nodes[i].z, r2->nodes[i].z, 0.001);
    }

    cbm_layout_free(r1);
    cbm_layout_free(r2);
    cbm_store_close(store);
    PASS();
}

TEST(layout_to_json) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);

    cbm_store_upsert_project(store, "test", "/tmp/test");

    cbm_node_t n = {.project = "test",
                    .label = "Function",
                    .name = "hello",
                    .qualified_name = "test::hello",
                    .file_path = "a.c",
                    .start_line = 1,
                    .end_line = 5};
    cbm_store_upsert_node(store, &n);

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);

    char *json = cbm_layout_to_json(r);
    ASSERT_NOT_NULL(json);

    /* Should contain key fields */
    ASSERT(strstr(json, "\"nodes\"") != NULL);
    ASSERT(strstr(json, "\"edges\"") != NULL);
    ASSERT(strstr(json, "\"total_nodes\"") != NULL);
    ASSERT(strstr(json, "\"hello\"") != NULL);
    ASSERT(strstr(json, "\"Function\"") != NULL);

    free(json);
    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

TEST(layout_null_inputs) {
    /* NULL store → NULL result */
    cbm_layout_result_t *r = cbm_layout_compute(NULL, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NULL(r);

    /* NULL project → NULL result */
    cbm_store_t *store = cbm_store_open_memory();
    r = cbm_layout_compute(store, NULL, CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NULL(r);

    /* cbm_layout_free(NULL) should not crash */
    cbm_layout_free(NULL);

    /* cbm_layout_to_json(NULL) should return NULL */
    char *json = cbm_layout_to_json(NULL);
    ASSERT_NULL(json);

    cbm_store_close(store);
    PASS();
}

/* ── Dead-code classification (distilled from PR #789) ────────── */

static const cbm_layout_node_t *find_layout_node(const cbm_layout_result_t *r, const char *name) {
    for (int i = 0; i < r->node_count; i++) {
        if (r->nodes[i].name && strcmp(r->nodes[i].name, name) == 0) {
            return &r->nodes[i];
        }
    }
    return NULL;
}

/* A function with zero callers/usages and no entry/test/exported flag is
 * "dead"; entry-point, test, and exported functions are NOT dead even at zero
 * callers; a called function reports its true full-graph incoming CALLS degree
 * ("single" at 1, "normal" at >=2). Non-Function labels are "structural". */
TEST(layout_dead_code_classification) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "dc", "/tmp/dc"), CBM_STORE_OK);

    /* Candidates (Function, non-test path unless noted). */
    cbm_node_t dead = {.project = "dc",
                       .label = "Function",
                       .name = "deadfn",
                       .qualified_name = "dc::deadfn",
                       .file_path = "src/a.c",
                       .properties_json = "{\"is_entry_point\":false,\"is_test\":false,"
                                          "\"is_exported\":false}"};
    cbm_node_t entry = {.project = "dc",
                        .label = "Function",
                        .name = "entryfn",
                        .qualified_name = "dc::entryfn",
                        .file_path = "src/b.c",
                        .properties_json = "{\"is_entry_point\":true}"};
    cbm_node_t tst = {.project = "dc",
                      .label = "Function",
                      .name = "testfn",
                      .qualified_name = "dc::testfn",
                      .file_path = "src/c.c",
                      .properties_json = "{\"is_test\":true}"};
    cbm_node_t tstpath = {.project = "dc",
                          .label = "Function",
                          .name = "bypathfn",
                          .qualified_name = "dc::bypathfn",
                          .file_path = "tests/mod_helpers.c",
                          .properties_json = "{}"};
    cbm_node_t exp = {.project = "dc",
                      .label = "Function",
                      .name = "exportedfn",
                      .qualified_name = "dc::exportedfn",
                      .file_path = "src/d.c",
                      .properties_json = "{\"is_exported\":true}"};
    cbm_node_t single = {.project = "dc",
                         .label = "Function",
                         .name = "calledonce",
                         .qualified_name = "dc::calledonce",
                         .file_path = "src/e.c",
                         .properties_json = "{}"};
    cbm_node_t norm = {.project = "dc",
                       .label = "Function",
                       .name = "callednormal",
                       .qualified_name = "dc::callednormal",
                       .file_path = "src/f.c",
                       .properties_json = "{}"};
    cbm_node_t caller = {.project = "dc",
                         .label = "Function",
                         .name = "caller",
                         .qualified_name = "dc::caller",
                         .file_path = "src/g.c",
                         .properties_json = "{}"};
    /* A structural (non-Function) node is never a dead-code candidate. */
    cbm_node_t cls = {.project = "dc",
                      .label = "Class",
                      .name = "SomeClass",
                      .qualified_name = "dc::SomeClass",
                      .file_path = "src/h.c",
                      .properties_json = "{}"};

    int64_t id_dead = cbm_store_upsert_node(store, &dead);
    cbm_store_upsert_node(store, &entry);
    cbm_store_upsert_node(store, &tst);
    cbm_store_upsert_node(store, &tstpath);
    cbm_store_upsert_node(store, &exp);
    int64_t id_single = cbm_store_upsert_node(store, &single);
    int64_t id_norm = cbm_store_upsert_node(store, &norm);
    int64_t id_caller = cbm_store_upsert_node(store, &caller);
    cbm_store_upsert_node(store, &cls);
    ASSERT_GT(id_dead, 0);

    /* calledonce ← 1 CALLS; callednormal ← 2 CALLS (full-graph inbound). */
    cbm_edge_t e1 = {
        .project = "dc", .source_id = id_caller, .target_id = id_single, .type = "CALLS"};
    cbm_edge_t e2 = {
        .project = "dc", .source_id = id_caller, .target_id = id_norm, .type = "CALLS"};
    cbm_edge_t e3 = {.project = "dc", .source_id = id_dead, .target_id = id_norm, .type = "CALLS"};
    cbm_store_insert_edge(store, &e1);
    cbm_store_insert_edge(store, &e2);
    cbm_store_insert_edge(store, &e3);

    cbm_layout_result_t *r = cbm_layout_compute(store, "dc", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    ASSERT_NOT_NULL(r);

    const cbm_layout_node_t *ln;

    ln = find_layout_node(r, "deadfn");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "dead");
    ASSERT_EQ(ln->in_calls, 0);

    ln = find_layout_node(r, "entryfn");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "entry");

    ln = find_layout_node(r, "testfn");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "test");

    ln = find_layout_node(r, "bypathfn"); /* test detected via file path */
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "test");

    ln = find_layout_node(r, "exportedfn");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "exported");

    ln = find_layout_node(r, "calledonce");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "single");
    ASSERT_EQ(ln->in_calls, 1);

    ln = find_layout_node(r, "callednormal");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "normal");
    ASSERT_EQ(ln->in_calls, 2);

    ln = find_layout_node(r, "SomeClass");
    ASSERT_NOT_NULL(ln);
    ASSERT_STR_EQ(ln->status, "structural");

    /* The classification must survive JSON serialization. */
    char *json = cbm_layout_to_json(r);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"status\":\"dead\"") != NULL);
    ASSERT(strstr(json, "\"in_calls\":2") != NULL);
    free(json);

    cbm_layout_free(r);
    cbm_store_close(store);
    PASS();
}

/* ── Octree recursion guard (distilled from PR #821; refs #498/#726/#402) ── */

/* Bodies that share a position made octree_insert subdivide forever — the
 * cell around them shrinks but never separates them, so one octree cell is
 * calloc'd per level until the process dies (stack overflow) or freezes the
 * machine allocating (the 34GB-swap reports). Fixed by the depth/half-size
 * floor in src/ui/layout3d.c (OCTREE_MAX_DEPTH / OCTREE_MIN_HALF).
 *
 * Coincident positions are reachable through the public layout API: layout3d
 * anchors each node by fnv1a(file cluster key) and jitters it with a PRNG
 * seeded by fnv1a(qualified_name). The three QNs below are distinct strings
 * with IDENTICAL 32-bit FNV-1a hashes (0x06bb012e, found by offline brute
 * force), so in the same file they get bit-identical positions on every
 * platform (integer hashing only — no libm in the coincidence path).
 *
 * A literal sub-ULP-separated pair cannot be constructed through the public
 * API: same-anchor positions are quantized to exact multiples of the jitter
 * quantum (5/4096 — exactly 20 ULP at anchor magnitude ~600), and
 * cross-anchor separations depend on the platform's cosf/sinf bits. Exact
 * coincidence is the API-reachable degenerate input, and it necessarily
 * drives the recursion through the sub-ULP regime: half_size falls below
 * ULP(center) with the bodies still unseparated, freezing child centers
 * while cells keep being allocated.
 */
#if !defined(_WIN32)
/* Child body: builds the store and runs the layout so a crash or hang cannot
 * take down the runner (alarm bounds a hang, fork isolates a SIGSEGV).
 * Deliberately NO memory rlimit: under a rlimit a failing calloc makes
 * octree_insert silently truncate and the UNFIXED code would complete —
 * turning this guard vacuously green. The alarm alone bounds the runaway.
 * Exit codes: 0 ok, 2 store setup, 3 layout NULL, 4 node count/lookup,
 * 5 fixture no longer coincident, 6 non-finite coordinate. Never returns. */
static void layout_octree_guard_child(void) {
    alarm(5); /* post-fix the whole child runs in milliseconds */
    cbm_store_t *store = cbm_store_open_memory();
    if (!store)
        _exit(2);
    if (cbm_store_upsert_project(store, "test", "/tmp/test") != CBM_STORE_OK)
        _exit(2);

    /* Distinct QNs, one fnv1a hash — coincident after anchor + jitter. */
    static const char *cqn[3] = {"test::octree_c5988474", "test::octree_c11394919",
                                 "test::octree_c33141700"};
    for (int i = 0; i < 3; i++) {
        char name[32];
        snprintf(name, sizeof(name), "co%d", i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = cqn[i],
                        .file_path = "pkg/sub/mod/a.c",
                        .start_line = i + 1,
                        .end_line = i + 2};
        if (cbm_store_upsert_node(store, &n) <= 0)
            _exit(2);
    }
    /* A few normally-spread nodes so the octree root box has realistic
     * (non-degenerate) extent, as in the reported repositories. */
    for (int i = 0; i < 3; i++) {
        char name[32], qn[64], fp[32];
        snprintf(name, sizeof(name), "fn%d", i);
        snprintf(qn, sizeof(qn), "test::spread_fn%d", i);
        snprintf(fp, sizeof(fp), "dir%d/f%d.c", i, i);
        cbm_node_t n = {.project = "test",
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = fp,
                        .start_line = 1,
                        .end_line = 2};
        if (cbm_store_upsert_node(store, &n) <= 0)
            _exit(2);
    }

    cbm_layout_result_t *r = cbm_layout_compute(store, "test", CBM_LAYOUT_OVERVIEW, NULL, 0, 100);
    if (!r)
        _exit(3);
    if (r->node_count != 6)
        _exit(4);

    /* The colliding QNs must actually be coincident — identical output
     * coordinates (identical seeds → identical positions, and coincident
     * bodies receive identical forces every iteration, so they stay
     * together). If a seeding change ever breaks this, the fixture no longer
     * reproduces the bug: fail loudly instead of going vacuously green. */
    int ci[3], nc = 0;
    for (int i = 0; i < r->node_count && nc < 3; i++) {
        if (r->nodes[i].qualified_name &&
            strncmp(r->nodes[i].qualified_name, "test::octree_c", 14) == 0)
            ci[nc++] = i;
    }
    if (nc != 3)
        _exit(4);
    for (int k = 1; k < 3; k++) {
        if (r->nodes[ci[k]].x != r->nodes[ci[0]].x || r->nodes[ci[k]].y != r->nodes[ci[0]].y ||
            r->nodes[ci[k]].z != r->nodes[ci[0]].z)
            _exit(5);
    }
    for (int i = 0; i < r->node_count; i++) {
        if (!isfinite(r->nodes[i].x) || !isfinite(r->nodes[i].y) || !isfinite(r->nodes[i].z))
            _exit(6);
    }

    cbm_layout_free(r);
    cbm_store_close(store);
    _exit(0);
}
#endif

TEST(layout_coincident_nodes_bounded) {
#if defined(_WIN32)
    SKIP_PLATFORM("fork/alarm not available; POSIX-only bounded-hang reproduction");
#else
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0)
        FAIL("fork() failed");
    if (pid == 0)
        layout_octree_guard_child(); /* never returns */

    int status = 0;
    (void)waitpid(pid, &status, 0);

    /* Unfixed code dies here: SIGSEGV (unbounded recursion overflowing the
     * stack) or SIGALRM (tail-call-optimized allocation runaway cut off by
     * the child's alarm). Fixed code exits 0 well within the budget. */
    ASSERT_FALSE(WIFSIGNALED(status));
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    PASS();
#endif
}

/* ── Suite ────────────────────────────────────────────────────── */

SUITE(ui) {
    /* Config */
    RUN_TEST(config_load_defaults);
    RUN_TEST(config_save_and_reload);
    RUN_TEST(config_save_atomically_replaces_a_complete_generation);
    RUN_TEST(config_overwrite);
    RUN_TEST(config_corrupt_file);
    RUN_TEST(config_missing_fields);

    /* External UI asset pack */
    RUN_TEST(ui_asset_pack_validates_before_publication);
    RUN_TEST(ui_asset_pack_wrong_hash_fails_closed);
    RUN_TEST(ui_asset_pack_structural_corruption_fails_after_valid_hash);
    RUN_TEST(ui_asset_pack_prestart_cancellation_is_not_lost);
    RUN_TEST(ui_asset_pack_install_never_follows_a_predictable_temp_symlink);
    RUN_TEST(ui_asset_pack_install_remove_is_hash_bound_and_dry_run_safe);
    RUN_TEST(ui_asset_pack_staged_removal_is_owned_and_transactional);
    RUN_TEST(ui_asset_pack_commit_rejects_in_place_rewrite_after_stage);
    RUN_TEST(ui_asset_standard_manifest_has_no_capability);

    /* Layout engine */
    RUN_TEST(layout_empty_graph);
    RUN_TEST(layout_single_node);
    RUN_TEST(layout_two_connected);
    RUN_TEST(layout_respects_max_nodes);
    RUN_TEST(layout_clamps_render_cap_from_env);
    RUN_TEST(layout_honors_budget_above_default);
    RUN_TEST(layout_deterministic);
    RUN_TEST(layout_to_json);
    RUN_TEST(layout_null_inputs);
    RUN_TEST(layout_dead_code_classification);
    RUN_TEST(layout_coincident_nodes_bounded);
}
