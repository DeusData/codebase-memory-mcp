/*
 * test_cli.c — Tests for CLI subcommands: install, uninstall, update, version.
 *
 * Port of Go test files:
 *   - cmd/codebase-memory-mcp/cli_test.go (11 tests)
 *   - cmd/codebase-memory-mcp/install_test.go (24 tests)
 *   - cmd/codebase-memory-mcp/update_test.go (5 tests)
 *   - internal/selfupdate/selfupdate_test.go (7 tests)
 *
 * Total: 47 Go tests → 47 C tests
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/constants.h"
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>
#include <depindex/depindex.h>
#include <mcp/mcp.h>
#include <foundation/yaml.h>
#include <pagerank/pagerank.h>
#include <pipeline/pipeline.h>
#include <store/store.h>
#include <yyjson/yyjson.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#ifndef _WIN32
#include <sys/wait.h>
#endif
#include <errno.h>
#include <zlib.h>

/* Helper: create a file with content */
static int write_test_file(const char *path, const char *content) {
    FILE *f = cbm_fopen(path, "wb");
    if (!f)
        return -1;
    fprintf(f, "%s", content);
    fclose(f);
    return 0;
}

/* Helper: read a file into static buffer */
static const char *read_test_file(const char *path) {
    static char buf[8192];
    FILE *f = fopen(path, "r");
    if (!f)
        return NULL;
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

static char *read_test_file_alloc(const char *path) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long len = ftell(f);
    if (len < 0 || fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return NULL;
    }
    char *buf = malloc((size_t)len + 1U);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t read_len = fread(buf, 1, (size_t)len, f);
    fclose(f);
    if (read_len != (size_t)len) {
        free(buf);
        return NULL;
    }
    buf[read_len] = '\0';
    return buf;
}

static char *save_test_env(const char *name) {
    const char *value = getenv(name);
    return value ? cbm_strndup(value, strlen(value)) : NULL;
}

static void restore_test_env(const char *name, char *saved) {
    if (saved) {
        cbm_setenv(name, saved, 1);
        free(saved);
    } else {
        cbm_unsetenv(name);
    }
}

static size_t count_substr(const char *s, const char *needle) {
    size_t count = 0;
    if (!s || !needle) {
        return 0;
    }
    size_t needle_len = strlen(needle);
    if (needle_len == 0) {
        return 0;
    }
    while ((s = strstr(s, needle)) != NULL) {
        count++;
        s += needle_len;
    }
    return count;
}

#define test_count_substring count_substr

typedef struct {
    const char *name;
    char *value;
} cli_env_snapshot_t;

static bool cli_env_snapshot(cli_env_snapshot_t *snap, const char *name) {
    const char *value = getenv(name);
    snap->name = name;
    snap->value = value ? cbm_strndup(value, strlen(value)) : NULL;
    return !value || snap->value;
}

static void cli_env_restore(cli_env_snapshot_t *snap) {
    if (!snap->name) {
        return;
    }
    if (snap->value) {
        cbm_setenv(snap->name, snap->value, 1);
        free(snap->value);
        snap->value = NULL;
    } else {
        cbm_unsetenv(snap->name);
    }
}

static int cli_run_help_without_home(int (*cmd)(int, char **)) {
    cli_env_snapshot_t home = {0};
    cli_env_snapshot_t userprofile = {0};
    if (!cli_env_snapshot(&home, "HOME") || !cli_env_snapshot(&userprofile, "USERPROFILE")) {
        cli_env_restore(&userprofile);
        cli_env_restore(&home);
        return -1;
    }

    cbm_unsetenv("HOME");
    cbm_unsetenv("USERPROFILE");
    char *args[] = {"--help"};
    int rc = cmd(1, args);

    cli_env_restore(&userprofile);
    cli_env_restore(&home);
    return rc;
}

/* Helper: mkdirp */
static int test_mkdirp(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            cbm_mkdir(tmp);
            *p = '/';
        }
    }
    return cbm_mkdir(tmp) == 0 || errno == EEXIST ? 0 : -1;
}

static char *make_overlong_nested_path(const char *base, const char *leaf) {
    size_t cap = (size_t)CBM_PATH_MAX * 2;
    char *path = malloc(cap);
    if (!path) {
        return NULL;
    }
    int n = snprintf(path, cap, "%s", base);
    if (n < 0 || (size_t)n >= cap) {
        free(path);
        return NULL;
    }
    size_t used = (size_t)n;
    const char *segment = "/a";
    size_t segment_len = strlen(segment);
    while (used <= (size_t)CBM_PATH_MAX) {
        if (used + segment_len >= cap) {
            free(path);
            return NULL;
        }
        memcpy(path + used, segment, segment_len + 1);
        used += segment_len;
    }
    size_t leaf_len = strlen(leaf);
    const char *separator = "/";
    size_t separator_len = strlen(separator);
    if (used + separator_len + leaf_len >= cap) {
        free(path);
        return NULL;
    }
    memcpy(path + used, separator, separator_len + 1);
    used += separator_len;
    if (leaf_len > 0) {
        memcpy(path + used, leaf, leaf_len + 1);
    } else {
        path[used] = '\0';
    }
    return path;
}

static int test_path_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

/* Helper: recursive remove */
static void test_rmdir_r(const char *path) {
    th_rmtree(path);
}

/* Helper: create tar.gz with a single file */
static unsigned char *create_test_targz(const char *filename, const unsigned char *content,
                                        int content_len, int *out_len) {
    /* Build tar data: 512-byte header + content padded to 512-byte boundary + 2x512 zero blocks */
    int data_blocks = (content_len + 511) / 512;
    int tar_size = 512 + data_blocks * 512 + 1024; /* header + data + end-of-archive */
    unsigned char *tar = calloc(1, (size_t)tar_size);
    if (!tar)
        return NULL;

    /* Filename (bytes 0-99) */
    strncpy((char *)tar, filename, 99);

    /* Mode (bytes 100-107): octal 0700 */
    memcpy(tar + 100, "0000700\0", 8);

    /* UID/GID (bytes 108-123): 0 */
    memcpy(tar + 108, "0000000\0", 8);
    memcpy(tar + 116, "0000000\0", 8);

    /* Size (bytes 124-135): octal */
    char size_str[12];
    snprintf(size_str, sizeof(size_str), "%011o", content_len);
    memcpy(tar + 124, size_str, 11);

    /* Mtime (bytes 136-147): 0 */
    memcpy(tar + 136, "00000000000\0", 12);

    /* Type flag (byte 156): '0' = regular file */
    tar[156] = '0';

    /* Checksum (bytes 148-155): compute over header with checksum field as spaces */
    memset(tar + 148, ' ', 8);
    unsigned int checksum = 0;
    for (int i = 0; i < 512; i++)
        checksum += tar[i];
    snprintf((char *)tar + 148, 7, "%06o", checksum);
    tar[154] = '\0';

    /* File content */
    memcpy(tar + 512, content, (size_t)content_len);

    /* Compress with gzip */
    z_stream strm = {0};
    if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 16 + MAX_WBITS, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK) {
        free(tar);
        return NULL;
    }

    size_t gz_cap = (size_t)tar_size + 256;
    unsigned char *gz = malloc(gz_cap);
    if (!gz) {
        deflateEnd(&strm);
        free(tar);
        return NULL;
    }

    strm.next_in = tar;
    strm.avail_in = (unsigned int)tar_size;
    strm.next_out = gz;
    strm.avail_out = (unsigned int)gz_cap;

    deflate(&strm, Z_FINISH);
    *out_len = (int)(gz_cap - strm.avail_out);

    deflateEnd(&strm);
    free(tar);
    return gz;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Version comparison tests (port of selfupdate_test.go)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_compare_versions) {
    /* Port of TestCompareVersions — 13 cases */
    ASSERT(cbm_compare_versions("0.2.1", "0.2.0") > 0);
    ASSERT_EQ(cbm_compare_versions("0.2.0", "0.2.0"), 0);
    ASSERT(cbm_compare_versions("0.1.9", "0.2.0") < 0);
    ASSERT(cbm_compare_versions("0.10.0", "0.2.0") > 0);
    ASSERT(cbm_compare_versions("1.0.0", "0.99.99") > 0);
    ASSERT(cbm_compare_versions("0.0.1", "0.0.2") < 0);
    ASSERT_EQ(cbm_compare_versions("v0.2.1", "0.2.1"), 0);
    ASSERT_EQ(cbm_compare_versions("0.2.1", "v0.2.1"), 0);
    ASSERT(cbm_compare_versions("0.2.1-dev", "0.2.1") < 0);
    ASSERT(cbm_compare_versions("0.2.1", "0.2.1-dev") > 0);
    ASSERT_EQ(cbm_compare_versions("0.2.1-dev", "0.2.1-dev"), 0);
    ASSERT(cbm_compare_versions("0.3.0", "0.2.1-dev") > 0);
    ASSERT(cbm_compare_versions("0.2.0", "0.2.1-dev") < 0);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Version get/set (port of TestCLI_Version)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_version_get_set) {
    cbm_cli_set_version("1.2.3");
    ASSERT_STR_EQ(cbm_cli_get_version(), "1.2.3");
    cbm_cli_set_version("dev");
    ASSERT_STR_EQ(cbm_cli_get_version(), "dev");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Shell RC detection (port of TestDetectShellRC + BashWithBashrc)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_detect_shell_rc_zsh) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-rc-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Save and override SHELL — must strdup because setenv may realloc env block */
    const char *raw = getenv("SHELL");
    char *old_shell = raw ? strdup(raw) : NULL;
    cbm_setenv("SHELL", "/bin/zsh", 1);

    const char *rc = cbm_detect_shell_rc(tmpdir);
    ASSERT_NOT_NULL(rc);
    ASSERT(strstr(rc, ".zshrc") != NULL);

    if (old_shell) {
        cbm_setenv("SHELL", old_shell, 1);
        free(old_shell);
    } else
        cbm_unsetenv("SHELL");
    cbm_rmdir(tmpdir);
    PASS();
}

TEST(cli_detect_shell_rc_bash) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-rc-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *raw = getenv("SHELL");
    char *old_shell = raw ? strdup(raw) : NULL;
    cbm_setenv("SHELL", "/bin/bash", 1);

    /* No .bashrc → falls back to .bash_profile */
    const char *rc = cbm_detect_shell_rc(tmpdir);
    ASSERT_NOT_NULL(rc);
    ASSERT(strstr(rc, ".bash_profile") != NULL);

    if (old_shell) {
        cbm_setenv("SHELL", old_shell, 1);
        free(old_shell);
    } else
        cbm_unsetenv("SHELL");
    cbm_rmdir(tmpdir);
    PASS();
}

TEST(cli_detect_shell_rc_bash_with_bashrc) {
    /* Port of TestDetectShellRC_BashWithBashrc */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-rc-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *raw = getenv("SHELL");
    char *old_shell = raw ? strdup(raw) : NULL;
    cbm_setenv("SHELL", "/bin/bash", 1);

    /* Create .bashrc */
    char bashrc[512];
    snprintf(bashrc, sizeof(bashrc), "%s/.bashrc", tmpdir);
    write_test_file(bashrc, "# test\n");

    const char *rc = cbm_detect_shell_rc(tmpdir);
    ASSERT_STR_EQ(rc, bashrc);

    cbm_unlink(bashrc);
    if (old_shell) {
        cbm_setenv("SHELL", old_shell, 1);
        free(old_shell);
    } else
        cbm_unsetenv("SHELL");
    cbm_rmdir(tmpdir);
    PASS();
}

TEST(cli_detect_shell_rc_fish) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-rc-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *raw = getenv("SHELL");
    char *old_shell = raw ? strdup(raw) : NULL;
    cbm_setenv("SHELL", "/usr/bin/fish", 1);

    const char *rc = cbm_detect_shell_rc(tmpdir);
    ASSERT(strstr(rc, ".config/fish/config.fish") != NULL);

    if (old_shell) {
        cbm_setenv("SHELL", old_shell, 1);
        free(old_shell);
    } else
        cbm_unsetenv("SHELL");
    cbm_rmdir(tmpdir);
    PASS();
}

TEST(cli_detect_shell_rc_default) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-rc-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *raw = getenv("SHELL");
    char *old_shell = raw ? strdup(raw) : NULL;
    cbm_setenv("SHELL", "/bin/sh", 1);

    const char *rc = cbm_detect_shell_rc(tmpdir);
    ASSERT(strstr(rc, ".profile") != NULL);

    if (old_shell) {
        cbm_setenv("SHELL", old_shell, 1);
        free(old_shell);
    } else
        cbm_unsetenv("SHELL");
    cbm_rmdir(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  CLI binary detection (port of TestFindCLI_*)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_find_cli_not_found) {
    /* Port of TestFindCLI_NotFound */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-find-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *raw = getenv("PATH");
    char *old_path = raw ? strdup(raw) : NULL;
    cbm_setenv("PATH", tmpdir, 1);

    const char *result = cbm_find_cli("nonexistent-binary-xyz", tmpdir);
    ASSERT_STR_EQ(result, "");

    if (old_path) {
        cbm_setenv("PATH", old_path, 1);
        free(old_path);
    }
    cbm_rmdir(tmpdir);
    PASS();
}

TEST(cli_find_cli_on_path) {
    /* Port of TestFindCLI_FoundOnPATH */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-find-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char fakecli[512];
    snprintf(fakecli, sizeof(fakecli), "%s/fakecli", tmpdir);
    write_test_file(fakecli, "#!/bin/sh\n");
    th_make_executable(fakecli);

#ifdef _WIN32
    cbm_rmdir(tmpdir);
    SKIP_PLATFORM("Windows: PATH-based CLI lookup uses POSIX semantics");
#endif
    const char *raw = getenv("PATH");
    char *old_path = raw ? strdup(raw) : NULL;
    cbm_setenv("PATH", tmpdir, 1);

    const char *result = cbm_find_cli("fakecli", tmpdir);
    ASSERT(result[0] != '\0');
    ASSERT(strstr(result, "fakecli") != NULL);

    if (old_path) {
        cbm_setenv("PATH", old_path, 1);
        free(old_path);
    }
    cbm_unlink(fakecli);
    cbm_rmdir(tmpdir);
    PASS();
}

TEST(cli_find_cli_fallback_paths) {
    /* Port of TestFindCLI_FallbackPaths */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-find-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

#ifdef _WIN32
    cbm_rmdir(tmpdir);
    SKIP_PLATFORM("Windows: fallback path lookup uses POSIX semantics");
#endif
    char localbin[512];
    snprintf(localbin, sizeof(localbin), "%s/.local/bin", tmpdir);
    test_mkdirp(localbin);

    char fakecli[512];
    snprintf(fakecli, sizeof(fakecli), "%s/testcli", localbin);
    write_test_file(fakecli, "#!/bin/sh\n");
    th_make_executable(fakecli);

    const char *raw = getenv("PATH");
    char *old_path = raw ? strdup(raw) : NULL;
    cbm_setenv("PATH", "/nonexistent", 1);

    const char *result = cbm_find_cli("testcli", tmpdir);
    ASSERT_STR_EQ(result, fakecli);

    if (old_path) {
        cbm_setenv("PATH", old_path, 1);
        free(old_path);
    }
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_find_cli_fallback_scans_cargo_bin) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-find-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

#ifdef _WIN32
    cbm_rmdir(tmpdir);
    SKIP_PLATFORM("Windows: fallback path lookup uses POSIX semantics");
#endif
    char cargobin[512];
    snprintf(cargobin, sizeof(cargobin), "%s/.cargo/bin", tmpdir);
    test_mkdirp(cargobin);

    char fakecli[512];
    snprintf(fakecli, sizeof(fakecli), "%s/testcargo", cargobin);
    write_test_file(fakecli, "#!/bin/sh\n");
    th_make_executable(fakecli);

    const char *raw = getenv("PATH");
    char *old_path = raw ? strdup(raw) : NULL;
    cbm_setenv("PATH", "/nonexistent", 1);

    const char *result = cbm_find_cli("testcargo", tmpdir);
    ASSERT_STR_EQ(result, fakecli);

    if (old_path) {
        cbm_setenv("PATH", old_path, 1);
        free(old_path);
    }
    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Dry-run flag parsing (port of TestDryRun)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_dry_run_flags) {
    /* Port of TestDryRun — just verifies the pattern */
    bool dry_run = false, force = false;
    const char *args[] = {"--dry-run", "--force"};
    for (int i = 0; i < 2; i++) {
        if (strcmp(args[i], "--dry-run") == 0)
            dry_run = true;
        if (strcmp(args[i], "--force") == 0)
            force = true;
    }
    ASSERT_TRUE(dry_run);
    ASSERT_TRUE(force);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Skill file management tests (port of install_test.go skill tests)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_skill_creation) {
    /* Port of TestInstallSkillCreation */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-skill-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", tmpdir);

    int written = cbm_install_skills(skills_dir, false, false);
    ASSERT_EQ(written, CBM_SKILL_COUNT);

    /* Verify all 4 skills exist and have content */
    const cbm_skill_t *sk = cbm_get_skills();
    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s/SKILL.md", skills_dir, sk[i].name);
        const char *data = read_test_file(path);
        ASSERT_NOT_NULL(data);
        ASSERT(strlen(data) > 0);
        /* Check YAML frontmatter */
        ASSERT(strncmp(data, "---\n", 4) == 0);
        /* Check name field */
        ASSERT(strstr(data, sk[i].name) != NULL);
    }

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_skill_idempotent) {
    /* Port of TestInstallIdempotent */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-skill-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", tmpdir);

    /* Install twice */
    cbm_install_skills(skills_dir, false, false);
    int second = cbm_install_skills(skills_dir, false, false);

    /* Second install should write 0 (skills exist, no force) */
    ASSERT_EQ(second, 0);

    /* All skills should still exist */
    const cbm_skill_t *sk = cbm_get_skills();
    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s/SKILL.md", skills_dir, sk[i].name);
        struct stat st;
        ASSERT_EQ(stat(path, &st), 0);
    }

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_skill_force_overwrite) {
    /* Port of TestCLI_InstallForceOverwrites */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-skill-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", tmpdir);

    cbm_install_skills(skills_dir, false, false);
    int force_count = cbm_install_skills(skills_dir, true, false);

    /* Force should overwrite all */
    ASSERT_EQ(force_count, CBM_SKILL_COUNT);

    test_rmdir_r(tmpdir);
    PASS();
}

#ifndef _WIN32
TEST(cli_skills_reject_symlink_and_preserve_unowned_content) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-skill-safety-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    char target_dir[512];
    char target_file[640];
    char skill_path[640];
    char skill_file[768];
    snprintf(skills_dir, sizeof(skills_dir), "%s/skills", tmpdir);
    snprintf(target_dir, sizeof(target_dir), "%s/user-target", tmpdir);
    snprintf(target_file, sizeof(target_file), "%s/SKILL.md", target_dir);
    snprintf(skill_path, sizeof(skill_path), "%s/codebase-memory", skills_dir);
    snprintf(skill_file, sizeof(skill_file), "%s/SKILL.md", skill_path);
    test_mkdirp(skills_dir);
    test_mkdirp(target_dir);
    const char *sentinel = "user-owned sentinel\n";
    write_test_file(target_file, sentinel);
    ASSERT_EQ(symlink(target_dir, skill_path), 0);

    int installed = cbm_install_skills(skills_dir, true, false);
    char *after_install = read_test_file_alloc(target_file);
    bool install_safe = installed == 0 && after_install && strcmp(after_install, sentinel) == 0;
    free(after_install);

    /* Restore the target in case the red implementation followed the link, so
     * uninstall behavior is independently observable. */
    write_test_file(target_file, sentinel);
    int removed_link = cbm_remove_skills(skills_dir, false);
    char *after_remove = read_test_file_alloc(target_file);
    bool remove_safe = removed_link == 0 && after_remove && strcmp(after_remove, sentinel) == 0;
    free(after_remove);
    (void)cbm_unlink(skill_path);

    test_mkdirp(skill_path);
    write_test_file(skill_file, sentinel);
    int skipped = cbm_install_skills(skills_dir, false, false);
    int removed_user = cbm_remove_skills(skills_dir, false);
    char *user_after = read_test_file_alloc(skill_file);
    bool preserves_skipped =
        skipped == 0 && removed_user == 0 && user_after && strcmp(user_after, sentinel) == 0;
    free(user_after);

    test_rmdir_r(tmpdir);
    if (!install_safe || !remove_safe || !preserves_skipped)
        FAIL("skill install/uninstall must reject links and preserve user-owned content");
    PASS();
}

TEST(cli_legacy_skill_cleanup_rejects_links_and_user_content) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-legacy-skill-safety-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    char target_dir[512];
    char target_file[640];
    char legacy_link[640];
    snprintf(skills_dir, sizeof(skills_dir), "%s/skills", tmpdir);
    snprintf(target_dir, sizeof(target_dir), "%s/user-target", tmpdir);
    snprintf(target_file, sizeof(target_file), "%s/sentinel.txt", target_dir);
    snprintf(legacy_link, sizeof(legacy_link), "%s/codebase-memory-exploring", skills_dir);
    test_mkdirp(skills_dir);
    test_mkdirp(target_dir);
    const char *sentinel = "user-owned legacy target\n";
    write_test_file(target_file, sentinel);
    ASSERT_EQ(symlink(target_dir, legacy_link), 0);

    (void)cbm_install_skills(skills_dir, false, false);
    char *after_install = read_test_file_alloc(target_file);
    bool install_link_safe = after_install && strcmp(after_install, sentinel) == 0;
    free(after_install);
    (void)cbm_unlink(legacy_link);

    char old_dir[640];
    char old_file[768];
    snprintf(old_dir, sizeof(old_dir), "%s/codebase-memory-tracing", skills_dir);
    snprintf(old_file, sizeof(old_file), "%s/user-notes.md", old_dir);
    test_mkdirp(old_dir);
    write_test_file(old_file, sentinel);
    (void)cbm_install_skills(skills_dir, false, false);
    char *after_directory_cleanup = read_test_file_alloc(old_file);
    bool user_directory_safe =
        after_directory_cleanup && strcmp(after_directory_cleanup, sentinel) == 0;
    free(after_directory_cleanup);

    char monolithic_link[640];
    snprintf(monolithic_link, sizeof(monolithic_link), "%s/codebase-memory-mcp", skills_dir);
    ASSERT_EQ(symlink(target_dir, monolithic_link), 0);
    bool reported_removed = cbm_remove_old_monolithic_skill(skills_dir, false);
    char *after_remove = read_test_file_alloc(target_file);
    bool remove_link_safe =
        !reported_removed && after_remove && strcmp(after_remove, sentinel) == 0;
    free(after_remove);

    test_rmdir_r(tmpdir);
    if (!install_link_safe || !user_directory_safe || !remove_link_safe)
        FAIL("legacy skill cleanup must not follow links or delete unowned content");
    PASS();
}
#endif

TEST(cli_uninstall_removes_skills) {
    /* Port of TestUninstallRemovesSkills */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-skill-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", tmpdir);

    cbm_install_skills(skills_dir, false, false);
    int removed = cbm_remove_skills(skills_dir, false);
    ASSERT_EQ(removed, CBM_SKILL_COUNT);

    /* Verify all removed */
    const cbm_skill_t *sk = cbm_get_skills();
    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", skills_dir, sk[i].name);
        struct stat st;
        ASSERT(stat(path, &st) != 0);
    }

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_remove_old_monolithic_skill) {
    /* Port of TestRemoveOldMonolithicSkill */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-skill-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", tmpdir);

    /* Only an empty legacy directory is safe to remove automatically. */
    char old_dir[1024];
    snprintf(old_dir, sizeof(old_dir), "%s/codebase-memory-mcp", skills_dir);
    test_mkdirp(old_dir);

    bool removed = cbm_remove_old_monolithic_skill(skills_dir, false);
    ASSERT_TRUE(removed);

    struct stat st;
    ASSERT(stat(old_dir, &st) != 0);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_skill_files_content) {
    /* Consolidated skill: all 4 former skills merged into one. */
    const cbm_skill_t *sk = cbm_get_skills();
    ASSERT_EQ(CBM_SKILL_COUNT, 1);
    ASSERT(strcmp(sk[0].name, "codebase-memory") == 0);

    /* Exploring capabilities */
    ASSERT(strstr(sk[0].content, "search_graph") != NULL);
    ASSERT(strstr(sk[0].content, "get_graph_schema") != NULL);

    /* Tracing capabilities */
    ASSERT(strstr(sk[0].content, "trace_path") != NULL);
    ASSERT(strstr(sk[0].content, "direction") != NULL);
    ASSERT(strstr(sk[0].content, "detect_changes") != NULL);

    /* Hermes isolates delegated context. Keep its actionable handoff contract in
     * the shared installed skill instead of relying on parent conversation state. */
    ASSERT(strstr(sk[0].content, "delegate_task") != NULL);
    ASSERT(strstr(sk[0].content, "`context`") != NULL);

    /* Quality capabilities */
    ASSERT(strstr(sk[0].content, "max_degree=0") != NULL);
    ASSERT(strstr(sk[0].content, "exclude_entry_points") != NULL);

    /* Reference capabilities */
    ASSERT(strstr(sk[0].content, "query_graph") != NULL);
    ASSERT(strstr(sk[0].content, "Cypher") != NULL);
    ASSERT(strstr(sk[0].content, "MCP Tools") != NULL);
    ASSERT(strstr(sk[0].content, "index_dependencies") != NULL);
    ASSERT(strstr(sk[0].content, "auto_index=true") != NULL);
    ASSERT(strstr(sk[0].content, "auto_index_deps=true") != NULL);
    ASSERT(strstr(sk[0].content, "auto_dep_limit") != NULL);
    ASSERT(strstr(sk[0].content, "get_code_snippet` in classic mode") != NULL);
    ASSERT(strstr(sk[0].content, "_hidden_tools") != NULL);
    ASSERT(strstr(sk[0].content, "problem-specific Cypher") != NULL);
    ASSERT(strstr(sk[0].content, "query_max_output_bytes") != NULL);
    ASSERT(strstr(sk[0].content, "## 15 MCP Tools") == NULL);

    /* Gotchas section */
    ASSERT(strstr(sk[0].content, "Gotchas") != NULL);

    PASS();
}

TEST(cli_codex_instructions) {
    /* Port of TestCodexInstructionsCreation */
    const char *instr = cbm_get_codex_instructions();
    ASSERT_NOT_NULL(instr);
    ASSERT(strstr(instr, "Codebase Knowledge Graph") != NULL);
    ASSERT(strstr(instr, "trace_path") != NULL);
    ASSERT(strstr(instr, "effective, computationally efficient") != NULL);
    ASSERT(strstr(instr, "examples and LIMIT are optional guidance") != NULL);
    ASSERT(strstr(instr, "get_code` in streamlined mode") != NULL);
    ASSERT(strstr(instr, "auto_index_deps") != NULL);
    ASSERT(strstr(instr, "auto_dep_limit") != NULL);
    ASSERT(strstr(instr, "retry that operation with escalation") != NULL);
    ASSERT(strstr(instr, "MCP approval and shell sandbox authorization are separate") != NULL);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Editor MCP config tests (Cursor/Windsurf/Gemini)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_editor_mcp_install) {
    /* Port of TestEditorMCPInstall */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.cursor/mcp.json", tmpdir);

    int rc = cbm_install_editor_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "mcpServers") != NULL);
    ASSERT(strstr(data, "\"codebase-memory-mcp\"") != NULL);
    ASSERT(strstr(data, "/usr/local/bin/codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_editor_mcp_idempotent) {
    /* Port of TestEditorMCPInstallIdempotent */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.cursor/mcp.json", tmpdir);

    cbm_install_editor_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    int rc = cbm_install_editor_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    /* Should still parse as valid JSON with only 1 server */
    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    /* Count occurrences of "codebase-memory-mcp" (should be exactly 1 in mcpServers) */
    int count = 0;
    const char *p = data;
    while ((p = strstr(p, "\"codebase-memory-mcp\"")) != NULL) {
        count++;
        p += 20;
    }
    /* The key appears once as key name */
    ASSERT_EQ(count, 1);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_editor_mcp_preserves_others) {
    /* Port of TestEditorMCPPreservesOtherServers */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.cursor/mcp.json", tmpdir);
    test_mkdirp(tmpdir);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.cursor", tmpdir);
    test_mkdirp(dir);

    /* Write config with existing server */
    write_test_file(configpath,
                    "{\"mcpServers\": {\"other-server\": {\"command\": \"/usr/bin/other\"}}}");

    cbm_install_editor_mcp("/usr/local/bin/codebase-memory-mcp", configpath);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "other-server") != NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_editor_mcp_uninstall) {
    /* Port of TestEditorMCPUninstall */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.cursor/mcp.json", tmpdir);

    cbm_install_editor_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    int rc = cbm_remove_editor_mcp_owned("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    /* codebase-memory-mcp should be removed */
    ASSERT(strstr(data, "\"codebase-memory-mcp\"") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_junie_mcp_install_issue651) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.junie/mcp/mcp.json", tmpdir);

    int rc = cbm_upsert_junie_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "mcpServers") != NULL);
    ASSERT(strstr(data, "\"codebase-memory-mcp\"") != NULL);
    ASSERT(strstr(data, "\"codebase-memory-analysis\"") != NULL);
    ASSERT(strstr(data, "\"codebase-memory-scout\"") != NULL);
    ASSERT(strstr(data, "/usr/local/bin/codebase-memory-mcp") != NULL);
    ASSERT(strstr(data, "--tool-profile=analysis") != NULL);
    ASSERT(strstr(data, "--tool-profile=scout") != NULL);

    rc = cbm_upsert_junie_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    int count = 0;
    const char *p = data;
    while ((p = strstr(p, "\"codebase-memory-mcp\"")) != NULL) {
        count++;
        p += 20;
    }
    ASSERT_EQ(count, 1);
    count = 0;
    p = data;
    while ((p = strstr(p, "\"codebase-memory-scout\"")) != NULL) {
        count++;
        p += strlen("\"codebase-memory-scout\"");
    }
    ASSERT_EQ(count, 1);
    count = 0;
    p = data;
    while ((p = strstr(p, "\"codebase-memory-analysis\"")) != NULL) {
        count++;
        p += strlen("\"codebase-memory-analysis\"");
    }
    ASSERT_EQ(count, 1);

    rc = cbm_remove_junie_mcp_owned("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"codebase-memory-mcp\"") == NULL);
    ASSERT(strstr(data, "\"codebase-memory-analysis\"") == NULL);
    ASSERT(strstr(data, "\"codebase-memory-scout\"") == NULL);

    const char *partly_foreign =
        "{\"mcpServers\":{"
        "\"codebase-memory-mcp\":{\"command\":\"/usr/local/bin/codebase-memory-mcp\","
        "\"args\":[]},"
        "\"codebase-memory-scout\":{\"command\":\"/usr/local/bin/codebase-memory-mcp\","
        "\"args\":[\"--tool-profile=scout\"]},"
        "\"codebase-memory-analysis\":{\"command\":\"/opt/user-tool\","
        "\"args\":[\"--private\"]}}}\n";
    write_test_file(configpath, partly_foreign);
    rc = cbm_remove_junie_mcp_owned("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);
    data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"codebase-memory-mcp\"") == NULL);
    ASSERT(strstr(data, "\"codebase-memory-scout\"") == NULL);
    ASSERT(strstr(data, "\"codebase-memory-analysis\"") != NULL);
    ASSERT(strstr(data, "/opt/user-tool") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_gemini_mcp_install) {
    /* Port of TestGeminiMCPInstall */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.gemini/settings.json", tmpdir);

    /* Gemini uses same mcpServers format as Cursor */
    int rc = cbm_install_editor_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "mcpServers") != NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_openclaw_mcp_install_uses_nested_servers) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-openclaw-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.openclaw/openclaw.json", tmpdir);

    int rc = cbm_install_openclaw_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    yyjson_doc *doc = yyjson_read(data, strlen(data), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *mcp = yyjson_obj_get(root, "mcp");
    yyjson_val *servers = yyjson_obj_get(mcp, "servers");
    yyjson_val *entry = yyjson_obj_get(servers, "codebase-memory-mcp");
    ASSERT(entry && yyjson_is_obj(entry));
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(entry, "command")),
                  "/usr/local/bin/codebase-memory-mcp");
    yyjson_val *args = yyjson_obj_get(entry, "args");
    ASSERT(args && yyjson_is_arr(args));
    ASSERT_EQ(yyjson_arr_size(args), 0U);
    ASSERT_NULL(yyjson_obj_get(root, "mcpServers"));
    yyjson_doc_free(doc);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_openclaw_mcp_preserves_existing_config) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-openclaw-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.openclaw", tmpdir);
    test_mkdirp(dir);

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/openclaw.json", dir);
    write_test_file(configpath,
                    "{\"theme\":\"dark\",\"mcp\":{\"servers\":{\"other\":{\"command\":\"x\"}}}}");

    int rc = cbm_install_openclaw_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "theme") != NULL);
    ASSERT(strstr(data, "other") != NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);
    ASSERT(strstr(data, "\"mcpServers\"") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_openclaw_mcp_preserves_valid_json5) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-openclaw-json5-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.openclaw", tmpdir);
    test_mkdirp(dir);
    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/openclaw.json", dir);
    write_test_file(configpath,
                    "{ theme: 'dark', mcp: { servers: { other: { command: 'x' } } } }\n");

    int rc = cbm_install_openclaw_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    char *data = read_test_file_alloc(configpath);
    bool preserved_theme = data && strstr(data, "theme") && strstr(data, "dark");
    bool preserved_server = data && strstr(data, "other") && strstr(data, "command");
    bool installed = data && strstr(data, "codebase-memory-mcp");

    free(data);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !preserved_theme || !preserved_server || !installed)
        FAIL("OpenClaw MCP install must preserve valid JSON5 settings and sibling servers");
    PASS();
}

TEST(cli_openclaw_mcp_uninstall_uses_nested_servers) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-openclaw-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.openclaw/openclaw.json", tmpdir);

    ASSERT_EQ(cbm_install_openclaw_mcp("/usr/local/bin/codebase-memory-mcp", configpath), 0);
    ASSERT_EQ(cbm_remove_openclaw_mcp_owned("/usr/local/bin/codebase-memory-mcp", configpath), 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"mcp\"") != NULL);
    ASSERT(strstr(data, "\"servers\"") != NULL);
    ASSERT(strstr(data, "\"codebase-memory-mcp\"") == NULL);
    ASSERT(strstr(data, "\"mcpServers\"") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_openclaw_compaction_preserves_user_owned_section) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-openclaw-compaction-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    char config_path[640];
    snprintf(config_dir, sizeof(config_dir), "%s/.openclaw", tmpdir);
    snprintf(config_path, sizeof(config_path), "%s/openclaw.json", config_dir);
    test_mkdirp(config_dir);
    write_test_file(config_path,
                    "{\"agents\":{\"defaults\":{\"compaction\":{"
                    "\"postCompactionSections\":[\"Codebase Memory\",\"User Notes\"]}}}}\n");

    const char *const env_names[] = {"HOME",
                                     "PATH",
                                     "OPENCLAW_HOME",
                                     "OPENCLAW_STATE_DIR",
                                     "OPENCLAW_CONFIG_PATH",
                                     "OPENCLAW_WORKSPACE_DIR",
                                     "OPENCLAW_PROFILE"};
    char *saved_env[sizeof(env_names) / sizeof(env_names[0])];
    for (size_t i = 0; i < sizeof(env_names) / sizeof(env_names[0]); i++) {
        saved_env[i] = save_test_env(env_names[i]);
        cbm_unsetenv(env_names[i]);
    }
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);

    cbm_install_agent_configs(tmpdir, "/usr/local/bin/codebase-memory-mcp", false, false);
    char *installed = read_test_file_alloc(config_path);
    bool installed_owned =
        installed && strstr(installed, "Codebase Knowledge Graph (codebase-memory-mcp)");
    bool retained_existing =
        installed && strstr(installed, "Codebase Memory") && strstr(installed, "User Notes");
    free(installed);

    char *argv[] = {"uninstall", "--yes"};
    int rc = cbm_cmd_uninstall(2, argv);
    char *uninstalled = read_test_file_alloc(config_path);
    bool preserved_user =
        uninstalled && strstr(uninstalled, "Codebase Memory") && strstr(uninstalled, "User Notes");
    bool removed_owned =
        uninstalled && !strstr(uninstalled, "Codebase Knowledge Graph (codebase-memory-mcp)");
    free(uninstalled);

    for (size_t i = 0; i < sizeof(env_names) / sizeof(env_names[0]); i++) {
        restore_test_env(env_names[i], saved_env[i]);
    }
    test_rmdir_r(tmpdir);
    if (!installed_owned || !retained_existing || rc != 0 || !preserved_user || !removed_owned)
        FAIL("OpenClaw uninstall must remove only its namespaced compaction section");
    PASS();
}

TEST(cli_openclaw_profile_uses_profile_state_and_default_workspace) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-openclaw-profile-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char profile_dir[512];
    char config_path[640];
    snprintf(profile_dir, sizeof(profile_dir), "%s/.openclaw-work", tmpdir);
    snprintf(config_path, sizeof(config_path), "%s/openclaw.json", profile_dir);
    test_mkdirp(profile_dir);
    write_test_file(config_path, "{}\n");

    const char *const env_names[] = {"PATH",
                                     "OPENCLAW_HOME",
                                     "OPENCLAW_STATE_DIR",
                                     "OPENCLAW_CONFIG_PATH",
                                     "OPENCLAW_WORKSPACE_DIR",
                                     "OPENCLAW_PROFILE"};
    char *saved_env[sizeof(env_names) / sizeof(env_names[0])];
    for (size_t i = 0; i < sizeof(env_names) / sizeof(env_names[0]); i++) {
        saved_env[i] = save_test_env(env_names[i]);
        cbm_unsetenv(env_names[i]);
    }
    cbm_setenv("PATH", tmpdir, 1);
    cbm_setenv("OPENCLAW_PROFILE", "work", 1);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    char *plan = cbm_build_install_plan_json(tmpdir, "/usr/local/bin/codebase-memory-mcp");
    bool correct = agents.openclaw && plan && strstr(plan, "/.openclaw-work/openclaw.json") &&
                   strstr(plan, "/.openclaw/workspace-work/AGENTS.md") &&
                   !strstr(plan, "/.openclaw-work/workspace-work/AGENTS.md");

    free(plan);
    for (size_t i = 0; i < sizeof(env_names) / sizeof(env_names[0]); i++) {
        restore_test_env(env_names[i], saved_env[i]);
    }
    test_rmdir_r(tmpdir);
    if (!correct)
        FAIL("OpenClaw profiles must use ~/.openclaw-<profile> state and ~/.openclaw workspace");
    PASS();
}

TEST(cli_openclaw_uninstall_removes_compaction_when_workspace_is_ambiguous) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-openclaw-uninstall-ambiguous-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char config_dir[512];
    char config_path[640];
    snprintf(config_dir, sizeof(config_dir), "%s/.openclaw", tmpdir);
    snprintf(config_path, sizeof(config_path), "%s/openclaw.json", config_dir);
    test_mkdirp(config_dir);
    write_test_file(config_path,
                    "{\"$include\":[\"one.json\",\"two.json\"],\"agents\":{\"defaults\":{"
                    "\"compaction\":{\"postCompactionSections\":["
                    "\"Codebase Knowledge Graph (codebase-memory-mcp)\"]}}}}\n");

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    char *argv[] = {"uninstall", "--yes"};
    int rc = cbm_cmd_uninstall(2, argv);
    char *after = read_test_file_alloc(config_path);
    bool removed = after && !strstr(after, "Codebase Knowledge Graph (codebase-memory-mcp)");

    free(after);
    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !removed)
        FAIL("OpenClaw compaction cleanup must not depend on resolving a workspace");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  VS Code MCP config tests
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_vscode_mcp_install) {
    /* Port of TestVSCodeMCPInstall */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/Code/User/mcp.json", tmpdir);

    int rc = cbm_install_vscode_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"servers\"") != NULL);
    ASSERT(strstr(data, "\"type\"") != NULL);
    ASSERT(strstr(data, "\"stdio\"") != NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);
    ASSERT(strstr(data, "/usr/local/bin/codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_vscode_mcp_uninstall) {
    /* Port of TestVSCodeMCPUninstall */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/Code/User/mcp.json", tmpdir);

    cbm_install_vscode_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    int rc = cbm_remove_vscode_mcp_owned("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"codebase-memory-mcp\"") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_vscode_profile_mcp_uninstall) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-vscode-profile-uninstall-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char code_user[640];
#ifdef __APPLE__
    snprintf(code_user, sizeof(code_user), "%s/Library/Application Support/Code/User", tmpdir);
#elif defined(_WIN32)
    snprintf(code_user, sizeof(code_user), "%s/AppData/Roaming/Code/User", tmpdir);
#else
    snprintf(code_user, sizeof(code_user), "%s/.config/Code/User", tmpdir);
#endif
    char profile_dir[768];
    char base_config[768];
    char profile_config[896];
    snprintf(profile_dir, sizeof(profile_dir), "%s/profiles/profile-one", code_user);
    snprintf(base_config, sizeof(base_config), "%s/mcp.json", code_user);
    snprintf(profile_config, sizeof(profile_config), "%s/mcp.json", profile_dir);
    test_mkdirp(profile_dir);
    char installed_binary[640];
#ifdef _WIN32
    snprintf(installed_binary, sizeof(installed_binary), "%s/.local/bin/codebase-memory-mcp.exe",
             tmpdir);
#else
    snprintf(installed_binary, sizeof(installed_binary), "%s/.local/bin/codebase-memory-mcp",
             tmpdir);
#endif
    ASSERT_EQ(cbm_install_vscode_mcp(installed_binary, base_config), 0);
    ASSERT_EQ(cbm_install_vscode_mcp(installed_binary, profile_config), 0);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_appdata = save_test_env("APPDATA");
    char *saved_xdg = save_test_env("XDG_CONFIG_HOME");
    char xdg_dir[640];
    snprintf(xdg_dir, sizeof(xdg_dir), "%s/.config", tmpdir);
    cbm_setenv("XDG_CONFIG_HOME", xdg_dir, 1); /* Linux resolvers prefer XDG */
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
#ifdef _WIN32
    char appdata[512];
    snprintf(appdata, sizeof(appdata), "%s/AppData/Roaming", tmpdir);
    cbm_setenv("APPDATA", appdata, 1);
#endif
    char *argv[] = {"uninstall", "--yes"};
    int rc = cbm_cmd_uninstall(2, argv);
    char *base = read_test_file_alloc(base_config);
    char *profile = read_test_file_alloc(profile_config);
    bool removed = base && profile && !strstr(base, "codebase-memory-mcp") &&
                   !strstr(profile, "codebase-memory-mcp");

    free(base);
    free(profile);
    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("APPDATA", saved_appdata);
    restore_test_env("XDG_CONFIG_HOME", saved_xdg);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !removed)
        FAIL("VS Code uninstall must remove MCP entries from every existing profile");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Zed MCP config tests
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_zed_mcp_install) {
    /* Port of TestZedMCPInstall */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.config/zed/settings.json", tmpdir);

    int rc = cbm_install_zed_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"context_servers\"") != NULL);
    ASSERT(strstr(data, "\"command\"") != NULL);
    ASSERT(strstr(data, "\"args\"") != NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);
    ASSERT(strstr(data, "/usr/local/bin/codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_zed_mcp_preserves_settings) {
    /* Port of TestZedMCPPreservesSettings */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.config/zed/settings.json", tmpdir);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.config/zed", tmpdir);
    test_mkdirp(dir);

    /* Pre-existing Zed settings */
    write_test_file(configpath, "{\"theme\": \"One Dark\", \"vim_mode\": true}");

    cbm_install_zed_mcp("/usr/local/bin/codebase-memory-mcp", configpath);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    /* Original settings preserved */
    ASSERT(strstr(data, "One Dark") != NULL);
    ASSERT(strstr(data, "vim_mode") != NULL);
    /* MCP server added */
    ASSERT(strstr(data, "context_servers") != NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_zed_mcp_uninstall) {
    /* Port of TestZedMCPUninstall */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.config/zed/settings.json", tmpdir);

    cbm_install_zed_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    int rc = cbm_remove_zed_mcp_owned("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"codebase-memory-mcp\"") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_zed_mcp_jsonc_comments) {
    /* Issue #24: Zed settings.json uses JSONC (comments + trailing commas) */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/.config/zed/settings.json", tmpdir);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.config/zed", tmpdir);
    test_mkdirp(dir);

    /* JSONC with comments and trailing commas — must not fail */
    write_test_file(configpath, "// Zed settings\n"
                                "{\n"
                                "  \"theme\": \"One Dark\",\n"
                                "  /* multi-line\n"
                                "     comment */\n"
                                "  \"vim_mode\": true,\n" /* trailing comma */
                                "}\n");

    int rc = cbm_install_zed_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    /* Original settings preserved */
    ASSERT(strstr(data, "One Dark") != NULL);
    ASSERT(strstr(data, "vim_mode") != NULL);
    /* MCP server added */
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);
    ASSERT(strstr(data, "context_servers") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  PATH management tests (port of TestCLI_InstallPATHAppend)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_ensure_path_append) {
    /* Port of TestCLI_InstallPATHAppend */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-path-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char rcfile[512];
    snprintf(rcfile, sizeof(rcfile), "%s/.zshrc", tmpdir);
    write_test_file(rcfile, "# existing content\n");

    int rc = cbm_ensure_path("/usr/local/bin", rcfile, false);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(rcfile);
    ASSERT(strstr(data, "export PATH=\"/usr/local/bin:$PATH\"") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_ensure_path_already_present) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-path-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char rcfile[512];
    snprintf(rcfile, sizeof(rcfile), "%s/.zshrc", tmpdir);
    write_test_file(rcfile, "export PATH=\"/usr/local/bin:$PATH\"\n");

    int rc = cbm_ensure_path("/usr/local/bin", rcfile, false);
    ASSERT_EQ(rc, 1); /* 1 = already present */

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_ensure_path_dry_run) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-path-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char rcfile[512];
    snprintf(rcfile, sizeof(rcfile), "%s/.zshrc", tmpdir);
    write_test_file(rcfile, "# clean\n");

    int rc = cbm_ensure_path("/usr/local/bin", rcfile, true);
    ASSERT_EQ(rc, 0);

    /* File should NOT be modified */
    const char *data = read_test_file(rcfile);
    ASSERT(strstr(data, "export PATH") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_remove_owned_path_block_preserves_user_content) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-path-remove-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char rcfile[512];
    snprintf(rcfile, sizeof(rcfile), "%s/.zshrc", tmpdir);
    write_test_file(rcfile, "# user prefix\nexport KEEP_ME=1\n");
    ASSERT_EQ(cbm_ensure_path("/usr/local/bin", rcfile, false), 0);
    ASSERT_EQ(cbm_remove_owned_path("/usr/local/bin", rcfile, false), 0);

    const char *data = read_test_file(rcfile);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "export KEEP_ME=1") != NULL);
    ASSERT(strstr(data, "Added by codebase-memory-mcp install") == NULL);
    ASSERT(strstr(data, "export PATH=\"/usr/local/bin:$PATH\"") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_remove_owned_path_dry_run_preserves_block) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-path-remove-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char rcfile[512];
    snprintf(rcfile, sizeof(rcfile), "%s/config.fish", tmpdir);
    write_test_file(rcfile, "# user prefix\n");
    ASSERT_EQ(cbm_ensure_path("/usr/local/bin", rcfile, false), 0);
    ASSERT_EQ(cbm_remove_owned_path("/usr/local/bin", rcfile, true), 0);

    const char *data = read_test_file(rcfile);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "Added by codebase-memory-mcp install") != NULL);
    ASSERT(strstr(data, "fish_add_path /usr/local/bin") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

/* issue #319: a fish config must get fish-native syntax, never `export PATH=`
 * (which is a syntax error in fish and breaks config.fish). */
TEST(cli_ensure_path_fish_syntax_issue319) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-path-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char rcfile[512];
    snprintf(rcfile, sizeof(rcfile), "%s/config.fish", tmpdir);
    write_test_file(rcfile, "# existing fish config\n");

    int rc = cbm_ensure_path("/usr/local/bin", rcfile, false);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(rcfile);
    ASSERT_NOT_NULL(data);
    /* fish-native form, and NO sh-style export. */
    ASSERT(strstr(data, "fish_add_path /usr/local/bin") != NULL);
    ASSERT(strstr(data, "export PATH") == NULL);

    /* Idempotent: a second call detects the existing fish line. */
    int rc2 = cbm_ensure_path("/usr/local/bin", rcfile, false);
    ASSERT_EQ(rc2, 1);

    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  File copy tests (port of update_test.go)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_copy_file) {
    /* Port of TestCopyFile */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-copy-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char src[512], dst[512];
    snprintf(src, sizeof(src), "%s/source", tmpdir);
    snprintf(dst, sizeof(dst), "%s/dest", tmpdir);

    write_test_file(src, "test content for copy");

    int rc = cbm_copy_file(src, dst);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(dst);
    ASSERT_STR_EQ(data, "test content for copy");

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_copy_file_source_not_found) {
    /* Port of TestCopyFile_SourceNotFound */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-copy-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char src[512], dst[512];
    snprintf(src, sizeof(src), "%s/nonexistent", tmpdir);
    snprintf(dst, sizeof(dst), "%s/dest", tmpdir);

    int rc = cbm_copy_file(src, dst);
    ASSERT(rc != 0);

    cbm_rmdir(tmpdir);
    PASS();
}

/* #472: install --force must copy the freshly-built binary to the target and
 * make it executable — previously it re-signed whatever was already there. */
TEST(cli_install_copies_binary_to_target_issue472) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-binswap-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char src[512], dst[512];
    snprintf(src, sizeof(src), "%s/new-build", tmpdir);
    snprintf(dst, sizeof(dst), "%s/installed", tmpdir);

    write_test_file(src, "fresh build bytes");

    /* Target does not exist yet → must be created with the source content. */
    int rc = cbm_copy_binary_to_target(src, dst);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(dst);
    ASSERT_STR_EQ(data, "fresh build bytes");

#ifndef _WIN32
    /* The exec bit is set via chmod, which is POSIX-only; on Windows it is not
     * meaningful and MinGW stat() derives it from the file extension. */
    struct stat st;
    ASSERT_EQ(stat(dst, &st), 0);
    ASSERT((st.st_mode & S_IXUSR) != 0); /* executable bit set */
#endif

    /* Overwrite an existing (stale) target with new content. */
    write_test_file(dst, "STALE");
#ifndef _WIN32
    struct stat stale_st;
    ASSERT_EQ(stat(dst, &stale_st), 0);
#endif
    write_test_file(src, "upgraded build bytes");
    rc = cbm_copy_binary_to_target(src, dst);
    ASSERT_EQ(rc, 0);
    data = read_test_file(dst);
    ASSERT_STR_EQ(data, "upgraded build bytes");
#ifndef _WIN32
    /* Replacement must publish a completed temporary file with rename(2),
     * not truncate the installed executable in place. A new inode proves the
     * old file can remain mapped by a just-stopped MCP process safely. */
    struct stat upgraded_st;
    ASSERT_EQ(stat(dst, &upgraded_st), 0);
    ASSERT_NEQ(stale_st.st_ino, upgraded_st.st_ino);
#endif

    test_rmdir_r(tmpdir);
    PASS();
}

/* #472: copying the running binary onto itself must NOT truncate it. */
TEST(cli_install_same_file_guard_issue472) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-samefile-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char path[512];
    snprintf(path, sizeof(path), "%s/self", tmpdir);
    write_test_file(path, "must survive self-copy");

    int rc = cbm_copy_binary_to_target(path, path);
    ASSERT_EQ(rc, 0); /* skipped, not failed */

    const char *data = read_test_file(path);
    ASSERT_STR_EQ(data, "must survive self-copy"); /* intact, not zeroed */

#ifndef _WIN32
    /* Distinct path strings resolving to the same inode (a symlink — exactly
     * what a non-canonical cbm_detect_self_path vs the hardcoded target can
     * produce) must also be detected as same-file and skipped, not truncated. */
    char link[512];
    snprintf(link, sizeof(link), "%s/self-link", tmpdir);
    if (symlink(path, link) == 0) {
        rc = cbm_copy_binary_to_target(link, path);
        ASSERT_EQ(rc, 0);
        data = read_test_file(path);
        ASSERT_STR_EQ(data, "must survive self-copy"); /* still intact via symlink */
    }
#endif

    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Tar.gz extraction tests (port of update_test.go)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_extract_binary_from_targz) {
    /* Port of TestExtractBinaryFromTarGz */
    const char *content = "fake binary content";
    int gz_len;
    unsigned char *gz =
        create_test_targz("codebase-memory-mcp-linux-amd64", (const unsigned char *)content,
                          (int)strlen(content), &gz_len);
    ASSERT_NOT_NULL(gz);

    int out_len;
    unsigned char *extracted = cbm_extract_binary_from_targz(gz, gz_len, &out_len);
    ASSERT_NOT_NULL(extracted);
    ASSERT_EQ(out_len, (int)strlen(content));
    ASSERT_MEM_EQ(extracted, content, out_len);

    free(extracted);
    free(gz);
    PASS();
}

TEST(cli_extract_binary_from_targz_not_found) {
    /* Port of TestExtractBinaryFromTarGz_NotFound */
    const char *content = "hello";
    int gz_len;
    unsigned char *gz = create_test_targz("some-other-file", (const unsigned char *)content,
                                          (int)strlen(content), &gz_len);
    ASSERT_NOT_NULL(gz);

    int out_len;
    unsigned char *extracted = cbm_extract_binary_from_targz(gz, gz_len, &out_len);
    ASSERT_NULL(extracted);

    free(gz);
    PASS();
}

TEST(cli_extract_binary_from_targz_invalid_data) {
    /* Port of TestExtractBinaryFromTarGz_InvalidData */
    const unsigned char bad_data[] = "not a valid tar.gz";
    int out_len;
    unsigned char *extracted = cbm_extract_binary_from_targz(bad_data, sizeof(bad_data), &out_len);
    ASSERT_NULL(extracted);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Zip extraction tests
 * ═══════════════════════════════════════════════════════════════════ */

/* Build a minimal zip file with one stored (uncompressed) entry. */
static unsigned char *create_test_zip_stored(const char *filename, const unsigned char *content,
                                             int content_len, int *out_len) {
    /* Local file header (30 bytes) + filename + content + central dir + EOCD */
    int name_len = (int)strlen(filename);
    int local_hdr_sz = 30 + name_len;
    int cd_hdr_sz = 46 + name_len;
    int eocd_sz = 22;
    int total = local_hdr_sz + content_len + cd_hdr_sz + eocd_sz;
    unsigned char *zip = calloc(1, (size_t)total);
    if (!zip)
        return NULL;
    int pos = 0;

    /* Local file header */
    zip[pos] = 0x50;
    zip[pos + 1] = 0x4B;
    zip[pos + 2] = 0x03;
    zip[pos + 3] = 0x04; /* signature */
    zip[pos + 4] = 20;
    zip[pos + 5] = 0; /* version needed = 2.0 */
    zip[pos + 8] = 0;
    zip[pos + 9] = 0; /* compression = stored */
    zip[pos + 18] = (unsigned char)(content_len & 0xFF);
    zip[pos + 19] = (unsigned char)((content_len >> 8) & 0xFF);
    zip[pos + 20] = (unsigned char)((content_len >> 16) & 0xFF);
    zip[pos + 21] = (unsigned char)((content_len >> 24) & 0xFF);
    zip[pos + 22] = zip[pos + 18];
    zip[pos + 23] = zip[pos + 19];
    zip[pos + 24] = zip[pos + 20];
    zip[pos + 25] = zip[pos + 21];
    zip[pos + 26] = (unsigned char)(name_len & 0xFF);
    zip[pos + 27] = (unsigned char)((name_len >> 8) & 0xFF);
    memcpy(zip + pos + 30, filename, (size_t)name_len);
    pos += 30 + name_len;
    memcpy(zip + pos, content, (size_t)content_len);
    pos += content_len;

    int cd_start = pos;
    /* Central directory header */
    zip[pos] = 0x50;
    zip[pos + 1] = 0x4B;
    zip[pos + 2] = 0x01;
    zip[pos + 3] = 0x02;
    zip[pos + 10] = 0;
    zip[pos + 11] = 0; /* compression = stored */
    zip[pos + 20] = (unsigned char)(content_len & 0xFF);
    zip[pos + 21] = (unsigned char)((content_len >> 8) & 0xFF);
    zip[pos + 22] = (unsigned char)((content_len >> 16) & 0xFF);
    zip[pos + 23] = (unsigned char)((content_len >> 24) & 0xFF);
    zip[pos + 24] = zip[pos + 20];
    zip[pos + 25] = zip[pos + 21];
    zip[pos + 26] = zip[pos + 22];
    zip[pos + 27] = zip[pos + 23];
    zip[pos + 28] = (unsigned char)(name_len & 0xFF);
    zip[pos + 29] = (unsigned char)((name_len >> 8) & 0xFF);
    pos += 46 + name_len;

    /* EOCD */
    zip[pos] = 0x50;
    zip[pos + 1] = 0x4B;
    zip[pos + 2] = 0x05;
    zip[pos + 3] = 0x06;
    zip[pos + 8] = 1;  /* num entries this disk */
    zip[pos + 10] = 1; /* total entries */
    int cd_size = pos - cd_start;
    zip[pos + 12] = (unsigned char)(cd_size & 0xFF);
    zip[pos + 13] = (unsigned char)((cd_size >> 8) & 0xFF);
    zip[pos + 16] = (unsigned char)(cd_start & 0xFF);
    zip[pos + 17] = (unsigned char)((cd_start >> 8) & 0xFF);

    *out_len = total;
    return zip;
}

TEST(cli_extract_binary_from_zip) {
    const char *content = "#!/bin/sh\necho test\n";
    int zip_len = 0;
    unsigned char *zip = create_test_zip_stored(
        "codebase-memory-mcp", (const unsigned char *)content, (int)strlen(content), &zip_len);
    ASSERT_NOT_NULL(zip);

    int out_len = 0;
    unsigned char *extracted = cbm_extract_binary_from_zip(zip, zip_len, &out_len);
    ASSERT_NOT_NULL(extracted);
    ASSERT_EQ(out_len, (int)strlen(content));
    ASSERT_MEM_EQ(extracted, content, (size_t)out_len);
    free(extracted);
    free(zip);
    PASS();
}

TEST(cli_extract_binary_from_zip_not_found) {
    const char *content = "data";
    int zip_len = 0;
    unsigned char *zip = create_test_zip_stored("other-file.txt", (const unsigned char *)content,
                                                (int)strlen(content), &zip_len);
    ASSERT_NOT_NULL(zip);

    int out_len = 0;
    unsigned char *extracted = cbm_extract_binary_from_zip(zip, zip_len, &out_len);
    ASSERT_NULL(extracted);
    free(zip);
    PASS();
}

TEST(cli_extract_binary_from_zip_path_traversal) {
    const char *content = "malicious";
    int zip_len = 0;
    unsigned char *zip =
        create_test_zip_stored("../../etc/codebase-memory-mcp", (const unsigned char *)content,
                               (int)strlen(content), &zip_len);
    ASSERT_NOT_NULL(zip);

    int out_len = 0;
    unsigned char *extracted = cbm_extract_binary_from_zip(zip, zip_len, &out_len);
    ASSERT_NULL(extracted);
    free(zip);
    PASS();
}

TEST(cli_extract_binary_from_zip_invalid) {
    const unsigned char bad_data[] = "not a zip file";
    int out_len = 0;
    unsigned char *extracted = cbm_extract_binary_from_zip(bad_data, sizeof(bad_data), &out_len);
    ASSERT_NULL(extracted);
    PASS();
}

TEST(cli_extract_binary_from_zip_rejects_truncated_deflate_size_over_int_max) {
    const char *filename = "codebase-memory-mcp";
    const unsigned char deflated[] = {0xAB, 0x00, 0x00}; /* raw DEFLATE for "x" */
    size_t name_len = strlen(filename);
    size_t zip_len = 30 + name_len + sizeof(deflated);
    unsigned char *zip = calloc(1, zip_len);
    ASSERT_NOT_NULL(zip);

    uint32_t comp_size = 0xFFFF0000U;
    uint32_t uncomp_size = 1U;
    zip[0] = 0x50;
    zip[1] = 0x4B;
    zip[2] = 0x03;
    zip[3] = 0x04;
    zip[8] = 8;
    zip[9] = 0;
    zip[18] = (unsigned char)(comp_size & 0xFF);
    zip[19] = (unsigned char)((comp_size >> 8) & 0xFF);
    zip[20] = (unsigned char)((comp_size >> 16) & 0xFF);
    zip[21] = (unsigned char)((comp_size >> 24) & 0xFF);
    zip[22] = (unsigned char)(uncomp_size & 0xFF);
    zip[23] = (unsigned char)((uncomp_size >> 8) & 0xFF);
    zip[24] = (unsigned char)((uncomp_size >> 16) & 0xFF);
    zip[25] = (unsigned char)((uncomp_size >> 24) & 0xFF);
    zip[26] = (unsigned char)(name_len & 0xFF);
    zip[27] = (unsigned char)((name_len >> 8) & 0xFF);
    memcpy(zip + 30, filename, name_len);
    memcpy(zip + 30 + name_len, deflated, sizeof(deflated));

    int out_len = 0;
    unsigned char *extracted = cbm_extract_binary_from_zip(zip, (int)zip_len, &out_len);
    if (extracted) {
        free(extracted);
        free(zip);
        FAIL("accepted a truncated deflated zip entry with a wrapped compressed size");
    }
    ASSERT_EQ(out_len, 0);
    free(zip);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Skill dry-run tests
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_install_dry_run) {
    /* Port of TestCLI_InstallDryRun */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-dry-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", tmpdir);

    int count = cbm_install_skills(skills_dir, false, true);
    ASSERT_EQ(count, CBM_SKILL_COUNT);

    /* Skills should NOT be created */
    const cbm_skill_t *sk = cbm_get_skills();
    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s/SKILL.md", skills_dir, sk[i].name);
        struct stat st;
        ASSERT(stat(path, &st) != 0);
    }

    cbm_rmdir(tmpdir);
    PASS();
}

TEST(cli_uninstall_dry_run) {
    /* Port of TestCLI_UninstallDryRun */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-dry-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", tmpdir);

    cbm_install_skills(skills_dir, false, false);
    int removed = cbm_remove_skills(skills_dir, true);
    ASSERT_EQ(removed, CBM_SKILL_COUNT);

    /* Skills should still exist */
    const cbm_skill_t *sk = cbm_get_skills();
    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s/SKILL.md", skills_dir, sk[i].name);
        struct stat st;
        ASSERT_EQ(stat(path, &st), 0);
    }

    test_rmdir_r(tmpdir);
    PASS();
}

/* A dry-run is a read-only preview even when -y is supplied.  In particular,
 * it must not route the automatic answer through the destructive index-removal
 * branch. */
TEST(cli_uninstall_dry_run_preserves_indexes) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-uninstall-preview-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char cache_dir[512];
    char project_db[768];
    snprintf(cache_dir, sizeof(cache_dir), "%s/cache", tmpdir);
    snprintf(project_db, sizeof(project_db), "%s/project.db", cache_dir);
    ASSERT_EQ(test_mkdirp(cache_dir), 0);
    ASSERT_EQ(write_test_file(project_db, "indexed graph"), 0);

    cli_env_snapshot_t home = {0};
    cli_env_snapshot_t cache = {0};
    ASSERT_TRUE(cli_env_snapshot(&home, "HOME"));
    ASSERT_TRUE(cli_env_snapshot(&cache, "CBM_CACHE_DIR"));
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("CBM_CACHE_DIR", cache_dir, 1);

    char *args[] = {"--dry-run", "-y"};
    ASSERT_EQ(cbm_cmd_uninstall(2, args), 0);

    struct stat st;
    ASSERT_EQ(stat(project_db, &st), 0);

    /* parse_auto_answer() is process-global; do not leak this test's -y into
     * later lifecycle tests. */
    extern void cbm_set_auto_answer_for_test(int value);
    cbm_set_auto_answer_for_test(0);
    cli_env_restore(&cache);
    cli_env_restore(&home);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_uninstall_removes_codex_json_hook_only) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-uninstall-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char codex_dir[512];
    char hooks_path[768];
    snprintf(codex_dir, sizeof(codex_dir), "%s/.codex", tmpdir);
    snprintf(hooks_path, sizeof(hooks_path), "%s/hooks.json", codex_dir);
    ASSERT_EQ(test_mkdirp(codex_dir), 0);
    ASSERT_EQ(write_test_file(hooks_path, "{\"hooks\":{\"SessionStart\":[{\"matcher\":\"startup\","
                                          "\"hooks\":[{\"type\":\"command\","
                                          "\"command\":\"echo user-hook\"}]}]}}"),
              0);
    ASSERT_EQ(cbm_upsert_gemini_session_hooks(hooks_path, "/usr/local/bin/codebase-memory-mcp"),
              0);

    cli_env_snapshot_t home = {0};
    ASSERT_TRUE(cli_env_snapshot(&home, "HOME"));
    cbm_setenv("HOME", tmpdir, 1);
    char *args[] = {"-n"};
    ASSERT_EQ(cbm_cmd_uninstall(1, args), 0);

    const char *contents = read_test_file(hooks_path);
    ASSERT_NOT_NULL(contents);
    ASSERT(strstr(contents, "echo user-hook") != NULL);
    ASSERT(strstr(contents, "codebase-memory-mcp reminder") == NULL);

    extern void cbm_set_auto_answer_for_test(int value);
    cbm_set_auto_answer_for_test(0);
    cli_env_restore(&home);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_uninstall_removes_owned_claude_hook_scripts) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-claude-uninstall-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char hooks_dir[512];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.claude/hooks", tmpdir);
    ASSERT_EQ(test_mkdirp(hooks_dir), 0);
#ifdef _WIN32
    const char *names[] = {"cbm-code-discovery-gate.cmd", "cbm-session-reminder.cmd",
                           "cbm-subagent-reminder.cmd"};
#else
    const char *names[] = {"cbm-code-discovery-gate", "cbm-session-reminder",
                           "cbm-subagent-reminder"};
#endif
    char paths[3][768];
    for (size_t i = 0; i < 3; i++) {
        snprintf(paths[i], sizeof(paths[i]), "%s/%s", hooks_dir, names[i]);
    }

    cli_env_snapshot_t home = {0};
    ASSERT_TRUE(cli_env_snapshot(&home, "HOME"));
    cbm_setenv("HOME", tmpdir, 1);
    char binary[768];
#ifdef _WIN32
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp.exe", tmpdir);
#else
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp", tmpdir);
#endif
    ASSERT_EQ(cbm_install_agent_configs(tmpdir, binary, false, false), 0);
    struct stat st;
    for (size_t i = 0; i < 3; i++)
        ASSERT_EQ(stat(paths[i], &st), 0);
    char *args[] = {"-n"};
    ASSERT_EQ(cbm_cmd_uninstall(1, args), 0);

    for (size_t i = 0; i < 3; i++)
        ASSERT_NEQ(stat(paths[i], &st), 0);

    extern void cbm_set_auto_answer_for_test(int value);
    cbm_set_auto_answer_for_test(0);
    cli_env_restore(&home);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_uninstall_removes_vscode_profile_mcp_only) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-vscode-uninstall-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char profile_dir[768];
    char profile_mcp[1024];
#ifdef __APPLE__
    snprintf(profile_dir, sizeof(profile_dir),
             "%s/Library/Application Support/Code/User/profiles/profile-a", tmpdir);
#else
    snprintf(profile_dir, sizeof(profile_dir), "%s/.config/Code/User/profiles/profile-a", tmpdir);
#endif
    snprintf(profile_mcp, sizeof(profile_mcp), "%s/mcp.json", profile_dir);
    ASSERT_EQ(test_mkdirp(profile_dir), 0);
    ASSERT_EQ(
        write_test_file(profile_mcp, "{\"servers\":{\"user-server\":{\"command\":\"user\"}}}"), 0);
    char binary[768];
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp", tmpdir);
    ASSERT_EQ(cbm_install_vscode_mcp(binary, profile_mcp), 0);

    cli_env_snapshot_t home = {0};
    ASSERT_TRUE(cli_env_snapshot(&home, "HOME"));
    cbm_setenv("HOME", tmpdir, 1);
    char *args[] = {"-n"};
    ASSERT_EQ(cbm_cmd_uninstall(1, args), 0);

    const char *contents = read_test_file(profile_mcp);
    ASSERT_NOT_NULL(contents);
    ASSERT(strstr(contents, "user-server") != NULL);
    ASSERT(strstr(contents, "codebase-memory-mcp") == NULL);

    extern void cbm_set_auto_answer_for_test(int value);
    cbm_set_auto_answer_for_test(0);
    cli_env_restore(&home);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_install_help_does_not_require_home) {
    ASSERT_EQ(cli_run_help_without_home(cbm_cmd_install), 0);
    PASS();
}

TEST(cli_uninstall_help_does_not_require_home) {
    ASSERT_EQ(cli_run_help_without_home(cbm_cmd_uninstall), 0);
    PASS();
}

TEST(cli_update_help_does_not_require_home) {
    ASSERT_EQ(cli_run_help_without_home(cbm_cmd_update), 0);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Full install + uninstall lifecycle
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_install_and_uninstall) {
    /* Port of TestCLI_InstallAndUninstall */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-full-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char skills_dir[512];
    snprintf(skills_dir, sizeof(skills_dir), "%s/.claude/skills", tmpdir);

    /* Install */
    int written = cbm_install_skills(skills_dir, false, false);
    ASSERT_EQ(written, CBM_SKILL_COUNT);

    /* Verify */
    const cbm_skill_t *sk = cbm_get_skills();
    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s/SKILL.md", skills_dir, sk[i].name);
        struct stat st;
        ASSERT_EQ(stat(path, &st), 0);
    }

    /* Uninstall */
    int removed = cbm_remove_skills(skills_dir, false);
    ASSERT_EQ(removed, CBM_SKILL_COUNT);

    /* Verify removed */
    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", skills_dir, sk[i].name);
        struct stat st;
        ASSERT(stat(path, &st) != 0);
    }

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_agent_install_reports_safe_editor_refusal) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-install-refusal-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char config_dir[512];
    char config_path[640];
    snprintf(config_dir, sizeof(config_dir), "%s/.openclaw", tmpdir);
    snprintf(config_path, sizeof(config_path), "%s/openclaw.json", config_dir);
    test_mkdirp(config_dir);
    const char *malformed = "{ invalid config\n";
    write_test_file(config_path, malformed);

    char *saved_path = save_test_env("PATH");
    cbm_setenv("PATH", tmpdir, 1);
    int rc = cbm_install_agent_configs(tmpdir, "/usr/local/bin/codebase-memory-mcp", false, false);
    char *after = read_test_file_alloc(config_path);
    bool preserved = after && strcmp(after, malformed) == 0;

    free(after);
    restore_test_env("PATH", saved_path);
    test_rmdir_r(tmpdir);
    if (rc == 0 || !preserved)
        FAIL("agent install must return failure when a safe editor refuses a config");
    PASS();
}

TEST(cli_agent_uninstall_reports_safe_editor_refusal) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-uninstall-refusal-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char config_dir[512];
    char config_path[640];
    snprintf(config_dir, sizeof(config_dir), "%s/.openclaw", tmpdir);
    snprintf(config_path, sizeof(config_path), "%s/openclaw.json", config_dir);
    test_mkdirp(config_dir);
    const char *malformed = "{ invalid config\n";
    write_test_file(config_path, malformed);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    char *argv[] = {"uninstall", "--yes"};
    int rc = cbm_cmd_uninstall(2, argv);
    char *after = read_test_file_alloc(config_path);
    bool preserved = after && strcmp(after, malformed) == 0;

    free(after);
    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    test_rmdir_r(tmpdir);
    if (rc == 0 || !preserved)
        FAIL("agent uninstall must return failure when a safe editor refuses a config");
    PASS();
}

TEST(cli_special_hook_failures_propagate_from_install_and_uninstall) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-special-hook-refusal-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char factory_dir[512];
    char hooks_path[640];
    snprintf(factory_dir, sizeof(factory_dir), "%s/.factory", tmpdir);
    snprintf(hooks_path, sizeof(hooks_path), "%s/hooks.json", factory_dir);
    test_mkdirp(factory_dir);
    write_test_file(hooks_path, "[]\n");

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    int install_rc = cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);
    char *args[] = {"-n"};
    int uninstall_rc = cbm_cmd_uninstall(1, args);

    char *after = read_test_file_alloc(hooks_path);
    bool unchanged = after && strcmp(after, "[]\n") == 0;
#ifdef _WIN32
    bool results_ok = install_rc == 0 && uninstall_rc != 0;
#else
    bool results_ok = install_rc != 0 && uninstall_rc != 0;
#endif
    free(after);
    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    test_rmdir_r(tmpdir);
    if (!results_ok || !unchanged)
        FAIL("special hook editor failures must propagate without changing foreign content");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  YAML parser unit tests
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_yaml_parse_simple) {
    /* Basic key-value parsing */
    const char *yaml = "name: test\nversion: 1.0\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "name"), "test");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "version"), "1.0");
    cbm_yaml_free(root);
    PASS();
}

TEST(cli_yaml_parse_nested) {
    /* Nested map */
    const char *yaml = "parent:\n"
                       "  child: value\n"
                       "  number: 42\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "parent.child"), "value");
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "parent.number", 0), 42.0, 0.001);
    cbm_yaml_free(root);
    PASS();
}

TEST(cli_yaml_parse_list) {
    /* String list */
    const char *yaml = "items:\n"
                       "  - alpha\n"
                       "  - beta\n"
                       "  - gamma\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *items[8];
    int count = cbm_yaml_get_str_list(root, "items", items, 8);
    ASSERT_EQ(count, 3);
    ASSERT_STR_EQ(items[0], "alpha");
    ASSERT_STR_EQ(items[1], "beta");
    ASSERT_STR_EQ(items[2], "gamma");
    cbm_yaml_free(root);
    PASS();
}

TEST(cli_yaml_parse_bool) {
    const char *yaml = "enabled: true\n"
                       "disabled: false\n"
                       "on_flag: yes\n"
                       "off_flag: no\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_get_bool(root, "enabled", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "disabled", true));
    ASSERT_TRUE(cbm_yaml_get_bool(root, "on_flag", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "off_flag", true));
    cbm_yaml_free(root);
    PASS();
}

TEST(cli_yaml_parse_comments) {
    const char *yaml = "# This is a comment\n"
                       "key: value # inline comment\n"
                       "\n"
                       "# Another comment\n"
                       "other: data\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "key"), "value");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "other"), "data");
    cbm_yaml_free(root);
    PASS();
}

TEST(cli_yaml_parse_empty) {
    cbm_yaml_node_t *root = cbm_yaml_parse("", 0);
    ASSERT_NOT_NULL(root);
    ASSERT_NULL(cbm_yaml_get_str(root, "anything"));
    cbm_yaml_free(root);
    PASS();
}

TEST(cli_yaml_has) {
    const char *yaml = "a:\n  b: c\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_has(root, "a"));
    ASSERT_TRUE(cbm_yaml_has(root, "a.b"));
    ASSERT_FALSE(cbm_yaml_has(root, "a.c"));
    ASSERT_FALSE(cbm_yaml_has(root, "x"));
    cbm_yaml_free(root);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group A: Agent Detection
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_detect_agents_finds_claude) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.claude", tmpdir);
    test_mkdirp(dir);

    /* Unset CLAUDE_CONFIG_DIR so detection is exercised against home_dir/.claude
     * and the runner's real env (which may set it) does not leak in. */
    const char *saved_ccd = getenv("CLAUDE_CONFIG_DIR");
    char *saved_ccd_copy = saved_ccd ? strdup(saved_ccd) : NULL;
    cbm_unsetenv("CLAUDE_CONFIG_DIR");

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.claude_code);

    if (saved_ccd_copy) {
        cbm_setenv("CLAUDE_CONFIG_DIR", saved_ccd_copy, 1);
        free(saved_ccd_copy);
    }

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_detect_agents_finds_claude_via_env) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Config dir lives OUTSIDE home_dir/.claude, pointed at by CLAUDE_CONFIG_DIR. */
    char ccd[512];
    snprintf(ccd, sizeof(ccd), "%s/custom-claude", tmpdir);
    test_mkdirp(ccd);

    const char *saved_ccd = getenv("CLAUDE_CONFIG_DIR");
    char *saved_ccd_copy = saved_ccd ? strdup(saved_ccd) : NULL;
    cbm_setenv("CLAUDE_CONFIG_DIR", ccd, 1);

    /* home_dir has no .claude, but detection must still find Claude via the env var. */
    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.claude_code);

    if (saved_ccd_copy) {
        cbm_setenv("CLAUDE_CONFIG_DIR", saved_ccd_copy, 1);
        free(saved_ccd_copy);
    } else {
        cbm_unsetenv("CLAUDE_CONFIG_DIR");
    }

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_detect_agents_finds_codex) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.codex", tmpdir);
    test_mkdirp(dir);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.codex);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_standalone_kilo_install_plan_and_uninstall_preserve_foreign_entries) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-kilo-standalone-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    char config_path[768];
    snprintf(config_dir, sizeof(config_dir), "%s/.config/kilo", tmpdir);
    ASSERT_EQ(test_mkdirp(config_dir), 0);
    snprintf(config_path, sizeof(config_path), "%s/kilo.jsonc", config_dir);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.kilo_cli);

    char *plan = cbm_build_install_plan_json(tmpdir, "/usr/local/bin/codebase-memory-mcp");
    ASSERT_NOT_NULL(plan);
    ASSERT(strstr(plan, "\"kilo-cli\"") != NULL);
    ASSERT(strstr(plan, ".config/kilo/kilo.jsonc") != NULL);
    free(plan);

    ASSERT_EQ(write_test_file(
                  config_path,
                  "{\n  // user-owned server\n  \"mcp\": {\n    \"foreign\": {\"type\": \"local\", "
                  "\"command\": [\"keep-me\"]},\n  },\n}\n"),
              0);
    char binary[768];
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp", tmpdir);
    ASSERT_EQ(cbm_upsert_opencode_mcp(binary, config_path), 0);

    cli_env_snapshot_t home = {0};
    ASSERT_TRUE(cli_env_snapshot(&home, "HOME"));
    cbm_setenv("HOME", tmpdir, 1);
    char *args[] = {"-n"};
    ASSERT_EQ(cbm_cmd_uninstall(1, args), 0);
    cli_env_restore(&home);

    const char *contents = read_test_file(config_path);
    ASSERT_NOT_NULL(contents);
    ASSERT(strstr(contents, "keep-me") != NULL);
    ASSERT(strstr(contents, "codebase-memory-mcp") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

/* A pre-consolidation installer release wrote local-array MCP entries with an
 * extra "enabled": true member: {"enabled":true,"type":"local","command":[bin]}.
 * Upsert must recognize that exact released shape as installer-owned and
 * rewrite it canonically, and owned removal must delete it; an entry with
 * "enabled": false is user-modified and must still be refused. */
TEST(cli_json_mcp_migrates_legacy_enabled_true_entry) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-legacy-enabled-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_path[768];
    snprintf(config_path, sizeof(config_path), "%s/kilo.jsonc", tmpdir);
    char binary[768];
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp", tmpdir);

    char legacy[1600];
    snprintf(legacy, sizeof(legacy),
             "{\n  \"mcp\": {\n    \"codebase-memory-mcp\": {\n      \"enabled\": true,\n"
             "      \"type\": \"local\",\n      \"command\": [\"%s\"]\n    }\n  }\n}\n",
             binary);
    ASSERT_EQ(write_test_file(config_path, legacy), 0);
    ASSERT_EQ(cbm_upsert_opencode_mcp(binary, config_path), 0);

    const char *contents = read_test_file(config_path);
    ASSERT_NOT_NULL(contents);
    ASSERT(strstr(contents, "\"enabled\"") == NULL);
    ASSERT(strstr(contents, binary) != NULL);

    /* Owned removal must also recognize the legacy released shape. */
    ASSERT_EQ(write_test_file(config_path, legacy), 0);
    ASSERT_EQ(cbm_remove_opencode_mcp_owned(binary, config_path), 0);
    contents = read_test_file(config_path);
    ASSERT_NOT_NULL(contents);
    ASSERT(strstr(contents, "codebase-memory-mcp\"") == NULL || strstr(contents, binary) == NULL);

    /* "enabled": false was never a released shape: refuse to overwrite it. */
    char modified[1600];
    snprintf(modified, sizeof(modified),
             "{\n  \"mcp\": {\n    \"codebase-memory-mcp\": {\n      \"enabled\": false,\n"
             "      \"type\": \"local\",\n      \"command\": [\"%s\"]\n    }\n  }\n}\n",
             binary);
    ASSERT_EQ(write_test_file(config_path, modified), 0);
    ASSERT(cbm_upsert_opencode_mcp(binary, config_path) != 0);
    contents = read_test_file(config_path);
    ASSERT_NOT_NULL(contents);
    ASSERT(strstr(contents, "\"enabled\": false") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

/* issue #222: Cursor (~/.cursor/) must be detected so install/update registers
 * the MCP server in ~/.cursor/mcp.json — previously it was never discovered. */
TEST(cli_detect_agents_finds_cursor_issue222) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.cursor", tmpdir);
    test_mkdirp(dir);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.cursor);

    test_rmdir_r(tmpdir);
    PASS();
}

/* issue #388: `install --plan` must emit a machine-readable receipt of planned
 * writes WITHOUT mutating any config (the pre-mutation trust primitive). */
TEST(cli_install_plan_receipt_no_mutation_issue388) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-plan-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Make Claude Code + Cursor + Codex "detected". */
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.claude", tmpdir);
    test_mkdirp(dir);
    snprintf(dir, sizeof(dir), "%s/.cursor", tmpdir);
    test_mkdirp(dir);
    snprintf(dir, sizeof(dir), "%s/.codex", tmpdir);
    test_mkdirp(dir);

    char *json = cbm_build_install_plan_json(tmpdir, "/usr/local/bin/codebase-memory-mcp");
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "agent.install.plan.v1") != NULL);
    ASSERT(strstr(json, "writes_started") != NULL);
    ASSERT(strstr(json, "next_safe_command") != NULL);
    ASSERT(strstr(json, "cursor") != NULL);
    ASSERT(strstr(json, "skill_dirs_planned") != NULL);
    ASSERT(strstr(json, ".claude/skills") != NULL);
    ASSERT(strstr(json, ".cursor/mcp.json") != NULL);
    ASSERT(strstr(json, ".codex/config.toml") != NULL);
    const char *instrs = strstr(json, "\"instruction_files_planned\"");
    ASSERT_NOT_NULL(instrs);
    const char *skill_dirs = strstr(json, "\"skill_dirs_planned\"");
    ASSERT_NOT_NULL(skill_dirs);
    ASSERT(instrs < skill_dirs);
    const char *misclassified = strstr(instrs, ".claude/skills");
    ASSERT(misclassified == NULL || misclassified > skill_dirs);
    free(json);

    /* Critical: building the plan must NOT have created any config file. */
    char cfg[512];
    struct stat st;
    snprintf(cfg, sizeof(cfg), "%s/.cursor/mcp.json", tmpdir);
    ASSERT(stat(cfg, &st) != 0); /* must not exist */
    snprintf(cfg, sizeof(cfg), "%s/.codex/config.toml", tmpdir);
    ASSERT(stat(cfg, &st) != 0); /* must not exist */

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_reference_harnesses_are_planned_without_mutation) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-reference-harnesses-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *dirs[] = {".qwen", ".codeium/windsurf"};
    for (size_t i = 0; i < sizeof(dirs) / sizeof(dirs[0]); i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", tmpdir, dirs[i]);
        ASSERT_EQ(test_mkdirp(path), 0);
    }

    char *json = cbm_build_install_plan_json(tmpdir, "/usr/local/bin/codebase-memory-mcp");
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, ".qwen/settings.json") != NULL);
    ASSERT(strstr(json, ".qwen/QWEN.md") != NULL);
    ASSERT(strstr(json, ".codeium/windsurf/mcp_config.json") != NULL);
    const char *detected = strstr(json, "\"agents_detected\"");
    const char *configs = strstr(json, "\"config_files_planned\"");
    ASSERT_NOT_NULL(detected);
    ASSERT_NOT_NULL(configs);
    const char *qwen_detected = strstr(detected, "\"qwen\"");
    const char *windsurf_detected = strstr(detected, "\"windsurf\"");
    ASSERT(qwen_detected != NULL && qwen_detected < configs);
    ASSERT(windsurf_detected != NULL && windsurf_detected < configs);

    /* Plan mode must not publish any of the planned files. */
    char path[768];
    struct stat st;
    snprintf(path, sizeof(path), "%s/.qwen/settings.json", tmpdir);
    ASSERT_NEQ(stat(path, &st), 0);
    snprintf(path, sizeof(path), "%s/.codeium/windsurf/mcp_config.json", tmpdir);
    ASSERT_NEQ(stat(path, &st), 0);

    free(json);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_claude_desktop_plan_and_uninstall_preserve_foreign_entries) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-claude-desktop-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    char config_path[768];
#ifdef __APPLE__
    snprintf(config_dir, sizeof(config_dir), "%s/Library/Application Support/Claude", tmpdir);
#elif defined(_WIN32)
    snprintf(config_dir, sizeof(config_dir), "%s/AppData/Roaming/Claude", tmpdir);
#else
    snprintf(config_dir, sizeof(config_dir), "%s/.config/Claude", tmpdir);
#endif
    ASSERT_EQ(test_mkdirp(config_dir), 0);
    snprintf(config_path, sizeof(config_path), "%s/claude_desktop_config.json", config_dir);

    char *json = cbm_build_install_plan_json(tmpdir, "/usr/local/bin/codebase-memory-mcp");
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"claude-desktop\"") != NULL);
    ASSERT(strstr(json, "claude_desktop_config.json") != NULL);
    struct stat st;
    ASSERT_NEQ(stat(config_path, &st), 0);
    free(json);

    ASSERT_EQ(
        write_test_file(config_path, "{\"mcpServers\":{\"foreign\":{\"command\":\"keep-me\"}}}"),
        0);
    char binary[768];
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp", tmpdir);
    ASSERT_EQ(cbm_install_editor_mcp(binary, config_path), 0);

    cli_env_snapshot_t home = {0};
    ASSERT_TRUE(cli_env_snapshot(&home, "HOME"));
    cbm_setenv("HOME", tmpdir, 1);
    char *args[] = {"-n"};
    ASSERT_EQ(cbm_cmd_uninstall(1, args), 0);
    cli_env_restore(&home);

    const char *contents = read_test_file(config_path);
    ASSERT_NOT_NULL(contents);
    ASSERT(strstr(contents, "keep-me") != NULL);
    ASSERT(strstr(contents, "codebase-memory-mcp") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_reference_harnesses_uninstall_owned_entries_only) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-reference-uninstall-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *dirs[] = {".qwen", ".codeium/windsurf"};
    for (size_t i = 0; i < sizeof(dirs) / sizeof(dirs[0]); i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", tmpdir, dirs[i]);
        ASSERT_EQ(test_mkdirp(path), 0);
    }

    const char *config_rel[] = {".qwen/settings.json", ".codeium/windsurf/mcp_config.json"};
    char binary[768];
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp", tmpdir);
    char config_paths[2][768];
    for (size_t i = 0; i < 2; i++) {
        snprintf(config_paths[i], sizeof(config_paths[i]), "%s/%s", tmpdir, config_rel[i]);
        ASSERT_EQ(write_test_file(
                      config_paths[i],
                      "{\"mcpServers\":{\"foreign\":{\"command\":\"keep-me\"}}}"),
                  0);
        ASSERT_EQ(cbm_install_editor_mcp(binary, config_paths[i]), 0);
    }

    const char *instruction_rel[] = {".qwen/QWEN.md"};
    char instruction_paths[1][768];
    for (size_t i = 0; i < 1; i++) {
        snprintf(instruction_paths[i], sizeof(instruction_paths[i]), "%s/%s", tmpdir,
                 instruction_rel[i]);
        ASSERT_EQ(write_test_file(instruction_paths[i], "user-authored guidance\n"), 0);
        ASSERT_EQ(cbm_upsert_instructions(instruction_paths[i], cbm_get_agent_instructions()), 0);
    }

    cli_env_snapshot_t home = {0};
    ASSERT_TRUE(cli_env_snapshot(&home, "HOME"));
    cbm_setenv("HOME", tmpdir, 1);
    char *args[] = {"-n"};
    ASSERT_EQ(cbm_cmd_uninstall(1, args), 0);

    for (size_t i = 0; i < 2; i++) {
        const char *contents = read_test_file(config_paths[i]);
        ASSERT_NOT_NULL(contents);
        ASSERT(strstr(contents, "keep-me") != NULL);
        ASSERT(strstr(contents, "codebase-memory-mcp") == NULL);
    }
    for (size_t i = 0; i < 1; i++) {
        const char *contents = read_test_file(instruction_paths[i]);
        ASSERT_NOT_NULL(contents);
        ASSERT(strstr(contents, "user-authored guidance") != NULL);
        ASSERT(strstr(contents, "codebase-memory-mcp:start") == NULL);
    }

    extern void cbm_set_auto_answer_for_test(int value);
    cbm_set_auto_answer_for_test(0);
    cli_env_restore(&home);
    test_rmdir_r(tmpdir);
    PASS();
}

/* issue #330: Codex SessionStart reminder hook in config.toml — installed,
 * idempotent, preserves other content, and cleanly removed. */
TEST(cli_codex_session_hook_issue330) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codexhook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char cfg[512];
    snprintf(cfg, sizeof(cfg), "%s/config.toml", tmpdir);
    write_test_file(cfg, "[mcp_servers.other]\ncommand = \"x\"\n");

    ASSERT_EQ(cbm_upsert_codex_hooks(cfg), 0);
    const char *d = read_test_file(cfg);
    ASSERT_NOT_NULL(d);
    ASSERT(strstr(d, "[[hooks.SessionStart]]") != NULL);
    ASSERT(strstr(d, "[[hooks.SessionStart.hooks]]") != NULL);
    ASSERT(strstr(d, "codebase-memory-mcp hook-augment") != NULL);
    ASSERT(strstr(d, "[mcp_servers.other]") != NULL); /* pre-existing content preserved */
    /* Idempotent: a second upsert leaves exactly ONE hook block. */
    ASSERT_EQ(cbm_upsert_codex_hooks(cfg), 0);
    d = read_test_file(cfg);
    const char *first = strstr(d, "[[hooks.SessionStart]]");
    ASSERT_NOT_NULL(first);
    ASSERT_NULL(strstr(first + 1, "[[hooks.SessionStart]]"));

    ASSERT_EQ(cbm_remove_codex_hooks(cfg), 0);
    d = read_test_file(cfg);
    ASSERT_NULL(strstr(d, "hooks.SessionStart"));
    ASSERT_NULL(strstr(d, "hooks.SubagentStart"));
    ASSERT(strstr(d, "[mcp_servers.other]") != NULL); /* still preserved after removal */

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_codex_mcp_and_hook_upserts_are_idempotent) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codexhook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char cfg[512];
    snprintf(cfg, sizeof(cfg), "%s/config.toml", tmpdir);
    write_test_file(cfg, "model = \"gpt-5\"\n");

    ASSERT_EQ(cbm_upsert_codex_mcp("/usr/local/bin/codebase-memory-mcp", cfg), 0);
    ASSERT_EQ(cbm_upsert_codex_hooks(cfg), 0);
    ASSERT_EQ(cbm_upsert_codex_mcp("/usr/local/bin/codebase-memory-mcp", cfg), 0);
    ASSERT_EQ(cbm_upsert_codex_hooks(cfg), 0);

    const char *d = read_test_file(cfg);
    ASSERT_NOT_NULL(d);
    ASSERT_EQ(count_substr(d, "[mcp_servers.codebase-memory-mcp]"), 1);
    ASSERT_EQ(count_substr(d, "# >>> codebase-memory-mcp SessionStart >>>"), 1);
    ASSERT_EQ(count_substr(d, "# <<< codebase-memory-mcp SessionStart <<<"), 1);
    ASSERT_EQ(count_substr(d, "[[hooks.SessionStart]]"), 1);
    ASSERT_EQ(count_substr(d, "[[hooks.SessionStart.hooks]]"), 1);
    ASSERT(strstr(d, "model = \"gpt-5\"") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_codex_hook_upsert_rejects_orphan_end_sentinel) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codexhook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char cfg[512];
    snprintf(cfg, sizeof(cfg), "%s/config.toml", tmpdir);
    write_test_file(cfg, "model = \"gpt-5\"\n"
                         "\n"
                         "[[hooks.SessionStart]]\n"
                         "[[hooks.SessionStart.hooks]]\n"
                         "type = \"command\"\n"
                         "command = 'echo old'\n"
                         "# <<< codebase-memory-mcp SessionStart <<<\n"
                         "\n"
                         "# >>> codebase-memory-mcp SessionStart >>>\n"
                         "[[hooks.SessionStart]]\n"
                         "[[hooks.SessionStart.hooks]]\n"
                         "type = \"command\"\n"
                         "command = 'echo duplicate'\n"
                         "# <<< codebase-memory-mcp SessionStart <<<\n");

    const char *before = read_test_file(cfg);
    ASSERT_NOT_NULL(before);
    char *snapshot = strdup(before);
    ASSERT_NOT_NULL(snapshot);

    ASSERT_EQ(cbm_upsert_codex_hooks(cfg), -1);

    const char *d = read_test_file(cfg);
    ASSERT_NOT_NULL(d);
    ASSERT_STR_EQ(d, snapshot);
    free(snapshot);

    test_rmdir_r(tmpdir);
    PASS();
}

/* Gemini/Antigravity SessionStart reminder parity (settings.json JSON path). */
TEST(cli_gemini_session_hook_parity) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-gemhook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char cfg[512];
    snprintf(cfg, sizeof(cfg), "%s/settings.json", tmpdir);

    ASSERT_EQ(cbm_upsert_gemini_session_hooks(cfg, "/opt/cbm/bin/codebase-memory-mcp"), 0);
    const char *d = read_test_file(cfg);
    ASSERT_NOT_NULL(d);
    ASSERT(strstr(d, "SessionStart") != NULL);
    ASSERT(strstr(d, "hook-augment") != NULL);
    ASSERT(strstr(d, "--dialect gemini") == NULL);
    ASSERT(strstr(d, "get_code_snippet") == NULL);
    ASSERT(strstr(d, "run index_repository first") == NULL);
    ASSERT(strstr(d, "\"matcher\": \"startup\"") != NULL);
    ASSERT(strstr(d, "\"matcher\": \"resume\"") != NULL);
    ASSERT(strstr(d, "\"matcher\": \"clear\"") != NULL);
    ASSERT(strstr(d, "startup|resume|clear") == NULL);

    ASSERT_EQ(cbm_remove_gemini_session_hooks(cfg, "/opt/cbm/bin/codebase-memory-mcp"), 0);
    d = read_test_file(cfg);
    ASSERT_NULL(strstr(d, "SessionStart"));

    test_rmdir_r(tmpdir);
    PASS();
}

/* Claude SubagentStart reminder: subagents spawned via the Agent tool do not
 * fire SessionStart, so this hook is their code-discovery channel. Verify the
 * install shape, idempotent re-install, and clean removal. */
TEST(cli_claude_subagent_hook) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-subhook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char cfg[512];
    snprintf(cfg, sizeof(cfg), "%s/settings.json", tmpdir);

    ASSERT_EQ(cbm_upsert_claude_subagent_hooks(cfg), 0);
    const char *d = read_test_file(cfg);
    ASSERT_NOT_NULL(d);
    ASSERT(strstr(d, "SubagentStart") != NULL);
    ASSERT(strstr(d, "\"*\"") != NULL);                 /* match-all matcher */
    ASSERT(strstr(d, "cbm-subagent-reminder") != NULL); /* points at the hook script */

    /* Idempotent: a second upsert must not duplicate our entry. */
    ASSERT_EQ(cbm_upsert_claude_subagent_hooks(cfg), 0);
    d = read_test_file(cfg);
    ASSERT_NOT_NULL(d);
    int count = 0;
    for (const char *p = d; (p = strstr(p, "cbm-subagent-reminder")) != NULL; p++)
        count++;
    ASSERT_EQ(count, 1);

    ASSERT_EQ(cbm_remove_claude_subagent_hooks(cfg), 0);
    d = read_test_file(cfg);
    ASSERT_NULL(strstr(d, "SubagentStart"));

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_claude_hook_mutation_converges_mixed_owned_duplicates) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX fixture for platform-neutral hook mutation");
#else
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-duplicates-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    char cfg[640];
    char current_command[1024];
    char released_command[1024];
    char original[8192];
    snprintf(config_dir, sizeof(config_dir), "%s/.claude", tmpdir);
    snprintf(cfg, sizeof(cfg), "%s/settings.json", config_dir);
    snprintf(released_command, sizeof(released_command), "%s/hooks/cbm-subagent-reminder",
             config_dir);
    test_mkdirp(config_dir);

    char *saved_config = save_test_env("CLAUDE_CONFIG_DIR");
    cbm_setenv("CLAUDE_CONFIG_DIR", config_dir, 1);
    ASSERT_EQ(cbm_resolve_claude_hook_command_for_testing("cbm-subagent-reminder", false,
                                                          current_command, sizeof(current_command)),
              0);
    snprintf(original, sizeof(original),
             "{\"hooks\":{\"SubagentStart\":["
             "{\"matcher\":\"*\",\"hooks\":[{\"type\":\"command\",\"command\":\"%s\"}]},"
             "{\"matcher\":\"*\",\"hooks\":[{\"type\":\"command\",\"command\":\"%s\"}]},"
             "{\"matcher\":\"*\",\"hooks\":[{\"type\":\"command\","
             "\"command\":\"echo user-subagent-hook\"}]}]}}\n",
             current_command, released_command);

    write_test_file(cfg, original);
    int upsert_rc = cbm_upsert_claude_subagent_hooks(cfg);
    char *after_upsert = read_test_file_alloc(cfg);
    bool converged = upsert_rc == 0 && after_upsert &&
                     test_count_substring(after_upsert, "cbm-subagent-reminder") == 1U &&
                     strstr(after_upsert, "echo user-subagent-hook");
    free(after_upsert);

    write_test_file(cfg, original);
    int remove_rc = cbm_remove_claude_subagent_hooks(cfg);
    char *after_remove = read_test_file_alloc(cfg);
    bool removed_all = remove_rc == 0 && after_remove &&
                       !strstr(after_remove, "cbm-subagent-reminder") &&
                       strstr(after_remove, "echo user-subagent-hook");
    free(after_remove);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_config);
    test_rmdir_r(tmpdir);

    if (!converged || !removed_all)
        FAIL("hook mutation must converge mixed exact-owned duplicates while preserving foreign "
             "siblings");
#endif
    PASS();
}

/* A user's own catch-all ("*") SubagentStart hook must survive CMM install and
 * uninstall: ownership is keyed on the command, not just the "*" matcher. */
TEST(cli_claude_subagent_hook_preserves_user_entry) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-subuser-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char cfg[512];
    snprintf(cfg, sizeof(cfg), "%s/settings.json", tmpdir);
    /* Pre-existing user SubagentStart hook, also matcher "*", different command. */
    write_test_file(
        cfg, "{\"hooks\":{\"SubagentStart\":[{\"matcher\":\"*\","
             "\"hooks\":[{\"type\":\"command\",\"command\":\"echo user-subagent-hook\"}]}]}}");

    /* Install CMM's hook: the user's "*" entry must remain, ours added alongside. */
    ASSERT_EQ(cbm_upsert_claude_subagent_hooks(cfg), 0);
    const char *d = read_test_file(cfg);
    ASSERT_NOT_NULL(d);
    ASSERT(strstr(d, "echo user-subagent-hook") != NULL); /* user's hook untouched */
    ASSERT(strstr(d, "cbm-subagent-reminder") != NULL);   /* ours added */

    /* Remove CMM's hook: the user's entry must still be intact, ours gone. */
    ASSERT_EQ(cbm_remove_claude_subagent_hooks(cfg), 0);
    d = read_test_file(cfg);
    ASSERT_NOT_NULL(d);
    ASSERT(strstr(d, "echo user-subagent-hook") != NULL); /* user's hook preserved */
    ASSERT_NULL(strstr(d, "cbm-subagent-reminder"));      /* only ours removed */

    test_rmdir_r(tmpdir);
    PASS();
}

/* SessionStart source matchers are common user choices. Matching a source is
 * not ownership proof: install must retain a foreign command with the same
 * matcher and add the codebase-memory hook alongside it. */
TEST(cli_claude_session_hook_preserves_user_entry) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-session-user-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    char settings_path[640];
    snprintf(config_dir, sizeof(config_dir), "%s/.claude", tmpdir);
    snprintf(settings_path, sizeof(settings_path), "%s/settings.json", config_dir);
    test_mkdirp(config_dir);
    write_test_file(settings_path, "{\"hooks\":{\"SessionStart\":[{\"matcher\":\"startup\","
                                   "\"hooks\":[{\"type\":\"command\","
                                   "\"command\":\"echo user-session-hook\"}]}]}}\n");

    char *saved_path = save_test_env("PATH");
    char *saved_config = save_test_env("CLAUDE_CONFIG_DIR");
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_install_agent_configs(tmpdir, "/usr/local/bin/codebase-memory-mcp", false, false);

    char *installed = read_test_file_alloc(settings_path);
    bool preserved = installed && strstr(installed, "echo user-session-hook") &&
                     strstr(installed, "cbm-session-reminder");
    free(installed);
    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_config);
    test_rmdir_r(tmpdir);
    if (!preserved)
        FAIL("Claude SessionStart install must preserve foreign hooks sharing a matcher");
    PASS();
}

/* Session/subagent augmentation must use the same bounded compiled path as the
 * PreToolUse augmenter. Static shell payloads cannot resolve the active graph
 * project and drift independently from the tested hook JSON contract. */
TEST(cli_claude_lifecycle_hooks_delegate_to_augmenter) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-lifecycle-hooks-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    snprintf(config_dir, sizeof(config_dir), "%s/.claude", tmpdir);
    test_mkdirp(config_dir);

    char *saved_path = save_test_env("PATH");
    char *saved_claude = save_test_env("CLAUDE_CONFIG_DIR");
    char *saved_codex = save_test_env("CODEX_HOME");
    char *saved_opencode = save_test_env("OPENCODE_CONFIG");
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_unsetenv("CODEX_HOME");
    cbm_unsetenv("OPENCODE_CONFIG");

    const char *binary = "/opt/codebase memory/bin/codebase-memory-mcp";
    cbm_install_agent_configs(tmpdir, binary, false, false);

    char session_path[640];
    char subagent_path[640];
    char settings_path[640];
#ifdef _WIN32
    snprintf(session_path, sizeof(session_path), "%s/hooks/cbm-session-reminder.cmd", config_dir);
    snprintf(subagent_path, sizeof(subagent_path), "%s/hooks/cbm-subagent-reminder.cmd",
             config_dir);
#else
    snprintf(session_path, sizeof(session_path), "%s/hooks/cbm-session-reminder", config_dir);
    snprintf(subagent_path, sizeof(subagent_path), "%s/hooks/cbm-subagent-reminder", config_dir);
#endif
    snprintf(settings_path, sizeof(settings_path), "%s/settings.json", config_dir);
    char *session = read_test_file_alloc(session_path);
    char *subagent = read_test_file_alloc(subagent_path);
    char *settings = read_test_file_alloc(settings_path);

    bool delegates = session && subagent && strstr(session, binary) && strstr(subagent, binary) &&
                     strstr(session, "hook-augment") && strstr(subagent, "hook-augment");
    bool no_static_payload =
        session && subagent && !strstr(session, "cat <<") && !strstr(subagent, "cat <<");
    bool events_installed =
        settings && strstr(settings, "SessionStart") && strstr(settings, "SubagentStart");
    const char *session_event = settings ? strstr(settings, "\"SessionStart\"") : NULL;
    const char *subagent_event = settings ? strstr(settings, "\"SubagentStart\"") : NULL;
    const char *session_timeout = session_event ? strstr(session_event, "\"timeout\": 5") : NULL;
    const char *subagent_timeout = subagent_event ? strstr(subagent_event, "\"timeout\": 5") : NULL;
    bool lifecycle_timeouts =
        session_timeout && subagent_event && session_timeout < subagent_event && subagent_timeout;

    free(session);
    free(subagent);
    free(settings);
    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_claude);
    restore_test_env("CODEX_HOME", saved_codex);
    restore_test_env("OPENCODE_CONFIG", saved_opencode);
    test_rmdir_r(tmpdir);

    if (!delegates || !no_static_payload || !events_installed || !lifecycle_timeouts)
        FAIL("SessionStart and SubagentStart must delegate to the compiled augmenter with bounded "
             "timeouts");
    PASS();
}

TEST(cli_copilot_install_preserves_foreign_named_manifest) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-copilot-foreign-install-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char hooks_dir[512];
    char manifest_path[640];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.copilot/hooks", tmpdir);
    snprintf(manifest_path, sizeof(manifest_path), "%s/codebase-memory-mcp.json", hooks_dir);
    test_mkdirp(hooks_dir);
    const char *foreign = "{\"version\":1,\"hooks\":{\"sessionStart\":[{\"type\":\"command\","
                          "\"bash\":\"user-hook\"}]},\"owner\":\"user\"}\n";
    write_test_file(manifest_path, foreign);

    char *saved_path = save_test_env("PATH");
    char *saved_copilot = save_test_env("COPILOT_HOME");
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("COPILOT_HOME");
    cbm_install_agent_configs(tmpdir, "/usr/local/bin/codebase-memory-mcp", false, false);
    char *after = read_test_file_alloc(manifest_path);
    bool preserved = after && strcmp(after, foreign) == 0;

    free(after);
    restore_test_env("PATH", saved_path);
    restore_test_env("COPILOT_HOME", saved_copilot);
    test_rmdir_r(tmpdir);
    if (!preserved)
        FAIL("Copilot install must fail closed on a foreign same-named hook manifest");
    PASS();
}

TEST(cli_copilot_uninstall_preserves_foreign_named_manifest) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-copilot-foreign-uninstall-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char hooks_dir[512];
    char manifest_path[640];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.copilot/hooks", tmpdir);
    snprintf(manifest_path, sizeof(manifest_path), "%s/codebase-memory-mcp.json", hooks_dir);
    test_mkdirp(hooks_dir);
    const char *foreign = "{\"version\":1,\"hooks\":{\"sessionStart\":[{\"type\":\"command\","
                          "\"bash\":\"user-hook\"}]},\"owner\":\"user\"}\n";
    write_test_file(manifest_path, foreign);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_copilot = save_test_env("COPILOT_HOME");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("COPILOT_HOME");
    char *argv[] = {"uninstall", "--yes"};
    int rc = cbm_cmd_uninstall(2, argv);
    char *after = read_test_file_alloc(manifest_path);
    bool preserved = after && strcmp(after, foreign) == 0;

    free(after);
    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("COPILOT_HOME", saved_copilot);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !preserved)
        FAIL("Copilot uninstall must preserve a foreign same-named hook manifest");
    PASS();
}

TEST(cli_copilot_uninstall_preserves_canonical_shaped_foreign_manifest) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-copilot-canonical-foreign-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char hooks_dir[512];
    char manifest_path[640];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.copilot/hooks", tmpdir);
    snprintf(manifest_path, sizeof(manifest_path), "%s/codebase-memory-mcp.json", hooks_dir);
    test_mkdirp(hooks_dir);
    const char *foreign =
        "{\"version\":1,\"hooks\":{"
        "\"sessionStart\":[{\"type\":\"command\","
        "\"bash\":\"/opt/foreign/cbm hook-augment --event SessionStart --dialect copilot\","
        "\"powershell\":\"& /opt/foreign/cbm hook-augment --event SessionStart --dialect "
        "copilot\",\"timeoutSec\":5}],"
        "\"subagentStart\":[{\"type\":\"command\","
        "\"bash\":\"/opt/foreign/cbm hook-augment --event SubagentStart --dialect copilot\","
        "\"powershell\":\"& /opt/foreign/cbm hook-augment --event SubagentStart --dialect "
        "copilot\",\"timeoutSec\":5}]}}\n";
    write_test_file(manifest_path, foreign);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_copilot = save_test_env("COPILOT_HOME");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("COPILOT_HOME");
    char *argv[] = {"uninstall", "--yes"};
    int rc = cbm_cmd_uninstall(2, argv);
    char *after = read_test_file_alloc(manifest_path);
    bool preserved = after && strcmp(after, foreign) == 0;

    free(after);
    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("COPILOT_HOME", saved_copilot);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !preserved)
        FAIL("Copilot uninstall must not claim a canonical-shaped manifest for another binary");
    PASS();
}

TEST(cli_vscode_only_installs_copilot_durable_context) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-vscode-durable-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char code_user[640];
#ifdef __APPLE__
    snprintf(code_user, sizeof(code_user), "%s/Library/Application Support/Code/User", tmpdir);
#elif defined(_WIN32)
    snprintf(code_user, sizeof(code_user), "%s/AppData/Roaming/Code/User", tmpdir);
#else
    snprintf(code_user, sizeof(code_user), "%s/.config/Code/User", tmpdir);
#endif
    test_mkdirp(code_user);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_copilot = save_test_env("COPILOT_HOME");
    char *saved_xdg = save_test_env("XDG_CONFIG_HOME");
    char *saved_appdata = save_test_env("APPDATA");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("COPILOT_HOME");
#if !defined(__APPLE__) && !defined(_WIN32)
    char xdg[512];
    snprintf(xdg, sizeof(xdg), "%s/.config", tmpdir);
    cbm_setenv("XDG_CONFIG_HOME", xdg, 1);
#elif defined(_WIN32)
    char appdata[512];
    snprintf(appdata, sizeof(appdata), "%s/AppData/Roaming", tmpdir);
    cbm_setenv("APPDATA", appdata, 1);
#endif

    char binary[640];
#ifdef _WIN32
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp.exe", tmpdir);
#else
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp", tmpdir);
#endif
    cbm_install_agent_configs(tmpdir, binary, false, false);
    int second_install_rc = cbm_install_agent_configs(tmpdir, binary, false, false);

    char hook_path[640];
    char skill_path[640];
    char agent_path[640];
    char copilot_mcp_path[640];
    char copilot_instructions_path[640];
    snprintf(hook_path, sizeof(hook_path), "%s/.copilot/hooks/codebase-memory-mcp.json", tmpdir);
    snprintf(skill_path, sizeof(skill_path), "%s/.copilot/skills/codebase-memory/SKILL.md", tmpdir);
    snprintf(agent_path, sizeof(agent_path), "%s/.copilot/agents/codebase-memory.agent.md", tmpdir);
    snprintf(copilot_mcp_path, sizeof(copilot_mcp_path), "%s/.copilot/mcp-config.json", tmpdir);
    snprintf(copilot_instructions_path, sizeof(copilot_instructions_path),
             "%s/.copilot/copilot-instructions.md", tmpdir);
    char *hook = read_test_file_alloc(hook_path);
    char *skill = read_test_file_alloc(skill_path);
    char *agent = read_test_file_alloc(agent_path);
    struct stat absent_mcp;
    bool hook_installed = hook && strstr(hook, "sessionStart") && strstr(hook, "subagentStart") &&
                          strstr(hook, "--dialect copilot");
    bool skill_installed = skill && strstr(skill, "search_graph");
    bool agent_installed = agent && strstr(agent, "search_graph") && strstr(agent, "tools:");
    bool mcp_absent = stat(copilot_mcp_path, &absent_mcp) != 0;
    bool instructions_absent = stat(copilot_instructions_path, &absent_mcp) != 0;
    bool installed = second_install_rc == 0 && hook_installed && skill_installed &&
                     agent_installed && mcp_absent && instructions_absent;
    free(hook);
    free(skill);
    free(agent);

    const char *modified = "user-modified-vscode-agent\n";
    write_test_file(agent_path, modified);
    char *argv[] = {"uninstall", "--yes"};
    int rc = cbm_cmd_uninstall(2, argv);
    struct stat removed_hook;
    struct stat removed_skill;
    char *preserved = read_test_file_alloc(agent_path);
    bool hook_removed = stat(hook_path, &removed_hook) != 0;
    bool skill_removed = stat(skill_path, &removed_skill) != 0;
    bool modified_agent_preserved = preserved && strcmp(preserved, modified) == 0;
    bool ownership_safe = hook_removed && skill_removed && modified_agent_preserved;
    free(preserved);

    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("COPILOT_HOME", saved_copilot);
    restore_test_env("XDG_CONFIG_HOME", saved_xdg);
    restore_test_env("APPDATA", saved_appdata);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !installed || !ownership_safe) {
        fprintf(stderr,
                "VS Code durable diag install_rc=%d hook=%d skill=%d agent=%d mcp_absent=%d "
                "instructions_absent=%d uninstall_rc=%d hook_removed=%d skill_removed=%d "
                "agent_preserved=%d\n",
                second_install_rc, hook_installed, skill_installed, agent_installed, mcp_absent,
                instructions_absent, rc, hook_removed, skill_removed, modified_agent_preserved);
        FAIL("VS Code-only installs must receive current user skill, read-only agent, and "
             "SessionStart/SubagentStart context without a Copilot CLI MCP config");
    }
    PASS();
}

TEST(cli_lifecycle_hooks_preserve_foreign_substring_commands) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-ownership-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char qwen_dir[512];
    char factory_dir[512];
    char qwen_settings[640];
    char factory_hooks[640];
    char binary_path[640];
    snprintf(qwen_dir, sizeof(qwen_dir), "%s/.qwen", tmpdir);
    snprintf(factory_dir, sizeof(factory_dir), "%s/.factory", tmpdir);
    snprintf(qwen_settings, sizeof(qwen_settings), "%s/settings.json", qwen_dir);
    snprintf(factory_hooks, sizeof(factory_hooks), "%s/hooks.json", factory_dir);
#ifdef _WIN32
    snprintf(binary_path, sizeof(binary_path), "%s/.local/bin/codebase-memory-mcp.exe", tmpdir);
#else
    snprintf(binary_path, sizeof(binary_path), "%s/.local/bin/codebase-memory-mcp", tmpdir);
#endif
    test_mkdirp(qwen_dir);
    test_mkdirp(factory_dir);
    const char *qwen_foreign =
        "{\"hooks\":{"
        "\"SessionStart\":[{\"matcher\":\"startup|resume|clear|compact\",\"hooks\":[{"
        "\"type\":\"command\",\"command\":\"/opt/user-codebase-memory-mcp-wrapper "
        "--keep-session\"}]}],"
        "\"SubagentStart\":[{\"matcher\":\"*\",\"hooks\":[{\"type\":\"command\","
        "\"command\":\"/opt/user-codebase-memory-mcp-wrapper --keep-subagent\"}]}]}}\n";
    const char *factory_foreign =
        "{\"hooks\":{\"SessionStart\":[{\"hooks\":[{\"type\":\"command\","
        "\"command\":\"/opt/user-codebase-memory-mcp-wrapper --keep-factory\"}]}]}}\n";
    write_test_file(qwen_settings, qwen_foreign);
    write_test_file(factory_hooks, factory_foreign);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    int install_rc = cbm_install_agent_configs(tmpdir, binary_path, false, false);
    char *qwen_after_install = read_test_file_alloc(qwen_settings);
    char *factory_after_install = read_test_file_alloc(factory_hooks);
    bool qwen_install_preserved =
        qwen_after_install && strstr(qwen_after_install, "--keep-session") &&
        strstr(qwen_after_install, "--keep-subagent") && strstr(qwen_after_install, "hook-augment");
#ifdef _WIN32
    bool factory_install_preserved = factory_after_install &&
                                     strcmp(factory_after_install, factory_foreign) == 0 &&
                                     !strstr(factory_after_install, "hook-augment");
#else
    bool factory_install_preserved = factory_after_install &&
                                     strstr(factory_after_install, "--keep-factory") &&
                                     strstr(factory_after_install, "hook-augment");
#endif
    bool install_preserved = qwen_install_preserved && factory_install_preserved;
    free(qwen_after_install);
    free(factory_after_install);

    char *argv[] = {"uninstall", "--yes"};
    int uninstall_rc = cbm_cmd_uninstall(2, argv);
    char *qwen_after_uninstall = read_test_file_alloc(qwen_settings);
    char *factory_after_uninstall = read_test_file_alloc(factory_hooks);
    bool qwen_uninstall_preserved = qwen_after_uninstall &&
                                    strstr(qwen_after_uninstall, "--keep-session") &&
                                    strstr(qwen_after_uninstall, "--keep-subagent") &&
                                    !strstr(qwen_after_uninstall, "hook-augment");
#ifdef _WIN32
    bool factory_uninstall_preserved =
        factory_after_uninstall && strcmp(factory_after_uninstall, factory_foreign) == 0;
#else
    bool factory_uninstall_preserved = factory_after_uninstall &&
                                       strstr(factory_after_uninstall, "--keep-factory") &&
                                       !strstr(factory_after_uninstall, "hook-augment");
#endif
    bool uninstall_preserved = qwen_uninstall_preserved && factory_uninstall_preserved;
    free(qwen_after_uninstall);
    free(factory_after_uninstall);

    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    test_rmdir_r(tmpdir);
    if (install_rc != 0 || uninstall_rc != 0 || !install_preserved || !uninstall_preserved)
        FAIL("Qwen and Factory hooks must preserve foreign commands that merely contain the "
             "product name");
    PASS();
}

TEST(cli_read_only_agents_do_not_receive_mutating_mcp_server) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-readonly-agent-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char qoder_dir[512];
    char junie_dir[512];
    char kiro_dir[512];
    snprintf(qoder_dir, sizeof(qoder_dir), "%s/.qoder", tmpdir);
    snprintf(junie_dir, sizeof(junie_dir), "%s/.junie", tmpdir);
    snprintf(kiro_dir, sizeof(kiro_dir), "%s/.kiro", tmpdir);
    test_mkdirp(qoder_dir);
    test_mkdirp(junie_dir);
    test_mkdirp(kiro_dir);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_kiro = save_test_env("KIRO_HOME");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("KIRO_HOME");
    int rc = cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);

    char qoder_agent[640];
    char junie_agent[640];
    char kiro_agent[640];
    snprintf(qoder_agent, sizeof(qoder_agent), "%s/agents/codebase-memory.md", qoder_dir);
    snprintf(junie_agent, sizeof(junie_agent), "%s/agents/codebase-memory.md", junie_dir);
    snprintf(kiro_agent, sizeof(kiro_agent), "%s/agents/codebase-memory.json", kiro_dir);
    char *qoder = read_test_file_alloc(qoder_agent);
    char *junie = read_test_file_alloc(junie_agent);
    char *kiro = read_test_file_alloc(kiro_agent);
    bool qoder_confined = qoder && strstr(qoder, "mcpServers:") &&
                          strstr(qoder, "- codebase-memory-mcp") &&
                          strstr(qoder, "mcp__codebase-memory-mcp__search_graph") &&
                          strstr(qoder, "check_index_coverage") && !strstr(qoder, "Bash") &&
                          !strstr(qoder, "Write") && !strstr(qoder, "Edit");
    bool junie_confined = junie && strstr(junie, "mcpServers: [\"codebase-memory-analysis\"]") &&
                          strstr(junie, "hard-enforces the analysis tool profile") &&
                          strstr(junie, "tools: [\"Read\", \"Grep\", \"Glob\"]") &&
                          strstr(junie, "check_index_coverage") && !strstr(junie, "Bash") &&
                          !strstr(junie, "Write") && !strstr(junie, "Edit");
    bool kiro_confined =
        kiro && strstr(kiro, "\"mcpServers\"") && strstr(kiro, "\"includeMcpJson\": false") &&
        strstr(kiro, "@codebase-memory-mcp/search_graph") && strstr(kiro, "--tool-profile") &&
        strstr(kiro, "analysis") && strstr(kiro, "check_index_coverage") &&
        !strstr(kiro, "\"@codebase-memory-mcp\"") && !strstr(kiro, "delete_project") &&
        !strstr(kiro, "manage_adr") && !strstr(kiro, "index_repository") &&
        !strstr(kiro, "ingest_traces");
    bool confined = qoder_confined && junie_confined && kiro_confined;
    free(qoder);
    free(junie);
    free(kiro);

    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("KIRO_HOME", saved_kiro);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !confined)
        FAIL("Kiro, Junie, and Qoder graph agents must remain least privilege");
    PASS();
}

TEST(cli_junie_foreign_analysis_alias_falls_back_to_parent_handoff) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-junie-alias-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char junie_dir[512];
    char mcp_dir[640];
    char config_path[768];
    char agent_path[768];
    snprintf(junie_dir, sizeof(junie_dir), "%s/.junie", tmpdir);
    snprintf(mcp_dir, sizeof(mcp_dir), "%s/mcp", junie_dir);
    snprintf(config_path, sizeof(config_path), "%s/mcp.json", mcp_dir);
    snprintf(agent_path, sizeof(agent_path), "%s/agents/codebase-memory.md", junie_dir);
    test_mkdirp(mcp_dir);
    const char *foreign =
        "{\"mcpServers\":{\"codebase-memory-analysis\":{\"command\":\"/opt/user-tool\","
        "\"args\":[\"--private\"]}},\"theme\":\"dark\"}\n";
    write_test_file(config_path, foreign);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    int rc = cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);
    char *config = read_test_file_alloc(config_path);
    char *agent = read_test_file_alloc(agent_path);
    bool safe = rc != 0 && config && strcmp(config, foreign) == 0 && agent &&
                strstr(agent, "parent agent must supply") && strstr(agent, "coverage evidence") &&
                !strstr(agent, "mcpServers") && !strstr(agent, "codebase-memory-analysis");
    free(config);
    free(agent);

    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    test_rmdir_r(tmpdir);
    if (!safe)
        FAIL("foreign Junie analysis aliases must be preserved and force parent handoff");
    PASS();
}

TEST(cli_mcp_installers_preserve_foreign_same_name_entries) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-foreign-mcp-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char json_path[512];
    char toml_path[512];
    snprintf(json_path, sizeof(json_path), "%s/settings.json", tmpdir);
    snprintf(toml_path, sizeof(toml_path), "%s/config.toml", tmpdir);
    const char *foreign_json =
        "{\"mcpServers\":{\"codebase-memory-mcp\":{\"command\":"
        "\"/opt/custom/codebase-memory-mcp\",\"args\":[]}},\"theme\":\"dark\"}\n";
    const char *foreign_toml = "[mcp_servers.codebase-memory-mcp]\n"
                               "command = \"/opt/user-tool\"\n"
                               "args = [\"--private\"]\n"
                               "env = { KEEP = \"yes\" }\n";

    write_test_file(json_path, foreign_json);
    int json_install_rc = cbm_install_editor_mcp("/opt/codebase-memory-mcp", json_path);
    char *json_after_install = read_test_file_alloc(json_path);
    bool json_install_preserved =
        json_after_install && strcmp(json_after_install, foreign_json) == 0;
    free(json_after_install);
    write_test_file(json_path, foreign_json);
    int json_remove_rc = cbm_remove_editor_mcp(json_path);
    char *json_after_remove = read_test_file_alloc(json_path);
    bool json_remove_preserved = json_after_remove && strcmp(json_after_remove, foreign_json) == 0;
    free(json_after_remove);

    write_test_file(toml_path, foreign_toml);
    int toml_install_rc = cbm_upsert_codex_mcp("/opt/codebase-memory-mcp", toml_path);
    char *toml_after_install = read_test_file_alloc(toml_path);
    bool toml_install_preserved =
        toml_after_install && strcmp(toml_after_install, foreign_toml) == 0;
    free(toml_after_install);
    write_test_file(toml_path, foreign_toml);
    int toml_remove_rc = cbm_remove_codex_mcp(toml_path);
    char *toml_after_remove = read_test_file_alloc(toml_path);
    bool toml_remove_preserved = toml_after_remove && strcmp(toml_after_remove, foreign_toml) == 0;
    free(toml_after_remove);

    test_rmdir_r(tmpdir);
    if (json_install_rc == 0 || json_remove_rc != 0 || toml_install_rc == 0 ||
        toml_remove_rc != 0 || !json_install_preserved || !json_remove_preserved ||
        !toml_install_preserved || !toml_remove_preserved)
        FAIL("generic JSON and Codex MCP installers must fail closed on foreign same-name "
             "entries and never remove them");
    PASS();
}

TEST(cli_installer_rejects_symlinked_agent_roots) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX symlink parent-chain contract");
#else
    char tmpdir[256];
    char qoder_target[256];
    char junie_target[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-linked-roots-XXXXXX");
    snprintf(qoder_target, sizeof(qoder_target), "/tmp/cli-linked-qoder-XXXXXX");
    snprintf(junie_target, sizeof(junie_target), "/tmp/cli-linked-junie-XXXXXX");
    if (!cbm_mkdtemp(tmpdir) || !cbm_mkdtemp(qoder_target) || !cbm_mkdtemp(junie_target))
        FAIL("cbm_mkdtemp failed");
    char qoder_link[512];
    char junie_link[512];
    snprintf(qoder_link, sizeof(qoder_link), "%s/.qoder", tmpdir);
    snprintf(junie_link, sizeof(junie_link), "%s/.junie", tmpdir);
    if (symlink(qoder_target, qoder_link) != 0 || symlink(junie_target, junie_link) != 0)
        FAIL("symlink failed");

    char qoder_executable[512];
    snprintf(qoder_executable, sizeof(qoder_executable), "%s/qodercli", tmpdir);
    write_test_file(qoder_executable, "#!/bin/sh\nexit 0\n");
    if (chmod(qoder_executable, 0700) != 0)
        FAIL("chmod qodercli failed");

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    (void)cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);

    char outside_qoder_settings[512];
    char outside_qoder_skill[512];
    char outside_junie_mcp[512];
    char outside_junie_agent[512];
    snprintf(outside_qoder_settings, sizeof(outside_qoder_settings), "%s/settings.json",
             qoder_target);
    snprintf(outside_qoder_skill, sizeof(outside_qoder_skill), "%s/skills/codebase-memory/SKILL.md",
             qoder_target);
    snprintf(outside_junie_mcp, sizeof(outside_junie_mcp), "%s/mcp/mcp.json", junie_target);
    snprintf(outside_junie_agent, sizeof(outside_junie_agent), "%s/agents/codebase-memory.md",
             junie_target);
    struct stat state;
    bool refused = stat(outside_qoder_settings, &state) != 0 &&
                   stat(outside_qoder_skill, &state) != 0 && stat(outside_junie_mcp, &state) != 0 &&
                   stat(outside_junie_agent, &state) != 0;

    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    cbm_unlink(qoder_link);
    cbm_unlink(junie_link);
    test_rmdir_r(tmpdir);
    test_rmdir_r(qoder_target);
    test_rmdir_r(junie_target);
    if (!refused)
        FAIL("installer must not follow symlinked agent roots outside the selected home");
    PASS();
#endif
}

TEST(cli_claude_hook_scripts_shell_quote_binary_path) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX shell quoting contract");
#endif
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-quote-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char config_dir[512];
    snprintf(config_dir, sizeof(config_dir), "%s/.claude", tmpdir);
    test_mkdirp(config_dir);
    char copilot_dir[512];
    snprintf(copilot_dir, sizeof(copilot_dir), "%s/.copilot", tmpdir);
    test_mkdirp(copilot_dir);
    char copilot_marker[640];
    snprintf(copilot_marker, sizeof(copilot_marker), "%s/mcp-config.json", copilot_dir);
    write_test_file(copilot_marker, "{}\n");

    char *saved_path = save_test_env("PATH");
    char *saved_claude = save_test_env("CLAUDE_CONFIG_DIR");
    char *saved_copilot = save_test_env("COPILOT_HOME");
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_unsetenv("COPILOT_HOME");
    const char *binary = "/opt/$(touch cbm-hook-pwned)/it's codebase-memory-mcp";
    cbm_install_agent_configs(tmpdir, binary, false, false);

    const char *const names[] = {
        "cbm-code-discovery-gate",
        "cbm-session-reminder",
        "cbm-subagent-reminder",
    };
    bool safely_quoted = true;
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        char path[640];
        snprintf(path, sizeof(path), "%s/hooks/%s", config_dir, names[i]);
        char *script = read_test_file_alloc(path);
        safely_quoted = safely_quoted && script && strstr(script, "BIN='") &&
                        strstr(script, "'\\''") && !strstr(script, "BIN=\"");
        free(script);
    }

    char manifest_path[640];
    snprintf(manifest_path, sizeof(manifest_path), "%s/hooks/codebase-memory-mcp.json",
             copilot_dir);
    char *manifest = read_test_file_alloc(manifest_path);
    yyjson_doc *manifest_doc = manifest ? yyjson_read(manifest, strlen(manifest), 0) : NULL;
    if (manifest_doc) {
        yyjson_val *hooks = yyjson_obj_get(yyjson_doc_get_root(manifest_doc), "hooks");
        yyjson_val *session = hooks ? yyjson_obj_get(hooks, "sessionStart") : NULL;
        yyjson_val *entry = session && yyjson_is_arr(session) ? yyjson_arr_get(session, 0U) : NULL;
        const char *bash = entry ? yyjson_get_str(yyjson_obj_get(entry, "bash")) : NULL;
        const char *powershell = entry ? yyjson_get_str(yyjson_obj_get(entry, "powershell")) : NULL;
        safely_quoted = safely_quoted && bash && powershell && strstr(bash, "'\\''") &&
                        strstr(bash, "--dialect copilot") && strstr(powershell, "& '") &&
                        strstr(powershell, "it''s") && strstr(powershell, "--dialect copilot");
        yyjson_doc_free(manifest_doc);
    } else {
        safely_quoted = false;
    }
    free(manifest);

    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_claude);
    restore_test_env("COPILOT_HOME", saved_copilot);
    test_rmdir_r(tmpdir);
    if (!safely_quoted)
        FAIL("hook scripts must shell-quote paths without command substitution");
    PASS();
}

TEST(cli_claude_hook_commands_shell_quote_custom_config_dir) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX shell quoting contract");
#endif
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-config-quote-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[640];
    snprintf(config_dir, sizeof(config_dir), "%s/custom claude;$(touch cbm-hook-path-pwned)",
             tmpdir);
    test_mkdirp(config_dir);
    char *saved_path = save_test_env("PATH");
    char *saved_claude = save_test_env("CLAUDE_CONFIG_DIR");
    cbm_setenv("PATH", tmpdir, 1);
    cbm_setenv("CLAUDE_CONFIG_DIR", config_dir, 1);

    cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);
    char settings_path[768];
    snprintf(settings_path, sizeof(settings_path), "%s/settings.json", config_dir);
    char *settings = read_test_file_alloc(settings_path);
    char quoted_prefix[704];
    snprintf(quoted_prefix, sizeof(quoted_prefix), "'%s/hooks/", config_dir);
    bool quoted = settings && strstr(settings, quoted_prefix) &&
                  strstr(settings, "cbm-code-discovery-gate'") &&
                  strstr(settings, "cbm-session-reminder'") &&
                  strstr(settings, "cbm-subagent-reminder'");
    free(settings);

    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_claude);
    test_rmdir_r(tmpdir);
    if (!quoted)
        FAIL("Claude settings must shell-quote the complete custom hook script path");
    PASS();
}

TEST(cli_codex_migrates_to_single_hook_representation) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-hook-migrate-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char codex_dir[512];
    snprintf(codex_dir, sizeof(codex_dir), "%s/.codex", tmpdir);
    test_mkdirp(codex_dir);

    char *saved_path = save_test_env("PATH");
    char *saved_codex = save_test_env("CODEX_HOME");
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CODEX_HOME");
    cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);

    char hooks_path[640];
    char config_path[640];
    snprintf(hooks_path, sizeof(hooks_path), "%s/hooks.json", codex_dir);
    snprintf(config_path, sizeof(config_path), "%s/config.toml", codex_dir);
    write_test_file(hooks_path, "{}\n");
    cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);

    char *toml = read_test_file_alloc(config_path);
    char *hooks = read_test_file_alloc(hooks_path);
    bool migrated = toml && !strstr(toml, "codebase-memory-mcp SessionStart") && hooks &&
                    strstr(hooks, "SessionStart") && strstr(hooks, "SubagentStart");
    free(toml);
    free(hooks);
    restore_test_env("PATH", saved_path);
    restore_test_env("CODEX_HOME", saved_codex);
    test_rmdir_r(tmpdir);
    if (!migrated)
        FAIL("Codex install must leave exactly one lifecycle hook representation");
    PASS();
}

/* The PreToolUse augmenter parses search_graph's format:"json" payload to
 * build additionalContext. This test feeds it the REAL envelope from a live
 * in-memory server, so any drift between the response shape and the parser
 * fails HERE — not only in the Windows CI guard (which caught exactly that:
 * the json-tree reshape left the parser reading a key that no longer exists,
 * and the hook silently emitted nothing). */
TEST(cli_hook_augment_context_tracks_search_json_shape) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "hookproj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/hookproj");
    cbm_node_t n = {.project = proj,
                    .label = "Function",
                    .name = "someIndexedSymbol",
                    .qualified_name = "hookproj.mod.someIndexedSymbol",
                    .file_path = "mod.py",
                    .start_line = 1,
                    .end_line = 4};
    ASSERT_GT(cbm_store_upsert_node(st, &n), 0);

    /* The exact request ha_build_args produces: format:"json". */
    char *envelope =
        cbm_mcp_handle_tool(srv, "search_graph",
                            "{\"project\":\"hookproj\",\"name_pattern\":\".*someIndexedSymbol.*\","
                            "\"limit\":5,\"format\":\"json\"}");
    ASSERT_NOT_NULL(envelope);

    bool is_error = true;
    char *ctx =
        cbm_hook_augment_format_context_for_testing(envelope, "someIndexedSymbol", &is_error);
    ASSERT_FALSE(is_error);
    ASSERT_NOT_NULL(ctx); /* one hit MUST produce context — empty = broken hook */
    ASSERT_NOT_NULL(strstr(ctx, "someIndexedSymbol"));
    ASSERT_NOT_NULL(strstr(ctx, "mod.py"));
    free(ctx);
    free(envelope);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(cli_hook_augment_lifecycle_output_contract) {
    static const struct {
        const char *event;
        const char *scope;
    } cases[] = {
        {"SessionStart", "Session context"},
        {"SubagentStart", "Subagent context"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char input[512];
        snprintf(input, sizeof(input),
                 "{\"hook_event_name\":\"%s\","
                 "\"cwd\":\"/definitely-not-indexed/cbm-secret-path\"}",
                 cases[i].event);
        char *output = cbm_hook_augment_lifecycle_json(input);
        ASSERT_NOT_NULL(output);
        yyjson_doc *doc = yyjson_read(output, strlen(output), 0);
        ASSERT_NOT_NULL(doc);
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *specific = yyjson_obj_get(root, "hookSpecificOutput");
        ASSERT(specific && yyjson_is_obj(specific));
        ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(specific, "hookEventName")), cases[i].event);
        const char *context = yyjson_get_str(yyjson_obj_get(specific, "additionalContext"));
        ASSERT_NOT_NULL(context);
        ASSERT(strstr(context, cases[i].scope) != NULL);
        ASSERT(strstr(context, "search_graph") != NULL);
        ASSERT(strstr(context, "trace_path") != NULL);
        ASSERT(strstr(context, "check_index_coverage") != NULL);
        ASSERT(strstr(context, "grep") != NULL);
        ASSERT(strstr(context, "cbm-secret-path") == NULL);
        if (strcmp(cases[i].event, "SessionStart") == 0)
            ASSERT(strstr(context, "Active tier: Tier 2") != NULL);
        yyjson_doc_free(doc);
        free(output);
    }
    ASSERT_NULL(
        cbm_hook_augment_lifecycle_json("{\"hook_event_name\":\"PostToolUse\",\"cwd\":\"/tmp\"}"));
    ASSERT_NULL(cbm_hook_augment_lifecycle_json("not-json"));

    char *copilot = cbm_hook_augment_lifecycle_json_for(
        "{\"cwd\":\"/definitely-not-indexed/cbm-secret-path\"}", "SubagentStart", true);
    ASSERT_NOT_NULL(copilot);
    yyjson_doc *copilot_doc = yyjson_read(copilot, strlen(copilot), 0);
    ASSERT_NOT_NULL(copilot_doc);
    yyjson_val *copilot_root = yyjson_doc_get_root(copilot_doc);
    const char *copilot_context = yyjson_get_str(yyjson_obj_get(copilot_root, "additionalContext"));
    ASSERT_NOT_NULL(copilot_context);
    ASSERT(strstr(copilot_context, "Subagent context") != NULL);
    ASSERT(strstr(copilot_context, "search_graph") != NULL);
    ASSERT(strstr(copilot_context, "cbm-secret-path") == NULL);
    ASSERT_NULL(yyjson_obj_get(copilot_root, "hookSpecificOutput"));
    yyjson_doc_free(copilot_doc);
    free(copilot);
    ASSERT_NULL(cbm_hook_augment_lifecycle_json_for("{}", "PostToolUse", true));
    PASS();
}

TEST(cli_hook_augment_subagent_tier_router_contract) {
    static const struct {
        const char *agent_type;
        const char *tier;
        const char *mode;
    } cases[] = {
        {"scout", "Tier 1", "quick"},
        {"verify", "Tier 2", "verification"},
        {"auditor", "Tier 3", "full graph"},
    };
    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char input[512];
        snprintf(input, sizeof(input),
                 "{\"hook_event_name\":\"SubagentStart\",\"agent_type\":\"%s\","
                 "\"cwd\":\"/definitely-not-indexed/tier-router\"}",
                 cases[i].agent_type);
        char *output = cbm_hook_augment_lifecycle_json(input);
        ASSERT_NOT_NULL(output);
        yyjson_doc *doc = yyjson_read(output, strlen(output), 0);
        ASSERT_NOT_NULL(doc);
        yyjson_val *specific = yyjson_obj_get(yyjson_doc_get_root(doc), "hookSpecificOutput");
        const char *context =
            specific ? yyjson_get_str(yyjson_obj_get(specific, "additionalContext")) : NULL;
        ASSERT_NOT_NULL(context);
        char active[64];
        snprintf(active, sizeof(active), "Active tier: %s", cases[i].tier);
        ASSERT(strstr(context, active) != NULL);
        ASSERT(strstr(context, cases[i].mode) != NULL);
        ASSERT(strstr(context, "check_index_coverage") != NULL);
        ASSERT(strstr(context, "missed") != NULL);
        yyjson_doc_free(doc);
        free(output);
    }
    PASS();
}

TEST(cli_hook_augment_subagent_no_project_guidance_is_read_only) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-guidance-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cli_env_snapshot_t cache = {0};
    cli_env_snapshot_t tool_mode = {0};
    cli_env_snapshot_t auto_index = {0};
    cli_env_snapshot_t auto_index_limit = {0};
    ASSERT_TRUE(cli_env_snapshot(&cache, "CBM_CACHE_DIR"));
    ASSERT_TRUE(cli_env_snapshot(&tool_mode, "CBM_TOOL_MODE"));
    ASSERT_TRUE(cli_env_snapshot(&auto_index, "CBM_AUTO_INDEX"));
    ASSERT_TRUE(cli_env_snapshot(&auto_index_limit, "CBM_AUTO_INDEX_LIMIT"));
    cbm_setenv("CBM_CACHE_DIR", tmpdir, 1);
    cbm_unsetenv("CBM_TOOL_MODE");
    cbm_unsetenv("CBM_AUTO_INDEX");
    cbm_unsetenv("CBM_AUTO_INDEX_LIMIT");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_TOOL_MODE, CBM_CONFIG_TOOL_MODE_STREAMLINED), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX, "true"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX_LIMIT, "17"), 0);
    cbm_config_close(cfg);

    const char *session = cbm_hook_no_project_index_guidance_for_testing("SessionStart");
    const char *subagent = cbm_hook_no_project_index_guidance_for_testing("SubagentStart");
    ASSERT_NOT_NULL(session);
    ASSERT_NOT_NULL(subagent);
    ASSERT(strstr(session, "search_graph") != NULL);
    ASSERT(strstr(session, "auto_index_limit=17") != NULL);
    ASSERT(strstr(subagent, "Ask the parent") != NULL);
    ASSERT(strstr(subagent, "auto_index_limit=17") != NULL);
    ASSERT(strstr(subagent, "Do not mutate the graph") != NULL);

    cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX, "false"), 0);
    cbm_config_close(cfg);
    session = cbm_hook_no_project_index_guidance_for_testing("SessionStart");
    subagent = cbm_hook_no_project_index_guidance_for_testing("SubagentStart");
    ASSERT(strstr(session, "auto_index=false") != NULL);
    ASSERT(strstr(session, "index_repository") != NULL);
    ASSERT(strstr(subagent, "Ask the parent") != NULL);
    ASSERT(strstr(subagent, "auto_index=false") != NULL);
    ASSERT(strstr(subagent, "Do not mutate the graph") != NULL);

    cli_env_restore(&auto_index_limit);
    cli_env_restore(&auto_index);
    cli_env_restore(&tool_mode);
    cli_env_restore(&cache);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_hook_augment_guidance_tracks_tool_and_dependency_config) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-config-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cli_env_snapshot_t cache = {0};
    cli_env_snapshot_t tool_mode = {0};
    cli_env_snapshot_t auto_index = {0};
    cli_env_snapshot_t auto_index_limit = {0};
    cli_env_snapshot_t auto_index_deps = {0};
    cli_env_snapshot_t auto_dep_limit = {0};
    ASSERT_TRUE(cli_env_snapshot(&cache, "CBM_CACHE_DIR"));
    ASSERT_TRUE(cli_env_snapshot(&tool_mode, "CBM_TOOL_MODE"));
    ASSERT_TRUE(cli_env_snapshot(&auto_index, "CBM_AUTO_INDEX"));
    ASSERT_TRUE(cli_env_snapshot(&auto_index_limit, "CBM_AUTO_INDEX_LIMIT"));
    ASSERT_TRUE(cli_env_snapshot(&auto_index_deps, "CBM_AUTO_INDEX_DEPS"));
    ASSERT_TRUE(cli_env_snapshot(&auto_dep_limit, "CBM_AUTO_DEP_LIMIT"));
    cbm_setenv("CBM_CACHE_DIR", tmpdir, 1);
    cbm_unsetenv("CBM_TOOL_MODE");
    cbm_unsetenv("CBM_AUTO_INDEX");
    cbm_unsetenv("CBM_AUTO_INDEX_LIMIT");
    cbm_unsetenv("CBM_AUTO_INDEX_DEPS");
    cbm_unsetenv("CBM_AUTO_DEP_LIMIT");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_TOOL_MODE, CBM_CONFIG_TOOL_MODE_STREAMLINED), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX, "true"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX_LIMIT, "17"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX_DEPS, "false"), 0);
    cbm_config_close(cfg);

    const char *input =
        "{\"hook_event_name\":\"SessionStart\","
        "\"cwd\":\"/definitely-not-indexed/config-guidance\"}";
    char *output = cbm_hook_augment_lifecycle_json(input);
    ASSERT_NOT_NULL(output);
    ASSERT(strstr(output, "API=streamlined") != NULL);
    ASSERT(strstr(output, "_hidden_tools") != NULL);
    ASSERT(strstr(output, "get_code") != NULL);
    ASSERT(strstr(output, "auto_index_limit=17") != NULL);
    ASSERT(strstr(output, "auto_index_deps=false") != NULL);
    free(output);

    cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_TOOL_MODE, CBM_CONFIG_TOOL_MODE_CLASSIC), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX, "false"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX_DEPS, "true"), 0);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_AUTO_DEP_LIMIT, "3"), 0);
    cbm_config_close(cfg);

    output = cbm_hook_augment_lifecycle_json(input);
    ASSERT_NOT_NULL(output);
    ASSERT(strstr(output, "API=classic") != NULL);
    ASSERT(strstr(output, "get_code_snippet") != NULL);
    ASSERT(strstr(output, "directly visible") != NULL);
    ASSERT(strstr(output, "_hidden_tools") == NULL);
    ASSERT(strstr(output, "auto_index=false") != NULL);
    ASSERT(strstr(output, "auto_dep_limit=3") != NULL);
    free(output);

    /* Environment-only configuration must still shape guidance when no config
     * database exists, and the hook must not create one while reading it. */
    char missing_cache[512];
    snprintf(missing_cache, sizeof(missing_cache), "%s/missing", tmpdir);
    cbm_setenv("CBM_CACHE_DIR", missing_cache, 1);
    cbm_setenv("CBM_TOOL_MODE", CBM_CONFIG_TOOL_MODE_STREAMLINED, 1);
    cbm_setenv("CBM_AUTO_INDEX", "true", 1);
    cbm_setenv("CBM_AUTO_INDEX_LIMIT", "9", 1);
    output = cbm_hook_augment_lifecycle_json(input);
    ASSERT_NOT_NULL(output);
    ASSERT(strstr(output, "API=streamlined") != NULL);
    ASSERT(strstr(output, "auto_index_limit=9") != NULL);
    struct stat missing_cache_stat;
    ASSERT_EQ(stat(missing_cache, &missing_cache_stat), -1);
    free(output);

    cli_env_restore(&auto_dep_limit);
    cli_env_restore(&auto_index_deps);
    cli_env_restore(&auto_index_limit);
    cli_env_restore(&auto_index);
    cli_env_restore(&tool_mode);
    cli_env_restore(&cache);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_hook_augment_post_read_event_and_path_contract) {
    static const struct {
        const char *dialect;
        const char *input;
        const char *event;
        const char *path;
    } cases[] = {
        {NULL,
         "{\"hook_event_name\":\"PostToolUse\",\"tool_name\":\"Read\","
         "\"tool_input\":{\"file_path\":\"src/a.c\"},\"cwd\":\"/repo\"}",
         "PostToolUse", "/repo/src/a.c"},
        {"gemini",
         "{\"hook_event_name\":\"AfterTool\",\"tool_name\":\"read_file\","
         "\"tool_input\":{\"file_path\":\"src/b.c\"},\"cwd\":\"/repo\"}",
         "AfterTool", "/repo/src/b.c"},
        {"qwen",
         "{\"hook_event_name\":\"PostToolUse\",\"tool_name\":\"ReadFile\","
         "\"tool_input\":{\"path\":\"src\\\\c.c\"},\"cwd\":\"C:/repo\"}",
         "PostToolUse", "C:/repo/src/c.c"},
        {"qoder",
         "{\"hook_event_name\":\"PostToolUse\",\"tool_name\":\"Read\","
         "\"tool_input\":{\"path\":\"src/d.c\"},\"cwd\":\"/repo\"}",
         "PostToolUse", "/repo/src/d.c"},
        {"factory",
         "{\"hook_event_name\":\"PostToolUse\",\"tool_name\":\"Read\","
         "\"tool_input\":{\"file_path\":\"src/e.c\"},\"cwd\":\"/repo\"}",
         "PostToolUse", "/repo/src/e.c"},
        {"augment",
         "{\"hook_event_name\":\"PostToolUse\",\"tool_name\":\"view\","
         "\"tool_input\":{\"path\":\"src/f.c\"},\"cwd\":\"/repo\"}",
         "PostToolUse", "/repo/src/f.c"},
    };
    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char path[4096];
        char *output = cbm_hook_augment_tool_json_for_testing(
            cases[i].input, cases[i].dialect, "coverage-context", path, sizeof(path));
        ASSERT_NOT_NULL(output);
        ASSERT_STR_EQ(path, cases[i].path);
        yyjson_doc *doc = yyjson_read(output, strlen(output), 0);
        ASSERT_NOT_NULL(doc);
        yyjson_val *specific = yyjson_obj_get(yyjson_doc_get_root(doc), "hookSpecificOutput");
        ASSERT(specific && yyjson_is_obj(specific));
        ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(specific, "hookEventName")), cases[i].event);
        ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(specific, "additionalContext")),
                      "coverage-context");
        yyjson_doc_free(doc);
        free(output);
    }
    char path[64];
    ASSERT_NULL(cbm_hook_augment_tool_json_for_testing(
        "{\"hook_event_name\":\"PreToolUse\",\"tool_name\":\"Read\","
        "\"tool_input\":{\"file_path\":\"a.c\"},\"cwd\":\"/repo\"}",
        NULL, "context", path, sizeof(path)));
    ASSERT_NULL(cbm_hook_augment_tool_json_for_testing(
        "{\"hook_event_name\":\"PostToolUse\",\"tool_name\":\"Read\","
        "\"tool_input\":{\"file_path\":\"a.c\"},\"cwd\":\"relative\"}",
        NULL, "context", path, sizeof(path)));
    PASS();
}

TEST(cli_hook_augment_hermes_dialect_contract) {
    const char *input =
        "{\"hook_event_name\":\"pre_llm_call\",\"cwd\":\"/unindexed/hermes-project\","
        "\"session_id\":\"session-1\",\"user_message\":\"inspect code\"}";
    char *output = cbm_hook_augment_lifecycle_json_for_dialect(input, "pre_llm_call", "hermes");
    ASSERT_NOT_NULL(output);
    yyjson_doc *doc = yyjson_read(output, strlen(output), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT(root && yyjson_is_obj(root));
    yyjson_val *context = yyjson_obj_get(root, "context");
    ASSERT(context && yyjson_is_str(context));
    ASSERT(strstr(yyjson_get_str(context), "search_graph") != NULL);
    ASSERT_EQ(yyjson_obj_size(root), 1U);
    ASSERT_NULL(yyjson_obj_get(root, "additionalContext"));
    ASSERT_NULL(yyjson_obj_get(root, "hookSpecificOutput"));
    ASSERT_NULL(yyjson_obj_get(root, "decision"));
    ASSERT_NULL(yyjson_obj_get(root, "permissionDecision"));
    yyjson_doc_free(doc);
    free(output);

    ASSERT_NULL(cbm_hook_augment_lifecycle_json_for_dialect("not-json", "pre_llm_call", "hermes"));
    ASSERT_NULL(cbm_hook_augment_lifecycle_json_for_dialect(
        "{\"hook_event_name\":\"post_llm_call\",\"cwd\":\"/tmp\"}", "post_llm_call", "hermes"));
    ASSERT_NULL(cbm_hook_augment_lifecycle_json_for_dialect(input, "pre_llm_call", "unknown"));
    PASS();
}

TEST(cli_hook_augment_qoder_lifecycle_contract) {
    const char *input =
        "{\"hook_event_name\":\"SessionStart\",\"cwd\":\"/unindexed/qoder-project\","
        "\"session_id\":\"session-2\",\"source\":\"compact\"}";
    char *output = cbm_hook_augment_lifecycle_json_for_dialect(input, "SessionStart", "qoder");
    ASSERT_NOT_NULL(output);
    yyjson_doc *doc = yyjson_read(output, strlen(output), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *specific = root ? yyjson_obj_get(root, "hookSpecificOutput") : NULL;
    ASSERT(specific && yyjson_is_obj(specific));
    yyjson_val *event = yyjson_obj_get(specific, "hookEventName");
    yyjson_val *context = yyjson_obj_get(specific, "additionalContext");
    ASSERT(event && yyjson_is_str(event));
    ASSERT_STR_EQ(yyjson_get_str(event), "SessionStart");
    ASSERT(context && yyjson_is_str(context));
    ASSERT(strstr(yyjson_get_str(context), "search_graph") != NULL);
    ASSERT(strstr(yyjson_get_str(context), "Tier 2") != NULL);
    ASSERT(strstr(yyjson_get_str(context), "check_index_coverage") != NULL);
    ASSERT_NULL(yyjson_obj_get(specific, "permissionDecision"));
    ASSERT_NULL(yyjson_obj_get(specific, "permissionDecisionReason"));
    ASSERT_NULL(yyjson_obj_get(specific, "updatedInput"));
    ASSERT_NULL(yyjson_obj_get(root, "decision"));
    ASSERT_NULL(yyjson_obj_get(root, "context"));
    yyjson_doc_free(doc);
    free(output);

    char *subagent = cbm_hook_augment_lifecycle_json_for_dialect(
        "{\"hook_event_name\":\"SubagentStart\",\"agent_type\":\"auditor\","
        "\"cwd\":\"/tmp\"}",
        "SubagentStart", "qoder");
    ASSERT_NOT_NULL(subagent);
    ASSERT(strstr(subagent, "Tier 3") != NULL);
    free(subagent);
    ASSERT_NULL(cbm_hook_augment_lifecycle_json_for_dialect(
        "{\"hook_event_name\":\"UserPromptSubmit\",\"cwd\":\"/tmp\"}", "UserPromptSubmit",
        "qoder"));
    PASS();
}

#ifndef _WIN32
TEST(cli_qoder_migrates_user_prompt_hook_to_lifecycle_and_read) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-qoder-hook-migrate-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char settings[512];
    snprintf(settings, sizeof(settings), "%s/settings.json", tmpdir);
    const char *binary = "/opt/codebase-memory-mcp";
    char command[1024];
    char shell[32];
    ASSERT_EQ(cbm_build_qoder_hook_command_for_testing(binary, false, command, sizeof(command),
                                                       shell, sizeof(shell)),
              0);
    char legacy[4096];
    int written = snprintf(legacy, sizeof(legacy),
                           "{\"hooks\":{\"UserPromptSubmit\":[{\"hooks\":[{\"type\":\"command\","
                           "\"command\":\"%s\"}]}]}}",
                           command);
    ASSERT(written > 0 && (size_t)written < sizeof(legacy));
    write_test_file(settings, legacy);

    ASSERT_EQ(cbm_upsert_qoder_context_hooks_for_testing(settings, binary), 0);
    char *upgraded = read_test_file_alloc(settings);
    ASSERT_NOT_NULL(upgraded);
    ASSERT(strstr(upgraded, "UserPromptSubmit") == NULL);
    ASSERT(strstr(upgraded, "SessionStart") != NULL);
    ASSERT(strstr(upgraded, "SubagentStart") != NULL);
    ASSERT(strstr(upgraded, "PostToolUse") != NULL);
    ASSERT(strstr(upgraded, "\"matcher\": \"Read\"") != NULL);
    free(upgraded);

    ASSERT_EQ(cbm_remove_qoder_context_hooks_for_testing(settings, binary), 0);
    char *removed = read_test_file_alloc(settings);
    ASSERT_NOT_NULL(removed);
    ASSERT(strstr(removed, "--dialect qoder") == NULL);
    free(removed);
    test_rmdir_r(tmpdir);
    PASS();
}
#endif

TEST(cli_hook_augment_kimi_user_prompt_contract) {
    const char *input =
        "{\"hook_event_name\":\"UserPromptSubmit\",\"cwd\":\"/unindexed/kimi-project\","
        "\"session_id\":\"session-3\",\"prompt\":\"inspect code\"}";
    char *output = cbm_hook_augment_lifecycle_json_for_dialect(input, "UserPromptSubmit", "kimi");
    ASSERT_NOT_NULL(output);
    ASSERT(strstr(output, "[codebase-memory] Prompt context") != NULL);
    ASSERT(strstr(output, "index_repository") != NULL);
    ASSERT(strstr(output, "search_graph") != NULL);
    ASSERT(strchr(output, '{') == NULL);
    ASSERT(strstr(output, "hookSpecificOutput") == NULL);
    free(output);

    ASSERT_NULL(cbm_hook_augment_lifecycle_json_for_dialect(
        "{\"hook_event_name\":\"SessionStart\",\"cwd\":\"/tmp\"}", "SessionStart", "kimi"));
    PASS();
}

TEST(cli_hook_augment_devin_lifecycle_contract) {
    static const struct {
        const char *event;
        const char *payload;
        const char *scope;
    } cases[] = {
        {"SessionStart", "\"source\":\"startup\"", "Session context"},
        {"UserPromptSubmit", "\"prompt\":\"inspect code\"", "Prompt context"},
        {"PostCompaction", "\"summary\":null", "Compaction context"},
    };
    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char input[512];
        snprintf(input, sizeof(input),
                 "{\"hook_event_name\":\"%s\",\"cwd\":\"/unindexed/devin\",%s}", cases[i].event,
                 cases[i].payload);
        char *output = cbm_hook_augment_lifecycle_json_for_dialect(input, cases[i].event, "devin");
        ASSERT_NOT_NULL(output);
        yyjson_doc *doc = yyjson_read(output, strlen(output), 0);
        ASSERT_NOT_NULL(doc);
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *specific = root ? yyjson_obj_get(root, "hookSpecificOutput") : NULL;
        const char *event =
            specific ? yyjson_get_str(yyjson_obj_get(specific, "hookEventName")) : NULL;
        const char *context =
            specific ? yyjson_get_str(yyjson_obj_get(specific, "additionalContext")) : NULL;
        ASSERT_NOT_NULL(event);
        ASSERT_STR_EQ(event, cases[i].event);
        ASSERT_NOT_NULL(context);
        ASSERT(strstr(context, cases[i].scope) != NULL);
        ASSERT(strstr(context, "search_graph") != NULL);
        ASSERT_NULL(yyjson_obj_get(root, "decision"));
        yyjson_doc_free(doc);
        free(output);
    }
    ASSERT_NULL(cbm_hook_augment_lifecycle_json_for_dialect(
        "{\"hook_event_name\":\"SubagentStart\"}", "SubagentStart", "devin"));
    PASS();
}

TEST(cli_hook_augment_cline_lifecycle_contract) {
    static const char *const events[] = {"TaskStart", "TaskResume", "UserPromptSubmit",
                                         "PreCompact"};
    for (size_t i = 0U; i < sizeof(events) / sizeof(events[0]); i++) {
        char input[512];
        snprintf(input, sizeof(input),
                 "{\"hookName\":\"%s\",\"workspaceRoots\":[\"/unindexed/cline\"]}", events[i]);
        char *output = cbm_hook_augment_lifecycle_json_for_dialect(input, events[i], "cline");
        ASSERT_NOT_NULL(output);
        yyjson_doc *doc = yyjson_read(output, strlen(output), 0);
        ASSERT_NOT_NULL(doc);
        yyjson_val *root = yyjson_doc_get_root(doc);
        yyjson_val *cancel = root ? yyjson_obj_get(root, "cancel") : NULL;
        yyjson_val *context = root ? yyjson_obj_get(root, "contextModification") : NULL;
        yyjson_val *error = root ? yyjson_obj_get(root, "errorMessage") : NULL;
        ASSERT(cancel && yyjson_is_bool(cancel) && !yyjson_get_bool(cancel));
        ASSERT(context && yyjson_is_str(context));
        ASSERT(strstr(yyjson_get_str(context), "search_graph") != NULL);
        ASSERT(error && yyjson_is_str(error) && strcmp(yyjson_get_str(error), "") == 0);
        ASSERT_NULL(yyjson_obj_get(root, "decision"));
        yyjson_doc_free(doc);
        free(output);
    }
    ASSERT_NULL(cbm_hook_augment_lifecycle_json_for_dialect("{\"hookName\":\"SubagentStart\"}",
                                                            "SubagentStart", "cline"));
    PASS();
}

/* A malformed user-owned hook config must never be treated as an absent file:
 * doing so replaces the user's bytes with a fresh hooks object. */
TEST(cli_hook_upsert_rejects_malformed_settings) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-malformed-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settings_path[512];
    snprintf(settings_path, sizeof(settings_path), "%s/settings.json", tmpdir);
    const char *original = "{ this is not valid JSON\n";
    write_test_file(settings_path, original);

    int rc = cbm_upsert_claude_hooks(settings_path);
    char *after = read_test_file_alloc(settings_path);
    bool unchanged = after && strcmp(after, original) == 0;
    free(after);
    test_rmdir_r(tmpdir);

    if (rc != -1 || !unchanged)
        FAIL("malformed hook config must fail closed without changing user bytes");
    PASS();
}

typedef struct {
    const char *content;
    int result;
} cli_hook_prewrite_change_t;

static void cli_hook_replace_before_editor(const char *settings_path, void *context) {
    cli_hook_prewrite_change_t *change = context;
    change->result = write_test_file(settings_path, change->content);
}

TEST(cli_hook_upsert_rejects_concurrent_same_event_update) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-race-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char settings[512];
    snprintf(settings, sizeof(settings), "%s/settings.json", tmpdir);
    write_test_file(settings, "{\"hooks\":{\"BeforeTool\":[{\"matcher\":\"user\",\"hooks\":[{"
                              "\"type\":\"command\",\"command\":\"echo existing\"}]}]}}\n");
    const char *concurrent =
        "{\"hooks\":{\"BeforeTool\":[{\"matcher\":\"user\",\"hooks\":[{\"type\":"
        "\"command\",\"command\":\"echo existing\"}]},{\"matcher\":\"concurrent\","
        "\"hooks\":[{\"type\":\"command\",\"command\":\"echo concurrent\"}]}]}}\n";
    cli_hook_prewrite_change_t change = {.content = concurrent, .result = -1};
    cbm_set_hook_json_prewrite_hook_for_testing(cli_hook_replace_before_editor, &change);
    int result = cbm_upsert_gemini_hooks(settings);
    cbm_set_hook_json_prewrite_hook_for_testing(NULL, NULL);

    char *after = read_test_file_alloc(settings);
    bool preserved = after && strcmp(after, concurrent) == 0;
    free(after);
    test_rmdir_r(tmpdir);
    if (change.result != 0 || result != -1 || !preserved)
        FAIL("hook mutation must reject a concurrent same-event update without losing it");
    PASS();
}

static const char test_released_session_hook_script[] =
    "#!/usr/bin/env bash\n"
    "# SessionStart hook: remind agent to use codebase-memory-mcp tools.\n"
    "# Installed by codebase-memory-mcp. Fires on startup/resume/clear/compact.\n"
    "cat << 'REMINDER'\n"
    "CRITICAL - Code Discovery Protocol:\n"
    "1. ALWAYS use codebase-memory-mcp tools FIRST for ANY code exploration:\n"
    "   - search_graph(name_pattern/label/qn_pattern) to find functions/classes/routes\n"
    "   - trace_path(function_name, mode=calls|data_flow|cross_service) for call chains\n"
    "   - get_code_snippet(qualified_name) for exact symbol source (precise ranges)\n"
    "   - query_graph(query) for complex Cypher patterns\n"
    "   - get_architecture(aspects) for project structure\n"
    "   - search_code(pattern) for text search (graph-augmented grep)\n"
    "2. Use Grep/Glob/Read freely for text, configs, non-code files, and\n"
    "   always Read a file before editing it.\n"
    "3. If a project is not indexed yet, run index_repository FIRST.\n"
    "REMINDER\n";

/* Exact installer output from the streamlined auto-index guidance release.
 * Upgrades must recognize their own prior bytes while preserving near-matches. */
static const char test_intermediate_session_hook_script[] =
    "#!/usr/bin/env bash\n"
    "# SessionStart hook: remind agent to use codebase-memory-mcp tools.\n"
    "# Installed by codebase-memory-mcp. Fires on startup/resume/clear/compact.\n"
    "cat << 'REMINDER'\n"
    "Code Discovery Protocol:\n"
    "1. Prefer codebase-memory-mcp tools first for structural code exploration:\n"
    "   - search_graph(name_pattern/label/qn_pattern) to find functions/classes/routes\n"
    "   - trace_path(function_name, mode=calls|data_flow|cross_service) for call chains\n"
    "   - get_code(qualified_name) for exact symbol source in streamlined mode\n"
    "   - query_graph(query) for complex Cypher patterns\n"
    "   - search_code(pattern) for text/regex source search in an indexed project\n"
    "2. Use Grep/Glob/Read freely for text, configs, non-code files, and\n"
    "   always Read a file before editing it.\n"
    "3. Graph-backed tools auto-index the server CWD or explicit repo paths when\n"
    "   auto_index=true and under auto_index_limit. search_code needs an\n"
    "   indexed project. Use _hidden_tools\n"
    "   to reveal index_repository or get_architecture when explicit control is needed.\n"
    "REMINDER\n";

static const char test_released_subagent_hook_script[] =
    "#!/usr/bin/env bash\n"
    "# SubagentStart hook: tell subagents to use codebase-memory-mcp tools.\n"
    "# Installed by codebase-memory-mcp. Fires when any subagent is spawned.\n"
    "# SubagentStart injects context via JSON additionalContext, not plain stdout.\n"
    "cat << 'REMINDER'\n"
    "{\"hookSpecificOutput\":{\"hookEventName\":\"SubagentStart\","
    "\"additionalContext\":\"Code discovery: prefer codebase-memory-mcp tools "
    "(search_graph, trace_path, get_code_snippet, query_graph, get_architecture, "
    "search_code) over grep/file-read for navigating code. Use Grep/Glob/Read for "
    "text, configs, and non-code files.\"}}\n"
    "REMINDER\n";

static bool test_build_released_gate_hook_script(const char *binary_path, char *script,
                                                 size_t script_size) {
    int written = snprintf(script, script_size,
                           "#!/usr/bin/env bash\n"
                           "# codebase-memory-mcp search augmenter (Claude Code PreToolUse).\n"
                           "# NOTE: the legacy filename is kept for zero-migration upgrades.\n"
                           "# Despite the name this NEVER blocks a tool call - it only adds\n"
                           "# graph context. Any failure is silent (exit 0, no output).\n"
                           "BIN=\"%s\"\n"
                           "[ -x \"$BIN\" ] || exit 0\n"
                           "\"$BIN\" hook-augment 2>/dev/null\n"
                           "exit 0\n",
                           binary_path);
    return written > 0 && (size_t)written < script_size;
}

#ifndef _WIN32
TEST(cli_upgrade_migrates_released_claude_hook_scripts) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-upgrade-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char hooks_dir[512];
    char gate_path[640];
    char session_path[640];
    char subagent_path[640];
    char settings_path[640];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.claude/hooks", tmpdir);
    snprintf(gate_path, sizeof(gate_path), "%s/cbm-code-discovery-gate", hooks_dir);
    snprintf(session_path, sizeof(session_path), "%s/cbm-session-reminder", hooks_dir);
    snprintf(subagent_path, sizeof(subagent_path), "%s/cbm-subagent-reminder", hooks_dir);
    snprintf(settings_path, sizeof(settings_path), "%s/.claude/settings.json", tmpdir);
    test_mkdirp(hooks_dir);

    char legacy_gate[8192];
    ASSERT_TRUE(test_build_released_gate_hook_script("/opt/codebase-memory-mcp", legacy_gate,
                                                     sizeof(legacy_gate)));
    const char *legacy_session = test_released_session_hook_script;
    const char *legacy_subagent = test_released_subagent_hook_script;
    write_test_file(gate_path, legacy_gate);
    write_test_file(session_path, legacy_session);
    write_test_file(subagent_path, legacy_subagent);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_claude = save_test_env("CLAUDE_CONFIG_DIR");
    char *saved_codex = save_test_env("CODEX_HOME");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_unsetenv("CODEX_HOME");
    int rc = cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);

    char *gate = read_test_file_alloc(gate_path);
    char *session = read_test_file_alloc(session_path);
    char *subagent = read_test_file_alloc(subagent_path);
    char *settings = read_test_file_alloc(settings_path);
    bool migrated = rc == 0 && gate && strcmp(gate, legacy_gate) != 0 && session &&
                    strcmp(session, legacy_session) != 0 && subagent &&
                    strcmp(subagent, legacy_subagent) != 0 && settings &&
                    strstr(settings, "cbm-code-discovery-gate") &&
                    strstr(settings, "cbm-session-reminder") &&
                    strstr(settings, "cbm-subagent-reminder");

    /* A later installer emitted a second byte-exact session reminder before
     * hooks delegated to the binary. It must migrate on an idempotent reinstall. */
    ASSERT_EQ(write_test_file(session_path, test_intermediate_session_hook_script), 0);
    int intermediate_rc =
        cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);
    char *intermediate_session = read_test_file_alloc(session_path);
    bool intermediate_migrated =
        intermediate_rc == 0 && intermediate_session &&
        strcmp(intermediate_session, test_intermediate_session_hook_script) != 0;
    free(intermediate_session);
    free(gate);
    free(session);
    free(subagent);
    free(settings);
    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_claude);
    restore_test_env("CODEX_HOME", saved_codex);
    test_rmdir_r(tmpdir);
    if (!migrated || !intermediate_migrated)
        FAIL("every released Claude hook script must migrate byte-exactly and stay registered");
    PASS();
}

TEST(cli_upgrade_preserves_near_legacy_claude_hook_script) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-near-legacy-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char hooks_dir[512];
    char gate_path[640];
    char settings_path[640];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.claude/hooks", tmpdir);
    snprintf(gate_path, sizeof(gate_path), "%s/cbm-code-discovery-gate", hooks_dir);
    snprintf(settings_path, sizeof(settings_path), "%s/.claude/settings.json", tmpdir);
    test_mkdirp(hooks_dir);
    const char *modified_legacy =
        "#!/usr/bin/env bash\n"
        "# codebase-memory-mcp search augmenter (Claude Code PreToolUse).\n"
        "# NOTE: the legacy filename is kept for zero-migration upgrades.\n"
        "# Despite the name this NEVER blocks a tool call - it only adds\n"
        "# graph context. Any failure is silent (exit 0, no output).\n"
        "BIN=\"/opt/codebase-memory-mcp\"\n"
        "[ -x \"$BIN\" ] || exit 0\n"
        "\"$BIN\" hook-augment 2>/dev/null\n"
        "exit 0\n"
        "# user change\n";
    write_test_file(gate_path, modified_legacy);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_claude = save_test_env("CLAUDE_CONFIG_DIR");
    char *saved_codex = save_test_env("CODEX_HOME");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_unsetenv("CODEX_HOME");
    (void)cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);

    char *gate = read_test_file_alloc(gate_path);
    char *settings = read_test_file_alloc(settings_path);
    bool preserved = gate && strcmp(gate, modified_legacy) == 0 &&
                     (!settings || !strstr(settings, "cbm-code-discovery-gate"));
    free(gate);
    free(settings);
    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_claude);
    restore_test_env("CODEX_HOME", saved_codex);
    test_rmdir_r(tmpdir);
    if (!preserved)
        FAIL("near-legacy Claude hook bytes are foreign and must stay untouched/unregistered");
    PASS();
}

TEST(cli_hook_upsert_rejects_linked_settings) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-links-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char target[512];
    char settings[512];
    snprintf(target, sizeof(target), "%s/user-settings.json", tmpdir);
    snprintf(settings, sizeof(settings), "%s/settings.json", tmpdir);
    const char *original = "{\"userOwned\":true}\n";
    write_test_file(target, original);

    ASSERT_EQ(symlink(target, settings), 0);
    int symlink_rc = cbm_upsert_claude_hooks(settings);
    char *after_symlink = read_test_file_alloc(target);
    bool symlink_safe = symlink_rc == -1 && after_symlink && strcmp(after_symlink, original) == 0;
    free(after_symlink);
    (void)cbm_unlink(settings);

    write_test_file(target, original);
    ASSERT_EQ(link(target, settings), 0);
    int hardlink_rc = cbm_upsert_claude_hooks(settings);
    char *after_hardlink = read_test_file_alloc(target);
    bool hardlink_safe =
        hardlink_rc == -1 && after_hardlink && strcmp(after_hardlink, original) == 0;
    free(after_hardlink);

    test_rmdir_r(tmpdir);
    if (!symlink_safe || !hardlink_safe)
        FAIL("hook config edits must reject symlinks and hard links without changing targets");
    PASS();
}

TEST(cli_claude_hook_script_collisions_are_not_registered) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX linked-hook ownership contract");
#endif
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-script-collision-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char hooks_dir[512];
    char victim[640];
    char gate[640];
    char session[640];
    char settings[640];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.claude/hooks", tmpdir);
    snprintf(victim, sizeof(victim), "%s/victim", tmpdir);
    snprintf(gate, sizeof(gate), "%s/cbm-code-discovery-gate", hooks_dir);
    snprintf(session, sizeof(session), "%s/cbm-session-reminder", hooks_dir);
    snprintf(settings, sizeof(settings), "%s/.claude/settings.json", tmpdir);
    test_mkdirp(hooks_dir);
    write_test_file(victim, "victim-owned\n");
    ASSERT_EQ(symlink(victim, gate), 0);
    write_test_file(session, "#!/bin/sh\necho user-owned\n");

    char *saved_path = save_test_env("PATH");
    char *saved_claude = save_test_env("CLAUDE_CONFIG_DIR");
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_install_agent_configs(tmpdir, "/usr/local/bin/codebase-memory-mcp", false, false);

    char *settings_data = read_test_file_alloc(settings);
    char *victim_data = read_test_file_alloc(victim);
    char *session_data = read_test_file_alloc(session);
    bool safe = victim_data && strcmp(victim_data, "victim-owned\n") == 0 && session_data &&
                strcmp(session_data, "#!/bin/sh\necho user-owned\n") == 0 &&
                (!settings_data || (!strstr(settings_data, "cbm-code-discovery-gate") &&
                                    !strstr(settings_data, "cbm-session-reminder")));

    free(settings_data);
    free(victim_data);
    free(session_data);
    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_claude);
    test_rmdir_r(tmpdir);
    if (!safe)
        FAIL("foreign or linked Claude hook scripts must be preserved and never registered");
    PASS();
}

TEST(cli_codex_legacy_migration_rejects_linked_config) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-link-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char target[512];
    char config[512];
    snprintf(target, sizeof(target), "%s/user-config.toml", tmpdir);
    snprintf(config, sizeof(config), "%s/config.toml", tmpdir);
    const char *original = "user_key = true\n\n[mcp_servers.codebase-memory-mcp]\n"
                           "command = \"old\"\nargs = []\n";
    write_test_file(target, original);

    ASSERT_EQ(symlink(target, config), 0);
    int rc = cbm_upsert_codex_mcp("/usr/local/bin/codebase-memory-mcp", config);
    char *after = read_test_file_alloc(target);
    bool safe = rc == -1 && after && strcmp(after, original) == 0;
    free(after);

    test_rmdir_r(tmpdir);
    if (!safe)
        FAIL("Codex legacy migration must reject linked config without modifying its target");
    PASS();
}
#endif

/* Full uninstall owns the three Claude shims it creates and must remove them
 * along with their settings.json registrations. */
TEST(cli_uninstall_removes_claude_hook_scripts) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-uninstall-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    snprintf(config_dir, sizeof(config_dir), "%s/.claude", tmpdir);
    test_mkdirp(config_dir);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_claude = save_test_env("CLAUDE_CONFIG_DIR");
    char *saved_codex = save_test_env("CODEX_HOME");
    char *saved_opencode = save_test_env("OPENCODE_CONFIG");
    char *saved_copilot = save_test_env("COPILOT_HOME");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_unsetenv("CODEX_HOME");
    cbm_unsetenv("OPENCODE_CONFIG");
    cbm_unsetenv("COPILOT_HOME");

    char binary[640];
#ifdef _WIN32
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp.exe", tmpdir);
#else
    snprintf(binary, sizeof(binary), "%s/.local/bin/codebase-memory-mcp", tmpdir);
#endif
    cbm_install_agent_configs(tmpdir, binary, false, false);

    char *args[] = {"-n"};
    int rc = cbm_cmd_uninstall(1, args);
#ifdef _WIN32
    const char *const names[] = {
        "cbm-code-discovery-gate.cmd",
        "cbm-session-reminder.cmd",
        "cbm-subagent-reminder.cmd",
    };
#else
    const char *const names[] = {
        "cbm-code-discovery-gate",
        "cbm-session-reminder",
        "cbm-subagent-reminder",
    };
#endif
    bool removed = true;
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        char path[640];
        struct stat state;
        snprintf(path, sizeof(path), "%s/hooks/%s", config_dir, names[i]);
#ifdef _WIN32
        removed = removed && stat(path, &state) != 0;
#else
        removed = removed && lstat(path, &state) != 0;
#endif
    }

    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_claude);
    restore_test_env("CODEX_HOME", saved_codex);
    restore_test_env("OPENCODE_CONFIG", saved_opencode);
    restore_test_env("COPILOT_HOME", saved_copilot);
    test_rmdir_r(tmpdir);

    if (rc != 0 || !removed)
        FAIL("uninstall must remove every Claude hook shim owned by the installer");
    PASS();
}

TEST(cli_uninstall_preserves_modified_claude_hook_script) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-preserve-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char config_dir[512];
    snprintf(config_dir, sizeof(config_dir), "%s/.claude", tmpdir);
    test_mkdirp(config_dir);

    char *saved_home = save_test_env("HOME");
    char *saved_path = save_test_env("PATH");
    char *saved_claude = save_test_env("CLAUDE_CONFIG_DIR");
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_install_agent_configs(tmpdir, "/opt/codebase-memory-mcp", false, false);

    char modified_path[640];
    snprintf(modified_path, sizeof(modified_path), "%s/hooks/cbm-session-reminder", config_dir);
    const char *sentinel = "#!/bin/sh\necho user-modified-session-hook\n";
    write_test_file(modified_path, sentinel);
    char *args[] = {"-n"};
    (void)cbm_cmd_uninstall(1, args);
    char *after = read_test_file_alloc(modified_path);
    bool preserved = after && strcmp(after, sentinel) == 0;
    free(after);

    restore_test_env("HOME", saved_home);
    restore_test_env("PATH", saved_path);
    restore_test_env("CLAUDE_CONFIG_DIR", saved_claude);
    test_rmdir_r(tmpdir);
    if (!preserved)
        FAIL("uninstall must preserve a Claude hook script modified after installation");
    PASS();
}

TEST(cli_detect_agents_finds_gemini) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
    char settings[640];
    snprintf(dir, sizeof(dir), "%s/.gemini", tmpdir);
    test_mkdirp(dir);
    snprintf(settings, sizeof(settings), "%s/settings.json", dir);
    write_test_file(settings, "{}\n");

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.gemini);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_detect_agents_finds_claude_desktop) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
#ifdef __APPLE__
    snprintf(dir, sizeof(dir), "%s/Library/Application Support/Claude", tmpdir);
#elif defined(_WIN32)
    snprintf(dir, sizeof(dir), "%s/AppData/Roaming/Claude", tmpdir);
#else
    snprintf(dir, sizeof(dir), "%s/.config/Claude", tmpdir);
#endif
    test_mkdirp(dir);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.claude_desktop);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_detect_agents_finds_zed) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
#ifdef __APPLE__
    snprintf(dir, sizeof(dir), "%s/Library/Application Support/Zed", tmpdir);
#elif defined(_WIN32)
    snprintf(dir, sizeof(dir), "%s/AppData/Roaming/Zed", tmpdir);
#else
    snprintf(dir, sizeof(dir), "%s/.config/zed", tmpdir);
#endif
    test_mkdirp(dir);

    char *saved_xdg = save_test_env("XDG_CONFIG_HOME");
    char xdg_dir[640];
    snprintf(xdg_dir, sizeof(xdg_dir), "%s/.config", tmpdir);
    cbm_setenv("XDG_CONFIG_HOME", xdg_dir, 1); /* Linux resolver prefers XDG */
    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    restore_test_env("XDG_CONFIG_HOME", saved_xdg);
    ASSERT_TRUE(agents.zed);

    test_rmdir_r(tmpdir);
    PASS();
}

#if !defined(__APPLE__) && !defined(_WIN32)
TEST(cli_detect_agents_finds_zed_via_xdg_config_home) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-zed-xdg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char xdg[512];
    char zed_dir[640];
    snprintf(xdg, sizeof(xdg), "%s/custom-config", tmpdir);
    snprintf(zed_dir, sizeof(zed_dir), "%s/zed", xdg);
    test_mkdirp(zed_dir);
    char *saved = save_test_env("XDG_CONFIG_HOME");
    cbm_setenv("XDG_CONFIG_HOME", xdg, 1);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    restore_test_env("XDG_CONFIG_HOME", saved);
    test_rmdir_r(tmpdir);
    if (!agents.zed)
        FAIL("Zed detection on Linux must honor XDG_CONFIG_HOME");
    PASS();
}
#endif

#ifdef _WIN32
TEST(cli_detect_agents_finds_zed_in_roaming_appdata) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "%s\\cli-zed-win", cbm_tmpdir());
    test_rmdir_r(tmpdir);
    test_mkdirp(tmpdir);
    char zed_dir[512];
    snprintf(zed_dir, sizeof(zed_dir), "%s/AppData/Roaming/Zed", tmpdir);
    test_mkdirp(zed_dir);
    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    test_rmdir_r(tmpdir);
    if (!agents.zed)
        FAIL("Zed detection on Windows must use Roaming AppData, not Local AppData");
    PASS();
}
#endif

TEST(cli_detect_agents_finds_antigravity) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
    /* Antigravity CLI installs under ~/.gemini/antigravity-cli/ (2026). */
    snprintf(dir, sizeof(dir), "%s/.gemini/antigravity-cli", tmpdir);
    test_mkdirp(dir);

    char *saved_path = save_test_env("PATH");
    cbm_setenv("PATH", tmpdir, 1);
    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    restore_test_env("PATH", saved_path);
    test_rmdir_r(tmpdir);
    if (!agents.antigravity || agents.gemini)
        FAIL("Antigravity detection must not imply Gemini CLI");
    PASS();
}

TEST(cli_detect_agents_finds_kilocode) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
#ifdef __APPLE__
    snprintf(dir, sizeof(dir),
             "%s/Library/Application Support/Code/User/globalStorage/kilocode.kilo-code", tmpdir);
#elif defined(_WIN32)
    snprintf(dir, sizeof(dir), "%s/AppData/Roaming/Code/User/globalStorage/kilocode.kilo-code",
             tmpdir);
#else
    snprintf(dir, sizeof(dir), "%s/.config/Code/User/globalStorage/kilocode.kilo-code", tmpdir);
#endif
    test_mkdirp(dir);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.kilocode);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_detect_agents_finds_modern_kilo) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-kilo-modern-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.config/kilo", tmpdir);
    test_mkdirp(dir);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    char *json = cbm_build_install_plan_json(tmpdir, "/usr/local/bin/codebase-memory-mcp");
    bool modern_config = json && strstr(json, "/.config/kilo/kilo.jsonc") != NULL;
    bool legacy_config =
        json && strstr(json, "kilocode.kilo-code/settings/mcp_settings.json") != NULL;

    free(json);
    test_rmdir_r(tmpdir);
    if (!agents.kilocode)
        FAIL("modern Kilo installation at ~/.config/kilo must be detected");
    if (!modern_config || legacy_config)
        FAIL("modern Kilo install plan must target kilo.jsonc, not legacy VS Code globalStorage");
    PASS();
}

TEST(cli_detect_agents_finds_kiro) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.kiro", tmpdir);
    test_mkdirp(dir);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.kiro);

    test_rmdir_r(tmpdir);
    PASS();
}

/* issue #651: Junie (~/.junie/) must be detected so install registers the
 * MCP server in ~/.junie/mcp/mcp.json. */
TEST(cli_detect_agents_finds_junie_issue651) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/.junie", tmpdir);
    test_mkdirp(dir);
    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_TRUE(agents.junie);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_detect_agents_none_found) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-detect-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Empty home and isolated PATH must not inherit the host's agents. */
    char *saved_ccd = save_test_env("CLAUDE_CONFIG_DIR");
    char *saved_codex = save_test_env("CODEX_HOME");
    char *saved_path = save_test_env("PATH");
    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    cbm_unsetenv("CODEX_HOME");
    cbm_setenv("PATH", tmpdir, 1);

    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    ASSERT_FALSE(agents.claude_code);
    ASSERT_FALSE(agents.claude_desktop);
    ASSERT_FALSE(agents.codex);
    ASSERT_FALSE(agents.gemini);
    ASSERT_FALSE(agents.zed);
    ASSERT_FALSE(agents.antigravity);
    ASSERT_FALSE(agents.kilocode);
    ASSERT_FALSE(agents.kiro);

    restore_test_env("CLAUDE_CONFIG_DIR", saved_ccd);
    restore_test_env("CODEX_HOME", saved_codex);
    restore_test_env("PATH", saved_path);

    cbm_rmdir(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group B: MCP Config Upsert — Codex TOML
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_upsert_codex_mcp_fresh) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/config.toml", tmpdir);

    int rc = cbm_upsert_codex_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "[mcp_servers.codebase-memory-mcp]") != NULL);
    ASSERT(strstr(data, "/usr/local/bin/codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_codex_mcp_escapes_windows_path) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-winpath-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/config.toml", tmpdir);
    const char *binary = "C:\\Users\\Martin Vogel\\bin\\codebase-memory-mcp.exe";

    int rc = cbm_upsert_codex_mcp(binary, configpath);
    char *data = read_test_file_alloc(configpath);
    bool escaped_basic = data && strstr(data, "command = \"C:\\\\Users") != NULL;
    bool literal = data && strstr(data, "command = 'C:\\Users") != NULL;
    bool has_args = data && strstr(data, "args = []") != NULL;

    free(data);
    test_rmdir_r(tmpdir);
    if (rc != 0 || (!escaped_basic && !literal))
        FAIL("Codex MCP TOML must escape Windows backslashes or use a literal string");
    if (!has_args)
        FAIL("Codex MCP TOML must include the documented empty args array");
    PASS();
}

TEST(cli_upsert_codex_mcp_existing) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/config.toml", tmpdir);
    write_test_file(configpath, "model = \"gpt-4\"\n\n[other_setting]\nfoo = \"bar\"\n");

    int rc = cbm_upsert_codex_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    /* Existing settings preserved */
    ASSERT(strstr(data, "model = \"gpt-4\"") != NULL);
    ASSERT(strstr(data, "[other_setting]") != NULL);
    /* Our entry added */
    ASSERT(strstr(data, "[mcp_servers.codebase-memory-mcp]") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_codex_mcp_replace) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/config.toml", tmpdir);
    write_test_file(configpath, "[mcp_servers.codebase-memory-mcp]\n"
                                "command = \"/old/path/codebase-memory-mcp\"\n"
                                "\n"
                                "[other_setting]\nfoo = \"bar\"\n");

    int rc = cbm_upsert_codex_mcp("/new/path/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    /* Old path replaced */
    ASSERT(strstr(data, "/old/path") == NULL);
    ASSERT(strstr(data, "/new/path/codebase-memory-mcp") != NULL);
    /* Other settings preserved */
    ASSERT(strstr(data, "[other_setting]") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_codex_mcp_preserves_owned_descendant_tool_policy) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-descendant-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/config.toml", tmpdir);
    const char *initial = "[mcp_servers.codebase-memory-mcp]\n"
                          "command = \"/old/path/codebase-memory-mcp\"\n"
                          "args = []\n\n"
                          "[mcp_servers.codebase-memory-mcp.tools.query_graph]\n"
                          "approval_mode = \"approve\"\n\n"
                          "[other]\nkeep = true\n";
    ASSERT_EQ(write_test_file(configpath, initial), 0);

    ASSERT_EQ(cbm_upsert_codex_mcp("/new/path/codebase-memory-mcp", configpath), 0);
    ASSERT_EQ(cbm_upsert_codex_mcp("/new/path/codebase-memory-mcp", configpath), 0);
    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT_NOT_NULL(strstr(data, "/new/path/codebase-memory-mcp"));
    ASSERT_NULL(strstr(data, "/old/path/codebase-memory-mcp"));
    ASSERT_NOT_NULL(strstr(data, "[mcp_servers.codebase-memory-mcp.tools.query_graph]\n"
                                 "approval_mode = \"approve\""));
    ASSERT_NOT_NULL(strstr(data, "[other]\nkeep = true"));
    ASSERT_EQ(count_substr(data, "# >>> codebase-memory-mcp MCP >>>"), 1);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_codex_legacy_migration_ignores_header_text_in_multiline_string) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-codex-multiline-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/config.toml", tmpdir);
    const char *original = "[other]\n"
                           "description = \"\"\"\n"
                           "This is documentation, not a table:\n"
                           "[mcp_servers.codebase-memory-mcp]\n"
                           "keep this text intact\n"
                           "\"\"\"\n"
                           "enabled = true\n";
    write_test_file(configpath, original);

    int rc = cbm_upsert_codex_mcp("/new/codebase-memory-mcp", configpath);
    char *after = read_test_file_alloc(configpath);
    bool preserved = after && strstr(after, original) != NULL &&
                     strstr(after, "command = \"/new/codebase-memory-mcp\"") != NULL;
    free(after);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !preserved)
        FAIL("Codex legacy migration must ignore table-looking text inside multiline strings");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group B: MCP Config Upsert — Zed (corrected format)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_zed_mcp_uses_args_format) {
    /* Zed expects no arguments, not one real empty-string argument. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-zed-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/settings.json", tmpdir);

    cbm_install_zed_mcp("/usr/local/bin/codebase-memory-mcp", configpath);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    yyjson_doc *doc = yyjson_read(data, strlen(data), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *servers = yyjson_obj_get(root, "context_servers");
    yyjson_val *entry = yyjson_obj_get(servers, "codebase-memory-mcp");
    yyjson_val *args = yyjson_obj_get(entry, "args");
    ASSERT(args && yyjson_is_arr(args));
    ASSERT_EQ(yyjson_arr_size(args), 0U);
    ASSERT_NULL(yyjson_obj_get(entry, "source"));
    yyjson_doc_free(doc);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_zed_mcp_preserves_jsonc_comments) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-zed-jsonc-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/settings.json", tmpdir);
    write_test_file(configpath,
                    "{\n  // preserve the user's Zed setting\n  \"theme\": \"Ayu Dark\",\n}\n");

    int rc = cbm_install_zed_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    char *data = read_test_file_alloc(configpath);
    bool preserved = data && strstr(data, "preserve the user's Zed setting") &&
                     strstr(data, "Ayu Dark") && strstr(data, "codebase-memory-mcp");
    free(data);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !preserved)
        FAIL("Zed MCP install must preserve JSONC comments and unrelated settings");
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group B: MCP Config Upsert — OpenCode
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_upsert_opencode_mcp_fresh) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-ocode-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/opencode.json", tmpdir);

    int rc = cbm_upsert_opencode_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);
    ASSERT(strstr(data, "/usr/local/bin/codebase-memory-mcp") != NULL);
    /* command must be emitted as an array, not a string */
    ASSERT(strstr(data, "\"command\":[") != NULL || strstr(data, "\"command\": [") != NULL);
    /* type must be explicitly set to \"local\" */
    ASSERT(strstr(data, "\"type\":\"local\"") != NULL ||
           strstr(data, "\"type\": \"local\"") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_opencode_mcp_preserves_jsonc_comments) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-ocode-jsonc-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/opencode.jsonc", tmpdir);
    write_test_file(configpath, "{\n  // keep this user explanation\n  \"theme\": \"dark\",\n}\n");

    int rc = cbm_upsert_opencode_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    char *data = read_test_file_alloc(configpath);
    bool comment_kept = data && strstr(data, "keep this user explanation") != NULL;
    bool setting_kept = data && strstr(data, "theme") && strstr(data, "dark");
    bool installed = data && strstr(data, "codebase-memory-mcp");

    free(data);
    test_rmdir_r(tmpdir);
    if (rc != 0 || !comment_kept || !setting_kept || !installed)
        FAIL("OpenCode MCP upsert must preserve JSONC comments and unrelated settings");
    PASS();
}

TEST(cli_upsert_opencode_mcp_existing) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-ocode-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/opencode.json", tmpdir);
    write_test_file(configpath, "{\"mcp\":{\"other-server\":{\"command\":\"/usr/bin/other\"}}}");

    int rc = cbm_upsert_opencode_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "other-server") != NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group B: MCP Config Upsert — Antigravity
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_upsert_antigravity_mcp_fresh) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-anti-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/mcp_config.json", tmpdir);

    int rc = cbm_upsert_antigravity_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_antigravity_mcp_replace) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-anti-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char configpath[512];
    snprintf(configpath, sizeof(configpath), "%s/mcp_config.json", tmpdir);
    write_test_file(configpath, "{\"mcpServers\":{\"codebase-memory-mcp\":{"
                                "\"command\":\"codebase-memory-mcp\"}}}");

    int rc = cbm_upsert_antigravity_mcp("/new/path/codebase-memory-mcp", configpath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(configpath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"command\":\"codebase-memory-mcp\"") == NULL);
    ASSERT(strstr(data, "/new/path/codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_json_rejects_overlong_path_without_truncated_parent) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-json-long-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char unexpected[512];
    snprintf(unexpected, sizeof(unexpected), "%s/a", tmpdir);
    char *configpath = make_overlong_nested_path(tmpdir, "mcp_config.json");
    ASSERT_NOT_NULL(configpath);

    int rc = cbm_upsert_antigravity_mcp("/usr/local/bin/codebase-memory-mcp", configpath);
    ASSERT_NEQ(rc, 0);
    ASSERT_FALSE(test_path_exists(unexpected));

    free(configpath);
    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group C: Instructions File Upsert
 * ═══════════════════════════════════════════════════════════════════ */

/* #1032: Aider has no MCP support, but its installed CONVENTIONS.md told the
 * model to call MCP tools it cannot invoke (search_graph(...) style). The
 * Aider variant must teach the runnable CLI form instead. */
TEST(cli_aider_instructions_are_cli_form_issue1032) {
    const char *content = cbm_get_aider_instructions();
    ASSERT_NOT_NULL(content);
    /* Every discovery example is a runnable CLI command... */
    ASSERT(strstr(content, "codebase-memory-mcp cli search_graph") != NULL);
    ASSERT(strstr(content, "codebase-memory-mcp cli trace_path") != NULL);
    ASSERT(strstr(content, "codebase-memory-mcp cli index_repository") != NULL);
    /* ...and no bare MCP-call syntax remains to mislead the model. */
    ASSERT_NULL(strstr(content, "search_graph(name_pattern"));
    /* States the constraint explicitly. */
    ASSERT(strstr(content, "no MCP support") != NULL);
    PASS();
}

TEST(cli_upsert_instructions_fresh) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-instr-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/AGENTS.md", tmpdir);

    int rc = cbm_upsert_instructions(filepath, "# Test content\nHello world\n");
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(filepath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "<!-- codebase-memory-mcp:start -->") != NULL);
    ASSERT(strstr(data, "<!-- codebase-memory-mcp:end -->") != NULL);
    ASSERT(strstr(data, "Hello world") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_instructions_rejects_overlong_path_without_truncated_parent) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-instr-long-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char unexpected[512];
    snprintf(unexpected, sizeof(unexpected), "%s/a", tmpdir);
    char *filepath = make_overlong_nested_path(tmpdir, "AGENTS.md");
    ASSERT_NOT_NULL(filepath);

    int rc = cbm_upsert_instructions(filepath, "# Test content\n");
    ASSERT_NEQ(rc, 0);
    ASSERT_FALSE(test_path_exists(unexpected));

    free(filepath);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_instructions_existing) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-instr-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/AGENTS.md", tmpdir);
    write_test_file(filepath, "# My Project Rules\n\nDo the thing.\n");

    int rc = cbm_upsert_instructions(filepath, "# CMM\nUse search_graph\n");
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(filepath);
    ASSERT_NOT_NULL(data);
    /* Original content preserved */
    ASSERT(strstr(data, "My Project Rules") != NULL);
    ASSERT(strstr(data, "Do the thing") != NULL);
    /* CMM section appended */
    ASSERT(strstr(data, "codebase-memory-mcp:start") != NULL);
    ASSERT(strstr(data, "search_graph") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_instructions_replace) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-instr-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/AGENTS.md", tmpdir);
    write_test_file(filepath, "# Rules\n"
                              "<!-- codebase-memory-mcp:start -->\n"
                              "OLD CONTENT\n"
                              "<!-- codebase-memory-mcp:end -->\n"
                              "# Other stuff\n");

    int rc = cbm_upsert_instructions(filepath, "NEW CONTENT\n");
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(filepath);
    ASSERT_NOT_NULL(data);
    /* Old content replaced */
    ASSERT(strstr(data, "OLD CONTENT") == NULL);
    ASSERT(strstr(data, "NEW CONTENT") != NULL);
    /* Surrounding content preserved */
    ASSERT(strstr(data, "# Rules") != NULL);
    ASSERT(strstr(data, "# Other stuff") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_instructions_no_duplicate) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-instr-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/AGENTS.md", tmpdir);

    /* Install twice */
    cbm_upsert_instructions(filepath, "Content v1\n");
    cbm_upsert_instructions(filepath, "Content v2\n");

    const char *data = read_test_file(filepath);
    ASSERT_NOT_NULL(data);
    /* Only one start marker */
    int count = 0;
    const char *p = data;
    while ((p = strstr(p, "codebase-memory-mcp:start")) != NULL) {
        count++;
        p += 25;
    }
    ASSERT_EQ(count, 1);
    /* Latest content */
    ASSERT(strstr(data, "Content v2") != NULL);
    ASSERT(strstr(data, "Content v1") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_remove_instructions) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-instr-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/AGENTS.md", tmpdir);
    write_test_file(filepath, "# Rules\n"
                              "<!-- codebase-memory-mcp:start -->\n"
                              "CMM Content\n"
                              "<!-- codebase-memory-mcp:end -->\n"
                              "# Other\n");

    int rc = cbm_remove_instructions(filepath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(filepath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "CMM Content") == NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") == NULL);
    ASSERT(strstr(data, "# Rules") != NULL);
    ASSERT(strstr(data, "# Other") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_agent_instructions_content) {
    const char *instr = cbm_get_agent_instructions();
    ASSERT_NOT_NULL(instr);
    ASSERT(strstr(instr, "search_graph") != NULL);
    ASSERT(strstr(instr, "trace_path") != NULL);
    ASSERT(strstr(instr, "get_code") != NULL);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group D: Pre-Tool Hook Upsert — Claude Code
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_upsert_claude_hook_fresh) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);

    int rc = cbm_upsert_claude_hooks(settingspath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "PreToolUse") != NULL);
    ASSERT(strstr(data, "PostToolUse") != NULL);
    ASSERT(strstr(data, "\"Grep|Glob\"") != NULL);
    ASSERT(strstr(data, "\"Read\"") != NULL);
    ASSERT(strstr(data, "\"Grep|Glob|Read\"") == NULL);
    ASSERT_EQ(test_count_substring(data, "cbm-code-discovery-gate"), 2U);

    test_rmdir_r(tmpdir);
    PASS();
}

/* issue #384: the PreToolUse gate shim must never use a predictable /tmp
 * filename (the old `/tmp/cbm-code-discovery-gate-$PPID` was a symlink-attack
 * vector). The shim is now a stateless wrapper around the compiled augmenter. */
TEST(cli_hook_gate_script_no_predictable_tmp_issue384) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-gate-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_install_hook_gate_script(tmpdir, "/usr/local/bin/codebase-memory-mcp");

    char script_path[512];
#ifdef _WIN32
    snprintf(script_path, sizeof(script_path), "%s/.claude/hooks/cbm-code-discovery-gate.cmd",
             tmpdir);
#else
    snprintf(script_path, sizeof(script_path), "%s/.claude/hooks/cbm-code-discovery-gate", tmpdir);
#endif
    const char *data = read_test_file(script_path);
    ASSERT_NOT_NULL(data);
    /* No predictable temp/state file and no PPID-derived path. */
    ASSERT(strstr(data, "/tmp") == NULL);
    ASSERT(strstr(data, "PPID") == NULL);
    /* It delegates to the stateless compiled augmenter (stdout only). */
    ASSERT(strstr(data, "hook-augment") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_hook_gate_script_rejects_overlong_home_without_truncated_parent) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-gate-long-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *saved_ccd = getenv("CLAUDE_CONFIG_DIR");
    char *saved_ccd_copy = saved_ccd ? strdup(saved_ccd) : NULL;
    cbm_unsetenv("CLAUDE_CONFIG_DIR");

    char unexpected[512];
    snprintf(unexpected, sizeof(unexpected), "%s/a", tmpdir);
    char *home = make_overlong_nested_path(tmpdir, "home");
    ASSERT_NOT_NULL(home);

    cbm_install_hook_gate_script(home, "/usr/local/bin/codebase-memory-mcp");
    ASSERT_FALSE(test_path_exists(unexpected));

    free(home);
    if (saved_ccd_copy) {
        cbm_setenv("CLAUDE_CONFIG_DIR", saved_ccd_copy, 1);
        free(saved_ccd_copy);
    } else {
        cbm_unsetenv("CLAUDE_CONFIG_DIR");
    }
    test_rmdir_r(tmpdir);
    PASS();
}

/* #929: on Windows, extensionless bash shims under .claude/hooks trigger the
 * "How do you want to open this file?" dialog when editors (Cursor) scan the
 * dir, and cannot execute without bash. Windows must install .cmd scripts
 * (and remove the extensionless legacy twin on upgrade); POSIX keeps the
 * extensionless bash form with no .cmd twin. */
TEST(cli_hook_scripts_platform_shape_issue929) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook929-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char hooks_dir[512];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.claude/hooks", tmpdir);

#ifdef _WIN32
    /* Upgrade path: seed byte-exact pre-#929 owned content at the extensionless
     * path. Only exact-owned bytes may be removed. */
    cbm_mkdir_p(hooks_dir, 0755);
    char legacy_path[512];
    char seed_path[512];
    snprintf(legacy_path, sizeof(legacy_path), "%s/cbm-code-discovery-gate", hooks_dir);
    snprintf(seed_path, sizeof(seed_path), "%s/cbm-code-discovery-gate.cmd", hooks_dir);
    ASSERT_TRUE(cbm_install_hook_gate_script(tmpdir, "/usr/local/bin/codebase-memory-mcp"));
    char *owned_legacy = read_test_file_alloc(seed_path);
    ASSERT_NOT_NULL(owned_legacy);
    ASSERT_EQ(write_test_file(legacy_path, owned_legacy), 0);
    free(owned_legacy);
    ASSERT_EQ(cbm_unlink(seed_path), 0);
#endif

    cbm_install_hook_gate_script(tmpdir, "/usr/local/bin/codebase-memory-mcp");

    char script_path[512];
#ifdef _WIN32
    snprintf(script_path, sizeof(script_path), "%s/cbm-code-discovery-gate.cmd", hooks_dir);
    const char *data = read_test_file(script_path);
    ASSERT_NOT_NULL(data);
    ASSERT(strncmp(data, "@echo off", 9) == 0); /* cmd, not bash */
    ASSERT(strstr(data, "setlocal DisableDelayedExpansion") != NULL);
    ASSERT(strstr(data, "#!/usr/bin/env bash") == NULL);
    ASSERT(strstr(data, "hook-augment") != NULL);
    /* Legacy extensionless twin removed on upgrade. */
    FILE *lf = fopen(legacy_path, "r");
    if (lf) {
        fclose(lf);
        FAIL("legacy extensionless hook file still present after install");
    }
#else
    snprintf(script_path, sizeof(script_path), "%s/cbm-code-discovery-gate", hooks_dir);
    const char *data = read_test_file(script_path);
    ASSERT_NOT_NULL(data);
    ASSERT(strncmp(data, "#!/usr/bin/env bash", 19) == 0);
    /* No .cmd twin on POSIX. */
    char cmd_path[512];
    snprintf(cmd_path, sizeof(cmd_path), "%s/cbm-code-discovery-gate.cmd", hooks_dir);
    FILE *cf = fopen(cmd_path, "r");
    if (cf) {
        fclose(cf);
        FAIL(".cmd twin must not exist on POSIX");
    }
#endif
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_claude_hooks_reject_overlong_config_dir_command) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-env-long-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    const char *saved_ccd = getenv("CLAUDE_CONFIG_DIR");
    char *saved_ccd_copy = saved_ccd ? strdup(saved_ccd) : NULL;
    char *config_dir = make_overlong_nested_path(tmpdir, "claude-config");
    ASSERT_NOT_NULL(config_dir);
    cbm_setenv("CLAUDE_CONFIG_DIR", config_dir, 1);

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);
    ASSERT_NEQ(cbm_upsert_claude_hooks(settingspath), 0);
    ASSERT_NEQ(cbm_upsert_claude_session_hooks(settingspath), 0);
    ASSERT_FALSE(test_path_exists(settingspath));

    char unexpected[512];
    snprintf(unexpected, sizeof(unexpected), "%s/a", tmpdir);
    ASSERT_FALSE(test_path_exists(unexpected));

    free(config_dir);
    if (saved_ccd_copy) {
        cbm_setenv("CLAUDE_CONFIG_DIR", saved_ccd_copy, 1);
        free(saved_ccd_copy);
    } else {
        cbm_unsetenv("CLAUDE_CONFIG_DIR");
    }
    test_rmdir_r(tmpdir);
    PASS();
}

#ifdef _WIN32
TEST(cli_windows_claude_lifecycle_migrates_only_exact_owned_legacy_state) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-windows-legacy-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    char hooks_dir[640];
    char settings_path[640];
    char appdata[512];
    char binary_path[640];
    snprintf(config_dir, sizeof(config_dir), "%s/.claude", tmpdir);
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/hooks", config_dir);
    snprintf(settings_path, sizeof(settings_path), "%s/settings.json", config_dir);
    snprintf(appdata, sizeof(appdata), "%s/AppData/Roaming", tmpdir);
    snprintf(binary_path, sizeof(binary_path), "%s/.local/bin/codebase-memory-mcp.exe", tmpdir);
    test_mkdirp(hooks_dir);

    const char *const script_names[] = {
        "cbm-code-discovery-gate",
        "cbm-session-reminder",
        "cbm-subagent-reminder",
    };
    const char *foreign_script = "@echo off\r\necho user-owned-hook\r\n";
    for (size_t i = 0U; i < sizeof(script_names) / sizeof(script_names[0]); i++) {
        char path[768];
        snprintf(path, sizeof(path), "%s/%s", hooks_dir, script_names[i]);
        write_test_file(path, foreign_script);
    }

    const char *const env_names[] = {"HOME", "PATH", "CLAUDE_CONFIG_DIR", "APPDATA"};
    char *saved_env[sizeof(env_names) / sizeof(env_names[0])];
    for (size_t i = 0U; i < sizeof(env_names) / sizeof(env_names[0]); i++) {
        saved_env[i] = save_test_env(env_names[i]);
    }
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_setenv("CLAUDE_CONFIG_DIR", config_dir, 1);
    cbm_setenv("APPDATA", appdata, 1);

    char session_current[1024] = {0};
    char session_previous[1024] = {0};
    char session_released[1024] = {0};
    char subagent_current[1024] = {0};
    char subagent_previous[1024] = {0};
    char subagent_released[1024] = {0};
    bool commands_ready =
        cbm_resolve_claude_hook_command_for_testing(
            "cbm-session-reminder.cmd", true, session_current, sizeof(session_current)) == 0 &&
        cbm_resolve_claude_hook_command_for_testing("cbm-session-reminder", false, session_previous,
                                                    sizeof(session_previous)) == 0 &&
        cbm_resolve_claude_hook_command_for_testing(
            "cbm-subagent-reminder.cmd", true, subagent_current, sizeof(subagent_current)) == 0 &&
        cbm_resolve_claude_hook_command_for_testing(
            "cbm-subagent-reminder", false, subagent_previous, sizeof(subagent_previous)) == 0;
    snprintf(session_released, sizeof(session_released), "%s/cbm-session-reminder", hooks_dir);
    snprintf(subagent_released, sizeof(subagent_released), "%s/cbm-subagent-reminder", hooks_dir);
    const char *foreign_command = "cmd.exe /d /s /c user-owned-hook.cmd";

    yyjson_mut_doc *initial_doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = initial_doc ? yyjson_mut_obj(initial_doc) : NULL;
    yyjson_mut_val *hooks = initial_doc ? yyjson_mut_obj(initial_doc) : NULL;
    yyjson_mut_val *session_entries = initial_doc ? yyjson_mut_arr(initial_doc) : NULL;
    yyjson_mut_val *subagent_entries = initial_doc ? yyjson_mut_arr(initial_doc) : NULL;
    if (initial_doc && root) {
        yyjson_mut_doc_set_root(initial_doc, root);
    }
    bool json_ready =
        commands_ready && initial_doc && root && hooks && session_entries && subagent_entries &&
        yyjson_mut_obj_add_val(initial_doc, root, "hooks", hooks) &&
        yyjson_mut_obj_add_val(initial_doc, hooks, "SessionStart", session_entries) &&
        yyjson_mut_obj_add_val(initial_doc, hooks, "SubagentStart", subagent_entries) &&
        test_append_command_hook(initial_doc, session_entries, "startup", session_current) &&
        test_append_command_hook(initial_doc, session_entries, "startup", session_previous) &&
        test_append_command_hook(initial_doc, session_entries, "startup", session_released) &&
        test_append_command_hook(initial_doc, session_entries, "startup", foreign_command) &&
        test_append_command_hook(initial_doc, subagent_entries, "*", subagent_current) &&
        test_append_command_hook(initial_doc, subagent_entries, "*", subagent_previous) &&
        test_append_command_hook(initial_doc, subagent_entries, "*", subagent_released) &&
        test_append_command_hook(initial_doc, subagent_entries, "*", foreign_command);
    char *initial_json =
        json_ready ? yyjson_mut_write(initial_doc, YYJSON_WRITE_PRETTY, NULL) : NULL;
    bool seeded = initial_json && write_test_file(settings_path, initial_json) == 0;
    free(initial_json);
    yyjson_mut_doc_free(initial_doc);

    int install_rc = seeded ? cbm_install_agent_configs(tmpdir, binary_path, false, false) : -1;
    char *installed_settings = read_test_file_alloc(settings_path);
    yyjson_doc *installed_doc =
        installed_settings ? yyjson_read(installed_settings, strlen(installed_settings), 0) : NULL;
    yyjson_val *installed_root = installed_doc ? yyjson_doc_get_root(installed_doc) : NULL;
    bool commands_migrated =
        install_rc == 0 &&
        test_count_hook_command(installed_root, "SessionStart", session_current) == 4U &&
        test_count_hook_command(installed_root, "SessionStart", session_previous) == 0U &&
        test_count_hook_command(installed_root, "SessionStart", session_released) == 0U &&
        test_count_hook_command(installed_root, "SessionStart", foreign_command) == 1U &&
        test_count_hook_command(installed_root, "SubagentStart", subagent_current) == 1U &&
        test_count_hook_command(installed_root, "SubagentStart", subagent_previous) == 0U &&
        test_count_hook_command(installed_root, "SubagentStart", subagent_released) == 0U &&
        test_count_hook_command(installed_root, "SubagentStart", foreign_command) == 1U;
    yyjson_doc_free(installed_doc);
    free(installed_settings);

    bool foreign_scripts_preserved = true;
    for (size_t i = 0U; i < sizeof(script_names) / sizeof(script_names[0]); i++) {
        char path[768];
        snprintf(path, sizeof(path), "%s/%s", hooks_dir, script_names[i]);
        char *data = read_test_file_alloc(path);
        foreign_scripts_preserved =
            foreign_scripts_preserved && data && strcmp(data, foreign_script) == 0;
        free(data);
    }

    char *uninstall_argv[] = {"uninstall", "--yes"};
    int uninstall_rc = cbm_cmd_uninstall(2, uninstall_argv);
    char *uninstalled_settings = read_test_file_alloc(settings_path);
    yyjson_doc *uninstalled_doc =
        uninstalled_settings ? yyjson_read(uninstalled_settings, strlen(uninstalled_settings), 0)
                             : NULL;
    yyjson_val *uninstalled_root = uninstalled_doc ? yyjson_doc_get_root(uninstalled_doc) : NULL;
    bool commands_clean =
        uninstall_rc == 0 &&
        test_count_hook_command(uninstalled_root, "SessionStart", session_current) == 0U &&
        test_count_hook_command(uninstalled_root, "SessionStart", session_previous) == 0U &&
        test_count_hook_command(uninstalled_root, "SessionStart", session_released) == 0U &&
        test_count_hook_command(uninstalled_root, "SessionStart", foreign_command) == 1U &&
        test_count_hook_command(uninstalled_root, "SubagentStart", subagent_current) == 0U &&
        test_count_hook_command(uninstalled_root, "SubagentStart", subagent_previous) == 0U &&
        test_count_hook_command(uninstalled_root, "SubagentStart", subagent_released) == 0U &&
        test_count_hook_command(uninstalled_root, "SubagentStart", foreign_command) == 1U;
    yyjson_doc_free(uninstalled_doc);
    free(uninstalled_settings);

    for (size_t i = 0U; i < sizeof(env_names) / sizeof(env_names[0]); i++) {
        restore_test_env(env_names[i], saved_env[i]);
    }
    test_rmdir_r(tmpdir);
    if (!commands_migrated || !foreign_scripts_preserved || !commands_clean)
        FAIL("Windows lifecycle migration must converge exact-owned commands and preserve foreign "
             "extensionless scripts");
    PASS();
}

TEST(cli_windows_claude_hook_scripts_migrate_and_uninstall_all_owned_shapes) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-windows-owned-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char config_dir[512];
    char hooks_dir[640];
    char appdata[512];
    char binary_path[640];
    snprintf(config_dir, sizeof(config_dir), "%s/.claude", tmpdir);
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/hooks", config_dir);
    snprintf(appdata, sizeof(appdata), "%s/AppData/Roaming", tmpdir);
    snprintf(binary_path, sizeof(binary_path), "%s/.local/bin/codebase-memory-mcp.exe", tmpdir);
    test_mkdirp(hooks_dir);

    const char *const env_names[] = {"HOME",        "PATH",       "CLAUDE_CONFIG_DIR",
                                     "APPDATA",     "CODEX_HOME", "OPENCODE_CONFIG",
                                     "COPILOT_HOME"};
    char *saved_env[sizeof(env_names) / sizeof(env_names[0])];
    for (size_t i = 0U; i < sizeof(env_names) / sizeof(env_names[0]); i++) {
        saved_env[i] = save_test_env(env_names[i]);
    }
    cbm_setenv("HOME", tmpdir, 1);
    cbm_setenv("PATH", tmpdir, 1);
    cbm_setenv("CLAUDE_CONFIG_DIR", config_dir, 1);
    cbm_setenv("APPDATA", appdata, 1);
    cbm_unsetenv("CODEX_HOME");
    cbm_unsetenv("OPENCODE_CONFIG");
    cbm_unsetenv("COPILOT_HOME");

    const char *const legacy_names[] = {
        "cbm-code-discovery-gate",
        "cbm-session-reminder",
        "cbm-subagent-reminder",
    };
    const char *const current_names[] = {
        "cbm-code-discovery-gate.cmd",
        "cbm-session-reminder.cmd",
        "cbm-subagent-reminder.cmd",
    };
    char *current_scripts[sizeof(current_names) / sizeof(current_names[0])] = {0};

    int initial_install_rc = cbm_install_agent_configs(tmpdir, binary_path, false, false);
    bool current_scripts_ready = initial_install_rc == 0;
    for (size_t i = 0U; i < sizeof(current_names) / sizeof(current_names[0]); i++) {
        char current_path[768];
        char legacy_path[768];
        snprintf(current_path, sizeof(current_path), "%s/%s", hooks_dir, current_names[i]);
        snprintf(legacy_path, sizeof(legacy_path), "%s/%s", hooks_dir, legacy_names[i]);
        current_scripts[i] = read_test_file_alloc(current_path);
        current_scripts_ready = current_scripts_ready && current_scripts[i] &&
                                write_test_file(legacy_path, current_scripts[i]) == 0;
    }

    int current_upgrade_rc =
        current_scripts_ready ? cbm_install_agent_configs(tmpdir, binary_path, false, false) : -1;
    bool current_legacy_removed = current_upgrade_rc == 0;
    for (size_t i = 0U; i < sizeof(legacy_names) / sizeof(legacy_names[0]); i++) {
        char current_path[768];
        char legacy_path[768];
        struct stat state;
        snprintf(current_path, sizeof(current_path), "%s/%s", hooks_dir, current_names[i]);
        snprintf(legacy_path, sizeof(legacy_path), "%s/%s", hooks_dir, legacy_names[i]);
        current_legacy_removed = current_legacy_removed && stat(legacy_path, &state) != 0 &&
                                 stat(current_path, &state) == 0;
    }

    char released_gate[8192];
    bool released_ready =
        test_build_released_gate_hook_script(binary_path, released_gate, sizeof(released_gate));
    const char *const released_scripts[] = {
        released_gate,
        test_released_session_hook_script,
        test_released_subagent_hook_script,
    };
    for (size_t i = 0U; i < sizeof(legacy_names) / sizeof(legacy_names[0]); i++) {
        char legacy_path[768];
        snprintf(legacy_path, sizeof(legacy_path), "%s/%s", hooks_dir, legacy_names[i]);
        released_ready = released_ready && write_test_file(legacy_path, released_scripts[i]) == 0;
    }

    int released_upgrade_rc =
        released_ready ? cbm_install_agent_configs(tmpdir, binary_path, false, false) : -1;
    bool released_legacy_removed = released_upgrade_rc == 0;
    for (size_t i = 0U; i < sizeof(legacy_names) / sizeof(legacy_names[0]); i++) {
        char legacy_path[768];
        struct stat state;
        snprintf(legacy_path, sizeof(legacy_path), "%s/%s", hooks_dir, legacy_names[i]);
        released_legacy_removed = released_legacy_removed && stat(legacy_path, &state) != 0;
    }

    bool uninstall_seeded = current_scripts_ready;
    for (size_t i = 0U; i < sizeof(legacy_names) / sizeof(legacy_names[0]); i++) {
        char legacy_path[768];
        snprintf(legacy_path, sizeof(legacy_path), "%s/%s", hooks_dir, legacy_names[i]);
        uninstall_seeded = uninstall_seeded && current_scripts[i] &&
                           write_test_file(legacy_path, current_scripts[i]) == 0;
    }
    char *uninstall_argv[] = {"uninstall", "--yes"};
    int uninstall_rc = uninstall_seeded ? cbm_cmd_uninstall(2, uninstall_argv) : -1;
    bool all_owned_shapes_removed = uninstall_rc == 0;
    for (size_t i = 0U; i < sizeof(legacy_names) / sizeof(legacy_names[0]); i++) {
        char current_path[768];
        char legacy_path[768];
        struct stat state;
        snprintf(current_path, sizeof(current_path), "%s/%s", hooks_dir, current_names[i]);
        snprintf(legacy_path, sizeof(legacy_path), "%s/%s", hooks_dir, legacy_names[i]);
        all_owned_shapes_removed = all_owned_shapes_removed && stat(current_path, &state) != 0 &&
                                   stat(legacy_path, &state) != 0;
    }

    for (size_t i = 0U; i < sizeof(current_scripts) / sizeof(current_scripts[0]); i++) {
        free(current_scripts[i]);
    }
    for (size_t i = 0U; i < sizeof(env_names) / sizeof(env_names[0]); i++) {
        restore_test_env(env_names[i], saved_env[i]);
    }
    test_rmdir_r(tmpdir);
    if (!current_legacy_removed || !released_legacy_removed || !all_owned_shapes_removed)
        FAIL("Windows lifecycle must migrate and uninstall current and released owned hook "
             "script shapes");
    PASS();
}
#endif

/* Claude may execute shell-form hooks through PowerShell when Git Bash is not
 * available. Windows registrations must therefore invoke the .cmd shim via an
 * explicit command interpreter instead of evaluating a quoted path string. */
TEST(cli_windows_claude_hook_command_is_shell_portable) {
    char *saved_config = save_test_env("CLAUDE_CONFIG_DIR");
    char command[1024];

    cbm_unsetenv("CLAUDE_CONFIG_DIR");
    ASSERT_EQ(cbm_resolve_claude_hook_command_for_testing("cbm-session-reminder.cmd", true, command,
                                                          sizeof(command)),
              0);
    ASSERT_STR_EQ(command, "cmd.exe /d /v:off /s /c '\"\"%USERPROFILE%\\.claude\\hooks\\"
                           "cbm-session-reminder.cmd\"\"'");

    cbm_setenv("CLAUDE_CONFIG_DIR", "C:\\Users\\A & B\\.claude!100%", 1);
    ASSERT_EQ(cbm_resolve_claude_hook_command_for_testing("cbm-subagent-reminder.cmd", true,
                                                          command, sizeof(command)),
              0);
    ASSERT_STR_EQ(command, "cmd.exe /d /v:off /s /c '\"\"%CLAUDE_CONFIG_DIR%\\hooks\\"
                           "cbm-subagent-reminder.cmd\"\"'");
    ASSERT(strstr(command, "A & B") == NULL);
    ASSERT_EQ(cbm_resolve_claude_hook_command_for_testing("../foreign.cmd", true, command,
                                                          sizeof(command)),
              -1);

    restore_test_env("CLAUDE_CONFIG_DIR", saved_config);
    PASS();
}

/* issue #618: hook-augment was a structural no-op on Windows because its path
 * guards required POSIX-style '/'-prefixed absolute paths, so a drive-letter
 * cwd (C:/repo) was rejected before any search_graph query. The predicate must
 * accept POSIX and Windows drive roots alike (callers normalize '\\' to '/'). */
TEST(cli_hook_augment_path_is_abs) {
    /* POSIX absolute (unchanged behavior) */
    ASSERT(cbm_hook_path_is_abs("/home/u/proj"));
    /* Windows drive roots — the #618 regression */
    ASSERT(cbm_hook_path_is_abs("C:/Users/me/proj"));
    ASSERT(cbm_hook_path_is_abs("C:/"));
    ASSERT(cbm_hook_path_is_abs("C:"));
    ASSERT(cbm_hook_path_is_abs("d:/lowercase/drive"));
    /* Not absolute → augmenter no-ops cleanly */
    ASSERT(!cbm_hook_path_is_abs("relative/path"));
    ASSERT(!cbm_hook_path_is_abs("proj"));
    ASSERT(!cbm_hook_path_is_abs(""));
    ASSERT(!cbm_hook_path_is_abs(NULL));
    PASS();
}

/* #858: a fired hook-augment deadline used to be a SILENT _exit(0) —
 * indistinguishable from "no matches" — and the 300ms default self-terminated
 * on real cold starts, so augmentation never appeared in real sessions
 * (0/24 observed). The deadline is now env-configurable
 * (CBM_HOOK_DEADLINE_MS, generous default) and a fired deadline leaves an
 * observable breadcrumb in a local log. Deterministic reproduction: stdin is
 * a pipe with a live writer that never sends data, so ha_read_stdin blocks
 * past a 60ms deadline and the timer must fire, breadcrumb, and _exit(0). */
TEST(cli_hook_augment_deadline_breadcrumb_issue858) {
#ifdef _WIN32
    SKIP_PLATFORM("in-process SIGALRM deadline is POSIX-only (settings.json timeout on Windows)");
#else
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hookdl-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char logpath[512];
    snprintf(logpath, sizeof(logpath), "%s/timeouts.log", tmpdir);

    int fds[2];
    if (pipe(fds) != 0) {
        test_rmdir_r(tmpdir);
        FAIL("pipe failed");
    }

    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        /* Child: hook-augment with a 60ms deadline and stdin that blocks
         * forever (parent keeps the write end open, sends nothing). */
        close(fds[1]);
        dup2(fds[0], 0);
        close(fds[0]);
        setenv("CBM_HOOK_DEADLINE_MS", "60", 1);
        setenv("CBM_HOOK_TIMEOUT_LOG", logpath, 1);
        alarm(10); /* backstop: never hang the suite */
        _exit(cbm_cmd_hook_augment(0, NULL));
    }
    ASSERT_GT(pid, 0);
    close(fds[0]);

    int status = 0;
    waitpid(pid, &status, 0);
    close(fds[1]);

    /* The deadline must have fired as a clean exit 0 (fail-open, no signal). */
    ASSERT(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    /* RED before the fix: no breadcrumb existed — a fired deadline was
     * indistinguishable from a no-match run. GREEN: the log names the
     * deadline and the knob. */
    FILE *f = fopen(logpath, "r");
    if (!f) {
        fprintf(stderr, "  [858] FAIL no timeout breadcrumb written to %s\n", logpath);
    }
    ASSERT_NOT_NULL(f);
    char line[256] = "";
    char *got = fgets(line, sizeof(line), f);
    fclose(f);
    ASSERT_NOT_NULL(got);
    ASSERT(strstr(line, "deadline_exceeded") != NULL);
    ASSERT(strstr(line, "CBM_HOOK_DEADLINE_MS") != NULL);

    cbm_unsetenv("CBM_HOOK_DEADLINE_MS");
    cbm_unsetenv("CBM_HOOK_TIMEOUT_LOG");
    test_rmdir_r(tmpdir);
    PASS();
#endif
}

TEST(cli_upsert_claude_hook_existing) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);
    /* Pre-existing settings with other hooks */
    write_test_file(settingspath,
                    "{\"hooks\":{\"PreToolUse\":[{\"matcher\":\"Bash\","
                    "\"hooks\":[{\"type\":\"command\",\"command\":\"echo firewall\"}]}]}}");

    int rc = cbm_upsert_claude_hooks(settingspath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"Grep|Glob\"") != NULL);
    ASSERT(strstr(data, "PostToolUse") != NULL);
    ASSERT(strstr(data, "\"Read\"") != NULL);
    /* Existing hook preserved */
    ASSERT(strstr(data, "Bash") != NULL);
    ASSERT(strstr(data, "firewall") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_tool_hooks_preserve_foreign_same_matcher) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-owner-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char claude_path[512];
    char gemini_path[512];
    snprintf(claude_path, sizeof(claude_path), "%s/claude.json", tmpdir);
    snprintf(gemini_path, sizeof(gemini_path), "%s/gemini.json", tmpdir);
    write_test_file(claude_path, "{\"hooks\":{\"PreToolUse\":[{\"matcher\":\"Grep|Glob|Read\","
                                 "\"hooks\":[{\"type\":\"command\","
                                 "\"command\":\"echo user-claude-tool-hook\"}]},"
                                 "{\"matcher\":\"Grep|Glob|Read\",\"hooks\":["
                                 "{\"type\":\"command\",\"command\":"
                                 "\"~/.claude/hooks/cbm-code-discovery-gate\"},"
                                 "{\"type\":\"command\",\"command\":"
                                 "\"echo user-claude-sibling\"}]}]}}\n");
    write_test_file(gemini_path, "{\"hooks\":{\"BeforeTool\":[{"
                                 "\"matcher\":\"google_web_search|grep_search\","
                                 "\"hooks\":[{\"type\":\"command\","
                                 "\"command\":\"echo user-gemini-tool-hook\"}]}]}}\n");

    ASSERT_EQ(cbm_upsert_claude_hooks(claude_path), 0);
    ASSERT_EQ(cbm_upsert_gemini_hooks(gemini_path), 0);
    char *claude = read_test_file_alloc(claude_path);
    char *gemini = read_test_file_alloc(gemini_path);
    bool installed = claude && strstr(claude, "user-claude-tool-hook") &&
                     strstr(claude, "user-claude-sibling") &&
                     strstr(claude, "cbm-code-discovery-gate") && gemini &&
                     strstr(gemini, "user-gemini-tool-hook") &&
                     strstr(gemini, "codebase-memory-mcp search_graph");
    free(claude);
    free(gemini);

    ASSERT_EQ(cbm_remove_claude_hooks(claude_path), 0);
    ASSERT_EQ(cbm_remove_gemini_hooks(gemini_path), 0);
    claude = read_test_file_alloc(claude_path);
    gemini = read_test_file_alloc(gemini_path);
    bool removed_owned_only = claude && strstr(claude, "user-claude-tool-hook") &&
                              strstr(claude, "user-claude-sibling") &&
                              !strstr(claude, "cbm-code-discovery-gate") && gemini &&
                              strstr(gemini, "user-gemini-tool-hook") &&
                              !strstr(gemini, "codebase-memory-mcp search_graph");
    free(claude);
    free(gemini);
    test_rmdir_r(tmpdir);
    if (!installed || !removed_owned_only)
        FAIL("tool hook ownership must include the installed command, not only its matcher");
    PASS();
}

TEST(cli_upsert_claude_hook_replace) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);
    /* Pre-existing CMM hook with an OLD matcher (pre-#963) + old message */
    write_test_file(settingspath, "{\"hooks\":{\"PreToolUse\":[{\"matcher\":\"Grep|Glob\","
                                  "\"hooks\":[{\"type\":\"command\","
                                  "\"command\":\"~/.claude/hooks/cbm-code-discovery-gate\"}]}]}}");

    int rc = cbm_upsert_claude_hooks(settingspath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"Grep|Glob|Read\"") == NULL);
    ASSERT(strstr(data, "\"Grep|Glob\"") != NULL);
    ASSERT(strstr(data, "PostToolUse") != NULL);
    ASSERT_EQ(test_count_substring(data, "cbm-code-discovery-gate"), 2U);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_claude_hook_preserves_others) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);
    write_test_file(settingspath,
                    "{\"apiKey\":\"sk-123\","
                    "\"hooks\":{\"PreToolUse\":[{\"matcher\":\"Bash\","
                    "\"hooks\":[{\"type\":\"command\",\"command\":\"echo guard\"}]}]}}");

    cbm_upsert_claude_hooks(settingspath);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    /* Non-hook settings preserved */
    ASSERT(strstr(data, "apiKey") != NULL);
    ASSERT(strstr(data, "sk-123") != NULL);
    /* Bash hook preserved */
    ASSERT(strstr(data, "Bash") != NULL);
    ASSERT(strstr(data, "guard") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_remove_claude_hooks) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-hook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);

    /* Install then remove */
    cbm_upsert_claude_hooks(settingspath);
    int rc = cbm_remove_claude_hooks(settingspath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "Grep|Glob|Read") == NULL);
    ASSERT(strstr(data, "cbm-code-discovery-gate") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_claude_session_hooks_all_lifecycle_matchers) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-session-hook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);

    ASSERT_EQ(cbm_upsert_claude_session_hooks(settingspath), 0);
    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT_EQ(count_substr(data, "\"SessionStart\""), 1);
    ASSERT(strstr(data, "\"startup\"") != NULL);
    ASSERT(strstr(data, "\"resume\"") != NULL);
    ASSERT(strstr(data, "\"clear\"") != NULL);
    ASSERT(strstr(data, "\"compact\"") != NULL);
    ASSERT_EQ(count_substr(data, "cbm-session-reminder"), 4);

    ASSERT_EQ(cbm_remove_claude_session_hooks(settingspath), 0);
    data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT_NULL(strstr(data, "SessionStart"));
    ASSERT_NULL(strstr(data, "cbm-session-reminder"));

    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group D: Pre-Tool Hook Upsert — Gemini CLI / Antigravity
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_upsert_gemini_hook_fresh) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-ghook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);

    int rc = cbm_upsert_gemini_hooks(settingspath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "BeforeTool") != NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);
    if (!strstr(data, "google_web_search"))
        FAIL("Gemini BeforeTool hook must use the current google_web_search tool name");
    if (!strstr(data, "hookSpecificOutput") || !strstr(data, "additionalContext"))
        FAIL("Gemini BeforeTool hook must emit JSON additionalContext, not bare stderr text");

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_gemini_hook_existing) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-ghook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);
    write_test_file(settingspath,
                    "{\"hooks\":{\"BeforeTool\":[{\"matcher\":\"shell\","
                    "\"hooks\":[{\"type\":\"command\",\"command\":\"echo guard\"}]}]}}");

    int rc = cbm_upsert_gemini_hooks(settingspath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    /* Our hook added */
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);
    /* Existing hook preserved */
    ASSERT(strstr(data, "shell") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_upsert_gemini_hook_replace) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-ghook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);
    write_test_file(
        settingspath,
        "{\"hooks\":{\"BeforeTool\":[{\"matcher\":\"google_search|read_file|grep_search\","
        "\"hooks\":[{\"type\":\"command\","
        "\"command\":\"echo 'Reminder: prefer codebase-memory-mcp "
        "search_graph/trace_path/get_code_snippet over grep/file search for code "
        "discovery.' >&2\"}]}]}}");

    int rc = cbm_upsert_gemini_hooks(settingspath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "google_search|read_file|grep_search") == NULL);
    ASSERT(strstr(data, "codebase-memory-mcp") != NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_remove_gemini_hooks) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-ghook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char settingspath[512];
    snprintf(settingspath, sizeof(settingspath), "%s/settings.json", tmpdir);

    cbm_upsert_gemini_hooks(settingspath);
    int rc = cbm_remove_gemini_hooks(settingspath);
    ASSERT_EQ(rc, 0);

    const char *data = read_test_file(settingspath);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "codebase-memory-mcp") == NULL);

    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group E: Skill descriptions use directive pattern
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_skill_descriptions_directive) {
    /* Verify skill description has trigger phrases for agent matching */
    const cbm_skill_t *sk = cbm_get_skills();
    for (int i = 0; i < CBM_SKILL_COUNT; i++) {
        ASSERT(strstr(sk[i].content, "Triggers on:") != NULL);
        ASSERT(strstr(sk[i].content, "search_graph") != NULL);
    }
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group F: Config store (persistent key-value)
 * ═══════════════════════════════════════════════════════════════════ */

TEST(cli_config_open_close) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_config_close(cfg);

    /* DB file should exist */
    char dbpath[512];
    snprintf(dbpath, sizeof(dbpath), "%s/_config.db", tmpdir);
    struct stat st;
    ASSERT_EQ(stat(dbpath, &st), 0);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_config_get_set) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);

    /* Default when key doesn't exist */
    ASSERT_STR_EQ(cbm_config_get(cfg, "foo", "default"), "default");

    /* Set and get */
    ASSERT_EQ(cbm_config_set(cfg, "foo", "bar"), 0);
    ASSERT_STR_EQ(cbm_config_get(cfg, "foo", "default"), "bar");

    /* Overwrite */
    ASSERT_EQ(cbm_config_set(cfg, "foo", "baz"), 0);
    ASSERT_STR_EQ(cbm_config_get(cfg, "foo", "default"), "baz");

    cbm_config_close(cfg);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_config_get_bool) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);

    /* Default */
    ASSERT_FALSE(cbm_config_get_bool(cfg, "auto_index", false));
    ASSERT_TRUE(cbm_config_get_bool(cfg, "auto_index", true));

    /* true variants */
    cbm_config_set(cfg, "k1", "true");
    ASSERT_TRUE(cbm_config_get_bool(cfg, "k1", false));
    cbm_config_set(cfg, "k2", "1");
    ASSERT_TRUE(cbm_config_get_bool(cfg, "k2", false));
    cbm_config_set(cfg, "k3", "on");
    ASSERT_TRUE(cbm_config_get_bool(cfg, "k3", false));

    /* false variants */
    cbm_config_set(cfg, "k4", "false");
    ASSERT_FALSE(cbm_config_get_bool(cfg, "k4", true));
    cbm_config_set(cfg, "k5", "0");
    ASSERT_FALSE(cbm_config_get_bool(cfg, "k5", true));

    cbm_config_close(cfg);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_config_get_int) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);

    ASSERT_EQ(cbm_config_get_int(cfg, "limit", 50000), 50000);

    cbm_config_set(cfg, "limit", "20000");
    ASSERT_EQ(cbm_config_get_int(cfg, "limit", 50000), 20000);

    /* Non-numeric → default */
    cbm_config_set(cfg, "limit", "abc");
    ASSERT_EQ(cbm_config_get_int(cfg, "limit", 50000), 50000);

    cbm_config_close(cfg);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_config_get_effective_env_overrides_db) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, "auto_index_limit", "111"), 0);

    const char *old_limit = getenv("CBM_AUTO_INDEX_LIMIT");
    char *old_limit_copy = old_limit ? strdup(old_limit) : NULL;
    if (old_limit) {
        ASSERT_NOT_NULL(old_limit_copy);
    }
    cbm_setenv("CBM_AUTO_INDEX_LIMIT", "222", 1);

    ASSERT_STR_EQ(cbm_config_get_effective(cfg, "auto_index_limit", "50000"), "222");
    ASSERT_EQ(cbm_config_get_effective_int(cfg, "auto_index_limit", 50000), 222);

    if (old_limit_copy) {
        cbm_setenv("CBM_AUTO_INDEX_LIMIT", old_limit_copy, 1);
        free(old_limit_copy);
    } else {
        cbm_unsetenv("CBM_AUTO_INDEX_LIMIT");
    }
    cbm_config_close(cfg);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_config_registry_includes_dep_ranking_toggle) {
    const cbm_config_entry_t *found = NULL;
    for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
        if (strcmp(CBM_CONFIG_REGISTRY[i].key, "search_disable_dep_ranking") == 0) {
            found = &CBM_CONFIG_REGISTRY[i];
            break;
        }
    }

    ASSERT_NOT_NULL(found);
    ASSERT_STR_EQ(found->default_val, "false");
    ASSERT_STR_EQ(found->range, "true|false");
    ASSERT_NOT_NULL(strstr(found->description, "search_graph"));
    ASSERT_NOT_NULL(strstr(found->guidance, "dependency"));
    PASS();
}

TEST(cli_config_registry_includes_query_max_rows) {
    const cbm_config_entry_t *found = NULL;
    for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
        if (strcmp(CBM_CONFIG_REGISTRY[i].key, CBM_CONFIG_QUERY_MAX_ROWS) == 0) {
            found = &CBM_CONFIG_REGISTRY[i];
            break;
        }
    }

    ASSERT_NOT_NULL(found);
    ASSERT_STR_EQ(found->default_val, CBM_DEFAULT_QUERY_MAX_ROWS_STR);
    ASSERT_STR_EQ(found->range, "0-1000000");
    ASSERT_NOT_NULL(strstr(found->description, "result-row cap"));
    ASSERT_NOT_NULL(strstr(found->description, "query_graph"));
    ASSERT_NOT_NULL(strstr(found->guidance, "without changing which rows match"));
    ASSERT_NOT_NULL(strstr(found->guidance, "may lower but not bypass this cap"));
    PASS();
}

TEST(cli_config_registry_reindex_startup_guidance_is_precise) {
    const cbm_config_entry_t *found = NULL;
    for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
        if (strcmp(CBM_CONFIG_REGISTRY[i].key, "reindex_on_startup") == 0) {
            found = &CBM_CONFIG_REGISTRY[i];
            break;
        }
    }

    ASSERT_NOT_NULL(found);
    ASSERT_NOT_NULL(strstr(found->guidance, "startup"));
    ASSERT_NOT_NULL(strstr(found->guidance, "stale"));
    ASSERT_NULL(strstr(found->guidance, "always-fresh"));
    PASS();
}

TEST(cli_configuration_doc_auto_index_default_matches_registry) {
    const cbm_config_entry_t *entry = NULL;
    for (int i = 0; CBM_CONFIG_REGISTRY[i].key; i++) {
        if (strcmp(CBM_CONFIG_REGISTRY[i].key, "auto_index") == 0) {
            entry = &CBM_CONFIG_REGISTRY[i];
            break;
        }
    }
    ASSERT_NOT_NULL(entry);

    const char *doc = read_test_file("docs/CONFIGURATION.md");
    ASSERT_NOT_NULL(doc);
    char expected[128];
    snprintf(expected, sizeof(expected), "| `auto_index` | `%s` |", entry->default_val);
    ASSERT_NOT_NULL(strstr(doc, expected));
    PASS();
}

TEST(cli_config_delete) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);

    cbm_config_set(cfg, "foo", "bar");
    ASSERT_STR_EQ(cbm_config_get(cfg, "foo", ""), "bar");

    cbm_config_delete(cfg, "foo");
    ASSERT_STR_EQ(cbm_config_get(cfg, "foo", "gone"), "gone");

    cbm_config_close(cfg);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_config_persists) {
    /* Values survive close + reopen */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    cbm_config_set(cfg, "auto_index", "true");
    cbm_config_close(cfg);

    /* Reopen */
    cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_TRUE(cbm_config_get_bool(cfg, "auto_index", false));
    cbm_config_close(cfg);

    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_config_presets_apply_exact_capability_sets) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);

    ASSERT_EQ(cbm_config_apply_preset(cfg, "streamlined-quality"), 0);
    ASSERT_STR_EQ(cbm_config_get(cfg, CBM_CONFIG_TOOL_MODE, ""), "streamlined");
    ASSERT_TRUE(cbm_config_get_bool(cfg, CBM_CONFIG_RANK_ENABLED, false));
    ASSERT_TRUE(cbm_config_get_bool(cfg, CBM_CONFIG_AUTO_INDEX_DEPS, false));
    ASSERT_TRUE(cbm_config_get_bool(cfg, CBM_CONFIG_SIMILARITY_ENABLED, false));
    ASSERT_TRUE(cbm_config_get_bool(cfg, CBM_CONFIG_SEMANTIC_EDGES_ENABLED, false));
    ASSERT_TRUE(cbm_config_get_bool(cfg, CBM_CONFIG_GITHISTORY_ENABLED, false));
    ASSERT_TRUE(cbm_config_get_bool(cfg, CBM_CONFIG_HTTPLINKS_ENABLED, false));

    ASSERT_EQ(cbm_config_apply_preset(cfg, "minimal-indexing"), 0);
    ASSERT_FALSE(cbm_config_get_bool(cfg, CBM_CONFIG_RANK_ENABLED, true));
    ASSERT_FALSE(cbm_config_get_bool(cfg, CBM_CONFIG_AUTO_INDEX_DEPS, true));
    ASSERT_FALSE(cbm_config_get_bool(cfg, CBM_CONFIG_SIMILARITY_ENABLED, true));
    ASSERT_FALSE(cbm_config_get_bool(cfg, CBM_CONFIG_SEMANTIC_EDGES_ENABLED, true));
    ASSERT_FALSE(cbm_config_get_bool(cfg, CBM_CONFIG_GITHISTORY_ENABLED, true));
    ASSERT_FALSE(cbm_config_get_bool(cfg, CBM_CONFIG_HTTPLINKS_ENABLED, true));
    ASSERT_NEQ(cbm_config_apply_preset(cfg, "unknown"), 0);

    cbm_config_close(cfg);
    test_rmdir_r(tmpdir);
    PASS();
}

/* Named presets are a user-facing config capability, not only an internal
 * benchmark helper. Exercise the real dispatcher so it cannot drift away from
 * the existing atomic preset implementation again. */
TEST(cli_config_command_dispatches_presets) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-cfg-command-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    cli_env_snapshot_t cache = {0};
    cli_env_snapshot_t tool_mode = {0};
    ASSERT_TRUE(cli_env_snapshot(&cache, "CBM_CACHE_DIR"));
    ASSERT_TRUE(cli_env_snapshot(&tool_mode, "CBM_TOOL_MODE"));
    cbm_setenv("CBM_CACHE_DIR", tmpdir, 1);
    cbm_unsetenv("CBM_TOOL_MODE");

    char *preset_list_args[] = {"preset", "list"};
    char *preset_apply_args[] = {"preset", "apply", "minimal-indexing"};
    ASSERT_EQ(cbm_cmd_config(2, preset_list_args), 0);
    ASSERT_EQ(cbm_cmd_config(3, preset_apply_args), 0);

    cbm_config_t *cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_FALSE(cbm_config_get_bool(cfg, CBM_CONFIG_RANK_ENABLED, true));
    ASSERT_FALSE(cbm_config_get_bool(cfg, CBM_CONFIG_AUTO_INDEX_DEPS, true));
    cbm_config_close(cfg);

    /* Stored preset values remain deterministic, but an active environment
     * override must be reported through a nonzero command status. */
    cbm_setenv("CBM_TOOL_MODE", CBM_CONFIG_TOOL_MODE_CLASSIC, 1);
    char *overridden_args[] = {"preset", "apply", "streamlined-quality"};
    ASSERT_NEQ(cbm_cmd_config(3, overridden_args), 0);
    cfg = cbm_config_open(tmpdir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_STR_EQ(cbm_config_get(cfg, CBM_CONFIG_TOOL_MODE, ""),
                  CBM_CONFIG_TOOL_MODE_STREAMLINED);
    ASSERT_STR_EQ(cbm_config_get_effective(cfg, CBM_CONFIG_TOOL_MODE, ""),
                  CBM_CONFIG_TOOL_MODE_CLASSIC);
    cbm_config_close(cfg);

    char *unknown_args[] = {"preset", "apply", "not-a-preset"};
    ASSERT_NEQ(cbm_cmd_config(3, unknown_args), 0);

    cli_env_restore(&tool_mode);
    cli_env_restore(&cache);
    test_rmdir_r(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Group H: cbm_replace_binary (update command helper)
 * ═══════════════════════════════════════════════════════════════════ */

#ifndef _WIN32

TEST(replace_binary_overwrites_readonly) {
    /* Simulate #114: existing binary has mode 0500 (no write permission).
     * cbm_replace_binary must unlink first, then create with 0755. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-replace-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("cbm_mkdtemp failed");
    }

    char path[512];
    snprintf(path, sizeof(path), "%s/test-binary", tmpdir);

    /* Create a read-only file (simulating an installed binary with 0500) */
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fputs("old-content", f);
    fclose(f);
    th_make_executable(path); /* r-x------ */

    /* Replace it with new content */
    const unsigned char new_data[] = "new-content-replaced";
    int rc = cbm_replace_binary(path, new_data, (int)sizeof(new_data) - 1, 0755);
    ASSERT_EQ(rc, 0);

    /* Verify new content was written */
    FILE *check = fopen(path, "r");
    ASSERT_NOT_NULL(check);
    char buf[64] = {0};
    fread(buf, 1, sizeof(buf) - 1, check);
    fclose(check);
    ASSERT_STR_EQ(buf, "new-content-replaced");

    /* Verify permissions are 0755 */
    struct stat st;
    ASSERT_EQ(stat(path, &st), 0);
    ASSERT_EQ(st.st_mode & 0777, 0755);

    cbm_unlink(path);
    cbm_rmdir(tmpdir);
    PASS();
}

TEST(replace_binary_creates_new_file) {
    /* If no existing file, cbm_replace_binary should create it. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-replace2-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("cbm_mkdtemp failed");
    }

    char path[512];
    snprintf(path, sizeof(path), "%s/new-binary", tmpdir);

    const unsigned char data[] = "brand-new";
    int rc = cbm_replace_binary(path, data, (int)sizeof(data) - 1, 0755);
    ASSERT_EQ(rc, 0);

    FILE *check = fopen(path, "r");
    ASSERT_NOT_NULL(check);
    char buf[64] = {0};
    fread(buf, 1, sizeof(buf) - 1, check);
    fclose(check);
    ASSERT_STR_EQ(buf, "brand-new");

    cbm_unlink(path);
    cbm_rmdir(tmpdir);
    PASS();
}

#endif /* _WIN32 */

TEST(cli_remove_indexes_preserves_config_db) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-index-clean-XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("cbm_mkdtemp failed");
    }

    const char *old_cache = getenv("CBM_CACHE_DIR");
    char *old_cache_copy = old_cache ? strdup(old_cache) : NULL;
    if (old_cache) {
        ASSERT_NOT_NULL(old_cache_copy);
    }
    cbm_setenv("CBM_CACHE_DIR", tmpdir, 1);

    char project_db[512];
    char project_tmp[512];
    char config_db[512];
    snprintf(project_db, sizeof(project_db), "%s/project.db", tmpdir);
    snprintf(project_tmp, sizeof(project_tmp), "%s/project.db.tmp", tmpdir);
    snprintf(config_db, sizeof(config_db), "%s/_config.db", tmpdir);
    ASSERT_EQ(write_test_file(project_db, "project"), 0);
    ASSERT_EQ(write_test_file(project_tmp, "tmp"), 0);
    ASSERT_EQ(write_test_file(config_db, "config"), 0);

    ASSERT_EQ(cbm_remove_indexes(NULL), 1);

    struct stat st;
    ASSERT_NEQ(stat(project_db, &st), 0);
    ASSERT_NEQ(stat(project_tmp, &st), 0);
    ASSERT_EQ(stat(config_db, &st), 0);

    if (old_cache_copy) {
        cbm_setenv("CBM_CACHE_DIR", old_cache_copy, 1);
        free(old_cache_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    cbm_unlink(config_db);
    cbm_rmdir(tmpdir);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  CLI tool-argument flags / per-tool --help (#680)
 * ═══════════════════════════════════════════════════════════════════ */

/* A plain `--flag value` pair maps to a string property by schema type. */
TEST(cli_build_args_json_string_flag_issue680) {
    char *err = NULL;
    char *argv[] = {"--repo-path", "/x"};
    char *json = cbm_cli_build_args_json("index_repository", 2, argv, &err);
    ASSERT_NOT_NULL(json);
    ASSERT_NULL(err);
    ASSERT(strstr(json, "\"repo_path\":\"/x\"") != NULL);
    free(json);
    PASS();
}

/* An integer-typed property serializes as a JSON NUMBER, not a quoted string. */
TEST(cli_build_args_json_integer_flag_issue680) {
    char *err = NULL;
    char *argv[] = {"--limit", "100"};
    char *json = cbm_cli_build_args_json("search_graph", 2, argv, &err);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"limit\":100") != NULL);
    ASSERT(strstr(json, "\"limit\":\"100\"") == NULL);
    free(json);
    PASS();
}

/* A bare boolean flag (no value) becomes true. */
TEST(cli_build_args_json_bare_boolean_issue680) {
    char *err = NULL;
    char *argv[] = {"--exclude-entry-points"};
    char *json = cbm_cli_build_args_json("search_graph", 1, argv, &err);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"exclude_entry_points\":true") != NULL);
    free(json);
    PASS();
}

/* An unknown flag for a KNOWN tool must be rejected loudly, not silently
 * typed as a string and dropped server-side (#997). GF1 eval: `trace_path
 * --max-depth 1` was accepted, the real --depth stayed at default 3, and
 * the trace silently returned hop-2/3 results — silent-wrong output. The
 * error names the closest valid flag as a suggestion. */
TEST(cli_build_args_json_unknown_flag_rejected) {
    char *err = NULL;
    char *argv[] = {"--max-depth", "1"};
    char *json = cbm_cli_build_args_json("trace_path", 2, argv, &err);
    ASSERT_NULL(json);
    ASSERT_NOT_NULL(err);
    ASSERT(strstr(err, "unknown flag") != NULL);
    ASSERT(strstr(err, "max-depth") != NULL);
    ASSERT(strstr(err, "--depth") != NULL); /* nearest-flag suggestion */
    free(err);
    PASS();
}

/* A repeated array-typed flag accumulates into a JSON array. */
TEST(cli_build_args_json_repeated_array_issue680) {
    char *err = NULL;
    char *argv[] = {"--semantic-query", "send", "--semantic-query", "publish"};
    char *json = cbm_cli_build_args_json("search_graph", 4, argv, &err);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"semantic_query\":[\"send\",\"publish\"]") != NULL);
    free(json);
    PASS();
}

/* An array-typed flag whose value is itself JSON array text must be parsed
 * into its string elements, not wrapped as one literal element. Previously
 * `--target-projects '["*"]'` produced ["[\"*\"]"]; the cross-repo matcher
 * then treated that as a literal project name, silently created an empty
 * database named ["*"].db, and reported success with zero matches. */
TEST(cli_build_args_json_json_array_value) {
    char *err = NULL;
    char *argv[] = {"--target-projects", "[\"*\"]"};
    char *json = cbm_cli_build_args_json("index_repository", 2, argv, &err);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"target_projects\":[\"*\"]") != NULL);
    ASSERT(strstr(json, "[\\\"*\\\"]") == NULL);
    free(json);

    /* JSON-array values and plain values accumulate into one array. */
    char *argv2[] = {"--target-projects", "[\"a\",\"b\"]", "--target-projects", "c"};
    json = cbm_cli_build_args_json("index_repository", 4, argv2, &err);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"target_projects\":[\"a\",\"b\",\"c\"]") != NULL);
    free(json);
    PASS();
}

/* kebab-case flag names map to snake_case JSON keys. */
TEST(cli_build_args_json_kebab_to_snake_issue680) {
    char *err = NULL;
    char *argv[] = {"--name-pattern", "Foo.*"};
    char *json = cbm_cli_build_args_json("search_graph", 2, argv, &err);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"name_pattern\":\"Foo.*\"") != NULL);
    free(json);
    PASS();
}

/* `--key=value` form splits on the FIRST `=`; value may contain spaces/dashes. */
TEST(cli_build_args_json_key_equals_value_issue680) {
    char *err = NULL;
    char *argv[] = {"--repo-path=/a b"};
    char *json = cbm_cli_build_args_json("index_repository", 1, argv, &err);
    ASSERT_NOT_NULL(json);
    ASSERT(strstr(json, "\"repo_path\":\"/a b\"") != NULL);
    free(json);
    PASS();
}

/* A non-`--` positional is an error: returns NULL and sets *err_out. */
TEST(cli_build_args_json_bad_positional_errors_issue680) {
    char *err = NULL;
    char *argv[] = {"foo"};
    char *json = cbm_cli_build_args_json("search_graph", 1, argv, &err);
    ASSERT_NULL(json);
    ASSERT_NOT_NULL(err);
    free(err);
    PASS();
}

/* Per-tool --help returns 0 for a known tool, -1 for an unknown one. */
TEST(cli_print_tool_help_issue680) {
    ASSERT_EQ(cbm_cli_print_tool_help("index_repository"), 0);
    ASSERT_EQ(cbm_cli_print_tool_help("nope_not_a_tool"), -1);
    PASS();
}

/* Top-level --help must advertise every working config subcommand. `config
 * preset <list|apply>` dispatches in cbm_cmd_config and is listed by the
 * config-specific usage, but the main help's config line omitted it, so
 * `--help` readers never learn presets exist. Capture stdout via pipe+dup2
 * (same technique as test_log.c's stderr capture) and assert the preset
 * line is present next to the other config usage line. */
TEST(cli_main_help_lists_config_preset_subcommand) {
    fflush(stdout);
    int saved_stdout = dup(STDOUT_FILENO);
    ASSERT_TRUE(saved_stdout >= 0);
    int fds[2];
    ASSERT_EQ(cbm_pipe(fds), 0);
    dup2(fds[1], STDOUT_FILENO);
    close(fds[1]);

    cbm_cli_print_main_help();

    fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);

    static char help_buf[8192];
    size_t used = 0;
    ssize_t n;
    while (used < sizeof(help_buf) - 1 &&
           (n = read(fds[0], help_buf + used, sizeof(help_buf) - 1 - used)) > 0) {
        used += (size_t)n;
    }
    close(fds[0]);
    help_buf[used] = '\0';

    /* Existing config line still present ... */
    ASSERT_NOT_NULL(strstr(help_buf, "config <list|get|set|reset>"));
    /* ... and the preset subcommand is advertised beside it. */
    ASSERT_NOT_NULL(strstr(help_buf, "config preset <list|apply>"));
    PASS();
}

/* The self-update path verifies a downloaded archive against a published
 * checksum. That check is only meaningful if the digest is actually computed —
 * a broken hash command (it once invoked `shasum -a CBM_SZ_256`, an invalid
 * algorithm, from a bad macro rename inside the shell string) makes every
 * digest fail, and the caller then falls through and installs unverified.
 * Guard the digest itself against a known vector. */
extern int cbm_cli_sha256_file(const char *path, char *out, size_t out_size);

/* Hash `content` (len bytes) via a temp file and compare to expected hex.
 * Returns 1 on match, 0 otherwise. */
static int sha256_vector_ok(const void *content, size_t len, const char *expected) {
    char path[512];
    snprintf(path, sizeof(path), "%s/cbm_sha_XXXXXX", cbm_tmpdir());
    int fd = cbm_mkstemp(path);
    if (fd < 0) {
        return 0;
    }
    FILE *fp = fdopen(fd, "wb");
    if (!fp) {
        return 0;
    }
    if (len > 0) {
        fwrite(content, 1, len, fp);
    }
    fclose(fp);

    char digest[128] = {0};
    int rc = cbm_cli_sha256_file(path, digest, sizeof(digest));
    remove(path);
    return rc == 0 && strcmp(digest, expected) == 0;
}

/* NIST FIPS 180-4 SHA-256 test vectors: empty input, a single block ("abc"),
 * and a 56-byte input that forces the length padding into a second block. */
TEST(cli_sha256_file_matches_known_vector) {
    ASSERT_TRUE(sha256_vector_ok(
        "", 0, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
    ASSERT_TRUE(sha256_vector_ok(
        "abc", 3, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
    ASSERT_TRUE(
        sha256_vector_ok("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 56,
                         "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"));
    PASS();
}

TEST(cli_qwen_windows_hook_command_uses_powershell_schema) {
    char command[1024];
    char shell[32];
    ASSERT_EQ(cbm_build_qwen_hook_command_for_testing(
                  "C:\\Program Files\\codebase-memory-mcp.exe", true, command, sizeof(command),
                  shell, sizeof(shell)),
              0);
    ASSERT_STR_EQ(shell, "powershell");
    ASSERT(strstr(command, "& '") != NULL);
    ASSERT(strstr(command, "hook-augment --dialect qwen") != NULL);

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cli-qwen-windows-hook-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");
    char settings[512];
    snprintf(settings, sizeof(settings), "%s/settings.json", tmpdir);
    ASSERT_EQ(cbm_upsert_qwen_lifecycle_hooks_for_testing(
                  settings, "C:\\Program Files\\codebase-memory-mcp.exe", true),
              0);
    char *data = read_test_file_alloc(settings);
    ASSERT_NOT_NULL(data);
    ASSERT(strstr(data, "\"shell\": \"powershell\"") != NULL);
    ASSERT(strstr(data, "\"command_windows\"") == NULL);
    ASSERT(strstr(data, "SessionStart") != NULL);
    ASSERT(strstr(data, "SubagentStart") != NULL);
    ASSERT(strstr(data, "PostToolUse") != NULL);
    ASSERT(strstr(data, "ReadFile") != NULL);
    free(data);
    test_rmdir_r(tmpdir);
    PASS();
}

TEST(cli_windows_optional_hooks_require_a_documented_shell) {
    const char *const withheld[] = {"gitlab", "devin", "factory"};
    for (size_t i = 0U; i < sizeof(withheld) / sizeof(withheld[0]); i++) {
        ASSERT_FALSE(cbm_optional_hook_supported_for_testing(withheld[i], true));
        ASSERT_TRUE(cbm_optional_hook_supported_for_testing(withheld[i], false));
    }
    ASSERT_FALSE(cbm_optional_hook_supported_for_testing("cline", true));
    ASSERT_FALSE(cbm_optional_hook_supported_for_testing("cline", false));
    ASSERT_TRUE(cbm_optional_hook_supported_for_testing("kimi", true));
    ASSERT_TRUE(cbm_optional_hook_supported_for_testing("hermes", true));
    ASSERT_TRUE(cbm_optional_hook_supported_for_testing("qoder", true));
    char command[1024];
    char shell[32];
    ASSERT_EQ(cbm_build_qoder_hook_command_for_testing(
                  "C:\\Program Files\\codebase-memory-mcp.exe", true, command, sizeof(command),
                  shell, sizeof(shell)),
              0);
    ASSERT_STR_EQ(shell, "powershell");
    ASSERT(strstr(command, "hook-augment --dialect qoder") != NULL);
    PASS();
}

TEST(cli_installed_skill_limits_match_server_contract) {
    const cbm_skill_t *installed = cbm_get_skills();
    ASSERT_NOT_NULL(installed);
    ASSERT_NOT_NULL(installed[0].content);
    ASSERT(strstr(installed[0].content, "query_max_output_bytes") != NULL);
    ASSERT(strstr(installed[0].content, "search_limit (50 unless configured)") != NULL);
    ASSERT(strstr(installed[0].content, "100k row ceiling") == NULL);
    ASSERT(strstr(installed[0].content, "200-row cap") == NULL);
    ASSERT(strstr(installed[0].content, "default to 10") == NULL);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 *  Suite definition
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(cli) {
    RUN_TEST(cli_sha256_file_matches_known_vector);
    /* Version (2 tests — selfupdate_test.go) */
    RUN_TEST(cli_compare_versions);
    RUN_TEST(cli_version_get_set);

    /* Shell RC detection (5 tests — install_test.go) */
    RUN_TEST(cli_detect_shell_rc_zsh);
    RUN_TEST(cli_detect_shell_rc_bash);
    RUN_TEST(cli_detect_shell_rc_bash_with_bashrc);
    RUN_TEST(cli_detect_shell_rc_fish);
    RUN_TEST(cli_detect_shell_rc_default);

    /* CLI binary detection (3 tests — install_test.go) */
    RUN_TEST(cli_find_cli_not_found);
    RUN_TEST(cli_find_cli_on_path);
    RUN_TEST(cli_find_cli_fallback_paths);
    RUN_TEST(cli_find_cli_fallback_scans_cargo_bin);

    /* Dry-run flag parsing (1 test — install_test.go) */
    RUN_TEST(cli_dry_run_flags);

    /* Skill management (7 tests — install_test.go) */
    RUN_TEST(cli_skill_creation);
    RUN_TEST(cli_skill_idempotent);
    RUN_TEST(cli_skill_force_overwrite);
#ifndef _WIN32
    RUN_TEST(cli_skills_reject_symlink_and_preserve_unowned_content);
    RUN_TEST(cli_legacy_skill_cleanup_rejects_links_and_user_content);
#endif
    RUN_TEST(cli_uninstall_removes_skills);
    RUN_TEST(cli_remove_old_monolithic_skill);
    RUN_TEST(cli_skill_files_content);
    RUN_TEST(cli_codex_instructions);

    /* Editor MCP: Cursor/Windsurf/Gemini (5 tests — install_test.go) */
    RUN_TEST(cli_editor_mcp_install);
    RUN_TEST(cli_editor_mcp_idempotent);
    RUN_TEST(cli_editor_mcp_preserves_others);
    RUN_TEST(cli_editor_mcp_uninstall);
    RUN_TEST(cli_junie_mcp_install_issue651);
    RUN_TEST(cli_gemini_mcp_install);
    RUN_TEST(cli_openclaw_mcp_install_uses_nested_servers);
    RUN_TEST(cli_openclaw_mcp_preserves_existing_config);
    RUN_TEST(cli_openclaw_mcp_preserves_valid_json5);
    RUN_TEST(cli_openclaw_mcp_uninstall_uses_nested_servers);
    RUN_TEST(cli_openclaw_compaction_preserves_user_owned_section);
    RUN_TEST(cli_openclaw_profile_uses_profile_state_and_default_workspace);
    RUN_TEST(cli_openclaw_uninstall_removes_compaction_when_workspace_is_ambiguous);

    /* VS Code MCP (2 tests — install_test.go) */
    RUN_TEST(cli_vscode_mcp_install);
    RUN_TEST(cli_vscode_mcp_uninstall);
    RUN_TEST(cli_vscode_profile_mcp_uninstall);

    /* Zed MCP (3 tests — install_test.go) */
    RUN_TEST(cli_zed_mcp_install);
    RUN_TEST(cli_zed_mcp_preserves_settings);
    RUN_TEST(cli_zed_mcp_uninstall);
    RUN_TEST(cli_zed_mcp_jsonc_comments);

    /* PATH management (3 tests) */
    RUN_TEST(cli_ensure_path_append);
    RUN_TEST(cli_ensure_path_already_present);
    RUN_TEST(cli_ensure_path_dry_run);
    RUN_TEST(cli_remove_owned_path_block_preserves_user_content);
    RUN_TEST(cli_remove_owned_path_dry_run_preserves_block);
    RUN_TEST(cli_ensure_path_fish_syntax_issue319);

    /* File copy (2 tests — update_test.go) */
    RUN_TEST(cli_copy_file);
    RUN_TEST(cli_copy_file_source_not_found);

    /* Tar.gz extraction (3 tests — update_test.go) */
    RUN_TEST(cli_extract_binary_from_targz);
    RUN_TEST(cli_extract_binary_from_targz_not_found);
    RUN_TEST(cli_extract_binary_from_targz_invalid_data);
    RUN_TEST(cli_extract_binary_from_zip);
    RUN_TEST(cli_extract_binary_from_zip_not_found);
    RUN_TEST(cli_extract_binary_from_zip_path_traversal);
    RUN_TEST(cli_extract_binary_from_zip_invalid);
    RUN_TEST(cli_extract_binary_from_zip_rejects_truncated_deflate_size_over_int_max);

    /* Dry-run lifecycle (2 tests) */
    RUN_TEST(cli_install_dry_run);
    RUN_TEST(cli_uninstall_dry_run);
    RUN_TEST(cli_uninstall_dry_run_preserves_indexes);
    RUN_TEST(cli_uninstall_removes_codex_json_hook_only);
    RUN_TEST(cli_uninstall_removes_owned_claude_hook_scripts);
    RUN_TEST(cli_uninstall_removes_vscode_profile_mcp_only);
    RUN_TEST(cli_install_help_does_not_require_home);
    RUN_TEST(cli_uninstall_help_does_not_require_home);
    RUN_TEST(cli_update_help_does_not_require_home);

    /* Full lifecycle (1 test — cli_test.go) */
    RUN_TEST(cli_install_and_uninstall);
    RUN_TEST(cli_agent_install_reports_safe_editor_refusal);
    RUN_TEST(cli_agent_uninstall_reports_safe_editor_refusal);
    RUN_TEST(cli_special_hook_failures_propagate_from_install_and_uninstall);

    /* Binary swap on install --force (#472) */
    RUN_TEST(cli_install_copies_binary_to_target_issue472);
    RUN_TEST(cli_install_same_file_guard_issue472);
    RUN_TEST(cli_remove_indexes_preserves_config_db);

    /* YAML parser (7 unit tests) */
    RUN_TEST(cli_yaml_parse_simple);
    RUN_TEST(cli_yaml_parse_nested);
    RUN_TEST(cli_yaml_parse_list);
    RUN_TEST(cli_yaml_parse_bool);
    RUN_TEST(cli_yaml_parse_comments);
    RUN_TEST(cli_yaml_parse_empty);
    RUN_TEST(cli_yaml_has);

    /* Agent detection (6 tests — group A) */
    RUN_TEST(cli_detect_agents_finds_claude);
    RUN_TEST(cli_detect_agents_finds_claude_via_env);
    RUN_TEST(cli_detect_agents_finds_codex);
    RUN_TEST(cli_standalone_kilo_install_plan_and_uninstall_preserve_foreign_entries);
    RUN_TEST(cli_json_mcp_migrates_legacy_enabled_true_entry);
    RUN_TEST(cli_detect_agents_finds_cursor_issue222);
    RUN_TEST(cli_install_plan_receipt_no_mutation_issue388);
    RUN_TEST(cli_reference_harnesses_are_planned_without_mutation);
    RUN_TEST(cli_claude_desktop_plan_and_uninstall_preserve_foreign_entries);
    RUN_TEST(cli_reference_harnesses_uninstall_owned_entries_only);
    RUN_TEST(cli_codex_session_hook_issue330);
    RUN_TEST(cli_codex_mcp_and_hook_upserts_are_idempotent);
    RUN_TEST(cli_codex_hook_upsert_rejects_orphan_end_sentinel);
    RUN_TEST(cli_gemini_session_hook_parity);
    RUN_TEST(cli_claude_subagent_hook);
    RUN_TEST(cli_claude_hook_mutation_converges_mixed_owned_duplicates);
    RUN_TEST(cli_claude_subagent_hook_preserves_user_entry);
    RUN_TEST(cli_claude_session_hook_preserves_user_entry);
    RUN_TEST(cli_claude_lifecycle_hooks_delegate_to_augmenter);
    RUN_TEST(cli_copilot_install_preserves_foreign_named_manifest);
    RUN_TEST(cli_copilot_uninstall_preserves_foreign_named_manifest);
    RUN_TEST(cli_copilot_uninstall_preserves_canonical_shaped_foreign_manifest);
    RUN_TEST(cli_vscode_only_installs_copilot_durable_context);
    RUN_TEST(cli_lifecycle_hooks_preserve_foreign_substring_commands);
    RUN_TEST(cli_read_only_agents_do_not_receive_mutating_mcp_server);
    RUN_TEST(cli_junie_foreign_analysis_alias_falls_back_to_parent_handoff);
    RUN_TEST(cli_mcp_installers_preserve_foreign_same_name_entries);
    RUN_TEST(cli_installer_rejects_symlinked_agent_roots);
    RUN_TEST(cli_claude_hook_scripts_shell_quote_binary_path);
    RUN_TEST(cli_claude_hook_commands_shell_quote_custom_config_dir);
    RUN_TEST(cli_codex_migrates_to_single_hook_representation);
    RUN_TEST(cli_hook_augment_context_tracks_search_json_shape);
    RUN_TEST(cli_hook_augment_lifecycle_output_contract);
    RUN_TEST(cli_hook_augment_subagent_tier_router_contract);
    RUN_TEST(cli_hook_augment_subagent_no_project_guidance_is_read_only);
    RUN_TEST(cli_hook_augment_guidance_tracks_tool_and_dependency_config);
    RUN_TEST(cli_hook_augment_post_read_event_and_path_contract);
    RUN_TEST(cli_hook_augment_hermes_dialect_contract);
    RUN_TEST(cli_hook_augment_qoder_lifecycle_contract);
#ifndef _WIN32
    RUN_TEST(cli_qoder_migrates_user_prompt_hook_to_lifecycle_and_read);
#endif
    RUN_TEST(cli_hook_augment_kimi_user_prompt_contract);
    RUN_TEST(cli_hook_augment_devin_lifecycle_contract);
    RUN_TEST(cli_hook_augment_cline_lifecycle_contract);
    RUN_TEST(cli_hook_upsert_rejects_malformed_settings);
    RUN_TEST(cli_hook_upsert_rejects_concurrent_same_event_update);
#ifndef _WIN32
    RUN_TEST(cli_upgrade_migrates_released_claude_hook_scripts);
    RUN_TEST(cli_upgrade_preserves_near_legacy_claude_hook_script);
    RUN_TEST(cli_hook_upsert_rejects_linked_settings);
    RUN_TEST(cli_claude_hook_script_collisions_are_not_registered);
    RUN_TEST(cli_codex_legacy_migration_rejects_linked_config);
#endif
    RUN_TEST(cli_uninstall_removes_claude_hook_scripts);
    RUN_TEST(cli_uninstall_preserves_modified_claude_hook_script);
    RUN_TEST(cli_detect_agents_finds_gemini);
    RUN_TEST(cli_detect_agents_finds_claude_desktop);
    RUN_TEST(cli_detect_agents_finds_zed);
#if !defined(__APPLE__) && !defined(_WIN32)
    RUN_TEST(cli_detect_agents_finds_zed_via_xdg_config_home);
#endif
#ifdef _WIN32
    RUN_TEST(cli_detect_agents_finds_zed_in_roaming_appdata);
#endif
    RUN_TEST(cli_detect_agents_finds_antigravity);
    RUN_TEST(cli_detect_agents_finds_kilocode);
    RUN_TEST(cli_detect_agents_finds_modern_kilo);
    RUN_TEST(cli_detect_agents_finds_kiro);
    RUN_TEST(cli_detect_agents_finds_junie_issue651);
    RUN_TEST(cli_detect_agents_none_found);

    /* Codex MCP config upsert (3 tests — group B) */
    RUN_TEST(cli_upsert_codex_mcp_fresh);
    RUN_TEST(cli_upsert_codex_mcp_escapes_windows_path);
    RUN_TEST(cli_upsert_codex_mcp_existing);
    RUN_TEST(cli_upsert_codex_mcp_replace);
    RUN_TEST(cli_upsert_codex_mcp_preserves_owned_descendant_tool_policy);
    RUN_TEST(cli_codex_legacy_migration_ignores_header_text_in_multiline_string);

    /* Zed MCP format fix (1 test — group B) */
    RUN_TEST(cli_zed_mcp_uses_args_format);
    RUN_TEST(cli_zed_mcp_preserves_jsonc_comments);

    /* OpenCode MCP config upsert (2 tests — group B) */
    RUN_TEST(cli_upsert_opencode_mcp_fresh);
    RUN_TEST(cli_upsert_opencode_mcp_preserves_jsonc_comments);
    RUN_TEST(cli_upsert_opencode_mcp_existing);

    /* Antigravity MCP config upsert (3 tests — group B) */
    RUN_TEST(cli_upsert_antigravity_mcp_fresh);
    RUN_TEST(cli_upsert_antigravity_mcp_replace);
    RUN_TEST(cli_upsert_json_rejects_overlong_path_without_truncated_parent);

    /* Instructions file upsert (8 tests — group C) */
    RUN_TEST(cli_aider_instructions_are_cli_form_issue1032);
    RUN_TEST(cli_upsert_instructions_fresh);
    RUN_TEST(cli_upsert_instructions_rejects_overlong_path_without_truncated_parent);
    RUN_TEST(cli_upsert_instructions_existing);
    RUN_TEST(cli_upsert_instructions_replace);
    RUN_TEST(cli_upsert_instructions_no_duplicate);
    RUN_TEST(cli_remove_instructions);
    RUN_TEST(cli_agent_instructions_content);
    RUN_TEST(cli_qwen_windows_hook_command_uses_powershell_schema);
    RUN_TEST(cli_windows_optional_hooks_require_a_documented_shell);
    RUN_TEST(cli_installed_skill_limits_match_server_contract);

    /* Claude Code hooks (12 tests — group D) */
    RUN_TEST(cli_hook_gate_script_no_predictable_tmp_issue384);
    RUN_TEST(cli_hook_gate_script_rejects_overlong_home_without_truncated_parent);
    RUN_TEST(cli_claude_hooks_reject_overlong_config_dir_command);
    RUN_TEST(cli_hook_scripts_platform_shape_issue929);
#ifdef _WIN32
    RUN_TEST(cli_windows_claude_lifecycle_migrates_only_exact_owned_legacy_state);
    RUN_TEST(cli_windows_claude_hook_scripts_migrate_and_uninstall_all_owned_shapes);
#endif
    RUN_TEST(cli_windows_claude_hook_command_is_shell_portable);
    RUN_TEST(cli_hook_augment_path_is_abs);
    RUN_TEST(cli_hook_augment_deadline_breadcrumb_issue858);
    RUN_TEST(cli_upsert_claude_hook_fresh);
    RUN_TEST(cli_upsert_claude_hook_existing);
    RUN_TEST(cli_tool_hooks_preserve_foreign_same_matcher);
    RUN_TEST(cli_upsert_claude_hook_replace);
    RUN_TEST(cli_upsert_claude_hook_preserves_others);
    RUN_TEST(cli_remove_claude_hooks);
    RUN_TEST(cli_claude_session_hooks_all_lifecycle_matchers);

    /* Gemini CLI hooks (4 tests — group D) */
    RUN_TEST(cli_upsert_gemini_hook_fresh);
    RUN_TEST(cli_upsert_gemini_hook_existing);
    RUN_TEST(cli_upsert_gemini_hook_replace);
    RUN_TEST(cli_remove_gemini_hooks);

    /* Skill directive descriptions (1 test — group E) */
    RUN_TEST(cli_skill_descriptions_directive);

    /* Config store and registry - group F */
    RUN_TEST(cli_config_open_close);
    RUN_TEST(cli_config_get_set);
    RUN_TEST(cli_config_get_bool);
    RUN_TEST(cli_config_get_int);
    RUN_TEST(cli_config_get_effective_env_overrides_db);
    RUN_TEST(cli_config_registry_includes_dep_ranking_toggle);
    RUN_TEST(cli_config_registry_includes_query_max_rows);
    RUN_TEST(cli_config_registry_reindex_startup_guidance_is_precise);
    RUN_TEST(cli_configuration_doc_auto_index_default_matches_registry);
    RUN_TEST(cli_config_delete);
    RUN_TEST(cli_config_persists);
    RUN_TEST(cli_config_presets_apply_exact_capability_sets);
    RUN_TEST(cli_config_command_dispatches_presets);

    /* Replace binary (update command helper — group H) */
#ifndef _WIN32
    RUN_TEST(replace_binary_overwrites_readonly);
    RUN_TEST(replace_binary_creates_new_file);
#endif

    /* CLI tool-argument flags / per-tool --help (#680) */
    RUN_TEST(cli_build_args_json_string_flag_issue680);
    RUN_TEST(cli_build_args_json_integer_flag_issue680);
    RUN_TEST(cli_build_args_json_bare_boolean_issue680);
    RUN_TEST(cli_build_args_json_unknown_flag_rejected);
    RUN_TEST(cli_build_args_json_repeated_array_issue680);
    RUN_TEST(cli_build_args_json_json_array_value);
    RUN_TEST(cli_build_args_json_kebab_to_snake_issue680);
    RUN_TEST(cli_build_args_json_key_equals_value_issue680);
    RUN_TEST(cli_build_args_json_bad_positional_errors_issue680);
    RUN_TEST(cli_print_tool_help_issue680);
    RUN_TEST(cli_main_help_lists_config_preset_subcommand);
}
