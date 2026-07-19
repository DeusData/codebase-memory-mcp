/*
 * test_config_toml_edit.c — Standalone tests for conservative TOML edits.
 *
 * This suite is intentionally not registered in test_main.c.
 */
#define CBM_TOML_EDIT_ENABLE_TEST_API 1
#include "cli/config_toml_edit.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "test_framework.h"
#include "test_helpers.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#endif

#define CTE_PATH_CAP 1024
#define CTE_FILE_CAP 16384

static const char *CTE_BEGIN = "# BEGIN codebase-memory-mcp";
static const char *CTE_END = "# END codebase-memory-mcp";
static const char *CTE_TABLE = "mcp_servers";
static const char *CTE_KEY = "name";
static const char *CTE_IDENTITY = "codebase-memory-mcp";
static const char *CTE_BODY = "name = \"codebase-memory-mcp\"\n"
                              "command = \"codebase-memory-mcp\"\n";

static int cte_fixture(char *dir, size_t dir_size, char *path, size_t path_size) {
    char *created = th_mktempdir("cbm_toml_edit");
    if (!created) {
        return -1;
    }
    int dir_len = snprintf(dir, dir_size, "%s", created);
    int path_len = snprintf(path, path_size, "%s/config.toml", dir);
    if (dir_len < 0 || (size_t)dir_len >= dir_size || path_len < 0 ||
        (size_t)path_len >= path_size) {
        th_cleanup(created);
        return -1;
    }
    return 0;
}

static int cte_read(const char *path, char *output, size_t output_size) {
    if (!path || !output || output_size == 0) {
        return -1;
    }
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return -1;
    }
    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return -1;
    }
    long raw_size = ftell(file);
    if (raw_size < 0 || (size_t)raw_size >= output_size || fseek(file, 0, SEEK_SET) != 0) {
        fclose(file);
        return -1;
    }
    size_t size = (size_t)raw_size;
    size_t read_count = size ? fread(output, 1, size, file) : 0;
    int close_error = fclose(file);
    if (read_count != size || close_error != 0) {
        return -1;
    }
    output[size] = '\0';
    return 0;
}

static int cte_occurrences(const char *text, const char *needle) {
    int count = 0;
    size_t needle_len = strlen(needle);
    const char *cursor = text;
    while ((cursor = strstr(cursor, needle)) != NULL) {
        ++count;
        cursor += needle_len;
    }
    return count;
}

static size_t cte_temp_count(const char *dir) {
    cbm_dir_t *directory = cbm_opendir(dir);
    if (!directory) {
        return SIZE_MAX;
    }
    size_t count = 0;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(directory)) != NULL) {
        if (strncmp(entry->name, "config.toml.", strlen("config.toml.")) == 0) {
            count++;
        }
    }
    cbm_closedir(directory);
    return count;
}

typedef struct {
    const char *content;
    const char *backup_path;
    bool replace_identity;
    int result;
} cte_precommit_change_t;

static void cte_change_before_commit(const char *path, void *context) {
    cte_precommit_change_t *change = context;
    if (change->replace_identity &&
        (!change->backup_path || cbm_rename_replace(path, change->backup_path) != 0)) {
        change->result = -1;
        return;
    }
    change->result = th_write_file(path, change->content);
}

static int cte_assert_unchanged_after_managed_upsert(const char *path, const char *original,
                                                     const char *block) {
    char actual[CTE_FILE_CAP];
    return th_write_file(path, original) == 0 &&
                   cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, block) == -1 &&
                   cte_read(path, actual, sizeof(actual)) == 0 && strcmp(actual, original) == 0
               ? 1
               : 0;
}

static int cte_assert_unchanged_after_vibe_upsert(const char *path, const char *original,
                                                  const char *body) {
    char actual[CTE_FILE_CAP];
    return th_write_file(path, original) == 0 &&
                   cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY,
                                                     body) == -1 &&
                   cte_read(path, actual, sizeof(actual)) == 0 && strcmp(actual, original) == 0
               ? 1
               : 0;
}

TEST(config_toml_rejects_stale_content_and_identity) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char backup[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.toml", dir) > 0);
    ASSERT_EQ(th_write_file(path, "keep = true\n"), 0);

    cte_precommit_change_t content_change = {
        .content = "concurrent = true\n",
        .backup_path = NULL,
        .replace_identity = false,
        .result = -1,
    };
    cbm_toml_set_precommit_hook_for_testing(cte_change_before_commit, &content_change);
    int result = cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n");
    cbm_toml_set_precommit_hook_for_testing(NULL, NULL);
    ASSERT_EQ(content_change.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "concurrent = true\n");
    ASSERT_EQ(cte_temp_count(dir), 0U);

    ASSERT_EQ(th_write_file(path, "keep = true\n"), 0);
    cte_precommit_change_t identity_change = {
        .content = "keep = true\n",
        .backup_path = backup,
        .replace_identity = true,
        .result = -1,
    };
    cbm_toml_set_precommit_hook_for_testing(cte_change_before_commit, &identity_change);
    result = cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n");
    cbm_toml_set_precommit_hook_for_testing(NULL, NULL);
    ASSERT_EQ(identity_change.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "keep = true\n");
    ASSERT_EQ(cte_temp_count(dir), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_missing_target_race_does_not_replace_winner) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    cte_precommit_change_t race = {
        .content = "winner = true\n",
        .backup_path = NULL,
        .replace_identity = false,
        .result = -1,
    };
    cbm_toml_set_prepublish_hook_for_testing(cte_change_before_commit, &race);
    int result = cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n");
    cbm_toml_set_prepublish_hook_for_testing(NULL, NULL);
    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "winner = true\n");
    ASSERT_EQ(cte_temp_count(dir), 0U);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_existing_target_swap_after_check_preserves_winner) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char backup[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *original = "keep = true\n";
    const char *winner = "winner = true\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.toml", dir) > 0);
    ASSERT_EQ(th_write_file(path, original), 0);
    cte_precommit_change_t race = {
        .content = winner,
        .backup_path = backup,
        .replace_identity = true,
        .result = -1,
    };

    cbm_toml_set_prepublish_hook_for_testing(cte_change_before_commit, &race);
    int result = cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n");
    cbm_toml_set_prepublish_hook_for_testing(NULL, NULL);

    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, winner);
    ASSERT_EQ(cte_temp_count(dir), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_rejects_non_regular_path) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(cbm_mkdir(path), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), -1);
    ASSERT_EQ(cte_temp_count(dir), 0U);
    ASSERT_EQ(cbm_rmdir(path), 0);
    th_cleanup(dir);
    PASS();
}

#ifndef _WIN32
TEST(config_toml_rejects_symlink_hardlink_and_preserves_metadata) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char target[CTE_PATH_CAP];
    char alias[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(target, sizeof(target), "%s/target.toml", dir) > 0);
    ASSERT(snprintf(alias, sizeof(alias), "%s/alias.toml", dir) > 0);
    ASSERT_EQ(th_write_file(target, "target = true\n"), 0);
    ASSERT_EQ(symlink(target, path), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), -1);
    struct stat link_state;
    ASSERT_EQ(lstat(path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    ASSERT_EQ(cte_read(target, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "target = true\n");
    ASSERT_EQ(cbm_unlink(path), 0);

    ASSERT_EQ(th_write_file(path, "shared = true\n"), 0);
    ASSERT_EQ(link(path, alias), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), -1);
    ASSERT_EQ(cte_read(alias, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "shared = true\n");
    ASSERT_EQ(cbm_unlink(alias), 0);

    ASSERT_EQ(chmod(path, 04755), 0);
    struct stat privileged;
    ASSERT_EQ(stat(path, &privileged), 0);
    /* Some sandboxed filesystems silently clear set-id bits even when chmod
     * reports success. Exercise the rejection contract only when the fixture
     * can actually retain the privileged bit. */
    if ((privileged.st_mode & S_ISUID) != 0) {
        ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), -1);
    }
    ASSERT_EQ(chmod(path, 0640), 0);
    struct stat before;
    ASSERT_EQ(stat(path, &before), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), 0);
    struct stat after;
    ASSERT_EQ(stat(path, &after), 0);
    ASSERT_EQ(after.st_uid, before.st_uid);
    ASSERT_EQ(after.st_gid, before.st_gid);
    ASSERT_EQ(after.st_mode & 07777, before.st_mode & 07777);
    ASSERT_EQ(cbm_unlink(target), 0);
    th_cleanup(dir);
    PASS();
}
#endif

TEST(config_toml_managed_markers_ignore_multiline_strings) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *original = "basic = \"\"\"\n"
                           "# BEGIN codebase-memory-mcp\n"
                           "# END codebase-memory-mcp\n"
                           "\"\"\"\n"
                           "literal = '''\n"
                           "# BEGIN codebase-memory-mcp\n"
                           "# END codebase-memory-mcp\n"
                           "'''\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, original), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_EQ(cte_occurrences(actual, CTE_BEGIN), 3);
    ASSERT_EQ(cte_occurrences(actual, CTE_END), 3);
    ASSERT_NOT_NULL(strstr(actual, "owned = true"));
    ASSERT_EQ(cbm_toml_remove_managed_block(path, CTE_BEGIN, CTE_END), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, original);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_managed_rejects_marker_in_block_and_unclosed_multiline) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "keep = true\n"), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END,
                                            "value = true\n# END codebase-memory-mcp\n"),
              -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "keep = true\n");

    const char *unclosed = "description = \"\"\"\n# BEGIN codebase-memory-mcp\n";
    ASSERT_EQ(th_write_file(path, unclosed), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, unclosed);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_codex_semantic_conflicts_fail_closed) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char *codex_block = "[mcp_servers.codebase-memory-mcp]\ncommand = \"new\"\n";
    static const char *conflicts[] = {
        "[mcp_servers.\"codebase-memory-mcp\"]\ncommand = \"old\"\n",
        "[\"mcp_servers\".'codebase-memory-mcp']\ncommand = \"old\"\n",
        "mcp_servers.\"codebase-memory-mcp\".command = \"old\"\n",
        "[mcp_servers]\n\"codebase-memory-mcp\".command = \"old\"\n",
        ("[mcp_servers.codebase-memory-mcp]\ncommand = \"one\"\n"
         "[mcp_servers.\"codebase-memory-mcp\"]\ncommand = \"two\"\n"),
    };
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    for (size_t i = 0; i < sizeof(conflicts) / sizeof(conflicts[0]); ++i) {
        ASSERT(cte_assert_unchanged_after_managed_upsert(path, conflicts[i], codex_block));
    }
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_body_validation_fail_closed) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    const char *original = "keep = true\n";
    static const char *invalid_bodies[] = {
        "command = \"missing identity\"\n",
        "name = \"wrong\"\ncommand = \"x\"\n",
        "name = \"codebase-memory-mcp\"\nname = \"codebase-memory-mcp\"\n",
        "name = \"codebase-memory-mcp\"\n[evil]\npwned = true\n",
        "name = \"codebase-memory-mcp\"\n[[evil]]\npwned = true\n",
        "name = \"codebase-memory-mcp\"\ncommand.value = \"x\"\n",
        "name = \"codebase-memory-mcp\"\ndescription = \"\"\"ambiguous\"\"\"\n",
        "name = \"codebase-memory-mcp\"\nthis is not an assignment\n",
    };
    for (size_t i = 0; i < sizeof(invalid_bodies) / sizeof(invalid_bodies[0]); ++i) {
        ASSERT(cte_assert_unchanged_after_vibe_upsert(path, original, invalid_bodies[i]));
    }
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_target_table_rejects_significant_nonassignments_byte_identically) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    static const char *malformed[] = {
        "[[mcp_servers]]\n"
        "name = \"codebase-memory-mcp\"\n"
        "this is not an assignment\n"
        "command = \"winner\"\n",
        "[[mcp_servers]]\n"
        "name = \"codebase-memory-mcp\"\n"
        "command \"missing equals\"\n",
        "[[mcp_servers]]\n"
        "name = \"codebase-memory-mcp\"\n"
        "@invalid\n",
    };
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    for (size_t i = 0U; i < sizeof(malformed) / sizeof(malformed[0]); ++i) {
        ASSERT_EQ(th_write_file(path, malformed[i]), 0);
        ASSERT_EQ(
            cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, CTE_BODY),
            -1);
        ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
        ASSERT_STR_EQ(actual, malformed[i]);

        ASSERT_EQ(cbm_toml_remove_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY), -1);
        ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
        ASSERT_STR_EQ(actual, malformed[i]);
    }

    const char *malformed_regular = "[mcp_servers.codebase-memory-mcp]\n"
                                    "command = \"winner\"\n"
                                    "this is not an assignment\n"
                                    "[unrelated]\n"
                                    "keep = true\n";
    ASSERT_EQ(th_write_file(path, malformed_regular), 0);
    ASSERT_EQ(
        cbm_toml_remove_legacy_table(path, "mcp_servers.codebase-memory-mcp", CTE_BEGIN, CTE_END),
        -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, malformed_regular);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_legacy_remove_reports_foreign_table_without_mutation) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *foreign = "theme = \"dark\"\n"
                          "[mcp_servers.codebase-memory-mcp]\n"
                          "command = \"/opt/user-tool\"\n"
                          "args = []\n"
                          "user_field = true\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, foreign), 0);
    ASSERT_EQ(
        cbm_toml_remove_legacy_table(path, "mcp_servers.codebase-memory-mcp", CTE_BEGIN, CTE_END),
        1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, foreign);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_non_array_and_dotted_conflicts_fail_closed) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    static const char *conflicts[] = {
        "[mcp_servers]\nenabled = true\n",
        "[\"mcp_servers\"]\nenabled = true\n",
        "mcp_servers = []\n",
        "mcp_servers.child = {}\n",
        "[mcp_servers.child]\nenabled = true\n",
        "[[mcp_servers.child]]\nname = \"nested\"\n",
    };
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    for (size_t i = 0; i < sizeof(conflicts) / sizeof(conflicts[0]); ++i) {
        ASSERT(cte_assert_unchanged_after_vibe_upsert(path, conflicts[i], CTE_BODY));
    }
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_remove_includes_descendant_tables) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "[[mcp_servers]]\n"
                                  "name = \"codebase-memory-mcp\"\n"
                                  "command = \"owned\"\n"
                                  "[mcp_servers.environment]\n"
                                  "TOKEN = \"owned\"\n"
                                  "[[mcp_servers.children]]\n"
                                  "value = \"owned-child\"\n\n"
                                  "[[mcp_servers]]\n"
                                  "name = \"other\"\n"
                                  "command = \"keep\"\n"),
              0);
    ASSERT_EQ(cbm_toml_remove_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_NULL(strstr(actual, "TOKEN"));
    ASSERT_NULL(strstr(actual, "owned-child"));
    ASSERT_NOT_NULL(strstr(actual, "name = \"other\""));
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_reinstall_preserves_user_fields_and_descendants) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *desired = "name = \"codebase-memory-mcp\"\n"
                          "transport = \"stdio\"\n"
                          "command = \"new\"\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "[[mcp_servers]]\n"
                                  "name = \"codebase-memory-mcp\" # identity comment\n"
                                  "command = \"old\"\n"
                                  "timeout = 45 # user field\n"
                                  "args = [\"--user\"]\n"
                                  "[mcp_servers.environment]\n"
                                  "KEEP = \"yes\"\n"),
              0);
    ASSERT_EQ(cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, desired),
              0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_NOT_NULL(strstr(actual, "command = \"new\""));
    ASSERT_NULL(strstr(actual, "command = \"old\""));
    ASSERT_NOT_NULL(strstr(actual, "transport = \"stdio\""));
    ASSERT_NOT_NULL(strstr(actual, "timeout = 45 # user field"));
    ASSERT_NOT_NULL(strstr(actual, "args = [\"--user\"]"));
    ASSERT_NOT_NULL(strstr(actual, "[mcp_servers.environment]"));
    ASSERT_NOT_NULL(strstr(actual, "KEEP = \"yes\""));
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_preserves_bom_crlf_and_handles_no_final_newline) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "\xEF\xBB\xBFkeep = true\r\n"), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT(memcmp(actual, "\xEF\xBB\xBF", 3U) == 0);
    ASSERT_NOT_NULL(strstr(actual, "keep = true\r\n"));
    ASSERT_NOT_NULL(strstr(actual, "# BEGIN codebase-memory-mcp\r\nowned = true\r\n"));
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_EQ(cte_occurrences(actual, CTE_BEGIN), 1);
    ASSERT_EQ(cbm_toml_remove_managed_block(path, CTE_BEGIN, CTE_END), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "\xEF\xBB\xBFkeep = true\r\n");

    ASSERT_EQ(th_write_file(path, "\xEF\xBB\xBF# BEGIN codebase-memory-mcp\r\n"
                                  "old = true\r\n"
                                  "# END codebase-memory-mcp\r\n"),
              0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "new = true\n"), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT(memcmp(actual, "\xEF\xBB\xBF# BEGIN", 10U) == 0);
    ASSERT_EQ(cte_occurrences(actual, CTE_BEGIN), 1);
    ASSERT_NOT_NULL(strstr(actual, "new = true\r\n"));
    ASSERT_EQ(cbm_toml_remove_managed_block(path, CTE_BEGIN, CTE_END), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "\xEF\xBB\xBF");

    ASSERT_EQ(th_write_file(path, "\xEF\xBB\xBF[[\"mcp_servers\"]]\r\n"
                                  "name = \"codebase-memory-mcp\"\r\n"
                                  "command = \"old\""),
              0);
    ASSERT_EQ(cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, CTE_BODY),
              0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT(memcmp(actual, "\xEF\xBB\xBF", 3U) == 0);
    ASSERT_NOT_NULL(strstr(actual, "command = \"codebase-memory-mcp\"\r\n"));

    ASSERT_EQ(th_write_file(path, "keep = true"), 0);
    ASSERT_EQ(cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, CTE_BODY),
              0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_NOT_NULL(strstr(actual, "keep = true\n\n[[mcp_servers]]\n"));
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_rejects_oversized_input_file) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    FILE *file = cbm_fopen(path, "wb");
    ASSERT_NOT_NULL(file);
    char chunk[4096];
    memset(chunk, '#', sizeof(chunk));
    for (size_t i = 0; i < (16U * 1024U * 1024U) / sizeof(chunk) + 1U; ++i) {
        ASSERT_EQ(fwrite(chunk, 1U, sizeof(chunk), file), sizeof(chunk));
    }
    ASSERT_EQ(fclose(file), 0);
    ASSERT_EQ(cbm_toml_remove_managed_block(path, CTE_BEGIN, CTE_END), -1);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_escape_windows_path_quotes) {
    char escaped[256];
    ASSERT_EQ(
        cbm_toml_escape_basic_string("C:\\Users\\Jane\\tool \"quoted\"", escaped, sizeof(escaped)),
        0);
    ASSERT_STR_EQ(escaped, "C:\\\\Users\\\\Jane\\\\tool \\\"quoted\\\"");
    PASS();
}

TEST(config_toml_escape_newlines_controls) {
    char escaped[256];
    ASSERT_EQ(cbm_toml_escape_basic_string("line1\nline2\t\r\b\f\x01", escaped, sizeof(escaped)),
              0);
    ASSERT_STR_EQ(escaped, "line1\\nline2\\t\\r\\b\\f\\u0001");

    char too_small[4] = "bad";
    ASSERT_EQ(cbm_toml_escape_basic_string("overflow", too_small, sizeof(too_small)), -1);
    ASSERT_STR_EQ(too_small, "");
    ASSERT_EQ(cbm_toml_escape_basic_string(NULL, escaped, sizeof(escaped)), -1);
    ASSERT_EQ(cbm_toml_escape_basic_string("value", NULL, 0), -1);
    char invalid_utf8[] = {(char)0xff, '\0'};
    ASSERT_EQ(cbm_toml_escape_basic_string(invalid_utf8, escaped, sizeof(escaped)), -1);
    ASSERT_STR_EQ(escaped, "");
    PASS();
}

TEST(config_toml_managed_fresh_missing_file) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);

    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "enabled = true\n"), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "# BEGIN codebase-memory-mcp\n"
                          "enabled = true\n"
                          "# END codebase-memory-mcp\n");
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_managed_replace_preserves_unrelated) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "title = \"keep\"\n"
                                  "# BEGIN codebase-memory-mcp\n"
                                  "old = true\n"
                                  "# END codebase-memory-mcp\n"
                                  "tail = \"keep\"\n"),
              0);

    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "new = \"value\""), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "title = \"keep\"\n"
                          "# BEGIN codebase-memory-mcp\n"
                          "new = \"value\"\n"
                          "# END codebase-memory-mcp\n"
                          "tail = \"keep\"\n");
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_managed_remove) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "before = true\n"
                                  "# BEGIN codebase-memory-mcp\n"
                                  "owned = true\n"
                                  "# END codebase-memory-mcp\n"
                                  "after = true\n"),
              0);

    ASSERT_EQ(cbm_toml_remove_managed_block(path, CTE_BEGIN, CTE_END), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "before = true\nafter = true\n");
    ASSERT_EQ(cbm_toml_remove_managed_block(path, CTE_BEGIN, CTE_END), 0);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_managed_idempotent) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char first[CTE_FILE_CAP];
    char second[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "keep = true\n"), 0);

    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), 0);
    ASSERT_EQ(cte_read(path, first, sizeof(first)), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned = true\n"), 0);
    ASSERT_EQ(cte_read(path, second, sizeof(second)), 0);
    ASSERT_STR_EQ(first, second);
    ASSERT_EQ(cte_occurrences(second, CTE_BEGIN), 1);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_managed_unbalanced_duplicate_fail_closed) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *unbalanced = "keep = true\n"
                             "# BEGIN codebase-memory-mcp\n"
                             "owned = true\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, unbalanced), 0);
    ASSERT_EQ(cbm_toml_upsert_managed_block(path, CTE_BEGIN, CTE_END, "new = true\n"), -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, unbalanced);

    const char *duplicate = "# BEGIN codebase-memory-mcp\n"
                            "one = true\n"
                            "# END codebase-memory-mcp\n"
                            "# BEGIN codebase-memory-mcp\n"
                            "two = true\n"
                            "# END codebase-memory-mcp\n";
    ASSERT_EQ(th_write_file(path, duplicate), 0);
    ASSERT_EQ(cbm_toml_remove_managed_block(path, CTE_BEGIN, CTE_END), -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, duplicate);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_insert_among_other_tables) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *original = "# global comment\n"
                           "[settings]\n"
                           "theme = \"dark\"\n\n"
                           "[[mcp_servers]]\n"
                           "name = \"other-server\"\n"
                           "command = \"other\"\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, original), 0);

    ASSERT_EQ(cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, CTE_BODY),
              0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "# global comment\n"
                          "[settings]\n"
                          "theme = \"dark\"\n\n"
                          "[[mcp_servers]]\n"
                          "name = \"other-server\"\n"
                          "command = \"other\"\n\n"
                          "[[mcp_servers]]\n"
                          "name = \"codebase-memory-mcp\"\n"
                          "command = \"codebase-memory-mcp\"\n");
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_replace_target_preserves_comments_tables) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *replacement = "name = \"codebase-memory-mcp\"\n"
                              "command = \"/new/path\"\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "# keep top\n"
                                  "[[mcp_servers]]\n"
                                  "name = \"other-server\"\n"
                                  "command = \"other\"\n\n"
                                  "# keep target preface\n"
                                  "[[mcp_servers]]\n"
                                  "name = \"codebase-memory-mcp\" # owned\n"
                                  "command = \"old\"\n"
                                  "args = [\"--old\"]\n\n"
                                  "# keep after target\n"
                                  "[ui]\n"
                                  "enabled = true\n"),
              0);

    ASSERT_EQ(
        cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, replacement), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_NOT_NULL(strstr(actual, "# keep top"));
    ASSERT_NOT_NULL(strstr(actual, "# keep target preface"));
    ASSERT_NOT_NULL(strstr(actual, "# keep after target"));
    ASSERT_NOT_NULL(strstr(actual, "name = \"other-server\""));
    ASSERT_NOT_NULL(strstr(actual, "command = \"/new/path\""));
    ASSERT_NULL(strstr(actual, "command = \"old\""));
    ASSERT_NOT_NULL(strstr(actual, "--old"));
    ASSERT_EQ(cte_occurrences(actual, "[[mcp_servers]]"), 2);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_owned_table_installs_idempotently_and_removes_exact_state) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *canonical = "name = \"codebase-memory-mcp\"\n"
                            "transport = \"stdio\"\n"
                            "command = \"/opt/codebase-memory-mcp\"\n"
                            "args = []\n";
    const char *installed = "[[mcp_servers]]\n"
                            "name = \"codebase-memory-mcp\"\n"
                            "transport = \"stdio\"\n"
                            "command = \"/opt/codebase-memory-mcp\"\n"
                            "args = []\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);

    ASSERT_EQ(
        cbm_toml_upsert_owned_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, canonical),
        0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, installed);

    ASSERT_EQ(
        cbm_toml_upsert_owned_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, canonical),
        0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, installed);

    ASSERT_EQ(
        cbm_toml_remove_owned_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, canonical),
        0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "");
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_owned_table_preserves_foreign_same_name_state) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *canonical = "name = \"codebase-memory-mcp\"\n"
                            "transport = \"stdio\"\n"
                            "command = \"/opt/codebase-memory-mcp\"\n"
                            "args = []\n";
    const char *foreign_cases[] = {
        "# user-owned Vibe server\n"
        "[[mcp_servers]]\n"
        "name = \"codebase-memory-mcp\"\n"
        "transport = \"stdio\"\n"
        "command = \"/opt/user-owned-mcp\"\n"
        "args = [\"--custom\"]\n",
        "[[mcp_servers]]\n"
        "name = \"codebase-memory-mcp\"\n"
        "transport = \"stdio\"\n"
        "command = \"/opt/codebase-memory-mcp\"\n"
        "args = []\n"
        "startup_timeout_sec = 45\n",
    };
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);

    for (size_t i = 0U; i < sizeof(foreign_cases) / sizeof(foreign_cases[0]); i++) {
        ASSERT_EQ(th_write_file(path, foreign_cases[i]), 0);
        ASSERT_EQ(cbm_toml_upsert_owned_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY,
                                                          canonical),
                  CBM_TOML_OWNED_EDIT_FOREIGN);
        ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
        ASSERT_STR_EQ(actual, foreign_cases[i]);

        ASSERT_EQ(cbm_toml_remove_owned_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY,
                                                          canonical),
                  CBM_TOML_OWNED_EDIT_FOREIGN);
        ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
        ASSERT_STR_EQ(actual, foreign_cases[i]);
    }
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_remove_first_target) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "[[mcp_servers]]\n"
                                  "name = \"codebase-memory-mcp\"\n"
                                  "command = \"owned\"\n\n"
                                  "[[mcp_servers]]\n"
                                  "name = \"other\"\n"
                                  "command = \"keep\"\n"),
              0);
    ASSERT_EQ(cbm_toml_remove_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_NULL(strstr(actual, "command = \"owned\""));
    ASSERT_NOT_NULL(strstr(actual, "name = \"other\""));
    ASSERT_EQ(cte_occurrences(actual, "[[mcp_servers]]"), 1);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_remove_middle_target) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "[[mcp_servers]]\n"
                                  "name = \"first\"\n\n"
                                  "[[mcp_servers]]\n"
                                  "name = \"codebase-memory-mcp\"\n"
                                  "command = \"owned\"\n\n"
                                  "[[mcp_servers]]\n"
                                  "name = \"last\"\n"),
              0);
    ASSERT_EQ(cbm_toml_remove_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_NOT_NULL(strstr(actual, "name = \"first\""));
    ASSERT_NOT_NULL(strstr(actual, "name = \"last\""));
    ASSERT_NULL(strstr(actual, CTE_IDENTITY));
    ASSERT_EQ(cte_occurrences(actual, "[[mcp_servers]]"), 2);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_remove_last_target) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "[[mcp_servers]]\n"
                                  "name = \"other\"\n\n"
                                  "[[mcp_servers]]\n"
                                  "name = \"codebase-memory-mcp\"\n"),
              0);
    ASSERT_EQ(cbm_toml_remove_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_NOT_NULL(strstr(actual, "name = \"other\""));
    ASSERT_NULL(strstr(actual, CTE_IDENTITY));
    ASSERT_EQ(cte_occurrences(actual, "[[mcp_servers]]"), 1);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_remove_only_target) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "[[mcp_servers]]\n"
                                  "name = \"codebase-memory-mcp\"\n"
                                  "command = \"owned\"\n"),
              0);
    ASSERT_EQ(cbm_toml_remove_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "");
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_literal_and_basic_identity) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "[[mcp_servers]]\n"
                                  "name = 'codebase-memory-mcp' # literal\n"
                                  "command = 'owned'\n"),
              0);
    ASSERT_EQ(cbm_toml_remove_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY), 0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "");

    ASSERT_EQ(th_write_file(path, "[[mcp_servers]]\n"
                                  "name = \"codebase-memory-\\u006dcp\" # basic\n"
                                  "command = \"old\"\n"),
              0);
    ASSERT_EQ(cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, CTE_BODY),
              0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, "[[mcp_servers]]\n"
                          "name = \"codebase-memory-mcp\"\n"
                          "command = \"codebase-memory-mcp\"\n");
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_duplicate_target_fail_closed) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *duplicate = "# preserve exactly\n"
                            "[[mcp_servers]]\n"
                            "name = \"codebase-memory-mcp\"\n"
                            "command = \"first\"\n\n"
                            "[[mcp_servers]]\n"
                            "name = 'codebase-memory-mcp'\n"
                            "command = 'second'\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, duplicate), 0);
    ASSERT_EQ(cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, CTE_BODY),
              -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, duplicate);
    th_cleanup(dir);
    PASS();
}

TEST(config_toml_vibe_ambiguous_target_fail_closed) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char actual[CTE_FILE_CAP];
    const char *ambiguous = "[[mcp_servers]]\n"
                            "command = \"missing-name\"\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, ambiguous), 0);
    ASSERT_EQ(cbm_toml_remove_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY), -1);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_STR_EQ(actual, ambiguous);

    const char *multiline = "[[mcp_servers]]\n"
                            "description = \"\"\"\n"
                            "name = \"codebase-memory-mcp\"\n"
                            "\"\"\"\n"
                            "name = \"other\"\n";
    ASSERT_EQ(th_write_file(path, multiline), 0);
    ASSERT_EQ(cbm_toml_upsert_named_array_table(path, CTE_TABLE, CTE_KEY, CTE_IDENTITY, CTE_BODY),
              0);
    ASSERT_EQ(cte_read(path, actual, sizeof(actual)), 0);
    ASSERT_NOT_NULL(strstr(actual, "description = \"\"\""));
    ASSERT_NOT_NULL(strstr(actual, "name = \"other\""));
    ASSERT_NOT_NULL(strstr(actual, "name = \"codebase-memory-mcp\""));
    th_cleanup(dir);
    PASS();
}

SUITE(config_toml_edit) {
    RUN_TEST(config_toml_rejects_stale_content_and_identity);
    RUN_TEST(config_toml_missing_target_race_does_not_replace_winner);
    RUN_TEST(config_toml_existing_target_swap_after_check_preserves_winner);
    RUN_TEST(config_toml_rejects_non_regular_path);
#ifndef _WIN32
    RUN_TEST(config_toml_rejects_symlink_hardlink_and_preserves_metadata);
#endif
    RUN_TEST(config_toml_managed_markers_ignore_multiline_strings);
    RUN_TEST(config_toml_managed_rejects_marker_in_block_and_unclosed_multiline);
    RUN_TEST(config_toml_codex_semantic_conflicts_fail_closed);
    RUN_TEST(config_toml_vibe_body_validation_fail_closed);
    RUN_TEST(config_toml_vibe_non_array_and_dotted_conflicts_fail_closed);
    RUN_TEST(config_toml_vibe_remove_includes_descendant_tables);
    RUN_TEST(config_toml_vibe_reinstall_preserves_user_fields_and_descendants);
    RUN_TEST(config_toml_preserves_bom_crlf_and_handles_no_final_newline);
    RUN_TEST(config_toml_rejects_oversized_input_file);
    RUN_TEST(config_toml_escape_windows_path_quotes);
    RUN_TEST(config_toml_escape_newlines_controls);
    RUN_TEST(config_toml_managed_fresh_missing_file);
    RUN_TEST(config_toml_managed_replace_preserves_unrelated);
    RUN_TEST(config_toml_managed_remove);
    RUN_TEST(config_toml_managed_idempotent);
    RUN_TEST(config_toml_managed_unbalanced_duplicate_fail_closed);
    RUN_TEST(config_toml_vibe_insert_among_other_tables);
    RUN_TEST(config_toml_vibe_replace_target_preserves_comments_tables);
    RUN_TEST(config_toml_vibe_owned_table_installs_idempotently_and_removes_exact_state);
    RUN_TEST(config_toml_vibe_owned_table_preserves_foreign_same_name_state);
    RUN_TEST(config_toml_vibe_remove_first_target);
    RUN_TEST(config_toml_vibe_remove_middle_target);
    RUN_TEST(config_toml_vibe_remove_last_target);
    RUN_TEST(config_toml_vibe_remove_only_target);
    RUN_TEST(config_toml_vibe_literal_and_basic_identity);
    RUN_TEST(config_toml_vibe_duplicate_target_fail_closed);
    RUN_TEST(config_toml_vibe_ambiguous_target_fail_closed);
    RUN_TEST(config_toml_target_table_rejects_significant_nonassignments_byte_identically);
    RUN_TEST(config_toml_legacy_remove_reports_foreign_table_without_mutation);
}
