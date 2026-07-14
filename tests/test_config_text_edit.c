/*
 * test_config_text_edit.c — Hardened managed-text editor contracts.
 */
#include "../src/cli/config_text_edit.h"
#include "test_framework.h"
#include "test_helpers.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

#define CTE_BEGIN "<!-- codebase-memory-mcp:start -->"
#define CTE_END "<!-- codebase-memory-mcp:end -->"
#define CTE_LIMIT (16U * 1024U * 1024U)
#define CTE_PATH_CAP 1024U

/* Expected test/public contracts for the post-verification race fix. Keeping
 * these declarations here makes this RED test file independent of production
 * header edits while the implementation is developed. */
void cbm_text_set_prepublish_hook_for_testing(cbm_text_precommit_test_hook_t hook, void *context);
void cbm_text_set_temp_closed_hook_for_testing(cbm_text_precommit_test_hook_t hook, void *context);
int cbm_text_write_owned_document_if_unchanged(const char *file_path, const char *owned_content,
                                               const char *expected_content,
                                               size_t expected_length);

static int cte_fixture(char *dir, size_t dir_size, char *path, size_t path_size) {
    char *created = th_mktempdir("cbm_text_edit");
    if (!created) {
        return -1;
    }
    int dir_count = snprintf(dir, dir_size, "%s", created);
    int path_count = snprintf(path, path_size, "%s/AGENTS.md", created);
    if (dir_count < 0 || (size_t)dir_count >= dir_size || path_count < 0 ||
        (size_t)path_count >= path_size) {
        th_cleanup(created);
        return -1;
    }
    return 0;
}

static int cte_write_bytes(const char *path, const char *data, size_t len) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file) {
        return -1;
    }
    int result = len == 0U || fwrite(data, 1U, len, file) == len ? 0 : -1;
    if (fclose(file) != 0) {
        result = -1;
    }
    return result;
}

static char *cte_read_bytes(const char *path, size_t *len_out) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file || fseek(file, 0L, SEEK_END) != 0) {
        if (file) {
            fclose(file);
        }
        return NULL;
    }
    long raw_len = ftell(file);
    if (raw_len < 0 || fseek(file, 0L, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }
    size_t len = (size_t)raw_len;
    char *data = (char *)malloc(len + 1U);
    if (!data) {
        fclose(file);
        return NULL;
    }
    size_t count = len == 0U ? 0U : fread(data, 1U, len, file);
    int failed = ferror(file) || fclose(file) != 0 || count != len;
    if (failed) {
        free(data);
        return NULL;
    }
    data[len] = '\0';
    *len_out = len;
    return data;
}

static int cte_assert_bytes(const char *path, const char *expected, size_t expected_len) {
    size_t actual_len = 0U;
    char *actual = cte_read_bytes(path, &actual_len);
    int matches = actual && actual_len == expected_len &&
                  (actual_len == 0U || memcmp(actual, expected, actual_len) == 0);
    free(actual);
    return matches;
}

static int cte_path_exists(const char *path) {
    struct stat state;
    return stat(path, &state) == 0;
}

static size_t cte_temp_count(const char *dir) {
    cbm_dir_t *directory = cbm_opendir(dir);
    if (!directory) {
        return SIZE_MAX;
    }
    size_t count = 0U;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(directory)) != NULL) {
        if (strstr(entry->name, ".cbm-text-") != NULL) {
            count++;
        }
    }
    cbm_closedir(directory);
    return count;
}

typedef struct {
    const char *content;
    const char *backup_path;
    int replace_identity;
    int result;
} cte_precommit_change_t;

static void cte_change_before_commit(const char *path, void *context) {
    cte_precommit_change_t *change = (cte_precommit_change_t *)context;
    if (change->replace_identity &&
        (!change->backup_path || cbm_rename_replace(path, change->backup_path) != 0)) {
        change->result = -1;
        return;
    }
    change->result = th_write_file(path, change->content);
}

#ifndef _WIN32
static void cte_replace_closed_temp(const char *path, void *context) {
    cte_precommit_change_t *change = (cte_precommit_change_t *)context;
    if (!change->backup_path || cbm_rename_replace(path, change->backup_path) != 0) {
        change->result = -1;
        return;
    }
    change->result = th_write_file(path, change->content);
    if (change->result == 0 && chmod(path, 0644) != 0) {
        change->result = -1;
    }
}
#endif

TEST(config_text_managed_insert_preserves_bom_comments_and_is_idempotent) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char original[] = "\xEF\xBB\xBF# User heading\n<!-- arbitrary comment -->\n";
    const char expected[] = "\xEF\xBB\xBF# User heading\n<!-- arbitrary comment -->\n" CTE_BEGIN
                            "\nUse search_graph first.\n" CTE_END "\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(cte_write_bytes(path, original, sizeof(original) - 1U), 0);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "Use search_graph first.\n"),
              0);
    ASSERT(cte_assert_bytes(path, expected, sizeof(expected) - 1U));
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "Use search_graph first.\n"),
              0);
    ASSERT(cte_assert_bytes(path, expected, sizeof(expected) - 1U));
    ASSERT_EQ(cte_temp_count(dir), 0U);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_managed_replace_preserves_crlf_surroundings) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char original[] = "before\r\n" CTE_BEGIN "\r\nold\r\n" CTE_END "\r\nafter\r\n";
    const char expected[] =
        "before\r\n" CTE_BEGIN "\r\nnew one\r\nnew two\r\n" CTE_END "\r\nafter\r\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(cte_write_bytes(path, original, sizeof(original) - 1U), 0);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "new one\nnew two"), 0);
    ASSERT(cte_assert_bytes(path, expected, sizeof(expected) - 1U));
    th_cleanup(dir);
    PASS();
}

TEST(config_text_managed_no_final_newline_round_trip) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char original[] = "user text";
    const char installed[] = "user text\n" CTE_BEGIN "\nowned\n" CTE_END;
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(cte_write_bytes(path, original, sizeof(original) - 1U), 0);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned"), 0);
    ASSERT(cte_assert_bytes(path, installed, sizeof(installed) - 1U));
    ASSERT_EQ(cbm_text_remove_managed_block(path, CTE_BEGIN, CTE_END), 0);
    ASSERT(cte_assert_bytes(path, original, sizeof(original) - 1U));
    th_cleanup(dir);
    PASS();
}

TEST(config_text_managed_remove_preserves_user_bytes) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char original[] = "alpha\n" CTE_BEGIN "\nowned\n" CTE_END "\nomega\n";
    const char expected[] = "alpha\nomega\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(cte_write_bytes(path, original, sizeof(original) - 1U), 0);
    ASSERT_EQ(cbm_text_remove_managed_block(path, CTE_BEGIN, CTE_END), 0);
    ASSERT(cte_assert_bytes(path, expected, sizeof(expected) - 1U));
    ASSERT_EQ(cbm_text_remove_managed_block(path, CTE_BEGIN, CTE_END), 0);
    ASSERT(cte_assert_bytes(path, expected, sizeof(expected) - 1U));
    th_cleanup(dir);
    PASS();
}

TEST(config_text_managed_malformed_markers_fail_closed) {
    static const char *cases[] = {
        CTE_BEGIN "\nowned\n",
        CTE_END "\n",
        CTE_BEGIN "\n" CTE_BEGIN "\n" CTE_END "\n" CTE_END "\n",
        CTE_BEGIN "\n" CTE_END "\n" CTE_BEGIN "\n" CTE_END "\n",
        "prefix " CTE_BEGIN " suffix\n",
        CTE_BEGIN "\ninside " CTE_END " text\n" CTE_END "\n",
    };
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        size_t len = strlen(cases[i]);
        ASSERT_EQ(cte_write_bytes(path, cases[i], len), 0);
        ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "new"), -1);
        ASSERT(cte_assert_bytes(path, cases[i], len));
        ASSERT_EQ(cbm_text_remove_managed_block(path, CTE_BEGIN, CTE_END), -1);
        ASSERT(cte_assert_bytes(path, cases[i], len));
    }
    th_cleanup(dir);
    PASS();
}

TEST(config_text_managed_rejects_unsafe_markers_and_owned_content) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, "keep\n"), 0);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "nested " CTE_BEGIN), -1);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "nested " CTE_END), -1);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, "same", "same", "owned"), -1);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, "bad\nmarker", CTE_END, "owned"), -1);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "bad\x01text"), -1);
    ASSERT(cte_assert_bytes(path, "keep\n", strlen("keep\n")));
    th_cleanup(dir);
    PASS();
}

TEST(config_text_owned_document_write_remove_exact_only) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char owned[] = "# Managed\n\nUse the graph.\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(cbm_text_write_owned_document(path, owned), 0);
    ASSERT_EQ(cbm_text_create_owned_document(path, owned), -1);
    ASSERT_EQ(cbm_text_ensure_owned_document(path, owned), 0);
    ASSERT(cte_assert_bytes(path, owned, sizeof(owned) - 1U));
    ASSERT_EQ(cbm_text_write_owned_document(path, owned), 0);
    ASSERT_EQ(th_write_file(path, "# User changed this\n"), 0);
    ASSERT_EQ(cbm_text_ensure_owned_document(path, owned), -1);
    ASSERT_EQ(cbm_text_remove_owned_document(path, owned), 1);
    ASSERT(cte_assert_bytes(path, "# User changed this\n", strlen("# User changed this\n")));
    ASSERT_EQ(cbm_text_write_owned_document(path, owned), 0);
    ASSERT_EQ(cbm_text_remove_owned_document(path, owned), 0);
    ASSERT(!cte_path_exists(path));
    ASSERT_EQ(cbm_text_ensure_owned_document(path, owned), 0);
    ASSERT(cte_assert_bytes(path, owned, sizeof(owned) - 1U));
    ASSERT_EQ(cbm_text_remove_owned_document(path, owned), 0);
    ASSERT_EQ(cbm_text_create_owned_document(path, owned), 0);
    ASSERT(cte_assert_bytes(path, owned, sizeof(owned) - 1U));
    ASSERT_EQ(cbm_text_remove_owned_document(path, owned), 0);
    ASSERT_EQ(cbm_text_remove_owned_document(path, owned), 0);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_owned_document_migrates_exact_releases_only) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char *current = "name: codebase-memory\ntier: verify\n";
    const char *released[] = {"name: codebase-memory\n", "name: codebase-memory\nlegacy: 2\n"};
    const char *modified = "name: codebase-memory\nuser-note: keep\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);

    ASSERT_EQ(cbm_text_migrate_owned_document(path, current, released, 2U), 0);
    ASSERT(cte_assert_bytes(path, current, strlen(current)));

    ASSERT_EQ(th_write_file(path, released[1]), 0);
    ASSERT_EQ(cbm_text_migrate_owned_document(path, current, released, 2U), 0);
    ASSERT(cte_assert_bytes(path, current, strlen(current)));

    ASSERT_EQ(th_write_file(path, modified), 0);
    ASSERT_EQ(cbm_text_migrate_owned_document(path, current, released, 2U), 1);
    ASSERT(cte_assert_bytes(path, modified, strlen(modified)));
    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, current, released, 2U, 04755U), -1);
    ASSERT(cte_assert_bytes(path, modified, strlen(modified)));
    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, current, released, 2U, 0200U), -1);
    ASSERT(cte_assert_bytes(path, modified, strlen(modified)));

    ASSERT_EQ(cbm_text_remove_owned_document_any(path, current, released, 2U), 1);
    ASSERT(cte_assert_bytes(path, modified, strlen(modified)));
    ASSERT_EQ(th_write_file(path, released[0]), 0);
    ASSERT_EQ(cbm_text_remove_owned_document_any(path, current, released, 2U), 0);
    ASSERT_FALSE(cte_path_exists(path));

    th_cleanup(dir);
    PASS();
}

TEST(config_text_rejects_invalid_utf8_and_controls) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char invalid_utf8[] = {'k', 'e', 'e', 'p', (char)0xC0, (char)0xAF, '\n'};
    const char c1_control[] = {'k', 'e', 'e', 'p', (char)0xC2, (char)0x85, '\n'};
    const char nul_byte[] = {'k', 'e', 'e', 'p', '\0', 'x', '\n'};
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(cte_write_bytes(path, invalid_utf8, sizeof(invalid_utf8)), 0);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned"), -1);
    ASSERT(cte_assert_bytes(path, invalid_utf8, sizeof(invalid_utf8)));
    ASSERT_EQ(cte_write_bytes(path, c1_control, sizeof(c1_control)), 0);
    ASSERT_EQ(cbm_text_write_owned_document(path, "replacement\n"), -1);
    ASSERT(cte_assert_bytes(path, c1_control, sizeof(c1_control)));
    ASSERT_EQ(cte_write_bytes(path, nul_byte, sizeof(nul_byte)), 0);
    ASSERT_EQ(cbm_text_remove_managed_block(path, CTE_BEGIN, CTE_END), -1);
    ASSERT(cte_assert_bytes(path, nul_byte, sizeof(nul_byte)));
    ASSERT_EQ(cbm_text_write_owned_document(path, "bad\x7ftext"), -1);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_rejects_oversized_existing_file) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    FILE *file = cbm_fopen(path, "wb");
    ASSERT_NOT_NULL(file);
    char chunk[4096];
    memset(chunk, 'a', sizeof(chunk));
    size_t remaining = CTE_LIMIT + 1U;
    while (remaining != 0U) {
        size_t amount = remaining < sizeof(chunk) ? remaining : sizeof(chunk);
        ASSERT_EQ(fwrite(chunk, 1U, amount, file), amount);
        remaining -= amount;
    }
    ASSERT_EQ(fclose(file), 0);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned"), -1);
    struct stat state;
    ASSERT_EQ(stat(path, &state), 0);
    ASSERT_EQ((uint64_t)state.st_size, (uint64_t)CTE_LIMIT + 1U);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_rejects_non_regular_paths) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(cbm_mkdir(path), 0);
    ASSERT_EQ(cbm_text_write_owned_document(path, "owned\n"), -1);
    ASSERT_EQ(cbm_rmdir(path), 0);
#ifndef _WIN32
    ASSERT_EQ(mkfifo(path, 0600), 0);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned"), -1);
    ASSERT_EQ(cbm_unlink(path), 0);
#endif
    ASSERT_EQ(cte_temp_count(dir), 0U);
    th_cleanup(dir);
    PASS();
}

#ifndef _WIN32
TEST(config_text_rejects_links_privileged_mode_and_preserves_metadata) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char target[CTE_PATH_CAP];
    char alias[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(target, sizeof(target), "%s/target.md", dir) > 0);
    ASSERT(snprintf(alias, sizeof(alias), "%s/alias.md", dir) > 0);
    ASSERT_EQ(th_write_file(target, "target\n"), 0);
    ASSERT_EQ(symlink(target, path), 0);
    ASSERT_EQ(cbm_text_write_owned_document(path, "owned\n"), -1);
    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, "owned\n", NULL, 0U, 0755U), -1);
    ASSERT_EQ(cbm_unlink(path), 0);

    ASSERT_EQ(th_write_file(path, "shared\n"), 0);
    ASSERT_EQ(link(path, alias), 0);
    ASSERT_EQ(cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned"), -1);
    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, "owned\n", NULL, 0U, 0755U), -1);
    ASSERT_EQ(cbm_unlink(alias), 0);

    ASSERT_EQ(chmod(path, 04755), 0);
    struct stat privileged;
    ASSERT_EQ(stat(path, &privileged), 0);
    if ((privileged.st_mode & S_ISUID) != 0) {
        ASSERT_EQ(cbm_text_write_owned_document(path, "owned\n"), -1);
    }
    ASSERT_EQ(chmod(path, 0640), 0);
    struct stat before;
    struct stat after;
    ASSERT_EQ(stat(path, &before), 0);
    ASSERT_EQ(cbm_text_write_owned_document(path, "owned\n"), 0);
    ASSERT_EQ(stat(path, &after), 0);
    ASSERT_EQ(after.st_mode & 0777U, before.st_mode & 0777U);
    ASSERT_EQ(after.st_uid, before.st_uid);
    ASSERT_EQ(after.st_gid, before.st_gid);
    ASSERT_EQ(cte_temp_count(dir), 0U);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_owned_document_mode_publishes_exact_bytes_atomically) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char *current = "#!/bin/sh\necho current\n";
    const char *released[] = {"#!/bin/sh\necho released\n"};
    const char *foreign = "#!/bin/sh\necho foreign\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);

    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, current, released, 1U, 0755U), 0);
    struct stat state;
    ASSERT_EQ(stat(path, &state), 0);
    ASSERT_EQ(state.st_mode & 0777U, 0755U);
    ASSERT_EQ(state.st_uid, geteuid());
    ASSERT(cte_assert_bytes(path, current, strlen(current)));
    struct stat stable;
    ASSERT_EQ(stat(path, &stable), 0);
    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, current, released, 1U, 0755U), 0);
    ASSERT_EQ(stat(path, &state), 0);
    ASSERT_EQ(state.st_ino, stable.st_ino);

    ASSERT_EQ(chmod(path, 0600), 0);
    struct stat before;
    ASSERT_EQ(stat(path, &before), 0);
    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, current, released, 1U, 0755U), 0);
    ASSERT_EQ(stat(path, &state), 0);
    ASSERT_EQ(state.st_mode & 0777U, 0755U);
    ASSERT(state.st_ino != before.st_ino);
    ASSERT(cte_assert_bytes(path, current, strlen(current)));

    ASSERT_EQ(th_write_file(path, released[0]), 0);
    ASSERT_EQ(chmod(path, 0640), 0);
    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, current, released, 1U, 0755U), 0);
    ASSERT_EQ(stat(path, &state), 0);
    ASSERT_EQ(state.st_mode & 0777U, 0755U);
    ASSERT(cte_assert_bytes(path, current, strlen(current)));

    ASSERT_EQ(th_write_file(path, foreign), 0);
    ASSERT_EQ(chmod(path, 0640), 0);
    ASSERT_EQ(cbm_text_migrate_owned_document_mode(path, current, released, 1U, 0755U), 1);
    ASSERT_EQ(stat(path, &state), 0);
    ASSERT_EQ(state.st_mode & 0777U, 0640U);
    ASSERT(cte_assert_bytes(path, foreign, strlen(foreign)));
    ASSERT_EQ(cte_temp_count(dir), 0U);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_owned_document_mode_rejects_prepublish_replacement) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char backup[CTE_PATH_CAP];
    const char *current = "#!/bin/sh\necho current\n";
    const char *winner = "#!/bin/sh\necho winner\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.sh", dir) > 0);
    ASSERT_EQ(th_write_file(path, current), 0);
    ASSERT_EQ(chmod(path, 0600), 0);
    cte_precommit_change_t race = {
        .content = winner, .backup_path = backup, .replace_identity = 1, .result = -1};

    cbm_text_set_prepublish_hook_for_testing(cte_change_before_commit, &race);
    int result = cbm_text_migrate_owned_document_mode(path, current, NULL, 0U, 0755U);
    cbm_text_set_prepublish_hook_for_testing(NULL, NULL);

    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT(cte_assert_bytes(path, winner, strlen(winner)));
    struct stat state;
    ASSERT_EQ(stat(path, &state), 0);
    ASSERT_EQ(state.st_mode & 0111U, 0U);
    ASSERT_EQ(cte_temp_count(dir), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_owned_document_mode_rejects_closed_temp_replacement) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char backup[CTE_PATH_CAP];
    const char *current = "#!/bin/sh\necho current\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(backup, sizeof(backup), "%s/original-temp", dir) > 0);
    cte_precommit_change_t race = {
        .content = current, .backup_path = backup, .replace_identity = 1, .result = -1};

    cbm_text_set_temp_closed_hook_for_testing(cte_replace_closed_temp, &race);
    int result = cbm_text_migrate_owned_document_mode(path, current, NULL, 0U, 0755U);
    cbm_text_set_temp_closed_hook_for_testing(NULL, NULL);

    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT_FALSE(cte_path_exists(path));
    ASSERT(cte_assert_bytes(backup, current, strlen(current)));
    struct stat trusted;
    ASSERT_EQ(stat(backup, &trusted), 0);
    ASSERT_EQ(trusted.st_mode & 0777U, 0755U);
    th_cleanup(dir);
    PASS();
}
#endif

TEST(config_text_rejects_stale_content_and_identity) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char backup[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.md", dir) > 0);
    ASSERT_EQ(th_write_file(path, "keep\n"), 0);
    cte_precommit_change_t content_change = {
        .content = "concurrent\n", .backup_path = NULL, .replace_identity = 0, .result = -1};
    cbm_text_set_precommit_hook_for_testing(cte_change_before_commit, &content_change);
    int result = cbm_text_upsert_managed_block(path, CTE_BEGIN, CTE_END, "owned");
    cbm_text_set_precommit_hook_for_testing(NULL, NULL);
    ASSERT_EQ(content_change.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT(cte_assert_bytes(path, "concurrent\n", strlen("concurrent\n")));
    ASSERT_EQ(cte_temp_count(dir), 0U);

    ASSERT_EQ(th_write_file(path, "keep\n"), 0);
    cte_precommit_change_t identity_change = {
        .content = "keep\n", .backup_path = backup, .replace_identity = 1, .result = -1};
    cbm_text_set_precommit_hook_for_testing(cte_change_before_commit, &identity_change);
    result = cbm_text_write_owned_document(path, "owned\n");
    cbm_text_set_precommit_hook_for_testing(NULL, NULL);
    ASSERT_EQ(identity_change.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT(cte_assert_bytes(path, "keep\n", strlen("keep\n")));
    ASSERT_EQ(cbm_unlink(backup), 0);
    ASSERT_EQ(cte_temp_count(dir), 0U);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_missing_target_race_does_not_replace_winner) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    cte_precommit_change_t race = {
        .content = "winner\n", .backup_path = NULL, .replace_identity = 0, .result = -1};
    cbm_text_set_precommit_hook_for_testing(cte_change_before_commit, &race);
    int result = cbm_text_write_owned_document(path, "owned\n");
    cbm_text_set_precommit_hook_for_testing(NULL, NULL);
    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT(cte_assert_bytes(path, "winner\n", strlen("winner\n")));
    ASSERT_EQ(cte_temp_count(dir), 0U);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_exact_remove_rechecks_stale_snapshot) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char owned[] = "owned\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, owned), 0);
    cte_precommit_change_t change = {
        .content = "winner\n", .backup_path = NULL, .replace_identity = 0, .result = -1};
    cbm_text_set_precommit_hook_for_testing(cte_change_before_commit, &change);
    int result = cbm_text_remove_owned_document(path, owned);
    cbm_text_set_precommit_hook_for_testing(NULL, NULL);
    ASSERT_EQ(change.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT(cte_assert_bytes(path, "winner\n", strlen("winner\n")));
    th_cleanup(dir);
    PASS();
}

TEST(config_text_existing_target_swap_after_check_preserves_winner) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char backup[CTE_PATH_CAP];
    const char *original = "keep\n";
    const char *winner = "winner\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.md", dir) > 0);
    ASSERT_EQ(th_write_file(path, original), 0);
    cte_precommit_change_t race = {
        .content = winner, .backup_path = backup, .replace_identity = 1, .result = -1};

    cbm_text_set_prepublish_hook_for_testing(cte_change_before_commit, &race);
    int result = cbm_text_write_owned_document(path, "owned\n");
    cbm_text_set_prepublish_hook_for_testing(NULL, NULL);

    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT(cte_assert_bytes(path, winner, strlen(winner)));
    ASSERT_EQ(cte_temp_count(dir), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_exact_remove_swap_after_check_preserves_winner) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    char backup[CTE_PATH_CAP];
    const char *owned = "owned\n";
    const char *winner = "winner\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.md", dir) > 0);
    ASSERT_EQ(th_write_file(path, owned), 0);
    cte_precommit_change_t race = {
        .content = winner, .backup_path = backup, .replace_identity = 1, .result = -1};

    cbm_text_set_prepublish_hook_for_testing(cte_change_before_commit, &race);
    int result = cbm_text_remove_owned_document(path, owned);
    cbm_text_set_prepublish_hook_for_testing(NULL, NULL);

    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    ASSERT(cte_assert_bytes(path, winner, strlen(winner)));
    ASSERT_EQ(cbm_unlink(backup), 0);
    th_cleanup(dir);
    PASS();
}

TEST(config_text_write_owned_document_if_unchanged_rejects_stale_snapshot) {
    char dir[CTE_PATH_CAP];
    char path[CTE_PATH_CAP];
    const char *original = "name: Continue\n";
    const char *updated = "name: Continue\nmcpServers:\n  - name: codebase-memory-mcp\n";
    const char *winner = "name: Continue\nuser-setting: preserved\n";
    ASSERT_EQ(cte_fixture(dir, sizeof(dir), path, sizeof(path)), 0);
    ASSERT_EQ(th_write_file(path, original), 0);
    ASSERT_EQ(cbm_text_write_owned_document_if_unchanged(path, updated, original, strlen(original)),
              0);
    ASSERT(cte_assert_bytes(path, updated, strlen(updated)));

    ASSERT_EQ(th_write_file(path, winner), 0);
    ASSERT_EQ(cbm_text_write_owned_document_if_unchanged(path, updated, original, strlen(original)),
              -1);
    ASSERT(cte_assert_bytes(path, winner, strlen(winner)));

    ASSERT_EQ(cbm_text_write_owned_document_if_unchanged(path, updated, NULL, 0U), -1);
    ASSERT(cte_assert_bytes(path, winner, strlen(winner)));
    ASSERT_EQ(cte_temp_count(dir), 0U);
    th_cleanup(dir);
    PASS();
}

SUITE(config_text_edit) {
    RUN_TEST(config_text_managed_insert_preserves_bom_comments_and_is_idempotent);
    RUN_TEST(config_text_managed_replace_preserves_crlf_surroundings);
    RUN_TEST(config_text_managed_no_final_newline_round_trip);
    RUN_TEST(config_text_managed_remove_preserves_user_bytes);
    RUN_TEST(config_text_managed_malformed_markers_fail_closed);
    RUN_TEST(config_text_managed_rejects_unsafe_markers_and_owned_content);
    RUN_TEST(config_text_owned_document_write_remove_exact_only);
    RUN_TEST(config_text_owned_document_migrates_exact_releases_only);
    RUN_TEST(config_text_rejects_invalid_utf8_and_controls);
    RUN_TEST(config_text_rejects_oversized_existing_file);
    RUN_TEST(config_text_rejects_non_regular_paths);
#ifndef _WIN32
    RUN_TEST(config_text_rejects_links_privileged_mode_and_preserves_metadata);
    RUN_TEST(config_text_owned_document_mode_publishes_exact_bytes_atomically);
    RUN_TEST(config_text_owned_document_mode_rejects_prepublish_replacement);
    RUN_TEST(config_text_owned_document_mode_rejects_closed_temp_replacement);
#endif
    RUN_TEST(config_text_rejects_stale_content_and_identity);
    RUN_TEST(config_text_missing_target_race_does_not_replace_winner);
    RUN_TEST(config_text_exact_remove_rechecks_stale_snapshot);
    RUN_TEST(config_text_existing_target_swap_after_check_preserves_winner);
    RUN_TEST(config_text_exact_remove_swap_after_check_preserves_winner);
    RUN_TEST(config_text_write_owned_document_if_unchanged_rejects_stale_snapshot);
}
