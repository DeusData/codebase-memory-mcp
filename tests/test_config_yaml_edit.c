/*
 * test_config_yaml_edit.c — Conservative YAML config editor tests.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"

#define CBM_YAML_ENABLE_TEST_API 1
#include <cli/config_yaml_edit.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#endif

/* Expected test seams for races after final verification and failures after
 * lock-object creation. Production code must not expose them outside the
 * test API build. */
void cbm_yaml_set_prepublish_hook_for_testing(cbm_yaml_precommit_test_hook_t hook, void *context);
#ifndef _WIN32
typedef void (*cbm_yaml_lock_postcreate_test_hook_t)(const char *lock_path, void *context);
void cbm_yaml_set_lock_postcreate_hook_for_testing(cbm_yaml_lock_postcreate_test_hook_t hook,
                                                   void *context);
#endif

typedef struct {
    char dir[512];
    char path[768];
} yaml_fixture_t;

static int yaml_fixture_init(yaml_fixture_t *fixture, const char *initial) {
    char *temp = th_mktempdir("cbm_yaml_edit");
    if (!temp) {
        return -1;
    }
    int dir_len = snprintf(fixture->dir, sizeof(fixture->dir), "%s", temp);
    int path_len = snprintf(fixture->path, sizeof(fixture->path), "%s/config.yaml", temp);
    if (dir_len < 0 || (size_t)dir_len >= sizeof(fixture->dir) || path_len < 0 ||
        (size_t)path_len >= sizeof(fixture->path)) {
        th_cleanup(temp);
        return -1;
    }
    return initial ? th_write_file(fixture->path, initial) : 0;
}

static char *yaml_read_alloc(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp || fseek(fp, 0L, SEEK_END) != 0) {
        if (fp) {
            fclose(fp);
        }
        return NULL;
    }
    long file_len = ftell(fp);
    if (file_len < 0L || fseek(fp, 0L, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }
    size_t len = (size_t)file_len;
    char *data = (char *)malloc(len + 1U);
    if (!data) {
        fclose(fp);
        return NULL;
    }
    size_t read_len = fread(data, 1U, len, fp);
    int close_rc = fclose(fp);
    if (read_len != len || close_rc != 0) {
        free(data);
        return NULL;
    }
    data[len] = '\0';
    return data;
}

static size_t yaml_count_occurrences(const char *text, const char *needle) {
    size_t count = 0U;
    size_t needle_len = strlen(needle);
    const char *cursor = text;
    while ((cursor = strstr(cursor, needle)) != NULL) {
        count++;
        cursor += needle_len;
    }
    return count;
}

static size_t yaml_temp_file_count(const yaml_fixture_t *fixture) {
    cbm_dir_t *directory = cbm_opendir(fixture->dir);
    if (!directory) {
        return SIZE_MAX;
    }
    size_t count = 0U;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(directory)) != NULL) {
        if (strncmp(entry->name, "config.yaml.cbm-yaml-", strlen("config.yaml.cbm-yaml-")) == 0) {
            count++;
        }
    }
    cbm_closedir(directory);
    return count;
}

static bool yaml_lock_released_state_is_safe(const char *path) {
    struct stat state;
#ifdef _WIN32
    return stat(path, &state) != 0;
#else
    return lstat(path, &state) == 0 && S_ISREG(state.st_mode) && state.st_nlink == 1 &&
           state.st_uid == geteuid() && (state.st_mode & 0777U) == 0600U &&
           (state.st_mode & (S_ISUID | S_ISGID | S_ISVTX)) == 0;
#endif
}

static bool yaml_upsert_failed_unchanged(const char *path, const char *original) {
    const char *block = "    command: codebase-memory-mcp\n";
    if (th_write_file(path, original) != 0 ||
        cbm_yaml_upsert_mapping_entry(path, "mcp_servers", "codebase-memory", block) == 0) {
        return false;
    }
    char *after = yaml_read_alloc(path);
    bool unchanged = after && strcmp(after, original) == 0;
    free(after);
    return unchanged;
}

typedef struct {
    const char *content;
    const char *backup_path;
    bool replace_identity;
    int result;
} yaml_precommit_change_t;

typedef struct {
    int competing_result;
    bool lock_observed;
} yaml_lock_contention_t;

static void yaml_change_before_commit(const char *path, void *context) {
    yaml_precommit_change_t *change = (yaml_precommit_change_t *)context;
    if (change->replace_identity &&
        (!change->backup_path || cbm_rename_replace(path, change->backup_path) != 0)) {
        change->result = -1;
        return;
    }
    change->result = th_write_file(path, change->content);
}

#ifndef _WIN32
static void yaml_make_lock_mode_unsafe(const char *lock_path, void *context) {
    int *result = (int *)context;
    *result = chmod(lock_path, 0755);
}
#endif

static void yaml_attempt_competing_edit(const char *path, void *context) {
    yaml_lock_contention_t *contention = (yaml_lock_contention_t *)context;
    char lock_path[1024];
    int written = snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", path);
    struct stat state;
    bool expected_type = false;
    if (written > 0 && (size_t)written < sizeof(lock_path) && stat(lock_path, &state) == 0) {
#ifdef _WIN32
        expected_type = S_ISDIR(state.st_mode);
#else
        expected_type = S_ISREG(state.st_mode);
#endif
    }
    contention->lock_observed = expected_type;
    contention->competing_result = cbm_yaml_upsert_string_list_item(path, "read", "COMPETING.md");
    contention->lock_observed = contention->lock_observed || contention->competing_result == -1;
}

TEST(config_yaml_edit_serializes_two_editor_instances) {
    const char *original = "model: fast\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, original), 0);
    yaml_lock_contention_t contention = {
        .competing_result = 0,
        .lock_observed = false,
    };

    cbm_yaml_set_precommit_hook_for_testing(yaml_attempt_competing_edit, &contention);
    int result = cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md");
    cbm_yaml_set_precommit_hook_for_testing(NULL, NULL);

    ASSERT_EQ(result, 0);
    ASSERT(contention.lock_observed);
    ASSERT_EQ(contention.competing_result, -1);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_NOT_NULL(strstr(after, "  - \"AGENTS.md\"\n"));
    ASSERT_NULL(strstr(after, "COMPETING.md"));
    free(after);
    char lock_path[1024];
    ASSERT(snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", fixture.path) > 0);
    ASSERT(yaml_lock_released_state_is_safe(lock_path));
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_missing_target_appearance_fails_without_replace) {
    const char *concurrent = "model: concurrently-created\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);
    yaml_precommit_change_t change = {
        .content = concurrent,
        .backup_path = NULL,
        .replace_identity = false,
        .result = -1,
    };

    cbm_yaml_set_precommit_hook_for_testing(yaml_change_before_commit, &change);
    int result = cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md");
    cbm_yaml_set_precommit_hook_for_testing(NULL, NULL);

    ASSERT_EQ(change.result, 0);
    ASSERT_EQ(result, -1);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, concurrent);
    free(after);
    char lock_path[1024];
    ASSERT(snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", fixture.path) > 0);
    ASSERT(yaml_lock_released_state_is_safe(lock_path));
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_rejects_stale_content_and_cleans_temp) {
    const char *original = "model: fast\n";
    const char *concurrent = "model: concurrent\n";
    const char *block = "    command: codebase-memory-mcp\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, original), 0);
    yaml_precommit_change_t change = {
        .content = concurrent,
        .backup_path = NULL,
        .replace_identity = false,
        .result = -1,
    };

    cbm_yaml_set_precommit_hook_for_testing(yaml_change_before_commit, &change);
    int result =
        cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", block);
    cbm_yaml_set_precommit_hook_for_testing(NULL, NULL);

    ASSERT_EQ(change.result, 0);
    ASSERT_EQ(result, -1);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, concurrent);
    free(after);
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_rejects_stale_identity_with_same_content) {
    const char *original = "model: fast\n";
    const char *block = "    command: codebase-memory-mcp\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, original), 0);
    char backup[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.yaml", fixture.dir) > 0);
    yaml_precommit_change_t change = {
        .content = original,
        .backup_path = backup,
        .replace_identity = true,
        .result = -1,
    };

    cbm_yaml_set_precommit_hook_for_testing(yaml_change_before_commit, &change);
    int result =
        cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", block);
    cbm_yaml_set_precommit_hook_for_testing(NULL, NULL);

    ASSERT_EQ(change.result, 0);
    ASSERT_EQ(result, -1);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, original);
    free(after);
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_existing_target_swap_after_check_preserves_winner) {
    const char *original = "model: fast\n";
    const char *winner = "model: winner\n";
    const char *block = "    command: codebase-memory-mcp\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, original), 0);
    char backup[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.yaml", fixture.dir) > 0);
    yaml_precommit_change_t race = {
        .content = winner,
        .backup_path = backup,
        .replace_identity = true,
        .result = -1,
    };

    cbm_yaml_set_prepublish_hook_for_testing(yaml_change_before_commit, &race);
    int result =
        cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", block);
    cbm_yaml_set_prepublish_hook_for_testing(NULL, NULL);

    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, winner);
    free(after);
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    char lock_path[1024];
    ASSERT(snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", fixture.path) > 0);
    ASSERT(yaml_lock_released_state_is_safe(lock_path));
    th_cleanup(fixture.dir);
    PASS();
}

#ifndef _WIN32
TEST(config_yaml_edit_lock_postcreate_verification_failure_preserves_unsafe_sidecar) {
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, "model: fast\n"), 0);
    int mutation_result = -1;

    cbm_yaml_set_lock_postcreate_hook_for_testing(yaml_make_lock_mode_unsafe, &mutation_result);
    int result = cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md");
    cbm_yaml_set_lock_postcreate_hook_for_testing(NULL, NULL);

    ASSERT_EQ(mutation_result, 0);
    ASSERT_EQ(result, -1);
    char lock_path[1024];
    ASSERT(snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", fixture.path) > 0);
    struct stat state;
    ASSERT_EQ(lstat(lock_path, &state), 0);
    ASSERT(S_ISREG(state.st_mode));
    ASSERT_EQ(state.st_uid, geteuid());
    ASSERT_EQ(state.st_mode & 0777U, 0755U);
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_reuses_persistent_safe_lock_sidecar) {
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, "model: fast\n"), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    char lock_path[1024];
    ASSERT(snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", fixture.path) > 0);
    ASSERT(yaml_lock_released_state_is_safe(lock_path));
    struct stat first;
    ASSERT_EQ(lstat(lock_path, &first), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "CONVENTIONS.md"), 0);
    ASSERT(yaml_lock_released_state_is_safe(lock_path));
    struct stat second;
    ASSERT_EQ(lstat(lock_path, &second), 0);
    ASSERT_EQ(first.st_dev, second.st_dev);
    ASSERT_EQ(first.st_ino, second.st_ino);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_NOT_NULL(strstr(after, "AGENTS.md"));
    ASSERT_NOT_NULL(strstr(after, "CONVENTIONS.md"));
    free(after);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_rejects_symlink_lock_sidecar) {
    const char *original = "model: fast\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, original), 0);
    char lock_path[1024];
    char target_path[1024];
    ASSERT(snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", fixture.path) > 0);
    ASSERT(snprintf(target_path, sizeof(target_path), "%s/foreign-lock", fixture.dir) > 0);
    ASSERT_EQ(th_write_file(target_path, "foreign\n"), 0);
    ASSERT_EQ(symlink(target_path, lock_path), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), -1);
    struct stat link_state;
    ASSERT_EQ(lstat(lock_path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, original);
    free(after);
    char *target = yaml_read_alloc(target_path);
    ASSERT_NOT_NULL(target);
    ASSERT_STR_EQ(target, "foreign\n");
    free(target);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_rejects_hard_linked_lock_sidecar) {
    const char *original = "model: fast\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, original), 0);
    char lock_path[1024];
    char source_path[1024];
    ASSERT(snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", fixture.path) > 0);
    ASSERT(snprintf(source_path, sizeof(source_path), "%s/foreign-lock", fixture.dir) > 0);
    ASSERT_EQ(th_write_file(source_path, "foreign\n"), 0);
    ASSERT_EQ(chmod(source_path, 0600), 0);
    ASSERT_EQ(link(source_path, lock_path), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), -1);
    struct stat source_state;
    struct stat lock_state;
    ASSERT_EQ(lstat(source_path, &source_state), 0);
    ASSERT_EQ(lstat(lock_path, &lock_state), 0);
    ASSERT_EQ(source_state.st_ino, lock_state.st_ino);
    ASSERT_EQ(source_state.st_nlink, 2);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, original);
    free(after);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_rejects_unsafe_mode_lock_sidecar) {
    const char *original = "model: fast\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, original), 0);
    char lock_path[1024];
    ASSERT(snprintf(lock_path, sizeof(lock_path), "%s.cbm-yaml.lock", fixture.path) > 0);
    ASSERT_EQ(th_write_file(lock_path, "foreign\n"), 0);
    ASSERT_EQ(chmod(lock_path, 0644), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), -1);
    struct stat state;
    ASSERT_EQ(lstat(lock_path, &state), 0);
    ASSERT(S_ISREG(state.st_mode));
    ASSERT_EQ(state.st_mode & 0777U, 0644U);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, original);
    free(after);
    th_cleanup(fixture.dir);
    PASS();
}
#endif

TEST(config_yaml_edit_rejects_non_regular_path) {
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);
    ASSERT(cbm_mkdir_p(fixture.path, 0755));
    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), -1);
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);
    th_cleanup(fixture.dir);
    PASS();
}

#ifndef _WIN32
TEST(config_yaml_edit_rejects_symlinks_without_touching_target) {
    const char *original = "model: safe\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);
    char target[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(target, sizeof(target), "%s/target.yaml", fixture.dir) > 0);
    ASSERT_EQ(th_write_file(target, original), 0);
    ASSERT_EQ(symlink(target, fixture.path), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), -1);
    struct stat link_state;
    ASSERT_EQ(lstat(fixture.path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    char *after = yaml_read_alloc(target);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, original);
    free(after);
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_rejects_dangling_symlink) {
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);
    char missing[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(missing, sizeof(missing), "%s/missing.yaml", fixture.dir) > 0);
    ASSERT_EQ(symlink(missing, fixture.path), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), -1);
    struct stat link_state;
    ASSERT_EQ(lstat(fixture.path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_preserves_owner_group_and_mode) {
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, "model: fast\n"), 0);
    ASSERT_EQ(chmod(fixture.path, 0640), 0);
    struct stat before;
    ASSERT_EQ(stat(fixture.path, &before), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    struct stat after;
    ASSERT_EQ(stat(fixture.path, &after), 0);
    ASSERT_EQ(after.st_uid, before.st_uid);
    ASSERT_EQ(after.st_gid, before.st_gid);
    ASSERT_EQ(after.st_mode & 07777, before.st_mode & 07777);

    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_rejects_hard_links_without_splitting_identity) {
    const char *original = "model: safe\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, original), 0);
    char alias[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(alias, sizeof(alias), "%s/alias.yaml", fixture.dir) > 0);
    ASSERT_EQ(link(fixture.path, alias), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), -1);
    char *after = yaml_read_alloc(alias);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, original);
    free(after);
    ASSERT_EQ(yaml_temp_file_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(alias), 0);
    th_cleanup(fixture.dir);
    PASS();
}
#endif

TEST(config_yaml_edit_encodes_dynamic_scalars_safely) {
    char *encoded = NULL;
    ASSERT_EQ(cbm_yaml_encode_double_quoted_scalar(
                  "C:\\Users\\Zo\xC3\xAB\\\xE4\xBB\xA3\xE7\xA0\x81 #1: \"tool\"", &encoded),
              0);
    ASSERT_NOT_NULL(encoded);
    ASSERT_STR_EQ(encoded,
                  "\"C:\\\\Users\\\\Zo\xC3\xAB\\\\\xE4\xBB\xA3\xE7\xA0\x81 #1: \\\"tool\\\"\"");
    free(encoded);

    encoded = (char *)0x1;
    ASSERT_EQ(cbm_yaml_encode_double_quoted_scalar("line one\nline two", &encoded), -1);
    ASSERT_NULL(encoded);
    ASSERT_EQ(cbm_yaml_encode_double_quoted_scalar("bad\x01"
                                                   "control",
                                                   &encoded),
              -1);
    ASSERT_NULL(encoded);
    PASS();
}

TEST(config_yaml_edit_rejects_newline_list_items) {
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, "model: fast\n"), 0);
    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "one\ntwo"), -1);
    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "one\rtwo"), -1);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, "model: fast\n");
    free(after);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_absent_target_requires_safe_root_mapping) {
    const char *cases[] = {
        "- sequence-root\n",
        "plain scalar root\n",
        "{}\n",
        "[]\n",
        "<<: *defaults\n",
        "? mcp_servers\n: {}\n",
        "\"bad\\xFF\": value\n",
        "%YAML 1.2\n---\nmodel: fast\n",
        "model: fast\n---\nother: value\n",
        "model: fast\n...\n",
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        yaml_fixture_t fixture;
        ASSERT_EQ(yaml_fixture_init(&fixture, cases[i]), 0);
        ASSERT(yaml_upsert_failed_unchanged(fixture.path, cases[i]));
        th_cleanup(fixture.dir);
    }

    yaml_fixture_t safe;
    ASSERT_EQ(yaml_fixture_init(&safe, "model:\n  fallbacks:\n    - local\n"), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(safe.path, "mcp_servers", "codebase-memory",
                                            "    command: codebase-memory-mcp\n"),
              0);
    char *after = yaml_read_alloc(safe.path);
    ASSERT_NOT_NULL(after);
    ASSERT_NOT_NULL(strstr(after, "model:\n  fallbacks:\n    - local\n"));
    ASSERT_NOT_NULL(strstr(after, "mcp_servers:\n  codebase-memory:\n"));
    free(after);
    th_cleanup(safe.dir);
    PASS();
}

TEST(config_yaml_edit_rejects_semantic_target_key_aliases) {
    const char *cases[] = {
        "\"mcp\\x5fservers\":\n  other:\n    command: other\n",
        "mcp_servers:\n  \"codebase\\x2dmemory\":\n    command: other\n",
        "\"r\\x65ad\":\n  - docs.md\n",
    };
    const char *block = "    command: codebase-memory-mcp\n";

    yaml_fixture_t section;
    ASSERT_EQ(yaml_fixture_init(&section, cases[0]), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(section.path, "mcp_servers", "codebase-memory", block),
              -1);
    char *after = yaml_read_alloc(section.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, cases[0]);
    free(after);
    th_cleanup(section.dir);

    yaml_fixture_t entry;
    ASSERT_EQ(yaml_fixture_init(&entry, cases[1]), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(entry.path, "mcp_servers", "codebase-memory", block),
              -1);
    after = yaml_read_alloc(entry.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, cases[1]);
    free(after);
    th_cleanup(entry.dir);

    yaml_fixture_t list;
    ASSERT_EQ(yaml_fixture_init(&list, cases[2]), 0);
    ASSERT_EQ(cbm_yaml_upsert_string_list_item(list.path, "read", "AGENTS.md"), -1);
    after = yaml_read_alloc(list.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, cases[2]);
    free(after);
    th_cleanup(list.dir);
    PASS();
}

TEST(config_yaml_edit_preserves_crlf_and_handles_no_final_newline) {
    const char *block = "    command: codebase-memory-mcp\n";
    yaml_fixture_t crlf;
    ASSERT_EQ(yaml_fixture_init(&crlf, "model: fast\r\n"), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(crlf.path, "mcp_servers", "codebase-memory", block), 0);
    char *after = yaml_read_alloc(crlf.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, "model: fast\r\n"
                         "mcp_servers:\r\n"
                         "  codebase-memory:\r\n"
                         "    command: codebase-memory-mcp\r\n");
    free(after);
    th_cleanup(crlf.dir);

    yaml_fixture_t no_final_newline;
    ASSERT_EQ(yaml_fixture_init(&no_final_newline, "model: fast"), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(no_final_newline.path, "mcp_servers", "codebase-memory",
                                            block),
              0);
    after = yaml_read_alloc(no_final_newline.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, "model: fast\n"
                         "mcp_servers:\n"
                         "  codebase-memory:\n"
                         "    command: codebase-memory-mcp\n");
    free(after);
    th_cleanup(no_final_newline.dir);
    PASS();
}

TEST(config_yaml_edit_hermes_mapping_lifecycle) {
    const char *initial = "# Hermes settings\n"
                          "model: fast\n"
                          "mcp_servers:\n"
                          "  # preserve sibling\n"
                          "  other:\n"
                          "    command: other-mcp\n"
                          "theme: dark\n";
    const char *first_block = "    command: codebase-memory-mcp\n"
                              "    args: [\"--stdio\"]\n";
    const char *replacement_block = "    command: /opt/codebase-memory-mcp\n"
                                    "    args: [\"--stdio\"]\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);

    ASSERT_EQ(
        cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", first_block),
        0);
    char *installed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(installed);
    ASSERT_NOT_NULL(strstr(installed, "# Hermes settings\n"));
    ASSERT_NOT_NULL(strstr(installed, "  # preserve sibling\n"));
    ASSERT_NOT_NULL(strstr(installed, "  other:\n    command: other-mcp\n"));
    ASSERT_NOT_NULL(strstr(installed, "  codebase-memory:\n"
                                      "    command: codebase-memory-mcp\n"
                                      "    args: [\"--stdio\"]\n"));
    ASSERT_NOT_NULL(strstr(installed, "theme: dark\n"));
    ASSERT_EQ(yaml_count_occurrences(installed, "  codebase-memory:\n"), 1);

    ASSERT_EQ(
        cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", first_block),
        0);
    char *idempotent = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(idempotent);
    ASSERT_STR_EQ(idempotent, installed);

    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory",
                                            replacement_block),
              0);
    char *replaced = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(replaced);
    ASSERT_NOT_NULL(strstr(replaced, "    command: /opt/codebase-memory-mcp\n"));
    ASSERT_NULL(strstr(replaced, "    command: codebase-memory-mcp\n"));
    ASSERT_NOT_NULL(strstr(replaced, "  other:\n    command: other-mcp\n"));

    ASSERT_EQ(cbm_yaml_remove_mapping_entry(fixture.path, "mcp_servers", "codebase-memory"), 0);
    char *removed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT_STR_EQ(removed, initial);

    free(removed);
    free(replaced);
    free(idempotent);
    free(installed);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_hermes_creates_missing_section) {
    const char *block = "    command: codebase-memory-mcp\n"
                        "    args: [\"--stdio\"]\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);

    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", block),
              0);
    char *installed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(installed);
    ASSERT_STR_EQ(installed, "mcp_servers:\n"
                             "  codebase-memory:\n"
                             "    command: codebase-memory-mcp\n"
                             "    args: [\"--stdio\"]\n");

    ASSERT_EQ(cbm_yaml_remove_mapping_entry(fixture.path, "mcp_servers", "codebase-memory"), 0);
    char *empty = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(empty);
    ASSERT_STR_EQ(empty, "mcp_servers: {}\n");

    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", block),
              0);
    char *reinstalled = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(reinstalled);
    ASSERT_STR_EQ(reinstalled, installed);

    free(reinstalled);
    free(empty);
    free(installed);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_goose_extensions_preserve_siblings) {
    const char *initial = "extensions:\n"
                          "  shell:\n"
                          "    type: builtin\n"
                          "  telemetry:\n"
                          "    enabled: false\n"
                          "ui: compact\n";
    const char *block = "    type: stdio\n"
                        "    cmd: codebase-memory-mcp\n"
                        "    args: []\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);

    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(fixture.path, "extensions", "codebase-memory", block),
              0);
    char *installed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(installed);
    ASSERT_NOT_NULL(strstr(installed, "  shell:\n    type: builtin\n"));
    ASSERT_NOT_NULL(strstr(installed, "  telemetry:\n    enabled: false\n"));
    ASSERT_NOT_NULL(strstr(installed, "  codebase-memory:\n"
                                      "    type: stdio\n"
                                      "    cmd: codebase-memory-mcp\n"
                                      "    args: []\n"));
    ASSERT_NOT_NULL(strstr(installed, "ui: compact\n"));

    ASSERT_EQ(cbm_yaml_remove_mapping_entry(fixture.path, "extensions", "codebase-memory"), 0);
    char *removed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT_STR_EQ(removed, initial);

    free(removed);
    free(installed);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_owned_agent_mapping_installs_idempotently_and_removes_exact_state) {
    struct owned_mapping_case {
        const char *section;
        const char *canonical;
        const char *installed;
        const char *empty_section;
    } cases[] = {
        {
            "mcp_servers",
            "    command: \"/opt/codebase-memory-mcp\"\n",
            "mcp_servers:\n"
            "  codebase-memory-mcp:\n"
            "    command: \"/opt/codebase-memory-mcp\"\n",
            "mcp_servers: {}\n",
        },
        {
            "extensions",
            "    type: stdio\n"
            "    cmd: \"/opt/codebase-memory-mcp\"\n"
            "    args: []\n"
            "    enabled: true\n",
            "extensions:\n"
            "  codebase-memory-mcp:\n"
            "    type: stdio\n"
            "    cmd: \"/opt/codebase-memory-mcp\"\n"
            "    args: []\n"
            "    enabled: true\n",
            "extensions: {}\n",
        },
    };

    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        yaml_fixture_t fixture;
        ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);
        ASSERT_EQ(cbm_yaml_upsert_owned_mapping_entry(fixture.path, cases[i].section,
                                                      "codebase-memory-mcp", cases[i].canonical),
                  CBM_YAML_IDENTITY_EDIT_OK);
        char *installed = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(installed);
        ASSERT_STR_EQ(installed, cases[i].installed);

        ASSERT_EQ(cbm_yaml_upsert_owned_mapping_entry(fixture.path, cases[i].section,
                                                      "codebase-memory-mcp", cases[i].canonical),
                  CBM_YAML_IDENTITY_EDIT_OK);
        char *idempotent = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(idempotent);
        ASSERT_STR_EQ(idempotent, installed);

        ASSERT_EQ(cbm_yaml_remove_owned_mapping_entry(fixture.path, cases[i].section,
                                                      "codebase-memory-mcp", cases[i].canonical),
                  CBM_YAML_IDENTITY_EDIT_OK);
        char *removed = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(removed);
        ASSERT_STR_EQ(removed, cases[i].empty_section);

        free(removed);
        free(idempotent);
        free(installed);
        th_cleanup(fixture.dir);
    }
    PASS();
}

TEST(config_yaml_edit_owned_agent_mapping_preserves_foreign_same_name_state) {
    struct foreign_mapping_case {
        const char *section;
        const char *canonical;
        const char *foreign;
    } cases[] = {
        {
            "mcp_servers",
            "    command: \"/opt/codebase-memory-mcp\"\n",
            "mcp_servers:\n"
            "  codebase-memory-mcp:\n"
            "    command: \"/opt/user-owned-mcp\"\n",
        },
        {
            "mcp_servers",
            "    command: \"/opt/codebase-memory-mcp\"\n",
            "mcp_servers:\n"
            "  codebase-memory-mcp:\n"
            "    command: \"/opt/codebase-memory-mcp\"\n"
            "    startup_timeout_sec: 45\n",
        },
        {
            "extensions",
            "    type: stdio\n"
            "    cmd: \"/opt/codebase-memory-mcp\"\n"
            "    args: []\n"
            "    enabled: true\n",
            "extensions:\n"
            "  codebase-memory-mcp:\n"
            "    type: stdio\n"
            "    cmd: \"/opt/user-owned-mcp\"\n"
            "    args: [\"--custom\"]\n"
            "    enabled: true\n",
        },
        {
            "extensions",
            "    type: stdio\n"
            "    cmd: \"/opt/codebase-memory-mcp\"\n"
            "    args: []\n"
            "    enabled: true\n",
            "extensions:\n"
            "  codebase-memory-mcp:\n"
            "    type: stdio\n"
            "    cmd: \"/opt/codebase-memory-mcp\"\n"
            "    args: []\n"
            "    enabled: true\n"
            "    startup_timeout_sec: 45\n",
        },
    };

    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        yaml_fixture_t fixture;
        ASSERT_EQ(yaml_fixture_init(&fixture, cases[i].foreign), 0);
        ASSERT_EQ(cbm_yaml_upsert_owned_mapping_entry(fixture.path, cases[i].section,
                                                      "codebase-memory-mcp", cases[i].canonical),
                  CBM_YAML_IDENTITY_EDIT_FOREIGN);
        char *after_upsert = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(after_upsert);
        ASSERT_STR_EQ(after_upsert, cases[i].foreign);
        free(after_upsert);

        ASSERT_EQ(cbm_yaml_remove_owned_mapping_entry(fixture.path, cases[i].section,
                                                      "codebase-memory-mcp", cases[i].canonical),
                  CBM_YAML_IDENTITY_EDIT_FOREIGN);
        char *after_remove = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(after_remove);
        ASSERT_STR_EQ(after_remove, cases[i].foreign);
        free(after_remove);
        th_cleanup(fixture.dir);
    }
    PASS();
}

TEST(config_yaml_edit_mapping_remove_first_middle_last) {
    const char *initial = "extensions:\n"
                          "  first:\n"
                          "    type: one\n"
                          "  middle:\n"
                          "    type: two\n"
                          "  last:\n"
                          "    type: three\n"
                          "mode: keep\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);

    ASSERT_EQ(cbm_yaml_remove_mapping_entry(fixture.path, "extensions", "first"), 0);
    char *after_first = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after_first);
    ASSERT_STR_EQ(after_first, "extensions:\n"
                               "  middle:\n"
                               "    type: two\n"
                               "  last:\n"
                               "    type: three\n"
                               "mode: keep\n");

    ASSERT_EQ(cbm_yaml_remove_mapping_entry(fixture.path, "extensions", "middle"), 0);
    char *after_middle = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after_middle);
    ASSERT_STR_EQ(after_middle, "extensions:\n"
                                "  last:\n"
                                "    type: three\n"
                                "mode: keep\n");

    ASSERT_EQ(cbm_yaml_remove_mapping_entry(fixture.path, "extensions", "last"), 0);
    char *after_last = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after_last);
    ASSERT_STR_EQ(after_last, "extensions: {}\nmode: keep\n");

    free(after_last);
    free(after_middle);
    free(after_first);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_mapping_remove_last_preserves_section_comments) {
    const char *initial = "extensions: # preserve section comment\n"
                          "  # preserve user note\n"
                          "  codebase-memory:\n"
                          "    type: stdio\n"
                          "mode: keep\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);

    ASSERT_EQ(cbm_yaml_remove_mapping_entry(fixture.path, "extensions", "codebase-memory"), 0);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, "extensions: {} # preserve section comment\n"
                         "  # preserve user note\n"
                         "mode: keep\n");

    free(after);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_mapping_entry_is_explicit_managed_boundary) {
    const char *initial = "mcp_servers:\n"
                          "  codebase-memory:\n"
                          "    command: old-binary\n"
                          "    user_added_field: replaced-with-managed-entry\n"
                          "  user-server:\n"
                          "    command: preserve-me\n";
    const char *managed = "    command: new-binary\n"
                          "    args: []\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);

    ASSERT_EQ(
        cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", managed), 0);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    ASSERT_NOT_NULL(strstr(after, "  codebase-memory:\n"
                                  "    command: new-binary\n"
                                  "    args: []\n"));
    ASSERT_NULL(strstr(after, "user_added_field"));
    ASSERT_NOT_NULL(strstr(after, "  user-server:\n    command: preserve-me\n"));

    free(after);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_aider_absent_read_key) {
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    char *installed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(installed);
    ASSERT_STR_EQ(installed, "read:\n  - \"AGENTS.md\"\n");

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    char *idempotent = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(idempotent);
    ASSERT_STR_EQ(idempotent, installed);

    ASSERT_EQ(cbm_yaml_remove_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    char *removed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT_STR_EQ(removed, "");

    free(removed);
    free(idempotent);
    free(installed);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_aider_scalar_read_add_remove) {
    const char *initial = "model: sonnet\n"
                          "read: docs.md # keep read comment\n"
                          "color: auto\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    char *added = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(added);
    ASSERT_STR_EQ(added, "model: sonnet\n"
                         "read: [docs.md, \"AGENTS.md\"] # keep read comment\n"
                         "color: auto\n");

    ASSERT_EQ(cbm_yaml_remove_string_list_item(fixture.path, "read", "docs.md"), 0);
    char *one_left = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(one_left);
    ASSERT_STR_EQ(one_left, "model: sonnet\n"
                            "read: [\"AGENTS.md\"] # keep read comment\n"
                            "color: auto\n");

    ASSERT_EQ(cbm_yaml_remove_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    char *removed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT_STR_EQ(removed, "model: sonnet\ncolor: auto\n");

    free(removed);
    free(one_left);
    free(added);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_aider_flow_read_add_remove) {
    const char *initial = "read: [README.md, 'notes file.md'] # user list\n"
                          "model: local\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "docs:guide #1.md"), 0);
    char *added = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(added);
    ASSERT_STR_EQ(added, "read: [README.md, 'notes file.md', \"docs:guide #1.md\"] # user list\n"
                         "model: local\n");

    ASSERT_EQ(cbm_yaml_remove_string_list_item(fixture.path, "read", "notes file.md"), 0);
    char *removed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT_STR_EQ(removed, "read: [README.md, \"docs:guide #1.md\"] # user list\n"
                           "model: local\n");

    free(removed);
    free(added);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_aider_block_read_add_remove) {
    const char *initial = "read:\n"
                          "  - README.md\n"
                          "  # preserve list comment\n"
                          "  - \"docs/file.md\"\n"
                          "model: local\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    char *added = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(added);
    ASSERT_STR_EQ(added, "read:\n"
                         "  - README.md\n"
                         "  # preserve list comment\n"
                         "  - \"docs/file.md\"\n"
                         "  - \"AGENTS.md\"\n"
                         "model: local\n");

    ASSERT_EQ(cbm_yaml_remove_string_list_item(fixture.path, "read", "docs/file.md"), 0);
    char *removed_middle = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed_middle);
    ASSERT_STR_EQ(removed_middle, "read:\n"
                                  "  - README.md\n"
                                  "  # preserve list comment\n"
                                  "  - \"AGENTS.md\"\n"
                                  "model: local\n");

    ASSERT_EQ(cbm_yaml_remove_string_list_item(fixture.path, "read", "AGENTS.md"), 0);
    char *removed_last = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed_last);
    ASSERT_STR_EQ(removed_last, "read:\n"
                                "  - README.md\n"
                                "  # preserve list comment\n"
                                "model: local\n");

    free(removed_last);
    free(removed_middle);
    free(added);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_quotes_windows_path_safely) {
    const char *item = "C:\\Users\\Ada\\My \"Notes\": #1.md";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", item), 0);
    char *installed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(installed);
    ASSERT_STR_EQ(installed, "read:\n"
                             "  - \"C:\\\\Users\\\\Ada\\\\My \\\"Notes\\\": #1.md\"\n");

    ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", item), 0);
    char *idempotent = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(idempotent);
    ASSERT_STR_EQ(idempotent, installed);

    ASSERT_EQ(cbm_yaml_remove_string_list_item(fixture.path, "read", item), 0);
    char *removed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT_STR_EQ(removed, "");

    free(removed);
    free(idempotent);
    free(installed);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_mapping_ambiguity_fails_unchanged) {
    const char *cases[] = {
        "mcp_servers:\n  one:\n    command: one\nmcp_servers:\n",
        ("mcp_servers:\n  codebase-memory:\n    command: one\n"
         "  codebase-memory:\n    command: two\n"),
        "mcp_servers:\n   codebase-memory:\n    command: bad-indent\n",
        "mcp_servers:\n\tcodebase-memory:\n\t\tcommand: tabbed\n",
        "mcp_servers: &shared\n  other:\n    command: other\n",
        "mcp_servers:\n  <<: *defaults\n  other:\n    command: other\n",
        "mcp_servers: {codebase-memory: {command: codebase-memory-mcp}}\n",
        "mcp_servers:\n  other:\n    description: >\n      folded text\n",
    };
    const char *block = "    command: codebase-memory-mcp\n";

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        yaml_fixture_t fixture;
        ASSERT_EQ(yaml_fixture_init(&fixture, cases[i]), 0);
        ASSERT_EQ(
            cbm_yaml_upsert_mapping_entry(fixture.path, "mcp_servers", "codebase-memory", block),
            -1);
        char *after = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(after);
        ASSERT_STR_EQ(after, cases[i]);
        free(after);
        th_cleanup(fixture.dir);
    }

    yaml_fixture_t invalid_block;
    const char *safe = "mcp_servers:\n  other:\n    command: other\n";
    ASSERT_EQ(yaml_fixture_init(&invalid_block, safe), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(invalid_block.path, "mcp_servers", "codebase-memory",
                                            "    command: &shared\n"),
              -1);
    char *after_block = yaml_read_alloc(invalid_block.path);
    ASSERT_NOT_NULL(after_block);
    ASSERT_STR_EQ(after_block, safe);
    free(after_block);
    th_cleanup(invalid_block.dir);

    yaml_fixture_t comment_truncation;
    ASSERT_EQ(yaml_fixture_init(&comment_truncation, safe), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(comment_truncation.path, "mcp_servers",
                                            "codebase-memory",
                                            "    command: /tmp/tool # truncated\n"),
              -1);
    char *after_comment = yaml_read_alloc(comment_truncation.path);
    ASSERT_NOT_NULL(after_comment);
    ASSERT_STR_EQ(after_comment, safe);
    free(after_comment);
    th_cleanup(comment_truncation.dir);

    yaml_fixture_t quoted_hash;
    ASSERT_EQ(yaml_fixture_init(&quoted_hash, safe), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_entry(quoted_hash.path, "mcp_servers", "codebase-memory",
                                            "    command: \"/tmp/tool # literal\"\n"),
              0);
    char *after_quoted = yaml_read_alloc(quoted_hash.path);
    ASSERT_NOT_NULL(after_quoted);
    ASSERT_NOT_NULL(strstr(after_quoted, "    command: \"/tmp/tool # literal\"\n"));
    free(after_quoted);
    th_cleanup(quoted_hash.dir);
    PASS();
}

TEST(config_yaml_edit_list_ambiguity_fails_unchanged) {
    const char control_yaml[] = "read:\n  - good.md\n  # bad\x01"
                                "control\n";
    const char *cases[] = {
        "read: one.md\nread: two.md\n",
        "read: *defaults\nmodel: local\n",
        "read: |\n  one.md\n",
        "read: {path: one.md}\n",
        "read:\n  - &shared one.md\n",
        "read:\n   - three-space-indent.md\n",
        control_yaml,
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        yaml_fixture_t fixture;
        ASSERT_EQ(yaml_fixture_init(&fixture, cases[i]), 0);
        ASSERT_EQ(cbm_yaml_upsert_string_list_item(fixture.path, "read", "AGENTS.md"), -1);
        char *after = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(after);
        ASSERT_STR_EQ(after, cases[i]);
        free(after);
        th_cleanup(fixture.dir);
    }
    PASS();
}

static const char *const yaml_hook_sequence_path[] = {"hooks", "pre_llm_call"};
static const char yaml_hook_identity[] = "\"codebase-memory-mcp\"";
static const char yaml_hook_canonical_item[] = "- id: \"codebase-memory-mcp\"\n"
                                               "  type: \"command\"\n"
                                               "  command: \"/opt/Codebase Memory/bin/cbm\"\n";

TEST(config_yaml_edit_nested_sequence_preserves_siblings_comments_and_is_idempotent) {
    const char *initial = "# user header\n"
                          "hooks:\n"
                          "  session_start:\n"
                          "    - id: \"session-owner\"\n"
                          "      command: \"session-command\"\n"
                          "  pre_llm_call:\n"
                          "    # preserve same-event comment\n"
                          "    - id: \"other-hook\"\n"
                          "      command: \"other-command\"\n"
                          "  post_llm_call:\n"
                          "    - id: \"post-owner\"\n"
                          "      command: \"post-command\"\n"
                          "model: local\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U, "id",
                                                    yaml_hook_identity, yaml_hook_canonical_item),
              CBM_YAML_IDENTITY_EDIT_OK);
    char *installed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(installed);
    ASSERT_NOT_NULL(strstr(installed, "# user header\n"));
    ASSERT_NOT_NULL(strstr(installed, "    # preserve same-event comment\n"));
    ASSERT_NOT_NULL(strstr(installed, "    - id: \"other-hook\"\n"));
    ASSERT_NOT_NULL(strstr(installed, "  session_start:\n"));
    ASSERT_NOT_NULL(strstr(installed, "  post_llm_call:\n"));
    ASSERT_NOT_NULL(strstr(installed, "model: local\n"));
    ASSERT_NOT_NULL(strstr(installed, "    - id: \"codebase-memory-mcp\"\n"
                                      "      type: \"command\"\n"
                                      "      command: \"/opt/Codebase Memory/bin/cbm\"\n"));
    ASSERT_EQ(yaml_count_occurrences(installed, "id: \"codebase-memory-mcp\""), 1U);

    ASSERT_EQ(cbm_yaml_upsert_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U, "id",
                                                    yaml_hook_identity, yaml_hook_canonical_item),
              CBM_YAML_IDENTITY_EDIT_OK);
    char *second = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(second);
    ASSERT_STR_EQ(second, installed);
    free(second);
    free(installed);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_nested_sequence_creates_missing_file_section_and_list) {
    const char *initial_documents[] = {
        NULL,
        "model: local\n",
        "hooks:\n  session_start:\n    - id: \"keep\"\n      command: \"keep\"\n",
    };
    for (size_t i = 0U; i < sizeof(initial_documents) / sizeof(initial_documents[0]); i++) {
        yaml_fixture_t fixture;
        ASSERT_EQ(yaml_fixture_init(&fixture, initial_documents[i]), 0);
        ASSERT_EQ(cbm_yaml_upsert_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U,
                                                        "id", yaml_hook_identity,
                                                        yaml_hook_canonical_item),
                  CBM_YAML_IDENTITY_EDIT_OK);
        char *installed = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(installed);
        ASSERT_NOT_NULL(strstr(installed, "hooks:\n"));
        ASSERT_NOT_NULL(strstr(installed, "  pre_llm_call:\n"));
        ASSERT_NOT_NULL(strstr(installed, "    - id: \"codebase-memory-mcp\"\n"));
        ASSERT_EQ(yaml_count_occurrences(installed, "id: \"codebase-memory-mcp\""), 1U);
        if (initial_documents[i]) {
            if (strstr(initial_documents[i], "model:")) {
                ASSERT_NOT_NULL(strstr(installed, "model: local\n"));
            }
            if (strstr(initial_documents[i], "session_start:")) {
                ASSERT_NOT_NULL(strstr(installed, "  session_start:\n"));
                ASSERT_NOT_NULL(strstr(installed, "    - id: \"keep\"\n"));
            }
        }
        free(installed);
        th_cleanup(fixture.dir);
    }
    PASS();
}

TEST(config_yaml_edit_nested_sequence_preserves_crlf) {
    const char *initial = "hooks:\r\n"
                          "  pre_llm_call:\r\n"
                          "    - id: \"other\"\r\n"
                          "      command: \"other\"\r\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);
#ifndef _WIN32
    ASSERT_EQ(chmod(fixture.path, 0640), 0);
    struct stat before;
    ASSERT_EQ(stat(fixture.path, &before), 0);
#endif
    ASSERT_EQ(cbm_yaml_upsert_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U, "id",
                                                    yaml_hook_identity, yaml_hook_canonical_item),
              CBM_YAML_IDENTITY_EDIT_OK);
    char *after = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(after);
    for (const char *newline = strchr(after, '\n'); newline; newline = strchr(newline + 1U, '\n')) {
        ASSERT(newline > after && newline[-1] == '\r');
    }
    ASSERT_NOT_NULL(strstr(after, "    - id: \"codebase-memory-mcp\"\r\n"));
#ifndef _WIN32
    struct stat after_state;
    ASSERT_EQ(stat(fixture.path, &after_state), 0);
    ASSERT_EQ(after_state.st_uid, before.st_uid);
    ASSERT_EQ(after_state.st_gid, before.st_gid);
    ASSERT_EQ(after_state.st_mode & 07777, before.st_mode & 07777);
#endif
    free(after);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_nested_sequence_foreign_identity_is_preserved) {
    const char *cases[] = {
        "hooks:\n"
        "  pre_llm_call:\n"
        "    - id: \"codebase-memory-mcp\"\n"
        "      type: \"command\"\n"
        "      command: \"foreign\"\n",
        "hooks:\n"
        "  pre_llm_call:\n"
        "    - id: \"codebase-memory-mcp\"\n"
        "      type: \"command\"\n"
        "      command: \"/opt/Codebase Memory/bin/cbm\"\n"
        "      timeout: 30\n",
    };
    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        yaml_fixture_t fixture;
        ASSERT_EQ(yaml_fixture_init(&fixture, cases[i]), 0);
        ASSERT_EQ(cbm_yaml_upsert_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U,
                                                        "id", yaml_hook_identity,
                                                        yaml_hook_canonical_item),
                  CBM_YAML_IDENTITY_EDIT_FOREIGN);
        char *after_upsert = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(after_upsert);
        ASSERT_STR_EQ(after_upsert, cases[i]);
        free(after_upsert);
        ASSERT_EQ(cbm_yaml_remove_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U,
                                                        "id", yaml_hook_identity,
                                                        yaml_hook_canonical_item),
                  CBM_YAML_IDENTITY_EDIT_FOREIGN);
        char *after_remove = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(after_remove);
        ASSERT_STR_EQ(after_remove, cases[i]);
        free(after_remove);
        th_cleanup(fixture.dir);
    }
    PASS();
}

TEST(config_yaml_edit_nested_sequence_removes_only_exact_canonical_item) {
    const char *initial = "hooks:\n"
                          "  pre_llm_call:\n"
                          "    - id: \"other-hook\"\n"
                          "      command: \"other\"\n"
                          "    - id: \"codebase-memory-mcp\"\n"
                          "      type: \"command\"\n"
                          "      command: \"/opt/Codebase Memory/bin/cbm\"\n"
                          "  post_llm_call:\n"
                          "    - id: \"post\"\n"
                          "      command: \"post\"\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, initial), 0);
    ASSERT_EQ(cbm_yaml_remove_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U, "id",
                                                    yaml_hook_identity, yaml_hook_canonical_item),
              CBM_YAML_IDENTITY_EDIT_OK);
    char *removed = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT_NULL(strstr(removed, "id: \"codebase-memory-mcp\""));
    ASSERT_NOT_NULL(strstr(removed, "    - id: \"other-hook\"\n"));
    ASSERT_NOT_NULL(strstr(removed, "  post_llm_call:\n"));
    ASSERT_EQ(cbm_yaml_remove_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U, "id",
                                                    yaml_hook_identity, yaml_hook_canonical_item),
              CBM_YAML_IDENTITY_EDIT_OK);
    char *second = yaml_read_alloc(fixture.path);
    ASSERT_NOT_NULL(second);
    ASSERT_STR_EQ(second, removed);
    free(second);
    free(removed);
    th_cleanup(fixture.dir);
    PASS();
}

TEST(config_yaml_edit_nested_sequence_ambiguity_fails_byte_identically) {
    const char *cases[] = {
        "hooks:\n   pre_llm_call:\n    - id: \"bad-indent\"\n",
        "hooks:\n  pre_llm_call:\n    - id: \"one\"\nhooks:\n  keep: true\n",
        "hooks:\n  pre_llm_call:\n    - id: \"one\"\n  pre_llm_call:\n    - id: \"two\"\n",
        "shared: &shared\n  - id: \"other\"\nhooks:\n  pre_llm_call: *shared\n",
        "hooks: {pre_llm_call: []}\n",
        "hooks:\n  pre_llm_call: [{id: \"other\", command: \"other\"}]\n",
        "hooks:\n  pre_llm_call:\n    - id: \"unterminated\n",
        "hooks:\n  pre_llm_call:\n    - id: \"codebase-memory-mcp\"\n"
        "      command: \"one\"\n"
        "    - id: \"codebase-memory-mcp\"\n"
        "      command: \"two\"\n",
    };
    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        yaml_fixture_t fixture;
        ASSERT_EQ(yaml_fixture_init(&fixture, cases[i]), 0);
        ASSERT_EQ(cbm_yaml_upsert_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U,
                                                        "id", yaml_hook_identity,
                                                        yaml_hook_canonical_item),
                  CBM_YAML_IDENTITY_EDIT_ERROR);
        char *after = yaml_read_alloc(fixture.path);
        ASSERT_NOT_NULL(after);
        ASSERT_STR_EQ(after, cases[i]);
        free(after);
        th_cleanup(fixture.dir);
    }
    PASS();
}

#ifndef _WIN32
TEST(config_yaml_edit_nested_sequence_rejects_symlink_byte_identically) {
    const char *original = "hooks:\n  pre_llm_call:\n    - id: \"other\"\n";
    yaml_fixture_t fixture;
    ASSERT_EQ(yaml_fixture_init(&fixture, NULL), 0);
    char target[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(target, sizeof(target), "%s/target-hooks.yaml", fixture.dir) > 0);
    ASSERT_EQ(th_write_file(target, original), 0);
    ASSERT_EQ(symlink(target, fixture.path), 0);
    ASSERT_EQ(cbm_yaml_upsert_mapping_sequence_item(fixture.path, yaml_hook_sequence_path, 2U, "id",
                                                    yaml_hook_identity, yaml_hook_canonical_item),
              CBM_YAML_IDENTITY_EDIT_ERROR);
    char *after = yaml_read_alloc(target);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, original);
    free(after);
    struct stat link_state;
    ASSERT_EQ(lstat(fixture.path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    th_cleanup(fixture.dir);
    PASS();
}
#endif

SUITE(config_yaml_edit) {
    RUN_TEST(config_yaml_edit_serializes_two_editor_instances);
    RUN_TEST(config_yaml_edit_missing_target_appearance_fails_without_replace);
    RUN_TEST(config_yaml_edit_rejects_stale_content_and_cleans_temp);
    RUN_TEST(config_yaml_edit_rejects_stale_identity_with_same_content);
    RUN_TEST(config_yaml_edit_existing_target_swap_after_check_preserves_winner);
#ifndef _WIN32
    RUN_TEST(config_yaml_edit_lock_postcreate_verification_failure_preserves_unsafe_sidecar);
    RUN_TEST(config_yaml_edit_reuses_persistent_safe_lock_sidecar);
    RUN_TEST(config_yaml_edit_rejects_symlink_lock_sidecar);
    RUN_TEST(config_yaml_edit_rejects_hard_linked_lock_sidecar);
    RUN_TEST(config_yaml_edit_rejects_unsafe_mode_lock_sidecar);
#endif
    RUN_TEST(config_yaml_edit_rejects_non_regular_path);
#ifndef _WIN32
    RUN_TEST(config_yaml_edit_rejects_symlinks_without_touching_target);
    RUN_TEST(config_yaml_edit_rejects_dangling_symlink);
    RUN_TEST(config_yaml_edit_preserves_owner_group_and_mode);
    RUN_TEST(config_yaml_edit_rejects_hard_links_without_splitting_identity);
#endif
    RUN_TEST(config_yaml_edit_encodes_dynamic_scalars_safely);
    RUN_TEST(config_yaml_edit_rejects_newline_list_items);
    RUN_TEST(config_yaml_edit_absent_target_requires_safe_root_mapping);
    RUN_TEST(config_yaml_edit_rejects_semantic_target_key_aliases);
    RUN_TEST(config_yaml_edit_preserves_crlf_and_handles_no_final_newline);
    RUN_TEST(config_yaml_edit_hermes_mapping_lifecycle);
    RUN_TEST(config_yaml_edit_hermes_creates_missing_section);
    RUN_TEST(config_yaml_edit_goose_extensions_preserve_siblings);
    RUN_TEST(config_yaml_edit_owned_agent_mapping_installs_idempotently_and_removes_exact_state);
    RUN_TEST(config_yaml_edit_owned_agent_mapping_preserves_foreign_same_name_state);
    RUN_TEST(config_yaml_edit_mapping_remove_first_middle_last);
    RUN_TEST(config_yaml_edit_mapping_remove_last_preserves_section_comments);
    RUN_TEST(config_yaml_edit_mapping_entry_is_explicit_managed_boundary);
    RUN_TEST(config_yaml_edit_aider_absent_read_key);
    RUN_TEST(config_yaml_edit_aider_scalar_read_add_remove);
    RUN_TEST(config_yaml_edit_aider_flow_read_add_remove);
    RUN_TEST(config_yaml_edit_aider_block_read_add_remove);
    RUN_TEST(config_yaml_edit_quotes_windows_path_safely);
    RUN_TEST(config_yaml_edit_mapping_ambiguity_fails_unchanged);
    RUN_TEST(config_yaml_edit_list_ambiguity_fails_unchanged);
    RUN_TEST(config_yaml_edit_nested_sequence_preserves_siblings_comments_and_is_idempotent);
    RUN_TEST(config_yaml_edit_nested_sequence_creates_missing_file_section_and_list);
    RUN_TEST(config_yaml_edit_nested_sequence_preserves_crlf);
    RUN_TEST(config_yaml_edit_nested_sequence_foreign_identity_is_preserved);
    RUN_TEST(config_yaml_edit_nested_sequence_removes_only_exact_canonical_item);
    RUN_TEST(config_yaml_edit_nested_sequence_ambiguity_fails_byte_identically);
#ifndef _WIN32
    RUN_TEST(config_yaml_edit_nested_sequence_rejects_symlink_byte_identically);
#endif
}
