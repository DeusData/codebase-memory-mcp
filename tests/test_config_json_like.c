/*
 * test_config_json_like.c — Structure-preserving JSON/JSONC/JSON5 edits.
 *
 * This suite is intentionally standalone. The installation work that consumes
 * the editor can register it with the main runner alongside its build wiring.
 */
#include "test_framework.h"

#define CBM_JSON_LIKE_ENABLE_TEST_API 1
#include "../src/cli/config_json_like.h"
#define CBM_CONFIG_EDIT_PATH_ENABLE_TEST_API 1
#include "../src/cli/config_edit_path.h"
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#endif

typedef struct {
    char directory[512];
    char path[640];
} jl_fixture_t;

static int jl_fixture_open(jl_fixture_t *fixture) {
    snprintf(fixture->directory, sizeof(fixture->directory), "%s/cbm-json-like-XXXXXX",
             cbm_tmpdir());
    if (!cbm_mkdtemp(fixture->directory)) {
        return -1;
    }
    int written =
        snprintf(fixture->path, sizeof(fixture->path), "%s/config.json", fixture->directory);
    return written >= 0 && (size_t)written < sizeof(fixture->path) ? 0 : -1;
}

static void jl_fixture_close(jl_fixture_t *fixture) {
    cbm_dir_t *directory = cbm_opendir(fixture->directory);
    if (directory) {
        cbm_dirent_t *entry = NULL;
        while ((entry = cbm_readdir(directory)) != NULL) {
            if (strncmp(entry->name, "config.json.cbm.tmp.", strlen("config.json.cbm.tmp.")) == 0) {
                char temp_path[sizeof(fixture->path) + 96U];
                int written = snprintf(temp_path, sizeof(temp_path), "%s/%s", fixture->directory,
                                       entry->name);
                if (written >= 0 && (size_t)written < sizeof(temp_path)) {
                    (void)cbm_unlink(temp_path);
                }
            }
        }
        cbm_closedir(directory);
    }
    (void)cbm_unlink(fixture->path);
    (void)cbm_rmdir(fixture->directory);
}

static size_t jl_temp_file_count(const jl_fixture_t *fixture) {
    cbm_dir_t *directory = cbm_opendir(fixture->directory);
    if (!directory) {
        return SIZE_MAX;
    }
    size_t count = 0U;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(directory)) != NULL) {
        if (strncmp(entry->name, "config.json.cbm.tmp.", strlen("config.json.cbm.tmp.")) == 0) {
            count++;
        }
    }
    cbm_closedir(directory);
    return count;
}

static int jl_write(const char *path, const char *content) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file) {
        return -1;
    }
    size_t length = strlen(content);
    size_t written = fwrite(content, 1U, length, file);
    int close_result = fclose(file);
    return written == length && close_result == 0 ? 0 : -1;
}

static char *jl_read(const char *path) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file || fseek(file, 0, SEEK_END) != 0) {
        if (file) {
            fclose(file);
        }
        return NULL;
    }
    long size = ftell(file);
    if (size < 0 || fseek(file, 0, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }
    char *content = malloc((size_t)size + 1U);
    if (!content) {
        fclose(file);
        return NULL;
    }
    size_t read_count = fread(content, 1U, (size_t)size, file);
    int failed = ferror(file);
    fclose(file);
    if (read_count != (size_t)size || failed) {
        free(content);
        return NULL;
    }
    content[read_count] = '\0';
    return content;
}

static int jl_failed_unchanged(const char *path, const char *original,
                               const char *const *object_path, size_t path_len, const char *key,
                               const char *value) {
    if (jl_write(path, original) != 0) {
        return 0;
    }
    if (cbm_json_like_upsert_entry(path, object_path, path_len, key, value) == 0) {
        return 0;
    }
    char *after = jl_read(path);
    int unchanged = after && strcmp(after, original) == 0;
    free(after);
    return unchanged;
}

static size_t jl_occurrences(const char *text, const char *needle) {
    size_t count = 0;
    size_t needle_length = strlen(needle);
    while (needle_length > 0U && (text = strstr(text, needle)) != NULL) {
        count++;
        text += needle_length;
    }
    return count;
}

typedef struct {
    const char *content;
    const char *backup_path;
    bool replace_identity;
    int result;
} jl_precommit_change_t;

static void jl_change_before_commit(const char *path, void *context) {
    jl_precommit_change_t *change = context;
    if (change->replace_identity &&
        (!change->backup_path || cbm_rename_replace(path, change->backup_path) != 0)) {
        change->result = -1;
        return;
    }
    change->result = jl_write(path, change->content);
}

TEST(config_json_like_rejects_stale_content_and_cleans_temp) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    ASSERT_EQ(jl_write(fixture.path, "{\"keep\":1}\n"), 0);
    const char *root[] = {NULL};
    jl_precommit_change_t change = {
        .content = "{\"concurrent\":true}\n",
        .backup_path = NULL,
        .replace_identity = false,
        .result = -1,
    };
    cbm_json_like_set_precommit_hook_for_testing(jl_change_before_commit, &change);
    int result = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    cbm_json_like_set_precommit_hook_for_testing(NULL, NULL);

    ASSERT_EQ(change.result, 0);
    ASSERT_EQ(result, -1);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, "{\"concurrent\":true}\n");
    free(content);
    ASSERT_EQ(jl_temp_file_count(&fixture), 0U);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_rejects_stale_identity_with_same_content) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original = "{\"keep\":1}\n";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    char backup[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.json", fixture.directory) > 0);
    const char *root[] = {NULL};
    jl_precommit_change_t change = {
        .content = original,
        .backup_path = backup,
        .replace_identity = true,
        .result = -1,
    };
    cbm_json_like_set_precommit_hook_for_testing(jl_change_before_commit, &change);
    int result = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    cbm_json_like_set_precommit_hook_for_testing(NULL, NULL);

    ASSERT_EQ(change.result, 0);
    ASSERT_EQ(result, -1);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);
    ASSERT_EQ(jl_temp_file_count(&fixture), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_missing_target_race_does_not_replace_winner) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *root[] = {NULL};
    jl_precommit_change_t race = {
        .content = "{\"winner\":true}\n",
        .backup_path = NULL,
        .replace_identity = false,
        .result = -1,
    };
    cbm_json_like_set_prepublish_hook_for_testing(jl_change_before_commit, &race);
    int result = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    cbm_json_like_set_prepublish_hook_for_testing(NULL, NULL);
    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, "{\"winner\":true}\n");
    free(content);
    ASSERT_EQ(jl_temp_file_count(&fixture), 0U);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_existing_target_swap_after_check_preserves_winner) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original = "{\"keep\":1}\n";
    const char *winner = "{\"winner\":true}\n";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    char backup[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.json", fixture.directory) > 0);
    const char *root[] = {NULL};
    jl_precommit_change_t race = {
        .content = winner,
        .backup_path = backup,
        .replace_identity = true,
        .result = -1,
    };

    cbm_json_like_set_prepublish_hook_for_testing(jl_change_before_commit, &race);
    int result = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    cbm_json_like_set_prepublish_hook_for_testing(NULL, NULL);

    ASSERT_EQ(race.result, 0);
    ASSERT_EQ(result, -1);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, winner);
    free(content);
    ASSERT_EQ(jl_temp_file_count(&fixture), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_rejects_non_regular_path) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    ASSERT_EQ(cbm_mkdir(fixture.path), 0);
    const char *root[] = {NULL};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true"), -1);
    ASSERT_EQ(jl_temp_file_count(&fixture), 0U);
    ASSERT_EQ(cbm_rmdir(fixture.path), 0);
    jl_fixture_close(&fixture);
    PASS();
}

#ifndef _WIN32
/* Any editor temp file left anywhere in the fixture directory, whatever
 * document it was staged for: a link's target lives in the same directory. */
static size_t jl_stray_temp_count(const jl_fixture_t *fixture) {
    cbm_dir_t *directory = cbm_opendir(fixture->directory);
    if (!directory) {
        return SIZE_MAX;
    }
    size_t count = 0U;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(directory)) != NULL) {
        if (strstr(entry->name, ".cbm.tmp.")) {
            count++;
        }
    }
    cbm_closedir(directory);
    return count;
}

/* A link owned by someone else is never followed. A test cannot create a
 * foreign-owned link without root, so the observer moves instead (test seam):
 * the refusal touches neither the link nor its target and stages no temp file. */
TEST(config_json_like_rejects_foreign_owned_symlink_without_touching_target) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char target[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(target, sizeof(target), "%s/target.json", fixture.directory) > 0);
    const char *original = "{\"target\":true}\n";
    ASSERT_EQ(jl_write(target, original), 0);
    ASSERT_EQ(symlink(target, fixture.path), 0);
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(fixture.directory), 0);
    cbm_config_edit_path_set_invoking_uid_for_test((unsigned)geteuid() + 1U, 1);

    const char *root[] = {NULL};
    int upsert_rc = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    char reason[256];
    int refused = cbm_config_edit_path_refusal(fixture.path, reason, sizeof(reason));
    cbm_config_edit_path_set_invoking_uid_for_test(0U, 0);
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(upsert_rc, -1);
    ASSERT_EQ(refused, 1);
    ASSERT_NOT_NULL(strstr(reason, "not the invoking user"));
    struct stat link_state;
    ASSERT_EQ(lstat(fixture.path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    char *content = jl_read(target);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);
    ASSERT_EQ(jl_stray_temp_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    jl_fixture_close(&fixture);
    PASS();
}

/* A root process never follows a link a non-root user owns: a user-planted
 * link must not redirect a privileged writer. A real root run (CI containers)
 * demotes the link to an unprivileged owner first; elsewhere the observer moves. */
TEST(config_json_like_root_refuses_user_owned_symlink) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char target[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(target, sizeof(target), "%s/target.json", fixture.directory) > 0);
    const char *original = "{\"target\":true}\n";
    ASSERT_EQ(jl_write(target, original), 0);
    ASSERT_EQ(symlink(target, fixture.path), 0);
    if (geteuid() == 0 && lchown(fixture.path, 65534, 65534) != 0) {
        (void)cbm_unlink(fixture.path);
        (void)cbm_unlink(target);
        jl_fixture_close(&fixture);
        FAIL("lchown to an unprivileged owner failed");
    }
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(fixture.directory), 0);
    cbm_config_edit_path_set_invoking_uid_for_test(0U, 1);

    const char *root[] = {NULL};
    int upsert_rc = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    char reason[256];
    int refused = cbm_config_edit_path_refusal(fixture.path, reason, sizeof(reason));
    cbm_config_edit_path_set_invoking_uid_for_test(0U, 0);
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(upsert_rc, -1);
    ASSERT_EQ(refused, 1);
    ASSERT_NOT_NULL(strstr(reason, "root process"));
    struct stat link_state;
    ASSERT_EQ(lstat(fixture.path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    char *content = jl_read(target);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);
    ASSERT_EQ(jl_stray_temp_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_refuses_dangling_symlink) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char missing[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(missing, sizeof(missing), "%s/missing.json", fixture.directory) > 0);
    ASSERT_EQ(symlink(missing, fixture.path), 0);
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(fixture.directory), 0);

    const char *root[] = {NULL};
    int upsert_rc = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    char *document = NULL;
    size_t length = 0U;
    int read_rc = cbm_json_like_read_document(fixture.path, &document, &length);
    free(document);
    char reason[256];
    int refused = cbm_config_edit_path_refusal(fixture.path, reason, sizeof(reason));
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(upsert_rc, -1);
    ASSERT_EQ(read_rc, -1);
    ASSERT_EQ(refused, 1);
    ASSERT_NOT_NULL(strstr(reason, "dangling"));
    struct stat state;
    ASSERT_EQ(lstat(fixture.path, &state), 0);
    ASSERT(S_ISLNK(state.st_mode));
    ASSERT(lstat(missing, &state) != 0);
    ASSERT_EQ(jl_stray_temp_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_refuses_symlink_to_directory) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char sub[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(sub, sizeof(sub), "%s/sub", fixture.directory) > 0);
    ASSERT_EQ(cbm_mkdir(sub), 0);
    ASSERT_EQ(symlink(sub, fixture.path), 0);
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(fixture.directory), 0);

    const char *root[] = {NULL};
    int upsert_rc = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    char reason[256];
    int refused = cbm_config_edit_path_refusal(fixture.path, reason, sizeof(reason));
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(upsert_rc, -1);
    ASSERT_EQ(refused, 1);
    ASSERT_NOT_NULL(strstr(reason, "directory"));
    struct stat state;
    ASSERT_EQ(lstat(fixture.path, &state), 0);
    ASSERT(S_ISLNK(state.st_mode));
    ASSERT_EQ(jl_stray_temp_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_rmdir(sub), 0);
    jl_fixture_close(&fixture);
    PASS();
}

/* Without the agent-config writers' opt-in, a same-owner link is still
 * refused: the follow is never a property of every editor caller. */
TEST(config_json_like_refuses_symlink_without_opt_in) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char target[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(target, sizeof(target), "%s/target.json", fixture.directory) > 0);
    const char *original = "{\"target\":true}\n";
    ASSERT_EQ(jl_write(target, original), 0);
    ASSERT_EQ(symlink(target, fixture.path), 0);
    cbm_config_edit_path_follow_clear();

    const char *root[] = {NULL};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true"), -1);
    char reason[256];
    ASSERT_EQ(cbm_config_edit_path_refusal(fixture.path, reason, sizeof(reason)), 1);
    ASSERT_NOT_NULL(strstr(reason, "not followed by this command"));

    /* An opt-in for a different root does not cover this link either. */
    char elsewhere[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(elsewhere, sizeof(elsewhere), "%s/elsewhere", fixture.directory) > 0);
    ASSERT_EQ(cbm_mkdir(elsewhere), 0);
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(elsewhere), 0);
    int outside_rc = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    int outside_refused = cbm_config_edit_path_refusal(fixture.path, reason, sizeof(reason));
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(outside_rc, -1);
    ASSERT_EQ(outside_refused, 1);
    ASSERT_NOT_NULL(strstr(reason, "outside the opted-in configuration roots"));

    struct stat link_state;
    ASSERT_EQ(lstat(fixture.path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    char *content = jl_read(target);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);
    ASSERT_EQ(jl_stray_temp_count(&fixture), 0U);

    ASSERT_EQ(cbm_rmdir(elsewhere), 0);
    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    jl_fixture_close(&fixture);
    PASS();
}

/* The validated descriptor must be the very file stat(realpath) saw: a
 * target swapped between the two checks (seam) is refused, untouched. */
TEST(config_json_like_refuses_symlink_when_target_identity_skews) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char target[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(target, sizeof(target), "%s/target.json", fixture.directory) > 0);
    const char *original = "{\"target\":true}\n";
    ASSERT_EQ(jl_write(target, original), 0);
    ASSERT_EQ(symlink(target, fixture.path), 0);
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(fixture.directory), 0);
    cbm_config_edit_path_set_identity_skew_for_test(1);

    const char *root[] = {NULL};
    int upsert_rc = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    char reason[256];
    int refused = cbm_config_edit_path_refusal(fixture.path, reason, sizeof(reason));
    cbm_config_edit_path_set_identity_skew_for_test(0);
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(upsert_rc, -1);
    ASSERT_EQ(refused, 1);
    ASSERT_NOT_NULL(strstr(reason, "changed while it was being checked"));
    char *content = jl_read(target);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);
    ASSERT_EQ(jl_stray_temp_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    jl_fixture_close(&fixture);
    PASS();
}

/* The target's parent directory is pinned and must not be writable by others
 * (a sticky directory is fine): otherwise anyone could stage a swap there. */
TEST(config_json_like_refuses_symlink_with_world_writable_parent) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char dotfiles[sizeof(fixture.path) + 32U];
    char target[sizeof(fixture.path) + 64U];
    ASSERT(snprintf(dotfiles, sizeof(dotfiles), "%s/dotfiles", fixture.directory) > 0);
    ASSERT(snprintf(target, sizeof(target), "%s/target.json", dotfiles) > 0);
    ASSERT_EQ(cbm_mkdir(dotfiles), 0);
    const char *original = "{\"target\":true}\n";
    ASSERT_EQ(jl_write(target, original), 0);
    ASSERT_EQ(symlink(target, fixture.path), 0);
    ASSERT_EQ(chmod(dotfiles, 0777), 0);
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(fixture.directory), 0);

    const char *root[] = {NULL};
    int loose_rc = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    char reason[256];
    int loose_refused = cbm_config_edit_path_refusal(fixture.path, reason, sizeof(reason));
    ASSERT_EQ(chmod(dotfiles, 0755), 0);
    int tight_rc = cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true");
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(loose_rc, -1);
    ASSERT_EQ(loose_refused, 1);
    ASSERT_NOT_NULL(strstr(reason, "writable by others"));
    ASSERT_EQ(tight_rc, 0);
    char *content = jl_read(target);
    ASSERT_NOT_NULL(content);
    ASSERT_NOT_NULL(strstr(content, "\"owned\""));
    free(content);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    ASSERT_EQ(cbm_rmdir(dotfiles), 0);
    jl_fixture_close(&fixture);
    PASS();
}

typedef struct {
    char seen_path[640];
    size_t temps_beside_target;
    size_t calls;
} jl_follow_hook_state_t;

/* Pre-commit hook: the editor hands over the path it is about to publish —
 * for a followed link that must be the RESOLVED target, with the staged
 * temp file sitting right beside it, never beside the link. */
static void jl_follow_precommit_hook(const char *path, void *context) {
    jl_follow_hook_state_t *state = context;
    state->calls++;
    (void)snprintf(state->seen_path, sizeof(state->seen_path), "%s", path);
    char directory[640];
    (void)snprintf(directory, sizeof(directory), "%s", path);
    char *slash = strrchr(directory, '/');
    if (!slash) {
        return;
    }
    const char *base = slash + 1;
    char prefix[320];
    (void)snprintf(prefix, sizeof(prefix), "%s.cbm.tmp.", base);
    *slash = '\0';
    cbm_dir_t *handle = cbm_opendir(directory);
    if (!handle) {
        return;
    }
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(handle)) != NULL) {
        if (strncmp(entry->name, prefix, strlen(prefix)) == 0) {
            state->temps_beside_target++;
        }
    }
    cbm_closedir(handle);
}

/* Decision C (#1954): a symlink the invoking user owns, pointing at a regular
 * file the same user owns, is edited THROUGH once the writer opted in — the
 * link survives and still points at the same target, the temp file is staged
 * beside the target, and every edit is byte-identical to the same edit on a
 * plain file. */
TEST(config_json_like_follows_user_owned_symlink_in_place) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char target[sizeof(fixture.path) + 32U];
    char control[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(target, sizeof(target), "%s/target.json", fixture.directory) > 0);
    ASSERT(snprintf(control, sizeof(control), "%s/control.json", fixture.directory) > 0);
    const char *original = "{\n  // dotfiles\n  \"mcpServers\": {\n"
                           "    \"keep\": {\"command\": \"x\"}\n  }\n}\n";
    ASSERT_EQ(jl_write(target, original), 0);
    ASSERT_EQ(jl_write(control, original), 0);
    /* Relative, the way a dotfiles checkout links its files. */
    ASSERT_EQ(symlink("target.json", fixture.path), 0);
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(fixture.directory), 0);
    char target_real[CBM_CONFIG_EDIT_PATH_MAX];
    ASSERT_EQ(cbm_canonical_path(target, target_real, sizeof(target_real)), 1);
    jl_follow_hook_state_t hook_state = {0};
    cbm_json_like_set_precommit_hook_for_testing(jl_follow_precommit_hook, &hook_state);

    const char *path[] = {"mcpServers"};
    int through_rc =
        cbm_json_like_upsert_entry(fixture.path, path, 1U, "owned", "{\"command\":\"y\"}");
    cbm_json_like_set_precommit_hook_for_testing(NULL, NULL);
    int plain_rc = cbm_json_like_upsert_entry(control, path, 1U, "owned", "{\"command\":\"y\"}");
    if (through_rc != 0 || plain_rc != 0) {
        cbm_config_edit_path_follow_clear();
    }
    ASSERT_EQ(through_rc, 0);
    ASSERT_EQ(plain_rc, 0);
    ASSERT_EQ(hook_state.calls, 1U);
    ASSERT_STR_EQ(hook_state.seen_path, target_real);
    ASSERT_EQ(hook_state.temps_beside_target, 1U);
    struct stat link_state;
    ASSERT_EQ(lstat(fixture.path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    char link_value[256] = {0};
    ASSERT(readlink(fixture.path, link_value, sizeof(link_value) - 1U) > 0);
    ASSERT_STR_EQ(link_value, "target.json");
    char *through = jl_read(target);
    char *plain = jl_read(control);
    ASSERT_NOT_NULL(through);
    ASSERT_NOT_NULL(plain);
    ASSERT_STR_EQ(through, plain);
    ASSERT_NOT_NULL(strstr(through, "\"owned\""));
    ASSERT_NOT_NULL(strstr(through, "// dotfiles"));

    /* Reading through the link sees the target, and the compare-and-remove
     * form uninstall relies on works through the link as well. */
    char *document = NULL;
    size_t length = 0U;
    ASSERT_EQ(cbm_json_like_read_document(fixture.path, &document, &length), 0);
    ASSERT_NOT_NULL(document);
    ASSERT_STR_EQ(document, through);
    ASSERT_EQ(
        cbm_json_like_remove_entry_if_unchanged(fixture.path, path, 1U, "owned", document, length),
        0);
    ASSERT_EQ(cbm_json_like_remove_entry(control, path, 1U, "owned"), 0);
    free(document);
    free(through);
    free(plain);
    through = jl_read(target);
    plain = jl_read(control);
    ASSERT_NOT_NULL(through);
    ASSERT_NOT_NULL(plain);
    ASSERT_STR_EQ(through, plain);
    ASSERT_NULL(strstr(through, "\"owned\""));
    free(through);
    free(plain);
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(lstat(fixture.path, &link_state), 0);
    ASSERT(S_ISLNK(link_state.st_mode));
    ASSERT_EQ(jl_stray_temp_count(&fixture), 0U);

    ASSERT_EQ(cbm_unlink(fixture.path), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    ASSERT_EQ(cbm_unlink(control), 0);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_rejects_hard_link_without_splitting_identity) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    char alias[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(alias, sizeof(alias), "%s/alias.json", fixture.directory) > 0);
    const char *original = "{\"shared\":true}\n";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    ASSERT_EQ(link(fixture.path, alias), 0);

    const char *root[] = {NULL};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true"), -1);
    char *content = jl_read(alias);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);

    ASSERT_EQ(cbm_unlink(alias), 0);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_preserves_owner_group_and_mode) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    ASSERT_EQ(jl_write(fixture.path, "{\"keep\":1}\n"), 0);
    ASSERT_EQ(chmod(fixture.path, 0640), 0);
    struct stat before;
    ASSERT_EQ(stat(fixture.path, &before), 0);

    const char *root[] = {NULL};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "true"), 0);
    struct stat after;
    ASSERT_EQ(stat(fixture.path, &after), 0);
    ASSERT_EQ(after.st_uid, before.st_uid);
    ASSERT_EQ(after.st_gid, before.st_gid);
    ASSERT_EQ(after.st_mode & 07777, before.st_mode & 07777);

    jl_fixture_close(&fixture);
    PASS();
}
#endif

TEST(config_json_like_supports_bom_and_common_json5_whitespace) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original = "\xEF\xBB\xBF"
                           "{\x0C"
                           "theme:'dark',\x0B"
                           "// retained\xE2\x80\xA8"
                           "mcp:\xC2\xA0{servers:{}},\xE2\x80\xA9"
                           "}";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    const char *path[] = {"mcp", "servers"};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, path, 2U, "owned", "true"), 0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(memcmp(content, "\xEF\xBB\xBF", 3U) == 0);
    ASSERT(strstr(content, "// retained\xE2\x80\xA8") != NULL);
    ASSERT(strstr(content, "theme:'dark'") != NULL);
    ASSERT(strstr(content, "\"owned\": true") != NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_rejects_json5_decimal_escapes_byte_identical) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *root[] = {NULL};
    ASSERT(jl_failed_unchanged(fixture.path, "{bad:'\\8'}\n", root, 0U, "owned", "true"));
    ASSERT(jl_failed_unchanged(fixture.path, "{bad:'\\01'}\n", root, 0U, "owned", "true"));
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_fresh_strict_upsert_replace_remove) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *root[] = {NULL};

    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "{\"command\":\"cbm\"}"),
              0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, "{\n  \"owned\": {\"command\":\"cbm\"}\n}\n");
    free(content);

    ASSERT_EQ(jl_write(fixture.path, "{\"keep\":1,\"owned\":false}\n"), 0);
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, root, 0U, "owned", "[1,2]"), 0);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, "{\"keep\":1,\"owned\":[1,2]}\n");
    free(content);

    ASSERT_EQ(cbm_json_like_remove_entry(fixture.path, root, 0U, "owned"), 0);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "\"keep\":1") != NULL);
    ASSERT(strstr(content, "owned") == NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_preserves_jsonc_comments) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original = "{\n"
                           "  // user preference\n"
                           "  \"theme\": \"dark\",\n"
                           "  \"mcpServers\": {\n"
                           "    /* retained server */\n"
                           "    \"other\": {\"command\": \"other\"},\n"
                           "  },\n"
                           "}\n";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    const char *path[] = {"mcpServers"};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, path, 1U, "codebase-memory",
                                         "{\"command\":\"cbm\"}"),
              0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "// user preference") != NULL);
    ASSERT(strstr(content, "/* retained server */") != NULL);
    ASSERT(strstr(content, "\"theme\": \"dark\"") != NULL);
    ASSERT(strstr(content, "\"other\": {\"command\": \"other\"}") != NULL);
    ASSERT(strstr(content, "\"codebase-memory\": {\"command\":\"cbm\"}") != NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_openclaw_json5_nested_servers) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original =
        "{ theme: 'dark', mcp: { servers: { other: { command: 'other' }, }, }, }";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    const char *path[] = {"mcp", "servers"};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, path, 2U, "codebase-memory",
                                         "{\"command\":\"cbm\",\"args\":[]}"),
              0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "theme: 'dark'") != NULL);
    ASSERT(strstr(content, "other: { command: 'other' }") != NULL);
    ASSERT(strstr(content, "\"codebase-memory\"") != NULL);
    free(content);

    ASSERT_EQ(cbm_json_like_remove_entry(fixture.path, path, 2U, "codebase-memory"), 0);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "codebase-memory") == NULL);
    ASSERT(strstr(content, "theme: 'dark'") != NULL);
    ASSERT(strstr(content, "other: { command: 'other' }") != NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_creates_missing_nested_path) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    ASSERT_EQ(jl_write(fixture.path, "{\n  \"theme\": \"dark\"\n}\n"), 0);
    const char *path[] = {"mcp", "servers"};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, path, 2U, "codebase-memory",
                                         "{\"command\":\"cbm\"}"),
              0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "\"theme\": \"dark\"") != NULL);
    ASSERT(strstr(content, "\"mcp\": {") != NULL);
    ASSERT(strstr(content, "\"servers\": {") != NULL);
    ASSERT(strstr(content, "\"codebase-memory\": {\"command\":\"cbm\"}") != NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_rejects_duplicate_path_byte_identical) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *path[] = {"mcp"};
    const char *duplicate = "{\"mcp\":{}, 'mcp':{}}\n";
    ASSERT(jl_failed_unchanged(fixture.path, duplicate, path, 1U, "owned", "true"));

    const char *root[] = {NULL};
    const char *duplicate_entry = "{\"owned\":1, owned:2}\n";
    ASSERT(jl_failed_unchanged(fixture.path, duplicate_entry, root, 0U, "owned", "3"));
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_rejects_invalid_and_malformed_byte_identical) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *root[] = {NULL};
    ASSERT(jl_failed_unchanged(fixture.path, "{\"keep\":1}\n", root, 0U, "owned", "{\"broken\":}"));
    ASSERT(jl_failed_unchanged(fixture.path, "{\"keep\":1}\n", root, 0U, "owned", ".5"));
    ASSERT(jl_failed_unchanged(fixture.path, "{\"keep\":1 /* never closes", root, 0U, "owned",
                               "true"));
    ASSERT(jl_failed_unchanged(fixture.path, "{\"keep\":[1,2}\n", root, 0U, "owned", "true"));

    const char *path[] = {"mcp"};
    ASSERT(jl_failed_unchanged(fixture.path, "{\"mcp\":false}\n", path, 1U, "owned", "true"));
    ASSERT_EQ(jl_write(fixture.path, "{\"keep\":1}\n"), 0);
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, root, 0U, "bad\nkey", "true"), -1);
    ASSERT_EQ(cbm_json_like_upsert_entry(NULL, root, 0U, "owned", "true"), -1);
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, NULL, 0U, "owned", "true"), -1);
    const char *deep_path[64];
    for (size_t i = 0; i < sizeof(deep_path) / sizeof(deep_path[0]); i++) {
        deep_path[i] = "level";
    }
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, deep_path, 64U, "owned", "true"), -1);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, "{\"keep\":1}\n");
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_upsert_is_byte_idempotent) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    ASSERT_EQ(jl_write(fixture.path, "{\n  \"mcp\": {}\n}\n"), 0);
    const char *path[] = {"mcp"};
    const char *value = "{\"command\":\"cbm\",\"args\":[\"serve\"]}";
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, path, 1U, "owned", value), 0);
    char *first = jl_read(fixture.path);
    ASSERT_NOT_NULL(first);
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, path, 1U, "owned", value), 0);
    char *second = jl_read(fixture.path);
    ASSERT_NOT_NULL(second);
    ASSERT_STR_EQ(second, first);
    free(first);
    free(second);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_ignores_braces_and_comments_inside_strings) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original = "{\"note\":\"} // text /* text */ \\\" {\",\"mcp\":{\"servers\":{}}}\n";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    const char *path[] = {"mcp", "servers"};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, path, 2U, "owned", "{\"command\":\"cbm\"}"),
              0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "} // text /* text */ \\\" {") != NULL);
    ASSERT(strstr(content, "\"owned\": {\"command\":\"cbm\"}") != NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_handles_trailing_commas) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original = "{\n  mcp: {\n    servers: {\n      other: [1, 2,],\n    },\n  },\n}\n";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    const char *path[] = {"mcp", "servers"};
    ASSERT_EQ(cbm_json_like_upsert_entry(fixture.path, path, 2U, "owned", "true"), 0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "other: [1, 2,]") != NULL);
    ASSERT(strstr(content, "\"owned\": true,") != NULL);
    free(content);
    ASSERT_EQ(cbm_json_like_remove_entry(fixture.path, path, 2U, "owned"), 0);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "owned") == NULL);
    ASSERT(strstr(content, "other: [1, 2,]") != NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_removes_first_middle_last_and_only) {
    static const char *inputs[] = {
        "{\"owned\":1,\"b\":2,\"c\":3}",
        "{\"a\":1,\"owned\":2,\"c\":3}",
        "{\"a\":1,\"b\":2,\"owned\":3}",
        "{\"owned\":1}",
    };
    static const char *required[] = {"\"b\":2", "\"a\":1", "\"b\":2", "{"};
    const char *root[] = {NULL};

    for (size_t i = 0; i < sizeof(inputs) / sizeof(inputs[0]); i++) {
        jl_fixture_t fixture;
        ASSERT_EQ(jl_fixture_open(&fixture), 0);
        ASSERT_EQ(jl_write(fixture.path, inputs[i]), 0);
        ASSERT_EQ(cbm_json_like_remove_entry(fixture.path, root, 0U, "owned"), 0);
        char *content = jl_read(fixture.path);
        ASSERT_NOT_NULL(content);
        ASSERT(strstr(content, "owned") == NULL);
        ASSERT(strstr(content, required[i]) != NULL);
        ASSERT_EQ(cbm_json_like_remove_entry(fixture.path, root, 0U, "owned"), 0);
        free(content);
        jl_fixture_close(&fixture);
    }
    PASS();
}

TEST(config_json_like_removal_preserves_comments_and_siblings) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original =
        "{\n  \"first\": 1,\n  /* keep this comment */\n  \"owned\": 2, // keep this too\n"
        "  \"last\": 3\n}\n";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    const char *root[] = {NULL};
    ASSERT_EQ(cbm_json_like_remove_entry(fixture.path, root, 0U, "owned"), 0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "owned") == NULL);
    ASSERT(strstr(content, "/* keep this comment */") != NULL);
    ASSERT(strstr(content, "// keep this too") != NULL);
    ASSERT(strstr(content, "\"first\": 1") != NULL);
    ASSERT(strstr(content, "\"last\": 3") != NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_top_level_array_unique_string) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original = "{\n"
                           "  theme: 'dark',\n"
                           "  instructions: [\n"
                           "    '~/rules/other.md',\n"
                           "    // keep this user comment\n"
                           "  ],\n"
                           "}\n";
    const char *owned = "~/.config/kilo/rules/codebase-memory-mcp.md";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    ASSERT_EQ(cbm_json_like_add_unique_string(fixture.path, "instructions", owned), 0);
    char *first = jl_read(fixture.path);
    ASSERT_NOT_NULL(first);
    ASSERT(strstr(first, "// keep this user comment") != NULL);
    ASSERT(strstr(first, "'~/rules/other.md'") != NULL);
    ASSERT_EQ(jl_occurrences(first, owned), 1U);

    ASSERT_EQ(cbm_json_like_add_unique_string(fixture.path, "instructions", owned), 0);
    char *second = jl_read(fixture.path);
    ASSERT_NOT_NULL(second);
    ASSERT_STR_EQ(second, first);
    free(first);
    free(second);

    ASSERT_EQ(cbm_json_like_remove_string(fixture.path, "instructions", owned), 0);
    char *removed = jl_read(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT(strstr(removed, owned) == NULL);
    ASSERT(strstr(removed, "// keep this user comment") != NULL);
    ASSERT(strstr(removed, "'~/rules/other.md'") != NULL);
    free(removed);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_top_level_array_create_escape_and_fail_closed) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *owned = "C:\\rules\\\"main\".md";
    ASSERT_EQ(cbm_json_like_add_unique_string(fixture.path, "instructions", owned), 0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "\"instructions\": [") != NULL);
    ASSERT(strstr(content, "C:") != NULL);
    ASSERT(strstr(content, "rules") != NULL);
    free(content);
    ASSERT_EQ(cbm_json_like_remove_string(fixture.path, "instructions", owned), 0);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "rules") == NULL);
    free(content);

    const char *duplicate = "{instructions:['owned', \"owned\",], keep:true}\n";
    ASSERT_EQ(jl_write(fixture.path, duplicate), 0);
    ASSERT_EQ(cbm_json_like_add_unique_string(fixture.path, "instructions", "owned"), -1);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, duplicate);
    free(content);
    ASSERT_EQ(cbm_json_like_remove_string(fixture.path, "instructions", "owned"), -1);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, duplicate);
    free(content);

    const char *wrong_type = "{instructions:false, keep:true}\n";
    ASSERT_EQ(jl_write(fixture.path, wrong_type), 0);
    ASSERT_EQ(cbm_json_like_add_unique_string(fixture.path, "instructions", "owned"), -1);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, wrong_type);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_nested_array_creates_missing_path_and_escapes) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *path[] = {"agents", "defaults", "compaction"};
    const char *owned = "Session \"memory\"\\path\nsection";

    ASSERT_EQ(cbm_json_like_add_unique_string_at_path(fixture.path, path, 3U,
                                                      "postCompactionSections", owned),
              0);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "\"agents\": {") != NULL);
    ASSERT(strstr(content, "\"defaults\": {") != NULL);
    ASSERT(strstr(content, "\"compaction\": {") != NULL);
    ASSERT(strstr(content, "\"postCompactionSections\": [") != NULL);
    ASSERT(strstr(content, "Session \\\"memory\\\"\\\\path\\nsection") != NULL);
    free(content);

    ASSERT_EQ(cbm_json_like_remove_string_at_path(fixture.path, path, 3U, "postCompactionSections",
                                                  owned),
              0);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT(strstr(content, "postCompactionSections") != NULL);
    ASSERT(strstr(content, "Session") == NULL);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_nested_array_preserves_jsonc_and_is_idempotent) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *original = "{\n"
                           "  // retain root comment\n"
                           "  theme: 'dark',\n"
                           "  options: {\n"
                           "    keep: true,\n"
                           "    context_paths: [\n"
                           "      'memory.md.bak',\n"
                           "      // retain array comment\n"
                           "    ],\n"
                           "  },\n"
                           "}\n";
    const char *path[] = {"options"};
    const char *owned = "memory.md";
    ASSERT_EQ(jl_write(fixture.path, original), 0);

    ASSERT_EQ(
        cbm_json_like_add_unique_string_at_path(fixture.path, path, 1U, "context_paths", owned), 0);
    char *first = jl_read(fixture.path);
    ASSERT_NOT_NULL(first);
    ASSERT(strstr(first, "// retain root comment") != NULL);
    ASSERT(strstr(first, "// retain array comment") != NULL);
    ASSERT(strstr(first, "theme: 'dark'") != NULL);
    ASSERT(strstr(first, "keep: true") != NULL);
    ASSERT(strstr(first, "'memory.md.bak',") != NULL);
    ASSERT_EQ(jl_occurrences(first, "\"memory.md\""), 1U);

    ASSERT_EQ(
        cbm_json_like_add_unique_string_at_path(fixture.path, path, 1U, "context_paths", owned), 0);
    char *second = jl_read(fixture.path);
    ASSERT_NOT_NULL(second);
    ASSERT_STR_EQ(second, first);
    free(first);
    free(second);

    ASSERT_EQ(cbm_json_like_remove_string_at_path(fixture.path, path, 1U, "context_paths", owned),
              0);
    char *removed = jl_read(fixture.path);
    ASSERT_NOT_NULL(removed);
    ASSERT(strstr(removed, "\"memory.md\"") == NULL);
    ASSERT(strstr(removed, "'memory.md.bak'") != NULL);
    ASSERT(strstr(removed, "// retain array comment") != NULL);
    ASSERT(strstr(removed, "keep: true") != NULL);
    free(removed);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_nested_array_fails_closed_on_ambiguous_or_invalid_paths) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *path[] = {"options"};
    const char *wrong_type = "{options:false, keep:true}\n";
    ASSERT_EQ(jl_write(fixture.path, wrong_type), 0);
    ASSERT_EQ(
        cbm_json_like_add_unique_string_at_path(fixture.path, path, 1U, "context_paths", "owned"),
        -1);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, wrong_type);
    free(content);

    const char *duplicate_path = "{options:{context_paths:[]}, options:{keep:true}}\n";
    ASSERT_EQ(jl_write(fixture.path, duplicate_path), 0);
    ASSERT_EQ(
        cbm_json_like_add_unique_string_at_path(fixture.path, path, 1U, "context_paths", "owned"),
        -1);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, duplicate_path);
    free(content);

    const char *duplicate_array =
        "{options:{context_paths:['owned'], context_paths:[]}, keep:true}\n";
    ASSERT_EQ(jl_write(fixture.path, duplicate_array), 0);
    ASSERT_EQ(cbm_json_like_remove_string_at_path(fixture.path, path, 1U, "context_paths", "owned"),
              -1);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, duplicate_array);
    free(content);

    ASSERT_EQ(
        cbm_json_like_add_unique_string_at_path(fixture.path, NULL, 1U, "context_paths", "owned"),
        -1);
    const char *null_path[] = {NULL};
    ASSERT_EQ(cbm_json_like_add_unique_string_at_path(fixture.path, null_path, 1U, "context_paths",
                                                      "owned"),
              -1);
    const char *control_path[] = {"bad\npath"};
    ASSERT_EQ(cbm_json_like_remove_string_at_path(fixture.path, control_path, 1U, "context_paths",
                                                  "owned"),
              -1);
    const char *deep_path[64];
    for (size_t i = 0; i < sizeof(deep_path) / sizeof(deep_path[0]); i++) {
        deep_path[i] = "level";
    }
    ASSERT_EQ(cbm_json_like_add_unique_string_at_path(fixture.path, deep_path, 64U, "context_paths",
                                                      "owned"),
              -1);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, duplicate_array);
    free(content);
    jl_fixture_close(&fixture);
    PASS();
}

#ifndef _WIN32
TEST(config_json_like_nested_array_rejects_foreign_symlink_and_hardlink) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *path[] = {"options"};
    const char *original = "{options:{context_paths:[]}}\n";
    char target[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(target, sizeof(target), "%s/target.json", fixture.directory) > 0);
    ASSERT_EQ(jl_write(target, original), 0);
    ASSERT_EQ(symlink(target, fixture.path), 0);
    /* Foreign-owned link (observer moved by the test seam): still refused. */
    ASSERT_EQ(cbm_config_edit_path_follow_add_root(fixture.directory), 0);
    cbm_config_edit_path_set_invoking_uid_for_test((unsigned)geteuid() + 1U, 1);
    int add_rc =
        cbm_json_like_add_unique_string_at_path(fixture.path, path, 1U, "context_paths", "owned");
    char *workspace = (char *)(uintptr_t)1U;
    int get_rc =
        cbm_json_like_get_string_at_path(fixture.path, path, 1U, "context_paths", &workspace);
    cbm_config_edit_path_set_invoking_uid_for_test(0U, 0);
    cbm_config_edit_path_follow_clear();
    ASSERT_EQ(add_rc, -1);
    ASSERT_EQ(get_rc, -1);
    ASSERT_NULL(workspace);
    char *content = jl_read(target);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);
    ASSERT_EQ(cbm_unlink(fixture.path), 0);

    ASSERT_EQ(jl_write(fixture.path, original), 0);
    char alias[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(alias, sizeof(alias), "%s/alias.json", fixture.directory) > 0);
    ASSERT_EQ(link(fixture.path, alias), 0);
    ASSERT_EQ(cbm_json_like_remove_string_at_path(fixture.path, path, 1U, "context_paths", "owned"),
              -1);
    workspace = (char *)(uintptr_t)1U;
    ASSERT_EQ(cbm_json_like_get_string_at_path(fixture.path, path, 1U, "context_paths", &workspace),
              -1);
    ASSERT_NULL(workspace);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);
    ASSERT_EQ(cbm_unlink(alias), 0);
    ASSERT_EQ(cbm_unlink(target), 0);
    jl_fixture_close(&fixture);
    PASS();
}
#endif

TEST(config_json_like_nested_array_rejects_precommit_content_and_identity_races) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *path[] = {"options"};
    const char *original = "{options:{context_paths:['owned']}, keep:true}\n";
    ASSERT_EQ(jl_write(fixture.path, original), 0);
    jl_precommit_change_t content_change = {
        .content = "{concurrent:true}\n",
        .backup_path = NULL,
        .replace_identity = false,
        .result = -1,
    };
    cbm_json_like_set_precommit_hook_for_testing(jl_change_before_commit, &content_change);
    int result =
        cbm_json_like_add_unique_string_at_path(fixture.path, path, 1U, "context_paths", "new");
    cbm_json_like_set_precommit_hook_for_testing(NULL, NULL);
    ASSERT_EQ(content_change.result, 0);
    ASSERT_EQ(result, -1);
    char *content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, "{concurrent:true}\n");
    free(content);
    ASSERT_EQ(jl_temp_file_count(&fixture), 0U);

    ASSERT_EQ(jl_write(fixture.path, original), 0);
    char backup[sizeof(fixture.path) + 32U];
    ASSERT(snprintf(backup, sizeof(backup), "%s/original.json", fixture.directory) > 0);
    jl_precommit_change_t identity_change = {
        .content = original,
        .backup_path = backup,
        .replace_identity = true,
        .result = -1,
    };
    cbm_json_like_set_precommit_hook_for_testing(jl_change_before_commit, &identity_change);
    result = cbm_json_like_remove_string_at_path(fixture.path, path, 1U, "context_paths", "owned");
    cbm_json_like_set_precommit_hook_for_testing(NULL, NULL);
    ASSERT_EQ(identity_change.result, 0);
    ASSERT_EQ(result, -1);
    content = jl_read(fixture.path);
    ASSERT_NOT_NULL(content);
    ASSERT_STR_EQ(content, original);
    free(content);
    ASSERT_EQ(jl_temp_file_count(&fixture), 0U);
    ASSERT_EQ(cbm_unlink(backup), 0);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_nested_string_lookup_decodes_json5_workspace) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *document =
        "{agents:{defaults:{workspace:'~\\/Open\\x43law\\u0020work\\\'space'}}}\n";
    const char *path[] = {"agents", "defaults"};
    ASSERT_EQ(jl_write(fixture.path, document), 0);

    char *workspace = NULL;
    ASSERT_EQ(cbm_json_like_get_string_at_path(fixture.path, path, 2U, "workspace", &workspace), 0);
    ASSERT_NOT_NULL(workspace);
    ASSERT_STR_EQ(workspace, "~/OpenClaw work'space");
    free(workspace);
    jl_fixture_close(&fixture);
    PASS();
}

TEST(config_json_like_nested_string_lookup_distinguishes_missing_and_fails_closed) {
    jl_fixture_t fixture;
    ASSERT_EQ(jl_fixture_open(&fixture), 0);
    const char *path[] = {"agents", "defaults"};
    char *workspace = (char *)(uintptr_t)1U;
    ASSERT_EQ(cbm_json_like_get_string_at_path(fixture.path, path, 2U, "workspace", &workspace), 1);
    ASSERT_NULL(workspace);

    const char *missing = "{agents:{defaults:{keep:true}}}\n";
    ASSERT_EQ(jl_write(fixture.path, missing), 0);
    workspace = (char *)(uintptr_t)1U;
    ASSERT_EQ(cbm_json_like_get_string_at_path(fixture.path, path, 2U, "workspace", &workspace), 1);
    ASSERT_NULL(workspace);

    const char *non_string = "{agents:{defaults:{workspace:false}}}\n";
    ASSERT_EQ(jl_write(fixture.path, non_string), 0);
    workspace = (char *)(uintptr_t)1U;
    ASSERT_EQ(cbm_json_like_get_string_at_path(fixture.path, path, 2U, "workspace", &workspace),
              -1);
    ASSERT_NULL(workspace);

    const char *duplicate_key = "{agents:{defaults:{workspace:'one', workspace:'two'}}}\n";
    ASSERT_EQ(jl_write(fixture.path, duplicate_key), 0);
    workspace = (char *)(uintptr_t)1U;
    ASSERT_EQ(cbm_json_like_get_string_at_path(fixture.path, path, 2U, "workspace", &workspace),
              -1);
    ASSERT_NULL(workspace);

    const char *duplicate_path =
        "{agents:{defaults:{workspace:'one'}}, agents:{defaults:{workspace:'two'}}}\n";
    ASSERT_EQ(jl_write(fixture.path, duplicate_path), 0);
    workspace = (char *)(uintptr_t)1U;
    ASSERT_EQ(cbm_json_like_get_string_at_path(fixture.path, path, 2U, "workspace", &workspace),
              -1);
    ASSERT_NULL(workspace);
    jl_fixture_close(&fixture);
    PASS();
}

SUITE(config_json_like) {
    RUN_TEST(config_json_like_rejects_stale_content_and_cleans_temp);
    RUN_TEST(config_json_like_rejects_stale_identity_with_same_content);
    RUN_TEST(config_json_like_missing_target_race_does_not_replace_winner);
    RUN_TEST(config_json_like_existing_target_swap_after_check_preserves_winner);
    RUN_TEST(config_json_like_rejects_non_regular_path);
#ifndef _WIN32
    RUN_TEST(config_json_like_rejects_foreign_owned_symlink_without_touching_target);
    RUN_TEST(config_json_like_root_refuses_user_owned_symlink);
    RUN_TEST(config_json_like_refuses_dangling_symlink);
    RUN_TEST(config_json_like_refuses_symlink_to_directory);
    RUN_TEST(config_json_like_refuses_symlink_without_opt_in);
    RUN_TEST(config_json_like_refuses_symlink_when_target_identity_skews);
    RUN_TEST(config_json_like_refuses_symlink_with_world_writable_parent);
    RUN_TEST(config_json_like_follows_user_owned_symlink_in_place);
    RUN_TEST(config_json_like_rejects_hard_link_without_splitting_identity);
    RUN_TEST(config_json_like_preserves_owner_group_and_mode);
#endif
    RUN_TEST(config_json_like_supports_bom_and_common_json5_whitespace);
    RUN_TEST(config_json_like_rejects_json5_decimal_escapes_byte_identical);
    RUN_TEST(config_json_like_fresh_strict_upsert_replace_remove);
    RUN_TEST(config_json_like_preserves_jsonc_comments);
    RUN_TEST(config_json_like_openclaw_json5_nested_servers);
    RUN_TEST(config_json_like_creates_missing_nested_path);
    RUN_TEST(config_json_like_rejects_duplicate_path_byte_identical);
    RUN_TEST(config_json_like_rejects_invalid_and_malformed_byte_identical);
    RUN_TEST(config_json_like_upsert_is_byte_idempotent);
    RUN_TEST(config_json_like_ignores_braces_and_comments_inside_strings);
    RUN_TEST(config_json_like_handles_trailing_commas);
    RUN_TEST(config_json_like_removes_first_middle_last_and_only);
    RUN_TEST(config_json_like_removal_preserves_comments_and_siblings);
    RUN_TEST(config_json_like_top_level_array_unique_string);
    RUN_TEST(config_json_like_top_level_array_create_escape_and_fail_closed);
    RUN_TEST(config_json_like_nested_array_creates_missing_path_and_escapes);
    RUN_TEST(config_json_like_nested_array_preserves_jsonc_and_is_idempotent);
    RUN_TEST(config_json_like_nested_array_fails_closed_on_ambiguous_or_invalid_paths);
#ifndef _WIN32
    RUN_TEST(config_json_like_nested_array_rejects_foreign_symlink_and_hardlink);
#endif
    RUN_TEST(config_json_like_nested_array_rejects_precommit_content_and_identity_races);
    RUN_TEST(config_json_like_nested_string_lookup_decodes_json5_workspace);
    RUN_TEST(config_json_like_nested_string_lookup_distinguishes_missing_and_fails_closed);
}
