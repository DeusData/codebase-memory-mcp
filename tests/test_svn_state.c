#include "../src/watcher/svn_state.h"
#include "../src/foundation/compat.h"
#include "svn_test_helpers.h"
#include "test_framework.h"
#include "test_helpers.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int parse_status(const char *root, const char *path,
                        cbm_svn_observation_t *observation) {
    char xml[4096];
    int n = snprintf(xml, sizeof(xml),
                     "<?xml version=\"1.0\"?>\n"
                     "<status><target path=\"%s\"><entry path=\"%s\">"
                     "<wc-status item=\"modified\" props=\"none\" revision=\"7\"/>"
                     "</entry></target></status>",
                     root, path);
    if (n < 0 || (size_t)n >= sizeof(xml)) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }

    FILE *stream = tmpfile();
    if (!stream) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    fputs(xml, stream);
    rewind(stream);
    int result = cbm_svn_parse_status_stream(stream, root, observation);
    fclose(stream);
    return result;
}

TEST(content_change_updates_observation_without_status_change) {
    char *root = th_mktempdir("cbm_svn_state");
    ASSERT_NOT_NULL(root);

    char root_copy[1024];
    char path[1024];
    snprintf(root_copy, sizeof(root_copy), "%s", root);
    snprintf(path, sizeof(path), "%s/source.c", root_copy);
    ASSERT_EQ(th_write_file(path, "int value = 1;\n"), 0);

    cbm_svn_observation_t before = {0};
    ASSERT_EQ(parse_status(root_copy, path, &before), CBM_SVN_PROBE_OK);

    ASSERT_EQ(th_write_file(path, "int value = 2;\n"), 0);
    cbm_svn_observation_t after = {0};
    ASSERT_EQ(parse_status(root_copy, path, &after), CBM_SVN_PROBE_OK);

    ASSERT_EQ(before.semantic_signature, after.semantic_signature);
    ASSERT_NEQ(before.content_signature, after.content_signature);
    ASSERT_TRUE(before.has_local_changes);
    ASSERT_TRUE(after.has_local_changes);
    ASSERT_EQ(before.entry_count, 1);
    ASSERT_EQ(after.entry_count, 1);

    th_cleanup(root_copy);
    PASS();
}

static int parse_xml(const char *root, const char *xml, cbm_svn_observation_t *observation) {
    FILE *stream = tmpfile();
    if (!stream) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    fputs(xml, stream);
    rewind(stream);
    int result = cbm_svn_parse_status_stream(stream, root, observation);
    fclose(stream);
    return result;
}

TEST(parser_rejects_unsafe_xml) {
    char *root = th_mktempdir("cbm_svn_xml");
    ASSERT_NOT_NULL(root);
    char root_copy[1024];
    snprintf(root_copy, sizeof(root_copy), "%s", root);

    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_xml(root_copy,
                        "<?xml version=\"1.0\"?><!DOCTYPE status [<!ENTITY x SYSTEM "
                        "\"file:///etc/passwd\">]><status><target path=\".\"/></status>",
                        &observation),
              CBM_SVN_PROBE_UNCERTAIN);
    ASSERT_EQ(parse_xml(root_copy,
                        "<?xml version=\"1.0\"?><status><target path=\".\"><entry "
                        "path=\"source.c\"><wc-status item=\"modified\"/>",
                        &observation),
              CBM_SVN_PROBE_UNCERTAIN);

    th_cleanup(root_copy);
    PASS();
}

TEST(clean_properties_and_externals_are_not_local_changes) {
    char *root = th_mktempdir("cbm_svn_clean_state");
    ASSERT_NOT_NULL(root);
    char xml[4096];
    snprintf(xml, sizeof(xml),
             "<?xml version=\"1.0\"?><status><target path=\"%s\">"
             "<entry path=\"%s\"><wc-status item=\"normal\" props=\"normal\" "
             "revision=\"4\"/></entry>"
             "</target></status>",
             root, root);
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_xml(root, xml, &observation), CBM_SVN_PROBE_OK);
    ASSERT_FALSE(observation.has_local_changes);
    ASSERT_EQ(observation.entry_count, 1);

    snprintf(xml, sizeof(xml),
             "<?xml version=\"1.0\"?><status><target path=\"%s\">"
             "<entry path=\"%s/external\"><wc-status item=\"external\" props=\"none\"/></entry>"
             "</target></status>",
             root, root);
    memset(&observation, 0, sizeof(observation));
    ASSERT_EQ(parse_xml(root, xml, &observation), CBM_SVN_PROBE_OK);
    ASSERT_FALSE(observation.has_local_changes);
    th_cleanup(root);
    PASS();
}

TEST(discovery_excluded_candidate_directory_is_not_hashed) {
    char *root = th_mktempdir("cbm_svn_ignored_state");
    ASSERT_NOT_NULL(root);
    char directory[1024];
    char source[1024];
    snprintf(directory, sizeof(directory), "%s/node_modules", root);
    ASSERT_EQ(cbm_mkdir(directory), 0);
    snprintf(source, sizeof(source), "%s/ignored.js", directory);
    ASSERT_EQ(th_write_file(source, "function Hidden() {}\n"), 0);

    char xml[4096];
    snprintf(xml, sizeof(xml),
             "<?xml version=\"1.0\"?><status><target path=\"%s\">"
             "<entry path=\"%s\"><wc-status item=\"ignored\" props=\"none\"/></entry>"
             "</target></status>",
             root, directory);
    cbm_svn_observation_t observation = {0};
    ASSERT_EQ(parse_xml(root, xml, &observation), CBM_SVN_PROBE_OK);
    ASSERT_EQ(observation.candidate_count, 0);
    ASSERT_EQ(observation.bytes_hashed, 0);
    th_cleanup(root);
    PASS();
}

TEST(client_init_skips_executable_in_current_directory) {
    char *base = th_mktempdir("cbm_svn_client_cwd");
    ASSERT_NOT_NULL(base);
    char watched[1024];
    char hostile[1024];
    char fake[1200];
    snprintf(watched, sizeof(watched), "%s/watched", base);
    snprintf(hostile, sizeof(hostile), "%s/hostile", base);
    ASSERT_EQ(cbm_mkdir(watched), 0);
    ASSERT_EQ(cbm_mkdir(hostile), 0);
#ifdef _WIN32
    snprintf(fake, sizeof(fake), "%s/svn.exe", hostile);
    const char delimiter = ';';
#else
    snprintf(fake, sizeof(fake), "%s/svn", hostile);
    const char delimiter = ':';
#endif
    ASSERT_EQ(th_write_file(fake, "#!/bin/sh\nexit 99\n"), 0);
    ASSERT_EQ(chmod(fake, 0755), 0);

    char original_cwd[4096];
    ASSERT_NOT_NULL(getcwd(original_cwd, sizeof(original_cwd)));
    const char *path = getenv("PATH");
    ASSERT_NOT_NULL(path);
    char *original_path = strdup(path);
    ASSERT_NOT_NULL(original_path);
    size_t path_size = strlen(hostile) + strlen(original_path) + 2;
    char *test_path = malloc(path_size);
    ASSERT_NOT_NULL(test_path);
    snprintf(test_path, path_size, "%s%c%s", hostile, delimiter, original_path);

    bool changed_cwd = chdir(hostile) == 0;
    if (changed_cwd) {
        cbm_setenv("PATH", test_path, 1);
    }
    cbm_svn_client_t client = {0};
    cbm_svn_probe_result_t result =
        changed_cwd ? cbm_svn_client_init(watched, &client) : CBM_SVN_PROBE_UNCERTAIN;
    bool restored_cwd = chdir(original_cwd) == 0;
    cbm_setenv("PATH", original_path, 1);
    free(test_path);
    free(original_path);

    ASSERT_TRUE(changed_cwd);
    ASSERT_TRUE(restored_cwd);
    ASSERT_EQ(result, CBM_SVN_PROBE_OK);
    ASSERT_TRUE(strstr(client.executable, hostile) == NULL);
    th_cleanup(base);
    PASS();
}

TEST(path_arg_matches_client_runtime) {
    cbm_svn_client_t client = {0};
    char path[128];
#ifdef _WIN32
    client.uses_posix_paths = true;
    ASSERT_TRUE(cbm_svn_format_path_arg(&client, "D:\\workspace\\project", path, sizeof(path)));
    ASSERT_STR_EQ(path, "/d/workspace/project");
#else
    ASSERT_TRUE(cbm_svn_format_path_arg(&client, "/workspace/project", path, sizeof(path)));
    ASSERT_STR_EQ(path, "/workspace/project");
#endif
    PASS();
}

TEST(client_runtime_detection_is_case_insensitive) {
    ASSERT_TRUE(cbm_svn_test_executable_uses_posix_paths("C:/MSYS64/USR/BIN/SVN.EXE"));
    ASSERT_FALSE(cbm_svn_test_executable_uses_posix_paths("C:/MSYS64/mingw64/bin/svn.exe"));
    PASS();
}

TEST(real_probe_tracks_repeated_working_copy_edits) {
    th_svn_fixture_t fixture;
    ASSERT_EQ(th_svn_fixture_init(&fixture, "cbm_svn_probe", "source.c", "int value = 1;\n"),
              0);
    cbm_svn_observation_t non_working_copy = {0};
    ASSERT_EQ(cbm_svn_probe(&fixture.client, fixture.root, &non_working_copy),
              CBM_SVN_PROBE_NOT_WORKING_COPY);

    cbm_svn_observation_t clean = {0};
    ASSERT_EQ(cbm_svn_probe(&fixture.client, fixture.working_copy, &clean), CBM_SVN_PROBE_OK);
    ASSERT_FALSE(clean.has_local_changes);
    ASSERT_EQ(th_write_file(fixture.source, "int value = 2;\n"), 0);
    cbm_svn_observation_t first_edit = {0};
    ASSERT_EQ(cbm_svn_probe(&fixture.client, fixture.working_copy, &first_edit), CBM_SVN_PROBE_OK);
    ASSERT_EQ(th_write_file(fixture.source, "int value = 3;\n"), 0);
    cbm_svn_observation_t second_edit = {0};
    ASSERT_EQ(cbm_svn_probe(&fixture.client, fixture.working_copy, &second_edit), CBM_SVN_PROBE_OK);

    ASSERT_NEQ(clean.semantic_signature, first_edit.semantic_signature);
    ASSERT_TRUE(first_edit.has_local_changes);
    ASSERT_TRUE(second_edit.has_local_changes);
    ASSERT_EQ(first_edit.semantic_signature, second_edit.semantic_signature);
    ASSERT_NEQ(first_edit.content_signature, second_edit.content_signature);
    ASSERT_GT(second_edit.bytes_hashed, 0);

    th_svn_fixture_cleanup(&fixture);
    PASS();
}

SUITE(svn_state) {
    RUN_TEST(content_change_updates_observation_without_status_change);
    RUN_TEST(parser_rejects_unsafe_xml);
    RUN_TEST(clean_properties_and_externals_are_not_local_changes);
    RUN_TEST(discovery_excluded_candidate_directory_is_not_hashed);
    RUN_TEST(client_init_skips_executable_in_current_directory);
    RUN_TEST(path_arg_matches_client_runtime);
    RUN_TEST(client_runtime_detection_is_case_insensitive);
    RUN_TEST(real_probe_tracks_repeated_working_copy_edits);
}
