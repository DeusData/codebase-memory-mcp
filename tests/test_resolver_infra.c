/* Independent source fixtures for infrastructure proof adapters. Required
 * references are fixed before indexing; every unexpected target is a bug. */
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"

#include <string.h>

typedef struct {
    const char *path, *source;
} infra_fixture_file_t;

static bool infra_fixture(const infra_fixture_file_t *files, size_t file_count,
                          const char *const *queries, size_t query_count, int *values) {
    const char *root = th_mktempdir("cbm_resolver_infra");
    if (!root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755);
    for (size_t i = 0; ok && i < file_count; i++) {
        char path[1024], directory[1024];
        int written = snprintf(path, sizeof(path), "%s/%s", repo, files[i].path);
        ok = written > 0 && (size_t)written < sizeof(path);
        if (!ok)
            break;
        memcpy(directory, path, (size_t)written + 1);
        char *slash = strrchr(directory, '/');
        if (slash)
            *slash = '\0';
        ok = cbm_mkdir_p(directory, 0755) && th_write_file(path, files[i].source) == 0;
    }
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "infra-proof") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    ok = ok && store;
    for (size_t i = 0; ok && i < query_count; i++) {
        sqlite3_stmt *statement = NULL;
        ok = sqlite3_prepare_v2(cbm_store_get_db(store), queries[i], -1, &statement, NULL) ==
                 SQLITE_OK &&
             sqlite3_step(statement) == SQLITE_ROW;
        if (ok)
            values[i] = sqlite3_column_int(statement, 0);
        sqlite3_finalize(statement);
    }
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return ok;
}

TEST(resolver_infra_terraform_literal_dependencies_are_required) {
    const infra_fixture_file_t files[] = {
        {"main.tf", "module \"lib\" {\n source = \"./lib\"\n}\n"
                    "output \"selected\" {\n value = module.lib.answer\n}\n"},
        {"lib/main.tf", "output \"answer\" {\n value = 1\n}\n"},
        {"decoy/main.tf", "output \"answer\" {\n value = 2\n}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.file_path='main.tf' AND e.type='IMPORTS' AND "
        "t.label='Folder' AND t.file_path='lib'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.file_path='main.tf' AND e.type='USAGE' AND "
        "t.file_path='lib/main.tf' AND t.name='output.answer'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.file_path='main.tf' AND e.type IN ('IMPORTS','USAGE') AND "
        "t.file_path LIKE 'decoy%'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 3, queries, 3, found));
    ASSERT_EQ(found[0], 1);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 0);
    PASS();
}

TEST(resolver_infra_terraform_duplicate_and_missing_names_are_counted) {
    const infra_fixture_file_t files[] = {{"main.tf",
                                           "output \"one\" {\n value = var.duplicate\n}\n"
                                           "output \"two\" {\n value = var.missing\n}\n"},
                                          {"first.tf", "variable \"duplicate\" {}\n"},
                                          {"second.tf", "variable \"duplicate\" {}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE "
        "s.file_path='main.tf' AND e.type='USAGE'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='main.tf' AND "
        "relation='USAGE' AND emitted=0 AND binding=2 AND "
        "reason='terraform_duplicate_declaration'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='main.tf' AND "
        "relation='USAGE' AND emitted=0 AND binding=0 AND "
        "spelling='var.missing' AND reason='terraform_module_declaration_lookup'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 3, queries, 3, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 1);
    PASS();
}

TEST(resolver_infra_terraform_conflicting_sources_are_unsupported) {
    const infra_fixture_file_t files[] = {
        {"main.tf", "module \"ambiguous\" {\n source = \"./first\"\n"
                    " source = \"./second\"\n}\n"
                    "module \"dynamic\" {\n source = var.path\n}\n"},
        {"first/main.tf", "output \"answer\" {\n value = 1\n}\n"},
        {"second/main.tf", "output \"answer\" {\n value = 2\n}\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE "
        "s.file_path='main.tf' AND e.type='IMPORTS'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='main.tf' AND "
        "relation='IMPORTS' AND emitted=0 AND analysis=0 AND "
        "reason='terraform_literal_module_source'"};
    int found[2] = {-1, -1};
    ASSERT(infra_fixture(files, 3, queries, 2, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 2);
    PASS();
}

TEST(resolver_infra_ansible_duplicate_names_and_listener_membership) {
    const infra_fixture_file_t files[] = {
        {"site.yml", "- hosts: all\n  tasks:\n    - name: ambiguous\n      debug: msg=x\n"
                     "      notify: reload\n    - name: broadcast\n      debug: msg=y\n"
                     "      notify: changed\n  handlers:\n    - name: reload\n      debug: msg=a\n"
                     "    - name: reload\n      debug: msg=b\n"
                     "    - name: first listener\n      debug: msg=c\n      listen: changed\n"
                     "    - name: second listener\n      debug: msg=d\n      listen: changed\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE "
        "s.file_path='site.yml' AND s.name='ambiguous' AND e.type='CALLS'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE s.name='broadcast' AND e.type='CALLS' AND "
        "t.name IN ('first listener','second listener')",
        "SELECT count(*) FROM resolver_occurrences WHERE relation='CALLS' AND emitted=0 "
        "AND binding=2 AND reason='ansible_duplicate_handler_name'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 1, queries, 3, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 2);
    ASSERT_EQ(found[2], 1);
    PASS();
}

TEST(resolver_infra_ansible_plays_and_unknown_dependencies_stay_separate) {
    const infra_fixture_file_t files[] = {
        {"site.yml", "- hosts: first\n  handlers:\n    - name: reload\n      debug: msg=x\n"
                     "- hosts: second\n  tasks:\n    - name: missing handler\n      debug: msg=y\n"
                     "      notify: reload\n    - import_tasks: actual.yml\n"
                     "    - import_tasks: missing.yml\n    - import_tasks: \"{{ dynamic }}\"\n"},
        {"actual.yml", "- name: included\n  debug: msg=z\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges WHERE type='CALLS'",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='IMPORTS' AND s.file_path='site.yml' AND "
        "t.file_path='actual.yml'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='site.yml' AND "
        "relation='IMPORTS' AND emitted=0 AND reason='ansible_literal_dependency_lookup'",
        "SELECT count(*) FROM resolver_occurrences WHERE file_path='site.yml' AND "
        "relation='CALLS' AND emitted=0 AND reason='ansible_handler_not_resolved'"};
    int found[4] = {-1, -1, -1, -1};
    ASSERT(infra_fixture(files, 2, queries, 4, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 2);
    ASSERT_EQ(found[3], 1);
    PASS();
}

TEST(resolver_infra_ansible_duplicate_keys_and_unmodeled_escapes_abstain) {
    const infra_fixture_file_t files[] = {
        {"duplicate.yml", "- hosts: all\n  tasks:\n    - import_tasks: first.yml\n"
                          "      import_tasks: second.yml\n"},
        {"escape.yml", "- hosts: all\n  tasks:\n    - import_tasks: \"\\u0066irst.yml\"\n"},
        {"good.yml", "- hosts: all\n  tasks:\n    - import_tasks: first.yml\n"},
        {"first.yml", "- debug: msg=first\n"},
        {"second.yml", "- debug: msg=second\n"},
        {"u0066irst.yml", "- debug: msg=decoy\n"}};
    const char *queries[] = {
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.type='IMPORTS' "
        "AND s.file_path IN ('duplicate.yml','escape.yml')",
        "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON "
        "t.id=e.target_id WHERE e.type='IMPORTS' AND s.file_path='good.yml' AND "
        "t.file_path='first.yml'",
        "SELECT count(*) FROM resolver_occurrences WHERE relation='IMPORTS' AND emitted=0 "
        "AND analysis=0 AND file_path IN ('duplicate.yml','escape.yml') AND "
        "reason='ansible_literal_dependency_lookup'"};
    int found[3] = {-1, -1, -1};
    ASSERT(infra_fixture(files, 6, queries, 3, found));
    ASSERT_EQ(found[0], 0);
    ASSERT_EQ(found[1], 1);
    ASSERT_EQ(found[2], 3);
    PASS();
}

SUITE(resolver_infra) {
    RUN_TEST(resolver_infra_terraform_literal_dependencies_are_required);
    RUN_TEST(resolver_infra_terraform_duplicate_and_missing_names_are_counted);
    RUN_TEST(resolver_infra_terraform_conflicting_sources_are_unsupported);
    RUN_TEST(resolver_infra_ansible_duplicate_names_and_listener_membership);
    RUN_TEST(resolver_infra_ansible_plays_and_unknown_dependencies_stay_separate);
    RUN_TEST(resolver_infra_ansible_duplicate_keys_and_unmodeled_escapes_abstain);
}
