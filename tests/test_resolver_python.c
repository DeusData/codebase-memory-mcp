#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"

typedef struct {
    const char *path, *text;
} python_proof_file_t;

static int python_proof_edges(const python_proof_file_t *files, size_t count, const char *target,
                              const char *name, const char *label) {
    const char *root = th_mktempdir("cbm_python_proof");
    if (!root)
        return -1;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755) && cbm_mkdir_p(TH_PATH(repo, "lib"), 0755);
    for (size_t i = 0; ok && i < count; i++)
        ok = th_write_file(TH_PATH(repo, files[i].path), files[i].text) == 0;
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "python-proof")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (store &&
        sqlite3_prepare_v2(
            cbm_store_get_db(store),
            "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
            "JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' AND s.file_path='main.py' "
            "AND t.file_path=?1 AND t.name=?2 AND t.label=?3",
            -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, target, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, name, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 3, label, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            edges = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    return edges;
}

TEST(resolver_python_reexport_chain_preserves_actual_declaration) {
    const python_proof_file_t files[] = {
        {"lib/actual.py", "def real():\n    return 1\n"},
        {"lib/mid.py", "from lib.actual import real as alias\n"},
        {"lib/front.py", "from lib.mid import alias as endpoint\n"},
        {"main.py", "from lib.front import endpoint\ndef run():\n    return endpoint()\n"}};
    ASSERT_EQ(python_proof_edges(files, 4, "lib/actual.py", "real", "Function"), 1);
    PASS();
}

TEST(resolver_python_imported_class_is_the_static_callable) {
    const python_proof_file_t files[] = {
        {"lib/errors.py", "class Problem(Exception):\n    pass\n"},
        {"lib/front.py", "from lib.errors import Problem as Error\n"},
        {"main.py", "from lib.front import Error\ndef run():\n    raise Error()\n"}};
    ASSERT_EQ(python_proof_edges(files, 3, "lib/errors.py", "Problem", "Class"), 1);
    PASS();
}

TEST(resolver_python_reexport_shadow_and_cycle_abstain) {
    const char *front[] = {
        "from lib.actual import real as endpoint\nendpoint = unknown\n",
        "from lib.actual import real as endpoint\nfrom missing import endpoint\n",
        "from lib.mid import endpoint\n"};
    for (size_t i = 0; i < sizeof(front) / sizeof(front[0]); i++) {
        const python_proof_file_t files[] = {
            {"lib/actual.py", "def real():\n    return 1\n"},
            {"lib/front.py", front[i]},
            {"lib/mid.py", "from lib.front import endpoint\n"},
            {"main.py", "from lib.front import endpoint\ndef run():\n    return endpoint()\n"}};
        ASSERT_EQ(python_proof_edges(files, 4, "lib/actual.py", "real", "Function"), 0);
    }
    PASS();
}

TEST(resolver_python_package_value_blocks_physical_submodule) {
    const python_proof_file_t files[] = {
        {"lib/__init__.py", "from .actual import real as mod\n"},
        {"lib/actual.py", "def real():\n    return 1\n"},
        {"lib/mod.py", "def wrong():\n    return 2\n"},
        {"main.py", "from lib import mod\ndef run():\n    return mod.wrong()\n"}};
    ASSERT_EQ(python_proof_edges(files, 4, "lib/mod.py", "wrong", "Function"), 0);
    PASS();
}

TEST(resolver_python_typed_receiver_preserves_crossfile_method) {
    const python_proof_file_t files[] = {
        {"lib/service.py", "class Service:\n    def go(self):\n        pass\n"},
        {"lib/decoy.py", "class Service:\n    def go(self):\n        pass\n"},
        {"main.py",
         "from lib.service import Service as Svc\ndef run(value: Svc):\n    value.go()\n"}};
    ASSERT_EQ(python_proof_edges(files, 3, "lib/service.py", "go", "Method"), 1);
    ASSERT_EQ(python_proof_edges(files, 3, "lib/decoy.py", "go", "Method"), 0);
    PASS();
}

SUITE(resolver_python) {
    RUN_TEST(resolver_python_typed_receiver_preserves_crossfile_method);
    RUN_TEST(resolver_python_reexport_chain_preserves_actual_declaration);
    RUN_TEST(resolver_python_imported_class_is_the_static_callable);
    RUN_TEST(resolver_python_reexport_shadow_and_cycle_abstain);
    RUN_TEST(resolver_python_package_value_blocks_physical_submodule);
}
