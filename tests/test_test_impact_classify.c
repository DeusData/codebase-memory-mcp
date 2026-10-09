/*
 * test_test_impact_classify.c — the classified ledger of a pinned HEAD
 * inventory (src/mcp/test_impact_classify.c), on real git snapshots.
 *
 * The ledger must give every kept file the language discovery gives the same
 * bytes; a read it cannot make is an error, never a default language.
 */
#include "test_test_impact_inventory_internal.h"

#include "discover/discover.h"
#include "discover/userconfig.h"
#include "mcp/test_impact_classify.h"

typedef struct {
    const char *path;
    const char *content;
} tic_file_t;

/* A HEAD commit holding exactly `files`, its pinned tree and its inventory. */
static bool tic_snapshot(if_native *n, const tic_file_t *files, size_t count,
                         cbm_test_impact_inventory_t **inventory) {
    if (!ni_start(n) || !ni_reset_input(n)) {
        return false;
    }
    for (size_t i = 0; i < count; i++) {
        if (!if_text(&n->input, files[i].path, files[i].content)) {
            return false;
        }
    }
    n->input.limits.max_probe_prefix_bytes = CBM_LANGUAGE_PROBE_MAX;
    return ni_commit_input(n) && ni_prepare(n, inventory);
}

static const cbm_test_impact_classified_t *tic_row(const cbm_test_impact_ledger_t *ledger,
                                                   const if_native *n, const char *path) {
    size_t count = 0;
    const cbm_test_impact_classified_t *rows = cbm_test_impact_ledger_rows(ledger, &count);
    for (size_t i = 0; i < count; i++) {
        if (strcmp(n->input.paths[rows[i].file_index], path) == 0) {
            return &rows[i];
        }
    }
    return NULL;
}

/* The language discovery's rule gives the same bytes. */
static CBMLanguage tic_expected(const char *path, const char *content) {
    const char *slash = strrchr(path, '/');
    const char *name = slash ? slash + 1 : path;
    size_t len = strlen(content);
    size_t probe = cbm_language_probe_bytes(name);
    size_t head = len < probe ? len : probe;
    return cbm_language_classify(name, (const unsigned char *)content, head, len > head, true);
}

static const tic_file_t TIC_FILES[] = {
    {".gitignore", "drop.c\n"},
    {"a.m", "#import <Foundation/Foundation.h>\n@interface A\n@end\n"},
    {"b.m", "% matlab\nx = 1;\n"},
    {"c.cls", "Class Demo.Person Extends %Persistent\n{\n}\n"},
    {"d.xml", "<?xml version=\"1.0\"?>\n<Export generator=\"IRIS\">\n"},
    {"drop.c", "int dropped;\n"},
    {"package.json", "{}\n"},
    {"plain", "no shebang here\n"},
    {"sub/data.json", "{\"k\": 1}\n"},
    {"tool", "#!/usr/bin/env python3\nprint(1)\n"},
    {"x.go", "package x\n"},
};

TEST(test_impact_classify_gives_discovery_languages) {
    if_native n = {0};
    cbm_test_impact_inventory_t *inventory = NULL;
    cbm_test_impact_ledger_t *ledger = NULL;
    int result = 1;
    size_t nfiles = sizeof(TIC_FILES) / sizeof(TIC_FILES[0]);
    NI_CHECK(tic_snapshot(&n, TIC_FILES, nfiles, &inventory));
    cbm_inventory_error_t error;
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_OK);
    NI_CHECK(ledger && error.status == CBM_INVENTORY_OK);
    size_t count = 0;
    const cbm_test_impact_classified_t *rows = cbm_test_impact_ledger_rows(ledger, &count);
    /* Every kept file once, in file-index order; the ignored one is absent. */
    NI_CHECK(count == nfiles - 1);
    for (size_t i = 1; i < count; i++) {
        NI_CHECK(rows[i - 1].file_index < rows[i].file_index);
    }
    NI_CHECK(tic_row(ledger, &n, "drop.c") == NULL);
    for (size_t i = 0; i < nfiles; i++) {
        if (strcmp(TIC_FILES[i].path, "drop.c") == 0) {
            continue;
        }
        const cbm_test_impact_classified_t *row = tic_row(ledger, &n, TIC_FILES[i].path);
        NI_CHECK(row != NULL);
        NI_CHECK(row->language == tic_expected(TIC_FILES[i].path, TIC_FILES[i].content));
    }
    /* Spot values, so a wrong shared rule cannot pass by agreeing with itself. */
    NI_CHECK(tic_row(ledger, &n, "a.m")->language == CBM_LANG_OBJC);
    NI_CHECK(tic_row(ledger, &n, "b.m")->language == CBM_LANG_MATLAB);
    NI_CHECK(tic_row(ledger, &n, "c.cls")->language == CBM_LANG_OBJECTSCRIPT_UDL);
    NI_CHECK(tic_row(ledger, &n, "d.xml")->language == CBM_LANG_OBJECTSCRIPT_EXPORT);
    NI_CHECK(tic_row(ledger, &n, "package.json")->language == CBM_LANG_COUNT);
    NI_CHECK(tic_row(ledger, &n, "plain")->language == CBM_LANG_COUNT);
    NI_CHECK(tic_row(ledger, &n, "sub/data.json")->language == CBM_LANG_JSON);
    NI_CHECK(tic_row(ledger, &n, "tool")->language == CBM_LANG_PYTHON);
    NI_CHECK(tic_row(ledger, &n, "x.go")->language == CBM_LANG_GO);
    /* Only names that need content were read: a.m b.m c.cls d.xml, and the
     * shebang probe for the names without a language (.gitignore plain tool). */
    cbm_test_impact_classify_usage_t usage;
    NI_CHECK(cbm_test_impact_ledger_usage(ledger, &usage));
    NI_CHECK(usage.verified_file_reads == 7);
    NI_CHECK(memcmp(cbm_test_impact_ledger_manifest(ledger),
                    cbm_test_impact_inventory_view(inventory)->manifest_sha256, 32) == 0);
    result = 0;
done:
    cbm_test_impact_ledger_free(ledger);
    return ni_finish(&n, inventory, result);
}

/* Reads and the probe length are charged to what the inventory's limits leave
 * after the filter. Running out is a LIMIT for the whole pass, with no ledger,
 * and never a default language for the files that were not read. */
TEST(test_impact_classify_limits_are_errors_not_defaults) {
    if_native n = {0};
    cbm_test_impact_inventory_t *inventory = NULL;
    cbm_test_impact_ledger_t *ledger = (cbm_test_impact_ledger_t *)(uintptr_t)1;
    int result = 1;
    NI_CHECK(tic_snapshot(&n, TIC_FILES, sizeof(TIC_FILES) / sizeof(TIC_FILES[0]), &inventory));
    cbm_test_impact_inventory_usage_t used;
    NI_CHECK(cbm_test_impact_inventory_usage(inventory, &used));
    cbm_inventory_error_t error;

    /* One read fewer than the seven probes need. The limits live in the
     * inventory, so a tighter one needs a fresh inventory. */
    cbm_test_impact_inventory_free(inventory);
    inventory = NULL;
    n.input.limits.max_verified_file_reads = used.filter.verified_file_reads_reserved + 6;
    NI_CHECK(ni_prepare(&n, &inventory));
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_LIMIT);
    NI_CHECK(ledger == NULL && error.status == CBM_INVENTORY_LIMIT);
    /* Exactly enough is fine. */
    cbm_test_impact_inventory_free(inventory);
    inventory = NULL;
    n.input.limits.max_verified_file_reads = used.filter.verified_file_reads_reserved + 7;
    NI_CHECK(ni_prepare(&n, &inventory));
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_OK);
    cbm_test_impact_ledger_free(ledger);
    ledger = NULL;

    /* A probe longer than the probe limit: the shebang probe needs 255. */
    cbm_test_impact_inventory_free(inventory);
    inventory = NULL;
    n.input.limits.max_probe_prefix_bytes = 254;
    NI_CHECK(ni_prepare(&n, &inventory));
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_LIMIT);
    NI_CHECK(ledger == NULL);

    /* Arguments. */
    NI_CHECK(cbm_test_impact_classify(NULL, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_INVALID);
    NI_CHECK(cbm_test_impact_classify(inventory, NULL, NULL, &n.input.control, &ledger, &error) ==
             CBM_INVENTORY_INVALID);
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, NULL,
                                      &error) == CBM_INVENTORY_INVALID);
    result = 0;
done:
    if (ledger != (cbm_test_impact_ledger_t *)(uintptr_t)1) {
        cbm_test_impact_ledger_free(ledger);
    }
    return ni_finish(&n, inventory, result);
}

/* The snapshot's language config decides, passed explicitly: the process-wide
 * config another index may have installed takes no part. */
TEST(test_impact_classify_uses_the_given_config) {
    if_native n = {0};
    cbm_test_impact_inventory_t *inventory = NULL;
    cbm_test_impact_ledger_t *with = NULL;
    cbm_test_impact_ledger_t *without = NULL;
    cbm_userconfig_t *config = NULL;
    int result = 1;
    static const tic_file_t files[] = {{"script.foo", "x = 1\n"}};
    NI_CHECK(tic_snapshot(&n, files, 1, &inventory));
    /* A config that maps .foo to Python, from a project file in a scratch dir. */
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/cbm-classify-config-XXXXXX", cbm_tmpdir());
    NI_CHECK(cbm_mkdtemp(dir) != NULL);
    char file[600];
    snprintf(file, sizeof(file), "%s/.codebase-memory.json", dir);
    static const char project[] = "{\"extra_extensions\":{\".foo\":\"python\"}}";
    NI_CHECK(ni_write(&n, file, project, sizeof(project) - 1));
    config = cbm_userconfig_load(dir);
    remove(file);
    th_rmtree(dir);
    NI_CHECK(config != NULL);
    NI_CHECK(cbm_userconfig_lookup(config, ".foo") == CBM_LANG_PYTHON);
    const cbm_userconfig_t *installed = cbm_get_user_lang_config();
    cbm_inventory_error_t error;
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, config, &n.input.control, &with,
                                      &error) == CBM_INVENTORY_OK);
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &without,
                                      &error) == CBM_INVENTORY_OK);
    NI_CHECK(cbm_get_user_lang_config() == installed);
    NI_CHECK(tic_row(with, &n, "script.foo")->language == CBM_LANG_PYTHON);
    NI_CHECK(tic_row(without, &n, "script.foo")->language == CBM_LANG_COUNT);
    result = 0;
done:
    cbm_test_impact_ledger_free(with);
    cbm_test_impact_ledger_free(without);
    cbm_userconfig_free(config);
    return ni_finish(&n, inventory, result);
}

/* A blob that cannot be read fails the pass: no ledger, the native error, and
 * the index of the file. A default language here would index a different
 * graph than the one the selection claims to use. */
TEST(test_impact_classify_read_failure_is_an_error) {
    if_native n = {0};
    cbm_test_impact_inventory_t *inventory = NULL;
    cbm_test_impact_ledger_t *ledger = (cbm_test_impact_ledger_t *)(uintptr_t)1;
    int result = 1;
    NI_CHECK(tic_snapshot(&n, TIC_FILES, sizeof(TIC_FILES) / sizeof(TIC_FILES[0]), &inventory));
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    NI_CHECK(cbm_pinned_tree_test_set_read_fault(n.input.tree, CBM_PINNED_TREE_READ_FAULT_READ));
    cbm_inventory_error_t error;
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_IO);
    NI_CHECK(ledger == NULL && error.status == CBM_INVENTORY_IO);
    /* The first survivor that needs its content is .gitignore, file 0. */
    NI_CHECK(error.file_index == 0);
    /* A started failed read leaves the tree disposal-only, as for the filter. */
    NI_CHECK(!cbm_pinned_tree_view(n.input.tree));
    result = 0;
#else
    fprintf(stderr, "classification read faults require canonical CBM_ENABLE_TEST_SEAMS\n");
#endif
done:
    if (ledger != (cbm_test_impact_ledger_t *)(uintptr_t)1) {
        cbm_test_impact_ledger_free(ledger);
    }
    return ni_finish(&n, inventory, result);
}

/* The inventory and the tree must describe the same snapshot. */
TEST(test_impact_classify_refuses_another_snapshot) {
    if_native first = {0};
    if_native second = {0};
    cbm_test_impact_inventory_t *inventory = NULL;
    cbm_test_impact_inventory_t *other = NULL;
    cbm_test_impact_ledger_t *ledger = (cbm_test_impact_ledger_t *)(uintptr_t)1;
    int result = 1;
    static const tic_file_t files[] = {{"x.go", "package x\n"}};
    NI_CHECK(tic_snapshot(&first, TIC_FILES, sizeof(TIC_FILES) / sizeof(TIC_FILES[0]), &inventory));
    NI_CHECK(tic_snapshot(&second, files, 1, &other));
    cbm_inventory_error_t error;
    NI_CHECK(cbm_test_impact_classify(inventory, second.input.tree, NULL, &first.input.control,
                                      &ledger, &error) == CBM_INVENTORY_CHANGED);
    NI_CHECK(ledger == NULL);
    /* Each with its own tree is fine. */
    NI_CHECK(cbm_test_impact_classify(other, second.input.tree, NULL, &second.input.control,
                                      &ledger, &error) == CBM_INVENTORY_OK);
    result = 0;
done:
    if (ledger != (cbm_test_impact_ledger_t *)(uintptr_t)1) {
        cbm_test_impact_ledger_free(ledger);
    }
    result = ni_finish(&second, other, result);
    return ni_finish(&first, inventory, result);
}

/* ── The runnable registry of the same snapshot ─────────────────── */

static const tic_file_t TIC_SUITE_FILES[] = {
    {"README.md", "# readme\n"},
    {"src/x.c", "int x(void) { return 1; }\n"},
    {"tests/test_alpha.c", "TEST(a1) {\n    PASS();\n}\nSUITE(alpha) {\n    RUN_TEST(a1);\n}\n"},
    {"tests/test_main.c", "int main(void) {\n    RUN_SELECTED_SUITE(alpha);\n    return 0;\n}\n"},
};

static const char TIC_C_CBM[] = "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":"
                                "{\"c-cbm\":true}}}}";

/* The registry reads the snapshot's own test sources under the snapshot's
 * declarations: the runner suite, the case and its registration. */
TEST(test_impact_registry_reads_the_snapshot_tests) {
    if_native n = {0};
    cbm_test_impact_inventory_t *inventory = NULL;
    cbm_test_impact_ledger_t *ledger = NULL;
    cbm_test_declarations_t *declarations = NULL;
    cbm_test_model_t *model = NULL;
    int result = 1;
    NI_CHECK(tic_snapshot(&n, TIC_SUITE_FILES, sizeof(TIC_SUITE_FILES) / sizeof(TIC_SUITE_FILES[0]),
                          &inventory));
    cbm_inventory_error_t error;
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_OK);
    declarations = cbm_test_declarations_parse(TIC_C_CBM, sizeof(TIC_C_CBM) - 1, false);
    NI_CHECK(declarations != NULL);
    NI_CHECK(cbm_test_impact_registry(ledger, inventory, n.input.tree, declarations,
                                      &n.input.control, &model, &error) == CBM_INVENTORY_OK);
    NI_CHECK(model && error.status == CBM_INVENTORY_OK);
    NI_CHECK(cbm_test_model_complete(model));
    int count = 0;
    const cbm_test_runner_suite_t *runner = cbm_test_model_runner_suites(model, &count);
    NI_CHECK(count == 1 && strcmp(runner[0].name, "alpha") == 0);
    const cbm_test_case_t *cases = cbm_test_model_cases(model, &count);
    NI_CHECK(count == 1 && strcmp(cases[0].name, "a1") == 0 &&
             strcmp(cases[0].file, "tests/test_alpha.c") == 0);
    const cbm_test_registration_t *regs = cbm_test_model_registrations(model, &count);
    NI_CHECK(count == 1 && strcmp(regs[0].suite, "alpha") == 0 && strcmp(regs[0].test, "a1") == 0 &&
             regs[0].resolved);
    result = 0;
done:
    cbm_test_model_free(model);
    cbm_test_declarations_free(declarations);
    cbm_test_impact_ledger_free(ledger);
    return ni_finish(&n, inventory, result);
}

/* Without conventions the registry names no test at all, and a registry that
 * cannot read every source it needs is an error, never a smaller registry. */
TEST(test_impact_registry_names_nothing_it_cannot_read) {
    if_native n = {0};
    cbm_test_impact_inventory_t *inventory = NULL;
    cbm_test_impact_ledger_t *ledger = NULL;
    cbm_test_declarations_t *absent = NULL;
    cbm_test_declarations_t *c_cbm = NULL;
    cbm_test_model_t *model = NULL;
    int result = 1;
    size_t nfiles = sizeof(TIC_SUITE_FILES) / sizeof(TIC_SUITE_FILES[0]);
    NI_CHECK(tic_snapshot(&n, TIC_SUITE_FILES, nfiles, &inventory));
    cbm_inventory_error_t error;
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_OK);
    absent = cbm_test_declarations_parse(NULL, 0, true);
    NI_CHECK(absent != NULL);
    NI_CHECK(cbm_test_impact_registry(ledger, inventory, n.input.tree, absent, &n.input.control,
                                      &model, &error) == CBM_INVENTORY_OK);
    int count = -1;
    (void)cbm_test_model_cases(model, &count);
    NI_CHECK(count == 0);
    cbm_test_model_free(model);
    model = NULL;

    /* Three C sources to read; the reads the filter and the ledger left over
     * are cut to two. The limits live in the inventory, so a fresh one. */
    cbm_test_impact_classify_usage_t spent;
    NI_CHECK(cbm_test_impact_ledger_usage(ledger, &spent));
    cbm_test_impact_inventory_usage_t used;
    NI_CHECK(cbm_test_impact_inventory_usage(inventory, &used));
    cbm_test_impact_ledger_free(ledger);
    ledger = NULL;
    cbm_test_impact_inventory_free(inventory);
    inventory = NULL;
    n.input.limits.max_verified_file_reads =
        used.filter.verified_file_reads_reserved + spent.verified_file_reads + 2;
    NI_CHECK(ni_prepare(&n, &inventory));
    NI_CHECK(cbm_test_impact_classify(inventory, n.input.tree, NULL, &n.input.control, &ledger,
                                      &error) == CBM_INVENTORY_OK);
    c_cbm = cbm_test_declarations_parse(TIC_C_CBM, sizeof(TIC_C_CBM) - 1, false);
    NI_CHECK(c_cbm != NULL);
    model = (cbm_test_model_t *)(uintptr_t)1;
    NI_CHECK(cbm_test_impact_registry(ledger, inventory, n.input.tree, c_cbm, &n.input.control,
                                      &model, &error) == CBM_INVENTORY_LIMIT);
    NI_CHECK(model == NULL && error.status == CBM_INVENTORY_LIMIT);
    result = 0;
done:
    if (model != (cbm_test_model_t *)(uintptr_t)1) {
        cbm_test_model_free(model);
    }
    cbm_test_declarations_free(absent);
    cbm_test_declarations_free(c_cbm);
    cbm_test_impact_ledger_free(ledger);
    return ni_finish(&n, inventory, result);
}

SUITE(test_impact_classify) {
    RUN_TEST(test_impact_registry_reads_the_snapshot_tests);
    RUN_TEST(test_impact_registry_names_nothing_it_cannot_read);
    RUN_TEST(test_impact_classify_gives_discovery_languages);
    RUN_TEST(test_impact_classify_limits_are_errors_not_defaults);
    RUN_TEST(test_impact_classify_uses_the_given_config);
    RUN_TEST(test_impact_classify_read_failure_is_an_error);
    RUN_TEST(test_impact_classify_refuses_another_snapshot);
}
