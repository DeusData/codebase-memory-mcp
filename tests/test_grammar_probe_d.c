/*
 * test_grammar_probe_d.c — Remaining code-grammar node/import/inheritance probe.
 *
 * SCOPE
 * ─────
 * Probes 25 grammar-only code languages not yet covered by probes _a, _b, _c:
 *   agda, assembly, bicep, cfml, cfscript, cobol, elm, func, janet, lean,
 *   llvm_ir, magma, move, nasm, objc, pony, purescript, pine, qml, smali,
 *   tablegen, tlaplus, verilog, vhdl, wolfram.
 *
 * SKIPPED (with reason):
 *   systemverilog — CBM_LANG_SYSTEMVERILOG has no extension entry in the
 *     EXT_TABLE (language.c); the only SV extension ".sv" maps to
 *     CBM_LANG_VERILOG.  File-based index_repository cannot route to
 *     CBM_LANG_SYSTEMVERILOG via extension alone.  It is already exercised
 *     by the test_grammar_regression.c direct-language fixture and the
 *     grammar_labels histogram (Class:1,Function:1,Module:1).
 *
 * COLOUR LEGEND
 * ─────────────
 *   GREEN = guard: pipeline already produces the correct result; a failure
 *           here is a real regression.
 *   RED   = bug reproduction: pipeline does NOT yet produce the expected
 *           node/edge; test FAILS until fixed.  Brief inline comment names
 *           root-cause class.
 *
 * CALLS dimensions are intentionally omitted (covered by P5 breadth).
 * Do NOT register this suite in test_main.c.
 *
 * SUITE(grammar_probe_d)
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "resolver_quality.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ══════════════════════════════════════════════════════════════════
 * Harness — mirrors test_grammar_probe_a.c exactly.
 * Prefix "gpd_" to avoid symbol clashes with sibling probe files.
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} GpdProj;

typedef struct {
    const char *name; /* relative filename, may include '/' for subdirs */
    const char *content;
} GpdFile;

static void gpd_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

static cbm_store_t *gpd_open_indexed(GpdProj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project)
        return NULL;
    const char *home = getenv("HOME");
    if (!home)
        home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv)
        return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp)
        free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *gpd_index_files(GpdProj *lp, const GpdFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_gpd_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    gpd_to_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f)
            return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return gpd_open_indexed(lp);
}

static void gpd_cleanup(GpdProj *lp, cbm_store_t *store) {
    if (store)
        cbm_store_close(store);
    if (lp->srv) {
        cbm_mcp_server_free(lp->srv);
        lp->srv = NULL;
    }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* ── Node-count helpers ─────────────────────────────────────────── */

static int gpd_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Sum of all type-like labels. */
static int gpd_type_nodes(cbm_store_t *store, const char *project) {
    static const char *labels[] = {"Class", "Struct", "Interface", "Enum", "Trait", "Type", NULL};
    int total = 0;
    for (int i = 0; labels[i]; i++) {
        int n = gpd_count_label(store, project, labels[i]);
        if (n > 0)
            total += n;
    }
    return total;
}

/* Metrics bundled per index pass. */
typedef struct {
    int ok;
    int total_nodes;
    int functions;
    int methods;
    int types;    /* type-like sum */
    int imports;  /* IMPORTS edges */
    int inherits; /* INHERITS edges */
} GpdMetrics;

static GpdMetrics gpd_metrics_files(const GpdFile *files, int nfiles) {
    GpdProj lp;
    cbm_store_t *store = gpd_index_files(&lp, files, nfiles);
    GpdMetrics m = {0};
    if (store) {
        m.ok = 1;
        m.total_nodes = cbm_store_count_nodes(store, lp.project);
        m.functions = gpd_count_label(store, lp.project, "Function");
        m.methods = gpd_count_label(store, lp.project, "Method");
        m.types = gpd_type_nodes(store, lp.project);
        m.imports = cbm_store_count_edges_by_type(store, lp.project, "IMPORTS");
        m.inherits = cbm_store_count_edges_by_type(store, lp.project, "INHERITS");
    }
    gpd_cleanup(&lp, store);
    return m;
}

static GpdMetrics gpd_metrics(const char *filename, const char *content) {
    GpdFile f = {filename, content};
    return gpd_metrics_files(&f, 1);
}

/* Full production indexing, a counted source-authored import obligation,
 * and a required exact ESM call keep an all-gap fixture from passing vacuously. */
static bool gpd_import_quality(const GpdFile *files, size_t file_count, const RQ_Source *inputs,
                               size_t source_count, const char *suite, const RQ_Expected *gap,
                               const RQ_Forbidden *forbidden, RQ_Report *report) {
    if (file_count > 7 || source_count > 7)
        return false;
    const GpdFile control = {
        "quality_control.js",
        "export function target() { return 1; }\nexport function caller() { return target(); }\n"};
    GpdFile indexed[8];
    RQ_Source sources[8];
    memcpy(indexed, files, file_count * sizeof(*files));
    memcpy(sources, inputs, source_count * sizeof(*inputs));
    indexed[file_count++] = control;
    sources[source_count++] = (RQ_Source){.path = control.name, .text = control.content};
    const RQ_Expected expected[] = {
        *gap,
        {.case_id = "required-esm-local-call",
         .site = {.file = "quality_control.js",
                  .caller = "quality_control.caller",
                  .needle = "target()",
                  .occurrence_index = 1},
         .relation = "CALLS",
         .target = {.file = "quality_control.js",
                    .suffix = "quality_control.target",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function target() { return 1; }"},
         .mode = RQ_REQUIRED}};
    GpdProj lp;
    cbm_store_t *store = gpd_index_files(&lp, indexed, (int)file_count);
    if (!store) {
        gpd_cleanup(&lp, store);
        return false;
    }
    bool ok =
        rq_check_project_declarations(store, lp.project, suite, sources, source_count, expected, 2,
                                      forbidden, forbidden ? 1 : 0, report, stderr);
    gpd_cleanup(&lp, store);
    return ok;
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 1 — Agda (.agda)
 *
 * Agda label histogram: Function:1, Module:1 (grammar_labels histogram).
 * Spec: agda_func_types = {"function"}, agda_class_types = {"data","record"},
 *       agda_import_types = {"import","open","import_directive","instance"}.
 * Extension: .agda → CBM_LANG_AGDA.
 * ══════════════════════════════════════════════════════════════════ */

/* Agda: function definition → Function node. */
TEST(probe_agda_function) {
    GpdMetrics m = gpd_metrics("Nat.agda", "module Nat where\n"
                                           "\n"
                                           "double : Nat -> Nat\n"
                                           "double n = n + n\n"
                                           "\n"
                                           "triple : Nat -> Nat\n"
                                           "triple n = n + n + n\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: function definitions must reach the graph as Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Agda: data type definition → type-like node.
 * RED: histogram shows Function:1/Module:1 only; data/record types not
 *      extracted as Class/Type nodes despite agda_class_types spec. */
TEST(probe_agda_data_type) {
    GpdMetrics m = gpd_metrics("Shape.agda", "module Shape where\n"
                                             "\n"
                                             "data Shape : Set where\n"
                                             "  Circle : Shape\n"
                                             "  Square : Shape\n"
                                             "\n"
                                             "area : Shape -> Nat\n"
                                             "area Circle = 1\n"
                                             "area Square = 2\n");
    ASSERT_TRUE(m.ok);
    /* RED: Agda data/record type not extracted as type-like node (node-extraction gap). */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_agda_imports_edge) {
    static const GpdFile files[] = {{"Utils.agda", "module Utils where\n"
                                                   "\n"
                                                   "double : Nat -> Nat\n"
                                                   "double n = n + n\n"},
                                    {"Main.agda", "module Main where\n"
                                                  "\n"
                                                  "import Utils\n"
                                                  "\n"
                                                  "quad : Nat -> Nat\n"
                                                  "quad n = Utils.double (Utils.double n)\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/quality.agda-lib", "name: quality\ninclude: src\n"},
        {"configured/src/Utils.agda",
         "module Utils where\ndata Token : Set where\n  token : Token\n"},
        {"configured/src/Main.agda",
         "module Main where\nimport Utils\nvalue : Utils.Token\nvalue = Utils.token\n"},
        {"configured/src/Other.agda",
         "module Other where\ndata Token : Set where\n  token : Token\n"}};
    /* The .agda-lib configuration is written above but has no indexed source snapshot. */
    const RQ_Source sources[] = {{.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_agda_imports_edge-exact-configured-import",
        .site = {.file = "configured/src/Main.agda",
                 .caller = "configured.src.Main.agda.__file__",
                 .needle = "import Utils"},
        .relation = "IMPORTS",
        .target = {.file = "configured/src/Utils.agda",
                   .suffix = "configured.src.Utils.agda.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "module Utils where"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "agda-configured-library-import",
        .why = "The local .agda-lib include root selects the exact module Utils. The separate "
               "self-contained companion avoids the original undeclared Nat and arithmetic names. "
               "Module Other is not Utils; no audited Agda import domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {.case_id = "probe_agda_imports_edge-wrong-target",
                                    .site = {.file = "configured/src/Main.agda",
                                             .caller = "configured.src.Main.agda.__file__",
                                             .needle = "import Utils"},
                                    .relation = "IMPORTS",
                                    .target = {.file = "configured/src/Other.agda",
                                               .suffix = "configured.src.Other.agda.__file__",
                                               .label = "File",
                                               .declaration_line = 1,
                                               .declaration_text = "module Other where"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]), "probe_agda_imports_edge",
                                   &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 2 — Assembly (.s)
 *
 * Assembly label histogram: Function:1, Module:1.
 * Spec: assembly_func_types = {"label"}, no class/import types.
 * Extension: .s → CBM_LANG_ASSEMBLY.
 * Assembly has no OOP, no imports — only labels extracted as Functions.
 * ══════════════════════════════════════════════════════════════════ */

/* Assembly: global label → Function node. */
TEST(probe_assembly_label_function) {
    GpdMetrics m = gpd_metrics("add.s", ".global add\n"
                                        "add:\n"
                                        "    add x0, x0, x1\n"
                                        "    ret\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: global label must be extracted as a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Assembly: multiple labels → multiple Function nodes. */
TEST(probe_assembly_multiple_labels) {
    GpdMetrics m = gpd_metrics("math.s", ".global square\n"
                                         "square:\n"
                                         "    mul x0, x0, x0\n"
                                         "    ret\n"
                                         "\n"
                                         ".global negate\n"
                                         "negate:\n"
                                         "    neg x0, x0\n"
                                         "    ret\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both labels should produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Assembly: no OOP — no type-like nodes expected. */
TEST(probe_assembly_no_type_nodes) {
    GpdMetrics m = gpd_metrics("noop.s", ".global noop\n"
                                         "noop:\n"
                                         "    ret\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: assembly produces 0 type-like nodes. */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 3 — Bicep (.bicep)
 *
 * Bicep label histogram: Function:1, Module:1 (from grammar_labels).
 * Spec: bicep_func_types = {"user_defined_function","lambda_expression"},
 *       bicep_class_types = {"resource_declaration","type_declaration","module_declaration"},
 *       bicep_import_types = {"import_statement","module_declaration","import","using_statement"}.
 * Extension: .bicep → CBM_LANG_BICEP.
 * ══════════════════════════════════════════════════════════════════ */

/* Bicep: user-defined function → Function node. */
TEST(probe_bicep_function) {
    GpdMetrics m =
        gpd_metrics("funcs.bicep", "func greet(name string) string => 'Hello, ${name}!'\n"
                                   "\n"
                                   "func square(n int) int => n * n\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both user-defined functions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Bicep: resource declaration → type-like node.
 * RED: histogram shows Function:1/Module:1 only; resource_declaration not
 *      extracted as Class/Type node despite bicep_class_types spec. */
TEST(probe_bicep_resource_node) {
    GpdMetrics m =
        gpd_metrics("storage.bicep",
                    "resource storageAccount 'Microsoft.Storage/storageAccounts@2021-02-01' = {\n"
                    "  name: 'mystorage'\n"
                    "  location: 'eastus'\n"
                    "  kind: 'StorageV2'\n"
                    "  sku: {\n"
                    "    name: 'Standard_LRS'\n"
                    "  }\n"
                    "}\n");
    ASSERT_TRUE(m.ok);
    /* RED: Bicep resource_declaration not yet extracted as type-like node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* Bicep: module declaration (cross-file reference) → IMPORTS edge.
 * RED: grammar-only Bicep has no module-resolver in the pipeline. */
TEST(probe_bicep_module_import) {
    static const GpdFile files[] = {
        {"storage.bicep", "param name string\n"
                          "resource sa 'Microsoft.Storage/storageAccounts@2021-02-01' = {\n"
                          "  name: name\n"
                          "  location: 'eastus'\n"
                          "  kind: 'StorageV2'\n"
                          "  sku: { name: 'Standard_LRS' }\n"
                          "}\n"},
        {"main.bicep", "module storage './storage.bicep' = {\n"
                       "  name: 'storageDeploy'\n"
                       "  params: { name: 'mystore' }\n"
                       "}\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Bicep module cross-reference not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 4 — CFML (.cfm — tag dialect)
 *
 * CFML label histogram: Function:1, Module:1.
 * Spec: cfml_func_types = {"function_declaration","function_expression"};
 *       cffunction tags handled separately as cf_function_tag.
 * Extension: .cfm → CBM_LANG_CFML.
 * ══════════════════════════════════════════════════════════════════ */

/* CFML: <cffunction> tag → Function node. */
TEST(probe_cfml_function_tag) {
    GpdMetrics m = gpd_metrics("greet.cfm", "<cffunction name=\"greet\" returntype=\"string\">\n"
                                            "  <cfargument name=\"name\" type=\"string\">\n"
                                            "  <cfreturn \"Hello, \" & name>\n"
                                            "</cffunction>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: <cffunction> tag must produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* CFML: two <cffunction> tags → at least 2 Function nodes. */
TEST(probe_cfml_two_functions) {
    GpdMetrics m = gpd_metrics("utils.cfm", "<cffunction name=\"double\" returntype=\"numeric\">\n"
                                            "  <cfargument name=\"n\" type=\"numeric\">\n"
                                            "  <cfreturn n * 2>\n"
                                            "</cffunction>\n"
                                            "\n"
                                            "<cffunction name=\"triple\" returntype=\"numeric\">\n"
                                            "  <cfargument name=\"n\" type=\"numeric\">\n"
                                            "  <cfreturn n * 3>\n"
                                            "</cffunction>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both cffunction tags must be extracted as Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* CFML: no OOP class nodes expected (tag dialect has no class construct). */
TEST(probe_cfml_no_type_nodes) {
    GpdMetrics m = gpd_metrics("simple.cfm", "<cffunction name=\"noop\" returntype=\"void\">\n"
                                             "</cffunction>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: CFML tag dialect produces 0 type-like nodes. */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 5 — CFScript (.cfc — script dialect)
 *
 * CFScript label histogram: Function:2, Module:1 (from grammar_labels for cfscript).
 * Spec: cfscript_func_types = {"function_declaration","function_expression",
 *                              "arrow_function","method_definition"}.
 * Extension: .cfc → CBM_LANG_CFSCRIPT.
 * ══════════════════════════════════════════════════════════════════ */

/* CFScript: component with two function declarations → 2 Function nodes. */
TEST(probe_cfscript_functions) {
    GpdMetrics m = gpd_metrics("MathService.cfc", "component {\n"
                                                  "  function double(numeric n) {\n"
                                                  "    return n * 2;\n"
                                                  "  }\n"
                                                  "\n"
                                                  "  function triple(numeric n) {\n"
                                                  "    return n * 3;\n"
                                                  "  }\n"
                                                  "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both function declarations must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* CFScript: no cross-file import syntax modeled — pipeline produces 0 IMPORTS. */
TEST(probe_cfscript_no_import_edges) {
    GpdMetrics m = gpd_metrics("Single.cfc", "component {\n"
                                             "  function greet(string name) {\n"
                                             "    return \"Hello, \" & name;\n"
                                             "  }\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: single-file CFScript produces 0 IMPORTS edges. */
    ASSERT_TRUE(m.imports == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 6 — COBOL (.cob)
 *
 * COBOL label histogram: Class:1, Function:1 (from MUST_EXTRACT_DEFS fixture).
 * Spec: cobol_func_types = {"program_definition"},
 *       cobol_import_types = {"open_statement","use_statement","with_clause"}.
 * Extension: .cob → CBM_LANG_COBOL.
 * ══════════════════════════════════════════════════════════════════ */

/* COBOL: PROGRAM-ID → Function (program_definition) node. */
TEST(probe_cobol_program_node) {
    GpdMetrics m = gpd_metrics("hello.cob", "       IDENTIFICATION DIVISION.\n"
                                            "       PROGRAM-ID. HELLO.\n"
                                            "       PROCEDURE DIVISION.\n"
                                            "           MAIN-PARA.\n"
                                            "               DISPLAY 'Hello, World!'.\n"
                                            "               STOP RUN.\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: PROGRAM-ID must produce a Function node (program_definition). */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* COBOL: pipeline indexes the file and produces at least 1 node. */
TEST(probe_cobol_no_crash) {
    GpdMetrics m = gpd_metrics("calc.cob", "       IDENTIFICATION DIVISION.\n"
                                           "       PROGRAM-ID. CALC.\n"
                                           "       DATA DIVISION.\n"
                                           "       WORKING-STORAGE SECTION.\n"
                                           "           01 NUM PIC 9(4) VALUE 0.\n"
                                           "       PROCEDURE DIVISION.\n"
                                           "           COMPUTE NUM = 2 + 2.\n"
                                           "           STOP RUN.\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 7 — Elm (.elm)
 *
 * Elm label histogram: Class:1, Function:1, Module:1.
 * Spec: elm_func_types = {"value_declaration","function_declaration"},
 *       elm_class_types = {"type_declaration","type_alias_declaration","module_declaration"},
 *       elm_import_types = {"import"}.
 * Extension: .elm → CBM_LANG_ELM.
 * ══════════════════════════════════════════════════════════════════ */

/* Elm: function definition → Function node. */
TEST(probe_elm_function) {
    GpdMetrics m = gpd_metrics("Math.elm", "module Math exposing (..)\n"
                                           "\n"
                                           "double : Int -> Int\n"
                                           "double n = n * 2\n"
                                           "\n"
                                           "triple : Int -> Int\n"
                                           "triple n = n * 3\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both function definitions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Elm: type alias declaration → type-like node. */
TEST(probe_elm_type_alias) {
    GpdMetrics m = gpd_metrics("Types.elm", "module Types exposing (..)\n"
                                            "\n"
                                            "type alias Point = { x : Float, y : Float }\n"
                                            "\n"
                                            "type Shape\n"
                                            "    = Circle Float\n"
                                            "    | Rectangle Float Float\n"
                                            "\n"
                                            "area : Shape -> Float\n"
                                            "area s =\n"
                                            "    case s of\n"
                                            "        Circle r -> 3.14159 * r * r\n"
                                            "        Rectangle w h -> w * h\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: type alias/type declaration must produce type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_elm_imports_edge) {
    static const GpdFile files[] = {{"Utils.elm", "module Utils exposing (..)\n"
                                                  "\n"
                                                  "double : Int -> Int\n"
                                                  "double n = n * 2\n"},
                                    {"Main.elm", "module Main exposing (..)\n"
                                                 "\n"
                                                 "import Utils\n"
                                                 "\n"
                                                 "quad : Int -> Int\n"
                                                 "quad n = Utils.double (Utils.double n)\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/elm.json",
         "{\"type\":\"application\",\"source-directories\":[\"src\"],\"elm-version\":\"0.19.1\","
         "\"dependencies\":{\"direct\":{\"elm/core\":\"1.0.5\",\"elm/"
         "json\":\"1.1.3\"},\"indirect\":{}},\"test-dependencies\":{\"direct\":{},\"indirect\":{}}}"
         "\n"},
        {"configured/src/Utils.elm",
         "module Utils exposing (..)\n\ndouble : Int -> Int\ndouble n = n * 2\n"},
        {"configured/src/Main.elm", "module Main exposing (..)\n\nimport Utils\n\nquad : Int -> "
                                    "Int\nquad n = Utils.double (Utils.double n)\n"},
        {"configured/src/Other.elm",
         "module Other exposing (..)\ndouble : Int -> Int\ndouble n = n * 3\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_elm_imports_edge-exact-configured-import",
        .site = {.file = "configured/src/Main.elm",
                 .caller = "configured.src.Main.elm.__file__",
                 .needle = "import Utils"},
        .relation = "IMPORTS",
        .target = {.file = "configured/src/Utils.elm",
                   .suffix = "configured.src.Utils.elm.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "module Utils exposing (..)"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "elm-configured-source-directory-import",
        .why = "The explicit elm.json source directory contains one module Utils; the declared "
               "module and relative module path agree. A double in module Other is a different "
               "declaration. No audited Elm import domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {.case_id = "probe_elm_imports_edge-wrong-target",
                                    .site = {.file = "configured/src/Main.elm",
                                             .caller = "configured.src.Main.elm.__file__",
                                             .needle = "import Utils"},
                                    .relation = "IMPORTS",
                                    .target = {.file = "configured/src/Other.elm",
                                               .suffix = "configured.src.Other.elm.__file__",
                                               .label = "File",
                                               .declaration_line = 1,
                                               .declaration_text = "module Other exposing (..)"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]), "probe_elm_imports_edge",
                                   &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 8 — FunC (.fc)
 *
 * FunC label histogram: Function:1, Module:1.
 * Spec: func_func_types = {"function_definition"},
 *       func_import_types = {"include_directive"}.
 * Extension: .fc → CBM_LANG_FUNC.
 * ══════════════════════════════════════════════════════════════════ */

/* FunC: function definition → Function node. */
TEST(probe_func_function) {
    GpdMetrics m = gpd_metrics("math.fc", "int square(int n) {\n"
                                          "    return n * n;\n"
                                          "}\n"
                                          "\n"
                                          "int cube(int n) {\n"
                                          "    return n * square(n);\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both function definitions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* FunC: incomplete directives abstain; complete literal includes stay required. */
TEST(probe_func_imports_edge) {
    static const GpdFile files[] = {{"utils.fc", "int double_val(int n) {\n"
                                                 "    return n * 2;\n"
                                                 "}\n"},
                                    {"main.fc", "#include \"utils.fc\"\n"
                                                "\n"
                                                "int run(int n) {\n"
                                                "    return double_val(n);\n"
                                                "}\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* The original directive lacks FunC's required semicolon. */
    ASSERT_EQ(m.imports, 0);
    const GpdFile valid[] = {files[0], {"valid.fc", "#include \"utils.fc\";\n() valid() {}\n"}};
    GpdProj lp;
    cbm_store_t *store = gpd_index_files(&lp, valid, 2);
    sqlite3_stmt *query = NULL;
    /* Legacy import records have 0/0 spans. Require exact physical endpoints
     * and matching persisted evidence without claiming an occurrence span. */
    const char *sql = "SELECT count(*) FROM resolver_occurrences r "
                      "JOIN nodes s ON s.project=r.project AND s.qualified_name=r.scope "
                      "JOIN nodes t ON t.project=r.project AND t.qualified_name=r.target "
                      "JOIN edges e ON e.source_id=s.id AND e.target_id=t.id AND e.type=r.relation "
                      "WHERE r.project=?1 AND r.relation='IMPORTS' AND r.emitted=1 "
                      "AND r.start_byte=0 AND r.end_byte=0 AND s.file_path='valid.fc' "
                      "AND s.label='File' AND t.file_path='utils.fc' AND t.label='File'";
    bool ok = store && cbm_store_count_edges_by_type(store, lp.project, "IMPORTS") == 1 &&
              sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &query, NULL) == SQLITE_OK &&
              sqlite3_bind_text(query, 1, lp.project, -1, SQLITE_STATIC) == SQLITE_OK &&
              sqlite3_step(query) == SQLITE_ROW && sqlite3_column_int(query, 0) == 1;
    sqlite3_finalize(query);
    gpd_cleanup(&lp, store);
    ASSERT_TRUE(ok);
    PASS();
}

/* FunC: no OOP — no type-like nodes expected. */
TEST(probe_func_no_type_nodes) {
    GpdMetrics m = gpd_metrics("noop.fc", "() noop() {\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: FunC produces 0 type-like nodes. */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 9 — Janet (.janet)
 *
 * Janet label histogram: Module:1 only (from grammar_labels MUST_EXTRACT_DEFS).
 * Spec: CBM_LANG_JANET uses empty_types for all definition types — no function
 *       extraction is configured; the grammar emits only a source/Module node.
 * Extension: .janet → CBM_LANG_JANET.
 * ══════════════════════════════════════════════════════════════════ */

/* Janet: pipeline indexes the file without crashing — total_nodes >= 1. */
TEST(probe_janet_no_crash) {
    GpdMetrics m = gpd_metrics("math.janet", "(defn double [n] (* 2 n))\n"
                                             "\n"
                                             "(defn square [n] (* n n))\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash; at least Module node expected. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Janet: functions NOT extracted — the spec has empty_types for func.
 * RED: defn forms not configured in the lang spec; 0 Function nodes is the
 *      current behaviour and a known gap. */
TEST(probe_janet_function_extraction_gap) {
    GpdMetrics m = gpd_metrics("ops.janet", "(defn add [a b] (+ a b))\n"
                                            "(defn sub [a b] (- a b))\n"
                                            "(defn mul [a b] (* a b))\n");
    ASSERT_TRUE(m.ok);
    /* RED: Janet defn forms not configured in lang_spec → 0 Function nodes. */
    ASSERT_TRUE(m.functions >= 1); /* expected RED — empty func_types in spec */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 10 — Lean (.lean)
 *
 * Lean label histogram: Function:2, Module:1.
 * Spec: lean_func_types = {"def","theorem","instance","abbrev"},
 *       lean_class_types = {"structure","class_inductive","inductive"},
 *       lean_import_types = {"import","extends","instance"}.
 * Extension: .lean → CBM_LANG_LEAN.
 * ══════════════════════════════════════════════════════════════════ */

/* Lean: two `def` definitions → 2 Function nodes. */
TEST(probe_lean_def_functions) {
    GpdMetrics m = gpd_metrics("math.lean", "def double (n : Nat) : Nat := n * 2\n"
                                            "\n"
                                            "def square (n : Nat) : Nat := n * n\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both def bindings must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Lean: theorem definition → Function node. */
TEST(probe_lean_theorem) {
    GpdMetrics m = gpd_metrics("proofs.lean", "theorem add_comm (a b : Nat) : a + b = b + a := by\n"
                                              "  omega\n"
                                              "\n"
                                              "theorem mul_comm (a b : Nat) : a * b = b * a := by\n"
                                              "  omega\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: theorem definitions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Lean: structure definition → type-like node.
 * RED: histogram shows Function/Module only; structure not yet extracted as
 *      type node despite lean_class_types having "structure". */
TEST(probe_lean_structure_type) {
    GpdMetrics m = gpd_metrics("types.lean", "structure Point where\n"
                                             "  x : Float\n"
                                             "  y : Float\n"
                                             "\n"
                                             "def origin : Point := { x := 0, y := 0 }\n");
    ASSERT_TRUE(m.ok);
    /* RED: Lean structure not extracted as type-like node (node-extraction gap). */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_lean_imports_edge) {
    static const GpdFile files[] = {{"MathUtils.lean", "def double (n : Nat) : Nat := n * 2\n"},
                                    {"Main.lean",
                                     "import MathUtils\n"
                                     "\n"
                                     "def quad (n : Nat) : Nat := double (double n)\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/lakefile.toml", "name = \"Quality\"\nversion = \"0.1.0\"\ndefaultTargets = "
                                     "[\"Quality\"]\n\n[[lean_lib]]\nname = \"Quality\"\nsrcDir = "
                                     "\"src\"\nroots = [\"MathUtils\", \"Main\", \"Other\"]\n"},
        {"configured/src/MathUtils.lean", "def double (n : Nat) : Nat := n * 2\n"},
        {"configured/src/Main.lean",
         "import MathUtils\n\ndef quad (n : Nat) : Nat := double (double n)\n"},
        {"configured/src/Other.lean",
         "namespace Other\ndef double (n : Nat) : Nat := n * 3\nend Other\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_lean_imports_edge-exact-configured-import",
        .site = {.file = "configured/src/Main.lean",
                 .caller = "configured.src.Main.lean.__file__",
                 .needle = "import MathUtils"},
        .relation = "IMPORTS",
        .target = {.file = "configured/src/MathUtils.lean",
                   .suffix = "configured.src.MathUtils.lean.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "def double (n : Nat) : Nat := n * 2"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "lean-configured-library-module-import",
        .why = "The Lake library explicitly lists MathUtils and Main as roots under src. This "
               "fixes source-module membership independently of ambient LEAN_PATH. Other.double is "
               "not the imported module. No audited Lean import domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {.case_id = "probe_lean_imports_edge-wrong-target",
                                    .site = {.file = "configured/src/Main.lean",
                                             .caller = "configured.src.Main.lean.__file__",
                                             .needle = "import MathUtils"},
                                    .relation = "IMPORTS",
                                    .target = {.file = "configured/src/Other.lean",
                                               .suffix = "configured.src.Other.lean.__file__",
                                               .label = "File",
                                               .declaration_line = 1,
                                               .declaration_text = "namespace Other"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]), "probe_lean_imports_edge",
                                   &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 11 — LLVM IR (.ll)
 *
 * LLVM IR label histogram: Function:1, Module:1.
 * Spec: llvm_func_types = {"function_header"}, no class/import types.
 * Extension: .ll → CBM_LANG_LLVM_IR.
 * LLVM IR has no OOP and no import mechanism at the IR level.
 * ══════════════════════════════════════════════════════════════════ */

/* LLVM IR: function definition → Function node. */
TEST(probe_llvmir_function) {
    GpdMetrics m = gpd_metrics("add.ll", "define i32 @add(i32 %a, i32 %b) {\n"
                                         "entry:\n"
                                         "  %r = add i32 %a, %b\n"
                                         "  ret i32 %r\n"
                                         "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: LLVM IR function definition must produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* LLVM IR: two function definitions → 2 Function nodes. */
TEST(probe_llvmir_two_functions) {
    GpdMetrics m = gpd_metrics("math.ll", "define i32 @square(i32 %n) {\n"
                                          "entry:\n"
                                          "  %r = mul i32 %n, %n\n"
                                          "  ret i32 %r\n"
                                          "}\n"
                                          "\n"
                                          "define i32 @negate(i32 %n) {\n"
                                          "entry:\n"
                                          "  %r = sub i32 0, %n\n"
                                          "  ret i32 %r\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both function definitions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* LLVM IR: no OOP — no type-like nodes expected. */
TEST(probe_llvmir_no_type_nodes) {
    GpdMetrics m = gpd_metrics("noop.ll", "define void @noop() {\n"
                                          "entry:\n"
                                          "  ret void\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: LLVM IR produces 0 type-like nodes. */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 12 — Magma (.mag)
 *
 * Magma label histogram: Function:2, Module:1 (from grammar_labels).
 * Spec: magma_func_types = {"function_definition","procedure_definition",
 *                           "intrinsic_definition","anonymous_function"},
 *       magma_import_types = {"load_statement","require","require_statement"}.
 * Extension: .mag → CBM_LANG_MAGMA.
 * Note: .m also resolves to Magma if content has end function/procedure markers
 *       (via cbm_disambiguate_m), but .mag is unambiguous.
 * ══════════════════════════════════════════════════════════════════ */

/* Magma: function definition → Function node. */
TEST(probe_magma_function) {
    GpdMetrics m = gpd_metrics("arith.mag", "function Square(n)\n"
                                            "    return n * n;\n"
                                            "end function;\n"
                                            "\n"
                                            "function Cube(n)\n"
                                            "    return n * Square(n);\n"
                                            "end function;\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both function definitions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 2);
    PASS();
}

/* Magma: procedure definition → Function node (procedures are in func_types). */
TEST(probe_magma_procedure) {
    GpdMetrics m = gpd_metrics("proc.mag", "procedure PrintVal(n)\n"
                                           "    print n;\n"
                                           "end procedure;\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: procedure must produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Magma: no OOP types — Magma is functional/mathematical. */
TEST(probe_magma_no_type_nodes) {
    GpdMetrics m = gpd_metrics("simple.mag", "function Id(x)\n"
                                             "    return x;\n"
                                             "end function;\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Magma produces 0 type-like nodes. */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 13 — Move (.move)
 *
 * Move label histogram: Function:1, Module:1.
 * Spec: move_func_types = {"function_item"}, no class types (grammar comment:
 *   struct/enum exist only as anonymous keyword tokens, never as parent nodes),
 *   move_import_types = {"use_declaration"}.
 * Extension: .move → CBM_LANG_MOVE.
 * ══════════════════════════════════════════════════════════════════ */

/* Move: function inside module → Function node. */
TEST(probe_move_function) {
    GpdMetrics m = gpd_metrics("math.move", "module 0x1::math {\n"
                                            "    public fun double(n: u64): u64 {\n"
                                            "        n * 2\n"
                                            "    }\n"
                                            "\n"
                                            "    public fun square(n: u64): u64 {\n"
                                            "        n * n\n"
                                            "    }\n"
                                            "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both function_item nodes must reach the graph. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_move_imports_edge) {
    static const GpdFile files[] = {{"utils.move", "module 0x1::utils {\n"
                                                   "    public fun double(n: u64): u64 { n * 2 }\n"
                                                   "}\n"},
                                    {"main.move", "module 0x1::main {\n"
                                                  "    use 0x1::utils;\n"
                                                  "\n"
                                                  "    public fun run(n: u64): u64 {\n"
                                                  "        utils::double(n)\n"
                                                  "    }\n"
                                                  "}\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/Move.toml", "[package]\nname = \"Quality\"\nversion = \"0.0.0\"\n"},
        {"configured/sources/utils.move",
         "module 0x1::utils {\n    public fun double(n: u64): u64 { n * 2 }\n}\n"},
        {"configured/sources/main.move",
         "module 0x1::main {\n    use 0x1::utils;\n\n    public fun run(n: u64): u64 {\n        "
         "utils::double(n)\n    }\n}\n"},
        {"configured/sources/other.move",
         "module 0x2::utils {\n    public fun double(n: u64): u64 { n * 3 }\n}\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_move_imports_edge-exact-configured-import",
        .site = {.file = "configured/sources/main.move",
                 .caller = "configured.sources.main.move.__file__",
                 .needle = "use 0x1::utils;"},
        .relation = "IMPORTS",
        .target = {.file = "configured/sources/utils.move",
                   .suffix = "configured.sources.utils.move.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "module 0x1::utils {"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "move-configured-address-module-import",
        .why = "The explicit package contains these source modules. The fully addressed use "
               "0x1::utils selects that module, not the same module leaf at address 0x2. No "
               "audited Move address-and-package import domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {.case_id = "probe_move_imports_edge-wrong-target",
                                    .site = {.file = "configured/sources/main.move",
                                             .caller = "configured.sources.main.move.__file__",
                                             .needle = "use 0x1::utils;"},
                                    .relation = "IMPORTS",
                                    .target = {.file = "configured/sources/other.move",
                                               .suffix = "configured.sources.other.move.__file__",
                                               .label = "File",
                                               .declaration_line = 1,
                                               .declaration_text = "module 0x2::utils {"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]), "probe_move_imports_edge",
                                   &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* Move: struct definition — grammar models struct only as anonymous token, not
 * as a named parent node.  Verify no spurious type nodes are emitted. */
TEST(probe_move_struct_not_extracted) {
    GpdMetrics m = gpd_metrics("types.move", "module 0x1::types {\n"
                                             "    struct Point has copy, drop {\n"
                                             "        x: u64,\n"
                                             "        y: u64,\n"
                                             "    }\n"
                                             "\n"
                                             "    public fun origin(): Point {\n"
                                             "        Point { x: 0, y: 0 }\n"
                                             "    }\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Move grammar produces 0 type nodes (struct is anonymous token). */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 14 — NASM (.nasm)
 *
 * NASM label histogram: Function:1, Module:1.
 * Spec: nasm_func_types = {"label","preproc_def","preproc_multiline_macro"},
 *       nasm_class_types = {"struc_declaration"},
 *       nasm_import_types = {"preproc_include"}.
 * Extension: .nasm → CBM_LANG_NASM.
 * ══════════════════════════════════════════════════════════════════ */

/* NASM: label → Function node. */
TEST(probe_nasm_label_function) {
    GpdMetrics m = gpd_metrics("add.nasm", "global add\n"
                                           "add:\n"
                                           "    mov eax, edi\n"
                                           "    add eax, esi\n"
                                           "    ret\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: label must be extracted as Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* NASM: struc declaration → type-like node.
 * RED: histogram shows Function:1/Module:1 only; struc_declaration not
 *      extracted as a type node despite nasm_class_types spec. */
TEST(probe_nasm_struc_node) {
    GpdMetrics m = gpd_metrics("point.nasm", "struc Point\n"
                                             "    .x: resd 1\n"
                                             "    .y: resd 1\n"
                                             "endstruc\n"
                                             "\n"
                                             "global get_x\n"
                                             "get_x:\n"
                                             "    mov eax, [edi + Point.x]\n"
                                             "    ret\n");
    ASSERT_TRUE(m.ok);
    /* RED: NASM struc_declaration not extracted as type-like node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_nasm_include_edge) {
    static const GpdFile files[] = {{"utils.nasm", "global double\n"
                                                   "double:\n"
                                                   "    shl edi, 1\n"
                                                   "    mov eax, edi\n"
                                                   "    ret\n"},
                                    {"main.nasm", "%include \"utils.nasm\"\n"
                                                  "\n"
                                                  "global run\n"
                                                  "run:\n"
                                                  "    call double\n"
                                                  "    ret\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/utils.nasm",
         "global double\ndouble:\n    shl edi, 1\n    mov eax, edi\n    ret\n"},
        {"configured/main.nasm",
         "%include \"utils.nasm\"\n\nglobal run\nrun:\n    call double\n    ret\n"},
        {"configured/compile.sh", "#!/bin/sh\nset -eu\ncd \"$(dirname \"$0\")\"\nexec nasm -I ./ "
                                  "-f elf64 main.nasm -o main.o\n"},
        {"configured/decoy.nasm", "global double\ndouble:\n    mov eax, 3\n    ret\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_nasm_include_edge-exact-configured-import",
        .site = {.file = "configured/main.nasm",
                 .caller = "configured.main.nasm.__file__",
                 .needle = "%include \"utils.nasm\""},
        .relation = "IMPORTS",
        .target = {.file = "configured/utils.nasm",
                   .suffix = "configured.utils.nasm.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "global double"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "nasm-configured-literal-include",
        .why = "The launcher fixes the working directory and explicit include root. The literal "
               "filename is utils.nasm; another file exporting double is not its target. No "
               "audited NASM include domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {.case_id = "probe_nasm_include_edge-wrong-target",
                                    .site = {.file = "configured/main.nasm",
                                             .caller = "configured.main.nasm.__file__",
                                             .needle = "%include \"utils.nasm\""},
                                    .relation = "IMPORTS",
                                    .target = {.file = "configured/decoy.nasm",
                                               .suffix = "configured.decoy.nasm.__file__",
                                               .label = "File",
                                               .declaration_line = 1,
                                               .declaration_text = "global double"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]), "probe_nasm_include_edge",
                                   &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 15 — Objective-C (.m with ObjC markers)
 *
 * ObjC label histogram: Class:1, Function:2, Module:1 (from grammar_labels).
 * Spec: objc_func_types = {"function_definition","method_definition","method_declaration"},
 *       objc_class_types = {"class_interface","class_implementation","protocol_declaration",...},
 *       objc_import_types = {"preproc_import","preproc_include"}.
 * Extension: .m → disambiguated to CBM_LANG_OBJC when @interface/@implementation
 *             markers are present (via cbm_disambiguate_m).
 * ══════════════════════════════════════════════════════════════════ */

/* ObjC: @interface declaration → Class node. */
TEST(probe_objc_class_node) {
    GpdMetrics m = gpd_metrics("Animal.m", "#import <Foundation/Foundation.h>\n"
                                           "\n"
                                           "@interface Animal : NSObject\n"
                                           "- (NSString *)speak;\n"
                                           "- (NSString *)name;\n"
                                           "@end\n"
                                           "\n"
                                           "@implementation Animal\n"
                                           "- (NSString *)speak { return @\"...\"; }\n"
                                           "- (NSString *)name { return @\"Animal\"; }\n"
                                           "@end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: @interface must produce a Class/type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* ObjC: methods → Function/Method nodes. */
TEST(probe_objc_method_nodes) {
    GpdMetrics m = gpd_metrics("Calc.m", "@interface Calc : NSObject\n"
                                         "- (int)square:(int)n;\n"
                                         "- (int)cube:(int)n;\n"
                                         "@end\n"
                                         "\n"
                                         "@implementation Calc\n"
                                         "- (int)square:(int)n { return n * n; }\n"
                                         "- (int)cube:(int)n { return n * n * n; }\n"
                                         "@end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: ObjC method_definition is labelled "Method" (extract_defs.c
     *        sets def.label = "Method" for CBM_LANG_OBJC method_definition),
     *        not "Function" — matches grammar_labels histogram objc=Method:1.
     *        Fixture fix: assert Method, not Function. */
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* ObjC: subclass via @interface Child : Parent → INHERITS edge.
 * RED: grammar-only ObjC has no base-class resolver in the pipeline. */
TEST(probe_objc_inherits_edge) {
    GpdMetrics m = gpd_metrics("Dog.m", "@interface Animal : NSObject\n"
                                        "- (NSString *)speak;\n"
                                        "@end\n"
                                        "\n"
                                        "@interface Dog : Animal\n"
                                        "- (void)bark;\n"
                                        "@end\n"
                                        "\n"
                                        "@implementation Dog\n"
                                        "- (NSString *)speak { return @\"Woof\"; }\n"
                                        "- (void)bark { NSLog(@\"Woof!\"); }\n"
                                        "@end\n");
    ASSERT_TRUE(m.ok);
    /* RED: ObjC @interface inheritance not yet resolved into INHERITS edges. */
    ASSERT_TRUE(m.inherits >= 1); /* expected RED */
    PASS();
}

/* ObjC: #import in two-file fixture → IMPORTS edge.
 * RED: grammar-only ObjC has no #import resolver in the pipeline. */
TEST(probe_objc_import_edge) {
    static const GpdFile files[] = {{"Utils.m", "@interface Utils : NSObject\n"
                                                "+ (int)double:(int)n;\n"
                                                "@end\n"
                                                "@implementation Utils\n"
                                                "+ (int)double:(int)n { return n * 2; }\n"
                                                "@end\n"},
                                    {"Main.m", "#import \"Utils.m\"\n"
                                               "\n"
                                               "@interface Main : NSObject\n"
                                               "+ (int)run:(int)n;\n"
                                               "@end\n"
                                               "@implementation Main\n"
                                               "+ (int)run:(int)n { return [Utils double:n]; }\n"
                                               "@end\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: ObjC #import not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 16 — Pony (.pony)
 *
 * Pony label histogram: Class:1, Function:1, Module:1 (from grammar_labels).
 * Spec: pony_func_types = {"method","constructor","ffi_method","lambda_expression"},
 *       pony_class_types = {"actor_definition","class_definition","struct_definition",
 *                           "trait_definition","interface_definition","primitive_definition",
 *                           "type_alias"},
 *       pony_import_types = {"use_statement"}.
 * Extension: .pony → CBM_LANG_PONY.
 * ══════════════════════════════════════════════════════════════════ */

/* Pony: class definition → Class node. */
TEST(probe_pony_class_node) {
    GpdMetrics m = gpd_metrics("Animal.pony", "class Animal\n"
                                              "  let _name: String\n"
                                              "\n"
                                              "  new create(name: String) =>\n"
                                              "    _name = name\n"
                                              "\n"
                                              "  fun name(): String =>\n"
                                              "    _name\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: class_definition must produce a type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Pony: actor definition → type-like node. */
TEST(probe_pony_actor_node) {
    GpdMetrics m = gpd_metrics("Counter.pony", "actor Counter\n"
                                               "  var _count: U64 = 0\n"
                                               "\n"
                                               "  be increment() =>\n"
                                               "    _count = _count + 1\n"
                                               "\n"
                                               "  be reset() =>\n"
                                               "    _count = 0\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: actor_definition must produce a type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Pony: methods (fun/be/new) inside a type → Method nodes. */
TEST(probe_pony_method_nodes) {
    GpdMetrics m = gpd_metrics("Math.pony", "primitive Math\n"
                                            "  fun square(n: U64): U64 =>\n"
                                            "    n * n\n"
                                            "\n"
                                            "  fun cube(n: U64): U64 =>\n"
                                            "    n * square(n)\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: fun methods inside a primitive/actor/class are promoted to Method
     * nodes (extract_defs.c Pony method-promotion), so assert on m.methods. */
    ASSERT_TRUE(m.methods >= 1);
    PASS();
}

/* Pony: trait with interface → type-like node. */
TEST(probe_pony_trait_node) {
    GpdMetrics m = gpd_metrics("Speakable.pony", "trait Speakable\n"
                                                 "  fun speak(): String\n"
                                                 "\n"
                                                 "class Dog is Speakable\n"
                                                 "  fun speak(): String =>\n"
                                                 "    \"Woof!\"\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: both trait and class must produce type-like nodes. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Pony imports package directories, never a same-named sibling source file. */
TEST(probe_pony_imports_edge) {
    static const GpdFile files[] = {{"util.pony", "primitive MathUtil\n"
                                                  "  fun double(n: U64): U64 =>\n"
                                                  "    n * 2\n"},
                                    {"main.pony", "use \"./util\"\n"
                                                  "\n"
                                                  "actor Main\n"
                                                  "  new create(env: Env) =>\n"
                                                  "    let r = MathUtil.double(21)\n"
                                                  "    env.out.print(r.string())\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    const GpdFile quality_files[] = {files[0], files[1], {"util/math.pony", files[0].content}};
    RQ_Source sources[sizeof(quality_files) / sizeof(quality_files[0])];
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++)
        sources[i] = (RQ_Source){.path = quality_files[i].name, .text = quality_files[i].content};
    const RQ_Expected expected = {
        .case_id = "probe_pony_imports_edge-exact-dependency",
        .site = {.file = "main.pony", .caller = "main.pony.__file__", .needle = "use \"./util\""},
        .relation = "IMPORTS",
        .target = {.file = "util/math.pony",
                   .suffix = "util.math.pony.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "primitive MathUtil"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "pony-relative-package-membership-proof",
        .why = "Pony use selects a package directory. Its sole source member is util/math.pony; "
               "sibling util.pony is not that package. Package-membership proof is unsupported.",
        .tried = "Preserved original sources and indexing checks; exact target, wrong-target "
                 "control, and required ESM call use full production indexing."};
    const RQ_Forbidden forbidden = {
        .case_id = "probe_pony_imports_edge-invalid-target",
        .site = {.file = "main.pony", .caller = "main.pony.__file__", .needle = "use \"./util\""},
        .relation = "IMPORTS",
        .target = {.file = "util.pony",
                   .suffix = "util.pony.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "primitive MathUtil"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(quality_files, sizeof(quality_files) / sizeof(quality_files[0]),
                                   sources, sizeof(sources) / sizeof(sources[0]),
                                   "probe_pony_imports_edge", &expected, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 17 — PureScript (.purs)
 *
 * PureScript label histogram: Class:1, Function:1, Module:1 (from grammar_labels).
 * Spec: purescript_func_types = {"function"},
 *       purescript_class_types = {"class_declaration","data","newtype","type_alias"},
 *       purescript_import_types = {"import","import_item","instance"}.
 * Extension: .purs → CBM_LANG_PURESCRIPT.
 * ══════════════════════════════════════════════════════════════════ */

/* PureScript: function definition → Function node. */
TEST(probe_purescript_function) {
    GpdMetrics m = gpd_metrics("Math.purs", "module Math where\n"
                                            "\n"
                                            "double :: Int -> Int\n"
                                            "double n = n * 2\n"
                                            "\n"
                                            "square :: Int -> Int\n"
                                            "square n = n * n\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: function definitions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* PureScript: data/type declaration → type-like node. */
TEST(probe_purescript_data_type) {
    GpdMetrics m = gpd_metrics("Types.purs", "module Types where\n"
                                             "\n"
                                             "data Shape\n"
                                             "  = Circle Number\n"
                                             "  | Rectangle Number Number\n"
                                             "\n"
                                             "type Alias = Shape\n"
                                             "\n"
                                             "area :: Shape -> Number\n"
                                             "area (Circle r) = 3.14159 * r * r\n"
                                             "area (Rectangle w h) = w * h\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: data declaration must produce a type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_purescript_imports_edge) {
    static const GpdFile files[] = {{"Utils.purs", "module Utils where\n"
                                                   "\n"
                                                   "double :: Int -> Int\n"
                                                   "double n = n * 2\n"},
                                    {"Main.purs", "module Main where\n"
                                                  "\n"
                                                  "import Utils (double)\n"
                                                  "\n"
                                                  "quad :: Int -> Int\n"
                                                  "quad n = double (double n)\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/src/Utils.purs", "module Utils (Token(..)) where\ndata Token = Token\n"},
        {"configured/src/Main.purs",
         "module Main where\nimport Utils (Token(..))\nvalue :: Token\nvalue = Token\n"},
        {"configured/src/Other.purs", "module Other (Token(..)) where\ndata Token = Token\n"},
        {"configured/compile.sh", "#!/bin/sh\nset -eu\ncd \"$(dirname \"$0\")\"\nexec purs compile "
                                  "src/Utils.purs src/Main.purs src/Other.purs\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_purescript_imports_edge-exact-configured-import",
        .site = {.file = "configured/src/Main.purs",
                 .caller = "configured.src.Main.purs.__file__",
                 .needle = "import Utils (Token(..))"},
        .relation = "IMPORTS",
        .target = {.file = "configured/src/Utils.purs",
                   .suffix = "configured.src.Utils.purs.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "module Utils (Token(..)) where"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "purescript-explicit-compilation-module-import",
        .why =
            "The launcher supplies an exact closed source-module set. Utils exports Token, while "
            "Other owns a distinct Token. The self-contained companion avoids the original missing "
            "Prelude arithmetic import. No audited PureScript import domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {
        .case_id = "probe_purescript_imports_edge-wrong-target",
        .site = {.file = "configured/src/Main.purs",
                 .caller = "configured.src.Main.purs.__file__",
                 .needle = "import Utils (Token(..))"},
        .relation = "IMPORTS",
        .target = {.file = "configured/src/Other.purs",
                   .suffix = "configured.src.Other.purs.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "module Other (Token(..)) where"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]),
                                   "probe_purescript_imports_edge", &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 18 — Pine Script (.pine)
 *
 * Pine label histogram: Function:1, Module:1 (from grammar_labels).
 * Spec: pine_func_types = {"function_declaration_statement"},
 *       pine_class_types = {"type_definition_statement"}.
 * Extension: .pine → CBM_LANG_PINE.
 * ══════════════════════════════════════════════════════════════════ */

/* Pine Script: function declaration → Function node. */
TEST(probe_pine_function) {
    GpdMetrics m = gpd_metrics("indicator.pine", "//@version=5\n"
                                                 "indicator('My Indicator')\n"
                                                 "\n"
                                                 "double(n) =>\n"
                                                 "    n * 2\n"
                                                 "\n"
                                                 "square(n) =>\n"
                                                 "    n * n\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: function_declaration_statement must produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Pine Script: type definition → type-like node.
 * RED: histogram shows Function/Module only; type_definition_statement not
 *      extracted as type-like node despite pine_class_types spec. */
TEST(probe_pine_type_node) {
    GpdMetrics m = gpd_metrics("types.pine", "//@version=5\n"
                                             "indicator('Types')\n"
                                             "\n"
                                             "type Point\n"
                                             "    float x = 0\n"
                                             "    float y = 0\n"
                                             "\n"
                                             "makePoint(x, y) =>\n"
                                             "    Point.new(x, y)\n");
    ASSERT_TRUE(m.ok);
    /* RED: Pine Script type_definition_statement not extracted as type-like node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 19 — QML (.qml)
 *
 * QML label histogram: Class:1, Function:2, Module:1 (from grammar_labels).
 * Spec: qml_class_types = {"class_declaration","class","abstract_class_declaration",
 *                          "enum_declaration","interface_declaration","ui_inline_component"},
 *       qml_import_types = {"import_statement","import","ui_import"}.
 *       Function types reuse js_func_types (QMLJS is a TS/JS superset).
 * Extension: .qml → CBM_LANG_QML.
 * ══════════════════════════════════════════════════════════════════ */

/* QML: JavaScript function inside a component → Function node. */
TEST(probe_qml_function) {
    GpdMetrics m = gpd_metrics("Button.qml", "import QtQuick 2.15\n"
                                             "\n"
                                             "Rectangle {\n"
                                             "    id: root\n"
                                             "    width: 100; height: 40\n"
                                             "\n"
                                             "    function greet(name) {\n"
                                             "        return \"Hello, \" + name\n"
                                             "    }\n"
                                             "\n"
                                             "    function dismiss() {\n"
                                             "        visible = false\n"
                                             "    }\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: JavaScript functions inside QML must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* QML: distinguish invalid quoted QML files from genuine aliased JS resources. */
TEST(probe_qml_imports_edge) {
    static const GpdFile files[] = {{"Utils.qml", "import QtQuick 2.15\n"
                                                  "\n"
                                                  "Rectangle {\n"
                                                  "    function double(n) { return n * 2 }\n"
                                                  "}\n"},
                                    {"Main.qml", "import QtQuick 2.15\n"
                                                 "import \"./Utils.qml\"\n"
                                                 "\n"
                                                 "Rectangle {\n"
                                                 "    function run(n) { return n * 4 }\n"
                                                 "}\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    const GpdFile quality_files[] = {
        files[0],
        files[1],
        {"valid.qml", "import QtQuick 2.15\nimport \"./utils.js\" as Utils\nRectangle {}\n"},
        {"utils.js", "function double(n) { return n * 2; }\n"}};
    RQ_Source sources[sizeof(quality_files) / sizeof(quality_files[0])];
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++)
        sources[i] = (RQ_Source){.path = quality_files[i].name, .text = quality_files[i].content};
    const RQ_Expected expected = {
        .case_id = "probe_qml_imports_edge-exact-dependency",
        .site = {.file = "valid.qml",
                 .caller = "valid.qml.__file__",
                 .needle = "import \"./utils.js\" as Utils"},
        .relation = "IMPORTS",
        .target = {.file = "utils.js",
                   .suffix = "utils.js.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "function double(n) { return n * 2; }"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "qml-relative-javascript-resource-proof",
        .why = "A quoted JavaScript resource imported with an alias is a genuine dependency. The "
               "original quoted QML file is neither a directory nor a JavaScript resource. No "
               "audited QML import adapter exists.",
        .tried = "Preserved original sources and indexing checks; exact target, wrong-target "
                 "control, and required ESM call use full production indexing."};
    const RQ_Forbidden forbidden = {.case_id = "probe_qml_imports_edge-invalid-target",
                                    .site = {.file = "Main.qml",
                                             .caller = "Main.qml.__file__",
                                             .needle = "import \"./Utils.qml\""},
                                    .relation = "IMPORTS",
                                    .target = {.file = "Utils.qml",
                                               .suffix = "Utils.qml.__file__",
                                               .label = "File",
                                               .declaration_line = 1,
                                               .declaration_text = "import QtQuick 2.15"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(quality_files, sizeof(quality_files) / sizeof(quality_files[0]),
                                   sources, sizeof(sources) / sizeof(sources[0]),
                                   "probe_qml_imports_edge", &expected, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 20 — Smali (.smali)
 *
 * Smali label histogram: Class:1, Function:1, Module:1.
 * Spec: smali_func_types = {"method_definition"},
 *       smali_class_types = {"class_definition"},
 *       smali_import_types = {"super_directive","implements_directive"}.
 * Extension: .smali → CBM_LANG_SMALI.
 * ══════════════════════════════════════════════════════════════════ */

/* Smali: class definition → Class node. */
TEST(probe_smali_class_node) {
    GpdMetrics m = gpd_metrics("A.smali", ".class public LA;\n"
                                          ".super Ljava/lang/Object;\n"
                                          "\n"
                                          ".method public constructor <init>()V\n"
                                          "    .registers 1\n"
                                          "    return-void\n"
                                          ".end method\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: class_definition must produce a type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Smali: method definition → Function node. */
TEST(probe_smali_method_node) {
    GpdMetrics m = gpd_metrics("Math.smali", ".class public LMath;\n"
                                             ".super Ljava/lang/Object;\n"
                                             "\n"
                                             ".method public static square(I)I\n"
                                             "    .registers 2\n"
                                             "    mul-int v0, p0, p0\n"
                                             "    return v0\n"
                                             ".end method\n"
                                             "\n"
                                             ".method public static cube(I)I\n"
                                             "    .registers 2\n"
                                             "    mul-int v0, p0, p0\n"
                                             "    mul-int v0, v0, p0\n"
                                             "    return v0\n"
                                             ".end method\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: method definitions must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Smali superclass descriptors are type references. A file-name coincidence
 * cannot supply their classpath identity or justify a module IMPORTS edge. */
TEST(probe_smali_super_import) {
    static const GpdFile files[] = {{"Base.smali", ".class public LBase;\n"
                                                   ".super Ljava/lang/Object;\n"
                                                   "\n"
                                                   ".method public doBase()V\n"
                                                   "    return-void\n"
                                                   ".end method\n"},
                                    {"Child.smali", ".class public LChild;\n"
                                                    ".super LBase;\n"
                                                    "\n"
                                                    ".method public doChild()V\n"
                                                    "    return-void\n"
                                                    ".end method\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_EQ(m.imports, 0);
    const RQ_Source sources[] = {{.path = files[0].name, .text = files[0].content},
                                 {.path = files[1].name, .text = files[1].content}};
    const RQ_Expected gap = {
        .case_id = "smali-superclass-type-reference",
        .site = {.file = "Child.smali", .caller = "Child.LChild;", .needle = "LBase;"},
        .relation = "USES_TYPE",
        .target = {.file = "Base.smali",
                   .suffix = "Base.LBase;",
                   .label = "Class",
                   .declaration_line = 1,
                   .declaration_text = ".class public LBase;"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "smali-descriptor-type-binding",
        .why = "The superclass descriptor is a type reference; no audited Smali type binding "
               "bridge exists.",
        .tried = "Preserved both class declarations and the exact descriptor; replaced the "
                 "aggregate import floor."};
    const RQ_Forbidden forbidden = {
        .case_id = "superclass-descriptor-is-not-module-import",
        .site = {.file = "Child.smali", .caller = "Child", .needle = ".super LBase;"},
        .relation = "IMPORTS",
        .any_target = true};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(files, 2, sources, 2, "smali-superclass-type", &gap, &forbidden,
                                   &report));
    ASSERT_EQ(report.genuine, 2);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 21 — TableGen (.td)
 *
 * TableGen label histogram: Function:1, Module:1.
 * Spec: tablegen_func_types = {"def","multiclass","defm"},
 *       tablegen_class_types = {"class"},
 *       tablegen_import_types = {"include","include_directive"}.
 * Extension: .td → CBM_LANG_TABLEGEN.
 * ══════════════════════════════════════════════════════════════════ */

/* TableGen: `def` record → Function node (def is in func_types). */
TEST(probe_tablegen_def_node) {
    GpdMetrics m = gpd_metrics("regs.td", "def R0 { int num = 0; }\n"
                                          "def R1 { int num = 1; }\n"
                                          "def R2 { int num = 2; }\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: def records must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* TableGen: `class` definition → type-like node.
 * RED: histogram shows Function:1/Module:1 only; class not extracted as
 *      type-like node despite tablegen_class_types spec. */
TEST(probe_tablegen_class_node) {
    GpdMetrics m = gpd_metrics("instrs.td", "class Instruction {\n"
                                            "    string mnemonic = \"\";\n"
                                            "    int opcode = 0;\n"
                                            "}\n"
                                            "\n"
                                            "def ADD : Instruction {\n"
                                            "    let mnemonic = \"add\";\n"
                                            "    let opcode = 1;\n"
                                            "}\n");
    ASSERT_TRUE(m.ok);
    /* RED: TableGen class not extracted as type-like node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED */
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_tablegen_include_edge) {
    static const GpdFile files[] = {{"base.td", "class BaseInstr {\n"
                                                "    int opcode = 0;\n"
                                                "}\n"},
                                    {"derived.td", "include \"base.td\"\n"
                                                   "\n"
                                                   "def ADD : BaseInstr {\n"
                                                   "    let opcode = 1;\n"
                                                   "}\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/base.td", "class BaseInstr {\n    int opcode = 0;\n}\n"},
        {"configured/derived.td",
         "include \"base.td\"\n\ndef ADD : BaseInstr {\n    let opcode = 1;\n}\n"},
        {"configured/compile.sh", "#!/bin/sh\nset -eu\ncd \"$(dirname \"$0\")\"\nexec llvm-tblgen "
                                  "-I . -print-records derived.td\n"},
        {"configured/decoy.td", "class BaseInstr {\n    int opcode = 2;\n}\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_tablegen_include_edge-exact-configured-import",
        .site = {.file = "configured/derived.td",
                 .caller = "configured.derived.td.__file__",
                 .needle = "include \"base.td\""},
        .relation = "IMPORTS",
        .target = {.file = "configured/base.td",
                   .suffix = "configured.base.td.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "class BaseInstr {"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "tablegen-configured-literal-include",
        .why = "The launcher fixes the working directory and TableGen include root. The include "
               "names base.td rather than another file defining BaseInstr. No audited TableGen "
               "include domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {.case_id = "probe_tablegen_include_edge-wrong-target",
                                    .site = {.file = "configured/derived.td",
                                             .caller = "configured.derived.td.__file__",
                                             .needle = "include \"base.td\""},
                                    .relation = "IMPORTS",
                                    .target = {.file = "configured/decoy.td",
                                               .suffix = "configured.decoy.td.__file__",
                                               .label = "File",
                                               .declaration_line = 1,
                                               .declaration_text = "class BaseInstr {"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]),
                                   "probe_tablegen_include_edge", &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 22 — TLA+ (.tla)
 *
 * TLA+ label histogram: Function:1, Module:1.
 * Spec: tlaplus_func_types = {"operator_definition","function_definition"},
 *       tlaplus_import_types = {"extends","instance"}.
 * Extension: .tla → CBM_LANG_TLAPLUS.
 * ══════════════════════════════════════════════════════════════════ */

/* TLA+: operator definition → Function node. */
TEST(probe_tlaplus_operator) {
    GpdMetrics m = gpd_metrics("counter.tla", "---- MODULE counter ----\n"
                                              "VARIABLE count\n"
                                              "\n"
                                              "Init == count = 0\n"
                                              "\n"
                                              "Increment == count' = count + 1\n"
                                              "====\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: operator definitions (Init, Increment) must produce Function nodes. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_tlaplus_extends_edge) {
    static const GpdFile files[] = {{"Naturals.tla", "---- MODULE Naturals ----\n"
                                                     "Max(a, b) == IF a > b THEN a ELSE b\n"
                                                     "====\n"},
                                    {"Counter.tla", "---- MODULE Counter ----\n"
                                                    "EXTENDS Naturals\n"
                                                    "\n"
                                                    "VARIABLE count\n"
                                                    "\n"
                                                    "Init == count = 0\n"
                                                    "Next == count' = Max(count + 1, count)\n"
                                                    "====\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/QualityHelpers.tla",
         "---- MODULE QualityHelpers ----\nIdentity(x) == x\n====\n"},
        {"configured/QualityRoot.tla",
         "---- MODULE QualityRoot ----\nEXTENDS QualityHelpers\nResult == Identity(TRUE)\n====\n"},
        {"configured/OtherHelpers.tla",
         "---- MODULE OtherHelpers ----\nIdentity(x) == FALSE\n====\n"},
        {"configured/check.sh",
         "#!/bin/sh\nset -eu\ncd \"$(dirname \"$0\")\"\nexec java -cp \"${1:?provide the "
         "tla2tools.jar path}\" tla2sany.SANY QualityRoot.tla\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_tlaplus_extends_edge-exact-configured-import",
        .site = {.file = "configured/QualityRoot.tla",
                 .caller = "configured.QualityRoot.tla.__file__",
                 .needle = "EXTENDS QualityHelpers"},
        .relation = "IMPORTS",
        .target = {.file = "configured/QualityHelpers.tla",
                   .suffix = "configured.QualityHelpers.tla.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "---- MODULE QualityHelpers ----"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "tlaplus-configured-unique-module-import",
        .why = "The explicit root module and launcher directory contain one uniquely named "
               "companion module. This avoids the original Naturals standard-module collision and "
               "undefined arithmetic operators. OtherHelpers is not QualityHelpers. No audited "
               "TLA+ EXTENDS domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {
        .case_id = "probe_tlaplus_extends_edge-wrong-target",
        .site = {.file = "configured/QualityRoot.tla",
                 .caller = "configured.QualityRoot.tla.__file__",
                 .needle = "EXTENDS QualityHelpers"},
        .relation = "IMPORTS",
        .target = {.file = "configured/OtherHelpers.tla",
                   .suffix = "configured.OtherHelpers.tla.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "---- MODULE OtherHelpers ----"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]),
                                   "probe_tlaplus_extends_edge", &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* TLA+: no OOP — no type-like nodes expected. */
TEST(probe_tlaplus_no_type_nodes) {
    GpdMetrics m = gpd_metrics("simple.tla", "---- MODULE simple ----\n"
                                             "VARIABLE x\n"
                                             "Init == x = 0\n"
                                             "====\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: TLA+ produces 0 type-like nodes. */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 23 — Verilog (.v)
 *
 * Verilog label histogram: Class:1, Module:1.
 * Spec: verilog_func_types = {"function_declaration","task_declaration",...},
 *       verilog_class_types = {"module_declaration","class_declaration",...},
 *       verilog_import_types = {"extends","import","package_import_declaration"}.
 * Extension: .v → CBM_LANG_VERILOG.
 * Note: .sv also maps to CBM_LANG_VERILOG (not CBM_LANG_SYSTEMVERILOG).
 * ══════════════════════════════════════════════════════════════════ */

/* Verilog: module declaration → type-like node. */
TEST(probe_verilog_module_node) {
    GpdMetrics m = gpd_metrics("counter.v", "module counter(\n"
                                            "    input clk,\n"
                                            "    input rst,\n"
                                            "    output reg [7:0] count\n"
                                            ");\n"
                                            "\n"
                                            "always @(posedge clk or posedge rst) begin\n"
                                            "    if (rst)\n"
                                            "        count <= 0;\n"
                                            "    else\n"
                                            "        count <= count + 1;\n"
                                            "end\n"
                                            "\n"
                                            "endmodule\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: module_declaration must produce a type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Verilog: function declaration → Function node. */
TEST(probe_verilog_function) {
    GpdMetrics m = gpd_metrics("math.v", "module math_funcs;\n"
                                         "\n"
                                         "function [7:0] double;\n"
                                         "    input [7:0] n;\n"
                                         "    begin\n"
                                         "        double = n << 1;\n"
                                         "    end\n"
                                         "endfunction\n"
                                         "\n"
                                         "endmodule\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: function declaration must produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Verilog: SystemVerilog-style file (.sv) — lands on CBM_LANG_VERILOG via EXT_TABLE.
 * No separate CBM_LANG_SYSTEMVERILOG routing is available through file extension. */
TEST(probe_verilog_sv_extension) {
    GpdMetrics m = gpd_metrics("adder.sv", "module adder(\n"
                                           "    input logic [7:0] a,\n"
                                           "    input logic [7:0] b,\n"
                                           "    output logic [7:0] sum\n"
                                           ");\n"
                                           "    assign sum = a + b;\n"
                                           "endmodule\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: .sv → CBM_LANG_VERILOG; module_declaration must produce type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 24 — VHDL (.vhd)
 *
 * VHDL label histogram: Class:1, Module:1.
 * Spec: vhdl_func_types = {"subprogram_declaration","subprogram_definition"},
 *       vhdl_class_types = {"entity_declaration","architecture_definition",
 *                           "component_declaration","interface_declaration",
 *                           "package_declaration","protected_type_declaration",
 *                           "record_type_definition","type_declaration"},
 *       vhdl_import_types = {"library_clause","use_clause"}.
 * Extension: .vhd / .vhdl → CBM_LANG_VHDL.
 * ══════════════════════════════════════════════════════════════════ */

/* VHDL: entity declaration → type-like node. */
TEST(probe_vhdl_entity_node) {
    GpdMetrics m = gpd_metrics("counter.vhd", "entity counter is\n"
                                              "    port (\n"
                                              "        clk : in bit;\n"
                                              "        rst : in bit;\n"
                                              "        count : out integer\n"
                                              "    );\n"
                                              "end counter;\n"
                                              "\n"
                                              "architecture rtl of counter is\n"
                                              "begin\n"
                                              "end architecture;\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: entity_declaration must produce a type-like node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* VHDL: subprogram (function/procedure) definition → Function node. */
TEST(probe_vhdl_subprogram) {
    GpdMetrics m = gpd_metrics("funcs.vhd", "package math_pkg is\n"
                                            "    function double(n : integer) return integer;\n"
                                            "end package;\n"
                                            "\n"
                                            "package body math_pkg is\n"
                                            "    function double(n : integer) return integer is\n"
                                            "    begin\n"
                                            "        return n * 2;\n"
                                            "    end function;\n"
                                            "end package body;\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: subprogram_definition must produce a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_vhdl_use_imports_edge) {
    static const GpdFile files[] = {{"math_pkg.vhd",
                                     "package math_pkg is\n"
                                     "    function double(n : integer) return integer;\n"
                                     "end package;\n"
                                     "\n"
                                     "package body math_pkg is\n"
                                     "    function double(n : integer) return integer is\n"
                                     "    begin return n * 2; end function;\n"
                                     "end package body;\n"},
                                    {"top.vhd", "library work;\n"
                                                "use work.math_pkg.all;\n"
                                                "\n"
                                                "entity top is end;\n"
                                                "architecture rtl of top is\n"
                                                "begin\n"
                                                "end architecture;\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/math_pkg.vhd",
         "package math_pkg is\n    function double(n : integer) return integer;\nend "
         "package;\n\npackage body math_pkg is\n    function double(n : integer) return integer "
         "is\n    begin return n * 2; end function;\nend package body;\n"},
        {"configured/top.vhd", "library work;\nuse work.math_pkg.all;\n\nentity top is "
                               "end;\narchitecture rtl of top is\nbegin\nend architecture;\n"},
        {"configured/other/math_pkg.vhd",
         "package math_pkg is\n    function double(n : integer) return integer;\nend "
         "package;\n\npackage body math_pkg is\n    function double(n : integer) return integer "
         "is\n    begin return n * 2; end function;\nend package body;\n"},
        {"configured/analyze.sh",
         "#!/bin/sh\nset -eu\ncd \"$(dirname \"$0\")\"\nghdl -a --std=08 --work=otherlib "
         "other/math_pkg.vhd\nexec ghdl -a --std=08 --work=work math_pkg.vhd top.vhd\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_vhdl_use_imports_edge-exact-configured-import",
        .site = {.file = "configured/top.vhd",
                 .caller = "configured.top.vhd.__file__",
                 .needle = "use work.math_pkg.all;"},
        .relation = "IMPORTS",
        .target = {.file = "configured/math_pkg.vhd",
                   .suffix = "configured.math_pkg.vhd.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "package math_pkg is"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "vhdl-configured-library-package-import",
        .why = "The ordered GHDL analysis explicitly maps math_pkg.vhd to work and its namesake to "
               "otherlib. The use work.math_pkg.all refers to the package in work, not an "
               "arbitrary matching file. No audited VHDL library-membership import domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {.case_id = "probe_vhdl_use_imports_edge-wrong-target",
                                    .site = {.file = "configured/top.vhd",
                                             .caller = "configured.top.vhd.__file__",
                                             .needle = "use work.math_pkg.all;"},
                                    .relation = "IMPORTS",
                                    .target = {.file = "configured/other/math_pkg.vhd",
                                               .suffix = "configured.other.math_pkg.vhd.__file__",
                                               .label = "File",
                                               .declaration_line = 1,
                                               .declaration_text = "package math_pkg is"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]),
                                   "probe_vhdl_use_imports_edge", &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 25 — Wolfram (.wl)
 *
 * Wolfram label histogram: Function:2, Module:1.
 * Spec: wolfram_func_types = {"set_delayed_top","set_top","set_delayed","set"},
 *       wolfram_import_types = {"get_top"}.
 * Extension: .wl / .wls → CBM_LANG_WOLFRAM.
 * ══════════════════════════════════════════════════════════════════ */

/* Wolfram: SetDelayed function definition → Function node. */
TEST(probe_wolfram_function) {
    GpdMetrics m = gpd_metrics("math.wl", "Square[n_] := n * n\n"
                                          "\n"
                                          "Cube[n_] := n * Square[n]\n");
    ASSERT_TRUE(m.ok);
    /* REAL BUG (class 16 node-extraction incompleteness): two top-level
     * `f[x_] := ...` SetDelayed defs in one file yield only 1 Function node,
     * not 2.  The grammar_labels histogram only guarantees Function:1 for a
     * single-def fixture; a 2-def file under-extracts.  Root cause likely in
     * extract_defs.c wolfram walk (resolve_wolfram_func_name / top-level
     * statement iteration capturing only the first set_delayed_top).  RED. */
    ASSERT_TRUE(m.functions >= 2); /* REAL BUG — multi-def under-extraction */
    PASS();
}

/* Wolfram: Set (=) definition → Function node. */
TEST(probe_wolfram_set_definition) {
    GpdMetrics m = gpd_metrics("const.wl", "Pi = 3.14159265\n"
                                           "\n"
                                           "E = 2.71828182\n"
                                           "\n"
                                           "Double[n_] := n * 2\n");
    ASSERT_TRUE(m.ok);
    /* REAL BUG (class 16 node-extraction incompleteness): a file whose first
     * top-level statements are plain `Pi = 3.14` / `E = 2.71` (set_top with a
     * non-`apply` LHS → resolve_wolfram_func_name returns null) followed by a
     * real `Double[n_] := n*2` def yields 0 Function nodes — the trailing
     * apply-LHS SetDelayed def is NOT extracted.  A lone `f[x_] := ...`
     * extracts fine (grammar_labels wolfram=Function:1), so the leading
     * non-def set_top statements suppress later extraction.  Root cause:
     * extract_defs.c wolfram top-level walk.  RED. */
    ASSERT_TRUE(m.functions >= 1); /* REAL BUG — set_top prefix suppresses later def */
    PASS();
}

/* Exact source import: retained as a named resolver-quality obligation. */
TEST(probe_wolfram_get_import) {
    static const GpdFile files[] = {{"utils.wl", "Double[n_] := n * 2\n"
                                                 "Triple[n_] := n * 3\n"},
                                    {"main.wls", "<< \"utils.wl\"\n"
                                                 "\n"
                                                 "Quad[n_] := Double[Double[n]]\n"}};
    GpdMetrics m = gpd_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* Preserve the original sources above; this separate configured fixture
     * defines the exact import obligation without guessing ambient build state. */
    const GpdFile configured[] = {
        {"configured/utils.wl", "Double[n_] := n * 2\nTriple[n_] := n * 3\n"},
        {"configured/entry.wls", "Get[FileNameJoin[{DirectoryName[$InputFileName], "
                                 "\"utils.wl\"}]];\nQuad[n_] := Double[Double[n]]\n"},
        {"configured/decoy.wl", "Double[n_] := n * 4\n"},
        {"configured/run.sh", "#!/bin/sh\nset -eu\ncd \"$(dirname \"$0\")\"\nexec wolframscript "
                              "-code 'Get[FileNameJoin[{Directory[], \"entry.wls\"}]]'\n"}};
    const RQ_Source sources[] = {{.path = configured[0].name, .text = configured[0].content},
                                 {.path = configured[1].name, .text = configured[1].content},
                                 {.path = configured[2].name, .text = configured[2].content},
                                 {.path = configured[3].name, .text = configured[3].content}};
    const RQ_Expected gap = {
        .case_id = "probe_wolfram_get_import-exact-configured-import",
        .site = {.file = "configured/entry.wls",
                 .caller = "configured.entry.wls.__file__",
                 .needle = "Get[FileNameJoin[{DirectoryName[$InputFileName], \"utils.wl\"}]]"},
        .relation = "IMPORTS",
        .target = {.file = "configured/utils.wl",
                   .suffix = "configured.utils.wl.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "Double[n_] := n * 2"},
        .mode = RQ_KNOWN_GAP,
        .gap_id = "wolfram-input-file-relative-get",
        .why = "The launcher loads entry.wls through Get, which sets its input filename; the "
               "entrypoint then constructs an explicit adjacent path before Get. The target does "
               "not depend on ambient $Path; a Double in decoy.wl is unrelated. No audited Wolfram "
               "file-load domain exists.",
        .tried = "Original fixture bytes and index checks are preserved. This separate "
                 "source/configuration obligation, forbidden target, and required exact ESM call "
                 "run through the full production indexer. Fixture launchers are never executed."};
    const RQ_Forbidden forbidden = {
        .case_id = "probe_wolfram_get_import-wrong-target",
        .site = {.file = "configured/entry.wls",
                 .caller = "configured.entry.wls.__file__",
                 .needle = "Get[FileNameJoin[{DirectoryName[$InputFileName], \"utils.wl\"}]]"},
        .relation = "IMPORTS",
        .target = {.file = "configured/decoy.wl",
                   .suffix = "configured.decoy.wl.__file__",
                   .label = "File",
                   .declaration_line = 1,
                   .declaration_text = "Double[n_] := n * 4"}};
    RQ_Report report;
    ASSERT_TRUE(gpd_import_quality(configured, sizeof(configured) / sizeof(configured[0]), sources,
                                   sizeof(sources) / sizeof(sources[0]), "probe_wolfram_get_import",
                                   &gap, &forbidden, &report));
    ASSERT_EQ(report.genuine, 2); /* exact import + required supported control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* Wolfram: no OOP types — Wolfram is functional/symbolic. */
TEST(probe_wolfram_no_type_nodes) {
    GpdMetrics m = gpd_metrics("simple.wl", "Id[x_] := x\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Wolfram produces 0 type-like nodes. */
    ASSERT_TRUE(m.types == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE wiring
 * ══════════════════════════════════════════════════════════════════ */

SUITE(grammar_probe_d) {
    /* Agda (.agda) */
    RUN_TEST(probe_agda_function);
    RUN_TEST(probe_agda_data_type);
    RUN_TEST(probe_agda_imports_edge);

    /* Assembly (.s) */
    RUN_TEST(probe_assembly_label_function);
    RUN_TEST(probe_assembly_multiple_labels);
    RUN_TEST(probe_assembly_no_type_nodes);

    /* Bicep (.bicep) */
    RUN_TEST(probe_bicep_function);
    RUN_TEST(probe_bicep_resource_node);
    RUN_TEST(probe_bicep_module_import);

    /* CFML (.cfm) */
    RUN_TEST(probe_cfml_function_tag);
    RUN_TEST(probe_cfml_two_functions);
    RUN_TEST(probe_cfml_no_type_nodes);

    /* CFScript (.cfc) */
    RUN_TEST(probe_cfscript_functions);
    RUN_TEST(probe_cfscript_no_import_edges);

    /* COBOL (.cob) */
    RUN_TEST(probe_cobol_program_node);
    RUN_TEST(probe_cobol_no_crash);

    /* Elm (.elm) */
    RUN_TEST(probe_elm_function);
    RUN_TEST(probe_elm_type_alias);
    RUN_TEST(probe_elm_imports_edge);

    /* FunC (.fc) */
    RUN_TEST(probe_func_function);
    RUN_TEST(probe_func_imports_edge);
    RUN_TEST(probe_func_no_type_nodes);

    /* Janet (.janet) */
    RUN_TEST(probe_janet_no_crash);
    RUN_TEST(probe_janet_function_extraction_gap);

    /* Lean (.lean) */
    RUN_TEST(probe_lean_def_functions);
    RUN_TEST(probe_lean_theorem);
    RUN_TEST(probe_lean_structure_type);
    RUN_TEST(probe_lean_imports_edge);

    /* LLVM IR (.ll) */
    RUN_TEST(probe_llvmir_function);
    RUN_TEST(probe_llvmir_two_functions);
    RUN_TEST(probe_llvmir_no_type_nodes);

    /* Magma (.mag) */
    RUN_TEST(probe_magma_function);
    RUN_TEST(probe_magma_procedure);
    RUN_TEST(probe_magma_no_type_nodes);

    /* Move (.move) */
    RUN_TEST(probe_move_function);
    RUN_TEST(probe_move_imports_edge);
    RUN_TEST(probe_move_struct_not_extracted);

    /* NASM (.nasm) */
    RUN_TEST(probe_nasm_label_function);
    RUN_TEST(probe_nasm_struc_node);
    RUN_TEST(probe_nasm_include_edge);

    /* Objective-C (.m with ObjC markers) */
    RUN_TEST(probe_objc_class_node);
    RUN_TEST(probe_objc_method_nodes);
    RUN_TEST(probe_objc_inherits_edge);
    RUN_TEST(probe_objc_import_edge);

    /* Pony (.pony) */
    RUN_TEST(probe_pony_class_node);
    RUN_TEST(probe_pony_actor_node);
    RUN_TEST(probe_pony_method_nodes);
    RUN_TEST(probe_pony_trait_node);
    RUN_TEST(probe_pony_imports_edge);

    /* PureScript (.purs) */
    RUN_TEST(probe_purescript_function);
    RUN_TEST(probe_purescript_data_type);
    RUN_TEST(probe_purescript_imports_edge);

    /* Pine Script (.pine) */
    RUN_TEST(probe_pine_function);
    RUN_TEST(probe_pine_type_node);

    /* QML (.qml) */
    RUN_TEST(probe_qml_function);
    RUN_TEST(probe_qml_imports_edge);

    /* Smali (.smali) */
    RUN_TEST(probe_smali_class_node);
    RUN_TEST(probe_smali_method_node);
    RUN_TEST(probe_smali_super_import);

    /* TableGen (.td) */
    RUN_TEST(probe_tablegen_def_node);
    RUN_TEST(probe_tablegen_class_node);
    RUN_TEST(probe_tablegen_include_edge);

    /* TLA+ (.tla) */
    RUN_TEST(probe_tlaplus_operator);
    RUN_TEST(probe_tlaplus_extends_edge);
    RUN_TEST(probe_tlaplus_no_type_nodes);

    /* Verilog (.v — also covers .sv routed as CBM_LANG_VERILOG) */
    RUN_TEST(probe_verilog_module_node);
    RUN_TEST(probe_verilog_function);
    RUN_TEST(probe_verilog_sv_extension);

    /* VHDL (.vhd) */
    RUN_TEST(probe_vhdl_entity_node);
    RUN_TEST(probe_vhdl_subprogram);
    RUN_TEST(probe_vhdl_use_imports_edge);

    /* Wolfram (.wl) */
    RUN_TEST(probe_wolfram_function);
    RUN_TEST(probe_wolfram_set_definition);
    RUN_TEST(probe_wolfram_get_import);
    RUN_TEST(probe_wolfram_no_type_nodes);
}
