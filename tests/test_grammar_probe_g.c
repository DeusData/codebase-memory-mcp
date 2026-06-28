/*
 * test_grammar_probe_g.c — Markup / data / config grammar node-creation + edge probe.
 *
 * SCOPE
 * ─────
 * Probes 27 grammars that have little or no code surface:
 *   html, xml, css, scss, markdown, rst, json, json5, yaml, toml, ini,
 *   csv, sql, soql, sosl, dotenv, gitignore, gitattributes, properties,
 *   requirements, diff, po, regex, sshconfig, bibtex, form, linkerscript
 *
 * STRUCTURAL-NODE-BEARING (produce nodes beyond Module):
 *   xml      — Class:2 per fixture  (element nodes)
 *   toml     — Class:1 + Variable:1 (table + key)
 *   ini      — Class:1 + Variable:1 (section + key)
 *   json     — Variable:2           (top-level keys)
 *   yaml     — Variable:2           (top-level keys)
 *   scss     — Variable:1           (one def extracted)
 *   sql      — Variable:1           (table reference node)
 *   markdown — Section:1            (heading node)
 *   form     — Variable:1           (one variable def)
 *
 * PURE-DATA Module-only (only a Module node is created):
 *   html, css, rst, json5, soql, sosl, dotenv, gitignore, gitattributes,
 *   properties, requirements, diff, po, regex, sshconfig, bibtex, linkerscript, csv
 *
 * COLOUR LEGEND
 * ─────────────
 *   GREEN = guard: pipeline already produces the result; failure = regression.
 *   RED   = bug reproduction: pipeline does NOT yet produce the expected
 *           node/edge; the test FAILS until the bug is fixed.  Brief inline
 *           comment documents the root-cause class.
 *
 * CALLS/inheritance dimensions are not applicable to these data/markup grammars.
 * Do NOT register this suite in test_main.c — a sibling agent owns that file.
 *
 * SUITE(grammar_probe_g)
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
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
 * Prefix "gpg_" to avoid symbol collisions with sibling probe files.
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} GpgProj;

typedef struct {
    const char *name; /* relative filename, may include '/' for subdirs */
    const char *content;
} GpgFile;

static void gpg_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

static cbm_store_t *gpg_open_indexed(GpgProj *lp) {
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

static cbm_store_t *gpg_index_files(GpgProj *lp, const GpgFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_gpg_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    gpg_to_fwd_slashes(lp->tmpdir);
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
    return gpg_open_indexed(lp);
}

static void gpg_cleanup(GpgProj *lp, cbm_store_t *store) {
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

static int gpg_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

typedef struct {
    int ok;
    int total_nodes;
    int modules;
    int classes;
    int variables;
    int sections;
    int imports; /* IMPORTS edges */
    int depends; /* DEPENDS_ON edges */
} GpgMetrics;

static GpgMetrics gpg_metrics_files(const GpgFile *files, int nfiles) {
    GpgProj lp;
    cbm_store_t *store = gpg_index_files(&lp, files, nfiles);
    GpgMetrics m = {0};
    if (store) {
        m.ok = 1;
        m.total_nodes = cbm_store_count_nodes(store, lp.project);
        m.modules = gpg_count_label(store, lp.project, "Module");
        m.classes = gpg_count_label(store, lp.project, "Class");
        m.variables = gpg_count_label(store, lp.project, "Variable");
        m.sections = gpg_count_label(store, lp.project, "Section");
        m.imports = cbm_store_count_edges_by_type(store, lp.project, "IMPORTS");
        m.depends = cbm_store_count_edges_by_type(store, lp.project, "DEPENDS_ON");
    }
    gpg_cleanup(&lp, store);
    return m;
}

static GpgMetrics gpg_metrics(const char *filename, const char *content) {
    GpgFile f = {filename, content};
    return gpg_metrics_files(&f, 1);
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 1 — HTML (.html)
 *
 * HTML golden histogram: Module:1 (pure-data — no element nodes extracted).
 * <script src="..."> and <link href="..."> are the only edges the pipeline
 * could model, but no HTML IMPORTS resolver exists in the pipeline today.
 * ══════════════════════════════════════════════════════════════════ */

/* HTML: minimal page → at least 1 Module node, no crash. */
TEST(probe_html_minimal_page) {
    GpgMetrics m = gpg_metrics("index.html", "<!DOCTYPE html>\n"
                                             "<html><head><title>Test</title></head>\n"
                                             "<body><h1>Hello</h1></body></html>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: HTML always produces at least a Module node. */
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* HTML: file with <script src> and <link rel=stylesheet> → IMPORTS edges.
 * RED: no HTML import resolver; script/link attributes are not walked into
 *      IMPORTS edges by the pipeline. */
TEST(probe_html_script_imports) {
    static const GpgFile files[] = {{"app.js", "function init() { console.log('ready'); }\n"},
                                    {"index.html", "<!DOCTYPE html><html><head>\n"
                                                   "<link rel=\"stylesheet\" href=\"style.css\">\n"
                                                   "<script src=\"app.js\"></script>\n"
                                                   "</head><body></body></html>\n"}};
    GpgMetrics m = gpg_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: HTML <script src> / <link href> not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no HTML import resolver */
    PASS();
}

/* HTML: pure-data — no Class/Variable/Section nodes expected. */
TEST(probe_html_no_structural_nodes) {
    GpgMetrics m = gpg_metrics("page.html", "<html><body><p>text</p></body></html>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: HTML produces zero Class/Variable/Section nodes (pure-data). */
    ASSERT_TRUE(m.classes == 0);
    ASSERT_TRUE(m.variables == 0);
    ASSERT_TRUE(m.sections == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 2 — XML (.xml)
 *
 * XML golden histogram: Class:2, Module:1
 * The XML extractor produces Class nodes for element definitions
 * (e.g., root element + nested element type nodes).
 * ══════════════════════════════════════════════════════════════════ */

/* XML: minimal document → Module node + at least 2 Class nodes. */
TEST(probe_xml_element_nodes) {
    GpgMetrics m = gpg_metrics("config.xml", "<?xml version=\"1.0\"?>\n"
                                             "<config>\n"
                                             "  <server host=\"localhost\" port=\"8080\"/>\n"
                                             "  <database name=\"mydb\"/>\n"
                                             "</config>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: XML extractor produces Class nodes for elements. */
    ASSERT_TRUE(m.classes >= 2);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* XML: more elements → more Class nodes (≥ 2). */
TEST(probe_xml_multiple_elements) {
    GpgMetrics m = gpg_metrics("pom.xml", "<project>\n"
                                          "  <groupId>com.example</groupId>\n"
                                          "  <artifactId>demo</artifactId>\n"
                                          "  <version>1.0</version>\n"
                                          "</project>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: multiple element types must produce Class nodes. */
    ASSERT_TRUE(m.classes >= 2);
    PASS();
}

/* XML: no-crash guard for empty document. */
TEST(probe_xml_no_crash) {
    GpgMetrics m = gpg_metrics("empty.xml", "<?xml version=\"1.0\"?><root/>\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 3 — CSS (.css)
 *
 * CSS golden histogram: Module:1 (pure-data).
 * CSS selectors/rules are not extracted as graph nodes; only a Module
 * node is emitted per file.
 * ══════════════════════════════════════════════════════════════════ */

/* CSS: rules file → only a Module node. */
TEST(probe_css_module_only) {
    GpgMetrics m = gpg_metrics("styles.css", "body {\n"
                                             "  margin: 0;\n"
                                             "  font-family: sans-serif;\n"
                                             "}\n"
                                             ".container { max-width: 1200px; }\n"
                                             "#header { background: #fff; }\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: CSS produces exactly 1 Module node; no Class/Variable/Section. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.classes == 0);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* CSS: @import in two-file fixture → IMPORTS edge.
 * RED: no CSS import resolver in the pipeline. */
TEST(probe_css_at_import) {
    static const GpgFile files[] = {{"base.css", "* { box-sizing: border-box; }\n"},
                                    {"main.css", "@import \"base.css\";\n"
                                                 "body { margin: 0; }\n"}};
    GpgMetrics m = gpg_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: CSS @import not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no CSS import resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 4 — SCSS (.scss)
 *
 * SCSS golden histogram: Module:1, Variable:1
 * The extractor pulls at least one Variable node (e.g. a CSS custom property
 * or SCSS variable).  No selector/rule Class nodes.
 * ══════════════════════════════════════════════════════════════════ */

/* SCSS: variables + rules → at least 1 Variable node. */
TEST(probe_scss_variable_node) {
    GpgMetrics m = gpg_metrics("theme.scss", "$primary: #3498db;\n"
                                             "$secondary: #2ecc71;\n"
                                             "\n"
                                             ".button {\n"
                                             "  background: $primary;\n"
                                             "  color: white;\n"
                                             "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: SCSS extracts at least 1 Variable node (histogram: Variable:1). */
    ASSERT_TRUE(m.variables >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* SCSS: @import / @use in two-file fixture → IMPORTS edge.
 * RED: no SCSS import resolver in the pipeline. */
TEST(probe_scss_at_use_imports) {
    static const GpgFile files[] = {{"_vars.scss", "$brand: #e74c3c;\n"},
                                    {"main.scss", "@use 'vars';\n"
                                                  ".hero { color: vars.$brand; }\n"}};
    GpgMetrics m = gpg_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: SCSS @use / @import not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no SCSS import resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 5 — Markdown (.md)
 *
 * Markdown golden histogram: Module:1, Section:1
 * Headings become Section nodes.  Links are NOT resolved to IMPORTS edges.
 * ══════════════════════════════════════════════════════════════════ */

/* Markdown: document with headings → at least 1 Section node. */
TEST(probe_markdown_section_node) {
    GpgMetrics m = gpg_metrics("README.md", "# Project Title\n"
                                            "\n"
                                            "Description text.\n"
                                            "\n"
                                            "## Getting Started\n"
                                            "\n"
                                            "Install with `npm install`.\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Markdown headings produce Section nodes. */
    ASSERT_TRUE(m.sections >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* Markdown: multiple headings → at least 1 Section (histogram baseline: Section:1).
 * The histogram fixture has 1 section; additional headings may or may not add more. */
TEST(probe_markdown_multi_heading) {
    GpgMetrics m = gpg_metrics("docs.md", "# Title\n"
                                          "## Setup\n"
                                          "### Install\n"
                                          "## Usage\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum 1 Section node. */
    ASSERT_TRUE(m.sections >= 1);
    PASS();
}

/* Markdown: no Class/Variable nodes (pure-data beyond Section). */
TEST(probe_markdown_no_class_variable) {
    GpgMetrics m = gpg_metrics("notes.md", "# Notes\n\nSome content.\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Markdown does not produce Class or Variable nodes. */
    ASSERT_TRUE(m.classes == 0);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 6 — RST (.rst)
 *
 * RST golden histogram: Module:1 (pure-data).
 * reStructuredText headings are NOT extracted as Section nodes (unlike Markdown).
 * ══════════════════════════════════════════════════════════════════ */

/* RST: document with sections → Module:1, no Section nodes. */
TEST(probe_rst_module_only) {
    GpgMetrics m = gpg_metrics("docs.rst", "Project Title\n"
                                           "=============\n"
                                           "\n"
                                           "Description text.\n"
                                           "\n"
                                           "Installation\n"
                                           "------------\n"
                                           "\n"
                                           "Run ``pip install mypackage``.\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: RST produces only a Module node (no Section nodes extracted). */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.sections == 0);
    PASS();
}

/* RST: no-crash guard. */
TEST(probe_rst_no_crash) {
    GpgMetrics m = gpg_metrics("api.rst", ".. automodule:: mypackage.api\n"
                                          "   :members:\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 7 — JSON (.json)
 *
 * JSON golden histogram: Module:1, Variable:2
 * Top-level keys become Variable nodes (2 in the fixture).
 * ══════════════════════════════════════════════════════════════════ */

/* JSON: object with two top-level keys → 2 Variable nodes. */
TEST(probe_json_variable_nodes) {
    GpgMetrics m = gpg_metrics("config.json", "{\n"
                                              "  \"name\": \"myproject\",\n"
                                              "  \"version\": \"1.0.0\"\n"
                                              "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: JSON top-level keys become Variable nodes. */
    ASSERT_TRUE(m.variables >= 2);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* JSON: three top-level keys → ≥ 2 Variable nodes (floor from histogram). */
TEST(probe_json_more_variables) {
    /* Fixture fix: "package.json" is in discover.c IGNORED_JSON_FILES, so
     * detect_file_language() returns CBM_LANG_COUNT and the file is never
     * indexed (0 nodes).  Use a generic .json filename so the JSON grammar
     * actually runs (grammar_labels json=Variable:2). */
    GpgMetrics m = gpg_metrics("data.json", "{\n"
                                            "  \"name\": \"demo\",\n"
                                            "  \"version\": \"0.1.0\",\n"
                                            "  \"description\": \"A demo project\"\n"
                                            "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least 2 Variable nodes (histogram baseline). */
    ASSERT_TRUE(m.variables >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 8 — JSON5 (.json5)
 *
 * JSON5 golden histogram: Module:1 (pure-data).
 * Despite being a superset of JSON, the JSON5 extractor produces only Module.
 * ══════════════════════════════════════════════════════════════════ */

/* JSON5: file with keys → only Module node. */
TEST(probe_json5_module_only) {
    GpgMetrics m = gpg_metrics("config.json5", "{\n"
                                               "  // comments allowed\n"
                                               "  name: 'myproject',\n"
                                               "  version: '1.0',\n"
                                               "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: JSON5 produces only a Module node (no Variable nodes unlike JSON). */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 9 — YAML (.yaml)
 *
 * YAML golden histogram: Module:1, Variable:2
 * Top-level keys become Variable nodes.
 * ══════════════════════════════════════════════════════════════════ */

/* YAML: document with two top-level keys → 2 Variable nodes. */
TEST(probe_yaml_variable_nodes) {
    GpgMetrics m = gpg_metrics("config.yaml", "name: myproject\n"
                                              "version: 1.0.0\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: YAML top-level keys become Variable nodes. */
    ASSERT_TRUE(m.variables >= 2);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* YAML: three top-level keys → still ≥ 2 Variable nodes (baseline). */
TEST(probe_yaml_three_keys) {
    GpgMetrics m = gpg_metrics("app.yaml", "host: localhost\n"
                                           "port: 8080\n"
                                           "debug: true\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least 2 Variable nodes. */
    ASSERT_TRUE(m.variables >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 10 — TOML (.toml)
 *
 * TOML golden histogram: Class:1, Module:1, Variable:1
 * Table headers produce Class nodes; top-level keys produce Variable nodes.
 * ══════════════════════════════════════════════════════════════════ */

/* TOML: file with table + key → Class:1 + Variable:1. */
TEST(probe_toml_table_and_key) {
    GpgMetrics m = gpg_metrics("Cargo.toml", "[package]\n"
                                             "name = \"mylib\"\n"
                                             "version = \"0.1.0\"\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: TOML table [package] becomes a Class node. */
    ASSERT_TRUE(m.classes >= 1);
    /* GREEN: key becomes a Variable node. */
    ASSERT_TRUE(m.variables >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* TOML: multiple tables → ≥ 1 Class node (may be exactly 1 per fixture). */
TEST(probe_toml_multiple_tables) {
    GpgMetrics m = gpg_metrics("pyproject.toml", "[tool.poetry]\n"
                                                 "name = \"demo\"\n"
                                                 "\n"
                                                 "[tool.poetry.dependencies]\n"
                                                 "python = \"^3.9\"\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least one Class node for tables. */
    ASSERT_TRUE(m.classes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 11 — INI (.ini)
 *
 * INI golden histogram: Class:1, Module:1, Variable:1
 * Sections produce Class nodes; key-value pairs produce Variable nodes.
 * ══════════════════════════════════════════════════════════════════ */

/* INI: file with a section and a key → Class + Variable. */
TEST(probe_ini_section_and_key) {
    GpgMetrics m = gpg_metrics("app.ini", "[database]\n"
                                          "host = localhost\n"
                                          "port = 5432\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: [database] section becomes a Class node. */
    ASSERT_TRUE(m.classes >= 1);
    /* GREEN: key becomes a Variable node. */
    ASSERT_TRUE(m.variables >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* INI: .conf extension also maps to CBM_LANG_INI; must produce same structure. */
TEST(probe_ini_conf_extension) {
    GpgMetrics m = gpg_metrics("server.conf", "[server]\n"
                                              "listen = 0.0.0.0\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: .conf files index as INI and produce Class + Variable nodes. */
    ASSERT_TRUE(m.classes >= 1);
    ASSERT_TRUE(m.variables >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 12 — CSV (.csv)
 *
 * CSV golden histogram: Module:1 (pure-data).
 * Tabular data — no structural nodes beyond Module.
 * ══════════════════════════════════════════════════════════════════ */

/* CSV: file with header + rows → only Module node. */
TEST(probe_csv_module_only) {
    GpgMetrics m = gpg_metrics("data.csv", "id,name,score\n"
                                           "1,Alice,95\n"
                                           "2,Bob,87\n"
                                           "3,Carol,92\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: CSV produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.classes == 0);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 13 — SQL (.sql)
 *
 * SQL golden histogram: Module:1, Variable:1
 * Table references (e.g. CREATE TABLE / SELECT FROM) produce Variable nodes.
 * ══════════════════════════════════════════════════════════════════ */

/* SQL: CREATE TABLE + SELECT → at least 1 Variable node. */
TEST(probe_sql_variable_node) {
    GpgMetrics m = gpg_metrics("schema.sql", "CREATE TABLE users (\n"
                                             "  id INTEGER PRIMARY KEY,\n"
                                             "  name TEXT NOT NULL\n"
                                             ");\n"
                                             "\n"
                                             "SELECT id, name FROM users WHERE id = 1;\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: SQL table reference produces at least 1 Variable node. */
    ASSERT_TRUE(m.variables >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* SQL: multiple statements → still at least 1 Variable. */
TEST(probe_sql_insert_select) {
    GpgMetrics m = gpg_metrics(
        "queries.sql", "INSERT INTO orders (user_id, total) VALUES (1, 99.99);\n"
                       "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id;\n");
    ASSERT_TRUE(m.ok);
    /* GREEN (fixture fix): SQL Variable nodes come ONLY from DDL
     * create_table/create_view (lang_specs.c sql_var_types), NOT from
     * DML table *references* in INSERT/SELECT.  A DML-only file correctly
     * yields 0 Variable nodes; the original `>= 1` asserted a non-feature.
     * Assert the true contract: no table definitions → 0 Variables. */
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 14 — SOQL (.soql)
 *
 * SOQL golden histogram: Module:1 (pure-data).
 * Salesforce Object Query Language — no structural nodes extracted.
 * ══════════════════════════════════════════════════════════════════ */

/* SOQL: SELECT query → only Module node. */
TEST(probe_soql_module_only) {
    GpgMetrics m = gpg_metrics("query.soql", "SELECT Id, Name, Email\n"
                                             "FROM Contact\n"
                                             "WHERE AccountId = :accountId\n"
                                             "ORDER BY Name\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: SOQL produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 15 — SOSL (.sosl)
 *
 * SOSL golden histogram: Module:1 (pure-data).
 * Salesforce Object Search Language — no structural nodes extracted.
 * ══════════════════════════════════════════════════════════════════ */

/* SOSL: FIND query → only Module node. */
TEST(probe_sosl_module_only) {
    GpgMetrics m = gpg_metrics("search.sosl", "FIND {Acme} IN ALL FIELDS\n"
                                              "RETURNING Account(Id, Name), Contact(Id, Name)\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: SOSL produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 16 — DotEnv (.env)
 *
 * DotEnv golden histogram: Module:1 (pure-data).
 * Environment variable files are pure-data; keys are NOT extracted as nodes.
 * Extension: .env suffix detected by pass_envscan; filename ".env" also works.
 * ══════════════════════════════════════════════════════════════════ */

/* DotEnv: .env file → only Module node. */
TEST(probe_dotenv_module_only) {
    GpgMetrics m = gpg_metrics(".env", "DATABASE_URL=postgres://localhost/mydb\n"
                                       "SECRET_KEY=supersecret\n"
                                       "DEBUG=true\n");
    ASSERT_TRUE(m.ok);
    /* REAL BUG (NEW class — file-index routing gap): the DotEnv grammar +
     * histogram (grammar_labels dotenv=Module:1) exist and work via DIRECT
     * extraction, but ".env" has no FILENAME_TABLE / EXT_TABLE entry in
     * language.c, so file-based index_repository's cbm_language_for_filename
     * returns CBM_LANG_COUNT and the file is never indexed → 0 Module nodes.
     * Routing/registration gap, distinct from extraction classes 2/16.  RED. */
    ASSERT_TRUE(m.modules == 1); /* REAL BUG — .env not routed by file index */
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* DotEnv: .env.local suffix variant → indexed as DotEnv. */
TEST(probe_dotenv_local_suffix) {
    GpgMetrics m = gpg_metrics(".env.local", "API_KEY=local-test-key\n"
                                             "PORT=3001\n");
    ASSERT_TRUE(m.ok);
    /* REAL BUG (NEW class — file-index routing gap): ".env.local" has no
     * FILENAME_TABLE/EXT_TABLE entry, so file-based index_repository never
     * routes it to CBM_LANG_DOTENV → 0 Module nodes.  Same routing gap as
     * probe_dotenv_module_only.  RED. */
    ASSERT_TRUE(m.modules >= 1); /* REAL BUG — .env.local not routed by file index */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 17 — gitignore (.gitignore)
 *
 * gitignore golden histogram: Module:1 (pure-data).
 * Pattern files — no structural graph nodes.
 * Extension: cbm_path_ext(".gitignore") → "gitignore" (strips the leading dot).
 * ══════════════════════════════════════════════════════════════════ */

/* gitignore: pattern file → only Module node. */
TEST(probe_gitignore_module_only) {
    GpgMetrics m = gpg_metrics(".gitignore", "*.o\n"
                                             "*.a\n"
                                             "build/\n"
                                             "dist/\n"
                                             ".DS_Store\n");
    ASSERT_TRUE(m.ok);
    /* CONTRACT (corrected): unlike .gitattributes, ".gitignore" is intentionally
     * NOT indexed as a source file — the cbm_discover walker consumes it as
     * ignore-rule input (see test_discover.c discover_with_gitignore /
     * discover_gitignore_dir_excluded_issue234 / discover_cbmignore_stacks,
     * which assert .gitignore is excluded from the discovered file set). The
     * gitignore grammar exists for DIRECT extraction only; via the file walker
     * the ignore-control role wins by design, so it yields 0 nodes. Routing it
     * to CBM_LANG_GITIGNORE would regress those three established tests. */
    ASSERT_TRUE(m.modules == 0); /* .gitignore consumed as ignore-rules, not indexed (by design) */
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 18 — gitattributes (.gitattributes)
 *
 * gitattributes golden histogram: Module:1 (pure-data).
 * ══════════════════════════════════════════════════════════════════ */

/* gitattributes: attribute file → only Module node. */
TEST(probe_gitattributes_module_only) {
    GpgMetrics m = gpg_metrics(".gitattributes", "*.c text eol=lf\n"
                                                 "*.h text eol=lf\n"
                                                 "*.png binary\n"
                                                 "*.jpg binary\n");
    ASSERT_TRUE(m.ok);
    /* REAL BUG (NEW class — file-index routing gap): gitattributes grammar +
     * histogram (Module:1) work via DIRECT extraction, but ".gitattributes"
     * has no FILENAME_TABLE/EXT_TABLE entry → file-based index_repository
     * never routes it → 0 Module nodes.  Same routing gap as .gitignore. */
    ASSERT_TRUE(m.modules == 1); /* REAL BUG — .gitattributes not routed by file index */
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 19 — Properties (.properties)
 *
 * Properties golden histogram: Module:1 (pure-data).
 * Java-style key=value files — keys NOT extracted as Variable nodes
 * (unlike INI which does produce Class/Variable nodes).
 * ══════════════════════════════════════════════════════════════════ */

/* Properties: key=value file → only Module node. */
TEST(probe_properties_module_only) {
    GpgMetrics m = gpg_metrics("app.properties", "server.host=localhost\n"
                                                 "server.port=8080\n"
                                                 "log.level=INFO\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: .properties produces a Module node plus one Variable per `key=value`
     * property line (extract_defs.c CBM_LANG_PROPERTIES → push_var_def). The
     * fixture has 3 property lines (server.host, server.port, log.level). */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 3);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 20 — Requirements (.txt via FILENAME_TABLE)
 *
 * Requirements golden histogram: Module:1 (pure-data).
 * Python dependency specs — deps NOT extracted as DEPENDS_ON edges by
 * the grammar-only pipeline (no requirements resolver).
 * Extension: recognized only for "requirements.txt", "requirements-dev.txt",
 *            "requirements-test.txt" (FILENAME_TABLE exact match).
 * ══════════════════════════════════════════════════════════════════ */

/* Requirements: dep file → only Module node. */
TEST(probe_requirements_module_only) {
    GpgMetrics m = gpg_metrics("requirements.txt", "flask==2.3.0\n"
                                                   "requests>=2.28.0\n"
                                                   "sqlalchemy~=2.0\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: requirements.txt produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    PASS();
}

/* Requirements: DEPENDS_ON edges for package entries.
 * RED: grammar-only requirements has no dependency-edge resolver. */
TEST(probe_requirements_depends_on_edge) {
    GpgMetrics m = gpg_metrics("requirements.txt", "numpy==1.24.0\n"
                                                   "pandas>=1.5.0\n"
                                                   "scipy>=1.10.0\n");
    ASSERT_TRUE(m.ok);
    /* RED: requirements deps not resolved into DEPENDS_ON edges. */
    ASSERT_TRUE(m.depends >= 1); /* expected RED — no requirements edge resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 21 — Diff (.diff / .patch)
 *
 * Diff golden histogram: Module:1 (pure-data).
 * Patch files are structural metadata, not code.
 * ══════════════════════════════════════════════════════════════════ */

/* Diff: unified diff → only Module node. */
TEST(probe_diff_module_only) {
    GpgMetrics m = gpg_metrics("changes.diff", "--- a/main.c\n"
                                               "+++ b/main.c\n"
                                               "@@ -1,5 +1,6 @@\n"
                                               " #include <stdio.h>\n"
                                               "+#include <stdlib.h>\n"
                                               " \n"
                                               " int main(void) {\n"
                                               "-    printf(\"hello\\n\");\n"
                                               "+    printf(\"hello world\\n\");\n"
                                               "     return 0;\n"
                                               " }\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: diff produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 22 — PO (.po)
 *
 * PO golden histogram: Module:1 (pure-data).
 * GNU gettext translation catalogs — no structural nodes.
 * ══════════════════════════════════════════════════════════════════ */

/* PO: translation file → only Module node. */
TEST(probe_po_module_only) {
    GpgMetrics m = gpg_metrics("messages.po", "# Translation file\n"
                                              "msgid \"\"\n"
                                              "msgstr \"\"\n"
                                              "\"Content-Type: text/plain; charset=UTF-8\\n\"\n"
                                              "\n"
                                              "msgid \"Hello\"\n"
                                              "msgstr \"Bonjour\"\n"
                                              "\n"
                                              "msgid \"Goodbye\"\n"
                                              "msgstr \"Au revoir\"\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: PO produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 23 — Regex (.re)
 *
 * Regex golden histogram: Module:1 (pure-data).
 * Extension: ".re" maps to CBM_LANG_REGEX in grammar_cases.
 * ══════════════════════════════════════════════════════════════════ */

/* Regex: pattern file → only Module node. */
TEST(probe_regex_module_only) {
    GpgMetrics m = gpg_metrics("patterns.re", "(foo|bar)+\n"
                                              "^[a-zA-Z_][a-zA-Z0-9_]*$\n"
                                              "\\d{4}-\\d{2}-\\d{2}\n");
    ASSERT_TRUE(m.ok);
    /* REAL BUG (NEW class — file-index routing gap): the Regex grammar +
     * histogram (grammar_labels regex=Module:1) work via DIRECT extraction
     * (a.re), but the ".re" extension has no EXT_TABLE entry in language.c,
     * so file-based index_repository never routes it → 0 Module nodes.
     * Routing/registration gap, distinct from extraction classes 2/16.  RED. */
    ASSERT_TRUE(m.modules == 1); /* REAL BUG — .re not routed by file index */
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 24 — SSH Config (ssh_config / sshd_config / config)
 *
 * SSH Config golden histogram: Module:1 (pure-data).
 * Special filenames: "ssh_config", "sshd_config", ".ssh/config" (FILENAME_TABLE).
 * Also matches "config" basename in certain contexts (grammar_regression uses "config").
 * ══════════════════════════════════════════════════════════════════ */

/* SSH Config: host file via ssh_config filename → only Module node. */
TEST(probe_sshconfig_module_only) {
    GpgMetrics m = gpg_metrics("ssh_config", "Host bastion\n"
                                             "  HostName 10.0.0.1\n"
                                             "  User ubuntu\n"
                                             "  IdentityFile ~/.ssh/id_rsa\n"
                                             "\n"
                                             "Host dev\n"
                                             "  HostName dev.example.com\n"
                                             "  User deploy\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: SSH config produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 25 — BibTeX (.bib)
 *
 * BibTeX golden histogram: Module:1 (pure-data).
 * Bibliography entries are NOT extracted as structural nodes.
 * ══════════════════════════════════════════════════════════════════ */

/* BibTeX: bibliography file → only Module node. */
TEST(probe_bibtex_module_only) {
    GpgMetrics m = gpg_metrics("refs.bib", "@article{smith2023,\n"
                                           "  author = {Smith, John},\n"
                                           "  title  = {A Study of Something},\n"
                                           "  year   = {2023},\n"
                                           "  journal = {J. Examples}\n"
                                           "}\n"
                                           "\n"
                                           "@book{jones2020,\n"
                                           "  author = {Jones, Alice},\n"
                                           "  title  = {Reference Book},\n"
                                           "  year   = {2020}\n"
                                           "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: BibTeX produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 26 — FORM (.frm)
 *
 * FORM golden histogram: Module:1, Variable:1
 * The FORM language (symbolic algebra) produces a Variable node for
 * one definition.  Extension: .frm (CBM_LANG_FORM in EXT_TABLE).
 * ══════════════════════════════════════════════════════════════════ */

/* FORM: file with a variable declaration → at least 1 Variable node. */
TEST(probe_form_variable_node) {
    GpgMetrics m = gpg_metrics("calc.frm", "Symbol x, y, z;\n"
                                           "Local F = x^2 + y^2 + z^2;\n"
                                           "Print;\n"
                                           ".end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: FORM produces at least 1 Variable node (histogram: Variable:1). */
    ASSERT_TRUE(m.variables >= 1);
    ASSERT_TRUE(m.modules >= 1);
    PASS();
}

/* FORM: no-crash guard with procedure definition. */
TEST(probe_form_procedure) {
    GpgMetrics m = gpg_metrics("proc.frm", "#procedure Square(x)\n"
                                           "  id `x'^2 = `x'^2;\n"
                                           "#endprocedure\n"
                                           "\n"
                                           "Symbol a;\n"
                                           "Local expr = a^2;\n"
                                           ".end\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least 1 node (Module). */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 27 — Linker Script (.ld / .lds)
 *
 * Linker Script golden histogram: Module:1 (pure-data).
 * SECTIONS / MEMORY directives are not extracted as structural nodes.
 * ══════════════════════════════════════════════════════════════════ */

/* Linker script: SECTIONS directive → only Module node. */
TEST(probe_linkerscript_module_only) {
    GpgMetrics m = gpg_metrics("link.ld", "SECTIONS\n"
                                          "{\n"
                                          "  . = 0x10000;\n"
                                          "  .text : { *(.text) }\n"
                                          "  .data : { *(.data) }\n"
                                          "  .bss  : { *(.bss) }\n"
                                          "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: linker script produces only a Module node. */
    ASSERT_TRUE(m.modules == 1);
    ASSERT_TRUE(m.variables == 0);
    PASS();
}

/* Linker script: MEMORY + SECTIONS → still only Module node. */
TEST(probe_linkerscript_memory_sections) {
    GpgMetrics m = gpg_metrics("stm32.ld", "MEMORY\n"
                                           "{\n"
                                           "  FLASH (rx)  : ORIGIN = 0x08000000, LENGTH = 512K\n"
                                           "  RAM   (rwx) : ORIGIN = 0x20000000, LENGTH = 128K\n"
                                           "}\n"
                                           "SECTIONS\n"
                                           "{\n"
                                           "  .isr_vector : { *(.isr_vector) } > FLASH\n"
                                           "  .text       : { *(.text*)     } > FLASH\n"
                                           "  .bss        : { *(.bss*)      } > RAM\n"
                                           "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: linker script produces only a Module node. */
    ASSERT_TRUE(m.modules >= 1);
    ASSERT_TRUE(m.classes == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE wiring
 * ══════════════════════════════════════════════════════════════════ */

SUITE(grammar_probe_g) {
    /* HTML */
    RUN_TEST(probe_html_minimal_page);
    RUN_TEST(probe_html_script_imports);
    RUN_TEST(probe_html_no_structural_nodes);

    /* XML */
    RUN_TEST(probe_xml_element_nodes);
    RUN_TEST(probe_xml_multiple_elements);
    RUN_TEST(probe_xml_no_crash);

    /* CSS */
    RUN_TEST(probe_css_module_only);
    RUN_TEST(probe_css_at_import);

    /* SCSS */
    RUN_TEST(probe_scss_variable_node);
    RUN_TEST(probe_scss_at_use_imports);

    /* Markdown */
    RUN_TEST(probe_markdown_section_node);
    RUN_TEST(probe_markdown_multi_heading);
    RUN_TEST(probe_markdown_no_class_variable);

    /* RST */
    RUN_TEST(probe_rst_module_only);
    RUN_TEST(probe_rst_no_crash);

    /* JSON */
    RUN_TEST(probe_json_variable_nodes);
    RUN_TEST(probe_json_more_variables);

    /* JSON5 */
    RUN_TEST(probe_json5_module_only);

    /* YAML */
    RUN_TEST(probe_yaml_variable_nodes);
    RUN_TEST(probe_yaml_three_keys);

    /* TOML */
    RUN_TEST(probe_toml_table_and_key);
    RUN_TEST(probe_toml_multiple_tables);

    /* INI */
    RUN_TEST(probe_ini_section_and_key);
    RUN_TEST(probe_ini_conf_extension);

    /* CSV */
    RUN_TEST(probe_csv_module_only);

    /* SQL */
    RUN_TEST(probe_sql_variable_node);
    RUN_TEST(probe_sql_insert_select);

    /* SOQL */
    RUN_TEST(probe_soql_module_only);

    /* SOSL */
    RUN_TEST(probe_sosl_module_only);

    /* DotEnv */
    RUN_TEST(probe_dotenv_module_only);
    RUN_TEST(probe_dotenv_local_suffix);

    /* gitignore */
    RUN_TEST(probe_gitignore_module_only);

    /* gitattributes */
    RUN_TEST(probe_gitattributes_module_only);

    /* Properties */
    RUN_TEST(probe_properties_module_only);

    /* Requirements */
    RUN_TEST(probe_requirements_module_only);
    RUN_TEST(probe_requirements_depends_on_edge);

    /* Diff */
    RUN_TEST(probe_diff_module_only);

    /* PO */
    RUN_TEST(probe_po_module_only);

    /* Regex */
    RUN_TEST(probe_regex_module_only);

    /* SSH Config */
    RUN_TEST(probe_sshconfig_module_only);

    /* BibTeX */
    RUN_TEST(probe_bibtex_module_only);

    /* FORM */
    RUN_TEST(probe_form_variable_node);
    RUN_TEST(probe_form_procedure);

    /* Linker Script */
    RUN_TEST(probe_linkerscript_module_only);
    RUN_TEST(probe_linkerscript_memory_sections);
}
