/*
 * test_doc_links_adoc.c — AsciiDoc documents -> Sections and MENTIONS edges
 * (doc_adoc.c, doc_links_adoc.c).
 *
 * Headings and delimited blocks, the tokens (include with the page's
 * attributes, attribute references, monospace spans), the Antora scope blob,
 * and the resolver: resource IDs and collector scans, tag regions read from
 * the included file, lines, javadoc attributes to types and members, the
 * monospace spans through the Markdown rules, incremental == full.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"

#include "cbm.h"
#include "doclink.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *const ADOC_PAGE =
    "= Writing Tests\n"                                                   /* 1 */
    ":testDir: example$java\n"                                            /* 2 */
    "\n"                                                                  /* 3 */
    "Intro with {Assertions} and {nbsp}.\n"                               /* 4 */
    "\n"                                                                  /* 5 */
    "== First Test\n"                                                     /* 6 */
    "\n"                                                                  /* 7 */
    "[source,java]\n"                                                     /* 8 */
    "----\n"                                                              /* 9 */
    "include::{testDir}/example/FirstTests.java[tags=user_guide]\n"       /* 10 */
    "== Not A Heading\n"                                                  /* 11 */
    "----\n"                                                              /* 12 */
    "\n"                                                                  /* 13 */
    "////\n"                                                              /* 14 */
    "include::example$java/Hidden.java[]\n"                               /* 15 */
    "////\n"                                                              /* 16 */
    "// include::example$java/Commented.java[]\n"                         /* 17 */
    "\n"                                                                  /* 18 */
    "=== Details\n"                                                       /* 19 */
    "\n"                                                                  /* 20 */
    "See `src/main/java/org/x/Assertions.java` and `org.x.Assertions`.\n" /* 21 */
    "include::../../../../src/main/java/org/x/Calc.java[lines=4..6]\n"    /* 22 */
    "include::{testDir}/example/WithImports.java[tag=all]\n";             /* 23 */

static const CBMDefinition *adoc_def(const CBMFileResult *r, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->label && strcmp(d->label, "Section") == 0 && d->name && strcmp(d->name, name) == 0) {
            return d;
        }
    }
    return NULL;
}

static bool adoc_raw_has(const CBMFileResult *r, const char *needle) {
    for (int i = 0; i < r->doc_links.count; i++) {
        if (strstr(r->doc_links.items[i].raw, needle)) {
            return true;
        }
    }
    return false;
}

TEST(adoc_scan_structure) {
    CBMFileResult *r =
        dm_extract(ADOC_PAGE, CBM_LANG_ASCIIDOC, "docs/modules/ROOT/pages/writing.adoc");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->doc_links.failed);
    const CBMDefinition *top = adoc_def(r, "Writing Tests");
    const CBMDefinition *first = adoc_def(r, "First Test");
    const CBMDefinition *details = adoc_def(r, "Details");
    ASSERT_NOT_NULL(top);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(details);
    ASSERT_NULL(adoc_def(r, "Not A Heading")); /* inside a listing block */
    ASSERT_EQ(first->start_line, 6);
    ASSERT_EQ(first->end_line, 17);
    ASSERT_STR_EQ(details->qualified_name, "p.docs.modules.ROOT.pages.writing.Details");
    /* an include with the page's attribute substituted, its tags */
    const CBMDocLink *inc =
        dm_find_token(r, "include::{testDir}/example/FirstTests.java[tags=user_guide]\t"
                         "example$java/example/FirstTests.java\tuser_guide\t");
    ASSERT_NOT_NULL(inc);
    ASSERT_EQ(inc->syntax, CBM_DOCLINK_ADOC_INCLUDE);
    ASSERT_EQ(inc->line, 10);
    ASSERT_STR_EQ(inc->source_qn, first->qualified_name);
    ASSERT_NOT_NULL(dm_find_token(r,
                                  "include::../../../../src/main/java/org/x/Calc.java[lines=4..6]"
                                  "\t../../../../src/main/java/org/x/Calc.java\t\t4..6"));
    /* nothing from a comment block or a line comment */
    ASSERT_FALSE(adoc_raw_has(r, "Hidden.java"));
    ASSERT_FALSE(adoc_raw_has(r, "Commented.java"));
    /* attribute references: built-ins are none; the page's own carry its value */
    ASSERT_NOT_NULL(dm_find_token(r, "{Assertions}\tAssertions\t\t"));
    ASSERT_FALSE(adoc_raw_has(r, "{nbsp}"));
    /* monospace spans */
    ASSERT_EQ(dm_find_token(r, "src/main/java/org/x/Assertions.java")->syntax,
              CBM_DOCLINK_ADOC_CODE_PATH);
    ASSERT_EQ(dm_find_token(r, "org.x.Assertions")->syntax, CBM_DOCLINK_ADOC_CODE_NAME);
    cbm_free_result(r);
    PASS();
}

static const char *const ANTORA_YML =
    "name: guide\n"
    "version: true\n"
    "ext:\n"
    "  collector:\n"
    "    scan:\n"
    "    - dir: ./build/generated\n"
    "      clean: true\n"
    "    - dir: ./src/test\n"
    "      into: modules/ROOT/examples\n"
    "asciidoc:\n"
    "  attributes:\n"
    "    javadoc-root: \"xref:attachment$api\"\n"
    "    # API\n"
    "    Assertions: '{javadoc-root}/org.x/org/x/Assertions.html[Assertions]'\n"
    "    assertEquals: '{javadoc-root}/org.x/org/x/Assertions.html#assertEquals(int,int)[x]'\n"
    "    Missing: '{javadoc-root}/org.x/org/x/Gone.html[Gone]'\n";

TEST(adoc_antora_scope_blob) {
    CBMFileResult *r = dm_extract(ANTORA_YML, CBM_LANG_YAML, "docs/antora.yml");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_STR_EQ(r->doc_scope,
                  "ad1\n"
                  "C\n"
                  "N\tguide\n"
                  "D\t./build/generated\n"
                  "D\t./src/test\n"
                  "I\tmodules/ROOT/examples\n"
                  "A\tjavadoc-root\txref:attachment$api\n"
                  "A\tAssertions\t{javadoc-root}/org.x/org/x/Assertions.html[Assertions]\n"
                  "A\tassertEquals\t{javadoc-root}/org.x/org/x/Assertions.html#assertEquals(int,"
                  "int)[x]\n"
                  "A\tMissing\t{javadoc-root}/org.x/org/x/Gone.html[Gone]\n");
    cbm_free_result(r);
    r = dm_extract("site:\n  title: x\n", CBM_LANG_YAML, "config/other.yml");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(r->doc_scope);
    cbm_free_result(r);
    PASS();
}

static void adoc_write_repo(const char *repo, const char *first_tests) {
    th_write_file(TH_PATH(repo, "docs/antora.yml"), ANTORA_YML);
    th_write_file(TH_PATH(repo, "docs/modules/ROOT/pages/writing.adoc"), ADOC_PAGE);
    th_write_file(TH_PATH(repo, "docs/src/test/java/example/FirstTests.java"), first_tests);
    th_write_file(TH_PATH(repo, "src/main/java/org/x/Assertions.java"),
                  "package org.x;\n"
                  "\n"
                  "public class Assertions {\n"
                  "    public static void assertEquals(int a, int b) {}\n"
                  "    public static void fail() {}\n"
                  "}\n");
    /* a tag region that takes the imports with the class: it binds the class */
    th_write_file(TH_PATH(repo, "docs/src/test/java/example/WithImports.java"),
                  "package example;\n"
                  "\n"
                  "// tag::all[]\n"
                  "import java.util.List;\n"
                  "\n"
                  "class WithImports {\n"
                  "    List<String> names() { return null; }\n"
                  "}\n"
                  "// end::all[]\n");
    th_write_file(TH_PATH(repo, "src/main/java/org/x/Calc.java"),
                  "package org.x;\n"
                  "\n"
                  "public class Calc {\n"
                  "    public int add(int a, int b) {\n"
                  "        return a + b;\n"
                  "    }\n"
                  "}\n");
}

static const char *const FIRST_TESTS = "package example;\n"         /* 1 */
                                       "\n"                         /* 2 */
                                       "class FirstTests {\n"       /* 3 */
                                       "    void helper() {}\n"     /* 4 */
                                       "\n"                         /* 5 */
                                       "    // tag::user_guide[]\n" /* 6 */
                                       "    void addition() {\n"    /* 7 */
                                       "        helper();\n"        /* 8 */
                                       "    }\n"                    /* 9 */
                                       "    // end::user_guide[]\n" /* 10 */
                                       "}\n";                       /* 11 */

static bool adoc_edge(const char *db, const char *src, const char *tgt, const char *needle) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    return n == 1 && (!needle || strstr(props, needle));
}

TEST(adoc_links_pipeline) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dladoc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    adoc_write_repo(repo, FIRST_TESTS);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    const char *page = "docs.modules.ROOT.pages.writing";
    char src[256];
    /* a resource ID through a collector scan; the tag region's innermost method */
    snprintf(src, sizeof(src), "%s.First-Test", page);
    ASSERT_TRUE(adoc_edge(db, src, "example.FirstTests.addition",
                          "{\"via\":\"asciidoc\",\"syntax\":\"include\",\"tier\":\"exact\""));
    ASSERT_TRUE(adoc_edge(db, src, "example.FirstTests.addition", "\"target_lines\":[7,9]"));
    /* a javadoc attribute (under the document title): the type it documents */
    snprintf(src, sizeof(src), "%s.Writing-Tests", page);
    ASSERT_TRUE(adoc_edge(db, src, "org.x.Assertions", "\"syntax\":\"attribute\""));
    /* lines= and a relative include */
    snprintf(src, sizeof(src), "%s.Details", page);
    ASSERT_TRUE(adoc_edge(db, src, "org.x.Calc.add", "\"target_lines\":[4,6]"));
    /* the region's imports are no definition: the one it holds whole binds */
    ASSERT_TRUE(adoc_edge(db, src, "example.WithImports", "\"target_lines\":[4,8]"));
    /* monospace spans through the Markdown rules */
    ASSERT_TRUE(adoc_edge(db, src, "src.main.java.org.x.Assertions.java.__file__",
                          "\"syntax\":\"code_path\""));
    /* back navigation: the method knows the section that includes it */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
                           "nodes t ON t.id=e.target_id WHERE e.type='MENTIONS' AND "
                           "t.name='addition' AND s.label='Section' AND s.name='First Test'"),
              1);
    th_cleanup(tmp);
    PASS();
}

TEST(adoc_links_incremental) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dladoc_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    adoc_write_repo(repo, FIRST_TESTS);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* the tagged region moves to another method: the page follows it */
    th_write_file(TH_PATH(repo, "docs/src/test/java/example/FirstTests.java"),
                  "package example;\n"
                  "\n"
                  "class FirstTests {\n"
                  "    // tag::user_guide[]\n"
                  "    void helper() {}\n"
                  "    // end::user_guide[]\n"
                  "\n"
                  "    void addition() {\n"
                  "        helper();\n"
                  "    }\n"
                  "}\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "tag moved", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    /* a component attribute changes: every page re-resolves */
    th_write_file(TH_PATH(repo, "docs/antora.yml"),
                  "name: guide\n"
                  "ext:\n"
                  "  collector:\n"
                  "    scan:\n"
                  "    - dir: ./src/test\n"
                  "      into: modules/ROOT/examples\n"
                  "asciidoc:\n"
                  "  attributes:\n"
                  "    javadoc-root: \"xref:attachment$api\"\n"
                  "    Assertions: '{javadoc-root}/org.x/org/x/Calc.html[Calc]'\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "attribute change", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    th_cleanup(tmp);
    PASS();
}

SUITE(doc_links_adoc) {
    dm_ship_held_families();
    RUN_TEST(adoc_scan_structure);
    RUN_TEST(adoc_antora_scope_blob);
    RUN_TEST(adoc_links_pipeline);
    RUN_TEST(adoc_links_incremental);
    cbm_doclink_test_reset_ships();
}
