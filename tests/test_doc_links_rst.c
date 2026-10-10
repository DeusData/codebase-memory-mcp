/*
 * test_doc_links_rst.c — reStructuredText documents -> Sections and MENTIONS
 * edges (doclink_rst.c, doclink_py.c, doc_links_rst.c).
 *
 * The line model (titles, literal blocks, directives, comments), the tokens
 * and their resolution contexts, the Python scope blob (package re-exports,
 * Sphinx conf.py), the resolver's rules as the field test measured them
 * (forms and re-exports, a module never answers a member's role, bare members
 * are never searched, a name written in full binds even test code while a
 * search leaves it out, intersphinx and other projects are no rows), the C
 * domain, include / literalinclude / kernel-doc, extlinks, inline literals,
 * Sphinx's source_suffix in discovery, reST ADRs, incremental == full.
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

/* ── the scanner ─────────────────────────────────────────────────── */

static const char *const RST_DOC =
    "Intro names :class:`pkg.Top` before any title.\n"           /* 1 */
    "\n"                                                         /* 2 */
    "=========\n"                                                /* 3 */
    " The API\n"                                                 /* 4 */
    "=========\n"                                                /* 5 */
    "\n"                                                         /* 6 */
    ".. module:: pkg\n"                                          /* 7 */
    "\n"                                                         /* 8 */
    "Use :class:`Model` and :meth:`~pkg.Model.save`::\n"         /* 9 */
    "\n"                                                         /* 10 */
    "    :class:`NotARole` in a literal block\n"                 /* 11 */
    "    Fake Title\n"                                           /* 12 */
    "    ==========\n"                                           /* 13 */
    "\n"                                                         /* 14 */
    ".. code-block:: python\n"                                   /* 15 */
    "\n"                                                         /* 16 */
    "   x = :func:`nope`\n"                                      /* 17 */
    "\n"                                                         /* 18 */
    ".. class:: Model(name)\n"                                   /* 19 */
    "\n"                                                         /* 20 */
    "   .. method:: save(force=False)\n"                         /* 21 */
    "\n"                                                         /* 22 */
    "      Saves; see :attr:`name`.\n"                           /* 23 */
    "\n"                                                         /* 24 */
    "Usage\n"                                                    /* 25 */
    "-----\n"                                                    /* 26 */
    "\n"                                                         /* 27 */
    ".. autofunction:: pkg.base.top\n"                           /* 28 */
    "\n"                                                         /* 29 */
    ".. literalinclude:: ../pkg/base.py\n"                       /* 30 */
    "   :lines: 2-3\n"                                           /* 31 */
    "\n"                                                         /* 32 */
    ".. include:: ../pkg/snippet.py\n"                           /* 33 */
    "\n"                                                         /* 34 */
    ".. include:: other.rst\n"                                   /* 35 */
    "\n"                                                         /* 36 */
    ".. kernel-doc:: drivers/x.c\n"                              /* 37 */
    "   :identifiers: foo bar\n"                                 /* 38 */
    "\n"                                                         /* 39 */
    ".. a comment with :class:`Hidden`\n"                        /* 40 */
    "\n"                                                         /* 41 */
    "Text ``see :class:`Hidden2` there`` and ``pkg/base.py``,\n" /* 42 */
    "``pkg.base.top`` and :source:`a file <pkg/base.py>`.\n";    /* 43 */

static int rst_count(const CBMFileResult *r, int syntax) {
    int n = 0;
    for (int i = 0; i < r->doc_links.count; i++) {
        n += r->doc_links.items[i].syntax == syntax;
    }
    return n;
}

static bool rst_raw_has(const CBMFileResult *r, const char *needle) {
    for (int i = 0; i < r->doc_links.count; i++) {
        if (strstr(r->doc_links.items[i].raw, needle)) {
            return true;
        }
    }
    return false;
}

static const CBMDefinition *rst_def(const CBMFileResult *r, const char *label, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->label && strcmp(d->label, label) == 0 && d->name && strcmp(d->name, name) == 0) {
            return d;
        }
    }
    return NULL;
}

TEST(rst_scan_structure) {
    CBMFileResult *r = dm_extract(RST_DOC, CBM_LANG_RST, "docs/api.rst");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->doc_links.failed);
    /* titles: over+under and under only; a title shape in a literal block is none */
    const CBMDefinition *api = rst_def(r, "Section", "The API");
    const CBMDefinition *usage = rst_def(r, "Section", "Usage");
    ASSERT_NOT_NULL(api);
    ASSERT_NOT_NULL(usage);
    ASSERT_NULL(rst_def(r, "Section", "Fake Title"));
    ASSERT_EQ(api->start_line, 4);
    ASSERT_EQ(api->end_line, 23);
    ASSERT_EQ(usage->start_line, 25);
    ASSERT_STR_EQ(api->qualified_name, "p.docs.api.The-API");
    ASSERT_NOT_NULL(api->docstring);
    ASSERT_NOT_NULL(strstr(api->docstring, "Use :class:`Model`"));
    /* nothing from a literal block, a code directive, a comment or an inline literal */
    ASSERT_FALSE(rst_raw_has(r, "NotARole"));
    ASSERT_FALSE(rst_raw_has(r, "nope"));
    ASSERT_FALSE(rst_raw_has(r, "Hidden"));
    /* text before the first title belongs to the file */
    const CBMDocLink *top = dm_find_token(r, ":class:`pkg.Top`\tclass\tpkg.Top\t\t\t");
    ASSERT_NOT_NULL(top);
    ASSERT_EQ(top->flags, CBM_DOCLINK_FLAG_FILE);
    ASSERT_EQ(top->line, 1);
    /* roles carry the module and class in force */
    const CBMDocLink *model = dm_find_token(r, ":class:`Model`\tclass\tModel\tpkg\t\t");
    ASSERT_NOT_NULL(model);
    ASSERT_STR_EQ(model->source_qn, "p.docs.api.The-API");
    ASSERT_EQ(model->line, 9);
    ASSERT_NOT_NULL(dm_find_token(r, ":attr:`name`\tattr\tname\tpkg\tModel\t"));
    /* object directives: the signature and Python's full name in its class */
    ASSERT_NOT_NULL(dm_find_token(r, ".. class:: Model(name)\tclass\tModel(name)\tpkg\t\t\tModel"));
    ASSERT_NOT_NULL(dm_find_token(
        r, ".. method:: save(force=False)\tmethod\tsave(force=False)\tpkg\tModel\t\tModel.save"));
    ASSERT_NOT_NULL(dm_find_token(r, ".. module:: pkg\tmodule\tpkg\tpkg\t\t\t"));
    /* autodoc, literalinclude, an include of code (not of a document), kernel-doc names */
    ASSERT_NOT_NULL(
        dm_find_token(r, ".. autofunction:: pkg.base.top\tautofunction\tpkg.base.top\tpkg\t"));
    ASSERT_NOT_NULL(
        dm_find_token(r, ".. literalinclude:: ../pkg/base.py\t../pkg/base.py\t2-3\t\t"));
    ASSERT_EQ(rst_count(r, CBM_DOCLINK_RST_INCLUDE), 1);
    ASSERT_EQ(rst_count(r, CBM_DOCLINK_RST_KERNEL_DOC), 3);
    ASSERT_NOT_NULL(dm_find_token(r, ".. kernel-doc:: drivers/x.c\tdrivers/x.c\tbar"));
    /* inline literals naming a path or a qualified name; a titled extlink */
    ASSERT_NOT_NULL(dm_find_token(r, "pkg/base.py"));
    ASSERT_EQ(dm_find_token(r, "pkg/base.py")->syntax, CBM_DOCLINK_RST_CODE_PATH);
    ASSERT_EQ(dm_find_token(r, "pkg.base.top")->syntax, CBM_DOCLINK_RST_CODE_NAME);
    ASSERT_NOT_NULL(
        dm_find_token(r, ":source:`a file <pkg/base.py>`\tsource\ta file <pkg/base.py>\tpkg\t\t"));
    cbm_free_result(r);
    PASS();
}

/* ── Python's scope ──────────────────────────────────────────────── */

TEST(rst_python_scope_blob) {
    CBMFileResult *r = dm_extract("from .base import Model\n"
                                  "from .sub.thing import helper as aid, other\n"
                                  "from ..up import x\n"
                                  "from os import *\n"
                                  "from django.db import models\n"
                                  "try:\n"
                                  "    from .opt import Nested\n"
                                  "except ImportError:\n"
                                  "    pass\n",
                                  CBM_LANG_PYTHON, "pkg/__init__.py");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_STR_EQ(r->doc_scope, "py1\n"
                                "I\t1\tbase\tModel\tModel\n"
                                "I\t1\tsub.thing\thelper\taid\n"
                                "I\t1\tsub.thing\tother\tother\n"
                                "I\t2\tup\tx\tx\n"
                                "I\t0\tdjango.db\tmodels\tmodels\n");
    ASSERT_EQ(r->doc_links.count, 0);
    cbm_free_result(r);
    r = dm_extract("project = 'x'\n"
                   "primary_domain = 'c'\n"
                   "intersphinx_mapping = {\n"
                   "    'python': ('https://docs.python.org/3', None),\n"
                   "    \"sphinx\": (\"https://www.sphinx-doc.org/en/master/\", None),\n"
                   "}\n"
                   "extlinks = {\n"
                   "    'source': ('https://github.com/o/r/blob/main/%s', '%s'),\n"
                   "    'issue': ('https://github.com/o/r/issues/%s', '#%s'),\n"
                   "}\n"
                   "extlinks['tree'] = ('https://github.com/o/r/tree/main/%s', '%s')\n",
                   CBM_LANG_PYTHON, "docs/conf.py");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(r->doc_scope, "py1\n"
                                "C\n"
                                "D\tc\n"
                                "S\tpython\n"
                                "S\tsphinx\n"
                                "X\tsource\n"
                                "X\ttree\n");
    cbm_free_result(r);
    r = dm_extract("from .x import y\n", CBM_LANG_PYTHON, "pkg/mod.py");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(r->doc_scope);
    cbm_free_result(r);
    PASS();
}

/* ── the pipeline ────────────────────────────────────────────────── */

static const char *const API_RST =
    "Models\n"                                                            /* 1 */
    "======\n"                                                            /* 2 */
    "\n"                                                                  /* 3 */
    ".. module:: pkg\n"                                                   /* 4 */
    "\n"                                                                  /* 5 */
    "Use :class:`Model`, :func:`pkg.aid` and :meth:`save`.\n"             /* 6 */
    "Not :class:`python:dict`, :class:`otherlib.Thing`; gone\n"           /* 7 */
    ":class:`pkg.Missing`.\n"                                             /* 8 */
    "\n"                                                                  /* 9 */
    ".. currentmodule:: pkg.views\n"                                      /* 10 */
    "\n"                                                                  /* 11 */
    ".. class:: View\n"                                                   /* 12 */
    "\n"                                                                  /* 13 */
    "   Calls :meth:`dispatch`.\n"                                        /* 14 */
    "\n"                                                                  /* 15 */
    "Testing\n"                                                           /* 16 */
    "=======\n"                                                           /* 17 */
    "\n"                                                                  /* 18 */
    "Run :class:`pkg.test.runner.Runner`, not :class:`Runner`.\n"         /* 19 */
    "\n"                                                                  /* 20 */
    "Code\n"                                                              /* 21 */
    "====\n"                                                              /* 22 */
    "\n"                                                                  /* 23 */
    "See :source:`pkg/base.py`, ``pkg.base.top`` and :c:func:`c_func`.\n" /* 24 */
    "\n"                                                                  /* 25 */
    ".. autofunction:: pkg.base.top\n"                                    /* 26 */
    "\n"                                                                  /* 27 */
    ".. literalinclude:: ../pkg/base.py\n"                                /* 28 */
    "   :lines: 2-3\n"                                                    /* 29 */
    "\n"                                                                  /* 30 */
    ".. include:: /../pkg/sub/thing.py\n"                                 /* 31 */
    "\n"                                                                  /* 32 */
    ".. kernel-doc:: csrc/lib.c\n"                                        /* 33 */
    "   :identifiers: c_func\n";                                          /* 34 */

static void rst_write_repo(const char *repo, const char *init) {
    th_write_file(TH_PATH(repo, "pkg/__init__.py"), init);
    th_write_file(TH_PATH(repo, "pkg/base.py"), "class Model:\n"
                                                "    def save(self):\n"
                                                "        return None\n"
                                                "\n"
                                                "\n"
                                                "def top():\n"
                                                "    return 1\n");
    th_write_file(TH_PATH(repo, "pkg/other.py"), "class Model:\n"
                                                 "    pass\n");
    th_write_file(TH_PATH(repo, "pkg/sub/__init__.py"), "");
    th_write_file(TH_PATH(repo, "pkg/sub/thing.py"), "def helper():\n"
                                                     "    return 2\n");
    th_write_file(TH_PATH(repo, "pkg/views.py"), "class View:\n"
                                                 "    def dispatch(self):\n"
                                                 "        return None\n");
    th_write_file(TH_PATH(repo, "pkg/test/__init__.py"), "");
    th_write_file(TH_PATH(repo, "pkg/test/runner.py"), "class Runner:\n"
                                                       "    pass\n");
    /* tests/ is no package: tests/dispatch's import name is `dispatch` */
    th_write_file(TH_PATH(repo, "tests/dispatch/__init__.py"), "");
    th_write_file(TH_PATH(repo, "tests/dispatch/test_x.py"), "def test_x():\n"
                                                             "    return None\n");
    th_write_file(TH_PATH(repo, "csrc/lib.c"), "int c_func(void) {\n"
                                               "    return 0;\n"
                                               "}\n");
    th_write_file(TH_PATH(repo, "docs/conf.py"),
                  "project = 'x'\n"
                  "source_suffix = {'.rst': 'restructuredtext', '.txt': 'restructuredtext'}\n"
                  "intersphinx_mapping = {\n"
                  "    'python': ('https://docs.python.org/3', None),\n"
                  "}\n"
                  "extlinks = {\n"
                  "    'source': ('https://github.com/o/r/blob/main/%s', '%s'),\n"
                  "}\n");
    th_write_file(TH_PATH(repo, "docs/api.rst"), API_RST);
    th_write_file(TH_PATH(repo, "docs/topics/guide.txt"), "Guide\n"
                                                          "=====\n"
                                                          "\n"
                                                          "The :class:`pkg.views.View`.\n");
    th_write_file(TH_PATH(repo, "notes/readme.txt"), "Title\n"
                                                     "=====\n"
                                                     "\n"
                                                     "Plain :class:`pkg.views.View`.\n");
}

static const char *const INIT_PY = "from .base import Model\n"
                                   "from .sub.thing import helper as aid\n";

static bool rst_edge(const char *db, const char *src, const char *tgt, const char *needle) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    return n == 1 && (!needle || strstr(props, needle));
}

static int rst_rows(const char *db, const char *raw_prefix, const char *reason) {
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT COUNT(*) FROM doc_link_unresolved WHERE raw LIKE '%s%%' AND reason='%s'",
             raw_prefix, reason);
    return dm_count(db, sql);
}

TEST(rst_links_pipeline) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlrst_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    rst_write_repo(repo, INIT_PY);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* a re-export through the module context: pkg.Model is pkg/base.py's */
    ASSERT_TRUE(rst_edge(db, "docs.api.Models", "pkg.base.Model",
                         "{\"via\":\"rst\",\"syntax\":\"role\",\"tier\":\"exact\",\"line\":6"));
    /* an aliased re-export: pkg.aid is helper */
    ASSERT_TRUE(rst_edge(db, "docs.api.Models", "pkg.sub.thing.helper", "\"tier\":\"exact\""));
    /* a bare member is never searched; intersphinx and another project: no rows */
    ASSERT_EQ(dm_count(db,
                       "SELECT COUNT(*) FROM doc_link_unresolved WHERE raw LIKE ':meth:`save`%' "
                       "OR raw LIKE ':class:`python:dict`%' OR raw LIKE "
                       "':class:`otherlib.Thing`%'"),
              0);
    ASSERT_EQ(rst_rows(db, ":class:`pkg.Missing`", "missing"), 1);
    /* a module never answers :meth: -- the class context does */
    ASSERT_TRUE(rst_edge(db, "docs.api.Models", "pkg.views.View.dispatch", "\"tier\":\"exact\""));
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes t ON t.id=e.target_id WHERE "
                           "e.type='MENTIONS' AND t.file_path LIKE 'tests/%'"),
              0);
    /* the object directive declares View in its module */
    ASSERT_TRUE(rst_edge(db, "docs.api.Models", "pkg.views.View", "\"syntax\":\"object\""));
    /* a name written in full binds test code; a search for it does not */
    ASSERT_TRUE(rst_edge(db, "docs.api.Testing", "pkg.test.runner.Runner", "\"tier\":\"exact\""));
    ASSERT_EQ(rst_rows(db, ":class:`Runner`", "test_only_target"), 1);
    /* extlink, inline literal, C domain, autodoc */
    ASSERT_TRUE(rst_edge(db, "docs.api.Code", "pkg.base.py.__file__", "\"syntax\":\"role\""));
    ASSERT_TRUE(rst_edge(db, "docs.api.Code", "pkg.base.top", "\"syntax\":\"code_name\""));
    ASSERT_TRUE(rst_edge(db, "docs.api.Code", "csrc.lib.c_func", NULL));
    /* literalinclude :lines: the innermost definition, the lines on the edge */
    ASSERT_TRUE(rst_edge(db, "docs.api.Code", "pkg.base.Model.save", "\"target_lines\":[2,3]"));
    /* include from the documentation set's directory; kernel-doc file */
    ASSERT_TRUE(
        rst_edge(db, "docs.api.Code", "pkg.sub.thing.py.__file__", "\"syntax\":\"include\""));
    ASSERT_TRUE(rst_edge(db, "docs.api.Code", "csrc.lib.c.__file__", "\"syntax\":\"kernel_doc\""));
    /* conf.py's source_suffix: .txt is reST in its documentation set only */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label='Section' AND "
                           "file_path='docs/topics/guide.txt' AND name='Guide'"),
              1);
    ASSERT_TRUE(rst_edge(db, "docs.topics.guide.Guide", "pkg.views.View", "\"via\":\"rst\""));
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE file_path='notes/readme.txt'"), 0);
    /* back navigation: the class knows the sections that name it */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
                           "nodes t ON t.id=e.target_id WHERE e.type='MENTIONS' AND "
                           "t.name='View' AND t.label='Class' AND s.label='Section'"),
              2);
    th_cleanup(tmp);
    PASS();
}

/* :c:member: names a field through its struct: the owner is the segment
 * before the last dot (-> reads as .), however far the name is qualified. */
TEST(rst_c_member_owner) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlrst_cm_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    th_write_file(TH_PATH(repo, "csrc/shapes.c"), "struct point {\n"
                                                  "    int x;\n"
                                                  "    int y;\n"
                                                  "};\n"
                                                  "\n"
                                                  "struct box {\n"
                                                  "    struct point corner;\n"
                                                  "    int x;\n"
                                                  "};\n");
    th_write_file(TH_PATH(repo, "docs/conf.py"), "project = 'x'\n");
    th_write_file(TH_PATH(repo, "docs/api.rst"),
                  "Shapes\n"
                  "======\n"
                  "\n"
                  "Read :c:member:`point.x`, :c:member:`shapes.box.x` and\n"
                  ":c:member:`struct point->y`; not :c:member:`box.corner.y`.\n");
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* the owner runs back to the start of the name */
    ASSERT_TRUE(rst_edge(db, "docs.api.Shapes", "csrc.shapes.point.x", "\"syntax\":\"role\""));
    /* the owner runs back to the dot before it */
    ASSERT_TRUE(rst_edge(db, "docs.api.Shapes", "csrc.shapes.box.x", "\"syntax\":\"role\""));
    ASSERT_TRUE(rst_edge(db, "docs.api.Shapes", "csrc.shapes.point.y", "\"syntax\":\"role\""));
    /* corner is a field, not a struct with a y */
    ASSERT_EQ(rst_rows(db, ":c:member:`box.corner.y`", "missing"), 1);
    th_cleanup(tmp);
    PASS();
}

TEST(rst_adr_record) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlrst_adr_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    th_write_file(TH_PATH(repo, "pkg/base.py"), "class Model:\n"
                                                "    pass\n");
    th_write_file(TH_PATH(repo, "docs/decisions/0002-use-models.rst"), "2. Use models\n"
                                                                       "=============\n"
                                                                       "\n"
                                                                       "Status\n"
                                                                       "------\n"
                                                                       "\n"
                                                                       "Accepted\n"
                                                                       "\n"
                                                                       "Context\n"
                                                                       "-------\n"
                                                                       "\n"
                                                                       "We store records.\n"
                                                                       "\n"
                                                                       "Decision\n"
                                                                       "--------\n"
                                                                       "\n"
                                                                       "Use ``pkg.base.Model``.\n");
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label='ADR' AND name='ADR-2' AND "
                           "json_extract(properties,'$.status')='accepted'"),
              1);
    ASSERT_TRUE(rst_edge(db, "docs.decisions.0002-use-models.Decision", "pkg.base.Model",
                         "\"syntax\":\"code_name\""));
    th_cleanup(tmp);
    PASS();
}

TEST(rst_links_incremental) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlrst_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    rst_write_repo(repo, INIT_PY);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_TRUE(rst_edge(db, "docs.api.Code", "pkg.base.Model.save", "\"target_lines\":[2,3]"));
    /* a body edit moves the lines a literalinclude names: the document
     * re-resolves with the code file (its names did not change) */
    th_write_file(TH_PATH(repo, "pkg/base.py"), "# moved\n"
                                                "# down\n"
                                                "class Model:\n"
                                                "    def save(self):\n"
                                                "        return None\n"
                                                "\n"
                                                "\n"
                                                "def top():\n"
                                                "    return 1\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "lines moved", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_FALSE(rst_edge(db, "docs.api.Code", "pkg.base.Model.save", "\"target_lines\":[2,3]"));
    /* the document changes alone: it re-resolves through the stored re-exports */
    th_write_file(TH_PATH(repo, "docs/api.rst"), "Models\n"
                                                 "======\n"
                                                 "\n"
                                                 ".. module:: pkg\n"
                                                 "\n"
                                                 "Only :class:`Model` and :func:`aid`.\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "document edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(rst_edge(db, "docs.api.Models", "pkg.base.Model", NULL));
    /* the package re-exports another Model: the unchanged document follows */
    th_write_file(TH_PATH(repo, "pkg/__init__.py"), "from .other import Model\n"
                                                    "from .sub.thing import helper as aid\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "re-export change", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_TRUE(rst_edge(db, "docs.api.Models", "pkg.other.Model", NULL));
    th_cleanup(tmp);
    PASS();
}

/* Two held-out findings (supplementary audit): `.. py:function:: autofit` on a
 * page without a module bound the example script module examples/autofit.py
 * (an object directive declares a definition, never a module), and
 * `:lines: 5-` (line 5 to the file's end) was read as line 5 alone. */
TEST(rst_object_and_open_lines) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlrsto_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    th_write_file(TH_PATH(repo, "examples/autofit.py"), "print('autofit example')\n");
    th_write_file(TH_PATH(repo, "examples/tail.py"), "import os\n"          /* 1 */
                                                     "\n"                   /* 2 */
                                                     "\n"                   /* 3 */
                                                     "class Tail:\n"        /* 4 */
                                                     "    def a(self):\n"   /* 5 */
                                                     "        return 1\n"   /* 6 */
                                                     "\n"                   /* 7 */
                                                     "    def b(self):\n"   /* 8 */
                                                     "        return 2\n"); /* 9 */
    th_write_file(TH_PATH(repo, "docs/conf.py"), "project = 'x'\n");
    th_write_file(TH_PATH(repo, "docs/worksheet.rst"), "Worksheet\n"
                                                       "=========\n"
                                                       "\n"
                                                       ".. py:function:: autofit()\n"
                                                       "\n"
                                                       "   Fit the columns.\n"
                                                       "\n"
                                                       ".. literalinclude:: ../examples/tail.py\n"
                                                       "   :lines: 5-\n");
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes t ON t.id=e.target_id WHERE "
                           "e.type='MENTIONS' AND t.file_path='examples/autofit.py'"),
              0);
    ASSERT_EQ(rst_rows(db, ".. py:function:: autofit", "missing"), 1);
    /* lines 5 to 9 lie in class Tail; line 5 alone lies in method a */
    ASSERT_TRUE(
        rst_edge(db, "docs.worksheet.Worksheet", "examples.tail.Tail", "\"target_lines\":[5,9]"));
    th_cleanup(tmp);
    PASS();
}

/* The production gate (doclink.h): object directives ship since their
 * held-out audit; include and kernel-doc are still held, so in the same
 * documents they write rows, not edges. Runs after the suite's forced
 * families are reset. */
TEST(rst_ship_gate) {
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_RST_OBJECT));
    ASSERT_FALSE(cbm_doclink_syntax_ships(CBM_DOCLINK_RST_INCLUDE));
    ASSERT_FALSE(cbm_doclink_syntax_ships(CBM_DOCLINK_RST_KERNEL_DOC));
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlrst_gate_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    rst_write_repo(repo, INIT_PY);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* `.. class:: View` in pkg.views: an edge */
    ASSERT_TRUE(rst_edge(db, "docs.api.Models", "pkg.views.View", "\"syntax\":\"object\""));
    /* the include resolves, and its family holds it as a row */
    ASSERT_FALSE(rst_edge(db, "docs.api.Code", "pkg.sub.thing.py.__file__", NULL));
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE "
                           "reason = 'below_bar_tier' AND syntax = 'include'"),
              1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS' AND "
                           "properties LIKE '%\"syntax\":\"kernel_doc\"%'"),
              0);
    th_cleanup(tmp);
    PASS();
}

SUITE(doc_links_rst) {
    dm_ship_held_families();
    RUN_TEST(rst_scan_structure);
    RUN_TEST(rst_python_scope_blob);
    RUN_TEST(rst_links_pipeline);
    RUN_TEST(rst_c_member_owner);
    RUN_TEST(rst_object_and_open_lines);
    RUN_TEST(rst_adr_record);
    RUN_TEST(rst_links_incremental);
    cbm_doclink_test_reset_ships();
    RUN_TEST(rst_ship_gate);
}
