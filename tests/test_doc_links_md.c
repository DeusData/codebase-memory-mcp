/*
 * test_doc_links_md.c — Markdown documents -> MENTIONS edges.
 *
 * Extraction (doclink_md.c: which text is a reference, its family, line and
 * section), the span classifier, the resolver (doc_links_md.c: paths, ranges,
 * members, qualified names, the unresolved reasons, what is no reference at
 * all), back navigation, and incremental == full. Pipeline tests index a real
 * fixture through cbm_pipeline_run (helpers: test_doc_mentions_helpers.h).
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

/* ── the fixture ─────────────────────────────────────────────────── */

/* docs/guide.md, line by line (the line numbers are asserted below). */
static const char MD_GUIDE[] =
    "Intro names `pkg/config.go` before any heading.\n"      /* 1 */
    "\n"                                                     /* 2 */
    "# Guide\n"                                              /* 3 */
    "\n"                                                     /* 4 */
    "See [the config](../pkg/config.go) and [routing][r].\n" /* 5 */
    "\n"                                                     /* 6 */
    "[r]: ../app/routing.py\n"                               /* 7 */
    "\n"                                                     /* 8 */
    "## Paths\n"                                             /* 9 */
    "\n"                                                     /* 10 */
    "Range `app/routing.py#L4-L5`, span `app/routing.py:1-9`, bare "
    "app/applications.py.\n" /* 11 */
    "Dir `app/middleware/`, route `/docs`, ![x](img/a.png) and "
    "https://example.org/a/b.py.\n" /* 12 */
    "\n"                            /* 13 */
    "## Names\n"                    /* 14 */
    "\n"                            /* 15 */
    "Use `app.routing.APIRouter.add`, `app.middleware`, `pkg.Config.Name`, "
    "`self.Name`.\n" /* 16 */
    "Also `os.path.join`, `app.Nope`, `test_routing.only_in_tests`, `main.py`, "
    "`../app/gone.py`.\n"                                                  /* 17 */
    "\n"                                                                   /* 18 */
    "```\n"                                                                /* 19 */
    "`app/fenced.py` inside a fence\n"                                     /* 20 */
    "```\n"                                                                /* 21 */
    "<!-- `app/commented.py` in a comment -->\n"                           /* 22 */
    "<code>app/html.py</code> in HTML, `cfg.Name` through an instance.\n"; /* 23 */

static void md_write_fixture(const char *tmp) {
    th_write_file(TH_PATH(tmp, "docs/guide.md"), MD_GUIDE);
    th_write_file(TH_PATH(tmp, "app/__init__.py"), "");
    th_write_file(TH_PATH(tmp, "app/routing.py"), "class APIRouter:\n"      /* 1 */
                                                  "    prefix = ''\n"       /* 2 */
                                                  "\n"                      /* 3 */
                                                  "    def add(self, p):\n" /* 4 */
                                                  "        return p\n"      /* 5 */
                                                  "\n"                      /* 6 */
                                                  "\n"                      /* 7 */
                                                  "def helper():\n"         /* 8 */
                                                  "    return 1\n");        /* 9 */
    th_write_file(TH_PATH(tmp, "app/applications.py"), "class App:\n"
                                                       "    def middleware(self):\n"
                                                       "        return None\n");
    th_write_file(TH_PATH(tmp, "app/middleware/__init__.py"), "VALUE = 1\n");
    th_write_file(TH_PATH(tmp, "tests/test_routing.py"), "def only_in_tests():\n"
                                                         "    return 0\n");
    th_write_file(TH_PATH(tmp, "lib/main.py"), "def run():\n"
                                               "    return None\n");
    th_write_file(TH_PATH(tmp, "pkg/config.go"), "package pkg\n"
                                                 "\n"
                                                 "type Config struct {\n"
                                                 "\tName string\n"
                                                 "}\n"
                                                 "\n"
                                                 "func Util() {}\n");
}

/* ── extraction ──────────────────────────────────────────────────── */

static const CBMDocLink *md_token(const CBMFileResult *r, const char *raw) {
    return dm_find_token(r, raw);
}

static bool md_ends_with(const char *s, const char *suffix) {
    size_t a = s ? strlen(s) : 0;
    size_t b = strlen(suffix);
    return a >= b && strcmp(s + a - b, suffix) == 0;
}

TEST(doc_links_md_extract_tokens) {
    CBMFileResult *r = dm_extract(MD_GUIDE, CBM_LANG_MARKDOWN, "docs/guide.md");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->doc_links.failed);
    /* before the first heading: the file is the source */
    const CBMDocLink *t = md_token(r, "pkg/config.go");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->line, 1);
    ASSERT_EQ(t->syntax, CBM_DOCLINK_MD_CODE_PATH);
    ASSERT_TRUE((t->flags & CBM_DOCLINK_FLAG_FILE) != 0);
    /* an inline link and a link reference definition, under "Guide" */
    t = md_token(r, "../pkg/config.go");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->line, 5);
    ASSERT_EQ(t->syntax, CBM_DOCLINK_MD_LINK);
    ASSERT_TRUE(md_ends_with(t->source_qn, "Guide"));
    ASSERT_EQ(t->def_line, 3);
    ASSERT_EQ(t->flags & CBM_DOCLINK_FLAG_FILE, 0);
    t = md_token(r, "../app/routing.py");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->line, 7);
    ASSERT_EQ(t->syntax, CBM_DOCLINK_MD_LINK);
    /* paths under "Paths" */
    t = md_token(r, "app/routing.py#L4-L5");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->line, 11);
    ASSERT_TRUE(md_ends_with(t->source_qn, "Paths"));
    ASSERT_NOT_NULL(md_token(r, "app/routing.py:1-9"));
    t = md_token(r, "app/applications.py");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->syntax, CBM_DOCLINK_MD_PATH);
    t = md_token(r, "app/middleware/");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->syntax, CBM_DOCLINK_MD_CODE_PATH);
    /* a token (the resolver calls it a route); one directory name alone is a
     * bare path: in a held-out audit `doc/` was another project's as often as not */
    t = md_token(r, "/docs");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->syntax, CBM_DOCLINK_MD_BARE_PATH);
    /* an image, a URL: no tokens */
    ASSERT_NULL(md_token(r, "img/a.png"));
    ASSERT_NULL(md_token(r, "https://example.org/a/b.py"));
    ASSERT_NULL(md_token(r, "a/b.py"));
    /* qualified names under "Names" */
    t = md_token(r, "app.routing.APIRouter.add");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->syntax, CBM_DOCLINK_MD_CODE_NAME);
    ASSERT_EQ(t->line, 16);
    ASSERT_TRUE(md_ends_with(t->source_qn, "Names"));
    ASSERT_NOT_NULL(md_token(r, "pkg.Config.Name"));
    ASSERT_NULL(md_token(r, "self.Name")); /* one identifier after `self.`: a bare name */
    ASSERT_NOT_NULL(md_token(r, "os.path.join"));
    /* a file name alone is its own family: in the held-out audit every wrong
     * code path was one (`config.yaml`: the reader's own file) */
    t = md_token(r, "main.py");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->syntax, CBM_DOCLINK_MD_BARE_PATH);
    /* fenced code and HTML comments are not scanned; HTML <code> is */
    ASSERT_NULL(md_token(r, "app/fenced.py"));
    ASSERT_NULL(md_token(r, "app/commented.py"));
    t = md_token(r, "app/html.py");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->line, 23);
    cbm_free_result(r);
    PASS();
}

/* Two headings with one title share one Section node, which owns only one of
 * them: their references belong to the file, never to the other heading. */
TEST(doc_links_md_repeated_headings) {
    const char *src = "# Changes\n"            /* 1 */
                      "\n"                     /* 2 */
                      "## Fixes\n"             /* 3 */
                      "\n"                     /* 4 */
                      "See `app/one.py`.\n"    /* 5 */
                      "\n"                     /* 6 */
                      "## Fixes\n"             /* 7 */
                      "\n"                     /* 8 */
                      "See `app/two.py`.\n"    /* 9 */
                      "\n"                     /* 10 */
                      "## Unique\n"            /* 11 */
                      "\n"                     /* 12 */
                      "See `app/three.py`.\n"; /* 13 */
    CBMFileResult *r = dm_extract(src, CBM_LANG_MARKDOWN, "CHANGES.md");
    ASSERT_NOT_NULL(r);
    const CBMDocLink *one = md_token(r, "app/one.py");
    const CBMDocLink *two = md_token(r, "app/two.py");
    const CBMDocLink *three = md_token(r, "app/three.py");
    ASSERT_NOT_NULL(one);
    ASSERT_NOT_NULL(two);
    ASSERT_NOT_NULL(three);
    ASSERT_TRUE((one->flags & CBM_DOCLINK_FLAG_FILE) != 0);
    ASSERT_TRUE((two->flags & CBM_DOCLINK_FLAG_FILE) != 0);
    ASSERT_EQ(three->flags & CBM_DOCLINK_FLAG_FILE, 0);
    ASSERT_TRUE(md_ends_with(three->source_qn, "Unique"));
    cbm_free_result(r);
    PASS();
}

typedef struct {
    const char *text;
    CBMDocLinkMdShape shape;
    const char *path;
    const char *member;
    uint32_t first;
    uint32_t last;
    bool instance;
    bool colon;
} md_case_t;

TEST(doc_links_md_classify_span) {
    static const md_case_t cases[] = {
        {"src/app/main.go", CBM_DOCLINK_MD_FILE, "src/app/main.go", NULL, 0, 0, false, false},
        {"src/app/", CBM_DOCLINK_MD_DIR, "src/app/", NULL, 0, 0, false, false},
        {"src/app/main.go#L3-L9", CBM_DOCLINK_MD_FILE, "src/app/main.go", NULL, 3, 9, false, false},
        {"src/app/main.go:12", CBM_DOCLINK_MD_FILE, "src/app/main.go", NULL, 12, 12, false, false},
        {"src/lib.rs::Type::run", CBM_DOCLINK_MD_FILE, "src/lib.rs", "Type::run", 0, 0, false,
         false},
        {"src\\win\\file.cs", CBM_DOCLINK_MD_FILE, "src/win/file.cs", NULL, 0, 0, false, false},
        {"main.go:7-8", CBM_DOCLINK_MD_FILENAME, "main.go", NULL, 7, 8, false, false},
        {"README.md", CBM_DOCLINK_MD_FILENAME, "README.md", NULL, 0, 0, false, false},
        {"Makefile", CBM_DOCLINK_MD_FILENAME, "Makefile", NULL, 0, 0, false, false},
        /* a built or shipped file is a file name, never a member (`OliveTin.exe`
         * took the module stem of OliveTin.exe.manifest in a held-out audit) */
        {"OliveTin.exe", CBM_DOCLINK_MD_FILENAME, "OliveTin.exe", NULL, 0, 0, false, false},
        {"app.manifest", CBM_DOCLINK_MD_FILENAME, "app.manifest", NULL, 0, 0, false, false},
        {"main.rs::run", CBM_DOCLINK_MD_FILENAME, "main.rs", "run", 0, 0, false, false},
        {"fastapi.routing.APIRouter", CBM_DOCLINK_MD_QUALIFIED, "fastapi.routing.APIRouter", NULL,
         0, 0, false, false},
        {"crate::extract::Json", CBM_DOCLINK_MD_QUALIFIED, "extract.Json", NULL, 0, 0, false,
         false},
        {"Vec<u8>::new", CBM_DOCLINK_MD_QUALIFIED, "Vec.new", NULL, 0, 0, false, false},
        {"self.app.state", CBM_DOCLINK_MD_QUALIFIED, "app.state", NULL, 0, 0, true, false},
        {"pkg.module:attr", CBM_DOCLINK_MD_QUALIFIED, "pkg.module.attr", NULL, 0, 0, false, true},
        {"Foo#bar", CBM_DOCLINK_MD_QUALIFIED, "Foo.bar", NULL, 0, 0, false, false},
        {"App\\Http\\Kernel", CBM_DOCLINK_MD_QUALIFIED, "App.Http.Kernel", NULL, 0, 0, false,
         false},
        {"server.New()", CBM_DOCLINK_MD_QUALIFIED, "server.New", NULL, 0, 0, false, false},
        /* no path and no qualified name */
        {"foo", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"self.x", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"Node.js", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"github.com/x/y", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"https://x.org/a.py", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"$HOME/a.py", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"PATH=/usr/bin", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"--verbose", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"go run main.go", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"\"a/b.py\"", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"pkg/Type.Member", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"@scope/pkg", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        /* a traversal or a root names no directory (a held-out link: `../` in a
         * warning about attacker-controlled paths) */
        {"../", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"./", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
        {"../..", CBM_DOCLINK_MD_NONE, NULL, NULL, 0, 0, false, false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const md_case_t *c = &cases[i];
        char buf[256];
        CBMDocLinkMdPath p;
        bool ok = cbm_doclink_md_classify_span(c->text, strlen(c->text), buf, sizeof(buf), &p);
        if (ok != (c->shape != CBM_DOCLINK_MD_NONE) || p.shape != c->shape) {
            printf("  case %s: shape %d, expected %d\n", c->text, (int)p.shape, (int)c->shape);
            FAIL("classification differs");
        }
        if (!ok) {
            continue;
        }
        if (strcmp(p.path, c->path) != 0 ||
            (c->member ? !p.member || strcmp(p.member, c->member) != 0 : p.member != NULL) ||
            p.first_line != c->first || p.last_line != c->last || p.instance != c->instance ||
            p.colon != c->colon) {
            printf("  case %s: path %s member %s lines %u-%u instance %d colon %d\n", c->text,
                   p.path, p.member ? p.member : "(none)", p.first_line, p.last_line, p.instance,
                   p.colon);
            FAIL("classification differs");
        }
    }
    /* a name never fits a buffer by being cut */
    char tiny[8];
    CBMDocLinkMdPath p;
    ASSERT_FALSE(cbm_doclink_md_classify_span("src/app/main.go", 15, tiny, sizeof(tiny), &p));
    PASS();
}

/* ── resolution ──────────────────────────────────────────────────── */

/* Properties of the MENTIONS edge from the section or file whose qualified
 * name ends with `src` to the node whose qualified name ends with `tgt`. */
static int md_edge(const char *db, const char *src, const char *tgt, char *props, size_t cap) {
    int n = 0;
    dm_edge(db, src, tgt, props, cap, &n);
    return n;
}

TEST(doc_links_md_resolve) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    md_write_fixture(tmp);
    char db[512];
    snprintf(db, sizeof(db), "%s/md.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    char props[512];
    char reason[64];
    char syntax[64];

    /* a link and a link reference definition: their files */
    ASSERT_EQ(md_edge(db, "Guide", "config.go.__file__", props, sizeof(props)), 1);
    ASSERT_NOT_NULL(strstr(props, "\"via\":\"markdown\""));
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"link\""));
    ASSERT_NOT_NULL(strstr(props, "\"tier\":\"exact\""));
    ASSERT_NOT_NULL(strstr(props, "\"line\":5"));
    ASSERT_EQ(md_edge(db, "Guide", "routing.py.__file__", props, sizeof(props)), 1);
    /* the reference before the first heading: from the File node */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND s.label = 'File' AND s.file_path = 'docs/guide.md' "
                           "AND t.file_path = 'pkg/config.go' AND t.label = 'File'"),
              1);
    /* a range held by one definition: that definition, the range on the edge */
    ASSERT_EQ(md_edge(db, "Paths", "APIRouter.add", props, sizeof(props)), 1);
    ASSERT_NOT_NULL(strstr(props, "\"target_lines\":[4,5]"));
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"code_path\""));
    /* a range across definitions: the file, with the range */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND s.qualified_name LIKE '%.Paths' AND t.label = 'File' "
                           "AND t.file_path = 'app/routing.py' "
                           "AND e.properties LIKE '%\"target_lines\":[1,9]%'"),
              1);
    /* a bare path in prose; a directory */
    ASSERT_EQ(md_edge(db, "Paths", "applications.py.__file__", props, sizeof(props)), 1);
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"path\""));
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND s.qualified_name LIKE '%.Paths' AND t.label = 'Folder' "
                           "AND t.file_path = 'app/middleware'"),
              1);
    /* a route, an image, a URL: neither edges nor rows */
    dm_row(db, "docs/guide.md", "/docs", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes t ON t.id = e.target_id "
                           "WHERE e.type = 'MENTIONS' AND t.label = 'Folder' "
                           "AND t.file_path = 'docs'"),
              0);
    dm_row(db, "docs/guide.md", "img/a.png", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");

    /* qualified names: a method, a field through its type */
    ASSERT_EQ(md_edge(db, "Names", "routing.APIRouter.add", props, sizeof(props)), 1);
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"code_name\""));
    ASSERT_EQ(md_edge(db, "Names", "Config.Name", props, sizeof(props)), 1);
    /* `app.middleware`: the package as written, not the method App.middleware */
    ASSERT_EQ(md_edge(db, "Names", "App.middleware", props, sizeof(props)), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND s.qualified_name LIKE '%.Names' "
                           "AND t.file_path = 'app/middleware/__init__.py'"),
              1);
    /* a field never through an instance */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes t ON t.id = e.target_id "
                           "WHERE e.type = 'MENTIONS' AND t.label = 'Field' "
                           "AND e.properties LIKE '%\"line\":23%'"),
              0);

    /* unresolved, by reason */
    dm_row(db, "docs/guide.md", "app.Nope", reason, sizeof(reason), syntax, sizeof(syntax));
    ASSERT_STR_EQ(reason, "missing");
    ASSERT_STR_EQ(syntax, "code_name");
    dm_row(db, "docs/guide.md", "test_routing.only_in_tests", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "test_only_target");
    dm_row(db, "docs/guide.md", "main.py", reason, sizeof(reason), syntax, sizeof(syntax));
    ASSERT_STR_EQ(reason, "ambiguous"); /* lib/main.py, but not next to the document */
    ASSERT_STR_EQ(syntax, "bare_path");
    dm_row(db, "docs/guide.md", "../app/gone.py", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    dm_row(db, "docs/guide.md", "app/html.py", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    /* another project's code is no reference to this repository */
    dm_row(db, "docs/guide.md", "os.path.join", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");

    /* back navigation: the sections that mention a method, one hop away */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND t.qualified_name LIKE '%.APIRouter.add' AND s.label = 'Section'"),
              2);

    th_cleanup(tmp);
    PASS();
}

/* ── incremental == full ─────────────────────────────────────────── */

TEST(doc_links_md_incremental) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    md_write_fixture(repo);
    char db[512];
    char full_db[512];
    snprintf(db, sizeof(db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);

    /* a body edit of a linked code file: its linking document re-resolves */
    th_write_file(TH_PATH(repo, "app/routing.py"), "class APIRouter:\n"
                                                   "    prefix = ''\n"
                                                   "\n"
                                                   "    def add(self, p):\n"
                                                   "        return p + p\n"
                                                   "\n"
                                                   "\n"
                                                   "def helper():\n"
                                                   "    return 2\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "code body edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    /* the document itself changes: a new link, a reference gone */
    char guide[sizeof(MD_GUIDE) + 64];
    snprintf(guide, sizeof(guide), "%s\nSee also [helpers](../app/routing.py#L8-L9).\n", MD_GUIDE);
    th_write_file(TH_PATH(repo, "docs/guide.md"), guide);
    ASSERT_EQ(dm_step(repo, db, full_db, "document edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    /* a body edit that moves lines: the document bound by lines (#L8-L9, the
     * helper until now) re-resolves, though no name changed */
    th_write_file(TH_PATH(repo, "app/routing.py"), "# moved\n"
                                                   "# down\n"
                                                   "class APIRouter:\n"
                                                   "    prefix = ''\n"
                                                   "\n"
                                                   "    def add(self, p):\n"
                                                   "        return p + p\n"
                                                   "\n"
                                                   "\n"
                                                   "def helper():\n"
                                                   "    return 2\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "lines moved", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    /* the name a row calls missing is added: the run goes FULL and the row
     * becomes an edge, exactly as a full index has it */
    th_write_file(TH_PATH(repo, "app/nope.py"), "def Nope():\n"
                                                "    return None\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "missing name added", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    th_cleanup(tmp);
    PASS();
}

/* ── ADRs ────────────────────────────────────────────────────────── */

static void md_write_adrs(const char *tmp, const char *first_status) {
    char first[1024];
    snprintf(first, sizeof(first),
             "---\n"
             "status: %s\n"
             "date: 2024-03-05\n"
             "deciders: Ana, Bo\n"
             "---\n"
             "# Use X for storage\n"
             "\n"
             "## Context and Problem Statement\n"
             "\n"
             "We need storage.\n"
             "\n"
             "## Decision Outcome\n"
             "\n"
             "We use X, see `app/routing.py`.\n"
             "\n"
             "Superseded by [ADR-0002](0002-use-y.md).\n",
             first_status);
    th_write_file(TH_PATH(tmp, "docs/adr/0001-use-x.md"), first);
    th_write_file(TH_PATH(tmp, "docs/adr/0002-use-y.md"), "# 2. Use Y instead\n"
                                                          "\n"
                                                          "Date: 12 May 2024\n"
                                                          "\n"
                                                          "## Status\n"
                                                          "\n"
                                                          "Accepted\n"
                                                          "\n"
                                                          "Supersedes [ADR-0001](0001-use-x.md).\n"
                                                          "\n"
                                                          "## Context\n"
                                                          "\n"
                                                          "X was slow.\n"
                                                          "\n"
                                                          "## Decision\n"
                                                          "\n"
                                                          "We use Y.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0003-cache.md"),
                  "# Cache reads\n"
                  "\n"
                  "## Status\n"
                  "\n"
                  "Proposed\n"
                  "\n"
                  "- Replaces ADR-2\n"
                  "\n"
                  "## Decision\n"
                  "\n"
                  "The cache replaces the reader described in [ADR-1](0001-use-x.md).\n");
    th_write_file(TH_PATH(tmp, "docs/adr/README.md"), "# Decisions\n\n## Status\n\nAccepted\n");
    th_write_file(TH_PATH(tmp, "docs/adr/template.md"),
                  "# Title\n\n## Status\n\n{proposed | accepted}\n\n## Decision\n\nTBD\n");
}

/* Properties of the ADR node of `file`; "" when there is none. */
static void md_adr_props(const char *db, const char *file, char *out, size_t cap) {
    out[0] = '\0';
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h,
                               "SELECT name || ' ' || properties FROM nodes WHERE label = 'ADR' "
                               "AND file_path = ?1",
                               -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, file, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
}

TEST(doc_links_md_adr) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_adr_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    md_write_fixture(tmp);
    md_write_adrs(tmp, "superseded");
    char db[512];
    snprintf(db, sizeof(db), "%s/adr.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    char props[2048];

    /* three records; README and template are none */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'ADR'"), 3);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes t ON t.id = e.target_id "
                           "WHERE e.type = 'DEFINES' AND t.label = 'ADR'"),
              3);
    /* facts: front matter */
    md_adr_props(db, "docs/adr/0001-use-x.md", props, sizeof(props));
    ASSERT_TRUE(strncmp(props, "ADR-1 ", strlen("ADR-1 ")) == 0);
    ASSERT_NOT_NULL(strstr(props, "\"status\":\"superseded\""));
    ASSERT_NOT_NULL(strstr(props, "\"date\":\"2024-03-05\""));
    ASSERT_NOT_NULL(strstr(props, "\"deciders\":\"Ana, Bo\""));
    ASSERT_NOT_NULL(strstr(props, "\"title\":\"Use X for storage\""));
    ASSERT_NOT_NULL(strstr(props, "\"superseded_by\":\"0002-use-y.md\""));
    ASSERT_NOT_NULL(strstr(props, "We use X")); /* the decision, searchable */
    /* facts: header field date, Status section */
    md_adr_props(db, "docs/adr/0002-use-y.md", props, sizeof(props));
    ASSERT_TRUE(strncmp(props, "ADR-2 ", strlen("ADR-2 ")) == 0);
    ASSERT_NOT_NULL(strstr(props, "\"status\":\"accepted\""));
    ASSERT_NOT_NULL(strstr(props, "\"date\":\"2024-05-12\""));
    md_adr_props(db, "docs/adr/README.md", props, sizeof(props));
    ASSERT_STR_EQ(props, "");

    /* SUPERSEDES: by link, and by id at a line head; never from prose that
     * uses "replaces" as a verb, never from the superseded record */
    const char *sup = "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                      "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'SUPERSEDES' "
                      "AND s.file_path = '%s' AND t.file_path = '%s'";
    char q[512];
    snprintf(q, sizeof(q), sup, "docs/adr/0002-use-y.md", "docs/adr/0001-use-x.md");
    ASSERT_EQ(dm_count(db, q), 1);
    snprintf(q, sizeof(q), sup, "docs/adr/0003-cache.md", "docs/adr/0002-use-y.md");
    ASSERT_EQ(dm_count(db, q), 1);
    snprintf(q, sizeof(q), sup, "docs/adr/0003-cache.md", "docs/adr/0001-use-x.md");
    ASSERT_EQ(dm_count(db, q), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges WHERE type = 'SUPERSEDES'"), 2);
    /* the record's sections still link to code */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND s.file_path = 'docs/adr/0001-use-x.md' "
                           "AND t.file_path = 'app/routing.py'"),
              1);

    /* incremental == full: a status edit, then a new record that a statement
     * names by id */
    char full_db[512];
    snprintf(full_db, sizeof(full_db), "%s/adr-full.db", tmp);
    md_write_adrs(tmp, "deprecated");
    ASSERT_EQ(dm_step(tmp, db, full_db, "ADR status edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    md_adr_props(db, "docs/adr/0001-use-x.md", props, sizeof(props));
    ASSERT_NOT_NULL(strstr(props, "\"status\":\"deprecated\""));
    th_write_file(TH_PATH(tmp, "docs/adr/0004-later.md"), "# Later\n\n## Status\n\nAccepted\n\n"
                                                          "Supersedes ADR-3.\n\n## Decision\n\n"
                                                          "Later.\n");
    ASSERT_EQ(dm_step(tmp, db, full_db, "ADR added", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    snprintf(q, sizeof(q), sup, "docs/adr/0004-later.md", "docs/adr/0003-cache.md");
    ASSERT_EQ(dm_count(db, q), 1);
    th_cleanup(tmp);
    PASS();
}

/* The production gate (doclink.h): families held back by the held-out audit
 * write rows, not edges; the families that passed still write edges. */
static int md_supersedes_from(const char *db, const char *file, const char *target);

TEST(doc_links_md_ship_gate) {
    for (size_t i = 0; i < sizeof(DM_HELD_FAMILIES) / sizeof(DM_HELD_FAMILIES[0]); i++) {
        ASSERT_FALSE(cbm_doclink_syntax_ships(DM_HELD_FAMILIES[i]));
    }
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_MD_LINK));
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_RST_ROLE));
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_ADOC_INCLUDE));
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_RST_LITERALINCLUDE));
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_RST_CODE_PATH));
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_MD_CODE_PATH));
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_MD_SUPERSEDES));
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_RST_OBJECT));
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmdgate_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    md_write_fixture(tmp);
    /* a file name alone, next to its file: it resolves, and its tier holds it */
    th_write_file(TH_PATH(tmp, "pkg/README.md"), "# Pkg\n\nSee `config.go`.\n");
    /* an ADR's statement ships as SUPERSEDES; the same words in prose are held */
    th_write_file(TH_PATH(tmp, "docs/adr/0001-base.md"),
                  "# Base\n\n## Status\n\nSuperseded\n\n## Decision\n\nWe use A.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0002-next.md"),
                  "# Next\n\n## Status\n\nAccepted\n\nSupersedes [ADR-0001](0001-base.md)\n\n"
                  "## Decision\n\nWe use B.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0003-last.md"),
                  "# Last\n\n## Status\n\nAccepted\n\n## Decision\n\n"
                  "This record also supersedes ADR-0001 for the cache.\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/md.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    char props[512];
    ASSERT_EQ(md_edge(db, "Guide", "config.go.__file__", props, sizeof(props)), 1);
    ASSERT_EQ(md_edge(db, "Paths", "APIRouter.add", props, sizeof(props)), 1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE "
                           "reason = 'below_bar_tier' AND syntax = 'code_path'"),
              0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE "
                           "reason = 'below_bar_tier' AND syntax = 'bare_path' "
                           "AND rel_path = 'pkg/README.md'"),
              1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS' AND "
                           "properties LIKE '%\"syntax\":\"bare_path\"%'"),
              0);
    ASSERT_EQ(md_supersedes_from(db, "0002-next.md", "0001-base.md"), 1);
    ASSERT_EQ(md_supersedes_from(db, "0003-last.md", "%"), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE "
                           "reason = 'below_bar_tier' AND syntax = 'supersedes_prose' "
                           "AND rel_path = 'docs/adr/0003-last.md'"),
              1);
    th_cleanup(tmp);
    PASS();
}

/* A held-out finding: a spec template's combined field `Supersedes / Depends
 * on: [Spec 2](...)` states no supersession (the record depended on the one it
 * named); the plain statement next to it still does. */
TEST(doc_links_md_adr_label_choice) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dladrl_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    th_write_file(TH_PATH(tmp, "docs/adr/0001-base.md"),
                  "# Base\n\n## Status\n\nAccepted\n\n## Decision\n\nWe use A.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0002-next.md"),
                  "# Next\n\n## Status\n\nAccepted\n\n"
                  "Supersedes / Depends on: [ADR-0001](0001-base.md)\n\n"
                  "## Decision\n\nWe add B.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0003-last.md"), "# Last\n\n## Status\n\nAccepted\n\n"
                                                         "Supersedes [ADR-0001](0001-base.md).\n\n"
                                                         "## Decision\n\nWe use C.\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/adr.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "WHERE e.type = 'SUPERSEDES' AND s.file_path = 'docs/adr/0002-next.md'"),
              0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "WHERE e.type = 'SUPERSEDES' AND s.file_path = 'docs/adr/0003-last.md'"),
              1);
    th_cleanup(tmp);
    PASS();
}

static int md_supersedes_from(const char *db, const char *file, const char *target) {
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
             "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'SUPERSEDES' "
             "AND s.file_path = 'docs/adr/%s' AND t.file_path LIKE 'docs/adr/%s'",
             file, target);
    return dm_count(db, sql);
}

static int md_supersedes_syntax(const char *db, const char *file, const char *syntax) {
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
             "WHERE e.type = 'SUPERSEDES' AND s.file_path = 'docs/adr/%s' "
             "AND e.properties LIKE '%%\"syntax\":\"%s\"%%'",
             file, syntax);
    return dm_count(db, sql);
}

/* Held-out findings: a field whose value is "none" goes on to name records it
 * extends; a record reports another record's supersession (`ADR 0002
 * supersedes ...`, a table cell `0002 (supersedes ...)`); the record after a
 * statement's full stop is no target. The record's own number as the subject
 * is its own statement. */
TEST(doc_links_md_adr_reported) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dladrr_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    th_write_file(TH_PATH(tmp, "docs/adr/0001-base.md"),
                  "# Base\n\n## Status\n\nAccepted\n\n## Decision\n\nWe use A.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0002-next.md"),
                  "# Next\n\n## Status\n\nAccepted\n\n## Decision\n\nWe use B.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0003-none.md"),
                  "# None\n\n- Status: Accepted\n"
                  "- Supersedes: none (it extends [ADR 0001](0001-base.md))\n\n"
                  "## Decision\n\nWe add C.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0004-na.md"),
                  "# NA\n\n- Status: Accepted\n- Supersedes: **N/A** -- refines ADR-0002\n\n"
                  "## Decision\n\nWe add D.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0005-report.md"),
                  "# Report\n\n## Status\n\nAccepted\n\n## Context\n\n"
                  "ADR 0002 supersedes ADR 0001's first decision.\n\n"
                  "| Row | Owner |\n|---|---|\n| retries | 0002 (supersedes ADR 0001's rule) |\n\n"
                  "[ADR 0002](0002-next.md) supersedes ADR 0001 too.\n\n"
                  "## Decision\n\nWe audit.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0006-own.md"),
                  "# Own\n\n## Status\n\nAccepted\n\n## Decision\n\n"
                  "ADR 0006 supersedes ADR 0001.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0007-stop.md"),
                  "# Stop\n\n## Status\n\nAccepted\n\n## Decision\n\n"
                  "> This record supersedes ADR-0001. ADR 0002 is an orthogonal decision.\n");
    /* adr-tools' own form: the link text holds the record's number and title */
    th_write_file(TH_PATH(tmp, "docs/adr/0008-tools.md"),
                  "# 8. Tools\n\n## Status\n\nAccepted\n\n"
                  "Supersedes [2. Next](0002-next.md)\n\n## Decision\n\nWe use E.\n");
    /* statements in a field table and a bold field */
    th_write_file(TH_PATH(tmp, "docs/adr/0009-table.md"),
                  "# Table\n\n| Field | Value |\n|---|---|\n| Status | Accepted |\n"
                  "| Supersedes | [ADR 0001](0001-base.md) |\n\n## Decision\n\nWe use F.\n");
    th_write_file(TH_PATH(tmp, "docs/adr/0010-field.md"),
                  "# Field\n\n- Status: Accepted\n- **Supersedes:** ADR-0002\n\n"
                  "## Decision\n\nWe use G.\n");
    /* a paragraph wrapped before the phrase: its subject is the line before */
    th_write_file(
        TH_PATH(tmp, "docs/adr/0011-wrap.md"),
        "# Wrap\n\n## Status\n\nAccepted\n\n## Context\n\nThe sealed store of\n"
        "[ADR 0002](0002-next.md)\nsupersedes ADR-0001 and leaves this record standing.\n\n"
        "## Decision\n\nWe use I.\n");
    /* field lines one under the other: each its own statement */
    th_write_file(TH_PATH(tmp, "docs/adr/0012-fields.md"),
                  "# Fields\n\nStatus: Accepted\nSupersedes [ADR-0001](0001-base.md)\n\n"
                  "## Decision\n\nWe use J.\n");
    /* a `Supersedes:` field right under running text is still a field */
    th_write_file(TH_PATH(tmp, "docs/adr/0013-colon.md"),
                  "# Colon\n\n## Status\n\nAccepted after a long review of the store\n"
                  "Supersedes: [ADR 0002](0002-next.md)\n\n## Decision\n\nWe use K.\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/adr.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    ASSERT_EQ(md_supersedes_from(db, "0003-none.md", "%"), 0);
    ASSERT_EQ(md_supersedes_from(db, "0004-na.md", "%"), 0);
    ASSERT_EQ(md_supersedes_from(db, "0005-report.md", "%"), 0);
    ASSERT_EQ(md_supersedes_from(db, "0006-own.md", "0001-base.md"), 1);
    ASSERT_EQ(md_supersedes_from(db, "0007-stop.md", "0001-base.md"), 1);
    ASSERT_EQ(md_supersedes_from(db, "0007-stop.md", "0002-next.md"), 0);
    ASSERT_EQ(md_supersedes_from(db, "0008-tools.md", "0002-next.md"), 1);
    ASSERT_EQ(md_supersedes_from(db, "0009-table.md", "0001-base.md"), 1);
    ASSERT_EQ(md_supersedes_from(db, "0010-field.md", "0002-next.md"), 1);
    /* a statement opens its line or field and names a record directly; the
     * same words in running prose are the held family supersedes_prose */
    ASSERT_EQ(md_supersedes_syntax(db, "0006-own.md", "supersedes_prose"), 1);
    ASSERT_EQ(md_supersedes_syntax(db, "0007-stop.md", "supersedes_prose"), 1);
    ASSERT_EQ(md_supersedes_syntax(db, "0008-tools.md", "supersedes"), 1);
    ASSERT_EQ(md_supersedes_syntax(db, "0009-table.md", "supersedes"), 1);
    ASSERT_EQ(md_supersedes_syntax(db, "0010-field.md", "supersedes"), 1);
    ASSERT_EQ(md_supersedes_syntax(db, "0011-wrap.md", "supersedes"), 0);
    ASSERT_EQ(md_supersedes_syntax(db, "0011-wrap.md", "supersedes_prose"), 1);
    ASSERT_EQ(md_supersedes_syntax(db, "0012-fields.md", "supersedes"), 1);
    ASSERT_EQ(md_supersedes_syntax(db, "0013-colon.md", "supersedes"), 1);
    th_cleanup(tmp);
    PASS();
}

/* Held-out findings (AsciiDoc code names, which resolve like Markdown's): a
 * `std::` name is the standard library's, not a specialization or header of
 * this repository; a Java name written from its root package is absolute, not
 * a relocated copy's tail; a type is named in its own case (`debug.log` is a
 * log file, not Debug.log); a definition of another copy of the document's
 * project (snapshots side by side) never answers the document. Each with a
 * control that keeps its link. */
TEST(doc_links_md_name_scope_rules) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmdscope_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    /* a compatibility header named like the standard one (Boost.Typeof's) */
    th_write_file(TH_PATH(tmp, "lib/include/mylib/std/complex.hpp"),
                  "#include <complex>\nnamespace mylib { using cplx = int; }\n");
    th_write_file(TH_PATH(tmp, "lib/include/mylib/widget.hpp"),
                  "namespace mylib {\nclass Widget {\n  public:\n    int size() const;\n};\n}\n");
    th_write_file(
        TH_PATH(tmp, "shaded/src/main/java/org/acme/shaded/com/google/protobuf/Message.java"),
        "package org.acme.shaded.com.google.protobuf;\npublic class Message {}\n");
    th_write_file(TH_PATH(tmp, "core/src/main/java/com/acme/Widget.java"),
                  "package com.acme;\npublic class Widget {}\n");
    th_write_file(TH_PATH(tmp, "core/src/main/java/com/acme/Ref.java"),
                  "package com.acme;\npublic class Ref {\n    public static class Debug {\n"
                  "        public static void log(String s) {}\n    }\n}\n");
    th_write_file(
        TH_PATH(tmp, "v1/src/main/java/org/acme/Store.java"),
        "package org.acme;\npublic class Store {\n    public static class Reader {}\n}\n");
    th_write_file(TH_PATH(tmp, "v2/src/main/java/org/acme/Store.java"),
                  "package org.acme;\npublic interface Store {}\n");
    th_write_file(
        TH_PATH(tmp, "docs/guide.md"),
        "# Keys\n\nKeys use `std::complex`, and `mylib::Widget` holds them.\n\n"
        "# Wire\n\nProtobuf's `com.google.protobuf.Message`; ours is `com.acme.Widget`.\n\n"
        "# Logs\n\nRead `debug.log`.\n\n"
        "# Debug\n\nCall `Debug.log`.\n");
    th_write_file(TH_PATH(tmp, "v2/docs/design.md"), "# Design\n\nThe `Store.Reader` reads.\n");
    th_write_file(TH_PATH(tmp, "v1/docs/notes.md"), "# Notes\n\nThe `Store.Reader` reads.\n");
    /* a configuration key that spells a Java package's tail; a Python package */
    th_write_file(TH_PATH(tmp, "core/src/main/java/io/acme/snapshot/mode/SnapshotMode.java"),
                  "package io.acme.snapshot.mode;\npublic enum SnapshotMode { INITIAL }\n");
    th_write_file(TH_PATH(tmp, "pyapp/__init__.py"), "");
    th_write_file(TH_PATH(tmp, "pyapp/sub/__init__.py"), "from .core import run\n");
    th_write_file(TH_PATH(tmp, "pyapp/sub/core.py"), "def run():\n    return 1\n");
    th_write_file(TH_PATH(tmp, "docs/config.md"),
                  "# Config\n\nSet `snapshot.mode` to `initial`.\n\n"
                  "# Py\n\nThe `pyapp.sub` package runs it.\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/md.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    char props[512];
    ASSERT_EQ(md_edge(db, "guide.Keys", "complex.hpp.__file__", props, sizeof(props)), 0);
    ASSERT_EQ(md_edge(db, "guide.Keys", "mylib.Widget", props, sizeof(props)), 1);
    ASSERT_EQ(md_edge(db, "guide.Wire", "protobuf.Message", props, sizeof(props)), 0);
    ASSERT_EQ(md_edge(db, "guide.Wire", "com.acme.Widget", props, sizeof(props)), 1);
    ASSERT_EQ(md_edge(db, "guide.Logs", "Debug.log", props, sizeof(props)), 0);
    ASSERT_EQ(md_edge(db, "guide.Debug", "Debug.log", props, sizeof(props)), 1);
    ASSERT_EQ(md_edge(db, "design.Design", "Store.Reader", props, sizeof(props)), 0);
    ASSERT_EQ(md_edge(db, "notes.Notes", "Store.Reader", props, sizeof(props)), 1);
    ASSERT_EQ(md_edge(db, "config.Config", "snapshot.mode", props, sizeof(props)), 0);
    ASSERT_EQ(md_edge(db, "config.Py", "sub.__init__.py.__file__", props, sizeof(props)), 1);
    th_cleanup(tmp);
    PASS();
}

SUITE(doc_links_md) {
    dm_ship_held_families();
    RUN_TEST(doc_links_md_extract_tokens);
    RUN_TEST(doc_links_md_repeated_headings);
    RUN_TEST(doc_links_md_classify_span);
    RUN_TEST(doc_links_md_resolve);
    RUN_TEST(doc_links_md_incremental);
    RUN_TEST(doc_links_md_adr);
    RUN_TEST(doc_links_md_adr_label_choice);
    RUN_TEST(doc_links_md_adr_reported);
    RUN_TEST(doc_links_md_name_scope_rules);
    cbm_doclink_test_reset_ships();
    RUN_TEST(doc_links_md_ship_gate);
}
