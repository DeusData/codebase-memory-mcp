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
    int fixes = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].name, "Fixes") == 0) {
            ASSERT_STR_EQ(r->defs.items[i].qualified_name, "p.CHANGES.Fixes");
            fixes++;
        }
    }
    ASSERT_EQ(fixes, 2);
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

/* Unchanged bare names have priority even when the bare title repeats. */
TEST(doc_links_md_repeated_bare_name_priority) {
    const char *sources[] = {
        "# A.H\n# A.H\n# A\n## H\n# B\n## H\n",
        "# A\n## H\n# B\n## H\n# A.H\n# A.H\n",
    };
    for (size_t k = 0; k < sizeof(sources) / sizeof(sources[0]); k++) {
        CBMFileResult *r = dm_extract(sources[k], CBM_LANG_MARKDOWN, "names.md");
        ASSERT_NOT_NULL(r);
        int bare = 0;
        int qualified = 0;
        for (int i = 0; i < r->defs.count; i++) {
            const CBMDefinition *d = &r->defs.items[i];
            if (strcmp(d->name, "A.H") == 0) {
                ASSERT_STR_EQ(d->qualified_name, "p.names.A.H");
                bare++;
            } else if (strcmp(d->name, "H") == 0 &&
                       strcmp(d->qualified_name, "p.names.A.H~2") == 0) {
                qualified++;
            }
        }
        ASSERT_EQ(bare, 2);
        ASSERT_EQ(qualified, 1);
        cbm_free_result(r);
    }
    PASS();
}

TEST(doc_links_md_heading_suffix_reservations) {
    CBMFileResult *r = dm_extract("# A.H\n# A.H~2\n# A.H~3\n"
                                  "# A\n## H\n# B\n## H\n",
                                  CBM_LANG_MARKDOWN, "names.md");
    ASSERT_NOT_NULL(r);
    int moved = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->name, "A.H~2") == 0)
            ASSERT_STR_EQ(d->qualified_name, "p.names.A.H~2");
        if (strcmp(d->name, "A.H~3") == 0)
            ASSERT_STR_EQ(d->qualified_name, "p.names.A.H~3");
        if (strcmp(d->qualified_name, "p.names.A.H~4") == 0)
            moved++;
    }
    ASSERT_EQ(moved, 1);
    cbm_free_result(r);
    PASS();
}

/* A suffix must also leave room for a parent-qualified name in another run. */
TEST(doc_links_md_generated_name_reservations) {
    const char *src = "# A.H\n# A\n## H\n## H~2\n# B\n## H\n## H~2\n";
    CBMFileResult *r = dm_extract(src, CBM_LANG_MARKDOWN, "names.md");
    ASSERT_NOT_NULL(r);
    int moved = 0;
    int reserved = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->qualified_name, "p.names.A.H~3") == 0) {
            ASSERT_STR_EQ(d->name, "H");
            moved++;
        }
        if (strcmp(d->qualified_name, "p.names.A.H~2") == 0) {
            ASSERT_STR_EQ(d->name, "H~2");
            reserved++;
        }
    }
    ASSERT_EQ(moved, 1);
    ASSERT_EQ(reserved, 1);
    cbm_free_result(r);
    PASS();
}

TEST(doc_links_md_distinct_chains_same_dotted_name) {
    CBMFileResult *r =
        dm_extract("# A.B\n## C\n# A\n## B\n### C\n### C\n", CBM_LANG_MARKDOWN, "names.md");
    ASSERT_NOT_NULL(r);
    int kept = 0;
    int moved = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->name, "C") != 0)
            continue;
        if (d->start_line == 2) {
            ASSERT_STR_EQ(d->qualified_name, "p.names.A.B.C");
            kept++;
        } else {
            ASSERT_STR_EQ(d->qualified_name, "p.names.A.B.C~2");
            moved++;
        }
    }
    ASSERT_EQ(kept, 1);
    ASSERT_EQ(moved, 2);
    cbm_free_result(r);
    PASS();
}

TEST(doc_links_md_setext_parent_names) {
    CBMFileResult *r =
        dm_extract("A\n===\nH\n---\nB\n===\nH\n---\n", CBM_LANG_MARKDOWN, "names.md");
    ASSERT_NOT_NULL(r);
    int children = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->name, "H") == 0) {
            ASSERT_STR_EQ(d->qualified_name, d->start_line == 3 ? "p.names.A.H" : "p.names.B.H");
            children++;
        }
    }
    ASSERT_EQ(children, 2);
    cbm_free_result(r);
    PASS();
}

/* Paragraph continuation markers are not part of a Setext heading title. */
TEST(doc_links_md_quoted_setext_heading_text) {
    const char *sources[] = {"First\nSecond\n---\n", "> First\n> Second\n> ---\n", "> H\n> ---\n"};
    for (size_t k = 0; k < sizeof(sources) / sizeof(sources[0]); k++) {
        CBMFileResult *r = dm_extract(sources[k], CBM_LANG_MARKDOWN, "names.md");
        ASSERT_NOT_NULL(r);
        int headings = 0;
        for (int i = 0; i < r->defs.count; i++) {
            const CBMDefinition *d = &r->defs.items[i];
            if (strcmp(d->label, "Section") != 0)
                continue;
            /* Display names use the first line; the QN must retain every line. */
            ASSERT_STR_EQ(d->name, k == 2 ? "H" : "First");
            ASSERT_STR_EQ(d->qualified_name, k == 2 ? "p.names.H" : "p.names.First-Second");
            headings++;
        }
        ASSERT_EQ(headings, 1);
        cbm_free_result(r);
    }
    PASS();
}

/* Setext and ATX markers share hierarchy; block containers retain their own
 * stacks even when a quote is visited between two outer headings. */
TEST(doc_links_md_mixed_heading_container_scope) {
    const char *src = "# Outside\n\nH\n---\n\n"
                      "> # Quote\n>\n> H\n> ---\n\n"
                      "H\n---\n\n# Later\n\nH\n---\n";
    CBMFileResult *r = dm_extract(src, CBM_LANG_MARKDOWN, "names.md");
    ASSERT_NOT_NULL(r);
    int outside = 0;
    int quote = 0;
    int later = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->name, "H") != 0)
            continue;
        if (strcmp(d->qualified_name, "p.names.Outside.H") == 0)
            outside++;
        if (strcmp(d->qualified_name, "p.names.Quote.H") == 0)
            quote++;
        if (strcmp(d->qualified_name, "p.names.Later.H") == 0)
            later++;
    }
    ASSERT_EQ(outside, 2);
    ASSERT_EQ(quote, 1);
    ASSERT_EQ(later, 1);
    cbm_free_result(r);
    PASS();
}

/* An unnamed H1 still ends A's scope; it must not create an empty Section. */
TEST(doc_links_md_empty_heading_resets_parent) {
    CBMFileResult *r = dm_extract("# A\n## H\n#\n## H\n", CBM_LANG_MARKDOWN, "names.md");
    ASSERT_NOT_NULL(r);
    int children = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (strcmp(d->label, "Section") == 0) {
            ASSERT_TRUE(d->name && d->name[0]);
        }
        if (strcmp(d->name, "H") == 0) {
            ASSERT_STR_EQ(d->qualified_name, d->start_line == 2 ? "p.names.A.H" : "p.names.H");
            children++;
        }
    }
    ASSERT_EQ(children, 2);
    cbm_free_result(r);
    PASS();
}

/* This measures the collector's actual cursor moves/node visits, excluding
 * hidden parser work. The former ts_node_parent root scans are established by
 * runtime-source review, not retrospectively measured by this counter. */
static uint64_t md_flat_collection_work(int n) {
    size_t cap = (size_t)n * 32 + 1;
    char *src = malloc(cap);
    if (!src)
        return 0;
    size_t used = 0;
    for (int i = 0; i < n; i++) {
        used += (size_t)snprintf(src + used, cap - used, "# H%d\n", i);
    }
    CBMFileResult *r = dm_extract(src, CBM_LANG_MARKDOWN, "names.md");
    free(src);
    uint64_t steps = r ? r->markdown_collection_steps : 0;
    cbm_free_result(r);
    return steps;
}

TEST(doc_links_md_flat_collection_work) {
    uint64_t small = md_flat_collection_work(128);
    uint64_t large = md_flat_collection_work(256);
    ASSERT_TRUE(small > 0);
    ASSERT_TRUE(large < small * 3);
    PASS();
}

/* Count name lookup/group checks, never elapsed time. Independent collision groups must
 * not search every heading; a single group must not rescan assigned chains
 * or restart its suffix search at ~2 for each new chain. */
static uint64_t md_collision_work(int n, bool one_base) {
    size_t cap = (size_t)n * 256 + 1;
    char *src = malloc(cap);
    if (!src)
        return 0;
    size_t used = 0;
    for (int k = 0; k < n; k++) {
        if (!one_base) {
            used += (size_t)snprintf(src + used, cap - used,
                                     "# P%d\n## H%d\n# Q%d\n## H%d\n# P%d.H%d\n", k, k, k, k, k, k);
        } else {
            /* 276 distinct partitions of 25 dotted components into three
             * parent segments, all with the same final dotted name. */
            int ordinal = 0;
            int first = 1;
            int second = 2;
            for (int i = 1; i < 24; i++) {
                for (int j = i + 1; j < 25; j++) {
                    if (ordinal++ == k) {
                        first = i;
                        second = j;
                    }
                }
            }
            used += (size_t)snprintf(src + used, cap - used, "# a");
            for (int i = 1; i < 25; i++) {
                used += (size_t)snprintf(src + used, cap - used,
                                         i == first    ? "\n## a"
                                         : i == second ? "\n### a"
                                                       : ".a");
            }
            used += (size_t)snprintf(src + used, cap - used, "\n#### H\n");
        }
    }
    CBMFileResult *r = dm_extract(src, CBM_LANG_MARKDOWN, "names.md");
    free(src);
    uint64_t work = r ? r->markdown_name_checks : 0;
    cbm_free_result(r);
    return work;
}

TEST(doc_links_md_independent_collision_work) {
    uint64_t small = md_collision_work(128, false);
    uint64_t large = md_collision_work(256, false);
    ASSERT_TRUE(small > 0);
    ASSERT_TRUE(large < small * 3);
    PASS();
}

TEST(doc_links_md_shared_collision_work) {
    uint64_t small = md_collision_work(128, true);
    uint64_t large = md_collision_work(256, true);
    ASSERT_TRUE(small > 0);
    ASSERT_TRUE(large < small * 3);
    PASS();
}

/* MENTIONS edges from the Section whose qualified name ends with `src` to the
 * node whose qualified name ends with `tgt`. */
static int rep_edge(const char *db, const char *src, const char *tgt) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    return n;
}

/* #2586: two `## Implementation` headings under different parents had one
 * qualified name, so the graph kept one Section (the last) and the references
 * under both went to the File. A repeated heading is now qualified by the
 * headings above it; a unique heading keeps its name alone. */
TEST(doc_links_md_repeated_headings_by_parent_issue2586) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_rep_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    th_write_file(TH_PATH(tmp, "docs/repeated.md"), "# Service A\n"
                                                    "\n"
                                                    "## Implementation\n"
                                                    "\n"
                                                    "Uses `src/ledger.py::balance`.\n"
                                                    "\n"
                                                    "# Service B\n"
                                                    "\n"
                                                    "## Implementation\n"
                                                    "\n"
                                                    "Uses `src/ledger.py::post_entry`.\n"
                                                    "\n"
                                                    "```mermaid\n"
                                                    "graph LR\n"
                                                    "  Ledger --> Balance\n"
                                                    "```\n"
                                                    "\n"
                                                    "## Notes\n"
                                                    "\n"
                                                    "See `src/ledger.py`.\n");
    th_write_file(TH_PATH(tmp, "src/ledger.py"), "def post_entry(ledger, amount):\n"
                                                 "    ledger.append(amount)\n"
                                                 "    return balance(ledger)\n"
                                                 "\n"
                                                 "\n"
                                                 "def balance(ledger):\n"
                                                 "    return sum(ledger)\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/md.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);

    /* two Sections, each qualified by its parent heading */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Section' "
                           "AND name = 'Implementation' AND file_path = 'docs/repeated.md'"),
              2);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Section' "
                           "AND qualified_name LIKE '%.repeated.Service-A.Implementation' "
                           "AND start_line = 3"),
              1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Section' "
                           "AND qualified_name LIKE '%.repeated.Service-B.Implementation' "
                           "AND start_line = 9"),
              1);
    /* unique headings keep their names */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Section' AND "
                           "(qualified_name LIKE '%.repeated.Service-A' OR "
                           "qualified_name LIKE '%.repeated.Notes')"),
              2);

    /* each reference from its own Section, none from the File */
    ASSERT_EQ(rep_edge(db, "Service-A.Implementation", "ledger.balance"), 1);
    ASSERT_EQ(rep_edge(db, "Service-B.Implementation", "ledger.post_entry"), 1);
    ASSERT_EQ(rep_edge(db, "Service-A.Implementation", "ledger.post_entry"), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "WHERE e.type = 'MENTIONS' AND s.label = 'File' "
                           "AND s.file_path = 'docs/repeated.md'"),
              0);

    /* the diagram under the second heading is that Section's */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'DEFINES' "
                           "AND t.label = 'Diagram' "
                           "AND s.qualified_name LIKE '%.Service-B.Implementation'"),
              1);

    th_cleanup(tmp);
    PASS();
}

/* #2586 review: a heading text may hold dots, so `# Service-A.Implementation`
 * named the same as the qualified `## Implementation` under `# Service A`.
 * The unique heading keeps its name; the qualified one takes `~2`. Five
 * headings, five Sections, every reference from its own. */
TEST(doc_links_md_repeated_heading_name_collision_issue2586) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_col_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    th_write_file(TH_PATH(tmp, "docs/repeated.md"), "# Service A\n"
                                                    "\n"
                                                    "## Implementation\n"
                                                    "\n"
                                                    "Uses `src/ledger.py::balance`.\n"
                                                    "\n"
                                                    "# Service B\n"
                                                    "\n"
                                                    "## Implementation\n"
                                                    "\n"
                                                    "Uses `src/ledger.py::post_entry`.\n"
                                                    "\n"
                                                    "# Service-A.Implementation\n"
                                                    "\n"
                                                    "See `src/ledger.py`.\n");
    th_write_file(TH_PATH(tmp, "src/ledger.py"), "def post_entry(ledger, amount):\n"
                                                 "    ledger.append(amount)\n"
                                                 "    return balance(ledger)\n"
                                                 "\n"
                                                 "\n"
                                                 "def balance(ledger):\n"
                                                 "    return sum(ledger)\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/md.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);

    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Section' "
                           "AND file_path = 'docs/repeated.md'"),
              5);
    /* the unique heading keeps its name; the qualified one moves aside */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Section' "
                           "AND qualified_name LIKE '%.repeated.Service-A.Implementation' "
                           "AND start_line = 13"),
              1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Section' "
                           "AND qualified_name LIKE '%.repeated.Service-A.Implementation~2' "
                           "AND start_line = 3"),
              1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Section' "
                           "AND qualified_name LIKE '%.repeated.Service-B.Implementation' "
                           "AND start_line = 9"),
              1);

    /* each reference from its own Section, none from the File */
    ASSERT_EQ(rep_edge(db, "Service-A.Implementation~2", "ledger.balance"), 1);
    ASSERT_EQ(rep_edge(db, "Service-B.Implementation", "ledger.post_entry"), 1);
    ASSERT_EQ(rep_edge(db, "repeated.Service-A.Implementation", "ledger.py.__file__"), 1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "WHERE e.type = 'MENTIONS' AND s.label = 'File' "
                           "AND s.file_path = 'docs/repeated.md'"),
              0);

    th_cleanup(tmp);
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

/* #2587: a `path::member` reference whose member the code file does not
 * define was an exact edge to the file, so a renamed or deleted definition
 * looked like a valid, coarser link. It is a missing reference; a member the
 * file defines more than once, with no owner to pick one, is ambiguous. */
TEST(doc_links_md_member_not_defined_issue2587) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    th_write_file(TH_PATH(tmp, "docs/members.md"),
                  "# Members\n"
                  "\n"
                  "See `src/util/rounding.py::clamp` and "
                  "`src/util/rounding.py::no_such_function`.\n"
                  "Areas: `src/shapes.py::Square::area`, `src/shapes.py::area`.\n"
                  "Notes: `docs/other.md::Intro`.\n");
    th_write_file(TH_PATH(tmp, "docs/other.md"), "# Intro\n\nText.\n");
    th_write_file(TH_PATH(tmp, "src/util/rounding.py"), "def clamp(x, lo, hi):\n"
                                                        "    return max(lo, min(hi, x))\n");
    th_write_file(TH_PATH(tmp, "src/shapes.py"), "class Circle:\n"
                                                 "    def area(self):\n"
                                                 "        return 3\n"
                                                 "\n"
                                                 "\n"
                                                 "class Square:\n"
                                                 "    def area(self):\n"
                                                 "        return 4\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/md.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    char props[512];
    char reason[64];

    /* a member the file defines, alone or through its owner */
    ASSERT_EQ(md_edge(db, "Members", "rounding.clamp", props, sizeof(props)), 1);
    ASSERT_EQ(md_edge(db, "Members", "Square.area", props, sizeof(props)), 1);
    ASSERT_EQ(md_edge(db, "Members", "Circle.area", props, sizeof(props)), 0);

    /* a member the file does not define: no edge to the file, a missing row */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND s.qualified_name LIKE '%.Members' AND t.label = 'File' "
                           "AND t.file_path = 'src/util/rounding.py'"),
              0);
    dm_row(db, "docs/members.md", "src/util/rounding.py::no_such_function", reason, sizeof(reason),
           NULL, 0);
    ASSERT_STR_EQ(reason, "missing");

    /* defined twice and no owner written: ambiguous, not the file */
    dm_row(db, "docs/members.md", "src/shapes.py::area", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "ambiguous");
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND s.qualified_name LIKE '%.Members' AND t.label = 'File' "
                           "AND t.file_path = 'src/shapes.py'"),
              0);

    /* a document's headings are no definitions: the file edge stays */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                           "AND s.qualified_name LIKE '%.Members' AND t.label = 'File' "
                           "AND t.file_path = 'docs/other.md'"),
              1);

    th_cleanup(tmp);
    PASS();
}

/* MENTIONS edges from the section whose qualified name ends with `section` to
 * the File node of `file`. */
static int md_file_edges(const char *db, const char *section, const char *file) {
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
             "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
             "AND s.qualified_name LIKE '%%.%s' AND t.label = 'File' AND t.file_path = '%s'",
             section, file);
    return dm_count(db, sql);
}

/* #2587, the members a code file has although the graph defines no node for
 * them: a call form, a macro, a prototype, a namespace, an enum member, a
 * class attribute. Each is there -- the file writes it -- so it keeps its
 * edge; only a name the file does not write at all is missing. */
static const struct {
    const char *section;
    const char *raw;
    const char *file;
    bool missing;
} MD_WRITTEN[] = {
    {"CallForm", "src/util.py::clamp()", NULL, false},
    {"Macro", "include/limits.h::MAX_ITEMS", "include/limits.h", false},
    {"FunctionMacro", "include/limits.h::SQUARE", "include/limits.h", false},
    {"Prototype", "include/limits.h::limits_init", "include/limits.h", false},
    {"Namespace", "src/ns.ts::Shapes", "src/ns.ts", false},
    {"EnumMember", "src/color.py::Color::RED", "src/color.py", false},
    {"Attribute", "src/model.py::Model::name", "src/model.py", false},
    /* not written: `blue` and LIGHT_BLUE are no BLUE */
    {"NotWritten", "src/color.py::Color::BLUE", "src/color.py", true},
    {"NotDefined", "src/util.py::no_such_function", "src/util.py", true},
};

TEST(doc_links_md_member_written_issue2587) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char doc[2048] = "";
    size_t used = 0;
    for (size_t i = 0; i < sizeof(MD_WRITTEN) / sizeof(MD_WRITTEN[0]); i++) {
        used += (size_t)snprintf(doc + used, sizeof(doc) - used, "# %s\n\nSee `%s`.\n\n",
                                 MD_WRITTEN[i].section, MD_WRITTEN[i].raw);
        ASSERT(used < sizeof(doc));
    }
    th_write_file(TH_PATH(tmp, "docs/written.md"), doc);
    th_write_file(TH_PATH(tmp, "src/util.py"), "def clamp(x, lo, hi):\n"
                                               "    return max(lo, min(hi, x))\n");
    th_write_file(TH_PATH(tmp, "include/limits.h"), "#ifndef LIMITS_H\n"
                                                    "#define LIMITS_H\n"
                                                    "#define MAX_ITEMS 64\n"
                                                    "#define SQUARE(x) ((x) * (x))\n"
                                                    "int limits_init(void);\n"
                                                    "#endif\n");
    th_write_file(TH_PATH(tmp, "src/ns.ts"), "export namespace Shapes {\n"
                                             "    export const unit = 1;\n"
                                             "}\n");
    th_write_file(TH_PATH(tmp, "src/color.py"), "from enum import Enum\n"
                                                "\n"
                                                "\n"
                                                "class Color(Enum):\n"
                                                "    RED = 1\n"
                                                "    LIGHT_BLUE = 2  # blue\n");
    th_write_file(TH_PATH(tmp, "src/model.py"), "class Model:\n"
                                                "    name: str\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/md.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    char props[512];
    char reason[64];

    /* every reference, under its own heading: an edge, or a missing row */
    int wrong = 0;
    for (size_t i = 0; i < sizeof(MD_WRITTEN) / sizeof(MD_WRITTEN[0]); i++) {
        dm_row(db, "docs/written.md", MD_WRITTEN[i].raw, reason, sizeof(reason), NULL, 0);
        int edges = MD_WRITTEN[i].file
                        ? md_file_edges(db, MD_WRITTEN[i].section, MD_WRITTEN[i].file)
                        : dm_mentions_from(db, MD_WRITTEN[i].section);
        bool ok = MD_WRITTEN[i].missing ? strcmp(reason, "missing") == 0 && edges == 0
                                        : reason[0] == '\0' && edges >= 1;
        if (!ok) {
            printf("  %s: row '%s', %d edge(s)\n", MD_WRITTEN[i].raw, reason, edges);
            wrong++;
        }
    }
    ASSERT_EQ(wrong, 0);
    /* the call form binds the function itself */
    ASSERT_EQ(md_edge(db, "CallForm", "util.clamp", props, sizeof(props)), 1);

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

/* #2587 incremental == full: whether a `path::member` row is missing depends
 * on the member file's text, and the row leads to that file by no edge. A
 * body edit that writes the name (no definition added) turns the row into a
 * file edge, one that drops it turns the edge back into the row; a same-named
 * group that shrinks to one turns an ambiguous row into an edge. The group is
 * Ruby: a Python file whose surface changes takes the full route (the Python
 * namespace admission), and a language with a doc-link scope (Rust, Go, ...)
 * reports the lost method as a removed name, so neither reaches the rule for
 * ambiguous member rows. */
TEST(doc_links_md_member_missing_incremental) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    th_write_file(TH_PATH(repo, "docs/helpers.md"), "# Helpers\n\nSee `src/tools.py::helper2`.\n");
    th_write_file(TH_PATH(repo, "docs/areas.md"), "# Areas\n\nSee `src/shapes.rb::area`.\n");
    th_write_file(TH_PATH(repo, "src/tools.py"), "def helper():\n"
                                                 "    return 1\n");
    th_write_file(TH_PATH(repo, "src/shapes.rb"), "class Circle\n"
                                                  "  def area\n"
                                                  "    3\n"
                                                  "  end\n"
                                                  "end\n"
                                                  "\n"
                                                  "class Square\n"
                                                  "  def area\n"
                                                  "    4\n"
                                                  "  end\n"
                                                  "end\n");
    char db[512];
    char full_db[512];
    snprintf(db, sizeof(db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    char reason[64];
    dm_row(db, "docs/helpers.md", "src/tools.py::helper2", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    dm_row(db, "docs/areas.md", "src/shapes.rb::area", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "ambiguous");

    /* the body writes the name, no definition added: the row becomes an edge */
    th_write_file(TH_PATH(repo, "src/tools.py"), "def helper():\n"
                                                 "    return 1  # helper2 follows\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "member written", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    ASSERT_EQ(md_file_edges(db, "Helpers", "src/tools.py"), 1);
    /* and drops it again: missing again */
    th_write_file(TH_PATH(repo, "src/tools.py"), "def helper():\n"
                                                 "    return 1\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "member dropped", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    dm_row(db, "docs/helpers.md", "src/tools.py::helper2", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    /* one of two `area` methods goes, the name stays: the one left binds */
    th_write_file(TH_PATH(repo, "src/shapes.rb"), "class Circle\n"
                                                  "end\n"
                                                  "\n"
                                                  "class Square\n"
                                                  "  def area\n"
                                                  "    4\n"
                                                  "  end\n"
                                                  "end\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "group shrinks", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    char props[512];
    ASSERT_EQ(md_edge(db, "Areas", "Square.area", props, sizeof(props)), 1);
    th_cleanup(tmp);
    PASS();
}

/* #2588: a reference into a directory that was moved or renamed named a
 * path whose directory is gone, so it counted as no reference to this
 * repository: no edge and no row. A directory above it that the reference
 * wrote is still there, so it is doc rot. A path none of whose directories
 * the repository has stays no reference. */
TEST(doc_links_md_moved_directory_issue2588) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlmd_mv_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    th_write_file(TH_PATH(repo, "docs/notes.md"), "# Notes\n"
                                                  "\n"
                                                  "See `src/util/rounding.py::clamp` and "
                                                  "[rounding](../src/util/rounding.py).\n"
                                                  "Create `scratch/new/thing.py` first.\n");
    th_write_file(TH_PATH(repo, "src/ledger.py"), "def balance(ledger):\n"
                                                  "    return sum(ledger)\n");
    th_write_file(TH_PATH(repo, "src/util/rounding.py"), "def clamp(x, lo, hi):\n"
                                                         "    return max(lo, min(hi, x))\n");
    char db[512];
    char full_db[512];
    snprintf(db, sizeof(db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    char props[512];
    char reason[64];
    ASSERT_EQ(md_edge(db, "Notes", "rounding.clamp", props, sizeof(props)), 1);

    /* src/util moves to src/helpers: both references are missing, on the
     * incremental route as in a full index */
    th_write_file(TH_PATH(repo, "src/helpers/rounding.py"), "def clamp(x, lo, hi):\n"
                                                            "    return max(lo, min(hi, x))\n");
    ASSERT_EQ(th_rmtree(TH_PATH(repo, "src/util")), 0);
    ASSERT_EQ(dm_step(repo, db, full_db, "directory moved", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_EQ(md_edge(db, "Notes", "rounding.clamp", props, sizeof(props)), 0);
    dm_row(db, "docs/notes.md", "src/util/rounding.py::clamp", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    dm_row(db, "docs/notes.md", "../src/util/rounding.py", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");

    /* no directory of it in the repository: still no reference */
    dm_row(db, "docs/notes.md", "scratch/new/thing.py", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");

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
    /* the doc-comment families whose held-out tiers passed (2026-10-11) */
    static const int passed[] = {
        CBM_DOCLINK_GO_DOC_LINK,  CBM_DOCLINK_JAVA_LINK,    CBM_DOCLINK_JAVA_LINKPLAIN,
        CBM_DOCLINK_JAVA_SEE,     CBM_DOCLINK_JAVA_THROWS,  CBM_DOCLINK_JAVA_VALUE,
        CBM_DOCLINK_KT_KDOC_LINK, CBM_DOCLINK_KT_SEE,       CBM_DOCLINK_KT_THROWS,
        CBM_DOCLINK_KT_SAMPLE,    CBM_DOCLINK_RS_INTRA_DOC, CBM_DOCLINK_RS_LINK_DEF,
        CBM_DOCLINK_PERL_POD_L,   CBM_DOCLINK_TS_LINK,      CBM_DOCLINK_PHP_SEE,
        CBM_DOCLINK_C_REF,        CBM_DOCLINK_C_SEE,
    };
    for (size_t i = 0; i < sizeof(passed) / sizeof(passed[0]); i++) {
        ASSERT_TRUE(cbm_doclink_syntax_ships(passed[i]));
    }
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
    RUN_TEST(doc_links_md_repeated_bare_name_priority);
    RUN_TEST(doc_links_md_heading_suffix_reservations);
    RUN_TEST(doc_links_md_generated_name_reservations);
    RUN_TEST(doc_links_md_distinct_chains_same_dotted_name);
    RUN_TEST(doc_links_md_setext_parent_names);
    RUN_TEST(doc_links_md_quoted_setext_heading_text);
    RUN_TEST(doc_links_md_mixed_heading_container_scope);
    RUN_TEST(doc_links_md_empty_heading_resets_parent);
    RUN_TEST(doc_links_md_flat_collection_work);
    RUN_TEST(doc_links_md_independent_collision_work);
    RUN_TEST(doc_links_md_shared_collision_work);
    RUN_TEST(doc_links_md_repeated_headings_by_parent_issue2586);
    RUN_TEST(doc_links_md_repeated_heading_name_collision_issue2586);
    RUN_TEST(doc_links_md_classify_span);
    RUN_TEST(doc_links_md_resolve);
    RUN_TEST(doc_links_md_member_not_defined_issue2587);
    RUN_TEST(doc_links_md_member_written_issue2587);
    RUN_TEST(doc_links_md_incremental);
    RUN_TEST(doc_links_md_member_missing_incremental);
    RUN_TEST(doc_links_md_moved_directory_issue2588);
    RUN_TEST(doc_links_md_adr);
    RUN_TEST(doc_links_md_adr_label_choice);
    RUN_TEST(doc_links_md_adr_reported);
    RUN_TEST(doc_links_md_name_scope_rules);
    cbm_doclink_test_reset_ships();
    RUN_TEST(doc_links_md_ship_gate);
}
