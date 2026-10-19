/*
 * test_doc_mentions_go.c — Go doc links ([Name], [pkg.Name], [Recv.Method])
 * -> MENTIONS edges.
 *
 * Extraction (doclink_go.c: the go/doc/comment text model, the candidate
 * tokens, the scope blob) and the resolver (doc_links_go.c) through real
 * fixtures indexed with cbm_pipeline_run.
 */
#include "test_framework.h"
#include "test_doc_mentions_helpers.h"

#include "foundation/log.h"

/* ── helpers of this file ────────────────────────────────────────── */

static CBMFileResult *go_extract(const char *src, const char *rel) {
    return dm_extract(src, CBM_LANG_GO, rel);
}

/* The tokens of one doc comment put above `type T struct{}` in package p. */
static CBMFileResult *go_doc(const char *comment) {
    char src[8192];
    snprintf(src, sizeof(src), "package p\n\n%stype T struct{}\n", comment);
    return go_extract(src, "pkg/t.go");
}

/* True when the scope blob holds exactly this record line. */
static bool go_has_rec(const char *scope, const char *rec) {
    size_t n = strlen(rec);
    for (const char *p = scope; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (len == n && memcmp(p, rec, n) == 0) {
            return true;
        }
        p = nl ? nl + 1 : NULL;
    }
    return false;
}

static void go_w(const char *repo, const char *rel, const char *content) {
    th_write_file(TH_PATH(repo, rel), content);
}

/* Tier of the one MENTIONS edge src -> tgt (qualified-name suffixes): "exact"
 * or "unique"; "" when there is no such edge; "many" for more than one. */
static const char *go_tier(const char *db, const char *src, const char *tgt) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    if (n == 0) {
        return "";
    }
    if (n != 1) {
        return "many";
    }
    if (strstr(props, "\"syntax\":\"doc_link\"") == NULL) {
        return "other-syntax";
    }
    return strstr(props, "\"tier\":\"exact\"") ? "exact" : "unique";
}

/* Reason of the unresolved row for `raw` in `rel`; "" when there is none. */
static const char *go_reason(const char *db, const char *rel, const char *raw) {
    static char reason[64];
    dm_row(db, rel, raw, reason, sizeof(reason), NULL, 0);
    return reason;
}

typedef struct {
    const char *src;
    const char *tgt;
    const char *tier; /* "": there must be no edge */
} go_want_edge_t;

typedef struct {
    const char *rel;
    const char *raw;
    const char *reason; /* "": there must be no row */
} go_want_row_t;

/* The number of edges that are not as wanted (each one printed). */
static int go_bad_edges(const char *db, const go_want_edge_t *want, size_t n) {
    int bad = 0;
    for (size_t i = 0; i < n; i++) {
        const char *tier = go_tier(db, want[i].src, want[i].tgt);
        if (strcmp(tier, want[i].tier) != 0) {
            printf("  edge %s -> %s: \"%s\", want \"%s\"\n", want[i].src, want[i].tgt, tier,
                   want[i].tier);
            bad++;
        }
    }
    return bad;
}

/* The number of rows that are not as wanted (each one printed). */
static int go_bad_rows(const char *db, const go_want_row_t *want, size_t n) {
    int bad = 0;
    for (size_t i = 0; i < n; i++) {
        const char *reason = go_reason(db, want[i].rel, want[i].raw);
        if (strcmp(reason, want[i].reason) != 0) {
            printf("  row %s [%s]: \"%s\", want \"%s\"\n", want[i].rel, want[i].raw, reason,
                   want[i].reason);
            bad++;
        }
    }
    return bad;
}

#define GO_COUNT(a) (sizeof(a) / sizeof((a)[0]))

/* ── extraction: tokens ──────────────────────────────────────────── */

TEST(doc_mentions_go_tokens) {
    const char *src =
        "package p\n"                                                                  /* 1 */
        "\n"                                                                           /* 2 */
        "// Widget does things. See [Thing], [*Star], [Thing.Run],\n"                  /* 3 */
        "// [fmt.Println], [encoding/json.Marshal], [io] and [pkg.Recv.Name].\n"       /* 4 */
        "// Not links: map[string]Value, a[i], x[Mid]y, [not a link], [lower.case],\n" /* 5 */
        "// [Plural]s and s[Sub].\n"                                                   /* 6 */
        "//\n"                                                                         /* 7 */
        "//\tcode [InCode] here\n"                                                     /* 8 */
        "//\n"                                                                         /* 9 */
        "// # Heading [InHeading]\n"                                                   /* 10 */
        "//\n"                                                                         /* 11 */
        "//   - item [InList] one\n"                                                   /* 12 */
        "//   - [RFC 7159] defined below\n"                                            /* 13 */
        "//\n"                                                                         /* 14 */
        "// [RFC 7159]: https://tools.ietf.org/html/rfc7159\n"                         /* 15 */
        "type Widget struct{}\n";                                                      /* 16 */
    CBMFileResult *r = go_extract(src, "pkg/w.go");
    ASSERT_NOT_NULL(r);
    const CBMDocLink *thing = dm_find_token(r, "Thing");
    ASSERT_NOT_NULL(thing);
    ASSERT_EQ(thing->line, 3);
    ASSERT_EQ(thing->syntax, CBM_DOCLINK_GO_DOC_LINK);
    ASSERT_STR_EQ(thing->source_qn, "p.pkg.Widget");
    ASSERT_EQ(thing->def_line, 16);
    /* the star stays in the raw text; the resolver skips it */
    ASSERT_NOT_NULL(dm_find_token(r, "*Star"));
    ASSERT_NOT_NULL(dm_find_token(r, "Thing.Run"));
    const CBMDocLink *println = dm_find_token(r, "fmt.Println");
    ASSERT_NOT_NULL(println);
    ASSERT_EQ(println->line, 4);
    ASSERT_NOT_NULL(dm_find_token(r, "encoding/json.Marshal"));
    ASSERT_NOT_NULL(dm_find_token(r, "io"));
    ASSERT_NOT_NULL(dm_find_token(r, "pkg.Recv.Name"));
    /* the border rule: a bracket pair touching a letter is an index or a
     * type, never a link */
    ASSERT_NULL(dm_find_token(r, "string"));
    ASSERT_NULL(dm_find_token(r, "i"));
    ASSERT_NULL(dm_find_token(r, "Mid"));
    ASSERT_NULL(dm_find_token(r, "Plural"));
    ASSERT_NULL(dm_find_token(r, "Sub"));
    /* no doc-link shape */
    ASSERT_NULL(dm_find_token(r, "not a link"));
    ASSERT_NULL(dm_find_token(r, "lower.case"));
    /* a code block and a heading hold no links; a list item does */
    ASSERT_NULL(dm_find_token(r, "InCode"));
    ASSERT_NULL(dm_find_token(r, "InHeading"));
    const CBMDocLink *in_list = dm_find_token(r, "InList");
    ASSERT_NOT_NULL(in_list);
    ASSERT_EQ(in_list->line, 12);
    /* a text with a link definition is a URL: one href token per use, none
     * for the definition itself */
    const CBMDocLink *href = dm_find_token(r, "https://tools.ietf.org/html/rfc7159");
    ASSERT_NOT_NULL(href);
    ASSERT_EQ(href->syntax, CBM_DOCLINK_HREF);
    ASSERT_EQ(href->line, 13);
    ASSERT_EQ(dm_count_tokens(r, "https://tools.ietf.org/html/rfc7159"), 1);
    ASSERT_NULL(dm_find_token(r, "RFC 7159"));
    ASSERT_EQ(r->doc_links.count, 9);
    cbm_free_result(r);

    /* the family and its name */
    ASSERT_STR_EQ(cbm_doclink_syntax_name(CBM_DOCLINK_GO_DOC_LINK), "doc_link");
    const CBMDocLinkFamily *f = cbm_doclink_family(CBM_DOCLINK_GO_DOC_LINK);
    ASSERT_NOT_NULL(f);
    ASSERT_EQ(f->lang, CBM_LANG_GO);
    /* ships in production: its held-out audit passed */
    cbm_doclink_test_reset_ships();
    bool ships_in_production = cbm_doclink_syntax_ships(CBM_DOCLINK_GO_DOC_LINK);
    dm_ship_held_families();
    ASSERT_TRUE(ships_in_production);
    ASSERT_TRUE(cbm_doclink_syntax_ships(CBM_DOCLINK_GO_DOC_LINK));
    PASS();
}

/* Every documented kind of definition is a source. */
TEST(doc_mentions_go_sources) {
    const char *src = "package p\n"
                      "\n"
                      "// F doc [RefF].\n"
                      "func F() {}\n"
                      "\n"
                      "// S doc [RefS].\n"
                      "type S struct {\n"
                      "\t// Field doc [RefField].\n"
                      "\tField int\n"
                      "}\n"
                      "\n"
                      "// M doc [RefM].\n"
                      "func (s *S) M() {}\n"
                      "\n"
                      "// I doc [RefI].\n"
                      "type I interface {\n"
                      "\t// Do doc [RefDo].\n"
                      "\tDo()\n"
                      "}\n"
                      "\n"
                      "// C doc [RefC].\n"
                      "const C = 1\n"
                      "\n"
                      "const (\n"
                      "\t// K doc [RefK].\n"
                      "\tK = iota\n"
                      ")\n"
                      "\n"
                      "// V doc [RefV].\n"
                      "var V = 2\n"
                      "\n"
                      "// A doc [RefA].\n"
                      "type A = S\n";
    CBMFileResult *r = go_extract(src, "pkg/s.go");
    ASSERT_NOT_NULL(r);
    static const struct {
        const char *raw;
        const char *source;
    } want[] = {
        {"RefF", "p.pkg.F"}, {"RefS", "p.pkg.S"},     {"RefField", "p.pkg.S.Field"},
        {"RefM", "p.pkg.M"}, /* Go method nodes have a flat name */
        {"RefI", "p.pkg.I"}, {"RefDo", "p.pkg.I.Do"}, {"RefC", "p.pkg.C"},
        {"RefK", "p.pkg.K"}, {"RefV", "p.pkg.V"},     {"RefA", "p.pkg.A"},
    };
    for (size_t i = 0; i < GO_COUNT(want); i++) {
        const CBMDocLink *t = dm_find_token(r, want[i].raw);
        if (!t || strcmp(t->source_qn, want[i].source) != 0) {
            printf("  %s: source %s, want %s\n", want[i].raw, t ? t->source_qn : "(no token)",
                   want[i].source);
            FAIL("source of a doc link");
        }
        ASSERT_EQ(dm_count_tokens(r, want[i].raw), 1);
        ASSERT_EQ(t->flags & CBM_DOCLINK_FLAG_FILE, 0);
    }
    cbm_free_result(r);
    PASS();
}

/* The package comment is a source of its own (the File node), harvested once:
 * no definition's docstring is the package comment. */
TEST(doc_mentions_go_package_comment_tokens) {
    static const struct {
        const char *what;
        const char *src;
        int file_tokens; /* tokens [Ref] flagged as written in the file's doc */
        int def_tokens;  /* tokens [Ref] of a definition */
        uint32_t line;   /* line of the file-level token */
    } cases[] = {
        {"package comment, then a documented declaration",
         "// Package p has a [Ref].\npackage p\n\n// F is.\nfunc F() {}\n", 1, 0, 1},
        {"declaration without a doc right after the clause",
         "// Package p has a [Ref].\npackage p\nfunc F() {}\ntype T struct{}\nvar V = 1\n", 1, 0,
         1},
        {"block comment", "/*\nPackage p has a [Ref].\n*/\npackage p\n\nfunc F() {}\n", 1, 0, 2},
        {"a header comment is no package comment",
         "// Copyright [Ref].\n\npackage p\n\nfunc F() {}\n", 0, 0, 0},
        {"header, then the package comment",
         "// Copyright.\n\n// Package p has a [Ref].\npackage p\n\nfunc F() {}\n", 1, 0, 3},
        {"build line, then the package comment",
         "//go:build linux\n\n// Package p has a [Ref].\npackage p\n", 1, 0, 3},
        {"the same link in the package comment and in a declaration's doc",
         "// Package p has a [Ref].\npackage p\n\n// F has a [Ref].\nfunc F() {}\n", 1, 1, 1},
    };
    for (size_t i = 0; i < GO_COUNT(cases); i++) {
        CBMFileResult *r = go_extract(cases[i].src, "pkg/doc.go");
        ASSERT_NOT_NULL(r);
        int file_tokens = 0;
        int def_tokens = 0;
        uint32_t line = 0;
        for (int k = 0; k < r->doc_links.count; k++) {
            const CBMDocLink *t = &r->doc_links.items[k];
            if (strcmp(t->raw, "Ref") != 0) {
                continue;
            }
            if (t->flags & CBM_DOCLINK_FLAG_FILE) {
                file_tokens++;
                line = t->line;
            } else {
                def_tokens++;
            }
        }
        if (file_tokens != cases[i].file_tokens || def_tokens != cases[i].def_tokens ||
            line != cases[i].line) {
            printf("  %s: %d file tokens (line %u), %d definition tokens\n", cases[i].what,
                   file_tokens, line, def_tokens);
            cbm_free_result(r);
            FAIL("package comment harvested once, as the file's");
        }
        cbm_free_result(r);
    }
    PASS();
}

/* The go/doc/comment text model, case by case. */
TEST(doc_mentions_go_text_model) {
    static const struct {
        const char *what;
        const char *comment;
        const char *present; /* '|'-separated raw texts that must be tokens */
        const char *absent;  /* '|'-separated raw texts that must not be */
    } cases[] = {
        {"block comment", "/*\nBlock [InBlock] text.\n\n\tcode [BlockCode]\n*/\n", "InBlock",
         "BlockCode"},
        {"block comment on one line", "/* One [OneLine] line. */\n", "OneLine", ""},
        {"common indent is no code", "/*\n    Indented [Para] text\n    more text\n*/\n", "Para",
         ""},
        {"no space after the slashes", "//[Tight] text\n", "Tight", ""},
        {"directive lines are no text", "// See [Kept].\n//go:generate echo [Gen]\n", "Kept",
         "Gen"},
        {"carriage returns", "// Line [Cr] one.\r\n// Line two.\r\n", "Cr", ""},
        {"numbered list", "// Steps:\n//  1. first [NumOne]\n//  2) second [NumTwo]\n",
         "NumOne|NumTwo", ""},
        {"list item continuation", "// List:\n//   - item starts\n//     and [Continued] here\n",
         "Continued", ""},
        {"code after a paragraph without a blank line",
         "// Text [BeforeCode] then\n//\tx := [NotThis]\n", "BeforeCode", "NotThis"},
        {"brace line joins the code block",
         "// func main() {\n//\tuse([InBody])\n// }\n// After [AfterBrace].\n", "AfterBrace",
         "InBody"},
        {"unindented list before its wrapped line",
         "// - first [ListA]\n// - second\n//   wrapped [ListB]\n", "ListA|ListB", ""},
        {"heading needs blank lines only towards text",
         "// Intro.\n//\n// # Title [InTitle]\n//\n// Body [InBody2].\n", "InBody2", "InTitle"},
        {"hash line inside a paragraph is text", "// Intro\n// # not a heading [InPara]\n",
         "InPara", ""},
        {"a pair restarts at every opening bracket", "// See [[Inner]] and [a [Second]].\n",
         "Inner|Second", "a [Second"},
        {"brackets across a line break are no link", "// See [Split\n// Name] here.\n", "",
         "Split\nName|Split Name"},
        {"unicode punctuation borders",
         "// See \xE2\x80\x9C[Quoted]\xE2\x80\x9D \xE2\x80\x94[Dashed]\xE2\x80\xA6\n",
         "Quoted|Dashed", ""},
        {"letters of any script are no border",
         "// See \xC3\xA9[AfterLetter] and [BeforeLetter]\xD0\xB6.\n", "",
         "AfterLetter|BeforeLetter"},
        {"symbols are no punctuation", "// See `[Ticked]` =[Equals] +[Plus]+ $[Dollar].\n", "",
         "Ticked|Equals|Plus|Dollar"},
        {"punctuation on both sides", "// See ([Paren]), \"[Quote]\", [Comma], [Dot].\n",
         "Paren|Quote|Comma|Dot", ""},
        {"paragraph edges", "// [First] word and last [Last]\n", "First|Last", ""},
        {"link definitions must fill their paragraph",
         "// [Mixed]: https://example.com/a\n// and text [Other].\n", "Other|Mixed", ""},
        {"unknown scheme defines nothing", "// Use [Odd].\n//\n// [Odd]: gopher2://example.com\n",
         "Odd", ""},
        /* go/doc looks a link text up from the FIRST opening bracket since the
         * last closing one, without the brackets: `[a [Def]` asks for `a Def` */
        {"nested opening brackets: the pair's own text is not the looked-up text",
         "// See [a [Def] here.\n//\n// [Def]: https://example.com/def\n", "Def",
         "https://example.com/def"},
        {"nested opening brackets: the text since the first one is",
         "// See [a [Def] here.\n//\n// [a Def]: https://example.com/adef\n",
         "https://example.com/adef", "Def"},
        {"name shapes", "// [x.Y] [x.Y.Z] [X.Y] [a/b] [a/b.C] [a/b.C.D] [*a/b.C]\n",
         "x.Y|x.Y.Z|X.Y|a/b|a/b.C|a/b.C.D|*a/b.C", ""},
        {"bad shapes",
         "// [x.y.Z] [X.y] [1x] [X-Y] [a//b] [a/b/] [-a/b] [a/.b] [x.Y.Z.W] [**X] [] [*]\n", "",
         "x.y.Z|X.y|X-Y|a//b|a/b/|-a/b|a/.b|x.Y.Z.W|**X|*"},
        {"a quirk kept from go/doc: a leading dot", "// See [.Dotted].\n", ".Dotted", ""},
        /* a Name starts with an upper-case letter of any script; the word
         * before the dot is a package name whatever its letters */
        {"capital letters beyond ASCII",
         "// See [\xC3\x9C"
         "ber], [\xC3\xBC"
         "ber.Name] and [X.\xC3\xBC"
         "ber].\n",
         "\xC3\x9C"
         "ber|\xC3\xBC"
         "ber.Name",
         "X.\xC3\xBC"
         "ber"},
    };
    for (size_t i = 0; i < GO_COUNT(cases); i++) {
        CBMFileResult *r = go_doc(cases[i].comment);
        ASSERT_NOT_NULL(r);
        for (int pass = 0; pass < 2; pass++) {
            char list[512];
            snprintf(list, sizeof(list), "%s", pass == 0 ? cases[i].present : cases[i].absent);
            for (char *tok = list; tok && *tok;) {
                char *bar = strchr(tok, '|');
                if (bar) {
                    *bar = '\0';
                }
                bool found = dm_find_token(r, tok) != NULL;
                if (found != (pass == 0)) {
                    printf("  %s: [%s] %s\n", cases[i].what, tok,
                           found ? "is a token" : "is no token");
                    cbm_free_result(r);
                    FAIL("go/doc text model");
                }
                tok = bar ? bar + 1 : NULL;
            }
        }
        cbm_free_result(r);
    }
    PASS();
}

/* A reference to the documented function's own parameter is local. */
TEST(doc_mentions_go_own_parameter) {
    const char *src = "package p\n"
                      "\n"
                      "// Fetch reads from [url] with [Client] and reports to [Other].\n"
                      "func Fetch(url string, Client int) {}\n"
                      "\n"
                      "// Later mentions [url] and [Client], which are no parameters here.\n"
                      "func Later() {}\n";
    CBMFileResult *r = go_extract(src, "pkg/f.go");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(dm_find_token(r, "Other"));
    /* one token each: the one of Later */
    ASSERT_EQ(dm_count_tokens(r, "url"), 1);
    ASSERT_EQ(dm_count_tokens(r, "Client"), 1);
    ASSERT_STR_EQ(dm_find_token(r, "url")->source_qn, "p.pkg.Later");
    ASSERT_STR_EQ(dm_find_token(r, "Client")->source_qn, "p.pkg.Later");
    cbm_free_result(r);
    PASS();
}

/* ── extraction: hostile input ───────────────────────────────────── */

TEST(doc_mentions_go_hostile_doc) {
    /* an unterminated block comment, a bracket at the very end, a lone
     * closing bracket, a bracket that never closes */
    CBMFileResult *r = go_doc("// ] [ [Ok] [\n");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 1);
    ASSERT_NOT_NULL(dm_find_token(r, "Ok"));
    cbm_free_result(r);

    /* invalid UTF-8 next to a bracket is no border: no token, no crash */
    r = go_doc("// \xFF[Bad1] [Bad2]\xC3 [Bad3]\x80 \xE2\x80[Bad4] [Good].\n");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(dm_find_token(r, "Bad1"));
    ASSERT_NULL(dm_find_token(r, "Bad2"));
    ASSERT_NULL(dm_find_token(r, "Bad3"));
    ASSERT_NULL(dm_find_token(r, "Bad4"));
    ASSERT_NOT_NULL(dm_find_token(r, "Good"));
    cbm_free_result(r);

    /* an oversized reference is harvested clipped (one byte past the limit,
     * so the resolver sees that it is too long and never resolves it); the
     * next reference is read as usual */
    enum { BIG = 1 << 20 };
    char *src = malloc(BIG + 256);
    ASSERT_NOT_NULL(src);
    size_t w = (size_t)snprintf(src, 64, "package p\n\n// See [");
    memset(src + w, 'A', BIG);
    w += BIG;
    w += (size_t)snprintf(src + w, 128, "] and [Small].\ntype T struct{}\n");
    r = cbm_extract_file(src, (int)w, CBM_LANG_GO, "p", "pkg/big.go", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 2);
    ASSERT_NOT_NULL(dm_find_token(r, "Small"));
    ASSERT_EQ(strlen(r->doc_links.items[0].raw), (size_t)CBM_DOCLINK_GO_REF_MAX + 1);
    ASSERT_EQ(r->doc_links.items[0].syntax, CBM_DOCLINK_GO_DOC_LINK);
    cbm_free_result(r);

    /* oversized text that is no link (it has spaces) is no token at all */
    w = (size_t)snprintf(src, 64, "package p\n\n// See [");
    memset(src + w, 'A', BIG);
    src[w + 100] = ' ';
    w += BIG;
    w += (size_t)snprintf(src + w, 128, "] and [Small].\ntype T struct{}\n");
    r = cbm_extract_file(src, (int)w, CBM_LANG_GO, "p", "pkg/prose.go", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 1);
    ASSERT_NOT_NULL(dm_find_token(r, "Small"));
    cbm_free_result(r);

    /* a megabyte of opening brackets, then one pair: linear, one token */
    w = (size_t)snprintf(src, 64, "package p\n\n// ");
    memset(src + w, '[', BIG);
    w += BIG;
    w += (size_t)snprintf(src + w, 128, "Deep] end.\ntype T struct{}\n");
    r = cbm_extract_file(src, (int)w, CBM_LANG_GO, "p", "pkg/deep.go", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 1);
    ASSERT_NOT_NULL(dm_find_token(r, "Deep"));
    cbm_free_result(r);

    /* thousands of link definitions and uses: every use resolves to its own
     * definition (first definition wins), without a quadratic scan */
    enum { DEFS = 4000 };
    size_t cap = (size_t)DEFS * 96 + 256;
    char *many = malloc(cap);
    ASSERT_NOT_NULL(many);
    w = (size_t)snprintf(many, cap, "package p\n\n");
    for (int i = 0; i < DEFS; i++) {
        w += (size_t)snprintf(many + w, cap - w, "// Use [t%d].\n", i);
    }
    w += (size_t)snprintf(many + w, cap - w, "//\n");
    for (int i = 0; i < DEFS; i++) {
        w += (size_t)snprintf(many + w, cap - w, "// [t%d]: https://example.com/%d\n", i, i);
    }
    w += (size_t)snprintf(many + w, cap - w,
                          "// [t7]: https://example.com/shadowed\n"
                          "type T struct{}\n");
    r = cbm_extract_file(many, (int)w, CBM_LANG_GO, "p", "pkg/many.go", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, DEFS);
    ASSERT_EQ(dm_count_tokens(r, "https://example.com/7"), 1);
    ASSERT_NULL(dm_find_token(r, "https://example.com/shadowed"));
    cbm_free_result(r);
    free(many);

    /* a NUL byte ends the comment text; what precedes it is still read */
    static const char nul_src[] = "package p\n\n// See [Before]\0[After].\ntype T struct{}\n";
    r = cbm_extract_file(nul_src, (int)sizeof(nul_src) - 1, CBM_LANG_GO, "p", "pkg/nul.go", 0, NULL,
                         NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(dm_find_token(r, "After"));
    cbm_free_result(r);
    free(src);
    PASS();
}

/* A fixed pseudo-random sequence: the same texts on every run and platform. */
static uint32_t go_rand(uint32_t *state) {
    *state = (*state * 1664525U) + 1013904223U;
    return *state >> 8;
}

/* Append one of `count` pieces, chosen by the sequence. */
static size_t go_rand_piece(char *buf, size_t len, size_t cap, uint32_t *state,
                            const char *const *pieces, size_t count) {
    const char *p = pieces[go_rand(state) % count];
    size_t pl = strlen(p);
    if (len + pl >= cap) {
        return len;
    }
    memcpy(buf + len, p, pl);
    return len + pl;
}

/* Comment text made of the pieces the text model reacts to, in every order:
 * the parser must stay inside its buffers (the runner has the sanitizers on)
 * and every token must keep the shape the resolver relies on. */
TEST(doc_mentions_go_random_docs) {
    static const char *const pieces[] = {
        "[",
        "]",
        "[",
        "]",
        "*",
        ".",
        "/",
        "Name",
        "pkg",
        "a/b",
        " ",
        " ",
        "\n// ",
        "\n//\t",
        "\n//",
        "\n//\n// ",
        "# ",
        "- ",
        "1. ",
        ": ",
        "https://x",
        "\xC3\xA9",
        "\xFF",
        "\xE2\x80",
        "\xE2\x80\x9C",
        "{",
        "}",
        "\\",
        "\r",
        "\t",
        "T",
        "x",
        "]: ",
        "[Name]",
        "[p.Name]",
        "[a/b.Name.M]",
        "\n//\n// [Name]: https://x/y\n//\n// ",
        "/*",
        "*/",
        "_",
        "9",
    };
    uint32_t state = 20261002U;
    char src[4096];
    int doc_links = 0;
    int hrefs = 0;
    for (int round = 0; round < 1500; round++) {
        size_t len = (size_t)snprintf(src, sizeof(src), "package p\n\n// ");
        int n = 1 + (int)(go_rand(&state) % 120);
        for (int i = 0; i < n; i++) {
            len = go_rand_piece(src, len, sizeof(src) - 64, &state, pieces, GO_COUNT(pieces));
        }
        len += (size_t)snprintf(src + len, sizeof(src) - len, "\nfunc F(Name int) {}\n");
        CBMFileResult *r =
            cbm_extract_file(src, (int)len, CBM_LANG_GO, "p", "pkg/r.go", 0, NULL, NULL);
        ASSERT_NOT_NULL(r);
        for (int k = 0; k < r->doc_links.count; k++) {
            const CBMDocLink *t = &r->doc_links.items[k];
            size_t raw_len = t->raw ? strlen(t->raw) : 0;
            bool shape_ok = t->source_qn && raw_len > 0 && t->line >= 3 &&
                            (t->syntax == CBM_DOCLINK_GO_DOC_LINK || t->syntax == CBM_DOCLINK_HREF);
            CBMDocLinkGoRef ref;
            if (shape_ok && t->syntax == CBM_DOCLINK_GO_DOC_LINK) {
                /* a doc-link token is what the resolver can read back */
                shape_ok = raw_len <= CBM_DOCLINK_GO_REF_MAX &&
                           cbm_doclink_go_parse_ref(t->raw, raw_len, &ref) &&
                           memchr(t->raw, '\n', raw_len) == NULL;
            }
            if (!shape_ok) {
                printf("  round %d: token [%s] syntax %d line %u\n", round, t->raw ? t->raw : "",
                       (int)t->syntax, t->line);
                cbm_free_result(r);
                FAIL("token shape");
            }
            doc_links += t->syntax == CBM_DOCLINK_GO_DOC_LINK;
            hrefs += t->syntax == CBM_DOCLINK_HREF;
        }
        cbm_free_result(r);
    }
    /* the texts do hold links of both families: the checks above ran */
    if (doc_links < 1000 || hrefs < 1) {
        printf("  %d doc links, %d hrefs\n", doc_links, hrefs);
        FAIL("random texts yield tokens");
    }
    PASS();
}

/* Declarations in every order and state of repair: the scope scan must stay
 * inside its buffers and write only well-formed records. */
TEST(doc_mentions_go_random_scopes) {
    static const char *const pieces[] = {
        "func ",   "type ",
        "var ",    "const ",
        "import ", "package q\n",
        "struct ", "interface ",
        "(",       ")",
        "{",       "}",
        "[",       "]",
        "\n",      "\n",
        "\t",      " ",
        "=",       ",",
        ".",       "*",
        "A",       "Bee",
        "c",       "T",
        "int",     "\"a/b\"",
        "`x`",     "//go:build ignore\n",
        "// c\n",  "/* c */",
        "1",       ";",
        "any",     "func (r *T[K]) ",
        "Bee int", "A, Bee string",
    };
    uint32_t state = 20261003U;
    char src[4096];
    for (int round = 0; round < 1500; round++) {
        size_t len =
            (size_t)snprintf(src, sizeof(src), "%s", (round % 4 == 0) ? "" : "package p\n\n");
        int n = 1 + (int)(go_rand(&state) % 150);
        for (int i = 0; i < n; i++) {
            len = go_rand_piece(src, len, sizeof(src) - 8, &state, pieces, GO_COUNT(pieces));
        }
        src[len] = '\0';
        CBMFileResult *r =
            cbm_extract_file(src, (int)len, CBM_LANG_GO, "p", "pkg/r.go", 0, NULL, NULL);
        ASSERT_NOT_NULL(r);
        const char *s = r->doc_scope;
        bool ok = s && strncmp(s, "go1", 3) == 0 && (s[3] == '\n' || s[3] == '\0');
        for (const char *p = ok ? strchr(s, '\n') : NULL; ok && p && p[1];) {
            const char *line = p + 1;
            ok = strchr("PIFMTVfmg", line[0]) != NULL && line[1] == '\t' && line[2] != '\0' &&
                 line[2] != '\n';
            p = strchr(line, '\n');
        }
        char *portable = ok ? cbm_doclink_portable_scope(s) : NULL;
        ok = ok && portable && strstr(portable, "\ng\t") == NULL;
        cbm_free(CBM_MEM_CLASS_OTHER, portable);
        if (!ok) {
            printf("  round %d: scope\n%s\nof\n%s\n", round, s ? s : "(null)", src);
            cbm_free_result(r);
            FAIL("scope records");
        }
        cbm_free_result(r);
    }
    PASS();
}

/* ── extraction: scope blob ──────────────────────────────────────── */

TEST(doc_mentions_go_scope_blob) {
    const char *src = "package alpha\n"
                      "\n"
                      "import (\n"
                      "\t\"fmt\"\n"
                      "\tal \"example.com/demo/pkg/alpha\"\n"
                      "\t. \"example.com/dot\"\n"
                      "\t_ \"example.com/blank\"\n"
                      "\tbad \"a\\tb\"\n"
                      ")\n"
                      "import \"io\"\n"
                      "\n"
                      "const Max = 10\n"
                      "const (\n"
                      "\tKindA, KindB = 1, 2\n"
                      ")\n"
                      "var Err = fmt.Errorf(\"x\")\n"
                      "var (\n"
                      "\tBlockVar = 1\n"
                      ")\n"
                      "var hidden = 1\n"
                      "\n"
                      "type Widget struct {\n"
                      "\tName, Size string\n"
                      "\tio.Reader\n"
                      "\t*Kind\n"
                      "\tsecret int\n"
                      "}\n"
                      "type Runner interface {\n"
                      "\tRun() error\n"
                      "\tstop()\n"
                      "\tio.Closer\n"
                      "}\n"
                      "type Kind int\n"
                      "type Alias = al.Widget\n"
                      "type Same = Kind\n"
                      "type List[T any, U comparable] struct{ items []T }\n"
                      "type lower struct{ Field int }\n"
                      "\n"
                      "func New() *Widget { return nil }\n"
                      "func (w *Widget) Run() error { return nil }\n"
                      "func (k Kind) Run() error { return nil }\n"
                      "func (l *List[A, B]) Push(v A) {}\n"
                      "func (x lower) Exported() {}\n"
                      "func Map[K comparable, V any](m map[K]V) {}\n"
                      "func helper() {}\n";
    CBMFileResult *r = go_extract(src, "pkg/alpha/a.go");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *s = r->doc_scope;
    ASSERT(strncmp(s, "go1\n", 4) == 0);
    static const char *const want[] = {
        "P\talpha\t-",
        "I\t-\tfmt",
        "I\t-\tio",
        "I\tal\texample.com/demo/pkg/alpha",
        "I\tbad\t?", /* a path this scan cannot record: the qualifier still exists */
        /* values: the first name of a constant spec or of a parenthesis-free
         * var has a node (and keeps it: w 1); the others have none (w -) */
        "V\tMax\t1",
        "V\tKindA\t1",
        "V\tKindB\t-",
        "V\tErr\t1",
        "V\tBlockVar\t-",
        /* types; fields (the first name of a declaration has a node, embedded
         * fields and later names have none); interface methods */
        "T\tWidget\ts\t-\t1",
        "f\tWidget\tName\t1",
        "f\tWidget\tSize\t0",
        "f\tWidget\tReader\t0",
        "f\tWidget\tKind\t0",
        "T\tRunner\ti\t-\t1",
        "m\tRunner\tRun",
        "T\tKind\to\t-\t1",
        "T\tAlias\ta\tal.Widget\t1",
        "T\tSame\ta\tKind\t1",
        "T\tList\ts\t-\t1",
        /* functions and methods; two methods named Run compete for one node
         * and the later one keeps it */
        "F\tNew\t1",
        "F\tMap\t1",
        "M\tWidget\tRun\t0",
        "M\tKind\tRun\t1",
        "M\tList\tPush\t1",
        "M\tlower\tExported\t1",
        /* own type parameters, by the declaration's first and last line */
        "g\t36\t36\tT,U",
        "g\t42\t42\tA,B",
        "g\t44\t44\tK,V",
    };
    for (size_t i = 0; i < GO_COUNT(want); i++) {
        if (!go_has_rec(s, want[i])) {
            printf("  no record \"%s\" in\n%s\n", want[i], s);
            FAIL("scope record");
        }
    }
    ASSERT_NULL(strstr(s, "example.com/dot"));   /* a dot import is no qualifier */
    ASSERT_NULL(strstr(s, "example.com/blank")); /* nor a blank one */
    ASSERT_NULL(strstr(s, "hidden"));
    ASSERT_NULL(strstr(s, "secret"));
    ASSERT_NULL(strstr(s, "stop"));
    ASSERT_NULL(strstr(s, "\tlower\tField")); /* unexported type: nothing to name */
    ASSERT_NULL(strstr(s, "helper"));
    /* records are sorted: the blob does not depend on declaration order */
    for (const char *p = strchr(s, '\n'); p && p[1];) {
        const char *next = strchr(p + 1, '\n');
        if (next && next[1]) {
            const char *after = strchr(next + 1, '\n');
            size_t la = (size_t)(next - p - 1);
            size_t lb = after ? (size_t)(after - next - 1) : strlen(next + 1);
            int c = memcmp(p + 1, next + 1, la < lb ? la : lb);
            ASSERT(c < 0 || (c == 0 && la < lb));
        }
        p = next;
    }
    /* the persisted scope has no line number: the g records are gone */
    char *portable = cbm_doclink_portable_scope(s);
    ASSERT_NOT_NULL(portable);
    ASSERT_NULL(strstr(portable, "\ng\t"));
    ASSERT_TRUE(go_has_rec(portable, "M\tKind\tRun\t1"));
    ASSERT(strncmp(portable, "go1\n", 4) == 0);
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    cbm_free_result(r);

    /* moving declarations changes nothing unless a node changes its owner */
    const char *a = "package p\n\nfunc (a A) Run() {}\nfunc (b B) Run() {}\nfunc F() {}\n";
    const char *moved = "package p\n\nfunc F() {}\n\n\nfunc (a A) Run() {}\nfunc (b B) Run() {}\n";
    const char *swapped = "package p\n\nfunc (b B) Run() {}\nfunc (a A) Run() {}\nfunc F() {}\n";
    CBMFileResult *ra = go_extract(a, "p/x.go");
    CBMFileResult *rm = go_extract(moved, "p/x.go");
    CBMFileResult *rs = go_extract(swapped, "p/x.go");
    ASSERT_NOT_NULL(ra);
    ASSERT_NOT_NULL(rm);
    ASSERT_NOT_NULL(rs);
    ASSERT_STR_EQ(ra->doc_scope, rm->doc_scope);
    ASSERT(strcmp(ra->doc_scope, rs->doc_scope) != 0);
    ASSERT_TRUE(go_has_rec(rs->doc_scope, "M\tA\tRun\t1"));
    ASSERT_TRUE(go_has_rec(rs->doc_scope, "M\tB\tRun\t0"));
    cbm_free_result(ra);
    cbm_free_result(rm);
    cbm_free_result(rs);

    /* the node <dir>.<name> is one for a function, a type, a value and the
     * methods of that name: the last declaration keeps it, whatever its kind,
     * and two on one line contest it */
    CBMFileResult *k1 = go_extract("package p\n\nfunc Open() {}\nfunc (t T) Open() {}\n", "p/k.go");
    CBMFileResult *k2 = go_extract("package p\n\nfunc (t T) Open() {}\nfunc Open() {}\n", "p/k.go");
    CBMFileResult *k3 = go_extract("package p\n\ntype Open int\nvar Open = 1\n"
                                   "const Open = 2; func (t T) Open() {}\n",
                                   "p/k.go");
    ASSERT_NOT_NULL(k1);
    ASSERT_NOT_NULL(k2);
    ASSERT_NOT_NULL(k3);
    ASSERT_TRUE(go_has_rec(k1->doc_scope, "F\tOpen\t0"));
    ASSERT_TRUE(go_has_rec(k1->doc_scope, "M\tT\tOpen\t1"));
    ASSERT_TRUE(go_has_rec(k2->doc_scope, "F\tOpen\t1"));
    ASSERT_TRUE(go_has_rec(k2->doc_scope, "M\tT\tOpen\t0"));
    ASSERT_TRUE(go_has_rec(k3->doc_scope, "T\tOpen\to\t-\t0"));
    ASSERT_TRUE(go_has_rec(k3->doc_scope, "V\tOpen\t0"));
    ASSERT_TRUE(go_has_rec(k3->doc_scope, "V\tOpen\tx"));
    ASSERT_TRUE(go_has_rec(k3->doc_scope, "M\tT\tOpen\tx"));
    cbm_free_result(k1);
    cbm_free_result(k2);
    cbm_free_result(k3);

    /* a build-ignored file, a build-constrained file and a file with a parse
     * error say so */
    CBMFileResult *ig =
        go_extract("//go:build ignore\n\npackage main\n\nfunc Gen() {}\n", "p/gen.go");
    ASSERT_NOT_NULL(ig);
    ASSERT_TRUE(go_has_rec(ig->doc_scope, "P\tmain\ti"));
    cbm_free_result(ig);
    static const char *const constrained[] = {
        "//go:build linux && !cgo\n\npackage p\n",
        "// Copyright.\n\n// +build windows\n\npackage p\n",
        "//+build darwin\n\npackage p\n",
    };
    for (size_t i = 0; i < GO_COUNT(constrained); i++) {
        CBMFileResult *cr = go_extract(constrained[i], "p/c.go");
        ASSERT_NOT_NULL(cr);
        ASSERT_STR_EQ(cr->doc_scope, "go1\nP\tp\tc");
        cbm_free_result(cr);
    }
    static const char *const unconstrained[] = {
        "package p\n\n//go:build linux\n",       /* after the package clause: a comment */
        "//go:buildx linux\n\npackage p\n",      /* no build line */
        "/* //go:build linux */\n\npackage p\n", /* inside a block comment */
    };
    for (size_t i = 0; i < GO_COUNT(unconstrained); i++) {
        CBMFileResult *fr = go_extract(unconstrained[i], "p/f.go");
        ASSERT_NOT_NULL(fr);
        ASSERT_STR_EQ(fr->doc_scope, "go1\nP\tp\t-");
        cbm_free_result(fr);
    }
    CBMFileResult *bad = go_extract("package q\n\nfunc Broken( {\n\tx := \n}\n\nfunc After() {}\n"
                                    "type T struct{}\nfunc (t T) M() {}\n",
                                    "q/broken.go");
    ASSERT_NOT_NULL(bad);
    ASSERT_TRUE(go_has_rec(bad->doc_scope, "P\tq\te"));
    /* what the extractor still finds after the error, the scan finds too */
    for (int i = 0; i < bad->defs.count; i++) {
        const CBMDefinition *d = &bad->defs.items[i];
        if (!d->name || d->name[0] < 'A' || d->name[0] > 'Z' || !d->label ||
            strcmp(d->label, "Module") == 0) {
            continue;
        }
        char needle[128];
        snprintf(needle, sizeof(needle), "\t%s", d->name);
        const char *hit = strstr(bad->doc_scope, needle);
        char after = hit ? hit[strlen(needle)] : 'x';
        if (after != '\t' && after != '\n' && after != '\0') {
            printf("  %s %s has a definition but no scope record\n", d->label, d->name);
            FAIL("scope of a file with a parse error");
        }
    }
    cbm_free_result(bad);

    /* go.mod: the module path */
    const char *mod = "module example.com/demo\n\ngo 1.22\n\nrequire example.com/dep v1.0.0\n";
    CBMFileResult *gm =
        cbm_extract_file(mod, (int)strlen(mod), CBM_LANG_GOMOD, "p", "go.mod", 0, NULL, NULL);
    ASSERT_NOT_NULL(gm);
    ASSERT_NOT_NULL(gm->doc_scope);
    ASSERT_STR_EQ(gm->doc_scope, "go1\nG\texample.com/demo");
    cbm_free_result(gm);
    PASS();
}

/* ── the resolver fixture ────────────────────────────────────────── */

static const char GO_ALPHA_A[] =
    "package alpha\n"
    "\n"
    "import (\n"
    "\t\"fmt\"\n"
    "\n"
    "\t\"example.com/demo/alpha/testdata/fixture\"\n"
    "\t\"example.com/demo/beta\"\n"
    "\tgam \"example.com/demo/gamma\"\n"
    "\t\"example.com/demo/realname\"\n"
    "\t\"example.com/demo/sub/deep\"\n"
    "\t\"example.com/dep/ext\"\n"
    "\t\"example.com/other/pkg\"\n"
    "\tbad \"a\\tb\"\n"
    ")\n"
    "\n"
    "// Widget is the main type. See [New], [Widget.Run], [Runner.Run], [Max],\n"
    "// [Default] and [Block].\n"
    "type Widget struct {\n"
    "\t// Name names it; its size is [Widget.Size].\n"
    "\tName string\n"
    "\tSize, Weight int\n"
    "\tbeta.Embedded\n"
    "}\n"
    "\n"
    "// New makes a [Widget] through [beta.Make] and [gam.Fn]; see [other.Real],\n"
    "// [deep.Deep], [pkg.Far], [beta.Thing.Do], [beta.Thing.Field],\n"
    "// [beta.Thing.Second], [beta.Doer.Act], [beta.Gr\xC3\xB6\xC3\x9F"
    "e], [beta.\xC3\x9C"
    "ber], [beta] and [bad.T].\n"
    "func New() *Widget { return nil }\n"
    "\n"
    "// Run runs. Plain text: [Make], [Thing], [realname.Real], [gamma.Fn],\n"
    "// [Widget.Promoted], [Widget.Depth], [Widget.Unknown], [nosuch.Name], [Nope], "
    "[beta.Fehlt_\xC3\x9F].\n"
    "// Rows: [beta.Nope], [beta.Thing.Nope], [beta.InBlock], [ext.Thing],\n"
    "// [fmt.Println], [encoding/json.Marshal], [io], [example.com/demo/nodir.X]\n"
    "// and [example.com/demo/nested/pkg.Far]. Fields: [Widget.Size], [Widget.Weight], "
    "[Widget.Embedded].\n"
    "func (w *Widget) Run() {}\n"
    "\n"
    "// Runner runs.\n"
    "type Runner interface {\n"
    "\t// Run runs; see [Widget].\n"
    "\tRun() error\n"
    "}\n"
    "\n"
    "// Max is the maximum.\n"
    "const Max = 10\n"
    "\n"
    "// Default is the default; tests have [Helper].\n"
    "var Default = New()\n"
    "\n"
    "var (\n"
    "\t// Block is in a block.\n"
    "\tBlock = 1\n"
    ")\n"
    "\n"
    "// T is a package-level type.\n"
    "type T struct{}\n"
    "\n"
    "// Map shadows: [T] is its own type parameter, [Widget] is not.\n"
    "func Map[T any](x T) {}\n"
    "\n"
    "// Plain has no type parameter: [T] is the type.\n"
    "func Plain() {}\n"
    "\n"
    "// Spec follows the [spec].\n"
    "//\n"
    "// [spec]: https://example.com/spec\n"
    "func Spec() {}\n"
    "\n"
    "// Box holds a value.\n"
    "type Box[T any] struct {\n"
    "\t// Value is of type [T], the type parameter of Box; [Widget] is a type.\n"
    "\tValue T\n"
    "}\n"
    "\n"
    "// Fixture names [fixture.Data], which is test data.\n"
    "func Fixture() {}\n";

static const char GO_ALPHA_B[] =
    "package alpha\n"
    "\n"
    "// Sibling imports nothing: [beta.Thing] and [gam.Fn] are plain text here,\n"
    "// [Widget] is a link.\n"
    "func Sibling() {}\n";

static const char GO_ALPHA_C[] = "package alpha\n"
                                 "\n"
                                 "// A is a.\n"
                                 "type A struct{}\n"
                                 "\n"
                                 "// B is b.\n"
                                 "type B struct{}\n"
                                 "\n"
                                 "// Close closes A; see [Target].\n"
                                 "func (A) Close() {}\n"
                                 "\n"
                                 "// Close closes B; see [Target].\n"
                                 "func (B) Close() {}\n"
                                 "\n"
                                 "// Target is a target; see [A.Close] and [B.Close].\n"
                                 "func Target() {}\n"
                                 "\n"
                                 "// Open is a type that loses its node name to the method below.\n"
                                 "type Open struct{}\n"
                                 "\n"
                                 "// Open opens: a method named like the type.\n"
                                 "func (B) Open() {}\n"
                                 "\n"
                                 "// UsesOpen names [Open] and [B.Open].\n"
                                 "func UsesOpen() {}\n";

static const char GO_ALPHA_D[] = "package alpha\n"
                                 "\n"
                                 "import \"example.com/demo/beta\"\n"
                                 "\n"
                                 "// Alias is beta's Thing.\n"
                                 "type Alias = beta.Thing\n"
                                 "\n"
                                 "// Same is Alias again.\n"
                                 "type Same = Alias\n"
                                 "\n"
                                 "// Bare names [Alias.Do]: plain text without a package.\n"
                                 "func Bare() {}\n"
                                 "\n"
                                 "type L0 = L1\n"
                                 "type L1 = L2\n"
                                 "type L2 = L3\n"
                                 "type L3 = L4\n"
                                 "type L4 = L5\n"
                                 "type L5 = L6\n"
                                 "type L6 = L7\n"
                                 "type L7 = L8\n"
                                 "type L8 = L9\n"
                                 "type L9 = Deep10\n"
                                 "\n"
                                 "// Deep10 ends the chain.\n"
                                 "type Deep10 struct{}\n"
                                 "\n"
                                 "// End is its method.\n"
                                 "func (Deep10) End() {}\n"
                                 "\n"
                                 "type CycA = CycB\n"
                                 "type CycB = CycA\n";

static const char GO_BETA[] = "package beta\n"
                              "\n"
                              "// Thing is a thing.\n"
                              "type Thing struct {\n"
                              "\t// Field is a field.\n"
                              "\tField int\n"
                              "\tFirst, Second int\n"
                              "}\n"
                              "\n"
                              "// Do does.\n"
                              "func (t Thing) Do() {}\n"
                              "\n"
                              "// Make makes.\n"
                              "func Make() Thing { return Thing{} }\n"
                              "\n"
                              "// Embedded is embedded elsewhere.\n"
                              "type Embedded struct {\n"
                              "\t// Depth is a field of Embedded, promoted where it is embedded.\n"
                              "\tDepth int\n"
                              "}\n"
                              "\n"
                              "// Promoted is promoted through embedding.\n"
                              "func (Embedded) Promoted() {}\n"
                              "\n"
                              "// Gr\xC3\xB6\xC3\x9F"
                              "e has letters beyond ASCII.\n"
                              "type Gr\xC3\xB6\xC3\x9F"
                              "e struct{}\n"
                              "\n"
                              "// \xC3\x9C"
                              "ber starts with a capital letter beyond ASCII: exported.\n"
                              "type \xC3\x9C"
                              "ber struct{}\n"
                              "\n"
                              "var (\n"
                              "\t// InBlock has no node.\n"
                              "\tInBlock = 1\n"
                              ")\n"
                              "\n"
                              "// Doer does.\n"
                              "type Doer interface {\n"
                              "\t// Act acts.\n"
                              "\tAct()\n"
                              "}\n";

static const char GO_GAMMA_K[] =
    "package gamma\n"
    "\n"
    "import \"example.com/demo/alpha\"\n"
    "\n"
    "// ViaAlias reaches [alpha.Alias.Do] and [alpha.Same.Field] through aliases;\n"
    "// [alpha.Alias] is the alias itself, [alpha.Alias.Nope] is nothing,\n"
    "// [alpha.Widget.Promoted] is promoted and not Widget's own,\n"
    "// [alpha.Widget.Name] is a field, [alpha.Widget.Weight] a field without a\n"
    "// node, [alpha.L3.End] is seven aliases away, [alpha.L0.End] ten and\n"
    "// [alpha.CycA.End] never ends. [alpha.Helper] is test code.\n"
    "func ViaAlias() {}\n";

/* The module example.com/demo with a nested module of its own path
 * (sub/), a nested module of another path (nested/) and the packages the
 * lookup tests need. */
static void go_write_lookup_fixture(const char *repo) {
    go_w(repo, "go.mod", "module example.com/demo\n\ngo 1.22\n");
    go_w(repo, "root.go",
         "// Package demo is the root. See [alpha.Widget].\n"
         "package demo\n"
         "\n"
         "import \"example.com/demo/alpha\"\n"
         "\n"
         "// Root names [example.com/demo], [alpha], [example.com/demo/mixed.F] and\n"
         "// [example.com/demo/fmt.Println].\n"
         "func Root() {}\n");
    go_w(repo, "alpha/doc.go",
         "// Package alpha is documented here. It builds a [Widget]; printing is\n"
         "// [fmt.Println] and [nosuch.Thing] is nothing.\n"
         "// [beta.Thing] is no link here: this file imports nothing, a.go imports beta.\n"
         "package alpha\n");
    go_w(repo, "alpha/a.go", GO_ALPHA_A);
    go_w(repo, "alpha/b.go", GO_ALPHA_B);
    go_w(repo, "alpha/c.go", GO_ALPHA_C);
    go_w(repo, "alpha/d.go", GO_ALPHA_D);
    go_w(repo, "alpha/a_test.go",
         "package alpha\n"
         "\n"
         "// Helper exists only in tests.\n"
         "type Helper struct{}\n"
         "\n"
         "// TestWidget uses [Helper] and [Widget].\n"
         "func TestWidget() {}\n");
    go_w(repo, "alpha/x_test.go",
         "package alpha_test\n"
         "\n"
         "import \"example.com/demo/alpha\"\n"
         "\n"
         "// Example uses [alpha.Widget]; [Widget] alone is plain text in this package.\n"
         "func Example() {}\n");
    go_w(repo, "alpha/testdata/fixture/f.go",
         "package fixture\n\n// Data is test data; it may name [Data] itself and [Twin].\n"
         "var Data = 1\n\n// Twin is another value.\nvar Twin = 2\n");
    go_w(repo, "beta/b.go", GO_BETA);
    go_w(repo, "gamma/g.go", "package gamma\n\n// Fn is a function.\nfunc Fn() {}\n");
    go_w(repo, "gamma/h.go",
         "package gamma\n"
         "\n"
         "import \"example.com/demo/fmt\"\n"
         "\n"
         "// Local names the repository's own [fmt.Println].\n"
         "func Local() {}\n");
    go_w(repo, "gamma/k.go", GO_GAMMA_K);
    go_w(repo, "realname/r.go", "package other\n\n// Real is real.\nfunc Real() {}\n");
    go_w(repo, "fmt/f.go",
         "package fmt\n\n// Println is the repository's own, not the standard library's.\n"
         "func Println() {}\n");
    go_w(repo, "log/l.go",
         "package log\n"
         "\n"
         "// Printf prints.\n"
         "func Printf() {}\n"
         "\n"
         "// Use names its own package's [log.Printf], not the standard library's.\n"
         "func Use() {}\n");
    go_w(repo, "strings/s.go",
         "package strings\n"
         "\n"
         "import \"strings\"\n"
         "\n"
         "// Join joins.\n"
         "func Join() {}\n"
         "\n"
         "// Use imports the standard library's strings: [strings.Join] is that one.\n"
         "func Use() {}\n");
    go_w(repo, "sub/go.mod", "module example.com/demo/sub\n");
    go_w(repo, "sub/deep/d.go", "package deep\n\n// Deep is deep.\nfunc Deep() {}\n");
    go_w(repo, "nested/go.mod", "module example.com/other\n");
    go_w(repo, "nested/pkg/p.go", "package pkg\n\n// Far is far.\nfunc Far() {}\n");
    /* one import name, two packages: each file's own import decides */
    go_w(repo, "same/a.go",
         "package same\n\nimport x \"example.com/demo/beta\"\n\n// A names [x.Thing].\nfunc A() "
         "{}\n");
    go_w(
        repo, "same/b.go",
        "package same\n\nimport x \"example.com/demo/gamma\"\n\n// B names [x.Fn].\nfunc B() {}\n");
    go_w(repo, "same/doc.go",
         "// Package same: [x.Thing] is no link in a file without imports.\npackage same\n");
    go_w(repo, "mixed/a.go", "package p1\n\n// F is p1's.\nfunc F() {}\n");
    go_w(repo, "mixed/b.go", "package p2\n\n// F is p2's.\nfunc F() {}\n");
    /* stand-alone programs in one directory: no package */
    go_w(repo, "progs/one.go",
         "package main\n\n// Config is one program's.\ntype Config struct{}\n\n"
         "// Load loads a [Config].\nfunc Load() {}\n");
    go_w(repo, "progs/two.go",
         "package main\n\n// Config is another program's.\ntype Config struct{}\n\n"
         "// Save saves a [Config].\nfunc Save() {}\n");
    go_w(repo, "progs/three.go",
         "package main\n\n// Print prints a [Config], whichever.\nfunc Print() {}\n");
    /* build variants of one function */
    go_w(repo, "plat/impl_linux.go", "package plat\n\n// Open opens on Linux.\nfunc Open() {}\n");
    go_w(repo, "plat/impl_windows.go",
         "package plat\n\n// Open opens on Windows.\nfunc Open() {}\n\n"
         "// Helper calls [Open].\nfunc Helper() {}\n");
    go_w(repo, "plat/tagged_a.go",
         "//go:build a\n\npackage plat\n\n// Tagged is one variant.\nfunc Tagged() {}\n");
    go_w(repo, "plat/tagged_b.go",
         "//go:build !a\n\npackage plat\n\n// Tagged is the other variant.\nfunc Tagged() {}\n");
    go_w(repo, "plat/use.go", "package plat\n\n// Use calls [Open] and [Tagged].\nfunc Use() {}\n");
    /* a test file's variant keeps the node */
    go_w(repo, "tw/aa_test.go",
         "//go:build integration\n\npackage tw\n\n// Shared is the test double.\n"
         "func Shared() {}\n");
    go_w(repo, "tw/zz.go",
         "package tw\n\n// Shared is the product one.\nfunc Shared() {}\n\n"
         "// Caller calls [Shared].\nfunc Caller() {}\n");
    /* one name, two kinds */
    go_w(repo, "amb/x.go", "package amb\n\n// Dual is a function here.\nfunc Dual() {}\n");
    go_w(repo, "amb/y.go",
         "//go:build other\n\npackage amb\n\n// Dual is a type there.\ntype Dual struct{}\n");
    go_w(repo, "amb/z.go", "package amb\n\n// Z names [Dual].\nfunc Z() {}\n");
    /* a reference longer than a reference can be */
    char *big = malloc(2048);
    size_t w = (size_t)snprintf(big, 2048, "package long\n\n// L names [");
    memset(big + w, 'A', CBM_DOCLINK_GO_REF_MAX + 88);
    w += CBM_DOCLINK_GO_REF_MAX + 88;
    snprintf(big + w, 2048 - w,
             "] and [Other].\nfunc L() {}\n\n// Other is other.\nfunc Other() {}\n");
    go_w(repo, "long/l.go", big);
    free(big);
}

/* Write the lookup fixture below `tmp` and index it into `db`.
 *
 * Self-diagnosing: on windows-11-arm the suite once hung in local_rules before
 * the pipeline's first info line (dry run 38186491924, 2026-10-12; not
 * reproduced since). The setup therefore runs at debug level, where this
 * helper, the pipeline's setup steps and every git child name themselves, so
 * a hang ends on the step it is stuck in. */
static int go_index_lookup(char *tmp, size_t tmp_cap, char *db, size_t db_cap) {
    CBMLogLevel prev_level = cbm_log_get_level();
    cbm_log_set_level(CBM_LOG_DEBUG);
    int rc = -1;
    cbm_log_debug("go_index_lookup", "step", "mkdtemp");
    snprintf(tmp, tmp_cap, "/tmp/cbm_dm_go_XXXXXX");
    if (cbm_mkdtemp(tmp)) {
        char repo[400];
        snprintf(repo, sizeof(repo), "%s/repo", tmp);
        cbm_log_debug("go_index_lookup", "step", "fixture", "dir", tmp);
        go_write_lookup_fixture(repo);
        snprintf(db, db_cap, "%s/go.db", tmp);
        cbm_log_debug("go_index_lookup", "step", "index");
        rc = dm_index(repo, db, NULL);
    }
    cbm_log_set_level(prev_level);
    return rc;
}

/* ── resolver: every reference form, in its first lookup level ───── */

TEST(doc_mentions_go_forms) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    static const go_want_edge_t edges[] = {
        /* [Name], [Recv.Method], an interface's method, a constant, a variable */
        {"alpha.Widget", "alpha.New", "unique"},
        {"alpha.Widget", "alpha.Run", "unique"},
        {"alpha.Widget", "alpha.Runner.Run", "unique"},
        {"alpha.Widget", "alpha.Max", "unique"},
        {"alpha.Widget", "alpha.Default", "unique"},
        {"alpha.New", "alpha.Widget", "unique"},
        /* an interface method and a field are sources */
        {"alpha.Runner.Run", "alpha.Widget", "unique"},
        /* [pkg.Name]: the file's import, its renamed import, the real package
         * name of an import whose directory is called otherwise, a nested
         * module, a module with another path */
        {"alpha.New", "beta.Make", "exact"},
        {"alpha.New", "gamma.Fn", "exact"},
        {"alpha.New", "realname.Real", "exact"},
        {"alpha.New", "sub.deep.Deep", "exact"},
        {"alpha.New", "nested.pkg.Far", "exact"},
        /* [pkg.Recv.Name]: a method, a field, an interface method */
        {"alpha.New", "beta.Do", "exact"},
        {"alpha.New", "beta.Thing.Field", "exact"},
        {"alpha.New", "beta.Doer.Act", "exact"},
        /* an identifier with letters beyond ASCII */
        {"alpha.New",
         "beta.Gr\xC3\xB6\xC3\x9F"
         "e",
         "exact"},
        {"alpha.New",
         "beta.\xC3\x9C"
         "ber",
         "exact"},
        /* [pkg]: the package's directory node */
        {"alpha.New", "beta", "exact"},
        /* an import path */
        {"Root", "fmt.Println", "exact"},
        {"Root", "alpha", "exact"},
        /* test code may name test code; an external test package imports */
        {"alpha.TestWidget", "alpha.Helper", "unique"},
        {"alpha.TestWidget", "alpha.Widget", "unique"},
        {"alpha.Example", "alpha.Widget", "exact"},
        {"fixture.Data", "fixture.Twin", "unique"}, /* inside a testdata directory */
    };
    ASSERT_EQ(go_bad_edges(db, edges, GO_COUNT(edges)), 0);
    /* the edge's properties, in full */
    char props[512];
    int n = 0;
    dm_edge(db, "alpha.New", "beta.Make", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(props, "{\"via\":\"doc_comment\",\"syntax\":\"doc_link\",\"tier\":\"exact\","
                         "\"line\":25,\"count\":1}");
    /* 12 targets; [bad.T] and [beta.Thing.Second] are rows */
    ASSERT_EQ(dm_mentions_from(db, "alpha.New"), 12);
    /* a [Text] with a link definition is the href family: a row, never an edge */
    char reason[64];
    char syntax[64];
    dm_row(db, "alpha/a.go", "https://example.com/spec", reason, sizeof(reason), syntax,
           sizeof(syntax));
    ASSERT_STR_EQ(reason, "external");
    ASSERT_STR_EQ(syntax, "href");
    ASSERT_EQ(dm_mentions_from(db, "alpha.Spec"), 0);
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: text go/doc renders as text is neither edge nor row ── */

TEST(doc_mentions_go_plain_text) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    const char *a = "alpha/a.go";
    static const go_want_row_t rows[] = {
        /* a name the package does not declare, although an imported package
         * or a sibling directory does */
        {"alpha/a.go", "Make", ""},
        {"alpha/a.go", "Thing", ""},
        {"alpha/a.go", "Nope", ""},
        /* a qualifier that is no import name of the file: the directory name
         * of a package called otherwise, the path element of a renamed
         * import, a word */
        {"alpha/a.go", "realname.Real", ""},
        {"alpha/a.go", "gamma.Fn", ""},
        {"alpha/a.go", "nosuch.Name", ""},
        /* members the type does not declare itself: a method and a field
         * promoted through an embedded type, a name nobody declares */
        {"alpha/a.go", "Widget.Promoted", ""},
        {"alpha/a.go", "Widget.Depth", ""},
        {"alpha/a.go", "Widget.Unknown", ""},
        /* a qualified name with a byte this scan cannot classify */
        {"alpha/a.go", "beta.Fehlt_\xC3\x9F", ""},
        /* the file's own imports only: a sibling's import is no qualifier */
        {"alpha/b.go", "beta.Thing", ""},
        {"alpha/b.go", "gam.Fn", ""},
        /* a member through an alias, without a package */
        {"alpha/d.go", "Alias.Do", ""},
        /* another package clause in the same directory is another package */
        {"alpha/x_test.go", "Widget", ""},
        {"alpha/doc.go", "nosuch.Thing", ""},
    };
    ASSERT_EQ(go_bad_rows(db, rows, GO_COUNT(rows)), 0);
    /* and none of them is an edge: Run's doc holds rows, plain text and one
     * link, to the field Widget.Size */
    ASSERT_EQ(dm_mentions_from(db, "alpha.Run"), 1);
    ASSERT_EQ(dm_mentions_from(db, "alpha.Bare"), 0);
    static const go_want_edge_t edges[] = {
        {"alpha.Sibling", "alpha.Widget", "unique"},
        {"alpha.Sibling", "beta.Thing", ""},
        {"alpha.Sibling", "gamma.Fn", ""},
        {"alpha.Example", "alpha.Widget", "exact"},
    };
    ASSERT_EQ(go_bad_edges(db, edges, GO_COUNT(edges)), 0);
    ASSERT_EQ(dm_mentions_from(db, "alpha.Sibling"), 1);
    ASSERT_STR_EQ(go_reason(db, a, "beta.Nope"), "missing"); /* the fixture is the one meant */
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: struct fields ─────────────────────────────────────── */

/* [Type.Field] binds the field written in that type's own struct declaration,
 * with or without a package. (Without one, go/doc itself is believed to
 * render it as plain text: a deliberate difference.) Nothing promoted, and
 * nothing for a name the type does not declare. */
TEST(doc_mentions_go_struct_fields) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    static const go_want_edge_t edges[] = {
        /* the same package: a field named from a method's doc and from a
         * sibling field's doc */
        {"alpha.Run", "alpha.Widget.Size", "exact"},
        {"alpha.Widget.Name", "alpha.Widget.Size", "exact"},
        /* another package, by its import */
        {"alpha.New", "beta.Thing.Field", "exact"},
        {"gamma.ViaAlias", "alpha.Widget.Name", "exact"},
        /* Depth is a field of beta.Embedded, which Widget embeds: it is no
         * field of Widget */
        {"alpha.Run", "beta.Embedded.Depth", ""},
    };
    ASSERT_EQ(go_bad_edges(db, edges, GO_COUNT(edges)), 0);
    /* the promoted field exists as a node: the test above is not vacuous */
    ASSERT_EQ(dm_count(db,
                       "SELECT COUNT(*) FROM nodes WHERE label = 'Field' AND qualified_name LIKE "
                       "'%.beta.Embedded.Depth'"),
              1);
    static const go_want_row_t rows[] = {
        {"alpha/a.go", "Widget.Size", ""},     /* an edge, no row */
        {"alpha/a.go", "Widget.Depth", ""},    /* promoted: plain text */
        {"alpha/a.go", "Widget.Promoted", ""}, /* a promoted method: plain text */
        {"alpha/a.go", "Widget.Unknown", ""},  /* nobody's: plain text */
        /* a field the graph has no node for is a gap, not plain text: the
         * second name of one declaration, and the embedded field itself */
        {"alpha/a.go", "Widget.Weight", "graph_gap"},
        {"alpha/a.go", "Widget.Embedded", "graph_gap"},
        {"gamma/k.go", "alpha.Widget.Promoted", "missing"}, /* with a package: a broken link */
    };
    ASSERT_EQ(go_bad_rows(db, rows, GO_COUNT(rows)), 0);
    ASSERT_EQ(dm_mentions_from(db, "alpha.Widget.Name"), 1);
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: the package of a qualifier is the FILE's import ───── */

/* The limit of this resolver against go/doc: a qualifier is looked up in the
 * imports of the documented file only, not in the union of the imports of
 * the package's files. A package comment in a file without imports (the
 * usual doc.go) therefore cannot name what only a sibling file imports: no
 * edge and no row. The other side of the same rule: where two files import
 * different packages under one name, go/doc links neither, and here each
 * file's links go through its own import. */
TEST(doc_mentions_go_file_only_package_lookup) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    static const go_want_row_t rows[] = {
        {"alpha/doc.go", "beta.Thing", ""}, /* a.go imports beta, doc.go nothing */
        {"same/doc.go", "x.Thing", ""},
        {"alpha/b.go", "beta.Thing", ""},
        {"alpha/b.go", "gam.Fn", ""},
    };
    ASSERT_EQ(go_bad_rows(db, rows, GO_COUNT(rows)), 0);
    static const go_want_edge_t edges[] = {
        {"alpha.doc.go.__file__", "beta.Thing", ""},
        {"same.doc.go.__file__", "beta.Thing", ""},
        {"alpha.Sibling", "beta.Thing", ""},
        /* the file that does import it links it */
        {"alpha.New", "beta.Make", "exact"},
        /* one name, two packages: the file's own import decides */
        {"same.A", "beta.Thing", "exact"},
        {"same.B", "gamma.Fn", "exact"},
        {"same.A", "gamma.Fn", ""},
        {"same.B", "beta.Thing", ""},
    };
    ASSERT_EQ(go_bad_edges(db, edges, GO_COUNT(edges)), 0);
    /* the package comment's one link is the one to its own package's type */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "WHERE e.type = 'MENTIONS' AND s.label = 'File' "
                           "AND s.file_path IN ('alpha/doc.go', 'same/doc.go')"),
              1);
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: the local rules ───────────────────────────────────── */

TEST(doc_mentions_go_local_rules) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    static const go_want_edge_t edges[] = {
        /* the function's own type parameter shadows the package's type T */
        {"alpha.Map", "alpha.T", ""},
        {"alpha.Map", "alpha.Widget", "unique"},
        /* a function without that type parameter means the type */
        {"alpha.Plain", "alpha.T", "unique"},
        /* a generic type's parameter is in scope of its fields' docs too */
        {"alpha.Box.Value", "alpha.T", ""},
        {"alpha.Box.Value", "alpha.Widget", "unique"},
    };
    ASSERT_EQ(go_bad_edges(db, edges, GO_COUNT(edges)), 0);
    ASSERT_STR_EQ(go_reason(db, "alpha/a.go", "T"), "");
    ASSERT_EQ(dm_mentions_from(db, "alpha.Map"), 1);
    ASSERT_EQ(dm_mentions_from(db, "alpha.Box.Value"), 1);
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: one test per level boundary of the package lookup ─── */

TEST(doc_mentions_go_lookup_order) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    /* 1. the file's import beats the package's own name: package strings
     * imports the standard library's strings */
    ASSERT_STR_EQ(go_reason(db, "strings/s.go", "strings.Join"), "external");
    ASSERT_STR_EQ(go_tier(db, "strings.Use", "strings.Join"), "");
    /* 2. the package's own name beats the standard-library list: package log
     * without an import of log */
    ASSERT_STR_EQ(go_tier(db, "log.Use", "log.Printf"), "exact");
    ASSERT_STR_EQ(go_reason(db, "log/l.go", "log.Printf"), "");
    /* 3. the standard-library list is the last level: [io] without an import */
    ASSERT_STR_EQ(go_reason(db, "alpha/a.go", "io"), "external");
    /* 4. an import decides by its PATH: fmt is the standard library's in a
     * file that imports "fmt", the repository's own in a file that imports
     * example.com/demo/fmt */
    ASSERT_STR_EQ(go_reason(db, "alpha/a.go", "fmt.Println"), "external");
    ASSERT_STR_EQ(go_tier(db, "alpha.Run", "fmt.Println"), "");
    ASSERT_STR_EQ(go_tier(db, "gamma.Local", "fmt.Println"), "exact");
    /* 5. the longest module path decides: a directory inside the root module
     * that belongs to a nested module of another path is not reachable through
     * the root module's path */
    ASSERT_STR_EQ(go_reason(db, "alpha/a.go", "example.com/demo/nested/pkg.Far"), "missing");
    ASSERT_STR_EQ(go_tier(db, "alpha.New", "nested.pkg.Far"), "exact");
    ASSERT_STR_EQ(go_tier(db, "alpha.New", "sub.deep.Deep"), "exact");
    /* 6. the bare name stops at the package: beta.Make is imported and still
     * no [Make] */
    ASSERT_STR_EQ(go_tier(db, "alpha.Run", "beta.Make"), "");
    /* 7. members stop at the type: nothing promoted through embedding */
    ASSERT_STR_EQ(go_reason(db, "gamma/k.go", "alpha.Widget.Promoted"), "missing");
    ASSERT_STR_EQ(go_tier(db, "gamma.ViaAlias", "beta.Promoted"), "");
    /* 8. an alias is followed for a member written with its package, to the
     * type that declares the member */
    static const go_want_edge_t alias[] = {
        {"gamma.ViaAlias", "beta.Do", "exact"},
        {"gamma.ViaAlias", "beta.Thing.Field", "exact"},
        {"gamma.ViaAlias", "alpha.Alias", "exact"},
        {"gamma.ViaAlias", "alpha.End", "exact"}, /* seven aliases away */
        {"gamma.ViaAlias", "alpha.Widget.Name", "exact"},
    };
    ASSERT_EQ(go_bad_edges(db, alias, GO_COUNT(alias)), 0);
    ASSERT_STR_EQ(go_reason(db, "gamma/k.go", "alpha.Alias.Nope"), "missing");
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: one test per reason ───────────────────────────────── */

TEST(doc_mentions_go_reasons) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    char long_raw[CBM_DOCLINK_GO_REF_MAX + 2];
    memset(long_raw, 'A', CBM_DOCLINK_GO_REF_MAX + 1);
    long_raw[CBM_DOCLINK_GO_REF_MAX + 1] = '\0';
    const go_want_row_t rows[] = {
        /* external: the standard library by import, a dependency by import,
         * the standard library by path */
        {"alpha/a.go", "fmt.Println", "external"},
        {"alpha/a.go", "ext.Thing", "external"},
        {"alpha/a.go", "encoding/json.Marshal", "external"},
        /* missing: a repository package that does not declare the name or the
         * member; a path inside a module with no package there */
        {"alpha/a.go", "beta.Nope", "missing"},
        {"alpha/a.go", "beta.Thing.Nope", "missing"},
        {"alpha/a.go", "example.com/demo/nodir.X", "missing"},
        /* graph_gap: declared, but without a node of its own: a variable of a
         * var block, a later name of a field declaration, the root package
         * (the Project node is no package node) */
        {"alpha/a.go", "Block", "graph_gap"},
        {"alpha/a.go", "beta.InBlock", "graph_gap"},
        {"alpha/a.go", "beta.Thing.Second", "graph_gap"},
        {"gamma/k.go", "alpha.Widget.Weight", "graph_gap"},
        {"root.go", "example.com/demo", "graph_gap"},
        /* test_only_target: product code naming what only test files declare,
         * or what a testdata directory holds */
        {"alpha/a.go", "Helper", "test_only_target"},
        {"gamma/k.go", "alpha.Helper", "test_only_target"},
        {"alpha/a.go", "fixture.Data", "test_only_target"},
        /* ambiguous: one name, a function here and a type there; one
         * directory, two packages, and nothing that picks one */
        {"amb/z.go", "Dual", "ambiguous"},
        {"root.go", "example.com/demo/mixed.F", "ambiguous"},
        /* unparseable: an import path the scan could not record, an alias
         * chain that is too long or never ends, a reference that is too long
         * (never resolved by a prefix) */
        {"alpha/a.go", "bad.T", "unparseable"},
        {"gamma/k.go", "alpha.L0.End", "unparseable"},
        {"gamma/k.go", "alpha.CycA.End", "unparseable"},
        {"long/l.go", long_raw, "unparseable"},
    };
    ASSERT_EQ(go_bad_rows(db, rows, GO_COUNT(rows)), 0);
    ASSERT_STR_EQ(go_tier(db, "long.L", "long.Other"), "unique");
    ASSERT_EQ(dm_mentions_from(db, "long.L"), 1);
    /* every row of the fixture is a Go doc link or a URL */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE syntax NOT IN "
                           "('doc_link','href')"),
              0);
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: one node name, several declarations ───────────────── */

TEST(doc_mentions_go_node_owner) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    /* Go method nodes have a flat name: A.Close and B.Close share
     * alpha.Close, and the later declaration keeps it */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE qualified_name LIKE '%.alpha.Close'"),
              1);
    ASSERT_STR_EQ(go_tier(db, "alpha.Target", "alpha.Close"), "unique");
    ASSERT_STR_EQ(go_reason(db, "alpha/c.go", "A.Close"), "graph_gap");
    ASSERT_STR_EQ(go_reason(db, "alpha/c.go", "B.Close"), "");
    /* the doc of the method that lost the name is not attributed to the one
     * that kept it: B.Close's [Target] is the edge, A.Close's a row */
    char props[512];
    int n = 0;
    dm_edge(db, "alpha.Close", "alpha.Target", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"line\":12,\"count\":1"));
    ASSERT_EQ(dm_count(db, "SELECT line FROM doc_link_unresolved WHERE rel_path = 'alpha/c.go' "
                           "AND raw = 'Target'"),
              9);
    /* a type that lost its name to a method of that name is no target, the
     * method is */
    ASSERT_STR_EQ(go_reason(db, "alpha/c.go", "Open"), "graph_gap");
    ASSERT_STR_EQ(go_tier(db, "alpha.UsesOpen", "alpha.Open"), "unique");
    ASSERT_EQ(dm_mentions_from(db, "alpha.UsesOpen"), 1);

    /* build variants are one entity: whichever file holds the node */
    static const go_want_edge_t variants[] = {
        {"plat.Use", "plat.Open", "unique"},
        {"plat.Use", "plat.Tagged", "unique"},
        {"plat.Helper", "plat.Open", "unique"},
    };
    ASSERT_EQ(go_bad_edges(db, variants, GO_COUNT(variants)), 0);

    /* two unconstrained files that declare one name are no package: the name
     * means the file's own declaration, and nothing from elsewhere */
    ASSERT_STR_EQ(go_tier(db, "progs.Load", "progs.Config"), "unique");
    ASSERT_STR_EQ(go_reason(db, "progs/two.go", "Config"), "graph_gap");
    ASSERT_STR_EQ(go_tier(db, "progs.Save", "progs.Config"), "");
    ASSERT_STR_EQ(go_reason(db, "progs/three.go", "Config"), "ambiguous");
    ASSERT_STR_EQ(go_tier(db, "progs.Print", "progs.Config"), "");

    /* a node kept by a test file's variant is no target for product code */
    ASSERT_STR_EQ(go_reason(db, "tw/zz.go", "Shared"), "graph_gap");
    ASSERT_STR_EQ(go_tier(db, "tw.Caller", "tw.Shared"), "");
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: the package comment, from the File node ───────────── */

TEST(doc_mentions_go_package_comment) {
    char tmp[256];
    char db[512];
    ASSERT_EQ(go_index_lookup(tmp, sizeof(tmp), db, sizeof(db)), 0);
    /* a reference of the package comment is an edge from the File node */
    char props[512];
    int n = 0;
    dm_edge(db, "alpha.doc.go.__file__", "alpha.Widget", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(props, "{\"via\":\"doc_comment\",\"syntax\":\"doc_link\",\"tier\":\"unique\","
                         "\"line\":1,\"count\":1}");
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                           "WHERE e.type = 'MENTIONS' AND s.label = 'File' "
                           "AND s.file_path = 'alpha/doc.go'"),
              1);
    /* ... or a row with the file's path and the reference's line */
    ASSERT_STR_EQ(go_reason(db, "alpha/doc.go", "fmt.Println"), "external");
    ASSERT_EQ(dm_count(db, "SELECT line FROM doc_link_unresolved WHERE rel_path = 'alpha/doc.go' "
                           "AND raw = 'fmt.Println'"),
              2);
    /* a file with imports: its package comment reads them */
    ASSERT_STR_EQ(go_tier(db, "root.go.__file__", "alpha.Widget"), "exact");
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── resolver: the repository's own standard library ─────────────── */

TEST(doc_mentions_go_std_module) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_gostd_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    go_w(repo, "src/go.mod", "module std\n\ngo 1.22\n");
    go_w(repo, "src/fmt/print.go", "package fmt\n\n// Println prints.\nfunc Println() {}\n");
    go_w(repo, "src/os/exec/exec.go", "package exec\n\n// Cmd is a command.\ntype Cmd struct{}\n");
    go_w(repo, "src/log/doc.go",
         "// Package log links [os/exec.Cmd] by its path and [fmt.Println] by the\n"
         "// standard library's own name.\n"
         "package log\n");
    go_w(repo, "src/log/log.go",
         "package log\n"
         "\n"
         "import (\n"
         "\t\"fmt\"\n"
         "\t\"os/exec\"\n"
         ")\n"
         "\n"
         "// Print uses [fmt.Println], [exec.Cmd] and [fmt.Missing].\n"
         "func Print() {}\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/std.db", tmp);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* a package of `module std` is imported by its bare directory path */
    static const go_want_edge_t edges[] = {
        {"log.Print", "fmt.Println", "exact"},
        {"log.Print", "exec.Cmd", "exact"},
        {"log.doc.go.__file__", "exec.Cmd", "exact"},
        {"log.doc.go.__file__", "fmt.Println", "exact"},
    };
    ASSERT_EQ(go_bad_edges(db, edges, GO_COUNT(edges)), 0);
    ASSERT_STR_EQ(go_reason(db, "src/log/log.go", "fmt.Missing"), "missing");
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── the ship gate ───────────────────────────────────────────────── */

TEST(doc_mentions_go_ship_gate) {
    char tmp[256];
    char db[512];
    cbm_doclink_test_set_ships(CBM_DOCLINK_GO_DOC_LINK, false);
    int rc = go_index_lookup(tmp, sizeof(tmp), db, sizeof(db));
    cbm_doclink_test_reset_ships(); /* production gates again for the tests after this one */
    dm_ship_held_families();
    ASSERT_EQ(rc, 0);
    /* a resolved doc link is a row with the gate's reason and no edge */
    ASSERT_STR_EQ(go_tier(db, "alpha.New", "beta.Make"), "");
    ASSERT_STR_EQ(go_reason(db, "alpha/a.go", "beta.Make"), "below_bar_tier");
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'"), 0);
    /* one that does not resolve keeps its own reason; plain text stays nothing */
    ASSERT_STR_EQ(go_reason(db, "alpha/a.go", "beta.Nope"), "missing");
    ASSERT_STR_EQ(go_reason(db, "alpha/a.go", "Make"), "");
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── incremental == full ─────────────────────────────────────────── */

#define GO_INC_A_HEAD "package a\n\nimport \"example.com/inc/b\"\n\n"
#define GO_INC_A_LOCAL "// Local is local.\nfunc Local() {}\n"
static const char GO_INC_A[] = GO_INC_A_HEAD
    "// Use names [b.Target], [b.Gone], [b.T.M], [b.W.Name], [b.V.Name] and [Local].\n"
    "func Use() {}\n\n" GO_INC_A_LOCAL;
static const char GO_INC_A_DOC_EDIT[] =
    GO_INC_A_HEAD "// Use names [b.Target], [b.Gone], [b.T.M], [b.W.Name], [b.V.Name], [Local]\n"
                  "// and now [b.T].\n"
                  "func Use() {}\n\n" GO_INC_A_LOCAL;
static const char GO_INC_OTHER[] = "package a\n\n// Other names [b.Target].\nfunc Other() {}\n";
static const char GO_INC_OTHER_IMPORT[] =
    "package a\n\nimport \"example.com/inc/b\"\n\n// Other names [b.Target].\nfunc Other() {}\n";
static const char GO_INC_DOC[] = "// Package a is documented. See [Local].\npackage a\n";
static const char GO_INC_DOC_EDIT[] =
    "// Package a is documented. See [Local] and [Use].\npackage a\n";

#define GO_INC_B_TARGET "// Target is the target.\nfunc Target() {}\n\n"
#define GO_INC_B_TARGET_BODY "// Target is the target.\nfunc Target() {\n\t_ = 1\n}\n\n"
#define GO_INC_B_RENAMED "// Renamed was the target.\nfunc Renamed() {}\n\n"
#define GO_INC_B_GONE "// Gone will go.\nfunc Gone() {}\n\n"
#define GO_INC_B_T "// T is a type.\ntype T struct{}\n\n"
#define GO_INC_B_TM "// M is a method of T.\nfunc (T) M() {}\n"
static const char GO_INC_B[] = "package b\n\n" GO_INC_B_TARGET GO_INC_B_GONE GO_INC_B_T GO_INC_B_TM;
static const char GO_INC_B_BODY[] =
    "package b\n\n" GO_INC_B_TARGET_BODY GO_INC_B_GONE GO_INC_B_T GO_INC_B_TM;
static const char GO_INC_B_NO_GONE[] = "package b\n\n" GO_INC_B_TARGET_BODY GO_INC_B_T GO_INC_B_TM;
static const char GO_INC_B_NO_M[] = "package b\n\n" GO_INC_B_TARGET_BODY GO_INC_B_T;
static const char GO_INC_B_RENAME[] = "package b\n\n" GO_INC_B_RENAMED GO_INC_B_T;
static const char GO_INC_C[] = "package b\n\n// U is another type.\ntype U struct{}\n\n"
                               "// M is U's method; see [Target].\nfunc (U) M() {}\n";
#define GO_INC_D_HEAD "package b\n\n// V is v.\ntype V struct{}\n\n// W is w.\ntype W struct{}\n\n"
#define GO_INC_D_V "// Name of V.\nfunc (V) Name() {}\n\n"
#define GO_INC_D_W "// Name of W.\nfunc (W) Name() {}\n\n"
static const char GO_INC_D[] = GO_INC_D_HEAD GO_INC_D_V GO_INC_D_W;
static const char GO_INC_D_SWAPPED[] = GO_INC_D_HEAD GO_INC_D_W GO_INC_D_V;

TEST(doc_mentions_go_incremental_equals_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_goinc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    go_w(repo, "go.mod", "module example.com/inc\n\ngo 1.22\n");
    go_w(repo, "a/a.go", GO_INC_A);
    go_w(repo, "a/doc.go", GO_INC_DOC);
    go_w(repo, "a/other.go", GO_INC_OTHER);
    go_w(repo, "b/b.go", GO_INC_B);
    go_w(repo, "b/c.go", GO_INC_C);
    go_w(repo, "b/d.go", GO_INC_D);
    char inc_db[512];
    char full_db[512];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    /* the preconditions the steps below move away from */
    static const go_want_edge_t before[] = {
        {"a.Use", "b.Target", "exact"}, {"a.Use", "b.Gone", "exact"},
        {"a.Use", "b.M", "exact"},    /* T.M, in the first file of the directory, keeps b.M */
        {"a.Use", "b.Name", "exact"}, /* W.Name, the later declaration, keeps b.Name */
        {"a.Use", "a.Local", "unique"}, {"a.doc.go.__file__", "a.Local", "unique"},
        {"a.Other", "b.Target", ""}, /* other.go does not import b */
    };
    ASSERT_EQ(go_bad_edges(inc_db, before, GO_COUNT(before)), 0);
    ASSERT_STR_EQ(go_reason(inc_db, "a/a.go", "b.V.Name"), "graph_gap");
    ASSERT_STR_EQ(go_reason(inc_db, "b/c.go", "Target"), "graph_gap"); /* U.M has no node */

    /* a doc-comment edit re-extracts the file alone */
    go_w(repo, "a/a.go", GO_INC_A_DOC_EDIT);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "doc edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_STR_EQ(go_tier(inc_db, "a.Use", "b.T"), "exact");

    /* a body edit in a file other files' references point into: its scope is
     * unchanged, and the edges into it stand */
    go_w(repo, "b/b.go", GO_INC_B_BODY);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "target body edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
        0);
    ASSERT_STR_EQ(go_tier(inc_db, "a.Use", "b.Target"), "exact");

    /* a file's imports are its own business: an import edit repairs the file */
    go_w(repo, "a/other.go", GO_INC_OTHER_IMPORT);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "import added", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    ASSERT_STR_EQ(go_tier(inc_db, "a.Other", "b.Target"), "exact");

    /* a reference added to the package comment: an edge from the File node */
    go_w(repo, "a/doc.go", GO_INC_DOC_EDIT);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "package comment", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_STR_EQ(go_tier(inc_db, "a.doc.go.__file__", "a.Use"), "unique");
    ASSERT_STR_EQ(go_tier(inc_db, "a.doc.go.__file__", "a.Local"), "unique");

    /* a removed function: the files with an edge into the changed file
     * re-resolve */
    go_w(repo, "b/b.go", GO_INC_B_NO_GONE);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "function removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
        0);
    ASSERT_STR_EQ(go_reason(inc_db, "a/a.go", "b.Gone"), "missing");

    /* the method that kept a shared node name goes: the other file's method of
     * that name takes the node over, and its doc becomes a source */
    go_w(repo, "b/b.go", GO_INC_B_NO_M);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "name keeper removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
        0);
    ASSERT_STR_EQ(go_reason(inc_db, "a/a.go", "b.T.M"), "missing");
    ASSERT_STR_EQ(go_tier(inc_db, "b.M", "b.Target"), "unique");
    ASSERT_STR_EQ(go_reason(inc_db, "b/c.go", "Target"), "");

    /* declaration order decides which declaration keeps a shared name:
     * swapping two methods moves the node, for files with no edge into it too */
    go_w(repo, "b/d.go", GO_INC_D_SWAPPED);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "methods swapped", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    ASSERT_STR_EQ(go_reason(inc_db, "a/a.go", "b.W.Name"), "graph_gap");
    ASSERT_STR_EQ(go_reason(inc_db, "a/a.go", "b.V.Name"), "");

    /* renaming a target */
    go_w(repo, "b/b.go", GO_INC_B_RENAME);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "rename", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_STR_EQ(go_reason(inc_db, "a/a.go", "b.Target"), "missing");
    ASSERT_STR_EQ(go_reason(inc_db, "a/other.go", "b.Target"), "missing");

    /* a deleted file takes its declarations along */
    ASSERT_EQ(unlink(TH_PATH(repo, "b/d.go")), 0);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "delete file", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_STR_EQ(go_reason(inc_db, "a/a.go", "b.V.Name"), "missing");

    /* the module path names every package of the repository */
    go_w(repo, "go.mod", "module example.com/moved\n\ngo 1.22\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "module path", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_STR_EQ(go_reason(inc_db, "a/a.go", "b.T"), "external");

    dm_unlink_db(inc_db);
    dm_unlink_db(full_db);
    th_rmtree(tmp);
    PASS();
}

/* ── which scope changes are repairable file by file ─────────────── */

#define GO_DELTA_HEAD "package p\n\nimport \"example.com/m/q\"\n\n"
#define GO_DELTA_F "// F is.\nfunc F() {}\n\n"
#define GO_DELTA_G "// G is.\nfunc G() {}\n\n"
#define GO_DELTA_T "type T struct {\n\tA int\n\tB int\n}\n\n"
#define GO_DELTA_T_NO_B "type T struct {\n\tA int\n}\n\n"
#define GO_DELTA_M "func (T) M() {}\n\n"
#define GO_DELTA_I "type I interface {\n\tDo()\n\tUndo()\n}\n\n"
#define GO_DELTA_I_NO_UNDO "type I interface {\n\tDo()\n}\n\n"
#define GO_DELTA_C "const C = 1\n"
#define GO_DELTA_V "var V = 2\n"
#define GO_DELTA_REST GO_DELTA_T GO_DELTA_M GO_DELTA_I GO_DELTA_C GO_DELTA_V

TEST(doc_mentions_go_scope_delta_rules) {
    const char *base = GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_REST;
    const char *twins = "package p\n\nfunc Open() {}\nfunc (T) Open() {}\n";
    const char *alias = "package p\n\nimport \"example.com/m/q\"\n\ntype X = q.Y\n";
    const struct {
        const char *what;
        const char *before;
        const char *after;
        int want;
        const char *names;
    } cases[] = {
        {"unchanged", base, base, CBM_DOCLINK_DELTA_LOCAL, ""},
        /* nothing of the persisted scope carries a line number or a body */
        {"lines moved", base, "// moved\n\n\n" GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_REST,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"body and doc edited", base,
         GO_DELTA_HEAD "// F is [G].\nfunc F() {\n\t_ = 1\n}\n\n" GO_DELTA_G GO_DELTA_REST,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"declarations reordered", base, GO_DELTA_HEAD GO_DELTA_G GO_DELTA_F GO_DELTA_REST,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"unexported declaration added", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G "func helper() {}\n\n" GO_DELTA_REST,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        /* the file's imports scope the file itself */
        {"import added", base,
         "package p\n\nimport (\n\t\"example.com/m/q\"\n\t\"example.com/m/r\"\n)\n\n" GO_DELTA_F
             GO_DELTA_G GO_DELTA_REST,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"import removed", base, "package p\n\n" GO_DELTA_F GO_DELTA_G GO_DELTA_REST,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"import renamed", base,
         "package p\n\nimport qq \"example.com/m/q\"\n\n" GO_DELTA_F GO_DELTA_G GO_DELTA_REST,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        /* a declaration that only LEFT: its name is reported */
        {"function removed", base, GO_DELTA_HEAD GO_DELTA_F GO_DELTA_REST, CBM_DOCLINK_DELTA_LOCAL,
         "G"},
        {"method removed", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_T GO_DELTA_I GO_DELTA_C GO_DELTA_V,
         CBM_DOCLINK_DELTA_LOCAL, "M"},
        {"field removed", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_T_NO_B GO_DELTA_M GO_DELTA_I GO_DELTA_C
             GO_DELTA_V,
         CBM_DOCLINK_DELTA_LOCAL, "B"},
        {"interface method removed", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_T GO_DELTA_M GO_DELTA_I_NO_UNDO GO_DELTA_C
             GO_DELTA_V,
         CBM_DOCLINK_DELTA_LOCAL, "Undo"},
        {"constant removed", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_T GO_DELTA_M GO_DELTA_I GO_DELTA_V,
         CBM_DOCLINK_DELTA_LOCAL, "C"},
        /* anything that appears or changes can re-route references in files
         * with no edge into this one */
        {"function added", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G "func H() {}\n\n" GO_DELTA_REST,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"type removed", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_T GO_DELTA_M GO_DELTA_C GO_DELTA_V,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"type changes its kind", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_T GO_DELTA_M
         "type I struct {\n\tDo int\n\tUndo int\n}\n\n" GO_DELTA_C GO_DELTA_V,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"package clause renamed", base,
         "package other\n\nimport \"example.com/m/q\"\n\n" GO_DELTA_F GO_DELTA_G GO_DELTA_REST,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"build constraint added", base,
         "//go:build linux\n\n" GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_REST,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"variable moved into a block (it loses its node)", base,
         GO_DELTA_HEAD GO_DELTA_F GO_DELTA_G GO_DELTA_T GO_DELTA_M GO_DELTA_I GO_DELTA_C
         "var (\n\tV = 2\n)\n",
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* the declaration that keeps a shared node name changes */
        {"same-named declarations swapped", twins,
         "package p\n\nfunc (T) Open() {}\nfunc Open() {}\n", CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"one of two same-named declarations removed", twins, "package p\n\nfunc Open() {}\n",
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* other files follow `type X = q.Y` through this file's imports */
        {"import changed under a qualified alias", alias,
         "package p\n\nimport q \"example.com/m/other\"\n\ntype X = q.Y\n",
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* a file that appears or goes */
        {"file deleted", base, NULL, CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"file added", NULL, base, CBM_DOCLINK_DELTA_GLOBAL, NULL},
    };
    for (size_t i = 0; i < GO_COUNT(cases); i++) {
        char names[256];
        int got = dm_scope_delta(CBM_LANG_GO, "p/x.go", cases[i].before, cases[i].after, names,
                                 sizeof(names));
        bool names_ok = !cases[i].names || strcmp(names, cases[i].names) == 0;
        if (got != cases[i].want || !names_ok) {
            printf("  %s: delta %d names \"%s\", want %d \"%s\"\n", cases[i].what, got, names,
                   cases[i].want, cases[i].names ? cases[i].names : "(any)");
            FAIL("scope delta");
        }
    }
    /* go.mod: the module path is everybody's scope; the rest of the file is not */
    char names[256];
    ASSERT_EQ(dm_scope_delta(CBM_LANG_GOMOD, "go.mod", "module example.com/a\n",
                             "module example.com/b\n", names, sizeof(names)),
              CBM_DOCLINK_DELTA_GLOBAL);
    ASSERT_EQ(dm_scope_delta(CBM_LANG_GOMOD, "go.mod", "module example.com/a\n\ngo 1.21\n",
                             "module example.com/a\n\ngo 1.22\n\nrequire example.com/x v1.0.0\n",
                             names, sizeof(names)),
              CBM_DOCLINK_DELTA_LOCAL);
    PASS();
}

/* ── one worker == several workers ───────────────────────────────── */

TEST(doc_mentions_go_workers_agree) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_gopar_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    go_w(repo, "go.mod", "module example.com/par\n\ngo 1.22\n");
    enum { PKGS = 12, FILES = 6 }; /* 72 files: past the parallel threshold */
    for (int p = 0; p < PKGS; p++) {
        for (int f = 0; f < FILES; f++) {
            char rel[64];
            char src[2048];
            int next = (p + 1) % PKGS;
            snprintf(rel, sizeof(rel), "p%02d/f%d.go", p, f);
            snprintf(src, sizeof(src),
                     "package p%02d\n"
                     "\n"
                     "import \"example.com/par/p%02d\"\n"
                     "\n"
                     "// T%d is a type; see [F%d], [T%d.Close], [p%02d.T%d], [p%02d.T%d.Close],\n"
                     "// [p%02d.Gone], [fmt.Println], [Shared] and [Nothing].\n"
                     "type T%d struct{}\n"
                     "\n"
                     "// Close closes T%d; see [T%d].\n"
                     "func (T%d) Close() {}\n"
                     "\n"
                     "// F%d is a function; see [p%02d].\n"
                     "func F%d() {}\n"
                     "\n"
                     "// Shared is declared by every file of the package.\n"
                     "func Shared() {}\n",
                     p, next, f, (f + 1) % FILES, (f + 2) % FILES, next, f, next, (f + 3) % FILES,
                     next, f, f, f, f, f, next, f);
            go_w(repo, rel, src);
        }
    }
    char par_db[512];
    char seq_db[512];
    snprintf(par_db, sizeof(par_db), "%s/par.db", tmp);
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", tmp);
    ASSERT_EQ(dm_workers_agree(repo, par_db, seq_db), 0);
    /* and the fixture exercises what it should: edges, and rows of more than
     * one reason */
    ASSERT_STR_EQ(go_tier(par_db, "p00.T0", "p00.F1"), "unique");
    ASSERT_STR_EQ(go_tier(par_db, "p00.T0", "p01.T0"), "exact");
    ASSERT_STR_EQ(go_reason(par_db, "p00/f0.go", "p01.Gone"), "missing");
    ASSERT_STR_EQ(go_reason(par_db, "p00/f0.go", "fmt.Println"), "external");
    ASSERT_STR_EQ(go_reason(par_db, "p00/f0.go", "p01.T3.Close"), "graph_gap");
    ASSERT_STR_EQ(go_reason(par_db, "p00/f3.go", "Shared"), "graph_gap");
    ASSERT_TRUE(dm_count(par_db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'") > 100);
    dm_unlink_db(par_db);
    dm_unlink_db(seq_db);
    th_rmtree(tmp);
    PASS();
}

SUITE(doc_mentions_go) {
    dm_ship_held_families();
    RUN_TEST(doc_mentions_go_tokens);
    RUN_TEST(doc_mentions_go_sources);
    RUN_TEST(doc_mentions_go_package_comment_tokens);
    RUN_TEST(doc_mentions_go_text_model);
    RUN_TEST(doc_mentions_go_own_parameter);
    RUN_TEST(doc_mentions_go_hostile_doc);
    RUN_TEST(doc_mentions_go_random_docs);
    RUN_TEST(doc_mentions_go_random_scopes);
    RUN_TEST(doc_mentions_go_scope_blob);
    RUN_TEST(doc_mentions_go_forms);
    RUN_TEST(doc_mentions_go_plain_text);
    RUN_TEST(doc_mentions_go_struct_fields);
    RUN_TEST(doc_mentions_go_file_only_package_lookup);
    RUN_TEST(doc_mentions_go_local_rules);
    RUN_TEST(doc_mentions_go_lookup_order);
    RUN_TEST(doc_mentions_go_reasons);
    RUN_TEST(doc_mentions_go_node_owner);
    RUN_TEST(doc_mentions_go_package_comment);
    RUN_TEST(doc_mentions_go_std_module);
    RUN_TEST(doc_mentions_go_ship_gate);
    RUN_TEST(doc_mentions_go_incremental_equals_full);
    RUN_TEST(doc_mentions_go_scope_delta_rules);
    RUN_TEST(doc_mentions_go_workers_agree);
}
