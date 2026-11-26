/*
 * test_doc_mentions_rs.c — Rust intra-doc links -> MENTIONS edges.
 *
 * Extraction (internal/cbm/doclink_rs.c): which text is a link, the path
 * grammar, the scope blob of a source file and of a Cargo manifest.
 * Resolution (src/pipeline/doc_links_rs.c): every pipeline test indexes a real
 * fixture through cbm_pipeline_run and reads the published database (helpers:
 * test_doc_mentions_helpers.h).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"

/* ── extraction: which text is a link ────────────────────────────── */

/* The token written as `raw` on `line`; NULL when there is none. */
static const CBMDocLink *rs_token_at(const CBMFileResult *r, const char *raw, uint32_t line) {
    for (int i = 0; i < r->doc_links.count; i++) {
        const CBMDocLink *l = &r->doc_links.items[i];
        if (l->line == line && strcmp(l->raw, raw) == 0) {
            return l;
        }
    }
    return NULL;
}

static int rs_tokens_on(const CBMFileResult *r, uint32_t line) {
    int n = 0;
    for (int i = 0; i < r->doc_links.count; i++) {
        n += r->doc_links.items[i].line == line;
    }
    return n;
}

TEST(doc_mentions_rs_tokens) {
    const char *src =
        "//! File doc with [`Alpha`] here.\n"                                              /* 1 */
        "#![allow(dead_code)]\n"                                                           /* 2 */
        "//! After an attribute: [beta::Gamma].\n"                                         /* 3 */
        "\n"                                                                               /* 4 */
        "/// Inline [the text](alpha::Path) and shortcut [`Short`] and [plain::path].\n"   /* 5 */
        "/// Collapsed [`Coll`][] and reference [see this][lbl] and [lbl].\n"              /* 6 */
        "/// URL [site](https://example.org/a), file [guide](../guide.md), [top](#top).\n" /* 7 */
        "/// Fragment [`Frag#method.x`], image ![alt](img::Path), footnote [^1].\n"        /* 8 */
        "/// Code `[NotALink]`, escaped \\[NotThis\\], prose [two words].\n"               /* 9 */
        "/// Kinds [struct@Dis], [call()], [mac!], generic [Vec<u8>].\n"                   /* 10 */
        "/// - [ ] task\n"                                                                 /* 11 */
        "/// - [x] done\n"                                                                 /* 12 */
        "/// [!NOTE]\n"                                                                    /* 13 */
        "/// Backticked [`weird(x)`] and nested [outer [`Inner`] text].\n"                 /* 14 */
        "///\n"                                                                            /* 15 */
        "/// ```\n"                                                                        /* 16 */
        "/// [InFence]\n"                                                                  /* 17 */
        "/// ```\n"                                                                        /* 18 */
        "///\n"                                                                            /* 19 */
        "///     [InIndentedCode]\n"                                                       /* 20 */
        "///\n"                                                                            /* 21 */
        "/// <!-- [InComment] -->\n"                                                       /* 22 */
        "/// [lbl]: defined::Target\n"                                                     /* 23 */
        "/// [unused]: https://example.org/unused\n"                                       /* 24 */
        "pub struct Documented;\n"                                                         /* 25 */
        "\n"                                                                               /* 26 */
        "//// [FourSlashes]\n"                                                             /* 27 */
        "// [PlainComment]\n"                                                              /* 28 */
        "pub struct NotDocumented;\n"                                                      /* 29 */
        "\n"                                                                               /* 30 */
        "/** Block doc with [`InBlock`]\n"                                                 /* 31 */
        " * and a second line [second::Line].\n"                                           /* 32 */
        " */\n"                                                                            /* 33 */
        "pub fn block_documented() {}\n";                                                  /* 34 */
    CBMFileResult *r = dm_extract(src, CBM_LANG_RUST, "src/lib.rs");
    ASSERT_NOT_NULL(r);

    /* the file's own doc: marked, with the real line of every doc line (the
     * attribute between them is no doc line) */
    const CBMDocLink *alpha = rs_token_at(r, "Alpha", 1);
    ASSERT_NOT_NULL(alpha);
    ASSERT_EQ(alpha->syntax, CBM_DOCLINK_RS_INTRA_DOC);
    ASSERT_TRUE((alpha->flags & CBM_DOCLINK_FLAG_FILE) != 0);
    const CBMDocLink *gamma = rs_token_at(r, "beta::Gamma", 3);
    ASSERT_NOT_NULL(gamma);
    ASSERT_TRUE((gamma->flags & CBM_DOCLINK_FLAG_FILE) != 0);

    /* an item's doc: inline, shortcut (backticks go), plain shortcut */
    const CBMDocLink *inl = rs_token_at(r, "alpha::Path", 5);
    ASSERT_NOT_NULL(inl);
    ASSERT_EQ(inl->syntax, CBM_DOCLINK_RS_INTRA_DOC);
    ASSERT_EQ(inl->flags & CBM_DOCLINK_FLAG_FILE, 0);
    ASSERT_STR_EQ(inl->source_qn, "p.src.lib.Documented");
    ASSERT_EQ(inl->def_line, 25);
    ASSERT_NOT_NULL(rs_token_at(r, "Short", 5));
    ASSERT_NOT_NULL(rs_token_at(r, "plain::path", 5));
    ASSERT_EQ(rs_tokens_on(r, 5), 3);

    /* collapsed; a label with a definition takes the definition's destination */
    ASSERT_NOT_NULL(rs_token_at(r, "Coll", 6));
    const CBMDocLink *def = rs_token_at(r, "defined::Target", 6);
    ASSERT_NOT_NULL(def);
    ASSERT_EQ(def->syntax, CBM_DOCLINK_RS_LINK_DEF);
    ASSERT_EQ(dm_count_tokens(r, "defined::Target"), 2); /* [see this][lbl] and [lbl] */
    ASSERT_EQ(rs_tokens_on(r, 6), 3);

    /* a URL is the core's href family; a relative file and a page anchor are
     * not harvested */
    const CBMDocLink *url = rs_token_at(r, "https://example.org/a", 7);
    ASSERT_NOT_NULL(url);
    ASSERT_EQ(url->syntax, CBM_DOCLINK_HREF);
    ASSERT_EQ(rs_tokens_on(r, 7), 1);

    /* the fragment is cut; an image and a footnote are no links */
    ASSERT_NOT_NULL(rs_token_at(r, "Frag", 8));
    ASSERT_EQ(rs_tokens_on(r, 8), 1);

    /* a code span, escaped brackets and prose hold no link */
    ASSERT_EQ(rs_tokens_on(r, 9), 0);

    /* disambiguators, suffixes and generics stay in the token */
    ASSERT_NOT_NULL(rs_token_at(r, "struct@Dis", 10));
    ASSERT_NOT_NULL(rs_token_at(r, "call()", 10));
    ASSERT_NOT_NULL(rs_token_at(r, "mac!", 10));
    ASSERT_NOT_NULL(rs_token_at(r, "Vec<u8>", 10));

    /* task markers and an admonition are no links */
    ASSERT_EQ(rs_tokens_on(r, 11), 0);
    ASSERT_EQ(rs_tokens_on(r, 12), 0);
    ASSERT_EQ(rs_tokens_on(r, 13), 0);

    /* backticks make rustdoc keep what it would otherwise drop as prose; of
     * nested brackets the inner pair is the link */
    ASSERT_NOT_NULL(rs_token_at(r, "weird(x)", 14));
    ASSERT_NOT_NULL(rs_token_at(r, "Inner", 14));
    ASSERT_EQ(rs_tokens_on(r, 14), 2);

    /* code blocks, an HTML comment and the definitions themselves */
    ASSERT_EQ(rs_tokens_on(r, 17), 0);
    ASSERT_EQ(rs_tokens_on(r, 20), 0);
    ASSERT_EQ(rs_tokens_on(r, 22), 0);
    ASSERT_EQ(rs_tokens_on(r, 23), 0);
    ASSERT_EQ(rs_tokens_on(r, 24), 0);

    /* `////` and `//` are comments, not documentation */
    ASSERT_NULL(dm_find_token(r, "FourSlashes"));
    ASSERT_NULL(dm_find_token(r, "PlainComment"));

    /* a block doc, its decoration removed */
    const CBMDocLink *blk = rs_token_at(r, "InBlock", 31);
    ASSERT_NOT_NULL(blk);
    ASSERT_STR_EQ(blk->source_qn, "p.src.lib.block_documented");
    ASSERT_NOT_NULL(rs_token_at(r, "second::Line", 32));
    cbm_free_result(r);
    PASS();
}

/* The doc of the first item is the item's, never the file's. */
TEST(doc_mentions_rs_item_doc_is_not_file_doc) {
    const char *src = "/// Outer doc of the first item: [`Target`].\n"
                      "pub struct First;\n";
    CBMFileResult *r = dm_extract(src, CBM_LANG_RUST, "src/first.rs");
    ASSERT_NOT_NULL(r);
    const CBMDocLink *t = dm_find_token(r, "Target");
    ASSERT_NOT_NULL(t);
    ASSERT_EQ(t->flags & CBM_DOCLINK_FLAG_FILE, 0);
    ASSERT_STR_EQ(t->source_qn, "p.src.first.First");
    ASSERT_EQ(r->doc_links.count, 1);
    cbm_free_result(r);
    PASS();
}

/* Text nobody controls: nothing here may crash, hang or grow without bound. */
TEST(doc_mentions_rs_untrusted_text) {
    enum { DEEP = 20000, LONG = 1 << 20, LABEL = 700 };
    /* a megabyte destination, a megabyte label, a megabyte of opening brackets,
     * a megabyte of backticks, and a label longer than a token carries */
    char *big = malloc((size_t)LONG * 4 + 4096);
    ASSERT_NOT_NULL(big);
    size_t w = (size_t)snprintf(big, 64, "/// [dest](");
    memset(big + w, 'a', LONG);
    w += LONG;
    w += (size_t)snprintf(big + w, 64, ")\n/// [`");
    memset(big + w, 'b', LONG);
    w += LONG;
    w += (size_t)snprintf(big + w, 64, "`]\n/// ");
    memset(big + w, '[', LONG);
    w += LONG;
    w += (size_t)snprintf(big + w, 64, "\n/// ticks "); /* not at the line start: no fence */
    memset(big + w, '`', LONG);
    w += LONG;
    w += (size_t)snprintf(big + w, 64, "\n///\n/// [`");
    memset(big + w, 'c', LABEL);
    w += LABEL;
    snprintf(big + w, 64, "`]\npub struct Big;\n");
    CBMFileResult *r = dm_extract(big, CBM_LANG_RUST, "src/big.rs");
    ASSERT_NOT_NULL(r);
    /* an over-long destination is one token, cut and marked: it can never
     * parse, so no truncated prefix is ever resolved. A label of more than
     * 999 characters is no link label at all (CommonMark). */
    ASSERT_EQ(r->doc_links.count, 2);
    for (int i = 0; i < r->doc_links.count; i++) {
        const char *raw = r->doc_links.items[i].raw;
        size_t rl = strlen(raw);
        CBMDocLinkRsPath path;
        ASSERT_TRUE(rl < CBM_DOCLINK_RS_RAW_CAP);
        ASSERT_TRUE(raw[0] == (i == 0 ? 'a' : 'c'));
        ASSERT_STR_EQ(raw + rl - 3, "...");
        ASSERT_EQ(cbm_doclink_rs_parse_path(raw, rl, false, &path), CBM_DOCLINK_RS_PATH_MALFORMED);
    }
    cbm_free_result(r);

    /* deeply nested links, lists and block quotes */
    size_t pos = (size_t)snprintf(big, 64, "/// ");
    for (int i = 0; i < DEEP; i++) {
        big[pos++] = '[';
    }
    pos += (size_t)snprintf(big + pos, 64, "`Core`");
    for (int i = 0; i < DEEP; i++) {
        big[pos++] = ']';
    }
    pos += (size_t)snprintf(big + pos, 64, "\n/// ");
    for (int i = 0; i < DEEP; i++) {
        big[pos++] = '>';
    }
    pos += (size_t)snprintf(big + pos, 64, " [`Quoted`]\n/// ");
    for (int i = 0; i < DEEP; i++) {
        big[pos++] = '-';
        big[pos++] = ' ';
    }
    snprintf(big + pos, 64, "[`Listed`]\npub struct Nested;\n");
    r = dm_extract(big, CBM_LANG_RUST, "src/nested.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(dm_find_token(r, "Core"));
    cbm_free_result(r);

    /* bytes that are no UTF-8, and a NUL inside the doc */
    const char bad[] = "/// [`Caf\xC3`] [`\xFF\xFE`] [`Ok`]\n/// [`Af\0ter`]\npub struct Bytes;\n";
    r = cbm_extract_file(bad, (int)sizeof(bad) - 1, CBM_LANG_RUST, "p", "src/bytes.rs", 0, NULL,
                         NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(dm_find_token(r, "Ok"));
    cbm_free_result(r);
    free(big);
    PASS();
}

/* ── extraction: the path grammar (rustdoc's preprocess_link) ────── */

TEST(doc_mentions_rs_parse_path) {
    /* where the grammar decides what is harvested: text in plain brackets
     * that can be no path is prose (no token); the same text in backticks,
     * and a path that is broken, is a link rustdoc reports (a token, which
     * the resolver turns into an `unparseable` row) */
    CBMFileResult *r = dm_extract("/// [a+b] [`c+d`] [bogus@Foo] [Vec<] [x y] [`x y`]\n"
                                  "pub struct Grammar;\n",
                                  CBM_LANG_RUST, "src/g.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(dm_find_token(r, "a+b"));
    ASSERT_NOT_NULL(dm_find_token(r, "c+d"));
    ASSERT_NOT_NULL(dm_find_token(r, "bogus@Foo"));
    ASSERT_NOT_NULL(dm_find_token(r, "Vec<"));
    ASSERT_NULL(dm_find_token(r, "x y"));
    ASSERT_EQ(r->doc_links.count, 3);
    cbm_free_result(r);

    struct {
        const char *raw;
        bool lenient;
        CBMDocLinkRsPathStatus want;
        int nsegs;
        const char *last; /* last segment */
        int dis;
        bool global;
    } cases[] = {
        {"Foo", false, CBM_DOCLINK_RS_PATH_OK, 1, "Foo", CBM_DOCLINK_RS_DIS_NONE, false},
        {"a::b::C", false, CBM_DOCLINK_RS_PATH_OK, 3, "C", CBM_DOCLINK_RS_DIS_NONE, false},
        {"::std::fmt", false, CBM_DOCLINK_RS_PATH_OK, 2, "fmt", CBM_DOCLINK_RS_DIS_NONE, true},
        {" crate::x ", false, CBM_DOCLINK_RS_PATH_OK, 2, "x", CBM_DOCLINK_RS_DIS_NONE, false},
        /* generics are stripped, as rustdoc does */
        {"Vec<T>::new", false, CBM_DOCLINK_RS_PATH_OK, 2, "new", CBM_DOCLINK_RS_DIS_NONE, false},
        {"HashMap<K, V>", false, CBM_DOCLINK_RS_PATH_OK, 1, "HashMap", CBM_DOCLINK_RS_DIS_NONE,
         false},
        /* disambiguators and suffixes */
        {"struct@Foo", false, CBM_DOCLINK_RS_PATH_OK, 1, "Foo", CBM_DOCLINK_RS_DIS_STRUCT, false},
        {"fn@a::run", false, CBM_DOCLINK_RS_PATH_OK, 2, "run", CBM_DOCLINK_RS_DIS_FN, false},
        {"method@T::m", false, CBM_DOCLINK_RS_PATH_OK, 2, "m", CBM_DOCLINK_RS_DIS_FN, false},
        {"mod@x", false, CBM_DOCLINK_RS_PATH_OK, 1, "x", CBM_DOCLINK_RS_DIS_MOD, false},
        {"prim@str", false, CBM_DOCLINK_RS_PATH_OK, 1, "str", CBM_DOCLINK_RS_DIS_PRIM, false},
        {"type@X", false, CBM_DOCLINK_RS_PATH_OK, 1, "X", CBM_DOCLINK_RS_DIS_TYPE_NS, false},
        {"run()", false, CBM_DOCLINK_RS_PATH_OK, 1, "run", CBM_DOCLINK_RS_DIS_FN, false},
        {"vec!", false, CBM_DOCLINK_RS_PATH_OK, 1, "vec", CBM_DOCLINK_RS_DIS_MACRO_BANG, false},
        {"vec![]", false, CBM_DOCLINK_RS_PATH_OK, 1, "vec", CBM_DOCLINK_RS_DIS_MACRO_BANG, false},
        /* a raw identifier, a tuple field, a name that is no ASCII */
        {"r#type", false, CBM_DOCLINK_RS_PATH_OK, 1, "r#type", CBM_DOCLINK_RS_DIS_NONE, false},
        {"Pair::0", false, CBM_DOCLINK_RS_PATH_OK, 2, "0", CBM_DOCLINK_RS_DIS_NONE, false},
        {"Gr\xC3\xB6\xC3\x9F"
         "e",
         false, CBM_DOCLINK_RS_PATH_OK, 1,
         "Gr\xC3\xB6\xC3\x9F"
         "e",
         CBM_DOCLINK_RS_DIS_NONE, false},
        /* rustdoc's spellings of primitive types that are no identifiers */
        {"!", false, CBM_DOCLINK_RS_PATH_OK, 1, "!", CBM_DOCLINK_RS_DIS_NONE, false},
        {"&mut", false, CBM_DOCLINK_RS_PATH_OK, 1, "&mut", CBM_DOCLINK_RS_DIS_NONE, false},
        {"*const", false, CBM_DOCLINK_RS_PATH_OK, 1, "*const", CBM_DOCLINK_RS_DIS_NONE, false},
        /* prose: no link at all */
        {"two words", true, CBM_DOCLINK_RS_PATH_NOT_LINK, 0, NULL, 0, false},
        {"a+b", true, CBM_DOCLINK_RS_PATH_NOT_LINK, 0, NULL, 0, false},
        {"!NOTE", false, CBM_DOCLINK_RS_PATH_NOT_LINK, 0, NULL, 0, false},
        {"", false, CBM_DOCLINK_RS_PATH_NOT_LINK, 0, NULL, 0, false},
        /* a link, but no path */
        {"a+b", false, CBM_DOCLINK_RS_PATH_MALFORMED, 0, NULL, 0, false},
        {"weird(x)", false, CBM_DOCLINK_RS_PATH_MALFORMED, 0, NULL, 0, false},
        {"Vec<", false, CBM_DOCLINK_RS_PATH_MALFORMED, 0, NULL, 0, false},
        {"a:b", false, CBM_DOCLINK_RS_PATH_MALFORMED, 0, NULL, 0, false},
        {"a::", false, CBM_DOCLINK_RS_PATH_MALFORMED, 0, NULL, 0, false},
        {"bogus@Foo", false, CBM_DOCLINK_RS_PATH_MALFORMED, 0, NULL, 0, false},
        {"<T as Tr>::m", false, CBM_DOCLINK_RS_PATH_MALFORMED, 0, NULL, 0, false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        CBMDocLinkRsPath p;
        CBMDocLinkRsPathStatus got =
            cbm_doclink_rs_parse_path(cases[i].raw, strlen(cases[i].raw), cases[i].lenient, &p);
        bool ok = got == cases[i].want;
        if (ok && got == CBM_DOCLINK_RS_PATH_OK) {
            ok = p.nsegs == cases[i].nsegs && p.dis == cases[i].dis &&
                 p.global == cases[i].global && strcmp(p.segs[p.nsegs - 1], cases[i].last) == 0;
        }
        if (!ok) {
            printf("  [%s]: status %d segs %d dis %d\n", cases[i].raw, (int)got, p.nsegs, p.dis);
            FAIL("path grammar");
        }
    }
    /* more segments than are carried, and a segment longer than is carried */
    char many[256] = "a";
    for (int i = 0; i < CBM_DOCLINK_RS_MAX_SEGS; i++) {
        strcat(many, "::a");
    }
    CBMDocLinkRsPath p;
    ASSERT_EQ(cbm_doclink_rs_parse_path(many, strlen(many), false, &p),
              CBM_DOCLINK_RS_PATH_MALFORMED);
    char wide[CBM_DOCLINK_RS_SEG_CAP + 8];
    memset(wide, 'w', sizeof(wide) - 1);
    wide[sizeof(wide) - 1] = '\0';
    ASSERT_EQ(cbm_doclink_rs_parse_path(wide, strlen(wide), false, &p),
              CBM_DOCLINK_RS_PATH_MALFORMED);
    PASS();
}

/* ── extraction: the scope blob ──────────────────────────────────── */

/* True when the blob holds exactly this line. */
static bool rs_blob_has(const char *blob, const char *line) {
    size_t n = strlen(line);
    for (const char *p = blob; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (len == n && memcmp(p, line, n) == 0) {
            return true;
        }
        p = nl ? nl + 1 : NULL;
    }
    return false;
}

TEST(doc_mentions_rs_scope_blob) {
    const char *src = "use std::fmt::{self, Debug as Dbg};\n"                    /* 1 */
                      "pub use crate::shapes::*;\n"                              /* 2 */
                      "extern crate alloc as heap;\n"                            /* 3 */
                      "mod shapes;\n"                                            /* 4 */
                      "#[cfg(test)]\n"                                           /* 5 */
                      "mod checks;\n"                                            /* 6 */
                      "#[path = \"odd/place.rs\"]\n"                             /* 7 */
                      "pub(crate) mod odd;\n"                                    /* 8 */
                      "/// A widget: [`Gadget`], [`size`], [`T`].\n"             /* 9 */
                      "#[derive(Clone, serde::Serialize)]\n"                     /* 10 */
                      "pub struct Widget<T> {\n"                                 /* 11 */
                      "    pub size: u32,\n"                                     /* 12 */
                      "    inner: T,\n"                                          /* 13 */
                      "}\n"                                                      /* 14 */
                      "pub enum Kind { Round, Flat }\n"                          /* 15 */
                      "pub struct Pair(pub u32, pub u32);\n"                     /* 16 */
                      "pub trait Draw {\n"                                       /* 17 */
                      "    const SIDES: u32;\n"                                  /* 18 */
                      "    type Out;\n"                                          /* 19 */
                      "    fn draw(&self);\n"                                    /* 20 */
                      "}\n"                                                      /* 21 */
                      "impl<T> Widget<T> {\n"                                    /* 22 */
                      "    pub const UNIT: u32 = 1;\n"                           /* 23 */
                      "    /// Resize by [`factor`].\n"                          /* 24 */
                      "    pub fn resize(&mut self, factor: u32) {}\n"           /* 25 */
                      "}\n"                                                      /* 26 */
                      "impl<T> Draw for Widget<T> {\n"                           /* 27 */
                      "    const SIDES: u32 = 0;\n"                              /* 28 */
                      "    type Out = ();\n"                                     /* 29 */
                      "    fn draw(&self) {}\n"                                  /* 30 */
                      "}\n"                                                      /* 31 */
                      "impl<T: Debug> Draw for T {\n"                            /* 32 */
                      "    const SIDES: u32 = 0;\n"                              /* 33 */
                      "    type Out = ();\n"                                     /* 34 */
                      "    fn draw(&self) {}\n"                                  /* 35 */
                      "}\n"                                                      /* 36 */
                      "pub type Alias = shapes::Circle;\n"                       /* 37 */
                      "pub const LIMIT: u32 = 3;\n"                              /* 38 */
                      "pub static NAME: &str = \"\";\n"                          /* 39 */
                      "pub(super) fn helper() {}\n"                              /* 40 */
                      "#[macro_export]\n"                                        /* 41 */
                      "macro_rules! declare {\n"                                 /* 42 */
                      "    ($n:ident) => { pub struct Knob; pub struct $n; };\n" /* 43 */
                      "}\n"                                                      /* 44 */
                      "declare!(Dial);\n"                                        /* 45 */
                      "lazy_static! { pub static ref TABLE: u32 = 0; }\n"        /* 46 */
                      "#[cfg(test)]\n"                                           /* 47 */
                      "mod tests {\n"                                            /* 48 */
                      "    use super::*;\n"                                      /* 49 */
                      "    fn helper() {}\n"                                     /* 50 */
                      "}\n";                                                     /* 51 */
    CBMFileResult *r = dm_extract(src, CBM_LANG_RUST, "src/lib.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *b = r->doc_scope;
    ASSERT_TRUE(strncmp(b, CBM_DOCLINK_RS_SCOPE_TAG "\n", 4) == 0);
    /* imports: a `self` import, a renamed one, a glob, an extern crate */
    ASSERT_TRUE(rs_blob_has(b, "U\t0\t-\ts\tfmt\tstd::fmt"));
    ASSERT_TRUE(rs_blob_has(b, "U\t0\t-\tb\tDbg\tstd::fmt::Debug"));
    ASSERT_TRUE(rs_blob_has(b, "U\t0\tp\tg\t-\tcrate::shapes"));
    ASSERT_TRUE(rs_blob_has(b, "U\t0\t-\tx\theap\talloc"));
    /* `mod x;`: plain, test-only, with a path */
    ASSERT_TRUE(rs_blob_has(b, "D\t0\t-\tshapes\t-"));
    ASSERT_TRUE(rs_blob_has(b, "D\t0\t-t\tchecks\t-"));
    ASSERT_TRUE(rs_blob_has(b, "D\t0\tc\todd\todd/place.rs"));
    /* items with their node, members with theirs, derive names */
    ASSERT_TRUE(rs_blob_has(b, "T\t0\t11\t14\tp\tS\tWidget\tS=\tClone,Serialize"));
    ASSERT_TRUE(rs_blob_has(b, "M\t12\td\tsize\tD:Widget.size"));
    ASSERT_TRUE(rs_blob_has(b, "M\t13\td\tinner\tD:Widget.inner"));
    ASSERT_TRUE(rs_blob_has(b, "T\t0\t15\t15\tp\tE\tKind\tE=\t-"));
    ASSERT_TRUE(rs_blob_has(b, "M\t15\tv\tRound\t!")); /* a variant has no node */
    ASSERT_TRUE(rs_blob_has(b, "M\t0\td\t0\t!"));      /* nor has a tuple field */
    ASSERT_TRUE(rs_blob_has(b, "T\t0\t17\t21\tp\tR\tDraw\tI=\t-"));
    ASSERT_TRUE(rs_blob_has(b, "M\t19\tt\tOut\t!"));
    ASSERT_TRUE(rs_blob_has(b, "M\t20\tf\tdraw\tM:Draw.draw"));
    /* impls: inherent, of a trait, and for no named type */
    ASSERT_TRUE(rs_blob_has(b, "I\t0\t22\t26\tWidget\t-"));
    ASSERT_TRUE(rs_blob_has(b, "M\t23\tc\tUNIT\t!")); /* an impl's constant has no node */
    ASSERT_TRUE(rs_blob_has(b, "M\t25\tf\tresize\tM:Widget.resize"));
    ASSERT_TRUE(rs_blob_has(b, "I\t0\t27\t31\tWidget\tDraw"));
    ASSERT_TRUE(rs_blob_has(b, "I\t0\t32\t36\t?\tDraw"));
    ASSERT_TRUE(rs_blob_has(b, "T\t0\t37\t37\tp\tA\tAlias\tT=\tshapes::Circle"));
    ASSERT_TRUE(rs_blob_has(b, "T\t0\t38\t38\tp\tC\tLIMIT\tV=\t-"));
    ASSERT_TRUE(rs_blob_has(b, "T\t0\t39\t39\tp\tG\tNAME\tV=\t-"));
    /* the module-level helper lost its node to the one in `mod tests` (the
     * extractor flattens inline modules) */
    ASSERT_TRUE(rs_blob_has(b, "T\t0\t40\t40\ts\tF\thelper\t!\t-"));
    ASSERT_TRUE(rs_blob_has(b, "T\t1\t50\t50\t-\tF\thelper\tF=\t-"));
    ASSERT_TRUE(rs_blob_has(b, "N\t1\t0\t48\t51\t-t\ttests"));
    ASSERT_TRUE(rs_blob_has(b, "U\t1\t-\tg\t-\tsuper"));
    /* macros: an exported definition and what its body declares by name (the
     * metavariable is no name); the invocation; the name written at it; a
     * `static ref` of a macro body */
    ASSERT_TRUE(rs_blob_has(b, "T\t0\t42\t44\t-e\tM\tdeclare\t!\t-"));
    ASSERT_TRUE(rs_blob_has(b, "W\tdeclare\tKnob"));
    ASSERT_TRUE(rs_blob_has(b, "V\t0\tdeclare"));
    ASSERT_TRUE(rs_blob_has(b, "Q\t0\tDial"));
    ASSERT_TRUE(rs_blob_has(b, "V\t0\tlazy_static"));
    ASSERT_TRUE(rs_blob_has(b, "Q\t0\tTABLE"));
    /* the names local to a documented definition that carries references */
    ASSERT_TRUE(rs_blob_has(b, "P\t11\tT"));
    ASSERT_TRUE(rs_blob_has(b, "P\t25\tT,factor"));
    ASSERT_FALSE(rs_blob_has(b, "B"));

    /* the persisted form: no line numbers, no per-definition records */
    char *portable = cbm_doclink_portable_scope(b);
    ASSERT_NOT_NULL(portable);
    ASSERT_TRUE(rs_blob_has(portable, "T\t0\t0\t0\tp\tS\tWidget\tS=\tClone,Serialize"));
    ASSERT_TRUE(rs_blob_has(portable, "M\t0\tf\tresize\tM:Widget.resize"));
    ASSERT_TRUE(rs_blob_has(portable, "N\t1\t0\t0\t0\t-t\ttests"));
    ASSERT_NULL(strstr(portable, "\nP\t"));
    ASSERT_NULL(strstr(portable, "\nZ\t"));
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    cbm_free_result(r);
    PASS();
}

/* A parse error: what follows it has no scope, unless the error stayed inside
 * a function body with its delimiters paired. */
TEST(doc_mentions_rs_scope_parse_errors) {
    /* syntax the grammar does not know, inside a body: the items around it
     * are where the tree puts them */
    const char *contained = "pub fn odd() {\n"
                            "    let x = 1 @@ 2;\n"
                            "}\n"
                            "pub struct After;\n";
    CBMFileResult *r = dm_extract(contained, CBM_LANG_RUST, "src/a.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_TRUE(rs_blob_has(r->doc_scope, "T\t0\t4\t4\tp\tS\tAfter\tS=\t-"));
    ASSERT_FALSE(rs_blob_has(r->doc_scope, "B"));
    ASSERT_NULL(strstr(r->doc_scope, "\nE\t"));
    cbm_free_result(r);

    /* an unpaired brace in a body can move every later item boundary: the
     * parser gave up on the whole file, and nothing of it is trusted */
    const char *open = "pub struct Before;\n"
                       "pub fn half() {\n"
                       "    if true {\n"
                       "}\n"
                       "pub struct Lost;\n";
    r = dm_extract(open, CBM_LANG_RUST, "src/b.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_TRUE(rs_blob_has(r->doc_scope, "E\t1"));
    ASSERT_TRUE(rs_blob_has(r->doc_scope, "B"));
    ASSERT_NULL(strstr(r->doc_scope, "\nT\t"));
    cbm_free_result(r);

    /* an error between items: what stands before it keeps its scope, what
     * follows is only a name the file may declare somewhere */
    const char *between = "pub struct One;\n"
                          "pub struct {\n"
                          "pub struct Two;\n";
    r = dm_extract(between, CBM_LANG_RUST, "src/c.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_TRUE(rs_blob_has(r->doc_scope, "T\t0\t1\t1\tp\tS\tOne\tS=\t-"));
    ASSERT_TRUE(rs_blob_has(r->doc_scope, "E\t2"));
    ASSERT_TRUE(rs_blob_has(r->doc_scope, "Q\t*\tTwo"));
    ASSERT_TRUE(rs_blob_has(r->doc_scope, "B"));
    cbm_free_result(r);
    PASS();
}

TEST(doc_mentions_rs_manifest_blob) {
    const char *toml = "[package]\n"
                       "name = \"acme-core\"\n"
                       "edition.workspace = true\n"
                       "build = \"gen/build.rs\"\n"
                       "autobins = false\n"
                       "\n"
                       "[lib]\n"
                       "name = \"acme\"\n"
                       "path = \"lib/root.rs\"\n"
                       "\n"
                       "[[bin]]\n"
                       "name = \"tool\"\n"
                       "path = \"cli/tool.rs\"\n"
                       "\n"
                       "[dependencies]\n"
                       "serde = \"1\"\n"
                       "util = { path = \"../util\" }\n"
                       "alias = { package = \"real-name\", path = \"../real\" }\n"
                       "shared.workspace = true\n"
                       "\n"
                       "[dev-dependencies.helper]\n"
                       "path = \"../helper\"\n"
                       "\n"
                       "[target.'cfg(unix)'.dependencies]\n"
                       "nix = \"0.29\"\n"
                       "\n"
                       "[workspace]\n"
                       "members = [\"crates/*\"]\n"
                       "\n"
                       "[workspace.package]\n"
                       "edition = \"2021\"\n"
                       "\n"
                       "[workspace.dependencies]\n"
                       "shared = { path = \"crates/shared\" }\n";
    CBMFileResult *r = dm_extract(toml, CBM_LANG_TOML, "crates/core/Cargo.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *b = r->doc_scope;
    ASSERT_TRUE(strncmp(b, CBM_DOCLINK_RS_SCOPE_TAG "\nC\n", 6) == 0);
    ASSERT_TRUE(rs_blob_has(b, "K\tpackage\tname\tacme-core"));
    ASSERT_TRUE(rs_blob_has(b, "K\tpackage\tedition.workspace\ttrue"));
    ASSERT_TRUE(rs_blob_has(b, "K\tpackage\tbuild\tgen/build.rs"));
    ASSERT_TRUE(rs_blob_has(b, "K\tpackage\tautobins\tfalse"));
    ASSERT_TRUE(rs_blob_has(b, "K\tlib\tname\tacme"));
    ASSERT_TRUE(rs_blob_has(b, "K\tlib\tpath\tlib/root.rs"));
    ASSERT_TRUE(rs_blob_has(b, "A\tbin"));
    ASSERT_TRUE(rs_blob_has(b, "a\tpath\tcli/tool.rs"));
    ASSERT_TRUE(rs_blob_has(b, "X\td\tserde\t-\t-"));
    ASSERT_TRUE(rs_blob_has(b, "X\td\tutil\tpath\t../util"));
    ASSERT_TRUE(rs_blob_has(b, "X\td\talias\tpackage\treal-name"));
    ASSERT_TRUE(rs_blob_has(b, "X\td\talias\tpath\t../real"));
    ASSERT_TRUE(rs_blob_has(b, "X\td\tshared\tworkspace\ttrue"));
    ASSERT_TRUE(rs_blob_has(b, "X\tv\thelper\tpath\t../helper"));
    ASSERT_TRUE(rs_blob_has(b, "X\td\tnix\t-\t-"));
    ASSERT_TRUE(rs_blob_has(b, "W"));
    ASSERT_TRUE(rs_blob_has(b, "K\tworkspace.package\tedition\t2021"));
    ASSERT_TRUE(rs_blob_has(b, "X\tw\tshared\tpath\tcrates/shared"));
    cbm_free_result(r);

    /* any other TOML file has no scope */
    r = dm_extract("[package]\nname = \"x\"\n", CBM_LANG_TOML, "config/settings.toml");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(r->doc_scope);
    cbm_free_result(r);
    PASS();
}

/* ── resolution: the fixture workspace ───────────────────────────── */

typedef struct {
    const char *path;
    const char *text;
} rs_fixture_file_t;

static void rs_write_files(const char *root, const rs_fixture_file_t *files, size_t n) {
    for (size_t i = 0; i < n; i++) {
        th_write_file(TH_PATH(root, files[i].path), files[i].text);
    }
}

/* A workspace of five packages. `core` is the one under test; `util` and
 * `extra` are path dependencies of it; `twin` has a lib and two binaries that
 * share a module; `old` is an edition-2015 crate. */
static void rs_write_workspace(const char *root) {
    static const rs_fixture_file_t files[] = {
        {"Cargo.toml",
         "[workspace]\n"
         "members = [\"crates/core\", \"crates/util\", \"crates/extra\", \"crates/twin\",\n"
         "           \"crates/old\"]\n"
         "\n"
         "[workspace.package]\n"
         "edition = \"2021\"\n"
         "\n"
         "[workspace.dependencies]\n"
         "util = { path = \"crates/util\" }\n"},

        /* util: a path dependency, inherited through the workspace table */
        {"crates/util/Cargo.toml", "[package]\nname = \"util\"\nedition = \"2021\"\n"},
        {"crates/util/src/lib.rs", "//! Utilities.\n"
                                   "mod inner;\n"
                                   "pub use crate::inner::Hidden as Shown;\n"
                                   "/// A tool.\n"
                                   "pub struct Tool;\n"
                                   "impl Tool {\n"
                                   "    /// Runs.\n"
                                   "    pub fn run(&self) {}\n"
                                   "}\n"},
        {"crates/util/src/inner.rs", "/// Hidden away.\npub struct Hidden;\n"},

        /* extra: its lib target is named unlike its package */
        {"crates/extra/Cargo.toml", "[package]\nname = \"acme-extra\"\nedition = \"2021\"\n"
                                    "\n[lib]\nname = \"extra_lib\"\n"},
        {"crates/extra/src/lib.rs", "/// Extra.\npub struct Extra;\n"},

        {"crates/core/Cargo.toml", "[package]\n"
                                   "name = \"acme-core\"\n"
                                   "edition.workspace = true\n"
                                   "\n"
                                   "[dependencies]\n"
                                   "util.workspace = true\n"
                                   "serde = \"1\"\n"
                                   "acme-extra = { path = \"../extra\" }\n"
                                   "Option = { package = \"acme-extra\", path = \"../extra\" }\n"},
        {"crates/core/src/lib.rs",
         "//! Crate doc: [`Gadget`] and [shapes::Circle], also [the tool][tool].\n" /* 1 */
         "//! Rotten: [`NoSuchThing`].\n"                                           /* 2 */
         "//!\n"
         "//! [tool]: util::Tool\n"
         "\n"
         "pub mod shapes;\n"
         "pub mod deep;\n"
         "pub mod fake;\n"
         "pub mod shadow;\n"
         "pub mod levels;\n"
         "pub mod char;\n"
         "pub mod first;\n"
         "pub mod chain;\n"
         "pub mod strict;\n"
         "pub mod broken;\n"
         "#[cfg(test)]\n"
         "mod tests_support;\n"
         "#[path = \"elsewhere/odd.rs\"]\n"
         "pub mod odd;\n"
         "pub mod outer_inline {\n"
         "    pub mod leaf;\n"
         "    /// In an inline module.\n"
         "    pub struct InInline;\n"
         "}\n"
         "#[cfg(unix)]\n"
         "#[path = \"imp_unix.rs\"]\n"
         "mod imp;\n"
         "#[cfg(windows)]\n"
         "#[path = \"imp_win.rs\"]\n"
         "mod imp;\n"
         "\n"
         "use fake::*;\n"
         "pub use shapes::Circle;\n"
         "use shapes::Circle as Disc;\n"
         "use util::Tool as Hammer;\n"
         "pub use paint_derive::Paintable;\n"
         "\n"
         "/// A gadget.\n"
         "pub struct Gadget;\n"
         "/// Own shape (the glob brings another one).\n"
         "pub struct Shape;\n"
         "/// A name that is no ASCII.\n"
         "pub struct Gr\xC3\xB6\xC3\x9F"
         "e;\n"
         "/// A trait named like a derive macro.\n"
         "pub trait Paintable {}\n"
         "/// Both a function and a macro.\n"
         "pub fn dual() {}\n"
         "#[macro_export]\n"
         "macro_rules! dual {\n"
         "    () => {};\n"
         "}\n"
         "/// Unix.\n"
         "#[cfg(unix)]\n"
         "pub fn plat() {}\n"
         "/// Windows.\n"
         "#[cfg(windows)]\n"
         "pub fn plat() {}\n"
         "/// One of two declarations under cfg.\n"
         "#[cfg(unix)]\n"
         "pub struct Twin;\n"
         "/// The other.\n"
         "#[cfg(not(unix))]\n"
         "pub struct Twin;\n"
         "/// Only for tests.\n"
         "#[cfg(test)]\n"
         "pub fn only_in_tests() {}\n"
         "macro_rules! declare_knob {\n"
         "    () => {\n"
         "        pub struct Knob;\n"
         "    };\n"
         "}\n"
         "declare_knob!();\n"
         "define_things! {\n"
         "    pub struct Made;\n"
         "}\n"
         "\n"
         "/// [`Gadget`]\n"
         "pub struct SrcShortcut;\n"
         "/// [the circle](shapes::Circle)\n"
         "pub struct SrcInline;\n"
         "/// [`Circle`][]\n"
         "pub struct SrcCollapsed;\n"
         "/// [the hammer][ham]\n"
         "///\n"
         "/// [ham]: Hammer\n"
         "pub struct SrcLinkDef;\n"
         "/// [`Disc`]\n"
         "pub struct SrcRenamed;\n"
         "/// [site](https://example.org/docs) and [guide](../guide.md)\n"
         "pub struct SrcUrl;\n"
         "/// [`Shape`]\n"
         "pub struct SrcOwnOverGlob;\n"
         "/// [`OnlyInFake`] [`CrateWide`] [`Secret`]\n"
         "pub struct SrcGlob;\n"
         "/// [util::Tool::run] [extra_lib::Extra] [Option::Extra] [serde::Serialize]\n"
         "/// [std::fmt::Debug] [util::Shown]\n"
         "pub struct SrcExtern;\n"
         "/// [`String`] [`u8`] [str::len] [`char`] [prim@char] [mod@char] [prim@Gadget]\n"
         "pub struct SrcStd;\n"
         "/// [crate::shapes::Kind] [self::Gadget] [crate]\n"
         "pub struct SrcKeywords;\n"
         "/// [Circle::area] [Circle::area()] [Circle::radius] [Circle::UNIT]\n"
         "/// [shapes::Kind::Round] [shapes::Draw::draw] [Circle::draw] [shapes::Ring::area]\n"
         "/// [shapes::Ring] [shapes::Pair] [shapes::Pair()] [shapes::Pair::0] [shapes::Bits]\n"
         "/// [Circle::no_such] [Circle::clone] [shapes::Lamp::glow]\n"
         "pub struct SrcMembers;\n"
         "/// [`dual`] [`dual()`] [`dual!`] [fn@dual] [macro@dual]\n"
         "pub struct SrcNamespaces;\n"
         "/// [struct@Gadget] [enum@Gadget] [fn@Gadget] [type@Gadget] [`Paintable`] [`plat`]\n"
         "/// [`Twin`]\n"
         "pub struct SrcKinds;\n"
         "/// [`NoSuchName`] [shapes::NoSuch] [`Gr\xC3\xB6\xC3\x9F"
         "e`] [`\xC3\x9C"
         "bel`]\n"
         "/// [outer_inline] [outer_inline::InInline] [`Knob`] [`Made`]\n"
         "/// [tests_support::Fixture] [`only_in_tests`] [`Circle::area(x)`] [Circle#a#b]\n"
         "/// [imp::Thing]\n"
         "pub struct SrcReasons;\n"
         "/// [deep::pit::Hole] [shapes::round::Ball] [odd::Odd] [outer_inline::leaf::Leaf]\n"
         "pub struct SrcModules;\n"
         "/// [`count`] and [`T`] and [`Gadget`]\n"
         "pub fn src_local<T>(count: u32) {}\n"},
        {"crates/core/src/shapes.rs",
         "pub mod round;\n"
         "/// A circle. [`Self`] [`Self::area`]\n"
         "#[derive(Clone)]\n"
         "pub struct Circle {\n"
         "    /// The radius; see [`Circle::area`].\n"
         "    pub radius: f64,\n"
         "}\n"
         "impl Circle {\n"
         "    /// The unit.\n"
         "    pub const UNIT: f64 = 1.0;\n"
         "    /// Area from [`Self::radius`]; see [`Draw::draw`], [`Circle::draw`],\n"
         "    /// [`Circle::outline`], [`Self::UNIT`].\n"
         "    pub fn area(&self) -> f64 {\n"
         "        0.0\n"
         "    }\n"
         "}\n"
         "/// Kinds.\n"
         "pub enum Kind {\n"
         "    Round,\n"
         "    Flat,\n"
         "}\n"
         "/// Draws.\n"
         "pub trait Draw {\n"
         "    /// Draw; see [`Self::outline`].\n"
         "    fn draw(&self);\n"
         "    /// Outline.\n"
         "    fn outline(&self) {}\n"
         "}\n"
         "impl Draw for Circle {\n"
         "    /// Draws a circle: [`Self::area`].\n"
         "    fn draw(&self) {}\n"
         "}\n"
         "/// Alias.\n"
         "pub type Ring = Circle;\n"
         "/// Tuple.\n"
         "pub struct Pair(pub u32, pub u32);\n"
         "/// Bits.\n"
         "pub union Bits {\n"
         "    pub a: u32,\n"
         "    pub b: f32,\n"
         "}\n"
         "/// A field and a method of one name: the method is what a path names.\n"
         "pub struct Lamp {\n"
         "    pub glow: u32,\n"
         "}\n"
         "impl Lamp {\n"
         "    /// Glows.\n"
         "    pub fn glow(&self) -> u32 {\n"
         "        self.glow\n"
         "    }\n"
         "}\n"},
        {"crates/core/src/shapes/round.rs",
         "/// A ball: [super::Circle] [super::super::Gadget] [`super::Kind`]\n"
         "pub struct Ball;\n"},
        {"crates/core/src/deep/mod.rs", "pub mod pit;\n"},
        {"crates/core/src/deep/pit.rs", "/// A hole.\npub struct Hole;\n"},
        {"crates/core/src/fake.rs", "/// Decoy shape.\n"
                                    "pub struct Shape;\n"
                                    "/// Only here.\n"
                                    "pub struct OnlyInFake;\n"
                                    "/// Crate wide.\n"
                                    "pub(crate) struct CrateWide;\n"
                                    "struct Secret;\n"},
        {"crates/core/src/shadow.rs", "pub mod util;\n"},
        {"crates/core/src/shadow/util.rs", "/// The shadowing tool.\npub struct Tool;\n"},
        {"crates/core/src/levels.rs", "use crate::shadow::*;\n"
                                      "/// Own Vec.\n"
                                      "pub struct Vec;\n"
                                      "/// [util::Tool] [`Vec`]\n"
                                      "pub struct LevelGlob;\n"},
        {"crates/core/src/char.rs", "/// In a module named like a primitive type.\n"
                                    "pub struct Unit;\n"},
        {"crates/core/src/first.rs", "/// [`crate::Gadget`]\npub struct First;\n"},
        {"crates/core/src/chain.rs", "mod a {\n"
                                     "    /// Deep.\n"
                                     "    pub struct Deep;\n"
                                     "}\n"
                                     "mod b {\n"
                                     "    pub use super::a::Deep as Mid;\n"
                                     "}\n"
                                     "pub use self::b::Mid as Top;\n"
                                     "mod g1 {\n"
                                     "    pub use super::g2::*;\n"
                                     "}\n"
                                     "mod g2 {\n"
                                     "    /// Via globs.\n"
                                     "    pub struct ViaGlobs;\n"
                                     "}\n"
                                     "use self::g1::*;\n"
                                     "mod c1 {\n"
                                     "    pub use super::c2::Loop;\n"
                                     "}\n"
                                     "mod c2 {\n"
                                     "    pub use super::c1::Loop;\n"
                                     "}\n"
                                     "/// [`Top`] [`ViaGlobs`] [c1::Loop]\n"
                                     "pub struct ChainUser;\n"},
        {"crates/core/src/strict.rs", "use serde::*;\n"
                                      "use util::Tool as Wrench;\n"
                                      "/// [util::Tool] [`Wrench`]\n"
                                      "pub struct StrictUser;\n"},
        {"crates/core/src/broken.rs", "/// Before the error: [`crate::Gadget`] [`Vec`]\n"
                                      "pub struct Whole;\n"
                                      "pub struct {\n"
                                      "/// After the error: [`crate::Shape`]\n"
                                      "pub struct Cut;\n"},
        /* declared by no `mod`: an orphan */
        {"crates/core/src/orphan.rs", "/// [crate::Gadget] [`String`] [serde::Serialize]\n"
                                      "pub struct Orphan;\n"},
        {"crates/core/src/tests_support.rs",
         "/// A fixture: [`crate::Gadget`] [`super::only_in_tests`]\n"
         "pub struct Fixture;\n"},
        {"crates/core/src/elsewhere/odd.rs", "/// Odd.\npub struct Odd;\n"},
        {"crates/core/src/outer_inline/leaf.rs", "/// Leaf.\npub struct Leaf;\n"},
        {"crates/core/src/imp_unix.rs", "/// Unix thing.\npub struct Thing;\n"},
        {"crates/core/src/imp_win.rs", "/// Windows thing.\npub struct Thing;\n"},
        {"crates/core/tests/it.rs", "/// [acme_core::Gadget] [`acme_core::shapes::Circle::area`]\n"
                                    "fn it_works() {}\n"},

        /* twin: one module file under two crate roots, and a second binary */
        {"crates/twin/Cargo.toml", "[package]\nname = \"twin\"\nedition = \"2021\"\n"},
        {"crates/twin/src/lib.rs", "mod common;\n"
                                   "/// Only in the lib.\n"
                                   "pub struct Only;\n"
                                   "/// In both roots.\n"
                                   "pub struct Both;\n"},
        {"crates/twin/src/main.rs", "mod common;\n"
                                    "/// In both roots.\n"
                                    "pub struct Both;\n"
                                    "fn main() {}\n"},
        {"crates/twin/src/common.rs", "/// [crate::Only] [crate::Both]\npub struct Shared;\n"},
        {"crates/twin/src/bin/tool.rs", "/// [twin::Only]\npub fn run_tool() {}\n"},

        /* old: no edition key means 2015, where a `use` path starts at the root */
        {"crates/old/Cargo.toml", "[package]\nname = \"old\"\n"},
        {"crates/old/src/lib.rs", "mod a;\nmod b;\n"},
        {"crates/old/src/a.rs", "/// Thing.\npub struct Thing;\n"},
        {"crates/old/src/b.rs", "use a::Thing;\n/// [`Thing`]\npub struct UsesThing;\n"},
    };
    rs_write_files(root, files, sizeof(files) / sizeof(files[0]));
}

/* One edge from the definition ending in `src` to the one ending in `tgt`,
 * with every fragment of `want` (separated by '|') in its properties. */
static bool rs_edge_is(const char *db, const char *src, const char *tgt, const char *want) {
    char props[512];
    char frags[256];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    if (n != 1) {
        printf("  edge %s -> %s: %d found\n", src, tgt, n);
        return false;
    }
    snprintf(frags, sizeof(frags), "%s", want);
    for (char *f = strtok(frags, "|"); f; f = strtok(NULL, "|")) {
        if (!strstr(props, f)) {
            printf("  edge %s -> %s: %s lacks %s\n", src, tgt, props, f);
            return false;
        }
    }
    return true;
}

static bool rs_no_edge(const char *db, const char *src, const char *tgt) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    if (n != 0) {
        printf("  edge %s -> %s: %d found, none expected\n", src, tgt, n);
    }
    return n == 0;
}

/* The unresolved row of `raw` in `rel` has this reason ("" for no row). */
static bool rs_row_is(const char *db, const char *rel, const char *raw, const char *want) {
    char reason[64];
    dm_row(db, rel, raw, reason, sizeof(reason), NULL, 0);
    if (strcmp(reason, want) != 0) {
        printf("  row %s [%s]: \"%s\", expected \"%s\"\n", rel, raw, reason, want);
        return false;
    }
    return true;
}

#define RS_SYNTAX_INTRA "\"syntax\":\"intra_doc\""
#define RS_SYNTAX_DEF "\"syntax\":\"link_def\""
#define RS_EXACT "\"tier\":\"exact\""
#define RS_UNIQUE "\"tier\":\"unique\""

/* One indexed copy of the workspace for the resolver tests. */
typedef struct {
    char tmp[256];
    char db[512];
} rs_ws_t;

static bool rs_ws_open(rs_ws_t *ws) {
    snprintf(ws->tmp, sizeof(ws->tmp), "/tmp/cbm_dmrs_XXXXXX");
    if (!cbm_mkdtemp(ws->tmp)) {
        return false;
    }
    rs_write_workspace(ws->tmp);
    snprintf(ws->db, sizeof(ws->db), "%s/ws.db", ws->tmp);
    return dm_index(ws->tmp, ws->db, NULL) == 0;
}

static void rs_ws_close(rs_ws_t *ws) {
    dm_unlink_db(ws->db);
    th_rmtree(ws->tmp);
}

static const char RS_LIB[] = "crates/core/src/lib.rs";

/* The two families, both tiers, and what is no reference at all. */
TEST(doc_mentions_rs_families_and_tiers) {
    rs_ws_t ws;
    ASSERT_TRUE(rs_ws_open(&ws));
    const char *db = ws.db;
    char syntax[64];
    char reason[64];
    /* intra_doc: the destination is written at the link */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcShortcut", "lib.Gadget",
                           "\"via\":\"doc_comment\"|" RS_SYNTAX_INTRA "|" RS_UNIQUE
                           "|\"count\":1"));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcInline", "shapes.Circle", RS_SYNTAX_INTRA "|" RS_EXACT));
    /* a name bound by a named import is an exact naming */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcCollapsed", "shapes.Circle", RS_SYNTAX_INTRA "|" RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcRenamed", "shapes.Circle", RS_EXACT));
    /* link_def: the destination comes from a link reference definition */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcLinkDef", "util.src.lib.Tool", RS_SYNTAX_DEF "|" RS_EXACT));
    /* a URL is the core's href family; a relative file is not harvested */
    dm_row(db, RS_LIB, "https://example.org/docs", reason, sizeof(reason), syntax, sizeof(syntax));
    ASSERT_STR_EQ(reason, "external");
    ASSERT_STR_EQ(syntax, "href");
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "../guide.md", ""));
    ASSERT_EQ(dm_mentions_from(db, "lib.SrcUrl"), 0);
    /* the definition's own parameter and type parameter: no edge, no row */
    ASSERT_TRUE(rs_edge_is(db, "lib.src_local", "lib.Gadget", RS_UNIQUE));
    ASSERT_EQ(dm_mentions_from(db, "lib.src_local"), 1);
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "count", ""));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "T", ""));
    rs_ws_close(&ws);
    PASS();
}

/* A file's own doc: the File node is the source. */
TEST(doc_mentions_rs_file_level_docs) {
    rs_ws_t ws;
    ASSERT_TRUE(rs_ws_open(&ws));
    const char *db = ws.db;
    const char *file = "core.src.lib.rs.__file__";
    ASSERT_TRUE(
        rs_edge_is(db, file, "core.src.lib.Gadget", RS_SYNTAX_INTRA "|" RS_UNIQUE "|\"line\":1"));
    ASSERT_TRUE(rs_edge_is(db, file, "shapes.Circle", RS_SYNTAX_INTRA "|" RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, file, "util.src.lib.Tool", RS_SYNTAX_DEF "|" RS_EXACT));
    ASSERT_EQ(dm_mentions_from(db, file), 3);
    /* an unresolved one: a row with the file's path and the reference's line */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "NoSuchThing", "missing"));
    ASSERT_EQ(dm_count(db, "SELECT line FROM doc_link_unresolved WHERE raw = 'NoSuchThing'"), 2);
    /* the outer doc of a file's first item belongs to the item */
    ASSERT_TRUE(rs_edge_is(db, "first.First", "core.src.lib.Gadget", RS_EXACT));
    ASSERT_EQ(dm_mentions_from(db, "first.rs.__file__"), 0);
    rs_ws_close(&ws);
    PASS();
}

/* The lookup levels, each with a decoy one level further out. */
TEST(doc_mentions_rs_lookup_levels) {
    rs_ws_t ws;
    ASSERT_TRUE(rs_ws_open(&ws));
    const char *db = ws.db;
    /* level 1 before level 2: the module's own item, not the glob's */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcOwnOverGlob", "core.src.lib.Shape", RS_UNIQUE));
    ASSERT_TRUE(rs_no_edge(db, "lib.SrcOwnOverGlob", "fake.Shape"));
    /* level 2: a glob import brings in what the importing module can see */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcGlob", "fake.OnlyInFake", RS_UNIQUE));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcGlob", "fake.CrateWide", RS_UNIQUE));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Secret", "missing")); /* private to `fake` */
    /* level 2 before level 3: the module a glob brings in, not the crate of
     * the extern prelude named like it */
    ASSERT_TRUE(rs_edge_is(db, "levels.LevelGlob", "shadow.util.Tool", RS_EXACT));
    ASSERT_TRUE(rs_no_edge(db, "levels.LevelGlob", "util.src.lib.Tool"));
    /* level 1 before level 4: the module's own `Vec`, not the prelude's */
    ASSERT_TRUE(rs_edge_is(db, "levels.LevelGlob", "levels.Vec", RS_UNIQUE));
    /* level 3: a path dependency (through the workspace table), a lib target
     * named unlike its package, and a re-export of the dependency */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcExtern", "util.src.lib.Tool.run", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcExtern", "extra.src.lib.Extra", RS_EXACT "|\"count\":2"));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcExtern", "util.src.inner.Hidden", RS_EXACT));
    /* level 3 before level 4: a dependency renamed `Option` is that crate,
     * not the prelude's type (the second of the two references above) */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Option::Extra", ""));
    /* a dependency without a path, and the standard library */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "serde::Serialize", "external"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "std::fmt::Debug", "external"));
    /* level 4: the std prelude and the primitive types */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "String", "external"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "u8", "external"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "str::len", "external"));
    /* a module named like a primitive type: rustdoc wants a disambiguator */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "char", "ambiguous"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "prim@char", "external"));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcStd", "core.src.char", RS_UNIQUE));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "prim@Gadget", "missing"));
    /* a doc-link path does not look past a glob of something outside the
     * repository (its names would silently win); an import path does */
    ASSERT_TRUE(rs_row_is(db, "crates/core/src/strict.rs", "util::Tool", "external"));
    ASSERT_TRUE(rs_edge_is(db, "strict.StrictUser", "util.src.lib.Tool", RS_EXACT));
    rs_ws_close(&ws);
    PASS();
}

/* `crate`, `self`, `super`, `Self`, and what a path walks through. */
TEST(doc_mentions_rs_paths_and_modules) {
    rs_ws_t ws;
    ASSERT_TRUE(rs_ws_open(&ws));
    const char *db = ws.db;
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcKeywords", "shapes.Kind", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcKeywords", "core.src.lib.Gadget", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcKeywords", "core.src.lib", RS_EXACT)); /* the root module */
    /* `super`, twice: the file that declares the module, then the crate root */
    ASSERT_TRUE(rs_edge_is(db, "round.Ball", "shapes.Circle", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "round.Ball", "core.src.lib.Gadget", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "round.Ball", "shapes.Kind", RS_EXACT));
    /* `Self`: the documented type (a reference to itself is no edge), the
     * impl's self type, the trait */
    ASSERT_TRUE(rs_edge_is(db, "shapes.Circle", "shapes.Circle.area", RS_EXACT));
    ASSERT_TRUE(rs_no_edge(db, "shapes.Circle", "shapes.Circle"));
    ASSERT_TRUE(rs_row_is(db, "crates/core/src/shapes.rs", "Self", ""));
    ASSERT_TRUE(rs_edge_is(db, "shapes.Circle.area", "shapes.Circle.radius", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "shapes.Draw.draw", "shapes.Draw.outline", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "shapes.Circle.draw", "shapes.Circle.area", RS_EXACT));
    /* a field's doc */
    ASSERT_TRUE(rs_edge_is(db, "shapes.Circle.radius", "shapes.Circle.area", RS_EXACT));
    /* where `mod x;` files are: beside a mod.rs, under the directory of any
     * other file, at a `#[path]`, under an inline module's directory */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcModules", "deep.pit.Hole", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcModules", "shapes.round.Ball", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcModules", "elsewhere.odd.Odd", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcModules", "outer_inline.leaf.Leaf", RS_EXACT));
    /* two files for one module name under different cfg */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "imp::Thing", "ambiguous"));
    /* imports: a chain of renaming re-exports, a chain of globs, a cycle */
    ASSERT_TRUE(rs_edge_is(db, "chain.ChainUser", "chain.Deep", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "chain.ChainUser", "chain.ViaGlobs", RS_UNIQUE));
    ASSERT_TRUE(rs_row_is(db, "crates/core/src/chain.rs", "c1::Loop", "graph_gap"));
    /* one module file under two crate roots: what one root declares binds,
     * what both declare is ambiguous */
    ASSERT_TRUE(rs_edge_is(db, "common.Shared", "twin.src.lib.Only", RS_EXACT));
    ASSERT_TRUE(rs_row_is(db, "crates/twin/src/common.rs", "crate::Both", "ambiguous"));
    /* a package's other targets see its lib by name */
    ASSERT_TRUE(rs_edge_is(db, "bin.tool.run_tool", "twin.src.lib.Only", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "tests.it.it_works", "core.src.lib.Gadget", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "tests.it.it_works", "shapes.Circle.area", RS_EXACT));
    /* edition 2015: a `use` path starts at the crate root */
    ASSERT_TRUE(rs_edge_is(db, "old.src.b.UsesThing", "old.src.a.Thing", RS_EXACT));
    rs_ws_close(&ws);
    PASS();
}

/* Associated items, namespaces, disambiguators. */
TEST(doc_mentions_rs_members_and_kinds) {
    rs_ws_t ws;
    ASSERT_TRUE(rs_ws_open(&ws));
    const char *db = ws.db;
    /* a method, with and without `()`, and through a type alias: one edge */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcMembers", "shapes.Circle.area", RS_EXACT "|\"count\":3"));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcMembers", "shapes.Circle.radius", RS_EXACT));
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Field' AND "
                           "qualified_name LIKE '%.shapes.Circle.radius'"),
              1);
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcMembers", "shapes.Draw.draw", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcMembers", "shapes.Ring", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcMembers", "shapes.Pair", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcMembers", "shapes.Bits", RS_EXACT));
    /* a field and a method of one name: the method (rustdoc's order) */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcMembers", "shapes.Lamp.glow", RS_EXACT));
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label = 'Method' AND "
                           "qualified_name LIKE '%.shapes.Lamp.glow'"),
              1);
    /* an item of a trait impl needs its trait in scope where the link is
     * written: not in lib.rs, but in shapes.rs, where a default item of the
     * trait is found through the impl as well */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Circle::draw", "missing"));
    ASSERT_TRUE(rs_edge_is(db, "shapes.Circle.area", "shapes.Circle.draw", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "shapes.Circle.area", "shapes.Draw.outline", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "shapes.Circle.area", "shapes.Draw.draw", RS_EXACT));
    /* declared, but without a graph node */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Circle::UNIT", "graph_gap"));        /* impl constant */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "shapes::Kind::Round", "graph_gap")); /* variant */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "shapes::Pair::0", "graph_gap"));     /* tuple field */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Circle::clone", "graph_gap"));       /* #[derive(Clone)] */
    /* not declared at all; a tuple struct is no function */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Circle::no_such", "missing"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "shapes::Pair()", "missing"));
    /* a function and a macro of one name */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "dual", "ambiguous"));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcNamespaces", "core.src.lib.dual", "\"count\":2"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "dual!", "graph_gap"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "macro@dual", "graph_gap"));
    /* a disambiguator of another kind is no match */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcKinds", "core.src.lib.Gadget", "\"count\":2"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "enum@Gadget", "missing"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "fn@Gadget", "missing"));
    /* a trait and a derive macro of one name are the trait */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcKinds", "core.src.lib.Paintable", RS_UNIQUE));
    /* declared twice under cfg: one definition with its variants on one node,
     * so the item binds */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "plat", ""));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcKinds", "core.src.lib.plat", RS_UNIQUE));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcKinds", "core.src.lib.Twin", RS_UNIQUE));
    rs_ws_close(&ws);
    PASS();
}

/* Every reason, with the cases that are easy to mislabel. */
TEST(doc_mentions_rs_reasons) {
    rs_ws_t ws;
    ASSERT_TRUE(rs_ws_open(&ws));
    const char *db = ws.db;
    /* missing: nothing of that name where the language looks */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "NoSuchName", "missing"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "shapes::NoSuch", "missing"));
    /* a name that is no ASCII is a name like any other */
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcReasons",
                           "lib.Gr\xC3\xB6\xC3\x9F"
                           "e",
                           RS_UNIQUE));
    ASSERT_TRUE(rs_row_is(db, RS_LIB,
                          "\xC3\x9C"
                          "bel",
                          "missing"));
    /* graph_gap: an inline module has no node, its items have */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "outer_inline", "graph_gap"));
    ASSERT_TRUE(rs_edge_is(db, "lib.SrcReasons", "lib.InInline", RS_EXACT));
    /* graph_gap: declared by a macro, in its body or at the invocation */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Knob", "graph_gap"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Made", "graph_gap"));
    /* test_only_target: product code never binds test code ... */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "tests_support::Fixture", "test_only_target"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "only_in_tests", "test_only_target"));
    /* ... test code binds both */
    ASSERT_TRUE(rs_edge_is(db, "tests_support.Fixture", "core.src.lib.Gadget", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(db, "tests_support.Fixture", "lib.only_in_tests", RS_EXACT));
    /* unparseable: a link whose destination is no path */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Circle::area(x)", "unparseable"));
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "Circle#a#b", "unparseable"));
    /* ambiguous */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "imp::Thing", "ambiguous"));
    /* external */
    ASSERT_TRUE(rs_row_is(db, RS_LIB, "serde::Serialize", "external"));
    /* a file with a parse error: what stands before the error resolves what
     * it can name for sure; a name the file does not show is a gap, not the
     * prelude's; what follows the error has no scope */
    ASSERT_TRUE(rs_edge_is(db, "broken.Whole", "core.src.lib.Gadget", RS_EXACT));
    ASSERT_TRUE(rs_row_is(db, "crates/core/src/broken.rs", "Vec", "graph_gap"));
    ASSERT_TRUE(rs_row_is(db, "crates/core/src/broken.rs", "crate::Shape", "graph_gap"));
    /* a file no crate root reaches: its `crate` is not known, its package is */
    ASSERT_TRUE(rs_row_is(db, "crates/core/src/orphan.rs", "crate::Gadget", "graph_gap"));
    ASSERT_TRUE(rs_row_is(db, "crates/core/src/orphan.rs", "String", "external"));
    ASSERT_TRUE(rs_row_is(db, "crates/core/src/orphan.rs", "serde::Serialize", "external"));
    /* no reason outside the core's set, and none left as an unknown one */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason NOT IN "
                           "('missing','ambiguous','external','test_only_target','graph_gap',"
                           "'unparseable')"),
              0);
    rs_ws_close(&ws);
    PASS();
}

/* ── resolution: structure nobody controls ───────────────────────── */

/* Deep nesting, long chains, cycles and crowds: every one resolves or ends
 * in a row; none may hang, crash or pick something at random. */
TEST(doc_mentions_rs_untrusted_structure) {
    enum { DEPTH = 600, CHAIN = 100, CROWD = 3000, RING = 40, BUF = 1 << 20 };
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dmrs_un_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char *src = malloc(BUF);
    ASSERT_NOT_NULL(src);
    th_write_file(TH_PATH(tmp, "Cargo.toml"), "[package]\nname = \"wild\"\nedition = \"2021\"\n");
    th_write_file(TH_PATH(tmp, "src/lib.rs"),
                  "pub mod nest;\npub mod globs;\npub mod ring;\npub mod crowd;\n"
                  "/// The root's target.\npub struct Top;\n");

    /* inline modules nested DEPTH deep; the innermost names the root through
     * `crate`, and through more `super`s than a path carries */
    size_t w = 0;
    for (int i = 0; i < DEPTH; i++) {
        w += (size_t)snprintf(src + w, BUF - w, "pub mod m {\n");
    }
    w += (size_t)snprintf(src + w, BUF - w, "/// [crate::Top] [");
    for (int i = 0; i < CBM_DOCLINK_RS_MAX_SEGS + 4; i++) {
        w += (size_t)snprintf(src + w, BUF - w, "super::");
    }
    w += (size_t)snprintf(src + w, BUF - w, "Top]\npub struct Innermost;\n");
    for (int i = 0; i < DEPTH; i++) {
        w += (size_t)snprintf(src + w, BUF - w, "}\n");
    }
    th_write_file(TH_PATH(tmp, "src/nest.rs"), src);

    /* a chain of re-exporting globs longer than is followed, and a short one */
    w = (size_t)snprintf(src, BUF, "use self::g0::*;\nuse self::s0::*;\n");
    for (int i = 0; i < CHAIN; i++) {
        w += (size_t)snprintf(src + w, BUF - w, "mod g%d { pub use super::g%d::*; }\n", i, i + 1);
    }
    w += (size_t)snprintf(src + w, BUF - w,
                          "mod g%d { /// Far.\n pub struct Far; }\n"
                          "mod s0 { pub use super::s1::*; }\n"
                          "mod s1 { /// Near.\n pub struct Near; }\n"
                          "/// [`Far`] [`Near`]\npub struct GlobUser;\n",
                          CHAIN);
    th_write_file(TH_PATH(tmp, "src/globs.rs"), src);

    /* a ring of imports, each waiting for the next */
    w = 0;
    for (int i = 0; i < RING; i++) {
        w += (size_t)snprintf(src + w, BUF - w, "mod r%d { pub use super::r%d::Round; }\n", i,
                              (i + 1) % RING);
    }
    w += (size_t)snprintf(src + w, BUF - w, "/// [r0::Round]\npub struct RingUser;\n");
    th_write_file(TH_PATH(tmp, "src/ring.rs"), src);

    /* thousands of declarations of one name, and of impls of one type */
    w = 0;
    for (int i = 0; i < CROWD; i++) {
        w += (size_t)snprintf(src + w, BUF - w, "#[cfg(c%d)]\npub fn many() {}\n", i);
    }
    w += (size_t)snprintf(src + w, BUF - w, "/// One type, many impls.\npub struct Hub;\n");
    for (int i = 0; i < CROWD; i++) {
        w += (size_t)snprintf(src + w, BUF - w,
                              "impl From<[u8; %d]> for Hub {\n"
                              "    fn from(_: [u8; %d]) -> Self { Hub }\n}\n",
                              i, i);
    }
    w += (size_t)snprintf(src + w, BUF - w,
                          "/// [`many`] [`Hub::from`] [`Hub`]\n"
                          "pub struct CrowdUser;\n");
    th_write_file(TH_PATH(tmp, "src/crowd.rs"), src);
    free(src);

    char db[512];
    snprintf(db, sizeof(db), "%s/wild.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    /* depth is no limit for `crate`; a path of more segments than are carried
     * is no path */
    ASSERT_TRUE(rs_edge_is(db, "nest.Innermost", "src.lib.Top", RS_EXACT));
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = "
                           "'src/nest.rs' AND reason = 'unparseable'"),
              1);
    /* a glob chain that is followed, and one that is not: a gap, never a miss */
    ASSERT_TRUE(rs_edge_is(db, "globs.GlobUser", "globs.Near", RS_UNIQUE));
    ASSERT_TRUE(rs_row_is(db, "src/globs.rs", "Far", "graph_gap"));
    /* a cycle of imports is not followed */
    ASSERT_TRUE(rs_row_is(db, "src/ring.rs", "r0::Round", "graph_gap"));
    /* a crowd of one name is ambiguous, whatever its size */
    ASSERT_TRUE(rs_row_is(db, "src/crowd.rs", "many", "ambiguous"));
    ASSERT_TRUE(rs_row_is(db, "src/crowd.rs", "Hub::from", "ambiguous"));
    ASSERT_TRUE(rs_edge_is(db, "crowd.CrowdUser", "crowd.Hub", RS_UNIQUE));
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── which scope changes are repairable file by file ─────────────── */

static int rs_delta(const char *before, const char *after, char *names, size_t cap) {
    return dm_scope_delta(CBM_LANG_RUST, "src/s.rs", before, after, names, cap);
}

#define RS_D_USE "use std::fmt;\n"
#define RS_D_STRUCT "pub struct W {\n    pub size: u32,\n    pub name: u32,\n}\n"
#define RS_D_IMPL_OPEN "impl W {\n"
#define RS_D_RUN "    pub fn run(&self) {}\n"
#define RS_D_STOP "    pub fn stop(&self) {}\n"
#define RS_D_LIMIT "    pub const LIMIT: u32 = 1;\n"
#define RS_D_IMPL_CLOSE "}\n"
#define RS_D_ENUM "pub enum Kind {\n    A,\n    B,\n}\n"
#define RS_D_FREE "pub fn free() {}\n"
#define RS_D_OTHER "pub struct Other;\n"
#define RS_D_IMPL RS_D_IMPL_OPEN RS_D_RUN RS_D_STOP RS_D_LIMIT RS_D_IMPL_CLOSE
#define RS_D_TAIL RS_D_ENUM RS_D_FREE RS_D_OTHER

TEST(doc_mentions_rs_scope_delta_rules) {
    const char *base = RS_D_USE RS_D_STRUCT RS_D_IMPL RS_D_TAIL;
    struct {
        const char *what;
        const char *after;
        int want;
        const char *names;
    } cases[] = {
        {"unchanged", base, CBM_DOCLINK_DELTA_LOCAL, ""},
        /* the persisted scope carries no line number, no body and no doc */
        {"lines moved", "// moved\n\n" RS_D_USE RS_D_STRUCT RS_D_IMPL RS_D_TAIL,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"body edit",
         RS_D_USE RS_D_STRUCT RS_D_IMPL_OPEN
         "    pub fn run(&self) {\n        let _ = 1;\n    }\n" RS_D_STOP RS_D_LIMIT RS_D_IMPL_CLOSE
             RS_D_TAIL,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"doc edit",
         RS_D_USE "/// Now documented: [`Other`], [`size`].\n" RS_D_STRUCT RS_D_IMPL RS_D_TAIL,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        /* a member's own visibility is no part of what a path can name */
        {"method made private",
         RS_D_USE RS_D_STRUCT RS_D_IMPL_OPEN
         "    fn run(&self) {}\n" RS_D_STOP RS_D_LIMIT RS_D_IMPL_CLOSE RS_D_TAIL,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        /* a removed member is reported by name */
        {"method removed",
         RS_D_USE RS_D_STRUCT RS_D_IMPL_OPEN RS_D_RUN RS_D_LIMIT RS_D_IMPL_CLOSE RS_D_TAIL,
         CBM_DOCLINK_DELTA_LOCAL, "stop"},
        {"field removed", RS_D_USE "pub struct W {\n    pub size: u32,\n}\n" RS_D_IMPL RS_D_TAIL,
         CBM_DOCLINK_DELTA_LOCAL, "name"},
        {"constant removed",
         RS_D_USE RS_D_STRUCT RS_D_IMPL_OPEN RS_D_RUN RS_D_STOP RS_D_IMPL_CLOSE RS_D_TAIL,
         CBM_DOCLINK_DELTA_LOCAL, "LIMIT"},
        {"two members removed",
         RS_D_USE "pub struct W {\n    pub size: u32,\n}\n" RS_D_IMPL_OPEN RS_D_RUN RS_D_LIMIT
             RS_D_IMPL_CLOSE RS_D_TAIL,
         CBM_DOCLINK_DELTA_LOCAL, "name,stop"},
        /* everything else can re-route references of files with no edge here */
        {"method added",
         RS_D_USE RS_D_STRUCT RS_D_IMPL_OPEN RS_D_RUN RS_D_STOP RS_D_LIMIT
         "    pub fn more(&self) {}\n" RS_D_IMPL_CLOSE RS_D_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"method renamed",
         RS_D_USE RS_D_STRUCT RS_D_IMPL_OPEN
         "    pub fn go(&self) {}\n" RS_D_STOP RS_D_LIMIT RS_D_IMPL_CLOSE RS_D_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"members reordered",
         RS_D_USE RS_D_STRUCT RS_D_IMPL_OPEN RS_D_STOP RS_D_RUN RS_D_LIMIT RS_D_IMPL_CLOSE
             RS_D_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* a variant can be imported under another name: no name finds the
         * references that depended on it */
        {"variant removed",
         RS_D_USE RS_D_STRUCT RS_D_IMPL "pub enum Kind {\n    A,\n}\n" RS_D_FREE RS_D_OTHER,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* ... and so can every item */
        {"function removed", RS_D_USE RS_D_STRUCT RS_D_IMPL RS_D_ENUM RS_D_OTHER,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"type removed", RS_D_USE RS_D_STRUCT RS_D_IMPL RS_D_ENUM RS_D_FREE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"type added", RS_D_USE RS_D_STRUCT RS_D_IMPL RS_D_TAIL "pub struct New;\n",
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"item made private", RS_D_USE RS_D_STRUCT RS_D_IMPL RS_D_ENUM RS_D_FREE "struct Other;\n",
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"item made test-only",
         RS_D_USE RS_D_STRUCT RS_D_IMPL RS_D_ENUM RS_D_FREE "#[cfg(test)]\n" RS_D_OTHER,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"derive added", RS_D_USE "#[derive(Clone)]\n" RS_D_STRUCT RS_D_IMPL RS_D_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"import added", RS_D_USE "use std::io;\n" RS_D_STRUCT RS_D_IMPL RS_D_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"import removed", RS_D_STRUCT RS_D_IMPL RS_D_TAIL, CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"module declared", RS_D_USE "mod extra;\n" RS_D_STRUCT RS_D_IMPL RS_D_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"impl header changed",
         RS_D_USE RS_D_STRUCT
         "impl fmt::Debug for W {\n" RS_D_RUN RS_D_STOP RS_D_LIMIT RS_D_IMPL_CLOSE RS_D_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* an impl is more than its members: through it a type takes the
         * default items of a trait, which live in another file */
        {"impl removed", RS_D_USE RS_D_STRUCT RS_D_TAIL, CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* a parse error where there was none */
        {"parse error", RS_D_USE RS_D_STRUCT RS_D_IMPL RS_D_TAIL "pub struct {\n",
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"file deleted", NULL, CBM_DOCLINK_DELTA_GLOBAL, NULL},
    };
    char names[256];
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        int got = rs_delta(base, cases[i].after, names, sizeof(names));
        if (got != cases[i].want || (cases[i].names && strcmp(names, cases[i].names) != 0)) {
            printf("  %s: delta %d [%s], want %d [%s]\n", cases[i].what, got, names, cases[i].want,
                   cases[i].names ? cases[i].names : "any");
            FAIL("scope delta");
        }
    }
    /* a member whose name the incremental route cannot look for in a row
     * (it splits rows at every byte that is no ASCII letter, digit or `_`) */
    const char *wide = "pub struct W {\n    pub gr\xC3\xB6\xC3\x9F"
                       "e: u32,\n    pub b: u32,\n}\n";
    ASSERT_EQ(rs_delta(wide, "pub struct W {\n    pub b: u32,\n}\n", names, sizeof(names)),
              CBM_DOCLINK_DELTA_GLOBAL);
    /* a raw identifier is reported as the name a reference writes */
    ASSERT_EQ(rs_delta("pub struct W {\n    pub r#type: u32,\n    pub b: u32,\n}\n",
                       "pub struct W {\n    pub b: u32,\n}\n", names, sizeof(names)),
              CBM_DOCLINK_DELTA_LOCAL);
    ASSERT_STR_EQ(names, "type");
    /* a scope that appears is as global as one that leaves */
    ASSERT_EQ(rs_delta(NULL, base, names, sizeof(names)), CBM_DOCLINK_DELTA_GLOBAL);

    /* a manifest: only what the resolver reads of it counts, and every such
     * change is global (it re-scopes the files of its package) */
    const char *toml = "[package]\nname = \"p\"\nedition = \"2021\"\n\n"
                       "[dependencies]\nserde = \"1\"\nutil = { path = \"../util\" }\n";
    struct {
        const char *what;
        const char *after;
        int want;
    } manifests[] = {
        {"unchanged", toml, CBM_DOCLINK_DELTA_LOCAL},
        {"comment and version",
         "# a comment\n[package]\nname = \"p\"\nedition = \"2021\"\nversion = \"2.0.0\"\n\n"
         "[dependencies]\nserde = \"2\"\nutil = { path = \"../util\" }\n",
         CBM_DOCLINK_DELTA_LOCAL},
        {"dependency added",
         "[package]\nname = \"p\"\nedition = \"2021\"\n\n"
         "[dependencies]\nserde = \"1\"\nutil = { path = \"../util\" }\nmore = \"1\"\n",
         CBM_DOCLINK_DELTA_GLOBAL},
        {"path changed",
         "[package]\nname = \"p\"\nedition = \"2021\"\n\n"
         "[dependencies]\nserde = \"1\"\nutil = { path = \"../other\" }\n",
         CBM_DOCLINK_DELTA_GLOBAL},
        {"edition changed",
         "[package]\nname = \"p\"\nedition = \"2018\"\n\n"
         "[dependencies]\nserde = \"1\"\nutil = { path = \"../util\" }\n",
         CBM_DOCLINK_DELTA_GLOBAL},
    };
    for (size_t i = 0; i < sizeof(manifests) / sizeof(manifests[0]); i++) {
        int got = dm_scope_delta(CBM_LANG_TOML, "Cargo.toml", toml, manifests[i].after, names,
                                 sizeof(names));
        if (got != manifests[i].want) {
            printf("  manifest %s: delta %d, want %d\n", manifests[i].what, got, manifests[i].want);
            FAIL("manifest scope delta");
        }
    }
    PASS();
}

/* ── incremental == full ─────────────────────────────────────────── */

static const char RS_INC_LIB[] = "//! The crate: [`Target`].\n"
                                 "mod a;\n"
                                 "mod b;\n"
                                 "mod user;\n"
                                 "mod rows;\n"
                                 "pub use a::Target;\n"
                                 "use b::Other;\n";
static const char RS_INC_LIB_DOC[] = "//! The crate: [`Target`] and [`Other`].\n"
                                     "mod a;\n"
                                     "mod b;\n"
                                     "mod user;\n"
                                     "mod rows;\n"
                                     "pub use a::Target;\n"
                                     "use b::Other;\n";
#define RS_INC_A_HEAD "/// The target.\npub struct Target;\nimpl Target {\n"
#define RS_INC_A_GO "    /// Go.\n    pub fn go(&self) {}\n"
#define RS_INC_A_STOP "    /// Stop.\n    pub fn stop(&self) {}\n"
/* two impl blocks with a method of one name: the later one owns the node */
#define RS_INC_A_TWIN1                                                         \
    "impl Target {\n    /// Twin.\n    pub fn twin(&self) {}\n    /// Kept.\n" \
    "    pub fn keep(&self) {}\n}\n"
#define RS_INC_A_KEEP "impl Target {\n    /// Kept.\n    pub fn keep(&self) {}\n}\n"
#define RS_INC_A_TWIN2 "impl Target {\n    /// Twin.\n    pub fn twin(&self) {}\n}\n"
#define RS_INC_A_DUP "/// A duplicate.\npub struct Dup;\n"
static const char RS_INC_A[] =
    RS_INC_A_HEAD RS_INC_A_GO RS_INC_A_STOP "}\n" RS_INC_A_TWIN1 RS_INC_A_TWIN2 RS_INC_A_DUP;
static const char RS_INC_A_BODY[] =
    RS_INC_A_HEAD "    /// Go.\n    pub fn go(&self) {\n        let _ = 1;\n    }\n" RS_INC_A_STOP
                  "}\n" RS_INC_A_TWIN1 RS_INC_A_TWIN2 RS_INC_A_DUP;
static const char RS_INC_A_NO_STOP[] = RS_INC_A_HEAD
    "    /// Go.\n    pub fn go(&self) {\n        let _ = 1;\n    }\n}\n" RS_INC_A_TWIN1
        RS_INC_A_TWIN2 RS_INC_A_DUP;
static const char RS_INC_A_ONE_TWIN[] = RS_INC_A_HEAD
    "    /// Go.\n    pub fn go(&self) {\n        let _ = 1;\n    }\n}\n" RS_INC_A_KEEP
        RS_INC_A_TWIN2 RS_INC_A_DUP;
static const char RS_INC_A_RENAMED[] =
    "/// The target.\npub struct Renamed;\nimpl Renamed {\n" RS_INC_A_GO "}\n" RS_INC_A_DUP;
static const char RS_INC_B[] = "/// A duplicate.\npub struct Dup;\n/// Other.\npub struct Other;\n";
static const char RS_INC_B_NO_DUP[] = "/// Other.\npub struct Other;\n";
static const char RS_INC_USER[] =
    "use crate::a::*;\n"
    "use crate::b::*;\n"
    "/// [`Target`] [`Target::go`] [`Target::stop`] [`Dup`] [`Missing`]\n"
    "pub struct User;\n";
static const char RS_INC_USER_DOC[] =
    "use crate::a::*;\n"
    "use crate::b::*;\n"
    "/// [`Target`] [`Target::go`] [`Target::stop`] [`Dup`] [`Missing`] [`Other`]\n"
    "pub struct User;\n";
/* its only reference is unresolved: it has no edge into the file it names */
static const char RS_INC_ROWS[] = "/// [crate::a::Target::twin]\npub struct Rows;\n";

TEST(doc_mentions_rs_incremental_equals_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dmrs_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    th_write_file(TH_PATH(repo, "Cargo.toml"), "[package]\nname = \"inc\"\nedition = \"2021\"\n");
    th_write_file(TH_PATH(repo, "src/lib.rs"), RS_INC_LIB);
    th_write_file(TH_PATH(repo, "src/a.rs"), RS_INC_A);
    th_write_file(TH_PATH(repo, "src/b.rs"), RS_INC_B);
    th_write_file(TH_PATH(repo, "src/user.rs"), RS_INC_USER);
    th_write_file(TH_PATH(repo, "src/rows.rs"), RS_INC_ROWS);
    char inc_db[512];
    char full_db[512];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    const char *file = "src.lib.rs.__file__";
    const char *user_rs = "src/user.rs";
    /* the preconditions the steps below move away from */
    ASSERT_TRUE(rs_edge_is(inc_db, file, "a.Target", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(inc_db, "user.User", "a.Target", RS_UNIQUE));
    ASSERT_TRUE(rs_edge_is(inc_db, "user.User", "a.Target.go", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(inc_db, "user.User", "a.Target.stop", RS_EXACT));
    ASSERT_TRUE(rs_row_is(inc_db, user_rs, "Dup", "ambiguous")); /* a::Dup and b::Dup */
    ASSERT_TRUE(rs_row_is(inc_db, user_rs, "Missing", "missing"));
    ASSERT_TRUE(rs_row_is(inc_db, "src/rows.rs", "crate::a::Target::twin", "ambiguous"));

    /* a doc edit re-extracts the file alone */
    th_write_file(TH_PATH(repo, "src/user.rs"), RS_INC_USER_DOC);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "doc edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(rs_edge_is(inc_db, "user.User", "b.Other", RS_UNIQUE));

    /* a reference in the FILE's own doc, added by an edit of that doc alone */
    th_write_file(TH_PATH(repo, "src/lib.rs"), RS_INC_LIB_DOC);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "file doc edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    ASSERT_TRUE(rs_edge_is(inc_db, file, "b.Other", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(inc_db, file, "a.Target", RS_EXACT));

    /* a body edit in a file others point into: its scope is unchanged, the
     * edges into it stand (from an item and from a file's own doc) */
    th_write_file(TH_PATH(repo, "src/a.rs"), RS_INC_A_BODY);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "target body edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
        0);
    ASSERT_TRUE(rs_edge_is(inc_db, "user.User", "a.Target.go", RS_EXACT));
    ASSERT_TRUE(rs_edge_is(inc_db, file, "a.Target", RS_EXACT));

    /* a removed member: the file with an edge to it gets a row instead */
    th_write_file(TH_PATH(repo, "src/a.rs"), RS_INC_A_NO_STOP);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "member removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(rs_no_edge(inc_db, "user.User", "a.Target.stop"));
    ASSERT_TRUE(rs_row_is(inc_db, user_rs, "Target::stop", "missing"));

    /* ... and a file with NO edge into the changed one, whose row names the
     * removed member, is resolved again: two candidates shrink to one */
    th_write_file(TH_PATH(repo, "src/a.rs"), RS_INC_A_ONE_TWIN);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "twin removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    ASSERT_TRUE(rs_edge_is(inc_db, "rows.Rows", "a.Target.twin", RS_EXACT));

    /* a removed item is not repaired file by file: one of two ambiguous
     * candidates goes, the other binds */
    th_write_file(TH_PATH(repo, "src/b.rs"), RS_INC_B_NO_DUP);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "item removed", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_TRUE(rs_edge_is(inc_db, "user.User", "a.Dup", RS_UNIQUE));

    /* renaming a target */
    th_write_file(TH_PATH(repo, "src/a.rs"), RS_INC_A_RENAMED);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "rename", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_TRUE(rs_row_is(inc_db, user_rs, "Target", "missing"));
    ASSERT_TRUE(rs_row_is(inc_db, "src/lib.rs", "Target", "graph_gap")); /* `pub use a::Target` */
    ASSERT_TRUE(rs_edge_is(inc_db, file, "b.Other", RS_EXACT));

    /* the manifest scopes every file of its package */
    th_write_file(
        TH_PATH(repo, "Cargo.toml"),
        "[package]\nname = \"inc\"\nedition = \"2021\"\n\n[dependencies]\nserde = \"1\"\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "manifest", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);

    /* deleting a file others point into */
    ASSERT_EQ(unlink(TH_PATH(repo, "src/b.rs")), 0);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "delete file", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_TRUE(rs_no_edge(inc_db, "user.User", "b.Other"));
    ASSERT_EQ(dm_mentions_from(inc_db, file), 0);

    dm_unlink_db(inc_db);
    dm_unlink_db(full_db);
    th_rmtree(tmp);
    PASS();
}

/* ── the parallel path == the sequential path ────────────────────── */

enum { RS_RING = 64 }; /* above MIN_FILES_FOR_PARALLEL */

/* Every module documents its successor (through `crate`), a shared target (by
 * a glob, by a path into its methods) and a name nothing declares; the root
 * documents the hub in its own doc. */
static void rs_write_ring_fixture(const char *repo) {
    char path[512];
    char body[1024];
    char lib[8192];
    size_t w = (size_t)snprintf(lib, sizeof(lib), "//! The ring's [`hub::Hub`].\npub mod hub;\n");
    for (int i = 0; i < RS_RING; i++) {
        w += (size_t)snprintf(lib + w, sizeof(lib) - w, "pub mod m%02d;\n", i);
        snprintf(path, sizeof(path), "%s/src/m%02d.rs", repo, i);
        snprintf(body, sizeof(body),
                 "use crate::hub::*;\n"
                 "/// Next [crate::m%02d::C%02d], shared [`Hub`], [`Hub::run`], [`Nothing%02d`].\n"
                 "pub struct C%02d;\n",
                 (i + 1) % RS_RING, (i + 1) % RS_RING, i, i);
        th_write_file(path, body);
    }
    th_write_file(TH_PATH(repo, "src/lib.rs"), lib);
    th_write_file(TH_PATH(repo, "src/hub.rs"), "/// The hub.\n"
                                               "pub struct Hub;\n"
                                               "impl Hub {\n"
                                               "    /// Runs.\n"
                                               "    pub fn run(&self) {}\n"
                                               "}\n");
    th_write_file(TH_PATH(repo, "Cargo.toml"), "[package]\nname = \"ring\"\nedition = \"2021\"\n");
}

TEST(doc_mentions_rs_parallel_equals_sequential) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dmrs_par_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    rs_write_ring_fixture(repo);
    char par_db[512];
    char seq_db[512];
    snprintf(par_db, sizeof(par_db), "%s/par.db", tmp);
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", tmp);
    ASSERT_EQ(dm_workers_agree(repo, par_db, seq_db), 0);
    /* per module: its successor, the hub, the hub's method; the root: the hub */
    ASSERT_EQ(dm_count(par_db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'"),
              RS_RING * 3 + 1);
    /* per module: the undeclared name */
    ASSERT_EQ(dm_count(par_db, "SELECT COUNT(*) FROM doc_link_unresolved"), RS_RING);
    ASSERT_EQ(dm_count(par_db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'missing'"),
              RS_RING);
    dm_unlink_db(par_db);
    dm_unlink_db(seq_db);
    th_rmtree(tmp);
    PASS();
}

SUITE(doc_mentions_rs) {
    dm_ship_held_families();
    RUN_TEST(doc_mentions_rs_tokens);
    RUN_TEST(doc_mentions_rs_item_doc_is_not_file_doc);
    RUN_TEST(doc_mentions_rs_untrusted_text);
    RUN_TEST(doc_mentions_rs_parse_path);
    RUN_TEST(doc_mentions_rs_scope_blob);
    RUN_TEST(doc_mentions_rs_scope_parse_errors);
    RUN_TEST(doc_mentions_rs_manifest_blob);
    RUN_TEST(doc_mentions_rs_families_and_tiers);
    RUN_TEST(doc_mentions_rs_file_level_docs);
    RUN_TEST(doc_mentions_rs_lookup_levels);
    RUN_TEST(doc_mentions_rs_paths_and_modules);
    RUN_TEST(doc_mentions_rs_members_and_kinds);
    RUN_TEST(doc_mentions_rs_reasons);
    RUN_TEST(doc_mentions_rs_untrusted_structure);
    RUN_TEST(doc_mentions_rs_scope_delta_rules);
    RUN_TEST(doc_mentions_rs_incremental_equals_full);
    RUN_TEST(doc_mentions_rs_parallel_equals_sequential);
}
