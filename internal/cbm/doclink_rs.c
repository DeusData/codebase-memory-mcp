/*
 * doclink_rs.c — Rust intra-doc links and the Rust doc-link scope.
 *
 * Tokens. Only what rustdoc itself documents is read: `///` (not `////`),
 * a block opened by slash-star-star (not by three stars), `//!` and the bang
 * block. The Markdown is read the way pulldown-cmark reads it for rustdoc:
 * fenced and indented code blocks, HTML blocks, code spans, inline HTML and
 * autolinks hold no links; link reference definitions are collected first;
 * inline `[text](dest)`, full `[text][label]`, collapsed `[label][]` and
 * shortcut `[label]` links are found, images, footnotes and task markers are
 * not links. A link is a reference when rustdoc's `preprocess_link` keeps it.
 * Families:
 *   intra_doc  the destination is written at the link: `[text](path)`, and
 *              `[path]`, `[path][]`, `[text][path]` whose label has no
 *              definition
 *   link_def   the destination comes from a link reference definition
 *   href       (the core's) an inline or defined destination that is a URL
 * A relative file destination and a page anchor are not harvested.
 *
 * Scope blob: one record per line, tab-separated, first line "rs1". Records
 * marked (fresh) exist only in the blob of the extraction itself and are
 * dropped by the persisted form, which also zeroes every line number:
 *   B                                 the tree has a parse error, or holds
 *                                     something this scan cannot represent:
 *                                     a name the file's modules do not show
 *                                     may still be declared there
 *   E  line                   (fresh) first line that is not trusted: a
 *                                     definition from there on has no scope
 *   N  id parent start end flags name inline module (the file itself is
 *                                     module 0)
 *   D  mod flags name path            `mod name;` declared in module `mod`;
 *                                     path is the `#[path]` literal or '-'
 *   U  mod vis kind local path        b named import, s `self` import (type
 *                                     namespace only), g glob, x extern crate
 *   T  mod start end flags kind name node extra
 *                                     item. kind: S struct E enum U union
 *                                     R trait A alias F fn C const G static
 *                                     M macro_rules V derive macro
 *                                     B attribute macro P function-like
 *                                     proc macro. extra: an alias's target
 *                                     path; the derive names of a struct,
 *                                     enum or union ("Clone,Debug", '*' for
 *                                     names that were not read); '-'
 *                                     otherwise
 *   M  start kind name node           member of the preceding T (trait,
 *                                     struct, union, enum) or I record:
 *                                     f function, c constant, t type,
 *                                     d field, v variant
 *   I  mod start end self trait       impl block; self '?' when it is not a
 *                                     path, trait '-' when inherent
 *   Q  mod name                       a name a macro invocation may declare
 *                                     in module `mod`; '*': somewhere in the
 *                                     file (it follows a parse error)
 *   V  mod name                       module `mod` invokes the macro `name`
 *                                     among its items
 *   W  name names                     the `macro_rules!` macro `name` declares
 *                                     these names (comma-joined) wherever it
 *                                     is invoked
 *   P  line names             (fresh) parameter and type parameter names of
 *                                     the documented definition at `line`
 *   Z  line suffix            (fresh) a documented definition that does not
 *                                     own its node
 * flags: visibility (p pub, c crate, s super, i in-path, - private), then
 * 't' for `#[cfg(test)]` / `#[test]`, 'e' for `#[macro_export]`, 'c' for a
 * `mod` whose file cannot be told from the tree (a conditional path
 * attribute; an inline module with a path attribute, which moves the files of
 * the `mod x;` inside it).
 * node: '!' when the definition has no graph node of its own, else a label
 * code (S Struct, E Enum, C Class, I Interface, T Type, F Function,
 * V Variable, M Method, D Field) followed by '=' when the node's qualified
 * name is <module>.<name>, or by ':' and the part after "<module>.".
 *
 * Every item is matched to its definition in ctx->result->defs, so a node's
 * qualified name is the extractor's own (cfg suffixes, impl self-type text).
 * The graph keeps one node per qualified name: a definition owns it only when
 * it is the survivor among the file's definitions of that name (largest start
 * line, then name, then label -- cbm_gbuf_upsert_node's rule).
 *
 * Everything is derived from the tree and the definitions extracted from it,
 * so the blob is a pure function of the file's bytes.
 */
#include "doclink.h"

#include "arena.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"
#include "helpers.h" /* cbm_fqn_compute: the qualified names the extractor gives */
#include "tree_sitter/api.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Small arena string builder ──────────────────────────────────── */

typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool failed;
} rs_sb_t;

enum { RS_SB_INIT = 1024, RS_UINT_DIGITS = 16 };

static void rs_sb_reserve(rs_sb_t *sb, size_t extra) {
    if (sb->failed || sb->len + extra + SKIP_ONE <= sb->cap) {
        return;
    }
    size_t ncap = sb->cap ? sb->cap : RS_SB_INIT;
    while (ncap < sb->len + extra + SKIP_ONE) {
        ncap *= PAIR_LEN;
    }
    char *grown = (char *)cbm_arena_alloc(sb->a, ncap);
    if (!grown) {
        sb->failed = true;
        return;
    }
    if (sb->len > 0) {
        memcpy(grown, sb->buf, sb->len);
    }
    sb->buf = grown;
    sb->cap = ncap;
}

static void rs_sb_putn(rs_sb_t *sb, const char *s, size_t n) {
    rs_sb_reserve(sb, n);
    if (sb->failed) {
        return;
    }
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

static void rs_sb_puts(rs_sb_t *sb, const char *s) {
    rs_sb_putn(sb, s, strlen(s));
}

static void rs_sb_putc(rs_sb_t *sb, char c) {
    rs_sb_putn(sb, &c, SKIP_ONE);
}

static void rs_sb_putu(rs_sb_t *sb, uint32_t v) {
    char tmp[RS_UINT_DIGITS];
    int n = snprintf(tmp, sizeof(tmp), "%u", v);
    if (n > 0) {
        rs_sb_putn(sb, tmp, (size_t)n);
    }
}

/* ── Characters ──────────────────────────────────────────────────── */

/* UTF-8 lead bytes and the code-point ranges that are neither letters nor
 * digits (rustdoc asks char::is_alphanumeric; this is that test without the
 * Unicode tables: every non-ASCII code point counts as a letter except the
 * punctuation, symbol and private-use blocks below). */
enum {
    RS_U8_ASCII_END = 0x80,
    RS_U8_LEAD2 = 0xC0,
    RS_U8_LEAD3 = 0xE0,
    RS_U8_LEAD4 = 0xF0,
    RS_U8_LEAD_END = 0xF8,
    RS_U8_CONT_MASK = 0xC0,
    RS_U8_CONT = 0x80,
    RS_U8_CONT_BITS = 6,
    RS_U8_CONT_VALUE = 0x3F,
    RS_U8_LEN2 = 2,
    RS_U8_LEN3 = 3,
    RS_U8_LEN4 = 4,
    RS_U8_MASK2 = 0x1F,
    RS_U8_MASK3 = 0x0F,
    RS_U8_MASK4 = 0x07,
    RS_CP_LATIN1_LETTERS = 0xC0, /* below: Latin-1 punctuation and symbols */
    RS_CP_FEMININE = 0xAA,
    RS_CP_MICRO = 0xB5,
    RS_CP_MASCULINE = 0xBA,
    RS_CP_TIMES = 0xD7,
    RS_CP_DIVIDE = 0xF7,
    RS_CP_PUNCT_FIRST = 0x2000, /* general punctuation .. misc symbols and arrows */
    RS_CP_PUNCT_LAST = 0x2BFF,
    RS_CP_CJK_PUNCT_FIRST = 0x3000,
    RS_CP_CJK_PUNCT_LAST = 0x303F,
    RS_CP_PRIVATE_FIRST = 0xE000,
    RS_CP_PRIVATE_LAST = 0xF8FF,
    RS_CP_VARIATION_FIRST = 0xFE00,
    RS_CP_VARIATION_LAST = 0xFE6F,
    RS_CP_SPECIALS_FIRST = 0xFFF0,
    RS_CP_SPECIALS_LAST = 0xFFFF,
    RS_CP_PICTO_FIRST = 0x1F000, /* emoji and pictographs */
    RS_CP_PICTO_LAST = 0x1FFFF,
    RS_CP_TAGS_FIRST = 0xE0000,
};

/* One code point at s[0..n): its value, or -1 for a byte that starts none;
 * *adv is the number of bytes to step over (at least 1). */
static int32_t rs_u8_decode(const unsigned char *s, size_t n, size_t *adv) {
    *adv = SKIP_ONE;
    if (n == 0) {
        return CBM_NOT_FOUND;
    }
    unsigned char b = s[0];
    if (b < RS_U8_ASCII_END) {
        return b;
    }
    size_t want = 0;
    int32_t cp = 0;
    if (b >= RS_U8_LEAD4 && b < RS_U8_LEAD_END) {
        want = RS_U8_LEN4;
        cp = b & RS_U8_MASK4;
    } else if (b >= RS_U8_LEAD3 && b < RS_U8_LEAD4) {
        want = RS_U8_LEN3;
        cp = b & RS_U8_MASK3;
    } else if (b >= RS_U8_LEAD2 && b < RS_U8_LEAD3) {
        want = RS_U8_LEN2;
        cp = b & RS_U8_MASK2;
    } else {
        return CBM_NOT_FOUND;
    }
    if (want > n) {
        return CBM_NOT_FOUND;
    }
    for (size_t i = SKIP_ONE; i < want; i++) {
        if ((s[i] & RS_U8_CONT_MASK) != RS_U8_CONT) {
            return CBM_NOT_FOUND;
        }
        cp = (cp << RS_U8_CONT_BITS) | (s[i] & RS_U8_CONT_VALUE);
    }
    *adv = want;
    return cp;
}

static bool rs_cp_in(int32_t cp, int32_t first, int32_t last) {
    return cp >= first && cp <= last;
}

static bool rs_cp_alnum(int32_t cp) {
    if (cp < 0) {
        return false;
    }
    if (cp < RS_U8_ASCII_END) {
        return isalnum(cp) != 0;
    }
    if (cp < RS_CP_LATIN1_LETTERS) {
        return cp == RS_CP_FEMININE || cp == RS_CP_MICRO || cp == RS_CP_MASCULINE;
    }
    if (cp == RS_CP_TIMES || cp == RS_CP_DIVIDE) {
        return false;
    }
    return !(rs_cp_in(cp, RS_CP_PUNCT_FIRST, RS_CP_PUNCT_LAST) ||
             rs_cp_in(cp, RS_CP_CJK_PUNCT_FIRST, RS_CP_CJK_PUNCT_LAST) ||
             rs_cp_in(cp, RS_CP_PRIVATE_FIRST, RS_CP_PRIVATE_LAST) ||
             rs_cp_in(cp, RS_CP_VARIATION_FIRST, RS_CP_VARIATION_LAST) ||
             rs_cp_in(cp, RS_CP_SPECIALS_FIRST, RS_CP_SPECIALS_LAST) ||
             rs_cp_in(cp, RS_CP_PICTO_FIRST, RS_CP_PICTO_LAST) || cp >= RS_CP_TAGS_FIRST);
}

/* True when every character of s[0..n) is a letter, a digit or one of
 * `extra`: rustdoc's test for text that can be a path. */
static bool rs_chars_ok(const char *s, size_t n, const char *extra) {
    const unsigned char *u = (const unsigned char *)s;
    for (size_t i = 0; i < n;) {
        size_t adv = SKIP_ONE;
        int32_t cp = rs_u8_decode(u + i, n - i, &adv);
        bool ok = rs_cp_alnum(cp) || (cp > 0 && cp < RS_U8_ASCII_END && strchr(extra, cp) != NULL);
        if (!ok) {
            return false;
        }
        i += adv;
    }
    return true;
}

/* A path segment: an identifier (optionally raw, `r#name`), or digits only (a
 * tuple field). */
static bool rs_is_segment(const char *s, size_t n) {
    static const char raw_prefix[] = "r#";
    const size_t rl = sizeof(raw_prefix) - SKIP_ONE;
    if (n > rl && memcmp(s, raw_prefix, rl) == 0) {
        s += rl;
        n -= rl;
    }
    if (n == 0) {
        return false;
    }
    bool digits = true;
    const unsigned char *u = (const unsigned char *)s;
    for (size_t i = 0; i < n;) {
        size_t adv = SKIP_ONE;
        int32_t cp = rs_u8_decode(u + i, n - i, &adv);
        if (!(rs_cp_alnum(cp) || cp == '_')) {
            return false;
        }
        digits = digits && cp < RS_U8_ASCII_END && isdigit(cp);
        i += adv;
    }
    return digits || !isdigit(u[0]);
}

/* ── The path grammar (rustdoc's preprocess_link) ────────────────── */

static const char RS_PATH_CHARS[] = ":_<>, !*&;";        /* should_ignore_link */
static const char RS_PATH_CHARS_DIS[] = ":_<>, !*&;@()"; /* ..._with_disambiguators */

static int rs_disambiguator(const char *word, size_t len) {
    static const struct {
        const char *word;
        int dis;
    } table[] = {
        {"struct", CBM_DOCLINK_RS_DIS_STRUCT},   {"enum", CBM_DOCLINK_RS_DIS_ENUM},
        {"trait", CBM_DOCLINK_RS_DIS_TRAIT},     {"union", CBM_DOCLINK_RS_DIS_UNION},
        {"module", CBM_DOCLINK_RS_DIS_MOD},      {"mod", CBM_DOCLINK_RS_DIS_MOD},
        {"const", CBM_DOCLINK_RS_DIS_CONST},     {"constant", CBM_DOCLINK_RS_DIS_CONST},
        {"static", CBM_DOCLINK_RS_DIS_STATIC},   {"function", CBM_DOCLINK_RS_DIS_FN},
        {"fn", CBM_DOCLINK_RS_DIS_FN},           {"method", CBM_DOCLINK_RS_DIS_FN},
        {"derive", CBM_DOCLINK_RS_DIS_DERIVE},   {"field", CBM_DOCLINK_RS_DIS_FIELD},
        {"variant", CBM_DOCLINK_RS_DIS_VARIANT}, {"type", CBM_DOCLINK_RS_DIS_TYPE_NS},
        {"value", CBM_DOCLINK_RS_DIS_VALUE_NS},  {"macro", CBM_DOCLINK_RS_DIS_MACRO_NS},
        {"prim", CBM_DOCLINK_RS_DIS_PRIM},       {"primitive", CBM_DOCLINK_RS_DIS_PRIM},
        {"tyalias", CBM_DOCLINK_RS_DIS_TYALIAS}, {"typealias", CBM_DOCLINK_RS_DIS_TYALIAS},
    };
    for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
        if (strlen(table[i].word) == len && memcmp(table[i].word, word, len) == 0) {
            return table[i].dis;
        }
    }
    return CBM_DOCLINK_RS_DIS_NONE;
}

typedef struct {
    char *s;
    size_t n;
} rs_text_t;

static void rs_text_trim(rs_text_t *t) {
    while (t->n > 0 && isspace((unsigned char)t->s[0])) {
        t->s++;
        t->n--;
    }
    while (t->n > 0 && isspace((unsigned char)t->s[t->n - SKIP_ONE])) {
        t->n--;
    }
}

/* The `()`, `!` and `!()` suffixes: the kind they ask for, with the suffix
 * cut off. A suffix that would leave nothing is part of the path (`!` is the
 * never type, `()` the unit type). */
static int rs_cut_suffix(rs_text_t *t) {
    static const struct {
        const char *suffix;
        int dis;
    } table[] = {
        {"!()", CBM_DOCLINK_RS_DIS_MACRO_BANG}, {"!{}", CBM_DOCLINK_RS_DIS_MACRO_BANG},
        {"![]", CBM_DOCLINK_RS_DIS_MACRO_BANG}, {"()", CBM_DOCLINK_RS_DIS_FN},
        {"!", CBM_DOCLINK_RS_DIS_MACRO_BANG},
    };
    for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
        size_t sl = strlen(table[i].suffix);
        if (t->n > sl && memcmp(t->s + t->n - sl, table[i].suffix, sl) == 0) {
            t->n -= sl;
            return table[i].dis;
        }
    }
    return CBM_DOCLINK_RS_DIS_NONE;
}

/* One `::`-separated piece with its generics removed, appended to `out`
 * (joined by `::`). false when the angle brackets do not balance or hold
 * fully-qualified syntax (`<T as Trait>`). */
static bool rs_strip_segment(const char *seg, size_t n, char *out, size_t *w) {
    static const char as_kw[] = " as ";
    const size_t as_len = sizeof(as_kw) - SKIP_ONE;
    int depth = 0;
    size_t start = *w;
    size_t chunk = 0; /* start of the innermost generics text read so far */
    if (start > 0) {
        out[(*w)++] = ':';
        out[(*w)++] = ':';
    }
    size_t body = *w;
    for (size_t i = 0; i < n; i++) {
        char c = seg[i];
        if (c == '<') {
            depth++;
            chunk = i + SKIP_ONE;
        } else if (c == '>') {
            depth--;
            for (size_t k = chunk; k + as_len <= i; k++) {
                if (memcmp(seg + k, as_kw, as_len) == 0) {
                    return false;
                }
            }
        } else if (depth == 0) {
            out[(*w)++] = c;
        }
    }
    if (depth != 0) {
        return false;
    }
    if (*w == body) {
        *w = start; /* nothing left of it: no separator either */
    }
    return true;
}

/* rustdoc's strip_generics_from_path, in place: `Vec<T>::new` -> `Vec::new`.
 * false for malformed generics. A path without angle brackets is left as it
 * is. */
static bool rs_strip_generics(rs_text_t *t) {
    if (!memchr(t->s, '<', t->n) && !memchr(t->s, '>', t->n)) {
        return true;
    }
    char seg[CBM_DOCLINK_RS_RAW_CAP];
    char out[CBM_DOCLINK_RS_RAW_CAP + PAIR_LEN];
    size_t sn = 0;
    size_t w = 0;
    size_t i = 0;
    while (i < t->n) {
        char c = t->s[i++];
        if (c == ':') {
            if (i >= t->n || t->s[i] != ':') {
                return false; /* a single colon */
            }
            i++;
            if (!rs_strip_segment(seg, sn, out, &w)) {
                return false;
            }
            sn = 0;
        } else if (c == '<') {
            seg[sn++] = c;
            if (i >= t->n) {
                break;
            }
            char d = t->s[i++];
            if (d == '<' || d == '>') {
                return false; /* `<<` or `<>` */
            }
            seg[sn++] = d;
            while (i < t->n && t->s[i] != '>') {
                seg[sn++] = t->s[i++];
            }
        } else {
            seg[sn++] = c;
        }
    }
    if (sn > 0 && !rs_strip_segment(seg, sn, out, &w)) {
        return false;
    }
    if (w == 0) {
        return false;
    }
    memcpy(t->s, out, w);
    t->n = w;
    return true;
}

/* rustdoc's spellings of primitive types that are no identifiers. */
static bool rs_symbol_primitive(const char *s, size_t n) {
    static const char *const names[] = {"!", "&", "&mut", "&str", "*const", "*mut"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        if (strlen(names[i]) == n && memcmp(names[i], s, n) == 0) {
            return true;
        }
    }
    return false;
}

/* Split a generics-free path at `::` into out->segs. */
static bool rs_split_segments(const char *s, size_t n, CBMDocLinkRsPath *out) {
    size_t i = 0;
    if (n >= PAIR_LEN && s[0] == ':' && s[1] == ':') {
        out->global = true;
        i = PAIR_LEN;
    }
    while (i <= n) {
        size_t e = i;
        while (e < n && !(s[e] == ':' && e + SKIP_ONE < n && s[e + SKIP_ONE] == ':')) {
            e++;
        }
        size_t len = e - i;
        if (out->nsegs >= CBM_DOCLINK_RS_MAX_SEGS || len >= CBM_DOCLINK_RS_SEG_CAP ||
            !rs_is_segment(s + i, len)) {
            return false;
        }
        memcpy(out->segs[out->nsegs], s + i, len);
        out->segs[out->nsegs][len] = '\0';
        out->nsegs++;
        if (e >= n) {
            break;
        }
        i = e + PAIR_LEN;
    }
    return out->nsegs > 0;
}

CBMDocLinkRsPathStatus cbm_doclink_rs_parse_path(const char *raw, size_t len, bool lenient,
                                                 CBMDocLinkRsPath *out) {
    memset(out, 0, sizeof(*out));
    char buf[CBM_DOCLINK_RS_RAW_CAP];
    if (!raw || len >= sizeof(buf)) {
        return CBM_DOCLINK_RS_PATH_MALFORMED;
    }
    memcpy(buf, raw, len);
    buf[len] = '\0';
    rs_text_t t = {buf, len};
    rs_text_trim(&t);
    if (t.n == 0) {
        return CBM_DOCLINK_RS_PATH_NOT_LINK;
    }
    char *at = (char *)memchr(t.s, '@', t.n);
    if (at) {
        out->dis = rs_disambiguator(t.s, (size_t)(at - t.s));
        if (out->dis == CBM_DOCLINK_RS_DIS_NONE) {
            /* an unknown prefix: rustdoc reports it unless the text could not
             * be a link anyway */
            return rs_chars_ok(t.s, t.n, RS_PATH_CHARS_DIS) ? CBM_DOCLINK_RS_PATH_MALFORMED
                                                            : CBM_DOCLINK_RS_PATH_NOT_LINK;
        }
        t.n -= (size_t)(at + SKIP_ONE - t.s);
        t.s = at + SKIP_ONE;
        rs_text_trim(&t);
    } else {
        out->dis = rs_cut_suffix(&t);
    }
    if (lenient && !rs_chars_ok(t.s, t.n, RS_PATH_CHARS)) {
        return CBM_DOCLINK_RS_PATH_NOT_LINK;
    }
    if (t.n > SKIP_ONE && t.s[0] == '!') {
        /* rustdoc: never a path (`!` alone is the never type). Admonitions
         * such as `[!NOTE]` are written this way. */
        return CBM_DOCLINK_RS_PATH_NOT_LINK;
    }
    if (rs_symbol_primitive(t.s, t.n)) {
        memcpy(out->segs[0], t.s, t.n);
        out->segs[0][t.n] = '\0';
        out->nsegs = SKIP_ONE;
        return CBM_DOCLINK_RS_PATH_OK;
    }
    if (!rs_strip_generics(&t)) {
        return CBM_DOCLINK_RS_PATH_MALFORMED;
    }
    if (memchr(t.s, ' ', t.n)) {
        return CBM_DOCLINK_RS_PATH_NOT_LINK; /* prose in brackets */
    }
    return rs_split_segments(t.s, t.n, out) ? CBM_DOCLINK_RS_PATH_OK
                                            : CBM_DOCLINK_RS_PATH_MALFORMED;
}

/* ── Doc text -> Markdown lines ──────────────────────────────────── */

enum { RS_L_BLANK = 0, RS_L_CODE, RS_L_TEXT, RS_L_DEF };
enum { RS_DOC_NONE = 0, RS_DOC_LINE, RS_DOC_BLOCK };
enum { RS_MARK_LEN = 3 }; /* `///`, `//!`, slash-star-star, slash-star-bang */

typedef struct {
    const char *p; /* content: after the comment marker, later after block markers */
    uint32_t len;
    uint32_t line; /* 1-based source line */
    uint32_t boff; /* offset of the content in its paragraph buffer */
    uint8_t kind;  /* RS_L_* */
    bool para_start;
} rs_line_t;

static CBMArena *rs_scratch(CBMExtractCtx *ctx) {
    return ctx->scratch ? ctx->scratch : ctx->arena;
}

/* Which kind of rustdoc comment `doc` starts with (a plain `//`, a `////`
 * line and a three-star block are comments, not documentation). */
static int rs_doc_style(const char *doc) {
    if (doc[0] != '/') {
        return RS_DOC_NONE;
    }
    if (doc[1] == '/') {
        bool outer = doc[2] == '/' && doc[3] != '/';
        return (outer || doc[2] == '!') ? RS_DOC_LINE : RS_DOC_NONE;
    }
    if (doc[1] == '*') {
        bool outer = doc[2] == '*' && doc[3] != '*' && doc[3] != '/';
        return (outer || doc[2] == '!') ? RS_DOC_BLOCK : RS_DOC_NONE;
    }
    return RS_DOC_NONE;
}

static bool rs_blank(const char *p, uint32_t n) {
    for (uint32_t i = 0; i < n; i++) {
        if (p[i] != ' ' && p[i] != '\t' && p[i] != '\r') {
            return false;
        }
    }
    return true;
}

/* The number of spaces and tabs `p` (`n` bytes) starts with. */
static uint32_t rs_indent_len(const char *p, uint32_t n) {
    uint32_t i = 0;
    while (i < n && (p[i] == ' ' || p[i] == '\t')) {
        i++;
    }
    return i;
}

/* The content of one `///` or `//!` line; nothing for any other line. */
static void rs_line_content(rs_line_t *ln) {
    const char *p = ln->p;
    uint32_t n = ln->len;
    uint32_t i = rs_indent_len(p, n);
    const char *mark = p + i;
    uint32_t rest = n - i;
    if (rest < RS_MARK_LEN || mark[0] != '/' || mark[SKIP_ONE] != '/') {
        ln->len = 0;
        return;
    }
    /* `//!` is a doc line, `///` too unless a fourth slash makes it `////` */
    bool inner = mark[PAIR_LEN] == '!';
    bool outer = mark[PAIR_LEN] == '/' && !(rest > RS_MARK_LEN && mark[RS_MARK_LEN] == '/');
    if (!inner && !outer) {
        ln->len = 0;
        return;
    }
    ln->p = mark + RS_MARK_LEN;
    ln->len = rest - RS_MARK_LEN;
}

/* A block comment's decoration: when every line after the first that has text
 * starts with a star, the stars go (rustdoc's beautify_doc_string). */
static void rs_block_stars(rs_line_t *lines, int n) {
    bool any = false;
    for (int i = SKIP_ONE; i < n; i++) {
        const rs_line_t *ln = &lines[i];
        uint32_t k = 0;
        while (k < ln->len && (ln->p[k] == ' ' || ln->p[k] == '\t')) {
            k++;
        }
        if (k >= ln->len) {
            continue; /* blank */
        }
        if (ln->p[k] != '*') {
            return;
        }
        any = true;
    }
    for (int i = SKIP_ONE; any && i < n; i++) {
        rs_line_t *ln = &lines[i];
        uint32_t k = 0;
        while (k < ln->len && (ln->p[k] == ' ' || ln->p[k] == '\t')) {
            k++;
        }
        if (k < ln->len) {
            ln->p += k + SKIP_ONE;
            ln->len -= k + SKIP_ONE;
        }
    }
}

/* Remove the indentation every line with text shares (rustdoc's
 * unindent_doc_fragments). */
static void rs_unindent(rs_line_t *lines, int n) {
    uint32_t min = UINT32_MAX;
    for (int i = 0; i < n; i++) {
        const rs_line_t *ln = &lines[i];
        if (rs_blank(ln->p, ln->len)) {
            continue;
        }
        uint32_t k = 0;
        while (k < ln->len && (ln->p[k] == ' ' || ln->p[k] == '\t')) {
            k++;
        }
        min = k < min ? k : min;
    }
    if (min == UINT32_MAX || min == 0) {
        return;
    }
    for (int i = 0; i < n; i++) {
        rs_line_t *ln = &lines[i];
        if (ln->len >= min && !rs_blank(ln->p, ln->len)) {
            ln->p += min;
            ln->len -= min;
        }
    }
}

/* Split the stored doc text into Markdown lines. NULL (and *out_n 0) when it
 * is no rustdoc comment or memory ran out. */
static rs_line_t *rs_doc_lines(CBMArena *a, const char *doc, uint32_t doc_line, int *out_n) {
    *out_n = 0;
    int style = rs_doc_style(doc);
    if (style == RS_DOC_NONE) {
        return NULL;
    }
    size_t total = strlen(doc);
    const char *body = doc;
    if (style == RS_DOC_BLOCK) {
        while (total > 0 && isspace((unsigned char)doc[total - SKIP_ONE])) {
            total--;
        }
        if (total >= RS_MARK_LEN + PAIR_LEN && doc[total - PAIR_LEN] == '*' &&
            doc[total - SKIP_ONE] == '/') {
            total -= PAIR_LEN;
        }
    }
    int count = SKIP_ONE;
    for (size_t i = 0; i < total; i++) {
        count += doc[i] == '\n';
    }
    rs_line_t *lines = (rs_line_t *)cbm_arena_alloc(a, (size_t)count * sizeof(rs_line_t));
    if (!lines) {
        return NULL;
    }
    int n = 0;
    size_t start = 0;
    for (size_t i = 0; i <= total && n < count; i++) {
        if (i < total && body[i] != '\n') {
            continue;
        }
        size_t end = i;
        while (end > start && body[end - SKIP_ONE] == '\r') {
            end--;
        }
        lines[n] = (rs_line_t){.p = body + start,
                               .len = (uint32_t)(end - start),
                               .line = doc_line + (uint32_t)n,
                               .kind = RS_L_BLANK};
        n++;
        start = i + SKIP_ONE;
    }
    if (style == RS_DOC_LINE) {
        for (int i = 0; i < n; i++) {
            rs_line_content(&lines[i]);
        }
    } else {
        lines[0].p += RS_MARK_LEN;
        lines[0].len = lines[0].len >= RS_MARK_LEN ? lines[0].len - RS_MARK_LEN : 0;
        rs_block_stars(lines, n);
    }
    rs_unindent(lines, n);
    *out_n = n;
    return lines;
}

/* text[0..*n) without the white space at both ends. */
static const char *rs_trimmed(const char *text, size_t *n) {
    while (*n > 0 && isspace((unsigned char)text[0])) {
        text++;
        (*n)--;
    }
    while (*n > 0 && isspace((unsigned char)text[*n - SKIP_ONE])) {
        (*n)--;
    }
    return text;
}

/* Give the lines of a file's own doc their real source lines.
 *
 * The inner docs of a file are every `//!` line and bang block before its
 * first item, joined: plain comments, blank lines and `#![..]` attributes
 * between them are not part of the text, so counting from the first line
 * drifts. Each doc line is looked up in the source instead, forward from the
 * line before it (a doc line equals its source line up to indentation).
 * Nothing changes unless every line is found. */
static void rs_lines_resync(CBMExtractCtx *ctx, rs_line_t *lines, int n, const char *doc,
                            uint32_t doc_line) {
    const char *src = ctx->source;
    size_t src_len = ctx->source_len > 0 ? (size_t)ctx->source_len : 0;
    uint32_t *found = (uint32_t *)cbm_arena_alloc(rs_scratch(ctx), (size_t)n * sizeof(uint32_t));
    if (!src || !found || doc_line == 0) {
        return;
    }
    size_t pos = 0; /* start of source line `at` */
    uint32_t at = SKIP_ONE;
    while (at < doc_line && pos < src_len) {
        const char *nl = (const char *)memchr(src + pos, '\n', src_len - pos);
        pos = nl ? (size_t)(nl - src) + SKIP_ONE : src_len;
        at++;
    }
    const char *d = doc;
    for (int i = 0; i < n; i++) {
        const char *dnl = strchr(d, '\n');
        size_t dlen = dnl ? (size_t)(dnl - d) : strlen(d);
        const char *want = rs_trimmed(d, &dlen);
        bool matched = false;
        while (!matched && pos < src_len) {
            const char *nl = (const char *)memchr(src + pos, '\n', src_len - pos);
            size_t slen = nl ? (size_t)(nl - (src + pos)) : src_len - pos;
            const char *have = rs_trimmed(src + pos, &slen);
            matched = slen == dlen && memcmp(have, want, dlen) == 0;
            found[i] = at;
            pos = nl ? (size_t)(nl - src) + SKIP_ONE : src_len;
            at++;
        }
        if (!matched) {
            return;
        }
        d = dnl ? dnl + SKIP_ONE : d + strlen(d);
    }
    for (int i = 0; i < n; i++) {
        lines[i].line = found[i];
    }
}

/* ── Markdown blocks ─────────────────────────────────────────────── */

enum {
    RS_MD_CODE_INDENT = 4, /* columns that make an indented code block */
    RS_MD_TAB = 4,
    RS_MD_FENCE_MIN = 3,
    RS_MD_ATX_MAX = 6,
    RS_MD_ORDERED_DIGITS = 9,
    RS_MD_LIST_INIT = 8,     /* open list items: the stack grows */
    RS_MD_LABEL_MAX = 999,   /* CommonMark's limit for a link label */
    RS_MD_TAG_NAME_MAX = 16, /* longest HTML block tag name + 1 */
    RS_HTML_NONE = 0,
    RS_HTML_RAW = 1,     /* pre, script, style, textarea: until the closing tag */
    RS_HTML_COMMENT = 2, /* until --> */
    RS_HTML_PI = 3,      /* until ?> */
    RS_HTML_DECL = 4,    /* until > */
    RS_HTML_CDATA = 5,   /* until ]]> */
    RS_HTML_BLOCK = 6,   /* a block tag: until a blank line */
    RS_HTML_ANY = 7,     /* any single tag alone on its line: until a blank line */
};

typedef struct {
    const char *label; /* normalized: trimmed, whitespace collapsed, ASCII lower case */
    const char *dest;
    uint32_t dest_len;
    int order;
} rs_def_t;

typedef struct {
    CBMArena *a;
    rs_def_t *v;
    int n;
    int cap;
    bool failed;
} rs_defs_t;

typedef struct {
    bool fence;
    char fence_ch;
    uint32_t fence_len;
    int html; /* RS_HTML_* of the open HTML block */
    bool para;
    bool quoted; /* the open paragraph is inside a block quote */
    CBMArena *a;
    int *list; /* content column of every open list item */
    int nlist;
    int list_cap;
    bool failed; /* out of memory: the doc text gets no tokens */
} rs_md_t;

/* A cursor in one line: byte position and the column it stands at. */
typedef struct {
    const char *p;
    uint32_t n;
    uint32_t pos;
    int col;
} rs_cur_t;

static void rs_cur_skip_ws(rs_cur_t *c) {
    while (c->pos < c->n && (c->p[c->pos] == ' ' || c->p[c->pos] == '\t')) {
        c->col += c->p[c->pos] == '\t' ? RS_MD_TAB - (c->col % RS_MD_TAB) : SKIP_ONE;
        c->pos++;
    }
}

static bool rs_cur_eol(const rs_cur_t *c) {
    return rs_blank(c->p + c->pos, c->n - c->pos);
}

/* An opening code fence at the cursor: its character and length. */
static bool rs_md_fence_open(const rs_cur_t *c, char *ch, uint32_t *len) {
    if (c->pos >= c->n || (c->p[c->pos] != '`' && c->p[c->pos] != '~')) {
        return false;
    }
    char f = c->p[c->pos];
    uint32_t k = c->pos;
    while (k < c->n && c->p[k] == f) {
        k++;
    }
    if (k - c->pos < RS_MD_FENCE_MIN) {
        return false;
    }
    if (f == '`' && memchr(c->p + k, '`', c->n - k)) {
        return false; /* the info string of a backtick fence holds no backtick */
    }
    *ch = f;
    *len = k - c->pos;
    return true;
}

static bool rs_md_fence_close(const rs_line_t *ln, const rs_md_t *st) {
    rs_cur_t c = {ln->p, ln->len, 0, 0};
    rs_cur_skip_ws(&c);
    uint32_t k = c.pos;
    while (k < c.n && c.p[k] == st->fence_ch) {
        k++;
    }
    return k - c.pos >= st->fence_len && rs_blank(c.p + k, c.n - k);
}

/* `---`, `***`, `___` (three or more, spaces allowed between). */
static bool rs_md_thematic(const rs_cur_t *c) {
    if (c->pos >= c->n) {
        return false;
    }
    char m = c->p[c->pos];
    if (m != '-' && m != '*' && m != '_') {
        return false;
    }
    int count = 0;
    for (uint32_t k = c->pos; k < c->n; k++) {
        char ch = c->p[k];
        if (ch == m) {
            count++;
        } else if (ch != ' ' && ch != '\t' && ch != '\r') {
            return false;
        }
    }
    return count >= RS_MD_FENCE_MIN;
}

/* `# heading`: the cursor moves to the heading's text. */
static bool rs_md_atx(rs_cur_t *c) {
    uint32_t k = c->pos;
    while (k < c->n && c->p[k] == '#') {
        k++;
    }
    uint32_t hashes = k - c->pos;
    if (hashes == 0 || hashes > RS_MD_ATX_MAX || (k < c->n && c->p[k] != ' ' && c->p[k] != '\t')) {
        return false;
    }
    c->col += (int)hashes;
    c->pos = k;
    return true;
}

/* A list marker at the cursor: its width in bytes, 0 when there is none.
 * *first is false for an ordered marker other than `1`, which cannot
 * interrupt a paragraph. */
static uint32_t rs_md_list_marker(const rs_cur_t *c, bool *first) {
    *first = true;
    uint32_t k = c->pos;
    if (k >= c->n) {
        return 0;
    }
    char m = c->p[k];
    if (m == '-' || m == '+' || m == '*') {
        k++;
    } else if (isdigit((unsigned char)m)) {
        while (k < c->n && isdigit((unsigned char)c->p[k]) && k - c->pos < RS_MD_ORDERED_DIGITS) {
            k++;
        }
        if (k >= c->n || (c->p[k] != '.' && c->p[k] != ')')) {
            return 0;
        }
        *first = k - c->pos == SKIP_ONE && m == '1';
        k++;
    } else {
        return 0;
    }
    if (k < c->n && c->p[k] != ' ' && c->p[k] != '\t' && c->p[k] != '\r') {
        return 0;
    }
    return k - c->pos;
}

static bool rs_ci_prefix(const char *p, uint32_t n, const char *word) {
    size_t wl = strlen(word);
    if (n < wl) {
        return false;
    }
    for (size_t i = 0; i < wl; i++) {
        if (tolower((unsigned char)p[i]) != word[i]) {
            return false;
        }
    }
    return true;
}

static bool rs_ci_find(const char *p, uint32_t n, const char *word) {
    size_t wl = strlen(word);
    for (uint32_t i = 0; i + wl <= n; i++) {
        if (rs_ci_prefix(p + i, n - i, word)) {
            return true;
        }
    }
    return false;
}

static bool rs_html_block_tag(const char *name, size_t len) {
    static const char *const tags[] = {
        "address", "article",  "aside",   "base",     "basefont", "blockquote", "body",
        "caption", "center",   "col",     "colgroup", "dd",       "details",    "dialog",
        "dir",     "div",      "dl",      "dt",       "fieldset", "figcaption", "figure",
        "footer",  "form",     "frame",   "frameset", "h1",       "h2",         "h3",
        "h4",      "h5",       "h6",      "head",     "header",   "hr",         "html",
        "iframe",  "legend",   "li",      "link",     "main",     "menu",       "menuitem",
        "nav",     "noframes", "ol",      "optgroup", "option",   "p",          "param",
        "search",  "section",  "summary", "table",    "tbody",    "td",         "tfoot",
        "th",      "thead",    "title",   "tr",       "track",    "ul",
    };
    char low[RS_MD_TAG_NAME_MAX];
    if (len == 0 || len >= sizeof(low)) {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        low[i] = (char)tolower((unsigned char)name[i]);
    }
    low[len] = '\0';
    for (size_t i = 0; i < sizeof(tags) / sizeof(tags[0]); i++) {
        if (strcmp(tags[i], low) == 0) {
            return true;
        }
    }
    return false;
}

/* End of an HTML tag that starts at p[0] == '<' (a quoted attribute value may
 * hold '>'): index just past its '>', or 0 when the line has none. */
static uint32_t rs_html_tag_end(const char *p, uint32_t n) {
    char quote = 0;
    for (uint32_t i = SKIP_ONE; i < n; i++) {
        char c = p[i];
        if (quote) {
            quote = c == quote ? 0 : quote;
        } else if (c == '"' || c == '\'') {
            quote = c;
        } else if (c == '>') {
            return i + SKIP_ONE;
        } else if (c == '<') {
            return 0;
        }
    }
    return 0;
}

/* The HTML block that starts at the cursor (RS_HTML_*), none inside a
 * paragraph for the kind that cannot interrupt one. */
static int rs_md_html_start(const rs_cur_t *c, bool in_para) {
    const char *p = c->p + c->pos;
    uint32_t n = c->n - c->pos;
    if (n < PAIR_LEN || p[0] != '<') {
        return RS_HTML_NONE;
    }
    static const char *const raw[] = {"<pre", "<script", "<style", "<textarea"};
    for (size_t i = 0; i < sizeof(raw) / sizeof(raw[0]); i++) {
        size_t rl = strlen(raw[i]);
        if (rs_ci_prefix(p, n, raw[i]) &&
            (n == rl || p[rl] == ' ' || p[rl] == '\t' || p[rl] == '>' || p[rl] == '\r')) {
            return RS_HTML_RAW;
        }
    }
    if (rs_ci_prefix(p, n, "<!--")) {
        return RS_HTML_COMMENT;
    }
    if (p[1] == '?') {
        return RS_HTML_PI;
    }
    if (rs_ci_prefix(p, n, "<![cdata[")) {
        return RS_HTML_CDATA;
    }
    if (p[1] == '!' && n > PAIR_LEN && isalpha((unsigned char)p[PAIR_LEN])) {
        return RS_HTML_DECL;
    }
    uint32_t k = p[1] == '/' ? PAIR_LEN : SKIP_ONE;
    uint32_t name = k;
    while (k < n && (isalnum((unsigned char)p[k]) || p[k] == '-')) {
        k++;
    }
    if (k == name || !isalpha((unsigned char)p[name])) {
        return RS_HTML_NONE;
    }
    bool ends = k >= n || p[k] == ' ' || p[k] == '\t' || p[k] == '>' || p[k] == '\r' ||
                (p[k] == '/' && k + SKIP_ONE < n && p[k + SKIP_ONE] == '>');
    if (ends && rs_html_block_tag(p + name, k - name)) {
        return RS_HTML_BLOCK;
    }
    uint32_t end = rs_html_tag_end(p, n);
    return (!in_para && end > 0 && rs_blank(p + end, n - end)) ? RS_HTML_ANY : RS_HTML_NONE;
}

/* True when this line ends the open HTML block of kind `html`. */
static bool rs_md_html_end(const rs_line_t *ln, int html) {
    switch (html) {
    case RS_HTML_RAW:
        return rs_ci_find(ln->p, ln->len, "</pre>") || rs_ci_find(ln->p, ln->len, "</script>") ||
               rs_ci_find(ln->p, ln->len, "</style>") || rs_ci_find(ln->p, ln->len, "</textarea>");
    case RS_HTML_COMMENT:
        return rs_ci_find(ln->p, ln->len, "-->");
    case RS_HTML_PI:
        return rs_ci_find(ln->p, ln->len, "?>");
    case RS_HTML_DECL:
        return memchr(ln->p, '>', ln->len) != NULL;
    case RS_HTML_CDATA:
        return rs_ci_find(ln->p, ln->len, "]]>");
    default:
        return false;
    }
}

/* ── Link reference definitions ──────────────────────────────────── */

/* A link label, normalized into the arena: trimmed, inner whitespace
 * collapsed, ASCII letters lower-cased. NULL when it holds no text. */
static const char *rs_label_norm(CBMArena *a, const char *s, size_t n) {
    char *out = (char *)cbm_arena_alloc(a, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    bool space = false;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (isspace(c)) {
            space = w > 0;
            continue;
        }
        if (space) {
            out[w++] = ' ';
            space = false;
        }
        out[w++] = (char)tolower(c);
    }
    out[w] = '\0';
    return w > 0 ? out : NULL;
}

/* End of the label that starts after the '[' at s[from - 1]: the index of its
 * closing ']', or UINT32_MAX when the text is no label (an unescaped bracket
 * inside, too long, or unclosed). */
static uint32_t rs_label_end(const char *s, uint32_t n, uint32_t from) {
    for (uint32_t i = from; i < n && i - from <= RS_MD_LABEL_MAX; i++) {
        char c = s[i];
        if (c == '\\' && i + SKIP_ONE < n) {
            i++;
        } else if (c == '[') {
            return UINT32_MAX;
        } else if (c == ']') {
            return i;
        }
    }
    return UINT32_MAX;
}

/* True when s[0..n) can be a link label: some text, no unescaped bracket, not
 * longer than CommonMark allows. */
static bool rs_label_clean(const char *s, uint32_t n) {
    if (n == 0 || n > RS_MD_LABEL_MAX || rs_blank(s, n)) {
        return false;
    }
    for (uint32_t i = 0; i < n; i++) {
        if (s[i] == '\\' && i + SKIP_ONE < n) {
            i++;
        } else if (s[i] == '[' || s[i] == ']') {
            return false;
        }
    }
    return true;
}

/* A link destination at s[*pos]: `<...>` or a run without spaces and with
 * balanced parentheses. On success *pos moves past it. */
static bool rs_link_dest(const char *s, uint32_t n, uint32_t *pos, const char **dest,
                         uint32_t *dest_len) {
    uint32_t i = *pos;
    if (i < n && s[i] == '<') {
        uint32_t k = i + SKIP_ONE;
        while (k < n && s[k] != '>' && s[k] != '<' && s[k] != '\n') {
            k += (s[k] == '\\' && k + SKIP_ONE < n) ? PAIR_LEN : SKIP_ONE;
        }
        if (k >= n || s[k] != '>') {
            return false;
        }
        *dest = s + i + SKIP_ONE;
        *dest_len = k - i - SKIP_ONE;
        *pos = k + SKIP_ONE;
        return true;
    }
    int depth = 0;
    uint32_t k = i;
    while (k < n) {
        unsigned char c = (unsigned char)s[k];
        if (c == '\\' && k + SKIP_ONE < n) {
            k += PAIR_LEN;
            continue;
        }
        if (c <= ' ') {
            break;
        }
        if (c == '(') {
            depth++;
        } else if (c == ')') {
            if (depth == 0) {
                break;
            }
            depth--;
        }
        k++;
    }
    if (depth != 0) {
        return false;
    }
    *dest = s + i;
    *dest_len = k - i;
    *pos = k;
    return true;
}

/* An optional link title at s[*pos] (`"..."`, `'...'`, `(...)`): *pos moves
 * past it. false when a title starts and does not end. */
static bool rs_link_title(const char *s, uint32_t n, uint32_t *pos) {
    uint32_t i = *pos;
    if (i >= n || (s[i] != '"' && s[i] != '\'' && s[i] != '(')) {
        return true;
    }
    char close = s[i] == '(' ? ')' : s[i];
    for (uint32_t k = i + SKIP_ONE; k < n; k++) {
        if (s[k] == '\\' && k + SKIP_ONE < n) {
            k++;
        } else if (s[k] == close) {
            *pos = k + SKIP_ONE;
            return true;
        }
    }
    return false;
}

static void rs_defs_push(rs_defs_t *d, rs_def_t def) {
    if (d->n >= d->cap) {
        int ncap = d->cap ? d->cap * PAIR_LEN : CBM_SZ_8;
        rs_def_t *grown = (rs_def_t *)cbm_arena_alloc(d->a, (size_t)ncap * sizeof(rs_def_t));
        if (!grown) {
            d->failed = true;
            return;
        }
        if (d->n > 0) {
            memcpy(grown, d->v, (size_t)d->n * sizeof(rs_def_t));
        }
        d->v = grown;
        d->cap = ncap;
    }
    def.order = d->n;
    d->v[d->n++] = def;
}

static uint32_t rs_skip_spaces(const char *s, uint32_t n, uint32_t i) {
    while (i < n && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r')) {
        i++;
    }
    return i;
}

/* A link reference definition `[label]: dest "title"` that starts at the
 * cursor of lines[i]; the destination may stand on the next line. Returns the
 * number of lines it takes (0: this is no definition). */
static int rs_md_definition(rs_line_t *lines, int n, int i, const rs_cur_t *c, rs_defs_t *defs) {
    const char *s = c->p;
    uint32_t len = c->n;
    uint32_t pos = c->pos;
    if (pos >= len || s[pos] != '[' || (pos + SKIP_ONE < len && s[pos + SKIP_ONE] == '^')) {
        return 0;
    }
    uint32_t close = rs_label_end(s, len, pos + SKIP_ONE);
    if (close == UINT32_MAX || close + SKIP_ONE >= len || s[close + SKIP_ONE] != ':') {
        return 0;
    }
    const char *label = rs_label_norm(defs->a, s + pos + SKIP_ONE, close - pos - SKIP_ONE);
    if (!label) {
        return 0;
    }
    int taken = SKIP_ONE;
    uint32_t k = rs_skip_spaces(s, len, close + PAIR_LEN);
    if (k >= len) {
        /* the destination is on the next line */
        if (i + SKIP_ONE >= n || rs_blank(lines[i + SKIP_ONE].p, lines[i + SKIP_ONE].len)) {
            return 0;
        }
        s = lines[i + SKIP_ONE].p;
        len = lines[i + SKIP_ONE].len;
        k = rs_skip_spaces(s, len, 0);
        taken = PAIR_LEN;
    }
    const char *dest = NULL;
    uint32_t dest_len = 0;
    if (!rs_link_dest(s, len, &k, &dest, &dest_len) || dest_len == 0) {
        return 0;
    }
    uint32_t after = rs_skip_spaces(s, len, k);
    if (after < len &&
        (after == k || !rs_link_title(s, len, &after) || rs_skip_spaces(s, len, after) < len)) {
        return 0; /* something that is no title follows the destination */
    }
    rs_defs_push(defs, (rs_def_t){.label = label, .dest = dest, .dest_len = dest_len});
    return taken;
}

static int rs_def_cmp(const void *a, const void *b) {
    const rs_def_t *x = (const rs_def_t *)a;
    const rs_def_t *y = (const rs_def_t *)b;
    int c = strcmp(x->label, y->label);
    return c ? c : (x->order > y->order) - (x->order < y->order);
}

/* The first definition of `label` (normalized), or NULL. */
static const rs_def_t *rs_def_find(const rs_defs_t *d, const char *label) {
    int lo = 0;
    int hi = d->n;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (strcmp(d->v[mid].label, label) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return (lo < d->n && strcmp(d->v[lo].label, label) == 0) ? &d->v[lo] : NULL;
}

/* ── Markdown blocks: one line ───────────────────────────────────── */

static void rs_md_pop_lists(rs_md_t *st, int col) {
    while (st->nlist > 0 && st->list[st->nlist - SKIP_ONE] > col) {
        st->nlist--;
    }
}

/* Column the text of a line is measured from: the content column of the
 * innermost open list item it reaches. The stack is strictly increasing (an
 * item opens to the right of everything still open), so this is a binary
 * search. */
static int rs_md_base(const rs_md_t *st, int col) {
    int lo = 0;
    int hi = st->nlist;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (st->list[mid] <= col) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo > 0 ? st->list[lo - SKIP_ONE] : 0;
}

static void rs_md_push_list(rs_md_t *st, int content) {
    if (st->nlist >= st->list_cap) {
        int ncap = st->list_cap ? st->list_cap * PAIR_LEN : RS_MD_LIST_INIT;
        int *grown = (int *)cbm_arena_alloc(st->a, (size_t)ncap * sizeof(int));
        if (!grown) {
            st->failed = true;
            return;
        }
        if (st->nlist > 0) {
            memcpy(grown, st->list, (size_t)st->nlist * sizeof(int));
        }
        st->list = grown;
        st->list_cap = ncap;
    }
    st->list[st->nlist++] = content;
}

/* Block-quote markers at the cursor; true when there was one. */
static bool rs_md_quote(rs_cur_t *c) {
    bool quoted = false;
    while (c->pos < c->n && c->p[c->pos] == '>') {
        quoted = true;
        c->pos++;
        c->col++;
        if (c->pos < c->n && c->p[c->pos] == ' ') {
            c->pos++;
            c->col++;
        }
        rs_cur_skip_ws(c);
    }
    return quoted;
}

/* Open a list item whose marker is `width` bytes at the cursor; the cursor
 * moves to the item's content (past a task marker). */
static void rs_md_open_item(rs_md_t *st, rs_cur_t *c, uint32_t width) {
    c->pos += width;
    c->col += (int)width;
    int marker_end = c->col;
    rs_cur_skip_ws(c);
    int content = c->col;
    if (rs_cur_eol(c) || content - marker_end > RS_MD_CODE_INDENT) {
        content = marker_end + SKIP_ONE;
    }
    rs_md_push_list(st, content);
    /* a task marker `[ ]` / `[x]` is no link */
    const char *p = c->p + c->pos;
    uint32_t left = c->n - c->pos;
    if (left > RS_MARK_LEN && p[0] == '[' && (p[1] == ' ' || p[1] == 'x' || p[1] == 'X') &&
        p[PAIR_LEN] == ']' && (p[RS_MARK_LEN] == ' ' || p[RS_MARK_LEN] == '\t')) {
        c->pos += RS_MARK_LEN;
        c->col += RS_MARK_LEN;
    }
}

/* What starts at the cursor. */
enum { RS_BLK_TEXT = 0, RS_BLK_FENCE, RS_BLK_HTML, RS_BLK_RULE, RS_BLK_HEADING, RS_BLK_ITEM };

typedef struct {
    int kind;
    char fence_ch;
    uint32_t fence_len;
    int html;
    uint32_t marker; /* width of a list marker */
    bool interrupts; /* may end an open paragraph */
} rs_blk_t;

static rs_blk_t rs_md_block_at(const rs_cur_t *c, bool in_para) {
    rs_blk_t b = {.kind = RS_BLK_TEXT, .interrupts = true};
    if (rs_md_fence_open(c, &b.fence_ch, &b.fence_len)) {
        b.kind = RS_BLK_FENCE;
        return b;
    }
    b.html = rs_md_html_start(c, in_para);
    if (b.html != RS_HTML_NONE) {
        b.kind = RS_BLK_HTML;
        return b;
    }
    if (rs_md_thematic(c)) {
        b.kind = RS_BLK_RULE;
        return b;
    }
    rs_cur_t probe = *c;
    if (rs_md_atx(&probe)) {
        b.kind = RS_BLK_HEADING;
        return b;
    }
    bool first = true;
    b.marker = rs_md_list_marker(c, &first);
    if (b.marker > 0) {
        b.kind = RS_BLK_ITEM;
        /* an empty item, or an ordered one not numbered 1, continues a paragraph */
        rs_cur_t rest = *c;
        rest.pos += b.marker;
        b.interrupts = first && !rs_cur_eol(&rest);
    }
    return b;
}

/* A line of an open fenced block or HTML block, or a blank line: classified
 * here. false for any other line. */
static bool rs_md_line_in_block(rs_md_t *st, rs_line_t *ln) {
    if (st->fence) {
        st->fence = !rs_md_fence_close(ln, st);
        return true;
    }
    bool blank = rs_blank(ln->p, ln->len);
    if (st->html != RS_HTML_NONE) {
        if (st->html >= RS_HTML_BLOCK ? blank : rs_md_html_end(ln, st->html)) {
            st->html = RS_HTML_NONE;
        }
        ln->kind = blank ? RS_L_BLANK : RS_L_CODE;
        return true;
    }
    if (blank) {
        ln->kind = RS_L_BLANK;
        st->para = false;
    }
    return blank;
}

/* The line is inline text from the cursor on. */
static void rs_md_text(rs_line_t *ln, const rs_cur_t *c, bool para_start) {
    ln->kind = RS_L_TEXT;
    ln->para_start = para_start;
    ln->len -= c->pos;
    ln->p += c->pos;
}

/* What is no list item starts at the cursor of lines[i]. Returns the lines it
 * takes, or 0 when the rest of the line is inline text (a heading's, or a
 * paragraph's first line). */
static int rs_md_start_block(rs_md_t *st, rs_line_t *lines, int n, int i, rs_cur_t *c,
                             const rs_blk_t *b, bool quoted, rs_defs_t *defs) {
    rs_line_t *ln = &lines[i];
    if (b->kind == RS_BLK_FENCE) {
        st->fence = true;
        st->fence_ch = b->fence_ch;
        st->fence_len = b->fence_len;
        return SKIP_ONE;
    }
    if (b->kind == RS_BLK_HTML) {
        bool closed = b->html < RS_HTML_BLOCK && rs_md_html_end(ln, b->html);
        st->html = closed ? RS_HTML_NONE : b->html;
        return SKIP_ONE;
    }
    if (b->kind == RS_BLK_RULE) {
        ln->kind = RS_L_BLANK;
        return SKIP_ONE;
    }
    if (b->kind == RS_BLK_HEADING) {
        (void)rs_md_atx(c);
        return 0;
    }
    int taken = rs_md_definition(lines, n, i, c, defs);
    if (taken > 0) {
        ln->kind = RS_L_DEF;
        if (taken > SKIP_ONE) {
            lines[i + SKIP_ONE].kind = RS_L_DEF;
        }
        return taken;
    }
    st->para = true;
    st->quoted = quoted;
    return 0;
}

/* Classify lines[i] and strip its block markers. Returns how many lines were
 * consumed (2 when a link definition's destination is on the next line). */
static int rs_md_line(rs_md_t *st, rs_line_t *lines, int n, int i, rs_defs_t *defs) {
    rs_line_t *ln = &lines[i];
    ln->kind = RS_L_CODE;
    if (rs_md_line_in_block(st, ln)) {
        return SKIP_ONE;
    }
    rs_cur_t c = {ln->p, ln->len, 0, 0};
    rs_cur_skip_ws(&c);
    bool quoted = rs_md_quote(&c);
    if (quoted && !st->quoted) {
        st->para = false; /* a block quote interrupts a paragraph */
    }
    /* every round opens one list item, which moves the cursor on */
    for (;;) {
        rs_blk_t b = rs_md_block_at(&c, st->para);
        if (st->para) {
            bool breaks = b.kind != RS_BLK_TEXT && b.interrupts &&
                          c.col - rs_md_base(st, c.col) < RS_MD_CODE_INDENT;
            if (!breaks) {
                rs_md_text(ln, &c, false); /* the paragraph goes on, however indented */
                return SKIP_ONE;
            }
            st->para = false;
        }
        rs_md_pop_lists(st, c.col);
        if (c.col - rs_md_base(st, c.col) >= RS_MD_CODE_INDENT) {
            return SKIP_ONE; /* an indented code block */
        }
        if (b.kind != RS_BLK_ITEM) {
            int taken = rs_md_start_block(st, lines, n, i, &c, &b, quoted, defs);
            if (taken > 0) {
                return taken;
            }
            break;
        }
        rs_md_open_item(st, &c, b.marker);
        if (rs_cur_eol(&c)) {
            ln->kind = RS_L_BLANK;
            return SKIP_ONE;
        }
    }
    rs_md_text(ln, &c, true);
    return SKIP_ONE;
}

/* ── Inline links ────────────────────────────────────────────────── */

enum { RS_LINK_INLINE = 0, RS_LINK_REFERENCE, RS_LINK_COLLAPSED, RS_LINK_SHORTCUT };

typedef struct {
    uint32_t pos;
    bool image;
} rs_opener_t;

/* A maximal run of backticks. */
typedef struct {
    uint32_t pos;
    uint32_t len;
} rs_run_t;

/* What a found link is handed to. */
typedef struct {
    CBMExtractCtx *ctx;
    const char *source_qn;
    uint32_t def_line;
    const rs_defs_t *defs;
    const rs_line_t *lines; /* the paragraph's lines */
    int nlines;
    int line_cursor;
} rs_sink_t;

/* Inline state of one paragraph. The arrays are allocated once per doc text,
 * large enough for any paragraph of it (one slot per '[' and per backtick), so
 * nothing here has a limit a long doc could hit. */
typedef struct {
    const char *s; /* the paragraph */
    uint32_t n;
    rs_opener_t *open; /* the '[' not yet closed */
    int nopen;
    int dead;         /* openers below this index are inside a link: they open none */
    rs_run_t *runs;   /* backtick runs in text order */
    rs_run_t *by_len; /* the same, sorted by (length, position) */
    int nruns;
    int run_cursor;      /* first run that ends after the scan position */
    bool no_comment_end; /* no later `-->` / `]]>` / `?>`: searched once */
    bool no_cdata_end;
    bool no_pi_end;
    rs_sink_t *sink;
} rs_inline_t;

/* Source line of paragraph offset `off`. Links are reported in the order of
 * their opening brackets, so the cursor rarely moves back. */
static uint32_t rs_sink_line(rs_sink_t *k, uint32_t off) {
    if (k->lines[k->line_cursor].boff > off) {
        k->line_cursor = 0;
    }
    while (k->line_cursor + SKIP_ONE < k->nlines &&
           k->lines[k->line_cursor + SKIP_ONE].boff <= off) {
        k->line_cursor++;
    }
    return k->lines[k->line_cursor].line;
}

static bool rs_is_url(const char *s, size_t n) {
    static const char mailto[] = "mailto:";
    static const char sep[] = "://";
    if (rs_ci_prefix(s, (uint32_t)n, mailto)) {
        return true;
    }
    size_t i = 0;
    if (n == 0 || !isalpha((unsigned char)s[0])) {
        return false;
    }
    while (i < n && (isalnum((unsigned char)s[i]) || s[i] == '+' || s[i] == '.' || s[i] == '-')) {
        i++;
    }
    return i + sizeof(sep) - SKIP_ONE <= n && memcmp(s + i, sep, sizeof(sep) - SKIP_ONE) == 0;
}

static void rs_push_token(rs_sink_t *k, const char *raw, size_t n, int syntax, uint32_t line) {
    static const char cut[] = "...";
    char buf[CBM_DOCLINK_RS_RAW_CAP];
    if (n >= sizeof(buf) - sizeof(cut)) {
        /* oversized: a prefix that can never parse as a path */
        n = sizeof(buf) - sizeof(cut) - SKIP_ONE;
        memcpy(buf, raw, n);
        memcpy(buf + n, cut, sizeof(cut));
        n += sizeof(cut) - SKIP_ONE;
    } else {
        memcpy(buf, raw, n);
        buf[n] = '\0';
    }
    for (size_t i = 0; i < n; i++) {
        if (buf[i] == '\n' || buf[i] == '\t' || buf[i] == '\r' || buf[i] == '\0') {
            buf[i] = ' ';
        }
    }
    const char *copy = cbm_arena_strndup(k->ctx->arena, buf, n);
    if (!copy) {
        return;
    }
    CBMDocLink link = {
        .source_qn = k->source_qn,
        .raw = copy,
        .line = line,
        .def_line = k->def_line,
        .syntax = (uint16_t)syntax,
    };
    cbm_doclinks_push(&k->ctx->result->doc_links, k->ctx->arena, link);
}

/* A found link: decide whether rustdoc treats its destination as an intra-doc
 * link, and push the token. `defined`: the destination was written (inline)
 * or comes from a definition, so it may be an ordinary URL. */
static void rs_emit_link(rs_sink_t *k, int kind, const char *dest, uint32_t dest_len, bool from_def,
                         uint32_t off) {
    while (dest_len > 0 && isspace((unsigned char)dest[0])) {
        dest++;
        dest_len--;
    }
    while (dest_len > 0 && isspace((unsigned char)dest[dest_len - SKIP_ONE])) {
        dest_len--;
    }
    if (dest_len == 0) {
        return;
    }
    uint32_t line = rs_sink_line(k, off);
    bool written = kind == RS_LINK_INLINE || from_def; /* rustdoc's can_be_url */
    int family = from_def ? CBM_DOCLINK_RS_LINK_DEF : CBM_DOCLINK_RS_INTRA_DOC;
    if (written && memchr(dest, '/', dest_len)) {
        if (rs_is_url(dest, dest_len)) {
            rs_push_token(k, dest, dest_len, CBM_DOCLINK_HREF, line);
        }
        return; /* a relative file is not harvested */
    }
    /* backticks go, the fragment is cut */
    char clean[CBM_DOCLINK_RS_RAW_CAP];
    size_t w = 0;
    int hashes = 0;
    bool backtick = false;
    for (uint32_t i = 0; i < dest_len; i++) {
        if (dest[i] == '`') {
            backtick = true;
            continue;
        }
        hashes += dest[i] == '#';
        if (w + SKIP_ONE < sizeof(clean)) {
            clean[w++] = dest[i];
        }
    }
    clean[w] = '\0';
    if (w + SKIP_ONE >= sizeof(clean)) {
        rs_push_token(k, clean, w, family, line); /* oversized */
        return;
    }
    /* rustdoc looks at the text before the first '#' first: empty means an
     * anchor of this page, whatever follows. A second '#' is then an error. */
    size_t path_len = hashes > 0 ? (size_t)((const char *)memchr(clean, '#', w) - clean) : w;
    while (path_len > 0 && isspace((unsigned char)clean[path_len - SKIP_ONE])) {
        path_len--;
    }
    if (path_len == 0) {
        return; /* an anchor of this page */
    }
    if (hashes > SKIP_ONE) {
        path_len = w; /* the row shows the whole destination */
    }
    bool lenient = written || (kind == RS_LINK_SHORTCUT && !backtick);
    CBMDocLinkRsPath path;
    CBMDocLinkRsPathStatus st = hashes > SKIP_ONE
                                    ? CBM_DOCLINK_RS_PATH_MALFORMED
                                    : cbm_doclink_rs_parse_path(clean, path_len, lenient, &path);
    if (st == CBM_DOCLINK_RS_PATH_NOT_LINK) {
        if (written && rs_is_url(dest, dest_len)) {
            rs_push_token(k, dest, dest_len, CBM_DOCLINK_HREF, line);
        }
        return;
    }
    rs_push_token(k, clean, path_len, family, line);
}

/* A reference-style link whose label is s[0..n): its destination is the
 * definition's, or (rustdoc's broken-link callback) the label itself. */
static void rs_emit_reference(rs_sink_t *k, int kind, const char *label, uint32_t n, uint32_t off) {
    const char *norm = rs_label_norm(rs_scratch(k->ctx), label, n);
    if (!norm || norm[0] == '^') {
        return; /* empty, or a footnote */
    }
    const rs_def_t *def = rs_def_find(k->defs, norm);
    if (def) {
        rs_emit_link(k, kind, def->dest, def->dest_len, true, off);
    } else {
        rs_emit_link(k, kind, label, n, false, off);
    }
}

static int rs_run_len_cmp(const void *a, const void *b) {
    const rs_run_t *x = (const rs_run_t *)a;
    const rs_run_t *y = (const rs_run_t *)b;
    if (x->len != y->len) {
        return x->len < y->len ? -1 : 1;
    }
    return (x->pos > y->pos) - (x->pos < y->pos);
}

/* Record the paragraph's backtick runs, in text order and by length: a code
 * span closes at the first later run of its opener's length, which is then a
 * binary search instead of a scan of the rest of the paragraph. */
static void rs_inline_runs(rs_inline_t *in) {
    in->nruns = 0;
    in->run_cursor = 0;
    for (uint32_t i = 0; i < in->n;) {
        if (in->s[i] != '`') {
            i++;
            continue;
        }
        uint32_t e = i;
        while (e < in->n && in->s[e] == '`') {
            e++;
        }
        in->runs[in->nruns++] = (rs_run_t){.pos = i, .len = e - i};
        i = e;
    }
    if (in->nruns > 0) {
        memcpy(in->by_len, in->runs, (size_t)in->nruns * sizeof(rs_run_t));
        qsort(in->by_len, (size_t)in->nruns, sizeof(rs_run_t), rs_run_len_cmp);
    }
}

/* Past the code span that opens at the backtick s[i], or past its run alone
 * when no later run has the opener's length. The opener is what is left of
 * its run from s[i] on (a backslash before the run takes its first backtick). */
static uint32_t rs_inline_backticks(rs_inline_t *in, uint32_t i) {
    while (in->run_cursor < in->nruns &&
           in->runs[in->run_cursor].pos + in->runs[in->run_cursor].len <= i) {
        in->run_cursor++;
    }
    if (in->run_cursor >= in->nruns) {
        return i + SKIP_ONE;
    }
    uint32_t run_end = in->runs[in->run_cursor].pos + in->runs[in->run_cursor].len;
    rs_run_t want = {.pos = run_end, .len = run_end - i};
    int lo = 0;
    int hi = in->nruns;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (rs_run_len_cmp(&in->by_len[mid], &want) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    if (lo < in->nruns && in->by_len[lo].len == want.len) {
        return in->by_len[lo].pos + want.len;
    }
    return run_end;
}

/* Index just past the first `mark` at or after s[from], or 0. */
static uint32_t rs_find_after(const rs_inline_t *in, uint32_t from, const char *mark) {
    size_t ml = strlen(mark);
    for (uint32_t k = from; k + ml <= in->n; k++) {
        if (memcmp(in->s + k, mark, ml) == 0) {
            return k + (uint32_t)ml;
        }
    }
    return 0;
}

/* Past the HTML comment, CDATA section or processing instruction that starts
 * at s[i]; 0 when s[i] starts none of them. One whose end marker is missing is
 * plain text, and so is every later one of its kind: the search for the marker
 * runs once per paragraph. */
static uint32_t rs_inline_raw_html(rs_inline_t *in, uint32_t i, bool *is_raw) {
    static const char comment[] = "<!--";
    static const char cdata[] = "<![CDATA[";
    static const char pi[] = "<?";
    const char *p = in->s + i;
    uint32_t n = in->n - i;
    const char *end_mark = NULL;
    bool *none = NULL;
    uint32_t skip = 0;
    if (n >= sizeof(comment) - SKIP_ONE && memcmp(p, comment, sizeof(comment) - SKIP_ONE) == 0) {
        end_mark = "-->";
        none = &in->no_comment_end;
        skip = sizeof(comment) - SKIP_ONE;
    } else if (n >= sizeof(cdata) - SKIP_ONE && memcmp(p, cdata, sizeof(cdata) - SKIP_ONE) == 0) {
        end_mark = "]]>";
        none = &in->no_cdata_end;
        skip = sizeof(cdata) - SKIP_ONE;
    } else if (n >= sizeof(pi) - SKIP_ONE && memcmp(p, pi, sizeof(pi) - SKIP_ONE) == 0) {
        end_mark = "?>";
        none = &in->no_pi_end;
        skip = sizeof(pi) - SKIP_ONE;
    }
    *is_raw = end_mark != NULL;
    if (!end_mark || *none) {
        return 0;
    }
    uint32_t end = rs_find_after(in, i + skip, end_mark);
    *none = end == 0;
    return end;
}

/* Past the autolink or HTML construct at s[i] == '<', or i + 1 when it is
 * neither. */
static uint32_t rs_inline_angle(rs_inline_t *in, uint32_t i) {
    const char *p = in->s + i;
    uint32_t n = in->n - i;
    if (n < PAIR_LEN) {
        return i + SKIP_ONE;
    }
    bool is_raw = false;
    uint32_t raw_end = rs_inline_raw_html(in, i, &is_raw);
    if (is_raw) {
        return raw_end ? raw_end : i + SKIP_ONE;
    }
    char c = p[1];
    if (!(isalpha((unsigned char)c) || c == '/' || c == '!')) {
        return i + SKIP_ONE;
    }
    uint32_t end = rs_html_tag_end(p, n);
    if (end == 0) {
        return i + SKIP_ONE;
    }
    if (isalpha((unsigned char)c)) {
        /* `<name ...>` is a tag; `<scheme:...>` and `<a@b>` are autolinks; text
         * with a space before any of that is neither */
        uint32_t k = SKIP_ONE;
        while (k < end &&
               (isalnum((unsigned char)p[k]) || p[k] == '-' || p[k] == '+' || p[k] == '.')) {
            k++;
        }
        bool tag = p[k] == '>' || p[k] == ' ' || p[k] == '\t' || p[k] == '\n' || p[k] == '/';
        bool autolink = (p[k] == ':' || p[k] == '@') && !memchr(p, ' ', end);
        if (!tag && !autolink) {
            return i + SKIP_ONE;
        }
    }
    return i + end;
}

/* The inline link `(dest "title")` at s[i] == '('. On success *end is past the
 * closing parenthesis. */
static bool rs_inline_dest(const rs_inline_t *in, uint32_t i, const char **dest, uint32_t *dest_len,
                           uint32_t *end) {
    const char *s = in->s;
    uint32_t n = in->n;
    uint32_t k = i + SKIP_ONE;
    while (k < n && isspace((unsigned char)s[k])) {
        k++;
    }
    if (!rs_link_dest(s, n, &k, dest, dest_len)) {
        return false;
    }
    uint32_t after = k;
    while (after < n && isspace((unsigned char)s[after])) {
        after++;
    }
    if (after > k && !rs_link_title(s, n, &after)) {
        return false;
    }
    while (after < n && isspace((unsigned char)s[after])) {
        after++;
    }
    if (after >= n || s[after] != ')') {
        return false;
    }
    *end = after + SKIP_ONE;
    return true;
}

/* The reference-style link the ']' at s[i] ends, when the text after it or
 * the bracket text itself is a label: full `[text][label]`, collapsed
 * `[label][]`, shortcut `[label]`. *made is set when it is one; returns where
 * scanning continues. */
static uint32_t rs_inline_reference(rs_inline_t *in, const rs_opener_t *op, uint32_t i,
                                    bool *made) {
    const char *s = in->s;
    const char *text = s + op->pos + SKIP_ONE;
    uint32_t text_len = i - op->pos - SKIP_ONE;
    uint32_t after = i + SKIP_ONE;
    bool text_is_label = rs_label_clean(text, text_len);
    if (after < in->n && s[after] == '[') {
        uint32_t close = rs_label_end(s, in->n, after + SKIP_ONE);
        if (close != UINT32_MAX && close > after + SKIP_ONE) {
            *made = true;
            if (!op->image) {
                rs_emit_reference(in->sink, RS_LINK_REFERENCE, s + after + SKIP_ONE,
                                  close - after - SKIP_ONE, op->pos);
            }
            return close + SKIP_ONE;
        }
        if (close == after + SKIP_ONE && text_is_label) {
            *made = true;
            if (!op->image) {
                rs_emit_reference(in->sink, RS_LINK_COLLAPSED, text, text_len, op->pos);
            }
            return close + SKIP_ONE;
        }
    }
    if (text_is_label) {
        *made = true;
        if (!op->image) {
            rs_emit_reference(in->sink, RS_LINK_SHORTCUT, text, text_len, op->pos);
        }
    }
    return i + SKIP_ONE;
}

/* The ']' at s[i]: make a link of the bracket pair it closes, when it is one.
 * Returns where scanning continues. */
static uint32_t rs_inline_close(rs_inline_t *in, uint32_t i) {
    if (in->nopen == 0) {
        return i + SKIP_ONE;
    }
    int idx = --in->nopen;
    rs_opener_t op = in->open[idx];
    bool dead = idx < in->dead && !op.image; /* inside a link: links do not nest */
    if (in->dead > in->nopen) {
        in->dead = in->nopen;
    }
    if (dead) {
        return i + SKIP_ONE;
    }
    uint32_t after = i + SKIP_ONE;
    const char *dest = NULL;
    uint32_t dest_len = 0;
    uint32_t end = 0;
    bool made = false;
    if (after < in->n && in->s[after] == '(' && rs_inline_dest(in, after, &dest, &dest_len, &end)) {
        made = true;
        if (!op.image) {
            rs_emit_link(in->sink, RS_LINK_INLINE, dest, dest_len, false, op.pos);
        }
    } else {
        end = rs_inline_reference(in, &op, i, &made);
    }
    if (made && !op.image) {
        in->dead = in->nopen; /* every '[' still open encloses this link */
    }
    return end;
}

/* Find the links of one paragraph. */
static void rs_inline_scan(rs_inline_t *in) {
    uint32_t i = 0;
    while (i < in->n) {
        char c = in->s[i];
        if (c == '\\' && i + SKIP_ONE < in->n && ispunct((unsigned char)in->s[i + SKIP_ONE])) {
            i += PAIR_LEN;
        } else if (c == '`') {
            i = rs_inline_backticks(in, i);
        } else if (c == '<') {
            i = rs_inline_angle(in, i);
        } else if (c == '[') {
            bool image = i > 0 && in->s[i - SKIP_ONE] == '!' &&
                         !(i > SKIP_ONE && in->s[i - PAIR_LEN] == '\\');
            in->open[in->nopen++] = (rs_opener_t){.pos = i, .image = image};
            i++;
        } else if (c == ']') {
            i = rs_inline_close(in, i);
        } else {
            i++;
        }
    }
}

/* ── Tokens of one doc text ──────────────────────────────────────── */

/* Scan the paragraph lines[first..last] (RS_L_TEXT lines): their contents are
 * joined by newlines in `buf`. */
static void rs_scan_paragraph(rs_inline_t *in, rs_sink_t *sink, rs_line_t *lines, int first,
                              int last, char *buf) {
    uint32_t w = 0;
    for (int i = first; i <= last; i++) {
        lines[i].boff = w;
        memcpy(buf + w, lines[i].p, lines[i].len);
        w += lines[i].len;
        buf[w++] = '\n';
    }
    in->s = buf;
    in->n = w;
    in->nopen = 0;
    in->dead = 0;
    in->no_comment_end = false;
    in->no_cdata_end = false;
    in->no_pi_end = false;
    in->sink = sink;
    sink->lines = lines + first;
    sink->nlines = last - first + SKIP_ONE;
    sink->line_cursor = 0;
    rs_inline_runs(in);
    rs_inline_scan(in);
}

/* The inline state for a doc text whose inline lines are lines[0..n): arrays
 * with one slot per '[' and per backtick of the whole text, so no paragraph can
 * outgrow them. NULL when there is no inline text or memory ran out; *buf_out
 * is the paragraph buffer. */
static rs_inline_t *rs_inline_new(CBMArena *tmp, const rs_line_t *lines, int n, char **buf_out) {
    size_t text_bytes = 0;
    size_t brackets = 0;
    size_t backticks = 0;
    for (int i = 0; i < n; i++) {
        if (lines[i].kind != RS_L_TEXT) {
            continue;
        }
        text_bytes += (size_t)lines[i].len + SKIP_ONE;
        for (uint32_t k = 0; k < lines[i].len; k++) {
            brackets += lines[i].p[k] == '[';
            backticks += lines[i].p[k] == '`';
        }
    }
    if (text_bytes == 0 || brackets == 0) {
        return NULL; /* no bracket, no link */
    }
    rs_inline_t *in = (rs_inline_t *)cbm_arena_alloc(tmp, sizeof(*in));
    char *buf = (char *)cbm_arena_alloc(tmp, text_bytes + SKIP_ONE);
    if (!in || !buf) {
        return NULL;
    }
    memset(in, 0, sizeof(*in));
    in->open = (rs_opener_t *)cbm_arena_alloc(tmp, brackets * sizeof(rs_opener_t));
    in->runs = (rs_run_t *)cbm_arena_alloc(tmp, (backticks + SKIP_ONE) * sizeof(rs_run_t));
    in->by_len = (rs_run_t *)cbm_arena_alloc(tmp, (backticks + SKIP_ONE) * sizeof(rs_run_t));
    if (!in->open || !in->runs || !in->by_len) {
        return NULL;
    }
    *buf_out = buf;
    return in;
}

/* The references of one doc text, pushed as tokens of the node `source_qn`.
 * `file_doc`: it is the file's own doc, whose lines need not be contiguous. */
static void rs_parse_text(CBMExtractCtx *ctx, const char *source_qn, uint32_t def_line,
                          const char *doc, uint32_t doc_line, bool file_doc) {
    if (!doc || !source_qn || !strchr(doc, '[')) {
        return; /* no bracket, no link */
    }
    CBMArena *tmp = rs_scratch(ctx);
    int n = 0;
    rs_line_t *lines = rs_doc_lines(tmp, doc, doc_line, &n);
    if (!lines || n == 0) {
        return;
    }
    if (file_doc) {
        rs_lines_resync(ctx, lines, n, doc, doc_line);
    }
    rs_defs_t defs = {.a = tmp};
    rs_md_t st;
    memset(&st, 0, sizeof(st));
    st.a = tmp;
    for (int i = 0; i < n;) {
        i += rs_md_line(&st, lines, n, i, &defs);
    }
    char *buf = NULL;
    rs_inline_t *in = (defs.failed || st.failed) ? NULL : rs_inline_new(tmp, lines, n, &buf);
    if (!in) {
        return;
    }
    if (defs.n > SKIP_ONE) {
        qsort(defs.v, (size_t)defs.n, sizeof(rs_def_t), rs_def_cmp);
    }
    rs_sink_t sink = {.ctx = ctx, .source_qn = source_qn, .def_line = def_line, .defs = &defs};
    int first = CBM_NOT_FOUND;
    for (int i = 0; i <= n; i++) {
        bool text = i < n && lines[i].kind == RS_L_TEXT;
        if (first >= 0 && (!text || lines[i].para_start)) {
            rs_scan_paragraph(in, &sink, lines, first, i - SKIP_ONE, buf);
            first = CBM_NOT_FOUND;
        }
        if (text && first < 0) {
            first = i;
        }
    }
}

void cbm_doclink_rs_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line) {
    if (!ctx || !ctx->result || !def) {
        return;
    }
    /* the driver's stand-in definition for the file's own doc is labelled File */
    bool file_doc = def->label && strcmp(def->label, "File") == 0;
    rs_parse_text(ctx, def->qualified_name, def->start_line, doc, doc_line, file_doc);
}

/* ── Scope scan: state ───────────────────────────────────────────── */

enum {
    RS_SCAN_NAME_CAP = 128,    /* bytes of one identifier, NUL included */
    RS_SCAN_PATH_CAP = 512,    /* bytes of one written path, NUL included */
    RS_SCAN_ATTR_CAP = 1024,   /* bytes of an attribute that are read */
    RS_SCAN_DERIVES_CAP = 256, /* bytes of the derive names of one item, NUL included */
    RS_SCAN_INIT = 16,
};

/* A block whose items are still to be read: the file, an inline module's
 * body, an extern block's body. Blocks are queued, never recursed into. */
typedef struct {
    TSNode node;
    int mod; /* module the items belong to */
} rs_block_t;

/* A definition of the file, keyed for the items the scan meets. */
typedef struct {
    uint32_t line;
    const char *name;
    int def;
} rs_defkey_t;

/* What the attributes above an item say about it. */
typedef struct {
    bool test;         /* #[cfg(test)], #[test], #[bench], #[tokio::test] ... */
    bool macro_export; /* #[macro_export] */
    bool cond_path;    /* #[cfg_attr(.., path = ..)]: where the module file is depends on cfg */
    bool derives_more; /* derive names that did not fit, or were not read */
    char macro_kind;   /* P #[proc_macro], B #[proc_macro_attribute], V #[proc_macro_derive] */
    char path[RS_SCAN_PATH_CAP];       /* #[path = "..."] */
    char derive[RS_SCAN_NAME_CAP];     /* the derive macro's name */
    char derives[RS_SCAN_DERIVES_CAP]; /* #[derive(A, b::C)] -> "A,C": impls not in the tree */
} rs_attrs_t;

typedef struct {
    CBMExtractCtx *ctx;
    CBMArena *tmp; /* nothing of it outlives the scan */
    const char *src;
    uint32_t src_len;
    rs_sb_t sb;
    bool failed;       /* out of memory */
    bool broken;       /* something the blob cannot represent, or a parse error */
    uint32_t err_byte; /* first byte that is not trusted; UINT32_MAX: none */
    int next_mod;
    rs_block_t *blocks;
    int nblocks;
    int blocks_cap;
    const char *item_prefix; /* "<qualified name of an item of this file minus its name>." */
    size_t item_prefix_len;
    rs_defkey_t *defkeys;
    int ndefkeys;
    unsigned char *owner; /* per definition: 1 when it owns its graph node */
    int *owner_of;        /* per definition: the definition that owns its qualified name */
    int *def_mod;         /* per definition: 1 + the module the scan met it in, 0: not met */
    uint32_t *ref_lines;  /* start lines of the definitions that carry references, sorted */
    int nref_lines;
    int last_use_mod;                /* the V record written last: a run of */
    char last_use[RS_SCAN_NAME_CAP]; /* invocations of one macro is written once */
} rs_scan_t;

static bool rs_kind(TSNode n, const char *kind) {
    return strcmp(ts_node_type(n), kind) == 0;
}

static TSNode rs_field(TSNode n, const char *field) {
    return ts_node_child_by_field_name(n, field, (uint32_t)strlen(field));
}

static uint32_t rs_line_of(TSNode n) {
    return ts_node_start_point(n).row + TS_LINE_OFFSET;
}

static uint32_t rs_end_line_of(TSNode n) {
    return ts_node_end_point(n).row + TS_LINE_OFFSET;
}

static void *rs_tmp_alloc(rs_scan_t *s, size_t n) {
    void *p = cbm_arena_alloc(s->tmp, n ? n : SKIP_ONE);
    s->failed = s->failed || !p;
    return p;
}

/* The node's text as a name: copied into `out` when it is an identifier that
 * fits (raw `r#name` keeps its marker, as the extractor's names do). false
 * for anything else -- error recovery can hand back a "name" spanning code. */
static bool rs_name_of(const rs_scan_t *s, TSNode n, char *out, size_t cap) {
    if (ts_node_is_null(n)) {
        return false;
    }
    uint32_t a = ts_node_start_byte(n);
    uint32_t b = ts_node_end_byte(n);
    if (b <= a || b > s->src_len || (size_t)(b - a) >= cap || !rs_is_segment(s->src + a, b - a)) {
        return false;
    }
    memcpy(out, s->src + a, b - a);
    out[b - a] = '\0';
    return true;
}

/* A written path (`a::b::C`, `crate::x`, `::std::fmt`) of the node's text,
 * whitespace removed: copied into `out` when it holds nothing but segments.
 * false for a type that is no path (a reference, a tuple, generics inside). */
static bool rs_path_of(const rs_scan_t *s, TSNode n, char *out, size_t cap) {
    if (ts_node_is_null(n)) {
        return false;
    }
    uint32_t a = ts_node_start_byte(n);
    uint32_t b = ts_node_end_byte(n);
    if (b <= a || b > s->src_len) {
        return false;
    }
    size_t w = 0;
    for (uint32_t i = a; i < b; i++) {
        char c = s->src[i];
        if (isspace((unsigned char)c)) {
            continue;
        }
        if (w + SKIP_ONE >= cap) {
            return false;
        }
        out[w++] = c;
    }
    out[w] = '\0';
    CBMDocLinkRsPath path;
    return w > 0 && !memchr(out, '<', w) && !memchr(out, '@', w) && !memchr(out, '!', w) &&
           !memchr(out, '(', w) &&
           cbm_doclink_rs_parse_path(out, w, false, &path) == CBM_DOCLINK_RS_PATH_OK;
}

/* ── Scope scan: definitions ─────────────────────────────────────── */

static int rs_defkey_cmp(const void *a, const void *b) {
    const rs_defkey_t *x = (const rs_defkey_t *)a;
    const rs_defkey_t *y = (const rs_defkey_t *)b;
    if (x->line != y->line) {
        return x->line < y->line ? -1 : 1;
    }
    int c = strcmp(x->name, y->name);
    return c ? c : (x->def > y->def) - (x->def < y->def);
}

/* The order in which the graph buffer lets definitions of one qualified name
 * replace each other (cbm_gbuf_upsert_node, same file): start line, then
 * name, then label; the last one owns the node. */
static int rs_defqn_cmp_ctx(const CBMDefinition *x, const CBMDefinition *y) {
    int c = strcmp(x->qualified_name, y->qualified_name);
    if (c) {
        return c;
    }
    if (x->start_line != y->start_line) {
        return x->start_line < y->start_line ? -1 : 1;
    }
    c = strcmp(x->name, y->name);
    return c ? c : strcmp(x->label ? x->label : "", y->label ? y->label : "");
}

typedef struct {
    const CBMDefinition *def;
    int idx;
} rs_defref_t;

static int rs_defref_cmp(const void *a, const void *b) {
    const rs_defref_t *x = (const rs_defref_t *)a;
    const rs_defref_t *y = (const rs_defref_t *)b;
    int c = rs_defqn_cmp_ctx(x->def, y->def);
    return c ? c : (x->idx > y->idx) - (x->idx < y->idx);
}

/* Index the file's definitions: by (line, name) for the items the scan meets,
 * and which definition of every qualified name owns the node. */
static void rs_scan_defs(rs_scan_t *s) {
    const CBMDefArray *defs = &s->ctx->result->defs;
    int n = defs->count;
    s->defkeys = (rs_defkey_t *)rs_tmp_alloc(s, (size_t)n * sizeof(rs_defkey_t));
    s->owner = (unsigned char *)rs_tmp_alloc(s, (size_t)n + SKIP_ONE);
    s->owner_of = (int *)rs_tmp_alloc(s, ((size_t)n + SKIP_ONE) * sizeof(int));
    s->def_mod = (int *)rs_tmp_alloc(s, ((size_t)n + SKIP_ONE) * sizeof(int));
    rs_defref_t *refs = (rs_defref_t *)rs_tmp_alloc(s, (size_t)n * sizeof(rs_defref_t));
    if (s->failed) {
        return;
    }
    memset(s->owner, 0, (size_t)n + SKIP_ONE);
    memset(s->def_mod, 0, ((size_t)n + SKIP_ONE) * sizeof(int));
    for (int i = 0; i <= n; i++) {
        s->owner_of[i] = CBM_NOT_FOUND;
    }
    int k = 0;
    for (int i = 0; i < n; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (!d->name || !d->qualified_name) {
            continue;
        }
        s->defkeys[k] = (rs_defkey_t){.line = d->start_line, .name = d->name, .def = i};
        refs[k] = (rs_defref_t){.def = d, .idx = i};
        k++;
    }
    s->ndefkeys = k;
    if (k == 0) {
        return;
    }
    qsort(s->defkeys, (size_t)k, sizeof(rs_defkey_t), rs_defkey_cmp);
    qsort(refs, (size_t)k, sizeof(rs_defref_t), rs_defref_cmp);
    int owner = CBM_NOT_FOUND; /* of the run of equal qualified names being walked, last first */
    for (int i = k - SKIP_ONE; i >= 0; i--) {
        bool last = i + SKIP_ONE >= k || strcmp(refs[i + SKIP_ONE].def->qualified_name,
                                                refs[i].def->qualified_name) != 0;
        owner = last ? refs[i].idx : owner;
        s->owner[refs[i].idx] = last ? SKIP_ONE : 0;
        s->owner_of[refs[i].idx] = owner;
    }
}

/* The definition the extractor made of an item of module `mod`: same start
 * line, name and label, declared by `parent` (the qualified name of its type,
 * NULL for a module-level item). CBM_NOT_FOUND when there is none. The module
 * of a definition that is found is remembered. */
static int rs_scan_def(rs_scan_t *s, int mod, uint32_t line, const char *name, const char *label,
                       const char *parent) {
    const CBMDefArray *defs = &s->ctx->result->defs;
    int lo = 0;
    int hi = s->ndefkeys;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        const rs_defkey_t *key = &s->defkeys[mid];
        bool less = key->line < line || (key->line == line && strcmp(key->name, name) < 0);
        if (less) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int i = lo; i < s->ndefkeys; i++) {
        const rs_defkey_t *key = &s->defkeys[i];
        if (key->line != line || strcmp(key->name, name) != 0) {
            break;
        }
        const CBMDefinition *d = &defs->items[key->def];
        bool same_parent = parent ? (d->parent_class && strcmp(d->parent_class, parent) == 0)
                                  : d->parent_class == NULL;
        if (d->label && strcmp(d->label, label) == 0 && same_parent) {
            s->def_mod[key->def] = mod + SKIP_ONE;
            return key->def;
        }
    }
    return CBM_NOT_FOUND;
}

static char rs_label_code(const char *label) {
    static const struct {
        const char *label;
        char code;
    } table[] = {
        {"Struct", 'S'},   {"Enum", 'E'},     {"Class", 'C'},  {"Interface", 'I'}, {"Type", 'T'},
        {"Function", 'F'}, {"Variable", 'V'}, {"Method", 'M'}, {"Field", 'D'},
    };
    for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
        if (strcmp(table[i].label, label) == 0) {
            return table[i].code;
        }
    }
    return 0;
}

/* Write an item's node field: '!' when definition `def` does not exist, does
 * not own its node, or has a qualified name the blob cannot carry; else the
 * label code and the qualified name's part after the file's item prefix. */
static void rs_put_node(rs_scan_t *s, int def, const char *name) {
    if (def < 0) {
        rs_sb_putc(&s->sb, '!');
        return;
    }
    const CBMDefinition *d = &s->ctx->result->defs.items[def];
    char code = rs_label_code(d->label);
    const char *qn = d->qualified_name;
    bool ok = code && qn && s->owner[def] && strlen(qn) > s->item_prefix_len &&
              memcmp(qn, s->item_prefix, s->item_prefix_len) == 0;
    const char *suffix = ok ? qn + s->item_prefix_len : "";
    if (!ok || strpbrk(suffix, "\t\n\r")) {
        rs_sb_putc(&s->sb, '!');
        return;
    }
    rs_sb_putc(&s->sb, code);
    if (strcmp(suffix, name) == 0) {
        rs_sb_putc(&s->sb, '=');
    } else {
        rs_sb_putc(&s->sb, ':');
        rs_sb_puts(&s->sb, suffix);
    }
}

/* The qualified name the extractor gives the type `owner` of this file: the
 * parent_class of its members' definitions. */
static const char *rs_owner_qn(rs_scan_t *s, const char *owner, size_t owner_len) {
    char *out = (char *)rs_tmp_alloc(s, s->item_prefix_len + owner_len + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    memcpy(out, s->item_prefix, s->item_prefix_len);
    memcpy(out + s->item_prefix_len, owner, owner_len);
    out[s->item_prefix_len + owner_len] = '\0';
    return out;
}

/* ── Scope scan: records ─────────────────────────────────────────── */

static void rs_rec(rs_scan_t *s, char tag) {
    rs_sb_putc(&s->sb, tag);
}

static void rs_rec_u(rs_scan_t *s, uint32_t v) {
    rs_sb_putc(&s->sb, '\t');
    rs_sb_putu(&s->sb, v);
}

static void rs_rec_s(rs_scan_t *s, const char *v) {
    rs_sb_putc(&s->sb, '\t');
    rs_sb_puts(&s->sb, v);
}

static void rs_rec_end(rs_scan_t *s) {
    rs_sb_putc(&s->sb, '\n');
}

/* A name some macro invocation may declare in module `mod` (-1: somewhere in
 * the file). */
static void rs_rec_maybe(rs_scan_t *s, int mod, const char *name) {
    rs_rec(s, 'Q');
    if (mod < 0) {
        rs_rec_s(s, "*");
    } else {
        rs_rec_u(s, (uint32_t)mod);
    }
    rs_rec_s(s, name);
    rs_rec_end(s);
}

/* The visibility of an item and what its attributes add: the flags field. */
static void rs_rec_flags(rs_scan_t *s, TSNode item, const rs_attrs_t *at) {
    char vis = '-';
    TSTreeCursor cur = ts_tree_cursor_new(item);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode ch = ts_tree_cursor_current_node(&cur);
            if (!rs_kind(ch, "visibility_modifier")) {
                continue;
            }
            vis = 'p';
            uint32_t nc = ts_node_named_child_count(ch);
            if (nc > 0) {
                const char *k = ts_node_type(ts_node_named_child(ch, 0));
                vis = strcmp(k, "crate") == 0
                          ? 'c'
                          : (strcmp(k, "super") == 0 ? 's' : (strcmp(k, "self") == 0 ? '-' : 'i'));
            }
            break;
        } while (ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    rs_sb_putc(&s->sb, '\t');
    rs_sb_putc(&s->sb, vis);
    if (at && at->test) {
        rs_sb_putc(&s->sb, 't');
    }
    if (at && at->macro_export) {
        rs_sb_putc(&s->sb, 'e');
    }
    if (at && at->cond_path) {
        rs_sb_putc(&s->sb, 'c');
    }
}

/* ── Scope scan: attributes ──────────────────────────────────────── */

static bool rs_word_is(const char *p, size_t n, const char *word) {
    return strlen(word) == n && memcmp(p, word, n) == 0;
}

/* True when the identifier `word` stands in text[0..n). */
static bool rs_has_word(const char *text, size_t n, const char *word) {
    size_t wl = strlen(word);
    for (size_t i = 0; i + wl <= n; i++) {
        bool starts =
            i == 0 || !(isalnum((unsigned char)text[i - SKIP_ONE]) || text[i - SKIP_ONE] == '_');
        bool ends = i + wl == n || !(isalnum((unsigned char)text[i + wl]) || text[i + wl] == '_');
        if (starts && ends && memcmp(text + i, word, wl) == 0) {
            return true;
        }
    }
    return false;
}

/* The string literal that follows `key =` in text[0..n), copied into `out`.
 * false when there is none, it holds an escape, or it does not fit. */
static bool rs_attr_string(const char *text, size_t n, char *out, size_t cap) {
    const char *eq = (const char *)memchr(text, '=', n);
    if (!eq) {
        return false;
    }
    size_t i = (size_t)(eq - text) + SKIP_ONE;
    while (i < n && isspace((unsigned char)text[i])) {
        i++;
    }
    if (i >= n || text[i] != '"') {
        return false;
    }
    size_t start = ++i;
    while (i < n && text[i] != '"' && text[i] != '\\' && (unsigned char)text[i] >= ' ') {
        i++;
    }
    if (i >= n || text[i] != '"' || i - start >= cap || i == start) {
        return false;
    }
    memcpy(out, text + start, i - start);
    out[i - start] = '\0';
    return true;
}

/* The first identifier inside the parentheses of text[0..n). */
static void rs_attr_first_arg(const char *text, size_t n, char *out, size_t cap) {
    out[0] = '\0';
    const char *open = (const char *)memchr(text, '(', n);
    if (!open) {
        return;
    }
    size_t i = (size_t)(open - text) + SKIP_ONE;
    while (i < n && isspace((unsigned char)text[i])) {
        i++;
    }
    size_t start = i;
    while (i < n && (isalnum((unsigned char)text[i]) || text[i] == '_')) {
        i++;
    }
    if (i > start && i - start < cap) {
        memcpy(out, text + start, i - start);
        out[i - start] = '\0';
    }
}

/* Append a derive name to the list of `at` (comma-joined). */
static void rs_derive_put(rs_attrs_t *at, const char *name, size_t n) {
    size_t have = strlen(at->derives);
    if (n == 0) {
        return;
    }
    if (have + n + PAIR_LEN >= sizeof(at->derives)) {
        at->derives_more = true;
        return;
    }
    if (have > 0) {
        at->derives[have++] = ',';
    }
    memcpy(at->derives + have, name, n);
    at->derives[have + n] = '\0';
}

/* The names inside every `derive(..)` of an attribute's text (also inside
 * `cfg_attr(x, derive(..))`): the last segment of each path. */
static void rs_attr_derives(const char *text, size_t n, rs_attrs_t *at) {
    static const char word[] = "derive";
    const size_t wl = sizeof(word) - SKIP_ONE;
    for (size_t i = 0; i + wl < n; i++) {
        bool starts =
            i == 0 || !(isalnum((unsigned char)text[i - SKIP_ONE]) || text[i - SKIP_ONE] == '_');
        size_t j = i + wl;
        if (!starts || memcmp(text + i, word, wl) != 0) {
            continue;
        }
        while (j < n && isspace((unsigned char)text[j])) {
            j++;
        }
        if (j >= n || text[j] != '(') {
            continue;
        }
        size_t id = 0; /* the identifier read last, and its length */
        size_t idlen = 0;
        bool in_id = false;
        for (j++; j < n && text[j] != ')'; j++) {
            unsigned char c = (unsigned char)text[j];
            if (isalnum(c) || c == '_') {
                id = in_id ? id : j;
                idlen = in_id ? idlen + SKIP_ONE : SKIP_ONE;
                in_id = true;
                continue;
            }
            in_id = false;
            if (c == ',') {
                rs_derive_put(at, text + id, idlen);
            }
            idlen = (c == ',' || c == ':') ? 0 : idlen;
        }
        rs_derive_put(at, text + id, idlen);
        at->derives_more = at->derives_more || j >= n; /* cut off before its `)` */
        i = j;
    }
}

/* Read one `#[...]` above an item into `at`. */
static void rs_attr_read(const rs_scan_t *s, TSNode attr_item, rs_attrs_t *at) {
    uint32_t a = ts_node_start_byte(attr_item);
    uint32_t b = ts_node_end_byte(attr_item);
    if (b <= a || b > s->src_len) {
        return;
    }
    const char *text = s->src + a;
    size_t n = b - a;
    bool long_attr = n > RS_SCAN_ATTR_CAP;
    n = long_attr ? RS_SCAN_ATTR_CAP : n;
    /* the attribute's own name: the path after `#[` up to `(`, `=` or `]` */
    size_t i = 0;
    while (i < n && (text[i] == '#' || text[i] == '[' || isspace((unsigned char)text[i]))) {
        i++;
    }
    size_t name = i;
    while (i < n && (isalnum((unsigned char)text[i]) || text[i] == '_' || text[i] == ':')) {
        i++;
    }
    size_t nl = i - name;
    const char *p = text + name;
    static const char test_suffix[] = "::test";
    const size_t tsl = sizeof(test_suffix) - SKIP_ONE;
    if (rs_word_is(p, nl, "test") || rs_word_is(p, nl, "bench") ||
        (nl > tsl && memcmp(p + nl - tsl, test_suffix, tsl) == 0)) {
        at->test = true;
    } else if (rs_word_is(p, nl, "cfg")) {
        at->test = at->test || (rs_has_word(text, n, "test") && !rs_has_word(text, n, "not"));
    } else if (rs_word_is(p, nl, "path")) {
        if (long_attr || !rs_attr_string(text, n, at->path, sizeof(at->path))) {
            at->cond_path = true; /* a path this scan cannot read */
        }
    } else if (rs_word_is(p, nl, "cfg_attr")) {
        at->cond_path = at->cond_path || long_attr || rs_has_word(text, n, "path");
        at->derives_more = at->derives_more || long_attr;
        rs_attr_derives(text, n, at);
    } else if (rs_word_is(p, nl, "derive")) {
        at->derives_more = at->derives_more || long_attr;
        rs_attr_derives(text, n, at);
    } else if (rs_word_is(p, nl, "macro_export")) {
        at->macro_export = true;
    } else if (rs_word_is(p, nl, "proc_macro")) {
        at->macro_kind = 'P';
    } else if (rs_word_is(p, nl, "proc_macro_attribute")) {
        at->macro_kind = 'B';
    } else if (rs_word_is(p, nl, "proc_macro_derive")) {
        at->macro_kind = 'V';
        rs_attr_first_arg(text, n, at->derive, sizeof(at->derive));
    }
}

/* ── Scope scan: names local to a documented item ────────────────── */

/* True when a definition that starts at `line` carries references. */
static bool rs_has_refs(const rs_scan_t *s, uint32_t line) {
    int lo = 0;
    int hi = s->nref_lines;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (s->ref_lines[mid] < line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo < s->nref_lines && s->ref_lines[lo] == line;
}

static void rs_put_local(rs_scan_t *s, TSNode name, bool *first) {
    char buf[RS_SCAN_NAME_CAP];
    if (!rs_name_of(s, name, buf, sizeof(buf))) {
        return;
    }
    if (!*first) {
        rs_sb_putc(&s->sb, ',');
    }
    rs_sb_puts(&s->sb, buf);
    *first = false;
}

/* The type and const parameters of a `type_parameters` list. */
static void rs_put_generics(rs_scan_t *s, TSNode tparams, bool *first) {
    if (ts_node_is_null(tparams)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(tparams);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_named_child(tparams, i);
        if (rs_kind(p, "type_parameter") || rs_kind(p, "const_parameter")) {
            rs_put_local(s, rs_field(p, "name"), first);
        } else if (rs_kind(p, "type_identifier")) {
            rs_put_local(s, p, first);
        }
    }
}

/* Every name the patterns of a `parameters` list bind. The walk is a cursor
 * walk of the list's subtree: no recursion. */
static void rs_put_params(rs_scan_t *s, TSNode params, bool *first) {
    if (ts_node_is_null(params)) {
        return;
    }
    TSTreeCursor cur = ts_tree_cursor_new(params);
    for (;;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        const char *field = ts_tree_cursor_current_field_name(&cur);
        bool in_type = field && strcmp(field, "type") == 0;
        if (rs_kind(n, "identifier") || rs_kind(n, "shorthand_field_identifier")) {
            rs_put_local(s, n, first);
        }
        if (!in_type && ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        bool moved = ts_tree_cursor_goto_next_sibling(&cur);
        while (!moved && ts_tree_cursor_goto_parent(&cur)) {
            moved = ts_tree_cursor_goto_next_sibling(&cur);
        }
        if (!moved) {
            break;
        }
    }
    ts_tree_cursor_delete(&cur);
}

/* The P record of the definition at `line`: the type parameters of its owner
 * (`outer`, may be null) and, for `item`, its own type parameters and
 * parameter names. Written only for a definition with references that has
 * such names. */
static void rs_rec_locals(rs_scan_t *s, uint32_t line, TSNode outer, TSNode item) {
    if (!rs_has_refs(s, line) || s->sb.failed) {
        return;
    }
    size_t mark = s->sb.len;
    rs_rec(s, 'P');
    rs_rec_u(s, line);
    rs_sb_putc(&s->sb, '\t');
    size_t names_at = s->sb.len;
    bool first = true;
    rs_put_generics(s, outer, &first);
    if (!ts_node_is_null(item)) {
        rs_put_generics(s, rs_field(item, "type_parameters"), &first);
        rs_put_params(s, rs_field(item, "parameters"), &first);
    }
    if (s->sb.failed) {
        return;
    }
    if (s->sb.len == names_at) {
        s->sb.len = mark; /* nothing local: no record */
        s->sb.buf[mark] = '\0';
        return;
    }
    rs_rec_end(s);
}

/* ── Scope scan: members ─────────────────────────────────────────── */

static void rs_rec_member(rs_scan_t *s, uint32_t line, char kind, const char *name, int def) {
    rs_rec(s, 'M');
    rs_rec_u(s, line);
    rs_sb_putc(&s->sb, '\t');
    rs_sb_putc(&s->sb, kind);
    rs_rec_s(s, name);
    rs_sb_putc(&s->sb, '\t');
    rs_put_node(s, def, name);
    rs_rec_end(s);
}

/* The members a type's or an impl's body declares. `owner_qn` is the
 * qualified name their definitions carry as parent (NULL: no definition is
 * looked up). `callable_label` is the label of the functions' definitions. */
static void rs_scan_members(rs_scan_t *s, int mod, TSNode body, const char *owner_qn, TSNode outer,
                            bool in_trait) {
    if (ts_node_is_null(body)) {
        return;
    }
    TSTreeCursor cur = ts_tree_cursor_new(body);
    bool more = ts_tree_cursor_goto_first_child(&cur);
    for (; more && !s->failed; more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode m = ts_tree_cursor_current_node(&cur);
        const char *k = ts_node_type(m);
        char kind = 0;
        const char *label = NULL;
        if (strcmp(k, "function_item") == 0 || strcmp(k, "function_signature_item") == 0) {
            kind = 'f';
            label = "Method";
        } else if (strcmp(k, "const_item") == 0) {
            kind = 'c';
            label = in_trait ? "Variable" : NULL; /* an impl's constant has no definition */
        } else if (strcmp(k, "type_item") == 0 || strcmp(k, "associated_type") == 0) {
            kind = 't';
        } else if (strcmp(k, "field_declaration") == 0) {
            kind = 'd';
            label = "Field";
        } else if (strcmp(k, "enum_variant") == 0) {
            kind = 'v';
        }
        if (!kind) {
            continue;
        }
        char name[RS_SCAN_NAME_CAP];
        if (ts_node_start_byte(m) >= s->err_byte ||
            !rs_name_of(s, rs_field(m, "name"), name, sizeof(name))) {
            s->broken = true; /* a member this scan cannot show */
            continue;
        }
        uint32_t line = rs_line_of(m);
        int def =
            (label && owner_qn) ? rs_scan_def(s, mod, line, name, label, owner_qn) : CBM_NOT_FOUND;
        rs_rec_member(s, line, kind, name, def);
        TSNode none = {0};
        rs_rec_locals(s, line, outer, kind == 'f' ? m : none);
    }
    ts_tree_cursor_delete(&cur);
}

/* The positional fields of a tuple struct: members `0`, `1` ... without a
 * node. */
static void rs_scan_tuple_fields(rs_scan_t *s, TSNode body) {
    TSTreeCursor cur = ts_tree_cursor_new(body);
    uint32_t index = 0;
    bool more = ts_tree_cursor_goto_first_child(&cur);
    for (; more && !s->failed; more = ts_tree_cursor_goto_next_sibling(&cur)) {
        const char *field = ts_tree_cursor_current_field_name(&cur);
        if (!field || strcmp(field, "type") != 0) {
            continue;
        }
        char name[RS_UINT_DIGITS];
        snprintf(name, sizeof(name), "%u", index++);
        rs_rec_member(s, 0, 'd', name, CBM_NOT_FOUND);
    }
    ts_tree_cursor_delete(&cur);
}

/* ── Scope scan: `use` ───────────────────────────────────────────── */

typedef struct {
    TSNode node;
    const char *prefix; /* the path written before this part of the tree, "" at the top */
} rs_use_frame_t;

typedef struct {
    rs_use_frame_t *v;
    int n;
    int cap;
} rs_use_stack_t;

static void rs_use_push(rs_scan_t *s, rs_use_stack_t *st, TSNode node, const char *prefix) {
    if (ts_node_is_null(node) || s->failed) {
        return;
    }
    if (st->n >= st->cap) {
        int ncap = st->cap ? st->cap * PAIR_LEN : RS_SCAN_INIT;
        rs_use_frame_t *grown =
            (rs_use_frame_t *)rs_tmp_alloc(s, (size_t)ncap * sizeof(rs_use_frame_t));
        if (!grown) {
            return;
        }
        if (st->n > 0) {
            memcpy(grown, st->v, (size_t)st->n * sizeof(rs_use_frame_t));
        }
        st->v = grown;
        st->cap = ncap;
    }
    st->v[st->n++] = (rs_use_frame_t){.node = node, .prefix = prefix};
}

/* `prefix::path` in the scratch arena. NULL (and the file is marked) when
 * `node` is no plain path or the result is too long to carry. */
static const char *rs_use_join(rs_scan_t *s, const char *prefix, TSNode node) {
    char path[RS_SCAN_PATH_CAP];
    path[0] = '\0';
    if (!ts_node_is_null(node) && !rs_path_of(s, node, path, sizeof(path))) {
        s->broken = true;
        return NULL;
    }
    size_t pl = strlen(prefix);
    size_t nl = strlen(path);
    if (pl + nl + PAIR_LEN >= RS_SCAN_PATH_CAP) {
        s->broken = true;
        return NULL;
    }
    char *out = (char *)rs_tmp_alloc(s, pl + nl + PAIR_LEN + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    snprintf(out, pl + nl + PAIR_LEN + SKIP_ONE, "%s%s%s", prefix, (pl && nl) ? "::" : "", path);
    return out;
}

static void rs_rec_use(rs_scan_t *s, int mod, TSNode decl, char kind, const char *local,
                       const char *path) {
    rs_rec(s, 'U');
    rs_rec_u(s, (uint32_t)mod);
    rs_rec_flags(s, decl, NULL);
    rs_sb_putc(&s->sb, '\t');
    rs_sb_putc(&s->sb, kind);
    rs_rec_s(s, local);
    rs_rec_s(s, path);
    rs_rec_end(s);
}

/* A leaf of a use tree: `full` is the whole path, `alias` the name after
 * `as` (NULL when there is none). `a::b::self` imports `b` itself, in the type
 * namespace only. */
static void rs_use_leaf(rs_scan_t *s, int mod, TSNode decl, const char *full, const char *alias) {
    static const char self_seg[] = "::self";
    const size_t sl = sizeof(self_seg) - SKIP_ONE;
    char path[RS_SCAN_PATH_CAP];
    snprintf(path, sizeof(path), "%s", full);
    size_t n = strlen(path);
    char kind = 'b';
    if (n > sl && strcmp(path + n - sl, self_seg) == 0) {
        path[n - sl] = '\0';
        kind = 's';
    }
    const char *last = strrchr(path, ':');
    const char *local = alias ? alias : (last ? last + SKIP_ONE : path);
    bool keyword =
        strcmp(local, "crate") == 0 || strcmp(local, "super") == 0 || strcmp(local, "self") == 0;
    if (!local[0] || (keyword && !alias)) {
        return; /* `use crate;` binds nothing */
    }
    rs_rec_use(s, mod, decl, kind, local, path);
}

/* The bindings of one `use` declaration: its tree is walked with a stack. */
static void rs_item_use(rs_scan_t *s, TSNode decl, int mod) {
    rs_use_stack_t st = {0};
    rs_use_push(s, &st, rs_field(decl, "argument"), "");
    while (st.n > 0 && !s->failed) {
        rs_use_frame_t f = st.v[--st.n];
        const char *k = ts_node_type(f.node);
        if (strcmp(k, "use_list") == 0) {
            for (uint32_t i = ts_node_named_child_count(f.node); i > 0; i--) {
                rs_use_push(s, &st, ts_node_named_child(f.node, i - SKIP_ONE), f.prefix);
            }
        } else if (strcmp(k, "scoped_use_list") == 0) {
            const char *prefix = rs_use_join(s, f.prefix, rs_field(f.node, "path"));
            if (prefix) {
                rs_use_push(s, &st, rs_field(f.node, "list"), prefix);
            }
        } else if (strcmp(k, "use_wildcard") == 0) {
            TSNode none = {0};
            TSNode inner =
                ts_node_named_child_count(f.node) > 0 ? ts_node_named_child(f.node, 0) : none;
            const char *path = rs_use_join(s, f.prefix, inner);
            if (path && path[0]) {
                rs_rec_use(s, mod, decl, 'g', "-", path);
            }
        } else if (strcmp(k, "use_as_clause") == 0) {
            char alias[RS_SCAN_NAME_CAP];
            const char *full = rs_use_join(s, f.prefix, rs_field(f.node, "path"));
            if (full && rs_name_of(s, rs_field(f.node, "alias"), alias, sizeof(alias))) {
                rs_use_leaf(s, mod, decl, full, alias);
            } else {
                s->broken = true;
            }
        } else if (strcmp(k, "line_comment") != 0 && strcmp(k, "block_comment") != 0) {
            const char *full = rs_use_join(s, f.prefix, f.node);
            if (full) {
                rs_use_leaf(s, mod, decl, full, NULL);
            }
        }
    }
}

static void rs_item_extern_crate(rs_scan_t *s, TSNode decl, int mod) {
    char name[RS_SCAN_NAME_CAP];
    char alias[RS_SCAN_NAME_CAP];
    if (!rs_name_of(s, rs_field(decl, "name"), name, sizeof(name))) {
        s->broken = true;
        return;
    }
    bool renamed = rs_name_of(s, rs_field(decl, "alias"), alias, sizeof(alias));
    rs_rec_use(s, mod, decl, 'x', renamed ? alias : name, name);
}

/* ── Scope scan: macro invocations ───────────────────────────────── */

typedef struct {
    TSNode *v;
    int n;
    int cap;
} rs_node_stack_t;

static void rs_node_push(rs_scan_t *s, rs_node_stack_t *st, TSNode node) {
    if (s->failed) {
        return;
    }
    if (st->n >= st->cap) {
        int ncap = st->cap ? st->cap * PAIR_LEN : RS_SCAN_INIT;
        TSNode *grown = (TSNode *)rs_tmp_alloc(s, (size_t)ncap * sizeof(TSNode));
        if (!grown) {
            return;
        }
        if (st->n > 0) {
            memcpy(grown, st->v, (size_t)st->n * sizeof(TSNode));
        }
        st->v = grown;
        st->cap = ncap;
    }
    st->v[st->n++] = node;
}

/* A keyword after which a name is declared. */
static bool rs_item_keyword(const char *t) {
    static const char *const words[] = {"struct", "enum", "union", "trait", "fn",
                                        "mod",    "type", "const", "static"};
    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); i++) {
        if (strcmp(words[i], t) == 0) {
            return true;
        }
    }
    return false;
}

/* A word that may stand between such a keyword and the name (`static ref X`,
 * `const unsafe fn f`). */
static bool rs_item_modifier(const char *t) {
    static const char *const words[] = {"ref", "mut", "unsafe", "async", "extern"};
    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); i++) {
        if (strcmp(words[i], t) == 0) {
            return true;
        }
    }
    return false;
}

static bool rs_delimiter(const char *t, bool open) {
    return open ? (strcmp(t, "(") == 0 || strcmp(t, "[") == 0 || strcmp(t, "{") == 0)
                : (strcmp(t, ")") == 0 || strcmp(t, "]") == 0 || strcmp(t, "}") == 0);
}

/* A name a token tree may declare: a Q record for module `mod`, or (reading a
 * macro definition) one more name of the list `names`. */
static void rs_macro_name(rs_scan_t *s, int mod, const char *name, rs_sb_t *names) {
    if (!names) {
        rs_rec_maybe(s, mod, name);
        return;
    }
    rs_sb_puts(names, name);
    rs_sb_putc(names, ',');
    s->failed = s->failed || names->failed;
}

/* The names one token tree may declare: an identifier after an item keyword
 * (at any depth), and at the top a whole argument that is one identifier
 * (`define_label!(AppLabel, ..)`). Nested trees are pushed, not recursed.
 * A metavariable (`struct $name`) is no identifier and is not recorded. */
static void rs_macro_tree(rs_scan_t *s, rs_node_stack_t *st, TSNode tree, bool top, int mod,
                          rs_sb_t *names) {
    bool after_keyword = false;
    bool arg_start = false;
    TSNode candidate = {0};
    TSTreeCursor cur = ts_tree_cursor_new(tree);
    bool more = ts_tree_cursor_goto_first_child(&cur);
    for (; more && !s->failed; more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        const char *t = ts_node_type(c);
        bool ends_arg = strcmp(t, ",") == 0 || rs_delimiter(t, false);
        if (!ts_node_is_null(candidate) && ends_arg) {
            char name[RS_SCAN_NAME_CAP];
            if (rs_name_of(s, candidate, name, sizeof(name))) {
                rs_macro_name(s, mod, name, names);
            }
        }
        candidate = (TSNode){0};
        if (strcmp(t, "identifier") == 0) {
            /* inside a token tree a contextual keyword (`union`) and a
             * modifier (`ref` of `static ref X`) are plain identifiers */
            char name[RS_SCAN_NAME_CAP];
            bool named = rs_name_of(s, c, name, sizeof(name));
            if (named && after_keyword && rs_item_modifier(name)) {
                arg_start = false;
                continue;
            }
            if (named && after_keyword) {
                rs_macro_name(s, mod, name, names);
            } else if (top && arg_start) {
                candidate = c;
            }
            after_keyword = named && !after_keyword && rs_item_keyword(name);
        } else if (strcmp(t, "token_tree") == 0) {
            rs_node_push(s, st, c);
            after_keyword = false;
        } else if (rs_item_keyword(t)) {
            after_keyword = true;
        } else if (!rs_item_modifier(t)) {
            after_keyword = false;
        }
        arg_start = strcmp(t, ",") == 0 || rs_delimiter(t, true);
    }
    ts_tree_cursor_delete(&cur);
}

/* A macro invocation among a module's items: what it expands to is not in
 * the tree. The names written at the invocation that it may declare are
 * recorded (Q), and so is the invocation itself (V): the macro's own body may
 * declare names too (W records, of this file or another). */
static void rs_item_macro(rs_scan_t *s, TSNode invocation, int mod) {
    rs_node_stack_t st = {0};
    char name[RS_SCAN_NAME_CAP];
    TSNode macro = rs_field(invocation, "macro");
    if (!ts_node_is_null(macro) && rs_kind(macro, "scoped_identifier")) {
        macro = rs_field(macro, "name");
    }
    if (rs_name_of(s, macro, name, sizeof(name)) &&
        !(s->last_use_mod == mod && strcmp(s->last_use, name) == 0)) {
        rs_rec(s, 'V');
        rs_rec_u(s, (uint32_t)mod);
        rs_rec_s(s, name);
        rs_rec_end(s);
        s->last_use_mod = mod;
        snprintf(s->last_use, sizeof(s->last_use), "%s", name);
    }
    uint32_t nc = ts_node_named_child_count(invocation);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(invocation, i);
        if (rs_kind(c, "token_tree")) {
            rs_macro_tree(s, &st, c, true, mod, NULL);
        }
    }
    while (st.n > 0 && !s->failed) {
        TSNode tree = st.v[--st.n];
        rs_macro_tree(s, &st, tree, false, mod, NULL);
    }
}

/* What the body of a `macro_rules!` definition declares by a written name
 * (`pub struct Options { .. }` in a transcriber): a W record. A module that
 * invokes the macro may then hold those names. */
static void rs_macro_declares(rs_scan_t *s, TSNode def, const char *name) {
    rs_node_stack_t st = {0};
    rs_sb_t names = {.a = s->tmp};
    uint32_t nc = ts_node_named_child_count(def);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode rule = ts_node_named_child(def, i);
        if (rs_kind(rule, "macro_rule")) {
            rs_node_push(s, &st, rs_field(rule, "right"));
        }
    }
    while (st.n > 0 && !s->failed) {
        TSNode tree = st.v[--st.n];
        if (!ts_node_is_null(tree)) {
            rs_macro_tree(s, &st, tree, false, 0, &names);
        }
    }
    if (names.len > SKIP_ONE && !s->failed) {
        rs_rec(s, 'W');
        rs_rec_s(s, name);
        rs_sb_putc(&s->sb, '\t');
        rs_sb_putn(&s->sb, names.buf, names.len - SKIP_ONE); /* without the last comma */
        rs_rec_end(s);
    }
}

/* ── Scope scan: items ───────────────────────────────────────────── */

static void rs_block_push(rs_scan_t *s, TSNode node, int mod) {
    if (ts_node_is_null(node) || s->failed) {
        return;
    }
    if (s->nblocks >= s->blocks_cap) {
        int ncap = s->blocks_cap ? s->blocks_cap * PAIR_LEN : RS_SCAN_INIT;
        rs_block_t *grown = (rs_block_t *)rs_tmp_alloc(s, (size_t)ncap * sizeof(rs_block_t));
        if (!grown) {
            return;
        }
        if (s->nblocks > 0) {
            memcpy(grown, s->blocks, (size_t)s->nblocks * sizeof(rs_block_t));
        }
        s->blocks = grown;
        s->blocks_cap = ncap;
    }
    s->blocks[s->nblocks++] = (rs_block_t){.node = node, .mod = mod};
}

/* The first fields of a T record: T mod start end flags kind name, then the
 * tab before the node field. */
static void rs_rec_item(rs_scan_t *s, int mod, TSNode item, const rs_attrs_t *at, char kind,
                        const char *name) {
    rs_rec(s, 'T');
    rs_rec_u(s, (uint32_t)mod);
    rs_rec_u(s, rs_line_of(item));
    rs_rec_u(s, rs_end_line_of(item));
    rs_rec_flags(s, item, at);
    rs_sb_putc(&s->sb, '\t');
    rs_sb_putc(&s->sb, kind);
    rs_rec_s(s, name);
    rs_sb_putc(&s->sb, '\t');
}

/* The path a type node names, generics dropped: `a::S<T>` -> `a::S`. false
 * for a type that is no path. */
static bool rs_type_path(const rs_scan_t *s, TSNode type, char *out, size_t cap) {
    if (!ts_node_is_null(type) && rs_kind(type, "generic_type")) {
        type = rs_field(type, "type");
    }
    if (ts_node_is_null(type) ||
        !(rs_kind(type, "type_identifier") || rs_kind(type, "scoped_type_identifier") ||
          rs_kind(type, "identifier") || rs_kind(type, "scoped_identifier"))) {
        return false;
    }
    return rs_path_of(s, type, out, cap);
}

/* True when `name` is one of the type parameters of `tparams`: `impl<T> Tr
 * for T` implements for every type, not for a type named T. */
static bool rs_is_generic(const rs_scan_t *s, TSNode tparams, const char *name) {
    if (ts_node_is_null(tparams)) {
        return false;
    }
    uint32_t nc = ts_node_named_child_count(tparams);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_named_child(tparams, i);
        TSNode pn = rs_kind(p, "type_parameter") ? rs_field(p, "name") : p;
        char buf[RS_SCAN_NAME_CAP];
        if (rs_name_of(s, pn, buf, sizeof(buf)) && strcmp(buf, name) == 0) {
            return true;
        }
    }
    return false;
}

static void rs_item_impl(rs_scan_t *s, TSNode n, int mod) {
    TSNode type = rs_field(n, "type");
    TSNode trait = rs_field(n, "trait");
    TSNode tparams = rs_field(n, "type_parameters");
    char self_path[RS_SCAN_PATH_CAP];
    char trait_path[RS_SCAN_PATH_CAP];
    bool has_self = rs_type_path(s, type, self_path, sizeof(self_path)) &&
                    !rs_is_generic(s, tparams, self_path);
    bool has_trait = !ts_node_is_null(trait);
    bool trait_ok = has_trait && rs_type_path(s, trait, trait_path, sizeof(trait_path));
    rs_rec(s, 'I');
    rs_rec_u(s, (uint32_t)mod);
    rs_rec_u(s, rs_line_of(n));
    rs_rec_u(s, rs_end_line_of(n));
    rs_rec_s(s, has_self ? self_path : "?");
    rs_rec_s(s, has_trait ? (trait_ok ? trait_path : "?") : "-");
    rs_rec_end(s);
    /* the extractor names a method <type text up to '<'>.<name> */
    const char *owner_qn = NULL;
    if (has_self && !ts_node_is_null(type)) {
        uint32_t a = ts_node_start_byte(type);
        uint32_t b = ts_node_end_byte(type);
        const char *lt = b > a ? (const char *)memchr(s->src + a, '<', b - a) : NULL;
        size_t len = lt ? (size_t)(lt - (s->src + a)) : (size_t)(b - a);
        owner_qn = rs_owner_qn(s, s->src + a, len);
    }
    rs_scan_members(s, mod, rs_field(n, "body"), owner_qn, tparams, false);
}

/* What the scan does with a module-level item of one node kind. */
typedef struct {
    const char *node_kind;
    char kind;
    const char *label; /* of its definition; NULL: the extractor makes none */
} rs_item_kind_t;

static const rs_item_kind_t *rs_item_kind(const char *k) {
    static const rs_item_kind_t kinds[] = {
        {"struct_item", 'S', "Struct"},
        {"enum_item", 'E', "Enum"},
        {"union_item", 'U', "Class"},
        {"trait_item", 'R', "Interface"},
        {"type_item", 'A', "Type"},
        {"function_item", 'F', "Function"},
        {"function_signature_item", 'F', "Function"},
        {"const_item", 'C', "Variable"},
        {"static_item", 'G', "Variable"},
        {"macro_definition", 'M', NULL},
    };
    for (size_t i = 0; i < sizeof(kinds) / sizeof(kinds[0]); i++) {
        if (strcmp(kinds[i].node_kind, k) == 0) {
            return &kinds[i];
        }
    }
    return NULL;
}

/* The members of a struct, union, enum or trait. */
static void rs_item_members(rs_scan_t *s, int mod, TSNode n, char kind, const char *name) {
    TSNode body = rs_field(n, "body");
    TSNode tparams = rs_field(n, "type_parameters");
    if (ts_node_is_null(body)) {
        return;
    }
    if (rs_kind(body, "ordered_field_declaration_list")) {
        rs_scan_tuple_fields(s, body);
        return;
    }
    rs_scan_members(s, mod, body, rs_owner_qn(s, name, strlen(name)), tparams, kind == 'R');
}

/* A named item: its T record, its members, its local names. */
static void rs_item_named(rs_scan_t *s, TSNode n, const rs_item_kind_t *ik, int mod,
                          const rs_attrs_t *at) {
    char name[RS_SCAN_NAME_CAP];
    if (!rs_name_of(s, rs_field(n, "name"), name, sizeof(name))) {
        s->broken = true;
        return;
    }
    if (strcmp(name, "_") == 0) {
        return; /* `const _: () = ..` declares nothing */
    }
    char kind = ik->kind;
    const char *shown = name;
    if (kind == 'F' && at->macro_kind) {
        kind = at->macro_kind; /* the function IS a procedural macro */
        shown = (kind == 'V' && at->derive[0]) ? at->derive : name;
    }
    uint32_t line = rs_line_of(n);
    int def = ik->label ? rs_scan_def(s, mod, line, name, ik->label, NULL) : CBM_NOT_FOUND;
    rs_rec_item(s, mod, n, at, kind, shown);
    rs_put_node(s, def, shown);
    char alias[RS_SCAN_PATH_CAP];
    bool aliased = kind == 'A' && rs_type_path(s, rs_field(n, "type"), alias, sizeof(alias));
    bool derived =
        (kind == 'S' || kind == 'E' || kind == 'U') && (at->derives[0] || at->derives_more);
    if (derived) {
        rs_rec_s(s, at->derives);
        rs_sb_puts(&s->sb, at->derives_more ? (at->derives[0] ? ",*" : "*") : "");
    } else {
        rs_rec_s(s, aliased ? alias : "-");
    }
    rs_rec_end(s);
    if (rs_kind(n, "macro_definition")) {
        rs_macro_declares(s, n, name);
    }
    if (kind == 'S' || kind == 'U' || kind == 'E' || kind == 'R') {
        rs_item_members(s, mod, n, kind, name);
    }
    TSNode none = {0};
    rs_rec_locals(s, line, none, n);
}

static void rs_item_mod(rs_scan_t *s, TSNode n, int mod, const rs_attrs_t *at) {
    char name[RS_SCAN_NAME_CAP];
    if (!rs_name_of(s, rs_field(n, "name"), name, sizeof(name))) {
        s->broken = true;
        return;
    }
    TSNode body = rs_field(n, "body");
    if (ts_node_is_null(body)) {
        rs_rec(s, 'D');
        rs_rec_u(s, (uint32_t)mod);
        rs_rec_flags(s, n, at);
        rs_rec_s(s, name);
        rs_rec_s(s, at->path[0] ? at->path : "-");
        rs_rec_end(s);
        return;
    }
    int id = s->next_mod++;
    rs_rec(s, 'N');
    rs_rec_u(s, (uint32_t)id);
    rs_rec_u(s, (uint32_t)mod);
    rs_rec_u(s, rs_line_of(n));
    rs_rec_u(s, rs_end_line_of(n));
    rs_rec_flags(s, n, at);
    if (at->path[0] && !at->cond_path) {
        rs_sb_putc(&s->sb, 'c'); /* `#[path]` on an inline module moves its `mod x;` files */
    }
    rs_rec_s(s, name);
    rs_rec_end(s);
    rs_block_push(s, body, id);
}

/* An item that starts where the tree is no longer trusted: only its name is
 * kept, as one that may be declared somewhere in the file. */
static void rs_item_untrusted(rs_scan_t *s, TSNode n) {
    char name[RS_SCAN_NAME_CAP];
    s->broken = true;
    if (rs_name_of(s, rs_field(n, "name"), name, sizeof(name))) {
        rs_rec_maybe(s, CBM_NOT_FOUND, name);
    }
}

static void rs_scan_item(rs_scan_t *s, TSNode n, int mod, const rs_attrs_t *at) {
    const char *k = ts_node_type(n);
    if (ts_node_start_byte(n) >= s->err_byte || strcmp(k, "ERROR") == 0) {
        rs_item_untrusted(s, n);
        return;
    }
    const rs_item_kind_t *ik = rs_item_kind(k);
    if (ik) {
        rs_item_named(s, n, ik, mod, at);
    } else if (strcmp(k, "mod_item") == 0) {
        rs_item_mod(s, n, mod, at);
    } else if (strcmp(k, "use_declaration") == 0) {
        rs_item_use(s, n, mod);
    } else if (strcmp(k, "extern_crate_declaration") == 0) {
        rs_item_extern_crate(s, n, mod);
    } else if (strcmp(k, "impl_item") == 0) {
        rs_item_impl(s, n, mod);
    } else if (strcmp(k, "foreign_mod_item") == 0) {
        rs_block_push(s, rs_field(n, "body"), mod); /* its items are the module's */
    } else if (strcmp(k, "macro_invocation") == 0) {
        rs_item_macro(s, n, mod);
    } else if (strcmp(k, "expression_statement") == 0 && ts_node_named_child_count(n) > 0 &&
               rs_kind(ts_node_named_child(n, 0), "macro_invocation")) {
        rs_item_macro(s, ts_node_named_child(n, 0), mod);
    }
}

/* The items of one block, in order; attributes belong to the item after
 * them. */
static void rs_scan_block(rs_scan_t *s, rs_block_t block) {
    rs_attrs_t *at = (rs_attrs_t *)rs_tmp_alloc(s, sizeof(*at));
    if (!at) {
        return;
    }
    memset(at, 0, sizeof(*at));
    TSTreeCursor cur = ts_tree_cursor_new(block.node);
    bool more = ts_tree_cursor_goto_first_child(&cur);
    for (; more && !s->failed; more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        const char *k = ts_node_type(n);
        if (strcmp(k, "attribute_item") == 0) {
            rs_attr_read(s, n, at);
            continue;
        }
        if (!ts_node_is_named(n) || strcmp(k, "line_comment") == 0 ||
            strcmp(k, "block_comment") == 0 || strcmp(k, "inner_attribute_item") == 0) {
            continue;
        }
        rs_scan_item(s, n, block.mod, at);
        memset(at, 0, sizeof(*at));
    }
    ts_tree_cursor_delete(&cur);
}

/* ── Scope scan: the driver ──────────────────────────────────────── */

/* True when the delimiters in text[0..n) pair up. */
static bool rs_delims_balanced(const char *text, size_t n) {
    static const char open[] = "([{";
    static const char close[] = ")]}";
    enum { KINDS = 3 };
    long depth[KINDS] = {0, 0, 0};
    for (size_t i = 0; i < n; i++) {
        for (int k = 0; k < KINDS; k++) {
            depth[k] += text[i] == open[k];
            depth[k] -= text[i] == close[k];
            if (depth[k] < 0) {
                return false;
            }
        }
    }
    return depth[0] == 0 && depth[1] == 0 && depth[PAIR_LEN] == 0;
}

/* An error node that cannot have moved an item boundary: it lies inside a
 * block (a function body: nothing in there is scanned), and the parser
 * neither skipped unpaired delimiters nor made one up. Syntax the grammar
 * does not know yet parses this way; the items around the function are then
 * exactly where the tree puts them. */
static bool rs_error_contained(const rs_scan_t *s, TSNode n, bool in_block) {
    uint32_t a = ts_node_start_byte(n);
    uint32_t b = ts_node_end_byte(n);
    if (!in_block || b > s->src_len) {
        return false;
    }
    if (ts_node_is_missing(n)) {
        const char *t = ts_node_type(n);
        return !rs_delimiter(t, true) && !rs_delimiter(t, false);
    }
    return b <= a || rs_delims_balanced(s->src + a, b - a);
}

/* The first ERROR or MISSING node of the tree, in document order, that is not
 * contained (rs_error_contained); a null node when there is none. Walks only
 * the nodes that hold an error, with a cursor. */
static TSNode rs_first_error(const rs_scan_t *s, TSNode root) {
    TSNode found = {0};
    if (!ts_node_has_error(root)) {
        return found;
    }
    TSTreeCursor cur = ts_tree_cursor_new(root);
    int depth = 0;
    int block_depth = CBM_NOT_FOUND; /* depth of the outermost block around the cursor */
    for (bool more = true; more;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        bool error = ts_node_is_missing(n) || rs_kind(n, "ERROR");
        if (error && !rs_error_contained(s, n, block_depth >= 0)) {
            found = n;
            break;
        }
        if (block_depth < 0 && rs_kind(n, "block")) {
            block_depth = depth;
        }
        if (!error && ts_node_has_error(n) && ts_tree_cursor_goto_first_child(&cur)) {
            depth++;
            continue;
        }
        for (;;) { /* on to the next node: a sibling, or an ancestor's sibling */
            if (block_depth == depth) {
                block_depth = CBM_NOT_FOUND; /* leaving the block itself */
            }
            if (ts_tree_cursor_goto_next_sibling(&cur)) {
                break;
            }
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
            depth--;
        }
    }
    ts_tree_cursor_delete(&cur);
    return found;
}

static int rs_u32_cmp(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

/* The start lines of the definitions whose docs hold references (the file's
 * own doc is no definition), sorted. */
static void rs_scan_ref_lines(rs_scan_t *s) {
    const CBMDocLinkArray *links = &s->ctx->result->doc_links;
    s->ref_lines = (uint32_t *)rs_tmp_alloc(s, (size_t)links->count * sizeof(uint32_t));
    if (!s->ref_lines) {
        return;
    }
    int n = 0;
    for (int i = 0; i < links->count; i++) {
        if (!(links->items[i].flags & CBM_DOCLINK_FLAG_FILE)) {
            s->ref_lines[n++] = links->items[i].def_line;
        }
    }
    if (n > SKIP_ONE) {
        qsort(s->ref_lines, (size_t)n, sizeof(uint32_t), rs_u32_cmp);
    }
    s->nref_lines = n;
}

/* Two definitions of one qualified name that are one member name of one type
 * (a field and the method named like it), or one item name of one module
 * (declared twice under different cfg): the node stands for both, and a
 * reference in the doc of either may leave from it. Definitions of different
 * modules or parents only share a name the graph cannot tell apart. */
static bool rs_same_declared_name(const rs_scan_t *s, int a, int b) {
    const CBMDefArray *defs = &s->ctx->result->defs;
    const char *pa = defs->items[a].parent_class;
    const char *pb = defs->items[b].parent_class;
    bool same_parent = (!pa && !pb) || (pa && pb && strcmp(pa, pb) == 0);
    return same_parent && s->def_mod[a] != 0 && s->def_mod[a] == s->def_mod[b];
}

/* Z records: a documented definition with references that does not own its
 * node and shares it with an unrelated definition (rs_same_declared_name).
 * Its references would otherwise leave from that other definition's node. */
static void rs_scan_unowned(rs_scan_t *s) {
    const CBMDefArray *defs = &s->ctx->result->defs;
    for (int i = 0; i < defs->count && !s->failed; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (s->owner[i] || !d->docstring || !d->qualified_name || !d->name ||
            !rs_has_refs(s, d->start_line) ||
            strncmp(d->qualified_name, s->item_prefix, s->item_prefix_len) != 0 ||
            (s->owner_of[i] >= 0 && rs_same_declared_name(s, i, s->owner_of[i]))) {
            continue;
        }
        const char *suffix = d->qualified_name + s->item_prefix_len;
        if (strpbrk(suffix, "\t\n\r")) {
            continue;
        }
        rs_rec(s, 'Z');
        rs_rec_u(s, d->start_line);
        rs_rec_s(s, suffix);
        rs_rec_end(s);
    }
}

const char *cbm_doclink_rs_scan_scope(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || ts_node_is_null(ctx->root) || !ctx->source) {
        return NULL;
    }
    rs_scan_t s = {.ctx = ctx,
                   .tmp = rs_scratch(ctx),
                   .src = ctx->source,
                   .src_len = ctx->source_len > 0 ? (uint32_t)ctx->source_len : 0,
                   .sb = {.a = ctx->arena},
                   .err_byte = UINT32_MAX,
                   .next_mod = SKIP_ONE,
                   .last_use_mod = CBM_NOT_FOUND};
    /* the qualified name of an item `x` of this file, minus the `x` */
    const char *probe = cbm_fqn_compute(s.tmp, ctx->project, ctx->rel_path, "x");
    if (!probe || strlen(probe) < PAIR_LEN) {
        return NULL;
    }
    s.item_prefix = probe;
    s.item_prefix_len = strlen(probe) - SKIP_ONE;
    rs_scan_defs(&s);
    rs_scan_ref_lines(&s);
    TSNode err = rs_first_error(&s, ctx->root);
    rs_sb_puts(&s.sb, CBM_DOCLINK_RS_SCOPE_TAG "\n");
    if (!ts_node_is_null(err)) {
        s.err_byte = ts_node_start_byte(err);
        s.broken = true;
        rs_rec(&s, 'E');
        rs_rec_u(&s, rs_line_of(err));
        rs_rec_end(&s);
    }
    rs_block_push(&s, ctx->root, 0);
    for (int b = 0; b < s.nblocks && !s.failed; b++) {
        rs_scan_block(&s, s.blocks[b]);
    }
    if (!s.failed) {
        rs_scan_unowned(&s);
    }
    if (s.broken) {
        rs_sb_puts(&s.sb, "B\n");
    }
    if (s.failed || s.sb.failed || !s.sb.buf) {
        return NULL;
    }
    return s.sb.buf;
}

/* ── Cargo manifest ──────────────────────────────────────────────────
 *
 * The scope blob of a `Cargo.toml` (tag line "rs1", then "C"): what the Rust
 * resolver knows of packages, targets and dependencies. The indexer extracts
 * the file anyway, so the resolver reads no file of its own.
 *   K  table key value          a key of [package], [lib] or
 *                               [workspace.package] the resolver uses
 *   A  kind                     one [[bin]] / [[example]] / [[test]] /
 *                               [[bench]] target; its keys follow as
 *   a  key value                `name` or `path` of that target
 *   W                           the manifest has a [workspace] table
 *   X  kind name attr value     a dependency. kind: d dependencies, v dev,
 *                               b build, w workspace.dependencies; attr '-'
 *                               when the entry is only named, else `path`,
 *                               `package` or `workspace`
 *   B                           the TOML tree has a parse error
 * A value this scan cannot carry (an escape, a multi-line string, a tab) is
 * written as "?": the resolver then knows the key is set, not to what. */

enum { RS_TOML_SEGS = 8 };

typedef struct {
    char seg[RS_TOML_SEGS][RS_SCAN_NAME_CAP];
    int n;
    bool ok; /* false: more segments, a longer one, or an escape */
} rs_toml_key_t;

enum {
    RS_TABLE_OTHER = 0,
    RS_TABLE_PACKAGE,
    RS_TABLE_LIB,
    RS_TABLE_WORKSPACE_PACKAGE,
    RS_TABLE_TARGET, /* one element of a target array */
    RS_TABLE_DEPS,   /* a dependency table: `dep` is its kind */
    RS_TABLE_DEP,    /* the table of one dependency: `dep` and `name` */
};

typedef struct {
    int kind;
    char dep;
    const char *name;  /* RS_TABLE_DEP: the dependency; else the table's dotted name */
    bool is_workspace; /* [workspace] or a table below it */
} rs_table_t;

void cbm_doclink_rs_manifest_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def,
                                       const char *doc, uint32_t doc_line) {
    (void)ctx;
    (void)def;
    (void)doc;
    (void)doc_line; /* a manifest holds no doc-comment references */
}

static bool rs_is_manifest(const char *rel_path) {
    static const char name[] = "Cargo.toml";
    const char *slash = strrchr(rel_path, '/');
    return strcmp(slash ? slash + SKIP_ONE : rel_path, name) == 0;
}

/* The dotted key written at text[0..n): `a.'b c'."d"` -> a, b c, d. */
static void rs_toml_key(const char *text, size_t n, rs_toml_key_t *out) {
    out->n = 0;
    out->ok = true;
    size_t i = 0;
    while (i < n && out->ok) {
        while (i < n && (text[i] == ' ' || text[i] == '\t')) {
            i++;
        }
        char quote = (i < n && (text[i] == '"' || text[i] == '\'')) ? text[i] : 0;
        size_t start = quote ? i + SKIP_ONE : i;
        size_t e = start;
        while (e < n &&
               (quote ? text[e] != quote : (text[e] != '.' && text[e] != ' ' && text[e] != '\t'))) {
            out->ok = out->ok && text[e] != '\\';
            e++;
        }
        if (out->n >= RS_TOML_SEGS || e - start >= RS_SCAN_NAME_CAP || e == start ||
            (quote && e >= n)) {
            out->ok = false;
            return;
        }
        memcpy(out->seg[out->n], text + start, e - start);
        out->seg[out->n][e - start] = '\0';
        out->n++;
        i = quote ? e + SKIP_ONE : e;
        while (i < n && (text[i] == ' ' || text[i] == '\t')) {
            i++;
        }
        if (i < n && text[i] != '.') {
            out->ok = false;
            return;
        }
        i++;
    }
}

static void rs_toml_key_of(const rs_scan_t *s, TSNode key, rs_toml_key_t *out) {
    uint32_t a = ts_node_start_byte(key);
    uint32_t b = ts_node_end_byte(key);
    if (ts_node_is_null(key) || b <= a || b > s->src_len) {
        out->n = 0;
        out->ok = false;
        return;
    }
    rs_toml_key(s->src + a, b - a, out);
}

static bool rs_toml_is_key(TSNode n) {
    return rs_kind(n, "bare_key") || rs_kind(n, "quoted_key") || rs_kind(n, "dotted_key");
}

/* A string or boolean value as text; "?" for one the blob cannot carry. NULL
 * for a value of another kind. */
static const char *rs_toml_scalar(rs_scan_t *s, TSNode v) {
    if (ts_node_is_null(v)) {
        return NULL;
    }
    uint32_t a = ts_node_start_byte(v);
    uint32_t b = ts_node_end_byte(v);
    if (b <= a || b > s->src_len) {
        return NULL;
    }
    const char *text = s->src + a;
    size_t n = b - a;
    if (rs_kind(v, "boolean")) {
        return rs_word_is(text, n, "true") ? "true" : "false";
    }
    if (!rs_kind(v, "string")) {
        return NULL;
    }
    char q = text[0];
    bool plain = n >= PAIR_LEN && (q == '"' || q == '\'') && text[n - SKIP_ONE] == q &&
                 n - PAIR_LEN < RS_SCAN_PATH_CAP;
    for (size_t i = SKIP_ONE; plain && i + SKIP_ONE < n; i++) {
        unsigned char c = (unsigned char)text[i];
        plain = c != (unsigned char)q && c >= ' ' && !(q == '"' && c == '\\');
    }
    if (!plain || n == PAIR_LEN) {
        return "?";
    }
    char *out = (char *)rs_tmp_alloc(s, n);
    if (!out) {
        return NULL;
    }
    memcpy(out, text + SKIP_ONE, n - PAIR_LEN);
    out[n - PAIR_LEN] = '\0';
    return out;
}

static char rs_dep_kind(const char *seg) {
    static const struct {
        const char *name;
        char kind;
    } table[] = {
        {"dependencies", 'd'},       {"dev-dependencies", 'v'},   {"dev_dependencies", 'v'},
        {"build-dependencies", 'b'}, {"build_dependencies", 'b'},
    };
    for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
        if (strcmp(table[i].name, seg) == 0) {
            return table[i].kind;
        }
    }
    return 0;
}

/* What the table named `key` is. `array`: it is written `[[key]]`. */
static rs_table_t rs_table_class(const rs_toml_key_t *key, bool array) {
    rs_table_t t = {.kind = RS_TABLE_OTHER};
    if (!key->ok || key->n == 0) {
        return t;
    }
    const char *first = key->seg[0];
    const char *last = key->seg[key->n - SKIP_ONE];
    t.is_workspace = strcmp(first, "workspace") == 0;
    static const char *const targets[] = {"bin", "example", "test", "bench"};
    for (size_t i = 0; array && key->n == SKIP_ONE && i < sizeof(targets) / sizeof(targets[0]);
         i++) {
        if (strcmp(first, targets[i]) == 0) {
            t.kind = RS_TABLE_TARGET;
            t.name = targets[i];
            return t;
        }
    }
    if (array) {
        return t;
    }
    if (key->n == SKIP_ONE && strcmp(first, "package") == 0) {
        t.kind = RS_TABLE_PACKAGE;
        t.name = "package";
    } else if (key->n == SKIP_ONE && strcmp(first, "lib") == 0) {
        t.kind = RS_TABLE_LIB;
        t.name = "lib";
    } else if (key->n == PAIR_LEN && t.is_workspace && strcmp(last, "package") == 0) {
        t.kind = RS_TABLE_WORKSPACE_PACKAGE;
        t.name = "workspace.package";
    } else if (rs_dep_kind(last)) {
        t.kind = RS_TABLE_DEPS;
        t.dep = t.is_workspace ? 'w' : rs_dep_kind(last);
    } else if (key->n >= PAIR_LEN && rs_dep_kind(key->seg[key->n - PAIR_LEN])) {
        t.kind = RS_TABLE_DEP;
        t.dep = t.is_workspace ? 'w' : rs_dep_kind(key->seg[key->n - PAIR_LEN]);
        t.name = last;
    }
    return t;
}

static void rs_rec_dep(rs_scan_t *s, char kind, const char *name, const char *attr,
                       const char *value) {
    rs_rec(s, 'X');
    rs_sb_putc(&s->sb, '\t');
    rs_sb_putc(&s->sb, kind);
    rs_rec_s(s, name);
    rs_rec_s(s, attr);
    rs_rec_s(s, value);
    rs_rec_end(s);
}

static bool rs_dep_attr(const char *key) {
    return strcmp(key, "path") == 0 || strcmp(key, "package") == 0 || strcmp(key, "workspace") == 0;
}

/* The key and value node of a `pair`. */
static void rs_toml_pair(TSNode pair, TSNode *key, TSNode *value) {
    *key = (TSNode){0};
    *value = (TSNode){0};
    uint32_t nc = ts_node_named_child_count(pair);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(pair, i);
        if (rs_kind(c, "comment")) {
            continue;
        }
        if (ts_node_is_null(*key) && rs_toml_is_key(c)) {
            *key = c;
        } else if (!ts_node_is_null(*key)) {
            *value = c;
        }
    }
}

/* One entry of a dependency table: `name = "1"`, `name = { path = .. }`,
 * `name.workspace = true`. */
static void rs_manifest_dep(rs_scan_t *s, char kind, const rs_toml_key_t *key, TSNode value) {
    const char *name = key->seg[0];
    if (strpbrk(name, "\t\n\r")) {
        return;
    }
    rs_rec_dep(s, kind, name, "-", "-");
    if (key->n == PAIR_LEN && rs_dep_attr(key->seg[1])) {
        const char *v = rs_toml_scalar(s, value);
        rs_rec_dep(s, kind, name, key->seg[1], v ? v : "?");
        return;
    }
    if (key->n != SKIP_ONE || ts_node_is_null(value) || !rs_kind(value, "inline_table")) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(value);
    for (uint32_t i = 0; i < nc && !s->failed; i++) {
        TSNode inner = ts_node_named_child(value, i);
        TSNode ik;
        TSNode iv;
        if (!rs_kind(inner, "pair")) {
            continue;
        }
        rs_toml_pair(inner, &ik, &iv);
        rs_toml_key_t in_key;
        rs_toml_key_of(s, ik, &in_key);
        if (in_key.ok && in_key.n == SKIP_ONE && rs_dep_attr(in_key.seg[0])) {
            const char *v = rs_toml_scalar(s, iv);
            rs_rec_dep(s, kind, name, in_key.seg[0], v ? v : "?");
        }
    }
}

/* True for the keys of [package], [lib], [workspace.package] and a target
 * that the resolver reads. */
static bool rs_manifest_key_used(const char *key) {
    static const char *const keys[] = {"name",       "edition",      "build",     "path",
                                       "autobins",   "autoexamples", "autotests", "autobenches",
                                       "proc-macro", "proc_macro"};
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {
        if (strcmp(keys[i], key) == 0) {
            return true;
        }
    }
    return false;
}

/* `key = value` of [package], [lib], [workspace.package] or a target.
 * `edition.workspace = true` and `edition = { workspace = true }` are both
 * written as the key `edition.workspace`. */
static void rs_manifest_setting(rs_scan_t *s, const rs_table_t *t, const rs_toml_key_t *key,
                                TSNode value) {
    if (!rs_manifest_key_used(key->seg[0])) {
        return;
    }
    const char *v = rs_toml_scalar(s, value);
    bool inherited = key->n == PAIR_LEN && strcmp(key->seg[1], "workspace") == 0;
    if (key->n == SKIP_ONE && !v && !ts_node_is_null(value) && rs_kind(value, "inline_table")) {
        uint32_t a = ts_node_start_byte(value);
        uint32_t b = ts_node_end_byte(value);
        inherited = b > a && b <= s->src_len && rs_has_word(s->src + a, b - a, "workspace");
        v = "true";
    }
    if (key->n > PAIR_LEN || (key->n == PAIR_LEN && !inherited)) {
        return;
    }
    if (t->kind == RS_TABLE_TARGET) {
        rs_rec(s, 'a');
    } else {
        rs_rec(s, 'K');
        rs_rec_s(s, t->name);
    }
    rs_sb_putc(&s->sb, '\t');
    rs_sb_puts(&s->sb, key->seg[0]);
    if (inherited) {
        rs_sb_puts(&s->sb, ".workspace");
    }
    rs_rec_s(s, v ? v : "?");
    rs_rec_end(s);
}

/* The pairs of one table (a `table` or `table_array_element` node). */
static void rs_manifest_table(rs_scan_t *s, TSNode table, bool array) {
    rs_toml_key_t name = {.n = 0, .ok = false};
    rs_table_t t = {.kind = RS_TABLE_OTHER};
    TSTreeCursor cur = ts_tree_cursor_new(table);
    bool more = ts_tree_cursor_goto_first_child(&cur);
    bool named = false;
    for (; more && !s->failed; more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (!named && rs_toml_is_key(c)) {
            named = true;
            rs_toml_key_of(s, c, &name);
            t = rs_table_class(&name, array);
            if (t.is_workspace) {
                rs_sb_puts(&s->sb, "W\n");
            }
            if (t.kind == RS_TABLE_TARGET) {
                rs_rec(s, 'A');
                rs_rec_s(s, t.name);
                rs_rec_end(s);
            } else if (t.kind == RS_TABLE_DEP && !strpbrk(t.name, "\t\n\r")) {
                rs_rec_dep(s, t.dep, t.name, "-", "-");
            }
            continue;
        }
        if (!rs_kind(c, "pair") || t.kind == RS_TABLE_OTHER) {
            continue;
        }
        TSNode k;
        TSNode v;
        rs_toml_pair(c, &k, &v);
        rs_toml_key_t key;
        rs_toml_key_of(s, k, &key);
        if (!key.ok || key.n == 0) {
            continue;
        }
        if (t.kind == RS_TABLE_DEPS) {
            rs_manifest_dep(s, t.dep, &key, v);
        } else if (t.kind == RS_TABLE_DEP) {
            if (key.n == SKIP_ONE && rs_dep_attr(key.seg[0])) {
                const char *val = rs_toml_scalar(s, v);
                rs_rec_dep(s, t.dep, t.name, key.seg[0], val ? val : "?");
            }
        } else {
            rs_manifest_setting(s, &t, &key, v);
        }
    }
    ts_tree_cursor_delete(&cur);
}

const char *cbm_doclink_rs_manifest_scan_scope(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || !ctx->rel_path || !ctx->source || ts_node_is_null(ctx->root) ||
        !rs_is_manifest(ctx->rel_path)) {
        return NULL;
    }
    rs_scan_t s = {.ctx = ctx,
                   .tmp = rs_scratch(ctx),
                   .src = ctx->source,
                   .src_len = ctx->source_len > 0 ? (uint32_t)ctx->source_len : 0,
                   .sb = {.a = ctx->arena}};
    rs_sb_puts(&s.sb, CBM_DOCLINK_RS_SCOPE_TAG "\nC\n");
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    bool more = ts_tree_cursor_goto_first_child(&cur);
    for (; more && !s.failed; more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (rs_kind(c, "table")) {
            rs_manifest_table(&s, c, false);
        } else if (rs_kind(c, "table_array_element")) {
            rs_manifest_table(&s, c, true);
        }
    }
    ts_tree_cursor_delete(&cur);
    if (ts_node_has_error(ctx->root)) {
        rs_sb_puts(&s.sb, "B\n");
    }
    if (s.failed || s.sb.failed || !s.sb.buf) {
        return NULL;
    }
    return s.sb.buf;
}

/* ── Persisted form ──────────────────────────────────────────────── */

/* Field positions (0-based, the tag is field 0) that hold line numbers. */
static bool rs_line_field(char tag, int field) {
    enum { F1 = 1, F2 = 2, F3 = 3, F4 = 4 };
    switch (tag) {
    case 'N':
        return field == F3 || field == F4;
    case 'T':
    case 'I':
        return field == F2 || field == F3;
    case 'M':
        return field == F1;
    default:
        return false;
    }
}

/* Records only the file's own references need: never persisted. */
static bool rs_fresh_only(char tag) {
    return tag == 'P' || tag == 'Z' || tag == 'E';
}

char *cbm_doclink_rs_portable_scope(const char *scope) {
    size_t n = strlen(scope);
    char *out = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    const char *p = scope;
    bool first = true;
    while (*p) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        char tag = first ? 0 : p[0];
        const char *next = nl ? nl + SKIP_ONE : p + len;
        if (rs_fresh_only(tag)) {
            p = next;
            first = false;
            continue;
        }
        int field = 0;
        size_t i = 0;
        while (i < len) {
            size_t fend = i;
            while (fend < len && p[fend] != '\t') {
                fend++;
            }
            if (rs_line_field(tag, field)) {
                out[w++] = '0';
            } else {
                memcpy(out + w, p + i, fend - i);
                w += fend - i;
            }
            i = fend;
            if (i < len) {
                out[w++] = '\t';
                i++;
                field++;
            }
        }
        if (nl) {
            out[w++] = '\n';
        }
        p = next;
        first = false;
    }
    out[w] = '\0';
    return out;
}
