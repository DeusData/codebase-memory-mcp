/*
 * doclink_jvm.c — Java and Kotlin doc-comment references and the JVM doc-link
 * scope.
 *
 * Tokens.
 *   Javadoc: `{@link R}` / `{@link R label}`, `{@linkplain R label}`,
 *   `{@value R#F}` (a bare `{@value}` names the documented field itself and is
 *   no token), `@see R` at the start of a block-tag line (`@see <a href=..>` is
 *   a URL, `@see "text"` is prose), `@throws T` / `@exception T`. Nothing in
 *   `{@code}`, `{@literal}`, `{@snippet}` or an HTML comment is a reference.
 *   KDoc: `[Name]` and `[text][Name]` in the comment text (not `[text](url)`,
 *   not an image, not a `[label]: url` definition, not in a code span or a
 *   fenced or indented code block), `@see Name`, `@throws T` / `@exception T`,
 *   `@sample qualified.name`. The subject of `@param` / `@property` is the
 *   definition's own parameter, never a reference.
 *   Only block doc comments (slash, two stars) are read: that is what javadoc
 *   and Dokka read.
 *
 * Scope blob (one record per line, tab-separated, first line "jvm1"; records
 * in document order, a member after its type):
 *   L  lang                               j Java, k Kotlin
 *   P  package                            "" when the file declares none
 *   I  kind  alias  target                import: s single (alias = the simple
 *                                         name it binds), w on-demand / star,
 *                                         m Java static single (alias = member,
 *                                         target = type), n Java static
 *                                         on-demand (target = type)
 *   T  start  end  kind  path  tparams  supers  slot  own  pnames
 *                                         type: c class, i interface, e enum,
 *                                         r record, a annotation, o object,
 *                                         C companion object, t typealias; '!'
 *                                         after the kind when a parse error
 *                                         hides some of its members. path is
 *                                         the dotted LOGICAL path (Outer.Inner),
 *                                         tparams ','-joined, supers '|'-joined
 *                                         as written without type arguments
 *                                         ('?' for one that is no name)
 *   M  start  kind  flags  owner  name  tparams  sig  recv  slot  own  pnames
 *                                         member: c callable, v value. flags: K
 *                                         constructor, p constructor property /
 *                                         record component, n enum constant, i
 *                                         implicit (declared by no text of its
 *                                         own). owner is the index of its T
 *                                         record ('-' for a Kotlin top-level
 *                                         declaration), sig the '|'-joined
 *                                         normalized parameter types ('-' for a
 *                                         value), recv the receiver type of a
 *                                         Kotlin extension
 *   Q  name                               a type that is declared, but could not
 *                                         be placed (parse error)
 *   X  from  to                           lines whose declarations could not be
 *                                         placed
 * slot / own say where the extractor put the declaration's node: slot is the
 * qualified name of its definition relative to the file's module ('=' when it
 * is the logical path, '-' when the declaration has no definition), own is 1
 * when that definition is the one the graph keeps for the qualified name
 * inside this file. Both are read off the extractor's own definitions (same
 * start line, name and kind), so this file models neither the qualified-name
 * scheme nor its collisions: Kotlin flattens nested types and puts class
 * properties at module level, Java names a field after its whole declarator
 * (`MAX = 10`), overloads share one name, and whatever the scheme does next is
 * followed without a change here.
 * pnames (the parameter names of a callable, the primary-constructor
 * parameters of a Kotlin class) and all line numbers serve the file's own
 * references only: the persisted form drops them.
 *
 * Everything is a pure function of the file's bytes. Nesting is the tree's
 * while the tree has no parse error; otherwise it is checked against the
 * braces (see "Braces").
 */
#include "doclink.h"

#include "arena.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"
#include "helpers.h"
#include "tree_sitter/api.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    JV_SB_INIT = 1024,
    JV_MAX_DEPTH = 128, /* nested type declarations */
    JV_NAME_MAX = 255,  /* one identifier */
    JV_TYPE_MAX = 512,  /* one written type */
    JV_SLOT_MAX = 512,  /* a definition's name relative to its module */
    JV_UINT_DIGITS = 16,
    JV_ITEMS_INIT = 64,
    JV_BRACES_INIT = 1024,
    JV_CHAR_LITERAL_MAX = 12, /* a quoted unicode escape */
    JV_QUOTES = 3,            /* """ */
    JV_UTF8_TAIL_MAX = 3,
};

/* ── Small arena string builder ──────────────────────────────────── */

typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool failed;
} jv_sb_t;

static void jv_reserve(jv_sb_t *sb, size_t extra) {
    if (sb->failed || sb->len + extra + SKIP_ONE <= sb->cap) {
        return;
    }
    size_t ncap = sb->cap ? sb->cap : JV_SB_INIT;
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

static void jv_putn(jv_sb_t *sb, const char *s, size_t n) {
    jv_reserve(sb, n);
    if (sb->failed) {
        return;
    }
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

static void jv_puts(jv_sb_t *sb, const char *s) {
    jv_putn(sb, s, strlen(s));
}

static void jv_putc(jv_sb_t *sb, char c) {
    jv_putn(sb, &c, SKIP_ONE);
}

static void jv_putu(jv_sb_t *sb, uint32_t v) {
    char tmp[JV_UINT_DIGITS];
    int n = snprintf(tmp, sizeof(tmp), "%u", v);
    if (n > 0) {
        jv_putn(sb, tmp, (size_t)n);
    }
}

/* ── Characters ──────────────────────────────────────────────────── */

static bool jv_blank(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v';
}

/* Identifier characters: Java and Kotlin identifiers are Unicode, so every
 * byte of a multi-byte character counts. */
static bool jv_ident_start(char c) {
    unsigned char u = (unsigned char)c;
    return isalpha(u) || c == '_' || c == '$' || u >= 0x80;
}

static bool jv_ident_char(char c) {
    unsigned char u = (unsigned char)c;
    return isalnum(u) || c == '_' || c == '$' || u >= 0x80;
}

/* ── Doc text ────────────────────────────────────────────────────── */

/* The text of the block doc comments in `doc` (slash, two stars) without its
 * decoration: every line break stays (a position's line is the doc's first
 * line plus the breaks before it), each continuation line loses its leading
 * blanks, its asterisks and one blank. Line comments and plain block comments
 * between doc comments leave only their line breaks. `nesting`: block
 * comments nest (Kotlin), so a comment written inside the doc is part of its
 * text and its end is not the doc's. */
static char *jv_clean_doc(CBMArena *a, const char *doc, bool nesting, size_t *out_len) {
    size_t n = strlen(doc);
    char *out = (char *)cbm_arena_alloc(a, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    size_t i = 0;
    bool in_doc = false;
    size_t inner = 0; /* comments open inside the doc */
    while (i < n) {
        char c = doc[i];
        if (!in_doc) {
            if (c == '/' && doc[i + 1] == '*' && doc[i + 2] == '*' && doc[i + 3] != '/') {
                in_doc = true;
                inner = 0;
                i += 3;
                while (doc[i] == '*' && doc[i + 1] != '/') {
                    i++;
                }
                continue;
            }
            if (c == '\n') {
                out[w++] = '\n';
            }
            i++;
            continue;
        }
        if (nesting && c == '/' && doc[i + 1] == '*') {
            inner++;
            out[w++] = '/';
            out[w++] = '*';
            i += 2;
            continue;
        }
        if (c == '*' && doc[i + 1] == '/') {
            if (inner > 0) {
                inner--;
                out[w++] = '*';
                out[w++] = '/';
            } else {
                in_doc = false;
            }
            i += 2;
            continue;
        }
        if (c == '\n') {
            out[w++] = '\n';
            i++;
            while (doc[i] == ' ' || doc[i] == '\t' || doc[i] == '\r') {
                i++;
            }
            if (doc[i] == '*' && doc[i + 1] != '/') {
                while (doc[i] == '*' && doc[i + 1] != '/') {
                    i++;
                }
                if (doc[i] == ' ') {
                    i++;
                }
            }
            continue;
        }
        if (c != '\r') {
            out[w++] = c;
        }
        i++;
    }
    out[w] = '\0';
    *out_len = w;
    return out;
}

typedef struct {
    CBMExtractCtx *ctx;
    const CBMDefinition *def;
    const char *t; /* the cleaned doc text */
    size_t n;
} jv_doc_t;

/* Push the reference s[0, len) written on source line `line`: whitespace runs
 * become one blank, control characters go, and a reference longer than any
 * name is cut (on a character boundary) just past CBM_DOCLINK_JVM_REF_MAX,
 * which is how the resolver knows it was oversized. */
static void jv_push(jv_doc_t *d, const char *s, size_t len, uint32_t line, int syntax) {
    size_t cap = (size_t)CBM_DOCLINK_JVM_REF_MAX + JV_UTF8_TAIL_MAX + SKIP_ONE;
    size_t take = len < cap ? len : cap;
    char *raw = (char *)cbm_arena_alloc(d->ctx->arena, take + SKIP_ONE);
    if (!raw) {
        return;
    }
    size_t w = 0;
    bool space = false;
    for (size_t i = 0; i < len && w < take; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c <= ' ') {
            space = w > 0;
            continue;
        }
        if (space && w + SKIP_ONE < take) {
            raw[w++] = ' ';
        }
        space = false;
        raw[w++] = (char)c;
    }
    if (w == take && len > take) {
        /* cut: do not leave half a multi-byte character behind */
        while (w > 0 && ((unsigned char)raw[w - SKIP_ONE] & 0xC0) == 0x80) {
            w--;
        }
        if (w > 0 && (unsigned char)raw[w - SKIP_ONE] >= 0xC0) {
            w--;
        }
    }
    raw[w] = '\0';
    if (w == 0) {
        return;
    }
    CBMDocLink link = {
        .source_qn = d->def->qualified_name,
        .raw = raw,
        .line = line,
        .def_line = d->def->start_line,
        .syntax = (uint16_t)syntax,
    };
    cbm_doclinks_push(&d->ctx->result->doc_links, d->ctx->arena, link);
}

/* Index just past the group opened at s[i] by `open` (nesting groups of the
 * same kind), or n when it does not close. */
static size_t jv_group_end(const char *s, size_t n, size_t i, char open, char close) {
    int depth = 0;
    for (size_t k = i; k < n; k++) {
        if (s[k] == open) {
            depth++;
        } else if (s[k] == close) {
            depth--;
            if (depth == 0) {
                return k + SKIP_ONE;
            }
        }
    }
    return n;
}

static bool jv_word_is(const char *s, size_t len, const char *word) {
    return strlen(word) == len && memcmp(s, word, len) == 0;
}

/* ── Javadoc ─────────────────────────────────────────────────────── */

/* Length of the Javadoc reference at s: a (qualified) type, `#member`,
 * `module/`, type arguments, array brackets and a parameter list. A parameter
 * list that does not close takes the rest (the reference is malformed, and
 * the resolver says so). */
static size_t jv_java_ref_len(const char *s, size_t n) {
    size_t j = 0;
    while (j < n) {
        char c = s[j];
        if (jv_ident_char(c) || c == '.' || c == '#' || c == '/') {
            j++;
        } else if (c == '<') {
            size_t e = jv_group_end(s, n, j, '<', '>');
            if (e >= n && (n == 0 || s[n - SKIP_ONE] != '>')) {
                break;
            }
            j = e;
        } else if (c == '[' && j + SKIP_ONE < n && s[j + SKIP_ONE] == ']') {
            j += PAIR_LEN;
        } else {
            break;
        }
    }
    if (j < n && s[j] == '(') {
        j = jv_group_end(s, n, j, '(', ')');
    }
    return j;
}

/* The reference at s[0, n): what the doc tool reads as one reference. Text
 * glued to it (`Foo,`, `http://x`) makes the whole word the (malformed)
 * reference. Returns its length, 0 when there is none. */
static size_t jv_java_ref(const char *s, size_t n) {
    size_t r = jv_java_ref_len(s, n);
    if (r < n && !jv_blank(s[r])) {
        while (r < n && !jv_blank(s[r])) {
            r++;
        }
    }
    return r;
}

/* The href of an `<a href="...">` at s[0, n): pushed as a URL. */
static void jv_java_href(jv_doc_t *d, const char *s, size_t n, uint32_t line) {
    static const char key[] = "href";
    for (size_t i = 0; i + sizeof(key) <= n && s[i] != '>'; i++) {
        if (memcmp(s + i, key, sizeof(key) - SKIP_ONE) != 0) {
            continue;
        }
        size_t j = i + sizeof(key) - SKIP_ONE;
        while (j < n && (s[j] == ' ' || s[j] == '\t')) {
            j++;
        }
        if (j >= n || s[j] != '=') {
            continue;
        }
        j++;
        while (j < n && (s[j] == ' ' || s[j] == '\t')) {
            j++;
        }
        if (j >= n || (s[j] != '"' && s[j] != '\'')) {
            return;
        }
        char quote = s[j];
        size_t v = j + SKIP_ONE;
        size_t e = v;
        while (e < n && s[e] != quote) {
            e++;
        }
        if (e < n && e > v) {
            jv_push(d, s + v, e - v, line, CBM_DOCLINK_HREF);
        }
        return;
    }
}

/* `@see ...` / `@throws ...`: the rest of the tag's line starts at `i`. */
static void jv_java_block_tag(jv_doc_t *d, size_t i, uint32_t line, bool see) {
    const char *t = d->t;
    while (i < d->n && (t[i] == ' ' || t[i] == '\t')) {
        i++;
    }
    size_t eol = i;
    while (eol < d->n && t[eol] != '\n') {
        eol++;
    }
    if (i >= eol) {
        return;
    }
    if (see && t[i] == '<') {
        jv_java_href(d, t + i, eol - i, line);
        return;
    }
    if (t[i] == '"' || t[i] == '{') {
        return; /* a quoted title; an inline tag is found by the text scan */
    }
    size_t r = jv_java_ref(t + i, eol - i);
    if (r > 0) {
        jv_push(d, t + i, r, line, see ? CBM_DOCLINK_JAVA_SEE : CBM_DOCLINK_JAVA_THROWS);
    }
}

/* An inline tag `{@name body}` whose body is t[b, e) and starts on `line`:
 * its reference. */
static void jv_java_inline(jv_doc_t *d, size_t b, size_t e, uint32_t line, int syntax) {
    const char *t = d->t;
    for (; b < e && jv_blank(t[b]); b++) {
        line += t[b] == '\n';
    }
    if (syntax == CBM_DOCLINK_JAVA_VALUE && b < e && t[b] == '"') {
        /* {@value "format" ref}: the format string is not the reference */
        size_t q = b + SKIP_ONE;
        while (q < e && t[q] != '"') {
            line += t[q] == '\n';
            q++;
        }
        for (b = q < e ? q + SKIP_ONE : e; b < e && jv_blank(t[b]); b++) {
            line += t[b] == '\n';
        }
    }
    size_t r = jv_java_ref(t + b, e - b);
    if (r > 0) {
        jv_push(d, t + b, r, line, syntax);
    }
}

/* An open inline tag. javadoc ends it at the brace that balances its own
 * `{`; braces outside inline tags are plain text and balance nothing. */
typedef struct {
    int syntax;    /* a link family, JV_TAG_OTHER, or JV_TAG_TEXT */
    size_t body;   /* first byte after the tag's name */
    uint32_t line; /* the line of that byte */
    int depth;     /* plain braces open inside it */
} jv_tag_t;

enum {
    JV_TAG_OTHER = -1, /* another inline tag: its body is scanned */
    JV_TAG_TEXT = -2,  /* code, literal, snippet: the body is text, not markup */
    JV_TAG_NEST_MAX = 32,
};

/* The kind of the inline tag `{@name`; `next` is the character after the
 * name (a tag named `linkage` is not `link`). */
static int jv_java_tag_kind(const char *name, size_t len, char next) {
    if (jv_word_is(name, len, "code") || jv_word_is(name, len, "literal") ||
        jv_word_is(name, len, "snippet")) {
        return JV_TAG_TEXT;
    }
    if (!jv_blank(next) && next != '}') {
        return JV_TAG_OTHER;
    }
    if (jv_word_is(name, len, "link")) {
        return CBM_DOCLINK_JAVA_LINK;
    }
    if (jv_word_is(name, len, "linkplain")) {
        return CBM_DOCLINK_JAVA_LINKPLAIN;
    }
    if (jv_word_is(name, len, "value")) {
        return CBM_DOCLINK_JAVA_VALUE;
    }
    return JV_TAG_OTHER;
}

/* Past an HTML comment or a CDATA section that starts at t[i] (the doc tool
 * reads no markup in either); i itself when none starts there. Line breaks
 * passed are added to *line. One that never ends takes the rest. */
static size_t jv_skip_hidden(const jv_doc_t *d, size_t i, uint32_t *line) {
    static const struct {
        const char *open;
        const char *close;
    } kinds[] = {{"<!--", "-->"}, {"<![CDATA[", "]]>"}};
    for (size_t k = 0; k < sizeof(kinds) / sizeof(kinds[0]); k++) {
        size_t ol = strlen(kinds[k].open);
        if (d->n - i < ol || memcmp(d->t + i, kinds[k].open, ol) != 0) {
            continue;
        }
        const char *close = strstr(d->t + i + ol, kinds[k].close);
        size_t past = close ? (size_t)(close - d->t) + strlen(kinds[k].close) : d->n;
        for (size_t p = i; p < past; p++) {
            *line += d->t[p] == '\n';
        }
        return past;
    }
    return i;
}

void cbm_doclink_java_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                uint32_t doc_line) {
    /* A field is also emitted as a package-level Variable with the same doc:
     * the Field carries the references. A module declaration is no code
     * definition (its "class" is named like a package). */
    if (!def->label || (strcmp(def->label, "Variable") == 0 && def->parent_class) ||
        (strcmp(def->label, "Class") == 0 && strchr(def->name, '.'))) {
        return;
    }
    CBMArena *scratch = ctx->scratch ? ctx->scratch : ctx->arena;
    jv_doc_t d = {.ctx = ctx, .def = def};
    d.t = jv_clean_doc(scratch, doc, false, &d.n);
    if (!d.t) {
        return;
    }
    const char *t = d.t;
    size_t n = d.n;
    jv_tag_t tags[JV_TAG_NEST_MAX];
    int sp = 0; /* open inline tags */
    uint32_t line = doc_line;
    size_t i = 0;
    bool bol = true; /* only blanks since the line began */
    while (i < n) {
        char c = t[i];
        if (c == '\n') {
            line++;
            bol = true;
            i++;
            continue;
        }
        if (bol && (c == ' ' || c == '\t')) {
            i++;
            continue;
        }
        bool in_text = sp > 0 && tags[sp - SKIP_ONE].syntax == JV_TAG_TEXT;
        if (bol && !in_text && c == '@' && i + SKIP_ONE < n &&
            isalpha((unsigned char)t[i + SKIP_ONE])) {
            /* A block tag. javadoc ends an open {@link ..} (or any inline tag
             * whose content is markup) at it: that tag never closed and is no
             * link. Inside {@code}, {@literal} and {@snippet} an `@` at the
             * start of a line is text (annotations in sample code). */
            size_t e = i + SKIP_ONE;
            while (e < n && isalpha((unsigned char)t[e])) {
                e++;
            }
            const char *name = t + i + SKIP_ONE;
            size_t len = e - i - SKIP_ONE;
            bool gap = e >= n || jv_blank(t[e]);
            if (gap && jv_word_is(name, len, "see")) {
                jv_java_block_tag(&d, e, line, true);
            } else if (gap &&
                       (jv_word_is(name, len, "throws") || jv_word_is(name, len, "exception"))) {
                jv_java_block_tag(&d, e, line, false);
            }
            sp = 0;
            bol = false;
            i = e;
            continue;
        }
        bol = false;
        bool text = in_text;
        if (c == '<' && sp == 0) {
            size_t past = jv_skip_hidden(&d, i, &line);
            if (past > i) {
                i = past;
                continue;
            }
        }
        if (c == '{' && !text && i + SKIP_ONE < n && t[i + SKIP_ONE] == '@' &&
            sp < JV_TAG_NEST_MAX) {
            size_t ne = i + PAIR_LEN;
            while (ne < n && isalpha((unsigned char)t[ne])) {
                ne++;
            }
            tags[sp++] = (jv_tag_t){.syntax = jv_java_tag_kind(t + i + PAIR_LEN, ne - i - PAIR_LEN,
                                                               ne < n ? t[ne] : '}'),
                                    .body = ne,
                                    .line = line};
            i = ne;
            continue;
        }
        if (c == '{' && sp > 0) {
            tags[sp - SKIP_ONE].depth++;
        } else if (c == '}' && sp > 0) {
            if (tags[sp - SKIP_ONE].depth > 0) {
                tags[sp - SKIP_ONE].depth--;
            } else {
                jv_tag_t done = tags[--sp];
                if (done.syntax > CBM_DOCLINK_NONE) {
                    jv_java_inline(&d, done.body, i, done.line, done.syntax);
                }
            }
        }
        i++;
    }
}

/* ── KDoc ────────────────────────────────────────────────────────── */

/* Length of the KDoc link target at s[0, n): a qualified name whose segments
 * are identifiers or backticked names. 0 when s is not exactly one. */
static size_t jv_kdoc_target_len(const char *s, size_t n) {
    size_t i = 0;
    for (;;) {
        if (i < n && s[i] == '`') {
            size_t e = i + SKIP_ONE;
            while (e < n && s[e] != '`' && s[e] != '\n') {
                e++;
            }
            if (e >= n || s[e] != '`' || e == i + SKIP_ONE) {
                return 0;
            }
            i = e + SKIP_ONE;
        } else {
            if (i >= n || !jv_ident_start(s[i]) || s[i] == '$') {
                return 0;
            }
            while (i < n && jv_ident_char(s[i]) && s[i] != '$') {
                i++;
            }
        }
        if (i < n && s[i] == '.') {
            i++;
            continue;
        }
        return i == n ? i : 0;
    }
}

/* A `[...]` group opening at `i` on the line that ends at `eol`: the index of
 * its `]`, or eol when it holds a bracket or does not close on the line. */
static size_t jv_kdoc_bracket_close(const char *t, size_t i, size_t eol) {
    for (size_t k = i + SKIP_ONE; k < eol; k++) {
        if (t[k] == ']') {
            return k;
        }
        if (t[k] == '[') {
            return eol;
        }
    }
    return eol;
}

/* One line of KDoc text. */
typedef struct {
    size_t first;  /* its first byte that is no blank */
    size_t eol;    /* its end */
    uint32_t line; /* its source line */
} jv_kline_t;

/* `[Name]` / `[text][Name]` at `i`. Returns the index to continue from. */
static size_t jv_kdoc_link(jv_doc_t *d, size_t i, const jv_kline_t *ln) {
    const char *t = d->t;
    size_t eol = ln->eol;
    size_t close = jv_kdoc_bracket_close(t, i, eol);
    if (close >= eol || close == i + SKIP_ONE) {
        return i + SKIP_ONE;
    }
    size_t b = i + SKIP_ONE;
    size_t e = close;
    size_t next = close + SKIP_ONE;
    if (next < eol && t[next] == '(') {
        return next; /* [text](url): a Markdown link, not a code reference */
    }
    if (next < eol && t[next] == '[') {
        size_t close2 = jv_kdoc_bracket_close(t, next, eol);
        if (close2 >= eol) {
            return next;
        }
        if (close2 > next + SKIP_ONE) {
            b = next + SKIP_ONE; /* [text][Name] */
            e = close2;
        }
        next = close2 + SKIP_ONE;
    } else if (next < eol && t[next] == ':' && i == ln->first) {
        return next; /* [label]: url -- a link definition */
    }
    while (b < e && (t[b] == ' ' || t[b] == '\t')) {
        b++;
    }
    while (e > b && (t[e - SKIP_ONE] == ' ' || t[e - SKIP_ONE] == '\t')) {
        e--;
    }
    if (jv_kdoc_target_len(t + b, e - b) > 0) {
        jv_push(d, t + b, e - b, ln->line, CBM_DOCLINK_KT_KDOC_LINK);
    }
    return next;
}

/* The subject of `@see` / `@throws` / `@sample` starting at `i`: one word.
 * Returns the index to continue the line's text scan from. */
static size_t jv_kdoc_subject(jv_doc_t *d, size_t i, const jv_kline_t *ln, int syntax) {
    const char *t = d->t;
    size_t eol = ln->eol;
    while (i < eol && (t[i] == ' ' || t[i] == '\t')) {
        i++;
    }
    size_t e = i;
    while (e < eol && t[e] != ' ' && t[e] != '\t') {
        e++;
    }
    if (e == i) {
        return i;
    }
    size_t b = i;
    size_t w = e;
    if (t[b] == '[') {
        size_t close = jv_kdoc_bracket_close(t, b, eol);
        if (close >= eol) {
            return e;
        }
        b++;
        w = close;
        e = close + SKIP_ONE;
    } else {
        while (w > b && strchr(".,;:", t[w - SKIP_ONE])) {
            w--;
        }
    }
    bool url =
        t[b] == '<' || (w - b > 4 && memcmp(t + b, "http", 4) == 0 && memchr(t + b, ':', w - b));
    if (w > b && !url) {
        jv_push(d, t + b, w - b, ln->line, syntax);
    }
    return e;
}

static int jv_kdoc_tag_syntax(const char *name, size_t len) {
    if (jv_word_is(name, len, "see")) {
        return CBM_DOCLINK_KT_SEE;
    }
    if (jv_word_is(name, len, "throws") || jv_word_is(name, len, "exception")) {
        return CBM_DOCLINK_KT_THROWS;
    }
    if (jv_word_is(name, len, "sample")) {
        return CBM_DOCLINK_KT_SAMPLE;
    }
    return CBM_DOCLINK_NONE;
}

/* Just past the code span that the run of `len` backticks ending before `run`
 * opens: the next run of exactly that many. 0 when the line has none (the
 * backticks are then text). */
static size_t jv_kdoc_span_end(const char *t, size_t run, size_t len, size_t eol) {
    size_t k = run;
    while (k < eol) {
        if (t[k] != '`') {
            k++;
            continue;
        }
        size_t r = k;
        while (r < eol && t[r] == '`') {
            r++;
        }
        if (r - k == len) {
            return r;
        }
        k = r;
    }
    return 0;
}

/* The text of one KDoc line from `i`: code spans and HTML comments are
 * skipped, bracket links pushed. *in_comment: an HTML comment is open (it may
 * have begun on an earlier line). */
static void jv_kdoc_text(jv_doc_t *d, size_t i, const jv_kline_t *ln, bool *in_comment) {
    const char *t = d->t;
    size_t eol = ln->eol;
    /* A run of n backticks was looked for once and the rest of the line has no
     * second one: a later run of n is text as well. Longer runs than a fence
     * are not looked for (they would be searched once per length). */
    bool no_span[JV_QUOTES + SKIP_ONE] = {false};
    while (i < eol) {
        char c = t[i];
        if (*in_comment) {
            if (c == '-' && eol - i >= 3 && t[i + 1] == '-' && t[i + 2] == '>') {
                *in_comment = false;
                i += 3;
            } else {
                i++;
            }
        } else if (c == '\\') {
            i += PAIR_LEN;
        } else if (c == '`') {
            size_t run = i;
            while (run < eol && t[run] == '`') {
                run++;
            }
            size_t len = run - i;
            size_t past =
                (len > JV_QUOTES || no_span[len]) ? 0 : jv_kdoc_span_end(t, run, len, eol);
            if (past == 0 && len <= JV_QUOTES) {
                no_span[len] = true;
            }
            i = past ? past : run;
        } else if (c == '<' && eol - i >= 4 && memcmp(t + i, "<!--", 4) == 0) {
            *in_comment = true;
            i += 4;
        } else if (c == '!' && i + SKIP_ONE < eol && t[i + SKIP_ONE] == '[') {
            /* an image: `![alt](url)`, `![alt][label]` -- its label names no code */
            size_t close = jv_kdoc_bracket_close(t, i + SKIP_ONE, eol);
            i = close < eol ? close + SKIP_ONE : i + PAIR_LEN;
            if (close < eol && i < eol && t[i] == '[') {
                size_t label = jv_kdoc_bracket_close(t, i, eol);
                i = label < eol ? label + SKIP_ONE : i + SKIP_ONE;
            }
        } else if (c == '[') {
            i = jv_kdoc_link(d, i, ln);
        } else {
            i++;
        }
    }
}

/* The width of the code fence (``` or ~~~) a line opens or closes, 0 when it
 * does neither. */
static size_t jv_kdoc_fence(const char *s, size_t n, char *mark) {
    if (n < JV_QUOTES || (s[0] != '`' && s[0] != '~')) {
        return 0;
    }
    size_t k = 0;
    while (k < n && s[k] == s[0]) {
        k++;
    }
    if (k < JV_QUOTES) {
        return 0;
    }
    *mark = s[0];
    return k;
}

enum { JV_KDOC_CODE_INDENT = 4 };

void cbm_doclink_kotlin_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                  uint32_t doc_line) {
    CBMArena *scratch = ctx->scratch ? ctx->scratch : ctx->arena;
    jv_doc_t d = {.ctx = ctx, .def = def};
    d.t = jv_clean_doc(scratch, doc, true, &d.n);
    if (!d.t) {
        return;
    }
    const char *t = d.t;
    size_t n = d.n;
    size_t fence = 0; /* width of the open code fence */
    char fence_mark = 0;
    bool after_blank = true; /* the comment's start counts as a blank line */
    bool in_code = false;    /* in an indented code block */
    bool in_comment = false; /* in an HTML comment */
    uint32_t line = doc_line;
    for (size_t ls = 0; ls < n; line++) {
        size_t eol = ls;
        while (eol < n && t[eol] != '\n') {
            eol++;
        }
        size_t i = ls;
        size_t indent = 0;
        while (i < eol && (t[i] == ' ' || t[i] == '\t')) {
            indent += t[i] == '\t' ? JV_KDOC_CODE_INDENT : SKIP_ONE;
            i++;
        }
        jv_kline_t ln = {.first = i, .eol = eol, .line = line};
        char mark = 0;
        size_t f = in_comment ? 0 : jv_kdoc_fence(t + i, eol - i, &mark);
        if (in_comment) {
            jv_kdoc_text(&d, i, &ln, &in_comment); /* up to its end, then text */
        } else if (fence > 0) {
            if (f >= fence && mark == fence_mark) {
                fence = 0;
            }
        } else if (f > 0) {
            fence = f;
            fence_mark = mark;
            in_code = false;
            after_blank = false;
        } else if (i >= eol) {
            after_blank = true; /* an indented block continues over blank lines */
        } else if (indent >= JV_KDOC_CODE_INDENT && (after_blank || in_code)) {
            in_code = true;
            after_blank = false;
        } else {
            in_code = false;
            after_blank = false;
            if (t[i] == '@') {
                size_t e = i + SKIP_ONE;
                while (e < eol && isalpha((unsigned char)t[e])) {
                    e++;
                }
                const char *name = t + i + SKIP_ONE;
                size_t len = e - i - SKIP_ONE;
                int syntax = jv_kdoc_tag_syntax(name, len);
                bool gap = e >= eol || t[e] == ' ' || t[e] == '\t';
                i = e;
                if (syntax != CBM_DOCLINK_NONE && gap) {
                    i = jv_kdoc_subject(&d, e, &ln, syntax);
                } else if (jv_word_is(name, len, "param") || jv_word_is(name, len, "property")) {
                    /* `@param name`, `@param [name]`, `@param[name]`: the
                     * definition's own parameter */
                    while (i < eol && (t[i] == ' ' || t[i] == '\t')) {
                        i++;
                    }
                    if (i < eol && t[i] == '[') {
                        size_t close = jv_kdoc_bracket_close(t, i, eol);
                        i = close < eol ? close + SKIP_ONE : i + SKIP_ONE;
                    }
                }
            }
            jv_kdoc_text(&d, i, &ln, &in_comment);
        }
        ls = eol + SKIP_ONE;
    }
}

/* ── Parameter type normalization (Java) ─────────────────────────── */

/* Remove [start, end) from the NUL-terminated s of length *len. */
static void jv_cut(char *s, size_t *len, size_t start, size_t end) {
    memmove(s + start, s + end, *len - end + SKIP_ONE);
    *len -= end - start;
}

/* Annotations (`@NonNull`, `@Size(max = 3)`) and every type-argument group. */
static void jv_drop_annotations_and_args(char *s, size_t *len) {
    for (size_t i = 0; i < *len;) {
        if (s[i] == '@') {
            size_t e = i + SKIP_ONE;
            while (e < *len && (jv_ident_char(s[e]) || s[e] == '.')) {
                e++;
            }
            size_t g = e;
            while (g < *len && jv_blank(s[g])) {
                g++;
            }
            if (g < *len && s[g] == '(') {
                e = jv_group_end(s, *len, g, '(', ')');
            }
            jv_cut(s, len, i, e);
        } else if (s[i] == '<') {
            jv_cut(s, len, i, jv_group_end(s, *len, i, '<', '>'));
        } else {
            i++;
        }
    }
}

size_t cbm_doclink_java_norm_type(const char *in, size_t len, char *out, size_t cap) {
    if (!out || cap == 0) {
        return 0;
    }
    out[0] = '\0';
    char work[JV_TYPE_MAX];
    if (!in || len == 0 || len >= sizeof(work)) {
        snprintf(out, cap, "?");
        return strlen(out);
    }
    memcpy(work, in, len);
    work[len] = '\0';
    size_t n = len;
    jv_drop_annotations_and_args(work, &n);
    /* "..." -> "[]" (a variable-arity parameter is an array) */
    for (size_t i = 0; i + PAIR_LEN < n; i++) {
        if (work[i] == '.' && work[i + 1] == '.' && work[i + 2] == '.') {
            work[i] = '[';
            work[i + 1] = ']';
            jv_cut(work, &n, i + 2, i + 3);
        }
    }
    /* words: [final] Type [name]; brackets written after the name belong to
     * the type (`int x[]`) */
    char base[JV_TYPE_MAX];
    size_t bl = 0;
    size_t dims = 0;
    int words = 0;
    for (size_t i = 0; i < n;) {
        while (i < n && jv_blank(work[i])) {
            i++;
        }
        size_t b = i;
        while (i < n && !jv_blank(work[i])) {
            i++;
        }
        if (i == b) {
            break;
        }
        if (i - b == 5 && memcmp(work + b, "final", 5) == 0) {
            continue;
        }
        if (words == 0) {
            memcpy(base, work + b, i - b);
            bl = i - b;
        } else if (work[b] == '[') {
            memcpy(base + bl, work + b, i - b); /* `int []`: brackets apart from the type */
            bl += i - b;
        } else {
            for (size_t k = b; k < i; k++) {
                dims += work[k] == '['; /* the parameter name, with its own brackets */
            }
        }
        words++;
    }
    base[bl] = '\0';
    /* the simple name: package and outer-type qualifiers go */
    size_t suffix = bl;
    while (suffix > 0 && (base[suffix - SKIP_ONE] == ']' || base[suffix - SKIP_ONE] == '[')) {
        suffix--;
    }
    size_t start = 0;
    for (size_t i = 0; i < suffix; i++) {
        if (base[i] == '.') {
            start = i + SKIP_ONE;
        }
    }
    /* no name, or one that does not fit: no type, never a truncated one */
    if (suffix <= start || (bl - start) + (dims * PAIR_LEN) + SKIP_ONE > cap) {
        snprintf(out, cap, "?");
        return strlen(out);
    }
    size_t w = 0;
    for (size_t i = start; i < bl; i++) {
        out[w++] = base[i];
    }
    for (size_t k = 0; k < dims; k++) {
        out[w++] = '[';
        out[w++] = ']';
    }
    out[w] = '\0';
    return w;
}

/* ── Scope scan: state ───────────────────────────────────────────── */

/* A definition the extractor made for this file. */
typedef struct {
    const CBMDefinition *def;
    const char *tail; /* its qualified name below the file's module */
    int order;        /* position among the file's definitions */
    bool winner;      /* the graph keeps this one for its qualified name */
} jv_def_t;

/* A structural brace of a file with parse errors. */
typedef struct {
    uint32_t pos;
    uint32_t row; /* 0-based */
    int match;    /* index of the paired brace, CBM_NOT_FOUND when unpaired */
    int depth;    /* nesting depth just after this brace */
    char kind;    /* '{' or '}' */
} jv_brace_t;

/* A byte range: the name a declaration keyword is followed by, or a member
 * whose inside is known to be its own. */
typedef struct {
    uint32_t start;
    uint32_t end;
} jv_range_t;

typedef struct {
    jv_range_t *items;
    int count;
    int cap;
} jv_ranges_t;

/* The container whose members are being read. */
typedef struct {
    int owner;        /* index of its T record; JV_NO_OWNER at file level */
    const char *path; /* "" at file level */
    const char *name; /* its simple name ("" at file level) */
    int inner_depth;  /* brace depth of its members (files with parse errors) */
} jv_frame_t;

enum { JV_NO_OWNER = -1, JV_LEX_NEST_MAX = 64 };

typedef struct {
    CBMExtractCtx *ctx;
    CBMArena *tmp; /* names, lists, braces: nothing of it outlives the scan */
    jv_sb_t sb;
    const char *src;
    uint32_t src_len;
    bool kotlin;
    const char *module_qn;
    size_t module_len;
    jv_def_t *defs; /* sorted by (start line, order) */
    int ndefs;
    int ntypes;   /* T records written so far */
    bool lexical; /* the tree has parse errors: nesting is checked against the braces */
    jv_brace_t *braces;
    int nbraces;
    int cap_braces;
    int *stack; /* brace pairing: the open braces */
    int sp;
    int cap_stack;
    jv_ranges_t heads;   /* names that follow a type keyword in the text */
    jv_ranges_t seen;    /* names of the type declarations the tree has */
    jv_ranges_t members; /* members whose inside is known to be their own */
    uint32_t untrusted;  /* byte offset from which the braces do not pair up */
    uint32_t row_pos;    /* row cursor: the row of byte row_pos is `row` */
    uint32_t row;
    bool failed;
} jv_scan_t;

static void *jv_tmp_alloc(jv_scan_t *s, size_t n) {
    void *p = cbm_arena_alloc(s->tmp, n ? n : SKIP_ONE);
    if (!p) {
        s->failed = true;
    }
    return p;
}

static void jv_ranges_push(jv_scan_t *s, jv_ranges_t *r, uint32_t start, uint32_t end) {
    if (s->failed) {
        return;
    }
    if (r->count >= r->cap) {
        int ncap = r->cap ? r->cap * PAIR_LEN : JV_ITEMS_INIT;
        jv_range_t *grown = (jv_range_t *)jv_tmp_alloc(s, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return;
        }
        if (r->count > 0) {
            memcpy(grown, r->items, (size_t)r->count * sizeof(*grown));
        }
        r->items = grown;
        r->cap = ncap;
    }
    r->items[r->count++] = (jv_range_t){.start = start, .end = end};
}

static bool jv_kind(TSNode n, const char *kind) {
    return strcmp(ts_node_type(n), kind) == 0;
}

static TSNode jv_field(TSNode n, const char *field) {
    return ts_node_child_by_field_name(n, field, (uint32_t)strlen(field));
}

/* The first named child of that kind; a null node when there is none. */
static TSNode jv_child(TSNode n, const char *kind) {
    uint32_t nc = ts_node_named_child_count(n);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(n, i);
        if (jv_kind(c, kind)) {
            return c;
        }
    }
    return (TSNode){0};
}

static uint32_t jv_line(TSNode n) {
    return ts_node_start_point(n).row + TS_LINE_OFFSET;
}

static uint32_t jv_end_line(TSNode n) {
    return ts_node_end_point(n).row + TS_LINE_OFFSET;
}

/* ── Definitions of the file ─────────────────────────────────────── */

static int jv_def_qn_cmp(const void *a, const void *b) {
    const jv_def_t *x = (const jv_def_t *)a;
    const jv_def_t *y = (const jv_def_t *)b;
    int c = strcmp(x->tail, y->tail);
    if (c == 0 && x->def->start_line != y->def->start_line) {
        c = x->def->start_line < y->def->start_line ? -1 : 1;
    }
    if (c == 0) {
        c = strcmp(x->def->name, y->def->name);
    }
    if (c == 0) {
        c = strcmp(x->def->label, y->def->label);
    }
    if (c == 0) {
        c = (x->order > y->order) - (x->order < y->order);
    }
    return c;
}

static int jv_def_line_cmp(const void *a, const void *b) {
    const jv_def_t *x = (const jv_def_t *)a;
    const jv_def_t *y = (const jv_def_t *)b;
    if (x->def->start_line != y->def->start_line) {
        return x->def->start_line < y->def->start_line ? -1 : 1;
    }
    int c = strcmp(x->def->name, y->def->name);
    if (c != 0) {
        return c;
    }
    return (x->order > y->order) - (x->order < y->order);
}

/* The file's definitions below its module, and which of them the graph keeps
 * when several share one qualified name: the graph buffer's rule inside one
 * file is the largest start line, then name, then label, then the later
 * arrival (cbm_gbuf_upsert_node). */
static void jv_defs_index(jv_scan_t *s) {
    const CBMDefArray *defs = &s->ctx->result->defs;
    if (defs->count <= 0) {
        return;
    }
    s->defs = (jv_def_t *)jv_tmp_alloc(s, (size_t)defs->count * sizeof(jv_def_t));
    if (!s->defs) {
        return;
    }
    int n = 0;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (!d->name || !d->qualified_name || !d->label ||
            strncmp(d->qualified_name, s->module_qn, s->module_len) != 0 ||
            d->qualified_name[s->module_len] != '.') {
            continue;
        }
        s->defs[n++] =
            (jv_def_t){.def = d, .tail = d->qualified_name + s->module_len + SKIP_ONE, .order = i};
    }
    s->ndefs = n;
    if (n == 0) {
        return;
    }
    qsort(s->defs, (size_t)n, sizeof(jv_def_t), jv_def_qn_cmp);
    for (int i = 0; i < n; i++) {
        s->defs[i].winner =
            i + SKIP_ONE >= n || strcmp(s->defs[i + SKIP_ONE].tail, s->defs[i].tail) != 0;
    }
    qsort(s->defs, (size_t)n, sizeof(jv_def_t), jv_def_line_cmp);
}

static bool jv_label_fits(const char *label, char kind) {
    if (kind == 'T') {
        return cbm_label_is_type_like(label);
    }
    if (kind == 'c') {
        return strcmp(label, "Method") == 0 || strcmp(label, "Function") == 0;
    }
    return strcmp(label, "Field") == 0 || strcmp(label, "Variable") == 0;
}

/* The definition of kind `kind` (T type, c callable, v value) on `line` that
 * is named key[0, len) -- or, with `prefix`, whose name begins with it. A
 * Field is preferred to the Variable twin the extractor emits beside it. The
 * definitions are sorted by (line, name): the candidates are one run. */
static const jv_def_t *jv_def_pick(const jv_scan_t *s, uint32_t line, const char *key, size_t len,
                                   char kind, bool prefix) {
    int lo = 0;
    int hi = s->ndefs;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        const CBMDefinition *d = s->defs[mid].def;
        bool below = d->start_line != line ? d->start_line < line : strncmp(d->name, key, len) < 0;
        if (below) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    const jv_def_t *best = NULL;
    for (int i = lo; i < s->ndefs; i++) {
        const CBMDefinition *d = s->defs[i].def;
        if (d->start_line != line || strncmp(d->name, key, len) != 0 ||
            (!prefix && d->name[len] != '\0')) {
            break;
        }
        if (!jv_label_fits(d->label, kind)) {
            continue;
        }
        if (strcmp(d->label, "Field") == 0) {
            return &s->defs[i];
        }
        if (!best) {
            best = &s->defs[i];
        }
    }
    return best;
}

/* The definition the extractor made for the declaration that starts on
 * `line` and is named name[0, len). A Java field's definition is named after
 * its whole declarator (`MAX = 10`, `MAX=10`). */
static const jv_def_t *jv_def_match(const jv_scan_t *s, uint32_t line, const char *name, size_t len,
                                    char kind) {
    const jv_def_t *exact = jv_def_pick(s, line, name, len, kind, false);
    if (kind != 'v' || s->kotlin || len > JV_NAME_MAX ||
        (exact && strcmp(exact->def->label, "Field") == 0)) {
        return exact;
    }
    /* the Field of an initialized declarator, before its Variable twin */
    char key[JV_NAME_MAX + PAIR_LEN];
    memcpy(key, name, len);
    key[len] = ' ';
    const jv_def_t *field = jv_def_pick(s, line, key, len + SKIP_ONE, kind, true);
    if (!field) {
        key[len] = '=';
        field = jv_def_pick(s, line, key, len + SKIP_ONE, kind, true);
    }
    return field ? field : exact;
}

/* ── Braces: the block structure of a file with parse errors ─────────
 *
 * Error recovery closes blocks early and late and leaves declarations in
 * error nodes, which moves what follows into the wrong type or out of it. In
 * the Kotlin dev corpus 55 of 837 files have a parse error (the central ones
 * among them: Table.kt, ColumnType.kt), nearly always inside one function
 * body, where it disturbs nothing outside. So a file whose tree has errors is
 * not given up: its declarations are checked against the braces of the text.
 * A declaration counts when the braces of its body pair up in the text and
 * its own depth is that of its container's members; a type with an error
 * among its members is marked incomplete; a type the tree has no declaration
 * for (its keyword and name stand in the text) is quarantined by name; what
 * lies behind the first brace without a partner is not placed at all.
 * The parser's own tokens cannot serve (it lexes code as string content once
 * it loses the thread), so the text is scanned here with the lexical grammar
 * a brace scanner needs: comments, character and string literals, Java text
 * blocks, Kotlin raw strings and template holes. */

static uint32_t jv_row_of(jv_scan_t *s, uint32_t pos) {
    if (pos < s->row_pos) {
        s->row_pos = 0;
        s->row = 0;
    }
    for (uint32_t i = s->row_pos; i < pos; i++) {
        s->row += s->src[i] == '\n';
    }
    s->row_pos = pos;
    return s->row;
}

static void jv_mark_untrusted(jv_scan_t *s, uint32_t pos) {
    if (pos < s->untrusted) {
        s->untrusted = pos;
    }
}

static void jv_lex_brace(jv_scan_t *s, uint32_t pos, bool open) {
    if (s->failed) {
        return;
    }
    if (s->nbraces >= s->cap_braces) {
        int ncap = s->cap_braces ? s->cap_braces * PAIR_LEN : JV_BRACES_INIT;
        jv_brace_t *grown = (jv_brace_t *)jv_tmp_alloc(s, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return;
        }
        if (s->nbraces > 0) {
            memcpy(grown, s->braces, (size_t)s->nbraces * sizeof(*grown));
        }
        s->braces = grown;
        s->cap_braces = ncap;
    }
    int b = s->nbraces++;
    s->braces[b] = (jv_brace_t){
        .pos = pos, .row = jv_row_of(s, pos), .match = CBM_NOT_FOUND, .kind = open ? '{' : '}'};
    if (open) {
        if (s->sp >= s->cap_stack) {
            int ncap = s->cap_stack ? s->cap_stack * PAIR_LEN : JV_ITEMS_INIT;
            int *grown = (int *)jv_tmp_alloc(s, (size_t)ncap * sizeof(int));
            if (!grown) {
                return;
            }
            if (s->sp > 0) {
                memcpy(grown, s->stack, (size_t)s->sp * sizeof(int));
            }
            s->stack = grown;
            s->cap_stack = ncap;
        }
        s->stack[s->sp++] = b;
    } else if (s->sp == 0) {
        jv_mark_untrusted(s, pos); /* closes nothing */
    } else {
        int o = s->stack[--s->sp];
        s->braces[o].match = b;
        s->braces[b].match = o;
    }
    s->braces[b].depth = s->sp;
}

/* Past a comment at src[i], or i when there is none. Kotlin block comments
 * nest. */
static uint32_t jv_lex_comment(const jv_scan_t *s, uint32_t i) {
    const char *src = s->src;
    uint32_t n = s->src_len;
    if (src[i] != '/' || i + SKIP_ONE >= n) {
        return i;
    }
    if (src[i + SKIP_ONE] == '/') {
        while (i < n && src[i] != '\n') {
            i++;
        }
        return i;
    }
    if (src[i + SKIP_ONE] != '*') {
        return i;
    }
    int depth = 1;
    i += PAIR_LEN;
    while (i + SKIP_ONE < n && depth > 0) {
        if (src[i] == '*' && src[i + SKIP_ONE] == '/') {
            depth--;
            i += PAIR_LEN;
        } else if (s->kotlin && src[i] == '/' && src[i + SKIP_ONE] == '*') {
            depth++;
            i += PAIR_LEN;
        } else {
            i++;
        }
    }
    return depth > 0 ? n : i;
}

/* 'x', '\n', a quoted unicode escape: past the literal, or past the lone
 * quote when it is not one. */
static uint32_t jv_lex_char(const jv_scan_t *s, uint32_t i) {
    const char *src = s->src;
    uint32_t n = s->src_len;
    uint32_t j = i + SKIP_ONE;
    if (j < n && src[j] == '\\') {
        j += PAIR_LEN;
    }
    while (j < n && src[j] != '\'' && src[j] != '\n' && j - i < JV_CHAR_LITERAL_MAX) {
        j++;
    }
    return (j < n && src[j] == '\'') ? j + SKIP_ONE : i + SKIP_ONE;
}

/* A Java string or text block at the quote src[i]: past it. An unterminated
 * string ends with its line. */
static uint32_t jv_lex_java_string(const jv_scan_t *s, uint32_t i) {
    const char *src = s->src;
    uint32_t n = s->src_len;
    bool block = i + PAIR_LEN < n && src[i + SKIP_ONE] == '"' && src[i + PAIR_LEN] == '"';
    uint32_t j = i + (block ? JV_QUOTES : SKIP_ONE);
    while (j < n) {
        char c = src[j];
        if (c == '\\') {
            j += PAIR_LEN;
        } else if (block && c == '"' && j + PAIR_LEN < n && src[j + SKIP_ONE] == '"' &&
                   src[j + PAIR_LEN] == '"') {
            return j + JV_QUOTES;
        } else if (!block && c == '"') {
            return j + SKIP_ONE;
        } else if (!block && c == '\n') {
            return j;
        } else {
            j++;
        }
    }
    return n;
}

/* The declaration keywords a type starts with. */
static bool jv_type_keyword(const jv_scan_t *s, const char *w, uint32_t len) {
    static const char *const java[] = {"class", "interface", "enum", "record", NULL};
    static const char *const kotlin[] = {"class", "interface", "object", "typealias", NULL};
    const char *const *words = s->kotlin ? kotlin : java;
    for (int i = 0; words[i]; i++) {
        if (strlen(words[i]) == len && memcmp(words[i], w, len) == 0) {
            return true;
        }
    }
    return false;
}

/* A word at src[i]: when it is a type keyword followed by a name, the name is
 * recorded (the tree may have no declaration for it). Returns the end of the
 * word. */
static uint32_t jv_lex_word(jv_scan_t *s, uint32_t i, char prev, bool record) {
    const char *src = s->src;
    uint32_t n = s->src_len;
    uint32_t e = i;
    while (e < n && jv_ident_char(src[e])) {
        e++;
    }
    if (!record || prev == '.' || prev == ':' || !jv_type_keyword(s, src + i, e - i)) {
        return e;
    }
    uint32_t a = e;
    while (a < n && (src[a] == ' ' || src[a] == '\t')) {
        a++;
    }
    if (a == e || a >= n) {
        return e;
    }
    uint32_t b = a;
    if (s->kotlin && src[a] == '`') {
        b = a + SKIP_ONE;
        while (b < n && src[b] != '`' && src[b] != '\n') {
            b++;
        }
        b = (b < n && src[b] == '`') ? b + SKIP_ONE : a;
    } else if (jv_ident_start(src[a])) {
        while (b < n && jv_ident_char(src[b])) {
            b++;
        }
    }
    if (b > a) {
        jv_ranges_push(s, &s->heads, a, b);
    }
    return e;
}

enum { JV_MODE_CODE = 0, JV_MODE_HOLE, JV_MODE_STRING, JV_MODE_RAW };

/* Scan the braces and the type keywords of the text and pair the braces. The
 * first brace without a partner starts the untrusted part of the file. Kotlin
 * strings hold code (`${...}`): the modes nest on an explicit stack, and a
 * text nested deeper than any program is not trusted from there on. */
static void jv_scan_text(jv_scan_t *s) {
    const char *src = s->src;
    uint32_t n = s->src_len;
    struct {
        int mode;
        int depth; /* a hole's own open braces */
    } modes[JV_LEX_NEST_MAX];
    int mp = 0;
    modes[mp].mode = JV_MODE_CODE;
    modes[mp].depth = 0;
    char prev = 0; /* the last significant character of code */
    uint32_t i = 0;
    while (i < n && !s->failed) {
        char c = src[i];
        int mode = modes[mp].mode;
        bool hole = c == '$' && i + SKIP_ONE < n && src[i + SKIP_ONE] == '{';
        if (mode == JV_MODE_STRING || mode == JV_MODE_RAW) {
            bool raw = mode == JV_MODE_RAW;
            if (hole) {
                if (mp + SKIP_ONE >= JV_LEX_NEST_MAX) {
                    jv_mark_untrusted(s, i);
                    return;
                }
                mp++;
                modes[mp].mode = JV_MODE_HOLE;
                modes[mp].depth = 0;
                i += PAIR_LEN;
            } else if (!raw && c == '\\') {
                i += PAIR_LEN;
            } else if (!raw && (c == '"' || c == '\n')) {
                mp--;
                i += c == '"';
            } else if (raw && c == '"' && i + PAIR_LEN < n && src[i + SKIP_ONE] == '"' &&
                       src[i + PAIR_LEN] == '"') {
                while (i < n && src[i] == '"') {
                    i++;
                }
                mp--;
            } else {
                i++;
            }
            continue;
        }
        if (jv_blank(c)) {
            i++;
            continue;
        }
        uint32_t e = jv_lex_comment(s, i);
        if (e > i) {
            i = e;
            continue;
        }
        if (c == '"' && !s->kotlin) {
            i = jv_lex_java_string(s, i);
        } else if (c == '"') {
            bool raw = i + PAIR_LEN < n && src[i + SKIP_ONE] == '"' && src[i + PAIR_LEN] == '"';
            if (mp + SKIP_ONE >= JV_LEX_NEST_MAX) {
                jv_mark_untrusted(s, i);
                return;
            }
            mp++;
            modes[mp].mode = raw ? JV_MODE_RAW : JV_MODE_STRING;
            modes[mp].depth = 0;
            i += raw ? JV_QUOTES : SKIP_ONE;
        } else if (c == '\'') {
            i = jv_lex_char(s, i);
        } else if (c == '`' && s->kotlin) {
            uint32_t b = i + SKIP_ONE;
            while (b < n && src[b] != '`' && src[b] != '\n') {
                b++;
            }
            i = (b < n && src[b] == '`') ? b + SKIP_ONE : i + SKIP_ONE;
        } else if (c == '{' || c == '}') {
            if (mode == JV_MODE_HOLE) {
                if (c == '{') {
                    modes[mp].depth++;
                } else if (modes[mp].depth == 0) {
                    mp--; /* the hole ends: back in its string */
                } else {
                    modes[mp].depth--;
                }
            } else {
                jv_lex_brace(s, i, c == '{');
            }
            i++;
        } else if (jv_ident_start(c)) {
            i = jv_lex_word(s, i, prev, mode == JV_MODE_CODE);
        } else {
            i++;
        }
        prev = c;
    }
    if (s->sp > 0) {
        jv_mark_untrusted(s, s->braces[s->stack[0]].pos);
    }
}

/* Index of the first brace at or after byte `pos`. */
static int jv_brace_lower(const jv_scan_t *s, uint32_t pos) {
    int lo = 0;
    int hi = s->nbraces;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (s->braces[mid].pos < pos) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* Brace nesting depth at byte `pos`. */
static int jv_depth_at(const jv_scan_t *s, uint32_t pos) {
    int i = jv_brace_lower(s, pos);
    return i > 0 ? s->braces[i - SKIP_ONE].depth : 0;
}

/* True when the block the tree gives `body` is a block of the text: it opens
 * with a brace whose partner is its last byte. */
static bool jv_block_pairs(const jv_scan_t *s, TSNode body) {
    uint32_t a = ts_node_start_byte(body);
    uint32_t b = ts_node_end_byte(body);
    int i = jv_brace_lower(s, a);
    if (i >= s->nbraces || s->braces[i].pos != a || s->braces[i].kind != '{' ||
        s->braces[i].match < 0) {
        return false;
    }
    return b > 0 && s->braces[s->braces[i].match].pos == b - SKIP_ONE;
}

/* True when a parse error sits among the parts of `decl` other than `body`:
 * an error or missing node there, or (`deep`) an error anywhere inside one. */
static bool jv_header_broken(TSNode decl, TSNode body, bool deep) {
    bool broken = false;
    TSTreeCursor cur = ts_tree_cursor_new(decl);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode ch = ts_tree_cursor_current_node(&cur);
            if (!ts_node_is_null(body) && ts_node_eq(ch, body)) {
                continue;
            }
            broken =
                ts_node_is_error(ch) || ts_node_is_missing(ch) || (deep && ts_node_has_error(ch));
        } while (!broken && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    return broken;
}

/* True when the tree's placement of a type declaration holds: no parse error
 * in its header, its depth is that of `depth` (its container's members), and
 * its body is a block of the text. Always true in a file without errors. */
static bool jv_type_placed(const jv_scan_t *s, TSNode decl, TSNode body, int depth) {
    if (!s->lexical) {
        return true;
    }
    uint32_t start = ts_node_start_byte(decl);
    if (start >= s->untrusted || jv_depth_at(s, start) != depth ||
        jv_header_broken(decl, body, true)) {
        return false;
    }
    return ts_node_is_null(body) || jv_block_pairs(s, body);
}

/* The same for a member: an error node among its own parts hides what it is;
 * a callable must have parsed up to its body, and a body in braces must be a
 * block of the text (what happens inside it is then its own affair). */
static bool jv_member_ok(const jv_scan_t *s, TSNode decl, TSNode body, bool callable, int depth) {
    if (!s->lexical) {
        return true;
    }
    uint32_t start = ts_node_start_byte(decl);
    if (start >= s->untrusted || jv_depth_at(s, start) != depth ||
        jv_header_broken(decl, body, callable)) {
        return false;
    }
    bool block = !ts_node_is_null(body) && s->src[ts_node_start_byte(body)] == '{';
    return !callable || !block || jv_block_pairs(s, body);
}

/* True when every parse error inside `decl` is held by braces of the text
 * that open and close inside it: an accessor's body, a `when`, a lambda, an
 * anonymous class. The declaration then ends where the tree says, whatever
 * went wrong within those braces. Needs the brace table (a file with
 * errors). */
static bool jv_errors_contained(const jv_scan_t *s, TSNode decl) {
    uint32_t a = ts_node_start_byte(decl);
    uint32_t b = ts_node_end_byte(decl);
    int base = jv_depth_at(s, a);
    int first = jv_brace_lower(s, a);
    int end = jv_brace_lower(s, b);
    for (int i = first; i < end; i++) {
        int m = s->braces[i].match;
        if (m < first || m >= end) {
            return false; /* a brace whose partner is outside the declaration */
        }
    }
    bool contained = true;
    bool down = true;
    TSTreeCursor cur = ts_tree_cursor_new(decl);
    while (contained) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        bool error = ts_node_is_error(n) || ts_node_is_missing(n);
        if (down && error) {
            contained = jv_depth_at(s, ts_node_start_byte(n)) > base &&
                        jv_depth_at(s, ts_node_end_byte(n)) > base;
        }
        /* an error node is judged as a whole; only what holds one is entered */
        if (down && !error && ts_node_has_error(n) && ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        if (ts_node_eq(n, decl)) {
            break;
        }
        if (ts_tree_cursor_goto_next_sibling(&cur)) {
            down = true;
            continue;
        }
        if (!ts_tree_cursor_goto_parent(&cur)) {
            break;
        }
        down = false;
    }
    ts_tree_cursor_delete(&cur);
    return contained;
}

/* True when a parse error inside `decl` may reach past it. */
static bool jv_errors_leak(const jv_scan_t *s, TSNode decl) {
    return ts_node_has_error(decl) && !jv_errors_contained(s, decl);
}

/* A member the scan keeps, in a file with parse errors: when nothing in it
 * failed to parse, or its body is a block of the text, or its errors are held
 * by blocks inside it, a type keyword inside it names a local type, not one
 * the tree lost. */
static void jv_member_seen(jv_scan_t *s, TSNode decl, TSNode body, bool callable) {
    if (!s->lexical) {
        return;
    }
    bool block = callable && !ts_node_is_null(body) && s->src[ts_node_start_byte(body)] == '{';
    if (block || !jv_errors_leak(s, decl)) {
        jv_ranges_push(s, &s->members, ts_node_start_byte(decl), ts_node_end_byte(decl));
    }
}

/* ── Names and types as written ──────────────────────────────────── */

/* The declared name at `node`: *raw is its text as the extractor names the
 * definition (a Kotlin name keeps its backticks there), the result is the
 * name itself. NULL unless it is one name of sane length. */
static const char *jv_name_of(jv_scan_t *s, TSNode node, const char **raw, size_t *raw_len) {
    if (ts_node_is_null(node)) {
        return NULL;
    }
    uint32_t a = ts_node_start_byte(node);
    uint32_t b = ts_node_end_byte(node);
    if (b <= a || b > s->src_len || b - a > JV_NAME_MAX) {
        return NULL;
    }
    const char *p = s->src + a;
    size_t n = b - a;
    *raw = p;
    *raw_len = n;
    bool quoted = n > PAIR_LEN && p[0] == '`' && p[n - SKIP_ONE] == '`';
    if (quoted) {
        p++;
        n -= PAIR_LEN;
    }
    for (size_t i = 0; i < n; i++) {
        char c = p[i];
        bool ok = quoted ? ((unsigned char)c >= ' ' && !strchr(".|,`", c)) : jv_ident_char(c);
        if (!ok) {
            return NULL;
        }
    }
    char *out = (char *)jv_tmp_alloc(s, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    memcpy(out, p, n);
    out[n] = '\0';
    return out;
}

static const char *jv_join(jv_scan_t *s, const char *outer, const char *name) {
    if (!outer[0]) {
        return name;
    }
    size_t ol = strlen(outer);
    size_t nl = strlen(name);
    char *out = (char *)jv_tmp_alloc(s, ol + nl + PAIR_LEN);
    if (!out) {
        return NULL;
    }
    memcpy(out, outer, ol);
    out[ol] = '.';
    memcpy(out + ol + SKIP_ONE, name, nl + SKIP_ONE);
    return out;
}

/* Source text [a, b) as a (qualified) name into `out`: blanks, annotations,
 * type arguments, nullable marks and backticks go. false when what is left is
 * no name. */
static bool jv_type_name(const jv_scan_t *s, uint32_t a, uint32_t b, char *out, size_t cap) {
    char work[JV_TYPE_MAX];
    if (b <= a || b > s->src_len || b - a >= sizeof(work)) {
        return false;
    }
    size_t n = b - a;
    memcpy(work, s->src + a, n);
    work[n] = '\0';
    jv_drop_annotations_and_args(work, &n);
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        char c = work[i];
        if (jv_blank(c) || c == '?' || c == '`') {
            continue;
        }
        if (!(jv_ident_char(c) || c == '.') || w + SKIP_ONE >= cap) {
            return false;
        }
        out[w++] = c;
    }
    out[w] = '\0';
    return w > 0 && out[0] != '.' && out[w - SKIP_ONE] != '.';
}

/* A written type as a name; "?" when it is none (a function type, text an
 * error left behind): the declaring type then has a supertype nobody can
 * follow. */
static void jv_put_type_name(jv_scan_t *s, TSNode node) {
    char name[JV_TYPE_MAX];
    if (!ts_node_is_null(node) &&
        jv_type_name(s, ts_node_start_byte(node), ts_node_end_byte(node), name, sizeof(name))) {
        jv_puts(&s->sb, name);
    } else {
        jv_putc(&s->sb, '?');
    }
}

/* `<T, U extends X>` / `<in T, out U : X>` -> "T,U:X" (Java) / "T,U" (Kotlin).
 * A Java type variable keeps its first bound: that is its erasure, and a
 * Javadoc signature names a parameter of that type by it. */
static void jv_put_tparams(jv_scan_t *s, TSNode list) {
    if (ts_node_is_null(list)) {
        return;
    }
    TSTreeCursor cur = ts_tree_cursor_new(list);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        bool first = true;
        do {
            TSNode tp = ts_tree_cursor_current_node(&cur);
            if (!jv_kind(tp, "type_parameter")) {
                continue;
            }
            const char *raw = NULL;
            size_t raw_len = 0;
            const char *name = jv_name_of(s, jv_child(tp, "type_identifier"), &raw, &raw_len);
            if (!name) {
                continue;
            }
            if (!first) {
                jv_putc(&s->sb, ',');
            }
            jv_puts(&s->sb, name);
            first = false;
            TSNode bound = s->kotlin ? (TSNode){0} : jv_child(tp, "type_bound");
            TSNode bt = (ts_node_is_null(bound) || ts_node_named_child_count(bound) == 0)
                            ? (TSNode){0}
                            : ts_node_named_child(bound, 0);
            if (!ts_node_is_null(bt) && ts_node_end_byte(bt) <= s->src_len) {
                char norm[CBM_SZ_256];
                uint32_t a = ts_node_start_byte(bt);
                cbm_doclink_java_norm_type(s->src + a, ts_node_end_byte(bt) - a, norm,
                                           sizeof(norm));
                if (norm[0] && !strpbrk(norm, ",:\t\n")) {
                    jv_putc(&s->sb, ':');
                    jv_puts(&s->sb, norm);
                }
            }
        } while (ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
}

/* ── Records ─────────────────────────────────────────────────────── */

/* slot and own of a declaration whose definition is `def` (NULL: it has
 * none) and whose logical path is `logical`. */
static void jv_put_slot(jv_scan_t *s, const jv_def_t *def, const char *logical) {
    size_t len = def ? strlen(def->tail) : 0;
    bool usable = def && logical && len > 0 && len <= JV_SLOT_MAX;
    for (size_t i = 0; usable && i < len; i++) {
        usable = (unsigned char)def->tail[i] >= ' ';
    }
    if (!usable) {
        jv_puts(&s->sb, "-\t0");
        return;
    }
    if (strcmp(def->tail, logical) == 0) {
        jv_putc(&s->sb, '=');
    } else {
        jv_putn(&s->sb, def->tail, len);
    }
    jv_putc(&s->sb, '\t');
    jv_putc(&s->sb, def->winner ? '1' : '0');
}

/* "T start end kind[!] path " -- the caller adds tparams, supers, slot, own,
 * pnames. */
static void jv_type_open(jv_scan_t *s, TSNode decl, char kind, bool incomplete, const char *path) {
    jv_puts(&s->sb, "T\t");
    jv_putu(&s->sb, jv_line(decl));
    jv_putc(&s->sb, '\t');
    jv_putu(&s->sb, jv_end_line(decl));
    jv_putc(&s->sb, '\t');
    jv_putc(&s->sb, kind);
    if (incomplete) {
        jv_putc(&s->sb, '!');
    }
    jv_putc(&s->sb, '\t');
    jv_puts(&s->sb, path);
    jv_putc(&s->sb, '\t');
}

/* "M start kind flags owner name " -- the caller adds tparams, sig, recv,
 * slot, own, pnames. */
static void jv_member_open(jv_scan_t *s, uint32_t line, char kind, const char *flags,
                           const jv_frame_t *fr, const char *name) {
    jv_puts(&s->sb, "M\t");
    jv_putu(&s->sb, line);
    jv_putc(&s->sb, '\t');
    jv_putc(&s->sb, kind);
    jv_putc(&s->sb, '\t');
    jv_puts(&s->sb, flags);
    jv_putc(&s->sb, '\t');
    if (fr->owner == JV_NO_OWNER) {
        jv_putc(&s->sb, '-');
    } else {
        jv_putu(&s->sb, (uint32_t)fr->owner);
    }
    jv_putc(&s->sb, '\t');
    jv_puts(&s->sb, name);
    jv_putc(&s->sb, '\t');
}

/* A value member without type parameters, receiver or parameter names: `sig`
 * is "-", or what its kind keeps there (a record component's type). */
static void jv_emit_value(jv_scan_t *s, uint32_t line, TSNode name_node, const char *flags,
                          const char *sig, const jv_frame_t *fr) {
    const char *raw = NULL;
    size_t raw_len = 0;
    const char *name = jv_name_of(s, name_node, &raw, &raw_len);
    if (!name) {
        return;
    }
    const jv_def_t *def = jv_def_match(s, line, raw, raw_len, 'v');
    jv_member_open(s, line, 'v', flags, fr, name);
    jv_putc(&s->sb, '\t');
    jv_puts(&s->sb, sig);
    jv_puts(&s->sb, "\t\t");
    jv_put_slot(s, def, jv_join(s, fr->path, name));
    jv_puts(&s->sb, "\t\n");
}

static void jv_emit_unplaced(jv_scan_t *s, uint32_t from, uint32_t to) {
    jv_puts(&s->sb, "X\t");
    jv_putu(&s->sb, from);
    jv_putc(&s->sb, '\t');
    jv_putu(&s->sb, to);
    jv_putc(&s->sb, '\n');
}

static char jv_java_type_kind(const char *k) {
    static const struct {
        const char *node;
        char kind;
    } kinds[] = {{"class_declaration", 'c'},
                 {"interface_declaration", 'i'},
                 {"enum_declaration", 'e'},
                 {"record_declaration", 'r'},
                 {"annotation_type_declaration", 'a'}};
    for (size_t i = 0; i < sizeof(kinds) / sizeof(kinds[0]); i++) {
        if (strcmp(k, kinds[i].node) == 0) {
            return kinds[i].kind;
        }
    }
    return 0;
}

static bool jv_kotlin_is_type(const char *k) {
    return strcmp(k, "class_declaration") == 0 || strcmp(k, "object_declaration") == 0 ||
           strcmp(k, "companion_object") == 0 || strcmp(k, "type_alias") == 0;
}

/* The name of a type declaration node; a null node for an unnamed one. */
static TSNode jv_type_name_node(const jv_scan_t *s, TSNode decl) {
    return s->kotlin ? jv_child(decl, "type_identifier") : jv_field(decl, "name");
}

/* Every type declared in the subtree is declared, but where is unknown (the
 * subtree is an error node, or a declaration that could not be placed): its
 * name must not resolve to anything else either. */
static void jv_quarantine(jv_scan_t *s, TSNode node) {
    TSTreeCursor cur = ts_tree_cursor_new(node);
    bool down = true;
    for (;;) {
        if (down) {
            TSNode n = ts_tree_cursor_current_node(&cur);
            const char *k = ts_node_type(n);
            if (s->kotlin ? jv_kotlin_is_type(k) : jv_java_type_kind(k) != 0) {
                TSNode name_node = jv_type_name_node(s, n);
                const char *raw = NULL;
                size_t raw_len = 0;
                const char *name = jv_name_of(s, name_node, &raw, &raw_len);
                if (name) {
                    jv_ranges_push(s, &s->seen, ts_node_start_byte(name_node),
                                   ts_node_end_byte(name_node));
                    jv_puts(&s->sb, "Q\t");
                    jv_puts(&s->sb, name);
                    jv_putc(&s->sb, '\n');
                }
            }
            if (ts_tree_cursor_goto_first_child(&cur)) {
                continue;
            }
        }
        if (ts_tree_cursor_goto_next_sibling(&cur)) {
            down = true;
            continue;
        }
        if (!ts_tree_cursor_goto_parent(&cur)) {
            break;
        }
        down = false;
    }
    ts_tree_cursor_delete(&cur);
}

/* A declaration or an error node that could not be placed. */
static void jv_unplaced(jv_scan_t *s, TSNode node, bool report_lines) {
    if (report_lines) {
        jv_emit_unplaced(s, jv_line(node), jv_end_line(node));
    }
    jv_quarantine(s, node);
}

/* ── Java ────────────────────────────────────────────────────────── */

static void jv_java_container(jv_scan_t *s, TSNode body, const jv_frame_t *fr, int depth);

static bool jv_java_is_callable(const char *k) {
    return strcmp(k, "method_declaration") == 0 || strcmp(k, "constructor_declaration") == 0 ||
           strcmp(k, "compact_constructor_declaration") == 0 ||
           strcmp(k, "annotation_type_element_declaration") == 0;
}

static bool jv_java_is_value(const char *k) {
    return strcmp(k, "field_declaration") == 0 || strcmp(k, "constant_declaration") == 0 ||
           strcmp(k, "enum_constant") == 0;
}

/* True when the scan does not show every member of the type whose body this
 * is: an error node among its members, a member or nested type that could
 * not be placed, an error inside a member that has no block of its own to
 * hold it, or (`leaf`: the type is as deep as the scan goes) a nested type. */
static bool jv_java_body_broken(const jv_scan_t *s, TSNode body, int inner, bool leaf) {
    bool broken = false;
    bool nested = jv_kind(body, "enum_body_declarations");
    TSTreeCursor cur = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode c = ts_tree_cursor_current_node(&cur);
            const char *k = ts_node_type(c);
            if (ts_node_is_error(c) || ts_node_is_missing(c)) {
                broken = true;
            } else if (strcmp(k, "enum_body_declarations") == 0) {
                /* the members of an enum, one level below its constants */
                broken = nested || jv_java_body_broken(s, c, inner, leaf);
            } else if (jv_java_type_kind(k)) {
                broken = leaf || !jv_type_placed(s, c, jv_field(c, "body"), inner);
            } else if (jv_java_is_callable(k)) {
                broken = !jv_member_ok(s, c, jv_field(c, "body"), true, inner);
            } else if (jv_java_is_value(k)) {
                broken = jv_errors_leak(s, c) || !jv_member_ok(s, c, (TSNode){0}, false, inner);
            }
        } while (!broken && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    return broken;
}

static void jv_java_put_supers(jv_scan_t *s, TSNode decl) {
    bool first = true;
    TSNode sc = jv_field(decl, "superclass");
    uint32_t snc = ts_node_is_null(sc) ? 0 : ts_node_named_child_count(sc);
    if (snc > 0) {
        jv_put_type_name(s, ts_node_named_child(sc, snc - SKIP_ONE));
        first = false;
    }
    TSNode lists[] = {jv_field(decl, "interfaces"), jv_child(decl, "extends_interfaces")};
    for (size_t l = 0; l < sizeof(lists) / sizeof(lists[0]); l++) {
        TSNode tl = ts_node_is_null(lists[l]) ? lists[l] : jv_child(lists[l], "type_list");
        uint32_t nc = ts_node_is_null(tl) ? 0 : ts_node_named_child_count(tl);
        for (uint32_t i = 0; i < nc; i++) {
            if (!first) {
                jv_putc(&s->sb, '|');
            }
            jv_put_type_name(s, ts_node_named_child(tl, i));
            first = false;
        }
    }
}

/* One declared parameter type, normalized, with the brackets that follow the
 * parameter's name (`int x[]`) or its ellipsis. */
static void jv_java_put_param_type(jv_scan_t *s, TSNode ty, int extra_dims) {
    char norm[CBM_SZ_256];
    if (ts_node_is_null(ty) || ts_node_end_byte(ty) > s->src_len) {
        snprintf(norm, sizeof(norm), "?");
    } else {
        uint32_t a = ts_node_start_byte(ty);
        cbm_doclink_java_norm_type(s->src + a, ts_node_end_byte(ty) - a, norm, sizeof(norm));
    }
    jv_puts(&s->sb, norm);
    for (int i = 0; i < extra_dims; i++) {
        jv_puts(&s->sb, "[]");
    }
}

static void jv_java_put_sig(jv_scan_t *s, TSNode params) {
    if (ts_node_is_null(params)) {
        return;
    }
    bool first = true;
    uint32_t nc = ts_node_named_child_count(params);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_named_child(params, i);
        TSNode ty = {0};
        int dims = 0;
        if (jv_kind(p, "formal_parameter")) {
            ty = jv_field(p, "type");
            TSNode d = jv_field(p, "dimensions");
            for (uint32_t k = ts_node_is_null(d) ? 0 : ts_node_start_byte(d);
                 !ts_node_is_null(d) && k < ts_node_end_byte(d) && k < s->src_len; k++) {
                dims += s->src[k] == '[';
            }
        } else if (jv_kind(p, "spread_parameter")) {
            uint32_t pc = ts_node_named_child_count(p);
            for (uint32_t k = 0; k < pc && ts_node_is_null(ty); k++) {
                TSNode ch = ts_node_named_child(p, k);
                if (!jv_kind(ch, "modifiers")) {
                    ty = ch;
                }
            }
            dims = SKIP_ONE;
        } else {
            continue; /* a receiver parameter, a comment */
        }
        if (!first) {
            jv_putc(&s->sb, '|');
        }
        jv_java_put_param_type(s, ty, dims);
        first = false;
    }
}

static void jv_java_callable(jv_scan_t *s, TSNode decl, const char *k, const jv_frame_t *fr) {
    bool compact = strcmp(k, "compact_constructor_declaration") == 0;
    bool ctor = compact || strcmp(k, "constructor_declaration") == 0;
    TSNode body = jv_field(decl, "body");
    const char *raw = NULL;
    size_t raw_len = 0;
    const char *name = jv_name_of(s, jv_field(decl, "name"), &raw, &raw_len);
    if (!name || !jv_member_ok(s, decl, body, true, fr->inner_depth)) {
        return;
    }
    jv_member_seen(s, decl, body, true);
    uint32_t line = jv_line(decl);
    const jv_def_t *def = jv_def_match(s, line, raw, raw_len, 'c');
    jv_member_open(s, line, 'c', ctor ? "K" : "", fr, name);
    jv_put_tparams(s, jv_field(decl, "type_parameters"));
    jv_putc(&s->sb, '\t');
    if (compact) {
        jv_putc(&s->sb, '*'); /* the canonical constructor: the components' types */
    } else {
        jv_java_put_sig(s, jv_field(decl, "parameters"));
    }
    jv_puts(&s->sb, "\t\t");
    jv_put_slot(s, def, jv_join(s, fr->path, name));
    jv_puts(&s->sb, "\t\n");
}

/* A field or constant declaration: one member per declarator. */
static void jv_java_fields(jv_scan_t *s, TSNode decl, const jv_frame_t *fr) {
    if (!jv_member_ok(s, decl, (TSNode){0}, false, fr->inner_depth)) {
        return;
    }
    jv_member_seen(s, decl, (TSNode){0}, false);
    uint32_t line = jv_line(decl);
    uint32_t nc = ts_node_named_child_count(decl);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode vd = ts_node_named_child(decl, i);
        if (jv_kind(vd, "variable_declarator")) {
            jv_emit_value(s, line, jv_field(vd, "name"), "", "-", fr);
        }
    }
}

/* The components of a record are its fields; each keeps its type, which is
 * the canonical constructor's signature. */
static void jv_java_components(jv_scan_t *s, TSNode decl, const jv_frame_t *fr) {
    TSNode params = jv_field(decl, "parameters");
    uint32_t nc = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_named_child(params, i);
        if (!jv_kind(p, "formal_parameter")) {
            continue;
        }
        char norm[CBM_SZ_256];
        TSNode ty = jv_field(p, "type");
        if (ts_node_is_null(ty) || ts_node_end_byte(ty) > s->src_len) {
            snprintf(norm, sizeof(norm), "?");
        } else {
            uint32_t a = ts_node_start_byte(ty);
            cbm_doclink_java_norm_type(s->src + a, ts_node_end_byte(ty) - a, norm, sizeof(norm));
        }
        jv_emit_value(s, jv_line(p), jv_field(p, "name"), "p", norm, fr);
    }
}

static void jv_java_import(jv_scan_t *s, TSNode decl) {
    if (s->lexical && (ts_node_start_byte(decl) >= s->untrusted || ts_node_has_error(decl))) {
        return;
    }
    bool is_static = false;
    bool star = false;
    TSNode name = {0};
    uint32_t cc = ts_node_child_count(decl);
    for (uint32_t i = 0; i < cc; i++) {
        TSNode ch = ts_node_child(decl, i);
        const char *k = ts_node_type(ch);
        if (strcmp(k, "static") == 0) {
            is_static = true;
        } else if (strcmp(k, "asterisk") == 0) {
            star = true;
        } else if (strcmp(k, "scoped_identifier") == 0 || strcmp(k, "identifier") == 0) {
            name = ch;
        }
    }
    char text[JV_TYPE_MAX];
    if (ts_node_is_null(name) ||
        !jv_type_name(s, ts_node_start_byte(name), ts_node_end_byte(name), text, sizeof(text))) {
        return;
    }
    char *dot = strrchr(text, '.');
    if (star) {
        jv_puts(&s->sb, is_static ? "I\tn\t-\t" : "I\tw\t-\t");
        jv_puts(&s->sb, text);
    } else if (!is_static) {
        jv_puts(&s->sb, "I\ts\t");
        jv_puts(&s->sb, dot ? dot + SKIP_ONE : text);
        jv_putc(&s->sb, '\t');
        jv_puts(&s->sb, text);
    } else if (dot) {
        *dot = '\0'; /* `import static a.b.C.member;`: the member of a type */
        jv_puts(&s->sb, "I\tm\t");
        jv_puts(&s->sb, dot + SKIP_ONE);
        jv_putc(&s->sb, '\t');
        jv_puts(&s->sb, text);
    } else {
        return;
    }
    jv_putc(&s->sb, '\n');
}

static void jv_java_type(jv_scan_t *s, TSNode decl, char kind, const jv_frame_t *outer, int depth) {
    TSNode name_node = jv_field(decl, "name");
    const char *raw = NULL;
    size_t raw_len = 0;
    const char *name = jv_name_of(s, name_node, &raw, &raw_len);
    if (!name) {
        return;
    }
    TSNode body = jv_field(decl, "body");
    const char *path = jv_join(s, outer->path, name);
    /* nested deeper than any real program: not placed, like a parse error */
    if (!path || depth >= JV_MAX_DEPTH || !jv_type_placed(s, decl, body, outer->inner_depth)) {
        jv_unplaced(s, decl, true);
        return;
    }
    if (s->lexical) {
        jv_ranges_push(s, &s->seen, ts_node_start_byte(name_node), ts_node_end_byte(name_node));
    }
    int inner = outer->inner_depth + SKIP_ONE;
    bool leaf = depth + SKIP_ONE >= JV_MAX_DEPTH;
    bool incomplete =
        (s->lexical || leaf) && !ts_node_is_null(body) && jv_java_body_broken(s, body, inner, leaf);
    const jv_def_t *def = jv_def_match(s, jv_line(decl), raw, raw_len, 'T');
    jv_frame_t fr = {.owner = s->ntypes++, .path = path, .name = name, .inner_depth = inner};
    jv_type_open(s, decl, kind, incomplete, path);
    jv_put_tparams(s, jv_field(decl, "type_parameters"));
    jv_putc(&s->sb, '\t');
    jv_java_put_supers(s, decl);
    jv_putc(&s->sb, '\t');
    jv_put_slot(s, def, path);
    jv_puts(&s->sb, "\t\n");
    if (kind == 'r') {
        jv_java_components(s, decl, &fr);
    }
    if (!ts_node_is_null(body)) {
        jv_java_container(s, body, &fr, depth + SKIP_ONE);
    }
}

/* The declarations of a class body (or of the file): types, members,
 * imports. `depth` is the nesting of the types declared here; jv_java_type
 * bounds it, and so the recursion. */
static void jv_java_container(jv_scan_t *s, TSNode body, const jv_frame_t *fr, int depth) {
    bool enum_members = jv_kind(body, "enum_body_declarations");
    TSTreeCursor cur = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode c = ts_tree_cursor_current_node(&cur);
            const char *k = ts_node_type(c);
            char tk = jv_java_type_kind(k);
            if (ts_node_is_error(c)) {
                jv_unplaced(s, c, true);
            } else if (ts_node_is_missing(c)) {
                continue;
            } else if (tk) {
                jv_java_type(s, c, tk, fr, depth);
            } else if (strcmp(k, "import_declaration") == 0) {
                jv_java_import(s, c);
            } else if (strcmp(k, "enum_body_declarations") == 0) {
                if (!enum_members) {
                    jv_java_container(s, c, fr, depth); /* the same type's members */
                }
            } else if (fr->owner == JV_NO_OWNER) {
                continue; /* the file itself has no members */
            } else if (jv_java_is_callable(k)) {
                jv_java_callable(s, c, k, fr);
            } else if (strcmp(k, "enum_constant") == 0) {
                if (jv_member_ok(s, c, (TSNode){0}, false, fr->inner_depth)) {
                    jv_member_seen(s, c, (TSNode){0}, false);
                    jv_emit_value(s, jv_line(c), jv_field(c, "name"), "n", "-", fr);
                }
            } else if (jv_java_is_value(k)) {
                jv_java_fields(s, c, fr);
            }
        } while (!s->failed && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
}

/* ── Kotlin ──────────────────────────────────────────────────────── */

static void jv_kotlin_container(jv_scan_t *s, TSNode body, const jv_frame_t *fr, int depth);

static TSNode jv_kotlin_body(TSNode decl) {
    TSNode body = jv_child(decl, "class_body");
    return ts_node_is_null(body) ? jv_child(decl, "enum_class_body") : body;
}

/* True when the scan does not show every member of the type whose body this
 * is (see jv_java_body_broken). A function with an expression body, a
 * property initializer or an accessor has no braces of its own to hold an
 * error. */
static bool jv_kotlin_body_broken(const jv_scan_t *s, TSNode body, int inner, bool leaf) {
    bool broken = false;
    TSTreeCursor cur = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode c = ts_tree_cursor_current_node(&cur);
            const char *k = ts_node_type(c);
            if (ts_node_is_error(c) || ts_node_is_missing(c)) {
                broken = true;
            } else if (jv_kotlin_is_type(k)) {
                broken = leaf || !jv_type_placed(s, c, jv_kotlin_body(c), inner);
            } else if (strcmp(k, "function_declaration") == 0) {
                TSNode fb = jv_child(c, "function_body");
                bool block = !ts_node_is_null(fb) && s->src[ts_node_start_byte(fb)] == '{';
                broken = !jv_member_ok(s, c, fb, true, inner) || (!block && jv_errors_leak(s, c));
            } else if (ts_node_is_named(c)) {
                broken = jv_errors_leak(s, c) || ((strcmp(k, "property_declaration") == 0 ||
                                                   strcmp(k, "secondary_constructor") == 0 ||
                                                   strcmp(k, "enum_entry") == 0) &&
                                                  !jv_member_ok(s, c, (TSNode){0}, false, inner));
            }
        } while (!broken && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    return broken;
}

static void jv_kotlin_put_supers(jv_scan_t *s, TSNode decl) {
    bool first = true;
    uint32_t nc = ts_node_named_child_count(decl);
    if (jv_kind(decl, "type_alias")) {
        /* `typealias A = B`: A has what B has, so B stands where a supertype
         * does. An alias of anything but a named type has no known members. */
        TSNode target = nc > 0 ? ts_node_named_child(decl, nc - SKIP_ONE) : (TSNode){0};
        bool is_type = !ts_node_is_null(target) && !jv_kind(target, "type_identifier") &&
                       !jv_kind(target, "type_parameters") && !jv_kind(target, "modifiers");
        jv_put_type_name(s, is_type ? target : (TSNode){0});
        return;
    }
    for (uint32_t i = 0; i < nc; i++) {
        TSNode ds = ts_node_named_child(decl, i);
        if (!jv_kind(ds, "delegation_specifier")) {
            continue;
        }
        TSNode x = ts_node_named_child_count(ds) > 0 ? ts_node_named_child(ds, 0) : (TSNode){0};
        if (!ts_node_is_null(x) &&
            (jv_kind(x, "constructor_invocation") || jv_kind(x, "explicit_delegation"))) {
            x = jv_child(x, "user_type");
        }
        if (!first) {
            jv_putc(&s->sb, '|');
        }
        jv_put_type_name(s, (!ts_node_is_null(x) && jv_kind(x, "user_type")) ? x : (TSNode){0});
        first = false;
    }
}

/* The names of the parameters under `list`, ','-joined (`item`: the node kind
 * of one parameter). */
static void jv_kotlin_put_pnames(jv_scan_t *s, TSNode list, const char *item) {
    bool first = true;
    uint32_t nc = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_named_child(list, i);
        if (!jv_kind(p, item)) {
            continue;
        }
        const char *raw = NULL;
        size_t raw_len = 0;
        const char *name = jv_name_of(s, jv_child(p, "simple_identifier"), &raw, &raw_len);
        if (!name) {
            continue;
        }
        if (!first) {
            jv_putc(&s->sb, ',');
        }
        jv_puts(&s->sb, name);
        first = false;
    }
}

/* Source text [a, b) without blanks; a byte that would break a record is
 * written as '?'. Text too long to be a type is "?". */
static void jv_put_compact(jv_scan_t *s, uint32_t a, uint32_t b) {
    if (b <= a || b > s->src_len || b - a > JV_TYPE_MAX) {
        jv_putc(&s->sb, '?');
        return;
    }
    for (uint32_t i = a; i < b; i++) {
        char c = s->src[i];
        if (jv_blank(c)) {
            continue;
        }
        jv_putc(&s->sb, ((unsigned char)c < ' ' || c == '|') ? '?' : c);
    }
}

/* The signature of a Kotlin declaration: what tells one declaration of a name
 * from another. `R=<receiver>` for an extension, then the parameter types as
 * written. KDoc cannot name a signature, so it is compared, never parsed. */
static void jv_kotlin_put_sig(jv_scan_t *s, TSNode receiver, TSNode params, bool callable) {
    bool any = false;
    if (!ts_node_is_null(receiver)) {
        jv_puts(&s->sb, "R=");
        jv_put_compact(s, ts_node_start_byte(receiver), ts_node_end_byte(receiver));
        any = true;
    }
    uint32_t nc = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_named_child(params, i);
        if (!jv_kind(p, "parameter")) {
            continue;
        }
        if (any) {
            jv_putc(&s->sb, '|');
        }
        uint32_t pc = ts_node_named_child_count(p);
        if (pc > SKIP_ONE) {
            TSNode ty = ts_node_named_child(p, SKIP_ONE);
            jv_put_compact(s, ts_node_start_byte(ty), ts_node_end_byte(ty));
        } else {
            jv_putc(&s->sb, '?');
        }
        any = true;
    }
    if (!any && !callable) {
        jv_putc(&s->sb, '-');
    }
}

/* The receiver type of an extension, as written (a dotted name): the type a
 * reference `Type.member` finds the extension under. A receiver that is one
 * of the declaration's own type parameters stands for its bound; one without
 * a bound (or a function type) has no name: "?". */
static void jv_kotlin_put_recv(jv_scan_t *s, TSNode receiver, TSNode tparams) {
    char name[JV_TYPE_MAX];
    if (ts_node_is_null(receiver)) {
        return;
    }
    if (!jv_type_name(s, ts_node_start_byte(receiver), ts_node_end_byte(receiver), name,
                      sizeof(name))) {
        jv_putc(&s->sb, '?');
        return;
    }
    bool simple = strchr(name, '.') == NULL;
    TSTreeCursor cur = ts_tree_cursor_new(ts_node_is_null(tparams) ? receiver : tparams);
    bool more = simple && !ts_node_is_null(tparams) && ts_tree_cursor_goto_first_child(&cur);
    for (; more; more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode tp = ts_tree_cursor_current_node(&cur);
        TSNode id = jv_kind(tp, "type_parameter") ? jv_child(tp, "type_identifier") : (TSNode){0};
        uint32_t a = ts_node_is_null(id) ? 0 : ts_node_start_byte(id);
        uint32_t b = ts_node_is_null(id) ? 0 : ts_node_end_byte(id);
        if (b - a != strlen(name) || b > s->src_len || memcmp(s->src + a, name, b - a) != 0) {
            continue;
        }
        /* its bound is the type after the name */
        char bound[JV_TYPE_MAX];
        uint32_t tc = ts_node_named_child_count(tp);
        TSNode bn = tc > SKIP_ONE ? ts_node_named_child(tp, tc - SKIP_ONE) : (TSNode){0};
        bool named =
            !ts_node_is_null(bn) && !ts_node_eq(bn, id) &&
            jv_type_name(s, ts_node_start_byte(bn), ts_node_end_byte(bn), bound, sizeof(bound));
        ts_tree_cursor_delete(&cur);
        jv_puts(&s->sb, named ? bound : "?");
        return;
    }
    ts_tree_cursor_delete(&cur);
    jv_puts(&s->sb, name);
}

static void jv_kotlin_function(jv_scan_t *s, TSNode decl, const jv_frame_t *fr) {
    TSNode body = jv_child(decl, "function_body");
    const char *raw = NULL;
    size_t raw_len = 0;
    const char *name = jv_name_of(s, jv_child(decl, "simple_identifier"), &raw, &raw_len);
    if (!name || !jv_member_ok(s, decl, body, true, fr->inner_depth)) {
        return;
    }
    jv_member_seen(s, decl, body, true);
    uint32_t line = jv_line(decl);
    TSNode receiver = jv_field(decl, "receiver");
    TSNode tparams = jv_child(decl, "type_parameters");
    TSNode params = jv_child(decl, "function_value_parameters");
    const jv_def_t *def = jv_def_match(s, line, raw, raw_len, 'c');
    jv_member_open(s, line, 'c', "", fr, name);
    jv_put_tparams(s, tparams);
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_sig(s, receiver, params, true);
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_recv(s, receiver, tparams);
    jv_putc(&s->sb, '\t');
    jv_put_slot(s, def, jv_join(s, fr->path, name));
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_pnames(s, params, "parameter");
    jv_putc(&s->sb, '\n');
}

static void jv_kotlin_property(jv_scan_t *s, TSNode decl, const jv_frame_t *fr) {
    TSNode vd = jv_child(decl, "variable_declaration");
    const char *raw = NULL;
    size_t raw_len = 0;
    const char *name = ts_node_is_null(vd)
                           ? NULL
                           : jv_name_of(s, jv_child(vd, "simple_identifier"), &raw, &raw_len);
    if (!name || !jv_member_ok(s, decl, (TSNode){0}, false, fr->inner_depth)) {
        return; /* a destructuring declaration names no member */
    }
    jv_member_seen(s, decl, (TSNode){0}, false);
    uint32_t line = jv_line(decl);
    TSNode receiver = jv_field(decl, "receiver");
    TSNode tparams = jv_child(decl, "type_parameters");
    const jv_def_t *def = jv_def_match(s, line, raw, raw_len, 'v');
    jv_member_open(s, line, 'v', "", fr, name);
    jv_put_tparams(s, tparams);
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_sig(s, receiver, (TSNode){0}, false);
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_recv(s, receiver, tparams);
    jv_putc(&s->sb, '\t');
    jv_put_slot(s, def, jv_join(s, fr->path, name));
    jv_puts(&s->sb, "\t\n");
}

/* A secondary constructor: the extractor names it after its class. */
static void jv_kotlin_constructor(jv_scan_t *s, TSNode decl, const jv_frame_t *fr) {
    if (fr->owner == JV_NO_OWNER || !jv_member_ok(s, decl, (TSNode){0}, true, fr->inner_depth)) {
        return;
    }
    jv_member_seen(s, decl, (TSNode){0}, true);
    uint32_t line = jv_line(decl);
    TSNode params = jv_child(decl, "function_value_parameters");
    const jv_def_t *def = jv_def_match(s, line, fr->name, strlen(fr->name), 'c');
    jv_member_open(s, line, 'c', "K", fr, fr->name);
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_sig(s, (TSNode){0}, params, true);
    jv_puts(&s->sb, "\t\t");
    jv_put_slot(s, def, jv_join(s, fr->path, fr->name));
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_pnames(s, params, "parameter");
    jv_putc(&s->sb, '\n');
}

static void jv_kotlin_import(jv_scan_t *s, TSNode header) {
    if (s->lexical && (ts_node_start_byte(header) >= s->untrusted || ts_node_has_error(header))) {
        return;
    }
    TSNode id = jv_child(header, "identifier");
    char text[JV_TYPE_MAX];
    if (ts_node_is_null(id) ||
        !jv_type_name(s, ts_node_start_byte(id), ts_node_end_byte(id), text, sizeof(text))) {
        return;
    }
    if (!ts_node_is_null(jv_child(header, "wildcard_import"))) {
        jv_puts(&s->sb, "I\tw\t-\t");
        jv_puts(&s->sb, text);
        jv_putc(&s->sb, '\n');
        return;
    }
    const char *dot = strrchr(text, '.');
    const char *alias = dot ? dot + SKIP_ONE : text;
    TSNode an = jv_child(header, "import_alias");
    if (!ts_node_is_null(an)) {
        const char *raw = NULL;
        size_t raw_len = 0;
        alias = jv_name_of(s, jv_child(an, "type_identifier"), &raw, &raw_len);
        if (!alias) {
            return;
        }
    }
    jv_puts(&s->sb, "I\ts\t");
    jv_puts(&s->sb, alias);
    jv_putc(&s->sb, '\t');
    jv_puts(&s->sb, text);
    jv_putc(&s->sb, '\n');
}

static char jv_kotlin_type_kind(const jv_scan_t *s, TSNode decl, TSNode body) {
    const char *k = ts_node_type(decl);
    if (strcmp(k, "companion_object") == 0) {
        return 'C';
    }
    if (strcmp(k, "object_declaration") == 0) {
        return 'o';
    }
    if (strcmp(k, "type_alias") == 0) {
        return 't';
    }
    if (!ts_node_is_null(body) && jv_kind(body, "enum_class_body")) {
        return 'e';
    }
    /* the keywords before the name: `interface`, `enum class` */
    uint32_t cc = ts_node_child_count(decl);
    for (uint32_t i = 0; i < cc; i++) {
        TSNode ch = ts_node_child(decl, i);
        if (ts_node_is_named(ch)) {
            if (jv_kind(ch, "type_identifier")) {
                break;
            }
            continue;
        }
        uint32_t a = ts_node_start_byte(ch);
        uint32_t len = ts_node_end_byte(ch) - a;
        if (jv_word_is(s->src + a, len, "interface")) {
            return 'i';
        }
        if (jv_word_is(s->src + a, len, "enum")) {
            return 'e';
        }
    }
    return 'c';
}

static void jv_kotlin_type(jv_scan_t *s, TSNode decl, const jv_frame_t *outer, int depth) {
    TSNode body = jv_kotlin_body(decl);
    char kind = jv_kotlin_type_kind(s, decl, body);
    TSNode name_node = jv_child(decl, "type_identifier");
    const char *raw = NULL;
    size_t raw_len = 0;
    const char *name = (kind == 'C' && ts_node_is_null(name_node))
                           ? "Companion"
                           : jv_name_of(s, name_node, &raw, &raw_len);
    if (!name) {
        return;
    }
    const char *path = jv_join(s, outer->path, name);
    /* nested deeper than any real program: not placed, like a parse error */
    if (!path || depth >= JV_MAX_DEPTH || !jv_type_placed(s, decl, body, outer->inner_depth)) {
        jv_unplaced(s, decl, true);
        return;
    }
    if (s->lexical && !ts_node_is_null(name_node)) {
        jv_ranges_push(s, &s->seen, ts_node_start_byte(name_node), ts_node_end_byte(name_node));
    }
    int inner = outer->inner_depth + SKIP_ONE;
    bool leaf = depth + SKIP_ONE >= JV_MAX_DEPTH;
    bool incomplete = (s->lexical || leaf) && !ts_node_is_null(body) &&
                      jv_kotlin_body_broken(s, body, inner, leaf);
    const jv_def_t *def = raw ? jv_def_match(s, jv_line(decl), raw, raw_len, 'T') : NULL;
    TSNode ctor = jv_child(decl, "primary_constructor");
    jv_frame_t fr = {.owner = s->ntypes++, .path = path, .name = name, .inner_depth = inner};
    jv_type_open(s, decl, kind, incomplete, path);
    jv_put_tparams(s, jv_child(decl, "type_parameters"));
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_supers(s, decl);
    jv_putc(&s->sb, '\t');
    jv_put_slot(s, def, path);
    jv_putc(&s->sb, '\t');
    jv_kotlin_put_pnames(s, ctor, "class_parameter");
    jv_putc(&s->sb, '\n');
    /* a constructor parameter declared with val / var is a property */
    uint32_t pc = ts_node_is_null(ctor) ? 0 : ts_node_named_child_count(ctor);
    for (uint32_t i = 0; i < pc; i++) {
        TSNode p = ts_node_named_child(ctor, i);
        if (jv_kind(p, "class_parameter") &&
            !ts_node_is_null(jv_child(p, "binding_pattern_kind"))) {
            jv_emit_value(s, jv_line(p), jv_child(p, "simple_identifier"), "p", "-", &fr);
        }
    }
    if (!ts_node_is_null(body)) {
        jv_kotlin_container(s, body, &fr, depth + SKIP_ONE);
    }
}

/* The declarations of a class body (or of the file). `depth` is the nesting
 * of the types declared here; jv_kotlin_type bounds it, and so the recursion. */
static void jv_kotlin_container(jv_scan_t *s, TSNode body, const jv_frame_t *fr, int depth) {
    TSTreeCursor cur = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode c = ts_tree_cursor_current_node(&cur);
            const char *k = ts_node_type(c);
            if (ts_node_is_error(c)) {
                jv_unplaced(s, c, true);
            } else if (ts_node_is_missing(c)) {
                continue;
            } else if (jv_kotlin_is_type(k)) {
                jv_kotlin_type(s, c, fr, depth);
            } else if (strcmp(k, "function_declaration") == 0) {
                jv_kotlin_function(s, c, fr);
            } else if (strcmp(k, "property_declaration") == 0) {
                jv_kotlin_property(s, c, fr);
            } else if (strcmp(k, "secondary_constructor") == 0) {
                jv_kotlin_constructor(s, c, fr);
            } else if (strcmp(k, "enum_entry") == 0) {
                if (jv_member_ok(s, c, (TSNode){0}, false, fr->inner_depth)) {
                    jv_member_seen(s, c, (TSNode){0}, false);
                    jv_emit_value(s, jv_line(c), jv_child(c, "simple_identifier"), "n", "-", fr);
                }
            } else if (strcmp(k, "import_list") == 0) {
                /* a cursor: an import list can have any number of entries */
                TSTreeCursor ic = ts_tree_cursor_new(c);
                if (ts_tree_cursor_goto_first_child(&ic)) {
                    do {
                        TSNode h = ts_tree_cursor_current_node(&ic);
                        if (jv_kind(h, "import_header")) {
                            jv_kotlin_import(s, h);
                        }
                    } while (ts_tree_cursor_goto_next_sibling(&ic));
                }
                ts_tree_cursor_delete(&ic);
            } else if (strcmp(k, "import_header") == 0) {
                jv_kotlin_import(s, c);
            }
        } while (!s->failed && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
}

/* ── The scan ────────────────────────────────────────────────────── */

/* The package the file declares, as a record. The declaration is looked for
 * with a cursor: the root can have any number of children. */
static void jv_put_package(jv_scan_t *s) {
    char text[JV_TYPE_MAX];
    text[0] = '\0';
    const char *kind = s->kotlin ? "package_header" : "package_declaration";
    TSNode decl = {0};
    TSTreeCursor cur = ts_tree_cursor_new(s->ctx->root);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode c = ts_tree_cursor_current_node(&cur);
            if (jv_kind(c, kind)) {
                decl = c;
            }
        } while (ts_node_is_null(decl) && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    uint32_t nc = ts_node_is_null(decl) ? 0 : ts_node_named_child_count(decl);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode ch = ts_node_named_child(decl, i);
        const char *k = ts_node_type(ch);
        if ((strcmp(k, "scoped_identifier") == 0 || strcmp(k, "identifier") == 0) &&
            !jv_type_name(s, ts_node_start_byte(ch), ts_node_end_byte(ch), text, sizeof(text))) {
            text[0] = '\0';
        }
    }
    jv_puts(&s->sb, "P\t");
    jv_puts(&s->sb, text);
    jv_putc(&s->sb, '\n');
}

/* What only the text shows in a file with parse errors: a type keyword and
 * name the tree has no declaration for (outside the members whose inside is
 * their own) is a quarantined name, and nothing behind the first brace
 * without a partner is placed. */
static void jv_finish_lexical(jv_scan_t *s) {
    int si = 0;
    int mi = 0;
    for (int h = 0; h < s->heads.count && !s->failed; h++) {
        jv_range_t head = s->heads.items[h];
        while (si < s->seen.count && s->seen.items[si].start < head.start) {
            si++;
        }
        if (si < s->seen.count && s->seen.items[si].start == head.start) {
            continue; /* the tree has this declaration */
        }
        while (mi < s->members.count && s->members.items[mi].end <= head.start) {
            mi++;
        }
        if (mi < s->members.count && s->members.items[mi].start <= head.start) {
            continue; /* a local type of a member that is its own */
        }
        uint32_t a = head.start;
        uint32_t b = head.end;
        if (b - a > PAIR_LEN && s->src[a] == '`') {
            a++;
            b--;
        }
        bool ok = b > a && b - a <= JV_NAME_MAX;
        for (uint32_t i = a; ok && i < b; i++) {
            ok = (unsigned char)s->src[i] >= ' ' && s->src[i] != '.';
        }
        if (ok) {
            jv_puts(&s->sb, "Q\t");
            jv_putn(&s->sb, s->src + a, b - a);
            jv_putc(&s->sb, '\n');
        }
    }
    if (s->untrusted < s->src_len) {
        jv_emit_unplaced(s, jv_row_of(s, s->untrusted) + TS_LINE_OFFSET, jv_end_line(s->ctx->root));
    }
}

static const char *jv_scan_scope(CBMExtractCtx *ctx, bool kotlin) {
    if (ts_node_is_null(ctx->root) || !ctx->source) {
        return NULL;
    }
    jv_scan_t s = {.ctx = ctx,
                   .tmp = ctx->scratch ? ctx->scratch : ctx->arena,
                   .sb = {.a = ctx->arena},
                   .src = ctx->source,
                   .src_len = ctx->source_len > 0 ? (uint32_t)ctx->source_len : 0,
                   .kotlin = kotlin,
                   .untrusted = UINT32_MAX};
    s.module_qn = cbm_fqn_module_source_lang(s.tmp, ctx->project, ctx->rel_path, ctx->language);
    if (!s.module_qn) {
        return NULL;
    }
    s.module_len = strlen(s.module_qn);
    s.lexical = ts_node_has_error(ctx->root);
    jv_defs_index(&s);
    if (s.lexical) {
        jv_scan_text(&s);
    }
    jv_puts(&s.sb, CBM_DOCLINK_JVM_SCOPE_TAG "\nL\t");
    jv_putc(&s.sb, kotlin ? 'k' : 'j');
    jv_putc(&s.sb, '\n');
    jv_put_package(&s);
    jv_frame_t top = {.owner = JV_NO_OWNER, .path = "", .name = ""};
    if (kotlin) {
        jv_kotlin_container(&s, ctx->root, &top, 0);
    } else {
        jv_java_container(&s, ctx->root, &top, 0);
    }
    if (s.lexical) {
        jv_finish_lexical(&s);
    }
    if (s.failed || s.sb.failed || !s.sb.buf) {
        return NULL;
    }
    return s.sb.buf;
}

const char *cbm_doclink_java_scan_scope(CBMExtractCtx *ctx) {
    return jv_scan_scope(ctx, false);
}

const char *cbm_doclink_kotlin_scan_scope(CBMExtractCtx *ctx) {
    return jv_scan_scope(ctx, true);
}

/* ── Persisted form ──────────────────────────────────────────────── */

/* Fields (0-based, the tag is field 0) that hold line numbers. */
static bool jv_line_field(char tag, int field) {
    switch (tag) {
    case 'T':
    case 'X':
        return field == 1 || field == 2;
    case 'M':
        return field == 1;
    default:
        return false;
    }
}

/* The parameter names: the last field of a type and of a member record. They
 * serve the file's own references only. */
enum { JV_T_PNAMES = 9, JV_M_PNAMES = 11 };

static bool jv_own_field(char tag, int field) {
    return (tag == 'T' && field == JV_T_PNAMES) || (tag == 'M' && field == JV_M_PNAMES);
}

char *cbm_doclink_jvm_portable_scope(const char *scope) {
    size_t n = strlen(scope);
    char *out = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    const char *p = scope;
    while (*p) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        char tag = p[0];
        int field = 0;
        size_t i = 0;
        while (i < len) {
            size_t fend = i;
            while (fend < len && p[fend] != '\t') {
                fend++;
            }
            if (jv_line_field(tag, field)) {
                out[w++] = '0';
            } else if (!jv_own_field(tag, field)) {
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
            p = nl + SKIP_ONE;
        } else {
            p += len;
        }
    }
    out[w] = '\0';
    return out;
}
