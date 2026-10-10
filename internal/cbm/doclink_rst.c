/*
 * doclink_rst.c — reStructuredText documents: their sections, and the code
 * their text names (the extraction half; src/pipeline/doc_links_rst.c
 * resolves).
 *
 * Structure: the field test's line model (H3 RstDoc) with CPython's str
 * semantics -- tabs expanded to 8 columns, str.strip / str.split whitespace
 * and re's \s and \w from the Unicode tables the PDF scanner uses. Every line
 * gets a kind: title, adornment, prose, directive head / argument / option,
 * literal block (after `::` or in a code directive), comment, hyperlink
 * target. Titles are found by adornment style (over+under or under only; the
 * level is the style's first appearance). Each title becomes a Section
 * definition: name the title, docstring the text up to the next title, lines
 * from its title to the line before the next one. Text before the first title
 * belongs to the file.
 *
 * References become tokens whose `raw` is the reference AS WRITTEN followed by
 * TAB-separated fields the resolver reads (a doc_link_unresolved row shows the
 * written form first; whitespace inside a field is collapsed, so no field
 * holds a TAB):
 *   role            :py:class:`x`, :func:`~a.b`, :c:type:`t`, :source:`p`
 *                   written, role, content, module, class, C namespace
 *   object          .. py:class:: sig, .. function:: sig (one per signature)
 *                   written, directive, signature, module, class, C namespace,
 *                   the Python full name in its class ("" when Sphinx's
 *                   signature grammar does not read it)
 *   autodoc         .. autoclass:: a.b
 *                   written, directive, argument, module, class
 *   include         .. include:: / .. kernel-include:: of a code file
 *                   written, directive, path
 *   literalinclude  .. literalinclude:: path
 *                   written, path, :lines:, :pyobject:, "1" when a
 *                   :start-after: / :end-before: / :start-at: / :end-at:
 *                   bounds it by text (the resolver binds the file then)
 *   kernel_doc      .. kernel-doc:: path, and each name of :identifiers: /
 *                   :functions: -- written, path, name ("" for the file)
 *   code_path,      an inline literal ``...`` that names a path or a qualified
 *   code_name       name (the Markdown code-span classifier); raw is its text
 * A role or directive without a domain is taken for Python's when this file
 * sets contexts (`module`, `currentmodule`, class directives: Sphinx's
 * default domain); which domain it belongs to is the resolver's call (the
 * documentation set's conf.py). Roles of no code domain are tokens only when
 * their content is a path (an extlink such as :source:`django/db/x.py`).
 */
#include "doclink.h"

#include "arena.h"
#include "cbm.h"
#include "helpers.h"
#include "pdf/pdf_unicode.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
    RST_TAB = 8,
    RST_SHORT_ADORN = 4, /* an underline this long fits any title; a transition */
    RST_BODY_MAX = 500,  /* a section's docstring (Markdown's MAX_COMMENT_LEN) */
    RST_SPAN_MAX = 150,  /* an inline literal the span classifier reads (MD_SPAN_MAX) */
    RST_INIT_CAP = 16,
    RST_REPLACEMENT = 0xFFFD,
    RST_ASCII_END = 0x80,
    RST_LEAD2_MIN = 0xC2, /* UTF-8 lead bytes (no overlong two-byte form) */
    RST_LEAD3_MIN = 0xE0,
    RST_LEAD3_MAX = 0xEF,
    RST_LEAD4_MIN = 0xF0,
    RST_LEAD4_MAX = 0xF4,
    RST_NEED3 = 2,
    RST_NEED4 = 3,
    RST_BULLET = 0x2022,
    RST_SIG_ARROW = 2, /* "->" */
};

#define RST_SEP '\t'

typedef enum {
    K_BLANK = 0,
    K_TITLE,
    K_ADORN,
    K_DHEAD,
    K_DHEAD_PROSE,
    K_DARG,
    K_DARG_PROSE,
    K_DOPT,
    K_LITERAL,
    K_PLIT,
    K_SKIP,
    K_TARGET,
    K_COMMENT,
    K_PROSE,
} rst_kind_t;

typedef struct {
    char *s;    /* the line, tabs expanded, NUL-terminated */
    int len;    /* bytes */
    int indent; /* leading spaces (H3 indent_of) */
    bool blank; /* nothing but whitespace */
} rst_line_t;

typedef struct {
    const char *key;
    const char *val;
} rst_opt_t;

typedef struct {
    int line;
    int indent;
    const char *name;  /* as written, lower-cased, with its domain */
    const char *dom;   /* "py", "c", "std" or the written domain */
    const char *dname; /* without the domain */
    const char **args;
    int nargs;
    rst_opt_t *opts;
    int nopts;
    int cs; /* content lines [cs, ce) */
    int ce;
} rst_dir_t;

typedef struct {
    int first; /* the title block's first line (an overline, or the title) */
    int idx;   /* the title text's line */
    int next;  /* the line after the title block */
    const char *text;
    const char *qn;
    bool shared_qn; /* another title of the file has the same QN: the file is the source */
} rst_title_t;

typedef struct {
    int line; /* 0-based */
    int col;  /* byte offset in its line; directives use their indent */
    int syntax;
    const char *raw;
} rst_tok_t;

typedef struct {
    CBMExtractCtx *ctx;
    CBMArena *scratch;
    rst_line_t *L;
    int n;
    unsigned char *K;
    rst_dir_t *dirs;
    int ndirs;
    int cap_dirs;
    rst_title_t *titles;
    int ntitles;
    int cap_titles;
    char styles[2 * RST_INIT_CAP * RST_INIT_CAP]; /* (char, overline) pairs in first-seen order */
    int nstyles;
    const char **mod_at;
    const char **cls_at;
    const char **ns_at;
    rst_tok_t *toks;
    int ntoks;
    int cap_toks;
    char *span_buf;
    bool failed;
} rst_doc_t;

/* ── Unicode, as CPython's str and re see it ─────────────────────── */

/* The code point at s[i] and its length; a malformed byte is U+FFFD of one
 * byte (decode(errors='replace')). */
static uint32_t rst_cp(const char *s, int len, int i, int *n) {
    unsigned char c = (unsigned char)s[i];
    if (c < RST_ASCII_END) {
        *n = 1;
        return c;
    }
    int need = 0; /* continuation bytes of a well-formed lead byte */
    if (c >= RST_LEAD4_MIN && c <= RST_LEAD4_MAX) {
        need = RST_NEED4;
    } else if (c >= RST_LEAD3_MIN && c <= RST_LEAD3_MAX) {
        need = RST_NEED3;
    } else if (c >= RST_LEAD2_MIN && c < RST_LEAD3_MIN) {
        need = 1;
    }
    if (need == 0 || i + need >= len) {
        *n = 1;
        return RST_REPLACEMENT;
    }
    uint32_t cp = c & (0x3F >> need);
    for (int k = 1; k <= need; k++) {
        unsigned char cc = (unsigned char)s[i + k];
        if ((cc & 0xC0) != 0x80) {
            *n = 1;
            return RST_REPLACEMENT;
        }
        cp = (cp << 6) | (cc & 0x3F);
    }
    static const uint32_t min_cp[] = {0, 0x80, 0x800, 0x10000};
    if (cp < min_cp[need] || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
        *n = 1;
        return RST_REPLACEMENT;
    }
    *n = need + 1;
    return cp;
}

static bool rst_space_cp(uint32_t cp) {
    if (cp < RST_ASCII_END) {
        return cp == ' ' || (cp >= '\t' && cp <= '\r') || (cp >= 0x1C && cp <= 0x1F);
    }
    return pdf_u_in(PDF_U_SPACE, PDF_U_SPACE_COUNT, cp);
}

static bool rst_word_cp(uint32_t cp) {
    if (cp < RST_ASCII_END) {
        return (cp >= 'a' && cp <= 'z') || (cp >= 'A' && cp <= 'Z') || (cp >= '0' && cp <= '9') ||
               cp == '_';
    }
    return pdf_u_in(PDF_U_WORD, PDF_U_WORD_COUNT, cp);
}

static bool rst_alpha_ascii(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

static bool rst_digit(unsigned char c) {
    return c >= '0' && c <= '9';
}

/* Whitespace code points from s[i]: the index after them. */
static int rst_skip_ws(const char *s, int len, int i) {
    while (i < len) {
        int n;
        if (!rst_space_cp(rst_cp(s, len, i, &n))) {
            break;
        }
        i += n;
    }
    return i;
}

/* str.strip(): [*a, *b) without leading and trailing whitespace. */
static void rst_strip(const char *s, int len, int *a, int *b) {
    int i = rst_skip_ws(s, len, 0);
    int end = i;
    int k = i;
    while (k < len) {
        int n;
        uint32_t cp = rst_cp(s, len, k, &n);
        k += n;
        if (!rst_space_cp(cp)) {
            end = k;
        }
    }
    *a = i;
    *b = end;
}

/* Code points in s[a, b). */
static int rst_cps(const char *s, int a, int b) {
    int count = 0;
    while (a < b) {
        int n;
        (void)rst_cp(s, b, a, &n);
        a += n;
        count++;
    }
    return count;
}

/* ' '.join(s.split()), into the arena. */
static char *rst_collapse(CBMArena *a, const char *s, int len) {
    char *out = (char *)cbm_arena_alloc(a, (size_t)len + 1);
    if (!out) {
        return NULL;
    }
    int w = 0;
    int i = 0;
    while (i < len) {
        int n;
        uint32_t cp = rst_cp(s, len, i, &n);
        if (rst_space_cp(cp)) {
            i += n;
            continue;
        }
        if (w > 0) {
            out[w++] = ' ';
        }
        while (i < len) {
            cp = rst_cp(s, len, i, &n);
            if (rst_space_cp(cp)) {
                break;
            }
            memcpy(out + w, s + i, (size_t)n);
            w += n;
            i += n;
        }
    }
    out[w] = '\0';
    return out;
}

/* ── Growing arrays in the scratch arena ─────────────────────────── */

static void *rst_grow(rst_doc_t *d, void *items, int count, int *cap, size_t size) {
    if (count < *cap) {
        return items;
    }
    int ncap = *cap ? *cap * 2 : RST_INIT_CAP;
    void *neu = cbm_arena_alloc(d->scratch, (size_t)ncap * size);
    if (!neu) {
        d->failed = true;
        return NULL;
    }
    if (items && count) {
        memcpy(neu, items, (size_t)count * size);
    }
    *cap = ncap;
    return neu;
}

/* ── Lines ───────────────────────────────────────────────────────── */

/* text.expandtabs(8).split('\n'): the column restarts after '\r' and '\n'. */
static bool rst_lines(rst_doc_t *d, const char *src, int n) {
    int count = 1;
    for (int i = 0; i < n; i++) {
        count += src[i] == '\n';
    }
    d->L = (rst_line_t *)cbm_arena_alloc(d->scratch, (size_t)count * sizeof(*d->L));
    d->K = (unsigned char *)cbm_arena_alloc(d->scratch, (size_t)count);
    if (!d->L || !d->K) {
        return false;
    }
    memset(d->K, 0, (size_t)count);
    int pos = 0;
    for (int li = 0; li < count; li++) {
        const char *nl = memchr(src + pos, '\n', (size_t)(n - pos));
        int end = nl ? (int)(nl - src) : n;
        int tabs = 0;
        for (int i = pos; i < end; i++) {
            tabs += src[i] == '\t';
        }
        int cap = (end - pos) + tabs * RST_TAB + 1;
        char *out = (char *)cbm_arena_alloc(d->scratch, (size_t)cap);
        if (!out) {
            return false;
        }
        int w = 0;
        int col = 0;
        int i = pos;
        while (i < end) {
            int cn;
            uint32_t cp = rst_cp(src, end, i, &cn);
            if (cp == '\t') {
                int fill = RST_TAB - (col % RST_TAB);
                memset(out + w, ' ', (size_t)fill);
                w += fill;
                col += fill;
            } else {
                memcpy(out + w, src + i, (size_t)cn);
                w += cn;
                col = cp == '\r' ? 0 : col + 1;
            }
            i += cn;
        }
        out[w] = '\0';
        rst_line_t *ln = &d->L[li];
        ln->s = out;
        ln->len = w;
        int sp = 0;
        while (sp < w && out[sp] == ' ') {
            sp++;
        }
        ln->indent = sp;
        int a;
        int b;
        rst_strip(out, w, &a, &b);
        ln->blank = a >= b;
        pos = end + 1;
    }
    d->n = count;
    return true;
}

/* ── Line patterns (H3's regexes) ────────────────────────────────── */

static bool rst_punct(unsigned char c) {
    return (c >= '!' && c <= '/') || (c >= ':' && c <= '@') || (c >= '[' && c <= '`') ||
           (c >= '{' && c <= '~');
}

/* ADORN_RE ^([!-/:-@\[-`{-~])\1*\s*$ */
static bool rst_adorn(const char *s, int len) {
    if (len == 0 || !rst_punct((unsigned char)s[0])) {
        return false;
    }
    int i = 1;
    while (i < len && s[i] == s[0]) {
        i++;
    }
    return rst_skip_ws(s, len, i) == len;
}

/* s.rstrip() length. */
static int rst_rlen(const char *s, int len) {
    int a;
    int b;
    rst_strip(s, len, &a, &b);
    return a >= b ? 0 : b;
}

static bool rst_style_index(rst_doc_t *d, char ch, bool over, int *level) {
    for (int i = 0; i < d->nstyles; i++) {
        if (d->styles[2 * i] == ch && d->styles[(2 * i) + 1] == (char)over) {
            *level = i;
            return true;
        }
    }
    if ((size_t)(2 * (d->nstyles + 1)) > sizeof(d->styles)) {
        return false;
    }
    d->styles[2 * d->nstyles] = ch;
    d->styles[(2 * d->nstyles) + 1] = (char)over;
    *level = d->nstyles++;
    return true;
}

/* H3 _title_at: the title's line, its style and the line after the block. */
static bool rst_title_at(const rst_doc_t *d, int i, int *tidx, char *ch, bool *over, int *next) {
    const rst_line_t *L = d->L;
    int n = d->n;
    int sl = rst_rlen(L[i].s, L[i].len);
    int sa;
    int sb;
    rst_strip(L[i].s, sl, &sa, &sb);
    if (rst_adorn(L[i].s, sl)) {
        if (sb - sa == 2 && L[i].s[sa] == ':' && L[i].s[sa + 1] == ':') {
            return false;
        }
        if (i + 2 < n) {
            const rst_line_t *t = &L[i + 1];
            int ul = rst_rlen(L[i + 2].s, L[i + 2].len);
            const char *u = L[i + 2].s;
            if (!t->blank && !rst_adorn(t->s, t->len) && rst_adorn(u, ul) && u[0] == L[i].s[sa]) {
                *tidx = i + 1;
                *ch = L[i].s[sa];
                *over = true;
                *next = i + 3;
                return true;
            }
        }
        return false;
    }
    if (i + 1 < n && !(sl >= 2 && L[i].s[0] == '.' && L[i].s[1] == '.')) {
        const char *u = L[i + 1].s;
        int ul = rst_rlen(u, L[i + 1].len);
        if (ul > 0 && !rst_space_cp((unsigned char)u[0]) && rst_adorn(u, ul)) {
            int ua;
            int ub;
            rst_strip(u, ul, &ua, &ub);
            bool dcolon = ub - ua == 2 && u[ua] == ':' && u[ua + 1] == ':';
            int ulen = ub - ua; /* ASCII punctuation: bytes are code points */
            if (!dcolon && (ulen >= rst_cps(L[i].s, sa, sb) || ulen >= RST_SHORT_ADORN)) {
                *tidx = i;
                *ch = u[ua];
                *over = false;
                *next = i + 2;
                return true;
            }
        }
    }
    return false;
}

static void rst_mark(rst_doc_t *d, int a, int b, rst_kind_t kind) {
    for (int k = a; k < b; k++) {
        d->K[k] = (unsigned char)(d->L[k].blank ? K_BLANK : kind);
    }
}

static int rst_block_end(const rst_doc_t *d, int j, int ind) {
    int k = j;
    while (k < d->n && (d->L[k].blank || d->L[k].indent > ind)) {
        k++;
    }
    return k;
}

static bool rst_name_char(const char *s, int len, int i, int *n) {
    uint32_t cp = rst_cp(s, len, i, n);
    return cp == ':' || cp == '.' || cp == '+' || cp == '-' || rst_word_cp(cp);
}

/* DIRECTIVE_RE: ^(\s*)\.\.\s+(?:\|[^|]+\|\s+)?([A-Za-z0-9][\w:.+-]*?)::(?:\s+(.*?))?\s*$
 * Fills the indent (code points), the name span and the first argument
 * span (stripped). */
static bool rst_directive_match(const char *s, int len, int *ind, int *na, int *nb, int *aa,
                                int *ab) {
    int p = rst_skip_ws(s, len, 0);
    *ind = rst_cps(s, 0, p);
    if (p + 2 > len || s[p] != '.' || s[p + 1] != '.') {
        return false;
    }
    int q = rst_skip_ws(s, len, p + 2);
    if (q == p + 2) {
        return false;
    }
    p = q;
    if (p < len && s[p] == '|') {
        const char *bar = memchr(s + p + 1, '|', (size_t)(len - p - 1));
        if (bar && bar > s + p + 1) {
            int after = (int)(bar - s) + 1;
            int w = rst_skip_ws(s, len, after);
            if (w > after) {
                p = w;
            }
        }
    }
    if (p >= len || !(rst_alpha_ascii((unsigned char)s[p]) || rst_digit((unsigned char)s[p]))) {
        return false;
    }
    int e = p + 1;
    for (;;) {
        if (e + 2 <= len && s[e] == ':' && s[e + 1] == ':') {
            int t = e + 2;
            int w = rst_skip_ws(s, len, t);
            if (t == len || w > t) {
                *na = p;
                *nb = e;
                int a;
                int b;
                rst_strip(s + w, len - w, &a, &b);
                *aa = w + a;
                *ab = w + (b > a ? b : a);
                return true;
            }
        }
        int cn;
        if (e >= len || !rst_name_char(s, len, e, &cn)) {
            return false;
        }
        e += cn;
    }
}

/* EXPLICIT_RE ^(\s*)\.\.(?:\s|$) */
static bool rst_explicit_match(const char *s, int len) {
    int p = rst_skip_ws(s, len, 0);
    if (p + 2 > len || s[p] != '.' || s[p + 1] != '.') {
        return false;
    }
    if (p + 2 == len) {
        return true;
    }
    int n;
    return rst_space_cp(rst_cp(s, len, p + 2, &n));
}

/* OPTION_LINE_RE ^(\s*):([A-Za-z][\w-]*):(?:\s+(.*?))?\s*$ */
static bool rst_option_match(const char *s, int len, int *ka, int *kb, int *va, int *vb) {
    int p = rst_skip_ws(s, len, 0);
    if (p >= len || s[p] != ':' || p + 1 >= len || !rst_alpha_ascii((unsigned char)s[p + 1])) {
        return false;
    }
    int k = p + 2;
    while (k < len) {
        int n;
        uint32_t cp = rst_cp(s, len, k, &n);
        if (cp != '-' && !rst_word_cp(cp)) {
            break;
        }
        k += n;
    }
    if (k >= len || s[k] != ':') {
        return false;
    }
    int t = k + 1;
    int w = rst_skip_ws(s, len, t);
    if (t != len && w == t) {
        return false;
    }
    *ka = p + 1;
    *kb = k;
    int a;
    int b;
    rst_strip(s + w, len - w, &a, &b);
    *va = w + a;
    *vb = w + (b > a ? b : a);
    return true;
}

static bool rst_roman(unsigned char c) {
    return strchr("ivxlcdmIVXLCDM", c) != NULL && c != '\0';
}

/* LIST_MARKER_RE: the match length in code points, or -1. */
static int rst_list_marker(const char *s, int len) {
    int p = rst_skip_ws(s, len, 0);
    if (p >= len) {
        return -1;
    }
    int n;
    uint32_t cp = rst_cp(s, len, p, &n);
    int q = -1;
    if (cp == '-' || cp == '*' || cp == '+' || cp == RST_BULLET) {
        q = p + n;
    } else {
        int r = p + (s[p] == '(');
        int e = -1;
        if (r < len && rst_digit((unsigned char)s[r])) {
            e = r;
            while (e < len && rst_digit((unsigned char)s[e])) {
                e++;
            }
            if (!(e < len && (s[e] == '.' || s[e] == ')'))) {
                e = -1;
            }
        }
        if (e < 0 && r + 1 < len && rst_alpha_ascii((unsigned char)s[r]) &&
            (s[r + 1] == '.' || s[r + 1] == ')')) {
            e = r + 1;
        }
        if (e < 0 && r + 1 < len && s[r] == '#' && (s[r + 1] == '.' || s[r + 1] == ')')) {
            e = r + 1;
        }
        if (e < 0 && r < len && rst_roman((unsigned char)s[r])) {
            e = r;
            while (e < len && rst_roman((unsigned char)s[e])) {
                e++;
            }
            if (!(e < len && (s[e] == '.' || s[e] == ')'))) {
                e = -1;
            }
        }
        if (e >= 0) {
            q = e + 1;
        }
    }
    if (q < 0) {
        return -1;
    }
    int w = rst_skip_ws(s, len, q);
    if (w == q || w >= len) {
        return -1;
    }
    return rst_cps(s, 0, w);
}

/* ── Block structure (H3 _scan) ──────────────────────────────────── */

static const char *const PY_DOMAIN_OBJ[] = {
    "class",     "exception", "method",        "classmethod", "staticmethod",
    "attribute", "property",  "function",      "decorator",   "decoratormethod",
    "data",      "module",    "currentmodule", NULL};
static const char *const LITERAL_CONTENT[] = {
    "code-block", "code",           "sourcecode", "literalinclude", "raw",         "math",
    "graphviz",   "digraph",        "graph",      "productionlist", "console",     "doctest",
    "testcode",   "testoutput",     "testsetup",  "testcleanup",    "highlight",   "kernel-render",
    "prompt",     "kernel-include", "include",    "kernel-abi",     "kernel-feat", "uml",
    "ipython",    "mermaid",        NULL};
static const char *const SKIP_CONTENT[] = {"toctree", "index", "tabularcolumns", NULL};
static const char *const ARG_PROSE[] = {"note",           "warning",    "tip",
                                        "hint",           "important",  "caution",
                                        "attention",      "danger",     "error",
                                        "seealso",        "admonition", "versionadded",
                                        "versionchanged", "deprecated", "versionremoved",
                                        "rubric",         "topic",      "sidebar",
                                        "replace",        "centered",   NULL};
static const char *const PY_OBJ[] = {
    "class",     "exception", "method",   "classmethod", "staticmethod",
    "attribute", "property",  "function", "decorator",   "decoratormethod",
    "data",      "module",    NULL};
static const char *const C_OBJ[] = {"function", "macro",      "type",   "struct", "union",
                                    "enum",     "enumerator", "member", "var",    NULL};
static const char *const AUTODOC[] = {
    "automodule",    "autoclass", "autofunction",  "automethod",   "autoattribute",
    "autoexception", "autodata",  "autodecorator", "autoproperty", NULL};
static const char *const PY_ROLES[] = {"func", "class", "meth",  "attr", "mod", "exc",
                                       "data", "obj",   "const", "deco", NULL};
static const char *const C_ROLES[] = {"func", "macro",  "struct", "union", "enum", "enumerator",
                                      "type", "member", "data",   "var",   NULL};
/* include / kernel-include of these is code (H3 CODE_EXT) */
static const char *const CODE_EXT[] = {".c",   ".h",     ".s",    ".py",   ".sh",  ".rs",   ".pl",
                                       ".awk", ".cpp",   ".cc",   ".hpp",  ".go",  ".js",   ".ts",
                                       ".lds", ".dts",   ".dtsi", ".yaml", ".yml", ".json", ".toml",
                                       ".mk",  ".cmake", ".bpf",  ".tc",   NULL};

static bool rst_in(const char *s, const char *const *list) {
    for (int i = 0; s && list[i]; i++) {
        if (strcmp(s, list[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* H3 split_domain with Python as the primary domain. */
static void rst_split_domain(rst_doc_t *d, const char *name, const char *const *py_members,
                             const char **dom, const char **rest) {
    const char *colon = strchr(name, ':');
    if (colon) {
        *dom = cbm_arena_strndup(d->scratch, name, (size_t)(colon - name));
        *rest = colon + 1;
        if (!*dom) {
            d->failed = true;
            *dom = "";
        }
        return;
    }
    *dom = rst_in(name, py_members) ? "py" : "std";
    *rest = name;
}

static char *rst_dup_stripped(rst_doc_t *d, const char *s, int len) {
    int a;
    int b;
    rst_strip(s, len, &a, &b);
    char *out = cbm_arena_strndup(d->scratch, s + a, (size_t)(b > a ? b - a : 0));
    if (!out) {
        d->failed = true;
    }
    return out;
}

static int rst_directive(rst_doc_t *d, int i, int ind, int na, int nb, int aa, int ab) {
    rst_line_t *L = d->L;
    int n = d->n;
    char *name = cbm_arena_strndup(d->scratch, L[i].s + na, (size_t)(nb - na));
    if (!name) {
        d->failed = true;
        return n;
    }
    for (char *c = name; *c; c++) {
        if (*c >= 'A' && *c <= 'Z') {
            *c = (char)(*c + ('a' - 'A'));
        }
    }
    rst_dir_t dir = {.line = i, .indent = ind, .name = name};
    rst_split_domain(d, name, PY_DOMAIN_OBJ, &dir.dom, &dir.dname);
    bool prose_arg = rst_in(dir.dname, ARG_PROSE);
    d->K[i] = prose_arg ? K_DHEAD_PROSE : K_DHEAD;
    int cap_args = 0;
    if (ab > aa) {
        dir.args = (const char **)rst_grow(d, NULL, 0, &cap_args, sizeof(char *));
        if (!dir.args) {
            return n;
        }
        dir.args[dir.nargs] = rst_dup_stripped(d, L[i].s + aa, ab - aa);
        dir.nargs++;
    }
    int j = i + 1;
    int ka;
    int kb;
    int va;
    int vb;
    while (j < n && !L[j].blank && L[j].indent > ind &&
           !rst_option_match(L[j].s, L[j].len, &ka, &kb, &va, &vb)) {
        dir.args = (const char **)rst_grow(d, dir.args, dir.nargs, &cap_args, sizeof(char *));
        if (!dir.args) {
            return n;
        }
        dir.args[dir.nargs++] = rst_dup_stripped(d, L[j].s, L[j].len);
        d->K[j] = prose_arg ? K_DARG_PROSE : K_DARG;
        j++;
    }
    int cap_opts = 0;
    while (j < n && !L[j].blank && L[j].indent > ind &&
           rst_option_match(L[j].s, L[j].len, &ka, &kb, &va, &vb)) {
        char *key = cbm_arena_strndup(d->scratch, L[j].s + ka, (size_t)(kb - ka));
        char *val = cbm_arena_strndup(d->scratch, L[j].s + va, (size_t)(vb - va));
        int oind = L[j].indent;
        d->K[j] = K_DOPT;
        j++;
        while (j < n && !L[j].blank && L[j].indent > oind &&
               !rst_option_match(L[j].s, L[j].len, &ka, &kb, &va, &vb)) {
            char *more = rst_dup_stripped(d, L[j].s, L[j].len);
            if (val && more) {
                val = cbm_arena_sprintf(d->scratch, "%s%s%s", val, val[0] && more[0] ? " " : "",
                                        more);
            }
            d->K[j] = K_DOPT;
            j++;
        }
        dir.opts = (rst_opt_t *)rst_grow(d, dir.opts, dir.nopts, &cap_opts, sizeof(rst_opt_t));
        if (!key || !val || !dir.opts) {
            d->failed = true;
            return n;
        }
        /* options[key] = val: a repeated key keeps its first place, the last value */
        bool found = false;
        for (int k = 0; k < dir.nopts; k++) {
            if (strcmp(dir.opts[k].key, key) == 0) {
                dir.opts[k].val = val;
                found = true;
            }
        }
        if (!found) {
            dir.opts[dir.nopts++] = (rst_opt_t){key, val};
        }
    }
    dir.cs = j;
    dir.ce = rst_block_end(d, j, ind);
    d->dirs = (rst_dir_t *)rst_grow(d, d->dirs, d->ndirs, &d->cap_dirs, sizeof(rst_dir_t));
    if (!d->dirs) {
        return n;
    }
    d->dirs[d->ndirs++] = dir;
    if (rst_in(dir.dname, LITERAL_CONTENT)) {
        rst_mark(d, dir.cs, dir.ce, K_LITERAL);
        return dir.ce;
    }
    if (strcmp(dir.dname, "parsed-literal") == 0) {
        rst_mark(d, dir.cs, dir.ce, K_PLIT);
        return dir.ce;
    }
    if (rst_in(dir.dname, SKIP_CONTENT)) {
        rst_mark(d, dir.cs, dir.ce, K_SKIP);
        return dir.ce;
    }
    return dir.cs;
}

static int rst_explicit(rst_doc_t *d, int i, int ind) {
    rst_line_t *L = d->L;
    int a;
    int b;
    rst_strip(L[i].s, L[i].len, &a, &b);
    const char *s = L[i].s + a;
    int sl = b - a;
    if ((sl >= 4 && strncmp(s, ".. _", 4) == 0) || (sl >= 3 && strncmp(s, "__ ", 3) == 0)) {
        d->K[i] = K_TARGET;
        int j = i + 1;
        while (j < d->n && !L[j].blank && L[j].indent > ind) {
            d->K[j] = K_TARGET;
            j++;
        }
        return j;
    }
    /* \.\.\s+\[[^\]]+\] : a footnote or citation */
    int p = rst_skip_ws(s, sl, 2);
    if (p > 2 && p < sl && s[p] == '[') {
        const char *close = memchr(s + p + 1, ']', (size_t)(sl - p - 1));
        if (close && close > s + p + 1) {
            d->K[i] = K_PROSE;
            return i + 1;
        }
    }
    if (sl == 2 && (i + 1 >= d->n || L[i + 1].blank)) {
        d->K[i] = K_COMMENT;
        return i + 1;
    }
    int j = rst_block_end(d, i + 1, ind);
    rst_mark(d, i, j, K_COMMENT);
    return j;
}

static void rst_scan(rst_doc_t *d) {
    rst_line_t *L = d->L;
    int n = d->n;
    int i = 0;
    int para_indent = 0;
    int literal_pending = -1;
    while (i < n && !d->failed) {
        if (L[i].blank) {
            d->K[i] = K_BLANK;
            i++;
            continue;
        }
        bool prev_blank = i == 0 || L[i - 1].blank;
        int ind = L[i].indent;
        if (literal_pending >= 0) {
            int lp = literal_pending;
            literal_pending = -1;
            if (prev_blank && ind > lp) {
                int j = rst_block_end(d, i, lp);
                rst_mark(d, i, j, K_LITERAL);
                i = j;
                continue;
            }
        }
        if (ind == 0 && prev_blank) {
            int tidx;
            char ch;
            bool over;
            int next;
            if (rst_title_at(d, i, &tidx, &ch, &over, &next)) {
                int level;
                d->titles = (rst_title_t *)rst_grow(d, d->titles, d->ntitles, &d->cap_titles,
                                                    sizeof(rst_title_t));
                char *text = rst_collapse(d->scratch, L[tidx].s, L[tidx].len);
                if (!d->titles || !text || !rst_style_index(d, ch, over, &level)) {
                    d->failed = true;
                    return;
                }
                (void)level;
                d->titles[d->ntitles++] =
                    (rst_title_t){.first = i, .idx = tidx, .next = next, .text = text};
                rst_mark(d, i, next, K_ADORN);
                d->K[tidx] = K_TITLE;
                i = next;
                continue;
            }
            int sa;
            int sb;
            rst_strip(L[i].s, L[i].len, &sa, &sb);
            if (rst_adorn(L[i].s, L[i].len) && rst_cps(L[i].s, sa, sb) >= RST_SHORT_ADORN &&
                (i + 1 >= n || L[i + 1].blank)) {
                d->K[i] = K_ADORN;
                i++;
                continue;
            }
        }
        int dind;
        int na;
        int nb;
        int aa;
        int ab;
        if (rst_directive_match(L[i].s, L[i].len, &dind, &na, &nb, &aa, &ab)) {
            i = rst_directive(d, i, dind, na, nb, aa, ab);
            continue;
        }
        if (rst_explicit_match(L[i].s, L[i].len)) {
            i = rst_explicit(d, i, ind);
            continue;
        }
        d->K[i] = K_PROSE;
        if (prev_blank) {
            int mm = rst_list_marker(L[i].s, L[i].len);
            para_indent = mm >= 0 ? mm : ind;
        }
        int sa;
        int sb;
        rst_strip(L[i].s, L[i].len, &sa, &sb);
        if (sb - sa >= 2 && L[i].s[sb - 1] == ':' && L[i].s[sb - 2] == ':') {
            literal_pending = para_indent;
        }
        i++;
    }
}

/* ── Python signatures (Sphinx py_sig_re, H3 join_sig_lines / py_fullname) ── */

/* One signature per argument line; a trailing backslash continues it. */
static int rst_join_sigs(rst_doc_t *d, const rst_dir_t *dir, const char ***out) {
    const char **sigs =
        (const char **)cbm_arena_alloc(d->scratch, (size_t)(dir->nargs + 1) * sizeof(char *));
    if (!sigs) {
        d->failed = true;
        return 0;
    }
    int ns = 0;
    const char *cur = "";
    bool open = false;
    for (int k = 0; k < dir->nargs; k++) {
        const char *a = dir->args[k];
        size_t al = strlen(a);
        if (al > 0 && a[al - 1] == '\\') {
            cur = cbm_arena_sprintf(d->scratch, "%s%.*s ", cur, (int)(al - 1), a);
            open = true;
        } else {
            sigs[ns++] = cbm_arena_sprintf(d->scratch, "%s%s", cur, a);
            cur = "";
            open = false;
        }
        if (!cur || (ns > 0 && !sigs[ns - 1])) {
            d->failed = true;
            return 0;
        }
    }
    if (open) {
        sigs[ns++] = cur;
    }
    *out = sigs;
    return ns;
}

/* After the name: \s* (\[ .* \])? \s* ( \( .* \) (\s* -> .*)? )? $ */
static bool rst_sig_paren_ok(const char *s, int len, int i) {
    if (i == len) {
        return true;
    }
    if (s[i] != '(') {
        return false;
    }
    for (int q = len - 1; q > i; q--) {
        if (s[q] != ')') {
            continue;
        }
        if (q + 1 == len) {
            return true;
        }
        int w = rst_skip_ws(s, len, q + 1);
        if (w + RST_SIG_ARROW <= len && s[w] == '-' && s[w + 1] == '>') {
            return true;
        }
    }
    return false;
}

static bool rst_sig_rest_ok(const char *s, int len, int i) {
    i = rst_skip_ws(s, len, i);
    if (i == len) {
        return true;
    }
    if (s[i] == '[') {
        for (int q = len - 1; q > i; q--) {
            if (s[q] == ']' && rst_sig_paren_ok(s, len, rst_skip_ws(s, len, q + 1))) {
                return true;
            }
        }
        return false;
    }
    return rst_sig_paren_ok(s, len, i);
}

/* PY_SIG_RE on sig.strip(): the prefix ("a.b." or "") and the name. */
static bool rst_py_sig(rst_doc_t *d, const char *sig, const char **prefix, const char **name) {
    int a;
    int b;
    rst_strip(sig, (int)strlen(sig), &a, &b);
    const char *s = sig + a;
    int len = b > a ? b - a : 0;
    int r = 0;
    int last_dot = -1;
    while (r < len) {
        int n;
        uint32_t cp = rst_cp(s, len, r, &n);
        if (cp == '.') {
            last_dot = r;
        } else if (!rst_word_cp(cp)) {
            break;
        }
        r += n;
    }
    int ns = last_dot + 1;
    if (ns >= r || !rst_sig_rest_ok(s, len, r)) {
        return false;
    }
    *prefix = cbm_arena_strndup(d->scratch, s, (size_t)ns);
    *name = cbm_arena_strndup(d->scratch, s + ns, (size_t)(r - ns));
    if (!*prefix || !*name) {
        d->failed = true;
        return false;
    }
    return true;
}

/* Sphinx PyObject.handle_signature's fullname, and the remaining prefix. */
static void rst_py_fullname(rst_doc_t *d, const char *prefix, const char *name, const char *cls,
                            const char **full, const char **rem) {
    if (cls && cls[0]) {
        size_t cl = strlen(cls);
        if (prefix[0] &&
            (strcmp(prefix, cls) == 0 || (strncmp(prefix, cls, cl) == 0 && prefix[cl] == '.'))) {
            const char *r = prefix + cl;
            while (*r == '.') {
                r++;
            }
            *full = cbm_arena_sprintf(d->scratch, "%s%s", prefix, name);
            *rem = r;
        } else if (prefix[0]) {
            *full = cbm_arena_sprintf(d->scratch, "%s.%s%s", cls, prefix, name);
            *rem = prefix;
        } else {
            *full = cbm_arena_sprintf(d->scratch, "%s.%s", cls, name);
            *rem = "";
        }
    } else {
        *full = cbm_arena_sprintf(d->scratch, "%s%s", prefix, name);
        *rem = prefix;
    }
    if (!*full) {
        d->failed = true;
        *full = "";
    }
}

static const char *rst_opt(const rst_dir_t *dir, const char *key) {
    for (int k = 0; k < dir->nopts; k++) {
        if (strcmp(dir->opts[k].key, key) == 0) {
            return dir->opts[k].val;
        }
    }
    return NULL;
}

/* H3 _contexts: the module, C namespace and class in force on every line. */
static void rst_contexts(rst_doc_t *d) {
    int n = d->n;
    d->mod_at = (const char **)cbm_arena_alloc(d->scratch, (size_t)n * sizeof(char *));
    d->ns_at = (const char **)cbm_arena_alloc(d->scratch, (size_t)n * sizeof(char *));
    d->cls_at = (const char **)cbm_arena_alloc(d->scratch, (size_t)n * sizeof(char *));
    if (!d->mod_at || !d->ns_at || !d->cls_at) {
        d->failed = true;
        return;
    }
    memset(d->cls_at, 0, (size_t)n * sizeof(char *));
    const char *cm = NULL;
    const char *cn = NULL;
    int k = 0;
    /* directives are in line order (the scan appends them so) */
    for (int di = 0; di <= d->ndirs; di++) {
        int upto = di < d->ndirs ? d->dirs[di].line : n;
        for (; k < upto; k++) {
            d->mod_at[k] = cm;
            d->ns_at[k] = cn;
        }
        if (di == d->ndirs) {
            break;
        }
        const rst_dir_t *dir = &d->dirs[di];
        const char *v = dir->nargs ? dir->args[0] : NULL;
        if (strcmp(dir->dom, "py") == 0 &&
            (strcmp(dir->dname, "module") == 0 || strcmp(dir->dname, "currentmodule") == 0)) {
            cm = (!v || !v[0] || strcmp(v, "None") == 0) ? NULL : v;
        }
        if (strcmp(dir->dom, "c") == 0 &&
            (strcmp(dir->dname, "namespace") == 0 || strcmp(dir->dname, "namespace-push") == 0)) {
            cn = (!v || !v[0] || strcmp(v, "NULL") == 0) ? NULL : v;
        }
        if (strcmp(dir->dom, "c") == 0 && strcmp(dir->dname, "namespace-pop") == 0) {
            cn = NULL;
        }
    }
    for (int di = 0; di < d->ndirs && !d->failed; di++) {
        const rst_dir_t *dir = &d->dirs[di];
        if (strcmp(dir->dom, "py") != 0 || !rst_in(dir->dname, PY_OBJ) ||
            strcmp(dir->dname, "module") == 0 || dir->cs >= dir->ce) {
            continue;
        }
        const char *outer = d->cls_at[dir->line];
        const char **sigs = NULL;
        int ns = rst_join_sigs(d, dir, &sigs);
        const char *content_cls = outer;
        const char *prefix;
        const char *name;
        if (ns > 0 && rst_py_sig(d, sigs[ns - 1], &prefix, &name)) {
            const char *full;
            const char *rem;
            rst_py_fullname(d, prefix, name, outer, &full, &rem);
            if (strcmp(dir->dname, "class") == 0 || strcmp(dir->dname, "exception") == 0) {
                content_cls = full;
            } else if (rem[0]) {
                size_t rl = strlen(rem);
                const char *r = rem;
                while (*r == '.') {
                    r++;
                    rl--;
                }
                while (rl > 0 && r[rl - 1] == '.') {
                    rl--;
                }
                content_cls = cbm_arena_strndup(d->scratch, r, rl);
            }
        }
        const char *mod = rst_opt(dir, "module");
        if (mod) {
            for (int j = dir->cs; j < dir->ce; j++) {
                d->mod_at[j] = mod;
            }
        }
        for (int j = dir->cs; j < dir->ce; j++) {
            d->cls_at[j] = content_cls;
        }
    }
}

/* ── Tokens ──────────────────────────────────────────────────────── */

static void rst_tok(rst_doc_t *d, int line, int col, int syntax, const char *raw) {
    if (!raw) {
        d->failed = true;
        return;
    }
    d->toks = (rst_tok_t *)rst_grow(d, d->toks, d->ntoks, &d->cap_toks, sizeof(rst_tok_t));
    if (!d->toks) {
        return;
    }
    d->toks[d->ntoks++] = (rst_tok_t){.line = line, .col = col, .syntax = syntax, .raw = raw};
}

static const char *rst_s(const char *s) {
    return s ? s : "";
}

/* The written form of a directive: its head line, whitespace collapsed. */
static const char *rst_written_dir(rst_doc_t *d, const rst_dir_t *dir) {
    const rst_line_t *ln = &d->L[dir->line];
    return rst_collapse(d->ctx->arena, ln->s, ln->len);
}

static bool rst_code_ext(const char *path) {
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    if (strncmp(base, "Makefile", strlen("Makefile")) == 0 ||
        strncmp(base, "Kconfig", strlen("Kconfig")) == 0) {
        return true;
    }
    const char *dot = strrchr(base, '.');
    if (!dot) {
        return false;
    }
    char ext[16];
    size_t el = strlen(dot);
    if (el >= sizeof(ext)) {
        return false;
    }
    for (size_t i = 0; i <= el; i++) {
        char c = dot[i];
        ext[i] = (char)(c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c);
    }
    return rst_in(ext, CODE_EXT);
}

static void rst_directive_tokens(rst_doc_t *d, const rst_dir_t *dir) {
    CBMArena *a = d->ctx->arena;
    const char *dn = dir->dname;
    const char *dom = dir->dom;
    bool py_obj = (strcmp(dom, "py") == 0 || strcmp(dom, "std") == 0) && rst_in(dn, PY_OBJ);
    bool c_obj = (strcmp(dom, "c") == 0 || strcmp(dom, "std") == 0 || strcmp(dom, "py") == 0) &&
                 rst_in(dn, C_OBJ);
    if (strcmp(dom, "py") != 0 && strcmp(dom, "c") != 0 && strcmp(dom, "std") != 0) {
        py_obj = false;
        c_obj = false;
    }
    if (strcmp(dom, "c") == 0) {
        py_obj = false;
    }
    int line = dir->line;
    const char *written = NULL;
    if (py_obj || c_obj) {
        written = rst_written_dir(d, dir);
        const char *mod = rst_opt(dir, "module");
        const char **sigs = NULL;
        int ns = rst_join_sigs(d, dir, &sigs);
        if (ns == 0 && !d->failed) {
            sigs = (const char **)cbm_arena_alloc(d->scratch, sizeof(char *));
            if (!sigs) {
                d->failed = true;
                return;
            }
            sigs[0] = "";
            ns = 1;
        }
        bool attr_like =
            strcmp(dn, "attribute") == 0 || strcmp(dn, "data") == 0 || strcmp(dn, "property") == 0;
        for (int k = 0; k < ns && !d->failed; k++) {
            char *sig = rst_collapse(d->scratch, sigs[k], (int)strlen(sigs[k]));
            if (!sig) {
                d->failed = true;
                return;
            }
            /* Python's reading (H3 py_directive): an attribute's type or
             * value is no part of its name; the full name in the class */
            char *py_sig = sig;
            if (attr_like) {
                size_t cut = strcspn(sig, ":=");
                while (cut > 0 && sig[cut - 1] == ' ') {
                    cut--;
                }
                py_sig = cbm_arena_strndup(d->scratch, sig, cut);
            }
            const char *prefix;
            const char *name;
            const char *full = "";
            const char *rem;
            if (py_sig && strcmp(dn, "module") != 0 && rst_py_sig(d, py_sig, &prefix, &name)) {
                rst_py_fullname(d, prefix, name, d->cls_at[line], &full, &rem);
            }
            rst_tok(d, line, dir->indent, CBM_DOCLINK_RST_OBJECT,
                    cbm_arena_sprintf(a, "%s\t%s\t%s\t%s\t%s\t%s\t%s", rst_s(written), dir->name,
                                      sig, rst_s(mod ? mod : d->mod_at[line]),
                                      rst_s(d->cls_at[line]), rst_s(d->ns_at[line]), full));
        }
        return;
    }
    const char *arg0 = dir->nargs ? dir->args[0] : "";
    if (rst_in(dn, AUTODOC)) {
        written = rst_written_dir(d, dir);
        char *arg = rst_collapse(d->scratch, arg0, (int)strlen(arg0));
        rst_tok(d, line, dir->indent, CBM_DOCLINK_RST_AUTODOC,
                cbm_arena_sprintf(a, "%s\t%s\t%s\t%s\t%s", rst_s(written), dn, rst_s(arg),
                                  rst_s(d->mod_at[line]), rst_s(d->cls_at[line])));
        return;
    }
    if (strcmp(dn, "literalinclude") == 0) {
        written = rst_written_dir(d, dir);
        const char *lines = rst_opt(dir, "lines");
        const char *pyobject = rst_opt(dir, "pyobject");
        bool bounded = rst_opt(dir, "start-after") || rst_opt(dir, "end-before") ||
                       rst_opt(dir, "start-at") || rst_opt(dir, "end-at");
        rst_tok(d, line, dir->indent, CBM_DOCLINK_RST_LITERALINCLUDE,
                cbm_arena_sprintf(a, "%s\t%s\t%s\t%s\t%s", rst_s(written), arg0, rst_s(lines),
                                  rst_s(pyobject), bounded ? "1" : ""));
        return;
    }
    if (strcmp(dn, "include") == 0 || strcmp(dn, "kernel-include") == 0) {
        if (!arg0[0] || arg0[0] == '<' || !rst_code_ext(arg0)) {
            return; /* a document or text include: not code */
        }
        written = rst_written_dir(d, dir);
        rst_tok(d, line, dir->indent, CBM_DOCLINK_RST_INCLUDE,
                cbm_arena_sprintf(a, "%s\t%s\t%s", rst_s(written), dn, arg0));
        return;
    }
    if (strcmp(dn, "kernel-doc") == 0) {
        written = rst_written_dir(d, dir);
        rst_tok(d, line, dir->indent, CBM_DOCLINK_RST_KERNEL_DOC,
                cbm_arena_sprintf(a, "%s\t%s\t", rst_s(written), arg0));
        const char *ids = rst_opt(dir, "identifiers");
        if (!ids) {
            ids = rst_opt(dir, "functions");
        }
        if (!ids || rst_opt(dir, "export") || rst_opt(dir, "internal") || rst_opt(dir, "doc")) {
            return; /* the names come from the source file's own kernel-doc comments */
        }
        /* each name, a trailing backslash dropped (kerneldoc.py) */
        const char *p = ids;
        while (*p && !d->failed) {
            while (*p == ' ') {
                p++;
            }
            const char *e = p;
            while (*e && *e != ' ') {
                e++;
            }
            size_t nl = (size_t)(e - p);
            while (nl > 0 && p[nl - 1] == '\\') {
                nl--;
            }
            if (nl > 0) {
                rst_tok(d, line, dir->indent, CBM_DOCLINK_RST_KERNEL_DOC,
                        cbm_arena_sprintf(a, "%s\t%s\t%.*s", rst_s(written), arg0, (int)nl, p));
            }
            p = e;
        }
    }
}

/* ── Inline text: roles and inline literals ──────────────────────── */

static bool rst_inline_kind(unsigned char k) {
    return k == K_PROSE || k == K_TITLE || k == K_DHEAD_PROSE || k == K_DARG_PROSE || k == K_PLIT;
}

typedef struct {
    char *text; /* the run's lines joined by '\n' */
    char *mask; /* inline literals blanked (H3 mask) */
    int len;
    int a; /* first line */
    int *offs;
    int nlines;
} rst_run_t;

static int rst_run_line(const rst_run_t *r, int pos) {
    int lo = 0;
    int hi = r->nlines;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / 2);
        if (r->offs[mid] <= pos) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo - 1;
}

/* Is the code point before byte i a role's left neighbour that blocks it
 * ((?<![\w`\\]))? */
static bool rst_role_blocked(const char *s, int i) {
    if (i == 0) {
        return false;
    }
    int k = i - 1;
    while (k > 0 && ((unsigned char)s[k] & 0xC0) == 0x80) {
        k--;
    }
    int n;
    uint32_t cp = rst_cp(s, i, k, &n);
    return cp == '`' || cp == '\\' || rst_word_cp(cp);
}

/* [A-Za-z][\w.+-]* at s[i]: its end, or -1. */
static int rst_role_name(const char *s, int len, int i) {
    if (i >= len || !rst_alpha_ascii((unsigned char)s[i])) {
        return -1;
    }
    i++;
    while (i < len) {
        int n;
        uint32_t cp = rst_cp(s, len, i, &n);
        if (cp != '.' && cp != '+' && cp != '-' && !rst_word_cp(cp)) {
            break;
        }
        i += n;
    }
    return i;
}

static bool rst_role_end_ok(const char *s, int len, int i) {
    if (i >= len) {
        return true;
    }
    int n;
    uint32_t cp = rst_cp(s, len, i, &n);
    return cp != '`' && !rst_word_cp(cp);
}

static void rst_emit_role(rst_doc_t *d, const rst_run_t *r, int start, int role_a, int role_b,
                          int ca, int cb) {
    CBMArena *a = d->ctx->arena;
    char *role = cbm_arena_strndup(d->scratch, r->text + role_a, (size_t)(role_b - role_a));
    if (!role) {
        d->failed = true;
        return;
    }
    const char *dom;
    const char *rname;
    rst_split_domain(d, role, PY_ROLES, &dom, &rname);
    bool code = ((strcmp(dom, "py") == 0 || strcmp(dom, "std") == 0) && rst_in(rname, PY_ROLES)) ||
                ((strcmp(dom, "c") == 0 || strcmp(dom, "std") == 0) && rst_in(rname, C_ROLES));
    char *content = rst_collapse(d->scratch, r->text + ca, cb - ca);
    if (!content) {
        d->failed = true;
        return;
    }
    if (!code) {
        /* an extlink to a repository path (:source:`django/db/x.py`, or with a
         * title, :source:`x <django/db/x.py>`): only a role without a domain
         * whose target is a path */
        const char *target = content;
        size_t tl = strlen(content);
        const char *lt = strrchr(content, '<');
        if (tl > 0 && content[tl - 1] == '>' && lt && lt > content) {
            target = lt + 1;
            tl = (size_t)(content + tl - 1 - target);
        }
        CBMDocLinkMdPath p;
        if (strchr(role, ':') ||
            !cbm_doclink_md_classify_span(target, tl, d->span_buf, RST_SPAN_MAX + 1, &p) ||
            p.shape == CBM_DOCLINK_MD_QUALIFIED) {
            return;
        }
    }
    int li = rst_run_line(r, start);
    int line = r->a + li;
    char *written = rst_collapse(a, r->text + start, cb + 1 - start);
    rst_tok(d, line, start - r->offs[li], CBM_DOCLINK_RST_ROLE,
            written ? cbm_arena_sprintf(a, "%s\t%s\t%s\t%s\t%s\t%s", written, role, content,
                                        rst_s(d->mod_at[line]), rst_s(d->cls_at[line]),
                                        rst_s(d->ns_at[line]))
                    : NULL);
}

/* ROLE_RE over the masked text. */
static void rst_roles(rst_doc_t *d, const rst_run_t *r) {
    const char *m = r->mask;
    int len = r->len;
    int i = 0;
    while (i < len && !d->failed) {
        const char *colon = memchr(m + i, ':', (size_t)(len - i));
        if (!colon) {
            break;
        }
        i = (int)(colon - m);
        if (rst_role_blocked(m, i)) {
            i++;
            continue;
        }
        int e1 = rst_role_name(m, len, i + 1);
        int role_end = -1;
        if (e1 > 0 && e1 < len && m[e1] == ':') {
            int e2 = rst_role_name(m, len, e1 + 1);
            if (e2 > 0 && e2 + 1 < len && m[e2] == ':' && m[e2 + 1] == '`') {
                role_end = e2;
            } else if (e1 + 1 < len && m[e1 + 1] == '`') {
                role_end = e1;
            }
        }
        if (role_end < 0) {
            i++;
            continue;
        }
        int open = role_end + 1;
        const char *close = memchr(m + open + 1, '`', (size_t)(len - open - 1));
        if (!close || close == m + open + 1 || !rst_role_end_ok(m, len, (int)(close - m) + 1)) {
            i++;
            continue;
        }
        int cb = (int)(close - m);
        rst_emit_role(d, r, i, i + 1, role_end, open + 1, cb);
        i = cb + 1;
    }
}

static void rst_literal_token(rst_doc_t *d, const rst_run_t *r, int i, int a, int b) {
    if (b <= a || b - a > RST_SPAN_MAX || memchr(r->text + a, '\n', (size_t)(b - a))) {
        return;
    }
    CBMDocLinkMdPath p;
    if (!cbm_doclink_md_classify_span(r->text + a, (size_t)(b - a), d->span_buf, RST_SPAN_MAX + 1,
                                      &p)) {
        return;
    }
    int li = rst_run_line(r, i);
    rst_tok(d, r->a + li, i - r->offs[li],
            p.shape == CBM_DOCLINK_MD_QUALIFIED ? CBM_DOCLINK_RST_CODE_NAME
                                                : CBM_DOCLINK_RST_CODE_PATH,
            cbm_arena_strndup(d->ctx->arena, r->text + a, (size_t)(b - a)));
}

/* INLINE_LIT_RE ``.+?`` (DOTALL): blank them in the mask, and take each as a
 * possible code_path / code_name. */
static void rst_literals(rst_doc_t *d, rst_run_t *r) {
    const char *t = r->text;
    int len = r->len;
    int i = 0;
    while (i + 1 < len && !d->failed) {
        if (t[i] != '`' || t[i + 1] != '`') {
            i++;
            continue;
        }
        int j = i + 3;
        while (j + 1 < len && !(t[j] == '`' && t[j + 1] == '`')) {
            j++;
        }
        if (j + 1 >= len) {
            i++;
            continue;
        }
        for (int k = i; k < j + 2; k++) {
            if (r->mask[k] != '\n') {
                r->mask[k] = ' ';
            }
        }
        rst_literal_token(d, r, i, i + 2, j);
        i = j + 2;
    }
}

static void rst_inline(rst_doc_t *d) {
    int i = 0;
    while (i < d->n && !d->failed) {
        if (!rst_inline_kind(d->K[i])) {
            i++;
            continue;
        }
        int j = i;
        int len = 0;
        while (j < d->n && rst_inline_kind(d->K[j])) {
            len += d->L[j].len + 1;
            j++;
        }
        rst_run_t r = {.a = i, .nlines = j - i};
        r.text = (char *)cbm_arena_alloc(d->scratch, (size_t)len + 1);
        r.mask = (char *)cbm_arena_alloc(d->scratch, (size_t)len + 1);
        r.offs = (int *)cbm_arena_alloc(d->scratch, (size_t)(j - i) * sizeof(int));
        if (!r.text || !r.mask || !r.offs) {
            d->failed = true;
            return;
        }
        int pos = 0;
        for (int k = i; k < j; k++) {
            r.offs[k - i] = pos;
            memcpy(r.text + pos, d->L[k].s, (size_t)d->L[k].len);
            pos += d->L[k].len;
            if (k + 1 < j) {
                r.text[pos++] = '\n';
            }
        }
        r.text[pos] = '\0';
        r.len = pos;
        memcpy(r.mask, r.text, (size_t)pos + 1);
        rst_literals(d, &r);
        rst_roles(d, &r);
        i = j;
    }
}

/* ── Sections ────────────────────────────────────────────────────── */

/* qn_safe_segment (extract_defs.c): whitespace runs become '-'. */
static const char *rst_qn_segment(CBMArena *a, const char *name) {
    char *out = cbm_arena_strdup(a, name);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    bool in_ws = false;
    for (const char *p = name; *p; p++) {
        if (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') {
            in_ws = true;
            continue;
        }
        if (in_ws && w > 0) {
            out[w++] = '-';
        }
        in_ws = false;
        out[w++] = *p;
    }
    out[w] = '\0';
    return out;
}

/* The section's own text (title block to the next title), whitespace
 * collapsed, at most RST_BODY_MAX bytes and never a cut character. */
static const char *rst_body(rst_doc_t *d, int from, int to) {
    char *out = (char *)cbm_arena_alloc(d->ctx->arena, RST_BODY_MAX + 1);
    if (!out) {
        return NULL;
    }
    int w = 0;
    for (int k = from; k < to && w < RST_BODY_MAX; k++) {
        const char *s = d->L[k].s;
        int len = d->L[k].len;
        int i = 0;
        while (i < len && w < RST_BODY_MAX) {
            int n;
            uint32_t cp = rst_cp(s, len, i, &n);
            if (rst_space_cp(cp)) {
                i += n;
                continue;
            }
            if (w > 0) {
                out[w++] = ' ';
            }
            while (i < len) {
                cp = rst_cp(s, len, i, &n);
                if (rst_space_cp(cp)) {
                    break;
                }
                if (w + n > RST_BODY_MAX) {
                    w = RST_BODY_MAX + 1; /* stop: the next character does not fit */
                    break;
                }
                memcpy(out + w, s + i, (size_t)n);
                w += n;
                i += n;
            }
        }
    }
    if (w > RST_BODY_MAX) {
        w = RST_BODY_MAX;
        while (w > 0 && ((unsigned char)out[w] & 0xC0) == 0x80) {
            w--;
        }
    }
    while (w > 0 && out[w - 1] == ' ') {
        w--;
    }
    out[w] = '\0';
    return w > 0 ? out : NULL;
}

static int rst_title_qn_cmp(const void *a, const void *b) {
    const rst_title_t *x = *(const rst_title_t *const *)a;
    const rst_title_t *y = *(const rst_title_t *const *)b;
    return strcmp(x->qn, y->qn);
}

static void rst_sections(rst_doc_t *d) {
    CBMExtractCtx *ctx = d->ctx;
    CBMArena *a = ctx->arena;
    for (int t = 0; t < d->ntitles; t++) {
        rst_title_t *ti = &d->titles[t];
        const char *seg = rst_qn_segment(a, ti->text);
        ti->qn = seg ? cbm_fqn_compute(a, ctx->project, ctx->rel_path, seg) : NULL;
        if (!ti->qn) {
            d->failed = true;
            return;
        }
    }
    if (d->ntitles > 1) {
        rst_title_t **by_qn =
            (rst_title_t **)cbm_arena_alloc(d->scratch, (size_t)d->ntitles * sizeof(*by_qn));
        if (!by_qn) {
            d->failed = true;
            return;
        }
        for (int t = 0; t < d->ntitles; t++) {
            by_qn[t] = &d->titles[t];
        }
        qsort(by_qn, (size_t)d->ntitles, sizeof(*by_qn), rst_title_qn_cmp);
        for (int t = 1; t < d->ntitles; t++) {
            if (strcmp(by_qn[t]->qn, by_qn[t - 1]->qn) == 0) {
                by_qn[t]->shared_qn = true;
                by_qn[t - 1]->shared_qn = true;
            }
        }
    }
    for (int t = 0; t < d->ntitles; t++) {
        const rst_title_t *ti = &d->titles[t];
        int end = t + 1 < d->ntitles ? d->titles[t + 1].first : d->n;
        int last = end - 1;
        while (last > ti->idx && d->L[last].blank) {
            last--;
        }
        char *name = cbm_arena_strdup(a, ti->text);
        if (!name) {
            d->failed = true;
            return;
        }
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = ti->qn;
        def.label = "Section";
        def.file_path = ctx->rel_path;
        def.start_line = (uint32_t)ti->idx + 1;
        def.end_line = (uint32_t)last + 1;
        def.is_exported = true;
        def.docstring = rst_body(d, ti->next, end);
        cbm_defs_push(&ctx->result->defs, a, def);
    }
}

/* ── Driver ──────────────────────────────────────────────────────── */

static int rst_tok_cmp(const void *a, const void *b) {
    const rst_tok_t *x = (const rst_tok_t *)a;
    const rst_tok_t *y = (const rst_tok_t *)b;
    if (x->line != y->line) {
        return x->line < y->line ? -1 : 1;
    }
    if (x->col != y->col) {
        return x->col < y->col ? -1 : 1;
    }
    if (x->syntax != y->syntax) {
        return x->syntax < y->syntax ? -1 : 1;
    }
    return strcmp(x->raw, y->raw);
}

/* The section a 0-based line is written in (by its title block's first line). */
static const rst_title_t *rst_section_of(const rst_doc_t *d, int line) {
    int lo = 0;
    int hi = d->ntitles;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / 2);
        if (d->titles[mid].first <= line) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo > 0 ? &d->titles[lo - 1] : NULL;
}

void cbm_doclink_rst_scan_file(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || !ctx->source || ctx->source_len <= 0) {
        return;
    }
    rst_doc_t d = {.ctx = ctx, .scratch = ctx->scratch ? ctx->scratch : ctx->arena};
    d.span_buf = (char *)cbm_arena_alloc(d.scratch, (size_t)(RST_SPAN_MAX + 1) * 2);
    if (!d.span_buf || !rst_lines(&d, ctx->source, ctx->source_len)) {
        ctx->result->doc_links.failed = true;
        return;
    }
    rst_scan(&d);
    if (!d.failed) {
        rst_contexts(&d);
    }
    if (!d.failed) {
        rst_sections(&d);
    }
    for (int i = 0; i < d.ndirs && !d.failed; i++) {
        rst_directive_tokens(&d, &d.dirs[i]);
    }
    if (!d.failed) {
        rst_inline(&d);
    }
    if (d.failed) {
        ctx->result->doc_links.failed = true;
        return;
    }
    qsort(d.toks, (size_t)d.ntoks, sizeof(*d.toks), rst_tok_cmp);
    for (int i = 0; i < d.ntoks; i++) {
        const rst_tok_t *t = &d.toks[i];
        const rst_title_t *sec = rst_section_of(&d, t->line);
        if (sec && sec->shared_qn) {
            sec = NULL; /* the file, not the node another title owns */
        }
        CBMDocLink link = {
            .source_qn = sec ? sec->qn : (ctx->module_qn ? ctx->module_qn : ""),
            .raw = t->raw,
            .line = (uint32_t)t->line + 1,
            .def_line = sec ? (uint32_t)sec->idx + 1 : 1,
            .syntax = (uint16_t)t->syntax,
            .flags = sec ? 0 : CBM_DOCLINK_FLAG_FILE,
        };
        cbm_doclinks_push(&ctx->result->doc_links, ctx->arena, link);
    }
    /* an architecture decision record in reST: its node, facts and supersedes */
    cbm_adr_extract(ctx);
}
