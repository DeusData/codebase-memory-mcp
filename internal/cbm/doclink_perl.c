/*
 * doclink_perl.c — Perl POD `L<>` references and the Perl doc-link scope.
 *
 * Tokens. A sub's stored doc is every POD section whose =headN / =item heading
 * names it (extract_defs.c), verbatim and joined by a blank line. Its L<>
 * formatting codes are the references, read the way perlpodspec and
 * Pod::Simple read them:
 *   - a paragraph is a run of non-blank lines. Codes are looked for in ordinary
 *     paragraphs and in the text of =headN and =item. A verbatim paragraph
 *     (first byte a space or tab), a =for paragraph and a =begin .. =end
 *     region of a format without a leading colon are data and hold no code;
 *   - a code is a capital letter and `<`. `X<< text >>` (two or more brackets
 *     and whitespace) ends at whitespace and as many `>`. Codes nest; an L
 *     inside an L is not a link (perlpodspec: links must not nest);
 *   - an L<> whose target (what follows `text|`) matches `\w+:[^:\s]\S*` is a
 *     URL: the core's href family. Every other one is a pod_L token whose raw
 *     text is the code's content as written, white space squeezed. A code the
 *     paragraph ends before, and one longer than CBM_DOCLINK_PERL_RAW_MAX,
 *     keeps its opening `L<` in the raw text: nothing parses that, so the
 *     resolver records it as unparseable.
 * A doc that does not start with a POD command is a `#` comment and has no
 * formatting codes at all.
 *
 * Lines. The stored doc carries no position, and several sections can be
 * joined in it, so every section of the file is found again here (the same
 * paragraph, command and section rules as extract_defs.c) and the doc is
 * matched against them byte for byte: a reference then gets the source line it
 * is written on. A part of a doc that matches no section keeps the line the
 * core passed (the definition's). That needs the file's sections once per
 * file, and parse_doc is called per definition: the call for the file's first
 * documented definition harvests the whole file, the later ones return.
 *
 * Scope blob (one record per line, tab-separated, first line "perl1"; no line
 * numbers, so it is persisted as it is):
 *   E               the tree has a parse error: which package a sub belongs to
 *                   is not trusted
 *   P  name         a package, class or role the file declares; document
 *                   order, each name once
 *   S  pkg  name    a sub that has a definition, in definition order; pkg is
 *                   the 0-based index of its P record, `-` before any package
 *                   (main), `?` when it could not be placed
 *   H  sub  key     a POD heading (=headN / =item): `key` is its text with
 *                   formatting codes reduced to what they render
 *                   (cbm_doclink_perl_plain_text) and an item's bullet
 *                   dropped; `sub` is the identifier the heading names by
 *                   extract_defs.c's rule, `-` when it names none
 * Packages and the placement of subs come from the tree; the subs themselves
 * from the file's definitions (so a sub listed here has a node); the headings
 * from a line scan of the file, which is what perldoc reads.
 */
#include "doclink.h"

#include "arena.h"
#include "foundation/constants.h"
#include "tree_sitter/api.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    PL_CODE_DEPTH = 32, /* open formatting codes; deeper nesting is malformed */
    PL_INIT_CAP = 32,
    PL_SB_INIT = 1024,
    PL_ENT_MAX = 12,     /* longest E<> body that is decoded */
    PL_MALFORMED = 2,    /* strlen("L<"): what a malformed code's raw text starts with */
    PL_UTF8_CONT = 0x80, /* 10xxxxxx */
    PL_UTF8_CONT_MASK = 0xC0,
    PL_UTF8_LEAD2 = 0xC0,
    PL_UTF8_LEAD3 = 0xE0,
    PL_UTF8_LEAD4 = 0xF0,
    PL_UTF8_LEAD_END = 0xF8,
    PL_ASCII_DEL = 0x7F,
    PL_HEAD_WORD = 5, /* strlen("headN") */
    PL_NAME_MAX = 255,
};

/* ── Characters ──────────────────────────────────────────────────── */

static bool pl_blank(char c) {
    return c == ' ' || c == '\t' || c == '\r';
}

static bool pl_space(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v';
}

static bool pl_word(char c) {
    return isalnum((unsigned char)c) || c == '_';
}

static bool pl_upper(char c) {
    return c >= 'A' && c <= 'Z';
}

/* A control byte that is not white space: never copied into a blob record or
 * a raw text (a NUL would end either). */
static bool pl_control(char c) {
    unsigned char u = (unsigned char)c;
    return (u < ' ' && !pl_space(c)) || u == PL_ASCII_DEL;
}

static CBMArena *pl_scratch(CBMExtractCtx *ctx) {
    return ctx->scratch ? ctx->scratch : ctx->arena;
}

/* ── Paragraphs ──────────────────────────────────────────────────── */

/* Next paragraph (a maximal run of non-blank lines) of s[*pos, end): [*pb,
 * *pe). The same rule as extract_defs.c's pod_next_para. */
static bool pl_next_para(const char *s, size_t *pos, size_t end, size_t *pb, size_t *pe) {
    size_t i = *pos;
    bool in = false;
    size_t start = 0;
    while (i < end) {
        const char *nl = (const char *)memchr(s + i, '\n', end - i);
        size_t le = nl ? (size_t)(nl - s) : end;
        bool blank = true;
        for (size_t k = i; k < le && blank; k++) {
            blank = pl_blank(s[k]);
        }
        if (!blank && !in) {
            in = true;
            start = i;
        }
        if (blank && in) {
            *pb = start;
            *pe = i;
            *pos = i;
            return true;
        }
        i = le < end ? le + SKIP_ONE : end;
    }
    *pos = end;
    if (in) {
        *pb = start;
        *pe = end;
    }
    return in;
}

/* True when the paragraph s[pb, pe) is a command (`=` and a letter). */
static bool pl_is_command(const char *s, size_t pb, size_t pe) {
    return pb + SKIP_ONE < pe && s[pb] == '=' && isalpha((unsigned char)s[pb + SKIP_ONE]);
}

/* End of the command word that follows the `=` at s[pb]. */
static size_t pl_command_word_end(const char *s, size_t pb, size_t pe) {
    size_t wb = pb + SKIP_ONE;
    while (wb < pe && pl_word(s[wb])) {
        wb++;
    }
    return wb;
}

static bool pl_word_is(const char *w, size_t n, const char *lit) {
    return n == strlen(lit) && memcmp(w, lit, n) == 0;
}

/* `headN`: the level, 0 for another word. */
static int pl_head_level(const char *w, size_t n) {
    static const char head[] = "head";
    size_t hl = sizeof(head) - SKIP_ONE;
    if (n != PL_HEAD_WORD || memcmp(w, head, hl) != 0 || !isdigit((unsigned char)w[hl])) {
        return 0;
    }
    return w[hl] - '0';
}

/* ── Formatting codes ────────────────────────────────────────────── */

typedef struct {
    uint8_t width[PL_CODE_DEPTH]; /* brackets of each open code: 1 simple, more complex */
    int depth;
    bool overflow; /* nested deeper than PL_CODE_DEPTH */
} pl_codes_t;

enum { PL_EV_TEXT = 0, PL_EV_OPEN, PL_EV_CLOSE };

/* A code opens at t[i]: push it and return where its content starts. */
static size_t pl_code_open(pl_codes_t *st, const char *t, size_t i, size_t e, bool *pushed) {
    size_t j = i + SKIP_ONE;
    while (j < e && t[j] == '<') {
        j++;
    }
    size_t brackets = j - i - SKIP_ONE;
    size_t content = i + PAIR_LEN;
    size_t width = SKIP_ONE;
    if (brackets >= PAIR_LEN && j < e && pl_space(t[j])) {
        while (j < e && pl_space(t[j])) {
            j++;
        }
        content = j;
        width = brackets > UINT8_MAX ? UINT8_MAX : brackets;
    }
    *pushed = st->depth < PL_CODE_DEPTH;
    if (*pushed) {
        st->width[st->depth++] = (uint8_t)width;
    } else {
        st->overflow = true;
    }
    return content;
}

/* One lexing step at t[i], i < e (Pod::Simple's treelet tokens).
 *   PL_EV_OPEN   a code opened at t[i]; its content starts at *next
 *   PL_EV_CLOSE  the innermost code's content ended at i; *next follows its
 *                closing brackets
 *   PL_EV_TEXT   t[i, *next) is literal */
static int pl_code_step(pl_codes_t *st, const char *t, size_t i, size_t e, size_t *next) {
    char c = t[i];
    if (pl_upper(c) && i + SKIP_ONE < e && t[i + SKIP_ONE] == '<') {
        bool pushed = false;
        *next = pl_code_open(st, t, i, e, &pushed);
        return pushed ? PL_EV_OPEN : PL_EV_TEXT;
    }
    size_t need = st->depth > 0 ? st->width[st->depth - SKIP_ONE] : 0;
    if (need == SKIP_ONE && c == '>') {
        st->depth--;
        *next = i + SKIP_ONE;
        return PL_EV_CLOSE;
    }
    if (need > SKIP_ONE && pl_space(c)) {
        size_t k = i;
        while (k < e && pl_space(t[k])) {
            k++;
        }
        size_t m = k;
        while (m < e && t[m] == '>') {
            m++;
        }
        if (m - k >= need) {
            st->depth--;
            *next = k + need;
            return PL_EV_CLOSE;
        }
        *next = m; /* white space and too few brackets: literal */
        return PL_EV_TEXT;
    }
    *next = i + SKIP_ONE;
    return PL_EV_TEXT;
}

/* ── Bounded text output ─────────────────────────────────────────── */

typedef struct {
    char *buf;
    size_t cap; /* bytes of text that fit (the buffer holds one more) */
    size_t len;
    bool space; /* a white-space run is pending */
    bool cut;   /* something did not fit */
} pl_out_t;

/* Bytes of the UTF-8 character that `lead` starts; 1 for anything else. */
static size_t pl_utf8_len(unsigned char lead) {
    if (lead >= PL_UTF8_LEAD4 && lead < PL_UTF8_LEAD_END) {
        return 4;
    }
    if (lead >= PL_UTF8_LEAD3 && lead < PL_UTF8_LEAD4) {
        return 3;
    }
    if (lead >= PL_UTF8_LEAD2 && lead < PL_UTF8_LEAD3) {
        return PAIR_LEN;
    }
    return SKIP_ONE;
}

/* Append t[0, n): white-space runs become one space (none at the start),
 * control bytes are dropped, and a character that does not fit whole ends
 * the output. */
static void pl_out_text(pl_out_t *o, const char *t, size_t n) {
    for (size_t i = 0; i < n && !o->cut; i++) {
        char c = t[i];
        if (pl_space(c)) {
            o->space = o->len > 0;
            continue;
        }
        if (pl_control(c)) {
            continue;
        }
        size_t need = pl_utf8_len((unsigned char)c) + (o->space ? SKIP_ONE : 0);
        if (o->len + need > o->cap) {
            o->cut = true;
            return;
        }
        if (o->space) {
            o->buf[o->len++] = ' ';
            o->space = false;
        }
        o->buf[o->len++] = c;
    }
}

static void pl_out_end(pl_out_t *o) {
    o->buf[o->len] = '\0';
}

/* t[b, e) squeezed into out (cap + 1 bytes); *cut when it did not fit. */
static size_t pl_squeeze(const char *t, size_t b, size_t e, char *out, size_t cap, bool *cut) {
    pl_out_t o = {.buf = out, .cap = cap};
    pl_out_text(&o, t + b, e - b);
    pl_out_end(&o);
    *cut = o.cut;
    return o.len;
}

/* ── Plain text ──────────────────────────────────────────────────── */

/* Append the UTF-8 form of `cp`; false for what is no character. */
static bool pl_out_codepoint(pl_out_t *o, unsigned long cp) {
    enum {
        CP_1 = 0x80,
        CP_2 = 0x800,
        CP_3 = 0x10000,
        CP_MAX = 0x10FFFF,
        CP_SURR_LO = 0xD800,
        CP_SURR_HI = 0xDFFF,
        SHIFT = 6,
        LOW = 0x3F,
    };
    if (cp == 0 || cp > CP_MAX || (cp >= CP_SURR_LO && cp <= CP_SURR_HI)) {
        return false;
    }
    char u[4];
    size_t n = 0;
    if (cp < CP_1) {
        u[n++] = (char)cp;
    } else if (cp < CP_2) {
        u[n++] = (char)(PL_UTF8_LEAD2 | (cp >> SHIFT));
        u[n++] = (char)(PL_UTF8_CONT | (cp & LOW));
    } else if (cp < CP_3) {
        u[n++] = (char)(PL_UTF8_LEAD3 | (cp >> (SHIFT + SHIFT)));
        u[n++] = (char)(PL_UTF8_CONT | ((cp >> SHIFT) & LOW));
        u[n++] = (char)(PL_UTF8_CONT | (cp & LOW));
    } else {
        u[n++] = (char)(PL_UTF8_LEAD4 | (cp >> (SHIFT + SHIFT + SHIFT)));
        u[n++] = (char)(PL_UTF8_CONT | ((cp >> (SHIFT + SHIFT)) & LOW));
        u[n++] = (char)(PL_UTF8_CONT | ((cp >> SHIFT) & LOW));
        u[n++] = (char)(PL_UTF8_CONT | (cp & LOW));
    }
    pl_out_text(o, u, n);
    return true;
}

/* The number an E<> body writes (decimal, 0x hex, 0 octal); false for a name. */
static bool pl_entity_number(const char *e, size_t n, unsigned long *cp) {
    enum { DEC = 10, HEX = 16, OCT = 8 };
    int base = DEC;
    size_t i = 0;
    if (n > PAIR_LEN && e[0] == '0' && (e[1] == 'x' || e[1] == 'X')) {
        base = HEX;
        i = PAIR_LEN;
    } else if (n > SKIP_ONE && e[0] == '0') {
        base = OCT;
        i = SKIP_ONE;
    }
    unsigned long v = 0;
    for (; i < n; i++) {
        int d = isdigit((unsigned char)e[i])    ? e[i] - '0'
                : isxdigit((unsigned char)e[i]) ? tolower((unsigned char)e[i]) - 'a' + DEC
                                                : base;
        if (d >= base) {
            return false;
        }
        v = (v * (unsigned long)base) + (unsigned long)d;
    }
    *cp = v;
    return n > 0;
}

/* Append what E<body> renders; an unknown body stays as written. */
static void pl_out_entity(pl_out_t *o, const char *body, size_t n, bool closed) {
    static const struct {
        const char *name;
        char ch;
    } ents[] = {{"lt", '<'},  {"gt", '>'},   {"verbar", '|'},
                {"sol", '/'}, {"quot", '"'}, {"amp", '&'}};
    for (size_t i = 0; closed && i < sizeof(ents) / sizeof(ents[0]); i++) {
        if (pl_word_is(body, n, ents[i].name)) {
            pl_out_text(o, &ents[i].ch, SKIP_ONE);
            return;
        }
    }
    unsigned long cp = 0;
    if (closed && pl_entity_number(body, n, &cp) && pl_out_codepoint(o, cp)) {
        return;
    }
    pl_out_text(o, "E<", PL_MALFORMED);
    pl_out_text(o, body, n);
    if (closed) {
        pl_out_text(o, ">", SKIP_ONE);
    }
}

size_t cbm_doclink_perl_plain_text(const char *in, size_t len, char *out, size_t cap) {
    if (!out || cap == 0) {
        return 0;
    }
    pl_out_t o = {.buf = out, .cap = cap - SKIP_ONE};
    pl_codes_t st = {.depth = 0};
    int drop_depth = 0; /* an X<> or Z<> is open at this depth: no text */
    int ent_depth = 0;  /* an E<> is open at this depth */
    char ent[PL_ENT_MAX];
    size_t ent_len = 0;
    size_t i = 0;
    while (in && i < len && !st.overflow) {
        size_t next = i;
        int ev = pl_code_step(&st, in, i, len, &next);
        if (ev == PL_EV_OPEN && drop_depth == 0 && ent_depth == 0) {
            if (in[i] == 'X' || in[i] == 'Z') {
                drop_depth = st.depth;
            } else if (in[i] == 'E') {
                ent_depth = st.depth;
                ent_len = 0;
            }
        } else if (ev == PL_EV_CLOSE && drop_depth != 0) {
            drop_depth = st.depth < drop_depth ? 0 : drop_depth;
        } else if (ev == PL_EV_CLOSE && ent_depth != 0 && st.depth < ent_depth) {
            pl_out_entity(&o, ent, ent_len, true);
            ent_depth = 0;
        } else if (ev == PL_EV_TEXT && drop_depth == 0 && ent_depth == 0) {
            pl_out_text(&o, in + i, next - i);
        } else if (ev == PL_EV_TEXT && drop_depth == 0) {
            for (size_t k = i; k < next && ent_len < sizeof(ent); k++) {
                ent[ent_len++] = in[k];
            }
        }
        i = next;
    }
    if (ent_depth != 0) {
        pl_out_entity(&o, ent, ent_len, false);
    }
    if (in && i < len) {
        pl_out_text(&o, in + i, len - i); /* nested too deep: the rest as written */
    }
    pl_out_end(&o);
    return o.len;
}

/* ── Link grammar (perlpodspec; Pod::Simple's _treat_Ls) ─────────── */

typedef struct {
    size_t bar;       /* first top-level `|`; SIZE_MAX: none */
    size_t slash;     /* first top-level `/` after it; SIZE_MAX: none */
    bool space;       /* a top-level space in a link that has no `|` */
    bool well_formed; /* every code closes, none nests too deep, no L inside */
} pl_shape_t;

/* Where the top-level `|` and `/` of a link's content are. */
static pl_shape_t pl_link_shape(const char *t, size_t n) {
    pl_shape_t sh = {.bar = SIZE_MAX, .slash = SIZE_MAX, .well_formed = true};
    pl_codes_t st = {.depth = 0};
    size_t i = 0;
    while (i < n && sh.well_formed) {
        size_t next = i;
        int ev = pl_code_step(&st, t, i, n, &next);
        sh.well_formed = !st.overflow && !(ev == PL_EV_OPEN && t[i] == 'L');
        for (size_t k = i; ev == PL_EV_TEXT && st.depth == 0 && k < next; k++) {
            if (t[k] == '|' && sh.bar == SIZE_MAX) {
                sh.bar = k;
                sh.slash = SIZE_MAX; /* the name and section follow the text */
            } else if (t[k] == '/' && sh.slash == SIZE_MAX) {
                sh.slash = k;
            } else if (t[k] == ' ' && sh.bar == SIZE_MAX) {
                sh.space = true;
            }
        }
        i = next;
    }
    sh.well_formed = sh.well_formed && st.depth == 0;
    return sh;
}

/* perlpodspec: a link whose target matches \A\w+:[^:\s]\S*\z is a URL. */
static bool pl_is_url(const char *t, size_t n) {
    size_t i = 0;
    while (i < n && pl_word(t[i])) {
        i++;
    }
    if (i == 0 || i + SKIP_ONE >= n || t[i] != ':' || t[i + SKIP_ONE] == ':') {
        return false;
    }
    for (size_t k = i + SKIP_ONE; k < n; k++) {
        if (pl_space(t[k])) {
            return false;
        }
    }
    return true;
}

/* The start of t[b, e) after its leading spaces. */
static size_t pl_lead_spaces_end(const char *t, size_t b, size_t e) {
    while (b < e && t[b] == ' ') {
        b++;
    }
    return b;
}

/* The end of t[b, e) before its trailing spaces. */
static size_t pl_trail_spaces_start(const char *t, size_t b, size_t e) {
    while (e > b && t[e - SKIP_ONE] == ' ') {
        e--;
    }
    return e;
}

/* Trim t[*b, *e) and drop one pair of surrounding double quotes. */
static void pl_trim(const char *t, size_t *b, size_t *e, bool *quoted) {
    size_t lo = pl_lead_spaces_end(t, *b, *e);
    size_t hi = pl_trail_spaces_start(t, lo, *e);
    *quoted = hi - lo >= PAIR_LEN && t[lo] == '"' && t[hi - SKIP_ONE] == '"';
    *b = *quoted ? lo + SKIP_ONE : lo;
    *e = *quoted ? hi - SKIP_ONE : hi;
}

/* `name(3)`: a man page. */
static bool pl_is_man_page(const char *p) {
    size_t n = strlen(p);
    if (n < 4 || p[n - SKIP_ONE] != ')') {
        return false;
    }
    size_t open = n - SKIP_ONE;
    while (open > 0 && p[open] != '(') {
        open--;
    }
    if (open == 0 || open + SKIP_ONE == n - SKIP_ONE) {
        return false;
    }
    for (size_t k = open + SKIP_ONE; k < n - SKIP_ONE; k++) {
        if (!isalnum((unsigned char)p[k]) && p[k] != '-') {
            return false;
        }
    }
    return memchr(p, '/', open) == NULL;
}

/* `Ident(::Ident)*`: a module name. */
static bool pl_is_module_name(const char *p) {
    if (!(isalpha((unsigned char)p[0]) || p[0] == '_')) {
        return false;
    }
    for (size_t i = 0; p[i]; i++) {
        if (pl_word(p[i])) {
            continue;
        }
        if (p[i] != ':' || p[i + SKIP_ONE] != ':' || !pl_word(p[i + PAIR_LEN])) {
            return false;
        }
        i++;
    }
    return true;
}

bool cbm_doclink_perl_parse_link(const char *raw, CBMPerlLink *out) {
    if (!out) {
        return false;
    }
    memset(out, 0, sizeof(*out));
    out->kind = CBM_PERL_LINK_UNPARSEABLE;
    size_t n = raw ? strlen(raw) : 0;
    if (n == 0 || n > CBM_DOCLINK_PERL_RAW_MAX || (raw[0] == 'L' && raw[1] == '<')) {
        return false; /* empty, or kept as written because it is malformed */
    }
    pl_shape_t sh = pl_link_shape(raw, n);
    if (!sh.well_formed) {
        return false;
    }
    size_t nb = sh.bar == SIZE_MAX ? 0 : sh.bar + SKIP_ONE;
    size_t ne = sh.slash == SIZE_MAX ? n : sh.slash;
    size_t sb = sh.slash == SIZE_MAX ? n : sh.slash + SKIP_ONE;
    size_t se = n;
    bool name_quoted = false;
    bool sec_quoted = false;
    pl_trim(raw, &nb, &ne, &name_quoted);
    pl_trim(raw, &sb, &se, &sec_quoted);
    bool section_only = sh.slash == SIZE_MAX && (name_quoted || sh.space);
    if (section_only) { /* L<"sec"> and the old L<two words> name a section */
        sb = nb;
        se = ne;
        ne = nb;
    } else if (name_quoted) {
        nb--; /* the quotes of a page name are part of it */
        ne++;
    }
    cbm_doclink_perl_plain_text(raw + nb, ne - nb, out->page, sizeof(out->page));
    cbm_doclink_perl_plain_text(raw + sb, se - sb, out->section, sizeof(out->section));
    if (!out->page[0]) {
        out->kind = out->section[0] ? CBM_PERL_LINK_SECTION : CBM_PERL_LINK_UNPARSEABLE;
    } else if (pl_is_man_page(out->page)) {
        out->kind = CBM_PERL_LINK_MAN;
    } else if (pl_is_module_name(out->page)) {
        out->kind = CBM_PERL_LINK_PAGE;
    }
    return out->kind != CBM_PERL_LINK_UNPARSEABLE;
}

/* ── Tokens ──────────────────────────────────────────────────────── */

typedef struct {
    CBMExtractCtx *ctx;
    const CBMDefinition *def;
    const char *text; /* what is being read: the file, or a doc of unknown position */
    size_t line_pos;  /* lines are counted from here ... */
    uint32_t line;    /* ... which is on this line */
    bool fixed_line;  /* every token gets `line` */
} pl_emit_t;

typedef struct {
    size_t at; /* the code's letter */
    size_t cb; /* its content */
    size_t ce;
    bool closed; /* false: the paragraph ended first */
} pl_code_t;

static uint32_t pl_line_at(pl_emit_t *em, size_t at) {
    if (em->fixed_line) {
        return em->line;
    }
    for (size_t k = em->line_pos; k < at; k++) {
        em->line += em->text[k] == '\n';
    }
    em->line_pos = at > em->line_pos ? at : em->line_pos;
    return em->line;
}

static void pl_push(pl_emit_t *em, const char *raw, size_t n, int syntax, uint32_t line) {
    char *copy = cbm_arena_strndup(em->ctx->arena, raw, n);
    if (!copy) {
        return;
    }
    CBMDocLink link = {
        .source_qn = em->def->qualified_name,
        .raw = copy,
        .line = line,
        .def_line = em->def->start_line,
        .syntax = (uint16_t)syntax,
    };
    cbm_doclinks_push(&em->ctx->result->doc_links, em->ctx->arena, link);
}

/* One L<> code of em->text -> one token (none for an empty code). */
static void pl_emit_link(pl_emit_t *em, const pl_code_t *code) {
    uint32_t line = pl_line_at(em, code->at);
    char buf[PL_MALFORMED + CBM_DOCLINK_PERL_RAW_MAX + SKIP_ONE] = "L<";
    char *content = buf + PL_MALFORMED;
    bool cut = false;
    size_t n = pl_squeeze(em->text, code->cb, code->ce, content, CBM_DOCLINK_PERL_RAW_MAX, &cut);
    if (!code->closed || cut) {
        size_t keep = n > CBM_DOCLINK_PERL_RAW_MAX - PL_MALFORMED
                          ? CBM_DOCLINK_PERL_RAW_MAX - PL_MALFORMED
                          : n;
        while (keep > 0 && ((unsigned char)content[keep] & PL_UTF8_CONT_MASK) == PL_UTF8_CONT) {
            keep--; /* not in the middle of a character */
        }
        pl_push(em, buf, PL_MALFORMED + keep, CBM_DOCLINK_PERL_POD_L, line);
        return;
    }
    if (n == 0) {
        return; /* Pod::Simple: "An empty L<>" is text */
    }
    pl_shape_t sh = pl_link_shape(content, n);
    size_t tb = sh.bar == SIZE_MAX ? 0 : sh.bar + SKIP_ONE;
    while (tb < n && content[tb] == ' ') {
        tb++;
    }
    if (sh.well_formed && pl_is_url(content + tb, n - tb)) {
        pl_push(em, content + tb, n - tb, CBM_DOCLINK_HREF, line);
        return;
    }
    pl_push(em, content, n, CBM_DOCLINK_PERL_POD_L, line);
}

/* The L<> codes of one paragraph's text em->text[b, e). */
static void pl_para_links(pl_emit_t *em, size_t b, size_t e) {
    const char *t = em->text;
    pl_codes_t st = {.depth = 0};
    pl_code_t link = {0};
    int link_depth = 0;   /* stack depth of the open L; 0: none */
    bool harvest = false; /* the open L is a reference */
    size_t i = b;
    while (i < e && !st.overflow) {
        size_t next = i;
        int ev = pl_code_step(&st, t, i, e, &next);
        if (ev == PL_EV_OPEN && link_depth == 0 && t[i] == 'L') {
            link_depth = st.depth;
            harvest = !(i > b && pl_word(t[i - SKIP_ONE]));
            link = (pl_code_t){.at = i, .cb = next, .ce = next, .closed = false};
        } else if (ev == PL_EV_CLOSE && link_depth != 0 && st.depth < link_depth) {
            link.ce = i;
            link.closed = true;
            if (harvest) {
                pl_emit_link(em, &link);
            }
            link_depth = 0;
        }
        i = next;
    }
    if (link_depth != 0 && harvest) {
        link.ce = e; /* never closed, or nested too deep */
        pl_emit_link(em, &link);
    }
}

/* First byte after the white space that follows s[i]. */
static size_t pl_skip_blank(const char *s, size_t i, size_t e) {
    while (i < e && pl_space(s[i])) {
        i++;
    }
    return i;
}

/* The L<> codes of the POD text em->text[b, e): from its first command on, or
 * from its start when `in_pod` (a run of the module doc, which is POD only). */
static void pl_harvest_pod(pl_emit_t *em, size_t b, size_t e, bool in_pod) {
    const char *t = em->text;
    size_t pos = b;
    size_t pb = 0;
    size_t pe = 0;
    int begin_depth = 0; /* open =begin regions */
    int data_from = 0;   /* depth of the outermost data region; 0: none */
    while (pl_next_para(t, &pos, e, &pb, &pe)) {
        bool cmd = pl_is_command(t, pb, pe);
        if (!cmd) {
            if (in_pod && data_from == 0 && t[pb] != ' ' && t[pb] != '\t') {
                pl_para_links(em, pb, pe);
            }
            continue;
        }
        size_t wb = pl_command_word_end(t, pb, pe);
        const char *w = t + pb + SKIP_ONE;
        size_t wl = wb - pb - SKIP_ONE;
        in_pod = !pl_word_is(w, wl, "cut");
        if (pl_word_is(w, wl, "begin")) {
            size_t f = pl_skip_blank(t, wb, pe);
            begin_depth++;
            if (data_from == 0 && !(f < pe && t[f] == ':')) {
                data_from = begin_depth;
            }
        } else if (pl_word_is(w, wl, "end") && begin_depth > 0) {
            data_from = begin_depth == data_from ? 0 : data_from;
            begin_depth--;
        } else if (data_from == 0 && (pl_head_level(w, wl) > 0 || pl_word_is(w, wl, "item"))) {
            pl_para_links(em, wb, pe);
        }
    }
}

/* ── The file's POD sections ─────────────────────────────────────── */

enum { PL_HEAD = 1, PL_ITEM, PL_OVER, PL_BACK, PL_CUT, PL_OTHER };

typedef struct {
    uint32_t sb; /* command paragraph */
    uint32_t eb;
    uint32_t wb; /* end of the command word */
    uint8_t type;
    uint8_t level; /* =headN: N; =item and =back: list depth */
} pl_cmd_t;

typedef struct {
    uint32_t sb; /* the heading's command paragraph */
    uint32_t eb; /* section end, trailing white space trimmed */
    uint32_t hb; /* heading text */
    uint32_t he;
    uint32_t line;    /* 1-based line of sb */
    const char *name; /* the identifier the heading names; NULL: none */
    uint32_t name_len;
    bool item;
} pl_sec_t;

typedef struct {
    CBMArena *a;
    const char *src;
    uint32_t len;
    pl_sec_t *secs;
    int nsecs;
    int cap_secs;
    pl_cmd_t *cmds; /* of the block being read */
    int ncmds;
    int cap_cmds;
    uint32_t line_pos; /* lines are counted from here ... */
    uint32_t line;     /* ... which is on this line */
    bool failed;
} pl_scan_t;

/* Room for one more element of `size` bytes in an arena array. */
static void *pl_grow(pl_scan_t *s, void *items, int count, int *cap, size_t size) {
    if (count < *cap) {
        return items;
    }
    int ncap = *cap ? *cap * PAIR_LEN : PL_INIT_CAP;
    void *grown = cbm_arena_alloc(s->a, (size_t)ncap * size);
    if (!grown) {
        s->failed = true;
        return NULL;
    }
    if (count > 0) {
        memcpy(grown, items, (size_t)count * size);
    }
    *cap = ncap;
    return grown;
}

/* Classify one command paragraph; `depth` is the =over nesting
 * (extract_defs.c's pod_classify). */
static pl_cmd_t pl_classify(const char *src, uint32_t sb, uint32_t eb, int *depth) {
    uint32_t wb = (uint32_t)pl_command_word_end(src, sb, eb);
    const char *w = src + sb + SKIP_ONE;
    size_t wl = wb - sb - SKIP_ONE;
    pl_cmd_t c = {sb, eb, wb, PL_OTHER, 0};
    if (pl_head_level(w, wl) > 0) {
        c.type = PL_HEAD;
        c.level = (uint8_t)pl_head_level(w, wl);
    } else if (pl_word_is(w, wl, "item")) {
        c.type = PL_ITEM;
        c.level = (uint8_t)*depth;
    } else if (pl_word_is(w, wl, "over")) {
        c.type = PL_OVER;
        (*depth)++;
    } else if (pl_word_is(w, wl, "back")) {
        c.type = PL_BACK;
        c.level = (uint8_t)*depth;
        if (*depth > 0) {
            (*depth)--;
        }
    } else if (pl_word_is(w, wl, "cut")) {
        c.type = PL_CUT;
    }
    return c;
}

/* The command paragraphs of the POD block src[pb, pe) into s->cmds. */
static void pl_block_commands(pl_scan_t *s, uint32_t pb, uint32_t pe) {
    s->ncmds = 0;
    int depth = 0;
    size_t pos = pb;
    size_t sb = 0;
    size_t eb = 0;
    while (!s->failed && pl_next_para(s->src, &pos, pe, &sb, &eb)) {
        if (!pl_is_command(s->src, sb, eb)) {
            continue;
        }
        pl_cmd_t *cmds = (pl_cmd_t *)pl_grow(s, s->cmds, s->ncmds, &s->cap_cmds, sizeof(pl_cmd_t));
        if (!cmds) {
            return;
        }
        s->cmds = cmds;
        s->cmds[s->ncmds++] = pl_classify(s->src, (uint32_t)sb, (uint32_t)eb, &depth);
    }
}

/* End byte of the section that cmds[ci] (a =head or =item) opens
 * (extract_defs.c's pod_section_end). */
static uint32_t pl_section_end(const pl_cmd_t *cmds, int n, int ci, uint32_t pe) {
    const pl_cmd_t *c = &cmds[ci];
    for (int j = ci + SKIP_ONE; j < n; j++) {
        const pl_cmd_t *d = &cmds[j];
        if (d->type == PL_CUT) {
            return d->sb;
        }
        if (d->type == PL_HEAD && (c->type == PL_ITEM || d->level <= c->level)) {
            return d->sb;
        }
        if (c->type == PL_ITEM && (d->type == PL_ITEM || d->type == PL_BACK) &&
            d->level == c->level) {
            return d->sb;
        }
    }
    return pe;
}

/* A heading's text with formatting codes removed and newlines and tabs as
 * spaces (extract_defs.c's pod_plain_heading, byte for byte: the name it
 * yields decides which sub a section is stored on). */
static char *pl_plain_heading(CBMArena *a, const char *src, uint32_t b, uint32_t e) {
    char *buf = (char *)cbm_arena_alloc(a, (size_t)(e - b) + SKIP_ONE);
    if (!buf) {
        return NULL;
    }
    size_t w = 0;
    for (uint32_t i = b; i < e; i++) {
        char c = src[i];
        if (pl_upper(c) && i + SKIP_ONE < e && src[i + SKIP_ONE] == '<') {
            uint32_t j = i + SKIP_ONE;
            while (j < e && src[j] == '<') {
                j++;
            }
            if (j - i > PAIR_LEN && j < e && (src[j] == ' ' || src[j] == '\t')) {
                j++; /* X<< text >> */
            }
            i = j - SKIP_ONE;
            continue;
        }
        if (c == '>' && !(w > 0 && buf[w - SKIP_ONE] == '-')) {
            continue;
        }
        buf[w++] = (c == '\n' || c == '\r' || c == '\t') ? ' ' : c;
    }
    buf[w] = '\0';
    return buf;
}

static const char *pl_skip_spaces(const char *p) {
    while (*p == ' ') {
        p++;
    }
    return p;
}

/* The sub a heading names: after a list bullet ("*", "1."), a `$obj->`
 * receiver and a sigil, the identifier that follows; its length, 0 for none
 * (extract_defs.c's pod_heading_name). */
static size_t pl_heading_name(CBMArena *a, const char *src, uint32_t b, uint32_t e,
                              const char **name) {
    const char *buf = pl_plain_heading(a, src, b, e);
    if (!buf) {
        return 0;
    }
    const char *p = pl_skip_spaces(buf);
    if (*p == '*') {
        p++;
    } else if (isdigit((unsigned char)*p)) {
        while (isdigit((unsigned char)*p)) {
            p++;
        }
        if (*p == '.') {
            p++;
        }
    }
    p = pl_skip_spaces(p);
    if (*p == '$') {
        const char *q = p + SKIP_ONE;
        while (pl_word(*q)) {
            q++;
        }
        q = pl_skip_spaces(q);
        if (q > p + SKIP_ONE && q[0] == '-' && q[SKIP_ONE] == '>') {
            p = pl_skip_spaces(q + PAIR_LEN);
        }
    }
    if (*p == '$' || *p == '@' || *p == '%' || *p == '&') {
        p++;
    }
    if (!(isalpha((unsigned char)*p) || *p == '_')) {
        return 0;
    }
    const char *s = p;
    while (pl_word(*p)) {
        p++;
    }
    *name = s;
    return (size_t)(p - s);
}

static uint32_t pl_trim_end(const char *src, uint32_t sb, uint32_t eb) {
    while (eb > sb && pl_space(src[eb - SKIP_ONE])) {
        eb--;
    }
    return eb;
}

/* The 1-based line of src[at]; `at` never decreases between calls. */
static uint32_t pl_scan_line(pl_scan_t *s, uint32_t at) {
    for (uint32_t k = s->line_pos; k < at; k++) {
        s->line += s->src[k] == '\n';
    }
    s->line_pos = at > s->line_pos ? at : s->line_pos;
    return s->line;
}

/* The =head / =item sections of the POD block src[pb, pe). */
static void pl_block_sections(pl_scan_t *s, uint32_t pb, uint32_t pe) {
    pl_block_commands(s, pb, pe);
    for (int ci = 0; ci < s->ncmds && !s->failed; ci++) {
        const pl_cmd_t *c = &s->cmds[ci];
        if (c->type != PL_HEAD && c->type != PL_ITEM) {
            continue;
        }
        pl_sec_t *secs = (pl_sec_t *)pl_grow(s, s->secs, s->nsecs, &s->cap_secs, sizeof(pl_sec_t));
        if (!secs) {
            return;
        }
        s->secs = secs;
        pl_sec_t sec = {.sb = c->sb, .hb = c->wb, .he = c->eb, .item = c->type == PL_ITEM};
        sec.eb = pl_trim_end(s->src, c->sb, pl_section_end(s->cmds, s->ncmds, ci, pe));
        sec.line = pl_scan_line(s, c->sb);
        sec.name_len = (uint32_t)pl_heading_name(s->a, s->src, c->wb, c->eb, &sec.name);
        s->secs[s->nsecs++] = sec;
    }
}

/* True when the line src[i, le) is the `=cut` that ends a POD block. */
static bool pl_is_cut_line(const char *src, uint32_t i, uint32_t le) {
    static const char cut[] = "=cut";
    uint32_t n = (uint32_t)(sizeof(cut) - SKIP_ONE);
    return le - i >= n && memcmp(src + i, cut, n) == 0 && (le - i == n || !pl_word(src[i + n]));
}

/* Every POD section of the file. A block runs from a line that starts with
 * `=` and a letter through the next line that starts with `=cut`, or to the
 * end of the file: what perl skips and perldoc reads. */
static void pl_scan_file(pl_scan_t *s) {
    bool in_pod = false;
    uint32_t pb = 0;
    uint32_t i = 0;
    while (i < s->len && !s->failed) {
        const char *nl = (const char *)memchr(s->src + i, '\n', s->len - i);
        uint32_t le = nl ? (uint32_t)(nl - s->src) : s->len;
        if (!in_pod && pl_is_command(s->src, i, le)) {
            in_pod = true;
            pb = i;
        }
        if (in_pod && pl_is_cut_line(s->src, i, le)) {
            pl_block_sections(s, pb, le);
            in_pod = false;
        }
        i = le < s->len ? le + SKIP_ONE : s->len;
    }
    if (in_pod && !s->failed) {
        pl_block_sections(s, pb, s->len);
    }
}

static void pl_scan_init(pl_scan_t *s, CBMExtractCtx *ctx) {
    memset(s, 0, sizeof(*s));
    s->a = pl_scratch(ctx);
    s->src = ctx->source;
    s->len = (ctx->source && ctx->source_len > 0) ? (uint32_t)ctx->source_len : 0;
    s->line = SKIP_ONE;
}

/* ── Docs -> tokens ──────────────────────────────────────────────── */

typedef struct {
    const char *name;
    uint32_t len;
    int sec;
} pl_named_t;

static int pl_name_cmp(const char *a, size_t al, const char *b, size_t bl) {
    int c = memcmp(a, b, al < bl ? al : bl);
    if (c != 0) {
        return c;
    }
    return (al > bl) - (al < bl);
}

static int pl_named_cmp(const void *a, const void *b) {
    const pl_named_t *x = (const pl_named_t *)a;
    const pl_named_t *y = (const pl_named_t *)b;
    int c = pl_name_cmp(x->name, x->len, y->name, y->len);
    return c != 0 ? c : (x->sec > y->sec) - (x->sec < y->sec);
}

/* The sections that name something, sorted by name and then by position. */
static pl_named_t *pl_named_sections(const pl_scan_t *s, int *count) {
    *count = 0;
    pl_named_t *named =
        (pl_named_t *)cbm_arena_alloc(s->a, (size_t)(s->nsecs + SKIP_ONE) * sizeof(*named));
    if (!named) {
        return NULL;
    }
    int n = 0;
    for (int i = 0; i < s->nsecs; i++) {
        if (s->secs[i].name_len > 0) {
            named[n++] = (pl_named_t){s->secs[i].name, s->secs[i].name_len, i};
        }
    }
    qsort(named, (size_t)n, sizeof(*named), pl_named_cmp);
    *count = n;
    return named;
}

/* Index of the first section named `name`, or `count`. */
static int pl_named_first(const pl_named_t *named, int count, const char *name) {
    size_t nl = strlen(name);
    int lo = 0;
    int hi = count;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (pl_name_cmp(named[mid].name, named[mid].len, name, nl) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* True when doc[o, dl) starts with the section `sec`, alone or followed by the
 * blank line that joins it to the next one. */
static bool pl_piece_is(const pl_scan_t *s, const pl_sec_t *sec, const char *doc, size_t o,
                        size_t dl) {
    size_t n = sec->eb - sec->sb;
    if (n == 0 || n > dl - o || memcmp(doc + o, s->src + sec->sb, n) != 0) {
        return false;
    }
    return o + n == dl ||
           (dl - o - n >= PAIR_LEN && doc[o + n] == '\n' && doc[o + n + SKIP_ONE] == '\n');
}

/* One definition's doc -> tokens: every piece of it that is a section of the
 * file is read where it stands, so its references carry their own lines. */
static void pl_harvest_def(CBMExtractCtx *ctx, const pl_scan_t *s, const pl_named_t *named,
                           int count, const CBMDefinition *def, uint32_t fallback_line) {
    const char *doc = def->docstring;
    size_t dl = strlen(doc);
    if (!pl_is_command(doc, 0, dl)) {
        return; /* a `#` comment: no formatting codes */
    }
    size_t nl = strlen(def->name);
    size_t o = 0;
    int cand = named ? pl_named_first(named, count, def->name) : count;
    for (; o < dl && cand < count &&
           pl_name_cmp(named[cand].name, named[cand].len, def->name, nl) == 0;
         cand++) {
        const pl_sec_t *sec = &s->secs[named[cand].sec];
        if (!pl_piece_is(s, sec, doc, o, dl)) {
            continue; /* a section the doc does not hold */
        }
        pl_emit_t em = {
            .ctx = ctx, .def = def, .text = s->src, .line_pos = sec->sb, .line = sec->line};
        pl_harvest_pod(&em, sec->sb, sec->eb, false);
        o += sec->eb - sec->sb;
        o += o < dl ? PAIR_LEN : 0;
    }
    if (o < dl) { /* text whose place in the file is not known */
        pl_emit_t em = {
            .ctx = ctx, .def = def, .text = doc, .line = fallback_line, .fixed_line = true};
        pl_harvest_pod(&em, o, dl, false);
    }
}

/* The core's filter: a definition whose doc is harvested. */
static bool pl_documented(const CBMDefinition *d) {
    return d->docstring && d->docstring[0] && d->qualified_name && d->name;
}

typedef struct {
    const char *qn;
    int idx;
} pl_docdef_t;

static int pl_docdef_cmp(const void *a, const void *b) {
    const pl_docdef_t *x = (const pl_docdef_t *)a;
    const pl_docdef_t *y = (const pl_docdef_t *)b;
    int c = strcmp(x->qn, y->qn);
    return c != 0 ? c : (x->idx > y->idx) - (x->idx < y->idx);
}

/* twin[i]: definition i repeats the qualified name AND the doc of an earlier
 * one (the same sub name in two packages of a file is one node with one doc):
 * its references are the earlier one's. NULL when there is nothing to mark. */
static bool *pl_twins(CBMExtractCtx *ctx) {
    const CBMDefArray *defs = &ctx->result->defs;
    CBMArena *a = pl_scratch(ctx);
    pl_docdef_t *docs = (pl_docdef_t *)cbm_arena_alloc(a, (size_t)defs->count * sizeof(*docs));
    bool *twin = (bool *)cbm_arena_calloc(a, (size_t)defs->count * sizeof(bool));
    if (!docs || !twin) {
        return NULL;
    }
    int n = 0;
    for (int i = 0; i < defs->count; i++) {
        if (pl_documented(&defs->items[i])) {
            docs[n++] = (pl_docdef_t){defs->items[i].qualified_name, i};
        }
    }
    qsort(docs, (size_t)n, sizeof(*docs), pl_docdef_cmp);
    int first = 0;
    for (int i = SKIP_ONE; i < n; i++) {
        if (strcmp(docs[i].qn, docs[first].qn) != 0) {
            first = i;
        } else {
            twin[docs[i].idx] = strcmp(defs->items[docs[i].idx].docstring,
                                       defs->items[docs[first].idx].docstring) == 0;
        }
    }
    return twin;
}

/* Every documented definition of the file -> tokens, in definition order. */
static void pl_harvest_file(CBMExtractCtx *ctx) {
    pl_scan_t s;
    pl_scan_init(&s, ctx);
    pl_scan_file(&s);
    int count = 0;
    pl_named_t *named = s.failed ? NULL : pl_named_sections(&s, &count);
    const bool *twin = pl_twins(ctx);
    const CBMDefArray *defs = &ctx->result->defs;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (pl_documented(d) && !(twin && twin[i])) {
            pl_harvest_def(ctx, &s, named, named ? count : 0, d, d->start_line);
        }
    }
}

/* First position >= from of `needle` (n bytes) in src[0, len); SIZE_MAX: none. */
static size_t pl_find(const char *src, size_t from, size_t len, const char *needle, size_t n) {
    if (n == 0 || n > len) {
        return SIZE_MAX;
    }
    for (size_t i = from; i + n <= len; i++) {
        const char *hit = (const char *)memchr(src + i, needle[0], len - n + SKIP_ONE - i);
        if (!hit) {
            return SIZE_MAX;
        }
        i = (size_t)(hit - src);
        if (memcmp(src + i, needle, n) == 0) {
            return i;
        }
    }
    return SIZE_MAX;
}

/* The module doc (the core's File stand-in): runs of the file's POD joined by
 * a blank line (extract_defs.c, doc_perl_module), each found again in the
 * source after the one before, so that a reference gets the line it is written
 * on. Text that is not found keeps the doc's first line. */
static void pl_harvest_module(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line) {
    const char *src = ctx->source;
    size_t sl = ctx->source_len > 0 ? (size_t)ctx->source_len : 0;
    size_t dl = strlen(doc);
    size_t o = 0;
    size_t at = 0;
    size_t line_pos = 0;
    uint32_t line = SKIP_ONE;
    while (src && o < dl) {
        size_t le = o;
        while (le < dl && doc[le] != '\n') {
            le++;
        }
        size_t p = pl_find(src, at, sl, doc + o, le - o);
        if (p == SIZE_MAX) {
            break;
        }
        size_t k = 0;
        while (o + k < dl && p + k < sl && doc[o + k] == src[p + k]) {
            k++;
        }
        for (; line_pos < p; line_pos++) {
            line += src[line_pos] == '\n' ? SKIP_ONE : 0;
        }
        pl_emit_t em = {.ctx = ctx, .def = def, .text = src, .line_pos = p, .line = line};
        pl_harvest_pod(&em, p, p + k, true);
        o += k;
        at = p + k;
        while (o < dl && doc[o] == '\n') {
            o++; /* the blank line that joins two runs */
        }
    }
    if (o < dl) {
        pl_emit_t em = {.ctx = ctx, .def = def, .text = doc, .line = doc_line, .fixed_line = true};
        pl_harvest_pod(&em, o, dl, true);
    }
}

/* Index of `def` in the file's definitions; -1 when it is not one of them. */
static int pl_def_index(const CBMDefArray *defs, const CBMDefinition *def) {
    uintptr_t base = (uintptr_t)defs->items;
    uintptr_t p = (uintptr_t)def;
    if (!defs->items || defs->count <= 0 || p < base) {
        return CBM_NOT_FOUND;
    }
    uintptr_t off = p - base;
    if (off % sizeof(CBMDefinition) != 0 || off / sizeof(CBMDefinition) >= (uintptr_t)defs->count) {
        return CBM_NOT_FOUND;
    }
    return (int)(off / sizeof(CBMDefinition));
}

void cbm_doclink_perl_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                                uint32_t doc_line) {
    if (!ctx || !ctx->result || !def || !doc || !def->qualified_name) {
        return;
    }
    if (def->label && strcmp(def->label, "File") == 0) {
        pl_harvest_module(ctx, def, doc, doc_line); /* the file's module doc */
        return;
    }
    const CBMDefArray *defs = &ctx->result->defs;
    int idx = pl_def_index(defs, def);
    if (idx < 0 || doc != def->docstring) {
        /* a doc that is not a listed definition's own: nothing says where in
         * the file it stands, so its references get the line the core passed */
        if (pl_is_command(doc, 0, strlen(doc))) {
            pl_emit_t em = {
                .ctx = ctx, .def = def, .text = doc, .line = doc_line, .fixed_line = true};
            pl_harvest_pod(&em, 0, strlen(doc), false);
        }
        return;
    }
    for (int i = idx - SKIP_ONE; i >= 0; i--) {
        if (pl_documented(&defs->items[i])) {
            return; /* harvested with the file's first documented definition */
        }
    }
    pl_harvest_file(ctx);
}

/* ── Scope blob ──────────────────────────────────────────────────── */

typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool failed;
} pl_sb_t;

static void pl_sb_put(pl_sb_t *sb, const char *s, size_t n) {
    if (sb->failed) {
        return;
    }
    if (sb->len + n + SKIP_ONE > sb->cap) {
        size_t ncap = sb->cap ? sb->cap : PL_SB_INIT;
        while (ncap < sb->len + n + SKIP_ONE) {
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
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

static void pl_sb_puts(pl_sb_t *sb, const char *s) {
    pl_sb_put(sb, s, strlen(s));
}

enum { PL_PKG_MAIN = -1, PL_PKG_UNKNOWN = -2 };

typedef struct {
    const char *name; /* in the source */
    uint32_t len;
    int order; /* declaration order in the file */
    int index; /* of its P record; set by pl_number_packages */
} pl_pkg_t;

typedef struct {
    const char *name; /* in the source */
    uint32_t len;
    uint32_t line;
    int pkg; /* declaration (pl_pkg_t.order) it stands in, or PL_PKG_* */
} pl_subdecl_t;

typedef struct {
    pl_scan_t *s; /* arena and growth */
    pl_pkg_t *pkgs;
    int npkgs;
    int cap_pkgs;
    pl_subdecl_t *subs;
    int nsubs;
    int cap_subs;
    int *saved; /* the package in force outside each open block */
    int nsaved;
    int cap_saved;
    int current;       /* declaration in force, or PL_PKG_* */
    int pending_block; /* declaration whose block comes next, or PL_PKG_UNKNOWN */
    bool pending;
} pl_walk_t;

static bool pl_kind(TSNode n, const char *kind) {
    return strcmp(ts_node_type(n), kind) == 0;
}

/* A package or sub name as a blob field: word characters, `:` and `'`. */
static bool pl_name_ok(const char *p, size_t n) {
    if (n == 0 || n > PL_NAME_MAX) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!pl_word(p[i]) && p[i] != ':' && p[i] != '\'') {
            return false;
        }
    }
    return true;
}

/* The source text of a declaration's `name` field. */
static bool pl_decl_name(const pl_walk_t *w, TSNode decl, const char **name, uint32_t *len) {
    static const char field[] = "name";
    TSNode n = ts_node_child_by_field_name(decl, field, (uint32_t)(sizeof(field) - SKIP_ONE));
    if (ts_node_is_null(n)) {
        return false;
    }
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    if (e <= b || e > w->s->len) {
        return false;
    }
    *name = w->s->src + b;
    *len = e - b;
    return pl_name_ok(*name, *len);
}

static bool pl_has_block(TSNode decl) {
    uint32_t n = ts_node_child_count(decl);
    for (uint32_t i = 0; i < n; i++) {
        if (pl_kind(ts_node_child(decl, i), "block")) {
            return true;
        }
    }
    return false;
}

/* `package NAME;` sets the package to the end of the enclosing block;
 * `package NAME { .. }` sets it inside its own block only. */
static void pl_walk_package(pl_walk_t *w, TSNode decl) {
    const char *name = NULL;
    uint32_t len = 0;
    int id = PL_PKG_UNKNOWN;
    if (pl_decl_name(w, decl, &name, &len)) {
        pl_pkg_t *pkgs =
            (pl_pkg_t *)pl_grow(w->s, w->pkgs, w->npkgs, &w->cap_pkgs, sizeof(pl_pkg_t));
        if (!pkgs) {
            return;
        }
        w->pkgs = pkgs;
        id = w->npkgs;
        w->pkgs[w->npkgs++] = (pl_pkg_t){.name = name, .len = len, .order = id};
    }
    if (pl_has_block(decl)) {
        w->pending = true;
        w->pending_block = id;
    } else {
        w->current = id;
    }
}

static void pl_walk_sub(pl_walk_t *w, TSNode decl) {
    const char *name = NULL;
    uint32_t len = 0;
    if (!pl_decl_name(w, decl, &name, &len)) {
        return;
    }
    pl_subdecl_t *subs =
        (pl_subdecl_t *)pl_grow(w->s, w->subs, w->nsubs, &w->cap_subs, sizeof(pl_subdecl_t));
    if (!subs) {
        return;
    }
    w->subs = subs;
    w->subs[w->nsubs++] = (pl_subdecl_t){.name = name,
                                         .len = len,
                                         .line = ts_node_start_point(decl).row + TS_LINE_OFFSET,
                                         .pkg = w->current};
}

static bool pl_is_scope(TSNode n) {
    return pl_kind(n, "block") || pl_kind(n, "block_statement");
}

/* Enter a node; false when its children are not walked (a sub's body and an
 * expression hold no declaration this blob lists). */
static bool pl_walk_enter(pl_walk_t *w, TSNode n) {
    if (pl_kind(n, "subroutine_declaration_statement")) {
        pl_walk_sub(w, n);
        return false;
    }
    if (pl_kind(n, "expression_statement") || pl_kind(n, "pod") ||
        pl_kind(n, "method_declaration_statement")) {
        return false;
    }
    if (pl_kind(n, "package_statement") || pl_kind(n, "class_statement") ||
        pl_kind(n, "role_statement")) {
        pl_walk_package(w, n);
        return true;
    }
    if (pl_is_scope(n)) {
        int *saved = (int *)pl_grow(w->s, w->saved, w->nsaved, &w->cap_saved, sizeof(int));
        if (!saved) {
            return false;
        }
        w->saved = saved;
        w->saved[w->nsaved++] = w->current;
        if (w->pending) {
            w->current = w->pending_block;
            w->pending = false;
        }
    }
    return true;
}

static void pl_walk_leave(pl_walk_t *w, TSNode n) {
    if (pl_is_scope(n) && w->nsaved > 0) {
        w->current = w->saved[--w->nsaved];
    }
}

/* Packages and subs of the tree, with the package each sub stands in. An
 * iterative walk: the nesting depth is the input's. */
static void pl_walk_tree(pl_walk_t *w, TSNode root) {
    TSTreeCursor cur = ts_tree_cursor_new(root);
    bool more = true;
    while (more && !w->s->failed) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        bool entered = pl_walk_enter(w, n);
        if (entered && ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        if (entered) {
            pl_walk_leave(w, n);
        }
        while (more && !ts_tree_cursor_goto_next_sibling(&cur)) {
            more = ts_tree_cursor_goto_parent(&cur);
            if (more) {
                pl_walk_leave(w, ts_tree_cursor_current_node(&cur));
            }
        }
    }
    ts_tree_cursor_delete(&cur);
}

static int pl_pkg_name_cmp(const void *a, const void *b) {
    const pl_pkg_t *x = (const pl_pkg_t *)a;
    const pl_pkg_t *y = (const pl_pkg_t *)b;
    int c = pl_name_cmp(x->name, x->len, y->name, y->len);
    return c != 0 ? c : (x->order > y->order) - (x->order < y->order);
}

static int pl_pkg_order_cmp(const void *a, const void *b) {
    const pl_pkg_t *x = (const pl_pkg_t *)a;
    const pl_pkg_t *y = (const pl_pkg_t *)b;
    return (x->order > y->order) - (x->order < y->order);
}

/* Give every declaration the index of its name's P record (names in order of
 * their first declaration) and write the P records. */
static void pl_number_packages(pl_walk_t *w, pl_sb_t *sb) {
    qsort(w->pkgs, (size_t)w->npkgs, sizeof(pl_pkg_t), pl_pkg_name_cmp);
    for (int i = 0; i < w->npkgs; i++) { /* index = order of the name's first declaration */
        bool repeat =
            i > 0 && pl_name_cmp(w->pkgs[i].name, w->pkgs[i].len, w->pkgs[i - SKIP_ONE].name,
                                 w->pkgs[i - SKIP_ONE].len) == 0;
        w->pkgs[i].index = repeat ? w->pkgs[i - SKIP_ONE].index : w->pkgs[i].order;
    }
    qsort(w->pkgs, (size_t)w->npkgs, sizeof(pl_pkg_t), pl_pkg_order_cmp);
    int records = 0;
    for (int i = 0; i < w->npkgs; i++) {
        pl_pkg_t *p = &w->pkgs[i];
        if (p->index != p->order) {
            p->index = w->pkgs[p->index].index; /* its first declaration's record */
            continue;
        }
        p->index = records++;
        pl_sb_puts(sb, "P\t");
        pl_sb_put(sb, p->name, p->len);
        pl_sb_puts(sb, "\n");
    }
}

static int pl_subdecl_cmp(const void *a, const void *b) {
    const pl_subdecl_t *x = (const pl_subdecl_t *)a;
    const pl_subdecl_t *y = (const pl_subdecl_t *)b;
    if (x->line != y->line) {
        return x->line < y->line ? -1 : 1;
    }
    return pl_name_cmp(x->name, x->len, y->name, y->len);
}

/* One S record per sub definition of the file, with the package the tree
 * places it in. */
static void pl_put_subs(CBMExtractCtx *ctx, pl_walk_t *w, pl_sb_t *sb) {
    qsort(w->subs, (size_t)w->nsubs, sizeof(pl_subdecl_t), pl_subdecl_cmp);
    const CBMDefArray *defs = &ctx->result->defs;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (!d->name || !d->label || strcmp(d->label, "Function") != 0 ||
            !pl_name_ok(d->name, strlen(d->name))) {
            continue;
        }
        pl_subdecl_t key = {
            .name = d->name, .len = (uint32_t)strlen(d->name), .line = d->start_line};
        const pl_subdecl_t *hit =
            w->nsubs > 0 ? (const pl_subdecl_t *)bsearch(&key, w->subs, (size_t)w->nsubs,
                                                         sizeof(pl_subdecl_t), pl_subdecl_cmp)
                         : NULL;
        int pkg = hit ? hit->pkg : PL_PKG_UNKNOWN;
        char field[CBM_SZ_32];
        if (pkg >= 0) {
            snprintf(field, sizeof(field), "S\t%d\t", w->pkgs[pkg].index);
        } else {
            snprintf(field, sizeof(field), "S\t%s\t", pkg == PL_PKG_MAIN ? "-" : "?");
        }
        pl_sb_puts(sb, field);
        pl_sb_puts(sb, d->name);
        pl_sb_puts(sb, "\n");
    }
}

static void pl_put_heading(pl_sb_t *sb, const pl_sec_t *sec, const char *key) {
    if (!key[0]) {
        return;
    }
    pl_sb_puts(sb, "H\t");
    if (sec->name_len > 0) {
        pl_sb_put(sb, sec->name, sec->name_len);
    } else {
        pl_sb_puts(sb, "-");
    }
    pl_sb_puts(sb, "\t");
    pl_sb_puts(sb, key);
    pl_sb_puts(sb, "\n");
}

/* The text after an =item's bullet (`*`) or number (`1.`), or NULL when the
 * item has neither. */
static const char *pl_after_bullet(const char *key) {
    const char *p = key;
    if (*p == '*') {
        p++;
    } else {
        while (isdigit((unsigned char)*p)) {
            p++;
        }
        if (p == key) {
            return NULL;
        }
        p += *p == '.';
    }
    return (*p == ' ' || *p == '\0') ? pl_skip_spaces(p) : NULL;
}

/* One H record per heading. Pod::Simple reads `=item * text` and `=item 1.
 * text` as a bullet and its text when the list is a bullet or numbered list,
 * and as the whole text otherwise; which kind a list is depends on its first
 * item, so a numbered text is recorded both ways. */
static void pl_put_headings(const pl_scan_t *s, pl_sb_t *sb) {
    for (int i = 0; i < s->nsecs; i++) {
        const pl_sec_t *sec = &s->secs[i];
        char key[CBM_DOCLINK_PERL_KEY_MAX];
        cbm_doclink_perl_plain_text(s->src + sec->hb, sec->he - sec->hb, key, sizeof(key));
        const char *rest = sec->item ? pl_after_bullet(key) : NULL;
        if (!rest) {
            pl_put_heading(sb, sec, key);
        } else if (key[0] == '*') {
            pl_put_heading(sb, sec, rest);
        } else if (rest[0]) {
            pl_put_heading(sb, sec, key);
            pl_put_heading(sb, sec, rest);
        }
    }
}

const char *cbm_doclink_perl_scan_scope(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || ts_node_is_null(ctx->root)) {
        return NULL;
    }
    pl_scan_t s;
    pl_scan_init(&s, ctx);
    pl_sb_t sb = {.a = ctx->arena};
    pl_sb_puts(&sb, CBM_DOCLINK_PERL_SCOPE_TAG "\n");
    if (ts_node_has_error(ctx->root)) {
        pl_sb_puts(&sb, "E\n");
    }
    pl_walk_t w = {.s = &s, .current = PL_PKG_MAIN, .pending_block = PL_PKG_UNKNOWN};
    pl_walk_tree(&w, ctx->root);
    pl_number_packages(&w, &sb);
    pl_put_subs(ctx, &w, &sb);
    pl_scan_file(&s);
    pl_put_headings(&s, &sb);
    if (s.failed || sb.failed || !sb.buf) {
        return NULL;
    }
    return sb.buf;
}
