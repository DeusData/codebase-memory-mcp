/*
 * doclink_cs.c — C# doc-comment references and the C# doc-link scope.
 *
 * Tokens: the cref of <see>, <seealso>, <exception> and <inheritdoc>, and the
 * href of <see>/<seealso> (external). <paramref>/<typeparamref> name the
 * definition's own parameters and are not references; <include> pulls doc
 * text from another file.
 *
 * Scope blob (one record per line, tab-separated, first line "cs1"; records
 * in document order, so a member follows its type):
 *   X  from  to                                lines whose declarations could
 *                                              not be placed (the braces stop
 *                                              pairing, a namespace cannot be
 *                                              named): a definition there has
 *                                              no known scope
 *   R  id  parent  start  end  namespace        namespace region (0 = file)
 *   U  region  kind  alias  target             using: n namespace, s static,
 *                                              a alias, g global
 *   T  region  start  end  kind  path  tparams  bases
 *                                              type: c s i e r d (class,
 *                                              struct, interface, enum,
 *                                              record, delegate), followed by
 *                                              '!' when a parse error hides
 *                                              some of its members; path is
 *                                              the dotted local path
 *                                              (Outer.Inner), tparams
 *                                              ','-joined, bases '|'-joined as
 *                                              written
 *   M  start  kind  explicit  path  tparams  sig
 *                                              member: c callable (method,
 *                                              constructor), v value
 *                                              (property, field, constant,
 *                                              enum member, record parameter),
 *                                              e event; sig '|'-joined
 *                                              normalized parameter types, '-'
 *                                              for a non-callable
 *   Q  name                                    a type that is declared, but
 *                                              whose namespace or outer type
 *                                              could not be established
 * Everything is derived from the tree alone, so the blob is a pure function
 * of the file's bytes. Nesting is the tree's while the tree has no parse
 * error, and the braces' otherwise (see "Braces" below).
 */
#include "doclink.h"

#include "arena.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"
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
} cs_sb_t;

enum { CS_SB_INIT = 1024, CS_SCAN_MAX_DEPTH = 256, CS_UINT_DIGITS = 16, CS_NAME_MAX = 512 };

static void sb_reserve(cs_sb_t *sb, size_t extra) {
    if (sb->failed || sb->len + extra + SKIP_ONE <= sb->cap) {
        return;
    }
    size_t ncap = sb->cap ? sb->cap : CS_SB_INIT;
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

static void sb_putn(cs_sb_t *sb, const char *s, size_t n) {
    sb_reserve(sb, n);
    if (sb->failed) {
        return;
    }
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

static void sb_puts(cs_sb_t *sb, const char *s) {
    sb_putn(sb, s, strlen(s));
}

static void sb_putc(cs_sb_t *sb, char c) {
    sb_putn(sb, &c, SKIP_ONE);
}

static void sb_putu(cs_sb_t *sb, uint32_t v) {
    char tmp[CS_UINT_DIGITS];
    int n = snprintf(tmp, sizeof(tmp), "%u", v);
    if (n > 0) {
        sb_putn(sb, tmp, (size_t)n);
    }
}

/* ── Doc-comment references ──────────────────────────────────────── */

static int cs_tag_syntax(const char *name, size_t len) {
    static const struct {
        const char *name;
        int syntax;
    } tags[] = {
        {"see", CBM_DOCLINK_CS_SEE},
        {"seealso", CBM_DOCLINK_CS_SEEALSO},
        {"exception", CBM_DOCLINK_CS_EXCEPTION},
        {"inheritdoc", CBM_DOCLINK_CS_INHERITDOC},
    };
    for (size_t i = 0; i < sizeof(tags) / sizeof(tags[0]); i++) {
        if (strlen(tags[i].name) == len && memcmp(tags[i].name, name, len) == 0) {
            return tags[i].syntax;
        }
    }
    return CBM_DOCLINK_NONE;
}

static bool cs_attr_name_char(char c) {
    return isalnum((unsigned char)c) || c == '_' || c == ':' || c == '.' || c == '-';
}

/* Decode the five XML entities, drop the comment prefix (`///`, ` * `) that
 * follows a line break inside a value, collapse whitespace, trim. */
static const char *cs_clean_value(CBMArena *a, const char *v, size_t n) {
    char *out = (char *)cbm_arena_alloc(a, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    static const struct {
        const char *ent;
        char ch;
    } ents[] = {{"&lt;", '<'}, {"&gt;", '>'}, {"&amp;", '&'}, {"&quot;", '"'}, {"&apos;", '\''}};
    size_t w = 0;
    bool space = false;
    size_t i = 0;
    while (i < n) {
        char c = v[i];
        if (c == '\n' || c == '\r') {
            i++;
            while (i < n && (v[i] == ' ' || v[i] == '\t' || v[i] == '\r' || v[i] == '\n')) {
                i++;
            }
            static const char line_doc[] = "///";
            size_t ld = sizeof(line_doc) - SKIP_ONE;
            if (i + ld <= n && memcmp(v + i, line_doc, ld) == 0) {
                i += ld; /* the next `///` line of the same comment */
            } else if (i < n && v[i] == '*' && !(i + SKIP_ONE < n && v[i + SKIP_ONE] == '/')) {
                i++; /* a block comment's leading star */
            }
            space = true;
            continue;
        }
        if (c == ' ' || c == '\t') {
            space = true;
            i++;
            continue;
        }
        if (space && w > 0) {
            out[w++] = ' ';
        }
        space = false;
        if (c == '&') {
            bool decoded = false;
            for (size_t e = 0; e < sizeof(ents) / sizeof(ents[0]); e++) {
                size_t el = strlen(ents[e].ent);
                if (i + el <= n && memcmp(v + i, ents[e].ent, el) == 0) {
                    out[w++] = ents[e].ch;
                    i += el;
                    decoded = true;
                    break;
                }
            }
            if (decoded) {
                continue;
            }
        }
        out[w++] = c;
        i++;
    }
    out[w] = '\0';
    return out;
}

void cbm_doclink_cs_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line) {
    const char *p = doc;
    const char *counted = doc;
    uint32_t line = doc_line;
    while ((p = strchr(p, '<')) != NULL) {
        const char *tag = p;
        const char *q = p + SKIP_ONE;
        while (*q == ' ' || *q == '\t') {
            q++;
        }
        const char *name = q;
        while (isalpha((unsigned char)*q)) {
            q++;
        }
        int syntax = cs_tag_syntax(name, (size_t)(q - name));
        if (syntax == CBM_DOCLINK_NONE ||
            !(*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r' || *q == '/' || *q == '>')) {
            p = tag + SKIP_ONE;
            continue;
        }
        const char *cref = NULL;
        size_t cref_len = 0;
        const char *href = NULL;
        size_t href_len = 0;
        bool closed = false;
        while (*q) {
            if (*q == '>') {
                closed = true;
                q++;
                break;
            }
            if (*q == '<') {
                break; /* malformed: another tag starts first */
            }
            if (*q == '"' || *q == '\'') {
                const char *end = strchr(q + SKIP_ONE, *q);
                if (!end) {
                    break;
                }
                q = end + SKIP_ONE;
                continue;
            }
            if (!cs_attr_name_char(*q)) {
                q++;
                continue;
            }
            const char *an = q;
            while (cs_attr_name_char(*q)) {
                q++;
            }
            size_t an_len = (size_t)(q - an);
            const char *r = q;
            while (*r == ' ' || *r == '\t' || *r == '\n' || *r == '\r') {
                r++;
            }
            if (*r != '=') {
                continue;
            }
            r++;
            while (*r == ' ' || *r == '\t' || *r == '\n' || *r == '\r') {
                r++;
            }
            if (*r != '"' && *r != '\'') {
                q = r;
                continue;
            }
            const char *vend = strchr(r + SKIP_ONE, *r);
            if (!vend) {
                break;
            }
            const char *val = r + SKIP_ONE;
            size_t val_len = (size_t)(vend - val);
            if (an_len == 4 && memcmp(an, "cref", 4) == 0) {
                cref = val;
                cref_len = val_len;
            } else if (an_len == 4 && memcmp(an, "href", 4) == 0) {
                href = val;
                href_len = val_len;
            }
            q = vend + SKIP_ONE;
        }
        if (!closed) {
            p = tag + SKIP_ONE;
            continue;
        }
        for (; counted < tag; counted++) {
            if (*counted == '\n') {
                line++;
            }
        }
        const char *raw = NULL;
        int tok_syntax = CBM_DOCLINK_NONE;
        if (cref) {
            raw = cs_clean_value(ctx->arena, cref, cref_len);
            tok_syntax = syntax;
        } else if (href && (syntax == CBM_DOCLINK_CS_SEE || syntax == CBM_DOCLINK_CS_SEEALSO)) {
            raw = cs_clean_value(ctx->arena, href, href_len);
            tok_syntax = CBM_DOCLINK_HREF;
        }
        if (raw && raw[0]) {
            CBMDocLink link = {
                .source_qn = def->qualified_name,
                .raw = raw,
                .line = line,
                .def_line = def->start_line,
                .syntax = (uint16_t)tok_syntax,
            };
            cbm_doclinks_push(&ctx->result->doc_links, ctx->arena, link);
        }
        p = q;
    }
}

/* ── Parameter type normalization ────────────────────────────────── */

typedef struct {
    char *s;
    size_t len;
} cs_str_t;

static bool cs_ident_start(char c) {
    return isalpha((unsigned char)c) || c == '_' || c == '@';
}

static bool cs_ident_char(char c) {
    return isalnum((unsigned char)c) || c == '_';
}

/* Remove [start, end) from s. */
static void cs_cut(cs_str_t *t, size_t start, size_t end) {
    memmove(t->s + start, t->s + end, t->len - end + SKIP_ONE);
    t->len -= end - start;
}

static void cs_trim(cs_str_t *t) {
    size_t b = 0;
    while (b < t->len && isspace((unsigned char)t->s[b])) {
        b++;
    }
    if (b > 0) {
        cs_cut(t, 0, b);
    }
    while (t->len > 0 && isspace((unsigned char)t->s[t->len - SKIP_ONE])) {
        t->s[--t->len] = '\0';
    }
}

/* Index just past the bracket group opened at s[i] (one of < { [ ( ),
 * matching nested groups of any of those kinds; t->len when unbalanced. */
static size_t cs_group_end(const cs_str_t *t, size_t i) {
    int depth = 0;
    for (size_t k = i; k < t->len; k++) {
        char c = t->s[k];
        if (c == '<' || c == '{' || c == '[' || c == '(') {
            depth++;
        } else if (c == '>' || c == '}' || c == ']' || c == ')') {
            depth--;
            if (depth == 0) {
                return k + SKIP_ONE;
            }
        }
    }
    return t->len;
}

/* Leading parameter attribute lists: [NotNull] [In] T x. */
static void cs_drop_attributes(cs_str_t *t) {
    cs_trim(t);
    while (t->len > 0 && t->s[0] == '[') {
        size_t e = cs_group_end(t, 0);
        cs_cut(t, 0, e);
        cs_trim(t);
    }
}

/* Nullable<X> / Nullable{X} (optionally System.- or global::System.-
 * qualified) -> X. */
static void cs_unwrap_nullable(cs_str_t *t) {
    static const char *const prefixes[] = {"global::System.Nullable", "System.Nullable",
                                           "Nullable"};
    for (;;) {
        bool changed = false;
        for (size_t pi = 0; pi < sizeof(prefixes) / sizeof(prefixes[0]) && !changed; pi++) {
            size_t pl = strlen(prefixes[pi]);
            for (size_t i = 0; i + pl <= t->len; i++) {
                if (memcmp(t->s + i, prefixes[pi], pl) != 0) {
                    continue;
                }
                if (i > 0 && (cs_ident_char(t->s[i - SKIP_ONE]) || t->s[i - SKIP_ONE] == '.' ||
                              t->s[i - SKIP_ONE] == ':')) {
                    continue;
                }
                size_t j = i + pl;
                while (j < t->len && t->s[j] == ' ') {
                    j++;
                }
                if (j >= t->len || (t->s[j] != '<' && t->s[j] != '{')) {
                    continue;
                }
                size_t e = cs_group_end(t, j);
                if (e > t->len || e <= j + SKIP_ONE) {
                    continue;
                }
                /* keep the inner text */
                size_t inner_len = e - j - PAIR_LEN;
                memmove(t->s + i, t->s + j + SKIP_ONE, inner_len);
                memmove(t->s + i + inner_len, t->s + e, t->len - e + SKIP_ONE);
                t->len = i + inner_len + (t->len - e);
                changed = true;
                break;
            }
        }
        if (!changed) {
            return;
        }
    }
}

static bool cs_word_at(const cs_str_t *t, size_t i, const char *w) {
    size_t wl = strlen(w);
    if (i + wl > t->len || memcmp(t->s + i, w, wl) != 0) {
        return false;
    }
    if (i > 0 && cs_ident_char(t->s[i - SKIP_ONE])) {
        return false;
    }
    return i + wl < t->len && isspace((unsigned char)t->s[i + wl]);
}

static void cs_drop_modifiers(cs_str_t *t) {
    static const char *const mods[] = {"ref",  "out",    "in",       "params",
                                       "this", "scoped", "readonly", "final"};
    for (size_t i = 0; i < t->len;) {
        bool cut = false;
        for (size_t m = 0; m < sizeof(mods) / sizeof(mods[0]); m++) {
            if (cs_word_at(t, i, mods[m])) {
                size_t e = i + strlen(mods[m]);
                while (e < t->len && isspace((unsigned char)t->s[e])) {
                    e++;
                }
                cs_cut(t, i, e);
                cut = true;
                break;
            }
        }
        if (!cut) {
            i++;
        }
    }
}

/* Doc-ID arity markers: `1, ``0. */
static void cs_drop_backtick_arity(cs_str_t *t) {
    for (size_t i = 0; i < t->len;) {
        if (t->s[i] != '`') {
            i++;
            continue;
        }
        size_t e = i;
        while (e < t->len && t->s[e] == '`') {
            e++;
        }
        size_t d = e;
        while (d < t->len && isdigit((unsigned char)t->s[d])) {
            d++;
        }
        if (d > e) {
            cs_cut(t, i, d);
        } else {
            i = e;
        }
    }
}

/* Remove every <...> and {...} group, innermost first. */
static void cs_drop_type_args(cs_str_t *t) {
    for (;;) {
        bool changed = false;
        for (size_t i = 0; i < t->len; i++) {
            char c = t->s[i];
            if (c != '<' && c != '{') {
                continue;
            }
            char close = c == '<' ? '>' : '}';
            size_t k = i + SKIP_ONE;
            while (k < t->len && t->s[k] != '<' && t->s[k] != '{' && t->s[k] != close) {
                k++;
            }
            if (k < t->len && t->s[k] == close) {
                cs_cut(t, i, k + SKIP_ONE);
                changed = true;
                break;
            }
        }
        if (!changed) {
            return;
        }
    }
}

static const char *cs_bcl_alias(const char *base, size_t len) {
    static const struct {
        const char *bcl;
        const char *kw;
    } map[] = {
        {"Int32", "int"},     {"Int64", "long"},    {"Int16", "short"},     {"Byte", "byte"},
        {"SByte", "sbyte"},   {"UInt32", "uint"},   {"UInt64", "ulong"},    {"UInt16", "ushort"},
        {"Single", "float"},  {"Double", "double"}, {"Decimal", "decimal"}, {"Boolean", "bool"},
        {"Char", "char"},     {"String", "string"}, {"Object", "object"},   {"IntPtr", "nint"},
        {"UIntPtr", "nuint"}, {"Void", "void"},
    };
    for (size_t i = 0; i < sizeof(map) / sizeof(map[0]); i++) {
        if (strlen(map[i].bcl) == len && memcmp(map[i].bcl, base, len) == 0) {
            return map[i].kw;
        }
    }
    return NULL;
}

enum { CS_NORM_WORK = 512 };

size_t cbm_doclink_cs_norm_type(const char *in, size_t len, char *out, size_t cap) {
    if (!out || cap == 0) {
        return 0;
    }
    out[0] = '\0';
    char work[CS_NORM_WORK];
    if (!in || len == 0 || len >= sizeof(work)) {
        snprintf(out, cap, "?");
        return strlen(out);
    }
    memcpy(work, in, len);
    work[len] = '\0';
    cs_str_t t = {work, len};
    cs_drop_attributes(&t);
    cs_unwrap_nullable(&t);
    cs_drop_modifiers(&t);
    cs_drop_backtick_arity(&t);
    cs_drop_type_args(&t);
    /* "..." -> "[]" (varargs spelling) */
    for (size_t i = 0; i + 2 < t.len; i++) {
        if (t.s[i] == '.' && t.s[i + 1] == '.' && t.s[i + 2] == '.') {
            t.s[i] = '[';
            t.s[i + 1] = ']';
            cs_cut(&t, i + 2, i + 3);
        }
    }
    while (t.len > 0 && t.s[t.len - SKIP_ONE] == '@') {
        t.s[--t.len] = '\0';
    }
    cs_trim(&t);
    /* A trailing identifier after whitespace is the parameter name. */
    size_t last_ws = 0;
    bool have_ws = false;
    for (size_t i = 0; i < t.len; i++) {
        if (isspace((unsigned char)t.s[i])) {
            last_ws = i;
            have_ws = true;
        }
    }
    if (have_ws) {
        size_t s0 = last_ws + SKIP_ONE;
        bool ident = s0 < t.len && cs_ident_start(t.s[s0]);
        for (size_t i = s0 + SKIP_ONE; ident && i < t.len; i++) {
            ident = cs_ident_char(t.s[i]);
        }
        if (ident) {
            t.s[last_ws] = '\0';
            t.len = last_ws;
        }
    }
    /* Drop all whitespace and trailing ?/! markers. */
    size_t w = 0;
    for (size_t i = 0; i < t.len; i++) {
        if (!isspace((unsigned char)t.s[i])) {
            t.s[w++] = t.s[i];
        }
    }
    t.s[w] = '\0';
    t.len = w;
    while (t.len > 0 && (t.s[t.len - SKIP_ONE] == '?' || t.s[t.len - SKIP_ONE] == '!')) {
        t.s[--t.len] = '\0';
    }
    /* base, then array ([] [,]) and pointer (*) suffixes */
    size_t suf = t.len;
    while (suf > 0 && t.s[suf - SKIP_ONE] == '*') {
        suf--;
    }
    for (;;) {
        if (suf > 0 && t.s[suf - SKIP_ONE] == ']') {
            size_t k = suf - SKIP_ONE;
            while (k > 0 && t.s[k - SKIP_ONE] == ',') {
                k--;
            }
            if (k > 0 && t.s[k - SKIP_ONE] == '[') {
                suf = k - SKIP_ONE;
                continue;
            }
        }
        break;
    }
    size_t base_end = suf;
    while (base_end > 0 && t.s[base_end - SKIP_ONE] == '?') {
        base_end--;
    }
    size_t base_start = 0;
    for (size_t i = 0; i < base_end; i++) {
        if (t.s[i] == '.') {
            base_start = i + SKIP_ONE;
        } else if (t.s[i] == ':' && i + SKIP_ONE < base_end && t.s[i + SKIP_ONE] == ':') {
            base_start = i + PAIR_LEN;
        }
    }
    if (base_start < base_end && t.s[base_start] == '@') {
        base_start++;
    }
    size_t blen = base_end > base_start ? base_end - base_start : 0;
    if (blen == 0) {
        snprintf(out, cap, "?");
        return strlen(out);
    }
    const char *kw = cs_bcl_alias(t.s + base_start, blen);
    int n = kw ? snprintf(out, cap, "%s%.*s", kw, (int)(t.len - suf), t.s + suf)
               : snprintf(out, cap, "%.*s%.*s", (int)blen, t.s + base_start, (int)(t.len - suf),
                          t.s + suf);
    return n > 0 ? strlen(out) : 0;
}

/* ── Scope scan ──────────────────────────────────────────────────── */

/* An entry of the brace list: a structural brace, or the nesting depth a
 * preprocessor branch starts from. */
typedef struct {
    uint32_t pos; /* byte offset */
    uint32_t row; /* 0-based */
    int match;    /* index of the paired brace, CBM_NOT_FOUND when unpaired */
    int alias;    /* the first branch's brace this one stands in for, or CBM_NOT_FOUND */
    int depth;    /* nesting depth just after this entry */
    char kind;    /* '{', '}', or '#' for a branch mark */
} cs_brace_t;

/* A declaration keyword (namespace, class, struct ...). */
typedef struct {
    uint32_t end; /* first byte after the keyword */
    uint32_t row; /* 0-based */
    char kind;    /* N namespace; c s i e r as for types */
} cs_head_t;

/* An open #if while the braces are paired. */
typedef struct {
    int *at_if; /* the open braces where the #if stands */
    int n_if;
    int *end1; /* ... and where its first branch ended; NULL until an #else */
    int n_end1;
    int mark; /* its entry in the brace list */
} cs_pp_t;

enum {
    CS_OWNER_NONE = -1,    /* not in a type */
    CS_OWNER_LEXICAL = -2, /* found in an error node: whichever type's braces hold it */
    CS_ITEM_TEXT = -3,     /* frame of a declaration read from the text */
    CS_PP_IF = 1,
    CS_PP_ELSE,
    CS_PP_ENDIF,
    CS_PP_MAX = 32,           /* nested #if */
    CS_CHAR_LITERAL_MAX = 12, /* '\U0010FFFF' */
    CS_RAW_QUOTES = 3,        /* """ */
};

/* One declaration of the file. `node` is the declaration (a field's
 * declaration for each of its declarators); a declaration read from the
 * text, where the tree has no node for it, has none. */
typedef struct {
    TSNode node;
    TSNode name;
    TSNode tparams;
    TSNode params;
    const char *text_name;    /* text declarations */
    const char *text_tparams; /* text declarations: "T,U" */
    uint32_t start;           /* byte offset: document order */
    uint32_t line;            /* 1-based */
    int brace;                /* text declarations: the brace opening the block, or CBM_NOT_FOUND */
    int tree_idx;             /* position among the tree's items; CS_ITEM_TEXT for a text one */
    int owner;                /* M: tree_idx of the type the tree nests it in, or CS_OWNER_* */
    char tag;                 /* N namespace block, F file-scoped namespace, U using, T type,
                                 M member */
    char kind;                /* T: c s i e r d;  M: c v e */
    bool explicit_impl;
    bool broken;    /* T: a parse error sits among its members */
    bool from_text; /* read from the text, not from a declaration node */
} cs_item_t;

typedef struct {
    CBMExtractCtx *ctx;
    CBMArena *tmp; /* names, paths, items, tokens: nothing of it outlives the scan */
    cs_sb_t sb;
    cs_item_t *items;
    int nitems;
    int cap_items;
    cs_brace_t *braces;
    int nbraces;
    int cap_braces;
    cs_head_t *heads;
    int nheads;
    int cap_heads;
    int *stack; /* brace pairing: the open braces */
    int sp;
    int cap_stack;
    cs_pp_t pp[CS_PP_MAX];
    int npp;
    uint32_t row_pos; /* row cursor: the row of byte row_pos is `row` */
    uint32_t row;
    bool failed;            /* out of memory */
    bool lexical;           /* the tree has parse errors: nesting is read from the braces */
    uint32_t untrusted;     /* byte offset from which the braces do not pair up */
    uint32_t untrusted_row; /* its 0-based row */
    int next_region;
    uint32_t root_end_byte;
    uint32_t root_end_line;
} cs_scan_t;

enum {
    CS_ITEMS_INIT = 256,
    CS_BRACES_INIT = 1024,
    CS_HEADS_INIT = 64,
    CS_HEADER_SCAN_MAX = 4096, /* bytes from a type's name to its `{` or `;` */
    CS_TPARAMS_SCAN_MAX = 1024,
};

static bool cs_kind_is(TSNode n, const char *kind) {
    return strcmp(ts_node_type(n), kind) == 0;
}

static TSNode cs_field(TSNode n, const char *field) {
    return ts_node_child_by_field_name(n, field, (uint32_t)strlen(field));
}

/* A declaration's type_parameter_list: a named field in some grammar
 * versions, an unnamed child in others. */
static TSNode cs_type_params(TSNode decl) {
    TSNode tp = cs_field(decl, "type_parameters");
    if (!ts_node_is_null(tp)) {
        return tp;
    }
    uint32_t nc = ts_node_named_child_count(decl);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(decl, i);
        if (strcmp(ts_node_type(c), "type_parameter_list") == 0) {
            return c;
        }
    }
    return (TSNode){0};
}

/* Node text without whitespace and without verbatim '@' markers, appended to
 * sb. `global::` prefixes are dropped. Text that is too long to be a type
 * name, or that holds a scope separator, is written as "?" (an unresolvable
 * name: the declaring type then counts as having an open hierarchy). */
static void cs_put_text_nows(cs_scan_t *s, TSNode n) {
    const char *src = s->ctx->source;
    uint32_t a = ts_node_start_byte(n);
    uint32_t b = ts_node_end_byte(n);
    if (b - a > 8 && memcmp(src + a, "global::", 8) == 0) {
        a += 8;
    }
    bool ok = b >= a && b - a <= CS_NAME_MAX;
    for (uint32_t i = a; ok && i < b; i++) {
        ok = src[i] != '|' && src[i] != ';' && src[i] != '{' && src[i] != '}';
    }
    if (!ok) {
        sb_putc(&s->sb, '?');
        return;
    }
    for (uint32_t i = a; i < b; i++) {
        char c = src[i];
        if (isspace((unsigned char)c) || c == '@') {
            continue;
        }
        sb_putc(&s->sb, c);
    }
}

static void *cs_tmp_alloc(cs_scan_t *s, size_t n) {
    void *p = cbm_arena_alloc(s->tmp, n ? n : SKIP_ONE);
    if (!p) {
        s->failed = true;
    }
    return p;
}

/* Copy of source bytes [a, b) without whitespace and '@'. NULL unless it is a
 * (dotted) identifier of sane length: an error-recovered parse can hand back a
 * "name" spanning arbitrary code, which must not become a declaration. */
static char *cs_ident_dup(cs_scan_t *s, uint32_t a, uint32_t b) {
    const char *src = s->ctx->source;
    if (b <= a || b - a > CS_NAME_MAX) {
        return NULL;
    }
    char *out = (char *)cs_tmp_alloc(s, (size_t)(b - a) + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    for (uint32_t i = a; i < b; i++) {
        unsigned char c = (unsigned char)src[i];
        if (isspace(c) || c == '@') {
            continue;
        }
        if (!(isalnum(c) || c == '_' || c == '.' || c >= 0x80)) {
            return NULL; /* not an identifier */
        }
        out[w++] = (char)c;
    }
    out[w] = '\0';
    return w > 0 ? out : NULL;
}

static char *cs_name_dup(cs_scan_t *s, TSNode n) {
    if (ts_node_is_null(n)) {
        return NULL;
    }
    return cs_ident_dup(s, ts_node_start_byte(n), ts_node_end_byte(n));
}

static uint32_t cs_line(TSNode n) {
    return ts_node_start_point(n).row + TS_LINE_OFFSET;
}

static uint32_t cs_end_line(TSNode n) {
    return ts_node_end_point(n).row + TS_LINE_OFFSET;
}

static const char *cs_join_path(cs_scan_t *s, const char *outer, const char *name) {
    if (!outer || !outer[0]) {
        return name;
    }
    size_t ol = strlen(outer);
    size_t nl = strlen(name);
    char *out = (char *)cs_tmp_alloc(s, ol + nl + PAIR_LEN);
    if (!out) {
        return NULL;
    }
    memcpy(out, outer, ol);
    out[ol] = '.';
    memcpy(out + ol + SKIP_ONE, name, nl + SKIP_ONE);
    return out;
}

/* type_parameter_list -> "T,U" into sb (nothing when absent). */
static void cs_put_tparams(cs_scan_t *s, TSNode list) {
    if (ts_node_is_null(list)) {
        return;
    }
    bool first = true;
    uint32_t nc = ts_node_named_child_count(list);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode tp = ts_node_named_child(list, i);
        if (!cs_kind_is(tp, "type_parameter")) {
            continue;
        }
        TSNode nm = cs_field(tp, "name");
        if (ts_node_is_null(nm)) {
            continue;
        }
        if (!first) {
            sb_putc(&s->sb, ',');
        }
        cs_put_text_nows(s, nm);
        first = false;
    }
}

static void cs_put_sig(cs_scan_t *s, TSNode params) {
    if (ts_node_is_null(params)) {
        return;
    }
    const char *src = s->ctx->source;
    bool first = true;
    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_child(params, i);
        /* A parameter is a `parameter` node -- except the `params` one, which
         * the grammar leaves inline in the list: its type is the list's own
         * "type" field. */
        TSNode ty = {0};
        if (cs_kind_is(p, "parameter")) {
            ty = cs_field(p, "type");
        } else {
            const char *field = ts_node_field_name_for_child(params, i);
            if (!field || strcmp(field, "type") != 0) {
                continue;
            }
            ty = p;
        }
        char norm[CBM_SZ_256];
        if (ts_node_is_null(ty)) {
            snprintf(norm, sizeof(norm), "?");
        } else {
            uint32_t a = ts_node_start_byte(ty);
            uint32_t b = ts_node_end_byte(ty);
            cbm_doclink_cs_norm_type(src + a, (size_t)(b - a), norm, sizeof(norm));
        }
        if (!first) {
            sb_putc(&s->sb, '|');
        }
        sb_puts(&s->sb, norm);
        first = false;
    }
}

static bool cs_has_child_kind(TSNode n, const char *kind) {
    uint32_t nc = ts_node_named_child_count(n);
    for (uint32_t i = 0; i < nc; i++) {
        if (cs_kind_is(ts_node_named_child(n, i), kind)) {
            return true;
        }
    }
    return false;
}

static char cs_type_kind(const char *k) {
    if (strcmp(k, "class_declaration") == 0) {
        return 'c';
    }
    if (strcmp(k, "struct_declaration") == 0) {
        return 's';
    }
    if (strcmp(k, "interface_declaration") == 0) {
        return 'i';
    }
    if (strcmp(k, "enum_declaration") == 0) {
        return 'e';
    }
    if (strcmp(k, "record_declaration") == 0 || strcmp(k, "record_struct_declaration") == 0) {
        return 'r';
    }
    if (strcmp(k, "delegate_declaration") == 0) {
        return 'd';
    }
    return 0;
}

/* ── Tokens: the block structure the tree lost ───────────────────────
 *
 * Error recovery closes blocks early and late, reports a block namespace as
 * a file-scoped one, or gives up on a declaration and leaves its header as
 * loose tokens in an error node. On the C# bench corpus 1,805 of 32,686 files
 * have parse errors, and in 230 of them a namespace or type has a tree extent
 * that is not its brace extent (the API reference files among them) -- which
 * moves every declaration after the error into the wrong namespace or out of
 * its outer type. A file whose tree has errors therefore takes its nesting
 * from the text instead: the braces say where blocks begin and end, the
 * declaration keywords say what the blocks are. The parser's own tokens
 * cannot serve: once it loses the thread inside a string it lexes code as
 * string content. So the braces are scanned here, with the lexical grammar a
 * brace scanner needs -- comments, character and string literals in all
 * their forms, interpolation holes, preprocessor branches. On the 30,881
 * error-free corpus files this scanner yields exactly the parser's brace
 * tokens. Where the braces still do not pair up, the rest of the file is not
 * placed at all. */

static uint32_t cs_row_of(cs_scan_t *s, uint32_t pos) {
    const char *src = s->ctx->source;
    if (pos < s->row_pos) {
        s->row_pos = 0;
        s->row = 0;
    }
    for (uint32_t i = s->row_pos; i < pos; i++) {
        s->row += src[i] == '\n';
    }
    s->row_pos = pos;
    return s->row;
}

/* A new entry of the brace list; its index, or CBM_NOT_FOUND. */
static int cs_brace_push(cs_scan_t *s, uint32_t pos, char kind) {
    if (s->failed) {
        return CBM_NOT_FOUND;
    }
    if (s->nbraces >= s->cap_braces) {
        int ncap = s->cap_braces ? s->cap_braces * PAIR_LEN : CS_BRACES_INIT;
        cs_brace_t *grown = (cs_brace_t *)cs_tmp_alloc(s, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return CBM_NOT_FOUND;
        }
        if (s->nbraces > 0) {
            memcpy(grown, s->braces, (size_t)s->nbraces * sizeof(*grown));
        }
        s->braces = grown;
        s->cap_braces = ncap;
    }
    s->braces[s->nbraces] = (cs_brace_t){.pos = pos,
                                         .row = cs_row_of(s, pos),
                                         .match = CBM_NOT_FOUND,
                                         .alias = CBM_NOT_FOUND,
                                         .kind = kind};
    return s->nbraces++;
}

static bool cs_stack_reserve(cs_scan_t *s, int n) {
    if (n <= s->cap_stack) {
        return true;
    }
    int ncap = s->cap_stack ? s->cap_stack : CS_HEADS_INIT;
    while (ncap < n) {
        ncap *= PAIR_LEN;
    }
    int *grown = (int *)cs_tmp_alloc(s, (size_t)ncap * sizeof(int));
    if (!grown) {
        return false;
    }
    if (s->sp > 0) {
        memcpy(grown, s->stack, (size_t)s->sp * sizeof(int));
    }
    s->stack = grown;
    s->cap_stack = ncap;
    return true;
}

static void cs_mark_untrusted(cs_scan_t *s, int brace) {
    const cs_brace_t *b = &s->braces[brace];
    if (b->pos < s->untrusted) {
        s->untrusted = b->pos;
        s->untrusted_row = b->row;
    }
}

/* The scanner reports a brace: pair it. */
static void cs_lex_brace(void *ud, uint32_t pos, bool open) {
    cs_scan_t *s = (cs_scan_t *)ud;
    int b = cs_brace_push(s, pos, open ? '{' : '}');
    if (b < 0) {
        return;
    }
    if (open) {
        if (cs_stack_reserve(s, s->sp + SKIP_ONE)) {
            s->stack[s->sp++] = b;
        }
    } else if (s->sp == 0) {
        cs_mark_untrusted(s, b); /* closes nothing */
    } else {
        int o = s->stack[--s->sp];
        if (s->braces[o].match < 0) {
            s->braces[o].match = b; /* the first branch's close stands */
        }
        s->braces[b].match = o;
    }
    s->braces[b].depth = s->sp;
}

static int *cs_stack_copy(cs_scan_t *s) {
    int *copy = (int *)cs_tmp_alloc(s, (size_t)(s->sp + SKIP_ONE) * sizeof(int));
    if (copy && s->sp > 0) {
        memcpy(copy, s->stack, (size_t)s->sp * sizeof(int));
    }
    return copy;
}

/* A later branch of a conditional ended. Where it leaves the same blocks
 * open as the first one did, its open braces stand in for the first
 * branch's (`class X : A {` / `#else` / `class X : B {` share one closing
 * brace). Where the branches disagree the first one stands alone: a file
 * can balance per configuration only (`#if A {` ... `#if A }`), and the
 * braces left over at the end say whether this one does. */
static void cs_branch_merge(cs_scan_t *s, const cs_pp_t *f) {
    if (s->sp != f->n_end1) {
        return;
    }
    for (int j = 0; j < s->sp; j++) {
        if (s->stack[j] != f->end1[j]) {
            s->braces[s->stack[j]].alias = f->end1[j];
        }
    }
}

/* The scanner reports #if / #else (or #elif) / #endif. Every branch starts
 * from the nesting the #if started from; after the #endif the first
 * branch's result stands. */
static void cs_lex_branch(void *ud, uint32_t pos, int what) {
    cs_scan_t *s = (cs_scan_t *)ud;
    int mark = cs_brace_push(s, pos, '#');
    if (mark < 0) {
        return;
    }
    if (what == CS_PP_IF) {
        if (s->npp >= CS_PP_MAX) {
            cs_mark_untrusted(s, mark);
        } else {
            cs_pp_t *f = &s->pp[s->npp];
            *f = (cs_pp_t){.at_if = cs_stack_copy(s), .n_if = s->sp, .mark = mark};
            s->npp += f->at_if != NULL;
        }
    } else if (s->npp > 0) {
        cs_pp_t *f = &s->pp[s->npp - SKIP_ONE];
        if (!f->end1) {
            if (what == CS_PP_ELSE) {
                f->end1 = cs_stack_copy(s);
                f->n_end1 = s->sp;
            }
        } else {
            cs_branch_merge(s, f);
        }
        if (what == CS_PP_ELSE && f->end1 && cs_stack_reserve(s, f->n_if)) {
            memcpy(s->stack, f->at_if, (size_t)f->n_if * sizeof(int));
            s->sp = f->n_if;
        } else if (what == CS_PP_ENDIF) {
            if (f->end1 && cs_stack_reserve(s, f->n_end1)) {
                memcpy(s->stack, f->end1, (size_t)f->n_end1 * sizeof(int));
                s->sp = f->n_end1;
            }
            s->npp--;
        }
    }
    s->braces[mark].depth = s->sp;
}

/* The scanner reports a declaration keyword. */
static void cs_lex_keyword(void *ud, uint32_t kw_end, char kind) {
    cs_scan_t *s = (cs_scan_t *)ud;
    if (s->failed) {
        return;
    }
    if (s->nheads >= s->cap_heads) {
        int ncap = s->cap_heads ? s->cap_heads * PAIR_LEN : CS_HEADS_INIT;
        cs_head_t *grown = (cs_head_t *)cs_tmp_alloc(s, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return;
        }
        if (s->nheads > 0) {
            memcpy(grown, s->heads, (size_t)s->nheads * sizeof(*grown));
        }
        s->heads = grown;
        s->cap_heads = ncap;
    }
    s->heads[s->nheads++] = (cs_head_t){.end = kw_end, .row = cs_row_of(s, kw_end), .kind = kind};
}

/* --- the scanner ---------------------------------------------------- */

typedef struct {
    const char *src;
    uint32_t n;
    void *ud;
    bool prev_sep; /* the previous token was ':' or ',' */
} cs_lex_t;

static bool cs_lex_word(unsigned char c) {
    return isalnum(c) || c == '_' || c >= 0x80;
}

/* The declaration a keyword starts: N for `namespace`, a type kind, or 0. */
static char cs_keyword_kind(const char *text, uint32_t len) {
    static const struct {
        const char *word;
        char kind;
    } words[] = {{"namespace", 'N'}, {"class", 'c'}, {"struct", 's'},
                 {"interface", 'i'}, {"enum", 'e'},  {"record", 'r'}};
    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); i++) {
        if (strlen(words[i].word) == len && memcmp(words[i].word, text, len) == 0) {
            return words[i].kind;
        }
    }
    return 0;
}

static uint32_t cs_lex_code(cs_lex_t *lx, uint32_t i, bool hole);

/* Length of the run of `c` at src[i]. */
static uint32_t cs_lex_run(const cs_lex_t *lx, uint32_t i, char c) {
    uint32_t j = i;
    while (j < lx->n && lx->src[j] == c) {
        j++;
    }
    return j - i;
}

/* 'x', '\n', 'A': past the literal, or past the lone quote when it is
 * not one. */
static uint32_t cs_lex_char(const cs_lex_t *lx, uint32_t i) {
    uint32_t j = i + SKIP_ONE;
    if (j < lx->n && lx->src[j] == '\\') {
        j += PAIR_LEN;
    }
    while (j < lx->n && lx->src[j] != '\'' && lx->src[j] != '\n' && j - i < CS_CHAR_LITERAL_MAX) {
        j++;
    }
    return (j < lx->n && lx->src[j] == '\'') ? j + SKIP_ONE : i + SKIP_ONE;
}

/* "..." with backslash escapes; an unterminated one ends with its line. */
static uint32_t cs_lex_string(const cs_lex_t *lx, uint32_t i) {
    uint32_t j = i + SKIP_ONE;
    while (j < lx->n) {
        char c = lx->src[j];
        if (c == '\\') {
            j += PAIR_LEN;
        } else if (c == '"') {
            return j + SKIP_ONE;
        } else if (c == '\n') {
            return j;
        } else {
            j++;
        }
    }
    return lx->n;
}

/* @"..." : a quote is written twice. `i` is at the opening quote. */
static uint32_t cs_lex_verbatim(const cs_lex_t *lx, uint32_t i) {
    uint32_t j = i + SKIP_ONE;
    while (j < lx->n) {
        if (lx->src[j] != '"') {
            j++;
        } else if (j + SKIP_ONE < lx->n && lx->src[j + SKIP_ONE] == '"') {
            j += PAIR_LEN;
        } else {
            return j + SKIP_ONE;
        }
    }
    return lx->n;
}

/* """...""" (`quotes` >= 3 of them): ends with a run of at least as many.
 * With `dollars`, a run of that many `{` opens a hole of code. */
static uint32_t cs_lex_raw(cs_lex_t *lx, uint32_t i, uint32_t quotes, uint32_t dollars) {
    uint32_t j = i + quotes;
    while (j < lx->n) {
        char c = lx->src[j];
        if (c == '"') {
            uint32_t run = cs_lex_run(lx, j, '"');
            if (run >= quotes) {
                return j + run;
            }
            j += run;
        } else if (dollars > 0 && c == '{') {
            uint32_t run = cs_lex_run(lx, j, '{');
            j += run;
            if (run >= dollars) {
                j = cs_lex_code(lx, j, true);
                j += cs_lex_run(lx, j, '}'); /* the rest of the closing run */
            }
        } else {
            j++;
        }
    }
    return lx->n;
}

/* $"..." / $@"..." : text with {holes} of code; {{ and }} are literal braces.
 * `i` is at the opening quote. */
static uint32_t cs_lex_interpolated(cs_lex_t *lx, uint32_t i, bool verbatim) {
    uint32_t j = i + SKIP_ONE;
    while (j < lx->n) {
        char c = lx->src[j];
        if (c == '"') {
            if (verbatim && j + SKIP_ONE < lx->n && lx->src[j + SKIP_ONE] == '"') {
                j += PAIR_LEN;
                continue;
            }
            return j + SKIP_ONE;
        }
        if (c == '\\' && !verbatim) {
            j += PAIR_LEN;
        } else if (c == '{' || c == '}') {
            if (j + SKIP_ONE < lx->n && lx->src[j + SKIP_ONE] == c) {
                j += PAIR_LEN;
            } else if (c == '{') {
                j = cs_lex_code(lx, j + SKIP_ONE, true);
            } else {
                j++;
            }
        } else if (c == '\n' && !verbatim) {
            return j;
        } else {
            j++;
        }
    }
    return lx->n;
}

/* A literal that starts with a quote, `@`, or `$` at src[i]; returns i itself
 * when there is none there. */
static uint32_t cs_lex_literal(cs_lex_t *lx, uint32_t i) {
    const char *src = lx->src;
    uint32_t n = lx->n;
    char c = src[i];
    if (c == '"') {
        uint32_t q = cs_lex_run(lx, i, '"');
        if (q >= CS_RAW_QUOTES) {
            return cs_lex_raw(lx, i, q, 0);
        }
        return q == PAIR_LEN ? i + PAIR_LEN : cs_lex_string(lx, i);
    }
    if (c == '@' && i + SKIP_ONE < n && src[i + SKIP_ONE] == '"') {
        return cs_lex_verbatim(lx, i + SKIP_ONE);
    }
    if (c == '@' && i + PAIR_LEN < n && src[i + SKIP_ONE] == '$' && src[i + PAIR_LEN] == '"') {
        return cs_lex_interpolated(lx, i + PAIR_LEN, true);
    }
    if (c == '$') {
        uint32_t d = cs_lex_run(lx, i, '$');
        uint32_t j = i + d;
        if (j < n && src[j] == '"') {
            uint32_t q = cs_lex_run(lx, j, '"');
            if (q >= CS_RAW_QUOTES) {
                return cs_lex_raw(lx, j, q, d);
            }
            return (d == SKIP_ONE) ? cs_lex_interpolated(lx, j, false) : i;
        }
        if (d == SKIP_ONE && j + SKIP_ONE < n && src[j] == '@' && src[j + SKIP_ONE] == '"') {
            return cs_lex_interpolated(lx, j + SKIP_ONE, true);
        }
    }
    return i;
}

static bool cs_lex_is(const char *src, uint32_t w, uint32_t len, const char *word) {
    return strlen(word) == len && memcmp(src + w, word, len) == 0;
}

/* A `#` directive at the start of a line: reports conditional branches and
 * returns the end of the line. */
static uint32_t cs_lex_directive(cs_lex_t *lx, uint32_t i, bool hole) {
    const char *src = lx->src;
    uint32_t j = i + SKIP_ONE;
    while (j < lx->n && (src[j] == ' ' || src[j] == '\t')) {
        j++;
    }
    uint32_t w = j;
    while (j < lx->n && isalpha((unsigned char)src[j])) {
        j++;
    }
    uint32_t len = j - w;
    if (!hole) {
        if (cs_lex_is(src, w, len, "if")) {
            cs_lex_branch(lx->ud, i, CS_PP_IF);
        } else if (cs_lex_is(src, w, len, "else") || cs_lex_is(src, w, len, "elif")) {
            cs_lex_branch(lx->ud, i, CS_PP_ELSE);
        } else if (cs_lex_is(src, w, len, "endif")) {
            cs_lex_branch(lx->ud, i, CS_PP_ENDIF);
        }
    }
    while (j < lx->n && src[j] != '\n') {
        j++;
    }
    return j;
}

/* A word at src[i] (an identifier, keyword or number, with an optional `@`):
 * reports a declaration keyword and returns its end; i when there is none. */
static uint32_t cs_lex_identifier(cs_lex_t *lx, uint32_t i, bool hole) {
    const char *src = lx->src;
    bool verbatim = src[i] == '@';
    uint32_t a = verbatim ? i + SKIP_ONE : i;
    uint32_t b = a;
    while (b < lx->n && cs_lex_word((unsigned char)src[b])) {
        b++;
    }
    if (b == a) {
        return i;
    }
    char kind = (verbatim || hole) ? 0 : cs_keyword_kind(src + a, b - a);
    if (kind && !lx->prev_sep) {
        cs_lex_keyword(lx->ud, b, kind);
    }
    return b;
}

/* Past a comment at src[i], or i when there is none. */
static uint32_t cs_lex_comment(const cs_lex_t *lx, uint32_t i) {
    const char *src = lx->src;
    uint32_t n = lx->n;
    if (src[i] != '/' || i + SKIP_ONE >= n) {
        return i;
    }
    if (src[i + SKIP_ONE] == '/') {
        while (i < n && src[i] != '\n') {
            i++;
        }
        return i;
    }
    if (src[i + SKIP_ONE] == '*') {
        i += PAIR_LEN;
        while (i + SKIP_ONE < n && !(src[i] == '*' && src[i + SKIP_ONE] == '/')) {
            i++;
        }
        return i + PAIR_LEN <= n ? i + PAIR_LEN : n;
    }
    return i;
}

/* Scan code from src[i]. In a `hole` (the code of an interpolation) nothing
 * is reported and the scan returns just past the brace that closes it. */
static uint32_t cs_lex_code(cs_lex_t *lx, uint32_t i, bool hole) {
    const char *src = lx->src;
    uint32_t n = lx->n;
    int depth = 0;
    bool line_start = !hole;
    while (i < n) {
        unsigned char c = (unsigned char)src[i];
        if (isspace(c)) {
            line_start = line_start || c == '\n';
            i++;
            continue;
        }
        if (c == '#' && line_start) {
            i = cs_lex_directive(lx, i, hole);
            continue;
        }
        line_start = false;
        uint32_t e = cs_lex_comment(lx, i);
        if (e > i) {
            i = e;
            continue;
        }
        bool sep = c == ':' || c == ',';
        if (c == '\'') {
            e = cs_lex_char(lx, i);
        } else if (c == '{' || c == '}') {
            if (hole && c == '}' && depth == 0) {
                return i + SKIP_ONE;
            }
            if (hole) {
                depth += c == '{' ? SKIP_ONE : -SKIP_ONE;
            } else {
                cs_lex_brace(lx->ud, i, c == '{');
            }
            e = i + SKIP_ONE;
        } else {
            e = (c == '"' || c == '$' || c == '@') ? cs_lex_literal(lx, i) : i;
            if (e == i && (cs_lex_word(c) || c == '@')) {
                e = cs_lex_identifier(lx, i, hole);
            }
            if (e == i) {
                e = i + SKIP_ONE;
            }
        }
        lx->prev_sep = sep;
        i = e;
    }
    return n;
}

/* Scan the file's braces and declaration keywords, pair the braces. The
 * first brace without a partner (or conditional whose branches disagree)
 * starts the untrusted part of the file. */
static void cs_scan_tokens(cs_scan_t *s) {
    cs_lex_t lx = {.src = s->ctx->source, .n = s->root_end_byte, .ud = s};
    (void)cs_lex_code(&lx, 0, false);
    if (s->failed) {
        return;
    }
    if (s->sp > 0) {
        cs_mark_untrusted(s, s->stack[0]);
    }
    /* a later branch's brace closes where the first branch's does */
    for (int i = 0; i < s->nbraces; i++) {
        cs_brace_t *b = &s->braces[i];
        int a = b->alias;
        for (int hops = 0; a >= 0 && b->match < 0 && hops < CS_PP_MAX; hops++) {
            b->match = s->braces[a].match;
            a = s->braces[a].alias;
        }
    }
}

/* Index of the first entry of the brace list at or after byte `pos`. */
static int cs_brace_lower(const cs_scan_t *s, uint32_t pos) {
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

/* The open brace AT byte `pos`, or CBM_NOT_FOUND. */
static int cs_open_brace_at(const cs_scan_t *s, uint32_t pos) {
    int b = cs_brace_lower(s, pos);
    return (b < s->nbraces && s->braces[b].pos == pos && s->braces[b].kind == '{') ? b
                                                                                   : CBM_NOT_FOUND;
}

/* Brace nesting depth at byte `pos`. */
static int cs_depth_at(const cs_scan_t *s, uint32_t pos) {
    int i = cs_brace_lower(s, pos);
    return i > 0 ? s->braces[i - SKIP_ONE].depth : 0;
}

/* The extent of the block opened by brace `b`. false when it has no partner. */
static bool cs_brace_extent(const cs_scan_t *s, int b, uint32_t *end, uint32_t *end_line,
                            int *inner_depth) {
    if (b < 0 || b >= s->nbraces || s->braces[b].kind != '{' || s->braces[b].match < 0) {
        return false;
    }
    const cs_brace_t *close = &s->braces[s->braces[b].match];
    *end = close->pos + SKIP_ONE;
    *end_line = close->row + TS_LINE_OFFSET;
    *inner_depth = s->braces[b].depth;
    return true;
}

/* ── Text ────────────────────────────────────────────────────────── */

/* Past whitespace and comments. */
static uint32_t cs_skip_space(const char *src, uint32_t i, uint32_t n) {
    for (;;) {
        while (i < n && isspace((unsigned char)src[i])) {
            i++;
        }
        if (i + SKIP_ONE >= n || src[i] != '/') {
            return i;
        }
        if (src[i + SKIP_ONE] == '/') {
            while (i < n && src[i] != '\n') {
                i++;
            }
        } else if (src[i + SKIP_ONE] == '*') {
            i += PAIR_LEN;
            while (i + SKIP_ONE < n && !(src[i] == '*' && src[i + SKIP_ONE] == '/')) {
                i++;
            }
            i = i + PAIR_LEN <= n ? i + PAIR_LEN : n;
        } else {
            return i;
        }
    }
}

static bool cs_word_char(unsigned char c) {
    return isalnum(c) || c == '_' || c >= 0x80;
}

/* End of the identifier starting at src[i] (i itself when there is none);
 * `dotted` accepts a qualified name. */
static uint32_t cs_ident_end(const char *src, uint32_t i, uint32_t n, bool dotted) {
    uint32_t e = i;
    if (e < n && src[e] == '@') {
        e++;
    }
    uint32_t first = e;
    while (e < n && (cs_word_char((unsigned char)src[e]) ||
                     (dotted && src[e] == '.' && e > first && e + SKIP_ONE < n &&
                      cs_word_char((unsigned char)src[e + SKIP_ONE])))) {
        e++;
    }
    if (e == first || isdigit((unsigned char)src[first])) {
        return i;
    }
    return e;
}

/* Words that follow `class` / `record` ... without being a declared name. */
static bool cs_not_a_name(const char *name) {
    static const char *const words[] = {"class", "struct", "interface", "enum", "record",
                                        "where", "in",     "is",        "as",   "when",
                                        "and",   "or",     "not",       "with", "switch"};
    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); i++) {
        if (strcmp(name, words[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* The type parameters written at src[*pos] (`<in T, U>`): their names
 * ','-joined, "" when there are none. *pos moves past the list. NULL when the
 * list cannot be read. */
static const char *cs_text_tparams(cs_scan_t *s, uint32_t *pos) {
    const char *src = s->ctx->source;
    uint32_t n = s->root_end_byte;
    uint32_t i = cs_skip_space(src, *pos, n);
    if (i >= n || src[i] != '<') {
        return "";
    }
    uint32_t limit = i + CS_TPARAMS_SCAN_MAX < n ? i + CS_TPARAMS_SCAN_MAX : n;
    char *out = (char *)cs_tmp_alloc(s, (size_t)(limit - i) + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    int square = 0;
    uint32_t word_s = 0;
    uint32_t word_e = 0; /* the last identifier of the current parameter */
    for (uint32_t k = i + SKIP_ONE; k < limit; k++) {
        char c = src[k];
        if (c == '[') {
            square++;
        } else if (c == ']') {
            square--;
        } else if (square > 0) {
            continue; /* an attribute on the parameter */
        } else if (c == ',' || c == '>') {
            if (word_e == word_s) {
                return NULL;
            }
            if (w > 0) {
                out[w++] = ',';
            }
            memcpy(out + w, src + word_s, word_e - word_s);
            w += word_e - word_s;
            word_s = word_e = 0;
            if (c == '>') {
                out[w] = '\0';
                *pos = k + SKIP_ONE;
                return out;
            }
        } else if (cs_word_char((unsigned char)c)) {
            uint32_t e = cs_ident_end(src, k, limit, false);
            if (e == k) {
                return NULL;
            }
            word_s = k;
            word_e = e;
            k = e - SKIP_ONE;
        } else if (!isspace((unsigned char)c)) {
            return NULL;
        }
    }
    return NULL;
}

/* From the end of a type's name and type parameters to the `{` that opens
 * its body (returned as a brace index) or the `;` that ends a body-less
 * declaration (*bodyless). CBM_NOT_FOUND with *bodyless false when neither is
 * found: then this was no declaration. */
static int cs_text_body(const cs_scan_t *s, uint32_t from, bool *bodyless) {
    const char *src = s->ctx->source;
    uint32_t n = s->root_end_byte;
    uint32_t limit = from + CS_HEADER_SCAN_MAX < n ? from + CS_HEADER_SCAN_MAX : n;
    int round = 0;
    *bodyless = false;
    for (uint32_t i = from; i < limit; i++) {
        char c = src[i];
        if (c == '/' && i + SKIP_ONE < limit &&
            (src[i + SKIP_ONE] == '/' || src[i + SKIP_ONE] == '*')) {
            uint32_t past = cs_skip_space(src, i, limit);
            if (past <= i) {
                return CBM_NOT_FOUND;
            }
            i = past - SKIP_ONE;
            continue;
        }
        if (c == '(') {
            round++;
        } else if (c == ')') {
            round--;
        } else if (round > 0) {
            continue;
        } else if (c == ';') {
            *bodyless = true;
            return CBM_NOT_FOUND;
        } else if (c == '{') {
            return cs_open_brace_at(s, i);
        } else if (c == '}' || c == '=') {
            return CBM_NOT_FOUND;
        }
    }
    return CBM_NOT_FOUND;
}

/* ── Collection ──────────────────────────────────────────────────── */

/* A new item; returns its index or CBM_NOT_FOUND when memory ran out. */
static int cs_item_new(cs_scan_t *s, char tag) {
    if (s->failed) {
        return CBM_NOT_FOUND;
    }
    if (s->nitems >= s->cap_items) {
        int ncap = s->cap_items ? s->cap_items * PAIR_LEN : CS_ITEMS_INIT;
        cs_item_t *grown = (cs_item_t *)cs_tmp_alloc(s, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return CBM_NOT_FOUND;
        }
        if (s->nitems > 0) {
            memcpy(grown, s->items, (size_t)s->nitems * sizeof(*grown));
        }
        s->items = grown;
        s->cap_items = ncap;
    }
    cs_item_t *it = &s->items[s->nitems];
    memset(it, 0, sizeof(*it));
    it->tag = tag;
    it->owner = CS_OWNER_NONE;
    it->brace = CBM_NOT_FOUND;
    it->tree_idx = s->nitems;
    return s->nitems++;
}

static int cs_item_of_node(cs_scan_t *s, char tag, TSNode node) {
    int i = cs_item_new(s, tag);
    if (i >= 0) {
        s->items[i].node = node;
        s->items[i].start = ts_node_start_byte(node);
        s->items[i].line = cs_line(node);
    }
    return i;
}

static void cs_member_new(cs_scan_t *s, TSNode decl, char kind, bool explicit_impl, int owner,
                          TSNode name, TSNode tparams, TSNode params) {
    if (ts_node_is_null(name)) {
        return;
    }
    int i = cs_item_of_node(s, 'M', decl);
    if (i < 0) {
        return;
    }
    cs_item_t *it = &s->items[i];
    it->kind = kind;
    it->explicit_impl = explicit_impl;
    it->owner = owner;
    it->name = name;
    it->tparams = tparams;
    it->params = params;
}

/* Every variable_declarator name under a field / event field declaration. */
static void cs_collect_declarators(cs_scan_t *s, TSNode decl, char kind, int owner) {
    TSNode null_node = {0};
    uint32_t nc = ts_node_named_child_count(decl);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode vd = ts_node_named_child(decl, i);
        if (!cs_kind_is(vd, "variable_declaration")) {
            continue;
        }
        uint32_t vc = ts_node_named_child_count(vd);
        for (uint32_t j = 0; j < vc; j++) {
            TSNode d = ts_node_named_child(vd, j);
            if (cs_kind_is(d, "variable_declarator")) {
                cs_member_new(s, decl, kind, false, owner, cs_field(d, "name"), null_node,
                              null_node);
            }
        }
    }
}

/* A member whose header did not parse: an error node among its own parts, or
 * (for a callable) an error anywhere outside its body. Its name and signature
 * cannot be trusted then -- `public safe extern int M();` comes back as a
 * method named `extern`. */
static bool cs_header_broken(TSNode decl, bool callable) {
    TSNode body = callable ? cs_field(decl, "body") : (TSNode){0};
    uint32_t cc = ts_node_child_count(decl);
    for (uint32_t i = 0; i < cc; i++) {
        TSNode ch = ts_node_child(decl, i);
        if (!ts_node_is_null(body) && ts_node_eq(ch, body)) {
            continue;
        }
        if (cs_kind_is(ch, "ERROR") || ts_node_is_missing(ch) ||
            (callable && ts_node_has_error(ch))) {
            return true;
        }
    }
    return false;
}

static void cs_collect_member(cs_scan_t *s, TSNode c, const char *k, int owner) {
    TSNode null_node = {0};
    bool callable =
        strcmp(k, "method_declaration") == 0 || strcmp(k, "constructor_declaration") == 0;
    bool member = callable || strcmp(k, "property_declaration") == 0 ||
                  strcmp(k, "field_declaration") == 0 ||
                  strcmp(k, "event_field_declaration") == 0 ||
                  strcmp(k, "event_declaration") == 0 || strcmp(k, "enum_member_declaration") == 0;
    if (member && ts_node_has_error(c) && cs_header_broken(c, callable)) {
        if (owner >= 0) {
            s->items[owner].broken = true; /* a member of it is hidden */
        }
        return;
    }
    if (strcmp(k, "method_declaration") == 0) {
        cs_member_new(s, c, 'c', cs_has_child_kind(c, "explicit_interface_specifier"), owner,
                      cs_field(c, "name"), cs_type_params(c), cs_field(c, "parameters"));
    } else if (strcmp(k, "constructor_declaration") == 0) {
        cs_member_new(s, c, 'c', false, owner, cs_field(c, "name"), null_node,
                      cs_field(c, "parameters"));
    } else if (strcmp(k, "property_declaration") == 0) {
        cs_member_new(s, c, 'v', cs_has_child_kind(c, "explicit_interface_specifier"), owner,
                      cs_field(c, "name"), null_node, null_node);
    } else if (strcmp(k, "field_declaration") == 0) {
        cs_collect_declarators(s, c, 'v', owner);
    } else if (strcmp(k, "event_field_declaration") == 0) {
        cs_collect_declarators(s, c, 'e', owner);
    } else if (strcmp(k, "event_declaration") == 0) {
        cs_member_new(s, c, 'e', cs_has_child_kind(c, "explicit_interface_specifier"), owner,
                      cs_field(c, "name"), null_node, null_node);
    } else if (strcmp(k, "enum_member_declaration") == 0) {
        cs_member_new(s, c, 'v', false, owner, cs_field(c, "name"), null_node, null_node);
    }
}

/* Collect the declarations under `node` in document order. `owner` is the
 * item of the type whose members `node` holds (CS_OWNER_NONE outside a type).
 * Preprocessor blocks and error nodes are looked into: a declaration that
 * parsed is a declaration wherever recovery left it, and its placement is
 * checked against the braces afterwards. What sits in an error node belongs
 * to whichever block's braces hold it. */
static void cs_collect(cs_scan_t *s, TSNode node, int owner, int depth) {
    if (depth > CS_SCAN_MAX_DEPTH || ts_node_is_null(node) || s->failed) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(node, i);
        const char *k = ts_node_type(c);
        if (strcmp(k, "ERROR") == 0) {
            if (owner >= 0) {
                s->items[owner].broken = true;
            }
            cs_collect(s, c, CS_OWNER_LEXICAL, depth + SKIP_ONE);
            continue;
        }
        if (strncmp(k, "preproc_", 8) == 0 || strcmp(k, "declaration_list") == 0 ||
            strcmp(k, "enum_member_declaration_list") == 0) {
            cs_collect(s, c, owner, depth + SKIP_ONE);
            continue;
        }
        if (strcmp(k, "using_directive") == 0) {
            (void)cs_item_of_node(s, 'U', c);
            continue;
        }
        bool file_scoped = strcmp(k, "file_scoped_namespace_declaration") == 0;
        if (file_scoped || strcmp(k, "namespace_declaration") == 0) {
            int ni = cs_item_of_node(s, file_scoped ? 'F' : 'N', c);
            if (ni < 0) {
                return;
            }
            s->items[ni].name = cs_field(c, "name");
            /* a file-scoped namespace holds nothing itself -- unless it is a
             * block namespace recovery could not parse, whose declarations
             * then sit in an error node under it */
            TSNode body = file_scoped ? (TSNode){0} : cs_field(c, "body");
            cs_collect(s, ts_node_is_null(body) ? c : body, CS_OWNER_NONE, depth + SKIP_ONE);
            continue;
        }
        char tk = cs_type_kind(k);
        if (tk) {
            int ti = cs_item_of_node(s, 'T', c);
            if (ti < 0) {
                return;
            }
            s->items[ti].kind = tk;
            s->items[ti].name = cs_field(c, "name");
            s->items[ti].tparams = cs_type_params(c);
            if (tk != 'd') {
                TSNode body = cs_field(c, "body");
                if (!ts_node_is_null(body)) {
                    cs_collect(s, body, ti, depth + SKIP_ONE);
                }
            }
            continue;
        }
        if (owner != CS_OWNER_NONE) {
            cs_collect_member(s, c, k, owner);
        }
    }
}

/* ── Declarations the tree has no node for ───────────────────────── */

static int cs_u32_cmp(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

static int cs_item_start_cmp(const void *a, const void *b) {
    const cs_item_t *x = (const cs_item_t *)a;
    const cs_item_t *y = (const cs_item_t *)b;
    if (x->start != y->start) {
        return x->start < y->start ? -1 : 1;
    }
    /* declarators of one field declaration keep their order */
    return (x->tree_idx > y->tree_idx) - (x->tree_idx < y->tree_idx);
}

/* Read the declaration a keyword starts from the text and add it as an item,
 * unless the tree already has a node for it (`parsed`: the name positions of
 * the tree's namespaces and types). */
static void cs_text_item(cs_scan_t *s, const cs_head_t *h, const uint32_t *parsed, int nparsed) {
    const char *src = s->ctx->source;
    uint32_t n = s->root_end_byte;
    uint32_t a = cs_skip_space(src, h->end, n);
    if (a == h->end) {
        return; /* the keyword runs into something: not a declaration */
    }
    bool is_ns = h->kind == 'N';
    uint32_t b = cs_ident_end(src, a, n, is_ns);
    if (b == a || bsearch(&a, parsed, (size_t)nparsed, sizeof(uint32_t), cs_u32_cmp)) {
        return;
    }
    char *name = cs_ident_dup(s, a, b);
    if (!name || cs_not_a_name(name)) {
        return;
    }
    int brace = CBM_NOT_FOUND;
    const char *tparams = "";
    char tag = 'T';
    if (is_ns) {
        uint32_t after = cs_skip_space(src, b, n);
        if (after < n && src[after] == '{') {
            brace = cs_open_brace_at(s, after);
            tag = 'N';
        } else if (after < n && src[after] == ';') {
            tag = 'F';
        } else {
            return;
        }
        if (tag == 'N' && brace < 0) {
            return;
        }
    } else {
        uint32_t pos = b;
        tparams = cs_text_tparams(s, &pos);
        bool bodyless = false;
        brace = tparams ? cs_text_body(s, pos, &bodyless) : CBM_NOT_FOUND;
        if (!tparams || (brace < 0 && !bodyless)) {
            return;
        }
    }
    int i = cs_item_new(s, tag);
    if (i < 0) {
        return;
    }
    cs_item_t *it = &s->items[i];
    it->from_text = true;
    it->tree_idx = CS_ITEM_TEXT;
    it->kind = is_ns ? 0 : h->kind;
    it->text_name = name;
    it->text_tparams = tparams;
    it->brace = brace;
    it->start = a; /* the name: after the modifiers, inside the same braces */
    it->line = h->row + TS_LINE_OFFSET;
    it->broken = true;
}

/* Add the declarations only the token stream shows and put all items in
 * document order. */
static void cs_add_text_items(cs_scan_t *s) {
    if (s->failed || s->nheads == 0) {
        return;
    }
    int tree_items = s->nitems;
    uint32_t *parsed =
        (uint32_t *)cs_tmp_alloc(s, (size_t)(tree_items + SKIP_ONE) * sizeof(uint32_t));
    if (!parsed) {
        return;
    }
    int nparsed = 0;
    for (int i = 0; i < tree_items; i++) {
        const cs_item_t *it = &s->items[i];
        if ((it->tag == 'N' || it->tag == 'F' || it->tag == 'T') && !ts_node_is_null(it->name)) {
            parsed[nparsed++] = ts_node_start_byte(it->name);
        }
    }
    qsort(parsed, (size_t)nparsed, sizeof(uint32_t), cs_u32_cmp);
    for (int h = 0; h < s->nheads && !s->failed; h++) {
        cs_text_item(s, &s->heads[h], parsed, nparsed);
    }
    if (s->nitems > tree_items) {
        qsort(s->items, (size_t)s->nitems, sizeof(cs_item_t), cs_item_start_cmp);
    }
}

/* ── Emission ────────────────────────────────────────────────────── */

static void cs_put_bases(cs_scan_t *s, TSNode type_decl, char kind) {
    if (kind == 'e' || kind == 'd') {
        return; /* an enum's base is its underlying integral type */
    }
    TSNode bl = {0};
    uint32_t nc = ts_node_named_child_count(type_decl);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(type_decl, i);
        if (cs_kind_is(c, "base_list")) {
            bl = c;
            break;
        }
    }
    if (ts_node_is_null(bl)) {
        return;
    }
    bool first = true;
    uint32_t bc = ts_node_named_child_count(bl);
    for (uint32_t i = 0; i < bc; i++) {
        TSNode b = ts_node_named_child(bl, i);
        if (cs_kind_is(b, "primary_constructor_base_type")) {
            TSNode ty = cs_field(b, "type");
            if (ts_node_is_null(ty) && ts_node_named_child_count(b) > 0) {
                ty = ts_node_named_child(b, 0);
            }
            b = ty;
        } else if (cs_kind_is(b, "argument_list") || cs_kind_is(b, "comment")) {
            continue;
        }
        if (ts_node_is_null(b)) {
            continue;
        }
        if (!first) {
            sb_putc(&s->sb, '|');
        }
        cs_put_text_nows(s, b);
        first = false;
    }
}

static void cs_emit_member(cs_scan_t *s, TSNode decl, char kind, bool explicit_impl,
                           const char *type_path, TSNode name, TSNode tparams, TSNode params) {
    char *nm = cs_name_dup(s, name);
    if (!nm) {
        return;
    }
    sb_puts(&s->sb, "M\t");
    sb_putu(&s->sb, cs_line(decl));
    sb_putc(&s->sb, '\t');
    sb_putc(&s->sb, kind);
    sb_putc(&s->sb, '\t');
    sb_putc(&s->sb, explicit_impl ? '1' : '0');
    sb_putc(&s->sb, '\t');
    sb_puts(&s->sb, type_path);
    sb_putc(&s->sb, '.');
    sb_puts(&s->sb, nm);
    sb_putc(&s->sb, '\t');
    cs_put_tparams(s, tparams);
    sb_putc(&s->sb, '\t');
    if (kind == 'c') {
        cs_put_sig(s, params);
    } else {
        sb_putc(&s->sb, '-');
    }
    sb_putc(&s->sb, '\n');
}

static void cs_emit_using(cs_scan_t *s, TSNode u, int region) {
    bool is_global = false;
    bool is_static = false;
    bool is_alias = false;
    uint32_t cc = ts_node_child_count(u);
    for (uint32_t i = 0; i < cc; i++) {
        TSNode ch = ts_node_child(u, i);
        if (ts_node_is_named(ch)) {
            continue;
        }
        const char *t = ts_node_type(ch);
        if (strcmp(t, "global") == 0) {
            is_global = true;
        } else if (strcmp(t, "static") == 0) {
            is_static = true;
        } else if (strcmp(t, "=") == 0) {
            is_alias = true;
        }
    }
    TSNode alias = is_alias ? cs_field(u, "name") : (TSNode){0};
    TSNode target = {0};
    uint32_t nc = ts_node_named_child_count(u);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode ch = ts_node_named_child(u, i);
        if (is_alias && ts_node_eq(ch, alias)) {
            continue;
        }
        if (cs_kind_is(ch, "comment")) {
            continue;
        }
        target = ch;
    }
    if (ts_node_is_null(target)) {
        return;
    }
    char kind = is_alias ? 'a' : (is_static ? 's' : 'n');
    if (is_global && kind == 'n') {
        kind = 'g';
    }
    sb_puts(&s->sb, "U\t");
    sb_putu(&s->sb, (uint32_t)region);
    sb_putc(&s->sb, '\t');
    sb_putc(&s->sb, kind);
    sb_putc(&s->sb, '\t');
    if (is_alias && !ts_node_is_null(alias)) {
        cs_put_text_nows(s, alias);
    } else {
        sb_putc(&s->sb, '-');
    }
    sb_putc(&s->sb, '\t');
    cs_put_text_nows(s, target);
    sb_putc(&s->sb, '\n');
}

/* Lines [from, to] hold declarations that could not be placed. */
static void cs_emit_unplaced(cs_scan_t *s, uint32_t from, uint32_t to) {
    sb_puts(&s->sb, "X\t");
    sb_putu(&s->sb, from);
    sb_putc(&s->sb, '\t');
    sb_putu(&s->sb, to);
    sb_putc(&s->sb, '\n');
}

/* A namespace region record; returns the new region id. */
static int cs_emit_region(cs_scan_t *s, int parent, uint32_t start, uint32_t end,
                          const char *full_ns) {
    int id = s->next_region++;
    sb_puts(&s->sb, "R\t");
    sb_putu(&s->sb, (uint32_t)id);
    sb_putc(&s->sb, '\t');
    sb_putu(&s->sb, (uint32_t)parent);
    sb_putc(&s->sb, '\t');
    sb_putu(&s->sb, start);
    sb_putc(&s->sb, '\t');
    sb_putu(&s->sb, end);
    sb_putc(&s->sb, '\t');
    sb_puts(&s->sb, full_ns);
    sb_putc(&s->sb, '\n');
    return id;
}

static void cs_emit_type(cs_scan_t *s, const cs_item_t *it, int region, uint32_t end_line,
                         const char *path, bool incomplete) {
    sb_puts(&s->sb, "T\t");
    sb_putu(&s->sb, (uint32_t)region);
    sb_putc(&s->sb, '\t');
    sb_putu(&s->sb, it->line);
    sb_putc(&s->sb, '\t');
    sb_putu(&s->sb, end_line);
    sb_putc(&s->sb, '\t');
    sb_putc(&s->sb, it->kind);
    if (incomplete) {
        sb_putc(&s->sb, '!');
    }
    sb_putc(&s->sb, '\t');
    sb_puts(&s->sb, path);
    sb_putc(&s->sb, '\t');
    if (it->from_text) {
        /* its header did not parse: the bases are unknown, which the "?"
         * records as a hierarchy that cannot be followed */
        sb_puts(&s->sb, it->text_tparams);
        sb_puts(&s->sb, it->kind == 'e' ? "\t\n" : "\t?\n");
        return;
    }
    cs_put_tparams(s, it->tparams);
    sb_putc(&s->sb, '\t');
    cs_put_bases(s, it->node, it->kind);
    sb_putc(&s->sb, '\n');
    /* Positional record parameters are properties. */
    if (it->kind != 'r') {
        return;
    }
    TSNode null_node = {0};
    uint32_t nc = ts_node_named_child_count(it->node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode pl = ts_node_named_child(it->node, i);
        if (!cs_kind_is(pl, "parameter_list")) {
            continue;
        }
        uint32_t pc = ts_node_named_child_count(pl);
        for (uint32_t j = 0; j < pc; j++) {
            TSNode p = ts_node_named_child(pl, j);
            if (cs_kind_is(p, "parameter")) {
                cs_emit_member(s, p, 'v', false, path, cs_field(p, "name"), null_node, null_node);
            }
        }
    }
}

/* An open block while the items are placed. */
typedef struct {
    int item;         /* tree_idx of its declaration; CBM_NOT_FOUND for the file itself */
    uint32_t end;     /* first byte after it */
    int inner_depth;  /* brace depth of what it holds (files with parse errors) */
    int region;       /* namespace region in effect inside */
    const char *ns;   /* namespace in effect inside */
    const char *path; /* type path ("" outside a type) */
    bool is_type;
    bool bad; /* its own place or name is unknown: so is everything inside */
} cs_frame_t;

typedef struct {
    cs_frame_t frames[CS_SCAN_MAX_DEPTH + SKIP_ONE];
    int sp;
} cs_stack_t;

static void cs_frame_push(cs_scan_t *s, cs_stack_t *st, cs_frame_t f) {
    if (st->sp > CS_SCAN_MAX_DEPTH) {
        s->failed = true; /* deeper than any real program: no scope for the file */
        return;
    }
    st->frames[st->sp++] = f;
}

/* What follows a namespace's name in a file with parse errors: the brace
 * that opens its block (returned), or the `;` of a file-scoped namespace
 * (*file_scoped). Recovery reports a block namespace it could not parse as a
 * file-scoped one whose content is an error node; the text says which it is.
 * Anything else -- `namespace A` / `#else` / `namespace B` / `#endif` / `{`
 * -- is a namespace this scan cannot name: CBM_NOT_FOUND, not file-scoped. */
static int cs_namespace_brace(const cs_scan_t *s, TSNode name, bool *file_scoped) {
    const char *src = s->ctx->source;
    uint32_t after = cs_skip_space(src, ts_node_end_byte(name), s->root_end_byte);
    *file_scoped = after < s->root_end_byte && src[after] == ';';
    return cs_open_brace_at(s, after);
}

static void cs_place_namespace(cs_scan_t *s, cs_stack_t *st, const cs_item_t *it, bool trusted) {
    const cs_frame_t top = st->frames[st->sp - SKIP_ONE];
    const char *name = it->from_text ? it->text_name : cs_name_dup(s, it->name);
    int brace = it->brace;
    bool file_scoped = it->tag == 'F';
    if (!it->from_text && s->lexical) {
        /* the text decides what kind of namespace declaration this is */
        file_scoped = false;
        brace = ts_node_is_null(it->name) ? CBM_NOT_FOUND
                                          : cs_namespace_brace(s, it->name, &file_scoped);
    }
    /* a file-scoped namespace is the first declaration of its file */
    bool ok = trusted && !top.is_type && name && (!file_scoped || st->sp == SKIP_ONE);
    uint32_t end = s->root_end_byte;
    uint32_t end_line = s->root_end_line;
    int inner = top.inner_depth;
    if (!file_scoped) {
        bool known = false;
        if (brace >= 0) {
            known = cs_brace_extent(s, brace, &end, &end_line, &inner);
        } else if (!s->lexical && !it->from_text) {
            end = ts_node_end_byte(it->node);
            end_line = cs_end_line(it->node);
            known = true;
        }
        if (!known) {
            ok = false;
            end = UINT32_MAX; /* where it ends is unknown: nothing after it is placed */
        }
    }
    const char *full = ok ? cs_join_path(s, top.ns, name) : NULL;
    ok = ok && full;
    if (!ok && !top.bad && it->start < s->untrusted) {
        cs_emit_unplaced(s, it->line, end == UINT32_MAX ? s->root_end_line : end_line);
    }
    int region = ok ? cs_emit_region(s, top.region, it->line, end_line, full) : top.region;
    cs_frame_push(s, st,
                  (cs_frame_t){.item = it->tree_idx,
                               .end = end,
                               .inner_depth = inner,
                               .region = region,
                               .ns = ok ? full : top.ns,
                               .path = "",
                               .is_type = false,
                               .bad = !ok});
}

static void cs_place_type(cs_scan_t *s, cs_stack_t *st, const cs_item_t *it, bool trusted) {
    const cs_frame_t top = st->frames[st->sp - SKIP_ONE];
    const char *name = it->from_text ? it->text_name : cs_name_dup(s, it->name);
    bool block = it->brace >= 0;
    uint32_t end = it->start;
    uint32_t end_line = it->line;
    int inner = 0;
    bool paired = true;
    bool whole = false; /* the tree's extent is the block's extent */
    if (it->from_text) {
        paired = !block || cs_brace_extent(s, it->brace, &end, &end_line, &inner);
    } else {
        TSNode body = it->kind == 'd' ? (TSNode){0} : cs_field(it->node, "body");
        block = !ts_node_is_null(body);
        end = ts_node_end_byte(it->node);
        end_line = cs_end_line(it->node);
        whole = true;
        if (block && s->lexical) {
            uint32_t node_end = end;
            paired = cs_brace_extent(s, cs_open_brace_at(s, ts_node_start_byte(body)), &end,
                                     &end_line, &inner);
            whole = paired && end == node_end;
        }
    }
    const char *path = name ? cs_join_path(s, top.path, name) : NULL;
    bool ok = trusted && paired && path;
    if (ok) {
        cs_emit_type(s, it, top.region, end_line, path, it->broken || !whole);
    } else if (name) {
        /* declared, but where is unknown: its name must not resolve to
         * anything else either */
        sb_puts(&s->sb, "Q\t");
        sb_puts(&s->sb, name);
        sb_putc(&s->sb, '\n');
    }
    if (!ok && !top.bad && block && it->start < s->untrusted) {
        cs_emit_unplaced(s, it->line, paired ? end_line : s->root_end_line);
    }
    if (block) {
        cs_frame_push(s, st,
                      (cs_frame_t){.item = it->tree_idx,
                                   .end = paired ? end : UINT32_MAX,
                                   .inner_depth = inner,
                                   .region = top.region,
                                   .ns = top.ns,
                                   .path = path ? path : "",
                                   .is_type = true,
                                   .bad = !ok});
    }
}

/* Place every collected declaration in the block that holds it and write
 * its record. */
static void cs_emit_items(cs_scan_t *s) {
    cs_stack_t *st = (cs_stack_t *)cs_tmp_alloc(s, sizeof(*st));
    if (!st) {
        return;
    }
    st->sp = 0;
    st->frames[st->sp++] = (cs_frame_t){.item = CBM_NOT_FOUND,
                                        .end = UINT32_MAX,
                                        .inner_depth = 0,
                                        .region = 0,
                                        .ns = "",
                                        .path = "",
                                        .is_type = false,
                                        .bad = false};
    for (int i = 0; i < s->nitems && !s->failed && !s->sb.failed; i++) {
        const cs_item_t *it = &s->items[i];
        /* leave the blocks that ended; with parse errors also those the
         * braces say this item is not in (an #else branch re-opening the
         * block its #if branch opened) */
        int depth = s->lexical ? cs_depth_at(s, it->start) : 0;
        while (st->sp > SKIP_ONE &&
               (st->frames[st->sp - SKIP_ONE].end <= it->start ||
                (s->lexical && st->frames[st->sp - SKIP_ONE].inner_depth > depth))) {
            st->sp--;
        }
        const cs_frame_t top = st->frames[st->sp - SKIP_ONE];
        bool trusted = !top.bad;
        if (trusted && s->lexical) {
            trusted = it->start < s->untrusted && depth == top.inner_depth;
        }
        switch (it->tag) {
        case 'U':
            if (trusted && !top.is_type) {
                cs_emit_using(s, it->node, top.region);
            }
            break;
        case 'N':
        case 'F':
            cs_place_namespace(s, st, it, trusted);
            break;
        case 'T':
            cs_place_type(s, st, it, trusted);
            break;
        case 'M':
            /* the braces decide whose member it is when the tree has errors */
            if (trusted && top.is_type && (s->lexical || it->owner == top.item)) {
                cs_emit_member(s, it->node, it->kind, it->explicit_impl, top.path, it->name,
                               it->tparams, it->params);
            }
            break;
        default:
            break;
        }
    }
}

/* Field positions (0-based, tag included) that hold line numbers. */
static bool cs_line_field(char tag, int field) {
    switch (tag) {
    case 'R':
        return field == 3 || field == 4;
    case 'T':
        return field == 2 || field == 3;
    case 'M':
        return field == 1;
    case 'X':
        return field == 1 || field == 2;
    default:
        return false;
    }
}

char *cbm_doclink_cs_portable_scope(const char *scope) {
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
            if (cs_line_field(tag, field)) {
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
            p = nl + SKIP_ONE;
        } else {
            p += len;
        }
    }
    out[w] = '\0';
    return out;
}

const char *cbm_doclink_cs_scan_scope(CBMExtractCtx *ctx) {
    if (ts_node_is_null(ctx->root)) {
        return NULL;
    }
    cs_scan_t s = {.ctx = ctx,
                   .tmp = ctx->scratch ? ctx->scratch : ctx->arena,
                   .sb = {.a = ctx->arena},
                   .untrusted = UINT32_MAX,
                   .next_region = SKIP_ONE};
    s.root_end_byte = ctx->source_len > 0 ? (uint32_t)ctx->source_len : 0;
    s.root_end_line = cs_end_line(ctx->root);
    s.lexical = ts_node_has_error(ctx->root);
    if (s.lexical) {
        cs_scan_tokens(&s);
    }
    /* the root itself is an error node when nothing of the file parsed */
    cs_collect(&s, ctx->root, cs_kind_is(ctx->root, "ERROR") ? CS_OWNER_LEXICAL : CS_OWNER_NONE, 0);
    cs_add_text_items(&s);
    sb_puts(&s.sb, CBM_DOCLINK_CS_SCOPE_TAG "\n");
    cs_emit_items(&s);
    if (s.untrusted != UINT32_MAX) {
        cs_emit_unplaced(&s, s.untrusted_row + TS_LINE_OFFSET, s.root_end_line);
    }
    if (s.failed || s.sb.failed || !s.sb.buf) {
        return NULL;
    }
    return s.sb.buf;
}
