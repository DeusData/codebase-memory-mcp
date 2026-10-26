/*
 * doc_adoc.c — AsciiDoc documents in the graph (the extraction half;
 * src/pipeline/doc_links_adoc.c resolves), and the Antora scope blob.
 *
 * AsciiDoc has no grammar here: like a PDF, the file is read directly. Each
 * heading (`= Title` .. `====== Title`, or `#` .. `######`) outside a
 * delimited block becomes a Section (name the title, the text up to the next
 * heading as its docstring, its lines up to the next heading); text before
 * the first heading belongs to the file. Delimited blocks (`----` listing,
 * `....` literal, `////` comment, `++++` passthrough, `====` example, `****`
 * sidebar, `____` quote, `|===` table, ``` fences) open and close on the same
 * delimiter line (the field test's H8 rules).
 *
 * References become tokens whose `raw` is the reference as written, then
 * TAB-separated fields:
 *   include     include::target[attrs] (Asciidoctor's preprocessor directive:
 *               read in every block but a comment block; `\include::` is
 *               escaped): written, target with the page's attributes
 *               substituted (references the page does not define stay as
 *               `{name}` for the component's), tags (`tag=` / `tags=`),
 *               lines (`lines=`)
 *   attribute   an attribute reference `{name}` in the text: written, name,
 *               the page's value as written and as the page expands it (both
 *               "" when the page does not define it).
 *               Antora components define their API links as attributes
 *               (`{Assertions}` -> a javadoc page): the resolver reads them.
 *   code_path,  a monospace span (`x`, ``x``, `+x+`) naming a path or a
 *   code_name   qualified name (the Markdown code-span classifier); raw is
 *               the span's text
 * Page attributes (`:name: value`, `:name!:` / `:!name:` to unset) apply from
 * their line on, in document order.
 *
 * The Antora scope (cbm_doclink_antora_scan_scope): an antora.yml's component
 * name, its asciidoc attributes and its collector scans; an
 * antora-playbook*.yml's asciidoc attributes. Read as text, never run.
 */
#include "doclink.h"

#include "arena.h"
#include "cbm.h"
#include "helpers.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    AD_HEAD_MAX = 6,      /* `======` */
    AD_DELIM_MIN = 4,     /* `----` and its kin */
    AD_TABLE_MIN = 3,     /* `|===` */
    AD_FENCE_MIN = 3,     /* ``` */
    AD_BODY_MAX = 500,    /* a section's docstring (Markdown's MAX_COMMENT_LEN) */
    AD_SPAN_MAX = 150,    /* a span the classifier reads (MD_SPAN_MAX) */
    AD_ATTRS_INIT = 16,   /* first capacity of the page attribute list */
    AD_EXPAND_DEPTH = 10, /* nested attribute references expanded (H8) */
    AD_FIELD_MAX = 512,   /* an attribute value or target longer than this is skipped */
};

typedef struct {
    const char *s;
    int len;
} ad_line_t;

typedef struct {
    char *name;
    char *value;
} ad_attr_t;

typedef struct {
    int line; /* 0-based */
    const char *title;
    const char *qn;
    bool shared_qn;
} ad_head_t;

typedef struct {
    CBMExtractCtx *ctx;
    CBMArena *scratch;
    ad_line_t *L;
    int n;
    unsigned char *in_block; /* 1: inside a delimited block; 2: inside a comment block */
    ad_head_t *heads;
    int nheads;
    ad_attr_t *attrs;
    int nattrs;
    int cap_attrs;
    char *span_buf;
    bool failed;
} ad_doc_t;

static bool ad_blank(char c) {
    return c == ' ' || c == '\t' || c == '\r';
}

static bool ad_word(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

/* The line without trailing whitespace: its length. */
static int ad_rlen(const ad_line_t *l) {
    int n = l->len;
    while (n > 0 && ad_blank(l->s[n - 1])) {
        n--;
    }
    return n;
}

/* ADOC_DELIM_RE: the delimiter's length, or 0. */
static int ad_delim(const ad_line_t *l) {
    int n = ad_rlen(l);
    if (n == 0) {
        return 0;
    }
    const char *s = l->s;
    if (s[0] == '|') {
        int k = 1;
        while (k < n && s[k] == '=') {
            k++;
        }
        return (k == n && n - 1 >= AD_TABLE_MIN) ? n : 0;
    }
    if (!strchr("-.=*_+/`", s[0])) {
        return 0;
    }
    for (int k = 1; k < n; k++) {
        if (s[k] != s[0]) {
            return 0;
        }
    }
    int min = s[0] == '`' ? AD_FENCE_MIN : AD_DELIM_MIN;
    return n >= min ? n : 0;
}

/* ADOC_HEAD_RE ^(={1,6}|#{1,6})\s+(\S.*?)\s*$: the level and the title span. */
static int ad_heading(const ad_line_t *l, int *ta, int *tb) {
    const char *s = l->s;
    int n = ad_rlen(l);
    if (n == 0 || (s[0] != '=' && s[0] != '#')) {
        return 0;
    }
    int k = 0;
    while (k < n && s[k] == s[0]) {
        k++;
    }
    if (k > AD_HEAD_MAX || k >= n || !ad_blank(s[k])) {
        return 0;
    }
    int a = k;
    while (a < n && ad_blank(s[a])) {
        a++;
    }
    if (a >= n) {
        return 0;
    }
    *ta = a;
    *tb = n;
    return k;
}

static bool ad_lines(ad_doc_t *d, const char *src, int len) {
    int count = 1;
    for (int i = 0; i < len; i++) {
        count += src[i] == '\n';
    }
    d->L = (ad_line_t *)cbm_arena_alloc(d->scratch, (size_t)count * sizeof(*d->L));
    d->in_block = (unsigned char *)cbm_arena_alloc(d->scratch, (size_t)count);
    if (!d->L || !d->in_block) {
        return false;
    }
    memset(d->in_block, 0, (size_t)count);
    int pos = 0;
    for (int i = 0; i < count; i++) {
        const char *nl = memchr(src + pos, '\n', (size_t)(len - pos));
        int end = nl ? (int)(nl - src) : len;
        d->L[i] = (ad_line_t){src + pos, end - pos};
        pos = end + 1;
    }
    d->n = count;
    return true;
}

/* Delimited blocks, then the headings outside them. */
static void ad_structure(ad_doc_t *d) {
    int open = -1;
    for (int i = 0; i < d->n; i++) {
        int dl = ad_delim(&d->L[i]);
        if (open < 0) {
            if (dl) {
                open = i;
                d->in_block[i] = d->L[i].s[0] == '/' ? 2 : 1;
            }
            continue;
        }
        const ad_line_t *o = &d->L[open];
        d->in_block[i] = d->L[open].s[0] == '/' ? 2 : 1;
        if (dl && dl == ad_rlen(o) && memcmp(d->L[i].s, o->s, (size_t)dl) == 0) {
            open = -1;
        }
    }
    d->heads = (ad_head_t *)cbm_arena_alloc(d->scratch, (size_t)d->n * sizeof(*d->heads));
    if (!d->heads) {
        d->failed = true;
        return;
    }
    for (int i = 0; i < d->n; i++) {
        int ta;
        int tb;
        if (d->in_block[i] || ad_heading(&d->L[i], &ta, &tb) == 0) {
            continue;
        }
        char *title = cbm_arena_strndup(d->ctx->arena, d->L[i].s + ta, (size_t)(tb - ta));
        if (!title) {
            d->failed = true;
            return;
        }
        d->heads[d->nheads++] = (ad_head_t){.line = i, .title = title};
    }
}

/* qn_safe_segment (extract_defs.c): whitespace runs become '-'. */
static const char *ad_qn_segment(CBMArena *a, const char *name) {
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

/* The section's own text, whitespace collapsed, at most AD_BODY_MAX bytes and
 * never a cut UTF-8 character. */
static const char *ad_body(ad_doc_t *d, int from, int to) {
    char *out = (char *)cbm_arena_alloc(d->ctx->arena, AD_BODY_MAX + 1);
    if (!out) {
        return NULL;
    }
    int w = 0;
    for (int k = from; k < to && w < AD_BODY_MAX; k++) {
        const char *s = d->L[k].s;
        int len = d->L[k].len;
        for (int i = 0; i < len && w < AD_BODY_MAX; i++) {
            char c = s[i];
            if (ad_blank(c)) {
                if (w > 0 && out[w - 1] != ' ') {
                    out[w++] = ' ';
                }
                continue;
            }
            out[w++] = c;
        }
        if (w > 0 && w < AD_BODY_MAX && out[w - 1] != ' ') {
            out[w++] = ' ';
        }
    }
    while (w > 0 && ((unsigned char)out[w - 1] & 0xC0) == 0x80) {
        w--; /* never end inside a character */
    }
    if (w > 0 && ((unsigned char)out[w - 1] & 0xC0) == 0xC0) {
        w--;
    }
    while (w > 0 && out[w - 1] == ' ') {
        w--;
    }
    out[w] = '\0';
    return w > 0 ? out : NULL;
}

static int ad_head_qn_cmp(const void *a, const void *b) {
    const ad_head_t *x = *(const ad_head_t *const *)a;
    const ad_head_t *y = *(const ad_head_t *const *)b;
    return strcmp(x->qn, y->qn);
}

static void ad_sections(ad_doc_t *d) {
    CBMExtractCtx *ctx = d->ctx;
    CBMArena *a = ctx->arena;
    for (int h = 0; h < d->nheads; h++) {
        const char *seg = ad_qn_segment(a, d->heads[h].title);
        d->heads[h].qn = seg ? cbm_fqn_compute(a, ctx->project, ctx->rel_path, seg) : NULL;
        if (!d->heads[h].qn) {
            d->failed = true;
            return;
        }
    }
    if (d->nheads > 1) {
        ad_head_t **by = (ad_head_t **)cbm_arena_alloc(d->scratch, (size_t)d->nheads * sizeof(*by));
        if (!by) {
            d->failed = true;
            return;
        }
        for (int h = 0; h < d->nheads; h++) {
            by[h] = &d->heads[h];
        }
        qsort(by, (size_t)d->nheads, sizeof(*by), ad_head_qn_cmp);
        for (int h = 1; h < d->nheads; h++) {
            if (strcmp(by[h]->qn, by[h - 1]->qn) == 0) {
                by[h]->shared_qn = true;
                by[h - 1]->shared_qn = true;
            }
        }
    }
    for (int h = 0; h < d->nheads; h++) {
        const ad_head_t *hd = &d->heads[h];
        int end = h + 1 < d->nheads ? d->heads[h + 1].line : d->n;
        int last = end - 1;
        while (last > hd->line && ad_rlen(&d->L[last]) == 0) {
            last--;
        }
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = hd->title;
        def.qualified_name = hd->qn;
        def.label = "Section";
        def.file_path = ctx->rel_path;
        def.start_line = (uint32_t)hd->line + 1;
        def.end_line = (uint32_t)last + 1;
        def.is_exported = true;
        def.docstring = ad_body(d, hd->line + 1, end);
        cbm_defs_push(&ctx->result->defs, a, def);
    }
}

/* ── Page attributes ─────────────────────────────────────────────── */

static ad_attr_t *ad_attr_find(ad_doc_t *d, const char *name, size_t n) {
    for (int i = 0; i < d->nattrs; i++) {
        if (strlen(d->attrs[i].name) == n && memcmp(d->attrs[i].name, name, n) == 0) {
            return &d->attrs[i];
        }
    }
    return NULL;
}

/* ATTR_ENTRY_RE ^:(!?)([\w][\w\-]*)(!?):(?:\s+(.*?))?\s*$ -- true when the
 * line is an attribute entry (set or unset). */
static bool ad_attr_entry(ad_doc_t *d, const ad_line_t *l) {
    const char *s = l->s;
    int n = ad_rlen(l);
    if (n < 3 || s[0] != ':') {
        return false;
    }
    int k = 1;
    bool unset = false;
    if (s[k] == '!') {
        unset = true;
        k++;
    }
    int na = k;
    if (k >= n || !ad_word(s[k])) {
        return false;
    }
    while (k < n && (ad_word(s[k]) || s[k] == '-')) {
        k++;
    }
    int nb = k;
    if (k < n && s[k] == '!') {
        unset = true;
        k++;
    }
    if (k >= n || s[k] != ':') {
        return false;
    }
    k++;
    if (k < n && !ad_blank(s[k])) {
        return false;
    }
    while (k < n && ad_blank(s[k])) {
        k++;
    }
    ad_attr_t *a = ad_attr_find(d, s + na, (size_t)(nb - na));
    if (unset) {
        if (a) {
            *a = d->attrs[--d->nattrs];
        }
        return true;
    }
    char *value = cbm_arena_strndup(d->scratch, s + k, (size_t)(n - k));
    if (!value) {
        d->failed = true;
        return true;
    }
    if (a) {
        a->value = value;
        return true;
    }
    if (d->nattrs >= d->cap_attrs) {
        int ncap = d->cap_attrs ? d->cap_attrs * 2 : AD_ATTRS_INIT;
        ad_attr_t *grown = (ad_attr_t *)cbm_arena_alloc(d->scratch, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            d->failed = true;
            return true;
        }
        if (d->nattrs) {
            memcpy(grown, d->attrs, (size_t)d->nattrs * sizeof(*grown));
        }
        d->attrs = grown;
        d->cap_attrs = ncap;
    }
    char *name = cbm_arena_strndup(d->scratch, s + na, (size_t)(nb - na));
    if (!name) {
        d->failed = true;
        return true;
    }
    d->attrs[d->nattrs++] = (ad_attr_t){name, value};
    return true;
}

/* A `{name}` reference at s[i] (not escaped `\{`): the name's span. */
static bool ad_attr_ref(const char *s, int n, int i, int *a, int *b) {
    if (s[i] != '{' || (i > 0 && s[i - 1] == '\\') || i + 2 >= n || !ad_word(s[i + 1])) {
        return false;
    }
    int k = i + 1;
    while (k < n && (ad_word(s[k]) || s[k] == '-')) {
        k++;
    }
    if (k >= n || s[k] != '}') {
        return false;
    }
    *a = i + 1;
    *b = k;
    return true;
}

/* The text with the page's attribute references substituted, pass after pass
 * (a value may reference others; at most AD_EXPAND_DEPTH passes, as H8's
 * depth); references the page does not define stay as written. NULL when it
 * grows past AD_FIELD_MAX or memory ran out. */
static char *ad_expand(ad_doc_t *d, const char *s, int n) {
    char cur[AD_FIELD_MAX + 1];
    char out[AD_FIELD_MAX + 1];
    if (n > AD_FIELD_MAX) {
        return NULL;
    }
    memcpy(cur, s, (size_t)n);
    int cl = n;
    for (int pass = 0; pass < AD_EXPAND_DEPTH; pass++) {
        int w = 0;
        bool changed = false;
        for (int i = 0; i < cl; i++) {
            int a;
            int b;
            const ad_attr_t *at = NULL;
            if (ad_attr_ref(cur, cl, i, &a, &b)) {
                at = ad_attr_find(d, cur + a, (size_t)(b - a));
            }
            if (at) {
                int vl = (int)strlen(at->value);
                if (w + vl > AD_FIELD_MAX) {
                    return NULL;
                }
                memcpy(out + w, at->value, (size_t)vl);
                w += vl;
                i = b;
                changed = true;
                continue;
            }
            if (w + 1 > AD_FIELD_MAX) {
                return NULL;
            }
            out[w++] = cur[i];
        }
        if (w > 0) {
            memcpy(cur, out, (size_t)w);
        }
        cl = w;
        if (!changed) {
            break;
        }
    }
    return cbm_arena_strndup(d->scratch, cur, (size_t)cl);
}

/* ── Tokens ──────────────────────────────────────────────────────── */

static void ad_push(ad_doc_t *d, int line, int syntax, const char *raw) {
    if (!raw) {
        d->failed = true;
        return;
    }
    CBMExtractCtx *ctx = d->ctx;
    int lo = 0;
    int hi = d->nheads;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / 2);
        if (d->heads[mid].line <= line) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    const ad_head_t *sec = lo > 0 ? &d->heads[lo - 1] : NULL;
    if (sec && sec->shared_qn) {
        sec = NULL;
    }
    CBMDocLink link = {
        .source_qn = sec ? sec->qn : (ctx->module_qn ? ctx->module_qn : ""),
        .raw = raw,
        .line = (uint32_t)line + 1,
        .def_line = sec ? (uint32_t)sec->line + 1 : 1,
        .syntax = (uint16_t)syntax,
        .flags = sec ? 0 : CBM_DOCLINK_FLAG_FILE,
    };
    cbm_doclinks_push(&ctx->result->doc_links, ctx->arena, link);
}

/* A value of the include's attribute list (`tag=x`, `tags="a;b"`, `lines=1..5`). */
static void ad_attrlist_value(const char *al, int n, const char *key, char *out, size_t cap) {
    out[0] = '\0';
    size_t kl = strlen(key);
    for (int i = 0; i + (int)kl < n; i++) {
        if ((i > 0 && ad_word(al[i - 1])) || memcmp(al + i, key, kl) != 0) {
            continue;
        }
        int k = i + (int)kl;
        while (k < n && ad_blank(al[k])) {
            k++;
        }
        if (k >= n || al[k] != '=') {
            continue;
        }
        k++;
        while (k < n && ad_blank(al[k])) {
            k++;
        }
        int a = k;
        int b;
        if (k < n && al[k] == '"') {
            a = k + 1;
            const char *q = memchr(al + a, '"', (size_t)(n - a));
            b = q ? (int)(q - al) : n;
        } else {
            b = a;
            while (b < n && al[b] != ',') {
                b++;
            }
        }
        while (b > a && ad_blank(al[b - 1])) {
            b--;
        }
        snprintf(out, cap, "%.*s", b - a, al + a);
        return;
    }
}

/* INCLUDE_RE ^(\\?)include::([^\[\s][^\[]*)\[(.*)\]\s*$ */
static void ad_include(ad_doc_t *d, int i) {
    const ad_line_t *l = &d->L[i];
    const char *s = l->s;
    int n = ad_rlen(l);
    static const char kw[] = "include::";
    int kl = (int)strlen(kw);
    if (n < kl + 2 || memcmp(s, kw, (size_t)kl) != 0 || s[kl] == '[' || ad_blank(s[kl]) ||
        s[n - 1] != ']') {
        return;
    }
    const char *open = memchr(s + kl, '[', (size_t)(n - kl));
    if (!open) {
        return;
    }
    int ta = kl;
    int tb = (int)(open - s);
    int aa = tb + 1;
    int ab = n - 1;
    char *target = ad_expand(d, s + ta, tb - ta);
    if (!target || strchr(target, '\t')) {
        return;
    }
    char tags[AD_FIELD_MAX];
    char tag1[AD_FIELD_MAX];
    char lines[AD_FIELD_MAX];
    ad_attrlist_value(s + aa, ab - aa, "tags", tags, sizeof(tags));
    ad_attrlist_value(s + aa, ab - aa, "tag", tag1, sizeof(tag1));
    ad_attrlist_value(s + aa, ab - aa, "lines", lines, sizeof(lines));
    char *written = cbm_arena_strndup(d->ctx->arena, s, (size_t)n);
    ad_push(d, i, CBM_DOCLINK_ADOC_INCLUDE,
            written ? cbm_arena_sprintf(d->ctx->arena, "%s\t%s\t%s\t%s", written, target,
                                        tags[0] ? tags : tag1, lines)
                    : NULL);
}

/* Built-in attributes: characters and document settings, never a link. */
static bool ad_builtin(const char *s, size_t n) {
    static const char *const names[] = {"nbsp",       "sp",
                                        "empty",      "blank",
                                        "zwsp",       "wj",
                                        "apos",       "quot",
                                        "lsquo",      "rsquo",
                                        "ldquo",      "rdquo",
                                        "deg",        "plus",
                                        "brvbar",     "vbar",
                                        "amp",        "lt",
                                        "gt",         "startsb",
                                        "endsb",      "caret",
                                        "asterisk",   "tilde",
                                        "backslash",  "backtick",
                                        "two-colons", "two-semicolons",
                                        "cpp",        "cxx",
                                        "pp",         "docname",
                                        "docdir",     "docfile",
                                        "doctitle",   "toc",
                                        "version",    "revnumber",
                                        "revdate",    "author",
                                        "email",      "page-component-version",
                                        NULL};
    for (int i = 0; names[i]; i++) {
        if (strlen(names[i]) == n && memcmp(names[i], s, n) == 0) {
            return true;
        }
    }
    return false;
}

static void ad_attr_refs(ad_doc_t *d, int i) {
    const ad_line_t *l = &d->L[i];
    const char *s = l->s;
    int n = l->len;
    for (int k = 0; k < n; k++) {
        int a;
        int b;
        if (!ad_attr_ref(s, n, k, &a, &b) || ad_builtin(s + a, (size_t)(b - a))) {
            continue;
        }
        const ad_attr_t *at = ad_attr_find(d, s + a, (size_t)(b - a));
        char *value = at ? ad_expand(d, at->value, (int)strlen(at->value)) : NULL;
        if (at && (!value || strchr(value, '\t') || strchr(at->value, '\t'))) {
            k = b;
            continue;
        }
        ad_push(d, i, CBM_DOCLINK_ADOC_ATTRIBUTE,
                cbm_arena_sprintf(d->ctx->arena, "{%.*s}\t%.*s\t%s\t%s", b - a, s + a, b - a, s + a,
                                  at ? at->value : "", value ? value : ""));
        k = b;
    }
}

/* Monospace spans of a prose line: `x` and ``x``, a `+x+` passthrough's inner
 * text; each a code_path / code_name when the classifier names one. */
static void ad_spans(ad_doc_t *d, int i) {
    const ad_line_t *l = &d->L[i];
    const char *s = l->s;
    int n = l->len;
    int k = 0;
    while (k < n) {
        if (s[k] != '`') {
            k++;
            continue;
        }
        int run = 1;
        while (k + run < n && s[k + run] == '`') {
            run++;
        }
        if (run > 2) {
            k += run;
            continue;
        }
        int a = k + run;
        int e = a;
        while (e + run <= n && !(memcmp(s + e, run == 2 ? "``" : "`", (size_t)run) == 0)) {
            e++;
        }
        if (e + run > n || e == a) {
            k = a;
            continue;
        }
        int b = e;
        if (b - a >= 2 && s[a] == '+' && s[b - 1] == '+') {
            a++;
            b--;
        }
        CBMDocLinkMdPath p;
        if (b > a && b - a <= AD_SPAN_MAX &&
            cbm_doclink_md_classify_span(s + a, (size_t)(b - a), d->span_buf, AD_SPAN_MAX + 1,
                                         &p)) {
            ad_push(d, i,
                    p.shape == CBM_DOCLINK_MD_QUALIFIED ? CBM_DOCLINK_ADOC_CODE_NAME
                                                        : CBM_DOCLINK_ADOC_CODE_PATH,
                    cbm_arena_strndup(d->ctx->arena, s + a, (size_t)(b - a)));
        }
        k = e + run;
    }
}

static void ad_tokens(ad_doc_t *d) {
    for (int i = 0; i < d->n && !d->failed; i++) {
        if (d->in_block[i] == 2) {
            continue; /* a comment block: nothing in it is read */
        }
        const ad_line_t *l = &d->L[i];
        if (l->len >= 2 && l->s[0] == '/' && l->s[1] == '/' && !d->in_block[i]) {
            continue; /* a line comment */
        }
        if (!d->in_block[i] && ad_attr_entry(d, l)) {
            continue;
        }
        if (l->len > 0 && l->s[0] == 'i') {
            ad_include(d, i);
        }
        ad_attr_refs(d, i);
        if (!d->in_block[i]) {
            ad_spans(d, i);
        }
    }
}

void cbm_adoc_extract_document(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || !ctx->source || ctx->source_len <= 0) {
        return;
    }
    ad_doc_t d = {.ctx = ctx, .scratch = ctx->scratch ? ctx->scratch : ctx->arena};
    d.span_buf = (char *)cbm_arena_alloc(d.scratch, (size_t)(AD_SPAN_MAX + 1) * 2);
    if (!d.span_buf || !ad_lines(&d, ctx->source, ctx->source_len)) {
        ctx->result->doc_links.failed = true;
        return;
    }
    ad_structure(&d);
    if (!d.failed) {
        ad_sections(&d);
    }
    if (!d.failed) {
        ad_tokens(&d);
    }
    if (d.failed) {
        ctx->result->doc_links.failed = true;
    }
}

/* ── The Antora scope (antora.yml, antora-playbook*.yml) ─────────── */

typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool failed;
} ad_sb_t;

static void ad_put(ad_sb_t *sb, const char *s, size_t n) {
    if (sb->failed) {
        return;
    }
    if (sb->len + n + 1 > sb->cap) {
        size_t ncap = sb->cap ? sb->cap : AD_FIELD_MAX;
        while (ncap < sb->len + n + 1) {
            ncap *= 2;
        }
        char *grown = (char *)cbm_arena_alloc(sb->a, ncap);
        if (!grown) {
            sb->failed = true;
            return;
        }
        if (sb->len) {
            memcpy(grown, sb->buf, sb->len);
        }
        sb->buf = grown;
        sb->cap = ncap;
    }
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

static void ad_rec(ad_sb_t *sb, const char *kind, const char *a, size_t an, const char *b,
                   size_t bn) {
    for (size_t i = 0; i < an; i++) {
        if (a[i] == '\t' || a[i] == '\n') {
            return;
        }
    }
    for (size_t i = 0; b && i < bn; i++) {
        if (b[i] == '\t' || b[i] == '\n') {
            return;
        }
    }
    ad_put(sb, kind, strlen(kind));
    ad_put(sb, "\t", 1);
    ad_put(sb, a, an);
    if (b) {
        ad_put(sb, "\t", 1);
        ad_put(sb, b, bn);
    }
    ad_put(sb, "\n", 1);
}

static int ad_indent(const char *s, int n) {
    int k = 0;
    while (k < n && s[k] == ' ') {
        k++;
    }
    return k;
}

/* `key: value` at s (after the indent): the key and value spans (value
 * unquoted, comments not stripped: the field test's reading). */
static bool ad_yaml_kv(const char *s, int n, int *ka, int *kb, int *va, int *vb) {
    int k = 0;
    if (k >= n || !(ad_word(s[k]))) {
        return false;
    }
    while (k < n && (ad_word(s[k]) || s[k] == '-')) {
        k++;
    }
    int e = k;
    while (k < n && ad_blank(s[k])) {
        k++;
    }
    if (k >= n || s[k] != ':') {
        return false;
    }
    k++;
    while (k < n && ad_blank(s[k])) {
        k++;
    }
    *ka = 0;
    *kb = e;
    *va = k;
    *vb = n; /* the caller's line has no trailing whitespace */
    return true;
}

/* A value in matching quotes loses them. */
static void ad_unquote(const char *s, int *va, int *vb) {
    if (*vb - *va >= 2 && s[*va] == s[*vb - 1] && (s[*va] == '\'' || s[*va] == '"')) {
        (*va)++;
        (*vb)--;
    }
}

/* H8 parse_antora_yml: `name:`, `asciidoc: attributes:` and
 * `ext: collector: scan: - dir: / into:`. */
static void ad_antora(const char *src, int len, bool component, ad_sb_t *sb) {
    bool in_asciidoc = false;
    bool in_attrs = false;
    int attrs_ind = -1;
    bool in_scan = false;
    int pos = 0;
    while (pos <= len) {
        const char *nl = memchr(src + pos, '\n', (size_t)(len - pos));
        int end = nl ? (int)(nl - src) : len;
        const char *s = src + pos;
        int n = end - pos;
        while (n > 0 && ad_blank(s[n - 1])) {
            n--;
        }
        int ind = ad_indent(s, n);
        int ka;
        int kb;
        int va;
        int vb;
        bool kv = ad_yaml_kv(s + ind, n - ind, &ka, &kb, &va, &vb);
        if (kv) {
            ad_unquote(s + ind, &va, &vb);
        }
        if (ind == 0 && n > 0) {
            in_asciidoc = kv && kb == (int)strlen("asciidoc") && memcmp(s, "asciidoc", 8) == 0;
            in_attrs = false;
            in_scan = false;
            if (component && kv && kb == (int)strlen("name") && memcmp(s, "name", 4) == 0 &&
                vb > va) {
                ad_rec(sb, "N", s + va, (size_t)(vb - va), NULL, 0);
            }
        } else if (in_asciidoc && kv && va == vb && kb == (int)strlen("attributes") &&
                   memcmp(s + ind, "attributes", 10) == 0) {
            in_attrs = true;
            attrs_ind = -1;
        } else if (in_attrs && n > ind && s[ind] != '#') {
            if (attrs_ind < 0) {
                attrs_ind = ind;
            }
            if (ind < attrs_ind) {
                in_attrs = false;
            } else if (ind == attrs_ind && kv) {
                ad_rec(sb, "A", s + ind, (size_t)kb, s + ind + va, (size_t)(vb - va));
            }
        }
        if (component && kv && kb == (int)strlen("scan") && memcmp(s + ind, "scan", 4) == 0) {
            in_scan = true;
        } else if (component && in_scan) {
            const char *t = s + ind;
            int tn = n - ind;
            if (tn > 1 && t[0] == '-' && ad_blank(t[1])) {
                int sk = 1;
                while (sk < tn && ad_blank(t[sk])) {
                    sk++;
                }
                t += sk;
                tn -= sk;
            }
            int a;
            int b;
            int c;
            int e;
            bool tkv = ad_yaml_kv(t, tn, &a, &b, &c, &e);
            if (tkv) {
                ad_unquote(t, &c, &e);
            }
            if (tkv && e > c) {
                if (b == 3 && memcmp(t, "dir", 3) == 0) {
                    ad_rec(sb, "D", t + c, (size_t)(e - c), NULL, 0);
                } else if (b == 4 && memcmp(t, "into", 4) == 0) {
                    ad_rec(sb, "I", t + c, (size_t)(e - c), NULL, 0);
                }
            }
        }
        pos = end + 1;
    }
}

const char *cbm_doclink_antora_scan_scope(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->rel_path || !ctx->source || ctx->source_len <= 0) {
        return NULL;
    }
    const char *base = strrchr(ctx->rel_path, '/');
    base = base ? base + 1 : ctx->rel_path;
    bool component = strcmp(base, "antora.yml") == 0;
    bool playbook = strncmp(base, "antora-playbook", strlen("antora-playbook")) == 0;
    if (!component && !playbook) {
        return NULL;
    }
    ad_sb_t sb = {.a = ctx->arena};
    ad_put(&sb, CBM_DOCLINK_ADOC_SCOPE_TAG "\n", strlen(CBM_DOCLINK_ADOC_SCOPE_TAG "\n"));
    ad_put(&sb, component ? "C\n" : "P\n", 2);
    ad_antora(ctx->source, ctx->source_len, component, &sb);
    if (sb.failed) {
        ctx->result->doc_links.failed = true;
        return NULL;
    }
    return sb.buf;
}
