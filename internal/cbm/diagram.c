/*
 * diagram.c — diagrams as graph nodes: a Diagram definition per diagram and a
 * DiagramElement definition per element, with the arrows between elements
 * (CBMDiagramArrow, written as DIAGRAM_ARROW edges by pass_diagrams.c).
 *
 * Where diagrams come from:
 *   - a fenced block of a Markdown file whose info string names a diagram
 *     language (mermaid; plantuml, puml, uml; dot, graphviz; d2): the Diagram
 *     hangs under the Section the block is written in (the last heading above
 *     it), or under the file before any heading;
 *   - a Mermaid file (.mmd, .mermaid): one Diagram, under the file.
 *
 * Qualified names: the parent's (a Section's, else the file's module QN) +
 * "#diagram-<k>", k counting the diagrams of that parent from 1; an element's
 * is the diagram's + "." + its id (dots and blanks as `_`).
 *
 * A Diagram's docstring is the text its elements and arrows carry (their
 * labels), which is what a reader of the diagram reads; its properties hold
 * the format and the diagram kind. An element's name is its label (its id
 * when it has none); its properties hold the id and the element kind. An
 * element named only by an arrow is an element too.
 *
 * Formats (one parser each, line by line, no grammar): Mermaid flowchart and
 * graph, sequenceDiagram, classDiagram, erDiagram, stateDiagram, C4 (Context,
 * Container, Component, Dynamic, Deployment) and architecture-beta. Every
 * other Mermaid kind (gantt, pie, journey, mindmap, ...) is a Diagram without
 * elements.
 */
#include "cbm.h"
#include "doclink.h"

#include "arena.h"
#include "helpers.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"
#include "tree_sitter/api.h"

#include <yyjson/yyjson.h>
#include <zlib.h>

#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    DG_DOC_MAX = 2048,  /* a Diagram's docstring */
    DG_LABEL_MAX = 256, /* an element's or an arrow's label */
    DG_ID_MAX = 128,
    DG_ID_HEAD = DG_ID_MAX - 18, /* a long id's head: `~` and 16 hex digits follow */
    DG_KIND_MAX = 32,
    DG_ARGS_MAX = 8,
};

static const uint64_t DG_FNV_OFFSET = 0xcbf29ce484222325ULL; /* FNV-1a 64 */
static const uint64_t DG_FNV_PRIME = 0x100000001b3ULL;
enum { DG_UTF8_CONT_MASK = 0xC0, DG_UTF8_CONT = 0x80 }; /* a continuation byte: 10xxxxxx */

/* ── The builder ─────────────────────────────────────────────────── */

typedef struct {
    CBMExtractCtx *ctx;
    const char *qn; /* the diagram's */
    const char *format;
    char kind[DG_KIND_MAX];
    const char *element_kind; /* what an element named only by an arrow is */
    int def_index;            /* the Diagram definition, in ctx->result->defs */
    uint32_t line0;           /* the source line of the diagram's text */
    CBMHashTable *ids;        /* element id -> its definition's index + 1 */
    char doc[DG_DOC_MAX];     /* the labels, for the docstring */
    size_t doc_len;
    bool oom;
} dg_t;

typedef struct {
    const char *p;
    size_t n;
} dg_str_t;

static dg_str_t dg_s(const char *p, size_t n) {
    return (dg_str_t){p, n};
}

static dg_str_t dg_trim(dg_str_t s) {
    while (s.n && isspace((unsigned char)s.p[0])) {
        s.p++;
        s.n--;
    }
    while (s.n && isspace((unsigned char)s.p[s.n - SKIP_ONE])) {
        s.n--;
    }
    return s;
}

/* Quotes and Markdown-string backticks around a label dropped. */
static dg_str_t dg_unquote(dg_str_t s) {
    s = dg_trim(s);
    if (s.n >= PAIR_LEN && (s.p[0] == '"' || s.p[0] == '\'' || s.p[0] == '`') &&
        s.p[s.n - SKIP_ONE] == s.p[0]) {
        s.p++;
        s.n -= PAIR_LEN;
    }
    if (s.n >= PAIR_LEN && s.p[0] == '`' && s.p[s.n - SKIP_ONE] == '`') {
        s.p++;
        s.n -= PAIR_LEN;
    }
    return dg_trim(s);
}

static bool dg_eq(dg_str_t s, const char *lit) {
    return strlen(lit) == s.n && memcmp(s.p, lit, s.n) == 0;
}

static bool dg_starts(dg_str_t s, const char *lit) {
    size_t l = strlen(lit);
    return s.n >= l && memcmp(s.p, lit, l) == 0;
}

/* A label's text: tags (`<br>`), the line breaks DOT, PlantUML and D2 write
 * as `\n` (also `\l`, `\r`, `\t`) and blank runs as one space, at most cap-1
 * bytes. */
static size_t dg_label_text(dg_str_t s, char *out, size_t cap) {
    size_t w = 0;
    bool space = false;
    for (size_t i = 0; i < s.n && w + SKIP_ONE < cap; i++) {
        char c = s.p[i];
        if (c == '\\' && i + SKIP_ONE < s.n && strchr("nlrt", s.p[i + SKIP_ONE]) &&
            s.p[i + SKIP_ONE]) {
            i++;
            space = true;
            continue;
        }
        if (c == '<') {
            const char *gt = memchr(s.p + i, '>', s.n - i);
            if (gt) {
                i = (size_t)(gt - s.p);
                space = true;
                continue;
            }
        }
        if (isspace((unsigned char)c)) {
            space = true;
            continue;
        }
        if (space && w) {
            out[w++] = ' ';
        }
        space = false;
        out[w++] = c;
    }
    out[w] = '\0';
    return w;
}

static void dg_doc_add(dg_t *dg, const char *text, size_t n) {
    if (!n || dg->doc_len + n + PAIR_LEN >= sizeof(dg->doc)) {
        return;
    }
    if (dg->doc_len) {
        memcpy(dg->doc + dg->doc_len, "; ", PAIR_LEN);
        dg->doc_len += PAIR_LEN;
    }
    memcpy(dg->doc + dg->doc_len, text, n);
    dg->doc_len += n;
    dg->doc[dg->doc_len] = '\0';
}

static const char *dg_dup(dg_t *dg, const char *s, size_t n) {
    char *c = cbm_arena_strndup(dg->ctx->arena, s, n);
    dg->oom = dg->oom || !c;
    return c;
}

static const char **dg_props(dg_t *dg, const char *k1, const char *v1, const char *k2,
                             const char *v2) {
    const char **kv = (const char **)cbm_arena_alloc(dg->ctx->arena, CBM_SZ_5 * sizeof(char *));
    if (!kv) {
        dg->oom = true;
        return NULL;
    }
    kv[0] = k1;
    kv[SKIP_ONE] = v1;
    kv[PAIR_LEN] = k2;
    kv[CBM_SZ_3] = v2;
    kv[CBM_SZ_4] = NULL;
    return kv;
}

/* An element id as a QN segment: dots, blanks, backslashes, quotes and `#`
 * (QN suffix syntax) as `_`. */
static const char *dg_element_qn(dg_t *dg, dg_str_t id) {
    char seg[DG_ID_MAX];
    size_t n = id.n < sizeof(seg) - SKIP_ONE ? id.n : sizeof(seg) - SKIP_ONE;
    for (size_t i = 0; i < n; i++) {
        char c = id.p[i];
        seg[i] =
            (c == '.' || c == '\\' || c == '"' || c == '#' || isspace((unsigned char)c)) ? '_' : c;
    }
    seg[n] = '\0';
    char *qn = cbm_arena_sprintf(dg->ctx->arena, "%s.%s", dg->qn, seg);
    dg->oom = dg->oom || !qn;
    return qn;
}

/* An element id as its table key and QN source. An id too long for one (a
 * DOT node named by a sentence) keeps its head, cut at a character boundary,
 * and the FNV-1a 64 of the whole: still one key per id. */
static size_t dg_id_key(dg_str_t id, char *key) {
    if (id.n < DG_ID_MAX) {
        memcpy(key, id.p, id.n);
        key[id.n] = '\0';
        return id.n;
    }
    uint64_t h = DG_FNV_OFFSET;
    for (size_t i = 0; i < id.n; i++) {
        h = (h ^ (unsigned char)id.p[i]) * DG_FNV_PRIME;
    }
    size_t head = DG_ID_HEAD;
    while (head > 0 && ((unsigned char)id.p[head] & DG_UTF8_CONT_MASK) == DG_UTF8_CONT) {
        head--; /* not inside a UTF-8 sequence */
    }
    int n = snprintf(key, DG_ID_MAX, "%.*s~%016llx", (int)head, id.p, (unsigned long long)h);
    return n > 0 ? (size_t)n : 0;
}

/* An explicit link written on an element (or, for a PlantUML !include, on the
 * diagram): a doc-link record the pipeline resolves as a Markdown link. An
 * in-page anchor, a data URI or a script links nothing. */
static void dg_link(dg_t *dg, const char *source_qn, dg_str_t raw, int syntax, uint32_t line) {
    raw = dg_unquote(raw);
    if (!source_qn || !raw.n || raw.p[0] == '#' || dg_starts(raw, "data:") ||
        dg_starts(raw, "javascript:")) {
        return;
    }
    CBMDocLink link = {
        .source_qn = source_qn,
        .raw = dg_dup(dg, raw.p, raw.n),
        .line = line,
        .def_line = line,
        .syntax = (uint16_t)syntax,
    };
    if (link.raw) {
        cbm_doclinks_push(&dg->ctx->result->doc_links, dg->ctx->arena, link);
    }
}

/* The element `id` (made on first sight), its label set when it has none. */
static const char *dg_element(dg_t *dg, dg_str_t id, dg_str_t label, const char *kind,
                              uint32_t line) {
    id = dg_trim(id);
    if (!id.n || dg->oom) {
        return NULL;
    }
    char key[DG_ID_MAX];
    size_t kn = dg_id_key(id, key);
    char text[DG_LABEL_MAX];
    size_t tl = dg_label_text(dg_unquote(label), text, sizeof(text));
    CBMDefArray *defs = &dg->ctx->result->defs;
    void *v = cbm_ht_get(dg->ids, key);
    if (v) {
        CBMDefinition *d = &defs->items[(uintptr_t)v - SKIP_ONE];
        if (tl && d->name == d->extra_props[SKIP_ONE]) { /* still named by its id */
            d->name = dg_dup(dg, text, tl);
            dg_doc_add(dg, text, tl);
        }
        return d->qualified_name;
    }
    const char *qn = dg_element_qn(dg, dg_s(key, kn));
    const char *idc = dg_dup(dg, id.p, id.n);
    if (!qn || !idc) {
        return NULL;
    }
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = tl ? dg_dup(dg, text, tl) : idc;
    def.qualified_name = qn;
    def.label = "DiagramElement";
    def.file_path = dg->ctx->rel_path;
    def.start_line = line;
    def.end_line = line;
    def.parent_class = dg->qn;
    /* the kind may live in a parser buffer (a JSON value, a draw.io cell) */
    const char *kindc = dg_dup(dg, kind, strlen(kind));
    def.extra_props = dg_props(dg, "element_id", idc, "element_kind", kindc);
    if (dg->oom || !def.name) {
        return NULL;
    }
    cbm_defs_push(defs, dg->ctx->arena, def);
    const char *k = dg_dup(dg, key, kn);
    if (k) {
        cbm_ht_set(dg->ids, k, (void *)(uintptr_t)defs->count);
    }
    if (tl) {
        dg_doc_add(dg, text, tl);
    }
    return qn;
}

static void dg_arrow(dg_t *dg, dg_str_t from, dg_str_t to, dg_str_t label, const char *style,
                     uint32_t line) {
    const char *kind = dg->element_kind ? dg->element_kind : "node";
    const char *a = dg_element(dg, from, dg_s(NULL, 0), kind, line);
    const char *b = dg_element(dg, to, dg_s(NULL, 0), kind, line);
    if (!a || !b) {
        return;
    }
    char text[DG_LABEL_MAX];
    size_t tl = dg_label_text(dg_unquote(label), text, sizeof(text));
    CBMDiagramArrow arrow = {.from_qn = a,
                             .to_qn = b,
                             .label = tl ? dg_dup(dg, text, tl) : NULL,
                             .style = style,
                             .line = line};
    if (!dg->oom) {
        cbm_diagram_arrows_push(&dg->ctx->result->diagram_arrows, dg->ctx->arena, arrow);
    }
    if (tl) {
        dg_doc_add(dg, text, tl);
    }
}

/* ── Lines ───────────────────────────────────────────────────────── */

typedef struct {
    const char *p;
    const char *end;
    uint32_t line;
} dg_lines_t;

/* The next line (no newline), false at the end. */
static bool dg_next_line(dg_lines_t *it, dg_str_t *out, uint32_t *line) {
    if (it->p >= it->end) {
        return false;
    }
    const char *nl = memchr(it->p, '\n', (size_t)(it->end - it->p));
    const char *le = nl ? nl : it->end;
    *out = dg_s(it->p, (size_t)(le - it->p));
    if (out->n && out->p[out->n - SKIP_ONE] == '\r') {
        out->n--;
    }
    *line = it->line++;
    it->p = nl ? nl + SKIP_ONE : it->end;
    return true;
}

/* A Mermaid line without its `%%` comment. */
static dg_str_t dg_mermaid_strip(dg_str_t s) {
    for (size_t i = 0; i + SKIP_ONE < s.n; i++) {
        if (s.p[i] == '%' && s.p[i + SKIP_ONE] == '%') {
            s.n = i;
            break;
        }
    }
    return dg_trim(s);
}

/* ── Mermaid: flowchart ──────────────────────────────────────────── */

static bool dg_id_char(char c) {
    return isalnum((unsigned char)c) || c == '_' || c == '-' || (unsigned char)c >= 0x80;
}

/* The closing bracket of a node shape opened at s[i]: the shape's text in
 * *label, the position after it. */
static size_t dg_shape(dg_str_t s, size_t i, dg_str_t *label) {
    static const char *const opens[] = {"(((", "([", "[[", "[(", "((", "{{", "[/",
                                        "[\\", "[",  "(",  "{",  ">",  NULL};
    static const char *const closes[] = {")))", "])", "]]", ")]", "))", "}}", "/]",
                                         "\\]", "]",  ")",  "}",  "]",  NULL};
    for (int k = 0; opens[k]; k++) {
        size_t ol = strlen(opens[k]);
        if (i + ol > s.n || memcmp(s.p + i, opens[k], ol) != 0) {
            continue;
        }
        size_t cl = strlen(closes[k]);
        bool quoted = false;
        for (size_t j = i + ol; j + cl <= s.n; j++) {
            if (s.p[j] == '"') {
                quoted = !quoted;
            }
            if (!quoted && memcmp(s.p + j, closes[k], cl) == 0) {
                *label = dg_s(s.p + i + ol, j - i - ol);
                return j + cl;
            }
        }
        /* `[/text\]` and `[\text/]` close with the other slash */
        if (k == CBM_SZ_6 || k == CBM_SZ_7) {
            const char *alt = k == CBM_SZ_6 ? "\\]" : "/]";
            for (size_t j = i + ol; j + PAIR_LEN <= s.n; j++) {
                if (memcmp(s.p + j, alt, PAIR_LEN) == 0) {
                    *label = dg_s(s.p + i + ol, j - i - ol);
                    return j + PAIR_LEN;
                }
            }
        }
        return i;
    }
    return i;
}

/* A node at s[i]: its id and label; the position after it (i when none). */
static size_t dg_flow_node(dg_str_t s, size_t i, dg_str_t *id, dg_str_t *label) {
    size_t j = i;
    while (j < s.n && dg_id_char(s.p[j])) {
        if (s.p[j] == '-' && j + SKIP_ONE < s.n &&
            (s.p[j + SKIP_ONE] == '-' || s.p[j + SKIP_ONE] == '.' || s.p[j + SKIP_ONE] == '>')) {
            break; /* an edge starts */
        }
        j++;
    }
    if (j == i) {
        return i;
    }
    *id = dg_s(s.p + i, j - i);
    *label = dg_s(NULL, 0);
    if (j < s.n && s.p[j] == '@' && j + SKIP_ONE < s.n && s.p[j + SKIP_ONE] == '{') {
        const char *close = memchr(s.p + j, '}', s.n - j);
        if (close) {
            dg_str_t body = dg_s(s.p + j + PAIR_LEN, (size_t)(close - s.p) - j - PAIR_LEN);
            const char *lab = NULL;
            for (size_t k = 0; k + CBM_SZ_6 <= body.n && !lab; k++) {
                lab = memcmp(body.p + k, "label:", CBM_SZ_6) == 0 ? body.p + k + CBM_SZ_6 : NULL;
            }
            if (lab) {
                const char *comma = memchr(lab, ',', (size_t)(body.p + body.n - lab));
                *label = dg_s(lab, (size_t)((comma ? comma : body.p + body.n) - lab));
            }
            return (size_t)(close - s.p) + SKIP_ONE;
        }
    }
    size_t k = dg_shape(s, j, label);
    if (k > j) {
        j = k;
    }
    if (j + CBM_SZ_3 <= s.n && memcmp(s.p + j, ":::", CBM_SZ_3) == 0) {
        j += CBM_SZ_3;
        while (j < s.n && dg_id_char(s.p[j])) {
            j++;
        }
    }
    return j;
}

static size_t dg_skip_ws(dg_str_t s, size_t i) {
    while (i < s.n && isspace((unsigned char)s.p[i])) {
        i++;
    }
    return i;
}

static bool dg_link_char(char c) {
    return c == '-' || c == '=' || c == '.' || c == '>' || c == '<' || c == '~';
}

/* An edge at s[i]: its style and label; the position after it (i when none). */
static size_t dg_flow_edge(dg_str_t s, size_t i, const char **style, dg_str_t *label) {
    size_t j = i;
    if (j < s.n && (s.p[j] == '<' || s.p[j] == 'x' || s.p[j] == 'o') && j + SKIP_ONE < s.n &&
        (s.p[j + SKIP_ONE] == '-' || s.p[j + SKIP_ONE] == '=')) {
        j++;
    }
    size_t start = j;
    while (j < s.n && dg_link_char(s.p[j])) {
        j++;
    }
    size_t run = j - start;
    if (run < PAIR_LEN) {
        return i;
    }
    if (j < s.n && (s.p[j] == 'x' || s.p[j] == 'o') &&
        (j + SKIP_ONE >= s.n || !dg_id_char(s.p[j + SKIP_ONE]))) {
        j++; /* an arrowhead x or o */
    }
    dg_str_t op = dg_s(s.p + i, j - i);
    *label = dg_s(NULL, 0);
    bool head =
        memchr(op.p, '>', op.n) || op.p[op.n - SKIP_ONE] == 'x' || op.p[op.n - SKIP_ONE] == 'o';
    if (!head && (dg_eq(op, "--") || dg_eq(op, "==") || dg_eq(op, "-."))) {
        /* `-- text -->`, `== text ==>`, `-. text .->`: the label runs to the
         * closing link, which opens with `--`, `==` or `.-` */
        char c0 = op.p[0] == '-' && op.p[SKIP_ONE] == '.' ? '.' : op.p[0];
        char c1 = op.p[0] == '-' && op.p[SKIP_ONE] == '.' ? '-' : op.p[SKIP_ONE];
        for (size_t k = j; k + SKIP_ONE < s.n; k++) {
            if (s.p[k] == c0 && s.p[k + SKIP_ONE] == c1) {
                size_t e = k;
                while (e < s.n && dg_link_char(s.p[e])) {
                    e++;
                }
                *label = dg_s(s.p + j, k - j);
                op = dg_s(s.p + i, e - i);
                j = e;
                break;
            }
        }
    }
    bool dotted = memchr(op.p, '.', op.n) != NULL;
    bool thick = memchr(op.p, '=', op.n) != NULL;
    bool both = op.p[0] == '<' && memchr(op.p + SKIP_ONE, '>', op.n - SKIP_ONE);
    head = memchr(op.p, '>', op.n) != NULL;
    *style = both ? "bidirectional" : dotted ? "dotted" : thick ? "thick" : head ? "arrow" : "line";
    j = dg_skip_ws(s, j);
    if (j < s.n && s.p[j] == '|') {
        const char *bar = memchr(s.p + j + SKIP_ONE, '|', s.n - j - SKIP_ONE);
        if (bar) {
            *label = dg_s(s.p + j + SKIP_ONE, (size_t)(bar - s.p) - j - SKIP_ONE);
            j = (size_t)(bar - s.p) + SKIP_ONE;
        }
    }
    return j;
}

enum { DG_GROUP_MAX = 16 };

typedef struct {
    dg_str_t ids[DG_GROUP_MAX];
    int n;
} dg_group_t;

/* `A & B[label]`: the nodes of a group, made; the position after it. */
static size_t dg_flow_group(dg_t *dg, dg_str_t s, size_t i, uint32_t line, dg_group_t *g) {
    g->n = 0;
    for (;;) {
        i = dg_skip_ws(s, i);
        dg_str_t id;
        dg_str_t label;
        size_t j = dg_flow_node(s, i, &id, &label);
        if (j == i) {
            return i;
        }
        dg_element(dg, id, label, "node", line);
        if (g->n < DG_GROUP_MAX) {
            g->ids[g->n++] = id;
        }
        i = dg_skip_ws(s, j);
        if (i < s.n && s.p[i] == '&') {
            i++;
            continue;
        }
        return i;
    }
}

/* `click A "url"`, `click A href "url"` (a class diagram's `link A "url"`):
 * the element's link; a callback (`click A call f()`) links nothing. */
static void dg_mermaid_click(dg_t *dg, dg_str_t s, uint32_t line) {
    size_t kw = dg_starts(s, "click ") ? CBM_SZ_6 : CBM_SZ_5;
    dg_str_t rest = dg_trim(dg_s(s.p + kw, s.n - kw));
    size_t j = 0;
    while (j < rest.n && !isspace((unsigned char)rest.p[j])) {
        j++;
    }
    dg_str_t tail = dg_trim(dg_s(rest.p + j, rest.n - j));
    if (dg_starts(tail, "href ")) {
        tail = dg_trim(dg_s(tail.p + CBM_SZ_5, tail.n - CBM_SZ_5));
    }
    const char *close =
        tail.n && tail.p[0] == '"' ? memchr(tail.p + SKIP_ONE, '"', tail.n - SKIP_ONE) : NULL;
    if (!close) {
        return;
    }
    const char *qn = dg_element(dg, dg_s(rest.p, j), dg_s(NULL, 0),
                                dg->element_kind ? dg->element_kind : "node", line);
    dg_link(dg, qn, dg_s(tail.p + SKIP_ONE, (size_t)(close - tail.p) - SKIP_ONE),
            CBM_DOCLINK_DG_MERMAID_CLICK, line);
}

static void dg_flow_statement(dg_t *dg, dg_str_t s, uint32_t line) {
    if (dg_starts(s, "click ")) {
        dg_mermaid_click(dg, s, line);
        return;
    }
    static const char *const skip[] = {"classDef ", "class ",     "style ", "linkStyle ",
                                       "click ",    "direction ", "end",    NULL};
    for (int k = 0; skip[k]; k++) {
        if (dg_starts(s, skip[k])) {
            return;
        }
    }
    if (dg_starts(s, "subgraph")) {
        /* `subgraph id [title]`, `subgraph id[title]`, or `subgraph Some title`
         * (the title is its id too) */
        dg_str_t rest = dg_trim(dg_s(s.p + CBM_SZ_8, s.n - CBM_SZ_8));
        dg_str_t id;
        dg_str_t label;
        size_t j = dg_flow_node(rest, 0, &id, &label);
        if (j == 0) {
            return;
        }
        size_t k = dg_skip_ws(rest, j);
        if (!label.n && k < rest.n && rest.p[k] == '[') {
            dg_shape(rest, k, &label);
        } else if (!label.n && k < rest.n) {
            id = rest; /* a title of several words */
            label = rest;
        }
        dg_element(dg, id, label, "subgraph", line);
        return;
    }
    dg_group_t left;
    size_t i = dg_flow_group(dg, s, 0, line, &left);
    while (i < s.n && left.n) {
        const char *style = NULL;
        dg_str_t label;
        size_t j = dg_flow_edge(s, i, &style, &label);
        if (j == i) {
            return;
        }
        dg_group_t right;
        i = dg_flow_group(dg, s, j, line, &right);
        for (int a = 0; a < left.n; a++) {
            for (int b = 0; b < right.n; b++) {
                dg_arrow(dg, left.ids[a], right.ids[b], label, style, line);
            }
        }
        left = right;
    }
}

static void dg_flowchart(dg_t *dg, dg_lines_t *it) {
    dg_str_t ln;
    uint32_t line = 0;
    while (dg_next_line(it, &ln, &line)) {
        ln = dg_mermaid_strip(ln);
        size_t start = 0;
        bool quoted = false;
        int depth = 0;
        for (size_t i = 0; i <= ln.n; i++) {
            char c = i < ln.n ? ln.p[i] : ';';
            quoted = c == '"' ? !quoted : quoted;
            depth += !quoted && (c == '[' || c == '(' || c == '{');
            depth -= !quoted && (c == ']' || c == ')' || c == '}') && depth > 0;
            if (c == ';' && !quoted && depth == 0) {
                dg_str_t st = dg_trim(dg_s(ln.p + start, i - start));
                if (st.n) {
                    dg_flow_statement(dg, st, line);
                }
                start = i + SKIP_ONE;
            }
        }
    }
}

/* ── Mermaid: sequence ───────────────────────────────────────────── */

/* A message line `A->>+B: text`: its arrow; false when it has none. */
static bool dg_sequence_message(dg_t *dg, dg_str_t ln, uint32_t line) {
    static const char *const arrows[] = {"<<-->>", "<<->>", "-->>", "->>", "--x", "-x",
                                         "--)",    "-)",    "-->",  "->",  NULL};
    const char *colon = memchr(ln.p, ':', ln.n);
    dg_str_t head = colon ? dg_s(ln.p, (size_t)(colon - ln.p)) : ln;
    for (int k = 0; arrows[k]; k++) {
        size_t al = strlen(arrows[k]);
        for (size_t i = 0; i + al <= head.n; i++) {
            if (memcmp(head.p + i, arrows[k], al) != 0) {
                continue;
            }
            dg_str_t to = dg_trim(dg_s(head.p + i + al, head.n - i - al));
            while (to.n && (to.p[0] == '+' || to.p[0] == '-')) {
                to.p++;
                to.n--;
            }
            dg_str_t label = colon
                                 ? dg_s(colon + SKIP_ONE, (size_t)(ln.p + ln.n - colon) - SKIP_ONE)
                                 : dg_s(NULL, 0);
            bool reply = arrows[k][0] == '-' && arrows[k][SKIP_ONE] == '-';
            dg_arrow(dg, dg_s(head.p, i), to, label, reply ? "reply" : "message", line);
            return true;
        }
    }
    return false;
}

static void dg_sequence(dg_t *dg, dg_lines_t *it) {
    static const char participant[] = "participant ";
    dg_str_t ln;
    uint32_t line = 0;
    while (dg_next_line(it, &ln, &line)) {
        ln = dg_mermaid_strip(ln);
        if (dg_starts(ln, "create ")) {
            ln = dg_trim(dg_s(ln.p + CBM_SZ_7, ln.n - CBM_SZ_7));
        }
        bool actor = dg_starts(ln, "actor ");
        if (actor || dg_starts(ln, participant)) {
            size_t kl = actor ? CBM_SZ_6 : sizeof(participant) - SKIP_ONE;
            dg_str_t rest = dg_trim(dg_s(ln.p + kl, ln.n - kl));
            dg_str_t id = rest;
            dg_str_t label = dg_s(NULL, 0);
            for (size_t i = 0; i + CBM_SZ_4 <= rest.n; i++) {
                if (memcmp(rest.p + i, " as ", CBM_SZ_4) == 0) {
                    id = dg_s(rest.p, i);
                    label = dg_s(rest.p + i + CBM_SZ_4, rest.n - i - CBM_SZ_4);
                    break;
                }
            }
            dg_element(dg, id, label, actor ? "actor" : "participant", line);
            continue;
        }
        (void)dg_sequence_message(dg, ln, line);
    }
}

/* ── Mermaid: class and ER ───────────────────────────────────────── */

typedef struct {
    const char *op;
    const char *style;
    bool to_left; /* the arrow points at the left side (child -> parent) */
} dg_rel_t;

static const dg_rel_t DG_CLASS_RELS[] = {
    {"<|--|>", "inheritance", false},
    {"<|--", "inheritance", true},
    {"--|>", "inheritance", false},
    {"<|..", "realization", true},
    {"..|>", "realization", false},
    {"*--", "composition", false},
    {"--*", "composition", true},
    {"o--", "aggregation", false},
    {"--o", "aggregation", true},
    {"<--", "association", true},
    {"-->", "association", false},
    {"<..", "dependency", true},
    {"..>", "dependency", false},
    {"--", "link", false},
    {"..", "link", false},
    {NULL, NULL, false},
};

/* `A "1" op "*" B : label`: the relation of a class line, false when none. */
static bool dg_class_relation(dg_t *dg, dg_str_t ln, uint32_t line) {
    const char *colon = NULL;
    bool quoted = false;
    for (size_t i = 0; i < ln.n && !colon; i++) {
        quoted = ln.p[i] == '"' ? !quoted : quoted;
        colon = !quoted && ln.p[i] == ':' ? ln.p + i : NULL;
    }
    dg_str_t head = colon ? dg_s(ln.p, (size_t)(colon - ln.p)) : ln;
    for (int k = 0; DG_CLASS_RELS[k].op; k++) {
        size_t ol = strlen(DG_CLASS_RELS[k].op);
        for (size_t i = 0; i + ol <= head.n; i++) {
            if (memcmp(head.p + i, DG_CLASS_RELS[k].op, ol) != 0) {
                continue;
            }
            dg_str_t l = dg_trim(dg_s(head.p, i));
            dg_str_t r = dg_trim(dg_s(head.p + i + ol, head.n - i - ol));
            if (l.n && l.p[l.n - SKIP_ONE] == '"') { /* a cardinality */
                const char *q = memchr(l.p, '"', l.n - SKIP_ONE);
                l = q ? dg_trim(dg_s(l.p, (size_t)(q - l.p))) : l;
            }
            if (r.n && r.p[0] == '"') {
                const char *q = memchr(r.p + SKIP_ONE, '"', r.n - SKIP_ONE);
                r = q ? dg_trim(dg_s(q + SKIP_ONE, (size_t)(r.p + r.n - q) - SKIP_ONE)) : r;
            }
            if (!l.n || !r.n) {
                return false;
            }
            dg_str_t label = colon
                                 ? dg_s(colon + SKIP_ONE, (size_t)(ln.p + ln.n - colon) - SKIP_ONE)
                                 : dg_s(NULL, 0);
            dg_element(dg, l, dg_s(NULL, 0), "class", line);
            dg_element(dg, r, dg_s(NULL, 0), "class", line);
            if (DG_CLASS_RELS[k].to_left) {
                dg_arrow(dg, r, l, label, DG_CLASS_RELS[k].style, line);
            } else {
                dg_arrow(dg, l, r, label, DG_CLASS_RELS[k].style, line);
            }
            return true;
        }
    }
    return false;
}

/* The name of `class Name~T~["Label"] {`, its label in *label. */
static dg_str_t dg_class_decl(dg_str_t rest, dg_str_t *label) {
    size_t j = 0;
    while (j < rest.n && (dg_id_char(rest.p[j]) || rest.p[j] == '.')) {
        j++;
    }
    dg_str_t name = dg_s(rest.p, j);
    *label = dg_s(NULL, 0);
    const char *lb = memchr(rest.p + j, '[', rest.n - j);
    const char *rb = lb ? memchr(lb, ']', (size_t)(rest.p + rest.n - lb)) : NULL;
    if (lb && rb) {
        *label = dg_s(lb + SKIP_ONE, (size_t)(rb - lb) - SKIP_ONE);
    }
    return name;
}

static void dg_classdiagram(dg_t *dg, dg_lines_t *it) {
    dg_str_t ln;
    uint32_t line = 0;
    int body = 0;
    while (dg_next_line(it, &ln, &line)) {
        ln = dg_mermaid_strip(ln);
        if (body > 0) {
            body -= ln.n && ln.p[0] == '}';
            continue;
        }
        if (dg_starts(ln, "class ")) {
            dg_str_t label;
            dg_str_t name = dg_class_decl(dg_trim(dg_s(ln.p + CBM_SZ_6, ln.n - CBM_SZ_6)), &label);
            dg_element(dg, name, label, "class", line);
            body += ln.n && ln.p[ln.n - SKIP_ONE] == '{';
            continue;
        }
        if (dg_starts(ln, "click ") || dg_starts(ln, "link ")) {
            dg_mermaid_click(dg, ln, line);
            continue;
        }
        if (dg_starts(ln, "namespace ") || dg_starts(ln, "note") || dg_starts(ln, "<<") ||
            dg_starts(ln, "style ") || dg_starts(ln, "cssClass") || dg_starts(ln, "classDef") ||
            dg_starts(ln, "direction") || dg_starts(ln, "link ") || dg_starts(ln, "callback ") ||
            dg_starts(ln, "click ") || dg_eq(ln, "}")) {
            continue;
        }
        if (!dg_class_relation(dg, ln, line)) {
            const char *colon = memchr(ln.p, ':', ln.n);
            if (colon) { /* `Name : +member`: the class, no member */
                dg_element(dg, dg_s(ln.p, (size_t)(colon - ln.p)), dg_s(NULL, 0), "class", line);
            }
        }
    }
}

static bool dg_er_card(char c) {
    return c == '|' || c == 'o' || c == '}' || c == '{';
}

static void dg_erdiagram(dg_t *dg, dg_lines_t *it) {
    dg_str_t ln;
    uint32_t line = 0;
    int body = 0;
    while (dg_next_line(it, &ln, &line)) {
        ln = dg_mermaid_strip(ln);
        if (body > 0) {
            body -= ln.n && ln.p[0] == '}';
            continue;
        }
        const char *colon = memchr(ln.p, ':', ln.n);
        dg_str_t head = colon ? dg_s(ln.p, (size_t)(colon - ln.p)) : ln;
        bool done = false;
        for (size_t i = 0; i + PAIR_LEN <= head.n && !done; i++) {
            bool solid = head.p[i] == '-' && head.p[i + SKIP_ONE] == '-';
            bool dashed = head.p[i] == '.' && head.p[i + SKIP_ONE] == '.';
            if (!(solid || dashed) || i == 0 || !dg_er_card(head.p[i - SKIP_ONE])) {
                continue;
            }
            size_t a = i;
            while (a > 0 && dg_er_card(head.p[a - SKIP_ONE])) {
                a--;
            }
            size_t b = i + PAIR_LEN;
            while (b < head.n && dg_er_card(head.p[b])) {
                b++;
            }
            dg_str_t l = dg_trim(dg_s(head.p, a));
            dg_str_t r = dg_trim(dg_s(head.p + b, head.n - b));
            dg_str_t label = colon
                                 ? dg_s(colon + SKIP_ONE, (size_t)(ln.p + ln.n - colon) - SKIP_ONE)
                                 : dg_s(NULL, 0);
            dg_element(dg, l, dg_s(NULL, 0), "entity", line);
            dg_element(dg, r, dg_s(NULL, 0), "entity", line);
            dg_arrow(dg, l, r, label, solid ? "identifying" : "non_identifying", line);
            done = true;
        }
        if (!done && ln.n && ln.p[ln.n - SKIP_ONE] == '{') {
            dg_str_t name = dg_trim(dg_s(ln.p, ln.n - SKIP_ONE));
            dg_str_t label = dg_s(NULL, 0);
            const char *lb = memchr(name.p, '[', name.n);
            if (lb && name.p[name.n - SKIP_ONE] == ']') {
                label = dg_s(lb + SKIP_ONE, (size_t)(name.p + name.n - lb) - PAIR_LEN);
                name = dg_trim(dg_s(name.p, (size_t)(lb - name.p)));
            }
            dg_element(dg, name, label, "entity", line);
            body++;
        }
    }
}

/* ── Mermaid: state ──────────────────────────────────────────────── */

enum { DG_STATE_DEPTH = 8 };

/* A state of a transition: `[*]` is the start (left) or the end (right) of
 * the composite state it is written in, `composite` (empty at the top). */
static dg_str_t dg_state_id(dg_str_t s, bool left, dg_str_t composite, char *buf, size_t cap) {
    s = dg_trim(s);
    if (!dg_eq(s, "[*]")) {
        return s;
    }
    const char *end = left ? "start" : "end";
    int n = composite.n ? snprintf(buf, cap, "%.*s_%s", (int)composite.n, composite.p, end)
                        : snprintf(buf, cap, "%s", end);
    return n > 0 && (size_t)n < cap ? dg_s(buf, (size_t)n) : dg_s(end, strlen(end));
}

static void dg_statediagram(dg_t *dg, dg_lines_t *it) {
    dg_str_t ln;
    uint32_t line = 0;
    dg_str_t stack[DG_STATE_DEPTH];
    int depth = 0;
    while (dg_next_line(it, &ln, &line)) {
        ln = dg_mermaid_strip(ln);
        if (dg_eq(ln, "}")) {
            depth -= depth > 0;
            continue;
        }
        if (dg_starts(ln, "state ")) {
            dg_str_t rest = dg_trim(dg_s(ln.p + CBM_SZ_6, ln.n - CBM_SZ_6));
            if (rest.n && rest.p[0] == '"') {
                const char *q = memchr(rest.p + SKIP_ONE, '"', rest.n - SKIP_ONE);
                if (q) {
                    dg_str_t label = dg_s(rest.p + SKIP_ONE, (size_t)(q - rest.p) - SKIP_ONE);
                    dg_str_t after =
                        dg_trim(dg_s(q + SKIP_ONE, (size_t)(rest.p + rest.n - q) - SKIP_ONE));
                    if (dg_starts(after, "as ")) {
                        dg_element(dg, dg_trim(dg_s(after.p + CBM_SZ_3, after.n - CBM_SZ_3)), label,
                                   "state", line);
                    }
                }
                continue;
            }
            size_t j = 0;
            while (j < rest.n && dg_id_char(rest.p[j])) {
                j++;
            }
            dg_element(dg, dg_s(rest.p, j), dg_s(NULL, 0), "state", line);
            if (rest.n && rest.p[rest.n - SKIP_ONE] == '{') {
                if (depth < DG_STATE_DEPTH) {
                    stack[depth] = dg_s(rest.p, j);
                }
                depth++;
            }
            continue;
        }
        const char *arrow = NULL;
        for (size_t i = 0; i + CBM_SZ_3 <= ln.n && !arrow; i++) {
            arrow = memcmp(ln.p + i, "-->", CBM_SZ_3) == 0 ? ln.p + i : NULL;
        }
        if (!arrow) {
            continue;
        }
        dg_str_t rest = dg_s(arrow + CBM_SZ_3, (size_t)(ln.p + ln.n - arrow) - CBM_SZ_3);
        const char *colon = memchr(rest.p, ':', rest.n);
        dg_str_t to = colon ? dg_s(rest.p, (size_t)(colon - rest.p)) : rest;
        dg_str_t label = colon
                             ? dg_s(colon + SKIP_ONE, (size_t)(rest.p + rest.n - colon) - SKIP_ONE)
                             : dg_s(NULL, 0);
        dg_str_t composite =
            depth > 0 && depth <= DG_STATE_DEPTH ? stack[depth - SKIP_ONE] : dg_s(NULL, 0);
        char from_buf[DG_ID_MAX];
        char to_buf[DG_ID_MAX];
        dg_arrow(dg,
                 dg_state_id(dg_s(ln.p, (size_t)(arrow - ln.p)), true, composite, from_buf,
                             sizeof(from_buf)),
                 dg_state_id(to, false, composite, to_buf, sizeof(to_buf)), label, "transition",
                 line);
    }
}

/* ── Mermaid: C4 ─────────────────────────────────────────────────── */

/* The arguments of `Name(a, "b", c)`: up to DG_ARGS_MAX, quotes dropped. */
static int dg_args(dg_str_t s, dg_str_t *args) {
    const char *open = memchr(s.p, '(', s.n);
    if (!open) {
        return 0;
    }
    int n = 0;
    bool quoted = false;
    const char *start = open + SKIP_ONE;
    for (const char *p = start; p < s.p + s.n && n < DG_ARGS_MAX; p++) {
        quoted = *p == '"' ? !quoted : quoted;
        if (!quoted && (*p == ',' || *p == ')')) {
            args[n++] = dg_unquote(dg_s(start, (size_t)(p - start)));
            start = p + SKIP_ONE;
            if (*p == ')') {
                break;
            }
        }
    }
    return n;
}

/* A C4 macro line `Person(id, "Label", ...)` or `Rel(a, b, "label")`: false
 * when the line is none. */
static bool dg_c4_line(dg_t *dg, dg_str_t ln, uint32_t line) {
    size_t j = 0;
    while (j < ln.n && (isalnum((unsigned char)ln.p[j]) || ln.p[j] == '_')) {
        j++;
    }
    if (j == 0 || j >= ln.n || ln.p[j] != '(') {
        return false;
    }
    {
        dg_str_t fn = dg_s(ln.p, j);
        dg_str_t args[DG_ARGS_MAX];
        int na = dg_args(ln, args);
        if (dg_starts(fn, "Rel") || dg_starts(fn, "BiRel")) {
            int base = dg_eq(fn, "RelIndex") ? SKIP_ONE : 0; /* RelIndex(i, from, to, ...) */
            if (na >= base + PAIR_LEN) {
                dg_str_t label = na > base + PAIR_LEN ? args[base + PAIR_LEN] : dg_s(NULL, 0);
                dg_arrow(dg, args[base], args[base + SKIP_ONE], label,
                         dg_starts(fn, "BiRel") ? "bidirectional" : "relation", line);
            }
            return true;
        }
        if (dg_starts(fn, "Update") || dg_starts(fn, "Add") || dg_starts(fn, "Lay_") ||
            dg_eq(fn, "title") || na < SKIP_ONE) {
            return true;
        }
        char kind[DG_KIND_MAX];
        size_t kl = fn.n < sizeof(kind) - SKIP_ONE ? fn.n : sizeof(kind) - SKIP_ONE;
        for (size_t k = 0; k < kl; k++) {
            kind[k] = (char)tolower((unsigned char)fn.p[k]);
        }
        kind[kl] = '\0';
        const char *kc = dg_dup(dg, kind, kl);
        dg_element(dg, args[0], na > SKIP_ONE ? args[SKIP_ONE] : dg_s(NULL, 0), kc ? kc : "c4",
                   line);
    }
    return true;
}

static void dg_c4(dg_t *dg, dg_lines_t *it) {
    dg_str_t ln;
    uint32_t line = 0;
    while (dg_next_line(it, &ln, &line)) {
        (void)dg_c4_line(dg, dg_mermaid_strip(ln), line);
    }
}

/* ── Mermaid: architecture ───────────────────────────────────────── */

/* `id(icon)[Label] in group`: the id; the label in *label. */
static dg_str_t dg_arch_decl(dg_str_t rest, dg_str_t *label) {
    size_t j = 0;
    while (j < rest.n && dg_id_char(rest.p[j])) {
        j++;
    }
    dg_str_t id = dg_s(rest.p, j);
    *label = dg_s(NULL, 0);
    const char *lb = memchr(rest.p + j, '[', rest.n - j);
    const char *rb = lb ? memchr(lb, ']', (size_t)(rest.p + rest.n - lb)) : NULL;
    if (lb && rb) {
        *label = dg_s(lb + SKIP_ONE, (size_t)(rb - lb) - SKIP_ONE);
    }
    return id;
}

/* `id{group}:R` or `R:id`: the id. */
static dg_str_t dg_arch_end(dg_str_t s) {
    s = dg_trim(s);
    const char *colon = memchr(s.p, ':', s.n);
    if (colon) {
        dg_str_t a = dg_s(s.p, (size_t)(colon - s.p));
        dg_str_t b = dg_s(colon + SKIP_ONE, (size_t)(s.p + s.n - colon) - SKIP_ONE);
        s = a.n > SKIP_ONE ? a : b; /* the side is one letter */
    }
    const char *brace = memchr(s.p, '{', s.n);
    return dg_trim(brace ? dg_s(s.p, (size_t)(brace - s.p)) : s);
}

static void dg_architecture(dg_t *dg, dg_lines_t *it) {
    static const char *const decls[] = {"group ", "service ", "junction ", NULL};
    dg_str_t ln;
    uint32_t line = 0;
    while (dg_next_line(it, &ln, &line)) {
        ln = dg_mermaid_strip(ln);
        bool decl = false;
        for (int k = 0; decls[k] && !decl; k++) {
            if (!dg_starts(ln, decls[k])) {
                continue;
            }
            size_t kl = strlen(decls[k]);
            dg_str_t label;
            dg_str_t id = dg_arch_decl(dg_trim(dg_s(ln.p + kl, ln.n - kl)), &label);
            char kind[DG_KIND_MAX];
            snprintf(kind, sizeof(kind), "%.*s", (int)(kl - SKIP_ONE), decls[k]);
            dg_element(dg, id, label, dg_dup(dg, kind, kl - SKIP_ONE), line);
            decl = true;
        }
        if (decl) {
            continue;
        }
        static const char *const ops[] = {"<-->", "-->", "<--", "--", NULL};
        for (int k = 0; ops[k]; k++) {
            size_t ol = strlen(ops[k]);
            const char *at = NULL;
            for (size_t i = 0; i + ol <= ln.n && !at; i++) {
                at = memcmp(ln.p + i, ops[k], ol) == 0 ? ln.p + i : NULL;
            }
            if (!at) {
                continue;
            }
            dg_str_t l = dg_arch_end(dg_s(ln.p, (size_t)(at - ln.p)));
            dg_str_t r = dg_arch_end(dg_s(at + ol, (size_t)(ln.p + ln.n - at) - ol));
            if (k == PAIR_LEN) { /* <-- points left */
                dg_arrow(dg, r, l, dg_s(NULL, 0), "arrow", line);
            } else {
                dg_arrow(dg, l, r, dg_s(NULL, 0),
                         k == 0   ? "bidirectional"
                         : k == 1 ? "arrow"
                                  : "line",
                         line);
            }
            break;
        }
    }
}

/* ── Mermaid: the header ─────────────────────────────────────────── */

typedef void (*dg_parser_fn)(dg_t *dg, dg_lines_t *it);

typedef struct {
    const char *keyword;
    const char *kind;
    const char *element_kind; /* of an element named only by an arrow */
    dg_parser_fn parse;
} dg_kind_t;

static const dg_kind_t DG_MERMAID_KINDS[] = {
    {"flowchart", "flowchart", "node", dg_flowchart},
    {"graph", "flowchart", "node", dg_flowchart},
    {"sequenceDiagram", "sequence", "participant", dg_sequence},
    {"classDiagram", "class", "class", dg_classdiagram},
    {"erDiagram", "er", "entity", dg_erdiagram},
    {"stateDiagram", "state", "state", dg_statediagram},
    {"C4Context", "c4_context", "c4", dg_c4},
    {"C4Container", "c4_container", "c4", dg_c4},
    {"C4Component", "c4_component", "c4", dg_c4},
    {"C4Dynamic", "c4_dynamic", "c4", dg_c4},
    {"C4Deployment", "c4_deployment", "c4", dg_c4},
    {"architecture", "architecture", "service", dg_architecture},
    {NULL, NULL, NULL, NULL},
};

/* A `title Text` line of the diagram (or `pie title Text` in its header),
 * when the front matter named none. */
static dg_str_t dg_title_line(dg_lines_t it, dg_str_t head) {
    for (size_t i = 0; i + CBM_SZ_7 <= head.n; i++) {
        if (memcmp(head.p + i, " title ", CBM_SZ_7) == 0) {
            return dg_unquote(dg_s(head.p + i + CBM_SZ_7, head.n - i - CBM_SZ_7));
        }
    }
    dg_str_t ln;
    uint32_t line = 0;
    while (dg_next_line(&it, &ln, &line)) {
        ln = dg_mermaid_strip(ln);
        if (dg_starts(ln, "title ")) {
            return dg_unquote(dg_s(ln.p + CBM_SZ_6, ln.n - CBM_SZ_6));
        }
    }
    return dg_s(NULL, 0);
}

/* The first line that says what the diagram is: front matter (`---` ...
 * `---`, its `title:` kept) and `%%` lines skipped. A title of the diagram's
 * own replaces one given from outside (a caption); none keeps it. */
static dg_str_t dg_mermaid_header(dg_lines_t *it, dg_str_t *title) {
    dg_str_t ln;
    uint32_t line = 0;
    bool front = false;
    while (dg_next_line(it, &ln, &line)) {
        dg_str_t t = dg_trim(ln);
        if (dg_eq(t, "---")) {
            front = !front;
            continue;
        }
        if (front) {
            if (dg_starts(t, "title:")) {
                *title = dg_unquote(dg_s(t.p + CBM_SZ_6, t.n - CBM_SZ_6));
            }
            continue;
        }
        t = dg_mermaid_strip(t);
        if (t.n) {
            return t;
        }
    }
    return dg_s(NULL, 0);
}

static void dg_mermaid(dg_t *dg, dg_lines_t *it, dg_str_t *title) {
    dg_str_t head = dg_mermaid_header(it, title);
    dg_str_t own = dg_title_line(*it, head);
    *title = own.n ? own : *title;
    size_t j = 0;
    while (j < head.n && !isspace((unsigned char)head.p[j]) && head.p[j] != ';') {
        j++;
    }
    dg_str_t word = dg_s(head.p, j);
    for (int k = 0; DG_MERMAID_KINDS[k].keyword; k++) {
        if (dg_starts(word, DG_MERMAID_KINDS[k].keyword)) {
            snprintf(dg->kind, sizeof(dg->kind), "%s", DG_MERMAID_KINDS[k].kind);
            dg->element_kind = DG_MERMAID_KINDS[k].element_kind;
            DG_MERMAID_KINDS[k].parse(dg, it);
            return;
        }
    }
    snprintf(dg->kind, sizeof(dg->kind), "%.*s",
             (int)(word.n < DG_KIND_MAX - SKIP_ONE ? word.n : DG_KIND_MAX - SKIP_ONE), word.p);
}

/* ── PlantUML ────────────────────────────────────────────────────── */

/* Declarations whose `{ ... }` body holds members, not elements. */
static const char *const DG_PUML_CLASSLIKE[] = {
    "abstract class", "abstract",  "class",  "interface", "enum",      "annotation", "protocol",
    "struct",         "exception", "object", "map",       "metaclass", NULL};
/* Declarations whose `{ ... }` body holds further elements. */
static const char *const DG_PUML_ELEMENTS[] = {
    "participant", "actor",     "boundary", "control", "database", "collections", "queue",
    "usecase",     "component", "node",     "cloud",   "frame",    "folder",      "package",
    "rectangle",   "artifact",  "file",     "storage", "card",     "agent",       "stack",
    "hexagon",     "person",    "process",  "state",   "entity",   "namespace",   NULL};

static bool dg_word_at(dg_str_t s, const char *word) {
    size_t l = strlen(word);
    return s.n > l && memcmp(s.p, word, l) == 0 && (s.p[l] == ' ' || s.p[l] == '\t');
}

/* An operand of a PlantUML line: `[Comp]`, `(use case)`, `:actor:`, `"text"`,
 * an id, or `[*]`; its id in *id; the position after it (i when none). */
static size_t dg_puml_operand(dg_str_t s, size_t i, dg_str_t *id, dg_str_t *label) {
    i = dg_skip_ws(s, i);
    *label = dg_s(NULL, 0);
    if (i + CBM_SZ_3 <= s.n && memcmp(s.p + i, "[*]", CBM_SZ_3) == 0) {
        *id = dg_s(s.p + i, CBM_SZ_3);
        return i + CBM_SZ_3;
    }
    static const char opens[] = "[(:\"";
    static const char closes[] = "]):\"";
    const char *o = i < s.n ? strchr(opens, s.p[i]) : NULL;
    if (o && s.p[i]) {
        char close = closes[o - opens];
        const char *c = memchr(s.p + i + SKIP_ONE, close, s.n - i - SKIP_ONE);
        if (!c) {
            return i;
        }
        *id = dg_s(s.p + i + SKIP_ONE, (size_t)(c - s.p) - i - SKIP_ONE);
        *label = *id;
        return (size_t)(c - s.p) + SKIP_ONE;
    }
    size_t j = i;
    while (j < s.n && (dg_id_char(s.p[j]) || s.p[j] == '.' || s.p[j] == ':' || s.p[j] == '$')) {
        if (s.p[j] == '-' || s.p[j] == ':') { /* an arrow or a label starts */
            break;
        }
        j++;
    }
    *id = dg_s(s.p + i, j - i);
    return j;
}

/* What a PlantUML arrow means, and whether it points at its left side. */
static const char *dg_puml_arrow_style(dg_str_t op, bool sequence, bool *to_left) {
    *to_left = false;
    bool lt = memchr(op.p, '<', op.n) != NULL;
    bool gt = memchr(op.p, '>', op.n) != NULL;
    bool dotted = memchr(op.p, '.', op.n) != NULL;
    bool inherit = false;
    for (size_t i = 0; i + SKIP_ONE < op.n; i++) {
        if (op.p[i] == '<' && op.p[i + SKIP_ONE] == '|') {
            inherit = true;
            *to_left = true;
        }
        if (op.p[i] == '|' && op.p[i + SKIP_ONE] == '>') {
            inherit = true;
        }
    }
    if (inherit) {
        return dotted ? "realization" : "inheritance";
    }
    if (op.p[0] == '+' || op.p[op.n - SKIP_ONE] == '+') { /* an inner class */
        *to_left = op.p[0] == '+'; /* nested -> its container, as `A ..+ B` reads */
        return "nesting";
    }
    if (op.p[0] == '*' || op.p[op.n - SKIP_ONE] == '*') {
        *to_left = op.p[op.n - SKIP_ONE] == '*';
        return "composition";
    }
    if (op.p[0] == 'o' || op.p[op.n - SKIP_ONE] == 'o') {
        *to_left = op.p[op.n - SKIP_ONE] == 'o';
        return "aggregation";
    }
    if (lt && gt) {
        return "bidirectional";
    }
    *to_left = lt;
    if (sequence) {
        return memchr(op.p, '-', op.n) && op.n >= PAIR_LEN && op.p[SKIP_ONE] == '-' ? "reply"
                                                                                    : "message";
    }
    return dotted ? (gt || lt ? "dependency" : "dotted") : (gt || lt ? "arrow" : "line");
}

static bool dg_puml_arrow_char(char c) {
    return c == '-' || c == '.' || c == '<' || c == '>' || c == '|' || c == '*' || c == 'o' ||
           c == '#' || c == '+' || c == '^' || c == '/' || c == '\\' || c == 'x' || c == '[' ||
           c == ']' || c == ',' || c == '=' || isalpha((unsigned char)c);
}

/* `A "1" -up-> "n" B : label`: false when the line holds no relation. */
static bool dg_puml_relation(dg_t *dg, dg_str_t ln, uint32_t line, bool sequence) {
    dg_str_t lid;
    dg_str_t llab;
    size_t i = dg_puml_operand(ln, 0, &lid, &llab);
    if (!lid.n) {
        return false;
    }
    i = dg_skip_ws(ln, i);
    if (i < ln.n && ln.p[i] == '"') { /* a cardinality */
        const char *q = memchr(ln.p + i + SKIP_ONE, '"', ln.n - i - SKIP_ONE);
        i = q ? dg_skip_ws(ln, (size_t)(q - ln.p) + SKIP_ONE) : i;
    }
    size_t a = i;
    while (i < ln.n && !isspace((unsigned char)ln.p[i]) && dg_puml_arrow_char(ln.p[i])) {
        i++;
    }
    dg_str_t op = dg_s(ln.p + a, i - a);
    bool arrow = false;
    for (size_t k = 0; k + SKIP_ONE < op.n && !arrow; k++) {
        arrow = (op.p[k] == '-' || op.p[k] == '.') &&
                (op.p[k + SKIP_ONE] == '-' || op.p[k + SKIP_ONE] == '.' ||
                 op.p[k + SKIP_ONE] == '>' || op.p[k + SKIP_ONE] == '|' ||
                 op.p[k + SKIP_ONE] == '[' || isalpha((unsigned char)op.p[k + SKIP_ONE]));
        arrow =
            arrow || (op.p[k] == '<' && (op.p[k + SKIP_ONE] == '-' || op.p[k + SKIP_ONE] == '.'));
    }
    if (!arrow) {
        return false;
    }
    i = dg_skip_ws(ln, i);
    if (i < ln.n && ln.p[i] == '"') {
        const char *q = memchr(ln.p + i + SKIP_ONE, '"', ln.n - i - SKIP_ONE);
        i = q ? (size_t)(q - ln.p) + SKIP_ONE : i;
    }
    dg_str_t rid;
    dg_str_t rlab;
    i = dg_puml_operand(ln, i, &rid, &rlab);
    if (!rid.n) {
        return false;
    }
    i = dg_skip_ws(ln, i);
    dg_str_t label =
        i < ln.n && ln.p[i] == ':' ? dg_s(ln.p + i + SKIP_ONE, ln.n - i - SKIP_ONE) : dg_s(NULL, 0);
    dg_element(dg, lid, llab, dg->element_kind, line);
    dg_element(dg, rid, rlab, dg->element_kind, line);
    bool to_left = false;
    const char *style = dg_puml_arrow_style(op, sequence, &to_left);
    if (to_left) {
        dg_arrow(dg, rid, lid, label, style, line);
    } else {
        dg_arrow(dg, lid, rid, label, style, line);
    }
    return true;
}

/* `[[url]]`, `[[url label]]`, `[[url{tooltip}]]` in a PlantUML line: the URL. */
static dg_str_t dg_puml_url(dg_str_t ln) {
    for (size_t i = 0; i + SKIP_ONE < ln.n; i++) {
        if (ln.p[i] == '[' && ln.p[i + SKIP_ONE] == '[') {
            size_t e = i + PAIR_LEN;
            while (e < ln.n && ln.p[e] != ']' && ln.p[e] != '{' &&
                   !isspace((unsigned char)ln.p[e])) {
                e++;
            }
            return dg_s(ln.p + i + PAIR_LEN, e - i - PAIR_LEN);
        }
    }
    return dg_s(NULL, 0);
}

/* `!include path` (also _many, _once, url): the diagram's link to the file it
 * takes in; `<stdlib>` names none of the repo's. `url of X is [[url]]`: X's. */
static bool dg_puml_link_line(dg_t *dg, dg_str_t ln, uint32_t line) {
    if (dg_starts(ln, "!include")) {
        size_t i = CBM_SZ_8;
        while (i < ln.n && !isspace((unsigned char)ln.p[i])) {
            i++;
        }
        dg_str_t arg = dg_trim(dg_s(ln.p + i, ln.n - i));
        const char *bang = memchr(arg.p, '!', arg.n); /* `file!2`: a block of it */
        arg = bang ? dg_trim(dg_s(arg.p, (size_t)(bang - arg.p))) : arg;
        if (arg.n && arg.p[0] != '<') {
            dg_link(dg, dg->qn, arg, CBM_DOCLINK_DG_PLANTUML_INCLUDE, line);
        }
        return true;
    }
    if (dg_starts(ln, "url of ") || dg_starts(ln, "url for ")) {
        size_t kw = dg_starts(ln, "url of ") ? CBM_SZ_7 : CBM_SZ_8;
        dg_str_t rest = dg_s(ln.p + kw, ln.n - kw);
        size_t j = 0;
        while (j + CBM_SZ_4 <= rest.n && memcmp(rest.p + j, " is ", CBM_SZ_4) != 0) {
            j++;
        }
        if (j + CBM_SZ_4 <= rest.n) {
            const char *qn =
                dg_element(dg, dg_unquote(dg_s(rest.p, j)), dg_s(NULL, 0), dg->element_kind, line);
            dg_link(dg, qn, dg_puml_url(rest), CBM_DOCLINK_DG_PLANTUML_URL, line);
        }
        return true;
    }
    return false;
}

/* `kw "Label" as Id`, `kw Id as "Label"`, `kw Id`, `kw [Label] as Id`: an
 * element. *body is set when its `{` opens a member body. */
static bool dg_puml_decl(dg_t *dg, dg_str_t ln, uint32_t line, bool *members) {
    const char *kw = NULL;
    bool classlike = false;
    for (int k = 0; DG_PUML_CLASSLIKE[k] && !kw; k++) {
        if (dg_word_at(ln, DG_PUML_CLASSLIKE[k])) {
            kw = DG_PUML_CLASSLIKE[k];
            classlike = true;
        }
    }
    for (int k = 0; DG_PUML_ELEMENTS[k] && !kw; k++) {
        kw = dg_word_at(ln, DG_PUML_ELEMENTS[k]) ? DG_PUML_ELEMENTS[k] : NULL;
    }
    if (!kw) {
        return false;
    }
    dg_str_t rest = dg_trim(dg_s(ln.p + strlen(kw), ln.n - strlen(kw)));
    dg_str_t id;
    dg_str_t label;
    size_t j = dg_puml_operand(rest, 0, &id, &label);
    dg_str_t after = dg_trim(dg_s(rest.p + j, rest.n - j));
    if (dg_starts(after, "as ")) {
        dg_str_t aid;
        dg_str_t alab;
        dg_puml_operand(dg_trim(dg_s(after.p + CBM_SZ_3, after.n - CBM_SZ_3)), 0, &aid, &alab);
        if (label.n || !alab.n) {
            label = label.n ? label : id;
            id = aid;
        } else {
            label = alab; /* `kw Id as "Label"` */
        }
    }
    *members = classlike && rest.n && rest.p[rest.n - SKIP_ONE] == '{';
    const char *kind = strcmp(kw, "abstract class") == 0 ? "abstract" : kw;
    const char *qn = dg_element(dg, id, label, kind, line);
    dg_link(dg, qn, dg_puml_url(after), CBM_DOCLINK_DG_PLANTUML_URL, line);
    return true;
}

/* What a PlantUML diagram is: the strongest kind any of its lines tells (the
 * packages around classes make no component diagram). */
enum {
    PK_C4,
    PK_CLASS,
    PK_SEQUENCE,
    PK_USECASE,
    PK_ACTOR, /* an actor alone: a sequence, unless use cases say otherwise */
    PK_STATE,
    PK_ACTIVITY,
    PK_COMPONENT,
    PK_COUNT
};

static const char *dg_puml_kind(dg_lines_t it) {
    static const char *const names[PK_COUNT] = {"c4",       "class", "sequence", "usecase",
                                                "sequence", "state", "activity", "component"};
    bool seen[PK_COUNT] = {false};
    dg_str_t ln;
    uint32_t line = 0;
    while (dg_next_line(&it, &ln, &line)) {
        ln = dg_trim(ln);
        seen[PK_C4] |= dg_starts(ln, "Person(") || dg_starts(ln, "System(") ||
                       dg_starts(ln, "Container(") || dg_starts(ln, "Component(") ||
                       dg_starts(ln, "Rel(");
        for (int k = 0; DG_PUML_CLASSLIKE[k]; k++) {
            seen[PK_CLASS] |= dg_word_at(ln, DG_PUML_CLASSLIKE[k]);
        }
        seen[PK_SEQUENCE] |= dg_word_at(ln, "participant") || dg_word_at(ln, "boundary") ||
                             dg_word_at(ln, "control") || dg_word_at(ln, "collections");
        seen[PK_USECASE] |= dg_word_at(ln, "usecase") || (ln.n && ln.p[0] == '(');
        seen[PK_ACTOR] |= dg_word_at(ln, "actor");
        seen[PK_STATE] |= dg_word_at(ln, "state") || dg_starts(ln, "[*]");
        seen[PK_ACTIVITY] |=
            dg_eq(ln, "start") || (ln.n && ln.p[0] == ':' && ln.p[ln.n - SKIP_ONE] == ';');
        seen[PK_COMPONENT] |= dg_word_at(ln, "component") || dg_word_at(ln, "node") ||
                              dg_word_at(ln, "package") || dg_word_at(ln, "database") ||
                              (ln.n && ln.p[0] == '[');
    }
    for (int k = 0; k < PK_COUNT; k++) {
        if (seen[k]) {
            return names[k];
        }
    }
    return "uml";
}

static bool dg_c4_line(dg_t *dg, dg_str_t ln, uint32_t line);

static void dg_plantuml(dg_t *dg, dg_lines_t *it, dg_str_t *title) {
    const char *kind = dg_puml_kind(*it);
    snprintf(dg->kind, sizeof(dg->kind), "%s", kind);
    bool sequence = strcmp(kind, "sequence") == 0;
    dg->element_kind = sequence                         ? "participant"
                       : strcmp(kind, "class") == 0     ? "class"
                       : strcmp(kind, "usecase") == 0   ? "usecase"
                       : strcmp(kind, "state") == 0     ? "state"
                       : strcmp(kind, "component") == 0 ? "component"
                                                        : "node";
    if (strcmp(kind, "activity") == 0) {
        return; /* an activity's steps are prose: the Diagram alone */
    }
    dg_str_t ln;
    uint32_t line = 0;
    int members = 0;
    bool comment = false;
    while (dg_next_line(it, &ln, &line)) {
        ln = dg_trim(ln);
        if (comment) {
            comment = !(ln.n >= PAIR_LEN && ln.p[ln.n - PAIR_LEN] == '\'' &&
                        ln.p[ln.n - SKIP_ONE] == '/');
            continue;
        }
        if (dg_starts(ln, "/'")) {
            comment = !(ln.n >= CBM_SZ_4 && ln.p[ln.n - PAIR_LEN] == '\'' &&
                        ln.p[ln.n - SKIP_ONE] == '/');
            continue;
        }
        if (members > 0) {
            members += ln.n && ln.p[ln.n - SKIP_ONE] == '{';
            members -= ln.n && ln.p[0] == '}';
            continue;
        }
        if (dg_puml_link_line(dg, ln, line)) {
            continue;
        }
        if (!ln.n || ln.p[0] == '\'' || ln.p[0] == '@' || ln.p[0] == '!' || dg_eq(ln, "}") ||
            dg_starts(ln, "skinparam") || dg_starts(ln, "hide ") || dg_starts(ln, "show ") ||
            dg_starts(ln, "note ") || dg_starts(ln, "end note") || dg_starts(ln, "legend") ||
            dg_starts(ln, "header") || dg_starts(ln, "footer") || dg_starts(ln, "left to right") ||
            dg_starts(ln, "top to bottom") || dg_starts(ln, "autonumber") ||
            dg_starts(ln, "activate") || dg_starts(ln, "deactivate")) {
            continue;
        }
        if (dg_starts(ln, "title ")) {
            if (!title->n) {
                *title = dg_unquote(dg_s(ln.p + CBM_SZ_6, ln.n - CBM_SZ_6));
            }
            continue;
        }
        if (strcmp(kind, "c4") == 0 && dg_c4_line(dg, ln, line)) {
            continue;
        }
        bool body = false;
        if (dg_puml_decl(dg, ln, line, &body)) {
            members += body;
            continue;
        }
        (void)dg_puml_relation(dg, ln, line, sequence);
    }
}

/* ── Graphviz DOT ────────────────────────────────────────────────── */

typedef enum {
    DT_END = 0,
    DT_ID,
    DT_LBRACE,
    DT_RBRACE,
    DT_LBRACK,
    DT_RBRACK,
    DT_SEMI,
    DT_COMMA,
    DT_EQ,
    DT_COLON,
    DT_EDGE,
} dt_kind_t;

typedef struct {
    dt_kind_t k;
    dg_str_t s;
    uint32_t line;
} dt_tok_t;

typedef struct {
    const char *p;
    const char *end;
    uint32_t line;
    dt_tok_t tok; /* the current token */
} dt_lex_t;

static void dt_skip(dt_lex_t *lx) {
    for (;;) {
        while (lx->p < lx->end && isspace((unsigned char)*lx->p)) {
            lx->line += *lx->p == '\n';
            lx->p++;
        }
        if (lx->p + SKIP_ONE < lx->end && lx->p[0] == '/' && lx->p[SKIP_ONE] == '/') {
            while (lx->p < lx->end && *lx->p != '\n') {
                lx->p++;
            }
            continue;
        }
        if (lx->p < lx->end && lx->p[0] == '#') {
            while (lx->p < lx->end && *lx->p != '\n') {
                lx->p++;
            }
            continue;
        }
        if (lx->p + SKIP_ONE < lx->end && lx->p[0] == '/' && lx->p[SKIP_ONE] == '*') {
            lx->p += PAIR_LEN;
            while (lx->p + SKIP_ONE < lx->end && !(lx->p[0] == '*' && lx->p[SKIP_ONE] == '/')) {
                lx->line += *lx->p == '\n';
                lx->p++;
            }
            lx->p = lx->p + PAIR_LEN <= lx->end ? lx->p + PAIR_LEN : lx->end;
            continue;
        }
        return;
    }
}

static void dt_next(dt_lex_t *lx) {
    dt_skip(lx);
    lx->tok = (dt_tok_t){DT_END, dg_s(NULL, 0), lx->line};
    if (lx->p >= lx->end) {
        return;
    }
    char c = *lx->p;
    static const char single[] = "{}[];,=:";
    static const dt_kind_t kinds[] = {DT_LBRACE, DT_RBRACE, DT_LBRACK, DT_RBRACK,
                                      DT_SEMI,   DT_COMMA,  DT_EQ,     DT_COLON};
    const char *hit = strchr(single, c);
    if (c && hit) {
        lx->tok.k = kinds[hit - single];
        lx->p++;
        return;
    }
    if (c == '-' && lx->p + SKIP_ONE < lx->end &&
        (lx->p[SKIP_ONE] == '>' || lx->p[SKIP_ONE] == '-')) {
        lx->tok.k = DT_EDGE;
        lx->tok.s = dg_s(lx->p, PAIR_LEN);
        lx->p += PAIR_LEN;
        return;
    }
    if (c == '"') {
        const char *q = lx->p + SKIP_ONE;
        while (q < lx->end && *q != '"') {
            lx->line += *q == '\n';
            q += *q == '\\' && q + SKIP_ONE < lx->end ? PAIR_LEN : SKIP_ONE;
        }
        lx->tok.k = DT_ID;
        lx->tok.s = dg_s(lx->p + SKIP_ONE, (size_t)(q - lx->p) - SKIP_ONE);
        lx->p = q < lx->end ? q + SKIP_ONE : lx->end;
        return;
    }
    if (c == '<') { /* an HTML string */
        int depth = 0;
        const char *q = lx->p;
        for (; q < lx->end; q++) {
            depth += *q == '<';
            depth -= *q == '>';
            lx->line += *q == '\n';
            if (depth == 0) {
                break;
            }
        }
        lx->tok.k = DT_ID;
        lx->tok.s = dg_s(lx->p + SKIP_ONE, (size_t)(q - lx->p) - (q > lx->p ? SKIP_ONE : 0));
        lx->p = q < lx->end ? q + SKIP_ONE : lx->end;
        return;
    }
    const char *q = lx->p;
    while (q < lx->end && (isalnum((unsigned char)*q) || *q == '_' || *q == '.' ||
                           (unsigned char)*q >= 0x80 || (*q == '-' && q == lx->p))) {
        q++;
    }
    if (q == lx->p) {
        lx->p++; /* a stray byte */
        dt_next(lx);
        return;
    }
    lx->tok.k = DT_ID;
    lx->tok.s = dg_s(lx->p, (size_t)(q - lx->p));
    lx->p = q;
}

/* `[a=b, c=d]` lists: the value of `label`, when one is set; url (when given)
 * gets the value of `URL` or `href`. */
static dg_str_t dt_attrs(dt_lex_t *lx, dg_str_t *url) {
    dg_str_t label = dg_s(NULL, 0);
    while (lx->tok.k == DT_LBRACK) {
        dt_next(lx);
        while (lx->tok.k != DT_RBRACK && lx->tok.k != DT_END) {
            if (lx->tok.k == DT_ID) {
                bool is_label = dg_eq(lx->tok.s, "label");
                bool is_url = dg_eq(lx->tok.s, "URL") || dg_eq(lx->tok.s, "href");
                dt_next(lx);
                if (lx->tok.k == DT_EQ) {
                    dt_next(lx);
                    if (lx->tok.k == DT_ID && is_label) {
                        label = lx->tok.s;
                    } else if (lx->tok.k == DT_ID && is_url && url) {
                        *url = lx->tok.s;
                    }
                }
            }
            dt_next(lx);
        }
        dt_next(lx);
    }
    return label;
}

enum { DT_SET_MAX = 64, DT_DEPTH_MAX = 32 };

typedef struct {
    dg_str_t ids[DT_SET_MAX];
    int n;
} dt_set_t;

static void dt_stmts(dg_t *dg, dt_lex_t *lx, bool directed, dt_set_t *all, int depth,
                     dg_str_t *title);

/* An edge operand: a node id (its port dropped) or a subgraph's nodes. */
static bool dt_operand(dg_t *dg, dt_lex_t *lx, bool directed, dt_set_t *out, int depth,
                       dg_str_t *title) {
    out->n = 0;
    if (lx->tok.k == DT_ID && dg_eq(lx->tok.s, "subgraph")) {
        dt_next(lx);
        if (lx->tok.k == DT_ID) {
            dt_next(lx);
        }
    }
    if (lx->tok.k == DT_LBRACE) {
        dt_next(lx);
        dt_stmts(dg, lx, directed, out, depth + SKIP_ONE, title);
        if (lx->tok.k == DT_RBRACE) {
            dt_next(lx);
        }
        return true;
    }
    if (lx->tok.k != DT_ID) {
        return false;
    }
    out->ids[out->n++] = lx->tok.s;
    dt_next(lx);
    while (lx->tok.k == DT_COLON) { /* a port, a compass point */
        dt_next(lx);
        if (lx->tok.k == DT_ID) {
            dt_next(lx);
        }
    }
    return true;
}

static void dt_stmt(dg_t *dg, dt_lex_t *lx, bool directed, dt_set_t *all, int depth,
                    dg_str_t *title) {
    uint32_t line = lx->tok.line;
    if (lx->tok.k == DT_ID &&
        (dg_eq(lx->tok.s, "graph") || dg_eq(lx->tok.s, "node") || dg_eq(lx->tok.s, "edge"))) {
        dt_next(lx);
        (void)dt_attrs(lx, NULL);
        return;
    }
    dt_set_t left;
    if (!dt_operand(dg, lx, directed, &left, depth, title)) {
        dt_next(lx);
        return;
    }
    if (lx->tok.k == DT_EQ) { /* a graph attribute */
        dt_next(lx);
        if (lx->tok.k == DT_ID && left.n && dg_eq(left.ids[0], "label") && depth == 0 &&
            !title->n) {
            *title = lx->tok.s;
        }
        dt_next(lx);
        return;
    }
    if (lx->tok.k != DT_EDGE) {
        dg_str_t url = dg_s(NULL, 0);
        dg_str_t label = dt_attrs(lx, &url);
        for (int i = 0; i < left.n; i++) {
            dg_link(dg, dg_element(dg, left.ids[i], label, "node", line), url,
                    CBM_DOCLINK_DG_DOT_URL, line);
            if (all->n < DT_SET_MAX) {
                all->ids[all->n++] = left.ids[i];
            }
        }
        return;
    }
    /* an edge chain: a -> b -> {c d} [label=x] */
    dt_set_t chain[DT_DEPTH_MAX];
    int nc = 0;
    chain[nc++] = left;
    while (lx->tok.k == DT_EDGE && nc < DT_DEPTH_MAX) {
        dt_next(lx);
        if (!dt_operand(dg, lx, directed, &chain[nc], depth, title)) {
            break;
        }
        nc++;
    }
    dg_str_t label = dt_attrs(lx, NULL);
    for (int k = 0; k < nc; k++) {
        for (int i = 0; i < chain[k].n; i++) {
            dg_element(dg, chain[k].ids[i], dg_s(NULL, 0), "node", line);
            if (all->n < DT_SET_MAX) {
                all->ids[all->n++] = chain[k].ids[i];
            }
        }
    }
    for (int k = 0; k + SKIP_ONE < nc; k++) {
        for (int i = 0; i < chain[k].n; i++) {
            for (int j = 0; j < chain[k + SKIP_ONE].n; j++) {
                dg_arrow(dg, chain[k].ids[i], chain[k + SKIP_ONE].ids[j], label,
                         directed ? "arrow" : "line", line);
            }
        }
    }
}

static void dt_stmts(dg_t *dg, dt_lex_t *lx, bool directed, dt_set_t *all, int depth,
                     dg_str_t *title) {
    if (depth > DT_DEPTH_MAX) {
        return;
    }
    while (lx->tok.k != DT_RBRACE && lx->tok.k != DT_END && !dg->oom) {
        if (lx->tok.k == DT_SEMI || lx->tok.k == DT_COMMA) {
            dt_next(lx);
            continue;
        }
        dt_stmt(dg, lx, directed, all, depth, title);
    }
}

static void dg_dot(dg_t *dg, dg_lines_t *it, dg_str_t *title) {
    dt_lex_t lx = {it->p, it->end, it->line, {DT_END, {NULL, 0}, 0}};
    dt_next(&lx);
    if (lx.tok.k == DT_ID && dg_eq(lx.tok.s, "strict")) {
        dt_next(&lx);
    }
    bool directed = lx.tok.k == DT_ID && dg_eq(lx.tok.s, "digraph");
    snprintf(dg->kind, sizeof(dg->kind), "%s", directed ? "digraph" : "graph");
    dg->element_kind = "node";
    while (lx.tok.k != DT_LBRACE && lx.tok.k != DT_END) {
        dt_next(&lx);
    }
    if (lx.tok.k == DT_END) {
        return;
    }
    dt_next(&lx);
    dt_set_t all = {.n = 0};
    dt_stmts(dg, &lx, directed, &all, 0, title);
}

/* ── D2 ──────────────────────────────────────────────────────────── */

enum { D2_DEPTH_MAX = 16, D2_PATH_MAX = 512, D2_DELIM_MAX = 8 };

/* Keys that set an attribute of the shape they are written in. */
static bool d2_reserved(dg_str_t key) {
    static const char *const words[] = {
        "shape",    "style",      "icon",      "near",  "direction",        "tooltip",
        "link",     "width",      "height",    "class", "classes",          "vars",
        "label",    "constraint", "top",       "left",  "grid-rows",        "grid-columns",
        "grid-gap", "layers",     "scenarios", "steps", "source-arrowhead", "target-arrowhead",
        "filled",   "font-size",  NULL};
    for (int k = 0; words[k]; k++) {
        if (dg_eq(key, words[k])) {
            return true;
        }
    }
    return false;
}

/* The containers a line is written in: the path of ids, and how far it
 * reached at each enclosing level (restored by the closing brace). */
typedef struct {
    char path[D2_PATH_MAX];
    size_t lens[D2_DEPTH_MAX];
    int depth;
    int skip_depth; /* inside a style, vars or connection block; -1 for none */
} d2_scope_t;

/* One level deeper: into the container `id`, or (no id) into a block that
 * keeps the path. */
static void d2_push(d2_scope_t *sc, dg_str_t id) {
    if (sc->depth < D2_DEPTH_MAX) {
        sc->lens[sc->depth] = strlen(sc->path);
        if (id.n) {
            char next[D2_PATH_MAX];
            snprintf(next, sizeof(next), "%.*s", (int)id.n, id.p);
            memcpy(sc->path, next, sizeof(next));
        }
    }
    sc->depth++;
}

static void d2_pop(d2_scope_t *sc) {
    if (sc->depth == 0) {
        return;
    }
    if (sc->skip_depth == sc->depth) {
        sc->skip_depth = -1;
    }
    sc->depth--;
    if (sc->depth < D2_DEPTH_MAX) {
        sc->path[sc->lens[sc->depth]] = '\0';
    }
}

/* The full id of a reference written inside the containers `path`. */
static dg_str_t d2_id(dg_str_t ref, const char *path, char *buf, size_t cap) {
    ref = dg_unquote(ref);
    if (!path[0]) {
        return ref;
    }
    int n = snprintf(buf, cap, "%s.%.*s", path, (int)ref.n, ref.p);
    return n > 0 && (size_t)n < cap ? dg_s(buf, (size_t)n) : ref;
}

/* A line without its `#` comment; a block string (`|`) ends the scan, its
 * text is no comment. */
static dg_str_t d2_strip_comment(dg_str_t ln) {
    bool quoted = false;
    for (size_t i = 0; i < ln.n; i++) {
        quoted = ln.p[i] == '"' ? !quoted : quoted;
        if (quoted) {
            continue;
        }
        if (ln.p[i] == '|') {
            break;
        }
        if (ln.p[i] == '#') {
            return dg_trim(dg_s(ln.p, i));
        }
    }
    return ln;
}

/* The first `:` outside quotes, NULL for none. */
static const char *d2_colon(dg_str_t ln) {
    bool quoted = false;
    for (size_t i = 0; i < ln.n; i++) {
        quoted = ln.p[i] == '"' ? !quoted : quoted;
        if (!quoted && ln.p[i] == ':') {
            return ln.p + i;
        }
    }
    return NULL;
}

/* A block string value (`|md ...|`, `||...||`, `|||...|||`, |`...`|): its
 * text, read on through the lines it spans, and what follows its closing
 * delimiter on that line. False when value opens none. */
static bool d2_block(dg_lines_t *it, dg_str_t value, dg_str_t *text, dg_str_t *after) {
    size_t pipes = 0;
    while (pipes < value.n && value.p[pipes] == '|') {
        pipes++;
    }
    if (pipes == 0 || pipes + PAIR_LEN > D2_DELIM_MAX) {
        return false;
    }
    char close[D2_DELIM_MAX];
    size_t cl = 0;
    size_t open = pipes;
    if (open < value.n && value.p[open] == '`') {
        close[cl++] = '`';
        open++;
    }
    for (size_t i = 0; i < pipes; i++) {
        close[cl++] = '|';
    }
    while (open < value.n && !isspace((unsigned char)value.p[open])) { /* the language tag */
        open++;
    }
    const char *from = value.p + open;
    const char *at = NULL;
    for (const char *q = from; q + cl <= it->end && !at; q++) {
        at = memcmp(q, close, cl) == 0 ? q : NULL;
    }
    at = at ? at : it->end;
    *text = dg_s(from, (size_t)(at - from));
    const char *rest = at + (at < it->end ? cl : 0);
    const char *eol = memchr(rest, '\n', (size_t)(it->end - rest));
    eol = eol ? eol : it->end;
    *after = dg_trim(dg_s(rest, (size_t)(eol - rest)));
    dg_str_t skipped;
    uint32_t l = 0;
    while (it->p <= at && dg_next_line(it, &skipped, &l)) { /* the lines the block spans */
    }
    return true;
}

/* A block string's label: its first line of text, tags and heading marks
 * dropped. */
static size_t d2_block_label(dg_str_t text, char *out, size_t cap) {
    dg_lines_t li = {text.p, text.p + text.n, 0};
    dg_str_t ln;
    uint32_t l = 0;
    while (dg_next_line(&li, &ln, &l)) {
        size_t n = dg_label_text(ln, out, cap);
        size_t k = 0;
        while (k < n && (out[k] == '#' || out[k] == ' ')) {
            k++;
        }
        if (k < n) {
            memmove(out, out + k, n - k + SKIP_ONE);
            return n - k;
        }
    }
    out[0] = '\0';
    return 0;
}

/* A value that is a block string read whole: the label it gives in buf, its
 * text added to the diagram's docstring, and whether a `{` follows it. */
static dg_str_t d2_value(dg_t *dg, dg_lines_t *it, dg_str_t value, char *buf, size_t cap,
                         bool *opens) {
    dg_str_t text;
    dg_str_t after;
    if (!value.n || value.p[0] != '|' || !d2_block(it, value, &text, &after)) {
        return value;
    }
    *opens = after.n && after.p[after.n - SKIP_ONE] == '{';
    size_t n = d2_block_label(text, buf, cap);
    char all[DG_LABEL_MAX];
    size_t an = dg_label_text(text, all, sizeof(all));
    if (an > n) {
        dg_doc_add(dg, all, an);
    }
    return dg_s(buf, n);
}

/* A connection chain `a -> b <- c: label`: true when the line is one. */
static bool d2_connection(dg_t *dg, dg_lines_t *it, dg_str_t ln, const char *path, uint32_t line,
                          bool *opens) {
    static const char *const ops[] = {"<->", "->", "<-", "--", NULL};
    dg_str_t parts[DT_SET_MAX];
    const char *styles[DT_SET_MAX];
    int np = 0;
    const char *colon = NULL;
    bool quoted = false;
    size_t start = 0;
    for (size_t i = 0; i < ln.n && np + SKIP_ONE < DT_SET_MAX; i++) {
        quoted = ln.p[i] == '"' ? !quoted : quoted;
        if (quoted) {
            continue;
        }
        if (ln.p[i] == ':') {
            colon = ln.p + i;
            break;
        }
        for (int k = 0; ops[k]; k++) {
            size_t l = strlen(ops[k]);
            if (i + l <= ln.n && memcmp(ln.p + i, ops[k], l) == 0) {
                parts[np] = dg_trim(dg_s(ln.p + start, i - start));
                styles[np++] = k == 0          ? "bidirectional"
                               : k == 1        ? "arrow"
                               : k == PAIR_LEN ? "<"
                                               : "line";
                start = i + l;
                i += l - SKIP_ONE;
                break;
            }
        }
    }
    if (np == 0) {
        return false;
    }
    size_t end = colon ? (size_t)(colon - ln.p) : ln.n;
    parts[np] = dg_trim(dg_s(ln.p + start, end - start));
    dg_str_t label = colon
                         ? dg_trim(dg_s(colon + SKIP_ONE, (size_t)(ln.p + ln.n - colon) - SKIP_ONE))
                         : dg_s(NULL, 0);
    char buf[DG_LABEL_MAX];
    label = d2_value(dg, it, label, buf, sizeof(buf), opens);
    if (label.n && label.p[label.n - SKIP_ONE] == '{') {
        label = dg_trim(dg_s(label.p, label.n - SKIP_ONE));
    }
    for (int k = 0; k < np; k++) {
        char ba[D2_PATH_MAX];
        char bb[D2_PATH_MAX];
        dg_str_t a = d2_id(parts[k], path, ba, sizeof(ba));
        dg_str_t b = d2_id(parts[k + SKIP_ONE], path, bb, sizeof(bb));
        if (!a.n || !b.n) {
            continue;
        }
        if (strcmp(styles[k], "<") == 0) {
            dg_arrow(dg, b, a, label, "arrow", line);
        } else {
            dg_arrow(dg, a, b, label, styles[k], line);
        }
    }
    return true;
}

/* The first reserved segment of a dotted key: its index, -1 for none. */
static int d2_reserved_segment(dg_str_t key, size_t *seg_start, size_t *seg_len) {
    int index = 0;
    size_t s = 0;
    bool quoted = false;
    for (size_t i = 0; i <= key.n; i++) {
        quoted = i < key.n && key.p[i] == '"' ? !quoted : quoted;
        if (i < key.n && (quoted || key.p[i] != '.')) {
            continue;
        }
        if (d2_reserved(dg_trim(dg_s(key.p + s, i - s)))) {
            *seg_start = s;
            *seg_len = i - s;
            return index;
        }
        index++;
        s = i + SKIP_ONE;
    }
    return -1;
}

/* `key: value` (or `key: value {`) that is no connection. */
static void d2_key(dg_t *dg, d2_scope_t *sc, dg_lines_t *it, dg_str_t ln, uint32_t line, bool opens,
                   dg_str_t *title) {
    const char *colon = d2_colon(ln);
    dg_str_t key = dg_trim(colon ? dg_s(ln.p, (size_t)(colon - ln.p))
                                 : dg_s(ln.p, ln.n - (opens ? SKIP_ONE : 0)));
    dg_str_t value = colon
                         ? dg_trim(dg_s(colon + SKIP_ONE, (size_t)(ln.p + ln.n - colon) - SKIP_ONE))
                         : dg_s(NULL, 0);
    char vbuf[DG_LABEL_MAX];
    value = d2_value(dg, it, value, vbuf, sizeof(vbuf), &opens);
    if (value.n && value.p[value.n - SKIP_ONE] == '{') {
        value = dg_trim(dg_s(value.p, value.n - SKIP_ONE));
    }
    size_t rs = 0;
    size_t rl = 0;
    int r = d2_reserved_segment(key, &rs, &rl);
    if (r >= 0) {
        /* an attribute: `label` names the shape it is set on */
        bool label = dg_eq(dg_trim(dg_s(key.p + rs, rl)), "label") && rs + rl == key.n;
        char buf[D2_PATH_MAX];
        dg_str_t owner = r == 0 ? dg_s(sc->path, strlen(sc->path))
                                : d2_id(dg_s(key.p, rs - SKIP_ONE), sc->path, buf, sizeof(buf));
        bool link = dg_eq(dg_trim(dg_s(key.p + rs, rl)), "link") && rs + rl == key.n;
        if (label && owner.n && value.n) {
            dg_element(dg, owner, value, "shape", line);
        } else if (link && owner.n && value.n) {
            dg_link(dg, dg_element(dg, owner, dg_s(NULL, 0), "shape", line), value,
                    CBM_DOCLINK_DG_D2_LINK, line);
        } else if (label && value.n && r == 0) { /* the board's own label */
            const char *t = dg_dup(dg, value.p, value.n);
            *title = t ? dg_s(t, value.n) : *title;
        }
        if (opens) {
            d2_push(sc, dg_s(NULL, 0));
            sc->skip_depth = sc->depth;
        }
        return;
    }
    char buf[D2_PATH_MAX];
    dg_str_t id = d2_id(key, sc->path, buf, sizeof(buf));
    dg_element(dg, id, value, opens ? "container" : "shape", line);
    if (opens) {
        d2_push(sc, id);
    }
}

static void dg_d2(dg_t *dg, dg_lines_t *it, dg_str_t *title) {
    snprintf(dg->kind, sizeof(dg->kind), "%s", "d2");
    dg->element_kind = "shape";
    d2_scope_t sc;
    memset(&sc, 0, sizeof(sc));
    sc.skip_depth = -1;
    dg_str_t ln;
    uint32_t line = 0;
    while (dg_next_line(it, &ln, &line)) {
        ln = d2_strip_comment(dg_trim(ln));
        if (!ln.n) {
            continue;
        }
        if (dg_eq(ln, "}")) {
            d2_pop(&sc);
            continue;
        }
        bool opens = ln.p[ln.n - SKIP_ONE] == '{';
        if (sc.skip_depth >= 0 || ln.p[0] == '(' || ln.p[0] == '|' || ln.p[0] == '*' ||
            ln.p[0] == '.' || ln.p[0] == '@') {
            if (opens) {
                d2_push(&sc, dg_s(NULL, 0));
            }
            continue;
        }
        if (d2_connection(dg, it, ln, sc.path, line, &opens)) {
            if (opens) {
                d2_push(&sc, dg_s(NULL, 0));
                sc.skip_depth = sc.depth; /* a connection's attribute block */
            }
            continue;
        }
        d2_key(dg, &sc, it, ln, line, opens, title);
    }
}

/* ── XML attributes (draw.io) ────────────────────────────────────── */

/* The value of attribute `name` in the tag text [p, end), entities
 * decoded, into out; false when the tag has none. */
static bool dg_xml_attr(const char *p, const char *end, const char *name, char *out, size_t cap) {
    size_t nl = strlen(name);
    for (const char *q = p; q + nl + PAIR_LEN < end; q++) {
        if (memcmp(q, name, nl) != 0 || q[nl] != '=' ||
            (q[nl + SKIP_ONE] != '"' && q[nl + SKIP_ONE] != '\'') ||
            (q > p && !isspace((unsigned char)q[-SKIP_ONE]))) {
            continue;
        }
        char quote = q[nl + SKIP_ONE];
        const char *v = q + nl + PAIR_LEN;
        size_t w = 0;
        for (; v < end && *v != quote && w + SKIP_ONE < cap; v++) {
            if (*v != '&') {
                out[w++] = *v;
                continue;
            }
            static const struct {
                const char *ent;
                char c;
            } ents[] = {{"&lt;", '<'},    {"&gt;", '>'},  {"&amp;", '&'}, {"&quot;", '"'},
                        {"&apos;", '\''}, {"&#10;", ' '}, {"&#xa;", ' '}, {"&#xA;", ' '},
                        {"&nbsp;", ' '},  {NULL, 0}};
            bool hit = false;
            for (int k = 0; ents[k].ent && !hit; k++) {
                size_t el = strlen(ents[k].ent);
                if (v + el <= end && memcmp(v, ents[k].ent, el) == 0) {
                    out[w++] = ents[k].c;
                    v += el - SKIP_ONE;
                    hit = true;
                }
            }
            if (!hit) {
                out[w++] = '&';
            }
        }
        out[w] = '\0';
        return true;
    }
    out[0] = '\0';
    return false;
}

enum { DG_DRAWIO_CELLS = 4096, DG_ATTR_MAX = 4096 };

/* The text of a draw.io label: its HTML entities once more (an HTML label is
 * escaped twice), its tags dropped. */
static size_t dg_drawio_text(const char *html, char *out, size_t cap) {
    char tmp[DG_ATTR_MAX];
    size_t w = 0;
    for (const char *v = html; *v && w + SKIP_ONE < sizeof(tmp); v++) {
        static const struct {
            const char *ent;
            char c;
        } ents[] = {{"&lt;", '<'},   {"&gt;", '>'},   {"&amp;", '&'},
                    {"&quot;", '"'}, {"&nbsp;", ' '}, {NULL, 0}};
        bool hit = false;
        for (int k = 0; ents[k].ent && !hit && *v == '&'; k++) {
            size_t el = strlen(ents[k].ent);
            if (strncmp(v, ents[k].ent, el) == 0) {
                tmp[w++] = ents[k].c;
                v += el - SKIP_ONE;
                hit = true;
            }
        }
        if (!hit) {
            tmp[w++] = *v;
        }
    }
    tmp[w] = '\0';
    /* a label cut short can end inside a tag: drop it, not show it */
    char *lt = strrchr(tmp, '<');
    if (lt && !strchr(lt, '>') &&
        (isalpha((unsigned char)lt[SKIP_ONE]) || lt[SKIP_ONE] == '/' || lt[SKIP_ONE] == '!')) {
        *lt = '\0';
        w = (size_t)(lt - tmp);
    }
    return dg_label_text(dg_s(tmp, w), out, cap);
}

typedef struct {
    char id[DG_ID_MAX];
    char label[DG_LABEL_MAX];
    char source[DG_ID_MAX];
    char target[DG_ID_MAX];
    char parent[DG_ID_MAX];
    bool vertex;
    bool edge;
    bool text;       /* a free text box */
    bool edge_label; /* the label of the edge `parent` */
    bool dashed;
    bool no_arrow;
    char kind[DG_KIND_MAX];
    const char *link; /* the cell's `link` attribute, NULL for none */
} dg_cell_t;

/* The cell a `<mxCell ...>` tag (inside an `<object>` wrapper: `wrap`) says. */
static void dg_drawio_cell(CBMArena *a, const char *tag, const char *tag_end, const char *wrap,
                           const char *wrap_end, dg_cell_t *c) {
    char buf[DG_ATTR_MAX];
    memset(c, 0, sizeof(*c));
    if (dg_xml_attr(wrap ? wrap : tag, wrap ? wrap_end : tag_end, "link", buf, sizeof(buf)) &&
        buf[0]) {
        c->link = cbm_arena_strdup(a, buf);
    }
    const char *idp = wrap ? wrap : tag;
    const char *ide = wrap ? wrap_end : tag_end;
    dg_xml_attr(idp, ide, "id", c->id, sizeof(c->id));
    if (dg_xml_attr(wrap ? wrap : tag, wrap ? wrap_end : tag_end, wrap ? "label" : "value", buf,
                    sizeof(buf))) {
        dg_drawio_text(buf, c->label, sizeof(c->label));
    }
    c->vertex = dg_xml_attr(tag, tag_end, "vertex", buf, sizeof(buf)) && buf[0] == '1';
    c->edge = dg_xml_attr(tag, tag_end, "edge", buf, sizeof(buf)) && buf[0] == '1';
    dg_xml_attr(tag, tag_end, "source", c->source, sizeof(c->source));
    dg_xml_attr(tag, tag_end, "target", c->target, sizeof(c->target));
    dg_xml_attr(tag, tag_end, "parent", c->parent, sizeof(c->parent));
    if (dg_xml_attr(tag, tag_end, "style", buf, sizeof(buf))) {
        c->text = strncmp(buf, "text;", CBM_SZ_5) == 0 || strncmp(buf, "text", CBM_SZ_4) == 0;
        c->edge_label = strstr(buf, "edgeLabel") != NULL;
        c->dashed = strstr(buf, "dashed=1") != NULL;
        c->no_arrow = strstr(buf, "endArrow=none") != NULL;
        const char *shape = strstr(buf, "shape=");
        const char *k = shape ? shape + CBM_SZ_6 : buf;
        size_t kl = 0;
        while (k[kl] && k[kl] != ';' && k[kl] != '=' && kl + SKIP_ONE < sizeof(c->kind)) {
            kl++;
        }
        if (k[kl] == '=' && !shape) {
            kl = 0; /* the first entry is a setting, not a shape */
        }
        memcpy(c->kind, k, kl);
        c->kind[kl] = '\0';
    }
    if (!c->kind[0]) {
        snprintf(c->kind, sizeof(c->kind), "%s", "shape");
    }
}

/* The elements and arrows of one draw.io page's mxGraphModel. */
static void dg_drawio(dg_t *dg, dg_lines_t *it, dg_str_t *title) {
    (void)title;
    snprintf(dg->kind, sizeof(dg->kind), "%s", "drawio");
    dg->element_kind = "shape";
    const char *p = it->p;
    const char *end = it->end;
    dg_cell_t *cells =
        (dg_cell_t *)cbm_arena_alloc(dg->ctx->arena, DG_DRAWIO_CELLS * sizeof(dg_cell_t));
    if (!cells) {
        dg->oom = true;
        return;
    }
    int n = 0;
    for (const char *q = p; q < end && n < DG_DRAWIO_CELLS;) {
        const char *lt = memchr(q, '<', (size_t)(end - q));
        if (!lt) {
            break;
        }
        const char *gt = memchr(lt, '>', (size_t)(end - lt));
        if (!gt) {
            break;
        }
        bool object = (size_t)(end - lt) > CBM_SZ_8 && (memcmp(lt, "<object ", CBM_SZ_8) == 0 ||
                                                        memcmp(lt, "<UserObject ", 12) == 0);
        if (object) {
            const char *cell = strstr(gt, "<mxCell");
            if (cell && cell < end) {
                const char *cend = memchr(cell, '>', (size_t)(end - cell));
                if (cend) {
                    dg_drawio_cell(dg->ctx->arena, cell, cend, lt, gt, &cells[n++]);
                    q = cend + SKIP_ONE;
                    continue;
                }
            }
        } else if ((size_t)(end - lt) > CBM_SZ_7 && memcmp(lt, "<mxCell", CBM_SZ_7) == 0) {
            dg_drawio_cell(dg->ctx->arena, lt, gt, NULL, NULL, &cells[n++]);
        }
        q = gt + SKIP_ONE;
    }
    uint32_t line = dg->line0;
    for (int i = 0; i < n; i++) { /* the shapes with text */
        dg_cell_t *c = &cells[i];
        if (c->vertex && !c->edge_label && c->label[0]) {
            if (c->text) {
                dg_doc_add(dg, c->label, strlen(c->label));
            } else {
                const char *qn = dg_element(dg, dg_s(c->id, strlen(c->id)),
                                            dg_s(c->label, strlen(c->label)), c->kind, line);
                dg_link(dg, qn, dg_s(c->link, c->link ? strlen(c->link) : 0),
                        CBM_DOCLINK_DG_DRAWIO_LINK, line);
            }
        }
    }
    for (int i = 0; i < n; i++) { /* the connections */
        dg_cell_t *c = &cells[i];
        if (!c->edge || !c->source[0] || !c->target[0]) {
            continue;
        }
        const char *label = c->label;
        for (int k = 0; k < n && !label[0]; k++) {
            if (cells[k].edge_label && strcmp(cells[k].parent, c->id) == 0) {
                label = cells[k].label;
            }
        }
        if (!cbm_ht_get(dg->ids, c->source) || !cbm_ht_get(dg->ids, c->target)) {
            continue; /* an end without text names nothing */
        }
        dg_arrow(dg, dg_s(c->source, strlen(c->source)), dg_s(c->target, strlen(c->target)),
                 dg_s(label, strlen(label)),
                 c->dashed     ? "dotted"
                 : c->no_arrow ? "line"
                               : "arrow",
                 line);
    }
}

/* ── Excalidraw ──────────────────────────────────────────────────── */

enum { DG_EX_MARGIN = 8 }; /* px: how near a loose arrow end comes to the shape it meets */

/* A shape of the scene: its box, and the label it gets. */
typedef struct {
    const char *id;
    const char *type;
    double x0;
    double y0;
    double x1;
    double y1;
    double area;
    const char *label;
    double label_x; /* a loose label's place: the topmost text inside wins */
    double label_y;
    const char *link; /* the shape's `link`, NULL for none */
    bool fixed;       /* bound text or a frame's name: no loose text replaces it */
    bool region;      /* a box that can hold loose text and meet loose arrow ends */
    bool frame;       /* a region around every other shape, the drawing's frame: loose
                       * arrow ends meet it at its border only */
} ex_shape_t;

static const char *dg_json_str(yyjson_val *obj, const char *key) {
    yyjson_val *v = yyjson_obj_get(obj, key);
    return yyjson_is_str(v) ? yyjson_get_str(v) : NULL;
}

static double ex_num(yyjson_val *obj, const char *key) {
    yyjson_val *v = yyjson_obj_get(obj, key);
    return yyjson_is_num(v) ? yyjson_get_num(v) : 0.0;
}

static bool ex_live(yyjson_val *e) {
    return !yyjson_get_bool(yyjson_obj_get(e, "isDeleted"));
}

static bool ex_is(const char *type, const char *const *kinds) {
    for (int k = 0; kinds[k]; k++) {
        if (strcmp(type, kinds[k]) == 0) {
            return true;
        }
    }
    return false;
}

static const char *const EX_REGIONS[] = {"rectangle",  "ellipse",    "diamond", "frame",
                                         "magicframe", "embeddable", "iframe",  NULL};
static const char *const EX_NOT_SHAPES[] = {"text", "arrow", "line", "freedraw", "selection", NULL};

static int ex_by_area(const void *a, const void *b) {
    const ex_shape_t *x = (const ex_shape_t *)a;
    const ex_shape_t *y = (const ex_shape_t *)b;
    if (x->area != y->area) {
        return x->area < y->area ? -1 : SKIP_ONE;
    }
    return strcmp(x->id, y->id);
}

/* The smallest region whose box, grown by margin, holds (x, y): -1 for none.
 * The shapes are sorted by area, so the first hit is the smallest. An arrow
 * end (labelled) deep inside the drawing's frame points at something in the
 * drawing, not at the frame: none. */
static int ex_region_at(const ex_shape_t *s, int n, double x, double y, double margin,
                        bool labelled) {
    for (int i = 0; i < n; i++) {
        if (!s[i].region || (labelled && !s[i].label)) {
            continue;
        }
        if (x >= s[i].x0 - margin && x <= s[i].x1 + margin && y >= s[i].y0 - margin &&
            y <= s[i].y1 + margin) {
            bool deep = x > s[i].x0 + margin && x < s[i].x1 - margin && y > s[i].y0 + margin &&
                        y < s[i].y1 - margin;
            return labelled && s[i].frame && deep ? -1 : i;
        }
    }
    return -1;
}

/* A loose text: the label of the smallest region it sits in (the topmost
 * text of several), else the diagram's docstring. */
static void ex_loose_text(dg_t *dg, ex_shape_t *s, int n, yyjson_val *e, const char *text) {
    double x = ex_num(e, "x");
    double y = ex_num(e, "y");
    int i = ex_region_at(s, n, x + ex_num(e, "width") / PAIR_LEN,
                         y + ex_num(e, "height") / PAIR_LEN, 0.0, false);
    if (i < 0 || s[i].fixed) {
        dg_doc_add(dg, text, strlen(text));
        return;
    }
    if (s[i].label && (s[i].label_y < y || (s[i].label_y == y && s[i].label_x <= x))) {
        dg_doc_add(dg, text, strlen(text));
        return;
    }
    if (s[i].label) {
        dg_doc_add(dg, s[i].label, strlen(s[i].label));
    }
    s[i].label = text;
    s[i].label_x = x;
    s[i].label_y = y;
}

/* The end bound to free text: the shape that text names, else the text as an
 * element of its own. */
static const char *ex_text_end(dg_t *dg, const ex_shape_t *s, int n, CBMHashTable *texts,
                               const char *id) {
    const char *text = (const char *)cbm_ht_get(texts, id);
    if (!text) {
        return NULL;
    }
    for (int i = 0; i < n; i++) {
        if (s[i].label == text) {
            return s[i].id;
        }
    }
    return dg_element(dg, dg_s(id, strlen(id)), dg_s(text, strlen(text)), "text", dg->line0) ? id
                                                                                             : NULL;
}

/* An arrow end: the shape (or free text) it is bound to, else (arrows only)
 * the smallest labelled region the point meets. */
static const char *ex_end(dg_t *dg, const ex_shape_t *s, int n, CBMHashTable *texts, yyjson_val *e,
                          const char *binding, bool first, bool loose_ok) {
    const char *bound = dg_json_str(yyjson_obj_get(e, binding), "elementId");
    if (bound) {
        return cbm_ht_get(dg->ids, bound) ? bound : ex_text_end(dg, s, n, texts, bound);
    }
    yyjson_val *pts = yyjson_obj_get(e, "points");
    yyjson_val *pt = first ? yyjson_arr_get_first(pts) : yyjson_arr_get_last(pts);
    if (!loose_ok || !yyjson_is_arr(pt)) {
        return NULL;
    }
    double x = ex_num(e, "x") + yyjson_get_num(yyjson_arr_get(pt, 0));
    double y = ex_num(e, "y") + yyjson_get_num(yyjson_arr_get(pt, SKIP_ONE));
    int i = ex_region_at(s, n, x, y, DG_EX_MARGIN, true);
    return i >= 0 ? s[i].id : NULL;
}

static void ex_arrows(dg_t *dg, const ex_shape_t *s, int n, yyjson_val *els, CBMHashTable *labels,
                      CBMHashTable *texts) {
    size_t i;
    size_t max;
    yyjson_val *e;
    yyjson_arr_foreach(els, i, max, e) {
        const char *type = dg_json_str(e, "type");
        const char *id = dg_json_str(e, "id");
        if (!type || !id || !ex_live(e) ||
            (strcmp(type, "arrow") != 0 && strcmp(type, "line") != 0)) {
            continue;
        }
        bool arrow = strcmp(type, "arrow") == 0;
        const char *from = ex_end(dg, s, n, texts, e, "startBinding", true, arrow);
        const char *to = ex_end(dg, s, n, texts, e, "endBinding", false, arrow);
        bool loose = !dg_json_str(yyjson_obj_get(e, "startBinding"), "elementId") ||
                     !dg_json_str(yyjson_obj_get(e, "endBinding"), "elementId");
        if (!from || !to || (loose && strcmp(from, to) == 0)) {
            continue; /* unbound, bound to a shape without text, or a loose loop in one box */
        }
        const char *label = (const char *)cbm_ht_get(labels, id);
        dg_arrow(dg, dg_s(from, strlen(from)), dg_s(to, strlen(to)),
                 dg_s(label ? label : "", label ? strlen(label) : 0), arrow ? "arrow" : "line",
                 dg->line0);
    }
}

static void dg_excalidraw(dg_t *dg, dg_lines_t *it, dg_str_t *title) {
    (void)title;
    snprintf(dg->kind, sizeof(dg->kind), "%s", "excalidraw");
    dg->element_kind = "shape";
    yyjson_doc *doc = yyjson_read(it->p, (size_t)(it->end - it->p), 0);
    yyjson_val *els = doc ? yyjson_obj_get(yyjson_doc_get_root(doc), "elements") : NULL;
    size_t total = yyjson_arr_size(els);
    CBMHashTable *labels =
        yyjson_is_arr(els) ? cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_64) : NULL;
    ex_shape_t *s = labels && total
                        ? (ex_shape_t *)cbm_arena_alloc(dg->ctx->arena, total * sizeof(ex_shape_t))
                        : NULL;
    CBMHashTable *texts = s ? cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_64) : NULL;
    if (!texts) {
        cbm_ht_free(labels);
        yyjson_doc_free(doc);
        return;
    }
    size_t i;
    size_t max;
    yyjson_val *e;
    yyjson_arr_foreach(els, i, max, e) { /* bound text: the label of its container */
        const char *text = dg_json_str(e, "originalText");
        text = text ? text : dg_json_str(e, "text");
        const char *type = dg_json_str(e, "type");
        const char *container = dg_json_str(e, "containerId");
        const char *id = dg_json_str(e, "id");
        if (ex_live(e) && type && strcmp(type, "text") == 0 && text && container) {
            cbm_ht_set(labels, container, (void *)text);
        } else if (ex_live(e) && type && strcmp(type, "text") == 0 && text && id) {
            cbm_ht_set(texts, id, (void *)text); /* free text an arrow may be bound to */
        }
    }
    int n = 0;
    yyjson_arr_foreach(els, i, max, e) { /* the shapes */
        const char *type = dg_json_str(e, "type");
        const char *id = dg_json_str(e, "id");
        if (!type || !id || !ex_live(e) || ex_is(type, EX_NOT_SHAPES)) {
            continue;
        }
        ex_shape_t *sh = &s[n++];
        memset(sh, 0, sizeof(*sh));
        sh->id = id;
        sh->type = type;
        sh->x0 = ex_num(e, "x");
        sh->y0 = ex_num(e, "y");
        sh->x1 = sh->x0 + ex_num(e, "width");
        sh->y1 = sh->y0 + ex_num(e, "height");
        sh->area = (sh->x1 - sh->x0) * (sh->y1 - sh->y0);
        sh->region = ex_is(type, EX_REGIONS);
        sh->label = (const char *)cbm_ht_get(labels, id);
        sh->label = sh->label ? sh->label : dg_json_str(e, "name"); /* a frame's name */
        sh->fixed = sh->label != NULL;
        sh->link = dg_json_str(e, "link");
    }
    qsort(s, (size_t)n, sizeof(ex_shape_t), ex_by_area);
    yyjson_arr_foreach(els, i, max, e) { /* loose text: in a shape, or the docstring */
        const char *type = dg_json_str(e, "type");
        const char *text = dg_json_str(e, "originalText");
        text = text ? text : dg_json_str(e, "text");
        if (ex_live(e) && type && strcmp(type, "text") == 0 && text &&
            !dg_json_str(e, "containerId")) {
            ex_loose_text(dg, s, n, e, text);
        }
    }
    if (n > SKIP_ONE && s[n - SKIP_ONE].region) { /* only the largest box can frame the rest */
        ex_shape_t *f = &s[n - SKIP_ONE];
        bool all = true;
        for (int j = 0; j < n - SKIP_ONE && all; j++) {
            all = s[j].x0 >= f->x0 && s[j].x1 <= f->x1 && s[j].y0 >= f->y0 && s[j].y1 <= f->y1;
        }
        f->frame = all;
    }
    for (int k = 0; k < n; k++) {
        if (s[k].label) {
            const char *qn = dg_element(dg, dg_s(s[k].id, strlen(s[k].id)),
                                        dg_s(s[k].label, strlen(s[k].label)), s[k].type, dg->line0);
            dg_link(dg, qn, dg_s(s[k].link, s[k].link ? strlen(s[k].link) : 0),
                    CBM_DOCLINK_DG_EXCALIDRAW_LINK, dg->line0);
        }
    }
    ex_arrows(dg, s, n, els, labels, texts);
    cbm_ht_free(texts);
    cbm_ht_free(labels);
    yyjson_doc_free(doc);
}

/* ── One diagram ─────────────────────────────────────────────────── */

typedef struct {
    const char *format;
    const char *title;     /* a draw.io page's name, NULL for none */
    const char *fallback;  /* the name when the text gives none: a diagram file's */
    const char *parent_qn; /* the Section's, NULL: the file */
    int ordinal;
    const char *text;
    size_t len;
    uint32_t first_line; /* the text's first line in the file */
    uint32_t start_line; /* the block's lines */
    uint32_t end_line;
} dg_source_t;

static void dg_build(CBMExtractCtx *ctx, const dg_source_t *src) {
    dg_t dg;
    memset(&dg, 0, sizeof(dg));
    dg.ctx = ctx;
    dg.format = src->format;
    const char *parent = src->parent_qn ? src->parent_qn : ctx->module_qn;
    if (!parent) {
        return;
    }
    dg.qn = cbm_arena_sprintf(ctx->arena, "%s#diagram-%d", parent, src->ordinal);
    dg.ids = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_32);
    if (!dg.qn || !dg.ids) {
        cbm_ht_free(dg.ids);
        return;
    }
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.label = "Diagram";
    def.qualified_name = dg.qn;
    def.file_path = ctx->rel_path;
    def.start_line = src->start_line;
    def.end_line = src->end_line;
    def.parent_class = src->parent_qn;
    def.name = src->format;
    cbm_defs_push(&ctx->result->defs, ctx->arena, def);
    dg.def_index = ctx->result->defs.count - SKIP_ONE;
    dg.line0 = src->first_line;
    dg_lines_t it = {src->text, src->text + src->len, src->first_line};
    dg_str_t title = src->title ? dg_s(src->title, strlen(src->title)) : dg_s(NULL, 0);
    if (strcmp(src->format, "mermaid") == 0) {
        dg_mermaid(&dg, &it, &title);
    } else if (strcmp(src->format, "plantuml") == 0) {
        dg_plantuml(&dg, &it, &title);
    } else if (strcmp(src->format, "dot") == 0) {
        dg_dot(&dg, &it, &title);
    } else if (strcmp(src->format, "d2") == 0) {
        dg_d2(&dg, &it, &title);
    } else if (strcmp(src->format, "drawio") == 0) {
        dg_drawio(&dg, &it, &title);
    } else if (strcmp(src->format, "excalidraw") == 0) {
        dg_excalidraw(&dg, &it, &title);
    }
    cbm_ht_free(dg.ids);
    CBMDefinition *d = &ctx->result->defs.items[dg.def_index];
    char tbuf[DG_LABEL_MAX];
    size_t tl = dg_label_text(title, tbuf, sizeof(tbuf));
    d->name = tl              ? cbm_arena_strndup(ctx->arena, tbuf, tl)
              : src->fallback ? src->fallback
                              : cbm_arena_sprintf(ctx->arena, "%s %s", src->format,
                                                  dg.kind[0] ? dg.kind : "diagram");
    d->docstring = dg.doc_len ? cbm_arena_strndup(ctx->arena, dg.doc, dg.doc_len) : NULL;
    const char *kind = dg.kind[0] ? cbm_arena_strdup(ctx->arena, dg.kind) : "unknown";
    const char **kv = (const char **)cbm_arena_alloc(ctx->arena, CBM_SZ_5 * sizeof(char *));
    if (kv) {
        kv[0] = "format";
        kv[SKIP_ONE] = src->format;
        kv[PAIR_LEN] = "diagram_kind";
        kv[CBM_SZ_3] = kind;
        kv[CBM_SZ_4] = NULL;
        d->extra_props = kv;
    }
}

/* ── Sources ─────────────────────────────────────────────────────── */

/* The diagram format an info string names, NULL for none. */
static const char *dg_format_of(const char *s, size_t n) {
    static const struct {
        const char *word;
        const char *format;
    } names[] = {
        {"mermaid", "mermaid"}, {"plantuml", "plantuml"}, {"puml", "plantuml"}, {"uml", "plantuml"},
        {"dot", "dot"},         {"graphviz", "dot"},      {"d2", "d2"},         {NULL, NULL}};
    for (int k = 0; names[k].word; k++) {
        size_t l = strlen(names[k].word);
        bool same = n == l;
        for (size_t i = 0; same && i < l; i++) {
            same = tolower((unsigned char)s[i]) == names[k].word[i];
        }
        if (same) {
            return names[k].format;
        }
    }
    return NULL;
}

/* The Section a line of the file is written under: the last heading at or
 * before it, NULL above the first. */
static const char *dg_section_at(const CBMExtractCtx *ctx, uint32_t line) {
    const char *qn = NULL;
    uint32_t best = 0;
    const CBMDefArray *defs = &ctx->result->defs;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (d->label && strcmp(d->label, "Section") == 0 && d->start_line <= line &&
            d->start_line >= best) {
            best = d->start_line;
            qn = d->qualified_name;
        }
    }
    return qn;
}

typedef struct {
    const char *parent;
    int count;
} dg_ordinal_t;

enum { DG_PARENTS_MAX = 512 };

/* The next ordinal of a parent's diagrams. */
static int dg_next_ordinal(dg_ordinal_t *ords, int *n, const char *parent) {
    for (int i = 0; i < *n; i++) {
        if (ords[i].parent == parent) {
            return ++ords[i].count;
        }
    }
    if (*n < DG_PARENTS_MAX) {
        ords[(*n)++] = (dg_ordinal_t){parent, SKIP_ONE};
    }
    return SKIP_ONE;
}

static bool dg_kind(TSNode n, const char *k) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), k) == 0;
}

/* The diagram format a Markdown fenced block's info string names, NULL for
 * none; *content is the block's text. */
static const char *dg_fence_format(const char *source, TSNode block, TSNode *content) {
    TSNode info = {0};
    *content = (TSNode){0};
    uint32_t nc = ts_node_named_child_count(block);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(block, i);
        info = dg_kind(c, "info_string") ? c : info;
        *content = dg_kind(c, "code_fence_content") ? c : *content;
    }
    if (ts_node_is_null(info) || ts_node_is_null(*content)) {
        return NULL;
    }
    TSNode lang = ts_node_named_child(info, 0);
    TSNode word = dg_kind(lang, "language") ? lang : info;
    uint32_t wb = ts_node_start_byte(word);
    uint32_t we = ts_node_end_byte(word);
    return dg_format_of(source + wb, we - wb);
}

/* A fenced block of a Markdown file: a diagram when its info string says so. */
static void dg_markdown_fence(CBMExtractCtx *ctx, TSNode block, dg_ordinal_t *ords, int *nords) {
    TSNode content;
    const char *format = dg_fence_format(ctx->source, block, &content);
    if (!format) {
        return;
    }
    uint32_t start = ts_node_start_point(block).row + SKIP_ONE;
    const char *parent = dg_section_at(ctx, start);
    uint32_t cb = ts_node_start_byte(content);
    dg_source_t src = {
        .format = format,
        .parent_qn = parent,
        .ordinal = dg_next_ordinal(ords, nords, parent),
        .text = ctx->source + cb,
        .len = ts_node_end_byte(content) - cb,
        .first_line = ts_node_start_point(content).row + SKIP_ONE,
        .start_line = start,
        .end_line = ts_node_end_point(block).row + SKIP_ONE,
    };
    dg_build(ctx, &src);
}

static void dg_markdown(CBMExtractCtx *ctx) {
    dg_ordinal_t *ords =
        (dg_ordinal_t *)cbm_arena_alloc(ctx->arena, DG_PARENTS_MAX * sizeof(dg_ordinal_t));
    if (!ords) {
        return;
    }
    int nords = 0;
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    for (bool more = true; more;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        if (dg_kind(n, "fenced_code_block")) {
            dg_markdown_fence(ctx, n, ords, &nords);
        } else if (ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
}

/* ── RST directives and AsciiDoc blocks ──────────────────────────── */

/* The diagram format a directive (RST) or block style (AsciiDoc) names; for
 * Sphinx's `digraph` / `graph` also the graph keyword its body goes inside. */
static const char *dg_block_format(dg_str_t word, const char **wrap) {
    static const struct {
        const char *word;
        const char *format;
        const char *wrap;
    } names[] = {
        {"mermaid", "mermaid", NULL}, {"uml", "plantuml", NULL}, {"plantuml", "plantuml", NULL},
        {"graphviz", "dot", NULL},    {"dot", "dot", NULL},      {"digraph", "dot", "digraph"},
        {"graph", "dot", "graph"},    {"d2", "d2", NULL},        {NULL, NULL, NULL}};
    *wrap = NULL;
    for (int k = 0; names[k].word; k++) {
        if (dg_eq(word, names[k].word)) {
            *wrap = names[k].wrap;
            return names[k].format;
        }
    }
    return NULL;
}

static dg_str_t dg_node_text(const CBMExtractCtx *ctx, TSNode n) {
    uint32_t b = ts_node_start_byte(n);
    return dg_s(ctx->source + b, ts_node_end_byte(n) - b);
}

/* The first descendant of kind k, a null node for none. */
static TSNode dg_find(TSNode n, const char *k) {
    uint32_t nc = ts_node_named_child_count(n);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(n, i);
        if (dg_kind(c, k)) {
            return c;
        }
        TSNode d = dg_find(c, k);
        if (!ts_node_is_null(d)) {
            return d;
        }
    }
    return (TSNode){0};
}

/* A directive's `:caption:` option, NULL for none. */
static const char *dg_rst_caption(const CBMExtractCtx *ctx, TSNode options) {
    uint32_t nc = ts_node_is_null(options) ? 0 : ts_node_named_child_count(options);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode f = ts_node_named_child(options, i);
        TSNode name = dg_find(f, "field_name");
        TSNode body = dg_find(f, "field_body");
        dg_str_t key = ts_node_is_null(name) ? dg_s("", 0) : dg_trim(dg_node_text(ctx, name));
        while (key.n && key.p[0] == ':') { /* `:caption:` or `caption` */
            key = dg_s(key.p + SKIP_ONE, key.n - SKIP_ONE);
        }
        while (key.n && key.p[key.n - SKIP_ONE] == ':') {
            key.n--;
        }
        if (!ts_node_is_null(body) && dg_eq(key, "caption")) {
            dg_str_t v = dg_trim(dg_node_text(ctx, body));
            return cbm_arena_strndup(ctx->arena, v.p, v.n);
        }
    }
    return NULL;
}

/* `.. mermaid::`, `.. uml::`, `.. graphviz::`, `.. digraph:: name`: a
 * diagram when the directive holds its text (a file argument is the file,
 * indexed as itself). */
static void dg_rst_directive(CBMExtractCtx *ctx, TSNode dir, dg_ordinal_t *ords, int *nords) {
    TSNode type = dg_find(dir, "type");
    const char *wrap = NULL;
    const char *format =
        ts_node_is_null(type) ? NULL : dg_block_format(dg_trim(dg_node_text(ctx, type)), &wrap);
    TSNode content = format ? dg_find(dir, "content") : (TSNode){0};
    if (ts_node_is_null(content)) {
        return;
    }
    dg_str_t text = dg_node_text(ctx, content);
    uint32_t first = ts_node_start_point(content).row + SKIP_ONE;
    if (wrap) { /* Sphinx writes the statements only */
        TSNode args = dg_find(dir, "arguments");
        dg_str_t name = ts_node_is_null(args) ? dg_s("", 0) : dg_trim(dg_node_text(ctx, args));
        char *w = cbm_arena_sprintf(ctx->arena, "%s \"%.*s\" {\n%.*s\n}\n", wrap, (int)name.n,
                                    name.p, (int)text.n, text.p);
        if (!w) {
            return;
        }
        text = dg_s(w, strlen(w));
        first--;
    }
    uint32_t start = ts_node_start_point(dir).row + SKIP_ONE;
    const char *parent = dg_section_at(ctx, start);
    dg_source_t src = {
        .format = format,
        .title = dg_rst_caption(ctx, dg_find(dir, "options")),
        .parent_qn = parent,
        .ordinal = dg_next_ordinal(ords, nords, parent),
        .text = text.p,
        .len = text.n,
        .first_line = first,
        .start_line = start,
        .end_line = ts_node_end_point(dir).row + SKIP_ONE,
    };
    dg_build(ctx, &src);
}

static void dg_rst(CBMExtractCtx *ctx) {
    dg_ordinal_t *ords =
        (dg_ordinal_t *)cbm_arena_alloc(ctx->arena, DG_PARENTS_MAX * sizeof(dg_ordinal_t));
    if (!ords) {
        return;
    }
    int nords = 0;
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    for (bool more = true; more;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        if (dg_kind(n, "directive")) {
            dg_rst_directive(ctx, n, ords, &nords);
        } else if (ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
}

/* An AsciiDoc block attribute line `[plantuml, target=x]` or
 * `[source,mermaid]`: the diagram format it names, NULL for none. */
static const char *dg_adoc_style(dg_str_t ln) {
    if (ln.n < CBM_SZ_3 || ln.p[0] != '[' || ln.p[ln.n - SKIP_ONE] != ']' ||
        ln.p[SKIP_ONE] == '[') {
        return NULL;
    }
    dg_str_t attrs = dg_s(ln.p + SKIP_ONE, ln.n - PAIR_LEN);
    const char *comma = memchr(attrs.p, ',', attrs.n);
    dg_str_t style = dg_trim(dg_s(attrs.p, comma ? (size_t)(comma - attrs.p) : attrs.n));
    if (dg_eq(style, "source") && comma) { /* a listing in a diagram language */
        dg_str_t rest = dg_s(comma + SKIP_ONE, (size_t)(attrs.p + attrs.n - comma) - SKIP_ONE);
        const char *c2 = memchr(rest.p, ',', rest.n);
        style = dg_trim(dg_s(rest.p, c2 ? (size_t)(c2 - rest.p) : rest.n));
    }
    const char *wrap = NULL;
    const char *format = dg_block_format(style, &wrap);
    return wrap ? NULL : format;
}

/* `----` or `....` (four or more): a listing or literal block delimiter. */
static bool dg_adoc_delimiter(dg_str_t ln) {
    if (ln.n < CBM_SZ_4 || (ln.p[0] != '-' && ln.p[0] != '.')) {
        return false;
    }
    for (size_t i = SKIP_ONE; i < ln.n; i++) {
        if (ln.p[i] != ln.p[0]) {
            return false;
        }
    }
    return true;
}

/* AsciiDoc diagram blocks (Asciidoctor Diagram): the delimited block after a
 * `[mermaid]`, `[plantuml]`, `[graphviz]` or `[d2]` line, titled by the
 * `.Title` line before it. */
static void dg_asciidoc(CBMExtractCtx *ctx) {
    dg_ordinal_t *ords =
        (dg_ordinal_t *)cbm_arena_alloc(ctx->arena, DG_PARENTS_MAX * sizeof(dg_ordinal_t));
    if (!ords) {
        return;
    }
    int nords = 0;
    dg_lines_t it = {ctx->source, ctx->source + ctx->source_len, SKIP_ONE};
    dg_str_t title = dg_s(NULL, 0);
    dg_str_t ln;
    uint32_t line = 0;
    while (dg_next_line(&it, &ln, &line)) {
        dg_str_t t = dg_trim(ln);
        if (t.n > SKIP_ONE && t.p[0] == '.' && t.p[SKIP_ONE] != '.' &&
            !isspace((unsigned char)t.p[SKIP_ONE])) {
            title = dg_s(t.p + SKIP_ONE, t.n - SKIP_ONE);
            continue;
        }
        const char *format = dg_adoc_style(t);
        dg_str_t delim;
        uint32_t attr_line = line;
        if (!format || !dg_next_line(&it, &delim, &line) || !dg_adoc_delimiter(dg_trim(delim))) {
            title = t.n && t.p[0] == '[' ? title : dg_s(NULL, 0); /* attribute lines stack */
            continue;
        }
        delim = dg_trim(delim);
        const char *body = it.p;
        uint32_t first = it.line;
        const char *stop = it.end;
        uint32_t last = line;
        while (dg_next_line(&it, &ln, &line)) {
            dg_str_t c = dg_trim(ln);
            if (c.n == delim.n && memcmp(c.p, delim.p, c.n) == 0) {
                stop = ln.p;
                last = line;
                break;
            }
        }
        const char *parent = dg_section_at(ctx, attr_line);
        dg_source_t src = {
            .format = format,
            .title = title.n ? cbm_arena_strndup(ctx->arena, title.p, title.n) : NULL,
            .parent_qn = parent,
            .ordinal = dg_next_ordinal(ords, &nords, parent),
            .text = body,
            .len = (size_t)(stop - body),
            .first_line = first,
            .start_line = attr_line,
            .end_line = last,
        };
        dg_build(ctx, &src);
        title = dg_s(NULL, 0);
    }
}

/* ── Diagram files ───────────────────────────────────────────────── */

enum {
    DG_INFLATE_MAX = 64 * 1024 * 1024, /* a draw.io page; beyond it the page is skipped */
    DG_STARTUML_LEN = 9,               /* @startuml */
    DG_DIAGRAM_CLOSE_LEN = 10,         /* </diagram> */
    DG_CONTENT_ATTR_LEN = 9,           /* content=\" */
    DG_PAYLOAD_START_LEN = 22,         /* <!-- payload-start --> */
    DG_PAYLOAD_VERSION_LEN = 21,       /* <!-- payload-version: */
    DG_ASCII_END = 0x80,
    DG_UTF8_LEAD_MASK = 0xE0, /* a two-byte sequence's lead: 110xxxxx */
    DG_UTF8_LEAD2 = 0xC0,
    DG_UTF8_LEAD_LATIN1 = 0xC3, /* the last lead below U+0100 */
    DG_UTF8_LOW5 = 0x1F,
    DG_UTF8_LOW6 = 0x3F,
};

static bool dg_ends(const char *path, const char *suffix) {
    size_t pl = strlen(path);
    size_t sl = strlen(suffix);
    return pl >= sl && strcmp(path + pl - sl, suffix) == 0;
}

/* The file line of p; line 1 for text that is no slice of the file (a page
 * inflated or unescaped into a buffer of its own). */
static uint32_t dg_line_of(const CBMExtractCtx *ctx, const char *p) {
    uint32_t line = SKIP_ONE;
    if (p < ctx->source || p > ctx->source + ctx->source_len) {
        return line;
    }
    for (const char *q = ctx->source; q < p; q++) {
        line += *q == '\n';
    }
    return line;
}

/* A diagram file's name: the file name without its extension (nor a .drawio
 * or .excalidraw before it). */
static const char *dg_file_stem(CBMExtractCtx *ctx) {
    const char *base = strrchr(ctx->rel_path, '/');
    base = base ? base + SKIP_ONE : ctx->rel_path;
    const char *dot = strrchr(base, '.');
    size_t n = dot && dot > base ? (size_t)(dot - base) : strlen(base);
    static const char *const inner[] = {".drawio", ".excalidraw", NULL};
    for (int k = 0; inner[k]; k++) {
        size_t l = strlen(inner[k]);
        if (n > l && strncmp(base + n - l, inner[k], l) == 0) {
            n -= l;
        }
    }
    return cbm_arena_strndup(ctx->arena, base, n);
}

static void dg_file_one(CBMExtractCtx *ctx, const char *format, const char *title, int ordinal,
                        const char *text, size_t len, uint32_t first, uint32_t last) {
    dg_source_t src = {
        .format = format,
        .title = title,
        .fallback = dg_file_stem(ctx),
        .parent_qn = NULL,
        .ordinal = ordinal,
        .text = text,
        .len = len,
        .first_line = first,
        .start_line = first,
        .end_line = last,
    };
    dg_build(ctx, &src);
}

/* PlantUML: each @startuml ... @enduml block a diagram, its name the title. */
static void dg_file_plantuml(CBMExtractCtx *ctx) {
    const char *p = ctx->source;
    const char *end = ctx->source + ctx->source_len;
    int ordinal = 0;
    for (;;) {
        const char *start = NULL;
        for (const char *q = p; q + DG_STARTUML_LEN <= end && !start; q++) {
            start = memcmp(q, "@startuml", DG_STARTUML_LEN) == 0 ? q : NULL;
        }
        if (!start) {
            break;
        }
        const char *body = memchr(start, '\n', (size_t)(end - start));
        body = body ? body + SKIP_ONE : end;
        const char *stop = NULL;
        for (const char *q = body; q + CBM_SZ_7 <= end && !stop; q++) {
            stop = memcmp(q, "@enduml", CBM_SZ_7) == 0 ? q : NULL;
        }
        stop = stop ? stop : end;
        dg_str_t name =
            dg_trim(dg_s(start + DG_STARTUML_LEN, (size_t)(body - start) - DG_STARTUML_LEN));
        const char *title = name.n ? cbm_arena_strndup(ctx->arena, name.p, name.n) : NULL;
        uint32_t first = dg_line_of(ctx, body);
        dg_file_one(ctx, "plantuml", title, ++ordinal, body, (size_t)(stop - body), first,
                    dg_line_of(ctx, stop));
        p = stop;
    }
    if (ordinal == 0) {
        dg_file_one(ctx, "plantuml", NULL, SKIP_ONE, ctx->source, (size_t)ctx->source_len, SKIP_ONE,
                    dg_line_of(ctx, ctx->source + ctx->source_len));
    }
}

/* Base64 into out (cap bytes): the decoded length, 0 on a bad digit. */
static size_t dg_base64(const char *s, size_t n, unsigned char *out, size_t cap) {
    uint32_t acc = 0;
    int bits = 0;
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        int v = c >= 'A' && c <= 'Z'   ? c - 'A'
                : c >= 'a' && c <= 'z' ? c - 'a' + 26
                : c >= '0' && c <= '9' ? c - '0' + 52
                : c == '+'             ? 62
                : c == '/'             ? 63
                                       : -1;
        if (v < 0) {
            if (c == '=' || isspace((unsigned char)c)) {
                continue;
            }
            return 0;
        }
        acc = (acc << CBM_SZ_6) | (uint32_t)v;
        bits += CBM_SZ_6;
        if (bits >= CBM_SZ_8) {
            bits -= CBM_SZ_8;
            if (w >= cap) {
                return 0;
            }
            out[w++] = (unsigned char)(acc >> bits);
        }
    }
    return w;
}

/* zlib data (wbits: raw deflate when negative) inflated into the arena,
 * NUL-terminated. NULL when it is none or would pass DG_INFLATE_MAX. */
static char *dg_inflate(CBMExtractCtx *ctx, unsigned char *raw, size_t rn, int wbits, size_t *len) {
    size_t cap = rn * CBM_SZ_8 + CBM_SZ_1K;
    for (;;) {
        if (cap > DG_INFLATE_MAX) {
            return NULL; /* a bomb, or a page too large: the page is skipped whole */
        }
        char *out = (char *)cbm_arena_alloc(ctx->arena, cap + SKIP_ONE);
        if (!out) {
            return NULL;
        }
        z_stream z;
        memset(&z, 0, sizeof(z));
        if (inflateInit2(&z, wbits) != Z_OK) {
            return NULL;
        }
        z.next_in = raw;
        z.avail_in = (uInt)rn;
        z.next_out = (Bytef *)out;
        z.avail_out = (uInt)cap;
        int r = inflate(&z, Z_FINISH);
        inflateEnd(&z);
        if (r == Z_STREAM_END) {
            *len = z.total_out;
            out[*len] = '\0';
            return out;
        }
        if (r != Z_BUF_ERROR && r != Z_OK) {
            return NULL;
        }
        cap *= PAIR_LEN;
    }
}

/* A compressed draw.io page: base64, raw deflate, then URI-encoded XML. NULL
 * when it is none or would pass DG_INFLATE_MAX. */
static char *dg_drawio_inflate(CBMExtractCtx *ctx, dg_str_t b64, size_t *len) {
    unsigned char *raw = (unsigned char *)cbm_arena_alloc(ctx->arena, b64.n + SKIP_ONE);
    size_t rn = raw ? dg_base64(b64.p, b64.n, raw, b64.n) : 0;
    size_t out = 0;
    char *xml = rn ? dg_inflate(ctx, raw, rn, -MAX_WBITS, &out) : NULL;
    if (!xml) {
        return NULL;
    }
    size_t w = 0; /* URI-decode in place */
    for (size_t i = 0; i < out; i++) {
        if (xml[i] == '%' && i + PAIR_LEN < out && isxdigit((unsigned char)xml[i + SKIP_ONE]) &&
            isxdigit((unsigned char)xml[i + PAIR_LEN])) {
            char hex[CBM_SZ_3] = {xml[i + SKIP_ONE], xml[i + PAIR_LEN], '\0'};
            xml[w++] = (char)strtol(hex, NULL, CBM_SZ_16);
            i += PAIR_LEN;
        } else {
            xml[w++] = xml[i];
        }
    }
    xml[w] = '\0';
    *len = w;
    return xml;
}

/* Code points up to U+00FF of UTF-8 text back to the bytes they stand for;
 * false on any other. */
static bool dg_utf8_bytes(const unsigned char *s, size_t n, unsigned char *out, size_t *len) {
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] < DG_ASCII_END) {
            out[w++] = s[i];
        } else if ((s[i] & DG_UTF8_LEAD_MASK) == DG_UTF8_LEAD2 && i + SKIP_ONE < n &&
                   s[i] <= DG_UTF8_LEAD_LATIN1) {
            out[w++] = (unsigned char)(((s[i] & DG_UTF8_LOW5) << CBM_SZ_6) |
                                       (s[i + SKIP_ONE] & DG_UTF8_LOW6));
            i++;
        } else {
            return false;
        }
    }
    *len = w;
    return true;
}

/* An Excalidraw SVG export keeps its scene in a payload comment: base64 of a
 * JSON wrapper whose `encoded` byte string is the scene, zlib-compressed when
 * `compressed` says so. Version 1 wrote the wrapper as UTF-8, later ones
 * byte for byte; a wrapper without `encoded` is the scene itself. NULL for
 * none. */
static char *dg_excalidraw_svg_scene(CBMExtractCtx *ctx, size_t *len) {
    const char *start = strstr(ctx->source, "<!-- payload-start -->");
    const char *stop = start ? strstr(start, "<!-- payload-end -->") : NULL;
    if (!stop) {
        return NULL;
    }
    const char *b = start + DG_PAYLOAD_START_LEN;
    size_t bn = (size_t)(stop - b);
    unsigned char *raw = (unsigned char *)cbm_arena_alloc(ctx->arena, bn + SKIP_ONE);
    size_t rn = raw ? dg_base64(b, bn, raw, bn) : 0;
    if (!rn) {
        return NULL;
    }
    raw[rn] = '\0';
    const char *ver = strstr(ctx->source, "<!-- payload-version:");
    bool v1 = !ver || ver[DG_PAYLOAD_VERSION_LEN] == '1';
    yyjson_doc *doc = yyjson_read((const char *)raw, rn, YYJSON_READ_ALLOW_INVALID_UNICODE);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *enc = yyjson_obj_get(root, "encoded");
    char *scene = NULL;
    if (yyjson_is_str(enc)) {
        size_t sn = yyjson_get_len(enc);
        unsigned char *bytes = (unsigned char *)cbm_arena_alloc(ctx->arena, sn + SKIP_ONE);
        size_t nb = 0;
        const unsigned char *s = (const unsigned char *)yyjson_get_str(enc);
        bool ok = bytes != NULL;
        if (ok && v1) {
            ok = dg_utf8_bytes(s, sn, bytes, &nb);
        } else if (ok) {
            memcpy(bytes, s, sn);
            nb = sn;
        }
        if (ok && yyjson_get_bool(yyjson_obj_get(root, "compressed"))) {
            scene = dg_inflate(ctx, bytes, nb, MAX_WBITS, len);
        } else if (ok) {
            bytes[nb] = '\0';
            scene = (char *)bytes;
            *len = nb;
        }
    } else if (yyjson_is_obj(root)) {
        scene = (char *)raw;
        *len = rn;
    }
    yyjson_doc_free(doc);
    return scene;
}

/* Entities of an attribute value decoded (a .drawio.svg holds the file in its
 * `content` attribute). */
static char *dg_xml_unescape(CBMExtractCtx *ctx, const char *s, size_t n, size_t *len) {
    char *out = (char *)cbm_arena_alloc(ctx->arena, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        static const struct {
            const char *ent;
            char c;
        } ents[] = {{"&lt;", '<'},    {"&gt;", '>'},  {"&quot;", '"'},
                    {"&apos;", '\''}, {"&amp;", '&'}, {NULL, 0}};
        bool hit = false;
        for (int k = 0; ents[k].ent && !hit && s[i] == '&'; k++) {
            size_t el = strlen(ents[k].ent);
            if (i + el <= n && memcmp(s + i, ents[k].ent, el) == 0) {
                out[w++] = ents[k].c;
                i += el - SKIP_ONE;
                hit = true;
            }
        }
        if (!hit) {
            out[w++] = s[i];
        }
    }
    out[w] = '\0';
    *len = w;
    return out;
}

/* A page's name. draw.io's default `Page-<n>` names nothing: the first page
 * goes by the file, the others by the file and the page. */
static const char *dg_drawio_page_title(CBMExtractCtx *ctx, const char *name) {
    if (!name[0]) {
        return NULL;
    }
    bool dflt = strncmp(name, "Page-", CBM_SZ_5) == 0 && name[CBM_SZ_5];
    for (const char *q = name + (dflt ? CBM_SZ_5 : 0); dflt && *q; q++) {
        dflt = isdigit((unsigned char)*q) != 0;
    }
    if (!dflt) {
        return cbm_arena_strdup(ctx->arena, name);
    }
    if (strcmp(name, "Page-1") == 0) {
        return NULL;
    }
    return cbm_arena_sprintf(ctx->arena, "%s %s", dg_file_stem(ctx), name);
}

/* draw.io: each <diagram> page a diagram, named by the page. */
static void dg_file_drawio(CBMExtractCtx *ctx, const char *text, size_t len) {
    const char *end = text + len;
    int ordinal = 0;
    for (const char *p = text; p < end;) {
        const char *tag = NULL;
        for (const char *q = p; q + CBM_SZ_8 <= end && !tag; q++) {
            tag = memcmp(q, "<diagram", CBM_SZ_8) == 0 && (q[CBM_SZ_8] == ' ' || q[CBM_SZ_8] == '>')
                      ? q
                      : NULL;
        }
        if (!tag) {
            break;
        }
        const char *gt = memchr(tag, '>', (size_t)(end - tag));
        if (!gt) {
            break;
        }
        const char *close = NULL;
        for (const char *q = gt; q + DG_DIAGRAM_CLOSE_LEN <= end && !close; q++) {
            close = memcmp(q, "</diagram>", DG_DIAGRAM_CLOSE_LEN) == 0 ? q : NULL;
        }
        close = close ? close : end;
        char name[DG_LABEL_MAX];
        dg_xml_attr(tag, gt, "name", name, sizeof(name));
        const char *title = dg_drawio_page_title(ctx, name);
        dg_str_t inner = dg_trim(dg_s(gt + SKIP_ONE, (size_t)(close - gt) - SKIP_ONE));
        uint32_t line = dg_line_of(ctx, tag);
        if (inner.n && inner.p[0] == '<') {
            dg_file_one(ctx, "drawio", title, ++ordinal, inner.p, inner.n, line,
                        dg_line_of(ctx, close));
        } else if (inner.n) {
            size_t xl = 0;
            char *xml = dg_drawio_inflate(ctx, inner, &xl);
            if (xml) {
                dg_file_one(ctx, "drawio", title, ++ordinal, xml, xl, line, line);
            }
        }
        p = close;
    }
    if (ordinal == 0 && strstr(text, "<mxGraphModel")) {
        dg_file_one(ctx, "drawio", NULL, SKIP_ONE, text, len, SKIP_ONE, SKIP_ONE);
    }
}

void cbm_extract_diagram_file(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || !ctx->source || !ctx->rel_path) {
        return;
    }
    const char *path = ctx->rel_path;
    size_t len = (size_t)ctx->source_len;
    uint32_t last = dg_line_of(ctx, ctx->source + len);
    if (dg_ends(path, ".puml") || dg_ends(path, ".plantuml") || dg_ends(path, ".iuml") ||
        dg_ends(path, ".wsd")) {
        dg_file_plantuml(ctx);
    } else if (dg_ends(path, ".dot") || dg_ends(path, ".gv")) {
        dg_file_one(ctx, "dot", NULL, SKIP_ONE, ctx->source, len, SKIP_ONE, last);
    } else if (dg_ends(path, ".d2")) {
        dg_file_one(ctx, "d2", NULL, SKIP_ONE, ctx->source, len, SKIP_ONE, last);
    } else if (dg_ends(path, ".excalidraw") || dg_ends(path, ".excalidraw.json")) {
        dg_file_one(ctx, "excalidraw", NULL, SKIP_ONE, ctx->source, len, SKIP_ONE, last);
    } else if (dg_ends(path, ".excalidraw.svg")) {
        size_t sl = 0;
        char *scene = dg_excalidraw_svg_scene(ctx, &sl);
        if (scene) {
            dg_file_one(ctx, "excalidraw", NULL, SKIP_ONE, scene, sl, SKIP_ONE, last);
        }
    } else if (dg_ends(path, ".drawio.svg")) {
        /* the editable SVG keeps the draw.io file in its `content` attribute */
        const char *svg = strstr(ctx->source, "<svg");
        const char *gt = svg ? strchr(svg, '>') : NULL;
        char probe[CBM_SZ_8];
        if (svg && gt && dg_xml_attr(svg, gt, "content", probe, sizeof(probe))) {
            const char *v = strstr(svg, "content=\"");
            const char *ve =
                v ? memchr(v + DG_CONTENT_ATTR_LEN, '"', (size_t)(gt - v - DG_CONTENT_ATTR_LEN))
                  : NULL;
            size_t xl = 0;
            char *xml = ve ? dg_xml_unescape(ctx, v + DG_CONTENT_ATTR_LEN,
                                             (size_t)(ve - v - DG_CONTENT_ATTR_LEN), &xl)
                           : NULL;
            if (xml) {
                dg_file_drawio(ctx, xml, xl);
            }
        }
    } else if (dg_ends(path, ".drawio") || dg_ends(path, ".dio")) {
        dg_file_drawio(ctx, ctx->source, len);
    }
}

/* ── What a Markdown section's text keeps of its diagram fences ───── */

bool cbm_diagram_fence_is_diagram(const char *source, TSNode block) {
    TSNode content;
    return dg_kind(block, "fenced_code_block") && dg_fence_format(source, block, &content);
}

/* The labels of one Markdown diagram fence, space-joined in source order: the
 * DiagramElement names and arrow texts its parser produces, without the
 * format's syntax. The parse runs into a scratch result, so the file's own
 * definitions are untouched. NULL when the fence says nothing. */
const char *cbm_diagram_fence_labels(CBMExtractCtx *ctx, TSNode block) {
    TSNode content;
    const char *format = dg_fence_format(ctx->source, block, &content);
    if (!format) {
        return NULL;
    }
    CBMFileResult scratch;
    memset(&scratch, 0, sizeof(scratch));
    CBMExtractCtx sc = *ctx;
    sc.result = &scratch;
    uint32_t cb = ts_node_start_byte(content);
    dg_source_t src = {
        .format = format,
        .parent_qn = "e1",
        .ordinal = SKIP_ONE,
        .text = ctx->source + cb,
        .len = ts_node_end_byte(content) - cb,
        .first_line = ts_node_start_point(content).row + SKIP_ONE,
        .start_line = ts_node_start_point(block).row + SKIP_ONE,
        .end_line = ts_node_end_point(block).row + SKIP_ONE,
    };
    dg_build(&sc, &src);
    size_t total = 0;
    for (int i = 0; i < scratch.defs.count; i++) {
        const CBMDefinition *d = &scratch.defs.items[i];
        if (d->name && d->label && strcmp(d->label, "DiagramElement") == 0) {
            total += strlen(d->name) + SKIP_ONE;
        }
    }
    for (int i = 0; i < scratch.diagram_arrows.count; i++) {
        const char *l = scratch.diagram_arrows.items[i].label;
        total += l ? strlen(l) + SKIP_ONE : 0;
    }
    char *out = total ? (char *)cbm_arena_alloc(ctx->arena, total + SKIP_ONE) : NULL;
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    for (int i = 0; i < scratch.defs.count; i++) {
        const CBMDefinition *d = &scratch.defs.items[i];
        if (d->name && d->label && strcmp(d->label, "DiagramElement") == 0) {
            size_t l = strlen(d->name);
            memcpy(out + w, d->name, l);
            w += l;
            out[w++] = ' ';
        }
    }
    for (int i = 0; i < scratch.diagram_arrows.count; i++) {
        const char *l = scratch.diagram_arrows.items[i].label;
        if (l) {
            size_t n = strlen(l);
            memcpy(out + w, l, n);
            w += n;
            out[w++] = ' ';
        }
    }
    out[w] = '\0';
    return out;
}

void cbm_extract_diagrams(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || !ctx->source) {
        return;
    }
    if (ctx->language == CBM_LANG_MARKDOWN && !ts_node_is_null(ctx->root)) {
        dg_markdown(ctx);
        return;
    }
    if (ctx->language == CBM_LANG_RST && !ts_node_is_null(ctx->root)) {
        dg_rst(ctx);
        return;
    }
    if (ctx->language == CBM_LANG_ASCIIDOC) {
        dg_asciidoc(ctx);
        return;
    }
    if (ctx->language == CBM_LANG_MERMAID) {
        dg_source_t src = {
            .format = "mermaid",
            .fallback = dg_file_stem(ctx),
            .parent_qn = NULL,
            .ordinal = SKIP_ONE,
            .text = ctx->source,
            .len = (size_t)ctx->source_len,
            .first_line = SKIP_ONE,
            .start_line = SKIP_ONE,
            .end_line = SKIP_ONE,
        };
        for (int i = 0; i < ctx->source_len; i++) {
            src.end_line += ctx->source[i] == '\n';
        }
        dg_build(ctx, &src);
    }
}
