#include "twincat_xml.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TWINCAT_MARKER "<TcPlcObject"
#define TC_MAX_NAME 256

/* A payload slice inside the XML buffer. `line` is the 1-based physical line
 * of the payload's first byte, used for newline padding. */
typedef struct {
    const char *p;
    int len;
    int line;
} TcSpan;

typedef struct {
    char *buf;
    int pos;
    int cap;
    int line; /* 1-based line the next appended byte lands on */
} TcBuf;

static const char *tc_find(const char *p, const char *end, const char *needle) {
    size_t n = strlen(needle);
    while (p + n <= end) {
        if (memcmp(p, needle, n) == 0) {
            return p;
        }
        p++;
    }
    return NULL;
}

static bool tc_self_closing(const char *start, const char *gt) {
    const char *p = gt - 1;
    while (p > start && (*p == ' ' || *p == '\t')) {
        p--;
    }
    return *p == '/';
}

static void tc_attr(const char *ts, const char *te, const char *attr, char *out, size_t sz) {
    out[0] = '\0';
    size_t an = strlen(attr);
    for (const char *p = ts; p + an < te; p++) {
        if (memcmp(p, attr, an) == 0 && p[an] == '=') {
            p += an + 1;
            char q = *p;
            if (q != '"' && q != '\'') {
                return;
            }
            p++;
            const char *v = p;
            while (p < te && *p != q) {
                p++;
            }
            size_t vl = (size_t)(p - v);
            if (vl >= sz) {
                vl = sz - 1;
            }
            memcpy(out, v, vl);
            out[vl] = '\0';
            return;
        }
    }
}

static int tc_line_of(const char *base, const char *p) {
    int line = 1;
    for (const char *c = base; c < p; c++) {
        if (*c == '\n') {
            line++;
        }
    }
    return line;
}

/* Locate <tag ...> inside [p,end) and return its content as a span: the CDATA
 * payload when present, otherwise the raw text up to </tag>. Returns the
 * position after the element, or NULL when the tag is absent. */
static const char *tc_elem(const char *base, const char *p, const char *end, const char *tag,
                           TcSpan *out) {
    out->p = NULL;
    out->len = 0;
    out->line = 0;
    char open[TC_MAX_NAME];
    snprintf(open, sizeof(open), "<%s", tag);
    /* `<ST` must not match `<STring`: the next char has to close the name. */
    const char *start = NULL;
    for (const char *cursor = p; cursor < end;) {
        const char *hit = tc_find(cursor, end, open);
        if (!hit) {
            return NULL;
        }
        const char *after = hit + strlen(open);
        if (after < end && *after != '>' && *after != ' ' && *after != '\t' && *after != '/' &&
            *after != '\r' && *after != '\n') {
            cursor = after;
            continue;
        }
        start = hit;
        break;
    }
    if (!start) {
        return NULL;
    }
    const char *gt = tc_find(start, end, ">");
    if (!gt) {
        return NULL;
    }
    if (tc_self_closing(start, gt)) {
        return gt + 1;
    }
    const char *cs = gt + 1;
    if ((size_t)(end - cs) >= 9 && memcmp(cs, "<![CDATA[", 9) == 0) {
        cs += 9;
        const char *ce = tc_find(cs, end, "]]>");
        if (!ce) {
            return NULL;
        }
        out->p = cs;
        out->len = (int)(ce - cs);
        out->line = tc_line_of(base, cs);
        return ce + 3;
    }
    char close[TC_MAX_NAME];
    snprintf(close, sizeof(close), "</%s>", tag);
    const char *cl = tc_find(cs, end, close);
    if (!cl) {
        return NULL;
    }
    out->p = cs;
    out->len = (int)(cl - cs);
    out->line = tc_line_of(base, cs);
    return cl + strlen(close);
}

static void tb_init(TcBuf *b, CBMArena *arena, int cap) {
    b->buf = (char *)cbm_arena_alloc(arena, (size_t)cap);
    b->pos = 0;
    b->cap = cap;
    b->line = 1;
    if (b->buf) {
        b->buf[0] = '\0';
    }
}

static void tb_app_n(TcBuf *b, const char *s, int n) {
    if (!b->buf || !s || n <= 0 || b->pos + n + 1 >= b->cap) {
        return;
    }
    memcpy(b->buf + b->pos, s, (size_t)n);
    for (int i = 0; i < n; i++) {
        if (s[i] == '\n') {
            b->line++;
        }
    }
    b->pos += n;
    b->buf[b->pos] = '\0';
}

static void tb_app(TcBuf *b, const char *s) {
    tb_app_n(b, s, s ? (int)strlen(s) : 0);
}

/* Pad with newlines so the next appended byte lands on `line`, keeping
 * synthesized ST aligned with the physical .TcPOU file. Never truncates:
 * content already past the target stays where it is. */
static void tb_pad_to(TcBuf *b, int line) {
    while (b->line < line) {
        int before = b->pos;
        tb_app(b, "\n");
        if (b->pos == before) {
            break;
        }
    }
}

static bool tc_kw_at(const char *p, const char *end, const char *kw) {
    size_t n = strlen(kw);
    if ((size_t)(end - p) < n) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        char a = p[i];
        char k = kw[i];
        if (a >= 'a' && a <= 'z') {
            a = (char)(a - 'a' + 'A');
        }
        if (a != k) {
            return false;
        }
    }
    /* keyword boundary: next char must not extend the identifier */
    if ((size_t)(end - p) > n) {
        char c = p[n];
        if (c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9')) {
            return false;
        }
    }
    return true;
}

/* Locate the <ST> payload inside the <Implementation> element of [from,to).
 * Scoping the search to the Implementation range keeps identifier text such
 * as `x<ST_Max` inside a Declaration CDATA from being taken for an ST tag. */
static void tc_impl_st(const char *base, const char *from, const char *to, TcSpan *out) {
    out->p = NULL;
    out->len = 0;
    out->line = 0;
    const char *is = tc_find(from, to, "<Implementation");
    if (!is) {
        return;
    }
    const char *ie = tc_find(is, to, "</Implementation>");
    if (!ie) {
        ie = to;
    }
    tc_elem(base, is, ie, "ST", out);
}

/* Sniff the POU kind from its declaration header and return the matching
 * END_* terminator. FUNCTION_BLOCK is probed before FUNCTION (prefix). */
static const char *tc_terminator(const TcSpan *decl) {
    const char *p = decl->p;
    const char *end = decl->p + decl->len;
    while (p < end) {
        if (tc_kw_at(p, end, "FUNCTION_BLOCK")) {
            return "END_FUNCTION_BLOCK";
        }
        if (tc_kw_at(p, end, "PROGRAM")) {
            return "END_PROGRAM";
        }
        if (tc_kw_at(p, end, "INTERFACE")) {
            return "END_INTERFACE";
        }
        if (tc_kw_at(p, end, "FUNCTION")) {
            return "END_FUNCTION";
        }
        p++;
    }
    return "END_FUNCTION_BLOCK";
}

/* Append a DUT declaration, inserting the `;` the grammar requires after
 * END_STRUCT/END_UNION when the TwinCAT editor omitted it. */
static void tb_app_dut(TcBuf *b, const TcSpan *decl) {
    const char *p = decl->p;
    const char *end = decl->p + decl->len;
    const char *chunk = p;
    while (p < end) {
        if (tc_kw_at(p, end, "END_STRUCT") || tc_kw_at(p, end, "END_UNION")) {
            size_t kn = tc_kw_at(p, end, "END_STRUCT") ? 10 : 9;
            const char *after = p + kn;
            const char *q = after;
            while (q < end && (*q == ' ' || *q == '\t' || *q == '\r')) {
                q++;
            }
            if (q >= end || *q != ';') {
                tb_app_n(b, chunk, (int)(after - chunk));
                tb_app(b, ";");
                chunk = after;
            }
            p = after;
            continue;
        }
        p++;
    }
    tb_app_n(b, chunk, (int)(end - chunk));
}

/* Emit one <Method>/<Action>/<Property> child. `kind` is the element name. */
static void tc_emit_member(TcBuf *b, const char *base, const char *ms, const char *me,
                           const char *kind) {
    char name[TC_MAX_NAME];
    const char *gt = tc_find(ms, me, ">");
    tc_attr(ms, gt ? gt : me, "Name", name, sizeof(name));

    if (strcmp(kind, "Property") == 0) {
        TcSpan decl;
        tc_elem(base, ms, me, "Declaration", &decl);
        if (decl.p) {
            tb_pad_to(b, decl.line);
            tb_app_n(b, decl.p, decl.len);
        } else if (name[0]) {
            tb_app(b, "PROPERTY ");
            tb_app(b, name);
        }
        tb_app(b, "\n");
        static const char *accessors[2][3] = {{"Get", "GET", "END_GET"}, {"Set", "SET", "END_SET"}};
        for (int i = 0; i < 2; i++) {
            const char *as = tc_find(ms, me, i == 0 ? "<Get" : "<Set");
            if (!as) {
                continue;
            }
            char close[16];
            snprintf(close, sizeof(close), "</%s>", accessors[i][0]);
            const char *ae = tc_find(as, me, close);
            if (!ae) {
                ae = me;
            }
            TcSpan body;
            tb_app(b, accessors[i][1]);
            tb_app(b, "\n");
            tc_impl_st(base, as, ae, &body);
            if (body.p) {
                tb_pad_to(b, body.line);
                tb_app_n(b, body.p, body.len);
            }
            tb_app(b, "\n");
            tb_app(b, accessors[i][2]);
            tb_app(b, "\n");
        }
        tb_app(b, "END_PROPERTY\n");
        return;
    }

    TcSpan decl;
    tc_elem(base, ms, me, "Declaration", &decl);
    if (strcmp(kind, "Action") == 0) {
        /* The grammar has no ACTION construct; a parameterless METHOD has the
         * same member shape and the same inst.Name() call sites. */
        tb_app(b, "METHOD ");
        tb_app(b, name[0] ? name : "TcAction");
        tb_app(b, "\n");
    } else if (decl.p) {
        tb_pad_to(b, decl.line);
        tb_app_n(b, decl.p, decl.len);
        tb_app(b, "\n");
    } else {
        tb_app(b, "METHOD ");
        tb_app(b, name[0] ? name : "TcMethod");
        tb_app(b, "\n");
    }
    TcSpan body;
    tc_impl_st(base, ms, me, &body);
    if (body.p) {
        tb_pad_to(b, body.line);
        tb_app_n(b, body.p, body.len);
        tb_app(b, "\n");
    }
    tb_app(b, "END_METHOD\n");
}

/* Find the next member child (<Method/<Action/<Property) at or after `p`.
 * Returns the element start or NULL; *close_out gets the matching close tag. */
static const char *tc_next_member(const char *p, const char *end, const char **kind_out,
                                  const char **close_out) {
    static const char *kinds[3] = {"Method", "Action", "Property"};
    static const char *opens[3] = {"<Method", "<Action", "<Property"};
    static const char *closes[3] = {"</Method>", "</Action>", "</Property>"};
    const char *best = NULL;
    int best_i = -1;
    for (int i = 0; i < 3; i++) {
        const char *hit = tc_find(p, end, opens[i]);
        /* require an attribute/tag boundary so <Methods> would not match */
        while (hit) {
            const char *after = hit + strlen(opens[i]);
            if (after < end && (*after == ' ' || *after == '>' || *after == '\t')) {
                break;
            }
            hit = tc_find(after, end, opens[i]);
        }
        if (hit && (!best || hit < best)) {
            best = hit;
            best_i = i;
        }
    }
    if (!best) {
        return NULL;
    }
    *kind_out = kinds[best_i];
    *close_out = closes[best_i];
    return best;
}

char **cbm_twincat_to_st(CBMArena *arena, const char *xml, int xml_len, int *unit_count) {
    *unit_count = 0;
    if (!xml || xml_len < (int)strlen(TWINCAT_MARKER)) {
        return NULL;
    }
    /* UTF-16 input would be garbled by byte scanning — reject it. */
    if (xml_len >= 2 && ((unsigned char)xml[0] == 0xFF && (unsigned char)xml[1] == 0xFE)) {
        return NULL;
    }
    if (xml_len >= 2 && ((unsigned char)xml[0] == 0xFE && (unsigned char)xml[1] == 0xFF)) {
        return NULL;
    }
    const char *base = xml;
    const char *end = xml + xml_len;
    if (xml_len >= 3 && (unsigned char)xml[0] == 0xEF && (unsigned char)xml[1] == 0xBB &&
        (unsigned char)xml[2] == 0xBF) {
        xml += 3; /* line accounting stays based at `base` (BOM is not a line) */
    }
    if (!tc_find(xml, end, TWINCAT_MARKER)) {
        return NULL;
    }

    static const struct {
        const char *open;
        const char *close;
        char kind; /* P=POU D=DUT G=GVL I=Itf */
    } roots[] = {
        {"<POU ", "</POU>", 'P'},
        {"<DUT ", "</DUT>", 'D'},
        {"<GVL ", "</GVL>", 'G'},
        {"<Itf ", "</Itf>", 'I'},
    };
    const char *es = NULL;
    const char *ee = NULL;
    char kind = 0;
    for (size_t i = 0; i < sizeof(roots) / sizeof(roots[0]); i++) {
        const char *hit = tc_find(xml, end, roots[i].open);
        if (hit && (!es || hit < es)) {
            const char *close = tc_find(hit, end, roots[i].close);
            if (close) {
                es = hit;
                ee = close;
                kind = roots[i].kind;
            }
        }
    }
    if (!es) {
        return NULL;
    }

    TcBuf b;
    tb_init(&b, arena, xml_len + 4096);
    if (!b.buf) {
        return NULL;
    }

    const char *first_kind = NULL;
    const char *first_close = NULL;
    const char *first_member = tc_next_member(es, ee, &first_kind, &first_close);
    const char *own_end = first_member ? first_member : ee;

    TcSpan decl;
    tc_elem(base, es, own_end, "Declaration", &decl);

    if (kind == 'D') {
        if (!decl.p) {
            return NULL;
        }
        tb_pad_to(&b, decl.line);
        tb_app_dut(&b, &decl);
        tb_app(&b, "\n");
    } else if (kind == 'G') {
        if (!decl.p) {
            return NULL;
        }
        tb_pad_to(&b, decl.line);
        tb_app_n(&b, decl.p, decl.len);
        tb_app(&b, "\n");
    } else {
        /* POU or Itf: declaration header, then member children in document
         * order, then the POU's own implementation body, then the terminator
         * (the textual composition the grammar accepts). */
        if (!decl.p) {
            return NULL;
        }
        tb_pad_to(&b, decl.line);
        tb_app_n(&b, decl.p, decl.len);
        tb_app(&b, "\n");

        const char *cursor = first_member;
        while (cursor) {
            const char *mkind = NULL;
            const char *mclose = NULL;
            const char *ms = tc_next_member(cursor, ee, &mkind, &mclose);
            if (!ms) {
                break;
            }
            const char *me = tc_find(ms, ee, mclose);
            if (!me) {
                break;
            }
            me += strlen(mclose);
            tc_emit_member(&b, base, ms, me, mkind);
            cursor = me;
        }

        if (kind == 'P') {
            TcSpan body;
            tc_impl_st(base, es, own_end, &body);
            if (body.p) {
                tb_pad_to(&b, body.line);
                tb_app_n(&b, body.p, body.len);
                tb_app(&b, "\n");
            }
            tb_app(&b, tc_terminator(&decl));
            tb_app(&b, "\n");
        } else {
            tb_app(&b, "END_INTERFACE\n");
        }
    }

    char **units = (char **)cbm_arena_alloc(arena, sizeof(char *));
    if (!units) {
        return NULL;
    }
    units[0] = b.buf;
    *unit_count = 1;
    return units;
}
