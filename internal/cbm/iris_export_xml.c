#include "iris_export_xml.h"
#include "arena.h"
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

#define EXPORT_MARKER "<Export generator="
#define MAX_NAME 256
#define MAX_PARAMS 32
#define MAX_CLASSES 64
#define BUF_CAP (1024 * 64)

typedef struct {
    char param_name[MAX_NAME];
    char param_value[MAX_NAME];
} PropParam;

typedef struct {
    char *buf;
    int pos;
    int cap;
} UdlBuf;

static const char *skip_ws(const char *p, const char *end) {
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n'))
        p++;
    return p;
}
static bool sw(const char *p, const char *end, const char *tag) {
    size_t n = strlen(tag);
    return (size_t)(end - p) >= n && memcmp(p, tag, n) == 0;
}
static const char *find_s(const char *p, const char *end, const char *needle) {
    size_t n = strlen(needle);
    while (p + n <= end) {
        if (memcmp(p, needle, n) == 0)
            return p;
        p++;
    }
    return NULL;
}
static const char *skip_tag(const char *p, const char *end) {
    while (p < end && *p != '>')
        p++;
    return p < end ? p + 1 : end;
}
static bool is_self_closing(const char *start, const char *gt) {
    const char *p = gt - 1;
    while (p > start && (*p == ' ' || *p == '\t'))
        p--;
    return *p == '/';
}
static void extract_attr(const char *ts, const char *te, const char *attr, char *out, size_t sz) {
    out[0] = '\0';
    size_t an = strlen(attr);
    for (const char *p = ts; p < te - an; p++) {
        if (memcmp(p, attr, an) == 0 && p[an] == '=') {
            p += an + 1;
            char q = *p;
            if (q != '"' && q != '\'')
                return;
            p++;
            const char *v = p;
            while (p < te && *p != q)
                p++;
            size_t vl = (size_t)(p - v);
            if (vl >= sz)
                vl = sz - 1;
            memcpy(out, v, vl);
            out[vl] = '\0';
            return;
        }
    }
}
static const char *elem_content(const char *p, const char *end, const char *tag, char *buf,
                                size_t bufsz) {
    buf[0] = '\0';
    char open[MAX_NAME + 2];
    snprintf(open, sizeof(open), "<%s", tag);
    const char *start = find_s(p, end, open);
    if (!start)
        return NULL;
    const char *gt = find_s(start, end, ">");
    if (!gt)
        return NULL;
    if (is_self_closing(start, gt))
        return gt + 1;
    const char *cs = gt + 1;
    if (sw(cs, end, "<![CDATA[")) {
        cs += 9;
        const char *ce = find_s(cs, end, "]]>");
        if (!ce)
            return NULL;
        size_t l = (size_t)(ce - cs);
        if (l >= bufsz)
            l = bufsz - 1;
        memcpy(buf, cs, l);
        buf[l] = '\0';
        return ce + 3;
    }
    char close[MAX_NAME + 4];
    snprintf(close, sizeof(close), "</%s>", tag);
    const char *cl = find_s(cs, end, close);
    if (!cl)
        return NULL;
    size_t l = (size_t)(cl - cs);
    if (l >= bufsz)
        l = bufsz - 1;
    memcpy(buf, cs, l);
    buf[l] = '\0';
    return cl + strlen(close);
}
static bool tag_is_one(const char *p, const char *end, const char *tag) {
    char buf[8];
    return elem_content(p, end, tag, buf, sizeof(buf)) && strcmp(buf, "1") == 0;
}

static void ub_init(UdlBuf *b, CBMArena *arena) {
    b->buf = (char *)cbm_arena_alloc(arena, BUF_CAP);
    b->pos = 0;
    b->cap = BUF_CAP;
    if (b->buf)
        b->buf[0] = '\0';
}
static void ub_app(UdlBuf *b, const char *s) {
    if (!b->buf || !s)
        return;
    size_t n = strlen(s);
    if (b->pos + (int)n + 1 >= b->cap)
        return;
    memcpy(b->buf + b->pos, s, n);
    b->pos += (int)n;
    b->buf[b->pos] = '\0';
}

static void emit_header(UdlBuf *b, const char *cs, const char *ce) {
    char name[MAX_NAME];
    extract_attr(cs, ce, "name", name, sizeof(name));
    if (!name[0])
        return;
    ub_app(b, "Class ");
    ub_app(b, name);
    char sup[MAX_NAME * 4] = "";
    elem_content(cs, ce, "Super", sup, sizeof(sup));
    if (sup[0]) {
        if (strchr(sup, ',')) {
            ub_app(b, " Extends (");
            ub_app(b, sup);
            ub_app(b, ")");
        } else {
            ub_app(b, " Extends ");
            ub_app(b, sup);
        }
    }
    char pragma[64] = "";
    if (tag_is_one(cs, ce, "Abstract"))
        strncat(pragma, "Abstract,", sizeof(pragma) - strlen(pragma) - 1);
    if (tag_is_one(cs, ce, "Final"))
        strncat(pragma, "Final,", sizeof(pragma) - strlen(pragma) - 1);
    if (pragma[0]) {
        pragma[strlen(pragma) - 1] = '\0';
        ub_app(b, " [ ");
        ub_app(b, pragma);
        ub_app(b, " ]");
    }
    ub_app(b, "\n{\n\n");
}

static void emit_method(UdlBuf *b, const char *ms, const char *me) {
    char mn[MAX_NAME];
    extract_attr(ms, me, "name", mn, sizeof(mn));
    if (!mn[0])
        return;
    bool cm = tag_is_one(ms, me, "ClassMethod");
    char formal[1024] = "";
    elem_content(ms, me, "FormalSpec", formal, sizeof(formal));
    char ret[MAX_NAME] = "";
    elem_content(ms, me, "ReturnType", ret, sizeof(ret));
    char desc[4096] = "";
    elem_content(ms, me, "Description", desc, sizeof(desc));
    if (desc[0]) {
        ub_app(b, "/// ");
        for (char *c = desc; *c; c++) {
            if (*c == '\n')
                ub_app(b, "\n/// ");
            else {
                char t[2] = {*c, 0};
                ub_app(b, t);
            }
        }
        ub_app(b, "\n");
    }
    ub_app(b, cm ? "ClassMethod " : "Method ");
    ub_app(b, mn);
    ub_app(b, "(");
    ub_app(b, formal);
    ub_app(b, ")");
    if (ret[0]) {
        ub_app(b, " As ");
        ub_app(b, ret);
    }
    ub_app(b, "\n{\n");
    char impl[1024 * 32] = "";
    elem_content(ms, me, "Implementation", impl, sizeof(impl));
    ub_app(b, impl);
    ub_app(b, "}\n\n");
}

static void emit_property(UdlBuf *b, const char *ps, const char *pe) {
    char pn[MAX_NAME];
    extract_attr(ps, pe, "name", pn, sizeof(pn));
    if (!pn[0])
        return;
    char pt[MAX_NAME] = "";
    elem_content(ps, pe, "Type", pt, sizeof(pt));
    PropParam params[MAX_PARAMS];
    int np = 0;
    const char *pp = ps;
    while (pp < pe && np < MAX_PARAMS) {
        const char *po = find_s(pp, pe, "<Parameter ");
        if (!po)
            break;
        const char *pg = find_s(po, pe, ">");
        if (!pg)
            break;
        extract_attr(po, pg, "name", params[np].param_name, MAX_NAME);
        extract_attr(po, pg, "value", params[np].param_value, MAX_NAME);
        if (!params[np].param_value[0]) {
            char db[MAX_NAME];
            const char *a = elem_content(po, pe, "Parameter", db, MAX_NAME);
            if (a && db[0])
                strncpy(params[np].param_value, db, MAX_NAME - 1);
        }
        if (params[np].param_name[0])
            np++;
        pp = pg + 1;
    }
    ub_app(b, "Property ");
    ub_app(b, pn);
    if (pt[0]) {
        ub_app(b, " As ");
        ub_app(b, pt);
    }
    if (np > 0) {
        ub_app(b, "(");
        for (int i = 0; i < np; i++) {
            if (i > 0)
                ub_app(b, ", ");
            ub_app(b, params[i].param_name);
            if (params[i].param_value[0]) {
                ub_app(b, " = ");
                ub_app(b, params[i].param_value);
            }
        }
        ub_app(b, ")");
    }
    ub_app(b, ";\n\n");
}

static void emit_parameter(UdlBuf *b, const char *ps, const char *pe) {
    char pn[MAX_NAME];
    extract_attr(ps, pe, "name", pn, sizeof(pn));
    if (!pn[0])
        return;
    char dv[MAX_NAME] = "";
    elem_content(ps, pe, "Default", dv, sizeof(dv));
    ub_app(b, "Parameter ");
    ub_app(b, pn);
    if (dv[0]) {
        ub_app(b, " = \"");
        ub_app(b, dv);
        ub_app(b, "\"");
    }
    ub_app(b, ";\n\n");
}

static void emit_index(UdlBuf *b, const char *is_, const char *ie) {
    char in_[MAX_NAME];
    extract_attr(is_, ie, "name", in_, sizeof(in_));
    if (!in_[0])
        return;
    char props[MAX_NAME * 4] = "";
    elem_content(is_, ie, "Properties", props, sizeof(props));
    bool uniq = tag_is_one(is_, ie, "Unique");
    bool pkey = tag_is_one(is_, ie, "PrimaryKey");
    ub_app(b, "Index ");
    ub_app(b, in_);
    if (props[0]) {
        ub_app(b, " On ");
        ub_app(b, props);
    }
    if (uniq || pkey) {
        ub_app(b, " [ ");
        if (pkey)
            ub_app(b, "PrimaryKey, ");
        if (uniq)
            ub_app(b, "Unique");
        ub_app(b, " ]");
    }
    ub_app(b, ";\n\n");
}

static void emit_xdata(UdlBuf *b, const char *xs, const char *xe) {
    char xn[MAX_NAME];
    extract_attr(xs, xe, "name", xn, sizeof(xn));
    if (!xn[0])
        return;
    char data[1024 * 32] = "";
    elem_content(xs, xe, "Data", data, sizeof(data));
    ub_app(b, "XData ");
    ub_app(b, xn);
    ub_app(b, "\n{\n");
    ub_app(b, data);
    ub_app(b, "\n}\n\n");
}

static char *transcode_class(CBMArena *arena, const char *cs, const char *ce) {
    UdlBuf b;
    ub_init(&b, arena);
    if (!b.buf)
        return NULL;
    emit_header(&b, cs, ce);
    const char *p = cs;
    while (p < ce) {
        p = skip_ws(p, ce);
        if (p >= ce || *p != '<') {
            if (p < ce)
                p++;
            continue;
        }
        if (sw(p, ce, "<Method ") || sw(p, ce, "<Method>")) {
            const char *gt = find_s(p, ce, ">");
            if (!gt)
                break;
            const char *me = find_s(gt + 1, ce, "</Method>");
            if (!me) {
                p = gt + 1;
                continue;
            }
            emit_method(&b, p, me + strlen("</Method>"));
            p = me + strlen("</Method>");
            continue;
        }
        if (sw(p, ce, "<Property ")) {
            const char *gt = find_s(p, ce, ">");
            if (!gt)
                break;
            const char *pe = find_s(gt + 1, ce, "</Property>");
            if (!pe) {
                p = gt + 1;
                continue;
            }
            emit_property(&b, p, pe + strlen("</Property>"));
            p = pe + strlen("</Property>");
            continue;
        }
        if (sw(p, ce, "<Parameter ")) {
            const char *gt = find_s(p, ce, ">");
            if (!gt)
                break;
            if (is_self_closing(p, gt)) {
                p = gt + 1;
                continue;
            }
            const char *pe = find_s(gt + 1, ce, "</Parameter>");
            if (!pe) {
                p = gt + 1;
                continue;
            }
            emit_parameter(&b, p, pe + strlen("</Parameter>"));
            p = pe + strlen("</Parameter>");
            continue;
        }
        if (sw(p, ce, "<Index ") || sw(p, ce, "<Index>")) {
            const char *gt = find_s(p, ce, ">");
            if (!gt)
                break;
            const char *ie = find_s(gt + 1, ce, "</Index>");
            if (!ie) {
                p = gt + 1;
                continue;
            }
            emit_index(&b, p, ie + strlen("</Index>"));
            p = ie + strlen("</Index>");
            continue;
        }
        if (sw(p, ce, "<XData ") || sw(p, ce, "<XData>")) {
            const char *gt = find_s(p, ce, ">");
            if (!gt)
                break;
            const char *xe = find_s(gt + 1, ce, "</XData>");
            if (!xe) {
                p = gt + 1;
                continue;
            }
            emit_xdata(&b, p, xe + strlen("</XData>"));
            p = xe + strlen("</XData>");
            continue;
        }
        p = skip_tag(p, ce);
    }
    ub_app(&b, "}\n");
    return b.buf;
}

char **cbm_iris_export_to_udl(CBMArena *arena, const char *xml, int xml_len, int *class_count) {
    if (class_count)
        *class_count = 0;
    if (!arena || !xml || xml_len <= 0)
        return NULL;
    const char *end = xml + xml_len;
    if (!find_s(xml, end, EXPORT_MARKER))
        return NULL;
    char *results[MAX_CLASSES];
    int count = 0;
    const char *p = xml;
    while (p < end && count < MAX_CLASSES) {
        const char *co = find_s(p, end, "<Class ");
        if (!co)
            break;
        const char *gt = find_s(co, end, ">");
        if (!gt)
            break;
        const char *cc = find_s(gt + 1, end, "</Class>");
        if (!cc)
            break;
        char *udl = transcode_class(arena, co, cc);
        if (udl && udl[0])
            results[count++] = udl;
        p = cc + strlen("</Class>");
    }
    if (!count)
        return NULL;
    char **arr = (char **)cbm_arena_alloc(arena, (size_t)(count + 1) * sizeof(char *));
    if (!arr)
        return NULL;
    for (int i = 0; i < count; i++)
        arr[i] = results[i];
    arr[count] = NULL;
    if (class_count)
        *class_count = count;
    return arr;
}
