#include "plcopen_xml.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h> // strcasecmp

#define PLCOPEN_MARKER "plcopen.org/xml"
#define PX_MAX_NAME 256
#define PX_BUF_CAP (1024 * 64)
#define PX_MAX_UNITS 512

/* A parsed `<tag ...>...</tag>` (or self-closing `<tag .../>`) element. */
typedef struct {
    const char *tag_start; /* position of '<' */
    const char *gt;        /* position of the opening tag's '>' */
    const char *cs;        /* content start (== gt + 1) */
    const char *ce;        /* content end (== cs when self-closing) */
    bool self_closing;
} PxElem;

typedef struct {
    char *buf;
    int pos;
    int cap;
} PxBuf;

static const char *px_find(const char *p, const char *end, const char *needle) {
    size_t n = strlen(needle);
    while (p + n <= end) {
        if (memcmp(p, needle, n) == 0) {
            return p;
        }
        p++;
    }
    return NULL;
}

static bool px_self_closing(const char *start, const char *gt) {
    const char *p = gt - 1;
    while (p > start && (*p == ' ' || *p == '\t')) {
        p--;
    }
    return *p == '/';
}

/* Extract attr="value" (or attr='value') from inside [ts, te). `te` bounds
 * the search to one tag's attribute region (typically its '>'). */
static void px_attr(const char *ts, const char *te, const char *attr, char *out, size_t sz) {
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

/* Locate the next `<tag ...>` (or self-closing `<tag .../>`) at or after `p`
 * within [p, end), guarding against a prefix false match (`<type` must not
 * match `<types`, `<variable` must not match `<variables`, `<body` must not
 * match `<bodyText`, `<pou` must not match `<pous`, `<action` must not match
 * `<actions`, `<dataType` must not match `<dataTypes`): the character
 * immediately after the tag name must close the name. Returns the position
 * right after the whole element, or NULL if no (correctly bounded) match
 * exists in the range. */
static const char *px_next_elem(const char *p, const char *end, const char *tag, PxElem *out) {
    out->tag_start = NULL;
    out->gt = NULL;
    out->cs = NULL;
    out->ce = NULL;
    out->self_closing = false;

    char open[PX_MAX_NAME];
    snprintf(open, sizeof(open), "<%s", tag);
    size_t open_len = strlen(open);

    const char *start = NULL;
    for (const char *cursor = p; cursor < end;) {
        const char *hit = px_find(cursor, end, open);
        if (!hit) {
            return NULL;
        }
        const char *after_name = hit + open_len;
        if (after_name < end &&
            (*after_name == '>' || *after_name == ' ' || *after_name == '\t' ||
             *after_name == '/' || *after_name == '\r' || *after_name == '\n')) {
            start = hit;
            break;
        }
        cursor = after_name;
    }
    if (!start) {
        return NULL;
    }

    const char *gt = px_find(start, end, ">");
    if (!gt) {
        return NULL;
    }
    out->tag_start = start;
    out->gt = gt;
    if (px_self_closing(start, gt)) {
        out->self_closing = true;
        out->cs = gt + 1;
        out->ce = gt + 1;
        return gt + 1;
    }
    char close[PX_MAX_NAME + 3];
    snprintf(close, sizeof(close), "</%s>", tag);
    const char *cl = px_find(gt + 1, end, close);
    if (!cl) {
        return NULL;
    }
    out->cs = gt + 1;
    out->ce = cl;
    return cl + strlen(close);
}

/* Find the first child ELEMENT inside [p, end) (skipping leading whitespace
 * and any text), returning its tag name and the span needed to read its
 * attributes. Returns false when the content opens with a closing tag or has
 * no element at all (plain text / empty). */
static bool px_first_child_tag(const char *p, const char *end, char *tag_out, size_t tag_sz,
                               const char **tag_start_out, const char **gt_out) {
    for (const char *q = p; q < end; q++) {
        if (*q != '<') {
            continue;
        }
        if (q + 1 < end && q[1] == '/') {
            return false;
        }
        const char *n = q + 1;
        size_t len = 0;
        while (n < end && len + 1 < tag_sz &&
               ((*n >= 'a' && *n <= 'z') || (*n >= 'A' && *n <= 'Z') || (*n >= '0' && *n <= '9') ||
                *n == '_')) {
            tag_out[len++] = *n;
            n++;
        }
        tag_out[len] = '\0';
        if (len == 0) {
            return false;
        }
        const char *gt = px_find(q, end, ">");
        if (!gt) {
            return false;
        }
        *tag_start_out = q;
        *gt_out = gt;
        return true;
    }
    return false;
}

/* Resolve a <type>/<baseType>/<returnType> element's content to an ST type
 * identifier: the first child's tag name (`<REAL/>` -> "REAL"), or a
 * <derived name="X"/> child's `name` attribute. `array` and `pointer` are ST
 * keywords (ARRAY/POINTER require full syntax this transcoder does not
 * synthesize) and are reported as unresolved rather than emitted as bare
 * identifiers the grammar would reject. */
static bool px_resolve_type(const char *cs, const char *ce, char *out, size_t out_sz) {
    char tag[PX_MAX_NAME];
    const char *tag_start = NULL;
    const char *gt = NULL;
    if (!px_first_child_tag(cs, ce, tag, sizeof(tag), &tag_start, &gt)) {
        return false;
    }
    if (strcasecmp(tag, "derived") == 0) {
        px_attr(tag_start, gt, "name", out, out_sz);
        return out[0] != '\0';
    }
    if (strcasecmp(tag, "array") == 0 || strcasecmp(tag, "pointer") == 0) {
        return false;
    }
    snprintf(out, out_sz, "%s", tag);
    return true;
}

/* Decode the five XML predefined entities into a caller-owned buffer.
 * Unrecognized `&...;` sequences are left verbatim. */
static void px_decode_attr(const char *in, char *out, size_t out_sz) {
    static const struct {
        const char *ent;
        char ch;
    } ents[] = {
        {"&amp;", '&'}, {"&lt;", '<'}, {"&gt;", '>'}, {"&quot;", '"'}, {"&apos;", '\''},
    };
    size_t oi = 0;
    const char *p = in;
    while (*p && oi + 1 < out_sz) {
        bool matched = false;
        if (*p == '&') {
            for (size_t i = 0; i < sizeof(ents) / sizeof(ents[0]); i++) {
                size_t el = strlen(ents[i].ent);
                if (strncmp(p, ents[i].ent, el) == 0) {
                    out[oi++] = ents[i].ch;
                    p += el;
                    matched = true;
                    break;
                }
            }
        }
        if (!matched) {
            out[oi++] = *p++;
        }
    }
    out[oi] = '\0';
}

static void pxb_init(PxBuf *b, CBMArena *arena, int cap) {
    b->buf = (char *)cbm_arena_alloc(arena, (size_t)cap);
    b->pos = 0;
    b->cap = cap;
    if (b->buf) {
        b->buf[0] = '\0';
    }
}

static void pxb_app_n(PxBuf *b, const char *s, int n) {
    if (!b->buf || !s || n <= 0 || b->pos + n + 1 >= b->cap) {
        return;
    }
    memcpy(b->buf + b->pos, s, (size_t)n);
    b->pos += n;
    b->buf[b->pos] = '\0';
}

static void pxb_app(PxBuf *b, const char *s) {
    pxb_app_n(b, s, s ? (int)strlen(s) : 0);
}

/* Append [s, s+n) with the five XML predefined entities decoded. */
static void pxb_app_decoded(PxBuf *b, const char *s, int n) {
    if (!b->buf || !s || n <= 0) {
        return;
    }
    static const struct {
        const char *ent;
        char ch;
    } ents[] = {
        {"&amp;", '&'}, {"&lt;", '<'}, {"&gt;", '>'}, {"&quot;", '"'}, {"&apos;", '\''},
    };
    const char *p = s;
    const char *end = s + n;
    while (p < end) {
        const char *amp = memchr(p, '&', (size_t)(end - p));
        const char *chunk_end = amp ? amp : end;
        pxb_app_n(b, p, (int)(chunk_end - p));
        if (!amp) {
            break;
        }
        bool matched = false;
        for (size_t i = 0; i < sizeof(ents) / sizeof(ents[0]); i++) {
            size_t el = strlen(ents[i].ent);
            if ((size_t)(end - amp) >= el && memcmp(amp, ents[i].ent, el) == 0) {
                pxb_app_n(b, &ents[i].ch, 1);
                p = amp + el;
                matched = true;
                break;
            }
        }
        if (!matched) {
            pxb_app_n(b, amp, 1);
            p = amp + 1;
        }
    }
}

/* Emit `name : TYPE[:= value];` for every <variable> child in [cs, ce),
 * skipping a variable whose type does not resolve (missing <type>, or an
 * array/pointer element type — see px_resolve_type). Never emits an address
 * (`AT %I*` is not supported by the grammar); a <variable>'s `address`
 * attribute, if any, is simply not read. */
static void px_emit_variable_list(PxBuf *b, const char *cs, const char *ce) {
    const char *cursor = cs;
    PxElem var;
    const char *after;
    while ((after = px_next_elem(cursor, ce, "variable", &var)) != NULL) {
        cursor = after;
        char vname[PX_MAX_NAME];
        px_attr(var.tag_start, var.gt, "name", vname, sizeof(vname));
        if (!vname[0]) {
            continue;
        }
        char type_name[PX_MAX_NAME];
        bool has_type = false;
        PxElem type_elem;
        if (!var.self_closing && px_next_elem(var.cs, var.ce, "type", &type_elem)) {
            has_type = px_resolve_type(type_elem.cs, type_elem.ce, type_name, sizeof(type_name));
        }
        if (!has_type) {
            continue;
        }
        pxb_app(b, "    ");
        pxb_app(b, vname);
        pxb_app(b, " : ");
        pxb_app(b, type_name);
        if (!var.self_closing) {
            PxElem iv;
            if (px_next_elem(var.cs, var.ce, "initialValue", &iv)) {
                PxElem sv;
                if (px_next_elem(iv.cs, iv.ce, "simpleValue", &sv)) {
                    char raw[PX_MAX_NAME];
                    px_attr(sv.tag_start, sv.gt, "value", raw, sizeof(raw));
                    if (raw[0]) {
                        char decoded[PX_MAX_NAME];
                        px_decode_attr(raw, decoded, sizeof(decoded));
                        pxb_app(b, " := ");
                        pxb_app(b, decoded);
                    }
                }
            }
        }
        pxb_app(b, ";\n");
    }
}

static void px_emit_var_section(PxBuf *b, const char *cs, const char *ce, const char *kw) {
    pxb_app(b, kw);
    pxb_app(b, "\n");
    px_emit_variable_list(b, cs, ce);
    pxb_app(b, "END_VAR\n");
}

/* Emit a <body><ST><xhtml>...</xhtml></ST></body> payload verbatim
 * (entity-decoded). Any other body kind (<LD>/<FBD>/<SFC>/<CFC>/<IL>, or no
 * <body> at all) is tolerated by emitting nothing — never garbage. */
static void px_emit_body(PxBuf *b, const char *cs, const char *ce) {
    PxElem body;
    if (!px_next_elem(cs, ce, "body", &body)) {
        return;
    }
    PxElem st;
    if (!px_next_elem(body.cs, body.ce, "ST", &st)) {
        return;
    }
    const char *tcs = st.cs;
    const char *tce = st.ce;
    PxElem xh;
    if (!st.self_closing && px_next_elem(st.cs, st.ce, "xhtml", &xh)) {
        tcs = xh.cs;
        tce = xh.ce;
    }
    pxb_app(b, "\n");
    pxb_app_decoded(b, tcs, (int)(tce - tcs));
    pxb_app(b, "\n");
}

/* One <pou> -> one ST unit: declaration header (+ return type for
 * functions), var-section blocks, the body, functionBlock-only actions
 * synthesized as METHODs, and the matching END_* terminator. Returns NULL
 * (via *out) when the POU carries no usable name. */
static void px_emit_pou(CBMArena *arena, const PxElem *pou, char **out) {
    *out = NULL;

    char name[PX_MAX_NAME];
    px_attr(pou->tag_start, pou->gt, "name", name, sizeof(name));
    char pou_type[64];
    px_attr(pou->tag_start, pou->gt, "pouType", pou_type, sizeof(pou_type));

    const char *open_kw = "FUNCTION_BLOCK";
    const char *close_kw = "END_FUNCTION_BLOCK";
    bool is_function = false;
    if (strcasecmp(pou_type, "program") == 0) {
        open_kw = "PROGRAM";
        close_kw = "END_PROGRAM";
    } else if (strcasecmp(pou_type, "function") == 0) {
        open_kw = "FUNCTION";
        close_kw = "END_FUNCTION";
        is_function = true;
    }

    PxBuf b;
    pxb_init(&b, arena, PX_BUF_CAP);
    if (!b.buf) {
        return;
    }

    pxb_app(&b, open_kw);
    pxb_app(&b, " ");
    pxb_app(&b, name[0] ? name : "PlcopenPOU");

    PxElem itf;
    bool has_itf = px_next_elem(pou->cs, pou->ce, "interface", &itf) != NULL;

    if (is_function) {
        char type_name[PX_MAX_NAME];
        bool resolved = false;
        if (has_itf) {
            PxElem rt;
            if (px_next_elem(itf.cs, itf.ce, "returnType", &rt)) {
                resolved = px_resolve_type(rt.cs, rt.ce, type_name, sizeof(type_name));
            }
        }
        /* The grammar requires a colon-typed FUNCTION header; fall back to
         * the generic ANY type when the export carries none. */
        pxb_app(&b, " : ");
        pxb_app(&b, resolved ? type_name : "ANY");
    }
    pxb_app(&b, "\n");

    if (has_itf) {
        static const struct {
            const char *tag;
            const char *kw;
        } sections[] = {
            {"inputVars", "VAR_INPUT"},   {"outputVars", "VAR_OUTPUT"},
            {"inOutVars", "VAR_IN_OUT"},  {"localVars", "VAR"},
            {"tempVars", "VAR_TEMP"},     {"externalVars", "VAR_EXTERNAL"},
            {"globalVars", "VAR_GLOBAL"},
        };
        for (size_t i = 0; i < sizeof(sections) / sizeof(sections[0]); i++) {
            const char *cursor = itf.cs;
            PxElem sec;
            const char *after;
            while ((after = px_next_elem(cursor, itf.ce, sections[i].tag, &sec)) != NULL) {
                px_emit_var_section(&b, sec.cs, sec.ce, sections[i].kw);
                cursor = after;
            }
        }
    }

    px_emit_body(&b, pou->cs, pou->ce);

    /* <actions> has no textual ST equivalent; a parameterless METHOD is
     * synthesized in its place (same substitution twincat_xml.c uses for
     * TwinCAT <Action>). Verified only for functionBlock POUs: the grammar
     * accepts METHOD nested in FUNCTION_BLOCK but rejects it inside PROGRAM,
     * and a "function" POU cannot carry actions in IEC 61131-3. */
    if (strcmp(open_kw, "FUNCTION_BLOCK") == 0) {
        PxElem actions;
        if (px_next_elem(pou->cs, pou->ce, "actions", &actions)) {
            const char *cursor = actions.cs;
            PxElem act;
            const char *after;
            while ((after = px_next_elem(cursor, actions.ce, "action", &act)) != NULL) {
                char aname[PX_MAX_NAME];
                px_attr(act.tag_start, act.gt, "name", aname, sizeof(aname));
                pxb_app(&b, "METHOD ");
                pxb_app(&b, aname[0] ? aname : "PlcopenAction");
                pxb_app(&b, "\n");
                px_emit_body(&b, act.cs, act.ce);
                pxb_app(&b, "END_METHOD\n");
                cursor = after;
            }
        }
    }

    pxb_app(&b, close_kw);
    pxb_app(&b, "\n");

    if (b.pos > 0) {
        *out = b.buf;
    }
}

/* One <dataType> -> one `TYPE X : ... END_TYPE` unit: an elementary/derived
 * alias, or a <struct> of <variable> fields (the grammar requires the `;`
 * after END_STRUCT it does not otherwise emit). Other baseType shapes (enum,
 * subrange, array) are skipped — awkward to synthesize safely and marked
 * optional by the brief. */
static void px_emit_datatype(CBMArena *arena, const PxElem *dt, char **out) {
    *out = NULL;
    char name[PX_MAX_NAME];
    px_attr(dt->tag_start, dt->gt, "name", name, sizeof(name));
    if (!name[0]) {
        return;
    }
    PxElem base;
    if (!px_next_elem(dt->cs, dt->ce, "baseType", &base)) {
        return;
    }

    PxBuf b;
    pxb_init(&b, arena, PX_BUF_CAP);
    if (!b.buf) {
        return;
    }

    pxb_app(&b, "TYPE ");
    pxb_app(&b, name);
    pxb_app(&b, " :\n");

    PxElem st;
    if (px_next_elem(base.cs, base.ce, "struct", &st)) {
        pxb_app(&b, "STRUCT\n");
        px_emit_variable_list(&b, st.cs, st.ce);
        pxb_app(&b, "END_STRUCT;\n");
    } else {
        char type_name[PX_MAX_NAME];
        if (!px_resolve_type(base.cs, base.ce, type_name, sizeof(type_name))) {
            return;
        }
        pxb_app(&b, type_name);
        pxb_app(&b, ";\n");
    }
    pxb_app(&b, "END_TYPE\n");

    if (b.pos > 0) {
        *out = b.buf;
    }
}

char **cbm_plcopen_to_st(CBMArena *arena, const char *xml, int xml_len, int *unit_count) {
    if (unit_count) {
        *unit_count = 0;
    }
    if (!arena || !xml || xml_len <= 0) {
        return NULL;
    }
    /* UTF-16 input would be garbled by byte scanning — reject it. */
    if (xml_len >= 2 && (unsigned char)xml[0] == 0xFF && (unsigned char)xml[1] == 0xFE) {
        return NULL;
    }
    if (xml_len >= 2 && (unsigned char)xml[0] == 0xFE && (unsigned char)xml[1] == 0xFF) {
        return NULL;
    }
    const char *p = xml;
    const char *end = xml + xml_len;
    if (xml_len >= 3 && (unsigned char)xml[0] == 0xEF && (unsigned char)xml[1] == 0xBB &&
        (unsigned char)xml[2] == 0xBF) {
        p += 3; /* UTF-8 BOM */
    }
    if (!px_find(p, end, PLCOPEN_MARKER)) {
        return NULL;
    }

    char *results[PX_MAX_UNITS];
    int count = 0;

    const char *cursor = p;
    PxElem pou;
    const char *after;
    while (count < PX_MAX_UNITS && (after = px_next_elem(cursor, end, "pou", &pou)) != NULL) {
        char *unit = NULL;
        px_emit_pou(arena, &pou, &unit);
        if (unit) {
            results[count++] = unit;
        }
        cursor = after;
    }

    cursor = p;
    PxElem dt;
    while (count < PX_MAX_UNITS && (after = px_next_elem(cursor, end, "dataType", &dt)) != NULL) {
        char *unit = NULL;
        px_emit_datatype(arena, &dt, &unit);
        if (unit) {
            results[count++] = unit;
        }
        cursor = after;
    }

    if (count == 0) {
        return NULL;
    }

    char **arr = (char **)cbm_arena_alloc(arena, (size_t)(count + 1) * sizeof(char *));
    if (!arr) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        arr[i] = results[i];
    }
    arr[count] = NULL;
    if (unit_count) {
        *unit_count = count;
    }
    return arr;
}
