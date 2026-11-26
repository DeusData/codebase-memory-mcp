/*
 * pdf_text.c — content streams, layout, pages and the driver of the PDF
 * text-layer extractor.
 *
 * Text comes out in content order. A word break is a gap of more than 0.15 em
 * along the writing direction (or a step back of more than 0.6 em); a line
 * break is a baseline shift of more than 0.5 em or a change of direction --
 * all measured on device-space glyph geometry. Runs of Hebrew/Arabic letters,
 * drawn in visual order, are reversed into logical order.
 */
#include "pdf_internal.h"

#include "foundation/mem_core.h"
#include "helpers.h"

#include <float.h>
#include <math.h>
#include <string.h>

enum {
    PDF_OPS_MIN = 16,
    PDF_OPS_ARENA = 16 * 1024,
    PDF_TEXT_MIN = 4096,
};

#define PDF_WORD_GAP 0.15
#define PDF_LINE_SHIFT 0.5
#define PDF_BACKSTEP 0.6
#define PDF_SAME_DIR 0.95

static const double IDENT[6] = {1.0, 0.0, 0.0, 1.0, 0.0, 0.0};

/* ── Arithmetic, exactly as the prototype ────────────────────────── */

static void mat_mul(const double *m, const double *n, double *o) {
    double a = m[0];
    double b = m[1];
    double c = m[2];
    double d = m[3];
    double e = m[4];
    double f = m[5];
    double r[6];
    r[0] = a * n[0] + b * n[2];
    r[1] = a * n[1] + b * n[3];
    r[2] = c * n[0] + d * n[2];
    r[3] = c * n[1] + d * n[3];
    r[4] = e * n[0] + f * n[2] + n[4];
    r[5] = e * n[1] + f * n[3] + n[5];
    memcpy(o, r, sizeof(r));
}

typedef struct {
    double hi;
    double lo;
} dl_t;

static dl_t dl_fast_sum(double a, double b) {
    double x = a + b;
    double z = x - a;
    double y = b - z;
    dl_t r = {x, y};
    return r;
}

static dl_t dl_mul(double x, double y) {
    double z = x * y;
    double zz = fma(x, y, -z);
    dl_t r = {z, zz};
    return r;
}

/* CPython's math.hypot for two coordinates (vector_norm): the prototype's
 * value to the last bit, on every platform. */
static double vec_norm2(double x0, double x1, double max) {
    int max_e;
    frexp(max, &max_e);
    if (max_e < -1023) {
        return DBL_MIN * vec_norm2(x0 / DBL_MIN, x1 / DBL_MIN, max / DBL_MIN);
    }
    double scale = ldexp(1.0, -max_e);
    double csum = 1.0;
    double frac1 = 0.0;
    double frac2 = 0.0;
    double v[2] = {x0, x1};
    for (int i = 0; i < 2; i++) {
        double x = v[i] * scale;
        dl_t pr = dl_mul(x, x);
        dl_t sm = dl_fast_sum(csum, pr.hi);
        csum = sm.hi;
        frac1 += pr.lo;
        frac2 += sm.lo;
    }
    double h = sqrt(csum - 1.0 + (frac1 + frac2));
    dl_t pr = dl_mul(-h, h);
    dl_t sm = dl_fast_sum(csum, pr.hi);
    csum = sm.hi;
    frac1 += pr.lo;
    frac2 += sm.lo;
    double x = csum - 1.0 + (frac1 + frac2);
    h += x / (2.0 * h);
    return h / scale;
}

static double py_hypot(double a, double b) {
    double x0 = fabs(a);
    double x1 = fabs(b);
    double max = 0.0;
    bool nan = false;
    if (isnan(x0)) {
        nan = true;
    } else if (x0 > max) {
        max = x0;
    }
    if (isnan(x1)) {
        nan = true;
    } else if (x1 > max) {
        max = x1;
    }
    if (isinf(max)) {
        return max;
    }
    if (nan) {
        return NAN;
    }
    if (max == 0.0) {
        return max;
    }
    return vec_norm2(x0, x1, max);
}

/* ── Page text ───────────────────────────────────────────────────── */

typedef struct {
    char *p;
    size_t n;
    size_t cap;
    bool fail;
    bool has_last;
    double lx;
    double ly;
    double ldx;
    double ldy;
    double lsize;
    int glyphs;
    int unmapped;
} pdf_out_t;

static void out_put(pdf_out_t *o, const char *s, size_t n) {
    if (o->fail || !n) {
        return;
    }
    if (o->n + n + 1 > o->cap) {
        size_t ncap = o->cap ? o->cap : PDF_TEXT_MIN;
        while (ncap < o->n + n + 1) {
            ncap *= 2;
        }
        char *g = (char *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, o->p, ncap);
        if (!g) {
            o->fail = true;
            return;
        }
        o->p = g;
        o->cap = ncap;
    }
    memcpy(o->p + o->n, s, n);
    o->n += n;
}

static void emit(pdf_out_t *o, const char *u, double ox, double oy, double ex, double ey, double dx,
                 double dy, double size) {
    if (o->has_last) {
        if (dx * o->ldx + dy * o->ldy < PDF_SAME_DIR) {
            out_put(o, "\n", 1);
        } else {
            double vx = ox - o->lx;
            double vy = oy - o->ly;
            double along = vx * o->ldx + vy * o->ldy;
            double perp = vy * o->ldx - vx * o->ldy;
            double L = size > o->lsize ? size : o->lsize;
            if (L <= 0) {
                L = 1e-6;
            }
            if (fabs(perp) > PDF_LINE_SHIFT * L) {
                out_put(o, "\n", 1);
            } else if (along > PDF_WORD_GAP * L || along < -PDF_BACKSTEP * L) {
                out_put(o, " ", 1);
            }
        }
    }
    out_put(o, u, strlen(u));
    o->has_last = true;
    o->lx = ex;
    o->ly = ey;
    o->ldx = dx;
    o->ldy = dy;
    o->lsize = size;
}

/* One UTF-8 sequence at s[i] (C0 80 is the NUL marker): its code point and length. */
static uint32_t u8_at(const char *s, size_t n, size_t i, size_t *len) {
    unsigned char c = (unsigned char)s[i];
    if (c < 0x80) {
        *len = 1;
        return c;
    }
    size_t k = c >= 0xF0 ? 4 : c >= 0xE0 ? 3 : 2;
    if (i + k > n) {
        *len = 1;
        return c;
    }
    uint32_t cp = c & (k == 2 ? 0x1FU : k == 3 ? 0x0FU : 0x07U);
    for (size_t j = 1; j < k; j++) {
        cp = (cp << 6) | ((unsigned char)s[i + j] & 0x3FU);
    }
    *len = k;
    return cp;
}

static bool is_rtl(uint32_t cp) {
    return (cp >= 0x0590 && cp <= 0x05FF) || (cp >= 0x0600 && cp <= 0x06FF) ||
           (cp >= 0x0700 && cp <= 0x074F) || (cp >= 0x0750 && cp <= 0x077F) ||
           (cp >= 0x08A0 && cp <= 0x08FF) || (cp >= 0xFB1D && cp <= 0xFDFF) ||
           (cp >= 0xFE70 && cp <= 0xFEFF);
}

/* The prototype's PageText.text(): [ \t ]+ -> " ", " ?\n ?" -> "\n",
 * RTL runs reversed, spaces stripped at both ends; then the NUL markers go. */
static char *page_text(pdf_doc_t *d, pdf_out_t *o, CBMArena *dst, size_t *out_len) {
    const char *s = o->p ? o->p : "";
    size_t n = o->n;
    char *a = (char *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, n + 1);
    char *b = (char *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, n + 1);
    if (!a || !b) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, a);
        cbm_free(CBM_MEM_CLASS_EXTRACT, b);
        d->nomem = true;
        return NULL;
    }
    /* 1: runs of space, tab, no-break space */
    size_t k = 0;
    for (size_t i = 0; i < n;) {
        bool sp = s[i] == ' ' || s[i] == '\t' ||
                  (i + 1 < n && (unsigned char)s[i] == 0xC2 && (unsigned char)s[i + 1] == 0xA0);
        if (!sp) {
            a[k++] = s[i++];
            continue;
        }
        while (i < n &&
               (s[i] == ' ' || s[i] == '\t' ||
                (i + 1 < n && (unsigned char)s[i] == 0xC2 && (unsigned char)s[i + 1] == 0xA0))) {
            i += s[i] == ' ' || s[i] == '\t' ? 1 : 2;
        }
        a[k++] = ' ';
    }
    /* 2: one space either side of a newline */
    size_t m = 0;
    for (size_t i = 0; i < k;) {
        if (a[i] == ' ' && i + 1 < k && a[i + 1] == '\n') {
            b[m++] = '\n';
            i += 2;
            if (i < k && a[i] == ' ') {
                i++;
            }
        } else if (a[i] == '\n') {
            b[m++] = '\n';
            i++;
            if (i < k && a[i] == ' ') {
                i++;
            }
        } else {
            b[m++] = a[i++];
        }
    }
    /* 3: RTL runs ([RTL]+( [RTL]+)*) reversed by code point */
    k = 0;
    for (size_t i = 0; i < m;) {
        size_t l;
        uint32_t cp = u8_at(b, m, i, &l);
        if (!is_rtl(cp)) {
            memcpy(a + k, b + i, l);
            k += l;
            i += l;
            continue;
        }
        size_t j = i;
        for (;;) {
            size_t l2;
            while (j < m && is_rtl(u8_at(b, m, j, &l2))) {
                j += l2;
            }
            if (j + 1 < m && b[j] == ' ' && is_rtl(u8_at(b, m, j + 1, &l2))) {
                j += 1;
                continue;
            }
            break;
        }
        /* write the run's sequences in reverse order */
        size_t end = j;
        size_t w = k + (end - i);
        for (size_t q = i; q < end;) {
            size_t l3;
            u8_at(b, m, q, &l3);
            w -= l3;
            memcpy(a + w, b + q, l3);
            q += l3;
        }
        k += end - i;
        i = end;
    }
    /* 4: strip spaces; 5: drop NUL markers */
    size_t lo = 0;
    size_t hi = k;
    while (lo < hi && a[lo] == ' ') {
        lo++;
    }
    while (hi > lo && a[hi - 1] == ' ') {
        hi--;
    }
    char *res = (char *)cbm_arena_alloc(dst, hi - lo + 1);
    size_t r = 0;
    if (res) {
        for (size_t i = lo; i < hi; i++) {
            if ((unsigned char)a[i] == 0xC0 && i + 1 < hi && (unsigned char)a[i + 1] == 0x80) {
                i++;
                continue;
            }
            res[r++] = a[i];
        }
        res[r] = '\0';
    } else {
        d->nomem = true;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, a);
    cbm_free(CBM_MEM_CLASS_EXTRACT, b);
    *out_len = r;
    return res;
}

/* ── Interpreter ─────────────────────────────────────────────────── */

typedef struct {
    double ctm[6];
    pdf_font_t *font;
    double fs;
    double tc;
    double tw;
    double th;
    double tl;
    double rise;
} pdf_gs_t;

typedef struct {
    bool is_ref;
    uint64_t key;
} pdf_formkey_t;

typedef struct {
    pdf_doc_t *d;
    pdf_out_t out; /* the page being read */
    pdf_formkey_t forms[PDF_MAX_FORM_DEPTH + 2];
    int nforms;
    pdf_map_t forms_read; /* the forms the document has read once */
    size_t reread;        /* decoded bytes of forms read again */
    pdf_glyph_out_t *gbuf;
    size_t gcap;
} pdf_interp_t;

static double onum(const pdf_val_t *v) {
    return (v && v->kind == PV_NUM) ? v->num : 0.0;
}

static void show(pdf_interp_t *it, const pdf_gs_t *gs, double *tm, const pdf_val_t *str) {
    pdf_font_t *font = gs->font;
    if (!font) {
        return;
    }
    pdf_doc_t *d = it->d;
    pdf_out_t *out = &it->out;
    if (str->n > it->gcap) {
        size_t ncap = str->n * 2;
        pdf_glyph_out_t *g =
            (pdf_glyph_out_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, it->gbuf, ncap * sizeof(*g));
        if (!g) {
            d->nomem = true;
            return;
        }
        it->gbuf = g;
        it->gcap = ncap;
    }
    size_t ng = 0;
    pdf_font_decode(d, font, str->s, str->n, it->gbuf, &ng);
    double fs = gs->fs;
    double tc = gs->tc;
    double tw = gs->tw;
    double th = gs->th;
    double rise = gs->rise;
    const double *ctm = gs->ctm;
    double m[6];
    mat_mul(tm, ctm, m);
    double a = m[0];
    double b = m[1];
    double size = fabs(fs) * py_hypot(m[2], m[3]) * pdf_font_em(font);
    double nrm = py_hypot(a, b);
    double dx = 1.0;
    double dy = 0.0;
    if (nrm > 0) {
        dx = a / nrm;
        dy = b / nrm;
    }
    if (th < 0) {
        dx = -dx;
        dy = -dy;
    }
    double e = tm[4];
    double f = tm[5];
    double ta = tm[0];
    double tb = tm[1];
    for (size_t i = 0; i < ng; i++) {
        const pdf_glyph_out_t *g = &it->gbuf[i];
        double adv = (g->width * fs + tc + (g->space ? tw : 0.0)) * th;
        out->glyphs++;
        const char *u = g->utf8;
        if (!u) {
            out->unmapped++;
            u = "\xEF\xBF\xBD";
        }
        if (u[0]) {
            /* the glyph's origin (0, rise) in text space, in device space */
            double ux = rise * tm[2] + e;
            double uy = rise * tm[3] + f;
            double ox = ux * ctm[0] + uy * ctm[2] + ctm[4];
            double oy = ux * ctm[1] + uy * ctm[3] + ctm[5];
            double ex = ox + adv * a; /* where the glyph ends */
            double ey = oy + adv * b;
            emit(out, u, ox, oy, ex, ey, dx, dy, size);
        }
        e += adv * ta;
        f += adv * tb;
    }
    tm[4] = e;
    tm[5] = f;
}

static bool run(pdf_interp_t *it, const unsigned char *data, size_t len, pdf_val_t *resources,
                const double *ctm, int depth);

static bool form_seen(const pdf_interp_t *it, pdf_formkey_t k) {
    for (int i = 0; i < it->nforms; i++) {
        if (it->forms[i].is_ref == k.is_ref && it->forms[i].key == k.key) {
            return true;
        }
    }
    return false;
}

/* false: the form's content could not be read to its end */
static bool do_xobject(pdf_interp_t *it, pdf_val_t *res, const pdf_val_t *name, const pdf_gs_t *gs,
                       int depth) {
    pdf_doc_t *d = it->d;
    pdf_val_t *xd = pdf_dget(d, res, "XObject");
    if (!xd || xd->kind != PV_DICT) {
        return true;
    }
    pdf_val_t *ref = pdf_dfind_n(xd, name->s, name->n);
    if (ref && ref->kind == PV_NULL) {
        ref = NULL;
    }
    pdf_val_t *xo = pdf_resolve(d, ref);
    if (!xo || xo->kind != PV_STREAM) {
        return true;
    }
    pdf_val_t *sub = pdf_dget_raw(xo->stream->dict, "Subtype");
    if (!pdf_is_name(sub, "Form")) {
        return true;
    }
    pdf_formkey_t key = {false, (uint64_t)(uintptr_t)xo};
    if (ref && ref->kind == PV_REF) {
        key.is_ref = true;
        key.key = (uint64_t)ref->ref_num;
    }
    if (form_seen(it, key)) {
        return true;
    }
    size_t dlen = 0;
    const unsigned char *data = pdf_stream_decode(d, xo->stream, &dlen);
    if (!data || !dlen) {
        return true;
    }
    uint64_t once = key.key << 1 | (key.is_ref ? 1U : 0U);
    if (pdf_map_get(&it->forms_read, once) >= 0) {
        if (dlen > PDF_MAX_FORM_REREAD - it->reread) {
            d->counts->form_rereads_cut++;
            return true;
        }
        it->reread += dlen;
    } else if (!pdf_map_put(&it->forms_read, once, 1)) {
        d->nomem = true;
        return false;
    }
    pdf_val_t *mat = pdf_dget(d, xo->stream->dict, "Matrix");
    double m[6];
    memcpy(m, IDENT, sizeof(m));
    if (mat && mat->kind == PV_ARR && mat->count == 6) {
        for (int i = 0; i < 6; i++) {
            m[i] = onum(pdf_resolve(d, mat->items[i]));
        }
    }
    pdf_val_t *fres = pdf_dget_raw(xo->stream->dict, "Resources");
    if (!fres) {
        fres = res;
    }
    double ctm[6];
    mat_mul(m, gs->ctm, ctm);
    if (it->nforms >= (int)(sizeof(it->forms) / sizeof(it->forms[0]))) {
        return true;
    }
    it->forms[it->nforms++] = key;
    bool ok = run(it, data, dlen, fres, ctm, depth + 1);
    it->nforms--;
    return ok;
}

static bool kw_is(const pdf_tok_t *t, const char *s) {
    return pdf_kw_is(t, s);
}

static bool path_paint(const pdf_tok_t *t) {
    return kw_is(t, "S") || kw_is(t, "s") || kw_is(t, "f") || kw_is(t, "F") || kw_is(t, "f*") ||
           kw_is(t, "B") || kw_is(t, "B*") || kw_is(t, "b") || kw_is(t, "b*") || kw_is(t, "sh");
}

/* "[ws]EI" followed by whitespace or the end, from p: the position after EI. */
static size_t skip_inline_image(const unsigned char *data, size_t len, size_t from) {
    for (size_t i = from; i + 3 <= len; i++) {
        unsigned char c = data[i];
        bool ws = c == 0 || c == '\t' || c == '\n' || c == '\f' || c == '\r' || c == ' ';
        if (!ws || data[i + 1] != 'E' || data[i + 2] != 'I') {
            continue;
        }
        size_t e = i + 3;
        if (e == len) {
            return e;
        }
        unsigned char x = data[e];
        if (x == 0 || x == '\t' || x == '\n' || x == '\f' || x == '\r' || x == ' ') {
            return e;
        }
    }
    return len;
}

typedef struct {
    pdf_val_t **v;
    int n;
    int cap;
} ops_t;

static bool ops_push(pdf_doc_t *d, ops_t *o, pdf_val_t *x) {
    if (o->n == o->cap) {
        int ncap = o->cap ? o->cap * 2 : PDF_OPS_MIN;
        pdf_val_t **g = (pdf_val_t **)cbm_realloc(CBM_MEM_CLASS_EXTRACT, (void *)o->v,
                                                  (size_t)ncap * sizeof(*g));
        if (!g) {
            d->nomem = true;
            return false;
        }
        o->v = g;
        o->cap = ncap;
    }
    o->v[o->n++] = x;
    return true;
}

static void tlm_next_line(const pdf_gs_t *gs, double *tm, double *tlm) {
    double e = -gs->tl * tlm[2] + tlm[4];
    double f = -gs->tl * tlm[3] + tlm[5];
    tlm[4] = e;
    tlm[5] = f;
    memcpy(tm, tlm, 6 * sizeof(double));
}

static bool run(pdf_interp_t *it, const unsigned char *data, size_t len, pdf_val_t *resources,
                const double *ctm, int depth) {
    if (depth > PDF_MAX_FORM_DEPTH) {
        return true;
    }
    pdf_doc_t *d = it->d;
    pdf_val_t *res = pdf_resolve(d, resources);
    if (res && res->kind != PV_DICT) {
        res = NULL;
    }
    pdf_gs_t gs;
    memset(&gs, 0, sizeof(gs));
    memcpy(gs.ctm, ctm, sizeof(gs.ctm));
    gs.th = 1.0;
    pdf_gs_t *stack = NULL;
    int nstack = 0;
    int stack_cap = 0;
    double tm[6];
    double tlm[6];
    memcpy(tm, IDENT, sizeof(tm));
    memcpy(tlm, IDENT, sizeof(tlm));
    CBMArena opa;
    cbm_arena_init_lazy(&opa, PDF_OPS_ARENA);
    ops_t ops = {0};
    size_t pos = 0;
    bool ok = true;
    while (pos < len && !d->nomem) {
        d->cur = &opa;
        pdf_tok_t t;
        pdf_lex(d, data, len, &pos, &t);
        if (t.kind == PT_EOF) {
            break;
        }
        pdf_val_t *v = NULL;
        bool operand = true;
        if (t.kind == PT_NUM || t.kind == PT_STR || t.kind == PT_NAME) {
            v = (pdf_val_t *)pdf_calloc(d, sizeof(*v));
            if (v) {
                v->kind = t.kind == PT_NUM ? PV_NUM : t.kind == PT_STR ? PV_STR : PV_NAME;
                v->is_int = t.is_int;
                v->num = t.num;
                v->inum = t.inum;
                v->s = t.s;
                v->n = t.n;
            }
        } else if (t.kind == PT_AS) {
            v = pdf_parse_content_array(d, data, len, &pos, 0);
            if (!v) {
                ok = false;
                break;
            }
        } else if (t.kind == PT_DS) {
            v = pdf_parse_value(d, data, len, &pos, &t, 0);
            if (!v) {
                ok = false;
                break;
            }
        } else if (t.kind != PT_KW) {
            continue;
        } else {
            operand = false;
        }
        d->cur = d->a;
        if (operand) {
            if (v) {
                ops_push(d, &ops, v);
            }
            continue;
        }
        int n = ops.n;
        pdf_val_t **o = ops.v;
        if (kw_is(&t, "Tj") || kw_is(&t, "'") || kw_is(&t, "\"") || kw_is(&t, "TJ")) {
            bool tj = kw_is(&t, "TJ");
            if (kw_is(&t, "\"") && n >= 3) {
                gs.tw = onum(o[n - 3]);
                gs.tc = onum(o[n - 2]);
            }
            if (!kw_is(&t, "Tj") && !tj) {
                tlm_next_line(&gs, tm, tlm);
            }
            if (n == 0) {
                /* nothing shown */
            } else if (tj) {
                const pdf_val_t *arr = o[n - 1];
                for (int i = 0; arr && arr->kind == PV_ARR && i < arr->count; i++) {
                    const pdf_val_t *item = arr->items[i];
                    if (item->kind == PV_STR) {
                        show(it, &gs, tm, item);
                    } else if (item->kind == PV_NUM) {
                        double tx = -item->num / 1000.0 * gs.fs * gs.th;
                        double e = tm[4] + tx * tm[0];
                        double f = tm[5] + tx * tm[1];
                        tm[4] = e;
                        tm[5] = f;
                    }
                }
            } else if (o[n - 1]->kind == PV_STR) {
                show(it, &gs, tm, o[n - 1]);
            }
        } else if (kw_is(&t, "Td") || kw_is(&t, "TD")) {
            if (n >= 2) {
                double tx = onum(o[n - 2]);
                double ty = onum(o[n - 1]);
                if (kw_is(&t, "TD")) {
                    gs.tl = -ty;
                }
                double e = tx * tlm[0] + ty * tlm[2] + tlm[4];
                double f = tx * tlm[1] + ty * tlm[3] + tlm[5];
                tlm[4] = e;
                tlm[5] = f;
                memcpy(tm, tlm, sizeof(tm));
            }
        } else if (kw_is(&t, "Tm")) {
            if (n >= 6) {
                for (int i = 0; i < 6; i++) {
                    tm[i] = onum(o[n - 6 + i]);
                }
                memcpy(tlm, tm, sizeof(tlm));
            }
        } else if (kw_is(&t, "T*")) {
            tlm_next_line(&gs, tm, tlm);
        } else if (kw_is(&t, "Tf")) {
            if (n >= 2) {
                gs.fs = onum(o[n - 1]);
                gs.font = o[n - 2]->kind == PV_NAME ? pdf_font_for(d, res, o[n - 2]) : NULL;
            }
        } else if (kw_is(&t, "BT")) {
            memcpy(tm, IDENT, sizeof(tm));
            memcpy(tlm, IDENT, sizeof(tlm));
        } else if (kw_is(&t, "Tc")) {
            if (n) {
                gs.tc = onum(o[n - 1]);
            }
        } else if (kw_is(&t, "Tw")) {
            if (n) {
                gs.tw = onum(o[n - 1]);
            }
        } else if (kw_is(&t, "Tz")) {
            if (n) {
                gs.th = onum(o[n - 1]) / 100.0;
            }
        } else if (kw_is(&t, "TL")) {
            if (n) {
                gs.tl = onum(o[n - 1]);
            }
        } else if (kw_is(&t, "Ts")) {
            if (n) {
                gs.rise = onum(o[n - 1]);
            }
        } else if (kw_is(&t, "q")) {
            if (nstack == stack_cap) {
                int ncap = stack_cap ? stack_cap * 2 : PDF_OPS_MIN;
                pdf_gs_t *g = (pdf_gs_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, stack,
                                                      (size_t)ncap * sizeof(*g));
                if (!g) {
                    d->nomem = true;
                    break;
                }
                stack = g;
                stack_cap = ncap;
            }
            stack[nstack++] = gs;
        } else if (kw_is(&t, "Q")) {
            if (nstack) {
                gs = stack[--nstack];
            }
        } else if (kw_is(&t, "cm")) {
            if (n >= 6) {
                double m[6];
                for (int i = 0; i < 6; i++) {
                    m[i] = onum(o[n - 6 + i]);
                }
                mat_mul(m, gs.ctm, gs.ctm);
            }
        } else if (kw_is(&t, "Do")) {
            if (n && o[n - 1]->kind == PV_NAME && !do_xobject(it, res, o[n - 1], &gs, depth)) {
                d->counts->op_errors++;
            }
        } else if (kw_is(&t, "BI")) {
            const unsigned char *id =
                pos < len ? (const unsigned char *)cbm_memmem(data + pos, len - pos, "ID", 2)
                          : NULL;
            if (!id) {
                break;
            }
            pos = skip_inline_image(data, len, (size_t)(id - data) + 2);
        } else if (path_paint(&t)) {
            /* vector content: not text */
        }
        ops.n = 0;
        cbm_arena_rewind(&opa);
    }
    d->cur = d->a;
    cbm_free(CBM_MEM_CLASS_EXTRACT, (void *)ops.v);
    cbm_free(CBM_MEM_CLASS_EXTRACT, stack);
    cbm_arena_destroy(&opa);
    return ok;
}

/* ── Pages ───────────────────────────────────────────────────────── */

typedef struct {
    pdf_val_t *node;
    pdf_val_t *res;
    pdf_val_t *box;
} pdf_pageref_t;

typedef struct {
    pdf_val_t *ref;
    pdf_val_t *inh;
    pdf_val_t *box;
    int depth;
} pdf_walk_t;

typedef struct {
    pdf_pageref_t *v;
    int n;
    int cap;
} pagelist_t;

static bool pl_push(pdf_doc_t *d, pagelist_t *l, pdf_val_t *node, pdf_val_t *res, pdf_val_t *box) {
    if (l->n == l->cap) {
        int ncap = l->cap ? l->cap * 2 : PDF_OPS_MIN;
        pdf_pageref_t *g =
            (pdf_pageref_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, l->v, (size_t)ncap * sizeof(*g));
        if (!g) {
            d->nomem = true;
            return false;
        }
        l->v = g;
        l->cap = ncap;
    }
    l->v[l->n].node = node;
    l->v[l->n].res = res;
    l->v[l->n].box = box;
    l->n++;
    return true;
}

/* dict.get(key, dflt): a present key wins even when its value is null. */
static pdf_val_t *dget_or(const pdf_val_t *dict, const char *key, pdf_val_t *dflt) {
    return pdf_dhas(dict, key) ? pdf_dfind(dict, key) : dflt;
}

static int i64cmp(const void *x, const void *y) {
    int64_t a = *(const int64_t *)x;
    int64_t b = *(const int64_t *)y;
    return a < b ? -1 : a > b ? 1 : 0;
}

static void collect_pages(pdf_doc_t *d, pagelist_t *pl) {
    pdf_val_t *root = pdf_resolve(d, pdf_dget_raw(d->trailer, "Root"));
    if (root && root->kind == PV_DICT) {
        pdf_map_t seen_ref = {0};
        pdf_map_t seen_ptr = {0};
        pdf_walk_t *stack = NULL;
        int ns = 0;
        int cap = 0;
#define WALK_PUSH(R, I, B, D)                                                                     \
    do {                                                                                          \
        if (ns == cap) {                                                                          \
            int nc = cap ? cap * 2 : PDF_OPS_MIN;                                                 \
            pdf_walk_t *g =                                                                       \
                (pdf_walk_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, stack, (size_t)nc * sizeof(*g)); \
            if (!g) {                                                                             \
                d->nomem = true;                                                                  \
                goto done;                                                                        \
            }                                                                                     \
            stack = g;                                                                            \
            cap = nc;                                                                             \
        }                                                                                         \
        stack[ns].ref = (R);                                                                      \
        stack[ns].inh = (I);                                                                      \
        stack[ns].box = (B);                                                                      \
        stack[ns].depth = (D);                                                                    \
        ns++;                                                                                     \
    } while (0)
        WALK_PUSH(pdf_dget_raw(root, "Pages"), NULL, NULL, 0);
        while (ns && !d->nomem) {
            pdf_walk_t w = stack[--ns];
            bool seen;
            if (w.ref && w.ref->kind == PV_REF) {
                seen = pdf_map_get(&seen_ref, (uint64_t)w.ref->ref_num) >= 0;
                if (!seen && !pdf_map_put(&seen_ref, (uint64_t)w.ref->ref_num, 1)) {
                    d->nomem = true;
                }
            } else {
                uint64_t key = (uint64_t)(uintptr_t)w.ref;
                seen = pdf_map_get(&seen_ptr, key) >= 0;
                if (!seen && !pdf_map_put(&seen_ptr, key, 1)) {
                    d->nomem = true;
                }
            }
            if (seen || w.depth > PDF_MAX_PAGE_DEPTH) {
                continue;
            }
            pdf_val_t *node = pdf_resolve(d, w.ref);
            if (!node || node->kind != PV_DICT) {
                continue;
            }
            pdf_val_t *res = dget_or(node, "Resources", w.inh);
            pdf_val_t *box = dget_or(node, "CropBox", dget_or(node, "MediaBox", w.box));
            pdf_val_t *kids = pdf_resolve(d, pdf_dget_raw(node, "Kids"));
            pdf_val_t *typ = pdf_dget_raw(node, "Type");
            bool is_arr = kids && kids->kind == PV_ARR;
            if (pdf_is_name(typ, "Pages") || (!pdf_is_name(typ, "Page") && is_arr)) {
                for (int i = is_arr ? kids->count - 1 : -1; i >= 0; i--) {
                    WALK_PUSH(kids->items[i], res, box, w.depth + 1);
                }
            } else {
                pl_push(d, pl, node, res, box);
            }
        }
#undef WALK_PUSH
    done:
        cbm_free(CBM_MEM_CLASS_EXTRACT, stack);
        pdf_map_free(&seen_ref);
        pdf_map_free(&seen_ptr);
    }
    if (pl->n || d->nomem) {
        return;
    }
    int n = d->xref.n;
    int64_t *nums =
        (int64_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)(n ? n : 1) * sizeof(int64_t));
    if (!nums) {
        d->nomem = true;
        return;
    }
    for (int i = 0; i < n; i++) {
        nums[i] = d->xref.v[i].num;
    }
    qsort(nums, (size_t)n, sizeof(int64_t), i64cmp);
    for (int i = 0; i < n; i++) {
        pdf_val_t *o = pdf_get(d, nums[i]);
        if (o && o->kind == PV_DICT && pdf_is_name(pdf_dget_raw(o, "Type"), "Page")) {
            pl_push(d, pl, o, pdf_dget_raw(o, "Resources"),
                    dget_or(o, "CropBox", pdf_dget_raw(o, "MediaBox")));
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, nums);
}

static void page_extract(pdf_doc_t *d, pdf_interp_t *it, const pdf_pageref_t *pg, CBMArena *dst,
                         cbm_pdf_page_t *out) {
    pdf_out_t *o = &it->out;
    memset(o, 0, sizeof(*o));
    it->nforms = 0;
    pdf_val_t *cont = pdf_dget(d, pg->node, "Contents");
    pdf_val_t *single[1] = {cont};
    pdf_val_t **parts = single;
    int nparts = cont ? 1 : 0;
    if (cont && cont->kind == PV_ARR) {
        parts = cont->items;
        nparts = cont->count;
    }
    /* the parts' decoded data joined by "\n" */
    const unsigned char *one = NULL;
    size_t one_len = 0;
    unsigned char *joined = NULL;
    size_t jn = 0;
    int nchunks = 0;
    for (int i = 0; i < nparts && !d->nomem; i++) {
        pdf_val_t *p = pdf_resolve(d, parts[i]);
        if (!p || p->kind != PV_STREAM) {
            continue;
        }
        size_t dl = 0;
        const unsigned char *dd = pdf_stream_decode(d, p->stream, &dl);
        if (!dd || !dl) {
            continue;
        }
        if (nchunks == 0) {
            one = dd;
            one_len = dl;
        } else {
            size_t add = (nchunks == 1 ? one_len : 0) + 1 + dl;
            unsigned char *g =
                (unsigned char *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, joined, jn + add);
            if (!g) {
                d->nomem = true;
                break;
            }
            joined = g;
            if (nchunks == 1) {
                memcpy(joined, one, one_len);
                jn = one_len;
            }
            joined[jn++] = '\n';
            memcpy(joined + jn, dd, dl);
            jn += dl;
        }
        nchunks++;
    }
    const unsigned char *content = nchunks > 1 ? joined : one;
    size_t clen = nchunks > 1 ? jn : one_len;
    if (!run(it, content ? content : (const unsigned char *)"", content ? clen : 0, pg->res, IDENT,
             0)) {
        d->counts->page_errors++;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, joined);
    size_t tl = 0;
    out->text = page_text(d, o, dst, &tl);
    out->len = tl;
    out->glyphs = o->glyphs;
    out->unmapped = o->unmapped;
    if (!out->text) {
        out->text = "";
        out->len = 0;
    }
    if (o->fail) {
        d->nomem = true;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, o->p);
}

bool cbm_pdf_extract(const unsigned char *data, size_t len, CBMArena *out, cbm_pdf_result_t *res) {
    memset(res, 0, sizeof(*res));
    const unsigned char *h = data ? (const unsigned char *)cbm_memmem(data, len, "%PDF-", 5) : NULL;
    if (!h || (size_t)(h - data) > PDF_HEADER_WINDOW) {
        res->status = CBM_PDF_NOT_PDF;
        return true;
    }
    CBMArena arena;
    cbm_arena_init(&arena);
    pdf_doc_t d;
    memset(&d, 0, sizeof(d));
    d.data = data;
    d.len = len;
    d.a = &arena;
    d.cur = &arena;
    d.counts = &res->counts;
    pdf_doc_open(&d);
    pagelist_t pl = {0};
    if (!d.nomem) {
        pdf_val_t *enc = pdf_dget_raw(d.trailer, "Encrypt");
        pdf_val_t *encd = pdf_resolve(&d, enc);
        if (encd && encd->kind == PV_DICT) {
            res->status = CBM_PDF_ENCRYPTED;
            pdf_doc_close(&d);
            cbm_arena_destroy(&arena);
            return true;
        }
        collect_pages(&d, &pl);
    }
    if (!d.nomem && pl.n) {
        res->pages = (cbm_pdf_page_t *)cbm_arena_calloc(out, (size_t)pl.n * sizeof(*res->pages));
        if (!res->pages) {
            d.nomem = true;
        }
    }
    pdf_interp_t it;
    memset(&it, 0, sizeof(it));
    it.d = &d;
    for (int i = 0; i < pl.n && !d.nomem; i++) {
        page_extract(&d, &it, &pl.v[i], out, &res->pages[i]);
        res->npages = i + 1;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, it.gbuf);
    pdf_map_free(&it.forms_read);
    cbm_free(CBM_MEM_CLASS_EXTRACT, pl.v);
    bool nomem = d.nomem;
    pdf_doc_close(&d);
    cbm_arena_destroy(&arena);
    if (nomem) {
        res->status = CBM_PDF_NOMEM;
        res->pages = NULL;
        res->npages = 0;
        return false;
    }
    res->status = res->npages ? CBM_PDF_OK : CBM_PDF_PARSE_FAILURE;
    return true;
}
