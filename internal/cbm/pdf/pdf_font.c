/*
 * pdf_font.c — fonts of the PDF text-layer extractor: what a shown byte
 * string says in Unicode, and how far each glyph advances.
 *
 * Simple fonts map one byte per glyph: ToUnicode first, then the encoding's
 * glyph name (a base encoding, /Differences, a Type1 program's built-in
 * table), then -- for fonts whose encoding cannot be read -- the byte itself
 * when it is printable ASCII. Type0 fonts split bytes by their CMap's code
 * space (or two bytes per code), map through ToUnicode, a UCS-2 CMap, or the
 * embedded TrueType program's cmap read backwards.
 *
 * Ranges (CMap bfrange/cidrange, CID widths) are painted once into disjoint
 * segments that keep the prototype's precedence (the first range written wins
 * a code; the last /W entry wins a CID), so a lookup is a binary search.
 */
#include "pdf_internal.h"

#include "foundation/mem_core.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    PDF_CODE_BYTES = 4,   /* CMap codes longer than this are ignored */
    PDF_BIG_RANGE = 4096, /* bfranges up to this size are written out */
    PDF_GID_SPACE = 65536,
    PDF_UTF8_MAX = 4,
    PDF_INLINE_MAX = 15, /* pdf_glyph_out_t.inl capacity, minus the NUL */
    PDF_STD_FIRST = 0x20,
    PDF_STD_LAST = 0x7E,
    PDF_DEFAULT_WIDTH = 500,
    PDF_COURIER_WIDTH = 600,
    PDF_TYPE1_CLEAR = 65536, /* the prototype's look into a Type1 program without Length1 */
    PDF_FLAG_SYMBOLIC = 4,
    PDF_LIST_MIN = 16,
};

#define PDF_SURR_LO 0xD800U
#define PDF_SURR_HI 0xDFFFU
#define PDF_MAX_CP 0x10FFFFU
#define PDF_REPL 0xFFFDU

/* ── UTF-8 ───────────────────────────────────────────────────────── */

/* One code point as UTF-8. U+0000 is written C0 80 so a glyph that maps to it
 * still counts as text in the layout (the prototype emitted it); the page
 * text drops it at the end. Surrogates become U+FFFD. */
static int utf8_put(uint32_t cp, char *out) {
    if (cp == 0) {
        out[0] = (char)0xC0;
        out[1] = (char)0x80;
        return 2;
    }
    if ((cp >= PDF_SURR_LO && cp <= PDF_SURR_HI) || cp > PDF_MAX_CP) {
        cp = PDF_REPL;
    }
    if (cp < 0x80) {
        out[0] = (char)cp;
        return 1;
    }
    if (cp < 0x800) {
        out[0] = (char)(0xC0 | (cp >> 6));
        out[1] = (char)(0x80 | (cp & 0x3F));
        return 2;
    }
    if (cp < 0x10000) {
        out[0] = (char)(0xE0 | (cp >> 12));
        out[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        out[2] = (char)(0x80 | (cp & 0x3F));
        return 3;
    }
    out[0] = (char)(0xF0 | (cp >> 18));
    out[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
    out[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
    out[3] = (char)(0x80 | (cp & 0x3F));
    return 4;
}

static char *persist(pdf_doc_t *d, const char *s, size_t n) {
    char *o = (char *)cbm_arena_alloc(d->a, n + 1);
    if (!o) {
        d->nomem = true;
        return NULL;
    }
    memcpy(o, s, n);
    o[n] = '\0';
    return o;
}

/* UTF-16BE bytes as UTF-8 (the prototype's _u16be): one byte is a Latin-1
 * character; an odd tail byte is dropped; a lone surrogate is U+FFFD. Writes
 * to buf when it fits, else to the arena. */
static const char *u16be(pdf_doc_t *d, const unsigned char *b, size_t n, char *buf, size_t cap) {
    char tmp[64];
    char *out = tmp;
    size_t need = n * 2 + 4;
    if (need > sizeof(tmp)) {
        out = (char *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, need);
        if (!out) {
            d->nomem = true;
            return NULL;
        }
    }
    size_t k = 0;
    if (n == 1) {
        k = (size_t)utf8_put(b[0], out);
    } else {
        n &= ~(size_t)1;
        for (size_t i = 0; i < n; i += 2) {
            uint32_t u = ((uint32_t)b[i] << 8) | b[i + 1];
            if (u >= PDF_SURR_LO && u <= 0xDBFFU && i + 3 < n) {
                uint32_t lo = ((uint32_t)b[i + 2] << 8) | b[i + 3];
                if (lo >= 0xDC00U && lo <= PDF_SURR_HI) {
                    uint32_t cp = 0x10000U + ((u - PDF_SURR_LO) << 10) + (lo - 0xDC00U);
                    k += (size_t)utf8_put(cp, out + k);
                    i += 2;
                    continue;
                }
            }
            k += (size_t)utf8_put(u, out + k); /* a lone surrogate becomes U+FFFD */
        }
    }
    const char *res;
    if (buf && k < cap) {
        memcpy(buf, out, k);
        buf[k] = '\0';
        res = buf;
    } else {
        res = persist(d, out, k);
    }
    if (out != tmp) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, out);
    }
    return res;
}

/* ── Glyph names ─────────────────────────────────────────────────── */

static const char *glyph_lookup(const unsigned char *name, size_t n) {
    int lo = 0;
    int hi = PDF_GLYPH_COUNT - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        const char *g = PDF_GLYPHS[mid].name;
        size_t gn = strlen(g);
        size_t m = gn < n ? gn : n;
        int c = m ? memcmp(g, name, m) : 0;
        if (!c) {
            c = gn < n ? -1 : gn > n ? 1 : 0;
        }
        if (!c) {
            return PDF_GLYPHS[mid].utf8;
        }
        if (c < 0) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    return NULL;
}

static bool all_hex(const unsigned char *s, size_t n) {
    for (size_t i = 0; i < n; i++) {
        unsigned char c = s[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
            return false;
        }
    }
    return true;
}

static uint32_t hex_num(const unsigned char *s, size_t n) {
    uint32_t v = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = s[i];
        uint32_t x = c <= '9' ? (uint32_t)(c - '0') : (uint32_t)((c | 0x20) - 'a' + 10);
        v = v * 16 + x;
    }
    return v;
}

static const char *g2u(pdf_doc_t *d, const unsigned char *name, size_t n, int depth) {
    const char *r = glyph_lookup(name, n);
    if (r) {
        return r;
    }
    const unsigned char *base = name;
    size_t bn = n;
    const unsigned char *dot = n > 1 ? (const unsigned char *)memchr(name + 1, '.', n - 1) : NULL;
    if (dot) {
        const unsigned char *first = (const unsigned char *)memchr(name, '.', n);
        bn = (size_t)(first - name);
        r = glyph_lookup(base, bn);
        if (r) {
            return r;
        }
        if (!bn) {
            return NULL;
        }
    }
    if (memchr(base, '_', bn)) {
        if (depth > 0) {
            return NULL;
        }
        char tmp[256];
        size_t k = 0;
        char *big = NULL;
        size_t cap = sizeof(tmp);
        char *out = tmp;
        size_t i = 0;
        for (;;) {
            const unsigned char *us = (const unsigned char *)memchr(base + i, '_', bn - i);
            size_t pe = us ? (size_t)(us - base) : bn;
            const char *part = g2u(d, base + i, pe - i, depth + 1);
            if (!part) {
                cbm_free(CBM_MEM_CLASS_EXTRACT, big);
                return NULL;
            }
            size_t pl = strlen(part);
            if (k + pl + 1 > cap) {
                size_t ncap = (k + pl + 1) * 2;
                char *g = (char *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, big, ncap);
                if (!g) {
                    cbm_free(CBM_MEM_CLASS_EXTRACT, big);
                    d->nomem = true;
                    return NULL;
                }
                if (!big) {
                    memcpy(g, tmp, k);
                }
                big = g;
                out = big;
                cap = ncap;
            }
            memcpy(out + k, part, pl);
            k += pl;
            if (!us) {
                break;
            }
            i = pe + 1;
        }
        const char *res = persist(d, out, k);
        cbm_free(CBM_MEM_CLASS_EXTRACT, big);
        return res;
    }
    if (bn >= 7 && memcmp(base, "uni", 3) == 0 && (bn - 3) % 4 == 0 && all_hex(base + 3, bn - 3)) {
        size_t units = (bn - 3) / 4;
        char *o = (char *)cbm_arena_alloc(d->a, units * PDF_UTF8_MAX + 1);
        if (!o) {
            d->nomem = true;
            return NULL;
        }
        size_t k = 0;
        for (size_t i = 3; i < bn; i += 4) {
            uint32_t v = hex_num(base + i, 4);
            if (v >= PDF_SURR_LO && v <= PDF_SURR_HI) {
                return NULL;
            }
            k += (size_t)utf8_put(v, o + k);
        }
        o[k] = '\0';
        return o;
    }
    if (bn >= 5 && bn <= 7 && base[0] == 'u' && all_hex(base + 1, bn - 1)) {
        uint32_t v = hex_num(base + 1, bn - 1);
        if (v <= PDF_MAX_CP && !(v >= PDF_SURR_LO && v <= PDF_SURR_HI)) {
            char b[PDF_UTF8_MAX];
            int k = utf8_put(v, b);
            return persist(d, b, (size_t)k);
        }
    }
    if (bn == 1) {
        char b[PDF_UTF8_MAX];
        int k = utf8_put(base[0], b);
        return persist(d, b, (size_t)k);
    }
    return NULL;
}

const char *pdf_glyph_to_utf8(pdf_doc_t *d, const unsigned char *name, size_t n) {
    return name ? g2u(d, name, n, 0) : NULL;
}

/* ── Interval painting ───────────────────────────────────────────── */

typedef struct {
    uint64_t lo;
    uint64_t hi;
    int idx; /* written order */
} pdf_iv_t;

typedef struct {
    uint64_t lo;
    uint64_t hi;
    int idx;
} pdf_seg_t;

typedef struct {
    pdf_iv_t *v;
    int n;
    int cap;
} iv_list_t;

static bool ivl_push(pdf_doc_t *d, iv_list_t *l, uint64_t lo, uint64_t hi, int idx) {
    if (l->n == l->cap) {
        int ncap = l->cap ? l->cap * 2 : PDF_LIST_MIN;
        pdf_iv_t *g =
            (pdf_iv_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, l->v, (size_t)ncap * sizeof(*g));
        if (!g) {
            d->nomem = true;
            return false;
        }
        l->v = g;
        l->cap = ncap;
    }
    l->v[l->n].lo = lo;
    l->v[l->n].hi = hi;
    l->v[l->n].idx = idx;
    l->n++;
    return true;
}

static int iv_lo_cmp(const void *x, const void *y) {
    const pdf_iv_t *a = (const pdf_iv_t *)x;
    const pdf_iv_t *b = (const pdf_iv_t *)y;
    if (a->lo != b->lo) {
        return a->lo < b->lo ? -1 : 1;
    }
    return a->idx < b->idx ? -1 : a->idx > b->idx ? 1 : 0;
}

static int u64_cmp(const void *x, const void *y) {
    uint64_t a = *(const uint64_t *)x;
    uint64_t b = *(const uint64_t *)y;
    return a < b ? -1 : a > b ? 1 : 0;
}

/* A binary heap of interval positions, ordered by written order (smallest
 * first for first-wins, largest first for last-wins). */
typedef struct {
    int *h;
    int n;
    const pdf_iv_t *iv;
    bool first_wins;
} iv_heap_t;

static bool heap_before(const iv_heap_t *hp, int a, int b) {
    return hp->first_wins ? hp->iv[a].idx < hp->iv[b].idx : hp->iv[a].idx > hp->iv[b].idx;
}

static void heap_push(iv_heap_t *hp, int x) {
    int i = hp->n++;
    hp->h[i] = x;
    while (i > 0) {
        int p = (i - 1) / 2;
        if (!heap_before(hp, hp->h[i], hp->h[p])) {
            break;
        }
        int t = hp->h[i];
        hp->h[i] = hp->h[p];
        hp->h[p] = t;
        i = p;
    }
}

static void heap_pop(iv_heap_t *hp) {
    hp->h[0] = hp->h[--hp->n];
    int i = 0;
    for (;;) {
        int l = 2 * i + 1;
        int r = l + 1;
        int m = i;
        if (l < hp->n && heap_before(hp, hp->h[l], hp->h[m])) {
            m = l;
        }
        if (r < hp->n && heap_before(hp, hp->h[r], hp->h[m])) {
            m = r;
        }
        if (m == i) {
            break;
        }
        int t = hp->h[i];
        hp->h[i] = hp->h[m];
        hp->h[m] = t;
        i = m;
    }
}

/* Disjoint segments, ascending, each owned by the winning interval. The list
 * is consumed (sorted). Intervals must have hi < UINT64_MAX. */
static pdf_seg_t *paint(pdf_doc_t *d, iv_list_t *l, bool first_wins, int *nout) {
    *nout = 0;
    if (!l->n) {
        return NULL;
    }
    int n = l->n;
    qsort(l->v, (size_t)n, sizeof(*l->v), iv_lo_cmp);
    uint64_t *pts = (uint64_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)n * 2 * sizeof(uint64_t));
    int *heap = (int *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)n * sizeof(int));
    pdf_seg_t *seg = (pdf_seg_t *)pdf_big(d, (size_t)n * 2 * sizeof(pdf_seg_t));
    if (!pts || !heap || !seg) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, pts);
        cbm_free(CBM_MEM_CLASS_EXTRACT, heap);
        d->nomem = true;
        return NULL;
    }
    int np = 0;
    for (int i = 0; i < n; i++) {
        pts[np++] = l->v[i].lo;
        pts[np++] = l->v[i].hi + 1;
    }
    qsort(pts, (size_t)np, sizeof(uint64_t), u64_cmp);
    iv_heap_t hp = {heap, 0, l->v, first_wins};
    int next = 0;
    int ns = 0;
    for (int k = 0; k < np; k++) {
        if (k > 0 && pts[k] == pts[k - 1]) {
            continue;
        }
        uint64_t x = pts[k];
        while (next < n && l->v[next].lo <= x) {
            heap_push(&hp, next++);
        }
        while (hp.n && l->v[hp.h[0]].hi < x) {
            heap_pop(&hp);
        }
        if (!hp.n) {
            continue;
        }
        int k2 = k + 1;
        while (k2 < np && pts[k2] == x) {
            k2++;
        }
        if (k2 >= np) {
            break;
        }
        uint64_t end = pts[k2] - 1;
        int w = l->v[hp.h[0]].idx;
        if (ns && seg[ns - 1].idx == w && seg[ns - 1].hi + 1 == x) {
            seg[ns - 1].hi = end;
        } else {
            seg[ns].lo = x;
            seg[ns].hi = end;
            seg[ns].idx = w;
            ns++;
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, pts);
    cbm_free(CBM_MEM_CLASS_EXTRACT, heap);
    *nout = ns;
    return seg;
}

static int seg_find(const pdf_seg_t *s, int n, uint64_t key) {
    int lo = 0;
    int hi = n - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (s[mid].hi < key) {
            lo = mid + 1;
        } else if (s[mid].lo > key) {
            hi = mid - 1;
        } else {
            return s[mid].idx;
        }
    }
    return -1;
}

/* ── CMaps ───────────────────────────────────────────────────────── */

typedef struct {
    uint64_t key; /* bytes << 32 | code */
    const char *utf8;
    int64_t cid;
    int seq;
} cm_ent_t;

typedef struct {
    uint64_t lo;
    uint64_t hi;
    const unsigned char *prefix;
    size_t plen;
    uint32_t last;
} cm_urange_t;

typedef struct {
    uint64_t lo;
    int64_t cid0;
} cm_crange_t;

typedef struct {
    cm_ent_t *uni;
    int nuni;
    cm_urange_t *ur; /* big bfranges in written order */
    pdf_seg_t *urseg;
    int nurseg;
    cm_ent_t *cid;
    int ncid;
    cm_crange_t *cr; /* cidranges in written order */
    pdf_seg_t *crseg;
    int ncrseg;
    pdf_seg_t *space[PDF_CODE_BYTES + 1]; /* code space per byte count (merged) */
    int nspace[PDF_CODE_BYTES + 1];
    bool has_space;
} pdf_cmap_t;

typedef struct {
    cm_ent_t *v;
    int n;
    int cap;
} ent_list_t;

static bool el_push(pdf_doc_t *d, ent_list_t *l, cm_ent_t e) {
    if (l->n == l->cap) {
        int ncap = l->cap ? l->cap * 2 : PDF_LIST_MIN;
        cm_ent_t *g =
            (cm_ent_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, l->v, (size_t)ncap * sizeof(*g));
        if (!g) {
            d->nomem = true;
            return false;
        }
        l->v = g;
        l->cap = ncap;
    }
    e.seq = l->n;
    l->v[l->n++] = e;
    return true;
}

static int ent_cmp(const void *x, const void *y) {
    const cm_ent_t *a = (const cm_ent_t *)x;
    const cm_ent_t *b = (const cm_ent_t *)y;
    if (a->key != b->key) {
        return a->key < b->key ? -1 : 1;
    }
    return a->seq < b->seq ? -1 : a->seq > b->seq ? 1 : 0;
}

/* Sorted, one entry per key: the last written. */
static cm_ent_t *el_finish(pdf_doc_t *d, ent_list_t *l, int *nout) {
    *nout = 0;
    if (!l->n) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, l->v);
        return NULL;
    }
    qsort(l->v, (size_t)l->n, sizeof(*l->v), ent_cmp);
    cm_ent_t *o = (cm_ent_t *)pdf_big(d, (size_t)l->n * sizeof(*o));
    int k = 0;
    if (o) {
        for (int i = 0; i < l->n; i++) {
            if (i + 1 < l->n && l->v[i + 1].key == l->v[i].key) {
                continue;
            }
            o[k++] = l->v[i];
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, l->v);
    *nout = k;
    return o;
}

static const cm_ent_t *ent_find(const cm_ent_t *v, int n, uint64_t key) {
    int lo = 0;
    int hi = n - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (v[mid].key < key) {
            lo = mid + 1;
        } else if (v[mid].key > key) {
            hi = mid - 1;
        } else {
            return &v[mid];
        }
    }
    return NULL;
}

static uint32_t be_code(const unsigned char *s, size_t n) {
    uint32_t v = 0;
    for (size_t i = 0; i < n; i++) {
        v = (v << 8) | s[i];
    }
    return v;
}

static uint64_t cm_key(size_t n, uint32_t code) {
    return ((uint64_t)n << 32) | code;
}

typedef struct {
    ent_list_t uni;
    ent_list_t cid;
    cm_urange_t *ur;
    int nur;
    int cur_cap;
    iv_list_t ur_iv;
    cm_crange_t *cr;
    int ncr;
    int ccap;
    iv_list_t cr_iv;
    iv_list_t space[PDF_CODE_BYTES + 1];
    bool any_space; /* the prototype tested its code-space list for being non-empty */
} cm_build_t;

static bool is_code_str(const pdf_val_t *v) {
    return v && v->kind == PV_STR && v->n >= 1 && v->n <= PDF_CODE_BYTES;
}

static bool is_int_val(const pdf_val_t *v) {
    return v && v->kind == PV_NUM && v->is_int;
}

static void cm_flush(pdf_doc_t *d, cm_build_t *b, const char *state, pdf_val_t **buf, int n) {
    if (strcmp(state, "codespacerange") == 0) {
        for (int i = 0; i + 1 < n; i += 2) {
            const pdf_val_t *lo = buf[i];
            const pdf_val_t *hi = buf[i + 1];
            if (lo && lo->kind == PV_STR && lo->n && hi && hi->kind == PV_STR) {
                b->any_space = true;
            }
            if (is_code_str(lo) && hi && hi->kind == PV_STR) {
                size_t m = lo->n;
                uint64_t a = be_code(lo->s, m);
                uint64_t z = hi->n <= PDF_CODE_BYTES ? be_code(hi->s, hi->n) : UINT32_MAX;
                if (z >= a) {
                    ivl_push(d, &b->space[m], a, z, b->space[m].n);
                }
            }
        }
    } else if (strcmp(state, "bfchar") == 0) {
        for (int i = 0; i + 1 < n; i += 2) {
            const pdf_val_t *s = buf[i];
            const pdf_val_t *dst = buf[i + 1];
            if (!is_code_str(s)) {
                continue;
            }
            cm_ent_t e = {cm_key(s->n, be_code(s->s, s->n)), NULL, 0, 0};
            if (dst && dst->kind == PV_STR) {
                e.utf8 = u16be(d, dst->s, dst->n, NULL, 0);
            } else if (dst && dst->kind == PV_NAME) {
                e.utf8 = g2u(d, dst->s, dst->n, 0);
                if (!e.utf8) {
                    continue;
                }
            } else if (dst && dst->kind == PV_NUM && dst->is_int) {
                char tmp[PDF_UTF8_MAX];
                uint32_t cp = (dst->inum >= 0 && dst->inum <= (int64_t)PDF_MAX_CP)
                                  ? (uint32_t)dst->inum
                                  : PDF_REPL;
                int k = utf8_put(cp, tmp);
                e.utf8 = persist(d, tmp, (size_t)k);
            } else {
                continue;
            }
            if (e.utf8) {
                el_push(d, &b->uni, e);
            }
        }
    } else if (strcmp(state, "bfrange") == 0) {
        for (int i = 0; i + 2 < n; i += 3) {
            const pdf_val_t *lo = buf[i];
            const pdf_val_t *hi = buf[i + 1];
            const pdf_val_t *dst = buf[i + 2];
            if (!is_code_str(lo) || !hi || hi->kind != PV_STR) {
                continue;
            }
            size_t m = lo->n;
            uint64_t a = be_code(lo->s, m);
            uint64_t z = hi->n <= PDF_CODE_BYTES ? be_code(hi->s, hi->n) : UINT32_MAX;
            if (z < a) {
                continue;
            }
            if (dst && dst->kind == PV_ARR) {
                uint64_t cnt = z - a + 1;
                for (int j = 0; j < dst->count && (uint64_t)j < cnt; j++) {
                    const pdf_val_t *x = dst->items[j];
                    if (x && x->kind == PV_STR) {
                        cm_ent_t e = {cm_key(m, (uint32_t)(a + (uint64_t)j)), NULL, 0, 0};
                        e.utf8 = u16be(d, x->s, x->n, NULL, 0);
                        if (e.utf8) {
                            el_push(d, &b->uni, e);
                        }
                    }
                }
            } else if (dst && dst->kind == PV_STR && dst->n) {
                unsigned char two[2] = {0, dst->s[0]};
                const unsigned char *db = dst->s;
                size_t dn = dst->n;
                if (dn == 1) {
                    db = two;
                    dn = 2;
                }
                uint32_t last = ((uint32_t)db[dn - 2] << 8) | db[dn - 1];
                size_t plen = dn - 2;
                if (z - a <= PDF_BIG_RANGE) {
                    unsigned char tmp[64];
                    unsigned char *w =
                        plen + 2 <= sizeof(tmp)
                            ? tmp
                            : (unsigned char *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, plen + 2);
                    if (!w) {
                        d->nomem = true;
                        return;
                    }
                    memcpy(w, db, plen);
                    for (uint64_t j = 0; j <= z - a; j++) {
                        uint32_t u = (last + (uint32_t)j) & 0xFFFFU;
                        w[plen] = (unsigned char)(u >> 8);
                        w[plen + 1] = (unsigned char)u;
                        cm_ent_t e = {cm_key(m, (uint32_t)(a + j)), NULL, 0, 0};
                        e.utf8 = u16be(d, w, plen + 2, NULL, 0);
                        if (!e.utf8 || !el_push(d, &b->uni, e)) {
                            break;
                        }
                    }
                    if (w != tmp) {
                        cbm_free(CBM_MEM_CLASS_EXTRACT, w);
                    }
                } else {
                    if (b->nur == b->cur_cap) {
                        int ncap = b->cur_cap ? b->cur_cap * 2 : PDF_LIST_MIN;
                        cm_urange_t *g = (cm_urange_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, b->ur,
                                                                    (size_t)ncap * sizeof(*g));
                        if (!g) {
                            d->nomem = true;
                            return;
                        }
                        b->ur = g;
                        b->cur_cap = ncap;
                    }
                    cm_urange_t *r = &b->ur[b->nur];
                    r->lo = cm_key(m, (uint32_t)a);
                    r->hi = cm_key(m, (uint32_t)z);
                    /* the token lives in the parse arena: keep a copy */
                    r->prefix = (const unsigned char *)persist(d, (const char *)db, plen);
                    if (!r->prefix) {
                        return;
                    }
                    r->plen = plen;
                    r->last = last;
                    ivl_push(d, &b->ur_iv, r->lo, r->hi, b->nur);
                    b->nur++;
                }
            }
        }
    } else if (strcmp(state, "cidrange") == 0 || strcmp(state, "notdefrange") == 0) {
        for (int i = 0; i + 2 < n; i += 3) {
            const pdf_val_t *lo = buf[i];
            const pdf_val_t *hi = buf[i + 1];
            const pdf_val_t *c = buf[i + 2];
            if (!is_code_str(lo) || !hi || hi->kind != PV_STR || !is_int_val(c)) {
                continue;
            }
            size_t m = lo->n;
            uint64_t a = be_code(lo->s, m);
            uint64_t z = hi->n <= PDF_CODE_BYTES ? be_code(hi->s, hi->n) : UINT32_MAX;
            if (b->ncr == b->ccap) {
                int ncap = b->ccap ? b->ccap * 2 : PDF_LIST_MIN;
                cm_crange_t *g = (cm_crange_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, b->cr,
                                                            (size_t)ncap * sizeof(*g));
                if (!g) {
                    d->nomem = true;
                    return;
                }
                b->cr = g;
                b->ccap = ncap;
            }
            b->cr[b->ncr].lo = cm_key(m, (uint32_t)a);
            b->cr[b->ncr].cid0 = c->inum;
            if (z >= a) {
                ivl_push(d, &b->cr_iv, cm_key(m, (uint32_t)a), cm_key(m, (uint32_t)z), b->ncr);
            }
            b->ncr++;
        }
    } else if (strcmp(state, "cidchar") == 0) {
        for (int i = 0; i + 1 < n; i += 2) {
            const pdf_val_t *s = buf[i];
            const pdf_val_t *c = buf[i + 1];
            if (is_code_str(s) && is_int_val(c)) {
                cm_ent_t e = {cm_key(s->n, be_code(s->s, s->n)), NULL, c->inum, 0};
                el_push(d, &b->cid, e);
            }
        }
    }
}

static const char *const CM_BEGIN[] = {"begincodespacerange", "beginbfchar",  "beginbfrange",
                                       "begincidrange",       "begincidchar", "beginnotdefrange"};

static pdf_cmap_t *cmap_parse(pdf_doc_t *d, const unsigned char *data, size_t len) {
    if (!d->scratch_arena_live) {
        cbm_arena_init_lazy(&d->scratch_arena, CBM_ARENA_DEFAULT_BLOCK_SIZE);
        d->scratch_arena_live = true;
    }
    CBMArena *tmp = &d->scratch_arena;
    CBMArena *saved = d->cur;
    d->cur = tmp;
    cm_build_t b;
    memset(&b, 0, sizeof(b));
    pdf_val_t **buf = NULL;
    int nbuf = 0;
    int cap = 0;
    char state[32] = "";
    bool in_state = false;
    bool failed = false;
    size_t pos = 0;
    for (;;) {
        pdf_tok_t t;
        pdf_lex(d, data, len, &pos, &t);
        if (t.kind == PT_EOF || d->nomem) {
            break;
        }
        if (t.kind == PT_KW) {
            bool begun = false;
            for (size_t i = 0; i < sizeof(CM_BEGIN) / sizeof(CM_BEGIN[0]); i++) {
                if (pdf_kw_is(&t, CM_BEGIN[i])) {
                    snprintf(state, sizeof(state), "%s", CM_BEGIN[i] + 5);
                    in_state = true;
                    nbuf = 0;
                    begun = true;
                    break;
                }
            }
            if (!begun && in_state && t.n >= 3 && memcmp(t.s, "end", 3) == 0) {
                d->cur = saved;
                cm_flush(d, &b, state, buf, nbuf);
                d->cur = tmp;
                in_state = false;
                nbuf = 0;
                cbm_arena_rewind(tmp);
            }
            continue;
        }
        if (!in_state) {
            continue;
        }
        pdf_val_t *v = NULL;
        if (t.kind == PT_STR || t.kind == PT_NUM || t.kind == PT_NAME) {
            v = (pdf_val_t *)pdf_calloc(d, sizeof(*v));
            if (v) {
                v->kind = t.kind == PT_STR ? PV_STR : t.kind == PT_NUM ? PV_NUM : PV_NAME;
                v->is_int = t.is_int;
                v->num = t.num;
                v->inum = t.inum;
                v->s = t.s;
                v->n = t.n;
            }
        } else if (t.kind == PT_AS) {
            v = pdf_parse_content_array(d, data, len, &pos, 0);
            if (!v) {
                failed = true;
                break;
            }
        } else {
            continue;
        }
        if (!v) {
            break;
        }
        if (nbuf == cap) {
            int ncap = cap ? cap * 2 : PDF_LIST_MIN;
            pdf_val_t **g = (pdf_val_t **)cbm_realloc(CBM_MEM_CLASS_EXTRACT, (void *)buf,
                                                      (size_t)ncap * sizeof(*g));
            if (!g) {
                d->nomem = true;
                break;
            }
            buf = g;
            cap = ncap;
        }
        buf[nbuf++] = v;
    }
    d->cur = saved;
    cbm_free(CBM_MEM_CLASS_EXTRACT, (void *)buf);
    cbm_arena_rewind(tmp);
    pdf_cmap_t *cm = NULL;
    if (!failed && !d->nomem) {
        cm = (pdf_cmap_t *)cbm_arena_calloc(d->a, sizeof(*cm));
    }
    if (cm) {
        cm->uni = el_finish(d, &b.uni, &cm->nuni);
        cm->cid = el_finish(d, &b.cid, &cm->ncid);
        b.uni.v = NULL;
        b.cid.v = NULL;
        if (b.nur) {
            cm->ur = (cm_urange_t *)pdf_big(d, (size_t)b.nur * sizeof(*cm->ur));
            if (cm->ur) {
                memcpy(cm->ur, b.ur, (size_t)b.nur * sizeof(*cm->ur));
            }
            cm->urseg = paint(d, &b.ur_iv, true, &cm->nurseg);
        }
        if (b.ncr) {
            cm->cr = (cm_crange_t *)pdf_big(d, (size_t)b.ncr * sizeof(*cm->cr));
            if (cm->cr) {
                memcpy(cm->cr, b.cr, (size_t)b.ncr * sizeof(*cm->cr));
            }
            cm->crseg = paint(d, &b.cr_iv, true, &cm->ncrseg);
        }
        for (int m = 1; m <= PDF_CODE_BYTES; m++) {
            if (b.space[m].n) {
                cm->space[m] = paint(d, &b.space[m], true, &cm->nspace[m]);
            }
        }
        cm->has_space = b.any_space;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, b.uni.v);
    cbm_free(CBM_MEM_CLASS_EXTRACT, b.cid.v);
    cbm_free(CBM_MEM_CLASS_EXTRACT, b.ur);
    cbm_free(CBM_MEM_CLASS_EXTRACT, b.ur_iv.v);
    cbm_free(CBM_MEM_CLASS_EXTRACT, b.cr);
    cbm_free(CBM_MEM_CLASS_EXTRACT, b.cr_iv.v);
    for (int m = 0; m <= PDF_CODE_BYTES; m++) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, b.space[m].v);
    }
    return d->nomem ? NULL : cm;
}

/* ToUnicode: an exact entry, else the first big range holding the code. */
static const char *cmap_lookup(pdf_doc_t *d, const pdf_cmap_t *cm, size_t n, uint32_t code,
                               char *buf, size_t cap) {
    uint64_t key = cm_key(n, code);
    const cm_ent_t *e = ent_find(cm->uni, cm->nuni, key);
    if (e) {
        return e->utf8;
    }
    int r = seg_find(cm->urseg, cm->nurseg, key);
    if (r < 0) {
        return NULL;
    }
    const cm_urange_t *u = &cm->ur[r];
    uint32_t v = (u->last + (uint32_t)(key - u->lo)) & 0xFFFFU;
    unsigned char tmp[64];
    unsigned char *w = u->plen + 2 <= sizeof(tmp)
                           ? tmp
                           : (unsigned char *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, u->plen + 2);
    if (!w) {
        d->nomem = true;
        return NULL;
    }
    memcpy(w, u->prefix, u->plen);
    w[u->plen] = (unsigned char)(v >> 8);
    w[u->plen + 1] = (unsigned char)v;
    const char *res = u16be(d, w, u->plen + 2, buf, cap);
    if (w != tmp) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, w);
    }
    return res;
}

static bool cmap_to_cid(const pdf_cmap_t *cm, size_t n, uint32_t code, int64_t *cid) {
    uint64_t key = cm_key(n, code);
    const cm_ent_t *e = ent_find(cm->cid, cm->ncid, key);
    if (e) {
        *cid = e->cid;
        return true;
    }
    int r = seg_find(cm->crseg, cm->ncrseg, key);
    if (r < 0) {
        return false;
    }
    *cid = pdf_sat_add(cm->cr[r].cid0, (int64_t)(key - cm->cr[r].lo));
    return true;
}

/* ── TrueType cmap, read backwards ───────────────────────────────── */

static bool rd16(const unsigned char *p, size_t len, size_t at, uint32_t *v) {
    if (at > len || len - at < 2) {
        return false;
    }
    *v = ((uint32_t)p[at] << 8) | p[at + 1];
    return true;
}

static bool rd32(const unsigned char *p, size_t len, size_t at, uint32_t *v) {
    if (at > len || len - at < 4) {
        return false;
    }
    *v = ((uint32_t)p[at] << 24) | ((uint32_t)p[at + 1] << 16) | ((uint32_t)p[at + 2] << 8) |
         p[at + 3];
    return true;
}

/* "next gid not yet assigned, from x" with path halving; nxt has 65537 slots */
static uint32_t dsu_next(uint32_t *nxt, uint32_t x) {
    while (nxt[x] != x) {
        nxt[x] = nxt[nxt[x]];
        x = nxt[x];
    }
    return x;
}

static void dsu_take(uint32_t *nxt, uint32_t x) {
    nxt[x] = x + 1;
}

typedef struct {
    uint32_t pe;
    uint64_t off;
    int rank;
    int order;
} ttf_sub_t;

static int ttf_sub_cmp(const void *x, const void *y) {
    const ttf_sub_t *a = (const ttf_sub_t *)x;
    const ttf_sub_t *b = (const ttf_sub_t *)y;
    if (a->rank != b->rank) {
        return a->rank < b->rank ? -1 : 1;
    }
    return a->order < b->order ? -1 : 1;
}

/* Fill rev[gid] = code point + 1 from one subtable; false: a read past the end
 * (the prototype then had no map at all). */
static bool ttf_fmt4(const unsigned char *p, size_t len, size_t so, uint32_t *rev, uint32_t *nxt,
                     bool *any) {
    uint32_t segx2;
    if (!rd16(p, len, so + 6, &segx2)) {
        return false;
    }
    uint32_t seg = segx2 / 2;
    size_t ends = so + 14;
    size_t starts = so + 16 + segx2;
    size_t deltas = so + 16 + 2 * (size_t)segx2;
    size_t ro_pos = so + 16 + 3 * (size_t)segx2;
    /* the prototype unpacked each array from a slice of segx2 bytes: an odd
     * count or a short slice failed the whole map */
    if (seg && ((segx2 & 1U) || ro_pos + segx2 > len)) {
        return false;
    }
    for (uint32_t s = 0; s < seg; s++) {
        uint32_t end;
        uint32_t start;
        uint32_t delta;
        uint32_t ro;
        /* in range by the check above; a failed read fails the map like the
         * prototype's short slice (and gcc -O2 sees every value set) */
        if (!rd16(p, len, ends + 2 * (size_t)s, &end) ||
            !rd16(p, len, starts + 2 * (size_t)s, &start) ||
            !rd16(p, len, deltas + 2 * (size_t)s, &delta) ||
            !rd16(p, len, ro_pos + 2 * (size_t)s, &ro)) {
            return false;
        }
        uint32_t last = end < 0xFFFEU ? end : 0xFFFEU;
        if (start > last) {
            continue;
        }
        if (ro == 0) {
            /* gid = (c + delta) & 0xFFFF walks gids upward; assign only those
             * still free (the first assignment wins) */
            uint32_t g0 = (start + delta) & 0xFFFFU;
            uint32_t span = last - start + 1;
            uint32_t done = 0;
            while (done < span) {
                uint32_t g = (g0 + done) & 0xFFFFU;
                uint32_t piece =
                    (PDF_GID_SPACE - g) < (span - done) ? (PDF_GID_SPACE - g) : (span - done);
                uint32_t x = dsu_next(nxt, g);
                while (x < g + piece) {
                    if (x != 0) {
                        rev[x] = start + done + (x - g) + 1;
                        *any = true;
                    }
                    dsu_take(nxt, x);
                    x = dsu_next(nxt, x);
                }
                done += piece;
            }
        } else {
            for (uint32_t c = start; c <= last; c++) {
                size_t a = ro_pos + 2 * (size_t)s + ro + 2 * (size_t)(c - start);
                uint32_t g = 0;
                if (a + 2 <= len) {
                    g = ((uint32_t)p[a] << 8) | p[a + 1];
                }
                if (g) {
                    g = (g + delta) & 0xFFFFU;
                }
                if (g && !rev[g] && nxt[g] == g) {
                    rev[g] = c + 1;
                    dsu_take(nxt, g);
                    *any = true;
                }
            }
        }
    }
    return true;
}

static bool ttf_fmt12(const unsigned char *p, size_t len, size_t so, uint32_t *rev, uint32_t *nxt,
                      bool *any) {
    uint32_t ng;
    if (!rd32(p, len, so + 12, &ng)) {
        return false;
    }
    if ((uint64_t)so + 16 + (uint64_t)ng * 12 > len) {
        return false;
    }
    for (uint32_t i = 0; i < ng; i++) {
        uint32_t a;
        uint32_t b;
        uint32_t g0;
        size_t at = so + 16 + 12 * (size_t)i;
        /* in range by the check above (gcc -O2 sees every value set) */
        if (!rd32(p, len, at, &a) || !rd32(p, len, at + 4, &b) || !rd32(p, len, at + 8, &g0)) {
            return false;
        }
        uint64_t cmax = (uint64_t)a + PDF_GID_SPACE;
        if ((uint64_t)b < cmax) {
            cmax = b;
        }
        if (cmax > PDF_MAX_CP) {
            cmax = PDF_MAX_CP;
        }
        if ((uint64_t)a > cmax || g0 >= PDF_GID_SPACE) {
            continue;
        }
        uint64_t glast = (uint64_t)g0 + (cmax - a);
        if (glast >= PDF_GID_SPACE) {
            glast = PDF_GID_SPACE - 1;
        }
        uint32_t x = dsu_next(nxt, g0);
        while (x <= glast) {
            rev[x] = a + (x - g0) + 1;
            *any = true;
            dsu_take(nxt, x);
            x = dsu_next(nxt, x);
        }
    }
    return true;
}

/* The prototype's ttf_reverse_cmap: gid -> code point + 1 (0: none), or NULL. */
static uint32_t *ttf_reverse(pdf_doc_t *d, const unsigned char *p, size_t len) {
    uint32_t num;
    if (!rd16(p, len, 4, &num)) {
        return NULL;
    }
    size_t off = 0;
    bool found = false;
    for (uint32_t i = 0; i < num; i++) {
        size_t r = 12 + 16 * (size_t)i;
        if (r + 4 <= len && memcmp(p + r, "cmap", 4) == 0) {
            uint32_t o;
            if (!rd32(p, len, r + 8, &o)) {
                return NULL;
            }
            off = o;
            found = true;
            break;
        }
        if (r >= len) {
            break;
        }
    }
    if (!found) {
        return NULL;
    }
    uint32_t n;
    if (!rd16(p, len, off + 2, &n)) {
        return NULL;
    }
    ttf_sub_t *subs =
        (ttf_sub_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)(n ? n : 1) * sizeof(*subs));
    uint32_t *rev = (uint32_t *)pdf_big(d, PDF_GID_SPACE * sizeof(uint32_t));
    uint32_t *nxt =
        (uint32_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (PDF_GID_SPACE + 1) * sizeof(uint32_t));
    if (!subs || !rev || !nxt) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, subs);
        cbm_free(CBM_MEM_CLASS_EXTRACT, nxt);
        d->nomem = true;
        return NULL;
    }
    bool bad = false;
    for (uint32_t i = 0; i < n; i++) {
        uint32_t pl;
        uint32_t en;
        uint32_t o;
        size_t at = off + 4 + 8 * (size_t)i;
        if (!rd16(p, len, at, &pl) || !rd16(p, len, at + 2, &en) || !rd32(p, len, at + 4, &o)) {
            bad = true;
            break;
        }
        subs[i].pe = (pl << 16) | en;
        subs[i].off = (uint64_t)off + o;
        subs[i].order = (int)i;
        uint32_t pe = subs[i].pe;
        subs[i].rank = pe == 0x0003000AU   ? 0
                       : pe == 0x00030001U ? 1
                       : pe == 0x00000004U ? 2
                       : pe == 0x00000003U ? 3
                                           : 9;
    }
    uint32_t *res = NULL;
    if (!bad) {
        qsort(subs, n, sizeof(*subs), ttf_sub_cmp);
        for (uint32_t i = 0; i < n && !res; i++) {
            uint32_t pe = subs[i].pe;
            if (!(pe == 0x0003000AU || pe == 0x00030001U || pe == 0x00000004U ||
                  pe == 0x00000003U || pe == 0x00000006U || pe == 0x00000000U ||
                  pe == 0x00000001U)) {
                continue;
            }
            if (subs[i].off > len) {
                break;
            }
            size_t so = (size_t)subs[i].off;
            uint32_t fmt;
            if (!rd16(p, len, so, &fmt)) {
                break;
            }
            memset(rev, 0, PDF_GID_SPACE * sizeof(uint32_t));
            for (uint32_t g = 0; g <= PDF_GID_SPACE; g++) {
                nxt[g] = g;
            }
            bool any = false;
            bool ok = true;
            if (fmt == 4) {
                ok = ttf_fmt4(p, len, so, rev, nxt, &any);
            } else if (fmt == 12) {
                ok = ttf_fmt12(p, len, so, rev, nxt, &any);
            }
            if (!ok) {
                break;
            }
            if (any) {
                res = rev;
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, subs);
    cbm_free(CBM_MEM_CLASS_EXTRACT, nxt);
    return res;
}

/* ── Fonts ───────────────────────────────────────────────────────── */

typedef enum {
    BASE_NONE = 0,
    BASE_TABLE, /* a base encoding, code -> UTF-8 */
    BASE_NAMES, /* a Type1 program's built-in table, code -> glyph name */
} pdf_base_kind_t;

struct pdf_font {
    bool multibyte;
    bool type3;
    double em;
    double wscale;
    pdf_cmap_t *tounicode;
    /* simple fonts */
    const char *map1[256];
    double widths[256];
    bool has_width[256];
    double missing;
    bool has_missing;
    bool std14;
    const unsigned short *std14_tab; /* NULL with std14: Courier */
    /* Type0 */
    pdf_cmap_t *enc;
    bool ucs2;
    bool cid_type2;
    double dw;
    pdf_seg_t *wseg;
    int nwseg;
    double *wval;
    pdf_val_t *fdesc;
    pdf_val_t *c2g_val;
    bool rev_done;
    uint32_t *rev;
    const unsigned char *c2g;
    size_t c2g_len;
};

static const char ASCII1[128][2] = {
#define A(c) {(char)(c), 0}
    A(0),   A(1),   A(2),   A(3),   A(4),   A(5),   A(6),   A(7),   A(8),   A(9),   A(10),  A(11),
    A(12),  A(13),  A(14),  A(15),  A(16),  A(17),  A(18),  A(19),  A(20),  A(21),  A(22),  A(23),
    A(24),  A(25),  A(26),  A(27),  A(28),  A(29),  A(30),  A(31),  A(32),  A(33),  A(34),  A(35),
    A(36),  A(37),  A(38),  A(39),  A(40),  A(41),  A(42),  A(43),  A(44),  A(45),  A(46),  A(47),
    A(48),  A(49),  A(50),  A(51),  A(52),  A(53),  A(54),  A(55),  A(56),  A(57),  A(58),  A(59),
    A(60),  A(61),  A(62),  A(63),  A(64),  A(65),  A(66),  A(67),  A(68),  A(69),  A(70),  A(71),
    A(72),  A(73),  A(74),  A(75),  A(76),  A(77),  A(78),  A(79),  A(80),  A(81),  A(82),  A(83),
    A(84),  A(85),  A(86),  A(87),  A(88),  A(89),  A(90),  A(91),  A(92),  A(93),  A(94),  A(95),
    A(96),  A(97),  A(98),  A(99),  A(100), A(101), A(102), A(103), A(104), A(105), A(106), A(107),
    A(108), A(109), A(110), A(111), A(112), A(113), A(114), A(115), A(116), A(117), A(118), A(119),
    A(120), A(121), A(122), A(123), A(124), A(125), A(126), A(127),
#undef A
};

static bool name_eq(const pdf_val_t *v, const char *s) {
    return pdf_is_name(v, s);
}

static bool bytes_starts(const unsigned char *s, size_t n, const char *pre) {
    size_t m = strlen(pre);
    return n >= m && memcmp(s, pre, m) == 0;
}

static bool std14_name(const unsigned char *s, size_t n) {
    for (int i = 0; PDF_STD14[i]; i++) {
        size_t m = strlen(PDF_STD14[i]);
        if (m == n && memcmp(PDF_STD14[i], s, n) == 0) {
            return true;
        }
    }
    return false;
}

static bool py_space(unsigned char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

static bool py_digit(unsigned char c) {
    return c >= '0' && c <= '9';
}

/* A name character of the prototype's "dup N /name put" pattern. */
static bool t1_name_char(unsigned char c) {
    return !py_space(c) && c != '/' && c != '[' && c != ']' && c != '{' && c != '}' && c != '(' &&
           c != ')' && c != '<' && c != '>';
}

/* The built-in encoding of a Type1 program: names per code (NULL: none), or
 * STANDARD (*standard), or neither (false). */
static bool type1_builtin(pdf_doc_t *d, pdf_val_t *ff, const unsigned char **names, size_t *nlen,
                          bool *standard) {
    *standard = false;
    if (!ff || ff->kind != PV_STREAM) {
        return false;
    }
    size_t len = 0;
    const unsigned char *data = pdf_stream_decode(d, ff->stream, &len);
    if (!data || !len) {
        return false;
    }
    pdf_val_t *l1 = pdf_resolve(d, pdf_dget_raw(ff->stream->dict, "Length1"));
    size_t clear = len < PDF_TYPE1_CLEAR ? len : PDF_TYPE1_CLEAR;
    if (l1 && l1->kind == PV_NUM && l1->is_int && l1->inum > 0 && (uint64_t)l1->inum <= len) {
        clear = (size_t)l1->inum;
    }
    /* /Encoding\s+StandardEncoding\s+def */
    for (size_t i = 0; i + 9 <= clear; i++) {
        if (data[i] != '/' || memcmp(data + i, "/Encoding", 9) != 0) {
            continue;
        }
        size_t p = i + 9;
        size_t w = p;
        while (p < clear && py_space(data[p])) {
            p++;
        }
        if (p == w || clear - p < 16 || memcmp(data + p, "StandardEncoding", 16) != 0) {
            continue;
        }
        p += 16;
        w = p;
        while (p < clear && py_space(data[p])) {
            p++;
        }
        if (p == w || clear - p < 3 || memcmp(data + p, "def", 3) != 0) {
            continue;
        }
        *standard = true;
        return true;
    }
    /* dup\s+(\d+)\s*\/([^\s/\[\]{}()<>]+)\s+put */
    bool hit = false;
    size_t i = 0;
    while (i + 3 <= clear) {
        if (data[i] != 'd' || memcmp(data + i, "dup", 3) != 0) {
            i++;
            continue;
        }
        size_t p = i + 3;
        size_t w = p;
        while (p < clear && py_space(data[p])) {
            p++;
        }
        size_t ds = p;
        while (p < clear && py_digit(data[p])) {
            p++;
        }
        bool ok = ds > w && p > ds;
        size_t de = p;
        while (ok && p < clear && py_space(data[p])) {
            p++;
        }
        ok = ok && p < clear && data[p] == '/';
        size_t ns = p + 1;
        if (ok) {
            p = ns;
            while (p < clear && t1_name_char(data[p])) {
                p++;
            }
            ok = p > ns;
        }
        size_t ne = p;
        if (ok) {
            size_t w2 = p;
            while (p < clear && py_space(data[p])) {
                p++;
            }
            ok = p > w2 && clear - p >= 3 && memcmp(data + p, "put", 3) == 0;
        }
        if (!ok) {
            i++;
            continue;
        }
        uint64_t c = 0;
        for (size_t k = ds; k < de && c < 256; k++) {
            c = c * 10 + (uint64_t)(data[k] - '0');
        }
        if (c < 256) {
            names[c] = data + ns;
            nlen[c] = ne - ns;
            hit = true;
        }
        i = p + 3;
    }
    return hit;
}

static const char *const *base_table(const pdf_val_t *name) {
    if (name_eq(name, "StandardEncoding")) {
        return PDF_ENC_STANDARD;
    }
    if (name_eq(name, "WinAnsiEncoding")) {
        return PDF_ENC_WINANSI;
    }
    if (name_eq(name, "MacRomanEncoding")) {
        return PDF_ENC_MACROMAN;
    }
    if (name_eq(name, "PDFDocEncoding")) {
        return PDF_ENC_PDFDOC;
    }
    return NULL;
}

static int dbl_cmp(const void *x, const void *y) {
    double a = *(const double *)x;
    double b = *(const double *)y;
    return a < b ? -1 : a > b ? 1 : 0;
}

static pdf_val_t *font_desc(pdf_doc_t *d, pdf_val_t *fd) {
    pdf_val_t *v = pdf_dget(d, fd, "FontDescriptor");
    return (v && v->kind == PV_DICT) ? v : NULL;
}

/* Which of FontFile, FontFile2, FontFile3 is present (the last listed wins). */
static const char *font_prog(const pdf_val_t *fdesc) {
    const char *prog = NULL;
    static const char *const keys[] = {"FontFile", "FontFile2", "FontFile3"};
    for (int i = 0; i < 3; i++) {
        if (pdf_dget_raw(fdesc, keys[i])) {
            prog = keys[i];
        }
    }
    return prog;
}

static void font_init_simple(pdf_doc_t *d, pdf_font_t *f, pdf_val_t *fd, const pdf_val_t *subtype,
                             const unsigned char *base, size_t base_n) {
    pdf_val_t *fdesc = font_desc(d, fd);
    pdf_val_t *flags = pdf_dget(d, fdesc, "Flags");
    int64_t fl = pdf_is_num(flags) ? (flags->is_int ? flags->inum : pdf_d2i(flags->num)) : 0;
    bool symbolic = (fl & PDF_FLAG_SYMBOLIC) != 0;
    pdf_val_t *fcv = pdf_dget(d, fd, "FirstChar");
    int64_t fc = pdf_is_num(fcv) ? (fcv->is_int ? fcv->inum : pdf_d2i(fcv->num)) : 0;
    pdf_val_t *ws = pdf_dget(d, fd, "Widths");
    bool any_width = false;
    double *all = NULL;
    int nall = 0;
    if (ws && ws->kind == PV_ARR) {
        all = (double *)cbm_alloc(CBM_MEM_CLASS_EXTRACT,
                                  (size_t)(ws->count ? ws->count : 1) * sizeof(double));
        for (int i = 0; i < ws->count; i++) {
            pdf_val_t *w = pdf_resolve(d, ws->items[i]);
            if (!pdf_is_num(w)) {
                continue;
            }
            any_width = true;
            int64_t code = pdf_sat_add(fc, i);
            if (code >= 0 && code < 256) {
                f->widths[code] = w->num;
                f->has_width[code] = true;
            }
            if (all) {
                all[nall++] = w->num;
            }
        }
    }
    pdf_val_t *mw = pdf_dget(d, fdesc, "MissingWidth");
    if (pdf_is_num(mw) && mw->num > 0) {
        f->missing = mw->num;
        f->has_missing = true;
    }
    const unsigned char *bare = base;
    size_t bare_n = base_n;
    const unsigned char *plus = base_n ? (const unsigned char *)memchr(base, '+', base_n) : NULL;
    if (plus) {
        bare = plus + 1;
        bare_n = base_n - (size_t)(bare - base);
    }
    f->std14 = std14_name(bare, bare_n) && !any_width;
    if (f->std14) {
        /* std14_width reads the name after the LAST '+' */
        const unsigned char *b = base;
        size_t bn = base_n;
        for (size_t i = 0; i < base_n; i++) {
            if (base[i] == '+') {
                b = base + i + 1;
                bn = base_n - i - 1;
            }
        }
        f->std14_tab = bytes_starts(b, bn, "Courier") ? NULL
                       : bytes_starts(b, bn, "Times") ? PDF_WIDTH_TIMES
                                                      : PDF_WIDTH_HELVETICA;
    }
    const char *prog = font_prog(fdesc);
    bool is_t1 = name_eq(subtype, "Type1") || name_eq(subtype, "MMType1");
    bool is_tt = name_eq(subtype, "TrueType");
    if (f->type3) {
        pdf_val_t *fm = pdf_dget(d, fd, "FontMatrix");
        double m0 = 0.001;
        if (fm && fm->kind == PV_ARR && fm->count == 6) {
            pdf_val_t *x = pdf_resolve(d, fm->items[0]);
            m0 = pdf_is_num(x) ? x->num : 0.0;
        }
        f->wscale = m0 != 0.0 ? m0 : 0.001;
        double sc = f->wscale < 0 ? -f->wscale : f->wscale;
        int npos = 0;
        for (int i = 0; i < nall; i++) {
            if (all[i] > 0) {
                all[npos++] = all[i] * sc;
            }
        }
        qsort(all, (size_t)npos, sizeof(double), dbl_cmp);
        double med = npos ? all[npos / 2] : 0.5;
        if (med >= 0.2 && med <= 1.5) {
            f->em = 1.0;
        } else {
            double e = 2.0 * med;
            f->em = e < 0.05 ? 0.05 : e > 20.0 ? 20.0 : e;
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, all);
    /* the encoding */
    pdf_val_t *enc = pdf_dget(d, fd, "Encoding");
    const char *const *table = NULL;
    pdf_val_t *diffs = NULL;
    if (enc && enc->kind == PV_NAME) {
        table = base_table(enc);
    } else if (enc && enc->kind == PV_DICT) {
        pdf_val_t *be = pdf_dget(d, enc, "BaseEncoding");
        if (be) {
            table = base_table(be);
        }
        diffs = pdf_dget(d, enc, "Differences");
    }
    pdf_base_kind_t kind = table ? BASE_TABLE : BASE_NONE;
    bool fallback_identity = false;
    const unsigned char *names[256];
    size_t nlen[256];
    memset((void *)names, 0, sizeof(names));
    memset(nlen, 0, sizeof(nlen));
    if (kind == BASE_NONE) {
        bool got = false;
        if (prog && strcmp(prog, "FontFile") == 0 && is_t1) {
            bool standard;
            if (type1_builtin(d, pdf_dget(d, fdesc, "FontFile"), names, nlen, &standard)) {
                got = true;
                if (standard) {
                    table = PDF_ENC_STANDARD;
                    kind = BASE_TABLE;
                } else {
                    kind = BASE_NAMES;
                }
            }
        }
        if (!got && bytes_starts(bare, bare_n, "Symbol") && !prog) {
            table = PDF_ENC_SYMBOL;
            kind = BASE_TABLE;
            got = true;
        }
        if (!got) {
            if ((is_t1 || is_tt) && !symbolic) {
                table = PDF_ENC_STANDARD;
                kind = BASE_TABLE;
            } else if (!f->type3) {
                fallback_identity = true;
            }
        }
    }
    bool named[256];
    memset(named, 0, sizeof(named));
    if (kind == BASE_NAMES) {
        for (int c = 0; c < 256; c++) {
            named[c] = names[c] != NULL;
        }
    }
    if (diffs && diffs->kind == PV_ARR) {
        int64_t code = 0;
        for (int i = 0; i < diffs->count; i++) {
            pdf_val_t *x = pdf_resolve(d, diffs->items[i]);
            if (pdf_is_num(x)) {
                code = x->is_int ? x->inum : pdf_d2i(x->num);
            } else if (x && x->kind == PV_NAME) {
                if (code >= 0 && code < 256) {
                    names[code] = x->s;
                    nlen[code] = x->n;
                    named[code] = true;
                }
                code = pdf_sat_add(code, 1);
            }
        }
    }
    for (int c = 0; c < 256; c++) {
        const char *u = NULL;
        if (f->tounicode) {
            u = cmap_lookup(d, f->tounicode, 1, (uint32_t)c, NULL, 0);
            if (!u) {
                u = cmap_lookup(d, f->tounicode, 2, (uint32_t)c, NULL, 0);
            }
        }
        if (!u) {
            if (named[c]) {
                u = g2u(d, names[c], nlen[c], 0);
            } else if (kind == BASE_TABLE) {
                u = table[c];
            }
        }
        bool entry_none = !named[c] && kind == BASE_NONE;
        if (!u && (fallback_identity || entry_none) && c >= PDF_STD_FIRST && c <= PDF_STD_LAST &&
            !f->type3) {
            u = ASCII1[c];
        }
        f->map1[c] = u;
    }
}

static void font_init_type0(pdf_doc_t *d, pdf_font_t *f, pdf_val_t *fd) {
    f->multibyte = true;
    pdf_val_t *enc = pdf_dget(d, fd, "Encoding");
    if (enc && enc->kind == PV_NAME) {
        if (!name_eq(enc, "Identity-H") && !name_eq(enc, "Identity-V")) {
            for (size_t i = 0; i + 4 <= enc->n; i++) {
                if (memcmp(enc->s + i, "UCS2", 4) == 0) {
                    f->ucs2 = true;
                }
            }
            for (size_t i = 0; i + 5 <= enc->n; i++) {
                if (memcmp(enc->s + i, "UTF16", 5) == 0) {
                    f->ucs2 = true;
                }
            }
        }
    } else if (enc && enc->kind == PV_STREAM) {
        size_t len = 0;
        const unsigned char *data = pdf_stream_decode(d, enc->stream, &len);
        f->enc = cmap_parse(d, data ? data : (const unsigned char *)"", data ? len : 0);
    }
    pdf_val_t *desc = pdf_dget(d, fd, "DescendantFonts");
    pdf_val_t *d0 = NULL;
    if (desc && desc->kind == PV_ARR && desc->count) {
        d0 = pdf_resolve(d, desc->items[0]);
    }
    if (!d0 || d0->kind != PV_DICT) {
        d0 = NULL;
    }
    f->cid_type2 = name_eq(pdf_dget(d, d0, "Subtype"), "CIDFontType2");
    pdf_val_t *dw = pdf_dget(d, d0, "DW");
    f->dw = pdf_is_num(dw) ? dw->num : 1000.0;
    pdf_val_t *W = pdf_dget(d, d0, "W");
    if (W && W->kind == PV_ARR) {
        iv_list_t iv = {0};
        double *vals = NULL;
        int nv = 0;
        int vcap = 0;
        int i = 0;
        while (i < W->count && !d->nomem) {
            pdf_val_t *first = pdf_resolve(d, W->items[i]);
            pdf_val_t *nxt = i + 1 < W->count ? pdf_resolve(d, W->items[i + 1]) : NULL;
            if (pdf_is_num(first) && nxt && nxt->kind == PV_ARR) {
                int64_t c0 = first->is_int ? first->inum : pdf_d2i(first->num);
                for (int j = 0; j < nxt->count; j++) {
                    pdf_val_t *w = pdf_resolve(d, nxt->items[j]);
                    int64_t c = pdf_sat_add(c0, j);
                    if (!pdf_is_num(w) || c < 0 || c > (int64_t)UINT32_MAX) {
                        continue;
                    }
                    if (nv == vcap) {
                        vcap = vcap ? vcap * 2 : PDF_LIST_MIN;
                        double *g = (double *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, vals,
                                                          (size_t)vcap * sizeof(double));
                        if (!g) {
                            d->nomem = true;
                            break;
                        }
                        vals = g;
                    }
                    vals[nv] = w->num;
                    ivl_push(d, &iv, (uint64_t)c, (uint64_t)c, nv);
                    nv++;
                }
                i += 2;
            } else if (pdf_is_num(first) && pdf_is_num(nxt) && i + 2 < W->count) {
                pdf_val_t *w = pdf_resolve(d, W->items[i + 2]);
                int64_t a = first->is_int ? first->inum : pdf_d2i(first->num);
                double span = nxt->num - first->num;
                if (pdf_is_num(w) && span >= 0 && span <= 65535) {
                    int64_t z = nxt->is_int ? nxt->inum : pdf_d2i(nxt->num);
                    if (z >= 0 && a <= (int64_t)UINT32_MAX) {
                        uint64_t lo = a < 0 ? 0 : (uint64_t)a;
                        if (nv == vcap) {
                            vcap = vcap ? vcap * 2 : PDF_LIST_MIN;
                            double *g = (double *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, vals,
                                                              (size_t)vcap * sizeof(double));
                            if (!g) {
                                d->nomem = true;
                                break;
                            }
                            vals = g;
                        }
                        vals[nv] = w->num;
                        if ((uint64_t)z >= lo) {
                            ivl_push(d, &iv, lo, (uint64_t)z, nv);
                        }
                        nv++;
                    }
                }
                i += 3;
            } else {
                i += 1;
            }
        }
        f->wseg = paint(d, &iv, false, &f->nwseg);
        cbm_free(CBM_MEM_CLASS_EXTRACT, iv.v);
        if (nv) {
            f->wval = (double *)pdf_big(d, (size_t)nv * sizeof(double));
            if (f->wval) {
                memcpy(f->wval, vals, (size_t)nv * sizeof(double));
            }
        }
        cbm_free(CBM_MEM_CLASS_EXTRACT, vals);
    }
    f->fdesc = font_desc(d, d0);
    f->c2g_val = pdf_dget(d, d0, "CIDToGIDMap");
}

static pdf_font_t *font_new(pdf_doc_t *d, pdf_val_t *fd) {
    pdf_font_t *f = (pdf_font_t *)cbm_arena_calloc(d->a, sizeof(*f));
    if (!f) {
        d->nomem = true;
        return NULL;
    }
    f->em = 1.0;
    f->wscale = 0.001;
    pdf_val_t *subtype = pdf_dget(d, fd, "Subtype");
    pdf_val_t *basev = pdf_dget(d, fd, "BaseFont");
    const unsigned char *base =
        (basev && basev->kind == PV_NAME) ? basev->s : (const unsigned char *)"";
    size_t base_n = (basev && basev->kind == PV_NAME) ? basev->n : 0;
    f->type3 = name_eq(subtype, "Type3");
    pdf_val_t *tu = pdf_dget(d, fd, "ToUnicode");
    if (tu && tu->kind == PV_STREAM) {
        size_t len = 0;
        const unsigned char *data = pdf_stream_decode(d, tu->stream, &len);
        if (data && len) {
            f->tounicode = cmap_parse(d, data, len);
        }
    }
    if (name_eq(subtype, "Type0")) {
        font_init_type0(d, f, fd);
    } else {
        font_init_simple(d, f, fd, subtype, base, base_n);
    }
    return d->nomem ? NULL : f;
}

pdf_font_t *pdf_font_for(pdf_doc_t *d, pdf_val_t *resources, const pdf_val_t *name) {
    if (!resources || resources->kind != PV_DICT || !name || name->kind != PV_NAME) {
        return NULL;
    }
    pdf_val_t *fdict = pdf_dget(d, resources, "Font");
    if (!fdict || fdict->kind != PV_DICT) {
        return NULL;
    }
    pdf_val_t *ref = NULL;
    for (int lo = 0, hi = fdict->nkv - 1; lo <= hi;) {
        int mid = lo + (hi - lo) / 2;
        size_t kn = fdict->kv[mid].klen;
        size_t m = kn < name->n ? kn : name->n;
        int c = m ? memcmp(fdict->kv[mid].key, name->s, m) : 0;
        if (!c) {
            c = kn < name->n ? -1 : kn > name->n ? 1 : 0;
        }
        if (!c) {
            ref = fdict->kv[mid].val;
            break;
        }
        if (c < 0) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    if (ref && ref->kind == PV_NULL) {
        ref = NULL;
    }
    uint64_t key = 0;
    bool keyed = false;
    if (ref && ref->kind == PV_REF) {
        key = (1ULL << 63) | (((uint64_t)ref->ref_num & 0xFFFFFFFFFFULL) << 22) |
              ((uint64_t)ref->ref_gen & 0x3FFFFFULL);
        keyed = true;
        int64_t i = pdf_map_get(&d->fonts, key);
        if (i >= 0) {
            return d->font_list[i];
        }
    }
    pdf_val_t *fd = pdf_resolve(d, ref);
    if (!fd || fd->kind != PV_DICT) {
        return NULL;
    }
    if (!keyed) {
        key = (uint64_t)(uintptr_t)fd & ~(1ULL << 63);
        int64_t i = pdf_map_get(&d->fonts, key);
        if (i >= 0) {
            return d->font_list[i];
        }
    }
    pdf_font_t *f = font_new(d, fd);
    if (d->nfonts == d->font_cap) {
        int ncap = d->font_cap ? d->font_cap * 2 : PDF_LIST_MIN;
        pdf_font_t **g = (pdf_font_t **)cbm_realloc(CBM_MEM_CLASS_EXTRACT, (void *)d->font_list,
                                                    (size_t)ncap * sizeof(*g));
        if (!g) {
            d->nomem = true;
            return f;
        }
        d->font_list = g;
        d->font_cap = ncap;
    }
    d->font_list[d->nfonts] = f;
    if (!pdf_map_put(&d->fonts, key, d->nfonts)) {
        d->nomem = true;
    }
    d->nfonts++;
    return f;
}

double pdf_font_em(const pdf_font_t *f) {
    return f->em;
}

static const uint32_t *font_rev(pdf_doc_t *d, pdf_font_t *f) {
    if (f->rev_done) {
        return f->rev;
    }
    f->rev_done = true;
    pdf_val_t *ff = pdf_dget(d, f->fdesc, "FontFile2");
    if (ff && ff->kind == PV_STREAM) {
        size_t len = 0;
        const unsigned char *data = pdf_stream_decode(d, ff->stream, &len);
        if (data && len) {
            f->rev = ttf_reverse(d, data, len);
        }
    }
    if (f->c2g_val && f->c2g_val->kind == PV_STREAM) {
        size_t len = 0;
        const unsigned char *m = pdf_stream_decode(d, f->c2g_val->stream, &len);
        f->c2g = m;
        f->c2g_len = m ? len : 0;
    }
    return f->rev;
}

void pdf_font_decode(pdf_doc_t *d, pdf_font_t *f, const unsigned char *s, size_t len,
                     pdf_glyph_out_t *out, size_t *n) {
    size_t k = 0;
    if (!f->multibyte) {
        for (size_t i = 0; i < len; i++) {
            unsigned c = s[i];
            double w;
            if (f->has_width[c]) {
                w = f->widths[c];
            } else if (f->std14) {
                if (!f->std14_tab) {
                    w = PDF_COURIER_WIDTH;
                } else {
                    w = (c >= PDF_STD_FIRST && c <= PDF_STD_LAST) ? f->std14_tab[c - PDF_STD_FIRST]
                                                                  : PDF_DEFAULT_WIDTH;
                }
            } else if (f->has_missing) {
                w = f->missing;
            } else {
                w = f->type3 ? 0.0 : PDF_DEFAULT_WIDTH;
            }
            out[k].utf8 = f->map1[c];
            out[k].width = w * f->wscale;
            out[k].space = c == ' ';
            k++;
        }
        *n = k;
        return;
    }
    size_t i = 0;
    while (i < len) {
        size_t nb = 0;
        uint32_t code = 0;
        if (f->enc && f->enc->has_space) {
            for (int m = 1; m <= PDF_CODE_BYTES && !nb; m++) {
                if (!f->enc->nspace[m] || i + (size_t)m > len) {
                    continue;
                }
                uint32_t c = be_code(s + i, (size_t)m);
                if (seg_find(f->enc->space[m], f->enc->nspace[m], c) >= 0) {
                    nb = (size_t)m;
                    code = c;
                }
            }
            if (!nb) {
                nb = 1;
                code = s[i];
            }
        } else if (i + 1 < len) {
            nb = 2;
            code = ((uint32_t)s[i] << 8) | s[i + 1];
        } else {
            nb = 1;
            code = s[i];
        }
        i += nb;
        pdf_glyph_out_t *g = &out[k++];
        g->utf8 = NULL;
        g->inl[0] = '\0';
        if (f->tounicode) {
            g->utf8 = cmap_lookup(d, f->tounicode, nb, code, g->inl, sizeof(g->inl));
        }
        int64_t cid = code;
        if (f->enc) {
            int64_t c2;
            if (cmap_to_cid(f->enc, nb, code, &c2)) {
                cid = c2;
            }
        }
        if (!g->utf8 && f->ucs2 && !(code >= PDF_SURR_LO && code <= PDF_SURR_HI)) {
            int m = utf8_put(code, g->inl);
            g->inl[m] = '\0';
            g->utf8 = g->inl;
        }
        if (!g->utf8 && f->cid_type2) {
            const uint32_t *rev = font_rev(d, f);
            if (rev) {
                int64_t gid = cid;
                if (f->c2g && f->c2g_len) {
                    gid = 0;
                    if (cid >= 0 && (uint64_t)cid * 2 + 2 <= f->c2g_len) {
                        gid = ((int64_t)f->c2g[2 * cid] << 8) | f->c2g[2 * cid + 1];
                    }
                }
                if (gid >= 0 && gid < PDF_GID_SPACE && rev[gid]) {
                    int m = utf8_put(rev[gid] - 1, g->inl);
                    g->inl[m] = '\0';
                    g->utf8 = g->inl;
                }
            }
        }
        double w = f->dw;
        if (cid >= 0 && f->nwseg) {
            int r = seg_find(f->wseg, f->nwseg, (uint64_t)cid);
            if (r >= 0 && f->wval) {
                w = f->wval[r];
            }
        }
        g->width = w * 0.001;
        g->space = nb == 1 && code == ' ';
    }
    *n = k;
}
