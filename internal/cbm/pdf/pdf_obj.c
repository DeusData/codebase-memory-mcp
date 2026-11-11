/*
 * pdf_obj.c — lexer, values, the cross-reference, indirect objects, object
 * streams and stream filters of the PDF text-layer extractor.
 *
 * Repairs follow the field-tested prototype: an xref section is looked for
 * near a wrong offset; a stream whose /Length does not end at "endstream" is
 * cut at the next "endstream"; an object missing at its offset is looked up
 * by its header; when the cross-reference cannot be read at all it is rebuilt
 * from every "N G obj" header in the file. Every header the repairs need comes
 * from one scan of the file, built on first need.
 */
#include "pdf_internal.h"

#include "foundation/mem_core.h"
#include "helpers.h"

#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

enum {
    PDF_NUM_BUF = 64,         /* a number's text for strtod */
    PDF_XREF_NEAR = 1024,     /* how far around a wrong xref offset to look */
    PDF_TAIL_WINDOW = 32,     /* where "endstream" must follow /Length */
    PDF_INFLATE_CHUNK = 4096, /* the prototype's recovery chunk */
    PDF_INFLATE_GROW = 65536, /* output growth step */
    PDF_ZLIB_WBITS = 15,
    PDF_MAX_FIELD = 8, /* bytes of one xref-stream field */
    PDF_A85_GROUP = 5,
    PDF_A85_BASE = 85,
    PDF_LZW_CLEAR = 256,
    PDF_LZW_EOD = 257,
    PDF_LZW_FIRST = 258,
    PDF_LZW_MAX_BITS = 12,
    PDF_LZW_MIN_BITS = 9,
    PDF_LZW_TABLE = 4096,
    PDF_RLD_EOD = 128,
    PDF_PNG_PRED = 10,
    PDF_TIFF_PRED = 2,
    PDF_BITS_PER_BYTE = 8,
    PDF_MAP_MIN = 16,
    PDF_OWN_MIN = 16,
    PDF_SCRATCH_MIN = 256,
    PDF_HEX_BASE = 16,
    PDF_OCT_DIGITS = 3,
};

#define PDF_SAT_LIMIT ((INT64_MAX - 9) / 10)

static pdf_val_t PDF_NULL_VALUE = {.kind = PV_NULL};

/* ── Memory ──────────────────────────────────────────────────────── */

void *pdf_alloc(pdf_doc_t *d, size_t n) {
    void *p = cbm_arena_alloc(d->cur, n ? n : 1);
    if (!p) {
        d->nomem = true;
    }
    return p;
}

void *pdf_calloc(pdf_doc_t *d, size_t n) {
    void *p = cbm_arena_calloc(d->cur, n ? n : 1);
    if (!p) {
        d->nomem = true;
    }
    return p;
}

bool pdf_own(pdf_doc_t *d, void *block) {
    if (!block) {
        return false;
    }
    if (d->nowned == d->owned_cap) {
        int ncap = d->owned_cap ? d->owned_cap * 2 : PDF_OWN_MIN;
        void **g = (void **)cbm_realloc(CBM_MEM_CLASS_EXTRACT, (void *)d->owned,
                                        (size_t)ncap * sizeof(*g));
        if (!g) {
            cbm_free(CBM_MEM_CLASS_EXTRACT, block);
            d->nomem = true;
            return false;
        }
        d->owned = g;
        d->owned_cap = ncap;
    }
    d->owned[d->nowned++] = block;
    return true;
}

void *pdf_big(pdf_doc_t *d, size_t n) {
    void *p = cbm_alloc(CBM_MEM_CLASS_EXTRACT, n ? n : 1);
    if (!p) {
        d->nomem = true;
        return NULL;
    }
    return pdf_own(d, p) ? p : NULL;
}

bool pdf_scratch(pdf_doc_t *d, size_t need) {
    if (need <= d->scratch_cap) {
        return true;
    }
    size_t ncap = d->scratch_cap ? d->scratch_cap : PDF_SCRATCH_MIN;
    while (ncap < need) {
        if (ncap > SIZE_MAX / 2) {
            ncap = need;
            break;
        }
        ncap *= 2;
    }
    unsigned char *g = (unsigned char *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, d->scratch, ncap);
    if (!g) {
        d->nomem = true;
        return false;
    }
    d->scratch = g;
    d->scratch_cap = ncap;
    return true;
}

/* A growable byte buffer from the memory core (decoder output). */
typedef struct {
    unsigned char *p;
    size_t n;
    size_t cap;
    bool fail;
} pdf_buf_t;

static bool buf_reserve(pdf_buf_t *b, size_t add) {
    if (b->fail) {
        return false;
    }
    if (add > SIZE_MAX - b->n) {
        b->fail = true;
        return false;
    }
    size_t need = b->n + add;
    if (need <= b->cap) {
        return true;
    }
    size_t ncap = b->cap ? b->cap : PDF_INFLATE_GROW;
    while (ncap < need) {
        if (ncap > SIZE_MAX / 2) {
            ncap = need;
            break;
        }
        ncap *= 2;
    }
    unsigned char *g = (unsigned char *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, b->p, ncap);
    if (!g) {
        b->fail = true;
        return false;
    }
    b->p = g;
    b->cap = ncap;
    return true;
}

static bool buf_put(pdf_buf_t *b, const unsigned char *src, size_t n) {
    if (!n) {
        return !b->fail;
    }
    if (!buf_reserve(b, n)) {
        return false;
    }
    memcpy(b->p + b->n, src, n);
    b->n += n;
    return true;
}

static bool buf_byte(pdf_buf_t *b, unsigned char c) {
    if (!buf_reserve(b, 1)) {
        return false;
    }
    b->p[b->n++] = c;
    return true;
}

static void buf_free(pdf_buf_t *b) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, b->p);
    memset(b, 0, sizeof(*b));
}

/* ── Integer maps ────────────────────────────────────────────────── */

static uint64_t map_mix(uint64_t x) {
    /* splitmix64's finalizer; the seed keeps crafted object numbers from
     * clustering (it changes speed only, never a result). */
    static const uint64_t seed = 0x9e3779b97f4a7c15ULL;
    x += seed;
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

int64_t pdf_map_get(const pdf_map_t *m, uint64_t key) {
    if (!m->cap) {
        return -1;
    }
    uint32_t mask = m->cap - 1;
    uint32_t i = (uint32_t)map_mix(key) & mask;
    while (m->vals[i] >= 0) {
        if (m->keys[i] == key) {
            return m->vals[i];
        }
        i = (i + 1) & mask;
    }
    return -1;
}

static bool map_grow(pdf_map_t *m) {
    uint32_t ncap = m->cap ? m->cap * 2 : PDF_MAP_MIN;
    if (ncap < m->cap) {
        return false;
    }
    uint64_t *keys = (uint64_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)ncap * sizeof(*keys));
    int64_t *vals = (int64_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)ncap * sizeof(*vals));
    if (!keys || !vals) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, keys);
        cbm_free(CBM_MEM_CLASS_EXTRACT, vals);
        return false;
    }
    for (uint32_t i = 0; i < ncap; i++) {
        vals[i] = -1;
    }
    uint32_t mask = ncap - 1;
    for (uint32_t i = 0; i < m->cap; i++) {
        if (m->vals[i] < 0) {
            continue;
        }
        uint32_t j = (uint32_t)map_mix(m->keys[i]) & mask;
        while (vals[j] >= 0) {
            j = (j + 1) & mask;
        }
        keys[j] = m->keys[i];
        vals[j] = m->vals[i];
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, m->keys);
    cbm_free(CBM_MEM_CLASS_EXTRACT, m->vals);
    m->keys = keys;
    m->vals = vals;
    m->cap = ncap;
    return true;
}

bool pdf_map_put(pdf_map_t *m, uint64_t key, int64_t val) {
    if ((m->n + 1) * 2 > m->cap && !map_grow(m)) {
        return false;
    }
    uint32_t mask = m->cap - 1;
    uint32_t i = (uint32_t)map_mix(key) & mask;
    while (m->vals[i] >= 0) {
        if (m->keys[i] == key) {
            m->vals[i] = val;
            return true;
        }
        i = (i + 1) & mask;
    }
    m->keys[i] = key;
    m->vals[i] = val;
    m->n++;
    return true;
}

void pdf_map_free(pdf_map_t *m) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, m->keys);
    cbm_free(CBM_MEM_CLASS_EXTRACT, m->vals);
    memset(m, 0, sizeof(*m));
}

/* ── Characters ──────────────────────────────────────────────────── */

static inline bool is_ws(unsigned char c) {
    return c == 0 || c == '\t' || c == '\n' || c == '\f' || c == '\r' || c == ' ';
}

static inline bool is_delim(unsigned char c) {
    return c == '(' || c == ')' || c == '<' || c == '>' || c == '[' || c == ']' || c == '{' ||
           c == '}' || c == '/' || c == '%';
}

static inline bool is_reg(unsigned char c) {
    return !is_ws(c) && !is_delim(c);
}

static inline bool is_digit(unsigned char c) {
    return c >= '0' && c <= '9';
}

static int hex_val(unsigned char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

/* A digit run as a saturated integer. */
static int64_t sat_digits(const unsigned char *s, size_t n) {
    int64_t v = 0;
    for (size_t i = 0; i < n; i++) {
        if (v > PDF_SAT_LIMIT) {
            return INT64_MAX;
        }
        v = v * 10 + (s[i] - '0');
    }
    return v;
}

/* ── Lexer ───────────────────────────────────────────────────────── */

bool pdf_kw_is(const pdf_tok_t *t, const char *kw) {
    size_t n = strlen(kw);
    return t->kind == PT_KW && t->n == n && memcmp(t->s, kw, n) == 0;
}

/* [+-]?(\d+\.?\d*|\.\d+) not followed by a regular character. */
static bool lex_num(const unsigned char *data, size_t len, size_t p, size_t *end, pdf_tok_t *t) {
    size_t q = p;
    bool neg = false;
    if (data[q] == '+' || data[q] == '-') {
        neg = data[q] == '-';
        q++;
    }
    size_t ds = q;
    bool dot = false;
    if (q < len && is_digit(data[q])) {
        while (q < len && is_digit(data[q])) {
            q++;
        }
        if (q < len && data[q] == '.') {
            dot = true;
            q++;
            while (q < len && is_digit(data[q])) {
                q++;
            }
        }
    } else if (q + 1 < len && data[q] == '.' && is_digit(data[q + 1])) {
        dot = true;
        q++;
        while (q < len && is_digit(data[q])) {
            q++;
        }
    } else {
        return false;
    }
    if (q < len && is_reg(data[q])) {
        return false;
    }
    t->kind = PT_NUM;
    t->is_int = !dot;
    if (!dot) {
        int64_t v = sat_digits(data + ds, q - ds);
        t->inum = neg ? -v : v;
        t->num = (double)t->inum;
    } else {
        char buf[PDF_NUM_BUF];
        size_t n = q - p;
        if (n < sizeof(buf)) {
            memcpy(buf, data + p, n);
            buf[n] = '\0';
            t->num = strtod(buf, NULL);
        } else {
            /* a number this long is noise; read its leading digits */
            memcpy(buf, data + p, sizeof(buf) - 1);
            buf[sizeof(buf) - 1] = '\0';
            t->num = strtod(buf, NULL);
        }
        t->inum = (int64_t)0;
    }
    *end = q;
    return true;
}

static void lex_name(pdf_doc_t *d, const unsigned char *s, size_t n, pdf_tok_t *t) {
    t->kind = PT_NAME;
    if (d->peek) {
        t->s = s;
        t->n = n;
        return;
    }
    unsigned char *o = (unsigned char *)pdf_alloc(d, n + 1);
    if (!o) {
        t->s = (const unsigned char *)"";
        t->n = 0;
        return;
    }
    size_t k = 0;
    for (size_t i = 0; i < n;) {
        if (s[i] == '#' && i + 2 < n && hex_val(s[i + 1]) >= 0 && hex_val(s[i + 2]) >= 0) {
            o[k++] = (unsigned char)(hex_val(s[i + 1]) * PDF_HEX_BASE + hex_val(s[i + 2]));
            i += 3;
        } else {
            o[k++] = s[i++];
        }
    }
    o[k] = '\0';
    t->s = o;
    t->n = k;
}

static bool lstr_special(unsigned char c) {
    return c == '(' || c == ')' || c == '\\' || c == '\r';
}

/* A literal string from data[i..] (after its "("); returns the position after it. */
static size_t lex_lstr(pdf_doc_t *d, const unsigned char *data, size_t len, size_t i,
                       pdf_tok_t *t) {
    int depth = 1;
    size_t k = 0;
    bool keep = !d->peek;
    size_t end = len;
    t->kind = PT_STR;
    for (;;) {
        size_t j = i;
        while (j < len && !lstr_special(data[j])) {
            j++;
        }
        if (keep && j > i) {
            if (!pdf_scratch(d, k + (j - i) + PDF_OCT_DIGITS + 1)) {
                keep = false;
            } else {
                memcpy(d->scratch + k, data + i, j - i);
                k += j - i;
            }
        }
        if (j >= len) {
            end = len;
            break;
        }
        if (keep && !pdf_scratch(d, k + 2)) {
            keep = false;
        }
        unsigned char c = data[j];
        int out = -1;
        if (c == '\\') {
            if (j + 1 >= len) {
                end = len;
                break;
            }
            unsigned char nx = data[j + 1];
            static const char esc_from[] = "nrtbf()\\";
            static const char esc_to[] = "\n\r\t\b\f()\\";
            const char *e = (nx != 0) ? strchr(esc_from, nx) : NULL;
            if (e) {
                out = (unsigned char)esc_to[e - esc_from];
                i = j + 2;
            } else if (nx >= '0' && nx <= '7') {
                size_t q = j + 1;
                unsigned v = 0;
                while (q < len && q < j + 1 + PDF_OCT_DIGITS && data[q] >= '0' && data[q] <= '7') {
                    v = v * PDF_BITS_PER_BYTE + (unsigned)(data[q] - '0');
                    q++;
                }
                out = (int)(v & 0xFFU);
                i = q;
            } else if (nx == '\r') {
                i = (j + 2 < len && data[j + 2] == '\n') ? j + 3 : j + 2;
            } else if (nx == '\n') {
                i = j + 2;
            } else {
                out = nx;
                i = j + 2;
            }
        } else if (c == '\r') {
            out = '\n';
            i = (j + 1 < len && data[j + 1] == '\n') ? j + 2 : j + 1;
        } else if (c == '(') {
            depth++;
            out = c;
            i = j + 1;
        } else {
            depth--;
            if (depth == 0) {
                end = j + 1;
                break;
            }
            out = c;
            i = j + 1;
        }
        if (keep && out >= 0) {
            d->scratch[k++] = (unsigned char)out;
        }
    }
    if (d->peek) {
        t->s = NULL;
        t->n = 0;
        return end;
    }
    unsigned char *o = (unsigned char *)pdf_alloc(d, k + 1);
    if (o) {
        if (k) {
            memcpy(o, d->scratch, k);
        }
        o[k] = '\0';
    }
    t->s = o ? o : (const unsigned char *)"";
    t->n = o ? k : 0;
    return end;
}

/* A hex string from data[i..] (after its "<"). */
static size_t lex_hstr(pdf_doc_t *d, const unsigned char *data, size_t len, size_t i,
                       pdf_tok_t *t) {
    const unsigned char *gt = (const unsigned char *)memchr(data + i, '>', len - i);
    size_t e = gt ? (size_t)(gt - data) : len;
    t->kind = PT_STR;
    size_t end = e < len ? e + 1 : len;
    if (d->peek) {
        t->s = NULL;
        t->n = 0;
        return end;
    }
    size_t nhex = 0;
    for (size_t q = i; q < e; q++) {
        nhex += hex_val(data[q]) >= 0;
    }
    unsigned char *o = (unsigned char *)pdf_alloc(d, (nhex + 1) / 2 + 1);
    if (!o) {
        t->s = (const unsigned char *)"";
        t->n = 0;
        return end;
    }
    size_t k = 0;
    int hi = -1;
    for (size_t q = i; q < e; q++) {
        int v = hex_val(data[q]);
        if (v < 0) {
            continue;
        }
        if (hi < 0) {
            hi = v;
        } else {
            o[k++] = (unsigned char)(hi * PDF_HEX_BASE + v);
            hi = -1;
        }
    }
    if (hi >= 0) {
        o[k++] = (unsigned char)(hi * PDF_HEX_BASE);
    }
    o[k] = '\0';
    t->s = o;
    t->n = k;
    return end;
}

void pdf_lex(pdf_doc_t *d, const unsigned char *data, size_t len, size_t *pos, pdf_tok_t *t) {
    size_t p = *pos;
    memset(t, 0, sizeof(*t));
    while (p < len) {
        unsigned char c = data[p];
        if (is_ws(c)) {
            p++;
            continue;
        }
        if (c == '%') {
            while (p < len && data[p] != '\r' && data[p] != '\n') {
                p++;
            }
            continue;
        }
        if (is_digit(c) || c == '+' || c == '-' || c == '.') {
            size_t e;
            if (lex_num(data, len, p, &e, t)) {
                *pos = e;
                return;
            }
        }
        switch (c) {
        case '/': {
            size_t q = p + 1;
            while (q < len && is_reg(data[q])) {
                q++;
            }
            lex_name(d, data + p + 1, q - p - 1, t);
            *pos = q;
            return;
        }
        case '(':
            *pos = lex_lstr(d, data, len, p + 1, t);
            return;
        case '<':
            if (p + 1 < len && data[p + 1] == '<') {
                t->kind = PT_DS;
                *pos = p + 2;
                return;
            }
            *pos = lex_hstr(d, data, len, p + 1, t);
            return;
        case '>':
            if (p + 1 < len && data[p + 1] == '>') {
                t->kind = PT_DE;
                *pos = p + 2;
                return;
            }
            p++;
            continue;
        case ')':
            p++;
            continue;
        case '[':
            t->kind = PT_AS;
            *pos = p + 1;
            return;
        case ']':
            t->kind = PT_AE;
            *pos = p + 1;
            return;
        case '{':
            t->kind = PT_BS;
            *pos = p + 1;
            return;
        case '}':
            t->kind = PT_BE;
            *pos = p + 1;
            return;
        default: {
            size_t q = p;
            while (q < len && is_reg(data[q])) {
                q++;
            }
            t->kind = PT_KW;
            t->s = data + p;
            t->n = q - p;
            *pos = q;
            return;
        }
        }
    }
    t->kind = PT_EOF;
    *pos = len;
}

/* The next token without copying names or strings. */
static void lex_peek(pdf_doc_t *d, const unsigned char *data, size_t len, size_t *pos,
                     pdf_tok_t *t) {
    bool was = d->peek;
    d->peek = true;
    pdf_lex(d, data, len, pos, t);
    d->peek = was;
}

/* ── Values ──────────────────────────────────────────────────────── */

static pdf_val_t *new_val(pdf_doc_t *d, pdf_kind_t kind) {
    pdf_val_t *v = (pdf_val_t *)pdf_calloc(d, sizeof(*v));
    if (v) {
        v->kind = kind;
    }
    return v;
}

static pdf_val_t *val_from_tok(pdf_doc_t *d, const pdf_tok_t *t) {
    pdf_val_t *v = new_val(d, t->kind == PT_NUM ? PV_NUM : t->kind == PT_NAME ? PV_NAME : PV_STR);
    if (!v) {
        return NULL;
    }
    v->is_int = t->is_int;
    v->num = t->num;
    v->inum = t->inum;
    v->s = t->s;
    v->n = t->n;
    return v;
}

typedef struct {
    pdf_val_t **v;
    int n;
    int cap;
    bool fail;
} val_list_t;

static void vl_push(val_list_t *l, pdf_val_t *x) {
    if (l->fail) {
        return;
    }
    if (l->n == l->cap) {
        int ncap = l->cap ? l->cap * 2 : PDF_MAP_MIN;
        pdf_val_t **g = (pdf_val_t **)cbm_realloc(CBM_MEM_CLASS_EXTRACT, (void *)l->v,
                                                  (size_t)ncap * sizeof(*g));
        if (!g) {
            l->fail = true;
            return;
        }
        l->v = g;
        l->cap = ncap;
    }
    l->v[l->n++] = x;
}

static pdf_val_t *vl_finish_array(pdf_doc_t *d, val_list_t *l) {
    pdf_val_t *arr = NULL;
    if (!l->fail) {
        arr = new_val(d, PV_ARR);
        if (arr && l->n) {
            arr->items = (pdf_val_t **)pdf_alloc(d, (size_t)l->n * sizeof(*arr->items));
            if (arr->items) {
                memcpy((void *)arr->items, (void *)l->v, (size_t)l->n * sizeof(*arr->items));
                arr->count = l->n;
            } else {
                arr = NULL;
            }
        }
    } else {
        d->nomem = true;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, (void *)l->v);
    return arr;
}

typedef struct {
    pdf_kv_t kv;
    int seq;
} kv_seq_t;

static int kv_key_cmp(const unsigned char *a, size_t an, const unsigned char *b, size_t bn) {
    size_t m = an < bn ? an : bn;
    int c = m ? memcmp(a, b, m) : 0;
    if (c) {
        return c;
    }
    return an < bn ? -1 : an > bn ? 1 : 0;
}

static int kv_seq_cmp(const void *x, const void *y) {
    const kv_seq_t *a = (const kv_seq_t *)x;
    const kv_seq_t *b = (const kv_seq_t *)y;
    int c = kv_key_cmp(a->kv.key, a->kv.klen, b->kv.key, b->kv.klen);
    if (c) {
        return c;
    }
    return a->seq < b->seq ? -1 : a->seq > b->seq ? 1 : 0;
}

typedef struct {
    kv_seq_t *v;
    int n;
    int cap;
    bool fail;
} kv_list_t;

static void kl_push(kv_list_t *l, const unsigned char *key, size_t klen, pdf_val_t *val) {
    if (l->fail) {
        return;
    }
    if (l->n == l->cap) {
        int ncap = l->cap ? l->cap * 2 : PDF_MAP_MIN;
        kv_seq_t *g =
            (kv_seq_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, l->v, (size_t)ncap * sizeof(*g));
        if (!g) {
            l->fail = true;
            return;
        }
        l->v = g;
        l->cap = ncap;
    }
    l->v[l->n].kv.key = key;
    l->v[l->n].kv.klen = klen;
    l->v[l->n].kv.val = val;
    l->v[l->n].seq = l->n;
    l->n++;
}

/* A dict from key/value pairs in written order: one entry per key, the last
 * written (keep_last) or the first. */
static pdf_val_t *kl_finish_dict(pdf_doc_t *d, kv_list_t *l, bool keep_last) {
    pdf_val_t *dict = NULL;
    if (l->fail) {
        d->nomem = true;
        cbm_free(CBM_MEM_CLASS_EXTRACT, l->v);
        return NULL;
    }
    if (l->n > 1) {
        qsort(l->v, (size_t)l->n, sizeof(*l->v), kv_seq_cmp);
    }
    dict = new_val(d, PV_DICT);
    if (dict && l->n) {
        dict->kv = (pdf_kv_t *)pdf_alloc(d, (size_t)l->n * sizeof(*dict->kv));
        if (!dict->kv) {
            dict = NULL;
        } else {
            int k = 0;
            for (int i = 0; i < l->n;) {
                int j = i;
                while (j + 1 < l->n && kv_key_cmp(l->v[j + 1].kv.key, l->v[j + 1].kv.klen,
                                                  l->v[i].kv.key, l->v[i].kv.klen) == 0) {
                    j++;
                }
                dict->kv[k++] = keep_last ? l->v[j].kv : l->v[i].kv;
                i = j + 1;
            }
            dict->nkv = k;
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, l->v);
    return dict;
}

static bool kw_ends_value(const pdf_tok_t *t) {
    return pdf_kw_is(t, "endobj") || pdf_kw_is(t, "stream") || pdf_kw_is(t, "endstream");
}

pdf_val_t *pdf_parse_value(pdf_doc_t *d, const unsigned char *data, size_t len, size_t *pos,
                           const pdf_tok_t *t, int depth) {
    if (depth > PDF_MAX_NESTING || d->nomem) {
        return NULL;
    }
    switch (t->kind) {
    case PT_NUM:
        if (t->is_int) {
            size_t p2 = *pos;
            pdf_tok_t t2;
            lex_peek(d, data, len, &p2, &t2);
            if (t2.kind == PT_NUM && t2.is_int) {
                pdf_tok_t t3;
                lex_peek(d, data, len, &p2, &t3);
                if (pdf_kw_is(&t3, "R")) {
                    pdf_val_t *r = new_val(d, PV_REF);
                    if (r) {
                        r->ref_num = t->inum;
                        r->ref_gen = t2.inum;
                        *pos = p2;
                    }
                    return r;
                }
            }
        }
        return val_from_tok(d, t);
    case PT_NAME:
    case PT_STR:
        return val_from_tok(d, t);
    case PT_AS: {
        val_list_t l = {0};
        for (;;) {
            pdf_tok_t a;
            pdf_lex(d, data, len, pos, &a);
            if (a.kind == PT_AE || a.kind == PT_EOF) {
                break;
            }
            if (a.kind == PT_DE || a.kind == PT_BE || a.kind == PT_BS) {
                continue;
            }
            if (a.kind == PT_KW && kw_ends_value(&a)) {
                break;
            }
            pdf_val_t *x = pdf_parse_value(d, data, len, pos, &a, depth + 1);
            if (!x) {
                cbm_free(CBM_MEM_CLASS_EXTRACT, (void *)l.v);
                return NULL;
            }
            vl_push(&l, x);
        }
        return vl_finish_array(d, &l);
    }
    case PT_DS: {
        kv_list_t l = {0};
        for (;;) {
            size_t save = *pos;
            pdf_tok_t k;
            pdf_lex(d, data, len, pos, &k);
            if (k.kind == PT_DE || k.kind == PT_EOF) {
                break;
            }
            if (k.kind == PT_KW && (kw_ends_value(&k) || pdf_kw_is(&k, "obj"))) {
                *pos = save;
                break;
            }
            if (k.kind != PT_NAME) {
                continue;
            }
            pdf_tok_t k2;
            pdf_lex(d, data, len, pos, &k2);
            if (k2.kind == PT_DE) {
                kl_push(&l, k.s, k.n, &PDF_NULL_VALUE);
                break;
            }
            pdf_val_t *x = pdf_parse_value(d, data, len, pos, &k2, depth + 1);
            if (!x) {
                cbm_free(CBM_MEM_CLASS_EXTRACT, l.v);
                return NULL;
            }
            kl_push(&l, k.s, k.n, x);
        }
        return kl_finish_dict(d, &l, true);
    }
    case PT_KW:
        if (pdf_kw_is(t, "true") || pdf_kw_is(t, "false")) {
            pdf_val_t *b = new_val(d, PV_BOOL);
            if (b) {
                b->b = t->s[0] == 't';
            }
            return b;
        }
        if (pdf_kw_is(t, "null")) {
            return &PDF_NULL_VALUE;
        }
        {
            pdf_val_t *k = new_val(d, PV_KW);
            if (k) {
                k->s = t->s;
                k->n = t->n;
            }
            return k;
        }
    default:
        return &PDF_NULL_VALUE;
    }
}

pdf_val_t *pdf_parse_content_array(pdf_doc_t *d, const unsigned char *data, size_t len, size_t *pos,
                                   int depth) {
    if (depth > PDF_MAX_NESTING) {
        return NULL;
    }
    val_list_t l = {0};
    for (;;) {
        pdf_tok_t a;
        pdf_lex(d, data, len, pos, &a);
        if (a.kind == PT_AE || a.kind == PT_EOF) {
            break;
        }
        pdf_val_t *x = NULL;
        if (a.kind == PT_NUM || a.kind == PT_STR || a.kind == PT_NAME) {
            x = val_from_tok(d, &a);
        } else if (a.kind == PT_AS) {
            x = pdf_parse_content_array(d, data, len, pos, depth + 1);
        } else if (a.kind == PT_DS) {
            x = pdf_parse_value(d, data, len, pos, &a, depth + 1);
        } else if (a.kind == PT_KW) {
            *pos = (size_t)(a.s - data); /* the operator is the caller's */
            break;
        } else {
            continue;
        }
        if (!x) {
            cbm_free(CBM_MEM_CLASS_EXTRACT, (void *)l.v);
            return NULL;
        }
        vl_push(&l, x);
    }
    return vl_finish_array(d, &l);
}

/* ── Dict access ─────────────────────────────────────────────────── */

pdf_val_t *pdf_dfind_n(const pdf_val_t *dict, const unsigned char *key, size_t kn) {
    if (!dict || dict->kind != PV_DICT) {
        return NULL;
    }
    int lo = 0;
    int hi = dict->nkv - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        int c = kv_key_cmp(dict->kv[mid].key, dict->kv[mid].klen, key, kn);
        if (c == 0) {
            return dict->kv[mid].val;
        }
        if (c < 0) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    return NULL;
}

static pdf_val_t *dict_find(const pdf_val_t *dict, const char *key) {
    return pdf_dfind_n(dict, (const unsigned char *)key, strlen(key));
}

pdf_val_t *pdf_dfind(const pdf_val_t *dict, const char *key) {
    return dict_find(dict, key);
}

bool pdf_dhas(const pdf_val_t *dict, const char *key) {
    return dict_find(dict, key) != NULL;
}

pdf_val_t *pdf_dget_raw(const pdf_val_t *dict, const char *key) {
    pdf_val_t *v = dict_find(dict, key);
    return (v && v->kind != PV_NULL) ? v : NULL;
}

pdf_val_t *pdf_dget(pdf_doc_t *d, const pdf_val_t *dict, const char *key) {
    pdf_val_t *v = pdf_resolve(d, pdf_dget_raw(dict, key));
    return (v && v->kind != PV_NULL) ? v : NULL;
}

bool pdf_is_name(const pdf_val_t *v, const char *name) {
    size_t n = strlen(name);
    return v && v->kind == PV_NAME && v->n == n && memcmp(v->s, name, n) == 0;
}

bool pdf_is_num(const pdf_val_t *v) {
    return v && v->kind == PV_NUM;
}

double pdf_num(const pdf_val_t *v) {
    return pdf_is_num(v) ? v->num : 0.0;
}

/* ── Objects ─────────────────────────────────────────────────────── */

/* "[ws]*(\d+)[ws]+(\d+)[ws]+obj" not followed by a regular character, at base. */
static bool match_obj_hdr(const pdf_doc_t *d, size_t base, int64_t *num, size_t *end) {
    const unsigned char *s = d->data;
    size_t len = d->len;
    size_t p = base;
    while (p < len && is_ws(s[p])) {
        p++;
    }
    size_t a = p;
    while (p < len && is_digit(s[p])) {
        p++;
    }
    if (p == a) {
        return false;
    }
    *num = sat_digits(s + a, p - a);
    size_t w = p;
    while (p < len && is_ws(s[p])) {
        p++;
    }
    if (p == w) {
        return false;
    }
    a = p;
    while (p < len && is_digit(s[p])) {
        p++;
    }
    if (p == a) {
        return false;
    }
    w = p;
    while (p < len && is_ws(s[p])) {
        p++;
    }
    if (p == w || p + 3 > len || memcmp(s + p, "obj", 3) != 0) {
        return false;
    }
    p += 3;
    if (p < len && is_reg(s[p])) {
        return false;
    }
    *end = p;
    return true;
}

static pdf_xent_t *xent(pdf_doc_t *d, int64_t num) {
    int64_t i = pdf_map_get(&d->xref.by_num, (uint64_t)num);
    return i < 0 ? NULL : &d->xref.v[i];
}

/* "endstream" follows q, after whitespace, within the 32 bytes after it. */
static bool endstream_follows(const pdf_doc_t *d, size_t q) {
    size_t lim = d->len - q < PDF_TAIL_WINDOW ? d->len : q + PDF_TAIL_WINDOW;
    for (size_t p = q; p < lim; p++) {
        if (!is_ws(d->data[p])) {
            return lim - p >= 9 && memcmp(d->data + p, "endstream", 9) == 0;
        }
    }
    return false;
}

static bool build_hdrs(pdf_doc_t *d);

/* Where the first object header at or after `at` starts (d->len when none):
 * an object's value is read up to there. The objects of a file do not overlap;
 * a token that ran on into the next ones (a string without its end) made every
 * object before it read the rest of the file again, n objects times the file.
 * The bytes of a stream are not bounded by this: its /Length places them. */
static size_t object_end(pdf_doc_t *d, size_t at) {
    if (!build_hdrs(d)) {
        return d->len;
    }
    int lo = 0;
    int hi = d->nhdrs;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (d->hdrs[mid].off >= at) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo < d->nhdrs ? d->hdrs[lo].off : d->len;
}

/* The value of the object at off (or off + hdr), without its stream; *stream_at
 * is where a "stream" keyword after the value starts, 0 when none follows. NULL:
 * no matching header there. */
static pdf_val_t *indirect_value_at(pdf_doc_t *d, int64_t off, bool has_expect, int64_t expect,
                                    bool *ok, size_t *stream_at) {
    *ok = false;
    *stream_at = 0;
    size_t at = 0;
    bool found = false;
    int64_t num = 0;
    for (int k = 0; k < 2 && !found; k++) {
        int64_t base = pdf_sat_add(off, k ? (int64_t)d->hdr : 0);
        if (base < 0 || (uint64_t)base > d->len) {
            continue;
        }
        size_t end;
        if (match_obj_hdr(d, (size_t)base, &num, &end) && (!has_expect || num == expect)) {
            at = end;
            found = true;
        }
    }
    if (!found) {
        return NULL;
    }
    const unsigned char *s = d->data;
    size_t len = object_end(d, at);
    size_t p = at;
    pdf_tok_t t;
    pdf_lex(d, s, len, &p, &t);
    pdf_val_t *val = pdf_parse_value(d, s, len, &p, &t, 0);
    if (!val) {
        return NULL;
    }
    *ok = true;
    size_t p2 = p;
    pdf_tok_t t2;
    lex_peek(d, s, len, &p2, &t2);
    if (pdf_kw_is(&t2, "stream") && val->kind == PV_DICT) {
        *stream_at = p2;
    }
    return val;
}

static int64_t hdr_first_exact(pdf_doc_t *d, int64_t num, int64_t gen);

/* An object a stream's /Length names, read as a value only and cached as
 * pdf_get caches it. An object that is a stream there gives no length and is
 * marked: loading it would read ITS /Length first, one nested call per stream
 * of a chain of such streams. */
static pdf_val_t *length_object(pdf_doc_t *d, int64_t num) {
    pdf_xent_t *e = xent(d, num);
    bool ok;
    size_t stream_at;
    pdf_val_t *v = indirect_value_at(d, e->a, true, num, &ok, &stream_at);
    if (!ok && !d->reconstructed) {
        int64_t off = hdr_first_exact(d, num, e->b);
        if (off >= 0) {
            v = indirect_value_at(d, off, true, num, &ok, &stream_at);
        }
    }
    e = xent(d, num);
    if (ok && stream_at) {
        e->not_length = true;
        return NULL;
    }
    e->cached = true;
    e->obj = ok ? v : NULL;
    return e->obj;
}

/* A stream's /Length through its references (at most PDF_MAX_RESOLVE, as
 * pdf_resolve). */
static pdf_val_t *length_value(pdf_doc_t *d, pdf_val_t *v) {
    for (int depth = 0; v && v->kind == PV_REF; depth++) {
        pdf_xent_t *e = xent(d, v->ref_num);
        if (depth >= PDF_MAX_RESOLVE || !e || e->loading || e->not_length) {
            return NULL;
        }
        if (e->cached) {
            v = e->obj;
        } else if (e->type == 1) {
            v = length_object(d, v->ref_num);
        } else {
            v = pdf_get(d, v->ref_num);
        }
    }
    return v;
}

/* The object at off (or off + hdr), a stream with its raw bytes when one
 * follows. NULL: no matching header there. */
static pdf_val_t *parse_indirect_at(pdf_doc_t *d, int64_t off, bool has_expect, int64_t expect,
                                    bool *ok) {
    size_t st;
    pdf_val_t *val = indirect_value_at(d, off, has_expect, expect, ok, &st);
    if (!val || !st) {
        return val;
    }
    const unsigned char *s = d->data;
    size_t len = d->len;
    if (st + 1 < len && s[st] == '\r' && s[st + 1] == '\n') {
        st += 2;
    } else if (st < len && (s[st] == '\n' || s[st] == '\r')) {
        st += 1;
    }
    pdf_val_t *ln = length_value(d, pdf_dget_raw(val, "Length"));
    const unsigned char *raw = NULL;
    size_t raw_len = 0;
    if (ln && ln->kind == PV_NUM && ln->is_int && ln->inum >= 0 &&
        (uint64_t)ln->inum <= (uint64_t)(len - st)) {
        if (endstream_follows(d, st + (size_t)ln->inum)) {
            raw = s + st;
            raw_len = (size_t)ln->inum;
        }
    }
    if (!raw) {
        d->counts->stream_length_recovered++;
        const unsigned char *e = cbm_memmem(s + st, len - st, "endstream", 9);
        size_t end = e ? (size_t)(e - s) : len;
        raw = s + st;
        raw_len = end - st;
        if (raw_len >= 2 && raw[raw_len - 2] == '\r' && raw[raw_len - 1] == '\n') {
            raw_len -= 2;
        } else if (raw_len >= 1 && (raw[raw_len - 1] == '\n' || raw[raw_len - 1] == '\r')) {
            raw_len -= 1;
        }
    }
    pdf_stream_t *sm = (pdf_stream_t *)pdf_calloc(d, sizeof(*sm));
    pdf_val_t *sv = new_val(d, PV_STREAM);
    if (!sm || !sv) {
        return NULL;
    }
    sm->dict = val;
    sm->raw = raw;
    sm->raw_len = raw_len;
    sv->stream = sm;
    return sv;
}

/* (number, position) pairs, sorted to find the first of a number. */
static int numidx_cmp(const void *x, const void *y) {
    const pdf_numidx_t *a = (const pdf_numidx_t *)x;
    const pdf_numidx_t *b = (const pdf_numidx_t *)y;
    if (a->num != b->num) {
        return a->num < b->num ? -1 : 1;
    }
    return a->idx < b->idx ? -1 : a->idx > b->idx ? 1 : 0;
}

/* The first pair of `num` in a sorted array; n when none. */
static int numidx_first(const pdf_numidx_t *v, int n, int64_t num) {
    int lo = 0;
    int hi = n;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (v[mid].num < num) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return (lo < n && v[lo].num == num) ? lo : n;
}

/* Every "N G obj" header in file order (the reconstruction and the fallback
 * for objects missing at their offset both read it). */
static bool build_hdrs(pdf_doc_t *d) {
    if (d->hdrs_built) {
        return true;
    }
    d->hdrs_built = true;
    const unsigned char *s = d->data;
    size_t len = d->len;
    int cap = 0;
    size_t from = 0;
    while (from + 3 <= len) {
        const unsigned char *o = cbm_memmem(s + from, len - from, "obj", 3);
        if (!o) {
            break;
        }
        size_t q = (size_t)(o - s);
        from = q + 3;
        size_t p = q;
        while (p > 0 && is_ws(s[p - 1])) {
            p--;
        }
        if (p == q) {
            continue;
        }
        size_t ge = p;
        while (p > 0 && is_digit(s[p - 1])) {
            p--;
        }
        size_t gs = p;
        if (gs == ge) {
            continue;
        }
        size_t w = p;
        while (p > 0 && is_ws(s[p - 1])) {
            p--;
        }
        if (p == w) {
            continue;
        }
        size_t ne = p;
        while (p > 0 && is_digit(s[p - 1])) {
            p--;
        }
        size_t ns = p;
        if (ns == ne) {
            continue;
        }
        if (d->nhdrs == cap) {
            int ncap = cap ? cap * 2 : PDF_MAP_MIN;
            pdf_hdr_t *g =
                (pdf_hdr_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, d->hdrs, (size_t)ncap * sizeof(*g));
            if (!g) {
                d->nomem = true;
                return false;
            }
            d->hdrs = g;
            cap = ncap;
        }
        pdf_hdr_t *h = &d->hdrs[d->nhdrs++];
        h->num = sat_digits(s + ns, ne - ns);
        h->gen = sat_digits(s + gs, ge - gs);
        h->off = ns;
        h->num_digits = (uint8_t)((ne - ns) > UINT8_MAX ? UINT8_MAX : (ne - ns));
        h->gen_digits = (uint8_t)((ge - gs) > UINT8_MAX ? UINT8_MAX : (ge - gs));
        h->canonical = (s[ns] != '0' || ne - ns == 1) && (s[gs] != '0' || ge - gs == 1);
        h->reg_after = q + 3 < len && is_reg(s[q + 3]);
    }
    return true;
}

/* The first header written exactly "num gen obj" (the prototype's search). */
static int64_t hdr_first_exact(pdf_doc_t *d, int64_t num, int64_t gen) {
    if (!build_hdrs(d) || !d->nhdrs) {
        return -1;
    }
    if (!d->hdr_sorted) {
        d->hdr_sorted = (pdf_numidx_t *)pdf_big(d, (size_t)d->nhdrs * sizeof(pdf_numidx_t));
        if (!d->hdr_sorted) {
            return -1;
        }
        for (int i = 0; i < d->nhdrs; i++) {
            d->hdr_sorted[i].num = d->hdrs[i].num;
            d->hdr_sorted[i].idx = i;
        }
        qsort(d->hdr_sorted, (size_t)d->nhdrs, sizeof(pdf_numidx_t), numidx_cmp);
    }
    for (int k = numidx_first(d->hdr_sorted, d->nhdrs, num);
         k < d->nhdrs && d->hdr_sorted[k].num == num; k++) {
        const pdf_hdr_t *h = &d->hdrs[d->hdr_sorted[k].idx];
        if (h->gen == gen && h->canonical) {
            return (int64_t)h->off;
        }
    }
    return -1;
}

static pdf_objstm_t *objstm(pdf_doc_t *d, int64_t stmnum);

struct pdf_objstm {
    const unsigned char *data;
    size_t len;
    int64_t first;
    int64_t *onum; /* member numbers, in stream order */
    int64_t *ooff;
    int n;
    pdf_numidx_t *sorted; /* (onum, order), built on first need */
    int64_t *starts;      /* first + each offset, ascending, built on first need */
};

static int i64_cmp(const void *x, const void *y);

/* Where the member after the one at `at` starts (sm->len when none): members
 * do not overlap, as the objects of a file do not (object_end). */
static size_t member_end(pdf_doc_t *d, pdf_objstm_t *sm, size_t at) {
    if (!sm->starts) {
        sm->starts = (int64_t *)pdf_big(d, (size_t)(sm->n ? sm->n : 1) * sizeof(int64_t));
        if (!sm->starts) {
            return sm->len;
        }
        for (int i = 0; i < sm->n; i++) {
            sm->starts[i] = pdf_sat_add(sm->first, sm->ooff[i]);
        }
        qsort(sm->starts, (size_t)sm->n, sizeof(int64_t), i64_cmp);
    }
    int lo = 0;
    int hi = sm->n;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (sm->starts[mid] > (int64_t)at) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    if (lo < sm->n && (uint64_t)sm->starts[lo] < (uint64_t)sm->len) {
        return (size_t)sm->starts[lo];
    }
    return sm->len;
}

static int64_t objstm_find(pdf_doc_t *d, pdf_objstm_t *sm, int64_t num, int64_t idx) {
    if (idx >= 0 && idx < sm->n && sm->onum[idx] == num) {
        return sm->ooff[idx];
    }
    if (!sm->sorted) {
        sm->sorted = (pdf_numidx_t *)pdf_big(d, (size_t)(sm->n ? sm->n : 1) * sizeof(pdf_numidx_t));
        if (!sm->sorted) {
            return INT64_MIN;
        }
        for (int i = 0; i < sm->n; i++) {
            sm->sorted[i].num = sm->onum[i];
            sm->sorted[i].idx = i;
        }
        qsort(sm->sorted, (size_t)sm->n, sizeof(pdf_numidx_t), numidx_cmp);
    }
    int k = numidx_first(sm->sorted, sm->n, num);
    return k < sm->n ? sm->ooff[sm->sorted[k].idx] : INT64_MIN;
}

pdf_val_t *pdf_get(pdf_doc_t *d, int64_t num) {
    int64_t idx = pdf_map_get(&d->xref.by_num, (uint64_t)num);
    if (idx < 0) {
        return NULL;
    }
    pdf_xent_t *e = &d->xref.v[idx];
    if (e->cached) {
        return e->obj;
    }
    if (e->loading) {
        return NULL;
    }
    if (e->type == 2 && d->objstm_opening > 0) {
        /* The objects that open an object stream (its /Length, its filters) are
         * no objects of an object stream (ISO 32000-1, 7.5.7): one that is would
         * open the next stream inside this one, one nested call per stream of a
         * chain. */
        d->counts->objstm_nested++;
        return NULL;
    }
    e->loading = true;
    uint8_t type = e->type;
    int64_t a = e->a;
    int64_t b = e->b;
    pdf_val_t *obj = NULL;
    if (type == 1) {
        bool ok;
        obj = parse_indirect_at(d, a, true, num, &ok);
        if (!ok && !d->reconstructed) {
            int64_t off = hdr_first_exact(d, num, b);
            if (off >= 0) {
                obj = parse_indirect_at(d, off, true, num, &ok);
            }
        }
        if (!ok) {
            obj = NULL;
        }
    } else if (type == 2) {
        pdf_objstm_t *sm = objstm(d, a);
        if (sm) {
            int64_t off = objstm_find(d, sm, num, b);
            int64_t at = off == INT64_MIN ? -1 : pdf_sat_add(sm->first, off);
            if (at >= 0 && (uint64_t)at <= (uint64_t)sm->len) {
                size_t p = (size_t)at;
                size_t end = member_end(d, sm, (size_t)at);
                pdf_tok_t t;
                pdf_lex(d, sm->data, end, &p, &t);
                obj = pdf_parse_value(d, sm->data, end, &p, &t, 0);
            }
        }
    }
    e = &d->xref.v[idx];
    e->loading = false;
    e->cached = true;
    e->obj = obj;
    return obj;
}

pdf_val_t *pdf_resolve(pdf_doc_t *d, pdf_val_t *v) {
    int depth = 0;
    while (v && v->kind == PV_REF) {
        v = pdf_get(d, v->ref_num);
        if (++depth > PDF_MAX_RESOLVE) {
            return NULL;
        }
    }
    return v;
}

static pdf_objstm_t *objstm_open(pdf_doc_t *d, int64_t stmnum) {
    pdf_xent_t *e = xent(d, stmnum);
    if (e && e->stm) {
        return e->stm;
    }
    pdf_val_t *st = pdf_get(d, stmnum);
    if (!st || st->kind != PV_STREAM) {
        return NULL;
    }
    size_t dlen = 0;
    const unsigned char *data = pdf_stream_decode(d, st->stream, &dlen);
    if (!data) {
        data = (const unsigned char *)"";
        dlen = 0;
    }
    pdf_val_t *nv = pdf_dget_raw(st->stream->dict, "N");
    pdf_val_t *fv = pdf_dget_raw(st->stream->dict, "First");
    int64_t n = (nv && nv->kind == PV_NUM && nv->is_int) ? nv->inum : 0;
    pdf_objstm_t *sm = (pdf_objstm_t *)pdf_calloc(d, sizeof(*sm));
    if (!sm) {
        return NULL;
    }
    sm->data = data;
    sm->len = dlen;
    sm->first = (fv && fv->kind == PV_NUM && fv->is_int) ? fv->inum : 0;
    /* at most one pair per two tokens of the header: bounded by the data */
    int64_t cap = n < 0 ? 0 : n;
    if (cap > (int64_t)(dlen / 2 + 1)) {
        cap = (int64_t)(dlen / 2 + 1);
    }
    sm->onum = (int64_t *)pdf_big(d, (size_t)(cap ? cap : 1) * sizeof(int64_t));
    sm->ooff = (int64_t *)pdf_big(d, (size_t)(cap ? cap : 1) * sizeof(int64_t));
    if (!sm->onum || !sm->ooff) {
        return NULL;
    }
    size_t p = 0;
    for (int64_t i = 0; i < n && sm->n < cap; i++) {
        pdf_tok_t t1;
        pdf_tok_t t2;
        lex_peek(d, data, dlen, &p, &t1);
        lex_peek(d, data, dlen, &p, &t2);
        if (t1.kind != PT_NUM || t2.kind != PT_NUM) {
            break;
        }
        sm->onum[sm->n] = t1.is_int ? t1.inum : pdf_d2i(t1.num);
        sm->ooff[sm->n] = t2.is_int ? t2.inum : pdf_d2i(t2.num);
        sm->n++;
    }
    e = xent(d, stmnum);
    if (e) {
        e->stm = sm;
    }
    return sm;
}

static pdf_objstm_t *objstm(pdf_doc_t *d, int64_t stmnum) {
    d->objstm_opening++;
    pdf_objstm_t *sm = objstm_open(d, stmnum);
    d->objstm_opening--;
    return sm;
}

/* ── Cross-reference ─────────────────────────────────────────────── */

static void xref_free(pdf_xref_t *x) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, x->v);
    pdf_map_free(&x->by_num);
    memset(x, 0, sizeof(*x));
}

/* The entry for num; created (zeroed) when absent. *fresh says which. */
static pdf_xent_t *xref_slot(pdf_doc_t *d, pdf_xref_t *x, int64_t num, bool *fresh) {
    int64_t i = pdf_map_get(&x->by_num, (uint64_t)num);
    if (i >= 0) {
        *fresh = false;
        return &x->v[i];
    }
    if (x->n == x->cap) {
        int ncap = x->cap ? x->cap * 2 : PDF_MAP_MIN;
        pdf_xent_t *g =
            (pdf_xent_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, x->v, (size_t)ncap * sizeof(*g));
        if (!g) {
            d->nomem = true;
            return NULL;
        }
        x->v = g;
        x->cap = ncap;
    }
    if (!pdf_map_put(&x->by_num, (uint64_t)num, x->n)) {
        d->nomem = true;
        return NULL;
    }
    pdf_xent_t *e = &x->v[x->n++];
    memset(e, 0, sizeof(*e));
    e->num = num;
    *fresh = true;
    return e;
}

/* "[ws]*(\d{1,10})[ws]+(\d{1,5})[ws]*([nf])" at p. */
static bool match_xref_entry(const pdf_doc_t *d, size_t p, int64_t *off, int64_t *gen, char *kind,
                             size_t *end) {
    const unsigned char *s = d->data;
    size_t len = d->len;
    while (p < len && is_ws(s[p])) {
        p++;
    }
    size_t a = p;
    while (p < len && is_digit(s[p])) {
        p++;
    }
    if (p == a || p - a > 10) {
        return false;
    }
    *off = sat_digits(s + a, p - a);
    size_t w = p;
    while (p < len && is_ws(s[p])) {
        p++;
    }
    if (p == w) {
        return false;
    }
    a = p;
    while (p < len && is_digit(s[p])) {
        p++;
    }
    if (p == a || p - a > 5) {
        return false;
    }
    *gen = sat_digits(s + a, p - a);
    while (p < len && is_ws(s[p])) {
        p++;
    }
    if (p >= len || (s[p] != 'n' && s[p] != 'f')) {
        return false;
    }
    *kind = (char)s[p];
    *end = p + 1;
    return true;
}

static bool xref_table(pdf_doc_t *d, size_t p, pdf_xref_t *sec, pdf_val_t **tr) {
    const unsigned char *s = d->data;
    size_t len = d->len;
    for (;;) {
        pdf_tok_t t;
        size_t p2 = p;
        pdf_lex(d, s, len, &p2, &t);
        if (pdf_kw_is(&t, "trailer")) {
            pdf_tok_t t2;
            pdf_lex(d, s, len, &p2, &t2);
            pdf_val_t *v = pdf_parse_value(d, s, len, &p2, &t2, 0);
            if (!v) {
                return false;
            }
            *tr = v->kind == PV_DICT ? v : NULL;
            return true;
        }
        if (t.kind != PT_NUM) {
            return false;
        }
        pdf_tok_t tc;
        pdf_lex(d, s, len, &p2, &tc);
        if (tc.kind != PT_NUM || !tc.is_int) {
            return false;
        }
        int64_t start = t.is_int ? t.inum : pdf_d2i(t.num);
        bool use = t.is_int || t.num == (double)start;
        p = p2;
        for (int64_t i = 0; i < tc.inum; i++) {
            int64_t off;
            int64_t gen;
            char kind;
            size_t end;
            if (!match_xref_entry(d, p, &off, &gen, &kind, &end)) {
                return false;
            }
            p = end;
            if (!use) {
                continue;
            }
            bool fresh;
            pdf_xent_t *e = xref_slot(d, sec, pdf_sat_add(start, i), &fresh);
            if (!e) {
                return false;
            }
            if (fresh && kind == 'n') {
                e->type = 1;
                e->a = off;
                e->b = gen;
            }
        }
    }
}

static int64_t be_field(const unsigned char *p, int w) {
    uint64_t v = 0;
    for (int i = 0; i < w; i++) {
        v = (v << PDF_BITS_PER_BYTE) | p[i];
    }
    return (int64_t)v;
}

static bool num_to_int(const pdf_val_t *v, int64_t *out) {
    if (!v || v->kind != PV_NUM) {
        return false;
    }
    *out = v->is_int ? v->inum : pdf_d2i(v->num);
    return true;
}

static const unsigned char *decode_filters(pdf_doc_t *d, const pdf_stream_t *st, size_t *len,
                                           pdf_buf_t *held_out);

/* The rows of a cross-reference stream's decoded bytes into sec. */
static bool xref_rows(pdf_doc_t *d, pdf_xref_t *sec, const unsigned char *raw, size_t rlen,
                      const int64_t w[3], pdf_val_t **items, int nitems, const int64_t pairs[2]) {
    int64_t rec = w[0] + w[1] + w[2];
    if (rec == 0) {
        return false;
    }
    size_t pos = 0;
    for (int j = 0; j + 1 < nitems; j += 2) {
        int64_t start;
        int64_t count;
        if (items) {
            if (!num_to_int(items[j], &start) || !num_to_int(items[j + 1], &count)) {
                return false;
            }
        } else {
            start = pairs[0];
            count = pairs[1];
        }
        for (int64_t i = 0; i < count; i++) {
            if (pos + (size_t)rec > rlen) {
                break;
            }
            const unsigned char *r = raw + pos;
            int64_t t = w[0] ? be_field(r, (int)w[0]) : 1;
            int64_t f2 = be_field(r + w[0], (int)w[1]);
            int64_t f3 = w[2] ? be_field(r + w[0] + w[1], (int)w[2]) : 0;
            pos += (size_t)rec;
            bool fresh;
            pdf_xent_t *e = xref_slot(d, sec, pdf_sat_add(start, i), &fresh);
            if (!e) {
                return false;
            }
            if (!fresh) {
                continue;
            }
            if (t == 1 || t == 2) {
                e->type = (uint8_t)t;
                e->a = f2;
                e->b = f3;
            }
        }
    }
    return true;
}

/* A cross-reference stream at off into sec. Its decoded bytes are freed once
 * read: a stream parsed here is no cached object, and keeping them made every
 * section that names one stream add a copy until the document closed. */
static bool xref_stream_at(pdf_doc_t *d, int64_t off, pdf_xref_t *sec, pdf_val_t **tr) {
    bool ok;
    pdf_val_t *v = parse_indirect_at(d, off, false, 0, &ok);
    if (!v || v->kind != PV_STREAM) {
        return false;
    }
    pdf_val_t *dict = v->stream->dict;
    pdf_val_t *W = pdf_dget_raw(dict, "W");
    if (!W || W->kind != PV_ARR || W->count != 3) {
        return false;
    }
    int64_t w[3];
    for (int i = 0; i < 3; i++) {
        if (!num_to_int(W->items[i], &w[i]) || w[i] < 0 || w[i] > PDF_MAX_FIELD) {
            return false;
        }
    }
    pdf_val_t *size = pdf_dget_raw(dict, "Size");
    pdf_val_t *index = pdf_dget_raw(dict, "Index");
    bool dflt = !index || (index->kind == PV_ARR && index->count == 0) ||
                (index->kind == PV_NUM && index->num == 0.0) ||
                (index->kind == PV_BOOL && !index->b);
    int64_t pairs[2] = {0, 0}; /* read only on the default /Index (no items) */
    pdf_val_t **items = NULL;
    int nitems = 0;
    if (dflt) {
        if (size && !num_to_int(size, &pairs[1])) {
            return false;
        }
        nitems = 2;
    } else if (index->kind == PV_ARR) {
        items = index->items;
        nitems = index->count;
    } else {
        return false;
    }
    size_t rlen = 0;
    pdf_buf_t held = {0};
    const unsigned char *raw = decode_filters(d, v->stream, &rlen, &held);
    if (!raw) {
        return false;
    }
    d->counts->xref_streams++;
    bool rows_ok = xref_rows(d, sec, raw, rlen, w, items, nitems, pairs);
    buf_free(&held);
    if (!rows_ok) {
        return false;
    }
    *tr = dict;
    return true;
}

static size_t skip_ws(const pdf_doc_t *d, size_t p) {
    while (p < d->len && is_ws(d->data[p])) {
        p++;
    }
    return p;
}

static bool starts_with(const pdf_doc_t *d, size_t p, const char *lit) {
    size_t n = strlen(lit);
    return p <= d->len && d->len - p >= n && memcmp(d->data + p, lit, n) == 0;
}

static bool find_xref_at(pdf_doc_t *d, int64_t off, size_t *out) {
    for (int k = 0; k < 2; k++) {
        int64_t base = pdf_sat_add(off, k ? (int64_t)d->hdr : 0);
        if (base < 0 || (uint64_t)base > d->len) {
            continue;
        }
        size_t p = skip_ws(d, (size_t)base);
        if (starts_with(d, p, "xref")) {
            *out = p;
            return true;
        }
        int64_t num;
        size_t end;
        if (match_obj_hdr(d, (size_t)base, &num, &end)) {
            *out = (size_t)base;
            return true;
        }
    }
    int64_t lo = pdf_sat_add(off, -PDF_XREF_NEAR);
    lo = lo < 0 ? 0 : lo;
    int64_t hi = pdf_sat_add(off, PDF_XREF_NEAR);
    if ((uint64_t)hi > d->len) {
        hi = (int64_t)d->len;
    }
    if (hi - lo >= 4 && (uint64_t)lo < d->len) {
        const unsigned char *x = cbm_memmem(d->data + lo, (size_t)(hi - lo), "xref", 4);
        if (x) {
            d->counts->xref_offset_fixed++;
            *out = (size_t)(x - d->data);
            return true;
        }
    }
    return false;
}

static bool trailer_key_structural(const pdf_kv_t *kv) {
    static const char *const keys[] = {"Prev",   "XRefStm",     "Type",   "W",   "Index",
                                       "Filter", "DecodeParms", "Length", "Size"};
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {
        size_t n = strlen(keys[i]);
        if (kv->klen == n && memcmp(kv->key, keys[i], n) == 0) {
            return true;
        }
    }
    return false;
}

static bool load_xref(pdf_doc_t *d) {
    const unsigned char *s = d->data;
    size_t len = d->len;
    /* the last "startxref" */
    size_t i = len < 9 ? 0 : len - 9 + 1;
    bool found = false;
    while (i-- > 0) {
        if (s[i] == 's' && memcmp(s + i, "startxref", 9) == 0) {
            found = true;
            break;
        }
    }
    if (!found) {
        return false;
    }
    size_t p = i + 9;
    pdf_tok_t t;
    pdf_lex(d, s, len, &p, &t);
    if (t.kind != PT_NUM) {
        return false;
    }
    pdf_map_t seen = {0};
    /* /XRefStm targets already merged: a later (older) section naming one again
     * adds nothing, since every number it holds is in d->xref already */
    pdf_map_t stm_seen = {0};
    kv_list_t trail = {0};
    bool first = true;
    bool ok = true;
    pdf_val_t *offv = new_val(d, PV_NUM);
    if (!offv) {
        return false;
    }
    offv->is_int = t.is_int;
    offv->inum = t.inum;
    pdf_val_t *off = offv;
    while (off && off->kind == PV_NUM && off->is_int &&
           pdf_map_get(&seen, (uint64_t)off->inum) < 0 && seen.n < PDF_MAX_PREV) {
        if (!pdf_map_put(&seen, (uint64_t)off->inum, 1)) {
            d->nomem = true;
            ok = false;
            break;
        }
        size_t at;
        if (!find_xref_at(d, off->inum, &at)) {
            ok = false;
            break;
        }
        pdf_xref_t sec = {0};
        pdf_val_t *tr = NULL;
        if (starts_with(d, at, "xref")) {
            if (!xref_table(d, at + 4, &sec, &tr)) {
                xref_free(&sec);
                ok = false;
                break;
            }
            pdf_val_t *xs = pdf_dget_raw(tr, "XRefStm");
            if (xs && xs->kind == PV_NUM && xs->is_int &&
                pdf_map_get(&stm_seen, (uint64_t)xs->inum) < 0) {
                if (!pdf_map_put(&stm_seen, (uint64_t)xs->inum, 1)) {
                    d->nomem = true;
                    xref_free(&sec);
                    ok = false;
                    break;
                }
                pdf_xref_t sec2 = {0};
                pdf_val_t *tr2 = NULL;
                if (xref_stream_at(d, xs->inum, &sec2, &tr2)) {
                    for (int k = 0; k < sec2.n; k++) {
                        bool fresh;
                        pdf_xent_t *e = xref_slot(d, &sec, sec2.v[k].num, &fresh);
                        if (e && (fresh || e->type == 0)) {
                            e->type = sec2.v[k].type;
                            e->a = sec2.v[k].a;
                            e->b = sec2.v[k].b;
                        }
                    }
                }
                xref_free(&sec2);
            }
        } else if (!xref_stream_at(d, (int64_t)at, &sec, &tr)) {
            xref_free(&sec);
            ok = false;
            break;
        }
        for (int k = 0; k < sec.n; k++) {
            bool fresh;
            pdf_xent_t *e = xref_slot(d, &d->xref, sec.v[k].num, &fresh);
            if (e && fresh) {
                e->type = sec.v[k].type;
                e->a = sec.v[k].a;
                e->b = sec.v[k].b;
            }
        }
        xref_free(&sec);
        for (int k = 0; tr && k < tr->nkv; k++) {
            if (first || !trailer_key_structural(&tr->kv[k])) {
                kl_push(&trail, tr->kv[k].key, tr->kv[k].klen, tr->kv[k].val);
            }
        }
        first = false;
        off = pdf_dget_raw(tr, "Prev");
    }
    pdf_map_free(&seen);
    pdf_map_free(&stm_seen);
    d->trailer = kl_finish_dict(d, &trail, false);
    if (!ok || d->nomem) {
        return false;
    }
    return d->xref.n > 0;
}

static int i64_cmp(const void *x, const void *y) {
    int64_t a = *(const int64_t *)x;
    int64_t b = *(const int64_t *)y;
    return a < b ? -1 : a > b ? 1 : 0;
}

/* The xref's object numbers, ascending (a snapshot). */
static int64_t *xref_sorted_nums(pdf_doc_t *d, int *n) {
    *n = d->xref.n;
    int64_t *v = (int64_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)(*n ? *n : 1) * sizeof(*v));
    if (!v) {
        d->nomem = true;
        *n = 0;
        return NULL;
    }
    for (int i = 0; i < *n; i++) {
        v[i] = d->xref.v[i].num;
    }
    qsort(v, (size_t)*n, sizeof(*v), i64_cmp);
    return v;
}

static void cache_clear(pdf_doc_t *d) {
    for (int i = 0; i < d->xref.n; i++) {
        d->xref.v[i].cached = false;
        d->xref.v[i].obj = NULL;
    }
}

static void reconstruct(pdf_doc_t *d) {
    d->reconstructed = true;
    d->counts->xref_reconstructed++;
    xref_free(&d->xref);
    if (!build_hdrs(d)) {
        return;
    }
    for (int i = 0; i < d->nhdrs; i++) {
        const pdf_hdr_t *h = &d->hdrs[i];
        if (h->num_digits > 10 || h->gen_digits > 5 || h->reg_after) {
            continue;
        }
        bool fresh;
        pdf_xent_t *e = xref_slot(d, &d->xref, h->num, &fresh);
        if (!e) {
            return;
        }
        e->type = 1;
        e->a = (int64_t)h->off;
        e->b = h->gen;
    }
    /* every "trailer[ws]*<<" dict, the later overriding */
    const unsigned char *s = d->data;
    size_t len = d->len;
    kv_list_t trail = {0};
    size_t from = 0;
    while (from + 7 <= len) {
        const unsigned char *x = cbm_memmem(s + from, len - from, "trailer", 7);
        if (!x) {
            break;
        }
        size_t q = (size_t)(x - s) + 7;
        from = q;
        q = skip_ws(d, q);
        if (!starts_with(d, q, "<<")) {
            continue;
        }
        size_t p = q;
        pdf_tok_t t;
        /* read up to the next "trailer": scanned trailers do not overlap either */
        const unsigned char *nx = cbm_memmem(s + q, len - q, "trailer", 7);
        size_t tlen = nx ? (size_t)(nx - s) : len;
        pdf_lex(d, s, tlen, &p, &t);
        pdf_val_t *v = pdf_parse_value(d, s, tlen, &p, &t, 0);
        if (v && v->kind == PV_DICT) {
            for (int k = 0; k < v->nkv; k++) {
                kl_push(&trail, v->kv[k].key, v->kv[k].klen, v->kv[k].val);
            }
        }
        from = q + 2 > from ? q + 2 : from;
    }
    pdf_val_t *tr = kl_finish_dict(d, &trail, true);
    static const char *const xkeys[] = {"Root", "Info", "ID", "Encrypt"};
    bool taken[4] = {false, false, false, false};
    kv_list_t extra = {0};
    int n = 0;
    int64_t *nums = xref_sorted_nums(d, &n);
    for (int i = 0; i < n; i++) {
        pdf_val_t *o = pdf_get(d, nums[i]);
        if (!o || o->kind != PV_STREAM) {
            continue;
        }
        pdf_val_t *typ = pdf_dget_raw(o->stream->dict, "Type");
        if (pdf_is_name(typ, "ObjStm")) {
            pdf_objstm_t *sm = objstm(d, nums[i]);
            for (int k = 0; sm && k < sm->n; k++) {
                bool fresh;
                pdf_xent_t *e = xref_slot(d, &d->xref, sm->onum[k], &fresh);
                if (e && fresh) {
                    e->type = 2;
                    e->a = nums[i];
                    e->b = k;
                }
            }
        } else if (pdf_is_name(typ, "XRef")) {
            for (size_t k = 0; k < sizeof(xkeys) / sizeof(xkeys[0]); k++) {
                pdf_val_t *v = dict_find(o->stream->dict, xkeys[k]);
                if (v && !taken[k] && !dict_find(tr, xkeys[k])) {
                    kl_push(&extra, (const unsigned char *)xkeys[k], strlen(xkeys[k]), v);
                    taken[k] = true;
                }
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, nums);
    if (extra.n && tr) {
        for (int k = 0; k < tr->nkv; k++) {
            kl_push(&extra, tr->kv[k].key, tr->kv[k].klen, tr->kv[k].val);
        }
        /* the scanned trailers come after: keep_last lets them win, and the
         * stream keys were only added where the trailers had none */
        tr = kl_finish_dict(d, &extra, true);
    } else {
        cbm_free(CBM_MEM_CLASS_EXTRACT, extra.v);
    }
    pdf_val_t *root = pdf_resolve(d, pdf_dget_raw(tr, "Root"));
    if (!root || root->kind != PV_DICT) {
        nums = xref_sorted_nums(d, &n);
        for (int i = 0; i < n; i++) {
            pdf_val_t *o = pdf_get(d, nums[i]);
            if (o && o->kind == PV_DICT && pdf_is_name(pdf_dget_raw(o, "Type"), "Catalog")) {
                pdf_val_t *r = new_val(d, PV_REF);
                if (r) {
                    r->ref_num = nums[i];
                    kv_list_t one = {0};
                    for (int k = 0; tr && k < tr->nkv; k++) {
                        kl_push(&one, tr->kv[k].key, tr->kv[k].klen, tr->kv[k].val);
                    }
                    kl_push(&one, (const unsigned char *)"Root", 4, r);
                    tr = kl_finish_dict(d, &one, true);
                }
                break;
            }
        }
        cbm_free(CBM_MEM_CLASS_EXTRACT, nums);
    }
    d->trailer = tr;
    cache_clear(d);
}

bool pdf_doc_open(pdf_doc_t *d) {
    const unsigned char *h = cbm_memmem(d->data, d->len, "%PDF-", 5);
    d->hdr = h ? (size_t)(h - d->data) : 0;
    bool ok = load_xref(d);
    if (ok) {
        pdf_val_t *root = pdf_resolve(d, pdf_dget_raw(d->trailer, "Root"));
        ok = root && root->kind == PV_DICT;
    }
    if (!ok && !d->nomem) {
        reconstruct(d);
    }
    return !d->nomem;
}

void pdf_doc_close(pdf_doc_t *d) {
    if (d->scratch_arena_live) {
        cbm_arena_destroy(&d->scratch_arena);
    }
    xref_free(&d->xref);
    pdf_map_free(&d->fonts);
    cbm_free(CBM_MEM_CLASS_EXTRACT, d->hdrs);
    cbm_free(CBM_MEM_CLASS_EXTRACT, d->scratch);
    for (int i = 0; i < d->nowned; i++) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, d->owned[i]);
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, (void *)d->owned);
    cbm_free(CBM_MEM_CLASS_EXTRACT, (void *)d->font_list);
}

/* ── Filters ─────────────────────────────────────────────────────── */

/* Inflate src into out until the stream ends, an error, or the output passes
 * the guard. chunk: feed the input in the prototype's 4096-byte steps, where
 * an error drops the output of the step it happened in. */
static void inflate_run(const unsigned char *src, size_t n, int wbits, bool chunk, pdf_buf_t *out,
                        bool *ended, bool *errored) {
    *ended = false;
    *errored = false;
    z_stream z;
    memset(&z, 0, sizeof(z));
    if (inflateInit2(&z, wbits) != Z_OK) {
        *errored = true;
        return;
    }
    size_t step = chunk ? PDF_INFLATE_CHUNK : (n ? n : 1);
    for (size_t i = 0; i < n || (n == 0 && i == 0); i += step) {
        size_t cl = (n - i < step) ? n - i : step;
        size_t mark = out->n;
        z.next_in = (Bytef *)(uintptr_t)(src + i);
        z.avail_in = (uInt)cl;
        bool bad = false;
        for (;;) {
            if (!buf_reserve(out, PDF_INFLATE_GROW)) {
                bad = true;
                break;
            }
            z.next_out = out->p + out->n;
            z.avail_out = (uInt)(out->cap - out->n > UINT_MAX ? UINT_MAX : out->cap - out->n);
            uInt before = z.avail_out;
            int r = inflate(&z, Z_NO_FLUSH);
            out->n += before - z.avail_out;
            if (r == Z_STREAM_END) {
                *ended = true;
                break;
            }
            if (r == Z_BUF_ERROR) {
                break;
            }
            if (r != Z_OK) {
                bad = true;
                break;
            }
            if (out->n > PDF_MAX_STREAM_OUT) {
                break;
            }
            if (z.avail_in == 0 && z.avail_out != 0) {
                break;
            }
        }
        if (bad) {
            out->n = mark;
            *errored = true;
            break;
        }
        if (*ended || out->n > PDF_MAX_STREAM_OUT || n == 0) {
            break;
        }
    }
    inflateEnd(&z);
}

static bool f_flate(pdf_doc_t *d, const unsigned char *in, size_t n, pdf_buf_t *out) {
    bool ended;
    bool errored;
    inflate_run(in, n, PDF_ZLIB_WBITS, false, out, &ended, &errored);
    if (out->fail) {
        return false;
    }
    if (ended || out->n > PDF_MAX_STREAM_OUT) {
        return true;
    }
    d->counts->flate_recovered++;
    for (int k = 0; k < 2; k++) {
        out->n = 0;
        const unsigned char *src = k ? in + (n >= 2 ? 2 : n) : in;
        size_t sn = k ? (n >= 2 ? n - 2 : 0) : n;
        inflate_run(src, sn, k ? -PDF_ZLIB_WBITS : PDF_ZLIB_WBITS, true, out, &ended, &errored);
        if (out->fail) {
            return false;
        }
        if (out->n) {
            return true;
        }
    }
    out->n = 0;
    return true;
}

/* PNG (>= 10) and TIFF (2) predictors. false: parameters it cannot apply. */
static bool f_predict(pdf_doc_t *d, pdf_val_t *parms, pdf_buf_t *io) {
    if (!parms || parms->kind != PV_DICT) {
        return true;
    }
    pdf_val_t *pv = pdf_dget(d, parms, "Predictor");
    double pred = 1;
    if (pv) {
        if (pv->kind != PV_NUM) {
            return false;
        }
        pred = pv->num == 0.0 ? 1 : pv->num;
    }
    if (pred == 1) {
        return true;
    }
    static const char *const keys[] = {"Colors", "BitsPerComponent", "Columns"};
    static const int64_t dflt[] = {1, 8, 1};
    int64_t prm[3];
    bool ints = true;
    for (int i = 0; i < 3; i++) {
        pdf_val_t *v = pdf_dget(d, parms, keys[i]);
        if (!v || (v->kind == PV_NUM && v->num == 0.0)) {
            prm[i] = dflt[i];
        } else if (v->kind != PV_NUM) {
            return false;
        } else {
            ints = ints && v->is_int;
            prm[i] = v->is_int ? v->inum : pdf_d2i(v->num);
        }
    }
    int64_t colors = prm[0];
    int64_t bpc = prm[1];
    int64_t cols = prm[2];
    if (pred < PDF_PNG_PRED && !(pred == PDF_TIFF_PRED && bpc == PDF_BITS_PER_BYTE)) {
        return true;
    }
    if (!ints) {
        return false;
    }
    /* products of hostile parameters: computed in double and bounded */
    double bits = (double)colors * (double)bpc;
    double rowd = floor((bits * (double)cols + 7) / PDF_BITS_PER_BYTE);
    double bppd = floor(bits / PDF_BITS_PER_BYTE);
    if (rowd > (double)PDF_MAX_STREAM_OUT) {
        return false;
    }
    int64_t rowlen = rowd < -2 ? -2 : (int64_t)rowd;
    int64_t bpp =
        bppd<1 ? 1 : bppd>(double) PDF_MAX_STREAM_OUT ? (int64_t)PDF_MAX_STREAM_OUT : (int64_t)bppd;
    if (pred >= PDF_PNG_PRED) {
        if (rowlen == -1) {
            return false;
        }
        pdf_buf_t out = {0};
        if (rowlen < 0) {
            buf_free(io);
            *io = out;
            return true;
        }
        size_t rl = (size_t)rowlen;
        unsigned char *prev = (unsigned char *)cbm_calloc(CBM_MEM_CLASS_EXTRACT, rl ? rl : 1);
        if (!prev) {
            d->nomem = true;
            return false;
        }
        for (size_t i = 0; i < io->n; i += rl + 1) {
            unsigned ft = io->p[i];
            if (!buf_reserve(&out, rl ? rl : 1)) {
                break;
            }
            unsigned char *row = out.p + out.n;
            size_t have = io->n - (i + 1) < rl ? io->n - (i + 1) : rl;
            if (have) {
                memcpy(row, io->p + i + 1, have);
            }
            memset(row + have, 0, rl - have);
            size_t b = (size_t)bpp;
            for (size_t j = 0; j < rl; j++) {
                unsigned a = j >= b ? row[j - b] : 0;
                unsigned up = prev[j];
                unsigned c = j >= b ? prev[j - b] : 0;
                switch (ft) {
                case 1:
                    if (j >= b) {
                        row[j] = (unsigned char)(row[j] + a);
                    }
                    break;
                case 2:
                    row[j] = (unsigned char)(row[j] + up);
                    break;
                case 3:
                    row[j] = (unsigned char)(row[j] + ((a + up) >> 1));
                    break;
                case 4: {
                    int p = (int)a + (int)up - (int)c;
                    int pa = abs(p - (int)a);
                    int pb = abs(p - (int)up);
                    int pc = abs(p - (int)c);
                    unsigned pr = (pa <= pb && pa <= pc) ? a : (pb <= pc ? up : c);
                    row[j] = (unsigned char)(row[j] + pr);
                    break;
                }
                default:
                    break;
                }
            }
            if (rl) {
                memcpy(prev, row, rl);
            }
            out.n += rl;
            if (out.n > PDF_MAX_STREAM_OUT) {
                break;
            }
        }
        cbm_free(CBM_MEM_CLASS_EXTRACT, prev);
        if (out.fail) {
            buf_free(&out);
            d->nomem = true;
            return false;
        }
        buf_free(io);
        *io = out;
        return true;
    }
    /* TIFF predictor 2, 8 bits per component */
    if (rowlen == 0) {
        return false;
    }
    if (rowlen < 0) {
        return true;
    }
    size_t rl = (size_t)rowlen;
    size_t b = (size_t)bpp;
    for (size_t r = 0; r < io->n; r += rl) {
        size_t lim = r + rl < io->n ? r + rl : io->n;
        for (size_t j = r + b; j < lim; j++) {
            io->p[j] = (unsigned char)(io->p[j] + io->p[j - b]);
        }
    }
    return true;
}

static void f_lzw(const unsigned char *in, size_t n, double early, pdf_buf_t *out) {
    /* table entries >= 258 are runs of the output: (position, length) */
    size_t *pos = (size_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, PDF_LZW_TABLE * sizeof(size_t));
    size_t *lenv = (size_t *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, PDF_LZW_TABLE * sizeof(size_t));
    if (!pos || !lenv) {
        out->fail = true;
        cbm_free(CBM_MEM_CLASS_EXTRACT, pos);
        cbm_free(CBM_MEM_CLASS_EXTRACT, lenv);
        return;
    }
    int bits = PDF_LZW_MIN_BITS;
    uint32_t buf = 0;
    int nbits = 0;
    int64_t table = PDF_LZW_FIRST; /* the prototype's table length (it never shrinks below) */
    bool have_prev = false;
    size_t prev_pos = 0;
    size_t prev_len = 0;
    for (size_t i = 0; i < n; i++) {
        buf = (buf << PDF_BITS_PER_BYTE) | in[i];
        nbits += PDF_BITS_PER_BYTE;
        while (nbits >= bits) {
            nbits -= bits;
            int64_t code = (int64_t)((buf >> nbits) & ((1U << bits) - 1));
            if (code == PDF_LZW_CLEAR) {
                table = PDF_LZW_FIRST;
                bits = PDF_LZW_MIN_BITS;
                have_prev = false;
                continue;
            }
            if (code == PDF_LZW_EOD) {
                goto done;
            }
            size_t at = out->n;
            if (!have_prev) {
                if (code >= table) {
                    goto done;
                }
                if (!buf_byte(out, (unsigned char)code)) {
                    goto done;
                }
            } else if (code < table) {
                if (code < PDF_LZW_CLEAR) {
                    if (!buf_byte(out, (unsigned char)code)) {
                        goto done;
                    }
                } else {
                    size_t sp = pos[code];
                    size_t sl = lenv[code];
                    if (!buf_reserve(out, sl)) {
                        goto done;
                    }
                    memmove(out->p + out->n, out->p + sp, sl);
                    out->n += sl;
                }
                if (table < PDF_LZW_TABLE) {
                    pos[table] = prev_pos;
                    lenv[table] = prev_len + 1;
                }
                table++;
            } else if (code == table) {
                size_t sl = prev_len + 1;
                if (!buf_reserve(out, sl)) {
                    goto done;
                }
                for (size_t k = 0; k < sl; k++) {
                    out->p[out->n + k] = out->p[prev_pos + k];
                }
                out->n += sl;
                if (table < PDF_LZW_TABLE) {
                    pos[table] = prev_pos;
                    lenv[table] = sl;
                }
                table++;
            } else {
                goto done;
            }
            have_prev = true;
            prev_pos = at;
            prev_len = out->n - at;
            if ((double)table + early >= (double)(1U << bits) && bits < PDF_LZW_MAX_BITS) {
                bits++;
            }
            if (out->n > PDF_MAX_STREAM_OUT) {
                goto done;
            }
        }
    }
done:
    cbm_free(CBM_MEM_CLASS_EXTRACT, pos);
    cbm_free(CBM_MEM_CLASS_EXTRACT, lenv);
}

/* Python's base64.a85decode: false on a character or group it rejects. */
static bool a85_strict(const unsigned char *in, size_t n, pdf_buf_t *out) {
    static const unsigned char pad[4] = {'u', 'u', 'u', 'u'};
    unsigned char cur[PDF_A85_GROUP];
    int nc = 0;
    out->n = 0;
    for (size_t i = 0; i < n + 4; i++) {
        unsigned char x = i < n ? in[i] : pad[i - n];
        if (x >= '!' && x <= 'u') {
            cur[nc++] = x;
            if (nc == PDF_A85_GROUP) {
                uint64_t acc = 0;
                for (int k = 0; k < PDF_A85_GROUP; k++) {
                    acc = acc * PDF_A85_BASE + (uint64_t)(cur[k] - '!');
                }
                if (acc > 0xFFFFFFFFULL) {
                    return false;
                }
                unsigned char b4[4] = {(unsigned char)(acc >> 24), (unsigned char)(acc >> 16),
                                       (unsigned char)(acc >> 8), (unsigned char)acc};
                if (!buf_put(out, b4, 4)) {
                    return false;
                }
                nc = 0;
            }
        } else if (x == 'z') {
            if (nc) {
                return false;
            }
            static const unsigned char zero[4] = {0, 0, 0, 0};
            if (!buf_put(out, zero, 4)) {
                return false;
            }
        } else if (x == ' ' || x == '\t' || x == '\n' || x == '\r' || x == '\v') {
            continue;
        } else {
            return false;
        }
    }
    size_t padding = (size_t)(4 - nc);
    out->n = padding >= out->n ? 0 : out->n - padding;
    return true;
}

static void f_a85(pdf_doc_t *d, const unsigned char *in, size_t n, pdf_buf_t *out) {
    unsigned char *t = (unsigned char *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, n ? n : 1);
    if (!t) {
        d->nomem = true;
        out->fail = true;
        return;
    }
    size_t k = 0;
    for (size_t i = 0; i < n; i++) {
        if (!is_ws(in[i])) {
            t[k++] = in[i];
        }
    }
    size_t s0 = 0;
    if (k >= 2 && t[0] == '<' && t[1] == '~') {
        s0 = 2;
    }
    const unsigned char *e = cbm_memmem(t + s0, k - s0, "~>", 2);
    size_t m = e ? (size_t)(e - (t + s0)) : k - s0;
    if (!a85_strict(t + s0, m, out) && !out->fail) {
        if (!a85_strict(t + s0, m - m % PDF_A85_GROUP, out) && !out->fail) {
            out->n = 0;
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, t);
}

static void f_ahx(const unsigned char *in, size_t n, pdf_buf_t *out) {
    const unsigned char *gt = (const unsigned char *)memchr(in, '>', n);
    size_t e = gt ? (size_t)(gt - in) : n;
    int hi = -1;
    for (size_t i = 0; i < e; i++) {
        int v = hex_val(in[i]);
        if (v < 0) {
            continue;
        }
        if (hi < 0) {
            hi = v;
        } else if (!buf_byte(out, (unsigned char)(hi * PDF_HEX_BASE + v))) {
            return;
        } else {
            hi = -1;
        }
    }
    if (hi >= 0) {
        buf_byte(out, (unsigned char)(hi * PDF_HEX_BASE));
    }
}

static void f_rld(const unsigned char *in, size_t n, pdf_buf_t *out) {
    size_t i = 0;
    while (i < n) {
        unsigned L = in[i];
        if (L == PDF_RLD_EOD) {
            break;
        }
        if (L < PDF_RLD_EOD) {
            size_t take = (size_t)L + 1;
            size_t have = n - (i + 1) < take ? n - (i + 1) : take;
            if (!buf_put(out, in + i + 1, have)) {
                return;
            }
            i += (size_t)L + 2;
        } else {
            if (i + 1 < n) {
                size_t rep = 257 - (size_t)L;
                if (!buf_reserve(out, rep)) {
                    return;
                }
                memset(out->p + out->n, in[i + 1], rep);
                out->n += rep;
            }
            i += 2;
        }
        if (out->n > PDF_MAX_STREAM_OUT) {
            return;
        }
    }
}

typedef enum {
    FL_FLATE,
    FL_LZW,
    FL_A85,
    FL_AHX,
    FL_RLD,
    FL_CRYPT,
    FL_IMAGE,
    FL_UNKNOWN,
} pdf_filter_t;

static pdf_filter_t filter_kind(const pdf_val_t *f) {
    if (!f || f->kind != PV_NAME) {
        return FL_UNKNOWN;
    }
    static const struct {
        const char *name;
        pdf_filter_t kind;
    } names[] = {
        {"FlateDecode", FL_FLATE},
        {"Fl", FL_FLATE},
        {"LZWDecode", FL_LZW},
        {"LZW", FL_LZW},
        {"ASCII85Decode", FL_A85},
        {"A85", FL_A85},
        {"ASCIIHexDecode", FL_AHX},
        {"AHx", FL_AHX},
        {"RunLengthDecode", FL_RLD},
        {"RL", FL_RLD},
        {"Crypt", FL_CRYPT},
        {"DCTDecode", FL_IMAGE},
        {"DCT", FL_IMAGE},
        {"JPXDecode", FL_IMAGE},
        {"CCITTFaxDecode", FL_IMAGE},
        {"CCF", FL_IMAGE},
        {"JBIG2Decode", FL_IMAGE},
    };
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        if (pdf_is_name(f, names[i].name)) {
            return names[i].kind;
        }
    }
    return FL_UNKNOWN;
}

/* The stream's bytes through its filters: the raw bytes themselves, or a
 * buffer in *held that the caller owns. NULL: a filter failed or is not read. */
static const unsigned char *decode_filters(pdf_doc_t *d, const pdf_stream_t *st, size_t *len,
                                           pdf_buf_t *held_out) {
    *len = 0;
    pdf_val_t *filters = pdf_resolve(d, pdf_dget_raw(st->dict, "Filter"));
    pdf_val_t *parms = pdf_resolve(d, pdf_dget_raw(st->dict, "DecodeParms"));
    pdf_val_t *single[1] = {filters};
    pdf_val_t **fl = single;
    int nf = 0;
    if (filters && filters->kind == PV_ARR) {
        fl = filters->items;
        nf = filters->count;
    } else if (filters && filters->kind != PV_NULL) {
        nf = 1;
    }
    const unsigned char *cur = st->raw;
    size_t cur_len = st->raw_len;
    pdf_buf_t held = {0}; /* the buffer cur points into, when it is ours */
    for (int i = 0; i < nf; i++) {
        pdf_filter_t kind = filter_kind(pdf_resolve(d, fl[i]));
        pdf_val_t *p = NULL;
        if (parms && parms->kind == PV_ARR) {
            p = i < parms->count ? pdf_resolve(d, parms->items[i]) : NULL;
        } else {
            p = parms;
        }
        if (kind == FL_CRYPT) {
            continue;
        }
        if (kind == FL_IMAGE || kind == FL_UNKNOWN) {
            if (kind == FL_UNKNOWN) {
                d->counts->filters_unsupported++;
            }
            buf_free(&held);
            return NULL;
        }
        pdf_buf_t out = {0};
        bool ok = true;
        switch (kind) {
        case FL_FLATE:
            ok = f_flate(d, cur, cur_len, &out) && f_predict(d, p, &out);
            break;
        case FL_LZW: {
            double early = 1;
            if (p && p->kind == PV_DICT) {
                pdf_val_t *ev = pdf_dget(d, p, "EarlyChange");
                if (ev && ev->kind != PV_NUM) {
                    ok = false;
                } else if (ev) {
                    early = ev->num;
                }
            }
            if (ok) {
                f_lzw(cur, cur_len, early, &out);
                ok = f_predict(d, p, &out);
            }
            break;
        }
        case FL_A85:
            f_a85(d, cur, cur_len, &out);
            break;
        case FL_AHX:
            f_ahx(cur, cur_len, &out);
            break;
        case FL_RLD:
            f_rld(cur, cur_len, &out);
            break;
        default:
            break;
        }
        if (out.fail) {
            d->nomem = true;
            ok = false;
        }
        buf_free(&held);
        if (!ok) {
            buf_free(&out);
            return NULL;
        }
        if (out.n > PDF_MAX_STREAM_OUT) {
            d->counts->stream_guard_hits++;
            out.n = PDF_MAX_STREAM_OUT;
        }
        held = out;
        cur = held.p ? held.p : (const unsigned char *)"";
        cur_len = held.n;
    }
    *held_out = held;
    *len = cur_len;
    return cur;
}

const unsigned char *pdf_stream_decode(pdf_doc_t *d, pdf_stream_t *st, size_t *len) {
    if (st->done) {
        *len = st->dec_len;
        return st->decoded;
    }
    st->done = true;
    st->decoded = NULL;
    st->dec_len = 0;
    *len = 0;
    pdf_buf_t held = {0};
    size_t cur_len;
    const unsigned char *cur = decode_filters(d, st, &cur_len, &held);
    if (!cur) {
        return NULL;
    }
    if (held.p) {
        unsigned char *fit =
            (unsigned char *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, held.p, held.n ? held.n : 1);
        if (fit) {
            held.p = fit;
        }
        if (!pdf_own(d, held.p)) {
            return NULL;
        }
        cur = held.p;
    }
    st->decoded = cur;
    st->dec_len = cur_len;
    *len = cur_len;
    return cur;
}
