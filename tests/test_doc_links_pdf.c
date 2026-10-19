/*
 * test_doc_links_pdf.c — PDF documents -> Sections and MENTIONS edges.
 *
 * The text-layer extractor (internal/cbm/pdf: fonts, encodings, CMaps,
 * cross-reference forms, filters, layout, hostile input), the mention scanner
 * (doc_pdf.c, through its test seam: expected tokens are the field-test
 * scanner's own output for the same text), the resolver (doc_links_pdf.c:
 * tiers, owner-qualified Go methods, hygiene, unresolved reasons), the
 * snippet of a page, and incremental == full. PDFs are built here, byte by
 * byte, with their cross-reference offsets computed.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"

#include "cbm.h"
#include "doclink.h"
#include "foundation/compat_thread.h"
#include "mcp/mcp.h"
#include "pdf/pdf.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

/* ── building PDFs ───────────────────────────────────────────────── */

typedef struct {
    unsigned char *p;
    size_t n;
    size_t cap;
} tp_buf_t;

static void tp_put(tp_buf_t *b, const void *s, size_t n) {
    if (b->n + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap * 2 : 4096;
        while (cap < b->n + n + 1) {
            cap *= 2;
        }
        b->p = (unsigned char *)realloc(b->p, cap);
        b->cap = cap;
    }
    memcpy(b->p + b->n, s, n);
    b->n += n;
    b->p[b->n] = 0;
}

static void tp_puts(tp_buf_t *b, const char *s) {
    tp_put(b, s, strlen(s));
}

/* One object's body: a value, or a dict plus stream data. */
typedef struct {
    const char *dict; /* the value, or the stream's dict without /Length */
    const void *data; /* stream data, or NULL */
    size_t len;
} tp_obj_t;

/* A PDF of objects 1..n (objs[i] is object i+1) with a classic xref table;
 * `trailer_extra` is added to the trailer dict. Free with free(). */
static unsigned char *tp_pdf(const tp_obj_t *objs, int n, const char *trailer_extra,
                             size_t *out_len) {
    tp_buf_t b = {0};
    tp_puts(&b, "%PDF-1.7\n%\xe2\xe3\xcf\xd3\n");
    size_t *off = (size_t *)calloc((size_t)n + 1, sizeof(size_t));
    char line[256];
    for (int i = 0; i < n; i++) {
        off[i + 1] = b.n;
        snprintf(line, sizeof(line), "%d 0 obj\n", i + 1);
        tp_puts(&b, line);
        if (objs[i].data) {
            snprintf(line, sizeof(line), "<< /Length %zu ", objs[i].len);
            tp_puts(&b, line);
            tp_puts(&b, objs[i].dict);
            tp_puts(&b, " >>\nstream\n");
            tp_put(&b, objs[i].data, objs[i].len);
            tp_puts(&b, "\nendstream");
        } else {
            tp_puts(&b, objs[i].dict);
        }
        tp_puts(&b, "\nendobj\n");
    }
    size_t xref = b.n;
    snprintf(line, sizeof(line), "xref\n0 %d\n0000000000 65535 f \n", n + 1);
    tp_puts(&b, line);
    for (int i = 1; i <= n; i++) {
        snprintf(line, sizeof(line), "%010zu 00000 n \n", off[i]);
        tp_puts(&b, line);
    }
    snprintf(line, sizeof(line),
             "trailer\n<< /Size %d /Root 1 0 R %s >>\nstartxref\n%zu\n%%%%EOF\n", n + 1,
             trailer_extra ? trailer_extra : "", xref);
    tp_puts(&b, line);
    free(off);
    *out_len = b.n;
    return b.p;
}

static unsigned char *tp_flate(const void *src, size_t n, size_t *out_len) {
    uLongf cap = compressBound((uLong)n);
    unsigned char *out = (unsigned char *)malloc(cap);
    compress(out, &cap, (const Bytef *)src, (uLong)n);
    *out_len = cap;
    return out;
}

#define TP_CATALOG "<< /Type /Catalog /Pages 2 0 R >>"
#define TP_HELV "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"

/* A one-page PDF whose content is `content` with font F1 = Helvetica. */
static unsigned char *tp_simple(const char *content, size_t *len) {
    tp_obj_t o[5] = {
        {TP_CATALOG, NULL, 0},
        {"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", NULL, 0},
        {"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 "
         "0 R >> >> /Contents 5 0 R >>",
         NULL, 0},
        {TP_HELV, NULL, 0},
        {"", content, strlen(content)},
    };
    return tp_pdf(o, 5, NULL, len);
}

typedef struct {
    CBMArena arena;
    cbm_pdf_result_t r;
} tp_text_t;

static bool tp_extract(const unsigned char *pdf, size_t len, tp_text_t *t) {
    cbm_arena_init(&t->arena);
    return cbm_pdf_extract(pdf, len, &t->arena, &t->r);
}

/* The smallest stack a thread of this program runs on (the daemon's). */
#define TP_SMALL_STACK (256 * 1024)

typedef struct {
    const unsigned char *pdf;
    size_t len;
    tp_text_t *t;
    bool ok;
} tp_job_t;

static void *tp_extract_job(void *arg) {
    tp_job_t *j = (tp_job_t *)arg;
    j->ok = tp_extract(j->pdf, j->len, j->t);
    return NULL;
}

/* tp_extract on a thread with the smallest stack: an input that nests calls
 * per object crashes here long before it would on a worker's 8 MiB. */
static bool tp_extract_small_stack(const unsigned char *pdf, size_t len, tp_text_t *t) {
    tp_job_t j = {pdf, len, t, false};
    cbm_thread_t th;
    if (cbm_thread_create(&th, TP_SMALL_STACK, tp_extract_job, &j) != 0) {
        return false;
    }
    cbm_thread_join(&th);
    return j.ok;
}

/* "N 0 obj\n" for object n at the end of b; records its offset. */
static void tp_obj_head(tp_buf_t *b, size_t *off, int n) {
    char line[64];
    off[n] = b->n;
    snprintf(line, sizeof(line), "%d 0 obj\n", n);
    tp_puts(b, line);
}

/* A classic xref table for objects 1..n at off[] and the trailer. */
static void tp_classic_tail(tp_buf_t *b, const size_t *off, int n) {
    char line[128];
    size_t xref = b->n;
    snprintf(line, sizeof(line), "xref\n0 %d\n0000000000 65535 f \n", n + 1);
    tp_puts(b, line);
    for (int i = 1; i <= n; i++) {
        snprintf(line, sizeof(line), "%010zu 00000 n \n", off[i]);
        tp_puts(b, line);
    }
    snprintf(line, sizeof(line), "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%zu\n%%%%EOF\n",
             n + 1, xref);
    tp_puts(b, line);
}

#define TP_PAGE_F1_5 \
    "<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"

/* A page whose content stream's /Length names a stream whose /Length names
 * the next one, `depth` streams deep (the last one's /Length is a number). */
static unsigned char *tp_length_chain(int depth, size_t *out_len) {
    int n = 4 + depth;
    size_t *off = (size_t *)calloc((size_t)n + 1, sizeof(size_t));
    tp_buf_t b = {0};
    tp_puts(&b, "%PDF-1.7\n");
    const char *head[4] = {TP_CATALOG, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", TP_PAGE_F1_5,
                           TP_HELV};
    for (int i = 1; i <= 4; i++) {
        tp_obj_head(&b, off, i);
        tp_puts(&b, head[i - 1]);
        tp_puts(&b, "\nendobj\n");
    }
    char line[96];
    for (int i = 5; i <= n; i++) {
        tp_obj_head(&b, off, i);
        if (i < n) {
            snprintf(line, sizeof(line), "<< /Length %d 0 R >>\nstream\n", i + 1);
        } else {
            snprintf(line, sizeof(line), "<< /Length 1 >>\nstream\n");
        }
        tp_puts(&b, line);
        tp_puts(&b, i == 5 ? "BT /F1 12 Tf 72 700 Td (chain) Tj ET" : "x");
        tp_puts(&b, "\nendstream\nendobj\n");
    }
    tp_classic_tail(&b, off, n);
    free(off);
    *out_len = b.n;
    return b.p;
}

/* The page object lies in object stream C1, whose /Length lies in object
 * stream C2, whose /Length lies in C3, ... `depth` streams deep; a
 * cross-reference stream places them. Objects: 1-5 as tp_simple (3 in C1),
 * C_i = 5 + i, L_i (C_i's length, in C_{i+1}) = 5 + depth + i, the xref stream
 * last. */
static unsigned char *tp_objstm_chain(int depth, size_t *out_len) {
    int x = 5 + 2 * depth; /* the xref stream */
    size_t *off = (size_t *)calloc((size_t)x + 1, sizeof(size_t));
    size_t *data_len = (size_t *)calloc((size_t)depth + 1, sizeof(size_t));
    tp_buf_t b = {0};
    tp_puts(&b, "%PDF-1.7\n");
    const char *plain[5] = {TP_CATALOG, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", NULL, TP_HELV,
                            NULL};
    char line[160];
    for (int i = 1; i <= 5; i++) {
        if (i == 3) {
            continue;
        }
        tp_obj_head(&b, off, i);
        if (i == 5) {
            const char *content = "BT /F1 12 Tf 72 700 Td (objstm chain) Tj ET";
            snprintf(line, sizeof(line), "<< /Length %zu >>\nstream\n", strlen(content));
            tp_puts(&b, line);
            tp_puts(&b, content);
            tp_puts(&b, "\nendstream");
        } else {
            tp_puts(&b, plain[i - 1]);
        }
        tp_puts(&b, "\nendobj\n");
    }
    for (int i = 1; i <= depth; i++) {
        char hdr[48];
        char body[160];
        if (i == 1) {
            snprintf(hdr, sizeof(hdr), "3 0 ");
            snprintf(body, sizeof(body), "%s", TP_PAGE_F1_5);
        } else {
            snprintf(hdr, sizeof(hdr), "%d 0 ", 5 + depth + i - 1);
            snprintf(body, sizeof(body), "%zu", data_len[i - 1]);
        }
        data_len[i] = strlen(hdr) + strlen(body);
        tp_obj_head(&b, off, 5 + i);
        if (i < depth) {
            snprintf(line, sizeof(line),
                     "<< /Type /ObjStm /N 1 /First %zu /Length %d 0 R >>\nstream\n", strlen(hdr),
                     5 + depth + i);
        } else {
            snprintf(line, sizeof(line),
                     "<< /Type /ObjStm /N 1 /First %zu /Length %zu >>\nstream\n", strlen(hdr),
                     data_len[i]);
        }
        tp_puts(&b, line);
        tp_puts(&b, hdr);
        tp_puts(&b, body);
        tp_puts(&b, "\nendstream\nendobj\n");
    }
    off[x] = b.n;
    /* rows W [1 4 2]: type, offset or containing stream, generation or index */
    size_t nrows = (size_t)x + 1;
    unsigned char *rows = (unsigned char *)calloc(nrows, 7);
    for (int i = 1; i <= x; i++) {
        unsigned char *r = rows + (size_t)i * 7;
        bool in_stm = i == 3 || (i > 5 + depth && i < x);
        uint32_t f2 = in_stm ? (uint32_t)(i == 3 ? 6 : i - depth + 1) : (uint32_t)off[i];
        r[0] = in_stm ? 2 : 1;
        r[1] = (unsigned char)(f2 >> 24);
        r[2] = (unsigned char)(f2 >> 16);
        r[3] = (unsigned char)(f2 >> 8);
        r[4] = (unsigned char)f2;
    }
    snprintf(line, sizeof(line),
             "%d 0 obj\n<< /Type /XRef /Size %d /W [1 4 2] /Root 1 0 R /Length %zu >>\nstream\n", x,
             x + 1, nrows * 7);
    tp_puts(&b, line);
    tp_put(&b, rows, nrows * 7);
    tp_puts(&b, "\nendstream\nendobj\n");
    snprintf(line, sizeof(line), "startxref\n%zu\n%%%%EOF\n", off[x]);
    tp_puts(&b, line);
    free(rows);
    free(data_len);
    free(off);
    *out_len = b.n;
    return b.p;
}

/* ── extractor ───────────────────────────────────────────────────── */

TEST(pdf_extract_layout) {
    /* inherited resources, two pages, a line break by Td, a word break by TJ
     * kerning, a new text object further down */
    const char *c1 = "BT /F1 12 Tf 72 700 Td (Hello src/auth/handler.go) Tj 0 -14 Td (second line) "
                     "Tj ET\nBT /F1 12 Tf 72 600 Td [(word)-1000(gap)] TJ ET";
    const char *c2 = "BT /F1 12 Tf 72 700 Td (page two) Tj ET";
    tp_obj_t o[7] = {
        {TP_CATALOG, NULL, 0},
        {"<< /Type /Pages /Kids [3 0 R 6 0 R] /Count 2 /Resources << /Font << /F1 4 0 R >> >> "
         "/MediaBox [0 0 612 792] >>",
         NULL, 0},
        {"<< /Type /Page /Parent 2 0 R /Contents 5 0 R >>", NULL, 0},
        {TP_HELV, NULL, 0},
        {"", c1, strlen(c1)},
        {"<< /Type /Page /Parent 2 0 R /Contents 7 0 R >>", NULL, 0},
        {"", c2, strlen(c2)},
    };
    size_t len;
    unsigned char *pdf = tp_pdf(o, 7, NULL, &len);
    tp_text_t t;
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.npages, 2);
    ASSERT_STR_EQ(t.r.pages[0].text, "Hello src/auth/handler.go\nsecond line\nword gap");
    ASSERT_STR_EQ(t.r.pages[1].text, "page two");
    ASSERT_EQ(t.r.pages[0].unmapped, 0);
    ASSERT_TRUE(t.r.pages[0].glyphs > 30);
    cbm_arena_destroy(&t.arena);
    free(pdf);
    PASS();
}

TEST(pdf_extract_fonts) {
    /* Type0 Identity-H through a ToUnicode CMap (bfchar, a ligature, a
     * bfrange, an unmapped code), and a simple font with /Differences */
    static const char cmap[] = "/CIDInit /ProcSet findresource begin 12 dict begin begincmap\n"
                               "1 begincodespacerange <0000> <FFFF> endcodespacerange\n"
                               "2 beginbfchar <0001> <0041> <0002> <00660069> endbfchar\n"
                               "1 beginbfrange <0003> <0005> <0061> endbfrange\n"
                               "endcmap CMapName currentdict /CMap defineresource pop end end\n";
    const char *c = "BT /F1 10 Tf 10 700 Td <000100020003000400050006> Tj ET\n"
                    "BT /F2 10 Tf 10 600 Td (AB) Tj ET";
    tp_obj_t o[8] = {
        {TP_CATALOG, NULL, 0},
        {"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", NULL, 0},
        {"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 "
         "R /F2 8 0 R >> >> /Contents 5 0 R >>",
         NULL, 0},
        {"<< /Type /Font /Subtype /Type0 /BaseFont /ABCDEE+Foo /Encoding /Identity-H "
         "/DescendantFonts [6 0 R] /ToUnicode 7 0 R >>",
         NULL, 0},
        {"", c, strlen(c)},
        {"<< /Type /Font /Subtype /CIDFontType2 /BaseFont /Foo /DW 500 /W [1 [600 600] 3 5 "
         "700] >>",
         NULL, 0},
        {"", cmap, sizeof(cmap) - 1},
        {"<< /Type /Font /Subtype /Type1 /BaseFont /Times-Roman /Encoding << /BaseEncoding "
         "/WinAnsiEncoding /Differences [65 /B /A] >> >>",
         NULL, 0},
    };
    size_t len;
    unsigned char *pdf = tp_pdf(o, 8, NULL, &len);
    tp_text_t t;
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_STR_EQ(t.r.pages[0].text, "Afiabc\xEF\xBF\xBD\nBA");
    ASSERT_EQ(t.r.pages[0].unmapped, 1);
    cbm_arena_destroy(&t.arena);
    free(pdf);
    PASS();
}

/* A PDF whose page and font live in a Flate object stream, indexed by a
 * PNG-predicted xref stream, with no classic table at all. */
static unsigned char *tp_compressed(size_t *out_len) {
    const char *content = "BT /F1 12 Tf 72 700 Td (from an object stream) Tj ET";
    size_t clen;
    unsigned char *cz = tp_flate(content, strlen(content), &clen);
    /* object stream: objects 3 (page) and 4 (font) */
    const char *o3 = "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << "
                     "/F1 4 0 R >> >> /Contents 5 0 R >>";
    const char *o4 = TP_HELV;
    char hdr[64];
    snprintf(hdr, sizeof(hdr), "3 0 4 %zu ", strlen(o3) + 1);
    tp_buf_t stm = {0};
    tp_puts(&stm, hdr);
    size_t first = stm.n;
    tp_puts(&stm, o3);
    tp_puts(&stm, " ");
    tp_puts(&stm, o4);
    size_t sz;
    unsigned char *stmz = tp_flate(stm.p, stm.n, &sz);
    tp_buf_t b = {0};
    tp_puts(&b, "%PDF-1.7\n");
    size_t off[8] = {0};
    char line[256];
    off[1] = b.n;
    tp_puts(&b, "1 0 obj\n" TP_CATALOG "\nendobj\n");
    off[2] = b.n;
    tp_puts(&b, "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");
    off[5] = b.n;
    snprintf(line, sizeof(line), "5 0 obj\n<< /Length %zu /Filter /FlateDecode >>\nstream\n", clen);
    tp_puts(&b, line);
    tp_put(&b, cz, clen);
    tp_puts(&b, "\nendstream\nendobj\n");
    off[6] = b.n;
    snprintf(line, sizeof(line),
             "6 0 obj\n<< /Type /ObjStm /N 2 /First %zu /Length %zu /Filter /FlateDecode "
             ">>\nstream\n",
             first, sz);
    tp_puts(&b, line);
    tp_put(&b, stmz, sz);
    tp_puts(&b, "\nendstream\nendobj\n");
    off[7] = b.n;
    /* xref stream rows: W [1 2 1], PNG Up/None rows of 4 bytes + filter byte */
    unsigned char rows[8][5];
    for (int i = 0; i < 8; i++) {
        unsigned type = (i == 0) ? 0 : (i == 3 || i == 4) ? 2 : 1;
        unsigned f2 = (type == 1) ? (unsigned)off[i] : (type == 2) ? 6U : 0U;
        unsigned f3 = (i == 4) ? 1U : 0U;
        rows[i][0] = 0; /* PNG filter: None */
        rows[i][1] = (unsigned char)type;
        rows[i][2] = (unsigned char)(f2 >> 8);
        rows[i][3] = (unsigned char)f2;
        rows[i][4] = (unsigned char)f3;
    }
    size_t xz;
    unsigned char *xrz = tp_flate(rows, sizeof(rows), &xz);
    snprintf(line, sizeof(line),
             "7 0 obj\n<< /Type /XRef /Size 8 /W [1 2 1] /Root 1 0 R /Length %zu /Filter "
             "/FlateDecode /DecodeParms << /Predictor 12 /Columns 4 >> >>\nstream\n",
             xz);
    tp_puts(&b, line);
    tp_put(&b, xrz, xz);
    tp_puts(&b, "\nendstream\nendobj\n");
    snprintf(line, sizeof(line), "startxref\n%zu\n%%%%EOF\n", off[7]);
    tp_puts(&b, line);
    free(cz);
    free(stm.p);
    free(stmz);
    free(xrz);
    *out_len = b.n;
    return b.p;
}

TEST(pdf_extract_structure) {
    size_t len;
    unsigned char *pdf = tp_compressed(&len);
    tp_text_t t;
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.npages, 1);
    ASSERT_STR_EQ(t.r.pages[0].text, "from an object stream");
    ASSERT_EQ(t.r.counts.xref_reconstructed, 0);
    cbm_arena_destroy(&t.arena);
    free(pdf);

    /* a wrong startxref: the xref section is found near it */
    unsigned char *ok = tp_simple("BT /F1 12 Tf 72 700 Td (rebuilt) Tj ET", &len);
    char *sx = strstr((char *)ok, "startxref\n");
    ASSERT_NOT_NULL(sx);
    sx[strlen("startxref\n")] = '9';
    ASSERT_TRUE(tp_extract(ok, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_STR_EQ(t.r.pages[0].text, "rebuilt");
    ASSERT_EQ(t.r.counts.xref_offset_fixed, 1);
    ASSERT_EQ(t.r.counts.xref_reconstructed, 0);
    cbm_arena_destroy(&t.arena);
    /* no xref section at all: rebuilt from the object headers, same text */
    char *xr = strstr((char *)ok, "xref\n0 ");
    ASSERT_NOT_NULL(xr);
    memcpy(xr, "XXXX", 4);
    ASSERT_TRUE(tp_extract(ok, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_STR_EQ(t.r.pages[0].text, "rebuilt");
    ASSERT_EQ(t.r.counts.xref_reconstructed, 1);
    cbm_arena_destroy(&t.arena);
    free(ok);

    /* a Form XObject with its own resources, a self-referencing form, an
     * inline image with binary data, content split over two streams */
    const char *page_a = "q 1 0 0 1 100 500 cm /Fm1 Do Q /Fm2 Do\n";
    const char *page_b = "BI /W 2 /H 2 /BPC 8 /CS /G ID \x01\xff(junk)\xfe EI\n"
                         "BT /F1 12 Tf 72 400 Td (after image) Tj ET";
    const char *form = "BT /F1 12 Tf 0 0 Td (in form) Tj ET";
    const char *self = "/Fm2 Do BT /F1 12 Tf 0 300 Td (self) Tj ET";
    tp_obj_t o[9] = {
        {TP_CATALOG, NULL, 0},
        {"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", NULL, 0},
        {"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /XObject << /Fm1 6 "
         "0 R /Fm2 9 0 R >> /Font << /F1 4 0 R >> >> /Contents [5 0 R 7 0 R] >>",
         NULL, 0},
        {TP_HELV, NULL, 0},
        {"", page_a, strlen(page_a)},
        {"/Type /XObject /Subtype /Form /BBox [0 0 200 200] /Resources << /Font << /F1 8 0 R "
         ">> >>",
         form, strlen(form)},
        {"", page_b, strlen(page_b)},
        {TP_HELV, NULL, 0},
        {"/Type /XObject /Subtype /Form /BBox [0 0 200 200]", self, strlen(self)},
    };
    unsigned char *fp = tp_pdf(o, 9, NULL, &len);
    ASSERT_TRUE(tp_extract(fp, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_NOT_NULL(strstr(t.r.pages[0].text, "in form"));
    ASSERT_NOT_NULL(strstr(t.r.pages[0].text, "self"));
    ASSERT_NOT_NULL(strstr(t.r.pages[0].text, "after image"));
    ASSERT_NULL(strstr(t.r.pages[0].text, "junk"));
    cbm_arena_destroy(&t.arena);
    free(fp);
    PASS();
}

static void tp_ahx(tp_buf_t *b, const char *s) {
    static const char hex[] = "0123456789abcdef";
    for (; *s; s++) {
        char h[3] = {hex[(unsigned char)*s >> 4], hex[(unsigned char)*s & 15], ' '};
        tp_put(b, h, 3);
    }
    tp_puts(b, ">");
}

static void tp_a85(tp_buf_t *b, const unsigned char *s, size_t n) {
    tp_puts(b, "<~");
    for (size_t i = 0; i < n; i += 4) {
        unsigned char g[4] = {0, 0, 0, 0};
        size_t k = n - i < 4 ? n - i : 4;
        memcpy(g, s + i, k);
        uint32_t v = ((uint32_t)g[0] << 24) | ((uint32_t)g[1] << 16) | ((uint32_t)g[2] << 8) | g[3];
        char out[5];
        for (int j = 4; j >= 0; j--) {
            out[j] = (char)('!' + v % 85);
            v /= 85;
        }
        tp_put(b, out, k + 1);
    }
    tp_puts(b, "~>");
}

TEST(pdf_extract_filters_and_hostile) {
    size_t len;
    tp_text_t t;
    const char *content = "BT /F1 12 Tf 72 700 Td (filtered text) Tj ET";
    /* ASCIIHex, then ASCII85 over Flate */
    tp_buf_t h = {0};
    tp_ahx(&h, content);
    size_t zl;
    unsigned char *z = tp_flate(content, strlen(content), &zl);
    tp_buf_t a = {0};
    tp_a85(&a, z, zl);
    const char *dicts[2] = {"/Filter /ASCIIHexDecode", "/Filter [/ASCII85Decode /FlateDecode]"};
    const tp_buf_t *datas[2] = {&h, &a};
    for (int i = 0; i < 2; i++) {
        tp_obj_t o[5] = {
            {TP_CATALOG, NULL, 0},
            {"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", NULL, 0},
            {"<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 "
             "R >>",
             NULL, 0},
            {TP_HELV, NULL, 0},
            {dicts[i], datas[i]->p, datas[i]->n},
        };
        unsigned char *pdf = tp_pdf(o, 5, NULL, &len);
        ASSERT_TRUE(tp_extract(pdf, len, &t));
        ASSERT_EQ(t.r.status, CBM_PDF_OK);
        ASSERT_STR_EQ(t.r.pages[0].text, "filtered text");
        cbm_arena_destroy(&t.arena);
        free(pdf);
    }
    free(h.p);
    free(a.p);
    free(z);

    /* not a PDF; encrypted; truncated anywhere; nested a hundred thousand deep */
    ASSERT_TRUE(tp_extract((const unsigned char *)"hello", 5, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_NOT_PDF);
    cbm_arena_destroy(&t.arena);
    tp_obj_t enc[5] = {
        {TP_CATALOG, NULL, 0},
        {"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", NULL, 0},
        {"<< /Type /Page /Parent 2 0 R /Contents 5 0 R >>", NULL, 0},
        {"<< /Filter /Standard /V 1 /R 2 /O <00> /U <00> /P -4 >>", NULL, 0},
        {"", "BT ET", 5},
    };
    unsigned char *pdf = tp_pdf(enc, 5, "/Encrypt 4 0 R", &len);
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_ENCRYPTED);
    ASSERT_EQ(t.r.npages, 0);
    cbm_arena_destroy(&t.arena);
    free(pdf);
    pdf = tp_simple("BT /F1 12 Tf 72 700 Td (cut short) Tj ET", &len);
    for (size_t cut = 0; cut < len; cut += 7) {
        ASSERT_TRUE(tp_extract(pdf, cut, &t)); /* any status; never a crash */
        cbm_arena_destroy(&t.arena);
    }
    free(pdf);
    enum { DEEP = 100000 };
    char *deep = (char *)malloc(DEEP * 2 + 64);
    memset(deep, '[', DEEP);
    strcpy(deep + DEEP, " BT /F1 12 Tf (x) Tj ET");
    pdf = tp_simple(deep, &len);
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.counts.page_errors, 1); /* the content stops at the nesting limit */
    cbm_arena_destroy(&t.arena);
    free(pdf);
    free(deep);
    /* a page tree that contains itself, a /Prev loop */
    tp_obj_t loop[5] = {
        {TP_CATALOG, NULL, 0},
        {"<< /Type /Pages /Kids [2 0 R 3 0 R] /Count 1 >>", NULL, 0},
        {"<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R "
         ">>",
         NULL, 0},
        {TP_HELV, NULL, 0},
        {"", "BT /F1 12 Tf (loops) Tj ET", 26},
    };
    pdf = tp_pdf(loop, 5, "/Prev 9", &len);
    char *sx = strstr((char *)pdf, "startxref\n");
    char prev[32];
    snprintf(prev, sizeof(prev), "/Prev %ld", strtol(sx + strlen("startxref\n"), NULL, 10));
    char *pv = strstr((char *)pdf, "/Prev 9");
    ASSERT_NOT_NULL(pv);
    /* the same width: point /Prev at this very section (a loop) */
    if (strlen(prev) <= strlen("/Prev 9") + 6) {
        tp_buf_t nb = {0};
        tp_put(&nb, pdf, (size_t)(pv - (char *)pdf));
        tp_puts(&nb, prev);
        tp_puts(&nb, pv + strlen("/Prev 9"));
        free(pdf);
        pdf = nb.p;
        len = nb.n;
    }
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.npages, 1);
    ASSERT_STR_EQ(t.r.pages[0].text, "loops");
    cbm_arena_destroy(&t.arena);
    free(pdf);
    PASS();
}

/* Objects 1-5 as tp_simple placed by one cross-reference stream (object 6),
 * then `sections` classic sections, each naming that stream as its /XRefStm,
 * chained by /Prev. */
static unsigned char *tp_xrefstm_sections(int sections, size_t *out_len) {
    size_t off[7] = {0};
    tp_buf_t b = {0};
    tp_puts(&b, "%PDF-1.7\n");
    const char *content = "BT /F1 12 Tf 72 700 Td (xrefstm) Tj ET";
    const char *body[5] = {TP_CATALOG, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", TP_PAGE_F1_5,
                           TP_HELV, NULL};
    char line[160];
    for (int i = 1; i <= 5; i++) {
        tp_obj_head(&b, off, i);
        if (i == 5) {
            snprintf(line, sizeof(line), "<< /Length %zu >>\nstream\n", strlen(content));
            tp_puts(&b, line);
            tp_puts(&b, content);
            tp_puts(&b, "\nendstream");
        } else {
            tp_puts(&b, body[i - 1]);
        }
        tp_puts(&b, "\nendobj\n");
    }
    off[6] = b.n;
    unsigned char rows[7][7] = {{0}};
    for (int i = 1; i <= 6; i++) {
        uint32_t f2 = (uint32_t)off[i];
        rows[i][0] = 1;
        rows[i][1] = (unsigned char)(f2 >> 24);
        rows[i][2] = (unsigned char)(f2 >> 16);
        rows[i][3] = (unsigned char)(f2 >> 8);
        rows[i][4] = (unsigned char)f2;
    }
    snprintf(line, sizeof(line),
             "6 0 obj\n<< /Type /XRef /Size 7 /W [1 4 2] /Length %zu >>\nstream\n", sizeof(rows));
    tp_puts(&b, line);
    tp_put(&b, rows, sizeof(rows));
    tp_puts(&b, "\nendstream\nendobj\n");
    size_t prev = 0;
    for (int s = 0; s < sections; s++) {
        size_t at = b.n;
        tp_puts(&b, "xref\n0 1\n0000000000 65535 f \ntrailer\n");
        if (s == 0) {
            snprintf(line, sizeof(line), "<< /Size 7 /Root 1 0 R /XRefStm %zu >>\n", off[6]);
        } else {
            snprintf(line, sizeof(line), "<< /Size 7 /Root 1 0 R /XRefStm %zu /Prev %zu >>\n",
                     off[6], prev);
        }
        tp_puts(&b, line);
        prev = at;
    }
    snprintf(line, sizeof(line), "startxref\n%zu\n%%%%EOF\n", prev);
    tp_puts(&b, line);
    *out_len = b.n;
    return b.p;
}

/* A page drawing form 1, form k drawing form k+1 `fan` times, `depth` forms,
 * each padded with a 64 KiB comment; the last one shows "x". */
static unsigned char *tp_form_ladder(int depth, int fan, size_t *out_len) {
    int n = 5 + depth;
    tp_obj_t *o = (tp_obj_t *)calloc((size_t)n, sizeof(tp_obj_t));
    char **dicts = (char **)calloc((size_t)n, sizeof(char *));
    tp_buf_t *bodies = (tp_buf_t *)calloc((size_t)n, sizeof(tp_buf_t));
    o[0] = (tp_obj_t){TP_CATALOG, NULL, 0};
    o[1] = (tp_obj_t){"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", NULL, 0};
    o[2] = (tp_obj_t){"<< /Type /Page /Parent 2 0 R /Resources << /XObject << /F 6 0 R >> >> "
                      "/Contents 5 0 R >>",
                      NULL, 0};
    o[3] = (tp_obj_t){TP_HELV, NULL, 0};
    o[4] = (tp_obj_t){"", "q /F Do Q", 9};
    char *pad = (char *)malloc(64 * 1024 + 3);
    pad[0] = '%';
    memset(pad + 1, 'x', 64 * 1024);
    pad[64 * 1024 + 1] = '\n';
    pad[64 * 1024 + 2] = 0;
    for (int k = 0; k < depth; k++) {
        int i = 5 + k; /* object i + 1 */
        dicts[i] = (char *)malloc(256);
        snprintf(dicts[i], 256,
                 "/Type /XObject /Subtype /Form /BBox [0 0 612 792] /Resources << /XObject << /F "
                 "%d 0 R >> /Font << /F1 4 0 R >> >>",
                 i + 2);
        tp_puts(&bodies[i], pad);
        if (k + 1 < depth) {
            for (int f = 0; f < fan; f++) {
                tp_puts(&bodies[i], "q /F Do Q\n");
            }
        } else {
            tp_puts(&bodies[i], "BT /F1 12 Tf 72 700 Td (x) Tj ET");
        }
        o[i] = (tp_obj_t){dicts[i], bodies[i].p, bodies[i].n};
    }
    unsigned char *pdf = tp_pdf(o, n, NULL, out_len);
    for (int i = 0; i < n; i++) {
        free(dicts[i]);
        free(bodies[i].p);
    }
    free(pad);
    free(bodies);
    free(dicts);
    free(o);
    return pdf;
}

/* Forms drawing the next one four times, seven levels: the first reading of
 * each form is whole, readings again stop at PDF_MAX_FORM_REREAD (about 1.3
 * GB of form content read again before, and draws ^ depth in general). */
TEST(pdf_extract_form_ladder_bounded) {
    size_t len;
    tp_text_t t;
    unsigned char *pdf = tp_form_ladder(7, 4, &len);
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.npages, 1);
    ASSERT_TRUE(t.r.counts.form_rereads_cut > 0);
    ASSERT_TRUE(t.r.pages[0].glyphs >= 1);
    ASSERT_TRUE((size_t)t.r.pages[0].glyphs <= (256U << 20) / (64U * 1024U) + 1);
    cbm_arena_destroy(&t.arena);
    free(pdf);
    PASS();
}

/* tp_simple's page, then `n` objects that each open a string and never close
 * it, and no cross-reference: the reader rebuilds it from the object headers. */
static unsigned char *tp_open_strings(int n, size_t *out_len) {
    tp_buf_t b = {0};
    tp_puts(&b, "%PDF-1.7\n");
    const char *content = "BT /F1 12 Tf 72 700 Td (open strings) Tj ET";
    const char *body[4] = {TP_CATALOG, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", TP_PAGE_F1_5,
                           TP_HELV};
    char line[128];
    for (int i = 1; i <= 4; i++) {
        snprintf(line, sizeof(line), "%d 0 obj\n", i);
        tp_puts(&b, line);
        tp_puts(&b, body[i - 1]);
        tp_puts(&b, "\nendobj\n");
    }
    snprintf(line, sizeof(line), "5 0 obj\n<< /Length %zu >>\nstream\n", strlen(content));
    tp_puts(&b, line);
    tp_puts(&b, content);
    tp_puts(&b, "\nendstream\nendobj\n");
    for (int i = 0; i < n; i++) {
        snprintf(line, sizeof(line), "%d 0 obj\n(", 6 + i);
        tp_puts(&b, line);
        for (int k = 0; k < 100; k++) {
            tp_puts(&b, "a");
        }
        tp_puts(&b, "\n");
    }
    *out_len = b.n;
    return b.p;
}

/* Objects whose strings never end cost what the file costs: each object's
 * value is read up to the next object, not to the end of the file again (a
 * thousand objects asked for about 60 MB). */
TEST(pdf_extract_open_strings_linear) {
    size_t len;
    tp_text_t t;
    unsigned char *pdf = tp_open_strings(1000, &len);
    cbm_mem_class_reset_peaks();
    size_t arena0 = cbm_mem_class_peak_bytes(CBM_MEM_CLASS_ARENA);
    size_t extract0 = cbm_mem_class_peak_bytes(CBM_MEM_CLASS_EXTRACT);
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    size_t grew = (cbm_mem_class_peak_bytes(CBM_MEM_CLASS_ARENA) - arena0) +
                  (cbm_mem_class_peak_bytes(CBM_MEM_CLASS_EXTRACT) - extract0);
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.counts.xref_reconstructed, 1);
    ASSERT_EQ(t.r.npages, 1);
    ASSERT_STR_EQ(t.r.pages[0].text, "open strings");
    if (grew > 16 * len + (1U << 20)) {
        fprintf(stderr, "open strings: %zu bytes for a %zu-byte file\n", grew, len);
    }
    ASSERT_TRUE(grew <= 16 * len + (1U << 20));
    cbm_arena_destroy(&t.arena);
    free(pdf);
    PASS();
}

/* A thousand sections naming one /XRefStm decode it once: a later (older)
 * section adds nothing, and every copy used to stay until the document closed. */
TEST(pdf_extract_xrefstm_once) {
    size_t len;
    tp_text_t t;
    unsigned char *pdf = tp_xrefstm_sections(1000, &len);
    ASSERT_TRUE(tp_extract(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.npages, 1);
    ASSERT_STR_EQ(t.r.pages[0].text, "xrefstm");
    ASSERT_EQ(t.r.counts.xref_streams, 1);
    cbm_arena_destroy(&t.arena);
    free(pdf);
    PASS();
}

/* A chain of objects, each needed to load the one before, costs no stack per
 * object: a stream's /Length is read as a value (a stream there is no length),
 * and the objects that open an object stream are no objects of one. Run on the
 * smallest stack in the program. */
TEST(pdf_extract_object_chains) {
    size_t len;
    tp_text_t t;
    unsigned char *pdf = tp_length_chain(20000, &len);
    ASSERT_TRUE(tp_extract_small_stack(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.npages, 1);
    ASSERT_STR_EQ(t.r.pages[0].text, "chain");
    ASSERT_TRUE(t.r.counts.stream_length_recovered >= 1);
    cbm_arena_destroy(&t.arena);
    free(pdf);
    pdf = tp_objstm_chain(3000, &len);
    ASSERT_TRUE(tp_extract_small_stack(pdf, len, &t));
    ASSERT_EQ(t.r.status, CBM_PDF_OK);
    ASSERT_EQ(t.r.npages, 1);
    ASSERT_STR_EQ(t.r.pages[0].text, "objstm chain");
    ASSERT_TRUE(t.r.counts.objstm_nested >= 1);
    cbm_arena_destroy(&t.arena);
    free(pdf);
    PASS();
}

/* ── scanner (expected: the field-test scanner on the same text) ─── */

TEST(pdf_scan_mentions) {
    const char *page = "See src/auth/handler.go and ./pkg/x.py, http://example.com/a/b.py and "
                       "and/or 2024/01/02.\n"
                       "Call pkg.Config.Name(), Handler::run and obj->field, e.g. x.y and "
                       "example.com.\n"
                       "The file config.py and setup.cfg; f(a, opts.debug=True).\n"
                       "long/path/to/mod-\n"
                       "ule.py and Handler::\n"
                       "serve here\n"
                       "\xEF\xAC\x81le_utils.py ligature\n";
    static const struct {
        uint32_t line;
        int syntax;
        uint16_t flags;
        const char *raw;
    } want[] = {
        {1, CBM_DOCLINK_PDF_PATH, 0, "src/auth/handler.go"},
        {1, CBM_DOCLINK_PDF_PATH, 0, "pkg/x.py"},
        {2, CBM_DOCLINK_PDF_QN, 0, "pkg.Config.Name"},
        {2, CBM_DOCLINK_PDF_QN, 0, "Handler.run"},
        {2, CBM_DOCLINK_PDF_QN, 0, "obj.field"},
        {3, CBM_DOCLINK_PDF_FILE, 0, "config.py"},
        {3, CBM_DOCLINK_PDF_FILE, 0, "setup.cfg"},
        {4, CBM_DOCLINK_PDF_PATH, 0, "long/path/to/mod"},
        {5, CBM_DOCLINK_PDF_FILE, 0, "ule.py"},
        {7, CBM_DOCLINK_PDF_FILE, 0, "file_utils.py"},
        {4, CBM_DOCLINK_PDF_PATH, CBM_DOCLINK_FLAG_JOIN,
         "long/path/to/module.py\x1flong/path/to/mod-ule.py\x1e"
         "7,8"},
        {5, CBM_DOCLINK_PDF_QN, CBM_DOCLINK_FLAG_JOIN, "Handler.serve\x1e"},
    };
    CBMFileResult *r = (CBMFileResult *)calloc(1, sizeof(*r));
    cbm_arena_init(&r->arena);
    CBMExtractCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.arena = &r->arena;
    ctx.result = r;
    cbm_pdf_test_scan_page(&ctx, page, strlen(page), 1, "p.doc.page_1");
    ASSERT_FALSE(r->doc_links.failed);
    ASSERT_EQ(r->doc_links.count, (int)(sizeof(want) / sizeof(want[0])));
    for (int i = 0; i < r->doc_links.count; i++) {
        const CBMDocLink *l = &r->doc_links.items[i];
        ASSERT_EQ(l->line, want[i].line);
        ASSERT_EQ(l->syntax, want[i].syntax);
        ASSERT_EQ(l->flags, want[i].flags);
        ASSERT_STR_EQ(l->raw, want[i].raw);
        ASSERT_EQ(l->def_line, 1);
        ASSERT_STR_EQ(l->source_qn, "p.doc.page_1");
    }
    cbm_free_result(r);
    PASS();
}

/* Join work on a page of `n` lines that all join, eight names a line. */
static size_t tp_join_work(int n) {
    tp_buf_t b = {0};
    for (int i = 0; i < n; i++) {
        tp_puts(&b, "pkg.mod.Alpha pkg.mod.Beta dir/sub/f1.py dir/sub/f2.py pkg.mod.Gamma "
                    "pkg.mod.Delta dir/sub/f3.py dir/sub/f4.py end_\n");
    }
    CBMFileResult *r = (CBMFileResult *)calloc(1, sizeof(*r));
    cbm_arena_init(&r->arena);
    CBMExtractCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.arena = &r->arena;
    ctx.result = r;
    size_t before = cbm_pdf_test_join_steps();
    cbm_pdf_test_scan_page(&ctx, (const char *)b.p, b.n, 1, "p.doc.page_1");
    size_t work = cbm_pdf_test_join_steps() - before;
    bool ok = !r->doc_links.failed && r->doc_links.count > n * 8;
    cbm_free_result(r);
    free(b.p);
    return ok ? work : 0;
}

/* A join compares only the candidates of its two lines: twice the page, about
 * twice the work (every candidate of the page per join was four times). */
TEST(pdf_scan_join_linear) {
    size_t small = tp_join_work(300);
    size_t large = tp_join_work(600);
    ASSERT_TRUE(small > 0);
    if (large > small * 5 / 2) {
        fprintf(stderr, "join work: %zu for 300 lines, %zu for 600\n", small, large);
    }
    ASSERT_TRUE(large <= small * 5 / 2);
    PASS();
}

/* ── the pipeline ────────────────────────────────────────────────── */

/* A PDF whose pages each show the given lines (Helvetica, one Tj a line). */
static void tp_write_pdf(const char *path, const char *const *pages, int npages) {
    int n = 3 + 2 * npages;
    tp_obj_t *o = (tp_obj_t *)calloc((size_t)n, sizeof(*o));
    char **bufs = (char **)calloc((size_t)npages * 2 + 1, sizeof(char *));
    o[0].dict = TP_CATALOG;
    tp_buf_t kids = {0};
    tp_puts(&kids, "<< /Type /Pages /Count 1 /Kids [");
    for (int i = 0; i < npages; i++) {
        char k[32];
        snprintf(k, sizeof(k), "%d 0 R ", 4 + 2 * i);
        tp_puts(&kids, k);
    }
    tp_puts(&kids, "] >>");
    o[1].dict = (const char *)kids.p;
    o[2].dict = TP_HELV;
    for (int i = 0; i < npages; i++) {
        char *pg = (char *)malloc(256);
        snprintf(pg, 256,
                 "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 "
                 "3 0 R >> >> /Contents %d 0 R >>",
                 5 + 2 * i);
        bufs[2 * i] = pg;
        o[3 + 2 * i].dict = pg;
        tp_buf_t c = {0};
        tp_puts(&c, "BT /F1 10 Tf 72 720 Td 12 TL\n");
        for (const char *s = pages[i]; *s;) {
            const char *e = strchr(s, '\n');
            size_t k = e ? (size_t)(e - s) : strlen(s);
            tp_puts(&c, "(");
            tp_put(&c, s, k);
            tp_puts(&c, ") Tj T*\n");
            s += k + (e ? 1 : 0);
        }
        tp_puts(&c, "ET");
        bufs[2 * i + 1] = (char *)c.p;
        o[4 + 2 * i].dict = "";
        o[4 + 2 * i].data = c.p;
        o[4 + 2 * i].len = c.n;
    }
    size_t len;
    unsigned char *pdf = tp_pdf(o, n, NULL, &len);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        cbm_mkdir_p(dir, 0755);
    }
    FILE *f = fopen(path, "wb");
    fwrite(pdf, 1, len, f);
    fclose(f);
    free(pdf);
    free(kids.p);
    for (int i = 0; i < npages * 2; i++) {
        free(bufs[i]);
    }
    free(bufs);
    free(o);
}

static const char *const DESIGN_P1 =
    "Design: pkg/server/handler.go serves via server.Handler.Serve\n"
    "and app.models.User.save stores users; see\n"
    "tests/test_api.py and config/settings.json.\n"
    "Gone: pkg/server/gone.go and other/thing.go.";
static const char *const DESIGN_P2 =
    "Factory server.NewHandler and models.py build it; app.models holds it.";

static void tp_write_repo(const char *repo, const char *p2) {
    th_write_file(TH_PATH(repo, "pkg/server/handler.go"),
                  "package server\n"
                  "\n"
                  "type Handler struct{}\n"
                  "\n"
                  "func (h *Handler) Serve() {}\n"
                  "\n"
                  "func NewHandler() *Handler { return nil }\n");
    th_write_file(TH_PATH(repo, "app/__init__.py"), "");
    th_write_file(TH_PATH(repo, "app/models.py"), "class User:\n"
                                                  "    def save(self):\n"
                                                  "        return None\n");
    th_write_file(TH_PATH(repo, "tests/test_api.py"), "def test_x():\n"
                                                      "    return None\n");
    th_write_file(TH_PATH(repo, "config/settings.json"), "{\"a\": 1}\n");
    char path[1024];
    snprintf(path, sizeof(path), "%s/docs/design.pdf", repo);
    const char *pages[2] = {DESIGN_P1, p2};
    tp_write_pdf(path, pages, 2);
    snprintf(path, sizeof(path), "%s/tests/fixtures/sample.pdf", repo);
    const char *fx[1] = {"Fixture names pkg/server/handler.go too."};
    tp_write_pdf(path, fx, 1);
}

TEST(pdf_links_pipeline) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlpdf_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    tp_write_repo(repo, DESIGN_P2);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* one Section per page, the page's text as its docstring */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label='Section' AND "
                           "file_path='docs/design.pdf'"),
              2);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM nodes WHERE label='Section' AND name='page 1' "
                           "AND file_path='docs/design.pdf' AND properties LIKE "
                           "'%pkg/server/handler.go serves%'"),
              1);
    char props[512];
    int n;
    /* path-exact */
    dm_edge(db, "docs.design.page_1", "pkg.server.handler.go.__file__", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"via\":\"pdf\""));
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"pdf_path\""));
    ASSERT_NOT_NULL(strstr(props, "\"tier\":\"exact\""));
    ASSERT_NOT_NULL(strstr(props, "\"line\":1"));
    /* qualified names (pdf_qn) passed a second held-out audit (97 of 99
     * correct, Wilson 95 % low 0.929) once a member whose QN leaves its owner
     * out binds only through the owner. A Go method through its receiver: */
    dm_edge(db, "docs.design.page_1", "pkg.server.Serve", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"pdf_qn\""));
    ASSERT_NOT_NULL(strstr(props, "\"tier\":\"unique\""));
    /* qn-exact */
    dm_edge(db, "docs.design.page_1", "app.models.User.save", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"tier\":\"exact\""));
    ASSERT_NOT_NULL(strstr(props, "\"line\":2"));
    /* page 2: a Go function, a file name */
    dm_edge(db, "docs.design.page_2", "pkg.server.NewHandler", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_edge(db, "docs.design.page_2", "app.models.py.__file__", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"pdf_file\""));
    /* a module by its qualified name (a Module node is named by its path) */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
                           "nodes t ON t.id=e.target_id WHERE e.type='MENTIONS' AND "
                           "t.label='Module' AND t.file_path='app/models.py' AND s.name='page 2' "
                           "AND e.properties LIKE '%\"tier\":\"exact\"%'"),
              1);
    /* back navigation: the method knows the page that names it */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
                           "nodes t ON t.id=e.target_id WHERE e.type='MENTIONS' AND t.name='Serve' "
                           "AND s.name='page 1'"),
              1);
    /* hygiene: a test target is a row, a data file nothing; a fixture PDF links nothing */
    char reason[64];
    dm_row(db, "docs/design.pdf", "tests/test_api.py", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "test_only_target");
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes t ON t.id=e.target_id WHERE "
                           "e.type='MENTIONS' AND t.file_path='config/settings.json'"),
              0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE "
                           "raw='config/settings.json'"),
              0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE "
                           "e.type='MENTIONS' AND s.file_path='tests/fixtures/sample.pdf'"),
              0);
    /* unresolved: a missing file under a known directory; another repo's path is no row */
    dm_row(db, "docs/design.pdf", "pkg/server/gone.go", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE raw='other/thing.go'"),
              0);
    /* back navigation: the file knows the page that names it */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id JOIN "
                           "nodes t ON t.id=e.target_id WHERE e.type='MENTIONS' AND "
                           "t.label='File' AND t.file_path='pkg/server/handler.go' AND "
                           "s.name='page 1'"),
              1);
    th_cleanup(tmp);
    PASS();
}

/* A held-out finding of the PDF audit: a Go method's QN has no receiver, so
 * `time.Now` (the standard library's function, in a spec) matched the method
 * Now of a type that the repository's own package time declares. A method is
 * named through its owner: `time.Provider.Now` still binds it. */
TEST(pdf_qn_method_through_owner) {
    cbm_doclink_test_set_ships(CBM_DOCLINK_PDF_QN, true);
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlpdfq_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    th_write_file(TH_PATH(repo, "pkg/time/provider.go"),
                  "package time\n"
                  "\n"
                  "type Provider struct{}\n"
                  "\n"
                  "func (p *Provider) Now() int { return 0 }\n");
    char path[1024];
    snprintf(path, sizeof(path), "%s/docs/spec.pdf", repo);
    const char *pages[2] = {"Go's clock: time.Now is the standard library's.",
                            "Our clock: time.Provider.Now wraps it."};
    tp_write_pdf(path, pages, 2);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    char props[512];
    int n;
    dm_edge(db, "docs.spec.page_1", "pkg.time.Now", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_edge(db, "docs.spec.page_2", "pkg.time.Now", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"pdf_qn\""));
    th_cleanup(tmp);
    cbm_doclink_test_reset_ships();
    PASS();
}

/* A page's docstring keeps 256 KiB of its text and says where it was cut; a
 * page's mentions still come from the whole text. */
TEST(pdf_page_doc_bounded) {
    enum { WORDS = 45000 }; /* 315 KB of text */
    tp_buf_t c = {0};
    tp_puts(&c, "BT /F1 12 Tf 72 700 Td (");
    for (int i = 0; i < WORDS; i++) {
        tp_puts(&c, "filler ");
    }
    tp_puts(&c, "src/tail/last.py) Tj ET");
    tp_obj_t o[5] = {
        {TP_CATALOG, NULL, 0},   {"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", NULL, 0},
        {TP_PAGE_F1_5, NULL, 0}, {TP_HELV, NULL, 0},
        {"", c.p, c.n},
    };
    size_t len;
    unsigned char *pdf = tp_pdf(o, 5, NULL, &len);
    CBMFileResult *r =
        cbm_extract_file((const char *)pdf, (int)len, CBM_LANG_PDF, "p", "doc.pdf", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    const CBMDefinition *page = NULL;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, "Section") == 0) {
            page = &r->defs.items[i];
        }
    }
    ASSERT_NOT_NULL(page);
    ASSERT_NOT_NULL(page->docstring);
    size_t dl = strlen(page->docstring);
    ASSERT_TRUE(dl > 256 * 1024 && dl < 256 * 1024 + 128);
    ASSERT_NOT_NULL(strstr(page->docstring, "[page text cut at 262144 of "));
    bool tail = false;
    for (int i = 0; i < r->doc_links.count; i++) {
        tail = tail || strcmp(r->doc_links.items[i].raw, "src/tail/last.py") == 0;
    }
    ASSERT_TRUE(tail);
    cbm_free_result(r);
    free(pdf);
    free(c.p);
    PASS();
}

TEST(pdf_snippet_is_page_text) {
    char tmp[256] = "/tmp/cbm_dlpdf_snip_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    char cache[400];
    char db[1024];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(cache, sizeof(cache), "%s/cache", tmp);
    tp_write_repo(repo, DESIGN_P2);
    cbm_mkdir_p(cache, 0700);
    char *project = cbm_project_name_from_path(repo);
    ASSERT_NOT_NULL(project);
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    snprintf(db, sizeof(db), "%s/%s.db", cache, project);
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char args[1024];
    snprintf(args, sizeof(args),
             "{\"project\":\"%s\",\"qualified_name\":\"%s.docs.design.page_2\"}", project, project);
    char *resp = cbm_mcp_handle_tool(srv, "get_code_snippet", args);
    char *text = dm_tool_text(resp);
    ASSERT_NOT_NULL(text);
    ASSERT_NOT_NULL(
        strstr(text, "Factory server.NewHandler and models.py build it; app.models holds it."));
    ASSERT_NULL(strstr(text, "%PDF"));
    free(text);
    free(resp);
    cbm_mcp_server_free(srv);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_copy);
    free(project);
    th_cleanup(tmp);
    PASS();
}

TEST(pdf_links_incremental) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlpdf_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    tp_write_repo(repo, DESIGN_P2);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* a body edit of a linked code file: the document re-resolves */
    th_write_file(TH_PATH(repo, "app/models.py"), "class User:\n"
                                                  "    def save(self):\n"
                                                  "        return 1\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "code body edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    /* the document changes while the Go file does not: its method binds
     * through the owner a proxy carries */
    char path[1024];
    snprintf(path, sizeof(path), "%s/docs/design.pdf", repo);
    const char *pages[2] = {DESIGN_P1, "Now only server.Handler.Serve and pkg/server/new.go."};
    tp_write_pdf(path, pages, 2);
    ASSERT_EQ(dm_step(repo, db, full_db, "document edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    /* the missing file appears: the row becomes an edge, as in a full index */
    th_write_file(TH_PATH(repo, "pkg/server/gone.go"), "package server\n"
                                                       "\n"
                                                       "func Gone() {}\n");
    ASSERT_EQ(dm_step(repo, db, full_db, "missing file added", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    th_cleanup(tmp);
    PASS();
}

SUITE(doc_links_pdf) {
    RUN_TEST(pdf_extract_layout);
    RUN_TEST(pdf_extract_fonts);
    RUN_TEST(pdf_extract_structure);
    RUN_TEST(pdf_extract_filters_and_hostile);
    RUN_TEST(pdf_extract_object_chains);
    RUN_TEST(pdf_extract_xrefstm_once);
    RUN_TEST(pdf_extract_open_strings_linear);
    RUN_TEST(pdf_extract_form_ladder_bounded);
    RUN_TEST(pdf_scan_mentions);
    RUN_TEST(pdf_scan_join_linear);
    RUN_TEST(pdf_links_pipeline);
    RUN_TEST(pdf_qn_method_through_owner);
    RUN_TEST(pdf_page_doc_bounded);
    RUN_TEST(pdf_snippet_is_page_text);
    RUN_TEST(pdf_links_incremental);
}
