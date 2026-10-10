/*
 * pdf_internal.h — the PDF text-layer extractor's internals (pdf_*.c).
 *
 * Stages, each in its own file:
 *   pdf_obj.c    lexer, values, the document (xref tables and streams, /Prev
 *                chains, reconstruction when offsets are broken), indirect
 *                objects, object streams, stream filters
 *   pdf_font.c   glyph names, ToUnicode and encoding CMaps, simple fonts
 *                (base encodings, /Differences, built-in Type1 encodings,
 *                standard-14 widths, Type3), Type0/CID fonts, the TrueType
 *                cmap fallback
 *   pdf_text.c   content streams (graphics and text state, text operators,
 *                Form XObjects, inline images skipped), layout (word and line
 *                breaks from glyph geometry), pages, the driver
 *   pdf_tables.c generated static tables
 *
 * The reader is a port of the field-tested prototype (field test E6): the
 * same tokens, repairs, font rules and layout thresholds, so its text is the
 * text that was measured. Where the prototype's cost could grow faster than
 * the input (a search per object, a table walk per glyph), this port answers
 * from an index built once, with the same result.
 */
#ifndef CBM_PDF_INTERNAL_H
#define CBM_PDF_INTERNAL_H

#include "pdf.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Layout geometry must come out bit for bit the same on every platform (and
 * the same as the measured prototype): no fused multiply-add. GCC in ISO C
 * mode never contracts; clang does unless told. */
#if defined(__clang__)
#pragma STDC FP_CONTRACT OFF
#endif

/* ── Tables (pdf_tables.c) ───────────────────────────────────────── */

typedef struct {
    const char *name;
    const char *utf8;
} pdf_glyph_t;

enum { PDF_STD_WIDTHS = 95 }; /* codes 0x20..0x7E */

extern const pdf_glyph_t PDF_GLYPHS[];
extern const int PDF_GLYPH_COUNT;
extern const char *const PDF_ENC_STANDARD[256];
extern const char *const PDF_ENC_WINANSI[256];
extern const char *const PDF_ENC_MACROMAN[256];
extern const char *const PDF_ENC_PDFDOC[256];
extern const char *const PDF_ENC_SYMBOL[256];
extern const unsigned short PDF_WIDTH_HELVETICA[PDF_STD_WIDTHS];
extern const unsigned short PDF_WIDTH_TIMES[PDF_STD_WIDTHS];
extern const char *const PDF_STD14[];

/* ── Limits: structure, never content ────────────────────────────── */

enum {
    PDF_MAX_NESTING = 256,    /* arrays/dicts inside one value */
    PDF_MAX_RESOLVE = 32,     /* reference chains */
    PDF_MAX_FORM_DEPTH = 16,  /* Form XObjects inside Form XObjects */
    PDF_MAX_PAGE_DEPTH = 64,  /* page-tree depth */
    PDF_MAX_PREV = 4096,      /* xref sections along /Prev */
    PDF_HEADER_WINDOW = 1024, /* "%PDF-" must start within this many bytes */
};

/* A decoded stream above this size is cut and counted: a decompression bomb is
 * no text layer, and real documents stay far below it. */
#define PDF_MAX_STREAM_OUT ((size_t)512 << 20)

/* A Form XObject's first reading in a document is always whole. Reading one
 * again (the same content drawn elsewhere: a logo, a header) is counted, and
 * beyond this many decoded bytes of such re-readings the document reads no
 * form again (counted): forms drawing the next one many times, level under
 * level, asked for (draws ^ depth) readings from a few kilobytes. */
#define PDF_MAX_FORM_REREAD ((size_t)256 << 20)

/* ── Integers from the file: saturated, never undefined ──────────── */

static inline int64_t pdf_sat_add(int64_t a, int64_t b) {
    int64_t r;
    if (__builtin_add_overflow(a, b, &r)) {
        return b > 0 ? INT64_MAX : INT64_MIN;
    }
    return r;
}

/* Python's int() of a float: truncated toward zero; saturated here, NaN 0. */
static inline int64_t pdf_d2i(double x) {
    if (x != x) {
        return 0;
    }
    if (x >= 9223372036854775807.0) {
        return INT64_MAX;
    }
    if (x <= -9223372036854775808.0) {
        return INT64_MIN;
    }
    return (int64_t)x;
}

/* ── Values ──────────────────────────────────────────────────────── */

typedef enum {
    PV_NULL = 0,
    PV_BOOL,
    PV_NUM,
    PV_NAME,
    PV_STR,
    PV_ARR,
    PV_DICT,
    PV_REF,
    PV_STREAM,
    PV_KW,
} pdf_kind_t;

typedef struct pdf_val pdf_val_t;
typedef struct pdf_stream pdf_stream_t;

typedef struct {
    const unsigned char *key; /* a name's bytes, NUL-terminated */
    size_t klen;
    pdf_val_t *val;
} pdf_kv_t;

struct pdf_val {
    pdf_kind_t kind;
    bool b;
    bool is_int;
    double num;
    int64_t inum;
    const unsigned char *s; /* PV_NAME, PV_STR, PV_KW (NUL-terminated copies) */
    size_t n;
    pdf_val_t **items; /* PV_ARR */
    int count;
    pdf_kv_t *kv; /* PV_DICT: sorted by key, one entry per key (the last written) */
    int nkv;
    int64_t ref_num; /* PV_REF */
    int64_t ref_gen;
    pdf_stream_t *stream; /* PV_STREAM */
};

struct pdf_stream {
    pdf_val_t *dict;
    const unsigned char *raw;
    size_t raw_len;
    bool done;
    const unsigned char *decoded; /* NULL: nothing decodable (image, unsupported) */
    size_t dec_len;
};

/* ── Lexer ───────────────────────────────────────────────────────── */

typedef enum {
    PT_EOF = 0,
    PT_NUM,
    PT_NAME,
    PT_STR,
    PT_DS, /* << */
    PT_DE, /* >> */
    PT_AS, /* [ */
    PT_AE, /* ] */
    PT_BS, /* { */
    PT_BE, /* } */
    PT_KW,
} pdf_tok_kind_t;

typedef struct {
    pdf_tok_kind_t kind;
    bool is_int;
    double num;
    int64_t inum;
    const unsigned char *s; /* NAME, STR: arena copies; KW: points into the data */
    size_t n;
} pdf_tok_t;

typedef struct pdf_doc pdf_doc_t;

/* The next token of data[*pos..len); *pos moves past it. */
void pdf_lex(pdf_doc_t *d, const unsigned char *data, size_t len, size_t *pos, pdf_tok_t *t);
/* The value that starts with token t. NULL: out of memory or nested too deep. */
pdf_val_t *pdf_parse_value(pdf_doc_t *d, const unsigned char *data, size_t len, size_t *pos,
                           const pdf_tok_t *t, int depth);
/* An array operand of a content stream: no references, an operator ends it. */
pdf_val_t *pdf_parse_content_array(pdf_doc_t *d, const unsigned char *data, size_t len, size_t *pos,
                                   int depth);
bool pdf_kw_is(const pdf_tok_t *t, const char *kw);

/* ── Integer maps (open addressing, owned by the document) ───────── */

typedef struct {
    uint64_t *keys;
    int64_t *vals;
    uint32_t cap; /* a power of two, or 0 */
    uint32_t n;
} pdf_map_t;

/* -1 when absent. */
int64_t pdf_map_get(const pdf_map_t *m, uint64_t key);
bool pdf_map_put(pdf_map_t *m, uint64_t key, int64_t val);
void pdf_map_free(pdf_map_t *m);

/* ── Document ────────────────────────────────────────────────────── */

typedef struct pdf_objstm pdf_objstm_t;

typedef struct {
    int64_t num;
    uint8_t type; /* 0 free, 1 at an offset, 2 inside an object stream */
    int64_t a;    /* type 1: offset; type 2: the object stream's number */
    int64_t b;    /* type 1: generation; type 2: index in the stream */
    pdf_val_t *obj;
    bool cached;
    bool loading;
    bool not_length;   /* read as a stream's /Length, it was a stream: no length */
    pdf_objstm_t *stm; /* this object as a decoded object stream */
} pdf_xent_t;

typedef struct {
    pdf_xent_t *v;
    int n;
    int cap;
    pdf_map_t by_num; /* object number -> index in v */
} pdf_xref_t;

/* One "N G obj" header found by scanning the whole file. */
typedef struct {
    int64_t num;
    int64_t gen;
    size_t off; /* where N starts */
    uint8_t num_digits;
    uint8_t gen_digits;
    bool canonical; /* both written without leading zeros */
    bool reg_after; /* "obj" runs on into a regular character */
} pdf_hdr_t;

typedef struct {
    int64_t num;
    int idx;
} pdf_numidx_t;

typedef struct pdf_font pdf_font_t;

struct pdf_doc {
    const unsigned char *data;
    size_t len;
    size_t hdr;    /* offset of "%PDF-" */
    CBMArena *a;   /* the document's values, fonts, CMaps */
    CBMArena *cur; /* where the lexer and the value parser allocate (a, or a
                    * content stream's operand arena) */
    bool peek;     /* lex without copying names or strings */
    bool nomem;    /* an allocation failed: the result is CBM_PDF_NOMEM */
    pdf_xref_t xref;
    pdf_val_t *trailer; /* a dict */
    bool reconstructed;
    int objstm_opening; /* object streams being opened, one inside the other */
    /* every "N G obj" in file order, built on first need */
    pdf_hdr_t *hdrs;
    int nhdrs;
    bool hdrs_built;
    pdf_numidx_t *hdr_sorted; /* (num, index) sorted, for the per-object fallback */
    /* blocks from the memory core, freed with the document */
    void **owned;
    int nowned;
    int owned_cap;
    /* an arena for token values that end with one CMap's parse */
    CBMArena scratch_arena;
    bool scratch_arena_live;
    /* a scratch buffer for string and stream assembly */
    unsigned char *scratch;
    size_t scratch_cap;
    /* fonts by reference (num << 20 | gen) or by inline dict address */
    pdf_map_t fonts;
    pdf_font_t **font_list;
    int nfonts;
    int font_cap;
    cbm_pdf_counts_t *counts;
};

pdf_val_t *pdf_get(pdf_doc_t *d, int64_t num);
pdf_val_t *pdf_resolve(pdf_doc_t *d, pdf_val_t *v);
/* dict[key] as written (a reference stays a reference); NULL when absent, null,
 * or the value is no dict. */
pdf_val_t *pdf_dget_raw(const pdf_val_t *dict, const char *key);
/* Whether dict has key at all (a null value counts). */
bool pdf_dhas(const pdf_val_t *dict, const char *key);
/* dict[key] as written, a null value included; NULL only when absent. */
pdf_val_t *pdf_dfind(const pdf_val_t *dict, const char *key);
pdf_val_t *pdf_dfind_n(const pdf_val_t *dict, const unsigned char *key, size_t n);
/* dict[key] resolved; NULL when absent or null. */
pdf_val_t *pdf_dget(pdf_doc_t *d, const pdf_val_t *dict, const char *key);
bool pdf_is_name(const pdf_val_t *v, const char *name);
bool pdf_is_num(const pdf_val_t *v);
/* A number's value; 0 for anything else. */
double pdf_num(const pdf_val_t *v);
/* The decoded data of a stream; NULL when it has none. */
const unsigned char *pdf_stream_decode(pdf_doc_t *d, pdf_stream_t *st, size_t *len);
bool pdf_doc_open(pdf_doc_t *d);
void pdf_doc_close(pdf_doc_t *d);

/* Memory: arena values, and big blocks the document owns. */
void *pdf_alloc(pdf_doc_t *d, size_t n);
void *pdf_calloc(pdf_doc_t *d, size_t n);
void *pdf_big(pdf_doc_t *d, size_t n);       /* owned, freed with the document */
bool pdf_own(pdf_doc_t *d, void *block);     /* hand a memory-core block to the document */
bool pdf_scratch(pdf_doc_t *d, size_t need); /* d->scratch holds at least `need` bytes */

/* ── Fonts (pdf_font.c) ──────────────────────────────────────────── */

typedef struct {
    const char *utf8; /* NULL: unmapped; "" maps to nothing; may point into inl */
    double width;     /* advance in text-space units per unit of font size */
    bool space;       /* the single-byte code 32 */
    char inl[16];     /* a short mapping made for this glyph */
} pdf_glyph_out_t;

/* The font resources[/Font][name] names; NULL when there is none. Cached. */
pdf_font_t *pdf_font_for(pdf_doc_t *d, pdf_val_t *resources, const pdf_val_t *name);
/* Decode shown bytes into glyphs, out[0..*n) (out holds at least len entries). */
void pdf_font_decode(pdf_doc_t *d, pdf_font_t *f, const unsigned char *s, size_t len,
                     pdf_glyph_out_t *out, size_t *n);
/* The text-space size of one em per unit of font size. */
double pdf_font_em(const pdf_font_t *f);
/* A glyph name's Unicode value as UTF-8 (arena or static); NULL when unknown. */
const char *pdf_glyph_to_utf8(pdf_doc_t *d, const unsigned char *name, size_t n);

#endif /* CBM_PDF_INTERNAL_H */
