/*
 * pdf.h — the text layer of a PDF, page by page.
 *
 * Reads what a PDF says in text (content-stream text operators decoded through
 * the document's fonts) and nothing else: no rendering, no images, no OCR, no
 * JavaScript, actions or URIs, nothing fetched, nothing executed. Input is
 * untrusted: every read is bounds-checked, every recursion is depth-limited,
 * every decoder is guarded against decompression bombs.
 *
 * Not read: encrypted documents (the result says so). Scanned pages have no
 * text layer and come back empty.
 */
#ifndef CBM_PDF_H
#define CBM_PDF_H

#include "../arena.h"

#include <stdbool.h>
#include <stddef.h>

typedef enum {
    CBM_PDF_OK = 0,
    CBM_PDF_NOT_PDF,       /* no "%PDF-" within the first 1024 bytes */
    CBM_PDF_PARSE_FAILURE, /* no page could be found */
    CBM_PDF_ENCRYPTED,     /* an Encrypt dictionary: the text is not read */
    CBM_PDF_NOMEM,
} cbm_pdf_status_t;

typedef struct {
    const char *text; /* UTF-8, NUL-terminated, no NUL inside */
    size_t len;
    int glyphs;   /* glyphs shown on the page */
    int unmapped; /* glyphs without a Unicode value (U+FFFD in the text) */
} cbm_pdf_page_t;

/* How the document was read: counts of the repairs and limits that applied. */
typedef struct {
    int xref_reconstructed;      /* cross-reference rebuilt from a scan of the file */
    int xref_offset_fixed;       /* an xref section found near a wrong offset */
    int stream_length_recovered; /* a stream's /Length was wrong */
    int objstm_nested;           /* opening an object stream needed an object of one */
    int xref_streams;            /* cross-reference streams decoded */
    int form_rereads_cut;        /* a form not read again: re-readings past 256 MiB */
    int flate_recovered;         /* a damaged Flate stream decoded in part */
    int stream_guard_hits;       /* a decoded stream cut at 512 MiB */
    int filters_unsupported;     /* a stream with a filter this reader does not decode */
    int op_errors;               /* content operators that could not be applied */
    int page_errors;             /* pages whose content stopped early */
} cbm_pdf_counts_t;

typedef struct {
    cbm_pdf_status_t status;
    cbm_pdf_page_t *pages;
    int npages;
    cbm_pdf_counts_t counts;
} cbm_pdf_result_t;

/* Read the text layer of data[0..len). Everything the result points to is
 * allocated from `out` and lives as long as it. Returns false only when memory
 * ran out (status CBM_PDF_NOMEM). */
bool cbm_pdf_extract(const unsigned char *data, size_t len, CBMArena *out, cbm_pdf_result_t *res);

#endif /* CBM_PDF_H */
