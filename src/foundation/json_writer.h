/* JSON output buffers owned by the memory core. */
#ifndef CBM_JSON_WRITER_H
#define CBM_JSON_WRITER_H

#include "foundation/mem_core.h"

#include <yyjson/yyjson.h>

static inline void *cbm_json_writer_malloc(void *ctx, size_t size) {
    (void)ctx;
    return cbm_alloc(CBM_MEM_CLASS_OTHER, size);
}

static inline void *cbm_json_writer_realloc(void *ctx, void *block, size_t old_size, size_t size) {
    (void)ctx;
    (void)old_size;
    return cbm_realloc(CBM_MEM_CLASS_OTHER, block, size);
}

static inline void cbm_json_writer_free(void *ctx, void *block) {
    (void)ctx;
    cbm_free(CBM_MEM_CLASS_OTHER, block);
}

/* Pass this allocator to yyjson_write_opts or yyjson_mut_write_opts. The
 * resulting string must be released with cbm_free(CBM_MEM_CLASS_OTHER, ...).
 * The document itself retains whichever allocator created it. */
static inline const yyjson_alc *cbm_json_writer_allocator(void) {
    static const yyjson_alc allocator = {cbm_json_writer_malloc, cbm_json_writer_realloc,
                                         cbm_json_writer_free, NULL};
    return &allocator;
}

/* yyjson's strict writer refuses a whole document over one undecodable byte. Text
 * read from a source file can be in any encoding (a latin-1 string or identifier is
 * ordinary in old C, Lua and Python), so these write valid documents unchanged and
 * replace such bytes with U+FFFD only in one that the writer refused. The result is
 * always strict JSON. NULL only for what the strict writer itself reports: a failed
 * allocation or a missing root. alc and length mean what they do in yyjson. */
char *cbm_json_write_text(yyjson_mut_doc *doc, const yyjson_alc *alc, size_t *length);
char *cbm_json_val_write_text(yyjson_mut_doc *doc, yyjson_mut_val *value, const yyjson_alc *alc,
                              size_t *length);

#endif /* CBM_JSON_WRITER_H */
