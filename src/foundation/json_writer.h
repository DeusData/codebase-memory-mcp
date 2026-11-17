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

/* yyjson's strict writer refuses a whole document over one byte that is not
 * UTF-8, and text read from a source file can be in any encoding. A valid
 * document is written exactly as the strict writer writes it. A refused one is
 * written again with the layout writer's flags (#2907): each such byte becomes
 * an escaped U+FFFD, and valid non-ASCII text is escaped too. The result is
 * always strict JSON; NULL only for another failure the strict writer reports,
 * such as a failed allocation or a missing root. alc and len mean what they do
 * in yyjson_mut_write_opts. */
static inline char *cbm_json_write_text(const yyjson_mut_doc *doc, const yyjson_alc *alc,
                                        size_t *len) {
    yyjson_write_err err = {0};
    char *json = yyjson_mut_write_opts(doc, 0, alc, len, &err);
    if (json || err.code != YYJSON_WRITE_ERROR_INVALID_STRING) {
        return json;
    }
    return yyjson_mut_write_opts(
        doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE | YYJSON_WRITE_ESCAPE_UNICODE, alc, len, NULL);
}

#endif /* CBM_JSON_WRITER_H */
