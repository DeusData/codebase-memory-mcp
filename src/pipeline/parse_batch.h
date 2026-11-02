#ifndef CBM_PIPELINE_PARSE_BATCH_H
#define CBM_PIPELINE_PARSE_BATCH_H

#include "pipeline_internal.h"

/* The preparation passes run on one thread, because what they build is shared, but most
 * of a pass over one file is parsing it again, and that part shares nothing. A batch
 * parses the next stretch of a pass's files on all workers. While the pass is at a file,
 * any parse of those exact bytes in that exact grammar and mode, however deep in the
 * pass, gets that tree (cbm_parse_bind). So no pass or language adapter changes: a parse
 * that has no tree ready, for any reason, simply parses as it always did.
 *
 * A pass says which mode each language's parses use. CBM_PARSE_NONE skips the file. */
typedef cbm_parse_mode_t (*cbm_parse_batch_mode_fn)(CBMLanguage language);
typedef struct cbm_parse_batch cbm_parse_batch_t;

/* NULL, which every call below accepts, when this run prepares on one thread or the pass
 * has no such files. */
cbm_parse_batch_t *cbm_parse_batch_new(const cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                       int file_count, CBMFileResult **cache,
                                       cbm_parse_batch_mode_fn mode);
/* The pass has moved to this file. Files are visited in ascending order. */
void cbm_parse_batch_at(cbm_parse_batch_t *batch, int file);
void cbm_parse_batch_free(cbm_parse_batch_t *batch);

#endif
