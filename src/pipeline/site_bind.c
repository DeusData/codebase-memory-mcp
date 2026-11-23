/*
 * site_bind.c — answers of the shared cross-file layer at call and usage
 * sites. See site_bind.h for where the resolvers ask and how they apply an
 * answer.
 *
 * No language has a row yet, so begin marks no file active, every site gets
 * NO_ANSWER and the end step draws nothing: the hook costs a few calls per
 * file and one call per site.
 */
#include "pipeline/site_bind.h"

#include "cbm.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <stdbool.h>
#include <stddef.h>

void cbm_pipeline_site_bind_begin(cbm_site_file_t *sf, const cbm_pipeline_ctx_t *ctx,
                                  const cbm_gbuf_t *graph, const char *rel, CBMLanguage lang,
                                  const CBMFileResult *result) {
    if (!sf) {
        return;
    }
    *sf = (cbm_site_file_t){
        .ctx = ctx, .graph = graph, .result = result, .rel = rel, .lang = lang, .active = false};
}

/* The answer at every site of a file no row covers. */
static cbm_site_answer_t sb_no_answer(void) {
    cbm_site_answer_t none = {.verdict = CBM_SITE_NO_ANSWER, .res = {0}, .target = NULL};
    return none;
}

cbm_site_answer_t cbm_pipeline_site_bind_call(cbm_site_file_t *sf, const CBMCall *call,
                                              const cbm_gbuf_node_t *source) {
    /* No row exists, so no file is active. */
    (void)sf;
    (void)call;
    (void)source;
    return sb_no_answer();
}

cbm_site_answer_t cbm_pipeline_site_bind_usage(cbm_site_file_t *sf, const CBMUsage *usage,
                                               const cbm_gbuf_node_t *source) {
    /* No row exists, so no file is active. */
    (void)sf;
    (void)usage;
    (void)source;
    return sb_no_answer();
}

void cbm_pipeline_site_bind_end(cbm_site_file_t *sf, cbm_gbuf_t *edges) {
    /* No POSSIBLE answer was given, so nothing is left to draw. */
    (void)sf;
    (void)edges;
}
