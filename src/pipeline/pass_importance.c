/*
 * pass_importance.c — Index-time persisted per-symbol importance score.
 *
 * RED-BOUNDARY STUB (spec Part 1 / AC1, build-plan piece P3). This file is
 * intentionally a no-op at the red-test commit: it exists only so the test
 * binary (which calls cbm_pipeline_pass_importance and
 * cbm_pipeline_importance_append_prop directly in gbuf-level unit tests)
 * links and runs, producing real assertion failures rather than a build
 * error. The real formula lands in the implementation commit on top of the
 * red-test boundary -- see pipeline_internal.h for the full contract this
 * pass must satisfy, and test-plan.md for the AC table.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"

void cbm_pipeline_pass_importance(cbm_pipeline_ctx_t *ctx) {
    (void)ctx; /* not yet implemented -- red-test boundary stub */
}

void cbm_pipeline_importance_append_prop(cbm_gbuf_node_t *node, double score) {
    (void)node;
    (void)score; /* not yet implemented -- red-test boundary stub */
}
