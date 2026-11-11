/*
 * pass_diagrams.c — the graph side of diagrams (internal/cbm/diagram.c).
 *
 * A Diagram node is a definition of the file that holds it; a diagram block of
 * a document also hangs under the Section it is written in, and each
 * DiagramElement under its Diagram (DEFINES, from the definition's
 * parent_class). The arrows of a diagram are DIAGRAM_ARROW edges between the
 * file's DiagramElement nodes, with the arrow's text, its style and its line.
 * Both paths of the definitions pass (pass_definitions.c, pass_parallel.c)
 * call in here, so they cannot drift apart.
 */
#include "pipeline/pipeline_internal.h"
#include "cbm.h"
#include "foundation/constants.h"
#include "foundation/str_util.h"
#include "graph_buffer/graph_buffer.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

enum { PD_TEXT_MAX = 512, PD_PROPS_MAX = 1536 };

static bool pd_is_diagram_label(const char *label) {
    return label && (strcmp(label, "Diagram") == 0 || strcmp(label, "DiagramElement") == 0);
}

void cbm_pipeline_diagram_parent_edge(cbm_pipeline_ctx_t *ctx, const CBMDefinition *def,
                                      int64_t node_id) {
    if (!ctx || !def || !def->parent_class || node_id <= 0 || !pd_is_diagram_label(def->label)) {
        return;
    }
    const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(ctx->gbuf, def->parent_class);
    if (parent && parent->id != node_id) {
        cbm_gbuf_insert_edge(ctx->gbuf, parent->id, node_id, "DEFINES", "{}");
    }
}

void cbm_pipeline_diagram_arrows(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result) {
    if (!ctx || !result) {
        return;
    }
    for (int i = 0; i < result->diagram_arrows.count; i++) {
        const CBMDiagramArrow *a = &result->diagram_arrows.items[i];
        if (!a->from_qn || !a->to_qn) {
            continue;
        }
        const cbm_gbuf_node_t *from = cbm_gbuf_find_by_qn(ctx->gbuf, a->from_qn);
        const cbm_gbuf_node_t *to = cbm_gbuf_find_by_qn(ctx->gbuf, a->to_qn);
        if (!from || !to) {
            continue;
        }
        char label[PD_TEXT_MAX];
        char style[PD_TEXT_MAX];
        cbm_json_escape(label, sizeof(label), a->label ? a->label : "");
        cbm_json_escape(style, sizeof(style), a->style ? a->style : "");
        char props[PD_PROPS_MAX];
        int n =
            a->label && a->label[0]
                ? snprintf(props, sizeof(props), "{\"label\":\"%s\",\"style\":\"%s\",\"line\":%u}",
                           label, style, a->line)
                : snprintf(props, sizeof(props), "{\"style\":\"%s\",\"line\":%u}", style, a->line);
        if (n <= 0 || (size_t)n >= sizeof(props)) {
            continue;
        }
        cbm_gbuf_insert_edge(ctx->gbuf, from->id, to->id, "DIAGRAM_ARROW", props);
    }
}
