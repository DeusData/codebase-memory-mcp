/*
 * emit_dot.c — Graphviz DOT format generator.
 */
#include "diagram/diagram.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} dot_buf_t;

static void dot_init(dot_buf_t *sb) {
    sb->cap = 1024;
    sb->buf = (char *)malloc(sb->cap);
    sb->len = 0;
    if (sb->buf) sb->buf[0] = '\0';
}

static void dot_append(dot_buf_t *sb, const char *str) {
    if (!str || !sb->buf) return;
    size_t slen = strlen(str);
    if (sb->len + slen + 1 >= sb->cap) {
        size_t ncap = (sb->cap + slen + 1) * 2;
        char *nbuf = (char *)realloc(sb->buf, ncap);
        if (!nbuf) return;
        sb->buf = nbuf;
        sb->cap = ncap;
    }
    memcpy(sb->buf + sb->len, str, slen);
    sb->len += slen;
    sb->buf[sb->len] = '\0';
}

static void dot_printf(dot_buf_t *sb, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void dot_printf(dot_buf_t *sb, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    char tmp[512];
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n > 0) {
        if ((size_t)n < sizeof(tmp)) {
            dot_append(sb, tmp);
        } else {
            char *dyn = (char *)malloc((size_t)n + 1);
            if (dyn) {
                va_start(ap, fmt);
                vsnprintf(dyn, (size_t)n + 1, fmt, ap);
                va_end(ap);
                dot_append(sb, dyn);
                free(dyn);
            }
        }
    }
}

char *cbm_diagram_emit_dot_graph(const cbm_diag_graph_t *graph) {
    if (!graph) return NULL;

    dot_buf_t sb;
    dot_init(&sb);

    dot_append(&sb, "digraph G {\n");
    dot_append(&sb, "  rankdir=LR;\n");
    dot_append(&sb, "  bgcolor=\"transparent\";\n");
    dot_append(&sb, "  node [shape=box, style=\"rounded,filled\", fillcolor=\"#1e293b\", fontcolor=\"#f8fafc\", color=\"#475569\", fontname=\"Helvetica\"];\n");
    dot_append(&sb, "  edge [color=\"#94a3b8\", fontcolor=\"#cbd5e1\", fontname=\"Helvetica\", fontsize=10];\n\n");

    /* Subgraphs */
    for (int g = 0; g < graph->group_count; g++) {
        const char *grp_name = graph->groups[g].name;
        const char *grp_label = graph->groups[g].label;
        if (!grp_name || grp_name[0] == '\0') continue;

        int count = 0;
        for (int i = 0; i < graph->node_count; i++) {
            if (strcmp(graph->nodes[i].group, grp_name) == 0) count++;
        }
        if (count == 0) continue;

        char clean_grp[128];
        cbm_diagram_sanitize_id(grp_name, clean_grp, sizeof(clean_grp));
        char clean_lbl[256];
        cbm_diagram_sanitize_label(grp_label ? grp_label : grp_name, clean_lbl, sizeof(clean_lbl));

        dot_printf(&sb, "  subgraph cluster_%s {\n", clean_grp);
        dot_printf(&sb, "    label = \"%s\";\n", clean_lbl);
        dot_append(&sb, "    color = \"#334155\";\n");
        dot_append(&sb, "    fontcolor = \"#94a3b8\";\n");

        for (int i = 0; i < graph->node_count; i++) {
            if (strcmp(graph->nodes[i].group, grp_name) == 0) {
                char clean_lbl2[256];
                cbm_diagram_sanitize_label(graph->nodes[i].label, clean_lbl2, sizeof(clean_lbl2));
                const char *shape = strcmp(graph->nodes[i].shape, "cylinder") == 0 ? "cylinder" : "box";
                dot_printf(&sb, "    %s [label=\"%s\", shape=%s];\n", graph->nodes[i].id, clean_lbl2, shape);
            }
        }
        dot_append(&sb, "  }\n\n");
    }

    /* Ungrouped nodes */
    for (int i = 0; i < graph->node_count; i++) {
        if (graph->nodes[i].group[0] == '\0') {
            char clean_lbl2[256];
            cbm_diagram_sanitize_label(graph->nodes[i].label, clean_lbl2, sizeof(clean_lbl2));
            const char *shape = strcmp(graph->nodes[i].shape, "cylinder") == 0 ? "cylinder" : "box";
            dot_printf(&sb, "  %s [label=\"%s\", shape=%s];\n", graph->nodes[i].id, clean_lbl2, shape);
        }
    }

    /* Edges */
    for (int i = 0; i < graph->edge_count; i++) {
        const cbm_diag_edge_t *e = &graph->edges[i];
        char clean_lbl[128];
        cbm_diagram_sanitize_label(e->label, clean_lbl, sizeof(clean_lbl));

        const char *style = "solid";
        const char *color = "#94a3b8";
        if (strcmp(e->style, "dashed") == 0) {
            style = "dashed";
        } else if (strcmp(e->style, "error") == 0) {
            color = "#ef4444";
        }

        if (clean_lbl[0] != '\0') {
            dot_printf(&sb, "  %s -> %s [label=\"%s\", style=\"%s\", color=\"%s\"];\n",
                       e->source, e->target, clean_lbl, style, color);
        } else {
            dot_printf(&sb, "  %s -> %s [style=\"%s\", color=\"%s\"];\n",
                       e->source, e->target, style, color);
        }
    }

    dot_append(&sb, "}\n");
    return sb.buf;
}

char *cbm_diagram_emit_dot_sequence(const cbm_seq_trace_t *trace) {
    if (!trace) return NULL;

    dot_buf_t sb;
    dot_init(&sb);

    dot_append(&sb, "digraph Sequence {\n");
    dot_append(&sb, "  rankdir=TB;\n");
    dot_append(&sb, "  node [shape=box, style=\"rounded,filled\", fillcolor=\"#1e293b\", fontcolor=\"#f8fafc\", color=\"#475569\", fontname=\"Helvetica\"];\n");
    dot_append(&sb, "  edge [color=\"#38bdf8\", fontcolor=\"#cbd5e1\", fontname=\"Helvetica\", fontsize=10];\n\n");

    /* Participants rank */
    dot_append(&sb, "  { rank=same;\n");
    for (int i = 0; i < trace->participant_count; i++) {
        char clean_file[256];
        cbm_diagram_sanitize_label(trace->participants[i].file, clean_file, sizeof(clean_file));
        dot_printf(&sb, "    %s [label=\"%s\"];\n", trace->participants[i].id, clean_file);
    }
    dot_append(&sb, "  }\n\n");

    /* Events timeline */
    int step_idx = 1;
    for (int i = 0; i < trace->message_count; i++) {
        const cbm_seq_message_t *m = &trace->messages[i];
        if (m->caller_idx < 0 || m->caller_idx >= trace->participant_count ||
            m->callee_idx < 0 || m->callee_idx >= trace->participant_count) {
            continue;
        }

        if (!m->is_return) {
            char clean_name[128];
            cbm_diagram_sanitize_label(m->call_name, clean_name, sizeof(clean_name));
            dot_printf(&sb, "  %s -> %s [label=\"%d: %s()\"];\n",
                       trace->participants[m->caller_idx].id,
                       trace->participants[m->callee_idx].id,
                       step_idx++, clean_name);
        }
    }

    dot_append(&sb, "}\n");
    return sb.buf;
}
