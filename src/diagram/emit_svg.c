/*
 * emit_svg.c — Self-contained lightweight SVG vector diagram renderer.
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
} svg_buf_t;

static void svg_init(svg_buf_t *sb) {
    sb->cap = 2048;
    sb->buf = (char *)malloc(sb->cap);
    sb->len = 0;
    if (sb->buf) sb->buf[0] = '\0';
}

static void svg_append(svg_buf_t *sb, const char *str) {
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

static void svg_printf(svg_buf_t *sb, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void svg_printf(svg_buf_t *sb, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    char tmp[512];
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n > 0) {
        if ((size_t)n < sizeof(tmp)) {
            svg_append(sb, tmp);
        } else {
            char *dyn = (char *)malloc((size_t)n + 1);
            if (dyn) {
                va_start(ap, fmt);
                vsnprintf(dyn, (size_t)n + 1, fmt, ap);
                va_end(ap);
                svg_append(sb, dyn);
                free(dyn);
            }
        }
    }
}

char *cbm_diagram_emit_svg_graph(const cbm_diag_graph_t *graph) {
    if (!graph) return NULL;

    svg_buf_t sb;
    svg_init(&sb);

    int node_count = graph->node_count;
    if (node_count == 0) {
        svg_append(&sb, "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"400\" height=\"100\">"
                        "<text x=\"20\" y=\"50\" fill=\"#94a3b8\" font-family=\"sans-serif\">Empty Diagram</text></svg>");
        return sb.buf;
    }

    int width = 800;
    int col_width = 240;
    int row_height = 80;
    int cols = 3;
    int rows = (node_count + cols - 1) / cols;
    int height = rows * row_height + 140;

    svg_printf(&sb, "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 %d %d\" width=\"100%%\" height=\"100%%\">\n", width, height);
    svg_append(&sb, "<defs>\n"
                    "  <marker id=\"arrow\" viewBox=\"0 0 10 10\" refX=\"6\" refY=\"5\" markerWidth=\"6\" markerHeight=\"6\" orient=\"auto-start-reverse\">\n"
                    "    <path d=\"M 0 1 L 10 5 L 0 9 z\" fill=\"#38bdf8\"/>\n"
                    "  </marker>\n"
                    "</defs>\n");
    svg_printf(&sb, "<rect width=\"%d\" height=\"%d\" fill=\"#0f172a\" rx=\"8\"/>\n", width, height);

    /* Render Nodes in Grid */
    typedef struct {
        int x;
        int y;
    } node_pos_t;

    node_pos_t *pos = (node_pos_t *)calloc(node_count, sizeof(node_pos_t));

    for (int i = 0; i < node_count; i++) {
        int c = i % cols;
        int r = i / cols;
        int x = 60 + c * col_width;
        int y = 60 + r * row_height;
        if (pos) {
            pos[i].x = x + 90;
            pos[i].y = y + 25;
        }

        char clean_lbl[256];
        cbm_diagram_sanitize_label(graph->nodes[i].label, clean_lbl, sizeof(clean_lbl));

        svg_printf(&sb, "<g transform=\"translate(%d, %d)\">\n", x, y);
        svg_append(&sb, "  <rect width=\"180\" height=\"50\" rx=\"6\" fill=\"#1e293b\" stroke=\"#334155\" stroke-width=\"1.5\"/>\n");
        svg_printf(&sb, "  <text x=\"90\" y=\"28\" fill=\"#f8fafc\" font-family=\"sans-serif\" font-size=\"12\" text-anchor=\"middle\" font-weight=\"500\">%s</text>\n", clean_lbl);
        svg_append(&sb, "</g>\n");
    }

    /* Render Edges */
    for (int e = 0; e < graph->edge_count; e++) {
        int src_idx = -1;
        int tgt_idx = -1;
        for (int i = 0; i < node_count; i++) {
            if (strcmp(graph->nodes[i].id, graph->edges[e].source) == 0) src_idx = i;
            if (strcmp(graph->nodes[i].id, graph->edges[e].target) == 0) tgt_idx = i;
        }

        if (src_idx >= 0 && tgt_idx >= 0 && pos) {
            int x1 = pos[src_idx].x;
            int y1 = pos[src_idx].y;
            int x2 = pos[tgt_idx].x;
            int y2 = pos[tgt_idx].y;

            svg_printf(&sb, "<path d=\"M %d %d Q %d %d %d %d\" stroke=\"#38bdf8\" stroke-width=\"1.5\" fill=\"none\" marker-end=\"url(#arrow)\"/>\n",
                       x1, y1, (x1 + x2) / 2, (y1 + y2) / 2 - 20, x2, y2);
        }
    }

    free(pos);
    svg_append(&sb, "</svg>\n");
    return sb.buf;
}

char *cbm_diagram_emit_svg_sequence(const cbm_seq_trace_t *trace) {
    if (!trace) return NULL;

    svg_buf_t sb;
    svg_init(&sb);

    int pcount = trace->participant_count;
    if (pcount == 0) {
        svg_append(&sb, "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"400\" height=\"100\">"
                        "<text x=\"20\" y=\"50\" fill=\"#94a3b8\" font-family=\"sans-serif\">Empty Sequence</text></svg>");
        return sb.buf;
    }

    int pwidth = 160;
    int width = 80 + pcount * pwidth;
    int mcount = trace->message_count;
    int mheight = 45;
    int height = 120 + mcount * mheight;

    svg_printf(&sb, "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 %d %d\" width=\"100%%\" height=\"100%%\">\n", width, height);
    svg_append(&sb, "<defs>\n"
                    "  <marker id=\"seq_arrow\" viewBox=\"0 0 10 10\" refX=\"6\" refY=\"5\" markerWidth=\"6\" markerHeight=\"6\" orient=\"auto-start-reverse\">\n"
                    "    <path d=\"M 0 1 L 10 5 L 0 9 z\" fill=\"#38bdf8\"/>\n"
                    "  </marker>\n"
                    "</defs>\n");
    svg_printf(&sb, "<rect width=\"%d\" height=\"%d\" fill=\"#0f172a\" rx=\"8\"/>\n", width, height);

    /* Draw participants and lifelines */
    for (int i = 0; i < pcount; i++) {
        int x = 60 + i * pwidth;
        int center_x = x + 60;

        char clean_file[256];
        cbm_diagram_sanitize_label(trace->participants[i].file, clean_file, sizeof(clean_file));

        /* Lifeline */
        svg_printf(&sb, "<line x1=\"%d\" y1=\"70\" x2=\"%d\" y2=\"%d\" stroke=\"#334155\" stroke-width=\"1.5\" stroke-dasharray=\"4 4\"/>\n",
                   center_x, center_x, height - 40);

        /* Box header */
        svg_printf(&sb, "<rect x=\"%d\" y=\"30\" width=\"120\" height=\"36\" rx=\"4\" fill=\"#1e293b\" stroke=\"#38bdf8\" stroke-width=\"1\"/>\n", x);
        svg_printf(&sb, "<text x=\"%d\" y=\"53\" fill=\"#f8fafc\" font-family=\"sans-serif\" font-size=\"12\" text-anchor=\"middle\">%s</text>\n",
                   center_x, clean_file);
    }

    /* Draw call arrows */
    int step = 1;
    for (int i = 0; i < mcount; i++) {
        const cbm_seq_message_t *m = &trace->messages[i];
        if (m->caller_idx < 0 || m->caller_idx >= pcount ||
            m->callee_idx < 0 || m->callee_idx >= pcount) {
            continue;
        }

        int x1 = 60 + m->caller_idx * pwidth + 60;
        int x2 = 60 + m->callee_idx * pwidth + 60;
        int y = 90 + i * mheight;

        if (m->is_return) {
            svg_printf(&sb, "<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"#64748b\" stroke-width=\"1\" stroke-dasharray=\"3 3\" marker-end=\"url(#seq_arrow)\"/>\n",
                       x1, y, x2, y);
        } else {
            char clean_name[128];
            cbm_diagram_sanitize_label(m->call_name, clean_name, sizeof(clean_name));

            if (x1 == x2) {
                /* Self-call loop */
                svg_printf(&sb, "<path d=\"M %d %d C %d %d, %d %d, %d %d\" stroke=\"#38bdf8\" stroke-width=\"1.5\" fill=\"none\" marker-end=\"url(#seq_arrow)\"/>\n",
                           x1, y, x1 + 40, y - 10, x1 + 40, y + 20, x1, y + 15);
                svg_printf(&sb, "<text x=\"%d\" y=\"%d\" fill=\"#cbd5e1\" font-family=\"sans-serif\" font-size=\"11\">%d: %s()</text>\n",
                           x1 + 45, y + 8, step++, clean_name);
            } else {
                svg_printf(&sb, "<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"#38bdf8\" stroke-width=\"1.5\" marker-end=\"url(#seq_arrow)\"/>\n",
                           x1, y, x2, y);
                svg_printf(&sb, "<text x=\"%d\" y=\"%d\" fill=\"#cbd5e1\" font-family=\"sans-serif\" font-size=\"11\" text-anchor=\"middle\">%d: %s()</text>\n",
                           (x1 + x2) / 2, y - 6, step++, clean_name);
            }
        }
    }

    svg_append(&sb, "</svg>\n");
    return sb.buf;
}
