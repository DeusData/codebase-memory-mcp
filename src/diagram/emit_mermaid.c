/*
 * emit_mermaid.c — Fast Mermaid diagram syntax generator.
 */
#include "diagram/diagram.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Dynamic string builder */
typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} str_buf_t;

static void sb_init(str_buf_t *sb) {
    sb->cap = 1024;
    sb->buf = (char *)malloc(sb->cap);
    sb->len = 0;
    if (sb->buf) sb->buf[0] = '\0';
}

static void sb_append(str_buf_t *sb, const char *str) {
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

static void sb_printf(str_buf_t *sb, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void sb_printf(str_buf_t *sb, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    char tmp[512];
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n > 0) {
        if ((size_t)n < sizeof(tmp)) {
            sb_append(sb, tmp);
        } else {
            char *dyn = (char *)malloc((size_t)n + 1);
            if (dyn) {
                va_start(ap, fmt);
                vsnprintf(dyn, (size_t)n + 1, fmt, ap);
                va_end(ap);
                sb_append(sb, dyn);
                free(dyn);
            }
        }
    }
}

char *cbm_diagram_emit_mermaid_graph(const cbm_diag_graph_t *graph, const char *direction) {
    if (!graph) return NULL;

    str_buf_t sb;
    sb_init(&sb);

    const char *dir = (direction && direction[0] != '\0') ? direction : "TD";
    if (strncmp(dir, "flowchart", 9) == 0) {
        sb_printf(&sb, "%s\n", dir);
    } else {
        sb_printf(&sb, "graph %s\n", dir);
    }

    /* Render grouped nodes */
    for (int g = 0; g < graph->group_count; g++) {
        const char *grp_name = graph->groups[g].name;
        const char *grp_label = graph->groups[g].label;
        if (!grp_name || grp_name[0] == '\0') continue;

        /* Count nodes in this group */
        int count_in_grp = 0;
        for (int i = 0; i < graph->node_count; i++) {
            if (strcmp(graph->nodes[i].group, grp_name) == 0) {
                count_in_grp++;
            }
        }
        if (count_in_grp == 0) continue;

        char clean_grp[128];
        cbm_diagram_sanitize_id(grp_name, clean_grp, sizeof(clean_grp));
        char clean_lbl[256];
        cbm_diagram_sanitize_label(grp_label ? grp_label : grp_name, clean_lbl, sizeof(clean_lbl));

        sb_printf(&sb, "    subgraph %s [\"%s\"]\n", clean_grp, clean_lbl);
        for (int i = 0; i < graph->node_count; i++) {
            if (strcmp(graph->nodes[i].group, grp_name) == 0) {
                char clean_node_lbl[256];
                cbm_diagram_sanitize_label(graph->nodes[i].label, clean_node_lbl, sizeof(clean_node_lbl));

                if (strcmp(graph->nodes[i].shape, "cylinder") == 0) {
                    sb_printf(&sb, "        %s[(\"%s\")]\n", graph->nodes[i].id, clean_node_lbl);
                } else if (strcmp(graph->nodes[i].shape, "rounded") == 0) {
                    sb_printf(&sb, "        %s(\"%s\")\n", graph->nodes[i].id, clean_node_lbl);
                } else {
                    sb_printf(&sb, "        %s[\"%s\"]\n", graph->nodes[i].id, clean_node_lbl);
                }
            }
        }
        sb_append(&sb, "    end\n");
    }

    /* Render ungrouped nodes */
    for (int i = 0; i < graph->node_count; i++) {
        if (graph->nodes[i].group[0] == '\0') {
            char clean_node_lbl[256];
            cbm_diagram_sanitize_label(graph->nodes[i].label, clean_node_lbl, sizeof(clean_node_lbl));
            if (strcmp(graph->nodes[i].shape, "cylinder") == 0) {
                sb_printf(&sb, "    %s[(\"%s\")]\n", graph->nodes[i].id, clean_node_lbl);
            } else if (strcmp(graph->nodes[i].shape, "rounded") == 0) {
                sb_printf(&sb, "    %s(\"%s\")\n", graph->nodes[i].id, clean_node_lbl);
            } else {
                sb_printf(&sb, "    %s[\"%s\"]\n", graph->nodes[i].id, clean_node_lbl);
            }
        }
    }

    /* Render edges */
    for (int i = 0; i < graph->edge_count; i++) {
        const cbm_diag_edge_t *e = &graph->edges[i];
        char clean_edge_lbl[128];
        cbm_diagram_sanitize_label(e->label, clean_edge_lbl, sizeof(clean_edge_lbl));

        if (strcmp(e->style, "dashed") == 0) {
            if (clean_edge_lbl[0] != '\0') {
                sb_printf(&sb, "    %s -.->|%s| %s\n", e->source, clean_edge_lbl, e->target);
            } else {
                sb_printf(&sb, "    %s -.-> %s\n", e->source, e->target);
            }
        } else if (strcmp(e->style, "error") == 0) {
            if (clean_edge_lbl[0] != '\0') {
                sb_printf(&sb, "    %s ==>|%s| %s\n", e->source, clean_edge_lbl, e->target);
            } else {
                sb_printf(&sb, "    %s ==> %s\n", e->source, e->target);
            }
        } else {
            if (clean_edge_lbl[0] != '\0') {
                sb_printf(&sb, "    %s -->|%s| %s\n", e->source, clean_edge_lbl, e->target);
            } else {
                sb_printf(&sb, "    %s --> %s\n", e->source, e->target);
            }
        }
    }

    return sb.buf;
}

char *cbm_diagram_emit_mermaid_sequence(const cbm_seq_trace_t *trace) {
    if (!trace) return NULL;

    str_buf_t sb;
    sb_init(&sb);

    sb_append(&sb, "sequenceDiagram\n");
    sb_append(&sb, "    autonumber\n");

    /* Declare participants */
    for (int i = 0; i < trace->participant_count; i++) {
        char clean_file[256];
        cbm_diagram_sanitize_label(trace->participants[i].file, clean_file, sizeof(clean_file));
        sb_printf(&sb, "    participant %s as %s\n", trace->participants[i].id, clean_file);
    }

    /* Trace calls */
    for (int i = 0; i < trace->message_count; i++) {
        const cbm_seq_message_t *m = &trace->messages[i];
        if (m->caller_idx < 0 || m->caller_idx >= trace->participant_count ||
            m->callee_idx < 0 || m->callee_idx >= trace->participant_count) {
            continue;
        }

        const char *caller = trace->participants[m->caller_idx].id;
        const char *callee = trace->participants[m->callee_idx].id;

        if (m->is_return) {
            sb_printf(&sb, "    %s-->>%s: return\n", caller, callee);
            sb_printf(&sb, "    deactivate %s\n", caller);
        } else {
            char clean_name[128];
            cbm_diagram_sanitize_label(m->call_name, clean_name, sizeof(clean_name));

            if (m->caller_idx == m->callee_idx) {
                sb_printf(&sb, "    %s->>%s: %s()\n", caller, caller, clean_name);
            } else {
                sb_printf(&sb, "    %s->>%s: %s()\n", caller, callee, clean_name);
                sb_printf(&sb, "    activate %s\n", callee);
            }
        }
    }

    return sb.buf;
}
