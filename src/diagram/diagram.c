/*
 * diagram.c — Top-level dispatcher and lifecycle for native diagram generation.
 */
#include "diagram/diagram.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Lifecycle & Helpers ───────────────────────────────────────── */

void cbm_diagram_result_free(cbm_diagram_result_t *res) {
    if (!res) return;
    if (res->content) {
        free(res->content);
        res->content = NULL;
    }
}

cbm_diagram_type_t cbm_diagram_parse_type(const char *str) {
    if (!str) return CBM_DIAGRAM_UNKNOWN;
    if (strcmp(str, "architecture") == 0 || strcmp(str, "arch") == 0) return CBM_DIAGRAM_ARCHITECTURE;
    if (strcmp(str, "sequence") == 0 || strcmp(str, "seq") == 0) return CBM_DIAGRAM_SEQUENCE;
    if (strcmp(str, "dataflow") == 0 || strcmp(str, "flow") == 0) return CBM_DIAGRAM_DATAFLOW;
    if (strcmp(str, "dependencies") == 0 || strcmp(str, "deps") == 0) return CBM_DIAGRAM_DEPENDENCIES;
    return CBM_DIAGRAM_UNKNOWN;
}

const char *cbm_diagram_type_name(cbm_diagram_type_t type) {
    switch (type) {
        case CBM_DIAGRAM_ARCHITECTURE: return "architecture";
        case CBM_DIAGRAM_SEQUENCE: return "sequence";
        case CBM_DIAGRAM_DATAFLOW: return "dataflow";
        case CBM_DIAGRAM_DEPENDENCIES: return "dependencies";
        default: return "unknown";
    }
}

cbm_diagram_format_t cbm_diagram_parse_format(const char *str) {
    if (!str || str[0] == '\0') return CBM_DIAGRAM_FORMAT_MERMAID;
    if (strcmp(str, "mermaid") == 0 || strcmp(str, "mmd") == 0) return CBM_DIAGRAM_FORMAT_MERMAID;
    if (strcmp(str, "dot") == 0 || strcmp(str, "graphviz") == 0) return CBM_DIAGRAM_FORMAT_DOT;
    if (strcmp(str, "svg") == 0) return CBM_DIAGRAM_FORMAT_SVG;
    return CBM_DIAGRAM_FORMAT_UNKNOWN;
}

const char *cbm_diagram_format_name(cbm_diagram_format_t fmt) {
    switch (fmt) {
        case CBM_DIAGRAM_FORMAT_MERMAID: return "mermaid";
        case CBM_DIAGRAM_FORMAT_DOT: return "dot";
        case CBM_DIAGRAM_FORMAT_SVG: return "svg";
        default: return "unknown";
    }
}

/* ── Intermediate Graph Memory ─────────────────────────────────── */

void cbm_diag_graph_init(cbm_diag_graph_t *g) {
    memset(g, 0, sizeof(*g));
    g->node_cap = 64;
    g->nodes = (cbm_diag_node_t *)calloc(g->node_cap, sizeof(cbm_diag_node_t));
    g->edge_cap = 128;
    g->edges = (cbm_diag_edge_t *)calloc(g->edge_cap, sizeof(cbm_diag_edge_t));
    g->group_cap = 16;
    g->groups = (cbm_diag_group_t *)calloc(g->group_cap, sizeof(cbm_diag_group_t));
}

int cbm_diag_graph_add_node(cbm_diag_graph_t *g, const char *id, const char *label,
                            const char *group, const char *shape, bool highlighted) {
    if (!g || !id || id[0] == '\0') return -1;

    /* Deduplicate node by id */
    for (int i = 0; i < g->node_count; i++) {
        if (strcmp(g->nodes[i].id, id) == 0) {
            if (group && group[0] != '\0' && g->nodes[i].group[0] == '\0') {
                snprintf(g->nodes[i].group, sizeof(g->nodes[i].group), "%s", group);
            }
            return i;
        }
    }

    if (g->node_count >= g->node_cap) {
        int ncap = g->node_cap * 2;
        cbm_diag_node_t *nn = (cbm_diag_node_t *)realloc(g->nodes, ncap * sizeof(cbm_diag_node_t));
        if (!nn) return -1;
        g->nodes = nn;
        g->node_cap = ncap;
    }

    int idx = g->node_count++;
    snprintf(g->nodes[idx].id, sizeof(g->nodes[idx].id), "%s", id);
    snprintf(g->nodes[idx].label, sizeof(g->nodes[idx].label), "%s", label ? label : id);
    snprintf(g->nodes[idx].group, sizeof(g->nodes[idx].group), "%s", group ? group : "");
    snprintf(g->nodes[idx].shape, sizeof(g->nodes[idx].shape), "%s", shape ? shape : "box");
    g->nodes[idx].is_highlighted = highlighted;
    return idx;
}

int cbm_diag_graph_add_edge(cbm_diag_graph_t *g, const char *source, const char *target,
                            const char *label, const char *style, int weight) {
    if (!g || !source || !target || source[0] == '\0' || target[0] == '\0') return -1;

    /* Deduplicate edge by (source, target) */
    for (int i = 0; i < g->edge_count; i++) {
        if (strcmp(g->edges[i].source, source) == 0 && strcmp(g->edges[i].target, target) == 0) {
            g->edges[i].weight += weight > 0 ? weight : 1;
            return i;
        }
    }

    if (g->edge_count >= g->edge_cap) {
        int ncap = g->edge_cap * 2;
        cbm_diag_edge_t *ne = (cbm_diag_edge_t *)realloc(g->edges, ncap * sizeof(cbm_diag_edge_t));
        if (!ne) return -1;
        g->edges = ne;
        g->edge_cap = ncap;
    }

    int idx = g->edge_count++;
    snprintf(g->edges[idx].source, sizeof(g->edges[idx].source), "%s", source);
    snprintf(g->edges[idx].target, sizeof(g->edges[idx].target), "%s", target);
    snprintf(g->edges[idx].label, sizeof(g->edges[idx].label), "%s", label ? label : "");
    snprintf(g->edges[idx].style, sizeof(g->edges[idx].style), "%s", style ? style : "solid");
    g->edges[idx].weight = weight > 0 ? weight : 1;
    return idx;
}

int cbm_diag_graph_add_group(cbm_diag_graph_t *g, const char *name, const char *label) {
    if (!g || !name || name[0] == '\0') return -1;

    for (int i = 0; i < g->group_count; i++) {
        if (strcmp(g->groups[i].name, name) == 0) return i;
    }

    if (g->group_count >= g->group_cap) {
        int ncap = g->group_cap * 2;
        cbm_diag_group_t *ng = (cbm_diag_group_t *)realloc(g->groups, ncap * sizeof(cbm_diag_group_t));
        if (!ng) return -1;
        g->groups = ng;
        g->group_cap = ncap;
    }

    int idx = g->group_count++;
    snprintf(g->groups[idx].name, sizeof(g->groups[idx].name), "%s", name);
    snprintf(g->groups[idx].label, sizeof(g->groups[idx].label), "%s", label ? label : name);
    return idx;
}

void cbm_diag_graph_free(cbm_diag_graph_t *g) {
    if (!g) return;
    if (g->nodes) { free(g->nodes); g->nodes = NULL; }
    if (g->edges) { free(g->edges); g->edges = NULL; }
    if (g->groups) { free(g->groups); g->groups = NULL; }
    g->node_count = g->node_cap = 0;
    g->edge_count = g->edge_cap = 0;
    g->group_count = g->group_cap = 0;
}

/* ── Intermediate Sequence Memory ──────────────────────────────── */

void cbm_seq_trace_init(cbm_seq_trace_t *t) {
    memset(t, 0, sizeof(*t));
    t->participant_cap = 16;
    t->participants = (cbm_seq_participant_t *)calloc(t->participant_cap, sizeof(cbm_seq_participant_t));
    t->message_cap = 64;
    t->messages = (cbm_seq_message_t *)calloc(t->message_cap, sizeof(cbm_seq_message_t));
}

int cbm_seq_trace_add_participant(cbm_seq_trace_t *t, const char *id, const char *name,
                                  const char *file) {
    if (!t || !id) return -1;

    for (int i = 0; i < t->participant_count; i++) {
        if (strcmp(t->participants[i].id, id) == 0) return i;
    }

    if (t->participant_count >= t->participant_cap) {
        int ncap = t->participant_cap * 2;
        cbm_seq_participant_t *np = (cbm_seq_participant_t *)realloc(t->participants, ncap * sizeof(cbm_seq_participant_t));
        if (!np) return -1;
        t->participants = np;
        t->participant_cap = ncap;
    }

    int idx = t->participant_count++;
    snprintf(t->participants[idx].id, sizeof(t->participants[idx].id), "%s", id);
    snprintf(t->participants[idx].name, sizeof(t->participants[idx].name), "%s", name ? name : id);
    snprintf(t->participants[idx].file, sizeof(t->participants[idx].file), "%s", file ? file : "");
    return idx;
}

int cbm_seq_trace_add_message(cbm_seq_trace_t *t, int caller_idx, int callee_idx,
                              const char *call_name, int call_line, int depth, bool is_return) {
    if (!t) return -1;

    if (t->message_count >= t->message_cap) {
        int ncap = t->message_cap * 2;
        cbm_seq_message_t *nm = (cbm_seq_message_t *)realloc(t->messages, ncap * sizeof(cbm_seq_message_t));
        if (!nm) return -1;
        t->messages = nm;
        t->message_cap = ncap;
    }

    int idx = t->message_count++;
    t->messages[idx].caller_idx = caller_idx;
    t->messages[idx].callee_idx = callee_idx;
    snprintf(t->messages[idx].call_name, sizeof(t->messages[idx].call_name), "%s", call_name ? call_name : "");
    t->messages[idx].call_line = call_line;
    t->messages[idx].depth = depth;
    t->messages[idx].is_return = is_return;
    return idx;
}

void cbm_seq_trace_free(cbm_seq_trace_t *t) {
    if (!t) return;
    if (t->participants) { free(t->participants); t->participants = NULL; }
    if (t->messages) { free(t->messages); t->messages = NULL; }
    t->participant_count = t->participant_cap = 0;
    t->message_count = t->message_cap = 0;
}

/* ── Main Dispatcher ───────────────────────────────────────────── */

int cbm_diagram_generate(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                         cbm_diagram_result_t *out) {
    if (!out) return -1;
    memset(out, 0, sizeof(*out));

    if (!store) {
        snprintf(out->errbuf, sizeof(out->errbuf), "Database store is null");
        return -1;
    }
    if (!opts) {
        snprintf(out->errbuf, sizeof(out->errbuf), "Options parameter is null");
        return -1;
    }

    cbm_diagram_format_t fmt = opts->format != CBM_DIAGRAM_FORMAT_UNKNOWN ? opts->format : CBM_DIAGRAM_FORMAT_MERMAID;

    if (opts->type == CBM_DIAGRAM_SEQUENCE) {
        cbm_seq_trace_t trace;
        cbm_seq_trace_init(&trace);

        int rc = cbm_diagram_query_sequence(store, opts, &trace, &out->nodes_analyzed,
                                            &out->edges_traversed, out->errbuf, sizeof(out->errbuf));
        if (rc != 0) {
            cbm_seq_trace_free(&trace);
            return rc;
        }

        if (fmt == CBM_DIAGRAM_FORMAT_DOT) {
            out->content = cbm_diagram_emit_dot_sequence(&trace);
        } else if (fmt == CBM_DIAGRAM_FORMAT_SVG) {
            out->content = cbm_diagram_emit_svg_sequence(&trace);
        } else {
            out->content = cbm_diagram_emit_mermaid_sequence(&trace);
        }

        cbm_seq_trace_free(&trace);
        return out->content ? 0 : -1;
    }

    /* Graph-based diagrams */
    cbm_diag_graph_t graph;
    cbm_diag_graph_init(&graph);
    int rc = -1;

    switch (opts->type) {
        case CBM_DIAGRAM_ARCHITECTURE:
            rc = cbm_diagram_query_arch(store, opts, &graph, &out->nodes_analyzed,
                                        &out->edges_traversed, out->errbuf, sizeof(out->errbuf));
            break;
        case CBM_DIAGRAM_DATAFLOW:
            rc = cbm_diagram_query_flow(store, opts, &graph, &out->nodes_analyzed,
                                        &out->edges_traversed, out->errbuf, sizeof(out->errbuf));
            break;
        case CBM_DIAGRAM_DEPENDENCIES:
            rc = cbm_diagram_query_dependencies(store, opts, &graph, &out->nodes_analyzed,
                                                &out->edges_traversed, out->errbuf, sizeof(out->errbuf));
            break;
        default:
            snprintf(out->errbuf, sizeof(out->errbuf), "Unsupported diagram type %d", opts->type);
            cbm_diag_graph_free(&graph);
            return -1;
    }

    if (rc != 0) {
        cbm_diag_graph_free(&graph);
        return rc;
    }

    if (fmt == CBM_DIAGRAM_FORMAT_DOT) {
        out->content = cbm_diagram_emit_dot_graph(&graph);
    } else if (fmt == CBM_DIAGRAM_FORMAT_SVG) {
        out->content = cbm_diagram_emit_svg_graph(&graph);
    } else {
        const char *dir = (opts->type == CBM_DIAGRAM_DATAFLOW) ? "flowchart LR" : "TD";
        out->content = cbm_diagram_emit_mermaid_graph(&graph, dir);
    }

    cbm_diag_graph_free(&graph);
    return out->content ? 0 : -1;
}
