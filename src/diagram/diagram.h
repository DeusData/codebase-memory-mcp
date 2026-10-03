/*
 * diagram.h — Native Diagram & Architecture Generation from SQLite Knowledge Graph
 *
 * Implements deterministic Architecture, Sequence, Data Flow, and Package
 * Dependency diagram extraction and emission in Mermaid, Graphviz DOT, and SVG.
 */
#ifndef CBM_DIAGRAM_H
#define CBM_DIAGRAM_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
typedef struct cbm_store cbm_store_t;

/* ── Supported Diagram Types ───────────────────────────────────── */

typedef enum {
    CBM_DIAGRAM_UNKNOWN = 0,
    CBM_DIAGRAM_ARCHITECTURE,
    CBM_DIAGRAM_SEQUENCE,
    CBM_DIAGRAM_DATAFLOW,
    CBM_DIAGRAM_DEPENDENCIES,
} cbm_diagram_type_t;

/* ── Supported Output Formats ──────────────────────────────────── */

typedef enum {
    CBM_DIAGRAM_FORMAT_UNKNOWN = 0,
    CBM_DIAGRAM_FORMAT_MERMAID,
    CBM_DIAGRAM_FORMAT_DOT,
    CBM_DIAGRAM_FORMAT_SVG,
} cbm_diagram_format_t;

/* ── Diagram Options ───────────────────────────────────────────── */

typedef struct {
    cbm_diagram_type_t type;
    cbm_diagram_format_t format;
    const char *project;
    const char *entry_point;
    const char *scope_path;
    int max_depth;        /* Call depth limit for sequence diagrams (default 3, max 8) */
    int max_participants; /* Maximum participants in sequence diagrams (default 8) */
} cbm_diagram_opts_t;
typedef cbm_diagram_opts_t cbm_diagram_options_t;


/* ── Diagram Result ────────────────────────────────────────────── */

typedef struct {
    char *content;       /* Heap-allocated diagram output, caller frees via cbm_diagram_result_free */
    int nodes_analyzed;  /* Count of nodes processed */
    int edges_traversed; /* Count of edges traversed */
    char errbuf[256];    /* Diagnostic message on error */
} cbm_diagram_result_t;

/* ── Intermediate Graph Models ─────────────────────────────────── */

typedef struct {
    char id[128];
    char label[256];
    char group[128];      /* Subsystem, layer, or stage */
    char shape[32];       /* box, cylinder, rounded, circle */
    bool is_highlighted;  /* Flag for cycles or critical paths */
} cbm_diag_node_t;

typedef struct {
    char source[128];
    char target[128];
    char label[128];
    char style[32];       /* solid, dashed, bold, error */
    int weight;
} cbm_diag_edge_t;

typedef struct {
    char name[128];
    char label[256];
} cbm_diag_group_t;

typedef struct {
    cbm_diag_node_t *nodes;
    int node_count;
    int node_cap;

    cbm_diag_edge_t *edges;
    int edge_count;
    int edge_cap;

    cbm_diag_group_t *groups;
    int group_count;
    int group_cap;
} cbm_diag_graph_t;

/* Sequence trace model */
typedef struct {
    char id[64];
    char name[128];
    char file[256];
} cbm_seq_participant_t;

typedef struct {
    int caller_idx;
    int callee_idx;
    char call_name[128];
    int call_line;
    int depth;
    bool is_return;
} cbm_seq_message_t;

typedef struct {
    cbm_seq_participant_t *participants;
    int participant_count;
    int participant_cap;

    cbm_seq_message_t *messages;
    int message_count;
    int message_cap;

    char root_name[128];
} cbm_seq_trace_t;

/* ── Primary Public Functions ──────────────────────────────────── */

/* Generate diagram content based on options. Returns 0 on success, -1 on failure. */
int cbm_diagram_generate(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                         cbm_diagram_result_t *out);

/* Free resources in cbm_diagram_result_t. */
void cbm_diagram_result_free(cbm_diagram_result_t *res);

/* String parsers */
cbm_diagram_type_t cbm_diagram_parse_type(const char *str);
const char *cbm_diagram_type_name(cbm_diagram_type_t type);

cbm_diagram_format_t cbm_diagram_parse_format(const char *str);
const char *cbm_diagram_format_name(cbm_diagram_format_t fmt);

static inline bool cbm_diagram_type_from_string(const char *str, cbm_diagram_type_t *out) {
    if (!str || !out) return false;
    cbm_diagram_type_t t = cbm_diagram_parse_type(str);
    if (t == CBM_DIAGRAM_UNKNOWN) return false;
    *out = t;
    return true;
}

static inline bool cbm_diagram_format_from_string(const char *str, cbm_diagram_format_t *out) {
    if (!str || !out) return false;
    cbm_diagram_format_t f = cbm_diagram_parse_format(str);
    if (f == CBM_DIAGRAM_FORMAT_UNKNOWN) return false;
    *out = f;
    return true;
}


/* Sanitization utilities */
void cbm_diagram_sanitize_id(const char *raw, char *buf, size_t cap);
void cbm_diagram_sanitize_label(const char *raw, char *buf, size_t cap);

/* Internal graph lifecycle */
void cbm_diag_graph_init(cbm_diag_graph_t *g);
int cbm_diag_graph_add_node(cbm_diag_graph_t *g, const char *id, const char *label,
                            const char *group, const char *shape, bool highlighted);
int cbm_diag_graph_add_edge(cbm_diag_graph_t *g, const char *source, const char *target,
                            const char *label, const char *style, int weight);
int cbm_diag_graph_add_group(cbm_diag_graph_t *g, const char *name, const char *label);
void cbm_diag_graph_free(cbm_diag_graph_t *g);

/* Internal sequence lifecycle */
void cbm_seq_trace_init(cbm_seq_trace_t *t);
int cbm_seq_trace_add_participant(cbm_seq_trace_t *t, const char *id, const char *name,
                                  const char *file);
int cbm_seq_trace_add_message(cbm_seq_trace_t *t, int caller_idx, int callee_idx,
                              const char *call_name, int call_line, int depth, bool is_return);
void cbm_seq_trace_free(cbm_seq_trace_t *t);

/* Internal query procedures */
int cbm_diagram_query_sequence(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                               cbm_seq_trace_t *trace, int *nodes_analyzed, int *edges_traversed,
                               char *errbuf, size_t errbuf_cap);

int cbm_diagram_query_arch(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                           cbm_diag_graph_t *graph, int *nodes_analyzed, int *edges_traversed,
                           char *errbuf, size_t errbuf_cap);

int cbm_diagram_query_flow(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                           cbm_diag_graph_t *graph, int *nodes_analyzed, int *edges_traversed,
                           char *errbuf, size_t errbuf_cap);

int cbm_diagram_query_dependencies(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                                   cbm_diag_graph_t *graph, int *nodes_analyzed, int *edges_traversed,
                                   char *errbuf, size_t errbuf_cap);

/* Internal emission procedures */
char *cbm_diagram_emit_mermaid_graph(const cbm_diag_graph_t *graph, const char *direction);
char *cbm_diagram_emit_mermaid_sequence(const cbm_seq_trace_t *trace);
char *cbm_diagram_emit_dot_graph(const cbm_diag_graph_t *graph);
char *cbm_diagram_emit_dot_sequence(const cbm_seq_trace_t *trace);
char *cbm_diagram_emit_svg_graph(const cbm_diag_graph_t *graph);
char *cbm_diagram_emit_svg_sequence(const cbm_seq_trace_t *trace);

#ifdef __cplusplus
}
#endif

#endif /* CBM_DIAGRAM_H */
