/*
 * pass_normalize.c — Structural invariant enforcement on graph buffer.
 *
 * Runs AFTER all extraction and resolution passes, BEFORE dump to SQLite.
 * Operates solely on the in-memory graph buffer (no disk I/O).
 *
 * Enforces invariants:
 *   I2: Every Method has a parent Class via DEFINES_METHOD + MEMBER_OF
 *   I3: Every Field has a parent Class/Enum via HAS_FIELD
 *
 * Resolution strategy for missing edges:
 *   1. Derive parent QN by stripping last dot-segment from child QN
 *   2. Exact QN lookup in gbuf hash table (O(1))
 *   3. Fallback: HC-1 shared helper cbm_gbuf_resolve_by_name_in_file (O(1) + O(k))
 *
 * Runtime: O(M + F) where M = Method count, F = Field count
 * Memory: O(1) extra — operates on existing gbuf data
 * Latency: <10ms for 16K nodes (hash lookups only, no I/O)
 */

#include "pipeline/pipeline.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Derive parent QN by stripping last dot-segment.
 * Returns heap-allocated string. Caller must free. Returns NULL if no dot. */
static char *derive_parent_qn(const char *qn) {
    if (!qn) return NULL;
    const char *dot = strrchr(qn, '.');
    if (!dot || dot == qn) return NULL;
    size_t len = (size_t)(dot - qn);
    char *parent = malloc(len + 1);
    if (!parent) return NULL;
    memcpy(parent, qn, len);
    parent[len] = '\0';
    return parent;
}

/* Resolve parent container for a child node (Method→Class, Field→Class).
 * Step 1: exact QN prefix lookup. Step 2: HC-1 shared helper. */
static const cbm_gbuf_node_t *resolve_parent(
    const cbm_gbuf_t *gb, const char *child_qn, const char *child_file,
    const char **parent_labels, int label_count)
{
    char *parent_qn = derive_parent_qn(child_qn);
    if (!parent_qn) return NULL;

    /* Step 1: exact QN lookup — O(1) hash */
    const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(gb, parent_qn);

    /* Step 2: HC-1 shared helper (name + label + file) — O(1) hash + O(k) filter */
    if (!parent) {
        parent = cbm_gbuf_resolve_by_name_in_file(gb, parent_qn, child_file,
                                                    parent_labels, label_count);
    }
    free(parent_qn);
    return parent;
}

void cbm_pipeline_pass_normalize(cbm_gbuf_t *gb) {
    if (!gb) return;

    static const char *class_labels[] = {"Class", "Interface", "Enum"};
    static const char *class_or_enum[] = {"Class", "Enum"};

    int methods_repaired = 0, orphan_methods = 0;
    int fields_repaired = 0, orphan_fields = 0;

    /* ── I2: Method → Class binding ────────────────────── */
    const cbm_gbuf_node_t **methods = NULL;
    int method_count = 0;
    cbm_gbuf_find_by_label(gb, "Method", &methods, &method_count);

    for (int i = 0; i < method_count; i++) {
        const cbm_gbuf_node_t *m = methods[i];
        if (!m->qualified_name || m->id <= 0) continue;

        /* Check if DEFINES_METHOD already exists — O(1) hash */
        const cbm_gbuf_edge_t **existing = NULL;
        int existing_count = 0;
        cbm_gbuf_find_edges_by_target_type(gb, m->id, "DEFINES_METHOD",
                                            &existing, &existing_count);
        if (existing_count > 0) continue;

        const cbm_gbuf_node_t *parent = resolve_parent(
            gb, m->qualified_name, m->file_path, class_labels, 3);

        if (parent) {
            cbm_gbuf_insert_edge(gb, parent->id, m->id, "DEFINES_METHOD", "{}");
            cbm_gbuf_insert_edge(gb, m->id, parent->id, "MEMBER_OF", "{}");
            methods_repaired++;
        } else {
            orphan_methods++;
        }
    }

    /* ── I3: Field → Class/Enum binding ────────────────── */
    const cbm_gbuf_node_t **fields = NULL;
    int field_count = 0;
    cbm_gbuf_find_by_label(gb, "Field", &fields, &field_count);

    for (int i = 0; i < field_count; i++) {
        const cbm_gbuf_node_t *f = fields[i];
        if (!f->qualified_name || f->id <= 0) continue;

        const cbm_gbuf_edge_t **existing = NULL;
        int existing_count = 0;
        cbm_gbuf_find_edges_by_target_type(gb, f->id, "HAS_FIELD",
                                            &existing, &existing_count);
        if (existing_count > 0) continue;

        const cbm_gbuf_node_t *parent = resolve_parent(
            gb, f->qualified_name, f->file_path, class_or_enum, 2);

        if (parent) {
            cbm_gbuf_insert_edge(gb, parent->id, f->id, "HAS_FIELD", "{}");
            fields_repaired++;
        } else {
            orphan_fields++;
        }
    }

    /* Logging */
    char mr[16], of[16], fr[16], om[16];
    snprintf(mr, sizeof(mr), "%d", methods_repaired);
    snprintf(om, sizeof(om), "%d", orphan_methods);
    snprintf(fr, sizeof(fr), "%d", fields_repaired);
    snprintf(of, sizeof(of), "%d", orphan_fields);
    cbm_log_info("pass.done", "pass", "normalize",
                 "methods_repaired", mr, "orphan_methods", om,
                 "fields_repaired", fr, "orphan_fields", of);
}
