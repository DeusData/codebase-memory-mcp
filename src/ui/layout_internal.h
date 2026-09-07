/*
 * layout_internal.h — shared internals of the server-side layout engine.
 *
 * layout3d.c owns the anchored ForceAtlas2 optimizer and its helpers;
 * layout_regions.c (the CBM Atlas region level) reuses them on a much
 * smaller body set. Nothing here is public API — src/ui only.
 */
#ifndef CBM_UI_LAYOUT_INTERNAL_H
#define CBM_UI_LAYOUT_INTERNAL_H

#include "store/store.h"
#include "ui/layout3d.h"

#include <stdint.h>

/* ── Body with anchor ─────────────────────────────────────────── */

typedef struct {
    float x, y, z;
    float ax, ay, az; /* anchor position (from the seeding pass) */
    float fx, fy, fz;
    float mass;
} cbm_layout_body_t;

/* Gentle anchor-preserving optimization (Barnes-Hut repulsion + edge
 * attraction + anchor springs). `es`/`ed` are edge endpoint indices into the
 * body array; `ne` is the edge count. */
void cbm_layout_local_optimize(cbm_layout_body_t *b, int n, const int *es, const int *ed, int ne);

/* FNV-1a over a NUL-terminated string (NULL-safe). */
uint32_t cbm_layout_fnv1a(const char *s);

/* Deterministic [-0.5, 0.5) jitter stream. */
float cbm_layout_rand_float(uint32_t *seed);

/* Layout core: seed + optimize + classify an explicit node array. Borrowed
 * `nodes` structs (strings are copied into the result). `total_nodes` is the
 * project-wide count the caller knows (may exceed n). */
cbm_layout_result_t *cbm_layout_from_nodes(cbm_store_t *store, const char *project,
                                           const cbm_node_t *nodes, int n, int total_nodes);

#endif /* CBM_UI_LAYOUT_INTERNAL_H */
