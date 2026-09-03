/*
 * layout3d_internal.h — Internal helpers exposed for testing.
 *
 * These functions are implementation details of layout3d.c; they are
 * declared here only so that tests/test_ui.c can drive them directly
 * without needing a store-backed cbm_layout_compute() call. Production
 * code outside layout3d.c should use the public API in layout3d.h instead.
 */
#ifndef CBM_UI_LAYOUT3D_INTERNAL_H
#define CBM_UI_LAYOUT3D_INTERNAL_H

/*
 * Call-depth BFS used to seed the z-axis of the layout.
 *
 * `n` nodes, edges given as parallel arrays es[0..ne)/ed[0..ne) of node
 * indices (source/target, both in [0,n)). `labels` is indexed by node;
 * Route/File/Module/Package nodes seed depth 0 (entry points). If none of
 * those labels are present, nodes with zero in-degree seed depth 0 instead.
 * BFS then propagates depth+1 along outgoing edges. Any node never reached
 * (including one with no entry point can reach it) is left at depth 0.
 *
 * `depth` must have room for n ints; it is fully overwritten.
 */
void cbm_layout_compute_call_depth(int n, const int *es, const int *ed, int ne,
                                    const char **labels, int *depth);

#endif /* CBM_UI_LAYOUT3D_INTERNAL_H */
