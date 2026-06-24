/*
 * lsp_node_iter.h — O(n) child enumeration for the per-language LSP walkers.
 *
 * tree-sitter's ts_node_child(node, i) is O(i): it walks the child iterator
 * from the first child every call. So the common idiom
 *
 *     uint32_t nc = ts_node_child_count(node);
 *     for (uint32_t i = 0; i < nc; i++) { TSNode c = ts_node_child(node, i); ... }
 *
 * is O(n²) over a node's children — catastrophic on a wide root (e.g. a program
 * node holding hundreds of thousands of top-level statements, as in TS's
 * reallyLargeFile.ts fixture: 583K comment lines made the per-file LSP passes
 * run for ~133 minutes). cbm_lsp_collect_children() enumerates the children in
 * a single O(n) cursor pass into an arena array; callers then index the array.
 */
#ifndef CBM_LSP_NODE_ITER_H
#define CBM_LSP_NODE_ITER_H

#include "../arena.h"
#include "tree_sitter/api.h"

/* Collect `node`'s children into an arena array (source order) via one O(n)
 * cursor pass. Returns NULL and sets *out_n=0 for a childless node or on OOM. */
static inline TSNode* cbm_lsp_collect_children(CBMArena* arena, TSNode node, uint32_t* out_n) {
    uint32_t nc = ts_node_child_count(node);
    *out_n = 0;
    if (nc == 0) return NULL;
    TSNode* kids = (TSNode*)cbm_arena_alloc(arena, (size_t)nc * sizeof(TSNode));
    if (!kids) return NULL;
    uint32_t kn = 0;
    TSTreeCursor cur = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            kids[kn++] = ts_tree_cursor_current_node(&cur);
        } while (kn < nc && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    *out_n = kn;
    return kids;
}

#endif /* CBM_LSP_NODE_ITER_H */
