#ifndef CBM_RESOLVE_SYNTAX_INDEX_H
#define CBM_RESOLVE_SYNTAX_INDEX_H

#include "tree_sitter/api.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* tree-sitter stores no parent links and no child offsets: ts_node_parent descends
 * from the root over every earlier sibling, and ts_node_child(n, i) steps over the
 * i children before it. Each answer is fine alone; asked once per child of a wide
 * node, as source analysis does, the total is quadratic in that width. A module with
 * tens of thousands of top-level statements paid minutes for it.
 *
 * While an index is current on this thread, the three calls below answer from one
 * cursor pass over the tree, so a tree of n nodes costs O(n) however many questions
 * it gets. Answers are exactly what tree-sitter returns. A node of another tree, or a
 * thread with no current index, simply gets the plain call.
 *
 * A source file includes this header last and needs no other change: every call,
 * including one added later, is routed here. One index is current per thread, and a
 * nested begin puts the previous one back at its end. */
typedef struct cbm_syntax_wide cbm_syntax_wide_t;
typedef struct cbm_syntax_index {
    struct cbm_syntax_index *previous;
    const TSTree *tree;
    TSNode *nodes;
    uint32_t *parents;
    uint32_t *before;
    uint32_t *slots;
    size_t mask;
    bool parents_unavailable;
    cbm_syntax_wide_t *wide;
    size_t wide_capacity, wide_count;
} cbm_syntax_index_t;

/* tree may be NULL: then nothing is indexed. Allocates nothing until a question
 * arrives, so a file that never asks pays nothing. The tree must outlive the end. */
void cbm_syntax_index_begin(cbm_syntax_index_t *index, const TSTree *tree);
void cbm_syntax_index_end(cbm_syntax_index_t *index);

TSNode cbm_syntax_parent(TSNode node);
TSNode cbm_syntax_child(TSNode node, uint32_t position);
TSNode cbm_syntax_named_child(TSNode node, uint32_t position);
/* The child before node in its parent's list, in ts_node_child order; a null node
 * for the first child and the root. Tree-sitter has no such call: code that wants
 * it counts up to node from the first child, once per child of a wide parent. */
TSNode cbm_syntax_previous_child(TSNode node);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* The plain calls, with the sibling steps they really take added to
 * CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, so a growth test can show its fixture
 * tells linear from quadratic. */
void cbm_syntax_index_test_set_plain(bool plain);
#endif

#ifndef CBM_SYNTAX_INDEX_IMPLEMENTATION
#define ts_node_parent(node) cbm_syntax_parent(node)
#define ts_node_child(node, position) cbm_syntax_child(node, position)
#define ts_node_named_child(node, position) cbm_syntax_named_child(node, position)
#endif

#endif
