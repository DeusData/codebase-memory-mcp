#define CBM_SYNTAX_INDEX_IMPLEMENTATION
#include "syntax_index.h"
#include "stats.h"
#include "foundation/mem_core.h"
#include <string.h>

#define SYNTAX_NO_PARENT UINT32_MAX
/* Below this a fetch by position is a handful of steps, and most nodes end here.
 * Only a node with more children than this gets a list. */
enum { SYNTAX_WIDE_POSITION = 32 };

struct cbm_syntax_wide {
    const void *id;
    TSNode *all, *named;
    uint32_t all_count, named_count;
};

static _Thread_local cbm_syntax_index_t *current_index;

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
static _Atomic bool test_plain;
void cbm_syntax_index_test_set_plain(bool plain) {
    atomic_store(&test_plain, plain);
}
/* The sibling steps ts_node_parent takes: at each level, the children before the
 * one on the path to node. */
static void test_charge_parent(TSNode node) {
    for (TSNode at = node, up = ts_node_parent(at); !ts_node_is_null(up);
         at = up, up = ts_node_parent(at)) {
        uint64_t steps = 0;
        TSTreeCursor cursor = ts_tree_cursor_new(up);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                steps++;
            } while (ts_tree_cursor_current_node(&cursor).id != at.id &&
                     ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, steps);
    }
}
#endif

void cbm_syntax_index_begin(cbm_syntax_index_t *index, const TSTree *tree) {
    *index = (cbm_syntax_index_t){.previous = current_index, .tree = tree};
    current_index = index;
}

void cbm_syntax_index_end(cbm_syntax_index_t *index) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, index->nodes);
    cbm_free(CBM_MEM_CLASS_EXTRACT, index->parents);
    cbm_free(CBM_MEM_CLASS_EXTRACT, index->before);
    cbm_free(CBM_MEM_CLASS_EXTRACT, index->slots);
    for (size_t i = 0; i < index->wide_capacity; i++) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, index->wide[i].all);
        cbm_free(CBM_MEM_CLASS_EXTRACT, index->wide[i].named);
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, index->wide);
    current_index = index->previous;
}

/* Node ids are addresses of pointer-aligned slots in their parent's child array. */
static size_t id_hash(const void *id) {
    uint64_t value = (uint64_t)(uintptr_t)id >> 3;
    value *= UINT64_C(0x9E3779B97F4A7C15);
    return (size_t)(value ^ (value >> 29));
}

static void parents_insert(cbm_syntax_index_t *index, uint32_t position) {
    size_t at = id_hash(index->nodes[position].id) & index->mask;
    while (index->slots[at])
        at = (at + 1) & index->mask;
    index->slots[at] = position + 1;
}

/* One walk with no stack: a node's list position leads back to its parent's. */
static void parents_build(cbm_syntax_index_t *index) {
    TSNode root = ts_tree_root_node(index->tree);
    size_t expected = ts_node_descendant_count(root);
    index->parents_unavailable = true;
    if (!expected || expected >= UINT32_MAX / 2)
        return;
    size_t capacity = 16;
    while (capacity < expected * 2)
        capacity *= 2;
    index->nodes = cbm_alloc(CBM_MEM_CLASS_EXTRACT, expected * sizeof(*index->nodes));
    index->parents = cbm_alloc(CBM_MEM_CLASS_EXTRACT, expected * sizeof(*index->parents));
    index->before = cbm_alloc(CBM_MEM_CLASS_EXTRACT, expected * sizeof(*index->before));
    index->slots = cbm_calloc(CBM_MEM_CLASS_EXTRACT, capacity * sizeof(*index->slots));
    if (!index->nodes || !index->parents || !index->before || !index->slots)
        return;
    index->mask = capacity - 1;
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    uint32_t at = 0, count = 1;
    bool complete = true;
    index->nodes[0] = root;
    index->parents[0] = SYNTAX_NO_PARENT;
    index->before[0] = SYNTAX_NO_PARENT;
    parents_insert(index, 0);
    for (;;) {
        uint32_t parent = at, before = SYNTAX_NO_PARENT;
        if (!ts_tree_cursor_goto_first_child(&cursor)) {
            bool ended = false;
            while (!ended && !ts_tree_cursor_goto_next_sibling(&cursor)) {
                ended = !ts_tree_cursor_goto_parent(&cursor);
                if (!ended)
                    at = index->parents[at];
            }
            if (ended)
                break;
            parent = index->parents[at];
            before = at;
        }
        if (count == expected) {
            complete = false;
            break;
        }
        at = count++;
        index->nodes[at] = ts_tree_cursor_current_node(&cursor);
        index->parents[at] = parent;
        index->before[at] = before;
        parents_insert(index, at);
    }
    ts_tree_cursor_delete(&cursor);
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_SYNTAX_INDEX_NODES, count);
    index->parents_unavailable = !complete;
}

/* The node's place in the table; SYNTAX_NO_PARENT when it has none. */
static uint32_t table_position(cbm_syntax_index_t *index, TSNode node) {
    if (!index || !node.id || node.tree != index->tree)
        return SYNTAX_NO_PARENT;
    if (!index->nodes && !index->parents_unavailable)
        parents_build(index);
    if (index->parents_unavailable)
        return SYNTAX_NO_PARENT;
    for (size_t at = id_hash(node.id) & index->mask; index->slots[at];
         at = (at + 1) & index->mask) {
        uint32_t position = index->slots[at] - 1;
        if (index->nodes[position].id == node.id)
            return position;
    }
    return SYNTAX_NO_PARENT;
}

TSNode cbm_syntax_parent(TSNode node) {
    cbm_syntax_index_t *index = current_index;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (atomic_load(&test_plain)) {
        if (index && node.tree == index->tree)
            test_charge_parent(node);
        return ts_node_parent(node);
    }
#endif
    uint32_t position = table_position(index, node);
    if (position != SYNTAX_NO_PARENT) {
        uint32_t parent = index->parents[position];
        return parent == SYNTAX_NO_PARENT ? (TSNode){0} : index->nodes[parent];
    }
    return ts_node_parent(node);
}

TSNode cbm_syntax_previous_child(TSNode node) {
    cbm_syntax_index_t *index = current_index;
    bool plain = false;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    plain = atomic_load(&test_plain);
#endif
    uint32_t position = plain ? SYNTAX_NO_PARENT : table_position(index, node);
    if (position != SYNTAX_NO_PARENT) {
        uint32_t before = index->before[position];
        return before == SYNTAX_NO_PARENT ? (TSNode){0} : index->nodes[before];
    }
    TSNode parent = plain ? ts_node_parent(node) : cbm_syntax_parent(node);
    if (ts_node_is_null(parent))
        return (TSNode){0};
    TSNode before = {0};
    uint64_t steps = 0;
    bool found = false;
    TSTreeCursor cursor = ts_tree_cursor_new(parent);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            steps++;
            found = ts_node_eq(child, node);
            if (!found)
                before = child;
        } while (!found && ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, steps);
    return found ? before : (TSNode){0};
}

static cbm_syntax_wide_t *wide_slot(const cbm_syntax_index_t *index, const void *id) {
    size_t mask = index->wide_capacity - 1;
    size_t at = id_hash(id) & mask;
    while (index->wide[at].id && index->wide[at].id != id)
        at = (at + 1) & mask;
    return &index->wide[at];
}

static bool wide_grow(cbm_syntax_index_t *index) {
    if (index->wide_capacity && index->wide_count < index->wide_capacity / 2)
        return true;
    size_t capacity = index->wide_capacity ? index->wide_capacity * 2 : 16;
    cbm_syntax_wide_t *grown = cbm_calloc(CBM_MEM_CLASS_EXTRACT, capacity * sizeof(*grown));
    if (!grown)
        return false;
    cbm_syntax_wide_t *old = index->wide;
    size_t old_capacity = index->wide_capacity;
    index->wide = grown;
    index->wide_capacity = capacity;
    for (size_t i = 0; i < old_capacity; i++)
        if (old[i].id)
            *wide_slot(index, old[i].id) = old[i];
    cbm_free(CBM_MEM_CLASS_EXTRACT, old);
    return true;
}

/* NULL when memory ran out: the caller falls back to the plain call. */
static const cbm_syntax_wide_t *wide_lists(cbm_syntax_index_t *index, TSNode node) {
    if (index->wide_capacity) {
        const cbm_syntax_wide_t *known = wide_slot(index, node.id);
        if (known->id)
            return known;
    }
    if (!wide_grow(index))
        return NULL;
    uint32_t total = ts_node_child_count(node), named_total = ts_node_named_child_count(node);
    TSNode *all = cbm_alloc(CBM_MEM_CLASS_EXTRACT, total * sizeof(*all));
    TSNode *named = cbm_alloc(CBM_MEM_CLASS_EXTRACT, (named_total + 1) * sizeof(*named));
    if (!all || !named) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, all);
        cbm_free(CBM_MEM_CLASS_EXTRACT, named);
        return NULL;
    }
    uint32_t all_count = 0, named_count = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (all_count < total)
                all[all_count++] = child;
            if (named_count < named_total && ts_node_is_named(child))
                named[named_count++] = child;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_SYNTAX_INDEX_NODES, all_count);
    cbm_syntax_wide_t *slot = wide_slot(index, node.id);
    *slot = (cbm_syntax_wide_t){.id = node.id,
                                .all = all,
                                .named = named,
                                .all_count = all_count,
                                .named_count = named_count};
    index->wide_count++;
    return slot;
}

static const cbm_syntax_wide_t *wide_for(TSNode node, uint32_t position) {
    cbm_syntax_index_t *index = current_index;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (atomic_load(&test_plain)) {
        if (index && node.tree == index->tree)
            cbm_resolve_stats_add(CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, position);
        return NULL;
    }
#endif
    if (position < SYNTAX_WIDE_POSITION || !index || !node.id || node.tree != index->tree)
        return NULL;
    return wide_lists(index, node);
}

TSNode cbm_syntax_child(TSNode node, uint32_t position) {
    const cbm_syntax_wide_t *wide = wide_for(node, position);
    if (!wide)
        return ts_node_child(node, position);
    return position < wide->all_count ? wide->all[position] : (TSNode){0};
}

TSNode cbm_syntax_named_child(TSNode node, uint32_t position) {
    const cbm_syntax_wide_t *wide = wide_for(node, position);
    if (!wide)
        return ts_node_named_child(node, position);
    return position < wide->named_count ? wide->named[position] : (TSNode){0};
}
