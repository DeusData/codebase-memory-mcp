/*
 * ts_node_stack.h — Growable TSNode stack for AST traversal.
 *
 * Replaces fixed-size TSNode stack[] arrays that silently drop AST subtrees
 * when the stack overflows (GitHub issue #199).
 *
 * Uses the arena allocator for zero-fragmentation growth: old blocks are
 * abandoned (freed when the arena is destroyed at end of file extraction).
 * Initial capacity matches the previous fixed caps so small files allocate
 * no extra memory.
 */
#ifndef CBM_TS_NODE_STACK_H
#define CBM_TS_NODE_STACK_H

#include "arena.h"
#include "tree_sitter/api.h"
#include <string.h> /* memcpy */

typedef struct {
    TSNode *items;
    int count;
    int cap;
} TSNodeStack;

/* Initialize a stack with the given initial capacity, arena-allocated. */
static inline void ts_nstack_init(TSNodeStack *s, CBMArena *arena, int initial_cap) {
    s->items = (TSNode *)cbm_arena_alloc(arena, (size_t)initial_cap * sizeof(TSNode));
    s->count = 0;
    s->cap = s->items ? initial_cap : 0;
}

/* Push a node onto the stack, growing 2x if needed. */
static inline void ts_nstack_push(TSNodeStack *s, CBMArena *arena, TSNode node) {
    if (s->count >= s->cap) {
        int new_cap = s->cap ? s->cap * 2 : 512;
        TSNode *new_items = (TSNode *)cbm_arena_alloc(arena, (size_t)new_cap * sizeof(TSNode));
        if (!new_items) return; /* OOM: best-effort, stop growing */
        if (s->items && s->count > 0) {
            memcpy(new_items, s->items, (size_t)s->count * sizeof(TSNode));
        }
        /* Old s->items is abandoned in the arena — freed on arena_destroy. */
        s->items = new_items;
        s->cap = new_cap;
    }
    s->items[s->count++] = node;
}

/* Pop a node from the stack. Caller must check s->count > 0. */
static inline TSNode ts_nstack_pop(TSNodeStack *s) {
    return s->items[--s->count];
}

#endif /* CBM_TS_NODE_STACK_H */
