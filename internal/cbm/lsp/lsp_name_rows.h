/*
 * lsp_name_rows.h — rows of a per-file table, found by name.
 *
 * A file's import table is searched once per identifier, and a scan of it makes that
 * imports x identifiers: a generated module with thousands of imports took seconds in
 * each pass. These chains answer with the same rows in the same order.
 *
 * cbm_name_rows_first/next index a table of names on the first question, and only a
 * table long enough for that to matter; a short one, which is nearly all of them, is
 * still scanned and allocates nothing. Rows appended later join on the next question.
 * A caller that shrinks or rewrites its table zeroes the CBMNameRows.
 *
 * cbm_name_rows_add/lookup are the same chains built by hand, for a key that also has a
 * scope. Memory comes from the caller's arena.
 */
#ifndef CBM_LSP_NAME_ROWS_H
#define CBM_LSP_NAME_ROWS_H

#include "../arena.h"
#include "lsp_work.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

enum { CBM_NAME_ROWS_MIN = 16, CBM_NAME_ROWS_FIRST_SLOTS = 32 };

typedef struct {
    uint64_t hash;
    const void *scope;
    const char *name;
    int first, last; /* row + 1; 0 marks an unused slot */
} CBMNameRowSlot;

typedef struct {
    CBMNameRowSlot *slots;
    uint32_t capacity, used;
    int *next; /* row -> the next row under its key, or -1 */
    int next_capacity;
    int synced; /* table rows already chained */
    bool failed;
    bool chained; /* cbm_name_rows_add callers: every row is in */
} CBMNameRows;

static inline uint64_t cbm_name_rows_hash(const void *scope, const char *name) {
    uint64_t hash = ((uint64_t)(uintptr_t)scope >> 3) * UINT64_C(0x9E3779B97F4A7C15);
    for (const char *at = name; at && *at; at++)
        hash = (hash ^ (unsigned char)*at) * UINT64_C(0x100000001B3);
    return hash ^ (hash >> 32);
}

static inline CBMNameRowSlot *cbm_name_rows_slot(const CBMNameRows *rows, uint64_t hash,
                                                 const void *scope, const char *name) {
    uint32_t mask = rows->capacity - 1;
    for (uint32_t at = (uint32_t)hash & mask;; at = (at + 1) & mask) {
        CBMNameRowSlot *slot = &rows->slots[at];
        if (!slot->first ||
            (slot->hash == hash && slot->scope == scope &&
             (slot->name == name || (slot->name && name && strcmp(slot->name, name) == 0)))) {
            return slot;
        }
    }
}

/* Rows go in ascending order. false when memory ran out. */
static inline bool cbm_name_rows_add(CBMArena *arena, CBMNameRows *rows, const void *scope,
                                     const char *name, int row) {
    if (!arena)
        return false;
    if (row >= rows->next_capacity) {
        int capacity = rows->next_capacity ? rows->next_capacity * 2 : CBM_NAME_ROWS_FIRST_SLOTS;
        while (capacity <= row)
            capacity *= 2;
        int *next = (int *)cbm_arena_alloc(arena, (size_t)capacity * sizeof(*next));
        if (!next)
            return false;
        if (rows->next_capacity)
            memcpy(next, rows->next, (size_t)rows->next_capacity * sizeof(*next));
        rows->next = next;
        rows->next_capacity = capacity;
    }
    if (rows->used * 2 >= rows->capacity) {
        uint32_t capacity = rows->capacity ? rows->capacity * 2 : CBM_NAME_ROWS_FIRST_SLOTS;
        CBMNameRowSlot *slots =
            (CBMNameRowSlot *)cbm_arena_alloc(arena, (size_t)capacity * sizeof(*slots));
        if (!slots)
            return false;
        memset(slots, 0, (size_t)capacity * sizeof(*slots));
        CBMNameRows grown = *rows;
        grown.slots = slots;
        grown.capacity = capacity;
        for (uint32_t i = 0; i < rows->capacity; i++) {
            const CBMNameRowSlot *old = &rows->slots[i];
            if (old->first)
                *cbm_name_rows_slot(&grown, old->hash, old->scope, old->name) = *old;
        }
        *rows = grown;
    }
    uint64_t hash = cbm_name_rows_hash(scope, name);
    CBMNameRowSlot *slot = cbm_name_rows_slot(rows, hash, scope, name);
    rows->next[row] = -1;
    if (slot->first) {
        rows->next[slot->last - 1] = row;
    } else {
        *slot = (CBMNameRowSlot){.hash = hash, .scope = scope, .name = name, .first = row + 1};
        rows->used++;
    }
    slot->last = row + 1;
    return true;
}

static inline int cbm_name_rows_lookup(const CBMNameRows *rows, const void *scope,
                                       const char *name) {
    if (!rows->capacity)
        return -1;
    return cbm_name_rows_slot(rows, cbm_name_rows_hash(scope, name), scope, name)->first - 1;
}

static inline bool cbm_name_rows_chained(const CBMNameRows *rows, int count) {
    return !rows->failed && count >= CBM_NAME_ROWS_MIN && rows->synced == count;
}

static inline int cbm_name_rows_scan(const char *const *names, int from, int count,
                                     const char *name) {
    for (int i = from; i < count; i++) {
        CBM_LSP_WORK(1);
        if (names[i] && strcmp(names[i], name) == 0)
            return i;
    }
    return -1;
}

/* The first row of names[0..count) equal to name, or -1. */
static inline int cbm_name_rows_first(CBMNameRows *rows, CBMArena *arena, const char *const *names,
                                      int count, const char *name) {
    if (count >= CBM_NAME_ROWS_MIN && !rows->failed) {
        for (; rows->synced < count; rows->synced++) {
            int row = rows->synced;
            if (names[row] && !cbm_name_rows_add(arena, rows, NULL, names[row], row)) {
                rows->failed = true;
                break;
            }
        }
    }
    if (!cbm_name_rows_chained(rows, count))
        return cbm_name_rows_scan(names, 0, count, name);
    CBM_LSP_WORK(1);
    return cbm_name_rows_lookup(rows, NULL, name);
}

/* The next such row after row, or -1. */
static inline int cbm_name_rows_next(const CBMNameRows *rows, const char *const *names, int count,
                                     const char *name, int row) {
    if (!cbm_name_rows_chained(rows, count))
        return cbm_name_rows_scan(names, row + 1, count, name);
    CBM_LSP_WORK(1);
    return rows->next[row];
}

#endif /* CBM_LSP_NAME_ROWS_H */
