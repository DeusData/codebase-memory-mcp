/*
 * dyn_array.h — Type-safe growable arrays via macros (header-only).
 *
 * Usage:
 *   CBM_DYN_ARRAY(int) nums = {0};    // zero-init
 *   cbm_da_push(&nums, 42);
 *   cbm_da_push(&nums, 99);
 *   for (int i = 0; i < nums.count; i++)
 *       printf("%d\n", nums.items[i]);
 *   cbm_da_free(&nums);
 *
 * Design:
 *   - Items are contiguous in memory (cache-friendly)
 *   - Grows by 2x (amortized O(1) push)
 *   - Uses realloc — NOT arena-compatible (use CBMDefArray etc. for arena)
 *   - Header-only: no .c file needed
 */
#ifndef CBM_DYN_ARRAY_H
#define CBM_DYN_ARRAY_H

#include <stdlib.h>
#include <string.h>
#include "mem_core.h" /* same directory: units without -Isrc include this header relatively */

/* Declare a dynamic array type for a given element type. */
#define CBM_DYN_ARRAY(T) \
    struct {             \
        T *items;        \
        int count;       \
        int cap;         \
    }

/* Push an element. Grows by 2x when full. */
#define cbm_da_push(da, item)                                                  \
    do {                                                                       \
        if ((da)->count >= (da)->cap) {                                        \
            int _new_cap = (da)->cap ? (da)->cap * 2 : 8;                      \
            void *_new = cbm_realloc(CBM_MEM_CLASS_DYN_ARRAY, (da)->items,     \
                                     (size_t)_new_cap * sizeof(*(da)->items)); \
            if (!_new)                                                         \
                break;                                                         \
            (da)->items = _new;                                                \
            (da)->cap = _new_cap;                                              \
        }                                                                      \
        (da)->items[(da)->count++] = (item);                                   \
    } while (0)

/* Push with a caller-chosen first capacity. cbm_da_push starts at 8 slots,
 * which is 64 bytes of headroom on every array; an index that keeps one
 * small array per key (millions of keys, most holding one or two entries)
 * pays that on every key. Growth stays 2x. */
#define cbm_da_push_min(da, item, min_cap)                                     \
    do {                                                                       \
        if ((da)->count >= (da)->cap) {                                        \
            int _new_cap = (da)->cap ? (da)->cap * 2 : (min_cap);              \
            void *_new = cbm_realloc(CBM_MEM_CLASS_DYN_ARRAY, (da)->items,     \
                                     (size_t)_new_cap * sizeof(*(da)->items)); \
            if (!_new)                                                         \
                break;                                                         \
            (da)->items = _new;                                                \
            (da)->cap = _new_cap;                                              \
        }                                                                      \
        (da)->items[(da)->count++] = (item);                                   \
    } while (0)

/* Push an element with a pointer return (for in-place init). */
#define cbm_da_push_ptr(da)                                                               \
    (((da)->count >= (da)->cap                                                            \
          ? ((void)((da)->cap = (da)->cap ? (da)->cap * 2 : 8),                           \
             (void)((da)->items = cbm_realloc(CBM_MEM_CLASS_DYN_ARRAY, (da)->items,       \
                                              (size_t)(da)->cap * sizeof(*(da)->items)))) \
          : (void)0),                                                                     \
     &(da)->items[(da)->count++])

/* Pop last element. Returns the element. Undefined if empty. */
#define cbm_da_pop(da) ((da)->items[--(da)->count])

/* Get last element without removing. Undefined if empty. */
#define cbm_da_last(da) ((da)->items[(da)->count - 1])

/* Clear without freeing (reset count to 0). */
#define cbm_da_clear(da) ((da)->count = 0)

/* Free all memory. */
#define cbm_da_free(da)                                 \
    do {                                                \
        cbm_free(CBM_MEM_CLASS_DYN_ARRAY, (da)->items); \
        (da)->items = NULL;                             \
        (da)->count = 0;                                \
        (da)->cap = 0;                                  \
    } while (0)

/* Reserve capacity (grow if needed, never shrink). */
#define cbm_da_reserve(da, n)                                              \
    do {                                                                   \
        if ((n) > (da)->cap) {                                             \
            void *_new = cbm_realloc(CBM_MEM_CLASS_DYN_ARRAY, (da)->items, \
                                     (size_t)(n) * sizeof(*(da)->items));  \
            if (_new) {                                                    \
                (da)->items = _new;                                        \
                (da)->cap = (n);                                           \
            }                                                              \
        }                                                                  \
    } while (0)

/* Insert at index, shifting elements right. */
#define cbm_da_insert(da, idx, item)                                           \
    do {                                                                       \
        cbm_da_push(da, item); /* ensure space */                              \
        if ((idx) < (da)->count - 1) {                                         \
            memmove(&(da)->items[(idx) + 1], &(da)->items[(idx)],              \
                    (size_t)((da)->count - 1 - (idx)) * sizeof(*(da)->items)); \
            (da)->items[(idx)] = (item);                                       \
        }                                                                      \
    } while (0)

/* Remove at index, shifting elements left. */
#define cbm_da_remove(da, idx)                                                 \
    do {                                                                       \
        if ((idx) < (da)->count - 1) {                                         \
            memmove(&(da)->items[(idx)], &(da)->items[(idx) + 1],              \
                    (size_t)((da)->count - 1 - (idx)) * sizeof(*(da)->items)); \
        }                                                                      \
        (da)->count--;                                                         \
    } while (0)

#endif /* CBM_DYN_ARRAY_H */
