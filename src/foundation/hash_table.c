/*
 * hash_table.c — CBMHashTable backed by Verstable.
 *
 * Public API in hash_table.h is unchanged. Internals are a Verstable
 * template instantiation (const char* → void*). Verstable is a 2024
 * open-addressing hash table using quadratic probing with metadata
 * stored separately from buckets (4-bit hash fragment + 11-bit
 * displacement + 1-bit in-home-bucket flag per uint16_t). Documented
 * in vendored/verstable/verstable.h.
 *
 * Why swap the prior Robin Hood implementation: cumulative profiling
 * showed cbm_ht_get is a hot path in resolve_file_calls's per-call
 * registry resolution. Verstable's 4-bit hash-fragment metadata
 * sidesteps most key comparisons during chain walks, which the prior
 * implementation could not.
 *
 * Lifetime: keys are BORROWED pointers (caller owns the strings).
 * Verstable's KEY_TY is const char*; the templated comparison +
 * hash use the standard vt_cmpr_string / vt_hash_string helpers.
 */
#include "foundation/constants.h"
#include "hash_table.h"
#include <stdlib.h>
#include <string.h>

/* Instantiate a Verstable map of (const char* → void*). The single
 * include below generates static inline functions named cbm_vt_init,
 * cbm_vt_cleanup, cbm_vt_get, cbm_vt_insert, etc., plus the cbm_vt
 * struct itself. */
#define NAME cbm_vt
#define KEY_TY const char *
#define VAL_TY void *
#define HASH_FN vt_hash_string
#define CMPR_FN vt_cmpr_string
#include "../../internal/cbm/vendored/verstable/verstable.h"

/* The opaque CBMHashTable struct holds the Verstable instance + a
 * count cache (Verstable's _size traversal is O(buckets) so we keep
 * our own atomic-free counter). */
struct CBMHashTable {
    cbm_vt vt;
};

CBMHashTable *cbm_ht_create(uint32_t initial_capacity) {
    CBMHashTable *ht = (CBMHashTable *)calloc(CBM_ALLOC_ONE, sizeof(*ht));
    if (!ht)
        return NULL;
    cbm_vt_init(&ht->vt);
    if (initial_capacity > 0) {
        /* Reserve enough buckets for the requested entries. Verstable
         * computes the minimum bucket count internally. */
        if (!cbm_vt_reserve(&ht->vt, (size_t)initial_capacity)) {
            cbm_vt_cleanup(&ht->vt);
            free(ht);
            return NULL;
        }
    }
    return ht;
}

void cbm_ht_free(CBMHashTable *ht) {
    if (!ht)
        return;
    cbm_vt_cleanup(&ht->vt);
    free(ht);
}

void *cbm_ht_set(CBMHashTable *ht, const char *key, void *value) {
    if (!ht || !key)
        return NULL;
    /* Capture previous value (if any) before overwriting.
     * Verstable's _insert overwrites silently and returns an iterator
     * to the (now updated) entry — we have to peek first to surface
     * the prior value to the caller (back-compat with our API). */
    void *prev = NULL;
    cbm_vt_itr itr = cbm_vt_get(&ht->vt, key);
    if (!cbm_vt_is_end(itr)) {
        prev = itr.data->val;
    }
    (void)cbm_vt_insert(&ht->vt, key, value);
    return prev;
}

void *cbm_ht_get(const CBMHashTable *ht, const char *key) {
    if (!ht || !key)
        return NULL;
    cbm_vt_itr itr = cbm_vt_get(&ht->vt, key);
    if (cbm_vt_is_end(itr))
        return NULL;
    return itr.data->val;
}

bool cbm_ht_has(const CBMHashTable *ht, const char *key) {
    if (!ht || !key)
        return false;
    cbm_vt_itr itr = cbm_vt_get(&ht->vt, key);
    return !cbm_vt_is_end(itr);
}

const char *cbm_ht_get_key(const CBMHashTable *ht, const char *key) {
    if (!ht || !key)
        return NULL;
    cbm_vt_itr itr = cbm_vt_get(&ht->vt, key);
    if (cbm_vt_is_end(itr))
        return NULL;
    return itr.data->key;
}

void *cbm_ht_delete(CBMHashTable *ht, const char *key) {
    if (!ht || !key)
        return NULL;
    cbm_vt_itr itr = cbm_vt_get(&ht->vt, key);
    if (cbm_vt_is_end(itr))
        return NULL;
    void *prev = itr.data->val;
    (void)cbm_vt_erase(&ht->vt, key);
    return prev;
}

uint32_t cbm_ht_count(const CBMHashTable *ht) {
    if (!ht)
        return 0;
    return (uint32_t)cbm_vt_size(&ht->vt);
}

void cbm_ht_foreach(const CBMHashTable *ht, cbm_ht_iter_fn fn, void *userdata) {
    if (!ht || !fn)
        return;
    for (cbm_vt_itr itr = cbm_vt_first(&ht->vt); !cbm_vt_is_end(itr); itr = cbm_vt_next(itr)) {
        fn(itr.data->key, itr.data->val, userdata);
    }
}

void cbm_ht_clear(CBMHashTable *ht) {
    if (!ht)
        return;
    cbm_vt_clear(&ht->vt);
}
