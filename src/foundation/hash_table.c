/*
 * hash_table.c — Robin Hood open-addressing hash table.
 *
 * Key design choices:
 *   - FNV-1a hash (fast, good distribution for strings)
 *   - Robin Hood insertion: on collision, the key with shorter probe
 *     distance yields its slot. This bounds max probe distance.
 *   - Backward-shift deletion: no tombstones needed.
 *   - Load factor 75% triggers 2x resize.
 */
#include "hash_table.h"
#include "foundation/constants.h"

enum {
    HT_MIN_CAP = 8,     /* minimum hash table capacity */
    HT_INITIAL_PSL = 1, /* initial probe sequence length */
    HT_LOAD_NUM = 3,    /* load factor numerator (75%) */
    HT_LOAD_DEN = 4,    /* load factor denominator */
    HT_SHIFT_1 = 1,     /* bit shift amounts for next_pow2 */
    HT_SHIFT_2 = 2,
    HT_SHIFT_4 = 4,
    HT_SHIFT_8 = 8,
    HT_SHIFT_16 = 16,
};
#include <stdint.h> // uint32_t
#include <stdlib.h>
#include <string.h>

/* FNV-1a hash constants (published by Fowler/Noll/Vo) */
#define FNV_OFFSET_BASIS 2166136261U
#define FNV_PRIME 16777619U

/* FNV-1a hash for strings */
static uint32_t fnv1a(const char *key) {
    uint32_t h = FNV_OFFSET_BASIS;
    for (const unsigned char *p = (const unsigned char *)key; *p; p++) {
        h ^= *p;
        h *= FNV_PRIME;
    }
    return h;
}

/* Round up to next power of 2 */
static uint32_t next_pow2(uint32_t v) {
    if (v < HT_MIN_CAP) {
        return HT_MIN_CAP;
    }
    v--;
    v |= v >> HT_SHIFT_1;
    v |= v >> HT_SHIFT_2;
    v |= v >> HT_SHIFT_4;
    v |= v >> HT_SHIFT_8;
    v |= v >> HT_SHIFT_16;
    return v + SKIP_ONE;
}

CBMHashTable *cbm_ht_create(uint32_t initial_capacity) {
    CBMHashTable *ht = (CBMHashTable *)calloc(CBM_ALLOC_ONE, sizeof(CBMHashTable));
    if (!ht) {
        return NULL;
    }
    ht->capacity = next_pow2(initial_capacity);
    ht->mask = ht->capacity - SKIP_ONE;
    ht->entries = (CBMHTEntry *)calloc(ht->capacity, sizeof(CBMHTEntry));
    if (!ht->entries) {
        free(ht);
        return NULL;
    }
    return ht;
}

void cbm_ht_free(CBMHashTable *ht) {
    if (!ht) {
        return;
    }
    free(ht->entries);
    free(ht);
}

static void ht_resize(CBMHashTable *ht) {
    uint32_t new_cap = ht->capacity * PAIR_LEN;
    uint32_t new_mask = new_cap - SKIP_ONE;
    CBMHTEntry *new_entries = (CBMHTEntry *)calloc(new_cap, sizeof(CBMHTEntry));
    if (!new_entries) {
        return; /* OOM: keep old table */
    }

    for (uint32_t i = 0; i < ht->capacity; i++) {
        const CBMHTEntry *e = &ht->entries[i];
        if (e->psl == 0) {
            continue; /* empty slot */
        }

        /* Re-insert into new table */
        uint32_t idx = e->hash & new_mask;
        CBMHTEntry cur = {.key = e->key, .value = e->value, .hash = e->hash, .psl = HT_INITIAL_PSL};
        for (;;) {
            CBMHTEntry *slot = &new_entries[idx];
            if (slot->psl == 0) {
                *slot = cur;
                break;
            }
            /* Robin Hood: steal from rich (shorter probe) */
            if (cur.psl > slot->psl) {
                CBMHTEntry tmp = *slot;
                *slot = cur;
                cur = tmp;
            }
            cur.psl++;
            idx = (idx + SKIP_ONE) & new_mask;
        }
    }

    free(ht->entries);
    ht->entries = new_entries;
    ht->capacity = new_cap;
    ht->mask = new_mask;
}

void *cbm_ht_set(CBMHashTable *ht, const char *key, void *value) {
    /* Resize at 75% load */
    if (ht->count * HT_LOAD_DEN >= ht->capacity * HT_LOAD_NUM) {
        ht_resize(ht);
    }

    uint32_t h = fnv1a(key);
    uint32_t idx = h & ht->mask;
    CBMHTEntry cur = {.key = key, .value = value, .hash = h, .psl = HT_INITIAL_PSL};
    void *prev_value = NULL;

    for (;;) {
        CBMHTEntry *slot = &ht->entries[idx];

        if (slot->psl == 0) {
            /* Empty slot — insert here */
            *slot = cur;
            ht->count++;
            return prev_value;
        }

        /* Check for existing key */
        if (slot->hash == cur.hash && strcmp(slot->key, cur.key) == 0) {
            prev_value = slot->value;
            slot->value = cur.value;
            slot->key = cur.key; /* update key pointer in case caller uses different buffer */
            return prev_value;
        }

        /* Robin Hood: steal from rich */
        if (cur.psl > slot->psl) {
            CBMHTEntry tmp = *slot;
            *slot = cur;
            cur = tmp;
        }

        cur.psl++;
        idx = (idx + SKIP_ONE) & ht->mask;
    }
}

void *cbm_ht_get(const CBMHashTable *ht, const char *key) {
    uint32_t h = fnv1a(key);
    uint32_t idx = h & ht->mask;
    uint32_t psl = SKIP_ONE;

    for (;;) {
        const CBMHTEntry *slot = &ht->entries[idx];
        if (slot->psl == 0) {
            return NULL; /* empty — not found */
        }
        if (psl > slot->psl) {
            return NULL; /* Robin Hood guarantee */
        }
        if (slot->hash == h && strcmp(slot->key, key) == 0) {
            return slot->value;
        }
        psl++;
        idx = (idx + SKIP_ONE) & ht->mask;
    }
}

bool cbm_ht_has(const CBMHashTable *ht, const char *key) {
    return cbm_ht_get(ht, key) != NULL;
}

const char *cbm_ht_get_key(const CBMHashTable *ht, const char *key) {
    if (!ht || !key) {
        return NULL;
    }
    uint32_t h = fnv1a(key);
    uint32_t idx = h & ht->mask;
    uint32_t psl = SKIP_ONE;
    for (;;) {
        const CBMHTEntry *slot = &ht->entries[idx];
        if (slot->psl == 0) {
            return NULL;
        }
        if (psl > slot->psl) {
            return NULL;
        }
        if (slot->hash == h && strcmp(slot->key, key) == 0) {
            return slot->key;
        }
        psl++;
        idx = (idx + SKIP_ONE) & ht->mask;
    }
}

void *cbm_ht_delete(CBMHashTable *ht, const char *key) {
    uint32_t h = fnv1a(key);
    uint32_t idx = h & ht->mask;
    uint32_t psl = SKIP_ONE;

    /* Find the entry */
    for (;;) {
        CBMHTEntry *slot = &ht->entries[idx];
        if (slot->psl == 0) {
            return NULL;
        }
        if (psl > slot->psl) {
            return NULL;
        }
        if (slot->hash == h && strcmp(slot->key, key) == 0) {
            void *removed = slot->value;
            ht->count--;

            /* Backward shift: fill the hole */
            for (;;) {
                uint32_t next_idx = (idx + SKIP_ONE) & ht->mask;
                const CBMHTEntry *next = &ht->entries[next_idx];
                if (next->psl <= HT_INITIAL_PSL) {
                    /* Next slot is empty or at home — stop */
                    ht->entries[idx] = (CBMHTEntry){0};
                    break;
                }
                /* Shift next entry back */
                ht->entries[idx] = *next;
                ht->entries[idx].psl--;
                idx = next_idx;
            }
            return removed;
        }
        psl++;
        idx = (idx + SKIP_ONE) & ht->mask;
    }
}

uint32_t cbm_ht_count(const CBMHashTable *ht) {
    return ht ? ht->count : 0;
}

void cbm_ht_foreach(const CBMHashTable *ht, cbm_ht_iter_fn fn, void *userdata) {
    if (!ht || !fn) {
        return;
    }
    for (uint32_t i = 0; i < ht->capacity; i++) {
        if (ht->entries[i].psl > 0) {
            fn(ht->entries[i].key, ht->entries[i].value, userdata);
        }
    }
}

void cbm_ht_clear(CBMHashTable *ht) {
    if (!ht) {
        return;
    }
    memset(ht->entries, 0, ht->capacity * sizeof(CBMHTEntry));
    ht->count = 0;
}
