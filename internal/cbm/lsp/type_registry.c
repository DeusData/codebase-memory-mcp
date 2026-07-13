#include "type_registry.h"
#include <string.h>
#include <stdlib.h>

// FNV-1a 64-bit hash. Public domain. Inline so no extra dep.
static uint64_t fnv1a(const char *s) {
    uint64_t h = 0xcbf29ce484222325ULL;
    if (!s)
        return h;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 0x100000001b3ULL;
    }
    return h;
}

static uint64_t fnv1a_pair(const char *a, const char *b) {
    uint64_t h = 0xcbf29ce484222325ULL;
    if (a) {
        while (*a) {
            h ^= (unsigned char)*a++;
            h *= 0x100000001b3ULL;
        }
    }
    h ^= 0xff; // separator
    h *= 0x100000001b3ULL;
    if (b) {
        while (*b) {
            h ^= (unsigned char)*b++;
            h *= 0x100000001b3ULL;
        }
    }
    return h;
}

static int next_pow2(int n) {
    int p = 1;
    while (p < n)
        p <<= 1;
    return p;
}

static void build_qn_index(CBMTypeRegistry *reg, CBMArena *idx_arena, bool for_funcs) {
    int count = for_funcs ? reg->func_count : reg->type_count;
    if (count == 0)
        return;
    int bucket_count = next_pow2(count * 2);
    if (bucket_count < 16)
        bucket_count = 16;

    int *buckets = (int *)cbm_arena_alloc(idx_arena, (size_t)bucket_count * sizeof(int));
    CBMRegistryHashEntry *entries = (CBMRegistryHashEntry *)cbm_arena_alloc(
        idx_arena, (size_t)count * sizeof(CBMRegistryHashEntry));
    if (!buckets || !entries)
        return;
    for (int i = 0; i < bucket_count; i++)
        buckets[i] = -1;

    for (int i = 0; i < count; i++) {
        const char *qn = for_funcs ? reg->funcs[i].qualified_name : reg->types[i].qualified_name;
        if (!qn)
            continue;
        uint64_t h = fnv1a(qn);
        int slot = (int)(h & (uint64_t)(bucket_count - 1));
        entries[i].hash = h;
        entries[i].payload_index = i;
        entries[i].next_index = buckets[slot];
        entries[i].slot = slot;
        buckets[slot] = i;
    }
    if (for_funcs) {
        reg->func_qn_buckets = buckets;
        reg->func_qn_entries = entries;
        reg->func_qn_bucket_count = bucket_count;
        reg->func_qn_entry_count = count;
    } else {
        reg->type_qn_buckets = buckets;
        reg->type_qn_entries = entries;
        reg->type_qn_bucket_count = bucket_count;
        reg->type_qn_entry_count = count;
    }
}

static void build_method_index(CBMTypeRegistry *reg, CBMArena *idx_arena) {
    if (reg->func_count == 0)
        return;
    int bucket_count = next_pow2(reg->func_count * 2);
    if (bucket_count < 16)
        bucket_count = 16;
    int *buckets = (int *)cbm_arena_alloc(idx_arena, (size_t)bucket_count * sizeof(int));
    CBMRegistryHashEntry *entries = (CBMRegistryHashEntry *)cbm_arena_alloc(
        reg->arena, (size_t)reg->func_count * sizeof(CBMRegistryHashEntry));
    if (!buckets || !entries)
        return;
    for (int i = 0; i < bucket_count; i++)
        buckets[i] = -1;

    int idx = 0;
    for (int i = 0; i < reg->func_count; i++) {
        const CBMRegisteredFunc *f = &reg->funcs[i];
        if (!f->receiver_type || !f->short_name)
            continue;
        uint64_t h = fnv1a_pair(f->receiver_type, f->short_name);
        int slot = (int)(h & (uint64_t)(bucket_count - 1));
        entries[idx].hash = h;
        entries[idx].payload_index = i;
        entries[idx].next_index = buckets[slot];
        entries[idx].slot = slot;
        buckets[slot] = idx;
        idx++;
    }
    reg->method_buckets = buckets;
    reg->method_entries = entries;
    reg->method_bucket_count = bucket_count;
    reg->method_entry_count = idx;
}

/* Index: bare(embedded_type) -> chain of TYPE indices declaring it. One entry per
 * (type, embedded_type). Types are iterated DESCENDING and prepended so each bucket
 * chain ends up in ASCENDING type-index order (preserves the Rust loops' first-match
 * tie-break); a type's multiple entries land adjacent so the iterator can dedup them. */
static void build_type_embed_index(CBMTypeRegistry *reg, CBMArena *idx_arena) {
    int ecount = 0;
    for (int i = 0; i < reg->type_count; i++) {
        const char **e = reg->types[i].embedded_types;
        if (!e)
            continue;
        for (int j = 0; e[j]; j++)
            ecount++;
    }
    if (ecount == 0)
        return;
    int bucket_count = next_pow2(ecount * 2);
    if (bucket_count < 16)
        bucket_count = 16;
    int *buckets = (int *)cbm_arena_alloc(idx_arena, (size_t)bucket_count * sizeof(int));
    CBMRegistryHashEntry *entries = (CBMRegistryHashEntry *)cbm_arena_alloc(
        idx_arena, (size_t)ecount * sizeof(CBMRegistryHashEntry));
    if (!buckets || !entries)
        return;
    for (int i = 0; i < bucket_count; i++)
        buckets[i] = -1;
    int idx = 0;
    for (int i = reg->type_count - 1; i >= 0; i--) {
        const char **e = reg->types[i].embedded_types;
        if (!e)
            continue;
        for (int j = 0; e[j]; j++) {
            const char *s = e[j];
            if (!s)
                continue;
            const char *dot = strrchr(s, '.');
            const char *bare = dot ? dot + 1 : s;
            uint64_t h = fnv1a(bare);
            int slot = (int)(h & (uint64_t)(bucket_count - 1));
            entries[idx].hash = h;
            entries[idx].payload_index = i;
            entries[idx].next_index = buckets[slot];
            entries[idx].slot = slot;
            buckets[slot] = idx;
            idx++;
        }
    }
    reg->type_embed_buckets = buckets;
    reg->type_embed_entries = entries;
    reg->type_embed_bucket_count = bucket_count;
    reg->type_embed_entry_count = idx;
}

/* Index: short_name -> chain of FREE-function (receiver_type==NULL) indices.
 * Descending-iterate + prepend for ascending chain order (as above). */
static void build_ffunc_short_index(CBMTypeRegistry *reg, CBMArena *idx_arena) {
    int fcount = 0;
    for (int i = 0; i < reg->func_count; i++) {
        const CBMRegisteredFunc *f = &reg->funcs[i];
        if (!f->receiver_type && f->short_name)
            fcount++;
    }
    if (fcount == 0)
        return;
    int bucket_count = next_pow2(fcount * 2);
    if (bucket_count < 16)
        bucket_count = 16;
    int *buckets = (int *)cbm_arena_alloc(idx_arena, (size_t)bucket_count * sizeof(int));
    CBMRegistryHashEntry *entries = (CBMRegistryHashEntry *)cbm_arena_alloc(
        idx_arena, (size_t)fcount * sizeof(CBMRegistryHashEntry));
    if (!buckets || !entries)
        return;
    for (int i = 0; i < bucket_count; i++)
        buckets[i] = -1;
    int idx = 0;
    for (int i = reg->func_count - 1; i >= 0; i--) {
        const CBMRegisteredFunc *f = &reg->funcs[i];
        if (f->receiver_type || !f->short_name)
            continue;
        uint64_t h = fnv1a(f->short_name);
        int slot = (int)(h & (uint64_t)(bucket_count - 1));
        entries[idx].hash = h;
        entries[idx].payload_index = i;
        entries[idx].next_index = buckets[slot];
        entries[idx].slot = slot;
        buckets[slot] = idx;
        idx++;
    }
    reg->ffunc_short_buckets = buckets;
    reg->ffunc_short_entries = entries;
    reg->ffunc_short_bucket_count = bucket_count;
    reg->ffunc_short_entry_count = idx;
}

void cbm_registry_types_by_embedded_bare(const CBMTypeRegistry *reg, const char *bare,
                                         CBMTypeEmbedIter *out) {
    out->reg = reg;
    out->hash = fnv1a(bare);
    out->prev_type = -1;
    if (reg->type_qn_buckets && reg->type_qn_bucket_count > 0) {
        /* finalized: walk the embed chain (if built) then the post-finalize tail */
        if (reg->type_embed_buckets && reg->type_embed_bucket_count > 0) {
            int slot = (int)(out->hash & (uint64_t)(reg->type_embed_bucket_count - 1));
            out->chain_idx = reg->type_embed_buckets[slot];
        } else {
            out->chain_idx = -1;
        }
        out->tail_i = reg->type_qn_entry_count;
        out->tail_end = reg->type_count;
    } else {
        /* unfinalized: full linear scan over all types (identical candidate set) */
        out->chain_idx = -1;
        out->tail_i = 0;
        out->tail_end = reg->type_count;
    }
}

int cbm_type_embed_iter_next(CBMTypeEmbedIter *it) {
    const CBMTypeRegistry *reg = it->reg;
    while (it->chain_idx >= 0) {
        const CBMRegistryHashEntry *e = &reg->type_embed_entries[it->chain_idx];
        int p = e->payload_index;
        uint64_t h = e->hash;
        it->chain_idx = e->next_index;
        if (h != it->hash)
            continue; /* hash collision */
        if (p == it->prev_type)
            continue; /* same type, multiple matching embeds — yield once */
        it->prev_type = p;
        return p;
    }
    if (it->tail_i < it->tail_end)
        return it->tail_i++;
    return -1;
}

void cbm_registry_free_funcs_by_short_name(const CBMTypeRegistry *reg, const char *short_name,
                                           CBMFreeFuncIter *out) {
    out->reg = reg;
    out->hash = fnv1a(short_name);
    if (reg->func_qn_buckets && reg->func_qn_bucket_count > 0) {
        if (reg->ffunc_short_buckets && reg->ffunc_short_bucket_count > 0) {
            int slot = (int)(out->hash & (uint64_t)(reg->ffunc_short_bucket_count - 1));
            out->chain_idx = reg->ffunc_short_buckets[slot];
        } else {
            out->chain_idx = -1;
        }
        out->tail_i = reg->func_qn_entry_count;
        out->tail_end = reg->func_count;
    } else {
        out->chain_idx = -1;
        out->tail_i = 0;
        out->tail_end = reg->func_count;
    }
}

int cbm_free_func_iter_next(CBMFreeFuncIter *it) {
    const CBMTypeRegistry *reg = it->reg;
    while (it->chain_idx >= 0) {
        const CBMRegistryHashEntry *e = &reg->ffunc_short_entries[it->chain_idx];
        int p = e->payload_index;
        uint64_t h = e->hash;
        it->chain_idx = e->next_index;
        if (h != it->hash)
            continue;
        return p; /* each free func has one short_name → no dedup needed */
    }
    if (it->tail_i < it->tail_end)
        return it->tail_i++;
    return -1;
}

void cbm_registry_finalize_into(CBMTypeRegistry *reg, CBMArena *idx_arena) {
    if (!reg || !idx_arena)
        return;
    build_qn_index(reg, idx_arena, /*for_funcs=*/true);
    build_qn_index(reg, idx_arena, /*for_funcs=*/false);
    build_method_index(reg, idx_arena);
    build_type_embed_index(reg, idx_arena);
    build_ffunc_short_index(reg, idx_arena);
}

void cbm_registry_finalize(CBMTypeRegistry *reg) {
    if (!reg)
        return;
    cbm_registry_finalize_into(reg, reg->arena);
}

void cbm_registry_init(CBMTypeRegistry *reg, CBMArena *arena) {
    memset(reg, 0, sizeof(CBMTypeRegistry));
    reg->arena = arena;
}

void cbm_registry_add_func(CBMTypeRegistry *reg, CBMRegisteredFunc func) {
    if (reg->read_only) {
        return; /* sealed Tier-2 shared registry: refuse post-finalize mutation (O(n^2)+race guard)
                 */
    }
    if (reg->func_count >= reg->func_cap) {
        int new_cap = reg->func_cap == 0 ? 64 : reg->func_cap * 2;
        CBMRegisteredFunc *new_items = (CBMRegisteredFunc *)cbm_arena_alloc(
            reg->arena, (size_t)new_cap * sizeof(CBMRegisteredFunc));
        if (!new_items)
            return;
        if (reg->funcs && reg->func_count > 0) {
            memcpy(new_items, reg->funcs, (size_t)reg->func_count * sizeof(CBMRegisteredFunc));
        }
        reg->funcs = new_items;
        reg->func_cap = new_cap;
    }
    reg->funcs[reg->func_count++] = func;
}

void cbm_registry_add_type(CBMTypeRegistry *reg, CBMRegisteredType type) {
    if (reg->read_only) {
        return; /* sealed Tier-2 shared registry: refuse post-finalize mutation (O(n^2)+race guard)
                 */
    }
    if (reg->type_count >= reg->type_cap) {
        int new_cap = reg->type_cap == 0 ? 64 : reg->type_cap * 2;
        CBMRegisteredType *new_items = (CBMRegisteredType *)cbm_arena_alloc(
            reg->arena, (size_t)new_cap * sizeof(CBMRegisteredType));
        if (!new_items)
            return;
        if (reg->types && reg->type_count > 0) {
            memcpy(new_items, reg->types, (size_t)reg->type_count * sizeof(CBMRegisteredType));
        }
        reg->types = new_items;
        reg->type_cap = new_cap;
    }
    reg->types[reg->type_count++] = type;
}

static const CBMRegisteredFunc *lookup_method_self(const CBMTypeRegistry *reg,
                                                   const char *receiver_qn,
                                                   const char *method_name) {
    // Hashed path when registry is finalized.
    if (reg->method_buckets && reg->method_bucket_count > 0) {
        uint64_t h = fnv1a_pair(receiver_qn, method_name);
        int slot = (int)(h & (uint64_t)(reg->method_bucket_count - 1));
        for (int idx = reg->method_buckets[slot]; idx >= 0;
             idx = reg->method_entries[idx].next_index) {
            if (reg->method_entries[idx].hash != h)
                continue;
            const CBMRegisteredFunc *f = &reg->funcs[reg->method_entries[idx].payload_index];
            if (f->receiver_type && f->short_name && strcmp(f->receiver_type, receiver_qn) == 0 &&
                strcmp(f->short_name, method_name) == 0) {
                return f;
            }
        }
        /* Funcs added AFTER finalize are not in the buckets — scan only that
         * tail (func_qn_entry_count == func_count at finalize time) so
         * post-finalize additions stay visible instead of silently vanishing. */
        for (int i = reg->func_qn_entry_count; i < reg->func_count; i++) {
            const CBMRegisteredFunc *f = &reg->funcs[i];
            if (f->receiver_type && f->short_name && strcmp(f->receiver_type, receiver_qn) == 0 &&
                strcmp(f->short_name, method_name) == 0) {
                return f;
            }
        }
        return NULL;
    }

    for (int i = 0; i < reg->func_count; i++) {
        const CBMRegisteredFunc *f = &reg->funcs[i];
        if (f->receiver_type && f->short_name && strcmp(f->receiver_type, receiver_qn) == 0 &&
            strcmp(f->short_name, method_name) == 0) {
            return f;
        }
    }
    return NULL;
}

const CBMRegisteredFunc *cbm_registry_lookup_method(const CBMTypeRegistry *reg,
                                                    const char *receiver_qn,
                                                    const char *method_name) {
    if (!reg || !receiver_qn || !method_name)
        return NULL;
    const CBMRegisteredFunc *r = lookup_method_self(reg, receiver_qn, method_name);
    if (!r && reg->fallback) {
        return cbm_registry_lookup_method(reg->fallback, receiver_qn, method_name);
    }
    return r;
}

static const CBMRegisteredType *lookup_type_self(const CBMTypeRegistry *reg,
                                                 const char *qualified_name) {
    if (reg->type_qn_buckets && reg->type_qn_bucket_count > 0) {
        uint64_t h = fnv1a(qualified_name);
        int slot = (int)(h & (uint64_t)(reg->type_qn_bucket_count - 1));
        for (int idx = reg->type_qn_buckets[slot]; idx >= 0;
             idx = reg->type_qn_entries[idx].next_index) {
            if (reg->type_qn_entries[idx].hash != h)
                continue;
            int p = reg->type_qn_entries[idx].payload_index;
            if (reg->types[p].qualified_name &&
                strcmp(reg->types[p].qualified_name, qualified_name) == 0) {
                return &reg->types[p];
            }
        }
        /* Tail-scan types added after finalize (see lookup_method_self). */
        for (int i = reg->type_qn_entry_count; i < reg->type_count; i++) {
            if (reg->types[i].qualified_name &&
                strcmp(reg->types[i].qualified_name, qualified_name) == 0) {
                return &reg->types[i];
            }
        }
        return NULL;
    }

    for (int i = 0; i < reg->type_count; i++) {
        if (strcmp(reg->types[i].qualified_name, qualified_name) == 0) {
            return &reg->types[i];
        }
    }
    return NULL;
}

const CBMRegisteredType *cbm_registry_lookup_type(const CBMTypeRegistry *reg,
                                                  const char *qualified_name) {
    if (!reg || !qualified_name)
        return NULL;
    const CBMRegisteredType *r = lookup_type_self(reg, qualified_name);
    if (!r && reg->fallback) {
        return cbm_registry_lookup_type(reg->fallback, qualified_name);
    }
    return r;
}

static const CBMRegisteredFunc *lookup_func_self(const CBMTypeRegistry *reg,
                                                 const char *qualified_name) {
    if (reg->func_qn_buckets && reg->func_qn_bucket_count > 0) {
        uint64_t h = fnv1a(qualified_name);
        int slot = (int)(h & (uint64_t)(reg->func_qn_bucket_count - 1));
        for (int idx = reg->func_qn_buckets[slot]; idx >= 0;
             idx = reg->func_qn_entries[idx].next_index) {
            if (reg->func_qn_entries[idx].hash != h)
                continue;
            int p = reg->func_qn_entries[idx].payload_index;
            if (reg->funcs[p].qualified_name &&
                strcmp(reg->funcs[p].qualified_name, qualified_name) == 0) {
                return &reg->funcs[p];
            }
        }
        /* Tail-scan funcs added after finalize (see lookup_method_self). */
        for (int i = reg->func_qn_entry_count; i < reg->func_count; i++) {
            if (reg->funcs[i].qualified_name &&
                strcmp(reg->funcs[i].qualified_name, qualified_name) == 0) {
                return &reg->funcs[i];
            }
        }
        return NULL;
    }

    for (int i = 0; i < reg->func_count; i++) {
        if (strcmp(reg->funcs[i].qualified_name, qualified_name) == 0) {
            return &reg->funcs[i];
        }
    }
    return NULL;
}

const CBMRegisteredFunc *cbm_registry_lookup_func(const CBMTypeRegistry *reg,
                                                  const char *qualified_name) {
    if (!reg || !qualified_name)
        return NULL;
    const CBMRegisteredFunc *r = lookup_func_self(reg, qualified_name);
    if (!r && reg->fallback) {
        return cbm_registry_lookup_func(reg->fallback, qualified_name);
    }
    return r;
}

const CBMRegisteredType *cbm_registry_resolve_alias(const CBMTypeRegistry *reg,
                                                    const char *type_qn) {
    if (!reg || !type_qn)
        return NULL;
    const CBMRegisteredType *rt = cbm_registry_lookup_type(reg, type_qn);
    for (int i = 0; i < 16 && rt && rt->alias_of; i++) {
        const CBMRegisteredType *next = cbm_registry_lookup_type(reg, rt->alias_of);
        if (!next)
            return rt;
        rt = next;
    }
    return rt;
}

const CBMRegisteredFunc *cbm_registry_lookup_method_aliased(const CBMTypeRegistry *reg,
                                                            const char *receiver_qn,
                                                            const char *method_name) {
    if (!reg || !receiver_qn || !method_name)
        return NULL;

    // Direct lookup first
    const CBMRegisteredFunc *f = cbm_registry_lookup_method(reg, receiver_qn, method_name);
    if (f)
        return f;

    // Follow alias chain
    const CBMRegisteredType *rt = cbm_registry_lookup_type(reg, receiver_qn);
    for (int i = 0; i < 16 && rt && rt->alias_of; i++) {
        f = cbm_registry_lookup_method(reg, rt->alias_of, method_name);
        if (f)
            return f;
        rt = cbm_registry_lookup_type(reg, rt->alias_of);
    }
    return NULL;
}

const CBMRegisteredFunc *cbm_registry_lookup_symbol(const CBMTypeRegistry *reg,
                                                    const char *package_qn, const char *name) {
    if (!reg || !package_qn || !name)
        return NULL;

    // Build expected QN: package_qn.name
    size_t pkg_len = strlen(package_qn);
    size_t name_len = strlen(name);
    size_t total_len = pkg_len + 1 + name_len;

    char buf[512];
    if (total_len >= sizeof(buf))
        return NULL;

    memcpy(buf, package_qn, pkg_len);
    buf[pkg_len] = '.';
    memcpy(buf + pkg_len + 1, name, name_len);
    buf[total_len] = '\0';

    return cbm_registry_lookup_func(reg, buf);
}

// Count parameters in a FUNC signature.
static int count_func_params(const CBMRegisteredFunc *f) {
    if (!f || !f->signature || f->signature->kind != CBM_TYPE_FUNC)
        return -1;
    if (!f->signature->data.func.param_types)
        return 0;
    int count = 0;
    while (f->signature->data.func.param_types[count])
        count++;
    return count;
}

const CBMRegisteredFunc *cbm_registry_lookup_method_by_args(const CBMTypeRegistry *reg,
                                                            const char *receiver_qn,
                                                            const char *method_name,
                                                            int arg_count) {
    if (!reg || !receiver_qn || !method_name)
        return NULL;

    const CBMRegisteredFunc *first_match = NULL;
    const CBMRegisteredFunc *range_match = NULL;

    // Hashed path when finalized: walk only the (receiver,name) overload chain
    // instead of every registered func. The chain holds exactly the funcs whose
    // (receiver_type, short_name) hash to this key, so results are identical to
    // the linear scan below — just O(overloads) instead of O(func_count). This
    // matters on project-wide registries: a header with thousands of method
    // calls × tens of thousands of registered funcs was O(calls×funcs) (#410).
    if (reg->method_buckets && reg->method_bucket_count > 0) {
        // The chain is in descending registration order, but the linear scan
        // resolves ties by LOWEST registration index (first exact match, else
        // first range match, else first match). Track the lowest index in each
        // category so the hashed path returns the identical func.
        int exact_pi = -1, range_pi = -1, first_pi = -1;
        uint64_t h = fnv1a_pair(receiver_qn, method_name);
        int slot = (int)(h & (uint64_t)(reg->method_bucket_count - 1));
        for (int idx = reg->method_buckets[slot]; idx >= 0;
             idx = reg->method_entries[idx].next_index) {
            if (reg->method_entries[idx].hash != h)
                continue;
            int pi = reg->method_entries[idx].payload_index;
            const CBMRegisteredFunc *f = &reg->funcs[pi];
            if (!f->receiver_type || !f->short_name || strcmp(f->receiver_type, receiver_qn) != 0 ||
                strcmp(f->short_name, method_name) != 0)
                continue;
            if (first_pi < 0 || pi < first_pi)
                first_pi = pi;
            int pc = count_func_params(f);
            if (pc == arg_count) {
                if (exact_pi < 0 || pi < exact_pi)
                    exact_pi = pi;
                continue;
            }
            int min_pc = (f->min_params >= 0) ? f->min_params : pc;
            if (arg_count >= min_pc && arg_count <= pc) {
                if (range_pi < 0 || pi < range_pi)
                    range_pi = pi;
            }
        }
        int sel = exact_pi >= 0 ? exact_pi : (range_pi >= 0 ? range_pi : first_pi);
        if (sel >= 0)
            return &reg->funcs[sel];
        if (reg->fallback) {
            return cbm_registry_lookup_method_by_args(reg->fallback, receiver_qn, method_name,
                                                      arg_count);
        }
        return NULL;
    }

    for (int i = 0; i < reg->func_count; i++) {
        const CBMRegisteredFunc *f = &reg->funcs[i];
        if (f->receiver_type && f->short_name && strcmp(f->receiver_type, receiver_qn) == 0 &&
            strcmp(f->short_name, method_name) == 0) {
            if (!first_match)
                first_match = f;
            int pc = count_func_params(f);
            if (pc == arg_count)
                return f; // exact match
            // Accept if arg_count is in [min_params, pc] (default args)
            int min_pc = (f->min_params >= 0) ? f->min_params : pc;
            if (!range_match && arg_count >= min_pc && arg_count <= pc) {
                range_match = f;
            }
        }
    }
    const CBMRegisteredFunc *res = range_match ? range_match : first_match;
    if (!res && reg->fallback) {
        return cbm_registry_lookup_method_by_args(reg->fallback, receiver_qn, method_name,
                                                  arg_count);
    }
    return res;
}

// --- Overload scoring by parameter type ---

// Unwrap pointer/reference to get the core QN for comparison.
static const char *type_to_qn_simple(const CBMType *t) {
    if (!t)
        return NULL;
    // Unwrap references and pointers
    while (t) {
        switch (t->kind) {
        case CBM_TYPE_POINTER:
            t = t->data.pointer.elem;
            continue;
        case CBM_TYPE_REFERENCE:
            t = t->data.reference.elem;
            continue;
        case CBM_TYPE_RVALUE_REF:
            t = t->data.reference.elem;
            continue;
        case CBM_TYPE_NAMED:
            return t->data.named.qualified_name;
        case CBM_TYPE_TEMPLATE:
            return t->data.template_type.template_name;
        case CBM_TYPE_BUILTIN:
            return t->data.builtin.name;
        default:
            return NULL;
        }
    }
    return NULL;
}

// Check if two types are compatible via implicit conversion.
static bool c_types_compatible(const char *expected_qn, const char *actual_qn) {
    if (!expected_qn || !actual_qn)
        return false;
    // const char* / char* -> std::string, std::string_view
    if (strcmp(actual_qn, "char") == 0) {
        if (strcmp(expected_qn, "std.string") == 0 ||
            strcmp(expected_qn, "std.basic_string") == 0 ||
            strcmp(expected_qn, "std.string_view") == 0 ||
            strcmp(expected_qn, "std.basic_string_view") == 0 ||
            strcmp(expected_qn, "absl.string_view") == 0)
            return true;
    }
    // Numeric promotions: all numeric builtins are interconvertible
    static const char *numerics[] = {"int",       "long",    "short",    "float",    "double",
                                     "unsigned",  "size_t",  "int8_t",   "int16_t",  "int32_t",
                                     "int64_t",   "uint8_t", "uint16_t", "uint32_t", "uint64_t",
                                     "ptrdiff_t", NULL};
    bool exp_numeric = false, act_numeric = false;
    for (int i = 0; numerics[i]; i++) {
        if (strcmp(expected_qn, numerics[i]) == 0)
            exp_numeric = true;
        if (strcmp(actual_qn, numerics[i]) == 0)
            act_numeric = true;
    }
    if (exp_numeric && act_numeric)
        return true;
    // bool <-> int
    if ((strcmp(expected_qn, "bool") == 0 && act_numeric) ||
        (exp_numeric && strcmp(actual_qn, "bool") == 0))
        return true;
    return false;
}

// Score an overload match: higher = better. 0 = wrong arg count (no match).
static int score_overload_match(const CBMRegisteredFunc *f, const CBMType **arg_types,
                                int arg_count) {
    int pc = count_func_params(f);
    int min_pc = (f->min_params >= 0) ? f->min_params : pc;
    if (arg_count < min_pc || arg_count > pc)
        return 0; // out of range
    if (!arg_types || !f->signature || !f->signature->data.func.param_types)
        return 50;
    int score = 50;
    for (int i = 0; i < arg_count; i++) {
        const CBMType *expected = f->signature->data.func.param_types[i];
        const CBMType *actual = arg_types[i];
        if (!expected || !actual || cbm_type_is_unknown(actual))
            continue; // neutral
        const char *exp_qn = type_to_qn_simple(expected);
        const char *act_qn = type_to_qn_simple(actual);
        if (!exp_qn || !act_qn)
            continue;
        if (strcmp(exp_qn, act_qn) == 0) {
            score += 10; // exact type match
        } else if (c_types_compatible(exp_qn, act_qn)) {
            score += 5; // implicit conversion
        }
    }
    return score;
}

const CBMRegisteredFunc *cbm_registry_lookup_method_by_types(const CBMTypeRegistry *reg,
                                                             const char *receiver_qn,
                                                             const char *method_name,
                                                             const CBMType **arg_types,
                                                             int arg_count) {
    if (!reg || !receiver_qn || !method_name)
        return NULL;
    // If no type info, fall back to arg-count matching
    if (!arg_types)
        return cbm_registry_lookup_method_by_args(reg, receiver_qn, method_name, arg_count);

    const CBMRegisteredFunc *best = NULL;
    int best_score = 0;
    const CBMRegisteredFunc *first_match = NULL;

    // Hashed path when finalized: score only the (receiver,name) overload chain
    // instead of every registered func. Identical result set to the linear scan
    // below (same membership test), but O(overloads) instead of O(func_count) —
    // the hot per-call lookup that made large-header resolve O(calls×funcs)
    // (#410: a 10989-def header took ~34s/file in the resolve phase).
    if (reg->method_buckets && reg->method_bucket_count > 0) {
        // The chain is in descending registration order. The linear scan keeps
        // the first (lowest-index) func at the best score (strict '>'), and
        // falls back to the lowest-index match. Track lowest index per category
        // so the hashed path returns the identical func.
        int best_pi = -1, first_pi = -1;
        uint64_t h = fnv1a_pair(receiver_qn, method_name);
        int slot = (int)(h & (uint64_t)(reg->method_bucket_count - 1));
        for (int idx = reg->method_buckets[slot]; idx >= 0;
             idx = reg->method_entries[idx].next_index) {
            if (reg->method_entries[idx].hash != h)
                continue;
            int pi = reg->method_entries[idx].payload_index;
            const CBMRegisteredFunc *f = &reg->funcs[pi];
            if (!f->receiver_type || !f->short_name || strcmp(f->receiver_type, receiver_qn) != 0 ||
                strcmp(f->short_name, method_name) != 0)
                continue;
            if (first_pi < 0 || pi < first_pi)
                first_pi = pi;
            int s = score_overload_match(f, arg_types, arg_count);
            if (s > best_score || (s == best_score && best_pi >= 0 && pi < best_pi)) {
                best_score = s;
                best_pi = pi;
            }
        }
        // best_score starts at 0; a score of 0 means "wrong arg count" → no
        // match, exactly as the linear scan (which never sets best on s==0).
        int sel = (best_pi >= 0 && best_score > 0) ? best_pi : first_pi;
        if (sel >= 0)
            return &reg->funcs[sel];
        if (reg->fallback) {
            return cbm_registry_lookup_method_by_types(reg->fallback, receiver_qn, method_name,
                                                       arg_types, arg_count);
        }
        return NULL;
    }

    for (int i = 0; i < reg->func_count; i++) {
        const CBMRegisteredFunc *f = &reg->funcs[i];
        if (f->receiver_type && f->short_name && strcmp(f->receiver_type, receiver_qn) == 0 &&
            strcmp(f->short_name, method_name) == 0) {
            if (!first_match)
                first_match = f;
            int s = score_overload_match(f, arg_types, arg_count);
            if (s > best_score) {
                best_score = s;
                best = f;
            }
        }
    }
    const CBMRegisteredFunc *res = best ? best : first_match;
    if (!res && reg->fallback) {
        return cbm_registry_lookup_method_by_types(reg->fallback, receiver_qn, method_name,
                                                   arg_types, arg_count);
    }
    return res;
}

const CBMRegisteredFunc *cbm_registry_lookup_symbol_by_types(const CBMTypeRegistry *reg,
                                                             const char *package_qn,
                                                             const char *name,
                                                             const CBMType **arg_types,
                                                             int arg_count) {
    if (!reg || !package_qn || !name)
        return NULL;
    if (!arg_types)
        return cbm_registry_lookup_symbol_by_args(reg, package_qn, name, arg_count);

    size_t pkg_len = strlen(package_qn);
    size_t name_len = strlen(name);
    size_t total_len = pkg_len + 1 + name_len;
    char buf[512];
    if (total_len >= sizeof(buf))
        return NULL;
    memcpy(buf, package_qn, pkg_len);
    buf[pkg_len] = '.';
    memcpy(buf + pkg_len + 1, name, name_len);
    buf[total_len] = '\0';

    const CBMRegisteredFunc *best = NULL;
    int best_score = 0;
    const CBMRegisteredFunc *first_match = NULL;

    // Hashed path when finalized: walk only the QN overload chain. The func QN
    // index chains every func sharing a qualified_name, so this scores the exact
    // same overload set as the linear scan, in O(overloads) not O(func_count).
    if (reg->func_qn_buckets && reg->func_qn_bucket_count > 0) {
        int best_pi = -1, first_pi = -1;
        uint64_t h = fnv1a(buf);
        int slot = (int)(h & (uint64_t)(reg->func_qn_bucket_count - 1));
        for (int idx = reg->func_qn_buckets[slot]; idx >= 0;
             idx = reg->func_qn_entries[idx].next_index) {
            if (reg->func_qn_entries[idx].hash != h)
                continue;
            int pi = reg->func_qn_entries[idx].payload_index;
            const CBMRegisteredFunc *f = &reg->funcs[pi];
            if (!f->qualified_name || strcmp(f->qualified_name, buf) != 0)
                continue;
            if (first_pi < 0 || pi < first_pi)
                first_pi = pi;
            int s = score_overload_match(f, arg_types, arg_count);
            if (s > best_score || (s == best_score && best_pi >= 0 && pi < best_pi)) {
                best_score = s;
                best_pi = pi;
            }
        }
        int sel = (best_pi >= 0 && best_score > 0) ? best_pi : first_pi;
        if (sel >= 0)
            return &reg->funcs[sel];
        if (reg->fallback) {
            return cbm_registry_lookup_symbol_by_types(reg->fallback, package_qn, name, arg_types,
                                                       arg_count);
        }
        return NULL;
    }

    for (int i = 0; i < reg->func_count; i++) {
        const CBMRegisteredFunc *f = &reg->funcs[i];
        if (strcmp(f->qualified_name, buf) == 0) {
            if (!first_match)
                first_match = f;
            int s = score_overload_match(f, arg_types, arg_count);
            if (s > best_score) {
                best_score = s;
                best = f;
            }
        }
    }
    const CBMRegisteredFunc *res = best ? best : first_match;
    if (!res && reg->fallback) {
        return cbm_registry_lookup_symbol_by_types(reg->fallback, package_qn, name, arg_types,
                                                   arg_count);
    }
    return res;
}

const CBMRegisteredFunc *cbm_registry_lookup_symbol_by_args(const CBMTypeRegistry *reg,
                                                            const char *package_qn,
                                                            const char *name, int arg_count) {
    if (!reg || !package_qn || !name)
        return NULL;

    size_t pkg_len = strlen(package_qn);
    size_t name_len = strlen(name);
    size_t total_len = pkg_len + 1 + name_len;
    char buf[512];
    if (total_len >= sizeof(buf))
        return NULL;
    memcpy(buf, package_qn, pkg_len);
    buf[pkg_len] = '.';
    memcpy(buf + pkg_len + 1, name, name_len);
    buf[total_len] = '\0';

    const CBMRegisteredFunc *first_match = NULL;
    const CBMRegisteredFunc *range_match = NULL;

    // Hashed path when finalized: walk only the QN overload chain (see
    // cbm_registry_lookup_symbol_by_types). O(overloads) not O(func_count).
    if (reg->func_qn_buckets && reg->func_qn_bucket_count > 0) {
        int exact_pi = -1, range_pi = -1, first_pi = -1;
        uint64_t h = fnv1a(buf);
        int slot = (int)(h & (uint64_t)(reg->func_qn_bucket_count - 1));
        for (int idx = reg->func_qn_buckets[slot]; idx >= 0;
             idx = reg->func_qn_entries[idx].next_index) {
            if (reg->func_qn_entries[idx].hash != h)
                continue;
            int pi = reg->func_qn_entries[idx].payload_index;
            const CBMRegisteredFunc *f = &reg->funcs[pi];
            if (!f->qualified_name || strcmp(f->qualified_name, buf) != 0)
                continue;
            if (first_pi < 0 || pi < first_pi)
                first_pi = pi;
            int pc = count_func_params(f);
            if (pc == arg_count) {
                if (exact_pi < 0 || pi < exact_pi)
                    exact_pi = pi;
                continue;
            }
            int min_pc = (f->min_params >= 0) ? f->min_params : pc;
            if (arg_count >= min_pc && arg_count <= pc) {
                if (range_pi < 0 || pi < range_pi)
                    range_pi = pi;
            }
        }
        int sel = exact_pi >= 0 ? exact_pi : (range_pi >= 0 ? range_pi : first_pi);
        if (sel >= 0)
            return &reg->funcs[sel];
        if (reg->fallback) {
            return cbm_registry_lookup_symbol_by_args(reg->fallback, package_qn, name, arg_count);
        }
        return NULL;
    }

    for (int i = 0; i < reg->func_count; i++) {
        const CBMRegisteredFunc *f = &reg->funcs[i];
        if (strcmp(f->qualified_name, buf) == 0) {
            if (!first_match)
                first_match = f;
            int pc = count_func_params(f);
            if (pc == arg_count)
                return f;
            int min_pc = (f->min_params >= 0) ? f->min_params : pc;
            if (!range_match && arg_count >= min_pc && arg_count <= pc) {
                range_match = f;
            }
        }
    }
    const CBMRegisteredFunc *res = range_match ? range_match : first_match;
    if (!res && reg->fallback) {
        return cbm_registry_lookup_symbol_by_args(reg->fallback, package_qn, name, arg_count);
    }
    return res;
}

// --- TS-specific helpers ---

const CBMRegisteredFunc *cbm_registry_lookup_callable(const CBMTypeRegistry *reg, CBMArena *arena,
                                                      const char *type_qn) {
    if (!reg || !type_qn || !arena)
        return NULL;

    const CBMRegisteredType *rt = cbm_registry_lookup_type(reg, type_qn);
    if (!rt || !rt->call_signature)
        return NULL;

    // Synthesise a CBMRegisteredFunc whose QN is "<type_qn>.__call".
    // Allocated in the caller's arena so it lives at least as long as the registry context.
    size_t qn_len = strlen(type_qn);
    const char suffix[] = ".__call";
    size_t total = qn_len + sizeof(suffix); // sizeof includes the NUL
    char *qn = (char *)cbm_arena_alloc(arena, total);
    if (!qn)
        return NULL;
    memcpy(qn, type_qn, qn_len);
    memcpy(qn + qn_len, suffix, sizeof(suffix));

    CBMRegisteredFunc *f = (CBMRegisteredFunc *)cbm_arena_alloc(arena, sizeof(CBMRegisteredFunc));
    if (!f)
        return NULL;
    memset(f, 0, sizeof(*f));
    f->qualified_name = qn;
    f->short_name = "__call";
    f->receiver_type = rt->qualified_name;
    f->signature = rt->call_signature;
    f->min_params = -1;
    return f;
}

const CBMType *cbm_registry_lookup_index_signature(const CBMTypeRegistry *reg, const char *type_qn,
                                                   const CBMType *key_type) {
    if (!reg || !type_qn)
        return NULL;

    const CBMRegisteredType *rt = cbm_registry_lookup_type(reg, type_qn);
    if (!rt || !rt->index_value_type)
        return NULL;

    // If no expected key type is recorded, accept any index. Otherwise require a match
    // on BUILTIN tag — TS allows string and number index signatures and one of each.
    if (!rt->index_key_type || !key_type)
        return rt->index_value_type;

    if (rt->index_key_type->kind == CBM_TYPE_BUILTIN && key_type->kind == CBM_TYPE_BUILTIN) {
        const char *a = rt->index_key_type->data.builtin.name;
        const char *b = key_type->data.builtin.name;
        if (a && b && strcmp(a, b) == 0)
            return rt->index_value_type;
        // String index also accepts numeric keys (number → toString).
        if (a && strcmp(a, "string") == 0)
            return rt->index_value_type;
    }
    return NULL;
}
