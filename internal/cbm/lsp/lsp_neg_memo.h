/*
 * lsp_neg_memo.h — generic negative-lookup memo for LSP resolve cascades.
 *
 * Problem (all hybrid-LSP languages): a resolve cascade probes a ladder of
 * hypotheses (direct hit, module-prefixed retry, alias walk, trait/base
 * dispatch, short-name fallback) and most probes MISS — macro-expanded or
 * generated code asks the SAME failing question thousands of times per file,
 * re-paying the whole ladder each time (linux kernel: 4 trait-heavy rust
 * files at ~63 s each; the C resolver had the same disease before its memo).
 *
 * A miss is a pure fact of (registry, query) — but ONLY while the registry
 * cannot change. Callers must therefore gate the memo on a SEALED registry
 * (reg->read_only, set at finalize; cbm_registry_add_* hard-return then) and
 * must key only queries whose cascade reads nothing but the registry and the
 * query strings (no per-function scope state in the memoized rungs).
 *
 * Shape: open-addressing set of 64-bit keys, arena-backed (dies with the
 * per-file arena — no explicit free, mirroring the py field overlay). A memo
 * HIT means "this exact query already ran the full cascade and returned
 * nothing" → the caller returns its miss result immediately. To make a hash
 * collision harmless, callers keep their cheap DIRECT lookup before the memo
 * check (the C-memo pattern): a colliding real hit is still found; only the
 * expensive registry-pure miss ladder is skipped.
 *
 * Per-language wiring is a few lines: add a CBMNegMemo to the language ctx,
 * key each cascade entry with cbm_negmemo_key (site tag + query strings),
 * check at entry, insert on the miss return. Wired: rust. Candidates per the
 * 2026-07 resolve audit: php, c# (extension methods), java, kotlin; the C
 * resolver's bespoke memo in c_lsp.c predates this header and can migrate.
 */
#ifndef CBM_LSP_NEG_MEMO_H
#define CBM_LSP_NEG_MEMO_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "arena.h"

typedef struct {
    uint64_t *slots; /* arena-owned; 0 = empty (keys are never 0) */
    int cap;         /* power of two */
    int count;
} CBMNegMemo;

enum {
    CBM_NEGMEMO_INIT_CAP = 1024,
    CBM_NEGMEMO_GROW = 2,
    CBM_NEGMEMO_LOAD_NUM = 7, /* grow at 70% load */
    CBM_NEGMEMO_LOAD_DEN = 10,
};

/* FNV-1a over (site tag, a, 0xff, b). The site tag keeps two cascades with
 * the same argument strings from sharing keys. Never returns 0. */
static inline uint64_t cbm_negmemo_key(uint8_t site, const char *a, const char *b) {
    uint64_t h = 0xcbf29ce484222325ULL;
    h ^= site;
    h *= 0x100000001b3ULL;
    if (a) {
        while (*a) {
            h ^= (unsigned char)*a++;
            h *= 0x100000001b3ULL;
        }
    }
    h ^= 0xff;
    h *= 0x100000001b3ULL;
    if (b) {
        while (*b) {
            h ^= (unsigned char)*b++;
            h *= 0x100000001b3ULL;
        }
    }
    return h ? h : 1;
}

static inline bool cbm_negmemo_contains(const CBMNegMemo *m, uint64_t key) {
    if (!m->slots || m->count == 0) {
        return false;
    }
    uint64_t mask = (uint64_t)(m->cap - 1);
    for (uint64_t i = key & mask;; i = (i + 1) & mask) {
        uint64_t s = m->slots[i];
        if (s == 0) {
            return false;
        }
        if (s == key) {
            return true;
        }
    }
}

static inline void cbm_negmemo_insert_raw(uint64_t *slots, int cap, uint64_t key) {
    uint64_t mask = (uint64_t)(cap - 1);
    for (uint64_t i = key & mask;; i = (i + 1) & mask) {
        if (slots[i] == key) {
            return;
        }
        if (slots[i] == 0) {
            slots[i] = key;
            return;
        }
    }
}

/* Arena-backed insert: lazy first allocation, grow-by-rehash at 70% load.
 * The abandoned table stays in the arena (bounded, freed with the file). */
static inline void cbm_negmemo_insert(CBMNegMemo *m, CBMArena *arena, uint64_t key) {
    if (!arena) {
        return; /* no arena — memo silently disabled */
    }
    if (!m->slots) {
        m->slots = cbm_arena_alloc(arena, sizeof(uint64_t) * CBM_NEGMEMO_INIT_CAP);
        if (!m->slots) {
            return;
        }
        memset(m->slots, 0, sizeof(uint64_t) * CBM_NEGMEMO_INIT_CAP);
        m->cap = CBM_NEGMEMO_INIT_CAP;
        m->count = 0;
    }
    if ((int64_t)(m->count + 1) * CBM_NEGMEMO_LOAD_DEN >=
        (int64_t)m->cap * CBM_NEGMEMO_LOAD_NUM) {
        int new_cap = m->cap * CBM_NEGMEMO_GROW;
        uint64_t *ns = cbm_arena_alloc(arena, sizeof(uint64_t) * (size_t)new_cap);
        if (!ns) {
            return; /* keep the old (full-ish) table; inserts degrade, reads stay correct */
        }
        memset(ns, 0, sizeof(uint64_t) * (size_t)new_cap);
        for (int i = 0; i < m->cap; i++) {
            if (m->slots[i]) {
                cbm_negmemo_insert_raw(ns, new_cap, m->slots[i]);
            }
        }
        m->slots = ns;
        m->cap = new_cap;
    }
    if (!cbm_negmemo_contains(m, key)) {
        cbm_negmemo_insert_raw(m->slots, m->cap, key);
        m->count++;
    }
}

/* ── CBMIdxMemo: exact-match string-key → int index map ─────────────────────
 * Companion for build-time registration loops that need "have I registered
 * this QN, and at which index?" in O(1) — the registry's own buckets don't
 * exist before finalize, and probing it linearly from inside the registration
 * loop is the classic quadratic (kernel shared rust registry: ~63 s).
 * Exact match: each slot stores the borrowed key pointer and verifies with
 * strcmp, so hash collisions can't map to a wrong index. First-put wins. */
typedef struct {
    struct cbm_idxmemo_slot {
        uint64_t h; /* 0 = empty */
        const char *key;
        int32_t val;
    } *slots;
    int cap; /* power of two */
    int count;
} CBMIdxMemo;

static inline int32_t cbm_idxmemo_get(const CBMIdxMemo *m, const char *key) {
    if (!m->slots || !key || m->count == 0) {
        return -1;
    }
    uint64_t h = cbm_negmemo_key(0, key, NULL);
    uint64_t mask = (uint64_t)(m->cap - 1);
    for (uint64_t i = h & mask;; i = (i + 1) & mask) {
        if (m->slots[i].h == 0) {
            return -1;
        }
        if (m->slots[i].h == h && m->slots[i].key && strcmp(m->slots[i].key, key) == 0) {
            return m->slots[i].val;
        }
    }
}

static inline void cbm_idxmemo_put_raw(struct cbm_idxmemo_slot *slots, int cap, uint64_t h,
                                       const char *key, int32_t val) {
    uint64_t mask = (uint64_t)(cap - 1);
    for (uint64_t i = h & mask;; i = (i + 1) & mask) {
        if (slots[i].h == 0) {
            slots[i].h = h;
            slots[i].key = key;
            slots[i].val = val;
            return;
        }
        if (slots[i].h == h && slots[i].key && strcmp(slots[i].key, key) == 0) {
            return; /* first-put wins */
        }
    }
}

static inline void cbm_idxmemo_put_if_absent(CBMIdxMemo *m, CBMArena *arena, const char *key,
                                             int32_t val) {
    if (!arena || !key) {
        return;
    }
    if (!m->slots) {
        m->slots = cbm_arena_alloc(arena,
                                   sizeof(struct cbm_idxmemo_slot) * CBM_NEGMEMO_INIT_CAP);
        if (!m->slots) {
            return;
        }
        memset(m->slots, 0, sizeof(struct cbm_idxmemo_slot) * CBM_NEGMEMO_INIT_CAP);
        m->cap = CBM_NEGMEMO_INIT_CAP;
        m->count = 0;
    }
    if ((int64_t)(m->count + 1) * CBM_NEGMEMO_LOAD_DEN >=
        (int64_t)m->cap * CBM_NEGMEMO_LOAD_NUM) {
        int new_cap = m->cap * CBM_NEGMEMO_GROW;
        struct cbm_idxmemo_slot *ns =
            cbm_arena_alloc(arena, sizeof(struct cbm_idxmemo_slot) * (size_t)new_cap);
        if (!ns) {
            return;
        }
        memset(ns, 0, sizeof(struct cbm_idxmemo_slot) * (size_t)new_cap);
        for (int i = 0; i < m->cap; i++) {
            if (m->slots[i].h) {
                cbm_idxmemo_put_raw(ns, new_cap, m->slots[i].h, m->slots[i].key, m->slots[i].val);
            }
        }
        m->slots = ns;
        m->cap = new_cap;
    }
    uint64_t h = cbm_negmemo_key(0, key, NULL);
    if (cbm_idxmemo_get(m, key) < 0) {
        cbm_idxmemo_put_raw(m->slots, m->cap, h, key, val);
        m->count++;
    }
}

#endif /* CBM_LSP_NEG_MEMO_H */
