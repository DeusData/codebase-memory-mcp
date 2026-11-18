/*
 * member_walk.c — the shared member lookup (see member_walk.h).
 *
 * The search uses no C stack depth that grows with the input. A lookup's
 * sub-questions (a mixin's whole order, the rest of a chain past a NEXT link)
 * are nodes, each answered once per walker and kept (mw_slot_t). A node's
 * pass that meets a question not answered yet records it as a need and is
 * run again once its needs are answered; the nodes waiting on each other sit
 * on an explicit stack of frames (mw_solve), which is a path: a need found on
 * it is a cycle.
 */
#include "member_walk.h"
#include "lsp_work.h"
#include "../../../src/foundation/hash_table.h"
#include "../../../src/foundation/mem_core.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    MW_FIRST_CAP = 16,
    MW_SLOTS_FIRST = 64,
    MW_VEC_FIRST = 16,
    MW_GROUP_INDEXED = 8, /* groups with at least this many members get a name index */
    MW_DEPTH_MAX = 255,
};

/* ── step lists ─────────────────────────────────────────────────── */

static CBMMemberStep *mw_step_slot(CBMMemberSteps *s) {
    if (!s || s->oom) {
        return NULL;
    }
    if (s->count == s->cap) {
        int cap = s->cap ? s->cap * 2 : MW_FIRST_CAP;
        CBMMemberStep *grown = (CBMMemberStep *)cbm_realloc(CBM_MEM_CLASS_RESOLVE, s->items,
                                                            (size_t)cap * sizeof(*grown));
        if (!grown) {
            s->oom = true;
            return NULL;
        }
        s->items = grown;
        s->cap = cap;
    }
    CBMMemberStep *st = &s->items[s->count++];
    memset(st, 0, sizeof(*st));
    return st;
}

void cbm_member_steps_push(CBMMemberSteps *s, CBMMemberStepKind kind, const char *type_id, int side,
                           CBMMemberPlace place) {
    CBMMemberStep *st = mw_step_slot(s);
    if (st) {
        st->kind = (uint8_t)kind;
        st->type_id = type_id;
        st->side = side;
        st->place = (uint8_t)place;
    }
}

void cbm_member_steps_group(CBMMemberSteps *s, const char *via, int count, uint8_t flags) {
    CBMMemberStep *st = mw_step_slot(s);
    if (st) {
        st->kind = CBM_MW_GROUP;
        st->via = via;
        st->count = count;
        st->flags = flags;
    }
}

void cbm_member_steps_soft(CBMMemberSteps *s, CBMMemberStepKind kind, int side,
                           CBMMemberPlace place) {
    CBMMemberStep *st = mw_step_slot(s);
    if (st) {
        st->kind = (uint8_t)(kind == CBM_MW_LATER ? CBM_MW_LATER : CBM_MW_UNSEEN);
        st->side = side;
        st->place = (uint8_t)place;
        st->flags = CBM_MW_SOFT;
    }
}

void cbm_member_steps_free(CBMMemberSteps *s) {
    if (s) {
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->items);
        memset(s, 0, sizeof(*s));
    }
}

/* ── orders, cached per type ────────────────────────────────────── */

typedef struct {
    const char *qn; /* the key, in the orders' arena */
    const CBMMemberStep *steps[CBM_MW_SIDES];
    int count[CBM_MW_SIDES];
    uint8_t state[CBM_MW_SIDES]; /* 0 not built, 1 built, 2 cannot be described */
} mw_entry_t;

void cbm_member_orders_init(CBMMemberOrders *o, const CBMTypeRegistry *reg, CBMLineariser lin,
                            CBMArena *arena) {
    if (!o) {
        return;
    }
    memset(o, 0, sizeof(*o));
    o->reg = reg;
    o->lin = lin;
    o->arena = arena;
}

void cbm_member_orders_free(CBMMemberOrders *o) {
    if (!o) {
        return;
    }
    if (o->by_type) {
        cbm_ht_free((CBMHashTable *)o->by_type);
    }
    cbm_member_steps_free(&o->scratch);
    memset(o, 0, sizeof(*o));
}

static mw_entry_t *mw_entry(CBMMemberOrders *o, const char *qn, bool create) {
    if (!o->by_type) {
        if (!create) {
            return NULL;
        }
        o->by_type = cbm_ht_create(0);
        if (!o->by_type) {
            return NULL;
        }
    }
    mw_entry_t *e = (mw_entry_t *)cbm_ht_get((CBMHashTable *)o->by_type, qn);
    if (e || !create || !o->arena) {
        return e;
    }
    e = (mw_entry_t *)cbm_arena_alloc(o->arena, sizeof(*e));
    char *key = cbm_arena_strdup(o->arena, qn);
    if (!e || !key) {
        return NULL;
    }
    memset(e, 0, sizeof(*e));
    e->qn = key;
    cbm_ht_set((CBMHashTable *)o->by_type, key, e);
    return e;
}

/* True when the registry `reg` itself holds `t` (not a registry it falls
 * back to): only those types are ever handed to `prepare`. */
static bool mw_owned(const CBMTypeRegistry *reg, const CBMRegisteredType *t) {
    return reg && t && reg->types && t >= reg->types && t < reg->types + reg->type_count;
}

static bool mw_build(CBMMemberOrders *o, mw_entry_t *e, int side) {
    CBMMemberSteps *s = &o->scratch;
    s->count = 0;
    s->oom = false;
    const CBMRegisteredType *t = o->reg ? cbm_registry_lookup_type(o->reg, e->qn) : NULL;
    bool ok;
    if (!t) {
        /* A type the registry does not hold: its own members answer (a
         * receiver registered only through its methods); its ancestry is a
         * SOFT unknown place, passed, so what follows is only possible. */
        cbm_member_steps_push(s, CBM_MW_OWN, e->qn, side, CBM_MW_PLACE_OWN);
        cbm_member_steps_soft(s, CBM_MW_UNSEEN, side, CBM_MW_PLACE_SUPER);
        cbm_member_steps_push(s, CBM_MW_STOP, NULL, side, CBM_MW_PLACE_OWN);
        ok = true;
    } else {
        if (o->prepare && mw_owned(o->reg, t)) {
            /* The registry's own types only: a type found through its
             * fallback (a shared, sealed registry) is read as it stands and
             * never written (member_walk.h, threading). */
            o->prepare(o->prepare_ctx, (CBMRegisteredType *)t);
        }
        ok = o->lin && o->lin(o, t, side, s);
    }
    if (!ok || s->oom) {
        e->state[side] = 2;
        return false;
    }
    CBMMemberStep *copy = NULL;
    if (s->count > 0) {
        copy = (CBMMemberStep *)cbm_arena_alloc(o->arena, (size_t)s->count * sizeof(*copy));
        if (!copy) {
            e->state[side] = 2;
            return false;
        }
        memcpy(copy, s->items, (size_t)s->count * sizeof(*copy));
    }
    e->steps[side] = copy;
    e->count[side] = s->count;
    e->state[side] = 1;
    o->built++;
    o->steps += (uint64_t)s->count;
    CBM_LSP_WORK(1 + s->count);
    return true;
}

bool cbm_member_orders_get(CBMMemberOrders *o, const char *type_qn, int side,
                           const CBMMemberStep **steps, int *count) {
    if (!o || !type_qn || !steps || !count || side < 0 || side >= CBM_MW_SIDES) {
        return false;
    }
    mw_entry_t *e = mw_entry(o, type_qn, !o->sealed);
    if (!e && o->sealed && o->reg && !cbm_registry_lookup_type(o->reg, type_qn)) {
        /* Sealed, and neither a type nor a receiver of the registry: no own
         * members, its ancestry a SOFT unknown place (as mw_build). */
        static const CBMMemberStep unheld[2] = {
            {.kind = CBM_MW_UNSEEN, .place = CBM_MW_PLACE_SUPER, .flags = CBM_MW_SOFT},
            {.kind = CBM_MW_STOP, .place = CBM_MW_PLACE_OWN},
        };
        *steps = unheld;
        *count = 2;
        return true;
    }
    if (!e) {
        return false;
    }
    if (e->state[side] == 0 && (o->sealed || !mw_build(o, e, side))) {
        return false;
    }
    if (e->state[side] != 1) {
        return false;
    }
    *steps = e->steps[side];
    *count = e->count[side];
    return true;
}

void cbm_member_orders_build_all(CBMMemberOrders *o, int sides) {
    if (!o || !o->reg) {
        return;
    }
    if (sides > CBM_MW_SIDES) {
        sides = CBM_MW_SIDES;
    }
    for (int i = 0; i < o->reg->type_count; i++) {
        const char *qn = o->reg->types[i].qualified_name;
        for (int side = 0; qn && side < sides; side++) {
            const CBMMemberStep *steps = NULL;
            int count = 0;
            (void)cbm_member_orders_get(o, qn, side, &steps, &count);
        }
    }
    /* Receivers registered only through their methods: their own members. */
    for (int i = 0; i < o->reg->func_count; i++) {
        const char *qn = o->reg->funcs[i].receiver_type;
        for (int side = 0; qn && side < sides; side++) {
            const CBMMemberStep *steps = NULL;
            int count = 0;
            (void)cbm_member_orders_get(o, qn, side, &steps, &count);
        }
    }
    o->sealed = true;
}

/* ── the walker's state ─────────────────────────────────────────── */

enum { MW_NONE = 0, MW_HIT = 1, MW_UNKNOWN = 2 }; /* a place's or a node's result */

/* Phase-2 entries in queue order; a chain's list ends in its parent's. */
typedef struct mw_later {
    const char *type; /* NULL: an unknown place */
    const struct mw_later *next;
    int side;
    bool soft;
} mw_later_t;

/* What an order holds for a name, as seen from the order's own type. */
typedef struct {
    uint8_t result; /* MW_NONE: an explicit STOP was reached; MW_HIT; MW_UNKNOWN */
    bool soft;      /* a SOFT place was passed: before the hit, or anywhere (NONE) */
    bool outside;   /* an OUTSIDE place was passed without the member, before the hit */
    bool open;      /* an OPEN step was passed */
    CBMMemberHit hit;
    const mw_later_t *laters; /* CHAIN, NONE: phase 2 still to search */
    const CBMMemberHit *hits; /* collecting modes: every declaration, in order */
    int hit_count;
} mw_ans_t;

static const mw_ans_t MW_ANS_UNKNOWN = {.result = MW_UNKNOWN};

enum { MW_NODE_CHAIN = 1, MW_NODE_WHOLE = 2, MW_NODE_QUERY = 3 };
enum { MW_SLOT_EMPTY = 0, MW_SLOT_QUEUED = 1, MW_SLOT_BUSY = 2, MW_SLOT_DONE = 3 };

/* A node: CHAIN (phase 1 of an order, a NEXT link followed as the parent's
 * CHAIN node) or WHOLE (phase 1, then phase 2 over what it deferred). */
typedef struct {
    uint64_t hash;
    const char *type;
    const char *name;
    const mw_ans_t *ans;
    int side;
    int frame;
    uint8_t node;
    uint8_t state;
} mw_slot_t;

typedef struct {
    const char *type;
    const char *name;
    int side;
    uint8_t node;
} mw_need_t;

typedef struct {
    const char *type;
    const char *name;
    int side;
    uint8_t node;
    bool cyclic;
    int need_lo;
    int need_next;
    int need_hi;
} mw_frame_t;

/* Per-pass visited marks (phase 2 entries, NEXT links walked inline). */
typedef struct {
    uint64_t hash;
    const char *type;
    uint32_t gen;
    int side;
    int pos; /* MW_SEEN_LATER: the entry's queue position */
    uint8_t tag;
    uint8_t color;
} mw_seen_t;

typedef struct {
    const char *name;
    const char *from_type;
    const char *from_name;
    uint8_t kind;
} mw_rename_t;

typedef struct {
    const mw_rename_t *items; /* sorted by name */
    int count;
} mw_renames_t;

/* A group's members by the names they may declare: the leaf members (an
 * OWN step, or a sub-order that is just [OWN of its type, STOP]) under each
 * name their type declares; the other members always asked. */
typedef struct {
    CBMHashTable *by_name; /* name -> mw_ints_t* (member positions, ascending) */
    const int *always;     /* the other members' positions, ascending */
    int always_count;
} mw_gindex_t;

typedef struct {
    int *items;
    int count;
    int cap;
} mw_ints_t;

/* Every group index, so their name tables can be freed. */
typedef struct mw_gindex_node {
    mw_gindex_t *gi;
    struct mw_gindex_node *next;
} mw_gindex_node_t;

typedef struct mw_state {
    CBMArena arena; /* answers, later lists, indexes: live as long as the walker */
    const CBMTypeRegistry *reg;
    uint8_t member_kind;
    uint8_t overloads;
    bool soft_off; /* test seam: SOFT places act as hard ones */
    mw_slot_t *slots;
    int slot_cap;
    int slot_used;
    mw_frame_t *frames;
    int frame_count;
    int frame_cap;
    mw_need_t *needs;
    int need_count;
    int need_cap;
    mw_seen_t *seen;
    int seen_cap;
    int seen_used;
    uint32_t seen_gen;
    mw_later_t *queue; /* phase 2 of one pass */
    int queue_count;
    int queue_cap;
    mw_later_t *later_tmp; /* the LATER steps one chain pass met, in order */
    int later_tmp_count;
    int later_tmp_cap;
    CBMMemberHit *coll; /* collecting modes: one pass's declarations */
    int coll_count;
    int coll_cap;
    int *dfs; /* phase-2 cycle check */
    int dfs_cap;
    CBMHashTable *receivers; /* receiver QN -> mw_names_t* (lazily, for group indexes) */
    CBMHashTable *gindex;    /* group step address (hex) -> mw_gindex_t* */
    mw_gindex_node_t *gindex_list;
    CBMHashTable *renames; /* holder QN -> mw_renames_t* */
} mw_state_t;

typedef struct {
    const char **items;
    int count;
    int cap;
} mw_names_t;

static bool mw_same(const char *a, const char *b) {
    return a == b || (a && b && strcmp(a, b) == 0);
}

static uint64_t mw_fnv(uint64_t h, const char *s) {
    for (; s && *s; s++) {
        h ^= (uint8_t)*s;
        h *= 1099511628211ULL;
    }
    return h;
}

static uint64_t mw_key(uint8_t node, const char *type, int side, const char *name) {
    uint64_t h = mw_fnv(1469598103934665603ULL ^ node, type);
    h = mw_fnv(h ^ 0x9e3779b97f4a7c15ULL, name);
    return h ^ ((uint64_t)(uint32_t)side * 0x100000001b3ULL);
}

/* Grow a vector of `size`-byte items to hold one more. */
static bool mw_vec_room(void **items, int *cap, int count, size_t size) {
    if (count < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * 2 : MW_VEC_FIRST;
    void *grown = cbm_realloc(CBM_MEM_CLASS_RESOLVE, *items, (size_t)ncap * size);
    if (!grown) {
        return false;
    }
    *items = grown;
    *cap = ncap;
    return true;
}

/* ── answers kept per walker ────────────────────────────────────── */

static mw_slot_t *mw_slot_find(mw_state_t *s, uint8_t node, const char *type, int side,
                               const char *name, bool insert) {
    if (!s->slots) {
        return NULL;
    }
    uint64_t h = mw_key(node, type, side, name);
    uint32_t mask = (uint32_t)s->slot_cap - 1;
    for (uint32_t i = (uint32_t)h & mask;; i = (i + 1) & mask) {
        mw_slot_t *sl = &s->slots[i];
        if (!sl->node) {
            if (!insert) {
                return NULL;
            }
            memset(sl, 0, sizeof(*sl));
            sl->hash = h;
            sl->node = node;
            sl->type = type;
            sl->side = side;
            sl->name = name;
            s->slot_used++;
            return sl;
        }
        if (sl->hash == h && sl->node == node && sl->side == side && mw_same(sl->type, type) &&
            mw_same(sl->name, name)) {
            return sl;
        }
    }
}

static bool mw_slots_grow(mw_state_t *s) {
    int cap = s->slot_cap ? s->slot_cap * 2 : MW_SLOTS_FIRST;
    mw_slot_t *tab = (mw_slot_t *)cbm_calloc(CBM_MEM_CLASS_RESOLVE, (size_t)cap * sizeof(*tab));
    if (!tab) {
        return false;
    }
    mw_slot_t *old = s->slots;
    int old_cap = s->slot_cap;
    s->slots = tab;
    s->slot_cap = cap;
    uint32_t mask = (uint32_t)cap - 1;
    for (int i = 0; old && i < old_cap; i++) {
        if (!old[i].node) {
            continue;
        }
        for (uint32_t j = (uint32_t)old[i].hash & mask;; j = (j + 1) & mask) {
            if (!tab[j].node) {
                tab[j] = old[i];
                break;
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_RESOLVE, old);
    return true;
}

/* The slot of a node, added when missing. NULL only when out of memory. */
static mw_slot_t *mw_slot_put(mw_state_t *s, uint8_t node, const char *type, int side,
                              const char *name) {
    if ((s->slot_used + 1) * 2 > s->slot_cap && !mw_slots_grow(s)) {
        return NULL;
    }
    return mw_slot_find(s, node, type, side, name, true);
}

static void mw_slots_clear(mw_state_t *s) {
    if (s->slots) {
        memset(s->slots, 0, (size_t)s->slot_cap * sizeof(*s->slots));
    }
    s->slot_used = 0;
}

/* ── per-pass visited marks ─────────────────────────────────────── */

enum { MW_SEEN_LATER = 1, MW_SEEN_LINK = 2 };

static void mw_seen_begin(mw_state_t *s) {
    s->seen_gen++;
    if (s->seen_gen == 0) {
        if (s->seen) {
            memset(s->seen, 0, (size_t)s->seen_cap * sizeof(*s->seen));
        }
        s->seen_gen = 1;
    }
    s->seen_used = 0;
}

static mw_seen_t *mw_seen_find(mw_state_t *s, const char *type, int side, uint8_t tag) {
    if (!s->seen) {
        return NULL;
    }
    uint64_t h = mw_fnv(1469598103934665603ULL ^ tag, type) ^ (uint64_t)(uint32_t)side;
    uint32_t mask = (uint32_t)s->seen_cap - 1;
    for (uint32_t i = (uint32_t)h & mask;; i = (i + 1) & mask) {
        mw_seen_t *e = &s->seen[i];
        if (e->gen != s->seen_gen) {
            return e;
        }
        if (e->hash == h && e->tag == tag && e->side == side && mw_same(e->type, type)) {
            return e;
        }
    }
}

static bool mw_seen_grow(mw_state_t *s) {
    int cap = s->seen_cap ? s->seen_cap * 2 : MW_SLOTS_FIRST;
    mw_seen_t *tab = (mw_seen_t *)cbm_calloc(CBM_MEM_CLASS_RESOLVE, (size_t)cap * sizeof(*tab));
    if (!tab) {
        return false;
    }
    mw_seen_t *old = s->seen;
    int old_cap = s->seen_cap;
    s->seen = tab;
    s->seen_cap = cap;
    uint32_t mask = (uint32_t)cap - 1;
    for (int i = 0; old && i < old_cap; i++) {
        if (old[i].gen != s->seen_gen) {
            continue;
        }
        for (uint32_t j = (uint32_t)old[i].hash & mask;; j = (j + 1) & mask) {
            if (tab[j].gen != s->seen_gen) {
                tab[j] = old[i];
                break;
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_RESOLVE, old);
    return true;
}

/* Mark (type, side, tag) visited in this pass. 1 = newly marked, 0 = seen
 * before, -1 = out of memory. */
static int mw_seen_add(mw_state_t *s, const char *type, int side, uint8_t tag) {
    if ((s->seen_used + 1) * 2 > s->seen_cap && !mw_seen_grow(s)) {
        return -1;
    }
    mw_seen_t *e = mw_seen_find(s, type, side, tag);
    if (!e) {
        return -1;
    }
    if (e->gen == s->seen_gen) {
        return 0;
    }
    e->gen = s->seen_gen;
    e->hash = mw_fnv(1469598103934665603ULL ^ tag, type) ^ (uint64_t)(uint32_t)side;
    e->type = type;
    e->side = side;
    e->tag = tag;
    e->color = 0;
    e->pos = -1;
    s->seen_used++;
    return 1;
}

/* ── the walk of one pass ───────────────────────────────────────── */

typedef struct {
    CBMMemberWalker *w;
    mw_state_t *s;
    const CBMMemberQuery *q;
    bool collect; /* COLLECT / HIDE_BY_SIGNATURE */
    bool blocked; /* a node this pass needs is not answered yet */
    bool failed;  /* out of memory: the answer is unknown */
} mw_walk_t;

static void mw_cycle(mw_state_t *s, int from) {
    for (int i = from; i >= 0 && i < s->frame_count; i++) {
        s->frames[i].cyclic = true;
    }
}

/* The answer of a node, or NULL when it is still to be answered (recorded as
 * a need of the running pass; k->blocked set). A node on the frame stack is
 * a cycle: the frames from it up are marked and the answer is unknown. */
static const mw_ans_t *mw_dep(mw_walk_t *k, uint8_t node, const char *type, int side,
                              const char *name) {
    mw_state_t *s = k->s;
    mw_slot_t *sl = mw_slot_put(s, node, type, side, name);
    if (!sl) {
        k->failed = true;
        return &MW_ANS_UNKNOWN;
    }
    if (sl->state == MW_SLOT_DONE) {
        return sl->ans;
    }
    if (sl->state == MW_SLOT_BUSY) {
        mw_cycle(s, sl->frame);
        return &MW_ANS_UNKNOWN;
    }
    k->blocked = true;
    if (sl->state == MW_SLOT_QUEUED) {
        return NULL; /* already a need of this pass */
    }
    if (!mw_vec_room((void **)&s->needs, &s->need_cap, s->need_count, sizeof(*s->needs))) {
        k->failed = true;
        return &MW_ANS_UNKNOWN;
    }
    sl->state = MW_SLOT_QUEUED;
    mw_need_t *nd = &s->needs[s->need_count++];
    nd->type = type;
    nd->name = name;
    nd->side = side;
    nd->node = node;
    return NULL;
}

static bool mw_soft(const mw_walk_t *k, uint8_t flags) {
    return (flags & CBM_MW_SOFT) != 0 && !k->s->soft_off;
}

static bool mw_order(mw_walk_t *k, const char *type, int side, const CBMMemberStep **steps,
                     int *count) {
    return k->w->orders && cbm_member_orders_get(k->w->orders, type, side, steps, count);
}

static void mw_fill(CBMMemberHit *h, const CBMRegisteredFunc *f, const CBMType *field,
                    const char *owner, const char *via, const char *name, uint8_t place,
                    bool complete) {
    memset(h, 0, sizeof(*h));
    h->func = f;
    h->field_type = field;
    h->owner_qn = owner;
    h->via_qn = via ? via : owner;
    h->member_name = name;
    h->place = place;
    h->prefix_complete = complete;
}

static bool mw_sig_equal(const CBMRegisteredFunc *a, const CBMRegisteredFunc *b) {
    const CBMType *sa = a->signature;
    const CBMType *sb = b->signature;
    if (!sa || !sb || sa->kind != CBM_TYPE_FUNC || sb->kind != CBM_TYPE_FUNC) {
        return false; /* unknown signatures never hide each other */
    }
    const CBMType **pa = sa->data.func.param_types;
    const CBMType **pb = sb->data.func.param_types;
    int i = 0;
    for (; pa && pb && pa[i] && pb[i]; i++) {
        if (!cbm_type_equal(pa[i], pb[i])) {
            return false;
        }
    }
    bool end_a = !pa || !pa[i];
    bool end_b = !pb || !pb[i];
    return end_a && end_b;
}

/* COLLECT modes: add h unless the same definition (or, hiding by signature,
 * one with an equal signature) is already collected in this pass. */
static bool mw_collect(mw_walk_t *k, const CBMMemberHit *h) {
    mw_state_t *s = k->s;
    for (int i = 0; i < s->coll_count; i++) {
        const CBMRegisteredFunc *g = s->coll[i].func;
        const CBMRegisteredFunc *f = h->func;
        k->w->work++;
        CBM_LSP_WORK(1);
        if (g == f || (g && f && mw_same(g->qualified_name, f->qualified_name))) {
            return true;
        }
        if (g && f && k->q->overloads == CBM_MW_HIDE_BY_SIGNATURE && mw_sig_equal(g, f)) {
            return true;
        }
    }
    if (!mw_vec_room((void **)&s->coll, &s->coll_cap, s->coll_count, sizeof(*s->coll))) {
        return false;
    }
    s->coll[s->coll_count++] = *h;
    return true;
}

/* The members `name` that `type` itself declares. */
static uint8_t mw_own(mw_walk_t *k, const char *type, const char *name, const char *via,
                      uint8_t place, bool complete, CBMMemberHit *out) {
    const CBMMemberQuery *q = k->q;
    uint8_t kind = q->member_kind ? q->member_kind : CBM_MW_MEMBER_METHOD;
    if (kind & CBM_MW_MEMBER_METHOD) {
        CBMMethodIter it;
        cbm_registry_methods_chain(q->reg, type, name, &it);
        if (k->collect) {
            bool any = false;
            for (int idx = cbm_method_iter_next(&it); idx >= 0; idx = cbm_method_iter_next(&it)) {
                k->w->work++;
                CBM_LSP_WORK(1);
                CBMMemberHit h;
                const CBMRegisteredFunc *f = &it.reg->funcs[idx];
                mw_fill(&h, f, NULL, type, via, f->short_name, place, complete);
                if (!mw_collect(k, &h)) {
                    k->failed = true;
                    return MW_UNKNOWN;
                }
                any = true;
            }
            if (any || kind == CBM_MW_MEMBER_METHOD) {
                return MW_NONE; /* collected; the walk goes on */
            }
        } else {
            /* The registry's own pick among same-QN registrations (first or
             * last, by its index mode); two distinct definitions abstain. */
            const CBMRegisteredFunc *f = cbm_registry_lookup_method(q->reg, type, name);
            if (f) {
                for (int idx = cbm_method_iter_next(&it); idx >= 0;
                     idx = cbm_method_iter_next(&it)) {
                    k->w->work++;
                    CBM_LSP_WORK(1);
                    if (!mw_same(it.reg->funcs[idx].qualified_name, f->qualified_name)) {
                        return MW_UNKNOWN;
                    }
                }
                mw_fill(out, f, NULL, type, via, name, place, complete);
                return MW_HIT;
            }
        }
    }
    if (kind & CBM_MW_MEMBER_FIELD) {
        const CBMRegisteredType *t = cbm_registry_lookup_type(q->reg, type);
        for (int i = 0; t && t->field_names && t->field_names[i]; i++) {
            k->w->work++;
            CBM_LSP_WORK(1);
            if (strcmp(t->field_names[i], name) == 0) {
                if (k->collect) {
                    return MW_NONE; /* fields have no overloads: collected modes skip them */
                }
                mw_fill(out, NULL, t->field_types ? t->field_types[i] : NULL, type, via, name,
                        place, complete);
                return MW_HIT;
            }
        }
    }
    return MW_NONE;
}

/* An abstract member a mixin declares is a requirement, not an
 * implementation (PHP: a trait's abstract method is satisfied by the class,
 * another trait or a parent). */
static bool mw_requirement(const CBMMemberHit *h) {
    return h->place == CBM_MW_PLACE_MIXIN && h->func &&
           (h->func->flags & CBM_FUNC_FLAG_ABSTRACTMETHOD) != 0;
}

/* Take a WHOLE node's answer at a mixin place of this order: the hit's place
 * and via become this order's, one mixin level deeper. *soft gets whether
 * the sub-order passed a SOFT place (before its hit, or anywhere). */
static uint8_t mw_take(mw_walk_t *k, const mw_ans_t *a, uint8_t place, const char *via,
                       bool complete, bool renamed, CBMMemberHit *out, bool *soft, bool *open) {
    *soft = a->soft;
    *open = *open || a->open;
    if (a->result == MW_UNKNOWN) {
        return MW_UNKNOWN;
    }
    if (k->collect) {
        for (int i = 0; i < a->hit_count; i++) {
            CBMMemberHit h = a->hits[i];
            h.place = place;
            h.via_qn = via ? via : h.owner_qn;
            h.mixin_depth =
                h.mixin_depth < MW_DEPTH_MAX ? (uint8_t)(h.mixin_depth + 1) : h.mixin_depth;
            h.renamed = h.renamed || renamed;
            h.prefix_complete = complete && h.prefix_complete;
            if (!mw_collect(k, &h)) {
                k->failed = true;
                return MW_UNKNOWN;
            }
        }
        return MW_NONE;
    }
    if (a->result != MW_HIT) {
        return MW_NONE;
    }
    *out = a->hit;
    out->place = place;
    out->via_qn = via ? via : out->owner_qn;
    out->mixin_depth =
        out->mixin_depth < MW_DEPTH_MAX ? (uint8_t)(out->mixin_depth + 1) : out->mixin_depth;
    out->renamed = out->renamed || renamed;
    out->prefix_complete = complete && a->hit.prefix_complete;
    return MW_HIT;
}

/* ── groups ─────────────────────────────────────────────────────── */

static int mw_rename_cmp(const void *a, const void *b) {
    const mw_rename_t *x = (const mw_rename_t *)a;
    const mw_rename_t *y = (const mw_rename_t *)b;
    if (!x->name || !y->name) {
        return x->name ? -1 : (y->name ? 1 : 0);
    }
    return strcmp(x->name, y->name);
}

/* The renames of the type that holds a group, sorted by name (built once per
 * holder and walker). NULL when it has none. */
static const mw_renames_t *mw_renames(mw_walk_t *k, const char *holder) {
    mw_state_t *s = k->s;
    const CBMRegisteredType *t = holder ? cbm_registry_lookup_type(k->q->reg, holder) : NULL;
    if (!t || t->rename_count <= 0 || !t->renames) {
        return NULL;
    }
    if (!s->renames) {
        s->renames = cbm_ht_create(0);
        if (!s->renames) {
            return NULL;
        }
    }
    mw_renames_t *r = (mw_renames_t *)cbm_ht_get(s->renames, holder);
    if (r) {
        return r;
    }
    r = (mw_renames_t *)cbm_arena_alloc(&s->arena, sizeof(*r));
    mw_rename_t *items =
        (mw_rename_t *)cbm_arena_alloc(&s->arena, (size_t)t->rename_count * sizeof(*items));
    char *key = cbm_arena_strdup(&s->arena, holder);
    if (!r || !items || !key) {
        return NULL;
    }
    for (int i = 0; i < t->rename_count; i++) {
        items[i].name = t->renames[i].name;
        items[i].from_type = t->renames[i].from_type;
        items[i].from_name = t->renames[i].from_name;
        items[i].kind = t->renames[i].kind;
    }
    qsort(items, (size_t)t->rename_count, sizeof(*items), mw_rename_cmp);
    r->items = items;
    r->count = t->rename_count;
    cbm_ht_set(s->renames, key, r);
    k->w->work += (uint64_t)t->rename_count;
    return r;
}

/* The first rename of `name`, or rn->count. */
static int mw_renames_first(const mw_renames_t *rn, const char *name) {
    int lo = 0;
    int hi = rn ? rn->count : 0;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        const char *m = rn->items[mid].name;
        if (m && strcmp(m, name) < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

static bool mw_excluded(const mw_renames_t *rn, const char *name, const char *member) {
    for (int i = mw_renames_first(rn, name); rn && i < rn->count; i++) {
        const mw_rename_t *r = &rn->items[i];
        if (!mw_same(r->name, name)) {
            break;
        }
        if (r->kind == CBM_RENAME_EXCLUDE && mw_same(r->from_type, member)) {
            return true;
        }
    }
    return false;
}

/* A leaf member: its answer for any name is its type's own lookup. */
static bool mw_leaf(mw_walk_t *k, const CBMMemberStep *m) {
    if (m->kind == CBM_MW_OWN) {
        return m->type_id != NULL && m->place != CBM_MW_PLACE_OUTSIDE;
    }
    if (m->kind != CBM_MW_SUB || !m->type_id) {
        return false;
    }
    const CBMMemberStep *steps = NULL;
    int count = 0;
    if (!mw_order(k, m->type_id, m->side, &steps, &count) || count != 2) {
        return false;
    }
    return steps[0].kind == CBM_MW_OWN && steps[0].place == CBM_MW_PLACE_OWN &&
           mw_same(steps[0].type_id, m->type_id) && steps[1].kind == CBM_MW_STOP;
}

/* The member names every receiver of the registry declares (once per walker). */
static bool mw_receivers(mw_walk_t *k) {
    mw_state_t *s = k->s;
    if (s->receivers) {
        return true;
    }
    s->receivers = cbm_ht_create(0);
    if (!s->receivers) {
        return false;
    }
    for (const CBMTypeRegistry *r = k->q->reg; r; r = r->fallback) {
        for (int i = 0; i < r->func_count; i++) {
            const CBMRegisteredFunc *f = &r->funcs[i];
            k->w->work++;
            CBM_LSP_WORK(1);
            if (!f->receiver_type || !f->short_name) {
                continue;
            }
            mw_names_t *n = (mw_names_t *)cbm_ht_get(s->receivers, f->receiver_type);
            if (!n) {
                n = (mw_names_t *)cbm_arena_alloc(&s->arena, sizeof(*n));
                char *key = cbm_arena_strdup(&s->arena, f->receiver_type);
                if (!n || !key) {
                    return false;
                }
                memset(n, 0, sizeof(*n));
                cbm_ht_set(s->receivers, key, n);
            }
            if (n->count == n->cap) {
                int cap = n->cap ? n->cap * 2 : 4;
                const char **items =
                    (const char **)cbm_arena_alloc(&s->arena, (size_t)cap * sizeof(*items));
                if (!items) {
                    return false;
                }
                if (n->count) {
                    memcpy(items, n->items, (size_t)n->count * sizeof(*items));
                }
                n->items = items;
                n->cap = cap;
            }
            n->items[n->count++] = f->short_name;
        }
    }
    return true;
}

static bool mw_ints_add(mw_state_t *s, mw_ints_t *v, int x) {
    if (v->count && v->items[v->count - 1] == x) {
        return true;
    }
    if (v->count == v->cap) {
        int cap = v->cap ? v->cap * 2 : 2;
        int *items = (int *)cbm_arena_alloc(&s->arena, (size_t)cap * sizeof(*items));
        if (!items) {
            return false;
        }
        if (v->count) {
            memcpy(items, v->items, (size_t)v->count * sizeof(*items));
        }
        v->items = items;
        v->cap = cap;
    }
    v->items[v->count++] = x;
    return true;
}

static bool mw_gindex_name(mw_state_t *s, mw_gindex_t *gi, const char *name, int j) {
    mw_ints_t *v = (mw_ints_t *)cbm_ht_get(gi->by_name, name);
    if (!v) {
        v = (mw_ints_t *)cbm_arena_alloc(&s->arena, sizeof(*v));
        char *key = cbm_arena_strdup(&s->arena, name);
        if (!v || !key) {
            return false;
        }
        memset(v, 0, sizeof(*v));
        cbm_ht_set(gi->by_name, key, v);
    }
    return mw_ints_add(s, v, j);
}

/* The name index of the group at `g` (built once per group and walker), or
 * NULL to ask every member. */
static const mw_gindex_t *mw_group_index(mw_walk_t *k, const CBMMemberStep *g) {
    mw_state_t *s = k->s;
    if (g->count < MW_GROUP_INDEXED || !mw_receivers(k)) {
        return NULL;
    }
    if (!s->gindex) {
        s->gindex = cbm_ht_create(0);
        if (!s->gindex) {
            return NULL;
        }
    }
    char key[2 * sizeof(void *) + 3];
    snprintf(key, sizeof(key), "%p", (const void *)g);
    mw_gindex_t *gi = (mw_gindex_t *)cbm_ht_get(s->gindex, key);
    if (gi) {
        return gi;
    }
    gi = (mw_gindex_t *)cbm_arena_alloc(&s->arena, sizeof(*gi));
    char *kcopy = cbm_arena_strdup(&s->arena, key);
    int *always = (int *)cbm_arena_alloc(&s->arena, (size_t)g->count * sizeof(int));
    if (!gi || !kcopy || !always) {
        return NULL;
    }
    memset(gi, 0, sizeof(*gi));
    mw_gindex_node_t *node = (mw_gindex_node_t *)cbm_arena_alloc(&s->arena, sizeof(*node));
    gi->by_name = node ? cbm_ht_create(0) : NULL;
    if (!gi->by_name) {
        return NULL;
    }
    node->gi = gi;
    node->next = s->gindex_list;
    s->gindex_list = node;
    const CBMMemberStep *members = g + 1;
    for (int j = 0; j < g->count; j++) {
        const CBMMemberStep *m = &members[j];
        k->w->work++;
        CBM_LSP_WORK(1);
        if (!mw_leaf(k, m)) {
            always[gi->always_count++] = j;
            continue;
        }
        const mw_names_t *n = (const mw_names_t *)cbm_ht_get(s->receivers, m->type_id);
        for (int i = 0; n && i < n->count; i++) {
            k->w->work++;
            if (!mw_gindex_name(s, gi, n->items[i], j)) {
                return NULL;
            }
        }
        const CBMRegisteredType *t = cbm_registry_lookup_type(k->q->reg, m->type_id);
        for (int i = 0; t && t->field_names && t->field_names[i]; i++) {
            k->w->work++;
            if (!mw_gindex_name(s, gi, t->field_names[i], j)) {
                return NULL;
            }
        }
    }
    gi->always = always;
    cbm_ht_set(s->gindex, kcopy, gi);
    return gi;
}

/* One member of a group, asked for `name`. */
static uint8_t mw_member(mw_walk_t *k, const CBMMemberStep *m, const char *name, const char *via,
                         bool complete, bool renamed, CBMMemberHit *out, bool *soft, bool *open) {
    *soft = false;
    if (m->kind == CBM_MW_UNSEEN) {
        if (mw_soft(k, m->flags)) {
            *soft = true; /* a member nobody can see: passed, no proof */
            return MW_NONE;
        }
        return MW_UNKNOWN;
    }
    if (!m->type_id) {
        return MW_UNKNOWN;
    }
    if (mw_leaf(k, m)) {
        uint8_t r = mw_own(k, m->type_id, name, via, m->place, complete, out);
        if (r == MW_HIT) {
            out->mixin_depth = m->kind == CBM_MW_SUB ? 1 : 0;
            out->renamed = renamed;
        }
        return r;
    }
    if (m->kind == CBM_MW_SUB) {
        const mw_ans_t *a = mw_dep(k, MW_NODE_WHOLE, m->type_id, m->side, name);
        if (!a) {
            return MW_NONE; /* k->blocked */
        }
        return mw_take(k, a, m->place, via, complete, renamed, out, soft, open);
    }
    return MW_UNKNOWN; /* malformed */
}

/* Combine one member's answer into the group's. Returns false on a conflict
 * (two distinct definitions). */
static bool mw_group_add(uint8_t r, const CBMMemberHit *hit, CBMMemberHit *one, bool *have,
                         bool *unknown) {
    if (r == MW_UNKNOWN) {
        *unknown = true;
        return true;
    }
    if (r != MW_HIT) {
        return true;
    }
    if (*have) {
        const CBMRegisteredFunc *a = one->func;
        const CBMRegisteredFunc *b = hit->func;
        bool same = a && b ? (a == b || mw_same(a->qualified_name, b->qualified_name))
                           : (!a && !b && mw_same(one->owner_qn, hit->owner_qn));
        if (same || mw_requirement(hit)) {
            return true; /* a requirement never conflicts: an implementation satisfies it */
        }
        if (mw_requirement(one)) {
            *one = *hit;
            return true;
        }
        return false;
    }
    *one = *hit;
    *have = true;
    return true;
}

/* The group at steps[i]. *gsoft: a member passed a SOFT place and it counts
 * for what follows (not when an exclusive group answers with its provider). */
static uint8_t mw_group(mw_walk_t *k, const CBMMemberStep *steps, int i, const char *name,
                        bool complete, CBMMemberHit *out, bool *gsoft, bool *open) {
    const CBMMemberStep *g = &steps[i];
    const CBMMemberStep *members = g + 1;
    bool exclusive = (g->flags & CBM_MW_GROUP_EXCLUSIVE) != 0;
    const mw_renames_t *rn = mw_renames(k, g->via);
    const mw_gindex_t *gi = mw_group_index(k, g);
    const mw_ints_t *named = gi ? (const mw_ints_t *)cbm_ht_get(gi->by_name, name) : NULL;
    CBMMemberHit one;
    memset(&one, 0, sizeof(one));
    bool have = false;
    bool unknown = false;
    bool soft_any = false;
    bool blocked = false;
    const bool entry_blocked = k->blocked;
    /* Members in their order: every one, or (indexed) the leaves declaring
     * the name merged with the members always asked. Every member's need is
     * recorded in one pass. */
    int a = 0;
    int b = 0;
    int na = named ? named->count : 0;
    int nb = gi ? gi->always_count : 0;
    for (int j = 0;;) {
        if (gi) {
            if (a >= na && b >= nb) {
                break;
            }
            if (b >= nb || (a < na && named->items[a] < gi->always[b])) {
                j = named->items[a++];
            } else {
                j = gi->always[b++];
            }
        } else if (j >= g->count) {
            break;
        }
        const CBMMemberStep *m = &members[j];
        k->w->work++;
        CBM_LSP_WORK(1);
        if (!gi) {
            j++;
        }
        if (m->type_id && mw_excluded(rn, name, m->type_id)) {
            continue;
        }
        CBMMemberHit hit;
        memset(&hit, 0, sizeof(hit));
        bool msoft = false;
        k->blocked = false;
        uint8_t r = mw_member(k, m, name, g->via, complete, false, &hit, &msoft, open);
        if (k->blocked) {
            blocked = true;
            continue;
        }
        soft_any = soft_any || msoft;
        if (!mw_group_add(r, &hit, &one, &have, &unknown)) {
            k->blocked = entry_blocked; /* two definitions: unknown whatever the rest holds */
            return MW_UNKNOWN;
        }
    }
    /* Aliases the holder gives a member: `name` is another name for it. */
    for (int ri = mw_renames_first(rn, name); rn && ri < rn->count; ri++) {
        const mw_rename_t *ren = &rn->items[ri];
        if (!mw_same(ren->name, name)) {
            break;
        }
        if (ren->kind != CBM_RENAME_ALIAS || !ren->from_name) {
            continue;
        }
        bool matched = false;
        for (int j = 0; j < g->count; j++) {
            const CBMMemberStep *m = &members[j];
            k->w->work++;
            CBM_LSP_WORK(1);
            if (ren->from_type && !mw_same(ren->from_type, m->type_id)) {
                continue;
            }
            matched = true;
            CBMMemberHit hit;
            memset(&hit, 0, sizeof(hit));
            bool msoft = false;
            k->blocked = false;
            uint8_t r = mw_member(k, m, ren->from_name, g->via, complete, true, &hit, &msoft, open);
            if (k->blocked) {
                blocked = true;
                continue;
            }
            soft_any = soft_any || msoft;
            if (!mw_group_add(r, &hit, &one, &have, &unknown)) {
                k->blocked = entry_blocked;
                return MW_UNKNOWN;
            }
        }
        if (!matched) {
            unknown = true; /* the alias names a mixin nobody can see */
        }
    }
    k->blocked = entry_blocked || blocked;
    if (blocked) {
        return MW_NONE;
    }
    *gsoft = soft_any;
    if (k->collect) {
        return unknown ? MW_UNKNOWN : MW_NONE;
    }
    /* A requirement beside an unknown member: that member may implement it. */
    if (have && (exclusive || !unknown) && !(unknown && mw_requirement(&one))) {
        if (exclusive && !mw_requirement(&one)) {
            /* Valid programs have one provider: a member nobody can see does
             * not provide it too, so the hit keeps its own prefix. */
            *gsoft = false;
        } else if (soft_any) {
            /* A member nobody can see may provide it too (an open group), or
             * may implement the requirement: only a possible answer. */
            one.prefix_complete = false;
        }
        *out = one;
        return MW_HIT;
    }
    return unknown ? MW_UNKNOWN : MW_NONE;
}

/* ── one pass over a chain (phase 1) ─────────────────────────────── */

static bool mw_later_tmp(mw_walk_t *k, const CBMMemberStep *st) {
    mw_state_t *s = k->s;
    if (!mw_vec_room((void **)&s->later_tmp, &s->later_tmp_cap, s->later_tmp_count,
                     sizeof(*s->later_tmp))) {
        return false;
    }
    mw_later_t *e = &s->later_tmp[s->later_tmp_count++];
    e->type = st->type_id;
    e->side = st->side;
    e->soft = st->type_id == NULL && mw_soft(k, st->flags);
    e->next = NULL;
    return true;
}

/* The LATER steps met in this pass, in front of `tail`, as a kept list. */
static const mw_later_t *mw_later_list(mw_walk_t *k, const mw_later_t *tail) {
    mw_state_t *s = k->s;
    const mw_later_t *list = tail;
    for (int i = s->later_tmp_count - 1; i >= 0; i--) {
        mw_later_t *e = (mw_later_t *)cbm_arena_alloc(&s->arena, sizeof(*e));
        if (!e) {
            k->failed = true;
            return NULL;
        }
        *e = s->later_tmp[i];
        e->next = list;
        list = e;
    }
    return list;
}

static void mw_set_hits(mw_walk_t *k, mw_ans_t *out) {
    mw_state_t *s = k->s;
    if (!k->collect || s->coll_count == 0) {
        return;
    }
    CBMMemberHit *hits =
        (CBMMemberHit *)cbm_arena_alloc(&s->arena, (size_t)s->coll_count * sizeof(*hits));
    if (!hits) {
        k->failed = true;
        return;
    }
    memcpy(hits, s->coll, (size_t)s->coll_count * sizeof(*hits));
    out->hits = hits;
    out->hit_count = s->coll_count;
}

/* A hit answers the pass. Its `soft`: anything nobody can see before it,
 * inside its own prefix too (a pending requirement then answers itself). */
static bool mw_answer(mw_ans_t *out, const CBMMemberHit *hit, bool soft, bool outside, bool open) {
    memset(out, 0, sizeof(*out));
    out->result = MW_HIT;
    out->hit = *hit;
    out->soft = soft || !hit->prefix_complete;
    out->outside = outside;
    out->open = open;
    return true;
}

static bool mw_unknown(mw_ans_t *out) {
    memset(out, 0, sizeof(*out));
    out->result = MW_UNKNOWN;
    return true;
}

/* The pending requirement answers here: possible when anything SOFT lies on
 * the way to it, or before it was met. */
static bool mw_answer_req(mw_ans_t *out, CBMMemberHit req, bool soft, bool outside, bool open) {
    req.prefix_complete = req.prefix_complete && !soft && !outside;
    return mw_answer(out, &req, soft, outside, open);
}

/* The rest of a chain, past a NEXT link, as the parent's CHAIN node says. */
static bool mw_link(mw_walk_t *k, mw_ans_t *out, const mw_ans_t *nx, bool soft, bool outside,
                    bool open, bool have_req, const CBMMemberHit *req) {
    if (nx->result == MW_UNKNOWN) {
        return mw_unknown(out);
    }
    open = open || nx->open;
    bool ok = !soft && !outside;
    if (k->collect) {
        for (int i = 0; i < nx->hit_count; i++) {
            CBMMemberHit h = nx->hits[i];
            if (h.place == CBM_MW_PLACE_OWN) {
                h.place = CBM_MW_PLACE_SUPER;
            }
            h.prefix_complete = ok && h.prefix_complete;
            if (!mw_collect(k, &h)) {
                k->failed = true;
                return mw_unknown(out);
            }
        }
        memset(out, 0, sizeof(*out));
        out->result = MW_NONE;
        out->soft = soft || nx->soft;
        out->outside = outside || nx->outside;
        out->open = open;
        out->laters = mw_later_list(k, nx->laters);
        mw_set_hits(k, out);
        return true;
    }
    if (have_req) {
        if (nx->soft) {
            /* Something nobody can see lies between the requirement and what
             * implements it: the requirement itself answers, possible. */
            return mw_answer_req(out, *req, true, outside || nx->outside, open);
        }
        if (nx->result == MW_HIT && !mw_requirement(&nx->hit)) {
            CBMMemberHit h = nx->hit;
            if (h.place == CBM_MW_PLACE_OWN) {
                h.place = CBM_MW_PLACE_SUPER;
            }
            h.prefix_complete = ok && h.prefix_complete;
            return mw_answer(out, &h, soft, outside || nx->outside, open);
        }
        return mw_answer_req(out, *req, soft, outside || nx->outside, open);
    }
    if (nx->result == MW_HIT) {
        CBMMemberHit h = nx->hit;
        if (h.place == CBM_MW_PLACE_OWN) {
            h.place = CBM_MW_PLACE_SUPER;
        }
        h.prefix_complete = ok && h.prefix_complete;
        return mw_answer(out, &h, soft || nx->soft, outside || nx->outside, open);
    }
    memset(out, 0, sizeof(*out));
    out->result = MW_NONE;
    out->soft = soft || nx->soft;
    out->outside = outside || nx->outside;
    out->open = open;
    out->laters = mw_later_list(k, nx->laters);
    return true;
}

/* One pass over (type, side)'s order up to its STOP or NEXT link (phase 1).
 * With `after` (a `super` lookup) the steps up to and including the OWN step
 * of `after` are skipped, NEXT links followed inline while skipping. Returns
 * false when a node it needs is still to be answered. */
static bool mw_pass_chain(mw_walk_t *k, const char *type, int side, const char *name,
                          const char *after, int after_side, mw_ans_t *out) {
    mw_state_t *s = k->s;
    CBMMemberWalker *w = k->w;
    const CBMMemberStep *steps = NULL;
    int count = 0;
    s->later_tmp_count = 0;
    s->coll_count = 0;
    if (!mw_order(k, type, side, &steps, &count)) {
        return mw_unknown(out);
    }
    bool skipping = after != NULL;
    if (skipping) {
        mw_seen_begin(s);
        if (mw_seen_add(s, type, side, MW_SEEN_LINK) < 0) {
            k->failed = true;
            return mw_unknown(out);
        }
    }
    const char *cur = type;
    int depth = 0;
    bool soft = false;
    bool outside = false;
    bool open = false;
    bool stopped = false;
    bool have_req = false;
    CBMMemberHit req;
    memset(&req, 0, sizeof(req));
    for (int i = 0; i < count; i++) {
        const CBMMemberStep *st = &steps[i];
        w->work++;
        CBM_LSP_WORK(1);
        if (st->kind == CBM_MW_NEXT) {
            /* The order goes on as another type's (the last step only). */
            if (!st->type_id || i != count - 1) {
                return mw_unknown(out);
            }
            if (skipping) {
                int fresh = mw_seen_add(s, st->type_id, st->side, MW_SEEN_LINK);
                if (fresh <= 0 || !mw_order(k, st->type_id, st->side, &steps, &count)) {
                    return mw_unknown(out); /* a cycle, or no memory */
                }
                cur = st->type_id;
                depth++;
                i = -1;
                continue;
            }
            const mw_ans_t *nx = mw_dep(k, MW_NODE_CHAIN, st->type_id, st->side, name);
            if (!nx) {
                return false;
            }
            return mw_link(k, out, nx, soft, outside, open, have_req, &req);
        }
        uint8_t place = st->place;
        if (depth > 0 && place == CBM_MW_PLACE_OWN) {
            place = CBM_MW_PLACE_SUPER;
        }
        if (skipping) {
            /* `super`: everything up to and including the definition being
             * extended is behind the call. */
            if (st->kind == CBM_MW_GROUP) {
                for (int j = 1; j <= st->count && i + j < count; j++) {
                    const CBMMemberStep *m = &steps[i + j];
                    if (m->side == after_side && mw_same(m->type_id, after)) {
                        return mw_unknown(out); /* where it stands among the others is not known */
                    }
                }
                i += st->count;
                continue;
            }
            if (st->kind == CBM_MW_OWN && st->side == after_side && mw_same(st->type_id, after)) {
                skipping = false;
            }
            if (st->kind == CBM_MW_LATER && !mw_later_tmp(k, st)) {
                k->failed = true;
                return mw_unknown(out);
            }
            if (st->kind == CBM_MW_OPEN) {
                open = true;
            }
            if (st->kind == CBM_MW_STOP) {
                stopped = true;
                break;
            }
            continue;
        }
        bool soft_before = soft;
        bool complete = !soft && !outside;
        bool step_soft = false;
        uint8_t r = MW_NONE;
        CBMMemberHit hit;
        memset(&hit, 0, sizeof(hit));
        switch (st->kind) {
        case CBM_MW_OWN:
            r = st->type_id ? mw_own(k, st->type_id, name, NULL, place, complete, &hit)
                            : MW_UNKNOWN;
            if (r == MW_NONE && st->place == CBM_MW_PLACE_OUTSIDE) {
                outside = true;
            }
            break;
        case CBM_MW_SUB: {
            if (!st->type_id) {
                return mw_unknown(out);
            }
            const mw_ans_t *a = mw_dep(k, MW_NODE_WHOLE, st->type_id, st->side, name);
            if (!a) {
                return false;
            }
            r = mw_take(k, a, place, cur, complete, false, &hit, &step_soft, &open);
            if (r == MW_NONE && st->place == CBM_MW_PLACE_OUTSIDE) {
                outside = true;
            }
            break;
        }
        case CBM_MW_GROUP:
            if (st->count <= 0 || i + st->count >= count) {
                return mw_unknown(out); /* malformed */
            }
            r = mw_group(k, steps, i, name, complete, &hit, &step_soft, &open);
            if (k->blocked) {
                return false;
            }
            i += st->count;
            break;
        case CBM_MW_LATER:
            if (!mw_later_tmp(k, st)) {
                k->failed = true;
                return mw_unknown(out);
            }
            break;
        case CBM_MW_OPEN:
            open = true;
            break;
        case CBM_MW_STOP:
            stopped = true;
            break;
        case CBM_MW_UNSEEN:
            if (mw_soft(k, st->flags)) {
                step_soft = true; /* passed: what follows is only possible */
                break;
            }
            return mw_unknown(out);
        default: /* malformed */
            return mw_unknown(out);
        }
        if (r == MW_UNKNOWN) {
            return mw_unknown(out);
        }
        if (r == MW_HIT && !k->collect && mw_requirement(&hit)) {
            if (soft_before || step_soft || !hit.prefix_complete) {
                /* A place nobody can see beside or before the requirement may
                 * implement it: the requirement itself answers, possible. */
                hit.prefix_complete = false;
                return mw_answer(out, &hit, true, outside, open);
            }
            if (!have_req) {
                req = hit; /* what implements it lies further on */
                have_req = true;
            }
            r = MW_NONE;
        }
        if (r == MW_HIT) {
            return mw_answer(out, &hit, soft_before || step_soft, outside, open);
        }
        if (step_soft) {
            soft = true;
            if (have_req) {
                return mw_answer_req(out, req, true, outside, open);
            }
        }
        if (stopped) {
            break;
        }
    }
    if (!stopped || skipping) {
        return mw_unknown(out); /* no STOP: unknown; or the `super` start was not in the order */
    }
    if (have_req) {
        return mw_answer_req(out, req, soft, outside, open);
    }
    memset(out, 0, sizeof(*out));
    out->result = MW_NONE;
    out->soft = soft;
    out->outside = outside;
    out->open = open;
    out->laters = mw_later_list(k, NULL);
    mw_set_hits(k, out);
    return true;
}

/* ── phase 2 ────────────────────────────────────────────────────── */

static bool mw_queue_add(mw_state_t *s, const char *type, int side, bool soft) {
    if (!mw_vec_room((void **)&s->queue, &s->queue_cap, s->queue_count, sizeof(*s->queue))) {
        return false;
    }
    mw_later_t *e = &s->queue[s->queue_count++];
    e->type = type;
    e->side = side;
    e->soft = soft;
    e->next = NULL;
    return true;
}

/* Is there a cycle among the phase-2 entries this pass visited? (They are
 * searched once each; a cycle must not end as "absent".) Depth-first over
 * their LATER steps, on an explicit stack. */
static bool mw_later_cycle(mw_walk_t *k) {
    mw_state_t *s = k->s;
    enum { WHITE = 0, GRAY = 1, BLACK = 2 };
    for (int qi = 0; qi < s->queue_count; qi++) {
        const mw_later_t *root = &s->queue[qi];
        if (!root->type) {
            continue;
        }
        mw_seen_t *r = mw_seen_find(s, root->type, root->side, MW_SEEN_LATER);
        if (!r || r->gen != s->seen_gen || r->color != WHITE) {
            continue;
        }
        /* The stack holds queue positions and, beside each, the next step. */
        int top = 0;
        if (!mw_vec_room((void **)&s->dfs, &s->dfs_cap, top + 1, 2 * sizeof(int))) {
            return true;
        }
        s->dfs[0] = qi;
        s->dfs[1] = 0;
        top = 1;
        r->color = GRAY;
        while (top > 0) {
            int at = s->dfs[2 * (top - 1)];
            int step = s->dfs[2 * (top - 1) + 1];
            const mw_later_t *e = &s->queue[at];
            const CBMMemberStep *steps = NULL;
            int count = 0;
            mw_seen_t *me = mw_seen_find(s, e->type, e->side, MW_SEEN_LATER);
            if (!mw_order(k, e->type, e->side, &steps, &count) || step >= count) {
                if (me) {
                    me->color = BLACK;
                }
                top--;
                continue;
            }
            s->dfs[2 * (top - 1) + 1] = step + 1;
            const CBMMemberStep *st = &steps[step];
            k->w->work++;
            if (st->kind != CBM_MW_LATER || !st->type_id) {
                continue;
            }
            mw_seen_t *c = mw_seen_find(s, st->type_id, st->side, MW_SEEN_LATER);
            if (!c || c->gen != s->seen_gen) {
                continue; /* not visited in this pass */
            }
            if (c->color == GRAY) {
                return true;
            }
            if (c->color == BLACK) {
                continue;
            }
            int pos = c->pos; /* where it was searched */
            if (pos < 0 || pos >= s->queue_count) {
                continue;
            }
            if (!mw_vec_room((void **)&s->dfs, &s->dfs_cap, top + 1, 2 * sizeof(int))) {
                return true;
            }
            s->dfs[2 * top] = pos;
            s->dfs[2 * top + 1] = 0;
            top++;
            c->color = GRAY;
        }
    }
    return false;
}

/* Phase 2 of a WHOLE node (or a query): the deferred entries, breadth first,
 * each type once, the first hit wins. `c` is the chain's answer (NONE). */
static bool mw_pass_later(mw_walk_t *k, const char *name, const mw_ans_t *c, mw_ans_t *out) {
    mw_state_t *s = k->s;
    CBMMemberWalker *w = k->w;
    s->queue_count = 0;
    s->coll_count = 0;
    for (int i = 0; k->collect && i < c->hit_count; i++) {
        if (!mw_collect(k, &c->hits[i])) {
            k->failed = true;
            return mw_unknown(out);
        }
    }
    for (const mw_later_t *e = c->laters; e; e = e->next) {
        if (!mw_queue_add(s, e->type, e->side, e->soft)) {
            k->failed = true;
            return mw_unknown(out);
        }
    }
    mw_seen_begin(s);
    bool soft = c->soft;
    bool outside = c->outside;
    bool open = c->open;
    bool revisit = false;
    for (int qi = 0; qi < s->queue_count; qi++) {
        mw_later_t e = s->queue[qi];
        w->work++;
        CBM_LSP_WORK(1);
        if (!e.type) {
            if (e.soft) {
                soft = true; /* an interface nobody can see: passed, no proof */
                continue;
            }
            return mw_unknown(out);
        }
        int fresh = mw_seen_add(s, e.type, e.side, MW_SEEN_LATER);
        if (fresh < 0) {
            k->failed = true;
            return mw_unknown(out);
        }
        if (fresh == 0) {
            revisit = true; /* searched already in this pass */
            continue;
        }
        mw_seen_t *mark = mw_seen_find(s, e.type, e.side, MW_SEEN_LATER);
        if (mark) {
            mark->pos = qi;
        }
        const CBMMemberStep *steps = NULL;
        int count = 0;
        if (!mw_order(k, e.type, e.side, &steps, &count)) {
            return mw_unknown(out);
        }
        bool ended = false;
        for (int i = 0; i < count && !ended; i++) {
            const CBMMemberStep *st = &steps[i];
            w->work++;
            CBM_LSP_WORK(1);
            bool complete = !soft && !outside;
            bool step_soft = false;
            uint8_t r = MW_NONE;
            CBMMemberHit hit;
            memset(&hit, 0, sizeof(hit));
            switch (st->kind) {
            case CBM_MW_OWN:
                r = st->type_id ? mw_own(k, st->type_id, name, NULL, CBM_MW_PLACE_INTERFACE_DEFAULT,
                                         complete, &hit)
                                : MW_UNKNOWN;
                break;
            case CBM_MW_SUB: {
                if (!st->type_id) {
                    return mw_unknown(out);
                }
                const mw_ans_t *a = mw_dep(k, MW_NODE_WHOLE, st->type_id, st->side, name);
                if (!a) {
                    return false;
                }
                r = mw_take(k, a, CBM_MW_PLACE_INTERFACE_DEFAULT, NULL, complete, false, &hit,
                            &step_soft, &open);
                break;
            }
            case CBM_MW_GROUP:
                if (st->count <= 0 || i + st->count >= count) {
                    return mw_unknown(out);
                }
                r = mw_group(k, steps, i, name, complete, &hit, &step_soft, &open);
                if (k->blocked) {
                    return false;
                }
                i += st->count;
                break;
            case CBM_MW_LATER:
                if (!mw_queue_add(s, st->type_id, st->side,
                                  st->type_id == NULL && mw_soft(k, st->flags))) {
                    k->failed = true;
                    return mw_unknown(out);
                }
                break;
            case CBM_MW_OPEN:
                open = true;
                break;
            case CBM_MW_STOP:
                ended = true;
                break;
            case CBM_MW_NEXT: {
                /* An order linked on: walk it as part of this entry. */
                if (!st->type_id || i != count - 1) {
                    return mw_unknown(out);
                }
                int f2 = mw_seen_add(s, st->type_id, st->side, MW_SEEN_LINK);
                if (f2 <= 0 || !mw_order(k, st->type_id, st->side, &steps, &count)) {
                    return mw_unknown(out);
                }
                i = -1;
                break;
            }
            case CBM_MW_UNSEEN:
                if (mw_soft(k, st->flags)) {
                    step_soft = true;
                    break;
                }
                return mw_unknown(out);
            default: /* malformed */
                return mw_unknown(out);
            }
            if (r == MW_UNKNOWN) {
                return mw_unknown(out);
            }
            if (r == MW_HIT) {
                return mw_answer(out, &hit, soft || step_soft, outside, open);
            }
            soft = soft || step_soft;
        }
        if (!ended) {
            return mw_unknown(out); /* an order that does not end with STOP is not complete */
        }
    }
    if (revisit && mw_later_cycle(k)) {
        return mw_unknown(out); /* a cycle among the interfaces: never "absent" */
    }
    memset(out, 0, sizeof(*out));
    out->result = MW_NONE;
    out->soft = soft;
    out->outside = outside;
    out->open = open;
    mw_set_hits(k, out);
    return true;
}

/* One pass over a frame's node. */
static bool mw_pass(mw_walk_t *k, const mw_frame_t *f, mw_ans_t *out) {
    if (f->node == MW_NODE_CHAIN) {
        return mw_pass_chain(k, f->type, f->side, f->name, NULL, 0, out);
    }
    mw_ans_t c;
    if (f->node == MW_NODE_QUERY) {
        if (!mw_pass_chain(k, f->type, f->side, f->name, k->q->after, k->q->after_side, &c)) {
            return false;
        }
    } else {
        const mw_ans_t *a = mw_dep(k, MW_NODE_CHAIN, f->type, f->side, f->name);
        if (!a) {
            return false;
        }
        c = *a;
    }
    if (c.result != MW_NONE) {
        *out = c;
        out->laters = NULL;
        return true;
    }
    return mw_pass_later(k, f->name, &c, out);
}

/* ── solving a node on an explicit stack ────────────────────────── */

static bool mw_frame_push(mw_state_t *s, uint8_t node, const char *type, int side,
                          const char *name) {
    if (!mw_vec_room((void **)&s->frames, &s->frame_cap, s->frame_count, sizeof(*s->frames))) {
        return false;
    }
    mw_frame_t *f = &s->frames[s->frame_count++];
    memset(f, 0, sizeof(*f));
    f->node = node;
    f->type = type;
    f->side = side;
    f->name = name;
    f->need_lo = f->need_next = f->need_hi = s->need_count;
    return true;
}

/* Drop the needs from `lo` on: their nodes are no longer queued. */
static void mw_needs_drop(mw_state_t *s, int lo) {
    for (int i = lo; i < s->need_count; i++) {
        const mw_need_t *nd = &s->needs[i];
        mw_slot_t *sl = mw_slot_find(s, nd->node, nd->type, nd->side, nd->name, false);
        if (sl && sl->state == MW_SLOT_QUEUED) {
            sl->state = MW_SLOT_EMPTY;
        }
    }
    s->need_count = lo;
}

static const mw_ans_t *mw_keep(mw_walk_t *k, const mw_ans_t *a) {
    mw_ans_t *kept = (mw_ans_t *)cbm_arena_alloc(&k->s->arena, sizeof(*kept));
    if (!kept) {
        k->failed = true;
        return &MW_ANS_UNKNOWN;
    }
    *kept = *a;
    return kept;
}

/* Answer `node` for (type, side, name), and every node it needs first. */
static const mw_ans_t *mw_solve(mw_walk_t *k, uint8_t node, const char *type, int side,
                                const char *name) {
    mw_state_t *s = k->s;
    if (node != MW_NODE_QUERY) {
        mw_slot_t *sl = mw_slot_put(s, node, type, side, name);
        if (!sl) {
            k->failed = true;
            return &MW_ANS_UNKNOWN;
        }
        if (sl->state == MW_SLOT_DONE) {
            return sl->ans;
        }
        sl->state = MW_SLOT_BUSY;
        sl->frame = 0;
    }
    s->frame_count = 0;
    s->need_count = 0;
    if (!mw_frame_push(s, node, type, side, name)) {
        k->failed = true;
        return &MW_ANS_UNKNOWN;
    }
    const mw_ans_t *root = &MW_ANS_UNKNOWN;
    while (s->frame_count > 0 && !k->failed) {
        int top = s->frame_count - 1;
        mw_frame_t *f = &s->frames[top];
        if (f->need_next < f->need_hi) {
            mw_need_t nd = s->needs[f->need_next++];
            mw_slot_t *sl = mw_slot_find(s, nd.node, nd.type, nd.side, nd.name, false);
            if (!sl || sl->state == MW_SLOT_DONE) {
                continue;
            }
            if (sl->state == MW_SLOT_BUSY) {
                mw_cycle(s, sl->frame);
                continue;
            }
            if (!mw_frame_push(s, nd.node, nd.type, nd.side, nd.name)) {
                k->failed = true;
                break;
            }
            sl = mw_slot_find(s, nd.node, nd.type, nd.side, nd.name, false);
            if (sl) {
                sl->state = MW_SLOT_BUSY;
                sl->frame = s->frame_count - 1;
            }
            continue;
        }
        /* Every need answered (or none yet): run the pass. */
        mw_needs_drop(s, f->need_lo);
        f->need_next = f->need_hi = s->need_count;
        k->blocked = false;
        mw_frame_t cur = *f;
        mw_ans_t ans;
        memset(&ans, 0, sizeof(ans));
        bool done = mw_pass(k, &cur, &ans);
        f = &s->frames[top];
        if (k->failed) {
            break;
        }
        if (!done) {
            f->need_hi = s->need_count;
            if (f->need_hi > f->need_lo) {
                continue;
            }
            ans = MW_ANS_UNKNOWN; /* blocked without a need: cannot happen; unknown */
        }
        mw_needs_drop(s, f->need_lo);
        if (f->cyclic) {
            ans = MW_ANS_UNKNOWN; /* a cycle: the language rejects it, nobody knows */
        }
        const mw_ans_t *kept = mw_keep(k, &ans);
        if (f->node == MW_NODE_QUERY) {
            root = kept;
        } else {
            mw_slot_t *sl = mw_slot_find(s, f->node, f->type, f->side, f->name, false);
            if (sl) {
                sl->state = MW_SLOT_DONE;
                sl->ans = kept;
            }
            if (top == 0) {
                root = kept;
            }
        }
        s->frame_count--;
    }
    if (k->failed) {
        /* Nothing half-answered stays: the nodes on the stack are emptied. */
        for (int i = 0; i < s->frame_count; i++) {
            const mw_frame_t *f = &s->frames[i];
            mw_slot_t *sl = mw_slot_find(s, f->node, f->type, f->side, f->name, false);
            if (sl && sl->state != MW_SLOT_DONE) {
                sl->state = MW_SLOT_EMPTY;
            }
        }
        mw_needs_drop(s, 0);
        s->frame_count = 0;
        return &MW_ANS_UNKNOWN;
    }
    return root;
}

/* ── the walker ─────────────────────────────────────────────────── */

void cbm_member_walker_init(CBMMemberWalker *w, CBMMemberOrders *orders) {
    if (!w) {
        return;
    }
    memset(w, 0, sizeof(*w));
    w->orders = orders;
    mw_state_t *s = (mw_state_t *)cbm_calloc(CBM_MEM_CLASS_RESOLVE, sizeof(*s));
    if (!s) {
        return;
    }
    cbm_arena_init(&s->arena);
#ifdef CBM_ENABLE_TEST_SEAMS
    /* Revert check of the SOFT places: with it set they act as hard ones. */
    const char *off = getenv("CBM_TEST_MEMBER_WALK_SOFT_OFF");
    s->soft_off = off && off[0] == '1';
#endif
    w->state = s;
}

/* Forget every answer and index (another registry or member kind). The
 * arena keeps what they held until the walker is freed. */
static void mw_state_reset(mw_state_t *s) {
    for (mw_gindex_node_t *n = s->gindex_list; n; n = n->next) {
        if (n->gi && n->gi->by_name) {
            cbm_ht_free(n->gi->by_name);
            n->gi->by_name = NULL;
        }
    }
    s->gindex_list = NULL;
    if (s->gindex) {
        cbm_ht_free(s->gindex);
        s->gindex = NULL;
    }
    if (s->receivers) {
        cbm_ht_free(s->receivers);
        s->receivers = NULL;
    }
    if (s->renames) {
        cbm_ht_free(s->renames);
        s->renames = NULL;
    }
    mw_slots_clear(s);
}

void cbm_member_walker_free(CBMMemberWalker *w) {
    if (!w) {
        return;
    }
    mw_state_t *s = (mw_state_t *)w->state;
    if (s) {
        mw_state_reset(s);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->slots);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->frames);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->needs);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->seen);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->queue);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->later_tmp);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->coll);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s->dfs);
        cbm_arena_destroy(&s->arena);
        cbm_free(CBM_MEM_CLASS_RESOLVE, s);
    }
    cbm_free(CBM_MEM_CLASS_RESOLVE, w->hits);
    memset(w, 0, sizeof(*w));
}

CBMMemberWalkResult cbm_member_lookup(CBMMemberWalker *w, const CBMMemberQuery *q,
                                      CBMMemberHit *out) {
    if (out) {
        memset(out, 0, sizeof(*out));
    }
    if (!w || !w->state || !q || !q->reg || !q->type_qn || !q->name || !out || q->side < 0 ||
        q->side >= CBM_MW_SIDES) {
        return CBM_MW_ABSTAIN;
    }
    mw_state_t *s = (mw_state_t *)w->state;
    uint8_t member_kind = q->member_kind ? q->member_kind : CBM_MW_MEMBER_METHOD;
    if (!s->slots && !mw_slots_grow(s)) {
        return CBM_MW_ABSTAIN;
    }
    if (s->reg != q->reg || s->member_kind != member_kind || s->overloads != q->overloads) {
        /* Answers hold for one registry, member kind and overload mode. */
        mw_state_reset(s);
        s->reg = q->reg;
        s->member_kind = member_kind;
        s->overloads = q->overloads;
    }
    w->hit_count = 0;
    mw_walk_t k;
    memset(&k, 0, sizeof(k));
    k.w = w;
    k.s = s;
    k.q = q;
    k.collect = q->overloads == CBM_MW_COLLECT || q->overloads == CBM_MW_HIDE_BY_SIGNATURE;
    const mw_ans_t *a = q->after ? mw_solve(&k, MW_NODE_QUERY, q->type_qn, q->side, q->name)
                                 : mw_solve(&k, MW_NODE_WHOLE, q->type_qn, q->side, q->name);
    if (k.failed || !a) {
        return CBM_MW_ABSTAIN;
    }
    if (k.collect) {
        if (a->result == MW_UNKNOWN) {
            return CBM_MW_ABSTAIN;
        }
        if (a->hit_count > 0) {
            if (a->hit_count > w->hit_cap) {
                CBMMemberHit *grown = (CBMMemberHit *)cbm_realloc(
                    CBM_MEM_CLASS_RESOLVE, w->hits, (size_t)a->hit_count * sizeof(*grown));
                if (!grown) {
                    return CBM_MW_ABSTAIN;
                }
                w->hits = grown;
                w->hit_cap = a->hit_count;
            }
            memcpy(w->hits, a->hits, (size_t)a->hit_count * sizeof(*w->hits));
            w->hit_count = a->hit_count;
            *out = w->hits[0];
            return CBM_MW_FOUND;
        }
        return a->open || a->soft ? CBM_MW_ABSTAIN : CBM_MW_ABSENT;
    }
    if (a->result == MW_HIT) {
        *out = a->hit;
        return CBM_MW_FOUND;
    }
    if (a->result == MW_NONE) {
        return a->open || a->soft ? CBM_MW_ABSTAIN : CBM_MW_ABSENT;
    }
    return CBM_MW_ABSTAIN;
}

/* ── linearisers ────────────────────────────────────────────────── */

bool cbm_lin_own_traits_parent(CBMMemberOrders *o, const CBMRegisteredType *t, int side,
                               CBMMemberSteps *out) {
    (void)o;
    if (!t || !t->qualified_name || !out) {
        return false;
    }
    cbm_member_steps_push(out, CBM_MW_OWN, t->qualified_name, side, CBM_MW_PLACE_OWN);
    if (t->member_flags & CBM_MEMBERS_DECLS_DISAGREE) {
        /* Its own members are known; which ancestry they sit on is not. */
        cbm_member_steps_push(out, CBM_MW_UNSEEN, NULL, side, CBM_MW_PLACE_OWN);
        return true;
    }
    if (t->member_flags & (CBM_MEMBERS_COMPUTED | CBM_MEMBERS_REOPENED_EXTERNAL)) {
        cbm_member_steps_push(out, CBM_MW_UNSEEN, NULL, side, CBM_MW_PLACE_OWN);
    }
    if (t->member_flags & CBM_MEMBERS_OPEN) {
        cbm_member_steps_push(out, CBM_MW_OPEN, NULL, side, CBM_MW_PLACE_OWN);
    }
    int traits = 0;
    int parents = 0;
    const CBMSupertype *parent = NULL;
    for (int i = 0; i < t->supertype_count; i++) {
        const CBMSupertype *s = &t->supertypes[i];
        if (s->kind == CBM_SUPER_TRAIT) {
            traits++;
        } else if (s->kind == CBM_SUPER_EXTENDS && !t->is_interface) {
            parents++;
            parent = s;
        }
    }
    if (traits > 0) {
        /* Traits: one place, a valid program has at most one provider of a
         * name (or an `insteadof` rename picks it). */
        cbm_member_steps_group(out, t->qualified_name, traits, CBM_MW_GROUP_EXCLUSIVE);
        for (int i = 0; i < t->supertype_count; i++) {
            const CBMSupertype *s = &t->supertypes[i];
            if (s->kind != CBM_SUPER_TRAIT) {
                continue;
            }
            if (s->state == CBM_SUPER_KNOWN && s->type_qn) {
                cbm_member_steps_push(out, CBM_MW_SUB, s->type_qn, side, CBM_MW_PLACE_MIXIN);
            } else {
                /* A trait nobody can see: passed, what follows only possible. */
                cbm_member_steps_soft(out, CBM_MW_UNSEEN, side, CBM_MW_PLACE_MIXIN);
            }
        }
    }
    /* Interfaces (an interface's own parents too): declarations only, searched
     * after the class chain. */
    for (int i = 0; i < t->supertype_count; i++) {
        const CBMSupertype *s = &t->supertypes[i];
        bool iface =
            s->kind == CBM_SUPER_IMPLEMENTS || (s->kind == CBM_SUPER_EXTENDS && t->is_interface);
        if (!iface) {
            continue;
        }
        if (s->state == CBM_SUPER_KNOWN && s->type_qn) {
            cbm_member_steps_push(out, CBM_MW_LATER, s->type_qn, side,
                                  CBM_MW_PLACE_INTERFACE_DEFAULT);
        } else {
            cbm_member_steps_soft(out, CBM_MW_LATER, side, CBM_MW_PLACE_INTERFACE_DEFAULT);
        }
    }
    if (parents == 0) {
        cbm_member_steps_push(out, CBM_MW_STOP, NULL, side, CBM_MW_PLACE_OWN);
    } else if (parents == 1 && parent->state == CBM_SUPER_KNOWN && parent->type_qn) {
        cbm_member_steps_push(out, CBM_MW_NEXT, parent->type_qn, side, CBM_MW_PLACE_SUPER);
    } else {
        /* A parent nobody can see (or two): passed; the interfaces' declarations
         * still follow, as possible answers; nothing is absent. */
        cbm_member_steps_soft(out, CBM_MW_UNSEEN, side, CBM_MW_PLACE_SUPER);
        cbm_member_steps_push(out, CBM_MW_STOP, NULL, side, CBM_MW_PLACE_OWN);
    }
    return true;
}
