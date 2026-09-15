/*
 * result_compact.c — copy a finished CBMFileResult into one exact-size arena.
 *
 * Why this exists (Go corpus census, 2026-09-13): extraction writes every
 * temporary into the result arena — cbm_node_text copies at 496 call sites,
 * per-node QN sprintf, enclosing-QN strings — and GROW_ARRAY leaves each
 * previous generation of every record array dead behind it. 14.8 GB written,
 * 3.4 GB reachable, and all of it retained until after resolve because the
 * result owned the arena. Compaction walks what is reachable, measures it,
 * copies it into a single block of exactly that size (strings interned by
 * content within the file), and destroys the working arena.
 *
 * Three passes over one traversal:
 *   COUNT   — number of string references, to size the intern table
 *   MEASURE — bytes every allocation will take (aligned like the arena does)
 *   COPY    — the same allocations, for real, into the fresh arena
 * MEASURE and COPY issue identical allocation sequences, so the block fits
 * exactly; any failure leaves the result untouched.
 */

#include "cbm.h"
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"
#include "result_spill.h" /* cbm_result_relocate */

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

enum { CR_ALIGN = 7, CR_MIN_TABLE = 64, CR_TABLE_LOAD = 2 };

typedef enum { CR_COUNT = 0, CR_MEASURE, CR_COPY, CR_RELOCATE } cr_phase_t;

typedef struct {
    const char *src; /* NULL = empty slot */
    char *dst;       /* copy in the new arena; NULL until COPY reaches it */
    uint64_t hash;
    size_t len; /* strlen(src) */
} cr_slot_t;

typedef struct {
    cr_phase_t phase;
    size_t refs;   /* COUNT: string references seen */
    size_t bytes;  /* MEASURE: total arena bytes */
    CBMArena *dst; /* COPY: the fresh arena */
    bool failed;
    cr_slot_t *slots;
    size_t cap; /* power of two */
    /* RELOCATE: pointers in [old_base, old_base + old_len) move by delta. */
    const char *old_base;
    size_t old_len;
    ptrdiff_t delta;
} cr_ctx_t;

static bool cr_in_old_block(const cr_ctx_t *c, const void *p) {
    const char *cp = (const char *)p;
    return cp >= c->old_base && cp < c->old_base + c->old_len;
}

static size_t cr_aligned(size_t n) {
    return (n + CR_ALIGN) & ~(size_t)CR_ALIGN;
}

static uint64_t cr_hash(const char *s, size_t len) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= (unsigned char)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

/* Find or insert the slot for s. Never fails: the table is sized from COUNT
 * at half load, and the same references are presented again in COPY. */
static cr_slot_t *cr_slot(cr_ctx_t *c, const char *s) {
    size_t len = strlen(s);
    uint64_t h = cr_hash(s, len);
    size_t mask = c->cap - SKIP_ONE;
    size_t i = (size_t)h & mask;
    for (;;) {
        cr_slot_t *slot = &c->slots[i];
        if (!slot->src) {
            slot->src = s;
            slot->hash = h;
            slot->len = len;
            slot->dst = NULL;
            return slot;
        }
        if (slot->hash == h && slot->len == len &&
            (slot->src == s || memcmp(slot->src, s, len) == 0)) {
            return slot;
        }
        i = (i + SKIP_ONE) & mask;
    }
}

/* A raw allocation of `bytes` in the new arena: MEASURE books it, COPY makes
 * it. Zero bytes is no allocation (the arena returns NULL for it too). */
static void *cr_alloc(cr_ctx_t *c, size_t bytes) {
    if (bytes == 0) {
        return NULL;
    }
    if (c->phase == CR_MEASURE) {
        c->bytes += cr_aligned(bytes);
        return NULL;
    }
    if (c->phase == CR_COPY) {
        void *p = cbm_arena_alloc(c->dst, bytes);
        if (!p) {
            c->failed = true;
        }
        return p;
    }
    return NULL;
}

/* A string field: interned by content. COPY rewrites the field. */
static void cr_str(cr_ctx_t *c, const char **field) {
    const char *s = *field;
    if (!s) {
        return;
    }
    if (c->phase == CR_RELOCATE) {
        if (cr_in_old_block(c, s)) {
            *field = s + c->delta;
        }
        return;
    }
    if (c->phase == CR_COUNT) {
        c->refs++;
        return;
    }
    cr_slot_t *slot = cr_slot(c, s);
    if (c->phase == CR_MEASURE) {
        if (!slot->dst) {
            slot->dst = (char *)s; /* mark as booked; reset before COPY */
            c->bytes += cr_aligned(slot->len + SKIP_ONE);
        }
        return;
    }
    if (!slot->dst) {
        char *copy = (char *)cr_alloc(c, slot->len + SKIP_ONE);
        if (!copy) {
            return;
        }
        memcpy(copy, slot->src, slot->len + SKIP_ONE);
        slot->dst = copy;
    }
    *field = slot->dst;
}

/* A blob (fingerprint, retained source): copied verbatim. */
static void cr_blob(cr_ctx_t *c, const void **field, size_t bytes) {
    if (!*field || bytes == 0) {
        return;
    }
    if (c->phase == CR_RELOCATE) {
        if (cr_in_old_block(c, *field)) {
            *field = (const char *)*field + c->delta;
        }
        return;
    }
    if (c->phase == CR_COPY) {
        void *copy = cr_alloc(c, bytes);
        if (!copy) {
            return;
        }
        memcpy(copy, *field, bytes);
        *field = copy;
    } else {
        (void)cr_alloc(c, bytes);
    }
}

/* A record array: copied at exact count, then the caller walks its fields. */
static void cr_array(cr_ctx_t *c, void **items, int count, size_t elem) {
    if (!*items || count <= 0) {
        if (c->phase == CR_COPY) {
            *items = NULL;
        }
        return;
    }
    if (c->phase == CR_RELOCATE) {
        if (cr_in_old_block(c, *items)) {
            *items = (char *)*items + c->delta;
        }
        return;
    }
    cr_blob(c, (const void **)items, (size_t)count * elem);
}

/* A NULL-terminated list of strings: the pointer array plus each string. */
static void cr_list(cr_ctx_t *c, const char ***field) {
    const char **list = *field;
    if (!list) {
        return;
    }
    int n = 0;
    if (c->phase == CR_RELOCATE) {
        /* The array may live in a block we can no longer read at its old
         * address: relocate the pointer first, then count and walk it. */
        cr_blob(c, (const void **)field, sizeof(char *));
        list = *field;
        while (list[n]) {
            n++;
        }
        for (int i = 0; i < n; i++) {
            cr_str(c, &list[i]);
        }
        return;
    }
    while (list[n]) {
        n++;
    }
    cr_blob(c, (const void **)field, (size_t)(n + SKIP_ONE) * sizeof(char *));
    const char **walk = *field; /* the copy in COPY, the original otherwise */
    for (int i = 0; i < n && walk; i++) {
        cr_str(c, &walk[i]);
    }
}

/* A counted list of strings (signature_param_types). */
static void cr_counted_list(cr_ctx_t *c, const char ***field, int count) {
    if (!*field || count <= 0) {
        return;
    }
    cr_blob(c, (const void **)field, (size_t)count * sizeof(char *));
    /* (RELOCATE: the pointer moved; the walk below now reads the new array.) */
    const char **walk = *field;
    for (int i = 0; i < count && walk; i++) {
        cr_str(c, &walk[i]);
    }
}

static void cr_walk_def(cr_ctx_t *c, CBMDefinition *d) {
    cr_str(c, &d->name);
    cr_str(c, &d->qualified_name);
    cr_str(c, &d->label);
    cr_str(c, &d->file_path);
    cr_str(c, &d->signature);
    cr_str(c, &d->return_type);
    cr_str(c, &d->receiver);
    cr_str(c, &d->docstring);
    cr_str(c, &d->parent_class);
    cr_list(c, &d->decorators);
    cr_list(c, &d->base_classes);
    cr_list(c, &d->param_names);
    cr_list(c, &d->param_types);
    cr_counted_list(c, &d->signature_param_types, d->signature_param_count);
    cr_list(c, &d->return_types);
    cr_str(c, &d->route_path);
    cr_str(c, &d->route_method);
    cr_blob(c, (const void **)&d->fingerprint,
            d->fingerprint_k > 0 ? (size_t)d->fingerprint_k * sizeof(uint32_t) : 0);
    cr_str(c, &d->structural_profile);
    cr_str(c, &d->body_tokens);
    cr_str(c, &d->impl_trait);
}

static void cr_walk_call(cr_ctx_t *c, CBMCall *call) {
    cr_str(c, &call->callee_name);
    cr_str(c, &call->enclosing_func_qn);
    cr_str(c, &call->first_string_arg);
    cr_str(c, &call->second_arg_name);
    int argc = call->arg_count;
    if (argc > CBM_MAX_CALL_ARGS) {
        argc = CBM_MAX_CALL_ARGS;
    }
    if (call->args && argc > 0) {
        cr_array(c, (void **)&call->args, argc, sizeof(CBMCallArg));
        for (int i = 0; i < argc && call->args; i++) {
            cr_str(c, &call->args[i].expr);
            cr_str(c, &call->args[i].value);
            cr_str(c, &call->args[i].keyword);
        }
    } else if (c->phase == CR_COPY) {
        call->args = NULL;
        call->arg_count = 0;
    }
}

static void cr_walk(cr_ctx_t *c, CBMFileResult *r) {
    cr_array(c, (void **)&r->defs.items, r->defs.count, sizeof(CBMDefinition));
    for (int i = 0; i < r->defs.count && r->defs.items; i++) {
        cr_walk_def(c, &r->defs.items[i]);
    }
    cr_array(c, (void **)&r->calls.items, r->calls.count, sizeof(CBMCall));
    for (int i = 0; i < r->calls.count && r->calls.items; i++) {
        cr_walk_call(c, &r->calls.items[i]);
    }
    cr_array(c, (void **)&r->imports.items, r->imports.count, sizeof(CBMImport));
    for (int i = 0; i < r->imports.count && r->imports.items; i++) {
        cr_str(c, &r->imports.items[i].local_name);
        cr_str(c, &r->imports.items[i].module_path);
    }
    cr_array(c, (void **)&r->usages.items, r->usages.count, sizeof(CBMUsage));
    for (int i = 0; i < r->usages.count && r->usages.items; i++) {
        cr_str(c, &r->usages.items[i].ref_name);
        cr_str(c, &r->usages.items[i].enclosing_func_qn);
    }
    cr_array(c, (void **)&r->throws.items, r->throws.count, sizeof(CBMThrow));
    for (int i = 0; i < r->throws.count && r->throws.items; i++) {
        cr_str(c, &r->throws.items[i].exception_name);
        cr_str(c, &r->throws.items[i].enclosing_func_qn);
    }
    cr_array(c, (void **)&r->rw.items, r->rw.count, sizeof(CBMReadWrite));
    for (int i = 0; i < r->rw.count && r->rw.items; i++) {
        cr_str(c, &r->rw.items[i].var_name);
        cr_str(c, &r->rw.items[i].enclosing_func_qn);
    }
    cr_array(c, (void **)&r->type_refs.items, r->type_refs.count, sizeof(CBMTypeRef));
    for (int i = 0; i < r->type_refs.count && r->type_refs.items; i++) {
        cr_str(c, &r->type_refs.items[i].type_name);
        cr_str(c, &r->type_refs.items[i].enclosing_func_qn);
    }
    cr_array(c, (void **)&r->env_accesses.items, r->env_accesses.count, sizeof(CBMEnvAccess));
    for (int i = 0; i < r->env_accesses.count && r->env_accesses.items; i++) {
        cr_str(c, &r->env_accesses.items[i].env_key);
        cr_str(c, &r->env_accesses.items[i].enclosing_func_qn);
    }
    cr_array(c, (void **)&r->type_assigns.items, r->type_assigns.count, sizeof(CBMTypeAssign));
    for (int i = 0; i < r->type_assigns.count && r->type_assigns.items; i++) {
        cr_str(c, &r->type_assigns.items[i].var_name);
        cr_str(c, &r->type_assigns.items[i].type_name);
        cr_str(c, &r->type_assigns.items[i].enclosing_func_qn);
    }
    cr_array(c, (void **)&r->impl_traits.items, r->impl_traits.count, sizeof(CBMImplTrait));
    for (int i = 0; i < r->impl_traits.count && r->impl_traits.items; i++) {
        cr_str(c, &r->impl_traits.items[i].trait_name);
        cr_str(c, &r->impl_traits.items[i].struct_name);
        cr_str(c, &r->impl_traits.items[i].struct_qn);
    }
    cr_array(c, (void **)&r->resolved_calls.items, r->resolved_calls.count,
             sizeof(CBMResolvedCall));
    for (int i = 0; i < r->resolved_calls.count && r->resolved_calls.items; i++) {
        cr_str(c, &r->resolved_calls.items[i].caller_qn);
        cr_str(c, &r->resolved_calls.items[i].callee_qn);
        cr_str(c, &r->resolved_calls.items[i].strategy);
        cr_str(c, &r->resolved_calls.items[i].reason);
    }
    cr_array(c, (void **)&r->string_refs.items, r->string_refs.count, sizeof(CBMStringRef));
    for (int i = 0; i < r->string_refs.count && r->string_refs.items; i++) {
        cr_str(c, &r->string_refs.items[i].value);
        cr_str(c, &r->string_refs.items[i].enclosing_func_qn);
        cr_str(c, &r->string_refs.items[i].key_path);
    }
    cr_array(c, (void **)&r->infra_bindings.items, r->infra_bindings.count,
             sizeof(CBMInfraBinding));
    for (int i = 0; i < r->infra_bindings.count && r->infra_bindings.items; i++) {
        cr_str(c, &r->infra_bindings.items[i].source_name);
        cr_str(c, &r->infra_bindings.items[i].target_url);
        cr_str(c, &r->infra_bindings.items[i].broker);
    }
    cr_array(c, (void **)&r->channels.items, r->channels.count, sizeof(CBMChannel));
    for (int i = 0; i < r->channels.count && r->channels.items; i++) {
        cr_str(c, &r->channels.items[i].channel_name);
        cr_str(c, &r->channels.items[i].transport);
        cr_str(c, &r->channels.items[i].enclosing_func_qn);
    }
    cr_str(c, &r->module_qn);
    cr_str(c, &r->namespace_name);
    cr_list(c, &r->exports);
    cr_list(c, &r->constants);
    cr_list(c, &r->global_vars);
    cr_list(c, &r->macros);
    cr_str(c, &r->error_msg);
    cr_str(c, &r->error_ranges);
    cr_blob(c, (const void **)&r->source, r->source ? (size_t)r->source_len + SKIP_ONE : 0);
}

void cbm_result_relocate(CBMFileResult *result, const char *old_base, size_t len, char *new_base) {
    if (!result || !old_base || !new_base || len == 0 || old_base == new_base) {
        return;
    }
    cr_ctx_t c;
    memset(&c, 0, sizeof(c));
    c.phase = CR_RELOCATE;
    c.old_base = old_base;
    c.old_len = len;
    c.delta = new_base - old_base;
    cr_walk(&c, result);
}

static size_t cr_pow2_at_least(size_t n) {
    size_t cap = CR_MIN_TABLE;
    while (cap < n) {
        cap *= PAIR_LEN;
    }
    return cap;
}

void cbm_result_compact(CBMFileResult *result) {
    if (!result || result->arena.nblocks == 0) {
        return;
    }
    cr_ctx_t c;
    memset(&c, 0, sizeof(c));

    /* Work on a copy of the header: every pointer rewrite lands here and the
     * caller's result is replaced only once everything succeeded. */
    CBMFileResult tmp = *result;

    c.phase = CR_COUNT;
    cr_walk(&c, &tmp);

    c.cap = cr_pow2_at_least(c.refs * CR_TABLE_LOAD + CR_MIN_TABLE);
    c.slots = (cr_slot_t *)cbm_calloc(CBM_MEM_CLASS_EXTRACT, c.cap * sizeof(cr_slot_t));
    if (!c.slots) {
        return;
    }

    c.phase = CR_MEASURE;
    cr_walk(&c, &tmp);
    for (size_t i = 0; i < c.cap; i++) {
        c.slots[i].dst = NULL; /* MEASURE used dst as a booked marker */
    }

    CBMArena fresh;
    cbm_arena_init_exact(&fresh, c.bytes);
    if (fresh.nblocks == 0) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, c.slots);
        return;
    }

    c.phase = CR_COPY;
    c.dst = &fresh;
    cr_walk(&c, &tmp);
    cbm_free(CBM_MEM_CLASS_EXTRACT, c.slots);
    if (c.failed) {
        cbm_arena_destroy(&fresh);
        return;
    }

    /* Exact-count arrays: nothing may append into the dead headroom. */
    tmp.defs.cap = tmp.defs.count;
    tmp.calls.cap = tmp.calls.count;
    tmp.imports.cap = tmp.imports.count;
    tmp.usages.cap = tmp.usages.count;
    tmp.throws.cap = tmp.throws.count;
    tmp.rw.cap = tmp.rw.count;
    tmp.type_refs.cap = tmp.type_refs.count;
    tmp.env_accesses.cap = tmp.env_accesses.count;
    tmp.type_assigns.cap = tmp.type_assigns.count;
    tmp.impl_traits.cap = tmp.impl_traits.count;
    tmp.resolved_calls.cap = tmp.resolved_calls.count;
    tmp.string_refs.cap = tmp.string_refs.count;
    tmp.infra_bindings.cap = tmp.infra_bindings.count;
    tmp.channels.cap = tmp.channels.count;

    /* A composite kept its per-unit results only so shallow-copied strings
     * stayed valid; every string is now a copy of its own. */
    cbm_result_release_owned(result);
    tmp.owned_results = NULL;
    tmp.owned_result_count = 0;

    cbm_work_arena_give(&result->arena); /* kept for this thread's next file */
    tmp.arena = fresh;
    *result = tmp;
}
