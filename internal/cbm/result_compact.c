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
#include "foundation/compat.h" /* CBM_TLS */
#include "foundation/constants.h"
#include "foundation/mem_core.h"
#include "result_spill.h" /* cbm_result_relocate */
#include "resolve/proof.h"

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
    const cbm_resolve_proof_t *source;
    cbm_resolve_proof_t *destination;
    bool complete;
} cr_proof_slot_t;

typedef struct {
    cr_phase_t phase;
    size_t refs;   /* COUNT: string references seen */
    size_t bytes;  /* MEASURE: total arena bytes */
    CBMArena *dst; /* COPY: the fresh arena */
    bool failed;
    cr_slot_t *slots;
    size_t cap; /* power of two */
    /* MEASURE records which slot each string reference landed in; COPY, which
     * presents the very same references in the same order, replays them
     * instead of measuring, hashing and comparing every string a second time
     * (20 M repeated strlen + 31 M memcmp on the Go corpus, waste sanitizer
     * 2026-09-17). */
    uint32_t *seq;
    size_t seq_len;
    size_t seq_pos;
    /* RELOCATE: pointers in [old_base, old_base + old_len) move by delta. */
    const char *old_base;
    size_t old_len;
    ptrdiff_t delta;
    cr_proof_slot_t *proof_slots;
    size_t proof_capacity, proof_count;
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
    if (c->failed)
        return NULL;
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
    if (c->failed)
        return;
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
    cr_slot_t *slot;
    if (c->phase == CR_COPY && c->seq && c->seq_pos < c->seq_len) {
        slot = &c->slots[c->seq[c->seq_pos++]];
    } else {
        slot = cr_slot(c, s);
    }
    if (c->phase == CR_MEASURE) {
        if (c->seq && c->seq_len < c->refs) {
            c->seq[c->seq_len++] = (uint32_t)(slot - c->slots);
        }
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
    if (c->failed)
        return;
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
    if (c->failed)
        return;
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
    cr_str(c, &d->http_client);
    cr_str(c, &d->http_base_url);
    cr_str(c, &d->variants);
    cr_list(c, &d->extra_props);
    cr_str(c, &d->decl_namespace);
    cr_str(c, &d->variant_key);
    cr_str(c, &d->alias_target);
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
            cr_str(c, &call->args[i].url_value);
            cr_str(c, &call->args[i].keyword);
        }
    } else if (c->phase == CR_COPY) {
        call->args = NULL;
        call->arg_count = 0;
    }
}

/* Facts have counted lists, not sentinel arrays. Bail before touching a failed
 * array copy so no string rewrite can land in the original result's storage. */
static bool cr_py_size(cr_ctx_t *c, size_t size) {
    if (c->failed)
        return false;
    if (c->phase == CR_MEASURE &&
        (size > SIZE_MAX - CR_ALIGN || c->bytes > SIZE_MAX - cr_aligned(size))) {
        c->failed = true;
        return false;
    }
    return true;
}

static bool cr_py_string(cr_ctx_t *c, const char **field) {
    if (!*field)
        return true;
    if (c->phase == CR_COUNT && c->refs == SIZE_MAX) {
        c->failed = true;
        return false;
    }
    if (c->phase == CR_MEASURE && !cr_py_size(c, strlen(*field) + 1))
        return false;
    cr_str(c, field);
    return !c->failed;
}

static void cr_walk_py_namespace(cr_ctx_t *c, CBMPyNamespaceFacts *facts) {
    if (!cr_py_size(c, (size_t)facts->count * sizeof(*facts->items)))
        return;
    cr_array(c, (void **)&facts->items, facts->count, sizeof(*facts->items));
    if (c->failed)
        return;
    for (int i = 0; i < facts->count; i++) {
        CBMPyNamespaceFact *f = &facts->items[i];
        if (!cr_py_string(c, &f->local_name) || !cr_py_string(c, &f->module_name) ||
            !cr_py_string(c, &f->member_name) ||
            !cr_py_size(c, (size_t)f->name_count * sizeof(*f->names)))
            return;
        cr_array(c, (void **)&f->names, f->name_count, sizeof(*f->names));
        if (c->failed)
            return;
        for (int j = 0; j < f->name_count; j++) {
            if (!cr_py_string(c, &f->names[j]))
                return;
        }
    }
}

enum { CR_PROOF_MAX_NODES = 65536, CR_PROOF_MAX_DEPTH = 256 };

static void cr_proofs_clear(cr_ctx_t *c) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, c->proof_slots);
    c->proof_slots = NULL;
    c->proof_capacity = c->proof_count = 0;
}
static void cr_proofs_rewind(cr_ctx_t *c) {
    if (c->proof_slots)
        memset(c->proof_slots, 0, c->proof_capacity * sizeof(*c->proof_slots));
    c->proof_count = 0;
}
static size_t cr_proof_hash(const cbm_resolve_proof_t *proof) {
    uintptr_t value = (uintptr_t)proof >> 3;
    return (size_t)(value ^ (value >> 17));
}
static cr_proof_slot_t *cr_proof_slot(cr_ctx_t *c, const cbm_resolve_proof_t *proof) {
    size_t slot = cr_proof_hash(proof) & (c->proof_capacity - 1);
    while (c->proof_slots[slot].source && c->proof_slots[slot].source != proof)
        slot = (slot + 1) & (c->proof_capacity - 1);
    return &c->proof_slots[slot];
}
static bool cr_proofs_grow(cr_ctx_t *c) {
    if (c->proof_capacity && c->proof_count < c->proof_capacity / 2)
        return true;
    size_t capacity = c->proof_capacity ? c->proof_capacity * 2 : CR_MIN_TABLE;
    if (capacity < c->proof_capacity || capacity > SIZE_MAX / sizeof(*c->proof_slots)) {
        c->failed = true;
        return false;
    }
    cr_proof_slot_t *slots = cbm_calloc(CBM_MEM_CLASS_EXTRACT, capacity * sizeof(*slots));
    if (!slots) {
        c->failed = true;
        return false;
    }
    cr_proof_slot_t *old = c->proof_slots;
    size_t old_capacity = c->proof_capacity;
    c->proof_slots = slots;
    c->proof_capacity = capacity;
    for (size_t i = 0; i < old_capacity; i++)
        if (old[i].source)
            *cr_proof_slot(c, old[i].source) = old[i];
    cbm_free(CBM_MEM_CLASS_EXTRACT, old);
    return true;
}
static bool cr_proof_range(const cr_ctx_t *c, const void *pointer, size_t bytes) {
    uintptr_t value = (uintptr_t)pointer, base = (uintptr_t)c->old_base;
    return value >= base && value - base <= c->old_len && bytes <= c->old_len - (value - base);
}
static bool cr_proof_string(cr_ctx_t *c, const char **field) {
    if (c->phase == CR_RELOCATE && *field) {
        if (!cr_proof_range(c, *field, 1)) {
            c->failed = true;
            return false;
        }
        size_t remaining = c->old_len - ((uintptr_t)*field - (uintptr_t)c->old_base);
        if (!memchr(*field + c->delta, 0, remaining)) {
            c->failed = true;
            return false;
        }
    }
    return cr_py_string(c, field);
}
static bool cr_proof_strings(cr_ctx_t *c, cbm_resolve_proof_view_t *v) {
    const char **fields[] = {&v->source.project,
                             &v->source.file,
                             &v->source.module,
                             &v->source.scope,
                             &v->source.content_identity,
                             &v->source.configuration_identity,
                             &v->key.lookup_domain,
                             &v->key.lookup_key,
                             &v->key.declaration_identity,
                             &v->key.binding_identity,
                             &v->key.type_identity,
                             &v->key.detail,
                             &v->outcome.target,
                             &v->outcome.reason};
    for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); i++)
        if (!cr_proof_string(c, fields[i]))
            return false;
    return true;
}

/* One memo spans every result root in each pass. Only COPY/RELOCATE rewrite
 * pointers; COUNT/MEASURE never touch immutable input nodes. Shared supports
 * remain shared, and an incomplete traversal is never published. */
static void cr_proof(cr_ctx_t *c, const cbm_resolve_proof_t **field, unsigned depth) {
    const cbm_resolve_proof_t *source = *field;
    if (!source || c->failed)
        return;
    if (depth > CR_PROOF_MAX_DEPTH || !cr_proofs_grow(c)) {
        c->failed = true;
        return;
    }
    cr_proof_slot_t *slot = cr_proof_slot(c, source);
    if (slot->source) {
        if (!slot->complete) {
            c->failed = true; /* cyclic/in-progress support */
            return;
        }
        if (c->phase == CR_COPY || c->phase == CR_RELOCATE)
            *field = slot->destination;
        return;
    }
    if (c->proof_count == CR_PROOF_MAX_NODES ||
        (c->phase == CR_RELOCATE && !cr_proof_range(c, source, sizeof(*source)))) {
        c->failed = true;
        return;
    }
    slot->source = source;
    c->proof_count++;
    cbm_resolve_proof_t *node = (cbm_resolve_proof_t *)source;
    if (!cr_py_size(c, sizeof(*source)))
        return;
    if (c->phase == CR_COPY) {
        node = cr_alloc(c, sizeof(*node));
        if (!node)
            return;
        *node = *source;
        *field = node;
    } else if (c->phase == CR_RELOCATE) {
        node = (cbm_resolve_proof_t *)((char *)source + c->delta);
        *field = node;
    } else {
        (void)cr_alloc(c, sizeof(*node));
    }
    slot->destination = node;
    cbm_resolve_proof_view_t *v = &node->value;
    if (!v->depth || v->depth > CR_PROOF_MAX_DEPTH || v->outcome.proof ||
        v->premise_count > CR_PROOF_MAX_NODES || (v->premise_count && !v->premises) ||
        (!v->premise_count && v->premises) || v->premise_count > SIZE_MAX / sizeof(*v->premises)) {
        c->failed = true;
        return;
    }
    size_t bytes = v->premise_count * sizeof(*v->premises);
    if (!cr_py_size(c, bytes) ||
        (c->phase == CR_RELOCATE && bytes && !cr_proof_range(c, v->premises, bytes))) {
        c->failed = true;
        return;
    }
    cr_blob(c, (const void **)&v->premises, bytes);
    if (c->failed || !cr_proof_strings(c, v))
        return;
    const cbm_resolve_proof_t **premises = (const cbm_resolve_proof_t **)v->premises;
    for (size_t i = 0; i < v->premise_count && !c->failed; i++)
        cr_proof(c, &premises[i], depth + 1);
    cr_proof_slot(c, source)->complete = !c->failed;
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
        cr_str(c, &r->imports.items[i].member);
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
        cr_str(c, &r->resolved_calls.items[i].callee_sig);
        if (!c->failed)
            cr_proof(c, &r->resolved_calls.items[i].evidence.proof, 1);
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
    cr_array(c, (void **)&r->diagram_arrows.items, r->diagram_arrows.count,
             sizeof(CBMDiagramArrow));
    for (int i = 0; i < r->diagram_arrows.count && r->diagram_arrows.items; i++) {
        cr_str(c, &r->diagram_arrows.items[i].from_qn);
        cr_str(c, &r->diagram_arrows.items[i].to_qn);
        cr_str(c, &r->diagram_arrows.items[i].label);
        cr_str(c, &r->diagram_arrows.items[i].style);
    }
    cr_array(c, (void **)&r->channels.items, r->channels.count, sizeof(CBMChannel));
    for (int i = 0; i < r->channels.count && r->channels.items; i++) {
        cr_str(c, &r->channels.items[i].channel_name);
        cr_str(c, &r->channels.items[i].transport);
        cr_str(c, &r->channels.items[i].enclosing_func_qn);
    }
    cr_array(c, (void **)&r->field_types.items, r->field_types.count, sizeof(CBMFieldType));
    for (int i = 0; i < r->field_types.count && r->field_types.items; i++) {
        cr_str(c, &r->field_types.items[i].class_qn);
        cr_str(c, &r->field_types.items[i].field_name);
        cr_str(c, &r->field_types.items[i].type_text);
    }
    cr_str(c, &r->module_qn);
    cr_str(c, &r->source_project);
    cr_str(c, &r->source_file);
    cr_str(c, &r->namespace_name);
    cr_list(c, &r->exports);
    cr_list(c, &r->constants);
    cr_list(c, &r->global_vars);
    cr_list(c, &r->macros);
    cr_str(c, &r->error_msg);
    cr_str(c, &r->error_ranges);
    cr_str(c, &r->module_doc);
    cr_array(c, (void **)&r->doc_links.items, r->doc_links.count, sizeof(CBMDocLink));
    for (int i = 0; i < r->doc_links.count && r->doc_links.items; i++) {
        cr_str(c, &r->doc_links.items[i].source_qn);
        cr_str(c, &r->doc_links.items[i].raw);
    }
    cr_str(c, &r->doc_scope);
    cr_blob(c, (const void **)&r->source, r->source ? (size_t)r->source_len + SKIP_ONE : 0);
    cr_walk_py_namespace(c, &r->py_namespace);
}

bool cbm_result_relocate(CBMFileResult *result, const char *old_base, size_t len, char *new_base) {
    if (!result || !old_base || !new_base)
        return false;
    if (len == 0)
        return result->resolved_calls.count == 0;
    cr_ctx_t c;
    memset(&c, 0, sizeof(c));
    c.phase = CR_RELOCATE;
    c.old_base = old_base;
    c.old_len = len;
    c.delta = new_base - old_base;
    cr_walk(&c, result);
    cr_proofs_clear(&c);
    return !c.failed;
}

static size_t cr_pow2_at_least(size_t n) {
    size_t cap = CR_MIN_TABLE;
    while (cap < n) {
        cap *= PAIR_LEN;
    }
    return cap;
}

/* The intern table and the replay sequence, kept per pipeline worker: a fresh
 * zeroed table per file was 21,875 allocations and 2.0 GB of pure churn on the
 * Go corpus (waste sanitizer, 2026-09-17). Kept only on threads whose
 * cbm_work_arena_release is guaranteed to run (cbm_work_arena_keeping), and
 * only up to CR_KEEP_BYTES; anything else allocates per call as before. */
enum { CR_KEEP_BYTES = 8 * 1024 * 1024 };
static CBM_TLS void *tl_cr_buf;
static CBM_TLS size_t tl_cr_bytes;

void cbm_result_compact_release_thread(void) {
    cbm_free(CBM_MEM_CLASS_EXTRACT, tl_cr_buf);
    tl_cr_buf = NULL;
    tl_cr_bytes = 0;
}

/* One zeroed block holding the slot table followed by the sequence. Returns
 * whether the block is the kept one (then the caller must not free it). */
static bool cr_scratch_get(cr_ctx_t *c) {
    size_t slot_bytes = c->cap * sizeof(cr_slot_t);
    size_t bytes = slot_bytes + (c->refs * sizeof(uint32_t));
    bool keep = bytes <= (size_t)CR_KEEP_BYTES && cbm_work_arena_keeping();
    void *buf = NULL;
    if (keep && tl_cr_buf && tl_cr_bytes >= bytes) {
        buf = tl_cr_buf;
    } else if (keep) {
        size_t grown = CR_MIN_TABLE; /* powers of two: a growing file mix reallocates rarely */
        while (grown < bytes) {
            grown *= PAIR_LEN;
        }
        if (grown > (size_t)CR_KEEP_BYTES) {
            grown = bytes;
        }
        cbm_result_compact_release_thread();
        tl_cr_buf = cbm_alloc(CBM_MEM_CLASS_EXTRACT, grown);
        tl_cr_bytes = tl_cr_buf ? grown : 0;
        buf = tl_cr_buf;
    } else {
        buf = cbm_alloc(CBM_MEM_CLASS_EXTRACT, bytes);
    }
    if (!buf) {
        return false;
    }
    memset(
        buf, 0,
        slot_bytes); /* MEASURE needs an empty table; the sequence is written before it is read */
    c->slots = (cr_slot_t *)buf;
    c->seq = c->refs ? (uint32_t *)((char *)buf + slot_bytes) : NULL;
    return true;
}

static void cr_scratch_put(cr_ctx_t *c) {
    cr_proofs_clear(c);
    if ((void *)c->slots != tl_cr_buf) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, c->slots);
    }
    c->slots = NULL;
    c->seq = NULL;
}

void cbm_result_compact(CBMFileResult *result) {
    if (!result || result->arena.nblocks == 0 ||
        !cbm_py_namespace_facts_valid(&result->py_namespace)) {
        return;
    }
    cr_ctx_t c;
    memset(&c, 0, sizeof(c));

    /* Work on a copy of the header: every pointer rewrite lands here and the
     * caller's result is replaced only once everything succeeded. */
    CBMFileResult tmp = *result;

    c.phase = CR_COUNT;
    cr_walk(&c, &tmp);
    /* Bound intern-table/replay sizing before the existing power-of-two step. */
    if (c.failed || c.refs > (SIZE_MAX / (sizeof(cr_slot_t) * 8) - CR_MIN_TABLE)) {
        cr_proofs_clear(&c);
        return;
    }

    c.cap = cr_pow2_at_least(c.refs * CR_TABLE_LOAD + CR_MIN_TABLE);
    if (c.cap > UINT32_MAX) { /* replay entries store slot indexes in uint32_t */
        cr_proofs_clear(&c);
        return;
    }
    if (!cr_scratch_get(&c)) {
        cr_proofs_clear(&c);
        return;
    }

    cr_proofs_rewind(&c);
    c.phase = CR_MEASURE;
    cr_walk(&c, &tmp);
    if (c.failed) {
        cr_scratch_put(&c);
        return;
    }
    for (size_t i = 0; i < c.cap; i++) {
        c.slots[i].dst = NULL; /* MEASURE used dst as a booked marker */
    }

    CBMArena fresh;
    cbm_arena_init_exact(&fresh, c.bytes);
    if (fresh.nblocks == 0) {
        cr_scratch_put(&c);
        return;
    }

    cr_proofs_rewind(&c);
    c.phase = CR_COPY;
    c.dst = &fresh;
    cr_walk(&c, &tmp);
    cr_scratch_put(&c);
    if (c.failed) {
        cbm_arena_destroy(&fresh);
        return;
    }

    /* Exact-count arrays: nothing may append into the dead headroom. */
    tmp.defs.cap = tmp.defs.count;
    tmp.calls.cap = tmp.calls.count;
    tmp.imports.cap = tmp.imports.count;
    tmp.py_namespace.cap = tmp.py_namespace.count;
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
    tmp.diagram_arrows.cap = tmp.diagram_arrows.count;
    tmp.field_types.cap = tmp.field_types.count;
    tmp.doc_links.cap = tmp.doc_links.count;

    /* A composite kept its per-unit results only so shallow-copied strings
     * stayed valid; every string is now a copy of its own. */
    cbm_result_release_owned(result);
    tmp.owned_results = NULL;
    tmp.owned_result_count = 0;

    cbm_work_arena_give(&result->arena); /* kept for this thread's next file */
    tmp.arena = fresh;
    *result = tmp;
}
