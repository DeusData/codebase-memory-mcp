#include "proof.h"
#include "policy.h"
#include <stdlib.h>
#include <string.h>

#define PROOF_MAX_DEPTH 256u
#define PROOF_DEFAULT_NODES 65536u
#define PROOF_DEFAULT_PREMISES 65536u
#define PROOF_DEFAULT_WORK UINT64_C(4194304)

struct cbm_resolve_proof_entry {
    cbm_resolve_proof_t node;
    struct cbm_resolve_proof_entry *next;
};

typedef struct {
    const cbm_resolve_proof_t *source;
    const cbm_resolve_proof_t *destination;
} proof_memo_slot_t;
typedef struct {
    proof_memo_slot_t *slots;
    size_t capacity, count;
    cbm_resolve_proof_pool_t *pool;
    cbm_resolve_proof_visit_fn visit;
    void *context;
    size_t limit;
    cbm_resolve_analysis_t failure;
} proof_walk_t;

static const char *text_or_empty(const char *text) {
    return text ? text : "";
}
static bool proof_fail(cbm_resolve_proof_pool_t *pool, cbm_resolve_analysis_t state) {
    if (pool && pool->failure == CBM_RESOLVE_ANALYSIS_COMPLETE)
        pool->failure = state;
    return false;
}
static bool proof_step(cbm_resolve_proof_pool_t *pool, uint64_t amount) {
    if (!pool || pool->failure != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return false;
    if (pool->work > pool->work_limit || amount > pool->work_limit - pool->work)
        return proof_fail(pool, CBM_RESOLVE_ANALYSIS_LIMITED);
    pool->work += amount;
    return true;
}
void cbm_resolve_proof_pool_init(cbm_resolve_proof_pool_t *pool, CBMArena *arena, size_t node_limit,
                                 size_t premise_limit, uint64_t work_limit) {
    if (!pool)
        return;
    *pool = (cbm_resolve_proof_pool_t){
        .arena = arena,
        .node_limit = node_limit ? node_limit : PROOF_DEFAULT_NODES,
        .premise_limit = premise_limit ? premise_limit : PROOF_DEFAULT_PREMISES,
        .work_limit = work_limit ? work_limit : PROOF_DEFAULT_WORK,
        .failure = arena ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_FAILED};
}
const cbm_resolve_proof_view_t *cbm_resolve_proof_view(const cbm_resolve_proof_t *proof) {
    return proof ? &proof->value : NULL;
}

/* Both hashing and equality cover every logical payload field. Pointer values,
 * padding, allocation order and the outcome's convenience proof link do not. */
static size_t proof_strings(const cbm_resolve_proof_view_t *v, const char **strings) {
    const char *values[] = {v->source.project,
                            v->source.file,
                            v->source.module,
                            v->source.scope,
                            v->source.content_identity,
                            v->source.configuration_identity,
                            v->key.lookup_domain,
                            v->key.lookup_key,
                            v->key.declaration_identity,
                            v->key.binding_identity,
                            v->key.type_identity,
                            v->key.detail,
                            v->outcome.target,
                            v->outcome.reason};
    memcpy(strings, values, sizeof(values));
    return sizeof(values) / sizeof(values[0]);
}
static size_t proof_numbers(const cbm_resolve_proof_view_t *v, uint64_t *numbers) {
    uint64_t values[] = {(uint64_t)v->kind,
                         v->source.source_origin,
                         v->key.start_byte,
                         v->key.end_byte,
                         v->key.origin,
                         v->key.flags,
                         (uint64_t)v->key.lookup_closed,
                         (uint64_t)v->outcome.binding,
                         (uint64_t)v->outcome.domain,
                         (uint64_t)v->outcome.analysis,
                         (uint64_t)v->outcome.rule,
                         v->outcome.facts,
                         v->outcome.candidate_count,
                         v->premise_count,
                         (uint64_t)v->predecessors_closed};
    memcpy(numbers, values, sizeof(values));
    return sizeof(values) / sizeof(values[0]);
}
static uint64_t hash_number(uint64_t hash, uint64_t value) {
    for (unsigned i = 0; i < 8; i++) {
        hash ^= (unsigned char)(value >> (8 * i));
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}
static bool proof_hash(cbm_resolve_proof_pool_t *pool, cbm_resolve_proof_view_t *v) {
    uint64_t hash = UINT64_C(14695981039346656037), numbers[16];
    const char *strings[16];
    size_t count = proof_numbers(v, numbers);
    for (size_t i = 0; i < count; i++)
        hash = hash_number(hash, numbers[i]);
    count = proof_strings(v, strings);
    for (size_t i = 0; i < count; i++) {
        const char *s = text_or_empty(strings[i]);
        size_t length = strlen(s);
        if (!proof_step(pool, length + 1))
            return false;
        hash = hash_number(hash, length);
        for (size_t j = 0; j < length; j++) {
            hash ^= (unsigned char)s[j];
            hash *= UINT64_C(1099511628211);
        }
    }
    for (size_t i = 0; i < v->premise_count; i++)
        hash = hash_number(hash, v->premises[i]->value.fingerprint);
    v->fingerprint = hash;
    return true;
}
static int compare_payload(const cbm_resolve_proof_view_t *a, const cbm_resolve_proof_view_t *b) {
    uint64_t an[16], bn[16];
    const char *as[16], *bs[16];
    size_t count = proof_numbers(a, an);
    (void)proof_numbers(b, bn);
    for (size_t i = 0; i < count; i++)
        if (an[i] != bn[i])
            return an[i] < bn[i] ? -1 : 1;
    count = proof_strings(a, as);
    (void)proof_strings(b, bs);
    for (size_t i = 0; i < count; i++) {
        int order = strcmp(text_or_empty(as[i]), text_or_empty(bs[i]));
        if (order)
            return order;
    }
    return 0;
}
int cbm_resolve_proof_compare(const cbm_resolve_proof_t *a, const cbm_resolve_proof_t *b) {
    if (a == b)
        return 0;
    if (!a || !b)
        return a ? 1 : -1;
    if (a->value.fingerprint != b->value.fingerprint)
        return a->value.fingerprint < b->value.fingerprint ? -1 : 1;
    int order = compare_payload(&a->value, &b->value);
    for (size_t i = 0; !order && i < a->value.premise_count; i++)
        order = cbm_resolve_proof_compare(a->value.premises[i], b->value.premises[i]);
    return order;
}
static int compare_premises(const void *left, const void *right) {
    return cbm_resolve_proof_compare(*(const cbm_resolve_proof_t *const *)left,
                                     *(const cbm_resolve_proof_t *const *)right);
}
static bool same_key(const cbm_resolve_proof_view_t *a, const cbm_resolve_proof_view_t *b) {
    if (a->fingerprint != b->fingerprint || compare_payload(a, b))
        return false;
    /* All premises have already been interned into this pool. Pointer equality
     * here is full structural equality, not an address-based fact identity. */
    for (size_t i = 0; i < a->premise_count; i++)
        if (a->premises[i] != b->premises[i])
            return false;
    return true;
}
bool cbm_resolve_proof_pool_owns(const cbm_resolve_proof_pool_t *pool,
                                 const cbm_resolve_proof_t *proof) {
    if (!pool || !pool->capacity || !proof)
        return false;
    size_t slot = proof->value.fingerprint & (pool->capacity - 1);
    for (cbm_resolve_proof_entry_t *entry = pool->buckets[slot]; entry; entry = entry->next)
        if (&entry->node == proof)
            return true;
    return false;
}
static bool grow_pool(cbm_resolve_proof_pool_t *pool) {
    if (pool->capacity && pool->count < pool->capacity * 2)
        return true;
    size_t capacity = pool->capacity ? pool->capacity * 2 : 32;
    if (capacity < pool->capacity || capacity > SIZE_MAX / sizeof(*pool->buckets))
        return proof_fail(pool, CBM_RESOLVE_ANALYSIS_FAILED);
    cbm_resolve_proof_entry_t **buckets =
        cbm_arena_calloc(pool->arena, capacity * sizeof(*buckets));
    if (!buckets)
        return proof_fail(pool, CBM_RESOLVE_ANALYSIS_FAILED);
    for (size_t i = 0; i < pool->capacity; i++)
        for (cbm_resolve_proof_entry_t *e = pool->buckets[i], *next; e; e = next) {
            next = e->next;
            size_t slot = e->node.value.fingerprint & (capacity - 1);
            e->next = buckets[slot];
            buckets[slot] = e;
        }
    pool->buckets = buckets;
    pool->capacity = capacity;
    return true;
}
static bool own_text(cbm_resolve_proof_pool_t *pool, const char **field) {
    if (!*field || !**field) {
        *field = NULL;
        return true;
    }
    *field = cbm_arena_strdup(pool->arena, *field);
    return *field || proof_fail(pool, CBM_RESOLVE_ANALYSIS_FAILED);
}
static bool own_payload(cbm_resolve_proof_pool_t *pool, cbm_resolve_proof_view_t *v) {
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
        if (!own_text(pool, fields[i]))
            return false;
    return true;
}
static const cbm_resolve_proof_t *intern(cbm_resolve_proof_pool_t *pool,
                                         cbm_resolve_proof_view_t value) {
    if (!proof_step(pool, 1) || !proof_hash(pool, &value) || !grow_pool(pool))
        return NULL;
    size_t slot = value.fingerprint & (pool->capacity - 1);
    for (cbm_resolve_proof_entry_t *e = pool->buckets[slot]; e; e = e->next) {
        if (!proof_step(pool, 1))
            return NULL;
        if (same_key(&e->node.value, &value))
            return &e->node;
    }
    if (pool->count >= pool->node_limit) {
        proof_fail(pool, CBM_RESOLVE_ANALYSIS_LIMITED);
        return NULL;
    }
    cbm_resolve_proof_entry_t *entry = cbm_arena_calloc(pool->arena, sizeof(*entry));
    if (!entry || !own_payload(pool, &value)) {
        proof_fail(pool, CBM_RESOLVE_ANALYSIS_FAILED);
        return NULL;
    }
    if (value.premise_count) {
        const cbm_resolve_proof_t **premises =
            cbm_arena_alloc(pool->arena, value.premise_count * sizeof(*premises));
        if (!premises) {
            proof_fail(pool, CBM_RESOLVE_ANALYSIS_FAILED);
            return NULL;
        }
        memcpy(premises, value.premises, value.premise_count * sizeof(*premises));
        value.premises = premises;
    }
    value.outcome.proof = NULL;
    entry->node.value = value;
    entry->next = pool->buckets[slot];
    pool->buckets[slot] = entry;
    pool->count++;
    return &entry->node;
}
static bool leaf_kind(cbm_resolve_proof_kind_t kind) {
    return kind >= CBM_PROOF_SOURCE_FACT && kind <= CBM_PROOF_REGISTERED_DECLARATION;
}
const cbm_resolve_proof_t *cbm_resolve_proof_leaf(cbm_resolve_proof_pool_t *pool,
                                                  cbm_resolve_proof_kind_t kind,
                                                  const cbm_resolve_proof_source_t *source,
                                                  const cbm_resolve_proof_key_t *key,
                                                  const cbm_resolve_result_t *outcome) {
    if (!leaf_kind(kind) || !key || !outcome || outcome->proof) {
        proof_fail(pool, CBM_RESOLVE_ANALYSIS_FAILED);
        return NULL;
    }
    cbm_resolve_proof_view_t value = {
        .kind = kind, .key = *key, .outcome = *outcome, .predecessors_closed = true, .depth = 1};
    if (outcome->analysis == CBM_RESOLVE_ANALYSIS_COMPLETE &&
        ((kind == CBM_PROOF_SOURCE_FACT &&
          (!source ||
           ((!source->file || !*source->file) && (!source->module || !*source->module)) ||
           !source->content_identity || !*source->content_identity)) ||
         (kind == CBM_PROOF_ADAPTER_TYPE &&
          (!key->type_identity || !*key->type_identity || !key->origin ||
           !cbm_resolve_admit(outcome) || !(outcome->facts & CBM_RESOLVE_FACT_TYPE_BOUND))) ||
         (kind == CBM_PROOF_ADAPTER_DOMAIN &&
          (!key->lookup_domain || !*key->lookup_domain || !key->lookup_key || !*key->lookup_key)) ||
         (kind == CBM_PROOF_REGISTERED_DECLARATION &&
          (!key->declaration_identity || !*key->declaration_identity)))) {
        value.outcome.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
        value.outcome.reason = "proof_leaf_inputs_incomplete";
    }
    if (value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE) {
        value.outcome.binding = CBM_RESOLVE_BINDING_UNRESOLVED;
        value.outcome.facts = 0;
        value.outcome.target = NULL;
        value.outcome.candidate_count = 0;
    }
    if (source)
        value.source = *source;
    return intern(pool, value);
}

static size_t memo_hash(const cbm_resolve_proof_t *proof) {
    uintptr_t p = (uintptr_t)proof >> 3;
    return (size_t)(p ^ (p >> 17));
}
static proof_memo_slot_t *memo_slot(proof_walk_t *walk, const cbm_resolve_proof_t *proof) {
    size_t slot = memo_hash(proof) & (walk->capacity - 1);
    while (walk->slots[slot].source && walk->slots[slot].source != proof)
        slot = (slot + 1) & (walk->capacity - 1);
    return &walk->slots[slot];
}
static bool memo_grow(proof_walk_t *walk) {
    if (walk->capacity && walk->count < walk->capacity / 2)
        return true;
    size_t capacity = walk->capacity ? walk->capacity * 2 : 32;
    if (capacity < walk->capacity || capacity > SIZE_MAX / sizeof(*walk->slots))
        return false;
    proof_memo_slot_t *slots = calloc(capacity, sizeof(*slots));
    if (!slots)
        return false;
    proof_memo_slot_t *old = walk->slots;
    size_t old_capacity = walk->capacity;
    walk->slots = slots;
    walk->capacity = capacity;
    for (size_t i = 0; i < old_capacity; i++)
        if (old[i].source)
            *memo_slot(walk, old[i].source) = old[i];
    free(old);
    return true;
}
static const cbm_resolve_proof_t *walk_node(proof_walk_t *walk, const cbm_resolve_proof_t *proof,
                                            unsigned depth) {
    if (!proof || !proof->value.depth || (proof->value.premise_count && !proof->value.premises)) {
        walk->failure = CBM_RESOLVE_ANALYSIS_FAILED;
        return NULL;
    }
    if (depth > PROOF_MAX_DEPTH || proof->value.depth > PROOF_MAX_DEPTH) {
        walk->failure = CBM_RESOLVE_ANALYSIS_LIMITED;
        return NULL;
    }
    if (walk->pool && cbm_resolve_proof_pool_owns(walk->pool, proof))
        return proof;
    if (!memo_grow(walk)) {
        walk->failure = CBM_RESOLVE_ANALYSIS_FAILED;
        return NULL;
    }
    proof_memo_slot_t *memo = memo_slot(walk, proof);
    if (memo->source) {
        if (!memo->destination)
            walk->failure = CBM_RESOLVE_ANALYSIS_FAILED;
        return memo->destination; /* NULL means a cycle/in-progress predecessor. */
    }
    if (walk->count >= walk->limit) {
        walk->failure = CBM_RESOLVE_ANALYSIS_LIMITED;
        return NULL;
    }
    *memo = (proof_memo_slot_t){.source = proof};
    walk->count++;
    size_t count = proof->value.premise_count;
    if (count > SIZE_MAX / sizeof(const cbm_resolve_proof_t *) ||
        (walk->pool && count > walk->pool->premise_limit)) {
        walk->failure = CBM_RESOLVE_ANALYSIS_LIMITED;
        return NULL;
    }
    const cbm_resolve_proof_t **premises = count ? malloc(count * sizeof(*premises)) : NULL;
    if (count && !premises) {
        walk->failure = CBM_RESOLVE_ANALYSIS_FAILED;
        return NULL;
    }
    bool ok = true;
    for (size_t i = 0; i < count; i++) {
        premises[i] = walk_node(walk, proof->value.premises[i], depth + 1);
        if (!premises[i]) {
            ok = false;
            break;
        }
    }
    const cbm_resolve_proof_t *result = NULL;
    if (ok && walk->pool) {
        cbm_resolve_proof_view_t value = proof->value;
        value.premises = premises;
        result = intern(walk->pool, value);
    } else if (ok && walk->visit(proof, walk->context)) {
        result = proof;
    }
    free(premises);
    memo_slot(walk, proof)->destination = result;
    return result;
}
bool cbm_resolve_proof_clone_begin(cbm_resolve_proof_clone_session_t *session,
                                   cbm_resolve_proof_pool_t *destination) {
    if (!session)
        return false;
    *session = (cbm_resolve_proof_clone_session_t){.destination = destination};
    if (!proof_step(destination, 1))
        return false;
    proof_walk_t *walk = calloc(1, sizeof(*walk));
    if (!walk)
        return proof_fail(destination, CBM_RESOLVE_ANALYSIS_FAILED);
    walk->pool = destination;
    walk->limit = destination->node_limit;
    session->state = walk;
    return true;
}
const cbm_resolve_proof_t *cbm_resolve_proof_clone_root(cbm_resolve_proof_clone_session_t *session,
                                                        const cbm_resolve_proof_t *proof) {
    if (!session || !session->state || !proof_step(session->destination, 1))
        return NULL;
    proof_walk_t *walk = session->state;
    const cbm_resolve_proof_t *result = walk_node(walk, proof, 1);
    if (!result && session->destination->failure == CBM_RESOLVE_ANALYSIS_COMPLETE)
        proof_fail(session->destination, walk->failure == CBM_RESOLVE_ANALYSIS_LIMITED
                                             ? CBM_RESOLVE_ANALYSIS_LIMITED
                                             : CBM_RESOLVE_ANALYSIS_FAILED);
    return result;
}
void cbm_resolve_proof_clone_end(cbm_resolve_proof_clone_session_t *session) {
    if (!session)
        return;
    proof_walk_t *walk = session->state;
    if (walk) {
        free(walk->slots);
        free(walk);
    }
    *session = (cbm_resolve_proof_clone_session_t){0};
}
const cbm_resolve_proof_t *cbm_resolve_proof_clone(cbm_resolve_proof_pool_t *destination,
                                                   const cbm_resolve_proof_t *proof) {
    if (destination && destination->failure == CBM_RESOLVE_ANALYSIS_COMPLETE &&
        cbm_resolve_proof_pool_owns(destination, proof))
        return proof_step(destination, 1) ? proof : NULL;
    cbm_resolve_proof_clone_session_t session;
    if (!cbm_resolve_proof_clone_begin(&session, destination))
        return NULL;
    const cbm_resolve_proof_t *result = cbm_resolve_proof_clone_root(&session, proof);
    cbm_resolve_proof_clone_end(&session);
    return result;
}
bool cbm_resolve_proof_visit(const cbm_resolve_proof_t *root, size_t node_limit,
                             cbm_resolve_proof_visit_fn visit, void *context) {
    if (!root || !visit)
        return false;
    proof_walk_t walk = {
        .visit = visit, .context = context, .limit = node_limit ? node_limit : PROOF_DEFAULT_NODES};
    bool ok = walk_node(&walk, root, 1) != NULL;
    free(walk.slots);
    return ok;
}
bool cbm_resolve_proof_visit_roots(const cbm_resolve_proof_t *const *roots, size_t root_count,
                                   size_t node_limit, cbm_resolve_proof_visit_fn visit,
                                   void *context) {
    if (!visit || (root_count && !roots) || root_count > SIZE_MAX / sizeof(*roots))
        return false;
    if (!root_count)
        return true;
    const cbm_resolve_proof_t **sorted = malloc(root_count * sizeof(*sorted));
    if (!sorted)
        return false;
    memcpy(sorted, roots, root_count * sizeof(*sorted));
    qsort(sorted, root_count, sizeof(*sorted), compare_premises);
    proof_walk_t walk = {
        .visit = visit, .context = context, .limit = node_limit ? node_limit : PROOF_DEFAULT_NODES};
    bool ok = true;
    for (size_t i = 0; i < root_count; i++) {
        if (!walk_node(&walk, sorted[i], 1)) {
            ok = false;
            break;
        }
    }
    free(walk.slots);
    free(sorted);
    return ok;
}
static unsigned state_rank(cbm_resolve_analysis_t state) {
    switch (state) {
    case CBM_RESOLVE_ANALYSIS_COMPLETE:
        return 0;
    case CBM_RESOLVE_ANALYSIS_UNKNOWN:
        return 1;
    case CBM_RESOLVE_ANALYSIS_PENDING:
        return 2;
    case CBM_RESOLVE_ANALYSIS_UNSUPPORTED:
        return 3;
    case CBM_RESOLVE_ANALYSIS_LIMITED:
        return 4;
    case CBM_RESOLVE_ANALYSIS_FAILED:
        return 5;
    }
    return 5;
}
const cbm_resolve_proof_t *cbm_resolve_proof_derive(cbm_resolve_proof_pool_t *pool,
                                                    cbm_resolve_proof_kind_t kind,
                                                    const cbm_resolve_proof_source_t *source,
                                                    const cbm_resolve_proof_key_t *key,
                                                    const cbm_resolve_result_t *outcome,
                                                    const cbm_resolve_proof_t *const *inputs,
                                                    size_t count, bool closed) {
    if (!proof_step(pool, 1))
        return NULL;
    if (kind < CBM_PROOF_TYPE_BIND || kind > CBM_PROOF_OCCURRENCE_BINDING || !key || !outcome ||
        !inputs || !count || (kind == CBM_PROOF_TYPE_COPY && count != 1) ||
        ((kind == CBM_PROOF_NAMED_COPY || kind == CBM_PROOF_NAMED_JOIN) && count < 2)) {
        proof_fail(pool, CBM_RESOLVE_ANALYSIS_FAILED);
        return NULL;
    }
    if (count > pool->premise_limit || count > SIZE_MAX / sizeof(*inputs)) {
        proof_fail(pool, CBM_RESOLVE_ANALYSIS_LIMITED);
        return NULL;
    }
    const cbm_resolve_proof_t **premises = malloc(count * sizeof(*premises));
    if (!premises) {
        proof_fail(pool, CBM_RESOLVE_ANALYSIS_FAILED);
        return NULL;
    }
    cbm_resolve_proof_view_t value = {
        .kind = kind, .key = *key, .outcome = *outcome, .predecessors_closed = closed, .depth = 1};
    if (source)
        value.source = *source;
    if (!closed && state_rank(value.outcome.analysis) < state_rank(CBM_RESOLVE_ANALYSIS_PENDING))
        value.outcome.analysis = CBM_RESOLVE_ANALYSIS_PENDING;
    proof_walk_t walk = {.pool = pool, .limit = pool->node_limit};
    bool ok = true;
    for (size_t i = 0; i < count; i++) {
        if (!proof_step(pool, 1)) {
            ok = false;
            break;
        }
        premises[i] = walk_node(&walk, inputs[i], 1);
        if (!premises[i]) {
            ok = false;
            break;
        }
        const cbm_resolve_proof_view_t *p = &premises[i]->value;
        if (state_rank(p->outcome.analysis) > state_rank(value.outcome.analysis))
            value.outcome.analysis = p->outcome.analysis;
        if (p->depth >= PROOF_MAX_DEPTH) {
            proof_fail(pool, CBM_RESOLVE_ANALYSIS_LIMITED);
            ok = false;
            break;
        }
        if (value.depth <= p->depth)
            value.depth = p->depth + 1;
    }
    free(walk.slots);
    if (!ok && pool->failure == CBM_RESOLVE_ANALYSIS_COMPLETE)
        proof_fail(pool, walk.failure == CBM_RESOLVE_ANALYSIS_LIMITED
                             ? CBM_RESOLVE_ANALYSIS_LIMITED
                             : CBM_RESOLVE_ANALYSIS_FAILED);
    const cbm_resolve_proof_t *result = NULL;
    if (ok) {
        qsort(premises, count, sizeof(*premises), compare_premises);
        size_t unique = 0;
        for (size_t i = 0; i < count; i++)
            if (!unique || premises[i] != premises[unique - 1])
                premises[unique++] = premises[i];
        value.premises = premises;
        value.premise_count = unique;
        if (value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE) {
            value.outcome.binding = CBM_RESOLVE_BINDING_UNRESOLVED;
            value.outcome.facts = 0;
            value.outcome.target = NULL;
            value.outcome.candidate_count = 0;
        }
        result = intern(pool, value);
    }
    free(premises);
    return result;
}
