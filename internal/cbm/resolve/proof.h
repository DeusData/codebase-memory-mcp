#ifndef CBM_RESOLVE_PROOF_H
#define CBM_RESOLVE_PROOF_H

#include "model.h"
#include "../arena.h"
#include <stdbool.h>
#include <stddef.h>

/* Immutable captured-source identity. Empty fields explicitly mean unknown;
 * a caller must not recover them by re-reading a potentially changed file. */
typedef struct {
    const char *project;
    const char *file;
    const char *module;
    const char *scope;
    const char *content_identity;
    const char *configuration_identity;
    uint32_t source_origin;
} cbm_resolve_proof_source_t;

typedef enum {
    CBM_PROOF_SOURCE_FACT = 1,
    /* Coarse audited adapter input, not a claim about parsed predecessors. */
    CBM_PROOF_ADAPTER_TYPE,
    CBM_PROOF_ADAPTER_DOMAIN,
    CBM_PROOF_REGISTERED_DECLARATION,
    CBM_PROOF_TYPE_BIND,
    CBM_PROOF_TYPE_COPY,
    CBM_PROOF_TYPE_JOIN,
    CBM_PROOF_REGISTRY_LOOKUP,
    CBM_PROOF_IMPORT_BINDING,
    CBM_PROOF_EXPORT_BINDING,
    CBM_PROOF_NAMED_COPY,
    CBM_PROOF_NAMED_JOIN,
    CBM_PROOF_OCCURRENCE_BINDING
} cbm_resolve_proof_kind_t;

typedef struct {
    const char *lookup_domain;
    const char *lookup_key;
    const char *declaration_identity;
    const char *binding_identity;
    const char *type_identity;
    const char *detail;
    uint32_t start_byte;
    uint32_t end_byte;
    uint32_t origin;
    uint32_t flags;
    /* Closure of this observed lookup domain/key, independent of the DAG's
     * predecessor-set closure. Unreported/unknown observations stay false. */
    bool lookup_closed;
} cbm_resolve_proof_key_t;

/* Immutable public view; all strings and premise arrays belong to the node's
 * arena. Fingerprints accelerate full-key equality, never identify a fact by
 * themselves. Premises are a complete, structurally sorted, deduplicated set. */
typedef struct {
    cbm_resolve_proof_kind_t kind;
    cbm_resolve_proof_source_t source;
    cbm_resolve_proof_key_t key;
    cbm_resolve_result_t outcome;
    const cbm_resolve_proof_t *const *premises;
    size_t premise_count;
    bool predecessors_closed;
    uint64_t fingerprint;
    uint32_t depth;
} cbm_resolve_proof_view_t;

/* Public immutable payload for existing arena compaction/spill walkers.
 * There are no pool/arena pointers. outcome.proof is always NULL; edges are
 * exclusively in premises. Only an ownership copier/relocator may rewrite
 * these pointers while publishing an otherwise identical immutable DAG. */
struct cbm_resolve_proof {
    cbm_resolve_proof_view_t value;
};

typedef struct cbm_resolve_proof_entry cbm_resolve_proof_entry_t;
typedef struct cbm_resolve_proof_pool {
    CBMArena *arena;
    cbm_resolve_proof_entry_t **buckets;
    size_t capacity, count;
    uint64_t work, work_limit;
    size_t node_limit, premise_limit;
    cbm_resolve_analysis_t failure;
} cbm_resolve_proof_pool_t;

/* A zero limit means no cap: a pool holds one node per distinct fact and each
 * operation is linear in its own text, so its total follows the input. A fixed
 * default stopped a large enough file partway through, at a point that also
 * depended on the length of the checkout path. A pool is confined to its owning
 * analysis thread; published nodes are immutable. Its arena must outlive
 * nodes, but pool storage may end after construction. Never allocate into a
 * shared registry's arena. */
void cbm_resolve_proof_pool_init(cbm_resolve_proof_pool_t *pool, CBMArena *arena, size_t node_limit,
                                 size_t premise_limit, uint64_t work_limit);
/* A pool whose population is the whole project: one node per distinct fact,
 * each operation linear in its own text, so its total is a property of the
 * input rather than a runaway. A cap here does not bound anything real; it
 * refuses to publish a large enough repository, and because every hashed byte
 * counts, how large depends on the length of the checkout path. Only the
 * memory core can refuse this pool. */
void cbm_resolve_proof_pool_init_project(cbm_resolve_proof_pool_t *pool, CBMArena *arena);
const cbm_resolve_proof_view_t *cbm_resolve_proof_view(const cbm_resolve_proof_t *proof);
/* Exact pointer membership in this pool's full-key interned entries. A hash
 * match alone never establishes ownership. The node must still be alive. */
bool cbm_resolve_proof_pool_owns(const cbm_resolve_proof_pool_t *pool,
                                 const cbm_resolve_proof_t *proof);

/* Leaf kinds are SOURCE_FACT, ADAPTER_TYPE, ADAPTER_DOMAIN and
 * REGISTERED_DECLARATION. No derived kind can be made without premises. */
const cbm_resolve_proof_t *cbm_resolve_proof_leaf(cbm_resolve_proof_pool_t *pool,
                                                  cbm_resolve_proof_kind_t kind,
                                                  const cbm_resolve_proof_source_t *source,
                                                  const cbm_resolve_proof_key_t *key,
                                                  const cbm_resolve_result_t *outcome);

/* Shared closed-input settle primitive. COPY requires one predecessor; JOIN
 * requires at least one. NAMED_COPY/NAMED_JOIN require an audited rule plus
 * at least one dependency (two inputs minimum). COMPLETE is possible only for a closed set whose
 * every premise is COMPLETE. Pending/unknown/unsupported/limited/failed
 * inputs retain their state and clear derived binding authority. Every input
 * is cloned into this pool before interning; no temporary arena is borrowed. */
const cbm_resolve_proof_t *cbm_resolve_proof_derive(cbm_resolve_proof_pool_t *pool,
                                                    cbm_resolve_proof_kind_t kind,
                                                    const cbm_resolve_proof_source_t *source,
                                                    const cbm_resolve_proof_key_t *key,
                                                    const cbm_resolve_result_t *outcome,
                                                    const cbm_resolve_proof_t *const *premises,
                                                    size_t premise_count, bool predecessors_closed);

/* Deep clone + full-key interning. Shared predecessors are visited once.
 * Use a destination pool in CBMFileResult's arena before hybrid teardown. */
const cbm_resolve_proof_t *cbm_resolve_proof_clone(cbm_resolve_proof_pool_t *destination,
                                                   const cbm_resolve_proof_t *proof);

/* A generation can import many roots from one retained immutable source DAG
 * without walking shared ancestors again. Every source node/arena presented
 * to a session MUST remain alive and immutable until end; never reuse the
 * session across destroyed file arenas. Destination nodes are fully owned and
 * remain valid after both the session and all source arenas are destroyed. */
typedef struct {
    cbm_resolve_proof_pool_t *destination;
    void *state;
} cbm_resolve_proof_clone_session_t;
bool cbm_resolve_proof_clone_begin(cbm_resolve_proof_clone_session_t *session,
                                   cbm_resolve_proof_pool_t *destination);
const cbm_resolve_proof_t *cbm_resolve_proof_clone_root(cbm_resolve_proof_clone_session_t *session,
                                                        const cbm_resolve_proof_t *proof);
void cbm_resolve_proof_clone_end(cbm_resolve_proof_clone_session_t *session);

/* Deterministic full structural ordering, including hash collisions. Nodes
 * need not share a pool. No pointer address participates in identity/order. */
int cbm_resolve_proof_compare(const cbm_resolve_proof_t *left, const cbm_resolve_proof_t *right);

/* Canonical postorder visits each distinct structural node once. The callback
 * assigns persistence/generation IDs after its predecessors. Returning false
 * stops traversal; count/work limits fail closed. IDs must use the full view,
 * not fingerprint alone. */
typedef bool (*cbm_resolve_proof_visit_fn)(const cbm_resolve_proof_t *, void *context);
bool cbm_resolve_proof_visit(const cbm_resolve_proof_t *root, size_t node_limit,
                             cbm_resolve_proof_visit_fn visit, void *context);

/* Roots must belong to one canonical interned DAG (normally the generation
 * pool after clone-session import). Sorts the root set structurally and uses
 * one shared visited set; callback IDs do not depend on occurrence order.
 * Import foreign pools first so full-key equality is canonical pointer
 * equality and the callback observes the generation-owned node identities. */
bool cbm_resolve_proof_visit_roots(const cbm_resolve_proof_t *const *roots, size_t root_count,
                                   size_t node_limit, cbm_resolve_proof_visit_fn visit,
                                   void *context);

#endif
