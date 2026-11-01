#ifndef CBM_PIPELINE_SHARED_RESOLVER_H
#define CBM_PIPELINE_SHARED_RESOLVER_H

#include "pipeline_internal.h"
#include "foundation/hash_table.h"
#include "resolve/solver.h"

typedef struct cbm_shared_resolver cbm_shared_resolver_t;

/* Register immutable extraction snapshots before infrastructure adapters run.
 * Reusing the same rows is idempotent; changed extraction identities fail closed.
 * Freezes original extraction quality separately from later LSP failure.
 * Reads the original in-memory/spilled results, never replacement file bytes;
 * an existing spill slot that cannot be read is a fatal capture failure. */
bool cbm_shared_resolver_capture_sources(const cbm_pipeline_ctx_t *ctx,
                                         const cbm_file_info_t *files, int file_count,
                                         CBMFileResult **cache);

/* Immutable after build, shared by every file worker. Cache access includes
 * spilled results; exported binding proofs are computed once per file. */
cbm_shared_resolver_t *cbm_shared_resolver_build(const cbm_pipeline_ctx_t *ctx,
                                                 const cbm_file_info_t *files, int file_count,
                                                 CBMFileResult **cache);
void cbm_shared_resolver_free(cbm_shared_resolver_t *resolver);

typedef struct {
    const char *file;
    cbm_resolve_solver_result_t binding;
} cbm_shared_resolver_export_summary_t;

/* Canonically sorted real exported bindings, excluding private import/query
 * facts. Borrowed strings and proof roots live until shared resolver teardown.
 * Persist in a generation-owned proof pool; never clone into each file arena. */
size_t cbm_shared_resolver_export_count(const cbm_shared_resolver_t *resolver);
const cbm_shared_resolver_export_summary_t *cbm_shared_resolver_export_at(
    const cbm_shared_resolver_t *resolver, size_t index);
bool cbm_shared_resolver_exports_complete(const cbm_shared_resolver_t *resolver);
/* Immutable shared type/binding result for hybrid consumers. Missing named
 * facts remain UNKNOWN and can never be donated as a lexical type. */
void cbm_shared_resolver_fact(const cbm_shared_resolver_t *, const char *,
                              cbm_resolve_solver_result_t *);
/* A Python class's proven project base, in source order: NULL past the end. */
const char *cbm_shared_resolver_python_base(const cbm_shared_resolver_t *resolver,
                                            const char *qualified_name, size_t offset);
bool cbm_shared_resolver_owns_proof(const cbm_shared_resolver_t *resolver,
                                    const cbm_resolve_proof_t *proof);

/* HTTP-only default-client observation. Checks the exact importing occurrence,
 * caller scope, content and configuration against the immutable shared audit.
 * A true result with NULL base proves the client, but not a relative endpoint. */
/* Negative-only annotation candidate: blocks weaker service-name fallback
 * after the audited HTTP lookup fails. This is never edge authority. */
bool cbm_shared_resolver_http_candidate_receiver(const cbm_shared_resolver_t *, const char *,
                                                 const CBMCall *);
bool cbm_shared_resolver_http_base(const cbm_shared_resolver_t *, const char *file,
                                   const CBMFileResult *, const CBMCall *, const char **base);

typedef struct {
    const cbm_pipeline_ctx_t *ctx;
    const cbm_registry_t *registry;
    const cbm_gbuf_t *graph;
    cbm_gbuf_t *output;
    const CBMFileResult *result;
    const char *rel_path;
    const char *module_qn;
    CBMLanguage language;
    const char **import_keys;
    const char **import_values;
    int import_count;
    /* Optional raw source from the extraction generation. The orchestration
     * verifies the manifest before publishing; an absent buffer is read once. */
    const char *source;
    size_t source_len;
} cbm_resolver_file_t;

/* Records every extracted in-scope occurrence, including failures and
 * unsupported syntax, and emits only admitted exact targets. False means an
 * allocation/record/edge-write failed; the generation must not be published. */
bool cbm_shared_resolver_file(const cbm_shared_resolver_t *resolver,
                              const cbm_resolver_file_t *file);

/* The only projection authority for CALLS/USAGE/CALL_REFERENCE/READS/WRITES/
 * USES_TYPE/IMPORTS. Removes legacy bypasses and rebuilds in deterministic
 * occurrence order. Invalid emitted claims fail closed. */
bool cbm_shared_resolver_project(cbm_gbuf_t *graph);

/* One sequential adapter pass: O(1) extraction-snapshot lookups and owned
 * immutable proof storage. Initialize once after extraction summaries exist;
 * the caller's arena outlives the adapter and all pending occurrences. */
typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMArena *arena;
    cbm_resolve_proof_pool_t proofs;
    CBMHashTable *source_files;
} cbm_shared_resolver_adapter_t;

bool cbm_shared_resolver_adapter_init(cbm_shared_resolver_adapter_t *adapter,
                                      cbm_pipeline_ctx_t *ctx, CBMArena *arena);
void cbm_shared_resolver_adapter_dispose(cbm_shared_resolver_adapter_t *adapter);
/* Verify the analyzed raw bytes against the original extraction summary and
 * retain that summary's configuration identity. Register publication-time
 * revalidation; never manufacture a replacement extraction identity. */
bool cbm_shared_resolver_adapter_capture(cbm_shared_resolver_adapter_t *adapter, const char *file,
                                         CBMLanguage language, const char *bytes, size_t length,
                                         cbm_resolve_proof_source_t *source);
/* Capture exact original spelling while its buffer is alive. Set source.scope
 * to the graph owner and relation to the audited AST role. The returned proof
 * owns its strings; source bytes can be released before lookup completes. */
const cbm_resolve_proof_t *cbm_shared_resolver_adapter_occurrence(
    cbm_shared_resolver_adapter_t *adapter, const cbm_resolve_proof_source_t *source,
    const char *bytes, size_t length, uint32_t start, uint32_t end, const char *spelling,
    const char *relation);
/* semantic is the adapter's independent closed lookup proof, with the same
 * source/span/spelling/role and exact target identity. Unsupported, ambiguous
 * and unresolved outcomes are recorded too. No raw graph edge is authority.
 * Any malformed proof or allocation failure makes publication fail closed. */
bool cbm_shared_resolver_adapter_record_proofs(cbm_shared_resolver_adapter_t *adapter,
                                               const cbm_resolve_proof_t *occurrence, int64_t owner,
                                               int64_t target, const char *properties,
                                               const cbm_resolve_proof_t *semantic);
/* Convenience for lookups completed while the original buffer is retained. */
bool cbm_shared_resolver_adapter_record(cbm_shared_resolver_adapter_t *adapter,
                                        const cbm_resolve_proof_source_t *source, const char *bytes,
                                        size_t length, uint32_t start, uint32_t end,
                                        const char *spelling, int64_t owner, int64_t target,
                                        const char *relation, const char *properties,
                                        const cbm_resolve_proof_t *semantic);

#endif
