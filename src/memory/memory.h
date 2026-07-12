/*
 * memory.h — User-global, revisioned knowledge memory.
 *
 * Global Memory is intentionally independent from repository graph stores.  A
 * handle owns one writable SQLite connection; it must not be shared between
 * threads.  Separate handles/processes coordinate through SQLite WAL and
 * entity-revision compare-and-swap checks.
 */
#ifndef CBM_MEMORY_H
#define CBM_MEMORY_H

#include <stdint.h>

typedef struct cbm_memory cbm_memory_t;
typedef struct cbm_store cbm_store_t;
struct sqlite3;

#define CBM_MEMORY_PROJECT "global-memory"
#define CBM_MEMORY_DB_FILENAME "_global_memory.db"
#define CBM_MEMORY_SCHEMA_VERSION 1

/* Open Global Memory.  When home_override is NULL or empty, resolution is:
 * CBM_MEMORY_HOME, then the platform user-data directory.  Opening creates the
 * raw/objects, wiki, export, and sync directories, initializes/migrates the DB,
 * and recovers pending wiki materialization outbox entries.
 *
 * The returned handle is not thread-safe.  Open one handle per worker/thread.
 */
cbm_memory_t *cbm_memory_open(const char *home_override);
void cbm_memory_close(cbm_memory_t *memory);

/* Handle-owned paths, valid until cbm_memory_close(). */
const char *cbm_memory_home(const cbm_memory_t *memory);
const char *cbm_memory_db_path(const cbm_memory_t *memory);

/* Underlying generic graph projection.  The pointer is owned by memory and is
 * exposed so the existing read-only Cypher engine can query graph="memory".
 */
cbm_store_t *cbm_memory_graph_store(cbm_memory_t *memory);

/* Borrow the underlying SQLite handle for deterministic export/import and
 * advanced integration.  The caller must not close it or use it concurrently
 * with another operation on the same cbm_memory_t handle. */
struct sqlite3 *cbm_memory_db(cbm_memory_t *memory);

/* Monotonically increasing committed-memory snapshot.  Returns -1 on error. */
int64_t cbm_memory_snapshot_epoch(cbm_memory_t *memory);

/* Rebuild derived nodes/edges and FTS documents from canonical memory_* rows.
 * Intended for deterministic import/recovery; numeric graph IDs are never a
 * portable part of the sharing format. */
int cbm_memory_rebuild_projection(cbm_memory_t *memory);

/* Recover expired leases and materialize all pending wiki outbox entries.
 * Returns the number completed by this call, or -1 on failure. */
int cbm_memory_materialize_pending(cbm_memory_t *memory);

/* Resolve symbolic CodeRefs against a repository graph, update their
 * resolution status/timestamp, and dirty linked memory when references are
 * missing.  code_store remains caller-owned. */
int cbm_memory_validate_code_refs(cbm_memory_t *memory, cbm_store_t *code_store,
                                  const char *project);

/* JSON APIs return a heap-allocated UTF-8 JSON object; caller frees with
 * free().  Domain/validation/conflict failures are returned as
 * {"ok":false,"error":..., ...}; NULL is reserved for allocation or fatal
 * internal failure.
 *
 * ingest: {content|string, path|string, title?, origin?, media_type?,
 *          published_at?, metadata?, revision_of?}
 * query:  {query?, mode?, current_context?, freshness?, limit?, as_of?}
 * propose:{proposal_id?, base_epoch?, agent_id?, session_id?, reason?,
 *          expected_revisions?, operations:[...]}
 * commit: {proposal_id, operation_id, agent_id?, session_id?}
 * lint:   {checks?:[...], limit?, apply?}
 * mark_code_changes:
 *         {project, files?:[...], qualified_names?:[...], deleted?:bool, reason?:string}
 */
char *cbm_memory_ingest_json(cbm_memory_t *memory, const char *args_json);
char *cbm_memory_query_json(cbm_memory_t *memory, const char *args_json);
char *cbm_memory_propose_json(cbm_memory_t *memory, const char *args_json);
char *cbm_memory_commit_json(cbm_memory_t *memory, const char *args_json);
char *cbm_memory_lint_json(cbm_memory_t *memory, const char *args_json);
char *cbm_memory_mark_code_changes_json(cbm_memory_t *memory, const char *args_json);

#endif /* CBM_MEMORY_H */
