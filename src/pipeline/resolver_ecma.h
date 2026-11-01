#ifndef CBM_PIPELINE_RESOLVER_ECMA_H
#define CBM_PIPELINE_RESOLVER_ECMA_H

#include "pipeline_internal.h"
#include "resolve/binding.h"
#include "resolve/lookup.h"

/* ECMAScript module surfaces for the whole project: exported name -> declaration,
 * following aliases, default exports, re-exports, `export *` and CommonJS
 * `module.exports`. Written once by the coordinator while the shared resolver is
 * built; read concurrently by workers afterwards, so a lookup never writes here.
 * A lookup has no work budget: its cost is bounded by the visited set. */
typedef struct cbm_ecma_table cbm_ecma_table_t;

typedef enum {
    CBM_ECMA_UNKNOWN = 0, /* a surface on the way is unproved or outside the project */
    CBM_ECMA_ABSENT,      /* every surface consulted is closed and lacks the name */
    CBM_ECMA_AMBIGUOUS,   /* two different declarations answer the name */
    CBM_ECMA_DECLARATION, /* target, or target's `member` when member is set */
    CBM_ECMA_NAMESPACE    /* the named exports of file */
} cbm_ecma_status_t;

typedef struct {
    cbm_ecma_status_t status;
    const char *target;
    const char *member;
    const char *file;
} cbm_ecma_value_t;

cbm_ecma_table_t *cbm_ecma_table_new(CBMArena *arena);
void cbm_ecma_table_free(cbm_ecma_table_t *table);

/* The surfaces the cross-file type pass and import maps read. Audit-free: the
 * shared resolver keeps its own table whose lookups carry proofs. A file that
 * fails here is simply absent, which every lookup reports as unknown. */
cbm_ecma_table_t *cbm_ecma_surfaces_build(const cbm_pipeline_ctx_t *ctx,
                                          const cbm_file_info_t *files, int file_count,
                                          CBMFileResult **cache);
void cbm_ecma_surfaces_free(cbm_ecma_table_t *surfaces);

/* Copies everything it keeps. `audit` is the file's source audit and joins the
 * proof of every lookup that answers through this module. */
bool cbm_ecma_table_add(cbm_ecma_table_t *table, const cbm_pipeline_ctx_t *ctx, const char *file,
                        const char *module_qn, const cbm_resolve_binding_index_t *index,
                        const cbm_resolve_proof_t *audit);

/* The repository file a module specifier in `file` names, or NULL. */
const char *cbm_ecma_dependency(const cbm_pipeline_ctx_t *ctx, const char *file, const char *source,
                                CBMArena *arena);

/* `query` may be NULL. Each module on the answering path adds its audit and the
 * name it answered, so the proof names what the answer rests on. TypeScript keeps
 * values and types apart, so `export * as X` and `export interface X` coexist;
 * the first lookup asks for the value, the second for the type. */
cbm_ecma_value_t cbm_ecma_lookup(const cbm_ecma_table_t *table, const char *file, const char *name,
                                 cbm_resolve_lookup_t *query);
cbm_ecma_value_t cbm_ecma_lookup_type(const cbm_ecma_table_t *table, const char *file,
                                      const char *name, cbm_resolve_lookup_t *query);
/* `import x from file` */
cbm_ecma_value_t cbm_ecma_default(const cbm_ecma_table_t *table, const char *file,
                                  cbm_resolve_lookup_t *query);
/* What `require(file)` evaluates to: module.exports, or the named exports. */
cbm_ecma_value_t cbm_ecma_whole(const cbm_ecma_table_t *table, const char *file,
                                cbm_resolve_lookup_t *query);

#endif
