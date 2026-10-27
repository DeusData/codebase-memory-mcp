#ifndef CBM_RESOLVE_BINDING_H
#define CBM_RESOLVE_BINDING_H

#include "../cbm.h"
#include "model.h"
#include "lookup.h"

typedef struct cbm_resolve_binding_index cbm_resolve_binding_index_t;

/* Build once per source file. Borrows result/source until the arena is freed.
 * Supported syntax is deliberately explicit: Python module functions and
 * direct imports, Lua local functions, and explicit JavaScript/TypeScript
 * module function declarations/imports. Script globals remain unsupported. All
 * potential local/reassigned names block that name across the file; this
 * over-abstention is a quality gap. Unknown syntax, dynamic namespace effects
 * and partial parses fail closed. The source must be the raw buffer used to
 * produce result->cached_tree. */
cbm_resolve_binding_index_t *cbm_resolve_binding_build(CBMArena *arena, const CBMFileResult *result,
                                                       CBMLanguage language, const char *source,
                                                       size_t source_len);

/* Set exact caller identity without re-reading or hashing source. Captured
 * content/configuration IDs already came from result at build. */
bool cbm_resolve_binding_set_identity(cbm_resolve_binding_index_t *, const char *project,
                                      const char *file);
cbm_resolve_lookup_context_t *cbm_resolve_binding_lookup_context(
    const cbm_resolve_binding_index_t *);
/* Separate local validation root. Never incorporates/clones an incoming shared
 * import proof. The accessor returns only the matching last audited site. */
const cbm_resolve_proof_t *cbm_resolve_binding_site_proof(const cbm_resolve_binding_index_t *,
                                                          const char *name, uint32_t start,
                                                          uint32_t end, CBMSourceOrigin origin);

/* O(log n) site/name queries. Candidate identity must already be exact.
 * Local definitions are checked against the source declaration. Imported
 * names additionally need independent target/export LOOKUP_COMPLETE evidence;
 * a source alias alone cannot certify another module's export semantics.
 * Does not trust callee_is_locally_bound's default false value. */
cbm_resolve_result_t cbm_resolve_binding_validate(const cbm_resolve_binding_index_t *index,
                                                  const char *name, uint32_t start, uint32_t end,
                                                  CBMSourceOrigin origin,
                                                  cbm_resolve_result_t candidate);

/* A fixed diagnostic capability/reason string, never an inferred success. */
const char *cbm_resolve_binding_status(const cbm_resolve_binding_index_t *index);

enum { CBM_RESOLVE_EXPORT_VALUE = 1u << 0, CBM_RESOLVE_EXPORT_TYPE = 1u << 1 };

/* Named-export syntax proof, after the complete syntax/shadow/write audit.
 * Python direct module functions and explicit JS/TS module declarations only.
 * Functions expose VALUE, classes VALUE|TYPE, interfaces/type aliases TYPE.
 * Private/default-only/aliased/re-export bindings are not named-export proof.
 * This does not describe wildcard exports or externally observed mutations. */
unsigned cbm_resolve_binding_export_kind(const cbm_resolve_binding_index_t *index,
                                         const char *qualified_name);
/* True only for one unshadowed source import in a completely audited module. */
bool cbm_resolve_binding_import(const cbm_resolve_binding_index_t *index, const char *local_name);
bool cbm_resolve_binding_export(const cbm_resolve_binding_index_t *index,
                                const char *qualified_name);

/* HTTP-only source facts; these do not expose a generic default export or
 * namespace binding. A true default result has an exact const value identity;
 * base == NULL means that the composed relative URL remains unknown. */
/* Runtime module-value access enumerated from the same AST audit. A NULL
 * local_name is a namespace/re-export/loader escape unless side_effect_only.
 * All strings are borrowed from the binding index arena. */
typedef struct {
    const char *path;
    const char *local_name;
    bool side_effect_only;
} cbm_resolve_http_dependency_t;
bool cbm_resolve_binding_http_domain_complete(const cbm_resolve_binding_index_t *);
size_t cbm_resolve_binding_http_dependency_count(const cbm_resolve_binding_index_t *);
const cbm_resolve_http_dependency_t *cbm_resolve_binding_http_dependency_at(
    const cbm_resolve_binding_index_t *, size_t);
bool cbm_resolve_binding_http_import_safe(const cbm_resolve_binding_index_t *, const char *,
                                          bool axios_factory);
/* Exact default-import source literal; NULL for blocked or other import forms. */
const char *cbm_resolve_binding_http_import_path(const cbm_resolve_binding_index_t *, const char *);
bool cbm_resolve_binding_http_default(const cbm_resolve_binding_index_t *, const char **target,
                                      const char **base);
bool cbm_resolve_binding_import_occurrence(const cbm_resolve_binding_index_t *, const CBMCall *,
                                           const char *local_name);

/* Source-audited library namespace facts. A complete namespace includes every
 * public spelling, including declarations whose edge binding is unsupported.
 * These facts describe one library file only; parts/re-exports fail closed. */
typedef struct {
    const char *uri;
    const char *prefix;
    const char *const *show;
    size_t show_count;
    const char *const *hide;
    size_t hide_count;
    bool has_show;
} cbm_resolve_source_import_t;

/* Strong, source-audited Python absence domain. Inert package initialization
 * only: imports, assignments, classes, decorators, defaults, dynamic attributes
 * and conditional/effectful statements remain open. Names are sorted and may
 * include NULL entries for non-module scopes; copy while the index is alive. */
/* Dynamic execution/module registry access remains an unknown project effect. */
bool cbm_resolve_binding_python_effects_complete(const cbm_resolve_binding_index_t *);
/* Conservatively includes writes and unknown alias/callee escapes. */
bool cbm_resolve_binding_python_import_escaped(const cbm_resolve_binding_index_t *, const char *);
bool cbm_resolve_binding_python_namespace_closed(const cbm_resolve_binding_index_t *index);
size_t cbm_resolve_binding_python_namespace_size(const cbm_resolve_binding_index_t *index);
const char *cbm_resolve_binding_python_namespace_name(const cbm_resolve_binding_index_t *index,
                                                      size_t offset);

bool cbm_resolve_binding_namespace_complete(const cbm_resolve_binding_index_t *index);
size_t cbm_resolve_binding_namespace_size(const cbm_resolve_binding_index_t *index);
const char *cbm_resolve_binding_namespace_member(const cbm_resolve_binding_index_t *index,
                                                 size_t offset, unsigned *kind);
size_t cbm_resolve_binding_import_count(const cbm_resolve_binding_index_t *index);
const cbm_resolve_source_import_t *cbm_resolve_binding_import_at(
    const cbm_resolve_binding_index_t *index, size_t offset);
bool cbm_resolve_binding_name_present(const cbm_resolve_binding_index_t *index, const char *name);

/* Source-established lexical declaration candidate. Projection still must
 * verify exact source/target graph ownership before emitting an edge. */
cbm_resolve_result_t cbm_resolve_binding_local(const cbm_resolve_binding_index_t *, const char *,
                                               uint32_t, uint32_t, CBMSourceOrigin);

#endif
