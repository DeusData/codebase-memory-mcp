#ifndef CBM_RESOLVE_RUST_NAMES_H
#define CBM_RESOLVE_RUST_NAMES_H

#include "../cbm.h"
#include "model.h"

/* Rust name resolution over the real module tree: crates and their extern
 * prelude from Cargo.toml, `mod` file layout, items per namespace, use trees
 * with re-exports and globs, inherent and trait impls, and receiver types.
 *
 * Facts only: a resolved site names a declaration and says which rule found
 * it. Admission and graph projection stay in the shared resolver. Immutable
 * after seal, so workers may analyze files concurrently. */
typedef struct cbm_rust_project cbm_rust_project_t;
typedef struct cbm_rust_file_analysis cbm_rust_file_analysis_t;

cbm_rust_project_t *cbm_rust_project_new(void);
void cbm_rust_project_free(cbm_rust_project_t *project);

/* rel is the repository-relative Cargo.toml path. */
bool cbm_rust_project_add_manifest(cbm_rust_project_t *project, const char *rel, const char *text,
                                   size_t length);
/* Borrows source and root for the call only. defs supply graph identity. */
bool cbm_rust_project_add_file(cbm_rust_project_t *project, const char *rel,
                               const CBMFileResult *result, const char *source, size_t length,
                               TSNode root);
bool cbm_rust_project_seal(cbm_rust_project_t *project);

typedef struct {
    const char *dependency_file; /* the file of the module the path names */
    const char *member_qn;       /* the declaration a use leaf binds, or NULL */
    bool ambiguous;
    bool external; /* proved to leave the indexed project */
    const char *reason;
} cbm_rust_import_t;

/* One use leaf or `mod name;` declaration of rel, identified by its byte span. */
cbm_rust_import_t cbm_rust_project_import(const cbm_rust_project_t *project, const char *rel,
                                          const CBMImport *import);

typedef enum { CBM_RUST_SITE_CALL = 0, CBM_RUST_SITE_REF } cbm_rust_site_kind_t;

typedef struct {
    uint32_t start, end;
    cbm_rust_site_kind_t kind;
    const char *target;
    const char *label; /* the graph label this declaration must have */
    cbm_resolve_rule_t rule;
    uint32_t facts;
    cbm_resolve_binding_t binding;
    cbm_resolve_domain_t domain;
    cbm_resolve_analysis_t analysis;
    const char *reason;
} cbm_rust_site_t;

/* Walks one file under every module instance that contains it and keeps the
 * sites all instances agree on. Allocates from arena. NULL on allocation
 * failure only. */
cbm_rust_file_analysis_t *cbm_rust_analyze_file(const cbm_rust_project_t *project, CBMArena *arena,
                                                const char *rel, const char *source, size_t length,
                                                TSNode root);
const cbm_rust_site_t *cbm_rust_site_find(const cbm_rust_file_analysis_t *analysis,
                                          cbm_rust_site_kind_t kind, uint32_t start, uint32_t end);

#endif
