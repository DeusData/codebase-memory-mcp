#ifndef CBM_RESOLVE_PY_MEMBERS_H
#define CBM_RESOLVE_PY_MEMBERS_H

#include "binding.h"

/* Project-wide Python class facts and the static member rules over them:
 * C3 method resolution order over resolved bases, instance attributes typed by
 * construction or annotation, annotated returns and module-level instances.
 *
 * A receiver gets a class only from one of those facts. Nothing here looks at
 * how many declarations share a member's name: an untyped `self.compiler.run()`
 * has no provable class and abstains even when one `run` exists in the project.
 * CALLS names the declaration these static rules select, not runtime dispatch,
 * so a subclass overriding a method does not change the edge from the base. */
typedef struct cbm_py_members cbm_py_members_t;

/* How a chain's root import reaches its shared solver fact. */
typedef struct {
    void *context;
    /* The fact key for a chain rooted at an import binding, and what follows
     * the part of the chain that import consumes. NULL when there is none. */
    const char *(*import_key)(void *context, CBMArena *arena,
                              const cbm_resolve_python_nested_import_t *nested, const char *chain,
                              const char **rest);
    /* The fact key of a name only a closed star import of this module binds. */
    const char *(*star_key)(void *context, CBMArena *arena, const char *name);
} cbm_py_import_lookup_t;

/* Stabilized cross-module bindings. All are read-only and safe from workers. */
typedef struct {
    const void *context;
    /* An import fact's value. external marks an import of no project module. */
    bool (*import_value)(const void *context, const char *key, const char **target, bool *module,
                         bool *external);
    /* `name` read from a module, as an exported binding or a submodule. */
    bool (*module_member)(const void *context, CBMArena *arena, const char *module,
                          const char *name, const char **target, bool *is_module);
    /* The declaration's graph label, NULL when it is not one project declaration. */
    const char *(*label)(const void *context, const char *qualified_name);
} cbm_py_members_env_t;

cbm_py_members_t *cbm_py_members_new(void);
void cbm_py_members_free(cbm_py_members_t *members);

/* Capture one audited file. Single-threaded, before sealing. */
bool cbm_py_members_add_file(cbm_py_members_t *members, const cbm_resolve_binding_index_t *index,
                             const cbm_py_import_lookup_t *imports);

/* Decide which decorated declarations keep their identity, using env. Runs before
 * sealing: class member tables and exports depend on the answer. The callback
 * receives each one that does, to publish it as an export. */
typedef bool (*cbm_py_keep_fn)(void *context, const char *target, bool is_class);
bool cbm_py_members_decide_decorated(cbm_py_members_t *members, const cbm_py_members_env_t *env,
                                     cbm_py_keep_fn keep, void *context);
/* Apply those decisions to a freshly built index of one file. */
void cbm_py_members_apply_decorated(const cbm_py_members_t *members,
                                    cbm_resolve_binding_index_t *index);

/* Resolve bases and compute every class's MRO once. Afterwards read-only. */
bool cbm_py_members_seal(cbm_py_members_t *members, const cbm_py_members_env_t *env);

/* The project classes a class names as bases, in source order: NULL past the end.
 * Bases reached through reexports and star imports are included, proven the same
 * way member lookup proves them. */
const char *cbm_py_members_base(const cbm_py_members_t *members, const char *qualified_name,
                                size_t offset);

/* The member declaration an attribute or call occurrence selects. False when no
 * static rule applies, leaving the caller's own outcome standing. */
bool cbm_py_members_resolve(const cbm_py_members_t *members, const cbm_py_members_env_t *env,
                            const cbm_py_import_lookup_t *imports,
                            const cbm_resolve_binding_index_t *index, CBMArena *arena,
                            const char *name, uint32_t start, uint32_t end, CBMSourceOrigin origin,
                            bool invocation, cbm_resolve_result_t *out);

#endif
